# -*- coding: utf-8 -*-
"""Scrapy 运行封装：设置组装 + 信号接线（指标 / run 上报）。

- CrawlContext：spider 与中间件共享的运行状态（stats 语义对齐 BaseCrawler）
- run_scrapy()：CrawlerProcess 单进程跑一个 spider；SIGTERM 由 CrawlerProcess
  优雅排空 → spider_closed(reason=shutdown) → 上报 stopped → atexit 推最终指标
- 限制：Twisted reactor 不可重启，Scrapy 爬虫只支持单次模式（CRAWL_LOOP 不适用）
"""

import datetime
import os
import time

from ..log import logger
from ..metrics import M
from ..storage import report_run


class CrawlContext:
    """挂在 spider.ctx 上，中间件经 spider 参数访问。"""

    def __init__(self):
        self.start_ts = time.time()
        self.started_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.stats = {
            "crawled": 0, "store_fail": 0, "timeout": 0, "blocked": 0,
            "server": 0, "validation": 0, "ip": 0, "pages": 0, "empty_pages": 0,
        }
        self.page_delay = float(os.environ.get("CRAWL_PAGE_DELAY", "1.5"))
        self.use_proxy = os.environ.get("CRAWL_USE_PROXY", "1") == "1"
        self.use_account = os.environ.get("CRAWL_NEED_ACCOUNT", "0") == "1"
        self.account_platform = ""  # spider 按目标站点覆写
        self.shard_index = int(os.environ.get("JOB_COMPLETION_INDEX", "0") or 0)
        self.shard_replicas = max(1, int(os.environ.get("SUPERSPIDER_REPLICAS", "1") or 1))
        self.proxy = None
        self.on_proxy_degraded = None  # ProxyMiddleware 注入

    def degrade_proxy(self, reason: str) -> None:
        """spider 侧发现代理劣化（200 但解析空）时调用。"""
        if self.on_proxy_degraded:
            self.on_proxy_degraded(reason)


def _build_settings() -> dict:
    delay = float(os.environ.get("CRAWL_PAGE_DELAY", "1.5"))
    return {
        # 礼貌速率 + 自动节流；robots 显式不遵循（平台既有约定）
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": delay,
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_TIMEOUT": 15,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": delay,
        "AUTOTHROTTLE_MAX_DELAY": 30,
        "RETRY_TIMES": 2,
        # 403 不盲目重试：同身份重试无意义，由中间件换代理/换号 + spider 直连重抓处理
        "RETRY_HTTP_CODES": [408, 429, 500, 502, 503, 504],
        "COOKIES_ENABLED": False,  # Cookie/UA 由 AccountMiddleware 管理
        "HTTPCACHE_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "LOG_LEVEL": os.environ.get("CRAWL_LOG_LEVEL", "INFO"),
        "DOWNLOADER_MIDDLEWARES": {
            "superspider.scrapy.middlewares.AccountMiddleware": 50,
            "superspider.scrapy.middlewares.ProxyMiddleware": 740,
            "superspider.scrapy.middlewares.MetricsMiddleware": 860,
        },
        "ITEM_PIPELINES": {
            "superspider.scrapy.pipelines.ClickHousePipeline": 300,
        },
    }


def _report_run(ctx: CrawlContext, status: str, error_message: str = "") -> None:
    task_id = os.environ.get("SUPERSPIDER_TASK_ID", "")
    if not task_id.isdigit():
        logger.warning("run report skip: no SUPERSPIDER_TASK_ID")
        return
	payload = {
		"taskId": int(task_id),
		"projectId": int(os.environ.get("SUPERSPIDER_PROJECT_ID", "0") or 0),
		"batchId": os.environ.get("SUPERSPIDER_BATCH_ID", ""),
		"pod": os.environ.get("HOSTNAME", ""),
		"status": status,
        "triggerType": os.environ.get("SUPERSPIDER_TRIGGER_TYPE", "manual"),
        "startedAt": ctx.started_at,
        "finishedAt": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "durationMs": int((time.time() - ctx.start_ts) * 1000),
        "retryCount": 0,
        "errorMessage": error_message,
        "crawledCount": ctx.stats["crawled"],
        "storeFailCount": ctx.stats["store_fail"],
        "errorTimeout": ctx.stats["timeout"],
        "errorBlocked": ctx.stats["blocked"],
        "errorServer": ctx.stats["server"],
        "errorValidation": ctx.stats["validation"],
        "ipConsumed": ctx.stats["ip"],
    }
    result = report_run(payload)
    logger.info("run reported | result={}", result)


def run_scrapy(spider_cls) -> None:
    from scrapy import signals
    from scrapy.crawler import CrawlerProcess

    M.start()  # 30s 周期推送；最终值由 atexit 推（与 BaseCrawler 相同）
    process = CrawlerProcess(settings=_build_settings())
    crawler = process.create_crawler(spider_cls)

    def on_item(item, response, spider):
        spider.ctx.stats["crawled"] += 1
        M.crawl(1)

    def on_spider_error(failure, response=None, spider=None):
        if spider is None and response is not None and hasattr(response, "request"):
            spider = response.request.meta.get("_spider")
        if spider is None:
            return
        spider.ctx.stats["validation"] += 1
        M.request(ok=False, error_type="validation")

    def on_spider_closed(spider, reason):
        stats = spider.ctx.stats
        net_fail = stats["timeout"] + stats["blocked"] + stats["server"]
        if reason in ("shutdown", "cancelled"):
            status = "stopped"
        elif reason == "finished" and net_fail < 5:
            status = "success"
        else:
            status = "failed"
        err = "" if status == "success" else f"close reason={reason} net_fail={net_fail} stats={stats}"
        _report_run(spider.ctx, status, err)
        logger.bind(**stats).info("scrapy done | reason={} status={}", reason, status)

    crawler.signals.connect(on_item, signal=signals.item_scraped)
    crawler.signals.connect(on_spider_error, signal=signals.spider_error)
    crawler.signals.connect(on_spider_closed, signal=signals.spider_closed)

    process.crawl(crawler)
    process.start()  # 阻塞至 reactor 停止
