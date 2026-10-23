# -*- coding: utf-8 -*-
"""BaseCrawler 抽象基类：子类只需实现 targets/parse/store，其余全自动。

自动处理：
- 分片（多 pod 按页码/URL 序号 % 副本数 分配）
- 代理路由（租代理→走代理请求→失败回退直连→归还是否成功）
- 账号租用与归还（可选）
- 打点（request/crawl/store_fail/proxy/account 全指标）
- 批量入库（CH crawl_data 表）
- run 上报（含业务汇总）
"""

import datetime
import os
import signal
import sys
import time
import urllib.parse

import requests

from .log import logger
from .metrics import M
from .session import lease_account, release_account, lease_proxy, release_proxy
from .storage import batch_insert, report_run


class BaseCrawler:
    """爬虫基类。子类实现 targets() / parse() / store() 三个方法。"""

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

        # 分片
        self.shard_index = int(os.environ.get("JOB_COMPLETION_INDEX", "0") or 0)
        self.shard_replicas = max(1, int(os.environ.get("SUPERSPIDER_REPLICAS", "1") or 1))

        # 优雅停止：待入库缓冲 + 停止标记（run 循环与信号处理器共用）
        self.pending: list[dict] = []
        self._stopping = False

        # 资源
        self.proxy = lease_proxy() if self.use_proxy else None
        self.account = lease_account() if self.use_account else None
        self.session = self._build_session()
        if self.proxy:
            self.stats["ip"] += 1
            M.proxy_request(ok=True, ip_consumed=1)

        # SIGTERM/SIGINT → 秒级优雅停止（冲账、释放租约、上报 stopped）。
        # 注册了 handler 的进程不受内核 PID1 默认信号免疫影响，长驻模式必须。
        signal.signal(signal.SIGTERM, self._on_stop_signal)
        signal.signal(signal.SIGINT, self._on_stop_signal)

    # ── 子类必须实现 ────────────────────────────────────────────────

    def targets(self) -> list[str]:
        """返回要抓取的 URL 列表（已按分片过滤或全量由基类分片）。"""
        raise NotImplementedError

    def parse(self, html: str, url: str) -> list[dict]:
        """解析页面 → 业务数据字典列表。打点由基类负责。"""
        raise NotImplementedError

    def store(self, items: list[dict]) -> int:
        """入库。返回实际入库条数（打 store_fail 由基类算）。"""
        raise NotImplementedError

    # ── 可选覆写 ────────────────────────────────────────────────────

    def on_start(self):
        """运行前钩子（如登录）。"""

    def on_finish(self, status: str):
        """运行后钩子。"""

    # ── 基类内部 ────────────────────────────────────────────────────

    def _build_session(self):
        session = requests.Session()
        session.headers["User-Agent"] = "SuperSpider-Crawler/0.2"
        if self.proxy:
            proxy_url = f"{self.proxy['protocol']}://{self.proxy['host']}:{self.proxy['port']}"
            session.proxies = {"http": proxy_url, "https": proxy_url}
        return session

    def _shard_targets(self, urls: list[str]) -> list[str]:
        """按 URL 序号 % 副本数 分片。"""
        if self.shard_replicas <= 1:
            return urls
        return [u for i, u in enumerate(urls) if i % self.shard_replicas == self.shard_index]

    def _fetch(self, url: str) -> str | None:
        try:
            resp = self.session.get(url, timeout=15)
        except requests.exceptions.Timeout:
            self.stats["timeout"] += 1
            M.request(ok=False, error_type="timeout")
            return None
        except requests.exceptions.ProxyError:
            self.stats["blocked"] += 1
            M.proxy_request(ok=False, via_proxy=True)
            return None
        except requests.exceptions.RequestException:
            self.stats["server"] += 1
            M.request(ok=False, error_type="server_5xx")
            return None
        if resp.status_code == 429:
            self.stats["blocked"] += 1
            M.request(ok=False, error_type="blocked_429")
            return None
        if resp.status_code >= 500:
            self.stats["server"] += 1
            M.request(ok=False, error_type="server_5xx")
            return None
        M.request(ok=True, duration=resp.elapsed.total_seconds())
        return resp.text

    def _report_run(self, status: str, error_message: str = ""):
        task_id = os.environ.get("SUPERSPIDER_TASK_ID", "")
        if not task_id.isdigit():
            logger.warning("run report skip: no SUPERSPIDER_TASK_ID")
            return
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        payload = {
            "taskId": int(task_id),
            "projectId": int(os.environ.get("SUPERSPIDER_PROJECT_ID", "0") or 0),
            "batchId": os.environ.get("SUPERSPIDER_BATCH_ID", ""),
            "pod": os.environ.get("SUPERSPIDER_POD_NAME") or os.environ.get("HOSTNAME", ""),
            "status": status,
            "triggerType": os.environ.get("SUPERSPIDER_TRIGGER_TYPE", "manual"),
            "startedAt": self.started_at,
            "finishedAt": now,
            "durationMs": int((time.time() - self.start_ts) * 1000),
            "retryCount": 0,
            "errorMessage": error_message,
            "crawledCount": self.stats["crawled"],
            "storeFailCount": self.stats["store_fail"],
            "errorTimeout": self.stats["timeout"],
            "errorBlocked": self.stats["blocked"],
            "errorServer": self.stats["server"],
            "errorValidation": self.stats["validation"],
            "ipConsumed": self.stats["ip"],
        }
        result = report_run(payload)
        logger.info("run reported | result={}", result)

    def _store_batch(self, items: list[dict]) -> bool:
        """批量写入 CH crawl_data 表（附带 run 元数据）。返回是否全部成功。"""
        if not items:
            return True
        task_id = os.environ.get("SUPERSPIDER_TASK_ID", "0")
        project = os.environ.get("SUPERSPIDER_PROJECT", "")
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for item in items:
            row = {"task_id": int(task_id or 0), "project": project, "crawled_at": now}
            row.update(item)
            rows.append(row)
        ok = batch_insert("crawl_data", rows)
        logger.info("ch batch inserted | count={} ok={}", len(rows), ok)
        return ok

    # ── 主入口 ──────────────────────────────────────────────────────

    def _process(self, url: str) -> list[dict] | None:
        """抓取+解析+计数。返回 None=页面失败；空列表=空页。"""
        html = self._fetch(url)
        if html is None:
            return None
        try:
            items = self.parse(html, url)
        except Exception as exc:
            logger.error("parse error | url={} err={}", url, exc)
            self.stats["validation"] += 1
            M.request(ok=False, error_type="validation")
            return []
        if items:
            self.stats["crawled"] += len(items)
            M.crawl(len(items))
        return items

    def run(self):
        M.start()
        logger.info("crawl start | shard={}/{} proxy={} account={}",
                    self.shard_index, self.shard_replicas,
                    "yes" if self.proxy else "direct", "yes" if self.account else "no")

        self.on_start()

        all_targets = self.targets()
        targets = self._shard_targets(all_targets)
        logger.info("targets assigned | all={} mine={} shard={}", len(all_targets), len(targets), self.shard_index)

        deferred: list[str] = []  # 经代理 200 但解析为空的页，待切直连后重抓
        for url in targets:
            if self._stopping:  # 双保险：信号处理器异常时循环也能退出
                break
            items = self._process(url)
            if items is None:
                time.sleep(2)
                continue
            self.pending.extend(items)

            if not items and self.proxy:
                # 200 但解析为空：免费代理常见劣化（劫持/插页返回垃圾 200 页）
                self.stats["empty_pages"] += 1
                logger.warning("empty parse via proxy | count={} url={}", self.stats["empty_pages"], url)
                deferred.append(url)
                if self.stats["empty_pages"] >= 2:
                    logger.warning("proxy degraded, release and switch to direct")
                    M.proxy_request(ok=False, via_proxy=True)
                    release_proxy(self.proxy, False)
                    self.proxy = None
                    self.session = self._build_session()
                    for u in deferred:
                        got = self._process(u)
                        if got:
                            self.pending.extend(got)
                    deferred = []
                continue

            # 批量入库（每 50 条或最后）
            if len(self.pending) >= 50:
                stored = self.store(self.pending)
                if stored < len(self.pending):
                    fail = len(self.pending) - stored
                    self.stats["store_fail"] += fail
                    M.store_fail(fail)
                self.pending = []

            self.stats["pages"] += 1
            time.sleep(self.page_delay)

        # 收尾
        ok = self.stats["timeout"] + self.stats["blocked"] + self.stats["server"] < 5
        status = "success" if ok else "failed"
        err = "" if ok else f"网络失败过多（timeout={self.stats['timeout']}, blocked={self.stats['blocked']}, 5xx={self.stats['server']}）"
        self._finish(status, err)
        logger.bind(**self.stats).info("crawl done | status={}", status)

    # ── 优雅停止 ────────────────────────────────────────────────────

    def _finish(self, status: str, err: str = ""):
        """收尾共用：冲账 → 归还租约 → 上报 run → on_finish。正常完成与信号停止共用。"""
        if self.pending:
            stored = self.store(self.pending)
            if stored < len(self.pending):
                fail = len(self.pending) - stored
                self.stats["store_fail"] += fail
                M.store_fail(fail)
            self.pending = []
        release_account(self.account, self.stats.get("login_ok", True))
        release_proxy(self.proxy, self.stats["blocked"] + self.stats["timeout"] == 0)
        self._report_run(status, err)
        self.on_finish(status)

    def _on_stop_signal(self, signum, _frame):
        """SIGTERM/SIGINT：冲账、释放租约、上报 stopped 后秒级退出。

        二次信号（清理进行中再收到）直接 os._exit，避免重入 HTTP 调用；
        30s K8s 宽限期是最后兜底。sys.exit 触发 atexit → VM 指标最终推送。
        """
        if self._stopping:
            os._exit(0)
        self._stopping = True
        name = signal.Signals(signum).name
        logger.warning("graceful stop: signal received | signal={}", name)
        try:
            self._finish("stopped", f"收到 {name} 优雅停止")
            logger.bind(**self.stats).warning("graceful stop finished | status=stopped")
        except Exception as exc:
            logger.error("graceful stop cleanup error (exit anyway) | err={}", exc)
        sys.exit(0)
