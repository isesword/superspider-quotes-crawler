# -*- coding: utf-8 -*-
"""Scrapy 下载中间件：指标、代理路由、账号/身份。

设计约定（行为与指标口径对齐 BaseCrawler）：
- 共享状态挂 spider.ctx（CrawlContext），经 spider 参数访问
- MetricsMiddleware：每个响应/异常打 M.request / M.proxy_request（规范 7 指标）
- ProxyMiddleware：spider_opened 租用 → 请求挂 meta['proxy']；硬封禁累计或
  连续空页（spider 调 ctx.degrade_proxy）达阈值 → 归还(ok=False) 切直连兜底
- AccountMiddleware：账号池租用走配额/健康度（lease 当前不返回凭证），
  身份=固定真实 UA + 随机 bid cookie；被封换号/换 bid 一次
"""

import random
import string

from scrapy import signals
from scrapy.exceptions import DownloadTimeoutError
from twisted.internet.error import (
    ConnectError,
    TCPTimedOutError,
    TimeoutError as TwistedTimeoutError,
)

from ..log import logger
from ..metrics import M
from ..session import lease_account, lease_proxy, release_account, release_proxy

# 封禁类状态码统一记 blocked_429（沿用规范枚举）
_BLOCK_STATUS = frozenset({403, 418, 429})

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
]


def _random_bid() -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=16))


def _direct_retry(request):
    """构造直连重抓请求：必须剥掉 meta 里的 proxy/via_proxy，否则仍走死代理。"""
    meta = {k: v for k, v in request.meta.items() if k not in ("proxy", "via_proxy")}
    meta.update({"tried_direct": True, "force_direct": True})
    return request.replace(meta=meta, dont_filter=True)


class MetricsMiddleware:
    """结果打点：request_total{result} / fail_total{error_type} / 耗时直方图；
    经代理的请求同时打 proxy_request_total{result,via_proxy}（代理成功率分子分母）。"""

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        mw._crawler = crawler
        return mw

    def process_response(self, request, response):
        spider = self._crawler.spider
        ok = response.status < 400
        error_type = ""
        if response.status in _BLOCK_STATUS:
            error_type = "blocked_429"
            spider.ctx.stats["blocked"] += 1
        elif response.status >= 500:
            error_type = "server_5xx"
            spider.ctx.stats["server"] += 1
        elif response.status >= 400:
            error_type = "other"
        M.request(ok=ok, error_type=error_type,
                  duration=request.meta.get("download_latency") or 0.0)
        if request.meta.get("via_proxy"):
            M.proxy_request(ok=ok, via_proxy=True)
        return response

    def process_exception(self, request, exception):
        spider = self._crawler.spider
        # Scrapy 会把底层超时包装成 DownloadTimeoutError（裸 Exception 子类，
        # 不继承 twisted/builtin TimeoutError），必须显式识别，否则误记 server_5xx
        if isinstance(exception, (TwistedTimeoutError, TCPTimedOutError, DownloadTimeoutError)):
            error_type, key = "timeout", "timeout"
        elif isinstance(exception, ConnectError):
            # 代理/目标连不通，按 BaseCrawler 口径计入 blocked
            error_type, key = "blocked_429", "blocked"
        else:
            error_type, key = "other", "server"
        spider.ctx.stats[key] += 1
        M.request(ok=False, error_type=error_type)
        if request.meta.get("via_proxy"):
            M.proxy_request(ok=False, via_proxy=True)
        return None  # 交给 RetryMiddleware（优先级更靠前）


class ProxyMiddleware:
    """代理路由与劣化降级（对齐 BaseCrawler：空页 ≥2 或封禁 ≥3 归还切直连）。"""

    EMPTY_DEGRADE_THRESHOLD = 2
    BLOCK_DEGRADE_THRESHOLD = 3
    FAIL_DEGRADE_THRESHOLD = 3  # 经代理的连接失败次数（代理连不通）

    def __init__(self):
        self._spider = None
        self._proxy = None
        self._direct = False
        self._proxy_fail = 0

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        mw._crawler = crawler
        crawler.signals.connect(mw._open, signal=signals.spider_opened)
        crawler.signals.connect(mw._close, signal=signals.spider_closed)
        return mw

    def _open(self, spider):
        self._spider = spider
        if spider.ctx.use_proxy:
            self._proxy = lease_proxy()
            if self._proxy:
                spider.ctx.proxy = self._proxy
                spider.ctx.stats["ip"] += 1
                M.proxy_request(ok=True, ip_consumed=1)
        spider.ctx.on_proxy_degraded = self._note_degraded
        logger.info("proxy middleware ready | mode={}",
                    f"{self._proxy['protocol']}://{self._proxy['host']}:{self._proxy['port']}"
                    if self._proxy else "direct")

    def process_request(self, request):
        if self._proxy and not self._direct and not request.meta.get("force_direct"):
            p = self._proxy
            request.meta["proxy"] = f"{p['protocol']}://{p['host']}:{p['port']}"
            request.meta["via_proxy"] = True

    def process_response(self, request, response):
        """经代理被硬封（403/418/429）：该页改直连重抓一次；失败计数达阈值整体切直连。"""
        if (response.status in _BLOCK_STATUS and request.meta.get("via_proxy")
                and not request.meta.get("tried_direct")):
            self._proxy_fail += 1
            if (self._proxy and not self._direct
                    and self._proxy_fail >= self.FAIL_DEGRADE_THRESHOLD):
                self._switch_direct(self._crawler.spider, f"proxy failures x{self._proxy_fail}")
            return _direct_retry(request)
        return response

    def process_exception(self, request, exception):
        """代理连不通：计失败次数；达阈值释放切直连，并把失败的页改直连重抓一次。"""
        if not request.meta.get("via_proxy") or request.meta.get("tried_direct"):
            return None
        self._proxy_fail += 1
        if self._proxy and not self._direct and self._proxy_fail >= self.FAIL_DEGRADE_THRESHOLD:
            self._switch_direct(self._crawler.spider, f"proxy failures x{self._proxy_fail}")
            return _direct_retry(request)
        return None

    def _note_degraded(self, reason: str) -> None:
        """spider 发现 200 但解析空（免费代理劫持垃圾页）时调用。"""
        ctx = self._spider.ctx
        ctx.stats["empty_pages"] += 1
        logger.warning("degraded page via proxy | count={} reason={}",
                       ctx.stats["empty_pages"], reason)
        if (self._proxy and not self._direct
                and ctx.stats["empty_pages"] >= self.EMPTY_DEGRADE_THRESHOLD):
            self._switch_direct(self._spider,
                                f"empty_pages>={self.EMPTY_DEGRADE_THRESHOLD}")

    def _switch_direct(self, spider, reason: str) -> None:
        self._direct = True
        M.proxy_request(ok=False, via_proxy=True)
        release_proxy(self._proxy, False)
        self._proxy = None
        spider.ctx.proxy = None
        logger.warning("proxy released, switching to direct | reason={}", reason)

    def _close(self, spider):
        if self._proxy:
            stats = spider.ctx.stats
            release_proxy(self._proxy, stats["blocked"] + stats["timeout"] == 0)
            self._proxy = None


class AccountMiddleware:
    """账号池接线：租用/归还真实现有配额与健康度；lease 当前不返回凭证，
    身份 = 每轮固定一个真实 UA + 随机 bid cookie（豆瓣匿名可抓）；
    被封时 release(ok=False) 换租一次并换 bid/UA；池空保持匿名。"""

    def __init__(self):
        self._spider = None
        self._account = None
        self._bid = _random_bid()
        self._ua = random.choice(USER_AGENTS)
        self._rotated = False

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls()
        mw._crawler = crawler
        crawler.signals.connect(mw._open, signal=signals.spider_opened)
        crawler.signals.connect(mw._close, signal=signals.spider_closed)
        return mw

    def _open(self, spider):
        if spider.ctx.use_account:
            self._account = lease_account(spider.ctx.account_platform)
            if self._account:
                M.account_request(ok=True, account=self._account.get("accountName", "unknown"))
        logger.info("account middleware ready | leased={} bid=***", bool(self._account))

    def process_request(self, request):
        request.headers.setdefault("User-Agent", self._ua)
        request.headers.setdefault("Cookie", f"bid={self._bid}")

    def process_response(self, request, response):
        if response.status in _BLOCK_STATUS and not self._rotated:
            self._rotated = True
            self._rotate(self._crawler.spider)
        return response

    def _rotate(self, spider) -> None:
        if self._account:
            M.account_request(ok=False, account=self._account.get("accountName", "unknown"))
            release_account(self._account, False)
            self._account = lease_account(spider.ctx.account_platform)
            if self._account:
                M.account_request(ok=True, account=self._account.get("accountName", "unknown"))
        self._bid = _random_bid()
        self._ua = random.choice(USER_AGENTS)
        logger.warning("identity rotated after ban | new_account={}", bool(self._account))

    def _close(self, spider):
        if self._account:
            release_account(self._account, spider.ctx.stats["blocked"] == 0)
            self._account = None
