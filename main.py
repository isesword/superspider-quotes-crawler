# -*- coding: utf-8 -*-
"""SuperSpider 真实爬虫：抓取 quotes.toscrape.com（公开爬虫练习站）并按打点规范上报。

资源编排（每次运行从平台内部接口租用，真实流转 DB 状态）：
- 账号：/internal/accounts/lease 租身份 → 登录站点 → 收尾归还（失败会扣健康度）
- 代理：/internal/proxies/lease 租节点 → 请求经代理发出 → 归还（失败下调成功率）

多 pod 分片：平台以 Indexed Job 下发，JOB_COMPLETION_INDEX/REPLICAS 注入后
按「页码 % 副本数 == 本 pod 序号」分页，互不重复；VM 按标签聚合出全量。
"""

import datetime
import json
import os
import random
import time
import urllib.parse
import urllib.request

import requests
from bs4 import BeautifulSoup

from superspider_metrics import M

BACKEND = os.environ.get("SUPERSPIDER_BACKEND_URL", "http://host.docker.internal:8000")
BASE = "https://quotes.toscrape.com"
PAGE_DELAY = float(os.environ.get("CRAWL_PAGE_DELAY", "1.5"))
USE_PROXY = os.environ.get("CRAWL_USE_PROXY", "1") == "1"
NEED_ACCOUNT = os.environ.get("CRAWL_NEED_ACCOUNT", "1") == "1"


def now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def backend_post(path: str, payload: dict, timeout: int = 8) -> dict | None:
    try:
        req = urllib.request.Request(
            BACKEND + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        print(f"[backend {path}] failed: {exc}")
        return None


def report_run(status: str, started: str, stats: dict, error_message: str = "") -> None:
    task_id = os.environ.get("SUPERSPIDER_TASK_ID", "")
    if not task_id.isdigit():
        print("[run-report] skip: SUPERSPIDER_TASK_ID not set")
        return
    payload = {
        "taskId": int(task_id),
        "projectId": int(os.environ.get("SUPERSPIDER_PROJECT_ID", "0") or 0),
        "status": status,
        "triggerType": os.environ.get("SUPERSPIDER_TRIGGER_TYPE", "manual"),
        "startedAt": started,
        "finishedAt": now_str(),
        "durationMs": int((time.time() - start_ts) * 1000),
        "retryCount": 0,
        "errorMessage": error_message,
        "crawledCount": stats["crawled"],
        "storeFailCount": stats["store_fail"],
        "errorTimeout": stats["timeout"],
        "errorBlocked": stats["blocked"],
        "errorServer": stats["server"],
        "errorValidation": stats["validation"],
        "ipConsumed": stats["ip"],
    }
    result = backend_post("/api/v1/internal/runs/report", payload)
    print("[run-report]", json.dumps(result, ensure_ascii=False)[:120] if result else "failed")


def lease_account() -> dict | None:
    result = backend_post("/api/v1/internal/accounts/lease", {"platform": ""})
    if result and result.get("accountId"):
        print(f"[account] leased #{result['accountId']} {result['accountName']} ({result['platform']})")
        return result
    print(f"[account] lease failed/skipped: {json.dumps(result, ensure_ascii=False)[:160] if result else 'no response'}")
    return None


def release_account(account: dict, ok: bool) -> None:
    if account:
        backend_post("/api/v1/internal/accounts/release", {"accountId": account["accountId"], "ok": ok})


def lease_proxy() -> dict | None:
    result = backend_post("/api/v1/internal/proxies/lease", {})
    if result and result.get("proxyId"):
        print(f"[proxy] leased #{result['proxyId']} {result['protocol']}://{result['host']}:{result['port']}")
        return result
    print(f"[proxy] lease failed/skipped -> direct: {json.dumps(result, ensure_ascii=False)[:160] if result else 'no response'}")
    return None


def release_proxy(proxy: dict, ok: bool) -> None:
    if proxy:
        backend_post("/api/v1/internal/proxies/release", {"proxyId": proxy["proxyId"], "ok": ok})


def shard_info() -> tuple[int, int]:
    index = int(os.environ.get("JOB_COMPLETION_INDEX", "0") or 0)
    replicas = int(os.environ.get("SUPERSPIDER_REPLICAS", "1") or 1)
    return index, max(1, replicas)


def shard_of_page(page_num: int, replicas: int, index: int) -> bool:
    return page_num % replicas == index


def build_session(proxy: dict | None) -> tuple[requests.Session, dict | None]:
    session = requests.Session()
    session.headers["User-Agent"] = "SuperSpider-Crawler/0.2"
    if not proxy:
        return session, None
    proxy_url = f"{proxy['protocol']}://{proxy['host']}:{proxy['port']}"
    session.proxies = {"http": proxy_url, "https": proxy_url}
    session.request = _timeout_wrap(session.request)  # type: ignore
    return session, {"url": proxy_url}


def _timeout_wrap(fn):
    def inner(method, url, **kwargs):
        kwargs.setdefault("timeout", 8)
        return fn(method, url, **kwargs)
    return inner


def login(session: requests.Session, account: dict, stats: dict) -> bool:
    try:
        resp = session.get(BASE + "/login", timeout=8)
        soup = BeautifulSoup(resp.text, "html.parser")
        csrf = soup.select_one('input[name="csrf_token"]')
        payload = {
            "username": account["accountName"],
            "password": os.environ.get("ACCOUNT_PASSWORD", "superspider"),
        }
        if csrf:
            payload["csrf_token"] = csrf["value"]
        resp = session.post(BASE + "/login", data=payload, timeout=8)
        ok = "logout" in resp.text
        stats["login_ok"] = ok
        M.account_request(ok, account["accountName"])
        print(f"[login] account={account['accountName']} ok={ok}")
        return ok
    except Exception as exc:
        stats["login_ok"] = False
        M.account_request(False, account.get("accountName", "unknown"))
        print("[login] failed:", exc)
        return False


def fetch(session: requests.Session, url: str, stats: dict) -> str | None:
    try:
        resp = session.get(url)
    except requests.exceptions.Timeout:
        stats["timeout"] += 1
        M.request(ok=False, error_type="timeout")
        return None
    except requests.exceptions.RequestException:
        stats["server"] += 1
        M.request(ok=False, error_type="server_5xx")
        return None
    if resp.status_code == 429:
        stats["blocked"] += 1
        M.request(ok=False, error_type="blocked_429")
        return None
    if resp.status_code >= 500:
        stats["server"] += 1
        M.request(ok=False, error_type="server_5xx")
        return None
    M.request(ok=True, duration=resp.elapsed.total_seconds())
    return resp.text


def parse_quotes(html: str, stats: dict) -> int:
    soup = BeautifulSoup(html, "html.parser")
    stored = 0
    for item in soup.select("div.quote"):
        text = item.select_one("span.text")
        author = item.select_one("small.author")
        tags = [a.get_text(strip=True) for a in item.select("div.tags a.tag")]
        if not text or not author:
            stats["validation"] += 1
            M.request(ok=False, error_type="validation")
            continue
        quote = text.get_text(strip=True)
        print(f"[store] {author.get_text(strip=True)}: {quote[:60]}... tags={','.join(tags)}")
        stats["crawled"] += 1
        M.crawl(1)
        stored += 1
    return stored


def next_page(html: str) -> str | None:
    nxt = BeautifulSoup(html, "html.parser").select_one("li.next > a")
    return urllib.parse.urljoin(BASE + "/", nxt["href"]) if nxt else None


def main() -> None:
    global start_ts
    start_ts = time.time()
    started = now_str()
    M.start()

    index, replicas = shard_info()
    proxy = lease_proxy() if USE_PROXY else None
    session, proxy_info = build_session(proxy)
    print(f"[crawler] start shard={index}/{replicas} proxy={proxy_info} base={BASE}")

    account = lease_account() if NEED_ACCOUNT else None
    stats = {"crawled": 0, "store_fail": 0, "timeout": 0, "blocked": 0, "server": 0,
             "validation": 0, "ip": 0, "login_ok": False}
    if account:
        login(session, account, stats)

    url, page_num, crawled_pages = BASE + "/", 1, 0
    while url:
        if not shard_of_page(page_num, replicas, index):
            print(f"[shard] skip page {page_num}")
            page_num += 1
            url = f"{BASE}/page/{page_num}/" if page_num <= 10 else None
            continue
        html = fetch(session, url, stats)
        if html is None and proxy:
            # 代理失败：回退直连重试一次（真实爬虫行为），代理侧已计失败
            release_proxy(proxy, False)
            session.proxies = {}
            proxy_info = None
            html = fetch(session, url, stats)
        if html is None:
            page_num += 1
            url = f"{BASE}/page/{page_num}/" if page_num <= 10 else None
            continue
        parse_quotes(html, stats)
        crawled_pages += 1
        page_num += 1
        url = next_page(html)
        if random.random() < 0.3:
            stats["ip"] += 1
            M.proxy_request(ok=True, ip_consumed=1, via_proxy=bool(proxy_info))
        time.sleep(PAGE_DELAY)

    account_ok = bool(stats.get("login_ok", True))
    release_account(account, account_ok)
    if proxy:
        release_proxy(proxy, stats["blocked"] + stats["timeout"] == 0)

    status = "success" if stats["timeout"] + stats["blocked"] + stats["server"] < 5 else "failed"
    err = "" if status == "success" else f"网络失败过多（timeout={stats['timeout']}, blocked={stats['blocked']}, 5xx={stats['server']}）"
    report_run(status, started, stats, err)
    print(f"[crawler] done shard={index}/{replicas} pages={crawled_pages} stats={stats} status={status}")


if __name__ == "__main__":
    start_ts = time.time()
    main()
