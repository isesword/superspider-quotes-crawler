# -*- coding: utf-8 -*-
"""SuperSpider 真实爬虫：抓取 quotes.toscrape.com（公开爬虫练习站）并按打点规范上报。

- 分页遍历全部页面，解析名人名言（作者/正文/标签）
- 业务打点：请求成功/失败、解析入库条数、IP 消耗（经代理时）
- 收尾：POST /internal/runs/report 上报执行流水（含业务汇总）
- 环境变量由平台下发（SUPERSPIDER_*），详见 docs/worker-metrics-spec.md
"""

import datetime
import json
import os
import random
import time
import urllib.request

import requests
from bs4 import BeautifulSoup

from superspider_metrics import M

BACKEND = os.environ.get("SUPERSPIDER_BACKEND_URL", "http://host.docker.internal:8000")
BASE = "http://quotes.toscrape.com"
PAGE_DELAY = float(os.environ.get("CRAWL_PAGE_DELAY", "1.5"))  # 每页间隔，礼貌抓取
MAX_PAGES = int(os.environ.get("CRAWL_MAX_PAGES", "0"))        # 0=全部页


def now_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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
    try:
        req = urllib.request.Request(
            BACKEND + "/api/v1/internal/runs/report",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            print("[run-report]", resp.status, resp.read().decode()[:120])
    except Exception as exc:
        print("[run-report] failed:", exc)


def fetch(url: str, stats: dict) -> str | None:
    """请求页面：成功返回 HTML；失败按类型打点并返回 None"""
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "SuperSpider-Demo/0.1"})
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
    """解析名言并入库（入库 = M.crawl 计数）；校验失败计 store_fail"""
    soup = BeautifulSoup(html, "html.parser")
    stored = 0
    for item in soup.select("div.quote"):
        text = item.select_one("span.text")
        author = item.select_one("small.author")
        if not text or not author:
            stats["validation"] += 1
            M.request(ok=False, error_type="validation")
            continue
        # 入库（演示：打印摘要；真实场景写库/写队列）
        quote = text.get_text(strip=True)
        print(f"[store] {author.get_text(strip=True)}: {quote[:60]}...")
        stats["crawled"] += 1
        M.crawl(1)
        stored += 1
    return stored


def main() -> None:
    global start_ts
    start_ts = time.time()
    started = now_str()
    M.start()
    print(f"[crawler] start labels={M._labels} base={BASE}")

    stats = {"crawled": 0, "store_fail": 0, "timeout": 0, "blocked": 0, "server": 0, "validation": 0, "ip": 0}
    url, pages = BASE + "/", 0

    while url and (MAX_PAGES <= 0 or pages < MAX_PAGES):
        html = fetch(url, stats)
        if html is None:
            time.sleep(3)
            continue
        stored = parse_quotes(html, stats)
        stats["crawled"] += 0  # crawl 计数在 parse 内逐条 M.crawl
        pages += 1

        # 下一页
        nxt = BeautifulSoup(html, "html.parser").select_one("li.next > a")
        url = BASE + nxt["href"] if nxt else None

        # 模拟 IP 消耗（经代理出口时）
        if random.random() < 0.3:
            stats["ip"] += 1
            M.proxy_request(ok=True, ip_consumed=1)

        time.sleep(PAGE_DELAY)

    status = "success" if stats["timeout"] + stats["blocked"] + stats["server"] < 5 else "failed"
    err = "" if status == "success" else f"网络失败过多（timeout={stats['timeout']}, blocked={stats['blocked']}, 5xx={stats['server']}）"
    report_run(status, started, stats, err)
    print(f"[crawler] done pages={pages} stats={stats} status={status}")


if __name__ == "__main__":
    start_ts = time.time()
    main()
