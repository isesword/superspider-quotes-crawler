#!/usr/bin/env python3
"""入口：根据 CRAWLER_NAME 环境变量选择具体爬虫类。

- 单次模式（默认）：跑一轮退出，cron/手动运行用。
- 长驻模式：CRAWL_LOOP=1 时每轮之间睡 CRAWL_LOOP_INTERVAL 秒（默认 30）。
  long 任务用 cmdOverride 启动：CRAWL_LOOP=1 python main.py。
  优雅停止由 BaseCrawler 的信号处理器负责（冲账/释放租约/上报 stopped 后退出），
  两轮之间则由这里的轻量 handler 直接退出（没有在途 run 需要收尾）。
- Scrapy 爬虫（scrapy.Spider 子类）走 run_scrapy()，但只支持单次模式
  （Twisted reactor 不可重启，CRAWL_LOOP 会降级为跑一次后退出）。
"""
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))
from crawlers.douban_top import DoubanTopSpider
from crawlers.quotes import QuotesCrawler
from superspider.log import logger, setup

setup()

try:
    from scrapy import Spider as ScrapySpider
except ImportError:  # 未装 scrapy 时 BaseCrawler 爬虫仍可跑
    ScrapySpider = None

CRAWLERS = {"quotes": QuotesCrawler, "douban_top": DoubanTopSpider}


def _is_scrapy(cls) -> bool:
    return ScrapySpider is not None and issubclass(cls, ScrapySpider)


def _run_once(cls) -> None:
    if _is_scrapy(cls):
        from superspider.scrapy.runner import run_scrapy
        run_scrapy(cls)
        return
    cls().run()


def main():
    name = os.environ.get("CRAWLER_NAME", "quotes")
    cls = CRAWLERS.get(name)
    if not cls:
        logger.error("unknown crawler {} available={}", name, list(CRAWLERS))
        sys.exit(1)

    if os.environ.get("CRAWL_LOOP", "0") != "1":
        _run_once(cls)
        return

    if _is_scrapy(cls):
        logger.error("CRAWL_LOOP=1 不支持 Scrapy 爬虫（reactor 不可重启），单次运行后退出")
        _run_once(cls)
        return

    interval = float(os.environ.get("CRAWL_LOOP_INTERVAL", "30"))
    between_passes = lambda *_: sys.exit(0)  # noqa: E731
    n = 0
    while True:
        n += 1
        logger.info("loop pass #{}", n)
        cls().run()
        # 两轮之间：信号直接退出（无在途 run）；run 内已重装 handler，这里换回轻量的
        signal.signal(signal.SIGTERM, between_passes)
        signal.signal(signal.SIGINT, between_passes)
        logger.info("sleep {}s before next pass", interval)
        time.sleep(interval)


if __name__ == "__main__":
    main()
