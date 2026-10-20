#!/usr/bin/env python3
"""入口：根据 CRAWLER_NAME 环境变量选择具体爬虫类。"""
import os, sys
sys.path.insert(0, os.path.dirname(__file__))
from crawlers.quotes import QuotesCrawler

CRAWLERS = {"quotes": QuotesCrawler}

def main():
    name = os.environ.get("CRAWLER_NAME", "quotes")
    cls = CRAWLERS.get(name)
    if not cls:
        print(f"[main] unknown crawler {name!r}, available: {list(CRAWLERS)}")
        sys.exit(1)
    cls().run()

if __name__ == "__main__":
    main()
