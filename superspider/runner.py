# -*- coding: utf-8 -*-
"""运行入口：加载 crawler 类 → run()"""
def run_crawler(crawler_cls):
    crawler = crawler_cls()
    crawler.run()
