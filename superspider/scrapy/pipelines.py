# -*- coding: utf-8 -*-
"""CH 入库 pipeline：50 条批量写 crawl_data（同 BaseCrawler._store_batch），失败打 store_fail。"""

import datetime
import os

from ..log import logger
from ..metrics import M
from ..storage import batch_insert

BATCH_SIZE = 50


class ClickHousePipeline:
    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls()
        pipeline._crawler = crawler
        return pipeline

    def open_spider(self):
        self._buf: list[dict] = []
        self._written = 0

    def process_item(self, item):
        self._buf.append(dict(item))
        if len(self._buf) >= BATCH_SIZE:
            self._flush()
        return item

    def close_spider(self):
        self._flush()
        logger.info("pipeline closed | total_written={}", self._written)

    def _flush(self) -> None:
        if not self._buf:
            return
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []
        for item in self._buf:
            row = {
                "task_id": int(os.environ.get("SUPERSPIDER_TASK_ID", "0") or 0),
                "project": os.environ.get("SUPERSPIDER_PROJECT", ""),
                "crawled_at": now,
            }
            row.update(item)
            rows.append(row)
        if batch_insert("crawl_data", rows):
            self._written += len(rows)
            logger.info("ch batch inserted | count={}", len(rows))
        else:
            self._crawler.spider.ctx.stats["store_fail"] += len(rows)
            M.store_fail(len(rows))
            logger.error("ch batch insert failed | count={}", len(rows))
        self._buf = []
