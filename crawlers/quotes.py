# -*- coding: utf-8 -*-
"""quotes.toscrape.com 名言爬虫（合法练习站，10 页固定分页）。"""

import urllib.parse

from bs4 import BeautifulSoup
from superspider.base import BaseCrawler
from superspider.metrics import M
from superspider.storage import batch_insert

BASE = "http://quotes.toscrape.com"
TOTAL_PAGES = 10


class QuotesCrawler(BaseCrawler):
    def targets(self) -> list[str]:
        return [f"{BASE}/page/{i}/" for i in range(1, TOTAL_PAGES + 1)]

    def parse(self, html: str, url: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for q in soup.select("div.quote"):
            text_el = q.select_one("span.text")
            author_el = q.select_one("small.author")
            if not text_el or not author_el:
                self.stats["validation"] += 1
                M.request(ok=False, error_type="validation")
                continue
            tags = [a.get_text(strip=True) for a in q.select("div.tags a.tag")]
            items.append({
                "url": url,
                "title": author_el.get_text(strip=True),
                "content": text_el.get_text(strip=True),
                "tags": ",".join(tags),
            })
        return items

    def store(self, items: list[dict]) -> int:
        return len(items) if self._store_batch(items) else 0
