# -*- coding: utf-8 -*-
"""豆瓣电影 Top100（top250 榜单前 N 条，默认 100）Scrapy 爬虫。

- 分片：JOB_COMPLETION_INDEX % SUPERSPIDER_REPLICAS（页级，同 BaseCrawler 约定）
- 劣化处理：列表页 200 但解析空（免费代理劫持垃圾页）→ ctx.degrade_proxy 打点
  并 force_direct 重抓一次；直连仍空才计 validation
- 字段落 crawl_data 窄表：title=排名+片名，content=结构化 JSON，tags=类型
"""

import json
import os

import scrapy

from superspider.log import logger
from superspider.metrics import M
from superspider.scrapy.runner import CrawlContext

BASE = "https://movie.douban.com/top250"
PAGE_SIZE = 25


class DoubanTopSpider(scrapy.Spider):
    name = "douban_top"
    allowed_domains = ["movie.douban.com"]

    def __init__(self, top_n=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ctx = CrawlContext()
        self.ctx.account_platform = "douban"
        self.top_n = int(top_n or os.environ.get("DOUBAN_TOP_N", "100"))

    async def start(self):
        """Scrapy ≥2.13 新式入口；start_requests 保留兼容旧版。"""
        for request in self._list_requests():
            yield request

    def start_requests(self):
        yield from self._list_requests()

    def _list_requests(self):
        pages = (self.top_n + PAGE_SIZE - 1) // PAGE_SIZE
        mine = [i for i in range(pages) if i % self.ctx.shard_replicas == self.ctx.shard_index]
        logger.info("douban targets | top_n={} pages={} mine={} shard={}/{}",
                    self.top_n, pages, mine, self.ctx.shard_index, self.ctx.shard_replicas)
        for i in mine:
            yield scrapy.Request(
                f"{BASE}?start={i * PAGE_SIZE}",
                callback=self.parse,
                meta={"page_kind": "list", "page_index": i},
            )

    def parse(self, response):
        rows = response.css("div.item")
        if not rows:
            if response.meta.get("via_proxy") and not response.meta.get("tried_direct"):
                self.ctx.degrade_proxy(f"empty list page | url={response.url}")
                # 剥掉 proxy/via_proxy 再重抓，否则仍走劣化代理
                meta = {k: v for k, v in response.meta.items() if k not in ("proxy", "via_proxy")}
                meta.update({"tried_direct": True, "force_direct": True})
                yield response.request.replace(meta=meta, dont_filter=True)
                return
            self.ctx.stats["validation"] += 1
            M.request(ok=False, error_type="validation")
            self.logger.error("empty list page even direct | url=%s", response.url)
            return

        self.ctx.stats["pages"] += 1
        for row in rows:
            rank_text = row.css("div.pic em::text").get(default="").strip()
            if not rank_text.isdigit():
                continue
            rank = int(rank_text)
            if rank > self.top_n:
                continue

            titles = [t.strip() for t in row.css("span.title::text").getall()]
            title = titles[0] if titles else ""
            original = titles[1].split("/")[-1].strip() if len(titles) > 1 else ""

            rating = row.css("span.rating_num::text").get(default="").strip()
            votes = 0
            for t in row.css("div.star span::text").getall():
                if "人评价" in t:
                    digits = t.replace("人评价", "").strip()
                    if digits.isdigit():
                        votes = int(digits)
                    break
            quote = row.css("span.inq::text").get(default="").strip()
            poster = row.css("div.pic img::attr(src)").get(default="")
            detail = row.css("div.hd a::attr(href)").get(default="")

            year = country = genres = ""
            director = ""
            bd_lines = [t.strip() for t in row.css("div.bd p::text").getall() if t.strip()]
            for idx, line in enumerate(bd_lines):
                if idx == 0:
                    director = line
                if len(line) >= 4 and line[:4].isdigit():
                    parts = [p.strip() for p in line.split("/") if p.strip()]
                    year = parts[0] if parts else ""
                    country = parts[1] if len(parts) > 1 else ""
                    genres = "/".join(parts[2:])
                    break

            genre_tags = [g for g in genres.replace("/", " ").split() if g]
            yield {
                "url": detail,
                "title": f"[{rank}] {title}({year})" if year else f"[{rank}] {title}",
                "content": json.dumps({
                    "rank": rank, "title": title, "original_title": original,
                    "year": year, "country": country, "genres": genres,
                    "rating": rating, "votes": votes, "quote": quote,
                    "poster": poster, "director": director,
                }, ensure_ascii=False),
                "tags": ",".join(["douban", "movie", f"top{self.top_n}", *genre_tags]),
            }
