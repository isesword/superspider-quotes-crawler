# SuperSpider Crawler Runtime

SuperSpider 平台的爬虫运行时。一个仓库承载全部爬虫，由任务配置（`CRAWLER_NAME`）选择运行目标。

## 结构

```
main.py                 入口：按 CRAWLER_NAME 分发（BaseCrawler 走 run()，Scrapy 走 run_scrapy()）
superspider/            SDK
├── base.py             BaseCrawler：targets/parse/store 同步基类（分片/代理降级/账号/打点/入库/优雅停止）
├── scrapy/             Scrapy 桥接层（代理/账号/指标中间件 + CH pipeline + runner）
├── session.py          代理/账号 租用与归还（后端 internal API）
├── metrics.py          业务指标打点（30s 推 VictoriaMetrics）
├── storage.py          CH 批量入库 + run 上报
└── log.py              loguru 配置
crawlers/               具体爬虫
├── quotes.py           名言采集（quotes.toscrape.com，BaseCrawler）
└── douban_top.py       豆瓣电影 Top100（Scrapy）
```

## 两种接入方式

- **git 方案**：Pod init 容器克隆本仓库 → `uv sync` → `uv run python main.py`
- **image 方案**：Dockerfile 构建镜像（uv 装好依赖）后部署

## 关键环境变量

| 变量 | 默认 | 说明 |
|---|---|---|
| `CRAWLER_NAME` | quotes | 选择爬虫 |
| `CRAWL_USE_PROXY` | 1 | 是否租用代理 |
| `CRAWL_NEED_ACCOUNT` | 0 | 是否租用账号（lease 无凭证时匿名降级） |
| `CRAWL_PAGE_DELAY` | 1.5 | 页间延迟（秒） |
| `DOUBAN_TOP_N` | 100 | 豆瓣 TopN 条数 |

`SUPERSPIDER_*`（TASK_ID/PROJECT/BIZ/VM_URL 等）由平台下发时注入。
Scrapy 爬虫仅支持单次模式（Twisted reactor 不可重启）。
