# superspider-quotes-crawler

SuperSpider 平台的真实爬虫示例（git 代码拉取方案）：抓取 [quotes.toscrape.com](http://quotes.toscrape.com)（公开爬虫练习站，允许抓取），解析名人名言并按打点规范上报。

## 打点

依赖 `superspider_metrics.py`（平台下发 `SUPERSPIDER_*` 环境变量，worker 零配置上报 VM + 执行流水），规范见主仓库 `docs/worker-metrics-spec.md`。

## 本地运行

```bash
uv sync
SUPERSPIDER_TASK_ID=1 SUPERSPIDER_VM_URL=http://localhost:8428 uv run python main.py
```

## 平台接入

平台建任务：代码拉取方案 → 仓库地址/分支 → 安装命令 `uv sync --frozen` → 启动命令 `uv run python main.py`。
