# -*- coding: utf-8 -*-
"""统一日志：loguru → stdout 扁平 JSON 单行。

- 集群（默认/有 SUPERSPIDER_TASK_ID）：扁平 JSON，LoongCollector processor_json
  解析成列直发 CH task_log；列名即此处的键名。
- 本地裸跑（无 TASK_ID 或 CRAWL_LOG_PRETTY=1）：彩色人读格式。
- 6 个标签维度（task_id/project/biz/task_type/scheme/pod）由环境自动注入每条日志，
  与 VM 指标共用同一标签体系。
"""

import json
import os
import sys

from loguru import logger

# 基础维度列（logger 内部字段不得覆盖）
_BASE_KEYS = {"ts", "level", "message"}
_BASE_COLS: dict = {}  # setup() 时按环境一次性计算


def _base_cols() -> dict:
    return {
        "task_id": int(os.environ.get("SUPERSPIDER_TASK_ID", "0") or 0),
        "project": os.environ.get("SUPERSPIDER_PROJECT", ""),
        "biz": os.environ.get("SUPERSPIDER_BIZ", ""),
        "task_type": os.environ.get("SUPERSPIDER_TASK_TYPE", ""),
        "scheme": os.environ.get("SUPERSPIDER_SCHEME", ""),
        "pod": os.environ.get("SUPERSPIDER_POD_NAME", ""),
    }


def _json_sink(message) -> None:
    """扁平 JSON sink：json.dumps 保证转义正确（消息含引号也安全）。"""
    r = message.record
    row = {
        "ts": r["time"].strftime("%Y-%m-%d %H:%M:%S"),
        "level": r["level"].name,
        "message": r["message"],
        **_BASE_COLS,
    }
    for k, v in r["extra"].items():
        if k not in _BASE_KEYS and v not in (None, ""):
            row[k] = v
    print(json.dumps(row, ensure_ascii=False, default=str), flush=True)


_PRETTY_FORMAT = (
    "<green>{time:HH:mm:ss}</green> | "
    "<level>{level: <7}</level> | "
    "<level>{message}</level> {extra}"
)


def setup() -> None:
    """初始化日志（幂等）。集群=JSON stdout；本地（无 TASK_ID 或显式 PRETTY）=彩色人读。"""
    global _BASE_COLS
    _BASE_COLS = _base_cols()
    logger.remove()
    pretty = (
        os.environ.get("CRAWL_LOG_PRETTY") == "1"
        or not os.environ.get("SUPERSPIDER_TASK_ID")
    )
    level = os.environ.get("CRAWL_LOG_LEVEL", "INFO")
    if pretty:
        logger.add(sys.stdout, level=level, format=_PRETTY_FORMAT, colorize=None)
    else:
        logger.add(_json_sink, level=level)
    # loguru 默认捕获未处理异常，保留默认行为即可
