# -*- coding: utf-8 -*-
"""统一日志：loguru → stdout 扁平 JSON 单行。

- 集群（默认/有 SUPERSPIDER_TASK_ID）：扁平 JSON，LoongCollector processor_json
  解析成列直发 CH task_log；列名即此处的键名。
- 本地裸跑（无 TASK_ID 或 CRAWL_LOG_PRETTY=1）：彩色人读格式。
- 6 个标签维度（task_id/project/biz/task_type/scheme/pod）由环境自动注入每条日志，
  与 VM 指标共用同一标签体系。
"""

import json
import logging
import os
import sys
import threading

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
    """扁平 JSON sink：json.dumps 保证转义正确（消息含引号也安全）。
    异常记录把完整堆栈文本并入 message（多行 → \n 转义，抽屉可全文检索）。"""
    r = message.record
    text = r["message"]
    if r["exception"]:
        import traceback

        text += "\n" + "".join(traceback.format_exception(*r["exception"])).rstrip()
    row = {
        "ts": r["time"].strftime("%Y-%m-%d %H:%M:%S"),
        "level": r["level"].name,
        "message": text,
        **_BASE_COLS,
    }
    for k, v in r["extra"].items():
        if k not in _BASE_KEYS and v not in (None, ""):
            row[k] = v
    print(json.dumps(row, ensure_ascii=False, default=str), flush=True)


class InterceptHandler(logging.Handler):
    """标准 logging → loguru 桥：Scrapy/Twisted 等库日志统一进 JSON 采集管道。"""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        logger.opt(depth=6, exception=record.exc_info).log(level, record.getMessage())


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
    # 标准 logging（scrapy/twisted/urllib3...）桥进 loguru：库日志统一 JSON 采集
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)

    # 未捕获异常（解释器直接打 stderr 的 traceback，绕过一切日志框架）→
    # loguru ERROR + 完整堆栈——硬崩溃也以结构化错误行进入采集管道。
    # import 期错误能否被接住取决于 setup() 先于业务 import 执行（见 main.py）。
    def _uncaught(exc_type, exc_value, exc_tb):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_tb)
            return
        logger.opt(exception=(exc_type, exc_value, exc_tb)).error("Unhandled exception")

    sys.excepthook = _uncaught
    if hasattr(threading, "excepthook"):

        def _thread_uncaught(args):
            _uncaught(args.exc_type, args.exc_value, args.exc_traceback)

        threading.excepthook = _thread_uncaught
