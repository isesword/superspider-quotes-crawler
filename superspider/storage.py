# -*- coding: utf-8 -*-
"""CH 批量入库（HTTP 接口 JSONEachRow）+ run 上报。"""

import json
import os
import urllib.request

_CH_URL = os.environ.get("SUPERSPIDER_CH_URL", "http://host.docker.internal:8123")
_CH_USER = os.environ.get("SUPERSPIDER_CH_USER", "default")
_CH_PASSWORD = os.environ.get("SUPERSPIDER_CH_PASSWORD", "123456")
_CH_DB = os.environ.get("SUPERSPIDER_CH_DB", "super_spider")
_BACKEND = os.environ.get("SUPERSPIDER_BACKEND_URL", "http://host.docker.internal:8000")


def batch_insert(table: str, rows: list[dict]) -> bool:
    """批量写入 CH 表（JSONEachRow），失败静默重试 1 次。"""
    if not rows:
        return True
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    auth = f"user={urllib.request.quote(_CH_USER)}&password={urllib.request.quote(_CH_PASSWORD)}&"
    url = f"{_CH_URL}/?{auth}query={urllib.request.quote('INSERT INTO ' + _CH_DB + '.' + table + ' FORMAT JSONEachRow')}"
    for _ in range(2):
        try:
            req = urllib.request.Request(url, data=body.encode("utf-8"),
                                         headers={"Content-Type": "text/plain"}, method="POST")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status < 300:
                    return True
        except Exception as exc:
            print(f"[ch-insert {table}] failed: {exc}")
    return False


def report_run(payload: dict) -> dict | None:
    try:
        req = urllib.request.Request(
            _BACKEND + "/api/v1/internal/runs/report",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        print(f"[run-report] failed: {exc}")
        return None
