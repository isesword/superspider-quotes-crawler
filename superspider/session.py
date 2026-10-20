# -*- coding: utf-8 -*-
"""会话管理：租账号/代理、代理路由、失败回退直连。"""

import os
import urllib.request
import json

_BACKEND = os.environ.get("SUPERSPIDER_BACKEND_URL", "http://host.docker.internal:8000")


def _post(path: str, payload: dict, timeout: int = 8) -> dict | None:
    """POST 并解包 {code,message,data} 信封：code!=0 返回 None。"""
    try:
        req = urllib.request.Request(
            _BACKEND + path,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            envelope = json.loads(resp.read().decode())
    except Exception as exc:
        print(f"[backend {path}] failed: {exc}")
        return None
    if envelope.get("code") != 0:
        print(f"[backend {path}] error: {str(envelope.get('message'))[:120]}")
        return None
    return envelope.get("data")


def lease_account(platform: str = "") -> dict | None:
    result = _post("/api/v1/internal/accounts/lease", {"platform": platform})
    if result and result.get("accountId"):
        print(f"[account] leased #{result['accountId']} {result['accountName']} ({result['platform']})")
        return result
    print(f"[account] lease failed/skipped: {result}")
    return None


def release_account(account: dict, ok: bool) -> None:
    if account:
        _post("/api/v1/internal/accounts/release", {"accountId": account["accountId"], "ok": ok})


def lease_proxy() -> dict | None:
    result = _post("/api/v1/internal/proxies/lease", {})
    if result and result.get("proxyId"):
        print(f"[proxy] leased #{result['proxyId']} {result['protocol']}://{result['host']}:{result['port']}")
        return result
    print(f"[proxy] lease failed/skipped -> direct")
    return None


def release_proxy(proxy: dict, ok: bool) -> None:
    if proxy:
        _post("/api/v1/internal/proxies/release", {"proxyId": proxy["proxyId"], "ok": ok})
