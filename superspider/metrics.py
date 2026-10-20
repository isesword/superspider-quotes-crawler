# -*- coding: utf-8 -*-
"""SuperSpider 爬虫指标上报（零依赖，stdlib only）。

- counter 累计值每 30s 推送 VictoriaMetrics（/api/v1/import/prometheus），收尾再推最终值
- 标签从 SUPERSPIDER_* 环境变量读取（K8s 下发时注入），pod 取 HOSTNAME
- 所有上报失败静默（最多重试 1 次），绝不阻塞业务
"""

import atexit
import os
import socket
import threading
import time
import urllib.request

_PUSH_INTERVAL_S = 30
_VM_PATH = "/api/v1/import/prometheus"


def _env(key: str, default: str = "unknown") -> str:
    value = os.environ.get(key, "").strip()
    return value if value else default


def _sanitize(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict = {}
        self._durations: list = []
        self._vm_url = _env("SUPERSPIDER_VM_URL", "http://victoriametrics.monitoring.svc:8428")
        self._labels = {
            "task_id": _env("SUPERSPIDER_TASK_ID"),
            "project": _env("SUPERSPIDER_PROJECT"),
            "biz": _env("SUPERSPIDER_BIZ"),
            "task_type": _env("SUPERSPIDER_TASK_TYPE"),
            "scheme": _env("SUPERSPIDER_SCHEME"),
            "pod": _env("HOSTNAME", socket.gethostname()),
        }
        self._thread = None
        self._stop = threading.Event()
        atexit.register(self.close)

    # 业务打点
    def request(self, ok: bool, error_type: str = "", duration: float = 0.0) -> None:
        with self._lock:
            self._inc("superspider_request_total", {"result": "success" if ok else "fail"})
            if not ok:
                self._inc("superspider_request_fail_total", {"error_type": error_type or "other"})
            if duration > 0:
                self._durations.append(duration)

    def crawl(self, count: int = 1) -> None:
        with self._lock:
            self._inc("superspider_crawled_total", {}, count)

    def store_fail(self, count: int = 1) -> None:
        with self._lock:
            self._inc("superspider_store_fail_total", {}, count)

    def proxy_request(self, ok: bool, ip_consumed: int = 0, via_proxy: bool = False) -> None:
        with self._lock:
            labels = {"result": "success" if ok else "fail"}
            if via_proxy:
                labels["via_proxy"] = "true"
            self._inc("superspider_proxy_request_total", labels)
            if ip_consumed > 0:
                self._inc("superspider_proxy_ip_consumed_total", {}, ip_consumed)

    def account_request(self, ok: bool, account: str) -> None:
        with self._lock:
            self._inc("superspider_account_request_total", {"account": account, "result": "success" if ok else "fail"})

    # 后台推送
    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        self.push()

    def _loop(self) -> None:
        while not self._stop.wait(_PUSH_INTERVAL_S):
            self.push()

    def _inc(self, name: str, labels: dict, value: int = 1) -> None:
        key = (name, frozenset(labels.items()))
        self._counters[key] = self._counters.get(key, 0) + value

    def render(self) -> str:
        lines = []
        base = ",".join(f'{k}="{_sanitize(v)}"' for k, v in sorted(self._labels.items()))
        with self._lock:
            for (name, label_items), value in sorted(self._counters.items()):
                labels = base if not label_items else base + "," + ",".join(
                    f'{k}="{_sanitize(v)}"' for k, v in sorted(label_items)
                )
                lines.append(f"{name}{{{labels}}} {value}")
            if self._durations:
                buckets = [0.5, 1, 3, 5, 10]
                total = sum(self._durations)
                for bound in buckets:
                    count = sum(1 for d in self._durations if d <= bound)
                    lines.append(
                        f'superspider_request_duration_seconds_bucket{{{base},le="{bound}"}} {count}'
                    )
                lines.append(
                    f'superspider_request_duration_seconds_bucket{{{base},le="+Inf"}} {len(self._durations)}'
                )
                lines.append(f"superspider_request_duration_seconds_sum{{{base}}} {total:.3f}")
                lines.append(f"superspider_request_duration_seconds_count{{{base}}} {len(self._durations)}")
        return "\n".join(lines) + "\n"

    def push(self) -> bool:
        body = self.render()
        if not body.strip():
            return True
        for _ in range(2):
            try:
                req = urllib.request.Request(
                    self._vm_url + _VM_PATH,
                    data=body.encode("utf-8"),
                    headers={"Content-Type": "text/plain"},
                    method="POST",
                )
                with urllib.request.urlopen(req, timeout=5) as resp:
                    if resp.status < 300:
                        return True
            except Exception:
                time.sleep(0.5)
        return False


M = Metrics()
