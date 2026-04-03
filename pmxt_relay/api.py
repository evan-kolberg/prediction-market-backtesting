from __future__ import annotations

import asyncio
from datetime import datetime
from datetime import timezone
import re
import time
from collections import defaultdict, deque
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
from xml.sax.saxutils import escape

from aiohttp import web

from pmxt_relay.config import RelayConfig
from pmxt_relay.filtered_store import FilteredHourStore
from pmxt_relay.filtered_store import create_filtered_hour_store
from pmxt_relay.index_db import PrebuildProgress
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.storage import processed_relative_path

_CONDITION_ID_RE = re.compile(r"^0x[a-f0-9]{64}$", re.IGNORECASE)
_TOKEN_ID_RE = re.compile(r"^\d+$")
_FILTERED_FILENAME_RE = re.compile(
    r"^polymarket_orderbook_\d{4}-\d{2}-\d{2}T\d{2}\.parquet$"
)
_RAW_FILENAME_RE = re.compile(
    r"^\d{4}/\d{2}/\d{2}/polymarket_orderbook_\d{4}-\d{2}-\d{2}T\d{2}\.parquet$"
)
_CACHE_CONTROL_FILE = "public, max-age=31536000, immutable"
_CACHE_CONTROL_JSON = "no-store"
_SECURITY_HEADERS = {
    "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}
_BADGE_SCHEMA_VERSION = 1
_BADGE_LABEL_COLOR = "#555"
_BADGE_COLOR_HEX = {
    "blue": "#007ec6",
    "brightgreen": "#4c1",
    "green": "#97ca00",
    "lightgrey": "#9f9f9f",
    "orange": "#fe7d37",
    "red": "#e05d44",
    "yellow": "#dfb317",
    "yellowgreen": "#a4a61d",
}
_SYSTEM_METRICS_CACHE_TTL_SECS = 2.0
_SYSTEM_METRICS_SAMPLE_SECS = 0.2
_SYSTEM_SERVICE_SPECS = {
    "api": ("pmxt-relay-api.service", "API service"),
    "worker": ("pmxt-relay-worker.service", "Worker service"),
    "clickhouse": ("clickhouse-server.service", "ClickHouse"),
}
_SYSTEM_METRICS_CACHE_LOCK = threading.Lock()
_SYSTEM_METRICS_CACHE: dict[str, object] | None = None
_SYSTEM_METRICS_CACHE_AT = 0.0


def _badge_color_hex(color: str) -> str:
    return _BADGE_COLOR_HEX.get(color, color)


def _badge_text_width(text: str) -> int:
    return max(12, (len(text) * 7) + 10)


def _badge_svg(payload: dict[str, object]) -> str:
    label = escape(str(payload["label"]))
    message = escape(str(payload["message"]))
    label_width = _badge_text_width(label)
    message_width = _badge_text_width(message)
    total_width = label_width + message_width
    label_center = label_width / 2
    message_center = label_width + (message_width / 2)
    message_color = _badge_color_hex(str(payload["color"]))
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{total_width}" height="20" role="img" '
        f'aria-label="{label}: {message}">'
        '<linearGradient id="s" x2="0" y2="100%">'
        '<stop offset="0" stop-color="#fff" stop-opacity=".7"/>'
        '<stop offset=".1" stop-color="#aaa" stop-opacity=".1"/>'
        '<stop offset=".9" stop-opacity=".3"/>'
        '<stop offset="1" stop-opacity=".5"/>'
        "</linearGradient>"
        '<clipPath id="r"><rect width="100%" height="20" rx="3" fill="#fff"/></clipPath>'
        '<g clip-path="url(#r)">'
        f'<rect width="{label_width}" height="20" fill="{_BADGE_LABEL_COLOR}"/>'
        f'<rect x="{label_width}" width="{message_width}" height="20" fill="{message_color}"/>'
        f'<rect width="{total_width}" height="20" fill="url(#s)"/>'
        "</g>"
        '<g fill="#fff" text-anchor="middle" '
        'font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,Arial,sans-serif" '
        'font-size="11">'
        f'<text x="{label_center}" y="15" fill="#010101" fill-opacity=".3">{label}</text>'
        f'<text x="{label_center}" y="14">{label}</text>'
        f'<text x="{message_center}" y="15" fill="#010101" fill-opacity=".3">{message}</text>'
        f'<text x="{message_center}" y="14">{message}</text>'
        "</g>"
        "</svg>"
    )


def _badge_svg_response(payload: dict[str, object]) -> web.Response:
    return web.Response(
        text=_badge_svg(payload),
        content_type="image/svg+xml",
        headers={"Cache-Control": "no-store"},
    )


def _usage_color(percent: float) -> str:
    if percent >= 90.0:
        return "red"
    if percent >= 75.0:
        return "orange"
    if percent >= 50.0:
        return "yellow"
    return "brightgreen"


def _cpu_percent_from_loadavg() -> float:
    cpu_count = os.cpu_count() or 1
    load_1min = os.getloadavg()[0]
    return max(0.0, min(100.0, (load_1min / cpu_count) * 100.0))


def _memory_percent() -> float:
    values: dict[str, int] = {}
    with Path("/proc/meminfo").open() as handle:
        for line in handle:
            key, raw = line.split(":", 1)
            values[key] = int(raw.strip().split()[0])
    total = max(1, values["MemTotal"])
    available = values.get("MemAvailable", values.get("MemFree", 0))
    used = max(0, total - available)
    return (used / total) * 100.0


def _disk_percent(path: Path) -> float:
    usage = shutil.disk_usage(path)
    return (usage.used / max(1, usage.total)) * 100.0


def _system_metrics_snapshot(config: RelayConfig) -> dict[str, float]:
    global _SYSTEM_METRICS_CACHE
    global _SYSTEM_METRICS_CACHE_AT

    now = time.monotonic()
    with _SYSTEM_METRICS_CACHE_LOCK:
        cached = _SYSTEM_METRICS_CACHE
        if (
            cached is not None
            and (now - _SYSTEM_METRICS_CACHE_AT) <= _SYSTEM_METRICS_CACHE_TTL_SECS
        ):
            return _clone_system_metrics_snapshot(cached)

        snapshot = _sample_system_metrics_snapshot(config)
        _SYSTEM_METRICS_CACHE = _clone_system_metrics_snapshot(snapshot)
        _SYSTEM_METRICS_CACHE_AT = now
        return _clone_system_metrics_snapshot(snapshot)


def _clone_system_metrics_snapshot(snapshot: dict[str, object]) -> dict[str, object]:
    services = {
        key: dict(value) for key, value in dict(snapshot.get("services") or {}).items()
    }
    clone = dict(snapshot)
    clone["services"] = services
    return clone


def _read_proc_stat_totals() -> tuple[int, int]:
    first_line = Path("/proc/stat").read_text().splitlines()[0]
    parts = first_line.split()
    values = [int(value) for value in parts[1:]]
    total = sum(values)
    iowait = values[4] if len(values) > 4 else 0
    return total, iowait


def _read_process_cpu_jiffies(pid: int) -> int | None:
    try:
        stat_text = Path(f"/proc/{pid}/stat").read_text()
    except OSError:
        return None

    after_name = stat_text[stat_text.rfind(")") + 2 :]
    fields = after_name.split()
    if len(fields) <= 12:
        return None
    return int(fields[11]) + int(fields[12])


def _read_systemd_service_state(service_name: str) -> dict[str, str]:
    try:
        result = subprocess.run(
            [
                "systemctl",
                "show",
                "--property=MainPID",
                "--property=ActiveState",
                "--property=SubState",
                service_name,
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.TimeoutExpired):
        return {
            "MainPID": "0",
            "ActiveState": "unknown",
            "SubState": "unknown",
        }

    payload: dict[str, str] = {
        "MainPID": "0",
        "ActiveState": "unknown",
        "SubState": "unknown",
    }
    for line in result.stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        payload[key] = value
    return payload


def _sample_system_metrics_snapshot(config: RelayConfig) -> dict[str, object]:
    services: dict[str, dict[str, object]] = {}
    for key, (service_name, label) in _SYSTEM_SERVICE_SPECS.items():
        state = _read_systemd_service_state(service_name)
        raw_pid = state.get("MainPID", "0").strip()
        try:
            pid = max(0, int(raw_pid))
        except ValueError:
            pid = 0
        services[key] = {
            "service_name": service_name,
            "label": label,
            "active_state": state.get("ActiveState", "unknown"),
            "sub_state": state.get("SubState", "unknown"),
            "pid": pid,
            "cpu_percent": 0.0,
        }

    process_cpu_before = {
        key: _read_process_cpu_jiffies(int(metric["pid"]))
        for key, metric in services.items()
        if int(metric["pid"]) > 0
    }
    total_before, iowait_before = _read_proc_stat_totals()
    time.sleep(_SYSTEM_METRICS_SAMPLE_SECS)
    total_after, iowait_after = _read_proc_stat_totals()
    process_cpu_after = {
        key: _read_process_cpu_jiffies(int(metric["pid"]))
        for key, metric in services.items()
        if int(metric["pid"]) > 0
    }

    cpu_count = os.cpu_count() or 1
    total_delta = max(1, total_after - total_before)
    iowait_delta = max(0, iowait_after - iowait_before)

    for key, metric in services.items():
        pid = int(metric["pid"])
        if pid <= 0:
            continue
        start = process_cpu_before.get(key)
        end = process_cpu_after.get(key)
        if start is None or end is None or end < start:
            continue
        metric["cpu_percent"] = round(
            max(0.0, ((end - start) / total_delta) * cpu_count * 100.0),
            1,
        )

    return {
        "cpu_percent": round(_cpu_percent_from_loadavg(), 1),
        "mem_percent": round(_memory_percent(), 1),
        "disk_percent": round(_disk_percent(config.data_dir), 1),
        "iowait_percent": round((iowait_delta / total_delta) * 100.0, 1),
        "services": services,
    }


def _system_badge_payload(label: str, percent: float) -> dict[str, object]:
    return _badge_payload(
        label=label,
        message=f"{percent:.1f}%",
        color=_usage_color(percent),
    )


def _service_badge_payload(
    service_metrics: dict[str, object] | None,
) -> dict[str, object]:
    if not service_metrics:
        return _badge_payload(label="Relay service", message="unknown", color="red")

    label = str(service_metrics.get("label") or "Relay service")
    active_state = str(service_metrics.get("active_state") or "unknown")
    sub_state = str(service_metrics.get("sub_state") or "").strip()

    if active_state == "active":
        state_label = sub_state or "active"
        return _badge_payload(
            label=label,
            message=f"{state_label} busy",
            color="brightgreen",
        )
    if active_state in {"activating", "deactivating", "reloading"}:
        return _badge_payload(
            label=label,
            message=active_state,
            color="yellow",
        )
    if active_state == "failed":
        return _badge_payload(
            label=label,
            message="failed",
            color="red",
        )
    return _badge_payload(
        label=label,
        message=active_state,
        color="lightgrey",
    )


def _stage_badge_payload(
    *,
    label: str,
    active_count: int,
    queued_count: int,
    error_count: int,
) -> dict[str, object]:
    if active_count > 0:
        return _badge_payload(
            label=label,
            message=f"active {active_count}",
            color="brightgreen",
        )
    if queued_count > 0:
        color = "orange" if queued_count >= 100 else "yellow"
        return _badge_payload(
            label=label,
            message=f"queued {queued_count}",
            color=color,
        )
    if error_count > 0:
        return _badge_payload(
            label=label,
            message=f"error {error_count}",
            color="red",
        )
    return _badge_payload(
        label=label,
        message="caught up",
        color="green",
    )


def _iso_hour_query(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    value = value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    return value


def _parse_db_timestamp(value: str | None) -> datetime | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _badge_payload(*, label: str, message: str, color: str) -> dict[str, object]:
    return {
        "schemaVersion": _BADGE_SCHEMA_VERSION,
        "label": label,
        "message": message,
        "color": color,
    }


def _progress_color(*, numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return "lightgrey"
    progress = numerator / denominator
    if progress >= 1.0:
        return "brightgreen"
    if progress >= 0.5:
        return "green"
    if progress >= 0.1:
        return "yellowgreen"
    return "orange"


def _short_hour_label(value: str | None) -> str:
    if value is None:
        return "none"
    normalized = value.strip()
    if not normalized:
        return "none"
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return value
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%HZ")


def _status_badge_payload(
    *,
    stats: dict[str, int | str | None],
    config: RelayConfig,
    now: datetime | None = None,
) -> dict[str, object]:
    current = (
        datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    )
    last_event_at = _parse_db_timestamp(stats.get("last_event_at"))  # type: ignore[arg-type]
    processing_hours = int(stats.get("processing_hours") or 0)
    ready_hours = int(stats.get("ready_to_process_hours") or 0)
    archive_hours = int(stats.get("archive_hours") or 0)

    if last_event_at is None:
        return _badge_payload(label="PMXT relay", message="starting", color="yellow")

    age_seconds = max(0.0, (current - last_event_at).total_seconds())
    stale_threshold = max(config.poll_interval_secs * 4, 3600)
    if age_seconds > stale_threshold:
        return _badge_payload(label="PMXT relay", message="stale", color="red")
    if processing_hours > 0:
        return _badge_payload(
            label="PMXT relay", message="processing", color="brightgreen"
        )
    if ready_hours > 0 or archive_hours > 0:
        return _badge_payload(label="PMXT relay", message="up", color="green")
    return _badge_payload(label="PMXT relay", message="idle", color="blue")


def _backfill_badge_payload(
    *,
    stats: dict[str, int | str | None],
) -> dict[str, object]:
    processed_hours = int(stats.get("processed_hours") or 0)
    archive_hours = int(stats.get("archive_hours") or 0)

    if archive_hours <= 0:
        return _badge_payload(
            label="Hours backfilled", message="0/0 hrs", color="lightgrey"
        )

    return _badge_payload(
        label="Hours backfilled",
        message=f"{processed_hours}/{archive_hours} hrs",
        color=_progress_color(numerator=processed_hours, denominator=archive_hours),
    )


def _ratio_badge_payload(
    *,
    label: str,
    numerator: int,
    denominator: int,
) -> dict[str, object]:
    if denominator <= 0:
        return _badge_payload(label=label, message="0/0 hrs", color="lightgrey")

    return _badge_payload(
        label=label,
        message=f"{numerator}/{denominator} hrs",
        color=_progress_color(numerator=numerator, denominator=denominator),
    )


def _mirrored_badge_payload(
    *,
    stats: dict[str, int | str | None],
) -> dict[str, object]:
    mirrored_hours = int(stats.get("mirrored_hours") or 0)
    archive_hours = int(stats.get("archive_hours") or 0)
    return _ratio_badge_payload(
        label="Hours mirrored",
        numerator=mirrored_hours,
        denominator=archive_hours,
    )


def _processed_badge_payload(
    *,
    stats: dict[str, int | str | None],
) -> dict[str, object]:
    processed_hours = int(stats.get("processed_hours") or 0)
    mirrored_hours = int(stats.get("mirrored_hours") or 0)
    return _ratio_badge_payload(
        label="Hours processed",
        numerator=processed_hours,
        denominator=mirrored_hours,
    )


def _latest_processed_badge_payload(
    *,
    queue: dict[str, int | str | None],
) -> dict[str, object]:
    latest_processed_hour = queue.get("latest_processed_hour")
    latest_label = (
        latest_processed_hour if isinstance(latest_processed_hour, str) else None
    )
    return _badge_payload(
        label="Latest hour",
        message=_short_hour_label(latest_label),
        color="blue",
    )


def _lag_badge_payload(
    *,
    stats: dict[str, int | str | None],
) -> dict[str, object]:
    archive_hours = int(stats.get("archive_hours") or 0)
    processed_hours = int(stats.get("processed_hours") or 0)
    lag_hours = max(0, archive_hours - processed_hours)

    if lag_hours == 0:
        color = "brightgreen"
    elif lag_hours <= 24:
        color = "green"
    elif lag_hours <= 168:
        color = "yellowgreen"
    else:
        color = "orange"

    return _badge_payload(
        label="Queue lag",
        message=f"{lag_hours} hrs",
        color=color,
    )


def _rate_badge_payload(
    *,
    stats: dict[str, int | str | None | float],
) -> dict[str, object]:
    rate = float(stats.get("processed_hours_per_hour_24h") or 0.0)
    if rate >= 4.0:
        color = "brightgreen"
    elif rate >= 1.0:
        color = "green"
    elif rate >= 0.5:
        color = "yellowgreen"
    elif rate > 0.0:
        color = "orange"
    else:
        color = "red"

    if rate >= 10.0:
        message = f"{rate:.0f} hr/hr"
    elif rate >= 1.0:
        message = f"{rate:.1f} hr/hr"
    else:
        message = f"{rate:.2f} hr/hr"

    return _badge_payload(
        label="Completion rate",
        message=message,
        color=color,
    )


def _file_badge_payload(
    *,
    stats: dict[str, int | str | None],
    progress: PrebuildProgress | None,
    current_filename: str | None = None,
) -> dict[str, object]:
    processing_hours = int(stats.get("processing_hours") or 0)
    if processing_hours <= 0:
        return _badge_payload(label="Current file", message="idle", color="lightgrey")
    if current_filename is not None:
        return _badge_payload(
            label="Current file",
            message=current_filename,
            color="blue",
        )
    if progress is None:
        return _badge_payload(label="Current file", message="starting", color="yellow")
    return _badge_payload(
        label="Current file",
        message=progress.filename,
        color="blue",
    )


def _rows_badge_payload(
    *,
    stats: dict[str, int | str | None],
    progress: PrebuildProgress | None,
    current_filename: str | None = None,
) -> dict[str, object]:
    processing_hours = int(stats.get("processing_hours") or 0)
    if processing_hours <= 0:
        return _badge_payload(label="Rows processed", message="idle", color="lightgrey")
    if progress is None:
        return _badge_payload(
            label="Rows processed", message="starting", color="yellow"
        )
    if current_filename is not None and progress.filename != current_filename:
        return _badge_payload(
            label="Rows processed", message="starting", color="yellow"
        )
    return _badge_payload(
        label="Rows processed",
        message=f"{progress.processed_rows:,} / {progress.total_rows:,}",
        color=_progress_color(
            numerator=progress.processed_rows,
            denominator=progress.total_rows,
        ),
    )


class RequestRateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        self._requests_per_minute = requests_per_minute
        self._requests: dict[str, deque[float]] = defaultdict(deque)

    @staticmethod
    def _prune_bucket(bucket: deque[float], *, window_start: float) -> None:
        while bucket and bucket[0] <= window_start:
            bucket.popleft()

    def _prune_stale_buckets(self, *, now: float) -> None:
        window_start = now - 60.0
        stale_clients: list[str] = []
        for client_id, bucket in self._requests.items():
            self._prune_bucket(bucket, window_start=window_start)
            if not bucket:
                stale_clients.append(client_id)
        for client_id in stale_clients:
            del self._requests[client_id]

    def allow(self, client_id: str, *, now: float | None = None) -> bool:
        if self._requests_per_minute <= 0:
            return True

        current = time.monotonic() if now is None else now
        window_start = current - 60.0
        bucket = self._requests[client_id]
        self._prune_bucket(bucket, window_start=window_start)

        if len(bucket) >= self._requests_per_minute:
            return False

        bucket.append(current)
        if len(self._requests) > 10000:
            self._prune_stale_buckets(now=current)
        return True

    def bucket_size(self, client_id: str, *, now: float | None = None) -> int:
        current = time.monotonic() if now is None else now
        window_start = current - 60.0
        bucket = self._requests.get(client_id)
        if bucket is None:
            return 0
        self._prune_bucket(bucket, window_start=window_start)
        if not bucket:
            del self._requests[client_id]
        return len(bucket)


CONFIG_APP_KEY = web.AppKey("config", RelayConfig)
INDEX_APP_KEY = web.AppKey("index", RelayIndex)
RATE_LIMITER_APP_KEY = web.AppKey("rate_limiter", RequestRateLimiter)
FILTERED_STORE_APP_KEY = web.AppKey("filtered_store", FilteredHourStore)


def _client_id(
    request: web.Request,
    *,
    trusted_proxy_ips: tuple[str, ...] = (),
) -> str:
    remote = request.remote
    if remote in trusted_proxy_ips:
        forwarded_for = request.headers.get("X-Forwarded-For", "")
        for candidate in forwarded_for.split(","):
            client_ip = candidate.strip()
            if client_ip:
                return client_ip
    if remote:
        return remote
    peername = (
        request.transport.get_extra_info("peername") if request.transport else None
    )
    if isinstance(peername, tuple) and peername:
        return str(peername[0])
    return "unknown"


def _resolve_path_under_root(root: Path, *parts: str) -> Path | None:
    try:
        resolved_root = root.resolve(strict=False)
        candidate = root.joinpath(*parts).resolve(strict=False)
    except OSError:
        return None

    if not candidate.is_relative_to(resolved_root):
        return None

    return candidate


def _resolve_filtered_path(
    config: RelayConfig,
    condition_id: str,
    token_id: str,
    filename: str,
) -> Path | None:
    if not _CONDITION_ID_RE.fullmatch(condition_id):
        return None
    if not _TOKEN_ID_RE.fullmatch(token_id):
        return None
    if not _FILTERED_FILENAME_RE.fullmatch(filename):
        return None
    return _resolve_path_under_root(
        config.filtered_root,
        condition_id,
        token_id,
        filename,
    )


def _resolve_raw_path(config: RelayConfig, filename: str) -> Path | None:
    if not _RAW_FILENAME_RE.fullmatch(filename):
        return None
    return _resolve_path_under_root(config.raw_root, *Path(filename).parts)


def _collect_inflight_processes(config: RelayConfig) -> list[dict[str, object]]:
    inflight: list[dict[str, object]] = []
    for tmp_dir in sorted(config.tmp_root.glob("*.filtered")):
        if not tmp_dir.is_dir():
            continue
        dir_count = 0
        file_count = 0
        byte_size = 0
        latest_mtime = 0.0
        for root, dirs, files in os.walk(tmp_dir):
            dir_count += len(dirs)
            file_count += len(files)
            for filename in files:
                path = Path(root) / filename
                try:
                    stat = path.stat()
                except FileNotFoundError:
                    continue
                byte_size += stat.st_size
                latest_mtime = max(latest_mtime, stat.st_mtime)
        if file_count == 0:
            latest_mtime = tmp_dir.stat().st_mtime
        inflight.append(
            {
                "filename": tmp_dir.name.removesuffix(".filtered"),
                "path": str(tmp_dir),
                "dir_count": dir_count + 1,
                "file_count": file_count,
                "byte_size": byte_size,
                "latest_mtime": latest_mtime,
            }
        )
    return inflight


@web.middleware
async def hardening_middleware(
    request: web.Request,
    handler,
) -> web.StreamResponse:
    def _apply_headers(response: web.StreamResponse) -> None:
        response.headers.pop("Server", None)
        response.headers.setdefault("Cache-Control", _CACHE_CONTROL_JSON)
        for header, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)

    config = request.app[CONFIG_APP_KEY]
    limiter = request.app[RATE_LIMITER_APP_KEY]
    client_id = _client_id(request, trusted_proxy_ips=config.trusted_proxy_ips)
    if not limiter.allow(client_id):
        exc = web.HTTPTooManyRequests(
            text="rate limit exceeded",
            headers={"Retry-After": "60"},
        )
        _apply_headers(exc)
        raise exc

    try:
        response = await handler(request)
    except web.HTTPException as exc:
        _apply_headers(exc)
        raise

    _apply_headers(response)
    return response


async def on_prepare_response(
    _request: web.Request,
    response: web.StreamResponse,
) -> None:
    response.headers.pop("Server", None)


async def healthz(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _index_stats_async(index: object) -> dict[str, object]:
    return await asyncio.to_thread(index.stats)


async def _index_queue_summary_async(index: object) -> dict[str, object]:
    return await asyncio.to_thread(index.queue_summary)


async def _index_recent_events_async(index: object, limit: int):
    return await asyncio.to_thread(index.recent_events, limit=limit)


async def _index_progress_snapshot_async(
    index: object,
) -> tuple[dict[str, object], dict[str, object] | None, str | None]:
    def _snapshot():
        return (
            index.stats(),
            index.latest_prebuild_progress(),
            index.current_processing_filename(),
        )

    return await asyncio.to_thread(_snapshot)


async def _filtered_store_list_hours_async(
    filtered_store: FilteredHourStore,
    condition_id: str,
    token_id: str,
    *,
    start_hour: str | None = None,
    end_hour: str | None = None,
):
    return await asyncio.to_thread(
        filtered_store.list_hours,
        condition_id,
        token_id,
        start_hour=start_hour,
        end_hour=end_hour,
    )


async def stats(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(await _index_stats_async(index))


async def queue(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(await _index_queue_summary_async(index))


async def events(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    limit_value = request.query.get("limit", "100").strip()
    try:
        limit = max(1, min(1000, int(limit_value)))
    except ValueError:
        limit = 100
    rows = await _index_recent_events_async(index, limit)
    events_payload = []
    for row in rows:
        event_payload = None
        raw_payload = row["payload_json"]
        if raw_payload is not None:
            try:
                event_payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                event_payload = {"raw_payload": raw_payload}
        events_payload.append(
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "level": row["level"],
                "event_type": row["event_type"],
                "filename": row["filename"],
                "message": row["message"],
                "payload": event_payload,
            }
        )
    return web.json_response({"events": events_payload})


async def inflight(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    payload = await asyncio.to_thread(_collect_inflight_processes, config)
    return web.json_response({"inflight": payload})


async def system_metrics(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    payload = await asyncio.to_thread(_system_metrics_snapshot, config)
    return web.json_response(payload)


async def badge_status(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _status_badge_payload(stats=await _index_stats_async(index), config=config)
    )


async def badge_backfill(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _backfill_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_mirrored(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _mirrored_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_processed(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _processed_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_latest(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _latest_processed_badge_payload(queue=await _index_queue_summary_async(index))
    )


async def badge_lag(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(_lag_badge_payload(stats=await _index_stats_async(index)))


async def badge_rate(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(_rate_badge_payload(stats=await _index_stats_async(index)))


async def badge_file(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    stats, progress, current_filename = await _index_progress_snapshot_async(index)
    return web.json_response(
        _file_badge_payload(
            stats=stats,
            progress=progress,
            current_filename=current_filename,
        )
    )


async def badge_rows(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    stats, progress, current_filename = await _index_progress_snapshot_async(index)
    return web.json_response(
        _rows_badge_payload(
            stats=stats,
            progress=progress,
            current_filename=current_filename,
        )
    )


async def badge_cpu_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _system_badge_payload("CPU load", float(metrics["cpu_percent"]))
    )


async def badge_mem_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _system_badge_payload("RAM", float(metrics["mem_percent"]))
    )


async def badge_disk_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _system_badge_payload("Disk", float(metrics["disk_percent"]))
    )


async def badge_load_svg(request: web.Request) -> web.Response:
    return await badge_cpu_svg(request)


async def badge_iowait_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _system_badge_payload("I/O wait", float(metrics["iowait_percent"]))
    )


def _service_metrics_for_badge(
    metrics: dict[str, object],
    service_key: str,
) -> dict[str, object] | None:
    services = metrics.get("services")
    if not isinstance(services, dict):
        return None
    service_metrics = services.get(service_key)
    if not isinstance(service_metrics, dict):
        return None
    return service_metrics


def _service_running_badge_payload(label: str) -> dict[str, object]:
    return _badge_payload(label=label, message="running busy", color="brightgreen")


async def badge_api_svg(request: web.Request) -> web.Response:
    return _badge_svg_response(_service_running_badge_payload("API service"))


async def badge_worker_svg(request: web.Request) -> web.Response:
    return _badge_svg_response(_service_running_badge_payload("Worker service"))


async def badge_mirroring_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    queue = await _index_queue_summary_async(index)
    return _badge_svg_response(
        _stage_badge_payload(
            label="Mirror service",
            active_count=int(queue.get("mirror_processing") or 0),
            queued_count=int(queue.get("mirror_pending") or 0),
            error_count=int(queue.get("mirror_error") or 0),
        )
    )


async def badge_processing_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    queue = await _index_queue_summary_async(index)
    return _badge_svg_response(
        _stage_badge_payload(
            label="Processing",
            active_count=int(queue.get("process_processing") or 0),
            queued_count=int(
                (queue.get("process_ready") or 0) + (queue.get("process_pending") or 0)
            ),
            error_count=int(queue.get("process_error") or 0),
        )
    )


async def badge_clickhouse_svg(request: web.Request) -> web.Response:
    return _badge_svg_response(_service_running_badge_payload("ClickHouse"))


async def badge_status_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _status_badge_payload(stats=await _index_stats_async(index), config=config)
    )


async def badge_backfill_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _backfill_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_mirrored_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _mirrored_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_processed_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _processed_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_latest_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _latest_processed_badge_payload(queue=await _index_queue_summary_async(index))
    )


async def badge_lag_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _lag_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_rate_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _rate_badge_payload(stats=await _index_stats_async(index))
    )


async def badge_file_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    stats, progress, current_filename = await _index_progress_snapshot_async(index)
    return _badge_svg_response(
        _file_badge_payload(
            stats=stats,
            progress=progress,
            current_filename=current_filename,
        )
    )


async def badge_rows_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    stats, progress, current_filename = await _index_progress_snapshot_async(index)
    return _badge_svg_response(
        _rows_badge_payload(
            stats=stats,
            progress=progress,
            current_filename=current_filename,
        )
    )


async def list_filtered_hours(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    filtered_store = request.app[FILTERED_STORE_APP_KEY]
    condition_id = request.match_info["condition_id"]
    token_id = request.match_info["token_id"]
    if not _CONDITION_ID_RE.fullmatch(condition_id) or not _TOKEN_ID_RE.fullmatch(
        token_id
    ):
        raise web.HTTPNotFound(text="filtered hours not found")
    start_hour = _iso_hour_query(request.query.get("start"))
    end_hour = _iso_hour_query(request.query.get("end"))
    rows = await _filtered_store_list_hours_async(
        filtered_store,
        condition_id,
        token_id,
        start_hour=start_hour,
        end_hour=end_hour,
    )
    truncated = len(rows) > config.api_list_max_hours
    if truncated:
        rows = rows[: config.api_list_max_hours]
    entries = []
    for row in rows:
        relative_url = f"/v1/filtered/{condition_id}/{token_id}/{row.filename}"
        url = (
            f"{config.public_base_url}{relative_url}"
            if config.public_base_url is not None
            else relative_url
        )
        entries.append(
            {
                "hour": row.hour,
                "filename": row.filename,
                "row_count": row.row_count,
                "byte_size": row.byte_size,
                "url": url,
            }
        )
    return web.json_response(
        {
            "condition_id": condition_id,
            "token_id": token_id,
            "hours": entries,
            "truncated": truncated,
        }
    )


async def serve_filtered(request: web.Request) -> web.StreamResponse:
    config = request.app[CONFIG_APP_KEY]
    filtered_store = request.app[FILTERED_STORE_APP_KEY]
    condition_id = request.match_info["condition_id"]
    token_id = request.match_info["token_id"]
    filename = request.match_info["filename"]
    if not _CONDITION_ID_RE.fullmatch(condition_id):
        raise web.HTTPNotFound(text="filtered hour not found")
    if not _TOKEN_ID_RE.fullmatch(token_id):
        raise web.HTTPNotFound(text="filtered hour not found")
    if not _FILTERED_FILENAME_RE.fullmatch(filename):
        raise web.HTTPNotFound(text="filtered hour not found")
    response = await filtered_store.serve_hour(
        request,
        condition_id=condition_id,
        token_id=token_id,
        filename=filename,
    )
    if response is None:
        processed_path = config.processed_root / processed_relative_path(filename)
        if processed_path.exists():
            raise web.HTTPNotFound(text="filtered hour not yet prebuilt")
        raise web.HTTPNotFound(text="filtered hour not found")
    return response


async def serve_raw(request: web.Request) -> web.StreamResponse:
    config = request.app[CONFIG_APP_KEY]
    filename = request.match_info["filename"]
    path = _resolve_raw_path(config, filename)
    if path is None or not path.exists():
        raise web.HTTPNotFound(text="raw hour not found")
    response = web.FileResponse(path)
    response.headers["Cache-Control"] = _CACHE_CONTROL_FILE
    return response


def create_app(config: RelayConfig) -> web.Application:
    app = web.Application(
        client_max_size=4096,
        middlewares=[hardening_middleware],
    )
    app[CONFIG_APP_KEY] = config
    index = RelayIndex(config.db_path, event_retention=config.event_retention)
    app[INDEX_APP_KEY] = index
    if config.filtered_api_enabled:
        app[FILTERED_STORE_APP_KEY] = create_filtered_hour_store(config, index)
    app[RATE_LIMITER_APP_KEY] = RequestRateLimiter(config.api_rate_limit_per_minute)
    index.initialize(apply_maintenance=False)
    app.on_response_prepare.append(on_prepare_response)
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/stats", stats)
    app.router.add_get("/v1/queue", queue)
    app.router.add_get("/v1/events", events)
    app.router.add_get("/v1/inflight", inflight)
    app.router.add_get("/v1/system", system_metrics)
    app.router.add_get("/v1/badge/status", badge_status)
    app.router.add_get("/v1/badge/backfill", badge_backfill)
    app.router.add_get("/v1/badge/mirrored", badge_mirrored)
    app.router.add_get("/v1/badge/processed", badge_processed)
    app.router.add_get("/v1/badge/latest", badge_latest)
    app.router.add_get("/v1/badge/lag", badge_lag)
    app.router.add_get("/v1/badge/rate", badge_rate)
    app.router.add_get("/v1/badge/file", badge_file)
    app.router.add_get("/v1/badge/rows", badge_rows)
    app.router.add_get("/v1/badge/status.svg", badge_status_svg)
    app.router.add_get("/v1/badge/backfill.svg", badge_backfill_svg)
    app.router.add_get("/v1/badge/mirrored.svg", badge_mirrored_svg)
    app.router.add_get("/v1/badge/processed.svg", badge_processed_svg)
    app.router.add_get("/v1/badge/latest.svg", badge_latest_svg)
    app.router.add_get("/v1/badge/lag.svg", badge_lag_svg)
    app.router.add_get("/v1/badge/rate.svg", badge_rate_svg)
    app.router.add_get("/v1/badge/file.svg", badge_file_svg)
    app.router.add_get("/v1/badge/rows.svg", badge_rows_svg)
    app.router.add_get("/v1/badge/cpu.svg", badge_cpu_svg)
    app.router.add_get("/v1/badge/load.svg", badge_load_svg)
    app.router.add_get("/v1/badge/mem.svg", badge_mem_svg)
    app.router.add_get("/v1/badge/disk.svg", badge_disk_svg)
    app.router.add_get("/v1/badge/iowait.svg", badge_iowait_svg)
    app.router.add_get("/v1/badge/api.svg", badge_api_svg)
    app.router.add_get("/v1/badge/worker.svg", badge_worker_svg)
    app.router.add_get("/v1/badge/mirroring.svg", badge_mirroring_svg)
    app.router.add_get("/v1/badge/processing.svg", badge_processing_svg)
    app.router.add_get("/v1/badge/clickhouse.svg", badge_clickhouse_svg)
    if config.filtered_api_enabled:
        app.router.add_get(
            "/v1/markets/{condition_id}/tokens/{token_id}/hours",
            list_filtered_hours,
        )
        app.router.add_get(
            "/v1/filtered/{condition_id}/{token_id}/{filename}",
            serve_filtered,
        )
    app.router.add_get("/v1/raw/{filename:.*}", serve_raw)
    return app
