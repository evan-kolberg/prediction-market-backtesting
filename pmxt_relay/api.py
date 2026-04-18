from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from xml.sax.saxutils import escape

from aiohttp import web

from pmxt_relay.config import RelayConfig
from pmxt_relay.coverage import count_raw_dump_files, elapsed_archive_hours
from pmxt_relay.index_db import RelayIndex

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
_RATE_LIMITER_PRUNE_INTERVAL_SECS = 60.0
_RATE_LIMITER_FORCE_PRUNE_CLIENTS = 10_000
_SYSTEM_SERVICE_SPECS = {
    "api": ("pmxt-relay-api.service", "API service"),
    "worker": ("pmxt-relay-worker.service", "Worker service"),
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
    global _SYSTEM_METRICS_CACHE, _SYSTEM_METRICS_CACHE_AT

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
    services = {key: dict(value) for key, value in dict(snapshot.get("services") or {}).items()}
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
        return {"MainPID": "0", "ActiveState": "unknown", "SubState": "unknown"}

    payload: dict[str, str] = {"MainPID": "0", "ActiveState": "unknown", "SubState": "unknown"}
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
            max(0.0, ((end - start) / total_delta) * cpu_count * 100.0), 1
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


def _service_badge_payload(service_metrics: dict[str, object] | None) -> dict[str, object]:
    if not service_metrics:
        return _badge_payload(
            label="Relay service",
            message="unknown",
            color="red",
        )

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
    *, label: str, active_count: int, queued_count: int, error_count: int
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


def _raw_source_label(raw_base_url: str) -> str:
    return urlparse(raw_base_url).netloc or raw_base_url or "PMXT archive"


def _raw_source_for_badge(config: RelayConfig, index: int = 0) -> str:
    raw_base_urls = config.resolved_raw_base_urls
    if raw_base_urls:
        return raw_base_urls[min(index, len(raw_base_urls) - 1)]
    return config.raw_base_url


def _status_badge_payload(
    *,
    stats: dict[str, int | str | None],
    system: dict[str, object] | None,
    config: RelayConfig,
    now: datetime | None = None,
) -> dict[str, object]:
    current = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    last_event_at = _parse_db_timestamp(stats.get("last_event_at"))  # type: ignore[arg-type]

    if last_event_at is None:
        return _badge_payload(
            label="PMXT relay",
            message="starting",
            color="yellow",
        )

    api_service = _service_metrics_for_badge(system or {}, "api")
    worker_service = _service_metrics_for_badge(system or {}, "worker")
    services = (api_service, worker_service)
    if any(service is None for service in services):
        return _badge_payload(
            label="PMXT relay",
            message="unknown",
            color="red",
        )

    active_states = {str(service.get("active_state") or "unknown") for service in services}
    if "failed" in active_states:
        return _badge_payload(
            label="PMXT relay",
            message="failed",
            color="red",
        )

    if active_states != {"active"}:
        return _badge_payload(
            label="PMXT relay",
            message="degraded",
            color="orange",
        )

    age_seconds = max(0.0, (current - last_event_at).total_seconds())
    stale_threshold = max(config.poll_interval_secs * 4, 3600)
    if age_seconds > stale_threshold:
        return _badge_payload(
            label="PMXT relay",
            message="stale",
            color="red",
        )

    return _badge_payload(
        label="PMXT relay",
        message="up",
        color="brightgreen",
    )


def _upstream_badge_payload(
    *,
    stats: dict[str, int | str | None],
    queue: dict[str, int | str | None],
    config: RelayConfig,
    raw_base_url: str | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    del queue
    current = datetime.now(timezone.utc) if now is None else now.astimezone(timezone.utc)
    last_event_at = _parse_db_timestamp(stats.get("last_event_at"))  # type: ignore[arg-type]
    upstream_label = _raw_source_label(raw_base_url or _raw_source_for_badge(config))

    if last_event_at is None:
        return _badge_payload(
            label=upstream_label,
            message="checking",
            color="yellow",
        )

    age_seconds = max(0.0, (current - last_event_at).total_seconds())
    stale_threshold = max(config.poll_interval_secs * 4, 3600)
    if age_seconds > stale_threshold:
        return _badge_payload(
            label=upstream_label,
            message="offline",
            color="red",
        )

    return _badge_payload(
        label=upstream_label,
        message="online",
        color="brightgreen",
    )


def _dump_files_badge_payload(*, stats: dict[str, int | str | None]) -> dict[str, object]:
    dump_files = int(stats.get("dump_files_on_disk") or stats.get("mirrored_hours") or 0)
    color = "brightgreen" if dump_files > 0 else "lightgrey"
    return _badge_payload(label="Hour files", message=f"{dump_files} files", color=color)


def _archive_hours_badge_payload(*, stats: dict[str, int | str | None]) -> dict[str, object]:
    archive_hours = int(stats.get("archive_hours") or 0)
    color = "blue" if archive_hours > 0 else "lightgrey"
    return _badge_payload(label="Hours since first", message=f"{archive_hours} hrs", color=color)


def _latest_hour_badge_payload(*, queue: dict[str, int | str | None]) -> dict[str, object]:
    latest_filename = queue.get("latest_mirrored_filename")
    if isinstance(latest_filename, str) and latest_filename:
        message = latest_filename.removesuffix(".parquet")
        return _badge_payload(label="Latest hour", message=message, color="blue")

    latest_hour = queue.get("latest_mirrored_hour")
    if isinstance(latest_hour, str) and latest_hour:
        hour_label = latest_hour.replace(":00:00+00:00", "").replace("+00:00", "")
        return _badge_payload(label="Latest hour", message=hour_label, color="blue")

    return _badge_payload(label="Latest hour", message="none", color="lightgrey")


class RequestRateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        self._requests_per_minute = requests_per_minute
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._last_prune_at = 0.0

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

    def _maybe_prune_stale_buckets(self, *, now: float) -> None:
        if len(self._requests) >= _RATE_LIMITER_FORCE_PRUNE_CLIENTS:
            self._prune_stale_buckets(now=now)
            self._last_prune_at = now
            return

        if (now - self._last_prune_at) < _RATE_LIMITER_PRUNE_INTERVAL_SECS:
            return

        self._prune_stale_buckets(now=now)
        self._last_prune_at = now

    def allow(self, client_id: str, *, now: float | None = None) -> bool:
        if self._requests_per_minute <= 0:
            return True

        current = time.monotonic() if now is None else now
        self._maybe_prune_stale_buckets(now=current)
        window_start = current - 60.0
        bucket = self._requests[client_id]
        self._prune_bucket(bucket, window_start=window_start)

        if len(bucket) >= self._requests_per_minute:
            return False

        bucket.append(current)
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


def _client_id(request: web.Request, *, trusted_proxy_ips: tuple[str, ...] = ()) -> str:
    remote = request.remote
    if remote in trusted_proxy_ips:
        forwarded_for = request.headers.get("X-Forwarded-For", "")
        for candidate in forwarded_for.split(","):
            client_ip = candidate.strip()
            if client_ip:
                return client_ip
    if remote:
        return remote
    peername = request.transport.get_extra_info("peername") if request.transport else None
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


def _resolve_raw_path(config: RelayConfig, filename: str) -> Path | None:
    if not _RAW_FILENAME_RE.fullmatch(filename):
        return None
    return _resolve_path_under_root(config.raw_root, *Path(filename).parts)


def _collect_inflight_downloads(config: RelayConfig) -> list[dict[str, object]]:
    inflight: list[dict[str, object]] = []
    for tmp_path in sorted(config.raw_root.rglob("*.tmp")):
        if not tmp_path.is_file():
            continue
        try:
            stat = tmp_path.stat()
        except FileNotFoundError:
            continue
        inflight.append(
            {
                "filename": tmp_path.name.removesuffix(".tmp"),
                "path": str(tmp_path),
                "dir_count": 0,
                "file_count": 1,
                "byte_size": stat.st_size,
                "latest_mtime": stat.st_mtime,
            }
        )
    return inflight


@web.middleware
async def hardening_middleware(request: web.Request, handler) -> web.StreamResponse:
    def _apply_headers(response: web.StreamResponse) -> None:
        response.headers.pop("Server", None)
        response.headers.setdefault("Cache-Control", _CACHE_CONTROL_JSON)
        for header, value in _SECURITY_HEADERS.items():
            response.headers.setdefault(header, value)

    config = request.app[CONFIG_APP_KEY]
    limiter = request.app[RATE_LIMITER_APP_KEY]
    client_id = _client_id(request, trusted_proxy_ips=config.trusted_proxy_ips)
    if not limiter.allow(client_id):
        exc = web.HTTPTooManyRequests(text="rate limit exceeded", headers={"Retry-After": "60"})
        _apply_headers(exc)
        raise exc

    try:
        response = await handler(request)
    except web.HTTPException as exc:
        _apply_headers(exc)
        raise

    _apply_headers(response)
    return response


async def on_prepare_response(_request: web.Request, response: web.StreamResponse) -> None:
    response.headers.pop("Server", None)


async def healthz(_request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def _index_stats_async(index: object) -> dict[str, object]:
    return await asyncio.to_thread(index.stats)


def _coverage_stats(config: RelayConfig) -> dict[str, object]:
    dump_files_on_disk = count_raw_dump_files(config.raw_root)
    archive_hours = elapsed_archive_hours(start_hour=config.archive_start_hour)
    return {
        "archive_start_hour": config.archive_start_hour.isoformat(),
        "archive_hours": archive_hours,
        "dump_files_on_disk": dump_files_on_disk,
        "mirrored_hours": dump_files_on_disk,
    }


async def _relay_stats_async(config: RelayConfig, index: RelayIndex) -> dict[str, object]:
    index_payload, coverage_payload = await asyncio.gather(
        _index_stats_async(index),
        asyncio.to_thread(_coverage_stats, config),
    )
    payload = dict(index_payload)
    payload.update(coverage_payload)
    return payload


async def _index_queue_summary_async(index: object) -> dict[str, object]:
    return await asyncio.to_thread(index.queue_summary)


async def _index_recent_events_async(index: object, limit: int):
    return await asyncio.to_thread(index.recent_events, limit=limit)


async def stats(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return web.json_response(await _relay_stats_async(config, index))


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
    payload = await asyncio.to_thread(_collect_inflight_downloads, config)
    return web.json_response({"inflight": payload})


async def system_metrics(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    payload = await asyncio.to_thread(_system_metrics_snapshot, config)
    return web.json_response(payload)


async def badge_status(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    stats_payload, system_payload = await asyncio.gather(
        _relay_stats_async(config, index), asyncio.to_thread(_system_metrics_snapshot, config)
    )
    return web.json_response(
        _status_badge_payload(
            stats=stats_payload,
            system=system_payload,
            config=config,
        )
    )


async def badge_dump_files(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _dump_files_badge_payload(stats=await _relay_stats_async(config, index))
    )


async def badge_archive_hours(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _archive_hours_badge_payload(stats=await _relay_stats_async(config, index))
    )


async def badge_cpu_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(_system_badge_payload("CPU load", float(metrics["cpu_percent"])))


async def badge_mem_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(_system_badge_payload("RAM", float(metrics["mem_percent"])))


async def badge_disk_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(_system_badge_payload("Disk", float(metrics["disk_percent"])))


async def badge_load_svg(request: web.Request) -> web.Response:
    return await badge_cpu_svg(request)


async def badge_iowait_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(_system_badge_payload("I/O wait", float(metrics["iowait_percent"])))


def _service_metrics_for_badge(
    metrics: dict[str, object], service_key: str
) -> dict[str, object] | None:
    services = metrics.get("services")
    if not isinstance(services, dict):
        return None
    service_metrics = services.get(service_key)
    if not isinstance(service_metrics, dict):
        return None
    return service_metrics


async def badge_api_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(_service_badge_payload(_service_metrics_for_badge(metrics, "api")))


async def badge_worker_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _service_badge_payload(_service_metrics_for_badge(metrics, "worker"))
    )


async def badge_mirroring_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    queue = await _index_queue_summary_async(index)
    return _badge_svg_response(
        _stage_badge_payload(
            label="Mirror service",
            active_count=int(queue.get("mirror_active") or 0),
            queued_count=int(queue.get("mirror_pending") or 0),
            error_count=int(queue.get("mirror_error") or 0),
        )
    )


async def badge_status_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    stats_payload, system_payload = await asyncio.gather(
        _relay_stats_async(config, index), asyncio.to_thread(_system_metrics_snapshot, config)
    )
    return _badge_svg_response(
        _status_badge_payload(
            stats=stats_payload,
            system=system_payload,
            config=config,
        )
    )


async def badge_upstream(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    stats_payload, queue_payload = await asyncio.gather(
        _relay_stats_async(config, index), _index_queue_summary_async(index)
    )
    return web.json_response(
        _upstream_badge_payload(
            stats=stats_payload,
            queue=queue_payload,
            config=config,
        )
    )


async def badge_upstream_r2(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    stats_payload, queue_payload = await asyncio.gather(
        _relay_stats_async(config, index), _index_queue_summary_async(index)
    )
    return web.json_response(
        _upstream_badge_payload(
            stats=stats_payload,
            queue=queue_payload,
            config=config,
            raw_base_url="https://r2.pmxt.dev",
        )
    )


async def badge_upstream_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    stats_payload, queue_payload = await asyncio.gather(
        _relay_stats_async(config, index), _index_queue_summary_async(index)
    )
    return _badge_svg_response(
        _upstream_badge_payload(
            stats=stats_payload,
            queue=queue_payload,
            config=config,
        )
    )


async def badge_upstream_r2_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    stats_payload, queue_payload = await asyncio.gather(
        _relay_stats_async(config, index), _index_queue_summary_async(index)
    )
    return _badge_svg_response(
        _upstream_badge_payload(
            stats=stats_payload,
            queue=queue_payload,
            config=config,
            raw_base_url="https://r2.pmxt.dev",
        )
    )


async def badge_dump_files_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _dump_files_badge_payload(stats=await _relay_stats_async(config, index))
    )


async def badge_archive_hours_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _archive_hours_badge_payload(stats=await _relay_stats_async(config, index))
    )


async def badge_latest_hour_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _latest_hour_badge_payload(queue=await _index_queue_summary_async(index))
    )


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
    async def close_index(app: web.Application) -> None:
        app[INDEX_APP_KEY].close()

    app = web.Application(client_max_size=4096, middlewares=[hardening_middleware])
    app[CONFIG_APP_KEY] = config
    index = RelayIndex(config.db_path, event_retention=config.event_retention)
    app[INDEX_APP_KEY] = index
    app[RATE_LIMITER_APP_KEY] = RequestRateLimiter(config.api_rate_limit_per_minute)
    index.initialize(apply_maintenance=False)
    app.on_cleanup.append(close_index)
    app.on_response_prepare.append(on_prepare_response)
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/stats", stats)
    app.router.add_get("/v1/queue", queue)
    app.router.add_get("/v1/events", events)
    app.router.add_get("/v1/inflight", inflight)
    app.router.add_get("/v1/system", system_metrics)
    app.router.add_get("/v1/badge/status", badge_status)
    app.router.add_get("/v1/badge/upstream", badge_upstream)
    app.router.add_get("/v1/badge/upstream-r2", badge_upstream_r2)
    app.router.add_get("/v1/badge/hour-files", badge_dump_files)
    app.router.add_get("/v1/badge/hours-since-first", badge_archive_hours)
    app.router.add_get("/v1/badge/status.svg", badge_status_svg)
    app.router.add_get("/v1/badge/upstream.svg", badge_upstream_svg)
    app.router.add_get("/v1/badge/upstream-r2.svg", badge_upstream_r2_svg)
    app.router.add_get("/v1/badge/hour-files.svg", badge_dump_files_svg)
    app.router.add_get("/v1/badge/hours-since-first.svg", badge_archive_hours_svg)
    app.router.add_get("/v1/badge/latest-hour.svg", badge_latest_hour_svg)
    app.router.add_get("/v1/badge/latest-file.svg", badge_latest_hour_svg)
    app.router.add_get("/v1/badge/cpu.svg", badge_cpu_svg)
    app.router.add_get("/v1/badge/load.svg", badge_load_svg)
    app.router.add_get("/v1/badge/mem.svg", badge_mem_svg)
    app.router.add_get("/v1/badge/disk.svg", badge_disk_svg)
    app.router.add_get("/v1/badge/iowait.svg", badge_iowait_svg)
    app.router.add_get("/v1/badge/api.svg", badge_api_svg)
    app.router.add_get("/v1/badge/worker.svg", badge_worker_svg)
    app.router.add_get("/v1/badge/mirroring.svg", badge_mirroring_svg)
    app.router.add_get("/v1/raw/{filename:.*}", serve_raw)
    return app
