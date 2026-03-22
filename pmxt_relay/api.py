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
import time as time_module
from xml.sax.saxutils import escape

from aiohttp import web

from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.processor import materialize_filtered_hour
from pmxt_relay.processor import materialize_partition_dir
from pmxt_relay.storage import parse_archive_hour
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


def _read_cpu_totals() -> tuple[int, int]:
    with Path("/proc/stat").open() as handle:
        parts = handle.readline().split()
    values = [int(value) for value in parts[1:]]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return idle, total


def _sample_cpu_percent(interval_secs: float = 0.1) -> float:
    idle_before, total_before = _read_cpu_totals()
    time_module.sleep(interval_secs)
    idle_after, total_after = _read_cpu_totals()
    total_delta = max(1, total_after - total_before)
    idle_delta = max(0, idle_after - idle_before)
    used_fraction = 1.0 - min(1.0, idle_delta / total_delta)
    return max(0.0, min(100.0, used_fraction * 100.0))


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
    return {
        "cpu_percent": round(_sample_cpu_percent(), 1),
        "mem_percent": round(_memory_percent(), 1),
        "disk_percent": round(_disk_percent(config.data_dir), 1),
    }


def _system_badge_payload(label: str, percent: float) -> dict[str, object]:
    return _badge_payload(
        label=label,
        message=f"{percent:.1f}%",
        color=_usage_color(percent),
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
            label="PMXT backfill", message="0/0 hrs", color="lightgrey"
        )

    progress = processed_hours / archive_hours
    if progress >= 1.0:
        color = "brightgreen"
    elif progress >= 0.5:
        color = "green"
    elif progress >= 0.1:
        color = "yellowgreen"
    else:
        color = "orange"

    return _badge_payload(
        label="PMXT backfill",
        message=f"{processed_hours}/{archive_hours} hrs",
        color=color,
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
        label="PMXT latest",
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
        label="PMXT lag",
        message=f"{lag_hours} hrs",
        color=color,
    )


class RequestRateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        self._requests_per_minute = requests_per_minute
        self._requests: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, client_id: str, *, now: float | None = None) -> bool:
        if self._requests_per_minute <= 0:
            return True

        current = time.monotonic() if now is None else now
        window_start = current - 60.0
        bucket = self._requests[client_id]
        while bucket and bucket[0] <= window_start:
            bucket.popleft()

        if len(bucket) >= self._requests_per_minute:
            return False

        bucket.append(current)
        return True

    def bucket_size(self, client_id: str, *, now: float | None = None) -> int:
        current = time.monotonic() if now is None else now
        window_start = current - 60.0
        bucket = self._requests[client_id]
        while bucket and bucket[0] <= window_start:
            bucket.popleft()
        return len(bucket)


CONFIG_APP_KEY = web.AppKey("config", RelayConfig)
INDEX_APP_KEY = web.AppKey("index", RelayIndex)
RATE_LIMITER_APP_KEY = web.AppKey("rate_limiter", RequestRateLimiter)
MATERIALIZE_LOCKS_APP_KEY = web.AppKey(
    "materialize_locks",
    dict[str, asyncio.Lock],
)


def _client_id(request: web.Request) -> str:
    if request.remote:
        return request.remote
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

    limiter = request.app[RATE_LIMITER_APP_KEY]
    client_id = _client_id(request)
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


async def stats(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(index.stats())


async def queue(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(index.queue_summary())


async def events(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    limit_value = request.query.get("limit", "100").strip()
    try:
        limit = max(1, min(1000, int(limit_value)))
    except ValueError:
        limit = 100
    rows = index.recent_events(limit=limit)
    payload = []
    for row in rows:
        payload.append(
            {
                "id": row["id"],
                "created_at": row["created_at"],
                "level": row["level"],
                "event_type": row["event_type"],
                "filename": row["filename"],
                "message": row["message"],
                "payload": json.loads(row["payload_json"])
                if row["payload_json"] is not None
                else None,
            }
        )
    return web.json_response({"events": payload})


async def inflight(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    return web.json_response({"inflight": _collect_inflight_processes(config)})


async def system_metrics(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    payload = await asyncio.to_thread(_system_metrics_snapshot, config)
    return web.json_response(payload)


async def badge_status(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return web.json_response(_status_badge_payload(stats=index.stats(), config=config))


async def badge_backfill(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(_backfill_badge_payload(stats=index.stats()))


async def badge_latest(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(
        _latest_processed_badge_payload(queue=index.queue_summary())
    )


async def badge_lag(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return web.json_response(_lag_badge_payload(stats=index.stats()))


async def badge_cpu_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _system_badge_payload("Relay CPU", float(metrics["cpu_percent"]))
    )


async def badge_mem_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _system_badge_payload("Relay mem", float(metrics["mem_percent"]))
    )


async def badge_disk_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    metrics = await asyncio.to_thread(_system_metrics_snapshot, config)
    return _badge_svg_response(
        _system_badge_payload("Relay disk", float(metrics["disk_percent"]))
    )


async def badge_status_svg(request: web.Request) -> web.Response:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _status_badge_payload(stats=index.stats(), config=config)
    )


async def badge_backfill_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(_backfill_badge_payload(stats=index.stats()))


async def badge_latest_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(
        _latest_processed_badge_payload(queue=index.queue_summary())
    )


async def badge_lag_svg(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    return _badge_svg_response(_lag_badge_payload(stats=index.stats()))


async def list_filtered_hours(request: web.Request) -> web.Response:
    index = request.app[INDEX_APP_KEY]
    config = request.app[CONFIG_APP_KEY]
    condition_id = request.match_info["condition_id"]
    token_id = request.match_info["token_id"]
    if not _CONDITION_ID_RE.fullmatch(condition_id) or not _TOKEN_ID_RE.fullmatch(
        token_id
    ):
        raise web.HTTPNotFound(text="filtered hours not found")
    start_hour = _iso_hour_query(request.query.get("start"))
    end_hour = _iso_hour_query(request.query.get("end"))
    rows = index.list_filtered_hours(
        condition_id,
        token_id,
        start_hour=start_hour,
        end_hour=end_hour,
    )
    if not rows:
        rows = _scan_filtered_hours(
            config,
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
        relative_url = f"/v1/filtered/{condition_id}/{token_id}/{row['filename']}"
        url = (
            f"{config.public_base_url}{relative_url}"
            if config.public_base_url is not None
            else relative_url
        )
        entries.append(
            {
                "hour": row["hour"],
                "filename": row["filename"],
                "row_count": row["row_count"],
                "byte_size": row["byte_size"],
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


def _scan_filtered_hours(
    config: RelayConfig,
    condition_id: str,
    token_id: str,
    *,
    start_hour: str | None = None,
    end_hour: str | None = None,
) -> list[dict[str, object]]:
    token_root = config.filtered_root / condition_id / token_id
    if not token_root.exists():
        return []

    entries: list[dict[str, object]] = []
    for path in sorted(token_root.glob("polymarket_orderbook_*.parquet")):
        hour = parse_archive_hour(path.name).isoformat()
        if start_hour is not None and hour < start_hour:
            continue
        if end_hour is not None and hour > end_hour:
            continue
        try:
            byte_size = path.stat().st_size
        except FileNotFoundError:
            continue
        entries.append(
            {
                "filename": path.name,
                "hour": hour,
                "row_count": None,
                "byte_size": byte_size,
            }
        )
    return entries


async def serve_filtered(request: web.Request) -> web.StreamResponse:
    config = request.app[CONFIG_APP_KEY]
    index = request.app[INDEX_APP_KEY]
    condition_id = request.match_info["condition_id"]
    token_id = request.match_info["token_id"]
    filename = request.match_info["filename"]
    cache_path = _resolve_filtered_path(config, condition_id, token_id, filename)
    if cache_path is None:
        raise web.HTTPNotFound(text="filtered hour not found")

    if cache_path.exists():
        response = web.FileResponse(cache_path)
        response.headers["Cache-Control"] = _CACHE_CONTROL_FILE
        return response

    row = index.get_filtered_hour(condition_id, token_id, filename)
    if row is not None:
        local_path = Path(row["local_path"])
    else:
        local_path = config.processed_root / processed_relative_path(filename)
        if not local_path.exists():
            legacy_partition_dir = (
                config.processed_root / filename / condition_id / token_id
            )
            local_path = legacy_partition_dir
            if not local_path.exists():
                raise web.HTTPNotFound(text="filtered hour not found")

    if local_path.is_file():
        materialize_locks = request.app[MATERIALIZE_LOCKS_APP_KEY]
        lock = materialize_locks.setdefault(str(cache_path), asyncio.Lock())
        async with lock:
            if not cache_path.exists():
                try:
                    await asyncio.to_thread(
                        materialize_filtered_hour,
                        local_path,
                        cache_path,
                        condition_id=condition_id,
                        token_id=token_id,
                    )
                except FileNotFoundError as exc:
                    raise web.HTTPNotFound(text="filtered hour not found") from exc
        response = web.FileResponse(cache_path)
        response.headers["Cache-Control"] = _CACHE_CONTROL_FILE
        return response
    if not local_path.is_dir():
        raise web.HTTPNotFound(text="filtered hour not found")

    materialize_locks = request.app[MATERIALIZE_LOCKS_APP_KEY]
    lock = materialize_locks.setdefault(str(cache_path), asyncio.Lock())
    async with lock:
        if not cache_path.exists():
            try:
                await asyncio.to_thread(
                    materialize_partition_dir, local_path, cache_path
                )
            except FileNotFoundError as exc:
                raise web.HTTPNotFound(text="filtered hour not found") from exc

    response = web.FileResponse(cache_path)
    response.headers["Cache-Control"] = _CACHE_CONTROL_FILE
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
    app[INDEX_APP_KEY] = RelayIndex(
        config.db_path, event_retention=config.event_retention
    )
    app[RATE_LIMITER_APP_KEY] = RequestRateLimiter(config.api_rate_limit_per_minute)
    app[MATERIALIZE_LOCKS_APP_KEY] = {}
    app[INDEX_APP_KEY].initialize()
    app.on_response_prepare.append(on_prepare_response)
    app.router.add_get("/healthz", healthz)
    app.router.add_get("/v1/stats", stats)
    app.router.add_get("/v1/queue", queue)
    app.router.add_get("/v1/events", events)
    app.router.add_get("/v1/inflight", inflight)
    app.router.add_get("/v1/system", system_metrics)
    app.router.add_get("/v1/badge/status", badge_status)
    app.router.add_get("/v1/badge/backfill", badge_backfill)
    app.router.add_get("/v1/badge/latest", badge_latest)
    app.router.add_get("/v1/badge/lag", badge_lag)
    app.router.add_get("/v1/badge/status.svg", badge_status_svg)
    app.router.add_get("/v1/badge/backfill.svg", badge_backfill_svg)
    app.router.add_get("/v1/badge/latest.svg", badge_latest_svg)
    app.router.add_get("/v1/badge/lag.svg", badge_lag_svg)
    app.router.add_get("/v1/badge/cpu.svg", badge_cpu_svg)
    app.router.add_get("/v1/badge/mem.svg", badge_mem_svg)
    app.router.add_get("/v1/badge/disk.svg", badge_disk_svg)
    app.router.add_get(
        "/v1/markets/{condition_id}/tokens/{token_id}/hours",
        list_filtered_hours,
    )
    app.router.add_get(
        "/v1/filtered/{condition_id}/{token_id}/{filename}",
        serve_filtered,
    )
    if config.expose_raw:
        app.router.add_get("/v1/raw/{filename:.*}", serve_raw)
    return app
