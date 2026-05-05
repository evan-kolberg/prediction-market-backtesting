from __future__ import annotations

import inspect
import json
import os
import re
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol, TextIO
from urllib.parse import urlparse

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR"]

TRACE_JSONL_ENV = "PREDICTION_MARKET_TRACE_JSONL"
LOADER_PROGRESS_ENV = "BACKTEST_LOADER_PROGRESS"
LOADER_PROGRESS_LOGS_ENV = "BACKTEST_LOADER_PROGRESS_LOGS"
LOADER_PROGRESS_LOG_INTERVAL_ENV = "BACKTEST_LOADER_PROGRESS_LOG_INTERVAL"
BACKTEST_ENABLE_TIMING_ENV = "BACKTEST_ENABLE_TIMING"
_VALID_LEVELS: frozenset[str] = frozenset({"DEBUG", "INFO", "WARNING", "ERROR"})
_LOG_LOCK = threading.RLock()
_PROGRESS_LOG_LOCK = threading.Lock()
_PROGRESS_LOG_STATE: dict[tuple[int, str, str, str], float] = {}
_DEFAULT_PROGRESS_LOG_INTERVAL_SECS = 2.0
_ANSI_RESET = "\033[0m"
_ANSI_BOLD_RED = "\033[1;31m"
_ANSI_BOLD_YELLOW = "\033[1;33m"
_LOG_LINE_STYLE_BY_LEVEL = {
    "ERROR": _ANSI_BOLD_RED,
    "WARNING": _ANSI_BOLD_YELLOW,
}


def format_utc_timestamp_ns(epoch_ns: int) -> str:
    seconds, nanos = divmod(int(epoch_ns), 1_000_000_000)
    base = datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base}.{nanos:09d}Z"


def _normalize_level(level: str) -> str:
    normalized_level = level.strip().upper()
    if normalized_level not in _VALID_LEVELS:
        raise ValueError(f"Unsupported log level {level!r}. Use one of: {sorted(_VALID_LEVELS)}.")
    return normalized_level


def _caller_origin(*, stacklevel: int) -> str:
    frame = inspect.currentframe()
    for _ in range(stacklevel):
        if frame is None:
            break
        frame = frame.f_back
    if frame is None:
        return "unknown.unknown"

    filename = Path(frame.f_code.co_filename).stem or "unknown"
    function_name = frame.f_code.co_name or "unknown"
    return f"{filename}.{function_name}"


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray):
        return [_json_safe(inner) for inner in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, bool | int | float | str) or value is None:
        return value
    return str(value)


def _env_flag_enabled(value: str | None, *, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().casefold() not in {"0", "false", "no", "off"}


def loader_progress_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return _env_flag_enabled(env.get(LOADER_PROGRESS_ENV), default=True)


def loader_progress_logs_enabled(environ: Mapping[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return (
        _env_flag_enabled(env.get(BACKTEST_ENABLE_TIMING_ENV), default=True)
        and loader_progress_enabled(env)
        and _env_flag_enabled(env.get(LOADER_PROGRESS_LOGS_ENV), default=True)
    )


def _progress_log_interval_secs(environ: Mapping[str, str] | None = None) -> float:
    env = os.environ if environ is None else environ
    configured = env.get(LOADER_PROGRESS_LOG_INTERVAL_ENV)
    if configured is None:
        return _DEFAULT_PROGRESS_LOG_INTERVAL_SECS
    try:
        return max(0.1, float(configured))
    except ValueError:
        return _DEFAULT_PROGRESS_LOG_INTERVAL_SECS


@dataclass(frozen=True)
class LoaderEvent:
    level: str
    message: str
    origin: str
    timestamp_ns: int
    stage: str = "runtime"
    vendor: str = "repo"
    status: str = "complete"
    platform: str | None = None
    data_type: str | None = None
    source_kind: str | None = None
    source: str | None = None
    cache_path: str | None = None
    market_id: str | None = None
    market_slug: str | None = None
    token_id: str | None = None
    condition_id: str | None = None
    outcome: str | None = None
    window_start_ns: int | None = None
    window_end_ns: int | None = None
    rows: int | None = None
    book_events: int | None = None
    trade_ticks: int | None = None
    bytes: int | None = None
    elapsed_ms: float | None = None
    attempt: int | None = None
    attrs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "level", _normalize_level(self.level))
        object.__setattr__(self, "message", str(self.message))
        object.__setattr__(self, "origin", str(self.origin or "unknown.unknown"))
        object.__setattr__(self, "stage", str(self.stage or "runtime"))
        object.__setattr__(self, "vendor", str(self.vendor or "repo"))
        object.__setattr__(self, "status", str(self.status or "complete"))
        object.__setattr__(self, "timestamp_ns", int(self.timestamp_ns))

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "timestamp_ns": self.timestamp_ns,
            "timestamp": format_utc_timestamp_ns(self.timestamp_ns),
            "level": self.level,
            "origin": self.origin,
            "stage": self.stage,
            "vendor": self.vendor,
            "status": self.status,
            "message": self.message,
        }
        for key in (
            "platform",
            "data_type",
            "source_kind",
            "source",
            "cache_path",
            "market_id",
            "market_slug",
            "token_id",
            "condition_id",
            "outcome",
            "window_start_ns",
            "window_end_ns",
            "rows",
            "book_events",
            "trade_ticks",
            "bytes",
            "elapsed_ms",
            "attempt",
        ):
            value = getattr(self, key)
            if value is not None:
                payload[key] = value
        if self.attrs:
            payload["attrs"] = _json_safe(self.attrs)
        return payload


class LoaderEventSink(Protocol):
    def emit(self, event: LoaderEvent) -> None: ...


_STRUCTURED_CONSOLE_VENDORS = frozenset({"pmxt", "telonex", "polymarket"})
_VENDOR_LABELS = {
    "pmxt": "PMXT",
    "telonex": "Telonex",
    "polymarket": "Polymarket",
}
_STATUS_LABELS = {
    "cache_hit": "cache hit",
    "cache_miss": "cache miss",
}


def _format_status(value: str) -> str:
    return _STATUS_LABELS.get(value, value.replace("_", " "))


def _format_source_kind(event: LoaderEvent) -> str | None:
    value = event.source_kind
    if value is None:
        return None
    if value == "remote":
        return "archive" if event.vendor == "pmxt" else "api"
    return value.replace("_", " ")


def _format_elapsed_ms(elapsed_ms: float | None) -> str | None:
    if elapsed_ms is None:
        return None
    return f"({elapsed_ms / 1000.0:.3f}s)"


def _format_int_count(value: int | None, label: str) -> str | None:
    if value is None:
        return None
    return f"({value:,} {label})"


def _format_bytes(value: int | None) -> str | None:
    if value is None:
        return None
    units = ("B", "KB", "MB", "GB", "TB")
    size = float(value)
    unit = units[0]
    for unit in units:
        if abs(size) < 1024.0 or unit == units[-1]:
            break
        size /= 1024.0
    if unit == "B":
        return f"({int(size):,} {unit})"
    return f"({size:.1f} {unit})"


def _format_progress_bytes(value: int | None) -> str:
    formatted = _format_bytes(value)
    return "?" if formatted is None else formatted.removeprefix("(").removesuffix(")")


def _progress_source_time_label(source: str) -> str | None:
    for pattern in (r"\d{4}-\d{2}-\d{2}T\d{2}", r"\d{4}-\d{2}-\d{2}"):
        match = re.search(pattern, source)
        if match is not None:
            return match.group(0)
    target = source.partition("::")[2] if "::" in source else source
    if target.startswith("archive:"):
        target = target.removeprefix("archive:")
    parsed = urlparse(target)
    path = parsed.path if parsed.scheme else target
    filename = Path(path).name
    return filename or None


def _infer_progress_source_kind(vendor: str, source: str, source_kind: str | None) -> str | None:
    if source_kind is not None:
        return source_kind
    if source.startswith("cache") or "cache::" in source:
        return "cache"
    if source.startswith("telonex-local::") or source.startswith("local:"):
        return "local"
    if source.startswith("telonex-api::"):
        return "remote"
    if source.startswith(("http://", "https://", "archive:")):
        return "remote"
    if vendor == "pmxt" and "r2" in source:
        return "remote"
    return "local" if source else None


def _progress_source_kind_label(vendor: str, source_kind: str | None) -> str | None:
    if source_kind == "remote":
        return "archive" if vendor == "pmxt" else "api"
    return source_kind.replace("_", " ") if source_kind is not None else None


def _progress_source_label(vendor: str, source: str, source_kind: str | None) -> str:
    kind_label = _progress_source_kind_label(vendor, source_kind)
    if not kind_label:
        return source
    if source.startswith(f"{kind_label}:"):
        return source
    if kind_label == "api" and source.startswith("telonex-api::"):
        return source
    if kind_label == "archive" and source.startswith("archive:"):
        return source
    return f"{kind_label} {source}"


def _progress_message(
    *,
    vendor: str,
    mode: str,
    source: str,
    source_kind: str | None,
    downloaded_bytes: int | None,
    total_bytes: int | None,
    scanned_batches: int | None,
    scanned_rows: int | None,
    matched_rows: int | None,
    finished: bool,
) -> str:
    vendor_label = _VENDOR_LABELS.get(vendor, vendor.upper())
    status = "complete" if finished else "progress"
    parts = [vendor_label, "book", mode, status]
    time_label = _progress_source_time_label(source)
    if time_label:
        parts.append(time_label)

    if mode == "download":
        amount = f"{_format_progress_bytes(downloaded_bytes)}/{_format_progress_bytes(total_bytes)}"
        if total_bytes:
            percent = (float(downloaded_bytes or 0) / float(total_bytes)) * 100.0
            amount = f"{amount} ({percent:.1f}%)"
        parts.append(amount)
    else:
        if total_bytes is not None:
            parts.append(f"file {_format_progress_bytes(total_bytes)}")
        if scanned_batches is not None:
            parts.append(f"{scanned_batches:,} batches")
        if scanned_rows is not None:
            parts.append(f"{scanned_rows:,} scanned rows")
        if matched_rows is not None:
            parts.append(f"{matched_rows:,} matched rows")

    parts.append(_progress_source_label(vendor, source, source_kind))
    return " ".join(parts)


def emit_loader_progress_snapshot(
    *,
    owner: object,
    vendor: str,
    mode: str,
    source: str,
    source_kind: str | None = None,
    downloaded_bytes: int | None = None,
    total_bytes: int | None = None,
    scanned_batches: int | None = None,
    scanned_rows: int | None = None,
    matched_rows: int | None = None,
    finished: bool = False,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Emit throttled line progress for environments that do not render tqdm."""
    if not loader_progress_logs_enabled():
        return

    normalized_vendor = vendor.strip().casefold()
    normalized_mode = mode.strip().casefold().replace("_", " ")
    normalized_source = str(source)
    resolved_source_kind = _infer_progress_source_kind(
        normalized_vendor, normalized_source, source_kind
    )
    key = (id(owner), normalized_vendor, normalized_mode, normalized_source)
    now = clock()
    with _PROGRESS_LOG_LOCK:
        last_emit = _PROGRESS_LOG_STATE.get(key)
        should_emit = (
            finished or last_emit is None or (now - last_emit) >= _progress_log_interval_secs()
        )
        if not should_emit:
            return
        if finished:
            _PROGRESS_LOG_STATE.pop(key, None)
        else:
            _PROGRESS_LOG_STATE[key] = now

    attrs: dict[str, object] = {"mode": normalized_mode}
    if scanned_batches is not None:
        attrs["scanned_batches"] = scanned_batches
    if scanned_rows is not None:
        attrs["scanned_rows"] = scanned_rows
    if total_bytes is not None:
        attrs["total_bytes"] = total_bytes
    time_label = _progress_source_time_label(normalized_source)
    if time_label is not None:
        attrs["hour"] = time_label

    emit_loader_event(
        _progress_message(
            vendor=normalized_vendor,
            mode=normalized_mode,
            source=normalized_source,
            source_kind=resolved_source_kind,
            downloaded_bytes=downloaded_bytes,
            total_bytes=total_bytes,
            scanned_batches=scanned_batches,
            scanned_rows=scanned_rows,
            matched_rows=matched_rows,
            finished=finished,
        ),
        level="INFO",
        stage="runtime",
        status="complete" if finished else "progress",
        vendor=normalized_vendor,
        platform="polymarket",
        data_type="book",
        source_kind=resolved_source_kind,
        source=normalized_source,
        rows=matched_rows if matched_rows is not None else scanned_rows,
        bytes=downloaded_bytes,
        attrs=attrs,
        stacklevel=3,
    )


def _event_time_label(event: LoaderEvent) -> str | None:
    for key in ("date", "day", "hour"):
        value = event.attrs.get(key)
        if value is not None:
            return str(value)
    return None


def _event_request_count(event: LoaderEvent) -> int | None:
    for key in ("request_count", "requests"):
        value = event.attrs.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    return None


def _event_error(event: LoaderEvent) -> str | None:
    value = event.attrs.get("error")
    if value is None:
        return None
    return str(value)


def _event_reason(event: LoaderEvent) -> str | None:
    value = event.attrs.get("reason")
    if value is None:
        return None
    return str(value)


def _event_count_label(event: LoaderEvent) -> str | None:
    if event.status == "start":
        return None
    if event.rows is not None:
        return _format_int_count(event.rows, "rows")
    if event.book_events is not None:
        return _format_int_count(event.book_events, "book events")
    if event.trade_ticks is not None:
        return _format_int_count(event.trade_ticks, "trades")
    return None


def _event_location_label(event: LoaderEvent) -> str | None:
    source_label = event.attrs.get("source_label")
    if source_label is not None:
        return str(source_label)
    source_kind = _format_source_kind(event)
    if event.stage == "raw_write" and event.source and event.cache_path:
        return f"{event.source} -> {event.cache_path}"
    if event.cache_path:
        label = (
            "cache" if event.source_kind == "cache" or event.stage.startswith("cache") else "file"
        )
        return f"{label} {event.cache_path}"
    if event.source:
        if source_kind and event.source.startswith(f"{source_kind}:"):
            return event.source
        if source_kind == "archive" and event.source.startswith("archive:"):
            return event.source
        return f"{source_kind} {event.source}" if source_kind else event.source
    return source_kind


def _event_operation_label(event: LoaderEvent) -> str:
    status = _format_status(event.status)
    if event.trade_ticks is not None:
        return "trades" if event.status == "complete" else f"trades {status}"
    if event.stage == "cache_read":
        return status if event.status in {"cache_hit", "cache_miss"} else f"cache read {status}"
    if event.stage == "cache_write":
        return f"cache write {status}"
    if event.stage == "raw_write":
        return f"raw copy {status}"
    if event.stage == "fetch":
        if event.data_type == "metadata":
            return f"fetch {status}"
        source_kind = _format_source_kind(event)
        if source_kind is not None:
            return f"{source_kind} {status}"
        return status
    if event.stage == "discover":
        return f"discover {status}"
    if event.stage == "runtime":
        return status
    return f"{event.stage.replace('_', ' ')} {status}"


def _should_format_loader_event(event: LoaderEvent) -> bool:
    if event.vendor not in _STRUCTURED_CONSOLE_VENDORS:
        return False
    if event.vendor == "polymarket" and event.trade_ticks is None and event.data_type != "metadata":
        return False
    if event.stage == "runtime":
        return False
    return any(
        (
            event.source_kind is not None,
            event.source is not None,
            event.cache_path is not None,
            event.rows is not None,
            event.book_events is not None,
            event.trade_ticks is not None,
            event.bytes is not None,
            event.elapsed_ms is not None,
            _event_time_label(event) is not None,
        )
    )


def format_loader_event_message(event: LoaderEvent) -> str:
    if not _should_format_loader_event(event):
        return event.message

    vendor = _VENDOR_LABELS.get(event.vendor, event.vendor.upper())
    noun = "trades" if event.trade_ticks is not None else str(event.data_type or "data")
    operation = _event_operation_label(event)
    if operation == noun:
        parts = [vendor, noun]
    elif operation.startswith(f"{noun} "):
        parts = [vendor, operation]
    else:
        parts = [vendor, noun, operation]

    for label in (
        _event_time_label(event),
        _format_elapsed_ms(event.elapsed_ms),
        _event_count_label(event),
        _format_bytes(event.bytes),
        _format_int_count(_event_request_count(event), "requests"),
        _event_location_label(event),
    ):
        if label:
            parts.append(label)

    error = _event_error(event)
    if error:
        parts.append(f"error={error}")
    reason = _event_reason(event)
    if reason:
        parts.append(f"reason={reason}")

    return " ".join(parts)


def _is_standard_stream(stream: TextIO) -> bool:
    return stream is sys.stdout or stream is sys.stderr


def _tqdm_write_line(line: str, *, stream: TextIO) -> bool:
    try:
        from tqdm import tqdm
    except Exception:
        return False

    tqdm.write(line, file=stream)
    stream.flush()
    return True


def _write_console_line(line: str, *, stream: TextIO) -> None:
    if _is_standard_stream(stream) and _tqdm_write_line(line, stream=stream):
        return
    print(line, file=stream, flush=True)


@dataclass
class ConsoleEventSink:
    stream: TextIO | None = None

    def emit(self, event: LoaderEvent) -> None:
        target = self.stream if self.stream is not None else sys.stderr
        _write_console_line(
            format_log_line(
                format_loader_event_message(event),
                level=event.level,
                origin=event.origin,
                timestamp_ns=event.timestamp_ns,
            ),
            stream=target,
        )


@dataclass
class JsonlEventSink:
    path: Path | str

    def emit(self, event: LoaderEvent) -> None:
        path = Path(self.path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event.to_dict(), sort_keys=True) + "\n")


@dataclass
class CaptureEventSink:
    events: list[LoaderEvent] = field(default_factory=list)

    def emit(self, event: LoaderEvent) -> None:
        self.events.append(event)


_DEFAULT_CONSOLE_SINK = ConsoleEventSink()


def loader_event_sinks_from_env(
    environ: Mapping[str, str] | None = None, *, include_console: bool = True
) -> tuple[LoaderEventSink, ...]:
    env = os.environ if environ is None else environ
    sinks: list[LoaderEventSink] = []
    if include_console:
        sinks.append(_DEFAULT_CONSOLE_SINK)
    trace_jsonl = str(env.get(TRACE_JSONL_ENV, "")).strip()
    if trace_jsonl:
        sinks.append(JsonlEventSink(trace_jsonl))
    return tuple(sinks)


_SINKS: list[LoaderEventSink] = list(loader_event_sinks_from_env())


def format_log_line(
    message: object,
    *,
    level: str,
    origin: str,
    timestamp_ns: int,
) -> str:
    normalized_level = _normalize_level(level)
    line = f"{format_utc_timestamp_ns(timestamp_ns)} [{normalized_level}] {origin}: {message}"
    style = _LOG_LINE_STYLE_BY_LEVEL.get(normalized_level)
    if style is None:
        return line
    return f"{style}{line}{_ANSI_RESET}"


def get_loader_event_sinks() -> tuple[LoaderEventSink, ...]:
    with _LOG_LOCK:
        return tuple(_SINKS)


def set_loader_event_sinks(sinks: Sequence[LoaderEventSink]) -> None:
    with _LOG_LOCK:
        _SINKS.clear()
        _SINKS.extend(sinks)


def register_loader_event_sink(sink: LoaderEventSink) -> None:
    with _LOG_LOCK:
        _SINKS.append(sink)


def configure_loader_event_sinks_from_env(environ: Mapping[str, str] | None = None) -> None:
    set_loader_event_sinks(loader_event_sinks_from_env(environ))


@contextmanager
def loader_event_sinks(sinks: Sequence[LoaderEventSink]) -> Iterator[None]:
    prior = get_loader_event_sinks()
    set_loader_event_sinks(sinks)
    try:
        yield
    finally:
        set_loader_event_sinks(prior)


@contextmanager
def capture_loader_events() -> Iterator[CaptureEventSink]:
    capture = CaptureEventSink()
    with loader_event_sinks([capture]):
        yield capture


def _emit_event(event: LoaderEvent, *, sinks: Sequence[LoaderEventSink] | None = None) -> None:
    with _LOG_LOCK:
        active_sinks = tuple(sinks) if sinks is not None else tuple(_SINKS)
        for sink in active_sinks:
            sink.emit(event)


def emit_loader_event(
    message: object,
    *,
    level: LogLevel = "INFO",
    stage: str = "runtime",
    vendor: str = "repo",
    status: str = "complete",
    origin: str | None = None,
    clock_ns: Callable[[], int] = time.time_ns,
    stacklevel: int = 2,
    sinks: Sequence[LoaderEventSink] | None = None,
    **fields: Any,
) -> None:
    resolved_origin = origin or _caller_origin(stacklevel=stacklevel)
    attrs = fields.pop("attrs", None)
    event_fields = {
        key: fields.pop(key)
        for key in tuple(fields)
        if key
        in {
            "platform",
            "data_type",
            "source_kind",
            "source",
            "cache_path",
            "market_id",
            "market_slug",
            "token_id",
            "condition_id",
            "outcome",
            "window_start_ns",
            "window_end_ns",
            "rows",
            "book_events",
            "trade_ticks",
            "bytes",
            "elapsed_ms",
            "attempt",
        }
    }
    merged_attrs: dict[str, Any] = {}
    if isinstance(attrs, Mapping):
        merged_attrs.update(attrs)
    elif attrs is not None:
        merged_attrs["attrs"] = attrs
    merged_attrs.update(fields)

    text = str(message)
    lines = text.splitlines() or [""]
    for line in lines:
        event = LoaderEvent(
            level=level,
            message=line,
            origin=resolved_origin,
            timestamp_ns=clock_ns(),
            stage=stage,
            vendor=vendor,
            status=status,
            attrs=merged_attrs,
            **event_fields,
        )
        _emit_event(event, sinks=sinks)


def log_message(
    message: object,
    *,
    level: LogLevel = "INFO",
    origin: str | None = None,
    stream: TextIO | None = None,
    clock_ns: Callable[[], int] = time.time_ns,
    stacklevel: int = 2,
) -> None:
    sinks = (ConsoleEventSink(stream=stream),) if stream is not None else None
    emit_loader_event(
        message,
        level=level,
        origin=origin,
        clock_ns=clock_ns,
        stacklevel=stacklevel + 1,
        sinks=sinks,
    )


def log_debug(message: object, *, origin: str | None = None, stacklevel: int = 2) -> None:
    log_message(message, level="DEBUG", origin=origin, stacklevel=stacklevel + 1)


def log_info(message: object, *, origin: str | None = None, stacklevel: int = 2) -> None:
    log_message(message, level="INFO", origin=origin, stacklevel=stacklevel + 1)


def log_warning(message: object, *, origin: str | None = None, stacklevel: int = 2) -> None:
    log_message(message, level="WARNING", origin=origin, stacklevel=stacklevel + 1)


def log_error(message: object, *, origin: str | None = None, stacklevel: int = 2) -> None:
    log_message(message, level="ERROR", origin=origin, stacklevel=stacklevel + 1)


def clone_event(event: LoaderEvent, **changes: Any) -> LoaderEvent:
    return replace(event, **changes)
