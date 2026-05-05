from __future__ import annotations

from io import StringIO
import json

import pytest

import prediction_market_extensions._runtime_log as runtime_log
from prediction_market_extensions._runtime_log import (
    CaptureEventSink,
    JsonlEventSink,
    LoaderEvent,
    TRACE_JSONL_ENV,
    capture_loader_events,
    configure_loader_event_sinks_from_env,
    emit_loader_event,
    format_log_line,
    format_utc_timestamp_ns,
    get_loader_event_sinks,
    loader_event_sinks_from_env,
    log_info,
    log_message,
    set_loader_event_sinks,
)


def test_format_utc_timestamp_ns_preserves_nanoseconds() -> None:
    assert format_utc_timestamp_ns(0) == "1970-01-01T00:00:00.000000000Z"
    assert format_utc_timestamp_ns(1_774_092_445_353_784_800) == ("2026-03-21T11:27:25.353784800Z")


def test_format_log_line_includes_severity_origin_and_message() -> None:
    line = format_log_line(
        "Information here",
        level="INFO",
        origin="file_origin.function_name",
        timestamp_ns=1_774_092_445_353_784_800,
    )

    assert line == (
        "2026-03-21T11:27:25.353784800Z [INFO] file_origin.function_name: Information here"
    )


def test_format_log_line_colors_warnings_and_errors() -> None:
    warning = format_log_line(
        "Careful",
        level="WARNING",
        origin="demo.warn",
        timestamp_ns=0,
    )
    error = format_log_line(
        "Broken",
        level="ERROR",
        origin="demo.error",
        timestamp_ns=0,
    )

    assert warning == (
        "\033[1;33m1970-01-01T00:00:00.000000000Z [WARNING] demo.warn: Careful\033[0m"
    )
    assert error == ("\033[1;31m1970-01-01T00:00:00.000000000Z [ERROR] demo.error: Broken\033[0m")


def test_log_message_rejects_unknown_levels() -> None:
    with pytest.raises(ValueError, match="Unsupported log level"):
        format_log_line("bad", level="NOTICE", origin="demo.test", timestamp_ns=0)


def test_log_info_writes_timestamped_lines() -> None:
    stream = StringIO()

    log_message(
        "first\nsecond",
        level="INFO",
        origin="demo.loader",
        stream=stream,
        clock_ns=lambda: 1_774_092_445_353_784_800,
    )

    assert stream.getvalue().splitlines() == [
        "2026-03-21T11:27:25.353784800Z [INFO] demo.loader: first",
        "2026-03-21T11:27:25.353784800Z [INFO] demo.loader: second",
    ]


def test_log_info_can_infer_caller_origin(capsys) -> None:
    log_info("hello")

    output = capsys.readouterr().err
    assert "[INFO] test_runtime_log.test_log_info_can_infer_caller_origin: hello" in output


def test_standard_console_sink_uses_tqdm_safe_write(monkeypatch, capsys) -> None:
    calls: list[tuple[str, object]] = []

    def fake_tqdm_write_line(line: str, *, stream) -> bool:  # type: ignore[no-untyped-def]
        calls.append((line, stream))
        stream.write(line + "\n")
        return True

    monkeypatch.setattr(runtime_log, "_tqdm_write_line", fake_tqdm_write_line)

    log_message(
        "progress-safe",
        level="INFO",
        origin="demo.loader",
        clock_ns=lambda: 1_774_092_445_353_784_800,
    )

    captured = capsys.readouterr()

    assert len(calls) == 1
    assert calls[0][0] == ("2026-03-21T11:27:25.353784800Z [INFO] demo.loader: progress-safe")
    assert calls[0][1] is runtime_log.sys.stderr
    assert captured.out == ""
    assert captured.err == ("2026-03-21T11:27:25.353784800Z [INFO] demo.loader: progress-safe\n")


def test_custom_console_stream_uses_plain_write(monkeypatch) -> None:
    stream = StringIO()

    def fail_tqdm_write_line(line: str, *, stream) -> bool:  # type: ignore[no-untyped-def]
        raise AssertionError("custom streams should not use tqdm.write")

    monkeypatch.setattr(runtime_log, "_tqdm_write_line", fail_tqdm_write_line)

    log_message(
        "plain stream",
        level="INFO",
        origin="demo.loader",
        stream=stream,
        clock_ns=lambda: 1_774_092_445_353_784_800,
    )

    assert stream.getvalue() == (
        "2026-03-21T11:27:25.353784800Z [INFO] demo.loader: plain stream\n"
    )


def test_emit_loader_event_captures_structured_fields() -> None:
    sink = CaptureEventSink()

    emit_loader_event(
        "loaded cache",
        level="INFO",
        stage="cache_read",
        vendor="pmxt",
        status="cache_hit",
        origin="pmxt.load",
        clock_ns=lambda: 1_774_092_445_353_784_800,
        sinks=[sink],
        platform="polymarket",
        data_type="book",
        source_kind="cache",
        source="cache:/tmp/demo.parquet",
        market_slug="demo-market",
        token_id="123",
        rows=25,
        elapsed_ms=1.5,
        attrs={"hour": "2026-03-21T11:00:00Z"},
    )

    assert sink.events == [
        LoaderEvent(
            level="INFO",
            message="loaded cache",
            origin="pmxt.load",
            timestamp_ns=1_774_092_445_353_784_800,
            stage="cache_read",
            vendor="pmxt",
            status="cache_hit",
            platform="polymarket",
            data_type="book",
            source_kind="cache",
            source="cache:/tmp/demo.parquet",
            market_slug="demo-market",
            token_id="123",
            rows=25,
            elapsed_ms=1.5,
            attrs={"hour": "2026-03-21T11:00:00Z"},
        )
    ]


def test_capture_loader_events_replaces_console_sink(capsys) -> None:
    with capture_loader_events() as capture:
        log_info("captured")

    output = capsys.readouterr().out

    assert output == ""
    assert len(capture.events) == 1
    assert capture.events[0].message == "captured"
    assert capture.events[0].level == "INFO"


def test_jsonl_event_sink_writes_structured_events(tmp_path) -> None:
    path = tmp_path / "trace.jsonl"

    emit_loader_event(
        "scan complete",
        level="INFO",
        stage="scan",
        vendor="telonex",
        status="complete",
        origin="telonex.scan",
        clock_ns=lambda: 1_774_092_445_353_784_800,
        sinks=[JsonlEventSink(path)],
        rows=12,
    )

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["timestamp"] == "2026-03-21T11:27:25.353784800Z"
    assert payload["level"] == "INFO"
    assert payload["origin"] == "telonex.scan"
    assert payload["stage"] == "scan"
    assert payload["vendor"] == "telonex"
    assert payload["status"] == "complete"
    assert payload["rows"] == 12


def test_loader_event_sinks_from_env_adds_jsonl_sink(tmp_path) -> None:
    path = tmp_path / "trace.jsonl"

    sinks = loader_event_sinks_from_env({TRACE_JSONL_ENV: str(path)})

    assert any(isinstance(sink, JsonlEventSink) and sink.path == str(path) for sink in sinks)


def test_configure_loader_event_sinks_from_env(tmp_path) -> None:
    path = tmp_path / "trace.jsonl"
    prior = get_loader_event_sinks()
    try:
        configure_loader_event_sinks_from_env({TRACE_JSONL_ENV: str(path)})

        emit_loader_event(
            "configured",
            origin="demo.configured",
            clock_ns=lambda: 1_774_092_445_353_784_800,
        )

        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["message"] == "configured"
    finally:
        set_loader_event_sinks(prior)
