from __future__ import annotations

from pathlib import Path

from pmxt_relay.clickhouse import ClickHouseRelay
from pmxt_relay.config import RelayConfig


def _make_config(tmp_path: Path) -> RelayConfig:
    return RelayConfig(
        data_dir=tmp_path,
        bind_host="127.0.0.1",
        bind_port=8080,
        public_base_url=None,
        archive_listing_url="https://archive.pmxt.dev/data/Polymarket",
        raw_base_url="https://r2.pmxt.dev",
        poll_interval_secs=900,
        http_timeout_secs=30,
        archive_stale_pages=3,
        archive_max_pages=None,
        duckdb_threads=1,
        duckdb_memory_limit="1GB",
        expose_raw=False,
        event_retention=1000,
        api_rate_limit_per_minute=2400,
        api_list_max_hours=2000,
    )


class _FakeResponse:
    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        return False

    def read(self) -> bytes:
        return b""


def test_ensure_schema_bootstraps_database_without_db_scoped_endpoint(
    tmp_path: Path,
    monkeypatch,
) -> None:
    relay = ClickHouseRelay(_make_config(tmp_path))
    requests: list[tuple[str, bytes]] = []

    def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
        requests.append((request.full_url, request.data))
        assert timeout == relay._config.clickhouse_timeout_secs  # noqa: SLF001
        return _FakeResponse()

    monkeypatch.setattr("pmxt_relay.clickhouse.urlopen", fake_urlopen)

    relay.ensure_schema()

    assert len(requests) == 3
    assert requests[0][0] == "http://127.0.0.1:8123/?date_time_input_format=best_effort"
    assert requests[0][1] == b"CREATE DATABASE IF NOT EXISTS pmxt_relay"
    assert requests[1][0] == (
        "http://127.0.0.1:8123/?database=pmxt_relay&date_time_input_format=best_effort"
    )
    assert b"CREATE TABLE IF NOT EXISTS pmxt_relay.filtered_updates" in requests[1][1]
    assert requests[2][0] == (
        "http://127.0.0.1:8123/?database=pmxt_relay&date_time_input_format=best_effort"
    )
    assert (
        b"CREATE TABLE IF NOT EXISTS pmxt_relay.filtered_updates_hours"
        in requests[2][1]
    )


def test_hour_exists_requires_completion_marker(tmp_path: Path, monkeypatch) -> None:
    relay = ClickHouseRelay(_make_config(tmp_path))
    queries: list[str] = []

    def fake_execute_query(query: str, **kwargs) -> bytes:  # type: ignore[no-untyped-def]
        del kwargs
        queries.append(query)
        return b"0\n"

    monkeypatch.setattr(relay, "_execute_query", fake_execute_query)

    assert relay.hour_exists("polymarket_orderbook_2026-02-21T18.parquet") is False
    assert "FROM pmxt_relay.filtered_updates_hours" in queries[0]
