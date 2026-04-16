from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from aiohttp.test_utils import TestClient, TestServer

from pmxt_relay.api import (
    INDEX_APP_KEY,
    RequestRateLimiter,
    _client_id,
    _collect_inflight_downloads,
    _cpu_percent_from_loadavg,
    _resolve_raw_path,
    _status_badge_payload,
    _upstream_badge_payload,
    create_app,
)
from pmxt_relay.config import RelayConfig


def _make_config(tmp_path: Path) -> RelayConfig:
    return RelayConfig(
        data_dir=tmp_path,
        bind_host="127.0.0.1",
        bind_port=8080,
        archive_listing_url="https://archive.pmxt.dev/data/Polymarket",
        raw_base_url="https://r2.pmxt.dev",
        poll_interval_secs=900,
        http_timeout_secs=30,
        archive_stale_pages=3,
        archive_max_pages=None,
        event_retention=1000,
        api_rate_limit_per_minute=2400,
        verify_batch_size=50,
    )


def test_cpu_percent_uses_load_average():
    with (
        patch("pmxt_relay.api.os.cpu_count", return_value=4),
        patch("pmxt_relay.api.os.getloadavg", return_value=(3.5, 3.0, 2.5)),
    ):
        assert _cpu_percent_from_loadavg() == 87.5


def test_rate_limiter_enforces_sliding_window():
    limiter = RequestRateLimiter(requests_per_minute=2)

    assert limiter.allow("203.0.113.1", now=0.0) is True
    assert limiter.allow("203.0.113.1", now=10.0) is True
    assert limiter.allow("203.0.113.1", now=20.0) is False


def test_rate_limiter_periodically_prunes_stale_clients():
    limiter = RequestRateLimiter(requests_per_minute=2)

    assert limiter.allow("203.0.113.1", now=0.0) is True
    assert "203.0.113.1" in limiter._requests

    assert limiter.allow("198.51.100.7", now=61.0) is True
    assert "203.0.113.1" not in limiter._requests


def test_client_id_uses_forwarded_for_from_trusted_proxy():
    class _Transport:
        def get_extra_info(self, name: str):  # type: ignore[no-untyped-def]
            if name == "peername":
                return ("127.0.0.1", 12345)
            return None

    class _Request:
        remote = "127.0.0.1"
        headers = {"X-Forwarded-For": "198.51.100.7, 127.0.0.1"}
        transport = _Transport()

    assert _client_id(_Request(), trusted_proxy_ips=("127.0.0.1",)) == "198.51.100.7"


def test_raw_path_resolution_requires_known_archive_layout(tmp_path: Path):
    config = _make_config(tmp_path)

    safe_path = _resolve_raw_path(config, "2026/03/21/polymarket_orderbook_2026-03-21T12.parquet")
    blocked_path = _resolve_raw_path(config, "../../etc/passwd")

    assert safe_path == (
        config.raw_root / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    assert blocked_path is None


def test_collect_inflight_downloads_reports_raw_tmp_files(tmp_path: Path):
    config = _make_config(tmp_path)
    config.ensure_directories()
    tmp_file = (
        config.raw_root / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet.tmp"
    )
    tmp_file.parent.mkdir(parents=True, exist_ok=True)
    tmp_file.write_bytes(b"abc")

    inflight = _collect_inflight_downloads(config)

    assert len(inflight) == 1
    assert inflight[0]["filename"] == "polymarket_orderbook_2026-03-21T12.parquet"
    assert inflight[0]["file_count"] == 1
    assert inflight[0]["byte_size"] == 3


def test_raw_route_serves_mirrored_hour(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        raw_path = (
            config.raw_root / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
        )
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(b"raw-payload")

        app = create_app(config)
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get(
                "/v1/raw/2026/03/21/polymarket_orderbook_2026-03-21T12.parquet"
            )
            payload = await response.read()
        finally:
            await client.close()

        assert response.status == 200
        assert payload == b"raw-payload"

    asyncio.run(scenario())


def test_active_api_has_no_filtered_routes(tmp_path: Path):
    async def scenario() -> None:
        config = replace(_make_config(tmp_path))
        config.ensure_directories()
        app = create_app(config)
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get(
                "/v1/filtered/"
                + ("0x" + ("ab" * 32))
                + "/123456789/polymarket_orderbook_2026-03-21T12.parquet"
            )
        finally:
            await client.close()

        assert response.status == 404

    asyncio.run(scenario())


def test_active_api_has_no_processing_badge_routes(tmp_path: Path):
    async def scenario() -> None:
        config = replace(_make_config(tmp_path))
        config.ensure_directories()
        app = create_app(config)
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            processing_response = await client.get("/v1/badge/processing.svg")
            file_response = await client.get("/v1/badge/file.svg")
            rows_response = await client.get("/v1/badge/rows.svg")
        finally:
            await client.close()

        assert processing_response.status == 404
        assert file_response.status == 404
        assert rows_response.status == 404

    asyncio.run(scenario())


def test_events_route_tolerates_invalid_payload_json(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        app = create_app(config)
        index = app[INDEX_APP_KEY]
        with index._conn:
            index._conn.execute(
                """
                INSERT INTO relay_events (
                    created_at,
                    level,
                    event_type,
                    filename,
                    message,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "2026-03-21T12:00:00+00:00",
                    "INFO",
                    "mirror_complete",
                    "polymarket_orderbook_2026-03-21T12.parquet",
                    "Mirrored hour",
                    "{not-json",
                ),
            )

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get("/v1/events?limit=1")
            payload = await response.json()
        finally:
            await client.close()

        assert response.status == 200
        assert payload["events"][0]["payload"] == {"raw_payload": "{not-json"}

    asyncio.run(scenario())


def test_worker_badge_reflects_live_service_state(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        app = create_app(config)
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            with patch(
                "pmxt_relay.api._system_metrics_snapshot",
                return_value={
                    "services": {
                        "worker": {
                            "label": "Worker service",
                            "active_state": "failed",
                            "sub_state": "failed",
                        }
                    }
                },
            ):
                response = await client.get("/v1/badge/worker.svg")
                payload = await response.text()
        finally:
            await client.close()

        assert response.status == 200
        assert "Worker service" in payload
        assert "failed" in payload

    asyncio.run(scenario())


def test_status_badge_uses_up_for_healthy_mirror_only_relay(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _status_badge_payload(
        stats={"last_event_at": (now - timedelta(minutes=5)).isoformat(), "last_error_at": None},
        system={
            "services": {"api": {"active_state": "active"}, "worker": {"active_state": "active"}}
        },
        config=config,
        now=now,
    )

    assert payload["message"] == "up"
    assert payload["color"] == "brightgreen"


def test_status_badge_uses_stale_for_old_last_event(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _status_badge_payload(
        stats={"last_event_at": (now - timedelta(hours=2)).isoformat(), "last_error_at": None},
        system={
            "services": {"api": {"active_state": "active"}, "worker": {"active_state": "active"}}
        },
        config=config,
        now=now,
    )

    assert payload["message"] == "stale"
    assert payload["color"] == "red"


def test_status_badge_ignores_fresh_upstream_errors_when_services_are_healthy(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _status_badge_payload(
        stats={
            "last_event_at": (now - timedelta(minutes=1)).isoformat(),
            "last_error_at": (now - timedelta(minutes=1)).isoformat(),
        },
        system={
            "services": {"api": {"active_state": "active"}, "worker": {"active_state": "active"}}
        },
        config=config,
        now=now,
    )

    assert payload["message"] == "up"
    assert payload["color"] == "brightgreen"


def test_status_badge_uses_degraded_for_inactive_service(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _status_badge_payload(
        stats={
            "last_event_at": (now - timedelta(minutes=1)).isoformat(),
            "last_error_at": (now - timedelta(minutes=1)).isoformat(),
        },
        system={
            "services": {
                "api": {"active_state": "active"},
                "worker": {"active_state": "deactivating"},
            }
        },
        config=config,
        now=now,
    )

    assert payload["message"] == "degraded"
    assert payload["color"] == "orange"


def test_upstream_badge_stays_online_for_fresh_unresolved_mirror_gaps(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _upstream_badge_payload(
        stats={
            "last_event_at": (now - timedelta(minutes=1)).isoformat(),
            "last_error_at": (now - timedelta(minutes=1)).isoformat(),
        },
        queue={
            "mirror_pending": 0,
            "mirror_active": 0,
            "mirror_error": 7,
            "latest_mirrored_hour": (now - timedelta(hours=1)).isoformat(),
        },
        config=config,
        now=now,
    )

    assert payload["label"] == "r2.pmxt.dev"
    assert payload["message"] == "online"
    assert payload["color"] == "brightgreen"


def test_upstream_badge_stays_online_for_backlog_with_old_latest_mirror(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _upstream_badge_payload(
        stats={"last_event_at": (now - timedelta(minutes=5)).isoformat(), "last_error_at": None},
        queue={
            "mirror_pending": 3,
            "mirror_active": 0,
            "mirror_error": 0,
            "latest_mirrored_hour": (now - timedelta(hours=12)).isoformat(),
        },
        config=config,
        now=now,
    )

    assert payload["label"] == "r2.pmxt.dev"
    assert payload["message"] == "online"
    assert payload["color"] == "brightgreen"


def test_upstream_badge_uses_offline_for_stale_polling(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _upstream_badge_payload(
        stats={"last_event_at": (now - timedelta(hours=2)).isoformat(), "last_error_at": None},
        queue={
            "mirror_pending": 0,
            "mirror_active": 0,
            "mirror_error": 0,
            "latest_mirrored_hour": (now - timedelta(hours=1)).isoformat(),
        },
        config=config,
        now=now,
    )

    assert payload["label"] == "r2.pmxt.dev"
    assert payload["message"] == "offline"
    assert payload["color"] == "red"


def test_latest_file_badge_reports_latest_mirrored_filename(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        app = create_app(config)
        index = app[INDEX_APP_KEY]
        index.upsert_discovered_hour(
            "polymarket_orderbook_2026-03-21T12.parquet",
            "https://raw.example.com/polymarket_orderbook_2026-03-21T12.parquet",
            1,
        )
        index.upsert_discovered_hour(
            "polymarket_orderbook_2026-03-21T13.parquet",
            "https://raw.example.com/polymarket_orderbook_2026-03-21T13.parquet",
            1,
        )
        index.mark_mirrored(
            "polymarket_orderbook_2026-03-21T12.parquet",
            local_path=str(tmp_path / "a.parquet"),
            etag=None,
            content_length=1,
            last_modified=None,
        )
        index.mark_mirrored(
            "polymarket_orderbook_2026-03-21T13.parquet",
            local_path=str(tmp_path / "b.parquet"),
            etag=None,
            content_length=1,
            last_modified=None,
        )

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get("/v1/badge/latest-file.svg")
            payload = await response.text()
        finally:
            await client.close()

        assert response.status == 200
        assert "Latest file" in payload
        assert "polymarket_orderbook_2026-03-21T13.parquet" in payload

    asyncio.run(scenario())


def test_stats_and_queue_payloads_are_mirror_only(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        app = create_app(config)
        index = app[INDEX_APP_KEY]
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        index.upsert_discovered_hour(
            filename, "https://raw.example.com/polymarket_orderbook_2026-03-21T12.parquet", 1
        )
        index.mark_mirrored(
            filename,
            local_path=str(tmp_path / "hour.parquet"),
            etag=None,
            content_length=1,
            last_modified=None,
        )

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            stats_response = await client.get("/v1/stats")
            queue_response = await client.get("/v1/queue")
            stats_payload = await stats_response.json()
            queue_payload = await queue_response.json()
        finally:
            await client.close()

        assert stats_response.status == 200
        assert queue_response.status == 200
        assert sorted(stats_payload.keys()) == [
            "archive_hours",
            "last_error_at",
            "last_event_at",
            "mirror_errors",
            "mirror_quarantined",
            "mirrored_hours",
        ]
        assert sorted(queue_payload.keys()) == [
            "latest_mirrored_filename",
            "latest_mirrored_hour",
            "mirror_active",
            "mirror_error",
            "mirror_pending",
            "mirror_quarantined",
            "mirror_retry_due",
            "mirror_retry_waiting",
            "next_retry_at",
        ]
        assert queue_payload["latest_mirrored_filename"] == filename

    asyncio.run(scenario())


def test_missing_hours_badge_shows_count(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        app = create_app(config)
        index = app[INDEX_APP_KEY]
        pending = "polymarket_orderbook_2026-03-21T12.parquet"
        ready = "polymarket_orderbook_2026-03-21T13.parquet"
        index.upsert_discovered_hour(pending, f"https://raw.example.com/{pending}", 1)
        index.upsert_discovered_hour(ready, f"https://raw.example.com/{ready}", 1)
        index.mark_mirrored(
            ready,
            local_path=str(tmp_path / ready),
            etag=None,
            content_length=1,
            last_modified=None,
        )

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get("/v1/badge/missing-hours.svg")
            payload = await response.text()
        finally:
            await client.close()

        assert response.status == 200
        assert "Missing hours" in payload
        assert "1/2" in payload

    asyncio.run(scenario())


def test_empty_hours_badge_shows_count(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        app = create_app(config)
        index = app[INDEX_APP_KEY]
        empty = "polymarket_orderbook_2026-03-21T12.parquet"
        nonempty = "polymarket_orderbook_2026-03-21T13.parquet"
        for filename in [empty, nonempty]:
            index.upsert_discovered_hour(filename, f"https://raw.example.com/{filename}", 1)
            index.mark_mirrored(
                filename,
                local_path=str(tmp_path / filename),
                etag=None,
                content_length=1,
                last_modified=None,
            )
        index.update_row_count(empty, 0)
        index.update_row_count(nonempty, 100)

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get("/v1/badge/empty-hours.svg")
            payload = await response.text()
        finally:
            await client.close()

        assert response.status == 200
        assert "Empty hours" in payload
        assert "1/2" in payload

    asyncio.run(scenario())


def test_upstream_badge_old_mirror_errors_still_report_online(tmp_path: Path):
    config = _make_config(tmp_path)
    now = datetime(2026, 4, 3, 20, 0, tzinfo=timezone.utc)

    payload = _upstream_badge_payload(
        stats={
            "last_event_at": (now - timedelta(minutes=5)).isoformat(),
            "last_error_at": (now - timedelta(hours=1)).isoformat(),
        },
        queue={
            "mirror_pending": 0,
            "mirror_active": 0,
            "mirror_error": 3,
            "latest_mirrored_hour": (now - timedelta(hours=1)).isoformat(),
        },
        config=config,
        now=now,
    )

    assert payload["label"] == "r2.pmxt.dev"
    assert payload["message"] == "online"
    assert payload["color"] == "brightgreen"
