from __future__ import annotations

import asyncio
from dataclasses import replace
import os
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
from aiohttp.test_utils import TestClient
from aiohttp.test_utils import TestServer

from pmxt_relay.api import (
    INDEX_APP_KEY,
    RequestRateLimiter,
    _client_id,
    _collect_inflight_processes,
    _cpu_percent_from_loadavg,
    _resolve_filtered_path,
    _resolve_raw_path,
    create_app,
)
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


def test_cpu_percent_uses_load_average():
    with patch("pmxt_relay.api.os.cpu_count", return_value=4):
        with patch("pmxt_relay.api.os.getloadavg", return_value=(3.5, 3.0, 2.5)):
            result = _cpu_percent_from_loadavg()
            assert result == 87.5  # 3.5 / 4 * 100

        with patch("pmxt_relay.api.os.getloadavg", return_value=(0.0, 0.0, 0.0)):
            result = _cpu_percent_from_loadavg()
            assert result == 0.0

        with patch("pmxt_relay.api.os.getloadavg", return_value=(5.0, 4.0, 3.0)):
            result = _cpu_percent_from_loadavg()
            assert result == 100.0  # capped at 100


def test_rate_limiter_enforces_sliding_window():
    limiter = RequestRateLimiter(requests_per_minute=2)

    assert limiter.allow("203.0.113.1", now=0.0) is True
    assert limiter.allow("203.0.113.1", now=10.0) is True
    assert limiter.allow("203.0.113.1", now=20.0) is False
    assert limiter.allow("203.0.113.1", now=61.0) is True
    assert limiter.bucket_size("203.0.113.1", now=61.0) == 2


def test_rate_limiter_evicts_stale_buckets_above_threshold():
    limiter = RequestRateLimiter(requests_per_minute=100)

    # Fill 10001 client buckets so the eviction triggers
    for i in range(10001):
        limiter.allow(f"client_{i}", now=0.0)

    # All buckets still exist because they have entries
    assert len(limiter._requests) == 10001

    # Expire all entries by advancing past the 60s window and trigger eviction.
    limiter.allow("trigger_client", now=120.0)
    assert len(limiter._requests) == 1
    assert limiter.bucket_size("client_0", now=120.0) == 0


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


def test_client_id_ignores_forwarded_for_from_untrusted_remote():
    class _Transport:
        def get_extra_info(self, name: str):  # type: ignore[no-untyped-def]
            if name == "peername":
                return ("203.0.113.9", 12345)
            return None

    class _Request:
        remote = "203.0.113.9"
        headers = {"X-Forwarded-For": "198.51.100.7"}
        transport = _Transport()

    assert _client_id(_Request(), trusted_proxy_ips=("127.0.0.1",)) == "203.0.113.9"


def test_rate_limiter_disabled_when_zero():
    limiter = RequestRateLimiter(requests_per_minute=0)
    for _ in range(1000):
        assert limiter.allow("flood", now=0.0) is True


def test_filtered_path_resolution_blocks_traversal(tmp_path: Path):
    config = _make_config(tmp_path)

    safe_path = _resolve_filtered_path(
        config,
        "0x" + ("ab" * 32),
        "123456789",
        "polymarket_orderbook_2026-03-21T12.parquet",
    )
    blocked_path = _resolve_filtered_path(
        config,
        "..",
        "123456789",
        "polymarket_orderbook_2026-03-21T12.parquet",
    )

    assert safe_path == (
        config.filtered_root
        / ("0x" + ("ab" * 32))
        / "123456789"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    assert blocked_path is None


def test_raw_path_resolution_requires_known_archive_layout(tmp_path: Path):
    config = _make_config(tmp_path)

    safe_path = _resolve_raw_path(
        config,
        "2026/03/21/polymarket_orderbook_2026-03-21T12.parquet",
    )
    blocked_path = _resolve_raw_path(
        config,
        "../../etc/passwd",
    )

    assert safe_path == (
        config.raw_root
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    assert blocked_path is None


def test_collect_inflight_processes_reports_tmp_tree(tmp_path: Path):
    config = _make_config(tmp_path)
    inflight_root = (
        config.tmp_root / "polymarket_orderbook_2026-03-21T12.parquet.filtered"
    )
    target_dir = inflight_root / "condition-a" / "token-yes"
    target_dir.mkdir(parents=True)
    payload_path = target_dir / "part-0.parquet"
    payload_path.write_bytes(b"abc")
    os.utime(payload_path, (1234, 1234))

    inflight = _collect_inflight_processes(config)

    assert inflight == [
        {
            "filename": "polymarket_orderbook_2026-03-21T12.parquet",
            "path": str(inflight_root),
            "dir_count": 3,
            "file_count": 1,
            "byte_size": 3,
            "latest_mtime": 1234.0,
        }
    ]


def test_proxy_forwarded_clients_do_not_share_rate_limit_bucket(tmp_path: Path):
    async def scenario() -> None:
        config = replace(_make_config(tmp_path), api_rate_limit_per_minute=1)
        config.ensure_directories()
        app = create_app(config)

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            first = await client.get(
                "/healthz",
                headers={"X-Forwarded-For": "198.51.100.1"},
            )
            second = await client.get(
                "/healthz",
                headers={"X-Forwarded-For": "198.51.100.1"},
            )
            third = await client.get(
                "/healthz",
                headers={"X-Forwarded-For": "198.51.100.2"},
            )
        finally:
            await client.close()

        assert first.status == 200
        assert second.status == 429
        assert third.status == 200

    asyncio.run(scenario())


def test_badge_endpoints_return_shields_payloads(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        ready_filename = "polymarket_orderbook_2026-03-21T12.parquet"
        processing_filename = "polymarket_orderbook_2026-03-21T13.parquet"

        app = create_app(config)
        index = app[INDEX_APP_KEY]
        index.upsert_discovered_hour(
            ready_filename,
            f"https://r2.pmxt.dev/{ready_filename}",
            1,
        )
        index.mark_mirrored(
            ready_filename,
            local_path="/tmp/raw-ready.parquet",
            etag=None,
            content_length=None,
            last_modified=None,
        )
        index.replace_filtered_hours(ready_filename, [])

        index.upsert_discovered_hour(
            processing_filename,
            f"https://r2.pmxt.dev/{processing_filename}",
            1,
        )
        index.mark_mirrored(
            processing_filename,
            local_path="/tmp/raw.parquet",
            etag=None,
            content_length=None,
            last_modified=None,
        )
        index.mark_processing(processing_filename)
        index.log_event(
            level="INFO",
            event_type="process_start",
            filename=processing_filename,
            message="Started processing hour",
        )

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            status_response = await client.get("/v1/badge/status")
            assert status_response.status == 200
            status_payload = await status_response.json()

            backfill_response = await client.get("/v1/badge/backfill")
            assert backfill_response.status == 200
            backfill_payload = await backfill_response.json()

            mirrored_response = await client.get("/v1/badge/mirrored")
            assert mirrored_response.status == 200
            mirrored_payload = await mirrored_response.json()

            processed_response = await client.get("/v1/badge/processed")
            assert processed_response.status == 200
            processed_payload = await processed_response.json()

            latest_response = await client.get("/v1/badge/latest")
            assert latest_response.status == 200
            latest_payload = await latest_response.json()

            lag_response = await client.get("/v1/badge/lag")
            assert lag_response.status == 200
            lag_payload = await lag_response.json()
        finally:
            await client.close()

        assert status_payload == {
            "schemaVersion": 1,
            "label": "PMXT relay",
            "message": "processing",
            "color": "brightgreen",
        }
        assert backfill_payload == {
            "schemaVersion": 1,
            "label": "PMXT backfill",
            "message": "1/2 hrs",
            "color": "green",
        }
        assert mirrored_payload == {
            "schemaVersion": 1,
            "label": "PMXT mirrored",
            "message": "2/2 hrs",
            "color": "brightgreen",
        }
        assert processed_payload == {
            "schemaVersion": 1,
            "label": "PMXT processed",
            "message": "1/2 hrs",
            "color": "green",
        }
        assert latest_payload == {
            "schemaVersion": 1,
            "label": "PMXT latest",
            "message": "2026-03-21T12Z",
            "color": "blue",
        }
        assert lag_payload == {
            "schemaVersion": 1,
            "label": "PMXT lag",
            "message": "1 hrs",
            "color": "green",
        }

    asyncio.run(scenario())


def test_badge_svg_endpoints_return_svg(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"

        app = create_app(config)
        index = app[INDEX_APP_KEY]
        index.upsert_discovered_hour(
            filename,
            f"https://r2.pmxt.dev/{filename}",
            1,
        )
        index.mark_mirrored(
            filename,
            local_path="/tmp/raw.parquet",
            etag=None,
            content_length=None,
            last_modified=None,
        )
        index.replace_filtered_hours(filename, [])

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            svg_payloads = {}
            for path in (
                "/v1/badge/status.svg",
                "/v1/badge/mirrored.svg",
                "/v1/badge/processed.svg",
            ):
                response = await client.get(path)
                assert response.status == 200
                assert response.headers["Content-Type"].startswith("image/svg+xml")
                svg_payloads[path] = await response.text()
        finally:
            await client.close()

        assert "PMXT relay" in svg_payloads["/v1/badge/status.svg"]
        assert "starting" in svg_payloads["/v1/badge/status.svg"]
        assert "<svg" in svg_payloads["/v1/badge/status.svg"]
        assert "PMXT mirrored" in svg_payloads["/v1/badge/mirrored.svg"]
        assert "PMXT processed" in svg_payloads["/v1/badge/processed.svg"]

    asyncio.run(scenario())


def test_system_endpoints_return_live_metrics_and_svg(tmp_path: Path):
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
                    "cpu_percent": 12.5,
                    "mem_percent": 34.0,
                    "disk_percent": 56.5,
                },
            ):
                metrics_response = await client.get("/v1/system")
                assert metrics_response.status == 200
                metrics_payload = await metrics_response.json()

                cpu_badge = await client.get("/v1/badge/cpu.svg")
                mem_badge = await client.get("/v1/badge/mem.svg")
                disk_badge = await client.get("/v1/badge/disk.svg")
                assert cpu_badge.status == 200
                assert mem_badge.status == 200
                assert disk_badge.status == 200
                cpu_svg = await cpu_badge.text()
                mem_svg = await mem_badge.text()
                disk_svg = await disk_badge.text()
        finally:
            await client.close()

        assert metrics_payload == {
            "cpu_percent": 12.5,
            "mem_percent": 34.0,
            "disk_percent": 56.5,
        }
        assert "Relay CPU" in cpu_svg and "12.5%" in cpu_svg
        assert "Relay mem" in mem_svg and "34.0%" in mem_svg
        assert "Relay disk" in disk_svg and "56.5%" in disk_svg

    asyncio.run(scenario())


def test_filtered_api_returns_404_for_non_prebuilt_hour(tmp_path: Path):
    """Relay must not scan processed parquet on the fly — return 404 instead."""

    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        processed_path = config.processed_root / "2026" / "03" / "21" / filename
        processed_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "market_id": ["0x" + ("ab" * 32)],
                    "token_id": ["123456789"],
                    "update_type": ["book_snapshot"],
                    "data": ['{"token_id":"123456789","seq":1}'],
                }
            ),
            processed_path,
        )

        app = create_app(config)

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get(
                "/v1/filtered/" + ("0x" + ("ab" * 32)) + "/123456789/" + filename
            )
            assert response.status == 404
            body = await response.text()
            assert "not yet prebuilt" in body
        finally:
            await client.close()

    asyncio.run(scenario())


def test_filtered_api_returns_404_when_processed_hour_has_no_matching_token(
    tmp_path: Path,
):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        processed_path = config.processed_root / "2026" / "03" / "21" / filename
        processed_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "market_id": ["0x" + ("ab" * 32)],
                    "token_id": ["111"],
                    "update_type": ["book_snapshot"],
                    "data": ['{"token_id":"111","seq":1}'],
                }
            ),
            processed_path,
        )

        app = create_app(config)
        index = app[INDEX_APP_KEY]
        index.upsert_discovered_hour(
            filename,
            f"https://r2.pmxt.dev/{filename}",
            1,
        )
        index.mark_mirrored(
            filename,
            local_path="/tmp/raw.parquet",
            etag=None,
            content_length=None,
            last_modified=None,
        )
        index.mark_sharded(filename)

        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get(
                "/v1/filtered/" + ("0x" + ("ab" * 32)) + "/999/" + filename
            )
            assert response.status == 404
        finally:
            await client.close()

    asyncio.run(scenario())


def test_list_filtered_hours_scans_filesystem_when_index_is_empty(tmp_path: Path):
    async def scenario() -> None:
        config = _make_config(tmp_path)
        config.ensure_directories()
        condition_id = "0x" + ("ab" * 32)
        token_id = "123456789"
        filtered_dir = config.filtered_root / condition_id / token_id
        filtered_dir.mkdir(parents=True, exist_ok=True)
        hour_path = filtered_dir / "polymarket_orderbook_2026-03-21T12.parquet"
        pq.write_table(
            pa.table(
                {
                    "update_type": ["book_snapshot"],
                    "data": ['{"token_id":"123456789","seq":1}'],
                }
            ),
            hour_path,
        )

        app = create_app(config)
        server = TestServer(app)
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get(
                f"/v1/markets/{condition_id}/tokens/{token_id}/hours"
            )
            assert response.status == 200
            payload = await response.json()
        finally:
            await client.close()

        assert payload["condition_id"] == condition_id
        assert payload["token_id"] == token_id
        assert payload["truncated"] is False
        assert payload["hours"] == [
            {
                "hour": "2026-03-21T12:00:00+00:00",
                "filename": "polymarket_orderbook_2026-03-21T12.parquet",
                "row_count": None,
                "byte_size": hour_path.stat().st_size,
                "url": f"/v1/filtered/{condition_id}/{token_id}/polymarket_orderbook_2026-03-21T12.parquet",
            }
        ]

    asyncio.run(scenario())
