from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request

from pmxt_relay.config import ArchiveSource, RelayConfig
from pmxt_relay.storage import raw_relative_path
from pmxt_relay.worker import RelayWorker


def _make_config(tmp_path: Path) -> RelayConfig:
    return RelayConfig(
        data_dir=tmp_path,
        bind_host="127.0.0.1",
        bind_port=8080,
        archive_listing_url="https://archive.pmxt.dev/Polymarket/v2",
        raw_base_url="https://r2v2.pmxt.dev",
        poll_interval_secs=900,
        http_timeout_secs=30,
        archive_stale_pages=3,
        archive_max_pages=None,
        event_retention=1000,
        api_rate_limit_per_minute=2400,
        verify_batch_size=50,
    )


class _FakeResponse:
    def __init__(self, payload: bytes, *, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.headers = headers or {}
        self._offset = 0

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # type: ignore[no-untyped-def]
        return False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def test_discover_archive_hours_uses_multiple_archive_sources(tmp_path: Path, monkeypatch) -> None:
    config = RelayConfig(
        data_dir=tmp_path,
        bind_host="127.0.0.1",
        bind_port=8080,
        archive_listing_url="https://archive.pmxt.dev/Polymarket/v2",
        raw_base_url="https://r2v2.pmxt.dev",
        poll_interval_secs=900,
        http_timeout_secs=30,
        archive_stale_pages=1,
        archive_max_pages=None,
        event_retention=1000,
        api_rate_limit_per_minute=2400,
        verify_batch_size=50,
        archive_sources=(
            ArchiveSource(
                listing_url="https://archive.pmxt.dev/Polymarket/v2",
                raw_base_url="https://r2v2.pmxt.dev",
            ),
            ArchiveSource(
                listing_url="https://archive.pmxt.dev/Polymarket/v1",
                raw_base_url="https://r2.pmxt.dev",
            ),
        ),
    )
    pages = {
        ("https://archive.pmxt.dev/Polymarket/v2", 1): (
            '<a href="https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet">12</a>'
        ),
        ("https://archive.pmxt.dev/Polymarket/v2", 2): "",
        ("https://archive.pmxt.dev/Polymarket/v1", 1): (
            '<a href="https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T11.parquet">11</a>'
            '<a href="https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet">12</a>'
        ),
        ("https://archive.pmxt.dev/Polymarket/v1", 2): "",
    }

    monkeypatch.setattr(
        "pmxt_relay.worker.fetch_archive_page",
        lambda archive_listing_url, page, timeout_secs: pages[(archive_listing_url, page)],  # type: ignore[no-untyped-def]
    )

    with RelayWorker(config, reset_inflight=False) as worker:
        discovered = worker._discover_archive_hours()
        rows = {
            row["filename"]: row
            for row in worker._index._fetchall("SELECT * FROM archive_hours ORDER BY filename")
        }

    assert discovered == 3
    assert rows["polymarket_orderbook_2026-03-21T11.parquet"]["source_url"] == (
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T11.parquet"
    )
    assert rows["polymarket_orderbook_2026-03-21T12.parquet"]["source_url"] == (
        "https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T12.parquet"
    )


def test_mirror_hour_falls_back_to_get_when_head_is_rejected(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        source_url = f"https://r2v2.pmxt.dev/{filename}"
        worker._index.upsert_discovered_hour(filename, source_url, 1)
        row = worker._index.list_hours_needing_mirror()[0]
        requested_methods: list[str] = []

        def fake_urlopen(request: Request, timeout):  # type: ignore[no-untyped-def]
            assert timeout == config.http_timeout_secs
            requested_methods.append(request.get_method())
            if request.get_method() == "HEAD":
                raise HTTPError(request.full_url, 403, "Forbidden", hdrs=None, fp=None)
            return _FakeResponse(
                b"raw-payload",
                headers={
                    "ETag": '"abc123"',
                    "Last-Modified": "Sun, 21 Mar 2026 12:59:59 GMT",
                    "Content-Length": "11",
                },
            )

        monkeypatch.setattr("pmxt_relay.worker.urlopen", fake_urlopen)

        worker._mirror_hour(row)

        raw_path = config.raw_root / raw_relative_path(filename)
        assert raw_path.read_bytes() == b"raw-payload"
        assert requested_methods == ["HEAD", "GET"]

        stats = worker._index.stats(now=datetime(2026, 3, 21, 12, 30, tzinfo=timezone.utc))
        assert stats["archive_hours"] == 1
        assert stats["mirrored_hours"] == 1


def test_content_changed_remirror_replaces_existing_raw(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        source_url = f"https://r2v2.pmxt.dev/{filename}"
        raw_path = config.raw_root / raw_relative_path(filename)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(b"old-raw-payload")
        worker._index.upsert_discovered_hour(filename, source_url, 1)
        worker._index.mark_mirrored(
            filename,
            local_path=str(raw_path),
            etag='"old"',
            content_length=len(b"old-raw-payload"),
            last_modified=None,
        )
        worker._index.mark_needs_remirror(filename)
        row = worker._index.list_hours_needing_mirror()[0]
        requested_methods: list[str] = []

        def fake_urlopen(request: Request, timeout):  # type: ignore[no-untyped-def]
            requested_methods.append(request.get_method())
            if request.get_method() == "HEAD":
                return _FakeResponse(
                    b"",
                    headers={
                        "ETag": '"new"',
                        "Last-Modified": "Sun, 21 Mar 2026 13:59:59 GMT",
                        "Content-Length": str(len(b"new-raw-payload")),
                    },
                )
            return _FakeResponse(b"new-raw-payload")

        monkeypatch.setattr("pmxt_relay.worker.urlopen", fake_urlopen)

        worker._mirror_hour(row)

        assert raw_path.read_bytes() == b"new-raw-payload"
        assert requested_methods == ["HEAD", "GET"]
        updated = worker._index._conn.execute(
            """
            SELECT mirror_status, etag, content_length, last_error
            FROM archive_hours
            WHERE filename = ?
            """,
            (filename,),
        ).fetchone()
        assert updated is not None
        assert updated["mirror_status"] == "ready"
        assert updated["etag"] == '"new"'
        assert updated["content_length"] == len(b"new-raw-payload")
        assert updated["last_error"] is None


def test_run_once_only_discovers_adopts_and_mirrors(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        monkeypatch.setattr(worker, "_discover_archive_hours", lambda: 2)
        monkeypatch.setattr(worker, "_adopt_local_raw_hours", lambda: 3)
        monkeypatch.setattr(worker, "_mirror_pending_hours", lambda: 5)

        assert worker.run_once() == 10


def test_adopt_local_raw_marks_hours_as_mirrored(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    raw_path = config.raw_root / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"x" * (2 * 1024 * 1024))

    with RelayWorker(config, reset_inflight=False) as worker:
        adopted = worker._adopt_local_raw_hours()

        assert adopted == 1
        stats = worker._index.stats()
        assert stats["mirrored_hours"] == 1


def test_adopt_local_raw_preserves_archive_source_url(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    raw_path = config.raw_root / "2026" / "03" / "21" / filename
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"x" * (2 * 1024 * 1024))

    with RelayWorker(config, reset_inflight=False) as worker:
        source_url = f"https://r2.pmxt.dev/{filename}"
        worker._index.upsert_discovered_hour(filename, source_url, 1)

        adopted = worker._adopt_local_raw_hours()

        row = worker._index._conn.execute(
            "SELECT source_url, mirror_status FROM archive_hours WHERE filename = ?",
            (filename,),
        ).fetchone()
        assert adopted == 1
        assert row is not None
        assert row["source_url"] == source_url
        assert row["mirror_status"] == "ready"


def test_run_once_scans_full_local_tree_only_on_first_cycle(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        calls = {"full": 0, "pending": 0}

        monkeypatch.setattr(worker, "_discover_archive_hours", lambda: 0)
        monkeypatch.setattr(worker, "_mirror_pending_hours", lambda: 0)

        def _adopt_all() -> int:
            calls["full"] += 1
            return 0

        def _adopt_pending() -> int:
            calls["pending"] += 1
            return 0

        monkeypatch.setattr(worker, "_adopt_all_local_raw_hours", _adopt_all)
        monkeypatch.setattr(worker, "_adopt_pending_local_raw_hours", _adopt_pending)

        assert worker.run_once() == 0
        assert worker.run_once() == 0

        assert calls == {"full": 1, "pending": 1}


def test_repeated_404s_are_quarantined(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        source_url = f"https://r2v2.pmxt.dev/{filename}"
        worker._index.upsert_discovered_hour(filename, source_url, 1)
        worker._index.mark_mirror_retry(
            filename, error="HTTP Error 404: Not Found", next_retry_at="1970-01-01T00:00:00+00:00"
        )
        worker._index.mark_mirror_retry(
            filename, error="HTTP Error 404: Not Found", next_retry_at="1970-01-01T00:00:00+00:00"
        )

        def _always_404(row) -> None:  # type: ignore[no-untyped-def]
            request = Request(row["source_url"])
            raise HTTPError(request.full_url, 404, "Not Found", hdrs=None, fp=None)

        monkeypatch.setattr(worker, "_mirror_hour", _always_404)

        assert worker._mirror_pending_hours() == 0

        queue = worker._index.queue_summary()
        stats = worker._index.stats()
        events = worker._index.recent_events(limit=1)

        assert queue["mirror_quarantined"] == 1
        assert queue["mirror_error"] == 1
        assert queue["mirror_retry_waiting"] == 1
        assert queue["next_retry_at"] is not None
        assert stats["mirror_quarantined"] == 1
        assert worker._index.list_hours_needing_mirror() == []
        assert events[0]["event_type"] == "mirror_quarantined"


def test_verify_ready_hours_requeues_changed_upstream(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        source_url = f"https://r2v2.pmxt.dev/{filename}"
        worker._index.upsert_discovered_hour(filename, source_url, 1)
        worker._index.mark_mirrored(
            filename,
            local_path=str(tmp_path / filename),
            etag="abc",
            content_length=11,
            last_modified=None,
        )

        def fake_urlopen(request: Request, timeout):  # type: ignore[no-untyped-def]
            assert timeout == 2
            assert request.get_method() == "HEAD"
            return _FakeResponse(b"", headers={"ETag": "xyz", "Content-Length": "11"})

        monkeypatch.setattr("pmxt_relay.worker.urlopen", fake_urlopen)

        assert worker._verify_ready_hours() == 1

        row = worker._index._conn.execute(
            "SELECT mirror_status, last_verified_at FROM archive_hours WHERE filename = ?",
            (filename,),
        ).fetchone()
        assert row is not None
        assert row["mirror_status"] == "pending"
        assert row["last_verified_at"] is None


def test_verify_ready_hours_skips_unchanged(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        source_url = f"https://r2v2.pmxt.dev/{filename}"
        worker._index.upsert_discovered_hour(filename, source_url, 1)
        worker._index.mark_mirrored(
            filename,
            local_path=str(tmp_path / filename),
            etag="abc",
            content_length=11,
            last_modified=None,
        )

        def fake_urlopen(request: Request, timeout):  # type: ignore[no-untyped-def]
            assert timeout == 2
            assert request.get_method() == "HEAD"
            return _FakeResponse(b"", headers={"ETag": "abc", "Content-Length": "11"})

        monkeypatch.setattr("pmxt_relay.worker.urlopen", fake_urlopen)

        assert worker._verify_ready_hours() == 0

        row = worker._index._conn.execute(
            "SELECT mirror_status, last_verified_at FROM archive_hours WHERE filename = ?",
            (filename,),
        ).fetchone()
        assert row is not None
        assert row["mirror_status"] == "ready"
        assert row["last_verified_at"] is not None


def test_verify_ready_hours_tolerates_head_failure(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        filename = "polymarket_orderbook_2026-03-21T12.parquet"
        source_url = f"https://r2v2.pmxt.dev/{filename}"
        worker._index.upsert_discovered_hour(filename, source_url, 1)
        worker._index.mark_mirrored(
            filename,
            local_path=str(tmp_path / filename),
            etag="abc",
            content_length=11,
            last_modified=None,
        )

        def fake_urlopen(request: Request, timeout):  # type: ignore[no-untyped-def]
            assert timeout == 2
            assert request.get_method() == "HEAD"
            raise OSError("HEAD failed")

        monkeypatch.setattr("pmxt_relay.worker.urlopen", fake_urlopen)

        assert worker._verify_ready_hours() == 0

        row = worker._index._conn.execute(
            "SELECT mirror_status FROM archive_hours WHERE filename = ?",
            (filename,),
        ).fetchone()
        assert row is not None
        assert row["mirror_status"] == "ready"


def test_run_once_includes_verification_step(tmp_path: Path, monkeypatch) -> None:
    config = _make_config(tmp_path)
    with RelayWorker(config, reset_inflight=False) as worker:
        monkeypatch.setattr(worker, "_discover_archive_hours", lambda: 2)
        monkeypatch.setattr(worker, "_adopt_local_raw_hours", lambda: 3)
        monkeypatch.setattr(worker, "_mirror_pending_hours", lambda: 5)
        monkeypatch.setattr(worker, "_verify_ready_hours", lambda: 7)

        assert worker.run_once() == 17
