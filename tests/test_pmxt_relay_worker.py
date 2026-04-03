from __future__ import annotations

from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request

from pmxt_relay.config import RelayConfig
from pmxt_relay.storage import raw_relative_path
from pmxt_relay.worker import RelayWorker


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
        event_retention=1000,
        api_rate_limit_per_minute=2400,
        api_list_max_hours=2000,
    )


class _FakeResponse:
    def __init__(
        self,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
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


def test_mirror_hour_falls_back_to_get_when_head_is_rejected(
    tmp_path: Path,
    monkeypatch,
) -> None:
    config = _make_config(tmp_path)
    worker = RelayWorker(config, reset_inflight=False)
    filename = "polymarket_orderbook_2026-03-21T12.parquet"
    source_url = f"https://r2.pmxt.dev/{filename}"
    worker._index.upsert_discovered_hour(filename, source_url, 1)  # noqa: SLF001
    row = worker._index.list_hours_needing_mirror()[0]  # noqa: SLF001
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

    worker._mirror_hour(row)  # noqa: SLF001

    raw_path = config.raw_root / raw_relative_path(filename)
    assert raw_path.read_bytes() == b"raw-payload"
    assert requested_methods == ["HEAD", "GET"]

    stats = worker._index.stats()  # noqa: SLF001
    assert stats["archive_hours"] == 1
    assert stats["mirrored_hours"] == 1
    assert stats["processing_disabled_hours"] == 1


def test_run_once_only_discovers_adopts_and_mirrors(
    tmp_path: Path, monkeypatch
) -> None:
    config = _make_config(tmp_path)
    worker = RelayWorker(config, reset_inflight=False)
    monkeypatch.setattr(worker, "_discover_archive_hours", lambda: 2)  # noqa: SLF001
    monkeypatch.setattr(worker, "_adopt_local_raw_hours", lambda: 3)  # noqa: SLF001
    monkeypatch.setattr(worker, "_mirror_pending_hours", lambda: 5)  # noqa: SLF001

    assert worker.run_once() == 10


def test_adopt_local_raw_marks_hours_as_disabled_for_processing(tmp_path: Path) -> None:
    config = _make_config(tmp_path)
    worker = RelayWorker(config, reset_inflight=False)
    raw_path = (
        config.raw_root
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_bytes(b"raw-payload")

    adopted = worker._adopt_local_raw_hours()  # noqa: SLF001

    assert adopted == 1
    stats = worker._index.stats()  # noqa: SLF001
    assert stats["mirrored_hours"] == 1
    assert stats["processing_disabled_hours"] == 1
