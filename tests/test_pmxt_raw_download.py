from __future__ import annotations

from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pyarrow as pa
import pyarrow.parquet as pq

import pmxt_raw_download as raw_download


class _Response:
    def __init__(
        self,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._payload = payload
        self._offset = 0
        self.headers = headers or {}

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def _raw_parquet_payload() -> bytes:
    buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-a"],
                "update_type": ["book_snapshot"],
                "data": ['{"token_id":"token-yes","seq":1}'],
            }
        ),
        buffer,
    )
    return buffer.getvalue()


def test_discover_archive_hours_reads_listing_pages(monkeypatch) -> None:
    pages = {
        1: (
            '<a href="/dumps/polymarket_orderbook_2026-03-21T12.parquet">12</a>'
            '<a href="/dumps/polymarket_orderbook_2026-03-21T11.parquet">11</a>'
        ),
        2: (
            '<a href="/dumps/polymarket_orderbook_2026-03-21T10.parquet">10</a>'
            '<a href="/dumps/polymarket_orderbook_2026-03-21T12.parquet">dup</a>'
        ),
        3: "",
    }

    monkeypatch.setattr(
        raw_download,
        "fetch_archive_page",
        lambda archive_listing_url, page, timeout_secs: pages[page],  # type: ignore[no-untyped-def]
    )

    hours = raw_download.discover_archive_hours(
        archive_listing_url="https://archive.pmxt.dev/data/Polymarket",
        timeout_secs=60,
    )

    assert [hour.isoformat() for hour in hours] == [
        "2026-03-21T10:00:00+00:00",
        "2026-03-21T11:00:00+00:00",
        "2026-03-21T12:00:00+00:00",
    ]


def test_download_raw_hours_fetches_archive_then_relay_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    payload = _raw_parquet_payload()
    requested_urls: list[str] = []

    monkeypatch.setattr(
        raw_download,
        "discover_archive_hours",
        lambda **_: [
            raw_download.parse_archive_hour(
                "polymarket_orderbook_2026-03-21T09.parquet"
            ),
            raw_download.parse_archive_hour(
                "polymarket_orderbook_2026-03-21T10.parquet"
            ),
        ],
    )

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        requested_urls.append(request.full_url)
        if (
            request.full_url.endswith("2026-03-21T10.parquet")
            and "/v1/raw/" not in request.full_url
        ):
            raise HTTPError(request.full_url, 404, "missing", hdrs=None, fp=None)
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=tmp_path / "raws",
        show_progress=False,
    )

    assert summary.requested_hours == 2
    assert summary.downloaded_hours == 2
    assert summary.skipped_existing_hours == 0
    assert summary.failed_hours == []
    assert summary.source_hits == {
        "archive:https://r2.pmxt.dev": 1,
        "relay:https://209-209-10-83.sslip.io": 1,
    }
    assert requested_urls == [
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T09.parquet",
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T10.parquet",
        "https://209-209-10-83.sslip.io/v1/raw/2026/03/21/polymarket_orderbook_2026-03-21T10.parquet",
    ]
    assert (
        tmp_path
        / "raws"
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T09.parquet"
    ).exists()


def test_download_raw_hours_skips_existing_files(monkeypatch, tmp_path: Path) -> None:
    payload = _raw_parquet_payload()
    destination = tmp_path / "raws"
    existing_path = (
        destination
        / "2026"
        / "03"
        / "21"
        / "polymarket_orderbook_2026-03-21T09.parquet"
    )
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"existing")

    monkeypatch.setattr(
        raw_download,
        "discover_archive_hours",
        lambda **_: [
            raw_download.parse_archive_hour(
                "polymarket_orderbook_2026-03-21T09.parquet"
            ),
            raw_download.parse_archive_hour(
                "polymarket_orderbook_2026-03-21T10.parquet"
            ),
        ],
    )
    monkeypatch.setattr(
        raw_download,
        "urlopen",
        lambda request, timeout=60: _Response(payload),  # type: ignore[arg-type]
    )

    summary = raw_download.download_raw_hours(
        destination=destination,
        show_progress=False,
    )

    assert summary.downloaded_hours == 1
    assert summary.skipped_existing_hours == 1
    assert existing_path.read_bytes() == b"existing"
