from __future__ import annotations

from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pyarrow as pa
import pyarrow.parquet as pq

from scripts import _pmxt_raw_download as raw_download


class _Response:
    def __init__(self, payload: bytes, *, headers: dict[str, str] | None = None) -> None:
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


class _FakeTqdm:
    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        del args
        self.total = kwargs["total"]
        self.desc = kwargs["desc"]
        self.unit = kwargs["unit"]
        self.leave = kwargs["leave"]
        self.bar_format = kwargs["bar_format"]
        self.n = 0
        self.descriptions = [self.desc]
        self.postfixes: list[str] = []
        self.writes: list[str] = []
        self.closed = False

    def set_description_str(self, desc: str, refresh: bool = True) -> None:
        del refresh
        self.desc = desc
        self.descriptions.append(desc)

    def set_postfix_str(self, postfix: str, refresh: bool = True) -> None:
        del refresh
        self.postfixes.append(postfix)

    def refresh(self) -> None:
        return

    def update(self, value: int) -> None:
        self.n += value

    def write(self, text: str) -> None:
        self.writes.append(text)

    def close(self) -> None:
        self.closed = True


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
        archive_listing_url="https://archive.pmxt.dev/data/Polymarket", timeout_secs=60
    )

    assert [hour.isoformat() for hour in hours] == [
        "2026-03-21T10:00:00+00:00",
        "2026-03-21T11:00:00+00:00",
        "2026-03-21T12:00:00+00:00",
    ]


def test_download_raw_hours_fetches_archive_then_relay_fallback(
    monkeypatch, tmp_path: Path
) -> None:
    payload = _raw_parquet_payload()
    requested_urls: list[str] = []

    monkeypatch.setattr(
        raw_download,
        "discover_archive_hours",
        lambda **_: [
            raw_download.parse_archive_hour("polymarket_orderbook_2026-03-21T09.parquet"),
            raw_download.parse_archive_hour("polymarket_orderbook_2026-03-21T10.parquet"),
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

    summary = raw_download.download_raw_hours(destination=tmp_path / "raws", show_progress=False)

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
        tmp_path / "raws" / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T09.parquet"
    ).exists()


def test_download_raw_hours_skips_existing_files(monkeypatch, tmp_path: Path) -> None:
    payload = _raw_parquet_payload()
    destination = tmp_path / "raws"
    existing_path = (
        destination / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T09.parquet"
    )
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"existing")

    monkeypatch.setattr(
        raw_download,
        "discover_archive_hours",
        lambda **_: [
            raw_download.parse_archive_hour("polymarket_orderbook_2026-03-21T09.parquet"),
            raw_download.parse_archive_hour("polymarket_orderbook_2026-03-21T10.parquet"),
        ],
    )
    monkeypatch.setattr(
        raw_download,
        "urlopen",
        lambda request, timeout=60: _Response(payload),  # type: ignore[arg-type]
    )

    summary = raw_download.download_raw_hours(destination=destination, show_progress=False)

    assert summary.downloaded_hours == 1
    assert summary.skipped_existing_hours == 1
    assert existing_path.read_bytes() == b"existing"


def test_download_raw_hours_removes_stale_temp_files_before_skipping(
    monkeypatch, tmp_path: Path
) -> None:
    destination = tmp_path / "raws"
    existing_path = (
        destination / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T09.parquet"
    )
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"existing")

    plain_tmp_path = existing_path.with_name(f"{existing_path.name}.tmp")
    plain_tmp_path.write_bytes(b"stale-plain-tmp")

    pid_tmp_path = existing_path.with_name(f"{existing_path.name}.tmp.999999")
    pid_tmp_path.write_bytes(b"stale-pid-tmp")

    monkeypatch.setattr(
        raw_download,
        "discover_archive_hours",
        lambda **_: [raw_download.parse_archive_hour("polymarket_orderbook_2026-03-21T09.parquet")],
    )

    def fake_pid_is_active(pid: int) -> bool:
        del pid
        return False

    def unexpected_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        raise AssertionError(f"unexpected download request for {request.full_url}")

    monkeypatch.setattr(raw_download, "_pid_is_active", fake_pid_is_active)
    monkeypatch.setattr(raw_download, "urlopen", unexpected_urlopen)

    summary = raw_download.download_raw_hours(destination=destination, show_progress=False)

    assert summary.downloaded_hours == 0
    assert summary.skipped_existing_hours == 1
    assert existing_path.read_bytes() == b"existing"
    assert not plain_tmp_path.exists()
    assert not pid_tmp_path.exists()


def test_download_raw_hours_progress_output_uses_short_hour_labels(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    payload = _raw_parquet_payload()
    bars: list[_FakeTqdm] = []

    monkeypatch.setattr(
        raw_download,
        "discover_archive_hours",
        lambda **_: [
            raw_download.parse_archive_hour("polymarket_orderbook_2026-03-21T09.parquet"),
            raw_download.parse_archive_hour("polymarket_orderbook_2026-03-21T10.parquet"),
        ],
    )

    def fake_tqdm(*args, **kwargs):  # type: ignore[no-untyped-def]
        bar = _FakeTqdm(*args, **kwargs)
        bars.append(bar)
        return bar

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        if (
            request.full_url.endswith("2026-03-21T10.parquet")
            and "/v1/raw/" not in request.full_url
        ):
            raise HTTPError(request.full_url, 404, "missing", hdrs=None, fp=None)
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setattr(raw_download, "tqdm", fake_tqdm)
    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(destination=tmp_path / "raws", show_progress=True)

    assert summary.downloaded_hours == 2
    assert len(bars) == 1

    bar = bars[0]
    captured = capsys.readouterr()

    assert (
        "PMXT raw source: explicit priority (archive https://r2.pmxt.dev -> relay https://209-209-10-83.sslip.io)"
    ) in captured.out
    assert "window_start=2026-03-21T09" in captured.out
    assert "window_end=2026-03-21T10" in captured.out
    assert any("active: archive 2026-03-21T09" in status for status in bar.postfixes)
    assert any("active: relay 2026-03-21T10" in status for status in bar.postfixes)
    assert not any("+00:00" in status for status in bar.postfixes)
    assert any("2026-03-21T09" in line and line.endswith("archive") for line in bar.writes)
    assert any("2026-03-21T10" in line and line.endswith("relay") for line in bar.writes)
    assert not any("+00:00" in line for line in bar.writes)
    assert "Downloading raw hours (0/2 done, 1 active)" in bar.descriptions
    assert bar.desc == "Downloading raw hours (2/2 done)"
