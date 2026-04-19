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

    def __enter__(self) -> _Response:
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


def _empty_parquet_payload() -> bytes:
    buffer = BytesIO()
    pq.write_table(
        pa.table(
            {
                "market_id": pa.array([], type=pa.string()),
                "update_type": pa.array([], type=pa.string()),
                "data": pa.array([], type=pa.string()),
            }
        ),
        buffer,
    )
    return buffer.getvalue()


def _window_kwargs(start: str = "2026-03-21T09", end: str = "2026-03-21T10") -> dict[str, str]:
    return {"start_time": start, "end_time": end}


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
        archive_listing_url="https://archive.pmxt.dev/Polymarket/v2", timeout_secs=60
    )

    assert [hour.isoformat() for hour in hours] == [
        "2026-03-21T12:00:00+00:00",
        "2026-03-21T11:00:00+00:00",
        "2026-03-21T10:00:00+00:00",
    ]


def test_download_raw_hours_fetches_archive_fallbacks(monkeypatch, tmp_path: Path) -> None:
    payload = _raw_parquet_payload()
    requested_urls: list[str] = []

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        if request.get_method() != "HEAD":
            requested_urls.append(request.full_url)
        if request.full_url == "https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T10.parquet":
            raise HTTPError(request.full_url, 404, "missing", hdrs=None, fp=None)
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=tmp_path / "raws", show_progress=False, **_window_kwargs()
    )

    assert summary.requested_hours == 2
    assert summary.downloaded_hours == 2
    assert summary.skipped_existing_hours == 0
    assert summary.failed_hours == []
    assert summary.source_hits == {
        "archive:https://r2v2.pmxt.dev": 1,
        "archive:https://r2.pmxt.dev": 1,
    }
    assert requested_urls == [
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T10.parquet",
        "https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T09.parquet",
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
        "urlopen",
        lambda request, timeout=60: _Response(payload),  # type: ignore[arg-type]
    )
    monkeypatch.setattr(raw_download, "_existing_refresh_reason", lambda **_: None)

    summary = raw_download.download_raw_hours(
        destination=destination, show_progress=False, **_window_kwargs()
    )

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

    def fake_pid_is_active(pid: int) -> bool:
        del pid
        return False

    def unexpected_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        raise AssertionError(f"unexpected download request for {request.full_url}")

    monkeypatch.setattr(raw_download, "_pid_is_active", fake_pid_is_active)
    monkeypatch.setattr(raw_download, "_existing_refresh_reason", lambda **_: None)
    monkeypatch.setattr(raw_download, "urlopen", unexpected_urlopen)

    summary = raw_download.download_raw_hours(
        destination=destination,
        show_progress=False,
        **_window_kwargs("2026-03-21T09", "2026-03-21T09"),
    )

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

    def fake_tqdm(*args, **kwargs):  # type: ignore[no-untyped-def]
        bar = _FakeTqdm(*args, **kwargs)
        bars.append(bar)
        return bar

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        if request.full_url == "https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T10.parquet":
            raise HTTPError(request.full_url, 404, "missing", hdrs=None, fp=None)
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setattr(raw_download, "tqdm", fake_tqdm)
    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=tmp_path / "raws", show_progress=True, **_window_kwargs()
    )

    assert summary.downloaded_hours == 2
    assert len(bars) == 1

    bar = bars[0]
    captured = capsys.readouterr()

    assert (
        "PMXT raw source: direct hour probes (archive best-of https://r2v2.pmxt.dev, https://r2.pmxt.dev)"
    ) in captured.out
    assert "window_start=2026-03-21T09" in captured.out
    assert "window_end=2026-03-21T10" in captured.out
    assert any("active: archive 2026-03-21T09" in status for status in bar.postfixes)
    assert any("active: archive 2026-03-21T10" in status for status in bar.postfixes)
    assert not any("+00:00" in status for status in bar.postfixes)
    assert any("2026-03-21T09" in line and line.endswith("archive") for line in bar.writes)
    assert any("2026-03-21T10" in line and line.endswith("archive") for line in bar.writes)
    assert not any("+00:00" in line for line in bar.writes)
    assert "Downloading raw hours (0/2 done, 1 active)" in bar.descriptions
    assert bar.desc == "Downloading raw hours (2/2 done)"


def test_download_raw_hours_reports_404_as_archive_missing_and_accepts_empty_parquet(
    monkeypatch, tmp_path: Path
) -> None:
    empty_payload = _empty_parquet_payload()

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        if request.full_url.endswith("2026-03-21T09.parquet"):
            raise HTTPError(request.full_url, 404, "missing", hdrs=None, fp=None)
        return _Response(empty_payload, headers={"Content-Length": str(len(empty_payload))})

    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=tmp_path / "raws",
        source_order=["archive"],
        show_progress=False,
        **_window_kwargs(),
    )

    assert summary.archive_missing_hours == ["2026-03-21T09:00:00+00:00"]
    assert summary.failed_hours == []
    assert summary.missing_local_hours == []
    assert summary.empty_local_hours == []
    assert summary.zero_row_local_hours == []
    assert summary.small_local_hours == []


def test_download_raw_hours_progress_output_includes_failure_error(
    monkeypatch, tmp_path: Path
) -> None:
    bars: list[_FakeTqdm] = []

    monkeypatch.setattr(
        raw_download,
        "tqdm",
        lambda *args, **kwargs: bars.append(_FakeTqdm(*args, **kwargs)) or bars[-1],
    )  # type: ignore[func-returns-value]

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        raise HTTPError(request.full_url, 503, "unavailable", hdrs=None, fp=None)

    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=tmp_path / "raws",
        source_order=["archive"],
        show_progress=True,
        **_window_kwargs("2026-03-21T09", "2026-03-21T09"),
    )

    assert summary.failed_hours == ["2026-03-21T09:00:00+00:00"]
    assert len(bars) == 1
    assert any("failed" in line and "last_error=HTTP 503" in line for line in bars[0].writes)


def test_download_raw_hours_uses_direct_hour_window(monkeypatch, tmp_path: Path) -> None:
    payload = _raw_parquet_payload()
    requested_urls: list[str] = []

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        if request.get_method() != "HEAD":
            requested_urls.append(request.full_url)
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=tmp_path / "raws",
        source_order=["archive"],
        show_progress=False,
        **_window_kwargs("2026-03-21T09", "2026-03-21T11"),
    )

    assert summary.requested_hours == 3
    assert summary.archive_listed_hours == 3
    assert summary.downloaded_hours == 3
    assert summary.failed_hours == []
    assert summary.missing_local_hours == []
    assert summary.archive_missing_hours == []
    assert requested_urls == [
        "https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T11.parquet",
        "https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T10.parquet",
        "https://r2v2.pmxt.dev/polymarket_orderbook_2026-03-21T09.parquet",
    ]


def test_download_raw_hours_keeps_larger_overlap_source(monkeypatch, tmp_path: Path) -> None:
    small_payload = b"s" * 10
    large_payload = b"l" * 20
    requested_urls: list[str] = []

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        is_r2 = request.full_url.startswith("https://r2.pmxt.dev/")
        payload = large_payload if is_r2 else small_payload
        if request.get_method() == "HEAD":
            return _Response(b"", headers={"Content-Length": str(len(payload))})
        requested_urls.append(request.full_url)
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=tmp_path / "raws",
        archive_sources=[
            ("https://archive.pmxt.dev/Polymarket/v2", "https://r2v2.pmxt.dev"),
            ("https://archive.pmxt.dev/Polymarket/v1", "https://r2.pmxt.dev"),
        ],
        source_order=["archive"],
        show_progress=False,
        **_window_kwargs("2026-03-21T10", "2026-03-21T10"),
    )

    assert summary.archive_listed_hours == 1
    assert summary.requested_hours == 1
    assert summary.archive_sources == ["https://r2v2.pmxt.dev", "https://r2.pmxt.dev"]
    assert requested_urls == [
        "https://r2.pmxt.dev/polymarket_orderbook_2026-03-21T10.parquet",
    ]
    downloaded = (
        tmp_path / "raws" / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T10.parquet"
    )
    assert downloaded.read_bytes() == large_payload


def test_download_raw_hours_refreshes_existing_when_upstream_is_larger(
    monkeypatch, tmp_path: Path
) -> None:
    payload = _raw_parquet_payload()
    destination = tmp_path / "raws"
    existing_path = (
        destination / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T09.parquet"
    )
    existing_path.parent.mkdir(parents=True, exist_ok=True)
    existing_path.write_bytes(b"old")

    monkeypatch.setattr(raw_download, "_local_raw_is_empty", lambda path: False)

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        if request.get_method() == "HEAD":
            return _Response(b"", headers={"Content-Length": "1024"})
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setattr(raw_download, "urlopen", fake_urlopen)

    summary = raw_download.download_raw_hours(
        destination=destination,
        source_order=["archive"],
        show_progress=False,
        **_window_kwargs("2026-03-21T09", "2026-03-21T09"),
    )

    assert summary.downloaded_hours == 1
    assert summary.refreshed_existing_hours == 1
    assert summary.skipped_existing_hours == 0
    assert existing_path.read_bytes() == payload
