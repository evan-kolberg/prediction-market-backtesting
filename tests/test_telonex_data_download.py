from __future__ import annotations

from pathlib import Path

import pytest

from scripts import _telonex_data_download as telonex_download


class _Response:
    def __init__(self, payload: bytes, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self._offset = 0
        self.headers = headers or {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
        return False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def test_download_telonex_days_writes_loader_local_layout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    payload = b"telonex-parquet"
    requested_urls: list[str] = []
    auth_headers: list[str | None] = []

    def fake_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        del timeout
        requested_urls.append(request.full_url)
        auth_headers.append(request.headers.get("Authorization"))
        return _Response(payload, headers={"Content-Length": str(len(payload))})

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "urlopen", fake_urlopen)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["us-recession-by-end-of-2026"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-20",
        show_progress=False,
    )

    assert summary.requested_days == 2
    assert summary.downloaded_days == 2
    assert summary.skipped_existing_days == 0
    assert summary.failed_days == []
    assert summary.missing_days == []
    assert requested_urls == [
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0",
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-20?slug=us-recession-by-end-of-2026&outcome_id=0",
    ]
    assert auth_headers == ["Bearer test-key", "Bearer test-key"]
    assert (
        tmp_path
        / "polymarket"
        / "quotes"
        / "us-recession-by-end-of-2026"
        / "0"
        / "2026-01-20.parquet"
    ).read_bytes() == payload


def test_download_telonex_days_requires_key_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    monkeypatch.delenv("TELONEX_API_KEY", raising=False)

    with pytest.raises(ValueError, match="TELONEX_API_KEY"):
        telonex_download.download_telonex_days(
            destination=tmp_path,
            market_slugs=["us-recession-by-end-of-2026"],
            outcome_id=0,
            start_date="2026-01-19",
            end_date="2026-01-19",
            show_progress=False,
        )


def test_download_telonex_days_skips_existing_without_api_call(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    destination = (
        tmp_path
        / "polymarket"
        / "quotes"
        / "us-recession-by-end-of-2026"
        / "0"
        / "2026-01-19.parquet"
    )
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"existing")

    def unexpected_urlopen(request, timeout=60):  # type: ignore[no-untyped-def]
        raise AssertionError(f"unexpected Telonex request for {request.full_url}")

    monkeypatch.setenv("TELONEX_API_KEY", "test-key")
    monkeypatch.setattr(telonex_download, "urlopen", unexpected_urlopen)

    summary = telonex_download.download_telonex_days(
        destination=tmp_path,
        market_slugs=["us-recession-by-end-of-2026"],
        outcome_id=0,
        start_date="2026-01-19",
        end_date="2026-01-19",
        show_progress=False,
    )

    assert summary.downloaded_days == 0
    assert summary.skipped_existing_days == 1
