from __future__ import annotations

import os
import subprocess
import importlib
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

import prediction_market_extensions.backtesting.data_sources.telonex as telonex_module
from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_CACHE_ROOT_ENV,
    TELONEX_API_KEY_ENV,
    TELONEX_LOCAL_DIR_ENV,
    RunnerPolymarketTelonexQuoteDataLoader,
    configured_telonex_data_source,
    resolve_telonex_loader_config,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class _FakeHTTPResponse:
    def __init__(self, payload: bytes) -> None:
        self._buffer = BytesIO(payload)
        self.headers = {"Content-Length": str(len(payload))}

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        return self._buffer.read(size)


def _quote_parquet_payload(timestamp_us: int) -> bytes:
    buffer = BytesIO()
    pd.DataFrame(
        {
            "timestamp_us": [timestamp_us],
            "bid_price": [0.42],
            "ask_price": [0.44],
            "bid_size": [10.0],
            "ask_size": [11.0],
        }
    ).to_parquet(buffer, index=False)
    return buffer.getvalue()


def test_configured_telonex_data_source_preserves_explicit_order(tmp_path) -> None:
    local_root = tmp_path / "telonex"
    local_root.mkdir()

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ) as selection:
        assert selection.mode == "auto"
        assert selection.summary == (
            f"Telonex source: explicit priority (cache -> local {local_root} -> "
            "api https://api.example.test (key missing))"
        )

        _selection, config = resolve_telonex_loader_config()
        assert config.channel == "quotes"
        assert [(entry.kind, entry.target) for entry in config.ordered_source_entries] == [
            ("local", str(local_root)),
            ("api", "https://api.example.test"),
        ]


def test_configured_telonex_data_source_omits_disabled_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    local_root = tmp_path / "telonex"
    local_root.mkdir()
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, "0")

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ) as selection:
        assert selection.summary == (
            f"Telonex source: explicit priority (local {local_root} -> "
            "api https://api.example.test (key missing))"
        )


def test_configured_telonex_data_source_expands_env_in_api_key(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "sk-live-42")
    with configured_telonex_data_source(sources=["api:${TELONEX_API_KEY}"]) as selection:
        assert "(key set)" in selection.summary
        _selection, config = resolve_telonex_loader_config()
        assert config.ordered_source_entries[0].api_key == "sk-live-42"


def test_configured_telonex_data_source_reports_missing_key_when_var_unset(monkeypatch) -> None:
    monkeypatch.delenv("TELONEX_API_KEY", raising=False)
    with configured_telonex_data_source(sources=["api:${TELONEX_API_KEY}"]) as selection:
        assert "(key missing)" in selection.summary
        _selection, config = resolve_telonex_loader_config()
        assert config.ordered_source_entries[0].api_key is None


def test_telonex_default_api_source_requires_key_only_from_env(monkeypatch) -> None:
    monkeypatch.delenv(TELONEX_LOCAL_DIR_ENV, raising=False)
    monkeypatch.delenv(TELONEX_API_KEY_ENV, raising=False)

    with pytest.raises(ValueError, match=TELONEX_API_KEY_ENV):
        resolve_telonex_loader_config()

    monkeypatch.setenv(TELONEX_API_KEY_ENV, "test-key")
    _selection, config = resolve_telonex_loader_config()

    assert [(entry.kind, entry.target) for entry in config.ordered_source_entries] == [
        ("api", "https://api.telonex.io")
    ]
    assert os.getenv(TELONEX_API_KEY_ENV) == "test-key"


def test_telonex_api_url_uses_slug_and_outcome_id_without_key() -> None:
    url = RunnerPolymarketTelonexQuoteDataLoader._api_url(
        base_url="https://api.telonex.io/",
        channel="quotes",
        date="2026-01-20",
        market_slug="will-the-us-strike-iran-next-433",
        token_index=1,
        outcome=None,
    )

    assert url == (
        "https://api.telonex.io/v1/downloads/polymarket/quotes/2026-01-20"
        "?slug=will-the-us-strike-iran-next-433&outcome_id=1"
    )


def test_telonex_runner_api_downloads_cache_then_clear(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    cache_root = tmp_path / "telonex-cache"
    monkeypatch.setenv(TELONEX_CACHE_ROOT_ENV, str(cache_root))
    payload = _quote_parquet_payload(1_768_780_800_000_000)
    resolve_calls: list[tuple[str, str]] = []
    fetch_calls: list[str] = []

    def fake_resolve_presigned_url(*, url: str, api_key: str) -> str:
        resolve_calls.append((url, api_key))
        return "https://presigned.example.test/day.parquet"

    def fake_urlopen(request, timeout: int):  # type: ignore[no-untyped-def]
        fetch_calls.append(request.full_url)
        return _FakeHTTPResponse(payload)

    monkeypatch.setattr(
        RunnerPolymarketTelonexQuoteDataLoader,
        "_resolve_presigned_url",
        staticmethod(fake_resolve_presigned_url),
    )
    monkeypatch.setattr(telonex_module, "urlopen", fake_urlopen)

    loader = RunnerPolymarketTelonexQuoteDataLoader.__new__(RunnerPolymarketTelonexQuoteDataLoader)
    load_kwargs = {
        "base_url": "https://api.example.test",
        "channel": "quotes",
        "date": "2026-01-19",
        "market_slug": "us-recession-by-end-of-2026",
        "token_index": 0,
        "outcome": None,
    }

    first = loader._load_api_day(**load_kwargs, api_key="test-key")

    assert first is not None
    assert len(first) == 1
    assert len(resolve_calls) == 1
    assert len(fetch_calls) == 1
    cache_path = loader._api_cache_path(**load_kwargs)
    assert cache_path is not None
    assert cache_path.exists()
    assert cache_path.is_relative_to(cache_root)
    assert loader._telonex_last_api_source == (
        "telonex-api::https://api.example.test/v1/downloads/polymarket/quotes/"
        "2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0"
    )

    def fail_resolve_presigned_url(*, url: str, api_key: str) -> str:
        raise AssertionError("cache hit should not request a presigned URL")

    def fail_urlopen(request, timeout: int):  # type: ignore[no-untyped-def]
        raise AssertionError("cache hit should not download from Telonex")

    monkeypatch.setattr(
        RunnerPolymarketTelonexQuoteDataLoader,
        "_resolve_presigned_url",
        staticmethod(fail_resolve_presigned_url),
    )
    monkeypatch.setattr(telonex_module, "urlopen", fail_urlopen)

    second = loader._load_api_day(**load_kwargs, api_key=None)

    assert second is not None
    pd.testing.assert_frame_equal(first, second)
    assert loader._telonex_last_api_source == f"telonex-cache::{cache_path}"

    result = subprocess.run(
        [
            "make",
            "clear-telonex-cache",
            f"TELONEX_CACHE_ROOT={cache_root}",
            f"TELONEX_DATA_DESTINATION={tmp_path / 'telonex-data'}",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert 'rm -rf "' in result.stdout
    assert str(cache_root) in result.stdout
    assert cache_root.exists()
    assert not cache_path.exists()

    monkeypatch.setattr(
        RunnerPolymarketTelonexQuoteDataLoader,
        "_resolve_presigned_url",
        staticmethod(fake_resolve_presigned_url),
    )
    monkeypatch.setattr(telonex_module, "urlopen", fake_urlopen)

    third = loader._load_api_day(**load_kwargs, api_key="test-key")

    assert third is not None
    pd.testing.assert_frame_equal(first, third)
    assert len(resolve_calls) == 2
    assert len(fetch_calls) == 2
    assert cache_path.exists()
    assert loader._telonex_last_api_source == (
        "telonex-api::https://api.example.test/v1/downloads/polymarket/quotes/"
        "2026-01-19?slug=us-recession-by-end-of-2026&outcome_id=0"
    )


def test_telonex_load_quotes_uses_cache_before_local(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexQuoteDataLoader
    loader = loader_cls.__new__(loader_cls)
    config = module.TelonexLoaderConfig(
        channel="quotes",
        ordered_source_entries=(
            module.TelonexSourceEntry(kind="local", target="/tmp/local"),
            module.TelonexSourceEntry(
                kind="api", target="https://api.example.test", api_key="test-key"
            ),
        ),
    )
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000],
            "bid_price": [0.42],
            "ask_price": [0.44],
            "bid_size": [10.0],
            "ask_size": [11.0],
        }
    )
    calls: list[str] = []

    monkeypatch.setattr(loader, "_config", lambda: config)

    def fake_cache(**kwargs: object) -> pd.DataFrame:
        calls.append("cache")
        return frame

    def fail_local(**kwargs: object) -> None:
        calls.append("local")
        raise AssertionError("local should not be checked when cache has the day")

    def fail_source(**kwargs: object) -> None:
        calls.append("api")
        raise AssertionError("api should not be checked when cache has the day")

    monkeypatch.setattr(loader, "_load_api_cache_day", fake_cache)
    monkeypatch.setattr(loader, "_try_load_day_from_local", fail_local)
    monkeypatch.setattr(loader, "_try_load_day_from_entry", fail_source)
    monkeypatch.setattr(
        loader,
        "_quote_ticks_from_frame",
        lambda _frame, *, start, end: [SimpleNamespace(ts_event=1)],
    )

    records = loader.load_quotes(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
        market_slug="cache-test",
        token_index=0,
        outcome=None,
    )

    assert len(records) == 1
    assert calls == ["cache"]


def test_telonex_load_quotes_uses_local_before_api_after_cache_miss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.reload(telonex_module)
    loader_cls = module.RunnerPolymarketTelonexQuoteDataLoader
    loader = loader_cls.__new__(loader_cls)
    config = module.TelonexLoaderConfig(
        channel="quotes",
        ordered_source_entries=(
            module.TelonexSourceEntry(kind="local", target="/tmp/local"),
            module.TelonexSourceEntry(
                kind="api", target="https://api.example.test", api_key="test-key"
            ),
        ),
    )
    frame = pd.DataFrame(
        {
            "timestamp_us": [1_768_780_800_000_000],
            "bid_price": [0.42],
            "ask_price": [0.44],
            "bid_size": [10.0],
            "ask_size": [11.0],
        }
    )
    calls: list[str] = []

    monkeypatch.setattr(loader, "_config", lambda: config)

    def fake_cache(**kwargs: object) -> None:
        calls.append("cache")
        return None

    def fake_local(**kwargs: object) -> pd.DataFrame:
        calls.append("local")
        return frame

    def fail_source(**kwargs: object) -> None:
        calls.append("api")
        raise AssertionError("api should not be checked when local has the day")

    monkeypatch.setattr(loader, "_load_api_cache_day", fake_cache)
    monkeypatch.setattr(loader, "_try_load_day_from_local", fake_local)
    monkeypatch.setattr(loader, "_try_load_day_from_entry", fail_source)
    monkeypatch.setattr(
        loader,
        "_quote_ticks_from_frame",
        lambda _frame, *, start, end: [SimpleNamespace(ts_event=1)],
    )

    records = loader.load_quotes(
        pd.Timestamp("2026-01-19", tz="UTC"),
        pd.Timestamp("2026-01-19 23:59:59", tz="UTC"),
        market_slug="local-test",
        token_index=0,
        outcome=None,
    )

    assert len(records) == 1
    assert calls == ["cache", "local"]


def test_telonex_local_range_matches_consolidated_download_script_layout(tmp_path) -> None:
    loader = RunnerPolymarketTelonexQuoteDataLoader.__new__(RunnerPolymarketTelonexQuoteDataLoader)
    local_path = tmp_path / "polymarket" / "us-recession-by-end-of-2026" / "0" / "quotes.parquet"
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"parquet")

    assert (
        loader._local_consolidated_path(
            root=tmp_path,
            channel="quotes",
            market_slug="us-recession-by-end-of-2026",
            token_index=0,
            outcome=None,
        )
        == local_path
    )


def test_telonex_local_path_matches_daily_download_script_layout(tmp_path) -> None:
    loader = RunnerPolymarketTelonexQuoteDataLoader.__new__(RunnerPolymarketTelonexQuoteDataLoader)
    local_path = (
        tmp_path
        / "polymarket"
        / "us-recession-by-end-of-2026"
        / "0"
        / "quotes"
        / "2026-01-19.parquet"
    )
    local_path.parent.mkdir(parents=True)
    local_path.write_bytes(b"parquet")

    assert (
        loader._local_path_for_day(
            root=tmp_path,
            channel="quotes",
            date="2026-01-19",
            market_slug="us-recession-by-end-of-2026",
            token_index=0,
            outcome=None,
        )
        == local_path
    )


def test_telonex_rejects_unprefixed_sources() -> None:
    with pytest.raises(ValueError, match="Use one of: local:, api:"):
        resolve_telonex_loader_config(sources=["https://api.telonex.io"])
