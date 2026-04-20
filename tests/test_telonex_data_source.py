from __future__ import annotations

import os

import pytest

from prediction_market_extensions.backtesting.data_sources.telonex import (
    TELONEX_API_KEY_ENV,
    TELONEX_LOCAL_DIR_ENV,
    RunnerPolymarketTelonexQuoteDataLoader,
    configured_telonex_data_source,
    resolve_telonex_loader_config,
)


def test_configured_telonex_data_source_preserves_explicit_order(tmp_path) -> None:
    local_root = tmp_path / "telonex"
    local_root.mkdir()

    with configured_telonex_data_source(
        sources=[f"local:{local_root}", "api:https://api.example.test"]
    ) as selection:
        assert selection.mode == "auto"
        assert selection.summary == (
            f"Telonex source: explicit priority (local {local_root} -> "
            "api https://api.example.test (key missing))"
        )

        _selection, config = resolve_telonex_loader_config()
        assert config.channel == "quotes"
        assert [(entry.kind, entry.target) for entry in config.ordered_source_entries] == [
            ("local", str(local_root)),
            ("api", "https://api.example.test"),
        ]


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
