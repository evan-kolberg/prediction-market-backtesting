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
            f"Telonex source: explicit priority (local {local_root} -> api https://api.example.test)"
        )

        _selection, config = resolve_telonex_loader_config()
        assert config.channel == "quotes"
        assert [(entry.kind, entry.target) for entry in config.ordered_source_entries] == [
            ("local", str(local_root)),
            ("api", "https://api.example.test"),
        ]


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


def test_telonex_rejects_unprefixed_sources() -> None:
    with pytest.raises(ValueError, match="Use one of: local:, api:"):
        resolve_telonex_loader_config(sources=["https://api.telonex.io"])
