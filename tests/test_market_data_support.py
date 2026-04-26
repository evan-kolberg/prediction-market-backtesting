from __future__ import annotations

from prediction_market_extensions.backtesting import data_sources
from prediction_market_extensions.backtesting._market_data_support import (
    build_single_market_replay,
    resolve_market_data_support,
    supported_market_data_keys,
)
from prediction_market_extensions.backtesting._replay_specs import BookReplay
from prediction_market_extensions.backtesting.data_sources import (
    PMXT,
    TELONEX_VENDOR,
    Book,
    Polymarket,
)


def test_support_matrix_matches_publicly_supported_combinations() -> None:
    assert set(supported_market_data_keys()) == {
        ("polymarket", "book", "pmxt"),
        ("polymarket", "book", "telonex"),
    }

    for platform, data_type, vendor in (
        (Polymarket, Book, PMXT),
        (Polymarket, Book, TELONEX_VENDOR),
    ):
        support = resolve_market_data_support(
            platform=platform,
            data_type=data_type,
            vendor=vendor,
        )

        assert support.adapter.key.platform == platform.name
        assert support.adapter.key.vendor == vendor.name
        assert support.adapter.key.data_type == data_type.name


def test_single_market_replay_construction_is_adapter_owned() -> None:
    pmxt = resolve_market_data_support(
        platform=Polymarket,
        data_type=Book,
        vendor=PMXT,
    )

    assert build_single_market_replay(
        support=pmxt,
        field_values={
            "market_slug": "demo-market",
            "token_index": 1,
            "start_time": "2026-03-24T03:00:00Z",
            "end_time": "2026-03-24T08:00:00Z",
        },
    ) == BookReplay(
        market_slug="demo-market",
        token_index=1,
        start_time="2026-03-24T03:00:00Z",
        end_time="2026-03-24T08:00:00Z",
    )


def test_telonex_vendor_is_exported() -> None:
    assert data_sources.Telonex.name == "telonex"
    assert data_sources.TELONEX_VENDOR.name == "telonex"
