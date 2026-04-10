from __future__ import annotations

import prediction_market_extensions.backtesting.data_sources as data_sources

from prediction_market_extensions.backtesting._market_data_support import build_single_market_replay
from prediction_market_extensions.backtesting._market_data_support import (
    resolve_market_data_support,
)
from prediction_market_extensions.backtesting._market_data_support import supported_market_data_keys
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting._replay_specs import TradeReplay
from prediction_market_extensions.backtesting.data_sources import Kalshi
from prediction_market_extensions.backtesting.data_sources import Native
from prediction_market_extensions.backtesting.data_sources import PMXT
from prediction_market_extensions.backtesting.data_sources import Polymarket
from prediction_market_extensions.backtesting.data_sources import QuoteTick
from prediction_market_extensions.backtesting.data_sources import TradeTick


def test_support_matrix_matches_publicly_supported_combinations() -> None:
    assert set(supported_market_data_keys()) == {
        ("kalshi", "trade_tick", "native"),
        ("polymarket", "trade_tick", "native"),
        ("polymarket", "quote_tick", "pmxt"),
    }

    for platform, data_type, vendor in (
        (Kalshi, TradeTick, Native),
        (Polymarket, TradeTick, Native),
        (Polymarket, QuoteTick, PMXT),
    ):
        support = resolve_market_data_support(platform=platform, data_type=data_type, vendor=vendor)
        assert support.adapter.key.platform == platform.name
        assert support.adapter.key.vendor == vendor.name
        assert support.adapter.key.data_type == data_type.name


def test_single_market_replay_construction_is_adapter_owned() -> None:
    kalshi = resolve_market_data_support(platform=Kalshi, data_type=TradeTick, vendor=Native)
    assert build_single_market_replay(
        support=kalshi, field_values={"market_ticker": "KALSHI-TEST", "lookback_days": 2}
    ) == TradeReplay(market_ticker="KALSHI-TEST", lookback_days=2)

    polymarket = resolve_market_data_support(
        platform=Polymarket, data_type=TradeTick, vendor=Native
    )
    assert build_single_market_replay(
        support=polymarket, field_values={"market_slug": "demo-market", "token_index": 1}
    ) == TradeReplay(market_slug="demo-market", token_index=1)

    pmxt = resolve_market_data_support(platform=Polymarket, data_type=QuoteTick, vendor=PMXT)
    assert build_single_market_replay(
        support=pmxt,
        field_values={
            "market_slug": "demo-market",
            "token_index": 1,
            "start_time": "2026-03-24T03:00:00Z",
            "end_time": "2026-03-24T08:00:00Z",
        },
    ) == QuoteReplay(
        market_slug="demo-market",
        token_index=1,
        start_time="2026-03-24T03:00:00Z",
        end_time="2026-03-24T08:00:00Z",
    )


def test_unsupported_vendor_is_not_exported() -> None:
    assert not hasattr(data_sources, "Telonex")
    assert not hasattr(data_sources, "TELONEX_VENDOR")
