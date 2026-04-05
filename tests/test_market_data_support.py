from __future__ import annotations

from backtests._shared._market_data_support import load_single_market_runner
from backtests._shared._market_data_support import resolve_market_data_support
from backtests._shared._market_data_support import supported_market_data_keys
from backtests._shared.data_sources import Kalshi
from backtests._shared.data_sources import Native
from backtests._shared.data_sources import PMXT
from backtests._shared.data_sources import Polymarket
from backtests._shared.data_sources import QuoteTick
from backtests._shared.data_sources import TradeTick
import backtests._shared.data_sources as data_sources


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
        support = resolve_market_data_support(
            platform=platform,
            data_type=data_type,
            vendor=vendor,
        )
        runner_fn = load_single_market_runner(support.single_market_runner)
        assert callable(runner_fn)


def test_unsupported_vendor_is_not_exported() -> None:
    assert not hasattr(data_sources, "Telonex")
    assert not hasattr(data_sources, "TELONEX_VENDOR")
