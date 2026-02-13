"""Tests for data feed loading and normalization."""

from __future__ import annotations

from pathlib import Path

from src.backtesting.feeds.kalshi import KalshiFeed
from src.backtesting.models import MarketStatus, Platform, Side


class TestKalshiFeedMarkets:
    def test_loads_markets(self, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(markets_dir=bt_kalshi_markets_dir)
        markets = feed.markets()
        assert len(markets) == 3
        assert "BT-MKT-A" in markets

    def test_market_status(self, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(markets_dir=bt_kalshi_markets_dir)
        markets = feed.markets()
        assert markets["BT-MKT-A"].status == MarketStatus.RESOLVED_YES
        assert markets["BT-MKT-A"].result == Side.YES
        assert markets["BT-MKT-B"].status == MarketStatus.RESOLVED_NO
        assert markets["BT-MKT-B"].result == Side.NO

    def test_market_platform(self, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(markets_dir=bt_kalshi_markets_dir)
        markets = feed.markets()
        for m in markets.values():
            assert m.platform == Platform.KALSHI


class TestKalshiFeedTrades:
    def test_yields_trades(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        trades = list(feed.trades())
        assert len(trades) == 10

    def test_chronological_order(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        trades = list(feed.trades())
        for i in range(1, len(trades)):
            assert trades[i].timestamp >= trades[i - 1].timestamp

    def test_price_normalization(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        trades = list(feed.trades())
        for t in trades:
            assert 0.0 <= t.yes_price <= 1.0
            assert 0.0 <= t.no_price <= 1.0
            assert abs(t.yes_price + t.no_price - 1.0) < 1e-9

    def test_filter_by_market(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        trades = list(feed.trades(market_ids=["BT-MKT-A"]))
        assert all(t.market_id == "BT-MKT-A" for t in trades)
        assert len(trades) == 4

    def test_platform_set(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        trades = list(feed.trades())
        for t in trades:
            assert t.platform == Platform.KALSHI

    def test_caches_markets(self, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(markets_dir=bt_kalshi_markets_dir)
        m1 = feed.markets()
        m2 = feed.markets()
        assert m1 is m2


class TestKalshiFeedTradeCount:
    def test_count_all(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        assert feed.trade_count() == 10

    def test_count_filtered_by_market(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        assert feed.trade_count(market_ids=["BT-MKT-A"]) == 4

    def test_count_matches_trades(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        assert feed.trade_count() == len(list(feed.trades()))
