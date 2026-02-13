"""Unit tests for backtesting data models."""

from __future__ import annotations

from datetime import datetime

from src.backtesting.models import (
    BacktestResult,
    Fill,
    MarketInfo,
    MarketStatus,
    Order,
    OrderAction,
    OrderStatus,
    Platform,
    PortfolioSnapshot,
    Position,
    Side,
    TradeEvent,
)


class TestEnums:
    def test_platform_values(self) -> None:
        assert Platform.KALSHI.value == "kalshi"
        assert Platform.POLYMARKET.value == "polymarket"

    def test_side_values(self) -> None:
        assert Side.YES.value == "yes"
        assert Side.NO.value == "no"

    def test_order_action_values(self) -> None:
        assert OrderAction.BUY.value == "buy"
        assert OrderAction.SELL.value == "sell"

    def test_order_status_values(self) -> None:
        assert OrderStatus.PENDING.value == "pending"
        assert OrderStatus.FILLED.value == "filled"
        assert OrderStatus.CANCELLED.value == "cancelled"

    def test_market_status_values(self) -> None:
        assert MarketStatus.OPEN.value == "open"
        assert MarketStatus.RESOLVED_YES.value == "resolved_yes"


class TestTradeEvent:
    def test_creation(self) -> None:
        t = TradeEvent(
            timestamp=datetime(2024, 1, 1),
            market_id="MKT-A",
            platform=Platform.KALSHI,
            yes_price=0.65,
            no_price=0.35,
            quantity=10.0,
            taker_side=Side.YES,
        )
        assert t.yes_price == 0.65
        assert t.no_price == 0.35
        assert t.raw_id is None

    def test_price_normalization_range(self) -> None:
        t = TradeEvent(
            timestamp=datetime(2024, 1, 1),
            market_id="MKT-A",
            platform=Platform.KALSHI,
            yes_price=0.01,
            no_price=0.99,
            quantity=1.0,
            taker_side=Side.YES,
        )
        assert 0.0 <= t.yes_price <= 1.0
        assert 0.0 <= t.no_price <= 1.0


class TestOrder:
    def test_defaults(self) -> None:
        o = Order()
        assert o.status == OrderStatus.PENDING
        assert o.action == OrderAction.BUY
        assert o.side == Side.YES
        assert len(o.order_id) > 0

    def test_unique_ids(self) -> None:
        o1 = Order()
        o2 = Order()
        assert o1.order_id != o2.order_id


class TestPosition:
    def test_defaults(self) -> None:
        p = Position(market_id="MKT-A")
        assert p.quantity == 0.0
        assert p.avg_entry_price == 0.0
        assert p.realized_pnl == 0.0


class TestFill:
    def test_creation(self) -> None:
        f = Fill(
            order_id="abc",
            market_id="MKT-A",
            action=OrderAction.BUY,
            side=Side.YES,
            price=0.50,
            quantity=10.0,
            timestamp=datetime(2024, 1, 1),
        )
        assert f.commission == 0.0


class TestPortfolioSnapshot:
    def test_creation(self) -> None:
        s = PortfolioSnapshot(
            timestamp=datetime(2024, 1, 1),
            cash=10000.0,
            total_equity=10500.0,
            unrealized_pnl=500.0,
            num_positions=3,
        )
        assert s.total_equity == 10500.0


class TestBacktestResult:
    def test_creation(self) -> None:
        r = BacktestResult(
            equity_curve=[],
            fills=[],
            metrics={},
            strategy_name="test",
            platform=Platform.KALSHI,
            start_time=None,
            end_time=None,
            initial_cash=10000.0,
            final_equity=10000.0,
            num_markets_traded=0,
            num_markets_resolved=0,
        )
        assert r.strategy_name == "test"


class TestMarketInfo:
    def test_creation(self) -> None:
        m = MarketInfo(
            market_id="MKT-A",
            platform=Platform.KALSHI,
            title="Test Market",
            open_time=datetime(2024, 1, 1),
            close_time=datetime(2024, 1, 2),
            result=Side.YES,
            status=MarketStatus.RESOLVED_YES,
        )
        assert m.event_id is None
        assert m.token_id_map is None
