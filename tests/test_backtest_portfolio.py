"""Unit tests for Portfolio position and cash tracking."""

from __future__ import annotations

from datetime import datetime

import pytest

from src.backtesting.models import Fill, OrderAction, Side
from src.backtesting.portfolio import Portfolio


def _fill(
    action: str = "buy",
    side: str = "yes",
    price: float = 0.50,
    quantity: float = 10.0,
    market_id: str = "MKT-A",
    commission: float = 0.0,
) -> Fill:
    """Helper to create a fill."""
    return Fill(
        order_id="test",
        market_id=market_id,
        action=OrderAction(action),
        side=Side(side),
        price=price,
        quantity=quantity,
        timestamp=datetime(2024, 1, 15),
        commission=commission,
    )


class TestBuyYes:
    def test_cash_decreases(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        assert p.cash == pytest.approx(996.0)

    def test_position_created(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        pos = p.positions["MKT-A"]
        assert pos.quantity == pytest.approx(10.0)
        assert pos.avg_entry_price == pytest.approx(0.40)

    def test_adding_to_position(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        p.apply_fill(_fill(action="buy", side="yes", price=0.60, quantity=10.0))
        pos = p.positions["MKT-A"]
        assert pos.quantity == pytest.approx(20.0)
        assert pos.avg_entry_price == pytest.approx(0.50)


class TestSellYes:
    def test_cash_increases(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        p.apply_fill(_fill(action="sell", side="yes", price=0.60, quantity=10.0))
        assert p.cash == pytest.approx(1000.0 - 4.0 + 6.0)

    def test_position_closed(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        p.apply_fill(_fill(action="sell", side="yes", price=0.60, quantity=10.0))
        pos = p.positions["MKT-A"]
        assert pos.quantity == pytest.approx(0.0)

    def test_realized_pnl(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        p.apply_fill(_fill(action="sell", side="yes", price=0.60, quantity=10.0))
        pos = p.positions["MKT-A"]
        assert pos.realized_pnl == pytest.approx(2.0)


class TestBuyNo:
    def test_cash_decreases_by_no_price(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="no", price=0.30, quantity=10.0))
        assert p.cash == pytest.approx(997.0)

    def test_position_is_negative(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="no", price=0.30, quantity=10.0))
        pos = p.positions["MKT-A"]
        assert pos.quantity == pytest.approx(-10.0)


class TestSellNo:
    def test_cash_increases_by_no_price(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="no", price=0.30, quantity=10.0))
        p.apply_fill(_fill(action="sell", side="no", price=0.40, quantity=10.0))
        assert p.cash == pytest.approx(1000.0 - 3.0 + 4.0)


class TestResolution:
    def test_resolve_yes_long_yes(self) -> None:
        """Long YES position, market resolves YES → receives $1 per contract."""
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        pnl = p.resolve_market("MKT-A", Side.YES)
        assert pnl == pytest.approx(6.0)
        assert p.cash == pytest.approx(1000.0 - 4.0 + 10.0)

    def test_resolve_no_long_yes(self) -> None:
        """Long YES position, market resolves NO → receives $0."""
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        pnl = p.resolve_market("MKT-A", Side.NO)
        assert pnl == pytest.approx(-4.0)
        assert p.cash == pytest.approx(1000.0 - 4.0 + 0.0)

    def test_resolve_yes_long_no(self) -> None:
        """Long NO position (short YES), market resolves YES → loses."""
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="no", price=0.30, quantity=10.0))
        pnl = p.resolve_market("MKT-A", Side.YES)
        # quantity is -10, settlement = 1.0, payout = -10
        assert pnl == pytest.approx(-10.0 - (-10.0 * 0.70))
        assert p.cash == pytest.approx(1000.0 - 3.0 + (-10.0))

    def test_resolve_no_long_no(self) -> None:
        """Long NO position, market resolves NO → profit."""
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="no", price=0.30, quantity=10.0))
        pnl = p.resolve_market("MKT-A", Side.NO)
        # quantity is -10, settlement = 0.0, payout = 0
        # pnl = 0 - (-10 * 0.70) = 7.0
        assert pnl == pytest.approx(7.0)
        assert p.cash == pytest.approx(1000.0 - 3.0 + 0.0)

    def test_double_resolve_ignored(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        p.resolve_market("MKT-A", Side.YES)
        cash_after = p.cash
        pnl = p.resolve_market("MKT-A", Side.YES)
        assert pnl == 0.0
        assert p.cash == cash_after

    def test_resolve_empty_position(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        pnl = p.resolve_market("MKT-A", Side.YES)
        assert pnl == 0.0


class TestCommission:
    def test_commission_deducted(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0, commission=0.10))
        assert p.cash == pytest.approx(1000.0 - 4.0 - 0.10)


class TestSnapshot:
    def test_unrealized_pnl(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.apply_fill(_fill(action="buy", side="yes", price=0.40, quantity=10.0))
        p.update_price("MKT-A", 0.60)
        snap = p.snapshot(datetime(2024, 1, 15))
        assert snap.unrealized_pnl == pytest.approx(2.0)
        assert snap.total_equity == pytest.approx(1000.0 - 4.0 + 2.0)
        assert snap.num_positions == 1

    def test_snapshot_stored(self) -> None:
        p = Portfolio(initial_cash=1000.0)
        p.snapshot(datetime(2024, 1, 15))
        assert len(p.equity_curve) == 1
