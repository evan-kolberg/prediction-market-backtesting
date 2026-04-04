from __future__ import annotations

from decimal import Decimal

from strategies.core import _cap_entry_size_to_free_balance
from strategies.core import _cap_entry_size_to_visible_liquidity
from strategies.core import _effective_entry_reference_price
from strategies.core import _estimate_entry_unit_cost


def test_estimate_entry_unit_cost_includes_polymarket_taker_fee() -> None:
    unit_cost = _estimate_entry_unit_cost(
        reference_price=Decimal("0.95"),
        taker_fee=Decimal("0.0035"),
    )

    assert unit_cost == Decimal("0.95016625")


def test_cap_entry_size_to_free_balance_reserves_fee_headroom() -> None:
    capped_size = _cap_entry_size_to_free_balance(
        desired_size=Decimal("100"),
        reference_price=Decimal("0.95"),
        taker_fee=Decimal("0.0035"),
        free_balance=Decimal("95.0"),
    )

    assert capped_size < Decimal("100")
    assert capped_size > Decimal("99")


def test_cap_entry_size_to_free_balance_leaves_size_when_balance_unknown() -> None:
    capped_size = _cap_entry_size_to_free_balance(
        desired_size=Decimal("100"),
        reference_price=Decimal("0.95"),
        taker_fee=Decimal("0.0035"),
        free_balance=None,
    )

    assert capped_size == Decimal("100")


def test_cap_entry_size_to_visible_liquidity_limits_quote_tick_sweeps() -> None:
    capped_size = _cap_entry_size_to_visible_liquidity(
        desired_size=Decimal("100"),
        visible_size=Decimal("18.2"),
    )

    assert capped_size == Decimal("18.2")


def test_effective_entry_reference_price_uses_worst_case_without_visible_ask() -> None:
    assert _effective_entry_reference_price(
        reference_price=Decimal("0.005"),
        visible_size=None,
    ) == Decimal("1")


def test_trade_tick_cash_cap_uses_worst_case_binary_price() -> None:
    capped_size = _cap_entry_size_to_free_balance(
        desired_size=Decimal("100"),
        reference_price=_effective_entry_reference_price(
            reference_price=Decimal("0.005"),
            visible_size=None,
        ),
        taker_fee=Decimal("0"),
        free_balance=Decimal("0.3"),
    )

    assert capped_size == Decimal("0.3")
