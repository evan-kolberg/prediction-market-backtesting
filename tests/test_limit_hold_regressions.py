from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies import QuoteTickLateFavoriteLimitHoldConfig, QuoteTickLateFavoriteLimitHoldStrategy

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


def test_late_favorite_limit_order_acceptance_unblocks_strategy_state() -> None:
    strategy = QuoteTickLateFavoriteLimitHoldStrategy(
        QuoteTickLateFavoriteLimitHoldConfig(
            instrument_id=INSTRUMENT_ID,
            trade_size=Decimal(5),
            entry_price=0.9,
        )
    )
    strategy._pending = True

    strategy.on_order_accepted(SimpleNamespace())

    assert strategy._pending is False
    assert strategy._entered_once is True
