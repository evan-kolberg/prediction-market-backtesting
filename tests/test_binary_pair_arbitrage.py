from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from strategies.binary_pair_arbitrage import (
    BookBinaryPairArbitrageConfig,
    BookBinaryPairArbitrageStrategy,
)

LEG_ONE = InstrumentId(Symbol("PM-PAIR-YES"), Venue("POLYMARKET"))
LEG_TWO = InstrumentId(Symbol("PM-PAIR-NO"), Venue("POLYMARKET"))
PAIR = (LEG_ONE, LEG_TWO)


class _PairArbHarness(BookBinaryPairArbitrageStrategy):
    def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(
            BookBinaryPairArbitrageConfig(
                instrument_ids=PAIR,
                trade_size=Decimal("5"),
                min_net_edge=0.02,
                max_total_cost=0.99,
                max_leg_price=0.99,
                max_spread=0.05,
                max_expected_slippage=0.02,
                min_visible_size=1.0,
                **kwargs,
            )
        )
        self.states = {
            LEG_ONE: (0.45, 10.0, 0.01),
            LEG_TWO: (0.52, 10.0, 0.01),
        }
        self.avg_prices = {LEG_ONE: 0.45, LEG_TWO: 0.52}
        self.fee_rates = {LEG_ONE: Decimal("0"), LEG_TWO: Decimal("0")}
        self.submissions: list[dict[str, object]] = []

    def _best_ask_state(self, instrument_id: InstrumentId) -> tuple[float, float, float] | None:
        return self.states.get(instrument_id)

    def _avg_entry_price(self, instrument_id: InstrumentId, size: Decimal) -> float | None:
        _ = size
        return self.avg_prices.get(instrument_id)

    def _instrument_fee_rate(self, instrument_id: InstrumentId) -> Decimal:
        return self.fee_rates[instrument_id]

    def _free_quote_balance(self, instrument_id: InstrumentId) -> Decimal | None:
        _ = instrument_id
        return None

    def _rounded_quantity(self, instrument_id: InstrumentId, size: Decimal):  # type: ignore[no-untyped-def]
        _ = instrument_id
        return SimpleNamespace(as_double=lambda: float(size))

    def _pair_has_position(self, pair: tuple[InstrumentId, InstrumentId]) -> bool:
        _ = pair
        return False

    def _submit_pair_entry(
        self,
        *,
        pair: tuple[InstrumentId, InstrumentId],
        quantities: list[object],
        visible_size: float,
        net_unit_cost: float,
        edge: float,
    ) -> None:
        self.submissions.append(
            {
                "pair": pair,
                "quantities": quantities,
                "visible_size": visible_size,
                "net_unit_cost": net_unit_cost,
                "edge": edge,
            }
        )


def test_pair_arbitrage_enters_when_combined_cost_is_below_settlement_value() -> None:
    strategy = _PairArbHarness()

    strategy._evaluate_pair(PAIR)

    assert len(strategy.submissions) == 1
    assert strategy.submissions[0]["pair"] == PAIR
    assert strategy.submissions[0]["visible_size"] == pytest.approx(10.0)
    assert strategy.submissions[0]["net_unit_cost"] == pytest.approx(0.97)
    assert strategy.submissions[0]["edge"] == pytest.approx(0.03)


def test_pair_arbitrage_rejects_entries_without_required_edge() -> None:
    strategy = _PairArbHarness()
    strategy.avg_prices[LEG_TWO] = 0.985

    strategy._evaluate_pair(PAIR)

    assert strategy.submissions == []
