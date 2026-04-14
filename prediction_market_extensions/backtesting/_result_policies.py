from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    compute_binary_settlement_pnl,
)

type Results = list[dict[str, Any]]
type SettlementPnlFn = Callable[[object, object], float | None]


class ResultPolicy(Protocol):
    def apply(self, results: Results) -> Results | None: ...


@dataclass(frozen=True)
class BinarySettlementPnlPolicy:
    settlement_pnl_fn: SettlementPnlFn = compute_binary_settlement_pnl
    pnl_key: str = "pnl"
    market_exit_pnl_key: str = "market_exit_pnl"
    fill_events_key: str = "fill_events"
    realized_outcome_key: str = "realized_outcome"

    def apply(self, results: Results) -> Results:
        for result in results:
            settlement_pnl = self.settlement_pnl_fn(
                result.get(self.fill_events_key, []), result.get(self.realized_outcome_key)
            )
            if settlement_pnl is None:
                continue
            result[self.market_exit_pnl_key] = float(result[self.pnl_key])
            result[self.pnl_key] = float(settlement_pnl)
        return results


__all__ = ["BinarySettlementPnlPolicy", "ResultPolicy"]
