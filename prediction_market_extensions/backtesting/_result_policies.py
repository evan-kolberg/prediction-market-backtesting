from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd

from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    compute_binary_settlement_pnl,
)

type Results = list[dict[str, Any]]
type SettlementPnlFn = Callable[[object, object], float | None]
_CURATED_REPLAY_WARNING = (
    "Replay selection is explicitly curated from named markets and may exclude cancelled, "
    "delisted, or zero-liquidity markets."
)
_PORTFOLIO_RISK_WARNING = (
    "No portfolio-level drawdown or daily-loss circuit breaker is configured for this run."
)


def _timestamp_ns(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        try:
            timestamp_ns = int(value)
        except (TypeError, ValueError):
            return None
        return timestamp_ns if timestamp_ns >= 0 else None
    if isinstance(value, str):
        try:
            timestamp = pd.Timestamp(value)
        except (TypeError, ValueError):
            return None
        if pd.isna(timestamp):
            return None
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        return int(timestamp.value)
    return None


def append_result_warning(result: dict[str, Any], message: str) -> None:
    warnings_value = result.setdefault("warnings", [])
    if isinstance(warnings_value, list):
        if message not in warnings_value:
            warnings_value.append(message)
        return
    result["warnings"] = [str(warnings_value), message]


def apply_repo_research_disclosures(results: Results) -> Results:
    if not results:
        return results

    append_result_warning(results[0], _CURATED_REPLAY_WARNING)
    append_result_warning(results[0], _PORTFOLIO_RISK_WARNING)
    return results


class ResultPolicy(Protocol):
    def apply(self, results: Results) -> Results | None: ...


def apply_binary_settlement_pnl(
    result: dict[str, Any],
    *,
    settlement_pnl_fn: SettlementPnlFn = compute_binary_settlement_pnl,
    pnl_key: str = "pnl",
    market_exit_pnl_key: str = "market_exit_pnl",
    fill_events_key: str = "fill_events",
    realized_outcome_key: str = "realized_outcome",
    settlement_observable_ns_key: str = "settlement_observable_ns",
    settlement_observable_time_key: str = "settlement_observable_time",
    simulated_through_key: str = "simulated_through",
) -> dict[str, Any]:
    settlement_observable_ns = _timestamp_ns(
        result.get(settlement_observable_ns_key) or result.get(settlement_observable_time_key)
    )
    simulated_through_ns = _timestamp_ns(result.get(simulated_through_key))
    if settlement_observable_ns is not None and simulated_through_ns is None:
        append_result_warning(
            result,
            "Settlement outcome metadata exists but simulated_through is missing; keeping "
            "mark-to-market PnL because settlement observability cannot be verified.",
        )
        result["settlement_pnl_applied"] = False
        return result
    if (
        settlement_observable_ns is not None
        and simulated_through_ns is not None
        and simulated_through_ns < settlement_observable_ns
    ):
        observable_time = result.get(settlement_observable_time_key) or result.get(
            settlement_observable_ns_key
        )
        append_result_warning(
            result,
            f"Settlement outcome exists after the replay window; keeping mark-to-market PnL "
            f"instead of resolved settlement because resolution was not observable by "
            f"{result.get(simulated_through_key)} (observable at {observable_time}).",
        )
        result["settlement_pnl_applied"] = False
        return result

    settlement_pnl = settlement_pnl_fn(
        result.get(fill_events_key, []),
        result.get(realized_outcome_key),
    )
    if settlement_pnl is None:
        result["settlement_pnl_applied"] = False
        return result

    result[market_exit_pnl_key] = float(result.get(pnl_key, 0.0))
    result[pnl_key] = float(settlement_pnl)
    result["settlement_pnl_applied"] = True
    return result


@dataclass(frozen=True)
class BinarySettlementPnlPolicy:
    settlement_pnl_fn: SettlementPnlFn = compute_binary_settlement_pnl
    pnl_key: str = "pnl"
    market_exit_pnl_key: str = "market_exit_pnl"
    fill_events_key: str = "fill_events"
    realized_outcome_key: str = "realized_outcome"

    def apply(self, results: Results) -> Results:
        for result in results:
            apply_binary_settlement_pnl(
                result,
                settlement_pnl_fn=self.settlement_pnl_fn,
                pnl_key=self.pnl_key,
                market_exit_pnl_key=self.market_exit_pnl_key,
                fill_events_key=self.fill_events_key,
                realized_outcome_key=self.realized_outcome_key,
            )
        return results


__all__ = [
    "BinarySettlementPnlPolicy",
    "ResultPolicy",
    "apply_binary_settlement_pnl",
    "apply_repo_research_disclosures",
    "append_result_warning",
]
