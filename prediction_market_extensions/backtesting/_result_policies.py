from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
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


def _timestamp_utc(value: object | None) -> pd.Timestamp | None:
    if value is None:
        return None
    try:
        if (
            isinstance(value, int | float)
            or isinstance(value, str)
            and value.strip().lstrip("+-").isdigit()
        ) and abs(float(value)) > 1e12:
            timestamp = pd.to_datetime(int(value), unit="ns", utc=True, errors="coerce")
        else:
            timestamp = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if pd.isna(timestamp):
        return None
    if isinstance(timestamp, pd.DatetimeIndex):
        if len(timestamp) == 0:
            return None
        timestamp = timestamp[0]
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _coerce_float(value: object | None) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if pd.notna(result) else None


def _pairs_to_series(pairs: object) -> pd.Series:
    if not isinstance(pairs, Sequence) or isinstance(pairs, str | bytes):
        return pd.Series(dtype=float)

    rows: list[tuple[pd.Timestamp, float]] = []
    for pair in pairs:
        if not isinstance(pair, Sequence) or isinstance(pair, str | bytes) or len(pair) != 2:
            continue
        timestamp = _timestamp_utc(pair[0])
        value = _coerce_float(pair[1])
        if timestamp is None or value is None:
            continue
        rows.append((timestamp, value))
    if not rows:
        return pd.Series(dtype=float)

    series = pd.Series(
        [value for _, value in rows],
        index=pd.DatetimeIndex([timestamp for timestamp, _ in rows]),
        dtype=float,
    )
    return series.groupby(series.index).last().sort_index()


def _series_to_pairs(series: pd.Series) -> list[tuple[str, float]]:
    if series.empty:
        return []
    return [
        (pd.Timestamp(timestamp).isoformat(), float(value)) for timestamp, value in series.items()
    ]


def _series_value_at_or_before(series: pd.Series, timestamp: pd.Timestamp) -> float | None:
    if series.empty:
        return None
    prior = series.loc[series.index <= timestamp]
    if not prior.empty:
        return float(prior.iloc[-1])
    return float(series.iloc[0])


def _fill_event_timestamp(event: Mapping[object, object]) -> pd.Timestamp | None:
    for key in ("timestamp", "ts_last", "ts_event", "ts_init"):
        timestamp = _timestamp_utc(event.get(key))
        if timestamp is not None:
            return timestamp
    return None


def _binary_mark_to_market_pnl_at_settlement(
    *,
    fill_events: object,
    price_series: object,
    timestamp: pd.Timestamp,
) -> tuple[float, float] | None:
    if not isinstance(fill_events, Sequence) or isinstance(fill_events, str | bytes):
        return None

    cash_pnl = 0.0
    open_qty = 0.0
    fallback_price: float | None = None
    saw_fill = False

    for event in sorted(
        (event for event in fill_events if isinstance(event, Mapping)),
        key=lambda item: _fill_event_timestamp(item) or pd.Timestamp.min.tz_localize("UTC"),
    ):
        fill_timestamp = _fill_event_timestamp(event)
        if fill_timestamp is not None and fill_timestamp > timestamp:
            continue

        action = str(event.get("action") or "").strip().lower()
        fill_price = _coerce_float(event.get("price"))
        quantity = _coerce_float(event.get("quantity")) or 0.0
        commission = _coerce_float(event.get("commission")) or 0.0
        if fill_price is None or quantity <= 0.0:
            continue

        if action == "buy":
            cash_pnl -= fill_price * quantity
            open_qty += quantity
        elif action == "sell":
            cash_pnl += fill_price * quantity
            open_qty -= quantity
        else:
            continue

        cash_pnl -= commission
        fallback_price = fill_price
        saw_fill = True

    if not saw_fill:
        return None

    market_price = _series_value_at_or_before(_pairs_to_series(price_series), timestamp)
    if market_price is None:
        market_price = fallback_price
    if market_price is None:
        return None

    position_value = (
        open_qty * market_price if open_qty >= 0.0 else abs(open_qty) * (1.0 - market_price)
    )
    return float(cash_pnl + max(position_value, 0.0)), float(cash_pnl)


def _series_bounds(*series_values: object) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    starts: list[pd.Timestamp] = []
    ends: list[pd.Timestamp] = []
    for value in series_values:
        series = _pairs_to_series(value)
        if not series.empty:
            starts.append(pd.Timestamp(series.index[0]))
            ends.append(pd.Timestamp(series.index[-1]))
    return (min(starts), max(ends)) if starts and ends else (None, None)


def _set_series_value_at_and_after(
    series: pd.Series, *, timestamp: pd.Timestamp, value: float
) -> pd.Series:
    if series.empty:
        return pd.Series([float(value)], index=pd.DatetimeIndex([timestamp]), dtype=float)

    updated = series.copy()
    updated.loc[timestamp] = float(value)
    updated = updated.groupby(updated.index).last().sort_index()
    updated.loc[updated.index >= timestamp] = float(value)
    return updated.astype(float)


def _add_series_delta_at_and_after(
    series: pd.Series, *, timestamp: pd.Timestamp, delta: float
) -> pd.Series:
    if series.empty:
        return series

    updated = series.copy()
    if timestamp not in updated.index:
        baseline = _series_value_at_or_before(updated, timestamp)
        if baseline is not None:
            updated.loc[timestamp] = baseline
    updated = updated.groupby(updated.index).last().sort_index()
    updated.loc[updated.index >= timestamp] = updated.loc[updated.index >= timestamp] + float(delta)
    return updated.astype(float)


def _add_settlement_delta_to_equity_like_series(
    series: pd.Series,
    *,
    timestamp: pd.Timestamp,
    settlement_delta: float,
    post_settlement_delta: float,
) -> pd.Series:
    if series.empty:
        return series

    updated = series.copy()
    if timestamp not in updated.index:
        baseline = _series_value_at_or_before(updated, timestamp)
        if baseline is not None:
            updated.loc[timestamp] = baseline
    updated = updated.groupby(updated.index).last().sort_index()
    updated.loc[updated.index == timestamp] = updated.loc[updated.index == timestamp] + float(
        settlement_delta
    )
    updated.loc[updated.index > timestamp] = updated.loc[updated.index > timestamp] + float(
        post_settlement_delta
    )
    return updated.astype(float)


def _settlement_timestamp(
    result: Mapping[str, Any],
    *,
    settlement_observable_ns_key: str,
    settlement_observable_time_key: str,
) -> pd.Timestamp | None:
    series_start, series_end = _series_bounds(
        result.get("equity_series"),
        result.get("cash_series"),
        result.get("pnl_series"),
        result.get("price_series"),
    )
    candidates = (
        result.get("market_close_time_ns"),
        result.get(settlement_observable_time_key),
        result.get(settlement_observable_ns_key),
        result.get("planned_end"),
        result.get("simulated_through"),
        series_end,
    )
    fallback: pd.Timestamp | None = None
    for candidate in candidates:
        timestamp = _timestamp_utc(candidate)
        if timestamp is None:
            continue
        if fallback is None:
            fallback = timestamp
        if series_start is not None and timestamp < series_start:
            continue
        return timestamp
    return series_end or fallback


def _apply_settlement_to_summary_series(
    result: dict[str, Any],
    *,
    settlement_pnl: float,
    settlement_observable_ns_key: str,
    settlement_observable_time_key: str,
) -> None:
    timestamp = _settlement_timestamp(
        result,
        settlement_observable_ns_key=settlement_observable_ns_key,
        settlement_observable_time_key=settlement_observable_time_key,
    )
    if timestamp is None:
        return

    equity_series = _pairs_to_series(result.get("equity_series"))
    cash_series = _pairs_to_series(result.get("cash_series"))
    pnl_series = _pairs_to_series(result.get("pnl_series"))
    initial_equity = (
        float(equity_series.iloc[0])
        if not equity_series.empty
        else float(cash_series.iloc[0])
        if not cash_series.empty
        else None
    )

    result["settlement_series_time"] = timestamp.isoformat()
    mark_to_market_pnl = _binary_mark_to_market_pnl_at_settlement(
        fill_events=result.get("fill_events"),
        price_series=result.get("price_series"),
        timestamp=timestamp,
    )
    if mark_to_market_pnl is not None:
        current_mtm_pnl, current_cash_pnl = mark_to_market_pnl
        result["settlement_equity_adjustment"] = float(settlement_pnl - current_mtm_pnl)
        result["settlement_cash_adjustment"] = float(settlement_pnl - current_cash_pnl)

    if initial_equity is None:
        if not pnl_series.empty:
            current_pnl = _series_value_at_or_before(pnl_series, timestamp)
            if current_pnl is not None and "settlement_equity_adjustment" not in result:
                result["settlement_equity_adjustment"] = float(settlement_pnl - current_pnl)
            result["pnl_series"] = _series_to_pairs(
                _set_series_value_at_and_after(
                    pnl_series, timestamp=timestamp, value=settlement_pnl
                )
            )
        return

    final_equity = float(initial_equity + settlement_pnl)

    if not equity_series.empty:
        current_equity = _series_value_at_or_before(equity_series, timestamp)
        if current_equity is not None and "settlement_equity_adjustment" not in result:
            result["settlement_equity_adjustment"] = float(final_equity - current_equity)
        result["equity_series"] = _series_to_pairs(
            _set_series_value_at_and_after(equity_series, timestamp=timestamp, value=final_equity)
        )
    elif not pnl_series.empty:
        current_pnl = _series_value_at_or_before(pnl_series, timestamp)
        if current_pnl is not None and "settlement_equity_adjustment" not in result:
            result["settlement_equity_adjustment"] = float(settlement_pnl - current_pnl)

    if not cash_series.empty:
        current_cash = _series_value_at_or_before(cash_series, timestamp)
        if current_cash is not None and "settlement_cash_adjustment" not in result:
            result["settlement_cash_adjustment"] = float(final_equity - current_cash)
        result["cash_series"] = _series_to_pairs(
            _set_series_value_at_and_after(cash_series, timestamp=timestamp, value=final_equity)
        )

    if not pnl_series.empty:
        result["pnl_series"] = _series_to_pairs(
            _set_series_value_at_and_after(pnl_series, timestamp=timestamp, value=settlement_pnl)
        )


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
    _apply_settlement_to_summary_series(
        result,
        settlement_pnl=float(settlement_pnl),
        settlement_observable_ns_key=settlement_observable_ns_key,
        settlement_observable_time_key=settlement_observable_time_key,
    )
    return result


def apply_joint_portfolio_settlement_pnl(results: Results) -> Results:
    if not results:
        return results

    joint_result = next(
        (
            result
            for result in results
            if result.get("joint_portfolio_equity_series")
            or result.get("joint_portfolio_cash_series")
            or result.get("joint_portfolio_pnl_series")
        ),
        None,
    )
    if joint_result is None or bool(joint_result.get("joint_portfolio_settlement_pnl_applied")):
        return results

    equity_series = _pairs_to_series(joint_result.get("joint_portfolio_equity_series"))
    cash_series = _pairs_to_series(joint_result.get("joint_portfolio_cash_series"))
    pnl_series = _pairs_to_series(joint_result.get("joint_portfolio_pnl_series"))
    applied = False

    for result in results:
        if not bool(result.get("settlement_pnl_applied")):
            continue
        timestamp = _timestamp_utc(result.get("settlement_series_time"))
        if timestamp is None:
            continue

        equity_adjustment = _coerce_float(result.get("settlement_equity_adjustment"))
        if equity_adjustment is not None and abs(equity_adjustment) > 1e-12:
            cash_adjustment = _coerce_float(result.get("settlement_cash_adjustment"))
            post_settlement_adjustment = (
                cash_adjustment if cash_adjustment is not None else equity_adjustment
            )
            equity_series = _add_settlement_delta_to_equity_like_series(
                equity_series,
                timestamp=timestamp,
                settlement_delta=equity_adjustment,
                post_settlement_delta=post_settlement_adjustment,
            )
            pnl_series = _add_settlement_delta_to_equity_like_series(
                pnl_series,
                timestamp=timestamp,
                settlement_delta=equity_adjustment,
                post_settlement_delta=post_settlement_adjustment,
            )
            applied = True

        cash_adjustment = _coerce_float(result.get("settlement_cash_adjustment"))
        if cash_adjustment is not None and abs(cash_adjustment) > 1e-12:
            cash_series = _add_series_delta_at_and_after(
                cash_series, timestamp=timestamp, delta=cash_adjustment
            )
            applied = True

    if not applied:
        return results

    if not equity_series.empty:
        joint_result["joint_portfolio_equity_series"] = _series_to_pairs(equity_series)
    if not cash_series.empty:
        joint_result["joint_portfolio_cash_series"] = _series_to_pairs(cash_series)
    if pnl_series.empty and not equity_series.empty:
        pnl_series = equity_series - float(equity_series.iloc[0])
    if not pnl_series.empty:
        joint_result["joint_portfolio_pnl_series"] = _series_to_pairs(pnl_series)
    joint_result["joint_portfolio_settlement_pnl_applied"] = True
    return results


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
    "apply_joint_portfolio_settlement_pnl",
    "apply_repo_research_disclosures",
    "append_result_warning",
]
