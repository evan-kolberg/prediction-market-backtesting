# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the
#  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied. See the License for the specific language governing
#  permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11, 2026-03-15, 2026-03-16, and 2026-03-31.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

import math
import re
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.analysis import MaxDrawdown, ProfitFactor, SharpeRatio, SortinoRatio
from nautilus_trader.analysis.reporter import ReportProvider
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.enums import AccountType, BookType, OmsType
from nautilus_trader.model.identifiers import TraderId, Venue
from nautilus_trader.model.objects import Currency, Money
from nautilus_trader.risk.config import RiskEngineConfig
from nautilus_trader.trading.strategy import Strategy

from prediction_market_extensions import install_commission_patch
from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    _timestamp_to_naive_utc_datetime,
    build_brier_inputs,
    build_market_prices,
    extract_price_points,
    extract_realized_pnl,
    infer_realized_outcome,
)
from prediction_market_extensions.adapters.prediction_market.fill_model import (
    PredictionMarketTakerFillModel,
)
from prediction_market_extensions.analysis import legacy_plot_adapter as legacy_plot_adapter
from prediction_market_extensions.analysis.legacy_backtesting.models import (
    DEFAULT_SUMMARY_PLOT_PANELS,
    PANEL_ALLOCATION,
    PANEL_BRIER_ADVANTAGE,
    PANEL_CASH_EQUITY,
    PANEL_DRAWDOWN,
    PANEL_EQUITY,
    PANEL_MARKET_PNL,
    PANEL_ROLLING_SHARPE,
    PANEL_TOTAL_BRIER_ADVANTAGE,
    PANEL_YES_PRICE,
    normalize_plot_panels,
)
from prediction_market_extensions.analysis.legacy_plot_adapter import (
    save_legacy_backtest_layout,
)
from prediction_market_extensions.backtesting._execution_config import StaticLatencyConfig
from prediction_market_extensions.backtesting._prediction_market_order_guard import (
    PredictionMarketOrderGuard,
)
from prediction_market_extensions.backtesting._result_policies import (
    apply_binary_settlement_pnl,
)

_DEFAULT_LATENCY_MODEL = object()
_DEFAULT_PREDICTION_MARKET_LATENCY = StaticLatencyConfig(
    base_latency_ms=75.0,
    insert_latency_ms=10.0,
    update_latency_ms=5.0,
    cancel_latency_ms=5.0,
)


def _default_prediction_market_latency_model() -> Any:
    latency_model = _DEFAULT_PREDICTION_MARKET_LATENCY.build_latency_model()
    if latency_model is None:
        raise AssertionError("default prediction-market latency model must be non-zero")
    return latency_model


def _extract_account_pnl_series(engine: BacktestEngine) -> pd.Series:
    accounts = list(engine.cache.accounts())
    if not accounts:
        return pd.Series(dtype=float)

    report = ReportProvider.generate_account_report(accounts[0])
    if report.empty or "total" not in report.columns:
        return pd.Series(dtype=float)

    frame = report.copy()
    frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce")
    frame = frame[~frame.index.isna()]
    if frame.empty:
        return pd.Series(dtype=float)

    total = pd.to_numeric(frame["total"], errors="coerce").dropna()
    total = total.groupby(total.index).last().sort_index()
    if total.empty:
        return pd.Series(dtype=float)

    return total - float(total.iloc[0])


def _dense_account_series_from_engine(
    *,
    engine: BacktestEngine,
    market_id: str,
    market_prices: Sequence[tuple[datetime, float]],
    initial_cash: float,
) -> tuple[pd.Series, pd.Series]:
    return _dense_account_series_from_engine_for_markets(
        engine=engine, market_prices={market_id: market_prices}, initial_cash=initial_cash
    )


def _dense_account_series_from_engine_for_markets(
    *,
    engine: BacktestEngine,
    market_prices: Mapping[str, Sequence[tuple[datetime, float]]],
    initial_cash: float,
) -> tuple[pd.Series, pd.Series]:
    models_module, _ = legacy_plot_adapter._load_legacy_modules()
    account_report = legacy_plot_adapter._extract_account_report(engine)
    fills_report = engine.trader.generate_order_fills_report()
    fills = legacy_plot_adapter._convert_fills(fills_report, models_module)
    sparse_snapshots = legacy_plot_adapter._build_portfolio_snapshots(
        models_module, account_report, fills
    )
    normalized_market_prices = legacy_plot_adapter._market_prices_with_fill_points(
        dict(market_prices), fills
    )
    dense_snapshots = legacy_plot_adapter._build_dense_portfolio_snapshots(
        models_module=models_module,
        sparse_snapshots=sparse_snapshots,
        fills=fills,
        market_prices=normalized_market_prices,
        initial_cash=float(initial_cash),
    )
    if not dense_snapshots:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    index = pd.to_datetime([snapshot.timestamp for snapshot in dense_snapshots], utc=True)
    equity = pd.Series(
        [float(snapshot.total_equity) for snapshot in dense_snapshots], index=index, dtype=float
    )
    cash = pd.Series(
        [float(snapshot.cash) for snapshot in dense_snapshots], index=index, dtype=float
    )
    return (
        equity.groupby(equity.index).last().sort_index(),
        cash.groupby(cash.index).last().sort_index(),
    )


def _dense_market_account_series_from_fill_events(
    *,
    market_id: str,
    market_prices: Sequence[tuple[datetime, float]],
    fill_events: Sequence[dict[str, Any]],
    initial_cash: float,
) -> tuple[pd.Series, pd.Series]:
    models_module, _ = legacy_plot_adapter._load_legacy_modules()
    fills = _deserialize_fill_events(
        market_id=market_id,
        fill_events=fill_events,
        models_module=models_module,
    )
    normalized_market_prices = legacy_plot_adapter._market_prices_with_fill_points(
        {market_id: market_prices}, fills
    )
    dense_dt = legacy_plot_adapter._build_dense_timeline(fills, normalized_market_prices)
    if len(dense_dt) == 0:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    cash_changes = pd.Series(0.0, index=dense_dt, dtype=float)
    for fill in fills:
        fill_ts = pd.Timestamp(fill.timestamp).to_datetime64()
        bar_idx = int(dense_dt.searchsorted(fill_ts, side="left"))
        bar_idx = max(0, min(len(dense_dt) - 1, bar_idx))
        action = str(fill.action.value).lower()
        gross = float(fill.price) * float(fill.quantity)
        cash_delta = -gross if action == "buy" else gross
        cash_delta -= float(fill.commission)
        cash_changes.iloc[bar_idx] = float(cash_changes.iloc[bar_idx]) + cash_delta

    cash = float(initial_cash) + cash_changes.cumsum()
    if not fills:
        index = pd.to_datetime(dense_dt, utc=True)
        cash.index = index
        return cash.copy(), cash

    dense_dts = dense_dt.to_numpy(dtype="datetime64[ns]")
    position_changes, fill_price_map = legacy_plot_adapter._replay_fill_position_deltas(
        fills, dense_dts
    )
    if not position_changes:
        index = pd.to_datetime(dense_dt, utc=True)
        cash.index = index
        return cash.copy(), cash

    position_quantities = {market: changes.cumsum() for market, changes in position_changes.items()}
    price_on_bar: dict[str, Any] = {}
    market_last_ts: dict[str, Any] = {}
    for position_market_id in position_quantities:
        prices, last_ts = legacy_plot_adapter._aligned_market_prices(
            market_id=position_market_id,
            market_prices=normalized_market_prices,
            dense_dts=dense_dts,
            n_bars=len(dense_dt),
            fallback_price=fill_price_map.get(position_market_id, 0.5),
        )
        price_on_bar[position_market_id] = prices
        market_last_ts[position_market_id] = last_ts

    legacy_plot_adapter._apply_resolution_cutoffs(
        position_quantities,
        position_changes,
        market_last_ts,
        dense_dts,
    )
    position_value, _ = legacy_plot_adapter._mark_to_market(position_quantities, price_on_bar)

    equity = cash + pd.Series(position_value, index=dense_dt, dtype=float)
    index = pd.to_datetime(dense_dt, utc=True)
    equity.index = index
    cash.index = index
    return equity.groupby(equity.index).last().sort_index(), cash.groupby(
        cash.index
    ).last().sort_index()


def _pairs_to_series(pairs: Sequence[tuple[str, float]] | Sequence[tuple[Any, float]]) -> pd.Series:
    if not pairs:
        return pd.Series(dtype=float)

    series = pd.Series(
        [float(value) for _, value in pairs],
        index=pd.to_datetime([ts for ts, _ in pairs], format="mixed", utc=True),
    )
    series = pd.to_numeric(series, errors="coerce").dropna()
    if series.empty:
        return pd.Series(dtype=float)

    return series.groupby(series.index).last().sort_index()


def _series_value_at_or_before(series: pd.Series, timestamp: pd.Timestamp) -> float | None:
    if series.empty:
        return None
    prior = series.loc[series.index <= timestamp]
    if prior.empty:
        return float(series.iloc[0])
    return float(prior.iloc[-1])


def _result_initial_capital(
    result: Mapping[str, Any],
    *,
    equity_series: pd.Series,
    cash_series: pd.Series,
    pnl_series: pd.Series,
) -> float | None:
    explicit = _coerce_float(result.get("initial_cash"))
    if explicit is not None:
        return explicit

    if not equity_series.empty:
        initial = float(equity_series.iloc[0])
        pnl_at_start = _series_value_at_or_before(pnl_series, pd.Timestamp(equity_series.index[0]))
        if pnl_at_start is not None:
            return initial - pnl_at_start
        return initial

    if not cash_series.empty:
        initial = float(cash_series.iloc[0])
        pnl_at_start = _series_value_at_or_before(pnl_series, pd.Timestamp(cash_series.index[0]))
        if pnl_at_start is not None:
            return initial - pnl_at_start
        return initial

    return None


def _initial_capital_from_pnl_series(
    *, equity_series: pd.Series, pnl_series: pd.Series
) -> float | None:
    if equity_series.empty:
        return None
    pnl_at_start = _series_value_at_or_before(pnl_series, pd.Timestamp(equity_series.index[0]))
    if pnl_at_start is None:
        return None
    return float(equity_series.iloc[0]) - pnl_at_start


def _joint_portfolio_initial_capital(
    result: Mapping[str, Any], *, equity_series: pd.Series
) -> float | None:
    explicit = _coerce_float(result.get("joint_portfolio_initial_cash"))
    if explicit is not None:
        return explicit
    pnl_series = _pairs_to_series(result.get("joint_portfolio_pnl_series") or [])
    return _initial_capital_from_pnl_series(
        equity_series=equity_series,
        pnl_series=pnl_series,
    )


def _return_fraction(*, initial_capital: float, final_equity: float) -> float:
    if not math.isfinite(initial_capital) or initial_capital <= 0.0:
        return float("nan")
    return (float(final_equity) - float(initial_capital)) / float(initial_capital)


def _series_with_initial_capital_basis(
    series: pd.Series,
    *,
    initial_capital: float | None,
) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if numeric.empty or initial_capital is None or not math.isfinite(float(initial_capital)):
        return numeric

    first = float(numeric.iloc[0])
    initial = float(initial_capital)
    if abs(first - initial) <= 1e-12:
        return numeric

    first_ts = pd.Timestamp(numeric.index[0])
    if pd.isna(first_ts) or first_ts.value <= 0:
        return numeric

    basis = pd.Series(
        [initial],
        index=pd.DatetimeIndex([first_ts - pd.Timedelta(nanoseconds=1)]),
        dtype=float,
    )
    return pd.concat([basis, numeric]).groupby(level=0).last().sort_index()


def _to_legacy_datetime(timestamp: pd.Timestamp) -> datetime:
    return _timestamp_to_naive_utc_datetime(pd.Timestamp(timestamp))


def _result_base_label(result: Mapping[str, Any], market_key: str | None) -> str:
    if market_key is not None:
        value = result.get(market_key)
        if value not in (None, ""):
            return str(value)

    for key in ("slug", "market", "instrument_id"):
        value = result.get(key)
        if value not in (None, ""):
            return str(value)
    return "unknown"


def _result_label_disambiguator(result: Mapping[str, Any]) -> str | None:
    for key in ("outcome", "realized_outcome", "instrument_id", "token_id", "token_index"):
        value = result.get(key)
        if value in (None, ""):
            continue
        return str(value)
    return None


def _unique_result_labels(results: Sequence[dict[str, Any]], market_key: str | None) -> list[str]:
    base_labels = [_result_base_label(result, market_key) for result in results]
    base_counts = Counter(base_labels)
    base_seen: Counter[str] = Counter()
    used_labels: set[str] = set()
    labels: list[str] = []

    for result, base_label in zip(results, base_labels, strict=True):
        base_seen[base_label] += 1
        label = base_label
        if base_counts[base_label] > 1 or label in used_labels:
            disambiguator = _result_label_disambiguator(result)
            if disambiguator is not None and disambiguator != base_label:
                label = f"{base_label} ({disambiguator})"
            else:
                label = f"{base_label} #{base_seen[base_label]}"

        suffix = 2
        unique_label = label
        while unique_label in used_labels:
            unique_label = f"{label} #{suffix}"
            suffix += 1

        labels.append(unique_label)
        used_labels.add(unique_label)

    return labels


def _series_to_iso_pairs(series: pd.Series) -> list[tuple[str, float]]:
    if series.empty:
        return []

    return [(pd.Timestamp(ts).isoformat(), float(value)) for ts, value in series.items()]


def _align_series_to_timeline(
    series: pd.Series, timeline: pd.DatetimeIndex, *, before: float, after: float
) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=float, index=timeline)

    aligned = series.reindex(timeline).ffill()
    aligned.loc[timeline < series.index[0]] = float(before)
    aligned.loc[timeline > series.index[-1]] = float(after)
    return aligned.astype(float)


def _extend_active_range(
    active_ranges: dict[str, tuple[pd.Timestamp, pd.Timestamp]],
    label: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> None:
    if label not in active_ranges:
        active_ranges[label] = (start, end)
        return
    current_start, current_end = active_ranges[label]
    active_ranges[label] = (min(current_start, start), max(current_end, end))


def _result_settlement_active_cutoff(result: Mapping[str, Any]) -> pd.Timestamp | None:
    if not bool(result.get("settlement_pnl_applied")):
        return None
    timestamp = pd.to_datetime(result.get("settlement_series_time"), utc=True, errors="coerce")
    if pd.isna(timestamp):
        return None
    return pd.Timestamp(timestamp)


def _record_active_cutoff(
    active_cutoffs: dict[str, pd.Timestamp],
    label: str,
    result: Mapping[str, Any],
) -> None:
    cutoff = _result_settlement_active_cutoff(result)
    if cutoff is None:
        return
    active_cutoffs[label] = (
        min(active_cutoffs[label], cutoff) if label in active_cutoffs else cutoff
    )


def _result_brier_cutoff(result: Mapping[str, Any]) -> pd.Timestamp | None:
    for key in ("settlement_series_time", "settlement_observable_time", "market_close_time_ns"):
        timestamp = pd.to_datetime(result.get(key), utc=True, errors="coerce")
        if not pd.isna(timestamp):
            return pd.Timestamp(timestamp)
    return None


def _truncate_brier_series_at_cutoff(
    result: Mapping[str, Any], *series_values: pd.Series
) -> tuple[pd.Series, ...]:
    cutoff = _result_brier_cutoff(result)
    if cutoff is None:
        return series_values

    truncated: list[pd.Series] = []
    for series in series_values:
        if series.empty:
            truncated.append(series)
            continue
        if not isinstance(series.index, pd.DatetimeIndex):
            index = pd.to_datetime(series.index, utc=True, errors="coerce")
            valid_mask = ~pd.isna(index)
            if not bool(valid_mask.any()):
                truncated.append(pd.Series(dtype=float))
                continue
            series = pd.Series(
                series.to_numpy()[valid_mask],
                index=pd.DatetimeIndex(index[valid_mask]),
                dtype=float,
            )
        truncated.append(series.loc[series.index < cutoff])
    return tuple(truncated)


def _active_range_mask(
    timeline: pd.DatetimeIndex,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cutoff: pd.Timestamp | None,
) -> Any:
    if cutoff is None:
        return (timeline >= start) & (timeline <= end)
    return (timeline >= start) & (timeline < cutoff)


def _fill_event_timestamp(event: Mapping[str, Any]) -> pd.Timestamp | None:
    for key in ("timestamp", "ts_last", "ts_event", "ts_init"):
        timestamp = pd.to_datetime(event.get(key), utc=True, errors="coerce")
        if isinstance(timestamp, pd.DatetimeIndex):
            if len(timestamp) == 0:
                continue
            timestamp = timestamp[0]
        if pd.isna(timestamp):
            continue
        return pd.Timestamp(timestamp)
    return None


def _fill_event_position_delta(event: Mapping[str, Any]) -> float | None:
    quantity = _parse_float_like(event.get("quantity"), default=0.0)
    if quantity <= 0.0:
        return None

    action = _fill_event_action(event, default_missing=None)
    if action == "buy":
        return quantity
    if action == "sell":
        return -quantity
    return None


def _normalize_fill_action_value(
    value: object,
    *,
    default_for_token_side: str | None,
) -> str | None:
    if _is_missing_fill_value(value):
        return None
    text = str(value or "").strip().lower()
    if text in {"buy", "bought"}:
        return "buy"
    if text in {"sell", "sold"}:
        return "sell"
    if text in {"yes", "no"}:
        return default_for_token_side
    return None


def _is_missing_fill_value(value: object) -> bool:
    if value is None:
        return True
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        return False
    try:
        return bool(missing)
    except (TypeError, ValueError):
        return False


def _fill_event_action(
    event: Mapping[str, Any],
    *,
    default_missing: str | None,
) -> str | None:
    for key in ("action", "order_side"):
        normalized = _normalize_fill_action_value(
            event.get(key),
            default_for_token_side=None,
        )
        if normalized is not None:
            return normalized

    # Tolerate raw venue/Data API rows where `side` is BUY/SELL, while keeping
    # YES/NO as the token side under the fill_events schema.
    normalized_side = _normalize_fill_action_value(
        event.get("side"),
        default_for_token_side=None,
    )
    if normalized_side is not None:
        return normalized_side

    for key in ("action", "order_side"):
        normalized = _normalize_fill_action_value(
            event.get(key),
            default_for_token_side="buy",
        )
        if normalized is not None:
            return normalized

    if _is_missing_fill_value(event.get("action")) and _is_missing_fill_value(
        event.get("order_side")
    ):
        return default_missing
    return None


def _result_active_position_intervals(
    result: Mapping[str, Any],
) -> list[tuple[pd.Timestamp, pd.Timestamp | None]] | None:
    fill_events = result.get("fill_events")
    if not isinstance(fill_events, Sequence) or isinstance(fill_events, str | bytes):
        return None

    deltas_by_time: dict[pd.Timestamp, float] = {}
    parsed_event = False
    for event in fill_events:
        if not isinstance(event, Mapping):
            continue
        timestamp = _fill_event_timestamp(event)
        delta = _fill_event_position_delta(event)
        if timestamp is None or delta is None:
            continue
        parsed_event = True
        deltas_by_time[timestamp] = deltas_by_time.get(timestamp, 0.0) + delta

    if not parsed_event:
        return None

    intervals: list[tuple[pd.Timestamp, pd.Timestamp | None]] = []
    position = 0.0
    interval_start: pd.Timestamp | None = None
    epsilon = 1e-12

    for timestamp in sorted(deltas_by_time):
        previous_position = position
        position += deltas_by_time[timestamp]
        was_open = abs(previous_position) > epsilon
        is_open = abs(position) > epsilon

        if not was_open and is_open:
            interval_start = timestamp
        elif was_open and not is_open:
            if interval_start is not None:
                intervals.append((interval_start, timestamp))
            interval_start = None
        elif was_open and is_open and previous_position * position < 0:
            if interval_start is not None:
                intervals.append((interval_start, timestamp))
            interval_start = timestamp

    if interval_start is not None and abs(position) > epsilon:
        intervals.append((interval_start, None))

    return intervals


def _position_interval_mask(
    timeline: pd.DatetimeIndex,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp | None,
    fallback_end: pd.Timestamp,
    cutoff: pd.Timestamp | None,
) -> Any:
    effective_end = fallback_end if end is None else end
    inclusive_end = end is None and cutoff is None
    if cutoff is not None and cutoff <= effective_end:
        effective_end = cutoff
        inclusive_end = False

    mask = timeline >= start
    if inclusive_end:
        return mask & (timeline <= effective_end)
    return mask & (timeline < effective_end)


def _overlay_range_mask(
    timeline: pd.DatetimeIndex,
    *,
    start: pd.Timestamp,
    end: pd.Timestamp,
    cutoff: pd.Timestamp | None,
) -> Any:
    if cutoff is None:
        return (timeline >= start) & (timeline <= end)
    return (timeline >= start) & (timeline <= cutoff)


def _parse_float_like(value: Any, default: float = 0.0) -> float:
    if _is_missing_fill_value(value):
        return default
    if isinstance(value, int | float):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else default

    text = str(value).strip().replace("_", "").replace("\u2212", "-")
    if not text:
        return default

    match = re.search(r"[-+]?\d*\.?\d+", text)
    if match is None:
        return default

    try:
        parsed = float(match.group(0))
    except ValueError:
        return default
    return parsed if math.isfinite(parsed) else default


def _first_non_missing_fill_value(source: Any, *keys: str) -> Any:
    for key in keys:
        try:
            value = source.get(key)
        except AttributeError:
            continue
        if not _is_missing_fill_value(value):
            return value
    return None


def _fill_value_text(value: Any) -> str:
    if _is_missing_fill_value(value):
        return ""
    return str(value).strip()


def _result_has_position_activity(result: Mapping[str, Any]) -> bool:
    fills_count = result.get("fills")
    if fills_count is not None:
        try:
            if int(fills_count) > 0:
                return True
        except (TypeError, ValueError):
            pass

    fill_events = result.get("fill_events")
    if not isinstance(fill_events, Sequence) or isinstance(fill_events, str | bytes):
        return False

    return any(
        isinstance(event, Mapping) and _parse_float_like(event.get("quantity"), default=0.0) > 0.0
        for event in fill_events
    )


def _serialize_fill_events(*, market_id: str, fills_report: pd.DataFrame) -> list[dict[str, Any]]:
    if fills_report.empty:
        return []

    frame = fills_report.copy()
    if frame.index.name and frame.index.name not in frame.columns:
        frame = frame.reset_index()

    market_id_upper = str(market_id).upper()
    inferred_side = (
        "no"
        if (
            market_id_upper.endswith("NO")
            or "-NO" in market_id_upper
            or ".NO." in market_id_upper
            or "_NO" in market_id_upper
        )
        else "yes"
    )

    events: list[dict[str, Any]] = []
    for idx, (_, row) in enumerate(frame.iterrows(), start=1):
        quantity = _parse_float_like(
            _first_non_missing_fill_value(row, "last_qty", "filled_qty", "quantity")
        )
        if quantity <= 0.0:
            continue

        timestamp = pd.to_datetime(
            _first_non_missing_fill_value(row, "ts_last", "ts_event", "ts_init"),
            utc=True,
            errors="coerce",
        )
        if pd.isna(timestamp):
            continue
        assert isinstance(timestamp, pd.Timestamp)

        raw_side = _fill_value_text(row.get("side")).lower()
        action = _fill_event_action(
            {
                "action": row.get("action"),
                "order_side": row.get("order_side"),
                "side": row.get("side"),
            },
            default_missing="buy",
        )
        if action is None:
            continue

        side_source_value = _first_non_missing_fill_value(row, "instrument_side")
        if side_source_value is None and raw_side in {"yes", "no"}:
            side_source_value = raw_side
        if side_source_value is None:
            side_source_value = _first_non_missing_fill_value(
                row, "instrument_id", "symbol", "market_id"
            )
        side_source = str(side_source_value if side_source_value is not None else market_id)
        side_source_upper = side_source.upper()
        normalized_side = (
            "no"
            if (
                side_source_upper.endswith("NO")
                or "-NO" in side_source_upper
                or ".NO." in side_source_upper
                or "_NO" in side_source_upper
            )
            else inferred_side
        )

        order_id_value = _first_non_missing_fill_value(
            row, "client_order_id", "venue_order_id", "order_id"
        )
        order_id = _fill_value_text(order_id_value) or f"fill-{idx}"

        events.append(
            {
                "order_id": order_id,
                "market_id": market_id,
                "action": action,
                "side": normalized_side,
                "price": _parse_float_like(
                    _first_non_missing_fill_value(row, "last_px", "avg_px", "price")
                ),
                "quantity": quantity,
                "timestamp": timestamp.isoformat(),
                "commission": _parse_float_like(
                    _first_non_missing_fill_value(row, "commission", "commissions", "fees")
                ),
            }
        )

    events.sort(key=lambda event: event["timestamp"])
    return events


def _deserialize_fill_events(
    *, market_id: str, fill_events: Sequence[dict[str, Any]], models_module: Any
) -> list[Any]:
    fills: list[Any] = []
    market_side = legacy_plot_adapter._infer_market_side(models_module, market_id)

    for idx, event in enumerate(fill_events, start=1):
        timestamp = _fill_event_timestamp(event)
        if timestamp is None:
            continue

        quantity = _parse_float_like(event.get("quantity"), default=0.0)
        if quantity <= 0.0:
            continue

        action = _fill_event_action(event, default_missing="buy")
        if action is None:
            continue

        event_side = _fill_value_text(event.get("side")).lower()
        if event_side == "no":
            fill_side = models_module.Side.NO
        elif event_side == "yes":
            fill_side = models_module.Side.YES
        else:
            fill_side = market_side
        order_id = _fill_value_text(event.get("order_id")) or f"fill-{idx}"
        fills.append(
            models_module.Fill(
                order_id=order_id,
                market_id=market_id,
                action=models_module.OrderAction.BUY
                if action == "buy"
                else models_module.OrderAction.SELL,
                side=fill_side,
                price=_parse_float_like(event.get("price"), default=0.0),
                quantity=quantity,
                timestamp=_to_legacy_datetime(timestamp),
                commission=_parse_float_like(event.get("commission"), default=0.0),
            )
        )

    fills.sort(key=lambda fill: fill.timestamp)
    return fills


def _aggregate_brier_frames(
    results: Sequence[dict[str, Any]], *, market_key: str | None
) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    labels = _unique_result_labels(results, market_key)

    for result, market_id in zip(results, labels, strict=True):
        user_series = _pairs_to_series(result.get("user_probability_series") or [])
        market_series = _pairs_to_series(result.get("market_probability_series") or [])
        outcome_series = _pairs_to_series(result.get("outcome_series") or [])
        user_series, market_series, outcome_series = _truncate_brier_series_at_cutoff(
            result, user_series, market_series, outcome_series
        )
        if user_series.empty or market_series.empty or outcome_series.empty:
            continue

        frame = legacy_plot_adapter.prepare_cumulative_brier_advantage(
            user_probabilities=user_series,
            market_probabilities=market_series,
            outcomes=outcome_series,
        )
        if (
            frame.empty
            or "brier_advantage" not in frame
            or "cumulative_brier_advantage" not in frame
        ):
            continue

        frames[market_id] = frame

    return frames


def _aggregate_brier_unavailable_reason(results: Sequence[dict[str, Any]]) -> str | None:
    user_series = pd.Series(dtype=float)
    market_series = pd.Series(dtype=float)
    outcome_series = pd.Series(dtype=float)

    for result in results:
        if user_series.empty:
            user_series = _pairs_to_series(result.get("user_probability_series") or [])
        if market_series.empty:
            market_series = _pairs_to_series(result.get("market_probability_series") or [])
        if outcome_series.empty:
            outcome_series = _pairs_to_series(result.get("outcome_series") or [])
        user_series, market_series, outcome_series = _truncate_brier_series_at_cutoff(
            result, user_series, market_series, outcome_series
        )
        if not user_series.empty and not market_series.empty and not outcome_series.empty:
            break

    return legacy_plot_adapter._brier_unavailable_reason(
        user_probabilities=user_series,
        market_probabilities=market_series,
        outcomes=outcome_series,
    )


def _summary_panels_need_market_prices(plot_panels: Sequence[str]) -> bool:
    return any(panel in {PANEL_YES_PRICE, PANEL_ALLOCATION} for panel in plot_panels)


def _summary_panels_need_fill_events(plot_panels: Sequence[str]) -> bool:
    return any(
        panel in {PANEL_YES_PRICE, PANEL_MARKET_PNL, PANEL_ALLOCATION} for panel in plot_panels
    )


def _summary_panels_need_overlay_series(plot_panels: Sequence[str]) -> bool:
    return any(
        panel in {PANEL_EQUITY, PANEL_DRAWDOWN, PANEL_ROLLING_SHARPE, PANEL_CASH_EQUITY}
        for panel in plot_panels
    )


def _yes_price_fill_marker_budget(max_points: int) -> int:
    if max_points <= 0:
        return 250
    return max(50, min(250, max_points // 10))


def _summary_yes_price_fill_marker_limit(fill_count: int, max_points: int) -> int | None:
    legacy_limit_fn = getattr(legacy_plot_adapter, "_yes_price_fill_marker_limit", None)
    if callable(legacy_limit_fn):
        return legacy_limit_fn(fill_count=fill_count, max_points=max_points)

    marker_budget = _yes_price_fill_marker_budget(max_points)
    if fill_count <= marker_budget:
        return None
    return marker_budget


def _configure_summary_report_downsampling(
    plotting_module: Any, *, adaptive: bool = True, max_points: int = 5000
) -> None:
    legacy_configure_fn = getattr(legacy_plot_adapter, "_configure_legacy_downsampling", None)
    if callable(legacy_configure_fn):
        legacy_configure_fn(plotting_module, adaptive=adaptive, max_points=max_points)
        return

    downsample_fn = getattr(plotting_module, "_downsample", None)
    if downsample_fn is None:
        return

    if not adaptive:

        def _identity_downsample(
            eq, fills_df, market_df, max_points=5000, alloc_df=None, keep_indices=None
        ):
            return eq, fills_df, market_df, alloc_df

        plotting_module._downsample = _identity_downsample
        return

    requested_max_points = max(2, int(max_points))

    def _adaptive_downsample(
        eq, fills_df, market_df, max_points=5000, alloc_df=None, keep_indices=None
    ):
        total_points = sum(
            len(frame)
            for frame in (eq, fills_df, market_df, alloc_df)
            if frame is not None and hasattr(frame, "__len__")
        )
        if total_points <= requested_max_points:
            return eq, fills_df, market_df, alloc_df

        return downsample_fn(
            eq,
            fills_df,
            market_df,
            max_points=requested_max_points,
            alloc_df=alloc_df,
            keep_indices=keep_indices,
        )

    plotting_module._downsample = _adaptive_downsample


def _build_summary_brier_panel(
    brier_frames: dict[str, pd.DataFrame], *, axis_label: str, max_points_per_market: int
) -> Any | None:
    build_panel_fn = legacy_plot_adapter._build_multi_market_brier_panel
    try:
        return build_panel_fn(
            brier_frames,
            axis_label=axis_label,
            max_points_per_market=max_points_per_market,
        )
    except TypeError:
        return build_panel_fn(brier_frames, axis_label=axis_label)


def _build_total_summary_brier_frame(brier_frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for frame in brier_frames.values():
        if frame.empty or "brier_advantage" not in frame:
            continue

        normalized = frame[["brier_advantage"]].copy()
        normalized.index = pd.to_datetime(normalized.index, utc=True, errors="coerce")
        normalized = normalized[~normalized.index.isna()]
        if not normalized.empty:
            frames.append(normalized)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames).sort_index()
    combined = combined.groupby(combined.index)["brier_advantage"].sum().to_frame()
    combined["cumulative_brier_advantage"] = combined["brier_advantage"].cumsum()
    return combined


def _build_summary_brier_extra_panels(
    *,
    results: Sequence[dict[str, Any]],
    market_key: str | None,
    resolved_plot_panels: Sequence[str],
    max_points_per_market: int,
) -> dict[str, Any]:
    extra_panels: dict[str, Any] = {}
    if (
        PANEL_BRIER_ADVANTAGE not in resolved_plot_panels
        and PANEL_TOTAL_BRIER_ADVANTAGE not in resolved_plot_panels
    ):
        return extra_panels

    brier_frames = _aggregate_brier_frames(results, market_key=market_key)
    if brier_frames:
        if PANEL_TOTAL_BRIER_ADVANTAGE in resolved_plot_panels:
            total_frame = _build_total_summary_brier_frame(brier_frames)
            panel = legacy_plot_adapter._build_total_brier_panel(total_frame)
            if panel is not None:
                extra_panels[PANEL_TOTAL_BRIER_ADVANTAGE] = panel
        if PANEL_BRIER_ADVANTAGE in resolved_plot_panels:
            panel = _build_summary_brier_panel(
                brier_frames,
                axis_label="Cumulative Brier Advantage",
                max_points_per_market=max_points_per_market,
            )
            if panel is not None:
                extra_panels[PANEL_BRIER_ADVANTAGE] = panel
        return extra_panels

    unavailable_reason = _aggregate_brier_unavailable_reason(results)
    if unavailable_reason is None:
        return extra_panels

    if PANEL_TOTAL_BRIER_ADVANTAGE in resolved_plot_panels:
        extra_panels[PANEL_TOTAL_BRIER_ADVANTAGE] = (
            legacy_plot_adapter._build_brier_placeholder_panel(unavailable_reason)
        )
    if PANEL_BRIER_ADVANTAGE in resolved_plot_panels:
        extra_panels[PANEL_BRIER_ADVANTAGE] = legacy_plot_adapter._build_brier_placeholder_panel(
            unavailable_reason
        )
    return extra_panels


def _apply_summary_layout_overrides(
    layout: Any, *, initial_cash: float, max_yes_price_fill_markers: int | None
) -> Any:
    apply_fn = legacy_plot_adapter._apply_layout_overrides
    try:
        return apply_fn(
            layout,
            initial_cash=float(initial_cash),
            max_yes_price_fill_markers=max_yes_price_fill_markers,
            max_market_pnl_fill_markers=max_yes_price_fill_markers,
        )
    except TypeError:
        return apply_fn(layout, initial_cash=float(initial_cash))


def _add_engine_data_by_type(engine: BacktestEngine, records: Sequence[Any]) -> None:
    records_by_type: dict[type[Any], list[Any]] = {}
    for record in records:
        records_by_type.setdefault(type(record), []).append(record)
    for typed_records in records_by_type.values():
        engine.add_data(typed_records)


def run_market_backtest(
    *,
    market_id: str,
    instrument: Any,
    data: Sequence[object],
    strategy: Strategy,
    strategy_name: str,
    output_prefix: str,
    platform: str,
    venue: Venue,
    base_currency: Currency,
    fee_model: Any,
    fill_model: Any | None = None,
    apply_default_fill_model: bool = False,
    initial_cash: float,
    probability_window: int,
    price_attr: str,
    count_key: str,
    data_count: int | None = None,
    chart_resample_rule: str | None = None,
    market_key: str = "market",
    open_browser: bool = False,
    return_summary_series: bool = False,
    book_type: BookType = BookType.L2_MBP,
    liquidity_consumption: bool = True,
    queue_position: bool = True,
    latency_model: Any | None = _DEFAULT_LATENCY_MODEL,
    nautilus_log_level: str = "INFO",
) -> dict[str, Any]:
    """
    Run one prediction-market backtest and emit a legacy chart.

    The repository is L2-book native, so this legacy helper defaults to
    passive ``L2_MBP`` book execution with queue position and a static latency
    model. Older synthetic taker fills remain available by passing
    ``apply_default_fill_model=True`` or a custom ``fill_model``.
    """
    install_commission_patch()
    if latency_model is _DEFAULT_LATENCY_MODEL:
        latency_model = _default_prediction_market_latency_model()
    elif isinstance(latency_model, StaticLatencyConfig):
        latency_model = latency_model.build_latency_model()

    if fill_model is None and apply_default_fill_model:
        fill_model = PredictionMarketTakerFillModel()

    data_records = data if isinstance(data, list) else list(data)
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("BACKTESTER-001"),
            logging=LoggingConfig(log_level=nautilus_log_level),
            risk_engine=RiskEngineConfig(),
        )
    )
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=base_currency,
        starting_balances=[Money(initial_cash, base_currency)],
        fill_model=fill_model,
        fee_model=fee_model,
        latency_model=latency_model,
        book_type=book_type,
        liquidity_consumption=liquidity_consumption,
        queue_position=queue_position,
        bar_execution=False,
        trade_execution=True,
    )
    engine.add_instrument(instrument)
    _add_engine_data_by_type(engine, data_records)
    order_guard = PredictionMarketOrderGuard()
    order_guard.install(strategy)
    engine.add_strategy(strategy)
    engine.run()

    fills = engine.trader.generate_order_fills_report()
    positions = engine.trader.generate_positions_report()
    pnl = extract_realized_pnl(positions)
    price_points = extract_price_points(data_records, price_attr=price_attr)
    realized_outcome = infer_realized_outcome(instrument)
    fill_events = _serialize_fill_events(market_id=market_id, fills_report=fills)
    result_warnings: list[str] = []
    result_warnings.extend(order_guard.warnings)
    user_probabilities, market_probabilities, outcomes = build_brier_inputs(
        points=price_points,
        window=probability_window,
        realized_outcome=realized_outcome,
        warnings_out=result_warnings,
    )
    chart_market_prices = build_market_prices(price_points, resample_rule=chart_resample_rule)

    summary_price_series = None
    summary_pnl_series = None
    summary_equity_series = None
    summary_cash_series = None
    summary_user_probability_series = None
    summary_market_probability_series = None
    summary_outcome_series = None
    summary_fill_events = None
    if return_summary_series:
        summary_legacy_models, _ = legacy_plot_adapter._load_legacy_modules()
        summary_legacy_fills = legacy_plot_adapter._convert_fills(fills, summary_legacy_models)
        summary_market_prices = legacy_plot_adapter._market_prices_with_fill_points(
            {str(instrument.id): chart_market_prices}, summary_legacy_fills
        ).get(str(instrument.id), chart_market_prices)
        dense_equity_series, dense_cash_series = _dense_market_account_series_from_fill_events(
            market_id=market_id,
            market_prices=chart_market_prices,
            fill_events=fill_events,
            initial_cash=initial_cash,
        )
        summary_price_series = _series_to_iso_pairs(_pairs_to_series(summary_market_prices))
        pnl_series = (
            dense_equity_series - float(dense_equity_series.iloc[0])
            if not dense_equity_series.empty
            else _extract_account_pnl_series(engine)
        )
        if not pnl_series.empty:
            summary_pnl_series = _series_to_iso_pairs(pnl_series)
        if not dense_equity_series.empty:
            summary_equity_series = _series_to_iso_pairs(dense_equity_series)
        if not dense_cash_series.empty:
            summary_cash_series = _series_to_iso_pairs(dense_cash_series)
        if not user_probabilities.empty:
            summary_user_probability_series = _series_to_iso_pairs(user_probabilities)
        if not market_probabilities.empty:
            summary_market_probability_series = _series_to_iso_pairs(market_probabilities)
        if not outcomes.empty:
            summary_outcome_series = _series_to_iso_pairs(outcomes)
        summary_fill_events = fill_events

    engine.reset()
    engine.dispose()

    result = {
        market_key: market_id,
        count_key: int(data_count) if data_count is not None else len(data_records),
        "fills": len(fills),
        "pnl": pnl,
        "initial_cash": float(initial_cash),
        "realized_outcome": realized_outcome,
        "fill_events": fill_events,
        "warnings": result_warnings,
        "settlement_observable_ns": getattr(instrument, "expiration_ns", None),
        "settlement_observable_time": (
            pd.Timestamp(
                getattr(instrument, "expiration_ns", None), unit="ns", tz="UTC"
            ).isoformat()
            if isinstance(getattr(instrument, "expiration_ns", None), int)
            and getattr(instrument, "expiration_ns", None) > 0
            else None
        ),
    }
    if return_summary_series:
        result["price_series"] = summary_price_series or []
        result["pnl_series"] = summary_pnl_series or []
        result["equity_series"] = summary_equity_series or []
        result["cash_series"] = summary_cash_series or []
        result["user_probability_series"] = summary_user_probability_series or []
        result["market_probability_series"] = summary_market_probability_series or []
        result["outcome_series"] = summary_outcome_series or []
        result["fill_events"] = summary_fill_events or []
    return apply_binary_settlement_pnl(result)


def save_combined_backtest_report(
    *,
    results: Sequence[dict[str, Any]],
    output_path: str | Path,
    title: str,
    market_key: str,
    pnl_label: str,
) -> str | None:
    """
    Save one HTML page by concatenating the generated per-market chart HTML bodies.
    """
    chart_paths: list[Path] = []
    for result in results:
        chart_path = result.get("chart_path")
        if chart_path is None:
            continue
        chart_paths.append(Path(str(chart_path)).expanduser().resolve())

    if not chart_paths:
        return None

    output_abs = Path(output_path).expanduser().resolve()
    output_abs.parent.mkdir(parents=True, exist_ok=True)
    first_html = chart_paths[0].read_text(encoding="utf-8")
    head_match = re.search(
        r"<head[^>]*>(?P<head>.*)</head>", first_html, flags=re.IGNORECASE | re.DOTALL
    )
    if head_match is None:
        raise ValueError(f"Unable to locate <head> in {chart_paths[0]}")

    body_pattern = re.compile(r"<body[^>]*>(?P<body>.*)</body>", flags=re.IGNORECASE | re.DOTALL)
    body_chunks: list[str] = []
    for chart_path in chart_paths:
        html_text = chart_path.read_text(encoding="utf-8")
        body_match = body_pattern.search(html_text)
        if body_match is None:
            raise ValueError(f"Unable to locate <body> in {chart_path}")
        body_chunks.append(body_match.group("body").strip())

    combined_html = (
        "<!DOCTYPE html>\n"
        '<html lang="en">\n'
        "  <head>\n"
        f"{head_match.group('head').strip()}\n"
        "  </head>\n"
        "  <body>\n"
        f"{'\n\n'.join(body_chunks)}\n"
        "  </body>\n"
        "</html>\n"
    )
    output_abs.write_text(combined_html, encoding="utf-8")
    return str(output_abs)


def save_aggregate_backtest_report(
    *,
    results: Sequence[dict[str, Any]],
    output_path: str | Path,
    title: str,
    market_key: str,
    pnl_label: str,
    max_points_per_market: int = 400,
    plot_panels: Sequence[str] | None = None,
) -> str | None:
    """
    Save one legacy Bokeh report spanning multiple markets in shared panels.
    """
    if not results:
        return None

    models_module, plotting_module = legacy_plot_adapter._load_legacy_modules()
    downsample_point_limit = max(5000, max_points_per_market * 12)
    resolved_plot_panels = normalize_plot_panels(plot_panels, default=DEFAULT_SUMMARY_PLOT_PANELS)
    _configure_summary_report_downsampling(
        plotting_module, adaptive=True, max_points=downsample_point_limit
    )
    include_market_prices = _summary_panels_need_market_prices(resolved_plot_panels)
    include_fill_events = _summary_panels_need_fill_events(resolved_plot_panels)
    include_overlay_series = _summary_panels_need_overlay_series(resolved_plot_panels)

    market_prices: dict[str, list[tuple[datetime, float]]] = {}
    fills: list[Any] = []
    equity_series_by_market: dict[str, pd.Series] = {}
    cash_series_by_market: dict[str, pd.Series] = {}
    active_ranges: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    active_cutoffs: dict[str, pd.Timestamp] = {}
    active_position_intervals: dict[str, list[tuple[pd.Timestamp, pd.Timestamp | None]]] = {}
    active_position_labels: set[str] = set()
    initial_capital_by_label: dict[str, float] = {}
    timeline_points: set[pd.Timestamp] = set()
    labels = _unique_result_labels(results, market_key)

    for result, label in zip(results, labels, strict=True):
        _record_active_cutoff(active_cutoffs, label, result)
        position_intervals = _result_active_position_intervals(result)
        if position_intervals is None:
            if _result_has_position_activity(result):
                active_position_labels.add(label)
        else:
            active_position_intervals[label] = position_intervals
            for interval_start, interval_end in position_intervals:
                timeline_points.add(interval_start)
                if interval_end is not None:
                    timeline_points.add(interval_end)
        final_pnl = float(result.get("pnl") or 0.0)

        price_series = _pairs_to_series(result.get("price_series") or [])
        if not price_series.empty:
            if include_market_prices:
                market_prices[label] = [
                    (_to_legacy_datetime(ts), float(value)) for ts, value in price_series.items()
                ]
            _extend_active_range(
                active_ranges, label, price_series.index[0], price_series.index[-1]
            )
            timeline_points.update(price_series.index.to_list())

        if include_fill_events:
            fills.extend(
                _deserialize_fill_events(
                    market_id=label,
                    fill_events=result.get("fill_events") or [],
                    models_module=models_module,
                )
            )
            for event in result.get("fill_events") or []:
                timestamp = pd.to_datetime(event.get("timestamp"), utc=True, errors="coerce")
                if not pd.isna(timestamp):
                    timeline_points.add(timestamp)

        equity_series = _pairs_to_series(result.get("equity_series") or [])
        cash_series = _pairs_to_series(result.get("cash_series") or [])
        pnl_series = _pairs_to_series(result.get("pnl_series") or [])

        if equity_series.empty:
            if not pnl_series.empty:
                explicit_initial_cash = _coerce_float(result.get("initial_cash"))
                start_equity = (
                    explicit_initial_cash
                    if explicit_initial_cash is not None
                    else float(cash_series.iloc[0])
                    if not cash_series.empty
                    else 100.0
                )
                equity_series = pnl_series.astype(float) + start_equity
            elif not price_series.empty:
                equity_series = pd.Series(
                    [100.0, 100.0 + final_pnl],
                    index=pd.DatetimeIndex([price_series.index[0], price_series.index[-1]]),
                    dtype=float,
                )

        if not pnl_series.empty:
            pnl_series = pnl_series.astype(float)
            pnl_series.iloc[-1] = final_pnl
        elif not equity_series.empty:
            pnl_series = (equity_series - float(equity_series.iloc[0])).astype(float)
            pnl_series.iloc[-1] = final_pnl

        if cash_series.empty and not equity_series.empty:
            fallback_start = float(equity_series.iloc[0])
            fallback_end = float(equity_series.iloc[-1])
            if len(equity_series.index) == 1:
                cash_series = pd.Series([fallback_start], index=equity_series.index, dtype=float)
            else:
                cash_series = pd.Series(
                    [fallback_start, fallback_end],
                    index=pd.DatetimeIndex([equity_series.index[0], equity_series.index[-1]]),
                    dtype=float,
                )

        initial_capital = _result_initial_capital(
            result,
            equity_series=equity_series,
            cash_series=cash_series,
            pnl_series=pnl_series,
        )
        if initial_capital is not None:
            initial_capital_by_label[label] = initial_capital

        if not equity_series.empty:
            equity_series_by_market[label] = equity_series.astype(float)
            timeline_points.update(equity_series.index.to_list())
            _extend_active_range(
                active_ranges, label, equity_series.index[0], equity_series.index[-1]
            )
        if not cash_series.empty:
            cash_series_by_market[label] = cash_series.astype(float)
            timeline_points.update(cash_series.index.to_list())
            _extend_active_range(active_ranges, label, cash_series.index[0], cash_series.index[-1])
        if not pnl_series.empty:
            timeline_points.update(pnl_series.index.to_list())
            _extend_active_range(active_ranges, label, pnl_series.index[0], pnl_series.index[-1])

    if timeline_points:
        timeline = pd.DatetimeIndex(sorted(timeline_points))
    else:
        now = pd.Timestamp.now(tz="UTC")
        timeline = pd.DatetimeIndex([now])
    if initial_capital_by_label and len(timeline) > 0 and timeline[0].value > 0:
        timeline = pd.DatetimeIndex([timeline[0] - pd.Timedelta(nanoseconds=1)]).union(timeline)

    aggregate_equity = pd.Series(0.0, index=timeline, dtype=float)
    aggregate_cash = pd.Series(0.0, index=timeline, dtype=float)
    active_count = pd.Series(0, index=timeline, dtype=int)
    overlay_equity: dict[str, pd.Series] = {}
    overlay_cash: dict[str, pd.Series] = {}

    for label, (start, end) in active_ranges.items():
        equity_series = equity_series_by_market.get(label, pd.Series(dtype=float))
        cash_series = cash_series_by_market.get(label, pd.Series(dtype=float))
        if equity_series.empty and cash_series.empty:
            continue

        if equity_series.empty:
            start_equity = float(cash_series.iloc[0]) if not cash_series.empty else 100.0
            end_equity = float(cash_series.iloc[-1]) if not cash_series.empty else start_equity
            equity_series = pd.Series(
                [start_equity, end_equity], index=pd.DatetimeIndex([start, end]), dtype=float
            )
        if cash_series.empty:
            cash_series = pd.Series(
                [float(equity_series.iloc[0]), float(equity_series.iloc[-1])],
                index=pd.DatetimeIndex([start, end]),
                dtype=float,
            )

        full_equity = _align_series_to_timeline(
            equity_series,
            timeline,
            before=initial_capital_by_label.get(label, float(equity_series.iloc[0])),
            after=float(equity_series.iloc[-1]),
        )
        full_cash = _align_series_to_timeline(
            cash_series,
            timeline,
            before=initial_capital_by_label.get(label, float(cash_series.iloc[0])),
            after=float(cash_series.iloc[-1]),
        )

        aggregate_equity = aggregate_equity.add(full_equity, fill_value=0.0)
        aggregate_cash = aggregate_cash.add(full_cash, fill_value=0.0)

        if label in active_position_intervals:
            for interval_start, interval_end in active_position_intervals[label]:
                active_mask = _position_interval_mask(
                    timeline,
                    start=interval_start,
                    end=interval_end,
                    fallback_end=end,
                    cutoff=active_cutoffs.get(label),
                )
                active_count.loc[active_mask] = active_count.loc[active_mask] + 1
        elif label in active_position_labels:
            active_mask = _active_range_mask(
                timeline,
                start=start,
                end=end,
                cutoff=active_cutoffs.get(label),
            )
            active_count.loc[active_mask] = active_count.loc[active_mask] + 1

        overlay_mask = _overlay_range_mask(
            timeline,
            start=start,
            end=end,
            cutoff=active_cutoffs.get(label),
        )
        clipped_equity = full_equity.copy()
        clipped_cash = full_cash.copy()
        clipped_equity.loc[~overlay_mask] = float("nan")
        clipped_cash.loc[~overlay_mask] = float("nan")
        overlay_equity[label] = clipped_equity
        overlay_cash[label] = clipped_cash

    if aggregate_equity.empty:
        return None

    initial_cash = float(aggregate_equity.iloc[0])
    equity_curve = [
        models_module.PortfolioSnapshot(
            timestamp=_to_legacy_datetime(ts),
            cash=float(aggregate_cash.loc[ts]),
            total_equity=float(aggregate_equity.loc[ts]),
            unrealized_pnl=float(aggregate_equity.loc[ts] - aggregate_cash.loc[ts]),
            num_positions=int(active_count.loc[ts]),
        )
        for ts in timeline
    ]

    final_equity = float(aggregate_equity.iloc[-1])
    equity_values = pd.Series([snapshot.total_equity for snapshot in equity_curve], dtype=float)
    running_peak = equity_values.cummax()
    drawdowns = (
        (running_peak - equity_values) / running_peak.where(running_peak > 0.0, pd.NA)
    ).fillna(0.0)
    max_drawdown = float(drawdowns.max()) if not drawdowns.empty else 0.0
    metrics = {
        "final_pnl": final_equity - initial_cash,
        "total_return": _return_fraction(
            initial_capital=initial_cash,
            final_equity=final_equity,
        ),
        "max_drawdown": max_drawdown,
    }

    result = models_module.BacktestResult(
        equity_curve=equity_curve,
        fills=fills,
        metrics=metrics,
        strategy_name=title,
        platform=models_module.Platform.POLYMARKET,
        start_time=_to_legacy_datetime(timeline[0]),
        end_time=_to_legacy_datetime(timeline[-1]),
        initial_cash=float(initial_cash),
        final_equity=float(final_equity),
        num_markets_traded=sum(1 for item in results if int(item.get("fills") or 0) > 0),
        num_markets_resolved=len(results),
        market_prices=market_prices if include_market_prices else {},
        market_pnls={},
        overlay_series=(
            {"equity": overlay_equity, "cash": overlay_cash} if include_overlay_series else {}
        ),
        hide_primary_panel_series=True,
        primary_series_name="Aggregate",
        prepend_total_equity_panel=True,
        total_equity_panel_label="Total Equity",
        plot_monthly_returns=True,
        plot_panels=resolved_plot_panels,
    )

    output_abs = Path(output_path).expanduser().resolve()
    output_abs.parent.mkdir(parents=True, exist_ok=True)
    extra_panels = _build_summary_brier_extra_panels(
        results=results,
        market_key=market_key,
        resolved_plot_panels=resolved_plot_panels,
        max_points_per_market=max_points_per_market,
    )
    layout = plotting_module.plot(
        result,
        filename=str(output_abs),
        max_markets=max(len(market_prices), 30),
        open_browser=False,
        progress=False,
        plot_panels=resolved_plot_panels,
        extra_panels=extra_panels,
    )
    layout = _apply_summary_layout_overrides(
        layout,
        initial_cash=float(initial_cash),
        max_yes_price_fill_markers=_summary_yes_price_fill_marker_limit(
            fill_count=len(fills),
            max_points=downsample_point_limit,
        ),
    )
    return save_legacy_backtest_layout(layout, output_abs, title)


def save_joint_portfolio_backtest_report(
    *,
    results: Sequence[dict[str, Any]],
    output_path: str | Path,
    title: str,
    market_key: str,
    pnl_label: str,
    max_points_per_market: int = 400,
    plot_panels: Sequence[str] | None = None,
) -> str | None:
    """
    Save one legacy Bokeh report for a shared-account, joint-portfolio multi-market run.
    """
    if not results:
        return None

    models_module, plotting_module = legacy_plot_adapter._load_legacy_modules()
    downsample_point_limit = max(5000, max_points_per_market * 12)
    resolved_plot_panels = normalize_plot_panels(plot_panels, default=DEFAULT_SUMMARY_PLOT_PANELS)
    _configure_summary_report_downsampling(
        plotting_module, adaptive=True, max_points=downsample_point_limit
    )
    include_market_prices = _summary_panels_need_market_prices(resolved_plot_panels)
    include_fill_events = _summary_panels_need_fill_events(resolved_plot_panels)
    include_overlay_series = _summary_panels_need_overlay_series(resolved_plot_panels)

    market_prices: dict[str, list[tuple[datetime, float]]] = {}
    fills: list[Any] = []
    equity_series_by_market: dict[str, pd.Series] = {}
    cash_series_by_market: dict[str, pd.Series] = {}
    active_ranges: dict[str, tuple[pd.Timestamp, pd.Timestamp]] = {}
    active_cutoffs: dict[str, pd.Timestamp] = {}
    active_position_intervals: dict[str, list[tuple[pd.Timestamp, pd.Timestamp | None]]] = {}
    active_position_labels: set[str] = set()
    timeline_points: set[pd.Timestamp] = set()
    labels = _unique_result_labels(results, market_key)

    portfolio_equity = pd.Series(dtype=float)
    portfolio_cash = pd.Series(dtype=float)

    for result, label in zip(results, labels, strict=True):
        _record_active_cutoff(active_cutoffs, label, result)
        position_intervals = _result_active_position_intervals(result)
        if position_intervals is None:
            if _result_has_position_activity(result):
                active_position_labels.add(label)
        else:
            active_position_intervals[label] = position_intervals
            for interval_start, interval_end in position_intervals:
                timeline_points.add(interval_start)
                if interval_end is not None:
                    timeline_points.add(interval_end)
        if portfolio_equity.empty:
            portfolio_equity = _pairs_to_series(result.get("joint_portfolio_equity_series") or [])
        if portfolio_cash.empty:
            portfolio_cash = _pairs_to_series(result.get("joint_portfolio_cash_series") or [])

        price_series = _pairs_to_series(result.get("price_series") or [])
        if not price_series.empty:
            if include_market_prices:
                market_prices[label] = [
                    (_to_legacy_datetime(ts), float(value)) for ts, value in price_series.items()
                ]
            _extend_active_range(
                active_ranges, label, price_series.index[0], price_series.index[-1]
            )
            timeline_points.update(price_series.index.to_list())

        if include_overlay_series:
            equity_series = _pairs_to_series(result.get("equity_series") or [])
            cash_series = _pairs_to_series(result.get("cash_series") or [])
            if not equity_series.empty:
                equity_series_by_market[label] = equity_series.astype(float)
                timeline_points.update(equity_series.index.to_list())
                _extend_active_range(
                    active_ranges, label, equity_series.index[0], equity_series.index[-1]
                )
            if not cash_series.empty:
                cash_series_by_market[label] = cash_series.astype(float)
                timeline_points.update(cash_series.index.to_list())
                _extend_active_range(
                    active_ranges, label, cash_series.index[0], cash_series.index[-1]
                )

        if include_fill_events:
            fills.extend(
                _deserialize_fill_events(
                    market_id=label,
                    fill_events=result.get("fill_events") or [],
                    models_module=models_module,
                )
            )
            for event in result.get("fill_events") or []:
                timestamp = pd.to_datetime(event.get("timestamp"), utc=True, errors="coerce")
                if not pd.isna(timestamp):
                    timeline_points.add(timestamp)

    if portfolio_equity.empty and portfolio_cash.empty:
        return None
    if portfolio_equity.empty and not portfolio_cash.empty:
        portfolio_equity = portfolio_cash.astype(float)
    if portfolio_cash.empty and not portfolio_equity.empty:
        portfolio_cash = pd.Series(
            [float(portfolio_equity.iloc[0]), float(portfolio_equity.iloc[-1])],
            index=pd.DatetimeIndex([portfolio_equity.index[0], portfolio_equity.index[-1]]),
            dtype=float,
        )

    joint_initial_capital = _joint_portfolio_initial_capital(
        results[0], equity_series=portfolio_equity
    )

    timeline_points.update(portfolio_equity.index.to_list())
    timeline_points.update(portfolio_cash.index.to_list())
    timeline = (
        pd.DatetimeIndex(sorted(timeline_points)) if timeline_points else portfolio_equity.index
    )
    if joint_initial_capital is not None and len(timeline) > 0 and timeline[0].value > 0:
        timeline = pd.DatetimeIndex([timeline[0] - pd.Timedelta(nanoseconds=1)]).union(timeline)

    aligned_equity = _align_series_to_timeline(
        portfolio_equity,
        timeline,
        before=(
            float(portfolio_equity.iloc[0])
            if joint_initial_capital is None
            else float(joint_initial_capital)
        ),
        after=float(portfolio_equity.iloc[-1]),
    )
    aligned_cash = _align_series_to_timeline(
        portfolio_cash,
        timeline,
        before=(
            float(portfolio_cash.iloc[0])
            if joint_initial_capital is None
            else float(joint_initial_capital)
        ),
        after=float(portfolio_cash.iloc[-1]),
    )

    active_count = pd.Series(0, index=timeline, dtype=int)
    for label, (start, end) in active_ranges.items():
        if label in active_position_intervals:
            for interval_start, interval_end in active_position_intervals[label]:
                active_mask = _position_interval_mask(
                    timeline,
                    start=interval_start,
                    end=interval_end,
                    fallback_end=end,
                    cutoff=active_cutoffs.get(label),
                )
                active_count.loc[active_mask] = active_count.loc[active_mask] + 1
        elif label in active_position_labels:
            active_mask = _active_range_mask(
                timeline,
                start=start,
                end=end,
                cutoff=active_cutoffs.get(label),
            )
            active_count.loc[active_mask] = active_count.loc[active_mask] + 1

    overlay_equity: dict[str, pd.Series] = {}
    overlay_cash: dict[str, pd.Series] = {}
    if include_overlay_series:
        for label, (start, end) in active_ranges.items():
            equity_series = equity_series_by_market.get(label, pd.Series(dtype=float))
            cash_series = cash_series_by_market.get(label, pd.Series(dtype=float))
            if equity_series.empty and cash_series.empty:
                continue

            if equity_series.empty:
                start_equity = float(cash_series.iloc[0]) if not cash_series.empty else 0.0
                end_equity = float(cash_series.iloc[-1]) if not cash_series.empty else start_equity
                equity_series = pd.Series(
                    [start_equity, end_equity],
                    index=pd.DatetimeIndex([start, end]),
                    dtype=float,
                )
            if cash_series.empty:
                cash_series = pd.Series(
                    [float(equity_series.iloc[0]), float(equity_series.iloc[-1])],
                    index=pd.DatetimeIndex([start, end]),
                    dtype=float,
                )

            full_equity = _align_series_to_timeline(
                equity_series,
                timeline,
                before=float(equity_series.iloc[0]),
                after=float(equity_series.iloc[-1]),
            )
            full_cash = _align_series_to_timeline(
                cash_series,
                timeline,
                before=float(cash_series.iloc[0]),
                after=float(cash_series.iloc[-1]),
            )
            active_mask = _overlay_range_mask(
                timeline,
                start=start,
                end=end,
                cutoff=active_cutoffs.get(label),
            )
            clipped_equity = full_equity.copy()
            clipped_cash = full_cash.copy()
            clipped_equity.loc[~active_mask] = float("nan")
            clipped_cash.loc[~active_mask] = float("nan")
            overlay_equity[label] = clipped_equity
            overlay_cash[label] = clipped_cash

    initial_cash = float(aligned_equity.iloc[0])
    final_equity = float(aligned_equity.iloc[-1])
    equity_curve = [
        models_module.PortfolioSnapshot(
            timestamp=_to_legacy_datetime(ts),
            cash=float(aligned_cash.loc[ts]),
            total_equity=float(aligned_equity.loc[ts]),
            unrealized_pnl=float(aligned_equity.loc[ts] - aligned_cash.loc[ts]),
            num_positions=int(active_count.loc[ts]),
        )
        for ts in timeline
    ]

    equity_values = pd.Series([snapshot.total_equity for snapshot in equity_curve], dtype=float)
    running_peak = equity_values.cummax()
    drawdowns = (
        (running_peak - equity_values) / running_peak.where(running_peak > 0.0, pd.NA)
    ).fillna(0.0)
    max_drawdown = float(drawdowns.max()) if not drawdowns.empty else 0.0
    metrics = {
        "final_pnl": final_equity - initial_cash,
        "total_return": _return_fraction(
            initial_capital=initial_cash,
            final_equity=final_equity,
        ),
        "max_drawdown": max_drawdown,
    }

    result = models_module.BacktestResult(
        equity_curve=equity_curve,
        fills=fills if include_fill_events else [],
        metrics=metrics,
        strategy_name=title,
        platform=models_module.Platform.POLYMARKET,
        start_time=_to_legacy_datetime(timeline[0]),
        end_time=_to_legacy_datetime(timeline[-1]),
        initial_cash=float(initial_cash),
        final_equity=float(final_equity),
        num_markets_traded=sum(1 for item in results if int(item.get("fills") or 0) > 0),
        num_markets_resolved=len(results),
        market_prices=market_prices if include_market_prices else {},
        market_pnls={
            label: float(item.get("pnl") or 0.0)
            for item, label in zip(results, labels, strict=True)
        },
        overlay_series=(
            {"equity": overlay_equity, "cash": overlay_cash} if include_overlay_series else {}
        ),
        hide_primary_panel_series=bool(overlay_equity or overlay_cash),
        primary_series_name="Joint Portfolio",
        prepend_total_equity_panel=True,
        total_equity_panel_label="Joint Portfolio Equity",
        plot_monthly_returns=True,
        plot_panels=resolved_plot_panels,
    )

    output_abs = Path(output_path).expanduser().resolve()
    output_abs.parent.mkdir(parents=True, exist_ok=True)
    extra_panels = _build_summary_brier_extra_panels(
        results=results,
        market_key=market_key,
        resolved_plot_panels=resolved_plot_panels,
        max_points_per_market=max_points_per_market,
    )
    layout = plotting_module.plot(
        result,
        filename=str(output_abs),
        max_markets=max(len(market_prices), 30),
        open_browser=False,
        progress=False,
        plot_panels=resolved_plot_panels,
        extra_panels=extra_panels,
    )
    layout = _apply_summary_layout_overrides(
        layout,
        initial_cash=float(initial_cash),
        max_yes_price_fill_markers=_summary_yes_price_fill_marker_limit(
            fill_count=len(fills),
            max_points=downsample_point_limit,
        ),
    )
    return save_legacy_backtest_layout(layout, output_abs, title)


def print_backtest_summary(
    *,
    results: list[dict[str, Any]],
    market_key: str,
    count_key: str,
    count_label: str,
    pnl_label: str,
    empty_message: str = "No markets had sufficient data.",
) -> None:
    """
    Print a normalized backtest summary table.
    """
    if not results:
        print(empty_message)
        return

    labels = _unique_result_labels(results, market_key)
    rows = [_summary_stats_for_result(result) for result in results]
    total_row = _summary_stats_total(rows=rows, results=results)
    col_w = max(len("Market"), len("TOTAL"), *(len(label) for label in labels)) + 2
    count_w = max(8, len(count_label))
    header = (
        f"{'Market':<{col_w}} {count_label:>{count_w}} {'Fills':>6} {'Qty':>10} "
        f"{'AvgPx':>7} {'Notional':>10} {pnl_label:>12} {'Return':>9} "
        f"{'MaxDD':>9} {'Sharpe':>8} {'Sortino':>8} {'PF':>7} {'Coverage':>9}"
    )
    sep = "─" * len(header)

    print(f"\n{sep}\n{header}\n{sep}")
    for result, row, label in zip(results, rows, labels, strict=True):
        print(
            f"{label:<{col_w}} {result[count_key]:>{count_w}} "
            f"{result['fills']:>6} {_format_summary_float(row['fill_qty'], 2):>10} "
            f"{_format_summary_float(row['avg_fill_price'], 4):>7} "
            f"{_format_summary_float(row['fill_notional'], 2):>10} "
            f"{result['pnl']:>+12.4f} {_format_summary_pct(row['return_pct']):>9} "
            f"{_format_summary_pct(row['max_drawdown_pct']):>9} "
            f"{_format_summary_float(row['sharpe'], 2):>8} "
            f"{_format_summary_float(row['sortino'], 2):>8} "
            f"{_format_summary_float(row['profit_factor'], 2):>7} "
            f"{_format_summary_pct(row['coverage_pct']):>9}"
        )

    total_pnl = sum(float(result["pnl"]) for result in results)
    total_fills = sum(int(result["fills"]) for result in results)
    print(sep)
    print(
        f"{'TOTAL':<{col_w}} {sum(int(result[count_key]) for result in results):>{count_w}} "
        f"{total_fills:>6} {_format_summary_float(total_row['fill_qty'], 2):>10} "
        f"{_format_summary_float(total_row['avg_fill_price'], 4):>7} "
        f"{_format_summary_float(total_row['fill_notional'], 2):>10} "
        f"{total_pnl:>+12.4f} {_format_summary_pct(total_row['return_pct']):>9} "
        f"{_format_summary_pct(total_row['max_drawdown_pct']):>9} "
        f"{_format_summary_float(total_row['sharpe'], 2):>8} "
        f"{_format_summary_float(total_row['sortino'], 2):>8} "
        f"{_format_summary_float(total_row['profit_factor'], 2):>7} "
        f"{_format_summary_pct(total_row['coverage_pct']):>9}"
    )
    print(sep)
    _print_portfolio_stats(results)


def _summary_stats_for_result(result: Mapping[str, Any]) -> dict[str, float | None]:
    fill_qty, fill_notional, avg_fill_price = _summary_fill_stats(result.get("fill_events") or ())
    equity_series = _pairs_to_series(result.get("equity_series") or [])
    cash_series = _pairs_to_series(result.get("cash_series") or [])
    pnl_series = _pairs_to_series(result.get("pnl_series") or [])
    initial_capital = _result_initial_capital(
        result,
        equity_series=equity_series,
        cash_series=cash_series,
        pnl_series=pnl_series,
    )
    if equity_series.empty:
        if not cash_series.empty:
            equity_series = cash_series.astype(float)
        elif not pnl_series.empty and initial_capital is not None:
            equity_series = pnl_series.astype(float) + float(initial_capital)
    returns = _summary_returns_from_series(equity_series, initial_capital=initial_capital)
    stats = _summary_return_stats(returns)
    coverage = _coerce_float(result.get("requested_coverage_ratio"))
    return {
        "fill_qty": fill_qty,
        "fill_notional": fill_notional,
        "avg_fill_price": avg_fill_price,
        "return_pct": _summary_total_return_pct_from_series(
            equity_series,
            initial_capital=initial_capital,
        ),
        "max_drawdown_pct": stats["max_drawdown_pct"],
        "sharpe": stats["sharpe"],
        "sortino": stats["sortino"],
        "profit_factor": stats["profit_factor"],
        "coverage_pct": coverage * 100.0 if coverage is not None else None,
    }


def _summary_stats_total(
    *, rows: Sequence[Mapping[str, float | None]], results: Sequence[Mapping[str, Any]]
) -> dict[str, float | None]:
    fill_qty = sum(float(row.get("fill_qty") or 0.0) for row in rows)
    fill_notional = sum(float(row.get("fill_notional") or 0.0) for row in rows)
    avg_fill_price = fill_notional / fill_qty if fill_qty > 0.0 else None
    equity_series = _summary_total_equity_series(results)
    stats = _summary_return_stats(_summary_returns_from_pairs(equity_series))
    coverage_values = [
        float(row["coverage_pct"])
        for row in rows
        if row.get("coverage_pct") is not None and math.isfinite(float(row["coverage_pct"]))
    ]
    return {
        "fill_qty": fill_qty,
        "fill_notional": fill_notional,
        "avg_fill_price": avg_fill_price,
        "return_pct": _summary_total_return_pct(equity_series),
        "max_drawdown_pct": stats["max_drawdown_pct"],
        "sharpe": stats["sharpe"],
        "sortino": stats["sortino"],
        "profit_factor": stats["profit_factor"],
        "coverage_pct": sum(coverage_values) / len(coverage_values) if coverage_values else None,
    }


def _summary_total_equity_series(results: Sequence[Mapping[str, Any]]) -> list[tuple[str, float]]:
    if not results:
        return []

    joint_equity_series = results[0].get("joint_portfolio_equity_series")
    if joint_equity_series:
        series = _pairs_to_series(
            joint_equity_series if isinstance(joint_equity_series, Sequence) else []
        )
        initial_capital = _joint_portfolio_initial_capital(
            results[0],
            equity_series=series,
        )
        series = _series_with_initial_capital_basis(
            series,
            initial_capital=initial_capital,
        )
        return _series_to_iso_pairs(series)

    frame_infos: list[tuple[pd.Series, float]] = []
    for result in results:
        if not isinstance(result, Mapping):
            continue
        equity_series = _pairs_to_series(result.get("equity_series") or [])
        cash_series = _pairs_to_series(result.get("cash_series") or [])
        pnl_series = _pairs_to_series(result.get("pnl_series") or [])
        initial_capital = _result_initial_capital(
            result,
            equity_series=equity_series,
            cash_series=cash_series,
            pnl_series=pnl_series,
        )
        if equity_series.empty:
            if not cash_series.empty:
                equity_series = cash_series.astype(float)
            elif not pnl_series.empty and initial_capital is not None:
                equity_series = pnl_series.astype(float) + float(initial_capital)
            else:
                continue
        frame_infos.append(
            (
                equity_series,
                float(equity_series.iloc[0]) if initial_capital is None else initial_capital,
            )
        )

    if not frame_infos:
        return []

    timeline = frame_infos[0][0].index
    for frame, _ in frame_infos[1:]:
        timeline = timeline.union(frame.index)
    timeline = timeline.sort_values()
    if len(timeline) > 0 and timeline[0].value > 0:
        timeline = pd.DatetimeIndex([timeline[0] - pd.Timedelta(nanoseconds=1)]).union(timeline)

    total = pd.Series(0.0, index=timeline, dtype=float)
    for frame, initial_capital in frame_infos:
        aligned = _align_series_to_timeline(
            frame.astype(float),
            timeline,
            before=float(initial_capital),
            after=float(frame.iloc[-1]),
        )
        total = total.add(aligned, fill_value=0.0)
    return _series_to_iso_pairs(total)


def _summary_fill_stats(fill_events: object) -> tuple[float, float, float | None]:
    if not isinstance(fill_events, Sequence) or isinstance(fill_events, str | bytes):
        return 0.0, 0.0, None

    qty = 0.0
    notional = 0.0
    for event in fill_events:
        if not isinstance(event, Mapping):
            continue
        event_qty = _coerce_float(event.get("quantity")) or 0.0
        event_price = _coerce_float(event.get("price")) or 0.0
        if event_qty <= 0.0 or event_price < 0.0:
            continue
        qty += event_qty
        notional += event_qty * event_price
    return qty, notional, notional / qty if qty > 0.0 else None


def _summary_returns_from_pairs(
    pairs: object,
    *,
    initial_capital: float | None = None,
) -> dict[int, float]:
    series = _pairs_to_series(pairs if isinstance(pairs, Sequence) else [])
    return _summary_returns_from_series(series, initial_capital=initial_capital)


def _summary_returns_from_series(
    series: pd.Series,
    *,
    initial_capital: float | None = None,
) -> dict[int, float]:
    if series.empty:
        return {}

    numeric = _series_with_initial_capital_basis(series, initial_capital=initial_capital)
    if len(numeric) < 2:
        return {}
    if float(numeric.iloc[0]) <= 0.0:
        return {}

    returns = numeric.pct_change().replace([float("inf"), -float("inf")], pd.NA).dropna()
    out: dict[int, float] = {}
    for timestamp, value in returns.items():
        if pd.isna(value):
            continue
        try:
            out[int(pd.Timestamp(timestamp).value)] = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
    return out


def _summary_return_stats(returns: dict[int, float]) -> dict[str, float | None]:
    if not returns:
        return {
            "max_drawdown_pct": None,
            "sharpe": None,
            "sortino": None,
            "profit_factor": None,
        }

    return {
        "max_drawdown_pct": _safe_stat_percent(MaxDrawdown().calculate_from_returns, returns),
        "sharpe": _safe_stat(SharpeRatio().calculate_from_returns, returns),
        "sortino": _safe_stat(SortinoRatio().calculate_from_returns, returns),
        "profit_factor": _safe_stat(ProfitFactor().calculate_from_returns, returns),
    }


def _summary_total_return_pct(
    pairs: object,
    *,
    initial_capital: float | None = None,
) -> float | None:
    series = _pairs_to_series(pairs if isinstance(pairs, Sequence) else [])
    return _summary_total_return_pct_from_series(series, initial_capital=initial_capital)


def _summary_total_return_pct_from_series(
    series: pd.Series,
    *,
    initial_capital: float | None = None,
) -> float | None:
    if series.empty:
        return None

    numeric = _series_with_initial_capital_basis(series, initial_capital=initial_capital)
    if len(numeric) < 2:
        return None

    first = float(numeric.iloc[0])
    last = float(numeric.iloc[-1])
    if first <= 0.0:
        return None
    return (last / first - 1.0) * 100.0


def _safe_stat(func: Any, returns: dict[int, float]) -> float | None:
    try:
        value = func(returns)
    except Exception:
        return None
    return _coerce_float(value)


def _safe_stat_percent(func: Any, returns: dict[int, float]) -> float | None:
    value = _safe_stat(func, returns)
    return value * 100.0 if value is not None else None


def _coerce_float(value: object) -> float | None:
    try:
        result = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _format_summary_float(value: object, decimals: int) -> str:
    result = _coerce_float(value)
    if result is None:
        return "n/a"
    return f"{result:.{decimals}f}"


def _format_summary_pct(value: object) -> str:
    result = _coerce_float(value)
    if result is None:
        return "n/a"
    return f"{result:+.2f}%"


def _print_portfolio_stats(results: Sequence[Mapping[str, Any]]) -> None:
    if not results:
        return
    raw_stats = results[0].get("portfolio_stats")
    if not isinstance(raw_stats, Mapping):
        return

    run_fields = [
        ("Iterations", raw_stats.get("iterations")),
        ("Events", raw_stats.get("total_events")),
        ("Orders", raw_stats.get("total_orders")),
        ("Positions", raw_stats.get("total_positions")),
        ("Elapsed", raw_stats.get("elapsed_time")),
    ]
    formatted_run = []
    for label, value in run_fields:
        number = _coerce_float(value)
        if number is None:
            continue
        if label == "Elapsed":
            formatted_run.append(f"{label}: {number:.3f}s")
        else:
            formatted_run.append(f"{label}: {int(number):,}")
    if formatted_run:
        print("\nPortfolio run stats: " + " | ".join(formatted_run))

    returns = raw_stats.get("stats_returns")
    if isinstance(returns, Mapping):
        selected_returns = _selected_named_stats(
            returns,
            (
                "Sharpe Ratio (252 days)",
                "Sortino Ratio (252 days)",
                "Profit Factor",
                "Risk Return Ratio",
                "Returns Volatility (252 days)",
                "Average (Return)",
            ),
        )
        if selected_returns:
            print("Portfolio return stats: " + " | ".join(selected_returns))

    pnls = raw_stats.get("stats_pnls")
    if not isinstance(pnls, Mapping):
        return
    for currency, stats in pnls.items():
        if not isinstance(stats, Mapping):
            continue
        selected_pnls = _selected_named_stats(
            stats,
            (
                "PnL (total)",
                "PnL% (total)",
                "Win Rate",
                "Expectancy",
                "Avg Winner",
                "Avg Loser",
            ),
        )
        if selected_pnls:
            print(f"Portfolio PnL stats ({currency}): " + " | ".join(selected_pnls))


def _selected_named_stats(stats: Mapping[str, Any], names: Sequence[str]) -> list[str]:
    selected: list[str] = []
    for name in names:
        value = _coerce_float(stats.get(name))
        if value is None:
            continue
        selected.append(f"{name}: {value:.4g}")
    return selected
