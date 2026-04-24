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
import os
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.analysis.reporter import ReportProvider
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.enums import AccountType, BookType, OmsType
from nautilus_trader.model.identifiers import TraderId, Venue
from nautilus_trader.model.objects import Currency, Money
from nautilus_trader.risk.config import RiskEngineConfig
from nautilus_trader.trading.strategy import Strategy

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
    build_legacy_backtest_layout,
    save_legacy_backtest_layout,
)
from prediction_market_extensions.backtesting._result_policies import (
    apply_binary_settlement_pnl,
)


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


def _to_legacy_datetime(timestamp: pd.Timestamp) -> datetime:
    return _timestamp_to_naive_utc_datetime(pd.Timestamp(timestamp))


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


def _parse_float_like(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, int | float):
        return float(value)

    text = str(value).strip().replace("_", "").replace("\u2212", "-")
    if not text:
        return default

    match = re.search(r"[-+]?\d*\.?\d+", text)
    if match is None:
        return default

    try:
        return float(match.group(0))
    except ValueError:
        return default


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
            row.get("filled_qty", row.get("last_qty", row.get("quantity")))
        )
        if quantity <= 0.0:
            continue

        timestamp = pd.to_datetime(
            row.get("ts_last", row.get("ts_event", row.get("ts_init"))), utc=True, errors="coerce"
        )
        if pd.isna(timestamp):
            continue
        assert isinstance(timestamp, pd.Timestamp)

        side_source = str(
            row.get("instrument_side")
            or row.get("instrument_id")
            or row.get("symbol")
            or row.get("market_id")
            or market_id
        )
        normalized_side = (
            "no"
            if (
                side_source.upper().endswith("NO")
                or "-NO" in side_source.upper()
                or ".NO." in side_source.upper()
                or "_NO" in side_source.upper()
            )
            else inferred_side
        )

        events.append(
            {
                "order_id": str(
                    row.get("client_order_id")
                    or row.get("venue_order_id")
                    or row.get("order_id")
                    or f"fill-{idx}"
                ),
                "market_id": market_id,
                "action": str(row.get("side") or row.get("order_side") or "BUY").strip().lower(),
                "side": normalized_side,
                "price": _parse_float_like(row.get("avg_px", row.get("last_px", row.get("price")))),
                "quantity": quantity,
                "timestamp": timestamp.isoformat(),
                "commission": _parse_float_like(
                    row.get("commissions", row.get("commission", row.get("fees")))
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
        timestamp = pd.to_datetime(event.get("timestamp"), utc=True, errors="coerce")
        if pd.isna(timestamp):
            continue
        assert isinstance(timestamp, pd.Timestamp)

        quantity = float(event.get("quantity") or 0.0)
        if quantity <= 0.0:
            continue

        action = str(event.get("action") or "buy").strip().lower()
        event_side = str(event.get("side") or "").strip().lower()
        if event_side == "no":
            fill_side = models_module.Side.NO
        elif event_side == "yes":
            fill_side = models_module.Side.YES
        else:
            fill_side = market_side
        fills.append(
            models_module.Fill(
                order_id=str(event.get("order_id") or f"fill-{idx}"),
                market_id=market_id,
                action=models_module.OrderAction.BUY
                if action == "buy"
                else models_module.OrderAction.SELL,
                side=fill_side,
                price=float(event.get("price") or 0.0),
                quantity=quantity,
                timestamp=_to_legacy_datetime(timestamp),
                commission=float(event.get("commission") or 0.0),
            )
        )

    fills.sort(key=lambda fill: fill.timestamp)
    return fills


def _aggregate_brier_frames(results: Sequence[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}

    for result in results:
        market_id = str(result.get("slug") or result.get("market") or "unknown")
        user_series = _pairs_to_series(result.get("user_probability_series") or [])
        market_series = _pairs_to_series(result.get("market_probability_series") or [])
        outcome_series = _pairs_to_series(result.get("outcome_series") or [])
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
    resolved_plot_panels: Sequence[str],
    max_points_per_market: int,
) -> dict[str, Any]:
    extra_panels: dict[str, Any] = {}
    if (
        PANEL_BRIER_ADVANTAGE not in resolved_plot_panels
        and PANEL_TOTAL_BRIER_ADVANTAGE not in resolved_plot_panels
    ):
        return extra_panels

    brier_frames = _aggregate_brier_frames(results)
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
        )
    except TypeError:
        return apply_fn(layout, initial_cash=float(initial_cash))


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
    apply_default_fill_model: bool = True,
    initial_cash: float,
    probability_window: int,
    price_attr: str,
    count_key: str,
    data_count: int | None = None,
    chart_resample_rule: str | None = None,
    market_key: str = "market",
    open_browser: bool = False,
    emit_html: bool = True,
    return_chart_layout: bool = False,
    return_summary_series: bool = False,
    chart_output_path: str | Path | None = None,
    book_type: BookType = BookType.L1_MBP,
    liquidity_consumption: bool = False,
    queue_position: bool = False,
    latency_model: Any | None = None,
) -> dict[str, Any]:
    """
    Run one prediction-market backtest and emit a legacy chart.

    Prediction-market market orders are taker-style orders against a central
    limit order book. Historical backtests here replay trades/bars without full
    book depth, so we apply a deterministic one-tick adverse fill model by
    default to approximate slippage. Callers can override this with a custom
    ``fill_model`` if needed.
    """
    if fill_model is None and apply_default_fill_model:
        fill_model = PredictionMarketTakerFillModel()

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("BACKTESTER-001"),
            logging=LoggingConfig(log_level="WARNING"),
            risk_engine=RiskEngineConfig(bypass=True),
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
    )
    engine.add_instrument(instrument)
    engine.add_data(data if isinstance(data, list) else list(data))
    engine.add_strategy(strategy)
    engine.run()

    fills = engine.trader.generate_order_fills_report()
    positions = engine.trader.generate_positions_report()
    pnl = extract_realized_pnl(positions)
    price_points = extract_price_points(data, price_attr=price_attr)
    realized_outcome = infer_realized_outcome(instrument)
    fill_events = _serialize_fill_events(market_id=market_id, fills_report=fills)
    result_warnings: list[str] = []
    user_probabilities, market_probabilities, outcomes = build_brier_inputs(
        points=price_points,
        window=probability_window,
        realized_outcome=realized_outcome,
        warnings_out=result_warnings,
    )
    chart_market_prices = build_market_prices(price_points, resample_rule=chart_resample_rule)

    chart_path = str(chart_output_path or f"output/{output_prefix}_{market_id}_legacy.html")
    chart_layout = None
    chart_title = f"{strategy_name} legacy chart"
    if emit_html or return_chart_layout:
        os.makedirs("output", exist_ok=True)
        chart_layout, chart_title = build_legacy_backtest_layout(
            engine=engine,
            output_path=chart_path,
            strategy_name=strategy_name,
            platform=platform,
            initial_cash=initial_cash,
            market_prices={str(instrument.id): chart_market_prices},
            user_probabilities=user_probabilities,
            market_probabilities=market_probabilities,
            outcomes=outcomes,
            open_browser=open_browser,
        )
        if emit_html:
            chart_path = save_legacy_backtest_layout(chart_layout, chart_path, chart_title)
    else:
        chart_path = None

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
        count_key: int(data_count) if data_count is not None else len(data),
        "fills": len(fills),
        "pnl": pnl,
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
    result = apply_binary_settlement_pnl(result)
    if chart_path is not None:
        result["chart_path"] = chart_path
    if return_chart_layout and chart_layout is not None:
        result["chart_layout"] = chart_layout
        result["chart_title"] = chart_title
    if return_summary_series:
        result["price_series"] = summary_price_series or []
        result["pnl_series"] = summary_pnl_series or []
        result["equity_series"] = summary_equity_series or []
        result["cash_series"] = summary_cash_series or []
        result["user_probability_series"] = summary_user_probability_series or []
        result["market_probability_series"] = summary_market_probability_series or []
        result["outcome_series"] = summary_outcome_series or []
        result["fill_events"] = summary_fill_events or []
    return result


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
    timeline_points: set[pd.Timestamp] = set()

    for result in results:
        label = str(result.get(market_key) or "unknown")
        final_pnl = float(result.get("pnl") or 0.0)

        price_series = _pairs_to_series(result.get("price_series") or [])
        if not price_series.empty:
            if include_market_prices:
                market_prices[label] = [
                    (_to_legacy_datetime(ts), float(value)) for ts, value in price_series.items()
                ]
            active_ranges[label] = (price_series.index[0], price_series.index[-1])
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
                start_equity = float(cash_series.iloc[0]) if not cash_series.empty else 100.0
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

        if not equity_series.empty:
            equity_series_by_market[label] = equity_series.astype(float)
            timeline_points.update(equity_series.index.to_list())
            if label not in active_ranges:
                active_ranges[label] = (equity_series.index[0], equity_series.index[-1])
        if not cash_series.empty:
            cash_series_by_market[label] = cash_series.astype(float)
            timeline_points.update(cash_series.index.to_list())
            if label not in active_ranges:
                active_ranges[label] = (cash_series.index[0], cash_series.index[-1])
        if not pnl_series.empty:
            timeline_points.update(pnl_series.index.to_list())
            if label not in active_ranges:
                active_ranges[label] = (pnl_series.index[0], pnl_series.index[-1])

    if timeline_points:
        timeline = pd.DatetimeIndex(sorted(timeline_points))
    else:
        now = pd.Timestamp.now(tz="UTC")
        timeline = pd.DatetimeIndex([now])

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
            before=float(equity_series.iloc[0]),
            after=float(equity_series.iloc[-1]),
        )
        full_cash = _align_series_to_timeline(
            cash_series,
            timeline,
            before=float(cash_series.iloc[0]),
            after=float(cash_series.iloc[-1]),
        )

        aggregate_equity = aggregate_equity.add(full_equity, fill_value=0.0)
        aggregate_cash = aggregate_cash.add(full_cash, fill_value=0.0)

        active_mask = (timeline >= start) & (timeline <= end)
        active_count.loc[active_mask] = active_count.loc[active_mask] + 1

        clipped_equity = full_equity.copy()
        clipped_cash = full_cash.copy()
        clipped_equity.loc[~active_mask] = float("nan")
        clipped_cash.loc[~active_mask] = float("nan")
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
        "total_return": 0.0 if initial_cash == 0 else (final_equity - initial_cash) / initial_cash,
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
    timeline_points: set[pd.Timestamp] = set()

    portfolio_equity = pd.Series(dtype=float)
    portfolio_cash = pd.Series(dtype=float)

    for result in results:
        if portfolio_equity.empty:
            portfolio_equity = _pairs_to_series(result.get("joint_portfolio_equity_series") or [])
        if portfolio_cash.empty:
            portfolio_cash = _pairs_to_series(result.get("joint_portfolio_cash_series") or [])

        label = str(result.get(market_key) or "unknown")
        price_series = _pairs_to_series(result.get("price_series") or [])
        if not price_series.empty:
            if include_market_prices:
                market_prices[label] = [
                    (_to_legacy_datetime(ts), float(value)) for ts, value in price_series.items()
                ]
            active_ranges[label] = (price_series.index[0], price_series.index[-1])
            timeline_points.update(price_series.index.to_list())

        if include_overlay_series:
            equity_series = _pairs_to_series(result.get("equity_series") or [])
            cash_series = _pairs_to_series(result.get("cash_series") or [])
            if not equity_series.empty:
                equity_series_by_market[label] = equity_series.astype(float)
                timeline_points.update(equity_series.index.to_list())
                if label not in active_ranges:
                    active_ranges[label] = (equity_series.index[0], equity_series.index[-1])
            if not cash_series.empty:
                cash_series_by_market[label] = cash_series.astype(float)
                timeline_points.update(cash_series.index.to_list())
                if label not in active_ranges:
                    active_ranges[label] = (cash_series.index[0], cash_series.index[-1])

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

    timeline_points.update(portfolio_equity.index.to_list())
    timeline_points.update(portfolio_cash.index.to_list())
    timeline = (
        pd.DatetimeIndex(sorted(timeline_points)) if timeline_points else portfolio_equity.index
    )

    aligned_equity = _align_series_to_timeline(
        portfolio_equity,
        timeline,
        before=float(portfolio_equity.iloc[0]),
        after=float(portfolio_equity.iloc[-1]),
    )
    aligned_cash = _align_series_to_timeline(
        portfolio_cash,
        timeline,
        before=float(portfolio_cash.iloc[0]),
        after=float(portfolio_cash.iloc[-1]),
    )

    active_count = pd.Series(0, index=timeline, dtype=int)
    for start, end in active_ranges.values():
        active_mask = (timeline >= start) & (timeline <= end)
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
            active_mask = (timeline >= start) & (timeline <= end)
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
        "total_return": 0.0 if initial_cash == 0 else (final_equity - initial_cash) / initial_cash,
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
            str(item.get(market_key) or "unknown"): float(item.get("pnl") or 0.0)
            for item in results
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


def _is_nan_like(value: Any) -> bool:
    if value is None:
        return True
    try:
        return math.isnan(float(value))
    except (TypeError, ValueError):
        return False


def _format_stat_value(value: Any) -> str:
    if _is_nan_like(value):
        return "nan"
    if isinstance(value, float):
        return f"{value:.6f}"
    return str(value)


def print_backtest_summary(
    *,
    results: list[dict[str, Any]],
    market_key: str,
    count_key: str,
    count_label: str,
    pnl_label: str,
    empty_message: str = "No markets had sufficient data.",
) -> None:
    """Print a per-market readout of raw Nautilus stats.

    The previous formatted table elided everything that wasn't fills/PnL.
    Emitting `stats_pnls` and `stats_returns` from the engine result keeps the
    ranking machinery (and human review) honest about Sharpe, expectancy, win
    rate, and per-instrument break-down — which is the data Nautilus already
    computes but the framework was hiding.
    """
    if not results:
        print(empty_message)
        return

    for result in results:
        label = str(result.get(market_key, "?"))
        fills = int(result.get("fills", 0))
        pnl = float(result.get("pnl", 0.0))
        count = int(result.get(count_key, 0))
        print(f"\n{label}  {count_label}={count}  fills={fills}  {pnl_label}={pnl:+.6f}")

        stats_pnls = result.get("stats_pnls") or {}
        if stats_pnls:
            for instrument_id, instrument_stats in stats_pnls.items():
                if len(stats_pnls) > 1:
                    print(f"  [{instrument_id}]")
                for stat_name, stat_val in instrument_stats.items():
                    print(f"    {stat_name}: {_format_stat_value(stat_val)}")
        else:
            print("  (no engine stats_pnls available)")

        stats_returns = result.get("stats_returns") or {}
        for stat_name, stat_val in stats_returns.items():
            print(f"    {stat_name}: {_format_stat_value(stat_val)}")

    total_pnl = sum(float(r.get("pnl", 0.0)) for r in results)
    total_fills = sum(int(r.get("fills", 0)) for r in results)
    print(f"\nTOTAL  fills={total_fills}  {pnl_label}={total_pnl:+.6f}")
