from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.common.component import is_backtest_force_stop
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.enums import AccountType, BookType, OmsType
from nautilus_trader.model.identifiers import TraderId, Venue
from nautilus_trader.model.objects import Currency, Money
from nautilus_trader.risk.config import RiskEngineConfig
from nautilus_trader.trading.strategy import Strategy

from prediction_market_extensions import install_commission_patch
from prediction_market_extensions.adapters.prediction_market import (
    research as prediction_market_research,
)
from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    build_brier_inputs,
    build_market_prices,
    extract_price_points,
    extract_realized_pnl,
    infer_realized_outcome,
)
from prediction_market_extensions.adapters.prediction_market.fill_model import (
    PredictionMarketTakerFillModel,
)
from prediction_market_extensions.analysis.legacy_plot_adapter import (
    build_legacy_backtest_layout,
    save_legacy_backtest_layout,
)


def _record_timestamp_ns(record: object) -> int | None:
    for attr in ("ts_init", "ts_event"):
        value = getattr(record, attr, None)
        if value is None:
            continue
        try:
            timestamp_ns = int(value)
        except (TypeError, ValueError):
            continue
        if timestamp_ns >= 0:
            return timestamp_ns
    return None


def _iso_from_nanos(timestamp_ns: int | None) -> str | None:
    if timestamp_ns is None:
        return None
    return pd.Timestamp(timestamp_ns, unit="ns", tz="UTC").isoformat()


def _data_window_ns(data: Sequence[object]) -> tuple[int | None, int | None]:
    start_ns: int | None = None
    end_ns: int | None = None
    for record in data:
        timestamp_ns = _record_timestamp_ns(record)
        if timestamp_ns is None:
            continue
        if start_ns is None or timestamp_ns < start_ns:
            start_ns = timestamp_ns
        if end_ns is None or timestamp_ns > end_ns:
            end_ns = timestamp_ns
    return start_ns, end_ns


def _coverage_ratio_for_window(
    *, start_ns: int | None, end_ns: int | None, simulated_through_ns: int | None
) -> float | None:
    if start_ns is None or end_ns is None:
        return None
    if end_ns <= start_ns:
        return 1.0 if simulated_through_ns is not None else 0.0
    if simulated_through_ns is None:
        return None

    clamped_end_ns = min(max(simulated_through_ns, start_ns), end_ns)
    return (clamped_end_ns - start_ns) / (end_ns - start_ns)


def build_backtest_run_state(
    *,
    data: Sequence[object],
    backtest_end_ns: int | None,
    forced_stop: bool,
    requested_start_ns: int | None = None,
    requested_end_ns: int | None = None,
) -> dict[str, Any]:
    loaded_start_ns, loaded_end_ns = _data_window_ns(data)
    planned_start_ns = loaded_start_ns if requested_start_ns is None else requested_start_ns
    planned_end_ns = loaded_end_ns if requested_end_ns is None else requested_end_ns
    simulated_through_ns = backtest_end_ns
    if simulated_through_ns is None and forced_stop:
        simulated_through_ns = planned_start_ns

    requested_coverage_ratio = _coverage_ratio_for_window(
        start_ns=planned_start_ns, end_ns=planned_end_ns, simulated_through_ns=simulated_through_ns
    )
    loaded_coverage_ratio = _coverage_ratio_for_window(
        start_ns=loaded_start_ns, end_ns=loaded_end_ns, simulated_through_ns=simulated_through_ns
    )

    terminated_by_window = False
    if loaded_end_ns is not None and simulated_through_ns is not None:
        terminated_by_window = simulated_through_ns < loaded_end_ns

    terminated_early = bool(forced_stop or terminated_by_window)
    stop_reason: str | None = None
    if forced_stop:
        stop_reason = "account_error"
    elif terminated_by_window:
        stop_reason = "incomplete_window"

    return {
        "terminated_early": terminated_early,
        "stop_reason": stop_reason,
        "planned_start": _iso_from_nanos(planned_start_ns),
        "planned_end": _iso_from_nanos(planned_end_ns),
        "loaded_start": _iso_from_nanos(loaded_start_ns),
        "loaded_end": _iso_from_nanos(loaded_end_ns),
        "simulated_through": _iso_from_nanos(simulated_through_ns),
        "coverage_ratio": loaded_coverage_ratio,
        "requested_coverage_ratio": requested_coverage_ratio,
    }


def apply_backtest_run_state(
    *, result: dict[str, Any], run_state: dict[str, Any]
) -> dict[str, Any]:
    result.update(run_state)
    return result


def print_backtest_result_warnings(*, results: Sequence[dict[str, Any]], market_key: str) -> None:
    warning_lines: list[str] = []
    for result in results:
        if not bool(result.get("terminated_early")):
            continue
        market_label = str(
            result.get(market_key)
            or result.get("slug")
            or result.get("ticker")
            or result.get("instrument_id")
            or "backtest"
        )
        stop_reason = str(result.get("stop_reason") or "unknown")
        simulated_through = str(result.get("simulated_through") or "unknown")
        coverage_ratio = result.get("coverage_ratio")
        requested_coverage_ratio = result.get("requested_coverage_ratio")
        coverage_parts: list[str] = []
        if isinstance(coverage_ratio, int | float):
            coverage_parts.append(f"{float(coverage_ratio) * 100.0:.1f}% of the loaded-data window")
        if isinstance(requested_coverage_ratio, int | float):
            coverage_parts.append(
                f"{float(requested_coverage_ratio) * 100.0:.1f}% of the requested window"
            )
        coverage_text = ", ".join(coverage_parts) if coverage_parts else "unknown coverage"
        if stop_reason == "account_error":
            warning_lines.append(
                f"WARNING: {market_label} terminated early after an engine AccountError "
                f"at {simulated_through} ({coverage_text})."
            )
            continue
        warning_lines.append(
            f"WARNING: {market_label} terminated early at {simulated_through} ({coverage_text})."
        )
    if warning_lines:
        print()
        for line in warning_lines:
            print(line)


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
    nautilus_log_level: str = "INFO",
    requested_start_ns: int | None = None,
    requested_end_ns: int | None = None,
) -> dict[str, Any]:
    install_commission_patch()
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
    )
    engine.add_instrument(instrument)
    engine.add_data(data_records)
    engine.add_strategy(strategy)
    try:
        engine.run()

        run_result = engine.get_result()
        forced_stop = bool(is_backtest_force_stop())
        run_state = build_backtest_run_state(
            data=data_records,
            backtest_end_ns=run_result.backtest_end,
            forced_stop=forced_stop,
            requested_start_ns=requested_start_ns,
            requested_end_ns=requested_end_ns,
        )

        fills = engine.trader.generate_order_fills_report()
        positions = engine.trader.generate_positions_report()
        pnl = extract_realized_pnl(positions)
        price_points = extract_price_points(data_records, price_attr=price_attr)
        realized_outcome = infer_realized_outcome(instrument)
        if realized_outcome is None:
            import logging
            logging.getLogger(__name__).warning(
                "Market %s has no realized outcome; P&L based on mark-to-market, not settlement",
                instrument.id,
            )
        user_probabilities, market_probabilities, outcomes = build_brier_inputs(
            points=price_points,
            window=probability_window,
            realized_outcome=realized_outcome,
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
            summary_legacy_models, _ = (
                prediction_market_research.legacy_plot_adapter._load_legacy_modules()
            )
            summary_legacy_fills = prediction_market_research.legacy_plot_adapter._convert_fills(
                fills, summary_legacy_models
            )
            summary_market_prices = (
                prediction_market_research.legacy_plot_adapter._market_prices_with_fill_points(
                    {market_id: chart_market_prices}, summary_legacy_fills
                ).get(market_id, chart_market_prices)
            )
            dense_equity_series, dense_cash_series = (
                prediction_market_research._dense_account_series_from_engine(
                    engine=engine,
                    market_id=market_id,
                    market_prices=chart_market_prices,
                    initial_cash=initial_cash,
                )
            )
            summary_price_series = prediction_market_research._series_to_iso_pairs(
                prediction_market_research._pairs_to_series(summary_market_prices)
            )
            pnl_series = (
                dense_equity_series - float(dense_equity_series.iloc[0])
                if not dense_equity_series.empty
                else prediction_market_research._extract_account_pnl_series(engine)
            )
            if not pnl_series.empty:
                summary_pnl_series = prediction_market_research._series_to_iso_pairs(pnl_series)
            if not dense_equity_series.empty:
                summary_equity_series = prediction_market_research._series_to_iso_pairs(
                    dense_equity_series
                )
            if not dense_cash_series.empty:
                summary_cash_series = prediction_market_research._series_to_iso_pairs(
                    dense_cash_series
                )
            if not user_probabilities.empty:
                summary_user_probability_series = prediction_market_research._series_to_iso_pairs(
                    user_probabilities
                )
            if not market_probabilities.empty:
                summary_market_probability_series = prediction_market_research._series_to_iso_pairs(
                    market_probabilities
                )
            if not outcomes.empty:
                summary_outcome_series = prediction_market_research._series_to_iso_pairs(outcomes)
            summary_fill_events = prediction_market_research._serialize_fill_events(
                market_id=market_id, fills_report=fills
            )

        result = {
            market_key: market_id,
            count_key: int(data_count) if data_count is not None else len(data_records),
            "fills": len(fills),
            "pnl": pnl,
            "instrument_id": str(instrument.id),
        }
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
        return apply_backtest_run_state(result=result, run_state=run_state)
    finally:
        engine.reset()
        engine.dispose()


__all__ = [
    "apply_backtest_run_state",
    "build_backtest_run_state",
    "print_backtest_result_warnings",
    "run_market_backtest",
]
