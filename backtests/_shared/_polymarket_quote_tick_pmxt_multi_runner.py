from __future__ import annotations

import asyncio
from typing import Any

from backtests._shared._artifact_paths import resolve_multi_sim_detail_chart_output_path
from backtests._shared._artifact_paths import sanitize_chart_label
from backtests._shared._polymarket_quote_tick_pmxt_runner import (
    run_single_market_pmxt_backtest,
)
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results


def _resolve_multi_sim_chart_output_path(
    *,
    backtest: PredictionMarketBacktest,
    sim: Any,
    sim_index: int,
) -> str | None:
    raw_market_id = getattr(sim, "market_slug", None) or getattr(
        sim, "market_ticker", None
    )
    market_id = str(raw_market_id or f"market-{sim_index + 1}")
    sim_label = sanitize_chart_label(
        (getattr(sim, "metadata", None) or {}).get("sim_label"),
        default=f"sim-{sim_index + 1}",
    )
    market_label = sanitize_chart_label(
        market_id,
        default=f"market-{sim_index + 1}",
    )
    return resolve_multi_sim_detail_chart_output_path(
        backtest_name=backtest.name,
        configured_path=backtest.chart_output_path,
        emit_html=backtest.emit_html,
        market_id=market_id,
        sim_label=sim_label,
        default_filename_label=f"{sim_label}_{market_label}",
        configured_suffix_label=sim_label,
    )


async def run_multi_sim_pmxt_backtest_async(
    *,
    backtest: PredictionMarketBacktest,
) -> list[dict[str, Any]]:
    if (
        backtest.data.platform != "polymarket"
        or backtest.data.data_type != "quote_tick"
        or backtest.data.vendor != "pmxt"
    ):
        raise ValueError(
            "run_multi_sim_pmxt_backtest_async requires PMXT quote-tick data"
        )

    results: list[dict[str, Any]] = []
    for sim_index, sim in enumerate(backtest.sims):
        if sim.market_slug is None:
            raise ValueError("market_slug is required for Polymarket quote-tick sims.")

        result = await run_single_market_pmxt_backtest(
            name=backtest.name,
            market_slug=sim.market_slug,
            token_index=sim.token_index,
            probability_window=backtest.probability_window,
            strategy_factory=backtest.strategy_factory,
            strategy_configs=backtest.strategy_configs,
            min_quotes=backtest.min_quotes,
            min_price_range=backtest.min_price_range,
            initial_cash=backtest.initial_cash,
            chart_resample_rule=backtest.chart_resample_rule,
            emit_summary=False,
            emit_html=backtest.emit_html,
            chart_output_path=_resolve_multi_sim_chart_output_path(
                backtest=backtest,
                sim=sim,
                sim_index=sim_index,
            ),
            return_chart_layout=backtest.return_chart_layout,
            return_summary_series=backtest.return_summary_series,
            detail_plot_panels=backtest.detail_plot_panels,
            start_time=sim.start_time,
            end_time=sim.end_time,
            data_sources=backtest.data.sources,
            execution=backtest.execution,
            nautilus_log_level=backtest.nautilus_log_level,
        )
        if result is None:
            continue

        result.update(dict(sim.metadata or {}))
        results.append(result)

    return results


def run_reported_multi_sim_pmxt_backtest(
    *,
    backtest: PredictionMarketBacktest,
    report: MarketReportConfig,
    empty_message: str | None = None,
    partial_message: str | None = None,
) -> list[dict[str, Any]]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        results = asyncio.run(run_multi_sim_pmxt_backtest_async(backtest=backtest))
    else:
        raise RuntimeError(
            "run_reported_multi_sim_pmxt_backtest() cannot run inside an active event loop."
        )

    if not results:
        if empty_message:
            print(empty_message)
        return []

    if partial_message and len(results) < len(backtest.sims):
        print(partial_message.format(completed=len(results), total=len(backtest.sims)))

    finalize_market_results(name=backtest.name, results=results, report=report)
    return results
