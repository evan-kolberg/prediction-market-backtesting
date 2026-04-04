from __future__ import annotations

import asyncio
from typing import Any

from backtests._shared._polymarket_quote_tick_pmxt_runner import (
    run_single_market_pmxt_backtest,
)
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results


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
    for sim in backtest.sims:
        if sim.market_slug is None:
            raise ValueError("market_slug is required for Polymarket quote-tick sims.")

        result = await run_single_market_pmxt_backtest(
            name=backtest.name,
            market_slug=sim.market_slug,
            token_index=sim.token_index,
            probability_window=backtest.probability_window,
            strategy_configs=backtest.strategy_configs,
            min_quotes=backtest.min_quotes,
            min_price_range=backtest.min_price_range,
            initial_cash=backtest.initial_cash,
            emit_summary=False,
            emit_html=False,
            start_time=sim.start_time,
            end_time=sim.end_time,
            data_sources=backtest.data.sources,
            execution=backtest.execution,
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
