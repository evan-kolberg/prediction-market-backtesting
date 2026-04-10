from __future__ import annotations

import asyncio
from typing import Any

from prediction_market_extensions.backtesting._artifact_paths import (
    resolve_independent_replay_detail_chart_output_path,
)
from prediction_market_extensions.backtesting._artifact_paths import sanitize_chart_label
from prediction_market_extensions.backtesting._isolated_replay_runner import (
    run_single_replay_backtest_in_subprocess,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)


_DEFAULT_PREDICTION_MARKET_BACKTEST_RUN_ASYNC = PredictionMarketBacktest.run_async


def _resolve_independent_replay_chart_output_path(
    *, backtest: PredictionMarketBacktest, sim: Any, sim_index: int
) -> str | None:
    raw_market_id = getattr(sim, "market_slug", None) or getattr(sim, "market_ticker", None)
    market_id = str(raw_market_id or f"market-{sim_index + 1}")
    market_label = sanitize_chart_label(market_id, default=f"market-{sim_index + 1}")
    sim_label = sanitize_chart_label(
        (getattr(sim, "metadata", None) or {}).get("sim_label"), default=market_label
    )
    filename_label = market_label if sim_label == market_label else f"{sim_label}_{market_label}"
    return resolve_independent_replay_detail_chart_output_path(
        backtest_name=backtest.name,
        configured_path=backtest.chart_output_path,
        emit_html=backtest.emit_html,
        market_id=market_id,
        sim_label=sim_label,
        default_filename_label=filename_label,
        configured_suffix_label=market_label if sim_label == market_label else sim_label,
    )


def _single_replay_backtest_kwargs(
    *, backtest: PredictionMarketBacktest, sim: Any, sim_index: int
) -> dict[str, Any]:
    return {
        "name": backtest.name,
        "data": backtest.data,
        "replays": (sim,),
        "strategy_factory": backtest.strategy_factory,
        "strategy_configs": backtest.strategy_configs,
        "initial_cash": backtest.initial_cash,
        "probability_window": backtest.probability_window,
        "min_trades": backtest.min_trades,
        "min_quotes": backtest.min_quotes,
        "min_price_range": backtest.min_price_range,
        "default_lookback_days": backtest.default_lookback_days,
        "default_lookback_hours": backtest.default_lookback_hours,
        "default_start_time": backtest.default_start_time,
        "default_end_time": backtest.default_end_time,
        "nautilus_log_level": backtest.nautilus_log_level,
        "execution": backtest.execution,
        "chart_resample_rule": backtest.chart_resample_rule,
        "emit_html": backtest.emit_html,
        "chart_output_path": _resolve_independent_replay_chart_output_path(
            backtest=backtest, sim=sim, sim_index=sim_index
        ),
        "return_chart_layout": backtest.return_chart_layout,
        "return_summary_series": backtest.return_summary_series,
        "detail_plot_panels": backtest.detail_plot_panels,
    }


async def run_independent_multi_replay_backtest_async(
    *, backtest: PredictionMarketBacktest
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    use_in_process_runner = (
        backtest.strategy_factory is not None
        or PredictionMarketBacktest.run_async is not _DEFAULT_PREDICTION_MARKET_BACKTEST_RUN_ASYNC
    )
    for sim_index, sim in enumerate(backtest.sims):
        if (
            getattr(sim, "market_slug", None) is None
            and getattr(sim, "market_ticker", None) is None
        ):
            raise ValueError("market_slug or market_ticker is required for multi-market sims.")

        single_replay_backtest_kwargs = _single_replay_backtest_kwargs(
            backtest=backtest, sim=sim, sim_index=sim_index
        )
        if use_in_process_runner:
            isolated_backtest = PredictionMarketBacktest(**single_replay_backtest_kwargs)
            isolated_results = await isolated_backtest.run_async()
            result = isolated_results[0] if isolated_results else None
        else:
            result = await asyncio.to_thread(
                run_single_replay_backtest_in_subprocess,
                backtest_kwargs=single_replay_backtest_kwargs,
            )
        if result is None:
            continue

        result.update(dict(getattr(sim, "metadata", None) or {}))
        results.append(result)

    return results


__all__ = ["run_independent_multi_replay_backtest_async"]
