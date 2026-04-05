from __future__ import annotations

import asyncio
from pathlib import Path
import re
from typing import Any

from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._result_policies import ResultPolicy


def _sanitize_chart_label(value: object, *, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default

    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-")
    return sanitized or default


def _resolve_multi_market_chart_output_path(
    *,
    backtest: PredictionMarketBacktest,
    sim: Any,
    sim_index: int,
) -> str | Path | None:
    configured_path = backtest.chart_output_path
    if not backtest.emit_html and configured_path is None:
        return None

    raw_market_id = getattr(sim, "market_slug", None) or getattr(
        sim,
        "market_ticker",
        None,
    )
    market_id = str(raw_market_id or f"market-{sim_index + 1}")
    market_label = _sanitize_chart_label(
        market_id,
        default=f"market-{sim_index + 1}",
    )
    sim_label = _sanitize_chart_label(
        (getattr(sim, "metadata", None) or {}).get("sim_label"),
        default=market_label,
    )
    filename_label = (
        market_label if sim_label == market_label else f"{sim_label}_{market_label}"
    )
    default_filename = f"{backtest.name}_{filename_label}_legacy.html"

    if configured_path is None:
        return str(Path("output") / default_filename)

    raw_path = str(configured_path)
    if "{" in raw_path:
        try:
            resolved = raw_path.format(
                name=backtest.name,
                market_id=market_id,
                sim_label=sim_label,
            )
        except KeyError as exc:
            raise ValueError(
                "chart_output_path may only reference {name}, {market_id}, and {sim_label}."
            ) from exc

        path = Path(resolved)
        if not path.suffix:
            path = path / default_filename
        return str(path)

    path = Path(raw_path)
    if path.suffix:
        unique_label = market_label if sim_label == market_label else sim_label
        return str(path.with_name(f"{path.stem}_{unique_label}{path.suffix}"))
    return str(path / default_filename)


async def run_multi_market_trade_backtest_async(
    *,
    backtest: PredictionMarketBacktest,
) -> list[dict[str, Any]]:
    if (
        backtest.data.platform != "polymarket"
        or backtest.data.data_type != "trade_tick"
        or backtest.data.vendor != "native"
    ):
        raise ValueError(
            "run_multi_market_trade_backtest_async requires Polymarket native trade-tick data"
        )

    results: list[dict[str, Any]] = []
    for sim_index, sim in enumerate(backtest.sims):
        if sim.market_slug is None:
            raise ValueError("market_slug is required for Polymarket trade-tick sims.")

        isolated_backtest = PredictionMarketBacktest(
            name=backtest.name,
            data=backtest.data,
            replays=(sim,),
            strategy_factory=backtest.strategy_factory,
            strategy_configs=backtest.strategy_configs,
            initial_cash=backtest.initial_cash,
            probability_window=backtest.probability_window,
            min_trades=backtest.min_trades,
            min_quotes=backtest.min_quotes,
            min_price_range=backtest.min_price_range,
            default_lookback_days=backtest.default_lookback_days,
            default_lookback_hours=backtest.default_lookback_hours,
            default_start_time=backtest.default_start_time,
            default_end_time=backtest.default_end_time,
            nautilus_log_level=backtest.nautilus_log_level,
            execution=backtest.execution,
            chart_resample_rule=backtest.chart_resample_rule,
            emit_html=backtest.emit_html,
            chart_output_path=_resolve_multi_market_chart_output_path(
                backtest=backtest,
                sim=sim,
                sim_index=sim_index,
            ),
            return_chart_layout=backtest.return_chart_layout,
            return_summary_series=backtest.return_summary_series,
        )
        isolated_results = await isolated_backtest.run_async()
        result = isolated_results[0] if isolated_results else None
        if result is None:
            continue

        result.update(dict(sim.metadata or {}))
        results.append(result)

    return results


def run_reported_multi_market_trade_backtest(
    *,
    backtest: PredictionMarketBacktest,
    report: MarketReportConfig,
    empty_message: str | None = None,
    partial_message: str | None = None,
    result_policy: ResultPolicy | None = None,
) -> list[dict[str, Any]]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        results = asyncio.run(run_multi_market_trade_backtest_async(backtest=backtest))
    else:
        raise RuntimeError(
            "run_reported_multi_market_trade_backtest() cannot run inside an active event loop."
        )

    if not results:
        if empty_message:
            print(empty_message)
        return []

    if partial_message and len(results) < len(backtest.sims):
        print(partial_message.format(completed=len(results), total=len(backtest.sims)))

    if result_policy is not None:
        transformed = result_policy.apply(results)
        if transformed is not None:
            results = transformed

    finalize_market_results(name=backtest.name, results=results, report=report)
    return results


__all__ = [
    "run_multi_market_trade_backtest_async",
    "run_reported_multi_market_trade_backtest",
]
