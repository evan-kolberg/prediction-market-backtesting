from __future__ import annotations

import asyncio
import contextlib
import multiprocessing
import pickle
import tempfile
import traceback
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any

from backtests._shared._artifact_paths import resolve_multi_sim_detail_chart_output_path
from backtests._shared._artifact_paths import sanitize_chart_label

if TYPE_CHECKING:
    from backtests._shared._prediction_market_backtest import MarketReportConfig
    from backtests._shared._prediction_market_backtest import PredictionMarketBacktest


async def run_single_market_pmxt_backtest(**kwargs: Any) -> dict[str, Any] | None:
    from backtests._shared._polymarket_quote_tick_pmxt_runner import (
        run_single_market_pmxt_backtest as _run_single_market_pmxt_backtest,
    )

    return await _run_single_market_pmxt_backtest(**kwargs)


_DEFAULT_RUN_SINGLE_MARKET_PMXT_BACKTEST = run_single_market_pmxt_backtest


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


def _single_sim_worker_kwargs(
    *,
    backtest: PredictionMarketBacktest,
    sim: Any,
    sim_index: int,
) -> dict[str, Any]:
    return {
        "name": backtest.name,
        "market_slug": sim.market_slug,
        "token_index": sim.token_index,
        "probability_window": backtest.probability_window,
        "strategy_factory": backtest.strategy_factory,
        "strategy_configs": backtest.strategy_configs,
        "min_quotes": backtest.min_quotes,
        "min_price_range": backtest.min_price_range,
        "initial_cash": backtest.initial_cash,
        "chart_resample_rule": backtest.chart_resample_rule,
        "emit_summary": False,
        "emit_html": backtest.emit_html,
        "chart_output_path": _resolve_multi_sim_chart_output_path(
            backtest=backtest,
            sim=sim,
            sim_index=sim_index,
        ),
        "return_chart_layout": backtest.return_chart_layout,
        "return_summary_series": backtest.return_summary_series,
        "detail_plot_panels": backtest.detail_plot_panels,
        "start_time": sim.start_time,
        "end_time": sim.end_time,
        "data_sources": backtest.data.sources,
        "execution": backtest.execution,
        "nautilus_log_level": backtest.nautilus_log_level,
    }


def _single_sim_worker(
    worker_kwargs: dict[str, Any],
    result_path: str,
    send_conn: Any,
) -> None:
    try:
        from prediction_market_extensions import install_commission_patch

        install_commission_patch()

        result = asyncio.run(run_single_market_pmxt_backtest(**worker_kwargs))
        with open(result_path, "wb") as result_file:
            pickle.dump(result, result_file)
        send_conn.send(("ok", result_path))
    except BaseException as exc:  # pragma: no cover - exercised via subprocess
        send_conn.send(
            (
                "error",
                {
                    "error": repr(exc),
                    "traceback": traceback.format_exc(),
                },
            )
        )
    finally:
        send_conn.close()


def _run_single_sim_in_subprocess(
    *,
    worker_kwargs: dict[str, Any],
) -> dict[str, Any] | None:
    ctx = multiprocessing.get_context("spawn")
    recv_conn, send_conn = ctx.Pipe(duplex=False)
    with tempfile.NamedTemporaryFile(
        prefix="pmxt-multi-sim-",
        suffix=".pkl",
        delete=False,
    ) as result_file:
        result_path = result_file.name
    process = ctx.Process(
        target=_single_sim_worker,
        args=(worker_kwargs, result_path, send_conn),
        daemon=False,
    )
    process.start()
    send_conn.close()

    payload: tuple[str, Any] | None = None
    try:
        payload = recv_conn.recv()
    except EOFError:
        payload = None
    finally:
        recv_conn.close()
        process.join()

    try:
        if payload is not None:
            status, data = payload
            if status == "ok":
                if process.exitcode not in (0, None):
                    raise RuntimeError(
                        f"PMXT worker exited with code {process.exitcode} after returning a result."
                    )
                with open(data, "rb") as result_file:
                    return pickle.load(result_file)

            if status == "error":
                message = data.get("error", "Unknown worker error")
                worker_traceback = data.get("traceback", "")
                raise RuntimeError(
                    f"{message}\n\nChild traceback:\n{worker_traceback}".rstrip()
                )

            raise RuntimeError(f"Unexpected PMXT worker payload status {status!r}")

        raise RuntimeError(
            f"PMXT worker exited with code {process.exitcode} without returning a result."
        )
    finally:
        with contextlib.suppress(FileNotFoundError):
            Path(result_path).unlink()


def _run_multi_sim_pmxt_backtest(
    *,
    backtest: PredictionMarketBacktest,
) -> list[dict[str, Any]]:
    if (
        backtest.data.platform != "polymarket"
        or backtest.data.data_type != "quote_tick"
        or backtest.data.vendor != "pmxt"
    ):
        raise ValueError("_run_multi_sim_pmxt_backtest requires PMXT quote-tick data")

    results: list[dict[str, Any]] = []
    use_in_process_runner = (
        backtest.strategy_factory is not None
        or run_single_market_pmxt_backtest
        is not _DEFAULT_RUN_SINGLE_MARKET_PMXT_BACKTEST
    )
    for sim_index, sim in enumerate(backtest.sims):
        if sim.market_slug is None:
            raise ValueError("market_slug is required for Polymarket quote-tick sims.")

        worker_kwargs = _single_sim_worker_kwargs(
            backtest=backtest,
            sim=sim,
            sim_index=sim_index,
        )
        if use_in_process_runner:
            result = asyncio.run(run_single_market_pmxt_backtest(**worker_kwargs))
        else:
            result = _run_single_sim_in_subprocess(worker_kwargs=worker_kwargs)
        if result is None:
            continue

        result.update(dict(sim.metadata or {}))
        results.append(result)

    return results


async def run_multi_sim_pmxt_backtest_async(
    *,
    backtest: PredictionMarketBacktest,
) -> list[dict[str, Any]]:
    return await asyncio.to_thread(
        _run_multi_sim_pmxt_backtest,
        backtest=backtest,
    )


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
        results = _run_multi_sim_pmxt_backtest(backtest=backtest)
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

    from backtests._shared._prediction_market_backtest import finalize_market_results

    finalize_market_results(name=backtest.name, results=results, report=report)
    return results
