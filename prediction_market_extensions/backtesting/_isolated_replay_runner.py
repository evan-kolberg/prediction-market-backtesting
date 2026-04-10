from __future__ import annotations

import asyncio
import contextlib
import multiprocessing
import pickle
import tempfile
import traceback
from pathlib import Path
from typing import Any


def _single_replay_worker(backtest_kwargs: dict[str, Any], result_path: str, send_conn: Any) -> None:
    try:
        from prediction_market_extensions import install_commission_patch
        from prediction_market_extensions.backtesting._timing_harness import install_timing_harness

        install_commission_patch()
        install_timing_harness()

        from prediction_market_extensions.backtesting._prediction_market_backtest import PredictionMarketBacktest

        backtest = PredictionMarketBacktest(**backtest_kwargs)
        isolated_results = asyncio.run(backtest.run_async())
        result = isolated_results[0] if isolated_results else None
        with open(result_path, "wb") as result_file:
            pickle.dump(result, result_file)
        send_conn.send(("ok", result_path))
    except BaseException as exc:  # pragma: no cover - exercised via subprocess
        send_conn.send(("error", {"error": repr(exc), "traceback": traceback.format_exc()}))
    finally:
        send_conn.close()


def run_single_replay_backtest_in_subprocess(*, backtest_kwargs: dict[str, Any]) -> dict[str, Any] | None:
    ctx = multiprocessing.get_context("spawn")
    recv_conn, send_conn = ctx.Pipe(duplex=False)
    with tempfile.NamedTemporaryFile(prefix="prediction-market-backtest-", suffix=".pkl", delete=False) as result_file:
        result_path = result_file.name
    process = ctx.Process(target=_single_replay_worker, args=(backtest_kwargs, result_path, send_conn), daemon=False)
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
                    raise RuntimeError(f"Backtest worker exited with code {process.exitcode} after returning a result.")
                with open(data, "rb") as result_file:
                    return pickle.load(result_file)

            if status == "error":
                message = data.get("error", "Unknown worker error")
                worker_traceback = data.get("traceback", "")
                raise RuntimeError(f"{message}\n\nChild traceback:\n{worker_traceback}".rstrip())

            raise RuntimeError(f"Unexpected worker payload status {status!r}")

        raise RuntimeError(f"Backtest worker exited with code {process.exitcode} without returning a result.")
    finally:
        with contextlib.suppress(FileNotFoundError):
            Path(result_path).unlink()
