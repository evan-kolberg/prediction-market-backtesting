from __future__ import annotations

import importlib
from pathlib import Path

from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting.optimizers import ParameterSearchWindow

EXPECTED_PMXT_SOURCES = (
    "local:/Volumes/LaCie/pmxt_raws",
    "archive:r2v2.pmxt.dev",
    "relay:209-209-10-83.sslip.io",
)
EXPECTED_PMXT_LATENCY = {
    "base_latency_ms": 75.0,
    "insert_latency_ms": 10.0,
    "update_latency_ms": 5.0,
    "cancel_latency_ms": 5.0,
}
EXPECTED_RUNNER_EMIT_HTML = True
EXPECTED_CHART_OUTPUT_PATH = "output"
EXPECTED_DETAIL_PLOT_PANELS = (
    "total_equity",
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "total_drawdown",
    "drawdown",
    "total_rolling_sharpe",
    "rolling_sharpe",
    "total_cash_equity",
    "cash_equity",
    "monthly_returns",
    "total_brier_advantage",
    "brier_advantage",
)
EXPECTED_MULTI_SIM_SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)
EXPECTED_25_SIM_SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)

BACKTESTS_ROOT = Path(__file__).resolve().parents[1] / "backtests"
SINGLE_RUNNER = BACKTESTS_ROOT / "polymarket_quote_tick_ema_crossover.py"
INDEPENDENT_MULTI_RUNNER = (
    BACKTESTS_ROOT / "polymarket_quote_tick_independent_multi_replay_runner.py"
)
JOINT_MULTI_RUNNER = BACKTESTS_ROOT / "polymarket_quote_tick_joint_portfolio_runner.py"
RUNNER_25 = BACKTESTS_ROOT / "polymarket_quote_tick_independent_25_replay_runner.py"
OPTIMIZER_RUNNER = BACKTESTS_ROOT / "polymarket_quote_tick_ema_optimizer.py"

EXPECTED_SINGLE_REPLAY = QuoteReplay(
    market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
    token_index=0,
    start_time="2026-04-05T00:00:00Z",
    end_time="2026-04-07T23:59:59Z",
)
EXPECTED_OPTIMIZER_BASE_REPLAY = QuoteReplay(
    market_slug="will-ludvig-aberg-win-the-2026-masters-tournament", token_index=0
)
EXPECTED_OPTIMIZER_TRAIN_WINDOWS = (
    ParameterSearchWindow(
        name="sample-a-full-window",
        start_time="2026-04-05T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    ParameterSearchWindow(
        name="sample-b-2026-04-06-day",
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-06T23:59:59Z",
    ),
    ParameterSearchWindow(
        name="sample-c-2026-04-07-late",
        start_time="2026-04-07T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
)
EXPECTED_OPTIMIZER_HOLDOUT_WINDOWS = (
    ParameterSearchWindow(
        name="sample-d-close-window",
        start_time="2026-04-07T00:00:00Z",
        end_time="2026-04-07T11:59:59Z",
    ),
)


def _import_runner(runner_path: Path):
    return importlib.import_module(f"backtests.{runner_path.stem}")


def _assert_latency_model(latency_model) -> None:
    assert {
        "base_latency_ms": latency_model.base_latency_ms,
        "insert_latency_ms": latency_model.insert_latency_ms,
        "update_latency_ms": latency_model.update_latency_ms,
        "cancel_latency_ms": latency_model.cancel_latency_ms,
    } == EXPECTED_PMXT_LATENCY


def test_quote_tick_single_runner_uses_expected_runtime_contract() -> None:
    module = _import_runner(SINGLE_RUNNER)

    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.EXECUTION.queue_position is True
    _assert_latency_model(module.EXECUTION.latency_model)
    assert module.REPLAYS == (EXPECTED_SINGLE_REPLAY,)
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert module.EXPERIMENT.execution == module.EXECUTION
    assert module.EXPERIMENT.emit_html is EXPECTED_RUNNER_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_DETAIL_PLOT_PANELS


def test_quote_tick_independent_runner_uses_explicit_summary_plot_contract() -> None:
    module = _import_runner(INDEPENDENT_MULTI_RUNNER)

    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_MULTI_SIM_SUMMARY_PLOT_PANELS
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.REPORT.summary_plot_panels == module.SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "independent"
    assert len(module.REPLAYS) == 8
    assert len({replay.market_slug for replay in module.REPLAYS}) == 8
    assert len({str((replay.metadata or {}).get("sim_label")) for replay in module.REPLAYS}) == len(
        module.REPLAYS
    )
    for replay in module.REPLAYS:
        assert replay.market_slug
        assert replay.token_index == 0
        assert isinstance(replay.start_time, str) and replay.start_time
        assert isinstance(replay.end_time, str) and replay.end_time


def test_quote_tick_joint_runner_uses_explicit_summary_plot_contract() -> None:
    module = _import_runner(JOINT_MULTI_RUNNER)

    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_MULTI_SIM_SUMMARY_PLOT_PANELS
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.REPORT.summary_plot_panels == module.SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "joint_portfolio"
    assert tuple(replay.market_slug for replay in module.REPLAYS) == (
        "human-moon-landing-in-2026",
        "new-coronavirus-pandemic-in-2026",
        "will-openais-market-cap-be-between-750b-and-1t-at-market-close-on-ipo-day",
        "okx-ipo-in-2026",
        "nothing-ever-happens-2026",
    )
    assert len({str((replay.metadata or {}).get("sim_label")) for replay in module.REPLAYS}) == len(
        module.REPLAYS
    )
    for replay in module.REPLAYS:
        assert replay.token_index == 0
        assert replay.start_time == module._LONG_WINDOW_START
        assert replay.end_time == module._LONG_WINDOW_END


def test_quote_tick_25_sim_runner_uses_explicit_summary_plot_contract() -> None:
    module = _import_runner(RUNNER_25)

    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_25_SIM_SUMMARY_PLOT_PANELS
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.REPORT.summary_plot_panels == module.SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "independent"
    assert len(module.REPLAYS) == 25
    assert len({replay.market_slug for replay in module.REPLAYS}) >= 8
    assert len({str((replay.metadata or {}).get("sim_label")) for replay in module.REPLAYS}) == len(
        module.REPLAYS
    )
    for replay in module.REPLAYS:
        assert replay.market_slug
        assert replay.token_index == 0
        assert isinstance(replay.start_time, str) and replay.start_time
        assert isinstance(replay.end_time, str) and replay.end_time


def test_quote_tick_optimizer_runner_inline_explicit_search_controls() -> None:
    module = _import_runner(OPTIMIZER_RUNNER)

    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.EXECUTION.queue_position is True
    _assert_latency_model(module.EXECUTION.latency_model)

    assert module.BASE_REPLAY == EXPECTED_OPTIMIZER_BASE_REPLAY
    assert module.TRAIN_WINDOWS == EXPECTED_OPTIMIZER_TRAIN_WINDOWS
    assert module.HOLDOUT_WINDOWS == EXPECTED_OPTIMIZER_HOLDOUT_WINDOWS
    assert module.PARAMETER_GRID == {
        "fast_period": (32, 64, 96),
        "slow_period": (128, 256, 384),
        "entry_buffer": (0.00025, 0.0005),
        "take_profit": (0.005, 0.01),
        "stop_loss": (0.005, 0.01),
    }

    assert module.OPTIMIZER is module.PARAMETER_SEARCH
    assert module.PARAMETER_SEARCH is module.OPTIMIZATION
    assert module.PARAMETER_SEARCH.optimizer_type == "parameter_search"
    assert module.PARAMETER_SEARCH.base_replay == module.BASE_REPLAY
    assert module.PARAMETER_SEARCH.train_windows == module.TRAIN_WINDOWS
    assert module.PARAMETER_SEARCH.holdout_windows == module.HOLDOUT_WINDOWS
    assert module.PARAMETER_SEARCH.execution == module.EXECUTION
