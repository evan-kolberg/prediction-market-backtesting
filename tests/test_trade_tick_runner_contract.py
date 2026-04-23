from __future__ import annotations

import importlib
from pathlib import Path

from prediction_market_extensions.backtesting._replay_specs import TradeReplay

EXPECTED_INITIAL_CASH = 100.0
EXPECTED_EMIT_HTML = True
EXPECTED_MULTI_RUNNER_EMIT_HTML = False
EXPECTED_CHART_OUTPUT_PATH = "output"
EXPECTED_KALSHI_DETAIL_PLOT_PANELS = (
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
EXPECTED_POLYMARKET_DETAIL_PLOT_PANELS = (
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
EXPECTED_SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)
EXPECTED_KALSHI_TRADE_SOURCES = ("rest:https://api.elections.kalshi.com/trade-api/v2",)
EXPECTED_POLYMARKET_TRADE_SOURCES = (
    "gamma:https://gamma-api.polymarket.com",
    "trades:https://data-api.polymarket.com",
    "clob:https://clob.polymarket.com",
)
EXPECTED_TRADE_TICK_LATENCY = {
    "base_latency_ms": 75.0,
    "insert_latency_ms": 10.0,
    "update_latency_ms": 5.0,
    "cancel_latency_ms": 5.0,
}

BACKTESTS_ROOT = Path(__file__).resolve().parents[1] / "backtests"

KALSHI_SINGLE_RUNNER = BACKTESTS_ROOT / "kalshi_trade_tick_breakout.py"
KALSHI_INDEPENDENT_RUNNER = BACKTESTS_ROOT / "kalshi_trade_tick_independent_multi_replay_runner.py"
KALSHI_JOINT_RUNNER = BACKTESTS_ROOT / "kalshi_trade_tick_joint_portfolio_runner.py"
POLYMARKET_SINGLE_RUNNER = BACKTESTS_ROOT / "polymarket_trade_tick_vwap_reversion.py"
POLYMARKET_INDEPENDENT_RUNNER = (
    BACKTESTS_ROOT / "polymarket_trade_tick_independent_multi_replay_runner.py"
)
POLYMARKET_JOINT_RUNNER = BACKTESTS_ROOT / "polymarket_trade_tick_joint_portfolio_runner.py"

EXPECTED_KALSHI_SINGLE_REPLAY = TradeReplay(
    market_ticker="KXLAYOFFSYINFO-26-494000",
    start_time="2026-03-15T00:00:00Z",
    end_time="2026-04-08T23:59:59Z",
)
EXPECTED_KALSHI_MULTI_REPLAYS = (
    TradeReplay(
        market_ticker="KXLAYOFFSYINFO-26-494000",
        start_time="2026-03-15T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "layoffs-infotech-window"},
    ),
    TradeReplay(
        market_ticker="KXCITRINI-28JUL01",
        start_time="2026-03-18T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "citrini-jul-window"},
    ),
    TradeReplay(
        market_ticker="KXPRESNOMR-28-MR",
        start_time="2026-03-24T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
        metadata={"sim_label": "presnomr-window"},
    ),
)
EXPECTED_POLYMARKET_SINGLE_REPLAY = TradeReplay(
    market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
    start_time="2026-02-21T16:00:00Z",
    end_time="2026-03-31T23:59:59Z",
)
EXPECTED_POLYMARKET_MULTI_REPLAYS = (
    TradeReplay(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        lookback_days=7,
        end_time="2026-03-26T23:53:59Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1774569239000000000},
    ),
    TradeReplay(
        market_slug="will-man-city-win-the-202526-champions-league",
        lookback_days=7,
        end_time="2026-03-18T01:28:17Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773797297000000000},
    ),
    TradeReplay(
        market_slug="will-chelsea-win-the-202526-champions-league",
        lookback_days=7,
        end_time="2026-03-18T01:22:09Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773796929000000000},
    ),
    TradeReplay(
        market_slug="will-newcastle-win-the-202526-champions-league",
        lookback_days=7,
        end_time="2026-03-18T22:56:01Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773874561000000000},
    ),
    TradeReplay(
        market_slug="will-leverkusen-win-the-202526-champions-league",
        lookback_days=7,
        end_time="2026-03-18T01:28:15Z",
        outcome="Yes",
        metadata={"market_close_time_ns": 1773797295000000000},
    ),
)


def _import_runner(runner_path: Path):
    return importlib.import_module(f"backtests.{runner_path.stem}")


def _assert_trade_tick_execution(module) -> None:
    assert module.EXECUTION.queue_position is False
    assert {
        "base_latency_ms": module.EXECUTION.latency_model.base_latency_ms,
        "insert_latency_ms": module.EXECUTION.latency_model.insert_latency_ms,
        "update_latency_ms": module.EXECUTION.latency_model.update_latency_ms,
        "cancel_latency_ms": module.EXECUTION.latency_model.cancel_latency_ms,
    } == EXPECTED_TRADE_TICK_LATENCY
    assert module.EXPERIMENT.execution == module.EXECUTION


def test_kalshi_trade_tick_single_runner_uses_expected_runtime_contract() -> None:
    module = _import_runner(KALSHI_SINGLE_RUNNER)

    assert module.DATA.sources == EXPECTED_KALSHI_TRADE_SOURCES
    _assert_trade_tick_execution(module)
    assert module.DETAIL_PLOT_PANELS == EXPECTED_KALSHI_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.emit_html is EXPECTED_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_KALSHI_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.initial_cash == EXPECTED_INITIAL_CASH
    assert module.REPLAYS == (EXPECTED_KALSHI_SINGLE_REPLAY,)


def test_kalshi_trade_tick_independent_runner_uses_expected_runtime_contract() -> None:
    module = _import_runner(KALSHI_INDEPENDENT_RUNNER)

    assert module.DATA.sources == EXPECTED_KALSHI_TRADE_SOURCES
    _assert_trade_tick_execution(module)
    assert module.DETAIL_PLOT_PANELS == EXPECTED_KALSHI_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.emit_html is EXPECTED_MULTI_RUNNER_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_KALSHI_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "independent"
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.REPORT.summary_plot_panels == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.REPLAYS == EXPECTED_KALSHI_MULTI_REPLAYS


def test_kalshi_trade_tick_joint_runner_uses_expected_runtime_contract() -> None:
    module = _import_runner(KALSHI_JOINT_RUNNER)

    assert module.DATA.sources == EXPECTED_KALSHI_TRADE_SOURCES
    _assert_trade_tick_execution(module)
    assert module.DETAIL_PLOT_PANELS == EXPECTED_KALSHI_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "joint_portfolio"
    assert module.REPLAYS == EXPECTED_KALSHI_MULTI_REPLAYS


def test_polymarket_trade_tick_single_runner_uses_expected_runtime_contract() -> None:
    module = _import_runner(POLYMARKET_SINGLE_RUNNER)

    assert module.DATA.sources == EXPECTED_POLYMARKET_TRADE_SOURCES
    _assert_trade_tick_execution(module)
    assert module.DETAIL_PLOT_PANELS == EXPECTED_POLYMARKET_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.emit_html is EXPECTED_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_POLYMARKET_DETAIL_PLOT_PANELS
    assert module.REPLAYS == (EXPECTED_POLYMARKET_SINGLE_REPLAY,)


def test_polymarket_trade_tick_independent_runner_uses_fixed_replay_windows() -> None:
    module = _import_runner(POLYMARKET_INDEPENDENT_RUNNER)

    assert module.DATA.sources == EXPECTED_POLYMARKET_TRADE_SOURCES
    _assert_trade_tick_execution(module)
    assert module.DETAIL_PLOT_PANELS == EXPECTED_POLYMARKET_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.emit_html is EXPECTED_MULTI_RUNNER_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_POLYMARKET_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "independent"
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.REPORT.summary_plot_panels == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.FIXED_LOOKBACK_DAYS == 7
    assert module.REPLAYS == EXPECTED_POLYMARKET_MULTI_REPLAYS


def test_polymarket_trade_tick_joint_runner_uses_fixed_replay_windows() -> None:
    module = _import_runner(POLYMARKET_JOINT_RUNNER)

    assert module.DATA.sources == EXPECTED_POLYMARKET_TRADE_SOURCES
    _assert_trade_tick_execution(module)
    assert module.DETAIL_PLOT_PANELS == EXPECTED_POLYMARKET_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "joint_portfolio"
    assert module.FIXED_LOOKBACK_DAYS == 7
    assert module.REPLAYS == EXPECTED_POLYMARKET_MULTI_REPLAYS
