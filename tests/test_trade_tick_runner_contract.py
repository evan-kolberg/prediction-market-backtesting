from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from backtests._shared._replay_specs import KalshiTradeTickReplay
from backtests._shared._replay_specs import PolymarketTradeTickReplay


EXPECTED_INITIAL_CASH = 100.0
EXPECTED_FIXED_SPORTS_LOOKBACK_DAYS = 7
EXPECTED_EMIT_HTML = True
EXPECTED_CHART_OUTPUT_PATH = "output"
EXPECTED_DETAIL_PLOT_PANELS = (
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)
EXPECTED_SUMMARY_PLOT_PANELS = (
    "total_equity",
    "equity",
    "periodic_pnl",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)
EXPECTED_KALSHI_TRADE_SOURCES = ("rest:https://api.elections.kalshi.com/trade-api/v2",)
EXPECTED_POLYMARKET_TRADE_SOURCES = (
    "gamma:https://gamma-api.polymarket.com",
    "trades:https://data-api.polymarket.com",
    "clob:https://clob.polymarket.com",
)

EXPECTED_KALSHI_REPLAYS = {
    "kalshi_trade_tick_breakout": KalshiTradeTickReplay(
        market_ticker="KXLAYOFFSYINFO-26-494000",
        start_time="2026-03-15T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
    ),
    "kalshi_trade_tick_ema_crossover": KalshiTradeTickReplay(
        market_ticker="KXCITRINI-28JUL01",
        start_time="2026-03-18T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
    ),
    "kalshi_trade_tick_panic_fade": KalshiTradeTickReplay(
        market_ticker="KXGREENLAND-29",
        start_time="2026-03-20T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
    ),
    "kalshi_trade_tick_rsi_reversion": KalshiTradeTickReplay(
        market_ticker="CONTROLH-2026-R",
        start_time="2026-03-22T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
    ),
    "kalshi_trade_tick_spread_capture": KalshiTradeTickReplay(
        market_ticker="KXPRESNOMR-28-MR",
        start_time="2026-03-24T00:00:00Z",
        end_time="2026-04-08T23:59:59Z",
    ),
}

EXPECTED_POLYMARKET_REPLAYS = {
    "polymarket_trade_tick_ema_crossover": PolymarketTradeTickReplay(
        market_slug="will-ukraine-qualify-for-the-2026-fifa-world-cup",
        start_time="2026-03-20T00:00:00Z",
        end_time="2026-03-26T23:53:59Z",
    ),
    "polymarket_trade_tick_panic_fade": PolymarketTradeTickReplay(
        market_slug="will-newcastle-win-the-202526-champions-league",
        start_time="2026-03-11T00:00:00Z",
        end_time="2026-03-18T22:56:01Z",
    ),
    "polymarket_trade_tick_rsi_reversion": PolymarketTradeTickReplay(
        market_slug="will-man-city-win-the-202526-champions-league",
        start_time="2026-03-11T00:00:00Z",
        end_time="2026-03-18T01:28:17Z",
    ),
    "polymarket_trade_tick_spread_capture": PolymarketTradeTickReplay(
        market_slug="will-chelsea-win-the-202526-champions-league",
        start_time="2026-03-11T00:00:00Z",
        end_time="2026-03-18T01:22:09Z",
    ),
    "polymarket_trade_tick_vwap_reversion": PolymarketTradeTickReplay(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        start_time="2026-02-21T16:00:00Z",
        end_time="2026-03-31T23:59:59Z",
    ),
}

BACKTESTS_ROOT = Path(__file__).resolve().parents[1] / "backtests"
KALSHI_SINGLE_MARKET_RUNNERS = sorted(BACKTESTS_ROOT.glob("kalshi_trade_tick_*.py"))
POLYMARKET_SINGLE_MARKET_RUNNERS = sorted(
    path
    for path in BACKTESTS_ROOT.glob("polymarket_trade_tick_*.py")
    if "sports_" not in path.name
)
POLYMARKET_SPORTS_RUNNERS = sorted(
    BACKTESTS_ROOT.glob("polymarket_trade_tick_sports_*.py")
)


def _import_runner(runner_path: Path):
    return importlib.import_module(f"backtests.{runner_path.stem}")


@pytest.mark.parametrize(
    "runner_path", KALSHI_SINGLE_MARKET_RUNNERS, ids=lambda path: path.stem
)
def test_kalshi_trade_tick_runners_use_expected_runtime_contract(
    runner_path: Path,
) -> None:
    module = _import_runner(runner_path)

    assert module.DATA.sources == EXPECTED_KALSHI_TRADE_SOURCES
    assert module.EMIT_HTML is EXPECTED_EMIT_HTML
    assert module.CHART_OUTPUT_PATH == EXPECTED_CHART_OUTPUT_PATH
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.emit_html is EXPECTED_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.initial_cash == EXPECTED_INITIAL_CASH
    assert module.REPLAYS == (EXPECTED_KALSHI_REPLAYS[module.NAME],)


@pytest.mark.parametrize(
    "runner_path",
    POLYMARKET_SINGLE_MARKET_RUNNERS,
    ids=lambda path: path.stem,
)
def test_polymarket_trade_tick_single_market_runners_use_expected_runtime_contract(
    runner_path: Path,
) -> None:
    module = _import_runner(runner_path)

    assert module.DATA.sources == EXPECTED_POLYMARKET_TRADE_SOURCES
    assert module.EMIT_HTML is EXPECTED_EMIT_HTML
    assert module.CHART_OUTPUT_PATH == EXPECTED_CHART_OUTPUT_PATH
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.emit_html is EXPECTED_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_DETAIL_PLOT_PANELS
    assert module.REPLAYS == (EXPECTED_POLYMARKET_REPLAYS[module.NAME],)


@pytest.mark.parametrize(
    "runner_path", POLYMARKET_SPORTS_RUNNERS, ids=lambda path: path.stem
)
def test_polymarket_trade_tick_sports_runners_use_fixed_replay_windows(
    runner_path: Path,
) -> None:
    module = _import_runner(runner_path)

    assert module.DATA.sources == EXPECTED_POLYMARKET_TRADE_SOURCES
    assert module.EMIT_HTML is EXPECTED_EMIT_HTML
    assert module.CHART_OUTPUT_PATH == EXPECTED_CHART_OUTPUT_PATH
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.EXPERIMENT.emit_html is EXPECTED_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.return_summary_series is True
    assert "output/" in module.SUMMARY_REPORT_PATH
    assert module.SUMMARY_REPORT_PATH.endswith("_multi_market.html")

    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.REPORT.summary_plot_panels == EXPECTED_SUMMARY_PLOT_PANELS

    assert module.FIXED_LOOKBACK_DAYS == EXPECTED_FIXED_SPORTS_LOOKBACK_DAYS
    assert len(module.REPLAYS) >= 2
    for replay in module.REPLAYS:
        assert replay.lookback_days == module.FIXED_LOOKBACK_DAYS
