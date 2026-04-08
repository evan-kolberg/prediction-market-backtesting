from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from backtests._shared._optimizer import OptimizationWindow
from backtests._shared._replay_specs import PolymarketPMXTQuoteReplay


EXPECTED_PMXT_SOURCES = (
    "local:/Volumes/LaCie/pmxt_raws",
    "archive:r2.pmxt.dev",
    "relay:209-209-10-83.sslip.io",
)
EXPECTED_PMXT_LATENCY = {
    "base_latency_ms": 75.0,
    "insert_latency_ms": 10.0,
    "update_latency_ms": 5.0,
    "cancel_latency_ms": 5.0,
}
EXPECTED_RUNNER_EMIT_HTML = True
EXPECTED_OPTIMIZER_EMIT_HTML = False
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
SUPPORTED_SUMMARY_PLOT_PANELS = frozenset(EXPECTED_SUMMARY_PLOT_PANELS)

RUNNER_FILES = sorted(
    Path(__file__)
    .resolve()
    .parents[1]
    .joinpath("backtests")
    .glob("polymarket_quote_tick_pmxt_*.py")
)
MULTI_SIM_RUNNER_FILES = sorted(
    path
    for path in RUNNER_FILES
    if "multi_sim_runner" in path.name or path.name.endswith("_sims_runner.py")
)
SINGLE_MARKET_RUNNER_FILES = sorted(
    path
    for path in RUNNER_FILES
    if "optimizer" not in path.name and path not in MULTI_SIM_RUNNER_FILES
)
OPTIMIZER_RUNNER_FILES = sorted(
    path for path in RUNNER_FILES if "optimizer" in path.name
)

EXPECTED_SINGLE_MARKET_REPLAYS = {
    "polymarket_quote_tick_pmxt_breakout": PolymarketPMXTQuoteReplay(
        market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
        token_index=0,
        start_time="2026-04-05T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    "polymarket_quote_tick_pmxt_deep_value_hold": PolymarketPMXTQuoteReplay(
        market_slug="will-the-tennessee-titans-draft-a-quarterback-in-the-first-round-of-the-2026-nfl-draft",
        token_index=0,
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    "polymarket_quote_tick_pmxt_ema_crossover": PolymarketPMXTQuoteReplay(
        market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
        token_index=0,
        start_time="2026-04-05T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    "polymarket_quote_tick_pmxt_final_period_momentum": PolymarketPMXTQuoteReplay(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-03-24T03:00:00Z",
        end_time="2026-03-24T08:00:00Z",
    ),
    "polymarket_quote_tick_pmxt_late_favorite_limit_hold": PolymarketPMXTQuoteReplay(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-03-24T03:00:00Z",
        end_time="2026-03-24T08:00:00Z",
    ),
    "polymarket_quote_tick_pmxt_panic_fade": PolymarketPMXTQuoteReplay(
        market_slug="will-fc-heidenheim-be-relegated-from-the-bundesliga-after-the-202526-season-382",
        token_index=0,
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-07T12:00:00Z",
    ),
    "polymarket_quote_tick_pmxt_rsi_reversion": PolymarketPMXTQuoteReplay(
        market_slug="will-ethan-agarwal-get-the-first-or-second-most-votes-in-the-2026-california-governor-primary-election",
        token_index=0,
        start_time="2026-04-07T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    "polymarket_quote_tick_pmxt_spread_capture": PolymarketPMXTQuoteReplay(
        market_slug="will-drake-release-an-album-in-2026",
        token_index=0,
        start_time="2026-04-05T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    "polymarket_quote_tick_pmxt_threshold_momentum": PolymarketPMXTQuoteReplay(
        market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
        token_index=0,
        start_time="2026-03-24T03:00:00Z",
        end_time="2026-03-24T08:00:00Z",
    ),
    "polymarket_quote_tick_pmxt_vwap_reversion": PolymarketPMXTQuoteReplay(
        market_slug="will-nana-araba-wilmot-win-top-chef-season-23",
        token_index=0,
        start_time="2026-04-06T06:00:00Z",
        end_time="2026-04-07T18:00:00Z",
    ),
}

EXPECTED_MULTI_SIM_REPLAYS = {
    "polymarket_quote_tick_pmxt_multi_sim_runner": (
        PolymarketPMXTQuoteReplay(
            market_slug="will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026",
            token_index=0,
            start_time="2026-03-23T00:00:00Z",
            end_time="2026-03-24T23:59:59Z",
            metadata={"sim_label": "openai-launch-mar-23-24"},
        ),
        PolymarketPMXTQuoteReplay(
            market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
            token_index=0,
            start_time="2026-04-05T00:00:00Z",
            end_time="2026-04-07T23:59:59Z",
            metadata={"sim_label": "aberg-masters-full-window"},
        ),
        PolymarketPMXTQuoteReplay(
            market_slug="will-the-tennessee-titans-draft-a-quarterback-in-the-first-round-of-the-2026-nfl-draft",
            token_index=0,
            start_time="2026-04-06T00:00:00Z",
            end_time="2026-04-07T23:59:59Z",
            metadata={"sim_label": "titans-draft-two-day-window"},
        ),
        PolymarketPMXTQuoteReplay(
            market_slug="will-fc-heidenheim-be-relegated-from-the-bundesliga-after-the-202526-season-382",
            token_index=0,
            start_time="2026-04-07T12:00:00Z",
            end_time="2026-04-07T23:59:59Z",
            metadata={"sim_label": "heidenheim-late-session"},
        ),
        PolymarketPMXTQuoteReplay(
            market_slug="will-the-south-african-reserve-bank-decrease-the-repo-rate-after-the-may-meeting",
            token_index=0,
            start_time="2026-04-06T12:00:00Z",
            end_time="2026-04-07T23:59:59Z",
            metadata={"sim_label": "sarb-rate-watch-window"},
        ),
        PolymarketPMXTQuoteReplay(
            market_slug="will-nana-araba-wilmot-win-top-chef-season-23",
            token_index=0,
            start_time="2026-04-06T06:00:00Z",
            end_time="2026-04-07T18:00:00Z",
            metadata={"sim_label": "top-chef-finale-runup"},
        ),
        PolymarketPMXTQuoteReplay(
            market_slug="will-drake-release-an-album-in-2026",
            token_index=0,
            start_time="2026-04-05T12:00:00Z",
            end_time="2026-04-07T23:59:59Z",
            metadata={"sim_label": "drake-weekend-window"},
        ),
        PolymarketPMXTQuoteReplay(
            market_slug="will-ethan-agarwal-get-the-first-or-second-most-votes-in-the-2026-california-governor-primary-election",
            token_index=0,
            start_time="2026-04-07T00:00:00Z",
            end_time="2026-04-07T23:59:59Z",
            metadata={"sim_label": "agarwal-election-day"},
        ),
    ),
}
EXPECTED_MULTI_SIM_COUNTS = {
    "polymarket_quote_tick_pmxt_multi_sim_runner": 8,
    "polymarket_quote_tick_pmxt_25_sims_runner": 25,
}
EXPECTED_MULTI_SIM_MIN_DISTINCT_MARKETS = {
    "polymarket_quote_tick_pmxt_multi_sim_runner": 8,
    "polymarket_quote_tick_pmxt_25_sims_runner": 8,
}

EXPECTED_OPTIMIZER_BASE_REPLAY = PolymarketPMXTQuoteReplay(
    market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
    token_index=0,
)
EXPECTED_OPTIMIZER_TRAIN_WINDOWS = (
    OptimizationWindow(
        name="sample-a-full-window",
        start_time="2026-04-05T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    OptimizationWindow(
        name="sample-b-2026-04-06-day",
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-06T23:59:59Z",
    ),
    OptimizationWindow(
        name="sample-c-2026-04-07-late",
        start_time="2026-04-07T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
)
EXPECTED_OPTIMIZER_HOLDOUT_WINDOWS = (
    OptimizationWindow(
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


@pytest.mark.parametrize(
    "runner_path", SINGLE_MARKET_RUNNER_FILES, ids=lambda path: path.stem
)
def test_quote_tick_runners_use_expected_runtime_contract(
    runner_path: Path,
) -> None:
    module = _import_runner(runner_path)

    assert module.EMIT_HTML is EXPECTED_RUNNER_EMIT_HTML
    assert module.CHART_OUTPUT_PATH == EXPECTED_CHART_OUTPUT_PATH
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.EXECUTION.queue_position is True
    _assert_latency_model(module.EXECUTION.latency_model)
    assert module.REPLAYS == (EXPECTED_SINGLE_MARKET_REPLAYS[module.NAME],)
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert module.EXPERIMENT.execution == module.EXECUTION
    assert module.EXPERIMENT.emit_html is EXPECTED_RUNNER_EMIT_HTML
    assert module.EXPERIMENT.chart_output_path == EXPECTED_CHART_OUTPUT_PATH
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.EXPERIMENT.detail_plot_panels == EXPECTED_DETAIL_PLOT_PANELS


@pytest.mark.parametrize(
    "runner_path", MULTI_SIM_RUNNER_FILES, ids=lambda path: path.stem
)
def test_pmxt_multi_sim_runners_use_explicit_summary_plot_contract(
    runner_path: Path,
) -> None:
    module = _import_runner(runner_path)

    assert module.NAME == runner_path.stem
    assert module.EMIT_HTML is EXPECTED_RUNNER_EMIT_HTML
    assert module.CHART_OUTPUT_PATH == EXPECTED_CHART_OUTPUT_PATH
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert len(module.REPLAYS) == EXPECTED_MULTI_SIM_COUNTS[module.NAME]

    summary_panels = module.SUMMARY_PLOT_PANELS
    assert isinstance(summary_panels, tuple)
    assert summary_panels
    assert set(summary_panels) <= SUPPORTED_SUMMARY_PLOT_PANELS

    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.REPORT.summary_plot_panels == summary_panels
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert (
        len({replay.market_slug for replay in module.REPLAYS})
        >= (EXPECTED_MULTI_SIM_MIN_DISTINCT_MARKETS[module.NAME])
    )
    assert len(
        {
            (replay.market_slug, replay.start_time, replay.end_time)
            for replay in module.REPLAYS
        }
    ) == len(module.REPLAYS)
    assert len(
        {str((replay.metadata or {}).get("sim_label")) for replay in module.REPLAYS}
    ) == len(module.REPLAYS)

    if module.NAME in EXPECTED_MULTI_SIM_REPLAYS:
        assert module.REPLAYS == EXPECTED_MULTI_SIM_REPLAYS[module.NAME]

    for replay in module.REPLAYS:
        assert replay.market_slug
        assert replay.token_index == 0
        assert isinstance(replay.start_time, str) and replay.start_time
        assert isinstance(replay.end_time, str) and replay.end_time


@pytest.mark.parametrize(
    "runner_path", OPTIMIZER_RUNNER_FILES, ids=lambda path: path.stem
)
def test_quote_tick_optimizer_runners_inline_explicit_search_controls(
    runner_path: Path,
) -> None:
    module = _import_runner(runner_path)

    assert module.EMIT_HTML is EXPECTED_OPTIMIZER_EMIT_HTML
    assert module.CHART_OUTPUT_PATH == EXPECTED_CHART_OUTPUT_PATH
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

    assert module.OPTIMIZATION.base_replay == module.BASE_REPLAY
    assert module.OPTIMIZATION.train_windows == module.TRAIN_WINDOWS
    assert module.OPTIMIZATION.holdout_windows == module.HOLDOUT_WINDOWS
    assert module.OPTIMIZATION.execution == module.EXECUTION
