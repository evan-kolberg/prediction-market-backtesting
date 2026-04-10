from __future__ import annotations

import importlib

import pytest

from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting._strategy_configs import build_strategies_from_configs
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from strategies import QuoteTickEMACrossoverConfig
from strategies import QuoteTickEMACrossoverStrategy
from strategies import QuoteTickVWAPReversionConfig
from strategies import QuoteTickVWAPReversionStrategy


INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))
EXPECTED_PMXT_SOURCES = (
    "local:/Volumes/LaCie/pmxt_raws",
    "archive:r2.pmxt.dev",
    "relay:209-209-10-83.sslip.io",
)
EXPECTED_DETAIL_PLOT_PANELS = (
    "total_equity",
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
EXPECTED_MULTI_SIM_SUMMARY_PLOT_PANELS = (
    "total_equity",
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
EXPECTED_25_SIM_SUMMARY_PLOT_PANELS = (
    "total_equity",
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
EXPECTED_SINGLE_REPLAY = QuoteReplay(
    market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
    token_index=0,
    start_time="2026-04-05T00:00:00Z",
    end_time="2026-04-07T23:59:59Z",
)


def test_pmxt_single_runner_builds_expected_quote_tick_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("backtests.polymarket_quote_tick_ema_crossover")
    captured: dict[str, object] = {}

    def _fake_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return []

    monkeypatch.setattr(module, "run_experiment", _fake_run_experiment)

    module.run()

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS, instrument_id=INSTRUMENT_ID
    )
    assert len(strategies) == 1
    strategy = strategies[0]

    assert isinstance(strategy, QuoteTickEMACrossoverStrategy)
    assert isinstance(strategy.config, QuoteTickEMACrossoverConfig)
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert module.REPLAYS == (EXPECTED_SINGLE_REPLAY,)
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert module.EXPERIMENT.detail_plot_panels == module.DETAIL_PLOT_PANELS
    assert captured["experiment"] is module.EXPERIMENT


def test_pmxt_independent_multi_runner_uses_fixed_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module(
        "backtests.polymarket_quote_tick_independent_multi_replay_runner"
    )
    captured: dict[str, object] = {}

    def _fake_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return []

    monkeypatch.setattr(module, "run_experiment", _fake_run_experiment)

    module.run()

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS, instrument_id=INSTRUMENT_ID
    )
    assert len(strategies) == 1
    strategy = strategies[0]

    assert isinstance(strategy, QuoteTickVWAPReversionStrategy)
    assert isinstance(strategy.config, QuoteTickVWAPReversionConfig)
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert len(module.REPLAYS) == 8
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_MULTI_SIM_SUMMARY_PLOT_PANELS
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.EXPERIMENT.report == module.REPORT
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "independent"
    assert captured["experiment"] is module.EXPERIMENT


def test_pmxt_joint_multi_runner_uses_fixed_windows(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("backtests.polymarket_quote_tick_joint_portfolio_runner")
    captured: dict[str, object] = {}

    def _fake_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return []

    monkeypatch.setattr(module, "run_experiment", _fake_run_experiment)

    module.run()

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS, instrument_id=INSTRUMENT_ID
    )
    assert len(strategies) == 1
    strategy = strategies[0]

    assert isinstance(strategy, QuoteTickVWAPReversionStrategy)
    assert isinstance(strategy.config, QuoteTickVWAPReversionConfig)
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert len(module.REPLAYS) == 8
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_MULTI_SIM_SUMMARY_PLOT_PANELS
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.EXPERIMENT.report == module.REPORT
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "joint_portfolio"
    assert captured["experiment"] is module.EXPERIMENT


def test_pmxt_independent_25_replay_runner_uses_fixed_windows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("backtests.polymarket_quote_tick_independent_25_replay_runner")
    captured: dict[str, object] = {}

    def _fake_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return []

    monkeypatch.setattr(module, "run_experiment", _fake_run_experiment)

    module.run()

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS, instrument_id=INSTRUMENT_ID
    )
    assert len(strategies) == 1
    strategy = strategies[0]

    assert isinstance(strategy, QuoteTickVWAPReversionStrategy)
    assert isinstance(strategy.config, QuoteTickVWAPReversionConfig)
    assert module.DATA.sources == EXPECTED_PMXT_SOURCES
    assert len(module.REPLAYS) == 25
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_25_SIM_SUMMARY_PLOT_PANELS
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.EXPERIMENT.report == module.REPORT
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "independent"
    assert captured["experiment"] is module.EXPERIMENT
