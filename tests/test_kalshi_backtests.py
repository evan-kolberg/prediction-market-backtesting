from __future__ import annotations

import importlib

import pytest

from prediction_market_extensions.backtesting._replay_specs import TradeReplay
from prediction_market_extensions.backtesting._strategy_configs import build_strategies_from_configs
from strategies import TradeTickBreakoutConfig
from strategies import TradeTickBreakoutStrategy
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


INSTRUMENT_ID = InstrumentId(Symbol("KALSHI-TEST"), Venue("KALSHI"))

EXPECTED_SINGLE_REPLAY = TradeReplay(
    market_ticker="KXLAYOFFSYINFO-26-494000",
    start_time="2026-03-15T00:00:00Z",
    end_time="2026-04-08T23:59:59Z",
)
EXPECTED_MULTI_REPLAYS = (
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


def test_kalshi_single_runner_builds_expected_trade_tick_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = importlib.import_module("backtests.kalshi_trade_tick_breakout")
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

    assert isinstance(strategy, TradeTickBreakoutStrategy)
    assert isinstance(strategy.config, TradeTickBreakoutConfig)
    assert module.REPLAYS == (EXPECTED_SINGLE_REPLAY,)
    assert module.EXPERIMENT.name == "kalshi_trade_tick_breakout"
    assert module.EXPERIMENT.data == module.DATA
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert module.EXPERIMENT.initial_cash == 100.0
    assert module.EXPERIMENT.min_trades == 200
    assert module.EXPERIMENT.min_price_range == 0.03
    assert module.EXPERIMENT.report == module.REPORT
    assert captured["experiment"] is module.EXPERIMENT


def test_kalshi_independent_runner_uses_fixed_replays(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("backtests.kalshi_trade_tick_independent_multi_replay_runner")
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

    assert isinstance(strategy, TradeTickBreakoutStrategy)
    assert isinstance(strategy.config, TradeTickBreakoutConfig)
    assert module.REPLAYS == EXPECTED_MULTI_REPLAYS
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "independent"
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.EXPERIMENT.report == module.REPORT
    assert module.EXPERIMENT.empty_message == module.EMPTY_MESSAGE
    assert module.EXPERIMENT.partial_message == module.PARTIAL_MESSAGE
    assert captured["experiment"] is module.EXPERIMENT


def test_kalshi_joint_runner_uses_fixed_replays(monkeypatch: pytest.MonkeyPatch) -> None:
    module = importlib.import_module("backtests.kalshi_trade_tick_joint_portfolio_runner")
    captured: dict[str, object] = {}

    def _fake_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return []

    monkeypatch.setattr(module, "run_experiment", _fake_run_experiment)

    module.run()

    assert module.REPLAYS == EXPECTED_MULTI_REPLAYS
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "joint_portfolio"
    assert module.REPORT.summary_report is True
    assert captured["experiment"] is module.EXPERIMENT
