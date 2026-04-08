from __future__ import annotations

import importlib

import pytest

from backtests._shared._strategy_configs import build_strategies_from_configs
from strategies import TradeTickBreakoutConfig
from strategies import TradeTickBreakoutStrategy
from strategies import TradeTickEMACrossoverConfig
from strategies import TradeTickEMACrossoverStrategy
from strategies import TradeTickMeanReversionConfig
from strategies import TradeTickMeanReversionStrategy
from strategies import TradeTickPanicFadeConfig
from strategies import TradeTickPanicFadeStrategy
from strategies import TradeTickRSIReversionConfig
from strategies import TradeTickRSIReversionStrategy
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


INSTRUMENT_ID = InstrumentId(Symbol("KALSHI-TEST"), Venue("KALSHI"))


@pytest.mark.parametrize(
    ("module_name", "strategy_cls", "config_cls"),
    [
        (
            "backtests.kalshi_trade_tick_breakout",
            TradeTickBreakoutStrategy,
            TradeTickBreakoutConfig,
        ),
        (
            "backtests.kalshi_trade_tick_ema_crossover",
            TradeTickEMACrossoverStrategy,
            TradeTickEMACrossoverConfig,
        ),
        (
            "backtests.kalshi_trade_tick_panic_fade",
            TradeTickPanicFadeStrategy,
            TradeTickPanicFadeConfig,
        ),
        (
            "backtests.kalshi_trade_tick_rsi_reversion",
            TradeTickRSIReversionStrategy,
            TradeTickRSIReversionConfig,
        ),
        (
            "backtests.kalshi_trade_tick_spread_capture",
            TradeTickMeanReversionStrategy,
            TradeTickMeanReversionConfig,
        ),
    ],
)
def test_kalshi_backtests_build_expected_trade_tick_strategy(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
    strategy_cls: type,
    config_cls: type,
):
    module = importlib.import_module(module_name)
    captured: dict[str, object] = {}

    def _fake_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return []

    monkeypatch.setattr(module, "run_experiment", _fake_run_experiment)

    module.run()

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS,
        instrument_id=INSTRUMENT_ID,
    )
    assert len(strategies) == 1
    strategy = strategies[0]

    assert isinstance(strategy, strategy_cls)
    assert isinstance(strategy.config, config_cls)
    assert len(module.REPLAYS) == 1
    assert module.REPLAYS[0].market_ticker == "KXNEXTIRANLEADER-45JAN01-MKHA"
    assert module.REPLAYS[0].lookback_days == 30
    assert str(module.REPLAYS[0].end_time) == "2026-03-08T21:44:24Z"
    assert module.EXPERIMENT.name == module.NAME
    assert module.EXPERIMENT.data == module.DATA
    assert module.EXPERIMENT.replays == module.REPLAYS
    assert module.EXPERIMENT.initial_cash == 100.0
    assert module.EXPERIMENT.min_trades == 200
    assert module.EXPERIMENT.min_price_range == 0.03
    assert module.EXPERIMENT.report == module.REPORT
    assert captured["experiment"] is module.EXPERIMENT
