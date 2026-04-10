from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from prediction_market_extensions.backtesting._prediction_market_backtest import MarketSimConfig
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import _LoadedMarketSim
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._strategy_configs import build_strategies_from_configs
from strategies import QuoteTickBreakoutConfig
from strategies import QuoteTickBreakoutStrategy
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


def test_strategy_configs_bind_primary_instrument_id() -> None:
    instrument_id = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))
    strategies = build_strategies_from_configs(
        strategy_configs=[
            {
                "strategy_path": "strategies:QuoteTickBreakoutStrategy",
                "config_path": "strategies:QuoteTickBreakoutConfig",
                "config": {
                    "trade_size": Decimal("100"),
                    "window": 20,
                    "breakout_std": 1.5,
                    "breakout_buffer": 0.001,
                    "mean_reversion_buffer": 0.0005,
                    "min_holding_periods": 5,
                    "reentry_cooldown": 10,
                    "max_entry_price": 0.9,
                    "take_profit": 0.01,
                    "stop_loss": 0.01,
                },
            }
        ],
        instrument_id=instrument_id,
    )

    assert len(strategies) == 1
    strategy = strategies[0]
    assert isinstance(strategy, QuoteTickBreakoutStrategy)
    assert isinstance(strategy.config, QuoteTickBreakoutConfig)
    assert strategy.config.instrument_id == instrument_id


def test_prediction_market_backtest_binds_strategy_configs_across_sims() -> None:
    instrument_id_one = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))
    instrument_id_two = InstrumentId(Symbol("PM-TEST-NO"), Venue("POLYMARKET"))
    backtest = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(platform="polymarket", data_type="trade_tick", vendor="native"),
        sims=(
            MarketSimConfig(
                market_slug="market-one",
                metadata={"market_close_time_ns": 111, "activation_start_time_ns": 11},
            ),
            MarketSimConfig(
                market_slug="market-two",
                metadata={"market_close_time_ns": 222, "activation_start_time_ns": 22},
            ),
        ),
        strategy_configs=[
            {
                "strategy_path": "strategies:TradeTickLateFavoriteLimitHoldStrategy",
                "config_path": "strategies:TradeTickLateFavoriteLimitHoldConfig",
                "config": {
                    "trade_size": Decimal("25"),
                    "activation_start_time_ns": "__SIM_METADATA__:activation_start_time_ns",
                    "market_close_time_ns": "__SIM_METADATA__:market_close_time_ns",
                    "entry_price": 0.9,
                },
            },
            {
                "strategy_path": "strategies:PortfolioProbeStrategy",
                "config_path": "strategies:PortfolioProbeConfig",
                "config": {"instrument_ids": "__ALL_SIM_INSTRUMENT_IDS__"},
            },
        ],
        initial_cash=100.0,
        probability_window=10,
    )
    loaded_sims = [
        _LoadedMarketSim(
            spec=backtest.sims[0],
            instrument=SimpleNamespace(id=instrument_id_one, outcome="Yes"),
            records=[],
            count=0,
            count_key="trades",
            market_key="slug",
            market_id="market-one",
            outcome="Yes",
            realized_outcome=1.0,
            prices=[],
            metadata={"market_close_time_ns": 111, "activation_start_time_ns": 11},
            requested_start_ns=None,
            requested_end_ns=None,
        ),
        _LoadedMarketSim(
            spec=backtest.sims[1],
            instrument=SimpleNamespace(id=instrument_id_two, outcome="Yes"),
            records=[],
            count=0,
            count_key="trades",
            market_key="slug",
            market_id="market-two",
            outcome="Yes",
            realized_outcome=1.0,
            prices=[],
            metadata={"market_close_time_ns": 222, "activation_start_time_ns": 22},
            requested_start_ns=None,
            requested_end_ns=None,
        ),
    ]

    importable_configs = backtest._build_importable_strategy_configs(loaded_sims)  # noqa: SLF001

    assert len(importable_configs) == 3
    assert importable_configs[0].config["market_close_time_ns"] == 111
    assert importable_configs[0].config["activation_start_time_ns"] == 11
    assert importable_configs[1].config["market_close_time_ns"] == 222
    assert importable_configs[1].config["activation_start_time_ns"] == 22
    assert importable_configs[2].config["instrument_ids"] == [instrument_id_one, instrument_id_two]
