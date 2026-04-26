# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)


def run() -> None:
    from prediction_market_extensions.backtesting._execution_config import (
        ExecutionModelConfig,
        StaticLatencyConfig,
    )
    from prediction_market_extensions.backtesting._experiments import (
        ParameterSearchExperiment,
        run_experiment,
    )
    from prediction_market_extensions.backtesting._prediction_market_runner import (
        MarketDataConfig,
    )
    from prediction_market_extensions.backtesting._replay_specs import BookReplay
    from prediction_market_extensions.backtesting._timing_harness import timing_harness
    from prediction_market_extensions.backtesting.data_sources import Book, PMXT, Polymarket
    from prediction_market_extensions.backtesting.optimizers import (
        ParameterSearchConfig,
        ParameterSearchWindow,
    )

    @timing_harness
    def _run() -> None:
        run_experiment(
            ParameterSearchExperiment(
                name="polymarket_book_ema_optimizer",
                description=(
                    "Random-search EMA optimizer with explicit train and holdout windows on PMXT L2 data"
                ),
                parameter_search=ParameterSearchConfig(
                    name="polymarket_book_ema_optimizer",
                    data=MarketDataConfig(
                        platform=Polymarket,
                        data_type=Book,
                        vendor=PMXT,
                        sources=(
                            "local:/Volumes/LaCie/pmxt_data",
                            "archive:r2v2.pmxt.dev",
                            "archive:r2.pmxt.dev",
                        ),
                    ),
                    base_replay=BookReplay(
                        market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
                        token_index=0,
                    ),
                    strategy_spec={
                        "strategy_path": "strategies:BookEMACrossoverStrategy",
                        "config_path": "strategies:BookEMACrossoverConfig",
                        "config": {
                            "trade_size": Decimal(5),
                            "fast_period": "__SEARCH__:fast_period",
                            "slow_period": "__SEARCH__:slow_period",
                            "entry_buffer": "__SEARCH__:entry_buffer",
                            "take_profit": "__SEARCH__:take_profit",
                            "stop_loss": "__SEARCH__:stop_loss",
                        },
                    },
                    parameter_grid={
                        "fast_period": (32, 64, 96),
                        "slow_period": (128, 256, 384),
                        "entry_buffer": (0.00025, 0.0005),
                        "take_profit": (0.005, 0.01),
                        "stop_loss": (0.005, 0.01),
                    },
                    train_windows=(
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
                    ),
                    holdout_windows=(
                        ParameterSearchWindow(
                            name="sample-d-close-window",
                            start_time="2026-04-07T00:00:00Z",
                            end_time="2026-04-07T11:59:59Z",
                        ),
                    ),
                    max_trials=18,
                    random_seed=7,
                    holdout_top_k=5,
                    initial_cash=100.0,
                    probability_window=256,
                    min_book_events=500,
                    min_price_range=0.005,
                    min_fills_per_window=1,
                    execution=ExecutionModelConfig(
                        queue_position=True,
                        latency_model=StaticLatencyConfig(
                            base_latency_ms=75.0,
                            insert_latency_ms=10.0,
                            update_latency_ms=5.0,
                            cancel_latency_ms=5.0,
                        ),
                    ),
                ),
            )
        )

    _run()


if __name__ == "__main__":
    run()
