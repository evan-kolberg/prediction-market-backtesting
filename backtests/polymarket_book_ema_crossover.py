# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, 2026-03-16, 2026-03-31, and 2026-04-05.
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
        build_replay_experiment,
        run_experiment,
    )
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        MarketReportConfig,
    )
    from prediction_market_extensions.backtesting._prediction_market_runner import (
        MarketDataConfig,
    )
    from prediction_market_extensions.backtesting._replay_specs import BookReplay
    from prediction_market_extensions.backtesting._timing_harness import timing_harness
    from prediction_market_extensions.backtesting.data_sources import Book, PMXT, Polymarket

    @timing_harness
    def _run() -> None:
        run_experiment(
            build_replay_experiment(
                name="polymarket_book_ema_crossover",
                description="EMA crossover momentum on a single Polymarket market using PMXT L2 data",
                data=MarketDataConfig(
                    platform=Polymarket,
                    data_type=Book,
                    vendor=PMXT,
                    sources=(
                        "local:/Volumes/storage/pmxt_data",
                        "archive:r2v2.pmxt.dev",
                        "archive:r2.pmxt.dev",
                    ),
                ),
                replays=(
                    BookReplay(
                        market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
                        token_index=0,
                        start_time="2026-04-05T00:00:00Z",
                        end_time="2026-04-07T23:59:59Z",
                    ),
                ),
                strategy_configs=[
                    {
                        "strategy_path": "strategies:BookEMACrossoverStrategy",
                        "config_path": "strategies:BookEMACrossoverConfig",
                        "config": {
                            "trade_size": Decimal(5),
                            "fast_period": 64,
                            "slow_period": 256,
                            "entry_buffer": 0.0005,
                            "take_profit": 0.01,
                            "stop_loss": 0.01,
                        },
                    }
                ],
                initial_cash=100.0,
                probability_window=256,
                min_book_events=500,
                min_price_range=0.005,
                execution=ExecutionModelConfig(
                    queue_position=True,
                    latency_model=StaticLatencyConfig(
                        base_latency_ms=75.0,
                        insert_latency_ms=10.0,
                        update_latency_ms=5.0,
                        cancel_latency_ms=5.0,
                    ),
                ),
                report=MarketReportConfig(
                    count_key="book_events",
                    count_label="Book Events",
                    pnl_label="PnL (USDC)",
                    summary_report=True,
                    summary_report_path="output/polymarket_book_ema_crossover_summary.html",
                    summary_plot_panels=(
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
                    ),
                ),
                empty_message="No PMXT EMA crossover sims met the book requirements.",
            )
        )

    _run()


if __name__ == "__main__":
    run()
