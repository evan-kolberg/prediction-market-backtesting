# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-26.
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
                name="polymarket_book_joint_portfolio_runner",
                description="Joint-portfolio PMXT book backtest using varied historical replays",
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
                        market_slug="human-moon-landing-in-2026",
                        token_index=0,
                        start_time="2026-03-01T00:00:00Z",
                        end_time="2026-04-11T23:59:59Z",
                        metadata={"sim_label": "moon-landing-2026"},
                    ),
                    BookReplay(
                        market_slug="new-coronavirus-pandemic-in-2026",
                        token_index=0,
                        start_time="2026-03-01T00:00:00Z",
                        end_time="2026-04-11T23:59:59Z",
                        metadata={"sim_label": "coronavirus-pandemic-2026"},
                    ),
                    BookReplay(
                        market_slug=(
                            "will-openais-market-cap-be-between-750b-and-1t-at-market-close-on-ipo-day"
                        ),
                        token_index=0,
                        start_time="2026-03-01T00:00:00Z",
                        end_time="2026-04-11T23:59:59Z",
                        metadata={"sim_label": "openai-ipo-market-cap-750b-1t"},
                    ),
                    BookReplay(
                        market_slug="okx-ipo-in-2026",
                        token_index=0,
                        start_time="2026-03-01T00:00:00Z",
                        end_time="2026-04-11T23:59:59Z",
                        metadata={"sim_label": "okx-ipo-2026"},
                    ),
                    BookReplay(
                        market_slug="nothing-ever-happens-2026",
                        token_index=0,
                        start_time="2026-03-01T00:00:00Z",
                        end_time="2026-04-11T23:59:59Z",
                        metadata={"sim_label": "nothing-ever-happens-2026"},
                    ),
                ),
                strategy_configs=[
                    {
                        "strategy_path": "strategies:BookMicropriceImbalanceStrategy",
                        "config_path": "strategies:BookMicropriceImbalanceConfig",
                        "config": {
                            "trade_size": Decimal(5),
                            "depth_levels": 3,
                            "entry_imbalance": 0.62,
                            "exit_imbalance": 0.48,
                            "min_microprice_edge": 0.0015,
                            "max_spread": 0.04,
                            "max_entry_price": 0.20,
                            "max_expected_slippage": 0.01,
                            "min_holding_updates": 0,
                            "reentry_cooldown_updates": 0,
                            "min_holding_seconds": 900.0,
                            "reentry_cooldown_seconds": 1_800.0,
                            "take_profit": 0.02,
                            "stop_loss": 0.025,
                        },
                    }
                ],
                initial_cash=100.0,
                probability_window=30,
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
                    market_key="sim_label",
                    summary_report=True,
                    summary_report_path=(
                        "output/polymarket_book_joint_portfolio_runner_joint_portfolio.html"
                    ),
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
                empty_message="No PMXT joint-portfolio example windows met the book requirements.",
                partial_message="Completed {completed} of {total} joint-portfolio example replays.",
                return_summary_series=True,
            )
        )

    _run()


if __name__ == "__main__":
    run()
