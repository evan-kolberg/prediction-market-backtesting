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
    from dotenv import load_dotenv

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
    from prediction_market_extensions.backtesting.data_sources import Book, Polymarket, Telonex

    load_dotenv()

    @timing_harness
    def _run() -> None:
        run_experiment(
            build_replay_experiment(
                name="polymarket_telonex_book_joint_portfolio_runner",
                description="Joint-portfolio Telonex book backtest over regular multi-day markets",
                data=MarketDataConfig(
                    platform=Polymarket,
                    data_type=Book,
                    vendor=Telonex,
                    sources=(
                        "local:/Users/evankolberg/Downloads/temp",
                        "api:${TELONEX_API_KEY}",
                    ),
                ),
                replays=(
                    BookReplay(
                        market_slug="will-the-iranian-regime-fall-by-may-31",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "will-the-iranian-regime-fall-by-may-31",
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="us-x-iran-permanent-peace-deal-by-may-15-2026",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "us-x-iran-permanent-peace-deal-by-may-15-2026",
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="us-x-iran-permanent-peace-deal-by-may-31-2026-333-871",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": ("us-x-iran-permanent-peace-deal-by-may-31-2026-333-871"),
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="strait-of-hormuz-traffic-returns-to-normal-by-may-15",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "strait-of-hormuz-traffic-returns-to-normal-by-may-15",
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="russia-x-ukraine-ceasefire-by-may-31-2026",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "russia-x-ukraine-ceasefire-by-may-31-2026",
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="will-judy-shelton-be-confirmed-as-fed-chair",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "will-judy-shelton-be-confirmed-as-fed-chair",
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="trump-out-as-president-by-june-30",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "trump-out-as-president-by-june-30",
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="kharg-island-no-longer-under-iranian-control-by-may-31-689",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": (
                                "kharg-island-no-longer-under-iranian-control-by-may-31-689"
                            ),
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="will-the-us-invade-iran-before-2027",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "will-the-us-invade-iran-before-2027",
                            "market_close_time_ns": 1777593599000000000,
                        },
                    ),
                    BookReplay(
                        market_slug="will-china-invade-taiwan-before-2027",
                        token_index=0,
                        start_time="2026-04-28T00:00:00Z",
                        end_time="2026-04-30T23:59:59Z",
                        metadata={
                            "sim_label": "will-china-invade-taiwan-before-2027",
                            "market_close_time_ns": 1777593599000000000,
                        },
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
                            "max_spread": 0.08,
                            "max_entry_price": 0.95,
                            "max_expected_slippage": 0.02,
                            "min_holding_updates": 0,
                            "reentry_cooldown_updates": 0,
                            "min_holding_seconds": 30.0,
                            "reentry_cooldown_seconds": 60.0,
                            "take_profit": 0.02,
                            "stop_loss": 0.025,
                        },
                    }
                ],
                initial_cash=100.0,
                probability_window=30,
                min_book_events=25,
                min_price_range=0.0,
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
                        "output/polymarket_telonex_book_joint_portfolio_runner_joint_portfolio.html"
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
                empty_message="No Telonex joint-portfolio example windows met the book requirements.",
                partial_message=(
                    "Completed {completed} of {total} joint-portfolio Telonex example replays."
                ),
                return_summary_series=True,
            )
        )

    _run()


if __name__ == "__main__":
    run()
