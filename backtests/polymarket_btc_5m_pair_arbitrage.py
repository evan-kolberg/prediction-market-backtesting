# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Added in this repository on 2026-04-27.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)


BTC_5M_WINDOW_START = datetime(2026, 4, 26, 18, 0, tzinfo=UTC)
BTC_5M_WINDOW_COUNT = 4
BTC_5M_WINDOW_SIZE = timedelta(minutes=5)


def _utc_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _btc_5m_windows() -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (
            f"btc-updown-5m-{int(start.timestamp())}",
            _utc_iso(start),
            _utc_iso(start + BTC_5M_WINDOW_SIZE),
        )
        for index in range(BTC_5M_WINDOW_COUNT)
        for start in (BTC_5M_WINDOW_START + (index * BTC_5M_WINDOW_SIZE),)
    )


def _btc_5m_replays():  # type: ignore[no-untyped-def]
    from prediction_market_extensions.backtesting._replay_specs import BookReplay

    return tuple(
        BookReplay(
            market_slug=slug,
            token_index=token_index,
            start_time=start_time,
            end_time=end_time,
            metadata={"sim_label": f"{slug}-{'up' if token_index == 0 else 'down'}"},
        )
        for slug, start_time, end_time in _btc_5m_windows()
        for token_index in (0, 1)
    )


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
    from prediction_market_extensions.backtesting._timing_harness import timing_harness
    from prediction_market_extensions.backtesting.data_sources import Book, PMXT, Polymarket

    load_dotenv()

    @timing_harness
    def _run() -> None:
        run_experiment(
            build_replay_experiment(
                name="polymarket_btc_5m_pair_arbitrage",
                description=(
                    "BTC 5m gross complementary-token entries using only PMXT L2 book data"
                ),
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
                replays=_btc_5m_replays(),
                strategy_configs=[
                    {
                        "strategy_path": "strategies:BookBinaryPairArbitrageStrategy",
                        "config_path": "strategies:BookBinaryPairArbitrageConfig",
                        "config": {
                            "instrument_ids": "__ALL_SIM_INSTRUMENT_IDS__",
                            "trade_size": Decimal("5"),
                            "min_net_edge": 0.0,
                            "max_total_cost": 1.0,
                            "max_leg_price": 0.985,
                            "max_spread": 0.080,
                            "max_expected_slippage": 0.015,
                            "min_visible_size": 5.0,
                            "max_entries_per_pair": 1,
                            "reentry_cooldown_updates": 25,
                            "pairing_mode": "sequential",
                            "hold_to_resolution": True,
                            "include_taker_fees_in_signal": False,
                        },
                    }
                ],
                initial_cash=1_000.0,
                probability_window=256,
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
                    summary_report_path=("output/polymarket_btc_5m_pair_arbitrage_summary.html"),
                    summary_plot_panels=(
                        "total_equity",
                        "equity",
                        "market_pnl",
                        "periodic_pnl",
                        "yes_price",
                        "allocation",
                        "total_drawdown",
                        "drawdown",
                        "total_cash_equity",
                        "cash_equity",
                        "total_brier_advantage",
                        "brier_advantage",
                    ),
                ),
                empty_message="No BTC 5m pair-arbitrage sims met the book requirements.",
                return_summary_series=True,
            )
        )

    _run()


if __name__ == "__main__":
    run()
