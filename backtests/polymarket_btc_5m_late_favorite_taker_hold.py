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
BTC_5M_WINDOW_COUNT = 24
BTC_5M_WINDOW_SIZE = timedelta(minutes=5)
ACTIVATION_SECONDS_BEFORE_CLOSE = 60
SUMMARY_REPORT_PATH = "output/polymarket_btc_5m_late_favorite_taker_hold_summary.html"


def _utc_iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def _btc_5m_windows() -> tuple[tuple[str, datetime, datetime], ...]:
    return tuple(
        (
            f"btc-updown-5m-{int(start.timestamp())}",
            start,
            start + BTC_5M_WINDOW_SIZE,
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
            start_time=_utc_iso(start_time),
            end_time=_utc_iso(end_time),
            metadata={
                "sim_label": f"{slug}-{'up' if token_index == 0 else 'down'}",
                "activation_start_time_ns": int(
                    (end_time - timedelta(seconds=ACTIVATION_SECONDS_BEFORE_CLOSE)).timestamp()
                    * 1_000_000_000
                ),
                "market_close_time_ns": int(end_time.timestamp() * 1_000_000_000),
            },
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
                name="polymarket_btc_5m_late_favorite_taker_hold",
                description=(
                    "BTC 5m late-window favorite and cheap-NO taker entries using PMXT L2 book data"
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
                        "strategy_path": "strategies:BookLateFavoriteTakerHoldStrategy",
                        "config_path": "strategies:BookLateFavoriteTakerHoldConfig",
                        "config": {
                            "trade_size": Decimal("5"),
                            "activation_start_time_ns": (
                                "__SIM_METADATA__:activation_start_time_ns"
                            ),
                            "market_close_time_ns": "__SIM_METADATA__:market_close_time_ns",
                            "min_midpoint": 0.90,
                            "min_bid_price": 0.88,
                            "max_entry_price": 0.95,
                            "max_spread": 0.04,
                            "min_visible_size": 5.0,
                            "enable_cheap_no_entry": True,
                            "max_cheap_no_entry_price": 0.05,
                            "max_cheap_no_midpoint": 0.10,
                            "max_cheap_no_spread": 0.05,
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
                    summary_report_path=SUMMARY_REPORT_PATH,
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
                empty_message="No BTC 5m late-favorite sims met the book requirements.",
                partial_message="Completed {completed} of {total} BTC 5m late-favorite replays.",
                return_summary_series=True,
            )
        )

    _run()


if __name__ == "__main__":
    run()
