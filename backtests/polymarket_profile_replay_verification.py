# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-27.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)


PROFILE_USER = "0xe29aff6a6ae1e1d6a3a1c4c904f2957afa98cda0"
PROFILE_TRADE_LIMIT = 500
PROFILE_MAX_GROUPS = 4
PROFILE_LEAD_SECONDS = 1.0

SNAPSHOT_PROFILE_TRADES: tuple[dict[str, object], ...] = (
    {
        "side": "BUY",
        "size": 15.687741,
        "price": 0.619999986359999,
        "timestamp": 1777241444,
        "slug": "btc-updown-5m-1777241400",
        "outcome": "Down",
        "outcomeIndex": 1,
        "title": "Bitcoin Up or Down - April 26, 6:10PM-6:15PM ET",
        "transactionHash": "0xfa98fd1afc6f01599ab5eadf990eaf462f0a79dd74f2b7bce90c591df36c12db",
    },
    {
        "side": "SELL",
        "size": 15.68,
        "price": 0.65,
        "timestamp": 1777241522,
        "slug": "btc-updown-5m-1777241400",
        "outcome": "Down",
        "outcomeIndex": 1,
        "title": "Bitcoin Up or Down - April 26, 6:10PM-6:15PM ET",
        "transactionHash": "0x738a5dcfd2ceb43b7828c7d0ae1765f3c7f1c54a00284de89897f49dcce029f7",
    },
)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_timestamp(*names: str):
    import pandas as pd

    for name in names:
        raw = os.getenv(name)
        if raw is None or not raw.strip():
            continue
        timestamp = pd.Timestamp(raw)
        if pd.isna(timestamp):
            continue
        if timestamp.tzinfo is None:
            return timestamp.tz_localize("UTC")
        return timestamp.tz_convert("UTC")
    return None


def _load_profile_trades():
    from prediction_market_extensions.backtesting.profile_replay import (
        fetch_profile_trades,
        normalize_profile_trades,
    )

    source = os.getenv("POLYMARKET_PROFILE_REPLAY_SOURCE", "live").strip().casefold()
    if source in {"snapshot", "static", "offline"}:
        return normalize_profile_trades(SNAPSHOT_PROFILE_TRADES)

    return fetch_profile_trades(
        user=os.getenv("POLYMARKET_PROFILE_REPLAY_USER", PROFILE_USER),
        limit=_env_int("POLYMARKET_PROFILE_REPLAY_LIMIT", PROFILE_TRADE_LIMIT),
        taker_only=False,
    )


def _initial_cash_for_profile_trades(groups) -> float:  # type: ignore[no-untyped-def]
    buy_notional = Decimal("0")
    for group in groups:
        for trade in group.trades:
            if trade.side == "BUY":
                buy_notional += trade.price * trade.size
    return max(1000.0, float(buy_notional * Decimal("3") + Decimal("100")))


def _build_experiment(groups, *, profile_user: str, lead_seconds: float):  # type: ignore[no-untyped-def]
    from prediction_market_extensions.backtesting._execution_config import (
        ExecutionModelConfig,
        StaticLatencyConfig,
    )
    from prediction_market_extensions.backtesting._experiments import build_replay_experiment
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        MarketReportConfig,
    )
    from prediction_market_extensions.backtesting._prediction_market_runner import (
        MarketDataConfig,
    )
    from prediction_market_extensions.backtesting.data_sources import Book, PMXT, Polymarket
    from prediction_market_extensions.backtesting.profile_replay import (
        build_profile_replays,
        profile_trades_by_key,
    )

    trades_by_key = profile_trades_by_key(groups)
    return build_replay_experiment(
        name="polymarket_profile_replay_verification",
        description=(
            "Formal profile-emulation verification using public Polymarket trades and PMXT L2 books"
        ),
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
        replays=build_profile_replays(
            groups,
            profile_user=profile_user,
            lead_time_seconds=lead_seconds,
        ),
        strategy_configs=[
            {
                "strategy_path": "strategies:BookProfileReplayStrategy",
                "config_path": "strategies:BookProfileReplayConfig",
                "config": {
                    "selection_key": "__SIM_METADATA__:profile_replay_key",
                    "trades_by_key": trades_by_key,
                    "lead_time_seconds": lead_seconds,
                },
            }
        ],
        initial_cash=_initial_cash_for_profile_trades(groups),
        probability_window=30,
        min_book_events=1,
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
            summary_report_path="output/polymarket_profile_replay_verification_summary.html",
            summary_plot_panels=(
                "total_equity",
                "equity",
                "market_pnl",
                "periodic_pnl",
                "yes_price",
                "allocation",
                "drawdown",
                "total_drawdown",
                "cash_equity",
                "total_cash_equity",
            ),
        ),
        empty_message="No profile replay verification windows met the PMXT book requirements.",
        partial_message=("Completed {completed} of {total} profile replay verification windows."),
        return_summary_series=True,
    )


def _simulated_net_quantity(fill_events: object) -> float:
    if not isinstance(fill_events, list | tuple):
        return 0.0
    total = 0.0
    for event in fill_events:
        if not isinstance(event, dict):
            continue
        quantity = float(event.get("quantity") or 0.0)
        action = str(event.get("action") or "").lower()
        total += quantity if action == "buy" else -quantity
    return total


def _print_profile_comparison(results: list[dict[str, Any]], groups) -> None:  # type: ignore[no-untyped-def]
    from prediction_market_extensions.backtesting.profile_replay import (
        append_profile_replay_diagnostics,
    )

    enriched = append_profile_replay_diagnostics(results, groups)
    print("\nProfile replay verification")
    print(
        "market/outcome".ljust(42),
        "trades".rjust(6),
        "fills".rjust(6),
        "target_qty".rjust(11),
        "sim_qty".rjust(11),
        "actual_pnl".rjust(12),
        "bt_pnl".rjust(12),
        "error".rjust(12),
    )
    for result in enriched:
        key = str(result.get("profile_replay_key") or result.get("sim_label") or "")
        actual_pnl = result.get("profile_actual_pnl")
        error = result.get("profile_pnl_error")
        print(
            key[:42].ljust(42),
            str(result.get("profile_trade_count", "")).rjust(6),
            str(result.get("fills", "")).rjust(6),
            f"{float(result.get('profile_buy_quantity') or 0.0):11.4f}",
            f"{_simulated_net_quantity(result.get('fill_events')):11.4f}",
            "n/a".rjust(12) if actual_pnl is None else f"{float(actual_pnl):12.4f}",
            f"{float(result.get('pnl') or 0.0):12.4f}",
            "n/a".rjust(12) if error is None else f"{float(error):12.4f}",
        )


def run() -> None:
    from prediction_market_extensions.backtesting import _experiments
    from prediction_market_extensions.backtesting._timing_harness import timing_harness
    from prediction_market_extensions.backtesting.profile_replay import select_profile_trade_groups

    @timing_harness
    def _run() -> None:
        profile_user = os.getenv("POLYMARKET_PROFILE_REPLAY_USER", PROFILE_USER)
        lead_seconds = _env_float("POLYMARKET_PROFILE_REPLAY_LEAD_SECONDS", PROFILE_LEAD_SECONDS)
        trades = _load_profile_trades()
        groups = select_profile_trade_groups(
            trades,
            max_groups=_env_int("POLYMARKET_PROFILE_REPLAY_MAX_GROUPS", PROFILE_MAX_GROUPS),
            min_latest_trade_time=_env_timestamp(
                "POLYMARKET_PROFILE_REPLAY_MIN_LATEST_TRADE_TIME",
                "POLYMARKET_PROFILE_REPLAY_AFTER",
            ),
            max_latest_trade_time=_env_timestamp(
                "POLYMARKET_PROFILE_REPLAY_MAX_LATEST_TRADE_TIME",
                "POLYMARKET_PROFILE_REPLAY_BEFORE",
            ),
        )
        if not groups:
            print("No complete profile trade groups were selected for replay verification.")
            return

        print(
            f"Selected {len(groups)} profile trade group(s) from {len(trades)} "
            f"public trades for {profile_user}; submitting {lead_seconds:g}s before each trade."
        )
        experiment = _build_experiment(
            groups,
            profile_user=profile_user,
            lead_seconds=lead_seconds,
        )
        results = _experiments.run_experiment(experiment)
        if isinstance(results, list):
            _print_profile_comparison(results, groups)

    _run()


if __name__ == "__main__":
    run()
