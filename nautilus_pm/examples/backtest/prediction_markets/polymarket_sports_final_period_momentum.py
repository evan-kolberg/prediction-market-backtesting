# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Public multi-market example: run a late-breakout sports strategy across many Polymarket markets.

This script defaults to 50 resolved sports markets so people can inspect:
- market discovery,
- token-side selection,
- repeated single-market backtests,
- per-market legacy charts, and
- one legacy multi-market summary chart.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from nautilus_trader.adapters.polymarket.common.market_selection import closed_time_utc
from nautilus_trader.adapters.polymarket.common.market_selection import end_date_utc
from nautilus_trader.adapters.polymarket.research import analyze_market_trade_window
from nautilus_trader.adapters.polymarket.research import discover_live_sports_markets
from nautilus_trader.adapters.polymarket.research import discover_resolved_sports_markets
from nautilus_trader.adapters.polymarket.research import fetch_market_by_slug
from nautilus_trader.adapters.prediction_market.research import print_backtest_summary
from nautilus_trader.adapters.prediction_market.research import save_aggregate_backtest_report
from nautilus_trader.adapters.prediction_market.research import save_combined_backtest_report
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.examples.strategies.prediction_market import (
    TradeTickFinalPeriodMomentumConfig,
)
from nautilus_trader.examples.strategies.prediction_market import (
    TradeTickFinalPeriodMomentumStrategy,
)


try:
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _polymarket_single_market_runner import run_single_market_trade_backtest
    from _script_helpers import parse_bool_env
    from _script_helpers import parse_csv_env
except ModuleNotFoundError:
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _polymarket_single_market_runner import run_single_market_trade_backtest
    from _script_helpers import parse_bool_env
    from _script_helpers import parse_csv_env


NAME = "polymarket_sports_final_period_momentum"
DESCRIPTION = "Public 50-market Polymarket sports example using late-breakout momentum"

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
CANDIDATE_LIMIT = int(os.getenv("CANDIDATE_LIMIT", "100"))
MARKET_LIMIT = int(os.getenv("MARKET_LIMIT", "100"))
TARGET_RESULTS = int(os.getenv("TARGET_RESULTS", "50"))
MARKET_MODE = os.getenv("MARKET_MODE", "resolved").strip().lower() or "resolved"
ACTIVE_WINDOW_HOURS = float(os.getenv("ACTIVE_WINDOW_HOURS", "3"))
DISCOVERY_MAX_HOURS_TO_CLOSE = float(
    os.getenv(
        "DISCOVERY_MAX_HOURS_TO_CLOSE",
        os.getenv("MAX_HOURS_TO_CLOSE", "4320"),
    ),
)
MAX_DAYS_SINCE_CLOSE = float(os.getenv("MAX_DAYS_SINCE_CLOSE", "365"))
DISCOVERY_STEP = int(os.getenv("DISCOVERY_STEP", "250"))
MAX_DISCOVERY_RESULTS = int(os.getenv("MAX_DISCOVERY_RESULTS", "10000"))
DISCOVERY_CANDIDATE_MULTIPLIER = int(os.getenv("DISCOVERY_CANDIDATE_MULTIPLIER", "4"))
DISCOVERY_SCAN_MULTIPLIER = int(os.getenv("DISCOVERY_SCAN_MULTIPLIER", "50"))
GAMES_ONLY = parse_bool_env(os.getenv("GAMES_ONLY", "true"))
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", "0"))
MIN_TRADES = int(os.getenv("MIN_TRADES", "25"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.05"))

FINAL_PERIOD_MINUTES = int(os.getenv("FINAL_PERIOD_MINUTES", "180"))
ENTRY_PRICE = float(os.getenv("ENTRY_PRICE", "0.80"))
TAKE_PROFIT_PRICE = float(os.getenv("TAKE_PROFIT_PRICE", "0.92"))
STOP_LOSS_PRICE = float(os.getenv("STOP_LOSS_PRICE", "0.50"))
SELECTION_ENTRY_PRICE = float(os.getenv("SELECTION_ENTRY_PRICE", str(ENTRY_PRICE)))

TRADE_SIZE = Decimal(os.getenv("TRADE_SIZE", "25"))
INITIAL_CASH = float(os.getenv("INITIAL_CASH", "1000"))
CHART_RESAMPLE_RULE = os.getenv("CHART_RESAMPLE_RULE") or None
EMIT_HTML = parse_bool_env(os.getenv("EMIT_HTML", "true"))
COMBINED_REPORT = parse_bool_env(os.getenv("COMBINED_REPORT", "false"))
COMBINED_REPORT_PATH = os.getenv(
    "COMBINED_REPORT_PATH",
    f"output/{NAME}_combined_legacy.html",
)
SUMMARY_REPORT = parse_bool_env(os.getenv("SUMMARY_REPORT", "true"))
SUMMARY_REPORT_PATH = os.getenv(
    "SUMMARY_REPORT_PATH",
    f"output/{NAME}_multi_market.html",
)
MARKET_SLUGS = parse_csv_env(os.getenv("MARKET_SLUGS", ""))


def _market_window_end(market: dict) -> datetime | None:
    if MARKET_MODE != "resolved":
        return None
    return closed_time_utc(market)


def _market_close_dt(market: dict) -> datetime | None:
    return closed_time_utc(market) or end_date_utc(market)


def _market_close_timestamp(market: dict) -> float:
    close_dt = _market_close_dt(market)
    return close_dt.timestamp() if close_dt is not None else 0.0


def _market_volume(market: dict) -> float:
    return float(market.get("volume24hr") or market.get("volume") or 0.0)


def _sort_markets(markets: list[dict]) -> list[dict]:
    if MARKET_MODE == "resolved":
        markets.sort(
            key=lambda market: (
                -_market_volume(market),
                -_market_close_timestamp(market),
            ),
        )
    else:
        markets.sort(
            key=lambda market: (
                end_date_utc(market) or datetime.max.replace(tzinfo=UTC),
                -_market_volume(market),
            ),
        )
    return markets


async def _select_markets(*, candidate_limit: int) -> list[dict]:
    if MARKET_SLUGS:
        client = nautilus_pyo3.HttpClient(
            default_quota=nautilus_pyo3.Quota.rate_per_second(10),
        )
        selected: list[dict] = []
        for slug in MARKET_SLUGS:
            try:
                market = await fetch_market_by_slug(slug, http_client=client)
            except Exception as exc:
                print(f"Skip {slug}: {exc}")
                continue
            selected.append(market)
        return selected

    max_results = min(
        MAX_DISCOVERY_RESULTS,
        max(candidate_limit * DISCOVERY_SCAN_MULTIPLIER, candidate_limit, 1000),
    )

    if MARKET_MODE == "live":
        markets = await discover_live_sports_markets(
            candidate_limit=candidate_limit,
            max_results=max_results,
            quota_rate_per_second=20,
            min_volume_24h=MIN_VOLUME_24H,
            max_hours_to_close=DISCOVERY_MAX_HOURS_TO_CLOSE,
            games_only=GAMES_ONLY,
        )
    else:
        markets = await discover_resolved_sports_markets(
            candidate_limit=candidate_limit,
            max_results=max_results,
            quota_rate_per_second=20,
            min_volume_24h=MIN_VOLUME_24H,
            max_days_since_close=MAX_DAYS_SINCE_CLOSE,
            games_only=GAMES_ONLY,
        )

    return _sort_markets(markets)


async def run() -> None:
    target_results = min(TARGET_RESULTS, len(MARKET_SLUGS)) if MARKET_SLUGS else TARGET_RESULTS
    candidate_budget = max(CANDIDATE_LIMIT, target_results * DISCOVERY_CANDIDATE_MULTIPLIER)
    if not MARKET_SLUGS:
        candidate_budget = min(candidate_budget, MAX_DISCOVERY_RESULTS)

    seen_slugs: set[str] = set()
    results = []
    attempted = 0

    while len(results) < target_results:
        markets = await _select_markets(candidate_limit=candidate_budget)
        if not markets:
            break

        fresh_markets = []
        for market in markets:
            slug = str(market.get("slug") or market.get("market_slug") or "")
            if not slug or slug in seen_slugs:
                continue
            fresh_markets.append(market)
            seen_slugs.add(slug)

        if not fresh_markets:
            break

        print(
            f"Loaded {len(fresh_markets)} new Polymarket candidates "
            f"(mode={MARKET_MODE}, target_results={target_results}, "
            f"completed={len(results)}, attempted={attempted})."
        )

        for market in fresh_markets:
            if len(results) >= target_results:
                break

            slug = str(market.get("slug") or market.get("market_slug") or "")
            analysis = await analyze_market_trade_window(
                market=market,
                lookback_days=LOOKBACK_DAYS,
                entry_price=SELECTION_ENTRY_PRICE,
                active_window_hours=ACTIVE_WINDOW_HOURS,
            )
            if analysis is None:
                print(f"Skip {slug}: unable to analyze market trade window")
                attempted += 1
                continue

            token_index = int(analysis.get("token_index") or 0)
            token_outcome = str(analysis.get("token_outcome") or "")
            activation_trades = int(analysis.get("activation_trades") or 0)
            first_activation_price = analysis.get("first_activation_price")
            max_activation_price = analysis.get("max_activation_price")
            crossed_entry = bool(analysis.get("crossed_entry"))
            window_end = _market_window_end(analysis)
            market_close_dt = _market_close_dt(analysis)
            attempted += 1

            if market_close_dt is None:
                print(f"Skip {slug}: market close time unavailable")
                continue

            first_text = f"{float(first_activation_price):.2f}" if first_activation_price is not None else "n/a"
            max_text = f"{float(max_activation_price):.2f}" if max_activation_price is not None else "n/a"
            print(
                f"Running {slug} on token_index={token_index} "
                f"outcome={token_outcome or 'n/a'} "
                f"first_active={first_text} max_active={max_text} "
                f"active_trades={activation_trades} crossed_entry={crossed_entry}"
            )

            normalized_close_dt = (
                market_close_dt
                if market_close_dt.tzinfo is not None
                else market_close_dt.replace(tzinfo=UTC)
            )
            market_close_time_ns = int(normalized_close_dt.timestamp() * 1_000_000_000)
            result = await run_single_market_trade_backtest(
                name=NAME,
                market_slug=slug,
                token_index=token_index,
                lookback_days=LOOKBACK_DAYS,
                min_trades=MIN_TRADES,
                min_price_range=MIN_PRICE_RANGE,
                initial_cash=INITIAL_CASH,
                probability_window=max(FINAL_PERIOD_MINUTES, 10),
                chart_resample_rule=CHART_RESAMPLE_RULE,
                emit_summary=False,
                emit_html=EMIT_HTML,
                return_chart_layout=False,
                return_summary_series=SUMMARY_REPORT,
                end_time=window_end,
                strategy_factory=lambda instrument_id, market_close_time_ns=market_close_time_ns: (
                    TradeTickFinalPeriodMomentumStrategy(
                        config=TradeTickFinalPeriodMomentumConfig(
                            instrument_id=instrument_id,
                            trade_size=TRADE_SIZE,
                            market_close_time_ns=market_close_time_ns,
                            final_period_minutes=FINAL_PERIOD_MINUTES,
                            entry_price=ENTRY_PRICE,
                            take_profit_price=TAKE_PROFIT_PRICE,
                            stop_loss_price=STOP_LOSS_PRICE,
                        ),
                    )
                ),
            )
            if result is not None:
                result["outcome"] = token_outcome
                result["activation_trades"] = activation_trades
                result["crossed_entry"] = crossed_entry
                results.append(result)
                print(
                    f"Completed {len(results)}/{target_results}: {slug} "
                    f"pnl={float(result['pnl']):+.4f} fills={int(result['fills'])} "
                    f"attempted={attempted}"
                )

        if MARKET_SLUGS or candidate_budget >= MAX_DISCOVERY_RESULTS:
            break
        candidate_budget = min(candidate_budget + DISCOVERY_STEP, MAX_DISCOVERY_RESULTS)

    if not results:
        print("No Polymarket sports markets had sufficient data for the current filters.")
        return

    if len(results) < target_results:
        print(
            f"Completed {len(results)} Polymarket backtests after attempting {attempted} markets. "
            f"Target was {target_results}."
        )

    print_backtest_summary(
        results=results,
        market_key="slug",
        count_key="trades",
        count_label="Trades",
        pnl_label="PnL (USDC)",
    )

    if COMBINED_REPORT:
        combined_path = save_combined_backtest_report(
            results=results,
            output_path=COMBINED_REPORT_PATH,
            title=f"{NAME} combined legacy chart",
            market_key="slug",
            pnl_label="PnL (USDC)",
        )
        if combined_path is not None:
            print(f"\nCombined legacy chart saved to {combined_path}")

    if SUMMARY_REPORT:
        summary_path = save_aggregate_backtest_report(
            results=results,
            output_path=SUMMARY_REPORT_PATH,
            title=f"{NAME} legacy multi-market chart",
            market_key="slug",
            pnl_label="PnL (USDC)",
        )
        if summary_path is not None:
            print(f"\nLegacy multi-market chart saved to {summary_path}")


if __name__ == "__main__":
    asyncio.run(run())
