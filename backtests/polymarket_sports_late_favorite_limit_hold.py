# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Run a resolved 50-market Polymarket sports basket using a late-favorite limit hold.

The strategy submits one passive buy after a late-game probability breakout and
then marks any remaining position to the resolved 0/1 settlement outcome.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

from nautilus_trader.adapters.polymarket.common.market_selection import closed_time_utc
from nautilus_trader.adapters.polymarket.common.market_selection import end_date_utc
from nautilus_trader.adapters.polymarket.research import analyze_market_trade_window
from nautilus_trader.adapters.polymarket.research import (
    discover_resolved_sports_markets,
)
from nautilus_trader.adapters.polymarket.research import fetch_market_by_slug
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    compute_binary_settlement_pnl,
)
from nautilus_trader.adapters.prediction_market.research import print_backtest_summary
from nautilus_trader.core import nautilus_pyo3
from strategies import (
    TradeTickLateFavoriteLimitHoldConfig,
)
from strategies import (
    TradeTickLateFavoriteLimitHoldStrategy,
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


NAME = "polymarket_sports_late_favorite_limit_hold"
DESCRIPTION = (
    "Resolved 50-market Polymarket sports basket using late-favorite limit holds"
)

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
CANDIDATE_LIMIT = int(os.getenv("CANDIDATE_LIMIT", "200"))
TARGET_RESULTS = int(os.getenv("TARGET_RESULTS", "50"))
DISCOVERY_STEP = int(os.getenv("DISCOVERY_STEP", "250"))
MAX_DISCOVERY_RESULTS = int(os.getenv("MAX_DISCOVERY_RESULTS", "4000"))
DISCOVERY_CANDIDATE_MULTIPLIER = int(
    os.getenv("DISCOVERY_CANDIDATE_MULTIPLIER", "6"),
)
MAX_DAYS_SINCE_CLOSE = float(os.getenv("MAX_DAYS_SINCE_CLOSE", "365"))
MIN_VOLUME_24H = float(os.getenv("MIN_VOLUME_24H", "0"))
GAMES_ONLY = parse_bool_env(os.getenv("GAMES_ONLY", "true"))

MIN_TRADES = int(os.getenv("MIN_TRADES", "25"))
MIN_PRICE_RANGE = float(os.getenv("MIN_PRICE_RANGE", "0.05"))
ACTIVE_WINDOW_HOURS = float(os.getenv("ACTIVE_WINDOW_HOURS", "3"))
MIN_ACTIVATION_TRADES = int(os.getenv("MIN_ACTIVATION_TRADES", "50"))
FIRST_ACTIVE_PRICE_MAX = float(os.getenv("FIRST_ACTIVE_PRICE_MAX", "0.95"))
ENTRY_PRICE = float(os.getenv("ENTRY_PRICE", "0.90"))

TRADE_SIZE = Decimal(os.getenv("TRADE_SIZE", "25"))
INITIAL_CASH = float(os.getenv("INITIAL_CASH", "1000"))
EMIT_HTML = parse_bool_env(os.getenv("EMIT_HTML", "false"))
MARKET_SLUGS = parse_csv_env(os.getenv("MARKET_SLUGS", ""))


def _market_close_dt(market: dict) -> datetime | None:
    return closed_time_utc(market) or end_date_utc(market)


def _market_close_timestamp(market: dict) -> float:
    close_dt = _market_close_dt(market)
    return close_dt.timestamp() if close_dt is not None else 0.0


def _market_volume(market: dict) -> float:
    return float(
        market.get("volume24hr")
        or market.get("volume")
        or market.get("event_total_volume")
        or 0.0
    )


def _sort_markets(markets: list[dict]) -> list[dict]:
    markets.sort(
        key=lambda market: (
            -_market_volume(market),
            -_market_close_timestamp(market),
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

    markets = await discover_resolved_sports_markets(
        candidate_limit=candidate_limit,
        max_results=min(MAX_DISCOVERY_RESULTS, max(candidate_limit * 8, 1000)),
        quota_rate_per_second=20,
        min_volume_24h=MIN_VOLUME_24H,
        max_days_since_close=MAX_DAYS_SINCE_CLOSE,
        games_only=GAMES_ONLY,
    )
    return _sort_markets(markets)


async def run() -> None:
    target_results = (
        min(TARGET_RESULTS, len(MARKET_SLUGS)) if MARKET_SLUGS else TARGET_RESULTS
    )
    candidate_budget = max(
        CANDIDATE_LIMIT,
        target_results * DISCOVERY_CANDIDATE_MULTIPLIER,
    )
    candidate_budget = min(candidate_budget, MAX_DISCOVERY_RESULTS)

    seen_slugs: set[str] = set()
    results: list[dict] = []
    attempted = 0
    qualified = 0

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
            f"(target_results={target_results}, completed={len(results)}, attempted={attempted})."
        )

        for market in fresh_markets:
            if len(results) >= target_results:
                break

            slug = str(market.get("slug") or market.get("market_slug") or "")
            analysis = await analyze_market_trade_window(
                market=market,
                lookback_days=LOOKBACK_DAYS,
                entry_price=ENTRY_PRICE,
                active_window_hours=ACTIVE_WINDOW_HOURS,
            )
            attempted += 1
            if analysis is None:
                print(f"Skip {slug}: unable to analyze market trade window")
                continue

            activation_trades = int(analysis.get("activation_trades") or 0)
            first_activation_price = analysis.get("first_activation_price")
            max_activation_price = analysis.get("max_activation_price")
            if activation_trades < MIN_ACTIVATION_TRADES:
                continue
            if (
                first_activation_price is None
                or float(first_activation_price) > FIRST_ACTIVE_PRICE_MAX
            ):
                continue
            if (
                max_activation_price is None
                or float(max_activation_price) < ENTRY_PRICE
            ):
                continue

            market_close_dt = _market_close_dt(analysis)
            if market_close_dt is None:
                print(f"Skip {slug}: market close time unavailable")
                continue

            qualified += 1
            token_index = int(analysis.get("token_index") or 0)
            token_outcome = str(analysis.get("token_outcome") or "")
            activation_start = analysis.get("activation_start")
            activation_start_ns = (
                int(pd.Timestamp(activation_start).value) if activation_start else 0
            )
            normalized_close_dt = (
                market_close_dt
                if market_close_dt.tzinfo is not None
                else market_close_dt.replace(tzinfo=UTC)
            )
            market_close_time_ns = int(normalized_close_dt.timestamp() * 1_000_000_000)

            print(
                f"Running {slug} on token_index={token_index} "
                f"outcome={token_outcome or 'n/a'} "
                f"first_active={float(first_activation_price):.3f} "
                f"max_active={float(max_activation_price):.3f} "
                f"active_trades={activation_trades}"
            )

            result = await run_single_market_trade_backtest(
                name=NAME,
                market_slug=slug,
                token_index=token_index,
                lookback_days=LOOKBACK_DAYS,
                min_trades=MIN_TRADES,
                min_price_range=MIN_PRICE_RANGE,
                initial_cash=INITIAL_CASH,
                probability_window=max(int(ACTIVE_WINDOW_HOURS * 60), 10),
                emit_summary=False,
                emit_html=EMIT_HTML,
                return_chart_layout=False,
                return_summary_series=True,
                end_time=market_close_dt,
                strategy_factory=lambda instrument_id, activation_start_time_ns=activation_start_ns, market_close_time_ns=market_close_time_ns: (
                    TradeTickLateFavoriteLimitHoldStrategy(
                        config=TradeTickLateFavoriteLimitHoldConfig(
                            instrument_id=instrument_id,
                            trade_size=TRADE_SIZE,
                            activation_start_time_ns=activation_start_time_ns,
                            market_close_time_ns=market_close_time_ns,
                            entry_price=ENTRY_PRICE,
                        ),
                    )
                ),
            )
            if result is None:
                continue

            settlement_pnl = compute_binary_settlement_pnl(
                result.get("fill_events", []),
                result.get("realized_outcome"),
            )
            if settlement_pnl is None:
                print(f"Skip {slug}: resolved outcome unavailable")
                continue

            result["market_exit_pnl"] = float(result["pnl"])
            result["pnl"] = float(settlement_pnl)
            result["outcome"] = token_outcome
            result["activation_trades"] = activation_trades
            result["first_activation_price"] = float(first_activation_price)
            result["max_activation_price"] = float(max_activation_price)
            results.append(result)
            print(
                f"Completed {len(results)}/{target_results}: {slug} "
                f"settlement_pnl={float(result['pnl']):+.4f} "
                f"market_exit_pnl={float(result['market_exit_pnl']):+.4f} "
                f"fills={int(result['fills'])} "
                f"qualified={qualified} attempted={attempted}"
            )

        if MARKET_SLUGS or candidate_budget >= MAX_DISCOVERY_RESULTS:
            break
        candidate_budget = min(candidate_budget + DISCOVERY_STEP, MAX_DISCOVERY_RESULTS)

    if not results:
        print("No Polymarket sports markets matched the late-favorite filters.")
        return

    if len(results) < target_results:
        print(
            f"Completed {len(results)} Polymarket backtests after attempting {attempted} markets. "
            f"Qualified markets: {qualified}. Target was {target_results}."
        )

    print_backtest_summary(
        results=results,
        market_key="slug",
        count_key="trades",
        count_label="Trades",
        pnl_label="Settlement PnL (USDC)",
    )


if __name__ == "__main__":
    asyncio.run(run())
