# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-29, 2026-03-31, 2026-04-03, and 2026-04-04.
# See the repository NOTICE file for provenance and licensing scope.

"""
VWAP reversion on current Polymarket sports-game markets using PMXT quote ticks.
"""

# ruff: noqa: E402

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import json
import re
import subprocess
from urllib.error import HTTPError

import pandas as pd

from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import finalize_market_results
from backtests._shared._polymarket_quote_tick_pmxt_runner import (
    run_single_market_pmxt_backtest,
)
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick


NAME = "polymarket_quote_tick_pmxt_sports_vwap_reversion"

DESCRIPTION = (
    "VWAP reversion on current Polymarket sports-game markets discovered from the "
    "live sports page using PMXT L2 data"
)

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=(
        "/Volumes/LaCie/pmxt_raws",
        "r2.pmxt.dev",
        "209-209-10-83.sslip.io",
    ),
)

DISCOVERY_PAGE_URL = "https://polymarket.com/sports/live"
_GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets/slug/{slug}"
_GAMMA_EVENT_URL = "https://gamma-api.polymarket.com/events/slug/{slug}"
_HTTP_USER_AGENT = "prediction-market-backtesting/1.0"
_HTTP_TIMEOUT_SECS = 10
_EVENT_SLUG_PATTERN = re.compile(r'href="/event/([^"]+)"')
_DATED_EVENT_SLUG_PATTERN = re.compile(r"-20\d{2}-\d{2}-\d{2}(?:$|-)")
_TARGET_SIM_COUNT = 5
_CANDIDATE_LIMIT = 24
_MAX_DISCOVERY_EVENT_SLUGS = 64
_DISCOVERY_LOOKBACK = pd.Timedelta(hours=24)
_DISCOVERY_LOOKAHEAD = pd.Timedelta(hours=12)
_CURRENT_WINDOW = pd.Timedelta(hours=2)
_BEST_ASK_DEAD_THRESHOLD = 0.002
_BEST_BID_DEAD_THRESHOLD = 0.998
_INITIAL_CASH = 100.0
_PROBABILITY_WINDOW = 30
_MIN_QUOTES = 500
_MIN_PRICE_RANGE = 0.005


def _sample(
    market_slug: str,
    *,
    start_time: str,
    end_time: str,
) -> MarketSimConfig:
    return MarketSimConfig(
        market_slug=market_slug,
        token_index=0,
        start_time=start_time,
        end_time=end_time,
    )


SIMS: tuple[MarketSimConfig, ...] = ()

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickVWAPReversionStrategy",
        "config_path": "strategies:QuoteTickVWAPReversionConfig",
        "config": {
            "trade_size": Decimal("100"),
            "vwap_window": 30,
            "entry_threshold": 0.0015,
            "exit_threshold": 0.0003,
            "min_tick_size": 0.0,
            "take_profit": 0.004,
            "stop_loss": 0.004,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
)

EXECUTION = ExecutionModelConfig(
    queue_position=True,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)

BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=_INITIAL_CASH,
    probability_window=_PROBABILITY_WINDOW,
    min_quotes=_MIN_QUOTES,
    min_price_range=_MIN_PRICE_RANGE,
    execution=EXECUTION,
)


def _iso_z(ts: pd.Timestamp) -> str:
    ts_utc = ts.tz_convert("UTC")
    return ts_utc.isoformat().replace("+00:00", "Z")


def _fetch_text(url: str) -> str:
    completed = subprocess.run(
        ["curl", "-fsSL", "-A", _HTTP_USER_AGENT, url],
        capture_output=True,
        check=True,
        text=True,
        timeout=_HTTP_TIMEOUT_SECS,
    )
    return completed.stdout


def _fetch_json(url: str) -> dict | list:
    return json.loads(_fetch_text(url))


def _extract_event_slugs(html: str) -> tuple[str, ...]:
    slugs: list[str] = []
    seen: set[str] = set()
    for raw_slug in _EVENT_SLUG_PATTERN.findall(html):
        slug = raw_slug.split("/", 1)[0].strip()
        if not slug or slug in seen:
            continue
        if _DATED_EVENT_SLUG_PATTERN.search(slug) is None:
            continue
        seen.add(slug)
        slugs.append(slug)
    return tuple(slugs[:_MAX_DISCOVERY_EVENT_SLUGS])


def _fetch_market(slug: str) -> dict | None:
    try:
        payload = _fetch_json(_GAMMA_MARKET_URL.format(slug=slug))
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except subprocess.CalledProcessError as exc:
        if exc.returncode == 22:
            return None
        raise

    if isinstance(payload, list):
        if not payload:
            return None
        payload = payload[0]
    if not isinstance(payload, dict):
        return None
    return payload


def _fetch_event(slug: str) -> dict | None:
    try:
        payload = _fetch_json(_GAMMA_EVENT_URL.format(slug=slug))
    except HTTPError as exc:
        if exc.code == 404:
            return None
        raise
    except subprocess.CalledProcessError as exc:
        if exc.returncode == 22:
            return None
        raise
    return payload if isinstance(payload, dict) else None


def _safe_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_game_start(value: object) -> pd.Timestamp | None:
    if not value:
        return None
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts


def _market_score(market: dict, *, now: pd.Timestamp) -> tuple[float, float, float]:
    best_bid = _safe_float(market.get("bestBid"))
    best_ask = _safe_float(market.get("bestAsk"))
    volume = _safe_float(market.get("volume24hrClob")) or 0.0
    liquidity = _safe_float(market.get("liquidityClob")) or 0.0
    game_start = _parse_game_start(market.get("gameStartTime"))

    if best_bid is None or best_ask is None:
        competitiveness = 1.0
    else:
        competitiveness = abs(((best_bid + best_ask) / 2.0) - 0.5)

    if game_start is None:
        recency_hours = float("inf")
    else:
        recency_hours = abs((now - game_start) / pd.Timedelta(hours=1))

    return competitiveness, recency_hours, -(volume + liquidity)


def _markets_for_candidate_slug(candidate_slug: str) -> tuple[dict, ...]:
    direct_market = _fetch_market(candidate_slug)
    if direct_market is not None:
        market_slugs = [str(direct_market.get("slug") or candidate_slug)]
    else:
        event = _fetch_event(candidate_slug)
        if event is None:
            return ()
        market_slugs = [
            str(market.get("slug") or "").strip() for market in event.get("markets", [])
        ]

    markets: list[dict] = []
    for market_slug in market_slugs:
        if not market_slug:
            continue
        market = (
            direct_market
            if direct_market and market_slug == direct_market.get("slug")
            else _fetch_market(market_slug)
        )
        if market is not None:
            markets.append(market)
    return tuple(markets)


def _candidate_market_dicts(*, now: pd.Timestamp) -> tuple[dict, ...]:
    html = _fetch_text(DISCOVERY_PAGE_URL)
    candidate_slugs = _extract_event_slugs(html)
    markets: list[dict] = []
    seen_market_slugs: set[str] = set()
    max_workers = min(8, len(candidate_slugs)) or 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for fetched_markets in executor.map(
            _markets_for_candidate_slug, candidate_slugs
        ):
            for market in fetched_markets:
                market_slug = str(market.get("slug") or "").strip()
                if not market_slug or market_slug in seen_market_slugs:
                    continue
                if str(market.get("sportsMarketType") or "").casefold() != "moneyline":
                    continue
                game_start = _parse_game_start(market.get("gameStartTime"))
                if game_start is None:
                    continue
                if game_start < now - _DISCOVERY_LOOKBACK:
                    continue
                if game_start > now + _DISCOVERY_LOOKAHEAD:
                    continue
                best_bid = _safe_float(market.get("bestBid"))
                best_ask = _safe_float(market.get("bestAsk"))
                if best_bid is not None and best_bid >= _BEST_BID_DEAD_THRESHOLD:
                    continue
                if best_ask is not None and best_ask <= _BEST_ASK_DEAD_THRESHOLD:
                    continue
                seen_market_slugs.add(market_slug)
                markets.append(market)

    markets.sort(key=lambda market: _market_score(market, now=now))
    return tuple(markets)


def discover_recent_market_sims(
    *,
    now: pd.Timestamp | None = None,
    limit: int = _CANDIDATE_LIMIT,
) -> tuple[MarketSimConfig, ...]:
    now_ts = pd.Timestamp.now(tz="UTC") if now is None else pd.Timestamp(now)
    if now_ts.tzinfo is None:
        now_ts = now_ts.tz_localize("UTC")
    else:
        now_ts = now_ts.tz_convert("UTC")

    sims: list[MarketSimConfig] = []
    for market in _candidate_market_dicts(now=now_ts):
        market_slug = str(market.get("slug") or "").strip()
        if not market_slug:
            continue
        start_time = now_ts - _CURRENT_WINDOW
        sims.append(
            _sample(
                market_slug,
                start_time=_iso_z(start_time),
                end_time=_iso_z(now_ts),
            )
        )
        if len(sims) >= limit:
            break

    return tuple(sims)


async def _run_discovered_market_sims(
    sims: tuple[MarketSimConfig, ...],
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    for sim in sims:
        market_slug = sim.market_slug
        if not market_slug:
            continue
        result = await run_single_market_pmxt_backtest(
            name=NAME,
            market_slug=market_slug,
            token_index=sim.token_index,
            probability_window=_PROBABILITY_WINDOW,
            strategy_configs=STRATEGY_CONFIGS,
            min_quotes=_MIN_QUOTES,
            min_price_range=_MIN_PRICE_RANGE,
            initial_cash=_INITIAL_CASH,
            emit_summary=False,
            emit_html=False,
            start_time=sim.start_time,
            end_time=sim.end_time,
            data_sources=DATA.sources,
            execution=EXECUTION,
        )
        if result is None:
            continue
        results.append(result)
        if len(results) >= _TARGET_SIM_COUNT:
            break

    return results


@timing_harness
def run() -> None:
    print(f"Discovering current sports-game markets from {DISCOVERY_PAGE_URL}...")
    sims = discover_recent_market_sims()
    if not sims:
        print(
            "No current Polymarket sports-game markets were eligible for PMXT probing."
        )
        return

    print(
        "Discovered "
        f"{len(sims)} candidate sports-game sims; collecting up to "
        f"{_TARGET_SIM_COUNT} usable PMXT runs."
    )
    results = asyncio.run(_run_discovered_market_sims(sims))
    if not results:
        print(
            "No current Polymarket PMXT sports-game sims met the quote-tick requirements."
        )
        return

    target_count = min(_TARGET_SIM_COUNT, len(sims))
    if len(results) < target_count:
        print(f"Completed {len(results)} of {len(sims)} discovered sports-game sims.")

    finalize_market_results(name=NAME, results=results[:target_count], report=REPORT)


if __name__ == "__main__":
    run()
