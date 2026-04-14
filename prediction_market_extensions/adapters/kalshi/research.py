# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the
#  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied. See the License for the specific language governing
#  permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any

import msgspec
import pandas as pd
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.data import Bar

from prediction_market_extensions.adapters.kalshi.loaders import KalshiDataLoader
from prediction_market_extensions.adapters.kalshi.market_selection import (
    end_date_utc,
    is_game_market,
    is_resolved_sports_market,
    is_sports_market,
    volume_24h,
    yes_price,
)
from prediction_market_extensions.adapters.kalshi.providers import (
    KALSHI_REST_BASE,
    market_dict_to_instrument,
)

MarketPredicate = Callable[[Mapping[str, Any]], bool]
MarketSortKey = Callable[[Mapping[str, Any]], float]


def _passes_filters(
    market: Mapping[str, Any],
    *,
    min_volume_24h: float,
    yes_price_min: float | None,
    yes_price_max: float | None,
    min_expiry_dt: datetime | None,
    predicate: MarketPredicate | None,
) -> bool:
    if volume_24h(market) < min_volume_24h:
        return False

    if yes_price_min is not None or yes_price_max is not None:
        market_yes_price = yes_price(market)
        if market_yes_price is None:
            return False
        if yes_price_min is not None and market_yes_price < yes_price_min:
            return False
        if yes_price_max is not None and market_yes_price > yes_price_max:
            return False

    if min_expiry_dt is not None:
        expiry = end_date_utc(market)
        if expiry is not None and expiry < min_expiry_dt:
            return False

    return predicate is None or predicate(market)


def _extend_with_event_markets(
    all_markets: list[dict[str, Any]],
    events: list[dict[str, Any]],
    *,
    exclude_ticker_prefixes: tuple[str, ...],
) -> None:
    for event in events:
        series_ticker = event.get("series_ticker", "")
        nested = event.get("markets") or []
        for market in nested:
            ticker = market.get("ticker", "")
            if ticker.startswith(exclude_ticker_prefixes):
                continue
            market["series_ticker"] = series_ticker
            market["category"] = event.get("category")
            market["event_title"] = event.get("title")
            market["event_sub_title"] = event.get("sub_title")
            all_markets.append(market)


def _default_http_client(*, quota_rate_per_second: int) -> nautilus_pyo3.HttpClient:
    return nautilus_pyo3.HttpClient(
        default_quota=nautilus_pyo3.Quota.rate_per_second(quota_rate_per_second)
    )


async def fetch_market_by_ticker(
    ticker: str,
    *,
    http_client: nautilus_pyo3.HttpClient | None = None,
    quota_rate_per_second: int = 10,
) -> dict[str, Any]:
    """
    Fetch one Kalshi market by ticker.
    """
    client = http_client or _default_http_client(quota_rate_per_second=quota_rate_per_second)
    resp = await client.get(url=f"{KALSHI_REST_BASE}/markets/{ticker}")
    if resp.status != 200:
        raise RuntimeError(
            f"HTTP {resp.status} while fetching market {ticker}: {resp.body.decode('utf-8')}"
        )

    data = msgspec.json.decode(resp.body)
    market = data.get("market")
    if not isinstance(market, dict):
        raise RuntimeError(f"Invalid market payload for {ticker}")

    return market


async def discover_markets(
    *,
    http_client: nautilus_pyo3.HttpClient,
    candidate_limit: int,
    status: str = "open",
    page_limit: int = 200,
    max_pages: int | None = None,
    include_nested_markets: bool = True,
    exclude_ticker_prefixes: tuple[str, ...] = ("KXMVE",),
    min_volume_24h: float = 0.0,
    yes_price_min: float | None = None,
    yes_price_max: float | None = None,
    min_days_to_expiry: int | None = None,
    predicate: MarketPredicate | None = None,
    sort_key: MarketSortKey = volume_24h,
    descending: bool = True,
) -> list[dict[str, Any]]:
    """
    Discover Kalshi markets from the events endpoint with optional filtering.
    """
    all_markets: list[dict[str, Any]] = []
    cursor: str | None = None
    page_count = 0

    while True:
        if max_pages is not None and page_count >= max_pages:
            break

        params: dict[str, str] = {
            "status": status,
            "limit": str(page_limit),
            "with_nested_markets": "true" if include_nested_markets else "false",
        }
        if cursor:
            params["cursor"] = cursor

        resp = await http_client.get(url=f"{KALSHI_REST_BASE}/events", params=params)
        if resp.status != 200:
            break

        data = msgspec.json.decode(resp.body)
        events = data.get("events", [])
        if not events:
            break

        _extend_with_event_markets(
            all_markets, events, exclude_ticker_prefixes=exclude_ticker_prefixes
        )
        page_count += 1

        cursor = data.get("cursor")
        if not cursor:
            break

    min_expiry_dt = None
    if min_days_to_expiry is not None:
        min_expiry_dt = datetime.now(UTC) + timedelta(days=min_days_to_expiry)

    filtered = [
        market
        for market in all_markets
        if _passes_filters(
            market,
            min_volume_24h=min_volume_24h,
            yes_price_min=yes_price_min,
            yes_price_max=yes_price_max,
            min_expiry_dt=min_expiry_dt,
            predicate=predicate,
        )
    ]
    filtered.sort(key=sort_key, reverse=descending)
    return filtered[:candidate_limit]


async def discover_live_sports_markets(
    *,
    candidate_limit: int,
    http_client: nautilus_pyo3.HttpClient | None = None,
    quota_rate_per_second: int = 10,
    max_pages: int | None = None,
    page_limit: int = 200,
    min_volume: float = 0.0,
    max_hours_to_close: float,
    max_market_duration_days: float | None = None,
    games_only: bool = False,
) -> list[dict[str, Any]]:
    """
    Discover live Kalshi sports markets near expiry.
    """
    client = http_client or _default_http_client(quota_rate_per_second=quota_rate_per_second)
    now = datetime.now(UTC)
    return await discover_markets(
        http_client=client,
        candidate_limit=candidate_limit,
        status="open",
        page_limit=page_limit,
        max_pages=max_pages,
        min_volume_24h=min_volume,
        predicate=lambda market: (
            is_sports_market(
                market,
                now=now,
                max_hours_to_close=max_hours_to_close,
                max_market_duration_days=max_market_duration_days,
            )
            and (not games_only or is_game_market(market))
        ),
    )


async def discover_resolved_sports_markets(
    *,
    candidate_limit: int,
    http_client: nautilus_pyo3.HttpClient | None = None,
    quota_rate_per_second: int = 10,
    max_pages: int | None = None,
    page_limit: int = 200,
    min_volume: float = 0.0,
    max_days_since_close: float,
    max_market_duration_days: float | None = None,
    games_only: bool = False,
) -> list[dict[str, Any]]:
    """
    Discover recently settled Kalshi sports markets.
    """
    client = http_client or _default_http_client(quota_rate_per_second=quota_rate_per_second)
    now = datetime.now(UTC)
    return await discover_markets(
        http_client=client,
        candidate_limit=candidate_limit,
        status="settled",
        page_limit=page_limit,
        max_pages=max_pages,
        min_volume_24h=min_volume,
        predicate=lambda market: (
            is_resolved_sports_market(
                market,
                now=now,
                max_days_since_close=max_days_since_close,
                max_market_duration_days=max_market_duration_days,
            )
            and (not games_only or is_game_market(market))
        ),
    )


def _analysis_window_end(*, market: Mapping[str, Any], now: datetime) -> datetime | None:
    status = str(market.get("status", "")).lower()
    if status in {"settled", "finalized"}:
        return end_date_utc(market)
    return now if now.tzinfo is not None else now.replace(tzinfo=UTC)


async def analyze_market_trade_window(
    *,
    market: Mapping[str, Any],
    lookback_days: int,
    entry_price: float,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    """
    Load a market's trade path over the analysis window and attach threshold diagnostics.
    """
    normalized_now = now if now is not None else datetime.now(UTC)
    window_end = _analysis_window_end(market=market, now=normalized_now)
    if window_end is None:
        return None

    start = pd.Timestamp(window_end - timedelta(days=lookback_days))
    end = pd.Timestamp(window_end)
    try:
        loader = await KalshiDataLoader.from_market_ticker(str(market["ticker"]))
        trades = await loader.load_trades(start, end)
    except Exception:
        return None

    if not trades:
        return None

    prices = [float(trade.price) for trade in trades]
    crossed_entry = False
    entry_cross_time: str | None = None
    for previous_trade, current_trade in zip(trades, trades[1:], strict=False):
        previous_price = float(previous_trade.price)
        current_price = float(current_trade.price)
        if previous_price < entry_price <= current_price:
            crossed_entry = True
            entry_cross_time = pd.Timestamp(
                int(current_trade.ts_event), unit="ns", tz="UTC"
            ).isoformat()
            break

    snapshot = dict(market)
    snapshot["analysis_window_start"] = start.isoformat()
    snapshot["analysis_window_end"] = end.isoformat()
    snapshot["first_window_price"] = prices[0]
    snapshot["max_window_price"] = max(prices)
    snapshot["last_window_price"] = prices[-1]
    snapshot["window_trades"] = len(trades)
    snapshot["crossed_entry"] = crossed_entry
    snapshot["entry_cross_time"] = entry_cross_time
    return snapshot


async def select_breakout_markets_per_game(
    *,
    markets: list[dict[str, Any]],
    lookback_days: int,
    entry_price: float,
    now: datetime | None = None,
    max_results: int | None = None,
) -> list[dict[str, Any]]:
    """
    Select one market per game using the earliest threshold breakout in the analysis window.
    """
    normalized_now = now if now is not None else datetime.now(UTC)
    grouped: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for market in markets:
        event_key = str(
            market.get("event_ticker") or market.get("event_title") or market.get("ticker") or ""
        ).strip()
        if event_key:
            grouped[event_key].append(market)

    selected: list[dict[str, Any]] = []

    for event_markets in grouped.values():
        if len(event_markets) < 2:
            continue

        snapshots = [
            snapshot
            for market in event_markets
            for snapshot in [
                await analyze_market_trade_window(
                    market=market,
                    lookback_days=lookback_days,
                    entry_price=entry_price,
                    now=normalized_now,
                )
            ]
            if snapshot is not None and snapshot.get("window_trades")
        ]

        if len(snapshots) < 2:
            continue

        crossed = [market for market in snapshots if market.get("crossed_entry")]
        if crossed:
            chosen = min(
                crossed,
                key=lambda market: (
                    str(market.get("entry_cross_time") or ""),
                    -float(market.get("volume") or 0.0),
                ),
            )
        else:
            chosen = min(
                snapshots,
                key=lambda market: (
                    float(market.get("first_window_price") or 1.0),
                    -float(market.get("volume") or 0.0),
                ),
            )
        opponent = max(snapshots, key=lambda market: float(market.get("first_window_price") or 0.0))

        chosen["opponent_ticker"] = opponent.get("ticker")
        chosen["opponent_side"] = opponent.get("yes_sub_title")
        chosen["opponent_first_window_price"] = opponent.get("first_window_price")
        chosen["event_total_volume"] = sum(float(m.get("volume") or 0.0) for m in snapshots)
        selected.append(chosen)

    selected.sort(
        key=lambda market: float(market.get("event_total_volume") or market.get("volume") or 0.0),
        reverse=True,
    )
    return selected[:max_results] if max_results is not None else selected


async def load_market_bars(
    *,
    market: Mapping[str, Any],
    start: pd.Timestamp,
    end: pd.Timestamp,
    http_client: nautilus_pyo3.HttpClient,
    interval: str = "Minutes1",
    chunk_minutes: int = 5_000,
    min_bars: int = 0,
    min_price_range: float = 0.0,
    max_retries: int = 4,
    retry_base_delay: float = 2.0,
) -> tuple[KalshiDataLoader, list[Bar]] | None:
    """
    Load and validate bar history for a Kalshi market.
    """
    ticker = str(market.get("ticker", "UNKNOWN"))
    try:
        instrument = market_dict_to_instrument(dict(market))
        series_ticker = str(market.get("series_ticker", ""))
        loader = KalshiDataLoader(
            instrument=instrument, series_ticker=series_ticker, http_client=http_client
        )

        bars: list[Bar] = []
        chunk_delta = pd.Timedelta(minutes=chunk_minutes)
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + chunk_delta, end)
            for attempt in range(max_retries + 1):
                try:
                    bars.extend(
                        await loader.load_bars(
                            start=chunk_start,
                            end=chunk_end,
                            interval=interval,
                        )
                    )
                    break
                except RuntimeError as rt_err:
                    if "429" not in str(rt_err) or attempt >= max_retries:
                        raise
                    delay = retry_base_delay * (2**attempt)
                    print(f"    rate-limited on {ticker}, retrying in {delay:.0f}s...")
                    await asyncio.sleep(delay)
            chunk_start = chunk_end

        if len(bars) < min_bars:
            print(f"  skip {ticker}: fewer than {min_bars} bars")
            return None

        closes = [float(bar.close) for bar in bars]
        if closes:
            price_range = max(closes) - min(closes)
            if price_range < min_price_range:
                print(f"  skip {ticker}: price range {price_range:.3f} < {min_price_range:.3f}")
                return None

        return loader, bars
    except Exception as exc:
        print(f"  skip {ticker}: {exc}")
        return None
