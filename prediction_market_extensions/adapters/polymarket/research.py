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

from collections.abc import Callable
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any

import pandas as pd
import msgspec

from nautilus_trader.adapters.polymarket.common.gamma_markets import list_markets
from prediction_market_extensions.adapters.polymarket.market_selection import closed_time_utc
from prediction_market_extensions.adapters.polymarket.market_selection import end_date_utc
from prediction_market_extensions.adapters.polymarket.market_selection import event_start_utc
from prediction_market_extensions.adapters.polymarket.market_selection import is_game_market
from prediction_market_extensions.adapters.polymarket.market_selection import (
    is_resolved_sports_market,
)
from prediction_market_extensions.adapters.polymarket.market_selection import is_sports_market
from prediction_market_extensions.adapters.polymarket.market_selection import volume_24h
from prediction_market_extensions.adapters.polymarket.market_selection import yes_price
from nautilus_trader.adapters.polymarket.loaders import PolymarketDataLoader
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.data import TradeTick


MarketPredicate = Callable[[Mapping[str, Any]], bool]
MarketSortKey = Callable[[Mapping[str, Any]], float]
GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events"
GAMMA_SPORTS_URL = "https://gamma-api.polymarket.com/sports"
GAME_BET_TAG_ID = "100639"


def _default_http_client(*, quota_rate_per_second: int) -> nautilus_pyo3.HttpClient:
    return nautilus_pyo3.HttpClient(
        default_quota=nautilus_pyo3.Quota.rate_per_second(quota_rate_per_second)
    )


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


def _event_volume(market: Mapping[str, Any]) -> float:
    return float(market.get("event_total_volume") or volume_24h(market))


def _main_market_from_event(event: Mapping[str, Any]) -> dict[str, Any] | None:
    event_slug = str(event.get("slug") or "").strip().lower()
    markets = event.get("markets")
    if not event_slug or not isinstance(markets, list):
        return None

    for market in markets:
        if not isinstance(market, dict):
            continue
        market_slug = str(market.get("slug") or market.get("market_slug") or "").strip().lower()
        if market_slug != event_slug:
            continue

        snapshot = dict(market)
        snapshot["events"] = [dict(event)]
        if snapshot.get("question") in (None, ""):
            snapshot["question"] = event.get("title")
        if snapshot.get("gameStartTime") in (None, ""):
            snapshot["gameStartTime"] = event.get("startTime")
        if snapshot.get("closedTime") in (None, ""):
            snapshot["closedTime"] = event.get("closedTime")
        snapshot["seriesSlug"] = event.get("seriesSlug")
        snapshot["event_total_volume"] = float(event.get("volume") or 0.0)
        return snapshot

    return None


async def _discover_resolved_game_markets_from_events(
    *,
    candidate_limit: int,
    http_client: nautilus_pyo3.HttpClient | None = None,
    max_results: int,
    quota_rate_per_second: int,
    min_volume_24h: float,
    max_days_since_close: float,
) -> list[dict[str, Any]]:
    client = http_client or _default_http_client(quota_rate_per_second=quota_rate_per_second)
    now = datetime.now(UTC)
    collected: list[dict[str, Any]] = []
    page_limit = 200

    sports_response = await client.get(url=GAMMA_SPORTS_URL)
    if sports_response.status != 200:
        return []
    sports_payload = msgspec.json.decode(sports_response.body)
    if not isinstance(sports_payload, list):
        return []

    series_ids = [
        str(sport.get("series"))
        for sport in sports_payload
        if isinstance(sport, dict)
        and sport.get("series") not in (None, "")
        and GAME_BET_TAG_ID in str(sport.get("tags") or "").split(",")
    ]

    seen_slugs: set[str] = set()
    per_series_limit = min(max_results, page_limit)
    target_buffer = max(candidate_limit * 3, candidate_limit)
    for series_id in series_ids:
        offset = 0
        while offset < per_series_limit and len(collected) < target_buffer:
            limit = min(page_limit, per_series_limit - offset)
            response = await client.get(
                url=GAMMA_EVENTS_URL,
                params={
                    "series_id": series_id,
                    "tag_id": GAME_BET_TAG_ID,
                    "closed": "true",
                    "limit": str(limit),
                    "offset": str(offset),
                },
            )
            if response.status != 200:
                break

            payload = msgspec.json.decode(response.body)
            if not isinstance(payload, list) or not payload:
                break

            for event in payload:
                if not isinstance(event, dict):
                    continue
                market = _main_market_from_event(event)
                if market is None:
                    continue
                slug = str(market.get("slug") or market.get("market_slug") or "")
                if not slug or slug in seen_slugs:
                    continue
                if _event_volume(market) < min_volume_24h:
                    continue
                if not is_game_market(market):
                    continue
                if not is_resolved_sports_market(
                    market, now=now, max_days_since_close=max_days_since_close
                ):
                    continue
                seen_slugs.add(slug)
                collected.append(market)

            if len(payload) < limit:
                break
            offset += limit
        if len(collected) >= target_buffer:
            break

    collected.sort(key=_event_volume, reverse=True)
    return collected[:candidate_limit]


async def discover_markets(
    *,
    candidate_limit: int,
    http_client: nautilus_pyo3.HttpClient | None = None,
    api_filters: dict[str, Any] | None = None,
    max_results: int = 200,
    quota_rate_per_second: int = 20,
    min_volume_24h: float = 0.0,
    yes_price_min: float | None = None,
    yes_price_max: float | None = None,
    min_days_to_expiry: int | None = None,
    predicate: MarketPredicate | None = None,
    sort_key: MarketSortKey = volume_24h,
    descending: bool = True,
) -> list[dict[str, Any]]:
    """
    Discover Polymarket markets from Gamma with optional filtering.
    """
    client = http_client
    if client is None:
        client = nautilus_pyo3.HttpClient(
            default_quota=nautilus_pyo3.Quota.rate_per_second(quota_rate_per_second)
        )

    filters = {"limit": 200}
    if api_filters is None:
        filters["is_active"] = True
    if api_filters is not None:
        filters.update(api_filters)

    markets = await list_markets(
        http_client=client,
        filters=filters,
        max_results=max_results,
    )

    if not markets:
        return []

    min_expiry_dt = None
    if min_days_to_expiry is not None:
        min_expiry_dt = datetime.now(UTC) + timedelta(days=min_days_to_expiry)

    filtered = [
        market
        for market in markets
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


async def fetch_market_by_slug(
    slug: str,
    *,
    http_client: nautilus_pyo3.HttpClient | None = None,
    quota_rate_per_second: int = 10,
) -> dict[str, Any]:
    """
    Fetch one Polymarket market by slug from Gamma.
    """
    client = http_client or _default_http_client(quota_rate_per_second=quota_rate_per_second)
    response = await client.get(url=f"https://gamma-api.polymarket.com/markets/slug/{slug}")
    if response.status != 200:
        raise RuntimeError(
            f"HTTP {response.status} while fetching market {slug}: {response.body.decode('utf-8')}"
        )

    payload = msgspec.json.decode(response.body)
    market = payload[0] if isinstance(payload, list) and payload else payload
    if not isinstance(market, dict):
        raise RuntimeError(f"Invalid market payload for {slug}")

    return market


async def discover_live_sports_markets(
    *,
    candidate_limit: int,
    http_client: nautilus_pyo3.HttpClient | None = None,
    max_results: int = 200,
    quota_rate_per_second: int = 20,
    min_volume_24h: float = 0.0,
    max_hours_to_close: float,
    games_only: bool = False,
) -> list[dict[str, Any]]:
    """
    Discover live Polymarket sports markets near expiry.
    """
    now = datetime.now(UTC)
    return await discover_markets(
        candidate_limit=candidate_limit,
        http_client=http_client,
        max_results=max_results,
        quota_rate_per_second=quota_rate_per_second,
        min_volume_24h=min_volume_24h,
        predicate=lambda market: (
            is_sports_market(market, now=now, max_hours_to_close=max_hours_to_close)
            and (not games_only or is_game_market(market))
        ),
    )


async def discover_resolved_sports_markets(
    *,
    candidate_limit: int,
    http_client: nautilus_pyo3.HttpClient | None = None,
    max_results: int = 200,
    quota_rate_per_second: int = 20,
    min_volume_24h: float = 0.0,
    max_days_since_close: float,
    games_only: bool = False,
) -> list[dict[str, Any]]:
    """
    Discover recently closed Polymarket sports markets.
    """
    if games_only:
        return await _discover_resolved_game_markets_from_events(
            candidate_limit=candidate_limit,
            http_client=http_client,
            max_results=max_results,
            quota_rate_per_second=quota_rate_per_second,
            min_volume_24h=min_volume_24h,
            max_days_since_close=max_days_since_close,
        )

    now = datetime.now(UTC)
    min_close_dt = now - timedelta(days=max_days_since_close)
    return await discover_markets(
        candidate_limit=candidate_limit,
        http_client=http_client,
        api_filters={
            "closed": True,
            "end_date_min": min_close_dt.isoformat().replace("+00:00", "Z"),
        },
        max_results=max_results,
        quota_rate_per_second=quota_rate_per_second,
        min_volume_24h=min_volume_24h,
        predicate=lambda market: (
            is_resolved_sports_market(market, now=now, max_days_since_close=max_days_since_close)
            and (not games_only or is_game_market(market))
        ),
    )


def market_trade_window_bounds(
    market: Mapping[str, Any], *, active_window_hours: float, now: datetime | None = None
) -> tuple[datetime | None, datetime | None]:
    """
    Return the activation start and backtest window end for a sports market.
    """
    normalized_now = now if now is not None else datetime.now(UTC)
    window_end = closed_time_utc(market)
    if window_end is None:
        window_end = (
            normalized_now
            if normalized_now.tzinfo is not None
            else normalized_now.replace(tzinfo=UTC)
        )

    activation_start = None
    if active_window_hours > 0:
        activation_start = window_end - timedelta(hours=active_window_hours)

    game_start = event_start_utc(market)
    if game_start is not None:
        activation_start = (
            game_start if activation_start is None else max(game_start, activation_start)
        )

    if activation_start is not None and activation_start > window_end:
        activation_start = window_end

    return activation_start, window_end


async def analyze_market_trade_window(
    *,
    market: Mapping[str, Any],
    lookback_days: int,
    entry_price: float,
    active_window_hours: float,
    now: datetime | None = None,
    http_client: nautilus_pyo3.HttpClient | None = None,
) -> dict[str, Any] | None:
    """
    Pick the Polymarket token side with the earliest entry breakout in the active window.
    """
    activation_start, window_end = market_trade_window_bounds(
        market, active_window_hours=active_window_hours, now=now
    )
    if window_end is None:
        return None

    start = pd.Timestamp(window_end - timedelta(days=lookback_days))
    end = pd.Timestamp(window_end)
    activation_start_ns = (
        int(pd.Timestamp(activation_start).value) if activation_start is not None else 0
    )
    slug = str(market.get("slug") or market.get("market_slug") or "")
    if not slug:
        return None

    snapshots: list[dict[str, Any]] = []
    for token_index in (0, 1):
        try:
            loader = await PolymarketDataLoader.from_market_slug(
                slug, token_index=token_index, http_client=http_client
            )
            trades = await loader.load_trades(start, end)
        except Exception:
            continue

        if not trades:
            continue

        prices = [float(trade.price) for trade in trades]
        active_trades = [
            trade
            for trade in trades
            if activation_start_ns <= 0 or int(trade.ts_event) >= activation_start_ns
        ]
        active_prices = [float(trade.price) for trade in active_trades]

        crossed_entry = False
        entry_cross_time: str | None = None
        for previous_trade, current_trade in zip(trades, trades[1:], strict=False):
            if activation_start_ns > 0 and int(current_trade.ts_event) < activation_start_ns:
                continue

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
        snapshot["activation_start"] = (
            activation_start.isoformat() if activation_start is not None else None
        )
        snapshot["window_trades"] = len(trades)
        snapshot["first_window_price"] = prices[0]
        snapshot["max_window_price"] = max(prices)
        snapshot["last_window_price"] = prices[-1]
        snapshot["activation_trades"] = len(active_trades)
        snapshot["first_activation_price"] = active_prices[0] if active_prices else None
        snapshot["max_activation_price"] = max(active_prices) if active_prices else None
        snapshot["last_activation_price"] = active_prices[-1] if active_prices else None
        snapshot["crossed_entry"] = crossed_entry
        snapshot["entry_cross_time"] = entry_cross_time
        snapshot["token_index"] = token_index
        snapshot["token_outcome"] = str(loader.instrument.outcome or "")
        snapshots.append(snapshot)

    if not snapshots:
        return None

    crossed = [snapshot for snapshot in snapshots if snapshot.get("crossed_entry")]
    if crossed:
        return min(
            crossed,
            key=lambda snapshot: (
                str(snapshot.get("entry_cross_time") or ""),
                -int(snapshot.get("activation_trades") or 0),
                -float(snapshot.get("volume24hr") or snapshot.get("volume") or 0.0),
            ),
        )

    return max(
        snapshots,
        key=lambda snapshot: (
            float(snapshot.get("max_activation_price") or 0.0),
            int(snapshot.get("activation_trades") or 0),
            float(snapshot.get("volume24hr") or snapshot.get("volume") or 0.0),
        ),
    )


async def load_market_trades(
    *,
    slug: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    min_trades: int = 0,
    min_price_range: float = 0.0,
) -> tuple[PolymarketDataLoader, list[TradeTick]] | None:
    """
    Load and validate trade history for a Polymarket market slug.
    """
    try:
        loader = await PolymarketDataLoader.from_market_slug(slug)
        trades = await loader.load_trades(start, end)
        if len(trades) < min_trades:
            print(f"  skip {slug}: fewer than {min_trades} trades")
            return None

        prices = [float(tick.price) for tick in trades]
        if prices:
            price_range = max(prices) - min(prices)
            if price_range < min_price_range:
                print(f"  skip {slug}: price range {price_range:.3f} < {min_price_range:.3f}")
                return None

        return loader, trades
    except Exception as exc:
        print(f"  skip {slug}: {exc}")
        return None
