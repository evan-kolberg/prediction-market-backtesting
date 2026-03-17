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

import re
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from typing import Any

import msgspec


SPORT_TEXT_PATTERN = re.compile(
    r"\bnba\b|\bwnba\b|\bnfl\b|\bmlb\b|\bnhl\b|\bncaa\b|\bcbb\b|\bsoccer\b|\bfootball\b|"
    r"\btennis\b|\bgolf\b|\bmma\b|\bufc\b|\bboxing\b|\bfifa\b|\bstanley cup\b|\bnba finals\b|"
    r"\bworld cup\b|\bfa cup\b|\bchampions league\b|\bmatch\b|\bgame\b|\bfight\b|\bvs\.?(?=\s)",
    re.IGNORECASE,
)
GAME_TEXT_PATTERN = re.compile(r"\bvs\.?\b|\bat\b|@", re.IGNORECASE)
NON_GAME_SLUG_PATTERN = re.compile(
    r"-(?:will|to|by|spread|total|ou|over|under|mvp|rookie|qualify|conference|"
    r"championship|playoffs|final-season|score|goals|points|rebounds|assists)-",
    re.IGNORECASE,
)
GAME_SLUG_PREFIXES = (
    "cbb-",
    "cwbb-",
    "nba-",
    "wnba-",
    "nfl-",
    "cfb-",
    "mlb-",
    "nhl-",
    "soccer-",
    "tennis-",
    "golf-",
    "mma-",
    "ufc-",
    "boxing-",
)


def _parse_datetime(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def _event_payload(market: Mapping[str, Any]) -> Mapping[str, Any]:
    events = market.get("events")
    if not isinstance(events, list):
        return {}
    for event in events:
        if isinstance(event, Mapping):
            return event
    return {}


def volume_24h(market: Mapping[str, Any]) -> float:
    """
    Extract volume from a Polymarket Gamma market payload.
    """
    for key in ("volume24hr", "volume24Hr", "volume"):
        try:
            value = market.get(key, 0)
            if value not in (None, ""):
                return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


def yes_price(market: Mapping[str, Any]) -> float | None:
    """
    Extract YES probability from ``outcomePrices``.
    """
    raw = market.get("outcomePrices")
    if not raw:
        return None

    try:
        prices = msgspec.json.decode(raw) if isinstance(raw, str | bytes) else raw
        if not isinstance(prices, list) or not prices:
            return None
        return float(prices[0])
    except Exception:
        return None


def end_date_utc(market: Mapping[str, Any]) -> datetime | None:
    """
    Parse market end date from Gamma payload fields.
    """
    end_dt = _parse_datetime(market.get("endDate") or market.get("end_date_iso"))
    return end_dt.astimezone(UTC) if end_dt is not None else None


def event_start_utc(market: Mapping[str, Any]) -> datetime | None:
    """
    Parse a Polymarket sports event start time from the richest available fields.
    """
    event = _event_payload(market)
    for raw in (
        market.get("gameStartTime"),
        market.get("startTime"),
        market.get("startDate"),
        event.get("gameStartTime"),
        event.get("startTime"),
        event.get("startDate"),
    ):
        start_dt = _parse_datetime(raw)
        if start_dt is not None:
            return start_dt.astimezone(UTC)
    return None


def closed_time_utc(market: Mapping[str, Any]) -> datetime | None:
    """
    Parse the actual Polymarket market close/resolution time when available.
    """
    event = _event_payload(market)
    for raw in (
        market.get("closedTime"),
        market.get("umaEndDate"),
        event.get("closedTime"),
        event.get("umaEndDate"),
        market.get("endDate"),
        market.get("end_date_iso"),
        event.get("endDate"),
    ):
        close_dt = _parse_datetime(raw)
        if close_dt is not None:
            return close_dt.astimezone(UTC)
    return None


def market_close_time_ns(raw: Any) -> int:
    """
    Convert a Polymarket market close time to nanoseconds since epoch.
    """
    close_dt = _parse_datetime(raw)
    if close_dt is None:
        return 0
    return int(close_dt.astimezone(UTC).timestamp() * 1_000_000_000)


def is_game_market(market: Mapping[str, Any]) -> bool:
    """
    Return True for Polymarket game/match slugs.
    """
    slug = str(market.get("slug") or market.get("market_slug") or "").lower()
    if not slug.startswith(GAME_SLUG_PREFIXES):
        return False
    if NON_GAME_SLUG_PATTERN.search(slug):
        return False

    event = _event_payload(market)
    text = " ".join(
        str(value or "")
        for value in (
            market.get("question"),
            market.get("groupItemTitle"),
            event.get("title"),
            event.get("slug"),
        )
    )
    if GAME_TEXT_PATTERN.search(text):
        return True

    return bool(re.search(r"\d{4}-\d{2}-\d{2}", slug))


def is_sports_market(
    market: Mapping[str, Any],
    *,
    now: datetime,
    max_hours_to_close: float,
) -> bool:
    """
    Return True for live Polymarket sports markets near expiry.
    """
    close_dt = closed_time_utc(market)
    if close_dt is None:
        return False

    normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    hours_left = (close_dt.astimezone(UTC) - normalized_now.astimezone(UTC)).total_seconds() / 3600.0
    if not (0.0 <= hours_left <= max_hours_to_close):
        return False

    slug = str(market.get("slug") or market.get("market_slug") or "").lower()
    question = str(market.get("question") or "")
    if slug.startswith(GAME_SLUG_PREFIXES):
        return True
    return SPORT_TEXT_PATTERN.search(f"{slug} {question}") is not None


def is_resolved_sports_market(
    market: Mapping[str, Any],
    *,
    now: datetime,
    max_days_since_close: float,
) -> bool:
    """
    Return True for recently closed Polymarket sports markets.
    """
    close_dt = closed_time_utc(market)
    if close_dt is None:
        return False

    normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    days_since_close = (
        normalized_now.astimezone(UTC) - close_dt.astimezone(UTC)
    ).total_seconds() / 86400.0
    if not (0.0 <= days_since_close <= max_days_since_close):
        return False

    slug = str(market.get("slug") or market.get("market_slug") or "").lower()
    question = str(market.get("question") or "")
    if slug.startswith(GAME_SLUG_PREFIXES):
        return True
    return SPORT_TEXT_PATTERN.search(f"{slug} {question}") is not None
