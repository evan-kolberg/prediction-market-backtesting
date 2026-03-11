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

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from typing import Any


GAME_TITLE_PATTERN = re.compile(r"\bvs\.?(?=\s)|\bat\b", re.IGNORECASE)
DERIVATIVE_MARKET_PATTERN = re.compile(r"SPREAD|TOTAL", re.IGNORECASE)


def _parse_datetime(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw))
    except (TypeError, ValueError):
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def volume_24h(market: Mapping[str, Any]) -> float:
    """
    Extract 24-hour volume from a Kalshi market payload.
    """
    for key in ("volume_24h", "volume_fp", "volume"):
        raw = market.get(key)
        if raw is None:
            continue
        try:
            volume = float(raw)
        except (TypeError, ValueError):
            continue
        if volume > 0:
            return volume
    return 0.0


def yes_price(market: Mapping[str, Any]) -> float | None:
    """
    Extract and normalize the current YES price for a Kalshi market.
    """
    for key in ("last_price_dollars", "yes_bid_dollars", "yes_price_dollars", "yes_price"):
        raw = market.get(key)
        if raw is None:
            continue
        try:
            price = float(raw)
        except (TypeError, ValueError):
            continue
        if price >= 1.0:
            price /= 100.0  # legacy integer-cents fields
        if 0.0 < price < 1.0:
            return price
    return None


def end_date_utc(market: Mapping[str, Any]) -> datetime | None:
    """
    Parse market expiry from a Kalshi market payload.
    """
    return _parse_datetime(market.get("close_time") or market.get("latest_expiration_time"))


def market_close_time_ns(raw: Any) -> int:
    """
    Convert a Kalshi market close time to nanoseconds since epoch.
    """
    close_dt = _parse_datetime(raw)
    if close_dt is None:
        return 0
    return int(close_dt.astimezone(UTC).timestamp() * 1_000_000_000)


def days_since_close(raw: Any, now: datetime) -> float | None:
    """
    Return elapsed days since a market closed.
    """
    close_dt = _parse_datetime(raw)
    if close_dt is None:
        return None
    normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    return (normalized_now - close_dt.astimezone(UTC)).total_seconds() / 86400.0


def market_duration_days(market: Mapping[str, Any]) -> float | None:
    """
    Return the elapsed days between market open and close.
    """
    open_raw = market.get("open_time") or market.get("created_time")
    close_dt = end_date_utc(market)
    if not open_raw or close_dt is None:
        return None

    open_dt = _parse_datetime(open_raw)
    if open_dt is None:
        return None

    return max(0.0, (close_dt.astimezone(UTC) - open_dt).total_seconds() / 86400.0)


def is_game_market(market: Mapping[str, Any]) -> bool:
    """
    Return True for direct game or match winner markets.
    """
    market_ticker = str(market.get("ticker") or "").upper()
    event_title = str(market.get("event_title") or market.get("title") or "")
    if DERIVATIVE_MARKET_PATTERN.search(market_ticker):
        return False
    if "GAME" in market_ticker or "MATCH" in market_ticker:
        return True
    return GAME_TITLE_PATTERN.search(event_title) is not None


def is_sports_market(
    market: Mapping[str, Any],
    *,
    now: datetime,
    max_hours_to_close: float,
    max_market_duration_days: float | None = None,
) -> bool:
    """
    Return True for live Kalshi sports markets near expiry.
    """
    if str(market.get("category", "")).lower() != "sports":
        return False

    if max_market_duration_days is not None:
        duration_days = market_duration_days(market)
        if duration_days is None or duration_days > max_market_duration_days:
            return False

    close_dt = end_date_utc(market)
    if close_dt is None:
        return False

    normalized_now = now if now.tzinfo is not None else now.replace(tzinfo=UTC)
    hours_left = (close_dt.astimezone(UTC) - normalized_now.astimezone(UTC)).total_seconds() / 3600.0
    return 0.0 <= hours_left <= max_hours_to_close


def is_resolved_sports_market(
    market: Mapping[str, Any],
    *,
    now: datetime,
    max_days_since_close: float,
    max_market_duration_days: float | None = None,
) -> bool:
    """
    Return True for recently settled Kalshi sports markets.
    """
    if str(market.get("category", "")).lower() != "sports":
        return False

    if max_market_duration_days is not None:
        duration_days = market_duration_days(market)
        if duration_days is None or duration_days > max_market_duration_days:
            return False

    status = str(market.get("status", "")).lower()
    if status not in {"settled", "finalized"}:
        return False

    days_closed = days_since_close(market.get("close_time"), now)
    if days_closed is None:
        return False

    return 0.0 <= days_closed <= max_days_since_close
