from __future__ import annotations

import json
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from prediction_market_extensions.backtesting._replay_specs import BookReplay

POLYMARKET_PROFILE_TRADES_URL = "https://data-api.polymarket.com/trades"
DEFAULT_PROFILE_REPLAY_PREFIXES = ("btc-updown-5m-", "btc-updown-15m-")
PROFILE_REPLAY_USER_AGENT = "prediction-market-backtesting/profile-replay"

_BTC_UPDOWN_SLUG_RE = re.compile(r"^btc-updown-(?P<minutes>\d+)m-(?P<start>\d+)$")


@dataclass(frozen=True)
class ProfileTrade:
    slug: str
    outcome_index: int
    side: str
    size: Decimal
    price: Decimal
    timestamp: pd.Timestamp
    transaction_hash: str
    title: str = ""
    outcome: str = ""

    @property
    def key(self) -> str:
        return profile_replay_key(slug=self.slug, outcome_index=self.outcome_index)

    @property
    def timestamp_ns(self) -> int:
        return int(self.timestamp.value)


@dataclass(frozen=True)
class ProfileTradeGroup:
    slug: str
    outcome_index: int
    trades: tuple[ProfileTrade, ...]

    @property
    def key(self) -> str:
        return profile_replay_key(slug=self.slug, outcome_index=self.outcome_index)

    @property
    def outcome(self) -> str:
        return self.trades[0].outcome if self.trades else ""

    @property
    def title(self) -> str:
        return self.trades[0].title if self.trades else self.slug


def profile_replay_key(*, slug: str, outcome_index: int) -> str:
    return f"{slug}:{int(outcome_index)}"


def _decimal_from_payload(value: object, *, field: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"profile trade field {field!r} is not decimal-like: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"profile trade field {field!r} is not finite: {value!r}")
    return parsed


def _timestamp_from_payload(value: object) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(float(value), unit="s", tz="UTC")
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"profile trade timestamp is invalid: {value!r}") from exc
    if pd.isna(timestamp):
        raise ValueError(f"profile trade timestamp is invalid: {value!r}")
    return timestamp


def normalize_profile_trade(payload: Mapping[str, object]) -> ProfileTrade:
    slug = str(payload.get("slug") or "").strip()
    if not slug:
        raise ValueError("profile trade is missing slug")

    side = str(payload.get("side") or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        raise ValueError(f"profile trade side must be BUY or SELL, got {side!r}")

    try:
        outcome_index = int(payload.get("outcomeIndex"))
    except (TypeError, ValueError) as exc:
        raise ValueError("profile trade is missing integer outcomeIndex") from exc

    size = _decimal_from_payload(payload.get("size"), field="size")
    price = _decimal_from_payload(payload.get("price"), field="price")
    if size <= 0:
        raise ValueError(f"profile trade size must be positive, got {size}")
    if price < 0 or price > 1:
        raise ValueError(f"profile trade price must be in [0, 1], got {price}")

    return ProfileTrade(
        slug=slug,
        outcome_index=outcome_index,
        side=side,
        size=size,
        price=price,
        timestamp=_timestamp_from_payload(payload.get("timestamp")),
        transaction_hash=str(payload.get("transactionHash") or ""),
        title=str(payload.get("title") or ""),
        outcome=str(payload.get("outcome") or ""),
    )


def normalize_profile_trades(payloads: Iterable[Mapping[str, object]]) -> tuple[ProfileTrade, ...]:
    trades: list[ProfileTrade] = []
    for payload in payloads:
        try:
            trades.append(normalize_profile_trade(payload))
        except ValueError:
            continue
    return tuple(sorted(trades, key=lambda trade: (trade.timestamp_ns, trade.transaction_hash)))


def fetch_profile_trades(
    *,
    user: str,
    limit: int = 500,
    taker_only: bool = False,
    base_url: str = POLYMARKET_PROFILE_TRADES_URL,
    timeout_seconds: float = 30.0,
) -> tuple[ProfileTrade, ...]:
    query = urlencode(
        {
            "user": user,
            "limit": int(limit),
            "takerOnly": "true" if taker_only else "false",
        }
    )
    request = Request(
        f"{base_url}?{query}",
        headers={"User-Agent": PROFILE_REPLAY_USER_AGENT},
    )
    with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Polymarket profile trades API did not return a list")
    return normalize_profile_trades(item for item in payload if isinstance(item, Mapping))


def _inventory_never_negative(trades: Sequence[ProfileTrade]) -> bool:
    inventory = Decimal("0")
    for trade in sorted(trades, key=lambda item: item.timestamp_ns):
        if trade.side == "BUY":
            inventory += trade.size
        else:
            inventory -= trade.size
        if inventory < Decimal("-0.000001"):
            return False
    return True


def select_profile_trade_groups(
    trades: Sequence[ProfileTrade],
    *,
    max_groups: int,
    allowed_slug_prefixes: Sequence[str] = DEFAULT_PROFILE_REPLAY_PREFIXES,
    require_complete_inventory: bool = True,
) -> tuple[ProfileTradeGroup, ...]:
    grouped: dict[tuple[str, int], list[ProfileTrade]] = defaultdict(list)
    for trade in trades:
        if allowed_slug_prefixes and not trade.slug.startswith(tuple(allowed_slug_prefixes)):
            continue
        grouped[(trade.slug, trade.outcome_index)].append(trade)

    candidates: list[ProfileTradeGroup] = []
    for (slug, outcome_index), group_trades in grouped.items():
        ordered = tuple(sorted(group_trades, key=lambda item: item.timestamp_ns))
        if require_complete_inventory and not _inventory_never_negative(ordered):
            continue
        candidates.append(ProfileTradeGroup(slug=slug, outcome_index=outcome_index, trades=ordered))

    candidates.sort(
        key=lambda group: max(trade.timestamp_ns for trade in group.trades),
        reverse=True,
    )
    return tuple(candidates[: max(0, int(max_groups))])


def infer_profile_replay_window(
    group: ProfileTradeGroup,
    *,
    lead_time_seconds: float,
    start_buffer_seconds: float,
    end_buffer_seconds: float,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    first_trade = min(trade.timestamp for trade in group.trades)
    last_trade = max(trade.timestamp for trade in group.trades)

    market_start: pd.Timestamp | None = None
    market_end: pd.Timestamp | None = None
    match = _BTC_UPDOWN_SLUG_RE.match(group.slug)
    if match is not None:
        market_start = pd.Timestamp(int(match.group("start")), unit="s", tz="UTC")
        market_end = market_start + pd.Timedelta(minutes=int(match.group("minutes")))

    start_anchor = min(first_trade, market_start) if market_start is not None else first_trade
    end_anchor = max(last_trade, market_end) if market_end is not None else last_trade
    return (
        start_anchor
        - pd.Timedelta(seconds=float(start_buffer_seconds) + max(float(lead_time_seconds), 0.0)),
        end_anchor + pd.Timedelta(seconds=float(end_buffer_seconds)),
    )


def profile_trades_by_key(
    groups: Sequence[ProfileTradeGroup],
) -> dict[str, list[dict[str, object]]]:
    return {
        group.key: [
            {
                "side": trade.side,
                "size": float(trade.size),
                "price": float(trade.price),
                "timestamp_ns": trade.timestamp_ns,
                "transaction_hash": trade.transaction_hash,
            }
            for trade in group.trades
        ]
        for group in groups
    }


def build_profile_replays(
    groups: Sequence[ProfileTradeGroup],
    *,
    profile_user: str,
    lead_time_seconds: float,
    start_buffer_seconds: float = 120.0,
    end_buffer_seconds: float = 1800.0,
) -> tuple[BookReplay, ...]:
    replays: list[BookReplay] = []
    for group in groups:
        start_time, end_time = infer_profile_replay_window(
            group,
            lead_time_seconds=lead_time_seconds,
            start_buffer_seconds=start_buffer_seconds,
            end_buffer_seconds=end_buffer_seconds,
        )
        replays.append(
            BookReplay(
                market_slug=group.slug,
                token_index=group.outcome_index,
                start_time=start_time,
                end_time=end_time,
                metadata={
                    "sim_label": group.key,
                    "profile_replay_key": group.key,
                    "profile_user": profile_user,
                    "profile_trade_count": len(group.trades),
                    "profile_outcome": group.outcome,
                },
            )
        )
    return tuple(replays)


def profile_actual_pnl(
    trades: Sequence[ProfileTrade],
    *,
    realized_outcome: float | int | None,
) -> dict[str, float | None]:
    cashflow = Decimal("0")
    inventory = Decimal("0")
    buy_quantity = Decimal("0")
    sell_quantity = Decimal("0")
    for trade in sorted(trades, key=lambda item: item.timestamp_ns):
        notional = trade.price * trade.size
        if trade.side == "BUY":
            cashflow -= notional
            inventory += trade.size
            buy_quantity += trade.size
        else:
            cashflow += notional
            inventory -= trade.size
            sell_quantity += trade.size

    settlement_value: Decimal | None = None
    pnl: Decimal | None = None
    if realized_outcome is not None:
        settlement_value = inventory * Decimal(str(realized_outcome))
        pnl = cashflow + settlement_value

    return {
        "profile_trade_cashflow": float(cashflow),
        "profile_open_quantity": float(inventory),
        "profile_buy_quantity": float(buy_quantity),
        "profile_sell_quantity": float(sell_quantity),
        "profile_settlement_value": float(settlement_value)
        if settlement_value is not None
        else None,
        "profile_actual_pnl": float(pnl) if pnl is not None else None,
    }


def append_profile_replay_diagnostics(
    results: Sequence[Mapping[str, object]],
    groups: Sequence[ProfileTradeGroup],
) -> list[dict[str, object]]:
    groups_by_key = {group.key: group for group in groups}
    enriched: list[dict[str, object]] = []
    for result in results:
        row = dict(result)
        key = str(row.get("profile_replay_key") or row.get("sim_label") or "")
        group = groups_by_key.get(key)
        if group is None:
            enriched.append(row)
            continue
        actual = profile_actual_pnl(
            group.trades,
            realized_outcome=row.get("realized_outcome"),  # type: ignore[arg-type]
        )
        row.update(actual)
        if actual["profile_actual_pnl"] is not None:
            row["profile_pnl_error"] = float(row.get("pnl") or 0.0) - float(
                actual["profile_actual_pnl"] or 0.0
            )
        enriched.append(row)
    return enriched


__all__ = [
    "DEFAULT_PROFILE_REPLAY_PREFIXES",
    "POLYMARKET_PROFILE_TRADES_URL",
    "PROFILE_REPLAY_USER_AGENT",
    "ProfileTrade",
    "ProfileTradeGroup",
    "append_profile_replay_diagnostics",
    "build_profile_replays",
    "fetch_profile_trades",
    "infer_profile_replay_window",
    "normalize_profile_trade",
    "normalize_profile_trades",
    "profile_actual_pnl",
    "profile_replay_key",
    "profile_trades_by_key",
    "select_profile_trade_groups",
]
