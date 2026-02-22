"""Live Polymarket WebSocket feed — streams real-time trades for front testing.

Connects to the Polymarket CLOB WebSocket (public MARKET channel — no auth
required) and yields TradeEvent objects as trades occur.

No environment variables required for public market data.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator
from datetime import datetime, timezone

from src.backtesting.models import MarketInfo, MarketStatus, Platform, Side, TradeEvent

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore[assignment]

__all__ = ["PolymarketLiveFeed", "fetch_polymarket_markets", "fetch_random_polymarket_condition"]

_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
_CLOB_API = "https://clob.polymarket.com"
_GAMMA_API = "https://gamma-api.polymarket.com"
_UA = "prediction-market-backtester/0.1"


def _get(url: str) -> dict | list:
    """GET JSON from a URL with proper headers."""
    import urllib.request

    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("User-Agent", _UA)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def fetch_polymarket_markets(n: int = 30) -> list[dict]:
    """Fetch n active Polymarket markets from the Gamma API, sorted by 24h volume.

    Returns a list of raw Gamma API market dicts.  Each dict includes at minimum:
    - conditionId: str
    - question: str
    - outcomePrices: JSON-encoded list of price strings, e.g. '["0.65", "0.35"]'
    - volume24hr: float (USD)
    - volume: float (total USD)
    - endDate: ISO date string
    - clobTokenIds: JSON-encoded list of token ID strings
    """
    try:
        # Fetch up to 200 markets sorted by 24h volume
        data = _get(f"{_GAMMA_API}/markets?active=true&closed=false&limit=200&order=volume24hr&ascending=false")
        all_markets = data if isinstance(data, list) else data.get("markets", [])

        # Keep only markets with valid condition IDs and CLOB tokens
        eligible = [m for m in all_markets if m.get("conditionId") and m.get("clobTokenIds")]
        if not eligible:
            eligible = [m for m in all_markets if m.get("conditionId")]
        if not eligible:
            return []

        # Sample randomly from the top 100 by volume (more active = better for live testing)
        pool = eligible[:100] if len(eligible) >= 100 else eligible
        return random.sample(pool, min(n, len(pool)))
    except Exception as exc:
        print(f"  Warning: Could not fetch markets from Gamma API: {exc}")
        return []


def fetch_random_polymarket_condition() -> str | None:
    """Fetch a single random active market condition ID (legacy helper)."""
    markets = fetch_polymarket_markets(n=1)
    if not markets:
        return None
    m = markets[0]
    title = m.get("question", "")
    if title:
        print(f"  -> {title}")
    return m.get("conditionId")


class PolymarketLiveFeed:
    """Async live feed that streams Polymarket trades via WebSocket.

    Usage::

        markets = fetch_polymarket_markets(30)
        condition_ids = [m["conditionId"] for m in markets]
        feed = PolymarketLiveFeed(condition_ids=condition_ids, gamma_data=markets)
        async for trade in feed.trades():
            ...

    ``condition_ids`` are the Polymarket condition IDs for the markets you
    want to track.  Pass ``gamma_data`` (from ``fetch_polymarket_markets``) to
    pre-populate display metadata (prices, volumes, close dates).
    """

    def __init__(self, condition_ids: list[str], gamma_data: list[dict] | None = None):
        if websockets is None:
            raise ImportError("Install websockets: pip install websockets")

        self.condition_ids = condition_ids
        self._markets: dict[str, MarketInfo] = {}
        # Maps token_id -> (condition_id, outcome_index)
        self._token_map: dict[str, tuple[str, int]] = {}
        # Raw Gamma API data keyed by conditionId for rich display
        self._gamma_data: dict[str, dict] = {}
        if gamma_data:
            for m in gamma_data:
                cid = m.get("conditionId", "")
                if cid:
                    self._gamma_data[cid] = m

    def _fetch_market_clob(self, cid: str) -> None:
        """Fetch a single market's metadata from the CLOB API."""
        try:
            raw = _get(f"{_CLOB_API}/markets/{cid}")
        except Exception:
            return
        if not isinstance(raw, dict):
            return
        data: dict = raw

        condition_id = data.get("condition_id", cid)
        tokens = data.get("tokens", [])
        for tok in tokens:
            token_id = str(tok["token_id"])
            outcome = tok.get("outcome", "")
            idx = 0 if outcome == "Yes" else 1
            self._token_map[token_id] = (condition_id, idx)

        closed = data.get("closed", False)
        active = data.get("active", True)
        status = MarketStatus.OPEN
        if closed or not active:
            status = MarketStatus.CLOSED

        end_date = None
        if data.get("end_date_iso"):
            try:
                end_date = datetime.fromisoformat(data["end_date_iso"].replace("Z", "+00:00"))
            except Exception:
                pass

        # Prefer the question from Gamma data if available (often richer)
        gamma = self._gamma_data.get(cid, {})
        title = data.get("question") or gamma.get("question") or condition_id

        self._markets[condition_id] = MarketInfo(
            market_id=condition_id,
            platform=Platform.POLYMARKET,
            title=title,
            open_time=None,
            close_time=end_date,
            result=None,
            status=status,
        )

    def markets(self) -> dict[str, MarketInfo]:
        """Return cached market metadata (call connect() first)."""
        return dict(self._markets)

    def get_market_summary_rows(self) -> list[dict]:
        """Return display rows with title, current YES price, volume, and close date.

        Each row is a dict with keys:
        - condition_id, title, yes_price (float|None), volume_24h (float|None),
          volume_total (float|None), end_date (str|None), liquidity (float|None)
        """
        rows = []
        for cid in self.condition_ids:
            extra = self._gamma_data.get(cid, {})
            info = self._markets.get(cid)
            title = (info.title if info else None) or extra.get("question", cid[:24])

            yes_price = None
            try:
                prices_raw = extra.get("outcomePrices", "[]")
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else list(prices_raw)
                if prices:
                    yes_price = float(prices[0])
            except Exception:
                pass

            end_raw = extra.get("endDate") or extra.get("endDateIso", "")
            end_date = end_raw[:10] if end_raw else None

            rows.append(
                {
                    "condition_id": cid,
                    "title": title,
                    "yes_price": yes_price,
                    "volume_24h": extra.get("volume24hr") or extra.get("oneDayVolume"),
                    "volume_total": extra.get("volume"),
                    "end_date": end_date,
                    "liquidity": extra.get("liquidity"),
                }
            )
        return rows

    async def connect(self) -> None:
        """Fetch market metadata from the CLOB API before streaming."""
        for cid in self.condition_ids:
            self._fetch_market_clob(cid)

    async def trades(self) -> AsyncIterator[TradeEvent]:
        """Connect to WebSocket and yield TradeEvent objects for live trades."""
        if not self._markets:
            await self.connect()

        asset_ids = list(self._token_map.keys())
        if not asset_ids:
            raise ValueError(
                "No token IDs found for the given condition_ids. "
                "Check that the condition IDs are valid and have CLOB tokens."
            )

        async for trade in self._stream_with_reconnect(asset_ids):
            yield trade

    async def _stream_with_reconnect(self, asset_ids: list[str]) -> AsyncIterator[TradeEvent]:
        """Stream trades with automatic reconnection on disconnect."""
        while True:
            try:
                async with websockets.connect(
                    _WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    subscribe_msg = {
                        "type": "subscribe",
                        "assets_ids": asset_ids,
                    }
                    await ws.send(json.dumps(subscribe_msg))

                    async for raw in ws:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]

                        for msg in msgs:
                            event_type = msg.get("event_type", "")

                            if event_type == "last_trade_price":
                                trade = self._parse_trade(msg)
                                if trade is not None:
                                    yield trade
                            elif event_type == "price_change":
                                ltp = msg.get("last_trade_price")
                                if ltp is not None:
                                    trade = self._parse_trade(msg)
                                    if trade is not None:
                                        yield trade
                            # book snapshots are silently ignored

            except websockets.exceptions.ConnectionClosed:
                await asyncio.sleep(2)
                continue
            except Exception:
                await asyncio.sleep(5)
                continue

    def _parse_trade(self, msg: dict) -> TradeEvent | None:
        """Parse a WebSocket message into a TradeEvent."""
        asset_id = msg.get("asset_id", "")
        token_info = self._token_map.get(asset_id)
        if token_info is None:
            return None

        condition_id, outcome_index = token_info
        if condition_id not in self._markets:
            return None

        price_val = msg.get("price") or msg.get("last_trade_price")
        if price_val is None:
            return None
        price = float(price_val)
        if price <= 0:
            return None

        size = float(msg.get("size", msg.get("quantity", 1)))

        if outcome_index == 0:
            yes_price = price
            no_price = 1.0 - price
            taker_side = Side.YES
        else:
            no_price = price
            yes_price = 1.0 - price
            taker_side = Side.NO

        ts_raw = msg.get("timestamp", "")
        try:
            if ts_raw:
                ts_val = str(ts_raw)
                if ts_val.isdigit():
                    timestamp = datetime.fromtimestamp(int(ts_val) / 1000, tz=timezone.utc)
                else:
                    timestamp = datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
            else:
                timestamp = datetime.now(timezone.utc)
        except Exception:
            timestamp = datetime.now(timezone.utc)

        return TradeEvent(
            timestamp=timestamp,
            market_id=condition_id,
            platform=Platform.POLYMARKET,
            yes_price=yes_price,
            no_price=no_price,
            quantity=size,
            taker_side=taker_side,
            raw_id=msg.get("id"),
        )
