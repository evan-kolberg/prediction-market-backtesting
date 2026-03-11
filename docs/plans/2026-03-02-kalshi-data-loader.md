# KalshiDataLoader Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement `KalshiDataLoader` in `nautilus_trader/adapters/kalshi/loaders.py` that fetches historical trade and bar (candlestick) data from the public Kalshi REST API and exposes it as `TradeTick` and `Bar` lists suitable for writing to a `ParquetDataCatalog`.

**Architecture:** Mirrors `PolymarketDataLoader` — pure Python, async, uses `nautilus_pyo3.HttpClient` with rate limiting. Provides a `from_market_ticker(ticker)` factory classmethod, `load_trades()` and `load_bars()` convenience methods, and separate lower-level `fetch_*` / `parse_*` methods. Historical endpoints are public (no auth required).

**Tech Stack:** Python 3.12, `nautilus_pyo3.HttpClient`, `msgspec`, `pandas`, `nautilus_trader.model` types (`TradeTick`, `Bar`, `BarType`, `BarSpecification`, `BinaryOption`).

**GitHub Issue:** https://github.com/ben-gramling/nautilus_pm/issues/2

**Reference files:**
- `nautilus_trader/adapters/polymarket/loaders.py` — primary pattern
- `nautilus_trader/adapters/kalshi/providers.py` — `_market_to_instrument` logic + `KALSHI_REST_BASE`
- `nautilus_trader/adapters/kalshi/config.py` — `KalshiDataClientConfig`
- `docs/plans/2026-03-01-kalshi-adapter-design.md` — API models and field docs

---

### Task 1: Scaffold `loaders.py` — class shell + HTTP client

**Files:**
- Create: `nautilus_trader/adapters/kalshi/loaders.py`
- Test: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Kalshi API base URL:** `https://api.elections.kalshi.com/trade-api/v2` (matches `KALSHI_REST_BASE` in `providers.py`)

**Step 1: Write the failing test**

```python
# tests/unit_tests/adapters/kalshi/test_loaders.py
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

from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.model.instruments import BinaryOption


def make_instrument() -> BinaryOption:
    """Return a minimal BinaryOption for testing."""
    import decimal
    from nautilus_trader.model.enums import AssetClass
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    from nautilus_trader.model.objects import Currency, Price, Quantity

    return BinaryOption(
        instrument_id=InstrumentId(Symbol("KXBTC-25MAR15-B100000"), Venue("KALSHI")),
        raw_symbol=Symbol("KXBTC-25MAR15-B100000"),
        asset_class=AssetClass.ALTERNATIVE,
        currency=Currency.from_str("USD"),
        activation_ns=0,
        expiration_ns=0,
        price_precision=4,
        size_precision=2,
        price_increment=Price.from_str("0.0001"),
        size_increment=Quantity.from_str("0.01"),
        maker_fee=decimal.Decimal("0"),
        taker_fee=decimal.Decimal("0"),
        outcome="Yes",
        description="Test market",
        ts_event=0,
        ts_init=0,
    )


def test_init_stores_instrument():
    instrument = make_instrument()
    http_client = MagicMock()
    loader = KalshiDataLoader(instrument=instrument, http_client=http_client)
    assert loader.instrument is instrument


def test_init_creates_default_http_client():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument)
    # If no http_client supplied, one is created
    assert loader._http_client is not None
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: `ImportError` — `KalshiDataLoader` not found.

**Step 3: Write minimal implementation**

```python
# nautilus_trader/adapters/kalshi/loaders.py
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
"""
Provides a data loader for historical Kalshi prediction market data.
"""

from __future__ import annotations

from nautilus_trader.adapters.kalshi.providers import KALSHI_REST_BASE
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.instruments import BinaryOption

KALSHI_HTTP_RATE_LIMIT_RPS = 20  # Basic tier


class KalshiDataLoader:
    """
    Provides a data loader for historical Kalshi market data.

    This loader fetches data from the public Kalshi REST API:
    - ``GET /markets/{ticker}`` — instrument discovery
    - ``GET /historical/markets/{ticker}/trades`` — historical trades (cursor-paginated)
    - ``GET /historical/markets/{ticker}/candlesticks`` — OHLCV bars

    Historical endpoints are public and require no authentication.

    If no ``http_client`` is provided, the loader creates one with a default
    rate limit of 20 requests per second (Kalshi Basic tier).

    Parameters
    ----------
    instrument : BinaryOption
        The binary option instrument to load data for.
    http_client : nautilus_pyo3.HttpClient, optional
        HTTP client to use for requests. If not provided, a new client is created.
    """

    def __init__(
        self,
        instrument: BinaryOption,
        http_client: nautilus_pyo3.HttpClient | None = None,
    ) -> None:
        self._instrument = instrument
        self._http_client = http_client or self._create_http_client()

    @staticmethod
    def _create_http_client() -> nautilus_pyo3.HttpClient:
        return nautilus_pyo3.HttpClient(
            default_quota=nautilus_pyo3.Quota.rate_per_second(KALSHI_HTTP_RATE_LIMIT_RPS),
        )

    @property
    def instrument(self) -> BinaryOption:
        """Return the instrument for this loader."""
        return self._instrument
```

**Step 4: Run test to verify it passes**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_init_stores_instrument tests/unit_tests/adapters/kalshi/test_loaders.py::test_init_creates_default_http_client -v
```
Expected: PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): scaffold KalshiDataLoader with HTTP client"
```

---

### Task 2: `from_market_ticker` factory classmethod

**Files:**
- Modify: `nautilus_trader/adapters/kalshi/loaders.py`
- Modify: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Kalshi API endpoint:** `GET /markets/{ticker}` returns a JSON object with a `market` key containing a single market dict. If 404, raise `ValueError`.

**Step 1: Write the failing test**

Add to `test_loaders.py`:

```python
import msgspec
from unittest.mock import AsyncMock, MagicMock


def make_market_dict(ticker: str = "KXBTC-25MAR15-B100000") -> dict:
    return {
        "ticker": ticker,
        "title": "BTC above 100k on March 15?",
        "open_time": "2025-01-01T00:00:00Z",
        "close_time": "2025-03-15T00:00:00Z",
        "latest_expiration_time": "2025-03-15T00:00:00Z",
    }


def make_mock_response(body: dict | list, status: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status = status
    mock.body = msgspec.json.encode(body)
    return mock


@pytest.mark.asyncio
async def test_from_market_ticker_returns_loader():
    ticker = "KXBTC-25MAR15-B100000"
    market = make_market_dict(ticker)
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"market": market})
    )

    loader = await KalshiDataLoader.from_market_ticker(ticker, http_client=mock_client)

    assert isinstance(loader, KalshiDataLoader)
    assert loader.instrument.id.symbol.value == ticker
    mock_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_from_market_ticker_raises_on_404():
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        return_value=make_mock_response({}, status=404)
    )

    with pytest.raises(ValueError, match="not found"):
        await KalshiDataLoader.from_market_ticker("NONEXISTENT", http_client=mock_client)
```

**Step 2: Run test to verify it fails**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_from_market_ticker_returns_loader -v
```
Expected: `AttributeError` — `from_market_ticker` not defined.

**Step 3: Write minimal implementation**

Add to `loaders.py` (after imports, add `msgspec` and `Any`; add classmethod to class):

New imports needed:
```python
from typing import Any
import msgspec
```

Add a module-level helper (mirrors `_market_to_instrument` from providers.py but as a standalone function):

```python
def _market_dict_to_instrument(market: dict[str, Any]) -> BinaryOption:
    """Convert a Kalshi market dict to a NautilusTrader BinaryOption."""
    import decimal
    from datetime import datetime

    from nautilus_trader.core.datetime import dt_to_unix_nanos
    from nautilus_trader.model.enums import AssetClass
    from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
    from nautilus_trader.model.objects import Currency, Price, Quantity

    ticker = market["ticker"]

    def parse_ts(s: str | None) -> int:
        if not s:
            return 0
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt_to_unix_nanos(dt)

    return BinaryOption(
        instrument_id=InstrumentId(Symbol(ticker), Venue("KALSHI")),
        raw_symbol=Symbol(ticker),
        asset_class=AssetClass.ALTERNATIVE,
        currency=Currency.from_str("USD"),
        activation_ns=parse_ts(market.get("open_time")),
        expiration_ns=parse_ts(
            market.get("close_time") or market.get("latest_expiration_time")
        ),
        price_precision=4,
        size_precision=2,
        price_increment=Price.from_str("0.0001"),
        size_increment=Quantity.from_str("0.01"),
        maker_fee=decimal.Decimal("0"),
        taker_fee=decimal.Decimal("0"),
        outcome="Yes",
        description=market.get("title"),
        ts_event=0,
        ts_init=0,
    )
```

Add classmethod to `KalshiDataLoader`:

```python
    @classmethod
    async def from_market_ticker(
        cls,
        ticker: str,
        http_client: nautilus_pyo3.HttpClient | None = None,
    ) -> KalshiDataLoader:
        """
        Create a loader by fetching market data for the given ticker.

        Parameters
        ----------
        ticker : str
            The Kalshi market ticker, e.g. ``"KXBTC-25MAR15-B100000"``.
        http_client : nautilus_pyo3.HttpClient, optional
            HTTP client to use. If not provided, a new client is created.

        Returns
        -------
        KalshiDataLoader

        Raises
        ------
        ValueError
            If the market ticker is not found.
        RuntimeError
            If the HTTP request fails.
        """
        client = http_client or cls._create_http_client()
        response = await client.get(url=f"{KALSHI_REST_BASE}/markets/{ticker}")

        if response.status == 404:
            raise ValueError(f"Market ticker '{ticker}' not found")
        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: "
                f"{response.body.decode('utf-8')}",
            )

        data = msgspec.json.decode(response.body)
        market = data["market"]
        instrument = _market_dict_to_instrument(market)

        return cls(instrument=instrument, http_client=client)
```

**Step 4: Run tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: all PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): add from_market_ticker factory to KalshiDataLoader"
```

---

### Task 3: `fetch_trades` with cursor-based pagination

**Files:**
- Modify: `nautilus_trader/adapters/kalshi/loaders.py`
- Modify: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Kalshi API:** `GET /historical/markets/{ticker}/trades`
- Query params: `min_ts` (unix seconds), `max_ts` (unix seconds), `cursor` (for next page), `limit` (max 1000)
- Response: `{"trades": [...], "cursor": "<next_cursor_or_empty>"}`
- Each trade: `{"ts": 1234567890, "yes_price": "0.4200", "no_price": "0.5800", "count": "10.00", "taker_side": "yes"}`
- Note: field is `count` (number of contracts), not `volume`. `taker_side` is `"yes"` or `"no"`.

**Step 1: Write the failing test**

Add to `test_loaders.py`:

```python
def make_trade_dict(ts: int = 1700000000, yes_price: str = "0.4200", count: str = "10.00", taker_side: str = "yes") -> dict:
    return {
        "ts": ts,
        "yes_price": yes_price,
        "no_price": str(round(1 - float(yes_price), 4)),
        "count": count,
        "taker_side": taker_side,
    }


@pytest.mark.asyncio
async def test_fetch_trades_single_page():
    instrument = make_instrument()
    mock_client = MagicMock()
    # Single page — empty cursor means done
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"trades": [make_trade_dict()], "cursor": ""})
    )
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    trades = await loader.fetch_trades()

    assert len(trades) == 1
    assert trades[0]["ts"] == 1700000000
    mock_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_fetch_trades_paginates():
    instrument = make_instrument()
    mock_client = MagicMock()
    page1 = make_mock_response({"trades": [make_trade_dict(ts=1)], "cursor": "abc"})
    page2 = make_mock_response({"trades": [make_trade_dict(ts=2)], "cursor": ""})
    mock_client.get = AsyncMock(side_effect=[page1, page2])
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    trades = await loader.fetch_trades()

    assert len(trades) == 2
    assert mock_client.get.call_count == 2
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_fetch_trades_single_page -v
```
Expected: `AttributeError` — `fetch_trades` not defined.

**Step 3: Write minimal implementation**

Add to `KalshiDataLoader`:

```python
    async def fetch_trades(
        self,
        min_ts: int | None = None,
        max_ts: int | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Fetch historical trades from the Kalshi API.

        Automatically paginates using cursor-based pagination until all
        trades are retrieved.

        Parameters
        ----------
        min_ts : int, optional
            Minimum Unix timestamp in seconds (inclusive).
        max_ts : int, optional
            Maximum Unix timestamp in seconds (inclusive).
        limit : int, default 1000
            Number of trades per page (Kalshi maximum is 1000).

        Returns
        -------
        list[dict[str, Any]]
            Raw trade dicts as returned by the Kalshi API.
        """
        ticker = self._instrument.id.symbol.value
        all_trades: list[dict[str, Any]] = []
        cursor: str | None = None

        while True:
            params: dict[str, Any] = {"limit": str(limit)}
            if min_ts is not None:
                params["min_ts"] = str(min_ts)
            if max_ts is not None:
                params["max_ts"] = str(max_ts)
            if cursor:
                params["cursor"] = cursor

            response = await self._http_client.get(
                url=f"{KALSHI_REST_BASE}/historical/markets/{ticker}/trades",
                params=params,
            )

            if response.status != 200:
                raise RuntimeError(
                    f"HTTP request failed with status {response.status}: "
                    f"{response.body.decode('utf-8')}",
                )

            data = msgspec.json.decode(response.body)
            page_trades = data.get("trades", [])
            all_trades.extend(page_trades)

            cursor = data.get("cursor") or None
            if not cursor or not page_trades:
                break

        return all_trades
```

**Step 4: Run tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: all PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): add fetch_trades with cursor pagination"
```

---

### Task 4: `parse_trades` → `TradeTick` objects

**Files:**
- Modify: `nautilus_trader/adapters/kalshi/loaders.py`
- Modify: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Field mapping:**
- `ts` (Unix seconds) → `secs_to_nanos(ts)` for `ts_event` / `ts_init`
- `yes_price` (dollar string e.g. `"0.4200"`) → `instrument.make_price(yes_price)`
- `count` (contract count string e.g. `"10.00"`) → `instrument.make_qty(count)`
- `taker_side == "yes"` → `AggressorSide.BUYER`; `"no"` → `AggressorSide.SELLER`; otherwise `NO_AGGRESSOR`
- `trade_id`: Kalshi trades don't have a unique ID field in the public API response; use `f"{ticker}_{ts}_{yes_price}_{count}"` truncated to 36 chars if needed. Note: If a `trade_id` or `id` field exists in real API responses, use that instead.

**Step 1: Write the failing test**

Add to `test_loaders.py`:

```python
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide


def test_parse_trades_returns_trade_ticks():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, http_client=MagicMock())

    raw = [
        make_trade_dict(ts=1700000000, yes_price="0.4200", count="10.00", taker_side="yes"),
        make_trade_dict(ts=1700000001, yes_price="0.5000", count="5.00", taker_side="no"),
    ]
    ticks = loader.parse_trades(raw)

    assert len(ticks) == 2
    assert isinstance(ticks[0], TradeTick)
    assert ticks[0].aggressor_side == AggressorSide.BUYER
    assert ticks[1].aggressor_side == AggressorSide.SELLER
    # Timestamp converted: 1700000000 seconds → nanoseconds
    assert ticks[0].ts_event == 1700000000 * 1_000_000_000


def test_parse_trades_unknown_side_gives_no_aggressor():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, http_client=MagicMock())

    raw = [make_trade_dict(taker_side="unknown")]
    ticks = loader.parse_trades(raw)

    assert ticks[0].aggressor_side == AggressorSide.NO_AGGRESSOR
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_parse_trades_returns_trade_ticks -v
```
Expected: `AttributeError` — `parse_trades` not defined.

**Step 3: Write minimal implementation**

New imports for `loaders.py`:
```python
from nautilus_trader.core.datetime import secs_to_nanos
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId
```

Add method to `KalshiDataLoader`:

```python
    def parse_trades(
        self,
        trades_data: list[dict[str, Any]],
    ) -> list[TradeTick]:
        """
        Parse raw Kalshi trade dicts into TradeTick objects.

        Parameters
        ----------
        trades_data : list[dict[str, Any]]
            Raw trade dicts from the Kalshi historical trades API.

        Returns
        -------
        list[TradeTick]
        """
        ticker = self._instrument.id.symbol.value
        instrument_id = self._instrument.id
        make_price = self._instrument.make_price
        make_qty = self._instrument.make_qty
        trades: list[TradeTick] = []

        for trade in trades_data:
            ts_event = secs_to_nanos(trade["ts"])
            taker_side = trade.get("taker_side", "")
            if taker_side == "yes":
                aggressor_side = AggressorSide.BUYER
            elif taker_side == "no":
                aggressor_side = AggressorSide.SELLER
            else:
                aggressor_side = AggressorSide.NO_AGGRESSOR

            # Build a deterministic trade_id from available fields
            raw_id = f"{ticker}_{trade['ts']}_{trade['yes_price']}_{trade['count']}"
            trade_id = TradeId(raw_id[:36])

            trades.append(
                TradeTick(
                    instrument_id=instrument_id,
                    price=make_price(trade["yes_price"]),
                    size=make_qty(trade["count"]),
                    aggressor_side=aggressor_side,
                    trade_id=trade_id,
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )

        return trades
```

**Step 4: Run tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: all PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): add parse_trades returning TradeTick objects"
```

---

### Task 5: `load_trades` convenience method

**Files:**
- Modify: `nautilus_trader/adapters/kalshi/loaders.py`
- Modify: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Step 1: Write the failing test**

Add to `test_loaders.py`:
```python
import pandas as pd


@pytest.mark.asyncio
async def test_load_trades_filters_by_time_range():
    instrument = make_instrument()
    mock_client = MagicMock()
    raw = [
        make_trade_dict(ts=1000),  # before start
        make_trade_dict(ts=2000),  # in range
        make_trade_dict(ts=3000),  # after end
    ]
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"trades": raw, "cursor": ""})
    )
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    start = pd.Timestamp("1970-01-01 00:16:40", tz="UTC")  # ts=1000
    end   = pd.Timestamp("1970-01-01 00:33:20", tz="UTC")  # ts=2000

    ticks = await loader.load_trades(start=start, end=end)

    ts_values = [t.ts_event // 1_000_000_000 for t in ticks]
    assert all(1000 <= ts <= 2000 for ts in ts_values)


@pytest.mark.asyncio
async def test_load_trades_sorted_chronologically():
    instrument = make_instrument()
    mock_client = MagicMock()
    raw = [make_trade_dict(ts=3000), make_trade_dict(ts=1000), make_trade_dict(ts=2000)]
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"trades": raw, "cursor": ""})
    )
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    ticks = await loader.load_trades()

    ts_values = [t.ts_event for t in ticks]
    assert ts_values == sorted(ts_values)
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_load_trades_filters_by_time_range -v
```
Expected: `AttributeError` — `load_trades` not defined.

**Step 3: Write minimal implementation**

Add to loaders.py imports: `import pandas as pd`

Add method to `KalshiDataLoader`:

```python
    async def load_trades(
        self,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> list[TradeTick]:
        """
        Load, parse, and sort trade ticks.

        Applies optional client-side time filtering after fetching.

        Parameters
        ----------
        start : pd.Timestamp, optional
            Inclusive lower bound (timezone-aware). If ``None``, no lower bound.
        end : pd.Timestamp, optional
            Inclusive upper bound (timezone-aware). If ``None``, no upper bound.

        Returns
        -------
        list[TradeTick]
            Trade ticks sorted chronologically.
        """
        min_ts = int(start.timestamp()) if start is not None else None
        max_ts = int(end.timestamp()) if end is not None else None

        raw_trades = await self.fetch_trades(min_ts=min_ts, max_ts=max_ts)

        # Client-side filter (API may return boundary-inclusive extras)
        if min_ts is not None:
            raw_trades = [t for t in raw_trades if t["ts"] >= min_ts]
        if max_ts is not None:
            raw_trades = [t for t in raw_trades if t["ts"] <= max_ts]

        raw_trades.sort(key=lambda t: t["ts"])

        return self.parse_trades(raw_trades)
```

**Step 4: Run tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: all PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): add load_trades convenience method"
```

---

### Task 6: `fetch_candlesticks`

**Files:**
- Modify: `nautilus_trader/adapters/kalshi/loaders.py`
- Modify: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Kalshi API:** `GET /historical/markets/{ticker}/candlesticks`
- Query params: `start_ts` (unix seconds), `end_ts` (unix seconds), `period_interval` (int: 1=Minutes1, 60=Hours1, 1440=Days1)
- Response: `{"candlesticks": [...]}`
- Not paginated (returns all candles for the time range)
- Each candlestick: `{"end_period_ts": 1700000060, "yes_bid": {...}, "yes_ask": {...}, "price": {"open": "0.42", "high": "0.45", "low": "0.40", "close": "0.43", "mean": "0.42"}, "volume": "100.00", "open_interest": "500.00"}`

Interval mapping:
- `"Minutes1"` → `1`
- `"Hours1"` → `60`
- `"Days1"` → `1440`

**Step 1: Write the failing test**

Add to `test_loaders.py`:

```python
def make_candle_dict(end_ts: int = 1700000060) -> dict:
    return {
        "end_period_ts": end_ts,
        "yes_bid": {"open": "0.41", "high": "0.44", "low": "0.40", "close": "0.42"},
        "yes_ask": {"open": "0.43", "high": "0.46", "low": "0.42", "close": "0.44"},
        "price": {"open": "0.42", "high": "0.45", "low": "0.41", "close": "0.43", "mean": "0.42"},
        "volume": "100.00",
        "open_interest": "500.00",
    }


@pytest.mark.asyncio
async def test_fetch_candlesticks_returns_raw_list():
    instrument = make_instrument()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"candlesticks": [make_candle_dict()]})
    )
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    candles = await loader.fetch_candlesticks(start_ts=1699999000, end_ts=1700000100)

    assert len(candles) == 1
    assert candles[0]["end_period_ts"] == 1700000060
    # Verify interval param sent as "1" (Minutes1 default)
    call_kwargs = mock_client.get.call_args
    assert call_kwargs.kwargs["params"]["period_interval"] == "1"


@pytest.mark.asyncio
async def test_fetch_candlesticks_hours_interval():
    instrument = make_instrument()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"candlesticks": []})
    )
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    await loader.fetch_candlesticks(start_ts=0, end_ts=1, interval="Hours1")

    call_kwargs = mock_client.get.call_args
    assert call_kwargs.kwargs["params"]["period_interval"] == "60"
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_fetch_candlesticks_returns_raw_list -v
```
Expected: `AttributeError` — `fetch_candlesticks` not defined.

**Step 3: Write minimal implementation**

Add to `KalshiDataLoader`:

```python
    _INTERVAL_MAP: dict[str, int] = {
        "Minutes1": 1,
        "Hours1": 60,
        "Days1": 1440,
    }

    async def fetch_candlesticks(
        self,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str = "Minutes1",
    ) -> list[dict[str, Any]]:
        """
        Fetch historical OHLCV candlesticks from the Kalshi API.

        Parameters
        ----------
        start_ts : int, optional
            Start Unix timestamp in seconds.
        end_ts : int, optional
            End Unix timestamp in seconds.
        interval : str, default "Minutes1"
            Candlestick interval. One of ``"Minutes1"``, ``"Hours1"``, ``"Days1"``.

        Returns
        -------
        list[dict[str, Any]]
            Raw candlestick dicts as returned by the Kalshi API.

        Raises
        ------
        ValueError
            If ``interval`` is not a recognized value.
        """
        if interval not in self._INTERVAL_MAP:
            raise ValueError(
                f"Invalid interval '{interval}'. Must be one of: "
                f"{list(self._INTERVAL_MAP.keys())}",
            )

        ticker = self._instrument.id.symbol.value
        params: dict[str, Any] = {
            "period_interval": str(self._INTERVAL_MAP[interval]),
        }
        if start_ts is not None:
            params["start_ts"] = str(start_ts)
        if end_ts is not None:
            params["end_ts"] = str(end_ts)

        response = await self._http_client.get(
            url=f"{KALSHI_REST_BASE}/historical/markets/{ticker}/candlesticks",
            params=params,
        )

        if response.status != 200:
            raise RuntimeError(
                f"HTTP request failed with status {response.status}: "
                f"{response.body.decode('utf-8')}",
            )

        data = msgspec.json.decode(response.body)
        return data.get("candlesticks", [])
```

**Step 4: Run tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: all PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): add fetch_candlesticks with interval mapping"
```

---

### Task 7: `parse_candlesticks` → `Bar` objects

**Files:**
- Modify: `nautilus_trader/adapters/kalshi/loaders.py`
- Modify: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Field mapping:**
- `end_period_ts` (Unix seconds) → `secs_to_nanos(end_period_ts)` for `ts_event` / `ts_init`
- `price.open/high/low/close` (dollar strings) → `instrument.make_price(...)`
- `volume` (contract count string) → `instrument.make_qty(...)`
- `BarType(instrument_id, BarSpecification(step, aggregation, PriceType.LAST), AggregationSource.EXTERNAL)`
- Interval → BarAggregation: `"Minutes1"` → `BarAggregation.MINUTE`, `"Hours1"` → `BarAggregation.HOUR`, `"Days1"` → `BarAggregation.DAY`

**Step 1: Write the failing test**

Add to `test_loaders.py`:

```python
from nautilus_trader.model.data import Bar


def test_parse_candlesticks_returns_bars():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, http_client=MagicMock())

    raw = [make_candle_dict(end_ts=1700000060)]
    bars = loader.parse_candlesticks(raw, interval="Minutes1")

    assert len(bars) == 1
    assert isinstance(bars[0], Bar)
    assert bars[0].ts_event == 1700000060 * 1_000_000_000
    # price.open = "0.42" → price with 4 decimals
    assert str(bars[0].open) == "0.4200"
    assert str(bars[0].close) == "0.4300"
    assert str(bars[0].volume) == "100.00"


def test_parse_candlesticks_invalid_interval_raises():
    instrument = make_instrument()
    loader = KalshiDataLoader(instrument=instrument, http_client=MagicMock())

    with pytest.raises(ValueError, match="Invalid interval"):
        loader.parse_candlesticks([], interval="Ticks1")
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_parse_candlesticks_returns_bars -v
```
Expected: `AttributeError` — `parse_candlesticks` not defined.

**Step 3: Write minimal implementation**

New imports for `loaders.py`:
```python
from nautilus_trader.model.data import Bar, BarSpecification, BarType
from nautilus_trader.model.enums import AggregationSource, BarAggregation, PriceType
```

Add class-level mapping and method:

```python
    _INTERVAL_TO_AGGREGATION: dict[str, BarAggregation] = {
        "Minutes1": BarAggregation.MINUTE,
        "Hours1": BarAggregation.HOUR,
        "Days1": BarAggregation.DAY,
    }

    def parse_candlesticks(
        self,
        candlesticks_data: list[dict[str, Any]],
        interval: str = "Minutes1",
    ) -> list[Bar]:
        """
        Parse raw Kalshi candlestick dicts into Bar objects.

        Parameters
        ----------
        candlesticks_data : list[dict[str, Any]]
            Raw candlestick dicts from the Kalshi API.
        interval : str, default "Minutes1"
            The candlestick interval used to construct the BarType.
            One of ``"Minutes1"``, ``"Hours1"``, ``"Days1"``.

        Returns
        -------
        list[Bar]

        Raises
        ------
        ValueError
            If ``interval`` is not a recognized value.
        """
        if interval not in self._INTERVAL_TO_AGGREGATION:
            raise ValueError(
                f"Invalid interval '{interval}'. Must be one of: "
                f"{list(self._INTERVAL_TO_AGGREGATION.keys())}",
            )

        aggregation = self._INTERVAL_TO_AGGREGATION[interval]
        bar_spec = BarSpecification(step=1, aggregation=aggregation, price_type=PriceType.LAST)
        bar_type = BarType(
            instrument_id=self._instrument.id,
            bar_spec=bar_spec,
            aggregation_source=AggregationSource.EXTERNAL,
        )
        make_price = self._instrument.make_price
        make_qty = self._instrument.make_qty
        bars: list[Bar] = []

        for candle in candlesticks_data:
            ts_event = secs_to_nanos(candle["end_period_ts"])
            price = candle["price"]
            bars.append(
                Bar(
                    bar_type=bar_type,
                    open=make_price(price["open"]),
                    high=make_price(price["high"]),
                    low=make_price(price["low"]),
                    close=make_price(price["close"]),
                    volume=make_qty(candle["volume"]),
                    ts_event=ts_event,
                    ts_init=ts_event,
                )
            )

        return bars
```

**Step 4: Run tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: all PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): add parse_candlesticks returning Bar objects"
```

---

### Task 8: `load_bars` convenience method

**Files:**
- Modify: `nautilus_trader/adapters/kalshi/loaders.py`
- Modify: `tests/unit_tests/adapters/kalshi/test_loaders.py`

**Step 1: Write the failing test**

Add to `test_loaders.py`:

```python
@pytest.mark.asyncio
async def test_load_bars_returns_sorted_bars():
    instrument = make_instrument()
    mock_client = MagicMock()
    candles = [make_candle_dict(end_ts=3000), make_candle_dict(end_ts=1000), make_candle_dict(end_ts=2000)]
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"candlesticks": candles})
    )
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    bars = await loader.load_bars()

    ts_values = [b.ts_event for b in bars]
    assert ts_values == sorted(ts_values)
    assert len(bars) == 3


@pytest.mark.asyncio
async def test_load_bars_passes_time_range():
    instrument = make_instrument()
    mock_client = MagicMock()
    mock_client.get = AsyncMock(
        return_value=make_mock_response({"candlesticks": []})
    )
    loader = KalshiDataLoader(instrument=instrument, http_client=mock_client)

    start = pd.Timestamp("2024-01-01", tz="UTC")
    end   = pd.Timestamp("2024-01-31", tz="UTC")
    await loader.load_bars(start=start, end=end, interval="Hours1")

    call_kwargs = mock_client.get.call_args
    params = call_kwargs.kwargs["params"]
    assert params["start_ts"] == str(int(start.timestamp()))
    assert params["end_ts"] == str(int(end.timestamp()))
    assert params["period_interval"] == "60"
```

**Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py::test_load_bars_returns_sorted_bars -v
```
Expected: `AttributeError` — `load_bars` not defined.

**Step 3: Write minimal implementation**

Add to `KalshiDataLoader`:

```python
    async def load_bars(
        self,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
        interval: str = "Minutes1",
    ) -> list[Bar]:
        """
        Load, parse, and sort bars (OHLCV candlesticks).

        Parameters
        ----------
        start : pd.Timestamp, optional
            Inclusive start time (timezone-aware).
        end : pd.Timestamp, optional
            Inclusive end time (timezone-aware).
        interval : str, default "Minutes1"
            Candlestick interval. One of ``"Minutes1"``, ``"Hours1"``, ``"Days1"``.

        Returns
        -------
        list[Bar]
            Bars sorted chronologically.
        """
        start_ts = int(start.timestamp()) if start is not None else None
        end_ts = int(end.timestamp()) if end is not None else None

        raw_candles = await self.fetch_candlesticks(
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval,
        )
        raw_candles.sort(key=lambda c: c["end_period_ts"])

        return self.parse_candlesticks(raw_candles, interval=interval)
```

**Step 4: Run tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/test_loaders.py -v
```
Expected: all PASS

**Step 5: Commit**

```bash
git add nautilus_trader/adapters/kalshi/loaders.py tests/unit_tests/adapters/kalshi/test_loaders.py
git commit -m "feat(kalshi): add load_bars convenience method"
```

---

### Task 9: Example script + linting

**Files:**
- Create: `examples/kalshitest/kalshi_data_load.py`
- Modify: `nautilus_trader/adapters/kalshi/loaders.py` (if ruff finds issues)

**Step 1: Write the example script**

```python
# examples/kalshitest/kalshi_data_load.py
"""
Example: Load historical Kalshi trade and bar data and write to a ParquetDataCatalog.

Usage:
    python examples/kalshitest/kalshi_data_load.py
"""

import asyncio

import pandas as pd

from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.persistence.catalog import ParquetDataCatalog


MARKET_TICKER = "KXBTC-25MAR15-B100000"
CATALOG_PATH = "./kalshi_catalog"


async def main() -> None:
    print(f"Loading data for market: {MARKET_TICKER}")

    loader = await KalshiDataLoader.from_market_ticker(MARKET_TICKER)
    print(f"Instrument: {loader.instrument}")

    # Load all available trades
    trades = await loader.load_trades()
    print(f"Loaded {len(trades)} trades")

    # Load hourly bars for the past 30 days
    end = pd.Timestamp.utcnow()
    start = end - pd.Timedelta(days=30)
    bars = await loader.load_bars(start=start, end=end, interval="Hours1")
    print(f"Loaded {len(bars)} hourly bars")

    # Write to Parquet catalog
    catalog = ParquetDataCatalog(CATALOG_PATH)
    if trades:
        catalog.write_data(trades)
        print(f"Wrote trades to {CATALOG_PATH}")
    if bars:
        catalog.write_data(bars)
        print(f"Wrote bars to {CATALOG_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
```

**Step 2: Run ruff on the new files**

```bash
python -m ruff check nautilus_trader/adapters/kalshi/loaders.py examples/kalshitest/kalshi_data_load.py --fix
python -m ruff format nautilus_trader/adapters/kalshi/loaders.py examples/kalshitest/kalshi_data_load.py
```

Fix any issues reported.

**Step 3: Run full test suite for the Kalshi adapter**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/ -v
```
Expected: all PASS

**Step 4: Commit**

```bash
git add examples/kalshitest/kalshi_data_load.py nautilus_trader/adapters/kalshi/loaders.py
git commit -m "feat(kalshi): add KalshiDataLoader example script and fix linting"
```

---

### Task 10: Final validation and GitHub issue update

**Step 1: Run full linting**

```bash
make check-code
```

Fix any remaining issues.

**Step 2: Run all Kalshi tests**

```bash
python -m pytest tests/unit_tests/adapters/kalshi/ -v
```

Expected: all PASS

**Step 3: Update GitHub issue #2**

Add a comment to https://github.com/ben-gramling/nautilus_pm/issues/2 summarizing:
- Files created/modified
- Test count and results
- Any API field deviations discovered during implementation

**Step 4: Close issue as complete**

Add label `status: complete / unmerged` to issue #2.

**Step 5: Final commit if any cleanup**

```bash
git add -p
git commit -m "chore(kalshi): final linting cleanup for KalshiDataLoader"
```
