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

import decimal
import logging
import math
from datetime import datetime

from prediction_market_extensions.adapters.kalshi.config import KalshiDataClientConfig
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


_log = logging.getLogger(__name__)

KALSHI_REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"

# Default Kalshi taker fee rate: 7% of expected earnings.
# Some markets may have different rates or fee waivers.
# See: https://kalshi.com/docs/kalshi-fee-schedule.pdf
KALSHI_TAKER_FEE_RATE = decimal.Decimal("0.07")

# Kalshi maker fee rate.  Most markets have zero maker fees; markets that
# do charge a maker fee are noted in the fee schedule PDF.  Set to zero by
# default; override per-market via ``fee_waiver_expiration_time`` check.
KALSHI_MAKER_FEE_RATE = decimal.Decimal(0)


def calculate_kalshi_commission(
    quantity: decimal.Decimal, price: decimal.Decimal, fee_rate: decimal.Decimal = KALSHI_TAKER_FEE_RATE
) -> decimal.Decimal:
    """
    Calculate Kalshi transaction fee.

    Kalshi charges a variable percentage of the expected earnings on each
    contract, rounded **up** to the next cent::

        fee = ceil_to_cent(fee_rate x C x P x (1 - P))

    Where:
    - C = number of contracts (quantity)
    - P = contract price in USD (0 to 1)
    - fee_rate = 0.07 (7%) for the standard taker fee schedule

    The fee peaks at P = 0.50 and decreases symmetrically toward the
    extremes (P -> 0 or P -> 1).  At P = 0.50, the effective rate is
    ~1.75% of notional (= 0.07 x 0.25).

    References
    ----------
    https://kalshi.com/docs/kalshi-fee-schedule.pdf
    https://help.kalshi.com/en/articles/13823805-fees

    Parameters
    ----------
    quantity : Decimal
        The number of contracts.
    price : Decimal
        The contract price in USD (0 to 1).
    fee_rate : Decimal, default 0.07
        The fee rate applied to expected earnings.

    Returns
    -------
    Decimal
        The fee amount, rounded up to the nearest cent.

    """
    if fee_rate <= 0 or price <= 0 or price >= 1:
        return decimal.Decimal(0)
    raw_fee = float(fee_rate * quantity * price * (1 - price))
    # Round up to the next cent (Kalshi rounds up)
    return decimal.Decimal(str(math.ceil(raw_fee * 100) / 100))


def _market_dict_to_instrument(market: dict) -> BinaryOption:
    """Convert a Kalshi market dict to a NautilusTrader ``BinaryOption``."""
    ticker = market["ticker"]
    venue = Venue("KALSHI")
    instrument_id = InstrumentId(Symbol(ticker), venue)

    def parse_ts(s: str | None) -> int:
        if not s:
            return 0
        dt = datetime.fromisoformat(s)
        return dt_to_unix_nanos(dt)

    # Check if this market has a fee waiver that is still active
    fee_waiver = market.get("fee_waiver_expiration_time")
    if fee_waiver:
        try:
            waiver_dt = datetime.fromisoformat(fee_waiver)
            if waiver_dt > datetime.now(waiver_dt.tzinfo):
                taker_fee = decimal.Decimal(0)
            else:
                taker_fee = KALSHI_TAKER_FEE_RATE
        except (ValueError, TypeError):
            taker_fee = KALSHI_TAKER_FEE_RATE
    else:
        taker_fee = KALSHI_TAKER_FEE_RATE

    return BinaryOption(
        instrument_id=instrument_id,
        raw_symbol=Symbol(ticker),
        asset_class=AssetClass.ALTERNATIVE,
        currency=Currency.from_str("USD"),
        activation_ns=parse_ts(market.get("open_time")),
        expiration_ns=parse_ts(market.get("close_time") or market.get("latest_expiration_time")),
        price_precision=4,
        size_precision=2,
        price_increment=Price.from_str("0.0001"),
        size_increment=Quantity.from_str("0.01"),
        maker_fee=KALSHI_MAKER_FEE_RATE,
        taker_fee=taker_fee,
        outcome="Yes",
        description=market.get("title"),
        ts_event=0,
        ts_init=0,
        info=market,
    )


# Backward-compatibility alias used by loaders/examples.
market_dict_to_instrument = _market_dict_to_instrument


class _KalshiHttpClient:
    """
    Thin async HTTP client wrapper exposing a ``get_markets`` method.

    This wrapper exists so that the ``KalshiInstrumentProvider`` can be tested
    by mocking ``provider._http_client.get_markets`` without coupling the
    provider directly to httpx internals.  When the Rust-backed PyO3 HTTP
    client gains a ``get_markets`` binding this class can be replaced.

    Parameters
    ----------
    base_url : str
        The REST base URL (without trailing slash).
    """

    def __init__(self, base_url: str) -> None:
        self._base_url = base_url
        try:
            import httpx

            self._client = httpx.AsyncClient(base_url=base_url, timeout=60)
        except ImportError as exc:
            raise RuntimeError(
                "httpx is required for KalshiInstrumentProvider; install it with: pip install httpx"
            ) from exc

    async def get_markets(
        self, series_tickers: tuple[str, ...] = (), event_tickers: tuple[str, ...] = ()
    ) -> list[dict]:
        """
        Fetch all active markets from the Kalshi REST API.

        Paginates automatically using the ``cursor`` field returned by the API.
        Server-side filtering by ``series_ticker`` is applied when
        ``series_tickers`` is non-empty.  Client-side filtering by
        ``event_ticker`` is applied when ``event_tickers`` is non-empty.

        Parameters
        ----------
        series_tickers : tuple[str, ...], optional
            Series tickers to restrict the query, e.g. ``("KXBTC",)``.
        event_tickers : tuple[str, ...], optional
            Event tickers for additional client-side filtering.

        Returns
        -------
        list[dict]
            Raw market dictionaries as returned by the Kalshi API.
        """
        params: dict = {"limit": 1000, "status": "open"}
        if series_tickers:
            params["series_ticker"] = ",".join(series_tickers)

        markets: list[dict] = []
        cursor: str | None = None

        while True:
            if cursor:
                params["cursor"] = cursor
            resp = await self._client.get("/markets", params=params)
            resp.raise_for_status()
            data = resp.json()
            markets.extend(data.get("markets", []))
            cursor = data.get("cursor") or None
            if not cursor:
                break

        if event_tickers:
            event_set = set(event_tickers)
            markets = [m for m in markets if m.get("event_ticker") in event_set]

        return markets


class KalshiInstrumentProvider(InstrumentProvider):
    """
    Provides Kalshi prediction market instruments as ``BinaryOption`` objects.

    Instruments are fetched from the Kalshi REST API and filtered by the
    configured series and/or event tickers.

    Parameters
    ----------
    config : KalshiDataClientConfig
        Configuration for the Kalshi adapter.
    """

    def __init__(self, config: KalshiDataClientConfig) -> None:
        super().__init__()  # InstrumentProvider expects InstrumentProviderConfig, not LiveDataClientConfig
        self._config = config
        self._base_url = config.base_url or KALSHI_REST_BASE
        self._http_client = _KalshiHttpClient(base_url=self._base_url)

    async def load_all_async(self, filters: dict | None = None) -> None:
        """Fetch and cache all instruments matching the configured filters."""
        markets = await self._fetch_markets()
        for market in markets:
            try:
                instrument = self._market_to_instrument(market)
                self.add(instrument)
            except Exception as exc:
                _log.warning("Kalshi: failed to parse market %s: %s", market.get("ticker"), exc)

    async def _fetch_markets(self) -> list[dict]:
        """Fetch markets from the Kalshi REST API with series/event filtering."""
        return await self._http_client.get_markets(
            series_tickers=self._config.series_tickers, event_tickers=self._config.event_tickers
        )

    def _market_to_instrument(self, market: dict) -> BinaryOption:
        """Convert a Kalshi market dict to a NautilusTrader ``BinaryOption``."""
        return _market_dict_to_instrument(market)
