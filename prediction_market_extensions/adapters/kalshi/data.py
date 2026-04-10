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
from typing import TYPE_CHECKING

from prediction_market_extensions.adapters.kalshi.config import KalshiDataClientConfig
from prediction_market_extensions.adapters.kalshi.providers import KalshiInstrumentProvider
from nautilus_trader.data.messages import RequestBars
from nautilus_trader.data.messages import RequestInstrument
from nautilus_trader.data.messages import RequestInstruments
from nautilus_trader.data.messages import RequestQuoteTicks
from nautilus_trader.data.messages import RequestTradeTicks
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeInstrumentClose
from nautilus_trader.data.messages import SubscribeInstrumentStatus
from nautilus_trader.data.messages import SubscribeOrderBook
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeBars
from nautilus_trader.data.messages import UnsubscribeInstrumentClose
from nautilus_trader.data.messages import UnsubscribeInstrumentStatus
from nautilus_trader.data.messages import UnsubscribeOrderBook
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.identifiers import ClientId


if TYPE_CHECKING:
    from nautilus_trader.cache.cache import Cache
    from nautilus_trader.common.component import LiveClock
    from nautilus_trader.common.component import MessageBus

KALSHI_CLIENT_ID = "KALSHI"


class KalshiDataClient(LiveMarketDataClient):
    """
    Provides a Kalshi market data client for live paper trading and backtesting.

    Currently a skeleton that loads instruments on connect. Subscribe methods
    for orderbook deltas, trade ticks, and OHLCV bars are not yet implemented.
    Reference nautilus_trader/adapters/polymarket/data.py for the patterns.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop.
    client_id : ClientId
        The data client ID.
    msgbus : MessageBus
        The message bus.
    cache : Cache
        The cache.
    clock : LiveClock
        The clock.
    instrument_provider : KalshiInstrumentProvider
        The instrument provider.
    config : KalshiDataClientConfig
        The adapter configuration.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: KalshiInstrumentProvider,
        config: KalshiDataClientConfig,
        name: str | None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or KALSHI_CLIENT_ID),
            venue=None,  # Multi-venue adapter
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
        )
        self._config = config

    async def _connect(self) -> None:
        await self._instrument_provider.initialize()
        self._send_all_instruments_to_data_engine()

    async def _disconnect(self) -> None:
        pass

    def _send_all_instruments_to_data_engine(self) -> None:
        for instrument in self._instrument_provider.get_all().values():
            self._handle_data(instrument)

        for currency in self._instrument_provider.currencies().values():
            self._cache.add_currency(currency)

    def _log_unsupported(self, action: str) -> None:
        self._log.error(
            f"KalshiDataClient does not yet support {action}; only instrument discovery is live"
        )

    async def _subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None:
        self._log_unsupported("order book subscriptions")

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        self._log_unsupported("quote subscriptions")

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        self._log_unsupported("trade subscriptions")

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        self._log_unsupported("bar subscriptions")

    async def _subscribe_instrument_status(self, command: SubscribeInstrumentStatus) -> None:
        self._log_unsupported("instrument status subscriptions")

    async def _subscribe_instrument_close(self, command: SubscribeInstrumentClose) -> None:
        self._log_unsupported("instrument close subscriptions")

    async def _unsubscribe_order_book_deltas(self, command: UnsubscribeOrderBook) -> None:
        self._log_unsupported("order book unsubscriptions")

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        self._log_unsupported("quote unsubscriptions")

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        self._log_unsupported("trade unsubscriptions")

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        self._log_unsupported("bar unsubscriptions")

    async def _unsubscribe_instrument_status(self, command: UnsubscribeInstrumentStatus) -> None:
        self._log_unsupported("instrument status unsubscriptions")

    async def _unsubscribe_instrument_close(self, command: UnsubscribeInstrumentClose) -> None:
        self._log_unsupported("instrument close unsubscriptions")

    async def _request_instrument(self, request: RequestInstrument) -> None:
        instrument = self._instrument_provider.find(request.instrument_id)
        if instrument is None:
            self._log.error(f"Cannot find instrument for {request.instrument_id}")
            return

        self._handle_instrument(instrument, request.id, request.start, request.end, request.params)

    async def _request_instruments(self, request: RequestInstruments) -> None:
        instruments = [
            instrument
            for instrument in self._instrument_provider.get_all().values()
            if request.venue is None or instrument.venue == request.venue
        ]
        self._handle_instruments(
            request.venue, instruments, request.id, request.start, request.end, request.params
        )

    async def _request_quote_ticks(self, request: RequestQuoteTicks) -> None:
        self._log_unsupported("historical quote requests")

    async def _request_trade_ticks(self, request: RequestTradeTicks) -> None:
        self._log_unsupported("historical trade requests")

    async def _request_bars(self, request: RequestBars) -> None:
        self._log_unsupported("historical bar requests")
