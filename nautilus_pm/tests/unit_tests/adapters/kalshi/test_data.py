# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.kalshi.config import KalshiDataClientConfig
from nautilus_trader.adapters.kalshi.data import KalshiDataClient
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.providers import InstrumentProvider
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


class StubInstrumentProvider(InstrumentProvider):
    def __init__(self, instrument) -> None:
        super().__init__()
        self._instrument = instrument
        self.initialize = AsyncMock()
        self.find = MagicMock(return_value=instrument)

    async def load_all_async(self, filters: dict | None = None) -> None:
        return None

    def get_all(self):
        return {"instrument": self._instrument}

    def currencies(self):
        return {}


@pytest.fixture
def kalshi_data_client(event_loop) -> tuple[KalshiDataClient, StubInstrumentProvider]:
    clock = LiveClock()
    msgbus = MessageBus(
        trader_id=TestIdStubs.trader_id(),
        clock=clock,
    )
    cache = TestComponentStubs.cache()
    instrument = SimpleNamespace(venue="KALSHI")
    provider = StubInstrumentProvider(instrument)

    client = KalshiDataClient(
        loop=event_loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=KalshiDataClientConfig(),
        name=None,
    )
    return client, provider


@pytest.mark.asyncio
async def test_connect_initializes_and_emits_instruments(kalshi_data_client) -> None:
    client, provider = kalshi_data_client
    client._handle_data = MagicMock()

    await client._connect()

    provider.initialize.assert_awaited_once()
    client._handle_data.assert_called_once_with(provider.get_all()["instrument"])


@pytest.mark.asyncio
async def test_request_instrument_uses_loaded_provider_state(kalshi_data_client) -> None:
    client, provider = kalshi_data_client
    client._handle_instrument = MagicMock()
    request = SimpleNamespace(
        instrument_id="KALSHI-INSTRUMENT",
        id="request-id",
        start=None,
        end=None,
        params=None,
    )

    await client._request_instrument(request)

    provider.find.assert_called_once_with("KALSHI-INSTRUMENT")
    client._handle_instrument.assert_called_once_with(
        provider.get_all()["instrument"],
        "request-id",
        None,
        None,
        None,
    )


@pytest.mark.asyncio
async def test_subscribe_trade_ticks_logs_unsupported_error(kalshi_data_client) -> None:
    client, _provider = kalshi_data_client
    client._log_unsupported = MagicMock()

    await client._subscribe_trade_ticks(SimpleNamespace())

    client._log_unsupported.assert_called_once_with("trade subscriptions")
