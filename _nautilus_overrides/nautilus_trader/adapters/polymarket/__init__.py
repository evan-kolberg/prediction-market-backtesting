# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11 and 2026-03-15.
#  See the repository NOTICE file for provenance and licensing scope.
#
"""
Polymarket decentralized prediction market integration adapter.

This subpackage provides instrument providers, data and execution client configurations,
factories, constants, and credential helpers for connecting to and interacting with
the Polymarket Central Limit Order Book (CLOB) API.

For convenience, the most commonly used symbols are re-exported at the subpackage's
top level, so downstream code can simply import from ``nautilus_trader.adapters.polymarket``.

"""

from __future__ import annotations

from importlib import import_module
from pathlib import Path


def _extend_package_path() -> None:
    import nautilus_trader.adapters as _adapters

    package_dir = Path(__file__).resolve().parent
    for root in _adapters.__path__:
        candidate = Path(root) / "polymarket"
        if candidate.is_dir() and candidate != package_dir:
            candidate_str = str(candidate)
            if candidate_str not in __path__:
                __path__.append(candidate_str)


_extend_package_path()

_constants = import_module("nautilus_trader.adapters.polymarket.common.constants")
_parsing = import_module("nautilus_trader.adapters.polymarket.common.parsing")
_symbol = import_module("nautilus_trader.adapters.polymarket.common.symbol")
_config = import_module("nautilus_trader.adapters.polymarket.config")
_factories = import_module("nautilus_trader.adapters.polymarket.factories")
_loaders = import_module("nautilus_trader.adapters.polymarket.loaders")
_pmxt = import_module("nautilus_trader.adapters.polymarket.pmxt")
_providers = import_module("nautilus_trader.adapters.polymarket.providers")

POLYMARKET = _constants.POLYMARKET
POLYMARKET_CLIENT_ID = _constants.POLYMARKET_CLIENT_ID
POLYMARKET_MAX_PRECISION_MAKER = _constants.POLYMARKET_MAX_PRECISION_MAKER
POLYMARKET_MAX_PRECISION_TAKER = _constants.POLYMARKET_MAX_PRECISION_TAKER
POLYMARKET_MAX_PRICE = _constants.POLYMARKET_MAX_PRICE
POLYMARKET_MIN_PRICE = _constants.POLYMARKET_MIN_PRICE
POLYMARKET_VENUE = _constants.POLYMARKET_VENUE
parse_polymarket_instrument = _parsing.parse_polymarket_instrument
get_polymarket_instrument_id = _symbol.get_polymarket_instrument_id
PolymarketDataClientConfig = _config.PolymarketDataClientConfig
PolymarketExecClientConfig = _config.PolymarketExecClientConfig
PolymarketLiveDataClientFactory = _factories.PolymarketLiveDataClientFactory
PolymarketLiveExecClientFactory = _factories.PolymarketLiveExecClientFactory
get_polymarket_http_client = _factories.get_polymarket_http_client
get_polymarket_instrument_provider = _factories.get_polymarket_instrument_provider
PolymarketDataLoader = _loaders.PolymarketDataLoader
PolymarketPMXTDataLoader = _pmxt.PolymarketPMXTDataLoader
PolymarketInstrumentProvider = _providers.PolymarketInstrumentProvider


__all__ = [
    "POLYMARKET",
    "POLYMARKET_CLIENT_ID",
    "POLYMARKET_MAX_PRECISION_MAKER",
    "POLYMARKET_MAX_PRECISION_TAKER",
    "POLYMARKET_MAX_PRICE",
    "POLYMARKET_MIN_PRICE",
    "POLYMARKET_VENUE",
    "PolymarketDataClientConfig",
    "PolymarketDataLoader",
    "PolymarketPMXTDataLoader",
    "PolymarketExecClientConfig",
    "PolymarketInstrumentProvider",
    "PolymarketLiveDataClientFactory",
    "PolymarketLiveExecClientFactory",
    "get_polymarket_http_client",
    "get_polymarket_instrument_id",
    "get_polymarket_instrument_provider",
    "parse_polymarket_instrument",
]
