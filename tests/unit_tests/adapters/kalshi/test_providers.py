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

import decimal
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from nautilus_trader.adapters.kalshi.config import KalshiDataClientConfig
from nautilus_trader.adapters.kalshi.providers import KALSHI_TAKER_FEE_RATE
from nautilus_trader.adapters.kalshi.providers import KalshiInstrumentProvider
from nautilus_trader.adapters.kalshi.providers import _market_dict_to_instrument
from nautilus_trader.adapters.kalshi.providers import calculate_kalshi_commission


@pytest.mark.asyncio
async def test_provider_load_filters_by_series():
    config = KalshiDataClientConfig(series_tickers=("KXBTC",))
    provider = KalshiInstrumentProvider(config=config)
    # Mock the HTTP client get_markets method
    provider._http_client.get_markets = AsyncMock(return_value=[])
    await provider.load_all_async()
    provider._http_client.get_markets.assert_called_once()


# ---------------------------------------------------------------------------
# calculate_kalshi_commission tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("quantity", "price", "fee_rate", "expected"),
    [
        # 1 contract at 50c: 0.07 * 1 * 0.50 * 0.50 = 0.0175 → ceil → 0.02
        (
            decimal.Decimal(1),
            decimal.Decimal("0.50"),
            KALSHI_TAKER_FEE_RATE,
            decimal.Decimal("0.02"),
        ),
        # 10 contracts at 50c: 0.07 * 10 * 0.50 * 0.50 = 0.175 → ceil → 0.18
        (
            decimal.Decimal(10),
            decimal.Decimal("0.50"),
            KALSHI_TAKER_FEE_RATE,
            decimal.Decimal("0.18"),
        ),
        # 100 contracts at 30c: 0.07 * 100 * 0.30 * 0.70 = 1.47 → ceil → 1.47
        (
            decimal.Decimal(100),
            decimal.Decimal("0.30"),
            KALSHI_TAKER_FEE_RATE,
            decimal.Decimal("1.47"),
        ),
        # 1 contract at 95c: 0.07 * 1 * 0.95 * 0.05 = 0.003325 → ceil → 0.01
        (
            decimal.Decimal(1),
            decimal.Decimal("0.95"),
            KALSHI_TAKER_FEE_RATE,
            decimal.Decimal("0.01"),
        ),
        # 50 contracts at 65c: 0.07 * 50 * 0.65 * 0.35 = 0.79625 → ceil → 0.80
        (
            decimal.Decimal(50),
            decimal.Decimal("0.65"),
            KALSHI_TAKER_FEE_RATE,
            decimal.Decimal("0.80"),
        ),
        # Zero fee rate
        (decimal.Decimal(100), decimal.Decimal("0.50"), decimal.Decimal(0), decimal.Decimal(0)),
        # Price at boundary (1.0) → (1-P) = 0 → fee = 0
        (decimal.Decimal(1), decimal.Decimal("1.0"), KALSHI_TAKER_FEE_RATE, decimal.Decimal(0)),
        # Price at boundary (0.0) → fee = 0
        (decimal.Decimal(1), decimal.Decimal("0.0"), KALSHI_TAKER_FEE_RATE, decimal.Decimal(0)),
    ],
)
def test_calculate_kalshi_commission(
    quantity: decimal.Decimal,
    price: decimal.Decimal,
    fee_rate: decimal.Decimal,
    expected: decimal.Decimal,
) -> None:
    """Test Kalshi fee formula: fee = ceil_to_cent(rate x C x P x (1-P))."""
    result = calculate_kalshi_commission(quantity, price, fee_rate)
    assert result == expected


# ---------------------------------------------------------------------------
# _market_dict_to_instrument fee assignment tests
# ---------------------------------------------------------------------------


def _make_market_dict(**overrides) -> dict:
    """Minimal Kalshi market dict for testing."""
    base = {
        "ticker": "TEST-TICKER",
        "title": "Test Market",
        "open_time": "2026-01-01T00:00:00Z",
        "close_time": "2026-12-31T23:59:59Z",
    }
    base.update(overrides)
    return base


def test_instrument_has_taker_fee():
    """Instruments should have the standard 7% taker fee by default."""
    market = _make_market_dict()
    instrument = _market_dict_to_instrument(market)
    assert instrument.taker_fee == KALSHI_TAKER_FEE_RATE


def test_instrument_fee_waiver_active():
    """Active fee waiver should set taker_fee to zero."""
    future = datetime(2099, 1, 1, tzinfo=UTC).isoformat()
    market = _make_market_dict(fee_waiver_expiration_time=future)
    instrument = _market_dict_to_instrument(market)
    assert instrument.taker_fee == decimal.Decimal(0)


def test_instrument_fee_waiver_expired():
    """Expired fee waiver should still apply the standard taker fee."""
    past = datetime(2020, 1, 1, tzinfo=UTC).isoformat()
    market = _make_market_dict(fee_waiver_expiration_time=past)
    instrument = _market_dict_to_instrument(market)
    assert instrument.taker_fee == KALSHI_TAKER_FEE_RATE


def test_instrument_maker_fee_zero():
    """Maker fee should default to zero."""
    market = _make_market_dict()
    instrument = _market_dict_to_instrument(market)
    assert instrument.maker_fee == decimal.Decimal(0)


def test_instrument_preserves_market_metadata():
    market = _make_market_dict(result="yes", settlement_value=1)

    instrument = _market_dict_to_instrument(market)

    assert instrument.info == market
