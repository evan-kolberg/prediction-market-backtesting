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

"""Unit tests for KalshiProportionalFeeModel."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModelConfig
from nautilus_trader.adapters.kalshi.providers import KALSHI_TAKER_FEE_RATE
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

USD = Currency.from_str("USD")


def _make_instrument(quote_currency=USD):
    inst = MagicMock()
    inst.quote_currency = quote_currency
    return inst


def _make_price(value: float):
    px = MagicMock()
    px.__float__ = lambda self: value
    return px


def _make_qty(value: float):
    qty = MagicMock()
    qty.__float__ = lambda self: value
    return qty


def _commission(price: float, qty: float, fee_rate: Decimal = KALSHI_TAKER_FEE_RATE) -> Decimal:
    """Invoke get_commission and return the Money amount as Decimal."""
    model = KalshiProportionalFeeModel(fee_rate=fee_rate)
    order = MagicMock()
    result: Money = model.get_commission(order, _make_qty(qty), _make_price(price), _make_instrument())
    return result.as_decimal()


# ---------------------------------------------------------------------------
# Core formula tests: fee = ceil(rate * qty * price * (1-price))
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("price", "qty", "fee_rate", "expected"),
    [
        # 1 contract @ 50c: 0.07 * 1 * 0.50 * 0.50 = 0.0175 -> ceil -> 0.02
        (0.50, 1.0, KALSHI_TAKER_FEE_RATE, Decimal("0.02")),
        # 10 contracts @ 50c: 0.07 * 10 * 0.50 * 0.50 = 0.175 -> ceil -> 0.18
        (0.50, 10.0, KALSHI_TAKER_FEE_RATE, Decimal("0.18")),
        # 100 contracts @ 30c: 0.07 * 100 * 0.30 * 0.70 = 1.47 -> exact, no ceil needed
        (0.30, 100.0, KALSHI_TAKER_FEE_RATE, Decimal("1.47")),
        # 1 contract @ 95c: 0.07 * 1 * 0.95 * 0.05 = 0.003325 -> ceil -> 0.01
        (0.95, 1.0, KALSHI_TAKER_FEE_RATE, Decimal("0.01")),
        # 50 contracts @ 65c: 0.07 * 50 * 0.65 * 0.35 = 0.79625 -> ceil -> 0.80
        (0.65, 50.0, KALSHI_TAKER_FEE_RATE, Decimal("0.80")),
        # 1 contract @ 10c: 0.07 * 1 * 0.10 * 0.90 = 0.0063 -> ceil -> 0.01
        (0.10, 1.0, KALSHI_TAKER_FEE_RATE, Decimal("0.01")),
    ],
)
def test_fee_formula(price, qty, fee_rate, expected):
    """KalshiProportionalFeeModel applies fee = ceil(rate * qty * price * (1-price))."""
    assert _commission(price, qty, fee_rate) == expected


# ---------------------------------------------------------------------------
# Boundary conditions: fee should be zero
# ---------------------------------------------------------------------------

def test_fee_zero_when_price_is_zero():
    assert _commission(price=0.0, qty=100.0) == Decimal(0)


def test_fee_zero_when_price_is_one():
    assert _commission(price=1.0, qty=100.0) == Decimal(0)


def test_fee_zero_when_fee_rate_is_zero():
    assert _commission(price=0.50, qty=100.0, fee_rate=Decimal(0)) == Decimal(0)


def test_fee_returns_money_object():
    model = KalshiProportionalFeeModel()
    order = MagicMock()
    result = model.get_commission(order, _make_qty(1.0), _make_price(0.50), _make_instrument())
    assert isinstance(result, Money)
    assert result.currency == USD


# ---------------------------------------------------------------------------
# Config-path construction (ImportableFeeModelConfig route)
# ---------------------------------------------------------------------------

def test_config_overrides_fee_rate():
    """KalshiProportionalFeeModelConfig with a custom rate should be honoured."""
    config = KalshiProportionalFeeModelConfig(fee_rate="0.05")
    model = KalshiProportionalFeeModel(config=config)
    # 0.05 * 1 * 0.50 * 0.50 = 0.0125 -> ceil -> 0.02
    order = MagicMock()
    result = model.get_commission(order, _make_qty(1.0), _make_price(0.50), _make_instrument())
    assert result.as_decimal() == Decimal("0.02")


def test_config_fee_rate_zero_suppresses_fee():
    config = KalshiProportionalFeeModelConfig(fee_rate="0")
    model = KalshiProportionalFeeModel(config=config)
    order = MagicMock()
    result = model.get_commission(order, _make_qty(100.0), _make_price(0.50), _make_instrument())
    assert result.as_decimal() == Decimal(0)


def test_config_default_fee_rate():
    """Default config should use 7%."""
    config = KalshiProportionalFeeModelConfig()
    assert Decimal(config.fee_rate) == KALSHI_TAKER_FEE_RATE


# ---------------------------------------------------------------------------
# Ceiling behaviour: ensure we always round up (never down) to nearest cent
# ---------------------------------------------------------------------------

def test_ceiling_rounds_up_not_down():
    """A raw fee that is not an exact number of cents must be rounded UP."""
    # 0.07 * 1 * 0.50 * 0.50 = 0.0175 -- not a whole cent; must round to 0.02, not 0.01
    result = _commission(price=0.50, qty=1.0)
    assert result == Decimal("0.02")


def test_exact_cent_not_double_rounded():
    """A raw fee that IS an exact cent should not be bumped to the next cent."""
    # 0.07 * 100 * 0.30 * 0.70 = 1.47 exactly -- should stay at 1.47
    result = _commission(price=0.30, qty=100.0)
    assert result == Decimal("1.47")
