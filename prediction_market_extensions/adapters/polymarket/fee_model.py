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

from decimal import Decimal

from nautilus_trader.backtest.models import FeeModel
from nautilus_trader.core.rust.model import OrderType
from nautilus_trader.model.objects import Money

from prediction_market_extensions.adapters.polymarket.parsing import calculate_commission


class PolymarketFeeModel(FeeModel):
    """
    Polymarket fee model for backtesting.

    Applies Polymarket's taker fee formula per fill::

        fee = qty x feeRate x p x (1 - p)

    Where:
    - ``feeRate = taker_base_fee_bps / 10_000``
    - ``p`` is the fill price in [0, 1]

    Maker fees remain zero. Taker fee rates come from the market payload when
    available, or from the CLOB fee-rate endpoint when the market payload still
    reports zeros.

    References
    ----------
    https://docs.polymarket.com/trading/fees
    """

    def get_commission(self, order, fill_qty, fill_px, instrument) -> Money:
        """
        Return the Polymarket commission for a fill.

        Parameters
        ----------
        order : Order
            The order being filled.
        fill_qty : Quantity
            The fill quantity (shares).
        fill_px : Price
            The fill price (0 < price < 1 for binary options).
        instrument : Instrument
            The instrument being traded.

        Returns
        -------
        Money
            Commission in the instrument's quote currency, rounded to 5 decimal places.

        """
        if order.order_type == OrderType.LIMIT:
            # The fee callback does not expose realized maker/taker liquidity.
            # Repo-owned Polymarket limit-order backtests use passive-posting
            # strategies, so treat their fills as maker-side and keep fees at
            # zero to match the documented venue model.
            return Money(Decimal("0"), instrument.quote_currency)

        # instrument.taker_fee is stored as bps/10_000 (decimal fraction)
        taker_fee_dec = instrument.taker_fee
        if taker_fee_dec is None:
            return Money(Decimal("0"), instrument.quote_currency)
        fee_rate_bps = taker_fee_dec * Decimal(10_000)

        if fee_rate_bps <= 0:
            return Money(Decimal("0"), instrument.quote_currency)

        commission = calculate_commission(
            quantity=Decimal(str(fill_qty)), price=Decimal(str(fill_px)), fee_rate_bps=fee_rate_bps
        )
        return Money(Decimal(str(commission)), instrument.quote_currency)
