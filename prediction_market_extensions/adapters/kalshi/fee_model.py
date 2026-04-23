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

from decimal import ROUND_CEILING
from decimal import Decimal
from datetime import UTC, datetime

from nautilus_trader.backtest.config import FeeModelConfig
from nautilus_trader.backtest.models import FeeModel
from nautilus_trader.model.objects import Money

from prediction_market_extensions.adapters.kalshi.providers import KALSHI_TAKER_FEE_RATE


class KalshiProportionalFeeModelConfig(FeeModelConfig, frozen=True):
    """
    Configuration for ``KalshiProportionalFeeModel``.

    Parameters
    ----------
    fee_rate : str, default "0.07"
        The Kalshi fee rate applied to expected earnings.
        Standard taker rate is 7% (``"0.07"``).
    """

    fee_rate: str = "0.07"


class KalshiProportionalFeeModel(FeeModel):
    """
    Kalshi's correct non-linear fee model for backtesting.

    Kalshi charges a percentage of *expected earnings* on each contract, not
    a flat percentage of notional.  The formula is::

        fee = ceil_to_cent(fee_rate * qty * price * (1 - price))

    This differs from NautilusTrader's built-in ``MakerTakerFeeModel``, which
    applies ``notional * fee_rate = qty * price * fee_rate`` -- missing the
    ``(1 - price)`` term.  The error is small near P = 0.25 but grows to 2x
    at P = 0.50 and 6x at P = 0.90.

    Both maker and taker fills use the same formula (Kalshi does not
    distinguish maker/taker in its fee schedule).  Pass
    ``fee_rate = Decimal("0")`` to simulate a fee-waived market.

    Parameters
    ----------
    fee_rate : Decimal, default KALSHI_TAKER_FEE_RATE (0.07)
        The Kalshi fee rate applied to expected earnings.
    config : KalshiProportionalFeeModelConfig, optional
        Serialised configuration (used by ``ImportableFeeModelConfig``).

    Examples
    --------
    Direct use with ``BacktestEngine.add_venue``::

        from prediction_market_extensions.adapters.kalshi.fee_model import KalshiProportionalFeeModel

        engine.add_venue(
            venue=Venue("KALSHI"),
            ...,
            fee_model=KalshiProportionalFeeModel(),
        )

    Via ``BacktestVenueConfig`` (``BacktestNode``)::

        from nautilus_trader.backtest.config import BacktestVenueConfig, ImportableFeeModelConfig

        venue_config = BacktestVenueConfig(
            name="KALSHI",
            ...,
            fee_model=ImportableFeeModelConfig(
                fee_model_path="nautilus_trader.adapters.kalshi.fee_model:KalshiProportionalFeeModel",
                config_path="nautilus_trader.adapters.kalshi.fee_model:KalshiProportionalFeeModelConfig",
                config={"fee_rate": "0.07"},
            ),
        )
    """

    def __init__(
        self,
        fee_rate: Decimal = KALSHI_TAKER_FEE_RATE,
        config: KalshiProportionalFeeModelConfig | None = None,
    ) -> None:
        if config is not None:
            fee_rate = Decimal(config.fee_rate)
        self._fee_rate = fee_rate

    @staticmethod
    def _fee_rate_for_fill(order, instrument, default_fee_rate: Decimal) -> Decimal:
        info = getattr(instrument, "info", None)
        if isinstance(info, dict):
            raw_expiration = info.get("fee_waiver_expiration_time")
            if raw_expiration:
                try:
                    waiver_expiration = datetime.fromisoformat(str(raw_expiration))
                except ValueError:
                    waiver_expiration = None
                if waiver_expiration is not None:
                    order_timestamp_ns = getattr(order, "ts_init", None)
                    try:
                        order_timestamp = (
                            datetime.fromtimestamp(int(order_timestamp_ns) / 1_000_000_000, UTC)
                            if order_timestamp_ns is not None
                            else None
                        )
                    except (OSError, OverflowError, TypeError, ValueError):
                        order_timestamp = None
                    if order_timestamp is not None:
                        if waiver_expiration.tzinfo is None:
                            waiver_expiration = waiver_expiration.replace(tzinfo=UTC)
                        else:
                            waiver_expiration = waiver_expiration.astimezone(UTC)
                        if order_timestamp <= waiver_expiration:
                            return Decimal("0")

        instrument_taker_fee = getattr(instrument, "taker_fee", None)
        if instrument_taker_fee is not None and instrument_taker_fee > 0:
            return instrument_taker_fee
        return default_fee_rate

    def get_commission(self, order, fill_qty, fill_px, instrument) -> Money:
        """
        Return the Kalshi commission for a fill.

        Parameters
        ----------
        order : Order
            The order being filled (liquidity side is ignored -- Kalshi charges
            the same rate for makers and takers).
        fill_qty : Quantity
            The fill quantity.
        fill_px : Price
            The fill price (0 < price < 1 for binary options).
        instrument : Instrument
            The instrument being traded.

        Returns
        -------
        Money
            Commission in the instrument's quote currency, rounded up to the
            nearest cent.

        """
        p = Decimal(str(fill_px))
        qty = Decimal(str(fill_qty))

        fee_rate = self._fee_rate_for_fill(order, instrument, self._fee_rate)

        if fee_rate <= 0 or p <= 0 or p >= 1:
            return Money(Decimal("0"), instrument.quote_currency)

        raw = fee_rate * qty * p * (Decimal("1") - p)
        commission = raw.quantize(Decimal("0.01"), rounding=ROUND_CEILING)
        return Money(commission, instrument.quote_currency)
