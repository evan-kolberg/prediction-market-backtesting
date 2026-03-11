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

from __future__ import annotations

from decimal import Decimal
from typing import Protocol

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy


class LongOnlyConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal


class LongOnlyPredictionMarketStrategy(Strategy):
    """
    Shared lifecycle + order plumbing for single-instrument long-only strategies.
    """

    def __init__(self, config: LongOnlyConfig) -> None:
        super().__init__(config)
        self._instrument = None
        self._pending: bool = False
        self._entry_price: float | None = None

    def _subscribe(self) -> None:
        raise NotImplementedError

    def on_start(self) -> None:
        self._instrument = self.cache.instrument(self.config.instrument_id)
        if self._instrument is None:
            self.log.error(f"Instrument {self.config.instrument_id} not found - stopping.")
            self.stop()
            return
        self._subscribe()

    def _in_position(self) -> bool:
        return not self.portfolio.is_flat(self.config.instrument_id)

    def _submit_entry(self) -> None:
        assert self._instrument is not None
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self._instrument.make_qty(float(self.config.trade_size)),
            time_in_force=TimeInForce.IOC,
        )
        self.submit_order(order)
        self._pending = True

    def _submit_exit(self) -> None:
        self.close_all_positions(self.config.instrument_id)
        self._pending = True

    def _risk_exit(
        self,
        *,
        price: float,
        take_profit: float,
        stop_loss: float,
    ) -> bool:
        if not self._in_position() or self._entry_price is None:
            return False

        take_profit_hit = take_profit > 0.0 and price >= self._entry_price + take_profit
        stop_loss_hit = stop_loss > 0.0 and price <= self._entry_price - stop_loss
        if take_profit_hit or stop_loss_hit:
            self._submit_exit()
            return True
        return False

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        if event.order_side == OrderSide.BUY:
            self._entry_price = float(event.last_px)
        else:
            self._entry_price = None
        self._pending = False

    def on_order_rejected(self, event) -> None:  # type: ignore[no-untyped-def]
        self._pending = False

    def on_order_canceled(self, event) -> None:  # type: ignore[no-untyped-def]
        self._pending = False

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)

    def on_reset(self) -> None:
        self._pending = False
        self._entry_price = None
        self._instrument = None
