# -------------------------------------------------------------------------------------------------
# Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
# https://nautechsystems.io
#
# Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing
# permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
# Derived from NautilusTrader prediction-market example code.
# Modified by Evan Kolberg in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Protocol

from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

ENTRY_AFFORDABILITY_BUFFER = Decimal("0.97")


class LongOnlyConfig(Protocol):
    instrument_id: InstrumentId
    trade_size: Decimal


def _decimal_or_none(value: object) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _estimate_entry_unit_cost(*, reference_price: Decimal, taker_fee: Decimal) -> Decimal:
    clamped_price = min(max(reference_price, Decimal(0)), Decimal(1))
    return clamped_price + (taker_fee * clamped_price * (Decimal(1) - clamped_price))


def _cap_entry_size_to_free_balance(
    *,
    desired_size: Decimal,
    reference_price: Decimal | None,
    taker_fee: Decimal,
    free_balance: Decimal | None,
) -> Decimal:
    if desired_size <= 0:
        return Decimal(0)
    if reference_price is None or reference_price <= 0 or free_balance is None:
        return desired_size

    unit_cost = _estimate_entry_unit_cost(
        reference_price=reference_price, taker_fee=max(taker_fee, Decimal(0))
    )
    if unit_cost <= 0:
        return desired_size

    # Leave a small cash buffer so marketable entries do not spend exactly to
    # the displayed top-of-book estimate on thin books.
    affordable_size = (free_balance * ENTRY_AFFORDABILITY_BUFFER) / unit_cost
    return max(Decimal(0), min(desired_size, affordable_size))


def _cap_entry_size_to_visible_liquidity(
    *, desired_size: Decimal, visible_size: Decimal | None
) -> Decimal:
    if desired_size <= 0:
        return Decimal(0)
    if visible_size is None:
        return desired_size
    if visible_size <= 0:
        return Decimal(0)
    return min(desired_size, visible_size)


def _effective_entry_reference_price(
    *, reference_price: Decimal | None, visible_size: Decimal | None
) -> Decimal:
    if reference_price is not None and visible_size is not None and visible_size > 0:
        return reference_price

    # Without a visible ask, a market buy can clear anywhere up to 1.0 in a
    # binary prediction market. Size against that worst-case cash usage rather
    # than the last print to avoid manufacturing affordable size from stale
    # trade-only signals.
    return Decimal(1)


class LongOnlyPredictionMarketStrategy(Strategy):
    """
    Shared lifecycle + order plumbing for single-instrument long-only strategies.
    """

    def __init__(self, config: LongOnlyConfig) -> None:
        super().__init__(config)
        self._instrument = None
        self._pending: bool = False
        self._entry_price: float | None = None
        self._entry_qty_sum: float = 0.0
        self._entry_cost_sum: float = 0.0

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

    def _free_quote_balance(self) -> Decimal | None:
        assert self._instrument is not None
        account = self.portfolio.account(venue=self.config.instrument_id.venue)
        if account is None:
            return None
        free_balance = account.balance_free(self._instrument.quote_currency)
        if free_balance is None:
            return None
        return _decimal_or_none(free_balance.as_double())

    def _entry_quantity(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ):
        assert self._instrument is not None
        desired_size = _decimal_or_none(self.config.trade_size)
        if desired_size is None or desired_size <= 0:
            return None

        visible_size_decimal = _decimal_or_none(visible_size)
        liquidity_capped_size = _cap_entry_size_to_visible_liquidity(
            desired_size=desired_size, visible_size=visible_size_decimal
        )
        capped_size = _cap_entry_size_to_free_balance(
            desired_size=liquidity_capped_size,
            reference_price=_effective_entry_reference_price(
                reference_price=_decimal_or_none(reference_price), visible_size=visible_size_decimal
            ),
            taker_fee=_decimal_or_none(self._instrument.taker_fee) or Decimal(0),
            free_balance=self._free_quote_balance(),
        )
        if capped_size <= 0:
            return None

        try:
            quantity = self._instrument.make_qty(float(capped_size), round_down=True)
        except ValueError:
            return None

        lot_size = self._instrument.lot_size
        if lot_size is not None and quantity.as_double() + 1e-12 < lot_size.as_double():
            return None

        min_quantity = getattr(self._instrument, "min_quantity", None)
        if min_quantity is not None and quantity.as_double() + 1e-12 < min_quantity.as_double():
            return None

        if quantity.as_double() + 1e-12 < float(desired_size):
            self.log.debug(
                f"Clipped BUY size for {self.config.instrument_id} from {desired_size} "
                f"to {quantity.as_double():.12f} using reference price "
                f"{float(reference_price or 0.0):.6f}"
            )
        return quantity

    def _submit_entry(
        self, *, reference_price: float | None = None, visible_size: float | None = None
    ) -> None:
        quantity = self._entry_quantity(reference_price=reference_price, visible_size=visible_size)
        if quantity is None:
            return
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            time_in_force=TimeInForce.IOC,
        )
        self.submit_order(order)
        self._pending = True

    def _submit_exit(self) -> None:
        self.close_all_positions(self.config.instrument_id)
        self._pending = True

    def _risk_exit(self, *, price: float, take_profit: float, stop_loss: float) -> bool:
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
            fill_px = float(event.last_px)
            fill_qty = float(event.last_qty)
            self._entry_cost_sum += fill_px * fill_qty
            self._entry_qty_sum += fill_qty
            if self._entry_qty_sum > 0:
                self._entry_price = self._entry_cost_sum / self._entry_qty_sum
        else:
            fill_qty = float(event.last_qty)
            self._entry_qty_sum -= fill_qty
            if self._entry_qty_sum > 0:
                self._entry_price = self._entry_cost_sum / self._entry_qty_sum
            else:
                self._entry_price = None
                self._entry_qty_sum = 0.0
                self._entry_cost_sum = 0.0
        self._pending = False

    def on_order_rejected(self, event) -> None:  # type: ignore[no-untyped-def]
        self._pending = False

    def on_order_canceled(self, event) -> None:  # type: ignore[no-untyped-def]
        self._pending = False

    def on_order_expired(self, event) -> None:  # type: ignore[no-untyped-def]
        self._pending = False

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)

    def on_reset(self) -> None:
        self._pending = False
        self._entry_price = None
        self._entry_qty_sum = 0.0
        self._entry_cost_sum = 0.0
        self._instrument = None
