# -------------------------------------------------------------------------------------------------
# Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
# https://nautechsystems.io
#
# Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing
# permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Added in this repository on 2026-04-27.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal, InvalidOperation

from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.enums import BookType, OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy, StrategyConfig

from prediction_market_extensions.adapters.prediction_market.order_tags import (
    format_order_intent_tag,
    format_visible_liquidity_tag,
)
from strategies._validation import (
    require_finite_nonnegative_float,
    require_nonnegative_int,
    require_positive_decimal,
    require_probability,
)

ENTRY_AFFORDABILITY_BUFFER = Decimal("0.97")


def _as_float(value: object | None) -> float | None:
    if value is None:
        return None
    if callable(value):
        value = value()
    as_double = getattr(value, "as_double", None)
    if callable(as_double):
        return float(as_double())
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decimal_or_none(value: object | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _clamp_probability(value: Decimal) -> Decimal:
    return min(max(value, Decimal("0")), Decimal("1"))


def _fee_per_share(*, price: Decimal, taker_fee: Decimal) -> Decimal:
    price = _clamp_probability(price)
    fee_rate = max(taker_fee, Decimal("0"))
    return fee_rate * price * (Decimal("1") - price)


class BookBinaryPairArbitrageConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    """
    Buy both complementary binary outcomes when the combined executable ask
    is sufficiently below 1.00 after taker fees.

    This is a book-only strategy. It does not use wallets, comments, trader
    identities, or external forecasts. It assumes the configured instruments
    are ordered as complementary binary pairs: (YES/UP, NO/DOWN), repeated.
    The signal includes taker fees by default; turning that off is useful for
    gross-edge fill diagnostics, while the execution model still charges fees.
    """

    instrument_ids: tuple[InstrumentId, ...]
    trade_size: Decimal = Decimal("25")
    min_net_edge: float = 0.020
    max_total_cost: float = 0.985
    max_leg_price: float = 0.985
    max_spread: float = 0.080
    max_expected_slippage: float = 0.015
    min_visible_size: float = 1.0
    max_entries_per_pair: int = 1
    reentry_cooldown_updates: int = 25
    pairing_mode: str = "sequential"
    hold_to_resolution: bool = True
    include_taker_fees_in_signal: bool = True

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_finite_nonnegative_float("min_net_edge", self.min_net_edge)
        require_probability("max_total_cost", self.max_total_cost)
        require_probability("max_leg_price", self.max_leg_price)
        require_probability("max_spread", self.max_spread)
        require_finite_nonnegative_float("max_expected_slippage", self.max_expected_slippage)
        require_finite_nonnegative_float("min_visible_size", self.min_visible_size)
        require_nonnegative_int("max_entries_per_pair", self.max_entries_per_pair)
        require_nonnegative_int("reentry_cooldown_updates", self.reentry_cooldown_updates)
        if self.pairing_mode != "sequential":
            raise ValueError("pairing_mode currently supports only 'sequential'")
        if len(self.instrument_ids) < 2 or len(self.instrument_ids) % 2 != 0:
            raise ValueError(
                "instrument_ids must contain an even number of instruments ordered as pairs"
            )


class BookBinaryPairArbitrageStrategy(Strategy):
    """
    Multi-instrument complementary-token arbitrage.

    For a binary market, one share of each complementary outcome settles to
    exactly 1.00 USDC in aggregate. The strategy watches both L2 books and
    submits market IOC buys for both legs when the executable pair cost is
    below 1.00 by at least `min_net_edge`, after estimated taker fees.

    Live caveat: Polymarket orders are not atomic across the two legs. This
    strategy sizes against visible liquidity and slippage, but a live bot
    should add an immediate hedge/flatten path for a one-leg fill.
    """

    def __init__(self, config: BookBinaryPairArbitrageConfig) -> None:
        super().__init__(config)
        self._books: dict[InstrumentId, OrderBook] = {}
        self._instruments: dict[InstrumentId, object] = {}
        self._pairs: list[tuple[InstrumentId, InstrumentId]] = []
        self._pair_by_instrument: dict[InstrumentId, tuple[InstrumentId, InstrumentId]] = {}
        self._pending_by_pair: dict[tuple[InstrumentId, InstrumentId], int] = {}
        self._cooldown_by_pair: dict[tuple[InstrumentId, InstrumentId], int] = {}
        self._entries_by_pair: dict[tuple[InstrumentId, InstrumentId], int] = {}

    def on_start(self) -> None:
        instrument_ids = tuple(self.config.instrument_ids)
        self._pairs = [
            (instrument_ids[i], instrument_ids[i + 1]) for i in range(0, len(instrument_ids), 2)
        ]
        self._pair_by_instrument = {
            instrument_id: pair for pair in self._pairs for instrument_id in pair
        }
        self._pending_by_pair = {pair: 0 for pair in self._pairs}
        self._cooldown_by_pair = {pair: 0 for pair in self._pairs}
        self._entries_by_pair = {pair: 0 for pair in self._pairs}

        for instrument_id in instrument_ids:
            instrument = self.cache.instrument(instrument_id)
            if instrument is None:
                self.log.error(f"Instrument {instrument_id} not found - stopping.")
                self.stop()
                return
            self._instruments[instrument_id] = instrument
            self.subscribe_order_book_deltas(
                instrument_id=instrument_id,
                book_type=BookType.L2_MBP,
            )

    def on_order_book_deltas(self, deltas) -> None:  # type: ignore[no-untyped-def]
        instrument_id = getattr(deltas, "instrument_id", None)
        if instrument_id not in self._pair_by_instrument:
            return
        if instrument_id not in self._books:
            self._books[instrument_id] = OrderBook(
                instrument_id,
                book_type=BookType.L2_MBP,
            )
        self._books[instrument_id].apply_deltas(deltas)
        self._evaluate_pair(self._pair_by_instrument[instrument_id])

    def _best_ask_state(self, instrument_id: InstrumentId) -> tuple[float, float, float] | None:
        book = self._books.get(instrument_id)
        if book is None:
            return None
        bid = _as_float(book.best_bid_price())
        ask = _as_float(book.best_ask_price())
        ask_size = _as_float(book.best_ask_size())
        spread = _as_float(book.spread())
        if bid is None or ask is None or ask_size is None or spread is None:
            return None
        if ask <= 0.0 or ask >= 1.0 or ask <= bid or ask_size <= 0.0 or spread <= 0.0:
            return None
        return ask, ask_size, spread

    def _instrument_fee_rate(self, instrument_id: InstrumentId) -> Decimal:
        instrument = self._instruments.get(instrument_id)
        if instrument is None:
            return Decimal("0")
        return _decimal_or_none(getattr(instrument, "taker_fee", None)) or Decimal("0")

    def _free_quote_balance(self, instrument_id: InstrumentId) -> Decimal | None:
        instrument = self._instruments.get(instrument_id)
        if instrument is None:
            return None
        account = self.portfolio.account(venue=instrument_id.venue)
        if account is None:
            return None
        free_balance = account.balance_free(instrument.quote_currency)
        if free_balance is None:
            return None
        return _decimal_or_none(free_balance.as_double())

    def _avg_entry_price(self, instrument_id: InstrumentId, size: Decimal) -> float | None:
        instrument = self._instruments.get(instrument_id)
        book = self._books.get(instrument_id)
        if instrument is None or book is None or size <= 0:
            return None
        try:
            quantity = instrument.make_qty(float(size), round_down=True)
        except ValueError:
            return None
        if quantity.as_double() <= 0.0:
            return None
        avg_px = _as_float(book.get_avg_px_for_quantity(quantity, OrderSide.BUY))
        if avg_px is None or avg_px <= 0.0:
            return None
        return avg_px

    def _rounded_quantity(self, instrument_id: InstrumentId, size: Decimal):  # type: ignore[no-untyped-def]
        instrument = self._instruments[instrument_id]
        try:
            quantity = instrument.make_qty(float(size), round_down=True)
        except ValueError:
            return None
        if quantity.as_double() <= 0.0:
            return None
        min_quantity = getattr(instrument, "min_quantity", None)
        if min_quantity is not None and quantity.as_double() + 1e-12 < min_quantity.as_double():
            return None
        lot_size = getattr(instrument, "lot_size", None)
        if lot_size is not None and quantity.as_double() + 1e-12 < lot_size.as_double():
            return None
        return quantity

    def _pair_has_position(self, pair: tuple[InstrumentId, InstrumentId]) -> bool:
        return any(not self.portfolio.is_flat(instrument_id) for instrument_id in pair)

    def _evaluate_pair(self, pair: tuple[InstrumentId, InstrumentId]) -> None:
        if self._pending_by_pair.get(pair, 0) > 0:
            return
        if self._cooldown_by_pair.get(pair, 0) > 0:
            self._cooldown_by_pair[pair] -= 1
            return
        if self._entries_by_pair.get(pair, 0) >= int(self.config.max_entries_per_pair):
            return
        if self._pair_has_position(pair):
            return

        states = [self._best_ask_state(instrument_id) for instrument_id in pair]
        if any(state is None for state in states):
            return
        assert states[0] is not None
        assert states[1] is not None

        asks = [Decimal(str(states[i][0])) for i in range(2)]
        ask_sizes = [Decimal(str(states[i][1])) for i in range(2)]
        spreads = [float(states[i][2]) for i in range(2)]

        if any(float(ask) > float(self.config.max_leg_price) for ask in asks):
            return
        if any(spread > float(self.config.max_spread) for spread in spreads):
            return
        if any(size < Decimal(str(self.config.min_visible_size)) for size in ask_sizes):
            return

        desired_size = min(Decimal(str(self.config.trade_size)), ask_sizes[0], ask_sizes[1])
        if desired_size <= 0:
            return

        avg_prices = [self._avg_entry_price(pair[i], desired_size) for i in range(2)]
        if avg_prices[0] is None or avg_prices[1] is None:
            return
        avg_price_decimals = [Decimal(str(avg_prices[i])) for i in range(2)]

        if any(
            float(avg_price) > float(self.config.max_leg_price) for avg_price in avg_price_decimals
        ):
            return

        expected_slippages = [float(avg_price_decimals[i] - asks[i]) for i in range(2)]
        if any(
            slippage > float(self.config.max_expected_slippage) for slippage in expected_slippages
        ):
            return

        fee_rates = (
            [self._instrument_fee_rate(pair[i]) for i in range(2)]
            if self.config.include_taker_fees_in_signal
            else [Decimal("0"), Decimal("0")]
        )
        fees = [
            _fee_per_share(price=avg_price_decimals[i], taker_fee=fee_rates[i]) for i in range(2)
        ]
        net_unit_cost = avg_price_decimals[0] + avg_price_decimals[1] + fees[0] + fees[1]
        edge = Decimal("1") - net_unit_cost

        if edge < Decimal(str(self.config.min_net_edge)):
            return
        if net_unit_cost > Decimal(str(self.config.max_total_cost)):
            return

        free_balance = self._free_quote_balance(pair[0])
        if free_balance is not None:
            affordable_size = (free_balance * ENTRY_AFFORDABILITY_BUFFER) / net_unit_cost
            desired_size = min(desired_size, affordable_size)
        if desired_size <= 0:
            return

        quantities = [self._rounded_quantity(pair[i], desired_size) for i in range(2)]
        if quantities[0] is None or quantities[1] is None:
            return

        self._submit_pair_entry(
            pair=pair,
            quantities=quantities,
            visible_size=float(min(ask_sizes[0], ask_sizes[1])),
            net_unit_cost=float(net_unit_cost),
            edge=float(edge),
        )

    def _submit_pair_entry(
        self,
        *,
        pair: tuple[InstrumentId, InstrumentId],
        quantities: list[object],
        visible_size: float,
        net_unit_cost: float,
        edge: float,
    ) -> None:
        self.log.info(
            "Submitting binary pair arb entry "
            f"pair={pair}, net_unit_cost={net_unit_cost:.6f}, edge={edge:.6f}, "
            f"visible_size={visible_size:.6f}"
        )
        tags = [format_order_intent_tag("pair_arb_entry")]
        visible_liquidity_tag = format_visible_liquidity_tag(visible_size)
        if visible_liquidity_tag is not None:
            tags.append(visible_liquidity_tag)

        orders = [
            self.order_factory.market(
                instrument_id=pair[i],
                order_side=OrderSide.BUY,
                quantity=quantities[i],
                time_in_force=TimeInForce.IOC,
                tags=tags,
            )
            for i in range(2)
        ]
        self._pending_by_pair[pair] = len(orders)
        self._entries_by_pair[pair] = self._entries_by_pair.get(pair, 0) + 1
        self._cooldown_by_pair[pair] = int(self.config.reentry_cooldown_updates)
        try:
            for order in orders:
                self.submit_order(order)
        except Exception:
            self._pending_by_pair[pair] = 0
            self._entries_by_pair[pair] = max(0, self._entries_by_pair.get(pair, 0) - 1)
            self._cooldown_by_pair[pair] = 0
            raise

    def _event_order_is_closed(self, event) -> bool:  # type: ignore[no-untyped-def]
        client_order_id = getattr(event, "client_order_id", None)
        if client_order_id is None:
            return True
        try:
            order = self.cache.order(client_order_id)
        except (AttributeError, KeyError, TypeError):
            return True
        if order is None:
            return True
        is_closed = getattr(order, "is_closed", True)
        if callable(is_closed):
            return bool(is_closed())
        return bool(is_closed)

    def _mark_order_event(self, event) -> None:  # type: ignore[no-untyped-def]
        instrument_id = getattr(event, "instrument_id", None)
        pair = self._pair_by_instrument.get(instrument_id)
        if pair is None:
            return
        if self._event_order_is_closed(event):
            self._pending_by_pair[pair] = max(0, self._pending_by_pair.get(pair, 0) - 1)

    def _rollback_empty_failed_pair_entry(self, event) -> None:  # type: ignore[no-untyped-def]
        instrument_id = getattr(event, "instrument_id", None)
        pair = self._pair_by_instrument.get(instrument_id)
        if pair is None:
            return
        if self._pending_by_pair.get(pair, 0) > 0 or self._pair_has_position(pair):
            return
        self._entries_by_pair[pair] = max(0, self._entries_by_pair.get(pair, 0) - 1)
        self._cooldown_by_pair[pair] = 0

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        self._mark_order_event(event)

    def on_order_rejected(self, event) -> None:  # type: ignore[no-untyped-def]
        self._mark_order_event(event)
        self._rollback_empty_failed_pair_entry(event)

    def on_order_denied(self, event) -> None:  # type: ignore[no-untyped-def]
        self._mark_order_event(event)
        self._rollback_empty_failed_pair_entry(event)

    def on_order_canceled(self, event) -> None:  # type: ignore[no-untyped-def]
        self._mark_order_event(event)
        self._rollback_empty_failed_pair_entry(event)

    def on_order_expired(self, event) -> None:  # type: ignore[no-untyped-def]
        self._mark_order_event(event)
        self._rollback_empty_failed_pair_entry(event)

    def on_stop(self) -> None:
        for instrument_id in self.config.instrument_ids:
            self.cancel_all_orders(instrument_id)
        if not self.config.hold_to_resolution:
            self.log.warning(
                "hold_to_resolution=False is not implemented for pair arbitrage; "
                "positions are left for the runner/settlement model."
            )

    def on_reset(self) -> None:
        self._books.clear()
        self._instruments.clear()
        self._pairs.clear()
        self._pair_by_instrument.clear()
        self._pending_by_pair.clear()
        self._cooldown_by_pair.clear()
        self._entries_by_pair.clear()
