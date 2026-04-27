# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11 and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.model.enums import BookType, OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import StrategyConfig

from strategies._validation import (
    require_finite_nonnegative_float,
    require_positive_decimal,
    require_probability,
)
from strategies.core import LongOnlyPredictionMarketStrategy


def _validate_late_favorite_config(
    *,
    trade_size: Decimal,
    entry_price: float,
    activation_start_time_ns: int,
    market_close_time_ns: int,
) -> None:
    if trade_size <= 0:
        raise ValueError(f"trade_size must be > 0, got {trade_size}")
    if not 0.0 <= float(entry_price) <= 1.0:
        raise ValueError(f"entry_price must be in [0.0, 1.0], got {entry_price}")
    if int(activation_start_time_ns) < 0:
        raise ValueError(f"activation_start_time_ns must be >= 0, got {activation_start_time_ns}")
    if int(market_close_time_ns) < 0:
        raise ValueError(f"market_close_time_ns must be >= 0, got {market_close_time_ns}")
    if (
        int(activation_start_time_ns) > 0
        and int(market_close_time_ns) > 0
        and int(activation_start_time_ns) > int(market_close_time_ns)
    ):
        raise ValueError(
            "activation_start_time_ns must be <= market_close_time_ns when both are set"
        )


class BookLateFavoriteLimitHoldConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(25)
    activation_start_time_ns: int = 0
    market_close_time_ns: int = 0
    entry_price: float = 0.90

    def __post_init__(self) -> None:
        _validate_late_favorite_config(
            trade_size=self.trade_size,
            entry_price=self.entry_price,
            activation_start_time_ns=self.activation_start_time_ns,
            market_close_time_ns=self.market_close_time_ns,
        )


class _LateFavoriteLimitHoldBase(LongOnlyPredictionMarketStrategy):
    """
    Submit one limit buy once a late-game favorite reaches the entry threshold.

    This strategy is intended for resolved-market backtests which mark any
    remaining position to settlement after the backtest completes.
    """

    def __init__(self, config: BookLateFavoriteLimitHoldConfig) -> None:
        super().__init__(config)
        self._entered_once = False

    def _on_price(
        self,
        *,
        signal_price: float,
        order_price: float,
        ts_event_ns: int,
        visible_size: float | None = None,
        exit_visible_size: float | None = None,
    ) -> None:
        self._remember_market_context(
            entry_reference_price=order_price,
            entry_visible_size=visible_size,
            exit_visible_size=exit_visible_size,
        )
        if self._pending or self._in_position() or self._entered_once:
            return

        if int(self.config.activation_start_time_ns) > 0 and ts_event_ns < int(
            self.config.activation_start_time_ns
        ):
            return
        if int(self.config.market_close_time_ns) > 0 and ts_event_ns > int(
            self.config.market_close_time_ns
        ):
            return

        if signal_price < float(self.config.entry_price):
            return

        assert self._instrument is not None
        quantity = self._entry_quantity(reference_price=order_price, visible_size=visible_size)
        if quantity is None:
            return
        order = self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=quantity,
            price=self._instrument.make_price(order_price),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self._pending = True

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._entered_once = True

    def on_order_expired(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_expired(event)

    def on_order_accepted(self, event) -> None:  # type: ignore[no-untyped-def]
        self._pending = False
        self._entered_once = True

    def on_stop(self) -> None:
        # Leave filled positions open so the runner can mark them to settlement.
        self.cancel_all_orders(self.config.instrument_id)

    def on_reset(self) -> None:
        super().on_reset()
        self._entered_once = False


class BookLateFavoriteLimitHoldStrategy(_LateFavoriteLimitHoldBase):
    def _subscribe(self) -> None:
        self.subscribe_order_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
        )

    def on_order_book(self, order_book) -> None:  # type: ignore[no-untyped-def]
        bid = order_book.best_bid_price()
        ask = order_book.best_ask_price()
        if bid is None or ask is None:
            return
        mid = (float(bid) + float(ask)) / 2.0
        ask_size = order_book.best_ask_size()
        self._on_price(
            signal_price=mid,
            order_price=float(ask),
            ts_event_ns=int(order_book.ts_event),
            visible_size=float(ask_size) if ask_size is not None else None,
        )


class BookLateFavoriteTakerHoldConfig(StrategyConfig, frozen=True):  # type: ignore[call-arg]
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal(5)
    activation_start_time_ns: int = 0
    market_close_time_ns: int = 0
    min_midpoint: float = 0.90
    min_bid_price: float = 0.88
    max_entry_price: float = 0.99
    max_spread: float = 0.04
    min_visible_size: float = 5.0
    enable_cheap_no_entry: bool = False
    max_cheap_no_entry_price: float = 0.05
    max_cheap_no_midpoint: float = 0.10
    max_cheap_no_spread: float = 0.05

    def __post_init__(self) -> None:
        require_positive_decimal("trade_size", self.trade_size)
        require_probability("min_midpoint", self.min_midpoint)
        require_probability("min_bid_price", self.min_bid_price)
        require_probability("max_entry_price", self.max_entry_price)
        require_probability("max_spread", self.max_spread)
        require_probability("max_cheap_no_entry_price", self.max_cheap_no_entry_price)
        require_probability("max_cheap_no_midpoint", self.max_cheap_no_midpoint)
        require_probability("max_cheap_no_spread", self.max_cheap_no_spread)
        require_finite_nonnegative_float("min_visible_size", self.min_visible_size)
        if int(self.activation_start_time_ns) < 0:
            raise ValueError(
                f"activation_start_time_ns must be >= 0, got {self.activation_start_time_ns}"
            )
        if int(self.market_close_time_ns) < 0:
            raise ValueError(f"market_close_time_ns must be >= 0, got {self.market_close_time_ns}")
        if (
            int(self.activation_start_time_ns) > 0
            and int(self.market_close_time_ns) > 0
            and int(self.activation_start_time_ns) > int(self.market_close_time_ns)
        ):
            raise ValueError(
                "activation_start_time_ns must be <= market_close_time_ns when both are set"
            )


class BookLateFavoriteTakerHoldStrategy(LongOnlyPredictionMarketStrategy):
    """
    Buy a late-window favorite, optionally also taking cheap complementary tails.

    This is intentionally more conservative than ``BookLateFavoriteLimitHoldStrategy``:
    it requires the late book to show a high midpoint, high bid support, a bounded
    executable ask, enough visible ask size, and a bounded spread before submitting
    a marketable IOC favorite entry. If enabled, the cheap complementary path uses
    separate low-price and spread thresholds. Positions are left open for the binary
    settlement PnL policy instead of being force-exited at strategy stop.
    """

    def __init__(self, config: BookLateFavoriteTakerHoldConfig) -> None:
        super().__init__(config)
        self._entered_once = False

    def _subscribe(self) -> None:
        self.subscribe_order_book_deltas(
            instrument_id=self.config.instrument_id,
            book_type=BookType.L2_MBP,
        )

    def on_order_book(self, order_book) -> None:  # type: ignore[no-untyped-def]
        bid = order_book.best_bid_price()
        ask = order_book.best_ask_price()
        if bid is None or ask is None:
            return
        bid_float = float(bid)
        ask_float = float(ask)
        ask_size = order_book.best_ask_size()
        spread = ask_float - bid_float
        self._on_book_signal(
            bid=bid_float,
            ask=ask_float,
            midpoint=(bid_float + ask_float) / 2.0,
            spread=spread,
            ask_size=float(ask_size) if ask_size is not None else None,
            ts_event_ns=int(order_book.ts_event),
        )

    def _entry_window_is_open(self, ts_event_ns: int) -> bool:
        activation_start_ns = int(self.config.activation_start_time_ns)
        if activation_start_ns > 0 and ts_event_ns < activation_start_ns:
            return False

        market_close_ns = int(self.config.market_close_time_ns)
        if market_close_ns > 0 and ts_event_ns > market_close_ns:
            return False

        return True

    def _on_book_signal(
        self,
        *,
        bid: float,
        ask: float,
        midpoint: float,
        spread: float,
        ask_size: float | None,
        ts_event_ns: int,
    ) -> None:
        self._remember_market_context(
            entry_reference_price=ask,
            entry_visible_size=ask_size,
            exit_visible_size=None,
        )
        if self._pending or self._in_position() or self._entered_once:
            return
        if not self._entry_window_is_open(ts_event_ns):
            return
        if ask <= bid or spread <= 0.0:
            return
        favorite_signal = (
            midpoint >= float(self.config.min_midpoint)
            and bid >= float(self.config.min_bid_price)
            and ask <= float(self.config.max_entry_price)
            and spread <= float(self.config.max_spread)
        )
        cheap_no_signal = (
            bool(self.config.enable_cheap_no_entry)
            and midpoint <= float(self.config.max_cheap_no_midpoint)
            and ask <= float(self.config.max_cheap_no_entry_price)
            and spread <= float(self.config.max_cheap_no_spread)
        )
        if not favorite_signal and not cheap_no_signal:
            return
        if ask_size is None or ask_size < float(self.config.min_visible_size):
            return

        self._submit_entry(reference_price=ask, visible_size=ask_size)

    def on_order_filled(self, event) -> None:  # type: ignore[no-untyped-def]
        super().on_order_filled(event)
        if event.order_side == OrderSide.BUY:
            self._entered_once = True

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)

    def on_reset(self) -> None:
        super().on_reset()
        self._entered_once = False
