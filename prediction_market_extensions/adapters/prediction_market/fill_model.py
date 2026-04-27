# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.backtest.models import FillModel
from nautilus_trader.core.rust.model import BookType, OrderSide, OrderType
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import BookOrder
from nautilus_trader.model.enums import OrderSide as OrderSideEnum
from nautilus_trader.model.objects import Quantity

from prediction_market_extensions.adapters.prediction_market.order_tags import (
    parse_order_intent,
    parse_visible_liquidity,
)

_KALSHI_ORDER_TICK = Decimal("0.01")
_DEFAULT_LIMIT_FILL_PROBABILITY = 0.25
_DEFAULT_MIN_SYNTHETIC_BOOK_SIZE = 10.0
_DEFAULT_SYNTHETIC_BOOK_DEPTH_MULTIPLIER = 1.0


def effective_prediction_market_slippage_tick(instrument) -> float:
    """
    Return the effective taker slippage tick for a prediction-market instrument.

    Polymarket publishes a market-specific minimum tick size, so we can use the
    instrument's `price_increment` directly.

    Kalshi's API exposes 4-decimal fixed-point dollar prices, but the current
    minimum tradable order tick is still one cent. For taker slippage modeling
    we therefore use $0.01 on the 0-1 probability scale.
    """
    if str(instrument.id.venue) == "KALSHI":
        return float(_KALSHI_ORDER_TICK)

    return float(instrument.price_increment)


def _coerce_positive_float(value: object) -> float | None:
    if value is None:
        return None
    if hasattr(value, "as_double"):
        try:
            numeric = float(value.as_double())
        except (TypeError, ValueError):
            return None
    else:
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
    if numeric <= 0.0:
        return None
    return numeric


def _order_quantity(order) -> float | None:
    for attr in ("leaves_qty", "quantity"):
        numeric = _coerce_positive_float(getattr(order, attr, None))
        if numeric is not None:
            return numeric
    return None


def _is_entry_order(order) -> bool:
    if getattr(order, "reduce_only", False):
        return False
    intent = parse_order_intent(getattr(order, "tags", None))
    if intent == "entry":
        return True
    if intent == "exit":
        return False
    return order.side == OrderSideEnum.BUY


def _synthetic_book_size(
    order, *, min_synthetic_book_size: float, synthetic_book_depth_multiplier: float
) -> float:
    observed_visible_liquidity = parse_visible_liquidity(getattr(order, "tags", None))
    quantity = _order_quantity(order)
    if observed_visible_liquidity is not None:
        return max(
            observed_visible_liquidity * synthetic_book_depth_multiplier,
            0.0,
        )
    if quantity is not None:
        return max(min_synthetic_book_size, quantity)
    return min_synthetic_book_size


class PredictionMarketTakerFillModel(FillModel):
    """
    Approximate taker slippage for prediction-market backtests.

    For trade-tick replays (no L2 book data), this model constructs a
    synthetic L2 order book shifted adverse to the taker, so the matching
    engine fills at a worse price than the last trade print.

    Slippage can be configured two ways (composable):

    **Tick-based** (``slippage_ticks``):
    Shifts the synthetic book by N venue ticks adverse. Default is 1.
    Kalshi 1 tick = $0.01; Polymarket 1 tick = instrument price_increment.

    **Percentage-based** (``entry_slippage_pct``, ``exit_slippage_pct``):
    Shifts the synthetic book by a percentage of the current price.
    For example, ``entry_slippage_pct=0.02`` on a BUY at $0.50 shifts
    the fill price to $0.51 (2% of $0.50). Set to 0.0 to disable.
    Entry and exit can have different slippage percentages, reflecting
    the reality that exiting a binary-option position is often harder
    (thinner book, more urgency) than entering.

    Entry vs exit is inferred from repo-owned order tags or reduce-only
    instructions first, with order side only as a fallback. This keeps
    long exits and future short-cover flows on the correct slippage path.

    When both methods are non-zero, they stack: the fill price is shifted
    by N ticks PLUS the percentage.

    Limit orders still use Nautilus' passive-book heuristics, but no longer
    default to a 100% touch-fill probability.
    """

    def __init__(
        self,
        *,
        slippage_ticks: int = 1,
        entry_slippage_pct: float = 0.0,
        exit_slippage_pct: float = 0.0,
        prob_fill_on_limit: float = _DEFAULT_LIMIT_FILL_PROBABILITY,
        min_synthetic_book_size: float = _DEFAULT_MIN_SYNTHETIC_BOOK_SIZE,
        synthetic_book_depth_multiplier: float = _DEFAULT_SYNTHETIC_BOOK_DEPTH_MULTIPLIER,
    ) -> None:
        if slippage_ticks < 0:
            raise ValueError(f"slippage_ticks must be >= 0, got {slippage_ticks}")
        if entry_slippage_pct < 0.0:
            raise ValueError(f"entry_slippage_pct must be >= 0, got {entry_slippage_pct}")
        if exit_slippage_pct < 0.0:
            raise ValueError(f"exit_slippage_pct must be >= 0, got {exit_slippage_pct}")
        if not 0.0 <= prob_fill_on_limit <= 1.0:
            raise ValueError(
                f"prob_fill_on_limit must be within [0.0, 1.0], got {prob_fill_on_limit}"
            )
        if min_synthetic_book_size <= 0.0:
            raise ValueError(
                f"min_synthetic_book_size must be > 0.0, got {min_synthetic_book_size}"
            )
        if synthetic_book_depth_multiplier <= 0.0:
            raise ValueError(
                "synthetic_book_depth_multiplier must be > 0.0, got "
                f"{synthetic_book_depth_multiplier}"
            )
        self._slippage_ticks = slippage_ticks
        self._entry_slippage_pct = entry_slippage_pct
        self._exit_slippage_pct = exit_slippage_pct
        self._prob_fill_on_limit = prob_fill_on_limit
        self._min_synthetic_book_size = min_synthetic_book_size
        self._synthetic_book_depth_multiplier = synthetic_book_depth_multiplier
        # The slippage is modeled through a synthetic order book rather than
        # FillModel.is_slipped(), so we disable the built-in L1 slip hook.
        super().__init__(prob_fill_on_limit=prob_fill_on_limit, prob_slippage=0.0)

    def get_orderbook_for_fill_simulation(self, instrument, order, best_bid, best_ask):
        if order.order_type == OrderType.LIMIT:
            return None

        tick = effective_prediction_market_slippage_tick(instrument)
        tick_shift = tick * self._slippage_ticks

        is_entry = _is_entry_order(order)
        pct = self._entry_slippage_pct if is_entry else self._exit_slippage_pct

        # Compute total adverse shift: tick-based + percentage-based
        # For BUY: fill at ask + shift (worse for buyer)
        # For SELL: fill at bid - shift (worse for seller)
        raw_ask = float(best_ask)
        raw_bid = float(best_bid)
        pct_shift_ask = raw_ask * pct
        pct_shift_bid = raw_bid * pct
        slipped_ask = min(1.0, raw_ask + tick_shift + pct_shift_ask)
        slipped_bid = max(0.0, raw_bid - tick_shift - pct_shift_bid)

        slipped_bid = instrument.make_price(slipped_bid)
        slipped_ask = instrument.make_price(slipped_ask)
        synthetic_book_size = Quantity(
            _synthetic_book_size(
                order,
                min_synthetic_book_size=self._min_synthetic_book_size,
                synthetic_book_depth_multiplier=self._synthetic_book_depth_multiplier,
            ),
            instrument.size_precision,
        )

        book = OrderBook(instrument_id=instrument.id, book_type=BookType.L2_MBP)

        # Build a symmetric synthetic book at the slipped prices with finite
        # depth. When trade/quote-tick strategies attach visible liquidity, the
        # engine can now produce partial fills instead of guaranteed full fills.
        book.add(
            BookOrder(
                side=OrderSide.BUY,
                price=slipped_bid,
                size=synthetic_book_size,
                order_id=1,
            ),
            0,
            0,
        )
        book.add(
            BookOrder(
                side=OrderSide.SELL,
                price=slipped_ask,
                size=synthetic_book_size,
                order_id=2,
            ),
            0,
            0,
        )

        return book
