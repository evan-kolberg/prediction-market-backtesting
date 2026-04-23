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

_KALSHI_ORDER_TICK = Decimal("0.01")
_UNLIMITED_BOOK_SIZE = 1_000_000


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

    Entry vs exit is inferred from order side: BUY = entry, SELL = exit.
    This is correct for all LongOnlyPredictionMarketStrategy subclasses
    in this framework (which buy YES to open, sell to close). Strategies
    that sell to open a short position would need to invert the mapping.

    When both methods are non-zero, they stack: the fill price is shifted
    by N ticks PLUS the percentage.

    Limit orders keep the default exchange matching behavior.
    """

    def __init__(
        self,
        *,
        slippage_ticks: int = 1,
        entry_slippage_pct: float = 0.0,
        exit_slippage_pct: float = 0.0,
    ) -> None:
        if slippage_ticks < 0:
            raise ValueError(f"slippage_ticks must be >= 0, got {slippage_ticks}")
        if entry_slippage_pct < 0.0:
            raise ValueError(f"entry_slippage_pct must be >= 0, got {entry_slippage_pct}")
        if exit_slippage_pct < 0.0:
            raise ValueError(f"exit_slippage_pct must be >= 0, got {exit_slippage_pct}")
        self._slippage_ticks = slippage_ticks
        self._entry_slippage_pct = entry_slippage_pct
        self._exit_slippage_pct = exit_slippage_pct
        # The slippage is modeled through a synthetic order book rather than
        # FillModel.is_slipped(), so we disable the built-in L1 slip hook.
        super().__init__(prob_fill_on_limit=1.0, prob_slippage=0.0)

    def get_orderbook_for_fill_simulation(self, instrument, order, best_bid, best_ask):
        if order.order_type == OrderType.LIMIT:
            return None

        tick = effective_prediction_market_slippage_tick(instrument)
        tick_shift = tick * self._slippage_ticks

        # Determine percentage shift based on order side:
        # BUY = entry (taking liquidity from the ask side)
        # SELL = exit (taking liquidity from the bid side)
        is_entry = order.side == OrderSideEnum.BUY
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

        book = OrderBook(instrument_id=instrument.id, book_type=BookType.L2_MBP)

        # Build a symmetric synthetic book at the slipped prices.
        # The matching engine will consume the relevant side depending
        # on order side. Each side has unlimited size to guarantee fill.
        book.add(
            BookOrder(
                side=OrderSide.BUY,
                price=slipped_bid,
                size=Quantity(_UNLIMITED_BOOK_SIZE, instrument.size_precision),
                order_id=1,
            ),
            0,
            0,
        )
        book.add(
            BookOrder(
                side=OrderSide.SELL,
                price=slipped_ask,
                size=Quantity(_UNLIMITED_BOOK_SIZE, instrument.size_precision),
                order_id=2,
            ),
            0,
            0,
        )

        return book
