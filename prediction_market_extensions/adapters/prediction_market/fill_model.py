# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.backtest.models import FillModel
from nautilus_trader.core.rust.model import BookType
from nautilus_trader.core.rust.model import OrderSide
from nautilus_trader.core.rust.model import OrderType
from nautilus_trader.model.book import OrderBook
from nautilus_trader.model.data import BookOrder
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

    The shared prediction-market backtests replay trades/bars without full
    historical order-book depth, so they cannot model true price walking.
    Instead, this fill model applies a deterministic one-tick adverse move for
    non-limit orders:

    - Polymarket: one market tick using the instrument's tick size
    - Kalshi: one cent, matching the current minimum order tick

    Limit orders keep the default exchange matching behavior.
    """

    def __init__(self) -> None:
        # The slippage is modeled through a synthetic order book rather than
        # FillModel.is_slipped(), so we disable the built-in L1 slip hook.
        super().__init__(prob_fill_on_limit=1.0, prob_slippage=0.0)

    def get_orderbook_for_fill_simulation(self, instrument, order, best_bid, best_ask):
        if order.order_type == OrderType.LIMIT:
            return None

        tick = effective_prediction_market_slippage_tick(instrument)
        slipped_bid = instrument.make_price(max(0.0, float(best_bid) - tick))
        slipped_ask = instrument.make_price(min(1.0, float(best_ask) + tick))

        book = OrderBook(instrument_id=instrument.id, book_type=BookType.L2_MBP)

        # Build a symmetric synthetic book one adverse tick away from the touch.
        # The matching engine will consume the relevant side depending on order side.
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
