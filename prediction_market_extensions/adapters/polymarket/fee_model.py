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

from collections.abc import Iterable, Mapping
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from nautilus_trader.backtest.models import FeeModel
from nautilus_trader.core.rust.model import OrderType
from nautilus_trader.model.objects import Money

from prediction_market_extensions.adapters.polymarket.parsing import calculate_commission

_CRYPTO_MAKER_REBATE_RATE = Decimal("0.20")
_DEFAULT_FEE_ENABLED_MAKER_REBATE_RATE = Decimal("0.25")
_REBATE_QUANTUM = Decimal("0.00001")
_CRYPTO_LABELS = frozenset({"crypto"})
_FEE_ENABLED_NON_CRYPTO_LABELS = frozenset(
    {
        "culture",
        "economics",
        "finance",
        "general",
        "mentions",
        "other",
        "other general",
        "politics",
        "sports",
        "tech",
        "weather",
    }
)
_CRYPTO_FEE_RATE_BPS = frozenset({Decimal("72"), Decimal("720")})
_NON_CRYPTO_FEE_RATE_BPS = frozenset(
    {
        Decimal("30"),
        Decimal("40"),
        Decimal("50"),
        Decimal("300"),
        Decimal("400"),
        Decimal("500"),
    }
)


def _normalize_label(value: object) -> str | None:
    if value is None:
        return None
    label = str(value).strip().casefold()
    if not label:
        return None
    return " ".join(label.replace("_", " ").replace("-", " ").split())


def _iter_tag_labels(tags: object) -> Iterable[str]:
    if isinstance(tags, str):
        label = _normalize_label(tags)
        if label is not None:
            yield label
        return

    if not isinstance(tags, Iterable):
        return

    for tag in tags:
        if isinstance(tag, Mapping):
            for key in ("label", "name", "slug", "title"):
                label = _normalize_label(tag.get(key))
                if label is not None:
                    yield label
        else:
            label = _normalize_label(tag)
            if label is not None:
                yield label


def _market_labels(info: Mapping[str, Any] | None) -> set[str]:
    if not info:
        return set()

    labels: set[str] = set()
    for key in ("category", "category_slug", "tag", "tag_slug"):
        label = _normalize_label(info.get(key))
        if label is not None:
            labels.add(label)

    for key in ("categories", "tags"):
        labels.update(_iter_tag_labels(info.get(key)))

    raw_events = info.get("events")
    if isinstance(raw_events, Iterable) and not isinstance(raw_events, str | bytes):
        for event in raw_events:
            if not isinstance(event, Mapping):
                continue
            for key in ("category", "category_slug", "tag", "tag_slug"):
                label = _normalize_label(event.get(key))
                if label is not None:
                    labels.add(label)
            for key in ("categories", "tags"):
                labels.update(_iter_tag_labels(event.get(key)))

    return labels


def infer_maker_rebate_rate(
    *,
    market_info: Mapping[str, Any] | None,
    fee_rate_bps: Decimal,
) -> Decimal:
    """
    Infer the maker rebate share for a fee-enabled Polymarket fill.

    Polymarket's current fee schedule pays a 20% maker rebate for crypto
    markets and 25% for other fee-enabled categories. Fee-free or
    unclassified markets receive no rebate credit because there is no reliable
    way to identify a rebate share.
    """
    if fee_rate_bps <= 0:
        return Decimal("0")

    labels = _market_labels(market_info)
    if labels & _CRYPTO_LABELS:
        return _CRYPTO_MAKER_REBATE_RATE
    if labels & _FEE_ENABLED_NON_CRYPTO_LABELS:
        return _DEFAULT_FEE_ENABLED_MAKER_REBATE_RATE

    if fee_rate_bps in _CRYPTO_FEE_RATE_BPS:
        return _CRYPTO_MAKER_REBATE_RATE
    if fee_rate_bps in _NON_CRYPTO_FEE_RATE_BPS:
        return _DEFAULT_FEE_ENABLED_MAKER_REBATE_RATE

    return Decimal("0")


def calculate_maker_rebate(
    *,
    quantity: Decimal,
    price: Decimal,
    fee_rate_bps: Decimal,
    maker_rebate_rate: Decimal,
) -> float:
    """
    Calculate a fill-level maker rebate estimate in quote currency.

    Polymarket distributes actual rebates daily from each market's rebate pool.
    For backtests, a per-fill credit equal to the documented rebate share of
    the fill's fee-equivalent value preserves the aggregate economics without
    pretending to know other makers' wallet-level state.
    """
    if fee_rate_bps <= 0 or maker_rebate_rate <= 0:
        return 0.0

    fee_equivalent = Decimal(
        str(calculate_commission(quantity=quantity, price=price, fee_rate_bps=fee_rate_bps))
    )
    rebate = fee_equivalent * maker_rebate_rate
    return float(rebate.quantize(_REBATE_QUANTUM, rounding=ROUND_HALF_UP))


class PolymarketFeeModel(FeeModel):
    """
    Polymarket fee model for backtesting.

    Applies Polymarket's taker fee formula per fill::

        fee = qty x feeRate x p x (1 - p)

    Where:
    - ``feeRate = taker_base_fee_bps / 10_000``
    - ``p`` is the fill price in [0, 1]

    Maker fees remain zero. Eligible passive maker fills receive a rebate
    credit modeled as a negative commission.

    Taker fee rates come from the market payload when available, or from the
    CLOB fee-rate endpoint when the market payload still reports zeros.

    References
    ----------
    https://docs.polymarket.com/trading/fees
    https://docs.polymarket.com/market-makers/maker-rebates
    """

    def __init__(self, *, maker_rebates_enabled: bool = True) -> None:
        self._maker_rebates_enabled = maker_rebates_enabled

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
        # instrument.taker_fee is stored as bps/10_000 (decimal fraction)
        taker_fee_dec = instrument.taker_fee
        if taker_fee_dec is None:
            return Money(Decimal("0"), instrument.quote_currency)
        fee_rate_bps = taker_fee_dec * Decimal(10_000)

        if fee_rate_bps <= 0:
            return Money(Decimal("0"), instrument.quote_currency)

        fill_quantity = Decimal(str(fill_qty))
        fill_price = Decimal(str(fill_px))

        if order.order_type == OrderType.LIMIT:
            # The fee callback does not expose realized maker/taker liquidity.
            # Repo-owned Polymarket book backtests use passive-posting limit
            # orders, so treat their limit fills as maker-side rebates.
            if not self._maker_rebates_enabled:
                return Money(Decimal("0"), instrument.quote_currency)

            rebate_rate = infer_maker_rebate_rate(
                market_info=getattr(instrument, "info", None),
                fee_rate_bps=fee_rate_bps,
            )
            rebate = calculate_maker_rebate(
                quantity=fill_quantity,
                price=fill_price,
                fee_rate_bps=fee_rate_bps,
                maker_rebate_rate=rebate_rate,
            )
            return Money(Decimal(str(-rebate)), instrument.quote_currency)

        commission = calculate_commission(
            quantity=fill_quantity, price=fill_price, fee_rate_bps=fee_rate_bps
        )
        return Money(Decimal(str(commission)), instrument.quote_currency)
