from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.core.rust.model import OrderType
from nautilus_trader.model.objects import Currency

from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader
from prediction_market_extensions.adapters.polymarket.fee_model import PolymarketFeeModel
from prediction_market_extensions.adapters.polymarket.parsing import (
    calculate_commission,
    infer_fee_exponent,
)


def test_calculate_commission_matches_current_polymarket_formula() -> None:
    commission = calculate_commission(
        quantity=Decimal(100),
        price=Decimal("0.5"),
        fee_rate_bps=Decimal(
            30,
        ),
    )

    assert commission == 0.075


def test_calculate_commission_rounds_to_five_decimals() -> None:
    commission = calculate_commission(
        quantity=Decimal(1),
        price=Decimal("0.5"),
        fee_rate_bps=Decimal(
            "2.2",
        ),
    )

    assert commission == 0.00006


def test_infer_fee_exponent_is_now_a_compatibility_shim() -> None:
    assert infer_fee_exponent(Decimal(0)) == 1
    assert infer_fee_exponent(Decimal(35)) == 1
    assert infer_fee_exponent(Decimal(2500)) == 1


def test_fee_rate_enrichment_keeps_maker_fee_zero(monkeypatch) -> None:
    async def fake_fetch_fee_rate_bps(cls, token_id: str, http_client) -> Decimal:
        del cls, token_id, http_client
        return Decimal(35)

    monkeypatch.setattr(
        PolymarketDataLoader, "_fetch_market_fee_rate_bps", classmethod(fake_fetch_fee_rate_bps)
    )

    enriched = asyncio.run(
        PolymarketDataLoader._enrich_market_details_with_fee_rate(
            {"maker_base_fee": 0, "taker_base_fee": 0}, "123", object()
        )
    )

    assert enriched["maker_base_fee"] == "0"
    assert enriched["taker_base_fee"] == "35"


def test_limit_orders_use_zero_polymarket_maker_fee() -> None:
    commission = PolymarketFeeModel().get_commission(
        SimpleNamespace(order_type=OrderType.LIMIT),
        fill_qty=10,
        fill_px=0.55,
        instrument=SimpleNamespace(
            taker_fee=Decimal("0.0035"),
            quote_currency=Currency.from_str("USD"),
        ),
    )

    assert commission.as_double() == 0.0
