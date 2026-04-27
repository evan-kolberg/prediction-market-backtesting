from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from nautilus_trader.core.rust.model import OrderType
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.objects import Currency

from prediction_market_extensions.adapters.polymarket.loaders import PolymarketDataLoader
from prediction_market_extensions.adapters.polymarket.fee_model import (
    PolymarketFeeModel,
    calculate_maker_rebate,
    infer_maker_rebate_rate,
)
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


def test_calculate_maker_rebate_uses_fee_equivalent_share() -> None:
    rebate = calculate_maker_rebate(
        quantity=Decimal(100),
        price=Decimal("0.5"),
        fee_rate_bps=Decimal(30),
        maker_rebate_rate=Decimal("0.25"),
    )

    assert rebate == 0.01875


def test_infer_maker_rebate_rate_uses_crypto_rate() -> None:
    rate = infer_maker_rebate_rate(
        market_info={"tags": ["Crypto", "All"]},
        fee_rate_bps=Decimal(30),
    )

    assert rate == Decimal("0.20")


def test_infer_maker_rebate_rate_uses_default_fee_enabled_rate() -> None:
    rate = infer_maker_rebate_rate(
        market_info={"tags": ["Sports", "All"]},
        fee_rate_bps=Decimal(30),
    )

    assert rate == Decimal("0.25")


def test_infer_maker_rebate_rate_zero_when_fee_free() -> None:
    rate = infer_maker_rebate_rate(
        market_info={"tags": ["Sports", "All"]},
        fee_rate_bps=Decimal(0),
    )

    assert rate == Decimal("0")


def test_infer_maker_rebate_rate_zero_when_fee_enabled_but_unclassified() -> None:
    rate = infer_maker_rebate_rate(
        market_info={},
        fee_rate_bps=Decimal(35),
    )

    assert rate == Decimal("0")


def test_infer_maker_rebate_rate_can_use_documented_fee_rate() -> None:
    rate = infer_maker_rebate_rate(
        market_info={},
        fee_rate_bps=Decimal(720),
    )

    assert rate == Decimal("0.20")


def test_unknown_limit_liquidity_does_not_assume_maker_rebate() -> None:
    commission = PolymarketFeeModel().get_commission(
        SimpleNamespace(order_type=OrderType.LIMIT),
        fill_qty=100,
        fill_px=0.5,
        instrument=SimpleNamespace(
            info={"tags": ["Sports"]},
            taker_fee=Decimal("0.003"),
            quote_currency=USDC_POS,
        ),
    )

    assert commission.as_double() == 0.075


def test_limit_fills_with_taker_liquidity_pay_taker_fee() -> None:
    commission = PolymarketFeeModel().get_commission(
        SimpleNamespace(order_type=OrderType.LIMIT, liquidity_side=LiquiditySide.TAKER),
        fill_qty=100,
        fill_px=0.5,
        instrument=SimpleNamespace(
            info={"tags": ["Sports"]},
            taker_fee=Decimal("0.003"),
            quote_currency=USDC_POS,
        ),
    )

    assert commission.as_double() == 0.075


def test_limit_fills_with_maker_liquidity_receive_maker_rebate_credit() -> None:
    commission = PolymarketFeeModel().get_commission(
        SimpleNamespace(order_type=OrderType.LIMIT, liquidity_side=LiquiditySide.MAKER),
        fill_qty=100,
        fill_px=0.5,
        instrument=SimpleNamespace(
            info={"tags": ["Sports"]},
            taker_fee=Decimal("0.003"),
            quote_currency=USDC_POS,
        ),
    )

    assert commission.as_double() == -0.01875


def test_limit_order_maker_rebates_can_be_disabled() -> None:
    commission = PolymarketFeeModel(maker_rebates_enabled=False).get_commission(
        SimpleNamespace(order_type=OrderType.LIMIT, liquidity_side=LiquiditySide.MAKER),
        fill_qty=100,
        fill_px=0.5,
        instrument=SimpleNamespace(
            info={"tags": ["Sports"]},
            taker_fee=Decimal("0.003"),
            quote_currency=Currency.from_str("USD"),
        ),
    )

    assert commission.as_double() == 0.0


def test_market_orders_still_pay_polymarket_taker_fee() -> None:
    commission = PolymarketFeeModel().get_commission(
        SimpleNamespace(order_type=OrderType.MARKET),
        fill_qty=100,
        fill_px=0.5,
        instrument=SimpleNamespace(
            info={"tags": ["Sports"]},
            taker_fee=Decimal("0.003"),
            quote_currency=USDC_POS,
        ),
    )

    assert commission.as_double() == 0.075
