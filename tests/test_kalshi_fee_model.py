from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
from nautilus_trader.model.objects import Currency

from prediction_market_extensions.adapters.kalshi.fee_model import KalshiProportionalFeeModel


def test_kalshi_fee_waiver_is_evaluated_at_fill_time() -> None:
    model = KalshiProportionalFeeModel()
    instrument = SimpleNamespace(
        taker_fee=Decimal("0.07"),
        quote_currency=Currency.from_str("USD"),
        info={"fee_waiver_expiration_time": "2026-04-10T00:00:00+00:00"},
    )

    waived = model.get_commission(
        SimpleNamespace(ts_init=pd.Timestamp("2026-04-05T00:00:00+00:00").value),
        fill_qty=10,
        fill_px=0.50,
        instrument=instrument,
    )
    charged = model.get_commission(
        SimpleNamespace(ts_init=pd.Timestamp("2026-04-12T00:00:00+00:00").value),
        fill_qty=10,
        fill_px=0.50,
        instrument=instrument,
    )

    assert waived.as_double() == 0.0
    assert charged.as_double() == 0.18
