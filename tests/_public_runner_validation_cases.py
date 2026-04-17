from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import TradeId

from prediction_market_extensions.adapters.kalshi.providers import market_dict_to_instrument
from prediction_market_extensions.backtesting import _prediction_market_backtest as backtest_module
from prediction_market_extensions.backtesting._experiments import run_experiment

RESULT_MARKER = "PUBLIC_RUNNER_VALIDATION_RESULT="


def _timestamp_ns(start: datetime, offset_seconds: int) -> int:
    return int((start + timedelta(seconds=offset_seconds)).timestamp() * 1_000_000_000)


def _make_trade_ticks(
    *,
    instrument: Any,
    start: datetime,
    prices: Sequence[float],
    size: float,
    trade_id_prefix: str,
) -> tuple[TradeTick, ...]:
    return tuple(
        TradeTick(
            instrument_id=instrument.id,
            price=instrument.make_price(price),
            size=instrument.make_qty(size),
            aggressor_side=AggressorSide.BUYER,
            trade_id=TradeId(f"{trade_id_prefix}{index:04d}"),
            ts_event=_timestamp_ns(start, index),
            ts_init=_timestamp_ns(start, index),
        )
        for index, price in enumerate(prices)
    )


def _minimal_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "fills": result["fills"],
        "pnl": result["pnl"],
        "terminated_early": result["terminated_early"],
        "realized_outcome": result["realized_outcome"],
        "fill_events": result["fill_events"],
    }


def _run_kalshi_breakout_case(*, prices: Sequence[float]) -> dict[str, Any]:
    import backtests.kalshi_trade_tick_breakout as runner

    market = {
        "ticker": "KXLAYOFFSYINFO-26-494000",
        "event_ticker": "SYNTHETIC",
        "title": "Synthetic validation market",
        "open_time": "2026-01-01T00:00:00+00:00",
        "close_time": "2026-12-31T00:00:00+00:00",
        "result": "yes",
    }
    instrument = market_dict_to_instrument(market)
    trades = _make_trade_ticks(
        instrument=instrument,
        start=datetime(2026, 3, 15, tzinfo=timezone.utc),
        prices=prices,
        size=10,
        trade_id_prefix="K",
    )

    class Loader:
        def __init__(self) -> None:
            self.instrument = instrument

        async def load_trades(self, start, end):  # type: ignore[no-untyped-def]
            return trades

    class LoaderFactory:
        @classmethod
        async def from_market_ticker(cls, ticker: str):  # type: ignore[no-untyped-def]
            if ticker != runner.REPLAYS[0].market_ticker:
                raise AssertionError(f"unexpected ticker {ticker!r}")
            return Loader()

    backtest_module.KalshiDataLoader = LoaderFactory
    experiment = replace(
        runner.EXPERIMENT,
        min_trades=0,
        min_price_range=0.0,
        nautilus_log_level="ERROR",
        emit_html=False,
        report=None,
        chart_output_path=None,
    )
    results = run_experiment(experiment)
    if len(results) != 1:
        raise AssertionError(f"expected one result, received {len(results)}")
    return _minimal_result(results[0])


def _run_polymarket_trade_case() -> dict[str, Any]:
    import backtests.polymarket_trade_tick_vwap_reversion as runner

    market_info = {
        "condition_id": "0x" + "1" * 64,
        "question": "Synthetic validation market",
        "minimum_tick_size": "0.01",
        "minimum_order_size": "1",
        "end_date_iso": "2026-12-31T00:00:00Z",
        "maker_base_fee": "0",
        "taker_base_fee": "0",
        "result": "yes",
    }
    instrument = parse_polymarket_instrument(
        market_info=market_info,
        token_id="2" * 64,
        outcome="Yes",
        ts_init=0,
    )
    prices = [0.50] * 30 + [0.49, 0.50, 0.505, 0.49, 0.50]
    trades = _make_trade_ticks(
        instrument=instrument,
        start=datetime(2026, 2, 21, 16, tzinfo=timezone.utc),
        prices=prices,
        size=10,
        trade_id_prefix="P",
    )

    class Loader:
        def __init__(self) -> None:
            self.instrument = instrument

        async def load_trades(self, start, end):  # type: ignore[no-untyped-def]
            return trades

    class LoaderFactory:
        @classmethod
        async def from_market_slug(cls, slug: str, *, token_index: int = 0):  # type: ignore[no-untyped-def]
            if slug != runner.REPLAYS[0].market_slug:
                raise AssertionError(f"unexpected slug {slug!r}")
            if token_index != runner.REPLAYS[0].token_index:
                raise AssertionError(f"unexpected token_index {token_index!r}")
            return Loader()

    backtest_module.PolymarketDataLoader = LoaderFactory
    experiment = replace(
        runner.EXPERIMENT,
        min_trades=0,
        min_price_range=0.0,
        nautilus_log_level="ERROR",
        emit_html=False,
        report=None,
        chart_output_path=None,
    )
    results = run_experiment(experiment)
    if len(results) != 1:
        raise AssertionError(f"expected one result, received {len(results)}")
    return _minimal_result(results[0])


def _run_case(case: str) -> dict[str, Any]:
    if case == "kalshi-baseline":
        return _run_kalshi_breakout_case(
            prices=[0.50] * 61 + [0.55, 0.59, 0.60, 0.53, 0.51, 0.48, 0.46, 0.44]
        )
    if case == "kalshi-prefix-normal":
        return _run_kalshi_breakout_case(prices=[0.50] * 61 + [0.55, 0.59, 0.60, 0.53, 0.51])
    if case == "kalshi-prefix-stressed":
        return _run_kalshi_breakout_case(prices=[0.50] * 61 + [0.55, 0.99, 0.01, 0.99, 0.01])
    if case == "polymarket-trade":
        return _run_polymarket_trade_case()
    raise AssertionError(f"unknown validation case {case!r}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("case")
    args = parser.parse_args()

    result = _run_case(args.case)
    print(f"{RESULT_MARKER}{json.dumps(result, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
