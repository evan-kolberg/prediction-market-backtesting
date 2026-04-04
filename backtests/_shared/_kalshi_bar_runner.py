# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""
Shared runner for single-market Kalshi bar backtests.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from typing import Any

import pandas as pd

from backtests._shared._backtest_runtime import print_backtest_result_warnings
from backtests._shared._backtest_runtime import run_market_backtest
from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.adapters.prediction_market.research import print_backtest_summary
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.trading.strategy import Strategy


type StrategyFactory = Callable[[InstrumentId, BarType], Strategy]


def _deduplicate_bars(bars: list[Bar]) -> list[Bar]:
    deduped = {int(bar.ts_event): bar for bar in bars}
    return [deduped[ts] for ts in sorted(deduped)]


async def _load_bars_chunked(
    *,
    loader: KalshiDataLoader,
    ticker: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    interval: str,
    chunk_minutes: int,
    max_retries: int,
    retry_base_delay: float,
) -> list[Bar]:
    bars: list[Bar] = []
    chunk_delta = pd.Timedelta(minutes=chunk_minutes)
    chunk_start = start
    while chunk_start < end:
        chunk_end = min(chunk_start + chunk_delta, end)
        for attempt in range(max_retries + 1):
            try:
                bars.extend(
                    await loader.load_bars(
                        start=chunk_start,
                        end=chunk_end,
                        interval=interval,
                    )
                )
                break
            except RuntimeError as exc:
                if "429" not in str(exc) or attempt >= max_retries:
                    raise
                delay = retry_base_delay * (2**attempt)
                print(f"  rate-limited on {ticker}, retrying in {delay:.0f}s...")
                await asyncio.sleep(delay)
        chunk_start = chunk_end

    return _deduplicate_bars(bars)


async def run_single_market_bar_backtest(
    *,
    name: str,
    market_ticker: str,
    lookback_days: int,
    strategy_factory: StrategyFactory,
    probability_window: int,
    interval: str = "Minutes1",
    chunk_minutes: int = 5_000,
    min_bars: int = 0,
    min_price_range: float = 0.0,
    max_retries: int = 4,
    retry_base_delay: float = 2.0,
    initial_cash: float = 100.0,
    chart_resample_rule: str | None = None,
    emit_summary: bool = True,
    emit_html: bool = True,
    return_chart_layout: bool = False,
    end_time: pd.Timestamp | datetime | None = None,
) -> dict[str, Any] | None:
    end = pd.Timestamp(end_time if end_time is not None else datetime.now(UTC))
    if end.tzinfo is None:
        end = end.tz_localize(UTC)
    start = end - pd.Timedelta(days=lookback_days)

    print(
        f"Loading Kalshi market {market_ticker} "
        f"({interval}, lookback={lookback_days}d, window_end={end.isoformat()})..."
    )

    try:
        loader = await KalshiDataLoader.from_market_ticker(market_ticker)
    except Exception as exc:
        print(f"Unable to load Kalshi market {market_ticker}: {exc}")
        return

    try:
        bars = await _load_bars_chunked(
            loader=loader,
            ticker=market_ticker,
            start=start,
            end=end,
            interval=interval,
            chunk_minutes=chunk_minutes,
            max_retries=max_retries,
            retry_base_delay=retry_base_delay,
        )
    except Exception as exc:
        print(f"Unable to load bars for {market_ticker}: {exc}")
        return

    if len(bars) < min_bars:
        print(f"Skip {market_ticker}: {len(bars)} bars < {min_bars} required")
        return

    closes = [float(bar.close) for bar in bars]
    if closes:
        price_range = max(closes) - min(closes)
        if price_range < min_price_range:
            print(
                f"Skip {market_ticker}: price range {price_range:.3f} < {min_price_range:.3f}"
            )
            return

    if not bars:
        print(f"No bars returned for {market_ticker}")
        return

    strategy = strategy_factory(loader.instrument.id, bars[0].bar_type)
    result = run_market_backtest(
        market_id=market_ticker,
        instrument=loader.instrument,
        data=bars,
        strategy=strategy,
        strategy_name=f"{name}:{market_ticker}",
        output_prefix=name,
        platform="kalshi",
        venue=Venue("KALSHI"),
        base_currency=USD,
        fee_model=KalshiProportionalFeeModel(),
        initial_cash=initial_cash,
        probability_window=probability_window,
        price_attr="close",
        count_key="bars",
        chart_resample_rule=chart_resample_rule,
        market_key="ticker",
        emit_html=emit_html,
        return_chart_layout=return_chart_layout,
        requested_start_ns=int(start.value),
        requested_end_ns=int(end.value),
    )

    if emit_summary:
        print_backtest_summary(
            results=[result],
            market_key="ticker",
            count_key="bars",
            count_label="Bars",
            pnl_label="PnL (USD)",
        )
        print_backtest_result_warnings(results=[result], market_key="ticker")
        if emit_html and result.get("chart_path"):
            print(f"\nLegacy chart saved to {result['chart_path']}")

    return result
