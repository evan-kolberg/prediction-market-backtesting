# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11, 2026-03-15, and 2026-03-31.
# See the repository NOTICE file for provenance and licensing scope.

"""
Shared runner for single-market Polymarket PMXT L2 backtests.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
import os
from typing import Any

import pandas as pd

from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.adapters.polymarket.fee_model import PolymarketFeeModel
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    infer_realized_outcome,
)
from nautilus_trader.adapters.prediction_market.research import print_backtest_summary
from nautilus_trader.adapters.prediction_market.research import run_market_backtest
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

from backtests._shared.data_sources.pmxt import configured_pmxt_data_source
from backtests._shared.data_sources.pmxt import (
    RunnerPolymarketPMXTDataLoader as PolymarketPMXTDataLoader,
)


type StrategyFactory = Callable[[InstrumentId], Strategy]


def _apply_window_env_overrides(
    *,
    start_time: pd.Timestamp | datetime | str | None,
    end_time: pd.Timestamp | datetime | str | None,
    lookback_hours: float | None,
) -> tuple[
    pd.Timestamp | datetime | str | None,
    pd.Timestamp | datetime | str | None,
    float | None,
]:
    override_start = os.getenv("START_TIME", "").strip()
    override_end = os.getenv("END_TIME", "").strip()
    override_lookback = os.getenv("LOOKBACK_HOURS", "").strip()

    if override_start:
        start_time = override_start
    if override_end:
        end_time = override_end
    if override_lookback:
        try:
            lookback_hours = float(override_lookback)
        except ValueError as exc:
            raise ValueError(
                f"LOOKBACK_HOURS must be numeric, got {override_lookback!r}"
            ) from exc
    return start_time, end_time, lookback_hours


async def run_single_market_pmxt_backtest(
    *,
    name: str,
    market_slug: str,
    token_index: int = 0,
    lookback_hours: float | None = None,
    strategy_factory: StrategyFactory,
    probability_window: int,
    min_quotes: int = 0,
    min_price_range: float = 0.0,
    initial_cash: float = 100.0,
    chart_resample_rule: str | None = None,
    emit_summary: bool = True,
    emit_html: bool = True,
    return_chart_layout: bool = False,
    return_summary_series: bool = False,
    start_time: pd.Timestamp | datetime | str | None = None,
    end_time: pd.Timestamp | datetime | str | None = None,
) -> dict[str, Any] | None:
    start_time, end_time, lookback_hours = _apply_window_env_overrides(
        start_time=start_time,
        end_time=end_time,
        lookback_hours=lookback_hours,
    )
    try:
        end = pd.Timestamp(end_time if end_time is not None else datetime.now(UTC))
        if end.tzinfo is None:
            end = end.tz_localize(UTC)
        else:
            end = end.tz_convert(UTC)

        if start_time is not None:
            start = pd.Timestamp(start_time)
            if start.tzinfo is None:
                start = start.tz_localize(UTC)
            else:
                start = start.tz_convert(UTC)
        elif lookback_hours is not None:
            start = end - pd.Timedelta(hours=lookback_hours)
        else:
            raise ValueError("set START_TIME/END_TIME or LOOKBACK_HOURS")

        if start >= end:
            raise ValueError(
                f"START_TIME {start.isoformat()} must be earlier than "
                f"END_TIME {end.isoformat()}"
            )
    except Exception as exc:
        print(f"Unable to resolve PMXT backtest window for {market_slug}: {exc}")
        return

    window_hours = (end - start) / pd.Timedelta(hours=1)

    print(
        f"Loading PMXT Polymarket market {market_slug} "
        f"(token_index={token_index}, window_start={start.isoformat()}, "
        f"window_end={end.isoformat()}, window_hours={window_hours:.1f})..."
    )

    try:
        with configured_pmxt_data_source() as data_source:
            print(data_source.summary)
            loader = await PolymarketPMXTDataLoader.from_market_slug(
                market_slug,
                token_index=token_index,
            )
            data = loader.load_order_book_and_quotes(start, end)
    except Exception as exc:
        print(f"Unable to load PMXT Polymarket market {market_slug}: {exc}")
        return

    quote_count = 0
    min_mid_price: float | None = None
    max_mid_price: float | None = None
    for record in data:
        if not isinstance(record, QuoteTick):
            continue
        quote_count += 1
        mid_price = (float(record.bid_price) + float(record.ask_price)) / 2.0
        min_mid_price = (
            mid_price if min_mid_price is None else min(min_mid_price, mid_price)
        )
        max_mid_price = (
            mid_price if max_mid_price is None else max(max_mid_price, mid_price)
        )

    if quote_count < min_quotes:
        print(f"Skip {market_slug}: {quote_count} quotes < {min_quotes} required")
        return

    if min_mid_price is not None and max_mid_price is not None:
        price_range = max_mid_price - min_mid_price
        if price_range < min_price_range:
            print(
                f"Skip {market_slug}: price range {price_range:.3f} < {min_price_range:.3f}"
            )
            return

    if not data:
        print(f"No PMXT events returned for {market_slug}")
        return

    result = run_market_backtest(
        market_id=market_slug,
        instrument=loader.instrument,
        data=data,
        strategy=strategy_factory(loader.instrument.id),
        strategy_name=f"{name}:{market_slug}",
        output_prefix=name,
        platform="polymarket",
        venue=POLYMARKET_VENUE,
        base_currency=USDC_POS,
        fee_model=PolymarketFeeModel(),
        fill_model=None,
        apply_default_fill_model=False,
        initial_cash=initial_cash,
        probability_window=probability_window,
        price_attr="mid_price",
        count_key="quotes",
        data_count=quote_count,
        chart_resample_rule=chart_resample_rule,
        market_key="slug",
        emit_html=emit_html,
        return_chart_layout=return_chart_layout,
        return_summary_series=return_summary_series,
        book_type=BookType.L2_MBP,
        liquidity_consumption=True,
    )

    if emit_summary:
        print_backtest_summary(
            results=[result],
            market_key="slug",
            count_key="quotes",
            count_label="Quotes",
            pnl_label="PnL (USDC)",
        )
        if emit_html and result.get("chart_path"):
            print(f"\nLegacy chart saved to {result['chart_path']}")

    result["token_index"] = token_index
    result["outcome"] = str(loader.instrument.outcome or "")
    result["realized_outcome"] = infer_realized_outcome(loader.instrument)
    return result
