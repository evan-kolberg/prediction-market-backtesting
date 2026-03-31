# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-29 and 2026-03-31.
# See the repository NOTICE file for provenance and licensing scope.

"""
Public multi-market example: run a quote-tick VWAP-reversion strategy across a
small fixed set of Polymarket sports markets using PMXT historical L2 data.

This runner is intentionally pinned to explicit historical windows so it does
not depend on rolling upstream PMXT availability.
"""

# ruff: noqa: E402

from __future__ import annotations

import asyncio
from decimal import Decimal

try:
    from ._script_helpers import ensure_repo_root
except ImportError:
    from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from nautilus_trader.adapters.prediction_market.research import print_backtest_summary
from nautilus_trader.adapters.prediction_market.research import (
    save_aggregate_backtest_report,
)
from nautilus_trader.adapters.prediction_market.research import (
    save_combined_backtest_report,
)
from strategies import QuoteTickVWAPReversionConfig
from strategies import QuoteTickVWAPReversionStrategy


try:
    from ._defaults import DEFAULT_INITIAL_CASH
    from ._polymarket_single_market_pmxt_runner import run_single_market_pmxt_backtest
except ImportError:
    from backtests.polymarket_quote_tick._defaults import DEFAULT_INITIAL_CASH
    from backtests.polymarket_quote_tick._polymarket_single_market_pmxt_runner import (
        run_single_market_pmxt_backtest,
    )


NAME = "polymarket_pmxt_relay_sports_vwap_reversion"
DESCRIPTION = (
    "VWAP reversion on a fixed set of Polymarket sports markets using PMXT L2 data"
)

SPORT_MARKET_SAMPLES = (
    {
        "market_slug": "nfl-kc-lac-2025-09-05",
        "token_index": 0,
        "outcome": "Chiefs",
        "start_time": "2025-09-01T05:22:48Z",
        "end_time": "2025-09-06T05:22:48Z",
    },
    {
        "market_slug": "nhl-buf-pit-2025-10-03",
        "token_index": 0,
        "outcome": "Sabres",
        "start_time": "2025-09-29T03:52:57Z",
        "end_time": "2025-10-04T03:52:57Z",
    },
    {
        "market_slug": "nba-lal-gsw-2025-10-05",
        "token_index": 0,
        "outcome": "Lakers",
        "start_time": "2025-10-01T05:06:27Z",
        "end_time": "2025-10-06T05:06:27Z",
    },
)

MIN_QUOTES = 500
MIN_PRICE_RANGE = 0.005
VWAP_WINDOW = 30
ENTRY_THRESHOLD = 0.0015
EXIT_THRESHOLD = 0.0003
MIN_TICK_SIZE = 0.0
TAKE_PROFIT = 0.004
STOP_LOSS = 0.004

TRADE_SIZE = Decimal("100")
INITIAL_CASH = float(DEFAULT_INITIAL_CASH)
CHART_RESAMPLE_RULE = None
EMIT_HTML = True
COMBINED_REPORT = False
COMBINED_REPORT_PATH = f"output/{NAME}_combined_legacy.html"
SUMMARY_REPORT = True
SUMMARY_REPORT_PATH = f"output/{NAME}_multi_market.html"


async def run() -> None:
    results: list[dict] = []
    target_results = len(SPORT_MARKET_SAMPLES)

    for sample in SPORT_MARKET_SAMPLES:
        slug = sample["market_slug"]
        token_index = int(sample["token_index"])
        outcome = str(sample["outcome"])
        print(
            f"Running fixed PMXT sports sample {slug} "
            f"on token_index={token_index} outcome={outcome}"
        )

        result = await run_single_market_pmxt_backtest(
            name=NAME,
            market_slug=slug,
            token_index=token_index,
            start_time=sample["start_time"],
            end_time=sample["end_time"],
            min_quotes=MIN_QUOTES,
            min_price_range=MIN_PRICE_RANGE,
            initial_cash=INITIAL_CASH,
            probability_window=VWAP_WINDOW,
            chart_resample_rule=CHART_RESAMPLE_RULE,
            emit_summary=False,
            emit_html=EMIT_HTML,
            return_chart_layout=False,
            return_summary_series=SUMMARY_REPORT,
            strategy_factory=lambda instrument_id: QuoteTickVWAPReversionStrategy(
                config=QuoteTickVWAPReversionConfig(
                    instrument_id=instrument_id,
                    trade_size=TRADE_SIZE,
                    vwap_window=VWAP_WINDOW,
                    entry_threshold=ENTRY_THRESHOLD,
                    exit_threshold=EXIT_THRESHOLD,
                    min_tick_size=MIN_TICK_SIZE,
                    take_profit=TAKE_PROFIT,
                    stop_loss=STOP_LOSS,
                ),
            ),
        )
        if result is None:
            continue

        result["outcome"] = outcome
        results.append(result)
        print(
            f"Completed {len(results)}/{target_results}: {slug} "
            f"pnl={float(result['pnl']):+.4f} "
            f"fills={int(result['fills'])} "
            f"quotes={int(result['quotes'])}"
        )

    if not results:
        print(
            "No Polymarket sports markets had sufficient PMXT quote data for the "
            "current fixed sample set."
        )
        return

    if len(results) < target_results:
        print(
            f"Completed {len(results)} Polymarket quote-tick backtests from "
            f"{target_results} fixed sports samples."
        )

    print_backtest_summary(
        results=results,
        market_key="slug",
        count_key="quotes",
        count_label="Quotes",
        pnl_label="PnL (USDC)",
    )

    if COMBINED_REPORT:
        combined_path = save_combined_backtest_report(
            results=results,
            output_path=COMBINED_REPORT_PATH,
            title=f"{NAME} combined legacy chart",
            market_key="slug",
            pnl_label="PnL (USDC)",
        )
        if combined_path is not None:
            print(f"\nCombined legacy chart saved to {combined_path}")

    if SUMMARY_REPORT:
        summary_path = save_aggregate_backtest_report(
            results=results,
            output_path=SUMMARY_REPORT_PATH,
            title=f"{NAME} legacy multi-market chart",
            market_key="slug",
            pnl_label="PnL (USDC)",
        )
        if summary_path is not None:
            print(f"\nLegacy multi-market chart saved to {summary_path}")


if __name__ == "__main__":
    asyncio.run(run())
