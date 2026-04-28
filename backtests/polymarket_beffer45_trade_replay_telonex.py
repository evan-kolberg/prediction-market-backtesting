# Hard-coded account-ledger replay for external backtest verification.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Added in this repository on 2026-04-28.

from __future__ import annotations

import csv
import re
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    from _beffer45_trade_data import (  # type: ignore[import-not-found]
        BEFFER45_LEDGER_SUMMARY,
        BEFFER45_LEDGER_TRADES,
        BEFFER45_POSITIONS_API_URL,
        BEFFER45_TRADES_API_URL,
        BEFFER45_WALLET,
    )
    from _script_helpers import ensure_repo_root
else:
    from ._beffer45_trade_data import (
        BEFFER45_LEDGER_SUMMARY,
        BEFFER45_LEDGER_TRADES,
        BEFFER45_POSITIONS_API_URL,
        BEFFER45_TRADES_API_URL,
        BEFFER45_WALLET,
    )
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)


_UPDOWN_SLUG_RE = re.compile(r"-(?P<minutes>5m|15m)-(?P<start_ts>\d+)$")
_REPORT_PATH = Path("output/polymarket_beffer45_trade_replay_telonex_summary.html")
_COMPARISON_CSV_PATH = Path("output/polymarket_beffer45_trade_replay_telonex_comparison.csv")
_INITIAL_CASH = 1_000.0


def _decimal(value: object) -> Decimal:
    return Decimal(str(value))


def _trade_notional(trade: Mapping[str, object]) -> Decimal:
    return _decimal(trade["size"]) * _decimal(trade["price"])


def _trade_timestamp(trade: Mapping[str, object]) -> datetime:
    return datetime.fromtimestamp(int(trade["ts"]), tz=UTC)


def _iso(timestamp: datetime) -> str:
    return timestamp.isoformat().replace("+00:00", "Z")


def _group_trades_by_instrument(
    trades: Iterable[Mapping[str, object]],
) -> dict[tuple[str, int], tuple[dict[str, object], ...]]:
    grouped: defaultdict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
    for trade in trades:
        grouped[(str(trade["slug"]), int(trade["outcome_index"]))].append(dict(trade))
    return {
        key: tuple(sorted(value, key=lambda item: (int(item["ts"]), str(item["tx"]))))
        for key, value in grouped.items()
    }


def _replay_window(*, slug: str, trades: Sequence[Mapping[str, object]]) -> tuple[str, str]:
    first_trade = min(_trade_timestamp(trade) for trade in trades)
    last_trade = max(_trade_timestamp(trade) for trade in trades)
    start = first_trade - timedelta(minutes=10)
    end = last_trade + timedelta(minutes=30)

    match = _UPDOWN_SLUG_RE.search(slug)
    if match is not None:
        market_start = datetime.fromtimestamp(int(match.group("start_ts")), tz=UTC)
        duration = timedelta(minutes=15 if match.group("minutes") == "15m" else 5)
        end = max(end, market_start + duration + timedelta(minutes=15))
    elif slug.startswith("cricipl-"):
        end = max(end, last_trade + timedelta(hours=12))
    else:
        end = max(end, last_trade + timedelta(hours=2))

    return _iso(start), _iso(end)


def _ledger_cash_pnl(trades: Sequence[Mapping[str, object]]) -> Decimal:
    cash = Decimal("0")
    for trade in trades:
        notional = _trade_notional(trade)
        if str(trade["side"]).upper() == "BUY":
            cash -= notional
        else:
            cash += notional
    return cash


def _ledger_open_quantity(trades: Sequence[Mapping[str, object]]) -> Decimal:
    quantity = Decimal("0")
    for trade in trades:
        size = _decimal(trade["size"])
        if str(trade["side"]).upper() == "BUY":
            quantity += size
        else:
            quantity -= size
    return quantity


def _ledger_settlement_pnl(
    trades: Sequence[Mapping[str, object]], realized_outcome: object
) -> Decimal | None:
    if realized_outcome is None:
        return None
    return _ledger_cash_pnl(trades) + (_decimal(realized_outcome) * _ledger_open_quantity(trades))


def _sum_notional(trades: Iterable[Mapping[str, object]], *, side: str) -> Decimal:
    return sum(
        (_trade_notional(trade) for trade in trades if str(trade["side"]).upper() == side),
        Decimal("0"),
    )


def _build_replays() -> tuple[Any, ...]:
    from prediction_market_extensions.backtesting._replay_specs import BookReplay

    replays = []
    for (slug, outcome_index), trades in sorted(
        _group_trades_by_instrument(BEFFER45_LEDGER_TRADES).items()
    ):
        start_time, end_time = _replay_window(slug=slug, trades=trades)
        outcome = str(trades[0]["outcome"])
        buy_notional = _sum_notional(trades, side="BUY")
        sell_notional = _sum_notional(trades, side="SELL")
        replays.append(
            BookReplay(
                market_slug=slug,
                token_index=outcome_index,
                start_time=start_time,
                end_time=end_time,
                outcome=outcome,
                metadata={
                    "sim_label": f"{slug}:{outcome}",
                    "ledger_trades": trades,
                    "ledger_trade_count": len(trades),
                    "ledger_buy_notional": str(buy_notional),
                    "ledger_sell_notional": str(sell_notional),
                    "ledger_cash_pnl": str(_ledger_cash_pnl(trades)),
                    "ledger_open_quantity": str(_ledger_open_quantity(trades)),
                },
            )
        )
    return tuple(replays)


def _print_ledger_header() -> None:
    print("Hard-coded @beffer45 Polymarket ledger snapshot")
    print(f"  wallet: {BEFFER45_WALLET}")
    print(f"  trades API: {BEFFER45_TRADES_API_URL}")
    print(f"  positions API: {BEFFER45_POSITIONS_API_URL}")
    print(f"  fetched_at: {BEFFER45_LEDGER_SUMMARY['fetched_at']}")
    print(
        "  trades: "
        f"{BEFFER45_LEDGER_SUMMARY['trade_count']} total, "
        f"{BEFFER45_LEDGER_SUMMARY['taker_trade_count']} taker-only API matches"
    )
    print(
        "  trade cashflow: "
        f"buys={BEFFER45_LEDGER_SUMMARY['buy_notional']} USDC, "
        f"sells={BEFFER45_LEDGER_SUMMARY['sell_notional']} USDC, "
        f"net={_ledger_cash_pnl(BEFFER45_LEDGER_TRADES)} USDC"
    )
    print(
        "  current positions snapshot: "
        f"positions={BEFFER45_LEDGER_SUMMARY['position_count']}, "
        f"initial_value={BEFFER45_LEDGER_SUMMARY['position_initial_value']} USDC, "
        f"current_value={BEFFER45_LEDGER_SUMMARY['position_current_value']} USDC, "
        f"cash_pnl={BEFFER45_LEDGER_SUMMARY['position_cash_pnl']} USDC, "
        f"realized_pnl={BEFFER45_LEDGER_SUMMARY['position_realized_pnl']} USDC"
    )


def _write_comparison_csv(rows: Sequence[Mapping[str, object]]) -> None:
    _COMPARISON_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _COMPARISON_CSV_PATH.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "slug",
                "token_index",
                "outcome",
                "ledger_trades",
                "engine_fills",
                "ledger_cash_pnl",
                "ledger_settlement_pnl",
                "backtest_pnl",
                "delta_backtest_minus_ledger",
                "settlement_pnl_applied",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def _print_backtest_comparison(results: Sequence[Mapping[str, Any]]) -> None:
    grouped = _group_trades_by_instrument(BEFFER45_LEDGER_TRADES)
    loaded_keys: set[tuple[str, int]] = set()
    rows: list[dict[str, object]] = []
    backtest_pnl = Decimal("0")
    ledger_cash_pnl = Decimal("0")
    ledger_loaded_pnl = Decimal("0")
    resolved_metadata_markets = 0
    settlement_applied_markets = 0
    engine_fills = 0

    for result in results:
        slug = str(result.get("slug") or result.get("market") or "")
        token_index = int(result.get("token_index", 0))
        key = (slug, token_index)
        loaded_keys.add(key)
        trades = grouped.get(key, ())
        fills = int(result.get("fills") or 0)
        engine_fills += fills
        result_pnl = _decimal(result.get("pnl", 0))
        backtest_pnl += result_pnl
        ledger_cash_pnl += _ledger_cash_pnl(trades)
        ledger_pnl = _ledger_settlement_pnl(trades, result.get("realized_outcome"))
        if ledger_pnl is not None:
            ledger_loaded_pnl += ledger_pnl
            resolved_metadata_markets += 1
        if bool(result.get("settlement_pnl_applied")):
            settlement_applied_markets += 1
        rows.append(
            {
                "slug": slug,
                "token_index": token_index,
                "outcome": result.get("outcome"),
                "ledger_trades": len(trades),
                "engine_fills": fills,
                "ledger_cash_pnl": str(_ledger_cash_pnl(trades)),
                "ledger_settlement_pnl": "" if ledger_pnl is None else str(ledger_pnl),
                "backtest_pnl": str(result_pnl),
                "delta_backtest_minus_ledger": ""
                if ledger_pnl is None
                else str(result_pnl - ledger_pnl),
                "settlement_pnl_applied": bool(result.get("settlement_pnl_applied")),
            }
        )

    skipped = sorted(set(grouped) - loaded_keys)
    _write_comparison_csv(rows)
    print("Backtest vs hard-coded ledger comparison")
    print(f"  loaded instruments: {len(loaded_keys)} / {len(grouped)}")
    print(
        f"  engine fills: {engine_fills} / {BEFFER45_LEDGER_SUMMARY['trade_count']} ledger trades"
    )
    print(f"  ledger cash PnL on loaded instruments: {ledger_cash_pnl} USDC")
    print(f"  instruments with resolved-outcome metadata: {resolved_metadata_markets}")
    print(f"  instruments with settlement applied by report: {settlement_applied_markets}")
    print(f"  ledger settlement PnL using resolved metadata: {ledger_loaded_pnl} USDC")
    print(f"  backtest report PnL on loaded instruments: {backtest_pnl} USDC")
    print(f"  delta report - ledger settlement metadata: {backtest_pnl - ledger_loaded_pnl} USDC")
    print(f"  comparison CSV: {_COMPARISON_CSV_PATH}")
    print(f"  summary HTML: {_REPORT_PATH}")
    if skipped:
        print(f"  skipped instruments: {len(skipped)}")
        for slug, token_index in skipped[:10]:
            print(f"    - {slug}:{token_index}")
        if len(skipped) > 10:
            print(f"    ... {len(skipped) - 10} more")


def run() -> None:
    from dotenv import load_dotenv

    from prediction_market_extensions.backtesting._execution_config import (
        ExecutionModelConfig,
        StaticLatencyConfig,
    )
    from prediction_market_extensions.backtesting._experiments import (
        build_replay_experiment,
        run_experiment,
    )
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        MarketReportConfig,
    )
    from prediction_market_extensions.backtesting._prediction_market_runner import (
        MarketDataConfig,
    )
    from prediction_market_extensions.backtesting._timing_harness import timing_harness
    from prediction_market_extensions.backtesting.data_sources import Book, Polymarket, Telonex

    load_dotenv()

    @timing_harness
    def _run() -> None:
        _print_ledger_header()
        results = run_experiment(
            build_replay_experiment(
                name="polymarket_beffer45_trade_replay_telonex",
                description=("Hard-coded @beffer45 public trade ledger replay on Telonex L2 books"),
                data=MarketDataConfig(
                    platform=Polymarket,
                    data_type=Book,
                    vendor=Telonex,
                    sources=("api:${TELONEX_API_KEY}",),
                ),
                replays=_build_replays(),
                strategy_configs=[
                    {
                        "strategy_path": "strategies:BookAccountTradeReplayStrategy",
                        "config_path": "strategies:BookAccountTradeReplayConfig",
                        "config": {
                            "trades": "__SIM_METADATA__:ledger_trades",
                            "trigger_on_trade_ticks": True,
                            "reduce_only_sells": True,
                        },
                    }
                ],
                initial_cash=_INITIAL_CASH,
                probability_window=30,
                min_book_events=1,
                min_price_range=0.0,
                execution=ExecutionModelConfig(
                    queue_position=False,
                    latency_model=StaticLatencyConfig(),
                ),
                report=MarketReportConfig(
                    count_key="book_events",
                    count_label="Book Events",
                    pnl_label="PnL (USDC)",
                    market_key="sim_label",
                    summary_report=True,
                    summary_report_path=str(_REPORT_PATH),
                    summary_plot_panels=(
                        "total_equity",
                        "market_pnl",
                        "yes_price",
                        "allocation",
                        "total_cash_equity",
                    ),
                ),
                empty_message="No Telonex @beffer45 ledger replay windows loaded.",
                partial_message=(
                    "Completed {completed} of {total} @beffer45 ledger replay windows."
                ),
                return_summary_series=True,
            )
        )
        if results is not None:
            _print_backtest_comparison(results)

    _run()


if __name__ == "__main__":
    run()
