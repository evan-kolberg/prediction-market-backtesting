from __future__ import annotations

import argparse
import asyncio
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv

if __package__ in {None, ""}:
    import importlib.util

    helper_path = Path(__file__).resolve().parents[1] / "backtests" / "_script_helpers.py"
    spec = importlib.util.spec_from_file_location("_script_helpers", helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load script helper from {helper_path}")
    helpers = importlib.util.module_from_spec(spec)
    sys.modules["_script_helpers"] = helpers
    spec.loader.exec_module(helpers)
    helpers.ensure_repo_root(__file__)

from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.config import ImportableStrategyConfig

from prediction_market_extensions.live.btc_5m import (
    DEFAULT_MARKET_COUNT,
    LIVE_BTC_5M_EVENT_SLUGS_ENV,
    LIVE_BTC_5M_INCLUDE_CURRENT_ENV,
    LIVE_BTC_5M_MARKET_COUNT_ENV,
    load_btc_5m_instrument_ids,
    upcoming_btc_5m_event_slugs,
    upcoming_btc_5m_window_label,
)
from prediction_market_extensions.live.sandbox import (
    DEFAULT_BTC_INSTRUMENT_ID,
    build_polymarket_binance_sandbox_config,
    build_polymarket_binance_sandbox_node,
)

EVENT_SLUG_BUILDER = "prediction_market_extensions.live.btc_5m:configured_btc_5m_event_slugs"
DEFAULT_MODEL_PATH = "live/models/btc_snapshot_model_s199_cost101_daily_stop120_profile.json"
STRATEGY_PATH = "strategies.private.btc_snapshot_model:BookBtcSnapshotModelStrategy"
CONFIG_PATH = "strategies.private.btc_snapshot_model:BookBtcSnapshotModelConfig"


def _model_path() -> str:
    return os.getenv("LIVE_BTC_SNAPSHOT_MODEL_PATH", DEFAULT_MODEL_PATH)


def _trade_size() -> Decimal:
    return Decimal(os.getenv("LIVE_BTC_SNAPSHOT_TRADE_SIZE", "2"))


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _diagnostics_path() -> str | None:
    raw = os.getenv("LIVE_BTC_SNAPSHOT_DIAGNOSTICS_PATH")
    if raw is None:
        return None
    path = raw.strip()
    return path or None


def _settlement_path() -> str | None:
    raw = os.getenv(
        "LIVE_BTC_SNAPSHOT_SETTLEMENT_PATH",
        "live/btc_snapshot_model_sandbox_settlements.json",
    )
    path = raw.strip()
    return path or None


def _strategy_parameters() -> dict[str, object]:
    return {
        "model_path": _model_path(),
        "trade_size": _trade_size(),
        "edge": 0.06,
        "snapshot_seconds": (60,),
        "min_ask_price": 0.10,
        "max_ask_price": 0.75,
        "max_spread": 0.20,
        "max_book_age_seconds": 8.0,
        "depth_levels": 5,
        "max_expected_slippage": 0.02,
        "min_visible_size": 1.0,
        "min_selected_probability": 0.65,
        "expensive_ask_floor": 0.70,
        "expensive_min_selected_probability": 0.80,
        "expensive_min_signed_momentum_30s": _env_float(
            "LIVE_BTC_EXPENSIVE_MIN_SIGNED_MOMENTUM_30S",
            2.0,
        ),
        "adverse_price_diff_floor": 5.0,
        "adverse_min_signed_momentum_30s": 2.0,
        "exhausted_price_diff_floor": 30.0,
        "exhausted_min_selected_probability": 0.80,
        "volatile_price_diff_floor": 2.5,
        "volatile_min_selected_probability": 0.72,
        "max_yes_no_ask_cost": 1.01,
        "diagnostics_path": _diagnostics_path(),
        "momentum_alignment": os.getenv(
            "LIVE_BTC_SNAPSHOT_MOMENTUM_ALIGNMENT",
            "pdiff_m15_m30",
        ),
        "live_btc_buffer_seconds": 900,
        "max_btc_feature_age_seconds": _env_float("LIVE_BTC_MAX_FEATURE_AGE_SECONDS", 8.0),
        "market_buy_quote_quantity": True,
        "min_market_buy_quote_amount": Decimal("1"),
        "daily_stop_loss": _env_float("LIVE_BTC_DAILY_STOP_LOSS", 1.2),
        "settlement_path": _settlement_path(),
        "settlement_poll_seconds": float(os.getenv("LIVE_BTC_SETTLEMENT_POLL_SECONDS", "10")),
        "settlement_grace_seconds": float(os.getenv("LIVE_BTC_SETTLEMENT_GRACE_SECONDS", "5")),
        "dynamic_instrument_scan_seconds": float(
            os.getenv("LIVE_BTC_DYNAMIC_INSTRUMENT_SCAN_SECONDS", "30")
        ),
        "market_retention_seconds": float(os.getenv("LIVE_BTC_MARKET_RETENTION_SECONDS", "600")),
        "heartbeat_log_seconds": float(os.getenv("LIVE_BTC_HEARTBEAT_LOG_SECONDS", "300")),
    }


def _build_strategy_config(
    *,
    instrument_ids: tuple[InstrumentId, ...],
    btc_instrument_id: InstrumentId,
) -> ImportableStrategyConfig:
    config = _strategy_parameters()
    config["instrument_ids"] = [str(instrument_id) for instrument_id in instrument_ids]
    config["btc_instrument_id"] = str(btc_instrument_id)
    return ImportableStrategyConfig(
        strategy_path=STRATEGY_PATH,
        config_path=CONFIG_PATH,
        config=config,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the BTC snapshot model in Nautilus sandbox.")
    parser.add_argument(
        "--run",
        action="store_true",
        help="Start the Nautilus sandbox node. Without this flag, only validates wiring.",
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="Build the Nautilus sandbox node, then exit before running it.",
    )
    parser.add_argument(
        "--markets",
        type=int,
        default=int(os.getenv("LIVE_BTC_5M_MARKET_COUNT", str(DEFAULT_MARKET_COUNT))),
        help="Number of upcoming BTC 5m markets to load.",
    )
    parser.add_argument(
        "--include-current",
        action="store_true",
        default=os.getenv("LIVE_INCLUDE_CURRENT_MARKET", "0").lower() in {"1", "true", "yes"},
        help="Include the currently running 5m market.",
    )
    parser.add_argument(
        "--starting-balance",
        default=os.getenv("LIVE_SANDBOX_STARTING_BALANCE", "20"),
        help="Sandbox pUSD starting balance.",
    )
    parser.add_argument(
        "--btc-instrument-id",
        default=os.getenv("LIVE_BTC_INSTRUMENT_ID", str(DEFAULT_BTC_INSTRUMENT_ID)),
        help="External BTC trade instrument ID.",
    )
    parser.add_argument(
        "--trader-id",
        default=os.getenv("LIVE_TRADER_ID", "BTC-SANDBOX-001"),
        help="Nautilus trader ID.",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("LIVE_LOG_LEVEL", "INFO"),
        help="Nautilus log level.",
    )
    parser.add_argument(
        "--polymarket-refresh-mins",
        type=int,
        default=int(os.getenv("LIVE_POLYMARKET_REFRESH_MINS", "5")),
        help="Minutes between Polymarket instrument refreshes; <=0 disables refresh.",
    )
    parser.add_argument(
        "--binance-global",
        action="store_true",
        default=_env_bool("LIVE_BTC_BINANCE_GLOBAL", False),
        help="Use Binance global BTC trades instead of Binance US.",
    )
    return parser.parse_args(argv)


async def _main(argv: Sequence[str] | None = None, *, force_run: bool = False) -> None:
    load_dotenv()
    args = _parse_args(argv)
    if force_run:
        args.run = True
    model_path = _model_path()
    model_exists = Path(model_path).exists()
    if (args.run or args.build_only) and not model_exists:
        raise FileNotFoundError(model_path)
    btc_instrument_id = InstrumentId.from_str(args.btc_instrument_id)
    event_slugs = upcoming_btc_5m_event_slugs(
        market_count=args.markets,
        include_current=args.include_current,
    )
    if not os.getenv(LIVE_BTC_5M_EVENT_SLUGS_ENV, "").strip():
        os.environ[LIVE_BTC_5M_MARKET_COUNT_ENV] = str(args.markets)
        os.environ[LIVE_BTC_5M_INCLUDE_CURRENT_ENV] = "1" if args.include_current else "0"
    instrument_ids = await load_btc_5m_instrument_ids(
        market_count=args.markets,
        include_current=args.include_current,
        event_slugs=event_slugs,
    )
    strategy_config = _build_strategy_config(
        instrument_ids=instrument_ids,
        btc_instrument_id=btc_instrument_id,
    )
    node_config = build_polymarket_binance_sandbox_config(
        strategies=[strategy_config],
        event_slug_builder=EVENT_SLUG_BUILDER,
        binance_instrument_ids=frozenset({btc_instrument_id}),
        starting_balance=Decimal(str(args.starting_balance)),
        trader_id=args.trader_id,
        log_level=args.log_level,
        polymarket_update_interval_mins=(
            args.polymarket_refresh_mins if args.polymarket_refresh_mins > 0 else None
        ),
        binance_us=not args.binance_global,
    )

    coverage_minutes = len(event_slugs) * 5
    print(
        f"Loaded {len(instrument_ids)} Polymarket instruments across "
        f"{len(event_slugs)} BTC 5m markets (~{coverage_minutes} minutes)."
    )
    print(f"Current BTC 5m window: {upcoming_btc_5m_window_label()}")
    print(f"Event slug range: {event_slugs[0]} -> {event_slugs[-1]}")
    print(f"Next event slugs: {', '.join(event_slugs[:3])}")
    model_suffix = "" if model_exists else " (missing; dry-run only)"
    print(f"Model profile: {model_path}{model_suffix}")
    print("Policy: S199 profile, 60s snapshot, edge>=0.06, ask-cost<=1.01, pdiff/momentum aligned")
    print(f"Trade size: target {_trade_size()} contracts; market buys sent as quote quantity")
    print(f"BTC trade source: {'Binance global' if args.binance_global else 'Binance US'}")
    print(f"Diagnostics: {_diagnostics_path() or 'disabled'}")
    print(f"Settlement ledger: {_settlement_path() or 'disabled'}")
    print(
        f"Polymarket refresh: {args.polymarket_refresh_mins} min"
        if args.polymarket_refresh_mins > 0
        else "Polymarket refresh: disabled"
    )
    print(f"Strategy: {STRATEGY_PATH}")
    if not args.run and not args.build_only:
        print("Dry run only. Pass --run to start the Nautilus sandbox node.")
        return

    node = build_polymarket_binance_sandbox_node(config=node_config)
    node.build()
    if args.build_only:
        print("Built Nautilus sandbox node. Exiting before run().")
        return
    await node.run_async()


def run() -> None:
    asyncio.run(_main((), force_run=True))


if __name__ == "__main__":
    asyncio.run(_main(sys.argv[1:]))
