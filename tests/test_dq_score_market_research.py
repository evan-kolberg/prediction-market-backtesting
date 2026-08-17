from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pandas as pd


RESEARCH_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "dq_score_market_research"
    / "research_dq_score_market.py"
)


def _load_research_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("dq_score_market_test", RESEARCH_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _settlement_frame(entry_us: float, game_end_seconds: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "home_entry_us": [entry_us],
            "away_entry_us": [entry_us],
            "home_entry_ask": [0.50],
            "away_entry_ask": [0.50],
            "home_entry_ask_size": [100.0],
            "away_entry_ask_size": [100.0],
            "home_entry_spread": [0.02],
            "away_entry_spread": [0.02],
            "home_entry_fair": [0.50],
            "away_entry_fair": [0.50],
            "entry_target_us": [entry_us - 1_000_000],
            "actual_end_ts_utc": [game_end_seconds],
            "home_settled": [1.0],
            "away_settled": [0.0],
        }
    )


def test_settlement_trade_rejects_entry_at_or_after_game_end() -> None:
    research = _load_research_module()
    at_end = _settlement_frame(entry_us=100_000_000, game_end_seconds=100.0)
    after_end = _settlement_frame(entry_us=101_000_000, game_end_seconds=100.0)

    at_end_result = research._trade_arrays(at_end, "settle", pd.Series([True]), friction_bps=25.0)
    after_end_result = research._trade_arrays(
        after_end, "settle", pd.Series([True]), friction_bps=25.0
    )

    assert not bool(at_end_result.loc[0, "valid_quote"])
    assert not bool(after_end_result.loc[0, "valid_quote"])


def test_settlement_trade_accepts_entry_before_game_end() -> None:
    research = _load_research_module()
    frame = _settlement_frame(entry_us=99_000_000, game_end_seconds=100.0)

    result = research._trade_arrays(frame, "settle", pd.Series([True]), friction_bps=25.0)

    assert bool(result.loc[0, "valid_quote"])
    assert result.loc[0, "entry_us"] < result.loc[0, "exit_us"]


def test_max_drawdown_uses_running_equity_peak() -> None:
    research = _load_research_module()

    result = research.max_drawdown(pd.Series([5.0, -3.0, -4.0, 6.0]))

    assert result == 7.0
