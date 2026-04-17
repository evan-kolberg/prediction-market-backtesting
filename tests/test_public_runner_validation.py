from __future__ import annotations

import json
import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from tests._public_runner_validation_cases import RESULT_MARKER

REPO_ROOT = Path(__file__).resolve().parents[1]


def _run_validation_case(case: str) -> dict[str, Any]:
    completed = subprocess.run(
        [sys.executable, "-m", "tests._public_runner_validation_cases", case],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    for line in reversed(completed.stdout.splitlines()):
        if line.startswith(RESULT_MARKER):
            return json.loads(line.removeprefix(RESULT_MARKER))
    raise AssertionError(
        f"validation case {case!r} did not emit {RESULT_MARKER!r}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def _ledger_realized_pnl(fill_events: Sequence[dict[str, Any]]) -> float:
    """Independent test oracle: do not call production PnL/report helpers here."""
    cash = 0.0
    commissions = 0.0
    position = 0.0
    for event in fill_events:
        action = str(event["action"]).lower()
        price = float(event["price"])
        quantity = float(event["quantity"])
        commission = float(event.get("commission", 0.0))
        commissions += commission
        if action == "buy":
            cash -= price * quantity
            position += quantity
        elif action == "sell":
            cash += price * quantity
            position -= quantity
        else:  # pragma: no cover - defensive guard for future fill schema drift.
            raise AssertionError(f"unexpected fill action {action!r}")

    assert abs(position) < 1e-9
    return cash - commissions


def test_public_kalshi_runner_reconciles_fills_fees_and_taker_slippage() -> None:
    result = _run_validation_case("kalshi-baseline")

    fill_events = result["fill_events"]
    assert result["fills"] == len(fill_events) == 4
    assert result["terminated_early"] is False
    assert result["realized_outcome"] == 1.0
    assert result["pnl"] == pytest.approx(_ledger_realized_pnl(fill_events))

    # Kalshi trade ticks expose 4-decimal prices, but taker execution must be
    # modeled with the real one-cent order tick.
    assert [event["price"] for event in fill_events] == [0.51, 0.54, 0.60, 0.59]
    assert [event["commission"] for event in fill_events] == [0.02, 0.02, 0.02, 0.02]


def test_public_kalshi_runner_first_decision_is_invariant_to_future_ticks() -> None:
    baseline = _run_validation_case("kalshi-prefix-normal")
    stressed_future = _run_validation_case("kalshi-prefix-stressed")

    assert baseline["fill_events"][0] == stressed_future["fill_events"][0]


def test_public_polymarket_trade_runner_reconciles_size_clipping_and_slippage() -> None:
    result = _run_validation_case("polymarket-trade")

    fill_events = result["fill_events"]
    assert result["fills"] == len(fill_events) == 4
    assert result["terminated_early"] is False
    assert result["pnl"] == pytest.approx(_ledger_realized_pnl(fill_events))
    assert [event["price"] for event in fill_events] == [0.51, 0.49, 0.52, 0.49]
    assert fill_events[0]["quantity"] == 97.0
    assert fill_events[2]["quantity"] == pytest.approx(95.1182)
