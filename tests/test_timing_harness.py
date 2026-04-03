from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace

from backtests._shared._timing_harness import timing_harness


def test_timing_harness_installs_pmxt_timing_by_default(monkeypatch) -> None:
    calls = {"timing": 0, "run": 0}

    async def _run() -> str:
        calls["run"] += 1
        return "ok"

    monkeypatch.delenv("BACKTEST_ENABLE_TIMING", raising=False)
    monkeypatch.setitem(
        sys.modules,
        "backtests._shared._timing_test",
        SimpleNamespace(install_timing=lambda: calls.__setitem__("timing", 1)),
    )

    wrapped = timing_harness(_run)
    result = asyncio.run(wrapped())

    assert result == "ok"
    assert calls == {"timing": 1, "run": 1}


def test_timing_harness_skips_install_when_disabled(monkeypatch) -> None:
    calls = {"timing": 0, "run": 0}

    async def _run() -> None:
        calls["run"] += 1

    monkeypatch.setenv("BACKTEST_ENABLE_TIMING", "0")
    monkeypatch.setitem(
        sys.modules,
        "backtests._shared._timing_test",
        SimpleNamespace(install_timing=lambda: calls.__setitem__("timing", 1)),
    )

    wrapped = timing_harness(_run)
    asyncio.run(wrapped())

    assert calls == {"timing": 0, "run": 1}
