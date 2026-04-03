from __future__ import annotations

import re
import sys
from pathlib import Path
from types import SimpleNamespace

import main as main_module


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_main_installs_timing_patch_by_default(monkeypatch):
    calls = {"timing": 0, "run": 0}

    async def _run() -> None:
        calls["run"] += 1

    monkeypatch.setattr(
        main_module,
        "discover",
        lambda: [{"name": "demo", "description": "", "run": _run}],
    )
    monkeypatch.setattr(main_module, "show_menu", lambda _backtests: 0)
    monkeypatch.delenv(main_module.ENABLE_TIMING_ENV, raising=False)
    monkeypatch.setitem(
        sys.modules,
        "backtests._shared._timing_test",
        SimpleNamespace(
            install_timing=lambda: calls.__setitem__("timing", calls["timing"] + 1),
        ),
    )

    main_module.main()

    assert calls == {"timing": 1, "run": 1}


def test_main_skips_timing_patch_when_disabled(monkeypatch):
    calls = {"timing": 0, "run": 0}

    async def _run() -> None:
        calls["run"] += 1

    monkeypatch.setattr(
        main_module,
        "discover",
        lambda: [{"name": "demo", "description": "", "run": _run}],
    )
    monkeypatch.setattr(main_module, "show_menu", lambda _backtests: 0)
    monkeypatch.setenv(main_module.ENABLE_TIMING_ENV, "0")
    monkeypatch.setitem(
        sys.modules,
        "backtests._shared._timing_test",
        SimpleNamespace(install_timing=lambda: calls.__setitem__("timing", 1)),
    )

    main_module.main()

    assert calls == {"timing": 0, "run": 1}


def test_show_menu_renders_folder_tree(capsys, monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _prompt: "2")

    choice = main_module.show_menu(
        [
            {
                "name": "kalshi_trade_tick_breakout",
                "description": "Kalshi breakout",
                "relative_parts": ("kalshi_trade_tick_breakout.py",),
                "run": object(),
            },
            {
                "name": "kalshi_trade_tick_ema_crossover",
                "description": "Kalshi EMA",
                "relative_parts": ("kalshi_trade_tick_ema_crossover.py",),
                "run": object(),
            },
            {
                "name": "polymarket_quote_tick_pmxt_breakout",
                "description": "PMXT breakout",
                "relative_parts": ("polymarket_quote_tick_pmxt_breakout.py",),
                "run": object(),
            },
        ],
    )

    rendered = _strip_ansi(capsys.readouterr().out)

    assert choice == 1
    assert "backtests/" in rendered
    assert "├── 1. kalshi_trade_tick_breakout.py — Kalshi breakout" in rendered
    assert "├── 2. kalshi_trade_tick_ema_crossover.py — Kalshi EMA" in rendered
    assert "└── 3. polymarket_quote_tick_pmxt_breakout.py — PMXT breakout" in rendered


def test_discoverable_backtest_paths_stay_flat(tmp_path: Path) -> None:
    backtests_root = tmp_path / "backtests"
    (backtests_root / "_shared").mkdir(parents=True)
    (backtests_root / "private").mkdir()
    (backtests_root / "nested").mkdir()

    (backtests_root / "__init__.py").write_text("")
    (backtests_root / "_script_helpers.py").write_text("")
    (backtests_root / "kalshi_trade_tick_breakout.py").write_text("")
    (backtests_root / "private" / "local_runner.py").write_text("")
    (backtests_root / "private" / "_helper.py").write_text("")
    (backtests_root / "nested" / "should_not_show.py").write_text("")
    (backtests_root / "_shared" / "_trade_tick_ui.py").write_text("")

    discovered = [
        path.relative_to(backtests_root)
        for path in main_module._discoverable_backtest_paths(backtests_root)
    ]

    assert discovered == [
        Path("kalshi_trade_tick_breakout.py"),
        Path("private/local_runner.py"),
    ]
