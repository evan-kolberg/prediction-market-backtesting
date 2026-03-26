from __future__ import annotations

import sys
from types import SimpleNamespace

import main as main_module


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
        "backtests._timing_test",
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
        "backtests._timing_test",
        SimpleNamespace(install_timing=lambda: calls.__setitem__("timing", 1)),
    )

    main_module.main()

    assert calls == {"timing": 0, "run": 1}
