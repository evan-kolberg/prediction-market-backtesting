from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path
from types import SimpleNamespace

import main as main_module
import pytest


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_main_installs_timing_patch_by_default(monkeypatch):
    calls = {"timing": 0, "run": 0}

    async def _run() -> None:
        calls["run"] += 1

    monkeypatch.setattr(
        main_module,
        "discover",
        lambda: [
            {
                "name": "demo",
                "description": "",
                "module_name": "backtests.demo_runner",
                "relative_parts": ("demo_runner.py",),
            }
        ],
    )
    monkeypatch.setattr(main_module, "show_menu", lambda _backtests: 0)
    monkeypatch.setattr(main_module, "_load_runner", lambda _backtest: _run)
    monkeypatch.delenv(main_module.ENABLE_TIMING_ENV, raising=False)
    monkeypatch.setitem(
        sys.modules,
        "prediction_market_extensions.backtesting._timing_test",
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
        lambda: [
            {
                "name": "demo",
                "description": "",
                "module_name": "backtests.demo_runner",
                "relative_parts": ("demo_runner.py",),
            }
        ],
    )
    monkeypatch.setattr(main_module, "show_menu", lambda _backtests: 0)
    monkeypatch.setattr(main_module, "_load_runner", lambda _backtest: _run)
    monkeypatch.setenv(main_module.ENABLE_TIMING_ENV, "0")
    monkeypatch.setitem(
        sys.modules,
        "prediction_market_extensions.backtesting._timing_test",
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
                "name": "kalshi_trade_tick_multi_sim_runner",
                "description": "Kalshi basket",
                "relative_parts": ("kalshi_trade_tick_multi_sim_runner.py",),
                "run": object(),
            },
            {
                "name": "polymarket_quote_tick_pmxt_25_sims_runner",
                "description": "PMXT 25 sims",
                "relative_parts": ("polymarket_quote_tick_pmxt_25_sims_runner.py",),
                "run": object(),
            },
        ],
    )

    rendered = _strip_ansi(capsys.readouterr().out)

    assert choice == 1
    assert "backtests/" in rendered
    assert "├── 1. kalshi_trade_tick_breakout.py — Kalshi breakout" in rendered
    assert "├── 2. kalshi_trade_tick_multi_sim_runner.py — Kalshi basket" in rendered
    assert (
        "└── 3. polymarket_quote_tick_pmxt_25_sims_runner.py — PMXT 25 sims" in rendered
    )


def test_assign_shortcuts_prefers_unique_letters_and_avoids_quit_key():
    backtests = [
        {
            "name": "kalshi_trade_tick_breakout",
            "description": "Kalshi breakout",
            "relative_parts": ("kalshi_trade_tick_breakout.py",),
            "run": object(),
        },
        {
            "name": "polymarket_trade_tick_vwap_reversion",
            "description": "Polymarket VWAP",
            "relative_parts": ("polymarket_trade_tick_vwap_reversion.py",),
            "run": object(),
        },
        {
            "name": "polymarket_quote_tick_pmxt_ema_crossover",
            "description": "PMXT EMA",
            "relative_parts": ("polymarket_quote_tick_pmxt_ema_crossover.py",),
            "run": object(),
        },
    ]

    shortcuts = main_module._assign_shortcuts(backtests)

    assigned = [value for value in shortcuts.values() if value is not None]

    assert len(set(assigned)) == len(backtests)
    assert all(len(value) == 1 and value.isalpha() for value in assigned)
    assert "q" not in assigned
    assert "Q" not in assigned


def test_assign_shortcuts_leaves_overflow_entries_without_hotkeys():
    backtests = [
        {
            "name": f"demo_runner_{index}",
            "description": f"Demo {index}",
            "relative_parts": (f"demo_runner_{index}.py",),
            "run": object(),
        }
        for index in range(len(main_module.SHORTCUT_LETTERS) + 5)
    ]

    shortcuts = main_module._assign_shortcuts(backtests)
    assigned = [value for value in shortcuts.values() if value is not None]
    unassigned = [value for value in shortcuts.values() if value is None]

    assert len(assigned) == len(main_module.SHORTCUT_LETTERS)
    assert len(set(assigned)) == len(assigned)
    assert len(unassigned) == 5


def test_runner_preview_shows_full_file_contents(
    tmp_path: Path,
    monkeypatch,
):
    runner_path = tmp_path / "backtests" / "demo_runner.py"
    runner_path.parent.mkdir(parents=True)
    contents = (
        'NAME = "demo_runner"\n'
        'DESCRIPTION = "Demo runner"\n'
        "\n"
        "from pathlib import Path\n"
        "\n"
        "DATA = object()\n"
        "REPLAYS = ()\n",
    )
    contents = "".join(contents)
    runner_path.write_text(contents, encoding="utf-8")

    backtest = {
        "name": "demo_runner",
        "description": "Demo runner",
        "relative_parts": ("demo_runner.py",),
        "run": object(),
    }
    monkeypatch.setattr(main_module, "PROJECT_ROOT", tmp_path)

    preview = main_module._runner_preview(backtest)

    assert preview == contents


def test_discoverable_backtest_paths_stay_flat(tmp_path: Path) -> None:
    backtests_root = tmp_path / "backtests"
    (backtests_root / "private").mkdir(parents=True)
    (backtests_root / "nested").mkdir()

    (backtests_root / "__init__.py").write_text("")
    (backtests_root / "_script_helpers.py").write_text("")
    (backtests_root / "kalshi_trade_tick_breakout.py").write_text("")
    (backtests_root / "notebook_runner.ipynb").write_text("{}", encoding="utf-8")
    (backtests_root / "private" / "local_runner.py").write_text("")
    (backtests_root / "private" / "local_notebook.ipynb").write_text(
        "{}",
        encoding="utf-8",
    )
    (backtests_root / "private" / "_helper.py").write_text("")
    (backtests_root / "nested" / "should_not_show.py").write_text("")

    discovered = [
        path.relative_to(backtests_root)
        for path in main_module._discoverable_backtest_paths(backtests_root)
    ]

    assert discovered == [
        Path("kalshi_trade_tick_breakout.py"),
        Path("notebook_runner.ipynb"),
        Path("private/local_notebook.ipynb"),
        Path("private/local_runner.py"),
    ]


def test_discover_reads_metadata_without_importing_modules(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root = tmp_path
    backtests_root = project_root / "backtests"
    backtests_root.mkdir()
    (backtests_root / "__init__.py").write_text("", encoding="utf-8")
    (backtests_root / "demo_runner.py").write_text(
        'NAME = "custom_demo"\n'
        'DESCRIPTION = "Demo runner"\n'
        'raise RuntimeError("should not import during discovery")\n'
        "def run() -> None:\n"
        "    pass\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(main_module, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(main_module, "BACKTESTS_ROOT", backtests_root)
    monkeypatch.setattr(
        main_module.importlib,
        "import_module",
        lambda _name: (_ for _ in ()).throw(
            AssertionError("discover imported a module")
        ),
    )

    discovered = main_module.discover()

    assert discovered == [
        {
            "name": "custom_demo",
            "description": "Demo runner",
            "module_name": "backtests.demo_runner",
            "relative_parts": ("demo_runner.py",),
        }
    ]


def test_discover_reads_notebook_metadata_without_execution(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import nbformat

    project_root = tmp_path
    backtests_root = project_root / "backtests"
    backtests_root.mkdir()
    (backtests_root / "__init__.py").write_text("", encoding="utf-8")
    notebook = nbformat.v4.new_notebook(
        metadata={
            "prediction_market_backtest": {
                "name": "custom_notebook",
                "description": "Notebook runner",
            }
        },
        cells=[nbformat.v4.new_code_cell("raise RuntimeError('should not execute')")],
    )
    nbformat.write(notebook, backtests_root / "demo_notebook.ipynb")

    monkeypatch.setattr(main_module, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(main_module, "BACKTESTS_ROOT", backtests_root)

    discovered = main_module.discover()

    assert discovered == [
        {
            "name": "custom_notebook",
            "description": "Notebook runner",
            "module_name": "backtests.demo_notebook",
            "relative_parts": ("demo_notebook.ipynb",),
        }
    ]


def test_load_runner_defers_import_failure_until_selection(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root = tmp_path
    backtests_root = project_root / "backtests"
    backtests_root.mkdir()
    (backtests_root / "__init__.py").write_text("", encoding="utf-8")
    (backtests_root / "lazy_bomb.py").write_text(
        'NAME = "lazy_bomb"\n'
        'DESCRIPTION = "Explodes on import"\n'
        'raise RuntimeError("boom")\n'
        "def run() -> None:\n"
        "    pass\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(main_module, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(main_module, "BACKTESTS_ROOT", backtests_root)
    monkeypatch.syspath_prepend(str(project_root))

    discovered = main_module.discover()

    with pytest.raises(
        RuntimeError, match=r"could not import backtests/lazy_bomb\.py: boom"
    ):
        main_module._load_runner(discovered[0])


def test_load_runner_supports_runner_local_script_helpers(
    tmp_path: Path,
    monkeypatch,
) -> None:
    project_root = tmp_path
    backtests_root = project_root / "backtests"
    backtests_root.mkdir()
    (backtests_root / "__init__.py").write_text("", encoding="utf-8")
    (backtests_root / "_script_helpers.py").write_text(
        'HELPER_VALUE = "helper-ok"\n',
        encoding="utf-8",
    )
    (backtests_root / "helper_runner.py").write_text(
        'NAME = "helper_runner"\n'
        'DESCRIPTION = "Uses a local helper shim"\n'
        "from _script_helpers import HELPER_VALUE\n"
        "\n"
        "def run() -> str:\n"
        "    return HELPER_VALUE\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(main_module, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(main_module, "BACKTESTS_ROOT", backtests_root)
    normalized_sys_path = [
        entry
        for entry in sys.path
        if Path(entry or ".").resolve() not in {project_root, backtests_root}
    ]
    monkeypatch.setattr(sys, "path", normalized_sys_path)

    discovered = main_module.discover()
    prior_helper_module = sys.modules.get("_script_helpers")

    try:
        sys.modules.pop("_script_helpers", None)
        runner = main_module._load_runner(discovered[0])
        assert runner() == "helper-ok"
        assert str(backtests_root) not in sys.path
    finally:
        if prior_helper_module is None:
            sys.modules.pop("_script_helpers", None)
        else:
            sys.modules["_script_helpers"] = prior_helper_module


def test_load_runner_executes_notebook_runner(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import nbformat

    project_root = tmp_path
    backtests_root = project_root / "backtests"
    backtests_root.mkdir()
    (backtests_root / "__init__.py").write_text("", encoding="utf-8")
    notebook = nbformat.v4.new_notebook(
        metadata={
            "prediction_market_backtest": {
                "name": "demo_notebook",
                "description": "Notebook runner",
            }
        },
        cells=[nbformat.v4.new_code_cell("x = 1")],
    )
    notebook_path = backtests_root / "demo_notebook.ipynb"
    nbformat.write(notebook, notebook_path)

    monkeypatch.setattr(main_module, "PROJECT_ROOT", project_root)
    monkeypatch.setattr(main_module, "BACKTESTS_ROOT", backtests_root)

    from prediction_market_extensions.backtesting import (
        _notebook_runner as notebook_runner,
    )

    captured: dict[str, object] = {}

    def _fake_execute_notebook_runner(path: Path, *, project_root: Path) -> None:
        captured["path"] = path
        captured["project_root"] = project_root

    monkeypatch.setattr(
        notebook_runner,
        "execute_notebook_runner",
        _fake_execute_notebook_runner,
    )

    discovered = main_module.discover()
    runner = main_module._load_runner(discovered[0])
    runner()

    assert captured == {
        "path": notebook_path,
        "project_root": project_root,
    }


def test_filter_backtests_matches_name_description_and_path() -> None:
    backtests = [
        {
            "name": "demo_runner",
            "description": "Demo runner",
            "relative_parts": ("demo_runner.py",),
        },
        {
            "name": "pmxt_runner",
            "description": "PMXT quote runner",
            "relative_parts": ("pmxt_runner.py",),
        },
    ]

    assert main_module._filter_backtests(backtests, "") == [0, 1]
    assert main_module._filter_backtests(backtests, "quote") == [1]
    assert main_module._filter_backtests(backtests, "backtests/demo_runner.py") == [0]


def test_textual_menu_keeps_preview_lazy(monkeypatch) -> None:
    if not main_module.TEXTUAL_AVAILABLE:
        pytest.skip("textual is not installed")

    preview_calls: list[str] = []
    monkeypatch.setattr(
        main_module,
        "_runner_preview",
        lambda backtest: preview_calls.append(backtest["name"]) or backtest["name"],
    )

    backtests = [
        {
            "name": "demo_runner",
            "description": "Demo runner",
            "module_name": "backtests.demo_runner",
            "relative_parts": ("demo_runner.py",),
        },
        {
            "name": "pmxt_runner",
            "description": "PMXT runner",
            "module_name": "backtests.pmxt_runner",
            "relative_parts": ("pmxt_runner.py",),
        },
    ]

    async def run() -> None:
        app = main_module._BacktestMenuApp(backtests)
        async with app.run_test(size=(120, 32)) as pilot:
            await pilot.pause()
            assert preview_calls == ["demo_runner"]
            await pilot.press("down")
            await pilot.pause()
            assert preview_calls == ["demo_runner", "pmxt_runner"]
            await pilot.press("enter")
        assert app.return_value == 1

    asyncio.run(run())


def test_textual_menu_filters_and_submits_selection() -> None:
    if not main_module.TEXTUAL_AVAILABLE:
        pytest.skip("textual is not installed")

    backtests = [
        {
            "name": "demo_runner",
            "description": "Demo runner",
            "module_name": "backtests.demo_runner",
            "relative_parts": ("demo_runner.py",),
        },
        {
            "name": "pmxt_runner",
            "description": "PMXT runner",
            "module_name": "backtests.pmxt_runner",
            "relative_parts": ("pmxt_runner.py",),
        },
    ]

    async def run() -> None:
        app = main_module._BacktestMenuApp(backtests)
        async with app.run_test(size=(120, 32)) as pilot:
            await pilot.pause()
            await pilot.press("slash")
            await pilot.pause()
            assert getattr(app.focused, "id", None) == "filter"
            await pilot.press("p", "m", "x", "t")
            await pilot.pause()
            assert app.filtered_indices == [1]
            assert (
                str(app.query_one("#details_title").content)
                == "backtests/pmxt_runner.py"
            )
            await pilot.press("enter")
        assert app.return_value == 1

    asyncio.run(run())


def test_textual_menu_shortcut_runs_selected_entry() -> None:
    if not main_module.TEXTUAL_AVAILABLE:
        pytest.skip("textual is not installed")

    backtests = [
        {
            "name": "demo_runner",
            "description": "Demo runner",
            "module_name": "backtests.demo_runner",
            "relative_parts": ("demo_runner.py",),
        },
        {
            "name": "pmxt_runner",
            "description": "PMXT runner",
            "module_name": "backtests.pmxt_runner",
            "relative_parts": ("pmxt_runner.py",),
        },
    ]

    async def run() -> None:
        app = main_module._BacktestMenuApp(backtests)
        async with app.run_test(size=(120, 32)) as pilot:
            await pilot.pause()
            await pilot.press("p")
        assert app.return_value == 1

    asyncio.run(run())


def test_textual_list_item_disables_markup() -> None:
    if not main_module.TEXTUAL_AVAILABLE:
        pytest.skip("textual is not installed")

    item = main_module._BacktestListItem(0, "[u] backtests/demo_runner.py")
    child = item._pending_children[0]

    assert getattr(child, "_render_markup", None) is False
