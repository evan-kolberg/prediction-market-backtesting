#!/usr/bin/env python3
"""Prediction market backtest runner.

Discovers runnable modules in flat runner entrypoints under `backtests/` and
`backtests/private/` and presents an interactive menu. Each backtest file must expose:

    EXPERIMENT  object — manifest executed by the shared experiment dispatcher,
                         constructed with explicit `name=` and `description=` kwargs
    run()       sync or async — optional thin entry point called when selected

The display name and one-line description are pulled from the `name=` and
`description=` kwargs of the EXPERIMENT constructor call via AST scanning so the
menu does not need to import each runner module on startup.

Run via:
    uv run python main.py
    make backtest
"""

from __future__ import annotations

import asyncio
import ast
import importlib
import importlib.util
import inspect
import os
import re
import subprocess
import sys
import time
from functools import lru_cache
from pathlib import Path
from string import ascii_lowercase
from string import ascii_uppercase
from typing import Any

from prediction_market_extensions import install_commission_patch

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Horizontal, Vertical
    from textual.events import Key
    from textual.widgets import Footer, Input, ListItem, ListView, Static

    TEXTUAL_AVAILABLE = True
except ImportError:  # pragma: no cover - fallback is covered through non-TTY tests
    App = None  # type: ignore[assignment]
    ComposeResult = Any  # type: ignore[misc,assignment]
    Binding = None  # type: ignore[assignment]
    Horizontal = Vertical = None  # type: ignore[assignment]
    Footer = Input = ListItem = ListView = Static = None  # type: ignore[assignment]
    Key = None  # type: ignore[assignment]
    TEXTUAL_AVAILABLE = False

install_commission_patch()

PROJECT_ROOT = Path(__file__).parent
BACKTESTS_ROOT = PROJECT_ROOT / "backtests"

DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"
ENABLE_TIMING_ENV = "BACKTEST_ENABLE_TIMING"
SHORTCUT_LETTERS = ascii_lowercase.replace("q", "") + ascii_uppercase.replace("Q", "")
MENU_TITLE = "Prediction Market Backtests"


def _env_flag_enabled(name: str) -> bool:
    value = os.getenv(name)
    if value is None:
        return True
    return value.strip().casefold() not in {"0", "false", "no", "off"}


def _discoverable_backtest_paths(backtests_root: Path) -> list[Path]:
    """Return flat public runner files plus flat private runner files."""
    if not backtests_root.exists():
        return []

    candidates = [
        *backtests_root.glob("*.py"),
        *backtests_root.glob("*.ipynb"),
        *backtests_root.glob("private/*.py"),
        *backtests_root.glob("private/*.ipynb"),
    ]
    return sorted(
        path
        for path in candidates
        if path.is_file() and path.name != "__init__.py" and not path.name.startswith("_")
    )


def _warn(message: str) -> None:
    print(f"{DIM}  Warning: {message}{RESET}")


def _literal_string(node: ast.AST | None) -> str | None:
    if node is None:
        return None
    try:
        value = ast.literal_eval(node)
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, str) else None


def _assignment_targets(node: ast.Assign | ast.AnnAssign) -> list[str]:
    if isinstance(node, ast.Assign):
        return [target.id for target in node.targets if isinstance(target, ast.Name)]
    if isinstance(node.target, ast.Name):
        return [node.target.id]
    return []


def _has_assignment(module_ast: ast.Module, target_name: str) -> bool:
    for node in module_ast.body:
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and target_name in _assignment_targets(
            node
        ):
            return True
    return False


def _experiment_constructor_kwargs(module_ast: ast.Module) -> dict[str, str] | None:
    """Extract `name=`/`description=` literal kwargs from the EXPERIMENT assignment call."""
    for node in module_ast.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        if "EXPERIMENT" not in _assignment_targets(node):
            continue
        if not isinstance(node.value, ast.Call):
            continue
        kwargs: dict[str, str] = {}
        for keyword in node.value.keywords:
            if keyword.arg in {"name", "description"}:
                literal = _literal_string(keyword.value)
                if literal is not None:
                    kwargs[keyword.arg] = literal
        return kwargs
    return None


def _has_run_entrypoint(module_ast: ast.Module) -> bool:
    for node in module_ast.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "run":
            return True
    return False


def _load_runner_metadata(path: Path) -> dict[str, Any] | None:
    if path.suffix == ".ipynb":
        from prediction_market_extensions.backtesting._notebook_runner import load_notebook_metadata

        return load_notebook_metadata(path, project_root=PROJECT_ROOT)

    relative_path = path.relative_to(PROJECT_ROOT)

    try:
        source = path.read_text(encoding="utf-8")
    except OSError as exc:
        _warn(f"could not read {relative_path}: {exc}")
        return None

    try:
        module_ast = ast.parse(source, filename=str(path))
    except SyntaxError as exc:
        _warn(f"could not parse {relative_path}: {exc}")
        return None

    if not (_has_assignment(module_ast, "EXPERIMENT") or _has_run_entrypoint(module_ast)):
        return None

    name = path.stem
    description = ""
    experiment_kwargs = _experiment_constructor_kwargs(module_ast)
    if experiment_kwargs is not None:
        kw_name = experiment_kwargs.get("name")
        kw_description = experiment_kwargs.get("description")
        if kw_name:
            name = kw_name
        if kw_description:
            description = kw_description

    return {
        "name": name,
        "description": description,
        "module_name": ".".join(relative_path.with_suffix("").parts),
        "relative_parts": path.relative_to(BACKTESTS_ROOT).parts,
    }


def discover() -> list[dict]:
    """Scan flat runner entrypoints without importing them on menu startup."""
    found = []
    if not BACKTESTS_ROOT.exists():
        return found

    for path in _discoverable_backtest_paths(BACKTESTS_ROOT):
        metadata = _load_runner_metadata(path)
        if metadata is not None:
            found.append(metadata)
    return found


def _relative_parts(backtest: dict[str, Any]) -> tuple[str, ...]:
    relative_parts = backtest.get("relative_parts")
    if isinstance(relative_parts, tuple):
        return relative_parts
    if isinstance(relative_parts, list):
        return tuple(str(part) for part in relative_parts)
    return (f"{backtest['name']}.py",)


def _relative_runner_path(backtest: dict[str, Any]) -> Path:
    return Path("backtests", *_relative_parts(backtest))


def _runner_stem(backtest: dict[str, Any]) -> str:
    return Path(_relative_parts(backtest)[-1]).stem


def _menu_label(backtest: dict[str, Any]) -> str:
    return _relative_runner_path(backtest).as_posix()


def _textual_menu_label(backtest: dict[str, Any], shortcut: str | None) -> str:
    label = _menu_label(backtest)
    if shortcut is None:
        return label
    return f"[{shortcut}] {label}"


def _runner_search_text(backtest: dict[str, Any]) -> str:
    return " ".join(
        part
        for part in (
            backtest.get("name", ""),
            backtest.get("description", ""),
            _menu_label(backtest),
        )
        if part
    ).casefold()


def _filter_backtests(backtests: list[dict[str, Any]], query: str) -> list[int]:
    normalized = query.strip().casefold()
    if not normalized:
        return list(range(len(backtests)))
    return [
        index
        for index, backtest in enumerate(backtests)
        if normalized in _runner_search_text(backtest)
    ]


def _shortcut_candidates(backtest: dict[str, Any]) -> list[str]:
    words = re.findall(
        r"[A-Za-z]+", f"{backtest.get('name', '')} {_runner_stem(backtest)} {_menu_label(backtest)}"
    )
    candidates: list[str] = []
    seen: set[str] = set()

    for word in words:
        candidate = word[0].lower()
        if candidate == "q":
            continue
        if candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)

    for word in words:
        for candidate in word[1:].lower():
            if candidate == "q":
                continue
            if candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)

    for candidate in SHORTCUT_LETTERS:
        if candidate not in seen:
            seen.add(candidate)
            candidates.append(candidate)

    return candidates


def _assign_shortcuts(backtests: list[dict[str, Any]]) -> dict[str, str | None]:
    shortcuts: dict[str, str | None] = {}
    used: set[str] = set()

    for backtest in backtests:
        key = _relative_runner_path(backtest).as_posix()
        for candidate in _shortcut_candidates(backtest):
            if candidate not in used:
                used.add(candidate)
                shortcuts[key] = candidate
                break
        else:
            shortcuts[key] = None

    return shortcuts


@lru_cache(maxsize=None)
def _runner_file_preview(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except OSError as exc:
        return f"(unable to read runner file: {exc})"


def _runner_preview(backtest: dict[str, Any]) -> str:
    return _runner_file_preview(PROJECT_ROOT / _relative_runner_path(backtest))


if TEXTUAL_AVAILABLE:

    class _BacktestListItem(ListItem):
        def __init__(self, backtest_index: int, label: str) -> None:
            super().__init__(Static(label, classes="runner-label", markup=False))
            self.backtest_index = backtest_index

    class _BacktestMenuApp(App[int]):
        CSS = """
        Screen {
            background: $surface;
            color: $text;
        }

        #banner {
            dock: top;
            height: 1;
            padding: 0 1;
            background: $panel;
            color: $text;
            text-style: bold;
        }

        #body {
            height: 1fr;
            padding: 0 1;
        }

        #sidebar {
            width: 80;
            min-width: 60;
            border: round $primary;
            padding: 0 1;
            margin: 1 1 1 0;
        }

        #filter {
            margin: 1 0;
        }

        #runner_list {
            height: 1fr;
            border: none;
        }

        _BacktestListItem {
            padding: 0 1;
        }

        .runner-label {
            width: 1fr;
            text-wrap: nowrap;
        }

        #details {
            width: 1fr;
            border: round $secondary;
            padding: 0 1;
            margin: 1 0 1 0;
        }

        #details_title {
            margin-top: 1;
            text-style: bold;
            color: $primary;
        }

        #details_meta {
            margin: 1 0;
        }

        #preview_heading {
            text-style: bold;
            color: $text;
        }

        #details_preview {
            height: 1fr;
            margin: 1 0 0 0;
            padding: 0 1 1 1;
            border: tall $secondary;
            overflow: auto auto;
            text-wrap: nowrap;
        }
        """

        BINDINGS = [
            Binding("q", "quit", "Quit"),
            Binding("slash", "focus_filter", "Filter", key_display="/"),
            Binding("escape", "dismiss_filter", "Back"),
            Binding("enter", "run_selected", "Run"),
        ]

        def __init__(self, backtests: list[dict[str, Any]]) -> None:
            super().__init__()
            self.backtests = backtests
            self.shortcuts = _assign_shortcuts(backtests)
            self.shortcut_to_index = {
                shortcut: index
                for index, backtest in enumerate(backtests)
                if (shortcut := self.shortcuts[_menu_label(backtest)]) is not None
            }
            self.filtered_indices: list[int] = list(range(len(backtests)))
            self._details_backtest_index: int | None = None

        def compose(self) -> ComposeResult:
            yield Static("", id="banner")
            with Horizontal(id="body"):
                with Vertical(id="sidebar"):
                    yield Input(
                        placeholder="Filter runners",
                        compact=True,
                        id="filter",
                    )

                    yield ListView(id="runner_list")
                with Vertical(id="details"):
                    yield Static("", id="details_title", markup=False)
                    yield Static("", id="details_meta", markup=False)
                    yield Static("File Preview", id="preview_heading")
                    yield Static("", id="details_preview", markup=False)
            yield Footer(show_command_palette=False, compact=True)

        async def on_mount(self) -> None:
            await self._refresh_menu()
            self.query_one(ListView).focus()

        def _set_banner(self) -> None:
            query = self.query_one(Input).value.strip()
            shown = len(self.filtered_indices)
            total = len(self.backtests)
            if query:
                banner = f"{MENU_TITLE} | showing {shown} of {total} for '{query}'"
            else:
                banner = f"{MENU_TITLE} | {total} runnable entries"
            self.query_one("#banner", Static).update(banner)

        def _update_details(self, backtest_index: int | None) -> None:
            if backtest_index == self._details_backtest_index:
                return

            title = self.query_one("#details_title", Static)
            meta = self.query_one("#details_meta", Static)
            preview = self.query_one("#details_preview", Static)

            if backtest_index is None:
                query = self.query_one(Input).value.strip()
                title.update("No runners match the current filter.")
                meta.update(f"Filter: {query}" if query else "No runnable backtests found.")
                preview.update("Try a broader search or press Esc to clear the filter.")
                self._details_backtest_index = None
                return

            backtest = self.backtests[backtest_index]
            shortcut = self.shortcuts[_menu_label(backtest)]
            description = str(backtest.get("description") or "").strip()
            title.update(_menu_label(backtest))
            meta_lines = [f"runner: {backtest.get('name', _runner_stem(backtest))}"]
            if shortcut is not None:
                meta_lines.append(f"shortcut: {shortcut}")
            if description:
                meta_lines.append(description)
            meta.update("\n".join(meta_lines))
            preview.update(_runner_preview(backtest))
            self._details_backtest_index = backtest_index

        async def _refresh_menu(self, preferred_index: int | None = None) -> None:
            list_view = self.query_one(ListView)
            query = self.query_one(Input).value
            self.filtered_indices = _filter_backtests(self.backtests, query)
            items = [
                _BacktestListItem(
                    backtest_index=index,
                    label=_textual_menu_label(
                        self.backtests[index], self.shortcuts[_menu_label(self.backtests[index])]
                    ),
                )
                for index in self.filtered_indices
            ]
            await list_view.clear()
            if items:
                await list_view.extend(items)
                target_index = (
                    preferred_index
                    if preferred_index in self.filtered_indices
                    else self.filtered_indices[0]
                )
                list_view.index = self.filtered_indices.index(target_index)
                self._update_details(target_index)
            else:
                self._update_details(None)
            self._set_banner()

        def _selected_backtest_index(self) -> int | None:
            highlighted = self.query_one(ListView).highlighted_child
            if isinstance(highlighted, _BacktestListItem):
                return highlighted.backtest_index
            return None

        def action_focus_filter(self) -> None:
            self.query_one(Input).focus()

        def action_dismiss_filter(self) -> None:
            filter_widget = self.query_one(Input)
            if filter_widget.value:
                filter_widget.value = ""
            self.query_one(ListView).focus()

        def action_run_selected(self) -> None:
            selected = self._selected_backtest_index()
            if selected is not None:
                self.exit(selected)

        async def on_input_changed(self, event: Input.Changed) -> None:
            if event.input.id != "filter":
                return
            await self._refresh_menu(self._selected_backtest_index())

        def on_input_submitted(self, event: Input.Submitted) -> None:
            if event.input.id != "filter":
                return
            self.action_run_selected()

        def on_list_view_highlighted(self, event: ListView.Highlighted) -> None:
            if isinstance(event.item, _BacktestListItem):
                self._update_details(event.item.backtest_index)

        def on_list_view_selected(self, event: ListView.Selected) -> None:
            if isinstance(event.item, _BacktestListItem):
                self.exit(event.item.backtest_index)

        def on_key(self, event: Key) -> None:
            if self.focused is self.query_one(Input):
                return
            character = event.character
            if character is None:
                return
            selected = self.shortcut_to_index.get(character)
            if selected is None:
                return
            self.exit(selected)
            event.stop()


def _load_runner(backtest: dict[str, Any]) -> Any:
    relative_path = _relative_runner_path(backtest)
    runner_path = PROJECT_ROOT / relative_path
    if runner_path.suffix == ".ipynb":
        from prediction_market_extensions.backtesting._notebook_runner import (
            execute_notebook_runner,
        )

        def _run_notebook() -> None:
            execute_notebook_runner(runner_path, project_root=PROJECT_ROOT)

        return _run_notebook

    module_name = backtest["module_name"]
    spec = importlib.util.spec_from_file_location(module_name, runner_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not import {relative_path}: no module spec")

    module = importlib.util.module_from_spec(spec)
    prior_module = sys.modules.get(module_name)
    prior_sys_path = list(sys.path)

    try:
        sys.path.insert(0, str(runner_path.parent))
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    except Exception as exc:
        raise RuntimeError(f"could not import {relative_path}: {exc}") from exc
    finally:
        sys.path[:] = prior_sys_path
        if prior_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = prior_module

    runner = getattr(module, "run", None)
    if callable(runner):
        return runner

    experiment = getattr(module, "EXPERIMENT", None)
    if experiment is None:
        raise RuntimeError(f"{relative_path} does not expose EXPERIMENT or run()")

    from prediction_market_extensions.backtesting._experiments import run_experiment

    def _run_manifest() -> Any:
        return run_experiment(experiment)

    return _run_manifest


def _supports_textual_menu() -> bool:
    if not TEXTUAL_AVAILABLE or App is None:
        return False
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return False
    term = os.getenv("TERM", "").strip()
    if not term or term.casefold() in {"dumb", "unknown"}:
        return False
    try:
        probe = subprocess.run(
            ["tput", "clear"],
            check=False,
            env={**os.environ, "TERM": term},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        return False
    return probe.returncode == 0


def _show_basic_menu(backtests: list[dict[str, Any]]) -> int:
    """Print numbered menu and return the chosen index (0-based), or -1 to exit."""
    print(f"\n{BOLD}Select a backtest:{RESET}\n")
    print(f"  {BOLD}backtests/{RESET}")
    for line in _render_menu_tree(_build_menu_tree(backtests), prefix="  "):
        print(line)
    print(f"\n  {DIM}0. Exit{RESET}\n")

    try:
        raw = input("Enter number: ").strip()
    except (EOFError, KeyboardInterrupt):
        return -1

    try:
        choice = int(raw)
    except ValueError:
        print("Invalid input.")
        return -1

    if choice == 0:
        return -1
    if choice < 1 or choice > len(backtests):
        print("Invalid choice.")
        return -1

    return choice - 1


def _show_textual_menu(backtests: list[dict[str, Any]]) -> int:
    if not TEXTUAL_AVAILABLE:
        raise NotImplementedError("Textual is not installed")
    app = _BacktestMenuApp(backtests)
    selection = app.run()
    if selection is None:
        return -1
    return int(selection)


def _build_menu_tree(backtests: list[dict[str, Any]]) -> dict[str, Any]:
    root: dict[str, Any] = {"dirs": {}, "entries": []}
    for index, backtest in enumerate(backtests, start=1):
        node = root
        relative_parts = _relative_parts(backtest)
        for folder in relative_parts[:-1]:
            node = node["dirs"].setdefault(folder, {"dirs": {}, "entries": []})
        node["entries"].append((index, relative_parts[-1], backtest))
    return root


def _render_menu_tree(node: dict[str, Any], *, prefix: str = "") -> list[str]:
    lines: list[str] = []
    children: list[tuple[str, Any, Any]] = [
        ("dir", name, child_node) for name, child_node in node["dirs"].items()
    ]
    children.extend(
        ("entry", (index, filename), backtest) for index, filename, backtest in node["entries"]
    )

    for position, (kind, payload, child) in enumerate(children):
        is_last = position == len(children) - 1
        connector = "└── " if is_last else "├── "
        child_prefix = prefix + ("    " if is_last else "│   ")

        if kind == "dir":
            lines.append(f"{prefix}{connector}{payload}/")
            lines.extend(_render_menu_tree(child, prefix=child_prefix))
            continue

        index, filename = payload
        description = child["description"]
        desc = f" {DIM}— {description}{RESET}" if description else ""
        lines.append(f"{prefix}{connector}{index}. {filename}{desc}")

    return lines


def show_menu(backtests: list[dict]) -> int:
    """Return the chosen backtest index, or -1 to exit."""
    if _supports_textual_menu():
        try:
            return _show_textual_menu(backtests)
        except (NotImplementedError, OSError, subprocess.SubprocessError):
            pass
    return _show_basic_menu(backtests)


def main() -> None:
    backtests = discover()

    if not backtests:
        print(
            f"No backtests found in {BACKTESTS_ROOT}\n"
            "Create a flat .py or .ipynb file in backtests/ or backtests/private/."
        )
        sys.exit(1)

    idx = show_menu(backtests)
    if idx == -1:
        print("Exiting.")
        sys.exit(0)

    chosen = backtests[idx]
    try:
        runner = _load_runner(chosen)
    except RuntimeError as exc:
        print(f"\n{exc}")
        sys.exit(1)

    print(f"\n{BOLD}Running: {chosen['name']}{RESET}\n")

    if _env_flag_enabled(ENABLE_TIMING_ENV):
        try:
            from prediction_market_extensions.backtesting._timing_test import install_timing

            install_timing()
        except ImportError:
            pass

    wall_start = time.perf_counter()
    result = runner()
    if inspect.isawaitable(result):
        asyncio.run(result)
    wall_total = time.perf_counter() - wall_start
    print(f"\nTotal wall time: {wall_total:.2f}s")


if __name__ == "__main__":
    main()
