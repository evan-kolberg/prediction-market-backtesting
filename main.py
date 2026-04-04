#!/usr/bin/env python3
"""Prediction market backtest runner.

Discovers runnable modules in flat runner entrypoints under `backtests/` and
`backtests/private/` and presents an interactive menu. Each backtest file must expose:

    NAME        str   — display name shown in the menu
    DESCRIPTION str   — one-line description shown in the menu
    run()       sync or async — entry point called when the backtest is selected

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

try:
    from simple_term_menu import TerminalMenu
except ImportError:  # pragma: no cover - fallback is covered through non-TTY tests
    TerminalMenu = None

PROJECT_ROOT = Path(__file__).parent
BACKTESTS_ROOT = PROJECT_ROOT / "backtests"

DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"
ENABLE_TIMING_ENV = "BACKTEST_ENABLE_TIMING"
SHORTCUT_LETTERS = ascii_lowercase.replace("q", "") + ascii_uppercase.replace("Q", "")
MENU_TITLE = "Prediction Market Backtests"
MENU_PREVIEW_SIZE = 0.30
RUNNER_SPEC_MAX_LINES = 10


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
        *backtests_root.glob("private/*.py"),
    ]
    return sorted(
        path
        for path in candidates
        if path.is_file()
        and path.name != "__init__.py"
        and not path.name.startswith("_")
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


def _has_run_entrypoint(module_ast: ast.Module) -> bool:
    for node in module_ast.body:
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "run"
        ):
            return True
        if isinstance(
            node, (ast.Assign, ast.AnnAssign)
        ) and "run" in _assignment_targets(
            node,
        ):
            return True
    return False


def _load_runner_metadata(path: Path) -> dict[str, Any] | None:
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

    if not _has_run_entrypoint(module_ast):
        return None

    name = path.stem
    description = ""
    for node in module_ast.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = _assignment_targets(node)
        if not targets:
            continue
        literal = _literal_string(node.value)
        if literal is None:
            continue
        if "NAME" in targets:
            name = literal
        if "DESCRIPTION" in targets:
            description = literal

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


def _shortcut_candidates(backtest: dict[str, Any]) -> list[str]:
    words = re.findall(
        r"[A-Za-z]+",
        f"{backtest.get('name', '')} {_runner_stem(backtest)} {_menu_label(backtest)}",
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


def _assign_shortcuts(
    backtests: list[dict[str, Any]],
) -> dict[str, str | None]:
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
def _runner_spec_preview(path: Path, *, max_lines: int = RUNNER_SPEC_MAX_LINES) -> str:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        return f"(unable to read runner file: {exc})"

    start = 0
    for index, line in enumerate(lines):
        if (
            line.startswith("NAME =")
            or line.startswith("DESCRIPTION =")
            or line.startswith("DATA =")
        ):
            start = index
            break

    snippet_lines = lines[start : start + max_lines]
    return "\n".join(snippet_lines).strip() or "(runner file is empty)"


def _runner_preview(backtest: dict[str, Any]) -> str:
    relative_path = _relative_runner_path(backtest)
    description = backtest.get("description") or "No description provided."
    snippet = _runner_spec_preview(PROJECT_ROOT / relative_path)

    return (
        f"{relative_path}\n\n"
        f"{description}\n\n"
        "Run\n"
        f"  uv run python {relative_path}\n\n"
        "Spec\n"
        f"{snippet}"
    )


def _load_runner(backtest: dict[str, Any]) -> Any:
    relative_path = _relative_runner_path(backtest)
    module_name = backtest["module_name"]
    runner_path = PROJECT_ROOT / relative_path
    spec = importlib.util.spec_from_file_location(module_name, runner_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"could not import {relative_path}: no module spec")

    module = importlib.util.module_from_spec(spec)
    prior_module = sys.modules.get(module_name)

    try:
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    except Exception as exc:
        raise RuntimeError(f"could not import {relative_path}: {exc}") from exc
    finally:
        if prior_module is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = prior_module

    runner = getattr(module, "run", None)
    if not callable(runner):
        raise RuntimeError(f"{relative_path} does not expose a callable run()")
    return runner


def _supports_terminal_menu() -> bool:
    if TerminalMenu is None:
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


def _show_terminal_menu(backtests: list[dict[str, Any]]) -> int:
    shortcuts = _assign_shortcuts(backtests)

    backtests_by_key: dict[str, dict[str, Any]] = {}
    menu_entries: list[str] = []
    for backtest in backtests:
        relative_key = _relative_runner_path(backtest).as_posix()
        shortcut = shortcuts[relative_key]
        backtests_by_key[relative_key] = backtest
        if shortcut is None:
            menu_entries.append(f"{_menu_label(backtest)}|{relative_key}")
        else:
            menu_entries.append(f"[{shortcut}] {_menu_label(backtest)}|{relative_key}")

    terminal_menu = TerminalMenu(
        menu_entries,
        title=(
            MENU_TITLE,
            "assigned letters run immediately, enter runs selection, / searches, q exits",
            f"{len(backtests)} runnable entries | preview shows the flat runner spec",
        ),
        menu_cursor="> ",
        menu_cursor_style=("fg_cyan", "bold"),
        menu_highlight_style=("bold",),
        shortcut_brackets_highlight_style=("fg_gray",),
        shortcut_key_highlight_style=("fg_blue", "bold"),
        preview_command=lambda preview_key: _runner_preview(
            backtests_by_key[preview_key]
        ),
        preview_size=MENU_PREVIEW_SIZE,
        preview_title="runner",
        search_case_sensitive=False,
        show_search_hint=True,
        show_shortcut_hints=False,
    )
    selection = terminal_menu.show()
    if selection is None:
        return -1

    selected_entry = menu_entries[selection]
    selected_key = selected_entry.split("|", 1)[1]
    for index, backtest in enumerate(backtests):
        if _relative_runner_path(backtest).as_posix() == selected_key:
            return index
    return -1


def _build_menu_tree(backtests: list[dict[str, Any]]) -> dict[str, Any]:
    root: dict[str, Any] = {"dirs": {}, "entries": []}
    for index, backtest in enumerate(backtests, start=1):
        node = root
        relative_parts = _relative_parts(backtest)
        for folder in relative_parts[:-1]:
            node = node["dirs"].setdefault(folder, {"dirs": {}, "entries": []})
        node["entries"].append((index, relative_parts[-1], backtest))
    return root


def _render_menu_tree(
    node: dict[str, Any],
    *,
    prefix: str = "",
) -> list[str]:
    lines: list[str] = []
    children: list[tuple[str, Any, Any]] = [
        ("dir", name, child_node) for name, child_node in node["dirs"].items()
    ]
    children.extend(
        ("entry", (index, filename), backtest)
        for index, filename, backtest in node["entries"]
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
    if _supports_terminal_menu():
        try:
            return _show_terminal_menu(backtests)
        except (NotImplementedError, OSError, subprocess.SubprocessError):
            pass
    return _show_basic_menu(backtests)


def main() -> None:
    backtests = discover()

    if not backtests:
        print(
            f"No backtests found in {BACKTESTS_ROOT}\n"
            "Create a .py file in backtests/ or backtests/private/ that exposes "
            "NAME, DESCRIPTION, and a run() entrypoint."
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
            from backtests._shared._timing_test import install_timing

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
