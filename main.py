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
import importlib
import inspect
import os
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).parent
BACKTESTS_ROOT = PROJECT_ROOT / "backtests"

DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"
ENABLE_TIMING_ENV = "BACKTEST_ENABLE_TIMING"


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


def discover() -> list[dict]:
    """Scan flat runner entrypoints for modules that expose NAME, DESCRIPTION, and run()."""
    found = []
    if not BACKTESTS_ROOT.exists():
        return found

    for path in _discoverable_backtest_paths(BACKTESTS_ROOT):
        relative_parts = path.relative_to(BACKTESTS_ROOT).parts

        mod_name = ".".join(path.relative_to(PROJECT_ROOT).with_suffix("").parts)
        try:
            mod = importlib.import_module(mod_name)
        except Exception as exc:
            rel_path = path.relative_to(PROJECT_ROOT)
            print(f"{DIM}  Warning: could not import {rel_path}: {exc}{RESET}")
            continue
        if not hasattr(mod, "run"):
            continue
        found.append(
            {
                "name": getattr(mod, "NAME", path.stem),
                "description": getattr(mod, "DESCRIPTION", ""),
                "relative_parts": relative_parts,
                "run": mod.run,
            }
        )
    return found


def _relative_parts(backtest: dict[str, Any]) -> tuple[str, ...]:
    relative_parts = backtest.get("relative_parts")
    if isinstance(relative_parts, tuple):
        return relative_parts
    if isinstance(relative_parts, list):
        return tuple(str(part) for part in relative_parts)
    return (f"{backtest['name']}.py",)


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
    print(f"\n{BOLD}Running: {chosen['name']}{RESET}\n")

    if _env_flag_enabled(ENABLE_TIMING_ENV):
        try:
            from backtests._shared._timing_test import install_timing

            install_timing()
        except ImportError:
            pass

    wall_start = time.perf_counter()
    result = chosen["run"]()
    if inspect.isawaitable(result):
        asyncio.run(result)
    wall_total = time.perf_counter() - wall_start
    print(f"\nTotal wall time: {wall_total:.2f}s")


if __name__ == "__main__":
    main()
