#!/usr/bin/env python3
"""Prediction market backtest runner.

Discovers strategies in the strategies/ directory and presents an
interactive menu. Each strategy file must expose:

    NAME        str   — display name shown in the menu
    DESCRIPTION str   — one-line description shown in the menu
    run()       async — entry point called when the strategy is selected

Run via:
    uv run python main.py
    make backtest
"""

from __future__ import annotations

import asyncio
import importlib
import sys
from pathlib import Path

STRATEGIES_DIR = Path(__file__).parent / "strategies"

DIM = "\033[2m"
BOLD = "\033[1m"
CYAN = "\033[36m"
RESET = "\033[0m"


def discover() -> list[dict]:
    """Scan strategies/ for modules that expose NAME, DESCRIPTION, and run()."""
    found = []
    for path in sorted(STRATEGIES_DIR.glob("*.py")):
        if path.name.startswith("_"):
            continue
        mod_name = f"strategies.{path.stem}"
        try:
            mod = importlib.import_module(mod_name)
        except Exception as exc:
            print(f"{DIM}  Warning: could not import {path.name}: {exc}{RESET}")
            continue
        if not hasattr(mod, "run"):
            continue
        found.append(
            {
                "name": getattr(mod, "NAME", path.stem),
                "description": getattr(mod, "DESCRIPTION", ""),
                "run": mod.run,
            }
        )
    return found


def show_menu(strategies: list[dict]) -> int:
    """Print numbered menu and return the chosen index (0-based), or -1 to exit."""
    print(f"\n{BOLD}Select a strategy:{RESET}\n")
    for i, s in enumerate(strategies, 1):
        desc = f" {DIM}— {s['description']}{RESET}" if s["description"] else ""
        print(f"  {CYAN}{i}{RESET}. {s['name']}{desc}")
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
    if choice < 1 or choice > len(strategies):
        print("Invalid choice.")
        return -1

    return choice - 1


def main() -> None:
    strategies = discover()

    if not strategies:
        print(
            f"No strategies found in {STRATEGIES_DIR}/\n"
            "Create a .py file there that exposes NAME, DESCRIPTION, and an async run()."
        )
        sys.exit(1)

    idx = show_menu(strategies)
    if idx == -1:
        print("Exiting.")
        sys.exit(0)

    chosen = strategies[idx]
    print(f"\n{BOLD}Running: {chosen['name']}{RESET}\n")
    asyncio.run(chosen["run"]())


if __name__ == "__main__":
    main()
