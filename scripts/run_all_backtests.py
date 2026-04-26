from __future__ import annotations

import argparse
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from tqdm import tqdm

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

REPO_ROOT = ensure_repo_root(__file__)
BACKTESTS_ROOT = REPO_ROOT / "backtests"
SKIP_BACKTEST_FILENAMES = {"__init__.py", "_script_helpers.py", "sitecustomize.py"}


@dataclass(frozen=True)
class RunnerResult:
    path: Path
    returncode: int
    elapsed_secs: float

    @property
    def ok(self) -> bool:
        return self.returncode == 0


def discover_runner_paths() -> list[Path]:
    return sorted(
        path.relative_to(REPO_ROOT)
        for path in BACKTESTS_ROOT.glob("*.py")
        if path.name not in SKIP_BACKTEST_FILENAMES and not path.name.startswith("_")
    )


def _resolve_selected_runners(raw_values: list[str] | None) -> list[Path]:
    available = discover_runner_paths()
    if not raw_values:
        return available

    by_relative = {path.as_posix(): path for path in available}
    by_name = {path.name: path for path in available}
    selected: list[Path] = []
    missing: list[str] = []

    for raw_value in raw_values:
        value = raw_value.strip()
        if not value:
            continue
        candidate = by_relative.get(value) or by_name.get(value)
        if candidate is None:
            missing.append(value)
            continue
        if candidate not in selected:
            selected.append(candidate)

    if missing:
        available_labels = ", ".join(path.as_posix() for path in available)
        raise SystemExit(
            "Unknown runner selection(s): "
            f"{', '.join(missing)}\nAvailable runners: {available_labels}"
        )

    return selected


def _run_runner(relative_path: Path, *, python_executable: str) -> RunnerResult:
    command = [python_executable, str(relative_path)]
    started = time.monotonic()
    completed = subprocess.run(command, cwd=REPO_ROOT, check=False)
    return RunnerResult(
        path=relative_path,
        returncode=int(completed.returncode),
        elapsed_secs=time.monotonic() - started,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run every public backtest script under backtests/ one at a time using "
            "their direct script entrypoints."
        )
    )
    parser.add_argument(
        "--runner",
        action="append",
        default=[],
        help=(
            "Run only the selected runner(s). Accepts either the bare filename, "
            "for example 'polymarket_book_ema_crossover.py', or the repo-relative "
            "path, for example 'backtests/polymarket_book_ema_crossover.py'. "
            "Repeat to select multiple."
        ),
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print the discovered public backtest runners and exit.",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop immediately after the first failing runner.",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to use for subprocess runs. Defaults to the current interpreter.",
    )
    args = parser.parse_args()

    runner_paths = _resolve_selected_runners(args.runner)
    if args.list:
        for path in runner_paths:
            print(path.as_posix())
        return 0

    if not runner_paths:
        print("No public backtest runners were discovered.")
        return 1

    results: list[RunnerResult] = []
    progress = tqdm(runner_paths, unit="runner", desc="Running backtests")
    for relative_path in progress:
        progress.set_postfix_str(relative_path.name)
        result = _run_runner(relative_path, python_executable=args.python)
        results.append(result)
        status = "ok" if result.ok else f"fail({result.returncode})"
        progress.write(f"[{status}] {relative_path.as_posix()} {result.elapsed_secs:.1f}s")
        if args.stop_on_failure and not result.ok:
            break

    passed = [result for result in results if result.ok]
    failed = [result for result in results if not result.ok]

    print()
    print(f"Completed {len(results)} runner(s): {len(passed)} passed, {len(failed)} failed.")
    for result in failed:
        print(
            f"FAILED {result.path.as_posix()} "
            f"(exit={result.returncode}, elapsed={result.elapsed_secs:.1f}s)"
        )

    return 0 if not failed and len(results) == len(runner_paths) else 1


if __name__ == "__main__":
    raise SystemExit(main())
