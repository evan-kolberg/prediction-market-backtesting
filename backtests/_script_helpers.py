from __future__ import annotations

import sys
from pathlib import Path


def ensure_repo_root(script_path: str | Path) -> Path:
    path = Path(script_path).resolve()
    for parent in path.parents:
        if (parent / "backtests").is_dir() and (parent / "strategies").is_dir():
            repo_root = parent
            if str(repo_root) not in sys.path:
                sys.path.insert(0, str(repo_root))
            return repo_root
    raise RuntimeError(f"Could not determine repository root for {path}")


def parse_csv_env(raw: str) -> list[str]:
    return [value.strip() for value in raw.split(",") if value.strip()]


def parse_bool_env(raw: str, *, default: bool = True) -> bool:
    value = raw.strip().lower()
    if not value:
        return default
    return value not in {"0", "false", "no", "off"}


__all__ = [
    "ensure_repo_root",
    "parse_bool_env",
    "parse_csv_env",
]
