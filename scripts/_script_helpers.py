# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

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
