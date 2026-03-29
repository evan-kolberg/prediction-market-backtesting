from __future__ import annotations

import re
import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
NOTICE_PATH = REPO_ROOT / "NOTICE"
ROOT_NOTICE_SPLIT = "Local modifications inside the vendored NautilusTrader subtree"
LGPL_HEADER_MARKERS = (
    "Derived from NautilusTrader",
    "Modified by Evan Kolberg in this repository",
    "Distributed under the GNU Lesser General Public License Version 3.0 or later.",
)


def _tracked_files() -> list[str]:
    output = subprocess.check_output(
        ["git", "ls-files"],
        cwd=REPO_ROOT,
        text=True,
    )
    return output.splitlines()


def _has_root_lgpl_header(relative_path: str) -> bool:
    if relative_path.startswith("nautilus_pm/"):
        return False

    path = REPO_ROOT / relative_path
    try:
        header_lines = path.read_text(errors="ignore").splitlines()[:30]
    except OSError:
        return False

    for line in header_lines:
        stripped = line.lstrip()
        if not stripped.startswith("#"):
            continue
        if any(marker in stripped for marker in LGPL_HEADER_MARKERS):
            return True
    return False


def _root_notice_paths() -> set[str]:
    notice_text = NOTICE_PATH.read_text()
    root_section = notice_text.split(ROOT_NOTICE_SPLIT, maxsplit=1)[0]
    return {
        path
        for path in re.findall(r"- `([^`]+)`", root_section)
        if not path.endswith("/") and not path.startswith("nautilus_pm/")
    }


def test_notice_lists_all_root_lgpl_files() -> None:
    root_lgpl_files = {
        relative_path
        for relative_path in _tracked_files()
        if _has_root_lgpl_header(relative_path)
    }
    notice_paths = _root_notice_paths()

    assert root_lgpl_files == notice_paths
