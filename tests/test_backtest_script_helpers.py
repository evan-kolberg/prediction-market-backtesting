from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backtests._script_helpers import ensure_repo_root, parse_bool_env, parse_csv_env


def test_ensure_repo_root_bootstraps_repo_path_and_commission_patch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo_root = tmp_path / "repo"
    backtests_dir = repo_root / "backtests"
    strategies_dir = repo_root / "strategies"
    backtests_dir.mkdir(parents=True)
    strategies_dir.mkdir()
    script_path = backtests_dir / "demo_runner.py"
    script_path.write_text("")

    added_paths: list[str] = []
    commission_calls: list[str] = []
    monkeypatch.setattr(
        "backtests._script_helpers.sys.path",
        added_paths,
    )
    monkeypatch.setattr(
        "backtests._script_helpers.importlib.import_module",
        lambda name: SimpleNamespace(
            install_commission_patch=lambda: commission_calls.append(name)
        ),
    )

    resolved = ensure_repo_root(script_path)

    assert resolved == repo_root
    assert added_paths == [str(repo_root)]
    assert commission_calls == ["prediction_market_extensions"]


def test_ensure_repo_root_raises_when_repo_markers_are_missing(tmp_path: Path) -> None:
    script_path = tmp_path / "scripts" / "demo_runner.py"
    script_path.parent.mkdir(parents=True)
    script_path.write_text("")

    with pytest.raises(RuntimeError, match="Could not determine repository root"):
        ensure_repo_root(script_path)


def test_parse_csv_env_discards_blank_entries() -> None:
    assert parse_csv_env(" alpha, ,beta ,, gamma ") == ["alpha", "beta", "gamma"]


@pytest.mark.parametrize(
    ("raw", "default", "expected"),
    [
        ("", True, True),
        ("", False, False),
        ("0", True, False),
        ("false", True, False),
        ("NO", True, False),
        ("off", True, False),
        ("1", False, True),
        ("yes", False, True),
    ],
)
def test_parse_bool_env_respects_common_false_tokens(
    raw: str, default: bool, expected: bool
) -> None:
    assert parse_bool_env(raw, default=default) is expected
