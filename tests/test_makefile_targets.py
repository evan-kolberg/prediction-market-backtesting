from __future__ import annotations

import subprocess
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_clear_telonex_cache_does_not_delete_local_data_destination() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "clear-telonex-cache",
            "TELONEX_DATA_DESTINATION=/tmp/local-telonex-data",
            "TELONEX_CACHE_ROOT=/tmp/telonex-runner-cache",
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert 'rm -rf "/tmp/local-telonex-data"' not in result.stdout
    assert 'rm -rf "/tmp/telonex-runner-cache"' in result.stdout
    assert "Telonex runner API downloads are not persisted" not in result.stdout
    assert "Clearing Telonex cache root only" not in result.stdout


def test_clear_telonex_cache_refuses_data_destination(tmp_path: Path) -> None:
    data_root = tmp_path / "telonex-data"
    data_root.mkdir()
    marker = data_root / "marker.parquet"
    marker.write_text("keep")

    result = subprocess.run(
        [
            "make",
            "clear-telonex-cache",
            f"TELONEX_DATA_DESTINATION={data_root}",
            f"TELONEX_CACHE_ROOT={data_root}",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Refusing to clear unsafe TELONEX_CACHE_ROOT" in result.stderr
    assert marker.read_text() == "keep"


def test_clear_telonex_cache_refuses_path_inside_data_destination(tmp_path: Path) -> None:
    data_root = tmp_path / "telonex-data"
    cache_root = data_root / "api-cache"
    cache_root.mkdir(parents=True)
    marker = cache_root / "marker.parquet"
    marker.write_text("keep")

    result = subprocess.run(
        [
            "make",
            "clear-telonex-cache",
            f"TELONEX_DATA_DESTINATION={data_root}",
            f"TELONEX_CACHE_ROOT={cache_root}",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Refusing to clear unsafe TELONEX_CACHE_ROOT" in result.stderr
    assert marker.read_text() == "keep"


def test_clear_telonex_cache_refuses_parent_of_data_destination(tmp_path: Path) -> None:
    data_root = tmp_path / "telonex-data"
    data_root.mkdir()
    marker = data_root / "marker.parquet"
    marker.write_text("keep")

    result = subprocess.run(
        [
            "make",
            "clear-telonex-cache",
            f"TELONEX_DATA_DESTINATION={data_root}",
            f"TELONEX_CACHE_ROOT={tmp_path}",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Refusing to clear unsafe TELONEX_CACHE_ROOT" in result.stderr
    assert marker.read_text() == "keep"


def test_clear_pmxt_cache_refuses_local_data_root(tmp_path: Path) -> None:
    data_root = tmp_path / "pmxt-raws"
    data_root.mkdir()
    marker = data_root / "polymarket_orderbook_2026-01-01T00.parquet"
    marker.write_text("keep")

    result = subprocess.run(
        [
            "make",
            "clear-pmxt-cache",
            f"PMXT_CACHE_ROOT={data_root}",
            f"PMXT_LOCAL_DATA_ROOT={data_root}",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Refusing to clear unsafe PMXT_CACHE_ROOT" in result.stderr
    assert marker.read_text() == "keep"


def test_clear_pmxt_cache_refuses_path_inside_local_data_root(tmp_path: Path) -> None:
    data_root = tmp_path / "pmxt-raws"
    cache_root = data_root / "filtered-cache"
    cache_root.mkdir(parents=True)
    marker = cache_root / "polymarket_orderbook_2026-01-01T00.parquet"
    marker.write_text("keep")

    result = subprocess.run(
        [
            "make",
            "clear-pmxt-cache",
            f"PMXT_CACHE_ROOT={cache_root}",
            f"PMXT_LOCAL_DATA_ROOT={data_root}",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "Refusing to clear unsafe PMXT_CACHE_ROOT" in result.stderr
    assert marker.read_text() == "keep"
