from __future__ import annotations

from pathlib import Path

import nautilus_trader
from prediction_market_extensions.adapters.polymarket.execution import PolymarketExecutionClient
from prediction_market_extensions.adapters.polymarket.pmxt import PolymarketPMXTDataLoader
from prediction_market_extensions.adapters.prediction_market import HistoricalReplayAdapter
from prediction_market_extensions.analysis import config as analysis_config
from prediction_market_extensions.analysis import legacy_plot_adapter
from prediction_market_extensions.analysis import tearsheet


REPO_ROOT = Path(__file__).resolve().parents[1]
EXTENSIONS_ROOT = REPO_ROOT / "prediction_market_extensions"


def test_repo_tree_no_longer_keeps_vendored_nautilus_checkout() -> None:
    assert not (REPO_ROOT / "nautilus_pm").exists()


def test_overlay_directory_removed() -> None:
    assert not (REPO_ROOT / "_nautilus_overrides").exists()


def test_bootstrap_file_removed() -> None:
    assert not (REPO_ROOT / "_nautilus_bootstrap.py").exists()


def test_nautilus_runtime_uses_upstream_package() -> None:
    nautilus_file = Path(nautilus_trader.__file__).resolve()
    assert ".venv" in nautilus_file.parts
    assert "site-packages" in nautilus_file.parts


def test_extensions_resolve_to_own_namespace() -> None:
    replay_path = Path(HistoricalReplayAdapter.__module__.replace(".", "/")).parts
    assert "prediction_market_extensions" in replay_path

    pmxt_path = Path(PolymarketPMXTDataLoader.__module__.replace(".", "/")).parts
    assert "prediction_market_extensions" in pmxt_path

    legacy_path = Path(legacy_plot_adapter.__file__).resolve()
    assert EXTENSIONS_ROOT in legacy_path.parents

    execution_path = Path(PolymarketExecutionClient.__module__.replace(".", "/")).parts
    assert "prediction_market_extensions" in execution_path

    config_path = Path(analysis_config.__file__).resolve()
    assert EXTENSIONS_ROOT in config_path.parents

    tearsheet_path = Path(tearsheet.__file__).resolve()
    assert EXTENSIONS_ROOT in tearsheet_path.parents


def test_commission_patch_installed() -> None:
    """Verify that the corrected calculate_commission is installed via monkey-patch."""
    from decimal import Decimal

    from nautilus_trader.adapters.polymarket.common.parsing import calculate_commission
    from prediction_market_extensions.adapters.polymarket.parsing import (
        calculate_commission as pm_calculate_commission,
    )

    # After conftest.py runs install_commission_patch(), the upstream function
    # should be our corrected version.
    assert calculate_commission is pm_calculate_commission

    # Verify the fee curve: at p=0.50, fee = qty * feeRate * 0.50 * 0.50
    fee = calculate_commission(
        quantity=Decimal("100"),
        price=Decimal("0.50"),
        fee_rate_bps=Decimal("200"),  # 2% = 200 bps
    )
    expected = 100 * 0.02 * 0.50 * 0.50  # = 0.50
    assert abs(fee - expected) < 0.001
