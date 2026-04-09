from __future__ import annotations

from pathlib import Path
import importlib
import sys

import nautilus_trader
import nautilus_trader.adapters as nautilus_adapters
from nautilus_trader.adapters.polymarket.execution import PolymarketExecutionClient
from nautilus_trader.adapters.polymarket import PolymarketPMXTDataLoader
from nautilus_trader.adapters.prediction_market import HistoricalReplayAdapter
from nautilus_trader.analysis import config as analysis_config
from nautilus_trader.analysis import legacy_plot_adapter
from nautilus_trader.analysis import tearsheet

from _nautilus_bootstrap import LOCAL_ADAPTERS
from _nautilus_bootstrap import install_local_nautilus_overrides


REPO_ROOT = Path(__file__).resolve().parents[1]
OVERLAY_ROOT = REPO_ROOT / "_nautilus_overrides"


def test_nautilus_runtime_uses_upstream_package_with_local_overrides() -> None:
    nautilus_file = Path(nautilus_trader.__file__).resolve()
    assert ".venv" in nautilus_file.parts
    assert "site-packages" in nautilus_file.parts
    assert "nautilus_pm" not in nautilus_file.parts

    prediction_market_module = sys.modules[HistoricalReplayAdapter.__module__]
    prediction_market_path = Path(prediction_market_module.__file__).resolve()
    assert OVERLAY_ROOT in prediction_market_path.parents
    assert (
        prediction_market_path.relative_to(OVERLAY_ROOT)
        .as_posix()
        .startswith("nautilus_trader/adapters/prediction_market/")
    )

    pmxt_module = sys.modules[PolymarketPMXTDataLoader.__module__]
    pmxt_path = Path(pmxt_module.__file__).resolve()
    assert OVERLAY_ROOT in pmxt_path.parents
    assert (
        pmxt_path.relative_to(OVERLAY_ROOT).as_posix()
        == "nautilus_trader/adapters/polymarket/pmxt.py"
    )

    legacy_plot_path = Path(legacy_plot_adapter.__file__).resolve()
    assert OVERLAY_ROOT in legacy_plot_path.parents

    execution_module = sys.modules[PolymarketExecutionClient.__module__]
    execution_path = Path(execution_module.__file__).resolve()
    assert OVERLAY_ROOT in execution_path.parents
    assert (
        execution_path.relative_to(OVERLAY_ROOT).as_posix()
        == "nautilus_trader/adapters/polymarket/execution.py"
    )

    analysis_config_path = Path(analysis_config.__file__).resolve()
    assert OVERLAY_ROOT in analysis_config_path.parents
    assert (
        analysis_config_path.relative_to(OVERLAY_ROOT).as_posix()
        == "nautilus_trader/analysis/config.py"
    )

    tearsheet_path = Path(tearsheet.__file__).resolve()
    assert OVERLAY_ROOT in tearsheet_path.parents
    assert (
        tearsheet_path.relative_to(OVERLAY_ROOT).as_posix()
        == "nautilus_trader/analysis/tearsheet.py"
    )


def test_direct_runner_bootstrap_reinstalls_adapter_overrides(
    monkeypatch,
) -> None:
    script_path = REPO_ROOT / "backtests/kalshi_trade_tick_breakout.py"
    overlay_adapters = str(LOCAL_ADAPTERS.resolve())

    while overlay_adapters in nautilus_adapters.__path__:
        nautilus_adapters.__path__.remove(overlay_adapters)

    prior_helper = sys.modules.pop("_script_helpers", None)
    prior_kalshi = sys.modules.pop("nautilus_trader.adapters.kalshi", None)
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    try:
        helper = importlib.import_module("_script_helpers")
        helper.ensure_repo_root(script_path)

        assert list(nautilus_adapters.__path__)[0] == overlay_adapters

        kalshi_module = importlib.import_module("nautilus_trader.adapters.kalshi")
        kalshi_path = Path(kalshi_module.__file__).resolve()
        assert OVERLAY_ROOT in kalshi_path.parents
    finally:
        install_local_nautilus_overrides()
        sys.modules.pop("_script_helpers", None)
        if prior_helper is not None:
            sys.modules["_script_helpers"] = prior_helper
        sys.modules.pop("nautilus_trader.adapters.kalshi", None)
        if prior_kalshi is not None:
            sys.modules["nautilus_trader.adapters.kalshi"] = prior_kalshi
