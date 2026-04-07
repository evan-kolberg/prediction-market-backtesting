from __future__ import annotations

import importlib
import runpy
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BACKTESTS_ROOT = REPO_ROOT / "backtests"


PUBLIC_RUNNER_PATHS = sorted(
    path.relative_to(REPO_ROOT)
    for path in BACKTESTS_ROOT.glob("*.py")
    if path.name not in {"__init__.py", "_script_helpers.py", "sitecustomize.py"}
    and not path.name.startswith("_")
)

PMXT_SINGLE_MARKET_QUOTE_TICK_RUNNERS = sorted(
    path.relative_to(REPO_ROOT)
    for path in BACKTESTS_ROOT.glob("polymarket_quote_tick_pmxt_*.py")
    if "sports_" not in path.name
    and "multi_sim_runner" not in path.name
    and not path.name.endswith("_sims_runner.py")
    and "optimizer" not in path.name
)
PMXT_MULTI_SIM_QUOTE_TICK_RUNNERS = sorted(
    path.relative_to(REPO_ROOT)
    for path in BACKTESTS_ROOT.glob("polymarket_quote_tick_pmxt_*.py")
    if "multi_sim_runner" in path.name or path.name.endswith("_sims_runner.py")
)
PMXT_QUOTE_TICK_OPTIMIZER_RUNNERS = sorted(
    path.relative_to(REPO_ROOT)
    for path in BACKTESTS_ROOT.glob("polymarket_quote_tick_pmxt_*optimizer.py")
)

FIXED_SPORTS_TRADE_TICK_RUNNERS = [
    Path("backtests/polymarket_trade_tick_sports_final_period_momentum.py"),
    Path("backtests/polymarket_trade_tick_sports_late_favorite_limit_hold.py"),
    Path("backtests/polymarket_trade_tick_sports_vwap_reversion.py"),
]

SCRIPT_ENTRYPOINT_PATHS = [
    Path("scripts/pmxt_download_raws.py"),
]

REPO_BOOTSTRAP_HELPERS = {
    Path("backtests/_script_helpers.py"),
    Path("scripts/_script_helpers.py"),
}


@pytest.mark.parametrize(
    "relative_path",
    [
        Path("backtests/kalshi_trade_tick_breakout.py"),
        Path("backtests/polymarket_quote_tick_pmxt_ema_crossover.py"),
        Path("backtests/polymarket_quote_tick_pmxt_ema_optimizer.py"),
        Path("backtests/polymarket_trade_tick_panic_fade.py"),
        Path("backtests/polymarket_trade_tick_vwap_reversion.py"),
    ],
)
def test_direct_script_entrypoints_import_without_repo_root_on_sys_path(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])
    sys.modules.pop("sitecustomize", None)
    __import__("sitecustomize")

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "NAME" in globals_dict
    assert "run" in globals_dict


@pytest.mark.parametrize("relative_path", SCRIPT_ENTRYPOINT_PATHS)
def test_repo_scripts_import_without_repo_root_on_sys_path(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "main" in globals_dict


def test_backtests_tree_keeps_public_runners_flat() -> None:
    top_level_dirs = {
        path.name
        for path in BACKTESTS_ROOT.iterdir()
        if path.is_dir() and path.name != "__pycache__"
    }
    assert top_level_dirs <= {"_shared", "private"}

    unexpected_nested_runners = [
        path.relative_to(BACKTESTS_ROOT)
        for path in BACKTESTS_ROOT.rglob("*.py")
        if len(path.relative_to(BACKTESTS_ROOT).parts) > 1
        and path.relative_to(BACKTESTS_ROOT).parts[0]
        not in {"_shared", "private", "__pycache__"}
    ]
    assert unexpected_nested_runners == []


def test_repo_keeps_script_bootstrap_helpers_only_next_to_entrypoints() -> None:
    helpers = {
        path.relative_to(REPO_ROOT)
        for path in REPO_ROOT.rglob("_script_helpers.py")
        if "nautilus_pm" not in path.parts
    }
    assert helpers == REPO_BOOTSTRAP_HELPERS


@pytest.mark.parametrize("relative_path", PUBLIC_RUNNER_PATHS)
def test_public_runner_modules_expose_metadata_contract(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert isinstance(globals_dict.get("NAME"), str) and globals_dict["NAME"]
    assert (
        isinstance(globals_dict.get("DESCRIPTION"), str) and globals_dict["DESCRIPTION"]
    )
    assert isinstance(globals_dict.get("EMIT_HTML"), bool)
    assert "CHART_OUTPUT_PATH" in globals_dict
    assert isinstance(globals_dict["CHART_OUTPUT_PATH"], str)
    assert globals_dict["CHART_OUTPUT_PATH"]
    if "DATA" in globals_dict:
        data = globals_dict["DATA"]
        assert getattr(data, "platform", None) in {"kalshi", "polymarket"}
        assert getattr(data, "data_type", None) in {"trade_tick", "quote_tick"}
        assert isinstance(getattr(data, "vendor", None), str) and data.vendor
        assert isinstance(getattr(data, "sources", ()), tuple)
    if "EXPERIMENT" in globals_dict:
        experiment = globals_dict["EXPERIMENT"]
        optimization = getattr(experiment, "optimization", None)
        if optimization is not None:
            assert getattr(optimization, "emit_html", None) == globals_dict["EMIT_HTML"]
            assert (
                getattr(optimization, "chart_output_path", object())
                == globals_dict["CHART_OUTPUT_PATH"]
            )
        else:
            assert getattr(experiment, "emit_html", None) == globals_dict["EMIT_HTML"]
            assert (
                getattr(experiment, "chart_output_path", object())
                == globals_dict["CHART_OUTPUT_PATH"]
            )
    if "OPTIMIZATION" in globals_dict:
        optimization = globals_dict["OPTIMIZATION"]
        assert getattr(optimization, "emit_html", None) == globals_dict["EMIT_HTML"]
        assert (
            getattr(optimization, "chart_output_path", object())
            == globals_dict["CHART_OUTPUT_PATH"]
        )
    assert callable(globals_dict.get("run"))


@pytest.mark.parametrize(
    "module_name",
    [
        "backtests.kalshi_trade_tick_breakout",
        "scripts.pmxt_download_raws",
    ],
)
def test_entrypoint_modules_import_as_packages_without_root_helper_shim(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
) -> None:
    normalized_sys_path = [
        entry
        for entry in sys.path
        if Path(entry or ".").resolve() not in {REPO_ROOT, BACKTESTS_ROOT}
    ]
    monkeypatch.setattr(sys, "path", [str(REPO_ROOT), *normalized_sys_path])

    prior_helper_module = sys.modules.get("_script_helpers")
    prior_module = sys.modules.get(module_name)
    try:
        sys.modules.pop("_script_helpers", None)
        sys.modules.pop(module_name, None)
        module = importlib.import_module(module_name)
        assert module is not None
    finally:
        sys.modules.pop(module_name, None)
        if prior_module is not None:
            sys.modules[module_name] = prior_module
        if prior_helper_module is None:
            sys.modules.pop("_script_helpers", None)
        else:
            sys.modules["_script_helpers"] = prior_helper_module


@pytest.mark.parametrize("relative_path", PMXT_SINGLE_MARKET_QUOTE_TICK_RUNNERS)
def test_pmxt_single_market_quote_tick_runners_expose_explicit_experiment_constants(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "MARKET_SLUG" not in globals_dict
    assert "TOKEN_INDEX" not in globals_dict
    assert "START_TIME" not in globals_dict
    assert "END_TIME" not in globals_dict
    assert "MIN_QUOTES" not in globals_dict
    assert "MIN_PRICE_RANGE" not in globals_dict
    assert "INITIAL_CASH" not in globals_dict

    data = globals_dict["DATA"]
    replays = globals_dict["REPLAYS"]
    experiment = globals_dict["EXPERIMENT"]

    assert data.platform == "polymarket"
    assert data.data_type == "quote_tick"
    assert data.vendor == "pmxt"
    assert len(replays) == 1
    assert replays[0].market_slug
    assert replays[0].start_time
    assert replays[0].end_time
    assert globals_dict["EMIT_HTML"] is True
    assert globals_dict["CHART_OUTPUT_PATH"] == "output"
    assert experiment.initial_cash == 100.0
    assert experiment.min_quotes == 500
    assert experiment.min_price_range == 0.005
    assert experiment.emit_html is True
    assert experiment.chart_output_path == "output"


@pytest.mark.parametrize("relative_path", PMXT_MULTI_SIM_QUOTE_TICK_RUNNERS)
def test_pmxt_multi_sim_quote_tick_runners_expose_explicit_summary_contract(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    data = globals_dict["DATA"]
    replays = globals_dict["REPLAYS"]
    report = globals_dict["REPORT"]
    experiment = globals_dict["EXPERIMENT"]

    assert globals_dict["NAME"] == relative_path.stem
    assert data.platform == "polymarket"
    assert data.data_type == "quote_tick"
    assert data.vendor == "pmxt"
    assert len(replays) > 1
    assert globals_dict["EMIT_HTML"] is True
    assert globals_dict["CHART_OUTPUT_PATH"] == "output"
    assert isinstance(globals_dict["SUMMARY_PLOT_PANELS"], tuple)
    assert globals_dict["SUMMARY_PLOT_PANELS"]
    assert report.summary_report is True
    assert report.summary_report_path == globals_dict["SUMMARY_REPORT_PATH"]
    assert report.summary_plot_panels == globals_dict["SUMMARY_PLOT_PANELS"]
    assert experiment.return_summary_series is True
    assert experiment.emit_html is True
    assert experiment.chart_output_path == "output"
    assert experiment.detail_plot_panels == globals_dict["DETAIL_PLOT_PANELS"]


@pytest.mark.parametrize("relative_path", PMXT_QUOTE_TICK_OPTIMIZER_RUNNERS)
def test_pmxt_quote_tick_optimizer_runners_expose_explicit_search_configuration(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "MARKET_SLUG" not in globals_dict
    assert "TOKEN_INDEX" not in globals_dict
    assert "START_TIME" not in globals_dict
    assert "END_TIME" not in globals_dict

    data = globals_dict["DATA"]
    base_replay = globals_dict["BASE_REPLAY"]
    train_windows = globals_dict["TRAIN_WINDOWS"]
    holdout_windows = globals_dict["HOLDOUT_WINDOWS"]
    parameter_grid = globals_dict["PARAMETER_GRID"]
    optimization = globals_dict["OPTIMIZATION"]

    assert data.platform == "polymarket"
    assert data.data_type == "quote_tick"
    assert data.vendor == "pmxt"
    assert base_replay.market_slug
    assert base_replay.token_index == 0
    assert len(train_windows) == 3
    assert len(holdout_windows) == 1
    assert set(parameter_grid) == {
        "fast_period",
        "slow_period",
        "entry_buffer",
        "take_profit",
        "stop_loss",
    }
    assert optimization.data is data
    assert optimization.base_replay is base_replay
    assert optimization.strategy_spec is globals_dict["STRATEGY_SPEC"]
    assert globals_dict["EMIT_HTML"] is False
    assert globals_dict["CHART_OUTPUT_PATH"] == "output"
    assert optimization.emit_html is False
    assert optimization.chart_output_path == "output"
    assert dict(optimization.parameter_grid) == parameter_grid
    assert optimization.train_windows == train_windows
    assert optimization.holdout_windows == holdout_windows
    assert optimization.execution is globals_dict["EXECUTION"]
    assert optimization.initial_cash == 100.0
    assert optimization.min_quotes == 500
    assert optimization.min_price_range == 0.005


@pytest.mark.parametrize("relative_path", FIXED_SPORTS_TRADE_TICK_RUNNERS)
def test_fixed_sports_trade_tick_runners_pin_historical_close_windows(
    monkeypatch: pytest.MonkeyPatch,
    relative_path: Path,
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [
        entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT
    ]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    replays = globals_dict["REPLAYS"]
    experiment = globals_dict["EXPERIMENT"]
    report = globals_dict["REPORT"]
    pd = pytest.importorskip("pandas")

    assert globals_dict["CHART_OUTPUT_PATH"] == "output"
    assert experiment.default_lookback_days is None
    assert experiment.min_price_range == 0.01
    assert experiment.chart_output_path == "output"
    assert experiment.return_summary_series is True
    assert report.summary_report is True
    assert (
        report.summary_report_path == f"output/{globals_dict['NAME']}_multi_market.html"
    )
    assert len(replays) >= 2
    for replay in replays:
        assert replay.market_slug
        assert replay.lookback_days == 7
        assert isinstance(replay.end_time, str) and replay.end_time
        close_ns = replay.metadata["market_close_time_ns"]
        assert pd.Timestamp(replay.end_time).value == close_ns
