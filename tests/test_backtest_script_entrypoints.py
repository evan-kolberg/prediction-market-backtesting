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
    for path in (*BACKTESTS_ROOT.glob("*.py"), *BACKTESTS_ROOT.glob("*.ipynb"))
    if path.name not in {"__init__.py", "_script_helpers.py", "sitecustomize.py"}
    and not path.name.startswith("_")
)

PUBLIC_SCRIPT_RUNNER_PATHS = sorted(
    path.relative_to(REPO_ROOT)
    for path in BACKTESTS_ROOT.glob("*.py")
    if path.name not in {"__init__.py", "_script_helpers.py", "sitecustomize.py"}
    and not path.name.startswith("_")
)

EXPECTED_PUBLIC_RUNNER_PATHS = [
    Path("backtests/generic_optimizer_research.ipynb"),
    Path("backtests/generic_tpe_research.ipynb"),
    Path("backtests/kalshi_trade_tick_breakout.py"),
    Path("backtests/kalshi_trade_tick_independent_multi_replay_runner.py"),
    Path("backtests/kalshi_trade_tick_joint_portfolio_runner.py"),
    Path("backtests/pmxt_quote_tick_joint_portfolio_runner.ipynb"),
    Path("backtests/polymarket_quote_tick_ema_crossover.py"),
    Path("backtests/polymarket_quote_tick_ema_optimizer.py"),
    Path("backtests/polymarket_quote_tick_independent_25_replay_runner.py"),
    Path("backtests/polymarket_quote_tick_independent_multi_replay_runner.py"),
    Path("backtests/polymarket_quote_tick_joint_portfolio_runner.py"),
    Path("backtests/polymarket_telonex_quote_tick_joint_portfolio_runner.py"),
    Path("backtests/polymarket_trade_tick_independent_multi_replay_runner.py"),
    Path("backtests/polymarket_trade_tick_joint_portfolio_runner.py"),
    Path("backtests/polymarket_trade_tick_vwap_reversion.py"),
    Path("backtests/telonex_quote_tick_joint_portfolio_runner.ipynb"),
]

PMXT_SINGLE_MARKET_QUOTE_TICK_RUNNERS = [Path("backtests/polymarket_quote_tick_ema_crossover.py")]
PMXT_INDEPENDENT_QUOTE_TICK_RUNNERS = [
    Path("backtests/polymarket_quote_tick_independent_multi_replay_runner.py"),
    Path("backtests/polymarket_quote_tick_independent_25_replay_runner.py"),
]
PMXT_JOINT_QUOTE_TICK_RUNNERS = [Path("backtests/polymarket_quote_tick_joint_portfolio_runner.py")]
TELONEX_JOINT_QUOTE_TICK_RUNNERS = [
    Path("backtests/polymarket_telonex_quote_tick_joint_portfolio_runner.py")
]
PMXT_QUOTE_TICK_OPTIMIZER_RUNNERS = [Path("backtests/polymarket_quote_tick_ema_optimizer.py")]
TRADE_TICK_INDEPENDENT_RUNNERS = [
    Path("backtests/kalshi_trade_tick_independent_multi_replay_runner.py"),
    Path("backtests/polymarket_trade_tick_independent_multi_replay_runner.py"),
]
TRADE_TICK_JOINT_RUNNERS = [
    Path("backtests/kalshi_trade_tick_joint_portfolio_runner.py"),
    Path("backtests/polymarket_trade_tick_joint_portfolio_runner.py"),
]

SCRIPT_ENTRYPOINT_PATHS = [
    Path("scripts/pmxt_download_raws.py"),
    Path("scripts/run_all_backtests.py"),
    Path("scripts/telonex_download_data.py"),
]

REPO_BOOTSTRAP_HELPERS = {Path("backtests/_script_helpers.py"), Path("scripts/_script_helpers.py")}


PUBLIC_NOTEBOOK_RUNNER_PATHS = [
    path for path in EXPECTED_PUBLIC_RUNNER_PATHS if path.suffix == ".ipynb"
]

EXPECTED_PUBLIC_SCRIPT_RUNNER_PATHS = [
    path for path in EXPECTED_PUBLIC_RUNNER_PATHS if path.suffix == ".py"
]


@pytest.mark.parametrize("relative_path", EXPECTED_PUBLIC_SCRIPT_RUNNER_PATHS)
def test_direct_script_entrypoints_import_without_repo_root_on_sys_path(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])
    sys.modules.pop("sitecustomize", None)
    __import__("sitecustomize")

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "EXPERIMENT" in globals_dict
    assert "run" in globals_dict


@pytest.mark.parametrize("relative_path", SCRIPT_ENTRYPOINT_PATHS)
def test_repo_scripts_import_without_repo_root_on_sys_path(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "main" in globals_dict


def test_backtests_tree_keeps_public_runners_flat() -> None:
    top_level_dirs = {
        path.name
        for path in BACKTESTS_ROOT.iterdir()
        if path.is_dir() and path.name != "__pycache__"
    }
    assert top_level_dirs <= {"private"}

    unexpected_nested_runners = [
        path.relative_to(BACKTESTS_ROOT)
        for path in (*BACKTESTS_ROOT.rglob("*.py"), *BACKTESTS_ROOT.rglob("*.ipynb"))
        if len(path.relative_to(BACKTESTS_ROOT).parts) > 1
        and path.relative_to(BACKTESTS_ROOT).parts[0] not in {"private", "__pycache__"}
    ]
    assert unexpected_nested_runners == []


def test_public_runner_set_matches_curated_examples() -> None:
    assert PUBLIC_RUNNER_PATHS == EXPECTED_PUBLIC_RUNNER_PATHS


def test_public_script_runner_set_matches_curated_examples() -> None:
    assert PUBLIC_SCRIPT_RUNNER_PATHS == EXPECTED_PUBLIC_SCRIPT_RUNNER_PATHS


def test_repo_keeps_script_bootstrap_helpers_only_next_to_entrypoints() -> None:
    helpers = {path.relative_to(REPO_ROOT) for path in REPO_ROOT.rglob("_script_helpers.py")}
    assert helpers == REPO_BOOTSTRAP_HELPERS


@pytest.mark.parametrize("relative_path", PUBLIC_SCRIPT_RUNNER_PATHS)
def test_public_runner_modules_expose_metadata_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    if "DATA" in globals_dict:
        data = globals_dict["DATA"]
        assert getattr(data, "platform", None) in {"kalshi", "polymarket"}
        assert getattr(data, "data_type", None) in {"trade_tick", "quote_tick"}
        assert isinstance(getattr(data, "vendor", None), str) and data.vendor
        assert isinstance(getattr(data, "sources", ()), tuple)
    if "EXPERIMENT" in globals_dict:
        experiment = globals_dict["EXPERIMENT"]
        assert isinstance(getattr(experiment, "name", None), str) and experiment.name
        assert isinstance(getattr(experiment, "description", None), str) and experiment.description
        optimization = getattr(experiment, "optimization", None)
        target = optimization if optimization is not None else experiment
        assert isinstance(getattr(target, "emit_html", None), bool)
        assert isinstance(getattr(target, "chart_output_path", None), str)
        assert target.chart_output_path
    parameter_search = globals_dict.get(
        "PARAMETER_SEARCH", globals_dict.get("OPTIMIZER", globals_dict.get("OPTIMIZATION"))
    )
    if parameter_search is not None:
        assert isinstance(getattr(parameter_search, "emit_html", None), bool)
        assert isinstance(getattr(parameter_search, "chart_output_path", None), str)
        assert parameter_search.chart_output_path
    assert callable(globals_dict.get("run"))


@pytest.mark.parametrize("relative_path", EXPECTED_PUBLIC_SCRIPT_RUNNER_PATHS)
def test_public_script_runners_attach_explicit_execution_model(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-telonex-key")
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    execution = globals_dict.get("EXECUTION")
    assert execution is not None
    assert execution.latency_model is not None

    parameter_search = globals_dict.get(
        "PARAMETER_SEARCH", globals_dict.get("OPTIMIZER", globals_dict.get("OPTIMIZATION"))
    )
    experiment = globals_dict["EXPERIMENT"]
    target = parameter_search if parameter_search is not None else experiment
    assert target.execution is execution

    data = globals_dict["DATA"]
    if data.data_type == "quote_tick":
        assert execution.queue_position is True
    else:
        assert execution.queue_position is False


@pytest.mark.parametrize("relative_path", PUBLIC_NOTEBOOK_RUNNER_PATHS)
def test_public_notebook_runners_expose_metadata_contract(relative_path: Path) -> None:
    from prediction_market_extensions.backtesting._notebook_runner import load_notebook_metadata

    metadata = load_notebook_metadata(REPO_ROOT / relative_path, project_root=REPO_ROOT)

    assert metadata is not None
    assert metadata["name"] == relative_path.stem
    assert isinstance(metadata["description"], str) and metadata["description"]
    assert metadata["module_name"] == ".".join(relative_path.with_suffix("").parts)
    assert metadata["relative_parts"] == (relative_path.name,)


@pytest.mark.parametrize(
    "module_name",
    [
        "backtests.kalshi_trade_tick_breakout",
        "scripts.pmxt_download_raws",
        "scripts.telonex_download_data",
    ],
)
def test_entrypoint_modules_import_as_packages_without_root_helper_shim(
    monkeypatch: pytest.MonkeyPatch, module_name: str
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
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

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
    assert experiment.initial_cash == 100.0
    assert experiment.min_quotes == 500
    assert experiment.min_price_range == 0.005
    assert experiment.emit_html is True
    assert experiment.chart_output_path == "output"


@pytest.mark.parametrize("relative_path", PMXT_INDEPENDENT_QUOTE_TICK_RUNNERS)
def test_pmxt_quote_tick_independent_runners_expose_explicit_summary_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    data = globals_dict["DATA"]
    replays = globals_dict["REPLAYS"]
    report = globals_dict["REPORT"]
    experiment = globals_dict["EXPERIMENT"]

    assert experiment.name == relative_path.stem
    assert data.platform == "polymarket"
    assert data.data_type == "quote_tick"
    assert data.vendor == "pmxt"
    assert len(replays) > 1
    assert isinstance(globals_dict["SUMMARY_PLOT_PANELS"], tuple)
    assert globals_dict["SUMMARY_PLOT_PANELS"]
    assert report.summary_report is True
    assert report.summary_report_path == globals_dict["SUMMARY_REPORT_PATH"]
    assert report.summary_plot_panels == globals_dict["SUMMARY_PLOT_PANELS"]
    assert experiment.return_summary_series is True
    assert experiment.multi_replay_mode == "independent"
    assert experiment.emit_html is False
    assert experiment.chart_output_path == "output"
    assert experiment.detail_plot_panels == globals_dict["DETAIL_PLOT_PANELS"]


@pytest.mark.parametrize("relative_path", PMXT_JOINT_QUOTE_TICK_RUNNERS)
def test_pmxt_quote_tick_joint_runners_expose_explicit_summary_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    experiment = globals_dict["EXPERIMENT"]
    report = globals_dict["REPORT"]

    assert experiment.name == relative_path.stem
    assert report.summary_report is True
    assert report.summary_report_path == globals_dict["SUMMARY_REPORT_PATH"]
    assert experiment.return_summary_series is True
    assert experiment.multi_replay_mode == "joint_portfolio"
    assert experiment.emit_html is False
    assert experiment.chart_output_path == "output"


@pytest.mark.parametrize("relative_path", TELONEX_JOINT_QUOTE_TICK_RUNNERS)
def test_telonex_quote_tick_joint_runners_expose_explicit_summary_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-telonex-key")
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    data = globals_dict["DATA"]
    experiment = globals_dict["EXPERIMENT"]
    report = globals_dict["REPORT"]

    assert experiment.name == relative_path.stem
    assert data.platform == "polymarket"
    assert data.data_type == "quote_tick"
    assert data.vendor == "telonex"
    assert data.sources == (
        "local:/Volumes/LaCie/telonex_data",
        "api:test-telonex-key",
    )
    assert report.summary_report is True
    assert report.summary_report_path == globals_dict["SUMMARY_REPORT_PATH"]
    assert experiment.return_summary_series is True
    assert experiment.multi_replay_mode == "joint_portfolio"
    assert experiment.emit_html is False
    assert experiment.chart_output_path == "output"


@pytest.mark.parametrize("relative_path", PMXT_QUOTE_TICK_OPTIMIZER_RUNNERS)
def test_pmxt_quote_tick_optimizer_runners_expose_explicit_search_configuration(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    data = globals_dict["DATA"]
    base_replay = globals_dict["BASE_REPLAY"]
    train_windows = globals_dict["TRAIN_WINDOWS"]
    holdout_windows = globals_dict["HOLDOUT_WINDOWS"]
    parameter_grid = globals_dict["PARAMETER_GRID"]
    optimizer = globals_dict["OPTIMIZER"]
    parameter_search = globals_dict["PARAMETER_SEARCH"]

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
    assert optimizer is parameter_search
    assert globals_dict["OPTIMIZATION"] is parameter_search
    assert parameter_search.optimizer_type == "parameter_search"
    assert parameter_search.data is data
    assert parameter_search.base_replay is base_replay
    assert parameter_search.strategy_spec is globals_dict["STRATEGY_SPEC"]
    assert parameter_search.emit_html is True
    assert parameter_search.chart_output_path == "output"
    assert dict(parameter_search.parameter_grid) == parameter_grid
    assert parameter_search.train_windows == train_windows
    assert parameter_search.holdout_windows == holdout_windows
    assert parameter_search.execution is globals_dict["EXECUTION"]
    assert parameter_search.initial_cash == 100.0
    assert parameter_search.min_quotes == 500
    assert parameter_search.min_price_range == 0.005


@pytest.mark.parametrize("relative_path", TRADE_TICK_INDEPENDENT_RUNNERS)
def test_trade_tick_independent_runners_emit_summary_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    experiment = globals_dict["EXPERIMENT"]
    report = globals_dict["REPORT"]

    assert experiment.emit_html is False
    assert experiment.chart_output_path == "output"
    assert experiment.return_summary_series is True
    assert experiment.multi_replay_mode == "independent"
    assert report.summary_report is True
    assert report.summary_report_path == globals_dict["SUMMARY_REPORT_PATH"]


@pytest.mark.parametrize("relative_path", TRADE_TICK_JOINT_RUNNERS)
def test_trade_tick_joint_runners_emit_summary_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    experiment = globals_dict["EXPERIMENT"]
    report = globals_dict["REPORT"]

    assert experiment.emit_html is False
    assert experiment.chart_output_path == "output"
    assert experiment.return_summary_series is True
    assert experiment.multi_replay_mode == "joint_portfolio"
    assert report.summary_report is True
    assert report.summary_report_path == globals_dict["SUMMARY_REPORT_PATH"]
