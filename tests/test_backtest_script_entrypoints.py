from __future__ import annotations

import importlib
import runpy
import sys
from decimal import Decimal
from pathlib import Path

import pytest

import main as main_module

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
    Path("backtests/pmxt_book_joint_portfolio_runner.ipynb"),
    Path("backtests/polymarket_beffer45_trade_replay_telonex.ipynb"),
    Path("backtests/polymarket_beffer45_trade_replay_telonex.py"),
    Path("backtests/polymarket_book_ema_crossover.py"),
    Path("backtests/polymarket_book_ema_optimizer.py"),
    Path("backtests/polymarket_book_joint_portfolio_runner.py"),
    Path("backtests/polymarket_btc_5m_late_favorite_taker_hold.py"),
    Path("backtests/polymarket_btc_5m_pair_arbitrage.py"),
    Path("backtests/polymarket_telonex_book_100_replay_joint_portfolio_runner.py"),
    Path("backtests/polymarket_telonex_book_joint_portfolio_runner.py"),
    Path("backtests/telonex_book_joint_portfolio_runner.ipynb"),
]

PMXT_SINGLE_MARKET_BOOK_RUNNERS = [Path("backtests/polymarket_book_ema_crossover.py")]
PMXT_JOINT_BOOK_RUNNERS = [Path("backtests/polymarket_book_joint_portfolio_runner.py")]
TELONEX_SMALL_JOINT_BOOK_RUNNER = Path(
    "backtests/polymarket_telonex_book_joint_portfolio_runner.py"
)
TELONEX_100_JOINT_BOOK_RUNNER = Path(
    "backtests/polymarket_telonex_book_100_replay_joint_portfolio_runner.py"
)
TELONEX_JOINT_BOOK_RUNNERS = [TELONEX_100_JOINT_BOOK_RUNNER, TELONEX_SMALL_JOINT_BOOK_RUNNER]
TELONEX_ACCOUNT_REPLAY_RUNNERS = [Path("backtests/polymarket_beffer45_trade_replay_telonex.py")]
PMXT_BOOK_OPTIMIZER_RUNNERS = [Path("backtests/polymarket_book_ema_optimizer.py")]

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


def _load_script_runner(monkeypatch: pytest.MonkeyPatch, relative_path: Path) -> dict[str, object]:
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])
    return runpy.run_path(str(script_path), run_name="__script_test__")


def _capture_script_experiment(monkeypatch: pytest.MonkeyPatch, relative_path: Path):
    from prediction_market_extensions.backtesting import _experiments

    captured: dict[str, object] = {}

    def capture_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment

    monkeypatch.setattr(_experiments, "run_experiment", capture_run_experiment)
    globals_dict = _load_script_runner(monkeypatch, relative_path)
    globals_dict["run"]()
    return captured["experiment"]


@pytest.mark.parametrize("relative_path", EXPECTED_PUBLIC_SCRIPT_RUNNER_PATHS)
def test_direct_script_entrypoints_import_without_repo_root_on_sys_path(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-telonex-key")
    script_path = REPO_ROOT / relative_path
    normalized_sys_path = [entry for entry in sys.path if Path(entry or ".").resolve() != REPO_ROOT]
    monkeypatch.setattr(sys, "path", [str(script_path.parent), *normalized_sys_path])
    sys.modules.pop("sitecustomize", None)
    __import__("sitecustomize")

    globals_dict = runpy.run_path(str(script_path), run_name="__script_test__")

    assert "run" in globals_dict
    assert "EXPERIMENT" not in globals_dict
    assert "DATA" not in globals_dict
    assert "REPLAYS" not in globals_dict


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
    helpers = {
        path.relative_to(REPO_ROOT)
        for path in REPO_ROOT.rglob("_script_helpers.py")
        if ".claude" not in path.parts
    }
    assert helpers == REPO_BOOTSTRAP_HELPERS


@pytest.mark.parametrize("relative_path", PUBLIC_SCRIPT_RUNNER_PATHS)
def test_public_runner_modules_expose_metadata_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-telonex-key")
    metadata = main_module._load_runner_metadata(REPO_ROOT / relative_path)

    assert metadata is not None
    assert metadata["name"] == relative_path.stem
    assert isinstance(metadata["description"], str) and metadata["description"]
    assert metadata["module_name"] == ".".join(relative_path.with_suffix("").parts)
    assert metadata["relative_parts"] == (relative_path.name,)


@pytest.mark.parametrize("relative_path", EXPECTED_PUBLIC_SCRIPT_RUNNER_PATHS)
def test_public_script_runners_attach_explicit_execution_model(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-telonex-key")
    experiment = _capture_script_experiment(monkeypatch, relative_path)
    target = getattr(experiment, "parameter_search", experiment)

    assert target.execution is not None
    assert target.execution.latency_model is not None


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


@pytest.mark.parametrize("relative_path", PMXT_SINGLE_MARKET_BOOK_RUNNERS)
def test_pmxt_single_market_book_runners_build_inline_experiment(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    experiment = _capture_script_experiment(monkeypatch, relative_path)

    assert experiment.data.platform == "polymarket"
    assert experiment.data.data_type == "book"
    assert experiment.data.vendor == "pmxt"
    assert len(experiment.replays) == 1
    assert experiment.replays[0].market_slug
    assert experiment.replays[0].start_time
    assert experiment.replays[0].end_time
    assert experiment.initial_cash == 100.0
    assert experiment.min_price_range == 0.005


@pytest.mark.parametrize("relative_path", PMXT_JOINT_BOOK_RUNNERS)
def test_pmxt_book_joint_runners_build_inline_summary_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    experiment = _capture_script_experiment(monkeypatch, relative_path)

    assert experiment.name == relative_path.stem
    assert experiment.report.summary_report is True
    assert (
        experiment.report.summary_report_path
        == "output/polymarket_book_joint_portfolio_runner_joint_portfolio.html"
    )
    assert experiment.strategy_configs[0]["strategy_path"] == (
        "strategies:BookMicropriceImbalanceStrategy"
    )
    assert experiment.strategy_configs[0]["config_path"] == (
        "strategies:BookMicropriceImbalanceConfig"
    )
    assert experiment.strategy_configs[0]["config"]["entry_imbalance"] == 0.62
    assert experiment.strategy_configs[0]["config"]["max_entry_price"] == 0.20
    assert "yes_price" in experiment.report.summary_plot_panels
    assert "allocation" in experiment.report.summary_plot_panels
    assert experiment.return_summary_series is True


def test_btc_5m_pair_arbitrage_runner_builds_pmxt_book_pairs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment = _capture_script_experiment(
        monkeypatch, Path("backtests/polymarket_btc_5m_pair_arbitrage.py")
    )

    assert experiment.name == "polymarket_btc_5m_pair_arbitrage"
    assert experiment.data.platform == "polymarket"
    assert experiment.data.data_type == "book"
    assert experiment.data.vendor == "pmxt"
    assert experiment.data.sources == (
        "local:/Volumes/LaCie/pmxt_data",
        "archive:r2v2.pmxt.dev",
        "archive:r2.pmxt.dev",
    )
    assert len(experiment.replays) == 8
    assert experiment.replays[0].market_slug == "btc-updown-5m-1777226400"
    assert experiment.replays[0].token_index == 0
    assert experiment.replays[1].market_slug == "btc-updown-5m-1777226400"
    assert experiment.replays[1].token_index == 1
    assert experiment.replays[-1].market_slug == "btc-updown-5m-1777227300"
    assert experiment.replays[-1].token_index == 1
    assert all(replay.market_slug.startswith("btc-updown-5m-") for replay in experiment.replays)
    assert experiment.strategy_configs[0]["config"]["trade_size"] == Decimal("5")
    assert experiment.strategy_configs[0]["config"]["min_net_edge"] == 0.0
    assert experiment.strategy_configs[0]["config"]["max_total_cost"] == 1.0
    assert experiment.strategy_configs[0]["config"]["include_taker_fees_in_signal"] is False
    assert experiment.report.market_key == "sim_label"
    assert experiment.report.summary_report is True
    assert (
        experiment.report.summary_report_path
        == "output/polymarket_btc_5m_pair_arbitrage_summary.html"
    )
    assert experiment.return_summary_series is True


def test_btc_5m_late_favorite_taker_runner_builds_pmxt_book_replays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    experiment = _capture_script_experiment(
        monkeypatch, Path("backtests/polymarket_btc_5m_late_favorite_taker_hold.py")
    )

    assert experiment.name == "polymarket_btc_5m_late_favorite_taker_hold"
    assert experiment.data.platform == "polymarket"
    assert experiment.data.data_type == "book"
    assert experiment.data.vendor == "pmxt"
    assert experiment.data.sources == (
        "local:/Volumes/LaCie/pmxt_data",
        "archive:r2v2.pmxt.dev",
        "archive:r2.pmxt.dev",
    )
    assert len(experiment.replays) == 48
    assert experiment.replays[0].market_slug == "btc-updown-5m-1777226400"
    assert experiment.replays[0].token_index == 0
    assert experiment.replays[0].metadata["activation_start_time_ns"] == 1777226640000000000
    assert experiment.replays[0].metadata["market_close_time_ns"] == 1777226700000000000
    assert experiment.replays[-1].market_slug == "btc-updown-5m-1777233300"
    assert experiment.replays[-1].token_index == 1
    assert experiment.strategy_configs[0]["strategy_path"] == (
        "strategies:BookLateFavoriteTakerHoldStrategy"
    )
    assert experiment.strategy_configs[0]["config_path"] == (
        "strategies:BookLateFavoriteTakerHoldConfig"
    )
    assert experiment.strategy_configs[0]["config"]["trade_size"] == Decimal("5")
    assert experiment.strategy_configs[0]["config"]["min_midpoint"] == 0.90
    assert experiment.strategy_configs[0]["config"]["min_bid_price"] == 0.88
    assert experiment.strategy_configs[0]["config"]["max_entry_price"] == 0.95
    assert experiment.strategy_configs[0]["config"]["max_spread"] == 0.04
    assert experiment.strategy_configs[0]["config"]["min_visible_size"] == 5.0
    assert experiment.strategy_configs[0]["config"]["enable_cheap_no_entry"] is True
    assert experiment.strategy_configs[0]["config"]["max_cheap_no_entry_price"] == 0.05
    assert experiment.strategy_configs[0]["config"]["max_cheap_no_midpoint"] == 0.10
    assert experiment.strategy_configs[0]["config"]["max_cheap_no_spread"] == 0.05
    assert (
        experiment.strategy_configs[0]["config"]["activation_start_time_ns"]
        == "__SIM_METADATA__:activation_start_time_ns"
    )
    assert experiment.report.market_key == "sim_label"
    assert experiment.report.summary_report is True
    assert (
        experiment.report.summary_report_path
        == "output/polymarket_btc_5m_late_favorite_taker_hold_summary.html"
    )
    assert experiment.return_summary_series is True


@pytest.mark.parametrize("relative_path", TELONEX_JOINT_BOOK_RUNNERS)
def test_telonex_book_joint_runners_build_inline_summary_contract(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-telonex-key")
    experiment = _capture_script_experiment(monkeypatch, relative_path)

    assert experiment.name == relative_path.stem
    assert experiment.data.platform == "polymarket"
    assert experiment.data.data_type == "book"
    assert experiment.data.vendor == "telonex"
    assert experiment.data.sources[-1] == "api:${TELONEX_API_KEY}"
    assert all("btc-updown-5m" not in replay.market_slug for replay in experiment.replays)
    assert len({replay.market_slug for replay in experiment.replays}) == len(experiment.replays)
    assert all(replay.token_index == 0 for replay in experiment.replays)
    assert all("result" not in replay.metadata for replay in experiment.replays)
    assert "test-telonex-key" not in repr(experiment)
    assert experiment.report.summary_report is True
    assert experiment.strategy_configs[0]["strategy_path"] == (
        "strategies:BookMicropriceImbalanceStrategy"
    )
    assert experiment.strategy_configs[0]["config_path"] == (
        "strategies:BookMicropriceImbalanceConfig"
    )
    assert experiment.strategy_configs[0]["config"]["entry_imbalance"] == 0.62
    assert experiment.strategy_configs[0]["config"]["max_entry_price"] == 0.95
    assert "yes_price" in experiment.report.summary_plot_panels
    assert "allocation" in experiment.report.summary_plot_panels
    assert experiment.return_summary_series is True

    if relative_path == TELONEX_SMALL_JOINT_BOOK_RUNNER:
        assert experiment.data.sources == (
            "local:/Users/evankolberg/Downloads/temp",
            "api:${TELONEX_API_KEY}",
        )
        assert len(experiment.replays) == 10
        assert experiment.replays[0].market_slug == "will-the-iranian-regime-fall-by-may-31"
        assert experiment.replays[-1].market_slug == "will-china-invade-taiwan-before-2027"
        assert all(replay.start_time == "2026-04-28T00:00:00Z" for replay in experiment.replays)
        assert all(replay.end_time == "2026-04-30T23:59:59Z" for replay in experiment.replays)
        assert all(
            replay.metadata["market_close_time_ns"] == 1777593599000000000
            for replay in experiment.replays
        )
        assert (
            experiment.report.summary_report_path
            == "output/polymarket_telonex_book_joint_portfolio_runner_joint_portfolio.html"
        )
    elif relative_path == TELONEX_100_JOINT_BOOK_RUNNER:
        assert experiment.data.sources == ("api:${TELONEX_API_KEY}",)
        assert len(experiment.replays) == 100
        assert experiment.initial_cash == 1_000.0
        assert experiment.replays[0].market_slug == "will-jesus-christ-return-before-2027"
        assert experiment.replays[-1].market_slug == (
            "will-gavin-newsom-win-the-2028-us-presidential-election"
        )
        assert all(replay.start_time == "2026-04-21T00:00:00Z" for replay in experiment.replays)
        assert all(
            replay.end_time == "2026-04-27T23:59:59.999999999Z" for replay in experiment.replays
        )
        assert all(
            replay.metadata["replay_window_start_ns"] == 1776729600000000000
            for replay in experiment.replays
        )
        assert all(
            replay.metadata["replay_window_end_ns"] == 1777334399999999999
            for replay in experiment.replays
        )
        assert experiment.report.summary_report_path == (
            "output/polymarket_telonex_book_100_replay_joint_portfolio_runner_joint_portfolio.html"
        )
    else:
        raise AssertionError(f"Unhandled Telonex joint book runner: {relative_path}")


@pytest.mark.parametrize("relative_path", TELONEX_JOINT_BOOK_RUNNERS)
def test_telonex_book_joint_runners_do_not_embed_empty_api_key(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "")
    experiment = _capture_script_experiment(monkeypatch, relative_path)

    assert experiment.data.sources[-1] == "api:${TELONEX_API_KEY}"
    assert "api:" in experiment.data.sources[-1]
    assert "api:," not in repr(experiment)


@pytest.mark.parametrize("relative_path", TELONEX_ACCOUNT_REPLAY_RUNNERS)
def test_telonex_account_replay_runner_builds_hard_coded_trade_replay(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    monkeypatch.setenv("TELONEX_API_KEY", "test-telonex-key")
    experiment = _capture_script_experiment(monkeypatch, relative_path)

    assert experiment.name == "polymarket_beffer45_trade_replay_telonex"
    assert experiment.data.platform == "polymarket"
    assert experiment.data.data_type == "book"
    assert experiment.data.vendor == "telonex"
    assert experiment.data.sources == ("api:${TELONEX_API_KEY}",)
    assert len(experiment.replays) == 86
    assert experiment.initial_cash == 1_000.0
    assert experiment.min_book_events == 1
    assert experiment.strategy_configs[0]["strategy_path"] == (
        "strategies:BookAccountTradeReplayStrategy"
    )
    assert experiment.strategy_configs[0]["config_path"] == (
        "strategies:BookAccountTradeReplayConfig"
    )
    assert experiment.strategy_configs[0]["config"]["trades"] == ("__SIM_METADATA__:ledger_trades")
    assert experiment.report.market_key == "sim_label"
    assert experiment.report.summary_report is True
    assert (
        experiment.report.summary_report_path
        == "output/polymarket_beffer45_trade_replay_telonex_summary.html"
    )
    assert experiment.return_summary_series is True


@pytest.mark.parametrize("relative_path", PMXT_BOOK_OPTIMIZER_RUNNERS)
def test_pmxt_book_optimizer_runners_build_inline_search_configuration(
    monkeypatch: pytest.MonkeyPatch, relative_path: Path
) -> None:
    experiment = _capture_script_experiment(monkeypatch, relative_path)
    parameter_search = experiment.parameter_search

    assert parameter_search.data.platform == "polymarket"
    assert parameter_search.data.data_type == "book"
    assert parameter_search.data.vendor == "pmxt"
    assert parameter_search.base_replay.market_slug
    assert parameter_search.base_replay.token_index == 0
    assert len(parameter_search.train_windows) == 3
    assert len(parameter_search.holdout_windows) == 1
    assert set(parameter_search.parameter_grid) == {
        "fast_period",
        "slow_period",
        "entry_buffer",
        "take_profit",
        "stop_loss",
    }
    assert parameter_search.optimizer_type == "parameter_search"
    assert parameter_search.initial_cash == 100.0
    assert parameter_search.min_price_range == 0.005
