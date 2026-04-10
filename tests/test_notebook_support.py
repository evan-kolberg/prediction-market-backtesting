from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from types import ModuleType

from prediction_market_extensions.backtesting import _notebook_support as notebook_support
from prediction_market_extensions.backtesting._timing_harness import ENABLE_TIMING_ENV


def test_ensure_notebook_repo_context_finds_repo_root_and_bootstraps_path(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    nested = repo_root / "backtests" / "nested"
    nested.mkdir(parents=True)
    (repo_root / "prediction_market_extensions").mkdir()

    monkeypatch.chdir(nested)
    monkeypatch.setattr(sys, "path", list(sys.path))
    commission_calls: list[str] = []
    monkeypatch.setattr(
        notebook_support.importlib,
        "import_module",
        lambda name: SimpleNamespace(install_commission_patch=lambda: commission_calls.append(name)),
    )

    resolved = notebook_support.ensure_notebook_repo_context()

    assert resolved == repo_root
    assert str(repo_root) in sys.path
    assert commission_calls == ["prediction_market_extensions"]
    # Must NOT chdir — parallel notebooks share the process working directory
    assert Path.cwd() == nested


def test_load_optimizer_handle_prefers_optimizer_attribute(monkeypatch) -> None:
    module = SimpleNamespace(OPTIMIZER="preferred", PARAMETER_SEARCH="secondary", OPTIMIZATION="legacy")
    monkeypatch.setattr(
        notebook_support.importlib, "import_module", lambda name: module if name == "demo.module" else None
    )

    loaded_module, optimizer_config = notebook_support.load_optimizer_handle("demo.module")

    assert loaded_module is module
    assert optimizer_config == "preferred"


def test_suppress_notebook_cell_output_redirects_stdout_and_stderr(capsys) -> None:
    with notebook_support.suppress_notebook_cell_output():
        print("hidden stdout")
        print("hidden stderr", file=sys.stderr)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_suppress_notebook_cell_output_uses_ipython_capture_when_available(monkeypatch) -> None:
    events: list[str] = []

    class _FakeCapture:
        def __enter__(self):
            events.append("enter")
            return self

        def __exit__(self, exc_type, exc, tb):
            events.append("exit")
            return False

    def _capture_output(*, stdout: bool, stderr: bool, display: bool):
        events.append(f"capture:{stdout}:{stderr}:{display}")
        return _FakeCapture()

    ipython_module = ModuleType("IPython")
    ipython_utils_module = ModuleType("IPython.utils")
    ipython_capture_module = ModuleType("IPython.utils.capture")
    ipython_capture_module.capture_output = _capture_output

    monkeypatch.setitem(sys.modules, "IPython", ipython_module)
    monkeypatch.setitem(sys.modules, "IPython.utils", ipython_utils_module)
    monkeypatch.setitem(sys.modules, "IPython.utils.capture", ipython_capture_module)

    with notebook_support.suppress_notebook_cell_output():
        pass

    assert events == ["capture:True:True:False", "enter", "exit"]


def test_suppress_notebook_cell_output_disables_timing_temporarily(monkeypatch) -> None:
    monkeypatch.delenv(ENABLE_TIMING_ENV, raising=False)

    with notebook_support.suppress_notebook_cell_output():
        assert os.environ[ENABLE_TIMING_ENV] == "0"

    assert ENABLE_TIMING_ENV not in os.environ


@dataclass(frozen=True)
class _DummyOptimizerConfig:
    name: str = "demo"
    max_trials: int = 4
    holdout_top_k: int = 2


def test_build_research_parameter_search_clamps_limits() -> None:
    config = _DummyOptimizerConfig()

    updated = notebook_support.build_research_parameter_search(config, max_trials=10, holdout_top_k=5)

    assert updated.name == "demo_research"
    assert updated.max_trials == 4
    assert updated.holdout_top_k == 2


def test_display_html_artifacts_prefers_summary_report_and_lists_extras(monkeypatch, tmp_path: Path) -> None:
    summary = tmp_path / "demo_joint_portfolio.html"
    detail = tmp_path / "demo_detail_legacy.html"
    summary.write_text("<html><body>summary</body></html>", encoding="utf-8")
    detail.write_text("<html><body>detail</body></html>", encoding="utf-8")

    displayed: list[tuple[str, str]] = []
    fake_display_module = SimpleNamespace(
        HTML=lambda value: ("HTML", value),
        Markdown=lambda value: ("Markdown", value),
        display=lambda value: displayed.append(value),
    )
    monkeypatch.setitem(sys.modules, "IPython", SimpleNamespace(display=fake_display_module))
    monkeypatch.setitem(sys.modules, "IPython.display", fake_display_module)

    notebook_support.display_html_artifacts([detail.resolve(), summary.resolve()], repo_root=tmp_path)

    assert displayed[0][0] == "Markdown"
    assert "demo_joint_portfolio.html" in displayed[0][1]
    assert displayed[1] == ("HTML", "<html><body>summary</body></html>")
    assert displayed[2][0] == "Markdown"
    assert "demo_detail_legacy.html" in displayed[2][1]
