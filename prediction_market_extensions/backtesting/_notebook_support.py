from __future__ import annotations

import importlib
import os
import sys
from collections.abc import Mapping, Sequence
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from typing import Any

from prediction_market_extensions.backtesting._timing_harness import ENABLE_TIMING_ENV


def find_repo_root(start_path: str | Path | None = None) -> Path:
    start = Path.cwd() if start_path is None else Path(start_path)
    path = start.resolve()
    for candidate in (path, *path.parents):
        if (candidate / "prediction_market_extensions").is_dir() and (
            candidate / "backtests"
        ).is_dir():
            return candidate
    raise RuntimeError("Could not locate repository root for notebook execution.")


def ensure_notebook_repo_context(start_path: str | Path | None = None) -> Path:
    repo_root = find_repo_root(start_path)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    install_commission_patch = importlib.import_module(
        "prediction_market_extensions"
    ).install_commission_patch
    install_commission_patch()
    return repo_root


@contextmanager
def suppress_notebook_cell_output():
    capture_output = None
    try:
        from IPython.utils.capture import capture_output
    except Exception:
        capture_output = None

    previous_timing_env = os.environ.get(ENABLE_TIMING_ENV)
    os.environ[ENABLE_TIMING_ENV] = "0"

    with ExitStack() as stack:
        stream = stack.enter_context(open(os.devnull, "w", encoding="utf-8"))
        devnull_fd = os.open(os.devnull, os.O_WRONLY)
        saved_stdout_fd = os.dup(1)
        saved_stderr_fd = os.dup(2)
        stack.callback(os.close, saved_stderr_fd)
        stack.callback(os.close, saved_stdout_fd)
        stack.callback(os.close, devnull_fd)
        stack.callback(os.dup2, saved_stderr_fd, 2)
        stack.callback(os.dup2, saved_stdout_fd, 1)
        os.dup2(devnull_fd, 1)
        os.dup2(devnull_fd, 2)
        stack.enter_context(redirect_stdout(stream))
        stack.enter_context(redirect_stderr(stream))
        if capture_output is not None:
            stack.enter_context(
                capture_output(stdout=True, stderr=True, display=False),
            )

        try:
            yield
        finally:
            if previous_timing_env is None:
                os.environ.pop(ENABLE_TIMING_ENV, None)
            else:
                os.environ[ENABLE_TIMING_ENV] = previous_timing_env


def resolve_optimizer_config(module: Any) -> Any:
    for attribute in ("OPTIMIZER", "PARAMETER_SEARCH", "OPTIMIZATION"):
        optimizer_config = getattr(module, attribute, None)
        if optimizer_config is not None:
            return optimizer_config
    raise AttributeError(
        f"{module!r} does not expose OPTIMIZER, PARAMETER_SEARCH, or OPTIMIZATION."
    )


def load_optimizer_handle(module_name: str) -> tuple[Any, Any]:
    optimizer_module = importlib.import_module(module_name)
    return optimizer_module, resolve_optimizer_config(optimizer_module)


def build_research_parameter_search(
    optimizer_config: Any, *, max_trials: int, holdout_top_k: int, name_suffix: str = "_research"
) -> Any:
    return replace(
        optimizer_config,
        name=f"{optimizer_config.name}{name_suffix}",
        max_trials=min(max_trials, optimizer_config.max_trials),
        holdout_top_k=min(holdout_top_k, optimizer_config.holdout_top_k),
    )


def select_parameter_search_window(parameter_search: Any) -> Any:
    if parameter_search.holdout_windows:
        return parameter_search.holdout_windows[0]
    return parameter_search.train_windows[-1]


def snapshot_html_artifacts(output_root: Path) -> dict[Path, tuple[int, int]]:
    if not output_root.exists():
        return {}

    snapshot: dict[Path, tuple[int, int]] = {}
    for path in output_root.rglob("*.html"):
        try:
            stat = path.stat()
        except OSError:
            continue
        snapshot[path.resolve()] = (stat.st_mtime_ns, stat.st_size)
    return snapshot


def find_updated_html_artifacts(
    output_root: Path, before: Mapping[Path, tuple[int, int]]
) -> list[Path]:
    if not output_root.exists():
        return []

    updated: list[tuple[int, Path]] = []
    for path in output_root.rglob("*.html"):
        resolved = path.resolve()
        try:
            stat = resolved.stat()
        except OSError:
            continue
        signature = (stat.st_mtime_ns, stat.st_size)
        if before.get(resolved) == signature:
            continue
        updated.append((stat.st_mtime_ns, resolved))
    updated.sort()
    return [path for _, path in updated]


def partition_html_artifacts(html_artifacts: Sequence[Path]) -> tuple[list[Path], list[Path]]:
    artifacts = list(html_artifacts)
    summary_suffixes = ("_joint_portfolio.html",)
    summary_reports = [path for path in artifacts if path.name.endswith(summary_suffixes)]
    if summary_reports:
        primary = summary_reports[-1:]
        secondary = [path for path in artifacts if path not in primary]
        return primary, secondary
    if len(artifacts) <= 3:
        return artifacts, []
    return artifacts[-1:], artifacts[:-1]


_MAX_INLINE_HTML_BYTES = 8 * 1024 * 1024


def _embed_html_as_iframe(html_text: str, *, height: int = 820) -> str:
    from html import escape as html_escape

    escaped = html_escape(html_text, quote=True)
    return (
        f'<iframe srcdoc="{escaped}" width="100%" height="{height}" '
        f'style="border:none; background:white;" '
        f'sandbox="allow-scripts allow-same-origin allow-popups"></iframe>'
    )


def _display_html_suppressing_iframe_warning(html_text: str) -> None:
    import warnings

    from IPython.display import HTML, display

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Consider using IPython.display.IFrame instead",
            category=UserWarning,
        )
        display(HTML(html_text))


def display_html_artifacts(
    html_artifacts: Sequence[Path], *, repo_root: Path, iframe_height: int = 820
) -> None:
    if not html_artifacts:
        print("No new HTML artifacts were detected under output/.")
        return

    from IPython.display import Markdown, display

    primary, additional = partition_html_artifacts(html_artifacts)
    primary_html = primary[-1]
    relative_html = primary_html.relative_to(repo_root).as_posix()
    display(Markdown(f"**Chart artifact:** `{relative_html}`"))

    html_text = primary_html.read_text(encoding="utf-8")
    if len(html_text.encode("utf-8")) > _MAX_INLINE_HTML_BYTES:
        _display_html_suppressing_iframe_warning(html_text)
        print(
            "[notice] Chart exceeds inline iframe limit; rendered as inline HTML. "
            "Re-run this cell after reopening the notebook to restore the view."
        )
    else:
        _display_html_suppressing_iframe_warning(
            _embed_html_as_iframe(html_text, height=iframe_height)
        )

    if additional:
        extras = "\n".join(f"- `{path.relative_to(repo_root).as_posix()}`" for path in additional)
        display(Markdown("Additional HTML artifacts:\n" + extras))


__all__ = [
    "build_research_parameter_search",
    "display_html_artifacts",
    "ensure_notebook_repo_context",
    "find_repo_root",
    "find_updated_html_artifacts",
    "load_optimizer_handle",
    "partition_html_artifacts",
    "resolve_optimizer_config",
    "select_parameter_search_window",
    "snapshot_html_artifacts",
    "suppress_notebook_cell_output",
]
