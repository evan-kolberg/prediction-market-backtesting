from __future__ import annotations

from pathlib import Path
from typing import Any


AUTO_EMBED_CELL_MARKER = "<!-- prediction-market-backtesting:auto-embedded-html -->"
NOTEBOOK_METADATA_KEY = "prediction_market_backtest"
NOTEBOOK_SUPPORTED_SUFFIX = ".ipynb"
_EMBED_HEIGHT_PX = 900


def load_notebook_metadata(
    notebook_path: Path,
    *,
    project_root: Path,
) -> dict[str, Any] | None:
    nbformat = _import_nbformat()

    try:
        with notebook_path.open("r", encoding="utf-8") as handle:
            notebook = nbformat.read(handle, as_version=4)
    except OSError as exc:
        raise RuntimeError(
            f"could not read {notebook_path.relative_to(project_root)}: {exc}"
        ) from exc

    metadata = getattr(notebook, "metadata", {}) or {}
    runner_metadata = metadata.get(NOTEBOOK_METADATA_KEY, {}) or {}
    name = runner_metadata.get("name")
    if not isinstance(name, str) or not name.strip():
        name = notebook_path.stem

    description = runner_metadata.get("description")
    if not isinstance(description, str) or not description.strip():
        description = _notebook_description(notebook)

    if not any(cell.get("cell_type") == "code" for cell in notebook.cells):
        return None

    relative_path = notebook_path.relative_to(project_root)
    return {
        "name": name.strip(),
        "description": description.strip(),
        "module_name": ".".join(relative_path.with_suffix("").parts),
        "relative_parts": notebook_path.relative_to(project_root / "backtests").parts,
    }


def execute_notebook_runner(
    notebook_path: Path,
    *,
    project_root: Path,
) -> None:
    nbclient = _import_nbclient()
    nbformat = _import_nbformat()

    with notebook_path.open("r", encoding="utf-8") as handle:
        notebook = nbformat.read(handle, as_version=4)

    artifact_snapshot = _html_artifact_snapshot(project_root)
    kernel_name = (
        getattr(notebook, "metadata", {}).get("kernelspec", {}).get("name") or "python3"
    )
    client = nbclient.NotebookClient(
        notebook,
        kernel_name=kernel_name,
        timeout=None,
        resources={"metadata": {"path": str(project_root)}},
    )

    try:
        client.execute()
    except Exception:
        _write_notebook(
            notebook_path=notebook_path, notebook=notebook, nbformat=nbformat
        )
        raise

    html_artifacts = _updated_html_artifacts(
        project_root=project_root,
        before=artifact_snapshot,
    )
    _replace_auto_embed_cell(
        notebook=notebook,
        notebook_path=notebook_path,
        html_artifacts=html_artifacts,
        nbformat=nbformat,
    )
    _write_notebook(notebook_path=notebook_path, notebook=notebook, nbformat=nbformat)


def _import_nbclient():
    try:
        import nbclient
    except ImportError as exc:  # pragma: no cover - dependency is present in repo env
        raise RuntimeError(
            "Notebook runner support requires nbclient to be installed."
        ) from exc
    return nbclient


def _import_nbformat():
    try:
        import nbformat
    except ImportError as exc:  # pragma: no cover - dependency is present in repo env
        raise RuntimeError(
            "Notebook runner support requires nbformat to be installed."
        ) from exc
    return nbformat


def _notebook_description(notebook: Any) -> str:
    for cell in notebook.cells:
        source = str(cell.get("source", "")).strip()
        if not source:
            continue
        if cell.get("cell_type") == "markdown":
            for line in source.splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                return stripped.lstrip("#").strip()
        if cell.get("cell_type") == "code":
            return ""
    return ""


def _html_artifact_snapshot(project_root: Path) -> dict[Path, tuple[int, int]]:
    output_root = project_root / "output"
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


def _updated_html_artifacts(
    *,
    project_root: Path,
    before: dict[Path, tuple[int, int]],
) -> list[Path]:
    output_root = project_root / "output"
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


def _replace_auto_embed_cell(
    *,
    notebook: Any,
    notebook_path: Path,
    html_artifacts: list[Path],
    nbformat: Any,
) -> None:
    notebook.cells = [
        cell
        for cell in notebook.cells
        if AUTO_EMBED_CELL_MARKER not in str(cell.get("source", ""))
    ]
    if not html_artifacts:
        return

    notebook.cells.append(
        nbformat.v4.new_markdown_cell(
            _auto_embed_cell_source(
                notebook_path=notebook_path,
                html_artifacts=html_artifacts,
            )
        )
    )


def _auto_embed_cell_source(
    *,
    notebook_path: Path,
    html_artifacts: list[Path],
) -> str:
    embedded, linked = _partition_html_artifacts(html_artifacts)
    lines = [
        AUTO_EMBED_CELL_MARKER,
        "## Generated HTML Artifacts",
        "",
        "Autogenerated after the most recent notebook-backed backtest run.",
        "",
    ]

    for path in embedded:
        relative = _relative_html_path(notebook_path=notebook_path, html_path=path)
        lines.extend(
            [
                f"### {path.name}",
                f'<iframe src="{relative}" width="100%" height="{_EMBED_HEIGHT_PX}"></iframe>',
                "",
            ]
        )

    if linked:
        lines.append("Additional HTML artifacts:")
        for path in linked:
            relative = _relative_html_path(notebook_path=notebook_path, html_path=path)
            lines.append(f"- [{path.name}]({relative})")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def _partition_html_artifacts(
    html_artifacts: list[Path],
) -> tuple[list[Path], list[Path]]:
    summary_reports = [
        path for path in html_artifacts if path.name.endswith("_multi_market.html")
    ]
    if summary_reports:
        primary = summary_reports[-1:]
        secondary = [path for path in html_artifacts if path not in primary]
        return primary, secondary
    if len(html_artifacts) <= 3:
        return html_artifacts, []
    return html_artifacts[-1:], html_artifacts[:-1]


def _relative_html_path(*, notebook_path: Path, html_path: Path) -> str:
    return html_path.relative_to(
        notebook_path.parent.resolve(), walk_up=True
    ).as_posix()


def _write_notebook(*, notebook_path: Path, notebook: Any, nbformat: Any) -> None:
    with notebook_path.open("w", encoding="utf-8") as handle:
        nbformat.write(notebook, handle)
