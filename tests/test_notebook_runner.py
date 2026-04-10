from __future__ import annotations

from pathlib import Path

import nbformat

from prediction_market_extensions.backtesting import _notebook_runner as notebook_runner


def test_load_notebook_metadata_prefers_explicit_notebook_metadata(tmp_path: Path) -> None:
    notebook_path = tmp_path / "backtests" / "demo_runner.ipynb"
    notebook_path.parent.mkdir(parents=True)
    notebook = nbformat.v4.new_notebook(
        metadata={
            notebook_runner.NOTEBOOK_METADATA_KEY: {"name": "demo_notebook", "description": "Demo notebook runner"}
        },
        cells=[nbformat.v4.new_code_cell("x = 1")],
    )
    nbformat.write(notebook, notebook_path)

    metadata = notebook_runner.load_notebook_metadata(notebook_path, project_root=tmp_path)

    assert metadata == {
        "name": "demo_notebook",
        "description": "Demo notebook runner",
        "module_name": "backtests.demo_runner",
        "relative_parts": ("demo_runner.ipynb",),
    }


def test_load_notebook_metadata_falls_back_to_markdown_heading(tmp_path: Path) -> None:
    notebook_path = tmp_path / "backtests" / "demo_runner.ipynb"
    notebook_path.parent.mkdir(parents=True)
    notebook = nbformat.v4.new_notebook(
        cells=[nbformat.v4.new_markdown_cell("# Demo Notebook"), nbformat.v4.new_code_cell("x = 1")]
    )
    nbformat.write(notebook, notebook_path)

    metadata = notebook_runner.load_notebook_metadata(notebook_path, project_root=tmp_path)

    assert metadata is not None
    assert metadata["name"] == "demo_runner"
    assert metadata["description"] == "Demo Notebook"


def test_execute_notebook_runner_replaces_auto_embed_cell_with_latest_html(tmp_path: Path) -> None:
    notebook_path = tmp_path / "backtests" / "demo_runner.ipynb"
    notebook_path.parent.mkdir(parents=True)
    output_root = tmp_path / "output"
    output_root.mkdir()
    stale_html = output_root / "stale_report.html"
    stale_html.write_text("<html><body>stale</body></html>", encoding="utf-8")

    notebook = nbformat.v4.new_notebook(
        cells=[
            nbformat.v4.new_code_cell(
                "\n".join(
                    [
                        "from pathlib import Path",
                        "output_root = Path('output')",
                        "output_root.mkdir(exist_ok=True)",
                        "(output_root / 'demo_joint_portfolio.html').write_text('<html><body>summary</body></html>', encoding='utf-8')",
                        "(output_root / 'demo_detail_legacy.html').write_text('<html><body>detail</body></html>', encoding='utf-8')",
                    ]
                )
            ),
            nbformat.v4.new_markdown_cell(notebook_runner.AUTO_EMBED_CELL_MARKER + "\nOld content\n"),
        ]
    )
    nbformat.write(notebook, notebook_path)

    notebook_runner.execute_notebook_runner(notebook_path, project_root=tmp_path)

    executed = nbformat.read(notebook_path, as_version=4)
    auto_embed_cells = [
        cell for cell in executed.cells if notebook_runner.AUTO_EMBED_CELL_MARKER in cell.get("source", "")
    ]

    assert len(auto_embed_cells) == 1
    source = auto_embed_cells[0]["source"]
    assert "demo_joint_portfolio.html" in source
    assert "../output/demo_joint_portfolio.html" in source
    assert "demo_detail_legacy.html" in source
    assert "stale_report.html" not in source
