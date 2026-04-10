from __future__ import annotations

import re
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DOCS_ROOT = REPO_ROOT / "docs"
MKDOCS_PATH = REPO_ROOT / "mkdocs.yml"
DOCS_URL_PREFIX = "/prediction-market-backtesting/"
README_PATH = REPO_ROOT / "README.md"
README_DOCS_URL_PREFIX = "https://evan-kolberg.github.io/prediction-market-backtesting/"
HEADING_RE = re.compile(r"^(?P<level>#{1,6})\s+(?P<title>.+?)\s*$", re.MULTILINE)


def _slugify(title: str) -> str:
    normalized = title.strip().casefold()
    normalized = re.sub(r"[^\w\s-]", "", normalized)
    normalized = re.sub(r"\s+", "-", normalized)
    return normalized.strip("-")


def _iter_nav_targets(node) -> list[str]:  # type: ignore[no-untyped-def]
    if isinstance(node, str):
        return [node]
    if isinstance(node, list):
        targets: list[str] = []
        for item in node:
            targets.extend(_iter_nav_targets(item))
        return targets
    if isinstance(node, dict):
        targets: list[str] = []
        for value in node.values():
            targets.extend(_iter_nav_targets(value))
        return targets
    return []


def _heading_slugs_for_doc(doc_path: Path) -> set[str]:
    text = doc_path.read_text()
    return {_slugify(match.group("title")) for match in HEADING_RE.finditer(text)}


def test_mkdocs_nav_anchor_targets_exist() -> None:
    config = yaml.safe_load(MKDOCS_PATH.read_text())
    nav_targets = _iter_nav_targets(config["nav"])

    for target in nav_targets:
        if not isinstance(target, str):
            continue
        if not target.startswith(DOCS_URL_PREFIX) or "#" not in target:
            continue

        doc_slug, _, anchor = target.removeprefix(DOCS_URL_PREFIX).partition("/#")
        doc_path = DOCS_ROOT / f"{doc_slug}.md"
        assert doc_path.exists(), f"missing docs file for nav target: {target}"
        heading_slugs = _heading_slugs_for_doc(doc_path)
        assert anchor in heading_slugs, f"missing nav anchor {anchor!r} in {doc_path}"


def test_root_readme_records_all_docs_and_subheaders() -> None:
    readme_text = README_PATH.read_text()

    docs_index_url = README_DOCS_URL_PREFIX
    assert docs_index_url in readme_text, "missing docs index link in root README"

    for doc_path in sorted(DOCS_ROOT.glob("*.md")):
        if doc_path.stem != "index":
            page_url = f"{README_DOCS_URL_PREFIX}{doc_path.stem}/"
            assert page_url in readme_text, f"missing docs page link in root README: {page_url}"

        for match in HEADING_RE.finditer(doc_path.read_text()):
            level = len(match.group("level"))
            if level not in {2, 3}:
                continue

            anchor = _slugify(match.group("title"))
            target = f"{README_DOCS_URL_PREFIX}{doc_path.stem}/#{anchor}"
            assert target in readme_text, f"missing docs heading link in root README: {target}"


def test_plotting_docs_distinguish_portfolio_and_comparison_summary_panels() -> None:
    plotting_text = (DOCS_ROOT / "plotting.md").read_text()
    normalized = re.sub(r"\s+", " ", plotting_text)

    assert "portfolio-wide panels collapse the whole basket into one combined series" in normalized
    assert "comparison panels keep one line per market or per labeled sim" in normalized
    assert (
        "`total_equity`, `periodic_pnl`, and `monthly_returns` are portfolio-wide summary panels"
    ) in normalized
    assert (
        "`equity`, `allocation`, `drawdown`, `rolling_sharpe`, `cash_equity`, "
        "and `brier_advantage` are comparison panels"
    ) in normalized
    assert "`brier_advantage` works on market slugs, not just individual sims" in normalized
    assert "`total_equity`, `periodic_pnl`, `drawdown`, and `monthly_returns`" not in normalized
