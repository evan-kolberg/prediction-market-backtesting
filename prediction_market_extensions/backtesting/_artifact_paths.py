from __future__ import annotations

from pathlib import Path
import re


def sanitize_chart_label(value: object, *, default: str) -> str:
    text = str(value or "").strip()
    if not text:
        return default

    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", text).strip("-")
    return sanitized or default


def resolve_independent_replay_detail_chart_output_path(
    *,
    backtest_name: str,
    configured_path: str | Path | None,
    emit_html: bool,
    market_id: str,
    sim_label: str,
    default_filename_label: str,
    configured_suffix_label: str,
) -> str | Path | None:
    if not emit_html and configured_path is None:
        return None

    default_filename = f"{backtest_name}_{default_filename_label}_legacy.html"

    if configured_path is None:
        return str(Path("output") / default_filename)

    raw_path = str(configured_path)
    if "{" in raw_path:
        try:
            resolved = raw_path.format(
                name=backtest_name,
                market_id=market_id,
                sim_label=sim_label,
            )

        except KeyError as exc:
            raise ValueError(
                "chart_output_path may only reference {name}, {market_id}, and {sim_label}."
            ) from exc

        path = Path(resolved)
        if not path.suffix:
            path = path / default_filename
        return str(path)

    path = Path(raw_path)
    if path.suffix:
        return str(path.with_name(f"{path.stem}_{configured_suffix_label}{path.suffix}"))
    return str(path / default_filename)
