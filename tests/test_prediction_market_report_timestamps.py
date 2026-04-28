from __future__ import annotations

import base64
import gzip
import json
import math
import re
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import numpy as np
import pytest

from prediction_market_extensions.adapters.prediction_market import research
from prediction_market_extensions.adapters.prediction_market.research import (
    save_aggregate_backtest_report,
    save_joint_portfolio_backtest_report,
)


def _decode_bokeh_value(value: object) -> object:
    if isinstance(value, dict) and value.get("type") == "ndarray":
        array = value.get("array")
        if isinstance(array, dict) and array.get("type") == "bytes":
            raw = gzip.decompress(base64.b64decode(str(array["data"])))
            dtype = np.dtype(str(value["dtype"])).newbyteorder(
                ">" if value.get("order") == "big" else "<"
            )
            return np.frombuffer(raw, dtype=dtype).reshape(value["shape"]).tolist()
        if isinstance(array, list):
            return array
    return value


def _bokeh_column_sources(html: str, required_key: str) -> list[dict[str, object]]:
    match = re.search(
        r'<script type="application/json"[^>]*>\s*(\{.*?\})\s*</script>',
        html,
        re.S,
    )
    assert match is not None
    document = json.loads(match.group(1))
    sources: list[dict[str, object]] = []

    def visit(value: object) -> None:
        if isinstance(value, dict):
            if value.get("name") == "ColumnDataSource":
                data = value.get("attributes", {}).get("data")
                if isinstance(data, dict) and data.get("type") == "map":
                    source = {
                        str(key): _decode_bokeh_value(encoded)
                        for key, encoded in data.get("entries", [])
                    }
                    if required_key in source:
                        sources.append(source)
            for item in value.values():
                visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(document)
    return sources


def _bokeh_column_source_shapes(html: str, required_key: str) -> list[dict[str, int]]:
    shapes: list[dict[str, int]] = []
    for source in _bokeh_column_sources(html, required_key):
        source_shapes: dict[str, int] = {}
        for key, decoded in source.items():
            if isinstance(decoded, list):
                source_shapes[key] = len(decoded)
        if required_key in source_shapes:
            shapes.append(source_shapes)
    return shapes


def test_save_aggregate_backtest_report_accepts_mixed_iso_timestamp_precision(tmp_path) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "aggregate_mixed_timestamps.html"
    results = [
        {
            "slug": "market-mixed",
            "trades": 10,
            "fills": 1,
            "pnl": 1.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:57:40.123456+00:00", 0.42),
            ],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.41),
                ("2026-03-14T17:57:40.123456+00:00", 0.43),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:57:40.123456+00:00", 0.42),
            ],
            "outcome_series": [
                ("2026-03-14T17:57:40+00:00", 1.0),
                ("2026-03-14T17:57:40.123456+00:00", 1.0),
            ],
            "fill_events": [
                {
                    "order_id": "fill-mixed",
                    "market_id": "market-mixed",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 10.0,
                    "timestamp": "2026-03-14T17:57:40+00:00",
                    "commission": 0.0,
                }
            ],
            "pnl_series": [
                ("2026-03-14T17:57:40+00:00", 0.0),
                ("2026-03-14T17:57:40.123456+00:00", 1.0),
            ],
            "equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:57:40.123456+00:00", 101.0),
            ],
            "cash_series": [
                ("2026-03-14T17:57:40+00:00", 96.0),
                ("2026-03-14T17:57:40.123456+00:00", 96.0),
            ],
        }
    ]

    report_path = save_aggregate_backtest_report(
        results=results,
        output_path=output_path,
        title="mixed timestamp precision chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "mixed timestamp precision chart" in html
    assert "market-mixed" in html


def test_save_aggregate_backtest_report_html_contains_fill_and_pnl_markers(tmp_path) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "aggregate_fill_markers.html"
    results = [
        {
            "slug": "marker-market",
            "book_events": 10,
            "fills": 2,
            "pnl": 2.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:58:40+00:00", 0.45),
            ],
            "fill_events": [
                {
                    "order_id": "fill-buy",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 5.0,
                    "timestamp": "2026-03-14T17:57:40+00:00",
                    "commission": 0.0,
                },
                {
                    "order_id": "fill-sell",
                    "action": "sell",
                    "side": "yes",
                    "price": 0.45,
                    "quantity": 5.0,
                    "timestamp": "2026-03-14T17:58:40+00:00",
                    "commission": 0.0,
                },
            ],
            "pnl_series": [
                ("2026-03-14T17:57:40+00:00", 0.0),
                ("2026-03-14T17:58:40+00:00", 2.0),
            ],
            "equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:58:40+00:00", 102.0),
            ],
            "cash_series": [
                ("2026-03-14T17:57:40+00:00", 98.0),
                ("2026-03-14T17:58:40+00:00", 102.0),
            ],
        }
    ]

    report_path = save_aggregate_backtest_report(
        results=results,
        output_path=output_path,
        title="aggregate marker chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("market_pnl", "yes_price"),
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "YES Price" in html
    assert "Fills (" in html
    assert "fill_color" in html
    assert "Profit / Loss" in html
    assert "pnl_long" in html
    assert "pnl_short" in html
    assert "triangle" in html
    assert "inverted_triangle" in html
    assert html.count("marker-market") >= 2
    pnl_sources = _bokeh_column_source_shapes(html, "pnl_long")
    fill_sources = _bokeh_column_source_shapes(html, "fill_color")
    assert any(source["pnl_long"] == 2 for source in pnl_sources)
    assert any(source["fill_color"] == 2 for source in fill_sources)


def test_save_aggregate_backtest_report_html_preserves_fill_values_and_sides(tmp_path) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "aggregate_exact_fill_markers.html"
    results = [
        {
            "slug": "marker-market",
            "book_events": 10,
            "fills": 1,
            "pnl": -0.25,
            "price_series": [
                ("2026-03-14T17:57:00+00:00", 0.20),
                ("2026-03-14T17:58:00+00:00", 0.30),
            ],
            "fill_events": [
                {
                    "order_id": "fill-buy-no",
                    "market_id": "raw-opaque-instrument",
                    "action": "buy",
                    "side": "no",
                    "price": 0.05,
                    "quantity": 5.0,
                    "timestamp": "2026-03-14T17:57:30+00:00",
                    "commission": 0.0,
                },
            ],
            "pnl_series": [
                ("2026-03-14T17:57:00+00:00", 0.0),
                ("2026-03-14T17:58:00+00:00", -0.25),
            ],
            "equity_series": [
                ("2026-03-14T17:57:00+00:00", 100.0),
                ("2026-03-14T17:58:00+00:00", 99.75),
            ],
            "cash_series": [
                ("2026-03-14T17:57:00+00:00", 100.0),
                ("2026-03-14T17:58:00+00:00", 99.75),
            ],
        }
    ]

    report_path = save_aggregate_backtest_report(
        results=results,
        output_path=output_path,
        title="aggregate exact marker chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("yes_price",),
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    fill_source = next(
        source
        for source in _bokeh_column_sources(html, "fill_color")
        if source.get("market_id") == ["marker-market"]
    )
    assert fill_source["action"] == ["buy"]
    assert fill_source["side"] == ["no"]
    assert fill_source["price"] == pytest.approx([0.05])
    assert fill_source["quantity"] == pytest.approx([5.0])

    price_source = next(
        source
        for source in _bokeh_column_sources(html, "price_marker-market")
        if "price_marker-market" in source
    )
    fill_bar = int(fill_source["index"][0])  # type: ignore[index]
    assert price_source["price_marker-market"][fill_bar] == pytest.approx(0.05)  # type: ignore[index]


def test_save_aggregate_backtest_report_uses_initial_capital_basis(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    save_aggregate_backtest_report(
        results=[
            {
                "slug": "first-point-after-fill",
                "book_events": 2,
                "fills": 2,
                "pnl": 1.0,
                "equity_series": [
                    ("2026-04-01T00:01:00+00:00", 100.5),
                    ("2026-04-01T00:02:00+00:00", 101.0),
                ],
                "cash_series": [
                    ("2026-04-01T00:01:00+00:00", 100.5),
                    ("2026-04-01T00:02:00+00:00", 101.0),
                ],
                "pnl_series": [
                    ("2026-04-01T00:01:00+00:00", 0.5),
                    ("2026-04-01T00:02:00+00:00", 1.0),
                ],
            }
        ],
        output_path=tmp_path / "aggregate_initial_basis.html",
        title="aggregate initial basis",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("total_equity", "total_cash_equity"),
    )

    assert len(captured_results) == 1
    result_kwargs = captured_results[0]
    assert result_kwargs["initial_cash"] == pytest.approx(100.0)
    assert result_kwargs["final_equity"] == pytest.approx(101.0)
    assert result_kwargs["metrics"]["total_return"] == pytest.approx(0.01)
    assert result_kwargs["equity_curve"][0].total_equity == pytest.approx(100.0)


def test_save_aggregate_backtest_report_uses_initial_cash_for_pnl_only_series(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    save_aggregate_backtest_report(
        results=[
            {
                "slug": "pnl-only",
                "book_events": 2,
                "fills": 0,
                "pnl": 25.0,
                "initial_cash": 1000.0,
                "pnl_series": [
                    ("2026-04-01T00:00:00+00:00", 0.0),
                    ("2026-04-01T00:01:00+00:00", 25.0),
                ],
            }
        ],
        output_path=tmp_path / "aggregate_pnl_only.html",
        title="aggregate pnl only",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("total_equity", "total_cash_equity"),
    )

    result_kwargs = captured_results[0]
    assert result_kwargs["initial_cash"] == pytest.approx(1000.0)
    assert result_kwargs["final_equity"] == pytest.approx(1025.0)
    assert result_kwargs["metrics"]["total_return"] == pytest.approx(0.025)


def test_save_aggregate_backtest_report_marks_zero_capital_return_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    save_aggregate_backtest_report(
        results=[
            {
                "slug": "zero-capital",
                "book_events": 2,
                "fills": 0,
                "pnl": 10.0,
                "initial_cash": 0.0,
                "equity_series": [
                    ("2026-04-01T00:00:00+00:00", 0.0),
                    ("2026-04-01T00:01:00+00:00", 10.0),
                ],
            }
        ],
        output_path=tmp_path / "aggregate_zero_capital.html",
        title="aggregate zero capital",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("total_equity", "total_cash_equity"),
    )

    result_kwargs = captured_results[0]
    assert math.isnan(result_kwargs["metrics"]["total_return"])


def test_save_joint_portfolio_backtest_report_html_contains_fill_and_pnl_markers(
    tmp_path,
) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "joint_fill_markers.html"
    results = [
        {
            "sim_label": "joint-win",
            "book_events": 10,
            "fills": 1,
            "pnl": 3.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.20),
                ("2026-03-14T17:58:40+00:00", 1.00),
            ],
            "fill_events": [
                {
                    "order_id": "fill-win",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.20,
                    "quantity": 5.0,
                    "timestamp": "2026-03-14T17:57:40+00:00",
                    "commission": 0.0,
                }
            ],
            "equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:58:40+00:00", 103.0),
            ],
            "cash_series": [
                ("2026-03-14T17:57:40+00:00", 99.0),
                ("2026-03-14T17:58:40+00:00", 103.0),
            ],
            "joint_portfolio_equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:58:40+00:00", 101.0),
            ],
            "joint_portfolio_cash_series": [
                ("2026-03-14T17:57:40+00:00", 98.5),
                ("2026-03-14T17:58:40+00:00", 101.0),
            ],
        },
        {
            "sim_label": "joint-loss",
            "book_events": 10,
            "fills": 1,
            "pnl": -2.0,
            "price_series": [
                ("2026-03-14T17:57:50+00:00", 0.40),
                ("2026-03-14T17:58:50+00:00", 0.00),
            ],
            "fill_events": [
                {
                    "order_id": "fill-loss",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 5.0,
                    "timestamp": "2026-03-14T17:57:50+00:00",
                    "commission": 0.0,
                }
            ],
            "equity_series": [
                ("2026-03-14T17:57:50+00:00", 100.0),
                ("2026-03-14T17:58:50+00:00", 98.0),
            ],
            "cash_series": [
                ("2026-03-14T17:57:50+00:00", 98.0),
                ("2026-03-14T17:58:50+00:00", 98.0),
            ],
        },
    ]

    report_path = save_joint_portfolio_backtest_report(
        results=results,
        output_path=output_path,
        title="joint marker chart",
        market_key="sim_label",
        pnl_label="PnL (USDC)",
        plot_panels=("market_pnl", "yes_price"),
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "YES Price" in html
    assert "Fills (" in html
    assert "fill_color" in html
    assert "Profit / Loss" in html
    assert "pnl_long" in html
    assert "pnl_short" in html
    assert "joint-win" in html
    assert "joint-loss" in html
    pnl_sources = _bokeh_column_source_shapes(html, "pnl_long")
    fill_sources = _bokeh_column_source_shapes(html, "fill_color")
    assert any(source["pnl_long"] == 2 for source in pnl_sources)
    assert any(source["fill_color"] == 2 for source in fill_sources)


def test_save_joint_portfolio_backtest_report_uses_initial_capital_basis(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "joint-first-point-after-fill",
                "book_events": 2,
                "fills": 1,
                "pnl": 1.0,
                "joint_portfolio_equity_series": [
                    ("2026-04-01T00:01:00+00:00", 200.5),
                    ("2026-04-01T00:02:00+00:00", 201.0),
                ],
                "joint_portfolio_cash_series": [
                    ("2026-04-01T00:01:00+00:00", 200.5),
                    ("2026-04-01T00:02:00+00:00", 201.0),
                ],
                "joint_portfolio_pnl_series": [
                    ("2026-04-01T00:01:00+00:00", 0.5),
                    ("2026-04-01T00:02:00+00:00", 1.0),
                ],
            }
        ],
        output_path=tmp_path / "joint_initial_basis.html",
        title="joint initial basis",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("total_equity", "total_cash_equity"),
    )

    assert len(captured_results) == 1
    result_kwargs = captured_results[0]
    assert result_kwargs["initial_cash"] == pytest.approx(200.0)
    assert result_kwargs["final_equity"] == pytest.approx(201.0)
    assert result_kwargs["metrics"]["total_return"] == pytest.approx(0.005)
    assert result_kwargs["equity_curve"][0].total_equity == pytest.approx(200.0)


def test_save_joint_portfolio_backtest_report_accepts_mixed_iso_timestamp_precision(
    tmp_path,
) -> None:
    pytest.importorskip("bokeh")

    output_path = tmp_path / "joint_mixed_timestamps.html"
    results = [
        {
            "slug": "market-a",
            "trades": 10,
            "fills": 1,
            "pnl": 1.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:57:40.123456+00:00", 0.42),
            ],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.41),
                ("2026-03-14T17:57:40.123456+00:00", 0.43),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:57:40.123456+00:00", 0.42),
            ],
            "outcome_series": [
                ("2026-03-14T17:57:40+00:00", 1.0),
                ("2026-03-14T17:57:40.123456+00:00", 1.0),
            ],
            "fill_events": [
                {
                    "order_id": "fill-a",
                    "market_id": "market-a",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 10.0,
                    "timestamp": "2026-03-14T17:57:40+00:00",
                    "commission": 0.0,
                }
            ],
            "joint_portfolio_pnl_series": [
                ("2026-03-14T17:57:40+00:00", 0.0),
                ("2026-03-14T17:57:40.123456+00:00", 1.5),
            ],
            "joint_portfolio_equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:57:40.123456+00:00", 101.5),
            ],
            "joint_portfolio_cash_series": [
                ("2026-03-14T17:57:40+00:00", 96.0),
                ("2026-03-14T17:57:40.123456+00:00", 96.0),
            ],
        },
        {
            "slug": "market-b",
            "trades": 8,
            "fills": 1,
            "pnl": 0.5,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.55),
                ("2026-03-14T17:57:40.123456+00:00", 0.57),
            ],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.54),
                ("2026-03-14T17:57:40.123456+00:00", 0.56),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.55),
                ("2026-03-14T17:57:40.123456+00:00", 0.57),
            ],
            "outcome_series": [
                ("2026-03-14T17:57:40+00:00", 0.0),
                ("2026-03-14T17:57:40.123456+00:00", 0.0),
            ],
            "fill_events": [
                {
                    "order_id": "fill-b",
                    "market_id": "market-b",
                    "action": "sell",
                    "side": "yes",
                    "price": 0.57,
                    "quantity": 5.0,
                    "timestamp": "2026-03-14T17:57:40.123456+00:00",
                    "commission": 0.0,
                }
            ],
        },
    ]

    report_path = save_joint_portfolio_backtest_report(
        results=results,
        output_path=output_path,
        title="joint mixed timestamp precision chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "joint mixed timestamp precision chart" in html
    assert "market-a" in html
    assert "market-b" in html


def test_save_aggregate_backtest_report_adds_brier_placeholder_when_outcomes_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    plot_calls: list[dict[str, object]] = []
    placeholder_panel = object()

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: plot_calls.append(kwargs) or object()),
        ),
    )

    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_build_brier_placeholder_panel",
        lambda message: placeholder_panel,
    )

    def _fake_deserialize_fill_events(**kwargs):
        return [SimpleNamespace() for _ in kwargs["fill_events"]]

    monkeypatch.setattr(
        research,
        "_deserialize_fill_events",
        _fake_deserialize_fill_events,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    results = [
        {
            "slug": "market-a",
            "trades": 10,
            "fills": 0,
            "pnl": 1.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:58:40+00:00", 0.42),
            ],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.41),
                ("2026-03-14T17:58:40+00:00", 0.43),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:58:40+00:00", 0.42),
            ],
            "outcome_series": [],
            "pnl_series": [
                ("2026-03-14T17:57:40+00:00", 0.0),
                ("2026-03-14T17:58:40+00:00", 1.0),
            ],
            "equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:58:40+00:00", 101.0),
            ],
            "cash_series": [
                ("2026-03-14T17:57:40+00:00", 96.0),
                ("2026-03-14T17:58:40+00:00", 96.0),
            ],
        }
    ]

    save_aggregate_backtest_report(
        results=results,
        output_path=tmp_path / "aggregate_brier_placeholder.html",
        title="aggregate placeholder chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("brier_advantage",),
    )

    assert plot_calls
    assert plot_calls[0]["extra_panels"]["brier_advantage"] is placeholder_panel


def test_save_joint_portfolio_backtest_report_adds_brier_placeholder_when_outcomes_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    plot_calls: list[dict[str, object]] = []
    placeholder_panel = object()

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: plot_calls.append(kwargs) or object()),
        ),
    )

    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_build_brier_placeholder_panel",
        lambda message: placeholder_panel,
    )

    def _fake_deserialize_fill_events(**kwargs):
        return [SimpleNamespace() for _ in kwargs["fill_events"]]

    monkeypatch.setattr(
        research,
        "_deserialize_fill_events",
        _fake_deserialize_fill_events,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    results = [
        {
            "slug": "market-a",
            "trades": 10,
            "fills": 0,
            "pnl": 1.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:58:40+00:00", 0.42),
            ],
            "user_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.41),
                ("2026-03-14T17:58:40+00:00", 0.43),
            ],
            "market_probability_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:58:40+00:00", 0.42),
            ],
            "outcome_series": [],
            "joint_portfolio_equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:58:40+00:00", 101.0),
            ],
            "joint_portfolio_cash_series": [
                ("2026-03-14T17:57:40+00:00", 96.0),
                ("2026-03-14T17:58:40+00:00", 96.0),
            ],
        }
    ]

    save_joint_portfolio_backtest_report(
        results=results,
        output_path=tmp_path / "joint_brier_placeholder.html",
        title="joint placeholder chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("brier_advantage",),
    )

    assert plot_calls
    assert plot_calls[0]["extra_panels"]["brier_advantage"] is placeholder_panel


def test_save_aggregate_backtest_report_limits_dense_yes_price_fill_markers(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    apply_calls: list[dict[str, object]] = []
    dummy_layout = object()

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: dummy_layout),
        ),
    )

    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: apply_calls.append(kwargs) or layout,
    )

    def _fake_deserialize_fill_events(**kwargs):
        return [SimpleNamespace() for _ in kwargs["fill_events"]]

    monkeypatch.setattr(
        research,
        "_deserialize_fill_events",
        _fake_deserialize_fill_events,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    results = [
        {
            "slug": "market-a",
            "trades": 10,
            "fills": 251,
            "pnl": 1.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:58:40+00:00", 0.42),
            ],
            "fill_events": [
                {
                    "order_id": f"fill-{idx}",
                    "market_id": "market-a",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 1.0,
                    "timestamp": f"2026-03-14T17:{idx % 60:02d}:40+00:00",
                    "commission": 0.0,
                }
                for idx in range(251)
            ],
            "pnl_series": [
                ("2026-03-14T17:57:40+00:00", 0.0),
                ("2026-03-14T17:58:40+00:00", 1.0),
            ],
            "equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:58:40+00:00", 101.0),
            ],
            "cash_series": [
                ("2026-03-14T17:57:40+00:00", 96.0),
                ("2026-03-14T17:58:40+00:00", 96.0),
            ],
        }
    ]

    report_path = save_aggregate_backtest_report(
        results=results,
        output_path=tmp_path / "aggregate_dense_fills.html",
        title="aggregate dense fills",
        market_key="slug",
        pnl_label="PnL (USDC)",
    )

    assert report_path == str((tmp_path / "aggregate_dense_fills.html").resolve())
    assert apply_calls == [{"max_market_pnl_fill_markers": 250, "max_yes_price_fill_markers": 250}]


def test_save_joint_portfolio_backtest_report_limits_dense_yes_price_fill_markers(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    apply_calls: list[dict[str, object]] = []
    dummy_layout = object()

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: dummy_layout),
        ),
    )

    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: apply_calls.append(kwargs) or layout,
    )

    def _fake_deserialize_fill_events(**kwargs):
        return [SimpleNamespace() for _ in kwargs["fill_events"]]

    monkeypatch.setattr(
        research,
        "_deserialize_fill_events",
        _fake_deserialize_fill_events,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    results = [
        {
            "slug": "market-a",
            "trades": 10,
            "fills": 251,
            "pnl": 1.0,
            "price_series": [
                ("2026-03-14T17:57:40+00:00", 0.40),
                ("2026-03-14T17:58:40+00:00", 0.42),
            ],
            "fill_events": [
                {
                    "order_id": f"fill-{idx}",
                    "market_id": "market-a",
                    "action": "buy",
                    "side": "yes",
                    "price": 0.40,
                    "quantity": 1.0,
                    "timestamp": f"2026-03-14T17:{idx % 60:02d}:40+00:00",
                    "commission": 0.0,
                }
                for idx in range(251)
            ],
            "joint_portfolio_equity_series": [
                ("2026-03-14T17:57:40+00:00", 100.0),
                ("2026-03-14T17:58:40+00:00", 101.0),
            ],
            "joint_portfolio_cash_series": [
                ("2026-03-14T17:57:40+00:00", 96.0),
                ("2026-03-14T17:58:40+00:00", 96.0),
            ],
        }
    ]

    report_path = save_joint_portfolio_backtest_report(
        results=results,
        output_path=tmp_path / "joint_dense_fills.html",
        title="joint dense fills",
        market_key="slug",
        pnl_label="PnL (USDC)",
    )

    assert report_path == str((tmp_path / "joint_dense_fills.html").resolve())
    assert apply_calls == [{"max_market_pnl_fill_markers": 250, "max_yes_price_fill_markers": 250}]


def test_save_aggregate_backtest_report_prunes_unused_payload_for_total_only_panels(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    def _deserialize_fill_events(**kwargs):
        raise AssertionError("fill events should not be deserialized for total-only summary panels")

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(research, "_deserialize_fill_events", _deserialize_fill_events)
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    report_path = save_aggregate_backtest_report(
        results=[
            {
                "slug": "market-a",
                "fills": 2,
                "pnl": 3.0,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.40),
                    ("2026-03-14T17:58:40+00:00", 0.42),
                ],
                "fill_events": [
                    {
                        "order_id": "fill-a",
                        "market_id": "market-a",
                        "action": "buy",
                        "side": "yes",
                        "price": 0.40,
                        "quantity": 10.0,
                        "timestamp": "2026-03-14T17:57:40+00:00",
                        "commission": 0.0,
                    }
                ],
                "equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 103.0),
                ],
                "cash_series": [
                    ("2026-03-14T17:57:40+00:00", 95.0),
                    ("2026-03-14T17:58:40+00:00", 96.0),
                ],
            },
            {
                "slug": "market-b",
                "fills": 1,
                "pnl": -1.0,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.55),
                    ("2026-03-14T17:58:40+00:00", 0.57),
                ],
                "fill_events": [
                    {
                        "order_id": "fill-b",
                        "market_id": "market-b",
                        "action": "sell",
                        "side": "yes",
                        "price": 0.57,
                        "quantity": 5.0,
                        "timestamp": "2026-03-14T17:58:40+00:00",
                        "commission": 0.0,
                    }
                ],
                "equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 99.0),
                ],
                "cash_series": [
                    ("2026-03-14T17:57:40+00:00", 97.0),
                    ("2026-03-14T17:58:40+00:00", 98.0),
                ],
            },
        ],
        output_path=tmp_path / "aggregate_total_only.html",
        title="aggregate total only",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("total_equity", "periodic_pnl", "monthly_returns"),
    )

    assert report_path == str((tmp_path / "aggregate_total_only.html").resolve())
    assert len(captured_results) == 1
    result_kwargs = captured_results[0]
    assert result_kwargs["market_prices"] == {}
    assert result_kwargs["fills"] == []
    assert result_kwargs["overlay_series"] == {}


def test_save_aggregate_backtest_report_keeps_market_payload_when_summary_panels_need_it(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []
    deserialize_calls: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    def _deserialize_fill_events(**kwargs):
        deserialize_calls.append(kwargs)
        return [SimpleNamespace(**kwargs)] if kwargs["fill_events"] else []

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research,
        "_configure_summary_report_downsampling",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(research, "_deserialize_fill_events", _deserialize_fill_events)
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    save_aggregate_backtest_report(
        results=[
            {
                "slug": "market-a",
                "fills": 2,
                "pnl": 3.0,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.40),
                    ("2026-03-14T17:58:40+00:00", 0.42),
                ],
                "fill_events": [
                    {
                        "order_id": "fill-a",
                        "market_id": "market-a",
                        "action": "buy",
                        "side": "yes",
                        "price": 0.40,
                        "quantity": 10.0,
                        "timestamp": "2026-03-14T17:57:40+00:00",
                        "commission": 0.0,
                    }
                ],
                "equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 103.0),
                ],
                "cash_series": [
                    ("2026-03-14T17:57:40+00:00", 95.0),
                    ("2026-03-14T17:58:40+00:00", 96.0),
                ],
            }
        ],
        output_path=tmp_path / "aggregate_rich.html",
        title="aggregate rich",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("equity", "yes_price", "allocation", "market_pnl"),
    )

    assert len(captured_results) == 1
    result_kwargs = captured_results[0]
    assert "market-a" in result_kwargs["market_prices"]
    assert result_kwargs["fills"] != []
    assert result_kwargs["overlay_series"] != {}
    assert len(deserialize_calls) == 1


def test_save_joint_portfolio_backtest_report_prunes_unused_payload_for_total_only_panels(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    def _deserialize_fill_events(**kwargs):
        raise AssertionError(
            "fill events should not be deserialized for total-only joint summary panels"
        )

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(research, "_deserialize_fill_events", _deserialize_fill_events)
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )
    save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "market-a",
                "fills": 1,
                "pnl": 1.0,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.40),
                    ("2026-03-14T17:58:40+00:00", 0.42),
                ],
                "fill_events": [
                    {
                        "order_id": "fill-a",
                        "market_id": "market-a",
                        "action": "buy",
                        "side": "yes",
                        "price": 0.40,
                        "quantity": 10.0,
                        "timestamp": "2026-03-14T17:57:40+00:00",
                        "commission": 0.0,
                    }
                ],
                "joint_portfolio_equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 101.0),
                ],
                "joint_portfolio_cash_series": [
                    ("2026-03-14T17:57:40+00:00", 96.0),
                    ("2026-03-14T17:58:40+00:00", 96.0),
                ],
            }
        ],
        output_path=tmp_path / "joint_total_only.html",
        title="joint total only",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("total_equity", "total_cash_equity", "periodic_pnl", "monthly_returns"),
    )

    assert len(captured_results) == 1
    result_kwargs = captured_results[0]
    assert result_kwargs["market_prices"] == {}
    assert result_kwargs["fills"] == []


def test_save_joint_portfolio_backtest_report_keeps_market_overlays_when_needed(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []
    plot_calls: list[dict[str, object]] = []
    total_brier_panel = object()
    market_brier_panel = object()

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: plot_calls.append(kwargs) or object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_build_total_brier_panel",
        lambda frame: total_brier_panel,
    )
    monkeypatch.setattr(
        research,
        "_build_summary_brier_panel",
        lambda *args, **kwargs: market_brier_panel,
    )

    save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "market-a",
                "fills": 1,
                "pnl": 1.0,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.40),
                    ("2026-03-14T17:58:40+00:00", 0.42),
                ],
                "user_probability_series": [
                    ("2026-03-14T17:57:40+00:00", 0.41),
                    ("2026-03-14T17:58:40+00:00", 0.43),
                ],
                "market_probability_series": [
                    ("2026-03-14T17:57:40+00:00", 0.40),
                    ("2026-03-14T17:58:40+00:00", 0.42),
                ],
                "outcome_series": [
                    ("2026-03-14T17:57:40+00:00", 1.0),
                    ("2026-03-14T17:58:40+00:00", 1.0),
                ],
                "equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 101.0),
                ],
                "cash_series": [
                    ("2026-03-14T17:57:40+00:00", 96.0),
                    ("2026-03-14T17:58:40+00:00", 96.0),
                ],
                "joint_portfolio_equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 101.5),
                ],
                "joint_portfolio_cash_series": [
                    ("2026-03-14T17:57:40+00:00", 96.0),
                    ("2026-03-14T17:58:40+00:00", 96.0),
                ],
            },
            {
                "slug": "market-b",
                "fills": 1,
                "pnl": 0.5,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.55),
                    ("2026-03-14T17:58:40+00:00", 0.57),
                ],
                "user_probability_series": [
                    ("2026-03-14T17:57:40+00:00", 0.54),
                    ("2026-03-14T17:58:40+00:00", 0.56),
                ],
                "market_probability_series": [
                    ("2026-03-14T17:57:40+00:00", 0.55),
                    ("2026-03-14T17:58:40+00:00", 0.57),
                ],
                "outcome_series": [
                    ("2026-03-14T17:57:40+00:00", 0.0),
                    ("2026-03-14T17:58:40+00:00", 0.0),
                ],
                "equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 100.5),
                ],
                "cash_series": [
                    ("2026-03-14T17:57:40+00:00", 102.85),
                    ("2026-03-14T17:58:40+00:00", 102.85),
                ],
            },
        ],
        output_path=tmp_path / "joint_with_overlays.html",
        title="joint with overlays",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=(
            "total_brier_advantage",
            "brier_advantage",
            "equity",
            "drawdown",
            "cash_equity",
        ),
    )

    assert len(captured_results) == 1
    assert len(plot_calls) == 1
    result_kwargs = captured_results[0]
    assert result_kwargs["hide_primary_panel_series"] is True
    assert set(result_kwargs["overlay_series"]["equity"]) == {"market-a", "market-b"}
    assert set(result_kwargs["overlay_series"]["cash"]) == {"market-a", "market-b"}
    assert result_kwargs["market_pnls"] == {"market-a": 1.0, "market-b": 0.5}
    assert plot_calls[0]["extra_panels"] == {
        "total_brier_advantage": total_brier_panel,
        "brier_advantage": market_brier_panel,
    }


def test_summary_brier_panels_exclude_settlement_and_post_settlement_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    total_brier_panel = object()
    market_brier_panel = object()
    t_pre = "2026-04-01T00:00:00+00:00"
    t_settle = "2026-04-01T00:01:00+00:00"
    t_post = "2026-04-01T00:02:00+00:00"

    def _capture_total(frame):
        captured["total"] = frame.copy()
        return total_brier_panel

    def _capture_market(frames, **kwargs):
        captured["market"] = {key: frame.copy() for key, frame in frames.items()}
        return market_brier_panel

    monkeypatch.setattr(research.legacy_plot_adapter, "_build_total_brier_panel", _capture_total)
    monkeypatch.setattr(research, "_build_summary_brier_panel", _capture_market)

    panels = research._build_summary_brier_extra_panels(
        results=[
            {
                "slug": "settlement-brier",
                "fills": 0,
                "pnl": 0.0,
                "user_probability_series": [(t_pre, 0.55), (t_settle, 0.55), (t_post, 0.55)],
                "market_probability_series": [(t_pre, 0.60), (t_settle, 1.0), (t_post, 1.0)],
                "outcome_series": [(t_pre, 1.0), (t_settle, 1.0), (t_post, 1.0)],
                "settlement_pnl_applied": True,
                "settlement_series_time": t_settle,
            }
        ],
        market_key="slug",
        resolved_plot_panels=("total_brier_advantage", "brier_advantage"),
        max_points_per_market=400,
    )

    expected_advantage = (0.60 - 1.0) ** 2 - (0.55 - 1.0) ** 2
    total_frame = captured["total"]
    market_frame = captured["market"]["settlement-brier"]

    assert panels == {
        "total_brier_advantage": total_brier_panel,
        "brier_advantage": market_brier_panel,
    }
    assert len(total_frame) == 1
    assert len(market_frame) == 1
    assert total_frame["cumulative_brier_advantage"].iloc[-1] == pytest.approx(expected_advantage)
    assert market_frame["cumulative_brier_advantage"].iloc[-1] == pytest.approx(expected_advantage)


def test_save_joint_portfolio_backtest_report_disambiguates_duplicate_market_labels(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
                Side=SimpleNamespace(YES="yes", NO="no"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "same-market",
                "instrument_id": "PM-SAME-YES.POLYMARKET",
                "outcome": "Yes",
                "fills": 0,
                "pnl": 1.0,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.40),
                    ("2026-03-14T17:58:40+00:00", 0.42),
                ],
                "equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 101.0),
                ],
                "cash_series": [
                    ("2026-03-14T17:57:40+00:00", 96.0),
                    ("2026-03-14T17:58:40+00:00", 96.0),
                ],
                "joint_portfolio_equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 103.0),
                ],
                "joint_portfolio_cash_series": [
                    ("2026-03-14T17:57:40+00:00", 96.0),
                    ("2026-03-14T17:58:40+00:00", 96.0),
                ],
            },
            {
                "slug": "same-market",
                "instrument_id": "PM-SAME-NO.POLYMARKET",
                "outcome": "No",
                "fills": 0,
                "pnl": 2.0,
                "price_series": [
                    ("2026-03-14T17:57:40+00:00", 0.60),
                    ("2026-03-14T17:58:40+00:00", 0.58),
                ],
                "equity_series": [
                    ("2026-03-14T17:57:40+00:00", 100.0),
                    ("2026-03-14T17:58:40+00:00", 102.0),
                ],
                "cash_series": [
                    ("2026-03-14T17:57:40+00:00", 99.0),
                    ("2026-03-14T17:58:40+00:00", 99.0),
                ],
            },
        ],
        output_path=tmp_path / "joint_duplicate_labels.html",
        title="joint duplicate labels",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("yes_price", "equity", "cash_equity"),
    )

    result_kwargs = captured_results[0]
    expected_labels = {"same-market (Yes)", "same-market (No)"}
    assert set(result_kwargs["market_prices"]) == expected_labels
    assert set(result_kwargs["overlay_series"]["equity"]) == expected_labels
    assert set(result_kwargs["overlay_series"]["cash"]) == expected_labels
    assert result_kwargs["market_pnls"] == {
        "same-market (Yes)": 1.0,
        "same-market (No)": 2.0,
    }


def test_save_joint_portfolio_backtest_report_stops_counting_settled_market_active(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    times = [
        "2026-03-14T12:00:00+00:00",
        "2026-03-14T12:01:00+00:00",
        "2026-03-14T12:02:00+00:00",
        "2026-03-14T12:03:00+00:00",
    ]
    save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "settled-market",
                "fills": 1,
                "pnl": 1.0,
                "settlement_pnl_applied": True,
                "settlement_series_time": times[1],
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
                "joint_portfolio_equity_series": [(ts, 200.0) for ts in times],
                "joint_portfolio_cash_series": [(ts, 200.0) for ts in times],
            },
            {
                "slug": "open-market",
                "fills": 1,
                "pnl": 0.0,
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
            },
        ],
        output_path=tmp_path / "joint_settled_active_count.html",
        title="joint settled active count",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("equity", "cash_equity"),
    )

    snapshots = captured_results[0]["equity_curve"]
    counts_by_time = {
        snapshot.timestamp.replace(tzinfo=UTC).isoformat(): snapshot.num_positions
        for snapshot in snapshots
    }
    assert counts_by_time == {
        times[0]: 2,
        times[1]: 1,
        times[2]: 1,
        times[3]: 1,
    }
    settled_overlay = captured_results[0]["overlay_series"]["equity"]["settled-market"]
    assert settled_overlay.iloc[0] == pytest.approx(100.0)
    assert settled_overlay.iloc[1] == pytest.approx(100.0)
    assert settled_overlay.iloc[2:].isna().all()


def test_save_joint_portfolio_backtest_report_starts_active_count_at_first_fill(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    times = [
        "2026-03-14T12:00:00+00:00",
        "2026-03-14T12:01:00+00:00",
        "2026-03-14T12:02:00+00:00",
    ]
    save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "filled-after-start",
                "fills": 1,
                "pnl": 1.0,
                "settlement_pnl_applied": True,
                "settlement_series_time": times[2],
                "fill_events": [
                    {
                        "action": "buy",
                        "side": "yes",
                        "price": 0.5,
                        "quantity": 1.0,
                        "timestamp": times[1],
                    }
                ],
                "price_series": [(ts, 0.5) for ts in times],
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
                "joint_portfolio_equity_series": [(ts, 100.0) for ts in times],
                "joint_portfolio_cash_series": [(ts, 100.0) for ts in times],
            }
        ],
        output_path=tmp_path / "joint_fill_start_active_count.html",
        title="joint fill start active count",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("equity", "cash_equity"),
    )

    snapshots = captured_results[0]["equity_curve"]
    counts_by_time = {
        snapshot.timestamp.replace(tzinfo=UTC).isoformat(): snapshot.num_positions
        for snapshot in snapshots
    }
    assert counts_by_time[times[0]] == 0
    assert counts_by_time[times[1]] == 1
    assert counts_by_time[times[2]] == 0


def test_save_joint_portfolio_backtest_report_does_not_count_no_fill_market_as_position(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    times = [
        "2026-03-14T12:00:00+00:00",
        "2026-03-14T12:01:00+00:00",
        "2026-03-14T12:02:00+00:00",
    ]
    save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "traded-market",
                "fills": 1,
                "pnl": 1.0,
                "settlement_pnl_applied": True,
                "settlement_series_time": times[1],
                "fill_events": [
                    {
                        "action": "buy",
                        "side": "yes",
                        "price": 0.5,
                        "quantity": 1.0,
                        "timestamp": times[0],
                    }
                ],
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
                "joint_portfolio_equity_series": [(ts, 200.0) for ts in times],
                "joint_portfolio_cash_series": [(ts, 200.0) for ts in times],
            },
            {
                "slug": "no-fill-market",
                "fills": 0,
                "pnl": 0.0,
                "fill_events": [],
                "price_series": [(ts, 0.5) for ts in times],
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
            },
        ],
        output_path=tmp_path / "joint_no_fill_position_count.html",
        title="joint no-fill position count",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("equity", "cash_equity"),
    )

    snapshots = captured_results[0]["equity_curve"]
    counts_by_time = {
        snapshot.timestamp.replace(tzinfo=UTC).isoformat(): snapshot.num_positions
        for snapshot in snapshots
    }
    assert counts_by_time == {
        times[0]: 1,
        times[1]: 0,
        times[2]: 0,
    }


def test_save_aggregate_backtest_report_does_not_count_no_fill_market_as_position(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "_build_summary_brier_extra_panels",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    times = [
        "2026-03-14T12:00:00+00:00",
        "2026-03-14T12:01:00+00:00",
        "2026-03-14T12:02:00+00:00",
    ]
    save_aggregate_backtest_report(
        results=[
            {
                "slug": "traded-market",
                "fills": 1,
                "pnl": 1.0,
                "settlement_pnl_applied": True,
                "settlement_series_time": times[1],
                "fill_events": [
                    {
                        "action": "buy",
                        "side": "yes",
                        "price": 0.5,
                        "quantity": 1.0,
                        "timestamp": times[0],
                    }
                ],
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
                "pnl_series": [(ts, 0.0) for ts in times],
            },
            {
                "slug": "no-fill-market",
                "fills": 0,
                "pnl": 0.0,
                "fill_events": [],
                "price_series": [(ts, 0.5) for ts in times],
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
                "pnl_series": [(ts, 0.0) for ts in times],
            },
        ],
        output_path=tmp_path / "aggregate_no_fill_position_count.html",
        title="aggregate no-fill position count",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("equity", "cash_equity"),
    )

    snapshots = captured_results[0]["equity_curve"]
    counts_by_time = {
        snapshot.timestamp.replace(tzinfo=UTC).isoformat(): snapshot.num_positions
        for snapshot in snapshots
    }
    assert counts_by_time[times[0]] == 1
    assert counts_by_time[times[1]] == 0
    assert counts_by_time[times[2]] == 0


def test_save_aggregate_backtest_report_starts_active_count_at_first_fill(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    captured_results: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            captured_results.append(kwargs)

    def _snapshot(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (
            SimpleNamespace(
                BacktestResult=_BacktestResult,
                PortfolioSnapshot=_snapshot,
                Platform=SimpleNamespace(POLYMARKET="POLYMARKET"),
            ),
            SimpleNamespace(plot=lambda *args, **kwargs: object()),
        ),
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research,
        "_build_summary_brier_extra_panels",
        lambda **kwargs: {},
    )
    monkeypatch.setattr(
        research,
        "save_legacy_backtest_layout",
        lambda layout, output_path, title: str(output_path),
    )

    times = [
        "2026-03-14T12:00:00+00:00",
        "2026-03-14T12:01:00+00:00",
        "2026-03-14T12:02:00+00:00",
    ]
    save_aggregate_backtest_report(
        results=[
            {
                "slug": "filled-after-start",
                "fills": 1,
                "pnl": 1.0,
                "settlement_pnl_applied": True,
                "settlement_series_time": times[2],
                "fill_events": [
                    {
                        "action": "buy",
                        "side": "yes",
                        "price": 0.5,
                        "quantity": 1.0,
                        "timestamp": times[1],
                    }
                ],
                "price_series": [(ts, 0.5) for ts in times],
                "equity_series": [(ts, 100.0) for ts in times],
                "cash_series": [(ts, 100.0) for ts in times],
                "pnl_series": [(ts, 0.0) for ts in times],
            }
        ],
        output_path=tmp_path / "aggregate_fill_start_active_count.html",
        title="aggregate fill start active count",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=("equity", "cash_equity"),
    )

    snapshots = captured_results[0]["equity_curve"]
    counts_by_time = {
        snapshot.timestamp.replace(tzinfo=UTC).isoformat(): snapshot.num_positions
        for snapshot in snapshots
    }
    assert counts_by_time[times[0]] == 0
    assert counts_by_time[times[1]] == 1
    assert counts_by_time[times[2]] == 0


def test_save_joint_portfolio_backtest_report_renders_market_overlay_labels(tmp_path) -> None:
    pytest.importorskip("bokeh")

    timestamps = [
        (datetime(2026, 3, 14, 18, tzinfo=UTC) + timedelta(minutes=index)).isoformat()
        for index in range(65)
    ]

    def _series(start: float, step: float) -> list[tuple[str, float]]:
        return [(ts, start + index * step) for index, ts in enumerate(timestamps)]

    output_path = tmp_path / "joint_with_rendered_overlays.html"
    report_path = save_joint_portfolio_backtest_report(
        results=[
            {
                "slug": "market-a",
                "fills": 1,
                "pnl": 2.0,
                "price_series": [
                    (ts, 0.40 + (index % 3) * 0.01) for index, ts in enumerate(timestamps)
                ],
                "user_probability_series": [
                    (ts, 0.41 + (index % 3) * 0.01) for index, ts in enumerate(timestamps)
                ],
                "market_probability_series": [
                    (ts, 0.40 + (index % 3) * 0.01) for index, ts in enumerate(timestamps)
                ],
                "outcome_series": [(ts, 1.0) for ts in timestamps],
                "equity_series": _series(100.0, 0.10),
                "cash_series": _series(96.0, 0.01),
                "joint_portfolio_equity_series": _series(200.0, 0.05),
                "joint_portfolio_cash_series": _series(195.0, 0.02),
            },
            {
                "slug": "market-b",
                "fills": 1,
                "pnl": -1.0,
                "price_series": [
                    (ts, 0.60 - (index % 3) * 0.01) for index, ts in enumerate(timestamps)
                ],
                "user_probability_series": [
                    (ts, 0.59 - (index % 3) * 0.01) for index, ts in enumerate(timestamps)
                ],
                "market_probability_series": [
                    (ts, 0.60 - (index % 3) * 0.01) for index, ts in enumerate(timestamps)
                ],
                "outcome_series": [(ts, 0.0) for ts in timestamps],
                "equity_series": _series(100.0, -0.05),
                "cash_series": _series(103.0, -0.01),
            },
        ],
        output_path=output_path,
        title="joint with rendered overlays",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=(
            "total_equity",
            "equity",
            "total_drawdown",
            "drawdown",
            "total_rolling_sharpe",
            "rolling_sharpe",
            "total_cash_equity",
            "cash_equity",
        ),
    )

    assert report_path == str(output_path.resolve())
    html = output_path.read_text(encoding="utf-8")
    assert "market-a equity" in html
    assert "market-b equity" in html
    assert "market-a cash" in html
    assert "market-b cash" in html
    assert "eq_overlay" in html
    assert "dd_overlay" in html
    assert "sharpe_overlay" in html
    assert "cash_eq_overlay" in html
    assert "cash_overlay" in html


def test_dense_market_account_series_from_fill_events_marks_isolated_market_value() -> None:
    equity, cash = research._dense_market_account_series_from_fill_events(
        market_id="market-a",
        market_prices=[
            ("2026-03-14T17:57:40+00:00", 0.40),
            ("2026-03-14T17:58:40+00:00", 0.50),
        ],
        fill_events=[
            {
                "order_id": "fill-a",
                "market_id": "market-a",
                "action": "buy",
                "side": "yes",
                "price": 0.40,
                "quantity": 10.0,
                "timestamp": "2026-03-14T17:57:40+00:00",
                "commission": 0.0,
            }
        ],
        initial_cash=100.0,
    )

    assert list(cash.round(6)) == [96.0, 96.0]
    assert list(equity.round(6)) == [100.0, 101.0]
