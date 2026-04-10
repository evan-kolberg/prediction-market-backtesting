from __future__ import annotations

from types import SimpleNamespace

import pytest

from prediction_market_extensions.adapters.prediction_market.research import (
    save_aggregate_backtest_report,
)
from prediction_market_extensions.adapters.prediction_market.research import (
    save_joint_portfolio_backtest_report,
)
from prediction_market_extensions.adapters.prediction_market import research
from prediction_market_extensions.analysis.legacy_backtesting.models import (
    PANEL_TOTAL_BRIER_ADVANTAGE,
)


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


def test_save_aggregate_backtest_report_adds_total_brier_placeholder_when_outcomes_missing(
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
        research.legacy_plot_adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        research.legacy_plot_adapter,
        "_build_brier_placeholder_panel",
        lambda message: placeholder_panel,
    )
    monkeypatch.setattr(
        research,
        "_deserialize_fill_events",
        lambda **kwargs: [SimpleNamespace() for _ in kwargs["fill_events"]],
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
        output_path=tmp_path / "aggregate_total_brier_placeholder.html",
        title="aggregate total placeholder chart",
        market_key="slug",
        pnl_label="PnL (USDC)",
        plot_panels=(PANEL_TOTAL_BRIER_ADVANTAGE,),
    )

    assert plot_calls[0]["extra_panels"][PANEL_TOTAL_BRIER_ADVANTAGE] is placeholder_panel


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
    assert apply_calls == [{}]


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
    assert apply_calls == [{}]


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
