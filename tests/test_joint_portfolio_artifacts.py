from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from prediction_market_extensions.adapters.prediction_market import (
    LoadedReplay,
    ReplayCoverageStats,
    ReplayWindow,
)
from prediction_market_extensions.backtesting.prediction_market import artifacts
from prediction_market_extensions.backtesting.prediction_market.artifacts import (
    PredictionMarketArtifactBuilder,
)


def _loaded_replay(*, market_id: str, instrument_id: str) -> LoadedReplay:
    return LoadedReplay(
        replay=SimpleNamespace(market_slug=market_id),
        instrument=SimpleNamespace(id=instrument_id),
        records=(SimpleNamespace(price=0.40), SimpleNamespace(price=0.50)),
        outcome="Yes",
        realized_outcome=None,
        metadata={},
        requested_window=ReplayWindow(),
        loaded_window=None,
        coverage_stats=ReplayCoverageStats(
            count=2,
            count_key="book_events",
            market_key="slug",
            market_id=market_id,
            prices=(0.40, 0.50),
        ),
    )


def test_joint_portfolio_dense_prices_are_keyed_by_instrument_id(monkeypatch) -> None:
    start = datetime(2026, 3, 14, 17, 57, tzinfo=UTC)
    price_points = [(start, 0.40), (start + timedelta(minutes=1), 0.50)]
    captured: dict[str, object] = {}

    def _fake_dense_account_series_from_engine_for_markets(**kwargs):
        captured["market_prices"] = kwargs["market_prices"]
        index = pd.DatetimeIndex([point[0] for point in price_points])
        return (
            pd.Series([100.0, 101.0], index=index, dtype=float),
            pd.Series([96.0, 96.0], index=index, dtype=float),
        )

    monkeypatch.setattr(artifacts, "extract_price_points", lambda *args, **kwargs: price_points)
    monkeypatch.setattr(artifacts, "downsample_price_points", lambda points, **kwargs: points)
    monkeypatch.setattr(artifacts, "build_market_prices", lambda points, **kwargs: points)
    monkeypatch.setattr(
        artifacts.prediction_market_research,
        "_dense_account_series_from_engine_for_markets",
        _fake_dense_account_series_from_engine_for_markets,
    )

    builder = PredictionMarketArtifactBuilder(
        name="joint-demo",
        platform="polymarket",
        data_type="book",
        initial_cash=100.0,
        probability_window=5,
        chart_resample_rule=None,
        return_summary_series=True,
        sim_count=2,
    )

    result = builder.build_joint_portfolio_artifacts(
        engine=SimpleNamespace(),
        loaded_sims=(
            _loaded_replay(market_id="market-a", instrument_id="PM-A-YES.POLYMARKET"),
            _loaded_replay(market_id="market-b", instrument_id="PM-B-YES.POLYMARKET"),
        ),
    )

    assert set(captured["market_prices"]) == {"PM-A-YES.POLYMARKET", "PM-B-YES.POLYMARKET"}
    assert "market-a" not in captured["market_prices"]
    assert result["joint_portfolio_equity_series"] == [
        (pd.Timestamp(start).isoformat(), 100.0),
        (pd.Timestamp(start + timedelta(minutes=1)).isoformat(), 101.0),
    ]


def test_market_artifacts_are_keyed_by_instrument_id_for_shared_slug(monkeypatch) -> None:
    def _fake_build_market_artifacts_for_loaded_sim(
        self, *, engine, loaded_sim, fills_report, include_portfolio_series
    ):
        return {
            "instrument_id": str(loaded_sim.instrument.id),
            "fill_count": len(fills_report),
        }

    monkeypatch.setattr(
        PredictionMarketArtifactBuilder,
        "_build_market_artifacts_for_loaded_sim",
        _fake_build_market_artifacts_for_loaded_sim,
    )
    builder = PredictionMarketArtifactBuilder(
        name="shared-slug-demo",
        platform="polymarket",
        data_type="book",
        initial_cash=100.0,
        probability_window=5,
        chart_resample_rule=None,
        return_summary_series=True,
        sim_count=2,
    )

    result = builder.build_market_artifacts(
        engine=SimpleNamespace(),
        loaded_sims=(
            _loaded_replay(market_id="shared-slug", instrument_id="PM-SHARED-UP.POLYMARKET"),
            _loaded_replay(market_id="shared-slug", instrument_id="PM-SHARED-DOWN.POLYMARKET"),
        ),
        fills_report=pd.DataFrame(
            {
                "instrument_id": [
                    "PM-SHARED-UP.POLYMARKET",
                    "PM-SHARED-DOWN.POLYMARKET",
                ]
            }
        ),
    )

    assert set(result) == {"PM-SHARED-UP.POLYMARKET", "PM-SHARED-DOWN.POLYMARKET"}
    assert result["PM-SHARED-UP.POLYMARKET"] == {
        "instrument_id": "PM-SHARED-UP.POLYMARKET",
        "fill_count": 1,
    }
    assert result["PM-SHARED-DOWN.POLYMARKET"] == {
        "instrument_id": "PM-SHARED-DOWN.POLYMARKET",
        "fill_count": 1,
    }


def test_market_artifact_filter_accepts_instrument_id_objects() -> None:
    instrument_id = InstrumentId(Symbol("PM-A-YES"), Venue("POLYMARKET"))
    report = pd.DataFrame({"instrument_id": [instrument_id], "last_qty": [5]})

    filtered = PredictionMarketArtifactBuilder._filter_report_rows(
        report, instrument_id=str(instrument_id)
    )

    assert len(filtered) == 1
    assert filtered.iloc[0]["last_qty"] == 5


def test_single_summary_dense_prices_are_keyed_by_instrument_id(monkeypatch) -> None:
    start = datetime(2026, 3, 14, 17, 57, tzinfo=UTC)
    price_points = [(start, 0.40), (start + timedelta(minutes=1), 0.50)]
    captured: dict[str, object] = {}

    def _fake_dense_market_account_series_from_fill_events(**kwargs):
        captured["market_id"] = kwargs["market_id"]
        index = pd.DatetimeIndex([point[0] for point in price_points])
        return (
            pd.Series([100.0, 101.0], index=index, dtype=float),
            pd.Series([96.0, 96.0], index=index, dtype=float),
        )

    monkeypatch.setattr(
        artifacts.prediction_market_research.legacy_plot_adapter,
        "_load_legacy_modules",
        lambda: (SimpleNamespace(), SimpleNamespace()),
    )
    monkeypatch.setattr(
        artifacts.prediction_market_research.legacy_plot_adapter,
        "_convert_fills",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        artifacts.prediction_market_research.legacy_plot_adapter,
        "_market_prices_with_fill_points",
        lambda market_prices, fills: market_prices,
    )
    monkeypatch.setattr(
        artifacts.prediction_market_research,
        "_serialize_fill_events",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        artifacts.prediction_market_research,
        "_dense_market_account_series_from_fill_events",
        _fake_dense_market_account_series_from_fill_events,
    )

    builder = PredictionMarketArtifactBuilder(
        name="single-demo",
        platform="polymarket",
        data_type="book",
        initial_cash=100.0,
        probability_window=5,
        chart_resample_rule=None,
        return_summary_series=True,
        sim_count=1,
    )

    result = builder._build_market_summary_series(
        engine=SimpleNamespace(),
        loaded_sim=_loaded_replay(market_id="market-a", instrument_id="PM-A-YES.POLYMARKET"),
        fills_report=pd.DataFrame(),
        market_prices=price_points,
        user_probabilities=pd.Series(dtype=float),
        market_probabilities=pd.Series(dtype=float),
        outcomes=pd.Series(dtype=float),
        include_portfolio_series=True,
    )

    assert captured["market_id"] == "market-a"
    assert result["equity_series"] == [
        (pd.Timestamp(start).isoformat(), 100.0),
        (pd.Timestamp(start + timedelta(minutes=1)).isoformat(), 101.0),
    ]


def test_build_result_marks_open_positions_to_settlement(monkeypatch) -> None:
    monkeypatch.setattr(artifacts, "extract_realized_pnl", lambda positions: -1.25)
    monkeypatch.setattr(
        artifacts.prediction_market_research,
        "_serialize_fill_events",
        lambda **kwargs: [
            {
                "action": "buy",
                "price": 0.90,
                "quantity": 25.0,
                "commission": 0.0,
            }
        ],
    )

    builder = PredictionMarketArtifactBuilder(
        name="single-demo",
        platform="polymarket",
        data_type="book",
        initial_cash=100.0,
        probability_window=5,
        chart_resample_rule=None,
        return_summary_series=False,
        sim_count=1,
    )

    loaded_sim = _loaded_replay(market_id="market-a", instrument_id="PM-A-YES.POLYMARKET")
    loaded_sim = loaded_sim.__class__(
        **{
            **loaded_sim.__dict__,
            "instrument": SimpleNamespace(id=loaded_sim.instrument.id, expiration_ns=1_000),
            "realized_outcome": 1.0,
        }
    )

    result = builder.build_result(
        loaded_sim=loaded_sim,
        fills_report=pd.DataFrame({"instrument_id": ["PM-A-YES.POLYMARKET"]}),
        positions_report=pd.DataFrame({"instrument_id": ["PM-A-YES.POLYMARKET"]}),
        run_state={
            "simulated_through": "1970-01-01T00:00:00.000001+00:00",
            "planned_end": "1970-01-01T00:00:00.000001+00:00",
        },
    )

    assert result["market_exit_pnl"] == -1.25
    assert result["pnl"] == 2.5
