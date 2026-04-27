from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pandas as pd
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.identifiers import Venue

from prediction_market_extensions.adapters.prediction_market import research as legacy_research
from prediction_market_extensions.backtesting import _backtest_runtime as runtime_module

EXPECTED_LATENCY_NANOS = {
    "base_latency_nanos": 75_000_000,
    "insert_latency_nanos": 85_000_000,
    "update_latency_nanos": 80_000_000,
    "cancel_latency_nanos": 80_000_000,
}


class _TraderStub:
    def generate_order_fills_report(self) -> pd.DataFrame:
        return pd.DataFrame()

    def generate_positions_report(self) -> pd.DataFrame:
        return pd.DataFrame()


class _EngineResultStub:
    backtest_end: int | None = 0


class _EngineStub:
    instances: list[_EngineStub] = []

    def __init__(self, *, config: object) -> None:
        self.config = config
        self.venues: list[dict[str, object]] = []
        self.data_batches: list[list[object]] = []
        self.strategies: list[object] = []
        self.trader = _TraderStub()
        type(self).instances.append(self)

    def add_venue(self, **kwargs: object) -> None:
        self.venues.append(kwargs)

    def add_instrument(self, instrument: object) -> None:
        self.instrument = instrument

    def add_data(self, records: object) -> None:
        self.data_batches.append(list(records))  # type: ignore[arg-type]

    def add_strategy(self, strategy: object) -> None:
        self.strategies.append(strategy)

    def run(self) -> None:
        self.ran = True

    def get_result(self) -> _EngineResultStub:
        return _EngineResultStub()

    def reset(self) -> None:
        self.reset_called = True

    def dispose(self) -> None:
        self.dispose_called = True


class _StrategyStub:
    def submit_order(self, order: object) -> None:
        self.last_order = order


def _empty_brier_inputs(*args: object, **kwargs: object) -> tuple[pd.Series, pd.Series, pd.Series]:
    del args, kwargs
    return pd.Series(dtype=float), pd.Series(dtype=float), pd.Series(dtype=float)


def _settlement_marker(result: dict[str, Any]) -> dict[str, Any]:
    result["settlement_probe_applied"] = True
    return result


def _latency_values(latency_model: object) -> dict[str, int]:
    return {
        name: int(getattr(latency_model, name))
        for name in (
            "base_latency_nanos",
            "insert_latency_nanos",
            "update_latency_nanos",
            "cancel_latency_nanos",
        )
    }


def _run_helper(monkeypatch, module: object, **overrides: object):  # type: ignore[no-untyped-def]
    _EngineStub.instances = []
    monkeypatch.setattr(module, "BacktestEngine", _EngineStub)
    monkeypatch.setattr(module, "extract_realized_pnl", lambda positions: 0.0)
    monkeypatch.setattr(module, "extract_price_points", lambda records, price_attr: [])
    monkeypatch.setattr(module, "infer_realized_outcome", lambda instrument: 1.0)
    monkeypatch.setattr(module, "build_brier_inputs", _empty_brier_inputs)
    monkeypatch.setattr(module, "build_market_prices", lambda points, resample_rule=None: [])
    monkeypatch.setattr(module, "apply_binary_settlement_pnl", _settlement_marker)

    strategy = _StrategyStub()
    result = module.run_market_backtest(
        market_id="demo-market",
        instrument=SimpleNamespace(id="DEMO.POLYMARKET", outcome="Yes", expiration_ns=2),
        data=[
            SimpleNamespace(ts_init=0, ts_event=0),
            SimpleNamespace(ts_init=1, ts_event=1),
        ],
        strategy=strategy,
        strategy_name="demo",
        output_prefix="tmp/test",
        platform="polymarket",
        venue=Venue("POLYMARKET"),
        base_currency=USDC_POS,
        fee_model=object(),
        initial_cash=100.0,
        probability_window=3,
        price_attr="price",
        count_key="book_events",
        **overrides,
    )
    return result, _EngineStub.instances[0], strategy


def _assert_legacy_book_defaults(engine: _EngineStub, strategy: _StrategyStub) -> None:
    venue_kwargs = engine.venues[0]
    assert venue_kwargs["book_type"] == BookType.L2_MBP
    assert venue_kwargs["liquidity_consumption"] is True
    assert venue_kwargs["queue_position"] is True
    assert venue_kwargs["fill_model"] is None
    assert venue_kwargs["bar_execution"] is False
    assert venue_kwargs["trade_execution"] is True

    latency_model = venue_kwargs["latency_model"]
    assert latency_model is not None
    assert _latency_values(latency_model) == EXPECTED_LATENCY_NANOS

    assert engine.config.logging.log_level == "INFO"
    assert engine.config.risk_engine.bypass is False
    assert getattr(strategy.submit_order, "__name__", "") == "guarded_submit_order"


def test_backtest_runtime_run_market_backtest_uses_repo_l2_defaults(monkeypatch) -> None:
    result, engine, strategy = _run_helper(monkeypatch, runtime_module)

    _assert_legacy_book_defaults(engine, strategy)
    assert result["settlement_probe_applied"] is True


def test_research_run_market_backtest_uses_repo_l2_defaults(monkeypatch) -> None:
    result, engine, strategy = _run_helper(monkeypatch, legacy_research)

    _assert_legacy_book_defaults(engine, strategy)
    assert result["settlement_probe_applied"] is True


def test_legacy_run_market_backtest_allows_explicit_zero_latency(monkeypatch) -> None:
    _, runtime_engine, _ = _run_helper(monkeypatch, runtime_module, latency_model=None)
    _, research_engine, _ = _run_helper(monkeypatch, legacy_research, latency_model=None)

    assert runtime_engine.venues[0]["latency_model"] is None
    assert research_engine.venues[0]["latency_model"] is None
