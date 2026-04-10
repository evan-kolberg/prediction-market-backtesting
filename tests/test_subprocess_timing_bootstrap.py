from __future__ import annotations

import pickle
from pathlib import Path
from types import SimpleNamespace

from prediction_market_extensions.backtesting import _isolated_replay_runner
from prediction_market_extensions.backtesting import _optimizer


class _FakeSendConn:
    def __init__(self) -> None:
        self.payloads: list[tuple[str, object]] = []
        self.closed = False

    def send(self, payload: tuple[str, object]) -> None:
        self.payloads.append(payload)

    def close(self) -> None:
        self.closed = True


def _install_fake_bootstrap_modules(monkeypatch, calls: list[str]) -> None:
    monkeypatch.setitem(
        __import__("sys").modules,
        "prediction_market_extensions",
        SimpleNamespace(install_commission_patch=lambda: calls.append("commission")),
    )
    monkeypatch.setitem(
        __import__("sys").modules,
        "prediction_market_extensions.backtesting._timing_harness",
        SimpleNamespace(install_timing_harness=lambda: calls.append("timing")),
    )


def test_optimizer_worker_installs_timing_harness(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    _install_fake_bootstrap_modules(monkeypatch, calls)

    class _FakeBacktest:
        def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
            self.kwargs = kwargs

        def run(self) -> dict[str, object]:
            return {"ok": True, "kwargs": self.kwargs}

    monkeypatch.setitem(
        __import__("sys").modules,
        "prediction_market_extensions.backtesting._prediction_market_backtest",
        SimpleNamespace(PredictionMarketBacktest=_FakeBacktest),
    )

    send_conn = _FakeSendConn()
    result_path = tmp_path / "optimizer-worker.pkl"
    _optimizer._default_evaluation_worker({"name": "demo"}, str(result_path), send_conn)

    assert calls == ["commission", "timing"]
    assert send_conn.closed is True
    assert send_conn.payloads == [("ok", str(result_path))]
    assert pickle.loads(result_path.read_bytes()) == {"ok": True, "kwargs": {"name": "demo"}}


def test_isolated_replay_worker_installs_timing_harness(monkeypatch, tmp_path: Path) -> None:
    calls: list[str] = []
    _install_fake_bootstrap_modules(monkeypatch, calls)

    class _FakeBacktest:
        def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
            self.kwargs = kwargs

        async def run_async(self) -> list[dict[str, object]]:
            return [{"ok": True, "kwargs": self.kwargs}]

    monkeypatch.setitem(
        __import__("sys").modules,
        "prediction_market_extensions.backtesting._prediction_market_backtest",
        SimpleNamespace(PredictionMarketBacktest=_FakeBacktest),
    )

    send_conn = _FakeSendConn()
    result_path = tmp_path / "isolated-worker.pkl"
    _isolated_replay_runner._single_replay_worker({"name": "demo"}, str(result_path), send_conn)

    assert calls == ["commission", "timing"]
    assert send_conn.closed is True
    assert send_conn.payloads == [("ok", str(result_path))]
    assert pickle.loads(result_path.read_bytes()) == {"ok": True, "kwargs": {"name": "demo"}}
