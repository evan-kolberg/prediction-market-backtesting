"""Integration test — full engine run with synthetic data."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.backtesting.engine import Engine
from src.backtesting.feeds.kalshi import KalshiFeed
from src.backtesting.models import Side, TradeEvent
from src.backtesting.strategy import Strategy


class AlwaysBuyLowStrategy(Strategy):
    """Test strategy that buys YES on every new market at 0.30."""

    def __init__(self) -> None:
        super().__init__(name="test_always_buy_low", description="test")
        self._seen: set[str] = set()

    def on_trade(self, trade: TradeEvent) -> None:
        if trade.market_id not in self._seen:
            self.buy_yes(trade.market_id, price=0.30, quantity=5.0)
            self._seen.add(trade.market_id)


class TestEngineRun:
    def test_produces_result(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        strategy = AlwaysBuyLowStrategy()
        engine = Engine(
            feed=feed,
            strategy=strategy,
            initial_cash=1000.0,
            snapshot_interval=3,
            progress=False,
        )
        result = engine.run()
        assert result.strategy_name == "test_always_buy_low"
        assert result.initial_cash == 1000.0
        assert len(result.equity_curve) > 0

    def test_fills_generated(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        strategy = AlwaysBuyLowStrategy()
        engine = Engine(
            feed=feed,
            strategy=strategy,
            initial_cash=1000.0,
            progress=False,
        )
        result = engine.run()
        assert len(result.fills) > 0

    def test_markets_resolved(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        strategy = AlwaysBuyLowStrategy()
        engine = Engine(
            feed=feed,
            strategy=strategy,
            initial_cash=1000.0,
            progress=False,
        )
        result = engine.run()
        assert result.num_markets_resolved > 0

    def test_metrics_computed(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        strategy = AlwaysBuyLowStrategy()
        engine = Engine(
            feed=feed,
            strategy=strategy,
            initial_cash=1000.0,
            snapshot_interval=2,
            progress=False,
        )
        result = engine.run()
        assert "total_return" in result.metrics
        assert "sharpe_ratio" in result.metrics
        assert "max_drawdown" in result.metrics
        assert "num_fills" in result.metrics

    def test_deterministic(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Same data + strategy produces identical results."""
        results = []
        for _ in range(2):
            feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
            strategy = AlwaysBuyLowStrategy()
            engine = Engine(
                feed=feed,
                strategy=strategy,
                initial_cash=1000.0,
                progress=False,
            )
            results.append(engine.run())

        assert results[0].final_equity == pytest.approx(results[1].final_equity)
        assert len(results[0].fills) == len(results[1].fills)


class NoOpStrategy(Strategy):
    """Strategy that does nothing — verifies engine runs with no fills."""

    def __init__(self) -> None:
        super().__init__(name="noop", description="test")

    def on_trade(self, trade: TradeEvent) -> None:
        pass


class TestNoOpRun:
    def test_no_fills(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        engine = Engine(feed=feed, strategy=NoOpStrategy(), initial_cash=1000.0, progress=False)
        result = engine.run()
        assert len(result.fills) == 0
        assert result.final_equity == pytest.approx(1000.0)

    def test_metrics_zero(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        engine = Engine(feed=feed, strategy=NoOpStrategy(), initial_cash=1000.0, progress=False)
        result = engine.run()
        assert result.metrics["total_return"] == pytest.approx(0.0)


class LifecycleTrackingStrategy(Strategy):
    """Tracks lifecycle callbacks for verification."""

    def __init__(self) -> None:
        super().__init__(name="lifecycle_tracker", description="test")
        self.opened: list[str] = []
        self.closed: list[str] = []
        self.resolved: list[tuple[str, Side]] = []
        self.initialized = False
        self.finalized = False

    def initialize(self) -> None:
        self.initialized = True

    def finalize(self) -> None:
        self.finalized = True

    def on_trade(self, trade: TradeEvent) -> None:
        pass

    def on_market_open(self, market) -> None:
        self.opened.append(market.market_id)

    def on_market_close(self, market) -> None:
        self.closed.append(market.market_id)

    def on_market_resolve(self, market, result) -> None:
        self.resolved.append((market.market_id, result))


class TestLifecycle:
    def test_initialize_finalize_called(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        strategy = LifecycleTrackingStrategy()
        engine = Engine(feed=feed, strategy=strategy, initial_cash=1000.0, progress=False)
        engine.run()
        assert strategy.initialized
        assert strategy.finalized

    def test_market_events_fired(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        strategy = LifecycleTrackingStrategy()
        engine = Engine(feed=feed, strategy=strategy, initial_cash=1000.0, progress=False)
        engine.run()
        assert len(strategy.opened) > 0
        assert len(strategy.resolved) > 0


class TestStrategyDiscovery:
    def test_load_example_strategies(self) -> None:
        strategies = Strategy.load()
        assert len(strategies) >= 2
        names = {cls().name for cls in strategies}
        assert "buy_low" in names
        assert "calibration_arb" in names


class TestMarketSample:
    def test_sample_reduces_markets(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Sampling at 50% should see fewer markets than 100%."""
        feed_full = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        result_full = Engine(
            feed=feed_full,
            strategy=AlwaysBuyLowStrategy(),
            initial_cash=1000.0,
            progress=False,
        ).run()

        feed_sampled = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
        result_sampled = Engine(
            feed=feed_sampled,
            strategy=AlwaysBuyLowStrategy(),
            initial_cash=1000.0,
            progress=False,
            market_sample=0.34,
        ).run()

        # 3 markets * 0.34 = 1 market sampled
        assert result_sampled.num_markets_resolved < result_full.num_markets_resolved

    def test_sample_deterministic(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Same seed produces same sample."""
        results = []
        for _ in range(2):
            feed = KalshiFeed(trades_dir=bt_kalshi_trades_dir, markets_dir=bt_kalshi_markets_dir)
            result = Engine(
                feed=feed,
                strategy=AlwaysBuyLowStrategy(),
                initial_cash=1000.0,
                progress=False,
                market_sample=0.5,
                seed=42,
            ).run()
            results.append(result)

        assert results[0].final_equity == pytest.approx(results[1].final_equity)
        assert len(results[0].fills) == len(results[1].fills)


class TestEventLog:
    """Tests for the NautilusTrader-style event log."""

    def _run_with_log(self, trades_dir: Path, markets_dir: Path):
        feed = KalshiFeed(trades_dir=trades_dir, markets_dir=markets_dir)
        strategy = AlwaysBuyLowStrategy()
        engine = Engine(
            feed=feed,
            strategy=strategy,
            initial_cash=1000.0,
            progress=False,
            verbose=False,
        )
        return engine.run()

    def test_event_log_populated(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Event log should contain lines even with verbose=False."""
        result = self._run_with_log(bt_kalshi_trades_dir, bt_kalshi_markets_dir)
        assert len(result.event_log) > 0

    def test_event_log_contains_start_end(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Log should have BACKTEST START and BACKTEST COMPLETE markers."""
        result = self._run_with_log(bt_kalshi_trades_dir, bt_kalshi_markets_dir)
        assert any("BACKTEST START" in line for line in result.event_log)
        assert any("BACKTEST COMPLETE" in line for line in result.event_log)

    def test_event_log_contains_orders(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Log should contain order submitted events."""
        result = self._run_with_log(bt_kalshi_trades_dir, bt_kalshi_markets_dir)
        assert any("Order.SUBMITTED" in line for line in result.event_log)

    def test_event_log_contains_fills(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Log should contain fill events matching actual fills."""
        result = self._run_with_log(bt_kalshi_trades_dir, bt_kalshi_markets_dir)
        fill_lines = [line for line in result.event_log if "Order.FILLED" in line]
        assert len(fill_lines) == len(result.fills)

    def test_event_log_contains_market_events(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Log should contain market open, close, and resolve events."""
        result = self._run_with_log(bt_kalshi_trades_dir, bt_kalshi_markets_dir)
        assert any("Market.OPEN" in line for line in result.event_log)
        assert any("Market.CLOSE" in line for line in result.event_log)
        assert any("Market.RESOLVE" in line for line in result.event_log)

    def test_event_log_contains_positions(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Log should contain position opened/closed events."""
        result = self._run_with_log(bt_kalshi_trades_dir, bt_kalshi_markets_dir)
        assert any("Position.OPENED" in line for line in result.event_log)
        assert any("Position.CLOSED" in line for line in result.event_log)

    def test_event_log_chronological(self, bt_kalshi_trades_dir: Path, bt_kalshi_markets_dir: Path) -> None:
        """Log lines with timestamps should be in non-decreasing order."""
        result = self._run_with_log(bt_kalshi_trades_dir, bt_kalshi_markets_dir)
        timestamps = []
        for line in result.event_log:
            parts = line.split(" [INFO]")
            if parts[0].strip():
                timestamps.append(parts[0].strip())
        assert timestamps == sorted(timestamps)
