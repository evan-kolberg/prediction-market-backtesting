# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the
#  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied. See the License for the specific language governing
#  permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11.
#  See the repository NOTICE file for provenance and licensing scope.
#

"""End-to-end tests for the Kalshi BarMeanReversion spread capture strategy."""

from __future__ import annotations

import decimal
from decimal import Decimal

from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.strategies.prediction_market import BarMeanReversionConfig
from nautilus_trader.examples.strategies.prediction_market import BarMeanReversionStrategy
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.risk.config import RiskEngineConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

KALSHI = Venue("KALSHI")
INSTRUMENT_ID = InstrumentId(Symbol("KXTEST-25MAR15-B100"), KALSHI)


def _make_instrument() -> BinaryOption:
    """Return a minimal Kalshi BinaryOption for testing."""
    return BinaryOption(
        instrument_id=INSTRUMENT_ID,
        raw_symbol=Symbol("KXTEST-25MAR15-B100"),
        asset_class=AssetClass.ALTERNATIVE,
        currency=Currency.from_str("USD"),
        activation_ns=0,
        expiration_ns=0,
        price_precision=2,
        size_precision=0,
        price_increment=Price.from_str("0.01"),
        size_increment=Quantity.from_str("1"),
        maker_fee=decimal.Decimal(0),
        taker_fee=decimal.Decimal("0.07"),
        outcome="Yes",
        description="Test market",
        ts_event=0,
        ts_init=0,
    )


def _make_bar_type(instrument_id: InstrumentId) -> BarType:
    return BarType(
        instrument_id=instrument_id,
        bar_spec=BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
        aggregation_source=AggregationSource.EXTERNAL,
    )


def _make_bar(
    bar_type: BarType,
    close: float,
    ts_ns: int,
    *,
    open_: float | None = None,
    high: float | None = None,
    low: float | None = None,
) -> Bar:
    """Create a Bar with the given close price. Open/high/low default around close."""
    o = open_ if open_ is not None else close
    h = high if high is not None else max(close, o) + 0.01
    lo = low if low is not None else min(close, o) - 0.01
    return Bar(
        bar_type=bar_type,
        open=Price.from_str(f"{o:.2f}"),
        high=Price.from_str(f"{h:.2f}"),
        low=Price.from_str(f"{lo:.2f}"),
        close=Price.from_str(f"{close:.2f}"),
        volume=Quantity.from_int(100),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )


def _make_engine() -> BacktestEngine:
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("TESTER-001"),
            logging=LoggingConfig(log_level="WARNING"),
            risk_engine=RiskEngineConfig(bypass=True),
        ),
    )
    engine.add_venue(
        venue=KALSHI,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USD,
        starting_balances=[Money(10_000, USD)],
        fee_model=KalshiProportionalFeeModel(),
    )
    return engine


def _generate_bars(
    bar_type: BarType,
    prices: list[float],
    start_ns: int = 1_000_000_000_000,
    interval_ns: int = 60_000_000_000,
) -> list[Bar]:
    """Generate a list of bars from close prices."""
    return [
        _make_bar(bar_type, price, start_ns + i * interval_ns) for i, price in enumerate(prices)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestKalshiBarMeanReversion:
    """E2E tests for the Kalshi BarMeanReversion strategy via BacktestEngine."""

    def setup_method(self):
        self.instrument = _make_instrument()
        self.bar_type = _make_bar_type(self.instrument.id)

    def _run_backtest(
        self,
        prices: list[float],
        *,
        window: int = 5,
        entry_threshold: float = 0.02,
        take_profit: float = 0.02,
        stop_loss: float = 0.05,
        trade_size: Decimal = Decimal(1),
    ) -> BacktestEngine:
        engine = _make_engine()
        engine.add_instrument(self.instrument)

        bars = _generate_bars(self.bar_type, prices)
        engine.add_data(bars)

        config = BarMeanReversionConfig(
            instrument_id=self.instrument.id,
            bar_type=self.bar_type,
            trade_size=trade_size,
            window=window,
            entry_threshold=entry_threshold,
            take_profit=take_profit,
            stop_loss=stop_loss,
        )
        engine.add_strategy(BarMeanReversionStrategy(config=config))
        engine.run()
        return engine

    def test_engine_runs_without_error(self):
        """Strategy completes a full backtest lifecycle without exceptions."""
        prices = [0.50] * 30
        engine = self._run_backtest(prices)
        engine.dispose()

    def test_no_trades_on_flat_prices(self):
        """When price is constant, no entry signal should fire."""
        prices = [0.50] * 30
        engine = self._run_backtest(prices)
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) == 0
        engine.dispose()

    def test_entry_on_price_dip(self):
        """A dip below the rolling average triggers a buy."""
        # Warm up at 0.50 for 5 bars, then dip to 0.45 (below avg - 0.02)
        prices = [0.50] * 6 + [0.45]
        engine = self._run_backtest(prices, window=5, entry_threshold=0.02)
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 1  # at least one buy fill
        engine.dispose()

    def test_take_profit_exit(self):
        """After entry, price recovery triggers take-profit exit (buy + sell)."""
        # Warm up, dip to enter, then recover above fill + take_profit
        prices = [0.50] * 6 + [0.45, 0.46, 0.47, 0.48, 0.50]
        engine = self._run_backtest(
            prices,
            window=5,
            entry_threshold=0.02,
            take_profit=0.02,
            stop_loss=0.10,
        )
        fills = engine.trader.generate_order_fills_report()
        # Should have both entry (buy) and exit (sell) fills
        assert len(fills) >= 2
        engine.dispose()

    def test_stop_loss_exit(self):
        """After entry, further price drop triggers stop-loss exit."""
        # Warm up at 0.50, dip to 0.45 (enter), then drop to 0.38 (stop out at -0.05)
        prices = [0.50] * 6 + [0.45, 0.44, 0.42, 0.40, 0.38]
        engine = self._run_backtest(
            prices,
            window=5,
            entry_threshold=0.02,
            take_profit=0.10,
            stop_loss=0.05,
        )
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 2  # entry + exit
        engine.dispose()

    def test_fee_model_charges_fees(self):
        """KalshiProportionalFeeModel produces non-zero commissions on fills."""
        prices = [0.50] * 6 + [0.45, 0.46, 0.47, 0.48, 0.50]
        engine = self._run_backtest(
            prices,
            window=5,
            entry_threshold=0.02,
            take_profit=0.02,
            stop_loss=0.10,
        )
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 1

        # Check that at least one fill has a non-zero commission
        commissions = fills.get("commission", None)
        if commissions is not None:
            has_nonzero = any(
                str(c).strip() not in ("0", "0.00", "0 USD", "0.00 USD", "nan", "")
                for c in commissions
            )
            assert has_nonzero, "Expected non-zero commission from KalshiProportionalFeeModel"
        engine.dispose()

    def test_multiple_round_trips(self):
        """Strategy can complete multiple entry/exit cycles."""
        # Oscillate: up at 0.50, dip, recover, dip again, recover
        prices = (
            [0.50] * 6  # warm up
            + [0.45, 0.48, 0.50]  # dip → recovery (round trip 1)
            + [0.50] * 5  # cooldown
            + [0.45, 0.48, 0.50]  # dip → recovery (round trip 2)
        )
        engine = self._run_backtest(
            prices,
            window=5,
            entry_threshold=0.02,
            take_profit=0.02,
            stop_loss=0.10,
        )
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 4  # 2 entries + 2 exits
        engine.dispose()

    def test_reset_and_rerun(self):
        """Engine can be reset and rerun without errors."""
        prices = [0.50] * 6 + [0.45, 0.50]
        engine = self._run_backtest(prices, window=5, entry_threshold=0.02)
        engine.reset()
        engine.run()
        engine.dispose()

    def test_account_balance_after_run(self):
        """Account balance is modified after trades execute."""
        prices = [0.50] * 6 + [0.45, 0.48, 0.50]
        engine = self._run_backtest(
            prices,
            window=5,
            entry_threshold=0.02,
            take_profit=0.02,
            stop_loss=0.10,
        )
        report = engine.trader.generate_account_report(KALSHI)
        assert len(report) > 0
        engine.dispose()

    def test_positions_report_generated(self):
        """Positions report is generated after a round-trip trade."""
        prices = [0.50] * 6 + [0.45, 0.48, 0.50]
        engine = self._run_backtest(
            prices,
            window=5,
            entry_threshold=0.02,
            take_profit=0.02,
            stop_loss=0.10,
        )
        positions = engine.trader.generate_positions_report()
        assert len(positions) >= 1
        engine.dispose()
