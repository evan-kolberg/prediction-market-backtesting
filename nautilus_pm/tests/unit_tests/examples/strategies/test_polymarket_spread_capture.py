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

"""End-to-end tests for the Polymarket SpreadCapture strategy."""

from __future__ import annotations

import decimal
from decimal import Decimal

from nautilus_trader.adapters.polymarket.fee_model import PolymarketFeeModel
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickMeanReversionConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickMeanReversionStrategy
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
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

POLYMARKET = Venue("POLYMARKET")
USDC = Currency.from_str("USDC")
INSTRUMENT_ID = InstrumentId(Symbol("POLYTEST-YES-12345"), POLYMARKET)


def _make_instrument() -> BinaryOption:
    """Return a minimal Polymarket BinaryOption for testing."""
    return BinaryOption(
        instrument_id=INSTRUMENT_ID,
        raw_symbol=Symbol("POLYTEST-YES-12345"),
        asset_class=AssetClass.ALTERNATIVE,
        currency=USDC,
        activation_ns=0,
        expiration_ns=0,
        price_precision=4,
        size_precision=2,
        price_increment=Price.from_str("0.0001"),
        size_increment=Quantity.from_str("0.01"),
        maker_fee=decimal.Decimal(0),
        taker_fee=decimal.Decimal("0.0175"),  # 175 bps → crypto exponent=1
        outcome="Yes",
        description="Test Polymarket market",
        ts_event=0,
        ts_init=0,
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
        venue=POLYMARKET,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USDC,
        starting_balances=[Money(10_000, USDC)],
        fee_model=PolymarketFeeModel(),
    )
    return engine


def _make_trade_tick(
    instrument_id: InstrumentId,
    price: float,
    qty: float,
    ts_ns: int,
    trade_id: str = "1",
) -> TradeTick:
    return TradeTick(
        instrument_id=instrument_id,
        price=Price.from_str(f"{price:.4f}"),
        size=Quantity.from_str(f"{qty:.2f}"),
        aggressor_side=AggressorSide.BUYER,
        trade_id=TradeId(trade_id),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )


def _generate_ticks(
    instrument_id: InstrumentId,
    prices: list[float],
    qty: float = 10.0,
    start_ns: int = 1_000_000_000_000,
    interval_ns: int = 1_000_000_000,
) -> list[TradeTick]:
    """Generate a list of trade ticks from prices."""
    return [
        _make_trade_tick(
            instrument_id,
            price,
            qty,
            start_ns + i * interval_ns,
            trade_id=str(i + 1),
        )
        for i, price in enumerate(prices)
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPolymarketSpreadCapture:
    """E2E tests for the Polymarket SpreadCapture strategy via BacktestEngine."""

    def setup_method(self):
        self.instrument = _make_instrument()

    def _run_backtest(
        self,
        prices: list[float],
        *,
        vwap_window: int = 5,
        entry_threshold: float = 0.005,
        take_profit: float = 0.008,
        stop_loss: float = 0.020,
        trade_size: Decimal = Decimal(10),
    ) -> BacktestEngine:
        engine = _make_engine()
        engine.add_instrument(self.instrument)

        ticks = _generate_ticks(self.instrument.id, prices)
        engine.add_data(ticks)

        config = TradeTickMeanReversionConfig(
            instrument_id=self.instrument.id,
            trade_size=trade_size,
            vwap_window=vwap_window,
            entry_threshold=entry_threshold,
            take_profit=take_profit,
            stop_loss=stop_loss,
        )
        engine.add_strategy(TradeTickMeanReversionStrategy(config=config))
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
        # Warm up at 0.50 for 5 ticks, then dip to 0.490 (below avg - 0.005)
        prices = [0.50] * 6 + [0.490]
        engine = self._run_backtest(prices, vwap_window=5, entry_threshold=0.005)
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 1  # at least one buy fill
        engine.dispose()

    def test_take_profit_exit(self):
        """After entry, price recovery triggers take-profit exit (buy + sell)."""
        # Warm up, dip to enter, then recover above fill + take_profit
        prices = [0.50] * 6 + [0.490, 0.495, 0.500, 0.505]
        engine = self._run_backtest(
            prices,
            vwap_window=5,
            entry_threshold=0.005,
            take_profit=0.008,
            stop_loss=0.050,
        )
        fills = engine.trader.generate_order_fills_report()
        # Should have both entry (buy) and exit (sell) fills
        assert len(fills) >= 2
        engine.dispose()

    def test_stop_loss_exit(self):
        """After entry, further price drop triggers stop-loss exit."""
        # Warm up at 0.50, dip to 0.490 (enter), then keep dropping
        prices = [0.50] * 6 + [0.490, 0.485, 0.480, 0.470, 0.460]
        engine = self._run_backtest(
            prices,
            vwap_window=5,
            entry_threshold=0.005,
            take_profit=0.050,
            stop_loss=0.020,
        )
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 2  # entry + exit
        engine.dispose()

    def test_fee_model_charges_fees(self):
        """PolymarketFeeModel produces non-zero commissions on fills."""
        prices = [0.50] * 6 + [0.490, 0.495, 0.500, 0.505]
        engine = self._run_backtest(
            prices,
            vwap_window=5,
            entry_threshold=0.005,
            take_profit=0.008,
            stop_loss=0.050,
        )
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 1

        commissions = fills.get("commission", None)
        if commissions is not None:
            has_nonzero = any(
                str(c).strip() not in ("0", "0.00", "0 USDC", "0.0000", "0.0000 USDC", "nan", "")
                for c in commissions
            )
            assert has_nonzero, "Expected non-zero commission from PolymarketFeeModel"
        engine.dispose()

    def test_multiple_round_trips(self):
        """Strategy can complete multiple entry/exit cycles."""
        prices = (
            [0.50] * 6  # warm up
            + [0.490, 0.495, 0.500, 0.505]  # dip → recovery (round trip 1)
            + [0.50] * 5  # cooldown
            + [0.490, 0.495, 0.500, 0.505]  # dip → recovery (round trip 2)
        )
        engine = self._run_backtest(
            prices,
            vwap_window=5,
            entry_threshold=0.005,
            take_profit=0.008,
            stop_loss=0.050,
        )
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) >= 4  # 2 entries + 2 exits
        engine.dispose()

    def test_reset_and_rerun(self):
        """Engine can be reset and rerun without errors."""
        prices = [0.50] * 6 + [0.490, 0.505]
        engine = self._run_backtest(prices, vwap_window=5, entry_threshold=0.005)
        engine.reset()
        engine.run()
        engine.dispose()

    def test_account_balance_after_run(self):
        """Account balance is modified after trades execute."""
        prices = [0.50] * 6 + [0.490, 0.495, 0.500, 0.505]
        engine = self._run_backtest(
            prices,
            vwap_window=5,
            entry_threshold=0.005,
            take_profit=0.008,
            stop_loss=0.050,
        )
        report = engine.trader.generate_account_report(POLYMARKET)
        assert len(report) > 0
        engine.dispose()

    def test_positions_report_generated(self):
        """Positions report is generated after a round-trip trade."""
        prices = [0.50] * 6 + [0.490, 0.495, 0.500, 0.505]
        engine = self._run_backtest(
            prices,
            vwap_window=5,
            entry_threshold=0.005,
            take_profit=0.008,
            stop_loss=0.050,
        )
        positions = engine.trader.generate_positions_report()
        assert len(positions) >= 1
        engine.dispose()
