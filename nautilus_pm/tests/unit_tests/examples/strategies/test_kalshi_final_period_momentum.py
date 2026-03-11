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

"""End-to-end tests for the Kalshi final-period momentum strategy."""

from __future__ import annotations

import decimal
from decimal import Decimal

from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.engine import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.strategies.prediction_market import (
    TradeTickFinalPeriodMomentumConfig,
)
from nautilus_trader.examples.strategies.prediction_market import (
    TradeTickFinalPeriodMomentumStrategy,
)
from nautilus_trader.model.currencies import USD
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


KALSHI = Venue("KALSHI")
INSTRUMENT_ID = InstrumentId(Symbol("KXTEST-26MAR07-GAME"), KALSHI)
START_NS = 1_000_000_000_000
INTERVAL_NS = 10_000_000_000


def _make_instrument() -> BinaryOption:
    return BinaryOption(
        instrument_id=INSTRUMENT_ID,
        raw_symbol=Symbol("KXTEST-26MAR07-GAME"),
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


def _make_trade_tick(
    instrument_id: InstrumentId,
    price: float,
    ts_ns: int,
    *,
    qty: int = 10,
    trade_id: str = "1",
) -> TradeTick:
    return TradeTick(
        instrument_id=instrument_id,
        price=Price.from_str(f"{price:.2f}"),
        size=Quantity.from_int(qty),
        aggressor_side=AggressorSide.BUYER,
        trade_id=TradeId(trade_id),
        ts_event=ts_ns,
        ts_init=ts_ns,
    )


def _generate_ticks(
    instrument_id: InstrumentId,
    prices: list[float],
    *,
    start_ns: int = START_NS,
    interval_ns: int = INTERVAL_NS,
) -> list[TradeTick]:
    return [
        _make_trade_tick(
            instrument_id,
            price,
            start_ns + i * interval_ns,
            trade_id=str(i + 1),
        )
        for i, price in enumerate(prices)
    ]


class TestKalshiFinalPeriodMomentum:
    def setup_method(self):
        self.instrument = _make_instrument()

    def _run_backtest(
        self,
        prices: list[float],
        *,
        final_period_minutes: int = 1,
        entry_price: float = 0.80,
        take_profit_price: float = 0.92,
        stop_loss_price: float = 0.50,
        trade_size: Decimal = Decimal(1),
    ) -> BacktestEngine:
        engine = _make_engine()
        engine.add_instrument(self.instrument)

        ticks = _generate_ticks(self.instrument.id, prices)
        engine.add_data(ticks)

        close_time_ns = START_NS + (len(prices) - 1) * INTERVAL_NS
        config = TradeTickFinalPeriodMomentumConfig(
            instrument_id=self.instrument.id,
            trade_size=trade_size,
            market_close_time_ns=close_time_ns,
            final_period_minutes=final_period_minutes,
            entry_price=entry_price,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
        )
        engine.add_strategy(TradeTickFinalPeriodMomentumStrategy(config=config))
        engine.run()
        return engine

    def test_engine_runs_without_error(self):
        engine = self._run_backtest([0.30] * 12)
        engine.dispose()

    def test_no_trade_when_entry_never_crosses(self):
        engine = self._run_backtest([0.30, 0.35, 0.40, 0.45, 0.48, 0.50, 0.52, 0.55, 0.58, 0.60])
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) == 0
        engine.dispose()

    def test_cross_from_pre_final_tick_into_final_period_enters(self):
        prices = [
            0.30,
            0.35,
            0.40,
            0.45,
            0.50,
            0.60,
            0.79,
            0.81,
            0.88,
            0.93,
            0.91,
            0.90,
            0.89,
            0.88,
        ]
        engine = self._run_backtest(prices)
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) == 2
        engine.dispose()

    def test_take_profit_exit_only_happens_once(self):
        prices = [0.25, 0.35, 0.50, 0.65, 0.79, 0.81, 0.88, 0.93, 0.70, 0.81, 0.95]
        engine = self._run_backtest(prices)
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) == 2
        engine.dispose()

    def test_stop_loss_exit_only_happens_once(self):
        prices = [0.25, 0.35, 0.50, 0.65, 0.79, 0.81, 0.75, 0.60, 0.49, 0.81, 0.95]
        engine = self._run_backtest(prices)
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) == 2
        engine.dispose()

    def test_cross_before_final_period_does_not_enter(self):
        prices = [0.25, 0.40, 0.55, 0.79, 0.81, 0.82, 0.84, 0.85, 0.86, 0.88, 0.90, 0.91]
        engine = self._run_backtest(prices)
        fills = engine.trader.generate_order_fills_report()
        assert len(fills) == 0
        engine.dispose()
