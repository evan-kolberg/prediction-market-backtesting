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

from __future__ import annotations

import pytest

from nautilus_trader.examples.strategies.prediction_market import BarBreakoutConfig
from nautilus_trader.examples.strategies.prediction_market import BarEMACrossoverConfig
from nautilus_trader.examples.strategies.prediction_market import BarMeanReversionConfig
from nautilus_trader.examples.strategies.prediction_market import BarPanicFadeConfig
from nautilus_trader.examples.strategies.prediction_market import BarRSIReversionConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickBreakoutConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickDeepValueHoldConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickEMACrossoverConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickMeanReversionConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickPanicFadeConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickRSIReversionConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickVWAPReversionConfig
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import Venue


INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))
BAR_TYPE = BarType(
    instrument_id=INSTRUMENT_ID,
    bar_spec=BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
    aggregation_source=AggregationSource.EXTERNAL,
)


@pytest.mark.parametrize(
    "config_cls",
    [
        BarMeanReversionConfig,
        BarEMACrossoverConfig,
        BarBreakoutConfig,
        BarRSIReversionConfig,
        BarPanicFadeConfig,
    ],
)
def test_bar_prediction_market_configs_construct(config_cls):
    config = config_cls(
        instrument_id=INSTRUMENT_ID,
        bar_type=BAR_TYPE,
    )
    assert config.instrument_id == INSTRUMENT_ID


@pytest.mark.parametrize(
    "config_cls",
    [
        TradeTickMeanReversionConfig,
        TradeTickEMACrossoverConfig,
        TradeTickBreakoutConfig,
        TradeTickDeepValueHoldConfig,
        TradeTickRSIReversionConfig,
        TradeTickVWAPReversionConfig,
        TradeTickPanicFadeConfig,
    ],
)
def test_trade_tick_prediction_market_configs_construct(config_cls):
    config = config_cls(instrument_id=INSTRUMENT_ID)
    assert config.instrument_id == INSTRUMENT_ID
