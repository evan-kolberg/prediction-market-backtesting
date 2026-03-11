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

"""
Prediction market strategy examples.
"""

from nautilus_trader.examples.strategies.prediction_market.breakout import BarBreakoutConfig
from nautilus_trader.examples.strategies.prediction_market.breakout import BarBreakoutStrategy
from nautilus_trader.examples.strategies.prediction_market.breakout import TradeTickBreakoutConfig
from nautilus_trader.examples.strategies.prediction_market.breakout import TradeTickBreakoutStrategy
from nautilus_trader.examples.strategies.prediction_market.deep_value import (
    TradeTickDeepValueHoldConfig,
)
from nautilus_trader.examples.strategies.prediction_market.deep_value import (
    TradeTickDeepValueHoldStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.ema_crossover import (
    BarEMACrossoverConfig,
)
from nautilus_trader.examples.strategies.prediction_market.ema_crossover import (
    BarEMACrossoverStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.ema_crossover import (
    TradeTickEMACrossoverConfig,
)
from nautilus_trader.examples.strategies.prediction_market.ema_crossover import (
    TradeTickEMACrossoverStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.final_period_momentum import (
    BarFinalPeriodMomentumConfig,
)
from nautilus_trader.examples.strategies.prediction_market.final_period_momentum import (
    BarFinalPeriodMomentumStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.final_period_momentum import (
    TradeTickFinalPeriodMomentumConfig,
)
from nautilus_trader.examples.strategies.prediction_market.final_period_momentum import (
    TradeTickFinalPeriodMomentumStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.mean_reversion import (
    BarMeanReversionConfig,
)
from nautilus_trader.examples.strategies.prediction_market.mean_reversion import (
    BarMeanReversionStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.mean_reversion import (
    TradeTickMeanReversionConfig,
)
from nautilus_trader.examples.strategies.prediction_market.mean_reversion import (
    TradeTickMeanReversionStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.panic_fade import BarPanicFadeConfig
from nautilus_trader.examples.strategies.prediction_market.panic_fade import BarPanicFadeStrategy
from nautilus_trader.examples.strategies.prediction_market.panic_fade import (
    TradeTickPanicFadeConfig,
)
from nautilus_trader.examples.strategies.prediction_market.panic_fade import (
    TradeTickPanicFadeStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.rsi_reversion import (
    BarRSIReversionConfig,
)
from nautilus_trader.examples.strategies.prediction_market.rsi_reversion import (
    BarRSIReversionStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.rsi_reversion import (
    TradeTickRSIReversionConfig,
)
from nautilus_trader.examples.strategies.prediction_market.rsi_reversion import (
    TradeTickRSIReversionStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.threshold_momentum import (
    BarThresholdMomentumConfig,
)
from nautilus_trader.examples.strategies.prediction_market.threshold_momentum import (
    BarThresholdMomentumStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.threshold_momentum import (
    TradeTickThresholdMomentumConfig,
)
from nautilus_trader.examples.strategies.prediction_market.threshold_momentum import (
    TradeTickThresholdMomentumStrategy,
)
from nautilus_trader.examples.strategies.prediction_market.vwap_reversion import (
    TradeTickVWAPReversionConfig,
)
from nautilus_trader.examples.strategies.prediction_market.vwap_reversion import (
    TradeTickVWAPReversionStrategy,
)


__all__ = [
    "BarBreakoutConfig",
    "BarBreakoutStrategy",
    "BarEMACrossoverConfig",
    "BarEMACrossoverStrategy",
    "BarFinalPeriodMomentumConfig",
    "BarFinalPeriodMomentumStrategy",
    "BarMeanReversionConfig",
    "BarMeanReversionStrategy",
    "BarPanicFadeConfig",
    "BarPanicFadeStrategy",
    "BarRSIReversionConfig",
    "BarRSIReversionStrategy",
    "TradeTickBreakoutConfig",
    "TradeTickBreakoutStrategy",
    "TradeTickDeepValueHoldConfig",
    "TradeTickDeepValueHoldStrategy",
    "TradeTickEMACrossoverConfig",
    "TradeTickEMACrossoverStrategy",
    "TradeTickFinalPeriodMomentumConfig",
    "TradeTickFinalPeriodMomentumStrategy",
    "TradeTickMeanReversionConfig",
    "TradeTickMeanReversionStrategy",
    "TradeTickPanicFadeConfig",
    "TradeTickPanicFadeStrategy",
    "TradeTickRSIReversionConfig",
    "TradeTickRSIReversionStrategy",
    "BarThresholdMomentumConfig",
    "BarThresholdMomentumStrategy",
    "TradeTickThresholdMomentumConfig",
    "TradeTickThresholdMomentumStrategy",
    "TradeTickVWAPReversionConfig",
    "TradeTickVWAPReversionStrategy",
]
