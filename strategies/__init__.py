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
#  Derived from NautilusTrader prediction-market example code.
#  Modified by Evan Kolberg in this repository on 2026-03-02, 2026-03-11, 2026-03-15, and 2026-03-16.
#  See the repository NOTICE file for provenance and licensing scope.
#

"""
Prediction market strategy examples.
"""

from strategies.breakout import (
    BarBreakoutConfig,
    BarBreakoutStrategy,
    QuoteTickBreakoutConfig,
    QuoteTickBreakoutStrategy,
    TradeTickBreakoutConfig,
    TradeTickBreakoutStrategy,
)
from strategies.deep_value import (
    QuoteTickDeepValueHoldConfig,
    QuoteTickDeepValueHoldStrategy,
    TradeTickDeepValueHoldConfig,
    TradeTickDeepValueHoldStrategy,
)
from strategies.ema_crossover import (
    BarEMACrossoverConfig,
    BarEMACrossoverStrategy,
    QuoteTickEMACrossoverConfig,
    QuoteTickEMACrossoverStrategy,
    TradeTickEMACrossoverConfig,
    TradeTickEMACrossoverStrategy,
)
from strategies.final_period_momentum import (
    BarFinalPeriodMomentumConfig,
    BarFinalPeriodMomentumStrategy,
    QuoteTickFinalPeriodMomentumConfig,
    QuoteTickFinalPeriodMomentumStrategy,
    TradeTickFinalPeriodMomentumConfig,
    TradeTickFinalPeriodMomentumStrategy,
)
from strategies.late_favorite_limit_hold import (
    QuoteTickLateFavoriteLimitHoldConfig,
    QuoteTickLateFavoriteLimitHoldStrategy,
    TradeTickLateFavoriteLimitHoldConfig,
    TradeTickLateFavoriteLimitHoldStrategy,
)
from strategies.mean_reversion import (
    BarMeanReversionConfig,
    BarMeanReversionStrategy,
    QuoteTickMeanReversionConfig,
    QuoteTickMeanReversionStrategy,
    TradeTickMeanReversionConfig,
    TradeTickMeanReversionStrategy,
)
from strategies.panic_fade import (
    BarPanicFadeConfig,
    BarPanicFadeStrategy,
    QuoteTickPanicFadeConfig,
    QuoteTickPanicFadeStrategy,
    TradeTickPanicFadeConfig,
    TradeTickPanicFadeStrategy,
)
from strategies.rsi_reversion import (
    BarRSIReversionConfig,
    BarRSIReversionStrategy,
    QuoteTickRSIReversionConfig,
    QuoteTickRSIReversionStrategy,
    TradeTickRSIReversionConfig,
    TradeTickRSIReversionStrategy,
)
from strategies.threshold_momentum import (
    BarThresholdMomentumConfig,
    BarThresholdMomentumStrategy,
    QuoteTickThresholdMomentumConfig,
    QuoteTickThresholdMomentumStrategy,
    TradeTickThresholdMomentumConfig,
    TradeTickThresholdMomentumStrategy,
)
from strategies.vwap_reversion import (
    QuoteTickVWAPReversionConfig,
    QuoteTickVWAPReversionStrategy,
    TradeTickVWAPReversionConfig,
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
    "BarThresholdMomentumConfig",
    "BarThresholdMomentumStrategy",
    "QuoteTickBreakoutConfig",
    "QuoteTickBreakoutStrategy",
    "QuoteTickDeepValueHoldConfig",
    "QuoteTickDeepValueHoldStrategy",
    "QuoteTickEMACrossoverConfig",
    "QuoteTickEMACrossoverStrategy",
    "QuoteTickFinalPeriodMomentumConfig",
    "QuoteTickFinalPeriodMomentumStrategy",
    "QuoteTickLateFavoriteLimitHoldConfig",
    "QuoteTickLateFavoriteLimitHoldStrategy",
    "QuoteTickMeanReversionConfig",
    "QuoteTickMeanReversionStrategy",
    "QuoteTickPanicFadeConfig",
    "QuoteTickPanicFadeStrategy",
    "QuoteTickRSIReversionConfig",
    "QuoteTickRSIReversionStrategy",
    "QuoteTickThresholdMomentumConfig",
    "QuoteTickThresholdMomentumStrategy",
    "QuoteTickVWAPReversionConfig",
    "QuoteTickVWAPReversionStrategy",
    "TradeTickBreakoutConfig",
    "TradeTickBreakoutStrategy",
    "TradeTickDeepValueHoldConfig",
    "TradeTickDeepValueHoldStrategy",
    "TradeTickEMACrossoverConfig",
    "TradeTickEMACrossoverStrategy",
    "TradeTickFinalPeriodMomentumConfig",
    "TradeTickFinalPeriodMomentumStrategy",
    "TradeTickLateFavoriteLimitHoldConfig",
    "TradeTickLateFavoriteLimitHoldStrategy",
    "TradeTickMeanReversionConfig",
    "TradeTickMeanReversionStrategy",
    "TradeTickPanicFadeConfig",
    "TradeTickPanicFadeStrategy",
    "TradeTickRSIReversionConfig",
    "TradeTickRSIReversionStrategy",
    "TradeTickThresholdMomentumConfig",
    "TradeTickThresholdMomentumStrategy",
    "TradeTickVWAPReversionConfig",
    "TradeTickVWAPReversionStrategy",
]
