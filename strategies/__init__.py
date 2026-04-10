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

from strategies.breakout import BarBreakoutConfig
from strategies.breakout import BarBreakoutStrategy
from strategies.breakout import QuoteTickBreakoutConfig
from strategies.breakout import QuoteTickBreakoutStrategy
from strategies.breakout import TradeTickBreakoutConfig
from strategies.breakout import TradeTickBreakoutStrategy
from strategies.deep_value import QuoteTickDeepValueHoldConfig
from strategies.deep_value import QuoteTickDeepValueHoldStrategy
from strategies.deep_value import TradeTickDeepValueHoldConfig
from strategies.deep_value import TradeTickDeepValueHoldStrategy
from strategies.ema_crossover import BarEMACrossoverConfig
from strategies.ema_crossover import BarEMACrossoverStrategy
from strategies.ema_crossover import QuoteTickEMACrossoverConfig
from strategies.ema_crossover import QuoteTickEMACrossoverStrategy
from strategies.ema_crossover import TradeTickEMACrossoverConfig
from strategies.ema_crossover import TradeTickEMACrossoverStrategy
from strategies.final_period_momentum import BarFinalPeriodMomentumConfig
from strategies.final_period_momentum import BarFinalPeriodMomentumStrategy
from strategies.final_period_momentum import QuoteTickFinalPeriodMomentumConfig
from strategies.final_period_momentum import QuoteTickFinalPeriodMomentumStrategy
from strategies.final_period_momentum import TradeTickFinalPeriodMomentumConfig
from strategies.final_period_momentum import TradeTickFinalPeriodMomentumStrategy
from strategies.late_favorite_limit_hold import QuoteTickLateFavoriteLimitHoldConfig
from strategies.late_favorite_limit_hold import QuoteTickLateFavoriteLimitHoldStrategy
from strategies.late_favorite_limit_hold import TradeTickLateFavoriteLimitHoldConfig
from strategies.late_favorite_limit_hold import TradeTickLateFavoriteLimitHoldStrategy
from strategies.mean_reversion import BarMeanReversionConfig
from strategies.mean_reversion import BarMeanReversionStrategy
from strategies.mean_reversion import QuoteTickMeanReversionConfig
from strategies.mean_reversion import QuoteTickMeanReversionStrategy
from strategies.mean_reversion import TradeTickMeanReversionConfig
from strategies.mean_reversion import TradeTickMeanReversionStrategy
from strategies.panic_fade import BarPanicFadeConfig
from strategies.panic_fade import BarPanicFadeStrategy
from strategies.panic_fade import QuoteTickPanicFadeConfig
from strategies.panic_fade import QuoteTickPanicFadeStrategy
from strategies.panic_fade import TradeTickPanicFadeConfig
from strategies.panic_fade import TradeTickPanicFadeStrategy
from strategies.rsi_reversion import BarRSIReversionConfig
from strategies.rsi_reversion import BarRSIReversionStrategy
from strategies.rsi_reversion import QuoteTickRSIReversionConfig
from strategies.rsi_reversion import QuoteTickRSIReversionStrategy
from strategies.rsi_reversion import TradeTickRSIReversionConfig
from strategies.rsi_reversion import TradeTickRSIReversionStrategy
from strategies.threshold_momentum import BarThresholdMomentumConfig
from strategies.threshold_momentum import BarThresholdMomentumStrategy
from strategies.threshold_momentum import QuoteTickThresholdMomentumConfig
from strategies.threshold_momentum import QuoteTickThresholdMomentumStrategy
from strategies.threshold_momentum import TradeTickThresholdMomentumConfig
from strategies.threshold_momentum import TradeTickThresholdMomentumStrategy
from strategies.vwap_reversion import QuoteTickVWAPReversionConfig
from strategies.vwap_reversion import QuoteTickVWAPReversionStrategy
from strategies.vwap_reversion import TradeTickVWAPReversionConfig
from strategies.vwap_reversion import TradeTickVWAPReversionStrategy


__all__ = [
    "BarBreakoutConfig",
    "BarBreakoutStrategy",
    "BarEMACrossoverConfig",
    "BarEMACrossoverStrategy",
    "QuoteTickBreakoutConfig",
    "QuoteTickBreakoutStrategy",
    "QuoteTickDeepValueHoldConfig",
    "QuoteTickDeepValueHoldStrategy",
    "QuoteTickEMACrossoverConfig",
    "QuoteTickEMACrossoverStrategy",
    "BarFinalPeriodMomentumConfig",
    "BarFinalPeriodMomentumStrategy",
    "BarMeanReversionConfig",
    "BarMeanReversionStrategy",
    "BarPanicFadeConfig",
    "BarPanicFadeStrategy",
    "BarRSIReversionConfig",
    "BarRSIReversionStrategy",
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
    "BarThresholdMomentumConfig",
    "BarThresholdMomentumStrategy",
    "TradeTickThresholdMomentumConfig",
    "TradeTickThresholdMomentumStrategy",
    "TradeTickVWAPReversionConfig",
    "TradeTickVWAPReversionStrategy",
]
