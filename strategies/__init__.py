# -------------------------------------------------------------------------------------------------
# Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
# https://nautechsystems.io
#
# Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the specific language governing
# permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
# Derived from NautilusTrader prediction-market example code.
# Modified by Evan Kolberg in this repository on 2026-03-02, 2026-03-11, 2026-03-15, and 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

"""
Prediction market strategy examples.
"""

from strategies.breakout import (
    BarBreakoutConfig,
    BarBreakoutStrategy,
    BookBreakoutConfig,
    BookBreakoutStrategy,
)
from strategies.binary_pair_arbitrage import (
    BookBinaryPairArbitrageConfig,
    BookBinaryPairArbitrageStrategy,
)
from strategies.deep_value import (
    BookDeepValueHoldConfig,
    BookDeepValueHoldStrategy,
)
from strategies.ema_crossover import (
    BarEMACrossoverConfig,
    BarEMACrossoverStrategy,
    BookEMACrossoverConfig,
    BookEMACrossoverStrategy,
)
from strategies.final_period_momentum import (
    BarFinalPeriodMomentumConfig,
    BarFinalPeriodMomentumStrategy,
    BookFinalPeriodMomentumConfig,
    BookFinalPeriodMomentumStrategy,
)
from strategies.late_favorite_limit_hold import (
    BookLateFavoriteLimitHoldConfig,
    BookLateFavoriteLimitHoldStrategy,
    BookLateFavoriteTakerHoldConfig,
    BookLateFavoriteTakerHoldStrategy,
)
from strategies.mean_reversion import (
    BarMeanReversionConfig,
    BarMeanReversionStrategy,
    BookMeanReversionConfig,
    BookMeanReversionStrategy,
)
from strategies.microprice_imbalance import (
    BookMicropriceImbalanceConfig,
    BookMicropriceImbalanceStrategy,
)
from strategies.panic_fade import (
    BarPanicFadeConfig,
    BarPanicFadeStrategy,
    BookPanicFadeConfig,
    BookPanicFadeStrategy,
)
from strategies.profile_replay import (
    BookProfileReplayConfig,
    BookProfileReplayStrategy,
)
from strategies.rsi_reversion import (
    BarRSIReversionConfig,
    BarRSIReversionStrategy,
    BookRSIReversionConfig,
    BookRSIReversionStrategy,
)
from strategies.threshold_momentum import (
    BarThresholdMomentumConfig,
    BarThresholdMomentumStrategy,
    BookThresholdMomentumConfig,
    BookThresholdMomentumStrategy,
)
from strategies.vwap_reversion import (
    BookVWAPReversionConfig,
    BookVWAPReversionStrategy,
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
    "BookBinaryPairArbitrageConfig",
    "BookBinaryPairArbitrageStrategy",
    "BookBreakoutConfig",
    "BookBreakoutStrategy",
    "BookDeepValueHoldConfig",
    "BookDeepValueHoldStrategy",
    "BookEMACrossoverConfig",
    "BookEMACrossoverStrategy",
    "BookFinalPeriodMomentumConfig",
    "BookFinalPeriodMomentumStrategy",
    "BookLateFavoriteLimitHoldConfig",
    "BookLateFavoriteLimitHoldStrategy",
    "BookLateFavoriteTakerHoldConfig",
    "BookLateFavoriteTakerHoldStrategy",
    "BookMeanReversionConfig",
    "BookMeanReversionStrategy",
    "BookMicropriceImbalanceConfig",
    "BookMicropriceImbalanceStrategy",
    "BookPanicFadeConfig",
    "BookPanicFadeStrategy",
    "BookProfileReplayConfig",
    "BookProfileReplayStrategy",
    "BookRSIReversionConfig",
    "BookRSIReversionStrategy",
    "BookThresholdMomentumConfig",
    "BookThresholdMomentumStrategy",
    "BookVWAPReversionConfig",
    "BookVWAPReversionStrategy",
]
