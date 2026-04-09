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

"""
Shared prediction-market adapter helpers.
"""

from prediction_market_extensions.adapters.prediction_market.fill_model import (
    PredictionMarketTakerFillModel,
)
from prediction_market_extensions.adapters.prediction_market.replay import (
    HistoricalReplayAdapter,
)
from prediction_market_extensions.adapters.prediction_market.replay import LoadedReplay
from prediction_market_extensions.adapters.prediction_market.replay import (
    ReplayAdapterKey,
)
from prediction_market_extensions.adapters.prediction_market.replay import (
    ReplayCoverageStats,
)
from prediction_market_extensions.adapters.prediction_market.replay import (
    ReplayEngineProfile,
)
from prediction_market_extensions.adapters.prediction_market.replay import (
    ReplayLoadRequest,
)
from prediction_market_extensions.adapters.prediction_market.replay import ReplayWindow

__all__ = [
    "HistoricalReplayAdapter",
    "LoadedReplay",
    "PredictionMarketTakerFillModel",
    "ReplayAdapterKey",
    "ReplayCoverageStats",
    "ReplayEngineProfile",
    "ReplayLoadRequest",
    "ReplayWindow",
]
