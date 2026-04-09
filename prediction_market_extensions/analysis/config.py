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
Prediction-market-specific tearsheet chart configurations.

These extend the upstream ``nautilus_trader.analysis.config.TearsheetChart`` base
and can be mixed into any ``TearsheetConfig.charts`` list.
"""

from __future__ import annotations

from nautilus_trader.analysis.config import TearsheetChart


class TearsheetPnLChart(TearsheetChart, frozen=True, kw_only=True):
    @property
    def name(self) -> str:
        return "pnl"


class TearsheetAllocationChart(TearsheetChart, frozen=True, kw_only=True):
    @property
    def name(self) -> str:
        return "allocation"


class TearsheetCumulativeBrierAdvantageChart(TearsheetChart, frozen=True, kw_only=True):
    @property
    def name(self) -> str:
        return "cumulative_brier_advantage"
