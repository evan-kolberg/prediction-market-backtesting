# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

from pathlib import Path


def _extend_package_path() -> None:
    import nautilus_trader.adapters.polymarket as _polymarket

    package_dir = Path(__file__).resolve().parent
    for root in _polymarket.__path__:
        candidate = Path(root) / "common"
        if candidate.is_dir() and candidate != package_dir:
            candidate_str = str(candidate)
            if candidate_str not in __path__:
                __path__.append(candidate_str)


_extend_package_path()
