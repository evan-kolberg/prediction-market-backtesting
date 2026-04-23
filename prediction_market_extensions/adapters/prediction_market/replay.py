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
#  Added by Evan Kolberg in this repository on 2026-04-05.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any

from nautilus_trader.model.enums import AccountType, BookType, OmsType
from nautilus_trader.model.identifiers import InstrumentId, Venue


@dataclass(frozen=True)
class ReplayAdapterKey:
    platform: str
    vendor: str
    data_type: str


@dataclass(frozen=True)
class ReplayWindow:
    start_ns: int | None = None
    end_ns: int | None = None

    def __post_init__(self) -> None:
        if (
            self.start_ns is not None
            and self.end_ns is not None
            and int(self.start_ns) > int(self.end_ns)
        ):
            raise ValueError(
                f"ReplayWindow start_ns must be <= end_ns, got {self.start_ns} > {self.end_ns}"
            )


@dataclass(frozen=True)
class ReplayCoverageStats:
    count: int
    count_key: str
    market_key: str
    market_id: str
    prices: tuple[float, ...] = ()


@dataclass(frozen=True)
class ReplayLoadRequest:
    min_record_count: int = 0
    min_price_range: float = 0.0
    default_lookback_days: float | None = None
    default_lookback_hours: float | None = None
    default_start_time: Any = None
    default_end_time: Any = None


@dataclass(frozen=True)
class ReplayEngineProfile:
    venue: Venue
    oms_type: OmsType
    account_type: AccountType
    base_currency: Any
    fee_model_factory: Any
    fill_model_mode: str = "taker"
    book_type: BookType = BookType.L1_MBP
    liquidity_consumption: bool = False
    queue_model_mode: str = "default"
    latency_policy: str = "external"


@dataclass(frozen=True)
class LoadedReplay:
    replay: Any
    instrument: Any
    records: tuple[Any, ...]
    outcome: str
    realized_outcome: float | None
    metadata: Mapping[str, Any]
    requested_window: ReplayWindow
    loaded_window: ReplayWindow | None
    coverage_stats: ReplayCoverageStats
    catalog: Any | None = None
    instrument_ids: tuple[InstrumentId, ...] = ()

    @property
    def spec(self) -> Any:
        return self.replay

    @property
    def count(self) -> int:
        return self.coverage_stats.count

    @property
    def count_key(self) -> str:
        return self.coverage_stats.count_key

    @property
    def market_key(self) -> str:
        return self.coverage_stats.market_key

    @property
    def market_id(self) -> str:
        return self.coverage_stats.market_id

    @property
    def prices(self) -> tuple[float, ...]:
        return self.coverage_stats.prices


class HistoricalReplayAdapter(ABC):
    @property
    @abstractmethod
    def key(self) -> ReplayAdapterKey:
        raise NotImplementedError

    @property
    @abstractmethod
    def replay_spec_type(self) -> type[Any]:
        raise NotImplementedError

    def build_single_market_replay(self, *, field_values: Mapping[str, Any]) -> Any:
        raise NotImplementedError(
            f"{type(self).__name__} does not support single-market replay construction."
        )

    @abstractmethod
    def configure_sources(self, *, sources: Sequence[str]) -> AbstractContextManager[Any]:
        raise NotImplementedError

    @property
    @abstractmethod
    def engine_profile(self) -> ReplayEngineProfile:
        raise NotImplementedError

    @abstractmethod
    async def load_replay(self, replay: Any, *, request: ReplayLoadRequest) -> LoadedReplay | None:
        raise NotImplementedError


__all__ = [
    "HistoricalReplayAdapter",
    "LoadedReplay",
    "ReplayAdapterKey",
    "ReplayCoverageStats",
    "ReplayEngineProfile",
    "ReplayLoadRequest",
    "ReplayWindow",
]
