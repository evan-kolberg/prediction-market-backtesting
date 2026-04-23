from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

from nautilus_trader.backtest.models import LatencyModel

_NANOS_PER_MILLISECOND = 1_000_000


def _validate_milliseconds(*, name: str, value: float) -> None:
    if not isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value!r}")


def _milliseconds_to_nanos(value: float) -> int:
    return int(round(value * _NANOS_PER_MILLISECOND))


@dataclass(frozen=True)
class StaticLatencyConfig:
    base_latency_ms: float = 0.0
    insert_latency_ms: float = 0.0
    update_latency_ms: float = 0.0
    cancel_latency_ms: float = 0.0

    def __post_init__(self) -> None:
        _validate_milliseconds(name="base_latency_ms", value=self.base_latency_ms)
        _validate_milliseconds(name="insert_latency_ms", value=self.insert_latency_ms)
        _validate_milliseconds(name="update_latency_ms", value=self.update_latency_ms)
        _validate_milliseconds(name="cancel_latency_ms", value=self.cancel_latency_ms)

    def build_latency_model(self) -> LatencyModel | None:
        if (
            self.base_latency_ms == 0.0
            and self.insert_latency_ms == 0.0
            and self.update_latency_ms == 0.0
            and self.cancel_latency_ms == 0.0
        ):
            return None

        return LatencyModel(
            base_latency_nanos=_milliseconds_to_nanos(self.base_latency_ms),
            insert_latency_nanos=_milliseconds_to_nanos(self.insert_latency_ms),
            update_latency_nanos=_milliseconds_to_nanos(self.update_latency_ms),
            cancel_latency_nanos=_milliseconds_to_nanos(self.cancel_latency_ms),
        )


@dataclass(frozen=True)
class ExecutionModelConfig:
    queue_position: bool = False
    latency_model: StaticLatencyConfig | None = None
    slippage_ticks: int = 1
    entry_slippage_pct: float = 0.0
    exit_slippage_pct: float = 0.0
    prob_fill_on_limit: float = 0.25

    def __post_init__(self) -> None:
        if self.slippage_ticks < 0:
            raise ValueError(f"slippage_ticks must be >= 0, got {self.slippage_ticks}")
        if self.entry_slippage_pct < 0.0:
            raise ValueError(f"entry_slippage_pct must be >= 0, got {self.entry_slippage_pct}")
        if self.exit_slippage_pct < 0.0:
            raise ValueError(f"exit_slippage_pct must be >= 0, got {self.exit_slippage_pct}")
        if not 0.0 <= self.prob_fill_on_limit <= 1.0:
            raise ValueError(
                f"prob_fill_on_limit must be within [0.0, 1.0], got {self.prob_fill_on_limit}"
            )

    def build_latency_model(self) -> LatencyModel | None:
        if self.latency_model is None:
            return None
        return self.latency_model.build_latency_model()

    def build_fill_model_kwargs(self) -> dict[str, int | float]:
        return {
            "slippage_ticks": self.slippage_ticks,
            "entry_slippage_pct": self.entry_slippage_pct,
            "exit_slippage_pct": self.exit_slippage_pct,
            "prob_fill_on_limit": self.prob_fill_on_limit,
        }


__all__ = ["ExecutionModelConfig", "StaticLatencyConfig"]
