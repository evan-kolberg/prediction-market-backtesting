from __future__ import annotations

from decimal import Decimal
from math import isfinite


def require_positive_decimal(name: str, value: Decimal) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def require_positive_int(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {value}")


def require_nonnegative_int(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {value}")


def require_finite_nonnegative_float(name: str, value: float) -> None:
    if not isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if value < 0.0:
        raise ValueError(f"{name} must be >= 0, got {value}")


def require_probability(name: str, value: float) -> None:
    if not isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} must be within [0, 1], got {value}")


def require_percentage(name: str, value: float) -> None:
    require_probability(name, value)


def require_rsi(name: str, value: float) -> None:
    if not isfinite(value):
        raise ValueError(f"{name} must be finite, got {value!r}")
    if not 0.0 <= value <= 100.0:
        raise ValueError(f"{name} must be within [0, 100], got {value}")


def require_less(name: str, left: float | int, other_name: str, right: float | int) -> None:
    if left >= right:
        raise ValueError(f"{name} must be < {other_name}, got {left} >= {right}")
