from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Iterable

ORDER_INTENT_PREFIX = "pm_order_intent="
VISIBLE_LIQUIDITY_PREFIX = "pm_visible_liquidity="


def _coerce_positive_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0.0:
        return None
    return numeric


def format_order_intent_tag(intent: str) -> str:
    normalized = str(intent).strip().lower()
    return f"{ORDER_INTENT_PREFIX}{normalized}"


def parse_order_intent(tags: Iterable[str] | None) -> str | None:
    if tags is None:
        return None
    for tag in tags:
        if tag.startswith(ORDER_INTENT_PREFIX):
            value = tag[len(ORDER_INTENT_PREFIX) :].strip().lower()
            return value or None
    return None


def format_visible_liquidity_tag(size: object) -> str | None:
    numeric = _coerce_positive_float(size)
    if numeric is None:
        return None
    value = Decimal(str(numeric)).normalize()
    return f"{VISIBLE_LIQUIDITY_PREFIX}{value}"


def parse_visible_liquidity(tags: Iterable[str] | None) -> float | None:
    if tags is None:
        return None
    for tag in tags:
        if not tag.startswith(VISIBLE_LIQUIDITY_PREFIX):
            continue
        raw_value = tag[len(VISIBLE_LIQUIDITY_PREFIX) :].strip()
        try:
            numeric = float(Decimal(raw_value))
        except (InvalidOperation, ValueError):
            return None
        return numeric if numeric > 0.0 else None
    return None


__all__ = [
    "ORDER_INTENT_PREFIX",
    "VISIBLE_LIQUIDITY_PREFIX",
    "format_order_intent_tag",
    "format_visible_liquidity_tag",
    "parse_order_intent",
    "parse_visible_liquidity",
]
