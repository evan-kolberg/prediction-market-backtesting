"""Strip resolution-revealing fields from instrument metadata.

Loaders persist whatever the venue API returns into `BinaryOption.info`. For
already-resolved markets that payload includes the answer (Kalshi `result`,
Polymarket per-token `winner`, settlement values, UMA resolution status, etc.).
A backtest strategy can read `self.cache.instrument(...).info` from `on_start`,
which is a look-ahead vector that silently inflates results.

This module redacts those fields before the instrument is constructed. The
resolution slice is returned separately so the loader can still surface a
realized outcome for post-hoc analytics (Brier scores, settlement PnL).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# Keys whose presence reveals (or strongly implies) the resolved outcome.
# Any field added here must be one a strategy could mine to peek at the answer.
_RESOLUTION_TOP_LEVEL_KEYS: frozenset[str] = frozenset(
    {
        "result",
        "settlement_value",
        "expiration_value",
        "closed",
        "closedTime",
        "uma_resolution_status",
        "umaResolutionStatus",
        "is_50_50_outcome",
        "resolved_by",
        "resolution_source",
    }
)

# Per-token field names that carry resolution flags (Polymarket).
_RESOLUTION_TOKEN_KEYS: frozenset[str] = frozenset({"winner"})


def extract_resolution_metadata(info: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return just the resolution-bearing slice of an info payload.

    The result is a fresh dict suitable for storage on the loader for
    post-backtest analytics. It mirrors any per-token winner flags via a
    parallel `tokens` list so downstream readers can still locate the
    winning outcome.
    """
    if not info:
        return {}

    metadata: dict[str, Any] = {}
    for key in _RESOLUTION_TOP_LEVEL_KEYS:
        if key in info:
            metadata[key] = info[key]

    raw_tokens = info.get("tokens")
    if isinstance(raw_tokens, list):
        slim_tokens: list[dict[str, Any]] = []
        for entry in raw_tokens:
            if not isinstance(entry, Mapping):
                continue
            slim_entry: dict[str, Any] = {}
            outcome = entry.get("outcome")
            if outcome is not None:
                slim_entry["outcome"] = outcome
            for key in _RESOLUTION_TOKEN_KEYS:
                if key in entry:
                    slim_entry[key] = entry[key]
            if len(slim_entry) > 1:  # outcome + at least one resolution flag
                slim_tokens.append(slim_entry)
        if slim_tokens:
            metadata["tokens"] = slim_tokens

    return metadata


def sanitize_info_for_simulation(info: Mapping[str, Any] | None) -> dict[str, Any]:
    """Return a copy of `info` with resolution-revealing fields stripped.

    Resolution keys are removed recursively so nested event/token payloads
    cannot leak the result through `instrument.info`.
    """
    if not info:
        return {}

    return _sanitize_mapping(info)


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _sanitize_mapping(value)
    if isinstance(value, list):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_value(item) for item in value)
    return value


def _sanitize_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    redacted_keys = _RESOLUTION_TOP_LEVEL_KEYS | _RESOLUTION_TOKEN_KEYS
    return {
        key: _sanitize_value(item) for key, item in value.items() if str(key) not in redacted_keys
    }


__all__ = [
    "extract_resolution_metadata",
    "sanitize_info_for_simulation",
]
