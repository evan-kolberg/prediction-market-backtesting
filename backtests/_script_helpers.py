# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations


def parse_csv_env(raw: str) -> list[str]:
    return [value.strip() for value in raw.split(",") if value.strip()]


def parse_bool_env(raw: str, *, default: bool = True) -> bool:
    value = raw.strip().lower()
    if not value:
        return default
    return value not in {"0", "false", "no", "off"}
