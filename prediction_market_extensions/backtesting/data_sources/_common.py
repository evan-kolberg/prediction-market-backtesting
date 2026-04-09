from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlparse


DISABLED_ENV_VALUES = {"", "0", "false", "no", "off", "none", "disabled"}

_WINDOWS_DRIVE_RE = re.compile(r"^[A-Za-z]:[\\/]")


def env_value(raw: str | None) -> str | None:
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped or None


def is_disabled(raw: str | None) -> bool:
    value = env_value(raw)
    if value is None:
        return False
    return value.casefold() in DISABLED_ENV_VALUES


def looks_like_local_path(value: str) -> bool:
    stripped = value.strip()
    if not stripped:
        return False
    if stripped.startswith(("~", "/", "./", "../")):
        return True
    if _WINDOWS_DRIVE_RE.match(stripped):
        return True
    if "://" in stripped:
        return False
    return False


def normalize_local_path(value: str) -> str:
    return str(Path(value).expanduser())


def normalize_urlish(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("Expected a non-empty URL or host value.")
    if "://" not in stripped:
        stripped = f"https://{stripped}"
    parsed = urlparse(stripped)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"Expected a URL or host, got {value!r}")
    return stripped.rstrip("/")


def trim_url_suffix(url: str, suffixes: tuple[str, ...]) -> str:
    normalized = normalize_urlish(url)
    for suffix in suffixes:
        if normalized.endswith(suffix):
            return normalized[: -len(suffix)].rstrip("/")
    return normalized
