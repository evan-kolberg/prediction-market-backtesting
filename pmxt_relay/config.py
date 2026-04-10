from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import uuid


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_csv(name: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None:
        return default
    parts = tuple(part.strip() for part in value.split(",") if part.strip())
    return parts or default


@dataclass(frozen=True)
class RelayConfig:
    data_dir: Path
    bind_host: str
    bind_port: int
    archive_listing_url: str
    raw_base_url: str
    poll_interval_secs: int
    http_timeout_secs: int
    archive_stale_pages: int
    archive_max_pages: int | None
    event_retention: int
    api_rate_limit_per_minute: int
    trusted_proxy_ips: tuple[str, ...] = ("127.0.0.1", "::1")

    @classmethod
    def from_env(cls) -> RelayConfig:
        default_data_dir = Path.cwd() / ".pmxt-relay"
        data_dir = Path(os.getenv("PMXT_RELAY_DATA_DIR", str(default_data_dir))).expanduser()
        archive_max_pages = _env_int("PMXT_RELAY_ARCHIVE_MAX_PAGES", 0)
        archive_listing_url = (os.getenv("PMXT_RELAY_ARCHIVE_LISTING_URL") or "").strip()
        raw_base_url = (os.getenv("PMXT_RELAY_RAW_BASE_URL") or "").strip()
        if not archive_listing_url:
            raise ValueError("PMXT_RELAY_ARCHIVE_LISTING_URL is required.")
        if not raw_base_url:
            raise ValueError("PMXT_RELAY_RAW_BASE_URL is required.")
        return cls(
            data_dir=data_dir,
            bind_host=os.getenv("PMXT_RELAY_BIND_HOST", "0.0.0.0"),
            bind_port=_env_int("PMXT_RELAY_BIND_PORT", 8080),
            archive_listing_url=archive_listing_url.rstrip("/"),
            raw_base_url=raw_base_url.rstrip("/"),
            poll_interval_secs=max(60, _env_int("PMXT_RELAY_POLL_INTERVAL_SECS", 900)),
            http_timeout_secs=max(5, _env_int("PMXT_RELAY_HTTP_TIMEOUT_SECS", 60)),
            archive_stale_pages=max(1, _env_int("PMXT_RELAY_ARCHIVE_STALE_PAGES", 1)),
            archive_max_pages=archive_max_pages or None,
            event_retention=max(100, _env_int("PMXT_RELAY_EVENT_RETENTION", 50000)),
            api_rate_limit_per_minute=max(
                0, _env_int("PMXT_RELAY_API_RATE_LIMIT_PER_MINUTE", 2400)
            ),
            trusted_proxy_ips=_env_csv("PMXT_RELAY_TRUSTED_PROXY_IPS", ("127.0.0.1", "::1")),
        )

    @property
    def raw_root(self) -> Path:
        return self.data_dir / "raw"

    @property
    def state_root(self) -> Path:
        return self.data_dir / "state"

    @property
    def tmp_root(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def db_path(self) -> Path:
        return self.state_root / "relay.sqlite3"

    def ensure_directories(self) -> None:
        paths = [self.data_dir, self.raw_root, self.state_root, self.tmp_root]
        for path in paths:
            path.mkdir(parents=True, exist_ok=True)
            self._assert_directory_writable(path)

    @staticmethod
    def _assert_directory_writable(path: Path) -> None:
        probe_path = path / f".relay-write-probe-{uuid.uuid4().hex}"
        try:
            with probe_path.open("wb") as handle:
                handle.write(b"")
        finally:
            probe_path.unlink(missing_ok=True)
