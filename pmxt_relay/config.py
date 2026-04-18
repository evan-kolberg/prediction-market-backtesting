from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from pmxt_relay.coverage import PMXT_ARCHIVE_START_HOUR


_DEFAULT_RAW_BASE_URLS = ("https://r2v2.pmxt.dev", "https://r2.pmxt.dev")
_DEFAULT_ARCHIVE_LISTING_URL = "https://archive.pmxt.dev/Polymarket/v2"


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


def _env_utc_hour(name: str, default: datetime) -> datetime:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).replace(minute=0, second=0, microsecond=0)


@dataclass(frozen=True)
class ArchiveSource:
    listing_url: str
    raw_base_url: str


def _env_archive_sources(name: str) -> tuple[ArchiveSource, ...]:
    value = os.getenv(name)
    if value is None or not value.strip():
        return ()
    sources: list[ArchiveSource] = []
    for part in value.split(","):
        stripped = part.strip()
        if not stripped:
            continue
        if "|" not in stripped:
            raise ValueError(f"{name} entries must use LISTING_URL|RAW_BASE_URL syntax.")
        listing_url, raw_base_url = (item.strip().rstrip("/") for item in stripped.split("|", 1))
        if not listing_url or not raw_base_url:
            raise ValueError(f"{name} entries must include both listing and raw base URLs.")
        sources.append(ArchiveSource(listing_url=listing_url, raw_base_url=raw_base_url))
    return tuple(sources)


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
    verify_batch_size: int = 50
    trusted_proxy_ips: tuple[str, ...] = ("127.0.0.1", "::1")
    archive_sources: tuple[ArchiveSource, ...] = ()
    raw_base_urls: tuple[str, ...] = ()
    archive_start_hour: datetime = PMXT_ARCHIVE_START_HOUR

    @classmethod
    def from_env(cls) -> RelayConfig:
        default_data_dir = Path.cwd() / ".pmxt-relay"
        data_dir = Path(os.getenv("PMXT_RELAY_DATA_DIR", str(default_data_dir))).expanduser()
        archive_max_pages = _env_int("PMXT_RELAY_ARCHIVE_MAX_PAGES", 0)
        archive_sources = _env_archive_sources("PMXT_RELAY_ARCHIVE_SOURCES")
        configured_raw_base_urls = _env_csv("PMXT_RELAY_RAW_BASE_URLS")
        archive_listing_url = (
            os.getenv("PMXT_RELAY_ARCHIVE_LISTING_URL") or _DEFAULT_ARCHIVE_LISTING_URL
        ).strip()
        raw_base_url = (os.getenv("PMXT_RELAY_RAW_BASE_URL") or "").strip()
        if archive_sources:
            archive_listing_url = archive_sources[0].listing_url
            raw_base_url = archive_sources[0].raw_base_url
        raw_base_urls = configured_raw_base_urls
        if not raw_base_urls and archive_sources:
            raw_base_urls = tuple(source.raw_base_url for source in archive_sources)
        if not raw_base_urls and raw_base_url:
            raw_base_urls = (raw_base_url.rstrip("/"),)
        if not raw_base_urls:
            raw_base_urls = _DEFAULT_RAW_BASE_URLS
        raw_base_urls = tuple(url.rstrip("/") for url in raw_base_urls)
        if not raw_base_url:
            raw_base_url = raw_base_urls[0]
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
            verify_batch_size=max(1, _env_int("PMXT_RELAY_VERIFY_BATCH_SIZE", 50)),
            trusted_proxy_ips=_env_csv("PMXT_RELAY_TRUSTED_PROXY_IPS", ("127.0.0.1", "::1")),
            archive_sources=archive_sources,
            raw_base_urls=raw_base_urls,
            archive_start_hour=_env_utc_hour(
                "PMXT_RELAY_ARCHIVE_START_HOUR", PMXT_ARCHIVE_START_HOUR
            ),
        )

    @property
    def resolved_archive_sources(self) -> tuple[ArchiveSource, ...]:
        return self.archive_sources or (
            ArchiveSource(
                listing_url=self.archive_listing_url.rstrip("/"),
                raw_base_url=self.raw_base_url.rstrip("/"),
            ),
        )

    @property
    def resolved_raw_base_urls(self) -> tuple[str, ...]:
        if self.raw_base_urls:
            return tuple(url.rstrip("/") for url in self.raw_base_urls)
        return tuple(source.raw_base_url.rstrip("/") for source in self.resolved_archive_sources)

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
