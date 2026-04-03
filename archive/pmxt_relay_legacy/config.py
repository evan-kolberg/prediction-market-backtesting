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


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if not normalized:
        return default
    return normalized not in {"0", "false", "no", "off"}


@dataclass(frozen=True)
class RelayConfig:
    data_dir: Path
    bind_host: str
    bind_port: int
    public_base_url: str | None
    archive_listing_url: str
    raw_base_url: str
    poll_interval_secs: int
    http_timeout_secs: int
    archive_stale_pages: int
    archive_max_pages: int | None
    duckdb_threads: int
    duckdb_memory_limit: str
    event_retention: int
    api_rate_limit_per_minute: int
    api_list_max_hours: int
    filtered_store_backend: str = "filesystem"
    clickhouse_url: str = "http://127.0.0.1:8123"
    clickhouse_database: str = "pmxt_relay"
    clickhouse_table: str = "filtered_updates"
    clickhouse_user: str | None = None
    clickhouse_password: str | None = None
    clickhouse_timeout_secs: int = 60
    clickhouse_insert_batch_rows: int = 2048
    trusted_proxy_ips: tuple[str, ...] = ("127.0.0.1", "::1")
    filtered_materialization_workers: int = 4
    mirror_only: bool = False

    @classmethod
    def from_env(cls) -> RelayConfig:
        default_data_dir = Path.cwd() / ".pmxt-relay"
        data_dir = Path(
            os.getenv("PMXT_RELAY_DATA_DIR", str(default_data_dir))
        ).expanduser()
        archive_max_pages = _env_int("PMXT_RELAY_ARCHIVE_MAX_PAGES", 0)
        public_base_url = os.getenv("PMXT_RELAY_PUBLIC_BASE_URL")
        if public_base_url is not None:
            public_base_url = public_base_url.rstrip("/")
        return cls(
            data_dir=data_dir,
            bind_host=os.getenv("PMXT_RELAY_BIND_HOST", "0.0.0.0"),
            bind_port=_env_int("PMXT_RELAY_BIND_PORT", 8080),
            public_base_url=public_base_url,
            archive_listing_url=os.getenv(
                "PMXT_RELAY_ARCHIVE_LISTING_URL",
                "https://archive.pmxt.dev/data/Polymarket",
            ).rstrip("/"),
            raw_base_url=os.getenv(
                "PMXT_RELAY_RAW_BASE_URL",
                "https://r2.pmxt.dev",
            ).rstrip("/"),
            poll_interval_secs=max(60, _env_int("PMXT_RELAY_POLL_INTERVAL_SECS", 900)),
            http_timeout_secs=max(5, _env_int("PMXT_RELAY_HTTP_TIMEOUT_SECS", 60)),
            archive_stale_pages=max(1, _env_int("PMXT_RELAY_ARCHIVE_STALE_PAGES", 1)),
            archive_max_pages=archive_max_pages or None,
            duckdb_threads=max(1, _env_int("PMXT_RELAY_DUCKDB_THREADS", 4)),
            duckdb_memory_limit=os.getenv("PMXT_RELAY_DUCKDB_MEMORY_LIMIT", "4GB"),
            event_retention=max(100, _env_int("PMXT_RELAY_EVENT_RETENTION", 50000)),
            api_rate_limit_per_minute=max(
                0,
                _env_int("PMXT_RELAY_API_RATE_LIMIT_PER_MINUTE", 2400),
            ),
            api_list_max_hours=max(
                1,
                _env_int("PMXT_RELAY_API_LIST_MAX_HOURS", 2000),
            ),
            filtered_store_backend=(
                os.getenv("PMXT_RELAY_FILTERED_STORE_BACKEND", "filesystem")
                .strip()
                .casefold()
            ),
            clickhouse_url=os.getenv(
                "PMXT_RELAY_CLICKHOUSE_URL",
                "http://127.0.0.1:8123",
            ).rstrip("/"),
            clickhouse_database=os.getenv(
                "PMXT_RELAY_CLICKHOUSE_DATABASE",
                "pmxt_relay",
            ).strip(),
            clickhouse_table=os.getenv(
                "PMXT_RELAY_CLICKHOUSE_TABLE",
                "filtered_updates",
            ).strip(),
            clickhouse_user=(os.getenv("PMXT_RELAY_CLICKHOUSE_USER") or "").strip()
            or None,
            clickhouse_password=(
                os.getenv("PMXT_RELAY_CLICKHOUSE_PASSWORD") or ""
            ).strip()
            or None,
            clickhouse_timeout_secs=max(
                5,
                _env_int("PMXT_RELAY_CLICKHOUSE_TIMEOUT_SECS", 60),
            ),
            clickhouse_insert_batch_rows=max(
                1024,
                _env_int("PMXT_RELAY_CLICKHOUSE_INSERT_BATCH_ROWS", 2048),
            ),
            trusted_proxy_ips=_env_csv(
                "PMXT_RELAY_TRUSTED_PROXY_IPS",
                ("127.0.0.1", "::1"),
            ),
            filtered_materialization_workers=max(
                1,
                _env_int(
                    "PMXT_RELAY_FILTERED_WORKERS",
                    max(1, _env_int("PMXT_RELAY_DUCKDB_THREADS", 4)),
                ),
            ),
            mirror_only=_env_bool("PMXT_RELAY_MIRROR_ONLY", False),
        )

    @property
    def raw_root(self) -> Path:
        return self.data_dir / "raw"

    @property
    def filtered_root(self) -> Path:
        return self.data_dir / "filtered"

    @property
    def processed_root(self) -> Path:
        return self.data_dir / "processed"

    @property
    def state_root(self) -> Path:
        return self.data_dir / "state"

    @property
    def tmp_root(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def db_path(self) -> Path:
        return self.state_root / "relay.sqlite3"

    @property
    def uses_clickhouse_filtered_store(self) -> bool:
        return self.filtered_store_backend.strip().casefold() == "clickhouse"

    @property
    def processing_enabled(self) -> bool:
        return not self.mirror_only

    @property
    def filtered_api_enabled(self) -> bool:
        return not self.mirror_only

    def ensure_directories(self) -> None:
        paths = [
            self.data_dir,
            self.raw_root,
            self.state_root,
            self.tmp_root,
        ]
        if self.processing_enabled and not self.uses_clickhouse_filtered_store:
            paths.extend((self.filtered_root, self.processed_root))
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
