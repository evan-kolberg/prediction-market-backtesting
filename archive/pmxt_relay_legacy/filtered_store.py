from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from typing import Protocol

from aiohttp import web

from pmxt_relay.config import RelayConfig
from pmxt_relay.filtered_types import FilteredHourEntry
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.storage import parse_archive_hour

if TYPE_CHECKING:
    from pmxt_relay.clickhouse import ClickHouseRelay


class FilteredHourStore(Protocol):
    def list_hours(
        self,
        condition_id: str,
        token_id: str,
        *,
        start_hour: str | None = None,
        end_hour: str | None = None,
    ) -> list[FilteredHourEntry]: ...

    def resolve_hour_path(
        self,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> Path | None: ...

    async def serve_hour(
        self,
        request: web.Request,
        *,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> web.StreamResponse | None: ...


def _resolve_path_under_root(root: Path, *parts: str) -> Path | None:
    try:
        resolved_root = root.resolve(strict=False)
        candidate = root.joinpath(*parts).resolve(strict=False)
    except OSError:
        return None

    if not candidate.is_relative_to(resolved_root):
        return None

    return candidate


class FilesystemFilteredHourStore:
    def __init__(self, config: RelayConfig, index: RelayIndex) -> None:
        self._config = config
        self._index = index

    def list_hours(
        self,
        condition_id: str,
        token_id: str,
        *,
        start_hour: str | None = None,
        end_hour: str | None = None,
    ) -> list[FilteredHourEntry]:
        rows = self._index.list_filtered_hours(
            condition_id,
            token_id,
            start_hour=start_hour,
            end_hour=end_hour,
        )
        if rows:
            return [
                FilteredHourEntry(
                    filename=str(row["filename"]),
                    hour=str(row["hour"]),
                    row_count=int(row["row_count"])
                    if row["row_count"] is not None
                    else None,
                    byte_size=int(row["byte_size"])
                    if row["byte_size"] is not None
                    else None,
                )
                for row in rows
            ]

        token_root = self._config.filtered_root / condition_id / token_id
        if not token_root.exists():
            return []

        entries: list[FilteredHourEntry] = []
        for path in sorted(token_root.glob("polymarket_orderbook_*.parquet")):
            hour = parse_archive_hour(path.name).isoformat()
            if start_hour is not None and hour < start_hour:
                continue
            if end_hour is not None and hour > end_hour:
                continue
            try:
                byte_size = path.stat().st_size
            except FileNotFoundError:
                continue
            entries.append(
                FilteredHourEntry(
                    filename=path.name,
                    hour=hour,
                    row_count=None,
                    byte_size=byte_size,
                )
            )
        return entries

    def resolve_hour_path(
        self,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> Path | None:
        return _resolve_path_under_root(
            self._config.filtered_root,
            condition_id,
            token_id,
            filename,
        )

    async def serve_hour(
        self,
        request: web.Request,
        *,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> web.StreamResponse | None:
        path = self.resolve_hour_path(condition_id, token_id, filename)
        if path is None or not path.exists():
            return None
        response = web.FileResponse(path)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


class ClickHouseFilteredHourStore:
    def __init__(self, clickhouse: ClickHouseRelay) -> None:
        self._clickhouse = clickhouse

    def list_hours(
        self,
        condition_id: str,
        token_id: str,
        *,
        start_hour: str | None = None,
        end_hour: str | None = None,
    ) -> list[FilteredHourEntry]:
        return self._clickhouse.list_hours(
            condition_id,
            token_id,
            start_hour=start_hour,
            end_hour=end_hour,
        )

    def resolve_hour_path(
        self,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> Path | None:
        return None

    async def serve_hour(
        self,
        request: web.Request,
        *,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> web.StreamResponse | None:
        return await self._clickhouse.serve_hour(
            request,
            condition_id=condition_id,
            token_id=token_id,
            filename=filename,
        )


def create_filtered_hour_store(
    config: RelayConfig,
    index: RelayIndex,
) -> FilteredHourStore:
    if config.uses_clickhouse_filtered_store:
        from pmxt_relay.clickhouse import ClickHouseRelay

        clickhouse = ClickHouseRelay(config)
        clickhouse.ensure_schema()
        return ClickHouseFilteredHourStore(clickhouse)
    return FilesystemFilteredHourStore(config, index)
