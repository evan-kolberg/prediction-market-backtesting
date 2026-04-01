from __future__ import annotations

import asyncio
from datetime import datetime
from io import BytesIO
import json
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen

import aiohttp
import pyarrow as pa
import pyarrow.parquet as pq
from aiohttp import web

from pmxt_relay.config import RelayConfig
from pmxt_relay.filtered_types import FilteredHourEntry


CLICKHOUSE_INSERT_SCHEMA = pa.schema(
    [
        ("filename", pa.string()),
        ("hour", pa.timestamp("s", tz="UTC")),
        ("condition_id", pa.string()),
        ("token_id", pa.string()),
        ("relay_row_index", pa.uint64()),
        ("update_type", pa.string()),
        ("data", pa.string()),
    ]
)


class ClickHouseRelay:
    def __init__(self, config: RelayConfig) -> None:
        self._config = config

    @property
    def _database(self) -> str:
        return self._config.clickhouse_database

    @property
    def _table(self) -> str:
        return self._config.clickhouse_table

    @property
    def _hours_table(self) -> str:
        return f"{self._config.clickhouse_table}_hours"

    @property
    def _http_url(self) -> str:
        return self._config.clickhouse_url

    def _request_url(self, *, use_database: bool = True) -> str:
        query_params = ["date_time_input_format=best_effort"]
        if use_database:
            query_params.insert(0, f"database={self._database}")
        return f"{self._http_url}/?{'&'.join(query_params)}"

    def _request_headers(self) -> dict[str, str]:
        headers = {"User-Agent": "pmxt-relay/1.0"}
        if self._config.clickhouse_user is not None:
            headers["X-ClickHouse-User"] = self._config.clickhouse_user
        if self._config.clickhouse_password is not None:
            headers["X-ClickHouse-Key"] = self._config.clickhouse_password
        return headers

    @staticmethod
    def _escape(value: str) -> str:
        return value.replace("\\", "\\\\").replace("'", "\\'")

    def _execute_query(
        self,
        query: str,
        *,
        data: bytes | None = None,
        use_database: bool = True,
    ) -> bytes:
        request = Request(
            self._request_url(use_database=use_database),
            data=(query.encode() + b"\n" + data)
            if data is not None
            else query.encode(),
            headers=self._request_headers(),
            method="POST",
        )
        try:
            with urlopen(
                request, timeout=self._config.clickhouse_timeout_secs
            ) as response:
                return response.read()
        except HTTPError as exc:  # pragma: no cover - error detail path
            detail = exc.read().decode(errors="replace")
            raise RuntimeError(
                f"ClickHouse query failed ({exc.code}): {detail}"
            ) from exc

    def ensure_schema(self) -> None:
        self._execute_query(
            f"CREATE DATABASE IF NOT EXISTS {self._database}",
            use_database=False,
        )
        self._execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self._database}.{self._table} (
                filename String,
                hour DateTime('UTC'),
                condition_id String,
                token_id String,
                relay_row_index UInt64,
                update_type LowCardinality(String),
                data String,
                inserted_at DateTime('UTC') DEFAULT now()
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(hour)
            ORDER BY (condition_id, token_id, hour, relay_row_index)
            """
        )
        self._execute_query(
            f"""
            CREATE TABLE IF NOT EXISTS {self._database}.{self._hours_table} (
                filename String,
                hour DateTime('UTC'),
                filtered_group_count UInt64,
                filtered_row_count UInt64,
                completed_at DateTime('UTC') DEFAULT now()
            )
            ENGINE = ReplacingMergeTree(completed_at)
            ORDER BY filename
            """
        )

    def hour_exists(self, filename: str) -> bool:
        escaped = self._escape(filename)
        payload = self._execute_query(
            f"""
            SELECT count()
            FROM {self._database}.{self._hours_table}
            WHERE filename = '{escaped}'
            FORMAT TabSeparated
            """
        )
        return int(payload.decode().strip() or "0") > 0

    def hour_data_exists(self, filename: str) -> bool:
        escaped = self._escape(filename)
        payload = self._execute_query(
            f"""
            SELECT count()
            FROM {self._database}.{self._table}
            WHERE filename = '{escaped}'
            FORMAT TabSeparated
            """
        )
        return int(payload.decode().strip() or "0") > 0

    def hour_group_count(self, filename: str) -> int:
        escaped = self._escape(filename)
        payload = self._execute_query(
            f"""
            SELECT filtered_group_count
            FROM {self._database}.{self._hours_table}
            WHERE filename = '{escaped}'
            ORDER BY completed_at DESC
            LIMIT 1
            FORMAT TabSeparated
            """
        )
        return int(payload.decode().strip() or "0")

    def reset_hour(self, filename: str) -> None:
        escaped = self._escape(filename)
        self._execute_query(
            f"""
            ALTER TABLE {self._database}.{self._table}
            DELETE WHERE filename = '{escaped}'
            SETTINGS mutations_sync = 2
            """
        )
        self._execute_query(
            f"""
            ALTER TABLE {self._database}.{self._hours_table}
            DELETE WHERE filename = '{escaped}'
            SETTINGS mutations_sync = 2
            """
        )

    def mark_hour_complete(
        self,
        *,
        filename: str,
        hour: str,
        filtered_group_count: int,
        filtered_row_count: int,
    ) -> None:
        escaped_filename = self._escape(filename)
        escaped_hour = self._escape(hour)
        self._execute_query(
            f"""
            INSERT INTO {self._database}.{self._hours_table}
                (filename, hour, filtered_group_count, filtered_row_count)
            VALUES (
                '{escaped_filename}',
                parseDateTimeBestEffort('{escaped_hour}'),
                {filtered_group_count},
                {filtered_row_count}
            )
            """
        )

    def backfill_completed_hour(
        self,
        *,
        filename: str,
        hour: str,
        filtered_group_count: int,
    ) -> None:
        if self.hour_exists(filename) or not self.hour_data_exists(filename):
            return
        escaped_filename = self._escape(filename)
        payload = self._execute_query(
            f"""
            SELECT count()
            FROM {self._database}.{self._table}
            WHERE filename = '{escaped_filename}'
            FORMAT TabSeparated
            """
        )
        row_count = int(payload.decode().strip() or "0")
        if row_count <= 0:
            return
        self.mark_hour_complete(
            filename=filename,
            hour=hour,
            filtered_group_count=filtered_group_count,
            filtered_row_count=row_count,
        )

    def insert_batch(
        self,
        *,
        filename: str,
        hour: str,
        batch: pa.RecordBatch,
    ) -> None:
        if batch.num_rows == 0:
            return

        hour_value = pa.scalar(
            datetime.fromisoformat(hour),
            type=pa.timestamp("s", tz="UTC"),
        )
        filename_values = pa.repeat(
            pa.scalar(filename, type=pa.string()), batch.num_rows
        )
        hour_values = pa.repeat(hour_value, batch.num_rows)
        relay_indices = batch.column("relay_row_index")
        if relay_indices.type != pa.uint64():
            relay_indices = relay_indices.cast(pa.uint64())
        table = pa.Table.from_arrays(
            [
                filename_values,
                hour_values,
                batch.column("market_id"),
                batch.column("token_id"),
                relay_indices,
                batch.column("update_type"),
                batch.column("data"),
            ],
            schema=CLICKHOUSE_INSERT_SCHEMA,
        )
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="zstd")
        self._execute_query(
            f"INSERT INTO {self._database}.{self._table} FORMAT Parquet",
            data=buffer.getvalue(),
        )

    def list_hours(
        self,
        condition_id: str,
        token_id: str,
        *,
        start_hour: str | None = None,
        end_hour: str | None = None,
    ) -> list[FilteredHourEntry]:
        clauses = [
            f"condition_id = '{self._escape(condition_id)}'",
            f"token_id = '{self._escape(token_id)}'",
        ]
        if start_hour is not None:
            clauses.append(
                f"hour >= parseDateTimeBestEffort('{self._escape(start_hour)}')"
            )
        if end_hour is not None:
            clauses.append(
                f"hour <= parseDateTimeBestEffort('{self._escape(end_hour)}')"
            )

        payload = self._execute_query(
            f"""
            SELECT
                filename,
                formatDateTime(hour, '%Y-%m-%dT%H:00:00+00:00', 'UTC') AS hour,
                count() AS row_count
            FROM {self._database}.{self._table}
            WHERE {" AND ".join(clauses)}
            GROUP BY filename, hour
            ORDER BY hour
            FORMAT JSONEachRow
            """
        )
        rows: list[FilteredHourEntry] = []
        for line in payload.decode().splitlines():
            if not line.strip():
                continue
            row = json.loads(line)
            rows.append(
                FilteredHourEntry(
                    filename=str(row["filename"]),
                    hour=str(row["hour"]),
                    row_count=int(row["row_count"]),
                    byte_size=None,
                )
            )
        return rows

    async def serve_hour(
        self,
        request: web.Request,
        *,
        condition_id: str,
        token_id: str,
        filename: str,
    ) -> web.StreamResponse | None:
        escaped_condition = self._escape(condition_id)
        escaped_token = self._escape(token_id)
        escaped_filename = self._escape(filename)

        count_query = f"""
            SELECT count()
            FROM {self._database}.{self._table}
            WHERE filename = '{escaped_filename}'
              AND condition_id = '{escaped_condition}'
              AND token_id = '{escaped_token}'
            FORMAT TabSeparated
        """
        count_bytes = await asyncio.to_thread(self._execute_query, count_query)
        if int(count_bytes.decode().strip() or "0") <= 0:
            return None

        parquet_query = f"""
            SELECT update_type, data
            FROM {self._database}.{self._table}
            WHERE filename = '{escaped_filename}'
              AND condition_id = '{escaped_condition}'
              AND token_id = '{escaped_token}'
            ORDER BY relay_row_index
            FORMAT Parquet
        """

        timeout = aiohttp.ClientTimeout(total=self._config.clickhouse_timeout_secs)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self._request_url(),
                data=parquet_query.encode(),
                headers=self._request_headers(),
            ) as upstream:
                if upstream.status != 200:
                    detail = await upstream.text()
                    raise web.HTTPBadGateway(text=f"ClickHouse query failed: {detail}")
                response = web.StreamResponse(status=200)
                response.headers["Cache-Control"] = (
                    "public, max-age=31536000, immutable"
                )
                response.headers["Content-Type"] = "application/octet-stream"
                await response.prepare(request)
                async for chunk in upstream.content.iter_chunked(1024 * 1024):
                    await response.write(chunk)
                await response.write_eof()
                return response
