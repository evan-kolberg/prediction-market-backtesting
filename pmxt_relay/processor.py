from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
import os
from pathlib import Path
import shutil

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import FilteredHourArtifact
from pmxt_relay.storage import parse_archive_hour
from pmxt_relay.storage import processed_relative_path


TOKEN_ID_REGEX = r'"token_id"\s*:\s*"(?P<token>[^"]+)"'
FILTERED_SCHEMA = pa.schema(
    [
        ("update_type", pa.string()),
        ("data", pa.string()),
    ]
)
PROCESSED_SCHEMA = pa.schema(
    [
        ("market_id", pa.string()),
        ("token_id", pa.string()),
        ("update_type", pa.string()),
        ("data", pa.string()),
    ]
)
RELEVANT_UPDATE_TYPES = pa.array(["book_snapshot", "price_change"])
PARQUET_BATCH_SIZE = 65536


@dataclass(frozen=True)
class ProcessedHourResult:
    artifacts: list[FilteredHourArtifact]
    total_filtered_rows: int


def materialize_partition_dir(
    partition_dir: Path,
    output_path: Path,
) -> tuple[int, int]:
    partition_files = sorted(partition_dir.glob("*.parquet"))
    if not partition_files:
        raise FileNotFoundError(
            f"No partition parquet files found under {partition_dir}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f"{output_path.name}.tmp")
    partition_dataset = ds.dataset(partition_files, format="parquet")
    table = partition_dataset.to_table(
        columns=["relay_row_index", "update_type", "data"]
    )
    sort_indices = pc.sort_indices(
        table,
        sort_keys=[("relay_row_index", "ascending")],
    )
    ordered_table = table.take(sort_indices).select(["update_type", "data"])
    try:
        pq.write_table(ordered_table, tmp_path, compression="zstd")
        os.replace(tmp_path, output_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return ordered_table.num_rows, output_path.stat().st_size


def materialize_filtered_hour(
    processed_path: Path,
    output_path: Path,
    *,
    condition_id: str,
    token_id: str,
) -> tuple[int, int]:
    dataset = ds.dataset(processed_path, format="parquet")
    scanner = dataset.scanner(
        columns=["update_type", "data"],
        filter=(ds.field("market_id") == condition_id)
        & (ds.field("token_id") == token_id),
    )
    table = scanner.to_table()
    if table.num_rows == 0:
        raise FileNotFoundError(
            f"No filtered rows found for {condition_id=} {token_id=} in {processed_path}"
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(f"{output_path.name}.tmp")
    try:
        pq.write_table(table, tmp_path, compression="zstd")
        os.replace(tmp_path, output_path)
    finally:
        tmp_path.unlink(missing_ok=True)
    return table.num_rows, output_path.stat().st_size


class RelayHourProcessor:
    def __init__(self, config: RelayConfig) -> None:
        self._config = config

    def process_hour(
        self,
        filename: str,
        raw_path: Path,
        *,
        progress_callback: Callable[[int, int], None] | None = None,
    ) -> ProcessedHourResult:
        hour = parse_archive_hour(filename).isoformat()
        temp_root = self._config.tmp_root / f"{filename}.filtered"
        temp_path = temp_root / "hour.parquet"
        final_path = self._config.processed_root / processed_relative_path(filename)
        shutil.rmtree(temp_root, ignore_errors=True)
        temp_root.mkdir(parents=True, exist_ok=True)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        parquet_file = pq.ParquetFile(raw_path)
        total_rows = parquet_file.metadata.num_rows
        processed_rows = 0
        total_filtered_rows = 0
        counts: Counter[tuple[str, str]] = Counter()
        wrote_any = False
        writer: pq.ParquetWriter | None = None

        try:
            for source_rows, batch in self._iter_filtered_batches(parquet_file):
                processed_rows += source_rows
                if batch is not None:
                    total_filtered_rows += batch.num_rows
                    counts.update(self._count_batch_groups(batch))
                    if writer is None:
                        writer = pq.ParquetWriter(
                            temp_path,
                            PROCESSED_SCHEMA,
                            compression="zstd",
                        )
                    writer.write_batch(
                        pa.RecordBatch.from_arrays(
                            [
                                batch.column("market_id"),
                                batch.column("token_id"),
                                batch.column("update_type"),
                                batch.column("data"),
                            ],
                            schema=PROCESSED_SCHEMA,
                        )
                    )
                    wrote_any = True
                if progress_callback is not None:
                    progress_callback(processed_rows, total_rows)

            if writer is not None:
                writer.close()
                writer = None

            if not wrote_any:
                final_path.unlink(missing_ok=True)
                return ProcessedHourResult(artifacts=[], total_filtered_rows=0)

            os.replace(temp_path, final_path)
            shard_size = final_path.stat().st_size
            artifacts = [
                FilteredHourArtifact(
                    filename=filename,
                    hour=hour,
                    condition_id=condition_id,
                    token_id=token_id,
                    local_path=str(final_path),
                    row_count=row_count,
                    byte_size=shard_size,
                )
                for (condition_id, token_id), row_count in sorted(counts.items())
            ]
            return ProcessedHourResult(
                artifacts=artifacts,
                total_filtered_rows=total_filtered_rows,
            )
        finally:
            if writer is not None:
                writer.close()
            shutil.rmtree(temp_root, ignore_errors=True)

    def _iter_filtered_batches(self, parquet_file: pq.ParquetFile):  # type: ignore[no-untyped-def]
        for batch in parquet_file.iter_batches(
            columns=["market_id", "update_type", "data"],
            batch_size=PARQUET_BATCH_SIZE,
            use_threads=True,
        ):
            source_rows = batch.num_rows
            filtered_batch = self._filter_batch(batch)
            yield (
                source_rows,
                filtered_batch
                if filtered_batch is not None and filtered_batch.num_rows
                else None,
            )

    @staticmethod
    def _filter_batch(batch: pa.RecordBatch) -> pa.RecordBatch | None:
        update_type_mask = pc.is_in(batch.column(1), value_set=RELEVANT_UPDATE_TYPES)
        if not update_type_mask.true_count:
            return None

        filtered_batch = batch.filter(update_type_mask)
        extracted = pc.extract_regex(filtered_batch.column(2), pattern=TOKEN_ID_REGEX)
        token_ids = extracted.field("token")
        token_mask = pc.not_equal(token_ids, "")
        if not token_mask.true_count:
            return None

        filtered_batch = filtered_batch.filter(token_mask)
        filtered_token_ids = pc.filter(token_ids, token_mask)
        return pa.record_batch(
            [
                filtered_batch.column(0),
                filtered_token_ids,
                filtered_batch.column(1),
                filtered_batch.column(2),
            ],
            schema=PROCESSED_SCHEMA,
        )

    @staticmethod
    def _count_batch_groups(batch: pa.RecordBatch) -> Counter[tuple[str, str]]:
        grouped = (
            pa.Table.from_batches([batch])
            .group_by(["market_id", "token_id"])
            .aggregate([("data", "count")])
        )
        market_ids = grouped.column("market_id").to_pylist()
        token_ids = grouped.column("token_id").to_pylist()
        counts = grouped.column("data_count").to_pylist()
        return Counter(
            {
                (market_id, token_id): count
                for market_id, token_id, count in zip(
                    market_ids, token_ids, counts, strict=True
                )
            }
        )
