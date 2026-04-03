from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from pmxt_relay.local_processing import process_local_raw_mirror


def test_process_local_raw_mirror_writes_filtered_layout(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    filtered_root = tmp_path / "filtered"
    raw_path = (
        raw_root / "2026" / "03" / "21" / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.table(
            {
                "market_id": ["condition-a", "condition-a", "condition-b"],
                "update_type": [
                    "book_snapshot",
                    "price_change",
                    "book_snapshot",
                ],
                "data": [
                    '{"token_id":"token-yes","seq":1}',
                    '{"token_id":"token-yes","seq":2}',
                    '{"token_id":"token-no","seq":3}',
                ],
            }
        ),
        raw_path,
    )

    summary = process_local_raw_mirror(
        vendor="pmxt",
        raw_root=raw_root,
        filtered_root=filtered_root,
    )

    output_path = (
        filtered_root
        / "condition-a"
        / "token-yes"
        / "polymarket_orderbook_2026-03-21T12.parquet"
    )
    table = pq.read_table(output_path)

    assert summary.scanned_files == 1
    assert summary.processed_files == 1
    assert summary.filtered_files == 2
    assert summary.filtered_rows == 3
    assert table.column_names == ["update_type", "data"]
    assert table.num_rows == 2
