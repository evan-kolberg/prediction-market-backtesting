from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.depthfeed_download_data import (
    DepthFeedClient,
    _hour_path,
    _write_hour,
    download_market,
    snapshot_rows,
)


def _snapshot(timestamp_ms: int = 1_768_780_800_123) -> dict[str, object]:
    return {
        "id": str(timestamp_ms),
        "time": "2026-01-19 00:00:00.123",
        "orderbook_up": {
            "bids": [[0.42, 10], [0, 5]],
            "asks": [[0.44, 11]],
        },
        "orderbook_down": {
            "bids": [[0.56, 11]],
            "asks": [[0.58, 10]],
        },
    }


def test_snapshot_rows_emit_both_outcomes_in_pmxt_legacy_shape() -> None:
    rows = snapshot_rows(
        _snapshot(),
        condition_id="0xcondition",
        token_up="up-token",
        token_down="down-token",
    )

    assert [row["update_type"] for row in rows] == ["book_snapshot", "book_snapshot"]
    payloads = [json.loads(row["data"]) for row in rows]
    assert [payload["token_id"] for payload in payloads] == ["up-token", "down-token"]
    assert payloads[0]["market_id"] == "0xcondition"
    assert payloads[0]["timestamp"] == 1_768_780_800.123
    assert payloads[0]["bids"] == [["0.42", "10"]]
    assert payloads[0]["asks"] == [["0.44", "11"]]


def test_hour_path_matches_pmxt_local_raw_layout(tmp_path: Path) -> None:
    assert _hour_path(tmp_path, 1_768_780_800_123) == (
        tmp_path / "2026" / "01" / "19" / "polymarket_orderbook_2026-01-19T00.parquet"
    )


def test_write_hour_merges_and_deduplicates_legacy_rows(tmp_path: Path) -> None:
    path = _hour_path(tmp_path, 1_768_780_800_123)
    rows = snapshot_rows(
        _snapshot(),
        condition_id="0xcondition",
        token_up="up-token",
        token_down="down-token",
    )

    assert _write_hour(path, rows, condition_id="0xcondition", overwrite=False) == 2
    assert _write_hour(path, rows, condition_id="0xcondition", overwrite=False) == 2
    table = pq.read_table(path)
    assert table.schema.names == ["market_id", "update_type", "data"]
    assert table.num_rows == 2


def test_write_hour_refuses_to_mix_fixed_and_legacy_schemas(tmp_path: Path) -> None:
    path = _hour_path(tmp_path, 1_768_780_800_123)
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"timestamp": [1], "market": ["0xcondition"]}), path)

    with pytest.raises(ValueError, match="fixed-column schema"):
        _write_hour(
            path,
            snapshot_rows(
                _snapshot(),
                condition_id="0xcondition",
                token_up="up-token",
                token_down="down-token",
            ),
            condition_id="0xcondition",
            overwrite=False,
        )


@pytest.mark.skipif(os.name == "nt", reason="the existing Telonex module imports resource")
def test_generated_file_is_readable_by_pmxt_loader(tmp_path: Path) -> None:
    from prediction_market_extensions.backtesting.data_sources.pmxt import (
        RunnerPolymarketPMXTDataLoader,
    )

    path = _hour_path(tmp_path, 1_768_780_800_123)
    rows = snapshot_rows(
        _snapshot(),
        condition_id="0xcondition",
        token_up="up-token",
        token_down="down-token",
    )
    _write_hour(path, rows, condition_id="0xcondition", overwrite=False)

    loader = object.__new__(RunnerPolymarketPMXTDataLoader)
    loader._pmxt_raw_root = tmp_path
    loader._pmxt_cache_dir = None
    loader._pmxt_progress_size_cache = {}
    loader._pmxt_scan_progress_callback = None
    loader._condition_id = "0xcondition"
    loader._token_id = "up-token"

    batches = loader._load_local_raw_market_batches(
        pd.Timestamp("2026-01-19T00:00:00Z"),
        batch_size=1_000,
    )

    assert batches is not None
    assert sum(batch.num_rows for batch in batches) == 1
    assert json.loads(batches[0].column("data")[0].as_py())["token_id"] == "up-token"


def test_download_market_pages_deduplicates_snapshots_and_writes_files(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, dict[str, object] | None]] = []

    def fake_get(
        self: DepthFeedClient,
        path: str,
        params: dict[str, object] | None = None,
    ) -> dict[str, object]:
        del self
        calls.append((path, params))
        if path.endswith("/markets/123"):
            return {
                "data": {
                    "condition_id": "0xcondition",
                    "clob_token_up": "up-token",
                    "clob_token_down": "down-token",
                }
            }
        if params and params.get("cursor") == "next":
            return {
                "data": [_snapshot(1_768_780_801_000)],
                "pagination": {"has_more": False, "next_cursor": None},
            }
        return {
            "data": [_snapshot(), _snapshot()],
            "pagination": {"has_more": True, "next_cursor": "next"},
        }

    monkeypatch.setattr(DepthFeedClient, "get", fake_get)
    client = DepthFeedClient(api_key="df_test", api_base="https://api.test", timeout_secs=1)
    summary = download_market(
        client=client,
        destination=tmp_path,
        coin="btc",
        market_id="123",
        start_time="2026-01-19T00:00:00Z",
        end_time="2026-01-19T01:00:00Z",
        overwrite=False,
        max_pages=None,
    )

    assert summary.snapshots == 2
    assert summary.rows == 4
    assert summary.pages == 2
    assert len(summary.files) == 1
    assert pq.read_table(summary.files[0]).num_rows == 4
    assert calls[1][1] == {
        "include_orderbook": True,
        "limit": 1_000,
        "start_time": "2026-01-19T00:00:00Z",
        "end_time": "2026-01-19T01:00:00Z",
    }
    assert calls[2][1] == {
        "include_orderbook": True,
        "limit": 1_000,
        "start_time": "2026-01-19T00:00:00Z",
        "end_time": "2026-01-19T01:00:00Z",
        "cursor": "next",
    }
