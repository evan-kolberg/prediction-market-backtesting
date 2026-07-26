from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DEPTHFEED_API_KEY_ENV = "DEPTHFEED_API_KEY"
DEFAULT_API_BASE = "https://api.depthfeed.com"
USER_AGENT = "prediction-market-backtesting/1.0"
LEGACY_COLUMNS = ("market_id", "update_type", "data")


@dataclass(frozen=True)
class DownloadSummary:
    market_id: str
    condition_id: str
    snapshots: int
    rows: int
    files: tuple[str, ...]
    pages: int

    def as_dict(self) -> dict[str, object]:
        return {
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "snapshots": self.snapshots,
            "rows": self.rows,
            "files": list(self.files),
            "pages": self.pages,
        }


class DepthFeedClient:
    def __init__(self, *, api_key: str, api_base: str, timeout_secs: int) -> None:
        key = api_key.strip()
        if not key:
            raise ValueError(
                f"Set {DEPTHFEED_API_KEY_ENV} to a DepthFeed API key before downloading."
            )
        self.api_key = key
        self.api_base = api_base.rstrip("/")
        self.timeout_secs = max(1, timeout_secs)

    def get(self, path: str, params: dict[str, object] | None = None) -> dict[str, Any]:
        query = urlencode(
            {
                key: str(value).lower() if isinstance(value, bool) else value
                for key, value in (params or {}).items()
            }
        )
        url = f"{self.api_base}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(
            url,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.api_key}",
                "User-Agent": USER_AGENT,
            },
        )
        try:
            with urlopen(request, timeout=self.timeout_secs) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"DepthFeed returned HTTP {exc.code} for {path}: {body}") from exc
        except (URLError, TimeoutError) as exc:
            raise RuntimeError(f"DepthFeed request failed for {path}: {exc}") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"DepthFeed returned invalid JSON for {path}.") from exc

        if not isinstance(payload, dict):
            raise TypeError(f"DepthFeed returned a non-object response for {path}.")
        if "error" in payload:
            raise RuntimeError(f"DepthFeed API error for {path}: {payload['error']}")
        return payload


def _timestamp_ms(snapshot: dict[str, Any]) -> int:
    raw_id = snapshot.get("id")
    try:
        timestamp_ms = int(str(raw_id))
    except (TypeError, ValueError):
        raw_time = snapshot.get("time")
        if not isinstance(raw_time, str) or not raw_time.strip():
            raise ValueError("Snapshot is missing both a millisecond id and time.")
        normalized = raw_time.strip().replace(" ", "T")
        if normalized.endswith("Z"):
            normalized = f"{normalized[:-1]}+00:00"
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        timestamp_ms = int(parsed.timestamp() * 1_000)
    if timestamp_ms <= 0:
        raise ValueError(f"Invalid snapshot timestamp: {timestamp_ms}")
    return timestamp_ms


def _decimal_text(value: object) -> str:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"Invalid order-book number: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"Non-finite order-book number: {value!r}")
    return format(parsed, "f")


def _levels(value: object, *, side: str) -> list[list[str]]:
    if not isinstance(value, list):
        raise TypeError(f"Snapshot {side} levels must be a list.")
    levels: list[list[str]] = []
    for level in value:
        if not isinstance(level, list | tuple) or len(level) != 2:
            raise ValueError(f"Invalid {side} level: {level!r}")
        price = _decimal_text(level[0])
        size = _decimal_text(level[1])
        if Decimal(price) <= 0 or Decimal(size) <= 0:
            continue
        levels.append([price, size])
    return levels


def _book_payload(
    *,
    snapshot: dict[str, Any],
    condition_id: str,
    token_id: str,
    book_key: str,
) -> str:
    book = snapshot.get(book_key)
    if not isinstance(book, dict):
        raise TypeError(
            f"Snapshot is missing {book_key}; request include_orderbook=true and verify plan access."
        )
    timestamp_ms = _timestamp_ms(snapshot)
    payload = {
        "asks": _levels(book.get("asks"), side=f"{book_key}.asks"),
        "bids": _levels(book.get("bids"), side=f"{book_key}.bids"),
        "market_id": condition_id,
        "timestamp": timestamp_ms / 1_000,
        "token_id": token_id,
        "update_type": "book_snapshot",
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def snapshot_rows(
    snapshot: dict[str, Any],
    *,
    condition_id: str,
    token_up: str,
    token_down: str,
) -> list[dict[str, str]]:
    return [
        {
            "market_id": condition_id,
            "update_type": "book_snapshot",
            "data": _book_payload(
                snapshot=snapshot,
                condition_id=condition_id,
                token_id=token_up,
                book_key="orderbook_up",
            ),
        },
        {
            "market_id": condition_id,
            "update_type": "book_snapshot",
            "data": _book_payload(
                snapshot=snapshot,
                condition_id=condition_id,
                token_id=token_down,
                book_key="orderbook_down",
            ),
        },
    ]


def _hour_path(destination: Path, timestamp_ms: int) -> Path:
    stamp = datetime.fromtimestamp(timestamp_ms / 1_000, tz=UTC)
    return (
        destination
        / f"{stamp.year:04d}"
        / f"{stamp.month:02d}"
        / f"{stamp.day:02d}"
        / f"polymarket_orderbook_{stamp:%Y-%m-%dT%H}.parquet"
    )


def _row_sort_key(row: dict[str, str]) -> tuple[float, str]:
    payload = json.loads(row["data"])
    return float(payload["timestamp"]), str(payload["token_id"])


def _write_hour(
    path: Path,
    rows: list[dict[str, str]],
    *,
    condition_id: str,
    overwrite: bool,
) -> int:
    existing_rows: list[dict[str, str]] = []
    if path.exists():
        existing = pq.read_table(path)
        if not set(LEGACY_COLUMNS).issubset(existing.schema.names):
            raise ValueError(
                f"{path} uses the PMXT fixed-column schema. Use a separate destination "
                "for DepthFeed legacy rows instead of mixing schemas."
            )
        if overwrite:
            existing = existing.filter(pc.not_equal(existing["market_id"], condition_id))
        existing_rows = [
            {name: str(row[name]) for name in LEGACY_COLUMNS}
            for row in existing.select(LEGACY_COLUMNS).to_pylist()
        ]

    unique: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in [*existing_rows, *rows]:
        unique[(row["market_id"], row["update_type"], row["data"])] = row
    merged = sorted(unique.values(), key=_row_sort_key)
    table = pa.Table.from_pylist(
        merged, schema=pa.schema([(name, pa.string()) for name in LEGACY_COLUMNS])
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(table, temporary, compression="zstd")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return len({(row["market_id"], row["update_type"], row["data"]) for row in rows})


def download_market(
    *,
    client: DepthFeedClient,
    destination: Path,
    coin: str,
    market_id: str,
    start_time: str | None,
    end_time: str | None,
    overwrite: bool,
    max_pages: int | None,
) -> DownloadSummary:
    market_path = f"/v3/{quote(coin, safe='')}/markets/{quote(market_id, safe='')}"
    market_response = client.get(market_path)
    market = market_response.get("data")
    if not isinstance(market, dict):
        raise TypeError("DepthFeed market response is missing data.")

    condition_id = str(market.get("condition_id") or "").strip()
    token_up = str(market.get("clob_token_up") or "").strip()
    token_down = str(market.get("clob_token_down") or "").strip()
    if not condition_id or not token_up or not token_down:
        raise RuntimeError(
            "DepthFeed market metadata is missing condition_id or CLOB outcome token ids."
        )

    snapshot_path = f"{market_path}/snapshots"
    cursor: str | None = None
    seen_cursors: set[str] = set()
    seen_snapshots: set[tuple[int, str]] = set()
    rows_by_hour: dict[Path, list[dict[str, str]]] = defaultdict(list)
    pages = 0

    while True:
        params: dict[str, object] = {"include_orderbook": True, "limit": 1_000}
        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time
        if cursor:
            params["cursor"] = cursor
        response = client.get(snapshot_path, params)
        pages += 1
        data = response.get("data")
        if not isinstance(data, list):
            raise TypeError("DepthFeed snapshots response is missing a data array.")
        for raw_snapshot in data:
            if not isinstance(raw_snapshot, dict):
                continue
            timestamp_ms = _timestamp_ms(raw_snapshot)
            snapshot_key = (timestamp_ms, json.dumps(raw_snapshot, sort_keys=True))
            if snapshot_key in seen_snapshots:
                continue
            seen_snapshots.add(snapshot_key)
            rows_by_hour[_hour_path(destination, timestamp_ms)].extend(
                snapshot_rows(
                    raw_snapshot,
                    condition_id=condition_id,
                    token_up=token_up,
                    token_down=token_down,
                )
            )

        pagination = response.get("pagination")
        if not isinstance(pagination, dict) or not pagination.get("has_more"):
            break
        next_cursor = str(pagination.get("next_cursor") or "").strip()
        if not next_cursor or next_cursor in seen_cursors:
            raise RuntimeError("DepthFeed pagination repeated or omitted next_cursor.")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
        if max_pages is not None and pages >= max_pages:
            break

    files: list[str] = []
    rows_written = 0
    for path, rows in sorted(rows_by_hour.items()):
        rows_written += _write_hour(
            path,
            rows,
            condition_id=condition_id,
            overwrite=overwrite,
        )
        files.append(str(path))

    return DownloadSummary(
        market_id=market_id,
        condition_id=condition_id,
        snapshots=len(seen_snapshots),
        rows=rows_written,
        files=tuple(files),
        pages=pages,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download one DepthFeed Polymarket L2 snapshot window into the local "
            "PMXT legacy parquet layout used by this repository."
        )
    )
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument(
        "--coin", required=True, choices=("btc", "eth", "sol", "xrp", "doge", "bnb", "hype")
    )
    parser.add_argument("--market-id", required=True)
    parser.add_argument("--start-time", default=None)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--api-base", default=DEFAULT_API_BASE)
    parser.add_argument("--timeout-secs", type=int, default=60)
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    api_key = os.getenv(DEPTHFEED_API_KEY_ENV, "")
    client = DepthFeedClient(
        api_key=api_key,
        api_base=args.api_base,
        timeout_secs=args.timeout_secs,
    )
    summary = download_market(
        client=client,
        destination=args.destination,
        coin=args.coin,
        market_id=args.market_id,
        start_time=args.start_time,
        end_time=args.end_time,
        overwrite=args.overwrite,
        max_pages=args.max_pages,
    )
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
