from __future__ import annotations

from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path

from pmxt_relay.processor import RelayHourProcessor
from pmxt_relay.storage import parse_archive_hour


def _parse_hour_bound(value: str | None) -> datetime | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = datetime.strptime(normalized, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    else:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        else:
            parsed = parsed.astimezone(UTC)
    return parsed.replace(minute=0, second=0, microsecond=0)


@dataclass(frozen=True)
class LocalProcessingConfig:
    filtered_root: Path
    tmp_root: Path
    filtered_materialization_workers: int
    processed_root: Path


@dataclass(frozen=True)
class LocalProcessingSummary:
    vendor: str
    raw_root: str
    filtered_root: str
    tmp_root: str
    scanned_files: int
    processed_files: int
    filtered_files: int
    filtered_rows: int
    start_hour: str | None
    end_hour: str | None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _iter_pmxt_raw_paths(
    raw_root: Path,
    *,
    start_hour: datetime | None = None,
    end_hour: datetime | None = None,
) -> list[Path]:
    by_filename: dict[str, Path] = {}
    for raw_path in sorted(raw_root.rglob("polymarket_orderbook_*.parquet")):
        if not raw_path.is_file():
            continue
        try:
            hour = parse_archive_hour(raw_path.name)
        except ValueError:
            continue
        if start_hour is not None and hour < start_hour:
            continue
        if end_hour is not None and hour > end_hour:
            continue
        by_filename.setdefault(raw_path.name, raw_path)
    return sorted(by_filename.values(), key=lambda path: parse_archive_hour(path.name))


def process_local_raw_mirror(
    *,
    vendor: str,
    raw_root: Path,
    filtered_root: Path,
    tmp_root: Path | None = None,
    workers: int = 4,
    limit: int | None = None,
    start_hour: str | None = None,
    end_hour: str | None = None,
) -> LocalProcessingSummary:
    normalized_vendor = vendor.strip().casefold()
    if normalized_vendor != "pmxt":
        raise ValueError(
            f"Unsupported vendor '{vendor}'. The local processor currently supports: pmxt"
        )

    normalized_raw_root = raw_root.expanduser().resolve()
    normalized_filtered_root = filtered_root.expanduser().resolve()
    normalized_tmp_root = (
        tmp_root.expanduser().resolve()
        if tmp_root is not None
        else normalized_filtered_root / ".tmp-processing"
    )

    normalized_filtered_root.mkdir(parents=True, exist_ok=True)
    normalized_tmp_root.mkdir(parents=True, exist_ok=True)

    processor = RelayHourProcessor(
        LocalProcessingConfig(
            filtered_root=normalized_filtered_root,
            tmp_root=normalized_tmp_root,
            filtered_materialization_workers=max(1, workers),
            processed_root=normalized_tmp_root / "processed-unused",
        )
    )

    start_bound = _parse_hour_bound(start_hour)
    end_bound = _parse_hour_bound(end_hour)
    raw_paths = _iter_pmxt_raw_paths(
        normalized_raw_root,
        start_hour=start_bound,
        end_hour=end_bound,
    )
    if limit is not None:
        raw_paths = raw_paths[: max(0, limit)]

    processed_files = 0
    filtered_files = 0
    filtered_rows = 0
    for raw_path in raw_paths:
        result = processor.process_hour(
            raw_path.name,
            raw_path,
            skip_filtered=False,
            write_processed=False,
        )
        processed_files += 1
        filtered_files += len(result.artifacts)
        filtered_rows += result.total_filtered_rows

    return LocalProcessingSummary(
        vendor=normalized_vendor,
        raw_root=str(normalized_raw_root),
        filtered_root=str(normalized_filtered_root),
        tmp_root=str(normalized_tmp_root),
        scanned_files=len(raw_paths),
        processed_files=processed_files,
        filtered_files=filtered_files,
        filtered_rows=filtered_rows,
        start_hour=start_bound.isoformat() if start_bound is not None else None,
        end_hour=end_bound.isoformat() if end_bound is not None else None,
    )
