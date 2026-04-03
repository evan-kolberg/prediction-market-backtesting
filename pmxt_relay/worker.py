from __future__ import annotations

import gc
import logging
import os
import shutil
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen

from pmxt_relay.archive import extract_archive_filenames
from pmxt_relay.clickhouse import ClickHouseRelay
from pmxt_relay.archive import fetch_archive_page
from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.processor import RelayHourProcessor
from pmxt_relay.storage import filtered_relative_path
from pmxt_relay.storage import parse_archive_hour
from pmxt_relay.storage import processed_relative_path
from pmxt_relay.storage import raw_relative_path


LOG = logging.getLogger(__name__)
_PROGRESS_LOG_ROW_INTERVAL = 5_000_000


class RelayWorker:
    def __init__(
        self,
        config: RelayConfig,
        *,
        reset_inflight: bool = True,
        reset_mirror_inflight: bool = True,
        reset_process_inflight: bool = True,
        reset_prebuild_inflight: bool = True,
        skip_prebuild: bool = False,
    ) -> None:
        self._config = config
        self._skip_prebuild = skip_prebuild
        self._config.ensure_directories()
        self._index = RelayIndex(config.db_path, event_retention=config.event_retention)
        self._clickhouse_retry_resets: set[str] = set()
        if (
            config.uses_clickhouse_filtered_store
            and reset_inflight
            and reset_process_inflight
        ):
            self._clickhouse_retry_resets = set(self._index.list_processing_filenames())
        reset_mirror, reset_process, reset_prebuild = self._index.initialize(
            reset_inflight=reset_inflight,
            reset_mirror_inflight=reset_mirror_inflight,
            reset_process_inflight=reset_process_inflight,
            reset_prebuild_inflight=reset_prebuild_inflight,
        )
        self._processor = RelayHourProcessor(config)
        self._clickhouse = (
            None
            if not config.processing_enabled
            else (
                ClickHouseRelay(config)
                if config.uses_clickhouse_filtered_store
                else None
            )
        )
        if self._clickhouse is not None:
            self._clickhouse.ensure_schema()
            for row in self._index.list_completed_hours():
                self._clickhouse.backfill_completed_hour(
                    filename=row["filename"],
                    hour=row["hour"],
                    filtered_group_count=int(row["filtered_artifact_count"]),
                )
        if not config.processing_enabled:
            retired = self._index.disable_processing_backlog()
            if retired > 0:
                self._record_event(
                    level="INFO",
                    event_type="processing_disabled",
                    message="Disabled server-side processing backlog for mirror-only mode",
                    payload={"retired_hours": retired},
                )
        if reset_mirror or reset_process or reset_prebuild:
            self._record_event(
                level="WARNING",
                event_type="resume_inflight",
                message="Reset inflight relay work after restart",
                payload={
                    "reset_mirror": reset_mirror,
                    "reset_process": reset_process,
                    "reset_prebuild": reset_prebuild,
                },
            )

    def _record_event(
        self,
        *,
        level: str,
        event_type: str,
        message: str,
        filename: str | None = None,
        payload: dict[str, object] | None = None,
    ) -> None:
        self._index.log_event(
            level=level,
            event_type=event_type,
            message=message,
            filename=filename,
            payload=payload,
        )

    @staticmethod
    def _should_report_progress(
        *,
        processed_rows: int,
        total_rows: int,
        last_reported_rows: int,
    ) -> bool:
        return (
            processed_rows >= total_rows
            or last_reported_rows < 0
            or (processed_rows - last_reported_rows) >= _PROGRESS_LOG_ROW_INTERVAL
        )

    def run_forever(self) -> None:
        while True:
            progress = self.run_once()
            if progress == 0:
                LOG.info(
                    "No relay work pending, sleeping for %ss",
                    self._config.poll_interval_secs,
                )
                time.sleep(self._config.poll_interval_secs)

    def run_once(self) -> int:
        discovered = self._discover_archive_hours()
        adopted = self._adopt_local_raw_hours()
        mirrored = self._mirror_pending_hours()
        if not self._config.processing_enabled:
            processed = 0
            prebuilt = 0
        else:
            processed = self._process_pending_hours()
            prebuilt = (
                0
                if self._skip_prebuild or self._clickhouse is not None
                else self._prebuild_filtered_hours(limit=1)
            )
        total = discovered + adopted + mirrored + processed + prebuilt
        self._record_event(
            level="INFO",
            event_type="cycle_complete",
            message="Relay cycle complete",
            payload={
                "discovered": discovered,
                "adopted": adopted,
                "mirrored": mirrored,
                "processed": processed,
                "prebuilt": prebuilt,
            },
        )
        LOG.info(
            "Relay cycle complete: discovered=%s adopted=%s mirrored=%s processed=%s prebuilt=%s",
            discovered,
            adopted,
            mirrored,
            processed,
            prebuilt,
        )
        return total

    def run_prebuild_forever(self) -> None:
        while True:
            prebuilt = self._prebuild_filtered_hours(limit=1)
            if prebuilt == 0:
                LOG.info(
                    "No prebuild work pending, sleeping for %ss",
                    self._config.poll_interval_secs,
                )
                time.sleep(self._config.poll_interval_secs)

    def _discover_archive_hours(self) -> int:
        discovered = 0
        page = 1
        stale_pages = 0
        while True:
            if (
                self._config.archive_max_pages is not None
                and page > self._config.archive_max_pages
            ):
                break
            html = fetch_archive_page(
                self._config.archive_listing_url,
                page,
                self._config.http_timeout_secs,
            )
            filenames = extract_archive_filenames(html)
            if not filenames:
                break

            page_new = 0
            for filename in filenames:
                source_url = f"{self._config.raw_base_url}/{filename}"
                if self._index.upsert_discovered_hour(filename, source_url, page):
                    page_new += 1

            discovered += page_new
            if page_new == 0:
                stale_pages += 1
                if stale_pages >= self._config.archive_stale_pages:
                    break
            else:
                self._record_event(
                    level="INFO",
                    event_type="discover_page",
                    message=f"Discovered {page_new} new PMXT archive hours on page {page}",
                    payload={
                        "page": page,
                        "new_hours": page_new,
                        "total_entries": len(filenames),
                    },
                )
                stale_pages = 0
            page += 1

        return discovered

    def _adopt_local_raw_hours(self) -> int:
        adopted = 0
        for raw_path in sorted(
            self._config.raw_root.rglob("polymarket_orderbook_*.parquet")
        ):
            if not raw_path.is_file():
                continue
            filename = raw_path.name
            try:
                byte_size = raw_path.stat().st_size
            except FileNotFoundError:
                continue
            changed = self._index.register_local_raw(
                filename,
                local_path=str(raw_path),
                content_length=byte_size,
                source_url=f"{self._config.raw_base_url}/{filename}",
            )
            if not changed:
                continue
            adopted += 1
        if adopted > 0 and not self._config.processing_enabled:
            self._index.disable_processing_backlog()
        if adopted > 0:
            self._record_event(
                level="INFO",
                event_type="adopt_local_raw",
                message=f"Adopted {adopted} existing raw hours from local disk",
                payload={"adopted_hours": adopted},
            )
            LOG.info("Adopted %s existing raw hours from local disk", adopted)
        return adopted

    def _mirror_pending_hours(self) -> int:
        mirrored = 0
        for row in self._index.list_hours_needing_mirror():
            try:
                self._mirror_hour(row)
            except Exception as exc:  # noqa: BLE001
                self._index.mark_mirror_error(row["filename"], str(exc))
                self._record_event(
                    level="ERROR",
                    event_type="mirror_error",
                    filename=row["filename"],
                    message=f"Failed to mirror {row['filename']}",
                    payload={"error": str(exc)},
                )
                LOG.exception("Failed to mirror %s", row["filename"])
                continue
            mirrored += 1
            # Start emitting filtered hours as soon as raw files land instead of
            # waiting for the full raw backlog to finish mirroring.
            if self._config.processing_enabled:
                self._process_pending_hours(limit=1, include_errors=False)
        return mirrored

    def _mirror_hour(self, row) -> None:  # type: ignore[no-untyped-def]
        filename = row["filename"]
        source_url = row["source_url"]
        raw_path = self._config.raw_root / raw_relative_path(filename)
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        self._index.mark_mirroring(filename)
        if raw_path.exists() and raw_path.stat().st_size > 0:
            self._index.mark_mirrored(
                filename,
                local_path=str(raw_path),
                etag=None,
                content_length=raw_path.stat().st_size,
                last_modified=None,
                processing_enabled=self._config.processing_enabled,
            )
            self._record_event(
                level="INFO",
                event_type="mirror_reuse",
                filename=filename,
                message=f"Reused mirrored raw hour for {filename}",
                payload={
                    "destination_path": str(raw_path),
                    "byte_size": raw_path.stat().st_size,
                },
            )
            LOG.info("Reused mirrored raw hour %s from %s", filename, raw_path)
            return
        self._record_event(
            level="INFO",
            event_type="mirror_start",
            filename=filename,
            message=f"Mirroring {filename}",
            payload={
                "source_url": source_url,
                "destination_path": str(raw_path),
            },
        )

        etag = None
        content_length = None
        last_modified = None
        try:
            head_request = Request(
                source_url, method="HEAD", headers={"User-Agent": "pmxt-relay/1.0"}
            )
            with urlopen(
                head_request, timeout=self._config.http_timeout_secs
            ) as response:
                etag = response.headers.get("ETag")
                last_modified = response.headers.get("Last-Modified")
                length_value = response.headers.get("Content-Length")
                content_length = int(length_value) if length_value else None
        except Exception as exc:  # noqa: BLE001
            head_error = (
                f"HEAD {source_url} failed with {exc.code}"
                if isinstance(exc, HTTPError)
                else f"HEAD {source_url} failed: {exc}"
            )
            self._record_event(
                level="WARNING",
                event_type="mirror_head_error",
                filename=filename,
                message=f"HEAD metadata probe failed for {filename}; trying GET anyway",
                payload={"error": head_error},
            )
            LOG.warning(
                "HEAD metadata probe failed for %s; trying GET anyway: %s",
                filename,
                head_error,
            )

        tmp_path = raw_path.with_name(f"{raw_path.name}.tmp")
        request = Request(source_url, headers={"User-Agent": "pmxt-relay/1.0"})
        with (
            urlopen(request, timeout=self._config.http_timeout_secs) as response,
            tmp_path.open("wb") as handle,
        ):
            shutil.copyfileobj(response, handle)
        os.replace(tmp_path, raw_path)
        self._index.mark_mirrored(
            filename,
            local_path=str(raw_path),
            etag=etag,
            content_length=content_length,
            last_modified=last_modified,
            processing_enabled=self._config.processing_enabled,
        )
        self._record_event(
            level="INFO",
            event_type="mirror_complete",
            filename=filename,
            message=f"Mirrored {filename}",
            payload={
                "destination_path": str(raw_path),
                "byte_size": raw_path.stat().st_size,
                "etag": etag,
                "content_length": content_length,
            },
        )
        LOG.info("Mirrored %s to %s", filename, raw_path)

    def _process_pending_hours(
        self,
        *,
        limit: int | None = None,
        include_errors: bool = True,
    ) -> int:
        processed = 0
        for row in self._index.list_hours_needing_process(
            include_errors=include_errors
        ):
            filename = row["filename"]
            raw_path = Path(row["local_path"])
            if self._clickhouse is not None and self._clickhouse.hour_exists(filename):
                self._index.mark_sharded(filename)
                self._index.mark_prebuilt(
                    filename,
                    filtered_artifact_count=self._clickhouse.hour_group_count(filename),
                )
                self._record_event(
                    level="INFO",
                    event_type="process_reuse",
                    filename=filename,
                    message=f"Reused ClickHouse hour for {filename}",
                )
                processed += 1
                if limit is not None and processed >= limit:
                    break
                continue
            processed_path = self._config.processed_root / processed_relative_path(
                filename
            )
            last_reported_rows = -1

            def report_progress(processed_rows: int, total_rows: int) -> None:
                nonlocal last_reported_rows
                if not self._should_report_progress(
                    processed_rows=processed_rows,
                    total_rows=total_rows,
                    last_reported_rows=last_reported_rows,
                ):
                    return
                last_reported_rows = processed_rows
                self._record_event(
                    level="INFO",
                    event_type="process_progress",
                    filename=filename,
                    message=f"Processing progress for {filename}",
                    payload={
                        "processed_rows": processed_rows,
                        "total_rows": total_rows,
                    },
                )

            if (
                self._clickhouse is None
                and processed_path.exists()
                and processed_path.stat().st_size > 0
            ):
                self._index.mark_sharded(filename)
                if self._skip_prebuild:
                    self._record_event(
                        level="INFO",
                        event_type="process_reuse",
                        filename=filename,
                        message=f"Reused processed shard for {filename} (prebuild deferred)",
                        payload={
                            "processed_path": str(processed_path),
                            "byte_size": processed_path.stat().st_size,
                        },
                    )
                    processed += 1
                    continue
                self._index.mark_prebuilding(filename)
                try:
                    artifacts = self._processor.prebuild_filtered_from_processed(
                        filename,
                        processed_path,
                        progress_callback=report_progress,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._index.mark_prebuild_error(filename, str(exc))
                    self._record_event(
                        level="ERROR",
                        event_type="process_reuse_error",
                        filename=filename,
                        message=f"Failed to reuse processed shard for {filename}",
                        payload={"error": str(exc)},
                    )
                    LOG.exception("Failed to reuse processed shard for %s", filename)
                    continue
                self._index.mark_prebuilt(
                    filename,
                    filtered_artifact_count=len(artifacts),
                    artifacts=artifacts,
                )
                self._record_event(
                    level="INFO",
                    event_type="process_reuse",
                    filename=filename,
                    message=f"Reused processed shard for {filename}",
                    payload={
                        "processed_path": str(processed_path),
                        "byte_size": processed_path.stat().st_size,
                        "filtered_files": len(artifacts),
                    },
                )
                LOG.info("Reused processed shard %s from %s", filename, processed_path)
                processed += 1
                if limit is not None and processed >= limit:
                    break
                continue
            self._index.mark_processing(filename)
            self._record_event(
                level="INFO",
                event_type="process_start",
                filename=filename,
                message=f"Processing {filename}",
                payload={"raw_path": str(raw_path)},
            )
            try:
                needs_clickhouse_reset = self._clickhouse is not None and (
                    str(row["process_status"]) == "error"
                    or filename in self._clickhouse_retry_resets
                )
                if needs_clickhouse_reset:
                    self._clickhouse.reset_hour(filename)
                    self._clickhouse_retry_resets.discard(filename)
                result = self._processor.process_hour(
                    filename,
                    raw_path,
                    progress_callback=report_progress,
                    skip_filtered=self._skip_prebuild or self._clickhouse is not None,
                    write_processed=self._clickhouse is None,
                    batch_sink=(
                        None
                        if self._clickhouse is None
                        else lambda hour, batch, filename=filename: (
                            self._clickhouse.insert_batch(
                                filename=filename,
                                hour=hour,
                                batch=batch,
                            )
                        )
                    ),
                )
                if self._clickhouse is not None:
                    self._clickhouse.mark_hour_complete(
                        filename=filename,
                        hour=parse_archive_hour(filename).isoformat(),
                        filtered_group_count=result.filtered_group_count,
                        filtered_row_count=result.total_filtered_rows,
                    )
            except Exception as exc:  # noqa: BLE001
                self._index.mark_process_error(filename, str(exc))
                self._record_event(
                    level="ERROR",
                    event_type="process_error",
                    filename=filename,
                    message=f"Failed to process {filename}",
                    payload={"error": str(exc)},
                )
                LOG.exception("Failed to process %s", filename)
                continue

            self._index.mark_sharded(filename)
            if self._clickhouse is not None:
                self._index.mark_prebuilt(
                    filename,
                    filtered_artifact_count=result.filtered_group_count,
                )
                self._record_event(
                    level="INFO",
                    event_type="process_complete",
                    filename=filename,
                    message=f"Ingested {filename} into ClickHouse",
                    payload={
                        "filtered_rows": result.total_filtered_rows,
                        "filtered_groups": result.filtered_group_count,
                    },
                )
                LOG.info(
                    "Ingested %s into ClickHouse with %s groups",
                    filename,
                    result.filtered_group_count,
                )
            elif self._skip_prebuild:
                self._record_event(
                    level="INFO",
                    event_type="process_complete",
                    filename=filename,
                    message=f"Sharded {filename} (prebuild deferred)",
                    payload={
                        "filtered_rows": result.total_filtered_rows,
                    },
                )
                LOG.info("Sharded %s (prebuild deferred)", filename)
            else:
                artifacts = result.artifacts
                existing_rows = self._index.list_filtered_for_filename(filename)
                keep_paths = {artifact.local_path for artifact in artifacts}
                for existing in existing_rows:
                    cached_path = self._config.filtered_root / filtered_relative_path(
                        existing["condition_id"],
                        existing["token_id"],
                        existing["filename"],
                    )
                    cached_path.unlink(missing_ok=True)
                    if existing["local_path"] in keep_paths:
                        continue
                    existing_path = Path(existing["local_path"])
                    if existing_path.is_dir():
                        shutil.rmtree(existing_path, ignore_errors=True)
                    elif existing_path.is_relative_to(self._config.filtered_root):
                        existing_path.unlink(missing_ok=True)
                self._index.mark_prebuilt(
                    filename,
                    filtered_artifact_count=len(artifacts),
                    artifacts=artifacts,
                )
                self._record_event(
                    level="INFO",
                    event_type="process_complete",
                    filename=filename,
                    message=f"Processed {filename}",
                    payload={
                        "filtered_files": len(artifacts),
                        "filtered_rows": result.total_filtered_rows,
                    },
                )
                LOG.info(
                    "Processed %s into %s filtered files", filename, len(artifacts)
                )
            processed += 1
            if limit is not None and processed >= limit:
                break
        return processed

    def _prebuild_filtered_hours(self, *, limit: int | None = None) -> int:
        prebuilt = 0
        for row in self._index.list_hours_needing_filtered_prebuild():
            filename = row["filename"]
            processed_path = self._config.processed_root / processed_relative_path(
                filename
            )
            if not processed_path.exists() or processed_path.stat().st_size <= 0:
                continue
            self._index.mark_prebuilding(filename)
            self._record_event(
                level="INFO",
                event_type="filtered_prebuild_start",
                filename=filename,
                message=f"Prebuilding filtered hours for {filename}",
                payload={"processed_path": str(processed_path)},
            )
            last_reported_rows = -1

            def report_progress(processed_rows: int, total_rows: int) -> None:
                nonlocal last_reported_rows
                if not self._should_report_progress(
                    processed_rows=processed_rows,
                    total_rows=total_rows,
                    last_reported_rows=last_reported_rows,
                ):
                    return
                last_reported_rows = processed_rows
                self._record_event(
                    level="INFO",
                    event_type="filtered_prebuild_progress",
                    filename=filename,
                    message=f"Prebuild progress for {filename}",
                    payload={
                        "processed_rows": processed_rows,
                        "total_rows": total_rows,
                    },
                )

            try:
                artifacts = self._processor.prebuild_filtered_from_processed(
                    filename,
                    processed_path,
                    progress_callback=report_progress,
                )
            except Exception as exc:  # noqa: BLE001
                self._index.mark_prebuild_error(filename, str(exc))
                self._record_event(
                    level="ERROR",
                    event_type="filtered_prebuild_error",
                    filename=filename,
                    message=f"Failed to prebuild filtered hours for {filename}",
                    payload={"error": str(exc)},
                )
                LOG.exception("Failed to prebuild filtered hours for %s", filename)
                continue

            self._index.mark_prebuilt(
                filename,
                filtered_artifact_count=len(artifacts),
                artifacts=artifacts,
            )
            self._record_event(
                level="INFO",
                event_type="filtered_prebuild_complete",
                filename=filename,
                message=f"Prebuilt filtered hours for {filename}",
                payload={"filtered_files": len(artifacts)},
            )
            LOG.info(
                "Prebuilt filtered hours for %s into %s files",
                filename,
                len(artifacts),
            )
            del artifacts
            gc.collect()
            prebuilt += 1
            if limit is not None and prebuilt >= limit:
                break
        return prebuilt
