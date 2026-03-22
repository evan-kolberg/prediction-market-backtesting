from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen

from pmxt_relay.archive import extract_archive_filenames
from pmxt_relay.archive import fetch_archive_page
from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.processor import RelayHourProcessor
from pmxt_relay.storage import filtered_relative_path
from pmxt_relay.storage import processed_relative_path
from pmxt_relay.storage import raw_relative_path


LOG = logging.getLogger(__name__)


class RelayWorker:
    def __init__(self, config: RelayConfig) -> None:
        self._config = config
        self._config.ensure_directories()
        self._index = RelayIndex(config.db_path, event_retention=config.event_retention)
        reset_mirror, reset_process = self._index.initialize(reset_inflight=True)
        self._processor = RelayHourProcessor(config)
        if reset_mirror or reset_process:
            self._record_event(
                level="WARNING",
                event_type="resume_inflight",
                message="Reset inflight relay work after restart",
                payload={
                    "reset_mirror": reset_mirror,
                    "reset_process": reset_process,
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
        mirrored = self._mirror_pending_hours()
        processed = self._process_pending_hours()
        prebuilt = self._prebuild_filtered_hours(limit=1)
        total = discovered + mirrored + processed + prebuilt
        self._record_event(
            level="INFO",
            event_type="cycle_complete",
            message="Relay cycle complete",
            payload={
                "discovered": discovered,
                "mirrored": mirrored,
                "processed": processed,
                "prebuilt": prebuilt,
            },
        )
        LOG.info(
            "Relay cycle complete: discovered=%s mirrored=%s processed=%s prebuilt=%s",
            discovered,
            mirrored,
            processed,
            prebuilt,
        )
        return total

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
        except HTTPError as exc:
            raise RuntimeError(f"HEAD {source_url} failed with {exc.code}") from exc

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
            processed_path = self._config.processed_root / processed_relative_path(
                filename
            )
            if processed_path.exists() and processed_path.stat().st_size > 0:
                artifacts = self._processor.prebuild_filtered_from_processed(
                    filename,
                    processed_path,
                )
                self._index.mark_processed(
                    filename,
                    filtered_artifact_count=len(artifacts),
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
            existing_rows = self._index.list_filtered_for_filename(filename)
            last_reported_rows = -1
            last_report_monotonic = 0.0

            def report_progress(processed_rows: int, total_rows: int) -> None:
                nonlocal last_reported_rows, last_report_monotonic
                current = time.monotonic()
                should_log = (
                    processed_rows >= total_rows
                    or last_reported_rows < 0
                    or (processed_rows - last_reported_rows) >= 5_000_000
                    or (current - last_report_monotonic) >= 5.0
                )
                if not should_log:
                    return
                last_reported_rows = processed_rows
                last_report_monotonic = current
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

            try:
                result = self._processor.process_hour(
                    filename,
                    raw_path,
                    progress_callback=report_progress,
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

            artifacts = result.artifacts
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

            self._index.mark_processed(
                filename,
                filtered_artifact_count=len(artifacts),
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
            LOG.info("Processed %s into %s filtered files", filename, len(artifacts))
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
            self._record_event(
                level="INFO",
                event_type="filtered_prebuild_start",
                filename=filename,
                message=f"Prebuilding filtered hours for {filename}",
                payload={"processed_path": str(processed_path)},
            )
            last_reported_rows = -1
            last_report_monotonic = 0.0

            def report_progress(processed_rows: int, total_rows: int) -> None:
                nonlocal last_reported_rows, last_report_monotonic
                current = time.monotonic()
                should_log = (
                    processed_rows >= total_rows
                    or last_reported_rows < 0
                    or (processed_rows - last_reported_rows) >= 5_000_000
                    or (current - last_report_monotonic) >= 5.0
                )
                if not should_log:
                    return
                last_reported_rows = processed_rows
                last_report_monotonic = current
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
                self._record_event(
                    level="ERROR",
                    event_type="filtered_prebuild_error",
                    filename=filename,
                    message=f"Failed to prebuild filtered hours for {filename}",
                    payload={"error": str(exc)},
                )
                LOG.exception("Failed to prebuild filtered hours for %s", filename)
                continue

            self._index.mark_processed(
                filename,
                filtered_artifact_count=len(artifacts),
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
            prebuilt += 1
            if limit is not None and prebuilt >= limit:
                break
        return prebuilt
