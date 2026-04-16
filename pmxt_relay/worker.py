from __future__ import annotations

import logging
import os
import shutil
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pmxt_relay.archive import extract_archive_filenames, fetch_archive_page
from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.storage import raw_relative_path

LOG = logging.getLogger(__name__)
_MIRROR_404_QUARANTINE_AFTER = 3
_MIRROR_RETRY_BACKOFF_CAP_SECS = 6 * 3600
_MIRROR_QUARANTINE_RETRY_SECS = 3600
_VERIFY_HTTP_TIMEOUT_CAP_SECS = 10


class RelayWorker:
    def __init__(
        self,
        config: RelayConfig,
        *,
        reset_inflight: bool = True,
        reset_mirror_inflight: bool = True,
    ) -> None:
        self._config = config
        self._config.ensure_directories()
        self._index = RelayIndex(config.db_path, event_retention=config.event_retention)
        reset_mirror = self._index.initialize(
            reset_inflight=reset_inflight, reset_mirror_inflight=reset_mirror_inflight
        )
        if reset_mirror:
            self._record_event(
                level="WARNING",
                event_type="resume_inflight",
                message="Reset inflight relay work after restart",
                payload={"reset_mirror": reset_mirror},
            )
        self._initial_local_raw_adoption_complete = False

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
            level=level, event_type=event_type, message=message, filename=filename, payload=payload
        )

    def close(self) -> None:
        self._index.close()

    def __enter__(self) -> RelayWorker:
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:
        self.close()

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

    def run_forever(self) -> None:
        try:
            while True:
                progress = self.run_once()
                if progress == 0:
                    LOG.info(
                        "No relay work pending, sleeping for %ss", self._config.poll_interval_secs
                    )
                    time.sleep(self._config.poll_interval_secs)
        finally:
            self.close()

    def run_once(self) -> int:
        discovered = self._discover_archive_hours()
        adopted = self._adopt_local_raw_hours()
        mirrored = self._mirror_pending_hours()
        reverified = self._verify_ready_hours()
        total = discovered + adopted + mirrored + reverified
        self._record_event(
            level="INFO",
            event_type="cycle_complete",
            message="Relay cycle complete",
            payload={
                "discovered": discovered,
                "adopted": adopted,
                "mirrored": mirrored,
                "reverified": reverified,
            },
        )
        LOG.info(
            "Relay cycle complete: discovered=%s adopted=%s mirrored=%s reverified=%s",
            discovered,
            adopted,
            mirrored,
            reverified,
        )
        return total

    def _discover_archive_hours(self) -> int:
        discovered = 0
        page = 1
        stale_pages = 0
        while True:
            if self._config.archive_max_pages is not None and page > self._config.archive_max_pages:
                break
            html = fetch_archive_page(
                self._config.archive_listing_url, page, self._config.http_timeout_secs
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
                    payload={"page": page, "new_hours": page_new, "total_entries": len(filenames)},
                )
                stale_pages = 0
            page += 1

        return discovered

    def _adopt_local_raw_hours(self) -> int:
        if not self._initial_local_raw_adoption_complete:
            adopted = self._adopt_all_local_raw_hours()
            self._initial_local_raw_adoption_complete = True
            return adopted
        return self._adopt_pending_local_raw_hours()

    def _adopt_all_local_raw_hours(self) -> int:
        adopted = 0
        for raw_path in sorted(self._config.raw_root.rglob("polymarket_orderbook_*.parquet")):
            adopted += self._register_local_raw_file(raw_path)
        self._emit_adopted_local_raw_event(adopted)
        return adopted

    def _adopt_pending_local_raw_hours(self) -> int:
        adopted = 0
        for row in self._index.list_hours_needing_mirror():
            raw_path = self._config.raw_root / raw_relative_path(row["filename"])
            adopted += self._register_local_raw_file(raw_path)
        self._emit_adopted_local_raw_event(adopted)
        return adopted

    def _register_local_raw_file(self, raw_path) -> int:  # type: ignore[no-untyped-def]
        if not raw_path.is_file():
            return 0
        filename = raw_path.name
        try:
            byte_size = raw_path.stat().st_size
        except FileNotFoundError:
            return 0
        changed = self._index.register_local_raw(
            filename,
            local_path=str(raw_path),
            content_length=byte_size,
            source_url=f"{self._config.raw_base_url}/{filename}",
        )
        if changed:
            row_count = self._read_parquet_row_count(raw_path)
            if row_count is not None:
                self._index.update_row_count(filename, row_count)
        return 1 if changed else 0

    def _emit_adopted_local_raw_event(self, adopted: int) -> None:
        if adopted <= 0:
            return
        self._record_event(
            level="INFO",
            event_type="adopt_local_raw",
            message=f"Adopted {adopted} existing raw hours from local disk",
            payload={"adopted_hours": adopted},
        )
        LOG.info("Adopted %s existing raw hours from local disk", adopted)

    def _mirror_pending_hours(self) -> int:
        mirrored = 0
        for row in self._index.list_hours_needing_mirror():
            try:
                self._mirror_hour(row)
            except Exception as exc:
                next_error_count = int(row["error_count"] or 0) + 1
                if self._should_quarantine_error(exc, error_count=next_error_count):
                    next_retry_at = self._quarantine_retry_at()
                    self._index.mark_mirror_quarantined(
                        row["filename"], error=str(exc), next_retry_at=next_retry_at.isoformat()
                    )
                    self._record_event(
                        level="WARNING",
                        event_type="mirror_quarantined",
                        filename=row["filename"],
                        message=(
                            f"Temporarily quarantined {row['filename']} after repeated mirror failures"
                        ),
                        payload={
                            "error": str(exc),
                            "error_count": next_error_count,
                            "next_retry_at": next_retry_at.isoformat(),
                        },
                    )
                    LOG.warning(
                        "Temporarily quarantined %s until %s after repeated mirror failures: %s",
                        row["filename"],
                        next_retry_at.isoformat(),
                        exc,
                    )
                    continue
                next_retry_at = self._next_retry_at(error_count=next_error_count)
                self._index.mark_mirror_retry(
                    row["filename"], error=str(exc), next_retry_at=next_retry_at.isoformat()
                )
                self._record_event(
                    level="ERROR",
                    event_type="mirror_error",
                    filename=row["filename"],
                    message=f"Failed to mirror {row['filename']}",
                    payload={
                        "error": str(exc),
                        "error_count": next_error_count,
                        "next_retry_at": next_retry_at.isoformat(),
                    },
                )
                LOG.exception("Failed to mirror %s", row["filename"])
                continue
            mirrored += 1
        return mirrored

    def _verify_ready_hours(self) -> int:
        batch = self._index.list_hours_needing_verification(
            batch_size=self._config.verify_batch_size
        )
        requeued = 0
        for row in batch:
            try:
                changed = self._check_upstream_changed(row)
            except Exception as exc:
                LOG.warning("Verification HEAD failed for %s: %s", row["filename"], exc)
                self._index.mark_verified(row["filename"])
                continue
            if changed:
                self._index.mark_needs_remirror(row["filename"])
                self._record_event(
                    level="WARNING",
                    event_type="verify_changed",
                    filename=row["filename"],
                    message=f"Upstream content changed for {row['filename']}, re-queuing for mirror",
                    payload={
                        "old_etag": row["etag"],
                        "old_content_length": row["content_length"],
                    },
                )
                LOG.info("Re-queuing %s: upstream content changed", row["filename"])
                requeued += 1
            else:
                self._index.mark_verified(row["filename"])
                self._backfill_row_count(row)
        if requeued > 0:
            self._record_event(
                level="INFO",
                event_type="verify_batch",
                message=f"Verification batch: {len(batch)} checked, {requeued} re-queued",
                payload={"batch_size": len(batch), "requeued": requeued},
            )
        return requeued

    def _check_upstream_changed(self, row) -> bool:  # type: ignore[no-untyped-def]
        source_url = row["source_url"]
        head_request = Request(source_url, method="HEAD", headers={"User-Agent": "pmxt-relay/1.0"})
        timeout_secs = min(self._config.http_timeout_secs, _VERIFY_HTTP_TIMEOUT_CAP_SECS)
        with urlopen(head_request, timeout=timeout_secs) as response:
            upstream_etag = response.headers.get("ETag")
            upstream_length_raw = response.headers.get("Content-Length")
            upstream_length = int(upstream_length_raw) if upstream_length_raw else None

        stored_etag = row["etag"]
        stored_length = row["content_length"]

        if stored_etag and upstream_etag:
            return stored_etag != upstream_etag

        if stored_length is not None and upstream_length is not None:
            return stored_length != upstream_length

        return False

    def _backfill_row_count(self, row) -> None:  # type: ignore[no-untyped-def]
        if row["row_count"] is not None:
            return
        local_path = row["local_path"]
        if local_path is None:
            return
        path = Path(local_path)
        if not path.is_file():
            return
        row_count = self._read_parquet_row_count(path)
        if row_count is not None:
            self._index.update_row_count(row["filename"], row_count)

    @staticmethod
    def _read_parquet_row_count(path: Path) -> int | None:
        try:
            import pyarrow.parquet as pq

            metadata = pq.read_metadata(path)
            return metadata.num_rows
        except Exception:
            return None

    def _next_retry_at(self, *, error_count: int) -> datetime:
        base_delay = max(60, int(self._config.poll_interval_secs))
        retry_delay = min(
            base_delay * (2 ** max(0, error_count - 1)), _MIRROR_RETRY_BACKOFF_CAP_SECS
        )
        return datetime.now(UTC) + timedelta(seconds=retry_delay)

    def _quarantine_retry_at(self) -> datetime:
        return datetime.now(UTC) + timedelta(seconds=_MIRROR_QUARANTINE_RETRY_SECS)

    def _should_quarantine_error(self, exc: Exception, *, error_count: int) -> bool:
        return (
            isinstance(exc, HTTPError)
            and exc.code == 404
            and error_count >= _MIRROR_404_QUARANTINE_AFTER
        )

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
            row_count = self._read_parquet_row_count(raw_path)
            if row_count is not None:
                self._index.update_row_count(filename, row_count)
            self._record_event(
                level="INFO",
                event_type="mirror_reuse",
                filename=filename,
                message=f"Reused mirrored raw hour for {filename}",
                payload={"destination_path": str(raw_path), "byte_size": raw_path.stat().st_size},
            )
            LOG.info("Reused mirrored raw hour %s from %s", filename, raw_path)
            return
        self._record_event(
            level="INFO",
            event_type="mirror_start",
            filename=filename,
            message=f"Mirroring {filename}",
            payload={"source_url": source_url, "destination_path": str(raw_path)},
        )

        etag = None
        content_length = None
        last_modified = None
        try:
            head_request = Request(
                source_url, method="HEAD", headers={"User-Agent": "pmxt-relay/1.0"}
            )
            with urlopen(head_request, timeout=self._config.http_timeout_secs) as response:
                etag = response.headers.get("ETag")
                last_modified = response.headers.get("Last-Modified")
                length_value = response.headers.get("Content-Length")
                content_length = int(length_value) if length_value else None
        except Exception as exc:
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
                "HEAD metadata probe failed for %s; trying GET anyway: %s", filename, head_error
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
        )
        row_count = self._read_parquet_row_count(raw_path)
        if row_count is not None:
            self._index.update_row_count(filename, row_count)
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
