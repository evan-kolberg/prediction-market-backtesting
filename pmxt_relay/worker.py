from __future__ import annotations

import logging
import os
import shutil
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from pmxt_relay.config import RelayConfig
from pmxt_relay.coverage import iter_archive_hours_desc
from pmxt_relay.index_db import REMIRROR_CONTENT_CHANGED_REASON, RelayIndex
from pmxt_relay.storage import archive_filename_for_hour, raw_relative_path

LOG = logging.getLogger(__name__)
_MIRROR_404_QUARANTINE_AFTER = 3
_MIRROR_RETRY_BACKOFF_CAP_SECS = 6 * 3600
_MIRROR_QUARANTINE_RETRY_SECS = 3600
_VERIFY_HTTP_TIMEOUT_CAP_SECS = 2
_MIN_NONEMPTY_RAW_BYTES = 1024 * 1024


@dataclass(frozen=True)
class _RawUrlCandidate:
    source_url: str
    source_priority: int
    content_length: int | None


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

    def _discover_archive_hours(self, *, now: datetime | None = None) -> int:
        discovered = 0
        probed = 0
        available = 0
        absent_filenames: list[str] = []
        source_errors = 0
        source_error_samples: list[dict[str, str]] = []
        raw_base_urls = self._config.resolved_raw_base_urls
        for hour in iter_archive_hours_desc(
            start_hour=self._config.archive_start_hour,
            now=now,
        ):
            probed += 1
            filename = archive_filename_for_hour(hour)
            best_candidate: _RawUrlCandidate | None = None
            probe_error = False
            for source_priority, raw_base_url in enumerate(raw_base_urls):
                candidate_url = f"{raw_base_url}/{filename}"
                try:
                    exists, content_length = self._raw_url_probe(candidate_url)
                except Exception as exc:
                    source_errors += 1
                    probe_error = True
                    if len(source_error_samples) < 10:
                        source_error_samples.append(
                            {
                                "filename": filename,
                                "source_url": candidate_url,
                                "error": str(exc),
                            }
                        )
                        LOG.warning("Failed to probe %s: %s", candidate_url, exc)
                    continue
                if not exists:
                    continue
                candidate = _RawUrlCandidate(
                    source_url=candidate_url,
                    source_priority=source_priority,
                    content_length=content_length,
                )
                if self._is_better_raw_candidate(candidate, best_candidate):
                    best_candidate = candidate
            if best_candidate is None:
                if not probe_error:
                    absent_filenames.append(filename)
                continue
            available += 1
            if self._index.upsert_discovered_hour(
                filename,
                best_candidate.source_url,
                0,
                source_priority=best_candidate.source_priority,
                allow_lower_priority_source=True,
            ):
                discovered += 1

        removed = self._index.remove_unmirrored_rows_for_filenames(absent_filenames)
        self._record_event(
            level="WARNING" if source_errors else "INFO",
            event_type="discover_scrape",
            message="Scraped PMXT raw archive URL patterns",
            payload={
                "start_hour": self._config.archive_start_hour.isoformat(),
                "probed_hours": probed,
                "available_hours": available,
                "new_or_changed_hours": discovered,
                "absent_hours": len(absent_filenames),
                "removed_stale_unmirrored_rows": removed,
                "source_errors": source_errors,
                "source_error_samples": source_error_samples,
                "raw_base_urls": list(raw_base_urls),
            },
        )
        if removed > 0:
            LOG.warning("Removed %s stale unmirrored PMXT archive rows after URL scrape", removed)
        return discovered

    @staticmethod
    def _is_better_raw_candidate(
        candidate: _RawUrlCandidate, best_candidate: _RawUrlCandidate | None
    ) -> bool:
        if best_candidate is None:
            return True
        if candidate.content_length is not None and best_candidate.content_length is not None:
            if candidate.content_length != best_candidate.content_length:
                return candidate.content_length > best_candidate.content_length
        if candidate.content_length is not None and best_candidate.content_length is None:
            return True
        if candidate.content_length is None and best_candidate.content_length is not None:
            return False
        return candidate.source_priority < best_candidate.source_priority

    @staticmethod
    def _content_length_from_headers(headers) -> int | None:  # type: ignore[no-untyped-def]
        content_range = headers.get("Content-Range")
        if content_range and "/" in content_range:
            total = content_range.rsplit("/", 1)[1].strip()
            if total and total != "*":
                try:
                    return int(total)
                except ValueError:
                    pass
        length_value = headers.get("Content-Length")
        if not length_value:
            return None
        try:
            return int(length_value)
        except ValueError:
            return None

    def _raw_url_probe(self, source_url: str) -> tuple[bool, int | None]:
        head_request = Request(source_url, method="HEAD", headers={"User-Agent": "pmxt-relay/1.0"})
        try:
            with urlopen(head_request, timeout=self._config.http_timeout_secs) as response:
                return True, self._content_length_from_headers(response.headers)
        except HTTPError as exc:
            if exc.code == 404:
                return False, None
            if exc.code not in {403, 405}:
                raise

        range_request = Request(
            source_url,
            headers={"User-Agent": "pmxt-relay/1.0", "Range": "bytes=0-0"},
        )
        try:
            with urlopen(range_request, timeout=self._config.http_timeout_secs) as response:
                return True, self._content_length_from_headers(response.headers)
        except HTTPError as exc:
            if exc.code == 404:
                return False, None
            raise

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
        if byte_size < _MIN_NONEMPTY_RAW_BYTES:
            return 0
        changed = self._index.register_local_raw(
            filename,
            local_path=str(raw_path),
            content_length=byte_size,
            source_url=f"{self._config.resolved_raw_base_urls[0]}/{filename}",
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
                if self._is_missing_raw(exc):
                    next_retry_at = self._missing_retry_at()
                    self._index.mark_mirror_missing(
                        row["filename"], error=str(exc), next_retry_at=next_retry_at.isoformat()
                    )
                    self._record_event(
                        level="WARNING",
                        event_type="mirror_missing",
                        filename=row["filename"],
                        message=f"Raw archive object missing for {row['filename']}",
                        payload={
                            "error": str(exc),
                            "error_count": next_error_count,
                            "next_retry_at": next_retry_at.isoformat(),
                        },
                    )
                    LOG.warning(
                        "Raw archive object missing for %s until %s: %s",
                        row["filename"],
                        next_retry_at.isoformat(),
                        exc,
                    )
                    continue
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
                if self._should_reclassify_ready_empty_as_missing(row, exc):
                    next_error_count = int(row["error_count"] or 0) + 1
                    next_retry_at = self._missing_retry_at()
                    self._index.mark_mirror_missing(
                        row["filename"], error=str(exc), next_retry_at=next_retry_at.isoformat()
                    )
                    self._record_event(
                        level="WARNING",
                        event_type="verify_empty_missing",
                        filename=row["filename"],
                        message=(
                            f"Upstream empty raw hour unavailable for {row['filename']}; "
                            "moving to missing"
                        ),
                        payload={
                            "error": str(exc),
                            "error_count": next_error_count,
                            "next_retry_at": next_retry_at.isoformat(),
                        },
                    )
                    LOG.warning(
                        "Moving empty raw hour %s to missing after verification failure: %s",
                        row["filename"],
                        exc,
                    )
                    requeued += 1
                    continue
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

    @staticmethod
    def _should_reclassify_ready_empty_as_missing(row, exc: Exception) -> bool:  # type: ignore[no-untyped-def]
        if not isinstance(exc, HTTPError) or exc.code != 404:
            return False
        row_count = row["row_count"]
        content_length = row["content_length"]
        return (row_count is not None and row_count == 0) or (
            content_length is not None and content_length < _MIN_NONEMPTY_RAW_BYTES
        )

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

    def _missing_retry_at(self) -> datetime:
        return datetime.now(UTC) + timedelta(seconds=_MIRROR_QUARANTINE_RETRY_SECS)

    @staticmethod
    def _is_missing_raw(exc: Exception) -> bool:
        return isinstance(exc, HTTPError) and exc.code == 404

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
        should_reuse_existing = row["last_error"] != REMIRROR_CONTENT_CHANGED_REASON
        if should_reuse_existing and raw_path.exists() and raw_path.stat().st_size > 0:
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
