from __future__ import annotations

import logging
import os
import shutil
import time
from urllib.error import HTTPError
from urllib.request import Request
from urllib.request import urlopen

from pmxt_relay.archive import extract_archive_filenames
from pmxt_relay.archive import fetch_archive_page
from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.storage import raw_relative_path


LOG = logging.getLogger(__name__)


class RelayWorker:
    def __init__(
        self,
        config: RelayConfig,
        *,
        reset_inflight: bool = True,
        reset_mirror_inflight: bool = True,
        reset_process_inflight: bool = False,
        reset_prebuild_inflight: bool = False,
        skip_prebuild: bool = True,
    ) -> None:
        del skip_prebuild
        self._config = config
        self._config.ensure_directories()
        self._index = RelayIndex(config.db_path, event_retention=config.event_retention)
        reset_mirror, reset_process, reset_prebuild = self._index.initialize(
            reset_inflight=reset_inflight,
            reset_mirror_inflight=reset_mirror_inflight,
            reset_process_inflight=reset_process_inflight,
            reset_prebuild_inflight=reset_prebuild_inflight,
        )
        retired = self._index.disable_processing_backlog()
        if retired > 0:
            self._record_event(
                level="INFO",
                event_type="processing_disabled",
                message="Disabled legacy processing backlog for mirror-only relay mode",
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
        total = discovered + adopted + mirrored
        self._record_event(
            level="INFO",
            event_type="cycle_complete",
            message="Relay cycle complete",
            payload={
                "discovered": discovered,
                "adopted": adopted,
                "mirrored": mirrored,
            },
        )
        LOG.info(
            "Relay cycle complete: discovered=%s adopted=%s mirrored=%s",
            discovered,
            adopted,
            mirrored,
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
        if adopted > 0:
            self._index.disable_processing_backlog()
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
                processing_enabled=False,
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
            processing_enabled=False,
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
