from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from scripts._pmxt_raw_download import download_raw_hours  # noqa: E402


def _parse_archive_source(value: str) -> tuple[str, str]:
    if "|" not in value:
        raise argparse.ArgumentTypeError(
            "Archive sources must use LISTING_URL|RAW_BASE_URL syntax."
        )
    listing_url, base_url = (part.strip() for part in value.split("|", maxsplit=1))
    if not listing_url or not base_url:
        raise argparse.ArgumentTypeError(
            "Archive sources must include both LISTING_URL and RAW_BASE_URL."
        )
    return listing_url, base_url


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download PMXT v2 raw archive hours into a local mirror. With no time "
            "window, the script discovers all archive hours and downloads them "
            "newest-first to the destination using archive first and relay as "
            "fallback, then reports missing and zero-row local hours."
        )
    )
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--archive-listing-url", default=None)
    parser.add_argument("--archive-base-url", default=None)
    parser.add_argument(
        "--archive-source",
        action="append",
        type=_parse_archive_source,
        default=[],
        help=(
            "Archive source pair in LISTING_URL|RAW_BASE_URL form. May be repeated. "
            "Defaults to PMXT Polymarket v2 first, then v1."
        ),
    )
    parser.add_argument("--relay-base-url", default="https://209-209-10-83.sslip.io")
    parser.add_argument(
        "--source",
        action="append",
        choices=("archive", "relay"),
        default=[],
        help="Download source order. Defaults to archive first, then relay.",
    )
    parser.add_argument("--start-time", default=None)
    parser.add_argument("--end-time", default=None)
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--timeout-secs", type=int, default=60)
    parser.add_argument("--discovery-stale-pages", type=int, default=1)
    parser.add_argument("--discovery-max-pages", type=int, default=None)
    args = parser.parse_args()

    archive_sources = args.archive_source or None
    archive_listing_url = args.archive_listing_url
    archive_base_url = args.archive_base_url
    if archive_sources is None and (archive_listing_url is None and archive_base_url is None):
        archive_sources = [
            ("https://archive.pmxt.dev/Polymarket/v2", "https://r2v2.pmxt.dev"),
            ("https://archive.pmxt.dev/Polymarket/v1", "https://r2.pmxt.dev"),
        ]
        archive_listing_url = "https://archive.pmxt.dev/Polymarket/v2"
        archive_base_url = "https://r2v2.pmxt.dev"
    else:
        archive_listing_url = archive_listing_url or "https://archive.pmxt.dev/Polymarket/v2"
        archive_base_url = archive_base_url or "https://r2v2.pmxt.dev"

    summary = download_raw_hours(
        destination=args.destination,
        archive_listing_url=archive_listing_url,
        archive_base_url=archive_base_url,
        archive_sources=archive_sources,
        relay_base_url=args.relay_base_url,
        source_order=args.source or None,
        start_time=args.start_time,
        end_time=args.end_time,
        overwrite=args.overwrite,
        timeout_secs=max(1, args.timeout_secs),
        show_progress=not args.no_progress,
        discovery_stale_pages=max(1, args.discovery_stale_pages),
        discovery_max_pages=args.discovery_max_pages,
    )
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    incomplete = summary.failed_hours or summary.missing_local_hours or summary.empty_local_hours
    return 1 if incomplete else 0


if __name__ == "__main__":
    raise SystemExit(main())
