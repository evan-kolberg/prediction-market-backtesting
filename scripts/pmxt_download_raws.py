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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download PMXT raw archive hours into a local mirror. With no time "
            "window, the script discovers all archive hours and downloads them "
            "newest-first to the destination using archive first and relay as "
            "fallback, then reports missing and zero-row local hours."
        )
    )
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--archive-listing-url", default="https://archive.pmxt.dev/Polymarket")
    parser.add_argument("--archive-base-url", default="https://r2.pmxt.dev")
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

    summary = download_raw_hours(
        destination=args.destination,
        archive_listing_url=args.archive_listing_url,
        archive_base_url=args.archive_base_url,
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
    return 1 if summary.failed_hours else 0


if __name__ == "__main__":
    raise SystemExit(main())
