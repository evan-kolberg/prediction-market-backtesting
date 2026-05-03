from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)
load_dotenv()

from scripts._telonex_data_download import (  # noqa: E402
    VALID_CHANNELS,
    download_telonex_days,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download Telonex Polymarket daily data into Hive-partitioned Parquet "
            "files under <destination>/data/ with a DuckDB manifest at "
            "<destination>/telonex.duckdb tracking completed days. Reads the API "
            "key from TELONEX_API_KEY (supports .env). Killed runs restart "
            "without re-downloading committed days."
        )
    )
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path("/Volumes/storage/telonex_data"),
        help="Blob store root (default: /Volumes/storage/telonex_data).",
    )
    parser.add_argument(
        "--all-markets",
        action="store_true",
        help=(
            "Scrape every market listed in /v1/datasets/polymarket/markets. "
            "Walks each market's published availability window for the selected channels."
        ),
    )
    parser.add_argument(
        "--market-slug",
        action="append",
        default=None,
        help="Polymarket market slug. Repeat to add more. Required unless --all-markets.",
    )
    outcome_group = parser.add_mutually_exclusive_group()
    outcome_group.add_argument("--outcome", default=None)
    outcome_group.add_argument("--outcome-id", type=int, default=None)
    parser.add_argument(
        "--outcomes-for-all",
        type=int,
        nargs="+",
        default=None,
        help="Outcome ids to scrape when --all-markets is set (default: 0 1).",
    )
    parser.add_argument(
        "--channel",
        choices=VALID_CHANNELS,
        default=None,
        help="Single channel shortcut (equivalent to --channels).",
    )
    parser.add_argument(
        "--channels",
        nargs="+",
        choices=VALID_CHANNELS,
        default=None,
        help="Channels to download (default: book_snapshot_full).",
    )
    parser.add_argument("--start-date", default=None, help="Inclusive UTC start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Inclusive UTC end date YYYY-MM-DD.")
    parser.add_argument(
        "--status",
        default=None,
        help="Filter --all-markets by status field (e.g. resolved, unopened).",
    )
    parser.add_argument(
        "--api-base-url",
        default="https://api.telonex.io",
        help="Override the Telonex API base URL.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument(
        "--max-days",
        type=int,
        default=None,
        help=(
            "Stop after this many post-resume day jobs. Useful for smoke-testing "
            "--all-markets without starting the full mirror."
        ),
    )
    parser.add_argument(
        "--recheck-empty-after-days",
        type=int,
        default=7,
        help=(
            "Reuse cached 404 day markers only while they are this many days old "
            "(default: 7). Use 0 to recheck 404s every run, or -1 to keep 404s "
            "forever unless --overwrite is used."
        ),
    )
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--timeout-secs", type=int, default=60)
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help=(
            "Concurrent in-flight downloads (default: 16). Increase only after "
            "checking RSS on book_snapshot_full downloads; transient 429/503 are "
            "retried automatically."
        ),
    )
    parser.add_argument(
        "--parse-workers",
        type=int,
        default=None,
        help=(
            "Concurrent Arrow parquet decode workers (default: min(8, cpu_count); "
            "also configurable with TELONEX_PARSE_WORKERS). Increase only when "
            "RAM headroom is available."
        ),
    )
    parser.add_argument(
        "--writer-queue-items",
        type=int,
        default=None,
        help=(
            "Maximum parsed day results waiting for the writer before applying "
            "backpressure (default: 128; also configurable with "
            "TELONEX_WRITER_QUEUE_ITEMS)."
        ),
    )
    parser.add_argument(
        "--pending-commit-items",
        type=int,
        default=None,
        help=(
            "Maximum completed day results the writer holds before committing "
            "to the manifest (default: 128; also configurable with "
            "TELONEX_PENDING_COMMIT_ITEMS)."
        ),
    )
    parser.add_argument(
        "--db-filename",
        default="telonex.duckdb",
        help="Name of the DuckDB manifest file inside --destination (default: telonex.duckdb).",
    )
    args = parser.parse_args()

    try:
        summary = download_telonex_days(
            destination=args.destination,
            market_slugs=args.market_slug,
            outcome=args.outcome,
            outcome_id=args.outcome_id,
            channel=args.channel,
            channels=args.channels,
            base_url=args.api_base_url,
            start_date=args.start_date,
            end_date=args.end_date,
            all_markets=args.all_markets,
            status_filter=args.status,
            outcomes_for_all=args.outcomes_for_all,
            overwrite=args.overwrite,
            timeout_secs=max(1, args.timeout_secs),
            workers=max(1, args.workers),
            show_progress=not args.no_progress,
            db_filename=args.db_filename,
            recheck_empty_after_days=(
                None if args.recheck_empty_after_days < 0 else args.recheck_empty_after_days
            ),
            parse_workers=args.parse_workers,
            writer_queue_items=args.writer_queue_items,
            pending_commit_items=args.pending_commit_items,
            max_days=args.max_days,
        )
    except KeyboardInterrupt:
        # SIGINT/SIGTERM landed in a blocking call outside the job loop (usually
        # the markets-catalog fetch). The store closed itself via the library's
        # finally block; nothing more to report.
        print('{"interrupted": true, "message": "cancelled before job loop started"}')
        return 130
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True, default=str))
    if summary.interrupted:
        return 130
    return 1 if summary.failed_days else 0


if __name__ == "__main__":
    raise SystemExit(main())
