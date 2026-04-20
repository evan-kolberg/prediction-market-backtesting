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
            "Download Telonex Polymarket daily Parquet files into a single-file "
            "DuckDB blob at <destination>/telonex.duckdb. Reads the API key from "
            "TELONEX_API_KEY (supports .env). A resumable manifest lives inside "
            "the same DuckDB file so killed runs restart without re-downloading "
            "committed days."
        )
    )
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path("/Volumes/LaCie/telonex_data"),
        help="Blob store root (default: /Volumes/LaCie/telonex_data).",
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
        help="Channels to download (default: quotes).",
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
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--timeout-secs", type=int, default=60)
    parser.add_argument(
        "--workers", type=int, default=16, help="Parallel download workers (default: 16)."
    )
    parser.add_argument(
        "--db-filename",
        default="telonex.duckdb",
        help="Name of the DuckDB blob file inside --destination (default: telonex.duckdb).",
    )
    args = parser.parse_args()

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
    )
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True, default=str))
    if summary.interrupted:
        return 130
    return 1 if summary.failed_days else 0


if __name__ == "__main__":
    raise SystemExit(main())
