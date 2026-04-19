from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from scripts._telonex_data_download import download_telonex_days  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Download Telonex Polymarket daily Parquet files into a local mirror. "
            "Reads the API key from the TELONEX_API_KEY environment variable only "
            "(never from CLI args). Files are written in the layout the Telonex "
            "loader expects: "
            "<destination>/polymarket/<channel>/<market_slug>/<outcome-or-outcome_id>/<YYYY-MM-DD>.parquet."
        )
    )
    parser.add_argument(
        "--destination",
        type=Path,
        default=Path("/Volumes/LaCie/telonex_data"),
        help="Local root directory, e.g. /Volumes/LaCie/telonex_data",
    )
    parser.add_argument(
        "--market-slug",
        action="append",
        required=True,
        help="Polymarket market slug. Repeat to warm more than one market.",
    )
    outcome_group = parser.add_mutually_exclusive_group(required=True)
    outcome_group.add_argument(
        "--outcome",
        default=None,
        help="Outcome label for the on-disk segment (e.g. Yes, No).",
    )
    outcome_group.add_argument(
        "--outcome-id",
        type=int,
        default=None,
        help="Outcome index for the on-disk segment (e.g. 0, 1).",
    )
    parser.add_argument("--channel", default="quotes")
    parser.add_argument("--start-date", required=True, help="Inclusive UTC start date, YYYY-MM-DD.")
    parser.add_argument("--end-date", required=True, help="Inclusive UTC end date, YYYY-MM-DD.")
    parser.add_argument(
        "--api-base-url",
        default="https://api.telonex.io",
        help="Override the Telonex API base URL. The API key still comes from TELONEX_API_KEY.",
    )
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--no-progress", action="store_true")
    parser.add_argument("--timeout-secs", type=int, default=60)
    args = parser.parse_args()

    summary = download_telonex_days(
        destination=args.destination,
        market_slugs=args.market_slug,
        outcome=args.outcome,
        outcome_id=args.outcome_id,
        channel=args.channel,
        base_url=args.api_base_url,
        start_date=args.start_date,
        end_date=args.end_date,
        overwrite=args.overwrite,
        timeout_secs=max(1, args.timeout_secs),
        show_progress=not args.no_progress,
    )
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    incomplete = summary.failed_days
    return 1 if incomplete else 0


if __name__ == "__main__":
    raise SystemExit(main())
