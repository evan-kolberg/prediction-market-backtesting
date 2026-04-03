from __future__ import annotations

import argparse
import json
from pathlib import Path

from pmxt_relay.local_processing import process_local_raw_mirror


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Process a local PMXT raw mirror into filtered market-hour parquet"
    )
    parser.add_argument("--vendor", choices=("pmxt",), default="pmxt")
    parser.add_argument("--raw-root", type=Path, required=True)
    parser.add_argument("--filtered-root", type=Path, required=True)
    parser.add_argument("--tmp-root", type=Path, default=None)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--start-hour", default=None)
    parser.add_argument("--end-hour", default=None)
    args = parser.parse_args()

    summary = process_local_raw_mirror(
        vendor=args.vendor,
        raw_root=args.raw_root,
        filtered_root=args.filtered_root,
        tmp_root=args.tmp_root,
        workers=args.workers,
        limit=args.limit,
        start_hour=args.start_hour,
        end_hour=args.end_hour,
    )
    print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
