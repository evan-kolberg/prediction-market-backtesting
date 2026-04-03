from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from aiohttp import web

from pmxt_relay.api import create_app
from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.raw_mirror_verifier import verify_local_raw_mirror
from pmxt_relay.worker import RelayWorker


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PMXT raw mirror utilities")
    parser.add_argument(
        "command",
        choices=(
            "api",
            "worker",
            "sync-once",
            "stats",
            "verify-raw-mirror",
        ),
        help="Relay command to run",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of hours mirrored by the selected command",
    )
    parser.add_argument(
        "--vendor",
        choices=("pmxt",),
        default="pmxt",
        help="Local processing vendor adapter to use",
    )
    parser.add_argument(
        "--raw-root",
        type=Path,
        default=None,
        help="Local raw mirror root for verify-raw-mirror",
    )
    parser.add_argument(
        "--skip-upstream-head",
        action="store_true",
        help="Skip upstream HEAD checks during verify-raw-mirror",
    )
    parser.add_argument(
        "--skip-parquet-check",
        action="store_true",
        help="Skip parquet metadata validation during verify-raw-mirror",
    )
    parser.add_argument(
        "--upstream-head-concurrency",
        type=int,
        default=32,
        help="Concurrent upstream HEAD probes for verify-raw-mirror",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = RelayConfig.from_env()
    config.ensure_directories()

    if args.command == "api":
        web.run_app(create_app(config), host=config.bind_host, port=config.bind_port)
        return 0

    if args.command == "worker":
        RelayWorker(
            config,
            reset_prebuild_inflight=False,
            skip_prebuild=True,
        ).run_forever()
        return 0

    if args.command == "sync-once":
        if args.limit is None:
            RelayWorker(
                config,
                reset_prebuild_inflight=False,
                skip_prebuild=True,
            ).run_once()
        else:
            worker = RelayWorker(
                config,
                reset_prebuild_inflight=False,
                skip_prebuild=True,
            )
            discovered = worker._discover_archive_hours()  # noqa: SLF001
            mirrored = worker._mirror_pending_hours()  # noqa: SLF001
            print(
                json.dumps(
                    {
                        "discovered": discovered,
                        "mirrored": mirrored,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        return 0

    if args.command == "stats":
        index = RelayIndex(config.db_path)
        index.initialize(apply_maintenance=False)
        print(json.dumps(index.stats(), indent=2, sort_keys=True))
        return 0

    if args.command == "verify-raw-mirror":
        if args.raw_root is None:
            parser.error("--raw-root is required for verify-raw-mirror")
        summary = verify_local_raw_mirror(
            vendor=args.vendor,
            raw_root=args.raw_root,
            archive_listing_url=config.archive_listing_url,
            raw_base_url=config.raw_base_url,
            timeout_secs=config.http_timeout_secs,
            stale_pages=config.archive_stale_pages,
            max_pages=config.archive_max_pages,
            check_upstream=not args.skip_upstream_head,
            check_parquet=not args.skip_parquet_check,
            upstream_head_concurrency=args.upstream_head_concurrency,
        )
        print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2
