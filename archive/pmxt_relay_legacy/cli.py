from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from aiohttp import web

from pmxt_relay.api import create_app
from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.local_processing import process_local_raw_mirror
from pmxt_relay.worker import RelayWorker


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PMXT relay server")
    parser.add_argument(
        "command",
        choices=("api", "worker", "sync-once", "stats", "process-local"),
        help="Relay command to run",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of hours processed by the selected command",
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
        help="Local raw mirror root for process-local",
    )
    parser.add_argument(
        "--filtered-root",
        type=Path,
        default=None,
        help="Local filtered output root for process-local",
    )
    parser.add_argument(
        "--tmp-root",
        type=Path,
        default=None,
        help="Temporary work directory for process-local",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Parallel local filtered materialization workers for process-local",
    )
    parser.add_argument(
        "--start-hour",
        default=None,
        help="Inclusive UTC hour lower bound for process-local",
    )
    parser.add_argument(
        "--end-hour",
        default=None,
        help="Inclusive UTC hour upper bound for process-local",
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
            processed = (
                0
                if not config.processing_enabled
                else worker._process_pending_hours(limit=args.limit)  # noqa: SLF001
            )
            print(
                json.dumps(
                    {
                        "discovered": discovered,
                        "mirrored": mirrored,
                        "processed": processed,
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

    if args.command == "process-local":
        if args.raw_root is None or args.filtered_root is None:
            parser.error(
                "--raw-root and --filtered-root are required for process-local"
            )
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

    parser.error(f"Unsupported command: {args.command}")
    return 2
