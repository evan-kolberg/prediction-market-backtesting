from __future__ import annotations

import argparse
import json
import logging

from aiohttp import web

from pmxt_relay.api import create_app
from pmxt_relay.config import RelayConfig
from pmxt_relay.index_db import RelayIndex
from pmxt_relay.worker import RelayWorker


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="PMXT relay server")
    parser.add_argument(
        "command",
        choices=("api", "worker", "sync-once", "prebuild-filtered", "stats"),
        help="Relay command to run",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of hours processed by the selected command",
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
        RelayWorker(config).run_forever()
        return 0

    if args.command == "sync-once":
        if args.limit is None:
            RelayWorker(config).run_once()
        else:
            worker = RelayWorker(config)
            discovered = worker._discover_archive_hours()  # noqa: SLF001
            mirrored = worker._mirror_pending_hours()  # noqa: SLF001
            processed = worker._process_pending_hours(limit=args.limit)  # noqa: SLF001
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

    if args.command == "prebuild-filtered":
        worker = RelayWorker(config)
        count = worker._prebuild_filtered_hours(limit=args.limit)  # noqa: SLF001
        print(json.dumps({"prebuilt_hours": count}, indent=2, sort_keys=True))
        return 0

    if args.command == "stats":
        index = RelayIndex(config.db_path)
        index.initialize()
        print(json.dumps(index.stats(), indent=2, sort_keys=True))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2
