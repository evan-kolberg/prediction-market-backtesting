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
        choices=("api", "worker", "sync-once", "stats"),
        help="Relay command to run",
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
        RelayWorker(config).run_once()
        return 0

    if args.command == "stats":
        index = RelayIndex(config.db_path)
        index.initialize()
        print(json.dumps(index.stats(), indent=2, sort_keys=True))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2
