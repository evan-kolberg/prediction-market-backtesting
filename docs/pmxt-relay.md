# Mirror And Relay Ops

This page covers the two PMXT infrastructure paths that still matter in this
repository:

- the active mirror-only relay in [`pmxt_relay/`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)
- the archived full-stack relay snapshot in [`archive/pmxt_relay_legacy/`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/README.md)

The active public recommendation is still local-first:

1. mirror raw archive hours to local disk
2. process them locally
3. use a shared server only for raw mirroring and raw file serving

## Active Mirror Service

The active `pmxt_relay/` service is mirror-only.

What it does today:

- discovers PMXT archive hours
- mirrors raw parquet files onto disk
- exposes raw files under `/v1/raw/*`
- serves health, queue, stats, system, events, inflight, and badge endpoints
- backs off failed mirror retries and temporarily quarantines repeated upstream
  404s on a slower retry cadence

What it does not do anymore:

- server-side filtered processing
- ClickHouse-backed filtered serving
- `/v1/filtered/*`
- processing or prebuild badge/status stages

The current deployment and operations details live in:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)

Operational note:

- the public relay status badge reports relay health only
- the public PMXT upstream badge reports unresolved mirror errors or lag

## Legacy Self-Hosted Relay

The archived relay under `archive/pmxt_relay_legacy/` preserves the older
full-stack pattern: mirror raw data, process it on the server, and serve the
processed outputs quickly to multiple users.

That archived design is not the repo's active public deployment path anymore,
but it is still a useful reference if you want to run a private processed-data
service on infrastructure you control.

Good fit:

- you have a storage-rich Linux box on your own network
- you want one shared processed-data service for yourself or a small team
- you are willing to own the operational complexity of the older stack

That can include a NAS-backed host or a NAS-class Linux machine that you manage
like a normal server:

![NAS-style self-hosted infrastructure](https://cdn.mos.cms.futurecdn.net/aJimJfHgV2uck6KefQMFzf.jpg)

Accuracy note:

- `archive/pmxt_relay_legacy/` is an archived reference snapshot
- it is not the code path this repo now deploys publicly
- if you revive it, treat it as your own self-hosted fork and validate it on
  your own infrastructure

## Storage-Rich Team Deployment

If you do have that self-hosting capacity, the archived full-stack relay can be
a reasonable pattern for one purpose: keep processed vendor data close to the
team that uses it.

That architecture can make repeated team pulls fast because the expensive work
has already happened once on your own box. The main tradeoff is that you now
own:

- disk planning
- service restarts
- schema/state drift
- operational debugging

For a private team environment, that may be worth it. For the public relay in
this repo, it is not.

## Local-First Alternative

If you do not want to run storage-heavy infrastructure for processed data, the
recommended path is much simpler:

- download the raw dumps to a local drive
- process them locally
- keep your own filtered cache warm

A large local or external drive is usually enough:

![External drive for local PMXT dumps](https://www.digitaltrends.com/tachyon/2017/03/Lacie-Rugged-4gb-HD-inhandscale.jpg?resize=1200%2C720)

That path avoids VPS processing costs and usually keeps first-pass processing
fast enough if the raw dump is on a decent SSD or a fast external drive.

The repository's preferred command for that workflow is:

```bash
uv run python scripts/pmxt_process_local.py \
  --raw-root /data/pmxt/raw \
  --filtered-root ~/.cache/nautilus_trader/pmxt
```

## Archived Full-Stack Relay

If you need the older design, start with these archive docs:

- [`archive/pmxt_relay_legacy/ARCHIVE.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/ARCHIVE.md)
- [`archive/pmxt_relay_legacy/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/README.md)

If you only need the active mirror service, use:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)
