# Mirror And Relay Ops

This page covers the two PMXT infrastructure paths that still matter in this
repository:

- the active mirror-only relay in [`pmxt_relay/`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)
- the archived relay snapshot in [`archive/pmxt_relay_legacy/`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/README.md)

The active public recommendation is still local-first:

1. mirror raw archive hours to local disk
2. point runners at those raws directly
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

- expose non-raw market data routes
- own any repo-level local replay preparation workflow

The current deployment and operations details live in:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)

Operational note:

- the public relay status badge reports relay health only
- the public PMXT upstream badge reports unresolved mirror errors or lag

The archived relay under `archive/pmxt_relay_legacy/` is historical context
only. It is not part of the active public relay path.

## PC-Side Alternative

The active relay docs here stay focused on VPS infrastructure. If you only need
a one-off local raw download for a PC or external drive, use the local workflow
docs instead:

- [`docs/pmxt-byod.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/docs/pmxt-byod.md)
- [`docs/setup.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/docs/setup.md)

## Archived Relay Snapshot

If you need historical context, start with these archive docs:

- [`archive/pmxt_relay_legacy/ARCHIVE.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/ARCHIVE.md)
- [`archive/pmxt_relay_legacy/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/README.md)

If you only need the active mirror service, use:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)
