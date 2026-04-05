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

## Local-First Alternative

If you do not want to run storage-heavy infrastructure for raw mirrors, the
recommended path is much simpler:

- download the raw dumps to a local drive
- point runners at that raw mirror with `local:/path`
- let the normal loader cache warm itself as you replay

A large local or external drive is usually enough:

![External drive for local PMXT dumps](https://www.digitaltrends.com/tachyon/2017/03/Lacie-Rugged-4gb-HD-inhandscale.jpg?resize=1200%2C720)

That path avoids VPS relay storage pressure and usually keeps first-pass replay
fast enough if the raw dump is on a decent SSD or a fast external drive.

The repo-level downloader for that workflow is:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The local mirror download is expected to run for a while and report progress in
place. Example output:

```text
Downloading PMXT raws:  13%|███████████████████████▍| 137/1017 [41:27<3:37:59, 14.86s/hour, archive 2026-02-27T11:00:00+00:00 392.0/445.9 MiB]
```

The exact percent, hour count, timestamp, source, and transferred bytes depend
on the archive state and the mirror window.

## Archived Relay Snapshot

If you need historical context, start with these archive docs:

- [`archive/pmxt_relay_legacy/ARCHIVE.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/ARCHIVE.md)
- [`archive/pmxt_relay_legacy/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/archive/pmxt_relay_legacy/README.md)

If you only need the active mirror service, use:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)
