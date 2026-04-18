# Mirror And Relay Ops

This page covers the two PMXT infrastructure paths that still matter in this
repository:

- the active mirror-only relay in [`pmxt_relay/`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/pmxt_relay/README.md)

The active public recommendation is still local-first:

1. mirror raw archive hours to local disk
2. point runners at those raws directly
3. use a shared server only for raw mirroring and raw file serving

## Active Mirror Service

The active `pmxt_relay/` service is mirror-only.

What it does today:

- discovers PMXT archive hours
- adopts already mirrored local raw hours on startup
- mirrors raw parquet files onto disk
- exposes raw files under `/v1/raw/*`
- serves health, queue, stats, system, events, inflight, and badge endpoints
- backs off failed mirror retries and temporarily quarantines repeated upstream
  404s on a slower retry cadence
- avoids rescanning the full raw tree on every steady-state worker cycle

What it does not do anymore:

- expose non-raw market data routes
- own any repo-level local replay preparation workflow

The current deployment and operations details live in:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/pmxt_relay/README.md)

Operational note:

- the public relay status badge reports relay health only
- the public PMXT upstream badge reports source polling as online or offline;
  missing archive hours, zero-row mirrored files, and listed-but-uncovered
  mirror rows have separate badges

Deployment facts for the active box:

- live checkout path: `/opt/prediction-market-backtesting`
- env file: `/etc/pmxt-relay.env`
- active PMXT archive sources:
  `https://archive.pmxt.dev/Polymarket/v2|https://r2v2.pmxt.dev`,
  then `https://archive.pmxt.dev/Polymarket/v1|https://r2.pmxt.dev`
- systemd units:
  `pmxt-relay-api.service` and `pmxt-relay-worker.service`
- public URL:
  `https://209-209-10-83.sslip.io`
- trusted-proxy env var:
  `PMXT_RELAY_TRUSTED_PROXY_IPS`

Useful health checks:

```bash
systemctl is-active pmxt-relay-api.service pmxt-relay-worker.service
curl -fsS https://209-209-10-83.sslip.io/healthz
curl -fsS https://209-209-10-83.sslip.io/v1/stats
curl -fsS https://209-209-10-83.sslip.io/v1/system
```

## PC-Side Alternative

The active relay docs here stay focused on VPS infrastructure. If you only need
a one-off local raw download for a PC or external drive, use the local workflow
docs instead:

- [`docs/pmxt-byod.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/docs/pmxt-byod.md)
- [`docs/setup.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/docs/setup.md)

If you only need the active mirror service, use:

- [`pmxt_relay/README.md`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/pmxt_relay/README.md)
