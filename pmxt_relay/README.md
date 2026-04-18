# PMXT Relay

`pmxt_relay/` is the VPS deployment subtree for the active PMXT mirror service.
Keep this folder limited to server infrastructure, service code, and deploy
artifacts. PC-side download helpers and other local workflows should live
outside `pmxt_relay/`.

Current direction:

- mirror raw PMXT archive hours onto disk
- optionally expose those mirrored raw files over `/v1/raw/*`
- keep the active relay scoped to raw mirroring and raw file serving

## Active Commands

Mirror worker:

```bash
uv run python -m pmxt_relay worker
```

Mirror API:

```bash
uv run python -m pmxt_relay api
```

## Directory Layout

Default relay-owned state:

```text
/srv/pmxt-relay/
  raw/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
  state/relay.sqlite3
  tmp/
```

The active relay only needs:

- `raw/` for mirrored PMXT hours
- `state/` for SQLite metadata
- `tmp/` for atomic download temp files

On startup the worker adopts any already mirrored local raw hours into the
state DB. During steady-state polling it only checks for incremental additions
instead of rescanning the full raw tree every cycle.

Archive discovery does not trust listing pages. Each worker cycle walks the
expected hourly filenames from the current UTC hour back through
`2026-02-21T16:00:00+00:00`, probes `r2v2.pmxt.dev` and `r2.pmxt.dev` by direct
raw URL, and queues the largest object when both origins have the same hour.
If neither origin has the hour, stale unmirrored queue rows for that filename
are removed.

Repeated upstream 404s are no longer retried every poll forever. The active
relay now backs off failed mirrors and temporarily quarantines repeated 404s on
a slower retry cadence so one stale archive reference does not dominate every
worker cycle while the mirror still heals automatically when upstream recovers.

Each cycle also re-validates a batch of already-mirrored files by HEAD-checking
upstream for ETag or Content-Length changes. Files corrected upstream (e.g.
initially broken uploads later replaced with larger files) are automatically
re-queued for download. The batch size is configurable via
`PMXT_RELAY_VERIFY_BATCH_SIZE` (default 50). Parquet row counts and byte sizes
are tracked to detect empty/broken files.

## Self-Healing Coverage

Coverage metrics stay deliberately simple:

- `dump_files_on_disk` / `mirrored_hours` counts valid hour files,
  `polymarket_orderbook_*.parquet` dump files physically present under
  `raw/`.
- `archive_hours` counts hours since the first expected archive hour,
  elapsed UTC hours from
  `PMXT_RELAY_ARCHIVE_START_HOUR` through the current UTC hour.

The mirror queue still tracks pending, active, retrying, and quarantined work
in SQLite, but those states do not define the public coverage denominator. The
overlap rule is also intentionally simple: when both raw origins expose the same
hour, keep the URL with the larger reported `Content-Length`; if sizes tie or
are unknown, prefer the earlier configured origin.

## Fresh Box Setup

On a fresh Ubuntu 24 box:

```bash
apt-get update
apt-get install -y git curl python3 python3-venv ufw fail2ban
curl -LsSf https://astral.sh/uv/install.sh | sh

git clone https://github.com/evan-kolberg/prediction-market-backtesting.git /opt/prediction-market-backtesting
cd /opt/prediction-market-backtesting

uv venv --python 3.12
uv pip install "nautilus_trader[polymarket,visualization]==1.225.0" bokeh plotly numpy py-clob-client duckdb textual

useradd --system --home /srv/pmxt-relay --shell /usr/sbin/nologin pmxtrelay || true
install -o pmxtrelay -g pmxtrelay -d /srv/pmxt-relay /srv/pmxt-relay/raw /srv/pmxt-relay/state /srv/pmxt-relay/tmp

cp pmxt_relay/systemd/pmxt-relay.env.example /etc/pmxt-relay.env
cp pmxt_relay/systemd/pmxt-relay-api.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-relay-worker.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-disable-wbt.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now pmxt-disable-wbt.service
systemctl enable --now pmxt-relay-api.service
systemctl enable --now pmxt-relay-worker.service
```

If you front the relay with Caddy, nginx, or another reverse proxy, serve
`/v1/raw/*` directly from `/srv/pmxt-relay/raw` when possible instead of
proxying large parquet downloads through Python.

Edit `/etc/pmxt-relay.env` before starting the services. The active public
relay defaults to direct raw URL probing against `r2v2.pmxt.dev` and
`r2.pmxt.dev`.

Important env knobs from `pmxt_relay/systemd/pmxt-relay.env.example`:

- `PMXT_RELAY_DATA_DIR` for relay-owned state under `/srv/pmxt-relay`
- `PMXT_RELAY_RAW_BASE_URLS` for ordered raw origins to probe by filename
  pattern. The default public order is `https://r2v2.pmxt.dev`,
  `https://r2.pmxt.dev`.
- `PMXT_RELAY_ARCHIVE_START_HOUR` for the first expected archive hour. The
  public PMXT mirror uses `2026-02-21T16:00:00+00:00`.
- `PMXT_RELAY_TRUSTED_PROXY_IPS` if the API sits behind Caddy, nginx, or
  another reverse proxy and should trust forwarded client IPs from that proxy
- `PMXT_RELAY_VERIFY_BATCH_SIZE` (default 50) how many mirrored files to
  re-validate per cycle

After startup, verify the deployment with:

```bash
systemctl is-active pmxt-relay-api.service pmxt-relay-worker.service
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/v1/stats
curl -fsS http://127.0.0.1:8080/v1/system
```

## API Surface

Active mirror-focused endpoints:

- `GET /healthz`
- `GET /v1/stats`
- `GET /v1/queue`
- `GET /v1/events?limit=100`
- `GET /v1/inflight`
- `GET /v1/system`
- `GET /v1/raw/{yyyy/mm/dd/filename}`
- mirror/system badge endpoints under `/v1/badge/*`

`/v1/stats`, `/v1/queue`, and the active badge routes only expose raw-mirror
state. The active relay path is limited to mirroring, health, and raw file
serving.

The public badges separate relay health from raw-origin availability:

- `/v1/badge/status(.svg)` reports whether the relay itself is up, recent, and
  has active API/worker services.
- `/v1/badge/upstream(.svg)` reports whether recent `r2v2.pmxt.dev` polling is
  online or offline.
- `/v1/badge/upstream-r2(.svg)` reports the same polling health for
  `r2.pmxt.dev`.
- `/v1/badge/hour-files(.svg)` reports actual hour files on disk.
- `/v1/badge/hours-since-first(.svg)` reports elapsed hours since
  `2026-02-21T16:00:00+00:00`.
- `/v1/badge/latest-hour.svg` reports the latest mirrored hour on disk.
