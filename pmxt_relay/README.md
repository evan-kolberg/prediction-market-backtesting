# PMXT Relay

`pmxt_relay/` is a self-hosted PMXT acceleration layer for the backtests in this
repo.

> UNDER CONSTRUCTION: the supported relay deployment path is now ClickHouse-only.
> This document focuses on the sustainable raw -> ClickHouse workflow instead of
> the old tiny-file fanout.

The relay API now has an explicit filtered-hour store seam in code, so the HTTP
surface can stay stable while the backend moves away from tiny filesystem
fanout and toward a database-backed query path.

What it does:

- mirrors every hourly raw PMXT Polymarket archive parquet file
- keeps polling the PMXT archive index for new hours
- parses each raw hour into a canonical filtered event stream with extracted
  `market_id`, `token_id`, and `relay_row_index`
- in ClickHouse mode, writes those filtered rows straight into a MergeTree table
  instead of exploding them into millions of tiny parquet files
- keeps the relay HTTP surface stable so the backtest loader can keep asking for
  filtered market hours without caring whether the backend is filesystem or
  ClickHouse-backed

The relay keeps the exact filtered shape the loader already uses today:

- `update_type`
- `data`

That means the backtests do not need a new schema. They just need a faster
source for already-filtered hours. In ClickHouse mode, those rows are stored in
one table keyed by `(condition_id, token_id, hour, relay_row_index)` instead of
one parquet file per `(condition_id, token_id, hour)`.

## Pipeline Stages

Each hourly archive file moves through four stages:

| Stage       | DB column             | Meaning                                           |
|-------------|-----------------------|---------------------------------------------------|
| Discovered  | `mirror_status`       | Filename found on PMXT archive index              |
| Mirrored    | `mirror_status=ready` | Raw parquet downloaded to `raw/`                  |
| Processed   | `process_status=ready`| Raw hour parsed into filtered relay rows          |
| Query-ready | completed flag | Internal completion state meaning the hour is ready to serve |

**"Processed" in the badges/stats means query-ready output**, not just "we
downloaded the raw parquet." The public badges and `/v1/stats` use this
definition:

- `mirrored` badge: `mirror_status=ready` / total discovered
- `processed` badge: completed query-ready hours / mirrored hours
- `rate` badge: rolling 24-hour completed-hour rate based on the completion timestamp
- `file` badge: current parquet filename from the latest
  progress event while work is active
- `rows` badge: full `processed_rows / total_rows` fraction from the latest
  progress event

The worker also adopts any raw parquet files that already exist under `raw/`
into the current relay state. That keeps `mirrored_hours` aligned with the
actual local raw inventory instead of only counting hours mirrored by the
current worker process.

The inflight reset on startup is split by stage so worker restarts do not
leave hours stranded in a fake in-progress state:

- Worker resets: mirror + process inflight
- the internal completion flag is preserved unless an explicit maintenance step resets it

## Directory Layout

By default the relay stores relay-owned state under `/srv/pmxt-relay`:

```text
/srv/pmxt-relay/
  raw/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
  state/relay.sqlite3
  tmp/
```

This means the relay keeps two persistent layers:

- `raw/` stores the mirrored PMXT archive hours as-is
- `state/` stores the relay SQLite metadata/index

When `PMXT_RELAY_FILTERED_STORE_BACKEND=clickhouse`, the parsed filtered rows
live in ClickHouse instead of `processed/` and `filtered/`. On a clean box,
those legacy directories should not exist at all.

Temporary `.tmp` files only exist during atomic writes.

## Processes

For the new ClickHouse-backed path, run two long-lived services:

- API: `uv run python -m pmxt_relay api`
- Worker: `uv run python -m pmxt_relay worker`

The worker mirrors raw hours and inserts filtered rows directly into ClickHouse.
The worker handles discovery, mirroring, and ClickHouse ingestion:

1. scrapes `https://archive.pmxt.dev/data/Polymarket?page=N`
2. discovers every hourly archive filename
3. downloads missing raw parquet files from `https://r2.pmxt.dev`
4. streams each raw hour through Arrow, extracts `token_id` from `data`, and
   inserts the filtered relay rows into ClickHouse
5. keeps polling for new hours

The ClickHouse worker path never fans back out into per-market parquet files.
That is the whole point of the migration.

Mirror and processing work is interleaved, so the relay starts producing
queryable filtered hours during the initial backfill instead of waiting for the
entire raw mirror backlog to finish first.

## Fresh Box Expectations

On a fresh ClickHouse-backed VPS, the steady-state layout should look like this:

- `raw/` grows with mirrored PMXT hourly parquet files
- `state/relay.sqlite3` tracks archive, queue, and event metadata
- ClickHouse stores the filtered relay rows and serves `/v1/filtered/...`
- `processed/` does not exist
- `filtered/` does not exist

If you are migrating from the old filesystem backend, do not keep both systems
alive. Stop the old fanout path, remove any legacy `processed/` and `filtered/`
directories, recreate `tmp/` if needed, and remove any renamed legacy
`*.purge-*` directories before calling the box fully clean.

The design is restart-safe:

- raw downloads go through a temp file and atomic rename
- ClickHouse inserts are idempotent at the "hour already ingested" check
- relay state lives in `state/relay.sqlite3`
- the worker can resume after interruption without losing already mirrored or
  already processed hours

## Ubuntu 24 Deployment

On a fresh Ubuntu 24 VPS:

```bash
apt-get update
apt-get install -y git curl python3 python3-venv ufw fail2ban clickhouse-server clickhouse-client
curl -LsSf https://astral.sh/uv/install.sh | sh

git clone https://github.com/evan-kolberg/prediction-market-backtesting.git /opt/prediction-market-backtesting
cd /opt/prediction-market-backtesting

uv venv --python 3.12
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb

useradd --system --home /srv/pmxt-relay --shell /usr/sbin/nologin pmxtrelay || true
install -o pmxtrelay -g pmxtrelay -d /srv/pmxt-relay /srv/pmxt-relay/raw /srv/pmxt-relay/state /srv/pmxt-relay/tmp

cp pmxt_relay/systemd/pmxt-relay.env.example /etc/pmxt-relay.env
systemctl enable --now clickhouse-server
```

Then edit `/etc/pmxt-relay.env` for your actual public URL, data dir, port, and
trusted proxy IPs if you are fronting the relay with Caddy or another reverse
proxy. For the database-backed path, keep
`PMXT_RELAY_FILTERED_STORE_BACKEND=clickhouse` and point the ClickHouse vars at
your local HTTP endpoint.

If you front the relay with Caddy, nginx, or another reverse proxy, serve
`/v1/raw/*` straight from `/srv/pmxt-relay/raw` instead of proxying those large
parquet downloads back through the Python API. Keeping raw-hour file serving in
the web server avoids badge/API stalls when a backtest is pulling many missing
hours at once.

Install the systemd units:

```bash
cp pmxt_relay/systemd/pmxt-relay-api.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-relay-worker.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-disable-wbt.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now pmxt-disable-wbt.service
systemctl enable --now pmxt-relay-api.service
systemctl enable --now pmxt-relay-worker.service
```

Turn on the firewall with only SSH and the public relay port exposed:

```bash
ufw default deny incoming
ufw default allow outgoing
ufw limit 22/tcp
ufw allow 8080/tcp
ufw --force enable
```

Enable a basic SSH jail as well:

```bash
cat >/etc/fail2ban/jail.d/pmxt-relay-sshd.local <<'EOF'
[sshd]
enabled = true
bantime = 1h
findtime = 10m
maxretry = 5
EOF

systemctl enable --now fail2ban
fail2ban-client status sshd
```

After the initial deploy, verify the live box matches the intended ClickHouse
layout:

```bash
systemctl is-active pmxt-disable-wbt.service pmxt-relay-api.service pmxt-relay-worker.service clickhouse-server.service
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/v1/stats
find /srv/pmxt-relay/raw -type f | wc -l
test ! -e /srv/pmxt-relay/processed
test ! -e /srv/pmxt-relay/filtered
```

Expected shape in ClickHouse mode:

- `pmxt-disable-wbt`, `pmxt-relay-api`, `pmxt-relay-worker`, and
  `clickhouse-server` are `active`
- `raw/` contains mirrored parquet hours
- `processed/` and `filtered/` are absent

## API

- `GET /healthz`
- `GET /v1/stats`
- `GET /v1/queue`
- `GET /v1/events?limit=100`
- `GET /v1/inflight`
- `GET /v1/badge/{status,backfill,mirrored,processed,rate,latest,lag,file,rows}`
- `GET /v1/badge/{status,backfill,mirrored,processed,rate,latest,lag,file,rows}.svg`
- `GET /v1/badge/{cpu,load,mem,disk,iowait,api,worker,mirroring,processing,clickhouse}.svg`
- `GET /v1/markets/{condition_id}/tokens/{token_id}/hours?start=...&end=...`
- `GET /v1/filtered/{condition_id}/{token_id}/{filename}`

Progress and observability:

- `/v1/system` CPU is based on `1-minute loadavg / cpu_count`, capped at `100`.
  A `100%` badge can reflect ClickHouse merges or I/O wait, not just pure API
  or worker CPU burn.
- `/v1/system` also reports sampled `iowait_percent` plus per-service
  `active_state`, `sub_state`, `pid`, and sampled `cpu_percent` for the relay
  API, relay worker, and ClickHouse
- `/v1/badge/mirroring.svg` and `/v1/badge/processing.svg` show the live queue
  stages that the worker is actively driving in ClickHouse mode

- `/v1/stats` includes both the total completed-hour count and a rolling
  `processed_hours_per_hour_24h` rate so stalled relay throughput is visible
  even when `processed_hours` changes slowly
- `/v1/queue` shows current `pending/processing/error` counts for mirror and
  preprocess work plus the latest mirrored and processed hour
- `/v1/events` shows recent relay events such as discover, mirror start,
  mirror complete, process start, process complete, and errors
- `/v1/inflight` reports active `.filtered` temp trees with recursive file and
  directory counts while a very large hour is still being materialized
- relay events are persisted in SQLite so the status history survives process
  restarts

The PMXT loader in this repo now defaults to the public relay at:

```bash
https://209-209-10-83.sslip.io
```

You can override that with:

```bash
PMXT_RELAY_BASE_URL=https://your-relay-host
```

Or disable relay usage entirely with:

```bash
PMXT_RELAY_BASE_URL=0
```

The loader prefers relay-hosted filtered hours before falling back to any local
relay raw hour mirror, then any local raw PMXT mirror configured via
`PMXT_LOCAL_ARCHIVE_DIR`, then finally `r2.pmxt.dev`.

## Environment

Common env vars:

- `PMXT_RELAY_DATA_DIR=/srv/pmxt-relay`
- `PMXT_RELAY_BIND_HOST=0.0.0.0`
- `PMXT_RELAY_BIND_PORT=8080`
- `PMXT_RELAY_PUBLIC_BASE_URL=https://209-209-10-83.sslip.io`
- `PMXT_RELAY_POLL_INTERVAL_SECS=900`
- `PMXT_RELAY_DUCKDB_THREADS=2`
- `PMXT_RELAY_DUCKDB_MEMORY_LIMIT=1500MB`
- `PMXT_RELAY_FILTERED_WORKERS=1`
- `PMXT_RELAY_API_RATE_LIMIT_PER_MINUTE=2400`
- `PMXT_RELAY_API_LIST_MAX_HOURS=2000`
- `PMXT_RELAY_TRUSTED_PROXY_IPS=127.0.0.1,::1`
- `PMXT_RELAY_ARCHIVE_STALE_PAGES=9999` — how many consecutive already-known
  archive listing pages the worker will scan through before stopping. Default is
  3, which is fine when the relay is already caught up. Set high (e.g. 9999) to
  ensure the worker crawls the entire PMXT archive listing on every cycle, so no
  historical hours are missed.
- `PMXT_RELAY_ARCHIVE_MAX_PAGES=0` — hard cap on archive listing pages to scan
  per cycle. 0 (default) means unlimited.

## Systemd

Example unit files live in [`systemd/`](./systemd/). For the ClickHouse-backed
path, enable the worker, API, and `pmxt-disable-wbt.service`:

```bash
systemctl enable --now pmxt-disable-wbt.service
systemctl enable --now pmxt-relay-worker.service
systemctl enable --now pmxt-relay-api.service
```

The shipped units are hardened for public deployment:

- dedicated `pmxtrelay` service user
- no root privileges
- read-only repo and system paths
- write access limited to `/srv/pmxt-relay`
- private `/tmp`
- no device access or Linux capability set
- API and worker wait for ClickHouse `http://127.0.0.1:8123/ping` before
  starting, with `TimeoutStartSec=0` so slow cold boots do not trip systemd's
  startup timeout while ClickHouse is still coming up
- worker runs at `Nice=5` with `IOSchedulingClass=best-effort` so raw-hour
  ingest wins over best-effort cleanup work
- `pmxt-disable-wbt.service` forces `wbt_lat_usec=0` on `xvda`, `xvdb`, and
  `xvdc`, which avoids the raw-file read stalls that show up on some VPS disks

## Memory Tuning

On the ClickHouse-backed path, `clickhouse-server` is usually the hottest
process on the box. That is normal during large inserts, background merges, or
I/O wait, and it is why `/v1/system` can report `cpu_percent=100` even while
the API is healthy.

On a 6 GB VPS the conservative service limits are:

| Service    | `MemoryMax` | `MemorySwapMax` | Notes                      |
|------------|-------------|-----------------|----------------------------|
| Worker     | 2500M       | 512M            | Mirror + ClickHouse ingest |
| ClickHouse | distro/unit | distro/unit     | Main query + merge engine  |

Key env vars that still matter:

- `PMXT_RELAY_DUCKDB_MEMORY_LIMIT` - DuckDB query memory cap (set to ~25% of
  total RAM when the worker is parsing very large raw hours)
- `PMXT_RELAY_DUCKDB_THREADS` - DuckDB parallelism (lower = less peak memory)
- `PMXT_RELAY_CLICKHOUSE_INSERT_BATCH_ROWS` - max filtered rows per ClickHouse
  insert batch (lower it if large hours trigger ClickHouse OOMs while parsing
  Parquet inserts)

The current conservative default is `2048`.

The systemd units in `systemd/` include `MemoryMax` to prevent OOM kills from
crashing the whole machine. If a service hits its limit, systemd kills just
that service and `Restart=always` brings it back.

## Public Relay Hardening

The relay is designed to be public-facing while exposing both filtered and raw parquet hours:

- `/v1/filtered/...` validates market, token, and filename patterns before
  touching disk
- `/v1/raw/...` validates the requested hour path before touching disk
- API responses carry `nosniff`, `no-referrer`, and deny-frame headers
- immutable parquet hours are cacheable, while JSON status endpoints are `no-store`
- an in-memory per-IP request limiter protects the API from obvious hammering

Operationally, keep:

- `ufw` enabled
- SSH rate-limited with `ufw limit 22/tcp`
- `fail2ban` enabled for `sshd`
- the relay on a non-root service account
- request logs and `/v1/events` enabled so abuse patterns are visible
- `/v1/raw/*` offloaded to the reverse proxy's static file server so large raw
  parquet downloads do not monopolize the Python API workers

If you are using Caddy in front of the relay, a simple shape is:

```caddyfile
209-209-10-83.sslip.io {
    encode gzip

    handle_path /v1/raw/* {
        root * /srv/pmxt-relay/raw
        header Cache-Control "public, max-age=31536000, immutable"
        file_server
    }

    reverse_proxy 127.0.0.1:8080
}
```

Make sure the proxy user can actually read `/srv/pmxt-relay/raw`. For example,
on Ubuntu with the packaged `caddy` user, adding `caddy` to the `pmxtrelay`
group is usually enough when the raw tree is group-readable.
