# PMXT Relay

`pmxt_relay/` is a self-hosted PMXT acceleration layer for the backtests in this
repo.

> UNDER CONSTRUCTION: the old filesystem prebuild model is being phased out.
> The relay HTTP surface is staying stable, but the backend is being refactored
> toward a ClickHouse-friendly path instead of millions of tiny parquet files.

The relay API now has an explicit filtered-hour store seam in code, so the HTTP
surface can stay stable while the backend moves away from tiny filesystem
shards and toward a database-backed query path.

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
| Query-ready | `prebuild_status=ready` | Available through the API, via files or ClickHouse |

**"Processed" in the badges/stats means query-ready output**, not just "we
downloaded the raw parquet." The public badges and `/v1/stats` use this
definition:

- `mirrored` badge: `mirror_status=ready` / total discovered
- `processed` badge: `prebuild_status=ready` / `mirror_status=ready`
- `rate` badge: rolling 24-hour completed-hour rate based on `prebuilt_at`
- `prebuild-file` badge: current parquet filename from the latest
  `process_progress` or `filtered_prebuild_progress` event while work is active
- `prebuild-progress` badge: full `processed_rows / total_rows` fraction from
  the latest `process_progress` or `filtered_prebuild_progress` event

The worker also adopts any raw parquet files that already exist under `raw/`
into the current relay state. That keeps `mirrored_hours` aligned with the
actual local raw inventory instead of only counting hours mirrored by the
current worker process.

The inflight reset on startup is split by stage so the worker and prebuild
service don't clobber each other's state:

- Worker resets: mirror + process inflight (not prebuild)
- Prebuild service resets: prebuild inflight (not mirror/process)

## Directory Layout

By default the relay stores relay-owned state under `/srv/pmxt-relay`:

```text
/srv/pmxt-relay/
  raw/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
  processed/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
  filtered/<condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
  state/relay.sqlite3
  tmp/
```

This means the relay keeps two persistent layers:

- `raw/` stores the mirrored PMXT archive hours as-is
- `state/` stores the relay SQLite metadata/index

When `PMXT_RELAY_FILTERED_STORE_BACKEND=clickhouse`, the parsed filtered rows
live in ClickHouse instead of `processed/` and `filtered/`. Those directories
can stay empty, and the old tiny-file fanout is no longer required.

The legacy filesystem backend still uses:

- `processed/` for one canonical parquet shard per hour
- `filtered/` for one parquet per `(condition_id, token_id, hour)`

Temporary `.tmp` files only exist during atomic writes.

## Processes

For the new ClickHouse-backed path, run two long-lived services:

- API: `uv run python -m pmxt_relay api`
- Worker: `uv run python -m pmxt_relay worker`

The worker mirrors raw hours and inserts filtered rows directly into ClickHouse.
There is no separate filtered-prebuild phase in that mode, so
`pmxt-relay-prebuild.service` should stay disabled.

The legacy filesystem backend still uses a third service:

- Prebuild: `uv run python -m pmxt_relay prebuild-filtered`

The worker handles discovery, mirroring, and ClickHouse ingestion:

1. scrapes `https://archive.pmxt.dev/data/Polymarket?page=N`
2. discovers every hourly archive filename
3. downloads missing raw parquet files from `https://r2.pmxt.dev`
4. streams each raw hour through Arrow, extracts `token_id` from `data`, and
   inserts the filtered relay rows into ClickHouse
5. keeps polling for new hours

If you stay on the legacy filesystem backend, the prebuild service is the only
process that writes final filtered output:

1. walks sharded hours whose `prebuild_status` is `pending`
2. materializes their final `(condition_id, token_id, hour)` parquet files
3. keeps running in the background so shards get converted to backtest-ready
   files without waiting for an API request

The ClickHouse worker path never fans back out into per-market parquet files.
That is the whole point of the migration.

Mirror and preprocess work is interleaved, so the relay starts producing
queryable filtered hours during the initial backfill instead of waiting for the
entire raw mirror backlog to finish first.

## Fresh Box Expectations

On a fresh ClickHouse-backed VPS, the steady-state layout should look like this:

- `raw/` grows with mirrored PMXT hourly parquet files
- `state/relay.sqlite3` tracks archive, queue, and event metadata
- ClickHouse stores the filtered relay rows and serves `/v1/filtered/...`
- `processed/` stays empty
- `filtered/` stays empty
- `pmxt-relay-prebuild.service` stays disabled

If you are migrating from the old filesystem backend, do not keep both systems
alive. Stop the old fanout path, recreate clean empty `processed/`, `filtered/`,
and `tmp/` directories for the new services, and remove any renamed legacy
`*.purge-*` directories before calling the box fully clean.

The design is restart-safe:

- raw downloads go through a temp file and atomic rename
- ClickHouse inserts are idempotent at the "hour already ingested" check
- legacy processed hour shards go through a temp file and atomic rename
- legacy eagerly prebuilt filtered outputs go through a temp file and atomic rename
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
install -o pmxtrelay -g pmxtrelay -d /srv/pmxt-relay /srv/pmxt-relay/raw /srv/pmxt-relay/processed /srv/pmxt-relay/filtered /srv/pmxt-relay/state /srv/pmxt-relay/tmp

cp pmxt_relay/systemd/pmxt-relay.env.example /etc/pmxt-relay.env
systemctl enable --now clickhouse-server
```

Then edit `/etc/pmxt-relay.env` for your actual public URL, data dir, port, and
trusted proxy IPs if you are fronting the relay with Caddy or another reverse
proxy. For the database-backed path, keep
`PMXT_RELAY_FILTERED_STORE_BACKEND=clickhouse` and point the ClickHouse vars at
your local HTTP endpoint.

Install the systemd units:

```bash
cp pmxt_relay/systemd/pmxt-relay-api.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-relay-worker.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-relay-prebuild.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-disable-wbt.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now pmxt-disable-wbt.service
systemctl enable --now pmxt-relay-api.service
systemctl enable --now pmxt-relay-worker.service
systemctl disable --now pmxt-relay-prebuild.service
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
systemctl is-active pmxt-disable-wbt.service pmxt-relay-api.service pmxt-relay-worker.service pmxt-relay-prebuild.service clickhouse-server.service
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:8080/v1/stats
find /srv/pmxt-relay/raw -type f | wc -l
find /srv/pmxt-relay/processed -type f | wc -l
find /srv/pmxt-relay/filtered -type f | wc -l
```

Expected shape in ClickHouse mode:

- `pmxt-disable-wbt`, `pmxt-relay-api`, `pmxt-relay-worker`, and
  `clickhouse-server` are `active`
- `pmxt-relay-prebuild.service` is `inactive`
- `raw/` contains mirrored parquet hours
- `processed/` and `filtered/` both report `0`

## API

- `GET /healthz`
- `GET /v1/stats`
- `GET /v1/queue`
- `GET /v1/events?limit=100`
- `GET /v1/inflight`
- `GET /v1/badge/{status,backfill,mirrored,processed,rate,latest,lag,prebuild-file,prebuild-progress}`
- `GET /v1/badge/{status,backfill,mirrored,processed,rate,latest,lag,prebuild-file,prebuild-progress}.svg`
- `GET /v1/badge/{cpu,mem,disk}.svg`
- `GET /v1/markets/{condition_id}/tokens/{token_id}/hours?start=...&end=...`
- `GET /v1/filtered/{condition_id}/{token_id}/{filename}`

Progress and observability:

- `/v1/system` CPU is based on `1-minute loadavg / cpu_count`, capped at `100`.
  A `100%` badge can reflect ClickHouse merges or I/O wait, not just pure API
  or worker CPU burn.

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

The loader prefers relay-hosted filtered hours before falling back to
`r2.pmxt.dev`.

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
- `PMXT_RELAY_EXPOSE_RAW=0`
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
path, enable the worker, API, and `pmxt-disable-wbt.service`, and keep the old
prebuild service disabled:

```bash
systemctl enable --now pmxt-disable-wbt.service
systemctl enable --now pmxt-relay-worker.service
systemctl enable --now pmxt-relay-api.service
systemctl disable --now pmxt-relay-prebuild.service
```

The shipped units are hardened for public deployment:

- dedicated `pmxtrelay` service user
- no root privileges
- read-only repo and system paths
- write access limited to `/srv/pmxt-relay`
- private `/tmp`
- no device access or Linux capability set
- worker runs at `Nice=5` with `IOSchedulingClass=best-effort` so raw-hour
  ingest wins over best-effort cleanup work
- `pmxt-disable-wbt.service` forces `wbt_lat_usec=0` on `xvda`, `xvdb`, and
  `xvdc`, which avoids the raw-file read stalls that show up on some VPS disks

## Memory Tuning

On the ClickHouse-backed path, `clickhouse-server` is usually the hottest
process on the box. That is normal during large inserts, background merges, or
I/O wait, and it is why `/v1/system` can report `cpu_percent=100` even while
the API is healthy.

The legacy prebuild path is still the biggest one-shot RAM spike. Each hourly
parquet file contains ~30M rows spread across thousands of
`(condition_id, token_id)` partitions, and materializing one hour can use 3-4
GB of RAM.

On a 6 GB VPS the conservative service limits are:

| Service  | `MemoryMax` | `MemorySwapMax` | Notes                            |
|----------|-------------|-----------------|----------------------------------|
| Worker   | 2500M       | 512M            | Mirror + ClickHouse ingest       |
| Prebuild | 4500M       | 486M            | Legacy one-hour fanout           |

Key env vars that still matter:

- `PMXT_RELAY_DUCKDB_MEMORY_LIMIT` - DuckDB query memory cap (set to ~25% of
  total RAM when two services run concurrently)
- `PMXT_RELAY_DUCKDB_THREADS` - DuckDB parallelism (lower = less peak memory)
- `PMXT_RELAY_FILTERED_WORKERS` - concurrent partition materializers in the
  legacy prebuild step (keep at 1 on low-RAM machines)

The systemd units in `systemd/` include `MemoryMax` to prevent OOM kills from
crashing the whole machine. If a service hits its limit, systemd kills just
that service and `Restart=always` brings it back.

## Public Relay Hardening

The relay is designed to be public-facing without exposing the raw mirror:

- raw hour passthrough stays off by default with `PMXT_RELAY_EXPOSE_RAW=0`
- `/v1/filtered/...` validates market, token, and filename patterns before
  touching disk
- API responses carry `nosniff`, `no-referrer`, and deny-frame headers
- immutable parquet hours are cacheable, while JSON status endpoints are `no-store`
- an in-memory per-IP request limiter protects the API from obvious hammering

Operationally, keep:

- `ufw` enabled
- SSH rate-limited with `ufw limit 22/tcp`
- `fail2ban` enabled for `sshd`
- the relay on a non-root service account
- request logs and `/v1/events` enabled so abuse patterns are visible
