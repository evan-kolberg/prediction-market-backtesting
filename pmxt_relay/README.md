# PMXT Relay

`pmxt_relay/` is a self-hosted PMXT acceleration layer for the backtests in this
repo.

What it does:

- mirrors every hourly raw PMXT Polymarket archive parquet file
- keeps polling the PMXT archive index for new hours
- precomputes one canonical processed parquet shard per hour with extracted
  `market_id` and `token_id` columns
- lazily materializes tiny filtered parquet slices keyed by
  `(condition_id, token_id, hour)` when clients actually request them
- serves those slices over HTTP so the backtest loader can skip the expensive
  remote market scan on first run

The relay keeps the exact filtered shape the loader already uses today:

- `update_type`
- `data`

That means the backtests do not need a new schema. They just need a faster
source for already-filtered hours.

## Directory Layout

By default the relay stores data under `/srv/pmxt-relay`:

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
- `processed/` stores one canonical prefiltered shard per hour with
  `market_id` and `token_id` extracted out of JSON

`filtered/` is then just a lazy cache of the exact tiny parquet files the
backtest loader wants.

So yes, the processed data is stored in addition to the raw mirror. The
filtered layer is still much smaller than the raw mirror because it keeps only
`update_type` and `data` for `book_snapshot` and `price_change` rows, split by
market/token/hour. Temporary `.tmp` files only exist during atomic writes.

## Processes

Run two long-lived services:

- API: `uv run python -m pmxt_relay api`
- Worker: `uv run python -m pmxt_relay worker`

The worker:

1. scrapes `https://archive.pmxt.dev/data/Polymarket?page=N`
2. discovers every hourly archive filename
3. downloads missing raw parquet files from `https://r2.pmxt.dev`
4. streams each raw hour through Arrow, extracts `token_id` from `data`, and
   writes one canonical processed parquet shard for that hour
5. records every discovered `(condition_id, token_id)` pair for the hour in SQLite
6. lets the API materialize the exact filtered `(condition_id, token_id, hour)`
   parquet lazily on first request
7. keeps polling for new hours

Mirror and preprocess work is interleaved, so the relay starts producing
queryable filtered hours during the initial backfill instead of waiting for the
entire raw mirror backlog to finish first.

The design is restart-safe:

- raw downloads go through a temp file and atomic rename
- processed hour shards go through a temp file and atomic rename
- lazily materialized filtered outputs go through a temp file and atomic rename
- relay state lives in `state/relay.sqlite3`
- the worker can resume after interruption without losing already mirrored or
  already processed hours

## Ubuntu 24 Deployment

On a fresh Ubuntu 24 VPS:

```bash
apt-get update
apt-get install -y git curl python3 python3-venv ufw fail2ban
curl -LsSf https://astral.sh/uv/install.sh | sh

git clone https://github.com/evan-kolberg/prediction-market-backtesting.git /opt/prediction-market-backtesting
cd /opt/prediction-market-backtesting

uv venv --python 3.12
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb

useradd --system --home /srv/pmxt-relay --shell /usr/sbin/nologin pmxtrelay || true
install -o pmxtrelay -g pmxtrelay -d /srv/pmxt-relay /srv/pmxt-relay/raw /srv/pmxt-relay/processed /srv/pmxt-relay/filtered /srv/pmxt-relay/state /srv/pmxt-relay/tmp

cp pmxt_relay/systemd/pmxt-relay.env.example /etc/pmxt-relay.env
```

Then edit `/etc/pmxt-relay.env` for your actual IP, data dir, and port.

Install the systemd units:

```bash
cp pmxt_relay/systemd/pmxt-relay-api.service /etc/systemd/system/
cp pmxt_relay/systemd/pmxt-relay-worker.service /etc/systemd/system/
systemctl daemon-reload
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

## API

- `GET /healthz`
- `GET /v1/stats`
- `GET /v1/queue`
- `GET /v1/events?limit=100`
- `GET /v1/inflight`
- `GET /v1/markets/{condition_id}/tokens/{token_id}/hours?start=...&end=...`
- `GET /v1/filtered/{condition_id}/{token_id}/{filename}`

Progress and observability:

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
http://209.209.10.83:8080
```

You can override that with:

```bash
PMXT_RELAY_BASE_URL=http://your-relay-host:8080
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
- `PMXT_RELAY_PUBLIC_BASE_URL=http://209.209.10.83:8080`
- `PMXT_RELAY_POLL_INTERVAL_SECS=900`
- `PMXT_RELAY_EXPOSE_RAW=0`
- `PMXT_RELAY_API_RATE_LIMIT_PER_MINUTE=2400`
- `PMXT_RELAY_API_LIST_MAX_HOURS=2000`

## Systemd

Example unit files live in [`systemd/`](./systemd/). Enable both:

```bash
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
