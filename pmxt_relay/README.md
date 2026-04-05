# PMXT Relay

`pmxt_relay/` is the VPS deployment subtree for the active PMXT mirror service.
Keep this folder limited to server infrastructure, service code, and deploy
artifacts. PC-side download helpers and other local workflows should live
outside `pmxt_relay/`.

Current direction:

- mirror raw PMXT archive hours onto disk
- optionally expose those mirrored raw files over `/v1/raw/*`
- keep the active relay scoped to raw mirroring and raw file serving

Older relay code has been archived under `archive/pmxt_relay_legacy/`.

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

Repeated upstream 404s are no longer retried every poll forever. The active
relay now backs off failed mirrors and temporarily quarantines repeated 404s on
a slower retry cadence so one stale archive reference does not dominate every
worker cycle while the mirror still heals automatically when upstream recovers.

## Fresh Box Setup

On a fresh Ubuntu 24 box:

```bash
apt-get update
apt-get install -y git curl python3 python3-venv ufw fail2ban
curl -LsSf https://astral.sh/uv/install.sh | sh

git clone https://github.com/evan-kolberg/prediction-market-backtesting.git /opt/prediction-market-backtesting
cd /opt/prediction-market-backtesting

uv venv --python 3.12
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb textual

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

Edit `/etc/pmxt-relay.env` before starting the services. The active relay does
not bake in archive or raw-origin URLs; set your mirror's upstream listing URL
and raw origin URL explicitly for the environment you run.

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

The public badges separate relay health from `r2.pmxt.dev` availability:

- `/v1/badge/status(.svg)` reports whether the relay itself is up, recent, and
  has active API/worker services.
- `/v1/badge/upstream(.svg)` reports whether `r2.pmxt.dev` is healthy,
  lagging, or erroring.

## Legacy Archive

Use `archive/pmxt_relay_legacy/` only if you need historical context for the
older relay implementation.
