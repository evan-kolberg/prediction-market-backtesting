# PMXT Relay

`pmxt_relay/` is now the active mirror-focused PMXT service layer for this repo.

Current direction:

- mirror raw PMXT archive hours onto disk
- optionally expose those mirrored raw files over `/v1/raw/*`
- process mirrored data locally with a separate script, not through the relay CLI
- keep server-side filtered processing retired from the active relay path

The old full-stack relay, including ClickHouse-backed processing and filtered
HTTP serving, has been archived under `archive/pmxt_relay_legacy/`.

## Active Commands

Mirror worker:

```bash
uv run python -m pmxt_relay worker
```

Mirror API:

```bash
uv run python -m pmxt_relay api
```

Local processing stays separate from the relay:

```bash
uv run python scripts/pmxt_process_local.py \
  --raw-root /data/pmxt/raw \
  --filtered-root ~/.cache/nautilus_trader/pmxt
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

## Fresh Box Setup

On a fresh Ubuntu 24 box:

```bash
apt-get update
apt-get install -y git curl python3 python3-venv ufw fail2ban
curl -LsSf https://astral.sh/uv/install.sh | sh

git clone https://github.com/evan-kolberg/prediction-market-backtesting.git /opt/prediction-market-backtesting
cd /opt/prediction-market-backtesting

uv venv --python 3.12
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb

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
not bake in archive or raw-origin URLs; set your mirror's upstream listing URL,
raw origin URL, and public base URL explicitly for the environment you run.

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
state. Filtered-hour and processing-oriented HTTP endpoints are intentionally
not part of the active relay path.

## Legacy Archive

Use `archive/pmxt_relay_legacy/` if you need to inspect or revive the older
server-side processing stack. That archive preserves the old relay-oriented
ClickHouse and filtered-serving code for teams that want to run that older
architecture on their own infrastructure.
