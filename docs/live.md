# Sandbox And Live Runners

## Current Scope

`make sandbox` is the operator menu for Nautilus sandbox runners. It is for
testing a strategy against live data feeds with sandbox execution, not for
sending real Polymarket orders.

Real Polymarket live trading support is planned later. Keep that path separate
from sandbox until account credentials, order permissions, kill switches,
position limits, and operational checks are explicit.

## Directory Contract

- `strategies/` contains reusable strategy implementations.
- `strategies/private/` contains git-ignored local strategy modules.
- `prediction_market_extensions/live/` contains shared live/sandbox helper
  code that is safe to ship with the framework.
- `live/` contains local sandbox and future live runner entrypoints.

Live runner and scaffold files can be tracked when they are safe to publish.
Private model artifacts, diagnostics, logs, `.env` files, and machine-specific
runtime files remain ignored by `live/.gitignore`.

Do not put private model weights, private research notes, or deployment secrets
in `prediction_market_extensions/live/`. That package is shared helper code
only. If a public runner needs a private model, reference the ignored model path
and keep the model file under `live/models/`.

## Sandbox Runner Contract

Sandbox runners are flat files under `live/`:

```text
live/my_strategy_sandbox.py
```

Each runner should expose:

```python
def run() -> None:
    ...
```

The runner is responsible for:

- importing the strategy implementation from `strategies/` or
  `strategies/private/`
- building the Nautilus `ImportableStrategyConfig`
- injecting local parameters and model artifact paths
- selecting the live data feeds and market universe
- calling shared helpers from `prediction_market_extensions/live/`

This mirrors the backtest runner model: runner files are the explicit operating
surface, while shared mechanics stay in extension modules.

## Shared Live Helpers

Reusable helper code belongs in `prediction_market_extensions/live/`.

Current helper responsibilities include:

- Polymarket BTC 5m slug and instrument loading
- Binance BTC trade instrument defaults
- Nautilus sandbox node config construction
- sandbox execution client factory registration
- live BTC trade feature buffering for strategies that need recent BTC
  momentum, volume, or volatility features
- public Polymarket CLOB settlement polling for sandbox portfolio accounting
- rolling BTC 5m market discovery and pruning
- BTC trade-feed freshness checks so live strategies can fail closed when the
  external reference-price feed is stale

These helpers must remain parameter-free. They can accept strategy configs and
runtime options from a local runner, but they should not embed private
thresholds, coefficients, model paths, or sizing rules.

## BTC 5m Sandbox Plumbing

The BTC 5m sandbox flow is split between public-safe plumbing and private
strategy assets.

The public-safe path is:

1. `make sandbox` starts `main.py --mode sandbox`.
2. The menu discovers flat runner files under `live/`.
3. A runner such as `live/btc_snapshot_model_sandbox.py` builds a Nautilus
   `ImportableStrategyConfig` and passes it into the shared sandbox helpers.
4. `prediction_market_extensions/live/btc_5m.py` builds the rolling BTC 5m
   Polymarket event-slug horizon, loads the initial UP/DOWN instrument IDs, and
   exposes an importable slug builder for provider refreshes.
5. `prediction_market_extensions/live/sandbox.py` builds the Nautilus sandbox
   node with public Polymarket market data, Binance US BTC trades, and Nautilus
   sandbox execution.
6. Strategy code receives live order-book deltas and Binance BTC trade ticks
   through Nautilus, then emits sandbox orders through Nautilus risk/execution.

The BTC 5m hooks are rolling, not fixed. On each Polymarket instrument refresh,
the slug builder returns the current upcoming market window. The public
instrument provider loads those markets, keeps only the current slug set, and
prunes stale instruments outside that set. The strategy then scans Nautilus'
cache for newly loaded BTC 5m instruments, subscribes the new UP/DOWN order
books, and can unsubscribe/prune expired markets after its configured retention
window.

This bounds the framework-level provider list, strategy subscriptions, and local
book maps. Nautilus may still retain some cache or sandbox-exchange metadata for
instruments seen during the process lifetime, so very long unattended sandbox
runs should watch memory, restart on a planned cadence if needed, and confirm
shutdown logs show expired books being cleaned up.

Settlement is separate from Nautilus' simulated exchange. The strategy can poll
public Polymarket CLOB market state after market expiry, record whether the
held token won, and write a local sandbox settlement ledger. That ledger is a
local runtime artifact and is ignored by `live/.gitignore`.

Long-running sandbox strategies should emit operational proof-of-life logs even
when they do not trade. The BTC snapshot sandbox emits `SANDBOX_MODEL_HEARTBEAT`
on a configurable interval and `SANDBOX_EVAL_SKIP` or `SANDBOX_EVAL_SIGNAL` at
model evaluation points. This makes it clear whether the node is merely
connected, actively receiving BTC ticks, and actually evaluating the current 5m
market.

Strategies should also guard against stale reference data. A connected Binance
websocket is not enough proof that BTC features are fresh; the runner can pass a
maximum BTC feature age so private strategy code skips entries when recent BTC
trade ticks are missing.

## Example BTC Snapshot Runner

`live/btc_snapshot_model_sandbox.py` is an example of how a local live runner
uses the public-safe plumbing. It is intentionally a wiring example, not a
complete public trading package:

- it selects the BTC 5m market horizon;
- it sets environment values used by the importable BTC 5m slug builder;
- it resolves the private model path from `LIVE_BTC_SNAPSHOT_MODEL_PATH` or a
  default under `live/models/`;
- it builds a strategy config that injects instrument IDs, the Binance BTC
  trade instrument, local runtime options, and the private model path;
- it calls `build_polymarket_binance_sandbox_config()` and
  `build_polymarket_binance_sandbox_node()`;
- it supports dry-run/build-only validation and a `--run` path for starting the
  Nautilus node.

By default, the example runner sets `heartbeat_log_seconds` from
`LIVE_BTC_HEARTBEAT_LOG_SECONDS`, defaulting to five minutes. Set that
environment variable lower while debugging startup, or higher if a production
sandbox log is too chatty.

The example runner also exposes operational switches for the live data path:

- `LIVE_BTC_MAX_FEATURE_AGE_SECONDS` controls how stale BTC trade-derived
  features may be before the private strategy should skip an entry.
- `LIVE_BTC_DAILY_STOP_LOSS` passes a sandbox daily loss limit into strategies
  that support one.
- `LIVE_BTC_BINANCE_GLOBAL=1` or `--binance-global` routes BTC trades through
  Binance global instead of Binance US, subject to network availability.

That runner is useful as a public example of the sandbox wiring. It references
private strategy and model paths to show where local artifacts plug in, but
those artifacts are not part of the public framework for obvious reasons. The
model artifact, private strategy implementation, private research runner,
optimizer history, and training data are not a public reproducibility package.

## Private Strategy Boundary

Do not open-source the private strategy or model artifacts.

The BTC snapshot model depends on private research code and private Telonex
backtesting data. The public repository can show how Nautilus sandbox plumbing,
Polymarket BTC 5m market discovery, Binance BTC trade features, heartbeat and
evaluation logging, settlement polling, and local runner injection fit together.
It should not include:

- model weights or serialized model profiles;
- private feature-generation or training code;
- private optimizer sweeps, validation reports, or iteration history;
- tuned strategy thresholds that are not intended as public examples;
- private Telonex-derived datasets or derived research artifacts.

Keep those under ignored paths such as `live/models/`, `strategies/private/`,
`backtests/private/`, or another local private storage path. Public helpers
should describe interfaces and operational mechanics, not the edge itself.

## Model And Parameter Placement

Open-source-safe runner parameters can live in a tracked file under `live/`.
Private model weights and private research artifacts should stay under ignored
paths such as:

```text
live/models/
strategies/private/
backtests/private/
```

A local runner can inject tracked operating parameters and ignored model paths
into the strategy config:

```python
STRATEGY_PARAMETERS = {
    "parameter_name": ...,
    "model_path": "live/models/my_private_model.json",
}
```

That local runner can then inject those values into the strategy config:

```python
ImportableStrategyConfig(
    strategy_path="strategies.private.my_strategy:MyStrategy",
    config_path="strategies.private.my_strategy:MyStrategyConfig",
    config={
        **STRATEGY_PARAMETERS,
        "instrument_ids": [...],
    },
)
```

Use `.env` for secrets and machine-specific operational switches. Do not rely
on `.env` as the only source of structured strategy parameters unless that is
intentional for the deployment.

Large diagnostics should be opt-in. For example, the BTC snapshot sandbox runner
only writes its evaluation/order/fill diagnostics when
`LIVE_BTC_SNAPSHOT_DIAGNOSTICS_PATH` is set.

## Running Sandbox

Open the sandbox menu:

```bash
make sandbox
```

Equivalent direct command:

```bash
uv run python main.py --mode sandbox
```

The menu discovers flat Python and notebook files under `live/`. Keep model
files in `live/models/`; they are ignored and are not included in the public
framework.

Direct runner execution is still useful while developing a sandbox runner:

```bash
uv run python live/my_strategy_sandbox.py
```

Local runners may choose to make direct execution a dry-run and reserve actual
node startup for `make sandbox` or a `--run` flag.

Sandbox runners that subscribe to a fixed set of upcoming markets should load a
large enough horizon for the planned session. For BTC 5m runners,
`LIVE_BTC_5M_MARKET_COUNT=36` covers about three hours of forward market
discovery while rolling refresh keeps advancing the window. Increase or reduce
that value based on the forward horizon a strategy needs to see at startup.

## Public Polymarket Data

The sandbox helper uses public Polymarket CLOB market-data access. Nautilus
1.226's upstream Polymarket data factory constructs an authenticated CLOB
client, but the framework sandbox helper registers a public-data factory so
`make sandbox` does not require Polymarket API credentials for market-data-only
paper trading.

The sandbox runner still uses Nautilus sandbox execution, so selected
strategies paper trade unless you intentionally add a separate real
live-trading command later.

## Path To Live Polymarket Trading

Future real live trading should reuse the same structure:

- `make sandbox` remains sandbox-only.
- A separate command should be added for real live trading when it is ready.
- Public-safe live runner scaffolds can live under `live/`.
- Shared Polymarket adapter helpers should remain under
  `prediction_market_extensions/live/`.
- Private keys, account identifiers, model parameters, and risk limits should
  stay out of tracked helper code. Model files should stay under ignored
  `live/models/`.
- Real Polymarket order routing will require authenticated Polymarket
  credentials and should not reuse the unauthenticated sandbox data factory for
  execution.

Before enabling real order routing, require explicit checks for account
balances, market permissions, max order size, max daily loss, stale data,
position limits, and emergency shutdown behavior.
