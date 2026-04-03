# PMXT Relay

The canonical PMXT relay deploy and operations guide lives at:

- [pmxt_relay/README.md on GitHub](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)

Use that page for:

- raw mirror mode and local-first workflow
- local processing examples that stay outside the relay CLI
- fresh-box deployment steps for raw mirroring
- systemd units
- firewall and fail2ban setup
- environment variables
- raw mirror endpoints and operational notes

Preferred direction:

- store raw PMXT hours locally
- process them locally with `uv run python scripts/pmxt_process_local.py`
- keep shared servers in mirror-only mode when you need a public raw mirror
- treat the old server-side processing relay as archived under `archive/pmxt_relay_legacy/`
