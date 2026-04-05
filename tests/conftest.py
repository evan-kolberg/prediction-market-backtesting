from __future__ import annotations

from pathlib import Path

import nautilus_trader.adapters as nautilus_adapters


REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL_ADAPTERS = REPO_ROOT / "nautilus_pm" / "nautilus_trader" / "adapters"

if str(LOCAL_ADAPTERS) not in nautilus_adapters.__path__:
    nautilus_adapters.__path__.insert(0, str(LOCAL_ADAPTERS))
