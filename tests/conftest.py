from __future__ import annotations

from pathlib import Path

import nautilus_trader.analysis as nautilus_analysis
import nautilus_trader.adapters as nautilus_adapters


REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL_NAUTILUS_ROOT = REPO_ROOT / "nautilus_pm" / "nautilus_trader"
LOCAL_ADAPTERS = LOCAL_NAUTILUS_ROOT / "adapters"
LOCAL_ANALYSIS = LOCAL_NAUTILUS_ROOT / "analysis"

if str(LOCAL_ADAPTERS) not in nautilus_adapters.__path__:
    nautilus_adapters.__path__.insert(0, str(LOCAL_ADAPTERS))
if str(LOCAL_ANALYSIS) not in nautilus_analysis.__path__:
    nautilus_analysis.__path__.insert(0, str(LOCAL_ANALYSIS))
