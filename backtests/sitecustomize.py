from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

install_commission_patch = importlib.import_module(
    "prediction_market_extensions"
).install_commission_patch
install_commission_patch()
