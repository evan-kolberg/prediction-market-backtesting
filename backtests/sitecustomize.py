from __future__ import annotations

import importlib
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

install_local_nautilus_overrides = importlib.import_module(
    "_nautilus_bootstrap",
).install_local_nautilus_overrides
install_local_nautilus_overrides()
