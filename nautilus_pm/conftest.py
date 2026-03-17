# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

# Root-level conftest.py
#
# NautilusTrader requires compiled Cython extensions that live in the installed
# wheel (.venv), not in the local source tree.  By default pytest adds the repo
# root to sys.path first, so the local `nautilus_trader/` source shadows the
# wheel and `nautilus_trader.core.nautilus_pyo3` cannot be found.
#
# This file is loaded by pytest before any other conftest or test module, so
# we can fix sys.path here once and be done.
#
# Strategy (mirrors the example scripts):
#   1. Put the wheel's site-packages first so compiled extensions are found.
#   2. Remove the repo root so the local source does NOT shadow the wheel.
#   3. Extend nautilus_trader.adapters.__path__ with the local adapters
#      directory (prepended) so that:
#        - Local-only adapters (kalshi) are importable.
#        - Locally modified adapters (polymarket fee changes) take priority
#          over the wheel version during tests.

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
_VENV_SITE = _REPO_ROOT / ".venv" / "lib" / "python3.13" / "site-packages"

# Step 1: wheel site-packages first
if str(_VENV_SITE) in sys.path:
    sys.path.remove(str(_VENV_SITE))
sys.path.insert(0, str(_VENV_SITE))

# Step 2: remove repo root so local source doesn't shadow the wheel
if str(_REPO_ROOT) in sys.path:
    sys.path.remove(str(_REPO_ROOT))

# Step 3: extend the adapters namespace with our local adapters directory
import nautilus_trader.adapters as _nt_adapters  # noqa: E402

_LOCAL_ADAPTERS = _REPO_ROOT / "nautilus_trader" / "adapters"
if str(_LOCAL_ADAPTERS) not in _nt_adapters.__path__:
    _nt_adapters.__path__.insert(0, str(_LOCAL_ADAPTERS))
