from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
LOCAL_OVERRIDE_ROOT = REPO_ROOT / "_nautilus_overrides" / "nautilus_trader"
LOCAL_ADAPTERS = LOCAL_OVERRIDE_ROOT / "adapters"
LOCAL_ANALYSIS = LOCAL_OVERRIDE_ROOT / "analysis"


def _prepend_package_path(package: object, path: Path) -> None:
    package_path = getattr(package, "__path__", None)
    if package_path is None:
        raise TypeError(f"{package!r} does not expose __path__")

    path_str = str(path)
    if path_str in package_path:
        package_path.remove(path_str)
    package_path.insert(0, path_str)


def install_local_nautilus_overrides() -> None:
    import nautilus_trader.adapters as nautilus_adapters
    import nautilus_trader.analysis as nautilus_analysis

    _prepend_package_path(nautilus_adapters, LOCAL_ADAPTERS)

    _prepend_package_path(nautilus_analysis, LOCAL_ANALYSIS)


__all__ = ["install_local_nautilus_overrides"]
