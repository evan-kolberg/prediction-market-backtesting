from __future__ import annotations

import argparse
import sys
from pathlib import Path


def _resolved(path: str) -> Path | None:
    cleaned = path.strip()
    if not cleaned:
        return None
    return Path(cleaned).expanduser().resolve(strict=False)


def _is_same_or_nested(a: Path, b: Path) -> bool:
    return a == b or a in b.parents or b in a.parents


def main() -> int:
    parser = argparse.ArgumentParser(description="Refuse unsafe cache clear roots.")
    parser.add_argument("--name", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--unsafe", action="append", default=[])
    args = parser.parse_args()

    target = _resolved(args.target)
    home = Path.home().resolve(strict=False)
    if target is None or target == Path(target.anchor) or target == home:
        print(f"Refusing to clear unsafe {args.name}: {args.target}", file=sys.stderr)
        return 2

    for unsafe_raw in args.unsafe:
        unsafe = _resolved(unsafe_raw)
        if unsafe is None:
            continue
        if _is_same_or_nested(target, unsafe):
            print(
                f"Refusing to clear unsafe {args.name}: {args.target} "
                f"(conflicts with local data store {unsafe_raw})",
                file=sys.stderr,
            )
            return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
