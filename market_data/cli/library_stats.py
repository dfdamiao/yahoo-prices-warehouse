#!/usr/bin/env python3
"""library_stats.py — row counts per ArcticDB library + on-disk size.

Examples:
    python market_data/cli/library_stats.py
    python market_data/cli/library_stats.py --json
"""

from __future__ import annotations

import argparse
from pathlib import Path


from arcticdb import Arctic  # noqa: E402

from market_data.cli._common import (  # noqa: E402
    add_common_args,
    emit_json,
    preflight_or_exit,
)

_DESCRIPTION = "Row counts per ArcticDB library + on-disk size."


def _disk_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return total


def _human_bytes(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    arctic = Arctic(args.db_path)
    try:
        libraries = sorted(arctic.list_libraries())
        rows: list[dict] = []
        total_symbols = 0
        for name in libraries:
            try:
                symbols = arctic.get_library(name).list_symbols()
                count = len(symbols)
            except Exception as exc:
                rows.append({"library": name, "symbols": None, "error": str(exc)})
                continue
            total_symbols += count
            rows.append({"library": name, "symbols": count})
    finally:
        del arctic

    db_dir_str = args.db_path.replace("lmdb:///", "", 1)
    db_dir = Path(db_dir_str)
    disk = _disk_bytes(db_dir)

    if args.json:
        emit_json(
            {
                "db_path": args.db_path,
                "disk_bytes": disk,
                "disk_human": _human_bytes(disk),
                "total_symbols": total_symbols,
                "libraries": rows,
            }
        )
    else:
        print(f"DB:    {args.db_path}")
        print(f"Disk:  {_human_bytes(disk)}")
        print(f"Total: {total_symbols} symbols across {len(rows)} libraries")
        print()
        print(f"{'library':<28} {'symbols':>10}")
        print("-" * 40)
        for r in rows:
            sym = r.get("symbols")
            print(f"{r['library']:<28} {(str(sym) if sym is not None else 'ERR'):>10}")


if __name__ == "__main__":
    main()
