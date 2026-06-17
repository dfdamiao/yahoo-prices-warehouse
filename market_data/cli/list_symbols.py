#!/usr/bin/env python3
"""list_symbols.py — list symbols from a library, with optional filters.

Examples:
    python market_data/cli/list_symbols.py
    python market_data/cli/list_symbols.py --prefix XL --limit 20
    python market_data/cli/list_symbols.py --library symbol_metadata --json
    python market_data/cli/list_symbols.py --status active
"""

from __future__ import annotations

import argparse


from market_data.cli._common import (  # noqa: E402
    add_common_args,
    emit_error,
    emit_json,
    open_mda,
    preflight_or_exit,
)

_DESCRIPTION = "List symbols from a library, with optional filters."
_LIB_CHOICES = ("price_history.daily", "price_history.metadata", "symbol_metadata")


def _resolve_lib(mda, name: str):
    return {
        "price_history.daily": mda.price_history_lib,
        "price_history.metadata": mda.price_metadata_lib,
        "symbol_metadata": mda.symbol_metadata_lib,
    }[name]


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument(
        "--library",
        default="price_history.daily",
        choices=_LIB_CHOICES,
        help="Which ArcticDB library to list (default: price_history.daily)",
    )
    parser.add_argument("--prefix", default=None, help="Case-sensitive prefix filter")
    parser.add_argument(
        "--status",
        default=None,
        help=(
            "Optional status filter (e.g. 'active'). Applies only when "
            "--library is price_history.daily; uses symbols_metadata.status."
        ),
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit number of returned symbols"
    )
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        lib = _resolve_lib(mda, args.library)
        symbols = sorted(lib.list_symbols())

        if args.status:
            if args.library != "price_history.daily":
                emit_error(
                    "--status is only supported when --library is price_history.daily",
                    as_json=args.json,
                    exit_code=1,
                )
            meta = mda.get_symbols_metadata()
            if meta.empty or "status" not in meta.columns:
                emit_error(
                    "symbols_metadata has no 'status' column; cannot filter",
                    as_json=args.json,
                    exit_code=1,
                )
            allowed = set(meta.index[meta["status"] == args.status])
            symbols = [s for s in symbols if s in allowed]

        if args.prefix:
            symbols = [s for s in symbols if s.startswith(args.prefix)]

        total_after_filters = len(symbols)
        if args.limit is not None:
            symbols = symbols[: args.limit]

    if args.json:
        emit_json(
            {
                "library": args.library,
                "prefix": args.prefix,
                "status": args.status,
                "limit": args.limit,
                "returned": len(symbols),
                "total_after_filters": total_after_filters,
                "symbols": symbols,
            }
        )
    else:
        print(
            f"library={args.library}  prefix={args.prefix or '-'}  "
            f"status={args.status or '-'}  returned={len(symbols)}/"
            f"{total_after_filters}"
        )
        for s in symbols:
            print(s)


if __name__ == "__main__":
    main()
