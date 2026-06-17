#!/usr/bin/env python3
"""search.py PATTERN — case-insensitive search across symbols + longName/shortName.

Examples:
    python market_data/cli/search.py gold
    python market_data/cli/search.py "S&P 500" --quote-type ETF
    python market_data/cli/search.py XL --limit 5 --json
"""

from __future__ import annotations

import argparse


import pandas as pd

from market_data.cli._common import (  # noqa: E402
    add_common_args,
    emit_error,
    emit_json,
    open_mda,
    preflight_or_exit,
)

_DESCRIPTION = "Search symbols + longName/shortName for a pattern (case-insensitive)."
_DEFAULT_LIMIT = 25


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument("pattern", help="Pattern to match (substring, case-insensitive)")
    parser.add_argument(
        "--quote-type",
        default=None,
        help="Optional quoteType filter (e.g. ETF, EQUITY, INDEX)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=_DEFAULT_LIMIT,
        help=f"Max rows returned (default: {_DEFAULT_LIMIT})",
    )
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        df = mda.search_symbols(args.pattern, quote_type=args.quote_type)

    if df is None or df.empty:
        emit_error(
            f"no symbols match pattern '{args.pattern}'"
            + (f" with quoteType={args.quote_type}" if args.quote_type else ""),
            as_json=args.json,
            exit_code=1,
        )
        return

    total = int(len(df))
    if args.limit and total > args.limit:
        df = df.head(args.limit)

    cols = [c for c in ("longName", "shortName", "quoteType", "exchange") if c in df.columns]

    if args.json:
        records = []
        for symbol, row in df.iterrows():
            entry: dict = {"symbol": str(symbol)}
            for c in cols:
                v = row.get(c)
                entry[c] = None if (v is None or (isinstance(v, float) and pd.isna(v))) else v
            records.append(entry)
        emit_json(
            {
                "pattern": args.pattern,
                "quote_type": args.quote_type,
                "total_matches": total,
                "returned": len(records),
                "results": records,
            }
        )
    else:
        print(f"# pattern={args.pattern!r}  quote_type={args.quote_type or '-'}  "
              f"matches={total}  showing={len(df)}")
        header = f"{'symbol':<10} " + "  ".join(f"{c:<24.24}" for c in cols)
        print(header)
        print("-" * len(header))
        for symbol, row in df.iterrows():
            cells = [f"{str(symbol):<10}"]
            for c in cols:
                v = row.get(c)
                v = "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
                cells.append(f"{v:<24.24}")
            print("  ".join(cells))


if __name__ == "__main__":
    main()
