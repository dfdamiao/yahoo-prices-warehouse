#!/usr/bin/env python3
"""filter_tickers.py — multi-axis filter on the ticker_classification table.

Examples:
    python market_data/cli/filter_tickers.py --asset-class equity --sector Technology
    python market_data/cli/filter_tickers.py --invested-region EM --domicile IE
    python market_data/cli/filter_tickers.py --quote-type ETF --include-discontinued --json
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

_DESCRIPTION = "Filter ticker_classification by asset class / sector / region / etc."
_DEFAULT_LIMIT = 50


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument("--asset-class", default=None, help="e.g. equity, bond, commodity")
    parser.add_argument("--sector", default=None, help="e.g. Technology, Energy")
    parser.add_argument("--invested-region", default=None, help="e.g. US, EM, Europe")
    parser.add_argument("--style", default=None, help="e.g. Large_Blend, Growth, Value")
    parser.add_argument("--currency", default=None, help="e.g. USD, EUR")
    parser.add_argument("--domicile", default=None, help="e.g. US, IE (UCITS-only)")
    parser.add_argument("--quote-type", default=None, help="e.g. ETF, EQUITY")
    parser.add_argument(
        "--include-discontinued",
        action="store_true",
        help="Include suspected_discontinued rows (default: exclude)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=_DEFAULT_LIMIT,
        help=f"Max rows returned (default: {_DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--symbols-only",
        action="store_true",
        help="Print only the symbol list (one per line / array in JSON)",
    )
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        df = mda.filter_tickers(
            asset_class=args.asset_class,
            sector=args.sector,
            invested_region=args.invested_region,
            style=args.style,
            currency=args.currency,
            domicile=args.domicile,
            quote_type=args.quote_type,
            exclude_discontinued=not args.include_discontinued,
        )

    if df is None or df.empty:
        emit_error("no tickers match the supplied filters", as_json=args.json, exit_code=1)
        return

    total = int(len(df))
    if args.limit and total > args.limit:
        df = df.head(args.limit)

    filters = {
        "asset_class": args.asset_class,
        "sector": args.sector,
        "invested_region": args.invested_region,
        "style": args.style,
        "currency": args.currency,
        "domicile": args.domicile,
        "quote_type": args.quote_type,
        "include_discontinued": args.include_discontinued,
    }

    if args.symbols_only:
        symbols = [str(s) for s in df.index]
        if args.json:
            emit_json(
                {
                    "filters": filters,
                    "total_matches": total,
                    "returned": len(symbols),
                    "symbols": symbols,
                }
            )
        else:
            print(f"# matches={total}  showing={len(symbols)}")
            for s in symbols:
                print(s)
        return

    cols = [
        c for c in (
            "asset_class", "sector", "invested_region", "style",
            "currency", "domicile", "quoteType", "longName",
        ) if c in df.columns
    ]

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
                "filters": filters,
                "total_matches": total,
                "returned": len(records),
                "results": records,
            }
        )
    else:
        active_filters = ", ".join(f"{k}={v}" for k, v in filters.items() if v not in (None, False))
        print(f"# {active_filters or '(no filters)'}  matches={total}  showing={len(df)}")
        header = f"{'symbol':<10} " + "  ".join(f"{c:<14.14}" for c in cols)
        print(header)
        print("-" * len(header))
        for symbol, row in df.iterrows():
            cells = [f"{str(symbol):<10}"]
            for c in cols:
                v = row.get(c)
                v = "" if (v is None or (isinstance(v, float) and pd.isna(v))) else str(v)
                cells.append(f"{v:<14.14}")
            print("  ".join(cells))


if __name__ == "__main__":
    main()
