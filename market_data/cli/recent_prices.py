#!/usr/bin/env python3
"""recent_prices.py SYMBOL — last N days of prices.

Single-symbol mode (positional SYMBOL): last N days, defaults to Close. Capped
at 365 days inline; pass ``--out-parquet PATH`` to dump full slice to disk.

Multi-symbol mode (``--symbols A,B,C``): aligned panel via
``MarketDataAccess.get_symbols_as_dataframe``. **Requires --out-parquet**
(payload bloat protection); JSON returns the parquet path, not the rows.

Examples:
    python market_data/cli/recent_prices.py SPY --days 30 --column Close
    python market_data/cli/recent_prices.py SPY --days 90 --json
    python market_data/cli/recent_prices.py SPY --days 1500 --out-parquet /tmp/spy.parquet
    python market_data/cli/recent_prices.py --symbols SPY,QQQ,TLT --days 60 \\
        --column Close --out-parquet /tmp/panel.parquet
"""

from __future__ import annotations

import argparse
from pathlib import Path


import pandas as pd

from market_data.cli._common import (  # noqa: E402
    add_common_args,
    emit_error,
    emit_json,
    open_mda,
    preflight_or_exit,
)

_DESCRIPTION = "Last N days of prices for one symbol; capped inline at 365 days."
_INLINE_DAYS_CAP = 365
_VALID_COLUMNS = ("Close", "Adj Close", "Open", "High", "Low", "Volume")


def _multi_symbol(args, multi_symbols: list[str]) -> None:
    """Multi-symbol mode: aligned panel, parquet-only output."""
    if not args.out_parquet:
        emit_error(
            "--symbols requires --out-parquet PATH (payload bloat protection)",
            as_json=args.json,
            exit_code=1,
        )
        return
    if args.all_columns:
        emit_error(
            "--all-columns is not supported with --symbols (single column only)",
            as_json=args.json,
            exit_code=1,
        )
        return

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        df = mda.get_symbols_as_dataframe(
            multi_symbols, column=args.column, period=f"{args.days}d", fill_method="ffill"
        )

    if df is None or df.empty:
        emit_error(
            f"no aligned data for symbols={multi_symbols} in last {args.days} days",
            as_json=args.json,
            exit_code=1,
        )
        return

    out_path = Path(args.out_parquet).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path)

    found = [s for s in multi_symbols if s in df.columns]
    missing = [s for s in multi_symbols if s not in df.columns]

    if args.json:
        emit_json(
            {
                "mode": "multi",
                "symbols_requested": multi_symbols,
                "symbols_found": found,
                "symbols_missing": missing,
                "days": args.days,
                "column": args.column,
                "rows": int(len(df)),
                "out_parquet": str(out_path),
            }
        )
    else:
        print(f"wrote {len(df)} rows x {len(found)} symbols -> {out_path}")
        if missing:
            print(f"missing: {missing}")


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument(
        "symbol",
        nargs="?",
        default=None,
        help="Single ticker (e.g. SPY). Mutually exclusive with --symbols.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Comma-separated tickers for multi-symbol panel; requires --out-parquet.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of trailing calendar days (default: 30)",
    )
    parser.add_argument(
        "--column",
        default="Close",
        choices=_VALID_COLUMNS,
        help="Column to return (default: Close). Ignored with --all-columns.",
    )
    parser.add_argument(
        "--all-columns",
        action="store_true",
        help="Return all OHLCV columns instead of one (single-symbol mode only)",
    )
    parser.add_argument(
        "--out-parquet",
        default=None,
        help=(
            "Write full slice to this parquet path and print the path. "
            "Required when --days > 365 or with --symbols."
        ),
    )
    add_common_args(parser)
    args = parser.parse_args()

    if args.days <= 0:
        emit_error("--days must be > 0", as_json=args.json, exit_code=1)

    # Mode selection
    if args.symbol and args.symbols:
        emit_error(
            "pass either positional SYMBOL or --symbols, not both",
            as_json=args.json,
            exit_code=1,
        )
        return
    if not args.symbol and not args.symbols:
        emit_error(
            "must pass either positional SYMBOL or --symbols A,B,C",
            as_json=args.json,
            exit_code=1,
        )
        return

    if args.symbols:
        multi = [s.strip() for s in args.symbols.split(",") if s.strip()]
        if len(multi) < 1:
            emit_error("--symbols list is empty", as_json=args.json, exit_code=1)
            return
        _multi_symbol(args, multi)
        return

    # Single-symbol mode
    if args.days > _INLINE_DAYS_CAP and not args.out_parquet:
        emit_error(
            f"--days={args.days} exceeds inline cap ({_INLINE_DAYS_CAP}); "
            f"pass --out-parquet PATH to write full slice to disk.",
            as_json=args.json,
            exit_code=1,
        )

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        prices = mda.get_price_history(args.symbol, period=f"{args.days}d")

    if prices is None or prices.empty:
        emit_error(
            f"no price history for '{args.symbol}' in last {args.days} days",
            as_json=args.json,
            exit_code=1,
        )
        return  # unreachable; emit_error calls sys.exit
    assert prices is not None  # narrow for type checker

    if args.all_columns:
        out_df = prices
    else:
        if args.column not in prices.columns:
            emit_error(
                f"column '{args.column}' not found; available: {list(prices.columns)}",
                as_json=args.json,
                exit_code=1,
            )
            return  # unreachable; emit_error calls sys.exit
        out_df = prices[[args.column]]

    if args.out_parquet:
        out_path = Path(args.out_parquet).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_df.to_parquet(out_path)
        if args.json:
            emit_json(
                {
                    "symbol": args.symbol,
                    "days": args.days,
                    "rows": int(len(out_df)),
                    "columns": [str(c) for c in out_df.columns],
                    "out_parquet": str(out_path),
                }
            )
        else:
            print(f"wrote {len(out_df)} rows -> {out_path}")
        return

    if args.json:
        records = [
            {"date": idx.strftime("%Y-%m-%d"), **{c: float(row[c]) for c in out_df.columns if pd.notna(row[c])}}
            for idx, row in out_df.iterrows()
        ]
        emit_json(
            {
                "symbol": args.symbol,
                "days": args.days,
                "column_filter": None if args.all_columns else args.column,
                "rows": len(records),
                "data": records,
            }
        )
    else:
        print(f"# {args.symbol}  last {args.days} days  ({len(out_df)} rows)")
        print(out_df.to_string())


if __name__ == "__main__":
    main()
