#!/usr/bin/env python3
"""metadata_get.py SYMBOL — fetch one row from symbol_metadata or ticker_classification.

Examples:
    python market_data/cli/metadata_get.py SPY
    python market_data/cli/metadata_get.py SPY --classification
    python market_data/cli/metadata_get.py SPY --classification --json
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

_DESCRIPTION = "Fetch one row from symbol_metadata or ticker_classification."


def _series_to_dict(s: pd.Series | None) -> dict | None:
    if s is None:
        return None
    out: dict = {}
    for k, v in s.to_dict().items():
        if hasattr(v, "strftime"):
            out[str(k)] = v.strftime("%Y-%m-%d")
        elif isinstance(v, float) and pd.isna(v):
            out[str(k)] = None
        else:
            out[str(k)] = v
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument("symbol", help="Ticker, e.g. SPY")
    parser.add_argument(
        "--classification",
        action="store_true",
        help="Read ticker_classification (default: symbols_metadata)",
    )
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        if args.classification:
            row = mda.get_ticker_classification_for(args.symbol)
            source = "ticker_classification"
        else:
            row = mda.get_symbol_metadata(args.symbol)
            source = "symbols_metadata"

    if row is None:
        emit_error(
            f"symbol '{args.symbol}' not found in {source}",
            as_json=args.json,
            exit_code=1,
        )
        return  # unreachable; emit_error calls sys.exit

    payload = {"symbol": args.symbol, "source": source, "fields": _series_to_dict(row)}

    if args.json:
        emit_json(payload)
    else:
        print(f"# {args.symbol}  source={source}")
        for k, v in payload["fields"].items():
            print(f"  {k:<28} {v}")


if __name__ == "__main__":
    main()
