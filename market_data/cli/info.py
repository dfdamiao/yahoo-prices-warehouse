#!/usr/bin/env python3
"""info.py SYMBOL — show last_update, row_count, date_range, columns, status.

Examples:
    python market_data/cli/info.py SPY
    python market_data/cli/info.py SPY --json
    python market_data/cli/info.py SPY --db-path lmdb:///path/to/test_db
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

_DESCRIPTION = "Show last_update, row_count, date_range, columns, status for a symbol."


def _build_payload(mda, symbol: str) -> dict | None:
    if symbol not in mda.price_history_lib.list_symbols():
        return None

    prices = mda.price_history_lib.read(symbol).data
    metadata = mda.get_symbol_metadata(symbol)

    payload: dict = {"symbol": symbol}

    if prices is not None and not prices.empty:
        first = prices.index.min()
        last = prices.index.max()
        payload["row_count"] = int(len(prices))
        payload["first_date"] = first.strftime("%Y-%m-%d") if pd.notna(first) else None
        payload["last_date"] = last.strftime("%Y-%m-%d") if pd.notna(last) else None
        payload["columns"] = [str(c) for c in prices.columns]
    else:
        payload["row_count"] = 0
        payload["first_date"] = None
        payload["last_date"] = None
        payload["columns"] = []

    try:
        price_meta = mda.price_metadata_lib.read(symbol).data
        if isinstance(price_meta, pd.DataFrame) and not price_meta.empty:
            row = price_meta.iloc[-1].to_dict()
            payload["data_status"] = row.get("data_status")
            last_date = row.get("last_date")
            payload["meta_last_date"] = (
                last_date.strftime("%Y-%m-%d")
                if hasattr(last_date, "strftime")
                else (str(last_date) if last_date is not None else None)
            )
    except Exception:
        payload["data_status"] = None
        payload["meta_last_date"] = None

    if metadata is not None:
        payload["status"] = metadata.get("status") if hasattr(metadata, "get") else None
        long_name = (
            metadata.get("longName") if hasattr(metadata, "get") else None
        ) or (metadata.get("shortName") if hasattr(metadata, "get") else None)
        payload["name"] = long_name

    return payload


def _print_human(payload: dict) -> None:
    print(f"Symbol: {payload['symbol']}")
    if payload.get("name"):
        print(f"Name:   {payload['name']}")
    print(f"Rows:   {payload['row_count']}")
    print(f"Range:  {payload['first_date']} -> {payload['last_date']}")
    print(f"Cols:   {', '.join(payload['columns'])}")
    if payload.get("data_status") is not None:
        print(f"Status: {payload['data_status']}")
    if payload.get("meta_last_date") is not None:
        print(f"Meta last_date: {payload['meta_last_date']}")


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument("symbol", help="Ticker, e.g. SPY")
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        payload = _build_payload(mda, args.symbol)

    if payload is None:
        emit_error(
            f"symbol '{args.symbol}' not found in price_history.daily",
            as_json=args.json,
            exit_code=1,
        )
        return  # unreachable; emit_error calls sys.exit

    if args.json:
        emit_json(payload)
    else:
        _print_human(payload)


if __name__ == "__main__":
    main()
