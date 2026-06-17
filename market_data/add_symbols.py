#!/usr/bin/env python3
"""Add one or more tickers to your prices database by ID.

Prices + metadata are fetched from Yahoo Finance. The simplest way to grow your
database: give it ticker symbols.

Usage:
    python -m market_data.add_symbols AAPL MSFT SPY
    python -m market_data.add_symbols TLT --force        # re-fetch even if present
"""
import argparse

from market_data.settings import DB_PATH, MARKET_DATA_DIR
from market_data.symbol_adder import SymbolAdder


def main():
    parser = argparse.ArgumentParser(
        description="Add one or more tickers to your prices database by ID."
    )
    parser.add_argument("symbols", nargs="+", help="Ticker symbols, e.g. AAPL MSFT SPY")
    parser.add_argument(
        "--force", action="store_true", help="Re-fetch even if the symbol is already present"
    )
    args = parser.parse_args()

    adder = SymbolAdder(market_data_dir=str(MARKET_DATA_DIR), db_path=DB_PATH)
    print(f"Adding {len(args.symbols)} symbol(s) into {DB_PATH}\n")

    added = 0
    for sym in args.symbols:
        result = adder.add_symbol(sym, force_update=args.force)
        if result.get("success"):
            added += 1
            print(f"  OK   {sym}")
        else:
            errors = "; ".join(result.get("errors", [])) or "see logs"
            print(f"  FAIL {sym}  ({errors})")

    print(f"\nDone: {added}/{len(args.symbols)} symbols added.")


if __name__ == "__main__":
    main()
