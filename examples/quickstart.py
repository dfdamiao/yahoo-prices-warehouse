"""Quick start: build a price database with the 10 essential tickers.

Prerequisites:
    1. pip install -r requirements.txt
    2. (optional) cp config.example.json config.json   and set the database path.
       Without it, the database defaults to ./market_data_store.

Run (from the repo root):
    python -m examples.quickstart
"""
from market_data import MarketDataAccess
from market_data.recommended_tickers import ESSENTIAL_TICKERS
from market_data.settings import DB_PATH, MARKET_DATA_DIR
from market_data.symbol_adder import SymbolAdder


def main():
    print(f"Building a prices database at {DB_PATH}")
    print(f"Downloading {len(ESSENTIAL_TICKERS)} essential tickers from Yahoo Finance...\n")

    adder = SymbolAdder(market_data_dir=str(MARKET_DATA_DIR), db_path=DB_PATH)
    for sym in ESSENTIAL_TICKERS:
        result = adder.add_symbol(sym)
        print(f"  {'OK  ' if result.get('success') else 'FAIL'} {sym}")

    mda = MarketDataAccess()
    print("\nSample — SPY Close (last 5 rows):")
    print(mda.get_symbols_as_dataframe(["SPY"], column="Close", period="1mo").tail())
    mda.disconnect()


if __name__ == "__main__":
    main()
