"""Use case: retrieve prices in several formats.

Assumes you've already built a database, e.g.:
    python -m examples.quickstart

Run (from the repo root):
    python -m examples.retrieve_prices
"""
from market_data import MarketDataAccess


def main():
    mda = MarketDataAccess()

    # 1) One symbol's full OHLCV history as a DataFrame.
    spy = mda.get_price_history("SPY", period="1y")
    print("1) SPY OHLCV (last 3 rows):")
    print(spy.tail(3), "\n")

    # 2) Several symbols' Close, aligned into one DataFrame (symbols as columns).
    closes = mda.get_symbols_as_dataframe(
        ["SPY", "QQQ", "TLT"], column="Close", period="1y"
    )
    print("2) Aligned Close panel (last 3 rows):")
    print(closes.tail(3), "\n")

    # 3) The latest row across symbols.
    print("3) Latest Close:")
    print(closes.tail(1).T, "\n")

    # 4) Export to Parquet.
    mda.export_to_parquet(["SPY", "QQQ"], "prices.parquet", period="1y")
    print("4) Wrote prices.parquet")

    mda.disconnect()


if __name__ == "__main__":
    main()
