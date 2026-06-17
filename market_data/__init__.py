"""yahoo-prices-warehouse — build your own local Yahoo Finance price database.

    from market_data import MarketDataAccess

    mda = MarketDataAccess()
    df = mda.get_symbols_as_dataframe(["SPY", "QQQ"], column="Close", period="1y")
    mda.disconnect()
"""
from market_data.access import MarketDataAccess
from market_data.manager import MarketDataManager

__all__ = ["MarketDataAccess", "MarketDataManager"]
