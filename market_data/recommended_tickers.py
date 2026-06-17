"""A curated starter universe of liquid, well-known US tickers + major ETFs.

Public knowledge, grouped by category. You can download *any* Yahoo Finance
ticker by ID — these lists just give you a sensible default to get started.
Company names, sectors, and other metadata are fetched live from Yahoo Finance
when you add a symbol.
"""
from __future__ import annotations

# --- US large-cap equities -------------------------------------------------
US_LARGE_CAP: list[str] = [
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "JPM", "V", "MA", "UNH", "HD", "PG", "JNJ", "XOM", "CVX", "KO", "PEP",
    "COST", "WMT", "BAC", "ABBV", "MRK", "LLY", "AVGO", "ORCL", "CRM", "ADBE",
    "NFLX", "AMD", "INTC", "CSCO", "QCOM", "TXN", "DIS", "NKE", "MCD", "BA",
    "CAT", "GE", "HON", "UPS", "GS", "MS", "WFC", "T", "VZ", "CMCSA",
]

# --- Broad-market / index ETFs ---------------------------------------------
INDEX_ETFS: list[str] = [
    "SPY", "VOO", "IVV", "QQQ", "DIA", "IWM", "VTI", "RSP", "MDY", "SCHB",
]

# --- Sector ETFs (SPDR Select Sector) --------------------------------------
SECTOR_ETFS: list[str] = [
    "XLK", "XLF", "XLE", "XLV", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE",
    "XLC",
]

# --- Bond / fixed-income ETFs ----------------------------------------------
BOND_ETFS: list[str] = [
    "TLT", "IEF", "SHY", "AGG", "BND", "LQD", "HYG", "TIP", "MUB", "BIL",
]

# --- International equity ETFs ----------------------------------------------
INTERNATIONAL_ETFS: list[str] = [
    "EFA", "EEM", "VEA", "VWO", "IEFA", "IEMG", "VXUS", "ACWI", "FXI", "EWJ",
]

# --- Commodity / real-asset ETFs -------------------------------------------
COMMODITY_ETFS: list[str] = [
    "GLD", "IAU", "SLV", "USO", "DBC", "GDX", "GDXJ", "PDBC",
]

# --- Style / factor ETFs ----------------------------------------------------
FACTOR_ETFS: list[str] = [
    "VUG", "VTV", "MTUM", "QUAL", "USMV", "VIG", "SPHD", "SPLV", "VYM", "SCHD",
]

# --- Volatility ------------------------------------------------------------
VOLATILITY: list[str] = ["VXX", "UVXY", "SVXY"]

# --- The 10 essentials: a fast, diversified starter set --------------------
ESSENTIAL_TICKERS: list[str] = [
    "SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TLT", "GLD", "EEM", "XLE",
]

# Map of category label -> list, used to build the catalog and pick subsets.
CATEGORIES: dict[str, list[str]] = {
    "US Large Cap": US_LARGE_CAP,
    "Index ETF": INDEX_ETFS,
    "Sector ETF": SECTOR_ETFS,
    "Bond ETF": BOND_ETFS,
    "International ETF": INTERNATIONAL_ETFS,
    "Commodity ETF": COMMODITY_ETFS,
    "Factor ETF": FACTOR_ETFS,
    "Volatility": VOLATILITY,
}

# Union of everything (de-duplicated, order-stable).
ALL_RECOMMENDED: list[str] = list(
    dict.fromkeys(t for group in CATEGORIES.values() for t in group)
)
