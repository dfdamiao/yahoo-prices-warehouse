# Ticker Catalog

This tool ships with a curated starter universe of **112 liquid US tickers + major
ETFs**, grouped by category. You can download *any* Yahoo Finance ticker by ID —
this list is just a sensible default to get going.

Two files:

- **[`ticker_catalog.csv`](ticker_catalog.csv)** — every ticker with its
  `category`. Sort/grep it to pick what you want.
- **`market_data/recommended_tickers.py`** — the same lists, importable
  (`from market_data.recommended_tickers import ESSENTIAL_TICKERS, SECTOR_ETFS, ...`).

Company names, sectors, market cap, and other metadata are fetched live from
Yahoo Finance when you add a symbol — so they're always current, not a stale
snapshot.

*****************************************************************************

## Categories

| Category | Count | Examples |
|---|---:|---|
| US Large Cap | 50 | AAPL, MSFT, NVDA, JPM, XOM |
| Index ETF | 10 | SPY, VOO, QQQ, IWM, VTI |
| Sector ETF | 11 | XLK, XLF, XLE, XLV, XLU |
| Bond ETF | 10 | TLT, IEF, AGG, LQD, HYG |
| International ETF | 10 | EFA, EEM, VEA, VWO, EWJ |
| Commodity ETF | 8 | GLD, SLV, USO, DBC, GDX |
| Factor ETF | 10 | MTUM, QUAL, USMV, VYM, SCHD |
| Volatility | 3 | VXX, UVXY, SVXY |

**112 total** (de-duplicated in `ALL_RECOMMENDED`).

*****************************************************************************

## The 10 essentials

`python -m examples.quickstart` builds a database with exactly these — a fast,
diversified cross-section:

`SPY` · `QQQ` · `IWM` · `AAPL` · `MSFT` · `NVDA` · `TLT` · `GLD` · `EEM` · `XLE`

*****************************************************************************

## Picking tickers

- Browse `ticker_catalog.csv`, copy the tickers you want.
- Add them: `python -m market_data.add_symbols AAPL MSFT SPY`.
- Or import a category in code:
  `from market_data.recommended_tickers import SECTOR_ETFS`.
- Not in the catalog? No problem — any valid Yahoo Finance ticker works:
  `python -m market_data.add_symbols BRK-B ASML TSM`.
