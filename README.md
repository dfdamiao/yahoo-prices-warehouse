# yahoo-prices-warehouse

Build your own local **stock & ETF price database** from Yahoo Finance. Choose
where the database lives, and the tool downloads OHLCV history into a fast
[ArcticDB](https://arcticdb.io) store you can query with pandas or export to
Parquet/CSV — with resumable bulk updates and a curated starter universe.

**No API key needed** — Yahoo Finance is free and keyless. You only configure
*where* the database goes.

*****************************************************************************

## What it does

- Downloads daily OHLCV for any Yahoo Finance ticker into a local ArcticDB.
- Ships a curated **112-ticker starter universe** (US large-caps + major ETFs) —
  see [`ticker_catalog.csv`](ticker_catalog.csv) / [TICKER_CATALOG.md](TICKER_CATALOG.md).
- **Add any ticker by ID** — `python -m market_data.add_symbols AAPL MSFT`.
- Resumable bulk updates with progress tracking + failed-symbol retry.
- A rich read API: price history, aligned multi-symbol panels, fuzzy search,
  metadata, export to Parquet/CSV/Excel.

## Why ArcticDB?

[ArcticDB](https://arcticdb.io) is a fast, columnar time-series store (built for
exactly this — large panels of financial data). The default backend is a local
[LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database) file, so
there's no server to run; you can also point it at S3.

*****************************************************************************

## Install

```bash
git clone <this-repo> yahoo-prices-warehouse
cd yahoo-prices-warehouse
pip install -r requirements.txt
```

## Configure (where the database goes)

```bash
cp config.example.json config.json
```

Edit `config.json` and set `arcticdb_uri` to **wherever you want** the database:

```json
{
  "database": { "arcticdb_uri": "lmdb:///absolute/path/to/market_data_store" }
}
```

Defaults to `lmdb://./market_data_store` if you skip this. You can also override
with the `MARKET_DATA_DB_URI` environment variable. `config.json` is git-ignored.

*****************************************************************************

## Quick start

```bash
python -m examples.quickstart           # build a DB with 10 essential tickers
```

Then grow it:

```bash
# add tickers by ID
python -m market_data.add_symbols AAPL MSFT SPY TLT GLD

# add an entire category from the catalog
python -c "from market_data.recommended_tickers import SECTOR_ETFS; print(*SECTOR_ETFS)" \
  | xargs python -m market_data.add_symbols
```

## The ticker catalog

Browse [`ticker_catalog.csv`](ticker_catalog.csv) (`ticker, category`) or import
the lists:

```python
from market_data.recommended_tickers import ESSENTIAL_TICKERS, SECTOR_ETFS, BOND_ETFS
```

See [TICKER_CATALOG.md](TICKER_CATALOG.md) for the categories and the 10
essentials. Any valid Yahoo Finance ticker works — the catalog is just a
starting point.

*****************************************************************************

## Retrieving prices (several formats)

```python
from market_data import MarketDataAccess

mda = MarketDataAccess()

# 1) one symbol's full OHLCV history
spy = mda.get_price_history("SPY", period="1y")

# 2) several symbols' Close, aligned into one DataFrame (symbols as columns)
closes = mda.get_symbols_as_dataframe(["SPY", "QQQ", "TLT"], column="Close", period="1y")

# 3) export to Parquet
mda.export_to_parquet(["SPY", "QQQ"], "prices.parquet", period="1y")

mda.disconnect()   # always disconnect to release the LMDB lock
```

`period` accepts `1d | 1mo | 3mo | 6mo | 1y | 2y | 5y | ytd | max` (or pass
explicit `start_date` / `end_date`). A runnable version is in
[`examples/retrieve_prices.py`](examples/retrieve_prices.py):

```bash
python -m examples.retrieve_prices
```

*****************************************************************************

## Database layout

A single ArcticDB store with three libraries:

- **`price_history.daily`** — daily OHLCV per symbol.
- **`symbol_metadata`** — per-symbol info from Yahoo Finance (name, sector, …).
- **`price_history.metadata`** — per-symbol data ranges / update bookkeeping.

## Commands

Run as modules from the repo root:

| Command | What it does |
|---|---|
| `python -m examples.quickstart` | Build a starter DB with the 10 essentials |
| `python -m market_data.add_symbols ID [ID ...] [--force]` | Add tickers by ID |
| `python -m market_data.run_market_data_updater --new --period 1mo` | Download/update the stored universe |
| `python -m market_data.cli.list_symbols` | List symbols in the database |
| `python -m market_data.cli.recent_prices SPY` | Show recent prices for a symbol |
| `python -m market_data.cli.search APPLE` | Fuzzy-search symbols/metadata |
| `python -m market_data.cli.library_stats` | Database statistics |

The read-only `cli/` tools (`info`, `last_update`, `metadata_get`,
`filter_tickers`, `failed_symbols`, …) each accept `--help`.

## Database maintenance

Once you have a store, `market_data.toolkit` keeps it healthy: inspect size,
diagnose symbol status, check/repair LMDB locks, and compact to reclaim disk. It
runs against the same ArcticDB libraries the updater writes.

| Command | What it does |
|---|---|
| `python -m market_data.toolkit analyze --mode storage` | Per-library size + storage breakdown |
| `python -m market_data.toolkit analyze --mode symbols` | Symbol counts across libraries |
| `python -m market_data.toolkit diagnose --mode status` | Per-symbol data-status report (complete/stale/failed) |
| `python -m market_data.toolkit test --mode connection` | Verify the store opens and reads |
| `python -m market_data.toolkit check --mode lmdb` | LMDB health check |
| `python -m market_data.toolkit repair --mode locks --fix` | Clear stale LMDB locks |
| `python -m market_data.toolkit optimize --mode prune --dry-run` | Preview prunable data |
| `python -m market_data.toolkit optimize --mode compact` | Compact the store to reclaim disk |

Every command takes an optional `--db-path <uri>`; without it, the path from your
`config.json` is used. Run any subcommand with `--help` for its full flags.

> **System dependency:** `repair --mode lmdb` and `optimize --mode compact` shell
> out to the LMDB command-line tools `mdb_copy` / `mdb_stat`. Install them via
> `lmdb-utils` (Debian/Ubuntu: `apt install lmdb-utils`) or `lmdb` (macOS:
> `brew install lmdb`). Every other command is pure Python + ArcticDB.

## Troubleshooting

- **Where's my database?** — at `database.arcticdb_uri` from `config.json`
  (default `./market_data_store`).
- **LMDB lock errors** — only one process can write at a time; always call
  `mda.disconnect()` when done, and don't run two updaters at once.
- **A ticker won't download** — Yahoo may not have it, or it's delisted; check
  `python -m market_data.cli.failed_symbols`.

## License

MIT — see [LICENSE](LICENSE).
