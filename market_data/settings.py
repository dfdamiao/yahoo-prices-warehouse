"""Runtime settings: where the ArcticDB prices database lives.

Read from ``config.json`` (or environment variables), with a local default.
No API key is needed — Yahoo Finance (yfinance) is keyless.

Resolution order for the config file:
    1. ``$MARKET_DATA_CONFIG_PATH``
    2. ``./config.json`` (current working directory)
    3. ``<repo root>/config.json``
Environment variables (``MARKET_DATA_DB_URI``, ``MARKET_DATA_DIR``,
``MARKET_DATA_LOGS_DIR``) override the file.
"""
from __future__ import annotations

import json
import os
from pathlib import Path


def _load_config() -> dict:
    env_path = os.environ.get("MARKET_DATA_CONFIG_PATH")
    candidates = [Path(env_path)] if env_path else []
    candidates += [
        Path.cwd() / "config.json",
        Path(__file__).resolve().parent.parent / "config.json",
    ]
    for p in candidates:
        if p.exists():
            return json.loads(p.read_text())
    return {}


_cfg = _load_config()

# ArcticDB connection URI. Local LMDB store by default; point it at any backend
# ArcticDB supports (lmdb://, s3://, ...). Put the database wherever you want.
DB_PATH = (
    os.environ.get("MARKET_DATA_DB_URI")
    or _cfg.get("database", {}).get("arcticdb_uri")
    or "lmdb://./market_data_store"
)

MARKET_DATA_DIR = Path(
    os.environ.get("MARKET_DATA_DIR")
    or _cfg.get("paths", {}).get("market_data_dir")
    or "./market_data_files"
)
LOGS_DIR = Path(
    os.environ.get("MARKET_DATA_LOGS_DIR")
    or _cfg.get("paths", {}).get("logs_dir")
    or "./logs"
)

for _d in (MARKET_DATA_DIR, LOGS_DIR):
    _d.mkdir(parents=True, exist_ok=True)
