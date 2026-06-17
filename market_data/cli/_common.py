"""Shared helpers for read-only CLI tools under market_data/cli/.

Each CLI script imports from here for:
- argparse setup (--db-path, --json)
- PID-file pre-flight (refuse if updater is active)
- JSON output helpers (with stable _schema_version)
- Connection lifecycle (context-managed MarketDataAccess)
"""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import psutil

from market_data.settings import DB_PATH, MARKET_DATA_DIR
from market_data.access import MarketDataAccess

SCHEMA_VERSION = 1


def add_common_args(parser: argparse.ArgumentParser) -> None:
    """Attach --db-path and --json to a script's argparse parser."""
    parser.add_argument(
        "--db-path",
        default=DB_PATH,
        help=f"ArcticDB URI (default: {DB_PATH}). Pass test fixture URI here.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of human-readable output.",
    )


def updater_is_active(market_data_dir: str | Path | None = None) -> bool:
    """Return True if the daily updater is running.

    Checks for ``<MARKET_DATA_DIR>/.updater.pid``. If present, verifies the PID
    is still alive via psutil; treats stale PID files as inactive.
    """
    market_data_dir = Path(market_data_dir or MARKET_DATA_DIR)
    pid_file = market_data_dir / ".updater.pid"
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text().strip())
    except (OSError, ValueError):
        return False
    return psutil.pid_exists(pid)


def preflight_or_exit(market_data_dir: str | Path | None = None) -> None:
    """Refuse to run if updater is active. Prints stderr message and exits 2."""
    if updater_is_active(market_data_dir):
        print(
            "ERROR: market data updater is running (.updater.pid present). "
            "CLI helpers refuse to run during updates to avoid LMDB lock "
            "contention. Wait for the updater to finish, or remove the stale "
            "PID file if you know it crashed.",
            file=sys.stderr,
        )
        sys.exit(2)


@contextmanager
def open_mda(db_path: str):
    """Open MarketDataAccess as a context manager and always disconnect."""
    mda = MarketDataAccess(db_path=db_path)
    try:
        yield mda
    finally:
        mda.disconnect()


def emit_json(payload: dict[str, Any]) -> None:
    """Print payload as JSON with a stable schema-version key."""
    payload_with_meta = {"_schema_version": SCHEMA_VERSION, **payload}
    print(json.dumps(payload_with_meta, default=str, indent=2))


def emit_error(message: str, *, as_json: bool, exit_code: int = 1) -> None:
    """Emit an error in either JSON or plain-text form, then exit."""
    if as_json:
        emit_json({"error": message})
    else:
        print(f"ERROR: {message}", file=sys.stderr)
    sys.exit(exit_code)
