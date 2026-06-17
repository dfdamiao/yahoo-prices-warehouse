#!/usr/bin/env python3
"""
Show failed symbols from price_update_progress.json and write a summary JSON.
Reads price_update_progress.json (never modified).
Writes failed_symbols.json in the same directory as this script.
"""

import json
import sys
from datetime import datetime
from pathlib import Path


from market_data.settings import MARKET_DATA_DIR

PROGRESS_FILE = Path(MARKET_DATA_DIR) / "price_update_progress.json"
OUTPUT_FILE = Path(__file__).resolve().parent / "failed_symbols.json"


def main():
    if not PROGRESS_FILE.exists():
        print(f"File not found: {PROGRESS_FILE}")
        sys.exit(1)

    with open(PROGRESS_FILE) as f:
        data = json.load(f)

    symbol_status = data.get("symbol_status", {})
    failed_entries = symbol_status.get("failed", [])
    if not isinstance(failed_entries, list):
        print("Unexpected format for 'failed' entries.")
        sys.exit(1)

    if not failed_entries:
        print("No failed symbols found.")
        output = {"generated_at": datetime.now().isoformat(), "total": 0, "by_category": {}, "symbols": []}
        OUTPUT_FILE.write_text(json.dumps(output, indent=2))
        print(f"Written: {OUTPUT_FILE}")
        return

    # Group by error_category
    by_category: dict[str, list] = {}
    for entry in failed_entries:
        cat = entry.get("error_category", "unknown")
        by_category.setdefault(cat, []).append({
            "symbol": entry.get("symbol"),
            "attempts": entry.get("attempts"),
            "last_attempt_time": entry.get("last_attempt_time"),
            "error": entry.get("error"),
        })

    session = data.get("session_info", {})
    print(f"Session:        {session.get('session_id', 'N/A')}")
    print(f"Progress file:  {PROGRESS_FILE}")
    print(f"Total failed:   {len(failed_entries)}")
    print()

    for category, entries in sorted(by_category.items()):
        print(f"── {category.upper()} ({len(entries)}) ──────────────────────────")
        for e in entries:
            ts    = (e["last_attempt_time"] or "")[:19]
            error = e["error"] or ""
            if len(error) > 80:
                error = error[:77] + "..."
            print(f"  {e['symbol']:<20}  attempts={e['attempts']}  {ts}  {error}")
        print()

    # Write output JSON
    output = {
        "generated_at": datetime.now().isoformat(),
        "source": str(PROGRESS_FILE),
        "session_id": session.get("session_id"),
        "total": len(failed_entries),
        "by_category": {cat: [e["symbol"] for e in entries] for cat, entries in by_category.items()},
        "symbols": [
            entry for entries in by_category.values() for entry in entries
        ],
    }
    OUTPUT_FILE.write_text(json.dumps(output, indent=2))
    print(f"Written: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
