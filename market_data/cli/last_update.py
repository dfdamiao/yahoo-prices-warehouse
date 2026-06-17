#!/usr/bin/env python3
"""last_update.py — batch staleness check across symbols.

Reports the most recent bar date for each symbol from price_history.daily,
plus optional cross-check against price_history.metadata's last_date column.

Examples:
    python market_data/cli/last_update.py --symbols SPY,QQQ,TLT
    python market_data/cli/last_update.py --symbols SPY --json
    python market_data/cli/last_update.py --all --limit 50
"""

from __future__ import annotations

import argparse


import pandas as pd

from market_data.cli._common import (  # noqa: E402
    add_common_args,
    emit_error,
    emit_json,
    open_mda,
    preflight_or_exit,
)

_DESCRIPTION = "Batch staleness check across symbols (last bar date)."


def _last_date(prices) -> str | None:
    if prices is None or prices.empty:
        return None
    last = prices.index.max()
    return last.strftime("%Y-%m-%d") if pd.notna(last) else None


def _meta_last_date(price_meta_lib, symbol: str) -> str | None:
    try:
        df = price_meta_lib.read(symbol).data
    except Exception:
        return None
    if not isinstance(df, pd.DataFrame) or df.empty or "last_date" not in df.columns:
        return None
    val = df.iloc[-1]["last_date"]
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    return str(val)


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--symbols", help="Comma-separated symbols, e.g. SPY,QQQ,TLT"
    )
    group.add_argument(
        "--all",
        action="store_true",
        help="Check all symbols in price_history.daily (combine with --limit)",
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Limit when --all is used"
    )
    parser.add_argument(
        "--stale-only",
        action="store_true",
        help="Return only symbols whose last_bar is older than --stale-days days",
    )
    parser.add_argument(
        "--stale-days",
        type=int,
        default=30,
        help="Threshold for --stale-only in calendar days (default: 30)",
    )
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    stale_cutoff = (
        pd.Timestamp.now().normalize() - pd.Timedelta(days=args.stale_days)
        if args.stale_only
        else None
    )

    with open_mda(args.db_path) as mda:
        if args.all:
            symbols = sorted(mda.price_history_lib.list_symbols())
            if args.limit is not None and not args.stale_only:
                symbols = symbols[: args.limit]
        else:
            symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
            if not symbols:
                emit_error(
                    "no symbols provided", as_json=args.json, exit_code=1
                )

        rows: list[dict] = []
        known = set(mda.price_history_lib.list_symbols())
        for sym in symbols:
            if sym not in known:
                rows.append(
                    {
                        "symbol": sym,
                        "found": False,
                        "last_bar": None,
                        "meta_last_date": None,
                    }
                )
                continue
            prices = mda.price_history_lib.read(sym).data
            last_bar_str = _last_date(prices)
            if args.stale_only and stale_cutoff is not None:
                if last_bar_str is None:
                    continue
                if pd.Timestamp(last_bar_str) >= stale_cutoff:
                    continue
            rows.append(
                {
                    "symbol": sym,
                    "found": True,
                    "last_bar": last_bar_str,
                    "meta_last_date": _meta_last_date(mda.price_metadata_lib, sym),
                }
            )

        if args.stale_only and args.limit is not None:
            rows = rows[: args.limit]

    if args.json:
        payload: dict = {"count": len(rows), "results": rows}
        if args.stale_only:
            payload["stale_days_threshold"] = args.stale_days
            payload["stale_cutoff"] = stale_cutoff.strftime("%Y-%m-%d")
        emit_json(payload)
    else:
        if args.stale_only:
            print(f"# stale-only: last_bar < {stale_cutoff.strftime('%Y-%m-%d')} "
                  f"(>{args.stale_days}d old)  matches={len(rows)}")
        print(f"{'symbol':<12} {'found':<6} {'last_bar':<12} {'meta_last_date':<14}")
        for r in rows:
            print(
                f"{r['symbol']:<12} "
                f"{str(r['found']):<6} "
                f"{(r['last_bar'] or '-'): <12} "
                f"{(r['meta_last_date'] or '-'): <14}"
            )


if __name__ == "__main__":
    main()
