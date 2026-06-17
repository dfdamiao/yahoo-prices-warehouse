#!/usr/bin/env python3
"""failed_symbols.py — list symbols flagged failed or suspected_discontinued.

Two sources:
- price_history.metadata: data_status == 'failed'
- symbol_metadata/ticker_classification: suspected_discontinued == True

Examples:
    python market_data/cli/failed_symbols.py
    python market_data/cli/failed_symbols.py --kind discontinued --json
    python market_data/cli/failed_symbols.py --kind both --limit 50
"""

from __future__ import annotations

import argparse


from market_data.cli._common import (  # noqa: E402
    add_common_args,
    emit_error,
    emit_json,
    open_mda,
    preflight_or_exit,
)

_DESCRIPTION = "List symbols flagged failed (data_status) or discontinued (classification)."
_KIND_CHOICES = ("failed", "discontinued", "both")


def main() -> None:
    parser = argparse.ArgumentParser(description=_DESCRIPTION)
    parser.add_argument(
        "--kind",
        default="failed",
        choices=_KIND_CHOICES,
        help=(
            "failed: data_status='failed' in price_history.metadata; "
            "discontinued: suspected_discontinued=True in ticker_classification; "
            "both: union of the two."
        ),
    )
    parser.add_argument(
        "--limit", type=int, default=None, help="Cap rows returned (default: no cap)"
    )
    add_common_args(parser)
    args = parser.parse_args()

    preflight_or_exit()

    with open_mda(args.db_path) as mda:
        failed: list[str] = []
        discontinued: list[str] = []

        if args.kind in ("failed", "both"):
            failed = sorted(mda.get_failed_symbols())

        if args.kind in ("discontinued", "both"):
            tclass = mda.get_ticker_classification()
            if tclass is not None and not tclass.empty and "suspected_discontinued" in tclass.columns:
                mask = tclass["suspected_discontinued"].fillna(False).astype(bool)
                discontinued = sorted(str(s) for s in tclass.index[mask])

    if args.kind == "failed":
        symbols = failed
    elif args.kind == "discontinued":
        symbols = discontinued
    else:  # both
        symbols = sorted(set(failed) | set(discontinued))

    if not symbols:
        emit_error(
            f"no symbols matched kind='{args.kind}' (clean DB or unsupported field)",
            as_json=args.json,
            exit_code=1,
        )
        return

    total = len(symbols)
    if args.limit:
        symbols = symbols[: args.limit]

    if args.json:
        payload: dict = {
            "kind": args.kind,
            "total": total,
            "returned": len(symbols),
            "symbols": symbols,
        }
        if args.kind == "both":
            payload["failed_count"] = len(failed)
            payload["discontinued_count"] = len(discontinued)
        emit_json(payload)
    else:
        print(f"# kind={args.kind}  total={total}  showing={len(symbols)}")
        if args.kind == "both":
            print(f"# failed={len(failed)}  discontinued={len(discontinued)}")
        for s in symbols:
            print(s)


if __name__ == "__main__":
    main()
