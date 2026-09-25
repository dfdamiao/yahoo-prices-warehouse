"""Per-ticker error capture must not depend on ``yfinance.shared._ERRORS``.

yfinance 1.4.0 made ``download()`` reentrant by moving per-ticker errors into a
per-call context. ``yfinance.shared._ERRORS`` still exists in 1.7 but
``download()`` never writes to it, so a reader of that global goes silently
blind: a dead ticker comes back as an empty frame with no error attached.
Both versions still emit one ERROR log line per distinct failure on the
``yfinance`` logger::

    ['SYM1', 'SYM2']: YFTzMissingError('$SYM1: possibly delisted; no timezone found')

Every test here is offline: the download is faked to behave like 1.7.
"""

from __future__ import annotations

import logging

import pandas as pd
import pytest
from yfinance import shared

YF_LOGGER = logging.getLogger("yfinance")


def _emit_failed_download(symbols: list[str], message: str) -> None:
    """Log exactly what yfinance 1.2 and 1.7 emit after a failed batch."""
    YF_LOGGER.error(
        "\n%.f Failed download%s:" % (len(symbols), "s" if len(symbols) > 1 else "")
    )
    YF_LOGGER.error(f"{symbols}: " + message)


def _fake_download_like_1_7(symbols_msgs: dict[str, str]):
    """Stand-in for yf.download that reports errors the 1.7 way: failing
    tickers are logged and omitted; every other ticker gets one OHLCV row
    under a ``group_by="ticker"`` MultiIndex."""

    def fake(*args, **kwargs):
        tickers = kwargs.get("tickers") or (args[0] if args else [])
        tickers = list(tickers)
        failing = {s: m for s, m in symbols_msgs.items() if s in tickers}
        for msg in set(failing.values()):
            syms = [s for s, m in failing.items() if m == msg]
            _emit_failed_download(syms, msg)
        assert not shared._ERRORS, "fake must not populate the removed global"
        good = [t for t in tickers if t not in failing]
        if not good:
            return pd.DataFrame()
        fields = ["Open", "High", "Low", "Close", "Volume"]
        cols = pd.MultiIndex.from_product([good, fields])
        idx = pd.DatetimeIndex([pd.Timestamp("2026-09-11")], name="Date")
        return pd.DataFrame([[1.0] * len(cols)], index=idx, columns=cols)

    return fake


def test_capture_parses_failed_download_log_lines() -> None:
    from market_data.yf_errors import capture_yf_errors

    msg = "YFTzMissingError('$AAA: possibly delisted; no timezone found')"
    with capture_yf_errors() as cap:
        _emit_failed_download(["AAA", "BBB"], msg)

    assert cap.errors == {"AAA": msg, "BBB": msg}


def test_capture_ignores_lines_without_symbol_list_prefix() -> None:
    from market_data.yf_errors import capture_yf_errors

    with capture_yf_errors() as cap:
        YF_LOGGER.error("\n1 Failed download:")
        YF_LOGGER.error("some unrelated yfinance error")

    assert cap.errors == {}


def test_capture_is_scoped_to_the_with_block() -> None:
    from market_data.yf_errors import capture_yf_errors

    with capture_yf_errors() as cap:
        pass
    _emit_failed_download(["ZZZ"], "Exception('after exit')")

    assert cap.errors == {}


def test_capture_survives_yfinance_logger_set_above_error() -> None:
    from market_data.yf_errors import capture_yf_errors

    previous = YF_LOGGER.level
    YF_LOGGER.setLevel(logging.CRITICAL)
    try:
        with capture_yf_errors() as cap:
            _emit_failed_download(["AAA"], "Exception('x')")
        assert cap.errors == {"AAA": "Exception('x')"}
        assert YF_LOGGER.level == logging.CRITICAL
    finally:
        YF_LOGGER.setLevel(previous)


def test_capture_keeps_yfinance_records_off_the_root_handlers() -> None:
    """The capture must consume the records rather than let every per-ticker
    failure propagate into the caller's handlers, and restore propagation."""
    from market_data.yf_errors import capture_yf_errors

    root_seen: list[str] = []

    class _Sink(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            root_seen.append(record.getMessage())

    sink = _Sink(level=logging.ERROR)
    root = logging.getLogger()
    root.addHandler(sink)
    propagate_before = YF_LOGGER.propagate
    try:
        with capture_yf_errors() as cap:
            _emit_failed_download(["AAA"], "Exception('x')")
        assert cap.errors == {"AAA": "Exception('x')"}
        assert not any("AAA" in m for m in root_seen), root_seen
        assert YF_LOGGER.propagate == propagate_before
    finally:
        root.removeHandler(sink)


def test_symbol_updater_download_batch_reports_errors_without_shared_globals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from market_data import symbol_updater as su

    msg = "YFTzMissingError('$ZZZZ: possibly delisted; no timezone found')"
    monkeypatch.setattr(su.yf, "download", _fake_download_like_1_7({"ZZZZ": msg}))
    monkeypatch.setattr(su.time, "sleep", lambda *_: None)

    class _NoopSession:
        def reset_session_if_needed(self) -> None:
            return None

    # Bypass __init__: it opens the store. Only the attributes the download
    # path touches are set.
    updater = object.__new__(su.SymbolUpdater)
    updater.update_period = "1mo"
    updater.enable_resource_monitoring = False
    updater.session_manager = _NoopSession()
    updater.successful_downloads = 0

    data, rate_limited, _status, yf_errors = updater._download_batch_enhanced(
        ["GOOD", "ZZZZ"], max_retries=1
    )

    assert rate_limited is False
    assert data is not None and not data.empty
    assert yf_errors == {"ZZZZ": msg}
