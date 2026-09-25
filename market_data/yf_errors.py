"""Per-ticker error capture for `yfinance.download` that works on 1.2 and 1.7.

yfinance 1.4.0 moved `download()`'s per-ticker errors from the module global
`yfinance.shared._ERRORS` into a per-call context (PR "Make yf.download()
reentrant"). The global still exists in 1.7.0 but is never written, so readers
of it go silently blind. Both versions still emit the failures on the
``yfinance`` logger at ERROR level, one line per distinct message::

    ['SYM1', 'SYM2']: YFTzMissingError('$SYM1: possibly delisted; no timezone found')

This module captures those lines for the duration of a ``with`` block and
parses them back into ``{symbol: message}``. On 1.2.x the global is merged in
as well, so nothing is lost on older pins.

Two side effects are scoped to the block and undone on exit:

* ``yfinance`` logger propagation is switched off. The collector needs the
  logger at ERROR to see the records; left propagating, every per-ticker
  failure also lands in the caller's file handlers (10k lines in one weekly
  run).
* ``yf.config.debug.hide_exceptions`` is set False. yfinance 1.7 ``download()``
  flips ``network.hide_exceptions`` but ``history()`` reads
  ``debug.hide_exceptions``, so request timeouts were swallowed and reported
  as "possibly delisted; no price data found", indistinguishable from a dead
  listing. Unhidden, they surface on the summary line as ``ReadTimeout(...)``
  and callers can file them as errors instead of empties.

Usage::

    from market_data.yf_errors import capture_yf_errors

    with capture_yf_errors() as cap:
        raw = yf.download(tickers=symbols, ...)
    errors = cap.errors   # {symbol: yfinance message}
"""

from __future__ import annotations

import ast
import logging
import re
from dataclasses import dataclass, field
from typing import Dict, Optional

YF_LOGGER_NAME = "yfinance"

# "['AAA', 'BBB']: <message>" — the list repr is what yfinance logs.
_LINE_RE = re.compile(r"^\s*(\[.*?\]):\s*(.*)$", re.DOTALL)


class _Collector(logging.Handler):
    def __init__(self, sink: Dict[str, str]) -> None:
        super().__init__(level=logging.ERROR)
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:  # noqa: BLE001 - a bad log record must not break capture
            return
        match = _LINE_RE.match(message)
        if not match:
            return
        try:
            symbols = ast.literal_eval(match.group(1))
        except (ValueError, SyntaxError):
            return
        if not isinstance(symbols, (list, tuple)):
            return
        text = match.group(2).strip()
        for sym in symbols:
            if isinstance(sym, str):
                self._sink[sym] = text


@dataclass
class capture_yf_errors:  # noqa: N801 - used as a context manager, reads like one
    """Collect per-ticker `yf.download` errors emitted during the block."""

    errors: Dict[str, str] = field(default_factory=dict)
    _handler: Optional[_Collector] = field(default=None, repr=False)
    _level_before: Optional[int] = field(default=None, repr=False)
    _propagate_before: Optional[bool] = field(default=None, repr=False)
    _hide_before: Optional[bool] = field(default=None, repr=False)

    def __enter__(self) -> "capture_yf_errors":
        logger = logging.getLogger(YF_LOGGER_NAME)
        self._handler = _Collector(self.errors)
        logger.addHandler(self._handler)
        # A logger level above ERROR would drop the records before any handler
        # sees them; lower it for the block and restore on exit.
        if logger.level > logging.ERROR:
            self._level_before = logger.level
            logger.setLevel(logging.ERROR)
        self._propagate_before = logger.propagate
        logger.propagate = False
        self._hide_before = _set_hide_exceptions(False)
        _legacy_global().clear()
        return self

    def __exit__(self, *exc_info: object) -> None:
        logger = logging.getLogger(YF_LOGGER_NAME)
        if self._handler is not None:
            logger.removeHandler(self._handler)
            self._handler = None
        if self._level_before is not None:
            logger.setLevel(self._level_before)
            self._level_before = None
        if self._propagate_before is not None:
            logger.propagate = self._propagate_before
            self._propagate_before = None
        if self._hide_before is not None:
            _set_hide_exceptions(self._hide_before)
            self._hide_before = None
        # yfinance <= 1.3 still populates the global; merge without overriding
        # anything the log parser already recorded.
        for sym, err in _legacy_global().items():
            self.errors.setdefault(str(sym), str(err))


def _set_hide_exceptions(value: bool) -> Optional[bool]:
    """Set ``yf.config.debug.hide_exceptions``; return the previous value.

    Returns None (and does nothing) on yfinance builds without the config
    object, so the capture degrades to log parsing only.
    """
    try:
        import yfinance as yf

        debug = yf.config.debug
    except (ImportError, AttributeError):  # pragma: no cover - old yfinance
        return None
    before = debug.hide_exceptions
    debug.hide_exceptions = value
    return bool(before) if before is not None else True


def _legacy_global() -> Dict[str, str]:
    try:
        from yfinance import shared
    except ImportError:  # pragma: no cover - yfinance is a hard dependency
        return {}
    return getattr(shared, "_ERRORS", {})
