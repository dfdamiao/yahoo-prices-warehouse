"""Optional Telegram notifications for long-running ingestion jobs.

No-op unless both ``TELEGRAM_BOT_TOKEN`` and ``TELEGRAM_CHAT_ID`` are set in the
environment — so the updater runs fine with no notification config at all.
Credentials are never hardcoded.
"""
from __future__ import annotations

import functools
import logging
import os
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)


def send_telegram_notification(message: str, parse_mode: str = "HTML") -> bool:
    """Best-effort Telegram message. Returns False (and logs) if unconfigured or
    on any error — never raises, so it cannot break an ingestion run."""
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logger.debug("Telegram not configured; skipping notification.")
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = urllib.parse.urlencode(
            {"chat_id": chat_id, "text": message, "parse_mode": parse_mode}
        ).encode()
        with urllib.request.urlopen(url, data=data, timeout=10) as resp:
            return resp.status == 200
    except Exception as exc:  # noqa: BLE001 - notifications must never break a run
        logger.warning("Telegram notification failed: %s", exc)
        return False


def notification_decorator(*_args, **_kwargs):
    """Optional job-notification decorator. Runs the wrapped function and, if
    Telegram is configured, sends a one-line failure note. Accepts (and ignores)
    keyword options like ``message=``, ``include_result=``, ``notify_on_error=``
    for drop-in compatibility."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                send_telegram_notification(f"❌ {func.__name__} failed: {exc}")
                raise

        return wrapper

    return decorator
