"""Failure-isolated structured logging for Strategy Lab background services."""

from __future__ import annotations

import sys
from contextlib import suppress
from typing import Literal

from loguru import logger

LabLogLevel = Literal["warning", "error"]


def _safe_structured_log(
    level: LabLogLevel,
    event: str,
    *,
    message: str,
    **context: object,
) -> None:
    """Emit one structured event without making logging part of control flow."""
    normalized_message = (" ".join(message.split()) or event)[:400]
    try:
        logger.bind(failure=event, **context).log(level.upper(), "{}", normalized_message)
    except Exception as exc:
        with suppress(Exception):
            sys.stderr.write(
                f"rquant lab structured log failed event={event} error_type={type(exc).__name__}\n"
            )
