"""The ONE place the web app writes: forward a command to the page-control service.

The web process never touches files or databases. It builds a command, POSTs it to
``RQUANT_PAGE_CONTROL_URL`` (main's page-control consumer, default
http://127.0.0.1:8767/v1/commands) and returns the receipt as-is.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

Transport = Callable[[dict[str, Any]], dict[str, Any]]
DEFAULT_URL = "http://127.0.0.1:8767/v1/commands"


class PageControlUnavailableError(RuntimeError):
    pass


def _http_post(payload: dict[str, Any]) -> dict[str, Any]:
    url = os.environ.get("RQUANT_PAGE_CONTROL_URL", DEFAULT_URL)
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=True).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=3) as response:  # noqa: S310 - local URL
        body = json.loads(response.read().decode())
    if not isinstance(body, dict):
        raise PageControlUnavailableError("page control returned a non-object response")
    return body


def forward(
    kind: str, fields: dict[str, Any], transport: Transport | None = None
) -> dict[str, Any]:
    """Send one command; returns the page-control receipt (``status``, ``command_id``...)."""

    payload = {
        "kind": kind,
        "command_id": uuid.uuid4().hex,
        "requested_at": datetime.now(UTC).isoformat(),
        **fields,
    }
    try:
        return (transport or _http_post)(payload)
    except (OSError, TimeoutError, urllib.error.URLError, ValueError) as exc:
        raise PageControlUnavailableError(f"{type(exc).__name__}: {exc}") from exc
