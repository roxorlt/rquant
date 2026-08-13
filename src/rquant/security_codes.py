"""Storage-free security code normalization helpers."""

from __future__ import annotations


def to_ts_code(symbol: str) -> str | None:
    """Normalize a six-digit mainland symbol to a Tushare code."""

    normalized = str(symbol).strip()
    if len(normalized) != 6 or not normalized.isdigit():
        return None
    if normalized[0] == "6":
        return f"{normalized}.SH"
    if normalized[0] in {"0", "3"}:
        return f"{normalized}.SZ"
    if normalized[0] in {"4", "8", "9"}:
        return f"{normalized}.BJ"
    return None
