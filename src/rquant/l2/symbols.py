"""Supported routing syntax, not a listing or data-entitlement certificate."""
from __future__ import annotations

from collections.abc import Sequence

MAX_SYMBOLS = 100


def market_for_code(code: str) -> int:
    if type(code) is not str or len(code) != 6 or not code.isascii() or not code.isdigit():
        raise ValueError('symbol must contain six ASCII digits')
    if code[0] == '6':
        return 17
    if code[0] in '03':
        return 33
    raise ValueError('unsupported stock routing syntax')


def normalize_symbols(symbols: Sequence[str]) -> tuple[str, ...]:
    if (not isinstance(symbols, Sequence) or isinstance(symbols, (str, bytes, bytearray))
            or len(symbols) > MAX_SYMBOLS):
        raise ValueError('symbol list must contain at most100 entries')
    result: dict[str, None] = {}
    for code in symbols:
        market_for_code(code)
        result[code] = None
    return tuple(result)
