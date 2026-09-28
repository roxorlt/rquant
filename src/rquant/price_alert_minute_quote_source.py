"""Convert normalized Tushare real-time minute bars into price-alert evidence."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import TypeAdapter, ValidationError

from rquant.alert_price_rule import ObservedPriceQuote
from rquant.manual_watchlist import TsCode
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_price_alert_evaluation import PriceQuoteEvidence

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_CODES = TypeAdapter(tuple[TsCode, ...])
_MAX_CODES = 1000
_COLUMNS = ("ts_code", "trade_time", "close", "freq", "source")


def _source_minute(value: object, *, trade_date: date, received_at: datetime) -> datetime | None:
    if not isinstance(value, datetime) or pd.isna(value):
        return None
    local = (value if value.tzinfo is not None else value.replace(tzinfo=_SHANGHAI)).astimezone(
        _SHANGHAI
    )
    if local.second or local.microsecond or getattr(local, "nanosecond", 0):
        return None
    wall = local.time()
    if not (time(9, 30) <= wall < time(11, 30) or time(13, 0) <= wall < time(14, 57)):
        return None
    if local.date() != trade_date or local > received_at:
        return None
    return local


def _positive_price(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        price = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return price if price.is_finite() and price > 0 else None


def price_quote_evidence_from_rt_min(
    frame: pd.DataFrame,
    *,
    trade_date: date,
    received_at: datetime,
    requested_codes: tuple[TsCode, ...],
) -> tuple[PriceQuoteEvidence, ...]:
    """Keep only current-day source minutes; reject ambiguous or mislabelled batches."""
    if type(trade_date) is not date:
        raise ValueError("trade_date must be an explicit date")
    received = normalize_aware_utc(received_at)
    if received.astimezone(_SHANGHAI).date() != trade_date:
        raise ValueError("received_at must be on trade_date in Shanghai")
    if not 1 <= len(requested_codes) <= _MAX_CODES:
        raise ValueError("requested_codes must contain 1 to 1000 codes")
    try:
        codes = _CODES.validate_python(requested_codes)
    except ValidationError as exc:
        raise ValueError("requested_codes contain an invalid code") from exc
    if len(set(codes)) != len(codes):
        raise ValueError("requested_codes must be distinct")
    if len(frame) == 0:
        return ()
    if not frame.columns.is_unique or not set(_COLUMNS).issubset(frame.columns):
        raise ValueError("rt_min frame must contain unique normalized quote columns")

    rows = tuple(frame.loc[:, _COLUMNS].itertuples(index=False, name=None))
    requested = set(codes)
    seen: set[str] = set()
    for code, _, _, freq, source in rows:
        if not isinstance(code, str) or code not in requested:
            raise ValueError("rt_min frame contains an unrequested code")
        if code in seen:
            raise ValueError("rt_min frame contains a duplicate code")
        seen.add(code)
        if not isinstance(freq, str) or freq != "1min":
            raise ValueError("rt_min frame has an unexpected source or freq")
        if not isinstance(source, str) or source != "tushare_rt":
            raise ValueError("rt_min frame has an unexpected source or freq")

    by_code: dict[str, PriceQuoteEvidence] = {}
    for code, source_time, close, _, _ in rows:
        observed = _source_minute(source_time, trade_date=trade_date, received_at=received)
        price = _positive_price(close)
        if observed is None or price is None:
            continue
        by_code[code] = PriceQuoteEvidence(
            quote=ObservedPriceQuote(
                ts_code=code, price=price, observed_at=observed, trade_date=trade_date
            ),
            source_timestamp_provenance="provider_source_timestamp",
        )
    return tuple(by_code[code] for code in codes if code in by_code)
