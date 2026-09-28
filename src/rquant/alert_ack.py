"""Stable identities and bounded time rules for confirmable alerts."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from datetime import UTC, date, datetime, time, timedelta
from math import isfinite
from typing import Literal
from unicodedata import normalize
from zoneinfo import ZoneInfo

from rquant.runtime_contracts import canonical_sha256
from rquant.signal_contracts import SignalEnvelopeFamily, parse_signal_envelope

AlertSource = Literal["signal", "monitor_event", "surge_event"]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_STOCK_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_REQUIRED_TEXT = frozenset({"ts_code", "level", "name", "theme", "status"})
_MONITOR_LEVELS = frozenset(
    {
        "attack_open_strength",
        "attack_break_high",
        "attack_strong_carry",
        "attack_near_limit",
    }
)
_MONITOR_FIELDS = (
    "trade_date",
    "trigger_time",
    "ts_code",
    "level",
    "trigger_price",
    "level_price",
    "trigger_type",
    "pool",
)
_SURGE_FIELDS = (
    "trade_date",
    "confirmed_at",
    "ts_code",
    "name",
    "theme",
    "price",
    "pct_chg",
    "cum_amount",
    "rel_cum",
    "room_to_limit_pct",
    "status",
)
_NUMERIC_FIELDS = frozenset(
    {
        "trigger_price",
        "level_price",
        "price",
        "pct_chg",
        "cum_amount",
        "rel_cum",
        "room_to_limit_pct",
    }
)


def _trade_date(value: object) -> date:
    if type(value) is date:
        return value
    if isinstance(value, str):
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("invalid trade_date") from exc
        if parsed.isoformat() == value:
            return parsed
    raise ValueError("invalid trade_date")


def _utc_microsecond(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds")


def _monitor_time(value: object, trade_date: date) -> str:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError("invalid monitor trigger_time") from exc
    if not isinstance(value, datetime):
        raise ValueError("invalid monitor trigger_time")
    if value.tzinfo is None:
        local = value.replace(tzinfo=_SHANGHAI)
    elif value.utcoffset() is None:
        raise ValueError("invalid monitor trigger_time")
    else:
        local = value.astimezone(_SHANGHAI)
    if local.date() != trade_date:
        raise ValueError("monitor trigger_time and trade_date disagree")
    return _utc_microsecond(local)


def _surge_time(value: object, trade_date: date) -> str:
    if not isinstance(value, str) or len(value) != 5 or value[2] != ":":
        raise ValueError("invalid surge confirmed_at time")
    try:
        observed = time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("invalid surge confirmed_at time") from exc
    if observed.strftime("%H:%M") != value:
        raise ValueError("invalid surge confirmed_at time")
    if not (time(9, 25) <= observed <= time(11, 30) or time(13) <= observed <= time(15)):
        raise ValueError("surge confirmed_at is outside the A-share session")
    return _utc_microsecond(datetime.combine(trade_date, observed, tzinfo=_SHANGHAI))


def _field(name: str, value: object) -> object:
    if name in _NUMERIC_FIELDS:
        if value is None:
            return ["null"]
        if isinstance(value, bool) or not isinstance(value, (float, int)):
            raise ValueError(f"{name} must be a finite IEEE 754 number")
        try:
            number = float(value)
        except OverflowError as exc:
            raise ValueError(f"{name} must be finite") from exc
        if not isfinite(number):
            raise ValueError(f"{name} must be finite")
        return ["float", (0.0 if number == 0.0 else number).hex()]
    if value is None:
        if name in _REQUIRED_TEXT:
            raise ValueError(f"{name} is required")
        return ["null"]
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string or null")
    if name == "ts_code" and _STOCK_CODE.fullmatch(value) is None:
        raise ValueError("invalid ts_code")
    if name == "level" and value not in _MONITOR_LEVELS:
        raise ValueError("invalid monitor level")
    if name == "status" and value not in {"confirmed", "unbuyable"}:
        raise ValueError("invalid surge status")
    return ["str", normalize("NFC", value)]


def stable_alert_id(
    source: str,
    event: Mapping[str, object] | SignalEnvelopeFamily,
) -> str:
    """Derive a v1 identity from verified, published trigger facts only."""
    if source == "signal":
        payload = (
            event.model_dump(mode="json") if isinstance(event, SignalEnvelopeFamily) else event
        )
        verified = parse_signal_envelope(payload)
        if verified.signal_id is None:
            raise ValueError("signal_id identity is missing")
        return stable_signal_alert_id(verified.signal_id)
    elif source in {"monitor_event", "surge_event"}:
        if not isinstance(event, Mapping):
            raise ValueError("alert event must be a mapping")
        names = _MONITOR_FIELDS if source == "monitor_event" else _SURGE_FIELDS
        missing = set(names) - set(event)
        if missing:
            raise ValueError(f"missing alert identity fields: {', '.join(sorted(missing))}")
        trade_date = _trade_date(event["trade_date"])
        fields = []
        for name in names:
            if name == "trade_date":
                value: object = ["date", trade_date.isoformat()]
            elif name == "trigger_time":
                value = ["time", _monitor_time(event[name], trade_date)]
            elif name == "confirmed_at":
                value = ["time", _surge_time(event[name], trade_date)]
            else:
                value = _field(name, event[name])
            fields.append([name, value])
    else:
        raise ValueError(f"source {source!r} is not confirmable")
    return canonical_sha256({"domain": "rquant-alert/v1", "source": source, "fields": fields})


def stable_signal_alert_id(signal_id: str) -> str:
    """Map a signal ID already verified from a trusted Serving row to alert identity.

    A caller must not use this to treat arbitrary browser input as a verified envelope.
    """
    if re.fullmatch(r"[0-9a-f]{64}", signal_id) is None:
        raise ValueError("invalid published signal_id")
    return canonical_sha256(
        {
            "domain": "rquant-alert/v1",
            "source": "signal",
            "fields": [["signal_id", signal_id]],
        }
    )


def unique_alert_ids(
    source: AlertSource,
    events: Iterable[Mapping[str, object] | SignalEnvelopeFamily],
) -> tuple[str, ...]:
    identifiers = tuple(stable_alert_id(source, event) for event in events)
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("duplicate alert identity in source")
    return identifiers


def alert_event_at(
    source: AlertSource,
    event: Mapping[str, object] | SignalEnvelopeFamily,
) -> datetime:
    """Interpret the trigger instant using the same source clock rules as its identity."""
    if source == "signal":
        payload = (
            event.model_dump(mode="json") if isinstance(event, SignalEnvelopeFamily) else event
        )
        return parse_signal_envelope(payload).event_time.astimezone(UTC)
    if not isinstance(event, Mapping):
        raise ValueError("alert event must be a mapping")
    trade_date = _trade_date(event.get("trade_date"))
    if source == "monitor_event":
        return datetime.fromisoformat(_monitor_time(event.get("trigger_time"), trade_date))
    if source == "surge_event":
        return datetime.fromisoformat(_surge_time(event.get("confirmed_at"), trade_date))
    raise ValueError(f"source {source!r} is not confirmable")


def alert_window_start(*, count_as_of: datetime, activated_at: datetime) -> datetime:
    if count_as_of.tzinfo is None or activated_at.tzinfo is None:
        raise ValueError("alert window timestamps must be timezone-aware")
    local_day = count_as_of.astimezone(_SHANGHAI).date() - timedelta(days=29)
    calendar_start = datetime.combine(local_day, time.min, tzinfo=_SHANGHAI)
    return max(calendar_start.astimezone(UTC), activated_at.astimezone(UTC))
