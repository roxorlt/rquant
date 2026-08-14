"""Small shared primitives for immutable cross-process runtime contracts."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from pathlib import PurePath
from typing import Annotated
from uuid import UUID

from pydantic import AfterValidator, BaseModel, ConfigDict


class RuntimeContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        revalidate_instances="always",
        str_strip_whitespace=True,
    )


def normalize_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


AwareUtcDatetime = Annotated[datetime, AfterValidator(normalize_aware_utc)]


def _canonical_decimal(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("canonical numeric values must be finite")
    if value.is_zero():
        return "0"
    normalized = value.normalize()
    return format(normalized, "f")


def _canonical_value(value: object) -> object:
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump(mode="python"))
    if isinstance(value, Enum):
        return _canonical_value(value.value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical numeric values must be finite")
        return {"$float": repr(value)}
    if isinstance(value, Decimal):
        return {"$decimal": _canonical_decimal(value)}
    if isinstance(value, datetime):
        normalized = normalize_aware_utc(value)
        return {"$datetime": normalized.isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"$date": value.isoformat()}
    if isinstance(value, UUID):
        return {"$uuid": str(value)}
    if isinstance(value, PurePath):
        return str(value)
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical mappings require string keys")
        return {
            key: _canonical_value(item)
            for key, item in sorted(value.items(), key=lambda pair: pair[0])
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_canonical_value(item) for item in value]
    raise TypeError(f"unsupported canonical value: {type(value).__name__}")


def canonical_sha256(value: object) -> str:
    payload = json.dumps(
        _canonical_value(value),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
