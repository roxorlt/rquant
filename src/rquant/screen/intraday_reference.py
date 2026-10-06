"""Verify immutable observed references; this module does not acquire data."""

from __future__ import annotations

import hashlib
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.feature_spool import _read_bounded_file
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Code = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(SH|SZ|BJ)$")]


class IntradayReferenceObservation(RuntimeContractModel):
    ts_code: Code
    trade_date: date
    source_id: Sha256
    source_event_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    pre_close: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    up_limit: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    no_price_limit: bool | None = None
    float_shares: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    turnover_rate: float | None = Field(default=None, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def bind_observation_time(self) -> Self:
        if self.source_event_time > self.available_at:
            raise ValueError("reference availability precedes its observation")
        if self.no_price_limit is True and self.up_limit is not None:
            raise ValueError("unlimited security cannot claim an up limit")
        return self


class IntradayReferenceSnapshot(RuntimeContractModel):
    contract: Literal["intraday-screen-reference/v1"] = "intraday-screen-reference/v1"
    trade_date: date
    available_at: AwareUtcDatetime
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    universe_source_id: Sha256
    universe_available_at: AwareUtcDatetime
    universe_codes: tuple[Code, ...] = Field(min_length=1, max_length=8_000)
    calendar_source_id: Sha256
    calendar_available_at: AwareUtcDatetime
    open_dates: tuple[date, ...] = Field(min_length=2, max_length=1_000)
    observations: tuple[IntradayReferenceObservation, ...] = Field(default=(), max_length=8_000)
    identity: Sha256 | None = None

    @model_validator(mode="after")
    def bind_identity_and_universe(self) -> Self:
        if tuple(sorted(set(self.universe_codes))) != self.universe_codes:
            raise ValueError("intraday universe codes must be unique and sorted")
        if tuple(sorted(set(self.open_dates))) != self.open_dates or self.trade_date not in self.open_dates:
            raise ValueError("intraday calendar must include the requested open day")
        if self.universe_available_at > self.available_at or self.calendar_available_at > self.available_at:
            raise ValueError("intraday reference uses future universe or calendar")
        codes = tuple(item.ts_code for item in self.observations)
        if len(codes) != len(set(codes)) or set(codes) - set(self.universe_codes):
            raise ValueError("intraday reference observations exceed their universe")
        expected = canonical_sha256(self.model_dump(mode="python",exclude={"identity"}))
        if self.identity is None:
            object.__setattr__(self,"identity",expected)
        elif self.identity != expected:
            raise ValueError("intraday reference identity changed")
        return self


def visible_intraday_reference(
    observation: IntradayReferenceObservation | None, *, trade_date: date, cutoff: datetime,
) -> IntradayReferenceObservation | None:
    if observation is None or observation.trade_date != trade_date:
        return None
    if observation.source_event_time > cutoff or observation.available_at > cutoff:
        return None
    return observation


def read_intraday_reference(path: Path, *, expected_sha256: str, cutoff: datetime) -> IntradayReferenceSnapshot:
    payload = _read_bounded_file(path,label="intraday reference",maximum_bytes=4 * 1024 * 1024)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise ValueError("intraday reference file changed")
    reference = IntradayReferenceSnapshot.model_validate_json(payload)
    if reference.available_at > cutoff:
        raise ValueError("intraday reference is not yet visible")
    return reference
