"""Immutable SSE calendar authority and market-minute session gate."""

from __future__ import annotations

import os
import re
import stat
from datetime import date, datetime, time
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

from pydantic import Field, StringConstraints, ValidationError, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.strict_json import StrictJsonError, strict_json_loads

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_READ_CHUNK_BYTES = 1024 * 1024
_MAX_CALENDAR_BYTES = 4 * 1024 * 1024


class MarketSessionCalendarError(RuntimeError):
    """The frozen calendar cannot safely establish the market session."""


class MarketSessionPhase(StrEnum):
    PRE_OPEN = "pre_open"
    MORNING = "morning"
    LUNCH = "lunch"
    AFTERNOON = "afternoon"
    CLOSED = "closed"


class MarketCalendarAuthority(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    exchange: Literal["SSE"]
    producer_commit: CommitSha
    coverage_start: date
    coverage_end: date
    open_dates: tuple[date, ...]
    generated_at: AwareUtcDatetime
    content_sha256: Sha256

    @model_validator(mode="after")
    def validate_authority(self) -> Self:
        if self.coverage_start > self.coverage_end:
            raise ValueError("coverage_start must not be after coverage_end")
        if any(
            left >= right for left, right in zip(self.open_dates, self.open_dates[1:], strict=False)
        ):
            raise ValueError("open_dates must be strictly increasing and unique")
        if any(item < self.coverage_start or item > self.coverage_end for item in self.open_dates):
            raise ValueError("open_dates must be within calendar coverage")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("content_sha256 does not bind canonical calendar content")
        return self

    @classmethod
    def create(
        cls,
        *,
        schema_version: int,
        exchange: Literal["SSE"],
        producer_commit: str,
        coverage_start: date,
        coverage_end: date,
        open_dates: tuple[date, ...],
        generated_at: datetime,
    ) -> MarketCalendarAuthority:
        identity = {
            "schema_version": schema_version,
            "exchange": exchange,
            "producer_commit": producer_commit,
            "coverage_start": coverage_start,
            "coverage_end": coverage_end,
            "open_dates": tuple(open_dates),
            "generated_at": normalize_aware_utc(generated_at),
        }
        return cls(**identity, content_sha256=canonical_sha256(identity))


class MarketSessionDecision(RuntimeContractModel):
    observed_at: AwareUtcDatetime
    local_trade_date: date
    phase: MarketSessionPhase
    is_open_date: bool
    may_fetch_market_minute: bool

    @model_validator(mode="after")
    def validate_decision(self) -> Self:
        local = self.observed_at.astimezone(_SHANGHAI)
        if self.local_trade_date != local.date():
            raise ValueError("local_trade_date conflicts with observed_at")
        expected_phase = (
            _phase_at(local.timetz().replace(tzinfo=None))
            if self.is_open_date
            else MarketSessionPhase.CLOSED
        )
        if self.phase is not expected_phase:
            raise ValueError("market session phase conflicts with observed_at")
        expected_fetch = self.is_open_date and self.phase in {
            MarketSessionPhase.MORNING,
            MarketSessionPhase.AFTERNOON,
        }
        if self.may_fetch_market_minute != expected_fetch:
            raise ValueError("may_fetch_market_minute conflicts with market session phase")
        if not self.is_open_date and self.phase is not MarketSessionPhase.CLOSED:
            raise ValueError("non-open dates must be closed")
        return self


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _require_normalized_absolute_path(path: Path) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.normpath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError("calendar path must be absolute normalized")
    return candidate


def _open_parent_without_symlinks(path: Path) -> int:
    descriptor = -1
    try:
        descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
        traversed = Path(path.anchor)
        for component in path.parts[1:]:
            traversed /= component
            try:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except OSError as exc:
                raise MarketSessionCalendarError(
                    f"calendar parent is unavailable: {traversed}"
                ) from exc
            if stat.S_ISLNK(before.st_mode):
                raise MarketSessionCalendarError(f"calendar path contains a symlink: {traversed}")
            if not stat.S_ISDIR(before.st_mode):
                raise MarketSessionCalendarError(f"calendar parent is not a directory: {traversed}")
            try:
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                raise MarketSessionCalendarError(
                    f"calendar parent changed while opening: {traversed}"
                ) from exc
            opened = os.fstat(child)
            if (before.st_dev, before.st_ino, before.st_mode) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
            ):
                os.close(child)
                raise MarketSessionCalendarError(f"calendar parent identity changed: {traversed}")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        raise


def _read_private_authority(path: Path) -> bytes:
    parent_descriptor = _open_parent_without_symlinks(path.parent)
    descriptor = -1
    try:
        try:
            before = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except OSError as exc:
            raise MarketSessionCalendarError(f"calendar authority is unavailable: {path}") from exc
        if stat.S_ISLNK(before.st_mode):
            raise MarketSessionCalendarError(f"calendar authority is a symlink: {path}")
        if not stat.S_ISREG(before.st_mode):
            raise MarketSessionCalendarError(f"calendar authority is not a regular file: {path}")
        if before.st_uid != os.geteuid():
            raise MarketSessionCalendarError(
                "calendar authority owner does not match the current process"
            )
        if stat.S_IMODE(before.st_mode) != 0o600:
            raise MarketSessionCalendarError("calendar authority must have mode 0600")
        if before.st_nlink != 1:
            raise MarketSessionCalendarError("calendar authority must have one hard link")
        if before.st_size > _MAX_CALENDAR_BYTES:
            raise MarketSessionCalendarError("calendar authority exceeds size limit")
        try:
            descriptor = os.open(path.name, _FILE_FLAGS, dir_fd=parent_descriptor)
        except OSError as exc:
            raise MarketSessionCalendarError(
                f"calendar authority changed while opening: {path}"
            ) from exc
        opened = os.fstat(descriptor)
        active = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        if _identity(opened) != _identity(before) or _identity(active) != _identity(opened):
            raise MarketSessionCalendarError(
                f"calendar authority identity changed while opening: {path}"
            )
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, _READ_CHUNK_BYTES):
            total += len(chunk)
            if total > _MAX_CALENDAR_BYTES:
                raise MarketSessionCalendarError("calendar authority exceeds size limit")
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        if _identity(after) != _identity(opened) or _identity(current) != _identity(after):
            raise MarketSessionCalendarError(f"calendar authority changed while reading: {path}")
        return b"".join(chunks)
    except MarketSessionCalendarError:
        raise
    except OSError as exc:
        raise MarketSessionCalendarError(
            f"calendar authority changed while reading: {path}"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)


def load_market_calendar_authority(path: Path, *, expected_commit: str) -> MarketCalendarAuthority:
    """Load one immutable calendar without mutating its filesystem lifecycle."""

    candidate = _require_normalized_absolute_path(path)
    if not _COMMIT_PATTERN.fullmatch(expected_commit):
        raise ValueError("expected_commit must be a lowercase 40-character Git SHA")
    payload = _read_private_authority(candidate)
    try:
        decoded = strict_json_loads(payload)
        authority = MarketCalendarAuthority.model_validate(decoded)
    except (StrictJsonError, ValidationError, TypeError, ValueError) as exc:
        raise MarketSessionCalendarError(f"invalid calendar authority: {exc}") from exc
    if authority.producer_commit != expected_commit:
        raise MarketSessionCalendarError("calendar producer_commit does not match expected_commit")
    return authority


def _phase_at(local_time: time) -> MarketSessionPhase:
    if local_time < time(9, 30):
        return MarketSessionPhase.PRE_OPEN
    if local_time <= time(11, 30):
        return MarketSessionPhase.MORNING
    if local_time < time(13, 0):
        return MarketSessionPhase.LUNCH
    if local_time <= time(15, 0):
        return MarketSessionPhase.AFTERNOON
    return MarketSessionPhase.CLOSED


def decide_market_session(
    authority: MarketCalendarAuthority, observed_at: datetime
) -> MarketSessionDecision:
    """Return a fail-closed point-in-time decision in the Shanghai market clock."""

    observed_utc = normalize_aware_utc(observed_at)
    local = observed_utc.astimezone(_SHANGHAI)
    local_date = local.date()
    if local_date < authority.coverage_start or local_date > authority.coverage_end:
        raise MarketSessionCalendarError(
            f"local trade date {local_date.isoformat()} is outside calendar coverage"
        )
    if authority.generated_at > observed_utc:
        raise MarketSessionCalendarError("calendar authority was generated after observed_at")
    is_open = local_date in authority.open_dates
    phase = _phase_at(local.timetz().replace(tzinfo=None)) if is_open else MarketSessionPhase.CLOSED
    may_fetch = phase in {MarketSessionPhase.MORNING, MarketSessionPhase.AFTERNOON}
    return MarketSessionDecision(
        observed_at=observed_utc,
        local_trade_date=local_date,
        phase=phase,
        is_open_date=is_open,
        may_fetch_market_minute=may_fetch,
    )
