"""Bounded member files identify normalized claims, not provider coverage or PIT."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
from collections.abc import Iterable
from datetime import date
from itertools import pairwise
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.result_artifact import (
    _READ_FLAGS,
    _cleanup_owned_temporary,
    _file_identity,
    _open_private_root,
    _require_named_regular,
    _require_same_root,
    _root_path,
    _write_all,
)
from rquant.factor.time_series import MAX_TRADE_DAYS
from rquant.factor.universe import (
    MAX_UNIVERSE_SECURITIES,
    DailyIndexConstituentBatch,
    DailySecurityBatch,
    FactorUniverseRequest,
    ObservedTime,
    Sha256,
    SourceId,
    StockCode,
    UniverseSelection,
    select_factor_universe,
)
from rquant.private_fs import rename_noreplace_at
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

MAX_FACTOR_MEMBER_DAY_BYTES = 4 * 1024 * 1024
MAX_FACTOR_MEMBER_MANIFEST_BYTES = 2 * 1024 * 1024
_DAY_PREFIX = "factor-member-day-v1-"
_ARCHIVE_PREFIX = "factor-member-archive-v1-"
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")


def _bytes(model: BaseModel) -> bytes:
    return canonical_json_bytes(model.model_dump(mode="json", round_trip=True))


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FactorMemberDayInput(BaseModel):
    """Original labels and claimed hashes are retained without endorsing them."""

    model_config = _IMMUTABLE

    schema_version: Literal[1]
    trade_date: date
    securities: DailySecurityBatch
    membership: DailyIndexConstituentBatch | None

    @model_validator(mode="after")
    def _same_day(self) -> FactorMemberDayInput:
        if self.securities.trade_date != self.trade_date or (
            self.membership is not None and self.membership.trade_date != self.trade_date
        ):
            raise ValueError("member payload dates differ")
        return self


class FactorMemberArchiveRequest(BaseModel):
    model_config = _IMMUTABLE

    selection: UniverseSelection
    trading_days: tuple[date, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)
    as_of: ObservedTime
    computation_stock_codes: tuple[StockCode, ...] = Field(
        min_length=1, max_length=MAX_UNIVERSE_SECURITIES
    )

    @field_validator("computation_stock_codes")
    @classmethod
    def _codes(cls, codes: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(codes)) != len(codes):
            raise ValueError("duplicate member computation code")
        return tuple(sorted(codes))

    @model_validator(mode="after")
    def _schedule(self) -> FactorMemberArchiveRequest:
        if any(left >= right for left, right in pairwise(self.trading_days)):
            raise ValueError("member schedule must strictly ascend")
        return self


class FactorMemberSources(BaseModel):
    model_config = _IMMUTABLE

    source_mode: Literal["historical_retrospective"]
    security_source_id: SourceId
    security_source_sha256: Sha256
    index_source_id: SourceId | None
    index_source_sha256: Sha256 | None


class FactorMemberDayReference(BaseModel):
    model_config = _IMMUTABLE

    trade_date: date
    sha256: Sha256
    filename: str
    byte_count: int = Field(gt=0, le=MAX_FACTOR_MEMBER_DAY_BYTES)
    security_payload_sha256: Sha256
    index_payload_sha256: Sha256 | None
    security_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)
    index_count: int | None = Field(default=None, ge=0, le=MAX_UNIVERSE_SECURITIES)
    selected_count: int = Field(ge=0, le=MAX_UNIVERSE_SECURITIES)

    @model_validator(mode="after")
    def _reference(self) -> FactorMemberDayReference:
        if self.filename != f"{_DAY_PREFIX}{self.sha256}.json":
            raise ValueError("member day filename differs from digest")
        if (self.index_payload_sha256 is None) != (self.index_count is None):
            raise ValueError("member index component metadata differs")
        if self.selected_count > self.security_count:
            raise ValueError("member selection exceeds security facts")
        return self


def _sources(days: tuple[FactorMemberDayReference, ...]) -> FactorMemberSources:
    security = canonical_sha256(
        (
            "factor-member-security-v1",
            tuple((d.trade_date, d.security_payload_sha256) for d in days),
        )
    )
    index = (
        canonical_sha256(
            ("factor-member-index-v1", tuple((d.trade_date, d.index_payload_sha256) for d in days))
        )
        if days[0].index_payload_sha256 is not None
        else None
    )
    return FactorMemberSources(
        source_mode="historical_retrospective",
        security_source_id=f"factor-member-security-v1:{security}",
        security_source_sha256=security,
        index_source_id=None if index is None else f"factor-member-index-v1:{index}",
        index_source_sha256=index,
    )


class FactorMemberArchiveManifest(BaseModel):
    model_config = _IMMUTABLE

    schema_version: Literal[1]
    request: FactorMemberArchiveRequest
    request_sha256: Sha256
    sources: FactorMemberSources
    days: tuple[FactorMemberDayReference, ...] = Field(min_length=1, max_length=MAX_TRADE_DAYS)

    @model_validator(mode="after")
    def _bindings(self) -> FactorMemberArchiveManifest:
        if self.request_sha256 != canonical_sha256(self.request):
            raise ValueError("member archive request digest differs")
        if tuple(day.trade_date for day in self.days) != self.request.trading_days:
            raise ValueError("member archive schedule differs")
        is_index = self.request.selection in ("hs300", "zz1000")
        if any((day.index_payload_sha256 is not None) != is_index for day in self.days):
            raise ValueError("member archive index components differ from selection")
        if self.sources != _sources(self.days):
            raise ValueError("member archive source identity differs from payload sequence")
        return self


class FactorMemberArchiveReference(BaseModel):
    """Small, path-free reference suitable for a future explicit v2 job spec."""

    model_config = _IMMUTABLE

    sha256: Sha256
    filename: str
    byte_count: int = Field(gt=0, le=MAX_FACTOR_MEMBER_MANIFEST_BYTES)

    @model_validator(mode="after")
    def _name(self) -> FactorMemberArchiveReference:
        if self.filename != f"{_ARCHIVE_PREFIX}{self.sha256}.json":
            raise ValueError("member archive filename differs from digest")
        return self


def _input_name(name: str) -> str:
    if not isinstance(name, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,254}", name) is None:
        raise ValueError("member input must be a direct canonical filename")
    return name


def _read_file(
    root_fd: int, name: str, limit: int, expected_sha: str | None = None
) -> tuple[bytes, tuple[int, ...]]:
    descriptor = os.open(name, _READ_FLAGS, dir_fd=root_fd)
    try:
        before = _require_named_regular(root_fd, name, descriptor)
        if not 0 < before.st_size <= limit:
            raise ValueError("member file exceeds byte limit or is empty")
        data = bytearray()
        while len(data) <= limit:
            chunk = os.read(descriptor, min(1024 * 1024, limit + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        if len(data) != before.st_size or _file_identity(
            _require_named_regular(root_fd, name, descriptor)
        ) != _file_identity(before):
            raise ValueError("member file changed during read")
        result = bytes(data)
        if expected_sha is not None and _sha(result) != expected_sha:
            raise ValueError("member file content digest differs")
        return result, _file_identity(before)
    finally:
        os.close(descriptor)


def _check_identities(root: Path, root_fd: int, identities: dict[str, tuple[int, ...]]) -> None:
    _require_same_root(root, root_fd)
    for name, expected in identities.items():
        observed = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        if _file_identity(observed) != expected:
            raise ValueError("member file changed after read")


def _publish_bytes(root: Path, root_fd: int, name: str, data: bytes, limit: int) -> tuple[int, ...]:
    if not 0 < len(data) <= limit:
        raise ValueError("member file exceeds byte limit or is empty")
    temporary_name: str | None = f".factor-member-{secrets.token_hex(16)}.tmp"
    identity: tuple[int, int] | None = None
    try:
        _require_same_root(root, root_fd)
        fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=root_fd,
        )
        try:
            observed = os.fstat(fd)
            identity = observed.st_dev, observed.st_ino
            os.fchmod(fd, 0o600)
            _write_all(fd, data)
            os.fsync(fd)
        finally:
            os.close(fd)
        read_back, _ = _read_file(root_fd, temporary_name, limit, _sha(data))
        if read_back != data:
            raise ValueError("member temporary content differs")
        _require_same_root(root, root_fd)
        try:
            rename_noreplace_at(root_fd, temporary_name, root_fd, name)
        except FileExistsError:
            _cleanup_owned_temporary(root_fd, temporary_name, identity)
        temporary_name = None
        stored, stored_identity = _read_file(root_fd, name, limit, _sha(data))
        if stored != data:
            raise ValueError("existing member content differs")
        os.fsync(root_fd)
        _check_identities(root, root_fd, {name: stored_identity})
        return stored_identity
    finally:
        if temporary_name is not None and identity is not None:
            _cleanup_owned_temporary(root_fd, temporary_name, identity)


def _parse_day(data: bytes, *, stored: bool) -> FactorMemberDayInput:
    strict_canonical_json_loads(data)
    day = FactorMemberDayInput.model_validate_json(data)
    if stored and data != _bytes(day):
        raise ValueError("stored member day is not normalized canonical content")
    return day


def _universe(
    payload: FactorMemberDayInput,
    request: FactorMemberArchiveRequest,
    sources: FactorMemberSources | None = None,
) -> FactorUniverseRequest:
    if not set(payload.securities.complete_stock_codes) <= set(request.computation_stock_codes):
        raise ValueError("member securities outside computation scope")
    securities, membership = payload.securities, payload.membership
    if sources is not None:
        securities = DailySecurityBatch.model_validate(
            securities.model_copy(
                update={
                    "source_id": sources.security_source_id,
                    "source_sha256": sources.security_source_sha256,
                }
            )
        )
        if membership is not None:
            membership = DailyIndexConstituentBatch.model_validate(
                membership.model_copy(
                    update={
                        "source_id": sources.index_source_id,
                        "source_sha256": sources.index_source_sha256,
                    }
                )
            )
    universe = FactorUniverseRequest(
        selection=request.selection,
        trade_date=payload.trade_date,
        as_of=request.as_of,
        securities=securities,
        membership=membership,
    )
    select_factor_universe(universe)
    return universe


def _day_reference(
    payload: FactorMemberDayInput, request: FactorMemberArchiveRequest
) -> FactorMemberDayReference:
    selected = select_factor_universe(_universe(payload, request))
    data = _bytes(payload)
    sha = _sha(data)
    return FactorMemberDayReference(
        trade_date=payload.trade_date,
        sha256=sha,
        filename=f"{_DAY_PREFIX}{sha}.json",
        byte_count=len(data),
        security_payload_sha256=_sha(_bytes(payload.securities)),
        index_payload_sha256=None
        if payload.membership is None
        else _sha(_bytes(payload.membership)),
        security_count=len(payload.securities.facts),
        index_count=None if payload.membership is None else len(payload.membership.stock_codes),
        selected_count=selected.selected_count,
    )


def publish_factor_member_archive(
    request: FactorMemberArchiveRequest,
    *,
    input_root: Path,
    daily_filenames: Iterable[str],
    root: Path,
) -> FactorMemberArchiveReference:
    """Read every actual day, then publish a manifest only after complete import."""
    request = FactorMemberArchiveRequest.model_validate(request)
    input_root, root = _root_path(input_root), _root_path(root)
    input_fd = _open_private_root(input_root)
    root_fd: int | None = None
    iterator = None
    try:
        root_fd = _open_private_root(root)
        iterator = iter(daily_filenames)
        input_identities: dict[str, tuple[int, ...]] = {}
        stored_identities: dict[str, tuple[int, ...]] = {}
        days: list[FactorMemberDayReference] = []
        for expected_day in request.trading_days:
            try:
                name = _input_name(next(iterator))
            except StopIteration:
                raise ValueError("missing member input day") from None
            if name in input_identities:
                raise ValueError("duplicate member input filename")
            data, identity = _read_file(input_fd, name, MAX_FACTOR_MEMBER_DAY_BYTES)
            payload = _parse_day(data, stored=False)
            if payload.trade_date != expected_day:
                raise ValueError("member input date differs from schedule")
            day = _day_reference(payload, request)
            stored_identities[day.filename] = _publish_bytes(
                root, root_fd, day.filename, _bytes(payload), MAX_FACTOR_MEMBER_DAY_BYTES
            )
            input_identities[name] = identity
            days.append(day)
            del payload, data
        try:
            next(iterator)
        except StopIteration:
            pass
        else:
            raise ValueError("unexpected member input day")
        _check_identities(input_root, input_fd, input_identities)
        _check_identities(root, root_fd, stored_identities)
        manifest = FactorMemberArchiveManifest(
            schema_version=1,
            request=request,
            request_sha256=canonical_sha256(request),
            sources=_sources(tuple(days)),
            days=tuple(days),
        )
        data = _bytes(manifest)
        reference = FactorMemberArchiveReference(
            sha256=_sha(data), filename=f"{_ARCHIVE_PREFIX}{_sha(data)}.json", byte_count=len(data)
        )
        _publish_bytes(root, root_fd, reference.filename, data, MAX_FACTOR_MEMBER_MANIFEST_BYTES)
        return reference
    finally:
        try:
            close = getattr(iterator if iterator is not None else daily_filenames, "close", None)
            if close is not None:
                close()
        finally:
            if root_fd is not None:
                os.close(root_fd)
            os.close(input_fd)


def _load_manifest(
    root_fd: int, reference: FactorMemberArchiveReference
) -> tuple[FactorMemberArchiveManifest, tuple[int, ...]]:
    data, identity = _read_file(
        root_fd, reference.filename, MAX_FACTOR_MEMBER_MANIFEST_BYTES, reference.sha256
    )
    strict_canonical_json_loads(data)
    manifest = FactorMemberArchiveManifest.model_validate_json(data)
    if len(data) != reference.byte_count or data != _bytes(manifest):
        raise ValueError("member manifest reference or canonical content differs")
    return manifest, identity


def load_factor_member_archive(
    root: Path, reference: FactorMemberArchiveReference
) -> FactorMemberArchiveManifest:
    """Load bounded metadata; the daily reader verifies the referenced day files."""
    reference = FactorMemberArchiveReference.model_validate(reference)
    root = _root_path(root)
    descriptor = _open_private_root(root)
    try:
        manifest, identity = _load_manifest(descriptor, reference)
        _check_identities(root, descriptor, {reference.filename: identity})
        return manifest
    finally:
        os.close(descriptor)
