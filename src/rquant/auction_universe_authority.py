"""Immutable point-in-time universe used to qualify opening-auction coverage."""

from __future__ import annotations

import os
import re
import stat
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, ValidationError, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.strict_json import (
    StrictJsonError,
    canonical_json_bytes,
    strict_model_validate_canonical_json,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_TS_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_MAX_AUTHORITY_BYTES = 2 * 1024 * 1024
_READ_CHUNK_BYTES = 256 * 1024


class AuctionUniverseAuthorityIntegrityError(RuntimeError):
    """The frozen auction universe cannot be trusted at the requested time."""


class AuctionUniverseAuthority(RuntimeContractModel):
    schema_version: Literal[1] = 1
    effective_trade_date: date
    reference_trade_date: date
    available_at: AwareUtcDatetime
    producer_commit: CommitSha
    source_snapshot_id: Sha256
    codes: tuple[str, ...] = Field(min_length=1)
    content_sha256: Sha256

    @field_validator("codes")
    @classmethod
    def canonicalize_codes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(
            not isinstance(code, str) or _TS_CODE_PATTERN.fullmatch(code) is None for code in value
        ):
            raise ValueError("codes contain an invalid ts_code")
        return tuple(sorted(set(value)))

    @model_validator(mode="after")
    def validate_authority(self) -> Self:
        if self.reference_trade_date > self.effective_trade_date:
            raise ValueError("reference_trade_date cannot follow effective_trade_date")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("content_sha256 does not bind canonical auction universe")
        return self

    @classmethod
    def create(
        cls,
        *,
        effective_trade_date: date,
        reference_trade_date: date,
        available_at: datetime,
        producer_commit: str,
        source_snapshot_id: str,
        codes: tuple[str, ...],
    ) -> AuctionUniverseAuthority:
        normalized_codes = tuple(sorted(set(codes)))
        identity = {
            "schema_version": 1,
            "effective_trade_date": effective_trade_date,
            "reference_trade_date": reference_trade_date,
            "available_at": normalize_aware_utc(available_at),
            "producer_commit": producer_commit,
            "source_snapshot_id": source_snapshot_id,
            "codes": normalized_codes,
        }
        return cls(**identity, content_sha256=canonical_sha256(identity))

    def canonical_json_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))


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


def _normalized_absolute_path(path: Path) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.normpath(os.fspath(candidate)))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError("auction universe path must be absolute normalized")
    return candidate


def _open_parent(path: Path) -> int:
    descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:]:
            before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode):
                raise AuctionUniverseAuthorityIntegrityError(
                    "auction universe path contains an unsafe parent"
                )
            child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            opened = os.fstat(child)
            if _identity(before) != _identity(opened):
                os.close(child)
                raise AuctionUniverseAuthorityIntegrityError(
                    "auction universe parent changed while opening"
                )
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_private_authority(path: Path) -> bytes:
    parent = -1
    descriptor = -1
    try:
        parent = _open_parent(path.parent)
        before = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode):
            raise AuctionUniverseAuthorityIntegrityError("auction universe authority is a symlink")
        if not stat.S_ISREG(before.st_mode):
            raise AuctionUniverseAuthorityIntegrityError(
                "auction universe authority is not a regular file"
            )
        if before.st_uid != os.geteuid():
            raise AuctionUniverseAuthorityIntegrityError(
                "auction universe authority owner does not match the process"
            )
        if stat.S_IMODE(before.st_mode) != 0o600:
            raise AuctionUniverseAuthorityIntegrityError(
                "auction universe authority must have mode 0600"
            )
        if before.st_nlink != 1:
            raise AuctionUniverseAuthorityIntegrityError(
                "auction universe authority must have one hard link"
            )
        if before.st_size <= 0 or before.st_size > _MAX_AUTHORITY_BYTES:
            raise AuctionUniverseAuthorityIntegrityError(
                "auction universe authority has an unsafe size"
            )
        descriptor = os.open(path.name, _FILE_FLAGS, dir_fd=parent)
        opened = os.fstat(descriptor)
        active = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        if _identity(before) != _identity(opened) or _identity(active) != _identity(opened):
            raise AuctionUniverseAuthorityIntegrityError(
                "auction universe authority changed while opening"
            )
        chunks: list[bytes] = []
        total = 0
        while chunk := os.read(descriptor, _READ_CHUNK_BYTES):
            total += len(chunk)
            if total > _MAX_AUTHORITY_BYTES:
                raise AuctionUniverseAuthorityIntegrityError(
                    "auction universe authority exceeds its size limit"
                )
            chunks.append(chunk)
        after = os.fstat(descriptor)
        current = os.stat(path.name, dir_fd=parent, follow_symlinks=False)
        if _identity(after) != _identity(opened) or _identity(current) != _identity(after):
            raise AuctionUniverseAuthorityIntegrityError(
                "auction universe authority changed while reading"
            )
        return b"".join(chunks)
    except AuctionUniverseAuthorityIntegrityError:
        raise
    except OSError as exc:
        raise AuctionUniverseAuthorityIntegrityError(
            "auction universe authority is unavailable or unsafe"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent >= 0:
            os.close(parent)


def load_auction_universe_authority(
    path: Path,
    *,
    expected_commit: str,
    required_trade_date: date,
    as_of: datetime,
) -> AuctionUniverseAuthority:
    candidate = _normalized_absolute_path(path)
    if _COMMIT_PATTERN.fullmatch(expected_commit) is None:
        raise ValueError("expected_commit must be a full lowercase Git SHA")
    observed_at = normalize_aware_utc(as_of)
    try:
        authority = strict_model_validate_canonical_json(
            AuctionUniverseAuthority,
            _read_private_authority(candidate),
        )
    except (StrictJsonError, ValidationError, TypeError, ValueError) as exc:
        raise AuctionUniverseAuthorityIntegrityError(
            f"invalid or non-canonical auction universe authority: {exc}"
        ) from exc
    if authority.producer_commit != expected_commit:
        raise AuctionUniverseAuthorityIntegrityError(
            "auction universe producer_commit does not match expected_commit"
        )
    if authority.effective_trade_date != required_trade_date:
        raise AuctionUniverseAuthorityIntegrityError(
            "auction universe effective trade date does not match"
        )
    if authority.available_at > observed_at:
        raise AuctionUniverseAuthorityIntegrityError(
            "auction universe authority is not visible at as_of"
        )
    return authority


__all__ = [
    "AuctionUniverseAuthority",
    "AuctionUniverseAuthorityIntegrityError",
    "load_auction_universe_authority",
]
