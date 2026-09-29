"""Single-file, content-addressed archives for retrospective factor research."""

from __future__ import annotations

import hashlib
import os
import re
import secrets
import stat
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.factor.historical_adapter import HistoricalFactorResearch
from rquant.factor.result import assemble_factor_research_result
from rquant.private_fs import rename_noreplace_at
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

MAX_FACTOR_RESEARCH_ARTIFACT_BYTES = 128 * 1024 * 1024
_PREFIX = "factor-research-v1-"
_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_SHA256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
_DIRECTORY_FLAGS = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_READ_FLAGS = os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC


def _digest(payload: object) -> str:
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


class FactorResearchArtifactV1(BaseModel):
    """A complete, reproducible research result bound to a code revision."""

    model_config = _IMMUTABLE

    schema_version: Literal[1]
    code_revision: str
    research: HistoricalFactorResearch
    content_sha256: _SHA256

    @field_validator("code_revision")
    @classmethod
    def _valid_revision(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{40}", value) is None:
            raise ValueError("code revision must be a 40-digit lowercase commit")
        return value

    @model_validator(mode="after")
    def _verified_content(self) -> FactorResearchArtifactV1:
        checked = HistoricalFactorResearch.model_validate(self.research)
        if assemble_factor_research_result(checked.request) != checked.result:
            raise ValueError("factor research result differs from recomputed request")
        if self.content_sha256 != _digest(self.model_dump(mode="json", exclude={"content_sha256"})):
            raise ValueError("factor research artifact content digest differs")
        return self


class FactorResearchArtifactReceipt(BaseModel):
    """The exact immutable file a later authority may reference."""

    model_config = _IMMUTABLE

    sha256: _SHA256
    filename: str
    byte_count: int = Field(gt=0, le=MAX_FACTOR_RESEARCH_ARTIFACT_BYTES, strict=True)

    @model_validator(mode="after")
    def _matching_name(self) -> FactorResearchArtifactReceipt:
        if self.filename != f"{_PREFIX}{self.sha256}.json":
            raise ValueError("factor artifact filename differs from its digest")
        return self


def _artifact_bytes(
    research: HistoricalFactorResearch, code_revision: str
) -> tuple[FactorResearchArtifactV1, bytes]:
    checked = HistoricalFactorResearch.model_validate(research)
    if assemble_factor_research_result(checked.request) != checked.result:
        raise ValueError("factor research result differs from recomputed request")
    unsigned = {
        "schema_version": 1,
        "code_revision": code_revision,
        "research": checked.model_dump(mode="json", round_trip=True),
    }
    artifact = FactorResearchArtifactV1(
        schema_version=1,
        code_revision=code_revision,
        research=checked,
        content_sha256=_digest(unsigned),
    )
    data = canonical_json_bytes(artifact.model_dump(mode="json", round_trip=True))
    if not 0 < len(data) <= MAX_FACTOR_RESEARCH_ARTIFACT_BYTES:
        raise ValueError("factor research artifact exceeds 128 MiB")
    return artifact, data


def _parse_artifact(data: bytes, expected_sha256: str) -> FactorResearchArtifactV1:
    if not 0 < len(data) <= MAX_FACTOR_RESEARCH_ARTIFACT_BYTES:
        raise ValueError("factor research artifact exceeds 128 MiB or is empty")
    strict_canonical_json_loads(data)
    # Nested strict Pydantic models need JSON coercion for dates and tuples;
    # the byte-for-byte canonical re-encoding below rejects changed forms.
    artifact = FactorResearchArtifactV1.model_validate_json(data, strict=False)
    if artifact.content_sha256 != expected_sha256 or data != canonical_json_bytes(
        artifact.model_dump(mode="json", round_trip=True)
    ):
        raise ValueError("factor research artifact has a wrong name or noncanonical content")
    return artifact


def _root_path(root: Path) -> Path:
    raw = os.fspath(root)
    if not isinstance(raw, str) or not raw.startswith("/") or os.path.normpath(raw) != raw:
        raise ValueError("factor artifact root must be a canonical absolute path")
    path = Path(raw)
    if ".." in path.parts:
        raise ValueError("factor artifact root must not traverse directories")
    return path


def _root_identity(observed: os.stat_result) -> tuple[int, int]:
    if (
        not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise ValueError("factor artifact root must be a private owned directory")
    return observed.st_dev, observed.st_ino


def _open_private_root(root: Path) -> int:
    descriptor = os.open("/", _DIRECTORY_FLAGS)
    try:
        for component in root.parts[1:]:
            next_descriptor = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
        _root_identity(os.fstat(descriptor))
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _require_same_root(root: Path, descriptor: int) -> None:
    expected = _root_identity(os.fstat(descriptor))
    reopened = _open_private_root(root)
    try:
        if _root_identity(os.fstat(reopened)) != expected:
            raise ValueError("factor artifact root changed during access")
    finally:
        os.close(reopened)


def _file_identity(observed: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
    )


def _require_named_regular(root_fd: int, name: str, file_fd: int) -> os.stat_result:
    opened = os.fstat(file_fd)
    named = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.getuid()
        or stat.S_IMODE(opened.st_mode) != 0o600
        or opened.st_nlink != 1
        or _file_identity(named) != _file_identity(opened)
    ):
        raise ValueError("factor artifact is not the owned single-link regular file")
    return opened


def _read_verified(
    root_fd: int, name: str, expected_sha256: str
) -> tuple[FactorResearchArtifactV1, bytes, int, os.stat_result]:
    descriptor = os.open(name, _READ_FLAGS, dir_fd=root_fd)
    try:
        before = _require_named_regular(root_fd, name, descriptor)
        if not 0 < before.st_size <= MAX_FACTOR_RESEARCH_ARTIFACT_BYTES:
            raise ValueError("factor artifact file exceeds 128 MiB or is empty")
        data = bytearray()
        while len(data) <= MAX_FACTOR_RESEARCH_ARTIFACT_BYTES:
            chunk = os.read(
                descriptor, min(1024 * 1024, MAX_FACTOR_RESEARCH_ARTIFACT_BYTES + 1 - len(data))
            )
            if not chunk:
                break
            data.extend(chunk)
        if len(data) != before.st_size or len(data) > MAX_FACTOR_RESEARCH_ARTIFACT_BYTES:
            raise ValueError("factor artifact changed during read")
        artifact = _parse_artifact(bytes(data), expected_sha256)
        after = _require_named_regular(root_fd, name, descriptor)
        if _file_identity(after) != _file_identity(before):
            raise ValueError("factor artifact changed during read")
        return artifact, bytes(data), descriptor, after
    except BaseException:
        os.close(descriptor)
        raise


def _write_all(descriptor: int, data: bytes) -> None:
    written = 0
    while written < len(data):
        count = os.write(descriptor, data[written:])
        if count <= 0:
            raise OSError("factor artifact write made no progress")
        written += count


def _cleanup_owned_temporary(root_fd: int, name: str | None, identity: tuple[int, int]) -> None:
    if name is None:
        return
    try:
        observed = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    if stat.S_ISREG(observed.st_mode) and (observed.st_dev, observed.st_ino) == identity:
        os.unlink(name, dir_fd=root_fd)


def publish_factor_research_artifact(
    research: HistoricalFactorResearch, code_revision: str, root: Path
) -> FactorResearchArtifactReceipt:
    """Publish one verified file without replacing a prior content identity."""
    artifact, data = _artifact_bytes(research, code_revision)
    root = _root_path(root)
    name = f"{_PREFIX}{artifact.content_sha256}.json"
    root_fd = _open_private_root(root)
    temporary_name: str | None = None
    temporary_identity: tuple[int, int] | None = None
    try:
        _require_same_root(root, root_fd)
        temporary_name = f".factor-research-{secrets.token_hex(16)}.tmp"
        temporary_fd = os.open(
            temporary_name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
            dir_fd=root_fd,
        )
        try:
            temporary = os.fstat(temporary_fd)
            temporary_identity = temporary.st_dev, temporary.st_ino
            os.fchmod(temporary_fd, 0o600)
            temporary = os.fstat(temporary_fd)
            if not stat.S_ISREG(temporary.st_mode) or temporary.st_nlink != 1:
                raise ValueError("factor artifact temporary is not a single-link file")
            _write_all(temporary_fd, data)
            os.fsync(temporary_fd)
        finally:
            os.close(temporary_fd)
        _, read_back, temporary_read_fd, _ = _read_verified(
            root_fd, temporary_name, artifact.content_sha256
        )
        try:
            if read_back != data:
                raise ValueError("factor artifact temporary differs from request")
        finally:
            os.close(temporary_read_fd)
        _require_same_root(root, root_fd)
        try:
            rename_noreplace_at(root_fd, temporary_name, root_fd, name)
        except FileExistsError:
            _cleanup_owned_temporary(root_fd, temporary_name, temporary_identity)
            temporary_name = None
        else:
            temporary_name = None
        stored, stored_bytes, final_fd, verified_stat = _read_verified(
            root_fd, name, artifact.content_sha256
        )
        try:
            if stored != artifact or stored_bytes != data:
                raise ValueError("existing factor artifact differs from requested content")
            os.fsync(final_fd)
            current = _require_named_regular(root_fd, name, final_fd)
            if _file_identity(current) != _file_identity(verified_stat):
                raise ValueError("factor artifact changed before directory sync")
            os.fsync(root_fd)
            if _file_identity(_require_named_regular(root_fd, name, final_fd)) != _file_identity(
                verified_stat
            ):
                raise ValueError("factor artifact changed after directory sync")
            _require_same_root(root, root_fd)
        finally:
            os.close(final_fd)
        return FactorResearchArtifactReceipt(
            sha256=artifact.content_sha256, filename=name, byte_count=len(data)
        )
    finally:
        if temporary_name is not None and temporary_identity is not None:
            _cleanup_owned_temporary(root_fd, temporary_name, temporary_identity)
        os.close(root_fd)


def load_factor_research_artifact(root: Path, sha256: str) -> FactorResearchArtifactV1:
    """Read only a digest-derived direct child of a verified physical root."""
    if re.fullmatch(r"[0-9a-f]{64}", sha256) is None:
        raise ValueError("factor artifact identity must be a lowercase SHA-256")
    root = _root_path(root)
    root_fd = _open_private_root(root)
    try:
        artifact, _, descriptor, verified_stat = _read_verified(
            root_fd, f"{_PREFIX}{sha256}.json", sha256
        )
        try:
            _require_same_root(root, root_fd)
            current = _require_named_regular(root_fd, f"{_PREFIX}{sha256}.json", descriptor)
            if _file_identity(current) != _file_identity(verified_stat):
                raise ValueError("factor artifact changed after read")
            return artifact
        finally:
            os.close(descriptor)
    finally:
        os.close(root_fd)
