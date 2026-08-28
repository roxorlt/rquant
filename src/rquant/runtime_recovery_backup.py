"""Consistent producer for signed, real-artifact recovery generations."""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import os
import shutil
import sqlite3
import stat
import threading
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import contextmanager, suppress
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Annotated, Literal, Protocol, Self

import duckdb
from pydantic import (
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.paper_broker import (
    _LEDGER_ATTESTATION_COUNT_COLUMNS,
    PaperBrokerReconciliationError,
    PaperBrokerStore,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_production_profile import ProductionStrategyBinding
from rquant.runtime_recovery_artifacts import (
    RealRecoveryArtifactKind,
    RealRecoveryArtifactSpec,
    RealRecoveryTargetManifest,
    RecoveryPayloadSigner,
    RecoveryPayloadVerifier,
    RecoveryToolVerifierBundle,
    RecoveryVerificationBudget,
    build_real_recovery_target,
    seal_recovery_tool_bundle,
    validate_complete_recovery_artifact_graph,
)
from rquant.runtime_recovery_coordinator import (
    RuntimeRecoveryFixedReplayExpectation,
    RuntimeRecoveryFixedReplayVerifier,
    build_runtime_recovery_fixed_replay_expectations,
)
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]

_CHUNK_SIZE = 1024 * 1024
_MAX_CONTROL_BYTES = 16 * 1024 * 1024
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_IMMUTABLE_DIRECTORY_MODE = 0o500
_IMMUTABLE_FILE_MODE = 0o400
_DUCKDB_MUTABLE_KINDS = frozenset(
    {
        RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
        RealRecoveryArtifactKind.RESEARCH_CATALOG,
    }
)
_DUCKDB_KINDS = frozenset(
    {
        *_DUCKDB_MUTABLE_KINDS,
        RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
        RealRecoveryArtifactKind.SERVING_DATABASE,
    }
)
_SQLITE_KINDS = frozenset(
    {
        RealRecoveryArtifactKind.STATE_SQLITE,
        RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
    }
)


class RecoveryBackupIntegrityError(RuntimeError):
    """A recovery backup source, ledger head, or publication failed closed."""


class RecoveryBackupSigner(RecoveryPayloadSigner, Protocol):
    """Signer capability supplied by the production credential boundary."""


class RecoveryBackupAuthenticator:
    """HMAC signer/verifier loaded from one private service credential file."""

    def __init__(self, *, key_id: str, secret: bytes) -> None:
        if (
            not key_id
            or len(key_id) > 128
            or any(
                character not in "abcdefghijklmnopqrstuvwxyz0123456789-_." for character in key_id
            )
        ):
            raise ValueError("recovery credential key_id is invalid")
        if len(secret) < 32:
            raise ValueError("recovery credential secret must contain at least 32 bytes")
        self.key_id = key_id
        self._secret = bytes(secret)

    @classmethod
    def from_file(cls, path: Path) -> RecoveryBackupAuthenticator:
        credential = Path(_canonical_absolute(path, label="recovery credential"))
        descriptor = -1
        try:
            descriptor = os.open(
                credential,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            observed = os.fstat(descriptor)
            named = os.lstat(credential)
            if (
                not stat.S_ISREG(observed.st_mode)
                or observed.st_uid != os.geteuid()
                or observed.st_nlink != 1
                or stat.S_IMODE(observed.st_mode) & 0o077
                or observed.st_size > _MAX_CONTROL_BYTES
                or (observed.st_dev, observed.st_ino) != (named.st_dev, named.st_ino)
            ):
                raise RecoveryBackupIntegrityError("recovery credential file is unsafe")
            chunks: list[bytes] = []
            size = 0
            while chunk := os.read(descriptor, _CHUNK_SIZE):
                size += len(chunk)
                if size > _MAX_CONTROL_BYTES:
                    raise RecoveryBackupIntegrityError("recovery credential file is oversized")
                chunks.append(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
            if (
                observed.st_dev,
                observed.st_ino,
                observed.st_size,
                observed.st_mtime_ns,
                observed.st_ctime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ):
                raise RecoveryBackupIntegrityError("recovery credential changed while reading")
            decoded = strict_canonical_json_loads(payload)
            if not isinstance(decoded, dict) or set(decoded) != {"key_id", "secret_hex"}:
                raise ValueError("recovery credential fields are invalid")
            key_id = decoded["key_id"]
            secret_hex = decoded["secret_hex"]
            if not isinstance(key_id, str) or not isinstance(secret_hex, str):
                raise ValueError("recovery credential values are invalid")
            if canonical_json_bytes(decoded) != payload:
                raise ValueError("recovery credential is not canonical")
            return cls(key_id=key_id, secret=bytes.fromhex(secret_hex))
        except RecoveryBackupIntegrityError:
            raise
        except (OSError, ValueError, TypeError) as exc:
            raise RecoveryBackupIntegrityError("recovery credential is invalid") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def sign(self, payload: bytes) -> str:
        return hmac.new(self._secret, payload, hashlib.sha256).hexdigest()

    def verify(self, payload: bytes, signature: str) -> bool:
        return hmac.compare_digest(self.sign(payload), signature)


def load_recovery_backup_trusted_verifiers(
    credential_files: tuple[Path, ...],
) -> Mapping[str, RecoveryBackupAuthenticator]:
    """Load a rotation set from private credential files outside the artifact tree."""

    if not credential_files:
        raise RecoveryBackupIntegrityError("recovery trusted credential set is empty")
    authenticators: dict[str, RecoveryBackupAuthenticator] = {}
    for path in credential_files:
        authenticator = RecoveryBackupAuthenticator.from_file(path)
        if authenticator.key_id in authenticators:
            raise RecoveryBackupIntegrityError("recovery trusted credential key_id is duplicated")
        authenticators[authenticator.key_id] = authenticator
    return MappingProxyType(authenticators)


def _environment_trusted_verifiers() -> Mapping[str, RecoveryBackupAuthenticator]:
    configured = os.pathsep.join(
        value
        for value in (
            os.environ.get("RQUANT_RECOVERY_CREDENTIAL_FILE", ""),
            os.environ.get("RQUANT_RECOVERY_TRUSTED_CREDENTIAL_FILES", ""),
        )
        if value
    )
    paths = tuple(Path(item) for item in configured.split(os.pathsep) if item)
    return load_recovery_backup_trusted_verifiers(paths)


def recovery_backup_trusted_verifiers_for_active(
    active: RecoveryBackupAuthenticator,
) -> Mapping[str, RecoveryPayloadVerifier]:
    trusted: dict[str, RecoveryPayloadVerifier] = {active.key_id: active}
    configured = os.environ.get("RQUANT_RECOVERY_TRUSTED_CREDENTIAL_FILES", "")
    if configured:
        rotated = load_recovery_backup_trusted_verifiers(
            tuple(Path(item) for item in configured.split(os.pathsep) if item)
        )
        for key_id, verifier in rotated.items():
            existing = trusted.get(key_id)
            if existing is not None:
                challenge = b"rquant-recovery-key-rotation-equivalence/v1"
                if not (
                    existing.verify(challenge, verifier.sign(challenge))
                    and verifier.verify(challenge, active.sign(challenge))
                ):
                    raise RecoveryBackupIntegrityError(
                        "active and trusted recovery key_id conflict"
                    )
                continue
            trusted[key_id] = verifier
    return MappingProxyType(trusted)


def _canonical_absolute(value: str | Path, *, label: str) -> str:
    path = Path(value)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise ValueError(f"{label} must be an absolute canonical path")
    return str(path)


def _safe_relative(value: str) -> str:
    if "\\" in value:
        raise ValueError("relative path must use POSIX separators")
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or str(path) != value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ValueError("relative path is unsafe")
    return value


class PaperLedgerExternalHead(RuntimeContractModel):
    schema_version: Literal[1] = 1
    head_id: Sha256 | None = None
    profile_generation: Sha256
    source_artifact_role: str = Field(min_length=1, max_length=256)
    ledger_generation: str = Field(min_length=1, max_length=256)
    revision: int = Field(ge=1)
    attestation_fingerprint: Sha256
    head_marker_fingerprint: Sha256
    migration_attestation_fingerprint: Sha256
    counts: Mapping[str, int]
    persisted_at: AwareUtcDatetime
    observed_at: AwareUtcDatetime

    @field_validator("counts")
    @classmethod
    def canonicalize_counts(cls, value: Mapping[str, int]) -> Mapping[str, int]:
        expected = tuple(sorted(_LEDGER_ATTESTATION_COUNT_COLUMNS))
        if tuple(sorted(value)) != expected or any(
            type(count) is not int or count < 0 for count in value.values()
        ):
            raise ValueError("paper ledger external head counts are incomplete")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("counts")
    def serialize_counts(self, value: Mapping[str, int]) -> dict[str, int]:
        return dict(value)

    def model_post_init(self, _context: object) -> None:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"head_id"}))
        if self.head_id is not None and self.head_id != expected:
            raise ValueError("paper ledger external head identity differs")
        object.__setattr__(self, "head_id", expected)


class RecoveryFixedReplayExpectationsDocument(RuntimeContractModel):
    schema_version: Literal[1] = 1
    expectations_sha256: Sha256 | None = None
    expectations: tuple[RuntimeRecoveryFixedReplayExpectation, ...] = Field(
        min_length=3,
        max_length=3,
    )

    @field_validator("expectations")
    @classmethod
    def canonicalize_expectations(
        cls,
        value: tuple[RuntimeRecoveryFixedReplayExpectation, ...],
    ) -> tuple[RuntimeRecoveryFixedReplayExpectation, ...]:
        ordered = tuple(sorted(value, key=lambda item: item.strategy_id))
        if {item.strategy_id for item in ordered} != {
            "n_shape",
            "growth_board_surge",
            "auction_gap",
        }:
            raise ValueError("recovery expectations must cover all production strategies")
        return ordered

    def model_post_init(self, _context: object) -> None:
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"expectations_sha256"}))
        if self.expectations_sha256 is not None and self.expectations_sha256 != expected:
            raise ValueError("recovery expectations identity differs")
        object.__setattr__(self, "expectations_sha256", expected)


class RecoveryBackupConfig(RuntimeContractModel):
    schema_version: Literal[1] = 1
    config_id: Sha256 | None = None
    source_root: str
    publication_root: str
    target_commit: CommitSha
    target_profile_generation: Sha256
    verifier_commit: CommitSha
    signer_key_id: str = Field(min_length=1, max_length=128)
    as_of: AwareUtcDatetime
    replay_start_date: date
    replay_end_date: date
    production_artifact_role: str = Field(min_length=1, max_length=256)
    paper_ledger_artifact_role: str = Field(min_length=1, max_length=256)
    strategy_bindings: tuple[ProductionStrategyBinding, ...] = Field(min_length=3, max_length=3)
    artifacts: tuple[RealRecoveryArtifactSpec, ...] = Field(min_length=1, max_length=4096)
    deadline_seconds: int = Field(default=3600, ge=1, le=24 * 60 * 60)
    max_total_bytes: int = Field(default=256 * 1024**3, ge=1)

    @field_validator("source_root", "publication_root", mode="before")
    @classmethod
    def validate_absolute_paths(cls, value: str | Path) -> str:
        return _canonical_absolute(value, label="recovery backup root")

    def model_post_init(self, _context: object) -> None:
        if self.replay_start_date > self.replay_end_date:
            raise ValueError("recovery backup replay dates are reversed")
        if (
            Path(self.source_root) == Path(self.publication_root)
            or Path(self.source_root).is_relative_to(Path(self.publication_root))
            or Path(self.publication_root).is_relative_to(Path(self.source_root))
        ):
            raise ValueError("recovery source and publication roots must be isolated")
        validate_complete_recovery_artifact_graph(
            self.artifacts,
            production_artifact_role=self.production_artifact_role,
            paper_ledger_artifact_role=self.paper_ledger_artifact_role,
        )
        if {item.strategy_id for item in self.strategy_bindings} != {
            "n_shape",
            "growth_board_surge",
            "auction_gap",
        }:
            raise ValueError("recovery backup requires all production strategy bindings")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"config_id"}))
        if self.config_id is not None and self.config_id != expected:
            raise ValueError("recovery backup config identity differs")
        object.__setattr__(self, "config_id", expected)


class RecoveryBackupPreview(RuntimeContractModel):
    plan_id: Sha256
    config_id: Sha256
    target_profile_generation: Sha256
    signer_key_id: str = Field(min_length=1, max_length=128)
    artifact_count: int = Field(ge=1)
    total_source_bytes: int = Field(ge=0)
    current_generation_id: Sha256 | None = None


class RecoveryBackupCurrentPointer(RuntimeContractModel):
    schema_version: Literal[1] = 1
    generation_id: Sha256
    generation_path: str
    manifest_id: Sha256
    receipt_id: Sha256
    profile_generation: Sha256
    previous_generation_id: Sha256 | None = None
    published_at: AwareUtcDatetime
    key_id: str = Field(min_length=1, max_length=128)
    signature: str = Field(min_length=1, max_length=128 * 1024)

    @field_validator("generation_path")
    @classmethod
    def validate_generation_path(cls, value: str) -> str:
        return _safe_relative(value)

    @model_validator(mode="after")
    def validate_pointer(self) -> Self:
        if self.previous_generation_id == self.generation_id:
            raise ValueError("previous recovery backup generation must differ")
        return self

    def signing_payload(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class RecoveryBackupReceipt(RuntimeContractModel):
    schema_version: Literal[1] = 1
    receipt_id: Sha256 | None = None
    status: Literal["succeeded"] = "succeeded"
    manifest_id: Sha256
    tool_bundle_id: Sha256
    expectations_sha256: Sha256
    target_commit: CommitSha
    target_profile_generation: Sha256
    generation_path: str
    paper_ledger_head: PaperLedgerExternalHead
    artifact_count: int = Field(ge=1)
    total_bytes: int = Field(ge=0)
    duration_ms: int = Field(ge=0)
    completed_at: AwareUtcDatetime
    key_id: str = Field(min_length=1, max_length=128)
    signature: str = Field(min_length=1, max_length=128 * 1024)

    @field_validator("generation_path")
    @classmethod
    def validate_generation_path(cls, value: str) -> str:
        return _safe_relative(value)

    def model_post_init(self, _context: object) -> None:
        if self.paper_ledger_head.profile_generation != self.target_profile_generation:
            raise ValueError("backup receipt paper head profile differs")
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"receipt_id", "signature"})
        )
        if self.receipt_id is not None and self.receipt_id != expected:
            raise ValueError("recovery backup receipt identity differs")
        object.__setattr__(self, "receipt_id", expected)

    def signing_payload(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


class RecoveryBackupPublicationIntent(RuntimeContractModel):
    schema_version: Literal[1] = 1
    intent_id: Sha256 | None = None
    operation_id: str = Field(min_length=32, max_length=64)
    generation_id: Sha256
    generation_path: str
    receipt_id: Sha256
    paper_ledger_head_id: Sha256
    previous_pointer: RecoveryBackupCurrentPointer | None = None
    created_at: AwareUtcDatetime
    key_id: str = Field(min_length=1, max_length=128)
    signature: str = Field(min_length=1, max_length=128 * 1024)

    @field_validator("generation_path")
    @classmethod
    def validate_generation_path(cls, value: str) -> str:
        return _safe_relative(value)

    def model_post_init(self, _context: object) -> None:
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"intent_id", "signature"})
        )
        if self.intent_id is not None and self.intent_id != expected:
            raise ValueError("recovery backup publication intent identity differs")
        object.__setattr__(self, "intent_id", expected)

    def signing_payload(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))


def _verify_signed_backup_contract(
    contract: (
        RecoveryBackupReceipt | RecoveryBackupCurrentPointer | RecoveryBackupPublicationIntent
    ),
    *,
    trusted_verifiers: Mapping[str, RecoveryPayloadVerifier],
) -> None:
    verifier = trusted_verifiers.get(contract.key_id)
    if (
        verifier is None
        or verifier.key_id != contract.key_id
        or not verifier.verify(contract.signing_payload(), contract.signature)
    ):
        raise RecoveryBackupIntegrityError("recovery backup contract signature key is not trusted")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, payload: bytes, *, mode: int = _PRIVATE_FILE_MODE) -> None:
    path.parent.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(
        temporary,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        mode,
    )
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    os.chmod(path, mode)
    _fsync_directory(path.parent)


def _read_contract(path: Path, model: type[RuntimeContractModel]) -> RuntimeContractModel:
    try:
        parent_before = os.lstat(path.parent)
        observed = os.lstat(path)
        if (
            stat.S_ISLNK(parent_before.st_mode)
            or not stat.S_ISDIR(parent_before.st_mode)
            or parent_before.st_uid != os.geteuid()
            or not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != os.geteuid()
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) & 0o022
            or observed.st_size > _MAX_CONTROL_BYTES
        ):
            raise RecoveryBackupIntegrityError("recovery backup control file is unsafe")
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            payload = b""
            while chunk := os.read(descriptor, _CHUNK_SIZE):
                payload += chunk
                if len(payload) > _MAX_CONTROL_BYTES:
                    raise RecoveryBackupIntegrityError("recovery backup control file is oversized")
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        named_after = os.lstat(path)
        parent_after = os.lstat(path.parent)
        if (
            _source_identity(after) != _source_identity(observed)
            or _source_identity(named_after) != _source_identity(observed)
            or _source_identity(parent_after)[:5] != _source_identity(parent_before)[:5]
        ):
            raise RecoveryBackupIntegrityError("recovery backup control file changed")
        decoded = strict_canonical_json_loads(payload)
        contract = model.model_validate(decoded)
        if canonical_json_bytes(contract.model_dump(mode="json")) != payload:
            raise RecoveryBackupIntegrityError("recovery backup control file is not canonical")
        return contract
    except (OSError, ValueError) as exc:
        raise RecoveryBackupIntegrityError("recovery backup control file is invalid") from exc


def _safe_source(root: Path, relative: str) -> Path:
    path = root.joinpath(*PurePosixPath(relative).parts)
    current = root
    root_stat = os.lstat(root)
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise RecoveryBackupIntegrityError("recovery backup source root is unsafe")
    for part in PurePosixPath(relative).parts[:-1]:
        current /= part
        observed = os.lstat(current)
        if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
            raise RecoveryBackupIntegrityError("recovery backup source ancestor is unsafe")
    observed = os.lstat(path)
    if (
        not stat.S_ISREG(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or observed.st_nlink != 1
    ):
        raise RecoveryBackupIntegrityError("recovery backup source file is unsafe")
    return path


def _source_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


class _RecoverySourceLease:
    def __init__(self, *, root: Path, artifact: RealRecoveryArtifactSpec) -> None:
        self.artifact = artifact
        self.path = _safe_source(root, artifact.source_path)
        self.parent_identity = _source_identity(os.lstat(self.path.parent))[:5]
        self.descriptor = -1
        try:
            self.descriptor = os.open(
                self.path,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            self.opened = os.fstat(self.descriptor)
            if (
                not stat.S_ISREG(self.opened.st_mode)
                or self.opened.st_uid != os.geteuid()
                or self.opened.st_nlink != 1
                or stat.S_IMODE(self.opened.st_mode) & 0o022
            ):
                raise RecoveryBackupIntegrityError("recovery backup source identity is unsafe")
            self.verify_named(allow_content_change=False)
        except BaseException:
            self.close()
            raise

    def verify_named(self, *, allow_content_change: bool) -> os.stat_result:
        opened = os.fstat(self.descriptor)
        named = os.lstat(self.path)
        parent = os.lstat(self.path.parent)
        if (
            (opened.st_dev, opened.st_ino) != (named.st_dev, named.st_ino)
            or opened.st_uid != named.st_uid
            or opened.st_gid != named.st_gid
            or opened.st_mode != named.st_mode
            or opened.st_nlink != 1
            or _source_identity(parent)[:5] != self.parent_identity
            or (
                not allow_content_change
                and _source_identity(opened) != _source_identity(self.opened)
            )
        ):
            raise RecoveryBackupIntegrityError(
                f"recovery backup source identity/path swap detected: {self.artifact.logical_role}"
            )
        return opened

    def content_sha256(
        self,
        *,
        max_bytes: int,
        check: Callable[[], None] | None = None,
    ) -> tuple[int, str]:
        self.verify_named(allow_content_change=False)
        os.lseek(self.descriptor, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        size = 0
        while True:
            if check is not None:
                check()
            chunk = os.read(self.descriptor, _CHUNK_SIZE)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                raise RecoveryBackupIntegrityError("recovery backup source exceeds byte budget")
            digest.update(chunk)
        self.verify_named(allow_content_change=False)
        return size, digest.hexdigest()

    def close(self) -> None:
        descriptor = getattr(self, "descriptor", -1)
        if descriptor >= 0:
            os.close(descriptor)
            self.descriptor = -1


def _copy_regular(
    source: Path,
    destination: Path,
    *,
    max_bytes: int,
    check: Callable[[], None] | None = None,
) -> int:
    source_descriptor = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    destination.parent.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
    destination_descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        _PRIVATE_FILE_MODE,
    )
    size = 0
    try:
        before = os.fstat(source_descriptor)
        while True:
            if check is not None:
                check()
            chunk = os.read(source_descriptor, _CHUNK_SIZE)
            if not chunk:
                break
            size += len(chunk)
            if size > max_bytes:
                raise RecoveryBackupIntegrityError("recovery backup exceeds byte budget")
            view = memoryview(chunk)
            while view:
                written = os.write(destination_descriptor, view)
                view = view[written:]
        os.fsync(destination_descriptor)
        after = os.fstat(source_descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise RecoveryBackupIntegrityError("recovery backup source changed while copying")
    finally:
        os.close(destination_descriptor)
        os.close(source_descriptor)
    _fsync_directory(destination.parent)
    return size


def _snapshot_duckdb(
    source: Path,
    destination: Path,
    *,
    kind: RealRecoveryArtifactKind,
    max_bytes: int,
    check: Callable[[], None] | None = None,
    remaining_seconds: Callable[[], float] | None = None,
) -> int:
    source_wal = source.with_name(f"{source.name}.wal")
    del kind
    if source_wal.exists():
        raise RecoveryBackupIntegrityError("DuckDB source has an unsealed WAL")
    size = _copy_regular(source, destination, max_bytes=max_bytes, check=check)
    if source_wal.exists():
        raise RecoveryBackupIntegrityError("DuckDB WAL appeared during snapshot")
    destination_connection = duckdb.connect(str(destination))
    interrupted = threading.Event()
    timer: threading.Timer | None = None
    try:
        if remaining_seconds is not None:
            timer = threading.Timer(
                max(0.001, remaining_seconds()),
                lambda: (interrupted.set(), destination_connection.interrupt()),
            )
            timer.daemon = True
            timer.start()
        destination_connection.execute("CHECKPOINT")
    except duckdb.Error as exc:
        if interrupted.is_set():
            raise RecoveryBackupIntegrityError(
                "DuckDB snapshot checkpoint deadline exceeded"
            ) from exc
        raise RecoveryBackupIntegrityError("DuckDB snapshot checkpoint failed") from exc
    finally:
        if timer is not None:
            timer.cancel()
        destination_connection.close()
    if check is not None:
        check()
    if interrupted.is_set():
        raise RecoveryBackupIntegrityError("DuckDB snapshot checkpoint deadline exceeded")
    if destination.with_name(f"{destination.name}.wal").exists():
        raise RecoveryBackupIntegrityError("recovery DuckDB snapshot retained a WAL")
    return size


def _snapshot_sqlite(
    source: Path,
    destination: Path,
    *,
    check: Callable[[], None] | None = None,
) -> int:
    destination.parent.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
    source_connection = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    destination_connection = sqlite3.connect(destination)
    interrupted: list[BaseException] = []

    def progress(_status: int, _remaining: int, _total: int) -> None:
        if check is not None:
            check()

    def integrity_progress() -> int:
        if check is None:
            return 0
        try:
            check()
        except BaseException as exc:
            interrupted.append(exc)
            return 1
        return 0

    try:
        if check is not None:
            check()
        source_connection.backup(destination_connection, pages=256, progress=progress)
        destination_connection.commit()
        destination_connection.set_progress_handler(integrity_progress, 1000)
        if str(destination_connection.execute("PRAGMA integrity_check").fetchone()[0]) != "ok":
            raise RecoveryBackupIntegrityError("SQLite backup integrity check failed")
        if check is not None:
            check()
    except sqlite3.Error as exc:
        if interrupted:
            raise interrupted[0] from exc
        raise RecoveryBackupIntegrityError("SQLite backup API failed") from exc
    finally:
        destination_connection.set_progress_handler(None, 0)
        destination_connection.close()
        source_connection.close()
    for suffix in ("-wal", "-shm"):
        if destination.with_name(f"{destination.name}{suffix}").exists():
            raise RecoveryBackupIntegrityError("SQLite backup retained a sidecar")
    os.chmod(destination, _PRIVATE_FILE_MODE)
    with destination.open("rb") as stream:
        os.fsync(stream.fileno())
    _fsync_directory(destination.parent)
    return destination.stat().st_size


def _paper_head(
    path: Path,
    *,
    profile_generation: str,
    source_artifact_role: str,
    observed_at: datetime,
    check: Callable[[], None] | None = None,
) -> tuple[PaperLedgerExternalHead, sqlite3.Connection]:
    connection = sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)
    connection.row_factory = sqlite3.Row
    interrupted: list[BaseException] = []

    def progress() -> int:
        if check is None:
            return 0
        try:
            check()
        except BaseException as exc:
            interrupted.append(exc)
            return 1
        return 0

    connection.set_progress_handler(progress, 1000)
    try:
        if check is not None:
            check()
        status = PaperBrokerStore._ledger_trust_status(connection)
        if status.state != "trusted":
            raise RecoveryBackupIntegrityError(f"paper ledger is not trusted: {status.reason}")
        _migration, latest = PaperBrokerStore._attestation_head(connection)
        payload = PaperBrokerStore._validate_attestation_row(latest)
        marker = connection.execute(
            "SELECT * FROM paper_ledger_head_marker WHERE revision = ? LIMIT 1",
            (int(latest["revision"]),),
        ).fetchone()
        if marker is None:
            raise RecoveryBackupIntegrityError("paper ledger canonical head is missing")
        marker_payload = PaperBrokerStore._validate_head_marker_row(marker)
        if marker_payload["attestation_fingerprint"] != str(latest["attestation_fingerprint"]):
            raise RecoveryBackupIntegrityError("paper ledger canonical head differs")
        persisted_at = datetime.fromisoformat(str(payload["persisted_at"]).replace("Z", "+00:00"))
        head = PaperLedgerExternalHead(
            profile_generation=profile_generation,
            source_artifact_role=source_artifact_role,
            ledger_generation=str(payload["ledger_generation"]),
            revision=int(payload["revision"]),
            attestation_fingerprint=str(latest["attestation_fingerprint"]),
            head_marker_fingerprint=str(marker["head_marker_fingerprint"]),
            migration_attestation_fingerprint=str(payload["migration_attestation_fingerprint"]),
            counts={column: int(payload[column]) for column in _LEDGER_ATTESTATION_COUNT_COLUMNS},
            persisted_at=persisted_at,
            observed_at=observed_at,
        )
        return head, connection
    except RecoveryBackupIntegrityError:
        connection.close()
        raise
    except (PaperBrokerReconciliationError, sqlite3.Error, ValueError) as exc:
        connection.close()
        if interrupted:
            raise interrupted[0] from exc
        raise RecoveryBackupIntegrityError("paper ledger head verification failed") from exc


def _verify_head_monotonic(
    *,
    previous: PaperLedgerExternalHead | None,
    current: PaperLedgerExternalHead,
    snapshot: sqlite3.Connection,
    check: Callable[[], None] | None = None,
) -> None:
    if check is not None:
        check()
    if previous is None:
        return
    if (
        previous.profile_generation != current.profile_generation
        or previous.ledger_generation != current.ledger_generation
        or previous.migration_attestation_fingerprint != current.migration_attestation_fingerprint
        or current.revision < previous.revision
        or any(current.counts[key] < value for key, value in previous.counts.items())
    ):
        raise RecoveryBackupIntegrityError("paper ledger external head rollback detected")
    if current.revision == previous.revision:
        if current.attestation_fingerprint != previous.attestation_fingerprint:
            raise RecoveryBackupIntegrityError("paper ledger same-revision rollback detected")
        return
    attestation = snapshot.execute(
        "SELECT * FROM paper_ledger_attestation WHERE revision = ? LIMIT 1",
        (previous.revision,),
    ).fetchone()
    marker = snapshot.execute(
        "SELECT * FROM paper_ledger_head_marker WHERE revision = ? LIMIT 1",
        (previous.revision,),
    ).fetchone()
    if (
        attestation is None
        or marker is None
        or str(attestation["attestation_fingerprint"]) != previous.attestation_fingerprint
        or str(marker["head_marker_fingerprint"]) != previous.head_marker_fingerprint
    ):
        raise RecoveryBackupIntegrityError("paper ledger external head lineage is detached")
    PaperBrokerStore._validate_attestation_row(attestation)
    PaperBrokerStore._validate_head_marker_row(marker)
    if check is not None:
        check()


def _freeze_tree_contents(root: Path) -> None:
    for current, directories, files in os.walk(root, topdown=False):
        current_path = Path(current)
        for name in files:
            os.chmod(current_path / name, _IMMUTABLE_FILE_MODE)
        for name in directories:
            os.chmod(current_path / name, _IMMUTABLE_DIRECTORY_MODE)
        if current_path != root:
            os.chmod(current_path, _IMMUTABLE_DIRECTORY_MODE)


class _BackupOperationGuard:
    def __init__(
        self,
        *,
        deadline: float,
        monotonic: Callable[[], float],
        cancelled: Callable[[], bool],
    ) -> None:
        self.deadline = deadline
        self.monotonic = monotonic
        self.cancelled = cancelled

    def check(self) -> None:
        if self.cancelled():
            raise RecoveryBackupIntegrityError("recovery backup operation cancelled")
        if self.monotonic() > self.deadline:
            raise RecoveryBackupIntegrityError("recovery backup deadline exceeded")

    def remaining_seconds(self) -> float:
        self.check()
        return max(0.001, self.deadline - self.monotonic())


class RecoveryBackupProducer:
    def __init__(
        self,
        *,
        config: RecoveryBackupConfig,
        signer: RecoveryBackupSigner,
        clock: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
        cancelled: Callable[[], bool] | None = None,
        fault_hook: Callable[[str], None] | None = None,
        trusted_verifiers: Mapping[str, RecoveryPayloadVerifier] | None = None,
    ) -> None:
        if signer.key_id != config.signer_key_id:
            raise ValueError("recovery backup signer key differs from config")
        self.config = config
        self.signer = signer
        self.clock = clock or (lambda: datetime.now(UTC))
        self.monotonic = monotonic or time.monotonic
        self.cancelled = cancelled or (lambda: False)
        self.fault_hook = fault_hook
        trusted = dict(trusted_verifiers or {})
        trusted[signer.key_id] = signer
        self.trusted_verifiers: Mapping[str, RecoveryPayloadVerifier] = MappingProxyType(trusted)
        self.source_root = Path(config.source_root)
        self.publication_root = Path(config.publication_root)
        self.generations_root = self.publication_root / "generations"
        self.candidates_root = self.publication_root / ".candidates"
        self.failed_root = self.publication_root / ".failed"
        self.receipts_root = self.publication_root / "receipts"
        self.current_path = self.publication_root / "current.json"
        self.intent_path = self.publication_root / ".publication-intent.json"
        self.lock_path = self.publication_root / ".backup.lock"
        self.authority_path = (
            self.publication_root
            / "authorities"
            / "paper-ledger"
            / f"{config.target_profile_generation}.json"
        )
        self._active_source_leases: Mapping[str, _RecoverySourceLease] = MappingProxyType({})

    def _guard(self, *, started: float | None = None) -> _BackupOperationGuard:
        origin = self.monotonic() if started is None else started
        return _BackupOperationGuard(
            deadline=origin + self.config.deadline_seconds,
            monotonic=self.monotonic,
            cancelled=self.cancelled,
        )

    def _fault(self, stage: str) -> None:
        if self.fault_hook is not None:
            self.fault_hook(stage)

    def _prepare_layout(self) -> None:
        self.publication_root.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
        for path in (
            self.generations_root,
            self.candidates_root,
            self.failed_root,
            self.receipts_root,
            self.authority_path.parent,
        ):
            path.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
            observed = os.lstat(path)
            if stat.S_ISLNK(observed.st_mode) or not stat.S_ISDIR(observed.st_mode):
                raise RecoveryBackupIntegrityError("recovery backup layout is unsafe")

    @contextmanager
    def _lock(self):
        descriptor = os.open(
            self.lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
        )
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1:
                raise RecoveryBackupIntegrityError("recovery backup lock is unsafe")
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _current(self) -> RecoveryBackupCurrentPointer | None:
        if not self.current_path.exists():
            return None
        pointer = RecoveryBackupCurrentPointer.model_validate(
            _read_contract(self.current_path, RecoveryBackupCurrentPointer).model_dump(
                mode="python"
            )
        )
        _verify_signed_backup_contract(
            pointer,
            trusted_verifiers=self.trusted_verifiers,
        )
        return pointer

    def _checkpoint_mutable_duckdb_sources(self, *, guard: _BackupOperationGuard) -> None:
        for artifact in sorted(
            (item for item in self.config.artifacts if item.kind in _DUCKDB_MUTABLE_KINDS),
            key=lambda item: item.logical_role,
        ):
            guard.check()
            lease = _RecoverySourceLease(root=self.source_root, artifact=artifact)
            interrupted = threading.Event()
            try:
                try:
                    connection = duckdb.connect(str(lease.path))
                    timer = threading.Timer(
                        guard.remaining_seconds(),
                        lambda event=interrupted, active=connection: (
                            event.set(),
                            active.interrupt(),
                        ),
                    )
                    timer.daemon = True
                    try:
                        timer.start()
                        connection.execute("CHECKPOINT")
                    finally:
                        timer.cancel()
                        connection.close()
                    guard.check()
                    if interrupted.is_set():
                        raise RecoveryBackupIntegrityError(
                            "DuckDB preview checkpoint deadline exceeded"
                        )
                except duckdb.Error as exc:
                    if interrupted.is_set():
                        raise RecoveryBackupIntegrityError(
                            "DuckDB preview checkpoint deadline exceeded"
                        ) from exc
                    raise RecoveryBackupIntegrityError("DuckDB preview checkpoint failed") from exc
                lease.verify_named(allow_content_change=True)
                if lease.path.with_name(f"{lease.path.name}.wal").exists():
                    raise RecoveryBackupIntegrityError("DuckDB preview checkpoint retained a WAL")
            finally:
                lease.close()

    @contextmanager
    def _source_leases(self, *, guard: _BackupOperationGuard):
        leases: list[_RecoverySourceLease] = []
        try:
            for artifact in sorted(
                self.config.artifacts,
                key=lambda item: item.logical_role,
            ):
                guard.check()
                leases.append(_RecoverySourceLease(root=self.source_root, artifact=artifact))
            yield tuple(leases)
        finally:
            for lease in leases:
                lease.close()

    def _preview_from_leases(
        self,
        leases: tuple[_RecoverySourceLease, ...],
        *,
        guard: _BackupOperationGuard,
    ) -> RecoveryBackupPreview:
        source_evidence: list[dict[str, JsonValue]] = []
        total = 0
        for lease in leases:
            guard.check()
            observed = lease.verify_named(allow_content_change=False)
            if (
                lease.artifact.kind in _DUCKDB_KINDS
                and lease.path.with_name(f"{lease.path.name}.wal").exists()
            ):
                raise RecoveryBackupIntegrityError("DuckDB preview cannot bind an unsealed WAL")
            remaining = self.config.max_total_bytes - total
            size, content_sha256 = lease.content_sha256(
                max_bytes=remaining,
                check=guard.check,
            )
            total += size
            source_evidence.append(
                {
                    "logical_role": lease.artifact.logical_role,
                    "device": observed.st_dev,
                    "inode": observed.st_ino,
                    "owner": observed.st_uid,
                    "group": observed.st_gid,
                    "mode": stat.S_IMODE(observed.st_mode),
                    "nlink": observed.st_nlink,
                    "size_bytes": size,
                    "mtime_ns": observed.st_mtime_ns,
                    "ctime_ns": observed.st_ctime_ns,
                    "content_sha256": content_sha256,
                }
            )
        if total > self.config.max_total_bytes:
            raise RecoveryBackupIntegrityError("recovery backup source exceeds byte budget")
        current = self._current() if self.publication_root.exists() else None
        plan_id = canonical_sha256(
            {
                "contract": "runtime-recovery-backup-plan/v1",
                "config_id": self.config.config_id,
                "sources": source_evidence,
            }
        )
        return RecoveryBackupPreview(
            plan_id=plan_id,
            config_id=str(self.config.config_id),
            target_profile_generation=self.config.target_profile_generation,
            signer_key_id=self.config.signer_key_id,
            artifact_count=len(self.config.artifacts),
            total_source_bytes=total,
            current_generation_id=None if current is None else current.generation_id,
        )

    def _preview(self, *, guard: _BackupOperationGuard) -> RecoveryBackupPreview:
        self._checkpoint_mutable_duckdb_sources(guard=guard)
        with self._source_leases(guard=guard) as leases:
            return self._preview_from_leases(leases, guard=guard)

    def preview(self) -> RecoveryBackupPreview:
        return self._preview(guard=self._guard())

    def _check_deadline(self, deadline: float) -> None:
        if self.monotonic() > deadline:
            raise RecoveryBackupIntegrityError("recovery backup deadline exceeded")

    def _snapshot_artifacts(self, candidate: Path, *, guard: _BackupOperationGuard) -> int:
        artifacts_root = candidate / "artifacts"
        artifacts_root.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        total = 0
        for artifact in sorted(self.config.artifacts, key=lambda item: item.logical_role):
            guard.check()
            lease = self._active_source_leases.get(artifact.logical_role)
            if lease is None:
                raise RecoveryBackupIntegrityError("recovery backup source lease is missing")
            lease.verify_named(allow_content_change=False)
            source = _safe_source(self.source_root, artifact.source_path)
            destination = artifacts_root.joinpath(*PurePosixPath(artifact.source_path).parts)
            remaining = self.config.max_total_bytes - total
            if artifact.kind in _DUCKDB_KINDS:
                copied = _snapshot_duckdb(
                    source,
                    destination,
                    kind=artifact.kind,
                    max_bytes=remaining,
                    check=guard.check,
                    remaining_seconds=guard.remaining_seconds,
                )
            elif artifact.kind in _SQLITE_KINDS:
                copied = _snapshot_sqlite(source, destination, check=guard.check)
            else:
                copied = _copy_regular(
                    source,
                    destination,
                    max_bytes=remaining,
                    check=guard.check,
                )
            total += copied
            lease.verify_named(
                allow_content_change=False,
            )
            if total > self.config.max_total_bytes:
                raise RecoveryBackupIntegrityError("recovery backup exceeds byte budget")
        _fsync_directory(artifacts_root)
        return total

    def _previous_head(self) -> PaperLedgerExternalHead | None:
        pointer = self._current()
        if pointer is None:
            return None
        try:
            generation = self.publication_root.joinpath(
                *PurePosixPath(pointer.generation_path).parts
            )
            receipt = RecoveryBackupReceipt.model_validate(
                _read_contract(
                    generation / "recovery-backup-receipt.json",
                    RecoveryBackupReceipt,
                ).model_dump(mode="python")
            )
            _verify_signed_backup_contract(
                receipt,
                trusted_verifiers=self.trusted_verifiers,
            )
            authority = PaperLedgerExternalHead.model_validate(
                _read_contract(
                    generation / "paper-ledger-authority.json",
                    PaperLedgerExternalHead,
                ).model_dump(mode="python")
            )
            if (
                receipt.receipt_id != pointer.receipt_id
                or receipt.paper_ledger_head != authority
                or authority.profile_generation != pointer.profile_generation
            ):
                raise RecoveryBackupIntegrityError(
                    "paper ledger generation authority differs from signed current"
                )
            return authority
        except RecoveryBackupIntegrityError as exc:
            raise RecoveryBackupIntegrityError(
                "paper ledger external head rollback/lineage authority is invalid"
            ) from exc

    def _seal_receipt(self, **values: object) -> RecoveryBackupReceipt:
        unsigned = RecoveryBackupReceipt(
            **values,
            key_id=self.signer.key_id,
            signature="pending",
        )
        return RecoveryBackupReceipt.model_validate(
            {
                **unsigned.model_dump(mode="python", exclude={"signature"}),
                "signature": self.signer.sign(unsigned.signing_payload()),
            }
        )

    def _seal_pointer(self, **values: object) -> RecoveryBackupCurrentPointer:
        unsigned = RecoveryBackupCurrentPointer(
            **values,
            key_id=self.signer.key_id,
            signature="pending",
        )
        return RecoveryBackupCurrentPointer.model_validate(
            {
                **unsigned.model_dump(mode="python", exclude={"signature"}),
                "signature": self.signer.sign(unsigned.signing_payload()),
            }
        )

    def _seal_intent(self, **values: object) -> RecoveryBackupPublicationIntent:
        unsigned = RecoveryBackupPublicationIntent(
            **values,
            key_id=self.signer.key_id,
            signature="pending",
        )
        return RecoveryBackupPublicationIntent.model_validate(
            {
                **unsigned.model_dump(mode="python", exclude={"signature"}),
                "signature": self.signer.sign(unsigned.signing_payload()),
            }
        )

    def _publish_authority_cache(self, authority: PaperLedgerExternalHead) -> None:
        _atomic_write(
            self.authority_path,
            canonical_json_bytes(authority.model_dump(mode="json")),
            mode=_IMMUTABLE_FILE_MODE,
        )

    def _recover_interrupted_publication(self) -> None:
        if not self.intent_path.exists():
            return
        intent = RecoveryBackupPublicationIntent.model_validate(
            _read_contract(
                self.intent_path,
                RecoveryBackupPublicationIntent,
            ).model_dump(mode="python")
        )
        _verify_signed_backup_contract(
            intent,
            trusted_verifiers=self.trusted_verifiers,
        )
        if intent.previous_pointer is not None:
            _verify_signed_backup_contract(
                intent.previous_pointer,
                trusted_verifiers=self.trusted_verifiers,
            )
        current = self._current()
        active = intent.previous_pointer
        if current is not None and current.generation_id == intent.generation_id:
            active = current
        if active is not None:
            generation = self.publication_root.joinpath(
                *PurePosixPath(active.generation_path).parts
            )
            authority = PaperLedgerExternalHead.model_validate(
                _read_contract(
                    generation / "paper-ledger-authority.json",
                    PaperLedgerExternalHead,
                ).model_dump(mode="python")
            )
            self._publish_authority_cache(authority)
        with suppress(FileNotFoundError):
            self.intent_path.unlink()
            _fsync_directory(self.intent_path.parent)

    def execute(self, *, expected_plan_id: str) -> RecoveryBackupReceipt:
        started = self.monotonic()
        guard = self._guard(started=started)
        preview = self._preview(guard=guard)
        if expected_plan_id != preview.plan_id:
            raise RecoveryBackupIntegrityError("recovery backup plan changed after dry-run")
        self._prepare_layout()
        with self._lock():
            guard.check()
            self._recover_interrupted_publication()
            previous_pointer = self._current()
            operation_id = uuid.uuid4().hex
            candidate = self.candidates_root / operation_id
            candidate.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
            snapshot_connection: sqlite3.Connection | None = None
            try:
                with self._source_leases(guard=guard) as leases:
                    leased_preview = self._preview_from_leases(leases, guard=guard)
                    if leased_preview.plan_id != expected_plan_id:
                        raise RecoveryBackupIntegrityError(
                            "recovery backup source identity changed after preview"
                        )
                    self._active_source_leases = MappingProxyType(
                        {lease.artifact.logical_role: lease for lease in leases}
                    )
                    try:
                        total_bytes = self._snapshot_artifacts(candidate, guard=guard)
                    finally:
                        self._active_source_leases = MappingProxyType({})
                guard.check()
                by_role = {item.logical_role: item for item in self.config.artifacts}
                production = by_role[self.config.production_artifact_role]
                paper = by_role[self.config.paper_ledger_artifact_role]
                artifacts_root = candidate / "artifacts"
                paper_head, snapshot_connection = _paper_head(
                    artifacts_root / paper.source_path,
                    profile_generation=self.config.target_profile_generation,
                    source_artifact_role=paper.logical_role,
                    observed_at=self.clock().astimezone(UTC),
                    check=guard.check,
                )
                _verify_head_monotonic(
                    previous=self._previous_head(),
                    current=paper_head,
                    snapshot=snapshot_connection,
                    check=guard.check,
                )
                guard.check()
                expectations = build_runtime_recovery_fixed_replay_expectations(
                    target_root=artifacts_root,
                    dataset_path=artifacts_root / production.source_path,
                    strategy_bindings=self.config.strategy_bindings,
                    start_date=self.config.replay_start_date,
                    end_date=self.config.replay_end_date,
                )
                guard.check()
                expectations_document = RecoveryFixedReplayExpectationsDocument(
                    expectations=expectations
                )
                verifier = RuntimeRecoveryFixedReplayVerifier(expectations=expectations)
                published_artifacts = tuple(
                    artifact.model_copy(update={"source_path": f"artifacts/{artifact.source_path}"})
                    for artifact in self.config.artifacts
                )
                target = build_real_recovery_target(
                    source_root=candidate,
                    target_commit=self.config.target_commit,
                    target_profile_generation=self.config.target_profile_generation,
                    as_of=self.config.as_of,
                    production_artifact_role=self.config.production_artifact_role,
                    paper_ledger_artifact_role=self.config.paper_ledger_artifact_role,
                    artifacts=published_artifacts,
                    external_attestations={"paper_ledger": str(paper_head.head_id)},
                )
                tool = seal_recovery_tool_bundle(
                    target=target,
                    verifier_commit=self.config.verifier_commit,
                    executable_fingerprint=verifier.fingerprint,
                    key_id=self.config.signer_key_id,
                    signer=self.signer,
                )
                completed_at = self.clock().astimezone(UTC)
                generation_path = f"generations/{target.manifest_id}"
                receipt = self._seal_receipt(
                    manifest_id=str(target.manifest_id),
                    tool_bundle_id=str(tool.bundle_id),
                    expectations_sha256=str(expectations_document.expectations_sha256),
                    target_commit=self.config.target_commit,
                    target_profile_generation=self.config.target_profile_generation,
                    generation_path=generation_path,
                    paper_ledger_head=paper_head,
                    artifact_count=len(target.artifacts),
                    total_bytes=total_bytes,
                    duration_ms=max(0, int((self.monotonic() - started) * 1000)),
                    completed_at=completed_at,
                )
                documents = {
                    "recovery-target.json": target,
                    "recovery-tool.json": tool,
                    "fixed-replay-expectations.json": expectations_document,
                    "recovery-backup-receipt.json": receipt,
                    "paper-ledger-authority.json": paper_head,
                }
                for name, document in documents.items():
                    _atomic_write(
                        candidate / name,
                        canonical_json_bytes(document.model_dump(mode="json")),
                    )
                guard.check()
                generation = self.generations_root / str(target.manifest_id)
                if generation.exists():
                    existing = RecoveryBackupReceipt.model_validate(
                        _read_contract(
                            generation / "recovery-backup-receipt.json",
                            RecoveryBackupReceipt,
                        ).model_dump(mode="python")
                    )
                    _verify_signed_backup_contract(
                        existing,
                        trusted_verifiers=self.trusted_verifiers,
                    )
                    if existing != receipt:
                        existing_target = _read_contract(
                            generation / "recovery-target.json",
                            RealRecoveryTargetManifest,
                        )
                        existing_tool = _read_contract(
                            generation / "recovery-tool.json",
                            RecoveryToolVerifierBundle,
                        )
                        existing_expectations = _read_contract(
                            generation / "fixed-replay-expectations.json",
                            RecoveryFixedReplayExpectationsDocument,
                        )
                        existing_authority = _read_contract(
                            generation / "paper-ledger-authority.json",
                            PaperLedgerExternalHead,
                        )
                        if (
                            existing_target != target
                            or existing_tool != tool
                            or existing_expectations != expectations_document
                            or existing_authority != paper_head
                        ):
                            raise RecoveryBackupIntegrityError(
                                "recovery backup generation identity conflicts"
                            )
                    shutil.rmtree(candidate)
                    receipt = existing
                    paper_head = receipt.paper_ledger_head
                else:
                    _freeze_tree_contents(candidate)
                    os.replace(candidate, generation)
                    os.chmod(generation, _IMMUTABLE_DIRECTORY_MODE)
                    _fsync_directory(self.generations_root)
                receipt_path = self.receipts_root / f"{receipt.receipt_id}.json"
                if not receipt_path.exists():
                    _atomic_write(
                        receipt_path,
                        canonical_json_bytes(receipt.model_dump(mode="json")),
                        mode=_IMMUTABLE_FILE_MODE,
                    )
                pointer = self._seal_pointer(
                    generation_id=str(target.manifest_id),
                    generation_path=generation_path,
                    manifest_id=str(target.manifest_id),
                    receipt_id=str(receipt.receipt_id),
                    profile_generation=self.config.target_profile_generation,
                    previous_generation_id=(
                        previous_pointer.generation_id
                        if previous_pointer is not None
                        and previous_pointer.generation_id != target.manifest_id
                        else None
                    ),
                    published_at=completed_at,
                )
                intent = self._seal_intent(
                    operation_id=operation_id,
                    generation_id=str(target.manifest_id),
                    generation_path=generation_path,
                    receipt_id=str(receipt.receipt_id),
                    paper_ledger_head_id=str(paper_head.head_id),
                    previous_pointer=previous_pointer,
                    created_at=self.clock().astimezone(UTC),
                )
                _atomic_write(
                    self.intent_path,
                    canonical_json_bytes(intent.model_dump(mode="json")),
                )
                self._fault("after_publication_intent")
                guard.check()
                _atomic_write(
                    self.current_path,
                    canonical_json_bytes(pointer.model_dump(mode="json")),
                )
                self._fault("after_current")
                guard.check()
                self._publish_authority_cache(paper_head)
                self._fault("after_authority_cache")
                with suppress(FileNotFoundError):
                    self.intent_path.unlink()
                    _fsync_directory(self.intent_path.parent)
                return receipt
            except Exception:
                with suppress(Exception):
                    self._recover_interrupted_publication()
                if candidate.exists():
                    failed = self.failed_root / operation_id
                    with suppress(OSError):
                        os.replace(candidate, failed)
                        _fsync_directory(self.failed_root)
                raise
            finally:
                if snapshot_connection is not None:
                    snapshot_connection.close()


def load_recovery_backup_generation(
    publication_root: Path,
    *,
    verification_budget: RecoveryVerificationBudget | None = None,
    trusted_verifiers: Mapping[str, RecoveryPayloadVerifier] | None = None,
) -> tuple[
    RecoveryBackupCurrentPointer,
    RecoveryBackupReceipt,
    RealRecoveryTargetManifest,
    RecoveryToolVerifierBundle,
    RecoveryFixedReplayExpectationsDocument,
]:
    """Load one current backup generation through its canonical pointer and receipt."""

    root = Path(_canonical_absolute(publication_root, label="recovery publication root"))
    if trusted_verifiers is None:
        trusted_verifiers = _environment_trusted_verifiers()
    if not trusted_verifiers:
        raise RecoveryBackupIntegrityError("recovery backup trusted verifier set is empty")
    pointer = RecoveryBackupCurrentPointer.model_validate(
        _read_contract(root / "current.json", RecoveryBackupCurrentPointer).model_dump(
            mode="python"
        )
    )
    _verify_signed_backup_contract(pointer, trusted_verifiers=trusted_verifiers)
    generation = root.joinpath(*PurePosixPath(pointer.generation_path).parts)
    receipt = RecoveryBackupReceipt.model_validate(
        _read_contract(
            generation / "recovery-backup-receipt.json",
            RecoveryBackupReceipt,
        ).model_dump(mode="python")
    )
    _verify_signed_backup_contract(receipt, trusted_verifiers=trusted_verifiers)
    target = RealRecoveryTargetManifest.model_validate(
        _read_contract(
            generation / "recovery-target.json",
            RealRecoveryTargetManifest,
        ).model_dump(mode="python")
    )
    tool = RecoveryToolVerifierBundle.model_validate(
        _read_contract(
            generation / "recovery-tool.json",
            RecoveryToolVerifierBundle,
        ).model_dump(mode="python")
    )
    expectations = RecoveryFixedReplayExpectationsDocument.model_validate(
        _read_contract(
            generation / "fixed-replay-expectations.json",
            RecoveryFixedReplayExpectationsDocument,
        ).model_dump(mode="python")
    )
    try:
        authority = PaperLedgerExternalHead.model_validate(
            _read_contract(
                generation / "paper-ledger-authority.json",
                PaperLedgerExternalHead,
            ).model_dump(mode="python")
        )
    except RecoveryBackupIntegrityError as exc:
        raise RecoveryBackupIntegrityError(
            "recovery backup external paper ledger authority is invalid"
        ) from exc
    if (
        pointer.generation_id != receipt.manifest_id
        or pointer.manifest_id != target.manifest_id
        or pointer.receipt_id != receipt.receipt_id
        or pointer.profile_generation != receipt.target_profile_generation
        or pointer.profile_generation != target.target_profile_generation
        or receipt.tool_bundle_id != tool.bundle_id
        or receipt.expectations_sha256 != expectations.expectations_sha256
        or authority.profile_generation != pointer.profile_generation
        or authority.head_id != receipt.paper_ledger_head.head_id
        or target.external_attestations.get("paper_ledger") != authority.head_id
        or RuntimeRecoveryFixedReplayVerifier(expectations=expectations.expectations).fingerprint
        != tool.executable_fingerprint
    ):
        raise RecoveryBackupIntegrityError("current recovery backup generation differs")
    tool_verifier = trusted_verifiers.get(tool.key_id)
    if (
        tool_verifier is None
        or tool_verifier.key_id != tool.key_id
        or not tool_verifier.verify(tool.signing_payload(), tool.signature)
    ):
        raise RecoveryBackupIntegrityError("recovery tool signature key is not trusted")
    captured = build_real_recovery_target(
        source_root=generation,
        target_commit=target.target_commit,
        target_profile_generation=target.target_profile_generation,
        as_of=target.as_of,
        production_artifact_role=target.production_artifact_role,
        paper_ledger_artifact_role=target.paper_ledger_artifact_role,
        artifacts=tuple(
            RealRecoveryArtifactSpec(
                logical_role=artifact.logical_role,
                kind=artifact.kind,
                source_path=artifact.source_path,
                restore_path=artifact.restore_path,
                generation_id=artifact.generation_id,
                schema_version=artifact.schema_version,
                available_at=artifact.available_at,
                price_basis=artifact.price_basis,
                relations=tuple(item.relation_name for item in artifact.relations),
                references=artifact.references,
            )
            for artifact in target.artifacts
        ),
        external_attestations=target.external_attestations,
        verification_budget=verification_budget,
    )
    if captured != target:
        raise RecoveryBackupIntegrityError("current recovery backup artifact generation differs")
    return pointer, receipt, target, tool, expectations


def load_recovery_backup_config(path: Path) -> RecoveryBackupConfig:
    """Load one canonical producer config without accepting aliases or loose JSON."""

    return RecoveryBackupConfig.model_validate(
        _read_contract(
            Path(_canonical_absolute(path, label="recovery backup config")),
            RecoveryBackupConfig,
        ).model_dump(mode="python")
    )
