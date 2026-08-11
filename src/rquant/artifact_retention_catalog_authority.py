"""Immutable published catalog identity owned by the retention authority."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from uuid import uuid4

from pydantic import ConfigDict, Field, field_validator, model_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


class RetentionCatalogAuthorityError(RuntimeError):
    """The retention-owned catalog publication is unsafe or inconsistent."""


class _AuthorityModel(RuntimeContractModel):
    model_config = ConfigDict(extra="forbid", frozen=True, revalidate_instances="always")


class RetentionCatalogPathIdentity(_AuthorityModel):
    path: Path
    device: int = Field(ge=0)
    inode: int = Field(gt=0)
    mode: int

    @field_validator("path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        return _absolute(value, label="retention catalog identity")


class RetentionCatalogSnapshot(_AuthorityModel):
    schema_version: int = Field(default=1, frozen=True)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    reference_store_path: Path
    reference_store_identity: RetentionCatalogPathIdentity
    generation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("reference_store_path")
    @classmethod
    def require_reference_store_path(cls, value: Path) -> Path:
        return _absolute(value, label="retention reference store")

    @model_validator(mode="after")
    def bind_generation(self) -> RetentionCatalogSnapshot:
        if self.reference_store_identity.path != self.reference_store_path:
            raise ValueError("retention catalog snapshot reference path conflicts with identity")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"generation_id"}))
        if self.generation_id is None:
            object.__setattr__(self, "generation_id", expected)
        elif self.generation_id != expected:
            raise ValueError("retention catalog snapshot generation is invalid")
        return self


class RetentionCatalogCurrentReceipt(_AuthorityModel):
    schema_version: int = Field(default=1, frozen=True)
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    snapshot_file: str = Field(pattern=r"^[0-9a-f]{64}\.json$")
    receipt_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def bind_receipt(self) -> RetentionCatalogCurrentReceipt:
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("retention catalog receipt identity is invalid")
        return self


class RetentionCatalogAuthority(_AuthorityModel):
    root: Path
    current_receipt_path: Path
    receipt: RetentionCatalogCurrentReceipt
    snapshot: RetentionCatalogSnapshot


class LegacyCatalogReferenceQuarantineReceipt(_AuthorityModel):
    legacy_path: Path
    legacy_identity: RetentionCatalogPathIdentity
    quarantine_path: Path
    reason: str = Field(min_length=1, max_length=256)
    receipt_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("legacy_path", "quarantine_path")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        return _absolute(value, label="legacy catalog quarantine")

    @model_validator(mode="after")
    def bind_receipt(self) -> LegacyCatalogReferenceQuarantineReceipt:
        if self.legacy_identity.path != self.legacy_path:
            raise ValueError("legacy catalog quarantine identity path conflicts")
        expected = canonical_sha256(self.model_dump(mode="json", exclude={"receipt_id"}))
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("legacy catalog quarantine receipt identity is invalid")
        return self


def _absolute(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    normalized = Path(os.path.abspath(candidate))
    if not candidate.is_absolute() or candidate != normalized:
        raise ValueError(f"{label} path must be absolute and normalized")
    return candidate


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _model_bytes(value: _AuthorityModel) -> bytes:
    return _canonical_bytes(value.model_dump(mode="json")) + b"\n"


def _ensure_private_directory(path: Path, *, label: str) -> Path:
    selected = _absolute(path, label=label)
    selected.mkdir(parents=True, mode=0o700, exist_ok=True)
    observed = selected.lstat()
    if (
        stat.S_ISLNK(observed.st_mode)
        or not stat.S_ISDIR(observed.st_mode)
        or observed.st_uid != os.geteuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise RetentionCatalogAuthorityError(f"{label} must be an owned directory with mode 0700")
    return selected


def _file_identity(path: Path, *, label: str) -> RetentionCatalogPathIdentity:
    selected = _absolute(path, label=label)
    try:
        before = selected.lstat()
        descriptor = os.open(
            selected,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise RetentionCatalogAuthorityError(f"{label} cannot be opened safely") from exc
    try:
        opened = os.fstat(descriptor)
        after = selected.lstat()
        identities = {(item.st_dev, item.st_ino) for item in (before, opened, after)}
        if len(identities) != 1:
            raise RetentionCatalogAuthorityError(f"{label} changed while opening")
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise RetentionCatalogAuthorityError(
                f"{label} must be an owned singly linked file with mode 0600"
            )
        return RetentionCatalogPathIdentity(
            path=selected,
            device=opened.st_dev,
            inode=opened.st_ino,
            mode=stat.S_IMODE(opened.st_mode),
        )
    finally:
        os.close(descriptor)


def _read_model(path: Path, model: type[_AuthorityModel], *, label: str) -> _AuthorityModel:
    _file_identity(path, label=label)
    payload = path.read_bytes()
    if len(payload) > 1024 * 1024 or not payload.endswith(b"\n"):
        raise RetentionCatalogAuthorityError(f"{label} is not canonical")
    try:
        parsed = model.model_validate(json.loads(payload))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RetentionCatalogAuthorityError(f"{label} is invalid") from exc
    if payload != _model_bytes(parsed):
        raise RetentionCatalogAuthorityError(f"{label} is not canonical")
    return parsed


def _write_once(path: Path, payload: bytes) -> None:
    parent = _ensure_private_directory(path.parent, label="retention catalog authority parent")
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
    except FileExistsError:
        if path.read_bytes() != payload:
            raise RetentionCatalogAuthorityError(
                "immutable retention catalog publication conflicts"
            ) from None
        return
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _write_current(path: Path, payload: bytes) -> None:
    parent = _ensure_private_directory(path.parent, label="retention catalog authority parent")
    if path.exists():
        _file_identity(path, label="retention catalog current receipt")
        if path.read_bytes() == payload:
            return
    temporary = parent / f".{path.name}.{uuid4().hex}.tmp"
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def publish_retention_catalog_authority(
    *,
    state_root: Path,
    reference_store_path: Path,
    producer_commit: str,
) -> RetentionCatalogAuthority:
    """Atomically publish the retention writer's current catalog identity."""

    selected_state_root = _ensure_private_directory(state_root, label="retention state root")
    reference_identity = _file_identity(reference_store_path, label="retention reference store")
    root = _ensure_private_directory(
        selected_state_root / "catalog-authority",
        label="retention catalog authority",
    )
    snapshots = _ensure_private_directory(root / "snapshots", label="retention catalog snapshots")
    snapshot = RetentionCatalogSnapshot(
        producer_commit=producer_commit,
        reference_store_path=reference_identity.path,
        reference_store_identity=reference_identity,
    )
    assert snapshot.generation_id is not None
    snapshot_path = snapshots / f"{snapshot.generation_id}.json"
    snapshot_payload = _model_bytes(snapshot)
    _write_once(snapshot_path, snapshot_payload)
    receipt = RetentionCatalogCurrentReceipt(
        generation_id=snapshot.generation_id,
        snapshot_sha256=hashlib.sha256(snapshot_payload).hexdigest(),
        snapshot_file=snapshot_path.name,
    )
    _write_current(root / "current.json", _model_bytes(receipt))
    return RetentionCatalogAuthority(
        root=root,
        current_receipt_path=root / "current.json",
        receipt=receipt,
        snapshot=snapshot,
    )


def bootstrap_retention_catalog_authority(
    *,
    state_root: Path,
    reference_store_path: Path,
    producer_commit: str,
) -> RetentionCatalogAuthority:
    """Create or verify the empty/current retention-owned authority during bootstrap."""

    return publish_retention_catalog_authority(
        state_root=state_root,
        reference_store_path=reference_store_path,
        producer_commit=producer_commit,
    )


def initialize_retention_catalog_authority(
    *,
    state_root: Path,
    reference_store_path: Path,
    producer_commit: str,
) -> RetentionCatalogAuthority:
    """Initialize the sole retention store, then publish its empty authority.

    This is the clean-install boundary: Job Center must bind only after this
    function has durably created the private SQLite authority and published the
    immutable receipt.  Retrying after any interruption is safe because both
    SQLite schema setup and the catalog publication are idempotent.
    """

    root = _ensure_private_directory(state_root, label="retention state root")
    reference = _absolute(reference_store_path, label="retention reference store")
    if reference.parent != root:
        raise RetentionCatalogAuthorityError(
            "retention reference store must be a direct child of the retention state root"
        )
    from rquant.artifact_retention import ArtifactReferenceStore

    store = ArtifactReferenceStore(reference, managed_trust_root=root)
    try:
        return publish_retention_catalog_authority(
            state_root=root,
            reference_store_path=reference,
            producer_commit=producer_commit,
        )
    finally:
        store.close()


def load_retention_catalog_authority(
    root: Path,
    *,
    expected_producer_commit: str | None,
    expected_reference_store_path: Path,
) -> RetentionCatalogAuthority:
    """Load only the immutable retention publication, never a catalog-local SQLite file."""

    selected_root = _absolute(root, label="retention catalog authority")
    _ensure_private_directory(selected_root, label="retention catalog authority")
    if selected_root.name != "catalog-authority":
        raise RetentionCatalogAuthorityError("legacy catalog reference store is not an authority")
    receipt_path = selected_root / "current.json"
    receipt = _read_model(
        receipt_path,
        RetentionCatalogCurrentReceipt,
        label="retention catalog current receipt",
    )
    assert isinstance(receipt, RetentionCatalogCurrentReceipt)
    snapshot_path = selected_root / "snapshots" / receipt.snapshot_file
    snapshot = _read_model(
        snapshot_path,
        RetentionCatalogSnapshot,
        label="retention catalog snapshot",
    )
    assert isinstance(snapshot, RetentionCatalogSnapshot)
    payload = _model_bytes(snapshot)
    if (
        receipt.generation_id != snapshot.generation_id
        or receipt.snapshot_sha256 != hashlib.sha256(payload).hexdigest()
    ):
        raise RetentionCatalogAuthorityError("retention catalog receipt conflicts with snapshot")
    if (
        expected_producer_commit is not None
        and snapshot.producer_commit != expected_producer_commit
    ):
        raise RetentionCatalogAuthorityError("retention catalog producer commit is stale")
    if snapshot.reference_store_path != _absolute(
        expected_reference_store_path,
        label="expected retention reference store",
    ):
        raise RetentionCatalogAuthorityError("retention catalog reference store path conflicts")
    observed_reference_identity = _file_identity(
        snapshot.reference_store_path,
        label="retention reference store",
    )
    if observed_reference_identity != snapshot.reference_store_identity:
        raise RetentionCatalogAuthorityError("retention catalog reference store identity changed")
    return RetentionCatalogAuthority(
        root=selected_root,
        current_receipt_path=receipt_path,
        receipt=receipt,
        snapshot=snapshot,
    )


def quarantine_legacy_catalog_reference_store(
    *,
    legacy_path: Path,
    retention_state_root: Path,
    reason: str,
) -> LegacyCatalogReferenceQuarantineReceipt:
    """Record an old catalog-local SQLite authority as deprecated without reading it."""

    identity = _file_identity(legacy_path, label="legacy catalog reference store")
    state_root = _ensure_private_directory(retention_state_root, label="retention state root")
    quarantine_root = _ensure_private_directory(
        state_root / "legacy-catalog-quarantine",
        label="legacy catalog quarantine",
    )
    receipt = LegacyCatalogReferenceQuarantineReceipt(
        legacy_path=identity.path,
        legacy_identity=identity,
        quarantine_path=(
            quarantine_root / f"{canonical_sha256(identity.model_dump(mode='json'))}.json"
        ),
        reason=reason,
    )
    _write_once(receipt.quarantine_path, _model_bytes(receipt))
    return receipt


__all__ = [
    "LegacyCatalogReferenceQuarantineReceipt",
    "RetentionCatalogAuthority",
    "RetentionCatalogAuthorityError",
    "RetentionCatalogCurrentReceipt",
    "RetentionCatalogSnapshot",
    "bootstrap_retention_catalog_authority",
    "initialize_retention_catalog_authority",
    "load_retention_catalog_authority",
    "publish_retention_catalog_authority",
    "quarantine_legacy_catalog_reference_store",
]
