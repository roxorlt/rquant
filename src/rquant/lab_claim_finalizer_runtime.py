"""Offline-issued, generation-bound runtime material for the Lab claim finalizer.

The runtime installer intentionally has no root signing API.  It can only
verify an already-issued certificate and atomically publish a self-contained
generation after binding it to the Lab SQLite file identity.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
import subprocess
import uuid
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from rquant.adapter_manifest import (
    Ed25519ContractSigner,
    Ed25519PublicKeyRecord,
    VerifyOnlyEd25519Keyring,
    ed25519_public_key_fingerprint,
)
from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    read_secure_regular_file,
    secure_path_metadata,
)
from rquant.lab_claim_finalizer_trust import (
    LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE,
    LabClaimFinalizerTrustCertificate,
    LabClaimFinalizerTrustError,
    LabClaimFinalizerTrustVerifier,
    sign_lab_claim_finalizer_trust_certificate,
)
from rquant.lab_claim_publication import (
    LabClaimPublicationIdentity,
    LabClaimPublicationRolloutEvidence,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import (
    StrictJsonError,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

FINALIZER_RUNTIME_SCHEMA_VERSION = 1
FINALIZER_RUNTIME_SCHEMA_VERSION_BOUND = 16
_MAX_RUNTIME_FILE_BYTES = 1_048_576


class FinalizerRuntimeError(RuntimeError):
    """The finalizer runtime could not be safely issued, installed, or enabled."""


class FinalizerRuntimeArtifact(RuntimeContractModel):
    name: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,127}$")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    mode: int = Field(strict=True, ge=0, le=0o777)


class FinalizerRuntimeGenerationManifest(RuntimeContractModel):
    schema_version: Literal[1] = FINALIZER_RUNTIME_SCHEMA_VERSION
    contract: Literal["rquant-lab-claim-finalizer-generation/v1"] = (
        "rquant-lab-claim-finalizer-generation/v1"
    )
    store_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    database_device: int = Field(strict=True, ge=0)
    database_inode: int = Field(strict=True, ge=0)
    schema_version_bound: Literal[16] = FINALIZER_RUNTIME_SCHEMA_VERSION_BOUND
    certificate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_basis_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifacts: tuple[FinalizerRuntimeArtifact, ...] = Field(min_length=1)


class FinalizerRuntimeGenerationBasis(RuntimeContractModel):
    """The non-self-referential bytes from which a generation name is derived."""

    schema_version: Literal[1] = FINALIZER_RUNTIME_SCHEMA_VERSION
    contract: Literal["rquant-lab-claim-finalizer-generation-basis/v1"] = (
        "rquant-lab-claim-finalizer-generation-basis/v1"
    )
    store_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    database_device: int = Field(strict=True, ge=0)
    database_inode: int = Field(strict=True, ge=0)
    certificate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    artifacts: tuple[FinalizerRuntimeArtifact, ...] = Field(min_length=1)


class FinalizerRuntimeInstallRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    certificate: LabClaimFinalizerTrustCertificate
    database_path: Path
    store_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_version: Literal[16] = FINALIZER_RUNTIME_SCHEMA_VERSION_BOUND
    runtime_private_key_path: Path
    runtime_public_key_path: Path
    root_capability_secret_path: Path
    current_plan_private_key_path: Path
    current_plan_public_key_path: Path
    worker_verify_bundle_path: Path
    finalizer_public_key: Ed25519PublicKeyRecord
    # The installer consumes this once and emits the real, generation-local
    # runtime material.  Daemons never select this input directly.
    runtime_material_template_path: Path | None = None
    rollout_state_path: Path | None = None
    rollout_mode: Literal["candidate", "shadow", "live"] = "candidate"


class FinalizerRuntimeInstallReceipt(RuntimeContractModel):
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_generation_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    write_performed: bool


@dataclass(frozen=True)
class LoadedFinalizerRuntimeGeneration:
    generation_id: str
    generation_dir: Path
    manifest: FinalizerRuntimeGenerationManifest
    runtime_material_path: Path
    worker_verifier_path: Path
    runtime_material: object


class FinalizerPreflightCheck(RuntimeContractModel):
    name: str
    status: Literal["ok", "warn", "fail", "skip"]
    summary: str


class FinalizerPreflightReport(RuntimeContractModel):
    checks: tuple[FinalizerPreflightCheck, ...]

    @property
    def status(self) -> Literal["ok", "warn", "fail", "skip"]:
        statuses = {check.status for check in self.checks}
        if "fail" in statuses:
            return "fail"
        if "warn" in statuses:
            return "warn"
        if statuses == {"skip"}:
            return "skip"
        return "ok"

    def by_name(self, name: str) -> FinalizerPreflightCheck:
        return next(check for check in self.checks if check.name == name)

    def render_markdown(self) -> str:
        rows = ["| check | status | summary |", "| --- | --- | --- |"]
        rows.extend(f"| {item.name} | {item.status} | {item.summary} |" for item in self.checks)
        return "\n".join(rows) + "\n"


class FinalizerPreflightCollector:
    """Collect finalizer readiness directly from its selected runtime generation."""

    def __init__(
        self, settings: object, *, expected_uid: int | None = None, expected_gid: int | None = None
    ) -> None:
        self._settings = settings
        self._uid = os.getuid() if expected_uid is None else expected_uid
        self._gid = os.getgid() if expected_gid is None else expected_gid

    def _check(self, name: str, action: Callable[[], str]) -> FinalizerPreflightCheck:
        try:
            return FinalizerPreflightCheck(name=name, status="ok", summary=action())
        except (
            Exception
        ) as exc:  # defensive collector: one failed dependency reports, never masks peers
            return FinalizerPreflightCheck(name=name, status="fail", summary=str(exc)[:240])

    def collect(self) -> FinalizerPreflightReport:
        names = (
            "feature_flags",
            "generation",
            "schema",
            "certificate",
            "key_match",
            "filesystem",
            "unix_peer",
            "composition",
            "worker_verify_only",
            "scheduler_isolation",
            "sqlite_write_lock",
            "duckdb",
            "readonly_replica",
            "rotation_horizon",
            "outbox_backlog",
            "retry_slo",
        )
        enabled = bool(getattr(self._settings, "lab_claim_finalizer_enabled", False))
        workers_enabled = bool(getattr(self._settings, "lab_v2_claim_publication_enabled", False))
        if not enabled and not workers_enabled:
            return FinalizerPreflightReport(
                checks=tuple(
                    FinalizerPreflightCheck(
                        name=name, status="skip", summary="finalizer feature disabled"
                    )
                    for name in names
                )
            )
        root = getattr(self._settings, "lab_claim_finalizer_runtime_material_root", None)
        if root is None:
            return FinalizerPreflightReport(
                checks=tuple(
                    FinalizerPreflightCheck(
                        name=name,
                        status="fail" if name == "generation" else "skip",
                        summary="controlled runtime material root is required"
                        if name == "generation"
                        else "not collected",
                    )
                    for name in names
                )
            )
        selected: LoadedFinalizerRuntimeGeneration | None = None
        checks: list[FinalizerPreflightCheck] = [
            FinalizerPreflightCheck(
                name="feature_flags",
                status="ok" if enabled else "fail",
                summary="finalizer enabled with runtime-selected material",
            )
        ]
        try:
            selected = load_current_lab_claim_finalizer_generation(
                root,
                expected_uid=self._uid,
                expected_gid=self._gid,
                trusted_base=getattr(
                    self._settings,
                    "lab_claim_finalizer_runtime_trusted_base",
                    Path("/etc/rquant"),
                ),
            )
            checks.append(
                FinalizerPreflightCheck(
                    name="generation", status="ok", summary=f"current={selected.generation_id}"
                )
            )
        except FinalizerRuntimeError as exc:
            checks.append(
                FinalizerPreflightCheck(name="generation", status="fail", summary=str(exc))
            )
        if selected is None:
            checks.extend(
                FinalizerPreflightCheck(name=name, status="skip", summary="generation unavailable")
                for name in names[2:]
            )
            return FinalizerPreflightReport(checks=tuple(checks))
        material = selected.runtime_material
        database = Path(self._settings.lab_jobs_path_resolved)

        def schema() -> str:
            with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            if version != 16 or selected.manifest.schema_version_bound != 16:
                raise FinalizerRuntimeError("Lab SQLite schema is not 16")
            return "Lab SQLite schema 16"

        checks.append(self._check("schema", schema))

        def certificate() -> str:
            certificate_value = material.trust_certificate
            if certificate_value.purpose != LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE:
                raise FinalizerRuntimeError("certificate purpose differs")
            if certificate_value.expires_at <= datetime.now(UTC):
                raise FinalizerRuntimeError("certificate expired")
            observed = database.stat()
            if (certificate_value.database_device, certificate_value.database_inode) != (
                observed.st_dev,
                observed.st_ino,
            ):
                raise FinalizerRuntimeError("certificate database generation differs")
            return "certificate purpose, time, store generation"

        checks.append(self._check("certificate", certificate))

        def key_match() -> str:
            private = _secure_read(
                material.finalizer_runtime_private_key_path,
                modes=frozenset({0o600}),
                maximum_bytes=16_384,
            )
            expected = next(
                record
                for record in material.finalizer_public_keys
                if record.key_id == material.trust_certificate.finalizer_key_id
            )
            if (
                ed25519_public_key_fingerprint(_public_from_private(private))
                != expected.public_key_fingerprint
            ):
                raise FinalizerRuntimeError("runtime private/public key mismatch")
            return "finalizer private/public fingerprint"

        checks.append(self._check("key_match", key_match))
        checks.append(self._check("filesystem", lambda: "generation files and ancestors verified"))

        def unix_peer() -> str:
            worker_raw = _secure_read(
                selected.worker_verifier_path,
                modes=frozenset({0o640}),
                maximum_bytes=_MAX_RUNTIME_FILE_BYTES,
            )
            from rquant.lab_claim_finalizer_trust import LabClaimPublicationWorkerVerificationConfig

            worker = strict_model_validate_canonical_json(
                LabClaimPublicationWorkerVerificationConfig, worker_raw
            )
            observed = Path(worker.current_claim_socket_path).lstat()
            if (
                not stat.S_ISSOCK(observed.st_mode)
                or observed.st_uid != worker.current_claim_socket_owner_uid
                or stat.S_IMODE(observed.st_mode) != worker.current_claim_socket_mode
            ):
                raise FinalizerRuntimeError("AF_UNIX peer policy differs")
            return "AF_UNIX peer owner/mode"

        checks.append(self._check("unix_peer", unix_peer))

        def composition() -> str:
            from rquant.lab_claim_finalizer_composition import (
                compose_production_lab_claim_finalizer_daemon,
            )

            daemon = compose_production_lab_claim_finalizer_daemon(settings=self._settings)
            del daemon
            return "production composition accepted current generation"

        checks.append(self._check("composition", composition))

        def worker_verify_only() -> str:
            from rquant.lab_claim_finalizer_trust import LabClaimPublicationWorkerVerificationConfig

            worker = strict_model_validate_canonical_json(
                LabClaimPublicationWorkerVerificationConfig,
                _secure_read(
                    selected.worker_verifier_path,
                    modes=frozenset({0o640}),
                    maximum_bytes=_MAX_RUNTIME_FILE_BYTES,
                ),
            )
            worker.require_verify_only_roles()
            return "worker bundle has verify-only roles"

        checks.append(self._check("worker_verify_only", worker_verify_only))

        def scheduler_isolation() -> str:
            unit = (
                Path(__file__).resolve().parents[2]
                / "deploy"
                / "systemd"
                / "rquant-runtime-lab-jobs@.service"
            )
            text = unit.read_text(encoding="utf-8")
            secret_root = str(root)
            if (
                "InaccessiblePaths=" not in text
                or secret_root not in text
                or "ReadOnlyPaths=" not in text
                or "lab_jobs.sqlite3" not in text
            ):
                raise FinalizerRuntimeError(
                    "scheduler unit does not prove finalizer secret isolation/read-only ledger"
                )
            return "scheduler unit denies generation root and opens Lab SQLite read-only"

        checks.append(self._check("scheduler_isolation", scheduler_isolation))

        def write_lock() -> str:
            with sqlite3.connect(database, timeout=0.2) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.rollback()
            return "BEGIN IMMEDIATE rollback succeeded"

        checks.append(self._check("sqlite_write_lock", write_lock))
        checks.extend(
            (
                FinalizerPreflightCheck(
                    name="duckdb", status="skip", summary="not a finalizer dependency"
                ),
                FinalizerPreflightCheck(
                    name="readonly_replica", status="skip", summary="not a finalizer dependency"
                ),
            )
        )
        horizon = (material.trust_certificate.expires_at - datetime.now(UTC)).total_seconds()
        checks.append(
            FinalizerPreflightCheck(
                name="rotation_horizon",
                status="ok" if horizon >= 3600 else "warn",
                summary=f"certificate horizon seconds={max(0, int(horizon))}",
            )
        )

        def health_metrics() -> tuple[int, float | None, bool]:
            with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
                backlog = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM "
                        "lab_claim_publication_finalizer_observation_degradation "
                        "WHERE drained_at IS NULL"
                    ).fetchone()[0]
                )
                next_retry = connection.execute(
                    "SELECT MIN(next_retry_at) FROM "
                    "lab_claim_publication_finalizer_observation_degradation "
                    "WHERE drained_at IS NULL"
                ).fetchone()[0]
                lease = connection.execute(
                    "SELECT expires_at, released_at FROM lab_claim_publication_finalizer_lease "
                    "WHERE singleton = 1"
                ).fetchone()
            retry_seconds = None
            if next_retry is not None:
                retry_seconds = max(
                    0.0,
                    datetime.fromisoformat(str(next_retry)).timestamp()
                    - datetime.now(UTC).timestamp(),
                )
            lease_active = bool(
                lease is not None
                and lease[1] is None
                and datetime.fromisoformat(str(lease[0])) > datetime.now(UTC)
            )
            return backlog, retry_seconds, lease_active

        def outbox_backlog() -> str:
            backlog, _retry, _lease = health_metrics()
            if backlog > 100:
                raise FinalizerRuntimeError(
                    f"observation degradation backlog={backlog} exceeds fail threshold"
                )
            return f"observation degradation backlog={backlog}"

        checks.append(self._check("outbox_backlog", outbox_backlog))

        def retry_slo() -> str:
            backlog, retry_seconds, lease_active = health_metrics()
            if backlog == 0:
                return f"retry backlog clear; finalizer_lease_active={lease_active}"
            if retry_seconds is not None and retry_seconds > 30:
                raise FinalizerRuntimeError(
                    f"retry horizon seconds={int(retry_seconds)} exceeds threshold"
                )
            return (
                f"retry backlog={backlog}; retry horizon seconds={int(retry_seconds or 0)}; "
                f"finalizer_lease_active={lease_active}"
            )

        checks.append(self._check("retry_slo", retry_slo))
        return FinalizerPreflightReport(checks=tuple(checks))


class FinalizerPreflightInputs(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    finalizer_enabled: bool
    v2_workers_enabled: bool
    schema_version: int = 0
    certificate_valid: bool = False
    database_generation_matches: bool = False
    private_public_matches: bool = False
    filesystem_secure: bool = False
    unix_peer_secure: bool = False
    composition_valid: bool = False
    worker_verify_only: bool = False
    scheduler_has_no_secret: bool = False
    scheduler_sqlite_read_only: bool = False
    duckdb_dependency: bool = False
    readonly_replica_dependency: bool = False
    rotation_expires_at: datetime | None = None
    outbox_backlog: int = Field(default=0, ge=0)
    retry_latency_seconds: int = Field(default=0, ge=0)
    minimum_rotation_horizon_seconds: int = Field(default=3_600, ge=1)
    maximum_outbox_backlog: int = Field(default=100, ge=0)
    maximum_retry_latency_seconds: int = Field(default=30, ge=0)


class FinalizerRolloutPhase(StrEnum):
    OFF = "OFF"
    MATERIAL_INSTALLED = "MATERIAL_INSTALLED"
    PREFLIGHT_OK = "PREFLIGHT_OK"
    FINALIZER_READY = "FINALIZER_READY"
    V2_WORKERS_READY = "V2_WORKERS_READY"
    SCHEDULER_EMITS_V2 = "SCHEDULER_EMITS_V2"
    DRAINING = "DRAINING"


class FinalizerRolloutMode(StrEnum):
    CANDIDATE = "candidate"
    SHADOW = "shadow"
    LIVE = "live"


class FinalizerRolloutError(FinalizerRuntimeError):
    """An evidence-bound rollout transition is invalid."""


class FinalizerRolloutSnapshot(RuntimeContractModel):
    phase: FinalizerRolloutPhase
    revision: int = Field(ge=0)
    evidence: str
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    mode: FinalizerRolloutMode


class FinalizerRolloutEmitPermit(RuntimeContractModel):
    store_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    revision: int = Field(ge=0)
    mode: FinalizerRolloutMode
    holder: str = Field(min_length=1, max_length=200)
    binding_id: str = Field(pattern=r"^[0-9a-f]{32}$")


class FinalizerPublishedEvidence(RuntimeContractModel):
    """The exact, canonical evidence triple persisted for one publication attempt."""

    attempt_id: str = Field(
        pattern=(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}"
            r"-[0-9a-f]{12}$"
        )
    )
    evidence_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    publication_identity: str = Field(min_length=2, max_length=8_192)

    @field_validator("attempt_id", "evidence_hash", "publication_identity", mode="before")
    @classmethod
    def reject_noncanonical_text(cls, value: object) -> object:
        if not isinstance(value, str) or value != value.strip():
            raise ValueError("published evidence text must already be canonical")
        return value

    @model_validator(mode="after")
    def validate_identity_binding(self) -> FinalizerPublishedEvidence:
        try:
            identity = strict_model_validate_canonical_json(
                LabClaimPublicationIdentity, self.publication_identity
            )
        except (StrictJsonError, ValueError) as exc:
            raise ValueError("publication identity must be canonical") from exc
        if str(identity.attempt_id) != self.attempt_id:
            raise ValueError("publication identity attempt differs")
        return self


def issue_offline_finalizer_certificate(
    *,
    root_signer: Ed25519ContractSigner,
    finalizer_signer: Ed25519ContractSigner,
    store_id: str,
    database_generation: tuple[int, int],
    schema_version: int,
    not_before: datetime,
    expires_at: datetime,
) -> LabClaimFinalizerTrustCertificate:
    """Sign one canonical certificate in the offline root ceremony only."""

    if root_signer.key_purpose != "lab_claim_finalizer_root":
        raise FinalizerRuntimeError("offline root signer has an invalid purpose")
    if finalizer_signer.key_purpose != "lab_claim_finalizer":
        raise FinalizerRuntimeError("finalizer signer has an invalid purpose")
    if schema_version != FINALIZER_RUNTIME_SCHEMA_VERSION_BOUND:
        raise FinalizerRuntimeError("finalizer certificate requires schema 16")
    start = not_before.astimezone(UTC)
    end = expires_at.astimezone(UTC)
    if start >= end:
        raise FinalizerRuntimeError("certificate validity interval is invalid")
    return sign_lab_claim_finalizer_trust_certificate(
        root_signer=root_signer,
        certificate=LabClaimFinalizerTrustCertificate(
            root_issuer=root_signer.issuer,
            root_key_id=root_signer.key_id,
            finalizer_issuer=finalizer_signer.issuer,
            finalizer_key_id=finalizer_signer.key_id,
            finalizer_public_key_fingerprint=finalizer_signer.public_key_fingerprint,
            store_id=store_id,
            database_device=database_generation[0],
            database_inode=database_generation[1],
            schema_version_bound=schema_version,
            purpose=LAB_CLAIM_FINALIZER_PUBLICATION_PURPOSE,
            not_before=start,
            expires_at=end,
            signature="unsigned",
        ),
    )


def rotate_offline_finalizer_certificate(**kwargs: object) -> LabClaimFinalizerTrustCertificate:
    """Rotation is an offline re-issue; the runtime has no rotate capability."""

    return issue_offline_finalizer_certificate(**kwargs)  # type: ignore[arg-type]


def inspect_offline_finalizer_certificate(
    certificate: LabClaimFinalizerTrustCertificate,
) -> dict[str, object]:
    """Return public certificate fields suitable for an offline inspection CLI."""

    return {
        "contract": certificate.contract,
        "schema_version": certificate.schema_version,
        "store_id": certificate.store_id,
        "database_generation": [certificate.database_device, certificate.database_inode],
        "schema_version_bound": certificate.schema_version_bound,
        "purpose": certificate.purpose,
        "root_issuer": certificate.root_issuer,
        "root_key_id": certificate.root_key_id,
        "finalizer_issuer": certificate.finalizer_issuer,
        "finalizer_key_id": certificate.finalizer_key_id,
        "finalizer_public_key_fingerprint": certificate.finalizer_public_key_fingerprint,
        "not_before": certificate.not_before.isoformat(),
        "expires_at": certificate.expires_at.isoformat(),
        "signature": certificate.signature,
    }


def load_offline_finalizer_certificate(path: Path) -> LabClaimFinalizerTrustCertificate:
    """Read a public canonical certificate through the common secure path walk."""

    try:
        return strict_model_validate_canonical_json(
            LabClaimFinalizerTrustCertificate,
            _secure_read(path, modes=frozenset({0o444, 0o640, 0o644}), maximum_bytes=65_536),
        )
    except (AuthorityPathSecurityError, ValueError) as exc:
        raise FinalizerRuntimeError("offline certificate file is invalid") from exc


def read_offline_finalizer_material(path: Path, *, private: bool) -> bytes:
    """Use the same descriptor-bound reader for offline ceremony inputs."""

    return _secure_read(
        path,
        modes=frozenset({0o600}) if private else frozenset({0o444, 0o640, 0o644}),
        maximum_bytes=16_384,
    )


def write_offline_finalizer_certificate(
    path: Path,
    certificate: LabClaimFinalizerTrustCertificate,
) -> None:
    """Persist exactly the canonical signed artifact with file+directory fsync."""

    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise FinalizerRuntimeError("offline certificate output path is invalid")
    _write_atomic(path, canonical_model_json_bytes(certificate), mode=0o640)


def _secure_read(path: Path, *, modes: frozenset[int], maximum_bytes: int) -> bytes:
    return read_secure_regular_file(
        path,
        trusted_root=Path("/"),
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
        allowed_final_uids=frozenset({os.getuid()}),
        allowed_final_gids=frozenset({os.getgid()}),
        allowed_modes=modes,
        max_bytes=maximum_bytes,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_atomic(path: Path, payload: bytes, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    try:
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            if written < 1:
                raise OSError("atomic write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    _fsync_directory(path.parent)


def _ensure_directory(path: Path, *, uid: int, gid: int, mode: int) -> None:
    try:
        observed = path.lstat()
    except FileNotFoundError:
        path.mkdir(mode=mode)
        try:
            os.chown(path, uid, gid, follow_symlinks=False)
        except OSError as exc:
            raise FinalizerRuntimeError("cannot apply finalizer runtime directory owner") from exc
    else:
        if stat.S_ISLNK(observed.st_mode):
            raise FinalizerRuntimeError("finalizer runtime directory is a symlink")
    metadata = secure_path_metadata(
        path,
        trusted_root=path.parent,
        expected_uid=uid,
        expected_gid=gid,
        expected_mode=mode,
        kind="directory",
    )
    if metadata.mode != mode:
        raise FinalizerRuntimeError("finalizer runtime directory is unsafe")


def _public_from_private(private_key: bytes) -> bytes:
    completed = subprocess.run(
        ("openssl", "pkey", "-pubout"),
        input=private_key,
        check=False,
        capture_output=True,
        timeout=5,
    )
    if completed.returncode != 0 or not completed.stdout:
        raise FinalizerRuntimeError("runtime private key is not usable")
    return completed.stdout


def _read_generation_pointer(
    runtime_root: Path,
    name: str,
    *,
    expected_uid: int,
    expected_gid: int,
    trusted_base: Path | None = None,
    trusted_base_owner_uids: frozenset[int] | None = None,
) -> str | None:
    path = runtime_root / name
    try:
        observed = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise FinalizerRuntimeError("finalizer generation pointer is unavailable") from exc
    if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise FinalizerRuntimeError("finalizer generation pointer is unsafe")
    try:
        payload = read_secure_regular_file(
            path,
            trusted_root=trusted_base or runtime_root,
            allowed_ancestor_uids=trusted_base_owner_uids,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            allowed_final_uids=frozenset({expected_uid}),
            allowed_final_gids=frozenset({expected_gid}),
            allowed_modes=frozenset({0o640}),
            max_bytes=66,
        )
    except AuthorityPathSecurityError as exc:
        raise FinalizerRuntimeError("finalizer generation pointer is unsafe") from exc
    value = payload.decode("ascii", errors="strict").strip()
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise FinalizerRuntimeError("finalizer generation pointer is invalid")
    return value


class LabClaimFinalizerGenerationInstaller:
    """Publish validated runtime material using regular-file generation pointers."""

    def __init__(
        self,
        *,
        runtime_root: Path,
        root_keyring: VerifyOnlyEd25519Keyring,
        finalizer_keyring: VerifyOnlyEd25519Keyring,
        expected_uid: int = 0,
        expected_gid: int = 0,
        trusted_base: Path | None = None,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._runtime_root = runtime_root
        self._root_keyring = root_keyring
        self._finalizer_keyring = finalizer_keyring
        self._expected_uid = expected_uid
        self._expected_gid = expected_gid
        self._trusted_base = trusted_base or runtime_root.parent
        self._fault_hook = fault_hook

    def _fault(self, label: str) -> None:
        if self._fault_hook is not None:
            self._fault_hook(label)

    def _adopt(self, path: Path) -> None:
        """Apply the same service owner contract used by readers and preflight."""

        if os.geteuid() != 0 and (
            self._expected_uid != os.geteuid() or self._expected_gid != os.getegid()
        ):
            raise FinalizerRuntimeError(
                "installing for another service uid/gid requires a privileged installer"
            )
        try:
            os.chown(path, self._expected_uid, self._expected_gid, follow_symlinks=False)
        except OSError as exc:
            raise FinalizerRuntimeError("cannot apply finalizer service owner contract") from exc

    def _database_generation(self, request: FinalizerRuntimeInstallRequest) -> tuple[int, int]:
        try:
            before = secure_path_metadata(
                request.database_path,
                trusted_root=Path("/"),
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
                expected_mode=0o600,
                kind="file",
            )
            with sqlite3.connect(f"file:{request.database_path}?mode=ro", uri=True) as connection:
                version = connection.execute("PRAGMA user_version").fetchone()[0]
            after = secure_path_metadata(
                request.database_path,
                trusted_root=Path("/"),
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
                expected_mode=0o600,
                kind="file",
            )
        except (AuthorityPathSecurityError, OSError, sqlite3.Error) as exc:
            raise FinalizerRuntimeError("Lab SQLite identity is unavailable") from exc
        if (before.device, before.inode) != (after.device, after.inode):
            raise FinalizerRuntimeError("Lab SQLite identity changed while checking")
        if version != request.schema_version:
            raise FinalizerRuntimeError("Lab SQLite schema is not 16")
        return before.device, before.inode

    def _validated_artifacts(
        self, request: FinalizerRuntimeInstallRequest
    ) -> tuple[tuple[FinalizerRuntimeArtifact, bytes], ...]:
        private = _secure_read(
            request.runtime_private_key_path, modes=frozenset({0o600}), maximum_bytes=16_384
        )
        public = _secure_read(
            request.runtime_public_key_path,
            modes=frozenset({0o444, 0o640, 0o644}),
            maximum_bytes=16_384,
        )
        if (
            request.finalizer_public_key.key_purpose != "lab_claim_finalizer"
            or request.finalizer_public_key.public_key_pem != public
            or ed25519_public_key_fingerprint(public)
            != request.certificate.finalizer_public_key_fingerprint
            or ed25519_public_key_fingerprint(_public_from_private(private))
            != ed25519_public_key_fingerprint(public)
        ):
            raise FinalizerRuntimeError(
                "finalizer private/public material does not match certificate"
            )
        plan_private = _secure_read(
            request.current_plan_private_key_path, modes=frozenset({0o600}), maximum_bytes=16_384
        )
        plan_public = _secure_read(
            request.current_plan_public_key_path,
            modes=frozenset({0o444, 0o640, 0o644}),
            maximum_bytes=16_384,
        )
        if ed25519_public_key_fingerprint(
            _public_from_private(plan_private)
        ) != ed25519_public_key_fingerprint(plan_public):
            raise FinalizerRuntimeError("current plan private/public material does not match")
        secret = _secure_read(
            request.root_capability_secret_path, modes=frozenset({0o600}), maximum_bytes=4_096
        )
        worker = _secure_read(
            request.worker_verify_bundle_path,
            modes=frozenset({0o640}),
            maximum_bytes=_MAX_RUNTIME_FILE_BYTES,
        )
        if not secret or not worker:
            raise FinalizerRuntimeError("runtime capability material is empty")
        certificate = canonical_model_json_bytes(request.certificate)
        raw = (
            ("trust-certificate.json", certificate, 0o640),
            ("runtime.private.pem", private, 0o600),
            ("runtime.public.pem", public, 0o640),
            ("root.capability", secret, 0o600),
            ("current-plan.private.pem", plan_private, 0o600),
            ("current-plan.public.pem", plan_public, 0o640),
            ("worker-verifier.json", worker, 0o640),
        )
        return tuple(
            (
                FinalizerRuntimeArtifact(
                    name=name,
                    sha256=hashlib.sha256(payload).hexdigest(),
                    mode=mode,
                ),
                payload,
            )
            for name, payload, mode in raw
        )

    def _generation_material(
        self,
        *,
        request: FinalizerRuntimeInstallRequest,
        generation_id: str,
        artifacts: tuple[tuple[FinalizerRuntimeArtifact, bytes], ...],
    ) -> tuple[tuple[FinalizerRuntimeArtifact, bytes], ...]:
        """Create runtime inputs whose private paths cannot escape the selected generation."""

        if request.runtime_material_template_path is None:
            return artifacts
        from rquant.lab_claim_finalizer_composition import LabClaimFinalizerRuntimeMaterial
        from rquant.lab_claim_finalizer_trust import LabClaimPublicationWorkerVerificationConfig

        try:
            template = strict_model_validate_canonical_json(
                LabClaimFinalizerRuntimeMaterial,
                _secure_read(
                    request.runtime_material_template_path,
                    modes=frozenset({0o600}),
                    maximum_bytes=_MAX_RUNTIME_FILE_BYTES,
                ),
            )
            worker = strict_model_validate_canonical_json(
                LabClaimPublicationWorkerVerificationConfig,
                next(
                    payload
                    for artifact, payload in artifacts
                    if artifact.name == "worker-verifier.json"
                ),
            )
        except (AuthorityPathSecurityError, StopIteration, ValueError) as exc:
            raise FinalizerRuntimeError(
                "runtime material template or worker verifier is invalid"
            ) from exc
        if (
            template.trust_certificate != request.certificate
            or worker.trust_certificate != request.certificate
            or tuple(worker.root_public_keys) != tuple(template.root_public_keys)
            or tuple(worker.finalizer_public_keys) != tuple(template.finalizer_public_keys)
            or tuple(worker.source_plan_public_keys) != tuple(template.source_plan_public_keys)
        ):
            raise FinalizerRuntimeError("worker verifier does not exactly bind runtime material")
        generation = self._runtime_root / "generations" / generation_id
        external_root_public = _secure_read(
            template.current_claim_external_root_public_key_path,
            modes=frozenset({0o444, 0o640, 0o644}),
            maximum_bytes=16_384,
        )
        material = template.model_copy(
            update={
                "finalizer_runtime_private_key_path": generation / "runtime.private.pem",
                "finalizer_root_secret_path": generation / "root.capability",
                "current_claim_plan_private_key_path": generation / "current-plan.private.pem",
                "current_claim_external_root_public_key_path": generation
                / "current-root.public.pem",
            }
        )
        generated = (
            ("current-root.public.pem", external_root_public, 0o640),
            ("runtime-material.json", canonical_model_json_bytes(material), 0o600),
        )
        return artifacts + tuple(
            (
                FinalizerRuntimeArtifact(
                    name=name,
                    sha256=hashlib.sha256(payload).hexdigest(),
                    mode=mode,
                ),
                payload,
            )
            for name, payload, mode in generated
        )

    def _verify_request(
        self, request: FinalizerRuntimeInstallRequest
    ) -> tuple[FinalizerRuntimeGenerationBasis, tuple[tuple[FinalizerRuntimeArtifact, bytes], ...]]:
        generation = self._database_generation(request)
        verifier = LabClaimFinalizerTrustVerifier(
            root_keyring=self._root_keyring,
            finalizer_keyring=self._finalizer_keyring,
        )
        try:
            verifier.require_certificate(
                request.certificate,
                store_id=request.store_id,
                database_generation=generation,
                schema_version=request.schema_version,
                now=datetime.now(UTC),
            )
        except LabClaimFinalizerTrustError as exc:
            raise FinalizerRuntimeError(
                "finalizer certificate does not bind this Lab SQLite"
            ) from exc
        if (
            request.finalizer_public_key.public_key_fingerprint
            != request.certificate.finalizer_public_key_fingerprint
        ):
            raise FinalizerRuntimeError("finalizer public key fingerprint differs from certificate")
        artifacts = self._validated_artifacts(request)
        basis = FinalizerRuntimeGenerationBasis(
            store_id=request.store_id,
            database_device=generation[0],
            database_inode=generation[1],
            certificate_sha256=hashlib.sha256(
                canonical_model_json_bytes(request.certificate)
            ).hexdigest(),
            artifacts=tuple(item for item, _payload in artifacts),
        )
        return basis, artifacts

    def _read_pointer(self, name: str) -> str | None:
        return _read_generation_pointer(
            self._runtime_root,
            name,
            expected_uid=self._expected_uid,
            expected_gid=self._expected_gid,
            trusted_base=self._trusted_base,
            trusted_base_owner_uids=frozenset({0, self._expected_uid}),
        )

    def install(
        self,
        request: FinalizerRuntimeInstallRequest,
        *,
        dry_run: bool = False,
    ) -> FinalizerRuntimeInstallReceipt:
        basis, initial_artifacts = self._verify_request(request)
        basis_bytes = canonical_model_json_bytes(basis)
        generation_id = hashlib.sha256(basis_bytes).hexdigest()
        artifacts = self._generation_material(
            request=request,
            generation_id=generation_id,
            artifacts=initial_artifacts,
        )
        manifest = FinalizerRuntimeGenerationManifest(
            store_id=basis.store_id,
            database_device=basis.database_device,
            database_inode=basis.database_inode,
            certificate_sha256=basis.certificate_sha256,
            generation_id=generation_id,
            generation_basis_sha256=hashlib.sha256(basis_bytes).hexdigest(),
            artifacts=tuple(item for item, _payload in artifacts),
        )
        generation_bytes = canonical_model_json_bytes(manifest)
        if dry_run:
            return FinalizerRuntimeInstallReceipt(
                generation_id=generation_id,
                previous_generation_id=self._read_pointer("current"),
                write_performed=False,
            )
        try:
            _ensure_directory(
                self._runtime_root,
                uid=self._expected_uid,
                gid=self._expected_gid,
                mode=0o700,
            )
            secure_path_metadata(
                self._runtime_root,
                trusted_root=self._trusted_base,
                allowed_ancestor_uids=frozenset({0, self._expected_uid}),
                expected_uid=self._expected_uid,
                expected_gid=self._expected_gid,
                expected_mode=0o700,
                kind="directory",
            )
            generations = self._runtime_root / "generations"
            _ensure_directory(
                generations, uid=self._expected_uid, gid=self._expected_gid, mode=0o700
            )
            self._adopt(self._runtime_root)
            self._adopt(generations)
        except (AuthorityPathSecurityError, OSError) as exc:
            raise FinalizerRuntimeError("finalizer runtime root is unsafe") from exc
        previous = self._read_pointer("current")
        final = generations / generation_id
        if final.exists():
            try:
                observed = read_secure_regular_file(
                    final / "manifest.json",
                    trusted_root=self._runtime_root,
                    expected_uid=self._expected_uid,
                    expected_gid=self._expected_gid,
                    allowed_final_uids=frozenset({self._expected_uid}),
                    allowed_final_gids=frozenset({self._expected_gid}),
                    allowed_modes=frozenset({0o640}),
                    max_bytes=_MAX_RUNTIME_FILE_BYTES,
                )
            except AuthorityPathSecurityError as exc:
                raise FinalizerRuntimeError("existing finalizer generation is unsafe") from exc
            if observed != generation_bytes:
                raise FinalizerRuntimeError("existing finalizer generation differs")
        else:
            stage = generations / f".stage-{uuid.uuid4().hex}"
            try:
                stage.mkdir(mode=0o700)
                self._adopt(stage)
                for artifact, payload in artifacts:
                    _write_atomic(stage / artifact.name, payload, mode=artifact.mode)
                    self._adopt(stage / artifact.name)
                _write_atomic(stage / "generation-basis.json", basis_bytes, mode=0o640)
                self._adopt(stage / "generation-basis.json")
                _write_atomic(stage / "manifest.json", generation_bytes, mode=0o640)
                self._adopt(stage / "manifest.json")
                _fsync_directory(stage)
                self._fault("generation_verified")
                os.rename(stage, final)
                _fsync_directory(generations)
            except BaseException as exc:
                if stage.exists():
                    for path in stage.iterdir():
                        path.unlink(missing_ok=True)
                    stage.rmdir()
                if isinstance(exc, FinalizerRuntimeError):
                    raise
                raise FinalizerRuntimeError("finalizer generation staging failed") from exc
        if previous == generation_id:
            if request.rollout_state_path is not None:
                state = FinalizerRolloutStore(
                    request.rollout_state_path,
                    mode=FinalizerRolloutMode(request.rollout_mode),
                )
                if state.snapshot().phase is FinalizerRolloutPhase.OFF:
                    state.transition(
                        FinalizerRolloutPhase.MATERIAL_INSTALLED,
                        evidence=f"generation:{generation_id}",
                    )
            return FinalizerRuntimeInstallReceipt(
                generation_id=generation_id,
                previous_generation_id=previous,
                write_performed=True,
            )
        self._fault("before_pointer_switch")
        try:
            if previous is not None:
                _write_atomic(
                    self._runtime_root / "previous", f"{previous}\n".encode("ascii"), mode=0o640
                )
                self._adopt(self._runtime_root / "previous")
            _write_atomic(
                self._runtime_root / "current", f"{generation_id}\n".encode("ascii"), mode=0o640
            )
            self._adopt(self._runtime_root / "current")
            self._fault("after_pointer_switch")
        except BaseException as exc:
            if previous is not None:
                _write_atomic(
                    self._runtime_root / "current", f"{previous}\n".encode("ascii"), mode=0o640
                )
                self._adopt(self._runtime_root / "current")
            else:
                (self._runtime_root / "current").unlink(missing_ok=True)
                _fsync_directory(self._runtime_root)
            if isinstance(exc, FinalizerRuntimeError):
                raise
            raise FinalizerRuntimeError("finalizer generation pointer switch failed") from exc
        if request.rollout_state_path is not None:
            try:
                state = FinalizerRolloutStore(
                    request.rollout_state_path,
                    mode=FinalizerRolloutMode(request.rollout_mode),
                )
                if state.snapshot().phase is FinalizerRolloutPhase.OFF:
                    state.transition(
                        FinalizerRolloutPhase.MATERIAL_INSTALLED,
                        evidence=f"generation:{generation_id}",
                    )
            except BaseException as exc:
                # Publishing material without its CAS evidence would permit a half-enable.
                if previous is not None:
                    _write_atomic(
                        self._runtime_root / "current", f"{previous}\n".encode("ascii"), mode=0o640
                    )
                else:
                    (self._runtime_root / "current").unlink(missing_ok=True)
                    _fsync_directory(self._runtime_root)
                raise FinalizerRuntimeError(
                    "rollout state did not accept installed material"
                ) from exc
        return FinalizerRuntimeInstallReceipt(
            generation_id=generation_id,
            previous_generation_id=previous,
            write_performed=True,
        )

    def rotate(self, *_args: object, **_kwargs: object) -> None:
        raise FinalizerRuntimeError("runtime rotation is forbidden; issue an offline certificate")


def load_current_lab_claim_finalizer_generation(
    runtime_root: Path,
    *,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
    trusted_base: Path = Path("/etc/rquant"),
    trusted_base_owner_uids: frozenset[int] | None = None,
) -> LoadedFinalizerRuntimeGeneration:
    """Resolve and verify the only permitted runtime selection source: ``current``.

    The pointer is deliberately a regular file, not a symlink.  Every file is
    read through the descriptor-bound authority helper so replacement, dangling
    paths, and an unsafe ancestor all fail closed before composition sees bytes.
    """

    from rquant.lab_claim_finalizer_composition import LabClaimFinalizerRuntimeMaterial
    from rquant.lab_claim_finalizer_trust import LabClaimPublicationWorkerVerificationConfig

    uid = os.getuid() if expected_uid is None else expected_uid
    gid = os.getgid() if expected_gid is None else expected_gid
    trusted_owners = trusted_base_owner_uids or frozenset({0})
    try:
        secure_path_metadata(
            runtime_root,
            trusted_root=trusted_base,
            allowed_ancestor_uids=trusted_owners,
            expected_uid=uid,
            expected_gid=gid,
            expected_mode=0o700,
            kind="directory",
        )
    except AuthorityPathSecurityError as exc:
        raise FinalizerRuntimeError("finalizer runtime root is outside its trusted base") from exc
    generation_id = _read_generation_pointer(
        runtime_root,
        "current",
        expected_uid=uid,
        expected_gid=gid,
        trusted_base=trusted_base,
        trusted_base_owner_uids=trusted_owners,
    )
    if generation_id is None:
        raise FinalizerRuntimeError("finalizer runtime current generation is missing")
    generation_dir = runtime_root / "generations" / generation_id
    try:
        secure_path_metadata(
            generation_dir,
            trusted_root=runtime_root,
            expected_uid=uid,
            expected_gid=gid,
            expected_mode=0o700,
            kind="directory",
        )
        manifest_raw = read_secure_regular_file(
            generation_dir / "manifest.json",
            trusted_root=runtime_root,
            expected_uid=uid,
            expected_gid=gid,
            allowed_final_uids=frozenset({uid}),
            allowed_final_gids=frozenset({gid}),
            allowed_modes=frozenset({0o640}),
            max_bytes=_MAX_RUNTIME_FILE_BYTES,
        )
        basis_raw = read_secure_regular_file(
            generation_dir / "generation-basis.json",
            trusted_root=runtime_root,
            expected_uid=uid,
            expected_gid=gid,
            allowed_final_uids=frozenset({uid}),
            allowed_final_gids=frozenset({gid}),
            allowed_modes=frozenset({0o640}),
            max_bytes=_MAX_RUNTIME_FILE_BYTES,
        )
        manifest = strict_model_validate_canonical_json(
            FinalizerRuntimeGenerationManifest, manifest_raw
        )
        basis = strict_model_validate_canonical_json(FinalizerRuntimeGenerationBasis, basis_raw)
    except (AuthorityPathSecurityError, ValueError) as exc:
        raise FinalizerRuntimeError("finalizer runtime generation is unsafe") from exc
    if (
        manifest.generation_id != generation_id
        or manifest.generation_basis_sha256 != hashlib.sha256(basis_raw).hexdigest()
        or hashlib.sha256(basis_raw).hexdigest() != generation_id
        or manifest.store_id != basis.store_id
        or manifest.database_device != basis.database_device
        or manifest.database_inode != basis.database_inode
        or manifest.certificate_sha256 != basis.certificate_sha256
    ):
        raise FinalizerRuntimeError("current generation does not exactly bind its manifest")
    artifact_map = {artifact.name: artifact for artifact in manifest.artifacts}
    required = {"runtime-material.json", "worker-verifier.json", "trust-certificate.json"}
    if not required.issubset(artifact_map):
        raise FinalizerRuntimeError("current generation lacks paired runtime artifacts")
    payloads: dict[str, bytes] = {}
    for name, artifact in artifact_map.items():
        try:
            payload = read_secure_regular_file(
                generation_dir / name,
                trusted_root=runtime_root,
                expected_uid=uid,
                expected_gid=gid,
                allowed_final_uids=frozenset({uid}),
                allowed_final_gids=frozenset({gid}),
                allowed_modes=frozenset({artifact.mode}),
                max_bytes=_MAX_RUNTIME_FILE_BYTES,
            )
        except AuthorityPathSecurityError as exc:
            raise FinalizerRuntimeError("current generation artifact is unsafe") from exc
        if hashlib.sha256(payload).hexdigest() != artifact.sha256:
            raise FinalizerRuntimeError("current generation artifact digest differs")
        payloads[name] = payload
    try:
        material = strict_model_validate_canonical_json(
            LabClaimFinalizerRuntimeMaterial, payloads["runtime-material.json"]
        )
        worker = strict_model_validate_canonical_json(
            LabClaimPublicationWorkerVerificationConfig, payloads["worker-verifier.json"]
        )
    except ValueError as exc:
        raise FinalizerRuntimeError("current generation runtime contract is invalid") from exc
    if (
        material.trust_certificate != worker.trust_certificate
        or canonical_model_json_bytes(material.trust_certificate)
        != payloads["trust-certificate.json"]
        or material.finalizer_runtime_private_key_path != generation_dir / "runtime.private.pem"
        or material.finalizer_root_secret_path != generation_dir / "root.capability"
        or material.current_claim_plan_private_key_path
        != generation_dir / "current-plan.private.pem"
        or material.current_claim_external_root_public_key_path
        != generation_dir / "current-root.public.pem"
        or tuple(material.root_public_keys) != tuple(worker.root_public_keys)
        or tuple(material.finalizer_public_keys) != tuple(worker.finalizer_public_keys)
        or tuple(material.source_plan_public_keys) != tuple(worker.source_plan_public_keys)
    ):
        raise FinalizerRuntimeError("paired runtime material is not generation-bound")
    return LoadedFinalizerRuntimeGeneration(
        generation_id=generation_id,
        generation_dir=generation_dir,
        manifest=manifest,
        runtime_material_path=generation_dir / "runtime-material.json",
        worker_verifier_path=generation_dir / "worker-verifier.json",
        runtime_material=material,
    )


def run_lab_claim_finalizer_preflight(inputs: FinalizerPreflightInputs) -> FinalizerPreflightReport:
    """Pure preflight matrix; callers provide verified offline fixture evidence."""

    names = (
        "feature_flags",
        "schema",
        "certificate",
        "database_generation",
        "key_match",
        "filesystem",
        "unix_peer",
        "composition",
        "worker_verify_only",
        "scheduler_isolation",
        "duckdb",
        "readonly_replica",
        "rotation_horizon",
        "outbox_backlog",
        "retry_slo",
    )
    if not inputs.finalizer_enabled and not inputs.v2_workers_enabled:
        return FinalizerPreflightReport(
            checks=tuple(
                FinalizerPreflightCheck(name=name, status="skip", summary="finalizer disabled")
                for name in names
            )
        )
    checks = [
        FinalizerPreflightCheck(
            name="feature_flags",
            status="ok" if inputs.finalizer_enabled else "fail",
            summary="finalizer flag",
        ),
        FinalizerPreflightCheck(
            name="schema",
            status="ok" if inputs.schema_version == 16 else "fail",
            summary="Lab SQLite schema 16",
        ),
        FinalizerPreflightCheck(
            name="certificate",
            status="ok" if inputs.certificate_valid else "fail",
            summary="certificate validity and purpose",
        ),
        FinalizerPreflightCheck(
            name="database_generation",
            status="ok" if inputs.database_generation_matches else "fail",
            summary="SQLite device/inode binding",
        ),
        FinalizerPreflightCheck(
            name="key_match",
            status="ok" if inputs.private_public_matches else "fail",
            summary="private/public fingerprints",
        ),
        FinalizerPreflightCheck(
            name="filesystem",
            status="ok" if inputs.filesystem_secure else "fail",
            summary="owner mode and ancestor walk",
        ),
        FinalizerPreflightCheck(
            name="unix_peer",
            status="ok" if inputs.unix_peer_secure else "fail",
            summary="AF_UNIX peer identity",
        ),
        FinalizerPreflightCheck(
            name="composition",
            status="ok" if inputs.composition_valid else "fail",
            summary="offline composition fixture",
        ),
        FinalizerPreflightCheck(
            name="worker_verify_only",
            status="ok" if inputs.worker_verify_only else "fail",
            summary="worker public verification only",
        ),
        FinalizerPreflightCheck(
            name="scheduler_isolation",
            status="ok"
            if inputs.scheduler_has_no_secret and inputs.scheduler_sqlite_read_only
            else "fail",
            summary="scheduler has no secret or Lab SQLite write access",
        ),
        FinalizerPreflightCheck(
            name="duckdb",
            status="fail" if inputs.duckdb_dependency else "skip",
            summary="not a finalizer dependency",
        ),
        FinalizerPreflightCheck(
            name="readonly_replica",
            status="fail" if inputs.readonly_replica_dependency else "skip",
            summary="not a finalizer dependency",
        ),
    ]
    now = datetime.now(UTC)
    horizon = (
        None
        if inputs.rotation_expires_at is None
        else (inputs.rotation_expires_at.astimezone(UTC) - now).total_seconds()
    )
    checks.append(
        FinalizerPreflightCheck(
            name="rotation_horizon",
            status="ok"
            if horizon is not None and horizon >= inputs.minimum_rotation_horizon_seconds
            else "warn",
            summary="offline certificate rotation horizon",
        )
    )
    checks.append(
        FinalizerPreflightCheck(
            name="outbox_backlog",
            status="ok" if inputs.outbox_backlog <= inputs.maximum_outbox_backlog else "fail",
            summary="publication outbox backlog",
        )
    )
    checks.append(
        FinalizerPreflightCheck(
            name="retry_slo",
            status="ok"
            if inputs.retry_latency_seconds <= inputs.maximum_retry_latency_seconds
            else "warn",
            summary="retry/readiness latency",
        )
    )
    return FinalizerPreflightReport(checks=tuple(checks))


class FinalizerRolloutStore:
    """CAS-backed rollout state. Published records are append-only audit evidence."""

    _FORWARD = {
        FinalizerRolloutPhase.OFF: FinalizerRolloutPhase.MATERIAL_INSTALLED,
        FinalizerRolloutPhase.MATERIAL_INSTALLED: FinalizerRolloutPhase.PREFLIGHT_OK,
        FinalizerRolloutPhase.PREFLIGHT_OK: FinalizerRolloutPhase.FINALIZER_READY,
        FinalizerRolloutPhase.FINALIZER_READY: FinalizerRolloutPhase.V2_WORKERS_READY,
        FinalizerRolloutPhase.V2_WORKERS_READY: FinalizerRolloutPhase.SCHEDULER_EMITS_V2,
        FinalizerRolloutPhase.SCHEDULER_EMITS_V2: FinalizerRolloutPhase.DRAINING,
        FinalizerRolloutPhase.DRAINING: FinalizerRolloutPhase.OFF,
    }

    def __init__(
        self,
        path: Path,
        *,
        create: bool = True,
        mode: FinalizerRolloutMode = FinalizerRolloutMode.CANDIDATE,
        allow_live: bool = False,
    ) -> None:
        self._path = path
        if mode is FinalizerRolloutMode.LIVE and not allow_live:
            raise FinalizerRolloutError("live rollout is disabled by default")
        if not create and not path.exists():
            raise FinalizerRolloutError("rollout state is missing")
        with sqlite3.connect(path) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS finalizer_rollout_state ("
                "singleton INTEGER PRIMARY KEY CHECK(singleton = 1), "
                "phase TEXT NOT NULL, revision INTEGER NOT NULL, evidence TEXT NOT NULL, "
                "evidence_hash TEXT NOT NULL, mode TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE IF NOT EXISTS finalizer_rollout_published ("
                "attempt_id TEXT PRIMARY KEY, evidence_hash TEXT, "
                "publication_identity TEXT, recorded_at TEXT NOT NULL)"
            )
            columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(finalizer_rollout_published)"
                ).fetchall()
            }
            if "evidence_hash" not in columns:
                connection.execute(
                    "ALTER TABLE finalizer_rollout_published ADD COLUMN evidence_hash TEXT"
                )
            if "publication_identity" not in columns:
                connection.execute(
                    "ALTER TABLE finalizer_rollout_published ADD COLUMN publication_identity TEXT"
                )
            connection.execute(
                "INSERT OR IGNORE INTO finalizer_rollout_state VALUES (?, ?, ?, ?, ?, ?)",
                (1, "OFF", 0, "initial", hashlib.sha256(b"initial").hexdigest(), mode.value),
            )

    def snapshot(self) -> FinalizerRolloutSnapshot:
        with sqlite3.connect(self._path) as connection:
            phase, revision, evidence, evidence_hash, mode = connection.execute(
                "SELECT phase, revision, evidence, evidence_hash, mode "
                "FROM finalizer_rollout_state WHERE singleton = 1"
            ).fetchone()
        return FinalizerRolloutSnapshot(
            phase=phase,
            revision=revision,
            evidence=evidence,
            evidence_hash=evidence_hash,
            mode=mode,
        )

    @property
    def identity(self) -> str:
        return hashlib.sha256(str(self._path.resolve()).encode("utf-8")).hexdigest()

    @contextmanager
    def emit_permit(
        self,
        *,
        holder: str,
        timeout_seconds: float = 5.0,
    ) -> Iterator[FinalizerRolloutEmitPermit]:
        """Hold the rollout writer transaction across one V2 external emit."""

        if not holder.strip() or not 0 < timeout_seconds <= 30:
            raise FinalizerRolloutError("rollout emit permit arguments are invalid")
        try:
            connection = sqlite3.connect(self._path, timeout=timeout_seconds, isolation_level=None)
            connection.execute(f"PRAGMA busy_timeout = {int(timeout_seconds * 1000)}")
            connection.execute("BEGIN IMMEDIATE")
            phase, revision, mode = connection.execute(
                "SELECT phase, revision, mode FROM finalizer_rollout_state WHERE singleton = 1"
            ).fetchone()
            if (
                FinalizerRolloutPhase(phase) is not FinalizerRolloutPhase.SCHEDULER_EMITS_V2
                or FinalizerRolloutMode(mode) is FinalizerRolloutMode.LIVE
            ):
                raise FinalizerRolloutError("scheduler V2 emit permit is unavailable")
            permit = FinalizerRolloutEmitPermit(
                store_identity=self.identity,
                revision=revision,
                mode=mode,
                holder=holder.strip(),
                binding_id=uuid.uuid4().hex,
            )
            try:
                yield permit
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()
        except sqlite3.Error as exc:
            raise FinalizerRolloutError("scheduler V2 emit permit store is unavailable") from exc
        finally:
            if "connection" in locals():
                connection.close()

    def transition(
        self, phase: FinalizerRolloutPhase, *, evidence: str, expected_revision: int | None = None
    ) -> FinalizerRolloutSnapshot:
        if not evidence:
            raise FinalizerRolloutError("rollout evidence is required")
        with sqlite3.connect(self._path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            current, revision, _old, _old_hash, mode = connection.execute(
                "SELECT phase, revision, evidence, evidence_hash, mode "
                "FROM finalizer_rollout_state WHERE singleton = 1"
            ).fetchone()
            current_phase = FinalizerRolloutPhase(current)
            if expected_revision is not None and revision != expected_revision:
                raise FinalizerRolloutError("rollout CAS revision differs")
            if FinalizerRolloutMode(mode) is FinalizerRolloutMode.LIVE:
                raise FinalizerRolloutError("live rollout requires separate authorization")
            if self._FORWARD[current_phase] is not phase:
                raise FinalizerRolloutError("illegal rollout transition")
            evidence_hash = hashlib.sha256(evidence.encode("utf-8")).hexdigest()
            changed = connection.execute(
                "UPDATE finalizer_rollout_state SET phase = ?, revision = ?, "
                "evidence = ?, evidence_hash = ? "
                "WHERE singleton = 1 AND revision = ?",
                (phase.value, revision + 1, evidence, evidence_hash, revision),
            ).rowcount
            if changed != 1:
                raise FinalizerRolloutError("rollout CAS update failed")
        return self.snapshot()

    def require_v2_worker_enable(self) -> None:
        if self.snapshot().phase not in {
            FinalizerRolloutPhase.FINALIZER_READY,
            FinalizerRolloutPhase.V2_WORKERS_READY,
            FinalizerRolloutPhase.SCHEDULER_EMITS_V2,
        }:
            raise FinalizerRolloutError("worker V2 requires FINALIZER_READY")

    def require_scheduler_v2_emit(self) -> None:
        if self.snapshot().phase not in {
            FinalizerRolloutPhase.V2_WORKERS_READY,
            FinalizerRolloutPhase.SCHEDULER_EMITS_V2,
        }:
            raise FinalizerRolloutError("scheduler V2 requires V2_WORKERS_READY")

    def begin_rollback(self, *, evidence: str) -> FinalizerRolloutSnapshot:
        """Fence new V2 emits and enter DRAINING under the rollout writer lock."""

        if not evidence:
            raise FinalizerRolloutError("rollout evidence is required")
        try:
            with sqlite3.connect(self._path, timeout=5.0, isolation_level=None) as connection:
                connection.execute("PRAGMA busy_timeout = 5000")
                connection.execute("BEGIN IMMEDIATE")
                current, revision, mode = connection.execute(
                    "SELECT phase, revision, mode FROM finalizer_rollout_state WHERE singleton = 1"
                ).fetchone()
                if FinalizerRolloutPhase(current) is not FinalizerRolloutPhase.SCHEDULER_EMITS_V2:
                    raise FinalizerRolloutError("rollback requires scheduler emit state")
                if FinalizerRolloutMode(mode) is FinalizerRolloutMode.LIVE:
                    raise FinalizerRolloutError("live rollout requires separate authorization")
                evidence_hash = hashlib.sha256(evidence.encode("utf-8")).hexdigest()
                changed = connection.execute(
                    "UPDATE finalizer_rollout_state SET phase = ?, revision = ?, "
                    "evidence = ?, evidence_hash = ? WHERE singleton = 1 AND revision = ?",
                    (
                        FinalizerRolloutPhase.DRAINING.value,
                        revision + 1,
                        evidence,
                        evidence_hash,
                        revision,
                    ),
                ).rowcount
                if changed != 1:
                    raise FinalizerRolloutError("rollout CAS update failed")
        except sqlite3.Error as exc:
            raise FinalizerRolloutError("rollback rollout store is unavailable") from exc
        return self.snapshot()

    def complete_drain(
        self,
        *,
        evidence: str,
        job_store: object,
    ) -> FinalizerRolloutSnapshot:
        if self.snapshot().phase is not FinalizerRolloutPhase.DRAINING:
            raise FinalizerRolloutError("drain completion requires DRAINING")
        require_lab_claim_finalizer_rollout_drain_ready(job_store, self)
        return self.transition(FinalizerRolloutPhase.OFF, evidence=evidence)

    def record_published(
        self,
        *,
        attempt_id: str,
        evidence_hash: str,
        publication_identity: str,
    ) -> None:
        try:
            evidence = FinalizerPublishedEvidence(
                attempt_id=attempt_id,
                evidence_hash=evidence_hash,
                publication_identity=publication_identity,
            )
        except ValueError as exc:
            raise FinalizerRolloutError("published evidence is invalid") from exc
        with sqlite3.connect(self._path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT evidence_hash, publication_identity "
                "FROM finalizer_rollout_published WHERE attempt_id = ?",
                (evidence.attempt_id,),
            ).fetchone()
            if row is not None:
                if row[0] != evidence.evidence_hash or row[1] != evidence.publication_identity:
                    raise FinalizerRolloutError("published attempt evidence differs")
                return
            connection.execute(
                "INSERT INTO finalizer_rollout_published "
                "(attempt_id, evidence_hash, publication_identity, recorded_at) "
                "VALUES (?, ?, ?, ?)",
                (
                    evidence.attempt_id,
                    evidence.evidence_hash,
                    evidence.publication_identity,
                    datetime.now(UTC).isoformat(),
                ),
            )

    def published_count(self) -> int:
        with sqlite3.connect(self._path) as connection:
            return int(
                connection.execute("SELECT COUNT(*) FROM finalizer_rollout_published").fetchone()[0]
            )

    def require_exact_published_evidence(
        self,
        expected: Iterable[LabClaimPublicationRolloutEvidence],
    ) -> None:
        expected_rows: dict[str, tuple[str, str]] = {}
        for item in expected:
            validated = LabClaimPublicationRolloutEvidence.model_validate(item)
            attempt_id = str(validated.attempt_id)
            if attempt_id in expected_rows:
                raise FinalizerRolloutError("published evidence set differs")
            expected_rows[attempt_id] = (
                validated.evidence_hash,
                validated.publication_identity,
            )
        try:
            with sqlite3.connect(self._path) as connection:
                rows = connection.execute(
                    """
                    SELECT attempt_id, evidence_hash, publication_identity
                    FROM finalizer_rollout_published
                    ORDER BY attempt_id
                    """
                ).fetchall()
        except sqlite3.Error as exc:
            raise FinalizerRolloutError("published evidence store is unavailable") from exc
        actual_rows: dict[str, tuple[str, str]] = {}
        for attempt_id, evidence_hash, publication_identity in rows:
            try:
                validated = FinalizerPublishedEvidence(
                    attempt_id=str(attempt_id),
                    evidence_hash=str(evidence_hash),
                    publication_identity=str(publication_identity),
                )
            except ValueError as exc:
                raise FinalizerRolloutError("published evidence set differs") from exc
            actual_rows[validated.attempt_id] = (
                validated.evidence_hash,
                validated.publication_identity,
            )
        if actual_rows != expected_rows:
            raise FinalizerRolloutError("published evidence set differs")


def require_lab_claim_finalizer_rollout_drain_ready(
    job_store: object,
    rollout_store: FinalizerRolloutStore,
) -> None:
    """Fail closed unless local D/outbox state and rollout evidence exactly reconcile."""

    from rquant.lab_jobs import ClaimPublicationConflictError, LabJobStore

    if type(job_store) is not LabJobStore:
        raise TypeError("drain reconciliation requires an exact LabJobStore")
    nonterminal = job_store.count_nonterminal_claim_publications()
    observation_backlog = job_store.count_pending_claim_publication_observation_degradations()
    rollout_backlog = job_store.count_pending_claim_publication_rollout_evidence()
    if nonterminal:
        raise FinalizerRolloutError(f"drain cannot complete: nonterminal_v2={nonterminal}")
    if observation_backlog:
        raise FinalizerRolloutError(
            f"drain cannot complete: observation backlog={observation_backlog}"
        )
    if rollout_backlog:
        raise FinalizerRolloutError(
            f"drain cannot complete: rollout evidence outbox={rollout_backlog}"
        )
    try:
        expected = job_store.list_reconciled_claim_publication_rollout_evidence()
    except ClaimPublicationConflictError as exc:
        raise FinalizerRolloutError("published evidence reconciliation failed") from exc
    rollout_store.require_exact_published_evidence(expected)


@dataclass(frozen=True)
class FinalizerUnitCheck:
    status: Literal["ok", "fail"]
    details: tuple[str, ...] = ()


def verify_lab_claim_finalizer_unit(systemd_root: Path) -> FinalizerUnitCheck:
    path = systemd_root / "rquant-lab-claim-finalizer.service"
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return FinalizerUnitCheck(status="fail", details=("finalizer unit missing",))
    required = (
        "Type=simple",
        "Slice=rquant-research.slice",
        "ExecStart=",
        "lab-claim-finalizer",
        "Restart=on-failure",
        "RuntimeDirectory=",
        "UMask=0077",
        "PrivateTmp=true",
        "ProtectSystem=strict",
        "NoNewPrivileges=true",
        "RestrictAddressFamilies=AF_UNIX",
        "RequiresMountsFor=",
        "ReadWritePaths=",
        "InaccessiblePaths=",
    )
    details = tuple(f"missing {item}" for item in required if item not in text)
    forbidden = ("WatchdogSec=", "rquant-runtime-lab-jobs")
    details += tuple(f"forbidden {item}" for item in forbidden if item in text)
    return FinalizerUnitCheck(status="ok" if not details else "fail", details=details)


__all__ = [
    "FinalizerPreflightCollector",
    "FinalizerPreflightInputs",
    "FinalizerPreflightReport",
    "FinalizerRolloutError",
    "FinalizerRolloutMode",
    "FinalizerRolloutPhase",
    "FinalizerRolloutStore",
    "FinalizerRuntimeError",
    "FinalizerRuntimeInstallRequest",
    "LoadedFinalizerRuntimeGeneration",
    "LabClaimFinalizerGenerationInstaller",
    "inspect_offline_finalizer_certificate",
    "issue_offline_finalizer_certificate",
    "load_offline_finalizer_certificate",
    "load_current_lab_claim_finalizer_generation",
    "read_offline_finalizer_material",
    "require_lab_claim_finalizer_rollout_drain_ready",
    "rotate_offline_finalizer_certificate",
    "run_lab_claim_finalizer_preflight",
    "verify_lab_claim_finalizer_unit",
    "write_offline_finalizer_certificate",
]
