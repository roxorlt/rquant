"""The root-owned Phase C verifier process, its bounded IPC, and its append store.

`RESET-REG-P0-01` puts the trust boundary in the operating system rather than in Python.
This module owns the privileged half of it: the anchored no-follow open of the externally
installed policy and its policy-hashed fixed harness, the eight-step sequence of
authority.md L1409-1449 under one `RuntimeDeploymentLock`, the launch of an unprivileged
generation child that can reach nothing of the root's, the strict bounded IPC in both
directions, and the root-owned SQLite append store that only this process ever writes.

Three properties are load-bearing and are enforced structurally rather than by convention:

* The root process never imports generation code. Neither this module nor anything it
  imports reaches a pair-to-surface module, and the successor and overlay bundles are
  verified by recomputing their canonical preimages from their own bytes rather than by
  constructing the Phase B models, whose resolution imports actual transport classes.
* No append-store descriptor is opened before the child has exited. The store is opened
  inside step 8 and nowhere else.
* Nothing here reads an environment variable or a command-line flag to decide where the
  policy, the harness, or the store live. Those four anchors arrive only through the
  explicit `VerifierAnchors` constructor argument, and the production entry point is the
  one caller that supplies the production values.
"""

from __future__ import annotations

import hashlib
import os
import selectors
import shutil
import sqlite3
import stat
import subprocess
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, Final, Literal, Protocol

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    open_secure_regular_file_lease,
)
from rquant.runtime_authority import (
    RuntimeAuthorityState,
    RuntimeGenerationSlot,
)
from rquant.runtime_service_entrypoint import RuntimeServiceManifest
from rquant.signal_family_verification import (
    HARNESS_IDENTITY,
    MAX_IPC_RESPONSE_BYTES,
    OVERLAY_BUNDLE_RELATIVE_PATH,
    PAIR_IDS,
    SUCCESSOR_BUNDLE_RELATIVE_PATH,
    TEST_MANIFEST_RELATIVE_PATH,
    VERIFICATION_MANIFEST_RELATIVE_PATH,
    VERIFIER_POLICY_PATH,
    AuthoritySnapshotV1,
    ReleaseVerificationEntryV1,
    SignalFamilyAuditEvent,
    SignalFamilyAuditOutcome,
    SignalFamilyChildResultV1,
    SignalFamilyReadinessDecisionV1,
    SignalFamilyReadinessState,
    SignalFamilyReasonCode,
    SignalFamilyReceiptV1,
    SignalFamilyTestManifestV1,
    SignalFamilyVectorV1,
    SignalFamilyVerificationAuditRecordV1,
    SignalFamilyVerificationError,
    SignalFamilyVerificationManifestV1,
    SignalFamilyVerificationSnapshotV1,
    SignalFamilyVerifierPolicyV1,
    build_pair_receipts,
    build_readiness_decision,
    canonical_timestamp,
    expected_result_set_hash,
    five_pair_service_binding_set_hash,
    freshness_seconds,
    observed_result_set_hash,
    participating_service_ids,
    require_pair_derived_surfaces,
    require_readiness_transition,
    resolve_participating_service_manifests,
    service_freshness_seconds,
    vector_set_hash,
    verification_manifest_canonical_json_bytes,
    verifier_policy_canonical_json_bytes,
)
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_canonical_json_loads

# ---------------------------------------------------------------------------------------
# Frozen constants
# ---------------------------------------------------------------------------------------

#: The policy freezes no child deadline, so the verifier owns one (ruling: WP4-b).
CHILD_TIMEOUT_SECONDS: Final[float] = 600.0
#: How long a departed child may leave an IPC pipe held open by a descendant.
PIPE_DRAIN_GRACE_SECONDS: Final[float] = 1.0

PRODUCTION_POLICY_TRUSTED_ROOT: Final[Path] = Path("/")
PRODUCTION_POLICY_PATH: Final[Path] = Path(VERIFIER_POLICY_PATH)
PRODUCTION_HARNESS_PATH: Final[Path] = Path(HARNESS_IDENTITY)
PRODUCTION_STORE_ROOT: Final[Path] = Path("/var/lib/rquant/signal-family-verification")
PRODUCTION_OWNER_UID: Final[int] = 0
PRODUCTION_OWNER_GID: Final[int] = 0

POLICY_FILE_MODE: Final[int] = 0o444
HARNESS_FILE_MODE: Final[int] = 0o555
STORE_DIRECTORY_MODE: Final[int] = 0o700
STORE_FILE_MODE: Final[int] = 0o600
STORE_DATABASE_NAME: Final[str] = "store.sqlite3"

MAX_POLICY_BYTES: Final[int] = 1_048_576
MAX_HARNESS_BYTES: Final[int] = 33_554_432
MAX_GENERATION_DOCUMENT_BYTES: Final[int] = 4_194_304
MAX_GENERATION_SOURCE_BYTES: Final[int] = 8_388_608
MAX_REQUEST_BYTES: Final[int] = 1_048_576
CHILD_REQUEST_ENV_KEY: Final[str] = "RQUANT_SIGNAL_FAMILY_REQUEST_FD"
CHILD_RESULT_ENV_KEY: Final[str] = "RQUANT_SIGNAL_FAMILY_RESULT_FD"

#: The complete environment the unprivileged child receives, key for key. The two file
#: descriptor keys carry numbers, never the request itself, which travels only on the pipe.
SIGNAL_FAMILY_CHILD_ENV_KEYS: Final[tuple[str, ...]] = (
    "HOME",
    "LANG",
    "LC_ALL",
    "PATH",
    "PWD",
    CHILD_REQUEST_ENV_KEY,
    CHILD_RESULT_ENV_KEY,
    "TMPDIR",
)
CHILD_PATH_VALUE: Final[str] = "/usr/bin:/bin"

_GENERATION_DOCUMENTS: Final[tuple[str, ...]] = (
    OVERLAY_BUNDLE_RELATIVE_PATH,
    SUCCESSOR_BUNDLE_RELATIVE_PATH,
    TEST_MANIFEST_RELATIVE_PATH,
    VERIFICATION_MANIFEST_RELATIVE_PATH,
)


class SignalFamilyRootVerifierError(RuntimeError):
    """One bounded rejection. The message never carries a payload or an exception text."""

    def __init__(
        self,
        reason_code: SignalFamilyReasonCode,
        detail: str,
        *,
        audit_record: SignalFamilyVerificationAuditRecordV1 | None = None,
    ) -> None:
        super().__init__(f"{reason_code.value}: {detail}")
        self.reason_code = reason_code
        self.audit_record = audit_record


def _reject(
    reason_code: SignalFamilyReasonCode,
    detail: str,
    *,
    audit_record: SignalFamilyVerificationAuditRecordV1 | None = None,
) -> SignalFamilyRootVerifierError:
    return SignalFamilyRootVerifierError(reason_code, detail, audit_record=audit_record)


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


# ---------------------------------------------------------------------------------------
# Anchors: the only way the verifier learns where anything lives
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class VerifierAnchors:
    """The four fixed filesystem anchors plus the identities they must present.

    `policy_trusted_root` is the directory FD the anchored no-follow walk starts from; in
    production it is `/`, so the walk binds `/`, `/etc`, and `/etc/rquant` exactly as
    authority.md L1284-1287 requires. There is no environment or flag that can move any of
    these; the production entry point is the sole supplier of the production values.
    """

    policy_trusted_root: Path
    policy_path: Path
    harness_path: Path
    store_root: Path
    expected_owner_uid: int
    expected_owner_gid: int
    child_uid: int
    child_gid: int

    def __post_init__(self) -> None:
        for label, value in (
            ("policy_trusted_root", self.policy_trusted_root),
            ("policy_path", self.policy_path),
            ("harness_path", self.harness_path),
            ("store_root", self.store_root),
        ):
            if not isinstance(value, Path) or not value.is_absolute():
                raise ValueError(f"{label} must be one absolute path")
            if value != Path(os.path.abspath(value)):
                raise ValueError(f"{label} must be one absolute canonical path")
        for label, value in (
            ("policy_path", self.policy_path),
            ("harness_path", self.harness_path),
        ):
            if self.policy_trusted_root not in value.parents:
                raise ValueError(f"{label} must live beneath the anchored trusted root")
        if self.store_root in (self.policy_path.parent, self.harness_path.parent):
            raise ValueError("the anchors must not share the store root")
        for label, value in (
            ("expected_owner_uid", self.expected_owner_uid),
            ("expected_owner_gid", self.expected_owner_gid),
            ("child_uid", self.child_uid),
            ("child_gid", self.child_gid),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{label} must be a non-negative integer")


@dataclass(frozen=True)
class GenerationAuthoritySnapshot:
    """One reopened authority view: the record, the validated slot, and the closure."""

    operation_id: str
    sequence: int
    authority_state: RuntimeAuthorityState
    slot: RuntimeGenerationSlot
    profile_manifests: tuple[RuntimeServiceManifest, ...]
    full_manifest_entries: Mapping[str, str]

    def identity(self) -> tuple[Any, ...]:
        """Everything step 7 compares between the first and the second reopen."""

        return (
            self.operation_id,
            self.sequence,
            self.authority_state.value,
            self.slot.lifecycle.value,
            self.slot.generation_id,
            str(self.slot.generation_path),
            self.slot.commit,
            self.slot.full_manifest_hash,
            self.slot.profile_id,
            tuple(
                (name, role.module, str(role.python_path))
                for name, role in sorted(self.slot.roles.items())
            ),
            tuple(
                manifest.manifest_fingerprint
                for manifest in sorted(
                    self.profile_manifests,
                    key=lambda manifest: manifest.service_id,
                )
            ),
            tuple(sorted(self.full_manifest_entries.items())),
        )


class DeploymentLockHandle(Protocol):
    """The two operations the verifier may perform; the lock exposes no descriptor."""

    def assert_current(self) -> None: ...

    def close(self) -> None: ...


class RuntimeAuthorityGateway(Protocol):
    """Reopen the deployment lock and the validated authority snapshot."""

    def acquire_deployment_lock(self) -> DeploymentLockHandle: ...

    def load_snapshot(self) -> GenerationAuthoritySnapshot: ...


# ---------------------------------------------------------------------------------------
# The bounded IPC request and the child launch contract
# ---------------------------------------------------------------------------------------


def derive_run_id(
    *,
    authority_epoch_key: str,
    overlay_content_hash: str,
    test_manifest_hash: str,
    vector_set_hash: str,
) -> str:
    """One deterministic run identity, so an identical replay reproduces it exactly."""

    return _canonical_sha256(
        {
            "authority_epoch_key": authority_epoch_key,
            "overlay_content_hash": overlay_content_hash,
            "test_manifest_hash": test_manifest_hash,
            "vector_set_hash": vector_set_hash,
        }
    )


def build_child_request(
    *,
    run_id: str,
    test_manifest_hash: str,
    vectors: Sequence[SignalFamilyVectorV1],
) -> bytes:
    """The request carries vector identity and input bytes, never an expected result."""

    payload = {
        "schema_version": 1,
        "run_id": run_id,
        "test_manifest_hash": test_manifest_hash,
        "vectors": [vector.model_dump(mode="json") for vector in vectors],
    }
    raw = canonical_json_bytes(payload)
    if len(raw) > MAX_REQUEST_BYTES:
        raise _reject(
            SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
            "the child request exceeds its bounded size",
        )
    return raw


def build_child_argv(interpreter: Path, harness_path: Path) -> tuple[str, str, str]:
    """The fixed argv: the generation-local interpreter, isolation, and the harness."""

    return (str(interpreter), "-I", str(harness_path))


def child_environment(*, cwd: Path, request_fd: int, result_fd: int) -> dict[str, str]:
    """The sanitized fixed environment. Nothing is inherited from the root process."""

    environment = {
        "HOME": str(cwd),
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": CHILD_PATH_VALUE,
        "PWD": str(cwd),
        CHILD_REQUEST_ENV_KEY: str(request_fd),
        CHILD_RESULT_ENV_KEY: str(result_fd),
        "TMPDIR": str(cwd),
    }
    if tuple(sorted(environment)) != SIGNAL_FAMILY_CHILD_ENV_KEYS:  # pragma: no cover
        raise _reject(
            SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
            "the child environment drifted from its frozen allowlist",
        )
    return environment


@dataclass(frozen=True)
class ChildPrivilegePlan:
    """The exact identity change the child performs between fork and exec."""

    setresgid: tuple[int, int, int]
    setresuid: tuple[int, int, int]
    clear_supplementary_groups: bool


def child_privilege_plan(
    *,
    current_uid: int,
    current_gid: int,
    target_uid: int,
    target_gid: int,
) -> ChildPrivilegePlan | None:
    """Derive the drop, or refuse. A root verifier may never hand root to the child.

    Returning `None` means the verifier already runs as the target identity, which is the
    only case in which no drop is possible and none is needed. Any other mismatch is a
    launch that would either keep privilege or need privilege the verifier does not hold.
    """

    for label, value in (
        ("current_uid", current_uid),
        ("current_gid", current_gid),
        ("target_uid", target_uid),
        ("target_gid", target_gid),
    ):
        if type(value) is not int or value < 0:
            raise _reject(
                SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                f"{label} must be a non-negative integer",
            )
    if current_uid == 0:
        if target_uid == 0 or target_gid == 0:
            raise _reject(
                SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                "the generation child may never run as root",
            )
        return ChildPrivilegePlan(
            setresgid=(target_gid, target_gid, target_gid),
            setresuid=(target_uid, target_uid, target_uid),
            clear_supplementary_groups=True,
        )
    if (current_uid, current_gid) != (target_uid, target_gid):
        raise _reject(
            SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
            "an unprivileged verifier cannot launch another identity",
        )
    return None


def _child_preexec(
    plan: ChildPrivilegePlan | None,
    *,
    keep: tuple[int, ...],
) -> Callable[[], None]:
    """Drop privilege and close every descriptor `close_fds` would leave behind.

    `subprocess` closes inherited descriptors after this callback, so the `closerange`
    sweep is the belt-and-braces half of the pair: even if the interpreter's own sweep
    were bypassed, nothing but the standard streams and the two IPC pipes survives.
    """

    retained = tuple(sorted(set(keep)))
    limit = max(os.sysconf("SC_OPEN_MAX") if hasattr(os, "sysconf") else 4096, 4096)

    def apply() -> None:
        if plan is not None:
            if plan.clear_supplementary_groups:
                os.setgroups([])
            os.setresgid(*plan.setresgid)
            os.setresuid(*plan.setresuid)
        low = 3
        for descriptor in retained:
            if descriptor > low:
                os.closerange(low, descriptor)
            low = max(low, descriptor + 1)
        os.closerange(low, limit)

    return apply


@dataclass
class _ChildOutcome:
    result_bytes: bytes
    stdout: bytes
    stderr: bytes
    returncode: int
    timed_out: bool
    pipes_open: bool
    oversized: bool


# ---------------------------------------------------------------------------------------
# The root-owned append store. Nothing but this class ever writes it.
# ---------------------------------------------------------------------------------------

_SCHEMA: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS receipts (
        overlay_content_hash TEXT NOT NULL,
        authority_epoch_key TEXT NOT NULL,
        pair_id TEXT NOT NULL,
        receipt_fingerprint TEXT NOT NULL,
        receipt_json TEXT NOT NULL,
        recorded_at TEXT NOT NULL,
        UNIQUE (overlay_content_hash, authority_epoch_key, pair_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS decisions (
        overlay_content_hash TEXT NOT NULL,
        authority_epoch_key TEXT NOT NULL,
        decision_hash TEXT NOT NULL,
        decision_json TEXT NOT NULL,
        recorded_at TEXT NOT NULL,
        UNIQUE (overlay_content_hash, authority_epoch_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS readiness_state (
        overlay_content_hash TEXT NOT NULL,
        authority_epoch_key TEXT NOT NULL,
        state TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (overlay_content_hash, authority_epoch_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS conflict_audit (
        record_hash TEXT NOT NULL,
        record_json TEXT NOT NULL,
        recorded_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit (
        record_hash TEXT NOT NULL,
        record_json TEXT NOT NULL,
        recorded_at TEXT NOT NULL
    )
    """,
)


class SignalFamilyVerificationStore:
    """The append store. Every table is insert-only except one compare-and-swap column."""

    __slots__ = ("_connection", "_root", "_path")

    def __init__(self, *, connection: sqlite3.Connection, root: Path, path: Path) -> None:
        self._connection = connection
        self._root = root
        self._path = path

    @classmethod
    def open(cls, store_root: Path, *, owner_uid: int) -> SignalFamilyVerificationStore:
        """Bind the store directory and database, both private to the owning identity."""

        if not isinstance(store_root, Path) or not store_root.is_absolute():
            raise _reject(
                SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                "the store root must be one absolute path",
            )
        try:
            observed: os.stat_result | None = os.stat(store_root, follow_symlinks=False)
        except FileNotFoundError:
            observed = None
        except OSError as error:
            raise _reject(
                SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                "the store root is unavailable",
            ) from error
        if observed is None:
            if os.geteuid() != owner_uid:
                raise _reject(
                    SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                    "the store root is absent and cannot be created for its owner",
                )
            store_root.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            os.mkdir(store_root, STORE_DIRECTORY_MODE)
            os.chmod(store_root, STORE_DIRECTORY_MODE)
            observed = os.stat(store_root, follow_symlinks=False)
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != owner_uid
            or stat.S_IMODE(observed.st_mode) != STORE_DIRECTORY_MODE
        ):
            raise _reject(
                SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                "the store root owner, type, or mode is unsafe",
            )
        path = store_root / STORE_DATABASE_NAME
        if path.is_symlink():
            raise _reject(
                SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                "the store database is not a regular file",
            )
        previous_mask = os.umask(0o177)
        try:
            connection = sqlite3.connect(str(path), timeout=60.0, isolation_level=None)
        except sqlite3.Error as error:
            raise _reject(
                SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                "the store database cannot be opened",
            ) from error
        finally:
            os.umask(previous_mask)
        try:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("PRAGMA busy_timeout = 60000")
            connection.execute("PRAGMA foreign_keys = ON")
            for statement in _SCHEMA:
                connection.execute(statement)
        except sqlite3.Error as error:
            connection.close()
            raise _reject(
                SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                "the store schema cannot be established",
            ) from error
        os.chmod(path, STORE_FILE_MODE)
        database = os.stat(path, follow_symlinks=False)
        if (
            not stat.S_ISREG(database.st_mode)
            or database.st_uid != owner_uid
            or stat.S_IMODE(database.st_mode) != STORE_FILE_MODE
        ):
            connection.close()
            raise _reject(
                SignalFamilyReasonCode.STORE_ANCHOR_INVALID,
                "the store database owner, type, or mode is unsafe",
            )
        return cls(connection=connection, root=store_root, path=path)

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> SignalFamilyVerificationStore:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    # -- reads ---------------------------------------------------------------------

    def readiness_state(
        self,
        *,
        overlay_content_hash: str,
        authority_epoch_key: str,
    ) -> SignalFamilyReadinessState | None:
        row = self._connection.execute(
            "SELECT state FROM readiness_state "
            "WHERE overlay_content_hash = ? AND authority_epoch_key = ?",
            (overlay_content_hash, authority_epoch_key),
        ).fetchone()
        return None if row is None else SignalFamilyReadinessState(row[0])

    # -- appends -------------------------------------------------------------------

    def append_audit(self, record: SignalFamilyVerificationAuditRecordV1) -> None:
        self._append_record("audit", record)

    def append_conflict(self, record: SignalFamilyVerificationAuditRecordV1) -> None:
        self._append_record("conflict_audit", record)

    def _append_record(
        self,
        table: str,
        record: SignalFamilyVerificationAuditRecordV1,
    ) -> None:
        if type(record) is not SignalFamilyVerificationAuditRecordV1:
            raise TypeError("an audit row requires an exact audit record object")
        self._connection.execute(
            f"INSERT INTO {table} (record_hash, record_json, recorded_at) VALUES (?, ?, ?)",
            (
                record.record_hash,
                canonical_json_bytes(record.model_dump(mode="json")).decode("utf-8"),
                record.recorded_at,
            ),
        )

    def finalize(
        self,
        *,
        receipts: Sequence[SignalFamilyReceiptV1],
        decision: SignalFamilyReadinessDecisionV1,
        recorded_at: datetime,
    ) -> tuple[Literal["persisted", "idempotent"], bytes, SignalFamilyReadinessState]:
        """Declare, append the five receipts and the decision, and swap state, atomically.

        The whole transaction runs under `BEGIN IMMEDIATE` from one consistent snapshot,
        so a concurrent identical finalization observes the stored bytes and returns them
        unchanged, while a divergent one appends conflict evidence and rejects.
        """

        for receipt in receipts:
            if type(receipt) is not SignalFamilyReceiptV1:
                raise TypeError("a receipt row requires an exact receipt object")
        if type(decision) is not SignalFamilyReadinessDecisionV1:
            raise TypeError("a decision row requires an exact decision object")
        ordered = tuple(sorted(receipts, key=lambda receipt: receipt.pair_id))
        if tuple(receipt.pair_id for receipt in ordered) != PAIR_IDS:
            raise _reject(
                SignalFamilyReasonCode.PAIR_SET_INCOMPLETE,
                "one transaction persists exactly the five frozen pair receipts",
            )
        overlay = decision.overlay_content_hash
        epoch = decision.authority_epoch_key
        stamp = canonical_timestamp(recorded_at)
        decision_bytes = canonical_json_bytes(decision.model_dump(mode="json"))
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            self._connection.execute(
                "INSERT OR IGNORE INTO readiness_state "
                "(overlay_content_hash, authority_epoch_key, state, updated_at) "
                "VALUES (?, ?, ?, ?)",
                (overlay, epoch, SignalFamilyReadinessState.DECLARED.value, stamp),
            )
            existing = self._connection.execute(
                "SELECT decision_json FROM decisions "
                "WHERE overlay_content_hash = ? AND authority_epoch_key = ?",
                (overlay, epoch),
            ).fetchone()
            if existing is not None:
                stored = existing[0].encode("utf-8")
                if stored == decision_bytes:
                    self._connection.execute("COMMIT")
                    state = self.readiness_state(
                        overlay_content_hash=overlay,
                        authority_epoch_key=epoch,
                    )
                    assert state is not None
                    return ("idempotent", stored, state)
                conflict = _conflict_record(
                    event=SignalFamilyAuditEvent.DECISION_FINALIZED,
                    reason_code=SignalFamilyReasonCode.DECISION_CONFLICT,
                    recorded_at=recorded_at,
                    overlay_content_hash=overlay,
                    authority_epoch_key=epoch,
                    verifier_policy_content_hash=decision.verifier_policy_content_hash,
                    selected_entry_hash=decision.selected_entry_hash,
                    existing_hash=_canonical_sha256(
                        strict_canonical_json_loads(stored)
                    ),
                    attempted_hash=decision.decision_hash,
                )
                self.append_conflict(conflict)
                self._connection.execute("COMMIT")
                raise _reject(
                    SignalFamilyReasonCode.DECISION_CONFLICT,
                    "a divergent decision already occupies this overlay and epoch",
                    audit_record=conflict,
                )
            for receipt in ordered:
                stored_receipt = self._connection.execute(
                    "SELECT receipt_json FROM receipts WHERE overlay_content_hash = ? "
                    "AND authority_epoch_key = ? AND pair_id = ?",
                    (overlay, epoch, receipt.pair_id),
                ).fetchone()
                payload = canonical_json_bytes(receipt.model_dump(mode="json"))
                if stored_receipt is not None:
                    if stored_receipt[0].encode("utf-8") == payload:
                        continue
                    conflict = _conflict_record(
                        event=SignalFamilyAuditEvent.RECEIPT_APPENDED,
                        reason_code=SignalFamilyReasonCode.RECEIPT_CONFLICT,
                        recorded_at=recorded_at,
                        pair_id=receipt.pair_id,
                        overlay_content_hash=overlay,
                        authority_epoch_key=epoch,
                        verifier_policy_content_hash=receipt.verifier_policy_content_hash,
                        selected_entry_hash=receipt.selected_entry_hash,
                        existing_hash=_canonical_sha256(
                            strict_canonical_json_loads(stored_receipt[0])
                        ),
                        attempted_hash=receipt.receipt_fingerprint,
                    )
                    self.append_conflict(conflict)
                    self._connection.execute("COMMIT")
                    raise _reject(
                        SignalFamilyReasonCode.RECEIPT_CONFLICT,
                        "a divergent receipt already occupies this pair key",
                        audit_record=conflict,
                    )
                self._connection.execute(
                    "INSERT INTO receipts (overlay_content_hash, authority_epoch_key, "
                    "pair_id, receipt_fingerprint, receipt_json, recorded_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        overlay,
                        epoch,
                        receipt.pair_id,
                        receipt.receipt_fingerprint,
                        payload.decode("utf-8"),
                        stamp,
                    ),
                )
            self._connection.execute(
                "INSERT INTO decisions (overlay_content_hash, authority_epoch_key, "
                "decision_hash, decision_json, recorded_at) VALUES (?, ?, ?, ?, ?)",
                (
                    overlay,
                    epoch,
                    decision.decision_hash,
                    decision_bytes.decode("utf-8"),
                    stamp,
                ),
            )
            swapped = self._connection.execute(
                "UPDATE readiness_state SET state = ?, updated_at = ? "
                "WHERE overlay_content_hash = ? AND authority_epoch_key = ? AND state = ?",
                (
                    SignalFamilyReadinessState.READY.value,
                    stamp,
                    overlay,
                    epoch,
                    SignalFamilyReadinessState.DECLARED.value,
                ),
            ).rowcount
            if swapped != 1:
                raise _reject(
                    SignalFamilyReasonCode.READINESS_TRANSITION_INVALID,
                    "the readiness compare-and-swap did not observe a declared key",
                )
            for event in (
                SignalFamilyAuditEvent.READINESS_DECLARED,
                SignalFamilyAuditEvent.DECISION_FINALIZED,
            ):
                self.append_audit(
                    SignalFamilyVerificationAuditRecordV1.create(
                        event=event,
                        outcome=SignalFamilyAuditOutcome.ACCEPTED,
                        reason_code=None,
                        recorded_at=recorded_at,
                        overlay_content_hash=overlay,
                        authority_epoch_key=epoch,
                        verifier_policy_content_hash=decision.verifier_policy_content_hash,
                        selected_entry_hash=decision.selected_entry_hash,
                        subject_hash=decision.decision_hash,
                    )
                )
            self._connection.execute("COMMIT")
        except BaseException:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            raise
        return ("persisted", decision_bytes, SignalFamilyReadinessState.READY)

    def transition(
        self,
        *,
        overlay_content_hash: str,
        authority_epoch_key: str,
        target: SignalFamilyReadinessState,
        recorded_at: datetime,
        event: SignalFamilyAuditEvent,
    ) -> SignalFamilyReadinessState:
        """Append-only revoke or rollback. History is never deleted, only superseded."""

        stamp = canonical_timestamp(recorded_at)
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            row = self._connection.execute(
                "SELECT state FROM readiness_state "
                "WHERE overlay_content_hash = ? AND authority_epoch_key = ?",
                (overlay_content_hash, authority_epoch_key),
            ).fetchone()
            if row is None:
                raise _reject(
                    SignalFamilyReasonCode.READINESS_TRANSITION_INVALID,
                    "no readiness record exists for this overlay and epoch",
                )
            current = SignalFamilyReadinessState(row[0])
            try:
                require_readiness_transition(current, target)
            except SignalFamilyVerificationError as error:
                raise _reject(
                    SignalFamilyReasonCode.READINESS_TRANSITION_INVALID,
                    "the requested readiness transition is not allowed",
                ) from error
            swapped = self._connection.execute(
                "UPDATE readiness_state SET state = ?, updated_at = ? "
                "WHERE overlay_content_hash = ? AND authority_epoch_key = ? AND state = ?",
                (target.value, stamp, overlay_content_hash, authority_epoch_key, current.value),
            ).rowcount
            if swapped != 1:  # pragma: no cover - the immediate transaction serializes
                raise _reject(
                    SignalFamilyReasonCode.READINESS_TRANSITION_INVALID,
                    "the readiness compare-and-swap lost its expected state",
                )
            self.append_audit(
                SignalFamilyVerificationAuditRecordV1.create(
                    event=event,
                    outcome=SignalFamilyAuditOutcome.ACCEPTED,
                    reason_code=None,
                    recorded_at=recorded_at,
                    overlay_content_hash=overlay_content_hash,
                    authority_epoch_key=authority_epoch_key,
                )
            )
            self._connection.execute("COMMIT")
        except BaseException:
            if self._connection.in_transaction:
                self._connection.execute("ROLLBACK")
            raise
        return target


def _conflict_record(
    *,
    event: SignalFamilyAuditEvent,
    reason_code: SignalFamilyReasonCode,
    recorded_at: datetime,
    existing_hash: str,
    attempted_hash: str,
    pair_id: str | None = None,
    overlay_content_hash: str | None = None,
    authority_epoch_key: str | None = None,
    verifier_policy_content_hash: str | None = None,
    selected_entry_hash: str | None = None,
) -> SignalFamilyVerificationAuditRecordV1:
    return SignalFamilyVerificationAuditRecordV1.create(
        event=event,
        outcome=SignalFamilyAuditOutcome.REJECTED,
        reason_code=reason_code,
        recorded_at=recorded_at,
        pair_id=pair_id,
        overlay_content_hash=overlay_content_hash,
        authority_epoch_key=authority_epoch_key,
        verifier_policy_content_hash=verifier_policy_content_hash,
        selected_entry_hash=selected_entry_hash,
        existing_hash=existing_hash,
        attempted_hash=attempted_hash,
    )


# ---------------------------------------------------------------------------------------
# Anchored reads
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _AnchoredFile:
    payload: bytes
    sha256: str
    device: int
    inode: int


def _read_anchored(
    path: Path,
    *,
    trusted_root: Path,
    owner_uid: int,
    owner_gid: int,
    mode: int,
    max_bytes: int,
    reason: SignalFamilyReasonCode,
) -> _AnchoredFile:
    """Anchored no-follow open with a full post-open identity recheck of every component."""

    try:
        lease = open_secure_regular_file_lease(
            path,
            trusted_root=trusted_root,
            allowed_ancestor_uids=frozenset({owner_uid}),
            expected_uid=owner_uid,
            expected_gid=owner_gid,
            allowed_final_uids=frozenset({owner_uid}),
            allowed_final_gids=frozenset({owner_gid}),
            allowed_modes=frozenset({mode}),
            max_bytes=max_bytes,
        )
    except AuthorityPathSecurityError as error:
        raise _reject(reason, "the anchored path identity is unsafe") from error
    with lease:
        payload = lease.read_all(max_bytes=max_bytes)
        metadata = lease.metadata
        lease.require_unchanged()
    return _AnchoredFile(
        payload=payload,
        sha256=hashlib.sha256(payload).hexdigest(),
        device=metadata.device,
        inode=metadata.inode,
    )


def _validate_relative(value: str) -> tuple[str, ...]:
    if type(value) is not str or not value or "\\" in value:
        raise _reject(
            SignalFamilyReasonCode.BINDING_WRONG_PATH,
            "a generation-relative path must be a normalized POSIX path",
        )
    if PurePosixPath(value).is_absolute():
        raise _reject(
            SignalFamilyReasonCode.BINDING_WRONG_PATH,
            "a generation-relative path cannot be absolute",
        )
    parts = tuple(value.split("/"))
    if any(part in ("", ".", "..") for part in parts):
        raise _reject(
            SignalFamilyReasonCode.BINDING_WRONG_PATH,
            "a generation-relative path cannot contain an empty, dot, or parent component",
        )
    return parts


def _read_generation_file(
    generation_path: Path,
    relative: str,
    *,
    max_bytes: int,
    reason: SignalFamilyReasonCode,
) -> bytes:
    """Resolve beneath the selected generation with no traversal and no symlink escape."""

    parts = _validate_relative(relative)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    descriptors: list[int] = []
    try:
        try:
            parent = os.open(generation_path, directory_flags)
        except OSError as error:
            raise _reject(reason, "the selected generation is unavailable") from error
        descriptors.append(parent)
        for component in parts[:-1]:
            try:
                child = os.open(component, directory_flags, dir_fd=parent)
            except OSError as error:
                raise _reject(reason, "a generation path component is unavailable") from error
            descriptors.append(child)
            parent = child
        name = parts[-1]
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent,
            )
        except OSError as error:
            raise _reject(reason, "the generation file is unavailable") from error
        descriptors.append(descriptor)
        observed = os.fstat(descriptor)
        if not stat.S_ISREG(observed.st_mode) or observed.st_size > max_bytes:
            raise _reject(reason, "the generation file is not a bounded regular file")
        payload = b""
        while chunk := os.read(descriptor, 65536):
            payload += chunk
            if len(payload) > max_bytes:
                raise _reject(reason, "the generation file is oversized")
        return payload
    finally:
        for descriptor in reversed(descriptors):
            with suppress(OSError):  # pragma: no cover - descriptors are freshly opened
                os.close(descriptor)


def _decode_hashed_document(
    payload: bytes,
    *,
    hash_field: str,
    reason: SignalFamilyReasonCode,
) -> dict[str, Any]:
    """Recompute a Phase B canonical content hash from the document's own bytes.

    The root never constructs the Phase B bundle models: resolving a successor channel
    imports the actual transport class, and the root process imports no generation code.
    Recomputing the exact `canonical_sha256(dump excluding <hash_field>)` preimage from
    the raw bytes derives the same value without loading a single generation module.
    """

    try:
        decoded = strict_canonical_json_loads(payload)
    except StrictJsonError as error:
        raise _reject(reason, "the generation document is not canonical") from error
    if type(decoded) is not dict or hash_field not in decoded:
        raise _reject(reason, "the generation document is not a hashed object")
    declared = decoded[hash_field]
    body = {key: value for key, value in decoded.items() if key != hash_field}
    if type(declared) is not str or declared != _canonical_sha256(body):
        raise _reject(reason, "the generation document hash does not match its content")
    return decoded


# ---------------------------------------------------------------------------------------
# One verifier run
# ---------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _PolicySnapshot:
    policy: SignalFamilyVerifierPolicyV1
    raw_sha256: str
    device: int
    inode: int
    harness_sha256: str
    harness_device: int
    harness_inode: int


@dataclass(frozen=True)
class VerifierRunResult:
    """What one successful run produced, or what an identical replay already had."""

    outcome: Literal["persisted", "idempotent"]
    state: SignalFamilyReadinessState
    receipts: tuple[SignalFamilyReceiptV1, ...]
    decision: SignalFamilyReadinessDecisionV1
    decision_bytes: bytes


class RootVerifier:
    """The privileged half of `signal_family_verification`."""

    def __init__(
        self,
        *,
        anchors: VerifierAnchors,
        authority_gateway: RuntimeAuthorityGateway,
        clock: Callable[[], datetime] | None = None,
        child_timeout_seconds: float = CHILD_TIMEOUT_SECONDS,
    ) -> None:
        if type(anchors) is not VerifierAnchors:
            raise TypeError("the verifier requires exact VerifierAnchors")
        if type(child_timeout_seconds) is not float or child_timeout_seconds <= 0:
            raise ValueError("the child deadline must be a positive number of seconds")
        self._anchors = anchors
        self._gateway = authority_gateway
        self._clock = clock if clock is not None else (lambda: datetime.now(UTC))
        self._child_timeout_seconds = child_timeout_seconds

    # -- steps 1 and 7: the external anchors ---------------------------------------

    def _load_policy(self) -> _PolicySnapshot:
        policy_file = _read_anchored(
            self._anchors.policy_path,
            trusted_root=self._anchors.policy_trusted_root,
            owner_uid=self._anchors.expected_owner_uid,
            owner_gid=self._anchors.expected_owner_gid,
            mode=POLICY_FILE_MODE,
            max_bytes=MAX_POLICY_BYTES,
            reason=SignalFamilyReasonCode.POLICY_ANCHOR_INVALID,
        )
        raw = policy_file.payload
        try:
            decoded = strict_canonical_json_loads(raw)
        except StrictJsonError as error:
            raise _reject(
                SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                "the policy bytes are not strict canonical JSON",
            ) from error
        if type(decoded) is not dict or "content_hash" not in decoded:
            raise _reject(
                SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                "the policy is not a hashed object",
            )
        body = {key: value for key, value in decoded.items() if key != "content_hash"}
        if decoded["content_hash"] != _canonical_sha256(body):
            raise _reject(
                SignalFamilyReasonCode.POLICY_CONTENT_HASH_MISMATCH,
                "the policy content hash does not match its canonical content",
            )
        self._scan_release_entries(decoded.get("release_entries"))
        try:
            policy = SignalFamilyVerifierPolicyV1.from_canonical_json(raw)
        except (SignalFamilyVerificationError, ValueError, TypeError) as error:
            raise _reject(
                SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                "the policy does not satisfy its frozen schema",
            ) from error
        if raw != verifier_policy_canonical_json_bytes(policy):
            raise _reject(
                SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                "the policy bytes are not the canonical encoding of its model",
            )
        if policy.harness_identity != HARNESS_IDENTITY:  # pragma: no cover - Literal pinned
            raise _reject(
                SignalFamilyReasonCode.HARNESS_IDENTITY_MISMATCH,
                "the policy names another fixed harness identity",
            )
        harness = _read_anchored(
            self._anchors.harness_path,
            trusted_root=self._anchors.policy_trusted_root,
            owner_uid=self._anchors.expected_owner_uid,
            owner_gid=self._anchors.expected_owner_gid,
            mode=HARNESS_FILE_MODE,
            max_bytes=MAX_HARNESS_BYTES,
            reason=SignalFamilyReasonCode.POLICY_ANCHOR_INVALID,
        )
        if harness.sha256 != policy.harness_sha256:
            raise _reject(
                SignalFamilyReasonCode.HARNESS_HASH_MISMATCH,
                "the fixed harness bytes are not the ones the policy authorizes",
            )
        return _PolicySnapshot(
            policy=policy,
            raw_sha256=policy_file.sha256,
            device=policy_file.device,
            inode=policy_file.inode,
            harness_sha256=harness.sha256,
            harness_device=harness.device,
            harness_inode=harness.inode,
        )

    @staticmethod
    def _scan_release_entries(entries: object) -> None:
        """Structural entry rules, so a duplicate and a conflict get distinct codes."""

        if type(entries) is not list or not entries:
            raise _reject(
                SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                "the policy release entries must be a nonempty array",
            )
        keys: list[tuple[str, str]] = []
        for entry in entries:
            if type(entry) is not dict:
                raise _reject(
                    SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                    "a policy release entry is not an object",
                )
            successor = entry.get("successor_bundle_content_hash")
            overlay = entry.get("overlay_content_hash")
            if type(successor) is not str or type(overlay) is not str:
                raise _reject(
                    SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                    "a policy release entry does not name its release key",
                )
            keys.append((successor, overlay))
        if keys != sorted(keys):
            raise _reject(
                SignalFamilyReasonCode.POLICY_BYTES_NONCANONICAL,
                "the policy release entries are not sorted by release key",
            )
        for index, key in enumerate(keys):
            for other in range(index + 1, len(keys)):
                if keys[other] != key:
                    continue
                if entries[index] == entries[other]:
                    raise _reject(
                        SignalFamilyReasonCode.ENTRY_MULTIPLE,
                        "the policy repeats one release key",
                    )
                raise _reject(
                    SignalFamilyReasonCode.ENTRY_CONFLICTING,
                    "the policy holds conflicting entries for one release key",
                )

    # -- step 4 and 5: the unprivileged child --------------------------------------

    def _run_child(
        self,
        *,
        interpreter: Path,
        harness_path: Path,
        request: bytes,
    ) -> bytes:
        plan = child_privilege_plan(
            current_uid=os.geteuid(),
            current_gid=os.getegid(),
            target_uid=self._anchors.child_uid,
            target_gid=self._anchors.child_gid,
        )
        request_read, request_write = os.pipe()
        result_read, result_write = os.pipe()
        cwd = Path(tempfile.mkdtemp(prefix="rquant-signal-family-child-"))
        process: subprocess.Popen[bytes] | None = None
        try:
            os.chmod(cwd, 0o700)
            if plan is not None:
                os.chown(cwd, self._anchors.child_uid, self._anchors.child_gid)
            try:
                process = subprocess.Popen(  # noqa: S603 - fixed argv, sanitized env
                    build_child_argv(interpreter, harness_path),
                    cwd=str(cwd),
                    env=child_environment(
                        cwd=cwd,
                        request_fd=request_read,
                        result_fd=result_write,
                    ),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    close_fds=True,
                    pass_fds=(request_read, result_write),
                    preexec_fn=_child_preexec(plan, keep=(request_read, result_write)),
                )
            except OSError as error:
                raise _reject(
                    SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                    "the generation child could not be launched",
                ) from error
            os.close(request_read)
            request_read = -1
            os.close(result_write)
            result_write = -1
            pump_write, request_write = request_write, -1
            pump_read, result_read = result_read, -1
            outcome = self._pump(
                process=process,
                request_write=pump_write,
                request=request,
                result_read=pump_read,
            )
            self._require_clean_exit(outcome)
            return outcome.result_bytes
        finally:
            for descriptor in (request_read, request_write, result_read, result_write):
                if descriptor >= 0:
                    with suppress(OSError):  # pragma: no cover - already closed
                        os.close(descriptor)
            if process is not None and process.poll() is None:  # pragma: no cover
                process.kill()
                process.wait(timeout=10)
            shutil.rmtree(cwd, ignore_errors=True)

    def _pump(
        self,
        *,
        process: subprocess.Popen[bytes],
        request_write: int,
        request: bytes,
        result_read: int,
    ) -> _ChildOutcome:
        assert process.stdout is not None and process.stderr is not None
        stdout_fd = process.stdout.fileno()
        stderr_fd = process.stderr.fileno()
        streams = {
            result_read: bytearray(),
            stdout_fd: bytearray(),
            stderr_fd: bytearray(),
        }
        selector = selectors.DefaultSelector()
        os.set_blocking(request_write, False)
        for descriptor in streams:
            os.set_blocking(descriptor, False)
            selector.register(descriptor, selectors.EVENT_READ)
        offset = 0
        if request:
            selector.register(request_write, selectors.EVENT_WRITE)
        else:  # pragma: no cover - the request is never empty
            os.close(request_write)
            request_write = -1
        deadline = time.monotonic() + self._child_timeout_seconds
        drain_deadline: float | None = None
        oversized = False
        timed_out = False
        try:
            while selector.get_map():
                now = time.monotonic()
                if process.poll() is not None and drain_deadline is None:
                    drain_deadline = now + PIPE_DRAIN_GRACE_SECONDS
                effective = deadline if drain_deadline is None else min(deadline, drain_deadline)
                remaining = effective - now
                if remaining <= 0:
                    timed_out = drain_deadline is None
                    break
                for key, mask in selector.select(timeout=min(remaining, 0.2)):
                    descriptor = int(key.fileobj)  # type: ignore[arg-type]
                    if mask & selectors.EVENT_WRITE:
                        try:
                            offset += os.write(descriptor, request[offset:])
                        except BlockingIOError:  # pragma: no cover - retried next loop
                            continue
                        except BrokenPipeError:
                            selector.unregister(descriptor)
                            os.close(descriptor)
                            request_write = -1
                            continue
                        if offset >= len(request):
                            selector.unregister(descriptor)
                            os.close(descriptor)
                            request_write = -1
                        continue
                    try:
                        chunk = os.read(descriptor, 65536)
                    except BlockingIOError:  # pragma: no cover - retried next loop
                        continue
                    if not chunk:
                        selector.unregister(descriptor)
                        continue
                    buffer = streams[descriptor]
                    buffer += chunk
                    if descriptor == result_read and len(buffer) > MAX_IPC_RESPONSE_BYTES:
                        oversized = True
                        break
                if oversized:
                    process.kill()
                    break
        finally:
            pipes_open = bool(selector.get_map())
            for key in list(selector.get_map().values()):
                selector.unregister(key.fileobj)
            selector.close()
            if request_write >= 0:
                with suppress(OSError):  # pragma: no cover - already closed
                    os.close(request_write)
            with suppress(OSError):  # pragma: no cover - already closed
                os.close(result_read)
        if timed_out:
            process.kill()
        try:
            returncode = process.wait(timeout=30)
        except subprocess.TimeoutExpired:  # pragma: no cover - the kill above precedes it
            timed_out = True
            process.kill()
            returncode = process.wait(timeout=30)
        process.stdout.close()
        process.stderr.close()
        return _ChildOutcome(
            result_bytes=bytes(streams[result_read]),
            stdout=bytes(streams[stdout_fd]),
            stderr=bytes(streams[stderr_fd]),
            returncode=returncode,
            timed_out=timed_out,
            pipes_open=pipes_open and not timed_out and not oversized,
            oversized=oversized,
        )

    @staticmethod
    def _require_clean_exit(outcome: _ChildOutcome) -> None:
        if outcome.oversized:
            raise _reject(
                SignalFamilyReasonCode.CHILD_RESULT_OVERSIZED,
                "the generation child response exceeds its bounded size",
            )
        if outcome.timed_out:
            raise _reject(
                SignalFamilyReasonCode.CHILD_TIMEOUT,
                "the generation child exceeded its bounded deadline",
            )
        if outcome.returncode < 0:
            raise _reject(
                SignalFamilyReasonCode.CHILD_SIGNAL_DEATH,
                "the generation child died on a signal",
            )
        if outcome.pipes_open:
            raise _reject(
                SignalFamilyReasonCode.CHILD_DESCRIPTOR_MISMATCH,
                "the generation child left an inherited pipe open",
            )
        if outcome.returncode != 0:
            raise _reject(
                SignalFamilyReasonCode.CHILD_NONZERO_EXIT,
                "the generation child exited with a nonzero status",
            )
        if outcome.stdout or outcome.stderr:
            raise _reject(
                SignalFamilyReasonCode.CHILD_EXTRA_OUTPUT,
                "the generation child emitted output beyond its one IPC response",
            )

    # -- the sequence ---------------------------------------------------------------

    def run(self) -> VerifierRunResult:
        """The eight steps of authority.md L1409-1449, in that order and no other."""

        lock = self._gateway.acquire_deployment_lock()
        try:
            # 1. The external anchors, before any generation file is opened.
            self._assert_lock(lock)
            policy_snapshot = self._load_policy()

            # 2. The authority snapshot and exactly one matching release entry.
            authority = self._gateway.load_snapshot()
            generation = authority.slot.generation_path
            successor_bytes = _read_generation_file(
                generation,
                SUCCESSOR_BUNDLE_RELATIVE_PATH,
                max_bytes=MAX_GENERATION_DOCUMENT_BYTES,
                reason=SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
            )
            overlay_bytes = _read_generation_file(
                generation,
                OVERLAY_BUNDLE_RELATIVE_PATH,
                max_bytes=MAX_GENERATION_DOCUMENT_BYTES,
                reason=SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
            )
            successor = _decode_hashed_document(
                successor_bytes,
                hash_field="content_hash",
                reason=SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
            )
            overlay = _decode_hashed_document(
                overlay_bytes,
                hash_field="content_hash",
                reason=SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
            )
            successor_hash = str(successor["content_hash"])
            overlay_hash = str(overlay["content_hash"])
            entry = self._select_entry(policy_snapshot.policy, successor_hash, overlay_hash)

            # 3. The immutable manifests, the bindings, and the derived arithmetic.
            plan = self._load_generation_plan(
                authority=authority,
                entry=entry,
                successor=successor,
                overlay=overlay,
            )

            # 4 and 5. The unprivileged child.
            run_id = derive_run_id(
                authority_epoch_key=plan.authority.authority_epoch_key,
                overlay_content_hash=overlay_hash,
                test_manifest_hash=plan.test_manifest_sha256,
                vector_set_hash=entry.vector_set_hash,
            )
            request = build_child_request(
                run_id=run_id,
                test_manifest_hash=plan.test_manifest_sha256,
                vectors=plan.test_manifest.vectors,
            )
            raw_result = self._run_child(
                interpreter=plan.interpreter,
                harness_path=self._anchors.harness_path,
                request=request,
            )

            # 6. The root's own decode and revalidation. Child claims replace nothing.
            child_result = self._validate_child_result(
                raw_result,
                run_id=run_id,
                test_manifest=plan.test_manifest,
                test_manifest_sha256=plan.test_manifest_sha256,
                entry=entry,
            )
            revalidated = self._load_generation_plan(
                authority=authority,
                entry=entry,
                successor=successor,
                overlay=overlay,
            )
            if revalidated.identity != plan.identity:
                raise _reject(
                    SignalFamilyReasonCode.AUTHORITY_EPOCH_CHANGED,
                    "the generation revalidation diverged from the initial snapshot",
                )

            # 7. Still under the lock: reopen the anchors and the authority.
            self._assert_lock(lock)
            reopened_policy = self._load_policy()
            if (
                reopened_policy.raw_sha256 != policy_snapshot.raw_sha256
                or reopened_policy.policy.content_hash != policy_snapshot.policy.content_hash
                or (reopened_policy.device, reopened_policy.inode)
                != (policy_snapshot.device, policy_snapshot.inode)
                or reopened_policy.harness_sha256 != policy_snapshot.harness_sha256
                or (reopened_policy.harness_device, reopened_policy.harness_inode)
                != (policy_snapshot.harness_device, policy_snapshot.harness_inode)
            ):
                raise _reject(
                    SignalFamilyReasonCode.POLICY_CHANGED_DURING_RUN,
                    "the external policy or its fixed harness changed during the run",
                )
            reopened_entry = self._select_entry(
                reopened_policy.policy,
                successor_hash,
                overlay_hash,
            )
            if reopened_entry.entry_hash != entry.entry_hash:
                raise _reject(
                    SignalFamilyReasonCode.ENTRY_STALE,
                    "the selected release entry changed during the run",
                )
            reopened_authority = self._gateway.load_snapshot()
            if reopened_authority.identity() != authority.identity():
                raise _reject(
                    SignalFamilyReasonCode.AUTHORITY_EPOCH_CHANGED,
                    "the runtime authority changed between the child and the append",
                )
            self._assert_lock(lock)

            # 8. Only now may the store be opened, and only by the root process.
            verified_at = self._clock()
            snapshot = SignalFamilyVerificationSnapshotV1.create(
                authority=plan.authority,
                overlay_content_hash=overlay_hash,
                successor_bundle_content_hash=successor_hash,
                successor_declaration_hashes=plan.declaration_hashes,
                successor_channel_hashes=plan.channel_hashes,
                verification_manifest_sha256=plan.verification_manifest_sha256,
                test_manifest_hash=plan.test_manifest_sha256,
                profile_manifests=authority.profile_manifests,
                test_manifest=plan.test_manifest,
                child_result=child_result,
                policy=policy_snapshot.policy,
                selected_entry=entry,
                verified_at=verified_at,
                freshness_seconds=plan.freshness_seconds,
            )
            receipts = build_pair_receipts(snapshot)
            decision = build_readiness_decision(snapshot, receipts)
            with SignalFamilyVerificationStore.open(
                self._anchors.store_root,
                owner_uid=self._anchors.expected_owner_uid,
            ) as store:
                outcome, decision_bytes, state = store.finalize(
                    receipts=receipts,
                    decision=decision,
                    recorded_at=verified_at,
                )
            return VerifierRunResult(
                outcome=outcome,
                state=state,
                receipts=receipts,
                decision=decision,
                decision_bytes=decision_bytes,
            )
        finally:
            lock.close()

    @staticmethod
    def _assert_lock(lock: DeploymentLockHandle) -> None:
        try:
            lock.assert_current()
        except Exception as error:
            raise _reject(
                SignalFamilyReasonCode.DEPLOYMENT_LOCK_LOST,
                "the deployment lock identity changed during the run",
            ) from error

    # -- revocation and rollback -----------------------------------------------------

    def revoke(
        self,
        *,
        overlay_content_hash: str,
        authority_epoch_key: str,
    ) -> SignalFamilyReadinessState:
        return self._transition(
            overlay_content_hash=overlay_content_hash,
            authority_epoch_key=authority_epoch_key,
            target=SignalFamilyReadinessState.REVOKED,
            event=SignalFamilyAuditEvent.READINESS_REVOKED,
        )

    def rollback(
        self,
        *,
        overlay_content_hash: str,
        authority_epoch_key: str,
    ) -> SignalFamilyReadinessState:
        return self._transition(
            overlay_content_hash=overlay_content_hash,
            authority_epoch_key=authority_epoch_key,
            target=SignalFamilyReadinessState.ROLLED_BACK,
            event=SignalFamilyAuditEvent.READINESS_ROLLED_BACK,
        )

    def readiness_state(
        self,
        *,
        overlay_content_hash: str,
        authority_epoch_key: str,
    ) -> SignalFamilyReadinessState | None:
        with SignalFamilyVerificationStore.open(
            self._anchors.store_root,
            owner_uid=self._anchors.expected_owner_uid,
        ) as store:
            return store.readiness_state(
                overlay_content_hash=overlay_content_hash,
                authority_epoch_key=authority_epoch_key,
            )

    def _transition(
        self,
        *,
        overlay_content_hash: str,
        authority_epoch_key: str,
        target: SignalFamilyReadinessState,
        event: SignalFamilyAuditEvent,
    ) -> SignalFamilyReadinessState:
        lock = self._gateway.acquire_deployment_lock()
        try:
            with SignalFamilyVerificationStore.open(
                self._anchors.store_root,
                owner_uid=self._anchors.expected_owner_uid,
            ) as store:
                return store.transition(
                    overlay_content_hash=overlay_content_hash,
                    authority_epoch_key=authority_epoch_key,
                    target=target,
                    recorded_at=self._clock(),
                    event=event,
                )
        finally:
            lock.close()

    # -- step 3 in detail -------------------------------------------------------------

    @staticmethod
    def _select_entry(
        policy: SignalFamilyVerifierPolicyV1,
        successor_hash: str,
        overlay_hash: str,
    ) -> ReleaseVerificationEntryV1:
        try:
            return policy.select_entry(
                successor_bundle_content_hash=successor_hash,
                overlay_content_hash=overlay_hash,
            )
        except SignalFamilyVerificationError as error:
            raise _reject(
                SignalFamilyReasonCode.ENTRY_MISSING,
                "no external policy entry authorizes this exact release",
            ) from error

    def _load_generation_plan(
        self,
        *,
        authority: GenerationAuthoritySnapshot,
        entry: ReleaseVerificationEntryV1,
        successor: Mapping[str, Any],
        overlay: Mapping[str, Any],
    ) -> _GenerationPlan:
        generation = authority.slot.generation_path
        verification_bytes = _read_generation_file(
            generation,
            VERIFICATION_MANIFEST_RELATIVE_PATH,
            max_bytes=MAX_GENERATION_DOCUMENT_BYTES,
            reason=SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
        )
        verification_sha256 = hashlib.sha256(verification_bytes).hexdigest()
        if verification_sha256 != entry.verification_manifest_sha256:
            raise _reject(
                SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                "the in-generation verification manifest is not the one the policy names",
            )
        try:
            verification_manifest = SignalFamilyVerificationManifestV1.from_canonical_json(
                verification_bytes
            )
        except (StrictJsonError, SignalFamilyVerificationError, ValueError, TypeError) as error:
            raise _reject(
                SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                "the in-generation verification manifest is not canonical",
            ) from error
        if verification_bytes != verification_manifest_canonical_json_bytes(
            verification_manifest
        ):
            raise _reject(
                SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                "the verification manifest bytes are not its canonical encoding",
            )
        test_bytes = _read_generation_file(
            generation,
            TEST_MANIFEST_RELATIVE_PATH,
            max_bytes=MAX_GENERATION_DOCUMENT_BYTES,
            reason=SignalFamilyReasonCode.TEST_MANIFEST_HASH_MISMATCH,
        )
        test_sha256 = hashlib.sha256(test_bytes).hexdigest()
        if test_sha256 != verification_manifest.test_manifest_sha256:
            raise _reject(
                SignalFamilyReasonCode.TEST_MANIFEST_HASH_MISMATCH,
                "the immutable test manifest is not the one the verification manifest names",
            )
        if (
            verification_manifest.successor_bundle_content_hash
            != successor["content_hash"]
            or verification_manifest.overlay_content_hash != overlay["content_hash"]
        ):
            raise _reject(
                SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                "the verification manifest names another successor bundle or overlay",
            )
        try:
            test_manifest = SignalFamilyTestManifestV1.from_canonical_json(test_bytes)
        except (StrictJsonError, SignalFamilyVerificationError, ValueError, TypeError) as error:
            raise _reject(
                SignalFamilyReasonCode.TEST_MANIFEST_HASH_MISMATCH,
                "the immutable test manifest is not canonical",
            ) from error
        derived_vectors = vector_set_hash(test_manifest.vectors)
        if derived_vectors != entry.vector_set_hash:
            raise _reject(
                SignalFamilyReasonCode.VECTOR_SET_HASH_MISMATCH,
                "the recomputed vector set hash is not the one the policy authorizes",
            )
        derived_expected = expected_result_set_hash(test_manifest.expected_results)
        if derived_expected != entry.expected_result_set_hash:
            raise _reject(
                SignalFamilyReasonCode.EXPECTED_RESULT_SET_HASH_MISMATCH,
                "the recomputed expected result set hash is not the one the policy authorizes",
            )
        derived_pairs = five_pair_service_binding_set_hash(
            authority.profile_manifests,
            test_manifest.service_bindings,
        )
        if derived_pairs != entry.five_pair_service_binding_set_hash:
            raise _reject(
                SignalFamilyReasonCode.FIVE_PAIR_SET_HASH_MISMATCH,
                "the recomputed five pair and binding set hash is not policy authorized",
            )
        for relative, payload in (
            (SUCCESSOR_BUNDLE_RELATIVE_PATH, canonical_json_bytes(successor)),
            (OVERLAY_BUNDLE_RELATIVE_PATH, canonical_json_bytes(overlay)),
            (VERIFICATION_MANIFEST_RELATIVE_PATH, verification_bytes),
            (TEST_MANIFEST_RELATIVE_PATH, test_bytes),
        ):
            self._require_manifested(
                authority,
                relative,
                hashlib.sha256(payload).hexdigest(),
            )
        self._validate_bindings(authority, test_manifest)
        participating = participating_service_ids(authority.profile_manifests)
        resolved = resolve_participating_service_manifests(
            authority.profile_manifests,
            participating,
        )
        service_freshness = service_freshness_seconds(resolved)
        interpreters = {
            role.python_path for role in authority.slot.roles.values()
        }
        if len(interpreters) != 1:
            raise _reject(
                SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                "the generation does not name one interpreter for every role",
            )
        declaration_hashes, channel_hashes = self._overlay_bindings(successor, overlay)
        authority_snapshot = AuthoritySnapshotV1.create(
            operation_id=authority.operation_id,
            sequence=authority.sequence,
            authority_state=authority.authority_state,
            generation_id=authority.slot.generation_id,
            generation_lifecycle=authority.slot.lifecycle,
            full_manifest_hash=authority.slot.full_manifest_hash,
            profile_id=authority.slot.profile_id,
            role_names=tuple(sorted(authority.slot.roles)),
        )
        return _GenerationPlan(
            authority=authority_snapshot,
            verification_manifest_sha256=verification_sha256,
            test_manifest=test_manifest,
            test_manifest_sha256=test_sha256,
            declaration_hashes=declaration_hashes,
            channel_hashes=channel_hashes,
            freshness_seconds=freshness_seconds(
                service_freshness,
                entry.verifier_policy_max_age_seconds,
            ),
            interpreter=next(iter(interpreters)),
            identity=(
                verification_sha256,
                test_sha256,
                derived_vectors,
                derived_expected,
                derived_pairs,
                declaration_hashes,
                channel_hashes,
                authority_snapshot.authority_epoch_key,
            ),
        )

    @staticmethod
    def _require_manifested(
        authority: GenerationAuthoritySnapshot,
        relative: str,
        sha256: str,
    ) -> None:
        declared = authority.full_manifest_entries.get(relative)
        if declared != sha256:
            raise _reject(
                SignalFamilyReasonCode.BINDING_UNMANIFESTED,
                "a generation file is absent from the full manifest source closure",
            )

    @staticmethod
    def _overlay_bindings(
        successor: Mapping[str, Any],
        overlay: Mapping[str, Any],
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """Every overlay declaration binds its own hash and the channel hash it names."""

        channels = successor.get("channels")
        declarations = overlay.get("declarations")
        if type(channels) is not list or type(declarations) is not list or not declarations:
            raise _reject(
                SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                "the successor or overlay bundle does not declare its channels",
            )
        by_id: dict[str, Mapping[str, Any]] = {}
        for channel in channels:
            if type(channel) is not dict or "channel_hash" not in channel:
                raise _reject(
                    SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                    "a successor channel is not a hashed object",
                )
            body = {key: value for key, value in channel.items() if key != "channel_hash"}
            if channel["channel_hash"] != _canonical_sha256(body):
                raise _reject(
                    SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                    "a successor channel hash does not match its canonical content",
                )
            by_id[str(channel["channel_id"])] = channel
        declaration_hashes: list[str] = []
        channel_hashes: list[str] = []
        for declaration in declarations:
            if type(declaration) is not dict or "declaration_hash" not in declaration:
                raise _reject(
                    SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                    "an overlay declaration is not a hashed object",
                )
            body = {
                key: value for key, value in declaration.items() if key != "declaration_hash"
            }
            if declaration["declaration_hash"] != _canonical_sha256(body):
                raise _reject(
                    SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                    "an overlay declaration hash does not match its canonical content",
                )
            channel = by_id.get(str(declaration.get("channel_id")))
            if channel is None:
                raise _reject(
                    SignalFamilyReasonCode.VERIFICATION_MANIFEST_HASH_MISMATCH,
                    "an overlay declaration names a channel the successor base omits",
                )
            declaration_hashes.append(str(declaration["declaration_hash"]))
            channel_hashes.append(str(channel["channel_hash"]))
        return (
            tuple(sorted(set(declaration_hashes))),
            tuple(sorted(set(channel_hashes))),
        )

    def _validate_bindings(
        self,
        authority: GenerationAuthoritySnapshot,
        test_manifest: SignalFamilyTestManifestV1,
    ) -> None:
        bindings = test_manifest.service_bindings
        participating = participating_service_ids(authority.profile_manifests)
        service_ids = tuple(binding.service_id for binding in bindings)
        if list(service_ids) != sorted(service_ids):
            raise _reject(
                SignalFamilyReasonCode.BINDING_DUPLICATE,
                "the service bindings are not sorted by service id",
            )
        if len(set(service_ids)) != len(service_ids):
            raise _reject(
                SignalFamilyReasonCode.BINDING_DUPLICATE,
                "the service bindings repeat one service id",
            )
        if len({binding.binding_hash for binding in bindings}) != len(bindings):
            raise _reject(
                SignalFamilyReasonCode.BINDING_DUPLICATE,
                "the service bindings repeat one binding hash",
            )
        if service_ids != participating:
            raise _reject(
                SignalFamilyReasonCode.BINDING_MISSING,
                "the service bindings do not cover exactly the pair derived participants",
            )
        by_service = {
            manifest.service_id: manifest for manifest in authority.profile_manifests
        }
        roles_by_kind: dict[str, str] = {}
        kinds_by_role: dict[str, str] = {}
        for binding in bindings:
            manifest = by_service.get(binding.service_id)
            if manifest is None:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_MISSING,
                    "a binding names a service the validated profile does not declare",
                )
            if binding.runtime_service_kind is not manifest.service_kind:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_WRONG_KIND,
                    "a binding declares another runtime service kind",
                )
            if binding.service_manifest_fingerprint != manifest.manifest_fingerprint:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_MISSING,
                    "a binding names no exact manifest in the validated profile",
                )
            if binding.role_name not in authority.slot.roles:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_CROSS_ROLE,
                    "a binding names a role the generation slot does not assign",
                )
            kind = binding.runtime_service_kind.value
            if roles_by_kind.setdefault(kind, binding.role_name) != binding.role_name:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_CROSS_ROLE,
                    "one runtime service kind declares two different roles",
                )
            if kinds_by_role.setdefault(binding.role_name, kind) != kind:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_CROSS_ROLE,
                    "one role is shared by two different runtime service kinds",
                )
        for binding in bindings:
            role = authority.slot.roles[binding.role_name]
            if role.module != binding.executable_module:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_WRONG_MODULE,
                    "a binding executable module is not its slot role module",
                )
            source = _read_generation_file(
                authority.slot.generation_path,
                binding.executable_source_relative_path,
                max_bytes=MAX_GENERATION_SOURCE_BYTES,
                reason=SignalFamilyReasonCode.BINDING_WRONG_PATH,
            )
            observed = hashlib.sha256(source).hexdigest()
            if observed != binding.executable_source_sha256:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_WRONG_SOURCE_HASH,
                    "a binding executable source hash is not the generation source hash",
                )
            self._require_manifested(
                authority,
                binding.executable_source_relative_path,
                observed,
            )
            try:
                require_pair_derived_surfaces(
                    authority.profile_manifests,
                    binding.service_id,
                    binding.surface_ids,
                )
            except SignalFamilyVerificationError as error:
                raise _reject(
                    SignalFamilyReasonCode.BINDING_SURFACE_MISMATCH,
                    "a binding omits or adds a surface of the frozen allowlist",
                ) from error

    # -- step 6 in detail -------------------------------------------------------------

    @staticmethod
    def _validate_child_result(
        raw: bytes,
        *,
        run_id: str,
        test_manifest: SignalFamilyTestManifestV1,
        test_manifest_sha256: str,
        entry: ReleaseVerificationEntryV1,
    ) -> SignalFamilyChildResultV1:
        try:
            result = SignalFamilyChildResultV1.from_canonical_ipc_bytes(
                raw,
                max_vector_count=len(test_manifest.vectors),
            )
        except (SignalFamilyVerificationError, ValueError, TypeError) as error:
            raise _reject(
                SignalFamilyReasonCode.CHILD_RESULT_NONCANONICAL,
                "the child response does not satisfy its frozen bounded schema",
            ) from error
        if raw != canonical_json_bytes(result.model_dump(mode="json")):
            raise _reject(
                SignalFamilyReasonCode.CHILD_RESULT_NONCANONICAL,
                "the child response bytes are not the canonical encoding of its model",
            )
        if result.run_id != run_id or result.test_manifest_hash != test_manifest_sha256:
            raise _reject(
                SignalFamilyReasonCode.CHILD_RESULT_IDENTITY_MISMATCH,
                "the child response names a run or test manifest the root did not derive",
            )
        expected_identity = {
            (
                vector.vector_id,
                vector.pair_id,
                vector.family_id,
                vector.surface_id.value,
            )
            for vector in test_manifest.vectors
        }
        observed_identity = {
            (row.vector_id, row.pair_id, row.family_id, row.surface_id.value)
            for row in result.vector_results
        }
        if observed_identity != expected_identity:
            raise _reject(
                SignalFamilyReasonCode.CHILD_RESULT_IDENTITY_MISMATCH,
                "the child response does not cover exactly the policy authorized vectors",
            )
        if observed_result_set_hash(result.vector_results) != entry.expected_result_set_hash:
            raise _reject(
                SignalFamilyReasonCode.RESULT_SET_HASH_MISMATCH,
                "the recomputed result set hash is not the one the policy authorizes",
            )
        return result


class ProductionRuntimeAuthorityGateway:
    """Reopen the real deployment lock, authority record, slot, closure, and profile.

    The spec never freezes how the validated production profile's `RuntimeServiceManifest`
    tuple reaches a root process that may not import a builder, so this gateway reads it
    from one root-owned canonical generation document and requires its raw SHA-256 to
    equal `RuntimeGenerationSlot.profile_id`. That binding is as strong as the manifest
    and source-closure bindings around it, and it keeps every generation module out of
    the root interpreter. See the WP4-b report for the open spec question.
    """

    __slots__ = ()

    def acquire_deployment_lock(self) -> DeploymentLockHandle:
        from rquant.runtime_authority import acquire_runtime_deployment_lock

        return acquire_runtime_deployment_lock()

    def load_snapshot(self) -> GenerationAuthoritySnapshot:
        from rquant.runtime_authority import (
            GENERATION_MANIFEST_NAME,
            load_runtime_authority,
        )

        record = load_runtime_authority()
        slot = record.current
        manifest_payload = _read_generation_file(
            slot.generation_path,
            GENERATION_MANIFEST_NAME,
            max_bytes=MAX_HARNESS_BYTES,
            reason=SignalFamilyReasonCode.BINDING_UNMANIFESTED,
        )
        entries = _parse_full_manifest_entries(manifest_payload)
        profile_payload = _read_generation_file(
            slot.generation_path,
            PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH,
            max_bytes=MAX_GENERATION_DOCUMENT_BYTES,
            reason=SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
        )
        if hashlib.sha256(profile_payload).hexdigest() != slot.profile_id:
            raise _reject(
                SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
                "the profile service manifest document is not the validated profile",
            )
        return GenerationAuthoritySnapshot(
            operation_id=record.operation_id,
            sequence=record.sequence,
            authority_state=record.state,
            slot=slot,
            profile_manifests=_parse_profile_manifests(profile_payload),
            full_manifest_entries=entries,
        )


#: The root-owned canonical document that carries the validated profile's service
#: manifests into the root process without importing a production builder.
PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH: Final[str] = (
    "signal-family/profile-service-manifests-v1.json"
)


def _parse_full_manifest_entries(payload: bytes) -> Mapping[str, str]:
    try:
        decoded = strict_canonical_json_loads(payload)
    except StrictJsonError as error:
        raise _reject(
            SignalFamilyReasonCode.BINDING_UNMANIFESTED,
            "the full generation manifest is not canonical",
        ) from error
    entries = decoded.get("entries") if type(decoded) is dict else None
    if type(entries) is not list:
        raise _reject(
            SignalFamilyReasonCode.BINDING_UNMANIFESTED,
            "the full generation manifest declares no entries",
        )
    closure: dict[str, str] = {}
    for entry in entries:
        if type(entry) is not dict:
            raise _reject(
                SignalFamilyReasonCode.BINDING_UNMANIFESTED,
                "a full generation manifest entry is not an object",
            )
        path = entry.get("path")
        digest = entry.get("sha256")
        if type(path) is not str or digest is None:
            continue
        if type(digest) is not str or path in closure:
            raise _reject(
                SignalFamilyReasonCode.BINDING_UNMANIFESTED,
                "a full generation manifest entry is duplicated or malformed",
            )
        closure[path] = digest
    return closure


def _parse_profile_manifests(payload: bytes) -> tuple[RuntimeServiceManifest, ...]:
    try:
        decoded = strict_canonical_json_loads(payload)
    except StrictJsonError as error:
        raise _reject(
            SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
            "the profile service manifest document is not canonical",
        ) from error
    rows = decoded.get("service_manifests") if type(decoded) is dict else None
    if type(rows) is not list or not rows:
        raise _reject(
            SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
            "the profile service manifest document declares no manifests",
        )
    try:
        return tuple(RuntimeServiceManifest.model_validate(row) for row in rows)
    except (ValueError, TypeError) as error:
        raise _reject(
            SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
            "a profile service manifest does not satisfy its frozen schema",
        ) from error


@dataclass(frozen=True)
class _GenerationPlan:
    authority: AuthoritySnapshotV1
    verification_manifest_sha256: str
    test_manifest: SignalFamilyTestManifestV1
    test_manifest_sha256: str
    declaration_hashes: tuple[str, ...]
    channel_hashes: tuple[str, ...]
    freshness_seconds: float
    interpreter: Path
    identity: tuple[Any, ...]
