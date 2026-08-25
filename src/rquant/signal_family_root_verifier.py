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
    ALLOWED_READINESS_TRANSITIONS,
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
    missing_pair_surface_coverage,
    observed_result_set_hash,
    participating_service_ids,
    require_pair_derived_surfaces,
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
#: WP4-c round 1. The child's empty private cwd is created inside this root-owned `0700`
#: directory instead of the process default temp root. `runtime_serving_authority` refuses
#: an authority whose ancestry holds a group- or world-writable node, and the production
#: default temp root is a sticky `1777` `/tmp`, which made three reader surfaces
#: unreachable from inside the isolated cwd. The workspace holds no evidence and no
#: capability: it is a parent directory, never the store.
PRODUCTION_CHILD_WORKSPACE_ROOT: Final[Path] = Path(
    "/var/lib/rquant/signal-family-verifier-workspace"
)
#: Root-owned, `r-x` for everyone else. Two earlier values were both unusable in production,
#: where the child runs as `lighthouse` while this directory stays owned by root:
#:
#: * `0700` (round 1) denied traversal outright — every absolute path under the workspace was
#:   `EACCES` the moment the privilege drop landed;
#: * `0711` (fix round 1) granted traversal but not `open`. The two ancestry walks the child
#:   must pass — `runtime_serving_authority._open_existing_directory_chain` and the
#:   same-shaped walk in `signal_route_spool` — open *every* component with
#:   `O_RDONLY | O_DIRECTORY`, and `O_RDONLY` on a directory needs the read bit. All eight
#:   reader surfaces failed under it.
#:
#: `0715` grants read and execute and no write, so both of those walks accept it and so does
#: this module's own ancestry rule. The cost is that the child may `ls` this one directory
#: and see the random per-run names beside its own; each run directory is itself `0700` and
#: owned by that child, and the verifier holds `RuntimeDeploymentLock` throughout, so there
#: is normally nothing beside it to see.
CHILD_WORKSPACE_MODE: Final[int] = 0o715
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
    "DATA_DIR",
    "DUCKDB_PATH",
    "HOME",
    "LANG",
    "LC_ALL",
    "LOG_DIR",
    "PARQUET_DIR",
    "PATH",
    "PWD",
    CHILD_REQUEST_ENV_KEY,
    CHILD_RESULT_ENV_KEY,
    "TMPDIR",
    "TUSHARE_TOKEN_MAIN",
)
CHILD_PATH_VALUE: Final[str] = "/usr/bin:/bin"

#: WP4-c round 1. `rquant.config` constructs a process-wide `Settings` at import time, and
#: every production builder that reaches a notification provider or a serving authority
#: imports it, so a child without these five keys cannot enter those builders at all. The
#: root chooses every value: the four paths are anchored inside the child's own cwd, and the
#: credential slot carries a fixed non-credential literal. Nothing is read from the caller's
#: environment, so no operator configuration and no real token can reach the child.
CHILD_CONFIGURATION_PATH_ENV_KEYS: Final[tuple[str, ...]] = (
    "DATA_DIR",
    "DUCKDB_PATH",
    "LOG_DIR",
    "PARQUET_DIR",
)
CHILD_ABSENT_CREDENTIAL_VALUE: Final[str] = "rquant-signal-family-verifier-no-credential"
CHILD_CONFIGURATION_ENV_KEYS: Final[tuple[str, ...]] = (
    *CHILD_CONFIGURATION_PATH_ENV_KEYS,
    "TUSHARE_TOKEN_MAIN",
)

_GENERATION_DOCUMENTS: Final[tuple[str, ...]] = (
    OVERLAY_BUNDLE_RELATIVE_PATH,
    SUCCESSOR_BUNDLE_RELATIVE_PATH,
    TEST_MANIFEST_RELATIVE_PATH,
    VERIFICATION_MANIFEST_RELATIVE_PATH,
)


def _rejection_events() -> Mapping[SignalFamilyReasonCode, SignalFamilyAuditEvent]:
    """Which step each bounded reason code belongs to, for its rejection audit record."""

    code = SignalFamilyReasonCode
    event = SignalFamilyAuditEvent
    by_prefix: tuple[tuple[str, SignalFamilyAuditEvent], ...] = (
        ("POLICY_", event.POLICY_VALIDATED),
        ("HARNESS_", event.POLICY_VALIDATED),
        ("STORE_", event.RECEIPT_APPENDED),
        ("ENTRY_", event.ENTRY_SELECTED),
        ("FULL_MANIFEST_", event.MANIFEST_VALIDATED),
        ("VERIFICATION_MANIFEST_", event.MANIFEST_VALIDATED),
        ("TEST_MANIFEST_", event.MANIFEST_VALIDATED),
        ("VECTOR_SET_", event.MANIFEST_VALIDATED),
        ("EXPECTED_RESULT_SET_", event.MANIFEST_VALIDATED),
        ("FIVE_PAIR_", event.MANIFEST_VALIDATED),
        ("BINDING_", event.BINDING_VALIDATED),
        ("PAIR_SET_", event.BINDING_VALIDATED),
        ("PAIR_SURFACE_", event.CHILD_RESULT_VALIDATED),
        ("PARTICIPANT_", event.BINDING_VALIDATED),
        ("CHILD_LAUNCH", event.CHILD_LAUNCHED),
        ("CHILD_", event.CHILD_RESULT_VALIDATED),
        ("RESULT_SET_", event.CHILD_RESULT_VALIDATED),
        ("AUTHORITY_", event.AUTHORITY_REVALIDATED),
        ("DEPLOYMENT_LOCK_", event.AUTHORITY_REVALIDATED),
        ("RECEIPT_", event.RECEIPT_APPENDED),
        ("DECISION_", event.DECISION_FINALIZED),
        ("READINESS_", event.READINESS_DECLARED),
    )
    mapping: dict[SignalFamilyReasonCode, SignalFamilyAuditEvent] = {}
    for member in code:
        for prefix, chosen in by_prefix:
            if member.value.startswith(prefix):
                mapping[member] = chosen
                break
        else:  # pragma: no cover - the prefix table covers the closed enum
            raise RuntimeError(f"no audit event covers the reason code: {member.value}")
    return mapping


_REJECTION_EVENTS: Final[Mapping[SignalFamilyReasonCode, SignalFamilyAuditEvent]] = (
    _rejection_events()
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
    child_workspace_root: Path
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
            ("child_workspace_root", self.child_workspace_root),
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
        # The child owns its cwd, so the workspace may never be the store or contain it.
        if self.child_workspace_root == self.store_root:
            raise ValueError("the child workspace must not be the store root")
        if self.child_workspace_root in self.store_root.parents:
            raise ValueError("the child workspace must not contain the store root")
        if self.store_root in self.child_workspace_root.parents:
            raise ValueError("the child workspace must not live beneath the store root")
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
    full_manifest_sha256: str
    profile_document_sha256: str

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
            self.full_manifest_sha256,
            self.profile_document_sha256,
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

    data_directory = cwd / "data"
    environment = {
        "DATA_DIR": str(data_directory),
        "DUCKDB_PATH": str(data_directory / "rquant.duckdb"),
        "HOME": str(cwd),
        "LANG": "C",
        "LC_ALL": "C",
        "LOG_DIR": str(cwd / "logs"),
        "PARQUET_DIR": str(data_directory / "parquet"),
        "PATH": CHILD_PATH_VALUE,
        "PWD": str(cwd),
        CHILD_REQUEST_ENV_KEY: str(request_fd),
        CHILD_RESULT_ENV_KEY: str(result_fd),
        "TMPDIR": str(cwd),
        "TUSHARE_TOKEN_MAIN": CHILD_ABSENT_CREDENTIAL_VALUE,
    }
    if tuple(sorted(environment)) != SIGNAL_FAMILY_CHILD_ENV_KEYS:  # pragma: no cover
        raise _reject(
            SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
            "the child environment drifted from its frozen allowlist",
        )
    return environment


def open_child_workspace_root(
    root: Path,
    *,
    expected_uid: int,
    child_uid: int,
    child_gid: int,
) -> Path:
    """Create and validate the private parent the child's cwd is carved out of.

    The walk is anchored and no-follow from `/`, and it applies the same trust rule
    `rquant.runtime_serving_authority` applies to an authority root: every ancestor must be
    a real directory owned by root or by this process, with no group or other write bit. A
    child cwd whose ancestry fails that rule cannot host a serving authority, which is what
    made three reader surfaces unreachable before WP4-c round 1.

    The leaf is `CHILD_WORKSPACE_MODE`, whose value carries the whole production argument;
    see its definition. `child_uid` and `child_gid` decide which permission class the child
    lands in, and POSIX stops at the first class that matches: a child that shares the
    leaf's group is judged by the group bits and never reaches the more generous `other`
    ones, so that pairing is refused rather than silently losing the read bit.
    """

    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        os.mkdir(root, CHILD_WORKSPACE_MODE)
    except FileExistsError:
        # An existing workspace is validated, never normalized: silently repairing a mode
        # someone else set would hide exactly the misconfiguration this walk exists to catch.
        pass
    except OSError as error:
        raise _reject(
            SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
            "the child workspace root could not be created",
        ) from error
    else:
        # `os.mkdir` masks its mode with the process umask, which the verifier does not own.
        os.chmod(root, CHILD_WORKSPACE_MODE)
    descriptors: list[int] = []
    try:
        try:
            descriptor = os.open(os.path.sep, directory_flags)
        except OSError as error:  # pragma: no cover - the filesystem root always opens
            raise _reject(
                SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                "the filesystem root is unavailable",
            ) from error
        descriptors.append(descriptor)
        for component in root.parts[1:]:
            try:
                child = os.open(component, directory_flags, dir_fd=descriptor)
            except OSError as error:
                raise _reject(
                    SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                    "the child workspace ancestry is unavailable or is a symlink",
                ) from error
            descriptors.append(child)
            descriptor = child
        for opened in descriptors:
            observed = os.fstat(opened)
            if not stat.S_ISDIR(observed.st_mode):  # pragma: no cover - O_DIRECTORY covers it
                raise _reject(
                    SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                    "a child workspace ancestor is not a directory",
                )
            if observed.st_uid not in (0, expected_uid):
                raise _reject(
                    SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                    "a child workspace ancestor is owned by an untrusted account",
                )
            if observed.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
                raise _reject(
                    SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                    "a child workspace ancestor is group or world writable",
                )
        leaf = os.fstat(descriptors[-1])
        leaf_mode = stat.S_IMODE(leaf.st_mode)
        if leaf.st_uid != expected_uid or leaf_mode != CHILD_WORKSPACE_MODE:
            raise _reject(
                SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                "the child workspace root is not a private directory this verifier owns",
            )
        if child_uid != leaf.st_uid and child_gid == leaf.st_gid:
            raise _reject(
                SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                "the child shares the child workspace group, so the group bits would apply",
            )
        return root
    finally:
        for opened in reversed(descriptors):
            with suppress(OSError):  # pragma: no cover - descriptors are freshly opened
                os.close(opened)


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


def child_descriptor_limit() -> int:
    """The upper bound of the descriptor sweep on this host."""

    configured = os.sysconf("SC_OPEN_MAX") if hasattr(os, "sysconf") else 0
    return max(int(configured or 0), 4096)


def child_descriptor_sweep(
    pass_fds: Sequence[int],
    *,
    limit: int,
) -> tuple[tuple[int, int], ...]:
    """The exact `closerange` intervals that leave only 0/1/2 and the two IPC pipes.

    `subprocess` closes inherited descriptors *after* `preexec_fn` runs, so this sweep is
    the belt-and-braces half of the pair: even with the interpreter's own sweep bypassed,
    nothing but the standard streams and the retained pipes survives into the child.
    """

    retained = tuple(sorted(set(pass_fds)))
    if any(type(descriptor) is not int or descriptor < 3 for descriptor in retained):
        raise _reject(
            SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
            "a retained child descriptor is a standard stream or is not a descriptor",
        )
    if type(limit) is not int or limit <= (retained[-1] if retained else 2):
        raise _reject(
            SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
            "the descriptor sweep bound does not exceed every retained descriptor",
        )
    ranges: list[tuple[int, int]] = []
    low = 3
    for descriptor in retained:
        if descriptor > low:
            ranges.append((low, descriptor))
        low = descriptor + 1
    ranges.append((low, limit))
    return tuple(ranges)


#: The three identity syscalls a privilege drop is allowed to make, in order.
PRIVILEGE_SYSCALL_NAMES: Final[tuple[str, ...]] = ("setgroups", "setresgid", "setresuid")


def child_privilege_calls(
    plan: ChildPrivilegePlan | None,
) -> tuple[tuple[str, tuple[Any, ...]], ...]:
    """The exact ordered syscalls one plan performs. No plan performs none."""

    if plan is None:
        return ()
    calls: list[tuple[str, tuple[Any, ...]]] = []
    if plan.clear_supplementary_groups:
        calls.append(("setgroups", ([],)))
    calls.append(("setresgid", plan.setresgid))
    calls.append(("setresuid", plan.setresuid))
    return tuple(calls)


def _platform_privilege_syscalls() -> dict[str, Callable[..., None]]:
    return {
        name: call
        for name in PRIVILEGE_SYSCALL_NAMES
        if (call := getattr(os, name, None)) is not None
    }


def child_privilege_applier(
    plan: ChildPrivilegePlan | None,
    *,
    syscalls: Mapping[str, Callable[..., None]] | None = None,
) -> Callable[[], None]:
    """Turn one plan into the callable that performs it, dispatching through a fixed table.

    The table is looked up rather than called inline so the drop is observable without a
    real identity change: macOS has no `setresuid` at all, and a host that does have one
    must not actually drop privilege inside a unit test.
    """

    calls = child_privilege_calls(plan)
    table = _platform_privilege_syscalls() if syscalls is None else dict(syscalls)

    def apply() -> None:
        for name, arguments in calls:
            call = table.get(name)
            if call is None:
                raise _reject(
                    SignalFamilyReasonCode.CHILD_LAUNCH_FAILED,
                    "this platform cannot perform the child privilege drop",
                )
            call(*arguments)

    apply.privilege_calls = calls  # type: ignore[attr-defined]
    return apply


class ChildPreexec(Protocol):
    """The callable `Popen` runs between fork and exec, with its plan kept inspectable."""

    privilege_plan: ChildPrivilegePlan | None
    privilege_calls: tuple[tuple[str, tuple[Any, ...]], ...]
    descriptor_sweep: tuple[tuple[int, int], ...]
    pass_fds: tuple[int, ...]

    def __call__(self) -> None: ...


def build_child_preexec(
    plan: ChildPrivilegePlan | None,
    *,
    pass_fds: Sequence[int],
    limit: int,
    syscalls: Mapping[str, Callable[..., None]] | None = None,
) -> ChildPreexec:
    """Bind the privilege drop and the descriptor sweep into one inspectable callable."""

    retained = tuple(sorted(set(pass_fds)))
    sweep = child_descriptor_sweep(retained, limit=limit)
    drop = child_privilege_applier(plan, syscalls=syscalls)

    def apply() -> None:
        drop()
        for low, high in sweep:
            os.closerange(low, high)

    apply.privilege_plan = plan  # type: ignore[attr-defined]
    apply.privilege_calls = drop.privilege_calls  # type: ignore[attr-defined]
    apply.descriptor_sweep = sweep  # type: ignore[attr-defined]
    apply.pass_fds = retained  # type: ignore[attr-defined]
    return apply  # type: ignore[return-value]


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


def allowed_readiness_sources(
    target: SignalFamilyReadinessState,
) -> tuple[str, ...]:
    """The exact states the frozen lifecycle permits as the source of one transition."""

    if not isinstance(target, SignalFamilyReadinessState):
        raise TypeError("a readiness transition requires an exact readiness state")
    sources = tuple(
        sorted(
            source.value
            for source, allowed in ALLOWED_READINESS_TRANSITIONS
            if allowed is target
        )
    )
    if not sources:
        raise _reject(
            SignalFamilyReasonCode.READINESS_TRANSITION_INVALID,
            "the frozen lifecycle reaches that state from nowhere",
        )
    return sources


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
        sources = allowed_readiness_sources(target)
        placeholders = ", ".join("?" for _ in sources)
        self._connection.execute("BEGIN IMMEDIATE")
        try:
            # The frozen lifecycle edge is the SQL predicate, not a prior read: a state
            # this transition may not start from simply matches no row, so a blind write
            # cannot be substituted for the guard.
            swapped = self._connection.execute(
                "UPDATE readiness_state SET state = ?, updated_at = ? "
                "WHERE overlay_content_hash = ? AND authority_epoch_key = ? "
                f"AND state IN ({placeholders})",  # noqa: S608 - placeholders only
                (target.value, stamp, overlay_content_hash, authority_epoch_key, *sources),
            ).rowcount
            if swapped != 1:
                raise _reject(
                    SignalFamilyReasonCode.READINESS_TRANSITION_INVALID,
                    "no readiness record in an allowed source state matches this key",
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
        workspace = open_child_workspace_root(
            self._anchors.child_workspace_root,
            expected_uid=self._anchors.expected_owner_uid,
            child_uid=self._anchors.child_uid,
            child_gid=self._anchors.child_gid,
        )
        request_read, request_write = os.pipe()
        result_read, result_write = os.pipe()
        cwd = Path(
            tempfile.mkdtemp(prefix="rquant-signal-family-child-", dir=str(workspace))
        )
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
                    preexec_fn=build_child_preexec(
                        plan,
                        pass_fds=(request_read, result_write),
                        limit=child_descriptor_limit(),
                    ),
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

        try:
            return self._run()
        except SignalFamilyRootVerifierError as error:
            if error.audit_record is None:
                # Every rejection carries bounded evidence even though it can never be
                # appended: authority.md L1404-1405 forbids opening the store before the
                # child exits, and a step that rejects earlier never gets that far.
                error.audit_record = SignalFamilyVerificationAuditRecordV1.create(
                    event=_REJECTION_EVENTS[error.reason_code],
                    outcome=SignalFamilyAuditOutcome.REJECTED,
                    reason_code=error.reason_code,
                    recorded_at=self._clock(),
                )
            raise

    def _run(self) -> VerifierRunResult:
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
            # Ruling C1 / L1208-1209: a reader surface is *the code exercised* for that
            # pair's one receipt, so a pair whose readers never ran has nothing to issue a
            # receipt about. The gate is on the child's own results, not on the manifest's
            # declaration, and it covers all five pairs at once: partial coverage yields no
            # receipt and no readiness, only a bounded rejection.
            self._require_pair_surface_coverage(child_result)
            # L1441-1443: the root revalidates the immutable manifests, the binding
            # tuple, the source paths and hashes, the full-manifest closure, the service
            # manifests, and the policy age cap *again* after the child exits. Every
            # equality inside this second derivation is against the one policy entry both
            # derivations share, so the derivation itself is the enforcement: anything
            # that moved under the running child rejects here, by its own reason code.
            self._load_generation_plan(
                authority=authority,
                entry=entry,
                successor=successor,
                overlay=overlay,
            )

            # 7. Still under the lock: reopen the anchors and the authority.
            self._assert_lock(lock)
            reopened_policy = self._load_policy()
            # Ruling O8 calls both halves of this comparison "stale". The entry is
            # checked first so that a changed authorization for this exact release is
            # named as such, and a change anywhere else in the policy or its harness is
            # named as the broader replacement it is.
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
        try:
            derived_pairs = five_pair_service_binding_set_hash(
                authority.profile_manifests,
                test_manifest.service_bindings,
            )
        except SignalFamilyVerificationError as error:
            raise _reject(
                SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
                "the validated profile and binding tuple do not form the five pair set",
            ) from error
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
        # The source closure only means something once the document that carries it has
        # been authenticated, so the root requires that of any gateway before it treats a
        # single `full_manifest_entries` row as a fact about the generation.
        if authority.full_manifest_sha256 != authority.slot.full_manifest_hash:
            raise _reject(
                SignalFamilyReasonCode.FULL_MANIFEST_HASH_MISMATCH,
                "the parsed full generation manifest is not the one the slot identifies",
            )
        # The document that carried the profile's service manifests into this process is
        # held down by the same source closure as every other generation file it names.
        self._require_manifested(
            authority,
            PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH,
            authority.profile_document_sha256,
        )
        self._validate_bindings(authority, test_manifest)
        participating = self._participating(authority)
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
        )

    @staticmethod
    def _participating(authority: GenerationAuthoritySnapshot) -> tuple[str, ...]:
        """The pair-derived participant union, with a bounded code on a bad profile."""

        try:
            return participating_service_ids(authority.profile_manifests)
        except SignalFamilyVerificationError as error:
            raise _reject(
                SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
                "the validated profile does not resolve the exact five pair rows",
            ) from error

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
        participating = self._participating(authority)
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
    def _require_pair_surface_coverage(child_result: SignalFamilyChildResultV1) -> None:
        """Every frozen pair must have executed every one of its reader surfaces."""

        if missing_pair_surface_coverage(child_result.vector_results):
            raise _reject(
                SignalFamilyReasonCode.PAIR_SURFACE_COVERAGE_MISSING,
                "a frozen pair has reader surfaces no executed vector covered",
            )

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
    from one root-owned canonical generation document. That document is not self-
    authorizing; two independent bindings hold it down:

    * it is an entry of the full generation manifest, and that manifest's own raw bytes
      must hash to `RuntimeGenerationSlot.full_manifest_hash`, which is the slot identity
      and an input to the authority epoch key — so the closure is authenticated before
      any membership claim inside it is believed; and
    * every `manifest_fingerprint` inside it must equal the `service_manifest_fingerprint`
      of the matching `VerificationServiceBindingV1`, and that binding tuple is anchored
      by the external root policy through `five_pair_service_binding_set_hash`. Forging a
      manifest therefore requires a fingerprint preimage.

    It is deliberately *not* hashed against `RuntimeGenerationSlot.profile_id`:
    `RuntimeClosureProfile` fixes `profile_id` as the hash of the runtime closure body,
    which carries no service manifests at all, so that equation could only hold on a
    SHA-256 collision.
    """

    __slots__ = ("_authority_loader",)

    def __init__(self, *, authority_loader: Callable[[], Any] | None = None) -> None:
        self._authority_loader = authority_loader

    def acquire_deployment_lock(self) -> DeploymentLockHandle:
        from rquant.runtime_authority import acquire_runtime_deployment_lock

        return acquire_runtime_deployment_lock()

    def _load_record(self) -> Any:
        if self._authority_loader is not None:
            return self._authority_loader()
        from rquant.runtime_authority import load_runtime_authority

        return load_runtime_authority()

    def load_snapshot(self) -> GenerationAuthoritySnapshot:
        from rquant.runtime_authority import GENERATION_MANIFEST_NAME

        record = self._load_record()
        slot = record.current
        manifest_payload = _read_generation_file(
            slot.generation_path,
            GENERATION_MANIFEST_NAME,
            max_bytes=MAX_HARNESS_BYTES,
            reason=SignalFamilyReasonCode.FULL_MANIFEST_HASH_MISMATCH,
        )
        # The full manifest is the source closure (L1473-1475), so it authenticates
        # nothing until it authenticates itself: `full_manifest_hash` is the slot's own
        # identity and an input to the authority epoch key, and the document that claims
        # to be that closure must hash to it before a single one of its entries is read.
        manifest_sha256 = hashlib.sha256(manifest_payload).hexdigest()
        if manifest_sha256 != slot.full_manifest_hash:
            raise _reject(
                SignalFamilyReasonCode.FULL_MANIFEST_HASH_MISMATCH,
                "the full generation manifest is not the one the slot identifies",
            )
        entries = _parse_full_manifest_entries(manifest_payload)
        profile_payload = _read_generation_file(
            slot.generation_path,
            PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH,
            max_bytes=MAX_GENERATION_DOCUMENT_BYTES,
            reason=SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
        )
        profile_sha256 = hashlib.sha256(profile_payload).hexdigest()
        if entries.get(PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH) != profile_sha256:
            raise _reject(
                SignalFamilyReasonCode.BINDING_UNMANIFESTED,
                "the profile service manifest document is outside the source closure",
            )
        return GenerationAuthoritySnapshot(
            operation_id=record.operation_id,
            sequence=record.sequence,
            authority_state=record.state,
            slot=slot,
            profile_manifests=_parse_profile_manifests(profile_payload),
            full_manifest_entries=entries,
            full_manifest_sha256=manifest_sha256,
            profile_document_sha256=profile_sha256,
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
        manifests = tuple(RuntimeServiceManifest.model_validate(row) for row in rows)
    except (ValueError, TypeError) as error:
        raise _reject(
            SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
            "a profile service manifest does not satisfy its frozen schema",
        ) from error
    service_ids = tuple(manifest.service_id for manifest in manifests)
    if len(set(service_ids)) != len(service_ids):
        raise _reject(
            SignalFamilyReasonCode.PARTICIPANT_RESOLUTION_INVALID,
            "the profile service manifest document repeats one service id",
        )
    return manifests


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
