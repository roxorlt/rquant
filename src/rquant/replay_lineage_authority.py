"""Persistent independent authority for source replay lineage checkpoints."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Literal, Protocol
from uuid import uuid4

from pydantic import Field, ValidationError

from rquant.adapter_manifest import (
    REPLAY_CLAIM_NAMESPACE,
    Ed25519ContractSigner,
    VerifyOnlyEd25519Keyring,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_broker import ReplayLineageCheckpointReceipt

_SCHEMA_VERSION = 1
_APPLICATION_ID = 0x52514C41
_EMPTY_JOURNAL_ROOT = "0" * 64
_META_TABLE = "replay_lineage_authority_meta"
_HEAD_TABLE = "replay_lineage_head"
_OPERATION_TABLE = "replay_lineage_operation"
_PENDING_TABLE = "replay_lineage_pending_advance"
_TABLE_SQL = {
    _META_TABLE: """
        CREATE TABLE replay_lineage_authority_meta (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            schema_version INTEGER NOT NULL CHECK (schema_version = 1),
            authority_id TEXT NOT NULL,
            checkpoint_json TEXT NOT NULL
        ) STRICT
    """,
    _HEAD_TABLE: """
        CREATE TABLE replay_lineage_head (
            replay_authority_id TEXT PRIMARY KEY,
            head_json TEXT NOT NULL
        ) STRICT
    """,
    _OPERATION_TABLE: """
        CREATE TABLE replay_lineage_operation (
            operation_id TEXT PRIMARY KEY,
            journal_index INTEGER NOT NULL UNIQUE CHECK (journal_index > 0),
            request_json TEXT NOT NULL,
            result_json TEXT NOT NULL,
            receipt_json TEXT NOT NULL,
            previous_journal_hash TEXT NOT NULL,
            journal_hash TEXT NOT NULL
        ) STRICT
    """,
    _PENDING_TABLE: """
        CREATE TABLE replay_lineage_pending_advance (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            pending_json TEXT NOT NULL
        ) STRICT
    """,
}

DB_ONLY_ROLLBACK_LIMITATION = (
    "A standalone authority database cannot detect replacement by a valid older signed "
    "snapshot even when the signing key is unchanged. It also cannot resist replacement "
    "of the database together with its key or any root kept beside it; production must "
    "use an independently administered monotonic checkpoint store."
)


class ReplayLineageAuthoritySecurityError(RuntimeError):
    """Raised when authority configuration or persistent state cannot be trusted."""


class ReplayLineageAuthorityRepairState(RuntimeContractModel):
    status: Literal["repair_required"]
    reason: Literal[
        "external_root_ahead_without_pending_proof",
        "external_root_too_far_ahead",
        "local_checkpoint_ahead_of_root",
    ]
    authority_id: str = Field(min_length=1, max_length=200)
    root_authority_id: str = Field(min_length=1, max_length=200)
    local_operation_count: int = Field(strict=True, ge=0)
    root_operation_count: int = Field(strict=True, ge=0)
    local_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    root_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    non_production: Literal[False] = False


class ReplayLineageAuthorityRepairRequiredError(ReplayLineageAuthoritySecurityError):
    """Raised with a closed state when automatic high-water recovery is unsafe."""

    def __init__(self, state: ReplayLineageAuthorityRepairState) -> None:
        self.state = state
        super().__init__(f"replay lineage authority repair required: {state.reason}")


class ProductionPathSeparation(RuntimeContractModel):
    """Normalized database paths that have passed production isolation checks."""

    authority_db_path: Path
    broker_db_path: Path
    replay_db_path: Path
    root_store_path: Path | None = None
    lineage_key_paths: tuple[Path, ...] = ()


class SignedReplayLineageCheckpoint(RuntimeContractModel):
    """Signed timeless state root intended for independent external persistence."""

    schema_version: Literal[1]
    contract: Literal["rquant-replay-lineage-authority-checkpoint/v1"]
    proof_type: Literal["external_anti_rollback_root"]
    authority_id: str = Field(min_length=1, max_length=200)
    operation_count: int = Field(strict=True, ge=0)
    journal_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    heads_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=200)
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str

    def signing_bytes(self) -> bytes:
        return _canonical_json_bytes(self.model_dump(mode="json", exclude={"signature"}))

    @property
    def checkpoint_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class AntiRollbackRoot(RuntimeContractModel):
    """Closed high-water record returned by an independent monotonic store."""

    schema_version: Literal[1]
    contract: Literal["rquant-replay-lineage-anti-rollback-root/v1"]
    root_authority_id: str = Field(min_length=1, max_length=200)
    lineage_authority_id: str = Field(min_length=1, max_length=200)
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint: SignedReplayLineageCheckpoint

    @property
    def root_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class MonotonicCheckpointStore(Protocol):
    """Independent anti-rollback service contract used by production authorities."""

    @property
    def authority_id(self) -> str: ...

    @property
    def storage_path(self) -> Path | None: ...

    @property
    def verifier_fingerprints(self) -> frozenset[str]: ...

    def pin(
        self,
        *,
        operation_id: str,
        lineage_authority_id: str,
        checkpoint: SignedReplayLineageCheckpoint,
    ) -> AntiRollbackRoot: ...

    def current(self, *, lineage_authority_id: str) -> AntiRollbackRoot | None: ...

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        lineage_authority_id: str,
        previous_checkpoint_hash: str,
        checkpoint: SignedReplayLineageCheckpoint,
    ) -> AntiRollbackRoot: ...


class ReplayLineageAuthorityPreflight(RuntimeContractModel):
    mode: Literal["production", "test-standalone"]
    non_production: bool
    root_required: bool
    root_configured: bool
    root_authority_id: str | None = None
    lineage_db_path: Path
    root_store_path: Path | None = None
    lineage_key_paths: tuple[Path, ...]
    root_verifier_fingerprints: frozenset[str]


class ReplayLineageAuditSummary(RuntimeContractModel):
    authority_id: str = Field(min_length=1, max_length=200)
    operation_count: int = Field(strict=True, ge=0)
    lineage_count: int = Field(strict=True, ge=0)
    journal_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    heads_root: str = Field(pattern=r"^[0-9a-f]{64}$")
    checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    mode: Literal["production", "test-standalone"]
    non_production: bool
    limitation: str = DB_ONLY_ROLLBACK_LIMITATION


class ReplayLineageAdvanceRequest(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-replay-lineage-authority-advance/v1"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    previous_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    next_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=1)
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def request_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ReplayLineageOperationResult(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-replay-lineage-authority-result/v1"]
    result_type: Literal["committed_lineage_advance"]
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    sequence: int = Field(strict=True, ge=1)
    next_head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def result_hash(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class ReplayLineageHeadState(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-replay-lineage-authority-head/v1"]
    state_type: Literal["current_replay_authority_lineage"]
    replay_authority_id: str = Field(min_length=1, max_length=200)
    lineage_id: str = Field(min_length=1, max_length=200)
    head_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    sequence: int = Field(strict=True, ge=1)
    claim_binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    operation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    result_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ReplayLineagePendingAdvance(RuntimeContractModel):
    """Signed-checkpoint-bound proof for finishing one external-root advance."""

    schema_version: Literal[1]
    contract: Literal["rquant-replay-lineage-pending-advance/v1"]
    previous_checkpoint_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    request: ReplayLineageAdvanceRequest
    result: ReplayLineageOperationResult
    receipt: ReplayLineageCheckpointReceipt
    head: ReplayLineageHeadState
    checkpoint: SignedReplayLineageCheckpoint


class ReplayLineageBackupExport(RuntimeContractModel):
    schema_version: Literal[1]
    contract: Literal["rquant-replay-lineage-authority-backup/v1"]
    database_path: Path
    checkpoint: SignedReplayLineageCheckpoint


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _canonical_json(value: object) -> str:
    return _canonical_json_bytes(value).decode("utf-8")


def _model_json(value: RuntimeContractModel) -> str:
    return _canonical_json(value.model_dump(mode="json"))


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_fingerprint_set(values: frozenset[str], *, label: str) -> frozenset[str]:
    if not isinstance(values, frozenset) or any(not _is_sha256(value) for value in values):
        raise ReplayLineageAuthoritySecurityError(f"{label} contains an invalid fingerprint")
    return values


def _normalized_sql(value: str) -> str:
    return " ".join(value.split())


def _absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(Path(path).expanduser())))


def _existing_database_path(path: Path, *, label: str) -> Path:
    absolute = _absolute_path(path)
    try:
        resolved = absolute.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ReplayLineageAuthoritySecurityError(f"{label} database must already exist") from exc
    if not resolved.is_file():
        raise ReplayLineageAuthoritySecurityError(f"{label} database must be a file")
    return resolved


def _authority_database_path(path: Path) -> Path:
    absolute = _absolute_path(path)
    if absolute.exists() or absolute.is_symlink():
        try:
            resolved = absolute.resolve(strict=True)
        except FileNotFoundError as exc:
            raise ReplayLineageAuthoritySecurityError(
                "authority database symlink target must already exist"
            ) from exc
        if not resolved.is_file():
            raise ReplayLineageAuthoritySecurityError("authority database must be a file")
        return resolved
    try:
        parent = absolute.parent.resolve(strict=True)
    except FileNotFoundError as exc:
        raise ReplayLineageAuthoritySecurityError(
            "authority database parent must already exist"
        ) from exc
    return parent / absolute.name


def assert_production_path_separation(
    *,
    authority_db_path: Path,
    broker_db_path: Path,
    replay_db_path: Path,
) -> ProductionPathSeparation:
    """Reject aliases between the authority, broker, and replay databases.

    Broker and replay databases must exist before this check.  Resolving them
    strictly prevents a misspelled production path from being created as an
    empty SQLite database.  Both normalized names and inode identity are
    checked so symlinks and hard links cannot defeat the separation boundary.
    """

    authority = _authority_database_path(authority_db_path)
    broker = _existing_database_path(broker_db_path, label="broker")
    replay = _existing_database_path(replay_db_path, label="replay")
    paths = (authority, broker, replay)
    if len(set(paths)) != len(paths):
        raise ReplayLineageAuthoritySecurityError(
            "authority, broker, and replay databases must be independent files"
        )
    existing = [path for path in paths if path.exists()]
    for index, left in enumerate(existing):
        for right in existing[index + 1 :]:
            if os.path.samefile(left, right):
                raise ReplayLineageAuthoritySecurityError(
                    "authority, broker, and replay databases must be independent files"
                )
    return ProductionPathSeparation(
        authority_db_path=authority,
        broker_db_path=broker,
        replay_db_path=replay,
    )


def assert_production_composition(
    *,
    authority_id: str,
    authority_db_path: Path,
    broker_db_path: Path,
    replay_db_path: Path,
    root_store: MonotonicCheckpointStore,
    lineage_key_paths: tuple[Path, ...],
    lineage_verifier_fingerprints: frozenset[str],
) -> ProductionPathSeparation:
    """Validate the independent production authority/root trust boundary."""

    separated = assert_production_path_separation(
        authority_db_path=authority_db_path,
        broker_db_path=broker_db_path,
        replay_db_path=replay_db_path,
    )
    root_authority_id = root_store.authority_id.strip()
    if not root_authority_id or root_authority_id == authority_id:
        raise ReplayLineageAuthoritySecurityError(
            "root authority must be nonempty and independent from lineage authority"
        )
    root_fingerprints = _validate_fingerprint_set(
        root_store.verifier_fingerprints,
        label="anti-rollback root verifier set",
    )
    if not root_fingerprints or root_fingerprints & lineage_verifier_fingerprints:
        raise ReplayLineageAuthoritySecurityError(
            "root verifier fingerprints must be independent from lineage signing keys"
        )
    if not lineage_key_paths:
        raise ReplayLineageAuthoritySecurityError(
            "production composition requires explicit lineage key files"
        )
    root_path = (
        None
        if root_store.storage_path is None
        else _existing_database_path(root_store.storage_path, label="anti-rollback root")
    )
    key_paths = tuple(
        _existing_database_path(path, label="lineage key") for path in lineage_key_paths
    )
    all_paths = (
        separated.authority_db_path,
        separated.broker_db_path,
        separated.replay_db_path,
        *(() if root_path is None else (root_path,)),
        *key_paths,
    )
    if len(set(all_paths)) != len(all_paths):
        raise ReplayLineageAuthoritySecurityError(
            "root storage, databases, and key files must be independent paths"
        )
    for index, left in enumerate(all_paths):
        for right in all_paths[index + 1 :]:
            if left.exists() and right.exists() and os.path.samefile(left, right):
                raise ReplayLineageAuthoritySecurityError(
                    "root storage, databases, and key files must be independent physical files"
                )
    return separated.model_copy(
        update={"root_store_path": root_path, "lineage_key_paths": key_paths}
    )


class PersistentReplayLineageAuthority:
    """Independent SQLite implementation of the replay-lineage authority protocol.

    State order is derived exclusively from committed journal indices and replay
    sequences.  No wall or boot clock participates in authority decisions.

    Production mode verifies every open, advance, and current-state check against
    an independently administered :class:`MonotonicCheckpointStore`. The explicit
    ``test-standalone`` mode has no anti-rollback guarantee: even if its signing key
    is unchanged, replacing its database with a valid older signed snapshot is not
    distinguishable from legitimate history. ``DB_ONLY_ROLLBACK_LIMITATION`` applies.
    """

    def __init__(
        self,
        path: Path,
        *,
        authority_id: str,
        signer: Ed25519ContractSigner,
        keyring: VerifyOnlyEd25519Keyring,
        broker_db_path: Path,
        replay_db_path: Path,
        expected_verifier_fingerprints: frozenset[str] | None = None,
        forbidden_verifier_fingerprints: frozenset[str] = frozenset(),
        busy_timeout_ms: int = 5_000,
        monotonic_checkpoint_store: MonotonicCheckpointStore | None = None,
        lineage_key_paths: tuple[Path, ...] = (),
        mode: Literal["production", "test-standalone"] = "production",
    ) -> None:
        normalized_authority_id = authority_id.strip()
        if not normalized_authority_id:
            raise ReplayLineageAuthoritySecurityError("authority_id must be nonempty")
        if type(busy_timeout_ms) is not int or busy_timeout_ms < 1:
            raise ReplayLineageAuthoritySecurityError("busy_timeout_ms must be a positive int")
        if mode not in {"production", "test-standalone"}:
            raise ReplayLineageAuthoritySecurityError("authority mode is invalid")
        if mode == "production" and monotonic_checkpoint_store is None:
            raise ReplayLineageAuthoritySecurityError(
                "production authority requires a monotonic checkpoint store"
            )
        if mode == "test-standalone" and monotonic_checkpoint_store is not None:
            raise ReplayLineageAuthoritySecurityError(
                "test-standalone authority cannot compose a production root"
            )
        separation = assert_production_path_separation(
            authority_db_path=path,
            broker_db_path=broker_db_path,
            replay_db_path=replay_db_path,
        )
        self.path = separation.authority_db_path
        self._authority_id = normalized_authority_id
        self._signer = signer
        self._keyring = keyring
        self._busy_timeout_ms = busy_timeout_ms
        self._mode = mode
        self._monotonic_checkpoint_store = monotonic_checkpoint_store
        self._lineage_key_paths = tuple(lineage_key_paths)
        self._root_store_path: Path | None = None
        self._verifier_fingerprints = self._validate_signing_trust(
            expected=expected_verifier_fingerprints,
            forbidden=forbidden_verifier_fingerprints,
        )
        if mode == "production":
            if monotonic_checkpoint_store is None:
                raise ReplayLineageAuthoritySecurityError(
                    "production authority requires a monotonic checkpoint store"
                )
            separation = assert_production_composition(
                authority_id=normalized_authority_id,
                authority_db_path=self.path,
                broker_db_path=broker_db_path,
                replay_db_path=replay_db_path,
                root_store=monotonic_checkpoint_store,
                lineage_key_paths=self._lineage_key_paths,
                lineage_verifier_fingerprints=self._verifier_fingerprints,
            )
            self.path = separation.authority_db_path
            self._root_store_path = separation.root_store_path
            self._lineage_key_paths = separation.lineage_key_paths
        self._initialize_or_open()
        if signer.issuer != self._authority_id:
            raise ReplayLineageAuthoritySecurityError(
                "signer identity does not match the pinned authority_id"
            )
        if self._mode == "production":
            self._synchronize_production_root()

    @property
    def authority_id(self) -> str:
        return self._authority_id

    @property
    def verifier_fingerprints(self) -> frozenset[str]:
        return self._verifier_fingerprints

    def preflight(self) -> ReplayLineageAuthorityPreflight:
        if self._mode == "production":
            self._synchronize_production_root()
        store = self._monotonic_checkpoint_store
        return ReplayLineageAuthorityPreflight(
            mode=self._mode,
            non_production=self._mode != "production",
            root_required=self._mode == "production",
            root_configured=store is not None,
            root_authority_id=None if store is None else store.authority_id,
            lineage_db_path=self.path,
            root_store_path=self._root_store_path,
            lineage_key_paths=self._lineage_key_paths,
            root_verifier_fingerprints=(
                frozenset() if store is None else store.verifier_fingerprints
            ),
        )

    def compare_and_advance(
        self,
        *,
        operation_id: str,
        replay_authority_id: str,
        lineage_id: str,
        previous_head_hash: str,
        next_head_hash: str,
        sequence: int,
        claim_binding_hash: str,
    ) -> ReplayLineageCheckpointReceipt:
        request = self._advance_request(
            operation_id=operation_id,
            replay_authority_id=replay_authority_id,
            lineage_id=lineage_id,
            previous_head_hash=previous_head_hash,
            next_head_hash=next_head_hash,
            sequence=sequence,
            claim_binding_hash=claim_binding_hash,
        )
        if request.previous_head_hash == request.next_head_hash:
            raise ReplayLineageAuthoritySecurityError(
                "lineage advance cannot retain the previous head"
            )
        if self._mode == "production":
            return self._compare_and_advance_production(request)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                summary = self._audit_state(connection)
                existing = connection.execute(
                    "SELECT * FROM replay_lineage_operation WHERE operation_id = ?",
                    (request.operation_id,),
                ).fetchone()
                if existing is not None:
                    stored_request, _result, receipt = self._validate_operation_row(
                        existing,
                        expected_index=int(existing["journal_index"]),
                        expected_previous_hash=str(existing["previous_journal_hash"]),
                    )
                    if stored_request != request:
                        raise ReplayLineageAuthoritySecurityError(
                            "lineage operation_id was rebound to a different payload"
                        )
                    connection.commit()
                    return receipt

                current_row = connection.execute(
                    "SELECT head_json FROM replay_lineage_head WHERE replay_authority_id = ?",
                    (request.replay_authority_id,),
                ).fetchone()
                if current_row is None:
                    if request.sequence != 1:
                        raise ReplayLineageAuthoritySecurityError(
                            "lineage genesis sequence must be exactly 1"
                        )
                else:
                    current = self._parse_head(str(current_row["head_json"]))
                    if (
                        request.lineage_id != current.lineage_id
                        or request.previous_head_hash != current.head_hash
                        or request.sequence != current.sequence + 1
                    ):
                        raise ReplayLineageAuthoritySecurityError(
                            "lineage alternate genesis, fork, or rollback was rejected"
                        )

                receipt = self._sign_receipt(request)
                self._verify_receipt(request, receipt)
                result = ReplayLineageOperationResult(
                    schema_version=1,
                    contract="rquant-replay-lineage-authority-result/v1",
                    result_type="committed_lineage_advance",
                    operation_id=request.operation_id,
                    replay_authority_id=request.replay_authority_id,
                    lineage_id=request.lineage_id,
                    sequence=request.sequence,
                    next_head_hash=request.next_head_hash,
                    request_hash=request.request_hash,
                    receipt_hash=receipt.receipt_hash,
                )
                journal_index = summary.operation_count + 1
                journal_hash = self._journal_hash(
                    journal_index=journal_index,
                    previous_journal_hash=summary.journal_root,
                    request=request,
                    result=result,
                    receipt=receipt,
                )
                inserted = connection.execute(
                    """
                    INSERT INTO replay_lineage_operation(
                        operation_id, journal_index, request_json, result_json,
                        receipt_json, previous_journal_hash, journal_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.operation_id,
                        journal_index,
                        _model_json(request),
                        _model_json(result),
                        _model_json(receipt),
                        summary.journal_root,
                        journal_hash,
                    ),
                ).rowcount
                if inserted != 1:
                    raise ReplayLineageAuthoritySecurityError(
                        "lineage operation journal append failed closed"
                    )
                head = ReplayLineageHeadState(
                    schema_version=1,
                    contract="rquant-replay-lineage-authority-head/v1",
                    state_type="current_replay_authority_lineage",
                    replay_authority_id=request.replay_authority_id,
                    lineage_id=request.lineage_id,
                    head_hash=request.next_head_hash,
                    sequence=request.sequence,
                    claim_binding_hash=request.claim_binding_hash,
                    operation_id=request.operation_id,
                    receipt_hash=receipt.receipt_hash,
                    result_hash=result.result_hash,
                )
                if current_row is None:
                    connection.execute(
                        "INSERT INTO replay_lineage_head(replay_authority_id, head_json) "
                        "VALUES (?, ?)",
                        (request.replay_authority_id, _model_json(head)),
                    )
                else:
                    updated = connection.execute(
                        "UPDATE replay_lineage_head SET head_json = ? "
                        "WHERE replay_authority_id = ? AND head_json = ?",
                        (
                            _model_json(head),
                            request.replay_authority_id,
                            str(current_row["head_json"]),
                        ),
                    ).rowcount
                    if updated != 1:
                        raise ReplayLineageAuthoritySecurityError(
                            "lineage head compare-and-swap failed closed"
                        )
                heads_root = self._heads_root(connection)
                checkpoint = self._sign_checkpoint(
                    operation_count=journal_index,
                    journal_root=journal_hash,
                    heads_root=heads_root,
                )
                updated_meta = connection.execute(
                    "UPDATE replay_lineage_authority_meta SET checkpoint_json = ? "
                    "WHERE singleton = 1",
                    (_model_json(checkpoint),),
                ).rowcount
                if updated_meta != 1:
                    raise ReplayLineageAuthoritySecurityError(
                        "authority signed state update failed closed"
                    )
                self._audit_state(connection)
                connection.commit()
                return receipt
            except BaseException:
                connection.rollback()
                raise

    def verify_current(
        self,
        *,
        replay_authority_id: str,
        lineage_id: str,
        head_hash: str,
        sequence: int,
        receipt: ReplayLineageCheckpointReceipt | None,
    ) -> None:
        normalized_replay_id = replay_authority_id.strip()
        normalized_lineage_id = lineage_id.strip()
        if not normalized_replay_id or not normalized_lineage_id:
            raise ReplayLineageAuthoritySecurityError("current lineage identity must be nonempty")
        if type(sequence) is not int or sequence < 0:
            raise ReplayLineageAuthoritySecurityError(
                "current lineage sequence must be a nonnegative int"
            )
        if not _is_sha256(head_hash):
            raise ReplayLineageAuthoritySecurityError("current lineage head hash is malformed")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if self._mode == "production":
                    self._synchronize_external_root(connection)
                self._audit_state(connection)
                row = connection.execute(
                    "SELECT head_json FROM replay_lineage_head WHERE replay_authority_id = ?",
                    (normalized_replay_id,),
                ).fetchone()
                if row is None:
                    if sequence == 0 and receipt is None:
                        connection.commit()
                        return
                    raise ReplayLineageAuthoritySecurityError(
                        "lineage checkpoint is not pinned by this authority"
                    )
                current = self._parse_head(str(row["head_json"]))
                if receipt is None:
                    raise ReplayLineageAuthoritySecurityError(
                        "lineage current checkpoint receipt is missing"
                    )
                try:
                    supplied = ReplayLineageCheckpointReceipt.model_validate(receipt)
                except ValidationError as exc:
                    raise ReplayLineageAuthoritySecurityError(
                        "lineage current checkpoint receipt is malformed"
                    ) from exc
                operation_row = connection.execute(
                    "SELECT * FROM replay_lineage_operation WHERE operation_id = ?",
                    (current.operation_id,),
                ).fetchone()
                if operation_row is None:
                    raise ReplayLineageAuthoritySecurityError(
                        "lineage current operation row is missing"
                    )
                request, _result, stored_receipt = self._validate_operation_row(
                    operation_row,
                    expected_index=int(operation_row["journal_index"]),
                    expected_previous_hash=str(operation_row["previous_journal_hash"]),
                )
                self._verify_receipt(request, supplied)
                if (
                    normalized_lineage_id != current.lineage_id
                    or head_hash != current.head_hash
                    or sequence != current.sequence
                    or supplied.receipt_hash != current.receipt_hash
                    or supplied.receipt_hash != stored_receipt.receipt_hash
                ):
                    raise ReplayLineageAuthoritySecurityError(
                        "lineage current checkpoint verification failed"
                    )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def audit_summary(self) -> ReplayLineageAuditSummary:
        """Audit every persisted row against the signed timeless state root."""

        if self._mode == "production":
            self._synchronize_production_root()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                summary = self._audit_state(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return summary

    def _compare_and_advance_production(
        self, request: ReplayLineageAdvanceRequest
    ) -> ReplayLineageCheckpointReceipt:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                summary = self._synchronize_external_root(connection)
                existing = connection.execute(
                    "SELECT * FROM replay_lineage_operation WHERE operation_id = ?",
                    (request.operation_id,),
                ).fetchone()
                if existing is not None:
                    stored_request, _result, receipt = self._validate_operation_row(
                        existing,
                        expected_index=int(existing["journal_index"]),
                        expected_previous_hash=str(existing["previous_journal_hash"]),
                    )
                    if stored_request != request:
                        raise ReplayLineageAuthoritySecurityError(
                            "lineage operation_id was rebound to a different payload"
                        )
                    connection.commit()
                    return receipt
                pending = self._prepare_pending(connection, summary=summary, request=request)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

        self._synchronize_production_root()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._synchronize_external_root(connection)
                row = connection.execute(
                    "SELECT * FROM replay_lineage_operation WHERE operation_id = ?",
                    (request.operation_id,),
                ).fetchone()
                if row is None:
                    raise ReplayLineageAuthoritySecurityError(
                        "rooted lineage operation is missing after recovery"
                    )
                stored_request, _result, receipt = self._validate_operation_row(
                    row,
                    expected_index=pending.checkpoint.operation_count,
                    expected_previous_hash=str(row["previous_journal_hash"]),
                )
                if stored_request != request or receipt != pending.receipt:
                    raise ReplayLineageAuthoritySecurityError(
                        "rooted lineage operation recovery result diverged"
                    )
                connection.commit()
                return receipt
            except BaseException:
                connection.rollback()
                raise

    def _synchronize_production_root(self) -> ReplayLineageAuditSummary:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                summary = self._synchronize_external_root(connection)
                connection.commit()
                return summary
            except BaseException:
                connection.rollback()
                raise

    def _synchronize_external_root(
        self, connection: sqlite3.Connection
    ) -> ReplayLineageAuditSummary:
        store = self._monotonic_checkpoint_store
        if self._mode != "production" or store is None:
            raise ReplayLineageAuthoritySecurityError(
                "production root synchronization requires a monotonic checkpoint store"
            )
        summary = self._audit_state(connection)
        checkpoint = self._current_checkpoint(connection)
        pending = self._read_pending(connection, summary=summary)
        root = self._coerce_root(
            store.current(lineage_authority_id=self._authority_id),
            allow_missing=True,
        )
        if root is None:
            if summary.operation_count != 0 or pending is not None:
                raise ReplayLineageAuthoritySecurityError(
                    "production anti-rollback root is missing for existing authority state"
                )
            operation_id = canonical_sha256(
                {
                    "contract": "rquant-replay-lineage-root-pin-operation/v1",
                    "authority_id": self._authority_id,
                    "checkpoint_hash": checkpoint.checkpoint_hash,
                }
            )
            rooted = self._coerce_root(
                store.pin(
                    operation_id=operation_id,
                    lineage_authority_id=self._authority_id,
                    checkpoint=checkpoint,
                ),
                allow_missing=False,
            )
            expected = AntiRollbackRoot(
                schema_version=1,
                contract="rquant-replay-lineage-anti-rollback-root/v1",
                root_authority_id=store.authority_id,
                lineage_authority_id=self._authority_id,
                operation_id=operation_id,
                previous_checkpoint_hash="0" * 64,
                checkpoint=checkpoint,
            )
            if rooted != expected:
                raise ReplayLineageAuthoritySecurityError(
                    "anti-rollback root pin returned a divergent result"
                )
            return summary

        root_count = root.checkpoint.operation_count
        local_count = summary.operation_count
        if root_count == local_count:
            if root.checkpoint != checkpoint:
                raise ReplayLineageAuthoritySecurityError(
                    "external root and local checkpoint diverge at the same high-water"
                )
            self._validate_current_root_operation(connection, root)
            if pending is None:
                return summary
            rooted = self._coerce_root(
                store.compare_and_advance(
                    operation_id=pending.request.operation_id,
                    lineage_authority_id=self._authority_id,
                    previous_checkpoint_hash=pending.previous_checkpoint_hash,
                    checkpoint=pending.checkpoint,
                ),
                allow_missing=False,
            )
            self._validate_rooted_pending(rooted, pending)
            return self._finalize_pending(connection, pending)

        if root_count == local_count + 1:
            if pending is None:
                self._raise_repair_required(
                    reason="external_root_ahead_without_pending_proof",
                    summary=summary,
                    root=root,
                )
            self._validate_rooted_pending(root, pending)
            return self._finalize_pending(connection, pending)
        if root_count > local_count:
            self._raise_repair_required(
                reason="external_root_too_far_ahead",
                summary=summary,
                root=root,
            )
        self._raise_repair_required(
            reason="local_checkpoint_ahead_of_root",
            summary=summary,
            root=root,
        )

    def _validate_current_root_operation(
        self,
        connection: sqlite3.Connection,
        root: AntiRollbackRoot,
    ) -> None:
        if root.checkpoint.operation_count == 0:
            expected_operation_id = canonical_sha256(
                {
                    "contract": "rquant-replay-lineage-root-pin-operation/v1",
                    "authority_id": self._authority_id,
                    "checkpoint_hash": root.checkpoint.checkpoint_hash,
                }
            )
            valid = (
                root.operation_id == expected_operation_id
                and root.previous_checkpoint_hash == "0" * 64
            )
        else:
            latest = connection.execute(
                "SELECT operation_id FROM replay_lineage_operation "
                "ORDER BY journal_index DESC LIMIT 1"
            ).fetchone()
            valid = (
                latest is not None
                and root.operation_id == latest["operation_id"]
                and root.previous_checkpoint_hash != "0" * 64
            )
        if not valid:
            raise ReplayLineageAuthoritySecurityError(
                "external root operation does not bind the latest committed operation"
            )

    def _coerce_root(
        self,
        root: AntiRollbackRoot | None,
        *,
        allow_missing: bool,
    ) -> AntiRollbackRoot | None:
        if root is None:
            if allow_missing:
                return None
            raise ReplayLineageAuthoritySecurityError("monotonic checkpoint store returned no root")
        try:
            validated = AntiRollbackRoot.model_validate(root.model_dump(mode="python"))
        except (AttributeError, ValidationError, TypeError, ValueError) as exc:
            raise ReplayLineageAuthoritySecurityError(
                "monotonic checkpoint store returned a malformed root"
            ) from exc
        store = self._monotonic_checkpoint_store
        if (
            store is None
            or validated.root_authority_id != store.authority_id
            or validated.lineage_authority_id != self._authority_id
        ):
            raise ReplayLineageAuthoritySecurityError(
                "monotonic checkpoint root authority binding is invalid"
            )
        self._verify_checkpoint_signature(validated.checkpoint)
        return validated

    def _validate_rooted_pending(
        self,
        root: AntiRollbackRoot | None,
        pending: ReplayLineagePendingAdvance,
    ) -> None:
        store = self._monotonic_checkpoint_store
        if store is None or root is None:
            raise ReplayLineageAuthoritySecurityError("rooted pending result is missing")
        expected = AntiRollbackRoot(
            schema_version=1,
            contract="rquant-replay-lineage-anti-rollback-root/v1",
            root_authority_id=store.authority_id,
            lineage_authority_id=self._authority_id,
            operation_id=pending.request.operation_id,
            previous_checkpoint_hash=pending.previous_checkpoint_hash,
            checkpoint=pending.checkpoint,
        )
        if root != expected:
            raise ReplayLineageAuthoritySecurityError(
                "external root does not match the signed pending advance"
            )

    def _raise_repair_required(
        self,
        *,
        reason: Literal[
            "external_root_ahead_without_pending_proof",
            "external_root_too_far_ahead",
            "local_checkpoint_ahead_of_root",
        ],
        summary: ReplayLineageAuditSummary,
        root: AntiRollbackRoot,
    ) -> None:
        raise ReplayLineageAuthorityRepairRequiredError(
            ReplayLineageAuthorityRepairState(
                status="repair_required",
                reason=reason,
                authority_id=self._authority_id,
                root_authority_id=root.root_authority_id,
                local_operation_count=summary.operation_count,
                root_operation_count=root.checkpoint.operation_count,
                local_checkpoint_hash=summary.checkpoint_hash,
                root_checkpoint_hash=root.checkpoint.checkpoint_hash,
            )
        )

    def _prepare_pending(
        self,
        connection: sqlite3.Connection,
        *,
        summary: ReplayLineageAuditSummary,
        request: ReplayLineageAdvanceRequest,
    ) -> ReplayLineagePendingAdvance:
        if self._read_pending(connection, summary=summary) is not None:
            raise ReplayLineageAuthoritySecurityError(
                "a prior rooted lineage advance is still pending"
            )
        current_row = connection.execute(
            "SELECT head_json FROM replay_lineage_head WHERE replay_authority_id = ?",
            (request.replay_authority_id,),
        ).fetchone()
        if current_row is None:
            if request.sequence != 1:
                raise ReplayLineageAuthoritySecurityError(
                    "lineage genesis sequence must be exactly 1"
                )
        else:
            current = self._parse_head(str(current_row["head_json"]))
            if (
                request.lineage_id != current.lineage_id
                or request.previous_head_hash != current.head_hash
                or request.sequence != current.sequence + 1
            ):
                raise ReplayLineageAuthoritySecurityError(
                    "lineage alternate genesis, fork, or rollback was rejected"
                )
        receipt = self._sign_receipt(request)
        result = ReplayLineageOperationResult(
            schema_version=1,
            contract="rquant-replay-lineage-authority-result/v1",
            result_type="committed_lineage_advance",
            operation_id=request.operation_id,
            replay_authority_id=request.replay_authority_id,
            lineage_id=request.lineage_id,
            sequence=request.sequence,
            next_head_hash=request.next_head_hash,
            request_hash=request.request_hash,
            receipt_hash=receipt.receipt_hash,
        )
        head = ReplayLineageHeadState(
            schema_version=1,
            contract="rquant-replay-lineage-authority-head/v1",
            state_type="current_replay_authority_lineage",
            replay_authority_id=request.replay_authority_id,
            lineage_id=request.lineage_id,
            head_hash=request.next_head_hash,
            sequence=request.sequence,
            claim_binding_hash=request.claim_binding_hash,
            operation_id=request.operation_id,
            receipt_hash=receipt.receipt_hash,
            result_hash=result.result_hash,
        )
        journal_hash = self._journal_hash(
            journal_index=summary.operation_count + 1,
            previous_journal_hash=summary.journal_root,
            request=request,
            result=result,
            receipt=receipt,
        )
        heads = {
            str(row["replay_authority_id"]): self._parse_head(str(row["head_json"]))
            for row in connection.execute(
                "SELECT replay_authority_id, head_json FROM replay_lineage_head"
            )
        }
        heads[request.replay_authority_id] = head
        heads_root = canonical_sha256(
            [heads[replay_id].model_dump(mode="python") for replay_id in sorted(heads)]
        )
        checkpoint = self._sign_checkpoint(
            operation_count=summary.operation_count + 1,
            journal_root=journal_hash,
            heads_root=heads_root,
        )
        pending = ReplayLineagePendingAdvance(
            schema_version=1,
            contract="rquant-replay-lineage-pending-advance/v1",
            previous_checkpoint_hash=summary.checkpoint_hash,
            request=request,
            result=result,
            receipt=receipt,
            head=head,
            checkpoint=checkpoint,
        )
        inserted = connection.execute(
            "INSERT INTO replay_lineage_pending_advance(singleton, pending_json) VALUES (1, ?)",
            (_model_json(pending),),
        ).rowcount
        if inserted != 1:
            raise ReplayLineageAuthoritySecurityError(
                "lineage pending root proof persistence failed closed"
            )
        self._read_pending(connection, summary=summary)
        return pending

    def _read_pending(
        self,
        connection: sqlite3.Connection,
        *,
        summary: ReplayLineageAuditSummary,
    ) -> ReplayLineagePendingAdvance | None:
        rows = connection.execute(
            "SELECT singleton, pending_json FROM replay_lineage_pending_advance"
        ).fetchall()
        if not rows:
            return None
        if len(rows) != 1 or rows[0]["singleton"] != 1:
            raise ReplayLineageAuthoritySecurityError("lineage pending root proof row is invalid")
        pending_json = str(rows[0]["pending_json"])
        try:
            pending = ReplayLineagePendingAdvance.model_validate_json(pending_json)
        except ValidationError as exc:
            raise ReplayLineageAuthoritySecurityError(
                "lineage pending root proof is malformed"
            ) from exc
        if pending_json != _model_json(pending):
            raise ReplayLineageAuthoritySecurityError(
                "lineage pending root proof encoding was tampered"
            )
        self._validate_pending_proof(connection, summary=summary, pending=pending)
        return pending

    def _validate_pending_proof(
        self,
        connection: sqlite3.Connection,
        *,
        summary: ReplayLineageAuditSummary,
        pending: ReplayLineagePendingAdvance,
    ) -> None:
        request = pending.request
        result = pending.result
        receipt = pending.receipt
        self._verify_receipt(request, receipt)
        if (
            pending.previous_checkpoint_hash != summary.checkpoint_hash
            or result.operation_id != request.operation_id
            or result.replay_authority_id != request.replay_authority_id
            or result.lineage_id != request.lineage_id
            or result.sequence != request.sequence
            or result.next_head_hash != request.next_head_hash
            or result.request_hash != request.request_hash
            or result.receipt_hash != receipt.receipt_hash
        ):
            raise ReplayLineageAuthoritySecurityError(
                "lineage pending request, result, or receipt binding is invalid"
            )
        current_row = connection.execute(
            "SELECT head_json FROM replay_lineage_head WHERE replay_authority_id = ?",
            (request.replay_authority_id,),
        ).fetchone()
        current = None if current_row is None else self._parse_head(str(current_row["head_json"]))
        if current is None:
            progression_valid = request.sequence == 1
        else:
            progression_valid = (
                request.lineage_id == current.lineage_id
                and request.previous_head_hash == current.head_hash
                and request.sequence == current.sequence + 1
            )
        expected_head = ReplayLineageHeadState(
            schema_version=1,
            contract="rquant-replay-lineage-authority-head/v1",
            state_type="current_replay_authority_lineage",
            replay_authority_id=request.replay_authority_id,
            lineage_id=request.lineage_id,
            head_hash=request.next_head_hash,
            sequence=request.sequence,
            claim_binding_hash=request.claim_binding_hash,
            operation_id=request.operation_id,
            receipt_hash=receipt.receipt_hash,
            result_hash=result.result_hash,
        )
        journal_hash = self._journal_hash(
            journal_index=summary.operation_count + 1,
            previous_journal_hash=summary.journal_root,
            request=request,
            result=result,
            receipt=receipt,
        )
        heads = {
            str(row["replay_authority_id"]): self._parse_head(str(row["head_json"]))
            for row in connection.execute(
                "SELECT replay_authority_id, head_json FROM replay_lineage_head"
            )
        }
        heads[request.replay_authority_id] = expected_head
        heads_root = canonical_sha256(
            [heads[replay_id].model_dump(mode="python") for replay_id in sorted(heads)]
        )
        checkpoint = pending.checkpoint
        self._verify_checkpoint_signature(checkpoint)
        if (
            not progression_valid
            or pending.head != expected_head
            or checkpoint.operation_count != summary.operation_count + 1
            or checkpoint.journal_root != journal_hash
            or checkpoint.heads_root != heads_root
        ):
            raise ReplayLineageAuthoritySecurityError(
                "lineage pending proof does not commit the next valid authority state"
            )

    def _finalize_pending(
        self,
        connection: sqlite3.Connection,
        pending: ReplayLineagePendingAdvance,
    ) -> ReplayLineageAuditSummary:
        summary = self._audit_state(connection)
        validated = self._read_pending(connection, summary=summary)
        if validated != pending:
            raise ReplayLineageAuthoritySecurityError(
                "lineage pending proof changed before finalization"
            )
        request = pending.request
        result = pending.result
        receipt = pending.receipt
        connection.execute(
            """
            INSERT INTO replay_lineage_operation(
                operation_id, journal_index, request_json, result_json,
                receipt_json, previous_journal_hash, journal_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.operation_id,
                pending.checkpoint.operation_count,
                _model_json(request),
                _model_json(result),
                _model_json(receipt),
                summary.journal_root,
                pending.checkpoint.journal_root,
            ),
        )
        connection.execute(
            "INSERT INTO replay_lineage_head(replay_authority_id, head_json) VALUES (?, ?) "
            "ON CONFLICT(replay_authority_id) DO UPDATE SET head_json = excluded.head_json",
            (request.replay_authority_id, _model_json(pending.head)),
        )
        connection.execute(
            "UPDATE replay_lineage_authority_meta SET checkpoint_json = ? WHERE singleton = 1",
            (_model_json(pending.checkpoint),),
        )
        deleted = connection.execute(
            "DELETE FROM replay_lineage_pending_advance WHERE singleton = 1"
        ).rowcount
        if deleted != 1:
            raise ReplayLineageAuthoritySecurityError(
                "lineage pending root proof cleanup failed closed"
            )
        return self._audit_state(connection)

    def _current_checkpoint(self, connection: sqlite3.Connection) -> SignedReplayLineageCheckpoint:
        row = connection.execute(
            "SELECT checkpoint_json FROM replay_lineage_authority_meta WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise ReplayLineageAuthoritySecurityError("signed authority checkpoint row is missing")
        return self._parse_checkpoint(str(row["checkpoint_json"]))

    def _validate_signing_trust(
        self,
        *,
        expected: frozenset[str] | None,
        forbidden: frozenset[str],
    ) -> frozenset[str]:
        signer = self._signer
        keyring = self._keyring
        if signer.key_purpose != "replay_claim":
            raise ReplayLineageAuthoritySecurityError("lineage signer purpose must be replay_claim")
        fingerprints = _validate_fingerprint_set(
            keyring.fingerprints_for_purpose("replay_claim"),
            label="lineage keyring",
        )
        validated_expected = (
            None
            if expected is None
            else _validate_fingerprint_set(expected, label="expected verifier set")
        )
        validated_forbidden = _validate_fingerprint_set(forbidden, label="forbidden verifier set")
        if not fingerprints or signer.public_key_fingerprint not in fingerprints:
            raise ReplayLineageAuthoritySecurityError(
                "lineage signer fingerprint is not trusted by its keyring"
            )
        if not keyring.allows_signer(signer):
            raise ReplayLineageAuthoritySecurityError(
                "lineage signer identity is not allowed by its keyring"
            )
        if validated_expected is not None and fingerprints != validated_expected:
            raise ReplayLineageAuthoritySecurityError(
                "lineage verifier fingerprints do not match the expected trust set"
            )
        overlap = fingerprints & validated_forbidden
        if overlap:
            raise ReplayLineageAuthoritySecurityError(
                "lineage verifier fingerprint overlaps a forbidden signing role"
            )
        challenge = _canonical_json_bytes(
            {
                "authority_id": signer.issuer,
                "contract": "rquant-replay-lineage-signer-control/v1",
                "key_id": signer.key_id,
                "proof_type": "signer_control_challenge",
                "public_key_fingerprint": signer.public_key_fingerprint,
            }
        )
        signature = signer.sign(namespace=REPLAY_CLAIM_NAMESPACE, payload=challenge)
        if not keyring.verify(
            issuer=signer.issuer,
            key_id=signer.key_id,
            key_purpose="replay_claim",
            namespace=REPLAY_CLAIM_NAMESPACE,
            payload=challenge,
            signature=signature,
        ):
            raise ReplayLineageAuthoritySecurityError(
                "lineage signer does not control its declared verification key"
            )
        return fingerprints

    def _connect(self) -> sqlite3.Connection:
        return self._connect_database(self.path)

    def _connect_database(self, path: Path) -> sqlite3.Connection:
        connection = sqlite3.connect(
            path,
            timeout=self._busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA synchronous = FULL")
        connection.execute("PRAGMA trusted_schema = OFF")
        return connection

    def _initialize_or_open(self) -> None:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                objects = connection.execute(
                    "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
                ).fetchall()
                if not objects:
                    if self._signer.issuer != self._authority_id:
                        raise ReplayLineageAuthoritySecurityError(
                            "signer identity does not match authority_id on first open"
                        )
                    self._create_schema(connection)
                self._validate_schema(connection)
                self._audit_state(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise

    def _create_schema(self, connection: sqlite3.Connection) -> None:
        for sql in _TABLE_SQL.values():
            connection.execute(sql)
        connection.execute(f"PRAGMA application_id = {_APPLICATION_ID}")
        connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
        checkpoint = self._sign_checkpoint(
            operation_count=0,
            journal_root=_EMPTY_JOURNAL_ROOT,
            heads_root=canonical_sha256([]),
        )
        inserted = connection.execute(
            """
            INSERT INTO replay_lineage_authority_meta(
                singleton, schema_version, authority_id, checkpoint_json
            ) VALUES (1, 1, ?, ?)
            """,
            (self._authority_id, _model_json(checkpoint)),
        ).rowcount
        if inserted != 1:
            raise ReplayLineageAuthoritySecurityError(
                "authority metadata initialization failed closed"
            )

    def _validate_schema(self, connection: sqlite3.Connection) -> None:
        if connection.execute("PRAGMA application_id").fetchone()[0] != _APPLICATION_ID:
            raise ReplayLineageAuthoritySecurityError("authority schema application id is invalid")
        if connection.execute("PRAGMA user_version").fetchone()[0] != _SCHEMA_VERSION:
            raise ReplayLineageAuthoritySecurityError("authority schema version is invalid")
        objects = {
            (str(row["type"]), str(row["name"]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            )
        }
        expected_objects = {("table", name) for name in _TABLE_SQL}
        if objects != expected_objects:
            raise ReplayLineageAuthoritySecurityError(
                "authority schema has unknown or missing objects"
            )
        stored_sql = {
            str(row["name"]): str(row["sql"])
            for row in connection.execute(
                "SELECT name, sql FROM sqlite_master "
                "WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        for name, expected in _TABLE_SQL.items():
            if _normalized_sql(stored_sql.get(name, "")) != _normalized_sql(expected):
                raise ReplayLineageAuthoritySecurityError(
                    f"authority schema for {name} was tampered"
                )

    def _audit_state(self, connection: sqlite3.Connection) -> ReplayLineageAuditSummary:
        self._validate_schema(connection)
        operations = connection.execute(
            "SELECT * FROM replay_lineage_operation ORDER BY journal_index"
        ).fetchall()
        heads = connection.execute(
            "SELECT * FROM replay_lineage_head ORDER BY replay_authority_id"
        ).fetchall()
        meta_rows = connection.execute("SELECT * FROM replay_lineage_authority_meta").fetchall()
        if len(meta_rows) != 1:
            raise ReplayLineageAuthoritySecurityError(
                "authority metadata row is missing or duplicated"
            )
        meta = meta_rows[0]
        if (
            meta["singleton"] != 1
            or meta["schema_version"] != _SCHEMA_VERSION
            or meta["authority_id"] != self._authority_id
        ):
            if meta["authority_id"] != self._authority_id:
                raise ReplayLineageAuthoritySecurityError(
                    "authority_id does not match the first-open pinned identity"
                )
            raise ReplayLineageAuthoritySecurityError("authority metadata was tampered")
        checkpoint_json = str(meta["checkpoint_json"])
        checkpoint = self._parse_checkpoint(checkpoint_json)
        if checkpoint_json != _model_json(checkpoint):
            raise ReplayLineageAuthoritySecurityError(
                "signed authority checkpoint encoding was tampered"
            )
        journal_root = _EMPTY_JOURNAL_ROOT
        current_by_authority: dict[str, ReplayLineageHeadState] = {}
        for expected_index, row in enumerate(operations, start=1):
            request, result, receipt = self._validate_operation_row(
                row,
                expected_index=expected_index,
                expected_previous_hash=journal_root,
            )
            expected_journal_hash = self._journal_hash(
                journal_index=expected_index,
                previous_journal_hash=journal_root,
                request=request,
                result=result,
                receipt=receipt,
            )
            if row["journal_hash"] != expected_journal_hash:
                raise ReplayLineageAuthoritySecurityError(
                    "lineage operation journal commitment was tampered"
                )
            previous = current_by_authority.get(request.replay_authority_id)
            if previous is None:
                if request.sequence != 1:
                    raise ReplayLineageAuthoritySecurityError(
                        "persisted lineage genesis sequence is invalid"
                    )
            elif (
                request.lineage_id != previous.lineage_id
                or request.previous_head_hash != previous.head_hash
                or request.sequence != previous.sequence + 1
            ):
                raise ReplayLineageAuthoritySecurityError(
                    "persisted lineage contains a fork or rollback"
                )
            current_by_authority[request.replay_authority_id] = ReplayLineageHeadState(
                schema_version=1,
                contract="rquant-replay-lineage-authority-head/v1",
                state_type="current_replay_authority_lineage",
                replay_authority_id=request.replay_authority_id,
                lineage_id=request.lineage_id,
                head_hash=request.next_head_hash,
                sequence=request.sequence,
                claim_binding_hash=request.claim_binding_hash,
                operation_id=request.operation_id,
                receipt_hash=receipt.receipt_hash,
                result_hash=result.result_hash,
            )
            journal_root = expected_journal_hash

        stored_heads: dict[str, ReplayLineageHeadState] = {}
        for row in heads:
            replay_id = str(row["replay_authority_id"])
            head_json = str(row["head_json"])
            head = self._parse_head(head_json)
            if head_json != _model_json(head) or replay_id != head.replay_authority_id:
                raise ReplayLineageAuthoritySecurityError(
                    "lineage head row encoding or identity was tampered"
                )
            if replay_id in stored_heads:
                raise ReplayLineageAuthoritySecurityError("lineage head identity is duplicated")
            stored_heads[replay_id] = head
        if stored_heads != current_by_authority:
            raise ReplayLineageAuthoritySecurityError(
                "lineage head rows are missing, stale, or divergent"
            )
        expected_heads_root = canonical_sha256(
            [
                stored_heads[replay_id].model_dump(mode="python")
                for replay_id in sorted(stored_heads)
            ]
        )
        if (
            checkpoint.operation_count != len(operations)
            or checkpoint.journal_root != journal_root
            or checkpoint.heads_root != expected_heads_root
        ):
            raise ReplayLineageAuthoritySecurityError(
                "authority state does not match its signed checkpoint"
            )
        return ReplayLineageAuditSummary(
            authority_id=self._authority_id,
            operation_count=len(operations),
            lineage_count=len(stored_heads),
            journal_root=journal_root,
            heads_root=expected_heads_root,
            checkpoint_hash=checkpoint.checkpoint_hash,
            mode=self._mode,
            non_production=self._mode != "production",
        )

    def _advance_request(
        self,
        *,
        operation_id: str,
        replay_authority_id: str,
        lineage_id: str,
        previous_head_hash: str,
        next_head_hash: str,
        sequence: int,
        claim_binding_hash: str,
    ) -> ReplayLineageAdvanceRequest:
        try:
            return ReplayLineageAdvanceRequest(
                schema_version=1,
                contract="rquant-replay-lineage-authority-advance/v1",
                operation_id=operation_id,
                replay_authority_id=replay_authority_id,
                lineage_id=lineage_id,
                previous_head_hash=previous_head_hash,
                next_head_hash=next_head_hash,
                sequence=sequence,
                claim_binding_hash=claim_binding_hash,
            )
        except ValidationError as exc:
            raise ReplayLineageAuthoritySecurityError("lineage advance input is malformed") from exc

    def _sign_receipt(self, request: ReplayLineageAdvanceRequest) -> ReplayLineageCheckpointReceipt:
        unsigned = ReplayLineageCheckpointReceipt(
            schema_version=1,
            contract="rquant-source-replay-lineage-checkpoint/v1",
            authority_id=self._authority_id,
            operation_id=request.operation_id,
            replay_authority_id=request.replay_authority_id,
            lineage_id=request.lineage_id,
            previous_head_hash=request.previous_head_hash,
            next_head_hash=request.next_head_hash,
            sequence=request.sequence,
            claim_binding_hash=request.claim_binding_hash,
            outcome="applied",
            key_id=self._signer.key_id,
            signature="",
        )
        return unsigned.model_copy(
            update={
                "signature": self._signer.sign(
                    namespace=REPLAY_CLAIM_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _verify_receipt(
        self,
        request: ReplayLineageAdvanceRequest,
        receipt: ReplayLineageCheckpointReceipt,
    ) -> None:
        expected = (
            self._authority_id,
            request.operation_id,
            request.replay_authority_id,
            request.lineage_id,
            request.previous_head_hash,
            request.next_head_hash,
            request.sequence,
            request.claim_binding_hash,
            "applied",
        )
        actual = (
            receipt.authority_id,
            receipt.operation_id,
            receipt.replay_authority_id,
            receipt.lineage_id,
            receipt.previous_head_hash,
            receipt.next_head_hash,
            receipt.sequence,
            receipt.claim_binding_hash,
            receipt.outcome,
        )
        if actual != expected or not self._keyring.verify(
            issuer=receipt.authority_id,
            key_id=receipt.key_id,
            key_purpose="replay_claim",
            namespace=REPLAY_CLAIM_NAMESPACE,
            payload=receipt.signing_bytes(),
            signature=receipt.signature,
        ):
            raise ReplayLineageAuthoritySecurityError(
                "lineage receipt identity, fields, or signature are invalid"
            )

    def _validate_operation_row(
        self,
        row: sqlite3.Row,
        *,
        expected_index: int,
        expected_previous_hash: str,
    ) -> tuple[
        ReplayLineageAdvanceRequest,
        ReplayLineageOperationResult,
        ReplayLineageCheckpointReceipt,
    ]:
        try:
            request = ReplayLineageAdvanceRequest.model_validate_json(row["request_json"])
            result = ReplayLineageOperationResult.model_validate_json(row["result_json"])
            receipt = ReplayLineageCheckpointReceipt.model_validate_json(row["receipt_json"])
        except (ValidationError, TypeError, ValueError) as exc:
            raise ReplayLineageAuthoritySecurityError(
                "lineage operation row contains malformed request, result, or receipt"
            ) from exc
        if (
            row["operation_id"] != request.operation_id
            or row["journal_index"] != expected_index
            or row["previous_journal_hash"] != expected_previous_hash
            or row["request_json"] != _model_json(request)
            or row["result_json"] != _model_json(result)
            or row["receipt_json"] != _model_json(receipt)
        ):
            raise ReplayLineageAuthoritySecurityError(
                "lineage operation row fields or canonical encoding were tampered"
            )
        self._verify_receipt(request, receipt)
        if (
            result.operation_id != request.operation_id
            or result.replay_authority_id != request.replay_authority_id
            or result.lineage_id != request.lineage_id
            or result.sequence != request.sequence
            or result.next_head_hash != request.next_head_hash
            or result.request_hash != request.request_hash
            or result.receipt_hash != receipt.receipt_hash
        ):
            raise ReplayLineageAuthoritySecurityError(
                "lineage receipt and persisted result binding is invalid"
            )
        return request, result, receipt

    @staticmethod
    def _journal_hash(
        *,
        journal_index: int,
        previous_journal_hash: str,
        request: ReplayLineageAdvanceRequest,
        result: ReplayLineageOperationResult,
        receipt: ReplayLineageCheckpointReceipt,
    ) -> str:
        return canonical_sha256(
            {
                "contract": "rquant-replay-lineage-authority-journal-entry/v1",
                "entry_type": "committed_lineage_advance",
                "journal_index": journal_index,
                "operation_id": request.operation_id,
                "previous_journal_hash": previous_journal_hash,
                "receipt_hash": receipt.receipt_hash,
                "request_hash": request.request_hash,
                "result_hash": result.result_hash,
            }
        )

    def _parse_head(self, head_json: str) -> ReplayLineageHeadState:
        try:
            return ReplayLineageHeadState.model_validate_json(head_json)
        except ValidationError as exc:
            raise ReplayLineageAuthoritySecurityError("lineage head row is malformed") from exc

    def _heads_root(self, connection: sqlite3.Connection) -> str:
        heads = [
            self._parse_head(str(row["head_json"]))
            for row in connection.execute(
                "SELECT head_json FROM replay_lineage_head ORDER BY replay_authority_id"
            )
        ]
        return canonical_sha256([head.model_dump(mode="python") for head in heads])

    def _sign_checkpoint(
        self,
        *,
        operation_count: int,
        journal_root: str,
        heads_root: str,
    ) -> SignedReplayLineageCheckpoint:
        unsigned = SignedReplayLineageCheckpoint(
            schema_version=1,
            contract="rquant-replay-lineage-authority-checkpoint/v1",
            proof_type="external_anti_rollback_root",
            authority_id=self._authority_id,
            operation_count=operation_count,
            journal_root=journal_root,
            heads_root=heads_root,
            key_id=self._signer.key_id,
            signature="",
        )
        return unsigned.model_copy(
            update={
                "signature": self._signer.sign(
                    namespace=REPLAY_CLAIM_NAMESPACE,
                    payload=unsigned.signing_bytes(),
                )
            }
        )

    def _parse_checkpoint(self, checkpoint_json: str) -> SignedReplayLineageCheckpoint:
        try:
            checkpoint = SignedReplayLineageCheckpoint.model_validate_json(checkpoint_json)
        except ValidationError as exc:
            raise ReplayLineageAuthoritySecurityError(
                "signed authority checkpoint is malformed"
            ) from exc
        self._verify_checkpoint_signature(checkpoint)
        return checkpoint

    def _verify_checkpoint_signature(self, checkpoint: SignedReplayLineageCheckpoint) -> None:
        if checkpoint.authority_id != self._authority_id or not self._keyring.verify(
            issuer=checkpoint.authority_id,
            key_id=checkpoint.key_id,
            key_purpose="replay_claim",
            namespace=REPLAY_CLAIM_NAMESPACE,
            payload=checkpoint.signing_bytes(),
            signature=checkpoint.signature,
        ):
            raise ReplayLineageAuthoritySecurityError(
                "signed authority checkpoint identity or signature is invalid"
            )

    def preflight_checkpoint(
        self, checkpoint: SignedReplayLineageCheckpoint | str | bytes
    ) -> ReplayLineageAuditSummary:
        """Compare an externally retained anti-rollback root with current state.

        This backup-validation API does not replace the production monotonic store.
        Keeping a proof only inside or beside this DB does not address
        ``DB_ONLY_ROLLBACK_LIMITATION``.
        """

        supplied = self._coerce_external_checkpoint(checkpoint)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                summary = self._audit_state(connection)
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        if supplied.operation_count < summary.operation_count:
            raise ReplayLineageAuthoritySecurityError(
                "external checkpoint rollback relative to current authority state was rejected"
            )
        if supplied.operation_count > summary.operation_count:
            raise ReplayLineageAuthoritySecurityError(
                "database rollback behind the external anti-rollback root was rejected"
            )
        if (
            supplied.journal_root != summary.journal_root
            or supplied.heads_root != summary.heads_root
        ):
            raise ReplayLineageAuthoritySecurityError(
                "external checkpoint divergence from current authority state was rejected"
            )
        return summary

    def import_checkpoint(
        self, checkpoint: SignedReplayLineageCheckpoint | str | bytes
    ) -> ReplayLineageAuditSummary:
        """Import and preflight a proof without weakening its external-root role.

        No copy is stored back into the authority database. This method validates
        backup material; production high-water enforcement always comes from the
        configured :class:`MonotonicCheckpointStore`.
        """

        return self.preflight_checkpoint(checkpoint)

    def export_checkpoint(self) -> SignedReplayLineageCheckpoint:
        """Export the signed timeless root for independent durable retention."""

        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._audit_state(connection)
                row = connection.execute(
                    "SELECT checkpoint_json FROM replay_lineage_authority_meta WHERE singleton = 1"
                ).fetchone()
                if row is None:
                    raise ReplayLineageAuthoritySecurityError(
                        "signed authority checkpoint row is missing"
                    )
                checkpoint = self._parse_checkpoint(str(row["checkpoint_json"]))
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        return checkpoint

    def export_backup(self, destination: Path) -> ReplayLineageBackupExport:
        """Atomically export a consistent SQLite backup plus its signed root."""

        absolute = _absolute_path(destination)
        try:
            parent = absolute.parent.resolve(strict=True)
        except FileNotFoundError as exc:
            raise ReplayLineageAuthoritySecurityError(
                "backup destination parent must already exist"
            ) from exc
        resolved_destination = parent / absolute.name
        if resolved_destination.exists() or resolved_destination.is_symlink():
            raise ReplayLineageAuthoritySecurityError("backup destination must not already exist")
        if resolved_destination == self.path:
            raise ReplayLineageAuthoritySecurityError(
                "backup destination must differ from the authority database"
            )
        temporary = parent / f".{absolute.name}.{uuid4().hex}.sqlite-backup"
        try:
            with (
                self._connect() as source,
                sqlite3.connect(temporary, isolation_level=None) as target,
            ):
                source.backup(target)
            with self._connect_database(temporary) as copied:
                copied.execute("BEGIN IMMEDIATE")
                try:
                    self._audit_state(copied)
                    row = copied.execute(
                        "SELECT checkpoint_json FROM replay_lineage_authority_meta "
                        "WHERE singleton = 1"
                    ).fetchone()
                    if row is None:
                        raise ReplayLineageAuthoritySecurityError(
                            "signed authority checkpoint row is missing"
                        )
                    checkpoint = self._parse_checkpoint(str(row["checkpoint_json"]))
                    copied.commit()
                except BaseException:
                    copied.rollback()
                    raise
            temporary.chmod(0o600)
            os.replace(temporary, resolved_destination)
        finally:
            temporary.unlink(missing_ok=True)
        return ReplayLineageBackupExport(
            schema_version=1,
            contract="rquant-replay-lineage-authority-backup/v1",
            database_path=resolved_destination,
            checkpoint=checkpoint,
        )

    def _coerce_external_checkpoint(
        self, checkpoint: SignedReplayLineageCheckpoint | str | bytes
    ) -> SignedReplayLineageCheckpoint:
        try:
            if isinstance(checkpoint, SignedReplayLineageCheckpoint):
                supplied = SignedReplayLineageCheckpoint.model_validate(
                    checkpoint.model_dump(mode="python")
                )
            else:
                supplied = SignedReplayLineageCheckpoint.model_validate_json(checkpoint)
        except (ValidationError, TypeError, ValueError) as exc:
            raise ReplayLineageAuthoritySecurityError(
                "external checkpoint proof is malformed"
            ) from exc
        self._verify_checkpoint_signature(supplied)
        return supplied


def compose_production_replay_lineage_authority(
    path: Path,
    *,
    authority_id: str,
    signer: Ed25519ContractSigner,
    keyring: VerifyOnlyEd25519Keyring,
    broker_db_path: Path,
    replay_db_path: Path,
    monotonic_checkpoint_store: MonotonicCheckpointStore | None,
    lineage_key_paths: tuple[Path, ...],
    mode: Literal["production", "test-standalone"] = "production",
    expected_verifier_fingerprints: frozenset[str] | None = None,
    forbidden_verifier_fingerprints: frozenset[str] = frozenset(),
    busy_timeout_ms: int = 5_000,
) -> PersistentReplayLineageAuthority:
    """Compose a production authority while refusing non-production modes."""

    if mode != "production":
        raise ReplayLineageAuthoritySecurityError(
            "production composition rejects non-production authority mode"
        )
    return PersistentReplayLineageAuthority(
        path,
        authority_id=authority_id,
        signer=signer,
        keyring=keyring,
        broker_db_path=broker_db_path,
        replay_db_path=replay_db_path,
        expected_verifier_fingerprints=expected_verifier_fingerprints,
        forbidden_verifier_fingerprints=forbidden_verifier_fingerprints,
        busy_timeout_ms=busy_timeout_ms,
        monotonic_checkpoint_store=monotonic_checkpoint_store,
        lineage_key_paths=lineage_key_paths,
        mode=mode,
    )
