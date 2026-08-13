"""Explicit offline state machine for verified paper-ledger v4 copies."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import Field, computed_field

from rquant.paper_contracts import (
    PaperLedgerArchiveBinding,
    PaperLedgerArchiveTableBinding,
    PaperLedgerMigrationAttestation,
)
from rquant.paper_ledger_v4 import (
    V4LedgerReconciler,
    V4LedgerReconciliationReport,
    sha256_file,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

MIGRATION_ALGORITHM_ID = "paper-ledger-v4-to-v5-archive-v2"
TARGET_SCHEMA_IDENTITY = "paper-ledger-schema-v5-internal-4"
TARGET_SCHEMA_VERSION = 5
TARGET_INTERNAL_MIGRATION_VERSION = 4
OFFLINE_MIGRATION_PHASES = (
    "source_preflight",
    "source_reconciliation",
    "schema_additions",
    "legacy_cost_evidence",
    "archive",
    "v5_schema",
    "archive_protection",
    "attestation",
    "verification",
)
_ARCHIVE_TABLE_SPECS = (
    ("paper_ledger_schema_v4_archive", ("singleton",)),
    ("paper_ledger_attestation_v4_archive", ("revision",)),
    ("paper_ledger_head_marker_v4_archive", ("revision",)),
    ("paper_ledger_tamper_marker_v4_archive", ("tamper_id",)),
)


class PaperOfflineMigrationResult(RuntimeContractModel):
    source_path: Path
    candidate_path: Path
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    candidate_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    v4_report: V4LedgerReconciliationReport
    migration_attestation_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    anchor_state: Literal["CURRENT_HEAD_UNANCHORED", "CURRENT_HEAD_ANCHORED"]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def reconciliation_verified(self) -> bool:
        return self.v4_report.is_verified

    @computed_field  # type: ignore[prop-decorator]
    @property
    def promotion_allowed(self) -> bool:
        return self.reconciliation_verified and self.anchor_state == "CURRENT_HEAD_ANCHORED"


@dataclass(frozen=True)
class _SourceIdentity:
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True)
class _SourceSnapshot:
    path: Path
    sha256: str
    identity: _SourceIdentity


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _archive_rows(connection: sqlite3.Connection) -> tuple[dict[str, object], ...]:
    rows: list[dict[str, object]] = []
    for table, keys in _ARCHIVE_TABLE_SPECS:
        columns = tuple(str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")'))
        if not columns:
            raise ValueError(f"paper v4 archive table is missing: {table}")
        ordering = ", ".join(f'"{key}" ASC' for key in keys)
        for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY {ordering}'):
            rows.append(
                {
                    "table": table,
                    "columns": columns,
                    "values": tuple(row),
                }
            )
    return tuple(rows)


def archive_binding_payload(
    connection: sqlite3.Connection,
    *,
    source_sha256: str,
    predecessor_v4_schema_fingerprint: str,
    predecessor_v4_attestation_fingerprint: str,
    predecessor_v4_head_marker_fingerprint: str,
    source_schema_identity: str,
) -> PaperLedgerArchiveBinding:
    tables: list[PaperLedgerArchiveTableBinding] = []
    for table, keys in _ARCHIVE_TABLE_SPECS:
        columns = tuple(str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")'))
        if not columns:
            raise ValueError(f"paper v4 archive table is missing: {table}")
        tables.append(
            PaperLedgerArchiveTableBinding(
                table=table,
                columns=columns,
                source_key_ordering=keys,
            )
        )
    return PaperLedgerArchiveBinding(
        source_sha256=source_sha256,
        archive_tables=tuple(tables),
        predecessor_v4_schema_fingerprint=predecessor_v4_schema_fingerprint,
        predecessor_v4_attestation_fingerprint=predecessor_v4_attestation_fingerprint,
        predecessor_v4_head_marker_fingerprint=predecessor_v4_head_marker_fingerprint,
        source_schema_identity=source_schema_identity,
    )


def _archive_digest(
    connection: sqlite3.Connection,
    *,
    binding_payload: PaperLedgerArchiveBinding,
    migration_code_identity: str,
    target_schema_identity: str,
) -> str:
    return canonical_sha256(
        {
            "archive_rows": _archive_rows(connection),
            "archive_binding_payload": binding_payload.model_dump(mode="python"),
            "predecessor_v4_head_marker_fingerprint": (
                binding_payload.predecessor_v4_head_marker_fingerprint
            ),
            "migration_code_identity": migration_code_identity,
            "migration_algorithm_id": MIGRATION_ALGORITHM_ID,
            "source_schema_identity": binding_payload.source_schema_identity,
            "target_schema_identity": target_schema_identity,
        }
    )


def write_migration_attestation(
    connection: sqlite3.Connection,
    *,
    source_sha256: str,
    predecessor_v4_schema_fingerprint: str,
    predecessor_v4_attestation_fingerprint: str,
    predecessor_v4_head_marker_fingerprint: str,
    v4_reconciliation_report_digest: str,
    migration_code_identity: str,
    source_schema_identity: str,
    target_schema_identity: str = TARGET_SCHEMA_IDENTITY,
) -> PaperLedgerMigrationAttestation:
    binding_payload = archive_binding_payload(
        connection,
        source_sha256=source_sha256,
        predecessor_v4_schema_fingerprint=predecessor_v4_schema_fingerprint,
        predecessor_v4_attestation_fingerprint=predecessor_v4_attestation_fingerprint,
        predecessor_v4_head_marker_fingerprint=predecessor_v4_head_marker_fingerprint,
        source_schema_identity=source_schema_identity,
    )
    binding_payload_data = binding_payload.model_dump(mode="python")
    binding_fingerprint = canonical_sha256(binding_payload_data)
    archive_digest = _archive_digest(
        connection,
        binding_payload=binding_payload,
        migration_code_identity=migration_code_identity,
        target_schema_identity=target_schema_identity,
    )
    attestation = PaperLedgerMigrationAttestation(
        source_sha256=source_sha256,
        predecessor_v4_schema_fingerprint=predecessor_v4_schema_fingerprint,
        predecessor_v4_attestation_fingerprint=predecessor_v4_attestation_fingerprint,
        predecessor_v4_head_marker_fingerprint=predecessor_v4_head_marker_fingerprint,
        archive_binding_fingerprint=binding_fingerprint,
        archive_digest=archive_digest,
        v4_reconciliation_report_digest=v4_reconciliation_report_digest,
        migration_code_identity=migration_code_identity,
        source_schema_identity=source_schema_identity,
        target_schema_identity=target_schema_identity,
    )
    connection.execute(
        """
        INSERT INTO paper_ledger_v4_archive_binding(
            singleton, binding_payload_json, archive_binding_fingerprint
        ) VALUES (1, ?, ?)
        """,
        (_canonical_json(binding_payload.model_dump(mode="json")), binding_fingerprint),
    )
    connection.execute(
        """
        INSERT INTO paper_ledger_migration_attestation(
            singleton, report_json, migration_attestation_digest
        ) VALUES (1, ?, ?)
        """,
        (
            _canonical_json(attestation.model_dump(mode="json", exclude={"digest"})),
            attestation.digest,
        ),
    )
    return attestation


def validate_migration_attestation(
    connection: sqlite3.Connection,
) -> PaperLedgerMigrationAttestation:
    binding = connection.execute(
        "SELECT * FROM paper_ledger_v4_archive_binding WHERE singleton = 1"
    ).fetchone()
    row = connection.execute(
        "SELECT * FROM paper_ledger_migration_attestation WHERE singleton = 1"
    ).fetchone()
    if binding is None or row is None:
        raise ValueError("paper ledger migration attestation is missing")
    try:
        binding_payload = json.loads(str(binding["binding_payload_json"]))
        report_payload = json.loads(str(row["report_json"]))
        attestation = PaperLedgerMigrationAttestation.model_validate(report_payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("paper ledger migration attestation JSON is invalid") from exc
    expected_binding = archive_binding_payload(
        connection,
        source_sha256=attestation.source_sha256,
        predecessor_v4_schema_fingerprint=(attestation.predecessor_v4_schema_fingerprint),
        predecessor_v4_attestation_fingerprint=(attestation.predecessor_v4_attestation_fingerprint),
        predecessor_v4_head_marker_fingerprint=(attestation.predecessor_v4_head_marker_fingerprint),
        source_schema_identity=attestation.source_schema_identity,
    )
    expected_archive_digest = _archive_digest(
        connection,
        binding_payload=expected_binding,
        migration_code_identity=attestation.migration_code_identity,
        target_schema_identity=attestation.target_schema_identity,
    )
    expected_binding_data = expected_binding.model_dump(mode="python")
    if (
        canonical_sha256(binding_payload) != canonical_sha256(expected_binding_data)
        or str(binding["archive_binding_fingerprint"]) != canonical_sha256(expected_binding_data)
        or attestation.archive_binding_fingerprint != canonical_sha256(expected_binding_data)
        or attestation.archive_digest != expected_archive_digest
        or str(row["migration_attestation_digest"]) != attestation.digest
    ):
        raise ValueError("paper ledger migration archive digest mismatch")
    return attestation


def _source_identity(path: Path) -> _SourceIdentity:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise ValueError("offline migration source cannot be inspected") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise ValueError("offline migration source must be a regular file")
    return _SourceIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        size=metadata.st_size,
        modified_ns=metadata.st_mtime_ns,
        changed_ns=metadata.st_ctime_ns,
    )


def _assert_source_sidecars_absent(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        try:
            os.lstat(sidecar)
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise ValueError("offline migration source sidecars cannot be inspected") from exc
        else:
            raise ValueError("offline migration source must be closed without sidecars")


def _closed_regular_source(path: Path) -> _SourceIdentity:
    identity = _source_identity(path)
    _assert_source_sidecars_absent(path)
    return identity


def _assert_source_unchanged(path: Path, identity: _SourceIdentity) -> None:
    if _closed_regular_source(path) != identity:
        raise ValueError("offline migration source changed or was replaced during migration")


def _path_entry_exists(path: Path) -> bool:
    return os.path.lexists(path)


def _write_descriptor_copy(
    source_descriptor: int,
    destination: Path,
) -> str:
    destination_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    destination_flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        destination_descriptor = os.open(destination, destination_flags, 0o600)
    except OSError as exc:
        raise ValueError("offline migration private snapshot cannot be created") from exc
    digest = hashlib.sha256()
    try:
        while chunk := os.read(source_descriptor, 1024 * 1024):
            digest.update(chunk)
            offset = 0
            while offset < len(chunk):
                written = os.write(destination_descriptor, chunk[offset:])
                if written <= 0:
                    raise OSError("short write while creating private snapshot")
                offset += written
        os.fsync(destination_descriptor)
    except OSError as exc:
        raise ValueError("offline migration private snapshot cannot be written") from exc
    finally:
        os.close(destination_descriptor)
    return digest.hexdigest()


def _snapshot_closed_source(source: Path, snapshot: Path) -> _SourceSnapshot:
    expected_identity = _closed_regular_source(source)
    source_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    source_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        source_descriptor = os.open(source, source_flags)
    except OSError as exc:
        raise ValueError("offline migration source cannot be opened safely") from exc
    try:
        opened = os.fstat(source_descriptor)
        opened_identity = _SourceIdentity(
            device=opened.st_dev,
            inode=opened.st_ino,
            size=opened.st_size,
            modified_ns=opened.st_mtime_ns,
            changed_ns=opened.st_ctime_ns,
        )
        if not stat.S_ISREG(opened.st_mode) or opened_identity != expected_identity:
            raise ValueError("offline migration source changed before private snapshot")
        _assert_source_sidecars_absent(source)
        snapshot_sha256 = _write_descriptor_copy(source_descriptor, snapshot)
        after_copy = os.fstat(source_descriptor)
        after_copy_identity = _SourceIdentity(
            device=after_copy.st_dev,
            inode=after_copy.st_ino,
            size=after_copy.st_size,
            modified_ns=after_copy.st_mtime_ns,
            changed_ns=after_copy.st_ctime_ns,
        )
        if after_copy_identity != expected_identity:
            raise ValueError("offline migration source changed during private snapshot")
        _assert_source_unchanged(source, expected_identity)
    except BaseException:
        if snapshot.exists():
            snapshot.unlink()
        raise
    finally:
        os.close(source_descriptor)
    return _SourceSnapshot(
        path=snapshot,
        sha256=snapshot_sha256,
        identity=expected_identity,
    )


def _copy_private_snapshot(snapshot: Path, destination: Path) -> None:
    source_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0)
    source_flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        source_descriptor = os.open(snapshot, source_flags)
    except OSError as exc:
        raise ValueError("offline migration private snapshot cannot be reopened") from exc
    try:
        opened = os.fstat(source_descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise ValueError("offline migration private snapshot is not a regular file")
        _write_descriptor_copy(source_descriptor, destination)
    finally:
        os.close(source_descriptor)


def _checkpoint(failure_after_phase: str | None, phase: str) -> None:
    if failure_after_phase == phase:
        raise RuntimeError(f"simulated migration failure after {phase}")


def migrate_v4_ledger_copy(
    source_path: Path,
    candidate_path: Path,
    *,
    migration_code_identity: str,
    target_schema_identity: str = TARGET_SCHEMA_IDENTITY,
    failure_after_phase: str | None = None,
) -> PaperOfflineMigrationResult:
    if failure_after_phase not in {None, *OFFLINE_MIGRATION_PHASES}:
        raise ValueError("unsupported offline migration failure phase")
    source = Path(source_path).absolute()
    candidate = Path(candidate_path).absolute()
    if source == candidate:
        raise ValueError("offline migration source and candidate must differ")
    if _path_entry_exists(candidate):
        raise ValueError("offline migration candidate already exists")
    candidate.parent.mkdir(parents=True, exist_ok=True)
    snapshot = candidate.with_name(f".{candidate.name}.offline-source-{secrets.token_hex(12)}")
    temporary = candidate.with_name(f".{candidate.name}.offline-migrating-{secrets.token_hex(12)}")
    promoted = False
    try:
        source_snapshot = _snapshot_closed_source(source, snapshot)
        _checkpoint(failure_after_phase, "source_preflight")
        report = V4LedgerReconciler().reconcile(source_snapshot.path)
        if report.source_sha256 != source_snapshot.sha256 or not report.is_verified:
            raise ValueError("offline migration v4 reconciliation report is invalid")
        _assert_source_unchanged(source, source_snapshot.identity)
        _checkpoint(failure_after_phase, "source_reconciliation")
        _copy_private_snapshot(source_snapshot.path, temporary)
        from rquant.paper_broker import PaperBrokerStore

        store = object.__new__(PaperBrokerStore)
        store.path = temporary
        store.account_id = "offline-migration-verifier"
        store.busy_timeout_ms = 5_000
        connection = sqlite3.connect(temporary, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = FULL")
            store._ensure_ledger_schema_v5(
                connection,
                failure_after_phase=failure_after_phase,
                source_sha256=source_snapshot.sha256,
                v4_reconciliation_report_digest=report.digest,
                migration_code_identity=migration_code_identity,
                source_schema_identity=report.schema_fingerprint,
                target_schema_identity=target_schema_identity,
            )
        finally:
            connection.close()
        connection = sqlite3.connect(temporary, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            PaperBrokerStore._verify_v5_migration_in_connection(
                connection,
                expected_v4_report=report,
            )
            attestation = validate_migration_attestation(connection)
        finally:
            connection.close()
        _checkpoint(failure_after_phase, "verification")
        _assert_source_unchanged(source, source_snapshot.identity)
        candidate_sha256 = sha256_file(temporary)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        if _path_entry_exists(candidate):
            raise ValueError("offline migration candidate already exists")
        os.link(temporary, candidate)
        promoted = True
        temporary.unlink()
        directory = os.open(candidate.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return PaperOfflineMigrationResult(
            source_path=source,
            candidate_path=candidate,
            source_sha256=source_snapshot.sha256,
            candidate_sha256=candidate_sha256,
            v4_report=report,
            migration_attestation_digest=attestation.digest,
            anchor_state="CURRENT_HEAD_UNANCHORED",
        )
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        if snapshot.exists():
            snapshot.unlink()
        if promoted and _path_entry_exists(candidate) and not candidate.is_symlink():
            candidate.unlink()
        raise
    finally:
        if snapshot.exists():
            snapshot.unlink()


def migrate_paper_ledger_v4_offline_copy(
    source_path: Path,
    candidate_path: Path,
    *,
    failure_after_phase: str | None = None,
) -> PaperOfflineMigrationResult:
    return migrate_v4_ledger_copy(
        source_path,
        candidate_path,
        migration_code_identity="rquant-stage8-paper-cost-alignment",
        failure_after_phase=failure_after_phase,
    )


__all__ = [
    "MIGRATION_ALGORITHM_ID",
    "OFFLINE_MIGRATION_PHASES",
    "PaperLedgerMigrationAttestation",
    "PaperOfflineMigrationResult",
    "TARGET_INTERNAL_MIGRATION_VERSION",
    "TARGET_SCHEMA_IDENTITY",
    "archive_binding_payload",
    "migrate_paper_ledger_v4_offline_copy",
    "migrate_v4_ledger_copy",
    "validate_migration_attestation",
    "write_migration_attestation",
]
