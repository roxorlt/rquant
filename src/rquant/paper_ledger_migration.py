"""Explicit offline state machine for verified paper-ledger v4 copies."""

from __future__ import annotations

import json
import os
import secrets
import shutil
import sqlite3
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


def _closed_regular_source(path: Path) -> None:
    if not path.is_file() or path.is_symlink():
        raise ValueError("offline migration source must be a regular file")
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = Path(f"{path}{suffix}")
        if sidecar.exists():
            raise ValueError("offline migration source must be closed without sidecars")


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
    if candidate.exists():
        raise ValueError("offline migration candidate already exists")
    _closed_regular_source(source)
    source_sha256 = sha256_file(source)
    _checkpoint(failure_after_phase, "source_preflight")
    report = V4LedgerReconciler().reconcile(source)
    if report.source_sha256 != source_sha256 or not report.is_verified:
        raise ValueError("offline migration v4 reconciliation report is invalid")
    _checkpoint(failure_after_phase, "source_reconciliation")
    candidate.parent.mkdir(parents=True, exist_ok=True)
    temporary = candidate.with_name(f".{candidate.name}.offline-migrating-{secrets.token_hex(12)}")
    try:
        shutil.copyfile(source, temporary)
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
                source_sha256=report.source_sha256,
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
        if sha256_file(source) != source_sha256:
            raise ValueError("offline migration source changed during migration")
        candidate_sha256 = sha256_file(temporary)
        with temporary.open("rb") as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, candidate)
        directory = os.open(candidate.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return PaperOfflineMigrationResult(
            source_path=source,
            candidate_path=candidate,
            source_sha256=source_sha256,
            candidate_sha256=candidate_sha256,
            v4_report=report,
            migration_attestation_digest=attestation.digest,
            anchor_state="CURRENT_HEAD_UNANCHORED",
        )
    except BaseException:
        if temporary.exists():
            temporary.unlink()
        if sha256_file(source) != source_sha256:
            raise RuntimeError("offline migration source changed unexpectedly") from None
        raise


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
