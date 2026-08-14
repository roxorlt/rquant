"""Read-only loader for the exact-parent paper-ledger v4 binary fixture."""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

from rquant.paper_broker import PaperBrokerStore
from rquant.paper_ledger_v4 import V4LedgerReconciler

PARENT_V4_COMMIT = "c088774c3199c02edf203a3af758452eb38a5118"
EXPECTED_V4_FIXTURE_SHA256 = "250f54945a5b169355649df53c16ef4a172f6d9ca370619371216f2d96c46f82"
EXPECTED_V4_SCHEMA_FINGERPRINT = "3ae9e0749b1132f8b1e55f15866534e1bcae9a0815fd0d7c6837297fc45dcc70"
EXPECTED_V4_ATTESTATION_FINGERPRINT = (
    "1f0ecebc1cadcbec180f4f0602eddd011db53186f3343e08737a74970a2814b4"
)
EXPECTED_V4_HEAD_FINGERPRINT = "5dc1333dc63aa053e15405063083683c0ae2db15e655f21aa6bf433e47dc8ed1"
LEGACY_ACCOUNT_ID = "legacy-v4-account"

_ROOT = Path(__file__).resolve().parent
_BINARY = _ROOT / "paper_ledger_v4.sqlite3"
_MANIFEST = _ROOT / "paper_ledger_v4.manifest.json"
_SEED = _ROOT / "paper_ledger_v4_seed.json"
_TABLE_ORDER = {
    "broker_account": "account_id",
    "paper_intent": "intent_id",
    "paper_order": "order_id",
    "paper_fill": "fill_id",
    "paper_lot": "lot_id",
    "paper_lot_consumption": "fill_id, lot_id",
    "paper_execution_receipt": "execution_id",
    "paper_account_authority": "account_id",
    "paper_ledger_schema": "singleton",
    "paper_ledger_attestation": "revision",
    "paper_ledger_head_marker": "revision",
    "paper_ledger_tamper_marker": "tamper_id",
}


@dataclass(frozen=True)
class ParentV4Fixture:
    path: Path
    source_sha256: str
    schema_fingerprint: str
    predecessor_attestation_fingerprint: str
    predecessor_head_fingerprint: str
    historical_rows: dict[str, tuple[tuple[object, ...], ...]]

    def initial_cash_for(self, account_id: str) -> Decimal:
        matches = [
            row for row in self.historical_rows["broker_account"] if str(row[0]) == account_id
        ]
        if len(matches) != 1:
            raise AssertionError("parent-v4 fixture account identity is ambiguous")
        return Decimal(str(matches[0][1]))


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _manifest() -> dict[str, object]:
    payload = json.loads(_MANIFEST.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("parent-v4 manifest is not an object")
    return payload


def _connection(path: Path) -> sqlite3.Connection:
    uri = f"{path.absolute().as_uri()}?mode=ro&immutable=1"
    connection = sqlite3.connect(uri, uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _historical_rows(connection: sqlite3.Connection) -> dict[str, tuple[tuple[object, ...], ...]]:
    return {
        table: tuple(
            tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY {ordering}')
        )
        for table, ordering in _TABLE_ORDER.items()
    }


def create_parent_v4_fixture(path: Path) -> ParentV4Fixture:
    """Copy the frozen binary after independently checking its hardcoded identity."""

    if path.exists():
        raise ValueError("parent-v4 fixture destination must not exist")
    manifest = _manifest()
    fixture_sha256 = sha256_file(_BINARY)
    if (
        fixture_sha256 != EXPECTED_V4_FIXTURE_SHA256
        or manifest.get("fixture_sha256") != EXPECTED_V4_FIXTURE_SHA256
        or manifest.get("parent_commit") != PARENT_V4_COMMIT
        or manifest.get("schema_version") != 4
        or manifest.get("internal_migration_version") != 2
        or manifest.get("seed_sha256") != sha256_file(_SEED)
    ):
        raise AssertionError("parent-v4 fixture provenance differs from frozen identity")
    path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(_BINARY, path)
    with _connection(path) as connection:
        schema = connection.execute(
            "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1"
        ).fetchone()
        migration = connection.execute(
            "SELECT migration_version FROM paper_ledger_attestation WHERE revision = 1"
        ).fetchone()
        attestation = connection.execute(
            "SELECT schema_fingerprint, attestation_fingerprint "
            "FROM paper_ledger_attestation ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        head = connection.execute(
            "SELECT head_marker_fingerprint FROM paper_ledger_head_marker "
            "ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if (
            schema is None
            or int(schema[0]) != 4
            or migration is None
            or int(migration[0]) != 2
            or attestation is None
            or str(attestation[0]) != EXPECTED_V4_SCHEMA_FINGERPRINT
            or str(attestation[1]) != EXPECTED_V4_ATTESTATION_FINGERPRINT
            or head is None
            or str(head[0]) != EXPECTED_V4_HEAD_FINGERPRINT
        ):
            raise AssertionError("parent-v4 database trust identity differs")
        historical_rows = _historical_rows(connection)
    return ParentV4Fixture(
        path=path,
        source_sha256=fixture_sha256,
        schema_fingerprint=EXPECTED_V4_SCHEMA_FINGERPRINT,
        predecessor_attestation_fingerprint=EXPECTED_V4_ATTESTATION_FINGERPRINT,
        predecessor_head_fingerprint=EXPECTED_V4_HEAD_FINGERPRINT,
        historical_rows=historical_rows,
    )


def create_independent_v5_migration_fixture(path: Path) -> ParentV4Fixture:
    """Build a writable V5 test database directly from the frozen V4 seed."""

    fixture = create_parent_v4_fixture(path)
    report = V4LedgerReconciler().reconcile(path)
    store = object.__new__(PaperBrokerStore)
    store.path = path
    store.account_id = LEGACY_ACCOUNT_ID
    store.busy_timeout_ms = 5_000
    with sqlite3.connect(path, isolation_level=None) as connection:
        connection.row_factory = sqlite3.Row
        store._ensure_ledger_schema_v5(
            connection,
            source_sha256=fixture.source_sha256,
            v4_reconciliation_report_digest=report.digest,
            migration_code_identity="test-independent-v5-fixture",
            source_schema_identity=report.schema_fingerprint,
        )
        PaperBrokerStore._verify_v5_migration_in_connection(
            connection,
            expected_v4_report=report,
        )
    return fixture


def archived_v4_rows(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    with _connection(path) as connection:
        return {
            table.removesuffix("_v4_archive"): tuple(
                tuple(row)
                for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY {ordering}')
            )
            for table, ordering in (
                ("paper_ledger_schema_v4_archive", "singleton"),
                ("paper_ledger_attestation_v4_archive", "revision"),
                ("paper_ledger_head_marker_v4_archive", "revision"),
                ("paper_ledger_tamper_marker_v4_archive", "tamper_id"),
            )
        }


def canonical_legacy_receipt_json() -> str:
    seed = json.loads(_SEED.read_text(encoding="utf-8"))
    return json.dumps(seed, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
