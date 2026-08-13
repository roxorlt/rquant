from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from rquant.paper_broker import PaperBrokerStore
from rquant.paper_ledger_migration import (
    OFFLINE_MIGRATION_PHASES,
    migrate_v4_ledger_copy,
)
from rquant.paper_ledger_v4 import PaperV4ReconciliationError, V4LedgerReconciler
from rquant.runtime_contracts import canonical_sha256
from tests.fixtures.paper_ledger_v4_fixture import (
    EXPECTED_V4_ATTESTATION_FINGERPRINT,
    EXPECTED_V4_HEAD_FINGERPRINT,
    EXPECTED_V4_SCHEMA_FINGERPRINT,
    create_parent_v4_fixture,
)

_PARENT_COMMIT = "c088774c3199c02edf203a3af758452eb38a5118"
_PARENT_SCHEMA_VERSION = 4
_PARENT_INTERNAL_MIGRATION_VERSION = 2
_FIXTURE_SHA256 = "9dc51ed7104035a54172f850ecf6bb75f344b565d35db7845722b4acbb2a94f7"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_v4_migration_rejects_cash_not_explained_by_independent_replay(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    with sqlite3.connect(source.path) as connection:
        connection.execute(
            "UPDATE broker_account SET cash = cash + 1 WHERE account_id = ?",
            ("legacy-v4-account",),
        )
    source_bytes = source.path.read_bytes()

    with pytest.raises(PaperV4ReconciliationError, match="reconciliation"):
        migrate_v4_ledger_copy(
            source.path,
            tmp_path / "candidate.sqlite3",
            migration_code_identity="test-migration-code",
        )

    assert source.path.read_bytes() == source_bytes
    assert not (tmp_path / "candidate.sqlite3").exists()


def test_trust_inspection_rejects_coordinated_archive_and_binding_tamper(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    migrate_v4_ledger_copy(
        source.path,
        candidate,
        migration_code_identity="test-migration-code",
    )

    with sqlite3.connect(candidate) as connection:
        connection.execute("DROP TRIGGER paper_ledger_schema_v4_archive_update_immutable")
        connection.execute("DROP TRIGGER paper_ledger_v4_archive_binding_update_immutable")
        connection.execute("UPDATE paper_ledger_schema_v4_archive SET migrated_at = 'tampered'")
        binding = connection.execute(
            "SELECT binding_payload_json FROM paper_ledger_v4_archive_binding WHERE singleton = 1"
        ).fetchone()
        assert binding is not None
        binding_payload = json.loads(str(binding[0]))
        binding_payload["source_schema_identity"] = "coordinated-tamper"
        binding_json = json.dumps(
            binding_payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        connection.execute(
            """
            UPDATE paper_ledger_v4_archive_binding
            SET binding_payload_json = ?, archive_binding_fingerprint = ?
            WHERE singleton = 1
            """,
            (binding_json, canonical_sha256(binding_payload)),
        )

    store = object.__new__(PaperBrokerStore)
    store.path = candidate
    store.busy_timeout_ms = 5_000
    status = store.ledger_trust_status()
    assert status.state == "quarantined"
    assert status.reason == "migration_archive_digest_mismatch"


def test_parent_v4_binary_fixture_has_parent_provenance_and_migrates_without_downgrade(
    tmp_path: Path,
) -> None:
    fixture_dir = Path(__file__).parents[1] / "fixtures"
    fixture_path = fixture_dir / "paper_ledger_v4.sqlite3"
    manifest_path = fixture_dir / "paper_ledger_v4.manifest.json"
    seed_path = fixture_dir / "paper_ledger_v4_seed.json"

    assert fixture_path.is_file()
    assert _sha256(fixture_path) == _FIXTURE_SHA256
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    assert manifest["parent_commit"] == _PARENT_COMMIT
    assert manifest["schema_version"] == _PARENT_SCHEMA_VERSION
    assert manifest["internal_migration_version"] == _PARENT_INTERNAL_MIGRATION_VERSION
    assert manifest["fixture_sha256"] == _FIXTURE_SHA256
    assert manifest["seed_sha256"] == hashlib.sha256(seed_path.read_bytes()).hexdigest()
    assert {execution["side"] for execution in seed["executions"]} == {"BUY", "SELL"}
    assert manifest["schema_fingerprint"] == EXPECTED_V4_SCHEMA_FINGERPRINT
    assert manifest["predecessor_attestation_fingerprint"] == EXPECTED_V4_ATTESTATION_FINGERPRINT
    assert manifest["predecessor_head_fingerprint"] == EXPECTED_V4_HEAD_FINGERPRINT

    source = tmp_path / fixture_path.name
    source.write_bytes(fixture_path.read_bytes())
    candidate = tmp_path / "candidate.sqlite3"
    report = V4LedgerReconciler().reconcile(source)
    result = migrate_v4_ledger_copy(
        source,
        candidate,
        migration_code_identity="test-migration-code",
    )
    assert result.source_sha256 == _FIXTURE_SHA256
    assert result.v4_report == report
    assert result.reconciliation_verified
    assert not result.promotion_allowed
    with sqlite3.connect(candidate) as connection:
        schema_version = connection.execute("PRAGMA user_version").fetchone()[0]
        internal_version = connection.execute(
            "SELECT internal_migration_version FROM paper_ledger_schema WHERE singleton = 1"
        ).fetchone()[0]
    assert schema_version == 5
    assert internal_version == 4
    with (
        sqlite3.connect(source) as source_connection,
        sqlite3.connect(candidate) as candidate_connection,
    ):
        source_account = source_connection.execute(
            "SELECT initial_cash, cash, realized_pnl FROM broker_account ORDER BY account_id"
        ).fetchall()
        candidate_account = candidate_connection.execute(
            "SELECT initial_cash, cash, realized_pnl FROM broker_account ORDER BY account_id"
        ).fetchall()
        legacy_fills = candidate_connection.execute(
            """
            SELECT transfer_fee, total_fees, cost_spec_id, cost_spec_schema_version,
                   cost_context_fingerprint, cost_provenance_state
            FROM paper_fill ORDER BY fill_id
            """
        ).fetchall()
    assert candidate_account == source_account
    assert legacy_fills == [(None, None, None, None, None, "LEGACY_UNKNOWN")] * 2


@pytest.mark.parametrize("failure_after_phase", OFFLINE_MIGRATION_PHASES)
def test_all_offline_migration_phase_failures_leave_source_and_candidate_unchanged(
    tmp_path: Path,
    failure_after_phase: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_bytes = source.path.read_bytes()
    candidate = tmp_path / "candidate.sqlite3"

    with pytest.raises(RuntimeError, match="simulated migration failure"):
        migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
            failure_after_phase=failure_after_phase,
        )

    assert source.path.read_bytes() == source_bytes
    assert _sha256(source.path) == source.source_sha256
    assert not candidate.exists()
