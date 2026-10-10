from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from rquant.paper_broker import PaperBrokerStore
import rquant.paper_ledger_migration as paper_ledger_migration
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
    LEGACY_ACCOUNT_ID,
    SECOND_LEGACY_ACCOUNT_ID,
    create_parent_v4_fixture,
)

_PARENT_COMMIT = "c088774c3199c02edf203a3af758452eb38a5118"
_PARENT_SCHEMA_VERSION = 4
_PARENT_INTERNAL_MIGRATION_VERSION = 2
_FIXTURE_SHA256 = "6c29e3c1f1a6fe12a5871ad4aea3b0f200175c4c7b6e19ab1c907a6b997be33a"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _assert_corrupt_v4_rejected(
    source_path: Path,
    candidate_path: Path,
    *,
    match: str | None = None,
) -> None:
    source_bytes = source_path.read_bytes()
    source_sha256 = _sha256(source_path)

    with pytest.raises(PaperV4ReconciliationError, match=match):
        migrate_v4_ledger_copy(
            source_path,
            candidate_path,
            migration_code_identity="test-migration-code",
        )

    assert source_path.read_bytes() == source_bytes
    assert _sha256(source_path) == source_sha256
    assert not candidate_path.exists()


def _canonical_lot_ids(connection: sqlite3.Connection) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT lot_id FROM paper_lot
            ORDER BY available_date, acquisition_trade_date, buy_executed_at,
                     buy_persisted_at, buy_fill_sequence, lot_id
            """
        ).fetchall()
    )


def test_offline_migration_rejects_source_replacement_and_restore_during_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    alternate = create_parent_v4_fixture(tmp_path / "alternate.sqlite3")
    with sqlite3.connect(alternate.path) as connection:
        connection.execute(
            "UPDATE paper_ledger_schema SET migrated_at = '2026-08-14T00:00:00.000000Z'"
        )
    assert _sha256(alternate.path) != source.source_sha256
    source_bytes = source.path.read_bytes()
    hidden_source = tmp_path / "source-hidden.sqlite3"
    candidate = tmp_path / "candidate.sqlite3"
    original_copyfile = shutil.copyfile
    original_copyfileobj = shutil.copyfileobj
    attack_attempted = False

    def replace_and_restore(copy: object) -> object:
        nonlocal attack_attempted
        if attack_attempted:
            return copy()
        attack_attempted = True
        source.path.replace(hidden_source)
        alternate.path.replace(source.path)
        try:
            return copy()
        finally:
            source.path.unlink()
            hidden_source.replace(source.path)

    def attacked_copyfile(src: object, dst: object, *args: object, **kwargs: object) -> object:
        return replace_and_restore(lambda: original_copyfile(src, dst, *args, **kwargs))

    def attacked_copyfileobj(src: object, dst: object, *args: object, **kwargs: object) -> object:
        return replace_and_restore(lambda: original_copyfileobj(src, dst, *args, **kwargs))

    monkeypatch.setattr(paper_ledger_migration.shutil, "copyfile", attacked_copyfile)
    monkeypatch.setattr(paper_ledger_migration.shutil, "copyfileobj", attacked_copyfileobj)

    with pytest.raises((RuntimeError, ValueError), match="source|snapshot|changed"):
        migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
        )

    assert attack_attempted
    assert source.path.read_bytes() == source_bytes
    assert _sha256(source.path) == source.source_sha256
    assert not candidate.exists()


@pytest.mark.parametrize("substitution", ("later_same_order", "cross_role"))
def test_v4_initial_execution_must_bind_unique_initial_receipt_and_sequence_one(
    tmp_path: Path,
    substitution: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    with sqlite3.connect(source.path) as connection:
        connection.row_factory = sqlite3.Row
        buy_intent = connection.execute(
            """
            SELECT i.intent_id, o.order_id FROM paper_intent AS i
            JOIN paper_order AS o ON o.intent_id = i.intent_id
            WHERE i.account_id = 'legacy-v4-account' AND i.side = 'BUY'
            """
        ).fetchone()
        sell_intent = connection.execute(
            """
            SELECT i.intent_id FROM paper_intent AS i
            WHERE i.account_id = 'legacy-v4-account' AND i.side = 'SELL'
            """
        ).fetchone()
        assert buy_intent is not None and sell_intent is not None
        replacement = connection.execute(
            """
            SELECT f.execution_id, r.request_fingerprint
            FROM paper_fill AS f
            JOIN paper_execution_receipt AS r ON r.execution_id = f.execution_id
            WHERE f.order_id = ? AND f.sequence = 2
            """,
            (buy_intent["order_id"],),
        ).fetchone()
        trigger_sql = connection.execute(
            """
            SELECT sql FROM sqlite_master
            WHERE type = 'trigger' AND name = 'paper_intent_identity_immutable'
            """
        ).fetchone()
        assert replacement is not None and trigger_sql is not None
        target_intent_id = (
            buy_intent["intent_id"]
            if substitution == "later_same_order"
            else sell_intent["intent_id"]
        )
        connection.execute("DROP TRIGGER paper_intent_identity_immutable")
        connection.execute(
            """
            UPDATE paper_intent
            SET initial_execution_id = ?, initial_execution_request_fingerprint = ?
            WHERE intent_id = ?
            """,
            (replacement["execution_id"], replacement["request_fingerprint"], target_intent_id),
        )
        connection.execute(str(trigger_sql[0]))

    source_bytes = source.path.read_bytes()
    source_sha256 = _sha256(source.path)
    candidate = tmp_path / "candidate.sqlite3"
    reconciliation_rejected = False
    migration_rejected = False
    try:
        V4LedgerReconciler().reconcile(source.path)
    except PaperV4ReconciliationError:
        reconciliation_rejected = True
    try:
        migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
        )
    except PaperV4ReconciliationError:
        migration_rejected = True

    assert reconciliation_rejected, "v4 reconciliation accepted substituted initial execution"
    assert migration_rejected, "offline migration accepted substituted initial execution"
    assert source.path.read_bytes() == source_bytes
    assert _sha256(source.path) == source_sha256
    assert not candidate.exists()


def test_parent_v4_two_account_fixture_reconciles_independent_same_symbol_state(
    tmp_path: Path,
) -> None:
    fixture_path = Path(__file__).parents[1] / "fixtures" / "paper_ledger_v4.sqlite3"
    source_path = tmp_path / "source.sqlite3"
    shutil.copyfile(fixture_path, source_path)
    with sqlite3.connect(source_path) as connection:
        accounts = tuple(
            tuple(row)
            for row in connection.execute(
                """
                SELECT account_id, cash, realized_pnl FROM broker_account ORDER BY account_id
                """
            )
        )
        same_symbol_accounts = tuple(
            tuple(row)
            for row in connection.execute(
                """
                SELECT account_id, SUM(original_quantity), SUM(remaining_quantity)
                FROM paper_lot WHERE ts_code = '600000.SH'
                GROUP BY account_id ORDER BY account_id
                """
            )
        )
        latest_counts = tuple(
            connection.execute(
                """
                SELECT broker_account_count, fill_count, lot_count,
                       consumption_count, receipt_count
                FROM paper_ledger_attestation ORDER BY revision DESC LIMIT 1
                """
            ).fetchone()
        )
        actual_counts = (
            int(connection.execute("SELECT COUNT(*) FROM broker_account").fetchone()[0]),
            int(connection.execute("SELECT COUNT(*) FROM paper_fill").fetchone()[0]),
            int(connection.execute("SELECT COUNT(*) FROM paper_lot").fetchone()[0]),
            int(connection.execute("SELECT COUNT(*) FROM paper_lot_consumption").fetchone()[0]),
            int(connection.execute("SELECT COUNT(*) FROM paper_execution_receipt").fetchone()[0]),
        )

    report = V4LedgerReconciler().reconcile(source_path)
    assert accounts == (
        (LEGACY_ACCOUNT_ID, "162888.1000", "2900.1000"),
        (SECOND_LEGACY_ACCOUNT_ID, "45484.5000", "487.0000"),
    )
    assert same_symbol_accounts == (
        (LEGACY_ACCOUNT_ID, 500, 200),
        (SECOND_LEGACY_ACCOUNT_ID, 200, 100),
    )
    assert latest_counts == actual_counts == (2, 5, 3, 3, 5)
    assert tuple(account.account_id for account in report.accounts) == (
        LEGACY_ACCOUNT_ID,
        SECOND_LEGACY_ACCOUNT_ID,
    )
    assert all(account.is_verified for account in report.accounts)


@pytest.mark.parametrize("corruption", ("cross_account_lot", "cross_account_receipt"))
def test_v4_two_account_reconciliation_rejects_cross_account_provenance(
    tmp_path: Path,
    corruption: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    with sqlite3.connect(source.path) as connection:
        connection.row_factory = sqlite3.Row
        if corruption == "cross_account_lot":
            consumption = connection.execute(
                """
                SELECT c.fill_id, c.lot_id FROM paper_lot_consumption AS c
                JOIN paper_fill AS f ON f.fill_id = c.fill_id
                JOIN paper_order AS o ON o.order_id = f.order_id
                WHERE o.account_id = ? ORDER BY c.lot_id LIMIT 1
                """,
                (LEGACY_ACCOUNT_ID,),
            ).fetchone()
            foreign_lot = connection.execute(
                "SELECT lot_id, unit_cost FROM paper_lot WHERE account_id = ? LIMIT 1",
                (SECOND_LEGACY_ACCOUNT_ID,),
            ).fetchone()
            trigger = connection.execute(
                """
                SELECT sql FROM sqlite_master WHERE type = 'trigger'
                AND name = 'paper_lot_consumption_row_immutable'
                """
            ).fetchone()
            assert consumption is not None and foreign_lot is not None and trigger is not None
            connection.execute("DROP TRIGGER paper_lot_consumption_row_immutable")
            connection.execute(
                """
                UPDATE paper_lot_consumption SET lot_id = ?, unit_cost = ?
                WHERE fill_id = ? AND lot_id = ?
                """,
                (
                    foreign_lot["lot_id"],
                    foreign_lot["unit_cost"],
                    consumption["fill_id"],
                    consumption["lot_id"],
                ),
            )
            connection.execute(str(trigger[0]))
        else:
            receipt = connection.execute(
                """
                SELECT r.execution_id FROM paper_execution_receipt AS r
                JOIN paper_order AS o ON o.order_id = r.order_id
                WHERE o.account_id = ? AND o.side = 'SELL' LIMIT 1
                """,
                (LEGACY_ACCOUNT_ID,),
            ).fetchone()
            trigger = connection.execute(
                """
                SELECT sql FROM sqlite_master WHERE type = 'trigger'
                AND name = 'paper_execution_receipt_update_immutable'
                """
            ).fetchone()
            assert receipt is not None and trigger is not None
            connection.execute("DROP TRIGGER paper_execution_receipt_update_immutable")
            connection.execute(
                "UPDATE paper_execution_receipt SET account_id = ? WHERE execution_id = ?",
                (SECOND_LEGACY_ACCOUNT_ID, receipt["execution_id"]),
            )
            connection.execute(str(trigger[0]))

    source_bytes = source.path.read_bytes()
    source_sha256 = _sha256(source.path)
    with pytest.raises(PaperV4ReconciliationError):
        V4LedgerReconciler().reconcile(source.path)
    _assert_corrupt_v4_rejected(source.path, tmp_path / "candidate.sqlite3")
    assert source.path.read_bytes() == source_bytes
    assert _sha256(source.path) == source_sha256


def test_v4_migration_accepts_canonical_fifo_when_lot_ids_are_inverse_chronology(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    with sqlite3.connect(source.path) as connection:
        chronological_lot_ids = _canonical_lot_ids(connection)
        allocations = tuple(
            (str(row[0]), int(row[1]))
            for row in connection.execute(
                """
                SELECT c.lot_id, c.quantity
                FROM paper_lot_consumption AS c
                JOIN paper_lot AS l ON l.lot_id = c.lot_id
                ORDER BY l.available_date, l.acquisition_trade_date, l.buy_executed_at,
                         l.buy_persisted_at, l.buy_fill_sequence, l.lot_id
                """
            ).fetchall()
        )

    assert chronological_lot_ids == tuple(sorted(chronological_lot_ids, reverse=True))
    assert allocations == (
        (chronological_lot_ids[0], 200),
        (chronological_lot_ids[1], 100),
    )
    report = V4LedgerReconciler().reconcile(source.path)
    result = migrate_v4_ledger_copy(
        source.path,
        candidate,
        migration_code_identity="test-migration-code",
    )
    assert report.is_verified
    assert result.reconciliation_verified
    assert candidate.is_file()


def test_v4_migration_rejects_non_fifo_multi_lot_consumption_before_candidate(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    with sqlite3.connect(source.path) as connection:
        chronological_lot_ids = _canonical_lot_ids(connection)
        sell_fill_id = str(
            connection.execute(
                """
                SELECT f.fill_id FROM paper_fill AS f
                JOIN paper_order AS o ON o.order_id = f.order_id
                WHERE o.side = 'SELL'
                """
            ).fetchone()[0]
        )
        immutable_trigger = str(
            connection.execute(
                """
                SELECT sql FROM sqlite_master
                WHERE type = 'trigger' AND name = 'paper_lot_consumption_row_immutable'
                """
            ).fetchone()[0]
        )
        connection.execute("DROP TRIGGER paper_lot_consumption_row_immutable")
        connection.execute(
            "UPDATE paper_lot_consumption SET quantity = 100 WHERE fill_id = ? AND lot_id = ?",
            (sell_fill_id, chronological_lot_ids[0]),
        )
        connection.execute(
            "UPDATE paper_lot_consumption SET quantity = 200 WHERE fill_id = ? AND lot_id = ?",
            (sell_fill_id, chronological_lot_ids[1]),
        )
        connection.execute(
            "UPDATE paper_lot SET remaining_quantity = 100 WHERE lot_id IN (?, ?)",
            chronological_lot_ids,
        )
        connection.execute(immutable_trigger)

    _assert_corrupt_v4_rejected(
        source.path,
        tmp_path / "candidate.sqlite3",
        match="not FIFO",
    )


@pytest.mark.parametrize(
    ("column", "value"),
    (
        ("buy_executed_at", "2099-01-01T00:00:00.000000Z"),
        ("buy_persisted_at", "2099-01-01T00:00:00.000000Z"),
        ("buy_fill_sequence", 2),
        ("acquisition_trade_date", "2026-07-30"),
        ("available_date", "2026-08-01"),
    ),
)
def test_v4_migration_rejects_corrupt_buy_lot_provenance_before_candidate(
    tmp_path: Path,
    column: str,
    value: object,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    with sqlite3.connect(source.path) as connection:
        connection.execute(f'UPDATE paper_lot SET "{column}" = ?', (value,))

    _assert_corrupt_v4_rejected(source.path, tmp_path / "candidate.sqlite3")


def test_v4_migration_rejects_consumption_with_wrong_fill_provenance(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    with sqlite3.connect(source.path) as connection:
        buy_fill_id = connection.execute(
            "SELECT fill_id FROM paper_fill ORDER BY executed_at LIMIT 1"
        ).fetchone()[0]
        lot = connection.execute(
            "SELECT lot_id, original_quantity, unit_cost, persisted_at FROM paper_lot LIMIT 1"
        ).fetchone()
        connection.execute(
            "INSERT INTO paper_lot_consumption VALUES (?, ?, ?, ?, ?)",
            (buy_fill_id, *lot),
        )

    _assert_corrupt_v4_rejected(source.path, tmp_path / "candidate.sqlite3")


def test_v4_migration_rejects_corrupt_realized_pnl_before_candidate(tmp_path: Path) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    with sqlite3.connect(source.path) as connection:
        connection.execute("UPDATE broker_account SET realized_pnl = '89.9000'")

    _assert_corrupt_v4_rejected(source.path, tmp_path / "candidate.sqlite3")


def test_v4_migration_rejects_corrupt_receipt_payload_before_candidate(tmp_path: Path) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    payload = source.path.read_bytes()
    original = b'"executable_price":"210.00"'
    corrupted = b'"executable_price":"211.00"'
    assert payload.count(original) == 1
    source.path.write_bytes(payload.replace(original, corrupted, 1))

    _assert_corrupt_v4_rejected(source.path, tmp_path / "candidate.sqlite3")


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
    assert legacy_fills == [(None, None, None, None, None, "LEGACY_UNKNOWN")] * 5


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
