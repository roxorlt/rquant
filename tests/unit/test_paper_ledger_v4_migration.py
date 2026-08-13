from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from decimal import Decimal
from pathlib import Path

import pytest

import rquant.paper_ledger_migration as paper_ledger_migration
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
    LEGACY_ACCOUNT_ID,
    create_parent_v4_fixture,
)

_PARENT_COMMIT = "c088774c3199c02edf203a3af758452eb38a5118"
_PARENT_SCHEMA_VERSION = 4
_PARENT_INTERNAL_MIGRATION_VERSION = 2
_FIXTURE_SHA256 = "250f54945a5b169355649df53c16ef4a172f6d9ca370619371216f2d96c46f82"
_SOURCE_WIDE_COUNT_TABLES = {
    "broker_account_count": "broker_account",
    "intent_count": "paper_intent",
    "order_count": "paper_order",
    "fill_count": "paper_fill",
    "lot_count": "paper_lot",
    "consumption_count": "paper_lot_consumption",
    "receipt_count": "paper_execution_receipt",
    "authority_count": "paper_account_authority",
}


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


def _assert_no_offline_migration_artifacts(candidate_path: Path) -> None:
    assert not os.path.lexists(candidate_path)
    assert not tuple(candidate_path.parent.glob(f".{candidate_path.name}.offline-source-*"))
    assert not tuple(candidate_path.parent.glob(f".{candidate_path.name}.offline-migrating-*"))


def _assert_direct_and_offline_v4_rejected(
    source_path: Path,
    candidate_path: Path,
    *,
    match: str,
) -> None:
    source_bytes = source_path.read_bytes()
    source_sha256 = _sha256(source_path)

    with pytest.raises(PaperV4ReconciliationError, match=match):
        V4LedgerReconciler().reconcile(source_path)
    assert source_path.read_bytes() == source_bytes
    assert _sha256(source_path) == source_sha256

    with pytest.raises(PaperV4ReconciliationError, match=match):
        migrate_v4_ledger_copy(
            source_path,
            candidate_path,
            migration_code_identity="test-migration-code",
        )
    assert source_path.read_bytes() == source_bytes
    assert _sha256(source_path) == source_sha256
    _assert_no_offline_migration_artifacts(candidate_path)


def _canonical_lot_ids(connection: sqlite3.Connection, account_id: str) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT lot_id FROM paper_lot WHERE account_id = ?
            ORDER BY available_date, acquisition_trade_date, buy_executed_at,
                     buy_persisted_at, buy_fill_sequence, lot_id
            """,
            (account_id,),
        ).fetchall()
    )


def _two_account_seed() -> tuple[dict[str, object], ...]:
    seed_path = Path(__file__).parents[1] / "fixtures" / "paper_ledger_v4_seed.json"
    payload = json.loads(seed_path.read_text(encoding="utf-8"))
    accounts = payload.get("accounts")
    assert isinstance(accounts, list) and len(accounts) == 2, (
        "exact-parent v4 fixture must declare two deterministic accounts"
    )
    assert all(isinstance(account, dict) for account in accounts)
    return tuple(account for account in accounts if isinstance(account, dict))


def test_v4_migration_accepts_canonical_fifo_when_lot_ids_are_inverse_chronology(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    with sqlite3.connect(source.path) as connection:
        chronological_lot_ids = _canonical_lot_ids(connection, LEGACY_ACCOUNT_ID)
        allocations = tuple(
            (str(row[0]), int(row[1]))
            for row in connection.execute(
                """
                SELECT c.lot_id, c.quantity
                FROM paper_lot_consumption AS c
                JOIN paper_lot AS l ON l.lot_id = c.lot_id
                WHERE l.account_id = ?
                ORDER BY l.available_date, l.acquisition_trade_date, l.buy_executed_at,
                         l.buy_persisted_at, l.buy_fill_sequence, l.lot_id
                """,
                (LEGACY_ACCOUNT_ID,),
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
        chronological_lot_ids = _canonical_lot_ids(connection, LEGACY_ACCOUNT_ID)
        sell_fill_id = str(
            connection.execute(
                """
                SELECT f.fill_id FROM paper_fill AS f
                JOIN paper_order AS o ON o.order_id = f.order_id
                WHERE o.account_id = ? AND o.side = 'SELL'
                """,
                (LEGACY_ACCOUNT_ID,),
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


def test_rqs8_p1_006_offline_migration_rejects_source_replacement_and_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    replacement = create_parent_v4_fixture(tmp_path / "replacement.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    source_bytes = source.path.read_bytes()

    with sqlite3.connect(replacement.path) as connection:
        connection.execute("PRAGMA application_id = 0x52513838")
    assert replacement.path.read_bytes() != source_bytes
    assert V4LedgerReconciler().reconcile(replacement.path).is_verified

    original_reconcile = V4LedgerReconciler.reconcile

    def reconcile_then_replace_and_restore(
        self: V4LedgerReconciler,
        source_path: Path,
    ) -> object:
        report = original_reconcile(self, source_path)
        parked = source.path.with_name("source-before-replacement.sqlite3")
        os.replace(source.path, parked)
        os.replace(replacement.path, source.path)
        os.replace(source.path, replacement.path)
        os.replace(parked, source.path)
        return report

    monkeypatch.setattr(
        V4LedgerReconciler,
        "reconcile",
        reconcile_then_replace_and_restore,
    )

    with pytest.raises(ValueError, match="source.*changed|source.*replaced"):
        paper_ledger_migration.migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
        )

    assert source.path.read_bytes() == source_bytes
    assert not candidate.exists()


@pytest.mark.parametrize(
    "replacement_kind",
    ("regular-file", "hardlink", "symlink"),
)
def test_rqs8_p1_006_offline_migration_rejects_private_snapshot_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_kind: str,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    replacement = create_parent_v4_fixture(tmp_path / "replacement.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    source_bytes = source.path.read_bytes()
    source_sha256 = _sha256(source.path)

    with sqlite3.connect(replacement.path) as connection:
        connection.execute("PRAGMA application_id = 0x52513838")
    assert replacement.path.read_bytes() != source_bytes
    assert V4LedgerReconciler().reconcile(replacement.path).is_verified

    original_reconcile = V4LedgerReconciler.reconcile

    def reconcile_then_replace_private_snapshot(
        self: V4LedgerReconciler,
        snapshot_path: Path,
    ) -> object:
        report = original_reconcile(self, snapshot_path)
        snapshot = Path(snapshot_path)
        snapshot.unlink()
        if replacement_kind == "regular-file":
            os.replace(replacement.path, snapshot)
        elif replacement_kind == "hardlink":
            os.link(replacement.path, snapshot)
        else:
            snapshot.symlink_to(tmp_path / "missing-private-snapshot.sqlite3")
        return report

    monkeypatch.setattr(
        V4LedgerReconciler,
        "reconcile",
        reconcile_then_replace_private_snapshot,
    )

    with pytest.raises(ValueError, match="private snapshot changed or was replaced"):
        migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
        )

    assert source.path.read_bytes() == source_bytes
    assert _sha256(source.path) == source_sha256
    _assert_no_offline_migration_artifacts(candidate)


@pytest.mark.parametrize("cross_role", (False, True), ids=("later-fill", "cross-role"))
def test_rqs8_p2_003_v4_rejects_rebound_initial_execution_receipts(
    tmp_path: Path,
    cross_role: bool,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"

    with sqlite3.connect(source.path) as connection:
        immutable_trigger = str(
            connection.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'trigger' AND name = 'paper_intent_identity_immutable'"
            ).fetchone()[0]
        )
        rebound_receipt = connection.execute(
            """
            SELECT f.execution_id, r.request_fingerprint
            FROM paper_fill AS f
            JOIN paper_execution_receipt AS r ON r.execution_id = f.execution_id
            JOIN paper_order AS o ON o.order_id = f.order_id
            WHERE o.account_id = ? AND o.side = 'BUY' AND f.sequence = 2
            """,
            (LEGACY_ACCOUNT_ID,),
        ).fetchone()
        assert rebound_receipt is not None
        target_side = "SELL" if cross_role else "BUY"
        connection.execute("DROP TRIGGER paper_intent_identity_immutable")
        connection.execute(
            """
            UPDATE paper_intent
            SET initial_execution_id = ?, initial_execution_request_fingerprint = ?
            WHERE account_id = ? AND side = ?
            """,
            (
                str(rebound_receipt[0]),
                str(rebound_receipt[1]),
                LEGACY_ACCOUNT_ID,
                target_side,
            ),
        )
        connection.execute(immutable_trigger)

    with pytest.raises(PaperV4ReconciliationError, match="initial execution"):
        V4LedgerReconciler().reconcile(source.path)
    with pytest.raises(PaperV4ReconciliationError, match="initial execution"):
        migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
        )
    assert not candidate.exists()


def test_rqs8_p2_003_v4_rejects_initial_receipt_fingerprint_only_mismatch(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"

    with sqlite3.connect(source.path) as connection:
        immutable_trigger = str(
            connection.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'trigger' AND name = 'paper_intent_identity_immutable'"
            ).fetchone()[0]
        )
        initial_execution_id = str(
            connection.execute(
                "SELECT initial_execution_id FROM paper_intent "
                "WHERE account_id = ? AND side = 'BUY'",
                (LEGACY_ACCOUNT_ID,),
            ).fetchone()[0]
        )
        connection.execute("DROP TRIGGER paper_intent_identity_immutable")
        connection.execute(
            "UPDATE paper_intent SET initial_execution_request_fingerprint = ? "
            "WHERE account_id = ? AND side = 'BUY'",
            ("f" * 64, LEGACY_ACCOUNT_ID),
        )
        connection.execute(immutable_trigger)
        retained_execution_id = str(
            connection.execute(
                "SELECT initial_execution_id FROM paper_intent "
                "WHERE account_id = ? AND side = 'BUY'",
                (LEGACY_ACCOUNT_ID,),
            ).fetchone()[0]
        )

    assert retained_execution_id == initial_execution_id
    _assert_direct_and_offline_v4_rejected(
        source.path,
        candidate,
        match="^v4 initial execution receipt fingerprint differs$",
    )


def test_rqs8_p2_003_v4_rejects_cross_account_initial_id_rebound(
    tmp_path: Path,
) -> None:
    accounts = _two_account_seed()
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    target_account, foreign_account = (str(account["account_id"]) for account in accounts)

    with sqlite3.connect(source.path) as connection:
        immutable_trigger = str(
            connection.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'trigger' AND name = 'paper_intent_identity_immutable'"
            ).fetchone()[0]
        )
        foreign_incremental = connection.execute(
            """
            SELECT f.execution_id, r.request_fingerprint
            FROM paper_fill AS f
            JOIN paper_order AS o ON o.order_id = f.order_id
            JOIN paper_execution_receipt AS r ON r.execution_id = f.execution_id
            WHERE o.account_id = ? AND f.sequence = 2
            """,
            (foreign_account,),
        ).fetchone()
        assert foreign_incremental is not None
        connection.execute("DROP TRIGGER paper_intent_identity_immutable")
        connection.execute(
            """
            UPDATE paper_intent
            SET initial_execution_id = ?, initial_execution_request_fingerprint = ?
            WHERE account_id = ? AND side = 'BUY'
            """,
            (
                str(foreign_incremental[0]),
                str(foreign_incremental[1]),
                target_account,
            ),
        )
        connection.execute(immutable_trigger)

    _assert_direct_and_offline_v4_rejected(
        source.path,
        candidate,
        match="^v4 receipt identity set is incomplete or duplicated$",
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
    assert {
        execution["side"] for account in seed["accounts"] for execution in account["executions"]
    } == {"BUY", "SELL"}
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
    assert legacy_fills == [(None, None, None, None, None, "LEGACY_UNKNOWN")] * 6


def test_rqs8_p2_004_exact_parent_fixture_replays_two_independent_accounts(
    tmp_path: Path,
) -> None:
    accounts = _two_account_seed()
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    report = V4LedgerReconciler().reconcile(source.path)
    expected_accounts = {
        str(account["account_id"]): (
            Decimal(str(account["initial_cash"])),
            Decimal(str(account["expected_cash"])),
            Decimal(str(account["expected_realized_pnl"])),
        )
        for account in accounts
    }
    observed_accounts = {
        account.account_id: (
            account.initial_cash,
            account.stored_cash,
            account.stored_realized_pnl,
        )
        for account in report.accounts
    }
    assert observed_accounts == expected_accounts

    fixture_dir = Path(__file__).parents[1] / "fixtures"
    manifest = json.loads(
        (fixture_dir / "paper_ledger_v4.manifest.json").read_text(encoding="utf-8")
    )
    with sqlite3.connect(source.path) as connection:
        observed_counts = {
            column: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
            for column, table in _SOURCE_WIDE_COUNT_TABLES.items()
        }
        head = connection.execute(
            "SELECT payload_json FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        lots = connection.execute(
            "SELECT account_id, ts_code FROM paper_lot ORDER BY account_id, lot_id"
        ).fetchall()
    assert head is not None
    assert manifest["source_wide_counts"] == observed_counts
    assert {
        column: int(json.loads(str(head[0]))[column]) for column in _SOURCE_WIDE_COUNT_TABLES
    } == observed_counts
    assert {str(row[0]) for row in lots} == set(expected_accounts)
    assert {str(row[1]) for row in lots} == {"600000.SH"}

    result = migrate_v4_ledger_copy(
        source.path,
        candidate,
        migration_code_identity="test-migration-code",
    )
    assert result.v4_report == report
    assert result.reconciliation_verified


@pytest.mark.parametrize("corruption", ("lot", "receipt"))
def test_rqs8_p2_004_v4_rejects_cross_account_lot_and_receipt_corruption(
    tmp_path: Path,
    corruption: str,
) -> None:
    accounts = _two_account_seed()
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    candidate = tmp_path / "candidate.sqlite3"
    first_account, second_account = (str(account["account_id"]) for account in accounts)

    with sqlite3.connect(source.path) as connection:
        if corruption == "lot":
            immutable_trigger = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
                    "AND name = 'paper_lot_consumption_row_immutable'"
                ).fetchone()[0]
            )
            target = connection.execute(
                """
                SELECT c.fill_id, c.lot_id
                FROM paper_lot_consumption AS c
                JOIN paper_fill AS f ON f.fill_id = c.fill_id
                JOIN paper_order AS o ON o.order_id = f.order_id
                WHERE o.account_id = ? AND o.side = 'SELL'
                LIMIT 1
                """,
                (first_account,),
            ).fetchone()
            foreign_lot = connection.execute(
                "SELECT lot_id FROM paper_lot WHERE account_id = ? ORDER BY lot_id LIMIT 1",
                (second_account,),
            ).fetchone()
            assert target is not None and foreign_lot is not None
            connection.execute("DROP TRIGGER paper_lot_consumption_row_immutable")
            connection.execute(
                "UPDATE paper_lot_consumption SET lot_id = ? WHERE fill_id = ? AND lot_id = ?",
                (str(foreign_lot[0]), str(target[0]), str(target[1])),
            )
            connection.execute(immutable_trigger)
        else:
            immutable_trigger = str(
                connection.execute(
                    "SELECT sql FROM sqlite_master WHERE type = 'trigger' "
                    "AND name = 'paper_execution_receipt_update_immutable'"
                ).fetchone()[0]
            )
            receipt = connection.execute(
                "SELECT execution_id FROM paper_execution_receipt "
                "WHERE account_id = ? ORDER BY execution_id LIMIT 1",
                (first_account,),
            ).fetchone()
            assert receipt is not None
            connection.execute("DROP TRIGGER paper_execution_receipt_update_immutable")
            connection.execute(
                "UPDATE paper_execution_receipt SET account_id = ? WHERE execution_id = ?",
                (second_account, str(receipt[0])),
            )
            connection.execute(immutable_trigger)

    with pytest.raises(PaperV4ReconciliationError):
        V4LedgerReconciler().reconcile(source.path)
    with pytest.raises(PaperV4ReconciliationError):
        migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
        )
    assert not candidate.exists()


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


def test_rqs8_p1_006_publication_failure_removes_candidate_and_private_files(
    tmp_path: Path,
) -> None:
    source = create_parent_v4_fixture(tmp_path / "source.sqlite3")
    source_bytes = source.path.read_bytes()
    candidate = tmp_path / "candidate.sqlite3"

    with pytest.raises(RuntimeError, match="simulated migration failure after publication"):
        migrate_v4_ledger_copy(
            source.path,
            candidate,
            migration_code_identity="test-migration-code",
            failure_after_phase="publication",
        )

    assert source.path.read_bytes() == source_bytes
    assert _sha256(source.path) == source.source_sha256
    _assert_no_offline_migration_artifacts(candidate)
