"""Deterministic pre-v5 paper ledger fixture frozen at the Stage 8 parent."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

import rquant.paper_broker as paper_broker_module
from rquant.paper_broker import PaperBrokerStore

PARENT_V4_COMMIT = "c088774c3199c02edf203a3af758452eb38a5118"
# Updated only from the parent-v4 bootstrap below. It is deliberately not derived
# from a candidate-v5 database with rewritten metadata.
EXPECTED_V4_SCHEMA_FINGERPRINT = "3ae9e0749b1132f8b1e55f15866534e1bcae9a0815fd0d7c6837297fc45dcc70"
EXPECTED_V4_ATTESTATION_FINGERPRINT = (
    "cf399e4e8ed5aafaf3dc70a3e0ff52acad59f5d9057913a4458dbf5c813201a1"
)
EXPECTED_V4_HEAD_FINGERPRINT = "ff4dc081abf6341b73ed77bc741d9c456954fc4aebc834f40aed715bff9200be"
EXPECTED_V4_TABLE_COLUMNS = {
    "broker_account": (
        "account_id",
        "initial_cash",
        "cash",
        "realized_pnl",
        "cost_policy_fingerprint",
    ),
    "paper_fill": (
        "fill_id",
        "execution_id",
        "order_id",
        "sequence",
        "quantity",
        "price",
        "commission",
        "tax",
        "executed_at",
        "persisted_at",
        "price_snapshot_id",
    ),
    "paper_execution_receipt": (
        "execution_id",
        "account_id",
        "intent_id",
        "order_id",
        "request_fingerprint",
        "request_json",
        "receipt_json",
        "persisted_at",
    ),
    "paper_ledger_schema": (
        "singleton",
        "schema_version",
        "migrated_at",
        "unknown_fill_availability_count",
        "unknown_lot_availability_count",
        "unknown_consumption_availability_count",
        "unknown_lot_provenance_count",
        "unknown_intent_identity_count",
        "unknown_execution_identity_count",
        "unknown_lot_timeline_count",
        "unknown_initial_execution_identity_count",
        "unknown_execution_receipt_count",
    ),
}
EXPECTED_V4_TRIGGERS = frozenset(
    {
        "paper_intent_persisted_at_immutable",
        "paper_intent_identity_immutable",
        "paper_execution_receipt_update_immutable",
        "paper_execution_receipt_delete_immutable",
        "paper_fill_persisted_at_immutable",
        "paper_fill_row_immutable",
        "paper_fill_delete_immutable",
        "paper_lot_persisted_at_immutable",
        "paper_lot_entry_signal_id_immutable",
        "paper_lot_consumption_persisted_at_immutable",
        "paper_lot_consumption_row_immutable",
        "paper_lot_consumption_delete_immutable",
        "paper_ledger_attestation_update_immutable",
        "paper_ledger_attestation_delete_immutable",
        "paper_ledger_attestation_delete_tamper",
        "paper_ledger_head_marker_update_immutable",
        "paper_ledger_head_marker_delete_immutable",
        "paper_ledger_tamper_marker_update_immutable",
        "paper_ledger_tamper_marker_delete_immutable",
    }
)

LEGACY_ACCOUNT_ID = "legacy-v4-account"
LEGACY_INTENT_ID = "1" * 64
LEGACY_ORDER_ID = "2" * 64
LEGACY_FILL_ID = "3" * 64
LEGACY_INITIAL_EXECUTION_ID = "4" * 64
LEGACY_FILL_EXECUTION_ID = "5" * 64
LEGACY_REQUEST_FINGERPRINT = "6" * 64
LEGACY_FILL_REQUEST_FINGERPRINT = "7" * 64
LEGACY_SIGNAL_ID = "8" * 64
LEGACY_PRICE_SNAPSHOT_ID = "9" * 64
LEGACY_PRODUCER_COMMIT = "a" * 40
_PERSISTED_AT = "2026-07-31T01:31:00.000000Z"


@dataclass(frozen=True)
class ParentV4Fixture:
    path: Path
    source_sha256: str
    schema_fingerprint: str
    predecessor_attestation_fingerprint: str
    predecessor_head_fingerprint: str
    historical_rows: dict[str, tuple[tuple[object, ...], ...]]


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def create_parent_v4_fixture(path: Path) -> ParentV4Fixture:
    """Build a v4 ledger without ever creating or downgrading a v5 ledger."""

    if path.exists():
        raise ValueError("parent-v4 fixture path must not already exist")
    path.parent.mkdir(parents=True, exist_ok=True)
    store = object.__new__(PaperBrokerStore)
    store.path = path
    store.account_id = LEGACY_ACCOUNT_ID
    store.busy_timeout_ms = 5_000

    original_ensure_v4 = PaperBrokerStore._ensure_ledger_schema_v4
    original_connect = PaperBrokerStore._connect
    opened: list[sqlite3.Connection] = []

    def seed_then_attest(self: PaperBrokerStore, connection: sqlite3.Connection) -> None:
        _seed_historical_v4_rows(connection)
        original_ensure_v4(self, connection)

    def tracked_connect(self: PaperBrokerStore) -> sqlite3.Connection:
        connection = original_connect(self)
        opened.append(connection)
        return connection

    with (
        patch.object(PaperBrokerStore, "_ensure_ledger_schema_v4", seed_then_attest),
        patch.object(PaperBrokerStore, "_ensure_ledger_schema_v5", lambda self, connection: None),
        patch.object(PaperBrokerStore, "_initialize_account", lambda self, connection: None),
        patch.object(PaperBrokerStore, "_connect", tracked_connect),
        patch.object(paper_broker_module.secrets, "token_hex", return_value="d" * 64),
        patch.object(paper_broker_module, "_utc_iso", lambda _value: _PERSISTED_AT),
    ):
        PaperBrokerStore._initialize(store)
    for connection in opened:
        connection.close()

    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        connection.execute("PRAGMA journal_mode = DELETE")
        connection.execute("VACUUM")

    with _connection(path) as connection:
        schema_fingerprint = PaperBrokerStore._schema_fingerprint(
            connection,
            objects=paper_broker_module._V4_ATTESTED_SCHEMA_OBJECTS,
        )
        if schema_fingerprint is None:
            raise AssertionError("parent-v4 fixture schema cannot be fingerprinted")
        _assert_parent_v4_shape(connection, schema_fingerprint)
        attestation = connection.execute(
            "SELECT attestation_fingerprint FROM paper_ledger_attestation "
            "ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        head = connection.execute(
            "SELECT head_marker_fingerprint FROM paper_ledger_head_marker "
            "ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if attestation is None or head is None:
            raise AssertionError("parent-v4 fixture attestation head is missing")
        historical_rows = _historical_rows(connection)
    return ParentV4Fixture(
        path=path,
        source_sha256=sha256_file(path),
        schema_fingerprint=schema_fingerprint,
        predecessor_attestation_fingerprint=str(attestation[0]),
        predecessor_head_fingerprint=str(head[0]),
        historical_rows=historical_rows,
    )


def _connection(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    return connection


def _seed_historical_v4_rows(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        INSERT INTO broker_account(
            account_id, initial_cash, cash, realized_pnl, cost_policy_fingerprint
        ) VALUES (?, '10000.00', '8995.00', '0.00', 'legacy-v4-policy')
        """,
        (LEGACY_ACCOUNT_ID,),
    )
    connection.execute(
        """
        INSERT INTO paper_intent(
            intent_id, account_id, signal_id, entry_signal_id, ts_code, side,
            initial_execution_id, initial_execution_request_fingerprint,
            payload_json, persisted_at
        ) VALUES (?, ?, ?, NULL, '600000.SH', 'BUY', ?, ?, '{}', ?)
        """,
        (
            LEGACY_INTENT_ID,
            LEGACY_ACCOUNT_ID,
            LEGACY_SIGNAL_ID,
            LEGACY_INITIAL_EXECUTION_ID,
            LEGACY_REQUEST_FINGERPRINT,
            _PERSISTED_AT,
        ),
    )
    connection.execute(
        """
        INSERT INTO paper_order(
            order_id, intent_id, account_id, ts_code, side, entry_signal_id, order_type,
            quantity, filled_quantity, average_fill_price, status, reject_reason,
            created_at, updated_at
        ) VALUES (?, ?, ?, '600000.SH', 'BUY', NULL, 'MARKET', 100, 100, '10.00',
                  'FILLED', NULL, ?, ?)
        """,
        (LEGACY_ORDER_ID, LEGACY_INTENT_ID, LEGACY_ACCOUNT_ID, _PERSISTED_AT, _PERSISTED_AT),
    )
    connection.execute(
        """
        INSERT INTO paper_fill(
            fill_id, execution_id, order_id, sequence, quantity, price, commission, tax,
            executed_at, persisted_at, price_snapshot_id
        ) VALUES (?, ?, ?, 1, 100, '10.00', '5.00', '0.00', ?, ?, ?)
        """,
        (
            LEGACY_FILL_ID,
            LEGACY_FILL_EXECUTION_ID,
            LEGACY_ORDER_ID,
            _PERSISTED_AT,
            _PERSISTED_AT,
            LEGACY_PRICE_SNAPSHOT_ID,
        ),
    )
    connection.execute(
        """
        INSERT INTO paper_lot(
            lot_id, account_id, ts_code, entry_signal_id, acquisition_trade_date, available_date,
            original_quantity, remaining_quantity, unit_cost, persisted_at, buy_executed_at,
            buy_persisted_at, buy_fill_sequence
        ) VALUES (?, ?, '600000.SH', ?, '2026-07-31', '2026-08-03', 100, 100, '10.05',
                  ?, ?, ?, 1)
        """,
        (
            LEGACY_FILL_ID,
            LEGACY_ACCOUNT_ID,
            LEGACY_SIGNAL_ID,
            _PERSISTED_AT,
            _PERSISTED_AT,
            _PERSISTED_AT,
        ),
    )
    for execution_id, request_fingerprint in (
        (LEGACY_INITIAL_EXECUTION_ID, LEGACY_REQUEST_FINGERPRINT),
        (LEGACY_FILL_EXECUTION_ID, LEGACY_FILL_REQUEST_FINGERPRINT),
    ):
        connection.execute(
            """
            INSERT INTO paper_execution_receipt(
                execution_id, account_id, intent_id, order_id, request_fingerprint,
                request_json, receipt_json, persisted_at
            ) VALUES (?, ?, ?, ?, ?, '{}', '{}', ?)
            """,
            (
                execution_id,
                LEGACY_ACCOUNT_ID,
                LEGACY_INTENT_ID,
                LEGACY_ORDER_ID,
                request_fingerprint,
                _PERSISTED_AT,
            ),
        )


def _assert_parent_v4_shape(connection: sqlite3.Connection, schema_fingerprint: str) -> None:
    if schema_fingerprint != EXPECTED_V4_SCHEMA_FINGERPRINT:
        raise AssertionError(
            "parent-v4 schema fingerprint changed: "
            f"{schema_fingerprint} != {EXPECTED_V4_SCHEMA_FINGERPRINT}"
        )
    for table, expected_columns in EXPECTED_V4_TABLE_COLUMNS.items():
        columns = tuple(
            str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
        )
        if columns != expected_columns:
            raise AssertionError(f"parent-v4 {table} columns changed")
    triggers = frozenset(
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'trigger' ORDER BY name"
        ).fetchall()
    )
    if triggers != EXPECTED_V4_TRIGGERS:
        raise AssertionError("parent-v4 trigger set changed")
    schema = connection.execute(
        "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1"
    ).fetchone()
    if schema is None or int(schema[0]) != 4:
        raise AssertionError("parent-v4 schema metadata is invalid")
    attestation = connection.execute(
        "SELECT attestation_fingerprint FROM paper_ledger_attestation "
        "ORDER BY revision DESC LIMIT 1"
    ).fetchone()
    head = connection.execute(
        "SELECT head_marker_fingerprint FROM paper_ledger_head_marker "
        "ORDER BY revision DESC LIMIT 1"
    ).fetchone()
    if (
        attestation is None
        or str(attestation[0]) != EXPECTED_V4_ATTESTATION_FINGERPRINT
        or head is None
        or str(head[0]) != EXPECTED_V4_HEAD_FINGERPRINT
    ):
        raise AssertionError("parent-v4 attestation head changed")
    PaperBrokerStore._ledger_trust_status(connection)


def _historical_rows(connection: sqlite3.Connection) -> dict[str, tuple[tuple[object, ...], ...]]:
    tables = (
        "broker_account",
        "paper_intent",
        "paper_order",
        "paper_fill",
        "paper_lot",
        "paper_lot_consumption",
        "paper_execution_receipt",
        "paper_account_authority",
        "paper_ledger_schema",
        "paper_ledger_attestation",
        "paper_ledger_head_marker",
        "paper_ledger_tamper_marker",
    )
    return {
        table: tuple(
            tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')
        )
        for table in tables
    }


def archived_v4_rows(path: Path) -> dict[str, tuple[tuple[object, ...], ...]]:
    with _connection(path) as connection:
        tables = (
            "paper_ledger_schema_v4_archive",
            "paper_ledger_attestation_v4_archive",
            "paper_ledger_head_marker_v4_archive",
            "paper_ledger_tamper_marker_v4_archive",
        )
        return {
            table.removesuffix("_v4_archive"): tuple(
                tuple(row) for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')
            )
            for table in tables
        }


def canonical_legacy_receipt_json() -> str:
    """Keep the fixture's historical receipt bytes independently inspectable."""

    return json.dumps({}, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
