"""Single-writer SQLite runtime for deterministic A-share paper execution."""

from __future__ import annotations

import json
import secrets
import sqlite3
from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.order_execution_costs import calculate_execution_costs
from rquant.paper_contracts import (
    PaperAccountSnapshot,
    PaperCostProvenanceState,
    PaperExecutionCostComparison,
    PaperExecutionReceipt,
    PaperFill,
    PaperHolding,
    PaperOrder,
    PaperOrderIntent,
    PaperOrderStatus,
    PaperOrderType,
    PaperRejectReason,
    PaperSellQuantityAuthority,
    PaperSide,
)
from rquant.paper_ledger_anchor import (
    Ed25519PaperLedgerAnchorVerifier,
    PaperLedgerAnchor,
)
from rquant.research_run_spec import (
    ExecutionCostCalculation,
    ExecutionCostOrderInput,
    ExecutionCostSpec,
    InstrumentContext,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.strategy_execution_costs import (
    ExecutionCostBindingEvidence,
    compare_execution_cost_math,
)

NonNegativeDecimal = Annotated[Decimal, Field(ge=0, allow_inf_nan=False)]
PositiveDecimal = Annotated[Decimal, Field(gt=0, allow_inf_nan=False)]
_PRICE_TICK = Decimal("0.0001")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_LEDGER_UNKNOWN_COLUMNS = (
    "unknown_fill_availability_count",
    "unknown_lot_availability_count",
    "unknown_consumption_availability_count",
    "unknown_lot_provenance_count",
    "unknown_intent_identity_count",
    "unknown_execution_identity_count",
    "unknown_lot_timeline_count",
    "unknown_initial_execution_identity_count",
    "unknown_execution_receipt_count",
    "unknown_cost_provenance_count",
)
_LEDGER_MIGRATION_VERSION = 4
_V4_LEDGER_MIGRATION_VERSION = 2
_LEDGER_COUNT_TABLES = {
    "broker_account_count": "broker_account",
    "intent_count": "paper_intent",
    "order_count": "paper_order",
    "fill_count": "paper_fill",
    "lot_count": "paper_lot",
    "consumption_count": "paper_lot_consumption",
    "receipt_count": "paper_execution_receipt",
    "authority_count": "paper_account_authority",
}
_LEDGER_ATTESTATION_COUNT_COLUMNS = tuple(_LEDGER_COUNT_TABLES)
_V4_ATTESTED_SCHEMA_OBJECTS = (
    "broker_account",
    "paper_intent",
    "paper_order",
    "paper_fill",
    "paper_lot",
    "paper_lot_consumption",
    "paper_execution_receipt",
    "paper_ledger_schema",
    "paper_account_authority",
    "paper_ledger_attestation",
    "paper_ledger_head_marker",
    "paper_ledger_tamper_marker",
    "idx_paper_intent_account_signal",
    "idx_paper_intent_initial_execution",
    "idx_paper_intent_provenance",
    "idx_paper_order_position",
    "idx_paper_fill_execution_identity",
    "idx_paper_fill_order_timeline",
    "idx_paper_lot_position_fifo",
    "idx_paper_consumption_lot_pit",
    "idx_paper_execution_receipt_intent",
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
)
_V4_ARCHIVE_TABLES = (
    "paper_ledger_schema_v4_archive",
    "paper_ledger_attestation_v4_archive",
    "paper_ledger_head_marker_v4_archive",
    "paper_ledger_tamper_marker_v4_archive",
)
_V5_ATTESTED_ONLY_OBJECTS = (
    "paper_cost_spec",
    "idx_paper_fill_cost_provenance",
    "paper_execution_receipt_known_v3_required",
    "paper_cost_spec_update_immutable",
    "paper_cost_spec_delete_immutable",
    "broker_account_known_v3_required",
    "broker_account_cost_binding_immutable",
    "paper_fill_known_v3_required",
    *_V4_ARCHIVE_TABLES,
    "paper_ledger_v4_archive_binding",
    "paper_ledger_migration_attestation",
    "paper_ledger_schema_v4_archive_update_immutable",
    "paper_ledger_schema_v4_archive_delete_immutable",
    "paper_ledger_attestation_v4_archive_update_immutable",
    "paper_ledger_attestation_v4_archive_delete_immutable",
    "paper_ledger_head_marker_v4_archive_update_immutable",
    "paper_ledger_head_marker_v4_archive_delete_immutable",
    "paper_ledger_tamper_marker_v4_archive_update_immutable",
    "paper_ledger_tamper_marker_v4_archive_delete_immutable",
    "paper_ledger_v4_archive_binding_update_immutable",
    "paper_ledger_v4_archive_binding_delete_immutable",
    "paper_ledger_migration_attestation_update_immutable",
    "paper_ledger_migration_attestation_delete_immutable",
)
_ATTESTED_SCHEMA_OBJECTS = _V4_ATTESTED_SCHEMA_OBJECTS + _V5_ATTESTED_ONLY_OBJECTS
_OFFLINE_MIGRATION_PHASES = (
    "schema_additions",
    "legacy_cost_evidence",
    "archive",
    "v5_schema",
    "archive_protection",
    "attestation",
    "verification",
)


class DuplicateIntentConflictError(RuntimeError):
    """An existing intent id was reused with different immutable content."""


class DuplicateExecutionConflictError(ValueError):
    """An external execution id was reused with different immutable content."""


class PaperBrokerReconciliationError(RuntimeError):
    """The independently reconstructed ledger does not match stored balances."""


class PaperLedgerQuarantinedError(PaperBrokerReconciliationError):
    """The ledger contains legacy rows whose execution evidence is untrusted."""


class NoExecutableSellQuantityError(ValueError):
    """A nonterminal sell tranche has no legal 100-share quantity."""


class BrokerCostPolicy(RuntimeContractModel):
    """A paper broker can execute only from one explicit v3 cost authority."""

    execution_cost_spec: ExecutionCostSpec
    cost_spec_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_execution_cost_spec(self) -> BrokerCostPolicy:
        spec = ExecutionCostSpec.model_validate(self.execution_cost_spec)
        if not spec.is_alignment_eligible or spec.cost_spec_id is None:
            raise ValueError("paper broker requires an alignment-eligible v3 execution_cost_spec")
        if self.cost_spec_id is None:
            object.__setattr__(self, "cost_spec_id", spec.cost_spec_id)
        elif self.cost_spec_id != spec.cost_spec_id:
            raise ValueError("paper broker cost_spec_id must match execution_cost_spec")
        return self

    @classmethod
    def from_execution_cost_spec(cls, spec: ExecutionCostSpec) -> BrokerCostPolicy:
        validated = ExecutionCostSpec.model_validate(spec)
        if not validated.is_alignment_eligible or validated.cost_spec_id is None:
            raise ValueError("paper broker requires an explicit v3 execution_cost_spec")
        return cls(execution_cost_spec=validated, cost_spec_id=validated.cost_spec_id)

    @property
    def fingerprint(self) -> str:
        assert self.cost_spec_id is not None
        return self.cost_spec_id


class BrokerExecutionContext(RuntimeContractModel):
    """Frozen executable quote and explicit market/risk constraints."""

    executable_price: PositiveDecimal
    instrument_context: InstrumentContext
    executable_quantity: int | None = Field(default=None, ge=0, multiple_of=100)
    acquisition_available_date: date | None = None
    suspended: bool = False
    limit_locked: bool = False
    risk_rejected: bool = False


class PaperBrokerReconciliation(RuntimeContractModel):
    is_consistent: bool
    account_id: str = Field(min_length=1)
    order_count: int = Field(ge=0)
    fill_count: int = Field(ge=0)
    open_lot_quantity: int = Field(ge=0)
    cash: NonNegativeDecimal
    realized_pnl: Decimal = Field(allow_inf_nan=False)


class PaperLedgerUnknownEvidence(RuntimeContractModel):
    field: str = Field(min_length=1)
    count: int = Field(ge=1)


class PaperLedgerTrustStatus(RuntimeContractModel):
    state: Literal["trusted", "quarantined"]
    schema_version: int | None = Field(default=None, ge=1)
    reason: str = Field(min_length=1)
    unknown_evidence: tuple[PaperLedgerUnknownEvidence, ...] = ()


class PaperAccountAuthoritySnapshot(RuntimeContractModel):
    revision: int = Field(ge=1)
    state_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    snapshot: PaperAccountSnapshot

    @model_validator(mode="after")
    def validate_state_fingerprint(self) -> PaperAccountAuthoritySnapshot:
        expected = _account_state_fingerprint(self.snapshot)
        if self.state_fingerprint != expected:
            raise ValueError("paper account state_fingerprint does not match snapshot")
        return self


def _money(value: Decimal) -> str:
    if not value.is_finite():
        raise ValueError("money must be finite")
    return format(value, "f")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(UTC)


def _utc_iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _intent_payload(intent: PaperOrderIntent) -> str:
    return json.dumps(
        intent.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def _execution_request_evidence(value: Mapping[str, object]) -> tuple[str, str]:
    payload = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return payload, canonical_sha256(json.loads(payload))


def _execution_id(value: str) -> str:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError("execution_id must be a lowercase SHA-256 digest")
    return value


def _account_state_fingerprint(snapshot: PaperAccountSnapshot) -> str:
    return canonical_sha256(
        snapshot.model_dump(
            mode="python",
            exclude={"snapshot_id", "as_of_time"},
        )
    )


class PaperBrokerStore:
    """Own the only mutable paper ledger and serialize all writes through SQLite."""

    def __init__(
        self,
        path: Path,
        *,
        account_id: str,
        initial_cash: Decimal,
        cost_policy: BrokerCostPolicy,
        busy_timeout_ms: int = 5_000,
        ledger_id: str | None = None,
        ledger_anchor_path: Path | None = None,
        ledger_anchor_verifier: Ed25519PaperLedgerAnchorVerifier | None = None,
    ) -> None:
        if not account_id.strip():
            raise ValueError("account_id must not be empty")
        if not initial_cash.is_finite() or initial_cash <= 0:
            raise ValueError("initial_cash must be finite and positive")
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = Path(path)
        self.account_id = account_id.strip()
        self.initial_cash = initial_cash
        self.cost_policy = cost_policy
        self.busy_timeout_ms = busy_timeout_ms
        anchor_values = (ledger_id, ledger_anchor_path, ledger_anchor_verifier)
        if any(value is not None for value in anchor_values) and not all(
            value is not None for value in anchor_values
        ):
            raise ValueError("paper ledger anchor settings must be configured together")
        if ledger_id is not None and not ledger_id.strip():
            raise ValueError("paper ledger id must not be empty")
        self.ledger_id = None if ledger_id is None else ledger_id.strip()
        self.ledger_anchor_path = None if ledger_anchor_path is None else Path(ledger_anchor_path)
        self.ledger_anchor_verifier = ledger_anchor_verifier
        self._reject_online_v4_open()
        self._initialize()

    def _reject_online_v4_open(self) -> None:
        """Inspect an existing ledger read-only before any SQLite write pragma runs."""

        if not self.path.exists():
            return
        if not self.path.is_file() or self.path.is_symlink():
            raise PaperBrokerReconciliationError("paper ledger path is not a regular file")
        try:
            uri = f"{self.path.absolute().as_uri()}?mode=ro"
            connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        except sqlite3.DatabaseError as exc:
            raise PaperBrokerReconciliationError(
                "paper ledger cannot be inspected before opening"
            ) from exc
        try:
            row = connection.execute(
                "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1 LIMIT 1"
            ).fetchone()
        except sqlite3.DatabaseError:
            row = None
        finally:
            connection.close()
        if row is not None and int(row[0]) == 4:
            raise PaperBrokerReconciliationError(
                "offline migration required for paper ledger schema v4"
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys = ON")
            foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()
            if foreign_keys is None or int(foreign_keys[0]) != 1:
                raise PaperBrokerReconciliationError(
                    "paper ledger requires SQLite foreign_keys enforcement"
                )
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = FULL")
        except BaseException:
            connection.close()
            raise
        return connection

    def _initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            if self._has_attestation(connection):
                schema = connection.execute(
                    "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1 LIMIT 1"
                ).fetchone()
                if schema is not None and int(schema["schema_version"]) == 4:
                    raise PaperBrokerReconciliationError(
                        "offline migration required for paper ledger schema v4"
                    )
                # Reopening an existing account remains a diagnostic-only operation
                # on a quarantined v5 ledger.  Any mutation path rechecks trust.
                self._ledger_trust_status(connection)
                self._initialize_account(connection)
                return
            if self._requires_explicit_offline_audit(connection):
                self._initialize_account(connection)
                return
            self._execute_sql_statements(
                connection,
                """
                CREATE TABLE IF NOT EXISTS broker_account (
                    account_id TEXT PRIMARY KEY,
                    initial_cash TEXT NOT NULL CHECK(typeof(initial_cash) = 'text'),
                    cash TEXT NOT NULL CHECK(typeof(cash) = 'text'),
                    realized_pnl TEXT NOT NULL CHECK(typeof(realized_pnl) = 'text'),
                    cost_policy_fingerprint TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_intent (
                    intent_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    signal_id TEXT NOT NULL,
                    entry_signal_id TEXT,
                    ts_code TEXT NOT NULL,
                    side TEXT NOT NULL,
                    initial_execution_id TEXT NOT NULL UNIQUE,
                    initial_execution_request_fingerprint TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    persisted_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_order (
                    order_id TEXT PRIMARY KEY,
                    intent_id TEXT NOT NULL UNIQUE REFERENCES paper_intent(intent_id),
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    ts_code TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_signal_id TEXT,
                    order_type TEXT NOT NULL,
                    quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity % 100 = 0),
                    filled_quantity INTEGER NOT NULL CHECK(filled_quantity >= 0),
                    average_fill_price TEXT CHECK(
                        average_fill_price IS NULL OR typeof(average_fill_price) = 'text'
                    ),
                    status TEXT NOT NULL,
                    reject_reason TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_fill (
                    fill_id TEXT PRIMARY KEY,
                    execution_id TEXT NOT NULL UNIQUE,
                    order_id TEXT NOT NULL REFERENCES paper_order(order_id),
                    sequence INTEGER NOT NULL CHECK(sequence >= 1),
                    quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity % 100 = 0),
                    price TEXT NOT NULL CHECK(typeof(price) = 'text'),
                    commission TEXT NOT NULL CHECK(typeof(commission) = 'text'),
                    tax TEXT NOT NULL CHECK(typeof(tax) = 'text'),
                    executed_at TEXT NOT NULL,
                    persisted_at TEXT NOT NULL,
                    price_snapshot_id TEXT NOT NULL,
                    UNIQUE(order_id, sequence)
                );
                CREATE TABLE IF NOT EXISTS paper_lot (
                    lot_id TEXT PRIMARY KEY REFERENCES paper_fill(fill_id),
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    ts_code TEXT NOT NULL,
                    entry_signal_id TEXT NOT NULL,
                    acquisition_trade_date TEXT NOT NULL,
                    available_date TEXT NOT NULL,
                    original_quantity INTEGER NOT NULL CHECK(
                        original_quantity > 0 AND original_quantity % 100 = 0
                    ),
                    remaining_quantity INTEGER NOT NULL CHECK(
                        remaining_quantity >= 0 AND remaining_quantity % 100 = 0
                    ),
                    unit_cost TEXT NOT NULL CHECK(typeof(unit_cost) = 'text'),
                    persisted_at TEXT NOT NULL,
                    buy_executed_at TEXT NOT NULL,
                    buy_persisted_at TEXT NOT NULL,
                    buy_fill_sequence INTEGER NOT NULL CHECK(buy_fill_sequence >= 1)
                );
                CREATE TABLE IF NOT EXISTS paper_lot_consumption (
                    fill_id TEXT NOT NULL REFERENCES paper_fill(fill_id),
                    lot_id TEXT NOT NULL REFERENCES paper_lot(lot_id),
                    quantity INTEGER NOT NULL CHECK(quantity > 0 AND quantity % 100 = 0),
                    unit_cost TEXT NOT NULL CHECK(typeof(unit_cost) = 'text'),
                    persisted_at TEXT NOT NULL,
                    PRIMARY KEY(fill_id, lot_id)
                );
                CREATE TABLE IF NOT EXISTS paper_execution_receipt (
                    execution_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    intent_id TEXT NOT NULL REFERENCES paper_intent(intent_id),
                    order_id TEXT NOT NULL REFERENCES paper_order(order_id),
                    request_fingerprint TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    persisted_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_ledger_schema (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    schema_version INTEGER NOT NULL CHECK(schema_version = 4),
                    migrated_at TEXT NOT NULL,
                    unknown_fill_availability_count INTEGER NOT NULL CHECK(
                        unknown_fill_availability_count >= 0
                    ),
                    unknown_lot_availability_count INTEGER NOT NULL CHECK(
                        unknown_lot_availability_count >= 0
                    ),
                    unknown_consumption_availability_count INTEGER NOT NULL CHECK(
                        unknown_consumption_availability_count >= 0
                    ),
                    unknown_lot_provenance_count INTEGER NOT NULL CHECK(
                        unknown_lot_provenance_count >= 0
                    ),
                    unknown_intent_identity_count INTEGER NOT NULL CHECK(
                        unknown_intent_identity_count >= 0
                    ),
                    unknown_execution_identity_count INTEGER NOT NULL CHECK(
                        unknown_execution_identity_count >= 0
                    ),
                    unknown_lot_timeline_count INTEGER NOT NULL CHECK(
                        unknown_lot_timeline_count >= 0
                    ),
                    unknown_initial_execution_identity_count INTEGER NOT NULL CHECK(
                        unknown_initial_execution_identity_count >= 0
                    ),
                    unknown_execution_receipt_count INTEGER NOT NULL CHECK(
                        unknown_execution_receipt_count >= 0
                    )
                );
                CREATE TABLE IF NOT EXISTS paper_ledger_attestation (
                    revision INTEGER PRIMARY KEY CHECK(revision >= 1),
                    ledger_generation TEXT NOT NULL,
                    migration_version INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    previous_attestation_fingerprint TEXT,
                    migration_attestation_fingerprint TEXT NOT NULL,
                    event_kind TEXT NOT NULL,
                    event_fingerprint TEXT NOT NULL,
                    broker_account_count INTEGER NOT NULL CHECK(broker_account_count >= 0),
                    intent_count INTEGER NOT NULL CHECK(intent_count >= 0),
                    order_count INTEGER NOT NULL CHECK(order_count >= 0),
                    fill_count INTEGER NOT NULL CHECK(fill_count >= 0),
                    lot_count INTEGER NOT NULL CHECK(lot_count >= 0),
                    consumption_count INTEGER NOT NULL CHECK(consumption_count >= 0),
                    receipt_count INTEGER NOT NULL CHECK(receipt_count >= 0),
                    authority_count INTEGER NOT NULL CHECK(authority_count >= 0),
                    payload_json TEXT NOT NULL,
                    attestation_fingerprint TEXT NOT NULL UNIQUE,
                    persisted_at TEXT NOT NULL,
                    FOREIGN KEY(previous_attestation_fingerprint)
                        REFERENCES paper_ledger_attestation(attestation_fingerprint)
                        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
                );
                CREATE TABLE IF NOT EXISTS paper_ledger_head_marker (
                    revision INTEGER PRIMARY KEY CHECK(revision >= 1),
                    ledger_generation TEXT NOT NULL,
                    migration_version INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    attestation_fingerprint TEXT NOT NULL UNIQUE,
                    previous_head_marker_fingerprint TEXT,
                    migration_attestation_fingerprint TEXT NOT NULL,
                    broker_account_count INTEGER NOT NULL CHECK(broker_account_count >= 0),
                    intent_count INTEGER NOT NULL CHECK(intent_count >= 0),
                    order_count INTEGER NOT NULL CHECK(order_count >= 0),
                    fill_count INTEGER NOT NULL CHECK(fill_count >= 0),
                    lot_count INTEGER NOT NULL CHECK(lot_count >= 0),
                    consumption_count INTEGER NOT NULL CHECK(consumption_count >= 0),
                    receipt_count INTEGER NOT NULL CHECK(receipt_count >= 0),
                    authority_count INTEGER NOT NULL CHECK(authority_count >= 0),
                    payload_json TEXT NOT NULL,
                    head_marker_fingerprint TEXT NOT NULL UNIQUE,
                    persisted_at TEXT NOT NULL,
                    FOREIGN KEY(attestation_fingerprint)
                        REFERENCES paper_ledger_attestation(attestation_fingerprint)
                        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
                    FOREIGN KEY(previous_head_marker_fingerprint)
                        REFERENCES paper_ledger_head_marker(head_marker_fingerprint)
                        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
                );
                CREATE TABLE IF NOT EXISTS paper_ledger_tamper_marker (
                    tamper_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_revision INTEGER NOT NULL,
                    target_attestation_fingerprint TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detected_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_account_authority (
                    account_id TEXT PRIMARY KEY REFERENCES broker_account(account_id),
                    revision INTEGER NOT NULL CHECK(revision >= 1),
                    state_fingerprint TEXT NOT NULL,
                    producer_commit TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL
                );
                """,
            )
            self._ensure_ledger_schema_v4(connection)
            self._ensure_ledger_schema_v5(connection)
            self._initialize_account(connection)

    @staticmethod
    def _table_exists(connection: sqlite3.Connection, table: str) -> bool:
        return (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
                (table,),
            ).fetchone()
            is not None
        )

    @classmethod
    def _has_attestation(cls, connection: sqlite3.Connection) -> bool:
        return cls._table_exists(connection, "paper_ledger_attestation") and (
            connection.execute(
                "SELECT 1 FROM paper_ledger_attestation WHERE revision = 1 LIMIT 1"
            ).fetchone()
            is not None
        )

    @classmethod
    def _requires_explicit_offline_audit(cls, connection: sqlite3.Connection) -> bool:
        if not cls._table_exists(connection, "paper_ledger_schema"):
            return False
        schema = connection.execute(
            "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1 LIMIT 1"
        ).fetchone()
        if schema is not None and int(schema["schema_version"]) in {2, 3}:
            return False
        for table in _LEDGER_COUNT_TABLES.values():
            if (
                cls._table_exists(connection, table)
                and connection.execute(f'SELECT 1 FROM "{table}" LIMIT 1').fetchone() is not None
            ):
                return True
        return False

    def _initialize_account(self, connection: sqlite3.Connection) -> None:
        row = connection.execute(
            """
            SELECT initial_cash, cost_policy_fingerprint, cost_spec_id,
                   cost_spec_schema_version, cost_provenance_state
            FROM broker_account WHERE account_id = ?
            """,
            (self.account_id,),
        ).fetchone()
        if row is not None:
            if Decimal(row["initial_cash"]) != self.initial_cash:
                raise ValueError("existing account initial_cash does not match")
            if row["cost_provenance_state"] == PaperCostProvenanceState.LEGACY_UNKNOWN.value:
                return
            if (
                row["cost_provenance_state"] != PaperCostProvenanceState.KNOWN_V3.value
                or row["cost_spec_schema_version"] != 3
                or row["cost_spec_id"] != self.cost_policy.cost_spec_id
                or row["cost_policy_fingerprint"] != self.cost_policy.fingerprint
            ):
                raise ValueError("existing account v3 cost binding does not match")
            return
        self._require_schema_integrity(connection)
        connection.execute("BEGIN IMMEDIATE")
        try:
            self._require_schema_integrity(connection)
            existing = connection.execute(
                "SELECT 1 FROM broker_account WHERE account_id = ? LIMIT 1",
                (self.account_id,),
            ).fetchone()
            if existing is None:
                self._ensure_paper_cost_spec_authority(connection)
                assert self.cost_policy.cost_spec_id is not None
                connection.execute(
                    """
                    INSERT INTO broker_account(
                        account_id, initial_cash, cash, realized_pnl, cost_policy_fingerprint,
                        cost_spec_id, cost_spec_schema_version, cost_provenance_state
                    ) VALUES (?, ?, ?, ?, ?, ?, 3, 'KNOWN_V3')
                    """,
                    (
                        self.account_id,
                        _money(self.initial_cash),
                        _money(self.initial_cash),
                        "0",
                        self.cost_policy.fingerprint,
                        self.cost_policy.cost_spec_id,
                    ),
                )
                self._append_ledger_attestation(
                    connection,
                    event_kind="account_bootstrap",
                    event_fingerprint=canonical_sha256(
                        {
                            "account_id": self.account_id,
                            "initial_cash": self.initial_cash,
                            "cost_spec_id": self.cost_policy.cost_spec_id,
                            "cost_spec_schema_version": 3,
                        }
                    ),
                    count_deltas={"broker_account_count": 1},
                )
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _ensure_paper_cost_spec_authority(self, connection: sqlite3.Connection) -> None:
        spec = self.cost_policy.execution_cost_spec
        assert spec.cost_spec_id is not None
        assert spec.cost_engine_version is not None
        canonical_json = spec.canonical_json()
        row = connection.execute(
            "SELECT * FROM paper_cost_spec WHERE cost_spec_id = ?",
            (spec.cost_spec_id,),
        ).fetchone()
        if row is None:
            connection.execute(
                """
                INSERT INTO paper_cost_spec(
                    cost_spec_id, schema_version, cost_engine_version, canonical_json, persisted_at
                ) VALUES (?, 3, ?, ?, ?)
                """,
                (
                    spec.cost_spec_id,
                    spec.cost_engine_version,
                    canonical_json,
                    _utc_iso(datetime.now(UTC)),
                ),
            )
            return
        if (
            row["schema_version"] != 3
            or row["cost_engine_version"] != spec.cost_engine_version
            or row["canonical_json"] != canonical_json
        ):
            raise PaperBrokerReconciliationError(
                "paper cost spec authority conflicts with the active v3 execution cost spec"
            )

    @staticmethod
    def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
        return {
            str(row[1]) for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
        }

    @staticmethod
    def _schema_fingerprint(
        connection: sqlite3.Connection,
        *,
        objects: tuple[str, ...] = _ATTESTED_SCHEMA_OBJECTS,
    ) -> str | None:
        placeholders = ",".join("?" for _ in objects)
        rows = connection.execute(
            f"""
            SELECT type, name, sql FROM sqlite_master
            WHERE name IN ({placeholders})
            ORDER BY type, name
            """,
            objects,
        ).fetchall()
        if {str(row["name"]) for row in rows} != set(objects):
            return None
        return canonical_sha256(
            tuple(
                {
                    "type": str(row["type"]),
                    "name": str(row["name"]),
                    "sql": " ".join(str(row["sql"]).split()),
                }
                for row in rows
            )
        )

    @classmethod
    def _validate_v4_archive(cls, connection: sqlite3.Connection) -> None:
        from rquant.paper_ledger_migration import validate_migration_attestation

        archive_schema = connection.execute(
            "SELECT * FROM paper_ledger_schema_v4_archive WHERE singleton = 1 LIMIT 1"
        ).fetchone()
        archive_attestation = connection.execute(
            "SELECT * FROM paper_ledger_attestation_v4_archive ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        archive_head = connection.execute(
            "SELECT * FROM paper_ledger_head_marker_v4_archive ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if archive_schema is None or archive_attestation is None or archive_head is None:
            raise PaperBrokerReconciliationError("paper ledger v4 archive facts are missing")
        attestation_payload = cls._validate_attestation_row(archive_attestation)
        cls._validate_head_marker_row(archive_head)
        try:
            migration = validate_migration_attestation(connection)
        except (TypeError, ValueError, sqlite3.DatabaseError) as exc:
            raise PaperBrokerReconciliationError(
                "paper ledger migration archive digest mismatch"
            ) from exc
        if (
            int(archive_schema["schema_version"]) != 4
            or str(archive_attestation["attestation_fingerprint"])
            != migration.predecessor_v4_attestation_fingerprint
            or str(archive_head["head_marker_fingerprint"])
            != migration.predecessor_v4_head_marker_fingerprint
            or str(archive_head["attestation_fingerprint"])
            != str(archive_attestation["attestation_fingerprint"])
            or int(attestation_payload["schema_version"]) != 4
            or str(attestation_payload["schema_fingerprint"])
            != migration.predecessor_v4_schema_fingerprint
            or str(archive_head["schema_fingerprint"])
            != migration.predecessor_v4_schema_fingerprint
        ):
            raise PaperBrokerReconciliationError("paper ledger v4 archive integrity conflict")

    @classmethod
    def _verify_v5_migration_in_connection(
        cls,
        connection: sqlite3.Connection,
        *,
        expected_v4_report: object | None = None,
    ) -> None:
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or str(integrity[0]).lower() != "ok":
            raise PaperBrokerReconciliationError("paper ledger candidate integrity check failed")
        cls._validate_v4_archive(connection)
        from rquant.paper_ledger_migration import validate_migration_attestation

        migration_attestation = validate_migration_attestation(connection)
        if expected_v4_report is not None and (
            not bool(getattr(expected_v4_report, "is_verified", False))
            or str(getattr(expected_v4_report, "digest", ""))
            != migration_attestation.v4_reconciliation_report_digest
            or str(getattr(expected_v4_report, "source_sha256", ""))
            != migration_attestation.source_sha256
        ):
            raise PaperBrokerReconciliationError(
                "paper ledger candidate v4 reconciliation report differs"
            )
        status = cls._ledger_trust_status(connection)
        if status.schema_version != 5 or status.reason not in {
            "verified_schema_v5",
            "unknown_legacy_execution_evidence",
        }:
            raise PaperBrokerReconciliationError("paper ledger candidate trust verification failed")

    @staticmethod
    def _attestation_payload(row: sqlite3.Row) -> dict[str, object]:
        return {
            "revision": int(row["revision"]),
            "ledger_generation": str(row["ledger_generation"]),
            "migration_version": int(row["migration_version"]),
            "schema_version": int(row["schema_version"]),
            "schema_fingerprint": str(row["schema_fingerprint"]),
            "previous_attestation_fingerprint": row["previous_attestation_fingerprint"],
            "migration_attestation_fingerprint": str(row["migration_attestation_fingerprint"]),
            "event_kind": str(row["event_kind"]),
            "event_fingerprint": str(row["event_fingerprint"]),
            **{column: int(row[column]) for column in _LEDGER_ATTESTATION_COUNT_COLUMNS},
            "persisted_at": str(row["persisted_at"]),
        }

    @classmethod
    def _validate_attestation_row(cls, row: sqlite3.Row) -> dict[str, object]:
        payload = cls._attestation_payload(row)
        try:
            persisted_payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError) as exc:
            raise PaperBrokerReconciliationError(
                "paper ledger attestation payload is invalid"
            ) from exc
        fingerprint = canonical_sha256(payload)
        if persisted_payload != payload or str(row["attestation_fingerprint"]) != fingerprint:
            raise PaperBrokerReconciliationError(
                "paper ledger attestation content binding is invalid"
            )
        return payload

    @staticmethod
    def _head_marker_payload(row: sqlite3.Row) -> dict[str, object]:
        return {
            "revision": int(row["revision"]),
            "ledger_generation": str(row["ledger_generation"]),
            "migration_version": int(row["migration_version"]),
            "schema_version": int(row["schema_version"]),
            "schema_fingerprint": str(row["schema_fingerprint"]),
            "attestation_fingerprint": str(row["attestation_fingerprint"]),
            "previous_head_marker_fingerprint": row["previous_head_marker_fingerprint"],
            "migration_attestation_fingerprint": str(row["migration_attestation_fingerprint"]),
            **{column: int(row[column]) for column in _LEDGER_ATTESTATION_COUNT_COLUMNS},
            "persisted_at": str(row["persisted_at"]),
        }

    @classmethod
    def _validate_head_marker_row(cls, row: sqlite3.Row) -> dict[str, object]:
        payload = cls._head_marker_payload(row)
        try:
            persisted_payload = json.loads(str(row["payload_json"]))
        except (TypeError, ValueError) as exc:
            raise PaperBrokerReconciliationError(
                "paper ledger canonical head payload is invalid"
            ) from exc
        fingerprint = canonical_sha256(payload)
        if persisted_payload != payload or str(row["head_marker_fingerprint"]) != fingerprint:
            raise PaperBrokerReconciliationError(
                "paper ledger canonical head content binding is invalid"
            )
        return payload

    @classmethod
    def _validate_canonical_head(
        cls,
        connection: sqlite3.Connection,
        *,
        migration: sqlite3.Row,
        latest: sqlite3.Row,
        migration_payload: Mapping[str, object],
        latest_payload: Mapping[str, object],
    ) -> None:
        tamper = connection.execute(
            "SELECT tamper_id FROM paper_ledger_tamper_marker ORDER BY tamper_id DESC LIMIT 1"
        ).fetchone()
        if tamper is not None:
            raise PaperBrokerReconciliationError(
                "paper ledger attestation deletion tamper marker is present"
            )
        migration_marker = connection.execute(
            "SELECT * FROM paper_ledger_head_marker WHERE revision = 1 LIMIT 1"
        ).fetchone()
        latest_marker = connection.execute(
            "SELECT * FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if migration_marker is None or latest_marker is None:
            raise PaperBrokerReconciliationError("paper ledger canonical head marker is missing")
        migration_marker_payload = cls._validate_head_marker_row(migration_marker)
        latest_marker_payload = cls._validate_head_marker_row(latest_marker)
        if (
            int(migration_marker["revision"]) != 1
            or migration_marker_payload["previous_head_marker_fingerprint"] is not None
            or str(migration_marker["attestation_fingerprint"])
            != str(migration["attestation_fingerprint"])
            or int(latest_marker["revision"]) != int(latest["revision"])
            or str(latest_marker["attestation_fingerprint"])
            != str(latest["attestation_fingerprint"])
        ):
            raise PaperBrokerReconciliationError(
                "paper ledger canonical head conflicts with attestation head"
            )
        compared_fields = (
            "ledger_generation",
            "migration_version",
            "schema_version",
            "schema_fingerprint",
            "migration_attestation_fingerprint",
            *_LEDGER_ATTESTATION_COUNT_COLUMNS,
            "persisted_at",
        )
        for field in compared_fields:
            if migration_marker_payload[field] != migration_payload[field]:
                raise PaperBrokerReconciliationError(
                    "paper ledger canonical migration marker conflicts with attestation"
                )
            if latest_marker_payload[field] != latest_payload[field]:
                raise PaperBrokerReconciliationError(
                    "paper ledger canonical head content conflicts with attestation"
                )
        latest_revision = int(latest_marker["revision"])
        if latest_revision > 1:
            previous_marker = connection.execute(
                "SELECT head_marker_fingerprint FROM paper_ledger_head_marker "
                "WHERE revision = ? LIMIT 1",
                (latest_revision - 1,),
            ).fetchone()
            if previous_marker is None or str(previous_marker["head_marker_fingerprint"]) != str(
                latest_marker["previous_head_marker_fingerprint"]
            ):
                raise PaperBrokerReconciliationError(
                    "paper ledger canonical head chain is detached"
                )

    @classmethod
    def _attestation_head(
        cls,
        connection: sqlite3.Connection,
    ) -> tuple[sqlite3.Row, sqlite3.Row]:
        migration = connection.execute(
            "SELECT * FROM paper_ledger_attestation WHERE revision = 1 LIMIT 1"
        ).fetchone()
        latest = connection.execute(
            "SELECT * FROM paper_ledger_attestation ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if migration is None or latest is None:
            raise PaperBrokerReconciliationError("paper ledger attestation is missing")
        migration_payload = cls._validate_attestation_row(migration)
        latest_payload = cls._validate_attestation_row(latest)
        if (
            migration_payload["event_kind"] != "migration_audit"
            or migration_payload["previous_attestation_fingerprint"] is not None
            or latest_payload["ledger_generation"] != migration_payload["ledger_generation"]
            or latest_payload["migration_attestation_fingerprint"]
            != migration_payload["migration_attestation_fingerprint"]
        ):
            raise PaperBrokerReconciliationError(
                "paper ledger migration attestation conflicts with trust head"
            )
        latest_revision = int(latest["revision"])
        if latest_revision > 1:
            previous = connection.execute(
                "SELECT attestation_fingerprint FROM paper_ledger_attestation "
                "WHERE revision = ? LIMIT 1",
                (latest_revision - 1,),
            ).fetchone()
            if previous is None or str(previous["attestation_fingerprint"]) != str(
                latest["previous_attestation_fingerprint"]
            ):
                raise PaperBrokerReconciliationError(
                    "paper ledger attestation head was rolled back or detached"
                )
        for column in _LEDGER_ATTESTATION_COUNT_COLUMNS:
            if int(latest[column]) < int(migration[column]):
                raise PaperBrokerReconciliationError(
                    "paper ledger attestation count rolled back below migration baseline"
                )
        cls._validate_canonical_head(
            connection,
            migration=migration,
            latest=latest,
            migration_payload=migration_payload,
            latest_payload=latest_payload,
        )
        return migration, latest

    @classmethod
    def _insert_attestation(
        cls,
        connection: sqlite3.Connection,
        payload: Mapping[str, object],
    ) -> str:
        fingerprint = canonical_sha256(payload)
        connection.execute(
            """
            INSERT INTO paper_ledger_attestation(
                revision, ledger_generation, migration_version, schema_version,
                schema_fingerprint, previous_attestation_fingerprint,
                migration_attestation_fingerprint, event_kind, event_fingerprint,
                broker_account_count, intent_count, order_count, fill_count,
                lot_count, consumption_count, receipt_count, authority_count,
                payload_json, attestation_fingerprint, persisted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["revision"],
                payload["ledger_generation"],
                payload["migration_version"],
                payload["schema_version"],
                payload["schema_fingerprint"],
                payload["previous_attestation_fingerprint"],
                payload["migration_attestation_fingerprint"],
                payload["event_kind"],
                payload["event_fingerprint"],
                *(payload[column] for column in _LEDGER_ATTESTATION_COUNT_COLUMNS),
                json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
                fingerprint,
                payload["persisted_at"],
            ),
        )
        revision = int(payload["revision"])
        previous_marker_fingerprint: str | None = None
        if revision > 1:
            previous_marker = connection.execute(
                "SELECT head_marker_fingerprint FROM paper_ledger_head_marker "
                "WHERE revision = ? LIMIT 1",
                (revision - 1,),
            ).fetchone()
            if previous_marker is None:
                raise PaperBrokerReconciliationError(
                    "paper ledger canonical head predecessor is missing"
                )
            previous_marker_fingerprint = str(previous_marker["head_marker_fingerprint"])
        marker_payload: dict[str, object] = {
            "revision": revision,
            "ledger_generation": payload["ledger_generation"],
            "migration_version": payload["migration_version"],
            "schema_version": payload["schema_version"],
            "schema_fingerprint": payload["schema_fingerprint"],
            "attestation_fingerprint": fingerprint,
            "previous_head_marker_fingerprint": previous_marker_fingerprint,
            "migration_attestation_fingerprint": payload["migration_attestation_fingerprint"],
            **{column: payload[column] for column in _LEDGER_ATTESTATION_COUNT_COLUMNS},
            "persisted_at": payload["persisted_at"],
        }
        marker_fingerprint = canonical_sha256(marker_payload)
        connection.execute(
            """
            INSERT INTO paper_ledger_head_marker(
                revision, ledger_generation, migration_version, schema_version,
                schema_fingerprint, attestation_fingerprint,
                previous_head_marker_fingerprint, migration_attestation_fingerprint,
                broker_account_count, intent_count, order_count, fill_count,
                lot_count, consumption_count, receipt_count, authority_count,
                payload_json, head_marker_fingerprint, persisted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                marker_payload["revision"],
                marker_payload["ledger_generation"],
                marker_payload["migration_version"],
                marker_payload["schema_version"],
                marker_payload["schema_fingerprint"],
                marker_payload["attestation_fingerprint"],
                marker_payload["previous_head_marker_fingerprint"],
                marker_payload["migration_attestation_fingerprint"],
                *(marker_payload[column] for column in _LEDGER_ATTESTATION_COUNT_COLUMNS),
                json.dumps(
                    marker_payload,
                    ensure_ascii=True,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                marker_fingerprint,
                marker_payload["persisted_at"],
            ),
        )
        return fingerprint

    def _append_ledger_attestation(
        self,
        connection: sqlite3.Connection,
        *,
        event_kind: str,
        event_fingerprint: str,
        count_deltas: Mapping[str, int],
    ) -> None:
        migration, latest = self._attestation_head(connection)
        unknown_deltas = set(count_deltas) - set(_LEDGER_ATTESTATION_COUNT_COLUMNS)
        if unknown_deltas or any(value < 0 for value in count_deltas.values()):
            raise ValueError("ledger attestation count deltas are invalid")
        persisted_at = _utc_iso(datetime.now(UTC))
        payload: dict[str, object] = {
            "revision": int(latest["revision"]) + 1,
            "ledger_generation": str(latest["ledger_generation"]),
            "migration_version": _LEDGER_MIGRATION_VERSION,
            "schema_version": 5,
            "schema_fingerprint": str(latest["schema_fingerprint"]),
            "previous_attestation_fingerprint": str(latest["attestation_fingerprint"]),
            "migration_attestation_fingerprint": str(
                migration["migration_attestation_fingerprint"]
            ),
            "event_kind": event_kind,
            "event_fingerprint": event_fingerprint,
            **{
                column: int(latest[column]) + int(count_deltas.get(column, 0))
                for column in _LEDGER_ATTESTATION_COUNT_COLUMNS
            },
            "persisted_at": persisted_at,
        }
        self._insert_attestation(connection, payload)

    @classmethod
    def _ledger_trust_status(
        cls,
        connection: sqlite3.Connection,
    ) -> PaperLedgerTrustStatus:
        if "paper_ledger_schema" not in {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
                ("paper_ledger_schema",),
            ).fetchall()
        }:
            return PaperLedgerTrustStatus(
                state="quarantined",
                reason="schema_metadata_missing",
            )
        columns = cls._table_columns(connection, "paper_ledger_schema")
        required = {
            "schema_version",
            "internal_migration_version",
            *_LEDGER_UNKNOWN_COLUMNS,
        }
        if not required <= columns:
            return PaperLedgerTrustStatus(
                state="quarantined",
                reason="schema_metadata_incomplete",
            )
        row = connection.execute(
            "SELECT * FROM paper_ledger_schema WHERE singleton = 1 LIMIT 1"
        ).fetchone()
        if row is None:
            return PaperLedgerTrustStatus(
                state="quarantined",
                reason="schema_metadata_missing",
            )
        schema_version = int(row["schema_version"])
        unknown = tuple(
            PaperLedgerUnknownEvidence(field=column, count=int(row[column]))
            for column in _LEDGER_UNKNOWN_COLUMNS
            if int(row[column]) > 0
        )
        if schema_version != 5:
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="unsupported_schema_version",
                unknown_evidence=unknown,
            )
        if int(row["internal_migration_version"]) != _LEDGER_MIGRATION_VERSION:
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="unsupported_internal_migration_version",
                unknown_evidence=unknown,
            )
        if not cls._table_exists(connection, "paper_ledger_attestation"):
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="migration_attestation_missing",
            )
        try:
            cls._validate_v4_archive(connection)
        except (PaperBrokerReconciliationError, sqlite3.DatabaseError):
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="migration_archive_digest_mismatch",
            )
        schema_fingerprint = cls._schema_fingerprint(connection)
        if schema_fingerprint is None:
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="attested_schema_incomplete",
            )
        try:
            migration, latest = cls._attestation_head(connection)
        except PaperBrokerReconciliationError:
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="migration_attestation_conflict",
            )
        try:
            from rquant.paper_ledger_migration import validate_migration_attestation

            immutable_migration = validate_migration_attestation(connection)
        except (TypeError, ValueError, sqlite3.DatabaseError):
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="migration_archive_digest_mismatch",
                unknown_evidence=unknown,
            )
        if (
            int(migration["migration_version"]) != _LEDGER_MIGRATION_VERSION
            or int(latest["migration_version"]) != _LEDGER_MIGRATION_VERSION
            or int(migration["schema_version"]) != schema_version
            or int(latest["schema_version"]) != schema_version
            or str(migration["schema_fingerprint"]) != schema_fingerprint
            or str(latest["schema_fingerprint"]) != schema_fingerprint
            or str(migration["migration_attestation_fingerprint"]) != immutable_migration.digest
            or str(latest["migration_attestation_fingerprint"]) != immutable_migration.digest
        ):
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="migration_attestation_schema_conflict",
            )
        if unknown:
            return PaperLedgerTrustStatus(
                state="quarantined",
                schema_version=schema_version,
                reason="unknown_legacy_execution_evidence",
                unknown_evidence=unknown,
            )
        return PaperLedgerTrustStatus(
            state="trusted",
            schema_version=schema_version,
            reason="verified_schema_v5",
        )

    @staticmethod
    def _raise_if_ledger_untrusted(status: PaperLedgerTrustStatus) -> None:
        if status.state == "trusted":
            return
        detail = ", ".join(f"{item.field}={item.count}" for item in status.unknown_evidence)
        suffix = "" if not detail else f" ({detail})"
        raise PaperLedgerQuarantinedError(
            "paper ledger is quarantined/untrusted: "
            f"{status.reason}{suffix}; explicit offline repair is required"
        )

    def ledger_trust_status(self) -> PaperLedgerTrustStatus:
        """Return bounded schema-evidence diagnostics without reading trade history."""

        with self._connect() as connection:
            return self._ledger_trust_status(connection)

    def require_trusted_ledger(self) -> None:
        """Fail unless this account has a known immutable v3 cost binding."""

        with self._connect() as connection:
            self._require_execution_ready(connection)

    def _require_trusted_ledger(self, connection: sqlite3.Connection) -> None:
        self._require_execution_ready(connection)

    def _require_schema_integrity(self, connection: sqlite3.Connection) -> None:
        """Permit a fresh v5 account beside quarantined legacy evidence only."""

        status = self._ledger_trust_status(connection)
        if status.state == "trusted":
            return
        if status.schema_version == 5 and status.reason == "unknown_legacy_execution_evidence":
            return
        self._raise_if_ledger_untrusted(status)

    def _require_execution_ready(self, connection: sqlite3.Connection) -> None:
        self._require_schema_integrity(connection)
        row = connection.execute(
            """
            SELECT cost_spec_id, cost_spec_schema_version, cost_provenance_state
            FROM broker_account WHERE account_id = ?
            """,
            (self.account_id,),
        ).fetchone()
        if row is None:
            raise PaperBrokerReconciliationError("paper broker account is missing")
        if (
            row["cost_provenance_state"] != PaperCostProvenanceState.KNOWN_V3.value
            or row["cost_spec_schema_version"] != 3
            or row["cost_spec_id"] != self.cost_policy.cost_spec_id
        ):
            raise PaperLedgerQuarantinedError(
                "paper broker account is quarantined and audit-only because its cost "
                "provenance is not bound to the active v3 execution cost spec"
            )
        self._require_active_cost_spec_authority(connection)

    def _require_active_cost_spec_authority(self, connection: sqlite3.Connection) -> None:
        spec = self.cost_policy.execution_cost_spec
        assert spec.cost_spec_id is not None
        assert spec.cost_engine_version is not None
        authority = connection.execute(
            """
            SELECT schema_version, cost_engine_version, canonical_json
            FROM paper_cost_spec WHERE cost_spec_id = ?
            """,
            (spec.cost_spec_id,),
        ).fetchone()
        if (
            authority is None
            or authority["schema_version"] != 3
            or authority["cost_engine_version"] != spec.cost_engine_version
            or authority["canonical_json"] != spec.canonical_json()
        ):
            raise PaperLedgerQuarantinedError(
                "paper broker account is quarantined and audit-only because its v3 cost "
                "authority does not match the active execution cost spec"
            )

    def _ensure_ledger_schema_v4(self, connection: sqlite3.Connection) -> None:
        additions = {
            "paper_intent": {
                "signal_id": "TEXT",
                "entry_signal_id": "TEXT",
                "ts_code": "TEXT",
                "side": "TEXT",
                "initial_execution_id": "TEXT",
                "initial_execution_request_fingerprint": "TEXT",
            },
            "paper_order": {"entry_signal_id": "TEXT"},
            "paper_fill": {
                "persisted_at": "TEXT",
                "execution_id": "TEXT",
            },
            "paper_lot": {
                "entry_signal_id": "TEXT",
                "persisted_at": "TEXT",
                "buy_executed_at": "TEXT",
                "buy_persisted_at": "TEXT",
                "buy_fill_sequence": "INTEGER",
            },
            "paper_lot_consumption": {"persisted_at": "TEXT"},
        }
        connection.execute("BEGIN IMMEDIATE")
        try:
            for table, columns in additions.items():
                existing = self._table_columns(connection, table)
                for name, sql_type in columns.items():
                    if name not in existing:
                        connection.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {sql_type}')
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_execution_receipt (
                    execution_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL REFERENCES broker_account(account_id),
                    intent_id TEXT NOT NULL REFERENCES paper_intent(intent_id),
                    order_id TEXT NOT NULL REFERENCES paper_order(order_id),
                    request_fingerprint TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    persisted_at TEXT NOT NULL
                )
                """
            )
            legacy_intents = connection.execute(
                """
                SELECT intent_id, payload_json FROM paper_intent
                WHERE signal_id IS NULL OR ts_code IS NULL OR side IS NULL
                """
            ).fetchall()
            for row in legacy_intents:
                try:
                    intent = PaperOrderIntent.model_validate_json(row["payload_json"])
                except (TypeError, ValueError):
                    continue
                if intent.intent_id != row["intent_id"]:
                    continue
                connection.execute(
                    """
                    UPDATE paper_intent
                    SET signal_id = ?, entry_signal_id = ?, ts_code = ?, side = ?
                    WHERE intent_id = ?
                    """,
                    (
                        intent.signal_id,
                        intent.entry_signal_id,
                        intent.ts_code,
                        intent.side.value,
                        intent.intent_id,
                    ),
                )
            connection.execute(
                """
                UPDATE paper_lot
                SET buy_executed_at = (
                        SELECT executed_at FROM paper_fill WHERE fill_id = paper_lot.lot_id
                    ),
                    buy_persisted_at = (
                        SELECT persisted_at FROM paper_fill WHERE fill_id = paper_lot.lot_id
                    ),
                    buy_fill_sequence = (
                        SELECT sequence FROM paper_fill WHERE fill_id = paper_lot.lot_id
                    )
                WHERE buy_executed_at IS NULL OR buy_persisted_at IS NULL
                   OR buy_fill_sequence IS NULL
                """
            )
            unknown = {
                "fill": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paper_fill WHERE persisted_at IS NULL"
                    ).fetchone()[0]
                ),
                "lot": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paper_lot WHERE persisted_at IS NULL"
                    ).fetchone()[0]
                ),
                "consumption": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paper_lot_consumption WHERE persisted_at IS NULL"
                    ).fetchone()[0]
                ),
                "provenance": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paper_lot WHERE entry_signal_id IS NULL"
                    ).fetchone()[0]
                ),
                "intent_identity": int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM paper_intent
                        WHERE signal_id IS NULL OR ts_code IS NULL OR side IS NULL
                        """
                    ).fetchone()[0]
                ),
                "execution_identity": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paper_fill WHERE execution_id IS NULL"
                    ).fetchone()[0]
                ),
                "lot_timeline": int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM paper_lot
                        WHERE buy_executed_at IS NULL OR buy_persisted_at IS NULL
                           OR buy_fill_sequence IS NULL
                        """
                    ).fetchone()[0]
                ),
                "initial_execution_identity": int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM paper_intent
                        WHERE initial_execution_id IS NULL
                           OR initial_execution_request_fingerprint IS NULL
                        """
                    ).fetchone()[0]
                ),
                "execution_receipt": int(
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM (
                            SELECT initial_execution_id AS execution_id
                            FROM paper_intent
                            UNION
                            SELECT execution_id FROM paper_fill
                        ) AS expected
                        LEFT JOIN paper_execution_receipt AS receipt
                          ON receipt.execution_id = expected.execution_id
                        WHERE expected.execution_id IS NOT NULL
                          AND receipt.execution_id IS NULL
                        """
                    ).fetchone()[0]
                ),
            }
            existing_schema = connection.execute(
                "SELECT schema_version FROM paper_ledger_schema WHERE singleton = 1"
            ).fetchone()
            if existing_schema is not None and int(existing_schema[0]) not in {2, 3, 4}:
                raise PaperBrokerReconciliationError(
                    "unsupported paper ledger schema requires explicit migration"
                )
            schema_columns = self._table_columns(connection, "paper_ledger_schema")
            if existing_schema is not None and (
                int(existing_schema[0]) in {2, 3}
                or "unknown_execution_identity_count" not in schema_columns
                or "unknown_execution_receipt_count" not in schema_columns
            ):
                connection.execute(
                    "ALTER TABLE paper_ledger_schema RENAME TO paper_ledger_schema_legacy"
                )
                connection.execute(
                    """
                    CREATE TABLE paper_ledger_schema (
                        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                        schema_version INTEGER NOT NULL CHECK(schema_version = 4),
                        migrated_at TEXT NOT NULL,
                        unknown_fill_availability_count INTEGER NOT NULL CHECK(
                            unknown_fill_availability_count >= 0
                        ),
                        unknown_lot_availability_count INTEGER NOT NULL CHECK(
                            unknown_lot_availability_count >= 0
                        ),
                        unknown_consumption_availability_count INTEGER NOT NULL CHECK(
                            unknown_consumption_availability_count >= 0
                        ),
                        unknown_lot_provenance_count INTEGER NOT NULL CHECK(
                            unknown_lot_provenance_count >= 0
                        ),
                        unknown_intent_identity_count INTEGER NOT NULL CHECK(
                            unknown_intent_identity_count >= 0
                        ),
                        unknown_execution_identity_count INTEGER NOT NULL CHECK(
                            unknown_execution_identity_count >= 0
                        ),
                        unknown_lot_timeline_count INTEGER NOT NULL CHECK(
                            unknown_lot_timeline_count >= 0
                        ),
                        unknown_initial_execution_identity_count INTEGER NOT NULL CHECK(
                            unknown_initial_execution_identity_count >= 0
                        ),
                        unknown_execution_receipt_count INTEGER NOT NULL CHECK(
                            unknown_execution_receipt_count >= 0
                        )
                    )
                    """
                )
                connection.execute("DROP TABLE paper_ledger_schema_legacy")
            connection.execute(
                """
                INSERT INTO paper_ledger_schema(
                    singleton, schema_version, migrated_at,
                    unknown_fill_availability_count,
                    unknown_lot_availability_count,
                    unknown_consumption_availability_count,
                    unknown_lot_provenance_count,
                    unknown_intent_identity_count,
                    unknown_execution_identity_count,
                    unknown_lot_timeline_count,
                    unknown_initial_execution_identity_count,
                    unknown_execution_receipt_count
                ) VALUES (1, 4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(singleton) DO UPDATE SET
                    schema_version = 4,
                    unknown_fill_availability_count = excluded.unknown_fill_availability_count,
                    unknown_lot_availability_count = excluded.unknown_lot_availability_count,
                    unknown_consumption_availability_count =
                        excluded.unknown_consumption_availability_count,
                    unknown_lot_provenance_count = excluded.unknown_lot_provenance_count,
                    unknown_intent_identity_count = excluded.unknown_intent_identity_count,
                    unknown_execution_identity_count = excluded.unknown_execution_identity_count,
                    unknown_lot_timeline_count = excluded.unknown_lot_timeline_count,
                    unknown_initial_execution_identity_count =
                        excluded.unknown_initial_execution_identity_count,
                    unknown_execution_receipt_count = excluded.unknown_execution_receipt_count
                """,
                (
                    unknown["fill"],
                    unknown["lot"],
                    unknown["consumption"],
                    unknown["provenance"],
                    unknown["intent_identity"],
                    unknown["execution_identity"],
                    unknown["lot_timeline"],
                    unknown["initial_execution_identity"],
                    unknown["execution_receipt"],
                ),
            )
            self._execute_sql_statements(
                connection,
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_intent_account_signal
                ON paper_intent(account_id, signal_id) WHERE signal_id IS NOT NULL;
                CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_intent_initial_execution
                ON paper_intent(initial_execution_id)
                WHERE initial_execution_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_paper_intent_provenance
                ON paper_intent(account_id, ts_code, entry_signal_id, side, signal_id);
                CREATE INDEX IF NOT EXISTS idx_paper_order_position
                ON paper_order(account_id, ts_code, entry_signal_id, side, order_id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_paper_fill_execution_identity
                ON paper_fill(execution_id) WHERE execution_id IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_paper_fill_order_timeline
                ON paper_fill(order_id, sequence, executed_at, persisted_at);
                CREATE INDEX IF NOT EXISTS idx_paper_lot_position_fifo
                ON paper_lot(
                    account_id, ts_code, entry_signal_id,
                    available_date, acquisition_trade_date,
                    buy_executed_at, buy_persisted_at, buy_fill_sequence, lot_id
                );
                CREATE INDEX IF NOT EXISTS idx_paper_consumption_lot_pit
                ON paper_lot_consumption(lot_id, persisted_at, fill_id);
                CREATE INDEX IF NOT EXISTS idx_paper_execution_receipt_intent
                ON paper_execution_receipt(account_id, intent_id, execution_id);
                CREATE TRIGGER IF NOT EXISTS paper_intent_persisted_at_immutable
                BEFORE UPDATE OF persisted_at ON paper_intent
                WHEN NEW.persisted_at IS NOT OLD.persisted_at
                BEGIN SELECT RAISE(ABORT, 'paper_intent persisted_at is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_intent_identity_immutable
                BEFORE UPDATE OF signal_id, entry_signal_id, ts_code, side,
                                 initial_execution_id,
                                 initial_execution_request_fingerprint ON paper_intent
                BEGIN SELECT RAISE(ABORT, 'paper_intent identity is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_execution_receipt_update_immutable
                BEFORE UPDATE ON paper_execution_receipt
                BEGIN SELECT RAISE(ABORT, 'paper execution receipt is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_execution_receipt_delete_immutable
                BEFORE DELETE ON paper_execution_receipt
                BEGIN SELECT RAISE(ABORT, 'paper execution receipt is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_fill_persisted_at_immutable
                BEFORE UPDATE OF persisted_at ON paper_fill
                WHEN NEW.persisted_at IS NOT OLD.persisted_at
                BEGIN SELECT RAISE(ABORT, 'paper_fill persisted_at is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_fill_row_immutable
                BEFORE UPDATE ON paper_fill
                BEGIN SELECT RAISE(ABORT, 'paper_fill row is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_fill_delete_immutable
                BEFORE DELETE ON paper_fill
                BEGIN SELECT RAISE(ABORT, 'paper_fill row is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_lot_persisted_at_immutable
                BEFORE UPDATE OF persisted_at ON paper_lot
                WHEN NEW.persisted_at IS NOT OLD.persisted_at
                BEGIN SELECT RAISE(ABORT, 'paper_lot persisted_at is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_lot_entry_signal_id_immutable
                BEFORE UPDATE OF entry_signal_id ON paper_lot
                WHEN NEW.entry_signal_id IS NOT OLD.entry_signal_id
                BEGIN SELECT RAISE(ABORT, 'paper_lot entry_signal_id is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_lot_consumption_persisted_at_immutable
                BEFORE UPDATE OF persisted_at ON paper_lot_consumption
                WHEN NEW.persisted_at IS NOT OLD.persisted_at
                BEGIN SELECT RAISE(ABORT, 'paper_lot_consumption persisted_at is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_lot_consumption_row_immutable
                BEFORE UPDATE ON paper_lot_consumption
                BEGIN SELECT RAISE(ABORT, 'paper_lot_consumption row is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_lot_consumption_delete_immutable
                BEFORE DELETE ON paper_lot_consumption
                BEGIN SELECT RAISE(ABORT, 'paper_lot_consumption row is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_ledger_attestation_update_immutable
                BEFORE UPDATE ON paper_ledger_attestation
                BEGIN SELECT RAISE(ABORT, 'paper ledger attestation is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_ledger_attestation_delete_immutable
                BEFORE DELETE ON paper_ledger_attestation
                BEGIN SELECT RAISE(ABORT, 'paper ledger attestation is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_ledger_attestation_delete_tamper
                AFTER DELETE ON paper_ledger_attestation
                BEGIN
                    INSERT INTO paper_ledger_tamper_marker(
                        target_revision, target_attestation_fingerprint,
                        reason, detected_at
                    ) VALUES (
                        OLD.revision, OLD.attestation_fingerprint,
                        'attestation_deleted',
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    );
                END;
                CREATE TRIGGER IF NOT EXISTS paper_ledger_head_marker_update_immutable
                BEFORE UPDATE ON paper_ledger_head_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger canonical head is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_ledger_head_marker_delete_immutable
                BEFORE DELETE ON paper_ledger_head_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger canonical head is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_ledger_tamper_marker_update_immutable
                BEFORE UPDATE ON paper_ledger_tamper_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger tamper marker is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS paper_ledger_tamper_marker_delete_immutable
                BEFORE DELETE ON paper_ledger_tamper_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger tamper marker is immutable'); END;
                """,
            )
            if not connection.in_transaction:
                connection.execute("BEGIN IMMEDIATE")
            schema_fingerprint = self._schema_fingerprint(
                connection,
                objects=_V4_ATTESTED_SCHEMA_OBJECTS,
            )
            if schema_fingerprint is None:
                raise PaperBrokerReconciliationError(
                    "paper ledger schema cannot be attested after migration"
                )
            counts = {
                column: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
                for column, table in _LEDGER_COUNT_TABLES.items()
            }
            migration_report = {
                "migration_version": _V4_LEDGER_MIGRATION_VERSION,
                "schema_version": 4,
                "schema_fingerprint": schema_fingerprint,
                "unknown_evidence": unknown,
                "counts": counts,
            }
            migration_fingerprint = canonical_sha256(migration_report)
            persisted_at = _utc_iso(datetime.now(UTC))
            self._insert_attestation(
                connection,
                {
                    "revision": 1,
                    "ledger_generation": secrets.token_hex(32),
                    "migration_version": _V4_LEDGER_MIGRATION_VERSION,
                    "schema_version": 4,
                    "schema_fingerprint": schema_fingerprint,
                    "previous_attestation_fingerprint": None,
                    "migration_attestation_fingerprint": migration_fingerprint,
                    "event_kind": "migration_audit",
                    "event_fingerprint": migration_fingerprint,
                    **counts,
                    "persisted_at": persisted_at,
                },
            )
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise

    @staticmethod
    def _execute_sql_statements(connection: sqlite3.Connection, script: str) -> None:
        """Use SQLite's statement parser so a pending transaction is never implicitly committed."""

        statement = ""
        for line in script.splitlines(keepends=True):
            statement += line
            if sqlite3.complete_statement(statement):
                sql = statement.strip()
                if sql:
                    connection.execute(sql)
                statement = ""
        if statement.strip():
            raise PaperBrokerReconciliationError("paper migration SQL is incomplete")

    @staticmethod
    def _migration_checkpoint(
        failure_after_phase: str | None,
        phase: str,
    ) -> None:
        if failure_after_phase == phase:
            raise PaperBrokerReconciliationError(f"simulated migration failure after {phase}")

    def _ensure_ledger_schema_v5(
        self,
        connection: sqlite3.Connection,
        *,
        failure_after_phase: str | None = None,
        source_sha256: str | None = None,
        v4_reconciliation_report_digest: str | None = None,
        migration_code_identity: str = "rquant-fresh-v5-bootstrap",
        source_schema_identity: str | None = None,
        target_schema_identity: str = "paper-ledger-schema-v5-internal-4",
    ) -> None:
        """Promote v4 evidence without assigning fees that were never persisted."""

        if failure_after_phase not in {None, *_OFFLINE_MIGRATION_PHASES}:
            raise ValueError("unsupported offline migration failure phase")

        schema_row = connection.execute(
            "SELECT * FROM paper_ledger_schema WHERE singleton = 1 LIMIT 1"
        ).fetchone()
        if schema_row is None:
            raise PaperBrokerReconciliationError("paper ledger schema metadata is missing")
        schema_version = int(schema_row["schema_version"])
        if schema_version == 5:
            if (
                "internal_migration_version" not in schema_row
                or int(schema_row["internal_migration_version"]) != _LEDGER_MIGRATION_VERSION
            ):
                raise PaperBrokerReconciliationError(
                    "paper ledger schema v5 uses an unsupported internal migration version"
                )
            return
        if schema_version != 4:
            raise PaperBrokerReconciliationError(
                "unsupported paper ledger schema requires explicit migration"
            )

        previous_v4_schema_fingerprint = self._schema_fingerprint(
            connection,
            objects=_V4_ATTESTED_SCHEMA_OBJECTS,
        )
        if previous_v4_schema_fingerprint is None:
            raise PaperBrokerReconciliationError("paper ledger v4 schema cannot be attested")
        _previous_migration, previous_attestation = self._attestation_head(connection)
        previous_head = connection.execute(
            "SELECT * FROM paper_ledger_head_marker ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        if (
            previous_head is None
            or int(previous_attestation["schema_version"]) != 4
            or str(previous_attestation["schema_fingerprint"]) != previous_v4_schema_fingerprint
            or str(previous_head["attestation_fingerprint"])
            != str(previous_attestation["attestation_fingerprint"])
            or str(previous_head["schema_fingerprint"]) != previous_v4_schema_fingerprint
        ):
            raise PaperBrokerReconciliationError("paper ledger v4 attestation head is invalid")
        previous_v4_attestation = str(previous_attestation["attestation_fingerprint"])
        previous_v4_head = str(previous_head["head_marker_fingerprint"])
        source_sha256 = source_sha256 or canonical_sha256(
            {
                "kind": "fresh-v5-bootstrap",
                "schema_fingerprint": previous_v4_schema_fingerprint,
            }
        )
        v4_reconciliation_report_digest = v4_reconciliation_report_digest or canonical_sha256(
            {
                "kind": "fresh-v5-empty-v4-report",
                "schema_fingerprint": previous_v4_schema_fingerprint,
            }
        )
        source_schema_identity = source_schema_identity or previous_v4_schema_fingerprint
        schema_columns = set(schema_row.keys())
        old_unknown = {
            column: int(schema_row[column])
            for column in _LEDGER_UNKNOWN_COLUMNS
            if column in schema_columns
        }
        connection.execute("BEGIN IMMEDIATE")
        try:
            additions = {
                "broker_account": {
                    "cost_spec_id": "TEXT",
                    "cost_spec_schema_version": "INTEGER",
                    "cost_provenance_state": "TEXT",
                },
                "paper_fill": {
                    "transfer_fee": "TEXT",
                    "total_fees": "TEXT",
                    "cost_spec_id": "TEXT",
                    "cost_spec_schema_version": "INTEGER",
                    "cost_context_fingerprint": "TEXT",
                    "cost_provenance_state": "TEXT",
                },
                "paper_execution_receipt": {
                    "transfer_fee": "TEXT",
                    "total_fees": "TEXT",
                    "cost_spec_id": "TEXT",
                    "cost_spec_schema_version": "INTEGER",
                    "cost_context_fingerprint": "TEXT",
                    "cost_provenance_state": "TEXT",
                },
            }
            for table, columns in additions.items():
                existing = self._table_columns(connection, table)
                for name, sql_type in columns.items():
                    if name not in existing:
                        connection.execute(f'ALTER TABLE "{table}" ADD COLUMN "{name}" {sql_type}')

            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS paper_cost_spec (
                    cost_spec_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL CHECK(schema_version = 3),
                    cost_engine_version TEXT NOT NULL,
                    canonical_json TEXT NOT NULL,
                    persisted_at TEXT NOT NULL,
                    UNIQUE(schema_version, cost_engine_version, canonical_json)
                )
                """
            )
            self._migration_checkpoint(failure_after_phase, "schema_additions")
            for trigger in (
                "paper_execution_receipt_update_immutable",
                "paper_execution_receipt_delete_immutable",
                "paper_execution_receipt_known_v3_required",
                "paper_cost_spec_update_immutable",
                "paper_cost_spec_delete_immutable",
                "broker_account_known_v3_required",
                "paper_fill_persisted_at_immutable",
                "paper_fill_row_immutable",
                "paper_fill_delete_immutable",
                "paper_fill_known_v3_required",
                "broker_account_cost_binding_immutable",
                "paper_ledger_attestation_update_immutable",
                "paper_ledger_attestation_delete_immutable",
                "paper_ledger_attestation_delete_tamper",
                "paper_ledger_head_marker_update_immutable",
                "paper_ledger_head_marker_delete_immutable",
                "paper_ledger_tamper_marker_update_immutable",
                "paper_ledger_tamper_marker_delete_immutable",
            ):
                connection.execute(f'DROP TRIGGER IF EXISTS "{trigger}"')

            # Existing v4 rows have no receipt-level v3 authority.  Preserve their
            # balances and old fields exactly, rather than reconstructing a fee split.
            connection.execute(
                """
                UPDATE broker_account
                SET cost_spec_id = NULL,
                    cost_spec_schema_version = NULL,
                    cost_provenance_state = 'LEGACY_UNKNOWN'
                """
            )
            connection.execute(
                """
                UPDATE paper_fill
                SET transfer_fee = NULL,
                    total_fees = NULL,
                    cost_spec_id = NULL,
                    cost_spec_schema_version = NULL,
                    cost_context_fingerprint = NULL,
                    cost_provenance_state = 'LEGACY_UNKNOWN'
                """
            )

            connection.execute(
                """
                UPDATE paper_execution_receipt
                SET transfer_fee = NULL,
                    total_fees = NULL,
                    cost_spec_id = NULL,
                    cost_spec_schema_version = NULL,
                    cost_context_fingerprint = NULL,
                    cost_provenance_state = 'LEGACY_UNKNOWN'
                """
            )

            legacy_cost_evidence = {
                "account": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM broker_account "
                        "WHERE cost_provenance_state = 'LEGACY_UNKNOWN'"
                    ).fetchone()[0]
                ),
                "fill": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paper_fill "
                        "WHERE cost_provenance_state = 'LEGACY_UNKNOWN'"
                    ).fetchone()[0]
                ),
                "receipt": int(
                    connection.execute(
                        "SELECT COUNT(*) FROM paper_execution_receipt "
                        "WHERE cost_provenance_state = 'LEGACY_UNKNOWN'"
                    ).fetchone()[0]
                ),
            }
            self._migration_checkpoint(failure_after_phase, "legacy_cost_evidence")

            for table, archive in (
                ("paper_ledger_head_marker", "paper_ledger_head_marker_v4_archive"),
                ("paper_ledger_tamper_marker", "paper_ledger_tamper_marker_v4_archive"),
                ("paper_ledger_attestation", "paper_ledger_attestation_v4_archive"),
                ("paper_ledger_schema", "paper_ledger_schema_v4_archive"),
            ):
                if self._table_exists(connection, archive):
                    raise PaperBrokerReconciliationError(
                        "paper ledger v4 archive namespace is already occupied"
                    )
                connection.execute(f'ALTER TABLE "{table}" RENAME TO "{archive}"')
            self._migration_checkpoint(failure_after_phase, "archive")

            self._execute_sql_statements(
                connection,
                """
                CREATE TABLE paper_ledger_schema (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    schema_version INTEGER NOT NULL CHECK(schema_version = 5),
                    internal_migration_version INTEGER NOT NULL CHECK(
                        internal_migration_version = 4
                    ),
                    migrated_at TEXT NOT NULL,
                    unknown_fill_availability_count INTEGER NOT NULL CHECK(
                        unknown_fill_availability_count >= 0
                    ),
                    unknown_lot_availability_count INTEGER NOT NULL CHECK(
                        unknown_lot_availability_count >= 0
                    ),
                    unknown_consumption_availability_count INTEGER NOT NULL CHECK(
                        unknown_consumption_availability_count >= 0
                    ),
                    unknown_lot_provenance_count INTEGER NOT NULL CHECK(
                        unknown_lot_provenance_count >= 0
                    ),
                    unknown_intent_identity_count INTEGER NOT NULL CHECK(
                        unknown_intent_identity_count >= 0
                    ),
                    unknown_execution_identity_count INTEGER NOT NULL CHECK(
                        unknown_execution_identity_count >= 0
                    ),
                    unknown_lot_timeline_count INTEGER NOT NULL CHECK(
                        unknown_lot_timeline_count >= 0
                    ),
                    unknown_initial_execution_identity_count INTEGER NOT NULL CHECK(
                        unknown_initial_execution_identity_count >= 0
                    ),
                    unknown_execution_receipt_count INTEGER NOT NULL CHECK(
                        unknown_execution_receipt_count >= 0
                    ),
                    unknown_cost_provenance_count INTEGER NOT NULL CHECK(
                        unknown_cost_provenance_count >= 0
                    )
                );
                CREATE TABLE paper_ledger_attestation (
                    revision INTEGER PRIMARY KEY CHECK(revision >= 1),
                    ledger_generation TEXT NOT NULL,
                    migration_version INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    previous_attestation_fingerprint TEXT,
                    migration_attestation_fingerprint TEXT NOT NULL,
                    event_kind TEXT NOT NULL,
                    event_fingerprint TEXT NOT NULL,
                    broker_account_count INTEGER NOT NULL CHECK(broker_account_count >= 0),
                    intent_count INTEGER NOT NULL CHECK(intent_count >= 0),
                    order_count INTEGER NOT NULL CHECK(order_count >= 0),
                    fill_count INTEGER NOT NULL CHECK(fill_count >= 0),
                    lot_count INTEGER NOT NULL CHECK(lot_count >= 0),
                    consumption_count INTEGER NOT NULL CHECK(consumption_count >= 0),
                    receipt_count INTEGER NOT NULL CHECK(receipt_count >= 0),
                    authority_count INTEGER NOT NULL CHECK(authority_count >= 0),
                    payload_json TEXT NOT NULL,
                    attestation_fingerprint TEXT NOT NULL UNIQUE,
                    persisted_at TEXT NOT NULL,
                    FOREIGN KEY(previous_attestation_fingerprint)
                        REFERENCES paper_ledger_attestation(attestation_fingerprint)
                        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
                );
                CREATE TABLE paper_ledger_head_marker (
                    revision INTEGER PRIMARY KEY CHECK(revision >= 1),
                    ledger_generation TEXT NOT NULL,
                    migration_version INTEGER NOT NULL,
                    schema_version INTEGER NOT NULL,
                    schema_fingerprint TEXT NOT NULL,
                    attestation_fingerprint TEXT NOT NULL UNIQUE,
                    previous_head_marker_fingerprint TEXT,
                    migration_attestation_fingerprint TEXT NOT NULL,
                    broker_account_count INTEGER NOT NULL CHECK(broker_account_count >= 0),
                    intent_count INTEGER NOT NULL CHECK(intent_count >= 0),
                    order_count INTEGER NOT NULL CHECK(order_count >= 0),
                    fill_count INTEGER NOT NULL CHECK(fill_count >= 0),
                    lot_count INTEGER NOT NULL CHECK(lot_count >= 0),
                    consumption_count INTEGER NOT NULL CHECK(consumption_count >= 0),
                    receipt_count INTEGER NOT NULL CHECK(receipt_count >= 0),
                    authority_count INTEGER NOT NULL CHECK(authority_count >= 0),
                    payload_json TEXT NOT NULL,
                    head_marker_fingerprint TEXT NOT NULL UNIQUE,
                    persisted_at TEXT NOT NULL,
                    FOREIGN KEY(attestation_fingerprint)
                        REFERENCES paper_ledger_attestation(attestation_fingerprint)
                        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
                    FOREIGN KEY(previous_head_marker_fingerprint)
                        REFERENCES paper_ledger_head_marker(head_marker_fingerprint)
                        ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED
                );
                CREATE TABLE paper_ledger_tamper_marker (
                    tamper_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    target_revision INTEGER NOT NULL,
                    target_attestation_fingerprint TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    detected_at TEXT NOT NULL
                );
                CREATE TABLE paper_ledger_v4_archive_binding (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    binding_payload_json TEXT NOT NULL,
                    archive_binding_fingerprint TEXT NOT NULL
                );
                CREATE TABLE paper_ledger_migration_attestation (
                    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                    report_json TEXT NOT NULL,
                    migration_attestation_digest TEXT NOT NULL UNIQUE
                );
                """,
            )
            unknown_cost_count = sum(legacy_cost_evidence.values())
            connection.execute(
                """
                INSERT INTO paper_ledger_schema(
                    singleton, schema_version, internal_migration_version, migrated_at,
                    unknown_fill_availability_count,
                    unknown_lot_availability_count,
                    unknown_consumption_availability_count,
                    unknown_lot_provenance_count,
                    unknown_intent_identity_count,
                    unknown_execution_identity_count,
                    unknown_lot_timeline_count,
                    unknown_initial_execution_identity_count,
                    unknown_execution_receipt_count,
                    unknown_cost_provenance_count
                ) VALUES (
                    1, 5, 4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    old_unknown.get("unknown_fill_availability_count", 0),
                    old_unknown.get("unknown_lot_availability_count", 0),
                    old_unknown.get("unknown_consumption_availability_count", 0),
                    old_unknown.get("unknown_lot_provenance_count", 0),
                    old_unknown.get("unknown_intent_identity_count", 0),
                    old_unknown.get("unknown_execution_identity_count", 0),
                    old_unknown.get("unknown_lot_timeline_count", 0),
                    old_unknown.get("unknown_initial_execution_identity_count", 0),
                    old_unknown.get("unknown_execution_receipt_count", 0),
                    unknown_cost_count,
                ),
            )
            self._migration_checkpoint(failure_after_phase, "v5_schema")
            self._execute_sql_statements(
                connection,
                """
                CREATE INDEX IF NOT EXISTS idx_paper_fill_cost_provenance
                ON paper_fill(cost_provenance_state, cost_spec_id, cost_context_fingerprint);
                CREATE TRIGGER paper_execution_receipt_update_immutable
                BEFORE UPDATE ON paper_execution_receipt
                BEGIN SELECT RAISE(ABORT, 'paper execution receipt is immutable'); END;
                CREATE TRIGGER paper_execution_receipt_delete_immutable
                BEFORE DELETE ON paper_execution_receipt
                BEGIN SELECT RAISE(ABORT, 'paper execution receipt is immutable'); END;
                CREATE TRIGGER paper_execution_receipt_known_v3_required
                BEFORE INSERT ON paper_execution_receipt
                WHEN NEW.cost_provenance_state IS NOT 'KNOWN_V3'
                  OR NEW.transfer_fee IS NULL
                  OR NEW.total_fees IS NULL
                  OR NEW.cost_spec_id IS NULL
                  OR NEW.cost_spec_schema_version IS NOT 3
                  OR NEW.cost_context_fingerprint IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM paper_cost_spec AS spec
                      WHERE spec.cost_spec_id = NEW.cost_spec_id
                  )
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'new paper execution receipt requires KNOWN_V3 cost evidence'
                    );
                END;
                CREATE TRIGGER paper_cost_spec_update_immutable
                BEFORE UPDATE ON paper_cost_spec
                BEGIN SELECT RAISE(ABORT, 'paper cost spec authority is immutable'); END;
                CREATE TRIGGER paper_cost_spec_delete_immutable
                BEFORE DELETE ON paper_cost_spec
                BEGIN SELECT RAISE(ABORT, 'paper cost spec authority is immutable'); END;
                CREATE TRIGGER broker_account_known_v3_required
                BEFORE INSERT ON broker_account
                WHEN NEW.cost_provenance_state IS NOT 'KNOWN_V3'
                  OR NEW.cost_spec_id IS NULL
                  OR NEW.cost_spec_schema_version IS NOT 3
                  OR NEW.cost_policy_fingerprint IS NOT NEW.cost_spec_id
                  OR NOT EXISTS (
                      SELECT 1 FROM paper_cost_spec AS spec
                      WHERE spec.cost_spec_id = NEW.cost_spec_id
                  )
                BEGIN
                    SELECT RAISE(
                        ABORT,
                        'new paper broker account requires known v3 cost binding'
                    );
                END;
                CREATE TRIGGER broker_account_cost_binding_immutable
                BEFORE UPDATE OF initial_cash, cost_policy_fingerprint, cost_spec_id,
                                 cost_spec_schema_version, cost_provenance_state
                ON broker_account
                BEGIN SELECT RAISE(ABORT, 'paper broker account cost binding is immutable'); END;
                CREATE TRIGGER paper_fill_persisted_at_immutable
                BEFORE UPDATE OF persisted_at ON paper_fill
                WHEN NEW.persisted_at IS NOT OLD.persisted_at
                BEGIN SELECT RAISE(ABORT, 'paper_fill persisted_at is immutable'); END;
                CREATE TRIGGER paper_fill_row_immutable
                BEFORE UPDATE ON paper_fill
                BEGIN SELECT RAISE(ABORT, 'paper_fill row is immutable'); END;
                CREATE TRIGGER paper_fill_delete_immutable
                BEFORE DELETE ON paper_fill
                BEGIN SELECT RAISE(ABORT, 'paper_fill row is immutable'); END;
                CREATE TRIGGER paper_fill_known_v3_required
                BEFORE INSERT ON paper_fill
                WHEN NEW.cost_provenance_state IS NOT 'KNOWN_V3'
                  OR NEW.transfer_fee IS NULL
                  OR NEW.total_fees IS NULL
                  OR NEW.cost_spec_id IS NULL
                  OR NEW.cost_spec_schema_version IS NOT 3
                  OR NEW.cost_context_fingerprint IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM paper_cost_spec AS spec
                      WHERE spec.cost_spec_id = NEW.cost_spec_id
                  )
                BEGIN SELECT RAISE(ABORT, 'new paper fill requires KNOWN_V3 cost evidence'); END;
                CREATE TRIGGER paper_ledger_attestation_update_immutable
                BEFORE UPDATE ON paper_ledger_attestation
                BEGIN SELECT RAISE(ABORT, 'paper ledger attestation is immutable'); END;
                CREATE TRIGGER paper_ledger_attestation_delete_immutable
                BEFORE DELETE ON paper_ledger_attestation
                BEGIN SELECT RAISE(ABORT, 'paper ledger attestation is immutable'); END;
                CREATE TRIGGER paper_ledger_attestation_delete_tamper
                AFTER DELETE ON paper_ledger_attestation
                BEGIN
                    INSERT INTO paper_ledger_tamper_marker(
                        target_revision, target_attestation_fingerprint,
                        reason, detected_at
                    ) VALUES (
                        OLD.revision, OLD.attestation_fingerprint,
                        'attestation_deleted',
                        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                    );
                END;
                CREATE TRIGGER paper_ledger_head_marker_update_immutable
                BEFORE UPDATE ON paper_ledger_head_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger canonical head is immutable'); END;
                CREATE TRIGGER paper_ledger_head_marker_delete_immutable
                BEFORE DELETE ON paper_ledger_head_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger canonical head is immutable'); END;
                CREATE TRIGGER paper_ledger_tamper_marker_update_immutable
                BEFORE UPDATE ON paper_ledger_tamper_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger tamper marker is immutable'); END;
                CREATE TRIGGER paper_ledger_tamper_marker_delete_immutable
                BEFORE DELETE ON paper_ledger_tamper_marker
                BEGIN SELECT RAISE(ABORT, 'paper ledger tamper marker is immutable'); END;
                CREATE TRIGGER paper_ledger_schema_v4_archive_update_immutable
                BEFORE UPDATE ON paper_ledger_schema_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive schema is immutable'); END;
                CREATE TRIGGER paper_ledger_schema_v4_archive_delete_immutable
                BEFORE DELETE ON paper_ledger_schema_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive schema is immutable'); END;
                CREATE TRIGGER paper_ledger_attestation_v4_archive_update_immutable
                BEFORE UPDATE ON paper_ledger_attestation_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive attestation is immutable'); END;
                CREATE TRIGGER paper_ledger_attestation_v4_archive_delete_immutable
                BEFORE DELETE ON paper_ledger_attestation_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive attestation is immutable'); END;
                CREATE TRIGGER paper_ledger_head_marker_v4_archive_update_immutable
                BEFORE UPDATE ON paper_ledger_head_marker_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive head is immutable'); END;
                CREATE TRIGGER paper_ledger_head_marker_v4_archive_delete_immutable
                BEFORE DELETE ON paper_ledger_head_marker_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive head is immutable'); END;
                CREATE TRIGGER paper_ledger_tamper_marker_v4_archive_update_immutable
                BEFORE UPDATE ON paper_ledger_tamper_marker_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive tamper marker is immutable'); END;
                CREATE TRIGGER paper_ledger_tamper_marker_v4_archive_delete_immutable
                BEFORE DELETE ON paper_ledger_tamper_marker_v4_archive
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive tamper marker is immutable'); END;
                CREATE TRIGGER paper_ledger_v4_archive_binding_update_immutable
                BEFORE UPDATE ON paper_ledger_v4_archive_binding
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive binding is immutable'); END;
                CREATE TRIGGER paper_ledger_v4_archive_binding_delete_immutable
                BEFORE DELETE ON paper_ledger_v4_archive_binding
                BEGIN SELECT RAISE(ABORT, 'paper v4 archive binding is immutable'); END;
                CREATE TRIGGER paper_ledger_migration_attestation_update_immutable
                BEFORE UPDATE ON paper_ledger_migration_attestation
                BEGIN SELECT RAISE(ABORT, 'paper migration attestation is immutable'); END;
                CREATE TRIGGER paper_ledger_migration_attestation_delete_immutable
                BEFORE DELETE ON paper_ledger_migration_attestation
                BEGIN SELECT RAISE(ABORT, 'paper migration attestation is immutable'); END;
                """,
            )
            self._migration_checkpoint(failure_after_phase, "archive_protection")
            from rquant.paper_ledger_migration import write_migration_attestation

            immutable_migration = write_migration_attestation(
                connection,
                source_sha256=source_sha256,
                predecessor_v4_schema_fingerprint=previous_v4_schema_fingerprint,
                predecessor_v4_attestation_fingerprint=previous_v4_attestation,
                predecessor_v4_head_marker_fingerprint=previous_v4_head,
                v4_reconciliation_report_digest=v4_reconciliation_report_digest,
                migration_code_identity=migration_code_identity,
                source_schema_identity=source_schema_identity,
                target_schema_identity=target_schema_identity,
            )
            schema_fingerprint = self._schema_fingerprint(connection)
            if schema_fingerprint is None:
                raise PaperBrokerReconciliationError(
                    "paper ledger schema cannot be attested after v5 migration"
                )
            counts = {
                column: int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
                for column, table in _LEDGER_COUNT_TABLES.items()
            }
            migration_fingerprint = immutable_migration.digest
            self._insert_attestation(
                connection,
                {
                    "revision": 1,
                    "ledger_generation": secrets.token_hex(32),
                    "migration_version": _LEDGER_MIGRATION_VERSION,
                    "schema_version": 5,
                    "schema_fingerprint": schema_fingerprint,
                    "previous_attestation_fingerprint": None,
                    "migration_attestation_fingerprint": migration_fingerprint,
                    "event_kind": "migration_audit",
                    "event_fingerprint": migration_fingerprint,
                    **counts,
                    "persisted_at": _utc_iso(datetime.now(UTC)),
                },
            )
            self._migration_checkpoint(failure_after_phase, "attestation")
            self._verify_v5_migration_in_connection(connection)
            self._migration_checkpoint(failure_after_phase, "verification")
            connection.execute("PRAGMA user_version = 5")
            connection.commit()
        except BaseException:
            if connection.in_transaction:
                connection.rollback()
            raise

    def _before_commit(self, _connection: sqlite3.Connection) -> None:
        """Fault-injection boundary used to prove whole-submit rollback."""

    def sell_quantity_authority(
        self,
        *,
        exit_signal_id: str,
        entry_signal_id: str,
        ts_code: str,
        action: str,
        tranche_fraction: Decimal,
        decision_cutoff: datetime,
        trade_date: date,
    ) -> PaperSellQuantityAuthority:
        self.require_trusted_ledger()
        cutoff = _utc(decision_cutoff)
        if trade_date != cutoff.astimezone(_SHANGHAI).date():
            raise ValueError("trade_date must match decision_cutoff in Asia/Shanghai")
        with self._connect() as connection:
            remaining, available, source_fingerprint = self._entry_position_at(
                connection,
                entry_signal_id=entry_signal_id,
                ts_code=ts_code,
                decision_cutoff=cutoff,
                trade_date=trade_date,
            )
        if remaining <= 0:
            raise NoExecutableSellQuantityError("entry position is already closed")
        fraction = Decimal(str(tranche_fraction))
        if action == "S_INTENT":
            requested = remaining
        elif action == "REDUCE":
            requested = int(Decimal(remaining) * fraction) // 100 * 100
            if requested <= 0 or requested >= remaining:
                raise NoExecutableSellQuantityError("REDUCE has no legal partial 100-share lot")
        else:
            raise ValueError("sell quantity action must be REDUCE or S_INTENT")
        return PaperSellQuantityAuthority(
            exit_signal_id=exit_signal_id,
            entry_signal_id=entry_signal_id,
            account_id=self.account_id,
            ts_code=ts_code,
            action=action,
            decision_cutoff=cutoff,
            remaining_quantity=remaining,
            available_quantity=available,
            tranche_fraction=fraction,
            requested_quantity=requested,
            source_lot_fingerprint=source_fingerprint,
        )

    def _entry_position_at(
        self,
        connection: sqlite3.Connection,
        *,
        entry_signal_id: str,
        ts_code: str,
        decision_cutoff: datetime,
        trade_date: date,
    ) -> tuple[int, int, str]:
        entry_rows = connection.execute(
            """
            SELECT i.payload_json, i.persisted_at AS intent_persisted_at,
                   i.signal_id AS intent_signal_id,
                   i.entry_signal_id AS intent_entry_signal_id,
                   i.ts_code AS intent_ts_code,
                   i.side AS intent_side,
                   o.*
            FROM paper_intent AS i
            JOIN paper_order AS o ON o.intent_id = i.intent_id
            WHERE i.account_id = ? AND i.ts_code = ?
              AND i.signal_id = ? AND i.side = 'BUY'
            ORDER BY i.persisted_at, o.order_id
            """,
            (self.account_id, ts_code, entry_signal_id),
        ).fetchall()
        if len(entry_rows) != 1:
            raise PaperBrokerReconciliationError(
                "entry_signal_id must resolve to one authoritative BUY order"
            )
        entry_row = entry_rows[0]
        try:
            entry_intent = PaperOrderIntent.model_validate_json(entry_row["payload_json"])
        except (TypeError, ValueError) as exc:
            raise PaperBrokerReconciliationError("entry BUY intent is invalid") from exc
        intent_persisted = self._required_ledger_timestamp(
            entry_row["intent_persisted_at"],
            label="entry BUY intent persisted_at",
        )
        if intent_persisted > decision_cutoff:
            raise PaperBrokerReconciliationError(
                "entry BUY intent is not visible at decision cutoff"
            )
        if (
            entry_intent.side is not PaperSide.BUY
            or entry_intent.signal_id != entry_signal_id
            or entry_intent.account_id != self.account_id
            or entry_intent.ts_code != ts_code
            or entry_row["intent_signal_id"] != entry_intent.signal_id
            or entry_row["intent_entry_signal_id"] is not None
            or entry_row["intent_ts_code"] != entry_intent.ts_code
            or entry_row["intent_side"] != PaperSide.BUY.value
            or entry_row["side"] != PaperSide.BUY.value
            or entry_row["account_id"] != self.account_id
            or entry_row["ts_code"] != ts_code
            or entry_row["entry_signal_id"] is not None
        ):
            raise PaperBrokerReconciliationError(
                "entry_signal_id does not bind the requested account/code BUY"
            )

        fill_rows = connection.execute(
            """
            SELECT * FROM paper_fill
            WHERE order_id = ? AND executed_at <= ?
            ORDER BY sequence, fill_id
            """,
            (entry_row["order_id"], _utc_iso(decision_cutoff)),
        ).fetchall()
        visible_fills: list[tuple[PaperFill, datetime]] = []
        for row in fill_rows:
            persisted_at = self._required_ledger_timestamp(
                row["persisted_at"],
                label=f"BUY fill {row['fill_id']} persisted_at",
            )
            executed_at = self._required_ledger_timestamp(
                row["executed_at"],
                label=f"BUY fill {row['fill_id']} executed_at",
            )
            if persisted_at < executed_at:
                raise PaperBrokerReconciliationError(
                    "BUY fill persisted_at cannot precede execution"
                )
            if persisted_at > decision_cutoff:
                continue
            try:
                fill = self._fill_from_row(row)
            except (TypeError, ValueError) as exc:
                raise PaperBrokerReconciliationError("entry BUY fill is invalid") from exc
            visible_fills.append((fill, persisted_at))
        if not visible_fills:
            raise PaperBrokerReconciliationError(
                "entry_signal_id has no visible authoritative BUY fill"
            )
        if tuple(fill.sequence for fill, _ in visible_fills) != tuple(
            range(1, len(visible_fills) + 1)
        ):
            raise PaperBrokerReconciliationError("entry BUY fill sequence is incomplete")

        lot_evidence: list[dict[str, object]] = []
        remaining = 0
        available = 0
        for fill, fill_persisted in visible_fills:
            lot = connection.execute(
                "SELECT * FROM paper_lot WHERE lot_id = ?",
                (fill.fill_id,),
            ).fetchone()
            if lot is None or lot["entry_signal_id"] is None:
                raise PaperBrokerReconciliationError("entry BUY fill lacks exact lot provenance")
            lot_persisted = self._required_ledger_timestamp(
                lot["persisted_at"],
                label=f"lot {fill.fill_id} persisted_at",
            )
            lot_buy_executed = self._required_ledger_timestamp(
                lot["buy_executed_at"],
                label=f"lot {fill.fill_id} buy_executed_at",
            )
            lot_buy_persisted = self._required_ledger_timestamp(
                lot["buy_persisted_at"],
                label=f"lot {fill.fill_id} buy_persisted_at",
            )
            if (
                lot_persisted != fill_persisted
                or lot_buy_executed != fill.executed_at
                or lot_buy_persisted != fill_persisted
                or int(lot["buy_fill_sequence"]) != fill.sequence
                or lot_persisted > decision_cutoff
                or lot["entry_signal_id"] != entry_signal_id
                or lot["account_id"] != self.account_id
                or lot["ts_code"] != ts_code
                or int(lot["original_quantity"]) != fill.quantity
            ):
                raise PaperBrokerReconciliationError(
                    "entry BUY fill and lot provenance do not reconcile"
                )
            consumptions = connection.execute(
                """
                SELECT
                    c.fill_id AS sell_fill_id,
                    c.quantity,
                    c.persisted_at AS consumption_persisted_at,
                    sf.executed_at,
                    sf.persisted_at AS sell_fill_persisted_at,
                    so.account_id,
                    so.ts_code,
                    so.side,
                    so.entry_signal_id,
                    si.payload_json AS sell_intent_payload
                FROM paper_lot_consumption AS c
                JOIN paper_fill AS sf ON sf.fill_id = c.fill_id
                JOIN paper_order AS so ON so.order_id = sf.order_id
                JOIN paper_intent AS si ON si.intent_id = so.intent_id
                WHERE c.lot_id = ? AND sf.executed_at <= ?
                ORDER BY sf.executed_at, c.fill_id
                """,
                (fill.fill_id, _utc_iso(decision_cutoff)),
            ).fetchall()
            consumed = 0
            visible_consumptions: list[dict[str, object]] = []
            for item in consumptions:
                consumption_persisted = self._required_ledger_timestamp(
                    item["consumption_persisted_at"],
                    label=f"consumption {item['sell_fill_id']} persisted_at",
                )
                sell_fill_persisted = self._required_ledger_timestamp(
                    item["sell_fill_persisted_at"],
                    label=f"SELL fill {item['sell_fill_id']} persisted_at",
                )
                sell_executed = self._required_ledger_timestamp(
                    item["executed_at"],
                    label=f"SELL fill {item['sell_fill_id']} executed_at",
                )
                if (
                    consumption_persisted != sell_fill_persisted
                    or consumption_persisted < sell_executed
                ):
                    raise PaperBrokerReconciliationError(
                        "SELL fill and consumption availability do not reconcile"
                    )
                if consumption_persisted > decision_cutoff:
                    continue
                try:
                    sell_intent = PaperOrderIntent.model_validate_json(item["sell_intent_payload"])
                except (TypeError, ValueError) as exc:
                    raise PaperBrokerReconciliationError(
                        "SELL intent provenance is invalid"
                    ) from exc
                if (
                    item["account_id"] != self.account_id
                    or item["ts_code"] != ts_code
                    or item["side"] != PaperSide.SELL.value
                    or item["entry_signal_id"] != entry_signal_id
                    or sell_intent.entry_signal_id != entry_signal_id
                ):
                    raise PaperBrokerReconciliationError(
                        "SELL consumption does not bind the entry BUY"
                    )
                quantity = int(item["quantity"])
                consumed += quantity
                visible_consumptions.append(
                    {
                        "fill_id": str(item["sell_fill_id"]),
                        "quantity": quantity,
                        "persisted_at": _utc_iso(consumption_persisted),
                    }
                )
            lot_remaining = fill.quantity - consumed
            if lot_remaining < 0:
                raise PaperBrokerReconciliationError(
                    "SELL consumption exceeds entry BUY fill quantity"
                )
            remaining += lot_remaining
            available_date = date.fromisoformat(str(lot["available_date"]))
            if available_date <= trade_date:
                available += lot_remaining
            lot_evidence.append(
                {
                    "lot_id": str(fill.fill_id),
                    "original_quantity": fill.quantity,
                    "fill_persisted_at": _utc_iso(fill_persisted),
                    "available_date": available_date.isoformat(),
                    "consumptions": visible_consumptions,
                }
            )
        return (
            remaining,
            available,
            canonical_sha256(
                {
                    "account_id": self.account_id,
                    "entry_signal_id": entry_signal_id,
                    "ts_code": ts_code,
                    "decision_cutoff": decision_cutoff,
                    "lots": lot_evidence,
                }
            ),
        )

    @staticmethod
    def _required_ledger_timestamp(value: object, *, label: str) -> datetime:
        if value is None:
            raise PaperBrokerReconciliationError(f"{label} is unknown")
        try:
            return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError as exc:
            raise PaperBrokerReconciliationError(f"{label} is invalid") from exc

    def submit_intent(
        self,
        intent: PaperOrderIntent,
        *,
        execution_id: str | None = None,
        decision_time: datetime,
        persisted_at: datetime | None = None,
        trade_date: date,
        quote: BrokerExecutionContext,
    ) -> PaperOrder:
        decision_time = _utc(decision_time)
        persisted_at = decision_time if persisted_at is None else _utc(persisted_at)
        if intent.account_id != self.account_id:
            raise ValueError("intent account_id does not match broker account")
        if decision_time < intent.available_at:
            raise ValueError("decision_time cannot precede intent available_at")
        if persisted_at < decision_time or persisted_at < intent.available_at:
            raise ValueError("persisted_at cannot precede decision or intent availability")
        if trade_date != decision_time.astimezone(_SHANGHAI).date():
            raise ValueError("trade_date must match decision_time in Asia/Shanghai")
        resolved_execution_id = (
            _execution_id(execution_id)
            if execution_id is not None
            else (
                canonical_sha256(
                    {
                        "source": "paper_intent_initial_execution",
                        "intent_id": intent.intent_id,
                        "decision_time": decision_time,
                        "persisted_at": persisted_at,
                        "price_snapshot_id": intent.price_snapshot_id,
                    }
                )
            )
        )
        payload = _intent_payload(intent)
        calculation_quantity = (
            intent.quantity if quote.executable_quantity in {None, 0} else quote.executable_quantity
        )
        self._require_matching_instrument_context(intent.ts_code, quote.instrument_context)
        execution_costs = self._execution_costs(
            intent.side,
            quote.executable_price,
            calculation_quantity,
            quote.instrument_context,
        )
        request_payload, request_fingerprint = _execution_request_evidence(
            {
                "kind": "INITIAL",
                "execution_id": resolved_execution_id,
                "intent_id": intent.intent_id,
                "decision_time": _utc_iso(decision_time),
                "persisted_at": _utc_iso(persisted_at),
                "trade_date": trade_date.isoformat(),
                "quote": quote.model_dump(mode="json"),
                "cost_evidence": self._cost_evidence(execution_costs),
            }
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_trusted_ledger(connection)
                existing = connection.execute(
                    """
                    SELECT payload_json, initial_execution_id,
                           initial_execution_request_fingerprint
                    FROM paper_intent WHERE intent_id = ?
                    """,
                    (intent.intent_id,),
                ).fetchone()
                if existing is not None:
                    if existing["payload_json"] != payload:
                        raise DuplicateIntentConflictError(
                            f"intent_id {intent.intent_id} already has different content"
                        )
                    if existing["initial_execution_id"] != resolved_execution_id:
                        raise DuplicateExecutionConflictError(
                            "intent is already bound to a different initial execution_id"
                        )
                    if existing["initial_execution_request_fingerprint"] != request_fingerprint:
                        raise DuplicateExecutionConflictError(
                            "initial execution_id is already bound to different immutable content"
                        )
                    receipt = self._execution_receipt(
                        connection,
                        execution_id=resolved_execution_id,
                    )
                    if receipt is None:
                        raise PaperBrokerReconciliationError(
                            "persisted initial execution is missing its immutable receipt"
                        )
                    connection.rollback()
                    return receipt.order
                existing_receipt = self._execution_receipt(
                    connection,
                    execution_id=resolved_execution_id,
                )
                if existing_receipt is not None:
                    raise DuplicateExecutionConflictError(
                        "initial execution_id is already bound to a different intent"
                    )

                if intent.side is PaperSide.SELL:
                    self._validate_sell_quantity_authority(
                        connection,
                        intent=intent,
                        decision_time=decision_time,
                        trade_date=trade_date,
                    )

                connection.execute(
                    """
                    INSERT INTO paper_intent(
                        intent_id, account_id, signal_id, entry_signal_id,
                        ts_code, side, initial_execution_id,
                        initial_execution_request_fingerprint,
                        payload_json, persisted_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        intent.intent_id,
                        self.account_id,
                        intent.signal_id,
                        intent.entry_signal_id,
                        intent.ts_code,
                        intent.side.value,
                        resolved_execution_id,
                        request_fingerprint,
                        payload,
                        _utc_iso(persisted_at),
                    ),
                )
                order, fill = self._evaluate_intent(
                    connection,
                    intent=intent,
                    execution_id=resolved_execution_id,
                    decision_time=decision_time,
                    trade_date=trade_date,
                    quote=quote,
                    execution_costs=execution_costs,
                )
                if fill is not None and order.updated_at < persisted_at:
                    order = order.model_copy(update={"updated_at": persisted_at})
                self._insert_order(connection, order, intent=intent)
                lot_delta = 0
                consumption_delta = 0
                if fill is not None:
                    lot_delta, consumption_delta = self._apply_fill(
                        connection,
                        intent=intent,
                        fill=fill,
                        trade_date=trade_date,
                        available_date=quote.acquisition_available_date,
                        persisted_at=persisted_at,
                    )
                receipt = self._execution_receipt_with_cost(
                    execution_id=resolved_execution_id,
                    request_fingerprint=request_fingerprint,
                    intent_id=intent.intent_id,
                    order=order,
                    fill=fill,
                    calculation=execution_costs,
                    persisted_at=persisted_at,
                )
                self._insert_execution_receipt(
                    connection,
                    request_payload=request_payload,
                    receipt=receipt,
                )
                self._append_ledger_attestation(
                    connection,
                    event_kind="intent_execution",
                    event_fingerprint=canonical_sha256(
                        {
                            "intent": intent.model_dump(mode="python"),
                            "receipt": receipt.model_dump(mode="python"),
                        }
                    ),
                    count_deltas={
                        "intent_count": 1,
                        "order_count": 1,
                        "fill_count": int(fill is not None),
                        "lot_count": lot_delta,
                        "consumption_count": consumption_delta,
                        "receipt_count": 1,
                    },
                )
                self._before_commit(connection)
                connection.commit()
                return order
            except BaseException:
                connection.rollback()
                raise

    def _validate_sell_quantity_authority(
        self,
        connection: sqlite3.Connection,
        *,
        intent: PaperOrderIntent,
        decision_time: datetime,
        trade_date: date,
    ) -> None:
        authority = intent.sell_quantity_authority
        if authority is None or intent.entry_signal_id is None:
            raise PaperBrokerReconciliationError("SELL intent lacks quantity or entry provenance")
        if authority.decision_cutoff != decision_time:
            raise PaperBrokerReconciliationError(
                "SELL quantity authority cutoff must equal broker decision time"
            )
        remaining, available, source_fingerprint = self._entry_position_at(
            connection,
            entry_signal_id=intent.entry_signal_id,
            ts_code=intent.ts_code,
            decision_cutoff=decision_time,
            trade_date=trade_date,
        )
        expected = PaperSellQuantityAuthority(
            exit_signal_id=intent.signal_id,
            entry_signal_id=intent.entry_signal_id,
            account_id=self.account_id,
            ts_code=intent.ts_code,
            action=authority.action,
            decision_cutoff=decision_time,
            remaining_quantity=remaining,
            available_quantity=available,
            tranche_fraction=authority.tranche_fraction,
            requested_quantity=intent.quantity,
            source_lot_fingerprint=source_fingerprint,
        )
        if expected != authority:
            raise PaperBrokerReconciliationError(
                "SELL quantity authority changed before intent submission"
            )

    def apply_execution(
        self,
        order_id: str,
        *,
        execution_id: str,
        executed_at: datetime,
        trade_date: date,
        quantity: int,
        quote: BrokerExecutionContext,
        price_snapshot_id: str,
        persisted_at: datetime | None = None,
    ) -> PaperExecutionReceipt:
        execution_id = _execution_id(execution_id)
        execution_time = _utc(executed_at)
        persistence_time = execution_time if persisted_at is None else _utc(persisted_at)
        if persistence_time < execution_time:
            raise ValueError("persisted_at cannot precede execution")
        if trade_date != execution_time.astimezone(_SHANGHAI).date():
            raise ValueError("trade_date must match executed_at in Asia/Shanghai")
        if isinstance(quantity, bool) or quantity <= 0 or quantity % 100:
            raise ValueError("execution quantity must be a positive 100-share lot")
        if quote.executable_quantity not in {None, quantity}:
            raise ValueError("quote executable_quantity does not match execution quantity")
        if len(price_snapshot_id) != 64 or any(
            character not in "0123456789abcdef" for character in price_snapshot_id
        ):
            raise ValueError("price_snapshot_id must be a lowercase SHA-256 digest")
        with self._connect() as connection:
            intent_row = connection.execute(
                """
                SELECT i.payload_json
                FROM paper_order AS o
                JOIN paper_intent AS i ON i.intent_id = o.intent_id
                WHERE o.account_id = ? AND o.order_id = ?
                """,
                (self.account_id, order_id),
            ).fetchone()
        if intent_row is None:
            raise KeyError(f"unknown paper order: {order_id}")
        preflight_intent = PaperOrderIntent.model_validate_json(intent_row["payload_json"])
        self._require_matching_instrument_context(
            preflight_intent.ts_code,
            quote.instrument_context,
        )
        execution_costs = self._execution_costs(
            preflight_intent.side,
            quote.executable_price,
            quantity,
            quote.instrument_context,
        )
        request_payload, request_fingerprint = _execution_request_evidence(
            {
                "kind": "INCREMENTAL",
                "execution_id": execution_id,
                "order_id": order_id,
                "executed_at": _utc_iso(execution_time),
                "persisted_at": _utc_iso(persistence_time),
                "trade_date": trade_date.isoformat(),
                "quantity": quantity,
                "quote": quote.model_dump(mode="json"),
                "price_snapshot_id": price_snapshot_id,
                "cost_evidence": self._cost_evidence(execution_costs),
            }
        )
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_trusted_ledger(connection)
                existing_receipt = self._execution_receipt(
                    connection,
                    execution_id=execution_id,
                )
                if existing_receipt is not None:
                    if (
                        existing_receipt.order.order_id != order_id
                        or existing_receipt.request_fingerprint != request_fingerprint
                    ):
                        raise DuplicateExecutionConflictError(
                            "execution_id is already bound to different immutable content"
                        )
                    connection.rollback()
                    return existing_receipt
                existing_fill_row = connection.execute(
                    "SELECT * FROM paper_fill WHERE execution_id = ?",
                    (execution_id,),
                ).fetchone()
                if existing_fill_row is not None:
                    raise PaperBrokerReconciliationError(
                        "execution fill is missing its immutable receipt"
                    )
                row = connection.execute(
                    "SELECT * FROM paper_order WHERE account_id = ? AND order_id = ?",
                    (self.account_id, order_id),
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown paper order: {order_id}")
                order = self._order_from_row(row)
                intent_row = connection.execute(
                    "SELECT payload_json FROM paper_intent WHERE intent_id = ?",
                    (order.intent_id,),
                ).fetchone()
                if intent_row is None:
                    raise PaperBrokerReconciliationError("paper order is missing its intent")
                intent = PaperOrderIntent.model_validate_json(intent_row["payload_json"])
                if intent.side is not preflight_intent.side:
                    raise PaperBrokerReconciliationError(
                        "paper order changed while preparing v3 cost evidence"
                    )
                fill_price = execution_costs.executed_price
                if order.status not in {
                    PaperOrderStatus.ACCEPTED,
                    PaperOrderStatus.PARTIALLY_FILLED,
                }:
                    raise ValueError(f"order is not open for execution: {order.status.value}")
                sequence, previous_executed_at, previous_persisted_at = (
                    self._incremental_fill_timeline(connection, order=order)
                )
                if execution_time < order.created_at:
                    raise ValueError("executed_at cannot precede order creation")
                if previous_executed_at is not None and execution_time < previous_executed_at:
                    raise ValueError("executed_at cannot precede the previous fill execution")
                if previous_persisted_at is not None and persistence_time < previous_persisted_at:
                    raise ValueError("persisted_at cannot precede previous fill availability")
                if execution_time >= intent.expires_at:
                    raise ValueError("order expired before execution")
                remaining_order = order.quantity - order.filled_quantity
                if quantity > remaining_order:
                    raise ValueError("execution quantity exceeds unfilled order remainder")
                if quote.suspended or quote.limit_locked or quote.risk_rejected:
                    raise ValueError("execution quote is not executable")
                if intent.order_type is PaperOrderType.LIMIT:
                    assert intent.limit_price is not None
                    misses_limit = (
                        intent.side is PaperSide.BUY and fill_price > intent.limit_price
                    ) or (intent.side is PaperSide.SELL and fill_price < intent.limit_price)
                    if misses_limit:
                        raise ValueError("execution quote misses immutable limit price")
                if intent.side is PaperSide.BUY:
                    if quote.acquisition_available_date is None:
                        raise ValueError("BUY execution requires acquisition_available_date")
                    if quote.acquisition_available_date <= trade_date:
                        raise ValueError("BUY available date must be after acquisition trade date")
                    cash = self._account_values(connection)[0]
                    if cash < execution_costs.executed_notional + execution_costs.total_fees:
                        raise ValueError("insufficient cash for incremental execution")
                else:
                    if intent.entry_signal_id is None:
                        raise PaperBrokerReconciliationError(
                            "SELL order lacks entry_signal_id provenance"
                        )
                    total, available = self._position_quantities(
                        connection,
                        ts_code=intent.ts_code,
                        entry_signal_id=intent.entry_signal_id,
                        trade_date=trade_date,
                    )
                    if total < quantity or available < quantity:
                        raise ValueError("insufficient authoritative position for incremental SELL")
                fill = self._paper_fill(
                    order_id=order_id,
                    execution_id=execution_id,
                    sequence=sequence,
                    quantity=quantity,
                    calculation=execution_costs,
                    executed_at=execution_time,
                    price_snapshot_id=price_snapshot_id,
                )
                lot_delta, consumption_delta = self._apply_fill(
                    connection,
                    intent=intent,
                    fill=fill,
                    trade_date=trade_date,
                    available_date=quote.acquisition_available_date,
                    persisted_at=persistence_time,
                )
                new_filled = order.filled_quantity + quantity
                previous_notional = (
                    sum(
                        (
                            Decimal(item["price"]) * int(item["quantity"])
                            for item in connection.execute(
                                "SELECT price, quantity FROM paper_fill WHERE order_id = ?",
                                (order_id,),
                            ).fetchall()
                        ),
                        Decimal("0"),
                    )
                    - fill_price * quantity
                )
                average_price = ((previous_notional + fill_price * quantity) / new_filled).quantize(
                    self._execution_price_tick,
                    rounding=ROUND_HALF_UP,
                )
                status = (
                    PaperOrderStatus.FILLED
                    if new_filled == order.quantity
                    else PaperOrderStatus.PARTIALLY_FILLED
                )
                connection.execute(
                    """
                    UPDATE paper_order
                    SET filled_quantity = ?, average_fill_price = ?, status = ?,
                        reject_reason = NULL, updated_at = ?
                    WHERE order_id = ?
                    """,
                    (
                        new_filled,
                        _money(average_price),
                        status.value,
                        _utc_iso(max(order.updated_at, persistence_time)),
                        order_id,
                    ),
                )
                receipt_order = order.model_copy(
                    update={
                        "filled_quantity": new_filled,
                        "average_fill_price": average_price,
                        "status": status,
                        "reject_reason": None,
                        "updated_at": max(order.updated_at, persistence_time),
                    }
                )
                receipt = self._execution_receipt_with_cost(
                    execution_id=execution_id,
                    request_fingerprint=request_fingerprint,
                    intent_id=intent.intent_id,
                    order=receipt_order,
                    fill=fill,
                    calculation=execution_costs,
                    persisted_at=persistence_time,
                )
                self._insert_execution_receipt(
                    connection,
                    request_payload=request_payload,
                    receipt=receipt,
                )
                self._append_ledger_attestation(
                    connection,
                    event_kind="incremental_execution",
                    event_fingerprint=canonical_sha256(receipt.model_dump(mode="python")),
                    count_deltas={
                        "fill_count": 1,
                        "lot_count": lot_delta,
                        "consumption_count": consumption_delta,
                        "receipt_count": 1,
                    },
                )
                self._before_commit(connection)
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.execution(execution_id)
        assert result is not None
        return result

    def close_open_order(
        self,
        order_id: str,
        *,
        status: PaperOrderStatus,
        decided_at: datetime,
        persisted_at: datetime | None = None,
    ) -> PaperOrder:
        if status not in {PaperOrderStatus.CANCELLED, PaperOrderStatus.EXPIRED}:
            raise ValueError("close status must be CANCELLED or EXPIRED")
        decision_time = _utc(decided_at)
        persistence_time = decision_time if persisted_at is None else _utc(persisted_at)
        if persistence_time < decision_time:
            raise ValueError("persisted_at cannot precede close decision")
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_trusted_ledger(connection)
                row = connection.execute(
                    "SELECT * FROM paper_order WHERE account_id = ? AND order_id = ?",
                    (self.account_id, order_id),
                ).fetchone()
                if row is None:
                    raise KeyError(f"unknown paper order: {order_id}")
                order = self._order_from_row(row)
                if order.status not in {
                    PaperOrderStatus.ACCEPTED,
                    PaperOrderStatus.PARTIALLY_FILLED,
                }:
                    raise ValueError(f"order is not open for close: {order.status.value}")
                _, previous_executed_at, previous_persisted_at = self._incremental_fill_timeline(
                    connection, order=order
                )
                if previous_executed_at is not None and decision_time < previous_executed_at:
                    raise ValueError("close decision cannot precede the latest fill execution")
                if previous_persisted_at is not None and persistence_time < previous_persisted_at:
                    raise ValueError("close persistence cannot precede latest fill availability")
                intent_row = connection.execute(
                    "SELECT payload_json FROM paper_intent WHERE intent_id = ?",
                    (order.intent_id,),
                ).fetchone()
                if intent_row is None:
                    raise PaperBrokerReconciliationError("paper order is missing its intent")
                intent = PaperOrderIntent.model_validate_json(intent_row["payload_json"])
                if status is PaperOrderStatus.EXPIRED and decision_time < intent.expires_at:
                    raise ValueError("order cannot expire before intent expires_at")
                connection.execute(
                    "UPDATE paper_order SET status = ?, updated_at = ? WHERE order_id = ?",
                    (status.value, _utc_iso(max(order.updated_at, persistence_time)), order_id),
                )
                self._append_ledger_attestation(
                    connection,
                    event_kind="order_close",
                    event_fingerprint=canonical_sha256(
                        {
                            "order_id": order_id,
                            "status": status.value,
                            "decided_at": decision_time,
                            "persisted_at": persistence_time,
                        }
                    ),
                    count_deltas={},
                )
                connection.commit()
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise
        result = self.order(order_id)
        assert result is not None
        return result

    def _incremental_fill_timeline(
        self,
        connection: sqlite3.Connection,
        *,
        order: PaperOrder,
    ) -> tuple[int, datetime | None, datetime | None]:
        rows = connection.execute(
            """
            SELECT sequence, quantity, executed_at, persisted_at
            FROM paper_fill
            WHERE order_id = ?
            ORDER BY sequence, fill_id
            """,
            (order.order_id,),
        ).fetchall()
        sequences = tuple(int(row["sequence"]) for row in rows)
        if sequences != tuple(range(1, len(rows) + 1)):
            raise PaperBrokerReconciliationError(
                f"order {order.order_id} has a non-contiguous fill sequence"
            )
        if sum(int(row["quantity"]) for row in rows) != order.filled_quantity:
            raise PaperBrokerReconciliationError(
                f"order {order.order_id} fill quantity does not match the ledger"
            )
        previous_executed_at: datetime | None = None
        previous_persisted_at: datetime | None = None
        for row in rows:
            executed_at = self._required_ledger_timestamp(
                row["executed_at"],
                label=f"order {order.order_id} fill executed_at",
            )
            persisted_at = self._required_ledger_timestamp(
                row["persisted_at"],
                label=f"order {order.order_id} fill persisted_at",
            )
            if persisted_at < executed_at:
                raise PaperBrokerReconciliationError(
                    f"order {order.order_id} fill availability precedes execution"
                )
            if previous_executed_at is not None and executed_at < previous_executed_at:
                raise PaperBrokerReconciliationError(
                    f"order {order.order_id} fill execution sequence is nonmonotonic"
                )
            if previous_persisted_at is not None and persisted_at < previous_persisted_at:
                raise PaperBrokerReconciliationError(
                    f"order {order.order_id} fill availability sequence is nonmonotonic"
                )
            if order.updated_at < executed_at or order.updated_at < persisted_at:
                raise PaperBrokerReconciliationError(
                    f"order {order.order_id} updated_at precedes a fill timestamp"
                )
            previous_executed_at = executed_at
            previous_persisted_at = persisted_at
        return len(rows) + 1, previous_executed_at, previous_persisted_at

    def _evaluate_intent(
        self,
        connection: sqlite3.Connection,
        *,
        intent: PaperOrderIntent,
        execution_id: str,
        decision_time: datetime,
        trade_date: date,
        quote: BrokerExecutionContext,
        execution_costs: ExecutionCostCalculation,
    ) -> tuple[PaperOrder, PaperFill | None]:
        reject_reason: PaperRejectReason | None = None
        if quote.suspended:
            reject_reason = PaperRejectReason.SUSPENDED
        elif quote.limit_locked:
            reject_reason = PaperRejectReason.LIMIT_LOCKED
        elif quote.risk_rejected:
            reject_reason = PaperRejectReason.RISK_REJECTED
        elif decision_time >= intent.expires_at:
            reject_reason = PaperRejectReason.EXPIRED

        if reject_reason is not None:
            return self._new_order(intent, decision_time, reject_reason=reject_reason), None
        if decision_time < intent.earliest_execution_at:
            return self._new_order(intent, decision_time), None

        fill_price = execution_costs.executed_price
        # Frozen conservative rule: a missed limit remains ACCEPTED with no cash
        # reservation. A retry of the same immutable intent returns this evidence.
        if intent.order_type is PaperOrderType.LIMIT:
            assert intent.limit_price is not None
            misses_limit = (intent.side is PaperSide.BUY and fill_price > intent.limit_price) or (
                intent.side is PaperSide.SELL and fill_price < intent.limit_price
            )
            if misses_limit:
                return self._new_order(intent, decision_time), None

        execution_quantity = (
            intent.quantity if quote.executable_quantity is None else quote.executable_quantity
        )
        if execution_quantity > intent.quantity:
            raise ValueError("executable_quantity cannot exceed intent quantity")
        if execution_quantity == 0:
            return self._new_order(intent, decision_time), None
        if execution_costs.order_input.quantity != execution_quantity:
            raise PaperBrokerReconciliationError(
                "initial execution cost evidence quantity does not match the resolved fill"
            )
        fill_price = execution_costs.executed_price
        if intent.side is PaperSide.BUY:
            if quote.acquisition_available_date is None:
                raise ValueError("BUY execution requires acquisition_available_date")
            if quote.acquisition_available_date <= trade_date:
                raise ValueError("BUY available date must be after acquisition trade date")
            cash = self._account_values(connection)[0]
            if cash < execution_costs.executed_notional + execution_costs.total_fees:
                return self._new_order(
                    intent,
                    decision_time,
                    reject_reason=PaperRejectReason.INSUFFICIENT_CASH,
                ), None
        else:
            if intent.entry_signal_id is None:
                raise PaperBrokerReconciliationError(
                    "SELL intent is missing entry_signal_id provenance"
                )
            total, available = self._position_quantities(
                connection,
                ts_code=intent.ts_code,
                entry_signal_id=intent.entry_signal_id,
                trade_date=trade_date,
            )
            if total < intent.quantity:
                return self._new_order(
                    intent,
                    decision_time,
                    reject_reason=PaperRejectReason.INSUFFICIENT_POSITION,
                ), None
            if available < execution_quantity:
                return self._new_order(
                    intent,
                    decision_time,
                    reject_reason=PaperRejectReason.T_PLUS_ONE,
                ), None

        status = (
            PaperOrderStatus.FILLED
            if execution_quantity == intent.quantity
            else PaperOrderStatus.PARTIALLY_FILLED
        )
        filled = self._new_order(
            intent,
            decision_time,
            status=status,
            filled_quantity=execution_quantity,
            fill_price=fill_price,
        )
        fill = self._paper_fill(
            order_id=filled.order_id,
            execution_id=execution_id,
            sequence=1,
            quantity=execution_quantity,
            calculation=execution_costs,
            executed_at=decision_time,
            price_snapshot_id=intent.price_snapshot_id,
        )
        return filled, fill

    def _new_order(
        self,
        intent: PaperOrderIntent,
        decision_time: datetime,
        *,
        status: PaperOrderStatus = PaperOrderStatus.ACCEPTED,
        reject_reason: PaperRejectReason | None = None,
        filled_quantity: int = 0,
        fill_price: Decimal | None = None,
    ) -> PaperOrder:
        if reject_reason is not None:
            status = PaperOrderStatus.REJECTED
        return PaperOrder(
            intent_id=intent.intent_id,
            account_id=self.account_id,
            ts_code=intent.ts_code,
            side=intent.side,
            order_type=intent.order_type,
            quantity=intent.quantity,
            filled_quantity=filled_quantity,
            average_fill_price=fill_price,
            status=status,
            reject_reason=reject_reason,
            created_at=decision_time,
            updated_at=decision_time,
        )

    def _execution_costs(
        self,
        side: PaperSide,
        executable_price: Decimal,
        quantity: int,
        instrument_context: InstrumentContext,
    ) -> ExecutionCostCalculation:
        return calculate_execution_costs(
            self.cost_policy.execution_cost_spec,
            ExecutionCostOrderInput(
                side="BUY" if side is PaperSide.BUY else "SELL",
                reference_price=executable_price,
                quantity=quantity,
            ),
            instrument_context,
        )

    @staticmethod
    def _require_matching_instrument_context(
        ts_code: str,
        instrument_context: InstrumentContext,
    ) -> None:
        if instrument_context.ts_code != ts_code.strip().upper():
            raise ValueError("execution instrument_context ts_code does not match intent ts_code")
        provenance = instrument_context.classification_provenance
        if provenance is None or provenance.reference_dataset != "security_listing_status":
            raise ValueError("execution instrument_context requires trusted listing classification")
        if (
            instrument_context.market != "CN"
            or instrument_context.instrument_class != "EQUITY"
            or instrument_context.security_class != "A_SHARE"
        ):
            raise ValueError("execution instrument_context must be an attested CN A_SHARE")

    @staticmethod
    def _cost_evidence(calculation: ExecutionCostCalculation) -> dict[str, object]:
        return {
            "cost_spec_id": calculation.cost_spec_id,
            "cost_spec_schema_version": calculation.cost_spec_schema_version,
            "cost_engine_version": calculation.cost_engine_version,
            "cost_context_fingerprint": calculation.cost_context_fingerprint,
            "resolved_calculation_fingerprint": calculation.resolved_calculation_fingerprint,
            "calculation": calculation.model_dump(mode="json"),
        }

    @staticmethod
    def _paper_fill(
        *,
        order_id: str,
        execution_id: str,
        sequence: int,
        quantity: int,
        calculation: ExecutionCostCalculation,
        executed_at: datetime,
        price_snapshot_id: str,
    ) -> PaperFill:
        return PaperFill(
            order_id=order_id,
            execution_id=execution_id,
            sequence=sequence,
            quantity=quantity,
            price=calculation.executed_price,
            commission=calculation.commission,
            transfer_fee=calculation.transfer_fee,
            tax=calculation.stamp_duty,
            total_fees=calculation.total_fees,
            cost_spec_id=calculation.cost_spec_id,
            cost_spec_schema_version=calculation.cost_spec_schema_version,
            cost_context_fingerprint=calculation.cost_context_fingerprint,
            cost_provenance_state=PaperCostProvenanceState.KNOWN_V3,
            executed_at=executed_at,
            price_snapshot_id=price_snapshot_id,
        )

    @staticmethod
    def _execution_receipt_with_cost(
        *,
        execution_id: str,
        request_fingerprint: str,
        intent_id: str,
        order: PaperOrder,
        fill: PaperFill | None,
        calculation: ExecutionCostCalculation,
        persisted_at: datetime,
    ) -> PaperExecutionReceipt:
        return PaperExecutionReceipt(
            execution_id=execution_id,
            request_fingerprint=request_fingerprint,
            intent_id=intent_id,
            order=order,
            fill=fill,
            cost_spec_id=calculation.cost_spec_id,
            cost_spec_schema_version=calculation.cost_spec_schema_version,
            cost_context_fingerprint=calculation.cost_context_fingerprint,
            cost_provenance_state=PaperCostProvenanceState.KNOWN_V3,
            cost_calculation=calculation,
            persisted_at=persisted_at,
        )

    @property
    def _execution_price_tick(self) -> Decimal:
        slippage = self.cost_policy.execution_cost_spec.slippage
        assert slippage is not None
        return slippage.price_tick

    def _insert_order(
        self,
        connection: sqlite3.Connection,
        order: PaperOrder,
        *,
        intent: PaperOrderIntent,
    ) -> None:
        connection.execute(
            """
            INSERT INTO paper_order(
                order_id, intent_id, account_id, ts_code, side, entry_signal_id, order_type,
                quantity, filled_quantity, average_fill_price, status,
                reject_reason, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                order.order_id,
                order.intent_id,
                order.account_id,
                order.ts_code,
                order.side.value,
                intent.entry_signal_id,
                order.order_type.value,
                order.quantity,
                order.filled_quantity,
                _money(order.average_fill_price) if order.average_fill_price is not None else None,
                order.status.value,
                order.reject_reason.value if order.reject_reason is not None else None,
                _utc_iso(order.created_at),
                _utc_iso(order.updated_at),
            ),
        )

    def _insert_execution_receipt(
        self,
        connection: sqlite3.Connection,
        *,
        request_payload: str,
        receipt: PaperExecutionReceipt,
    ) -> None:
        try:
            request_value = json.loads(request_payload)
        except (TypeError, ValueError) as exc:
            raise PaperBrokerReconciliationError("execution request evidence is invalid") from exc
        if canonical_sha256(request_value) != receipt.request_fingerprint:
            raise PaperBrokerReconciliationError(
                "execution request evidence does not match its fingerprint"
            )
        receipt_payload = json.dumps(
            receipt.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        calculation = receipt.cost_calculation
        if (
            receipt.cost_provenance_state is not PaperCostProvenanceState.KNOWN_V3
            or calculation is None
            or receipt.cost_spec_id is None
            or receipt.cost_spec_schema_version is None
            or receipt.cost_context_fingerprint is None
        ):
            raise PaperBrokerReconciliationError(
                "new paper execution receipt requires complete KNOWN_V3 cost evidence"
            )
        connection.execute(
            """
            INSERT INTO paper_execution_receipt(
                execution_id, account_id, intent_id, order_id,
                request_fingerprint, request_json, receipt_json,
                transfer_fee, total_fees, cost_spec_id, cost_spec_schema_version,
                cost_context_fingerprint, cost_provenance_state, persisted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.execution_id,
                self.account_id,
                receipt.intent_id,
                receipt.order.order_id,
                receipt.request_fingerprint,
                request_payload,
                receipt_payload,
                _money(calculation.transfer_fee),
                _money(calculation.total_fees),
                receipt.cost_spec_id,
                receipt.cost_spec_schema_version,
                receipt.cost_context_fingerprint,
                receipt.cost_provenance_state.value,
                _utc_iso(receipt.persisted_at),
            ),
        )

    def _execution_receipt(
        self,
        connection: sqlite3.Connection,
        *,
        execution_id: str,
    ) -> PaperExecutionReceipt | None:
        row = connection.execute(
            """
            SELECT * FROM paper_execution_receipt
            WHERE account_id = ? AND execution_id = ?
            """,
            (self.account_id, execution_id),
        ).fetchone()
        if row is None:
            return None
        try:
            request_value = json.loads(row["request_json"])
            receipt = PaperExecutionReceipt.model_validate_json(row["receipt_json"])
        except (TypeError, ValueError) as exc:
            raise PaperBrokerReconciliationError(
                "persisted execution receipt evidence is invalid"
            ) from exc
        persisted_at = self._required_ledger_timestamp(
            row["persisted_at"],
            label=f"execution {execution_id} receipt persisted_at",
        )
        if (
            canonical_sha256(request_value) != row["request_fingerprint"]
            or receipt.execution_id != execution_id
            or receipt.request_fingerprint != row["request_fingerprint"]
            or receipt.intent_id != row["intent_id"]
            or receipt.order.order_id != row["order_id"]
            or receipt.order.account_id != self.account_id
            or receipt.persisted_at != persisted_at
        ):
            raise PaperBrokerReconciliationError(
                "persisted execution receipt columns do not match immutable evidence"
            )
        if not isinstance(request_value, dict):
            raise PaperBrokerReconciliationError("execution request evidence must be an object")
        if receipt.cost_provenance_state is PaperCostProvenanceState.KNOWN_V3:
            calculation = receipt.cost_calculation
            if (
                calculation is None
                or receipt.cost_spec_id != row["cost_spec_id"]
                or receipt.cost_spec_schema_version != row["cost_spec_schema_version"]
                or receipt.cost_context_fingerprint != row["cost_context_fingerprint"]
                or receipt.cost_provenance_state.value != row["cost_provenance_state"]
                or _money(calculation.transfer_fee) != row["transfer_fee"]
                or _money(calculation.total_fees) != row["total_fees"]
                or request_value.get("cost_evidence") != self._cost_evidence(calculation)
            ):
                raise PaperBrokerReconciliationError(
                    "persisted execution receipt v3 cost evidence does not reconcile"
                )
        elif (
            any(
                row[field] is not None
                for field in (
                    "transfer_fee",
                    "total_fees",
                    "cost_spec_id",
                    "cost_spec_schema_version",
                    "cost_context_fingerprint",
                )
            )
            or row["cost_provenance_state"] != PaperCostProvenanceState.LEGACY_UNKNOWN.value
        ):
            raise PaperBrokerReconciliationError(
                "legacy execution receipt must retain unknown cost provenance"
            )
        kind = request_value.get("kind")
        if request_value.get("execution_id") != receipt.execution_id or request_value.get(
            "persisted_at"
        ) != _utc_iso(receipt.persisted_at):
            raise PaperBrokerReconciliationError(
                "execution request identity does not match immutable receipt"
            )
        if kind == "INITIAL":
            if (
                request_value.get("intent_id") != receipt.intent_id
                or request_value.get("decision_time") != _utc_iso(receipt.order.created_at)
                or (
                    receipt.fill is not None
                    and receipt.fill.executed_at != receipt.order.created_at
                )
            ):
                raise PaperBrokerReconciliationError(
                    "initial execution request does not match immutable receipt"
                )
        elif kind == "INCREMENTAL":
            if receipt.fill is None or (
                request_value.get("order_id") != receipt.order.order_id
                or request_value.get("executed_at") != _utc_iso(receipt.fill.executed_at)
                or request_value.get("quantity") != receipt.fill.quantity
                or request_value.get("price_snapshot_id") != receipt.fill.price_snapshot_id
            ):
                raise PaperBrokerReconciliationError(
                    "incremental execution request does not match immutable receipt"
                )
        else:
            raise PaperBrokerReconciliationError("execution request kind is unsupported")
        return receipt

    def _apply_fill(
        self,
        connection: sqlite3.Connection,
        *,
        intent: PaperOrderIntent,
        fill: PaperFill,
        trade_date: date,
        available_date: date | None,
        persisted_at: datetime,
    ) -> tuple[int, int]:
        connection.execute(
            """
            INSERT INTO paper_fill(
                fill_id, execution_id, order_id, sequence, quantity, price, commission,
                transfer_fee, tax, total_fees, cost_spec_id, cost_spec_schema_version,
                cost_context_fingerprint, cost_provenance_state,
                executed_at, persisted_at, price_snapshot_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fill.fill_id,
                fill.execution_id,
                fill.order_id,
                fill.sequence,
                fill.quantity,
                _money(fill.price),
                _money(fill.commission),
                _money(fill.transfer_fee) if fill.transfer_fee is not None else None,
                _money(fill.tax),
                _money(fill.total_fees) if fill.total_fees is not None else None,
                fill.cost_spec_id,
                fill.cost_spec_schema_version,
                fill.cost_context_fingerprint,
                fill.cost_provenance_state.value,
                _utc_iso(fill.executed_at),
                _utc_iso(persisted_at),
                fill.price_snapshot_id,
            ),
        )
        cash, realized = self._account_values(connection)
        if intent.side is PaperSide.BUY:
            assert available_date is not None
            assert fill.total_fees is not None
            total_cost = fill.notional + fill.total_fees
            connection.execute(
                "UPDATE broker_account SET cash = ? WHERE account_id = ?",
                (_money(cash - total_cost), self.account_id),
            )
            connection.execute(
                """
                INSERT INTO paper_lot(
                    lot_id, account_id, ts_code, entry_signal_id, acquisition_trade_date,
                    available_date, original_quantity, remaining_quantity, unit_cost,
                    persisted_at, buy_executed_at, buy_persisted_at, buy_fill_sequence
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    fill.fill_id,
                    self.account_id,
                    intent.ts_code,
                    intent.signal_id,
                    trade_date.isoformat(),
                    available_date.isoformat(),
                    fill.quantity,
                    fill.quantity,
                    _money(total_cost / fill.quantity),
                    _utc_iso(persisted_at),
                    _utc_iso(fill.executed_at),
                    _utc_iso(persisted_at),
                    fill.sequence,
                ),
            )
            return 1, 0

        remaining = fill.quantity
        cost_basis = Decimal("0")
        consumption_count = 0
        lots = connection.execute(
            """
            SELECT lot_id, remaining_quantity, unit_cost
            FROM paper_lot
            WHERE account_id = ? AND ts_code = ? AND entry_signal_id = ?
              AND remaining_quantity > 0
              AND available_date <= ?
              AND EXISTS (
                  SELECT 1
                  FROM paper_fill AS bf
                  JOIN paper_order AS bo ON bo.order_id = bf.order_id
                  JOIN paper_intent AS bi ON bi.intent_id = bo.intent_id
                  WHERE bf.fill_id = paper_lot.lot_id
                    AND bo.side = 'BUY'
                    AND bi.signal_id = paper_lot.entry_signal_id
                    AND bi.account_id = paper_lot.account_id
                    AND bi.ts_code = paper_lot.ts_code
                    AND bi.side = 'BUY'
              )
            ORDER BY available_date, acquisition_trade_date,
                     buy_executed_at, buy_persisted_at, buy_fill_sequence, lot_id
            """,
            (
                self.account_id,
                intent.ts_code,
                intent.entry_signal_id,
                trade_date.isoformat(),
            ),
        ).fetchall()
        for lot in lots:
            if remaining == 0:
                break
            consumed = min(remaining, int(lot["remaining_quantity"]))
            unit_cost = Decimal(lot["unit_cost"])
            connection.execute(
                "UPDATE paper_lot SET remaining_quantity = remaining_quantity - ? WHERE lot_id = ?",
                (consumed, lot["lot_id"]),
            )
            connection.execute(
                """
                INSERT INTO paper_lot_consumption(
                    fill_id, lot_id, quantity, unit_cost, persisted_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    fill.fill_id,
                    lot["lot_id"],
                    consumed,
                    _money(unit_cost),
                    _utc_iso(persisted_at),
                ),
            )
            cost_basis += unit_cost * consumed
            remaining -= consumed
            consumption_count += 1
        if remaining:
            raise PaperBrokerReconciliationError("available lot allocation became incomplete")
        assert fill.total_fees is not None
        net_proceeds = fill.notional - fill.total_fees
        connection.execute(
            """
            UPDATE broker_account SET cash = ?, realized_pnl = ? WHERE account_id = ?
            """,
            (
                _money(cash + net_proceeds),
                _money(realized + net_proceeds - cost_basis),
                self.account_id,
            ),
        )
        return 0, consumption_count

    def _account_values(self, connection: sqlite3.Connection) -> tuple[Decimal, Decimal]:
        row = connection.execute(
            "SELECT cash, realized_pnl FROM broker_account WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if row is None:
            raise PaperBrokerReconciliationError("broker account is missing")
        return Decimal(row["cash"]), Decimal(row["realized_pnl"])

    def _position_quantities(
        self,
        connection: sqlite3.Connection,
        *,
        ts_code: str,
        entry_signal_id: str,
        trade_date: date,
    ) -> tuple[int, int]:
        row = connection.execute(
            """
            SELECT
                COALESCE(SUM(remaining_quantity), 0) AS total,
                COALESCE(SUM(
                    CASE WHEN available_date <= ? THEN remaining_quantity ELSE 0 END
                ), 0) AS available
            FROM paper_lot
            WHERE account_id = ? AND ts_code = ? AND entry_signal_id = ?
              AND EXISTS (
                  SELECT 1
                  FROM paper_fill AS bf
                  JOIN paper_order AS bo ON bo.order_id = bf.order_id
                  JOIN paper_intent AS bi ON bi.intent_id = bo.intent_id
                  WHERE bf.fill_id = paper_lot.lot_id
                    AND bo.side = 'BUY'
                    AND bi.signal_id = paper_lot.entry_signal_id
                    AND bi.account_id = paper_lot.account_id
                    AND bi.ts_code = paper_lot.ts_code
                    AND bi.side = 'BUY'
              )
            """,
            (trade_date.isoformat(), self.account_id, ts_code, entry_signal_id),
        ).fetchone()
        return int(row["total"]), int(row["available"])

    def order(self, order_id: str) -> PaperOrder | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM paper_order WHERE account_id = ? AND order_id = ?",
                (self.account_id, order_id),
            ).fetchone()
            return self._order_from_row(row) if row is not None else None

    def order_for_intent(self, intent_id: str) -> PaperOrder | None:
        with self._connect() as connection:
            return self._order_for_intent(connection, intent_id)

    def execution(self, execution_id: str) -> PaperExecutionReceipt | None:
        identity = _execution_id(execution_id)
        with self._connect() as connection:
            return self._execution_receipt(connection, execution_id=identity)

    def order_for_execution(self, execution_id: str) -> PaperOrder | None:
        receipt = self.execution(execution_id)
        return None if receipt is None else receipt.order

    def _order_for_intent(
        self, connection: sqlite3.Connection, intent_id: str
    ) -> PaperOrder | None:
        row = connection.execute(
            "SELECT * FROM paper_order WHERE account_id = ? AND intent_id = ?",
            (self.account_id, intent_id),
        ).fetchone()
        return self._order_from_row(row) if row is not None else None

    def _order_for_id(self, connection: sqlite3.Connection, order_id: str) -> PaperOrder | None:
        row = connection.execute(
            "SELECT * FROM paper_order WHERE account_id = ? AND order_id = ?",
            (self.account_id, order_id),
        ).fetchone()
        return self._order_from_row(row) if row is not None else None

    @staticmethod
    def _order_from_row(row: sqlite3.Row) -> PaperOrder:
        return PaperOrder(
            order_id=row["order_id"],
            intent_id=row["intent_id"],
            account_id=row["account_id"],
            ts_code=row["ts_code"],
            side=row["side"],
            order_type=row["order_type"],
            quantity=row["quantity"],
            filled_quantity=row["filled_quantity"],
            average_fill_price=row["average_fill_price"],
            status=row["status"],
            reject_reason=row["reject_reason"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def fills(self, order_id: str | None = None) -> tuple[PaperFill, ...]:
        if order_id is None:
            self.require_trusted_ledger()
        with self._connect() as connection:
            if order_id is None:
                rows = connection.execute(
                    """
                    SELECT f.* FROM paper_fill AS f
                    JOIN paper_order AS o ON o.order_id = f.order_id
                    WHERE o.account_id = ? ORDER BY f.executed_at, f.fill_id
                    """,
                    (self.account_id,),
                ).fetchall()
            else:
                rows = connection.execute(
                    """
                    SELECT f.* FROM paper_fill AS f
                    JOIN paper_order AS o ON o.order_id = f.order_id
                    WHERE o.account_id = ? AND f.order_id = ?
                    ORDER BY f.sequence
                    """,
                    (self.account_id, order_id),
                ).fetchall()
        return tuple(self._fill_from_row(row) for row in rows)

    @staticmethod
    def _fill_from_row(row: sqlite3.Row) -> PaperFill:
        return PaperFill(
            fill_id=row["fill_id"],
            execution_id=row["execution_id"],
            order_id=row["order_id"],
            sequence=row["sequence"],
            quantity=row["quantity"],
            price=row["price"],
            commission=row["commission"],
            transfer_fee=row["transfer_fee"],
            tax=row["tax"],
            total_fees=row["total_fees"],
            cost_spec_id=row["cost_spec_id"],
            cost_spec_schema_version=row["cost_spec_schema_version"],
            cost_context_fingerprint=row["cost_context_fingerprint"],
            cost_provenance_state=row["cost_provenance_state"],
            executed_at=row["executed_at"],
            price_snapshot_id=row["price_snapshot_id"],
        )

    def account_snapshot(
        self,
        *,
        as_of: AwareUtcDatetime,
        market_prices: Mapping[str, Decimal],
    ) -> PaperAccountSnapshot:
        self.require_trusted_ledger()
        as_of = _utc(as_of)
        as_of_trade_date = as_of.astimezone(_SHANGHAI).date().isoformat()
        with self._connect() as connection:
            latest = connection.execute(
                """
                SELECT MAX(updated_at) AS latest_at FROM paper_order
                WHERE account_id = ?
                """,
                (self.account_id,),
            ).fetchone()["latest_at"]
            if latest is not None and as_of < _utc(datetime.fromisoformat(latest)):
                raise ValueError("as_of cannot precede the latest ledger event")
            cash, realized = self._account_values(connection)
            rows = connection.execute(
                """
                SELECT
                    ts_code,
                    SUM(remaining_quantity) AS quantity,
                    SUM(CASE WHEN available_date <= ? THEN remaining_quantity ELSE 0 END)
                        AS available_quantity
                FROM paper_lot
                WHERE account_id = ? AND remaining_quantity > 0
                GROUP BY ts_code ORDER BY ts_code
                """,
                (as_of_trade_date, self.account_id),
            ).fetchall()
            holdings: list[PaperHolding] = []
            for row in rows:
                code = str(row["ts_code"])
                if code not in market_prices:
                    raise ValueError(f"missing market price for {code}")
                lot_rows = connection.execute(
                    """
                    SELECT remaining_quantity, unit_cost FROM paper_lot
                    WHERE account_id = ? AND ts_code = ? AND remaining_quantity > 0
                    """,
                    (self.account_id, code),
                ).fetchall()
                quantity = int(row["quantity"])
                cost = sum(
                    (
                        Decimal(lot["unit_cost"]) * int(lot["remaining_quantity"])
                        for lot in lot_rows
                    ),
                    Decimal("0"),
                )
                available = int(row["available_quantity"])
                holdings.append(
                    PaperHolding(
                        code=code,
                        quantity=quantity,
                        available_quantity=available,
                        frozen_quantity=quantity - available,
                        average_cost=cost / quantity,
                        market_price=market_prices[code],
                    )
                )
        unrealized = sum(
            (
                (holding.market_price - holding.average_cost) * holding.quantity
                for holding in holdings
            ),
            Decimal("0"),
        )
        holdings_value = sum(
            (holding.market_price * holding.quantity for holding in holdings),
            Decimal("0"),
        )
        return PaperAccountSnapshot(
            account_id=self.account_id,
            as_of_time=as_of,
            cash=cash,
            available_cash=cash,
            frozen_cash=Decimal("0"),
            holdings=tuple(holdings),
            realized_pnl=realized,
            unrealized_pnl=unrealized,
            nav=cash + holdings_value,
        )

    def latest_execution_prices(
        self,
        *,
        as_of: AwareUtcDatetime,
    ) -> Mapping[str, Decimal]:
        """Return the latest known execution price per symbol at the PIT cutoff."""

        self.require_trusted_ledger()
        cutoff = _utc_iso(as_of)
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT o.ts_code, f.price, f.executed_at, f.persisted_at
                FROM paper_fill AS f
                JOIN paper_order AS o ON o.order_id = f.order_id
                WHERE o.account_id = ? AND f.executed_at <= ?
                ORDER BY f.executed_at DESC, f.sequence DESC, f.fill_id DESC
                """,
                (self.account_id, cutoff),
            ).fetchall()
        prices: dict[str, Decimal] = {}
        for row in rows:
            if row["persisted_at"] is None:
                raise PaperBrokerReconciliationError(
                    "fill persisted_at is unknown; PIT execution prices fail closed"
                )
            try:
                executed_at = _utc(
                    datetime.fromisoformat(str(row["executed_at"]).replace("Z", "+00:00"))
                )
                persisted_at = _utc(
                    datetime.fromisoformat(str(row["persisted_at"]).replace("Z", "+00:00"))
                )
            except ValueError as exc:
                raise PaperBrokerReconciliationError(
                    "fill availability timestamp is invalid"
                ) from exc
            if persisted_at < executed_at:
                raise PaperBrokerReconciliationError("fill persisted_at cannot precede executed_at")
            if persisted_at > as_of:
                continue
            prices.setdefault(str(row["ts_code"]), Decimal(row["price"]))
        return prices

    def account_authority_snapshot(
        self,
        *,
        as_of: AwareUtcDatetime,
        market_prices: Mapping[str, Decimal],
        producer_commit: str,
    ) -> PaperAccountAuthoritySnapshot:
        self.require_trusted_ledger()
        candidate = self.account_snapshot(as_of=as_of, market_prices=market_prices)
        state_fingerprint = _account_state_fingerprint(candidate)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._require_trusted_ledger(connection)
                row = connection.execute(
                    """
                    SELECT revision, state_fingerprint, producer_commit, snapshot_json
                    FROM paper_account_authority WHERE account_id = ?
                    """,
                    (self.account_id,),
                ).fetchone()
                if row is not None:
                    persisted = PaperAccountAuthoritySnapshot(
                        revision=row["revision"],
                        state_fingerprint=row["state_fingerprint"],
                        producer_commit=row["producer_commit"],
                        snapshot=PaperAccountSnapshot.model_validate_json(row["snapshot_json"]),
                    )
                    if (
                        persisted.state_fingerprint == state_fingerprint
                        and persisted.producer_commit == producer_commit
                    ):
                        connection.rollback()
                        return persisted
                    revision = persisted.revision + 1
                else:
                    revision = 1
                state = PaperAccountAuthoritySnapshot(
                    revision=revision,
                    state_fingerprint=state_fingerprint,
                    producer_commit=producer_commit,
                    snapshot=candidate,
                )
                connection.execute(
                    """
                    INSERT INTO paper_account_authority(
                        account_id, revision, state_fingerprint,
                        producer_commit, snapshot_json
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(account_id) DO UPDATE SET
                        revision = excluded.revision,
                        state_fingerprint = excluded.state_fingerprint,
                        producer_commit = excluded.producer_commit,
                        snapshot_json = excluded.snapshot_json
                    """,
                    (
                        self.account_id,
                        state.revision,
                        state.state_fingerprint,
                        state.producer_commit,
                        json.dumps(
                            state.snapshot.model_dump(mode="json"),
                            ensure_ascii=True,
                            separators=(",", ":"),
                            sort_keys=True,
                        ),
                    ),
                )
                self._append_ledger_attestation(
                    connection,
                    event_kind="account_authority",
                    event_fingerprint=state.state_fingerprint,
                    count_deltas={"authority_count": int(row is None)},
                )
                connection.commit()
                return state
            except BaseException:
                if connection.in_transaction:
                    connection.rollback()
                raise

    def reconcile(self) -> PaperBrokerReconciliation:
        errors: list[str] = []
        with self._connect() as connection:
            self._require_trusted_ledger(connection)
            account = connection.execute(
                """
                SELECT initial_cash, cash, realized_pnl, cost_spec_id,
                       cost_spec_schema_version, cost_provenance_state
                FROM broker_account
                WHERE account_id = ?
                """,
                (self.account_id,),
            ).fetchone()
            if account is None:
                raise PaperBrokerReconciliationError("broker account is missing")
            spec = self.cost_policy.execution_cost_spec
            assert spec.cost_spec_id is not None
            assert spec.cost_engine_version is not None
            authority = connection.execute(
                "SELECT schema_version, cost_engine_version, canonical_json "
                "FROM paper_cost_spec WHERE cost_spec_id = ?",
                (spec.cost_spec_id,),
            ).fetchone()
            if (
                account["cost_provenance_state"] != PaperCostProvenanceState.KNOWN_V3.value
                or account["cost_spec_schema_version"] != 3
                or account["cost_spec_id"] != spec.cost_spec_id
                or authority is None
                or authority["schema_version"] != 3
                or authority["cost_engine_version"] != spec.cost_engine_version
                or authority["canonical_json"] != spec.canonical_json()
            ):
                raise PaperBrokerReconciliationError(
                    "paper account v3 cost authority does not reconcile"
                )
            schema = connection.execute(
                "SELECT * FROM paper_ledger_schema WHERE singleton = 1"
            ).fetchone()
            if schema is None or int(schema["schema_version"]) != 5:
                raise PaperBrokerReconciliationError(
                    "paper ledger requires explicit schema v5 migration"
                )
            orders = connection.execute(
                """
                SELECT
                    o.*,
                    i.payload_json AS order_intent_payload_json,
                    i.persisted_at AS order_intent_persisted_at,
                    i.signal_id AS order_intent_signal_id,
                    i.entry_signal_id AS order_intent_entry_signal_id,
                    i.ts_code AS order_intent_ts_code,
                    i.side AS order_intent_side,
                    i.initial_execution_id AS order_intent_initial_execution_id,
                    i.initial_execution_request_fingerprint AS
                        order_intent_initial_execution_request_fingerprint
                FROM paper_order AS o
                JOIN paper_intent AS i ON i.intent_id = o.intent_id
                WHERE o.account_id = ? ORDER BY o.order_id
                """,
                (self.account_id,),
            ).fetchall()
            fills = connection.execute(
                """
                SELECT
                    f.*,
                    o.side,
                    o.ts_code AS order_ts_code,
                    o.entry_signal_id AS order_entry_signal_id,
                    i.payload_json AS intent_payload_json,
                    i.persisted_at AS intent_persisted_at
                FROM paper_fill AS f
                JOIN paper_order AS o ON o.order_id = f.order_id
                JOIN paper_intent AS i ON i.intent_id = o.intent_id
                WHERE o.account_id = ? ORDER BY f.fill_id
                """,
                (self.account_id,),
            ).fetchall()
            receipt_rows = connection.execute(
                """
                SELECT execution_id FROM paper_execution_receipt
                WHERE account_id = ? ORDER BY execution_id
                """,
                (self.account_id,),
            ).fetchall()
            expected_receipt_ids = {
                str(row["order_intent_initial_execution_id"])
                for row in orders
                if row["order_intent_initial_execution_id"] is not None
            } | {str(row["execution_id"]) for row in fills}
            actual_receipt_ids = {str(row["execution_id"]) for row in receipt_rows}
            if expected_receipt_ids != actual_receipt_ids:
                errors.append("paper execution receipt identity set mismatch")
            receipts: dict[str, PaperExecutionReceipt] = {}
            for receipt_id in sorted(actual_receipt_ids):
                try:
                    receipt = self._execution_receipt(
                        connection,
                        execution_id=receipt_id,
                    )
                except PaperBrokerReconciliationError as exc:
                    errors.append(str(exc))
                    continue
                assert receipt is not None
                receipts[receipt_id] = receipt
            fill_by_order: dict[str, list[sqlite3.Row]] = {}
            for fill in fills:
                fill_by_order.setdefault(str(fill["order_id"]), []).append(fill)
            for row in orders:
                order = self._order_from_row(row)
                order_fills = sorted(
                    fill_by_order.get(order.order_id, []),
                    key=lambda item: (int(item["sequence"]), str(item["fill_id"])),
                )
                try:
                    intent = PaperOrderIntent.model_validate_json(row["order_intent_payload_json"])
                except (TypeError, ValueError) as exc:
                    errors.append(f"order {order.order_id} intent payload is invalid: {exc}")
                    continue
                self._reconciliation_timestamp(
                    row["order_intent_persisted_at"],
                    label=f"order {order.order_id} intent persisted_at",
                    errors=errors,
                )
                if (
                    intent.intent_id != order.intent_id
                    or intent.account_id != order.account_id
                    or intent.ts_code != order.ts_code
                    or intent.side is not order.side
                    or intent.order_type is not order.order_type
                    or intent.quantity != order.quantity
                    or row["order_intent_signal_id"] != intent.signal_id
                    or row["order_intent_entry_signal_id"] != intent.entry_signal_id
                    or row["order_intent_ts_code"] != intent.ts_code
                    or row["order_intent_side"] != intent.side.value
                ):
                    errors.append(f"order {order.order_id} intent/order mismatch")
                initial_execution_id = row["order_intent_initial_execution_id"]
                initial_receipt = receipts.get(str(initial_execution_id))
                if initial_receipt is None:
                    errors.append(f"order {order.order_id} initial execution receipt is missing")
                elif (
                    initial_receipt.intent_id != intent.intent_id
                    or initial_receipt.order.order_id != order.order_id
                    or initial_receipt.request_fingerprint
                    != row["order_intent_initial_execution_request_fingerprint"]
                ):
                    errors.append(f"order {order.order_id} initial execution receipt mismatch")
                expected_entry_signal_id = (
                    None if intent.side is PaperSide.BUY else intent.entry_signal_id
                )
                if row["entry_signal_id"] != expected_entry_signal_id:
                    errors.append(f"order {order.order_id} entry_signal_id provenance mismatch")
                if intent.side is PaperSide.SELL:
                    self._reconcile_sell_entry_provenance(
                        connection,
                        intent=intent,
                        order=order,
                        errors=errors,
                    )

                fill_quantity = sum(int(fill["quantity"]) for fill in order_fills)
                if fill_quantity != order.filled_quantity:
                    errors.append(f"order {order.order_id} fill quantity mismatch")
                sequences = tuple(int(fill["sequence"]) for fill in order_fills)
                if sequences != tuple(range(1, len(order_fills) + 1)):
                    errors.append(f"order {order.order_id} fill sequence mismatch")
                previous_executed_at: datetime | None = None
                previous_persisted_at: datetime | None = None
                for fill in order_fills:
                    sequence = int(fill["sequence"])
                    executed_at = self._reconciliation_timestamp(
                        fill["executed_at"],
                        label=f"order {order.order_id} fill {sequence} executed_at",
                        errors=errors,
                    )
                    persisted_at = self._reconciliation_timestamp(
                        fill["persisted_at"],
                        label=f"order {order.order_id} fill {sequence} persisted_at",
                        errors=errors,
                    )
                    if (
                        executed_at is not None
                        and persisted_at is not None
                        and persisted_at < executed_at
                    ):
                        errors.append(
                            f"order {order.order_id} fill {sequence} availability "
                            "precedes execution"
                        )
                    if (
                        executed_at is not None
                        and previous_executed_at is not None
                        and executed_at < previous_executed_at
                    ):
                        errors.append(
                            f"order {order.order_id} fill execution sequence is nonmonotonic"
                        )
                    if (
                        persisted_at is not None
                        and previous_persisted_at is not None
                        and persisted_at < previous_persisted_at
                    ):
                        errors.append(
                            f"order {order.order_id} fill availability sequence is nonmonotonic"
                        )
                    if executed_at is not None and order.updated_at < executed_at:
                        errors.append(f"order {order.order_id} updated_at precedes fill execution")
                    if persisted_at is not None and order.updated_at < persisted_at:
                        errors.append(
                            f"order {order.order_id} updated_at precedes fill availability"
                        )
                    if executed_at is not None:
                        previous_executed_at = executed_at
                    if persisted_at is not None:
                        previous_persisted_at = persisted_at
                if order_fills:
                    weighted_price = (
                        sum(
                            (
                                Decimal(fill["price"]) * int(fill["quantity"])
                                for fill in order_fills
                            ),
                            Decimal("0"),
                        )
                        / fill_quantity
                    ).quantize(self._execution_price_tick, rounding=ROUND_HALF_UP)
                    if order.average_fill_price != weighted_price:
                        errors.append(f"order {order.order_id} average fill price mismatch")
                elif order.average_fill_price is not None:
                    errors.append(f"order {order.order_id} has average price without fills")

                if (
                    order.status
                    in {
                        PaperOrderStatus.PENDING,
                        PaperOrderStatus.ACCEPTED,
                        PaperOrderStatus.REJECTED,
                    }
                    and fill_quantity != 0
                ):
                    errors.append(f"unfilled order {order.order_id} has fills")
                elif order.status is PaperOrderStatus.PARTIALLY_FILLED and not (
                    0 < fill_quantity < order.quantity
                ):
                    errors.append(
                        f"partially filled order {order.order_id} has invalid fill quantity"
                    )
                elif order.status is PaperOrderStatus.FILLED and fill_quantity != order.quantity:
                    errors.append(f"filled order {order.order_id} is not fully consumed")
                elif order.status in {
                    PaperOrderStatus.CANCELLED,
                    PaperOrderStatus.EXPIRED,
                } and not (0 <= fill_quantity < order.quantity):
                    errors.append(f"closed order {order.order_id} has no unfilled remainder")

            expected_cash = Decimal(account["initial_cash"])
            expected_realized = Decimal("0")
            for fill in fills:
                try:
                    parsed = self._fill_from_row(fill)
                except (TypeError, ValueError) as exc:
                    errors.append(
                        f"paper fill {fill['fill_id']} v3 cost evidence is invalid: {exc}"
                    )
                    continue
                receipt = receipts.get(str(parsed.execution_id))
                if receipt is None or receipt.fill != parsed:
                    errors.append(f"fill {parsed.fill_id} immutable execution receipt mismatch")
                try:
                    intent = PaperOrderIntent.model_validate_json(fill["intent_payload_json"])
                except (TypeError, ValueError) as exc:
                    errors.append(f"fill {parsed.fill_id} intent payload is invalid: {exc}")
                    continue
                calculation = None if receipt is None else receipt.cost_calculation
                if (
                    parsed.cost_provenance_state is not PaperCostProvenanceState.KNOWN_V3
                    or calculation is None
                    or calculation.order_input.side.value != fill["side"]
                    or calculation.order_input.quantity != parsed.quantity
                    or calculation.cost_spec_id != parsed.cost_spec_id
                    or calculation.cost_spec_schema_version != parsed.cost_spec_schema_version
                    or calculation.cost_context_fingerprint != parsed.cost_context_fingerprint
                ):
                    errors.append(f"fill {parsed.fill_id} v3 cost provenance mismatch")
                else:
                    try:
                        recomputed = calculate_execution_costs(
                            spec,
                            calculation.order_input,
                            calculation.instrument_context,
                        )
                    except (TypeError, ValueError) as exc:
                        errors.append(f"fill {parsed.fill_id} v3 cost context is invalid: {exc}")
                    else:
                        if (
                            recomputed != calculation
                            or parsed.price != calculation.executed_price
                            or parsed.commission != calculation.commission
                            or parsed.transfer_fee != calculation.transfer_fee
                            or parsed.tax != calculation.stamp_duty
                            or parsed.total_fees != calculation.total_fees
                        ):
                            errors.append(f"fill {parsed.fill_id} v3 cost calculation mismatch")
                persisted_at = self._reconciliation_timestamp(
                    fill["persisted_at"],
                    label=f"fill {parsed.fill_id} persisted_at",
                    errors=errors,
                )
                intent_persisted_at = self._reconciliation_timestamp(
                    fill["intent_persisted_at"],
                    label=f"fill {parsed.fill_id} intent persisted_at",
                    errors=errors,
                )
                if persisted_at is not None and persisted_at < parsed.executed_at:
                    errors.append(f"fill {parsed.fill_id} availability precedes execution")
                if (
                    persisted_at is not None
                    and intent_persisted_at is not None
                    and persisted_at < intent_persisted_at
                ):
                    errors.append(f"fill {parsed.fill_id} availability precedes its intent")
                expected_entry_signal_id = (
                    None if intent.side is PaperSide.BUY else intent.entry_signal_id
                )
                if fill["order_entry_signal_id"] != expected_entry_signal_id:
                    errors.append(f"order {parsed.order_id} entry_signal_id provenance mismatch")
                if fill["side"] == PaperSide.BUY.value:
                    assert parsed.total_fees is not None
                    expected_cash -= parsed.notional + parsed.total_fees
                    lot = connection.execute(
                        """
                        SELECT
                            original_quantity,
                            persisted_at,
                            entry_signal_id,
                            account_id,
                            ts_code,
                            buy_executed_at,
                            buy_persisted_at,
                            buy_fill_sequence
                        FROM paper_lot WHERE lot_id = ?
                        """,
                        (parsed.fill_id,),
                    ).fetchone()
                    if lot is None or int(lot["original_quantity"]) != parsed.quantity:
                        errors.append(f"buy fill {parsed.fill_id} lot mismatch")
                    elif (
                        lot["entry_signal_id"] != intent.signal_id
                        or lot["account_id"] != intent.account_id
                        or lot["ts_code"] != intent.ts_code
                    ):
                        errors.append(
                            f"buy fill {parsed.fill_id} lot entry_signal_id provenance mismatch"
                        )
                    if lot is not None:
                        lot_persisted_at = self._reconciliation_timestamp(
                            lot["persisted_at"],
                            label=f"lot {parsed.fill_id} persisted_at",
                            errors=errors,
                        )
                        lot_buy_executed_at = self._reconciliation_timestamp(
                            lot["buy_executed_at"],
                            label=f"lot {parsed.fill_id} buy_executed_at",
                            errors=errors,
                        )
                        lot_buy_persisted_at = self._reconciliation_timestamp(
                            lot["buy_persisted_at"],
                            label=f"lot {parsed.fill_id} buy_persisted_at",
                            errors=errors,
                        )
                        if (
                            lot_persisted_at is not None
                            and persisted_at is not None
                            and lot_persisted_at != persisted_at
                        ):
                            errors.append(f"buy fill {parsed.fill_id} lot availability mismatch")
                        if (
                            lot_buy_executed_at != parsed.executed_at
                            or lot_buy_persisted_at != persisted_at
                            or int(lot["buy_fill_sequence"]) != parsed.sequence
                        ):
                            errors.append(f"buy fill {parsed.fill_id} lot timeline mismatch")
                else:
                    assert parsed.total_fees is not None
                    expected_cash += parsed.notional - parsed.total_fees
                    allocations = connection.execute(
                        """
                        SELECT
                            c.quantity,
                            c.unit_cost,
                            c.persisted_at,
                            l.entry_signal_id,
                            l.account_id,
                            l.ts_code
                        FROM paper_lot_consumption AS c
                        JOIN paper_lot AS l ON l.lot_id = c.lot_id
                        WHERE c.fill_id = ?
                        """,
                        (parsed.fill_id,),
                    ).fetchall()
                    allocated = sum(int(item["quantity"]) for item in allocations)
                    if allocated != parsed.quantity:
                        errors.append(f"sell fill {parsed.fill_id} allocation mismatch")
                    for item in allocations:
                        consumption_persisted_at = self._reconciliation_timestamp(
                            item["persisted_at"],
                            label=f"sell fill {parsed.fill_id} consumption persisted_at",
                            errors=errors,
                        )
                        if (
                            consumption_persisted_at is not None
                            and persisted_at is not None
                            and consumption_persisted_at != persisted_at
                        ):
                            errors.append(
                                f"sell fill {parsed.fill_id} consumption availability mismatch"
                            )
                        if (
                            item["entry_signal_id"] != intent.entry_signal_id
                            or item["account_id"] != intent.account_id
                            or item["ts_code"] != intent.ts_code
                        ):
                            errors.append(
                                f"sell fill {parsed.fill_id} lot entry_signal_id "
                                "provenance mismatch"
                            )
                    cost_basis = sum(
                        (
                            Decimal(item["unit_cost"]) * int(item["quantity"])
                            for item in allocations
                        ),
                        Decimal("0"),
                    )
                    expected_realized += parsed.notional - parsed.total_fees - cost_basis

            lots = connection.execute(
                """
                SELECT lot_id, original_quantity, remaining_quantity FROM paper_lot
                WHERE account_id = ?
                """,
                (self.account_id,),
            ).fetchall()
            for lot in lots:
                consumed = connection.execute(
                    """
                    SELECT COALESCE(SUM(quantity), 0) FROM paper_lot_consumption
                    WHERE lot_id = ?
                    """,
                    (lot["lot_id"],),
                ).fetchone()[0]
                if int(lot["remaining_quantity"]) + int(consumed) != int(lot["original_quantity"]):
                    errors.append(f"lot {lot['lot_id']} quantity mismatch")

            stored_cash = Decimal(account["cash"])
            stored_realized = Decimal(account["realized_pnl"])
            if stored_cash != expected_cash:
                errors.append("cash does not reconcile from fills")
            if stored_realized != expected_realized:
                errors.append("realized_pnl does not reconcile from fills and lots")
            open_quantity = sum(int(lot["remaining_quantity"]) for lot in lots)
        if errors:
            raise PaperBrokerReconciliationError("; ".join(errors))
        return PaperBrokerReconciliation(
            is_consistent=True,
            account_id=self.account_id,
            order_count=len(orders),
            fill_count=len(fills),
            open_lot_quantity=open_quantity,
            cash=stored_cash,
            realized_pnl=stored_realized,
        )

    def compare_research_execution_costs(
        self,
        research: ExecutionCostBindingEvidence,
        *,
        account_id: str,
        execution_ids: tuple[str, ...],
    ) -> PaperExecutionCostComparison:
        """Compare only after one anchored, reconciled, read-transaction snapshot."""

        normalized_ids = tuple(_execution_id(value) for value in execution_ids)
        if not normalized_ids or len(normalized_ids) != len(set(normalized_ids)):
            raise ValueError("execution_ids must be a non-empty unique tuple")
        if account_id != self.account_id:
            return PaperExecutionCostComparison(
                is_comparable=False,
                reason="ACCOUNT_BINDING_MISMATCH",
                account_id=account_id,
                execution_ids=normalized_ids,
            )
        with self._connect() as connection:
            connection.execute("BEGIN")
            try:
                self._require_trusted_ledger(connection)
                account = connection.execute(
                    """
                    SELECT cost_spec_id, cost_spec_schema_version, cost_provenance_state
                    FROM broker_account WHERE account_id = ?
                    """,
                    (account_id,),
                ).fetchone()
                if (
                    account is None
                    or account["cost_provenance_state"] != PaperCostProvenanceState.KNOWN_V3.value
                    or int(account["cost_spec_schema_version"]) != 3
                    or account["cost_spec_id"] is None
                ):
                    raise PaperBrokerReconciliationError(
                        "paper comparator account lacks a known v3 cost binding"
                    )
                authority = connection.execute(
                    """
                    SELECT canonical_json FROM paper_cost_spec
                    WHERE cost_spec_id = ? AND schema_version = 3
                    """,
                    (account["cost_spec_id"],),
                ).fetchone()
                if authority is None:
                    raise PaperBrokerReconciliationError(
                        "paper comparator cost authority is missing"
                    )
                spec = ExecutionCostSpec.from_canonical_json(authority["canonical_json"])
                if spec.cost_spec_id != account["cost_spec_id"]:
                    raise PaperBrokerReconciliationError(
                        "paper comparator cost authority identity is invalid"
                    )
                reconciliation_digest, calculations = self._reconcile_comparison_snapshot(
                    connection,
                    spec=spec,
                    execution_ids=normalized_ids,
                )
                migration, latest = self._attestation_head(connection)
                marker = connection.execute(
                    "SELECT * FROM paper_ledger_head_marker WHERE revision = ? LIMIT 1",
                    (int(latest["revision"]),),
                ).fetchone()
                if marker is None:
                    raise PaperBrokerReconciliationError(
                        "paper comparator canonical head marker is missing"
                    )
                observed = {
                    "ledger_generation": str(latest["ledger_generation"]),
                    "head_revision": int(latest["revision"]),
                    "head_marker_fingerprint": str(marker["head_marker_fingerprint"]),
                    "attestation_fingerprint": str(latest["attestation_fingerprint"]),
                    "migration_attestation_digest": str(
                        migration["migration_attestation_fingerprint"]
                    ),
                    "reconciliation_digest": reconciliation_digest,
                }
                if not self._anchor_matches_current_head(observed):
                    return PaperExecutionCostComparison(
                        is_comparable=False,
                        reason="CURRENT_HEAD_UNANCHORED",
                        account_id=account_id,
                        execution_ids=normalized_ids,
                        **observed,
                    )
                math_match = compare_execution_cost_math(research, spec, calculations)
                if not math_match.matches:
                    return PaperExecutionCostComparison(
                        is_comparable=False,
                        reason=math_match.reason,
                        account_id=account_id,
                        execution_ids=normalized_ids,
                        **observed,
                    )
                return PaperExecutionCostComparison(
                    is_comparable=True,
                    reason="EXACT_V3_BOUND",
                    account_id=account_id,
                    execution_ids=normalized_ids,
                    **observed,
                )
            except (PaperBrokerReconciliationError, TypeError, ValueError, sqlite3.DatabaseError):
                return PaperExecutionCostComparison(
                    is_comparable=False,
                    reason="PAPER_LEDGER_RECONCILIATION_FAILED",
                    account_id=account_id,
                    execution_ids=normalized_ids,
                )
            finally:
                if connection.in_transaction:
                    connection.rollback()

    def _anchor_matches_current_head(self, observed: Mapping[str, object]) -> bool:
        path = self.ledger_anchor_path
        verifier = self.ledger_anchor_verifier
        if self.ledger_id is None or path is None or verifier is None:
            return False
        if not path.is_file() or path.is_symlink() or path.stat().st_size > 64 * 1024:
            return False
        try:
            anchor = PaperLedgerAnchor.model_validate_json(path.read_bytes())
        except (OSError, ValueError):
            return False
        claims = anchor.claims
        return verifier.verify(anchor) and (
            claims.ledger_id == self.ledger_id
            and claims.schema_version == 5
            and claims.migration_attestation_digest == observed["migration_attestation_digest"]
            and claims.head_revision == observed["head_revision"]
            and claims.head_marker_fingerprint == observed["head_marker_fingerprint"]
            and claims.attestation_fingerprint == observed["attestation_fingerprint"]
        )

    def _reconcile_comparison_snapshot(
        self,
        connection: sqlite3.Connection,
        *,
        spec: ExecutionCostSpec,
        execution_ids: tuple[str, ...],
    ) -> tuple[str, tuple[ExecutionCostCalculation, ...]]:
        account = connection.execute(
            "SELECT * FROM broker_account WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if account is None:
            raise PaperBrokerReconciliationError("paper comparator account is missing")
        authority_row = connection.execute(
            "SELECT * FROM paper_account_authority WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if authority_row is None:
            raise PaperBrokerReconciliationError("paper comparator account authority is missing")
        authority = PaperAccountAuthoritySnapshot(
            revision=authority_row["revision"],
            state_fingerprint=authority_row["state_fingerprint"],
            producer_commit=authority_row["producer_commit"],
            snapshot=PaperAccountSnapshot.model_validate_json(authority_row["snapshot_json"]),
        )
        if (
            authority.snapshot.account_id != self.account_id
            or authority.snapshot.cash != Decimal(account["cash"])
            or authority.snapshot.realized_pnl != Decimal(account["realized_pnl"])
        ):
            raise PaperBrokerReconciliationError(
                "paper comparator persisted account authority differs"
            )
        fill_rows = connection.execute(
            """
            SELECT f.*, o.side FROM paper_fill AS f
            JOIN paper_order AS o ON o.order_id = f.order_id
            WHERE o.account_id = ?
            ORDER BY f.executed_at, f.persisted_at, f.sequence, f.fill_id
            """,
            (self.account_id,),
        ).fetchall()
        expected_cash = Decimal(account["initial_cash"])
        expected_realized = Decimal("0")
        reconciled_fills: list[dict[str, object]] = []
        requested: dict[str, ExecutionCostCalculation] = {}
        for row in fill_rows:
            fill = self._fill_from_row(row)
            receipt = self._execution_receipt(connection, execution_id=fill.execution_id)
            if (
                receipt is None
                or receipt.fill != fill
                or receipt.order.account_id != self.account_id
                or receipt.cost_calculation is None
                or receipt.cost_provenance_state is not PaperCostProvenanceState.KNOWN_V3
            ):
                raise PaperBrokerReconciliationError(
                    "paper comparator fill receipt does not reconcile"
                )
            calculation = receipt.cost_calculation
            replayed = calculate_execution_costs(
                spec,
                calculation.order_input,
                calculation.instrument_context,
            )
            if (
                replayed != calculation
                or fill.price != replayed.executed_price
                or fill.commission != replayed.commission
                or fill.transfer_fee != replayed.transfer_fee
                or fill.tax != replayed.stamp_duty
                or fill.total_fees != replayed.total_fees
            ):
                raise PaperBrokerReconciliationError(
                    "paper comparator shared-engine replay differs from persisted fill"
                )
            assert fill.total_fees is not None
            if row["side"] == PaperSide.BUY.value:
                expected_cash -= fill.notional + fill.total_fees
                lot = connection.execute(
                    "SELECT * FROM paper_lot WHERE lot_id = ?",
                    (fill.fill_id,),
                ).fetchone()
                if (
                    lot is None
                    or int(lot["original_quantity"]) != fill.quantity
                    or Decimal(lot["unit_cost"])
                    != (fill.notional + fill.total_fees) / fill.quantity
                ):
                    raise PaperBrokerReconciliationError(
                        "paper comparator BUY lot basis does not reconcile"
                    )
            else:
                allocations = connection.execute(
                    """
                    SELECT quantity, unit_cost FROM paper_lot_consumption
                    WHERE fill_id = ? ORDER BY lot_id
                    """,
                    (fill.fill_id,),
                ).fetchall()
                if sum(int(item["quantity"]) for item in allocations) != fill.quantity:
                    raise PaperBrokerReconciliationError(
                        "paper comparator SELL FIFO allocation does not reconcile"
                    )
                basis = sum(
                    (Decimal(item["unit_cost"]) * int(item["quantity"]) for item in allocations),
                    Decimal("0"),
                )
                proceeds = fill.notional - fill.total_fees
                expected_cash += proceeds
                expected_realized += proceeds - basis
            if fill.execution_id in execution_ids:
                requested[fill.execution_id] = replayed
            reconciled_fills.append(fill.model_dump(mode="python"))
        for lot in connection.execute(
            "SELECT * FROM paper_lot WHERE account_id = ? ORDER BY lot_id",
            (self.account_id,),
        ).fetchall():
            consumed = int(
                connection.execute(
                    "SELECT COALESCE(SUM(quantity), 0) FROM paper_lot_consumption WHERE lot_id = ?",
                    (lot["lot_id"],),
                ).fetchone()[0]
            )
            if int(lot["remaining_quantity"]) + consumed != int(lot["original_quantity"]):
                raise PaperBrokerReconciliationError(
                    "paper comparator lot quantity does not reconcile"
                )
        authority_quantities = {
            holding.code: holding.quantity for holding in authority.snapshot.holdings
        }
        open_quantities = {
            str(row["ts_code"]): int(row["quantity"])
            for row in connection.execute(
                """
                SELECT ts_code, SUM(remaining_quantity) AS quantity FROM paper_lot
                WHERE account_id = ? AND remaining_quantity > 0 GROUP BY ts_code
                """,
                (self.account_id,),
            ).fetchall()
        }
        if authority_quantities != open_quantities:
            raise PaperBrokerReconciliationError(
                "paper comparator account authority holdings differ"
            )
        if (
            Decimal(account["cash"]) != expected_cash
            or Decimal(account["realized_pnl"]) != expected_realized
        ):
            raise PaperBrokerReconciliationError(
                "paper comparator account cash or realized P&L does not reconcile"
            )
        if set(requested) != set(execution_ids):
            raise PaperBrokerReconciliationError(
                "paper comparator requested execution topology does not reconcile"
            )
        digest = canonical_sha256(
            {
                "account_id": self.account_id,
                "initial_cash": str(account["initial_cash"]),
                "cash": str(account["cash"]),
                "realized_pnl": str(account["realized_pnl"]),
                "cost_spec_id": str(account["cost_spec_id"]),
                "account_authority_fingerprint": authority.state_fingerprint,
                "fills": tuple(reconciled_fills),
            }
        )
        return digest, tuple(requested[execution_id] for execution_id in execution_ids)

    def _reconcile_sell_entry_provenance(
        self,
        connection: sqlite3.Connection,
        *,
        intent: PaperOrderIntent,
        order: PaperOrder,
        errors: list[str],
    ) -> None:
        authority = intent.sell_quantity_authority
        if (
            intent.entry_signal_id is None
            or authority is None
            or authority.exit_signal_id != intent.signal_id
            or authority.entry_signal_id != intent.entry_signal_id
            or authority.account_id != intent.account_id
            or authority.ts_code != intent.ts_code
            or authority.requested_quantity != intent.quantity
        ):
            errors.append(f"order {order.order_id} SELL authority provenance mismatch")
            return
        entry_rows = connection.execute(
            """
            SELECT bi.payload_json, bo.account_id, bo.ts_code, bo.side,
                   bo.entry_signal_id,
                   EXISTS(
                       SELECT 1 FROM paper_fill AS bf
                       WHERE bf.order_id = bo.order_id
                   ) AS has_fill
            FROM paper_intent AS bi
            JOIN paper_order AS bo ON bo.intent_id = bi.intent_id
            WHERE bi.account_id = ? AND bi.ts_code = ?
              AND bi.signal_id = ? AND bi.side = 'BUY'
            """,
            (self.account_id, intent.ts_code, intent.entry_signal_id),
        ).fetchall()
        if len(entry_rows) != 1:
            errors.append(
                f"order {order.order_id} entry_signal_id does not resolve to one BUY entry"
            )
            return
        entry = entry_rows[0]
        try:
            entry_intent = PaperOrderIntent.model_validate_json(entry["payload_json"])
        except (TypeError, ValueError) as exc:
            errors.append(f"order {order.order_id} entry BUY intent is invalid: {exc}")
            return
        if (
            entry_intent.side is not PaperSide.BUY
            or entry_intent.signal_id != intent.entry_signal_id
            or entry_intent.account_id != intent.account_id
            or entry_intent.ts_code != intent.ts_code
            or entry["account_id"] != intent.account_id
            or entry["ts_code"] != intent.ts_code
            or entry["side"] != PaperSide.BUY.value
            or entry["entry_signal_id"] is not None
            or not bool(entry["has_fill"])
        ):
            errors.append(f"order {order.order_id} entry_signal_id provenance mismatch")

    @staticmethod
    def _reconciliation_timestamp(
        value: object,
        *,
        label: str,
        errors: list[str],
    ) -> datetime | None:
        if value is None:
            errors.append(f"{label} is unknown")
            return None
        try:
            return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError:
            errors.append(f"{label} is invalid")
            return None
