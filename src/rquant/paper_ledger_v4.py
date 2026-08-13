"""Frozen, independent reader and reconciler for paper-ledger schema v4."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from pydantic import Field, computed_field

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

V4_SCHEMA_VERSION = 4
V4_INTERNAL_MIGRATION_VERSION = 2
V4_ACCOUNTING_SEMANTICS = "V4_COMMISSION_TAX_ONLY_OBSERVED"

_COUNT_TABLES = {
    "broker_account_count": "broker_account",
    "intent_count": "paper_intent",
    "order_count": "paper_order",
    "fill_count": "paper_fill",
    "lot_count": "paper_lot",
    "consumption_count": "paper_lot_consumption",
    "receipt_count": "paper_execution_receipt",
    "authority_count": "paper_account_authority",
}
_COUNT_COLUMNS = tuple(_COUNT_TABLES)
_UNKNOWN_COLUMNS = (
    "unknown_fill_availability_count",
    "unknown_lot_availability_count",
    "unknown_consumption_availability_count",
    "unknown_lot_provenance_count",
    "unknown_intent_identity_count",
    "unknown_execution_identity_count",
    "unknown_lot_timeline_count",
    "unknown_initial_execution_identity_count",
    "unknown_execution_receipt_count",
)
V4_ATTESTED_SCHEMA_OBJECTS = (
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


class PaperV4ReconciliationError(RuntimeError):
    """A schema-v4 source cannot be explained by its immutable business ledger."""


class V4AccountReconciliation(RuntimeContractModel):
    account_id: str = Field(min_length=1)
    initial_cash: Decimal = Field(allow_inf_nan=False)
    replayed_cash: Decimal = Field(allow_inf_nan=False)
    stored_cash: Decimal = Field(allow_inf_nan=False)
    replayed_realized_pnl: Decimal = Field(allow_inf_nan=False)
    stored_realized_pnl: Decimal = Field(allow_inf_nan=False)
    fill_count: int = Field(ge=0)
    open_lot_quantity: int = Field(ge=0)
    errors: tuple[str, ...] = ()

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_verified(self) -> bool:
        return (
            not self.errors
            and self.replayed_cash == self.stored_cash
            and self.replayed_realized_pnl == self.stored_realized_pnl
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"is_verified", "digest"}))


class V4LedgerReconciliationReport(RuntimeContractModel):
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    schema_version: int
    internal_migration_version: int
    schema_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    predecessor_attestation_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    predecessor_head_marker_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    accounting_semantics: str
    accounts: tuple[V4AccountReconciliation, ...]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def is_verified(self) -> bool:
        return (
            self.schema_version == V4_SCHEMA_VERSION
            and self.internal_migration_version == V4_INTERNAL_MIGRATION_VERSION
            and bool(self.accounts)
            and all(account.is_verified for account in self.accounts)
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def digest(self) -> str:
        return canonical_sha256(
            {
                "source_sha256": self.source_sha256,
                "schema_version": self.schema_version,
                "internal_migration_version": self.internal_migration_version,
                "schema_fingerprint": self.schema_fingerprint,
                "predecessor_attestation_fingerprint": (self.predecessor_attestation_fingerprint),
                "predecessor_head_marker_fingerprint": (self.predecessor_head_marker_fingerprint),
                "accounting_semantics": self.accounting_semantics,
                "account_digests": tuple(account.digest for account in self.accounts),
            }
        )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _decimal(value: object, *, label: str, nonnegative: bool = False) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise PaperV4ReconciliationError(f"{label} is not a decimal") from exc
    if not parsed.is_finite() or (nonnegative and parsed < 0):
        raise PaperV4ReconciliationError(f"{label} is not a finite nonnegative decimal")
    return parsed


def _timestamp(value: object, *, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PaperV4ReconciliationError(f"{label} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PaperV4ReconciliationError(f"{label} is not timezone-aware")
    return parsed


def _json_object(value: object, *, label: str) -> dict[str, object]:
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise PaperV4ReconciliationError(f"{label} is invalid JSON") from exc
    if not isinstance(parsed, dict):
        raise PaperV4ReconciliationError(f"{label} must be a JSON object")
    return parsed


def _schema_fingerprint(connection: sqlite3.Connection) -> str:
    placeholders = ",".join("?" for _ in V4_ATTESTED_SCHEMA_OBJECTS)
    rows = connection.execute(
        f"""
        SELECT type, name, sql FROM sqlite_master
        WHERE name IN ({placeholders}) ORDER BY type, name
        """,
        V4_ATTESTED_SCHEMA_OBJECTS,
    ).fetchall()
    if {str(row["name"]) for row in rows} != set(V4_ATTESTED_SCHEMA_OBJECTS):
        raise PaperV4ReconciliationError("v4 attested schema object set is incomplete")
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
        **{column: int(row[column]) for column in _COUNT_COLUMNS},
        "persisted_at": str(row["persisted_at"]),
    }


def _head_payload(row: sqlite3.Row) -> dict[str, object]:
    return {
        "revision": int(row["revision"]),
        "ledger_generation": str(row["ledger_generation"]),
        "migration_version": int(row["migration_version"]),
        "schema_version": int(row["schema_version"]),
        "schema_fingerprint": str(row["schema_fingerprint"]),
        "attestation_fingerprint": str(row["attestation_fingerprint"]),
        "previous_head_marker_fingerprint": row["previous_head_marker_fingerprint"],
        "migration_attestation_fingerprint": str(row["migration_attestation_fingerprint"]),
        **{column: int(row[column]) for column in _COUNT_COLUMNS},
        "persisted_at": str(row["persisted_at"]),
    }


def _validate_chain(
    connection: sqlite3.Connection,
    *,
    schema_fingerprint: str,
) -> tuple[str, str]:
    if connection.execute("SELECT 1 FROM paper_ledger_tamper_marker LIMIT 1").fetchone():
        raise PaperV4ReconciliationError("v4 tamper marker is present")
    attestations = connection.execute(
        "SELECT * FROM paper_ledger_attestation ORDER BY revision"
    ).fetchall()
    heads = connection.execute(
        "SELECT * FROM paper_ledger_head_marker ORDER BY revision"
    ).fetchall()
    if not attestations or len(attestations) != len(heads):
        raise PaperV4ReconciliationError("v4 attestation/head topology is invalid")
    previous_attestation: str | None = None
    previous_head: str | None = None
    migration_digest: str | None = None
    for expected_revision, (attestation, head) in enumerate(
        zip(attestations, heads, strict=True), start=1
    ):
        attestation_payload = _attestation_payload(attestation)
        head_payload = _head_payload(head)
        attestation_fingerprint = canonical_sha256(attestation_payload)
        head_fingerprint = canonical_sha256(head_payload)
        if (
            int(attestation["revision"]) != expected_revision
            or int(head["revision"]) != expected_revision
            or _json_object(attestation["payload_json"], label="v4 attestation payload")
            != attestation_payload
            or _json_object(head["payload_json"], label="v4 head payload") != head_payload
            or str(attestation["attestation_fingerprint"]) != attestation_fingerprint
            or str(head["head_marker_fingerprint"]) != head_fingerprint
            or attestation_payload["previous_attestation_fingerprint"] != previous_attestation
            or head_payload["previous_head_marker_fingerprint"] != previous_head
            or head_payload["attestation_fingerprint"] != attestation_fingerprint
            or attestation_payload["migration_version"] != V4_INTERNAL_MIGRATION_VERSION
            or head_payload["migration_version"] != V4_INTERNAL_MIGRATION_VERSION
            or attestation_payload["schema_version"] != V4_SCHEMA_VERSION
            or head_payload["schema_version"] != V4_SCHEMA_VERSION
            or attestation_payload["schema_fingerprint"] != schema_fingerprint
            or head_payload["schema_fingerprint"] != schema_fingerprint
        ):
            raise PaperV4ReconciliationError("v4 attestation/head content is invalid")
        if migration_digest is None:
            migration_digest = str(attestation_payload["migration_attestation_fingerprint"])
            if attestation_payload["event_kind"] != "migration_audit":
                raise PaperV4ReconciliationError("v4 revision one is not migration audit")
        elif attestation_payload["migration_attestation_fingerprint"] != migration_digest:
            raise PaperV4ReconciliationError("v4 migration identity changed within the chain")
        previous_attestation = attestation_fingerprint
        previous_head = head_fingerprint
    latest = attestations[-1]
    for count_column, table in _COUNT_TABLES.items():
        actual = int(connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        if int(latest[count_column]) != actual:
            raise PaperV4ReconciliationError(f"v4 {table} count differs from trust head")
    assert previous_attestation is not None and previous_head is not None
    return previous_attestation, previous_head


def _validate_receipts(
    connection: sqlite3.Connection,
    *,
    account_id: str,
    intents: dict[str, sqlite3.Row],
    orders: dict[str, sqlite3.Row],
    fills: tuple[sqlite3.Row, ...],
) -> None:
    expected = {
        str(intent["initial_execution_id"]): (str(intent_id), None)
        for intent_id, intent in intents.items()
    }
    expected.update(
        {
            str(fill["execution_id"]): (str(orders[str(fill["order_id"])]["intent_id"]), fill)
            for fill in fills
        }
    )
    receipts = connection.execute(
        "SELECT * FROM paper_execution_receipt WHERE account_id = ? ORDER BY execution_id",
        (account_id,),
    ).fetchall()
    if {str(row["execution_id"]) for row in receipts} != set(expected):
        raise PaperV4ReconciliationError("v4 receipt identity set is incomplete or duplicated")
    for row in receipts:
        execution_id = str(row["execution_id"])
        intent_id, expected_fill = expected[execution_id]
        order = orders[str(row["order_id"])]
        request = _json_object(row["request_json"], label=f"v4 receipt {execution_id} request")
        receipt = _json_object(row["receipt_json"], label=f"v4 receipt {execution_id}")
        if canonical_sha256(request) != str(row["request_fingerprint"]):
            raise PaperV4ReconciliationError("v4 receipt request fingerprint differs")
        receipt_order = receipt.get("order")
        receipt_fill = receipt.get("fill")
        if (
            receipt.get("execution_id") != execution_id
            or receipt.get("request_fingerprint") != row["request_fingerprint"]
            or receipt.get("intent_id") != intent_id
            or _timestamp(receipt.get("persisted_at"), label="v4 receipt persisted_at")
            != _timestamp(row["persisted_at"], label="v4 receipt row persisted_at")
            or row["intent_id"] != intent_id
            or row["account_id"] != account_id
            or not isinstance(receipt_order, dict)
            or receipt_order.get("order_id") != row["order_id"]
            or receipt_order.get("intent_id") != intent_id
            or receipt_order.get("account_id") != account_id
            or receipt_order.get("side") != order["side"]
            or receipt_order.get("quantity") != int(order["quantity"])
            or receipt_order.get("filled_quantity") != int(order["filled_quantity"])
        ):
            raise PaperV4ReconciliationError("v4 receipt/order identity differs")
        if expected_fill is None:
            if receipt_fill is not None:
                raise PaperV4ReconciliationError("v4 initial receipt unexpectedly contains fill")
        elif (
            not isinstance(receipt_fill, dict)
            or receipt_fill.get("fill_id") != expected_fill["fill_id"]
            or receipt_fill.get("execution_id") != execution_id
            or receipt_fill.get("order_id") != expected_fill["order_id"]
            or receipt_fill.get("quantity") != int(expected_fill["quantity"])
            or _decimal(receipt_fill.get("price"), label="v4 receipt fill price")
            != _decimal(expected_fill["price"], label="v4 fill price")
            or _decimal(receipt_fill.get("commission"), label="v4 receipt commission")
            != _decimal(expected_fill["commission"], label="v4 commission")
            or _decimal(receipt_fill.get("tax"), label="v4 receipt tax")
            != _decimal(expected_fill["tax"], label="v4 tax")
        ):
            raise PaperV4ReconciliationError("v4 fill receipt differs from persisted fill")


def _account_report(
    connection: sqlite3.Connection, account: sqlite3.Row
) -> V4AccountReconciliation:
    account_id = str(account["account_id"])
    initial_cash = _decimal(account["initial_cash"], label="v4 initial_cash", nonnegative=True)
    stored_cash = _decimal(account["cash"], label="v4 cash", nonnegative=True)
    stored_realized = _decimal(account["realized_pnl"], label="v4 realized_pnl")
    intent_rows = connection.execute(
        "SELECT * FROM paper_intent WHERE account_id = ? ORDER BY intent_id", (account_id,)
    ).fetchall()
    order_rows = connection.execute(
        "SELECT * FROM paper_order WHERE account_id = ? ORDER BY order_id", (account_id,)
    ).fetchall()
    intents = {str(row["intent_id"]): row for row in intent_rows}
    orders = {str(row["order_id"]): row for row in order_rows}
    if len(intents) != len(intent_rows) or len(orders) != len(order_rows):
        raise PaperV4ReconciliationError("v4 intent/order identity is duplicated")
    for order in order_rows:
        intent = intents.get(str(order["intent_id"]))
        if (
            intent is None
            or intent["account_id"] != account_id
            or intent["ts_code"] != order["ts_code"]
            or intent["side"] != order["side"]
            or order["side"] not in {"BUY", "SELL"}
        ):
            raise PaperV4ReconciliationError("v4 intent/order relationship is invalid")
    fills = tuple(
        connection.execute(
            """
            SELECT f.* FROM paper_fill AS f JOIN paper_order AS o ON o.order_id = f.order_id
            WHERE o.account_id = ?
            ORDER BY f.executed_at, f.persisted_at, f.sequence, f.fill_id
            """,
            (account_id,),
        ).fetchall()
    )
    for order in order_rows:
        order_fills = [row for row in fills if row["order_id"] == order["order_id"]]
        quantity = sum(int(row["quantity"]) for row in order_fills)
        sequences = tuple(int(row["sequence"]) for row in order_fills)
        if quantity != int(order["filled_quantity"]) or sequences != tuple(
            range(1, len(order_fills) + 1)
        ):
            raise PaperV4ReconciliationError("v4 order fill topology is invalid")
        if quantity:
            weighted = (
                sum(
                    (
                        _decimal(row["price"], label="v4 fill price") * int(row["quantity"])
                        for row in order_fills
                    ),
                    Decimal("0"),
                )
                / quantity
            )
            if weighted.quantize(Decimal("0.0001")) != _decimal(
                order["average_fill_price"], label="v4 average fill price"
            ):
                raise PaperV4ReconciliationError("v4 weighted average fill price differs")
    _validate_receipts(
        connection,
        account_id=account_id,
        intents=intents,
        orders=orders,
        fills=fills,
    )
    replayed_cash = initial_cash
    replayed_realized = Decimal("0")
    lot_remaining: dict[str, int] = {}
    lot_rows = connection.execute(
        "SELECT * FROM paper_lot WHERE account_id = ? ORDER BY lot_id", (account_id,)
    ).fetchall()
    lots = {str(row["lot_id"]): row for row in lot_rows}
    fill_ids = {str(row["fill_id"]) for row in fills}
    account_consumptions = tuple(
        row
        for row in connection.execute(
            "SELECT * FROM paper_lot_consumption ORDER BY fill_id, lot_id"
        ).fetchall()
        if str(row["fill_id"]) in fill_ids or str(row["lot_id"]) in lots
    )
    reconciled_consumptions: set[tuple[str, str]] = set()
    for fill in fills:
        order = orders[str(fill["order_id"])]
        intent = intents[str(order["intent_id"])]
        quantity = int(fill["quantity"])
        if quantity <= 0 or quantity % 100:
            raise PaperV4ReconciliationError("v4 fill quantity is not a positive board lot")
        price = _decimal(fill["price"], label="v4 fill price", nonnegative=True)
        commission = _decimal(fill["commission"], label="v4 commission", nonnegative=True)
        tax = _decimal(fill["tax"], label="v4 tax", nonnegative=True)
        _timestamp(fill["executed_at"], label="v4 fill executed_at")
        _timestamp(fill["persisted_at"], label="v4 fill persisted_at")
        notional = price * quantity
        if order["side"] == "BUY":
            replayed_cash -= notional + commission
            lot = lots.get(str(fill["fill_id"]))
            expected_unit_cost = (notional + commission) / quantity
            receipt = connection.execute(
                "SELECT request_json FROM paper_execution_receipt WHERE execution_id = ?",
                (fill["execution_id"],),
            ).fetchone()
            if receipt is None:
                raise PaperV4ReconciliationError("v4 BUY receipt is missing")
            request = _json_object(
                receipt["request_json"],
                label=f"v4 BUY receipt {fill['execution_id']} request",
            )
            quote = request.get("quote")
            if not isinstance(quote, dict):
                raise PaperV4ReconciliationError("v4 BUY receipt quote is invalid")
            acquisition_trade_date = request.get("trade_date")
            available_date = quote.get("acquisition_available_date")
            if not isinstance(acquisition_trade_date, str) or not isinstance(available_date, str):
                raise PaperV4ReconciliationError("v4 BUY receipt trade-date provenance is missing")
            try:
                acquisition_date_value = date.fromisoformat(acquisition_trade_date)
                available_date_value = date.fromisoformat(available_date)
            except ValueError as exc:
                raise PaperV4ReconciliationError(
                    "v4 BUY receipt trade-date provenance is invalid"
                ) from exc
            fill_executed_at = _timestamp(fill["executed_at"], label="v4 fill executed_at")
            fill_persisted_at = _timestamp(fill["persisted_at"], label="v4 fill persisted_at")
            if (
                lot is None
                or lot["account_id"] != account_id
                or lot["ts_code"] != order["ts_code"]
                or lot["entry_signal_id"] != intent["signal_id"]
                or int(lot["original_quantity"]) != quantity
                or _decimal(lot["unit_cost"], label="v4 lot unit_cost") != expected_unit_cost
                or _timestamp(lot["persisted_at"], label="v4 lot persisted_at") != fill_persisted_at
                or _timestamp(lot["buy_executed_at"], label="v4 lot buy_executed_at")
                != fill_executed_at
                or _timestamp(lot["buy_persisted_at"], label="v4 lot buy_persisted_at")
                != fill_persisted_at
                or int(lot["buy_fill_sequence"]) != int(fill["sequence"])
                or str(lot["acquisition_trade_date"]) != acquisition_trade_date
                or str(lot["available_date"]) != available_date
                or available_date_value <= acquisition_date_value
            ):
                raise PaperV4ReconciliationError("v4 BUY lot provenance or basis is invalid")
            lot_remaining[str(lot["lot_id"])] = quantity
        else:
            consumptions = connection.execute(
                "SELECT * FROM paper_lot_consumption WHERE fill_id = ? ORDER BY lot_id",
                (fill["fill_id"],),
            ).fetchall()
            if sum(int(row["quantity"]) for row in consumptions) != quantity:
                raise PaperV4ReconciliationError("v4 SELL consumption quantity differs")
            eligible = sorted(
                (
                    lot
                    for lot in lot_rows
                    if lot["ts_code"] == order["ts_code"]
                    and lot["entry_signal_id"] == order["entry_signal_id"]
                    and str(lot["available_date"])
                    <= _timestamp(fill["executed_at"], label="v4 sell time").date().isoformat()
                    and lot_remaining.get(str(lot["lot_id"]), 0) > 0
                ),
                key=lambda lot: (
                    str(lot["available_date"]),
                    str(lot["acquisition_trade_date"]),
                    str(lot["buy_executed_at"]),
                    str(lot["buy_persisted_at"]),
                    int(lot["buy_fill_sequence"]),
                    str(lot["lot_id"]),
                ),
            )
            remaining_to_consume = quantity
            expected_allocations: list[tuple[str, int]] = []
            for lot in eligible:
                lot_id = str(lot["lot_id"])
                consumed = min(lot_remaining[lot_id], remaining_to_consume)
                if consumed:
                    expected_allocations.append((lot_id, consumed))
                    remaining_to_consume -= consumed
                if remaining_to_consume == 0:
                    break
            actual_allocations = [
                (str(row["lot_id"]), int(row["quantity"])) for row in consumptions
            ]
            if remaining_to_consume or actual_allocations != expected_allocations:
                raise PaperV4ReconciliationError("v4 SELL consumption is not FIFO")
            basis = Decimal("0")
            for row in consumptions:
                lot_id = str(row["lot_id"])
                lot = lots.get(lot_id)
                if (
                    lot is None
                    or str(row["fill_id"]) != str(fill["fill_id"])
                    or _timestamp(row["persisted_at"], label="v4 consumption persisted_at")
                    != _timestamp(fill["persisted_at"], label="v4 sell fill persisted_at")
                ):
                    raise PaperV4ReconciliationError("v4 consumption provenance is invalid")
                unit_cost = _decimal(row["unit_cost"], label="v4 consumption unit_cost")
                if unit_cost != _decimal(lot["unit_cost"], label="v4 lot unit_cost"):
                    raise PaperV4ReconciliationError("v4 consumption unit cost differs")
                consumed = int(row["quantity"])
                lot_remaining[lot_id] -= consumed
                basis += unit_cost * consumed
                reconciled_consumptions.add((str(row["fill_id"]), lot_id))
            proceeds = notional - commission - tax
            replayed_cash += proceeds
            replayed_realized += proceeds - basis
    if {
        (str(row["fill_id"]), str(row["lot_id"])) for row in account_consumptions
    } != reconciled_consumptions:
        raise PaperV4ReconciliationError("v4 consumption mapping is incomplete or invalid")
    for lot_id, lot in lots.items():
        if lot_id not in lot_remaining:
            raise PaperV4ReconciliationError("v4 lot has no corresponding BUY fill")
        if int(lot["remaining_quantity"]) != lot_remaining[lot_id]:
            raise PaperV4ReconciliationError("v4 lot remaining quantity differs from replay")
    errors: list[str] = []
    if replayed_cash != stored_cash:
        errors.append("cash does not reconcile from v4 fills")
    if replayed_realized != stored_realized:
        errors.append("realized_pnl does not reconcile from v4 fills and FIFO basis")
    return V4AccountReconciliation(
        account_id=account_id,
        initial_cash=initial_cash,
        replayed_cash=replayed_cash,
        stored_cash=stored_cash,
        replayed_realized_pnl=replayed_realized,
        stored_realized_pnl=stored_realized,
        fill_count=len(fills),
        open_lot_quantity=sum(lot_remaining.values()),
        errors=tuple(errors),
    )


class V4LedgerReconciler:
    """Read a closed schema-v4 ledger without importing broker or v5 contracts."""

    def reconcile(self, source_path: Path) -> V4LedgerReconciliationReport:
        source = Path(source_path).absolute()
        if not source.is_file() or source.is_symlink():
            raise PaperV4ReconciliationError("v4 source must be a regular file")
        source_sha256 = sha256_file(source)
        uri = f"{source.as_uri()}?mode=ro&immutable=1"
        try:
            connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        except sqlite3.DatabaseError as exc:
            raise PaperV4ReconciliationError("v4 source cannot be opened read-only") from exc
        connection.row_factory = sqlite3.Row
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()
            if integrity is None or str(integrity[0]).lower() != "ok":
                raise PaperV4ReconciliationError("v4 source integrity check failed")
            schema = connection.execute(
                "SELECT * FROM paper_ledger_schema WHERE singleton = 1"
            ).fetchone()
            if schema is None or int(schema["schema_version"]) != V4_SCHEMA_VERSION:
                raise PaperV4ReconciliationError("source is not paper ledger schema v4")
            if any(int(schema[column]) != 0 for column in _UNKNOWN_COLUMNS):
                raise PaperV4ReconciliationError("v4 source contains unknown trust evidence")
            schema_fingerprint = _schema_fingerprint(connection)
            predecessor_attestation, predecessor_head = _validate_chain(
                connection,
                schema_fingerprint=schema_fingerprint,
            )
            account_rows = connection.execute(
                "SELECT * FROM broker_account ORDER BY account_id"
            ).fetchall()
            reports = tuple(_account_report(connection, account) for account in account_rows)
        except sqlite3.DatabaseError as exc:
            raise PaperV4ReconciliationError("v4 source schema is invalid") from exc
        finally:
            connection.close()
        report = V4LedgerReconciliationReport(
            source_sha256=source_sha256,
            schema_version=V4_SCHEMA_VERSION,
            internal_migration_version=V4_INTERNAL_MIGRATION_VERSION,
            schema_fingerprint=schema_fingerprint,
            predecessor_attestation_fingerprint=predecessor_attestation,
            predecessor_head_marker_fingerprint=predecessor_head,
            accounting_semantics=V4_ACCOUNTING_SEMANTICS,
            accounts=reports,
        )
        if not report.is_verified:
            errors = "; ".join(error for account in reports for error in account.errors)
            raise PaperV4ReconciliationError(f"v4 reconciliation failed: {errors}")
        if sha256_file(source) != source_sha256:
            raise PaperV4ReconciliationError("v4 source changed during reconciliation")
        return report


__all__ = [
    "PaperV4ReconciliationError",
    "V4AccountReconciliation",
    "V4LedgerReconciler",
    "V4LedgerReconciliationReport",
    "V4_ACCOUNTING_SEMANTICS",
    "V4_ATTESTED_SCHEMA_OBJECTS",
    "V4_INTERNAL_MIGRATION_VERSION",
    "V4_SCHEMA_VERSION",
    "sha256_file",
]
