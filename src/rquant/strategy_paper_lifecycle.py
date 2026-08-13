"""Point-in-time read-only paper-ledger features for live strategy lifecycles."""

from __future__ import annotations

import json
import sqlite3
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo

from rquant.feature_contracts import (
    FeatureAvailability,
    FeatureFieldStatus,
    FeatureInstanceEnvelope,
)
from rquant.paper_contracts import (
    PaperCostProvenanceState,
    PaperFill,
    PaperOrder,
    PaperOrderIntent,
    PaperOrderStatus,
    PaperSide,
)
from rquant.research_run_spec import ExecutionCostSpec
from rquant.runtime_contracts import normalize_aware_utc
from rquant.signal_contracts import SignalAction, SignalEnvelope

_SHANGHAI = ZoneInfo("Asia/Shanghai")
_REQUIRED_TABLES = frozenset(
    {
        "broker_account",
        "paper_intent",
        "paper_order",
        "paper_fill",
        "paper_lot",
        "paper_lot_consumption",
        "paper_execution_receipt",
        "paper_ledger_schema",
        "paper_cost_spec",
    }
)


def _utc_iso(value: datetime) -> str:
    return normalize_aware_utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


class PaperLifecycleIntegrityError(RuntimeError):
    """The paper ledger cannot provide trustworthy lifecycle evidence."""


@dataclass(frozen=True)
class _VisibleEntry:
    intent: PaperOrderIntent
    order_row: sqlite3.Row
    intent_persisted_at: datetime


@dataclass(frozen=True)
class _RebuiltPosition:
    entry_quantity: int
    remaining_quantity: int
    available_quantity: int
    entry_price: Decimal
    entry_event_time: datetime
    entry_available_at: datetime
    position_event_time: datetime
    position_available_at: datetime


@dataclass(frozen=True)
class _ExitExecutionEvidence:
    status: str
    source_event_time: datetime
    available_at: datetime


class PaperBrokerLifecycleReader:
    """Resolve execution state without opening a writable broker connection."""

    def __init__(self, path: Path, *, account_id: str) -> None:
        self.path = Path(path)
        if not self.path.is_absolute():
            raise ValueError("paper broker path must be absolute")
        if not account_id or account_id != account_id.strip():
            raise ValueError("paper account_id cannot be empty or padded")
        self.account_id = account_id
        self._validate_file()
        with self._connect() as connection:
            tables = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            missing = _REQUIRED_TABLES - tables
            if missing:
                raise PaperLifecycleIntegrityError(
                    "paper broker schema is missing: " + ", ".join(sorted(missing))
                )
            schema = connection.execute(
                "SELECT * FROM paper_ledger_schema WHERE singleton = 1"
            ).fetchone()
            if schema is None or int(schema["schema_version"]) != 5:
                raise PaperLifecycleIntegrityError(
                    "paper broker ledger schema requires explicit v5 migration"
                )
            required_columns = {
                "paper_intent": {
                    "signal_id",
                    "entry_signal_id",
                    "initial_execution_id",
                    "initial_execution_request_fingerprint",
                },
                "paper_order": {"entry_signal_id"},
                "broker_account": {
                    "cost_spec_id",
                    "cost_spec_schema_version",
                    "cost_provenance_state",
                },
                "paper_fill": {
                    "execution_id",
                    "persisted_at",
                    "transfer_fee",
                    "total_fees",
                    "cost_spec_id",
                    "cost_spec_schema_version",
                    "cost_context_fingerprint",
                    "cost_provenance_state",
                },
                "paper_lot": {"entry_signal_id", "persisted_at"},
                "paper_lot_consumption": {"persisted_at"},
                "paper_execution_receipt": {
                    "execution_id",
                    "intent_id",
                    "order_id",
                    "request_fingerprint",
                    "request_json",
                    "receipt_json",
                    "persisted_at",
                    "transfer_fee",
                    "total_fees",
                    "cost_spec_id",
                    "cost_spec_schema_version",
                    "cost_context_fingerprint",
                    "cost_provenance_state",
                },
            }
            for table, expected in required_columns.items():
                columns = {
                    str(row[1])
                    for row in connection.execute(f'PRAGMA table_info("{table}")').fetchall()
                }
                if not expected.issubset(columns):
                    raise PaperLifecycleIntegrityError(
                        f"paper broker {table} lacks PIT/provenance columns"
                    )
            account = connection.execute(
                """
                SELECT cost_spec_id, cost_spec_schema_version, cost_provenance_state
                FROM broker_account WHERE account_id = ?
                """,
                (self.account_id,),
            ).fetchone()
            if account is None:
                raise PaperLifecycleIntegrityError("paper broker account is unavailable")
            if (
                account["cost_provenance_state"] != PaperCostProvenanceState.KNOWN_V3.value
                or account["cost_spec_schema_version"] != 3
                or account["cost_spec_id"] is None
            ):
                raise PaperLifecycleIntegrityError(
                    "paper broker account is audit-only because cost provenance is unknown"
                )
            authority = connection.execute(
                """
                SELECT schema_version, canonical_json
                FROM paper_cost_spec WHERE cost_spec_id = ?
                """,
                (account["cost_spec_id"],),
            ).fetchone()
            if authority is None or authority["schema_version"] != 3:
                raise PaperLifecycleIntegrityError("paper broker v3 cost authority is unavailable")
            try:
                cost_spec = ExecutionCostSpec.from_canonical_json(authority["canonical_json"])
            except (TypeError, ValueError) as exc:
                raise PaperLifecycleIntegrityError(
                    "paper broker v3 cost authority is invalid"
                ) from exc
            if (
                not cost_spec.is_alignment_eligible
                or cost_spec.cost_spec_id != account["cost_spec_id"]
                or cost_spec.slippage is None
            ):
                raise PaperLifecycleIntegrityError(
                    "paper broker v3 cost authority does not bind account"
                )
            self._price_tick = cost_spec.slippage.price_tick
            legacy_cost_rows = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM paper_fill AS f
                     JOIN paper_order AS o ON o.order_id = f.order_id
                     WHERE o.account_id = ?
                       AND f.cost_provenance_state IS NOT 'KNOWN_V3')
                    +
                    (SELECT COUNT(*) FROM paper_execution_receipt
                     WHERE account_id = ? AND cost_provenance_state IS NOT 'KNOWN_V3')
                """,
                (self.account_id, self.account_id),
            ).fetchone()[0]
            if int(legacy_cost_rows) != 0:
                raise PaperLifecycleIntegrityError(
                    "paper broker account has unknown execution cost provenance"
                )

    def _validate_file(self) -> None:
        try:
            observed = self.path.lstat()
        except OSError as exc:
            raise PaperLifecycleIntegrityError("paper broker database is unavailable") from exc
        if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
            raise PaperLifecycleIntegrityError("paper broker database must be a regular file")

    def _connect(self) -> sqlite3.Connection:
        uri = f"file:{quote(str(self.path), safe='/')}?mode=ro"
        connection = sqlite3.connect(uri, uri=True, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def resolve(
        self,
        *,
        candidate_id: str,
        entry_signal: SignalEnvelope,
        exit_signals: tuple[SignalEnvelope, ...],
        decision_cutoff: datetime,
        market_features: Mapping[str, object],
        market_feature_statuses: Mapping[str, FeatureFieldStatus],
        previous_eligible_high_price_raw: float | None,
        previous_high_source_event_time: datetime | None,
        previous_high_available_at: datetime | None,
    ) -> FeatureInstanceEnvelope:
        cutoff = normalize_aware_utc(decision_cutoff)
        if entry_signal.candidate_id != candidate_id:
            raise PaperLifecycleIntegrityError("entry signal candidate does not match")
        if entry_signal.available_at > cutoff:
            raise PaperLifecycleIntegrityError("entry signal is not visible at decision cutoff")
        session_high = self._positive_number(market_features, "session_high")
        session_high_status = market_feature_statuses.get("session_high")
        if (
            session_high_status is None
            or session_high_status.name != "session_high"
            or session_high_status.candidate_id not in {None, candidate_id}
            or session_high_status.status is not FeatureAvailability.AVAILABLE
            or session_high_status.decision_cutoff > cutoff
            or session_high_status.available_at > cutoff
        ):
            raise PaperLifecycleIntegrityError(
                "session_high lacks candidate-scoped PIT availability evidence"
            )
        previous_evidence = (
            previous_eligible_high_price_raw,
            previous_high_source_event_time,
            previous_high_available_at,
        )
        if any(item is None for item in previous_evidence) and any(
            item is not None for item in previous_evidence
        ):
            raise PaperLifecycleIntegrityError(
                "previous eligible high evidence must be all present or all absent"
            )
        previous_high = None
        previous_event = previous_available = None
        if previous_eligible_high_price_raw is not None:
            if (
                isinstance(previous_eligible_high_price_raw, bool)
                or previous_eligible_high_price_raw <= 0
            ):
                raise PaperLifecycleIntegrityError("previous eligible high must be positive")
            assert previous_high_source_event_time is not None
            assert previous_high_available_at is not None
            previous_event = normalize_aware_utc(previous_high_source_event_time)
            previous_available = normalize_aware_utc(previous_high_available_at)
            if previous_event > previous_available or previous_available > cutoff:
                raise PaperLifecycleIntegrityError(
                    "previous eligible high is not visible at decision cutoff"
                )
            previous_high = float(previous_eligible_high_price_raw)
        with self._connect() as connection:
            entry = self._visible_entry_order(
                connection,
                candidate_id=candidate_id,
                signal_id=entry_signal.signal_id,
                decision_cutoff=cutoff,
            )
            if entry is None:
                return self._envelope(
                    candidate_id=candidate_id,
                    values={"entry_fill_status": "pending"},
                    source_event_time=entry_signal.event_time,
                    available_at=entry_signal.available_at,
                    decision_cutoff=cutoff,
                )
            order_updated_at = normalize_aware_utc(
                datetime.fromisoformat(str(entry.order_row["updated_at"]).replace("Z", "+00:00"))
            )
            visible_order = (
                self._paper_order(entry.order_row) if order_updated_at <= cutoff else None
            )
            if visible_order is not None and visible_order.status in {
                PaperOrderStatus.REJECTED,
                PaperOrderStatus.CANCELLED,
                PaperOrderStatus.EXPIRED,
            }:
                return self._envelope(
                    candidate_id=candidate_id,
                    values={"entry_fill_status": "rejected"},
                    source_event_time=visible_order.updated_at,
                    available_at=visible_order.updated_at,
                    decision_cutoff=cutoff,
                )
            position = self._rebuild_position(
                connection,
                entry=entry,
                candidate_id=candidate_id,
                decision_cutoff=cutoff,
                visible_order=visible_order,
            )
            if position is None:
                return self._envelope(
                    candidate_id=candidate_id,
                    values={"entry_fill_status": "pending"},
                    source_event_time=entry.intent_persisted_at,
                    available_at=entry.intent_persisted_at,
                    decision_cutoff=cutoff,
                )
            exit_execution = self._exit_execution_evidence(
                connection,
                entry_signal=entry_signal,
                exit_signals=exit_signals,
                decision_cutoff=cutoff,
                position=position,
            )

        entry_price = float(position.entry_price)
        structure_stop, structure_evidence = self._structure_stop(
            entry_signal,
            entry_price=entry_price,
        )
        remaining_fraction = position.remaining_quantity / position.entry_quantity
        if previous_high is not None and previous_high >= session_high:
            eligible_high = max(entry_price, previous_high)
            assert previous_event is not None and previous_available is not None
            eligible_high_evidence = (previous_event, previous_available)
        else:
            eligible_high = max(entry_price, session_high)
            eligible_high_evidence = (
                session_high_status.source_event_time,
                session_high_status.available_at,
            )
        values: dict[str, object] = {
            "entry_fill_status": "filled",
            "exit_execution_status": exit_execution.status,
            "position_closed": position.remaining_quantity == 0,
            "holding_trading_sessions": int(position.available_quantity > 0),
            "position_sellable": position.available_quantity > 0,
            "entry_price_raw": entry_price,
            "structure_stop_price_raw": structure_stop,
            "eligible_high_price_raw": eligible_high,
            "remaining_position_fraction": remaining_fraction,
        }
        entry_evidence = (position.entry_event_time, position.entry_available_at)
        position_evidence = (
            position.position_event_time,
            position.position_available_at,
        )
        field_evidence = {
            "entry_fill_status": entry_evidence,
            "exit_execution_status": (
                exit_execution.source_event_time,
                exit_execution.available_at,
            ),
            "position_closed": position_evidence,
            "entry_price_raw": entry_evidence,
            "holding_trading_sessions": position_evidence,
            "position_sellable": position_evidence,
            "remaining_position_fraction": position_evidence,
            "eligible_high_price_raw": eligible_high_evidence,
        }
        if structure_evidence is not None:
            field_evidence["structure_stop_price_raw"] = structure_evidence
        else:
            field_evidence["structure_stop_price_raw"] = entry_evidence
        return self._envelope(
            candidate_id=candidate_id,
            values=values,
            source_event_time=position.position_event_time,
            available_at=position.position_available_at,
            decision_cutoff=cutoff,
            field_evidence=field_evidence,
        )

    def _visible_entry_order(
        self,
        connection: sqlite3.Connection,
        *,
        candidate_id: str,
        signal_id: str,
        decision_cutoff: datetime,
    ) -> _VisibleEntry | None:
        rows = connection.execute(
            """
            SELECT i.payload_json, i.persisted_at AS intent_persisted_at,
                   i.signal_id AS intent_signal_id,
                   i.entry_signal_id AS intent_entry_signal_id,
                   i.ts_code AS intent_ts_code,
                   i.side AS intent_side,
                   o.*
            FROM paper_intent AS i
            JOIN paper_order AS o ON o.intent_id = i.intent_id
            WHERE i.account_id = ? AND o.ts_code = ?
              AND i.signal_id = ? AND i.side = 'BUY'
              AND i.persisted_at <= ? AND o.created_at <= ?
            ORDER BY i.persisted_at DESC, o.order_id DESC
            """,
            (
                self.account_id,
                candidate_id,
                signal_id,
                _utc_iso(decision_cutoff),
                _utc_iso(decision_cutoff),
            ),
        ).fetchall()
        matching: list[_VisibleEntry] = []
        for row in rows:
            try:
                intent = PaperOrderIntent.model_validate(json.loads(row["payload_json"]))
            except (TypeError, ValueError) as exc:
                raise PaperLifecycleIntegrityError(
                    "paper broker intent payload is invalid"
                ) from exc
            if intent.signal_id != signal_id or intent.side is not PaperSide.BUY:
                continue
            if (
                intent.account_id != self.account_id
                or intent.ts_code != candidate_id
                or intent.intent_id != row["intent_id"]
                or row["intent_signal_id"] != intent.signal_id
                or row["intent_entry_signal_id"] != intent.entry_signal_id
                or row["intent_ts_code"] != intent.ts_code
                or row["intent_side"] != intent.side.value
                or row["account_id"] != self.account_id
                or row["side"] != PaperSide.BUY.value
            ):
                raise PaperLifecycleIntegrityError(
                    "paper broker entry intent and order do not reconcile"
                )
            persisted_at = normalize_aware_utc(
                datetime.fromisoformat(str(row["intent_persisted_at"]).replace("Z", "+00:00"))
            )
            if persisted_at < intent.available_at:
                raise PaperLifecycleIntegrityError(
                    "paper broker intent persistence precedes availability"
                )
            matching.append(
                _VisibleEntry(
                    intent=intent,
                    order_row=row,
                    intent_persisted_at=persisted_at,
                )
            )
        if len(matching) > 1:
            raise PaperLifecycleIntegrityError(
                "paper broker entry signal resolves to multiple BUY orders"
            )
        return matching[0] if matching else None

    @staticmethod
    def _paper_order(row: sqlite3.Row) -> PaperOrder:
        try:
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
        except (TypeError, ValueError) as exc:
            raise PaperLifecycleIntegrityError("paper broker order does not reconcile") from exc

    @staticmethod
    def _paper_fill(row: sqlite3.Row) -> PaperFill:
        try:
            fill = PaperFill(
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
        except (TypeError, ValueError) as exc:
            raise PaperLifecycleIntegrityError("paper broker fill is invalid") from exc
        if fill.cost_provenance_state is not PaperCostProvenanceState.KNOWN_V3:
            raise PaperLifecycleIntegrityError("paper broker fill has unknown cost provenance")
        return fill

    def _rebuild_position(
        self,
        connection: sqlite3.Connection,
        *,
        entry: _VisibleEntry,
        candidate_id: str,
        decision_cutoff: datetime,
        visible_order: PaperOrder | None,
    ) -> _RebuiltPosition | None:
        fill_rows = connection.execute(
            """
            SELECT * FROM paper_fill
            WHERE order_id = ? AND executed_at <= ?
            ORDER BY sequence, fill_id
            """,
            (entry.order_row["order_id"], _utc_iso(decision_cutoff)),
        ).fetchall()
        fills: list[PaperFill] = []
        fill_persisted_at: dict[str, datetime] = {}
        for row in fill_rows:
            if row["persisted_at"] is None:
                raise PaperLifecycleIntegrityError("paper broker BUY fill availability is unknown")
            persisted_at = normalize_aware_utc(
                datetime.fromisoformat(str(row["persisted_at"]).replace("Z", "+00:00"))
            )
            executed_at = normalize_aware_utc(
                datetime.fromisoformat(str(row["executed_at"]).replace("Z", "+00:00"))
            )
            if persisted_at < executed_at:
                raise PaperLifecycleIntegrityError(
                    "paper broker BUY fill persisted_at precedes execution"
                )
            if persisted_at > decision_cutoff:
                continue
            fill = self._paper_fill(row)
            fills.append(fill)
            fill_persisted_at[str(fill.fill_id)] = persisted_at
        if tuple(fill.sequence for fill in fills) != tuple(range(1, len(fills) + 1)):
            raise PaperLifecycleIntegrityError("paper broker BUY fill sequence is incomplete")
        filled_quantity = sum(fill.quantity for fill in fills)
        if filled_quantity > entry.intent.quantity:
            raise PaperLifecycleIntegrityError("paper broker BUY fills exceed order quantity")
        if visible_order is not None:
            if visible_order.filled_quantity != filled_quantity:
                raise PaperLifecycleIntegrityError(
                    "paper broker order and BUY fill quantity do not reconcile"
                )
            if filled_quantity:
                weighted_price = (
                    sum(
                        (fill.price * fill.quantity for fill in fills),
                        Decimal("0"),
                    )
                    / filled_quantity
                ).quantize(self._price_tick, rounding=ROUND_HALF_UP)
                if visible_order.average_fill_price != weighted_price:
                    raise PaperLifecycleIntegrityError(
                        "paper broker order and BUY fill price do not reconcile"
                    )
        if not fills:
            if visible_order is not None and visible_order.filled_quantity:
                raise PaperLifecycleIntegrityError("paper broker order claims a missing BUY fill")
            return None

        fill_ids = tuple(str(fill.fill_id) for fill in fills)
        placeholders = ",".join("?" for _ in fill_ids)
        lot_rows = connection.execute(
            f"""
            SELECT * FROM paper_lot
            WHERE lot_id IN ({placeholders})
            ORDER BY lot_id
            """,
            fill_ids,
        ).fetchall()
        lots_by_id = {str(row["lot_id"]): row for row in lot_rows}
        fills_by_id = {str(fill.fill_id): fill for fill in fills}
        if set(lots_by_id) != set(fill_ids):
            raise PaperLifecycleIntegrityError("paper broker BUY fill is missing its source lot")
        for lot_id, lot in lots_by_id.items():
            if (
                lot["persisted_at"] is None
                or lot["entry_signal_id"] is None
                or lot["buy_executed_at"] is None
                or lot["buy_persisted_at"] is None
                or lot["buy_fill_sequence"] is None
            ):
                raise PaperLifecycleIntegrityError(
                    "paper broker source lot availability or provenance is unknown"
                )
            lot_persisted_at = normalize_aware_utc(
                datetime.fromisoformat(str(lot["persisted_at"]).replace("Z", "+00:00"))
            )
            buy_executed_at = normalize_aware_utc(
                datetime.fromisoformat(str(lot["buy_executed_at"]).replace("Z", "+00:00"))
            )
            buy_persisted_at = normalize_aware_utc(
                datetime.fromisoformat(str(lot["buy_persisted_at"]).replace("Z", "+00:00"))
            )
            fill = fills_by_id[lot_id]
            if (
                lot_persisted_at != fill_persisted_at[lot_id]
                or buy_executed_at != fill.executed_at
                or buy_persisted_at != fill_persisted_at[lot_id]
                or int(lot["buy_fill_sequence"]) != fill.sequence
                or lot_persisted_at > decision_cutoff
                or lot["entry_signal_id"] != entry.intent.signal_id
            ):
                raise PaperLifecycleIntegrityError(
                    "paper broker BUY fill and source lot PIT provenance do not reconcile"
                )

        consumed_by_lot = dict.fromkeys(fill_ids, 0)
        consumption_rows = connection.execute(
            f"""
            SELECT c.lot_id, c.quantity, c.persisted_at AS consumption_persisted_at,
                   sf.executed_at, sf.persisted_at AS sell_fill_persisted_at,
                   so.account_id, so.ts_code, so.side, so.entry_signal_id,
                   si.payload_json AS sell_intent_payload
            FROM paper_lot_consumption AS c
            JOIN paper_fill AS sf ON sf.fill_id = c.fill_id
            JOIN paper_order AS so ON so.order_id = sf.order_id
            JOIN paper_intent AS si ON si.intent_id = so.intent_id
            WHERE c.lot_id IN ({placeholders}) AND sf.executed_at <= ?
            ORDER BY sf.executed_at, c.fill_id, c.lot_id
            """,
            (*fill_ids, _utc_iso(decision_cutoff)),
        ).fetchall()
        entry_event = max(fill.executed_at for fill in fills)
        latest_event = entry_event
        latest_available = max(fill_persisted_at.values())
        for consumption in consumption_rows:
            if (
                consumption["consumption_persisted_at"] is None
                or consumption["sell_fill_persisted_at"] is None
            ):
                raise PaperLifecycleIntegrityError("paper broker SELL fill availability is unknown")
            consumption_available = normalize_aware_utc(
                datetime.fromisoformat(
                    str(consumption["consumption_persisted_at"]).replace("Z", "+00:00")
                )
            )
            sell_fill_available = normalize_aware_utc(
                datetime.fromisoformat(
                    str(consumption["sell_fill_persisted_at"]).replace("Z", "+00:00")
                )
            )
            sell_event = normalize_aware_utc(
                datetime.fromisoformat(str(consumption["executed_at"]).replace("Z", "+00:00"))
            )
            if consumption_available != sell_fill_available or consumption_available < sell_event:
                raise PaperLifecycleIntegrityError(
                    "paper broker SELL fill and consumption availability do not reconcile"
                )
            if consumption_available > decision_cutoff:
                continue
            try:
                sell_intent = PaperOrderIntent.model_validate(
                    json.loads(consumption["sell_intent_payload"])
                )
            except (TypeError, ValueError) as exc:
                raise PaperLifecycleIntegrityError(
                    "paper broker SELL intent provenance is invalid"
                ) from exc
            if (
                consumption["account_id"] != self.account_id
                or consumption["ts_code"] != candidate_id
                or consumption["side"] != PaperSide.SELL.value
                or consumption["entry_signal_id"] != entry.intent.signal_id
                or sell_intent.entry_signal_id != entry.intent.signal_id
            ):
                raise PaperLifecycleIntegrityError(
                    "paper broker lot consumption provenance is invalid"
                )
            lot_id = str(consumption["lot_id"])
            quantity = int(consumption["quantity"])
            if quantity <= 0 or quantity % 100:
                raise PaperLifecycleIntegrityError(
                    "paper broker lot consumption quantity is invalid"
                )
            consumed_by_lot[lot_id] += quantity
            latest_event = max(latest_event, sell_event)
            latest_available = max(latest_available, consumption_available)

        trade_date = decision_cutoff.astimezone(_SHANGHAI).date()
        remaining_quantity = 0
        available_quantity = 0
        for fill in fills:
            fill_id = str(fill.fill_id)
            lot = lots_by_id[fill_id]
            if fill.total_fees is None:
                raise PaperLifecycleIntegrityError("paper broker BUY fill has unknown total fees")
            expected_unit_cost = (fill.notional + fill.total_fees) / fill.quantity
            if (
                lot["account_id"] != self.account_id
                or lot["ts_code"] != candidate_id
                or int(lot["original_quantity"]) != fill.quantity
                or Decimal(str(lot["unit_cost"])) != expected_unit_cost
            ):
                raise PaperLifecycleIntegrityError(
                    "paper broker BUY fill and source lot do not reconcile"
                )
            acquisition_date = date.fromisoformat(str(lot["acquisition_trade_date"]))
            available_date = date.fromisoformat(str(lot["available_date"]))
            if acquisition_date != fill.executed_at.astimezone(_SHANGHAI).date():
                raise PaperLifecycleIntegrityError(
                    "paper broker BUY fill and lot acquisition date do not reconcile"
                )
            if available_date <= acquisition_date:
                raise PaperLifecycleIntegrityError("paper broker lot violates T+1 availability")
            remaining = fill.quantity - consumed_by_lot[fill_id]
            if remaining < 0:
                raise PaperLifecycleIntegrityError(
                    "paper broker lot consumption exceeds BUY fill quantity"
                )
            remaining_quantity += remaining
            if available_date <= trade_date:
                available_quantity += remaining

        entry_price = (
            sum(
                (fill.price * fill.quantity for fill in fills),
                Decimal("0"),
            )
            / filled_quantity
        ).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        entry_available = max(entry.intent_persisted_at, *fill_persisted_at.values())
        latest_available = max(entry_available, latest_available)
        return _RebuiltPosition(
            entry_quantity=filled_quantity,
            remaining_quantity=remaining_quantity,
            available_quantity=available_quantity,
            entry_price=entry_price,
            entry_event_time=entry_event,
            entry_available_at=entry_available,
            position_event_time=latest_event,
            position_available_at=latest_available,
        )

    def _exit_execution_evidence(
        self,
        connection: sqlite3.Connection,
        *,
        entry_signal: SignalEnvelope,
        exit_signals: tuple[SignalEnvelope, ...],
        decision_cutoff: datetime,
        position: _RebuiltPosition,
    ) -> _ExitExecutionEvidence:
        if not isinstance(exit_signals, tuple):
            raise TypeError("exit_signals must be a tuple")
        visible: list[SignalEnvelope] = []
        seen_ids: set[str] = set()
        for signal in exit_signals:
            if not isinstance(signal, SignalEnvelope):
                raise TypeError("exit_signals must contain SignalEnvelope values")
            if signal.signal_id in seen_ids:
                raise PaperLifecycleIntegrityError("exit signal evidence is duplicated")
            seen_ids.add(str(signal.signal_id))
            if (
                signal.candidate_id != entry_signal.candidate_id
                or signal.strategy_id != entry_signal.strategy_id
                or signal.strategy_version != entry_signal.strategy_version
                or signal.action not in {SignalAction.REDUCE, SignalAction.S_INTENT}
                or signal.evidence.get("entry_signal_id") != entry_signal.signal_id
            ):
                raise PaperLifecycleIntegrityError(
                    "exit signal does not bind the exact entry lifecycle"
                )
            if signal.available_at <= decision_cutoff:
                visible.append(signal)
        if not visible:
            if position.remaining_quantity == 0:
                raise PaperLifecycleIntegrityError(
                    "closed paper position lacks a visible exit signal"
                )
            return _ExitExecutionEvidence(
                status="none",
                source_event_time=position.position_event_time,
                available_at=position.position_available_at,
            )
        visible.sort(key=lambda item: (item.available_at, item.event_time, str(item.signal_id)))
        evidence_by_signal = {
            str(signal.signal_id): self._one_exit_execution_evidence(
                connection,
                entry_signal=entry_signal,
                signal=signal,
                decision_cutoff=decision_cutoff,
            )
            for signal in visible
        }
        signal = visible[-1]
        result = evidence_by_signal[str(signal.signal_id)]
        if position.remaining_quantity == 0 and result.status != "filled":
            raise PaperLifecycleIntegrityError(
                "closed paper position lacks a verified visible SELL fill"
            )
        return result

    def _one_exit_execution_evidence(
        self,
        connection: sqlite3.Connection,
        *,
        entry_signal: SignalEnvelope,
        signal: SignalEnvelope,
        decision_cutoff: datetime,
    ) -> _ExitExecutionEvidence:
        rows = connection.execute(
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
              AND i.signal_id = ? AND i.side = 'SELL'
            ORDER BY i.persisted_at DESC, o.order_id DESC
            """,
            (self.account_id, signal.candidate_id, signal.signal_id),
        ).fetchall()
        if len(rows) > 1:
            raise PaperLifecycleIntegrityError("exit signal resolves to multiple paper orders")
        if not rows:
            status = "pending" if decision_cutoff < signal.expires_at else "retryable"
            return _ExitExecutionEvidence(
                status=status,
                source_event_time=signal.event_time,
                available_at=signal.available_at,
            )
        row = rows[0]
        persisted_at = self._required_timestamp(
            row["intent_persisted_at"],
            label="exit intent persisted_at",
        )
        if persisted_at < signal.available_at:
            raise PaperLifecycleIntegrityError(
                "exit intent persistence precedes signal availability"
            )
        if persisted_at > decision_cutoff:
            status = "pending" if decision_cutoff < signal.expires_at else "retryable"
            return _ExitExecutionEvidence(
                status=status,
                source_event_time=signal.event_time,
                available_at=signal.available_at,
            )
        try:
            intent = PaperOrderIntent.model_validate(json.loads(row["payload_json"]))
            order = self._paper_order(row)
        except (TypeError, ValueError) as exc:
            raise PaperLifecycleIntegrityError("exit order evidence is invalid") from exc
        if (
            intent.signal_id != signal.signal_id
            or intent.entry_signal_id != entry_signal.signal_id
            or intent.side is not PaperSide.SELL
            or intent.account_id != self.account_id
            or intent.ts_code != entry_signal.candidate_id
            or row["entry_signal_id"] != entry_signal.signal_id
            or row["intent_signal_id"] != intent.signal_id
            or row["intent_entry_signal_id"] != intent.entry_signal_id
            or row["intent_ts_code"] != intent.ts_code
            or row["intent_side"] != intent.side.value
            or order.intent_id != intent.intent_id
            or intent.sell_quantity_authority is None
            or intent.sell_quantity_authority.exit_signal_id != signal.signal_id
            or intent.sell_quantity_authority.entry_signal_id != entry_signal.signal_id
            or intent.sell_quantity_authority.account_id != self.account_id
            or intent.sell_quantity_authority.ts_code != entry_signal.candidate_id
            or intent.sell_quantity_authority.action != signal.action.name
            or intent.sell_quantity_authority.requested_quantity != intent.quantity
        ):
            raise PaperLifecycleIntegrityError(
                "exit order does not bind the exact entry fill provenance"
            )
        authority = intent.sell_quantity_authority
        expected_fraction = signal.evidence.get("sell_tranche_fraction")
        if Decimal(str(expected_fraction)) != authority.tranche_fraction:
            raise PaperLifecycleIntegrityError(
                "exit signal and SELL quantity authority tranche mismatch"
            )

        fill_rows = connection.execute(
            """
            SELECT * FROM paper_fill
            WHERE order_id = ? AND executed_at <= ?
            ORDER BY sequence, fill_id
            """,
            (order.order_id, _utc_iso(decision_cutoff)),
        ).fetchall()
        fills: list[tuple[PaperFill, datetime]] = []
        for fill_row in fill_rows:
            fill_available = self._required_timestamp(
                fill_row["persisted_at"],
                label=f"SELL fill {fill_row['fill_id']} persisted_at",
            )
            fill_event = self._required_timestamp(
                fill_row["executed_at"],
                label=f"SELL fill {fill_row['fill_id']} executed_at",
            )
            if fill_available < fill_event:
                raise PaperLifecycleIntegrityError("SELL fill availability precedes execution")
            if fill_available > decision_cutoff:
                continue
            fill = self._paper_fill(fill_row)
            fills.append((fill, fill_available))
        if tuple(fill.sequence for fill, _ in fills) != tuple(range(1, len(fills) + 1)):
            raise PaperLifecycleIntegrityError("SELL fill sequence is incomplete")

        for fill, fill_available in fills:
            allocations = connection.execute(
                """
                SELECT c.quantity, c.unit_cost,
                       c.persisted_at AS consumption_persisted_at,
                       l.entry_signal_id, l.account_id, l.ts_code,
                       l.unit_cost AS lot_unit_cost
                FROM paper_lot_consumption AS c
                JOIN paper_lot AS l ON l.lot_id = c.lot_id
                WHERE c.fill_id = ?
                ORDER BY c.lot_id
                """,
                (fill.fill_id,),
            ).fetchall()
            allocated = 0
            for allocation in allocations:
                consumption_available = self._required_timestamp(
                    allocation["consumption_persisted_at"],
                    label=f"SELL fill {fill.fill_id} consumption persisted_at",
                )
                if (
                    consumption_available != fill_available
                    or allocation["entry_signal_id"] != entry_signal.signal_id
                    or allocation["account_id"] != self.account_id
                    or allocation["ts_code"] != entry_signal.candidate_id
                    or Decimal(str(allocation["unit_cost"]))
                    != Decimal(str(allocation["lot_unit_cost"]))
                ):
                    raise PaperLifecycleIntegrityError(
                        "SELL fill consumption PIT provenance does not reconcile"
                    )
                allocated += int(allocation["quantity"])
            if allocated != fill.quantity:
                raise PaperLifecycleIntegrityError(
                    "SELL fill quantity does not match lot consumption"
                )

        filled_quantity = sum(fill.quantity for fill, _ in fills)
        if filled_quantity > intent.quantity:
            raise PaperLifecycleIntegrityError("SELL fills exceed intent quantity")
        order_visible_at = self._required_timestamp(
            row["updated_at"],
            label=f"SELL order {order.order_id} updated_at",
        )
        if order_visible_at <= decision_cutoff:
            if order.filled_quantity != filled_quantity:
                raise PaperLifecycleIntegrityError(
                    "SELL order and visible fill quantity do not reconcile"
                )
            if fills:
                average = (
                    sum(
                        (fill.price * fill.quantity for fill, _ in fills),
                        Decimal("0"),
                    )
                    / filled_quantity
                ).quantize(self._price_tick, rounding=ROUND_HALF_UP)
                if order.average_fill_price != average:
                    raise PaperLifecycleIntegrityError("SELL order and fill price do not reconcile")
            elif order.average_fill_price is not None:
                raise PaperLifecycleIntegrityError(
                    "SELL order has average price without a visible fill"
                )
            self._validate_exit_order_status(order, filled_quantity=filled_quantity)

        if order_visible_at <= decision_cutoff and order.status in {
            PaperOrderStatus.REJECTED,
            PaperOrderStatus.CANCELLED,
            PaperOrderStatus.EXPIRED,
        }:
            status = "retryable"
        elif filled_quantity == intent.quantity:
            status = "filled"
        else:
            status = "pending" if decision_cutoff < intent.expires_at else "retryable"
        if fills:
            latest_fill, latest_fill_available = max(
                fills,
                key=lambda item: (item[0].executed_at, item[0].sequence),
            )
            source_event_time = latest_fill.executed_at
            persisted_at = max(persisted_at, latest_fill_available)
        else:
            source_event_time = order.created_at
        if source_event_time > decision_cutoff or persisted_at > decision_cutoff:
            raise PaperLifecycleIntegrityError(
                "exit execution evidence is not visible at decision cutoff"
            )
        return _ExitExecutionEvidence(
            status=status,
            source_event_time=source_event_time,
            available_at=persisted_at,
        )

    @staticmethod
    def _validate_exit_order_status(
        order: PaperOrder,
        *,
        filled_quantity: int,
    ) -> None:
        if (
            order.status
            in {
                PaperOrderStatus.PENDING,
                PaperOrderStatus.ACCEPTED,
                PaperOrderStatus.REJECTED,
            }
            and filled_quantity != 0
        ):
            raise PaperLifecycleIntegrityError("unfilled SELL order has visible fills")
        if order.status is PaperOrderStatus.PARTIALLY_FILLED and not (
            0 < filled_quantity < order.quantity
        ):
            raise PaperLifecycleIntegrityError(
                "PARTIALLY_FILLED SELL order quantity does not reconcile"
            )
        if order.status is PaperOrderStatus.FILLED and filled_quantity != order.quantity:
            raise PaperLifecycleIntegrityError("FILLED SELL order quantity does not reconcile")
        if order.status in {
            PaperOrderStatus.CANCELLED,
            PaperOrderStatus.EXPIRED,
        } and not (0 <= filled_quantity < order.quantity):
            raise PaperLifecycleIntegrityError("closed SELL order lacks an unfilled remainder")

    @staticmethod
    def _required_timestamp(value: object, *, label: str) -> datetime:
        if value is None:
            raise PaperLifecycleIntegrityError(f"{label} is unknown")
        try:
            return normalize_aware_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
        except ValueError as exc:
            raise PaperLifecycleIntegrityError(f"{label} is invalid") from exc

    def _envelope(
        self,
        *,
        candidate_id: str,
        values: Mapping[str, object],
        source_event_time: datetime,
        available_at: datetime,
        decision_cutoff: datetime,
        field_evidence: Mapping[str, tuple[datetime, datetime]] | None = None,
    ) -> FeatureInstanceEnvelope:
        from rquant.strategy_evaluators import project_execution_lifecycle_features

        event = normalize_aware_utc(source_event_time)
        available = normalize_aware_utc(available_at)
        cutoff = normalize_aware_utc(decision_cutoff)
        projected = project_execution_lifecycle_features(values)
        evidence = field_evidence or {}
        statuses = tuple(
            FeatureFieldStatus(
                candidate_id=candidate_id,
                name=name,
                status=FeatureAvailability.AVAILABLE,
                source_event_time=normalize_aware_utc(evidence.get(name, (event, available))[0]),
                available_at=normalize_aware_utc(evidence.get(name, (event, available))[1]),
                decision_cutoff=cutoff,
                actual_delay_seconds=(
                    normalize_aware_utc(evidence.get(name, (event, available))[1])
                    - normalize_aware_utc(evidence.get(name, (event, available))[0])
                ).total_seconds(),
            )
            for name in sorted(projected)
        )
        return FeatureInstanceEnvelope(
            source_id=f"paper-broker:{self.account_id}",
            decision_cutoff=cutoff,
            values=projected,  # type: ignore[arg-type]
            field_statuses=statuses,
        )

    @staticmethod
    def _positive_number(features: Mapping[str, object], name: str) -> float:
        value = features.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise PaperLifecycleIntegrityError(f"market feature {name} must be numeric")
        normalized = float(value)
        if normalized <= 0:
            raise PaperLifecycleIntegrityError(f"market feature {name} must be positive")
        return normalized

    @staticmethod
    def _structure_stop(
        entry_signal: SignalEnvelope,
        *,
        entry_price: float,
    ) -> tuple[float, tuple[datetime, datetime] | None]:
        evidence = entry_signal.evidence
        for name in ("session_low", "opening_bar_low", "auction_price_raw"):
            value = evidence.get(name)
            if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0:
                return float(value), (entry_signal.event_time, entry_signal.available_at)
        return entry_price * 0.97, None


__all__ = ["PaperBrokerLifecycleReader", "PaperLifecycleIntegrityError"]
