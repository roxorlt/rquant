"""Condition rule heads, scopes and send admission on original authorities."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal, Self
from weakref import WeakKeyDictionary

from duckdb import Error as DuckDBError
from pydantic import Field, StrictInt, model_validator

from rquant.alert_rule_contracts import (
    ConditionAlertRuleDefinition,
    ConditionAlertScopeEvidence,
    OwnedConditionAlertRule,
)
from rquant.condition_alert_route import (
    ConditionAlertBusEventRecord,
    ConditionAlertBusRoutedRecord,
    ConditionAlertRecipientPolicy,
    notification_record,
)
from rquant.condition_alert_rule_store import ConditionAlertRuleEntry
from rquant.condition_alert_runtime_contracts import (
    ConditionAlertRuntimeActivation,
    ConditionRuntimeModel,
    require_condition_alert_activation,
    require_verified_condition_activation,
)
from rquant.delivery_contracts import DeliveryTarget, OutboxRecord, OutboxStatus
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256, normalize_aware_utc
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.signal_bus import _encode_time, _require_time

if TYPE_CHECKING:
    from rquant.condition_alert_runtime import (
        ConditionProducerRuntimeSnapshot,
        ConditionRuntimeRuleFact,
    )
    from rquant.notification_state import NotificationStateStore
    from rquant.web.models.condition_alert_rules import ConditionAlertTriggerItem
    from rquant.web.serving import BorrowedGeneration


class ConditionRuleAuthoritySnapshot(ConditionRuntimeModel):
    protocol: Literal["condition-alert-rule-authority/v1"] = "condition-alert-rule-authority/v1"
    activated_at: AwareUtcDatetime
    rows: tuple[ConditionAlertRuleEntry, ...] = Field(max_length=10000)
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def create(
        cls, *, activated_at: datetime, rows: tuple[ConditionAlertRuleEntry, ...]
    ) -> ConditionRuleAuthoritySnapshot:
        ordered = tuple(sorted(rows, key=lambda row: (row.owner_id, row.rule_id)))
        return cls(activated_at=activated_at, rows=ordered, rows_sha256=canonical_sha256(ordered))

    @model_validator(mode="after")
    def complete_heads(self) -> Self:
        keys = tuple((row.owner_id, row.rule_id) for row in self.rows)
        if keys != tuple(sorted(set(keys))) or self.rows_sha256 != canonical_sha256(self.rows):
            raise ValueError("condition rule authority head or digest differs")
        counts: dict[str, int] = {}
        for row in self.rows:
            if not row.deleted:
                counts[row.owner_id] = counts.get(row.owner_id, 0) + 1
        if (
            any(count > 100 for count in counts.values())
            or len(self.wire_bytes()) > 8 * 1024 * 1024
        ):
            raise ValueError("condition rule full authority exceeds its owner capacity")
        return self


class ConditionRuleAuthorityHead(ConditionRuntimeModel):
    protocol: Literal["condition-alert-rule-authority/v1"] = "condition-alert-rule-authority/v1"
    activated_at: AwareUtcDatetime
    row_count: StrictInt = Field(ge=0, le=10000)
    rows_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


def condition_rule_projections(
    snapshot: ConditionRuleAuthoritySnapshot | None,
    *,
    observed_at: datetime,
    unavailable: bool = False,
) -> tuple[ServingProjectionPayload, ...]:
    now = normalize_aware_utc(observed_at)
    if snapshot is not None and (
        snapshot.activated_at > now or any(row.updated_at > now for row in snapshot.rows)
    ):
        raise ValueError("condition authority is not yet visible")
    state = "unavailable" if unavailable else "not_activated" if snapshot is None else "ready"
    head = (
        None
        if snapshot is None
        else ConditionRuleAuthorityHead(
            activated_at=snapshot.activated_at,
            row_count=len(snapshot.rows),
            rows_sha256=snapshot.rows_sha256,
        )
    )
    return (
        ServingProjectionPayload(
            table_name="condition_alert_rule_state",
            rows=(
                {
                    "snapshot_key": "current",
                    "state": state,
                    "body_json": None if head is None else head.wire_bytes().decode(),
                },
            ),
            available_at=now,
        ),
        ServingProjectionPayload(
            table_name="condition_alert_rule",
            rows=()
            if snapshot is None
            else tuple(
                {
                    "owner_id": row.owner_id,
                    "rule_id": row.rule_id,
                    "version": row.version,
                    "body_json": row.model_dump_json(),
                }
                for row in snapshot.rows
            ),
            available_at=now,
        ),
    )


def validate_condition_rule_projections(
    projections: Mapping[str, ServingProjectionPayload],
) -> None:
    state, heads = (
        projections.get("condition_alert_rule_state"),
        projections.get("condition_alert_rule"),
    )
    if state is None and heads is None:
        return
    if (
        state is None
        or heads is None
        or len(state.rows) != 1
        or state.rows[0]["snapshot_key"] != "current"
    ):
        raise ValueError("condition rule projection is incomplete")
    row = state.rows[0]
    if row["state"] != "ready":
        if (
            row["state"] not in {"unavailable", "not_activated"}
            or row["body_json"] is not None
            or heads.rows
        ):
            raise ValueError("unknown condition rule authority carries live heads")
        return
    head = ConditionRuleAuthorityHead.model_validate_json(row["body_json"])
    entries = tuple(
        ConditionAlertRuleEntry.model_validate_json(item["body_json"]) for item in heads.rows
    )
    snapshot = ConditionRuleAuthoritySnapshot.create(activated_at=head.activated_at, rows=entries)
    if (
        head.row_count != len(entries)
        or head.rows_sha256 != snapshot.rows_sha256
        or any(
            (item["owner_id"], item["rule_id"], item["version"])
            != (entry.owner_id, entry.rule_id, entry.version)
            for item, entry in zip(heads.rows, entries, strict=True)
        )
    ):
        raise ValueError("condition rule heads differ from their complete authority")


def _condition_table_row_count(borrowed: BorrowedGeneration, table: str, *, now: datetime) -> int:
    if table not in PAGE_PROJECTION_CONTRACTS:
        raise ValueError("unknown condition source projection")
    contract = PAGE_PROJECTION_CONTRACTS[table]
    if borrowed.fallback_detail is not None or borrowed.manifest.built_at > now:
        raise ValueError("condition source generation is not current or visible")
    row = borrowed.cursor.execute(
        "SELECT available,row_count,owner_dataset_id,owner_generation_id,"
        "available_at FROM projection_status WHERE table_name=?",
        [table],
    ).fetchone()
    expected = borrowed.manifest.source_generations.get(contract.owner_dataset_id)
    if (
        row is None
        or row[0] is not True
        or type(row[1]) is not int
        or row[1] != borrowed.manifest.row_counts.get(table)
        or row[2] != contract.owner_dataset_id
        or row[3] != expected
        or expected is None
        or row[4] is None
        or row[4] > borrowed.manifest.built_at
        or row[4] > now
    ):
        raise ValueError(
            "condition source projection is incomplete or belongs to another generation"
        )
    return row[1]


def condition_table_rows(
    borrowed: BorrowedGeneration, table: str, *, now: datetime
) -> tuple[dict[str, object], ...]:
    expected_count = _condition_table_row_count(borrowed, table, now=now)
    contract = PAGE_PROJECTION_CONTRACTS[table]
    rows = borrowed.cursor.execute(
        f"SELECT {','.join(contract.column_names)} FROM {table} "
        f"ORDER BY {','.join(contract.sort_keys)} LIMIT ?",
        [contract.max_rows + 1],
    ).fetchall()
    if len(rows) > contract.max_rows or len(rows) != expected_count:
        raise ValueError("condition source full row count differs")
    return tuple(dict(zip(contract.column_names, item, strict=True)) for item in rows)


def read_condition_rule_authority(
    borrowed: BorrowedGeneration, *, now: datetime
) -> ConditionRuleAuthoritySnapshot | None:
    states = condition_table_rows(borrowed, "condition_alert_rule_state", now=now)
    if len(states) != 1 or states[0]["snapshot_key"] != "current":
        raise ValueError("condition rule state is incomplete")
    if states[0]["state"] == "not_activated":
        return None
    if states[0]["state"] != "ready":
        raise ValueError("condition rule state is unavailable")
    head = ConditionRuleAuthorityHead.model_validate_json(states[0]["body_json"])
    rows = condition_table_rows(borrowed, "condition_alert_rule", now=now)
    entries = tuple(ConditionAlertRuleEntry.model_validate_json(row["body_json"]) for row in rows)
    snapshot = ConditionRuleAuthoritySnapshot.create(activated_at=head.activated_at, rows=entries)
    if head.rows_sha256 != snapshot.rows_sha256 or head.row_count != len(entries):
        raise ValueError("condition rule head authority changed")
    return snapshot


def resolve_condition_scope(
    borrowed: BorrowedGeneration,
    *,
    owner_id: str,
    rule: ConditionAlertRuleDefinition,
    now: datetime,
) -> ConditionAlertScopeEvidence:
    from rquant.serving_manual_watchlist_projection import ManualWatchlistProjectionRow
    from rquant.web.screen_intraday import read_intraday_screen_snapshot

    scope = rule.scope
    inspected = normalize_aware_utc(now)
    if scope.kind == "market":
        snapshot = read_intraday_screen_snapshot(borrowed, now=inspected)
        codes = snapshot.source.universe_codes
        version = canonical_sha256(
            {
                "trade_date": snapshot.source.trade_date,
                "universe_source": snapshot.source.universe_source_id,
                "universe_digest": snapshot.source.universe_digest,
            }
        )
        available_at = snapshot.source.cutoff
    elif scope.kind == "watchlist":
        rows = condition_table_rows(borrowed, "manual_watchlist", now=inspected)
        selected = tuple(
            ManualWatchlistProjectionRow.model_validate(row)
            for row in rows
            if row["owner_id"] == owner_id
        )
        version = canonical_sha256({"owner": owner_id, "heads": selected})
        if version != scope.membership_version:
            raise ValueError("condition watchlist membership changed")
        codes = tuple(
            sorted(
                row.ts_code
                for row in selected
                if not row.deleted and (row.expires_at is None or row.expires_at > inspected)
            )
        )
        available_at = borrowed.manifest.built_at
    elif scope.kind == "pool":
        definitions = condition_table_rows(borrowed, "pool_definition", now=inspected)
        definition = next((row for row in definitions if row["pool_name"] == scope.pool_name), None)
        receipts = condition_table_rows(borrowed, "screen_run_receipt", now=inspected)
        receipt = next((row for row in receipts if row["preset_name"] == scope.pool_name), None)
        if (
            definition is None
            or definition["state"] != "available"
            or definition["version"] != scope.definition_version
            or receipt is None
            or receipt["definition_version"] != scope.definition_version
            or receipt["result_version"] != scope.result_version
            or receipt["lineage_complete"] is not True
            or receipt["current_definition"] is not True
            or receipt["completed_at"] is None
            or receipt["completed_at"] > inspected
        ):
            raise ValueError("condition pool definition or actual result changed")
        members = condition_table_rows(borrowed, "screen_result", now=inspected)
        codes = tuple(
            sorted(
                row["ts_code"]
                for row in members
                if row["preset_name"] == scope.pool_name
                and row["trade_date"] == receipt["trade_date"]
            )
        )
        if len(codes) != receipt["hit_count"] or len(codes) != len(set(codes)):
            raise ValueError("condition pool actual members differ from its receipt")
        from rquant.pool_result_receipt import member_set_digest

        if member_set_digest(list(codes)) != receipt["member_digest"]:
            raise ValueError("condition pool member digest differs")
        version = canonical_sha256(
            {
                "definition": scope.definition_version,
                "result": scope.result_version,
                "members": receipt["member_digest"],
            }
        )
        available_at = receipt["completed_at"]
    else:
        table = "stock_basic" if scope.sector_system == "industry" else "kpl_concept_member"
        rows = condition_table_rows(borrowed, table, now=inspected)
        selected = tuple(
            row
            for row in rows
            if row["industry" if scope.sector_system == "industry" else "board_code"]
            == scope.sector_code
        )
        version = canonical_sha256({"table": table, "components": selected})
        if version != scope.component_source_version:
            raise ValueError("condition sector components changed")
        codes = tuple(
            sorted(
                set(
                    row["ts_code" if scope.sector_system == "industry" else "con_code"]
                    for row in selected
                )
            )
        )
        available_at = borrowed.manifest.built_at
    return ConditionAlertScopeEvidence(
        owner_id=owner_id,
        scope=scope,
        scope_version=version,
        member_codes=codes,
        member_digest=canonical_sha256(codes),
        available_at=available_at,
    )


class ConditionConsumerProof(ConditionRuntimeModel):
    protocol: Literal["condition-alert-consumer-proof/v1"] = "condition-alert-consumer-proof/v1"
    producer_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    notifier_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    evaluation_contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    routing_contract_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    frequency_policy_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_epoch: str = Field(pattern=r"^[0-9a-f]{64}$")
    producer_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    serving_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    inspected_at: AwareUtcDatetime
    source_cutoff: AwareUtcDatetime
    full_source_ready: bool
    feature_contract_version: Literal[3, 4]

    @model_validator(mode="after")
    def visible_source(self) -> Self:
        if self.source_cutoff > self.inspected_at:
            raise ValueError("condition capability precedes source visibility")
        return self


def read_condition_consumer_proof(
    borrowed: BorrowedGeneration, *, now: datetime
) -> ConditionConsumerProof | None:
    try:
        rows = condition_table_rows(borrowed, "condition_alert_runtime_state", now=now)
        if len(rows) != 1 or rows[0]["snapshot_key"] != "current" or rows[0]["body_json"] is None:
            return None
        proof = ConditionConsumerProof.model_validate_json(rows[0]["body_json"])
        from rquant.runtime_builder_condition_alert import (
            condition_evaluation_contract_sha256,
            condition_routing_contract_sha256,
        )
        from rquant.web.screen_intraday import read_intraday_screen_snapshot

        for table in ("intraday_screen_source", "intraday_feature_snapshot", "market_snapshot"):
            _condition_table_row_count(borrowed, table, now=now)
        actual = read_intraday_screen_snapshot(borrowed, now=now)

        # The inspected input container cannot equal the new output containing this proof.
        # Current projection owners and actual source bytes bind the published output instead.
        if (
            proof.evaluation_contract_sha256 != condition_evaluation_contract_sha256()
            or proof.routing_contract_sha256 != condition_routing_contract_sha256()
            or proof.inspected_at > now
            or now - proof.inspected_at > timedelta(seconds=90)
            or now - proof.source_cutoff > timedelta(seconds=90)
            or proof.source_identity != actual.source.source_identity
            or proof.source_cutoff != actual.source.cutoff
            or proof.feature_contract_version != actual.source.feature_contract_version
            or actual.source.missing_codes
        ):
            return None
        return proof
    except (OSError, ValueError, RuntimeError, DuckDBError):
        return None


def condition_consumer_ready(borrowed: BorrowedGeneration, *, now: datetime) -> bool:
    proof = read_condition_consumer_proof(borrowed, now=now)
    return proof is not None and proof.full_source_ready and proof.feature_contract_version == 4


class ConditionAlertAuthorityConflict(ValueError):  # noqa: N818 - stable typed outcome
    pass


class ConditionAlertAuthorityUnavailable(ValueError):  # noqa: N818 - stable typed outcome
    pass


class ConditionAlertDeliveryRejected(ValueError):  # noqa: N818 - stable typed outcome
    pass


class ConditionDeliveryScope(ConditionRuntimeModel):
    rule: OwnedConditionAlertRule
    scope: ConditionAlertScopeEvidence | None

    @model_validator(mode="after")
    def owned_binding(self) -> Self:
        if self.scope is not None and (self.scope.owner_id, self.scope.scope) != (
            self.rule.owner_id,
            self.rule.rule.scope,
        ):
            raise ValueError("condition delivery scope differs from its owner")
        return self


class ConditionAlertDeliveryAuthorityInput(ConditionRuntimeModel):
    protocol: Literal["condition-alert-delivery-authority/v1"] = (
        "condition-alert-delivery-authority/v1"
    )
    rules: ConditionRuleAuthoritySnapshot | None
    scopes: tuple[ConditionDeliveryScope, ...] = Field(max_length=3200)
    policy: ConditionAlertRecipientPolicy
    producer: ConditionConsumerProof | None
    notifier_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    delivery_enabled: bool
    inspected_at: AwareUtcDatetime

    @model_validator(mode="after")
    def complete_authority(self) -> Self:
        keys = tuple((item.rule.owner_id, item.rule.rule.rule_id) for item in self.scopes)
        expected = (
            ()
            if self.rules is None
            else tuple((row.owner_id, row.rule_id) for row in self.rules.rows if not row.deleted)
        )
        if keys != expected or len(set(keys)) != len(keys):
            raise ValueError("condition authority omits or repeats a retained rule")
        heads = {} if self.rules is None else {(r.owner_id, r.rule_id): r for r in self.rules.rows}
        for item in self.scopes:
            head = heads[(item.rule.owner_id, item.rule.rule.rule_id)]
            if (head.version, head.rule, head.updated_at) != (
                item.rule.version,
                item.rule.rule,
                item.rule.updated_at,
            ):
                raise ValueError("condition scope does not bind its exact head")
            if item.rule.updated_at > self.inspected_at or (
                item.scope is not None and item.scope.available_at > self.inspected_at
            ):
                raise ValueError("condition scope was not visible")
        if self.producer is not None and self.producer.inspected_at > self.inspected_at:
            raise ValueError("condition producer proof is future")
        if len(self.wire_bytes()) > 8 * 1024 * 1024:
            raise ValueError("condition full delivery authority exceeds capacity")
        return self


class ConditionAlertDeliveryAuthoritySnapshot(ConditionAlertDeliveryAuthorityInput):
    authority_revision: StrictInt = Field(ge=1)
    applied_at: AwareUtcDatetime

    @model_validator(mode="after")
    def application_time(self) -> Self:
        if self.applied_at < self.inspected_at:
            raise ValueError("condition authority precedes inspection")
        return self


class ConditionAlertSendAdmission(ConditionRuntimeModel):
    protocol: Literal["condition-alert-send-admission/v1"] = "condition-alert-send-admission/v1"
    outbox_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    global_sequence: StrictInt = Field(ge=1)
    event_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    owner_id: str
    rule_id: str
    rule_version: StrictInt = Field(ge=1)
    scope_version: str = Field(pattern=r"^[0-9a-f]{64}$")
    member_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    target: DeliveryTarget
    authority_revision: StrictInt = Field(ge=1)
    authority_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    lease_worker_id: str
    claimed_attempt_no: StrictInt = Field(ge=1)
    lease_started_at: AwareUtcDatetime
    lease_until: AwareUtcDatetime
    admitted_at: AwareUtcDatetime

    @model_validator(mode="after")
    def exact_lease(self) -> Self:
        if not self.lease_started_at <= self.admitted_at < self.lease_until:
            raise ValueError("condition admission is outside its lease")
        return self


class ConditionAlertCancellationReceipt(ConditionRuntimeModel):
    outbox_id: str
    event_id: str
    authority_revision: int
    attempt_no_before: int
    attempt_no_after: int
    reason: Literal["rule_changed", "membership_changed", "recipient_revoked"]
    cancelled_at: AwareUtcDatetime


class ConditionAlertAdmittedDelivery:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("condition delivery rights require a new notifier COMMIT")


_CONDITION_ADMITTED: WeakKeyDictionary[
    ConditionAlertAdmittedDelivery, tuple[NotificationStateStore, ConditionAlertSendAdmission]
] = WeakKeyDictionary()
_CONDITION_DELIVERY_TABLES = (
    "condition_alert_delivery_authority",
    "condition_alert_send_admission",
    "condition_alert_unadmitted_cancel",
)
_CONDITION_DELIVERY_SQL = (
    "CREATE TABLE condition_alert_delivery_authority(singleton INTEGE"
    "R PRIMARY KEY CHECK(singleton=1),body_json BLOB NOT NULL,last_re"
    "ady_json BLOB)",
    "CREATE TABLE condition_alert_send_admission(outbox_id TEXT NOT N"
    "ULL,attempt_no INTEGER NOT NULL,body_json BLOB NOT NULL,PRIMARY "
    "KEY(outbox_id,attempt_no))",
    "CREATE TABLE condition_alert_unadmitted_cancel(outbox_id TEXT PR"
    "IMARY KEY,body_json BLOB NOT NULL)",
) + tuple(
    f"CREATE TRIGGER notification_revision_{table}_{operation.lower()} "
    f"AFTER {operation} ON {table} BEGIN UPDATE notification_state_revision "
    "SET revision=revision+1 WHERE singleton=1; END"
    for table in _CONDITION_DELIVERY_TABLES
    for operation in ("INSERT", "UPDATE", "DELETE")
)


def _require_condition_delivery(connection: sqlite3.Connection) -> None:
    marker = connection.execute(
        "SELECT metadata_value FROM signal_bus_metadata WHERE metadata_ke"
        "y='condition_alert_delivery_protocol'"
    ).fetchone()
    actual = {
        row[0]
        for row in connection.execute(
            "SELECT sql FROM sqlite_master WHERE tbl_name IN (?,?,?) AND sql IS NOT NULL",
            _CONDITION_DELIVERY_TABLES,
        )
    }
    if (
        marker is None
        or marker[0] != "condition-alert-send-admission/v1"
        or actual != set(_CONDITION_DELIVERY_SQL)
    ):
        raise ValueError("condition delivery protocol is not explicitly installed")


def install_condition_alert_delivery(
    store: NotificationStateStore, activation: ConditionAlertRuntimeActivation
) -> None:
    from rquant.condition_alert_route import _install_condition_history
    from rquant.notification_state import NotificationStateStore
    from rquant.price_alert_runtime_store import _private_parent

    if type(store) is not NotificationStateStore:
        raise TypeError("condition delivery uses the original notifier store")
    require_verified_condition_activation(activation, "notifier")
    _private_parent(store.path)
    with store._write_transaction() as connection:
        _install_condition_history(connection)
        marker = connection.execute(
            "SELECT 1 FROM signal_bus_metadata WHERE metadata_key='condition_"
            "alert_delivery_protocol'"
        ).fetchone()
        if marker is None:
            present = {
                row[0]
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
            }
            if set(_CONDITION_DELIVERY_TABLES) & present:
                raise ValueError("partial condition delivery schema")
            for sql in _CONDITION_DELIVERY_SQL:
                connection.execute(sql)
            connection.execute(
                "INSERT INTO signal_bus_metadata VALUES('condition_alert_delivery"
                "_protocol','condition-alert-send-admission/v1')"
            )
        _require_condition_delivery(connection)
        store._before_commit(connection)


def condition_delivery_authority(
    connection: sqlite3.Connection,
) -> ConditionAlertDeliveryAuthoritySnapshot | None:
    _require_condition_delivery(connection)
    row = connection.execute(
        "SELECT body_json FROM condition_alert_delivery_authority WHERE singleton=1"
    ).fetchone()
    return (
        None
        if row is None
        else ConditionAlertDeliveryAuthoritySnapshot.model_validate_json(bytes(row[0]))
    )


def condition_send_admission(
    connection: sqlite3.Connection, outbox_id: str, attempt_no: int
) -> ConditionAlertSendAdmission | None:
    _require_condition_delivery(connection)
    row = connection.execute(
        "SELECT body_json FROM condition_alert_send_admission WHERE outbox_id=? AND attempt_no=?",
        (outbox_id, attempt_no),
    ).fetchone()
    return None if row is None else ConditionAlertSendAdmission.model_validate_json(bytes(row[0]))


def apply_condition_alert_delivery_authority(
    store: NotificationStateStore,
    value: ConditionAlertDeliveryAuthorityInput,
    *,
    activation: ConditionAlertRuntimeActivation,
    expected_revision: int,
    applied_at: datetime,
) -> ConditionAlertDeliveryAuthoritySnapshot:
    binding = require_verified_condition_activation(activation, "notifier")
    if (
        type(value) is not ConditionAlertDeliveryAuthorityInput
        or type(expected_revision) is not int
        or expected_revision < 0
    ):
        raise TypeError("condition authority requires exact input and revision")
    value = ConditionAlertDeliveryAuthorityInput.model_validate_json(value.wire_bytes())
    if (value.notifier_manifest_sha256, value.policy.sha256, value.delivery_enabled) != (
        binding.producer_manifest_sha256,
        binding.recipient_policy_sha256,
        binding.delivery_enabled,
    ):
        raise ValueError("condition authority differs from its actual notifier")
    if value.producer is not None and (
        value.producer.evaluation_contract_sha256,
        value.producer.routing_contract_sha256,
        value.producer.frequency_policy_sha256,
        value.producer.source_epoch,
    ) != (
        binding.evaluation_contract_sha256,
        binding.routing_policy_sha256,
        binding.frequency_policy_sha256,
        binding.source_epoch,
    ):
        raise ValueError("condition producer contract differs")
    now = normalize_aware_utc(applied_at)
    if value.inspected_at > now:
        raise ValueError("condition authority inspection is future")
    head = ConditionAlertDeliveryAuthoritySnapshot(
        **value.model_dump(mode="python"), authority_revision=expected_revision + 1, applied_at=now
    )
    with store._write_transaction() as connection:
        previous = condition_delivery_authority(connection)
        if (0 if previous is None else previous.authority_revision) != expected_revision:
            raise ConditionAlertAuthorityConflict("condition revision changed")
        last_row = connection.execute(
            "SELECT last_ready_json FROM condition_alert_delivery_authority WHERE singleton=1"
        ).fetchone()
        last_ready = (
            None
            if last_row is None or last_row[0] is None
            else ConditionRuleAuthoritySnapshot.model_validate_json(bytes(last_row[0]))
        )
        if previous is not None and (
            value.inspected_at < previous.inspected_at
            or now < previous.applied_at
            or (
                value.policy.generation_id == previous.policy.generation_id
                and value.policy != previous.policy
            )
        ):
            raise ConditionAlertAuthorityConflict("condition authority regressed")
        if value.rules is not None:
            current = {(r.owner_id, r.rule_id): r for r in value.rules.rows}
            if last_ready is not None:
                for old in last_ready.rows:
                    new = current.get((old.owner_id, old.rule_id))
                    if (
                        new is None
                        or new.version < old.version
                        or (new.version == old.version and new != old)
                    ):
                        raise ConditionAlertAuthorityConflict(
                            "condition authority rewrote or omitted a retained head"
                        )
            last_ready = value.rules
        connection.execute(
            "INSERT INTO condition_alert_delivery_authority VALUES(1,?,?) ON "
            "CONFLICT(singleton) DO UPDATE SET body_json=excluded.body_json,l"
            "ast_ready_json=excluded.last_ready_json",
            (head.wire_bytes(), None if last_ready is None else last_ready.wire_bytes()),
        )
        store._condition_alert_failpoint("before_authority_commit")
        store._before_commit(connection)
    store._condition_alert_failpoint("after_authority_commit")
    return head


def fresh_condition_authority(
    head: ConditionAlertDeliveryAuthoritySnapshot | None, now: datetime
) -> ConditionAlertDeliveryAuthoritySnapshot:
    if (
        head is None
        or head.rules is None
        or head.producer is None
        or not head.delivery_enabled
        or not head.applied_at <= now
        or not head.inspected_at <= now <= head.inspected_at + timedelta(seconds=30)
        or not head.producer.full_source_ready
        or head.producer.feature_contract_version != 4
        or now - head.producer.source_cutoff > timedelta(seconds=90)
    ):
        raise ConditionAlertAuthorityUnavailable(
            "condition current source or consumer is unavailable"
        )
    return head


def _invalid_condition_event(
    head: ConditionAlertDeliveryAuthoritySnapshot,
    event: ConditionAlertBusEventRecord,
    target: DeliveryTarget,
    now: datetime,
) -> Literal["rule_changed", "membership_changed", "recipient_revoked"] | None:
    if head.rules is None:
        raise ConditionAlertAuthorityUnavailable("condition rule source is unknown")
    envelope = event.event
    current = next(
        (
            r
            for r in head.rules.rows
            if (r.owner_id, r.rule_id) == (envelope.owner_id, envelope.rule_id)
        ),
        None,
    )
    if (
        current is None
        or current.deleted
        or current.rule is None
        or not current.rule.enabled
        or (current.version, current.rule.rule_body_hash)
        != (envelope.rule_version, envelope.rule_body_hash)
    ):
        return "rule_changed"
    scope = next(
        (
            s.scope
            for s in head.scopes
            if (s.rule.owner_id, s.rule.rule.rule_id) == (envelope.owner_id, envelope.rule_id)
        ),
        None,
    )
    if scope is None:
        raise ConditionAlertAuthorityUnavailable("condition membership is unknown")
    if (scope.scope_version, scope.member_digest) != (
        envelope.scope_version,
        envelope.member_digest,
    ) or envelope.ts_code not in scope.member_codes:
        return "membership_changed"
    if (
        target not in head.policy.targets_for(envelope.owner_id)
        or target.channel not in current.rule.governance.channels
    ):
        return "recipient_revoked"
    return None


def admit_condition_alert_delivery(
    store: NotificationStateStore,
    record: OutboxRecord,
    *,
    activation: ConditionAlertRuntimeActivation,
    worker_id: str,
    expected_revision: int,
    admitted_at: datetime,
) -> ConditionAlertAdmittedDelivery | None:
    binding = require_condition_alert_activation(activation, "delivery")
    if type(record) is not OutboxRecord or type(expected_revision) is not int:
        raise TypeError("condition admission needs the original exact lease")
    now = normalize_aware_utc(admitted_at)
    with store._write_transaction() as connection:
        row = connection.execute(
            "SELECT * FROM delivery_outbox WHERE outbox_id=?", (record.outbox_id,)
        ).fetchone()
        if row is None or store._outbox_from_row(row) != record:
            raise ValueError("condition original lease changed")
        store._verify_lease(
            row, worker_id=worker_id, attempt_no=record.attempt_count, completed_at=now
        )
        if condition_send_admission(connection, record.outbox_id, record.attempt_count) is not None:
            return None
        head = condition_delivery_authority(connection)
        if head is None or head.authority_revision != expected_revision:
            raise ConditionAlertAuthorityConflict("condition revision changed before admission")
        head = fresh_condition_authority(head, now)
        if head.policy.sha256 != binding.recipient_policy_sha256:
            raise ConditionAlertAuthorityConflict("condition actual recipients changed")
        event = notification_record(connection, record.signal_id)
        if (
            type(event) is not ConditionAlertBusEventRecord
            or event.global_sequence != row["global_sequence"]
            or now >= event.event.expires_at
            or event.event.available_at > now
        ):
            raise ConditionAlertDeliveryRejected("condition exact event is missing or expired")
        invalid = _invalid_condition_event(head, event, record.target, now)
        if invalid is not None:
            raise ConditionAlertDeliveryRejected(invalid)
        receipt = ConditionAlertSendAdmission(
            outbox_id=record.outbox_id,
            global_sequence=row["global_sequence"],
            event_id=event.event_id,
            payload_sha256=event.payload_hash,
            owner_id=event.event.owner_id,
            rule_id=event.event.rule_id,
            rule_version=event.event.rule_version,
            scope_version=event.event.scope_version,
            member_digest=event.event.member_digest,
            target=record.target,
            authority_revision=head.authority_revision,
            authority_sha256=head.sha256,
            lease_worker_id=worker_id,
            claimed_attempt_no=record.attempt_count,
            lease_started_at=_require_time(row["lease_started_at"]),
            lease_until=record.lease_until,
            admitted_at=now,
        )
        connection.execute(
            "INSERT INTO condition_alert_send_admission VALUES(?,?,?)",
            (record.outbox_id, record.attempt_count, receipt.wire_bytes()),
        )
        store._condition_alert_failpoint("before_admission_commit")
        store._before_commit(connection)
    store._condition_alert_failpoint("after_admission_commit")
    token = object.__new__(ConditionAlertAdmittedDelivery)
    _CONDITION_ADMITTED[token] = store, receipt
    return token


def consume_condition_alert_admitted_delivery(
    value: object, *, store: NotificationStateStore, record: OutboxRecord, now: datetime
) -> ConditionAlertSendAdmission:
    if type(value) is not ConditionAlertAdmittedDelivery or value not in _CONDITION_ADMITTED:
        raise TypeError("condition transport requires a fresh single-use COMMIT right")
    issuer, receipt = _CONDITION_ADMITTED.pop(value)
    if issuer is not store or type(record) is not OutboxRecord:
        raise TypeError("condition admission issuer differs")
    current = normalize_aware_utc(now)
    with store._read_snapshot() as connection:
        persisted = condition_send_admission(connection, record.outbox_id, record.attempt_count)
        row = connection.execute(
            "SELECT * FROM delivery_outbox WHERE outbox_id=?", (record.outbox_id,)
        ).fetchone()
        event = notification_record(connection, record.signal_id)
        if persisted != receipt or row is None or store._outbox_from_row(row) != record:
            raise ValueError("condition admission or original lease changed")
        store._verify_lease(
            row,
            worker_id=receipt.lease_worker_id,
            attempt_no=receipt.claimed_attempt_no,
            completed_at=current,
        )
        if (
            type(event) is not ConditionAlertBusEventRecord
            or event.payload_hash != receipt.payload_sha256
            or current >= event.event.expires_at
            or current < receipt.admitted_at
        ):
            raise ValueError("condition transport original event expired")
        head = fresh_condition_authority(condition_delivery_authority(connection), current)
        if (
            head.authority_revision != receipt.authority_revision
            or _invalid_condition_event(head, event, record.target, current) is not None
        ):
            raise ValueError("condition authority changed before the provider call")
    return receipt


def cancel_condition_unadmitted(
    store: NotificationStateStore,
    outbox_id: str,
    *,
    expected_revision: int,
    cancelled_at: datetime,
    worker_id: str | None = None,
) -> ConditionAlertCancellationReceipt | None:
    now = normalize_aware_utc(cancelled_at)
    with store._write_transaction() as connection:
        head = condition_delivery_authority(connection)
        if head is None or head.authority_revision != expected_revision:
            raise ConditionAlertAuthorityConflict("condition authority changed before cancellation")
        row = connection.execute(
            "SELECT * FROM delivery_outbox WHERE outbox_id=?", (outbox_id,)
        ).fetchone()
        if row is None:
            raise ValueError("condition cancellation original outbox is missing")
        record = store._outbox_from_row(row)
        if record.status not in {OutboxStatus.PENDING, OutboxStatus.RETRY, OutboxStatus.LEASED}:
            return None
        if record.status is OutboxStatus.RETRY:
            previous = connection.execute(
                "SELECT * FROM delivery_attempt WHERE outbox_id=? AND attempt_no=?",
                (outbox_id, record.attempt_count),
            ).fetchone()
            if (
                previous is None
                or store._attempt_from_row(previous).success
                or connection.execute(
                    "SELECT 1 FROM delivery_unknown WHERE outbox_id=?", (outbox_id,)
                ).fetchone()
                is not None
            ):
                return None
        elif (
            condition_send_admission(connection, outbox_id, record.attempt_count) is not None
            or connection.execute(
                "SELECT 1 FROM delivery_unknown WHERE outbox_id=? AND attempt_no="
                "? UNION ALL SELECT 1 FROM delivery_attempt WHERE outbox_id=? AND"
                " attempt_no=? LIMIT 1",
                (outbox_id, record.attempt_count, outbox_id, record.attempt_count),
            ).fetchone()
            is not None
        ):
            return None
        event = notification_record(connection, record.signal_id)
        if type(event) is not ConditionAlertBusEventRecord:
            raise TypeError("condition cancellation cannot touch another event family")
        reason = _invalid_condition_event(head, event, record.target, now)
        if reason is None:
            return None
        after = record.attempt_count
        if record.status is OutboxStatus.LEASED:
            if worker_id is None:
                raise ValueError("condition cancellation requires its leased worker")
            store._verify_lease(
                row, worker_id=worker_id, attempt_no=record.attempt_count, completed_at=now
            )
            after -= 1
        receipt = ConditionAlertCancellationReceipt(
            outbox_id=outbox_id,
            event_id=record.signal_id,
            authority_revision=head.authority_revision,
            attempt_no_before=record.attempt_count,
            attempt_no_after=after,
            reason=reason,
            cancelled_at=now,
        )
        connection.execute(
            "INSERT INTO condition_alert_unadmitted_cancel VALUES(?,?)",
            (outbox_id, receipt.wire_bytes()),
        )
        connection.execute(
            "UPDATE delivery_outbox SET status=?,attempt_count=?,next_attempt"
            "_at=NULL,lease_owner=NULL,lease_started_at=NULL,lease_until=NULL"
            ",last_error=?,updated_at=? WHERE outbox_id=?",
            (
                OutboxStatus.DEAD_LETTER.value,
                after,
                "condition event cancelled before admission",
                _encode_time(now),
                outbox_id,
            ),
        )
        store._condition_alert_failpoint("before_cancel_commit")
        store._before_commit(connection)
    return receipt


def condition_runtime_projections(
    connection: sqlite3.Connection,
    *,
    producer: ConditionProducerRuntimeSnapshot,
    observed_at: datetime,
    history_limit: int,
) -> tuple[ServingProjectionPayload, ...]:
    from rquant.condition_alert_runtime import ConditionProducerRuntimeSnapshot
    from rquant.web.models.condition_alert_rules import (
        ConditionAlertTargetReceipt,
        ConditionAlertTriggerItem,
    )

    now = normalize_aware_utc(observed_at)
    if type(producer) is not ConditionProducerRuntimeSnapshot:
        raise TypeError("condition runtime projection requires the actual readonly producer")
    head = condition_delivery_authority(connection)
    proof = (
        None
        if head is None or head.producer is None or not head.delivery_enabled
        else head.producer
    )
    if producer.inspected_at > now:
        raise ValueError("condition producer is future")
    if proof is not None and (
        proof.producer_manifest_sha256,
        proof.source_epoch,
        proof.producer_generation_id,
    ) != (
        producer.source.producer_manifest_sha256,
        producer.source.source_epoch,
        producer.source.generation_id,
    ):
        raise ValueError("condition capability belongs to another installed producer")
    events = []
    rows = connection.execute(
        "SELECT event_id FROM condition_alert_route_receipt ORDER BY rowid DESC LIMIT ?",
        (min(history_limit, 100),),
    ).fetchall()
    for row in rows:
        routed = _condition_route_record(connection, row[0])
        if max(routed.event.available_at, routed.received_at, routed.receipt.routed_at) > now:
            continue
        leases = connection.execute(
            "SELECT * FROM delivery_outbox WHERE signal_id=? ORDER BY channel,recipient_id",
            (routed.event_id,),
        ).fetchall()
        targets = []
        for outbox_row in leases:
            outbox_id = outbox_row["outbox_id"]
            attempt = connection.execute(
                "SELECT * FROM delivery_attempt WHERE outbox_id=? ORDER BY attempt_no DESC LIMIT 1",
                (outbox_id,),
            ).fetchone()
            if _require_time(outbox_row["updated_at"]) > now or (
                attempt is not None and _require_time(attempt["completed_at"]) > now
            ):
                raise ValueError("condition delivery projection contains future attempt")
            unknown = connection.execute(
                "SELECT 1 FROM delivery_unknown WHERE outbox_id=? LIMIT 1", (outbox_id,)
            ).fetchone()
            cancel = connection.execute(
                "SELECT 1 FROM condition_alert_unadmitted_cancel WHERE outbox_id=?", (outbox_id,)
            ).fetchone()
            status = (
                "unknown"
                if unknown is not None or "unknown" in (outbox_row["last_error"] or "").lower()
                else "cancelled"
                if cancel is not None
                else "succeeded"
                if outbox_row["status"] == OutboxStatus.SUCCEEDED.value
                else "failed"
                if attempt is not None and not bool(attempt["success"])
                else "pending"
            )
            targets.append(
                ConditionAlertTargetReceipt(
                    channel=outbox_row["channel"],
                    recipient_id=outbox_row["recipient_id"],
                    state=status,
                    attempted_at=None
                    if attempt is None
                    else _require_time(attempt["completed_at"]),
                    provider_receipt=None if attempt is None else attempt["provider_receipt"],
                )
            )
        states = {target.state for target in targets}
        state = (
            "unknown"
            if "unknown" in states
            else "failed"
            if "failed" in states
            else "pending"
            if "pending" in states
            else "succeeded"
            if states == {"succeeded"}
            else "cancelled"
        )
        attempted = max(
            (target.attempted_at for target in targets if target.attempted_at is not None),
            default=None,
        )
        item = ConditionAlertTriggerItem(
            event=routed.event,
            route=routed.receipt,
            global_sequence=routed.global_sequence,
            delivery_state=state,
            attempted_at=attempted,
            provider_receipt=None if len(targets) != 1 else targets[0].provider_receipt,
            targets=targets,
        )
        events.append(
            {
                "owner_id": routed.event.owner_id,
                "event_id": routed.event_id,
                "global_sequence": routed.global_sequence,
                "body_json": item.model_dump_json(),
            }
        )
    return (
        ServingProjectionPayload(
            table_name="condition_alert_runtime_state",
            rows=(
                {
                    "snapshot_key": "current",
                    "body_json": None if proof is None else proof.wire_bytes().decode(),
                },
            ),
            available_at=now,
        ),
        ServingProjectionPayload(
            table_name="condition_alert_runtime",
            rows=tuple(
                {
                    "owner_id": fact.owner_id,
                    "rule_id": fact.rule_id,
                    "body_json": fact.wire_bytes().decode(),
                }
                for fact in producer.rules
            ),
            available_at=now,
        ),
        ServingProjectionPayload(
            table_name="condition_alert_runtime_event",
            rows=tuple(sorted(events, key=lambda r: (r["owner_id"], r["global_sequence"]))),
            available_at=now,
        ),
    )


def _condition_route_record(
    connection: sqlite3.Connection, event_id: str
) -> ConditionAlertBusRoutedRecord:
    from rquant.condition_alert_route import _condition_record

    return _condition_record(connection, event_id)


def unavailable_condition_runtime_projections(
    *, observed_at: datetime
) -> tuple[ServingProjectionPayload, ...]:
    return tuple(
        ServingProjectionPayload(
            table_name=table,
            rows=({"snapshot_key": "current", "body_json": None},)
            if table == "condition_alert_runtime_state"
            else (),
            available_at=observed_at,
        )
        for table in (
            "condition_alert_runtime_state",
            "condition_alert_runtime",
            "condition_alert_runtime_event",
        )
    )


def read_condition_runtime_item(
    borrowed: BorrowedGeneration, entry: ConditionAlertRuleEntry, *, now: datetime
) -> ConditionRuntimeRuleFact | None:
    from rquant.condition_alert_runtime import ConditionRuntimeRuleFact

    rows = condition_table_rows(borrowed, "condition_alert_runtime", now=now)
    row = next(
        (r for r in rows if (r["owner_id"], r["rule_id"]) == (entry.owner_id, entry.rule_id)), None
    )
    if row is None:
        return None
    fact = ConditionRuntimeRuleFact.model_validate_json(row["body_json"])
    if (
        entry.rule is None
        or (fact.rule_version, fact.rule_body_hash) != (entry.version, entry.rule.rule_body_hash)
        or fact.evaluated_at > now
        or now - fact.evaluated_at > timedelta(seconds=90)
    ):
        return None
    return fact


def read_condition_triggers(
    borrowed: BorrowedGeneration, *, owner_id: str, now: datetime
) -> list[ConditionAlertTriggerItem]:
    from rquant.web.models.condition_alert_rules import ConditionAlertTriggerItem

    try:
        rows = condition_table_rows(borrowed, "condition_alert_runtime_event", now=now)
    except (ValueError, RuntimeError, DuckDBError):
        return []
    selected = []
    for row in rows:
        if row["owner_id"] != owner_id:
            continue
        item = ConditionAlertTriggerItem.model_validate_json(row["body_json"])
        if (
            item.event.owner_id != owner_id
            or (item.event.event_id, item.global_sequence)
            != (row["event_id"], row["global_sequence"])
            or max(
                item.event.available_at,
                item.route.routed_at,
                item.attempted_at or item.event.available_at,
            )
            > now
        ):
            raise ValueError("condition trigger original owner or visibility differs")
        selected.append(item)
    return sorted(selected, key=lambda item: item.global_sequence, reverse=True)[:100]


def validate_condition_runtime_projections(
    projections: Mapping[str, ServingProjectionPayload],
) -> None:
    from rquant.condition_alert_runtime import ConditionRuntimeRuleFact
    from rquant.web.models.condition_alert_rules import ConditionAlertTriggerItem

    names = {
        "condition_alert_runtime_state",
        "condition_alert_runtime",
        "condition_alert_runtime_event",
    }
    if not names.intersection(projections):
        return
    if not names.issubset(projections):
        raise ValueError("condition runtime projections are partial")
    state = projections["condition_alert_runtime_state"]
    if len(state.rows) != 1 or state.rows[0]["snapshot_key"] != "current":
        raise ValueError("condition runtime current proof is incomplete")
    if state.rows[0]["body_json"] is not None:
        ConditionConsumerProof.model_validate_json(state.rows[0]["body_json"])
    for row in projections["condition_alert_runtime"].rows:
        fact = ConditionRuntimeRuleFact.model_validate_json(row["body_json"])
        if (fact.owner_id, fact.rule_id) != (row["owner_id"], row["rule_id"]):
            raise ValueError("condition runtime fact owner differs")
    for row in projections["condition_alert_runtime_event"].rows:
        trigger = ConditionAlertTriggerItem.model_validate_json(row["body_json"])
        if (trigger.event.owner_id, trigger.event.event_id, trigger.global_sequence) != (
            row["owner_id"],
            row["event_id"],
            row["global_sequence"],
        ):
            raise ValueError("condition trigger exact owner or global sequence differs")
