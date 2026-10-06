"""Condition receipts on the original global bus and notification outbox."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, StrictInt, StrictStr, field_validator, model_validator

from rquant.condition_alert_runtime_contracts import (
    ConditionAlertEventEnvelope,
    ConditionAlertProducerEventRecord,
    ConditionAlertRuntimeActivation,
    ConditionAlertSourceDescriptor,
    ConditionRuntimeModel,
    parse_condition_alert_event,
    require_condition_alert_activation,
)
from rquant.delivery_contracts import DeliveryTarget, OutboxStatus
from rquant.manual_watchlist import OwnerId
from rquant.price_alert_route import PriceAlertOwnerTargets, _targets, target_manifest_hash
from rquant.price_alert_runtime_contracts import PriceSha256
from rquant.runtime_contracts import AwareUtcDatetime, normalize_aware_utc
from rquant.strict_json import canonical_json_bytes, strict_json_loads

if TYPE_CHECKING:
    from rquant.price_alert_route import PriceAlertBusEventRecord
    from rquant.signal_bus import SignalBusSignalRecord, SignalBusStore


class ConditionAlertRecipientPolicy(ConditionRuntimeModel):
    policy_schema: Literal["condition-alert-recipient-policy/v1"] = (
        "condition-alert-recipient-policy/v1"
    )
    generation_id: PriceSha256
    owners: tuple[PriceAlertOwnerTargets, ...] = Field(max_length=32)

    @model_validator(mode="after")
    def unique_owners(self) -> Self:
        keys = tuple(item.owner_id for item in self.owners)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("condition policy owners must be sorted and unique")
        return self

    def targets_for(self, owner: str) -> tuple[DeliveryTarget, ...]:
        return next((item.targets for item in self.owners if item.owner_id == owner), ())


class ConditionAlertRouteReceipt(ConditionRuntimeModel):
    receipt_schema: Literal["rquant.condition-alert-route-receipt/v1"] = (
        "rquant.condition-alert-route-receipt/v1"
    )
    source_id: StrictStr = Field(min_length=1, max_length=128)
    source_sequence: StrictInt = Field(ge=1)
    event_id: PriceSha256
    owner_id: OwnerId
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    rule_version: StrictInt = Field(ge=1)
    scope_version: PriceSha256
    decision_fingerprint: PriceSha256
    disposition: Literal["routed", "no_target", "expired"]
    reason_code: (
        Literal[
            "no_owner_target",
            "target_capability_unavailable",
            "binding_inactive",
            "routing_policy_retired",
        ]
        | None
    )
    routing_policy_sha256: PriceSha256
    target_manifest_hash: PriceSha256
    targets: tuple[DeliveryTarget, ...] = Field(max_length=2)
    target_count: StrictInt = Field(ge=0, le=2)
    source_inspected_at: AwareUtcDatetime
    routed_at: AwareUtcDatetime

    _exact_targets = field_validator("targets", mode="before")(_targets)

    @model_validator(mode="after")
    def exact_decision(self) -> Self:
        PriceAlertOwnerTargets(owner_id=self.owner_id, targets=self.targets)
        if (
            self.target_count != len(self.targets)
            or target_manifest_hash(self.targets) != self.target_manifest_hash
            or self.source_inspected_at > self.routed_at
        ):
            raise ValueError("condition routing target or time binding differs")
        if self.disposition == "no_target":
            if self.targets or self.reason_code is None:
                raise ValueError("condition no-target receipt lacks its exact reason")
        elif self.reason_code is not None or (self.disposition == "routed" and not self.targets):
            raise ValueError("condition disposition differs from its actual targets")
        body = self.model_dump(
            mode="json", exclude={"decision_fingerprint", "source_inspected_at", "routed_at"}
        )
        if sha256(canonical_json_bytes(body)).hexdigest() != self.decision_fingerprint:
            raise ValueError("condition route fingerprint differs")
        return self

    @classmethod
    def create(cls, **facts: object) -> ConditionAlertRouteReceipt:
        body = dict(facts)
        body.setdefault("receipt_schema", "rquant.condition-alert-route-receipt/v1")
        body["targets"] = [item.model_dump(mode="json") for item in body["targets"]]
        body.pop("source_inspected_at")
        body.pop("routed_at")
        return cls(**facts, decision_fingerprint=sha256(canonical_json_bytes(body)).hexdigest())


class ConditionAlertBusEventRecord(ConditionRuntimeModel):
    global_sequence: StrictInt = Field(ge=1)
    event_id: PriceSha256
    payload_hash: PriceSha256
    payload_json: StrictStr = Field(min_length=1, max_length=16 * 1024)
    event: ConditionAlertEventEnvelope
    received_at: AwareUtcDatetime
    bus_generation_id: PriceSha256
    source: ConditionAlertSourceDescriptor
    source_sequence: StrictInt = Field(ge=1)

    @field_validator("event", mode="before")
    @classmethod
    def exact_event(cls, value: object) -> ConditionAlertEventEnvelope:
        return parse_condition_alert_event(
            canonical_json_bytes(value) if isinstance(value, dict) else value
        )

    @model_validator(mode="after")
    def exact_binding(self) -> Self:
        if (
            self.event.event_id != self.event_id
            or parse_condition_alert_event(self.payload_json) != self.event
            or self.event.sha256 != self.payload_hash
            or self.event.available_at > self.received_at
            or self.source_sequence > self.source.high_watermark
        ):
            raise ValueError("condition bus event differs from its original sealed facts")
        if (
            self.source.producer_manifest_sha256,
            self.source.source_epoch,
            self.source.evaluation_contract_sha256,
            self.source.frequency_policy_sha256,
        ) != (
            self.event.producer_manifest_sha256,
            self.event.source_epoch,
            self.event.evaluation_contract_sha256,
            self.event.frequency_policy_sha256,
        ):
            raise ValueError("condition bus event producer/source identity differs")
        return self


class ConditionAlertBusRoutedRecord(ConditionAlertBusEventRecord):
    receipt: ConditionAlertRouteReceipt

    @model_validator(mode="after")
    def exact_receipt(self) -> Self:
        event = self.event
        if (
            self.receipt.event_id,
            self.receipt.owner_id,
            self.receipt.rule_id,
            self.receipt.rule_version,
            self.receipt.scope_version,
            self.receipt.source_id,
            self.receipt.source_sequence,
            self.receipt.routing_policy_sha256,
        ) != (
            event.event_id,
            event.owner_id,
            event.rule_id,
            event.rule_version,
            event.scope_version,
            self.source.source_id,
            self.source_sequence,
            self.source.routing_policy_sha256,
        ):
            raise ValueError("condition receipt belongs to another source, owner or rule")
        if (
            event.available_at > self.receipt.source_inspected_at
            or self.received_at != self.receipt.routed_at
        ):
            raise ValueError("condition bus visibility differs from its route receipt")
        return self


_ROUTE_SQL = (
    "CREATE TABLE condition_alert_route_activation(singleton INTEGER "
    "PRIMARY KEY CHECK(singleton=1),body BLOB NOT NULL)",
    "CREATE TABLE condition_alert_route_source(source_id TEXT PRIMARY"
    " KEY,body BLOB NOT NULL,high_watermark INTEGER NOT NULL,last_seq"
    "uence INTEGER NOT NULL)",
    "CREATE TABLE condition_alert_route_receipt(event_id TEXT PRIMARY"
    " KEY REFERENCES signal_envelope(signal_id),source_id TEXT NOT NU"
    "LL,source_sequence INTEGER NOT NULL,source_body BLOB NOT NULL,bo"
    "dy BLOB NOT NULL,bus_generation_id TEXT NOT NULL,UNIQUE(source_i"
    "d,source_sequence))",
)
_ROUTE_TABLES = (
    "condition_alert_route_activation",
    "condition_alert_route_source",
    "condition_alert_route_receipt",
)


def _require_condition_history(connection: sqlite3.Connection) -> None:
    from rquant.price_alert_route import _history_installed

    _history_installed(connection)
    marker = connection.execute(
        "SELECT metadata_value FROM signal_bus_metadata WHERE metadata_ke"
        "y='condition_notification_history'"
    ).fetchone()
    actual = {
        row[0]
        for row in connection.execute(
            "SELECT sql FROM sqlite_master WHERE tbl_name IN (?,?,?) AND sql IS NOT NULL",
            _ROUTE_TABLES,
        )
    }
    if (
        marker is None
        or marker[0] != "condition-notification-history/v1"
        or actual != set(_ROUTE_SQL)
    ):
        raise ValueError("condition history is not explicitly installed on the original bus")


def _install_condition_history(connection: sqlite3.Connection) -> None:
    from rquant.price_alert_route import _install_price_alert_history

    _install_price_alert_history(connection)
    tables = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    marker = connection.execute(
        "SELECT 1 FROM signal_bus_metadata WHERE metadata_key='condition_notification_history'"
    ).fetchone()
    if set(_ROUTE_TABLES) & tables or marker is not None:
        _require_condition_history(connection)
        return
    for ddl in _ROUTE_SQL:
        connection.execute(ddl)
    connection.execute(
        "INSERT INTO signal_bus_metadata VALUES('condition_notification_h"
        "istory','condition-notification-history/v1')"
    )


def install_condition_alert_history(bus: SignalBusStore) -> None:
    from rquant.price_alert_runtime_store import _private_parent

    _private_parent(bus.path)
    with bus._write_transaction() as connection:
        _install_condition_history(connection)
        bus._before_commit(connection)


def install_condition_alert_route(
    bus: SignalBusStore, activation: ConditionAlertRuntimeActivation
) -> None:
    binding = require_condition_alert_activation(activation, "routing")
    from rquant.price_alert_runtime_store import _private_parent

    _private_parent(bus.path)
    with bus._write_transaction() as connection:
        _install_condition_history(connection)
        prior = connection.execute(
            "SELECT body FROM condition_alert_route_activation WHERE singleton=1"
        ).fetchone()
        if prior is not None:
            if bytes(prior[0]) != binding.wire_bytes():
                raise ValueError(
                    "condition routing activation conflicts with its actual installation"
                )
            return
        connection.execute(
            "INSERT INTO condition_alert_route_activation VALUES(1,?)", (binding.wire_bytes(),)
        )
        bus._before_commit(connection)


def _condition_record(
    connection: sqlite3.Connection, event_id: str
) -> ConditionAlertBusRoutedRecord | None:
    _require_condition_history(connection)
    row = connection.execute(
        "SELECT signal.*,receipt.body,receipt.source_body,receipt.source_"
        "sequence,receipt.bus_generation_id FROM signal_envelope AS signa"
        "l JOIN condition_alert_route_receipt AS receipt ON receipt.event"
        "_id=signal.signal_id WHERE signal.signal_id=?",
        (event_id,),
    ).fetchone()
    if row is None:
        return None
    return ConditionAlertBusRoutedRecord(
        global_sequence=row["global_sequence"],
        event_id=row["signal_id"],
        payload_hash=row["payload_hash"],
        payload_json=row["payload_json"],
        event=parse_condition_alert_event(row["payload_json"]),
        received_at=datetime.fromisoformat(row["received_at"]),
        bus_generation_id=row["bus_generation_id"],
        source=ConditionAlertSourceDescriptor.model_validate_json(row["source_body"]),
        source_sequence=row["source_sequence"],
        receipt=ConditionAlertRouteReceipt.model_validate_json(row["body"]),
    )


def notification_record(
    connection: sqlite3.Connection, identifier: int | str, *, routed: bool = False
) -> ConditionAlertBusEventRecord | PriceAlertBusEventRecord | SignalBusSignalRecord | None:
    from rquant.price_alert_route import notification_record as original_record

    key = "global_sequence" if type(identifier) is int else "signal_id"
    row = connection.execute(
        f"SELECT payload_json,signal_id FROM signal_envelope WHERE {key}=?", (identifier,)
    ).fetchone()
    if row is None:
        return None
    if (
        strict_json_loads(row["payload_json"]).get("envelope_schema")
        != "rquant.condition-alert-event/v1"
    ):
        return original_record(connection, identifier, routed=routed)
    record = _condition_record(connection, row["signal_id"])
    if record is None:
        raise ValueError("condition bus event lacks its actual committed source receipt")
    return (
        record
        if routed
        else ConditionAlertBusEventRecord.model_validate_json(
            canonical_json_bytes(record.model_dump(mode="json", exclude={"receipt"}))
        )
    )


def copy_condition_record(
    connection: sqlite3.Connection, record: ConditionAlertBusRoutedRecord
) -> None:
    _require_condition_history(connection)
    if type(record) is not ConditionAlertBusRoutedRecord:
        raise TypeError("condition replication requires its exact committed routed record")
    record = ConditionAlertBusRoutedRecord.model_validate_json(record.wire_bytes())
    descriptor = record.source
    immutable = canonical_json_bytes(descriptor.model_dump(mode="json", exclude={"high_watermark"}))
    row = connection.execute(
        "SELECT * FROM condition_alert_route_source WHERE source_id=?", (descriptor.source_id,)
    ).fetchone()
    if row is None:
        if record.source_sequence != descriptor.first_sequence:
            raise ValueError("condition producer source begins with a gap")
        connection.execute(
            "INSERT INTO condition_alert_route_source VALUES(?,?,?,?)",
            (descriptor.source_id, immutable, descriptor.high_watermark, record.source_sequence),
        )
    else:
        if (
            bytes(row["body"]) != immutable
            or record.source_sequence != row["last_sequence"] + 1
            or descriptor.high_watermark < row["high_watermark"]
        ):
            raise ValueError("condition producer source changed or is discontinuous")
        connection.execute(
            "UPDATE condition_alert_route_source SET high_watermark=?,last_se"
            "quence=? WHERE source_id=?",
            (descriptor.high_watermark, record.source_sequence, descriptor.source_id),
        )
    existing = connection.execute(
        "SELECT 1 FROM signal_envelope WHERE signal_id=?", (record.event_id,)
    ).fetchone()
    if existing is not None:
        raise ValueError("condition local identity conflicts with its original sealed source")
    cursor = connection.execute(
        "INSERT INTO signal_envelope(signal_id,payload_hash,payload_json,"
        "received_at) VALUES(?,?,?,?)",
        (record.event_id, record.payload_hash, record.payload_json, record.received_at.isoformat()),
    )
    if int(cursor.lastrowid) != record.global_sequence:
        raise ValueError("condition replication sequence conflicts with its actual source")
    connection.execute(
        "UPDATE signal_bus_metadata SET metadata_value=? WHERE metadata_k"
        "ey='signal_high_watermark'",
        (str(record.global_sequence),),
    )
    connection.execute(
        "INSERT INTO condition_alert_route_receipt VALUES(?,?,?,?,?,?)",
        (
            record.event_id,
            descriptor.source_id,
            record.source_sequence,
            descriptor.wire_bytes(),
            record.receipt.wire_bytes(),
            record.bus_generation_id,
        ),
    )
    _condition_outbox(connection, record)


def _condition_outbox(
    connection: sqlite3.Connection, record: ConditionAlertBusRoutedRecord
) -> None:
    from rquant.signal_bus import _encode_time

    event, receipt = record.event, record.receipt
    for target in receipt.targets:
        key = target.delivery_key(event.event_id)
        prior = connection.execute(
            "SELECT signal_id,global_sequence,recipient_id,channel FROM deliv"
            "ery_outbox WHERE outbox_id=?",
            (key,),
        ).fetchone()
        if prior is not None:
            if tuple(prior) != (
                event.event_id,
                record.global_sequence,
                target.recipient_id,
                target.channel.value,
            ):
                raise ValueError("condition outbox target conflicts with original receipt")
            continue
        expired = receipt.disposition == "expired"
        connection.execute(
            "INSERT INTO delivery_outbox(outbox_id,signal_id,global_sequence,"
            "recipient_id,channel,status,expires_at,attempt_count,last_error,"
            "created_at,updated_at) VALUES(?,?,?,?,?,?,?,0,?,?,?)",
            (
                key,
                event.event_id,
                record.global_sequence,
                target.recipient_id,
                target.channel.value,
                OutboxStatus.EXPIRED.value if expired else OutboxStatus.PENDING.value,
                _encode_time(event.expires_at),
                "condition alert expired before routing" if expired else None,
                _encode_time(event.available_at if expired else receipt.routed_at),
                _encode_time(receipt.routed_at),
            ),
        )


def route_condition_alert_event(
    bus: SignalBusStore,
    *,
    activation: ConditionAlertRuntimeActivation,
    policy: ConditionAlertRecipientPolicy,
    source: ConditionAlertSourceDescriptor,
    record: ConditionAlertProducerEventRecord,
    source_inspected_at: datetime,
    routed_at: datetime,
) -> ConditionAlertBusRoutedRecord:
    binding = require_condition_alert_activation(activation, "routing")
    if (
        type(source) is not ConditionAlertSourceDescriptor
        or type(record) is not ConditionAlertProducerEventRecord
        or type(policy) is not ConditionAlertRecipientPolicy
    ):
        raise TypeError("condition route requires exact source, event and original owner policy")
    source = ConditionAlertSourceDescriptor.model_validate_json(source.wire_bytes())
    record = ConditionAlertProducerEventRecord.model_validate_json(record.wire_bytes())
    policy = ConditionAlertRecipientPolicy.model_validate_json(policy.wire_bytes())
    names = (
        "source_id",
        "ledger_id",
        "source_epoch",
        "generation_id",
        "evaluation_contract_sha256",
        "frequency_policy_sha256",
        "routing_policy_sha256",
    )
    if (
        any(getattr(source, name) != getattr(binding, name) for name in names)
        or policy.sha256 != binding.recipient_policy_sha256
    ):
        raise ValueError(
            "condition route source or owner policy differs from its actual activation"
        )
    inspected, routed = normalize_aware_utc(source_inspected_at), normalize_aware_utc(routed_at)
    event = record.event
    if (
        event.available_at > inspected
        or inspected > routed
        or record.sequence > source.high_watermark
    ):
        raise ValueError("condition route facts are future or outside its durable ledger")
    targets = tuple(
        target for target in policy.targets_for(event.owner_id) if target.channel in event.channels
    )
    disposition = (
        "no_target" if not targets else ("expired" if routed >= event.expires_at else "routed")
    )
    receipt = ConditionAlertRouteReceipt.create(
        source_id=source.source_id,
        source_sequence=record.sequence,
        event_id=event.event_id,
        owner_id=event.owner_id,
        rule_id=event.rule_id,
        rule_version=event.rule_version,
        scope_version=event.scope_version,
        disposition=disposition,
        reason_code="no_owner_target" if not targets else None,
        routing_policy_sha256=source.routing_policy_sha256,
        target_manifest_hash=target_manifest_hash(targets),
        targets=targets,
        target_count=len(targets),
        source_inspected_at=inspected,
        routed_at=routed,
    )
    failpoint = getattr(bus, "_condition_alert_failpoint", lambda _: None)
    with bus._write_transaction() as connection:
        _require_condition_history(connection)
        activation_row = connection.execute(
            "SELECT body FROM condition_alert_route_activation WHERE singleton=1"
        ).fetchone()
        if activation_row is None or bytes(activation_row[0]) != binding.wire_bytes():
            raise ValueError("condition route actual activation is not installed")
        prior = connection.execute(
            "SELECT event_id FROM condition_alert_route_receipt WHERE source_"
            "id=? AND source_sequence=?",
            (source.source_id, record.sequence),
        ).fetchone()
        if prior is not None:
            original = _condition_record(connection, prior[0])
            if (
                original is None
                or original.event != event
                or original.receipt.decision_fingerprint != receipt.decision_fingerprint
            ):
                raise ValueError("condition replay differs from its original sealed decision")
            return original
        row = connection.execute(
            "SELECT * FROM condition_alert_route_source WHERE source_id=?", (source.source_id,)
        ).fetchone()
        immutable = canonical_json_bytes(source.model_dump(mode="json", exclude={"high_watermark"}))
        last = 0 if row is None else row["last_sequence"]
        if row is None:
            connection.execute(
                "INSERT INTO condition_alert_route_source VALUES(?,?,?,0)",
                (source.source_id, immutable, source.high_watermark),
            )
        elif bytes(row["body"]) != immutable or source.high_watermark < row["high_watermark"]:
            raise ValueError("condition route source changed or watermark regressed")
        failpoint("source")
        if record.sequence != last + 1:
            raise ValueError("condition source cannot skip its original contiguous sequence")
        existing = connection.execute(
            "SELECT 1 FROM signal_envelope WHERE signal_id=?", (event.event_id,)
        ).fetchone()
        if existing is not None:
            raise ValueError("condition event already exists outside its original source receipt")
        cursor = connection.execute(
            "INSERT INTO signal_envelope(signal_id,payload_hash,payload_json,"
            "received_at) VALUES(?,?,?,?)",
            (event.event_id, record.payload_sha256, record.payload_json, routed.isoformat()),
        )
        sequence = int(cursor.lastrowid)
        connection.execute(
            "UPDATE signal_bus_metadata SET metadata_value=? WHERE metadata_k"
            "ey='signal_high_watermark'",
            (str(sequence),),
        )
        failpoint("event")
        generation = connection.execute(
            "SELECT metadata_value FROM signal_bus_metadata WHERE metadata_ke"
            "y='source_generation_id'"
        ).fetchone()[0]
        result = ConditionAlertBusRoutedRecord(
            global_sequence=sequence,
            event_id=event.event_id,
            payload_hash=record.payload_sha256,
            payload_json=record.payload_json,
            event=event,
            received_at=routed,
            bus_generation_id=generation,
            source=source,
            source_sequence=record.sequence,
            receipt=receipt,
        )
        connection.execute(
            "INSERT INTO condition_alert_route_receipt VALUES(?,?,?,?,?,?)",
            (
                event.event_id,
                source.source_id,
                record.sequence,
                source.wire_bytes(),
                receipt.wire_bytes(),
                generation,
            ),
        )
        failpoint("receipt")
        _condition_outbox(connection, result)
        failpoint("outbox")
        connection.execute(
            "UPDATE condition_alert_route_source SET high_watermark=?,last_se"
            "quence=? WHERE source_id=?",
            (source.high_watermark, record.sequence, source.source_id),
        )
        failpoint("cursor")
        bus._before_commit(connection)
        failpoint("before_commit")
    return result
