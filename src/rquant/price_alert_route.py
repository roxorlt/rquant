"""Dedicated price source receipts using the original bus and notification outbox."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from hashlib import sha256
from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, StrictInt, StrictStr, ValidationInfo, field_validator, model_validator

from rquant.delivery_contracts import DeliveryTarget, OutboxStatus
from rquant.manual_watchlist import OwnerId
from rquant.price_alert_runtime_contracts import (
    PriceAlertEventEnvelope,
    PriceAlertRuntimeActivation,
    PriceAlertSourceDescriptor,
    PriceRuntimeModel,
    PriceSha256,
    parse_price_alert_event,
    require_price_alert_activation,
)
from rquant.price_alert_runtime_store import PriceAlertProducerEventRecord, _private_parent
from rquant.runtime_contracts import AwareUtcDatetime, normalize_aware_utc
from rquant.strict_json import canonical_json_bytes, strict_json_loads

if TYPE_CHECKING:
    from rquant.signal_bus import SignalBusRoutedRecord, SignalBusSignalRecord, SignalBusStore

HISTORY_PROTOCOL = "mixed-notification-history/v1"


def _targets(value: object, info: ValidationInfo) -> object:
    if isinstance(value, (list, tuple)):
        for target in value:
            if not isinstance(target, dict) and type(target) is not DeliveryTarget:
                raise TypeError("price targets must have the exact original target type")
    return tuple(value) if info.mode == "json" and isinstance(value, list) else value


def target_manifest_hash(targets: tuple[DeliveryTarget, ...]) -> str:
    return sha256(
        canonical_json_bytes([item.model_dump(mode="json") for item in targets])
    ).hexdigest()


class PriceAlertOwnerTargets(PriceRuntimeModel):
    owner_id: OwnerId
    targets: tuple[DeliveryTarget, ...] = Field(max_length=2)

    _exact_targets = field_validator("targets", mode="before")(_targets)

    @model_validator(mode="after")
    def unique_targets(self) -> Self:
        keys = tuple((item.recipient_id, item.channel.value) for item in self.targets)
        if keys != tuple(sorted(set(keys))) or any(
            len(item.recipient_id) > 128 for item in self.targets
        ):
            raise ValueError("price targets must be bounded, sorted and unique")
        return self


class PriceAlertRecipientPolicy(PriceRuntimeModel):
    policy_schema: Literal["price-alert-recipient-policy/v1"] = "price-alert-recipient-policy/v1"
    generation_id: PriceSha256
    owners: tuple[PriceAlertOwnerTargets, ...] = Field(max_length=32)

    @model_validator(mode="after")
    def unique_owners(self) -> Self:
        keys = tuple(item.owner_id for item in self.owners)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("price policy owners must be sorted and unique")
        return self

    def targets_for(self, owner: str) -> tuple[DeliveryTarget, ...]:
        return next((item.targets for item in self.owners if item.owner_id == owner), ())


class PriceAlertRouteReceipt(PriceRuntimeModel):
    receipt_schema: Literal["rquant.price-alert-route-receipt/v1"] = (
        "rquant.price-alert-route-receipt/v1"
    )
    source_id: StrictStr = Field(min_length=1, max_length=128)
    source_sequence: StrictInt = Field(ge=1)
    event_id: PriceSha256
    owner_id: OwnerId
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    rule_version: StrictInt = Field(ge=1)
    membership_version: StrictInt = Field(ge=1)
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
    def validate_receipt(self) -> Self:
        PriceAlertOwnerTargets(owner_id=self.owner_id, targets=self.targets)
        if (
            self.target_count != len(self.targets)
            or target_manifest_hash(self.targets) != self.target_manifest_hash
        ):
            raise ValueError("price route target manifest differs from its actual targets")
        if self.source_inspected_at > self.routed_at:
            raise ValueError("price source was inspected after routing")
        if self.disposition == "no_target":
            if self.targets or self.reason_code is None:
                raise ValueError("price no-target receipt lacks its exact reason")
        elif self.reason_code is not None or (self.disposition == "routed" and not self.targets):
            raise ValueError("price routed receipt has an inconsistent disposition")
        body = self.model_dump(
            mode="json", exclude={"decision_fingerprint", "source_inspected_at", "routed_at"}
        )
        if sha256(canonical_json_bytes(body)).hexdigest() != self.decision_fingerprint:
            raise ValueError("price routing decision fingerprint differs")
        return self

    @classmethod
    def create(cls, **facts: object) -> PriceAlertRouteReceipt:
        body = dict(facts)
        body.setdefault("receipt_schema", "rquant.price-alert-route-receipt/v1")
        body["targets"] = [target.model_dump(mode="json") for target in body["targets"]]
        body.pop("source_inspected_at")
        body.pop("routed_at")
        return cls(**facts, decision_fingerprint=sha256(canonical_json_bytes(body)).hexdigest())


class PriceAlertBusEventRecord(PriceRuntimeModel):
    global_sequence: StrictInt = Field(ge=1)
    event_id: PriceSha256
    payload_hash: PriceSha256
    payload_json: StrictStr = Field(min_length=1, max_length=4096)
    event: PriceAlertEventEnvelope
    received_at: AwareUtcDatetime
    bus_generation_id: PriceSha256
    source: PriceAlertSourceDescriptor
    source_sequence: StrictInt = Field(ge=1)

    @field_validator("event", mode="before")
    @classmethod
    def exact_event(cls, value: object) -> PriceAlertEventEnvelope:
        return parse_price_alert_event(
            canonical_json_bytes(value) if isinstance(value, dict) else value
        )

    @model_validator(mode="after")
    def validate_event_binding(self) -> Self:
        if (
            self.event.event_id != self.event_id
            or parse_price_alert_event(self.payload_json) != self.event
            or sha256(self.payload_json.encode()).hexdigest() != self.payload_hash
            or self.event.available_at > self.received_at
            or self.source_sequence > self.source.high_watermark
        ):
            raise ValueError("price bus event differs from its original sealed facts")
        if (
            self.source.producer_manifest_sha256,
            self.source.source_epoch,
            self.source.frequency_policy_sha256,
        ) != (
            self.event.producer_manifest_sha256,
            self.event.source_epoch,
            self.event.frequency_policy_sha256,
        ):
            raise ValueError("price bus event producer/source identity differs")
        return self


class PriceAlertBusRoutedRecord(PriceAlertBusEventRecord):
    receipt: PriceAlertRouteReceipt

    @model_validator(mode="after")
    def exact_receipt(self) -> Self:
        event = self.event
        if (
            self.receipt.event_id,
            self.receipt.owner_id,
            self.receipt.rule_id,
            self.receipt.rule_version,
            self.receipt.membership_version,
            self.receipt.source_id,
            self.receipt.source_sequence,
            self.receipt.routing_policy_sha256,
        ) != (
            event.event_id,
            event.owner_id,
            event.rule_id,
            event.rule_version,
            event.membership_version,
            self.source.source_id,
            self.source_sequence,
            self.source.routing_policy_sha256,
        ):
            raise ValueError("price receipt belongs to another source or rule binding")
        if (
            event.available_at > self.receipt.source_inspected_at
            or self.received_at != self.receipt.routed_at
        ):
            raise ValueError("price bus visibility differs from the actual route receipt")
        return self


def _history_installed(connection: sqlite3.Connection) -> None:
    marker = connection.execute(
        "SELECT metadata_value FROM signal_bus_metadata WHERE me"
        "tadata_key='mixed_notification_history'"
    ).fetchone()
    if marker is None or marker[0] != HISTORY_PROTOCOL:
        raise ValueError("mixed notification history is not explicitly installed")
    tables = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if (
        not {
            "price_alert_route_activation",
            "price_alert_route_source",
            "price_alert_route_receipt",
        }
        <= tables
    ):
        raise ValueError("mixed notification history marker and tables disagree")


def _install_price_alert_history(connection: sqlite3.Connection) -> None:
    marker = connection.execute(
        "SELECT metadata_value FROM signal_bus_metadata WHERE me"
        "tadata_key='mixed_notification_history'"
    ).fetchone()
    tables = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    required = {
        "price_alert_route_activation",
        "price_alert_route_source",
        "price_alert_route_receipt",
    }
    present = required & tables
    if marker is not None or present:
        _history_installed(connection)
        return
    for ddl in (
        (
            "CREATE TABLE price_alert_route_activation (singleton IN"
            "TEGER PRIMARY KEY CHECK(singleton=1),body BLOB NOT NULL"
            ")"
        ),
        (
            "CREATE TABLE price_alert_route_source (source_id TEXT P"
            "RIMARY KEY,body BLOB NOT NULL,high_watermark INTEGER NO"
            "T NULL,last_sequence INTEGER NOT NULL)"
        ),
        (
            "CREATE TABLE price_alert_route_receipt (event_id TEXT P"
            "RIMARY KEY REFERENCES signal_envelope(signal_id),source"
            "_id TEXT NOT NULL,source_sequence INTEGER NOT NULL,sour"
            "ce_body BLOB NOT NULL,body BLOB NOT NULL,bus_generation"
            "_id TEXT NOT NULL,UNIQUE(source_id,source_sequence))"
        ),
    ):
        connection.execute(ddl)
    connection.execute(
        "INSERT INTO signal_bus_metadata VALUES('mixed_notification_history',?)",
        (HISTORY_PROTOCOL,),
    )


def install_price_alert_history(bus: SignalBusStore) -> None:
    _private_parent(bus.path)
    with bus._write_transaction() as connection:
        _install_price_alert_history(connection)
        bus._before_commit(connection)


def install_price_alert_route(bus: SignalBusStore, activation: PriceAlertRuntimeActivation) -> None:
    binding = require_price_alert_activation(activation, "routing")
    _private_parent(bus.path)
    with bus._write_transaction() as connection:
        _install_price_alert_history(connection)
        _history_installed(connection)
        prior = connection.execute(
            "SELECT body FROM price_alert_route_activation WHERE singleton=1"
        ).fetchone()
        if prior is not None:
            if bytes(prior[0]) != binding.wire_bytes():
                raise ValueError("price route activation conflicts with its explicit installation")
            return
        connection.execute(
            "INSERT INTO price_alert_route_activation VALUES(1,?)", (binding.wire_bytes(),)
        )
        bus._before_commit(connection)


def _price_ingest(
    connection: sqlite3.Connection, event: PriceAlertEventEnvelope, received_at: datetime
) -> tuple[int, bool]:
    from rquant.signal_bus import _encode_time

    payload = event.wire_bytes().decode()
    digest = sha256(payload.encode()).hexdigest()
    prior = connection.execute(
        "SELECT * FROM signal_envelope WHERE signal_id=?", (event.event_id,)
    ).fetchone()
    if prior is not None:
        if prior["payload_hash"] != digest or prior["payload_json"] != payload:
            raise ValueError("price event identity already binds different bytes")
        return prior["global_sequence"], False
    result = connection.execute(
        (
            "INSERT INTO signal_envelope(signal_id,payload_hash,payl"
            "oad_json,received_at) VALUES(?,?,?,?)"
        ),
        (event.event_id, digest, payload, _encode_time(received_at)),
    )
    sequence = int(result.lastrowid)
    connection.execute(
        (
            "UPDATE signal_bus_metadata SET metadata_value=? WHERE m"
            "etadata_key='signal_high_watermark'"
        ),
        (str(sequence),),
    )
    return sequence, True


def _price_outbox(connection: sqlite3.Connection, record: PriceAlertBusRoutedRecord) -> None:
    from rquant.signal_bus import _encode_time

    event, receipt = record.event, record.receipt
    expired = receipt.disposition == "expired"
    for target in receipt.targets:
        key = target.delivery_key(event.event_id)
        prior = connection.execute(
            (
                "SELECT signal_id,global_sequence,recipient_id,channel F"
                "ROM delivery_outbox WHERE outbox_id=?"
            ),
            (key,),
        ).fetchone()
        if prior is not None:
            if tuple(prior) != (
                event.event_id,
                record.global_sequence,
                target.recipient_id,
                target.channel.value,
            ):
                raise ValueError("price outbox target conflicts with original receipt")
            continue
        connection.execute(
            (
                "INSERT INTO delivery_outbox(outbox_id,signal_id,global_"
                "sequence,recipient_id,channel,status,expires_at,attempt"
                "_count,last_error,created_at,updated_at) VALUES(?,?,?,?"
                ",?,?,?,0,?,?,?)"
            ),
            (
                key,
                event.event_id,
                record.global_sequence,
                target.recipient_id,
                target.channel.value,
                OutboxStatus.EXPIRED.value if expired else OutboxStatus.PENDING.value,
                _encode_time(event.expires_at),
                "price alert expired before routing" if expired else None,
                _encode_time(event.available_at if expired else receipt.routed_at),
                _encode_time(receipt.routed_at),
            ),
        )


def route_price_alert_event(
    bus: SignalBusStore,
    *,
    activation: PriceAlertRuntimeActivation,
    policy: PriceAlertRecipientPolicy,
    source: PriceAlertSourceDescriptor,
    record: PriceAlertProducerEventRecord,
    source_inspected_at: datetime,
    routed_at: datetime,
) -> PriceAlertBusRoutedRecord:
    binding = require_price_alert_activation(activation, "routing")
    if (
        type(source) is not PriceAlertSourceDescriptor
        or type(record) is not PriceAlertProducerEventRecord
        or type(policy) is not PriceAlertRecipientPolicy
    ):
        raise TypeError("price route requires exact source, producer record and owner policy")
    source = PriceAlertSourceDescriptor.model_validate(source)
    record = PriceAlertProducerEventRecord.model_validate(record)
    policy = PriceAlertRecipientPolicy.model_validate(policy)
    if (
        source.source_id,
        source.ledger_id,
        source.source_epoch,
        source.generation_id,
        source.evaluation_contract_sha256,
        source.frequency_policy_sha256,
        source.routing_policy_sha256,
    ) != (
        binding.source_id,
        binding.ledger_id,
        binding.source_epoch,
        binding.generation_id,
        binding.evaluation_contract_sha256,
        binding.frequency_policy_sha256,
        binding.routing_policy_sha256,
    ) or policy.sha256 != binding.recipient_policy_sha256:
        raise ValueError("price route source or owner policy differs from the actual activation")
    observed, routed = normalize_aware_utc(source_inspected_at), normalize_aware_utc(routed_at)
    event = record.event
    if (
        event.available_at > observed
        or observed > routed
        or record.sequence > source.high_watermark
    ):
        raise ValueError("price route source facts are future or outside the durable source")
    targets = policy.targets_for(event.owner_id)
    disposition = (
        "no_target" if not targets else ("expired" if routed >= event.expires_at else "routed")
    )
    receipt = PriceAlertRouteReceipt.create(
        source_id=source.source_id,
        source_sequence=record.sequence,
        event_id=event.event_id,
        owner_id=event.owner_id,
        rule_id=event.rule_id,
        rule_version=event.rule_version,
        membership_version=event.membership_version,
        disposition=disposition,
        reason_code="no_owner_target" if not targets else None,
        routing_policy_sha256=source.routing_policy_sha256,
        target_manifest_hash=target_manifest_hash(targets),
        targets=targets,
        target_count=len(targets),
        source_inspected_at=observed,
        routed_at=routed,
    )
    failpoint = getattr(bus, "_price_alert_failpoint", lambda _: None)
    with bus._write_transaction() as connection:
        _history_installed(connection)
        activation_row = connection.execute(
            "SELECT body FROM price_alert_route_activation WHERE singleton=1"
        ).fetchone()
        if activation_row is None or bytes(activation_row[0]) != binding.wire_bytes():
            raise ValueError("price route activation is not installed for this actual role")
        original = connection.execute(
            "SELECT * FROM price_alert_route_receipt WHERE source_id=? AND source_sequence=?",
            (source.source_id, record.sequence),
        ).fetchone()
        if original is not None:
            stored = _price_record(connection, event.event_id)
            if (
                stored is None
                or stored.event != event
                or stored.receipt.decision_fingerprint != receipt.decision_fingerprint
            ):
                raise ValueError(
                    "price source sequence already binds different event or decision bytes"
                )
            return stored
        row = connection.execute(
            "SELECT * FROM price_alert_route_source WHERE source_id=?", (source.source_id,)
        ).fetchone()
        immutable = canonical_json_bytes(source.model_dump(mode="json", exclude={"high_watermark"}))
        if row is None:
            connection.execute(
                "INSERT INTO price_alert_route_source VALUES(?,?,?,?)",
                (source.source_id, immutable, source.high_watermark, source.first_sequence - 1),
            )
            last = source.first_sequence - 1
        else:
            if bytes(row["body"]) != immutable or source.high_watermark < row["high_watermark"]:
                raise ValueError("price route source identity changed or watermark regressed")
            last = row["last_sequence"]
        failpoint("source")
        if record.sequence != last + 1:
            raise ValueError("price source sequence is not the next contiguous event")
        sequence, added = _price_ingest(connection, event, routed)
        if not added:
            raise ValueError("price event already exists outside its original source receipt")
        failpoint("event")
        generation = connection.execute(
            "SELECT metadata_value FROM signal_bus_metadata WHERE me"
            "tadata_key='source_generation_id'"
        ).fetchone()[0]
        result = PriceAlertBusRoutedRecord(
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
            "INSERT INTO price_alert_route_receipt VALUES(?,?,?,?,?,?)",
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
        _price_outbox(connection, result)
        failpoint("outbox")
        connection.execute(
            (
                "UPDATE price_alert_route_source SET high_watermark=?,la"
                "st_sequence=? WHERE source_id=?"
            ),
            (source.high_watermark, record.sequence, source.source_id),
        )
        failpoint("cursor")
        bus._before_commit(connection)
        failpoint("before_commit")
    return result


def _price_record(
    connection: sqlite3.Connection, event_id: str
) -> PriceAlertBusRoutedRecord | None:
    _history_installed(connection)
    row = connection.execute(
        (
            "SELECT signal.*,receipt.body,receipt.source_body,receipt.sou"
            "rce_sequence,receipt.bus_generation_id FROM signal_envelope "
            "AS signal JOIN price_alert_route_receipt AS receipt ON "
            "receipt.event_id=signal.signal_id WHERE signal.signal_id=?"
        ),
        (event_id,),
    ).fetchone()
    if row is None:
        return None
    generation = row["bus_generation_id"]
    return PriceAlertBusRoutedRecord(
        global_sequence=row["global_sequence"],
        event_id=row["signal_id"],
        payload_hash=row["payload_hash"],
        payload_json=row["payload_json"],
        event=parse_price_alert_event(row["payload_json"]),
        received_at=datetime.fromisoformat(row["received_at"]),
        bus_generation_id=generation,
        source=PriceAlertSourceDescriptor.model_validate_json(row["source_body"]),
        source_sequence=row["source_sequence"],
        receipt=PriceAlertRouteReceipt.model_validate_json(row["body"]),
    )


def notification_record(
    connection: sqlite3.Connection, identifier: int | str, *, routed: bool = False
) -> (
    SignalBusSignalRecord
    | SignalBusRoutedRecord
    | PriceAlertBusEventRecord
    | PriceAlertBusRoutedRecord
    | None
):
    from rquant.signal_bus import SignalBusRoutedRecord, SignalBusSignalRecord, parse_stored_signal
    from rquant.signal_contracts import SignalEnvelope

    key = "global_sequence" if type(identifier) is int else "signal_id"
    row = connection.execute(
        f"SELECT *,length(CAST(payload_json AS BLOB)) AS payload_size "
        f"FROM signal_envelope WHERE {key}=?",
        (identifier,),
    ).fetchone()
    if row is None:
        return None
    schema = strict_json_loads(row["payload_json"]).get("envelope_schema")
    if schema == "rquant.price-alert-event/v1":
        item = _price_record(connection, row["signal_id"])
        if item is None:
            raise ValueError("price bus event lacks its actual committed source receipt")
        return (
            item
            if routed
            else PriceAlertBusEventRecord.model_validate_json(
                canonical_json_bytes(item.model_dump(mode="json", exclude={"receipt"}))
            )
        )
    signal = parse_stored_signal(
        signal_id=row["signal_id"],
        payload_hash=row["payload_hash"],
        payload_json=row["payload_json"],
        payload_size=row["payload_size"],
    )
    if type(signal) is not SignalEnvelope:
        raise TypeError("mixed notification history accepts only original legacy or exact price")
    values = dict(
        global_sequence=row["global_sequence"],
        signal_id=row["signal_id"],
        payload_hash=row["payload_hash"],
        payload_json=row["payload_json"],
        signal=signal,
        received_at=datetime.fromisoformat(row["received_at"]),
    )
    if not routed:
        return SignalBusSignalRecord(**values)
    receipt_row = connection.execute(
        "SELECT * FROM signal_route_receipt WHERE signal_id=?", (row["signal_id"],)
    ).fetchone()
    if receipt_row is None:
        raise ValueError("mixed notification history prefix has an unrouted legacy event")
    from rquant.signal_bus import SignalBusStore

    return SignalBusRoutedRecord(
        **values, receipt=SignalBusStore._route_receipt_from_row(receipt_row)
    )
