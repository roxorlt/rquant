"""Typed notification authority and durable price admission facts."""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import datetime, time, timedelta
from hashlib import sha256
from typing import TYPE_CHECKING, Literal, Self
from weakref import WeakKeyDictionary
from zoneinfo import ZoneInfo

from pydantic import Field, StrictBool, StrictInt, StrictStr, model_validator

from rquant.delivery_contracts import DeliveryTarget, OutboxRecord, OutboxStatus
from rquant.manual_watchlist import OwnerId
from rquant.price_alert_route import (
    PriceAlertBusEventRecord,
    PriceAlertRecipientPolicy,
    notification_record,
)
from rquant.price_alert_runtime_contracts import (
    PriceAlertRuntimeActivation,
    PriceAlertSourceDescriptor,
    PriceRuntimeModel,
    PriceSha256,
    require_verified_price_alert_activation,
)
from rquant.price_alert_runtime_source import (
    PriceAlertScopeSnapshot,
    UnavailablePriceAlertScope,
    original_price_rule,
)
from rquant.price_alert_runtime_store import (
    PriceProducerRuntimeSnapshot,
    PriceRoundReceipt,
    PriceRuntimeRuleFact,
)
from rquant.runtime_contracts import AwareUtcDatetime, normalize_aware_utc
from rquant.signal_bus import SignalBusStore, _encode_time, _require_time
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.notification_state import NotificationStateStore
    from rquant.serving_read_models import ServingProjectionPayload


class PriceAlertAuthorityConflict(ValueError):  # noqa: N818 - Keep the typed v1 outcome name.
    """The applied source or expected local revision changed."""


class PriceAlertAuthorityUnavailable(ValueError):  # noqa: N818 - Keep the typed v1 outcome name.
    """The complete authority is unknown, stale or has delivery disabled."""


class PriceAlertDeliveryRejected(ValueError):  # noqa: N818 - Keep the typed v1 outcome name.
    """A trusted current authority proves this sealed event is no longer valid."""


class PriceAlertDeliveryAuthorityInput(PriceRuntimeModel):
    authority_schema: Literal["price-alert-delivery-authority/v1"] = (
        "price-alert-delivery-authority/v1"
    )
    scope: PriceAlertScopeSnapshot | UnavailablePriceAlertScope
    policy: PriceAlertRecipientPolicy
    owner_policy_manifest_sha256: PriceSha256
    delivery_enabled: StrictBool
    inspected_at: AwareUtcDatetime

    @model_validator(mode="after")
    def complete_body(self) -> Self:
        if (
            type(self.scope) not in {PriceAlertScopeSnapshot, UnavailablePriceAlertScope}
            or type(self.policy) is not PriceAlertRecipientPolicy
        ):
            raise TypeError("price authority requires exact verified domain models")
        if self.scope.inspected_at > self.inspected_at:
            raise ValueError("price authority precedes its scope inspection")
        if len(self.wire_bytes()) > 1024 * 1024:
            raise ValueError("complete price authority exceeds 1 MiB")
        return self


class PriceAlertDeliveryAuthoritySnapshot(PriceAlertDeliveryAuthorityInput):
    authority_revision: StrictInt = Field(ge=1)
    applied_at: AwareUtcDatetime

    @model_validator(mode="after")
    def application_time(self) -> Self:
        if self.applied_at < self.inspected_at:
            raise ValueError("price authority was applied before inspection")
        return self


class PriceAlertAuthorityReceipt(PriceRuntimeModel):
    authority_revision: StrictInt = Field(ge=1)
    authority_sha256: PriceSha256
    inspected_at: AwareUtcDatetime
    applied_at: AwareUtcDatetime
    availability: Literal["ready", "unavailable"]


class PriceAlertScopeSourceVersion(PriceRuntimeModel):
    generation_id: PriceSha256
    source_sequence: StrictInt = Field(ge=0)


class PriceAlertSendAdmission(PriceRuntimeModel):
    admission_schema: Literal["price-alert-send-admission/v1"] = "price-alert-send-admission/v1"
    outbox_id: PriceSha256
    global_sequence: StrictInt = Field(ge=1)
    event_id: PriceSha256
    payload_sha256: PriceSha256
    owner_id: OwnerId
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    rule_version: StrictInt = Field(ge=1)
    membership_version: StrictInt = Field(ge=1)
    target: DeliveryTarget
    authority_revision: StrictInt = Field(ge=1)
    scope_generation_id: PriceSha256
    scope_source_versions: PriceAlertScopeSourceVersion
    scope_manifest_sha256: PriceSha256
    rule_rows_sha256: PriceSha256
    member_rows_sha256: PriceSha256
    owner_policy_generation_id: PriceSha256
    owner_policy_sha256: PriceSha256
    lease_worker_id: StrictStr = Field(min_length=1, max_length=256)
    claimed_attempt_no: StrictInt = Field(ge=1)
    lease_started_at: AwareUtcDatetime
    lease_until: AwareUtcDatetime
    admitted_at: AwareUtcDatetime

    @model_validator(mode="after")
    def exact_lease(self) -> Self:
        if (
            type(self.target) is not DeliveryTarget
            or not self.lease_started_at <= self.admitted_at < self.lease_until
        ):
            raise ValueError("price admission differs from its exact target or lease")
        return self


class PriceAlertCancellationReceipt(PriceRuntimeModel):
    cancellation_schema: Literal["price-alert-unadmitted-cancel/v1"] = (
        "price-alert-unadmitted-cancel/v1"
    )
    outbox_id: PriceSha256
    event_id: PriceSha256
    authority_revision: StrictInt = Field(ge=1)
    attempt_no_before: StrictInt = Field(ge=0)
    attempt_no_after: StrictInt = Field(ge=0)
    reason: Literal["rule_changed", "membership_changed", "recipient_revoked"]
    cancelled_at: AwareUtcDatetime


class PriceAlertAdmittedDelivery:
    """A single-use process-local right minted only by a successful new COMMIT."""

    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("price delivery rights come only from the notifier transaction")


_ADMITTED: WeakKeyDictionary[PriceAlertAdmittedDelivery, tuple[object, PriceAlertSendAdmission]] = (
    WeakKeyDictionary()
)
_DELIVERY_PROTOCOL = "price-alert-send-admission/v1"
_TABLES = (
    "price_alert_delivery_authority",
    "price_alert_delivery_authority_receipt",
    "price_alert_send_admission",
    "price_alert_unadmitted_cancel",
)
_DELIVERY_SQL = (
    (
        "CREATE TABLE price_alert_delivery_authority(singleton I"
        "NTEGER PRIMARY KEY CHECK(singleton=1), body_json BLOB N"
        "OT NULL, last_ready_json BLOB)"
    ),
    (
        "CREATE TABLE price_alert_delivery_authority_receipt(rev"
        "ision INTEGER PRIMARY KEY, body_json BLOB NOT NULL)"
    ),
    (
        "CREATE TABLE price_alert_send_admission(outbox_id TEXT "
        "NOT NULL, attempt_no INTEGER NOT NULL, body_json BLOB N"
        "OT NULL, PRIMARY KEY(outbox_id,attempt_no))"
    ),
    (
        "CREATE TABLE price_alert_unadmitted_cancel(outbox_id TE"
        "XT PRIMARY KEY, body_json BLOB NOT NULL)"
    ),
) + tuple(
    f"CREATE TRIGGER notification_revision_{table}_{operation.lower()} "
    f"AFTER {operation} ON {table} BEGIN "
    "UPDATE notification_state_revision SET revision=revision+1 WHERE singleton=1; END"
    for table in (
        "price_alert_delivery_authority",
        "price_alert_send_admission",
        "price_alert_unadmitted_cancel",
    )
    for operation in ("INSERT", "UPDATE", "DELETE")
)


def _require_delivery_tables(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT metadata_value FROM signal_bus_metadata WHERE me"
        "tadata_key='price_alert_delivery_protocol'"
    ).fetchone()
    existing = {
        row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    if row is None or row[0] != _DELIVERY_PROTOCOL or not set(_TABLES) <= existing:
        raise ValueError("price delivery protocol is not explicitly installed")
    actual = {
        row[0]
        for row in connection.execute(
            "SELECT sql FROM sqlite_master WHERE tbl_name IN (?,?,?,?) AND sql IS NOT NULL", _TABLES
        )
    }
    if actual != set(_DELIVERY_SQL):
        raise ValueError("price delivery installed schema differs")


def install_price_alert_delivery(
    store: NotificationStateStore, activation: PriceAlertRuntimeActivation
) -> None:
    from rquant.price_alert_route import _install_price_alert_history
    from rquant.price_alert_runtime_store import _private_parent

    require_verified_price_alert_activation(activation, "notifier")
    _private_parent(store.path)
    with store._write_transaction() as connection:
        marker = connection.execute(
            "SELECT metadata_value FROM signal_bus_metadata WHERE me"
            "tadata_key='price_alert_delivery_protocol'"
        ).fetchone()
        present = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        } & set(_TABLES)
        if marker is not None:
            _require_delivery_tables(connection)
            return
        if present:
            raise ValueError("price delivery has unregistered existing tables")
        _install_price_alert_history(connection)
        for statement in _DELIVERY_SQL:
            connection.execute(statement)
        connection.execute(
            "INSERT INTO signal_bus_metadata VALUES(?,?)",
            ("price_alert_delivery_protocol", _DELIVERY_PROTOCOL),
        )
        store._before_commit(connection)


def _head(connection: sqlite3.Connection) -> PriceAlertDeliveryAuthoritySnapshot | None:
    _require_delivery_tables(connection)
    row = connection.execute(
        "SELECT body_json FROM price_alert_delivery_authority WHERE singleton=1"
    ).fetchone()
    return None if row is None else PriceAlertDeliveryAuthoritySnapshot.model_validate_json(row[0])


def _same_generation_body(scope: PriceAlertScopeSnapshot) -> bytes:
    return canonical_json_bytes(scope.model_dump(mode="json", exclude={"inspected_at"}))


def _check_scope_progress(
    previous: PriceAlertScopeSnapshot, current: PriceAlertScopeSnapshot
) -> None:
    if (
        current.inspected_at < previous.inspected_at
        or current.built_at < previous.built_at
        or current.available_at < previous.available_at
        or (
            current.source_generation_id == previous.source_generation_id
            and current.source_sequence < previous.source_sequence
        )
    ):
        raise PriceAlertAuthorityConflict("price source inspection or version regressed")
    if current.generation_id == previous.generation_id and _same_generation_body(
        previous
    ) != _same_generation_body(current):
        raise PriceAlertAuthorityConflict("same price scope generation has a different body")
    for old_rows, new_rows, key in (
        (previous.rules, current.rules, lambda item: (item.owner_id, item.rule_id)),
        (previous.members, current.members, lambda item: (item.owner_id, item.ts_code)),
    ):
        current_by_key = {key(row): row for row in new_rows}
        for old in old_rows:
            new = current_by_key.get(key(old))
            if (
                new is None
                or new.version < old.version
                or (new.version == old.version and new != old)
            ):
                raise PriceAlertAuthorityConflict(
                    "price authority omitted or rewrote a retained source version"
                )


def apply_price_alert_delivery_authority(
    store: NotificationStateStore,
    value: PriceAlertDeliveryAuthorityInput,
    *,
    activation: PriceAlertRuntimeActivation,
    expected_revision: int,
    applied_at: datetime,
) -> PriceAlertDeliveryAuthoritySnapshot:
    binding = require_verified_price_alert_activation(activation, "notifier")
    if (
        type(value) is not PriceAlertDeliveryAuthorityInput
        or type(expected_revision) is not int
        or expected_revision < 0
    ):
        raise TypeError("price authority application requires an exact typed input and revision")
    value = PriceAlertDeliveryAuthorityInput.model_validate_json(value.wire_bytes())
    if (
        value.owner_policy_manifest_sha256 != binding.producer_manifest_sha256
        or value.policy.sha256 != binding.recipient_policy_sha256
        or value.delivery_enabled != binding.delivery_enabled
    ):
        raise ValueError("price authority does not match the actual notifier manifest")
    now = normalize_aware_utc(applied_at)
    if value.inspected_at > now:
        raise ValueError("price authority inspection is in the future")
    snapshot = PriceAlertDeliveryAuthoritySnapshot(
        **value.model_dump(mode="python"), authority_revision=expected_revision + 1, applied_at=now
    )
    with store._write_transaction() as connection:
        previous = _head(connection)
        if (0 if previous is None else previous.authority_revision) != expected_revision:
            raise PriceAlertAuthorityConflict("price local authority revision changed")
        if previous is not None:
            if value.inspected_at < previous.inspected_at or now < previous.applied_at:
                raise PriceAlertAuthorityConflict(
                    "price authority application is older than the current head"
                )
            if (
                value.policy.generation_id == previous.policy.generation_id
                and value.policy != previous.policy
            ):
                raise PriceAlertAuthorityConflict(
                    "same recipient-policy generation has a different body"
                )
        row = connection.execute(
            "SELECT last_ready_json FROM price_alert_delivery_authority WHERE singleton=1"
        ).fetchone()
        last_ready = (
            None
            if row is None or row[0] is None
            else PriceAlertScopeSnapshot.model_validate_json(row[0])
        )
        if type(value.scope) is PriceAlertScopeSnapshot:
            if last_ready is not None:
                _check_scope_progress(last_ready, value.scope)
            last_ready = value.scope
        receipt = PriceAlertAuthorityReceipt(
            authority_revision=snapshot.authority_revision,
            authority_sha256=snapshot.sha256,
            availability=value.scope.availability,
            inspected_at=value.inspected_at,
            applied_at=now,
        )
        connection.execute(
            (
                "INSERT INTO price_alert_delivery_authority VALUES(1,?,?) ON "
                "CONFLICT(singleton) DO UPDATE SET "
                "body_json=excluded.body_json,last_ready_json=excluded.last_r"
                "eady_json"
            ),
            (snapshot.wire_bytes(), None if last_ready is None else last_ready.wire_bytes()),
        )
        connection.execute(
            "INSERT INTO price_alert_delivery_authority_receipt VALUES(?,?)",
            (snapshot.authority_revision, receipt.wire_bytes()),
        )
        store._price_alert_failpoint("before_authority_commit")
        store._before_commit(connection)
    store._price_alert_failpoint("after_authority_commit")
    return snapshot


def _fresh_authority(
    head: PriceAlertDeliveryAuthoritySnapshot | None, now: datetime
) -> PriceAlertScopeSnapshot:
    if (
        head is None
        or type(head.scope) is not PriceAlertScopeSnapshot
        or not head.delivery_enabled
        or not head.applied_at <= now
        or not head.inspected_at <= now
        or not head.scope.built_at <= now <= head.scope.built_at + timedelta(seconds=30)
    ):
        raise PriceAlertAuthorityUnavailable("complete current price authority is unavailable")
    return head.scope


def _valid_current_event(
    head: PriceAlertDeliveryAuthoritySnapshot,
    event: PriceAlertBusEventRecord,
    target: DeliveryTarget,
    now: datetime,
) -> Literal["rule_changed", "membership_changed", "recipient_revoked"] | None:
    scope = _fresh_authority(head, now)
    value = event.event
    rule = next(
        (
            row
            for row in scope.rules
            if (row.owner_id, row.rule_id) == (value.owner_id, value.rule_id)
        ),
        None,
    )
    if rule is None or rule.deleted or not rule.enabled or rule.version != value.rule_version:
        return "rule_changed"
    original = original_price_rule(rule)
    rule_sha = sha256(canonical_json_bytes(original.model_dump(mode="json"))).hexdigest()
    local = now.astimezone(ZoneInfo("Asia/Shanghai"))
    local_time = local.time().replace(tzinfo=None)
    if (
        rule_sha != value.rule_body_sha256
        or rule.ts_code != value.ts_code
        or not original.valid_from <= local_time < original.valid_until
        or not (time(9, 30) <= local_time < time(11, 30) or time(13) <= local_time < time(14, 57))
        or local.date() != value.trade_date
    ):
        return "rule_changed"
    member = next(
        (
            row
            for row in scope.members
            if (row.owner_id, row.ts_code) == (value.owner_id, value.ts_code)
        ),
        None,
    )
    if (
        member is None
        or member.deleted
        or member.version != value.membership_version
        or rule.membership_version != member.version
        or (member.expires_at is not None and member.expires_at <= now)
        or sha256(canonical_json_bytes(member.projection_row())).hexdigest()
        != value.member_binding_sha256
    ):
        return "membership_changed"
    if target not in head.policy.targets_for(value.owner_id):
        return "recipient_revoked"
    return None


def _admission(
    connection: sqlite3.Connection, outbox_id: str, attempt_no: int
) -> PriceAlertSendAdmission | None:
    _require_delivery_tables(connection)
    row = connection.execute(
        "SELECT body_json FROM price_alert_send_admission WHERE outbox_id=? AND attempt_no=?",
        (outbox_id, attempt_no),
    ).fetchone()
    return None if row is None else PriceAlertSendAdmission.model_validate_json(row[0])


def admit_price_alert_delivery(
    store: NotificationStateStore,
    record: OutboxRecord,
    *,
    activation: PriceAlertRuntimeActivation,
    worker_id: str,
    expected_revision: int,
    admitted_at: datetime,
) -> PriceAlertAdmittedDelivery | None:
    from rquant.price_alert_runtime_contracts import require_price_alert_activation

    binding = require_price_alert_activation(activation, "delivery")
    if type(record) is not OutboxRecord or type(expected_revision) is not int:
        raise TypeError("price admission requires the original typed lease and expected revision")
    now = normalize_aware_utc(admitted_at)
    with store._write_transaction() as connection:
        existing = _admission(connection, record.outbox_id, record.attempt_count)
        if existing is not None:
            original_row = connection.execute(
                "SELECT * FROM delivery_outbox WHERE outbox_id=?", (record.outbox_id,)
            ).fetchone()
            if (
                original_row is None
                or original_row["global_sequence"] != existing.global_sequence
                or (
                    existing.event_id,
                    existing.target,
                    existing.lease_worker_id,
                    existing.lease_started_at,
                    existing.lease_until,
                )
                != (
                    record.signal_id,
                    record.target,
                    worker_id,
                    _require_time(original_row["lease_started_at"]),
                    record.lease_until,
                )
            ):
                raise ValueError("persisted price admission differs from its original lease")
            return None
        head = _head(connection)
        if head is None or head.authority_revision != expected_revision:
            raise PriceAlertAuthorityConflict("price authority revision changed before admission")
        scope = _fresh_authority(head, now)
        if binding.recipient_policy_sha256 != head.policy.sha256:
            raise PriceAlertAuthorityConflict(
                "notifier manifest no longer matches the applied authority"
            )
        row = connection.execute(
            "SELECT * FROM delivery_outbox WHERE outbox_id=?", (record.outbox_id,)
        ).fetchone()
        if row is None or store._outbox_from_row(row) != record:
            raise ValueError("price admission does not match the complete original leased row")
        store._verify_lease(
            row, worker_id=worker_id, attempt_no=record.attempt_count, completed_at=now
        )
        event = notification_record(connection, record.signal_id)
        if type(event) is not PriceAlertBusEventRecord or (
            event.global_sequence != row["global_sequence"]
            or now >= event.event.expires_at
            or event.event.available_at > now
        ):
            raise PriceAlertDeliveryRejected(
                "price event is expired or does not match its original queue"
            )
        invalid = _valid_current_event(head, event, record.target, now)
        if invalid is not None:
            raise PriceAlertDeliveryRejected(invalid)
        receipt = PriceAlertSendAdmission(
            outbox_id=record.outbox_id,
            global_sequence=row["global_sequence"],
            event_id=event.event_id,
            payload_sha256=event.payload_hash,
            owner_id=event.event.owner_id,
            rule_id=event.event.rule_id,
            rule_version=event.event.rule_version,
            membership_version=event.event.membership_version,
            target=record.target,
            authority_revision=head.authority_revision,
            scope_generation_id=scope.generation_id,
            scope_source_versions=PriceAlertScopeSourceVersion(
                generation_id=scope.source_generation_id, source_sequence=scope.source_sequence
            ),
            scope_manifest_sha256=scope.manifest_sha256,
            rule_rows_sha256=scope.rule_rows_sha256,
            member_rows_sha256=scope.member_rows_sha256,
            owner_policy_generation_id=head.policy.generation_id,
            owner_policy_sha256=head.policy.sha256,
            lease_worker_id=worker_id,
            claimed_attempt_no=record.attempt_count,
            lease_started_at=_require_time(row["lease_started_at"]),
            lease_until=record.lease_until,
            admitted_at=now,
        )
        connection.execute(
            "INSERT INTO price_alert_send_admission VALUES(?,?,?)",
            (record.outbox_id, record.attempt_count, receipt.wire_bytes()),
        )
        store._price_alert_failpoint("before_admission_commit")
        store._before_commit(connection)
    store._price_alert_failpoint("after_admission_commit")
    delivery = object.__new__(PriceAlertAdmittedDelivery)
    _ADMITTED[delivery] = store, receipt
    return delivery


def consume_price_alert_admitted_delivery(
    value: object, *, store: NotificationStateStore, record: OutboxRecord, now: datetime
) -> PriceAlertSendAdmission:
    if type(value) is not PriceAlertAdmittedDelivery or value not in _ADMITTED:
        raise TypeError("price transport requires a fresh single-use committed admission")
    issuer, receipt = _ADMITTED.pop(value)
    if issuer is not store or type(record) is not OutboxRecord:
        raise TypeError("price admission belongs to a different notifier issuer")
    current = normalize_aware_utc(now)
    with store._read_snapshot() as connection:
        persisted = _admission(connection, record.outbox_id, record.attempt_count)
        row = connection.execute(
            "SELECT * FROM delivery_outbox WHERE outbox_id=?", (record.outbox_id,)
        ).fetchone()
        event = notification_record(connection, receipt.event_id)
        if persisted != receipt or row is None or store._outbox_from_row(row) != record:
            raise ValueError("price transport admission or original lease changed")
        store._verify_lease(
            row,
            worker_id=receipt.lease_worker_id,
            attempt_no=receipt.claimed_attempt_no,
            completed_at=current,
        )
        if (
            type(event) is not PriceAlertBusEventRecord
            or event.payload_hash != receipt.payload_sha256
            or current >= event.event.expires_at
            or current < receipt.admitted_at
        ):
            raise ValueError("price transport no longer has a valid original event TTL")
    return receipt


class PriceAlertOwnerTargetCount(PriceRuntimeModel):
    owner_id: OwnerId
    target_count: StrictInt = Field(ge=0, le=2)


class PriceAlertAuthorityFact(PriceRuntimeModel):
    revision: StrictInt = Field(ge=1)
    body_sha256: PriceSha256
    applied_at: AwareUtcDatetime
    scope_generation_id: PriceSha256 | None
    rule_rows_sha256: PriceSha256 | None
    member_rows_sha256: PriceSha256 | None
    recipient_policy_sha256: PriceSha256
    delivery_enabled: StrictBool
    available: StrictBool
    owner_targets: tuple[PriceAlertOwnerTargetCount, ...] = Field(default=(), max_length=32)


class PriceAlertProducerFact(PriceRuntimeModel):
    source: PriceAlertSourceDescriptor
    round: PriceRoundReceipt | None
    inspected_at: AwareUtcDatetime

    @model_validator(mode="after")
    def visible_round(self) -> Self:
        if self.round is not None and (
            self.round.evaluated_at > self.inspected_at
            or self.round.source_high_watermark != self.source.high_watermark
        ):
            raise ValueError("price producer round is future or differs from its actual source")
        return self


class PriceAlertRuntimeState(PriceRuntimeModel):
    snapshot_schema: Literal["price-alert-runtime-facts/v1"] = "price-alert-runtime-facts/v1"
    availability: Literal["ready", "unavailable", "not_running"]
    reason: StrictStr = Field(min_length=1, max_length=80)
    producer: PriceAlertProducerFact | None
    authority: PriceAlertAuthorityFact | None
    notifier_as_of: AwareUtcDatetime
    shadow: StrictBool
    rule_count: StrictInt = Field(ge=0, le=3200)
    event_count: StrictInt = Field(ge=0, le=640)
    attempt_count: StrictInt = Field(ge=0, le=6400)
    runtime_rows_sha256: PriceSha256
    event_rows_sha256: PriceSha256
    attempt_rows_sha256: PriceSha256

    @model_validator(mode="after")
    def complete_visible_state(self) -> Self:
        if (self.producer is not None and self.producer.inspected_at > self.notifier_as_of) or (
            self.authority is not None and self.authority.applied_at > self.notifier_as_of
        ):
            raise ValueError("price state contains future source evidence")
        if self.availability == "ready" and (
            self.producer is None
            or self.producer.round is None
            or self.producer.round.input_metadata.availability != "ready"
            or self.producer.round.decision_count != self.rule_count
            or self.authority is None
            or not self.authority.available
        ):
            raise ValueError("ready price state lacks a complete current round and authority")
        return self


class PriceAlertActualAttempt(PriceRuntimeModel):
    attempt_no: StrictInt = Field(ge=1, le=5)
    started_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    succeeded: StrictBool
    shadow: StrictBool
    row_sha256: PriceSha256

    @model_validator(mode="after")
    def ordered_time(self) -> Self:
        if self.started_at > self.completed_at:
            raise ValueError("price attempt completion precedes its start")
        return self


class PriceAlertUnknownFact(PriceRuntimeModel):
    attempt_no: StrictInt = Field(ge=1, le=5)
    observed_at: AwareUtcDatetime
    row_sha256: PriceSha256


class PriceAlertAttemptFact(PriceRuntimeModel):
    outbox_id: PriceSha256
    event_id: PriceSha256
    owner_id: OwnerId
    target: DeliveryTarget
    status: OutboxStatus
    attempt_count: StrictInt = Field(ge=0, le=5)
    updated_at: AwareUtcDatetime
    admission: PriceAlertSendAdmission | None
    cancellation: PriceAlertCancellationReceipt | None
    attempts: tuple[PriceAlertActualAttempt, ...] = Field(max_length=5)
    unknown: tuple[PriceAlertUnknownFact, ...] = Field(max_length=5)
    outbox_row_sha256: PriceSha256

    @model_validator(mode="after")
    def exact_receipts(self) -> Self:
        for values in (self.attempts, self.unknown):
            numbers = tuple(value.attempt_no for value in values)
            if numbers != tuple(sorted(set(numbers))) or any(
                number > self.attempt_count for number in numbers
            ):
                raise ValueError("price attempt evidence differs from the original count")
        if any(item.completed_at > self.updated_at for item in self.attempts) or any(
            item.observed_at > self.updated_at for item in self.unknown
        ):
            raise ValueError("price notification evidence follows the original outbox update")
        if self.admission is not None and (
            self.admission.outbox_id,
            self.admission.event_id,
            self.admission.owner_id,
            self.admission.target,
            self.admission.claimed_attempt_no,
        ) != (self.outbox_id, self.event_id, self.owner_id, self.target, self.attempt_count):
            raise ValueError("price admission belongs to another exact outbox attempt")
        if self.cancellation is not None and (
            self.cancellation.outbox_id,
            self.cancellation.event_id,
            self.cancellation.attempt_no_after,
        ) != (self.outbox_id, self.event_id, self.attempt_count):
            raise ValueError("price cancellation differs from its original outbox")
        return self


PriceAlertProducerFact.model_rebuild()
PriceAlertRuntimeState.model_rebuild()
PRICE_RUNTIME_TABLES = (
    "price_alert_runtime_state",
    "price_alert_runtime",
    "price_alert_runtime_event",
    "price_alert_runtime_attempt",
)


def _rows_sha(rows: object) -> str:
    def plain(value: object) -> object:
        if isinstance(value, Mapping):
            return {key: plain(item) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [plain(item) for item in value]
        return value

    normalized = plain(rows)
    if isinstance(normalized, list):
        # The publisher and reader may use different physical table orderings.
        normalized.sort(key=canonical_json_bytes)
    return sha256(canonical_json_bytes(normalized)).hexdigest()


def price_runtime_projections(
    connection: sqlite3.Connection,
    *,
    producer: PriceProducerRuntimeSnapshot | None,
    observed_at: datetime,
    shadow: bool,
) -> tuple[ServingProjectionPayload, ...]:
    from collections import Counter

    from rquant.price_alert_route import PriceAlertBusRoutedRecord
    from rquant.serving_read_models import (
        PAGE_PROJECTION_CONTRACTS,
        ServingProjectionInput,
        ServingProjectionPayload,
        _projection_json_bytes,
    )

    _require_delivery_tables(connection)
    now = normalize_aware_utc(observed_at)
    head = _head(connection)
    authority = (
        None
        if head is None
        else PriceAlertAuthorityFact(
            revision=head.authority_revision,
            body_sha256=head.sha256,
            applied_at=head.applied_at,
            scope_generation_id=None
            if type(head.scope) is not PriceAlertScopeSnapshot
            else head.scope.generation_id,
            rule_rows_sha256=getattr(head.scope, "rule_rows_sha256", None),
            member_rows_sha256=getattr(head.scope, "member_rows_sha256", None),
            recipient_policy_sha256=head.policy.sha256,
            delivery_enabled=head.delivery_enabled,
            available=type(head.scope) is PriceAlertScopeSnapshot,
            owner_targets=tuple(
                PriceAlertOwnerTargetCount(owner_id=item.owner_id, target_count=len(item.targets))
                for item in head.policy.owners
            ),
        )
    )
    availability, reason = (
        ("not_running", "not_started")
        if producer is None or producer.round is None
        else (
            ("ready", "evaluated")
            if producer.round.input_metadata.availability == "ready"
            else ("unavailable", producer.round.input_metadata.reason)
        )
    )
    if head is None or head.applied_at > now:
        availability, reason = "unavailable", "authority_unavailable"
    elif type(head.scope) is not PriceAlertScopeSnapshot:
        availability, reason = "unavailable", "scope_unavailable"
    rules = []
    if producer is not None:
        for fact in producer.rules:
            rules.append(
                {
                    "owner_id": fact.owner_id,
                    "rule_id": fact.rule_id,
                    "body_json": fact.wire_bytes().decode(),
                }
            )
    event_rows = connection.execute(
        (
            "SELECT global_sequence FROM (SELECT e.global_sequence, "
            "ROW_NUMBER() OVER(PARTITION BY "
            "json_extract(e.payload_json,'$.owner_id') ORDER BY "
            "e.global_sequence DESC) AS n FROM signal_envelope e JOIN "
            "price_alert_route_receipt r ON e.signal_id=r.event_id WHERE "
            "e.received_at<=?) WHERE n<=20 ORDER BY global_sequence "
            "LIMIT 641"
        ),
        (_encode_time(now),),
    ).fetchall()
    if len(event_rows) > 640:
        raise ValueError("price serving owner/event capacity exceeds the complete domain")
    events, attempts = [], []
    for raw in event_rows:
        record = notification_record(connection, raw["global_sequence"], routed=True)
        if (
            type(record) is not PriceAlertBusRoutedRecord
            or max(record.event.available_at, record.received_at, record.receipt.routed_at) > now
        ):
            raise ValueError("price serving source receipt is incomplete or future")
        events.append(
            {
                "owner_id": record.event.owner_id,
                "event_id": record.event_id,
                "global_sequence": record.global_sequence,
                "body_json": record.wire_bytes().decode(),
            }
        )
        outboxes = connection.execute(
            "SELECT * FROM delivery_outbox WHERE signal_id=? ORDER BY outbox_id LIMIT 3",
            (record.event_id,),
        ).fetchall()
        if len(outboxes) != record.receipt.target_count:
            raise ValueError("price serving outboxes differ from the exact original route")
        for row in outboxes:
            outbox = SignalBusStore._outbox_from_row(row)
            if outbox.target not in record.receipt.targets or outbox.updated_at > now:
                raise ValueError("price serving target or update evidence differs")
            actual = connection.execute(
                "SELECT * FROM delivery_attempt WHERE outbox_id=? ORDER BY attempt_no LIMIT 6",
                (outbox.outbox_id,),
            ).fetchall()
            uncertain = connection.execute(
                "SELECT * FROM delivery_unknown WHERE outbox_id=? ORDER BY attempt_no LIMIT 6",
                (outbox.outbox_id,),
            ).fetchall()
            if max(len(actual), len(uncertain)) > 5:
                raise ValueError(
                    "price serving attempts exceed the installed five-attempt contract"
                )
            checked = tuple(SignalBusStore._attempt_from_row(item) for item in actual)
            if any(item.completed_at > now for item in checked) or any(
                _require_time(item["observed_at"]) > now for item in uncertain
            ):
                raise ValueError("price serving actual attempt is in the future")
            cancellation = connection.execute(
                "SELECT body_json FROM price_alert_unadmitted_cancel WHERE outbox_id=?",
                (outbox.outbox_id,),
            ).fetchone()
            fact = PriceAlertAttemptFact(
                outbox_id=outbox.outbox_id,
                event_id=record.event_id,
                owner_id=record.event.owner_id,
                target=outbox.target,
                status=outbox.status,
                attempt_count=outbox.attempt_count,
                updated_at=outbox.updated_at,
                admission=_admission(connection, outbox.outbox_id, outbox.attempt_count),
                cancellation=None
                if cancellation is None
                else PriceAlertCancellationReceipt.model_validate_json(cancellation[0]),
                attempts=tuple(
                    PriceAlertActualAttempt(
                        attempt_no=item.attempt_no,
                        started_at=item.started_at,
                        completed_at=item.completed_at,
                        succeeded=item.success,
                        shadow=bool(
                            item.provider_receipt and item.provider_receipt.startswith("shadow:")
                        ),
                        row_sha256=_rows_sha(dict(raw)),
                    )
                    for item, raw in zip(checked, actual, strict=True)
                ),
                unknown=tuple(
                    PriceAlertUnknownFact(
                        attempt_no=item["attempt_no"],
                        observed_at=_require_time(item["observed_at"]),
                        row_sha256=_rows_sha(dict(item)),
                    )
                    for item in uncertain
                ),
                outbox_row_sha256=_rows_sha(dict(row)),
            )
            attempts.append(
                {
                    "owner_id": fact.owner_id,
                    "event_id": fact.event_id,
                    "outbox_id": fact.outbox_id,
                    "body_json": fact.wire_bytes().decode(),
                }
            )
    if len(set(row["owner_id"] for row in events)) > 32 or any(
        count > 20 for count in Counter(row["owner_id"] for row in events).values()
    ):
        raise ValueError("price serving owner history exceeds the complete domain")
    compact = None if producer is None else producer.model_copy(update={"rules": ()})
    # State keeps the signed round metadata; per-rule bodies live in the dedicated table.
    compact_body = None if compact is None else compact.model_dump(mode="json", exclude={"rules"})
    if compact_body is not None:
        compact_body["round"] = (
            None if compact.round is None else compact.round.model_dump(mode="json")
        )
    state_body = dict(
        snapshot_schema="price-alert-runtime-facts/v1",
        availability=availability,
        reason=reason,
        producer=compact_body,
        authority=None if authority is None else authority.model_dump(mode="json"),
        notifier_as_of=_encode_time(now),
        shadow=shadow,
        rule_count=len(rules),
        event_count=len(events),
        attempt_count=len(attempts),
        runtime_rows_sha256=_rows_sha(rules),
        event_rows_sha256=_rows_sha(events),
        attempt_rows_sha256=_rows_sha(attempts),
    )
    state_payload = (
        PriceAlertRuntimeState.model_validate_json(canonical_json_bytes(state_body))
        .wire_bytes()
        .decode()
    )
    payloads = tuple(
        ServingProjectionPayload(table_name=name, available_at=now, rows=tuple(rows))
        for name, rows in zip(
            PRICE_RUNTIME_TABLES,
            ([{"snapshot_key": "current", "body_json": state_payload}], rules, events, attempts),
            strict=True,
        )
    )
    # Every owner generation is exactly 64 hex digits; include its real wire
    # fields before the original Serving binder adds them to these payloads.
    size = sum(
        _projection_json_bytes(
            ServingProjectionInput.bind(
                value,
                owner_dataset_id=PAGE_PROJECTION_CONTRACTS[value.table_name].owner_dataset_id,
                owner_generation_id="0" * 64,
            )
        )
        for value in payloads
    )
    if size > 2 * 1024 * 1024:
        raise ValueError("price serving complete domain exceeds 2 MiB")
    validate_price_runtime_projections({value.table_name: value for value in payloads})
    return payloads


def unavailable_price_runtime_projections(
    *, observed_at: datetime, shadow: bool
) -> tuple[ServingProjectionPayload, ...]:
    from rquant.serving_read_models import ServingProjectionPayload

    now = normalize_aware_utc(observed_at)
    state = PriceAlertRuntimeState(
        availability="unavailable",
        reason="facts_unavailable",
        producer=None,
        authority=None,
        notifier_as_of=now,
        shadow=shadow,
        rule_count=0,
        event_count=0,
        attempt_count=0,
        runtime_rows_sha256=_rows_sha(()),
        event_rows_sha256=_rows_sha(()),
        attempt_rows_sha256=_rows_sha(()),
    )
    return tuple(
        ServingProjectionPayload(
            table_name=name,
            available_at=now,
            rows=({"snapshot_key": "current", "body_json": state.wire_bytes().decode()},)
            if name == PRICE_RUNTIME_TABLES[0]
            else (),
        )
        for name in PRICE_RUNTIME_TABLES
    )


def validate_price_runtime_projections(
    projections: Mapping[str, ServingProjectionPayload],
) -> None:
    if not set(PRICE_RUNTIME_TABLES) <= set(projections):
        raise ValueError("price runtime domain is incomplete")
    state_rows = projections[PRICE_RUNTIME_TABLES[0]].rows
    if len(state_rows) != 1 or state_rows[0]["snapshot_key"] != "current":
        raise ValueError("price runtime state is incomplete")
    state = PriceAlertRuntimeState.model_validate_json(state_rows[0]["body_json"])
    for name, count, digest, model in zip(
        PRICE_RUNTIME_TABLES[1:],
        (state.rule_count, state.event_count, state.attempt_count),
        (state.runtime_rows_sha256, state.event_rows_sha256, state.attempt_rows_sha256),
        (
            PriceRuntimeRuleFact,
            __import__(
                "rquant.price_alert_route", fromlist=["PriceAlertBusRoutedRecord"]
            ).PriceAlertBusRoutedRecord,
            PriceAlertAttemptFact,
        ),
        strict=True,
    ):
        rows = projections[name].rows
        if len(rows) != count or _rows_sha(rows) != digest:
            raise ValueError("price runtime rows differ from the exact complete receipt")
        for row in rows:
            value = model.model_validate_json(row["body_json"])
            owner = getattr(value, "owner_id", None) or value.event.owner_id
            if (
                owner != row["owner_id"]
                or (name == PRICE_RUNTIME_TABLES[1] and value.rule_id != row["rule_id"])
                or (name != PRICE_RUNTIME_TABLES[1] and value.event_id != row["event_id"])
                or (name == PRICE_RUNTIME_TABLES[3] and value.outbox_id != row["outbox_id"])
            ):
                raise ValueError("price runtime domain row identity differs")


def cancel_price_unadmitted(
    store: NotificationStateStore,
    outbox_id: str,
    *,
    expected_revision: int,
    cancelled_at: datetime,
    worker_id: str | None = None,
) -> PriceAlertCancellationReceipt | None:
    now = normalize_aware_utc(cancelled_at)
    with store._write_transaction() as connection:
        head = _head(connection)
        if head is None or head.authority_revision != expected_revision:
            raise PriceAlertAuthorityConflict(
                "price authority revision changed before cancellation"
            )
        row = connection.execute(
            "SELECT * FROM delivery_outbox WHERE outbox_id=?", (outbox_id,)
        ).fetchone()
        if row is None:
            raise KeyError("price cancellation outbox is missing")
        record = store._outbox_from_row(row)
        if record.status not in {OutboxStatus.PENDING, OutboxStatus.RETRY, OutboxStatus.LEASED}:
            return None
        if record.status is OutboxStatus.RETRY:
            # A completed rejection cannot authorize the next attempt.
            previous_attempt = connection.execute(
                "SELECT * FROM delivery_attempt WHERE outbox_id=? AND attempt_no=?",
                (outbox_id, record.attempt_count),
            ).fetchone()
            unknown = connection.execute(
                "SELECT 1 FROM delivery_unknown WHERE outbox_id=? LIMIT 1", (outbox_id,)
            ).fetchone()
            if (
                unknown is not None
                or previous_attempt is None
                or store._attempt_from_row(previous_attempt).success
            ):
                return None
        else:
            if _admission(connection, outbox_id, record.attempt_count) is not None:
                return None
            evidence = connection.execute(
                (
                    "SELECT 1 FROM delivery_unknown WHERE outbox_id=? AND "
                    "attempt_no=? UNION ALL SELECT 1 FROM delivery_attempt WHERE "
                    "outbox_id=? AND attempt_no=? LIMIT 1"
                ),
                (outbox_id, record.attempt_count, outbox_id, record.attempt_count),
            ).fetchone()
            if evidence is not None:
                return None
        event = notification_record(connection, record.signal_id)
        if type(event) is not PriceAlertBusEventRecord:
            raise TypeError("price cancellation cannot touch a strategy event")
        reason = _valid_current_event(head, event, record.target, now)
        if reason is None:
            return None
        after = record.attempt_count
        if record.status is OutboxStatus.LEASED:
            if worker_id is None:
                raise ValueError("price leased cancellation requires the actual worker")
            store._verify_lease(
                row, worker_id=worker_id, attempt_no=record.attempt_count, completed_at=now
            )
            after -= 1
        receipt = PriceAlertCancellationReceipt(
            outbox_id=outbox_id,
            event_id=record.signal_id,
            authority_revision=head.authority_revision,
            attempt_no_before=record.attempt_count,
            attempt_no_after=after,
            reason=reason,
            cancelled_at=now,
        )
        connection.execute(
            "INSERT INTO price_alert_unadmitted_cancel VALUES(?,?)",
            (outbox_id, receipt.wire_bytes()),
        )
        connection.execute(
            (
                "UPDATE delivery_outbox SET "
                "status=?,attempt_count=?,next_attempt_at=NULL,lease_owner=NU"
                "LL,lease_started_at=NULL,lease_until=NULL,last_error=?,updat"
                "ed_at=? WHERE outbox_id=?"
            ),
            (
                OutboxStatus.DEAD_LETTER.value,
                after,
                "price event cancelled before admission",
                _encode_time(now),
                outbox_id,
            ),
        )
        store._price_alert_failpoint("before_cancel_commit")
        store._before_commit(connection)
    return receipt
