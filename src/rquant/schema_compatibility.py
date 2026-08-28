"""Frozen contracts and fail-closed decisions for runtime schema evolution."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Annotated

from pydantic import (
    Field,
    JsonValue,
    StrictBool,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

MAX_SCHEMA_VERSION = 2**31 - 1
SchemaVersion = Annotated[int, Field(strict=True, ge=1, le=MAX_SCHEMA_VERSION)]
Revision = Annotated[int, Field(strict=True, ge=0, le=2**63 - 1)]
PositiveSeconds = Annotated[int, Field(strict=True, ge=1, le=86_400)]
ObservationSeconds = Annotated[int, Field(strict=True, ge=60, le=31 * 86_400)]
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
_GENESIS_HASH = "0" * 64
_REGISTRY_SCHEMA_VERSION = 2


class RolloutPhase(StrEnum):
    PREPARE = "prepare"
    DUAL_WRITE = "dual_write"
    CONSUMER_ACK = "consumer_ack"
    CUTOVER = "cutover"
    RETIRE = "retire"
    ROLLBACK = "rollback"

    # Source compatibility for the pre-v2 in-process API. Persisted v1 stores are
    # deliberately not migrated implicitly.
    PREPARE_OPTIONAL = "prepare"
    DUAL_READ = "consumer_ack"
    REQUIRE_NEW = "cutover"
    RETIRE_OLD = "retire"


class CompatibilityOutcome(StrEnum):
    COMPATIBLE = "compatible"
    DEGRADED = "degraded"
    INCOMPATIBLE = "incompatible"


class UnknownFieldPolicy(StrEnum):
    ALLOW = "allow"
    FORBID = "forbid"


class SchemaRequiredTransition(RuntimeContractModel):
    version: SchemaVersion
    required: bool


class SchemaField(RuntimeContractModel):
    name: str = Field(min_length=1)
    type_name: str = Field(min_length=1)
    required: bool
    introduced_in: SchemaVersion
    deprecated_in: SchemaVersion | None = None
    removed_in: SchemaVersion | None = None
    nullable: bool = False
    semantic_fingerprint: Sha256 | None = None
    required_history: tuple[SchemaRequiredTransition, ...] = ()

    @model_validator(mode="after")
    def validate_version_chronology(self) -> SchemaField:
        if self.deprecated_in is not None and self.deprecated_in < self.introduced_in:
            raise ValueError("deprecated_in cannot precede introduced_in")
        if self.removed_in is not None and self.deprecated_in is None:
            raise ValueError("removed fields require a prior deprecated_in version")
        if self.removed_in is not None and self.removed_in <= self.introduced_in:
            raise ValueError("removed_in must be later than introduced_in")
        if (
            self.deprecated_in is not None
            and self.removed_in is not None
            and self.removed_in <= self.deprecated_in
        ):
            raise ValueError("removed_in must be later than deprecated_in")
        history = self.required_history
        if not history:
            history = (
                SchemaRequiredTransition(
                    version=self.introduced_in,
                    required=self.required,
                ),
            )
            object.__setattr__(self, "required_history", history)
        versions = tuple(item.version for item in history)
        if versions != tuple(sorted(set(versions))):
            raise ValueError("required history versions must be unique and increasing")
        if history[0].version != self.introduced_in:
            raise ValueError("required history must start at introduced_in")
        if self.removed_in is not None and any(item.version >= self.removed_in for item in history):
            raise ValueError("required history cannot reach or follow removed_in")
        if history[-1].required is not self.required:
            raise ValueError("required must match the latest required history state")
        if self.removed_in is not None and self.required:
            raise ValueError("a removed field must be optional before retirement")
        return self

    def is_available_in(self, version: int) -> bool:
        return self.introduced_in <= version and (
            self.removed_in is None or version < self.removed_in
        )

    def is_required_in(self, version: int) -> bool:
        states = [item.required for item in self.required_history if item.version <= version]
        return states[-1] if states else False

    @property
    def effective_semantic_fingerprint(self) -> str:
        return self.semantic_fingerprint or canonical_sha256(
            {
                "type_name": self.type_name,
                "nullable": self.nullable,
            }
        )


class SchemaDeclaration(RuntimeContractModel):
    dataset_id: str = Field(min_length=1)
    schema_name: str = Field(min_length=1)
    min_reader_version: SchemaVersion
    current_version: SchemaVersion
    fields: tuple[SchemaField, ...]
    producer_commit: CommitSha

    @field_validator("fields")
    @classmethod
    def validate_unique_fields(
        cls,
        values: tuple[SchemaField, ...],
    ) -> tuple[SchemaField, ...]:
        names = tuple(field.name for field in values)
        if len(names) != len(set(names)):
            raise ValueError("schema field names must be unique")
        return values

    @model_validator(mode="after")
    def validate_version_bounds(self) -> SchemaDeclaration:
        if self.min_reader_version > self.current_version:
            raise ValueError("min_reader_version cannot exceed current_version")
        future_fields = sorted(
            field.name for field in self.fields if field.introduced_in > self.current_version
        )
        if future_fields:
            names = ", ".join(future_fields)
            raise ValueError(f"field introduced_in cannot exceed current_version: {names}")
        return self

    @property
    def semantic_fingerprint(self) -> str:
        return canonical_sha256(
            {
                "dataset_id": self.dataset_id,
                "schema_name": self.schema_name,
                "min_reader_version": self.min_reader_version,
                "current_version": self.current_version,
                "fields": tuple(
                    field.model_dump(mode="python")
                    for field in sorted(self.fields, key=lambda item: item.name)
                ),
            }
        )

    @property
    def schema_fingerprint(self) -> str:
        return canonical_sha256(
            {
                "semantic_fingerprint": self.semantic_fingerprint,
                "producer_commit": self.producer_commit,
            }
        )

    def available_fields(self) -> dict[str, SchemaField]:
        return {
            field.name: field
            for field in self.fields
            if field.is_available_in(self.current_version)
        }


class ConsumerFieldCapability(RuntimeContractModel):
    name: str = Field(min_length=1)
    type_name: str = Field(min_length=1)
    nullable: bool
    semantic_fingerprint: Sha256 | None = None


class ConsumerSchemaRequirement(RuntimeContractModel):
    consumer_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    min_version: SchemaVersion
    max_version: SchemaVersion
    required_fields: tuple[str, ...]
    optional_fields: tuple[str, ...]
    field_capabilities: tuple[ConsumerFieldCapability, ...]
    unknown_field_policy: UnknownFieldPolicy = UnknownFieldPolicy.ALLOW

    @field_validator("required_fields", "optional_fields")
    @classmethod
    def validate_unique_fields(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("consumer field names cannot be empty")
        if len(values) != len(set(values)):
            raise ValueError("consumer field names must be unique")
        return values

    @model_validator(mode="after")
    def validate_field_sets_and_versions(self) -> ConsumerSchemaRequirement:
        if self.max_version < self.min_version:
            raise ValueError("max_version cannot precede min_version")
        overlap = sorted(set(self.required_fields) & set(self.optional_fields))
        if overlap:
            raise ValueError("required_fields and optional_fields must be disjoint")
        capability_names = tuple(item.name for item in self.field_capabilities)
        if len(capability_names) != len(set(capability_names)):
            raise ValueError("consumer field capabilities must be unique")
        if set(capability_names) != set(self.supported_fields):
            raise ValueError("every supported consumer field requires one capability")
        return self

    @property
    def supported_fields(self) -> frozenset[str]:
        return frozenset((*self.required_fields, *self.optional_fields))

    @property
    def capabilities(self) -> dict[str, ConsumerFieldCapability]:
        return {item.name: item for item in self.field_capabilities}


class ProducerSchemaCapability(RuntimeContractModel):
    producer_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    min_writable_version: SchemaVersion
    max_writable_version: SchemaVersion
    writable_fields: tuple[str, ...]
    field_capabilities: tuple[ConsumerFieldCapability, ...]

    @model_validator(mode="after")
    def validate_capability(self) -> ProducerSchemaCapability:
        if self.max_writable_version < self.min_writable_version:
            raise ValueError("producer max writable version cannot precede minimum")
        if tuple(sorted(set(self.writable_fields))) != self.writable_fields:
            raise ValueError("producer writable fields must be unique and sorted")
        names = tuple(item.name for item in self.field_capabilities)
        if tuple(sorted(set(names))) != names or names != self.writable_fields:
            raise ValueError("producer field capabilities must match writable fields")
        return self


class CompatibilityDecision(RuntimeContractModel):
    outcome: CompatibilityOutcome
    reasons: tuple[str, ...]
    readable_version: SchemaVersion | None = None

    @model_validator(mode="after")
    def validate_outcome(self) -> CompatibilityDecision:
        if self.outcome is CompatibilityOutcome.COMPATIBLE and self.reasons:
            raise ValueError("compatible decisions cannot contain reasons")
        if self.outcome is not CompatibilityOutcome.COMPATIBLE and not self.reasons:
            raise ValueError("non-compatible decisions require reasons")
        if self.outcome is CompatibilityOutcome.INCOMPATIBLE and self.readable_version is not None:
            raise ValueError("incompatible decisions cannot expose a readable version")
        if self.outcome is not CompatibilityOutcome.INCOMPATIBLE and self.readable_version is None:
            raise ValueError("readable decisions require a readable version")
        return self


class SchemaParticipant(RuntimeContractModel):
    participant_id: str = Field(min_length=1)
    contract_fingerprint: Sha256


class ProductionConsumerCapability(RuntimeContractModel):
    consumer_id: str = Field(min_length=1)
    service_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    contract_fingerprint: Sha256
    code_commit: CommitSha
    min_readable_schema_version: SchemaVersion
    max_readable_schema_version: SchemaVersion
    required_fields: tuple[str, ...]
    requires_serving_generation_ack: StrictBool = False

    @field_validator("required_fields")
    @classmethod
    def validate_required_fields(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("required consumer fields cannot be empty")
        if tuple(sorted(set(values))) != values:
            raise ValueError("required consumer fields must be unique and sorted")
        return values

    @model_validator(mode="after")
    def validate_version_range(self) -> ProductionConsumerCapability:
        if self.max_readable_schema_version < self.min_readable_schema_version:
            raise ValueError("consumer maximum readable version cannot precede minimum")
        return self


class ProductionConsumerRegistry(RuntimeContractModel):
    registry_id: str = Field(min_length=1)
    consumers: tuple[ProductionConsumerCapability, ...]

    @model_validator(mode="after")
    def validate_registry(self) -> ProductionConsumerRegistry:
        identities = tuple(item.consumer_id for item in self.consumers)
        services = tuple(item.service_id for item in self.consumers)
        if not identities:
            raise ValueError("production consumer registry cannot be empty")
        if tuple(sorted(set(identities))) != identities:
            raise ValueError("production consumer ids must be unique and sorted")
        if len(services) != len(set(services)):
            raise ValueError("production consumer service ids must be unique")
        return self

    @property
    def registry_fingerprint(self) -> str:
        return canonical_sha256(
            {
                "registry_id": self.registry_id,
                "consumers": self.consumers,
            }
        )

    def for_dataset(self, dataset_id: str) -> dict[str, ProductionConsumerCapability]:
        return {
            consumer.consumer_id: consumer
            for consumer in self.consumers
            if consumer.dataset_id == dataset_id
        }


class ConsumerCapabilityReceipt(RuntimeContractModel):
    consumer_id: str = Field(min_length=1)
    service_id: str = Field(min_length=1)
    code_commit: CommitSha
    dataset_id: str = Field(min_length=1)
    min_readable_schema_version: SchemaVersion
    max_readable_schema_version: SchemaVersion
    required_fields: tuple[str, ...]
    serving_physical_schema_fingerprint: Sha256
    observed_generation_id: Sha256
    serving_generation_id: Sha256 | None = None
    available_at: AwareUtcDatetime

    @field_validator("required_fields")
    @classmethod
    def validate_required_fields(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("required consumer fields cannot be empty")
        if tuple(sorted(set(values))) != values:
            raise ValueError("required consumer fields must be unique and sorted")
        return values

    @model_validator(mode="after")
    def validate_version_range(self) -> ConsumerCapabilityReceipt:
        if self.max_readable_schema_version < self.min_readable_schema_version:
            raise ValueError("consumer maximum readable version cannot precede minimum")
        return self

    @property
    def receipt_fingerprint(self) -> str:
        return canonical_sha256(self)


class DualWriteConsistencyEvidence(RuntimeContractModel):
    generation_id: Sha256
    old_declaration_fingerprint: Sha256
    new_declaration_fingerprint: Sha256
    old_values_fingerprint: Sha256
    new_values_fingerprint: Sha256
    shared_values_fingerprint: Sha256
    observed_at: AwareUtcDatetime

    @property
    def evidence_fingerprint(self) -> str:
        return canonical_sha256(self)


class DualWriteValueRecord(RuntimeContractModel):
    write_id: Sha256
    evidence: DualWriteConsistencyEvidence
    old_values: Mapping[str, JsonValue]
    new_values: Mapping[str, JsonValue]

    @field_validator("old_values", "new_values")
    @classmethod
    def freeze_values(cls, values: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        return MappingProxyType(dict(sorted(values.items())))

    @field_serializer("old_values", "new_values")
    def serialize_values(self, values: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        return dict(values)

    @model_validator(mode="after")
    def validate_identity(self) -> DualWriteValueRecord:
        expected = canonical_sha256(
            {
                "evidence": self.evidence,
                "old_values": dict(self.old_values),
                "new_values": dict(self.new_values),
            }
        )
        if self.write_id != expected:
            raise ValueError("dual-write value record identity mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        evidence: DualWriteConsistencyEvidence,
        old_values: Mapping[str, JsonValue],
        new_values: Mapping[str, JsonValue],
    ) -> DualWriteValueRecord:
        values = {
            "evidence": evidence,
            "old_values": dict(old_values),
            "new_values": dict(new_values),
        }
        return cls(**values, write_id=canonical_sha256(values))


class LiveSchemaRolloutPlan(RuntimeContractModel):
    dataset_id: str = Field(min_length=1)
    old_declaration_fingerprint: Sha256
    new_declaration_fingerprint: Sha256
    producers: tuple[SchemaParticipant, ...]
    consumers: tuple[SchemaParticipant, ...]
    production_consumer_registry_fingerprint: Sha256 | None = None
    serving_physical_schema_fingerprint: Sha256 | None = None
    target_generation_id: Sha256 | None = None
    target_schema_version: SchemaVersion | None = None
    consumer_ack_max_age_seconds: PositiveSeconds = 300
    retire_observation_seconds: ObservationSeconds = 86_400
    started_at: AwareUtcDatetime
    deadline: AwareUtcDatetime

    @field_validator("producers", "consumers")
    @classmethod
    def validate_registries(
        cls,
        values: tuple[SchemaParticipant, ...],
    ) -> tuple[SchemaParticipant, ...]:
        identities = tuple(item.participant_id for item in values)
        if len(identities) != len(set(identities)):
            raise ValueError("schema participant registry identities must be unique")
        return tuple(sorted(values, key=lambda item: item.participant_id))

    @model_validator(mode="after")
    def validate_rollout_prerequisites(self) -> LiveSchemaRolloutPlan:
        if self.old_declaration_fingerprint == self.new_declaration_fingerprint:
            raise ValueError("old and new declaration fingerprints must differ")
        if self.deadline <= self.started_at:
            raise ValueError("deadline must be later than started_at")
        if not self.producers:
            raise ValueError("producer registry cannot be empty")
        if not self.consumers:
            raise ValueError("consumer registry cannot be empty")
        identities = [item.participant_id for item in (*self.producers, *self.consumers)]
        if len(identities) != len(set(identities)):
            raise ValueError("producer and consumer registries must be disjoint")
        strict_values = (
            self.production_consumer_registry_fingerprint,
            self.serving_physical_schema_fingerprint,
            self.target_generation_id,
            self.target_schema_version,
        )
        if any(value is not None for value in strict_values) and any(
            value is None for value in strict_values
        ):
            raise ValueError("production rollout capability bindings must be all present")
        return self

    @property
    def plan_id(self) -> str:
        return canonical_sha256(
            {
                "dataset_id": self.dataset_id,
                "old_declaration_fingerprint": self.old_declaration_fingerprint,
                "new_declaration_fingerprint": self.new_declaration_fingerprint,
                "producers": tuple(sorted(self.producers, key=lambda item: item.participant_id)),
                "consumers": tuple(sorted(self.consumers, key=lambda item: item.participant_id)),
                "production_consumer_registry_fingerprint": (
                    self.production_consumer_registry_fingerprint
                ),
                "serving_physical_schema_fingerprint": (self.serving_physical_schema_fingerprint),
                "target_generation_id": self.target_generation_id,
                "target_schema_version": self.target_schema_version,
                "consumer_ack_max_age_seconds": self.consumer_ack_max_age_seconds,
                "retire_observation_seconds": self.retire_observation_seconds,
                "started_at": self.started_at,
                "deadline": self.deadline,
            }
        )


class SchemaRolloutState(RuntimeContractModel):
    plan_id: Sha256
    phase: RolloutPhase
    revision: Revision
    updated_at: AwareUtcDatetime
    authority_declaration_fingerprint: Sha256
    new_data_preserved: bool = True


class SchemaRolloutReceipt(RuntimeContractModel):
    plan_id: Sha256
    revision: Revision
    operation_id: str = Field(min_length=1, max_length=256)
    event_type: str = Field(min_length=1, max_length=64)
    request_hash: Sha256
    payload_json: str = Field(min_length=2)
    previous_hash: Sha256
    event_hash: Sha256
    recorded_at: AwareUtcDatetime


_FORWARD_PHASES = (
    RolloutPhase.PREPARE,
    RolloutPhase.DUAL_WRITE,
    RolloutPhase.CONSUMER_ACK,
    RolloutPhase.CUTOVER,
    RolloutPhase.RETIRE,
)


def _json_payload(value: object) -> str:
    if isinstance(value, RuntimeContractModel):
        value = value.model_dump(mode="json")
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _decode_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return normalize_aware_utc(parsed)


def _event_hash(
    *,
    plan_id: str,
    revision: int,
    operation_id: str,
    event_type: str,
    request_hash: str,
    payload_json: str,
    previous_hash: str,
    recorded_at: AwareUtcDatetime,
) -> str:
    return canonical_sha256(
        {
            "plan_id": plan_id,
            "revision": revision,
            "operation_id": operation_id,
            "event_type": event_type,
            "request_hash": request_hash,
            "payload_json": payload_json,
            "previous_hash": previous_hash,
            "recorded_at": normalize_aware_utc(recorded_at),
        }
    )


def validate_dual_write_values(
    *,
    old_declaration: SchemaDeclaration,
    new_declaration: SchemaDeclaration,
    old_values: Mapping[str, object],
    new_values: Mapping[str, object],
    generation_id: str,
    observed_at: AwareUtcDatetime,
) -> DualWriteConsistencyEvidence:
    """Prove that both shapes encode the same shared values for one generation."""

    if old_declaration.dataset_id != new_declaration.dataset_id:
        raise ValueError("dual-write declarations target different datasets")
    old_fields = old_declaration.available_fields()
    new_fields = new_declaration.available_fields()
    unknown_old = sorted(set(old_values) - set(old_fields))
    unknown_new = sorted(set(new_values) - set(new_fields))
    if unknown_old or unknown_new:
        raise ValueError(
            "dual-write payload contains undeclared fields: "
            + ", ".join((*unknown_old, *unknown_new))
        )
    for name, field in old_fields.items():
        if field.required and name not in old_values:
            raise ValueError(f"old dual-write payload is missing required field {name}")
    for name, field in new_fields.items():
        if field.required and name not in new_values:
            raise ValueError(f"new dual-write payload is missing required field {name}")
    shared_names = tuple(sorted(set(old_fields) & set(new_fields)))
    for name in shared_names:
        if name not in old_values or name not in new_values:
            if old_fields[name].required or new_fields[name].required:
                raise ValueError(f"shared required field {name} is absent from one shape")
            continue
        if canonical_sha256(old_values[name]) != canonical_sha256(new_values[name]):
            raise ValueError(f"shared field {name} differs between dual-write shapes")
    shared_values = {
        name: new_values[name] for name in shared_names if name in old_values and name in new_values
    }
    return DualWriteConsistencyEvidence(
        generation_id=generation_id,
        old_declaration_fingerprint=old_declaration.schema_fingerprint,
        new_declaration_fingerprint=new_declaration.schema_fingerprint,
        old_values_fingerprint=canonical_sha256(dict(old_values)),
        new_values_fingerprint=canonical_sha256(dict(new_values)),
        shared_values_fingerprint=canonical_sha256(shared_values),
        observed_at=observed_at,
    )


class SchemaRolloutStore:
    """Persist a hash-chained schema rollout with trusted consumer receipts."""

    def __init__(
        self,
        path: Path,
        *,
        production_consumer_registry: ProductionConsumerRegistry | None = None,
    ) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.production_consumer_registry = production_consumer_registry
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            existing = {
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if "schema_rollout" in existing and "schema_registry_meta" not in existing:
                raise RuntimeError(
                    "legacy v1 schema rollout registry requires explicit migration; fail closed"
                )
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_registry_meta (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    registry_schema_version INTEGER NOT NULL
                ) STRICT;
                CREATE TABLE IF NOT EXISTS schema_rollout (
                    plan_id TEXT PRIMARY KEY,
                    plan_json TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    authority_declaration_fingerprint TEXT NOT NULL,
                    new_data_preserved INTEGER NOT NULL CHECK (new_data_preserved IN (0, 1)),
                    last_event_hash TEXT NOT NULL
                ) STRICT;
                CREATE TABLE IF NOT EXISTS schema_rollout_event (
                    plan_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    operation_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    PRIMARY KEY (plan_id, revision),
                    UNIQUE (plan_id, operation_id),
                    FOREIGN KEY (plan_id) REFERENCES schema_rollout(plan_id)
                ) STRICT;
                CREATE TABLE IF NOT EXISTS schema_consumer_capability_receipt (
                    plan_id TEXT NOT NULL,
                    consumer_id TEXT NOT NULL,
                    receipt_json TEXT NOT NULL,
                    receipt_fingerprint TEXT NOT NULL,
                    recorded_revision INTEGER NOT NULL,
                    PRIMARY KEY (plan_id, consumer_id),
                    FOREIGN KEY (plan_id) REFERENCES schema_rollout(plan_id)
                ) STRICT;
                CREATE TABLE IF NOT EXISTS schema_dual_write_evidence (
                    plan_id TEXT NOT NULL,
                    generation_id TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    evidence_fingerprint TEXT NOT NULL,
                    recorded_revision INTEGER NOT NULL,
                    PRIMARY KEY (plan_id, generation_id),
                    FOREIGN KEY (plan_id) REFERENCES schema_rollout(plan_id)
                ) STRICT;
                CREATE TABLE IF NOT EXISTS schema_dual_write_value (
                    plan_id TEXT NOT NULL,
                    write_id TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    recorded_revision INTEGER NOT NULL,
                    PRIMARY KEY (plan_id, write_id),
                    FOREIGN KEY (plan_id) REFERENCES schema_rollout(plan_id)
                ) STRICT;
                CREATE TRIGGER IF NOT EXISTS schema_rollout_event_no_update
                BEFORE UPDATE ON schema_rollout_event
                BEGIN SELECT RAISE(ABORT, 'schema rollout event is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS schema_rollout_event_no_delete
                BEFORE DELETE ON schema_rollout_event
                BEGIN SELECT RAISE(ABORT, 'schema rollout event is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS schema_consumer_receipt_no_update
                BEFORE UPDATE ON schema_consumer_capability_receipt
                BEGIN SELECT RAISE(ABORT, 'consumer receipt is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS schema_consumer_receipt_no_delete
                BEFORE DELETE ON schema_consumer_capability_receipt
                BEGIN SELECT RAISE(ABORT, 'consumer receipt is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS schema_dual_write_evidence_no_update
                BEFORE UPDATE ON schema_dual_write_evidence
                BEGIN SELECT RAISE(ABORT, 'dual-write evidence is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS schema_dual_write_evidence_no_delete
                BEFORE DELETE ON schema_dual_write_evidence
                BEGIN SELECT RAISE(ABORT, 'dual-write evidence is append-only'); END;
                CREATE TRIGGER IF NOT EXISTS schema_dual_write_value_no_update
                BEFORE UPDATE ON schema_dual_write_value
                BEGIN SELECT RAISE(ABORT, 'dual-write values are append-only'); END;
                CREATE TRIGGER IF NOT EXISTS schema_dual_write_value_no_delete
                BEFORE DELETE ON schema_dual_write_value
                BEGIN SELECT RAISE(ABORT, 'dual-write values are append-only'); END;
                """
            )
            connection.execute(
                "INSERT OR IGNORE INTO schema_registry_meta VALUES (1, ?)",
                (_REGISTRY_SCHEMA_VERSION,),
            )
            row = connection.execute(
                "SELECT registry_schema_version FROM schema_registry_meta WHERE singleton = 1"
            ).fetchone()
            if row is None or row[0] != _REGISTRY_SCHEMA_VERSION:
                raise RuntimeError("unsupported schema rollout registry version; fail closed")

    @contextmanager
    def _writer(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def create_plan(
        self,
        plan: LiveSchemaRolloutPlan,
        *,
        now: AwareUtcDatetime,
        operation_id: str | None = None,
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        operation_id = operation_id or f"create:{plan.plan_id}"
        request_hash = canonical_sha256({"action": "create", "plan": plan, "now": now})
        if not plan.started_at <= now <= plan.deadline:
            raise ValueError("rollout creation is outside plan deadline")
        self._validate_plan_registry(plan)
        initial = SchemaRolloutState(
            plan_id=plan.plan_id,
            phase=RolloutPhase.PREPARE,
            revision=0,
            updated_at=now,
            authority_declaration_fingerprint=plan.old_declaration_fingerprint,
            new_data_preserved=True,
        )
        payload_json = _json_payload(
            {
                "action": "create",
                "plan_fingerprint": canonical_sha256(plan),
                "resulting_phase": initial.phase.value,
                "authority_declaration_fingerprint": (initial.authority_declaration_fingerprint),
                "new_data_preserved": True,
            }
        )
        event_hash = _event_hash(
            plan_id=plan.plan_id,
            revision=0,
            operation_id=operation_id,
            event_type="create",
            request_hash=request_hash,
            payload_json=payload_json,
            previous_hash=_GENESIS_HASH,
            recorded_at=now,
        )
        with self._writer() as connection:
            existing = connection.execute(
                "SELECT * FROM schema_rollout WHERE plan_id = ?", (plan.plan_id,)
            ).fetchone()
            if existing is not None:
                stored_plan = LiveSchemaRolloutPlan.model_validate_json(existing["plan_json"])
                if stored_plan != plan:
                    raise ValueError("conflicting rollout plan identity")
                event = connection.execute(
                    """
                    SELECT request_hash FROM schema_rollout_event
                    WHERE plan_id = ? AND operation_id = ?
                    """,
                    (plan.plan_id, operation_id),
                ).fetchone()
                if event is not None and event["request_hash"] != request_hash:
                    raise ValueError("conflicting rollout operation retry")
                return self._verified_state(connection, existing)
            connection.execute(
                "INSERT INTO schema_rollout VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    plan.plan_id,
                    plan.model_dump_json(),
                    initial.phase.value,
                    initial.revision,
                    initial.updated_at.isoformat(),
                    initial.authority_declaration_fingerprint,
                    1,
                    event_hash,
                ),
            )
            connection.execute(
                "INSERT INTO schema_rollout_event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    plan.plan_id,
                    0,
                    operation_id,
                    "create",
                    request_hash,
                    payload_json,
                    _GENESIS_HASH,
                    event_hash,
                    now.isoformat(),
                ),
            )
        return initial

    def acknowledge(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        phase: RolloutPhase,
        participant_id: str,
        participant_fingerprint: str,
        declaration_fingerprint: str,
        now: AwareUtcDatetime,
        operation_id: str | None = None,
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        phase = RolloutPhase(phase)
        operation_id = operation_id or f"ack:{phase.value}:{participant_id}"
        request = {
            "action": "participant_ack",
            "phase": phase.value,
            "participant_id": participant_id,
            "participant_fingerprint": participant_fingerprint,
            "declaration_fingerprint": declaration_fingerprint,
            "now": now,
        }
        request_hash = canonical_sha256(request)
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            retried = self._idempotent_retry(
                connection, row, operation_id=operation_id, request_hash=request_hash
            )
            if retried is not None:
                return retried
            self._require_revision(row, expected_revision)
            if RolloutPhase(row["phase"]) is not phase:
                raise ValueError("acknowledgement phase does not match current rollout phase")
            self._validate_time(plan, row, now)
            participants = {
                item.participant_id: item for item in (*plan.producers, *plan.consumers)
            }
            participant = participants.get(participant_id)
            if participant is None:
                raise ValueError("participant is not in the frozen registry")
            if participant.contract_fingerprint != participant_fingerprint:
                raise ValueError("participant fingerprint does not match registry")
            if declaration_fingerprint != plan.new_declaration_fingerprint:
                raise ValueError("declaration fingerprint does not match rollout target")
            return self._append_mutation(
                connection,
                row=row,
                plan=plan,
                operation_id=operation_id,
                event_type="participant_ack",
                request_hash=request_hash,
                payload={
                    **{key: value for key, value in request.items() if key != "now"},
                    "resulting_phase": phase.value,
                    "authority_declaration_fingerprint": row["authority_declaration_fingerprint"],
                    "new_data_preserved": bool(row["new_data_preserved"]),
                },
                phase=phase,
                authority_fingerprint=row["authority_declaration_fingerprint"],
                new_data_preserved=bool(row["new_data_preserved"]),
                now=now,
            )

    def record_dual_write_evidence(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        evidence: DualWriteConsistencyEvidence,
        operation_id: str,
    ) -> SchemaRolloutState:
        request_hash = canonical_sha256({"action": "dual_write_evidence", "evidence": evidence})
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            retried = self._idempotent_retry(
                connection, row, operation_id=operation_id, request_hash=request_hash
            )
            if retried is not None:
                return retried
            self._require_revision(row, expected_revision)
            if RolloutPhase(row["phase"]) is not RolloutPhase.DUAL_WRITE:
                raise ValueError("dual-write evidence requires the dual_write phase")
            self._validate_time(plan, row, evidence.observed_at)
            if evidence.old_declaration_fingerprint != plan.old_declaration_fingerprint:
                raise ValueError("dual-write old declaration fingerprint does not match plan")
            if evidence.new_declaration_fingerprint != plan.new_declaration_fingerprint:
                raise ValueError("dual-write new declaration fingerprint does not match plan")
            if (
                plan.target_generation_id is not None
                and evidence.generation_id != plan.target_generation_id
            ):
                raise ValueError("dual-write generation does not match rollout target")
            existing = connection.execute(
                """
                SELECT evidence_fingerprint FROM schema_dual_write_evidence
                WHERE plan_id = ? AND generation_id = ?
                """,
                (plan_id, evidence.generation_id),
            ).fetchone()
            if existing is not None:
                if existing["evidence_fingerprint"] != evidence.evidence_fingerprint:
                    raise ValueError("conflicting dual-write evidence")
                return self._state_from_row(row)
            next_revision = int(row["revision"]) + 1
            connection.execute(
                "INSERT INTO schema_dual_write_evidence VALUES (?, ?, ?, ?, ?)",
                (
                    plan_id,
                    evidence.generation_id,
                    evidence.model_dump_json(),
                    evidence.evidence_fingerprint,
                    next_revision,
                ),
            )
            return self._append_mutation(
                connection,
                row=row,
                plan=plan,
                operation_id=operation_id,
                event_type="dual_write_evidence",
                request_hash=request_hash,
                payload={
                    "action": "dual_write_evidence",
                    "evidence_fingerprint": evidence.evidence_fingerprint,
                    "generation_id": evidence.generation_id,
                    "new_values_fingerprint": evidence.new_values_fingerprint,
                    "resulting_phase": RolloutPhase.DUAL_WRITE.value,
                    "authority_declaration_fingerprint": row["authority_declaration_fingerprint"],
                    "new_data_preserved": True,
                },
                phase=RolloutPhase.DUAL_WRITE,
                authority_fingerprint=row["authority_declaration_fingerprint"],
                new_data_preserved=True,
                now=evidence.observed_at,
            )

    def acknowledge_consumer(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        receipt: ConsumerCapabilityReceipt,
        now: AwareUtcDatetime,
        operation_id: str,
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        request_hash = canonical_sha256(
            {"action": "consumer_capability", "receipt": receipt, "now": now}
        )
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            trusted = self._trusted_consumers(plan)
            expected = trusted.get(receipt.consumer_id)
            if expected is None:
                raise ValueError("consumer is not a trusted production consumer")
            retried = self._idempotent_retry(
                connection, row, operation_id=operation_id, request_hash=request_hash
            )
            if retried is not None:
                return retried
            self._require_revision(row, expected_revision)
            if RolloutPhase(row["phase"]) is not RolloutPhase.CONSUMER_ACK:
                raise ValueError("consumer capability receipt requires consumer_ack phase")
            self._validate_time(plan, row, now)
            self._validate_consumer_receipt(plan, expected, receipt, now=now)
            existing = connection.execute(
                """
                SELECT receipt_fingerprint FROM schema_consumer_capability_receipt
                WHERE plan_id = ? AND consumer_id = ?
                """,
                (plan_id, receipt.consumer_id),
            ).fetchone()
            if existing is not None:
                if existing["receipt_fingerprint"] != receipt.receipt_fingerprint:
                    raise ValueError("conflicting consumer capability receipt")
                return self._state_from_row(row)
            next_revision = int(row["revision"]) + 1
            connection.execute(
                "INSERT INTO schema_consumer_capability_receipt VALUES (?, ?, ?, ?, ?)",
                (
                    plan_id,
                    receipt.consumer_id,
                    receipt.model_dump_json(),
                    receipt.receipt_fingerprint,
                    next_revision,
                ),
            )
            return self._append_mutation(
                connection,
                row=row,
                plan=plan,
                operation_id=operation_id,
                event_type="consumer_capability",
                request_hash=request_hash,
                payload={
                    "action": "consumer_capability",
                    "consumer_id": receipt.consumer_id,
                    "service_id": receipt.service_id,
                    "receipt_fingerprint": receipt.receipt_fingerprint,
                    "resulting_phase": RolloutPhase.CONSUMER_ACK.value,
                    "authority_declaration_fingerprint": row["authority_declaration_fingerprint"],
                    "new_data_preserved": True,
                },
                phase=RolloutPhase.CONSUMER_ACK,
                authority_fingerprint=row["authority_declaration_fingerprint"],
                new_data_preserved=True,
                now=now,
            )

    def record_dual_write_values(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        old_declaration: SchemaDeclaration,
        new_declaration: SchemaDeclaration,
        old_values: Mapping[str, JsonValue],
        new_values: Mapping[str, JsonValue],
        generation_id: str,
        observed_at: AwareUtcDatetime,
        operation_id: str,
    ) -> SchemaRolloutState:
        """Persist both projected shapes and their consistency proof atomically."""

        evidence = validate_dual_write_values(
            old_declaration=old_declaration,
            new_declaration=new_declaration,
            old_values=old_values,
            new_values=new_values,
            generation_id=generation_id,
            observed_at=observed_at,
        )
        record = DualWriteValueRecord.create(
            evidence=evidence,
            old_values=old_values,
            new_values=new_values,
        )
        request_hash = canonical_sha256({"action": "dual_write_values", "record": record})
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            retried = self._idempotent_retry(
                connection,
                row,
                operation_id=operation_id,
                request_hash=request_hash,
            )
            if retried is not None:
                return retried
            self._require_revision(row, expected_revision)
            current_phase = RolloutPhase(row["phase"])
            if current_phase not in {RolloutPhase.DUAL_WRITE, RolloutPhase.CONSUMER_ACK}:
                raise ValueError("dual-write values require the dual_write phase")
            self._validate_time(plan, row, evidence.observed_at)
            if evidence.old_declaration_fingerprint != plan.old_declaration_fingerprint:
                raise ValueError("dual-write old declaration fingerprint does not match plan")
            if evidence.new_declaration_fingerprint != plan.new_declaration_fingerprint:
                raise ValueError("dual-write new declaration fingerprint does not match plan")
            if (
                plan.target_generation_id is not None
                and evidence.generation_id != plan.target_generation_id
            ):
                raise ValueError("dual-write generation does not match rollout target")
            existing = connection.execute(
                """
                SELECT record_json FROM schema_dual_write_value
                WHERE plan_id = ? AND write_id = ?
                """,
                (plan_id, record.write_id),
            ).fetchone()
            if existing is not None:
                if DualWriteValueRecord.model_validate_json(existing["record_json"]) != record:
                    raise ValueError("conflicting dual-write value record")
                return self._state_from_row(row)
            next_revision = int(row["revision"]) + 1
            connection.execute(
                "INSERT INTO schema_dual_write_value VALUES (?, ?, ?, ?)",
                (plan_id, record.write_id, record.model_dump_json(), next_revision),
            )
            return self._append_mutation(
                connection,
                row=row,
                plan=plan,
                operation_id=operation_id,
                event_type="dual_write_values",
                request_hash=request_hash,
                payload={
                    "action": "dual_write_values",
                    "write_id": record.write_id,
                    "evidence_fingerprint": evidence.evidence_fingerprint,
                    "generation_id": evidence.generation_id,
                    "resulting_phase": current_phase.value,
                    "authority_declaration_fingerprint": row["authority_declaration_fingerprint"],
                    "new_data_preserved": True,
                },
                phase=current_phase,
                authority_fingerprint=row["authority_declaration_fingerprint"],
                new_data_preserved=True,
                now=evidence.observed_at,
            )

    def advance(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        target_phase: RolloutPhase,
        now: AwareUtcDatetime,
        operation_id: str | None = None,
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        target_phase = RolloutPhase(target_phase)
        operation_id = operation_id or f"advance:{target_phase.value}"
        request_hash = canonical_sha256(
            {"action": "advance", "target_phase": target_phase.value, "now": now}
        )
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            retried = self._idempotent_retry(
                connection, row, operation_id=operation_id, request_hash=request_hash
            )
            if retried is not None:
                return retried
            self._require_revision(row, expected_revision)
            current = RolloutPhase(row["phase"])
            if current not in _FORWARD_PHASES or target_phase not in _FORWARD_PHASES:
                raise ValueError("rollout terminal phases cannot advance")
            if _FORWARD_PHASES.index(target_phase) != _FORWARD_PHASES.index(current) + 1:
                raise ValueError("rollout phases must advance consecutively")
            self._validate_time(plan, row, now)
            self._validate_phase_exit(connection, plan, current=current, now=now)
            authority = (
                plan.new_declaration_fingerprint
                if target_phase in {RolloutPhase.CUTOVER, RolloutPhase.RETIRE}
                else plan.old_declaration_fingerprint
            )
            return self._append_mutation(
                connection,
                row=row,
                plan=plan,
                operation_id=operation_id,
                event_type="advance",
                request_hash=request_hash,
                payload={
                    "action": "advance",
                    "from_phase": current.value,
                    "resulting_phase": target_phase.value,
                    "authority_declaration_fingerprint": authority,
                    "new_data_preserved": True,
                },
                phase=target_phase,
                authority_fingerprint=authority,
                new_data_preserved=True,
                now=now,
            )

    def rollback(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        reason: str,
        now: AwareUtcDatetime,
        operation_id: str,
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        if not reason.strip():
            raise ValueError("rollback reason cannot be empty")
        request_hash = canonical_sha256({"action": "rollback", "reason": reason, "now": now})
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            retried = self._idempotent_retry(
                connection, row, operation_id=operation_id, request_hash=request_hash
            )
            if retried is not None:
                return retried
            self._require_revision(row, expected_revision)
            current = RolloutPhase(row["phase"])
            if current in {RolloutPhase.RETIRE, RolloutPhase.ROLLBACK}:
                raise ValueError("retired or rolled-back rollout is terminal")
            if now < self._state_from_row(row).updated_at:
                raise ValueError("rollout time cannot precede the current state")
            return self._append_mutation(
                connection,
                row=row,
                plan=plan,
                operation_id=operation_id,
                event_type="rollback",
                request_hash=request_hash,
                payload={
                    "action": "rollback",
                    "reason": reason,
                    "from_phase": current.value,
                    "resulting_phase": RolloutPhase.ROLLBACK.value,
                    "authority_declaration_fingerprint": (plan.old_declaration_fingerprint),
                    "new_data_preserved": True,
                },
                phase=RolloutPhase.ROLLBACK,
                authority_fingerprint=plan.old_declaration_fingerprint,
                new_data_preserved=True,
                now=now,
            )

    def reject_consumer(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        consumer_id: str,
        reason: str,
        now: AwareUtcDatetime,
        operation_id: str,
    ) -> SchemaRolloutState:
        with self._connect() as connection:
            row, plan = self._load(connection, plan_id)
            if consumer_id not in self._trusted_consumers(plan):
                raise ValueError("consumer is not a trusted production consumer")
            if RolloutPhase(row["phase"]) is not RolloutPhase.CONSUMER_ACK:
                raise ValueError("consumer rejection requires consumer_ack phase")
        return self.rollback(
            plan_id=plan_id,
            expected_revision=expected_revision,
            reason=f"consumer_reject:{consumer_id}:{reason}",
            now=now,
            operation_id=operation_id,
        )

    def expire(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        now: AwareUtcDatetime,
        operation_id: str,
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        with self._connect() as connection:
            row, plan = self._load(connection, plan_id)
            if now <= plan.deadline:
                raise ValueError("rollout deadline has not expired")
            if RolloutPhase(row["phase"]) in {RolloutPhase.RETIRE, RolloutPhase.ROLLBACK}:
                raise ValueError("terminal rollout cannot expire")
        return self.rollback(
            plan_id=plan_id,
            expected_revision=expected_revision,
            reason="deadline_expired; old authority retained",
            now=now,
            operation_id=operation_id,
        )

    def get_state(self, plan_id: str) -> SchemaRolloutState:
        with self._connect() as connection:
            row, _ = self._load(connection, plan_id)
            return self._verified_state(connection, row)

    def receipts(self, plan_id: str) -> tuple[SchemaRolloutReceipt, ...]:
        with self._connect() as connection:
            row, _ = self._load(connection, plan_id)
            self._verified_state(connection, row)
            rows = connection.execute(
                """
                SELECT * FROM schema_rollout_event
                WHERE plan_id = ? ORDER BY revision
                """,
                (plan_id,),
            ).fetchall()
        return tuple(self._receipt_from_row(row) for row in rows)

    def dual_write_evidence(
        self,
        plan_id: str,
    ) -> tuple[DualWriteConsistencyEvidence, ...]:
        with self._connect() as connection:
            row, _ = self._load(connection, plan_id)
            self._verified_state(connection, row)
            rows = connection.execute(
                """
                SELECT evidence_json, evidence_fingerprint
                FROM schema_dual_write_evidence
                WHERE plan_id = ? ORDER BY generation_id
                """,
                (plan_id,),
            ).fetchall()
        evidence = tuple(
            DualWriteConsistencyEvidence.model_validate_json(row["evidence_json"]) for row in rows
        )
        if any(
            item.evidence_fingerprint != row["evidence_fingerprint"]
            for item, row in zip(evidence, rows, strict=True)
        ):
            raise RuntimeError("dual-write evidence fingerprint mismatch")
        return evidence

    def dual_write_records(self, plan_id: str) -> tuple[DualWriteValueRecord, ...]:
        with self._connect() as connection:
            row, _ = self._load(connection, plan_id)
            self._verified_state(connection, row)
            rows = connection.execute(
                """
                SELECT record_json FROM schema_dual_write_value
                WHERE plan_id = ? ORDER BY write_id
                """,
                (plan_id,),
            ).fetchall()
        return tuple(DualWriteValueRecord.model_validate_json(row["record_json"]) for row in rows)

    def consumer_capability_receipts(
        self,
        plan_id: str,
    ) -> tuple[ConsumerCapabilityReceipt, ...]:
        with self._connect() as connection:
            row, _ = self._load(connection, plan_id)
            self._verified_state(connection, row)
            rows = connection.execute(
                """
                SELECT receipt_json FROM schema_consumer_capability_receipt
                WHERE plan_id = ? ORDER BY consumer_id
                """,
                (plan_id,),
            ).fetchall()
        return tuple(
            ConsumerCapabilityReceipt.model_validate_json(row["receipt_json"]) for row in rows
        )

    def _validate_plan_registry(self, plan: LiveSchemaRolloutPlan) -> None:
        if plan.production_consumer_registry_fingerprint is None:
            return
        if self.production_consumer_registry is None:
            raise RuntimeError("strict rollout requires its trusted production consumer registry")
        if (
            plan.production_consumer_registry_fingerprint
            != self.production_consumer_registry.registry_fingerprint
        ):
            raise ValueError("rollout plan is not bound to the trusted production registry")
        trusted = self.production_consumer_registry.for_dataset(plan.dataset_id)
        planned = {item.participant_id: item for item in plan.consumers}
        if set(planned) != set(trusted):
            raise ValueError("rollout consumer list differs from trusted production registry")
        for consumer_id, capability in trusted.items():
            if planned[consumer_id].contract_fingerprint != capability.contract_fingerprint:
                raise ValueError("rollout consumer fingerprint differs from trusted registry")

    def _trusted_consumers(
        self,
        plan: LiveSchemaRolloutPlan,
    ) -> dict[str, ProductionConsumerCapability]:
        if self.production_consumer_registry is None:
            raise ValueError("trusted production consumer registry is required")
        self._validate_plan_registry(plan)
        return self.production_consumer_registry.for_dataset(plan.dataset_id)

    @staticmethod
    def _validate_consumer_receipt(
        plan: LiveSchemaRolloutPlan,
        expected: ProductionConsumerCapability,
        receipt: ConsumerCapabilityReceipt,
        *,
        now: AwareUtcDatetime,
    ) -> None:
        comparisons = {
            "service id": (receipt.service_id, expected.service_id),
            "dataset": (receipt.dataset_id, expected.dataset_id),
            "code commit": (receipt.code_commit, expected.code_commit),
            "minimum readable version": (
                receipt.min_readable_schema_version,
                expected.min_readable_schema_version,
            ),
            "maximum readable version": (
                receipt.max_readable_schema_version,
                expected.max_readable_schema_version,
            ),
            "required fields": (receipt.required_fields, expected.required_fields),
            "physical schema": (
                receipt.serving_physical_schema_fingerprint,
                plan.serving_physical_schema_fingerprint,
            ),
            "observed generation": (
                receipt.observed_generation_id,
                plan.target_generation_id,
            ),
        }
        for label, (actual, wanted) in comparisons.items():
            if actual != wanted:
                raise ValueError(f"consumer receipt {label} does not match trusted rollout")
        if expected.requires_serving_generation_ack != (receipt.serving_generation_id is not None):
            raise ValueError(
                "consumer receipt serving generation evidence does not match trusted rollout"
            )
        if plan.target_schema_version is None or not (
            receipt.min_readable_schema_version
            <= plan.target_schema_version
            <= receipt.max_readable_schema_version
        ):
            raise ValueError("consumer receipt cannot read the target schema version")
        if receipt.available_at > now:
            raise ValueError("consumer receipt available_at cannot be in the future")
        if receipt.available_at < plan.started_at:
            raise ValueError("consumer receipt predates rollout")

    def _validate_phase_exit(
        self,
        connection: sqlite3.Connection,
        plan: LiveSchemaRolloutPlan,
        *,
        current: RolloutPhase,
        now: AwareUtcDatetime,
    ) -> None:
        if current is RolloutPhase.PREPARE:
            required = {item.participant_id for item in plan.producers}
            acknowledged = self._participant_acks(connection, plan.plan_id, RolloutPhase.PREPARE)
            if not required <= acknowledged:
                missing = ", ".join(sorted(required - acknowledged))
                raise ValueError(f"prepare lacks producer acknowledgement: {missing}")
        elif current is RolloutPhase.DUAL_WRITE:
            evidence = connection.execute(
                "SELECT generation_id FROM schema_dual_write_evidence WHERE plan_id = ?",
                (plan.plan_id,),
            ).fetchall()
            observed = {row[0] for row in evidence}
            durable_values = connection.execute(
                "SELECT 1 FROM schema_dual_write_value WHERE plan_id = ? LIMIT 1",
                (plan.plan_id,),
            ).fetchone()
            if not observed and durable_values is None:
                raise ValueError("dual_write lacks consistency evidence")
            if (
                plan.target_generation_id is not None
                and plan.target_generation_id not in observed
                and durable_values is None
            ):
                raise ValueError("dual_write lacks target generation consistency evidence")
        elif current is RolloutPhase.CONSUMER_ACK:
            trusted = self._trusted_consumers(plan)
            rows = connection.execute(
                """
                SELECT consumer_id, receipt_json
                FROM schema_consumer_capability_receipt WHERE plan_id = ?
                """,
                (plan.plan_id,),
            ).fetchall()
            receipts = {
                row["consumer_id"]: ConsumerCapabilityReceipt.model_validate_json(
                    row["receipt_json"]
                )
                for row in rows
            }
            if set(receipts) != set(trusted):
                missing = ", ".join(sorted(set(trusted) - set(receipts)))
                raise ValueError(f"cutover lacks required production consumer ACK: {missing}")
            for consumer_id, receipt in receipts.items():
                self._validate_consumer_receipt(plan, trusted[consumer_id], receipt, now=now)
                age = (now - receipt.available_at).total_seconds()
                if age > plan.consumer_ack_max_age_seconds:
                    raise ValueError(f"consumer ACK is stale: {consumer_id}")
        elif current is RolloutPhase.CUTOVER:
            required = {item.participant_id for item in plan.producers}
            acknowledged = self._participant_acks(connection, plan.plan_id, RolloutPhase.CUTOVER)
            if not required <= acknowledged:
                missing = ", ".join(sorted(required - acknowledged))
                raise ValueError(f"cutover lacks producer acknowledgement: {missing}")

    @staticmethod
    def _participant_acks(
        connection: sqlite3.Connection,
        plan_id: str,
        phase: RolloutPhase,
    ) -> set[str]:
        rows = connection.execute(
            """
            SELECT payload_json FROM schema_rollout_event
            WHERE plan_id = ? AND event_type = 'participant_ack'
            """,
            (plan_id,),
        ).fetchall()
        result: set[str] = set()
        for row in rows:
            payload = json.loads(row["payload_json"])
            if payload.get("phase") == phase.value:
                result.add(payload["participant_id"])
        return result

    @staticmethod
    def _validate_time(
        plan: LiveSchemaRolloutPlan,
        row: sqlite3.Row,
        now: AwareUtcDatetime,
    ) -> None:
        current_phase = RolloutPhase(row["phase"])
        if now > plan.deadline and current_phase is not RolloutPhase.CUTOVER:
            raise ValueError("rollout deadline has expired")
        if now < max(plan.started_at, SchemaRolloutStore._state_from_row(row).updated_at):
            raise ValueError("rollout time cannot precede the current state")

    def _append_mutation(
        self,
        connection: sqlite3.Connection,
        *,
        row: sqlite3.Row,
        plan: LiveSchemaRolloutPlan,
        operation_id: str,
        event_type: str,
        request_hash: str,
        payload: Mapping[str, object],
        phase: RolloutPhase,
        authority_fingerprint: str,
        new_data_preserved: bool,
        now: AwareUtcDatetime,
    ) -> SchemaRolloutState:
        revision = int(row["revision"]) + 1
        payload_json = _json_payload(dict(payload))
        previous_hash = row["last_event_hash"]
        event_hash = _event_hash(
            plan_id=plan.plan_id,
            revision=revision,
            operation_id=operation_id,
            event_type=event_type,
            request_hash=request_hash,
            payload_json=payload_json,
            previous_hash=previous_hash,
            recorded_at=now,
        )
        connection.execute(
            "INSERT INTO schema_rollout_event VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                plan.plan_id,
                revision,
                operation_id,
                event_type,
                request_hash,
                payload_json,
                previous_hash,
                event_hash,
                now.isoformat(),
            ),
        )
        changed = connection.execute(
            """
            UPDATE schema_rollout
            SET phase = ?, revision = ?, updated_at = ?,
                authority_declaration_fingerprint = ?, new_data_preserved = ?,
                last_event_hash = ?
            WHERE plan_id = ? AND revision = ?
            """,
            (
                phase.value,
                revision,
                now.isoformat(),
                authority_fingerprint,
                int(new_data_preserved),
                event_hash,
                plan.plan_id,
                row["revision"],
            ),
        ).rowcount
        if changed != 1:
            raise ValueError("rollout CAS revision mismatch")
        return SchemaRolloutState(
            plan_id=plan.plan_id,
            phase=phase,
            revision=revision,
            updated_at=now,
            authority_declaration_fingerprint=authority_fingerprint,
            new_data_preserved=new_data_preserved,
        )

    def _idempotent_retry(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        operation_id: str,
        request_hash: str,
    ) -> SchemaRolloutState | None:
        existing = connection.execute(
            """
            SELECT request_hash FROM schema_rollout_event
            WHERE plan_id = ? AND operation_id = ?
            """,
            (row["plan_id"], operation_id),
        ).fetchone()
        if existing is None:
            return None
        if existing["request_hash"] != request_hash:
            raise ValueError("conflicting rollout operation retry")
        return self._verified_state(connection, row)

    def _load(
        self,
        connection: sqlite3.Connection,
        plan_id: str,
    ) -> tuple[sqlite3.Row, LiveSchemaRolloutPlan]:
        row = connection.execute(
            "SELECT * FROM schema_rollout WHERE plan_id = ?", (plan_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown rollout plan: {plan_id}")
        self._verified_state(connection, row)
        plan = LiveSchemaRolloutPlan.model_validate_json(row["plan_json"])
        self._validate_plan_registry(plan)
        return row, plan

    def _verified_state(
        self,
        connection: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> SchemaRolloutState:
        try:
            plan = LiveSchemaRolloutPlan.model_validate_json(row["plan_json"])
        except ValueError as exc:
            raise RuntimeError("schema rollout plan is invalid") from exc
        if plan.plan_id != row["plan_id"]:
            raise RuntimeError("schema rollout plan identity mismatch")
        events = connection.execute(
            """
            SELECT * FROM schema_rollout_event
            WHERE plan_id = ? ORDER BY revision
            """,
            (row["plan_id"],),
        ).fetchall()
        if len(events) != int(row["revision"]) + 1:
            raise RuntimeError("schema rollout hash chain is incomplete")
        previous = _GENESIS_HASH
        for expected_revision, event in enumerate(events):
            if event["revision"] != expected_revision or event["previous_hash"] != previous:
                raise RuntimeError("schema rollout hash chain is not contiguous")
            expected_hash = _event_hash(
                plan_id=event["plan_id"],
                revision=event["revision"],
                operation_id=event["operation_id"],
                event_type=event["event_type"],
                request_hash=event["request_hash"],
                payload_json=event["payload_json"],
                previous_hash=event["previous_hash"],
                recorded_at=_decode_time(event["recorded_at"]),
            )
            if event["event_hash"] != expected_hash:
                raise RuntimeError("schema rollout hash chain event hash mismatch")
            try:
                decoded_payload = json.loads(event["payload_json"])
            except json.JSONDecodeError as exc:
                raise RuntimeError("schema rollout event payload is invalid") from exc
            if _json_payload(decoded_payload) != event["payload_json"]:
                raise RuntimeError("schema rollout event payload is not canonical")
            previous = event["event_hash"]
        if previous != row["last_event_hash"]:
            raise RuntimeError("schema rollout hash chain head mismatch")
        creation_payload = json.loads(events[0]["payload_json"])
        if creation_payload.get("plan_fingerprint") != canonical_sha256(plan):
            raise RuntimeError("schema rollout plan diverges from hash chain")
        payload = json.loads(events[-1]["payload_json"])
        if payload.get("resulting_phase") != row["phase"]:
            raise RuntimeError("schema rollout state diverges from hash chain")
        if (
            payload.get("authority_declaration_fingerprint")
            != row["authority_declaration_fingerprint"]
        ):
            raise RuntimeError("schema rollout authority diverges from hash chain")
        if bool(payload.get("new_data_preserved")) != bool(row["new_data_preserved"]):
            raise RuntimeError("schema rollout data-preservation state diverges from hash chain")
        if _decode_time(events[-1]["recorded_at"]) != self._state_from_row(row).updated_at:
            raise RuntimeError("schema rollout timestamp diverges from hash chain")
        self._verify_bound_receipts(connection, row["plan_id"], events)
        return self._state_from_row(row)

    @staticmethod
    def _verify_bound_receipts(
        connection: sqlite3.Connection,
        plan_id: str,
        events: list[sqlite3.Row],
    ) -> None:
        event_payloads = {
            (event["revision"], event["event_type"]): json.loads(event["payload_json"])
            for event in events
        }
        consumer_rows = connection.execute(
            """
            SELECT * FROM schema_consumer_capability_receipt WHERE plan_id = ?
            """,
            (plan_id,),
        ).fetchall()
        consumer_event_fingerprints = {
            payload["receipt_fingerprint"]
            for (revision, event_type), payload in event_payloads.items()
            if revision > 0 and event_type == "consumer_capability"
        }
        consumer_row_fingerprints = {row["receipt_fingerprint"] for row in consumer_rows}
        if consumer_event_fingerprints != consumer_row_fingerprints:
            raise RuntimeError("consumer capability receipt diverges from hash chain")
        for row in consumer_rows:
            try:
                receipt = ConsumerCapabilityReceipt.model_validate_json(row["receipt_json"])
            except ValueError as exc:
                raise RuntimeError("consumer capability receipt is invalid") from exc
            payload = event_payloads.get((row["recorded_revision"], "consumer_capability"))
            if (
                receipt.receipt_fingerprint != row["receipt_fingerprint"]
                or payload is None
                or payload.get("receipt_fingerprint") != row["receipt_fingerprint"]
                or payload.get("consumer_id") != receipt.consumer_id
            ):
                raise RuntimeError("consumer capability receipt diverges from hash chain")
        evidence_rows = connection.execute(
            "SELECT * FROM schema_dual_write_evidence WHERE plan_id = ?",
            (plan_id,),
        ).fetchall()
        evidence_event_fingerprints = {
            payload["evidence_fingerprint"]
            for (revision, event_type), payload in event_payloads.items()
            if revision > 0 and event_type == "dual_write_evidence"
        }
        evidence_row_fingerprints = {row["evidence_fingerprint"] for row in evidence_rows}
        if evidence_event_fingerprints != evidence_row_fingerprints:
            raise RuntimeError("dual-write evidence diverges from hash chain")
        for row in evidence_rows:
            try:
                evidence = DualWriteConsistencyEvidence.model_validate_json(row["evidence_json"])
            except ValueError as exc:
                raise RuntimeError("dual-write evidence is invalid") from exc
            payload = event_payloads.get((row["recorded_revision"], "dual_write_evidence"))
            if (
                evidence.evidence_fingerprint != row["evidence_fingerprint"]
                or payload is None
                or payload.get("evidence_fingerprint") != row["evidence_fingerprint"]
                or payload.get("generation_id") != evidence.generation_id
            ):
                raise RuntimeError("dual-write evidence diverges from hash chain")
        value_rows = connection.execute(
            "SELECT * FROM schema_dual_write_value WHERE plan_id = ?",
            (plan_id,),
        ).fetchall()
        value_event_ids = {
            payload["write_id"]
            for (revision, event_type), payload in event_payloads.items()
            if revision > 0 and event_type == "dual_write_values"
        }
        if value_event_ids != {row["write_id"] for row in value_rows}:
            raise RuntimeError("dual-write values diverge from hash chain")
        for row in value_rows:
            try:
                record = DualWriteValueRecord.model_validate_json(row["record_json"])
            except ValueError as exc:
                raise RuntimeError("dual-write value record is invalid") from exc
            payload = event_payloads.get((row["recorded_revision"], "dual_write_values"))
            if (
                record.write_id != row["write_id"]
                or payload is None
                or payload.get("write_id") != record.write_id
                or payload.get("evidence_fingerprint") != record.evidence.evidence_fingerprint
            ):
                raise RuntimeError("dual-write values diverge from hash chain")

    @staticmethod
    def _require_revision(row: sqlite3.Row, expected_revision: int) -> None:
        if isinstance(expected_revision, bool) or not isinstance(expected_revision, int):
            raise ValueError("rollout CAS revision must be a strict integer")
        if row["revision"] != expected_revision:
            raise ValueError("rollout CAS revision mismatch")

    @staticmethod
    def _state_from_row(row: sqlite3.Row) -> SchemaRolloutState:
        return SchemaRolloutState(
            plan_id=row["plan_id"],
            phase=RolloutPhase(row["phase"]),
            revision=row["revision"],
            updated_at=row["updated_at"],
            authority_declaration_fingerprint=row["authority_declaration_fingerprint"],
            new_data_preserved=bool(row["new_data_preserved"]),
        )

    @staticmethod
    def _receipt_from_row(row: sqlite3.Row) -> SchemaRolloutReceipt:
        return SchemaRolloutReceipt(
            plan_id=row["plan_id"],
            revision=row["revision"],
            operation_id=row["operation_id"],
            event_type=row["event_type"],
            request_hash=row["request_hash"],
            payload_json=row["payload_json"],
            previous_hash=row["previous_hash"],
            event_hash=row["event_hash"],
            recorded_at=row["recorded_at"],
        )


def _decision(
    *,
    fatal_reasons: list[str],
    degraded_reasons: list[str],
    readable_version: int,
) -> CompatibilityDecision:
    if fatal_reasons:
        return CompatibilityDecision(
            outcome=CompatibilityOutcome.INCOMPATIBLE,
            reasons=tuple(dict.fromkeys(fatal_reasons)),
            readable_version=None,
        )
    if degraded_reasons:
        return CompatibilityDecision(
            outcome=CompatibilityOutcome.DEGRADED,
            reasons=tuple(dict.fromkeys(degraded_reasons)),
            readable_version=readable_version,
        )
    return CompatibilityDecision(
        outcome=CompatibilityOutcome.COMPATIBLE,
        reasons=(),
        readable_version=readable_version,
    )


def evaluate_schema_compatibility(
    *,
    old_declaration: SchemaDeclaration,
    new_declaration: SchemaDeclaration,
    consumer: ConsumerSchemaRequirement,
    phase: RolloutPhase,
) -> CompatibilityDecision:
    """Evaluate one consumer against a target declaration without side effects."""

    phase = RolloutPhase(phase)
    fatal: list[str] = []
    degraded: list[str] = []
    target_version = new_declaration.current_version

    if old_declaration.dataset_id != new_declaration.dataset_id:
        fatal.append("old and new declarations target different datasets")
    if old_declaration.schema_name != new_declaration.schema_name:
        fatal.append("old and new declarations use different schema names")
    if consumer.dataset_id != new_declaration.dataset_id:
        fatal.append("consumer and producer target different datasets")
    if new_declaration.current_version < old_declaration.current_version:
        fatal.append("new declaration version cannot precede old declaration version")
    if (
        new_declaration.current_version == old_declaration.current_version
        and new_declaration.semantic_fingerprint != old_declaration.semantic_fingerprint
    ):
        fatal.append("same schema version cannot contain a semantic change")
    if not consumer.min_version <= target_version <= consumer.max_version:
        fatal.append(
            f"schema version {target_version} is outside consumer range "
            f"{consumer.min_version}..{consumer.max_version}"
        )
    if target_version < new_declaration.min_reader_version:
        fatal.append(
            f"schema version {target_version} is below producer min reader version "
            f"{new_declaration.min_reader_version}"
        )

    old_field_history = {field.name: field for field in old_declaration.fields}
    new_field_history = {field.name: field for field in new_declaration.fields}
    old_fields = old_declaration.available_fields()
    new_fields = new_declaration.available_fields()
    supported_fields = consumer.supported_fields

    for name in sorted(set(old_field_history) & set(new_field_history)):
        old_field = old_field_history[name]
        new_field = new_field_history[name]
        if old_field.type_name != new_field.type_name:
            fatal.append(
                f"field {name} type changed from {old_field.type_name} to {new_field.type_name}"
            )
        if old_field.effective_semantic_fingerprint != new_field.effective_semantic_fingerprint:
            fatal.append(f"field {name} semantic meaning changed")
        if old_field.introduced_in != new_field.introduced_in:
            fatal.append(f"field {name} introduced_in history cannot be rewritten")
        if old_field.deprecated_in is not None and (
            old_field.deprecated_in != new_field.deprecated_in
        ):
            fatal.append(f"field {name} deprecated_in history cannot be rewritten")
        if (
            old_field.deprecated_in is None
            and new_field.deprecated_in is not None
            and new_field.deprecated_in <= old_declaration.current_version
        ):
            fatal.append(f"field {name} deprecated_in history cannot be backfilled")
        if old_field.removed_in is not None and old_field.removed_in != new_field.removed_in:
            fatal.append(f"field {name} removed_in history cannot be rewritten")
        if (
            old_field.removed_in is None
            and new_field.removed_in is not None
            and new_field.removed_in <= old_declaration.current_version
        ):
            fatal.append(f"field {name} removed_in history cannot be backfilled")
        old_required_history = old_field.required_history
        if new_field.required_history[: len(old_required_history)] != old_required_history:
            fatal.append(f"field {name} required history cannot be rewritten")
        if any(
            transition.version <= old_declaration.current_version
            for transition in new_field.required_history[len(old_required_history) :]
        ):
            fatal.append(f"field {name} required history cannot be backfilled")

    for name in sorted(set(new_field_history) - set(old_field_history)):
        field = new_field_history[name]
        if field.introduced_in <= old_declaration.current_version:
            fatal.append(f"new field {name} introduced_in history cannot be backfilled")
        if (
            field.deprecated_in is not None
            and field.deprecated_in <= old_declaration.current_version
        ):
            fatal.append(f"new field {name} deprecated_in history cannot be backfilled")
        if field.removed_in is not None and field.removed_in <= old_declaration.current_version:
            fatal.append(f"new field {name} removed_in history cannot be backfilled")

    removed_fields = sorted(set(old_fields) - set(new_fields))
    if phase is not RolloutPhase.RETIRE:
        for name in removed_fields:
            phase_label = (
                "dual_read/consumer_ack" if phase is RolloutPhase.CONSUMER_ACK else phase.value
            )
            fatal.append(f"field {name} cannot be removed during {phase_label}")

    newly_required = sorted(
        name
        for name, field in new_fields.items()
        if field.required and (name not in old_fields or not old_fields[name].required)
    )
    for name in newly_required:
        if phase not in {RolloutPhase.CUTOVER, RolloutPhase.RETIRE}:
            fatal.append(f"field {name} became required before require_new/cutover")
        elif name not in supported_fields:
            fatal.append(
                f"consumer {consumer.consumer_id} does not explicitly support "
                f"newly required field {name}"
            )

    for name in sorted(consumer.required_fields):
        if name not in new_fields:
            fatal.append(f"required field {name} is unavailable")
    for name in sorted(consumer.optional_fields):
        if name not in new_fields:
            degraded.append(f"optional field {name} is unavailable")

    for name in sorted(set(new_fields) & consumer.supported_fields):
        field = new_fields[name]
        capability = consumer.capabilities[name]
        if field.type_name != capability.type_name:
            fatal.append(
                f"consumer {consumer.consumer_id} expects field {name} type "
                f"{capability.type_name}, producer exposes {field.type_name}"
            )
        expected_semantic = capability.semantic_fingerprint
        if expected_semantic is not None and (
            field.effective_semantic_fingerprint != expected_semantic
        ):
            fatal.append(f"consumer {consumer.consumer_id} expects different {name} semantics")
        if field.nullable and not capability.nullable:
            fatal.append(f"consumer {consumer.consumer_id} cannot decode nullable field {name}")
    if consumer.unknown_field_policy is UnknownFieldPolicy.FORBID:
        for name in sorted(set(new_fields) - consumer.supported_fields):
            fatal.append(f"consumer {consumer.consumer_id} forbids unknown field {name}")

    return _decision(
        fatal_reasons=fatal,
        degraded_reasons=degraded,
        readable_version=target_version,
    )


__all__ = [
    "CompatibilityDecision",
    "CompatibilityOutcome",
    "ConsumerCapabilityReceipt",
    "ConsumerFieldCapability",
    "ConsumerSchemaRequirement",
    "DualWriteConsistencyEvidence",
    "DualWriteValueRecord",
    "LiveSchemaRolloutPlan",
    "MAX_SCHEMA_VERSION",
    "ProducerSchemaCapability",
    "ProductionConsumerCapability",
    "ProductionConsumerRegistry",
    "RolloutPhase",
    "SchemaDeclaration",
    "SchemaField",
    "SchemaParticipant",
    "SchemaRequiredTransition",
    "SchemaRolloutReceipt",
    "SchemaRolloutState",
    "SchemaRolloutStore",
    "UnknownFieldPolicy",
    "evaluate_schema_compatibility",
    "validate_dual_write_values",
]
