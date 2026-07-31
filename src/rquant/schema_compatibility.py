"""Frozen contracts and fail-closed decisions for runtime schema evolution."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from enum import StrEnum
from pathlib import Path

from pydantic import Field, field_validator, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)


class RolloutPhase(StrEnum):
    PREPARE_OPTIONAL = "prepare_optional"
    DUAL_WRITE = "dual_write"
    DUAL_READ = "dual_read"
    REQUIRE_NEW = "require_new"
    RETIRE_OLD = "retire_old"


class CompatibilityOutcome(StrEnum):
    COMPATIBLE = "compatible"
    DEGRADED = "degraded"
    INCOMPATIBLE = "incompatible"


class UnknownFieldPolicy(StrEnum):
    ALLOW = "allow"
    FORBID = "forbid"


class SchemaRequiredTransition(RuntimeContractModel):
    version: int = Field(ge=1)
    required: bool


class SchemaField(RuntimeContractModel):
    name: str = Field(min_length=1)
    type_name: str = Field(min_length=1)
    required: bool
    introduced_in: int = Field(ge=1)
    deprecated_in: int | None = Field(default=None, ge=1)
    removed_in: int | None = Field(default=None, ge=1)
    nullable: bool = False
    required_history: tuple[SchemaRequiredTransition, ...] = ()

    @model_validator(mode="after")
    def validate_version_chronology(self) -> SchemaField:
        if self.deprecated_in is not None and self.deprecated_in < self.introduced_in:
            raise ValueError("deprecated_in cannot precede introduced_in")
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
        if history[-1].required is not self.required:
            raise ValueError("required must match the latest required history state")
        return self

    def is_available_in(self, version: int) -> bool:
        return self.introduced_in <= version and (
            self.removed_in is None or version < self.removed_in
        )

    def is_required_in(self, version: int) -> bool:
        states = [item.required for item in self.required_history if item.version <= version]
        return states[-1] if states else False


class SchemaDeclaration(RuntimeContractModel):
    dataset_id: str = Field(min_length=1)
    schema_name: str = Field(min_length=1)
    min_reader_version: int = Field(ge=1)
    current_version: int = Field(ge=1)
    fields: tuple[SchemaField, ...]
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

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


class ConsumerSchemaRequirement(RuntimeContractModel):
    consumer_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    min_version: int = Field(ge=1)
    max_version: int = Field(ge=1)
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


class CompatibilityDecision(RuntimeContractModel):
    outcome: CompatibilityOutcome
    reasons: tuple[str, ...]
    readable_version: int | None = Field(default=None, ge=1)

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
    contract_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")


class LiveSchemaRolloutPlan(RuntimeContractModel):
    dataset_id: str = Field(min_length=1)
    old_declaration_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    new_declaration_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    producers: tuple[SchemaParticipant, ...]
    consumers: tuple[SchemaParticipant, ...]
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
        return values

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
                "started_at": self.started_at,
                "deadline": self.deadline,
            }
        )


class SchemaRolloutState(RuntimeContractModel):
    plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    phase: RolloutPhase
    revision: int = Field(ge=0)
    updated_at: AwareUtcDatetime


_PHASES = tuple(RolloutPhase)


class SchemaRolloutStore:
    """Persist rollout progress with registry-bound acknowledgement and CAS."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_rollout (
                    plan_id TEXT PRIMARY KEY,
                    plan_json TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS schema_rollout_ack (
                    plan_id TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    participant_id TEXT NOT NULL,
                    participant_fingerprint TEXT NOT NULL,
                    declaration_fingerprint TEXT NOT NULL,
                    acknowledged_at TEXT NOT NULL,
                    PRIMARY KEY (plan_id, phase, participant_id),
                    FOREIGN KEY (plan_id) REFERENCES schema_rollout(plan_id)
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

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
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        if not plan.started_at <= now <= plan.deadline:
            raise ValueError("rollout creation is outside plan deadline")
        state = SchemaRolloutState(
            plan_id=plan.plan_id,
            phase=RolloutPhase.PREPARE_OPTIONAL,
            revision=0,
            updated_at=now,
        )
        with self._writer() as connection:
            existing = connection.execute(
                "SELECT * FROM schema_rollout WHERE plan_id = ?", (plan.plan_id,)
            ).fetchone()
            if existing is not None:
                stored_plan = LiveSchemaRolloutPlan.model_validate_json(existing["plan_json"])
                if stored_plan != plan:
                    raise ValueError("conflicting rollout plan identity")
                return self._state_from_row(existing)
            connection.execute(
                "INSERT INTO schema_rollout VALUES (?, ?, ?, ?, ?)",
                (
                    plan.plan_id,
                    plan.model_dump_json(),
                    state.phase.value,
                    state.revision,
                    state.updated_at.isoformat(),
                ),
            )
        return state

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
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            self._require_revision(row, expected_revision)
            if RolloutPhase(row["phase"]) is not phase:
                raise ValueError("acknowledgement phase does not match current rollout phase")
            if now > plan.deadline:
                raise ValueError("rollout deadline has expired")
            if now < max(plan.started_at, self._state_from_row(row).updated_at):
                raise ValueError("rollout time cannot precede the current state")
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
            existing = connection.execute(
                """
                SELECT * FROM schema_rollout_ack
                WHERE plan_id = ? AND phase = ? AND participant_id = ?
                """,
                (plan_id, phase.value, participant_id),
            ).fetchone()
            if existing is not None:
                if (
                    existing["participant_fingerprint"] != participant_fingerprint
                    or existing["declaration_fingerprint"] != declaration_fingerprint
                ):
                    raise ValueError("conflicting acknowledgement fingerprint")
                return self._state_from_row(row)
            connection.execute(
                "INSERT INTO schema_rollout_ack VALUES (?, ?, ?, ?, ?, ?)",
                (
                    plan_id,
                    phase.value,
                    participant_id,
                    participant_fingerprint,
                    declaration_fingerprint,
                    now.isoformat(),
                ),
            )
            return self._cas_update(connection, row, phase, now)

    def advance(
        self,
        *,
        plan_id: str,
        expected_revision: int,
        target_phase: RolloutPhase,
        now: AwareUtcDatetime,
    ) -> SchemaRolloutState:
        now = normalize_aware_utc(now)
        with self._writer() as connection:
            row, plan = self._load(connection, plan_id)
            self._require_revision(row, expected_revision)
            current = RolloutPhase(row["phase"])
            if _PHASES.index(target_phase) != _PHASES.index(current) + 1:
                raise ValueError("rollout phases must advance consecutively")
            if now > plan.deadline:
                raise ValueError("rollout deadline has expired")
            if now < max(plan.started_at, self._state_from_row(row).updated_at):
                raise ValueError("rollout time cannot precede the current state")
            required = {item.participant_id for item in (*plan.producers, *plan.consumers)}
            acknowledged = {
                ack[0]
                for ack in connection.execute(
                    """
                    SELECT participant_id FROM schema_rollout_ack
                    WHERE plan_id = ? AND phase = ?
                    """,
                    (plan_id, current.value),
                ).fetchall()
            }
            if acknowledged != required:
                missing = ", ".join(sorted(required - acknowledged))
                raise ValueError(
                    f"rollout phase lacks complete registry acknowledgement: {missing}"
                )
            return self._cas_update(connection, row, target_phase, now)

    @staticmethod
    def _load(
        connection: sqlite3.Connection,
        plan_id: str,
    ) -> tuple[sqlite3.Row, LiveSchemaRolloutPlan]:
        row = connection.execute(
            "SELECT * FROM schema_rollout WHERE plan_id = ?", (plan_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown rollout plan: {plan_id}")
        return row, LiveSchemaRolloutPlan.model_validate_json(row["plan_json"])

    @staticmethod
    def _require_revision(row: sqlite3.Row, expected_revision: int) -> None:
        if row["revision"] != expected_revision:
            raise ValueError("rollout CAS revision mismatch")

    @staticmethod
    def _cas_update(
        connection: sqlite3.Connection,
        row: sqlite3.Row,
        phase: RolloutPhase,
        now: AwareUtcDatetime,
    ) -> SchemaRolloutState:
        revision = int(row["revision"]) + 1
        changed = connection.execute(
            """
            UPDATE schema_rollout SET phase = ?, revision = ?, updated_at = ?
            WHERE plan_id = ? AND revision = ?
            """,
            (phase.value, revision, now.isoformat(), row["plan_id"], row["revision"]),
        ).rowcount
        if changed != 1:
            raise ValueError("rollout CAS revision mismatch")
        return SchemaRolloutState(
            plan_id=row["plan_id"], phase=phase, revision=revision, updated_at=now
        )

    @staticmethod
    def _state_from_row(row: sqlite3.Row) -> SchemaRolloutState:
        return SchemaRolloutState(
            plan_id=row["plan_id"],
            phase=RolloutPhase(row["phase"]),
            revision=row["revision"],
            updated_at=row["updated_at"],
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
        if old_field.introduced_in != new_field.introduced_in:
            fatal.append(f"field {name} introduced_in history cannot be rewritten")
        if (
            old_field.deprecated_in is not None
            and old_field.deprecated_in != new_field.deprecated_in
        ):
            fatal.append(f"field {name} deprecated_in history cannot be rewritten")
        if old_field.removed_in is not None and old_field.removed_in != new_field.removed_in:
            fatal.append(f"field {name} removed_in history cannot be rewritten")
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
    if phase is not RolloutPhase.RETIRE_OLD:
        for name in removed_fields:
            fatal.append(f"field {name} cannot be removed during {phase.value}")

    newly_required = sorted(
        name
        for name, field in new_fields.items()
        if field.required and (name not in old_fields or not old_fields[name].required)
    )
    for name in newly_required:
        if phase not in {RolloutPhase.REQUIRE_NEW, RolloutPhase.RETIRE_OLD}:
            fatal.append(f"field {name} became required before require_new")
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
    "ConsumerFieldCapability",
    "ConsumerSchemaRequirement",
    "LiveSchemaRolloutPlan",
    "RolloutPhase",
    "SchemaDeclaration",
    "SchemaField",
    "SchemaParticipant",
    "SchemaRequiredTransition",
    "SchemaRolloutState",
    "SchemaRolloutStore",
    "UnknownFieldPolicy",
    "evaluate_schema_compatibility",
]
