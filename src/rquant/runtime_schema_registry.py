"""Hash-bound runtime channel schemas and mixed-version deployment checks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal, Protocol

from pydantic import (
    BaseModel,
    Field,
    JsonValue,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.auction_universe_authority import AuctionUniverseAuthority
from rquant.daily_pipeline_orchestrator import DailyPipelineStatus
from rquant.feature_contracts import FeatureBatchEnvelope
from rquant.lab_artifact_catalog_runtime import LabArtifactCatalogRuntimeStepResult
from rquant.live_contracts import BatchEnvelope
from rquant.paper_execution_constraints import PaperExecutionConstraintBatch
from rquant.reference_data_registry import ReferenceGenerationManifest
from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_snapshot import (
    LabJobsPayload,
    PaperAccountsPayload,
    PromotionsPayload,
    RuntimeHealthPayload,
    SignalDeliveryPayload,
)
from rquant.runtime_shadow_validation import ShadowSessionReport
from rquant.schema_compatibility import (
    CompatibilityOutcome,
    ConsumerCapabilityReceipt,
    ConsumerFieldCapability,
    ConsumerSchemaRequirement,
    LiveSchemaRolloutPlan,
    ProducerSchemaCapability,
    ProductionConsumerCapability,
    ProductionConsumerRegistry,
    RolloutPhase,
    SchemaDeclaration,
    SchemaField,
    SchemaParticipant,
    SchemaRolloutState,
    SchemaRolloutStore,
    UnknownFieldPolicy,
    evaluate_schema_compatibility,
    validate_dual_write_values,
)
from rquant.signal_contracts import SignalEnvelope
from rquant.signal_route_spool import SignalRouteSpoolRecord
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshot

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class RuntimeSchemaCompatibilityError(RuntimeError):
    """A runtime bundle cannot safely overlap the currently installed generation."""


class RuntimeSchemaV1LifecycleReview(RuntimeContractModel):
    channel_id: str = Field(min_length=1)
    field_name: str = Field(min_length=1)
    introduced_in: int = Field(strict=True, ge=1, le=2**31 - 1)
    deprecated_in: int | None = Field(default=None, strict=True, ge=1, le=2**31 - 1)
    removed_in: int | None = Field(default=None, strict=True, ge=1, le=2**31 - 1)

    @model_validator(mode="after")
    def validate_chronology(self) -> RuntimeSchemaV1LifecycleReview:
        SchemaField(
            name=self.field_name,
            type_name="review-only",
            required=False,
            introduced_in=self.introduced_in,
            deprecated_in=self.deprecated_in,
            removed_in=self.removed_in,
        )
        return self


class RuntimeSchemaV1MigrationAudit(RuntimeContractModel):
    schema_version: Literal[1]
    status: Literal["explicit_v1_migration"]
    previous_generation_id: Sha256
    legacy_payload_sha256: Sha256
    candidate_content_hash: Sha256
    reason: str = Field(min_length=1)
    reviewed_lifecycles: tuple[RuntimeSchemaV1LifecycleReview, ...]
    migrated_at: AwareUtcDatetime
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> RuntimeSchemaV1MigrationAudit:
        identities = tuple((item.channel_id, item.field_name) for item in self.reviewed_lifecycles)
        if identities != tuple(sorted(set(identities))):
            raise ValueError("reviewed schema field lifecycles must be unique and sorted")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("runtime schema v1 migration audit content hash mismatch")
        return self


class RuntimeSchemaPreparedDualWrite(RuntimeContractModel):
    plan_id: Sha256
    generation_id: Sha256
    old_declaration: SchemaDeclaration
    new_declaration: SchemaDeclaration
    old_values: Mapping[str, JsonValue]
    new_values: Mapping[str, JsonValue]
    observed_at: AwareUtcDatetime

    @field_validator("old_values", "new_values")
    @classmethod
    def freeze_values(cls, values: Mapping[str, JsonValue]) -> Mapping[str, JsonValue]:
        return MappingProxyType(dict(sorted(values.items())))

    @field_serializer("old_values", "new_values")
    def serialize_values(self, values: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
        return dict(values)


class RuntimeSchemaDualWriter(Protocol):
    def prepare_payload(
        self,
        values: Mapping[str, JsonValue],
        *,
        observed_at: datetime,
    ) -> object | None: ...

    def commit_payload(self, prepared: object, *, operation_id: str) -> object: ...


class RuntimeSchemaConsumerAcknowledger(Protocol):
    def acknowledge_published_generation(
        self,
        *,
        serving_generation_id: str,
        serving_physical_schema_fingerprint: str,
        observed_at: datetime,
    ) -> object: ...


@dataclass(frozen=True)
class RuntimeSchemaDualWriteBinding:
    service_id: str
    plan: LiveSchemaRolloutPlan
    registry: ProductionConsumerRegistry
    store_path: Path
    old_declaration: SchemaDeclaration
    new_declaration: SchemaDeclaration

    def prepare_payload(
        self,
        values: Mapping[str, JsonValue],
        *,
        observed_at: datetime,
        old_values: Mapping[str, JsonValue] | None = None,
        new_values: Mapping[str, JsonValue] | None = None,
    ) -> RuntimeSchemaPreparedDualWrite | None:
        if self.plan.target_generation_id is None:
            raise RuntimeError("runtime schema dual-write target generation is unavailable")
        store = SchemaRolloutStore(
            self.store_path,
            production_consumer_registry=self.registry,
        )
        phase = store.get_state(self.plan.plan_id).phase
        if phase in {RolloutPhase.CUTOVER, RolloutPhase.RETIRE}:
            return None
        if phase is RolloutPhase.ROLLBACK:
            raise RuntimeError("rolled-back schema producer must stop before publishing")
        if phase is RolloutPhase.PREPARE:
            raise RuntimeError("schema producer cannot publish before dual_write")
        candidate = dict(values)
        if old_values is None:
            old_fields = self.old_declaration.available_fields()
            old_values = {name: candidate[name] for name in old_fields if name in candidate}
        if new_values is None:
            new_fields = self.new_declaration.available_fields()
            unknown = sorted(set(candidate) - set(new_fields))
            if unknown:
                raise ValueError(
                    "runtime schema producer emitted undeclared fields: " + ", ".join(unknown)
                )
            new_values = {name: candidate[name] for name in new_fields if name in candidate}
        validate_dual_write_values(
            old_declaration=self.old_declaration,
            new_declaration=self.new_declaration,
            old_values=old_values,
            new_values=new_values,
            generation_id=self.plan.target_generation_id,
            observed_at=observed_at,
        )
        return RuntimeSchemaPreparedDualWrite(
            plan_id=self.plan.plan_id,
            generation_id=self.plan.target_generation_id,
            old_declaration=self.old_declaration,
            new_declaration=self.new_declaration,
            old_values=old_values,
            new_values=new_values,
            observed_at=observed_at,
        )

    def commit_payload(
        self,
        prepared: object,
        *,
        operation_id: str,
    ) -> SchemaRolloutState:
        if not isinstance(prepared, RuntimeSchemaPreparedDualWrite):
            raise TypeError("prepared dual-write payload has an invalid identity")
        if prepared.plan_id != self.plan.plan_id:
            raise ValueError("prepared dual-write belongs to a different rollout")
        store = SchemaRolloutStore(
            self.store_path,
            production_consumer_registry=self.registry,
        )
        state = store.get_state(self.plan.plan_id)
        return store.record_dual_write_values(
            plan_id=self.plan.plan_id,
            expected_revision=state.revision,
            old_declaration=prepared.old_declaration,
            new_declaration=prepared.new_declaration,
            old_values=prepared.old_values,
            new_values=prepared.new_values,
            generation_id=prepared.generation_id,
            observed_at=prepared.observed_at,
            operation_id=operation_id,
        )


@dataclass(frozen=True)
class RuntimeSchemaConsumerAckBinding:
    service_id: str
    consumer: ProductionConsumerCapability
    plan: LiveSchemaRolloutPlan
    registry: ProductionConsumerRegistry
    store_path: Path

    def acknowledge_published_generation(
        self,
        *,
        serving_generation_id: str,
        serving_physical_schema_fingerprint: str,
        observed_at: datetime,
    ) -> SchemaRolloutState:
        if not self.consumer.requires_serving_generation_ack:
            raise RuntimeError("consumer does not require serving generation evidence")
        if serving_physical_schema_fingerprint != self.plan.serving_physical_schema_fingerprint:
            raise ValueError("serving physical schema differs from trusted rollout")
        if self.plan.target_generation_id is None:
            raise RuntimeError("runtime schema target generation is unavailable")
        receipt = ConsumerCapabilityReceipt(
            consumer_id=self.consumer.consumer_id,
            service_id=self.consumer.service_id,
            code_commit=self.consumer.code_commit,
            dataset_id=self.consumer.dataset_id,
            min_readable_schema_version=self.consumer.min_readable_schema_version,
            max_readable_schema_version=self.consumer.max_readable_schema_version,
            required_fields=self.consumer.required_fields,
            serving_physical_schema_fingerprint=(serving_physical_schema_fingerprint),
            observed_generation_id=self.plan.target_generation_id,
            serving_generation_id=serving_generation_id,
            available_at=observed_at,
        )
        store = SchemaRolloutStore(
            self.store_path,
            production_consumer_registry=self.registry,
        )
        state = store.get_state(self.plan.plan_id)
        return store.acknowledge_consumer(
            plan_id=self.plan.plan_id,
            expected_revision=state.revision,
            receipt=receipt,
            now=observed_at,
            operation_id=(
                f"serving-capability:{self.plan.target_generation_id}:"
                f"{serving_generation_id}:{self.consumer.consumer_id}"
            ),
        )


RuntimeSchemaServiceBinding = RuntimeSchemaDualWriteBinding | RuntimeSchemaConsumerAckBinding

_ACTIVE_SCHEMA_BINDINGS: ContextVar[tuple[RuntimeSchemaServiceBinding, ...]] = ContextVar(
    "rquant_runtime_schema_bindings",
    default=(),
)


@contextmanager
def runtime_schema_dual_write_context(
    bindings: tuple[RuntimeSchemaServiceBinding, ...],
) -> Iterator[None]:
    token = _ACTIVE_SCHEMA_BINDINGS.set(bindings)
    try:
        yield
    finally:
        _ACTIVE_SCHEMA_BINDINGS.reset(token)


def current_runtime_schema_dual_writer(
    channel_id: str,
    *,
    producer_commit: str,
) -> RuntimeSchemaDualWriteBinding | None:
    matches = tuple(
        binding
        for binding in _ACTIVE_SCHEMA_BINDINGS.get()
        if isinstance(binding, RuntimeSchemaDualWriteBinding)
        and binding.plan.dataset_id == channel_id
        and binding.new_declaration.producer_commit == producer_commit
    )
    if len(matches) > 1:
        raise RuntimeError("multiple active runtime schema dual-writers target one channel")
    return None if not matches else matches[0]


def current_runtime_schema_consumer_acknowledgers(
    *,
    service_id: str,
    producer_commit: str,
) -> tuple[RuntimeSchemaConsumerAckBinding, ...]:
    return tuple(
        binding
        for binding in _ACTIVE_SCHEMA_BINDINGS.get()
        if isinstance(binding, RuntimeSchemaConsumerAckBinding)
        and binding.service_id == service_id
        and binding.consumer.code_commit == producer_commit
    )


class RuntimeSchemaConsumerBinding(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    requirement: ConsumerSchemaRequirement
    requires_serving_generation_ack: bool = False


class RuntimeSchemaProducerBinding(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    capability: ProducerSchemaCapability


class RuntimePhysicalColumn(RuntimeContractModel):
    name: str = Field(min_length=1)
    type_name: str = Field(min_length=1)
    nullable: bool
    ordinal: int = Field(strict=True, ge=0, le=65_535)
    semantic_fingerprint: Sha256


class RuntimePhysicalTableSchema(RuntimeContractModel):
    storage_format: Literal["pydantic-json/v1"]
    object_name: str = Field(min_length=1)
    columns: tuple[RuntimePhysicalColumn, ...]
    primary_key_fields: tuple[str, ...] = ()
    physical_schema_fingerprint: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> RuntimePhysicalTableSchema:
        names = tuple(column.name for column in self.columns)
        ordinals = tuple(column.ordinal for column in self.columns)
        if not names or len(names) != len(set(names)):
            raise ValueError("physical schema columns must be nonempty and unique")
        if ordinals != tuple(range(len(self.columns))):
            raise ValueError("physical schema column ordinals must be contiguous")
        if tuple(sorted(set(self.primary_key_fields))) != self.primary_key_fields:
            raise ValueError("physical schema primary key fields must be unique and sorted")
        if not set(self.primary_key_fields) <= set(names):
            raise ValueError("physical schema primary key references an unknown column")
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"physical_schema_fingerprint"})
        )
        if self.physical_schema_fingerprint != expected:
            raise ValueError("physical schema fingerprint does not match canonical content")
        return self

    @classmethod
    def create(
        cls,
        *,
        object_name: str,
        declaration: SchemaDeclaration,
    ) -> RuntimePhysicalTableSchema:
        fields = tuple(
            sorted(declaration.available_fields().values(), key=lambda field: field.name)
        )
        values = {
            "storage_format": "pydantic-json/v1",
            "object_name": object_name,
            "columns": tuple(
                RuntimePhysicalColumn(
                    name=field.name,
                    type_name=field.type_name,
                    nullable=field.nullable,
                    ordinal=index,
                    semantic_fingerprint=field.effective_semantic_fingerprint,
                )
                for index, field in enumerate(fields)
            ),
            "primary_key_fields": (),
        }
        return cls(**values, physical_schema_fingerprint=canonical_sha256(values))


class RuntimeSchemaChannelContract(RuntimeContractModel):
    channel_id: str = Field(min_length=1)
    payload_model: str = Field(min_length=1)
    declaration: SchemaDeclaration
    physical_schema: RuntimePhysicalTableSchema
    producer_service_ids: tuple[str, ...] = ()
    producers: tuple[RuntimeSchemaProducerBinding, ...] = ()
    consumers: tuple[RuntimeSchemaConsumerBinding, ...] = ()

    @model_validator(mode="after")
    def validate_identity(self) -> RuntimeSchemaChannelContract:
        if self.declaration.dataset_id != self.channel_id:
            raise ValueError("channel declaration dataset_id does not match channel_id")
        if tuple(sorted(set(self.producer_service_ids))) != self.producer_service_ids:
            raise ValueError("channel producer service ids must be unique and sorted")
        producer_ids = tuple(binding.service_id for binding in self.producers)
        if producer_ids != self.producer_service_ids:
            raise ValueError("channel producer capabilities do not match producer service ids")
        for binding in self.producers:
            if binding.capability.producer_id != binding.service_id:
                raise ValueError("producer capability identity does not match service id")
            if binding.capability.dataset_id != self.channel_id:
                raise ValueError("producer capability dataset_id does not match channel_id")
        consumer_ids = tuple(binding.service_id for binding in self.consumers)
        if tuple(sorted(set(consumer_ids))) != consumer_ids:
            raise ValueError("channel consumer bindings must be unique and sorted")
        for binding in self.consumers:
            if binding.requirement.dataset_id != self.channel_id:
                raise ValueError("consumer requirement dataset_id does not match channel_id")
        if self.physical_schema.object_name != self.payload_model:
            raise ValueError("physical schema object does not match payload model")
        declared = tuple(
            sorted(self.declaration.available_fields().values(), key=lambda field: field.name)
        )
        columns = self.physical_schema.columns
        if tuple(column.name for column in columns) != tuple(field.name for field in declared):
            raise ValueError("physical schema columns do not match active declaration fields")
        for field, column in zip(declared, columns, strict=True):
            if (
                field.type_name != column.type_name
                or field.nullable != column.nullable
                or field.effective_semantic_fingerprint != column.semantic_fingerprint
            ):
                raise ValueError("physical schema column semantics do not match declaration")
        return self


class RuntimeSchemaContractBundle(RuntimeContractModel):
    schema_version: Literal[2]
    producer_commit: CommitSha
    manifest_fingerprints: Mapping[str, Sha256]
    serving_physical_schema_fingerprint: Sha256
    channels: tuple[RuntimeSchemaChannelContract, ...]
    content_hash: Sha256

    @field_validator("manifest_fingerprints")
    @classmethod
    def freeze_manifest_fingerprints(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not service_id for service_id in value):
            raise ValueError("schema manifest service ids cannot be empty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("manifest_fingerprints")
    def serialize_manifest_fingerprints(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_identity(self) -> RuntimeSchemaContractBundle:
        channel_ids = tuple(channel.channel_id for channel in self.channels)
        expected = tuple(sorted(_CHANNEL_SPEC_BY_ID))
        if channel_ids != expected:
            raise ValueError(
                "runtime schema channel catalog is missing or contains an unknown channel"
            )
        expected_hash = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash != expected_hash:
            raise ValueError(
                "runtime schema contract content_hash does not match canonical content"
            )
        known_services = set(self.manifest_fingerprints)
        participants = {
            service_id
            for channel in self.channels
            for service_id in (
                *channel.producer_service_ids,
                *(binding.service_id for binding in channel.consumers),
            )
        }
        if not participants <= known_services:
            raise ValueError("runtime schema participant is absent from manifest fingerprints")
        return self

    @classmethod
    def create(
        cls,
        *,
        producer_commit: str,
        manifest_fingerprints: Mapping[str, str],
        channels: tuple[RuntimeSchemaChannelContract, ...],
        serving_physical_schema_fingerprint: str | None = None,
    ) -> RuntimeSchemaContractBundle:
        if serving_physical_schema_fingerprint is None:
            from rquant.serving_read_models import (
                serving_physical_table_specs_fingerprint,
            )

            serving_physical_schema_fingerprint = serving_physical_table_specs_fingerprint()
        values = {
            "schema_version": 2,
            "producer_commit": producer_commit,
            "manifest_fingerprints": dict(sorted(manifest_fingerprints.items())),
            "serving_physical_schema_fingerprint": (serving_physical_schema_fingerprint),
            "channels": tuple(sorted(channels, key=lambda channel: channel.channel_id)),
        }
        return cls(**values, content_hash=canonical_sha256(values))

    def channel(self, channel_id: str) -> RuntimeSchemaChannelContract:
        try:
            return next(channel for channel in self.channels if channel.channel_id == channel_id)
        except StopIteration as exc:
            raise KeyError(f"unknown runtime schema channel: {channel_id}") from exc


@dataclass(frozen=True)
class _ChannelSpec:
    channel_id: str
    payload_model: type[BaseModel]
    producer_kinds: tuple[RuntimeServiceKind, ...]
    consumer_kinds: tuple[RuntimeServiceKind, ...]
    schema_version: int = 1
    lifecycle_baseline_version: int = 1


_CHANNEL_SPECS = (
    _ChannelSpec(
        "runtime.reference_slow.source-snapshot",
        ReferenceSlowSourceSnapshot,
        (RuntimeServiceKind.REFERENCE_SLOW_SOURCE,),
        (RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,),
    ),
    _ChannelSpec(
        "runtime.reference_slow.batch-envelope",
        BatchEnvelope,
        (RuntimeServiceKind.REFERENCE_SLOW_SOURCE,),
        (RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,),
    ),
    _ChannelSpec(
        "runtime.reference_slow.generation-manifest",
        ReferenceGenerationManifest,
        (RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,),
        (
            RuntimeServiceKind.CANDIDATE_PUBLISHER,
            RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        ),
        schema_version=2,
        lifecycle_baseline_version=2,
    ),
    _ChannelSpec(
        "runtime.auction_universe.authority",
        AuctionUniverseAuthority,
        (RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,),
        (RuntimeServiceKind.AUCTION_MATCH_SOURCE,),
    ),
    _ChannelSpec(
        "runtime.auction_match.batch-envelope",
        BatchEnvelope,
        (RuntimeServiceKind.AUCTION_MATCH_SOURCE,),
        (RuntimeServiceKind.CANDIDATE_PUBLISHER,),
    ),
    _ChannelSpec(
        "runtime.market_minute.batch-envelope",
        BatchEnvelope,
        (RuntimeServiceKind.MARKET_MINUTE_SOURCE,),
        (
            RuntimeServiceKind.FEATURE_LIVE,
            RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
            RuntimeServiceKind.PAPER_BROKER,
        ),
    ),
    _ChannelSpec(
        "runtime.daily_close.batch-envelope",
        BatchEnvelope,
        (RuntimeServiceKind.DAILY_CLOSE_SOURCE,),
        (),
    ),
    _ChannelSpec(
        "runtime.shadow.session-report",
        ShadowSessionReport,
        (RuntimeServiceKind.SHADOW_SESSION,),
        (),
    ),
    _ChannelSpec(
        "runtime.daily_pipeline.status",
        DailyPipelineStatus,
        (RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,),
        (),
    ),
    _ChannelSpec(
        "runtime.watchlist_quote.batch-envelope",
        BatchEnvelope,
        (RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,),
        (),
    ),
    _ChannelSpec(
        "runtime.intraday_feature.batch-envelope",
        FeatureBatchEnvelope,
        (RuntimeServiceKind.FEATURE_LIVE,),
        (RuntimeServiceKind.STRATEGY_LIVE,),
    ),
    _ChannelSpec(
        "runtime.strategy_candidate.snapshot",
        StrategyCandidateSnapshot,
        (RuntimeServiceKind.CANDIDATE_PUBLISHER,),
        (RuntimeServiceKind.STRATEGY_LIVE,),
    ),
    _ChannelSpec(
        "runtime.strategy_signal.envelope",
        SignalEnvelope,
        (RuntimeServiceKind.STRATEGY_LIVE,),
        (RuntimeServiceKind.SIGNAL_ROUTER,),
    ),
    _ChannelSpec(
        "runtime.signal_route.spool-record",
        SignalRouteSpoolRecord,
        (RuntimeServiceKind.SIGNAL_ROUTER,),
        (RuntimeServiceKind.NOTIFIER, RuntimeServiceKind.PAPER_BROKER),
    ),
    _ChannelSpec(
        "runtime.paper_execution.constraint-batch",
        PaperExecutionConstraintBatch,
        (RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,),
        (RuntimeServiceKind.PAPER_BROKER,),
    ),
    _ChannelSpec(
        "runtime.serving.signals",
        SignalDeliveryPayload,
        (RuntimeServiceKind.NOTIFIER,),
        (RuntimeServiceKind.SERVING_PUBLISHER,),
    ),
    _ChannelSpec(
        "runtime.serving.paper-accounts",
        PaperAccountsPayload,
        (RuntimeServiceKind.PAPER_BROKER,),
        (RuntimeServiceKind.SERVING_PUBLISHER,),
    ),
    _ChannelSpec(
        "runtime.serving.runtime-health",
        RuntimeHealthPayload,
        (RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,),
        (RuntimeServiceKind.SERVING_PUBLISHER,),
    ),
    _ChannelSpec(
        "runtime.serving.lab-jobs",
        LabJobsPayload,
        (RuntimeServiceKind.LAB_JOBS_PUBLISHER,),
        (RuntimeServiceKind.SERVING_PUBLISHER,),
    ),
    _ChannelSpec(
        "runtime.lab_artifact_catalog.step-result",
        LabArtifactCatalogRuntimeStepResult,
        (RuntimeServiceKind.LAB_ARTIFACT_CATALOG,),
        (),
    ),
    _ChannelSpec(
        "runtime.serving.promotions",
        PromotionsPayload,
        (RuntimeServiceKind.PROMOTIONS_PUBLISHER,),
        (RuntimeServiceKind.SERVING_PUBLISHER,),
    ),
)
_CHANNEL_SPEC_BY_ID = {spec.channel_id: spec for spec in _CHANNEL_SPECS}
_SUPPORTED_KINDS = frozenset(
    {
        *(kind for spec in _CHANNEL_SPECS for kind in (*spec.producer_kinds, *spec.consumer_kinds)),
        # Retention is generation-bound operational infrastructure. It does not
        # produce or consume a versioned inter-service payload.
        RuntimeServiceKind.ARTIFACT_RETENTION,
    }
)


def _payload_model_name(model: type[BaseModel]) -> str:
    return f"{model.__module__}.{model.__qualname__}"


def _field_schema_hashes(model: type[BaseModel]) -> dict[str, str]:
    schema = model.model_json_schema(mode="validation")
    definitions = schema.get("$defs", {})
    properties = schema.get("properties")
    if not isinstance(properties, dict) or not properties:
        raise RuntimeSchemaCompatibilityError(
            f"payload model {_payload_model_name(model)} has no object fields"
        )
    result: dict[str, str] = {}
    for name, field_schema in sorted(properties.items()):
        result[name] = canonical_sha256(
            {
                "field": field_schema,
                "definitions": definitions,
            }
        )
    return result


def _field_is_nullable(field_schema: object) -> bool:
    if not isinstance(field_schema, dict):
        return False
    if field_schema.get("type") == "null":
        return True
    alternatives = field_schema.get("anyOf", field_schema.get("oneOf", ()))
    return isinstance(alternatives, list) and any(
        isinstance(item, dict) and item.get("type") == "null" for item in alternatives
    )


def _declaration(spec: _ChannelSpec, *, producer_commit: str) -> SchemaDeclaration:
    schema = spec.payload_model.model_json_schema(mode="validation")
    properties = schema.get("properties", {})
    field_hashes = _field_schema_hashes(spec.payload_model)
    required = set(schema.get("required", ()))
    fields = tuple(
        SchemaField(
            name=name,
            type_name=f"json-schema-sha256:{field_hashes[name]}",
            required=name in required,
            introduced_in=spec.lifecycle_baseline_version,
            nullable=_field_is_nullable(properties[name]),
        )
        for name in sorted(field_hashes)
    )
    return SchemaDeclaration(
        dataset_id=spec.channel_id,
        schema_name=_payload_model_name(spec.payload_model),
        min_reader_version=spec.lifecycle_baseline_version,
        current_version=spec.schema_version,
        fields=fields,
        producer_commit=producer_commit,
    )


def _requirement(
    declaration: SchemaDeclaration,
    *,
    service_id: str,
) -> ConsumerSchemaRequirement:
    required = tuple(field.name for field in declaration.fields if field.required)
    optional = tuple(field.name for field in declaration.fields if not field.required)
    return ConsumerSchemaRequirement(
        consumer_id=service_id,
        dataset_id=declaration.dataset_id,
        min_version=declaration.min_reader_version,
        max_version=declaration.current_version,
        required_fields=required,
        optional_fields=optional,
        field_capabilities=tuple(
            ConsumerFieldCapability(
                name=field.name,
                type_name=field.type_name,
                nullable=field.nullable,
            )
            for field in declaration.fields
        ),
        unknown_field_policy=UnknownFieldPolicy.FORBID,
    )


def _producer_capability(
    declaration: SchemaDeclaration,
    *,
    service_id: str,
) -> ProducerSchemaCapability:
    fields = tuple(sorted(declaration.available_fields().values(), key=lambda field: field.name))
    return ProducerSchemaCapability(
        producer_id=service_id,
        dataset_id=declaration.dataset_id,
        min_writable_version=declaration.min_reader_version,
        max_writable_version=declaration.current_version,
        writable_fields=tuple(field.name for field in fields),
        field_capabilities=tuple(
            ConsumerFieldCapability(
                name=field.name,
                type_name=field.type_name,
                nullable=field.nullable,
                semantic_fingerprint=field.effective_semantic_fingerprint,
            )
            for field in fields
        ),
    )


def build_runtime_schema_contract_bundle(
    manifests: tuple[RuntimeServiceManifest, ...],
    *,
    producer_commit: str,
) -> RuntimeSchemaContractBundle:
    if not manifests:
        raise ValueError("runtime schema registry requires at least one manifest")
    if any(manifest.producer_commit != producer_commit for manifest in manifests):
        raise ValueError("runtime schema manifest commit does not match bundle commit")
    service_ids = tuple(manifest.service_id for manifest in manifests)
    if len(service_ids) != len(set(service_ids)):
        raise ValueError("runtime schema registry contains duplicate service ids")
    unsupported = sorted(
        manifest.service_kind.value
        for manifest in manifests
        if manifest.service_kind not in _SUPPORTED_KINDS
    )
    if unsupported:
        raise RuntimeSchemaCompatibilityError(
            "runtime service kind has no channel schema contract: " + ", ".join(unsupported)
        )
    manifests_by_kind: dict[RuntimeServiceKind, list[RuntimeServiceManifest]] = {}
    for manifest in manifests:
        manifests_by_kind.setdefault(manifest.service_kind, []).append(manifest)
    manifests_by_id = {manifest.service_id: manifest for manifest in manifests}

    channels: list[RuntimeSchemaChannelContract] = []
    for spec in _CHANNEL_SPECS:
        declaration = _declaration(spec, producer_commit=producer_commit)
        producer_ids = tuple(
            sorted(
                manifest.service_id
                for kind in spec.producer_kinds
                for manifest in manifests_by_kind.get(kind, ())
            )
        )
        consumer_ids = tuple(
            sorted(
                manifest.service_id
                for kind in spec.consumer_kinds
                for manifest in manifests_by_kind.get(kind, ())
            )
        )
        channels.append(
            RuntimeSchemaChannelContract(
                channel_id=spec.channel_id,
                payload_model=_payload_model_name(spec.payload_model),
                declaration=declaration,
                physical_schema=RuntimePhysicalTableSchema.create(
                    object_name=_payload_model_name(spec.payload_model),
                    declaration=declaration,
                ),
                producer_service_ids=producer_ids,
                producers=tuple(
                    RuntimeSchemaProducerBinding(
                        service_id=service_id,
                        capability=_producer_capability(
                            declaration,
                            service_id=service_id,
                        ),
                    )
                    for service_id in producer_ids
                ),
                consumers=tuple(
                    RuntimeSchemaConsumerBinding(
                        service_id=service_id,
                        requirement=_requirement(declaration, service_id=service_id),
                        requires_serving_generation_ack=(
                            manifests_by_id[service_id].service_kind
                            is RuntimeServiceKind.SERVING_PUBLISHER
                        ),
                    )
                    for service_id in consumer_ids
                ),
            )
        )
    return RuntimeSchemaContractBundle.create(
        producer_commit=producer_commit,
        manifest_fingerprints={
            manifest.service_id: manifest.manifest_fingerprint for manifest in manifests
        },
        channels=tuple(channels),
    )


def build_runtime_schema_rollout(
    *,
    previous: RuntimeSchemaContractBundle,
    candidate: RuntimeSchemaContractBundle,
    channel_id: str,
    target_generation_id: str,
    started_at: datetime,
    deadline: datetime,
    consumer_ack_max_age_seconds: int,
    retire_observation_seconds: int = 86_400,
) -> tuple[LiveSchemaRolloutPlan, ProductionConsumerRegistry]:
    """Derive the production rollout authority from two verified registry bundles."""

    previous = RuntimeSchemaContractBundle.model_validate(previous)
    candidate = RuntimeSchemaContractBundle.model_validate(candidate)
    old_channel = previous.channel(channel_id)
    new_channel = candidate.channel(channel_id)
    if old_channel.payload_model != new_channel.payload_model:
        raise RuntimeSchemaCompatibilityError(
            f"payload model changed on runtime schema channel {channel_id}"
        )
    consumers = tuple(
        ProductionConsumerCapability(
            consumer_id=binding.requirement.consumer_id,
            service_id=binding.service_id,
            dataset_id=channel_id,
            contract_fingerprint=candidate.manifest_fingerprints[binding.service_id],
            code_commit=candidate.producer_commit,
            min_readable_schema_version=binding.requirement.min_version,
            max_readable_schema_version=binding.requirement.max_version,
            required_fields=tuple(sorted(binding.requirement.required_fields)),
            requires_serving_generation_ack=(binding.requires_serving_generation_ack),
        )
        for binding in new_channel.consumers
    )
    if not consumers:
        raise RuntimeSchemaCompatibilityError(
            f"runtime schema channel {channel_id} has no production consumers"
        )
    registry = ProductionConsumerRegistry(
        registry_id=f"runtime-schema:{channel_id}:{candidate.content_hash}",
        consumers=tuple(sorted(consumers, key=lambda item: item.consumer_id)),
    )
    plan = LiveSchemaRolloutPlan(
        dataset_id=channel_id,
        old_declaration_fingerprint=old_channel.declaration.schema_fingerprint,
        new_declaration_fingerprint=new_channel.declaration.schema_fingerprint,
        producers=tuple(
            SchemaParticipant(
                participant_id=binding.service_id,
                contract_fingerprint=candidate.manifest_fingerprints[binding.service_id],
            )
            for binding in new_channel.producers
        ),
        consumers=tuple(
            SchemaParticipant(
                participant_id=consumer.consumer_id,
                contract_fingerprint=consumer.contract_fingerprint,
            )
            for consumer in registry.consumers
        ),
        production_consumer_registry_fingerprint=registry.registry_fingerprint,
        serving_physical_schema_fingerprint=(
            candidate.serving_physical_schema_fingerprint
            if any(consumer.requires_serving_generation_ack for consumer in registry.consumers)
            else new_channel.physical_schema.physical_schema_fingerprint
        ),
        target_generation_id=target_generation_id,
        target_schema_version=new_channel.declaration.current_version,
        consumer_ack_max_age_seconds=consumer_ack_max_age_seconds,
        retire_observation_seconds=retire_observation_seconds,
        started_at=started_at,
        deadline=deadline,
    )
    return plan, registry


def _raise_incompatible(
    *,
    direction: str,
    channel_id: str,
    consumer_id: str,
    reasons: tuple[str, ...],
) -> None:
    detail = "; ".join(reasons)
    raise RuntimeSchemaCompatibilityError(
        f"{direction} is incompatible on {channel_id} for {consumer_id}: {detail}"
    )


def _schema_history_requirement(
    declaration: SchemaDeclaration,
) -> ConsumerSchemaRequirement:
    fields = tuple(sorted(declaration.available_fields().values(), key=lambda field: field.name))
    return ConsumerSchemaRequirement(
        consumer_id="runtime-schema-history-guard",
        dataset_id=declaration.dataset_id,
        min_version=declaration.min_reader_version,
        max_version=declaration.current_version,
        required_fields=(),
        optional_fields=tuple(field.name for field in fields),
        field_capabilities=tuple(
            ConsumerFieldCapability(
                name=field.name,
                type_name=field.type_name,
                nullable=field.nullable,
            )
            for field in fields
        ),
        unknown_field_policy=UnknownFieldPolicy.ALLOW,
    )


def validate_runtime_schema_transition(
    *,
    previous: RuntimeSchemaContractBundle,
    candidate: RuntimeSchemaContractBundle,
) -> None:
    previous = RuntimeSchemaContractBundle.model_validate(previous)
    candidate = RuntimeSchemaContractBundle.model_validate(candidate)
    for channel_id in sorted(_CHANNEL_SPEC_BY_ID):
        old_channel = previous.channel(channel_id)
        new_channel = candidate.channel(channel_id)
        if old_channel.payload_model != new_channel.payload_model:
            raise RuntimeSchemaCompatibilityError(
                f"payload model changed on runtime schema channel {channel_id}"
            )
        if new_channel.declaration.current_version < old_channel.declaration.current_version:
            raise RuntimeSchemaCompatibilityError(
                f"schema version regressed on runtime channel {channel_id}"
            )
        if new_channel.producer_service_ids and old_channel.consumers:
            for binding in old_channel.consumers:
                decision = evaluate_schema_compatibility(
                    old_declaration=old_channel.declaration,
                    new_declaration=new_channel.declaration,
                    consumer=binding.requirement,
                    phase=RolloutPhase.DUAL_WRITE,
                )
                if decision.outcome is CompatibilityOutcome.INCOMPATIBLE:
                    _raise_incompatible(
                        direction="new producer -> old consumer",
                        channel_id=channel_id,
                        consumer_id=binding.service_id,
                        reasons=decision.reasons,
                    )

        history_decision = evaluate_schema_compatibility(
            old_declaration=old_channel.declaration,
            new_declaration=new_channel.declaration,
            consumer=_schema_history_requirement(new_channel.declaration),
            phase=RolloutPhase.DUAL_WRITE,
        )
        if history_decision.outcome is CompatibilityOutcome.INCOMPATIBLE:
            _raise_incompatible(
                direction="schema history transition",
                channel_id=channel_id,
                consumer_id="runtime-schema-history-guard",
                reasons=history_decision.reasons,
            )

        if old_channel.producer_service_ids and new_channel.consumers:
            for binding in new_channel.consumers:
                decision = evaluate_schema_compatibility(
                    old_declaration=old_channel.declaration,
                    new_declaration=old_channel.declaration,
                    consumer=binding.requirement,
                    phase=RolloutPhase.PREPARE_OPTIONAL,
                )
                if decision.outcome is CompatibilityOutcome.INCOMPATIBLE:
                    _raise_incompatible(
                        direction="old producer -> new consumer",
                        channel_id=channel_id,
                        consumer_id=binding.service_id,
                        reasons=decision.reasons,
                    )


def parse_runtime_schema_contract_bundle(payload: bytes) -> RuntimeSchemaContractBundle:
    try:
        raw = json.loads(payload)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        raise RuntimeSchemaCompatibilityError("runtime schema contract is invalid") from exc
    if isinstance(raw, dict) and raw.get("schema_version") == 1:
        raise RuntimeSchemaCompatibilityError(
            "legacy v1 runtime schema registry requires explicit migration; fail closed"
        )
    try:
        bundle = RuntimeSchemaContractBundle.model_validate_json(payload)
    except (ValueError, TypeError) as exc:
        raise RuntimeSchemaCompatibilityError("runtime schema contract is invalid") from exc
    canonical = bundle.model_dump_json().encode("utf-8")
    if payload != canonical:
        raise RuntimeSchemaCompatibilityError("runtime schema contract is not canonical")
    return bundle


def build_runtime_schema_v1_migration_audit(
    *,
    legacy_payload: bytes,
    candidate: RuntimeSchemaContractBundle,
    reason: str,
    previous_generation_id: str,
    reviewed_lifecycles: tuple[RuntimeSchemaV1LifecycleReview, ...],
    migrated_at: datetime,
) -> RuntimeSchemaV1MigrationAudit:
    """Bind a reviewed v1 replacement without inventing lifecycle history."""

    try:
        legacy = json.loads(legacy_payload)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        raise RuntimeSchemaCompatibilityError(
            "legacy v1 runtime schema contract is invalid"
        ) from exc
    if not isinstance(legacy, dict) or legacy.get("schema_version") != 1:
        raise RuntimeSchemaCompatibilityError("runtime schema migration source is not legacy v1")
    if not isinstance(reason, str) or not reason.strip():
        raise RuntimeSchemaCompatibilityError("legacy v1 migration requires an audit reason")
    candidate = RuntimeSchemaContractBundle.model_validate(candidate)
    expected = {
        (channel.channel_id, field.name): (
            field.introduced_in,
            field.deprecated_in,
            field.removed_in,
        )
        for channel in candidate.channels
        for field in channel.declaration.fields
    }
    observed = {
        (item.channel_id, item.field_name): (
            item.introduced_in,
            item.deprecated_in,
            item.removed_in,
        )
        for item in reviewed_lifecycles
    }
    if len(observed) != len(reviewed_lifecycles) or observed != expected:
        raise RuntimeSchemaCompatibilityError(
            "legacy v1 migration requires a complete exact field lifecycle review"
        )
    values = {
        "schema_version": 1,
        "status": "explicit_v1_migration",
        "previous_generation_id": previous_generation_id,
        "legacy_payload_sha256": hashlib.sha256(legacy_payload).hexdigest(),
        "candidate_content_hash": candidate.content_hash,
        "reason": reason.strip(),
        "reviewed_lifecycles": tuple(
            sorted(reviewed_lifecycles, key=lambda item: (item.channel_id, item.field_name))
        ),
        "migrated_at": migrated_at,
    }
    return RuntimeSchemaV1MigrationAudit(**values, content_hash=canonical_sha256(values))


__all__ = [
    "RuntimeSchemaChannelContract",
    "RuntimeSchemaCompatibilityError",
    "RuntimeSchemaConsumerAckBinding",
    "RuntimeSchemaConsumerAcknowledger",
    "RuntimeSchemaConsumerBinding",
    "RuntimeSchemaContractBundle",
    "RuntimeSchemaDualWriteBinding",
    "RuntimeSchemaDualWriter",
    "RuntimeSchemaPreparedDualWrite",
    "RuntimeSchemaProducerBinding",
    "RuntimeSchemaServiceBinding",
    "RuntimeSchemaV1LifecycleReview",
    "RuntimeSchemaV1MigrationAudit",
    "RuntimePhysicalColumn",
    "RuntimePhysicalTableSchema",
    "build_runtime_schema_contract_bundle",
    "build_runtime_schema_rollout",
    "build_runtime_schema_v1_migration_audit",
    "current_runtime_schema_consumer_acknowledgers",
    "current_runtime_schema_dual_writer",
    "parse_runtime_schema_contract_bundle",
    "runtime_schema_dual_write_context",
    "validate_runtime_schema_transition",
]
