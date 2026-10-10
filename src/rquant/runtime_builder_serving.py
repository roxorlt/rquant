"""Runtime builder for immutable read-only serving generations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import (
    Field,
    StrictInt,
    StrictStr,
    StringConstraints,
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
from rquant.runtime_health_details import RuntimeHealthOpsBinding
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_publisher import ServingPublisher
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingReadModelInput,
    build_serving_read_models,
    serving_physical_table_specs_fingerprint,
)

if TYPE_CHECKING:
    from rquant.runtime_schema_registry import RuntimeSchemaConsumerAcknowledger

GenerationId = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ServingSnapshotLoader = Callable[[datetime], "ServingRuntimeSnapshot"]
_REFERENCE_SLOW_AUTHORITY_DATASET_ID = "reference_slow_authority"
_REFERENCE_SLOW_DATASET_ID = "reference_slow"
_REFERENCE_SLOW_CONTRACT_DATASET_ID = "reference_slow_contract"

#: The research sources and ops_status may be absent without stopping serving. An absent
#: ops sample never counts as a healthy task or resource reading. Required pricing and
#: signal sources stay fail-closed: criterion (3b) reads "no signal today"
#: off an empty `signals` table, and that reading is only worth anything while a broken
#: `signals` reader still refuses the round instead of publishing the same empty table.
#: It lives here rather than in `runtime_serving_snapshot`, which imports this module.
DEFAULT_OPTIONAL_SOURCE_DATASETS: frozenset[str] = frozenset(
    {"lab_jobs", "promotions", "ops_status", "strategy_catalog"}
)


def current_runtime_schema_consumer_acknowledgers(
    *,
    service_id: str,
    producer_commit: str,
) -> tuple[RuntimeSchemaConsumerAcknowledger, ...]:
    from rquant.runtime_schema_registry import (
        current_runtime_schema_consumer_acknowledgers as current_acknowledgers,
    )

    return current_acknowledgers(
        service_id=service_id,
        producer_commit=producer_commit,
    )


class ServingRuntimeSettings(RuntimeContractModel):
    serving_root: Path
    schema_version: StrictInt = Field(ge=1)
    source_authorities: tuple[ServingSourceAuthoritySettings, ...] = ()
    ops_manifest_digest: GenerationId | None = None
    health_ops_binding: RuntimeHealthOpsBinding | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    #: The legacy six-owner production manifest keeps its explicit optional set; the
    #: missing ops owner is separately forced unavailable at build. The reverse does not hold:
    #: `RuntimeContractModel` forbids extra keys, so
    #: a runtime generation staged by this code and left published while the code rolls
    #: back to one that has no such field is refused at build. Roll the runtime generation
    #: back together with the code.
    optional_source_datasets: tuple[StrictStr, ...] = tuple(
        sorted(DEFAULT_OPTIONAL_SOURCE_DATASETS)
    )

    @field_validator("serving_root")
    @classmethod
    def require_absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("serving runtime root must be absolute")
        return value

    @field_validator("optional_source_datasets")
    @classmethod
    def validate_optional_source_datasets(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("optional serving source datasets contain a duplicate")
        unknown = sorted(set(value).difference(_SOURCE_PAYLOAD_KINDS))
        if unknown:
            raise ValueError(f"optional serving source datasets are not owner datasets: {unknown}")
        if _REFERENCE_SLOW_AUTHORITY_DATASET_ID in value:
            raise ValueError("reference_slow_authority can never be an optional serving source")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def validate_source_authorities(self) -> ServingRuntimeSettings:
        if not self.source_authorities:
            return self
        dataset_ids = tuple(item.dataset_id for item in self.source_authorities)
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("serving source authorities contain duplicate datasets")
        expected = set(_SOURCE_PAYLOAD_KINDS)
        observed = set(dataset_ids)
        seven = expected.difference({"strategy_catalog"})
        legacy = seven.difference({"ops_status"})
        if observed not in (expected, seven, legacy):
            missing = sorted(expected.difference(dataset_ids))
            unexpected = sorted(set(dataset_ids).difference(_SOURCE_PAYLOAD_KINDS))
            raise ValueError(
                "serving source authorities require exactly eight owner datasets, "
                "seven without strategy_catalog, or the legacy six without ops_status; "
                f"missing={missing}, unexpected={unexpected}"
            )
        if observed in (expected, seven) and "ops_status" not in self.optional_source_datasets:
            raise ValueError("ops_status must be optional in the seven-owner serving manifest")
        if observed in (expected, seven) and self.ops_manifest_digest is None:
            raise ValueError("seven-owner serving manifest requires ops_manifest_digest")
        if observed == legacy and self.ops_manifest_digest is not None:
            raise ValueError("legacy six-owner manifest cannot assert ops_manifest_digest")
        return self


class ServingSourceAuthoritySettings(RuntimeContractModel):
    dataset_id: StrictStr = Field(min_length=1)
    root: Path
    max_bytes: StrictInt = Field(default=8 * 1024 * 1024, gt=0)

    @field_validator("root")
    @classmethod
    def require_absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("serving source authority root must be absolute")
        return value


_SOURCE_PAYLOAD_KINDS = {
    "signals": "signal_delivery",
    "paper_accounts": "paper_accounts",
    "runtime_health": "runtime_health",
    "lab_jobs": "lab_jobs",
    "promotions": "promotions",
    "strategy_catalog": "strategy_catalog",
    "ops_status": "ops_status",
    _REFERENCE_SLOW_AUTHORITY_DATASET_ID: "reference_slow",
}

#: The one list of owner datasets serving reads. Both guards that have to name the seven
#: derive from here -- `ServingRuntimeSettings.validate_optional_source_datasets` above,
#: and `ServingSnapshotAssembler`'s construction check, which imports this name. A
#: profile's `source_authorities` cannot be derived (each entry carries its own root),
#: but it is checked against the same mapping by `validate_source_authorities`, so a
#: seventh source added in one place and missed in another is refused rather than
#: silently leaving the two guards out of step.
SERVING_SOURCE_DATASET_IDS: frozenset[str] = frozenset(_SOURCE_PAYLOAD_KINDS)


class ServingReferenceSlowEvidence(RuntimeContractModel):
    reference_generation_id: GenerationId
    revision: StrictInt = Field(ge=1)
    price_basis: Literal["raw_session"]
    adjustment_basis: Literal["tushare_adj_factor"]
    available_at: AwareUtcDatetime

    @property
    def contract_generation_id(self) -> str:
        return canonical_sha256(
            {
                "contract": "serving-reference-slow/v1",
                "reference_generation_id": self.reference_generation_id,
                "revision": self.revision,
                "price_basis": self.price_basis,
                "adjustment_basis": self.adjustment_basis,
                "available_at": self.available_at,
            }
        )


class ServingRuntimeSnapshot(RuntimeContractModel):
    """One coherent as-of snapshot supplied without production database access."""

    read_model: ServingReadModelInput
    reference_slow: ServingReferenceSlowEvidence
    watermarks: tuple[ServingDatasetWatermark, ...]
    source_generations: Mapping[str, GenerationId] = Field(min_length=1)

    @field_validator("source_generations")
    @classmethod
    def freeze_source_generations(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not dataset_id for dataset_id in value):
            raise ValueError("source generation dataset ids cannot be empty")
        if "serving_generation" in value:
            raise ValueError("serving_generation is reserved for runtime output")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("source_generations")
    def serialize_source_generations(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_source_binding(self) -> ServingRuntimeSnapshot:
        watermark_ids = tuple(watermark.dataset_id for watermark in self.watermarks)
        if len(watermark_ids) != len(set(watermark_ids)):
            raise ValueError("watermarks must be unique by dataset_id")
        if set(watermark_ids) != set(self.source_generations):
            raise ValueError("each source generation must have exactly one watermark")
        for watermark in self.watermarks:
            if watermark.generation_id != self.source_generations[watermark.dataset_id]:
                raise ValueError(
                    f"watermark {watermark.dataset_id} generation does not match source"
                )
            if watermark.published_at > self.read_model.observed_at:
                raise ValueError("serving watermark contains future snapshot evidence")
        expected_reference_bindings = {
            _REFERENCE_SLOW_DATASET_ID: self.reference_slow.reference_generation_id,
            _REFERENCE_SLOW_CONTRACT_DATASET_ID: (self.reference_slow.contract_generation_id),
        }
        for dataset_id, generation_id in expected_reference_bindings.items():
            if self.source_generations.get(dataset_id) != generation_id:
                raise ValueError(f"{dataset_id} does not bind reference slow evidence")
        if _REFERENCE_SLOW_AUTHORITY_DATASET_ID not in self.source_generations:
            raise ValueError("reference slow authority generation is missing")
        if self.reference_slow.available_at > self.read_model.observed_at:
            raise ValueError("reference slow evidence contains future availability")
        return self


def _degraded_reasons(
    watermarks: tuple[ServingDatasetWatermark, ...],
) -> tuple[str, ...]:
    return tuple(
        f"serving:{watermark.dataset_id}:{watermark.status.value}:{watermark.reason}"
        for watermark in watermarks
        if watermark.status is not FreshnessStatus.FRESH
    )


def serving_publisher_builder(
    *,
    snapshot_loader: ServingSnapshotLoader | None,
    clock: Callable[[], datetime],
    runtime_root: Path | None = None,
) -> RuntimeServiceBuilder:
    """Build a serving step from owner authorities or an explicit test loader."""

    if snapshot_loader is not None and not callable(snapshot_loader):
        raise TypeError("snapshot_loader must be callable")
    if not callable(clock):
        raise TypeError("clock must be callable")

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.SERVING_PUBLISHER:
            raise ValueError("runtime service kind must be serving_publisher")
        if manifest.plane is not RuntimeServicePlane.SERVING:
            raise ValueError("serving publisher must run on the serving plane")

        settings = ServingRuntimeSettings.model_validate(dict(manifest.settings))
        if snapshot_loader is not None and settings.source_authorities:
            raise ValueError("injected snapshot_loader cannot be combined with source authorities")
        resolved_snapshot_loader = snapshot_loader
        build_events: tuple[str, ...] = ()
        if resolved_snapshot_loader is None:
            if not settings.source_authorities:
                raise ValueError("default serving publisher requires seven source authorities")
            from rquant.runtime_generation_lineage import producer_commit_lineage
            from rquant.runtime_serving_authority import (
                ServingSourceAuthorityReader,
                serving_source_pointer_handover,
            )
            from rquant.runtime_serving_snapshot import ServingSnapshotAssembler

            # Each source `current.json` belongs to an independent owner,
            # and none of them is republished until its owner runs again, so after a
            # release every one of them still carries the previous generation's commit.
            # `signals` is the one the 2026-09-09 window failed on (#253); the rule is the
            # same for every configured owner, and an unknown commit is still refused.
            # The pointer cannot be rewritten from here: `deploy/systemd/
            # rquant-runtime-serving@.service` mounts `control/` and
            # `live/notifications/` read-only for this role, so the owner rewrites it on
            # its own next publish and this side accepts it until then.
            previous_generation_of_producer_commit = producer_commit_lineage(
                runtime_root,
                service_id=manifest.service_id,
            )
            readers = {
                authority.dataset_id: ServingSourceAuthorityReader(
                    root=authority.root,
                    expected_producer_commit=manifest.producer_commit,
                    expected_dataset_id=authority.dataset_id,
                    expected_payload_kind=_SOURCE_PAYLOAD_KINDS[authority.dataset_id],
                    max_bytes=authority.max_bytes,
                    previous_generation_of_producer_commit=(previous_generation_of_producer_commit),
                )
                for authority in settings.source_authorities
            }
            handover_events = tuple(
                event
                for event in (
                    serving_source_pointer_handover(readers[dataset_id])
                    for dataset_id in sorted(readers)
                )
                if event is not None
            )
            if "strategy_catalog" in readers:
                if runtime_root is None:
                    raise ValueError("strategy catalog requires the current runtime root")
                from rquant.strategy_catalog_source import CurrentStrategyCatalogAuthorityReader

                readers["strategy_catalog"] = CurrentStrategyCatalogAuthorityReader(
                    reader=readers["strategy_catalog"],
                    runtime_root=runtime_root,
                )
            health_ops_reference_reader = None
            if settings.health_ops_binding is not None:
                from rquant.runtime_health_authority import RuntimeHealthTrustedOpsProvider

                if settings.health_ops_binding.producer_commit != manifest.producer_commit:
                    raise ValueError("health Ops binding does not name this exact producer")
                health_ops_reference_reader = RuntimeHealthTrustedOpsProvider(
                    settings.health_ops_binding
                ).read_source
            elif "ops_status" in readers:
                health_ops_reference_reader = ServingSourceAuthorityReader(
                    root=readers["ops_status"].root,
                    expected_producer_commit=manifest.producer_commit,
                    expected_dataset_id="ops_status",
                    expected_payload_kind="ops_status",
                    max_bytes=512 * 1024,
                    previous_generation_of_producer_commit=previous_generation_of_producer_commit,
                )
            assembler = ServingSnapshotAssembler(
                signal_reader=readers["signals"],
                paper_accounts_reader=readers["paper_accounts"],
                runtime_health_reader=readers["runtime_health"],
                lab_jobs_reader=readers["lab_jobs"],
                promotions_reader=readers["promotions"],
                ops_status_reader=readers.get("ops_status"),
                strategy_catalog_reader=readers.get("strategy_catalog"),
                expected_ops_manifest_digest=settings.ops_manifest_digest,
                health_ops_reference_reader=health_ops_reference_reader,
                reference_slow_reader=readers[_REFERENCE_SLOW_AUTHORITY_DATASET_ID],
                #: The legacy six-owner shape has no ops authority yet. Other sources
                #: follow the manifest; only classified ops integrity is also optional.
                optional_datasets=frozenset(settings.optional_source_datasets).union(
                    ({"ops_status"} if "ops_status" not in readers else set())
                    | ({"strategy_catalog"} if "strategy_catalog" not in readers else set())
                ),
            )
            resolved_snapshot_loader = assembler.assemble
            build_events = handover_events
        publisher = ServingPublisher(
            settings.serving_root,
            producer_commit=manifest.producer_commit,
            schema_version=settings.schema_version,
            table_specs=SERVING_TABLE_SPECS,
        )

        def step() -> RuntimeStepResult:
            as_of = normalize_aware_utc(clock())
            snapshot = resolved_snapshot_loader(as_of)
            if not isinstance(snapshot, ServingRuntimeSnapshot):
                raise TypeError("snapshot_loader must return ServingRuntimeSnapshot")
            snapshot = ServingRuntimeSnapshot.model_validate(snapshot)
            if snapshot.read_model.observed_at > as_of:
                raise ValueError("serving snapshot contains future evidence at runtime clock")

            tables = build_serving_read_models(snapshot.read_model)
            publication = publisher.publish_generation(
                tables,
                watermarks=snapshot.watermarks,
                source_generations=snapshot.source_generations,
                built_at=snapshot.read_model.observed_at,
            )
            generation = publication.manifest
            for acknowledger in current_runtime_schema_consumer_acknowledgers(
                service_id=manifest.service_id,
                producer_commit=manifest.producer_commit,
            ):
                acknowledger.acknowledge_published_generation(
                    serving_generation_id=generation.generation_id,
                    serving_physical_schema_fingerprint=(
                        serving_physical_table_specs_fingerprint()
                    ),
                    observed_at=snapshot.read_model.observed_at,
                )
            high_watermark = max(watermark.sequence for watermark in snapshot.watermarks)
            return RuntimeStepResult(
                input_sequence=high_watermark,
                output_sequence=high_watermark,
                processed_count=sum(
                    row_count
                    for table_name, row_count in generation.row_counts.items()
                    if table_name != "projection_status"
                ),
                backlog_count=0,
                source_generations={
                    **snapshot.source_generations,
                    "serving_generation": generation.generation_id,
                },
                degraded_reasons=_degraded_reasons(snapshot.watermarks),
                generation_published=publication.written,
            )

        if build_events:
            step.generation_events = build_events

        return step

    return build


__all__ = [
    "ServingRuntimeSettings",
    "ServingRuntimeSnapshot",
    "ServingSourceAuthoritySettings",
    "ServingSnapshotLoader",
    "serving_publisher_builder",
]
