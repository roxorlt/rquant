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

    @field_validator("serving_root")
    @classmethod
    def require_absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("serving runtime root must be absolute")
        return value

    @model_validator(mode="after")
    def validate_source_authorities(self) -> ServingRuntimeSettings:
        if not self.source_authorities:
            return self
        dataset_ids = tuple(item.dataset_id for item in self.source_authorities)
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("serving source authorities contain duplicate datasets")
        if set(dataset_ids) != set(_SOURCE_PAYLOAD_KINDS):
            missing = sorted(set(_SOURCE_PAYLOAD_KINDS).difference(dataset_ids))
            unexpected = sorted(set(dataset_ids).difference(_SOURCE_PAYLOAD_KINDS))
            raise ValueError(
                "serving source authorities require exactly six owner datasets; "
                f"missing={missing}, unexpected={unexpected}"
            )
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
    _REFERENCE_SLOW_AUTHORITY_DATASET_ID: "reference_slow",
}


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
        if resolved_snapshot_loader is None:
            if not settings.source_authorities:
                raise ValueError("default serving publisher requires six source authorities")
            from rquant.runtime_serving_authority import ServingSourceAuthorityReader
            from rquant.runtime_serving_snapshot import ServingSnapshotAssembler

            readers = {
                authority.dataset_id: ServingSourceAuthorityReader(
                    root=authority.root,
                    expected_producer_commit=manifest.producer_commit,
                    expected_dataset_id=authority.dataset_id,
                    expected_payload_kind=_SOURCE_PAYLOAD_KINDS[authority.dataset_id],
                    max_bytes=authority.max_bytes,
                )
                for authority in settings.source_authorities
            }
            assembler = ServingSnapshotAssembler(
                signal_reader=readers["signals"],
                paper_accounts_reader=readers["paper_accounts"],
                runtime_health_reader=readers["runtime_health"],
                lab_jobs_reader=readers["lab_jobs"],
                promotions_reader=readers["promotions"],
                reference_slow_reader=readers[_REFERENCE_SLOW_AUTHORITY_DATASET_ID],
            )
            resolved_snapshot_loader = assembler.assemble
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
            generation = publisher.publish(
                tables,
                watermarks=snapshot.watermarks,
                source_generations=snapshot.source_generations,
                built_at=snapshot.read_model.observed_at,
            )
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
            )

        return step

    return build


__all__ = [
    "ServingRuntimeSettings",
    "ServingRuntimeSnapshot",
    "ServingSourceAuthoritySettings",
    "ServingSnapshotLoader",
    "serving_publisher_builder",
]
