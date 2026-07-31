"""Runtime builder for immutable read-only serving generations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated

from pydantic import (
    Field,
    StrictInt,
    StrictStr,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
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
)

GenerationId = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
ServingSnapshotLoader = Callable[[datetime], "ServingRuntimeSnapshot"]


class ServingRuntimeSettings(RuntimeContractModel):
    serving_root: Path
    schema_version: StrictInt = Field(ge=1)

    @field_validator("serving_root")
    @classmethod
    def require_absolute_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("serving runtime root must be absolute")
        return value


class ServingRuntimeSnapshot(RuntimeContractModel):
    """One coherent as-of snapshot supplied without production database access."""

    read_model: ServingReadModelInput
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
    snapshot_loader: ServingSnapshotLoader,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    """Build a serving step whose only input is an injected typed snapshot."""

    if not callable(snapshot_loader):
        raise TypeError("snapshot_loader must be callable")
    if not callable(clock):
        raise TypeError("clock must be callable")

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.SERVING_PUBLISHER:
            raise ValueError("runtime service kind must be serving_publisher")
        if manifest.plane is not RuntimeServicePlane.SERVING:
            raise ValueError("serving publisher must run on the serving plane")

        settings = ServingRuntimeSettings.model_validate(dict(manifest.settings))
        publisher = ServingPublisher(
            settings.serving_root,
            producer_commit=manifest.producer_commit,
            schema_version=settings.schema_version,
            table_specs=SERVING_TABLE_SPECS,
        )

        def step() -> RuntimeStepResult:
            as_of = normalize_aware_utc(clock())
            snapshot = snapshot_loader(as_of)
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
            high_watermark = max(
                watermark.sequence for watermark in snapshot.watermarks
            )
            return RuntimeStepResult(
                input_sequence=high_watermark,
                output_sequence=high_watermark,
                processed_count=sum(generation.row_counts.values()),
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
    "ServingSnapshotLoader",
    "serving_publisher_builder",
]
