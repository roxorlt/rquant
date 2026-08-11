"""Builders for isolated live and serving authority publishers."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from pydantic import Field, StrictInt, field_validator

from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.paper_execution_constraint_producer import (
    PaperExecutionConstraintProducer,
    PaperExecutionConstraintProductionRequest,
)
from rquant.paper_execution_constraints import PaperExecutionConstraintPublisher
from rquant.reference_data_registry import ReadonlyReferenceRegistry
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_service_control import (
    RuntimeServicePlane,
    RuntimeServiceSpec,
    RuntimeStepResult,
)
from rquant.runtime_service_entrypoint import (
    ArtifactTerminalOwnerStep,
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)

if TYPE_CHECKING:
    from rquant.runtime_artifact_terminal_lifecycle import ProductionArtifactTerminalLifecycle
    from rquant.runtime_health_authority import RuntimeHealthControlSource
    from rquant.serving_read_models import ServingProjectionPayload

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class PaperConstraintRuntimeSettings(RuntimeContractModel):
    minute_spool_root: Path
    reference_registry_path: Path
    authority_root: Path
    quote_ttl_seconds: StrictInt = Field(default=120, gt=0, le=900)

    @field_validator("minute_spool_root", "reference_registry_path", "authority_root")
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("runtime authority paths must be absolute")
        return value


class RuntimeHealthSourceSettings(RuntimeContractModel):
    control_root: Path
    service_id: str = Field(min_length=1)
    plane: RuntimeServicePlane
    stale_after_seconds: float = Field(gt=0)
    producer_commit: str = Field(pattern=r"^[0-9a-f]{40}$")

    @field_validator("control_root")
    @classmethod
    def require_absolute_control_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("runtime health control roots must be absolute")
        return value

    def source(self) -> RuntimeHealthControlSource:
        from rquant.runtime_health_authority import RuntimeHealthControlSource

        return RuntimeHealthControlSource(
            control_root=self.control_root,
            spec=RuntimeServiceSpec(
                service_id=self.service_id,
                plane=self.plane,
                stale_after=timedelta(seconds=self.stale_after_seconds),
                producer_commit=self.producer_commit,
            ),
        )


class RuntimeHealthPublisherSettings(RuntimeContractModel):
    authority_root: Path
    sources: tuple[RuntimeHealthSourceSettings, ...] = Field(min_length=1)

    @field_validator("authority_root")
    @classmethod
    def require_absolute_authority_root(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("runtime health authority root must be absolute")
        return value


class LabJobsPublisherSettings(RuntimeContractModel):
    lab_jobs_path: Path
    research_metadata_path: Path | None = None
    authority_root: Path
    max_jobs: StrictInt = Field(default=100, gt=0, le=100)
    eta_completed_limit: StrictInt = Field(default=256, ge=3, le=256)

    @field_validator("lab_jobs_path", "research_metadata_path", "authority_root")
    @classmethod
    def require_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute():
            raise ValueError("lab jobs authority paths must be absolute")
        return value


class PromotionsPublisherSettings(RuntimeContractModel):
    experiment_registry_path: Path
    experiment_registry_managed_trust_root: Path
    authority_root: Path
    max_decisions: StrictInt = Field(default=1_000, gt=0, le=10_000)

    @field_validator(
        "experiment_registry_path",
        "experiment_registry_managed_trust_root",
        "authority_root",
    )
    @classmethod
    def require_absolute_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("promotions authority paths must be absolute")
        return value


def _latest_visible_market_batch(
    spool: LiveBatchSpool,
    *,
    observed_at: datetime,
):
    visible = tuple(
        record
        for record in spool.list_after(LiveChannel.MARKET_MINUTE, sequence=-1)
        if record.envelope.available_at <= observed_at
    )
    if not visible:
        raise RuntimeError("paper constraints require a visible market-minute batch")
    latest = visible[-1]
    if latest.envelope.quality_status is not BatchQualityStatus.PUBLISHED:
        raise RuntimeError(
            "paper constraints require the latest visible market-minute batch to be published"
        )
    return latest


def paper_execution_constraint_publisher_builder(
    *,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER:
            raise ValueError("runtime service kind must be paper_constraint_publisher")
        if manifest.plane is not RuntimeServicePlane.LIVE:
            raise ValueError("paper constraint publisher must run on the live plane")
        settings = PaperConstraintRuntimeSettings.model_validate(dict(manifest.settings))
        spool = LiveBatchSpool(settings.minute_spool_root)
        registry = ReadonlyReferenceRegistry(settings.reference_registry_path)
        publisher = PaperExecutionConstraintPublisher(
            root=settings.authority_root,
            producer_commit=manifest.producer_commit,
            clock=clock,
        )
        producer = PaperExecutionConstraintProducer(
            reference_registry=registry,
            minute_spool=spool,
            publisher=publisher,
            producer_commit=manifest.producer_commit,
            quote_ttl=timedelta(seconds=settings.quote_ttl_seconds),
        )

        def step() -> RuntimeStepResult:
            observed_at = clock()
            latest = _latest_visible_market_batch(spool, observed_at=observed_at)
            frame = MarketMinuteGateway.decode_payload(spool.read_payload(latest))
            if "ts_code" not in frame.columns:
                raise RuntimeError("market-minute batch is missing ts_code")
            codes = tuple(sorted({str(value) for value in frame["ts_code"].tolist()}))
            if not codes:
                raise RuntimeError("market-minute batch contains no paper constraint codes")
            reference = registry.current_pointer()
            if reference.switched_at > observed_at:
                raise RuntimeError("current reference generation is future evidence")
            publication = producer.produce(
                PaperExecutionConstraintProductionRequest(
                    trade_date=observed_at.astimezone(_SHANGHAI).date(),
                    ts_codes=codes,
                    observed_at=observed_at,
                    reference_generation_id=reference.generation_id,
                    sequence=latest.envelope.sequence,
                )
            )
            return RuntimeStepResult(
                input_sequence=latest.envelope.sequence,
                output_sequence=publication.pointer.sequence,
                processed_count=len(publication.batch.records),
                source_generations={
                    "market_minute": latest.envelope.identity_sha256,
                    "reference_slow": reference.generation_id,
                    "paper_execution_constraints": publication.pointer.batch_hash,
                },
            )

        return step

    return build


def runtime_health_publisher_builder(
    *,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER:
            raise ValueError("runtime service kind must be runtime_health_publisher")
        if manifest.plane is not RuntimeServicePlane.SERVING:
            raise ValueError("runtime health publisher must run on the serving plane")
        settings = RuntimeHealthPublisherSettings.model_validate(dict(manifest.settings))
        from rquant.runtime_health_authority import RuntimeHealthSourceReader
        from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
        from rquant.runtime_serving_snapshot import RUNTIME_HEALTH_DATASET_ID

        reader = RuntimeHealthSourceReader(
            sources=tuple(source.source() for source in settings.sources),
            serving_service_id=manifest.service_id,
        )
        publisher = ServingSourceAuthorityPublisher(
            root=settings.authority_root,
            producer_commit=manifest.producer_commit,
            dataset_id=RUNTIME_HEALTH_DATASET_ID,
            payload_kind="runtime_health",
            clock=clock,
        )

        def step() -> RuntimeStepResult:
            source = reader(clock())
            pointer = publisher.publish(source)
            return RuntimeStepResult(
                input_sequence=source.sequence,
                output_sequence=source.sequence,
                processed_count=len(settings.sources),
                source_generations={RUNTIME_HEALTH_DATASET_ID: pointer.generation_id},
            )

        return step

    return build


def lab_jobs_publisher_builder(
    *,
    clock: Callable[[], datetime],
    open_artifact_terminal_lifecycle: (
        Callable[[], ProductionArtifactTerminalLifecycle] | None
    ) = None,
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.LAB_JOBS_PUBLISHER:
            raise ValueError("runtime service kind must be lab_jobs_publisher")
        if manifest.plane is not RuntimeServicePlane.RESEARCH:
            raise ValueError("lab jobs publisher must run on the research plane")
        settings = LabJobsPublisherSettings.model_validate(dict(manifest.settings))
        from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
        from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher

        publisher = ServingSourceAuthorityPublisher(
            root=settings.authority_root,
            producer_commit=manifest.producer_commit,
            dataset_id="lab_jobs",
            payload_kind="lab_jobs",
            clock=clock,
        )
        if open_artifact_terminal_lifecycle is None:
            raise RuntimeError("lab jobs publisher requires the production terminal lifecycle")
        lifecycle = open_artifact_terminal_lifecycle()

        try:
            lab_job_reader = lifecycle.lab_job_reader
            if lab_job_reader is None:
                raise RuntimeError("lab jobs lifecycle reader capability is missing")
            if lab_job_reader.path != settings.lab_jobs_path:
                raise ValueError("lab jobs lifecycle reader path conflicts with manifest")
            page_projection_reader = None
            if settings.research_metadata_path is not None:
                from rquant.serving_page_projection_source import DuckDBLabPageProjectionSource

                page_source = DuckDBLabPageProjectionSource(settings.research_metadata_path)

                def page_projection_reader(
                    observed_at: datetime,
                ) -> tuple[ServingProjectionPayload, ...]:
                    return page_source(observed_at).projections

            reader = LabJobsServingSourceReader(
                reader=lab_job_reader,
                max_jobs=settings.max_jobs,
                eta_completed_limit=settings.eta_completed_limit,
                page_projection_reader=page_projection_reader,
            )
            def step() -> RuntimeStepResult:
                source = reader(clock())
                pointer = publisher.publish(source)
                return RuntimeStepResult(
                    input_sequence=source.sequence,
                    output_sequence=source.sequence,
                    processed_count=len(source.payload.lab_jobs),
                    source_generations={"lab_jobs": pointer.generation_id},
                )
        except BaseException:
            lifecycle.close()
            raise
        return ArtifactTerminalOwnerStep(
            step=step,
            artifact_terminal_lifecycle=lifecycle,
        )

    return build


def promotions_publisher_builder(
    *,
    clock: Callable[[], datetime],
    open_artifact_terminal_lifecycle: (
        Callable[[], ProductionArtifactTerminalLifecycle] | None
    ) = None,
) -> RuntimeServiceBuilder:
    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.PROMOTIONS_PUBLISHER:
            raise ValueError("runtime service kind must be promotions_publisher")
        if manifest.plane is not RuntimeServicePlane.RESEARCH:
            raise ValueError("promotions publisher must run on the research plane")
        settings = PromotionsPublisherSettings.model_validate(dict(manifest.settings))
        if open_artifact_terminal_lifecycle is None:
            raise RuntimeError("promotions publisher requires the production terminal lifecycle")
        lifecycle = open_artifact_terminal_lifecycle()
        from rquant.promotions_serving_authority import PromotionsSourceReader
        from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher

        try:
            experiment_registry_reader = lifecycle.experiment_registry_reader
            if experiment_registry_reader is None:
                raise RuntimeError("promotions lifecycle reader capability is missing")
            if experiment_registry_reader.path != settings.experiment_registry_path:
                raise ValueError("promotions lifecycle registry path conflicts with manifest")
            reader = PromotionsSourceReader(
                registry=experiment_registry_reader,
                limit=settings.max_decisions,
            )
            publisher = ServingSourceAuthorityPublisher(
                root=settings.authority_root,
                producer_commit=manifest.producer_commit,
                dataset_id="promotions",
                payload_kind="promotions",
                clock=clock,
            )

            def step() -> RuntimeStepResult:
                source = reader(clock())
                pointer = publisher.publish(source)
                return RuntimeStepResult(
                    input_sequence=source.sequence,
                    output_sequence=source.sequence,
                    processed_count=len(source.payload.promotions),
                    source_generations={"promotions": pointer.generation_id},
                )
        except BaseException:
            lifecycle.close()
            raise
        return ArtifactTerminalOwnerStep(
            step=step,
            artifact_terminal_lifecycle=lifecycle,
        )

    return build


__all__ = [
    "PaperConstraintRuntimeSettings",
    "LabJobsPublisherSettings",
    "PromotionsPublisherSettings",
    "RuntimeHealthPublisherSettings",
    "RuntimeHealthSourceSettings",
    "lab_jobs_publisher_builder",
    "paper_execution_constraint_publisher_builder",
    "promotions_publisher_builder",
    "runtime_health_publisher_builder",
]
