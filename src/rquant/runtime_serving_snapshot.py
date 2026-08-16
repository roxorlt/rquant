"""Assemble isolated owner reads into one point-in-time serving snapshot."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Annotated, Literal, Protocol

from pydantic import (
    Field,
    StrictInt,
    StrictStr,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.delivery_contracts import OutboxRecord
from rquant.experiment_registry import PromotionDecision
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.runtime_builder_serving import (
    ServingReferenceSlowEvidence,
    ServingRuntimeSnapshot,
)
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_service_control import RuntimeServiceHealth
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_read_models import (
    ServingLabJobRecord,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    ServingSignalRecord,
    ServingSignalRegistryRecord,
)
from rquant.signal_bus import SignalRouteReceipt, require_legacy_signal_write

SIGNALS_DATASET_ID = "signals"
PAPER_ACCOUNTS_DATASET_ID = "paper_accounts"
RUNTIME_HEALTH_DATASET_ID = "runtime_health"
LAB_JOBS_DATASET_ID = "lab_jobs"
PROMOTIONS_DATASET_ID = "promotions"
REFERENCE_SLOW_AUTHORITY_DATASET_ID = "reference_slow_authority"
REFERENCE_SLOW_DATASET_ID = "reference_slow"
REFERENCE_SLOW_CONTRACT_DATASET_ID = "reference_slow_contract"

GenerationId = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class SignalDeliveryPayload(RuntimeContractModel):
    payload_kind: Literal["signal_delivery"] = "signal_delivery"
    signals: tuple[ServingSignalRegistryRecord, ...] = ()
    routes: tuple[SignalRouteReceipt, ...] = ()
    deliveries: tuple[OutboxRecord, ...] = ()
    projections: tuple[ServingProjectionPayload, ...] = ()

    @field_validator("signals", mode="before")
    @classmethod
    def enforce_legacy_signal_writer(
        cls,
        value: object,
    ) -> object:
        if not isinstance(value, (tuple, list)):
            return value
        validated: list[ServingSignalRegistryRecord] = []
        for item in value:
            if type(item) in (ServingSignalRegistryRecord, ServingSignalRecord):
                sequence = item.global_sequence
                candidate = item.signal
            elif isinstance(item, Mapping):
                record = ServingSignalRecord.model_validate(item)
                sequence = record.global_sequence
                candidate = record.signal
            else:
                return value
            signal = require_legacy_signal_write(
                candidate,
                operation="SignalDeliveryPayload",
            )
            validated.append(
                ServingSignalRegistryRecord(
                    global_sequence=sequence,
                    signal=signal,
                )
            )
        return tuple(validated)


class SignalDeliveryReadPayload(RuntimeContractModel):
    payload_kind: Literal["signal_delivery"] = "signal_delivery"
    signals: tuple[ServingSignalRecord, ...] = ()
    routes: tuple[SignalRouteReceipt, ...] = ()
    deliveries: tuple[OutboxRecord, ...] = ()
    projections: tuple[ServingProjectionPayload, ...] = ()


class PaperAccountsPayload(RuntimeContractModel):
    payload_kind: Literal["paper_accounts"] = "paper_accounts"
    paper_accounts: tuple[PaperAccountSnapshot, ...] = ()
    projections: tuple[ServingProjectionPayload, ...] = ()


class RuntimeHealthPayload(RuntimeContractModel):
    payload_kind: Literal["runtime_health"] = "runtime_health"
    runtime_services: tuple[RuntimeServiceHealth, ...] = ()
    live_backlog_age_seconds: float | None = Field(
        default=None,
        ge=0,
        allow_inf_nan=False,
    )
    live_p95_latency_seconds: float | None = Field(
        default=None,
        ge=0,
        allow_inf_nan=False,
    )
    live_healthy: bool = False
    projections: tuple[ServingProjectionPayload, ...] = ()
    dashboard_summary_observed_at: AwareUtcDatetime | None = None
    dashboard_summary_generation_id: GenerationId | None = None
    dashboard_summary_source_receipts: Mapping[str, GenerationId] = Field(default_factory=dict)

    @field_validator("dashboard_summary_source_receipts", mode="after")
    @classmethod
    def freeze_dashboard_receipts(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        if any(not source_id for source_id in value):
            raise ValueError("dashboard summary source ids cannot be empty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("dashboard_summary_source_receipts")
    def serialize_dashboard_receipts(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def derive_live_slo(self) -> RuntimeHealthPayload:
        live_services = tuple(
            service for service in self.runtime_services if service.plane.value == "live"
        )
        ages = tuple(
            (service.observed_at - service.heartbeat.last_success_at).total_seconds()
            for service in live_services
            if service.heartbeat is not None and service.heartbeat.last_success_at is not None
        )
        values = tuple(
            service.heartbeat.p95_step_duration_seconds
            for service in live_services
            if service.heartbeat is not None
            and service.heartbeat.p95_step_duration_seconds is not None
        )
        backlog = max(ages) if ages else None
        p95 = max(values) if values else None
        healthy = bool(live_services) and all(
            service.status.value == "running"
            and not service.stale
            and service.heartbeat is not None
            and service.heartbeat.status.value == "running"
            and service.heartbeat.last_success_at is not None
            and service.heartbeat.p95_step_duration_seconds is not None
            for service in live_services
        )
        for name, expected in (
            ("live_backlog_age_seconds", backlog),
            ("live_p95_latency_seconds", p95),
            ("live_healthy", healthy),
        ):
            if name in self.model_fields_set and getattr(self, name) != expected:
                raise ValueError(f"{name} conflicts with runtime service evidence")
            object.__setattr__(self, name, expected)
        dashboard = tuple(
            projection
            for projection in self.projections
            if projection.table_name == "dashboard_summary"
        )
        evidence = (
            self.dashboard_summary_observed_at,
            self.dashboard_summary_generation_id,
            self.dashboard_summary_source_receipts,
        )
        if dashboard:
            if len(dashboard) != 1 or not all(evidence):
                raise ValueError("dashboard summary projection requires complete source evidence")
            expected_dashboard_generation = canonical_sha256(
                {
                    "contract": "runtime-health-dashboard-summary/v1",
                    "observed_at": self.dashboard_summary_observed_at,
                    "source_receipts": dict(self.dashboard_summary_source_receipts),
                    "projection": dashboard[0],
                }
            )
            if self.dashboard_summary_generation_id != expected_dashboard_generation:
                raise ValueError("dashboard summary generation does not match source evidence")
        elif any(evidence):
            raise ValueError("dashboard summary evidence requires its projection")
        return self


class LabJobsPayload(RuntimeContractModel):
    payload_kind: Literal["lab_jobs"] = "lab_jobs"
    lab_jobs: tuple[ServingLabJobRecord, ...] = ()
    projections: tuple[ServingProjectionPayload, ...] = ()


class PromotionsPayload(RuntimeContractModel):
    payload_kind: Literal["promotions"] = "promotions"
    promotions: tuple[PromotionDecision, ...] = ()
    projections: tuple[ServingProjectionPayload, ...] = ()


class ReferenceSlowPayload(ServingReferenceSlowEvidence):
    payload_kind: Literal["reference_slow"] = "reference_slow"
    projections: tuple[ServingProjectionPayload, ...] = ()


SourcePayload = Annotated[
    SignalDeliveryReadPayload
    | PaperAccountsPayload
    | RuntimeHealthPayload
    | LabJobsPayload
    | PromotionsPayload
    | ReferenceSlowPayload,
    Field(discriminator="payload_kind"),
]


class SourceReadResult(RuntimeContractModel):
    """One owner's immutable read result observed no later than the requested time."""

    dataset_id: str = Field(min_length=1)
    generation_id: GenerationId
    sequence: StrictInt = Field(ge=0)
    event_time: AwareUtcDatetime
    published_at: AwareUtcDatetime
    status: FreshnessStatus
    reason: str | None = Field(default=None, min_length=1)
    payload: SourcePayload

    @field_validator("payload", mode="before")
    @classmethod
    def adapt_registry_signal_payload(cls, value: object) -> object:
        if type(value) is not SignalDeliveryPayload:
            return value
        return SignalDeliveryReadPayload(
            signals=tuple(
                ServingSignalRecord(
                    global_sequence=record.global_sequence,
                    signal=record.signal,
                )
                for record in value.signals
            ),
            routes=value.routes,
            deliveries=value.deliveries,
            projections=value.projections,
        )

    @model_validator(mode="after")
    def validate_result(self) -> SourceReadResult:
        ServingDatasetWatermark(
            dataset_id=self.dataset_id,
            generation_id=self.generation_id,
            event_time=self.event_time,
            published_at=self.published_at,
            sequence=self.sequence,
            status=self.status,
            reason=self.reason,
        )
        if self.status is FreshnessStatus.UNAVAILABLE and not _payload_is_empty(self.payload):
            raise ValueError("unavailable source payload must be empty")
        return self


class SourceReader(Protocol):
    def __call__(self, as_of: AwareUtcDatetime, /) -> SourceReadResult: ...


SignalReader = SourceReader
PaperAccountsReader = SourceReader
RuntimeHealthReader = SourceReader
LabJobsReader = SourceReader
PromotionsReader = SourceReader
ReferenceSlowReader = SourceReader


def _payload_is_empty(payload: SourcePayload) -> bool:
    if isinstance(payload, SignalDeliveryReadPayload):
        return (
            not payload.signals
            and not payload.routes
            and not payload.deliveries
            and not payload.projections
        )
    if isinstance(payload, PaperAccountsPayload):
        return not payload.paper_accounts and not payload.projections
    if isinstance(payload, RuntimeHealthPayload):
        return not payload.runtime_services and not payload.projections
    if isinstance(payload, LabJobsPayload):
        return not payload.lab_jobs and not payload.projections
    if isinstance(payload, PromotionsPayload):
        return not payload.promotions and not payload.projections
    return False


def _error_text(error: BaseException) -> str:
    detail = str(error).strip()
    return type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"


class ServingSnapshotAssembler:
    """Read each owner exactly once and build a deterministic as-of snapshot."""

    def __init__(
        self,
        *,
        signal_reader: SignalReader,
        paper_accounts_reader: PaperAccountsReader,
        runtime_health_reader: RuntimeHealthReader,
        lab_jobs_reader: LabJobsReader,
        promotions_reader: PromotionsReader,
        reference_slow_reader: ReferenceSlowReader,
        fail_closed: bool = True,
    ) -> None:
        readers = (
            signal_reader,
            paper_accounts_reader,
            runtime_health_reader,
            lab_jobs_reader,
            promotions_reader,
            reference_slow_reader,
        )
        if any(not callable(reader) for reader in readers):
            raise TypeError("all serving source readers must be callable")
        if type(fail_closed) is not bool:
            raise TypeError("fail_closed must be bool")
        self.signal_reader = signal_reader
        self.paper_accounts_reader = paper_accounts_reader
        self.runtime_health_reader = runtime_health_reader
        self.lab_jobs_reader = lab_jobs_reader
        self.promotions_reader = promotions_reader
        self.reference_slow_reader = reference_slow_reader
        self.fail_closed = fail_closed

    def assemble(self, as_of: AwareUtcDatetime) -> ServingRuntimeSnapshot:
        observed_at = normalize_aware_utc(as_of)
        specifications: tuple[tuple[str, SourceReader, type[RuntimeContractModel]], ...] = (
            (SIGNALS_DATASET_ID, self.signal_reader, SignalDeliveryReadPayload),
            (
                PAPER_ACCOUNTS_DATASET_ID,
                self.paper_accounts_reader,
                PaperAccountsPayload,
            ),
            (
                RUNTIME_HEALTH_DATASET_ID,
                self.runtime_health_reader,
                RuntimeHealthPayload,
            ),
            (LAB_JOBS_DATASET_ID, self.lab_jobs_reader, LabJobsPayload),
            (PROMOTIONS_DATASET_ID, self.promotions_reader, PromotionsPayload),
            (
                REFERENCE_SLOW_AUTHORITY_DATASET_ID,
                self.reference_slow_reader,
                ReferenceSlowPayload,
            ),
        )
        reads = tuple(
            self._read_source(
                dataset_id=dataset_id,
                reader=reader,
                payload_type=payload_type,
                as_of=observed_at,
            )
            for dataset_id, reader, payload_type in specifications
        )

        dataset_ids = tuple(read.dataset_id for read in reads)
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("serving source readers returned a duplicate dataset")

        for (expected_id, _reader, payload_type), read in zip(specifications, reads, strict=True):
            if read.dataset_id != expected_id:
                raise ValueError(f"{expected_id} reader returned dataset {read.dataset_id}")
            if not isinstance(read.payload, payload_type):
                label = "signals" if expected_id == SIGNALS_DATASET_ID else expected_id
                raise TypeError(f"{label} payload has the wrong owner type")
            if read.event_time > observed_at or read.published_at > observed_at:
                raise ValueError(f"{expected_id} source contains future evidence")

        by_dataset = {read.dataset_id: read for read in reads}
        signal_payload = by_dataset[SIGNALS_DATASET_ID].payload
        paper_payload = by_dataset[PAPER_ACCOUNTS_DATASET_ID].payload
        runtime_payload = by_dataset[RUNTIME_HEALTH_DATASET_ID].payload
        lab_payload = by_dataset[LAB_JOBS_DATASET_ID].payload
        promotion_payload = by_dataset[PROMOTIONS_DATASET_ID].payload
        reference_payload = by_dataset[REFERENCE_SLOW_AUTHORITY_DATASET_ID].payload
        assert isinstance(signal_payload, SignalDeliveryReadPayload)
        assert isinstance(paper_payload, PaperAccountsPayload)
        assert isinstance(runtime_payload, RuntimeHealthPayload)
        assert isinstance(lab_payload, LabJobsPayload)
        assert isinstance(promotion_payload, PromotionsPayload)
        assert isinstance(reference_payload, ReferenceSlowPayload)
        if reference_payload.available_at > observed_at:
            raise ValueError("reference slow evidence contains future availability")
        reference_evidence = ServingReferenceSlowEvidence.model_validate(
            reference_payload.model_dump(exclude={"payload_kind", "projections"})
        )

        bound_projections = tuple(
            sorted(
                (
                    ServingProjectionInput.bind(
                        projection,
                        owner_dataset_id=read.dataset_id,
                        owner_generation_id=read.generation_id,
                    )
                    for read in reads
                    for projection in read.payload.projections
                ),
                key=lambda projection: projection.table_name,
            )
        )

        read_model = ServingReadModelInput(
            observed_at=observed_at,
            signals=tuple(sorted(signal_payload.signals, key=lambda item: item.global_sequence)),
            routes=tuple(
                sorted(
                    signal_payload.routes,
                    key=lambda item: (item.source_id, item.source_sequence),
                )
            ),
            deliveries=tuple(
                sorted(signal_payload.deliveries, key=lambda item: item.outbox_id or "")
            ),
            paper_accounts=tuple(
                sorted(paper_payload.paper_accounts, key=lambda item: item.account_id)
            ),
            runtime_services=tuple(
                sorted(runtime_payload.runtime_services, key=lambda item: item.service_id)
            ),
            lab_jobs=tuple(sorted(lab_payload.lab_jobs, key=lambda item: str(item.summary.job_id))),
            promotions=tuple(
                sorted(
                    promotion_payload.promotions,
                    key=lambda item: item.decision_id or "",
                )
            ),
            projections=bound_projections,
        )
        ordered_reads = tuple(sorted(reads, key=lambda item: item.dataset_id))
        reference_watermarks = (
            ServingDatasetWatermark(
                dataset_id=REFERENCE_SLOW_DATASET_ID,
                generation_id=reference_payload.reference_generation_id,
                event_time=reference_payload.available_at,
                published_at=reference_payload.available_at,
                sequence=reference_payload.revision,
                status=FreshnessStatus.FRESH,
            ),
            ServingDatasetWatermark(
                dataset_id=REFERENCE_SLOW_CONTRACT_DATASET_ID,
                generation_id=reference_payload.contract_generation_id,
                event_time=reference_payload.available_at,
                published_at=reference_payload.available_at,
                sequence=reference_payload.revision,
                status=FreshnessStatus.FRESH,
            ),
        )
        return ServingRuntimeSnapshot(
            read_model=read_model,
            reference_slow=reference_evidence,
            watermarks=tuple(
                sorted(
                    tuple(
                        ServingDatasetWatermark(
                            dataset_id=read.dataset_id,
                            generation_id=read.generation_id,
                            event_time=read.event_time,
                            published_at=read.published_at,
                            sequence=read.sequence,
                            status=read.status,
                            reason=read.reason,
                        )
                        for read in ordered_reads
                    )
                    + reference_watermarks,
                    key=lambda watermark: watermark.dataset_id,
                )
            ),
            source_generations={
                **{read.dataset_id: read.generation_id for read in ordered_reads},
                REFERENCE_SLOW_DATASET_ID: reference_payload.reference_generation_id,
                REFERENCE_SLOW_CONTRACT_DATASET_ID: (reference_payload.contract_generation_id),
            },
        )

    def _read_source(
        self,
        *,
        dataset_id: str,
        reader: SourceReader,
        payload_type: type[RuntimeContractModel],
        as_of: AwareUtcDatetime,
    ) -> SourceReadResult:
        try:
            result = reader(as_of)
        except Exception as error:
            if self.fail_closed:
                raise RuntimeError(f"{dataset_id} reader failed: {_error_text(error)}") from error
            if payload_type is ReferenceSlowPayload:
                raise RuntimeError(f"{dataset_id} reader failed: {_error_text(error)}") from error
            reason = _error_text(error)
            return SourceReadResult(
                dataset_id=dataset_id,
                generation_id=canonical_sha256(
                    {
                        "contract": "serving-source-unavailable/v1",
                        "dataset_id": dataset_id,
                        "as_of": as_of,
                        "reason": reason,
                    }
                ),
                sequence=0,
                event_time=as_of,
                published_at=as_of,
                status=FreshnessStatus.UNAVAILABLE,
                reason=reason,
                payload=payload_type(),
            )
        if not isinstance(result, SourceReadResult):
            raise TypeError(f"{dataset_id} reader must return SourceReadResult")
        return result


__all__ = [
    "LAB_JOBS_DATASET_ID",
    "PAPER_ACCOUNTS_DATASET_ID",
    "PROMOTIONS_DATASET_ID",
    "RUNTIME_HEALTH_DATASET_ID",
    "SIGNALS_DATASET_ID",
    "LabJobsPayload",
    "PaperAccountsPayload",
    "PromotionsPayload",
    "RuntimeHealthPayload",
    "ServingSnapshotAssembler",
    "SignalDeliveryPayload",
    "SignalDeliveryReadPayload",
    "SourceReadResult",
]
