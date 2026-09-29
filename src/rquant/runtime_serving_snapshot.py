"""Assemble isolated owner reads into one point-in-time serving snapshot."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Annotated, Literal, Protocol

from loguru import logger
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
from rquant.lab_jobs import JobStatus
from rquant.ops_status import OpsSnapshot
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.runtime_builder_serving import (
    DEFAULT_OPTIONAL_SOURCE_DATASETS,
    SERVING_SOURCE_DATASET_IDS,
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
from rquant.serving_alert_projection import build_alert_read_projections
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_read_models import (
    LAB_EVENT_ALLOWED_LABELS,
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
STRATEGY_CATALOG_DATASET_ID = "strategy_catalog"
OPS_STATUS_DATASET_ID = "ops_status"
REFERENCE_SLOW_AUTHORITY_DATASET_ID = "reference_slow_authority"
REFERENCE_SLOW_DATASET_ID = "reference_slow"
REFERENCE_SLOW_CONTRACT_DATASET_ID = "reference_slow_contract"

#: Not a second copy of the seven: the owner datasets are named once, by
#: `runtime_builder_serving._SOURCE_PAYLOAD_KINDS`, and both guards read that one list.
SOURCE_DATASET_IDS: frozenset[str] = SERVING_SOURCE_DATASET_IDS

#: What an unavailable source stamps on its watermark instead of the clock. It is before
#: any evidence this system can hold, which is the truthful reading of "there is nothing
#: here", and it is constant, which is what keeps the generation identity still while a
#: source stays away.
UNAVAILABLE_EVIDENCE_INSTANT = datetime(1970, 1, 1, tzinfo=UTC)

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

    @model_validator(mode="after")
    def validate_event_windows(self) -> LabJobsPayload:
        from rquant.factor.serving_projection import (
            FACTOR_DEFINITION_PROJECTION_TABLES,
            validate_factor_definition_projections,
        )

        factor_projections = tuple(
            projection
            for projection in self.projections
            if projection.table_name in FACTOR_DEFINITION_PROJECTION_TABLES
        )
        if len(factor_projections) != len(
            {projection.table_name for projection in factor_projections}
        ):
            raise ValueError("factor definition Lab projections contain duplicate tables")
        validate_factor_definition_projections(
            {projection.table_name: projection for projection in factor_projections}
        )
        event_tables = {
            projection.table_name: projection
            for projection in self.projections
            if projection.table_name in {"lab_job_event_window", "lab_job_event"}
        }
        if not event_tables:
            return self  # Older Lab authority generations have no event publication.
        if len(event_tables) != 2 or sum(
            item.table_name in event_tables for item in self.projections
        ) != 2:
            raise ValueError("lab event projections require one complete table pair")
        windows = event_tables["lab_job_event_window"]
        events = event_tables["lab_job_event"]
        if windows.available_at != events.available_at:
            raise ValueError("lab event projections have different snapshot times")
        jobs = {str(item.summary.job_id): item.summary for item in self.lab_jobs}
        if len(jobs) != len(self.lab_jobs):
            raise ValueError("lab event jobs contain duplicate identities")
        window_rows = {str(row["job_id"]): row for row in windows.rows}
        if set(window_rows) != set(jobs):
            raise ValueError("lab event windows do not match published jobs")
        event_rows: dict[str, list[Mapping[str, object]]] = {job_id: [] for job_id in jobs}
        for row in events.rows:
            job_id = row["job_id"]
            if job_id not in event_rows:
                raise ValueError("lab event refers to a job outside this publication")
            if row["label"] not in LAB_EVENT_ALLOWED_LABELS:
                raise ValueError("lab event label is not registered")
            if row["new_status"] not in {status.value for status in JobStatus}:
                raise ValueError("lab event status is invalid")
            if type(row["event_id"]) is not int or row["event_id"] < 1:
                raise ValueError("lab event ID is invalid")
            if type(row["job_version"]) is not int or row["job_version"] < 0:
                raise ValueError("lab event version is invalid")
            event_rows[job_id].append(row)
        for job_id, window in window_rows.items():
            summary = jobs[job_id]
            entries = event_rows[job_id]
            count = window["retained_count"]
            truncated = window["truncated"]
            if (
                type(count) is not int
                or not 0 <= count <= 500
                or type(truncated) is not bool
                or count != len(entries)
                or window["job_version"] != summary.version
                or window["state"]
                != ("truncated" if truncated else "available" if count else "empty")
                or (truncated and count == 0)
            ):
                raise ValueError("lab event window conflicts with published job")
            ordered = sorted(entries, key=lambda item: item["event_id"], reverse=True)
            if ordered and (
                ordered[0]["job_version"] != summary.version
                or ordered[0]["new_status"] != summary.status.value
            ):
                raise ValueError("lab event latest state conflicts with published job")
            if any(
                older["job_version"] != newer["job_version"] - 1
                for newer, older in zip(ordered, ordered[1:], strict=False)
            ):
                raise ValueError("lab event versions are not consecutive")
            if ordered and not truncated and ordered[-1]["job_version"] != 0:
                raise ValueError("lab event complete window has a missing first version")
        return self


class PromotionsPayload(RuntimeContractModel):
    payload_kind: Literal["promotions"] = "promotions"
    promotions: tuple[PromotionDecision, ...] = ()
    projections: tuple[ServingProjectionPayload, ...] = ()


class OpsStatusPayload(RuntimeContractModel):
    payload_kind: Literal["ops_status"] = "ops_status"
    snapshot: OpsSnapshot | None = None
    projections: tuple[ServingProjectionPayload, ...] = ()

    @model_validator(mode="after")
    def validate_projection_set(self) -> OpsStatusPayload:
        expected = {"ops_host_status", "ops_unit_status", "ops_resource_status"}
        observed = {projection.table_name for projection in self.projections}
        if self.snapshot is None:
            if observed:
                raise ValueError("ops projections require a source snapshot")
        elif observed != expected or len(self.projections) != len(expected):
            raise ValueError("ops snapshot requires its exact serving projections")
        else:
            from rquant.ops_status_serving import ops_status_projections

            if self.projections != ops_status_projections(self.snapshot):
                raise ValueError("ops projections must match sample evidence")
        return self


class ReferenceSlowPayload(ServingReferenceSlowEvidence):
    payload_kind: Literal["reference_slow"] = "reference_slow"
    projections: tuple[ServingProjectionPayload, ...] = ()


class StrategyCatalogPayload(RuntimeContractModel):
    payload_kind: Literal["strategy_catalog"] = "strategy_catalog"
    source_digest: GenerationId | None = None
    runtime_generation_id: GenerationId | None = None
    projections: tuple[ServingProjectionPayload, ...] = ()

    @model_validator(mode="after")
    def validate_projection_pair(self) -> StrategyCatalogPayload:
        names = tuple(projection.table_name for projection in self.projections)
        if self.source_digest is None and self.runtime_generation_id is None and not names:
            return self
        if self.source_digest is None or self.runtime_generation_id is None or set(names) != {
            "strategy_catalog", "strategy_catalog_parameter"
        } or len(names) != 2:
            raise ValueError("strategy catalog requires a complete source and projection pair")
        return self


SourcePayload = Annotated[
    SignalDeliveryReadPayload
    | PaperAccountsPayload
    | RuntimeHealthPayload
    | LabJobsPayload
    | PromotionsPayload
    | OpsStatusPayload
    | StrategyCatalogPayload
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
OpsStatusReader = SourceReader
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
    if isinstance(payload, OpsStatusPayload):
        return payload.snapshot is None and not payload.projections
    if isinstance(payload, StrategyCatalogPayload):
        return (
            payload.source_digest is None
            and payload.runtime_generation_id is None
            and not payload.projections
        )
    return False


def _error_text(error: BaseException) -> str:
    detail = str(error).strip()
    return type(error).__name__ if not detail else f"{type(error).__name__}: {detail}"


def _missing_ops_status(_as_of: AwareUtcDatetime) -> SourceReadResult:
    from rquant.runtime_serving_authority import ServingSourceAuthorityUnavailableError

    raise ServingSourceAuthorityUnavailableError("ops status collector is not installed")


def _missing_strategy_catalog(_as_of: AwareUtcDatetime) -> SourceReadResult:
    from rquant.runtime_serving_authority import ServingSourceAuthorityUnavailableError

    raise ServingSourceAuthorityUnavailableError("strategy catalog source is not installed")


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
        ops_status_reader: OpsStatusReader | None = None,
        strategy_catalog_reader: SourceReader | None = None,
        expected_ops_manifest_digest: GenerationId | None = None,
        optional_datasets: frozenset[str] = DEFAULT_OPTIONAL_SOURCE_DATASETS,
    ) -> None:
        selected_ops_reader = (
            _missing_ops_status if ops_status_reader is None else ops_status_reader
        )
        selected_catalog_reader = (
            _missing_strategy_catalog
            if strategy_catalog_reader is None
            else strategy_catalog_reader
        )
        readers = (
            signal_reader,
            paper_accounts_reader,
            runtime_health_reader,
            lab_jobs_reader,
            promotions_reader,
            reference_slow_reader,
            selected_ops_reader,
            selected_catalog_reader,
        )
        if any(not callable(reader) for reader in readers):
            raise TypeError("all serving source readers must be callable")
        if not isinstance(optional_datasets, frozenset):
            raise TypeError("optional_datasets must be a frozenset")
        unknown = sorted(optional_datasets.difference(SOURCE_DATASET_IDS))
        if unknown:
            raise ValueError(
                f"optional_datasets names datasets this assembler never reads: {unknown}"
            )
        if REFERENCE_SLOW_AUTHORITY_DATASET_ID in optional_datasets:
            # The serving row's price and adjustment basis is quoted from this payload, and
            # `ReferenceSlowPayload` is the one source payload without a legal empty value
            # (`:217-219`): every field it carries is required, so "degraded" could only
            # mean an invented reference generation id in the evidence a consumer prices
            # against. It is refused here as well as at the read, so a profile that asks
            # for it is rejected at build rather than silently ignored.
            raise ValueError("reference_slow_authority can never be an optional serving source")
        self.signal_reader = signal_reader
        self.paper_accounts_reader = paper_accounts_reader
        self.runtime_health_reader = runtime_health_reader
        self.lab_jobs_reader = lab_jobs_reader
        self.promotions_reader = promotions_reader
        self.ops_status_reader = selected_ops_reader
        self.strategy_catalog_reader = selected_catalog_reader
        self.expected_ops_manifest_digest = expected_ops_manifest_digest
        self.reference_slow_reader = reference_slow_reader
        self.optional_datasets = optional_datasets
        self._last_ops_security_reason: str | None = None

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
            (OPS_STATUS_DATASET_ID, self.ops_status_reader, OpsStatusPayload),
            (
                STRATEGY_CATALOG_DATASET_ID,
                self.strategy_catalog_reader,
                StrategyCatalogPayload,
            ),
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
        ops_payload = by_dataset[OPS_STATUS_DATASET_ID].payload
        reference_payload = by_dataset[REFERENCE_SLOW_AUTHORITY_DATASET_ID].payload
        assert isinstance(signal_payload, SignalDeliveryReadPayload)
        assert isinstance(paper_payload, PaperAccountsPayload)
        assert isinstance(runtime_payload, RuntimeHealthPayload)
        assert isinstance(lab_payload, LabJobsPayload)
        assert isinstance(promotion_payload, PromotionsPayload)
        assert isinstance(ops_payload, OpsStatusPayload)
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

        base_read_model = ServingReadModelInput(
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
        if {item.table_name for item in bound_projections} & {
            "alert_event",
            "alert_source_coverage",
            "alert_overview",
        }:
            raise ValueError("derived alert projections cannot be supplied by an owner")
        alert_projections = tuple(
            ServingProjectionInput.bind(
                projection,
                owner_dataset_id=SIGNALS_DATASET_ID,
                owner_generation_id=by_dataset[SIGNALS_DATASET_ID].generation_id,
            )
            for projection in build_alert_read_projections(
                base_read_model,
                signal_generation_id=by_dataset[SIGNALS_DATASET_ID].generation_id,
            )
        )
        read_model = ServingReadModelInput(
            **base_read_model.model_dump(mode="python", exclude={"projections"}),
            projections=tuple(
                sorted((*bound_projections, *alert_projections), key=lambda p: p.table_name)
            ),
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
            if dataset_id == OPS_STATUS_DATASET_ID:
                from rquant.runtime_serving_authority import (
                    ServingSourceAuthorityIntegrityError,
                    ServingSourceAuthorityUnavailableError,
                )

                if not isinstance(result, SourceReadResult) or not isinstance(
                    result.payload, OpsStatusPayload
                ):
                    raise ServingSourceAuthorityIntegrityError("ops source payload is invalid")
                if result.status is not FreshnessStatus.UNAVAILABLE:
                    sample = result.payload.snapshot
                    if sample is None or sample.sampled_at != result.event_time:
                        raise ServingSourceAuthorityIntegrityError("ops sample identity is invalid")
                    if (
                        self.expected_ops_manifest_digest is None
                        or sample.manifest_digest != self.expected_ops_manifest_digest
                    ):
                        raise ServingSourceAuthorityIntegrityError(
                            "ops sample manifest digest does not match the serving manifest"
                        )
                    if sample.sampled_at > as_of:
                        raise ServingSourceAuthorityIntegrityError("ops sample is in the future")
                    if (as_of - sample.sampled_at).total_seconds() >= 120:
                        raise ServingSourceAuthorityUnavailableError(
                            "ops sample is at least 120 seconds old"
                        )
                    self._last_ops_security_reason = None
        except Exception as error:
            # The authority reader imports this module for SourceReadResult, so keep the
            # classified exception import local to the failure path.
            from rquant.runtime_serving_authority import (
                ServingSourceAuthorityIntegrityError,
                ServingSourceAuthorityUnavailableError,
            )

            classified = isinstance(error, ServingSourceAuthorityUnavailableError) or (
                dataset_id in {OPS_STATUS_DATASET_ID, STRATEGY_CATALOG_DATASET_ID}
                and isinstance(error, ServingSourceAuthorityIntegrityError)
            )
            if (
                dataset_id not in self.optional_datasets
                or payload_type is ReferenceSlowPayload
                or not classified
            ):
                raise RuntimeError(f"{dataset_id} reader failed: {_error_text(error)}") from error
            reason = _error_text(error)
            if (
                dataset_id == OPS_STATUS_DATASET_ID
                and reason != self._last_ops_security_reason
            ):
                self._last_ops_security_reason = reason
                logger.bind(
                    ops_status_security_event={"dataset_id": dataset_id, "reason": reason}
                ).warning("ops_status_source_unavailable")
            return SourceReadResult(
                dataset_id=dataset_id,
                #: Named by the refusal and nothing else. `as_of` used to be part of this
                #: identity, which gave an absent source a new generation id on every
                #: thirty-second iteration; `_generation_already_current` compares source
                #: generations and watermarks for equality (#271), so serving would have
                #: rebuilt and re-pointed `serving.duckdb` every iteration for as long as
                #: the source stayed away. A source that is not there has no evidence and
                #: therefore no instant of its own: the identity a missing source
                #: contributes is a function of which source it is and why it refused.
                generation_id=canonical_sha256(
                    {
                        "contract": "serving-source-unavailable/v1",
                        "dataset_id": dataset_id,
                        "reason": reason,
                    }
                ),
                sequence=0,
                #: the same reason, for the watermark's own two instants: they are compared
                #: by the same gate, and the epoch says "no evidence at all" without
                #: claiming the read observed anything at this clock
                event_time=UNAVAILABLE_EVIDENCE_INSTANT,
                published_at=UNAVAILABLE_EVIDENCE_INSTANT,
                status=FreshnessStatus.UNAVAILABLE,
                reason=reason,
                payload=payload_type(),
            )
        if not isinstance(result, SourceReadResult):
            raise TypeError(f"{dataset_id} reader must return SourceReadResult")
        return result


__all__ = [
    "DEFAULT_OPTIONAL_SOURCE_DATASETS",
    "LAB_JOBS_DATASET_ID",
    "OPS_STATUS_DATASET_ID",
    "PAPER_ACCOUNTS_DATASET_ID",
    "PROMOTIONS_DATASET_ID",
    "RUNTIME_HEALTH_DATASET_ID",
    "SIGNALS_DATASET_ID",
    "SOURCE_DATASET_IDS",
    "UNAVAILABLE_EVIDENCE_INSTANT",
    "LabJobsPayload",
    "PaperAccountsPayload",
    "PromotionsPayload",
    "RuntimeHealthPayload",
    "ServingSnapshotAssembler",
    "SignalDeliveryPayload",
    "SignalDeliveryReadPayload",
    "SourceReadResult",
]
