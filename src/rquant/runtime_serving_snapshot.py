"""Assemble isolated owner reads into one point-in-time serving snapshot."""

from __future__ import annotations

from typing import Annotated, Literal, Protocol

from pydantic import Field, StrictInt, StrictStr, StringConstraints, model_validator

from rquant.delivery_contracts import OutboxRecord
from rquant.experiment_registry import PromotionDecision
from rquant.paper_contracts import PaperAccountSnapshot
from rquant.runtime_builder_serving import ServingRuntimeSnapshot
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
    ServingReadModelInput,
    ServingSignalRecord,
)
from rquant.signal_bus import SignalRouteReceipt

SIGNALS_DATASET_ID = "signals"
PAPER_ACCOUNTS_DATASET_ID = "paper_accounts"
RUNTIME_HEALTH_DATASET_ID = "runtime_health"
LAB_JOBS_DATASET_ID = "lab_jobs"
PROMOTIONS_DATASET_ID = "promotions"

GenerationId = Annotated[StrictStr, StringConstraints(pattern=r"^[0-9a-f]{64}$")]


class SignalDeliveryPayload(RuntimeContractModel):
    payload_kind: Literal["signal_delivery"] = "signal_delivery"
    signals: tuple[ServingSignalRecord, ...] = ()
    routes: tuple[SignalRouteReceipt, ...] = ()
    deliveries: tuple[OutboxRecord, ...] = ()


class PaperAccountsPayload(RuntimeContractModel):
    payload_kind: Literal["paper_accounts"] = "paper_accounts"
    paper_accounts: tuple[PaperAccountSnapshot, ...] = ()


class RuntimeHealthPayload(RuntimeContractModel):
    payload_kind: Literal["runtime_health"] = "runtime_health"
    runtime_services: tuple[RuntimeServiceHealth, ...] = ()


class LabJobsPayload(RuntimeContractModel):
    payload_kind: Literal["lab_jobs"] = "lab_jobs"
    lab_jobs: tuple[ServingLabJobRecord, ...] = ()


class PromotionsPayload(RuntimeContractModel):
    payload_kind: Literal["promotions"] = "promotions"
    promotions: tuple[PromotionDecision, ...] = ()


SourcePayload = Annotated[
    SignalDeliveryPayload
    | PaperAccountsPayload
    | RuntimeHealthPayload
    | LabJobsPayload
    | PromotionsPayload,
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
        if self.status is FreshnessStatus.UNAVAILABLE and not _payload_is_empty(
            self.payload
        ):
            raise ValueError("unavailable source payload must be empty")
        return self


class SourceReader(Protocol):
    def __call__(self, as_of: AwareUtcDatetime, /) -> SourceReadResult: ...


SignalReader = SourceReader
PaperAccountsReader = SourceReader
RuntimeHealthReader = SourceReader
LabJobsReader = SourceReader
PromotionsReader = SourceReader


def _payload_is_empty(payload: SourcePayload) -> bool:
    if isinstance(payload, SignalDeliveryPayload):
        return not payload.signals and not payload.routes and not payload.deliveries
    if isinstance(payload, PaperAccountsPayload):
        return not payload.paper_accounts
    if isinstance(payload, RuntimeHealthPayload):
        return not payload.runtime_services
    if isinstance(payload, LabJobsPayload):
        return not payload.lab_jobs
    return not payload.promotions


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
        fail_closed: bool = True,
    ) -> None:
        readers = (
            signal_reader,
            paper_accounts_reader,
            runtime_health_reader,
            lab_jobs_reader,
            promotions_reader,
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
        self.fail_closed = fail_closed

    def assemble(self, as_of: AwareUtcDatetime) -> ServingRuntimeSnapshot:
        observed_at = normalize_aware_utc(as_of)
        specifications: tuple[
            tuple[str, SourceReader, type[RuntimeContractModel]], ...
        ] = (
            (SIGNALS_DATASET_ID, self.signal_reader, SignalDeliveryPayload),
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

        for (expected_id, _reader, payload_type), read in zip(
            specifications, reads, strict=True
        ):
            if read.dataset_id != expected_id:
                raise ValueError(
                    f"{expected_id} reader returned dataset {read.dataset_id}"
                )
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
        assert isinstance(signal_payload, SignalDeliveryPayload)
        assert isinstance(paper_payload, PaperAccountsPayload)
        assert isinstance(runtime_payload, RuntimeHealthPayload)
        assert isinstance(lab_payload, LabJobsPayload)
        assert isinstance(promotion_payload, PromotionsPayload)

        read_model = ServingReadModelInput(
            observed_at=observed_at,
            signals=tuple(
                sorted(signal_payload.signals, key=lambda item: item.global_sequence)
            ),
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
            lab_jobs=tuple(
                sorted(lab_payload.lab_jobs, key=lambda item: str(item.summary.job_id))
            ),
            promotions=tuple(
                sorted(
                    promotion_payload.promotions,
                    key=lambda item: item.decision_id or "",
                )
            ),
        )
        ordered_reads = tuple(sorted(reads, key=lambda item: item.dataset_id))
        return ServingRuntimeSnapshot(
            read_model=read_model,
            watermarks=tuple(
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
            ),
            source_generations={
                read.dataset_id: read.generation_id for read in ordered_reads
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
    "SourceReadResult",
]
