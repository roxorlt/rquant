"""Publish bounded point-in-time Lab Jobs serving source generations."""

from __future__ import annotations

from datetime import UTC, datetime

from rquant.lab_jobs import (
    LAB_ETA_COMPLETED_LIMIT_MAX,
    LAB_JOB_LIST_LIMIT_MAX,
    LabJobPage,
    LabJobReader,
)
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPointer,
    ServingSourceAuthorityPublisher,
)
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    LabJobsPayload,
    SourceReadResult,
)
from rquant.serving_contracts import FreshnessStatus
from rquant.serving_read_models import ServingLabJobRecord


class LabJobsServingAuthorityIntegrityError(RuntimeError):
    """The Lab Jobs read cannot be represented as a trustworthy PIT source."""


class LabJobsServingSourceReader:
    """Read a bounded, stable Lab Jobs projection without mutating its SQLite store."""

    def __init__(
        self,
        *,
        reader: LabJobReader,
        max_jobs: int = LAB_JOB_LIST_LIMIT_MAX,
        eta_completed_limit: int = LAB_ETA_COMPLETED_LIMIT_MAX,
    ) -> None:
        if not isinstance(reader, LabJobReader):
            raise TypeError("reader must be LabJobReader")
        if not 1 <= max_jobs <= LAB_JOB_LIST_LIMIT_MAX:
            raise ValueError(f"max_jobs must be between 1 and {LAB_JOB_LIST_LIMIT_MAX}")
        if not 3 <= eta_completed_limit <= LAB_ETA_COMPLETED_LIMIT_MAX:
            raise ValueError(
                f"eta_completed_limit must be between 3 and {LAB_ETA_COMPLETED_LIMIT_MAX}"
            )
        self.reader = reader
        self.max_jobs = max_jobs
        self.eta_completed_limit = eta_completed_limit

    def __call__(self, observed_at: datetime, /) -> SourceReadResult:
        observed = normalize_aware_utc(observed_at)
        first_page = self.reader.list_jobs(limit=self.max_jobs)
        self._validate_summaries(first_page, observed_at=observed)

        records = tuple(
            ServingLabJobRecord(
                summary=summary,
                eta=self.reader.estimate_eta(
                    summary.job_id,
                    as_of=observed,
                    completed_limit=self.eta_completed_limit,
                ),
            )
            for summary in first_page.items
        )
        self._validate_eta(records, observed_at=observed)

        second_page = self.reader.list_jobs(limit=self.max_jobs)
        if second_page != first_page:
            raise LabJobsServingAuthorityIntegrityError(
                "lab jobs changed while building serving source"
            )

        payload = LabJobsPayload(
            lab_jobs=tuple(
                sorted(
                    records,
                    key=lambda record: (
                        record.summary.created_at,
                        str(record.summary.job_id),
                    ),
                    reverse=True,
                )
            )
        )
        values: dict[str, object] = {
            "dataset_id": LAB_JOBS_DATASET_ID,
            "sequence": _sequence_for(observed),
            "event_time": max(
                (
                    timestamp
                    for record in payload.lab_jobs
                    for timestamp in (
                        record.summary.updated_at,
                        record.eta.as_of if record.eta is not None else None,
                    )
                    if timestamp is not None
                ),
                default=observed,
            ),
            "published_at": observed,
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": payload,
        }
        values["generation_id"] = canonical_sha256(values)
        return SourceReadResult.model_validate(values)

    @staticmethod
    def _validate_summaries(page: LabJobPage, *, observed_at: datetime) -> None:
        if any(
            summary.created_at > observed_at or summary.updated_at > observed_at
            for summary in page.items
        ):
            raise LabJobsServingAuthorityIntegrityError("lab job summary contains future evidence")

    @staticmethod
    def _validate_eta(
        records: tuple[ServingLabJobRecord, ...],
        *,
        observed_at: datetime,
    ) -> None:
        if any(record.eta is not None and record.eta.as_of > observed_at for record in records):
            raise LabJobsServingAuthorityIntegrityError("lab job ETA contains future evidence")


class LabJobsServingAuthorityPublisher:
    """Publish one verified Lab Jobs projection through its owner authority."""

    def __init__(
        self,
        *,
        reader: LabJobsServingSourceReader,
        publisher: ServingSourceAuthorityPublisher,
    ) -> None:
        if not isinstance(reader, LabJobsServingSourceReader):
            raise TypeError("reader must be LabJobsServingSourceReader")
        if not isinstance(publisher, ServingSourceAuthorityPublisher):
            raise TypeError("publisher must be ServingSourceAuthorityPublisher")
        if publisher.dataset_id != LAB_JOBS_DATASET_ID:
            raise ValueError("publisher must own the lab_jobs dataset")
        if publisher.payload_kind != "lab_jobs":
            raise ValueError("publisher must own the lab_jobs payload kind")
        self.reader = reader
        self.publisher = publisher

    def publish(self, observed_at: datetime) -> ServingSourceAuthorityPointer:
        return self.publisher.publish(self.reader(observed_at))


def _sequence_for(observed_at: datetime) -> int:
    elapsed = observed_at - datetime(1970, 1, 1, tzinfo=UTC)
    sequence = elapsed.days * 86_400_000_000 + elapsed.seconds * 1_000_000 + elapsed.microseconds
    if sequence < 0:
        raise ValueError("observed_at must not precede the Unix epoch")
    return sequence


__all__ = [
    "LabJobsServingAuthorityIntegrityError",
    "LabJobsServingAuthorityPublisher",
    "LabJobsServingSourceReader",
]
