"""Fail-closed validation for immutable daily-close raw batches."""

from __future__ import annotations

import math
from collections.abc import Callable
from datetime import date
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, field_validator, model_validator

from rquant.daily_close_gateway import (
    DAILY_CLOSE_DATASETS,
    DailyCloseDataset,
    DailyCloseFacts,
    DailyCloseGateway,
    DailyCloseRawPayload,
    SuspensionStatusFact,
)
from rquant.live_contracts import BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
)
from rquant.runtime_market_session import MarketCalendarAuthority

Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
TsCode = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]
PositiveFloat = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
NonnegativeFloat = Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)]


class DailyCloseValidationError(ValueError):
    """The raw generation is not eligible to become a canonical candidate."""


class DailyCloseValidationPolicy(RuntimeContractModel):
    expected_schema_version: int = Field(default=1, ge=1)
    min_daily_rows: int = Field(default=1_000, ge=1)
    max_daily_rows: int = Field(default=10_000, ge=1)
    required_index_codes: tuple[TsCode, ...] = (
        "000001.SH",
        "399001.SZ",
        "399006.SZ",
        "000300.SH",
        "000905.SH",
        "000852.SH",
    )
    numeric_relative_tolerance: float = Field(default=1e-8, ge=0, allow_inf_nan=False)
    numeric_absolute_tolerance: float = Field(default=1e-8, ge=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def validate_bounds(self) -> Self:
        if self.max_daily_rows < self.min_daily_rows:
            raise ValueError("max_daily_rows must be at least min_daily_rows")
        if len(self.required_index_codes) != len(set(self.required_index_codes)):
            raise ValueError("required_index_codes must be unique")
        return self


class DailyMinuteAggregate(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date
    open: PositiveFloat
    high: PositiveFloat
    low: PositiveFloat
    close: PositiveFloat
    vol: NonnegativeFloat
    amount: NonnegativeFloat

    @model_validator(mode="after")
    def validate_ohlc(self) -> Self:
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("minute high is below an OHLC value")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("minute low is above an OHLC value")
        return self


class DailyMinuteSnapshot(RuntimeContractModel):
    trade_date: date
    available_at: AwareUtcDatetime
    rows: tuple[DailyMinuteAggregate, ...]

    @field_validator("rows")
    @classmethod
    def validate_rows(
        cls,
        rows: tuple[DailyMinuteAggregate, ...],
    ) -> tuple[DailyMinuteAggregate, ...]:
        keys = [row.ts_code for row in rows]
        if len(keys) != len(set(keys)):
            raise ValueError("minute snapshot contains duplicate keys")
        return tuple(sorted(rows, key=lambda row: row.ts_code))

    @model_validator(mode="after")
    def validate_dates(self) -> Self:
        if any(row.trade_date != self.trade_date for row in self.rows):
            raise ValueError("minute snapshot row date changed")
        return self

    @property
    def content_sha256(self) -> Sha256Hex:
        return canonical_sha256(self.model_dump(mode="python"))


class DailyDatasetRowCount(RuntimeContractModel):
    dataset: DailyCloseDataset
    row_count: int = Field(ge=0)


class VerifiedDailyCloseBatch(RuntimeContractModel):
    contract: Literal["verified-daily-close/v1"] = "verified-daily-close/v1"
    validation_sha256: Sha256Hex | None = None
    source_generation_id: Sha256Hex
    source_sequence: int = Field(ge=0)
    source_batch_id: Sha256Hex
    source_request_id: Sha256Hex
    envelope_sha256: Sha256Hex
    payload_sha256: Sha256Hex
    raw_content_sha256: Sha256Hex
    calendar_generation_id: Sha256Hex
    calendar_producer_commit: CommitSha
    calendar_content_sha256: Sha256Hex
    calendar_as_of: AwareUtcDatetime
    minute_content_sha256: Sha256Hex | None = None
    trade_date: date
    revision: int = Field(ge=1)
    available_at: AwareUtcDatetime
    dataset_row_counts: tuple[DailyDatasetRowCount, ...]
    facts: DailyCloseFacts

    @model_validator(mode="after")
    def bind_validation_identity(self) -> Self:
        expected = canonical_sha256(
            self.model_dump(mode="python", exclude={"validation_sha256", "facts"})
        )
        if self.validation_sha256 is None:
            object.__setattr__(self, "validation_sha256", expected)
        elif self.validation_sha256 != expected:
            raise ValueError("validation identity does not match bound evidence")
        if self.raw_content_sha256 != self.facts.identity_sha256:
            raise ValueError("verified facts content identity changed")
        if self.calendar_generation_id != self.calendar_content_sha256:
            raise ValueError("calendar generation does not bind calendar content")
        return self


class DailyCloseValidator:
    def __init__(
        self,
        *,
        spool: LiveBatchSpool,
        policy: DailyCloseValidationPolicy,
        calendar: MarketCalendarAuthority,
        minute_source: Callable[[date], DailyMinuteSnapshot | None] | None = None,
    ) -> None:
        self.spool = spool
        self.policy = DailyCloseValidationPolicy.model_validate(policy)
        self._calendar = MarketCalendarAuthority.model_validate(calendar)
        self._minute_source = minute_source

    def validate(self, record: LiveBatchRecord) -> VerifiedDailyCloseBatch:
        envelope = record.envelope
        descriptor = self.spool.source_descriptor(LiveChannel.DAILY_CLOSE)
        current = self.spool.current(LiveChannel.DAILY_CLOSE)
        if envelope.channel is not LiveChannel.DAILY_CLOSE:
            raise DailyCloseValidationError("raw batch is not from DAILY_CLOSE")
        if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
            raise DailyCloseValidationError("raw batch quality must be PUBLISHED")
        if current is None or not (
            current.source_generation_id == descriptor.generation_id
            and current.sequence == envelope.sequence
            and current.batch_id == envelope.batch_id
            and current.content_sha256 == envelope.content_sha256
            and current.revision == envelope.revision
        ):
            raise DailyCloseValidationError("raw batch is not the verified current revision")

        spool_records = self.spool.list_after(
            LiveChannel.DAILY_CLOSE,
            sequence=envelope.sequence - 1,
        )
        spool_record = next(
            (item for item in spool_records if item.envelope.sequence == envelope.sequence),
            None,
        )
        if spool_record is None or spool_record != record:
            raise DailyCloseValidationError("raw batch is not the immutable spool record")
        envelope = spool_record.envelope

        payload_bytes = self.spool.read_payload(spool_record)
        payload = DailyCloseGateway.decode_payload(payload_bytes)
        self._validate_bindings(payload, spool_record)
        trade_date = payload.source_request.trade_date
        if self._calendar.generated_at > payload.observed_at:
            raise DailyCloseValidationError("trade calendar was not available at raw observation")
        if not (
            self._calendar.coverage_start <= trade_date <= self._calendar.coverage_end
            and trade_date in self._calendar.open_dates
        ):
            raise DailyCloseValidationError("trade_date is not an open trading day")
        self._validate_facts(payload.facts)

        available_at = payload.available_at
        minute_hash: str | None = None
        if self._minute_source is not None:
            try:
                minute = self._minute_source(trade_date)
            except Exception as exc:
                raise DailyCloseValidationError("minute consistency source is unavailable") from exc
            if minute is None:
                raise DailyCloseValidationError("minute consistency source returned no snapshot")
            minute = DailyMinuteSnapshot.model_validate(minute)
            self._validate_minute_consistency(payload.facts, minute)
            minute_hash = minute.content_sha256
            available_at = max(available_at, minute.available_at)

        row_counts = tuple(
            DailyDatasetRowCount(dataset=dataset, row_count=len(payload.facts.rows(dataset)))
            for dataset in DAILY_CLOSE_DATASETS
        )
        return VerifiedDailyCloseBatch(
            source_generation_id=descriptor.generation_id,
            source_sequence=envelope.sequence,
            source_batch_id=envelope.batch_id,
            source_request_id=payload.source_request_id,
            envelope_sha256=envelope.identity_sha256,
            payload_sha256=envelope.content_sha256,
            raw_content_sha256=payload.content_sha256,
            calendar_generation_id=self._calendar.content_sha256,
            calendar_producer_commit=self._calendar.producer_commit,
            calendar_content_sha256=self._calendar.content_sha256,
            calendar_as_of=self._calendar.generated_at,
            minute_content_sha256=minute_hash,
            trade_date=trade_date,
            revision=payload.revision,
            available_at=available_at,
            dataset_row_counts=row_counts,
            facts=payload.facts,
        )

    def _validate_bindings(
        self,
        payload: DailyCloseRawPayload,
        record: LiveBatchRecord,
    ) -> None:
        envelope = record.envelope
        if not (
            payload.schema_version == self.policy.expected_schema_version
            and envelope.schema_version == self.policy.expected_schema_version
        ):
            raise DailyCloseValidationError("daily-close schema version is not accepted")
        if not (
            payload.quality_status is BatchQualityStatus.PUBLISHED
            and not payload.degraded_reasons
            and envelope.source_request_id == payload.source_request_id
            and envelope.revision == payload.revision
            and envelope.revises_batch_id == payload.revises_batch_id
            and envelope.received_at == payload.observed_at
            and envelope.available_at == payload.available_at
            and envelope.row_count == payload.facts.total_rows
            and envelope.quality_status is payload.quality_status
            and envelope.degraded_reasons == payload.degraded_reasons
        ):
            raise DailyCloseValidationError("raw envelope and typed payload binding changed")

    def _validate_facts(self, facts: DailyCloseFacts) -> None:
        daily_count = len(facts.daily_bar)
        if not self.policy.min_daily_rows <= daily_count <= self.policy.max_daily_rows:
            raise DailyCloseValidationError("daily row count is outside the accepted range")
        for dataset in DAILY_CLOSE_DATASETS:
            rows = facts.rows(dataset)
            keys: set[tuple[object, ...]] = set()
            for row in rows:
                key: tuple[object, ...] = (row.ts_code, row.trade_date)
                if isinstance(row, SuspensionStatusFact):
                    key += (row.suspend_type, row.suspend_timing)
                if key in keys:
                    raise DailyCloseValidationError(f"duplicate key in {dataset.value}")
                keys.add(key)

        daily_keys = {row.ts_code for row in facts.daily_bar}
        for label, observed in (
            ("daily_basic", {row.ts_code for row in facts.daily_basic}),
            ("adj_factor", {row.ts_code for row in facts.adj_factor}),
            ("security_status", {row.ts_code for row in facts.security_status}),
        ):
            if observed != daily_keys:
                raise DailyCloseValidationError(f"{label} key coverage is incomplete")
        suspension_keys = {row.ts_code for row in facts.suspension_status}
        if not suspension_keys <= daily_keys:
            raise DailyCloseValidationError("suspension key coverage is invalid")
        status_by_code = {row.ts_code: row for row in facts.security_status}
        if any(
            status_by_code[ts_code].listing_status != "L" or status_by_code[ts_code].is_st is None
            for ts_code in daily_keys
        ):
            raise DailyCloseValidationError("security status is incomplete or ineligible")
        index_codes = {row.ts_code for row in facts.index_daily}
        missing_indexes = set(self.policy.required_index_codes) - index_codes
        if missing_indexes:
            raise DailyCloseValidationError("required index key coverage is incomplete")

    def _validate_minute_consistency(
        self,
        facts: DailyCloseFacts,
        snapshot: DailyMinuteSnapshot,
    ) -> None:
        trade_date = facts.daily_bar[0].trade_date
        if snapshot.trade_date != trade_date:
            raise DailyCloseValidationError("minute consistency trade_date changed")
        suspended = {row.ts_code for row in facts.suspension_status if row.suspend_type == "S"}
        expected = {row.ts_code: row for row in facts.daily_bar if row.ts_code not in suspended}
        observed = {row.ts_code: row for row in snapshot.rows}
        if set(observed) != set(expected):
            raise DailyCloseValidationError("minute consistency key coverage differs")
        for ts_code, daily in expected.items():
            minute = observed[ts_code]
            for field_name in ("open", "high", "low", "close", "vol", "amount"):
                if not math.isclose(
                    float(getattr(daily, field_name)),
                    float(getattr(minute, field_name)),
                    rel_tol=self.policy.numeric_relative_tolerance,
                    abs_tol=self.policy.numeric_absolute_tolerance,
                ):
                    raise DailyCloseValidationError(
                        f"daily-minute consistency differs for {ts_code}:{field_name}"
                    )


__all__ = [
    "DailyCloseValidationError",
    "DailyCloseValidationPolicy",
    "DailyCloseValidator",
    "DailyDatasetRowCount",
    "DailyMinuteAggregate",
    "DailyMinuteSnapshot",
    "VerifiedDailyCloseBatch",
]
