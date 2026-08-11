"""Single-request daily-close raw capture into an immutable live spool."""

from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

from pydantic import (
    BaseModel,
    Field,
    StrictBool,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from rquant.live_contracts import BatchEnvelope, BatchQualityStatus, LiveChannel
from rquant.live_spool import LiveBatchRecord, LiveBatchSpool
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaExhaustedError,
    SourceQuotaStore,
)
from rquant.source_quota_transport import (
    QuotaBoundTransportObserver,
    SourceTransportCallReceipt,
)

NonEmptyStr = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
TsCode = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]
FiniteFloat = Annotated[float, Field(strict=True, allow_inf_nan=False)]
PositiveFloat = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
NonnegativeFloat = Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)]

_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _completion_utc_now() -> datetime:
    return datetime.now(UTC)


class DailyCloseDataset(StrEnum):
    DAILY_BAR = "daily_bar"
    DAILY_BASIC = "daily_basic"
    ADJ_FACTOR = "adj_factor"
    INDEX_DAILY = "index_daily"
    SECURITY_STATUS = "security_status"
    SUSPENSION_STATUS = "suspension_status"


DAILY_CLOSE_DATASETS = tuple(DailyCloseDataset)
DAILY_CLOSE_SOURCE_INTERFACES = (
    "daily_by_date",
    "daily_basic_by_date",
    "adj_factor_by_date",
    "index_daily_major_by_date",
    "stock_basic",
    "stock_st_raw",
    "suspend_d_raw",
)
_NONEMPTY_DATASETS = frozenset(DAILY_CLOSE_DATASETS) - {DailyCloseDataset.SUSPENSION_STATUS}


class DailyCloseValidationError(ValueError):
    def __init__(self, message: str, *, evidence_source: object | None = None) -> None:
        super().__init__(message)
        self.evidence_source = evidence_source


class _OhlcvFact(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date
    open: PositiveFloat
    high: PositiveFloat
    low: PositiveFloat
    close: PositiveFloat
    pre_close: PositiveFloat
    change: FiniteFloat
    pct_chg: FiniteFloat
    vol: NonnegativeFloat
    amount: NonnegativeFloat

    @model_validator(mode="after")
    def validate_price_range(self) -> _OhlcvFact:
        if self.high < max(self.open, self.low, self.close):
            raise ValueError("high is below an OHLC value")
        if self.low > min(self.open, self.high, self.close):
            raise ValueError("low is above an OHLC value")
        return self


class DailyBarFact(_OhlcvFact):
    pass


class IndexDailyFact(_OhlcvFact):
    pass


class DailyBasicFact(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date
    turnover_rate: NonnegativeFloat
    volume_ratio: NonnegativeFloat
    total_mv: NonnegativeFloat
    circ_mv: NonnegativeFloat


class AdjFactorFact(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date
    adj_factor: PositiveFloat


class SecurityStatusFact(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date
    name: NonEmptyStr
    is_st: StrictBool | None
    listing_status: NonEmptyStr


class SuspensionStatusFact(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date
    suspend_type: NonEmptyStr
    suspend_timing: NonEmptyStr | None = None


class DailyCloseFacts(RuntimeContractModel):
    daily_bar: tuple[DailyBarFact, ...]
    daily_basic: tuple[DailyBasicFact, ...]
    adj_factor: tuple[AdjFactorFact, ...]
    index_daily: tuple[IndexDailyFact, ...]
    security_status: tuple[SecurityStatusFact, ...]
    suspension_status: tuple[SuspensionStatusFact, ...]
    partial_datasets: tuple[DailyCloseDataset, ...] = ()

    @field_validator("partial_datasets")
    @classmethod
    def validate_partial_datasets(
        cls,
        value: tuple[DailyCloseDataset, ...],
    ) -> tuple[DailyCloseDataset, ...]:
        if len(value) != len(set(value)):
            raise ValueError("partial_datasets must be unique")
        return tuple(sorted(value, key=lambda item: item.value))

    @property
    def total_rows(self) -> int:
        return sum(len(self.rows(dataset)) for dataset in DAILY_CLOSE_DATASETS)

    def rows(self, dataset: DailyCloseDataset) -> tuple[RuntimeContractModel, ...]:
        return getattr(self, dataset.value)

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class DailyCloseSourceRequest(RuntimeContractModel):
    schema_version: int = Field(default=1, ge=1)
    source: NonEmptyStr
    trade_date: date
    datasets: tuple[DailyCloseDataset, ...] = DAILY_CLOSE_DATASETS

    @field_validator("datasets")
    @classmethod
    def validate_datasets(
        cls,
        value: tuple[DailyCloseDataset, ...],
    ) -> tuple[DailyCloseDataset, ...]:
        if value != DAILY_CLOSE_DATASETS:
            raise ValueError("daily-close request must cover the fixed dataset set")
        return value

    @property
    def identity_sha256(self) -> str:
        return canonical_sha256(self.model_dump(mode="python"))


class DailyCloseFetchResult(RuntimeContractModel):
    source: NonEmptyStr
    actual_call_count: int = Field(strict=True, ge=0)
    interface_calls: tuple[NonEmptyStr, ...]
    call_receipts: tuple[SourceTransportCallReceipt, ...] = ()
    payload: object

    @model_validator(mode="after")
    def validate_call_count(self) -> DailyCloseFetchResult:
        if self.actual_call_count != len(self.interface_calls):
            raise ValueError("actual_call_count does not match interface_calls")
        if self.call_receipts:
            if self.actual_call_count != len(self.call_receipts):
                raise ValueError("actual_call_count does not match call_receipts")
            if self.interface_calls != tuple(receipt.api_name for receipt in self.call_receipts):
                raise ValueError("interface_calls do not match transport receipts")
        return self


class DailyCloseRawPayload(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    source_request: DailyCloseSourceRequest
    source_request_id: Sha256Hex
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    revision: int = Field(ge=1)
    revises_batch_id: Sha256Hex | None = None
    content_sha256: Sha256Hex
    quality_status: BatchQualityStatus
    degraded_reasons: tuple[NonEmptyStr, ...] = ()
    facts: DailyCloseFacts

    @model_validator(mode="after")
    def validate_identity_and_quality(self) -> DailyCloseRawPayload:
        if self.source_request_id != self.source_request.identity_sha256:
            raise ValueError("source_request_id does not match source request")
        if self.content_sha256 != self.facts.identity_sha256:
            raise ValueError("content_sha256 does not match facts")
        if self.available_at < self.observed_at:
            raise ValueError("available_at must be after or equal to observed_at")
        if (self.revision == 1) != (self.revises_batch_id is None):
            raise ValueError("revision parent is inconsistent")
        if self.quality_status not in {
            BatchQualityStatus.PUBLISHED,
            BatchQualityStatus.DEGRADED,
            BatchQualityStatus.STALE,
        }:
            raise ValueError("raw payload quality is not publishable")
        requires_reasons = self.quality_status in {
            BatchQualityStatus.DEGRADED,
            BatchQualityStatus.STALE,
        }
        if requires_reasons != bool(self.degraded_reasons):
            raise ValueError("raw payload quality reasons are inconsistent")
        if len(self.degraded_reasons) != len(set(self.degraded_reasons)):
            raise ValueError("degraded_reasons must be unique")
        return self


class DailyCloseGatewayConfig(RuntimeContractModel):
    source: NonEmptyStr = "tushare.daily_close"
    dataset_id: NonEmptyStr = "daily_close"
    schema_version: int = Field(default=1, ge=1)
    producer_version: NonEmptyStr
    producer_commit: CommitSha
    quota_units_per_window: int | None = Field(default=None, gt=0)
    quota_accounting_mode: Literal["request", "transport"] = "request"
    quota_cost_per_request: int | None = Field(default=1, gt=0)
    require_source_usage_receipt: StrictBool = False
    pending_recovery_min_age_seconds: int = Field(default=300, strict=True, ge=30)
    max_payload_bytes: int = Field(default=64 * 1024 * 1024, ge=1)
    max_total_rows: int = Field(default=100_000, ge=1)
    max_rows_per_dataset: int = Field(default=20_000, ge=1)
    max_fields_per_row: int = Field(default=32, ge=1)
    max_cell_bytes: int = Field(default=16 * 1024, ge=1)
    max_evidence_bytes: int = Field(default=64 * 1024, ge=1_024)
    max_evidence_nodes: int = Field(default=1_024, ge=8)
    max_evidence_depth: int = Field(default=12, ge=1)
    max_evidence_container_items: int = Field(default=128, ge=1)
    max_evidence_string_bytes: int = Field(default=8 * 1024, ge=64)

    @model_validator(mode="after")
    def validate_quota_receipt(self) -> DailyCloseGatewayConfig:
        if self.require_source_usage_receipt and self.quota_units_per_window is None:
            raise ValueError("source usage receipt requires quota governance")
        if (
            self.quota_accounting_mode == "request"
            and self.require_source_usage_receipt
            and self.quota_cost_per_request != len(DAILY_CLOSE_SOURCE_INTERFACES)
        ):
            raise ValueError("daily-close reserved cost must match the source interface contract")
        if self.quota_accounting_mode == "request" and self.quota_cost_per_request is None:
            raise ValueError("request quota accounting requires quota_cost_per_request")
        if self.quota_accounting_mode == "transport" and self.quota_cost_per_request is not None:
            raise ValueError("transport quota accounting cannot declare a fixed request cost")
        if self.quota_accounting_mode == "transport" and not self.require_source_usage_receipt:
            raise ValueError("transport quota accounting requires call receipts")
        return self


class DailyCloseCapture(RuntimeContractModel):
    source_generation_id: Sha256Hex
    source_request_id: Sha256Hex
    batch_id: Sha256Hex
    sequence: int | None = Field(default=None, ge=0)
    revision: int = Field(ge=1)
    content_sha256: Sha256Hex
    quality_status: BatchQualityStatus
    degraded_reasons: tuple[NonEmptyStr, ...] = ()
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    published: bool
    quarantined: bool = False

    @model_validator(mode="after")
    def validate_capture_kind(self) -> DailyCloseCapture:
        if self.quarantined != (self.quality_status is BatchQualityStatus.QUARANTINED):
            raise ValueError("quarantine flag does not match quality")
        if self.quarantined and (self.sequence is not None or self.published):
            raise ValueError("quarantine capture cannot be a spool publication")
        if not self.quarantined and self.sequence is None:
            raise ValueError("published channel capture requires a sequence")
        return self


class DailyCloseQuarantineRecord(RuntimeContractModel):
    schema_version: int = Field(ge=1)
    source_request: DailyCloseSourceRequest
    source_request_id: Sha256Hex
    quarantine_id: Sha256Hex
    attempted_revision: int = Field(ge=1)
    observed_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    content_sha256: Sha256Hex
    raw_size_bytes: int = Field(ge=0)
    raw_encoding: Literal["json", "truncated"]
    raw_payload_base64: str
    evidence_truncated: bool = False
    truncation_reason: NonEmptyStr | None = None
    degraded_reasons: tuple[NonEmptyStr, ...]
    quality_status: Literal[BatchQualityStatus.QUARANTINED] = BatchQualityStatus.QUARANTINED

    @model_validator(mode="after")
    def validate_quarantine(self) -> DailyCloseQuarantineRecord:
        if self.source_request_id != self.source_request.identity_sha256:
            raise ValueError("quarantine source request identity changed")
        if not self.degraded_reasons:
            raise ValueError("quarantine requires reasons")
        if self.evidence_truncated != (self.raw_encoding == "truncated"):
            raise ValueError("quarantine truncation status is inconsistent")
        if self.evidence_truncated != (self.truncation_reason is not None):
            raise ValueError("quarantine truncation reason is inconsistent")
        try:
            raw_payload = base64.b64decode(self.raw_payload_base64, validate=True)
        except ValueError as exc:
            raise ValueError("quarantine raw evidence is not valid base64") from exc
        if len(raw_payload) != self.raw_size_bytes:
            raise ValueError("quarantine raw evidence size changed")
        if hashlib.sha256(raw_payload).hexdigest() != self.content_sha256:
            raise ValueError("quarantine raw evidence content changed")
        expected_quarantine_id = canonical_sha256(
            {
                "source_request_id": self.source_request_id,
                "content_sha256": self.content_sha256,
                "degraded_reasons": self.degraded_reasons,
            }
        )
        if self.quarantine_id != expected_quarantine_id:
            raise ValueError("quarantine identity changed")
        return self


class _PendingDailyCloseCapture(RuntimeContractModel):
    envelope: BatchEnvelope
    payload_size_bytes: int = Field(ge=1)
    payload_sha256: Sha256Hex


@dataclass(frozen=True)
class _StoredDailyCloseBatch:
    record: LiveBatchRecord
    payload: DailyCloseRawPayload


@dataclass(frozen=True)
class _GatewayState:
    records: tuple[_StoredDailyCloseBatch, ...]
    latest_by_trade_date: dict[date, _StoredDailyCloseBatch]


class DailyCloseGateway:
    """Own one daily-close source request and publish only immutable raw evidence."""

    def __init__(
        self,
        *,
        spool: LiveBatchSpool,
        fetcher: Callable[[DailyCloseSourceRequest], object],
        config: DailyCloseGatewayConfig,
        completion_clock: Callable[[], datetime] | None = None,
        quota_store: SourceQuotaStore | None = None,
        transport_observer: QuotaBoundTransportObserver | None = None,
    ) -> None:
        self.spool = spool
        self._fetcher = fetcher
        self.config = config
        self._completion_clock = completion_clock
        self._quota_store = quota_store
        self._transport_observer = transport_observer
        self._quarantine_root = self.spool.root / "quarantine" / LiveChannel.DAILY_CLOSE.value
        self._staging_root = self.spool.root / "capture-staging"
        self._pending_path = self._staging_root / "daily_close.pending"
        self._ensure_private_quarantine_root()
        self._ensure_private_staging_root()
        if config.quota_units_per_window is not None and quota_store is None:
            raise ValueError("quota_store is required when quota governance is enabled")
        if config.quota_accounting_mode == "transport" and transport_observer is None:
            raise ValueError("transport_observer is required for transport quota accounting")

    def _quota_attempt_id(
        self,
        *,
        request: DailyCloseSourceRequest,
        retry_ordinal: int,
    ) -> str:
        return canonical_sha256(
            {
                "protocol": "daily-close-source-attempt-v2",
                "source": self.config.source,
                "trade_date": request.trade_date,
                "source_request_id": request.identity_sha256,
                "retry_ordinal": retry_ordinal,
            }
        )

    def _prepare_source_attempt(
        self,
        *,
        request: DailyCloseSourceRequest,
        observed_at: datetime,
        retry_ordinal: int,
    ) -> tuple[str | None, SourceQuotaAttemptOutcome | None]:
        if (
            self._quota_store is None
            or self.config.quota_units_per_window is None
            or self.config.quota_accounting_mode == "transport"
        ):
            return None, None
        window_start = observed_at.replace(hour=0, minute=0, second=0, microsecond=0)
        window_reset = window_start + timedelta(days=1)
        self._quota_store.declare_window(
            source=self.config.source,
            window_id=window_start.strftime("%Y%m%d"),
            starts_at=window_start,
            resets_at=window_reset,
            total_units=self.config.quota_units_per_window,
        )
        attempt_id = self._quota_attempt_id(request=request, retry_ordinal=retry_ordinal)
        existing = self._quota_store.get_attempt(attempt_id)
        if existing is not None:
            return attempt_id, existing.outcome
        attempt = self._quota_store.begin_attempt(
            source=self.config.source,
            owner=f"daily-close:{attempt_id}",
            attempt_id=attempt_id,
            units=self.config.quota_cost_per_request,
            now=observed_at,
            expires_at=window_reset,
        )
        self._quota_store.mark_dispatched(attempt.attempt_id, now=observed_at)
        return attempt.attempt_id, None

    def recover_stale_source_attempts(
        self,
        *,
        observed_at: datetime,
    ) -> tuple[str, ...]:
        with self._capture_lock():
            return self._recover_stale_source_attempts(observed_at=observed_at)

    def _recover_stale_source_attempts(
        self,
        *,
        observed_at: datetime,
    ) -> tuple[str, ...]:
        if self._quota_store is None or self.config.quota_units_per_window is None:
            return ()
        try:
            observed = normalize_aware_utc(observed_at)
        except ValueError as exc:
            raise DailyCloseValidationError("observed_at must be timezone-aware") from exc
        recovered = self._quota_store.recover_stale_attempts(
            source=self.config.source,
            now=observed,
            min_age=timedelta(seconds=self.config.pending_recovery_min_age_seconds),
        )
        return tuple(attempt.attempt_id for attempt in recovered)

    def _source_result_payload(
        self,
        result: object,
        *,
        logical_request_id: str,
    ) -> tuple[object, bool]:
        if isinstance(result, DailyCloseFetchResult):
            if self.config.quota_accounting_mode == "transport":
                matches = bool(result.call_receipts) and result.source == self.config.source
                for receipt in result.call_receipts:
                    attempt = (
                        None
                        if self._quota_store is None
                        else self._quota_store.get_attempt(receipt.attempt_id)
                    )
                    matches = matches and (
                        receipt.source == self.config.source
                        and receipt.logical_request_id == logical_request_id
                        and attempt is not None
                        and attempt.outcome is receipt.outcome
                        and attempt.dispatched_at == receipt.dispatched_at
                        and attempt.committed_at == receipt.committed_at
                    )
            else:
                matches = (
                    result.source == self.config.source
                    and result.actual_call_count == self.config.quota_cost_per_request
                    and result.interface_calls == DAILY_CLOSE_SOURCE_INTERFACES
                )
            return result.payload, matches
        return result, not self.config.require_source_usage_receipt

    def _complete_source_attempt(
        self,
        attempt_id: str | None,
        *,
        outcome: SourceQuotaAttemptOutcome,
        observed_at: datetime,
    ) -> None:
        if attempt_id is not None and self._quota_store is not None:
            self._quota_store.commit_attempt(attempt_id, outcome=outcome, now=observed_at)

    def _ensure_private_quarantine_root(self) -> None:
        self._quarantine_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        observed = self._quarantine_root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid():
            raise DailyCloseValidationError("quarantine root is unsafe")
        if stat.S_IMODE(observed.st_mode) != 0o700:
            self._quarantine_root.chmod(0o700)

    def _ensure_private_staging_root(self) -> None:
        self._staging_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        observed = self._staging_root.lstat()
        if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != os.getuid():
            raise DailyCloseValidationError("capture staging root is unsafe")
        if stat.S_IMODE(observed.st_mode) != 0o700:
            self._staging_root.chmod(0o700)

    @staticmethod
    def _event_time(trade_date: date) -> datetime:
        return datetime.combine(trade_date, time(15, 0), tzinfo=_SHANGHAI).astimezone(UTC)

    def _available_at(self, observed_at: datetime) -> datetime:
        completion_clock = self._completion_clock or _completion_utc_now
        try:
            available_at = normalize_aware_utc(completion_clock())
        except ValueError as exc:
            raise DailyCloseValidationError("completion time must be timezone-aware") from exc
        if available_at < observed_at:
            raise DailyCloseValidationError("completion time precedes observed_at")
        return available_at

    @contextmanager
    def _capture_lock(self) -> Iterator[bool]:
        lock_path = self.spool.root / ".daily_close.capture.lock"
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        if nofollow == 0:
            raise DailyCloseValidationError("capture lock requires O_NOFOLLOW")
        descriptor = -1
        locked = False
        try:
            descriptor = os.open(
                lock_path,
                os.O_RDWR | os.O_CREAT | nofollow | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
            opened = os.fstat(descriptor)
            linked = lock_path.lstat()
            self._validate_lock_identity(opened, linked)
            waited = False
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                waited = True
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            locked = True
            self._validate_lock_identity(os.fstat(descriptor), lock_path.lstat())
            yield waited
        except OSError as exc:
            raise DailyCloseValidationError("capture lock is unsafe") from exc
        finally:
            if descriptor >= 0:
                if locked:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    @staticmethod
    def _validate_lock_identity(opened: os.stat_result, linked: os.stat_result) -> None:
        if not (
            stat.S_ISREG(opened.st_mode)
            and opened.st_uid == os.getuid()
            and opened.st_nlink == 1
            and stat.S_IMODE(opened.st_mode) == 0o600
            and opened.st_dev == linked.st_dev
            and opened.st_ino == linked.st_ino
        ):
            raise DailyCloseValidationError("capture lock is unsafe")

    @staticmethod
    def encode_payload(payload: DailyCloseRawPayload) -> bytes:
        return json.dumps(
            payload.model_dump(mode="json"),
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")

    @staticmethod
    def decode_payload(payload: bytes) -> DailyCloseRawPayload:
        try:
            return DailyCloseRawPayload.model_validate_json(payload)
        except ValidationError as exc:
            raise DailyCloseValidationError("stored daily-close payload is invalid") from exc

    def _preflight_bounds(self, raw: object) -> object:
        if isinstance(raw, BaseModel):
            raw = raw.model_dump(mode="python")
        if not isinstance(raw, Mapping):
            return raw
        allowed_fields = {dataset.value for dataset in DAILY_CLOSE_DATASETS} | {"partial_datasets"}
        for observed_fields, field_name in enumerate(raw, start=1):
            if not isinstance(field_name, str) or field_name not in allowed_fields:
                raise DailyCloseValidationError(f"unknown_field:{field_name}")
            if observed_fields > len(allowed_fields):
                raise DailyCloseValidationError("root_field_bound")
        bounded = {
            field_name: raw[field_name] for field_name in allowed_fields if field_name in raw
        }
        total_rows = 0
        for dataset in DAILY_CLOSE_DATASETS:
            rows = bounded.get(dataset.value)
            if not isinstance(rows, Iterable) or isinstance(rows, (str, bytes, bytearray)):
                continue
            materialized_rows: list[object] = []
            for row in rows:
                if len(materialized_rows) >= self.config.max_rows_per_dataset:
                    raise DailyCloseValidationError(
                        f"row_bound:{dataset.value}",
                        evidence_source={dataset.value: tuple(materialized_rows)},
                    )
                total_rows += 1
                if total_rows > self.config.max_total_rows:
                    raise DailyCloseValidationError(
                        "row_bound:total",
                        evidence_source={dataset.value: tuple(materialized_rows)},
                    )
                inspected_row = row.model_dump(mode="python") if isinstance(row, BaseModel) else row
                if not isinstance(inspected_row, Mapping):
                    materialized_rows.append(row)
                    continue
                if len(inspected_row) > self.config.max_fields_per_row:
                    raise DailyCloseValidationError(
                        f"field_bound:{dataset.value}",
                        evidence_source={dataset.value: tuple(materialized_rows)},
                    )
                for value in inspected_row.values():
                    try:
                        encoded = json.dumps(
                            value,
                            ensure_ascii=True,
                            allow_nan=False,
                            default=self._json_default,
                        ).encode("utf-8")
                    except (TypeError, ValueError):
                        continue
                    if len(encoded) > self.config.max_cell_bytes:
                        raise DailyCloseValidationError(
                            f"byte_bound:{dataset.value}:cell",
                            evidence_source={dataset.value: tuple(materialized_rows)},
                        )
                materialized_rows.append(row)
            bounded[dataset.value] = tuple(materialized_rows)
        return bounded

    @staticmethod
    def _json_default(value: object) -> object:
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, StrEnum):
            return value.value
        raise TypeError(f"unsupported raw value: {type(value).__name__}")

    def _validate_facts(self, raw: object, *, trade_date: date) -> DailyCloseFacts:
        bounded_raw = self._preflight_bounds(raw)
        try:
            facts = DailyCloseFacts.model_validate(bounded_raw)
        except ValidationError as exc:
            raise DailyCloseValidationError("invalid_payload") from exc
        if facts.total_rows > self.config.max_total_rows:
            raise DailyCloseValidationError("row_bound:total")
        for dataset in DAILY_CLOSE_DATASETS:
            rows = facts.rows(dataset)
            if len(rows) > self.config.max_rows_per_dataset:
                raise DailyCloseValidationError(f"row_bound:{dataset.value}")
            keys: set[tuple[object, ...]] = set()
            for row in rows:
                if row.trade_date != trade_date:
                    raise DailyCloseValidationError(f"trade_date_mismatch:{dataset.value}")
                key: tuple[object, ...] = (row.ts_code, row.trade_date)
                if isinstance(row, SuspensionStatusFact):
                    key += (row.suspend_type, row.suspend_timing)
                if key in keys:
                    raise DailyCloseValidationError(f"duplicate_key:{dataset.value}")
                keys.add(key)
        canonical_rows = {
            dataset.value: tuple(
                sorted(
                    facts.rows(dataset),
                    key=self._fact_sort_key,
                )
            )
            for dataset in DAILY_CLOSE_DATASETS
        }
        return facts.model_copy(update=canonical_rows)

    @staticmethod
    def _fact_sort_key(row: RuntimeContractModel) -> tuple[str, ...]:
        key = (str(row.ts_code), row.trade_date.isoformat())
        if isinstance(row, SuspensionStatusFact):
            key += (row.suspend_type, row.suspend_timing or "")
        return key

    @staticmethod
    def _empty_facts() -> DailyCloseFacts:
        return DailyCloseFacts(
            daily_bar=(),
            daily_basic=(),
            adj_factor=(),
            index_daily=(),
            security_status=(),
            suspension_status=(),
            partial_datasets=DAILY_CLOSE_DATASETS,
        )

    @staticmethod
    def _quality(facts: DailyCloseFacts) -> tuple[BatchQualityStatus, tuple[str, ...]]:
        reasons = {f"partial:{dataset.value}" for dataset in facts.partial_datasets}
        for dataset in _NONEMPTY_DATASETS:
            if not facts.rows(dataset) and dataset not in facts.partial_datasets:
                reasons.add(f"partial:{dataset.value}:empty")
        if reasons:
            return BatchQualityStatus.DEGRADED, tuple(sorted(reasons))
        return BatchQualityStatus.PUBLISHED, ()

    def _load_state(self) -> _GatewayState:
        self.spool.current(LiveChannel.DAILY_CLOSE)
        records = self.spool.list_after(LiveChannel.DAILY_CLOSE, sequence=-1)
        stored: list[_StoredDailyCloseBatch] = []
        latest_by_trade_date: dict[date, _StoredDailyCloseBatch] = {}
        for record in records:
            if record.payload_path.stat().st_size > self.config.max_payload_bytes:
                raise DailyCloseValidationError("stored daily-close payload exceeds byte bound")
            payload = self.decode_payload(self.spool.read_payload(record))
            envelope = record.envelope
            trade_date = payload.source_request.trade_date
            previous = latest_by_trade_date.get(trade_date)
            expected_revision = 1 if previous is None else previous.payload.revision + 1
            expected_parent = None if previous is None else previous.record.envelope.batch_id
            expected_event = self._event_time(trade_date)
            if not (
                envelope.schema_version == payload.schema_version == self.config.schema_version
                and envelope.dataset_id == self.config.dataset_id
                and envelope.source == payload.source_request.source == self.config.source
                and envelope.source_request_id == payload.source_request_id
                and envelope.revision == payload.revision == expected_revision
                and envelope.revises_batch_id == payload.revises_batch_id == expected_parent
                and envelope.event_time_start == envelope.event_time_end == expected_event
                and envelope.source_time == expected_event
                and envelope.received_at == payload.observed_at
                and envelope.available_at == payload.available_at
                and envelope.row_count == payload.facts.total_rows
                and envelope.quality_status is payload.quality_status
                and envelope.degraded_reasons == payload.degraded_reasons
            ):
                raise DailyCloseValidationError("stored daily-close revision chain is invalid")
            item = _StoredDailyCloseBatch(record=record, payload=payload)
            stored.append(item)
            latest_by_trade_date[trade_date] = item
        return _GatewayState(
            records=tuple(stored),
            latest_by_trade_date=latest_by_trade_date,
        )

    def _capture_from_stored(
        self,
        stored: _StoredDailyCloseBatch,
        *,
        published: bool,
    ) -> DailyCloseCapture:
        payload = stored.payload
        envelope = stored.record.envelope
        descriptor = self.spool.source_descriptor(LiveChannel.DAILY_CLOSE)
        return DailyCloseCapture(
            source_generation_id=descriptor.generation_id,
            source_request_id=payload.source_request_id,
            batch_id=envelope.batch_id,
            sequence=envelope.sequence,
            revision=payload.revision,
            content_sha256=payload.content_sha256,
            quality_status=payload.quality_status,
            degraded_reasons=payload.degraded_reasons,
            observed_at=payload.observed_at,
            available_at=payload.available_at,
            published=published,
        )

    def _store_pending(self, envelope: BatchEnvelope, payload: bytes) -> None:
        header = _PendingDailyCloseCapture(
            envelope=envelope,
            payload_size_bytes=len(payload),
            payload_sha256=hashlib.sha256(payload).hexdigest(),
        )
        header_bytes = json.dumps(
            header.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(header_bytes) > 64 * 1024:
            raise DailyCloseValidationError("capture staging header exceeds byte bound")
        LiveBatchSpool._atomic_write(self._pending_path, header_bytes + b"\n" + payload)

    def _load_pending(
        self,
    ) -> tuple[_PendingDailyCloseCapture, bytes, DailyCloseRawPayload] | None:
        if not self._pending_path.exists():
            return None
        observed = self._pending_path.lstat()
        if not (
            stat.S_ISREG(observed.st_mode)
            and observed.st_uid == os.getuid()
            and observed.st_nlink == 1
            and stat.S_IMODE(observed.st_mode) == 0o600
            and observed.st_size <= self.config.max_payload_bytes + 64 * 1024 + 1
        ):
            raise DailyCloseValidationError("capture staging journal is unsafe")
        try:
            header_bytes, payload_bytes = self._pending_path.read_bytes().split(b"\n", 1)
            header = _PendingDailyCloseCapture.model_validate_json(header_bytes)
        except (OSError, ValueError, ValidationError) as exc:
            raise DailyCloseValidationError("capture staging journal is invalid") from exc
        if not (
            len(payload_bytes) == header.payload_size_bytes
            and hashlib.sha256(payload_bytes).hexdigest() == header.payload_sha256
            and header.envelope.content_sha256 == header.payload_sha256
        ):
            raise DailyCloseValidationError("capture staging payload identity changed")
        payload = self.decode_payload(payload_bytes)
        if not (
            header.envelope.channel is LiveChannel.DAILY_CLOSE
            and header.envelope.source_request_id == payload.source_request_id
            and header.envelope.revision == payload.revision
            and header.envelope.revises_batch_id == payload.revises_batch_id
            and header.envelope.received_at == payload.observed_at
            and header.envelope.available_at == payload.available_at
            and header.envelope.row_count == payload.facts.total_rows
            and header.envelope.quality_status is payload.quality_status
            and header.envelope.degraded_reasons == payload.degraded_reasons
        ):
            raise DailyCloseValidationError("capture staging manifest is inconsistent")
        return header, payload_bytes, payload

    def _clear_pending(self) -> None:
        try:
            self._pending_path.unlink()
        except FileNotFoundError:
            return
        LiveBatchSpool._fsync_directory(self._staging_root)

    def _recover_pending(
        self,
        state: _GatewayState,
    ) -> tuple[_StoredDailyCloseBatch, bool] | None:
        pending = self._load_pending()
        if pending is None:
            return None
        header, payload_bytes, payload = pending
        for stored in state.records:
            if stored.record.envelope.batch_id == header.envelope.batch_id:
                if (
                    stored.record.envelope != header.envelope
                    or self.spool.read_payload(stored.record) != payload_bytes
                ):
                    raise DailyCloseValidationError("capture staging conflicts with spool")
                self._clear_pending()
                return stored, False

        previous = state.latest_by_trade_date.get(payload.source_request.trade_date)
        expected_sequence = (
            0 if not state.records else state.records[-1].record.envelope.sequence + 1
        )
        expected_revision = 1 if previous is None else previous.payload.revision + 1
        expected_parent = None if previous is None else previous.record.envelope.batch_id
        if not (
            header.envelope.sequence == expected_sequence
            and header.envelope.revision == payload.revision == expected_revision
            and header.envelope.revises_batch_id == payload.revises_batch_id == expected_parent
        ):
            raise DailyCloseValidationError("capture staging revision is no longer appendable")
        self.spool.publish(header.envelope, payload_bytes)
        record = self.spool.list_after(
            LiveChannel.DAILY_CLOSE,
            sequence=header.envelope.sequence - 1,
        )
        if len(record) != 1 or record[0].envelope != header.envelope:
            raise DailyCloseValidationError("recovered capture cannot be resolved")
        stored = _StoredDailyCloseBatch(record=record[0], payload=payload)
        self._clear_pending()
        return stored, True

    def _raw_evidence(
        self,
        raw: object,
        *,
        reasons: tuple[str, ...],
    ) -> tuple[str, int, Literal["json", "truncated"], str, bool, str | None]:
        source = self._evidence_source(raw, reasons=reasons)
        node_count = 0
        truncation_reason: str | None = None

        def truncate(reason: str) -> None:
            nonlocal truncation_reason
            if truncation_reason is None:
                truncation_reason = reason

        def scalar(value: object) -> object:
            if value is None or isinstance(value, (bool, int)):
                return value
            if isinstance(value, float):
                if value != value or value in {float("inf"), float("-inf")}:
                    return {"$type": "non_finite_float"}
                return value
            if isinstance(value, str):
                encoded = value.encode("utf-8", errors="surrogatepass")
                if len(encoded) <= self.config.max_evidence_string_bytes:
                    return value
                truncate("string_bytes")
                return {
                    "$string_prefix_base64": base64.b64encode(
                        encoded[: self.config.max_evidence_string_bytes]
                    ).decode("ascii"),
                    "$string_truncated": True,
                }
            if isinstance(value, bytes):
                if len(value) <= self.config.max_evidence_string_bytes:
                    return {"$bytes_base64": base64.b64encode(value).decode("ascii")}
                truncate("string_bytes")
                return {
                    "$bytes_prefix_base64": base64.b64encode(
                        value[: self.config.max_evidence_string_bytes]
                    ).decode("ascii"),
                    "$bytes_truncated": True,
                }
            if isinstance(value, (date, datetime, StrEnum)):
                return self._json_default(value)
            return None

        def visit(value: object, depth: int) -> object:
            nonlocal node_count
            if depth > self.config.max_evidence_depth:
                truncate("depth")
                return {"$truncated": "depth"}
            node_count += 1
            if node_count > self.config.max_evidence_nodes:
                truncate("nodes")
                return {"$truncated": "nodes"}
            primitive = scalar(value)
            if primitive is not None:
                return primitive
            if isinstance(value, BaseModel):
                value = value.model_dump(mode="python")
            if isinstance(value, Mapping):
                entries: list[tuple[str, object]] = []
                iterator = iter(value.items())
                for index in range(self.config.max_evidence_container_items + 1):
                    try:
                        key, nested = next(iterator)
                    except StopIteration:
                        break
                    if index == self.config.max_evidence_container_items:
                        truncate("container_items")
                        break
                    if not isinstance(key, str):
                        truncate("mapping_key")
                        key = f"$non_string_key:{type(key).__name__}"
                    entries.append((key, visit(nested, depth + 1)))
                return {"$mapping": entries}
            if isinstance(value, Iterable):
                items: list[object] = []
                iterator = iter(value)
                for index in range(self.config.max_evidence_container_items + 1):
                    try:
                        nested = next(iterator)
                    except StopIteration:
                        break
                    if index == self.config.max_evidence_container_items:
                        truncate("container_items")
                        break
                    items.append(visit(nested, depth + 1))
                return {"$iterable": items}
            return {"$type": type(value).__name__}

        observed = visit(source, 0)
        summary: dict[str, object] = {
            "contract": "daily-close-quarantine-evidence/v1",
            "degraded_reasons": reasons,
            "observed": observed,
            "truncated": truncation_reason is not None,
            "truncation_reason": truncation_reason,
        }
        evidence = json.dumps(
            summary,
            ensure_ascii=True,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(evidence) > self.config.max_evidence_bytes:
            observed_prefix_sha256 = hashlib.sha256(evidence).hexdigest()
            truncation_reason = "byte_budget"
            summary = {
                "contract": "daily-close-quarantine-evidence/v1",
                "degraded_reasons": reasons,
                "observed_prefix_sha256": observed_prefix_sha256,
                "truncated": True,
                "truncation_reason": truncation_reason,
            }
            evidence = json.dumps(
                summary,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
        if len(evidence) > self.config.max_evidence_bytes:
            raise DailyCloseValidationError("quarantine evidence byte bound is too small")
        return (
            hashlib.sha256(evidence).hexdigest(),
            len(evidence),
            "truncated" if truncation_reason is not None else "json",
            base64.b64encode(evidence).decode("ascii"),
            truncation_reason is not None,
            truncation_reason,
        )

    @staticmethod
    def _evidence_source(raw: object, *, reasons: tuple[str, ...]) -> object:
        if isinstance(raw, BaseModel):
            raw = raw.model_dump(mode="python")
        if isinstance(raw, Mapping):
            unknown_reasons = [reason for reason in reasons if reason.startswith("unknown_field:")]
            if unknown_reasons:
                field_name = unknown_reasons[0].split(":", 1)[1]
                if field_name in raw:
                    return {field_name: raw[field_name]}
        return raw

    def _persist_quarantine(
        self,
        *,
        request: DailyCloseSourceRequest,
        raw: object,
        revision: int,
        observed_at: datetime,
        available_at: datetime,
        reasons: tuple[str, ...],
    ) -> DailyCloseCapture:
        (
            content_hash,
            size,
            encoding,
            encoded,
            evidence_truncated,
            truncation_reason,
        ) = self._raw_evidence(raw, reasons=reasons)
        quarantine_id = canonical_sha256(
            {
                "source_request_id": request.identity_sha256,
                "content_sha256": content_hash,
                "degraded_reasons": reasons,
            }
        )
        path = self._quarantine_root / f"{quarantine_id}.json"
        candidate = DailyCloseQuarantineRecord(
            schema_version=self.config.schema_version,
            source_request=request,
            source_request_id=request.identity_sha256,
            quarantine_id=quarantine_id,
            attempted_revision=revision,
            observed_at=observed_at,
            available_at=available_at,
            content_sha256=content_hash,
            raw_size_bytes=size,
            raw_encoding=encoding,
            raw_payload_base64=encoded,
            evidence_truncated=evidence_truncated,
            truncation_reason=truncation_reason,
            degraded_reasons=reasons,
        )
        record_bytes = json.dumps(
            candidate.model_dump(mode="json"),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        try:
            self._create_quarantine_record(path.name, record_bytes)
        except FileExistsError:
            stored = self._read_quarantine_record(path.name)
            if not (
                stored.quarantine_id == candidate.quarantine_id
                and stored.source_request_id == candidate.source_request_id
                and stored.content_sha256 == candidate.content_sha256
                and stored.degraded_reasons == candidate.degraded_reasons
            ):
                raise DailyCloseValidationError("quarantine identity conflicts") from None
            candidate = stored
        descriptor = self.spool.source_descriptor(LiveChannel.DAILY_CLOSE)
        return DailyCloseCapture(
            source_generation_id=descriptor.generation_id,
            source_request_id=candidate.source_request_id,
            batch_id=candidate.quarantine_id,
            sequence=None,
            revision=candidate.attempted_revision,
            content_sha256=candidate.content_sha256,
            quality_status=BatchQualityStatus.QUARANTINED,
            degraded_reasons=candidate.degraded_reasons,
            observed_at=candidate.observed_at,
            available_at=candidate.available_at,
            published=False,
            quarantined=True,
        )

    def list_quarantined(self) -> tuple[DailyCloseQuarantineRecord, ...]:
        records: list[DailyCloseQuarantineRecord] = []
        for path in sorted(self._quarantine_root.glob("*.json")):
            record = self._read_quarantine_record(path.name)
            if path.stem != record.quarantine_id:
                raise DailyCloseValidationError("quarantine path identity changed")
            records.append(record)
        return tuple(records)

    @contextmanager
    def _quarantine_directory_fd(self) -> Iterator[int]:
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        if nofollow == 0:
            raise DailyCloseValidationError("quarantine root requires O_NOFOLLOW")
        descriptor = -1
        try:
            descriptor = os.open(
                self._quarantine_root,
                os.O_RDONLY | os.O_DIRECTORY | nofollow | getattr(os, "O_CLOEXEC", 0),
            )
            opened = os.fstat(descriptor)
            linked = self._quarantine_root.lstat()
            if not (
                stat.S_ISDIR(opened.st_mode)
                and opened.st_uid == os.getuid()
                and stat.S_IMODE(opened.st_mode) == 0o700
                and (opened.st_dev, opened.st_ino) == (linked.st_dev, linked.st_ino)
            ):
                raise DailyCloseValidationError("quarantine root is unsafe")
            yield descriptor
            checked = os.fstat(descriptor)
            linked_after = self._quarantine_root.lstat()
            if (checked.st_dev, checked.st_ino) != (linked_after.st_dev, linked_after.st_ino):
                raise DailyCloseValidationError("quarantine root identity changed")
        except DailyCloseValidationError:
            raise
        except OSError as exc:
            raise DailyCloseValidationError("quarantine record is unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _read_quarantine_record(self, name: str) -> DailyCloseQuarantineRecord:
        maximum = max(1024 * 1024, self.config.max_evidence_bytes * 4)
        descriptor = -1
        try:
            with self._quarantine_directory_fd() as directory:
                descriptor = os.open(
                    name,
                    os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
                    dir_fd=directory,
                )
                before = os.fstat(descriptor)
                if not (
                    stat.S_ISREG(before.st_mode)
                    and before.st_uid == os.getuid()
                    and before.st_nlink == 1
                    and stat.S_IMODE(before.st_mode) == 0o600
                    and before.st_size <= maximum
                ):
                    raise DailyCloseValidationError("quarantine record is unsafe")
                chunks: list[bytes] = []
                remaining = maximum + 1
                while remaining > 0:
                    chunk = os.read(descriptor, min(64 * 1024, remaining))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                if remaining <= 0 and os.read(descriptor, 1):
                    raise DailyCloseValidationError("quarantine record is unsafe")
                after = os.fstat(descriptor)
                linked = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                    after.st_dev,
                    after.st_ino,
                    after.st_size,
                    after.st_mtime_ns,
                ) or (before.st_dev, before.st_ino) != (linked.st_dev, linked.st_ino):
                    raise DailyCloseValidationError("quarantine record is unsafe")
                payload = b"".join(chunks)
        except DailyCloseValidationError:
            raise
        except (OSError, ValidationError) as exc:
            raise DailyCloseValidationError("quarantine record is unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        try:
            return DailyCloseQuarantineRecord.model_validate_json(payload)
        except ValidationError as exc:
            raise DailyCloseValidationError("quarantine record is invalid") from exc

    def _create_quarantine_record(self, name: str, payload: bytes) -> None:
        descriptor = -1
        try:
            with self._quarantine_directory_fd() as directory:
                descriptor = os.open(
                    name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_NOFOLLOW", 0)
                    | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                    dir_fd=directory,
                )
                written = 0
                while written < len(payload):
                    written += os.write(descriptor, payload[written:])
                os.fsync(descriptor)
                observed = os.fstat(descriptor)
                if not (
                    stat.S_ISREG(observed.st_mode)
                    and observed.st_uid == os.getuid()
                    and observed.st_nlink == 1
                    and stat.S_IMODE(observed.st_mode) == 0o600
                    and observed.st_size == len(payload)
                ):
                    raise DailyCloseValidationError("quarantine record is unsafe")
                os.fsync(directory)
        except FileExistsError:
            raise
        except DailyCloseValidationError:
            raise
        except OSError as exc:
            raise DailyCloseValidationError("quarantine record is unsafe") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def capture_once(
        self,
        *,
        trade_date: date,
        observed_at: datetime,
        refresh: bool = False,
        retry_ordinal: int = 0,
    ) -> DailyCloseCapture:
        if type(trade_date) is not date:
            raise DailyCloseValidationError("trade_date must be a date")
        if not isinstance(refresh, bool):
            raise DailyCloseValidationError("refresh must be a bool")
        if type(retry_ordinal) is not int or retry_ordinal < 0:
            raise DailyCloseValidationError("retry_ordinal must be a nonnegative int")
        try:
            observed = normalize_aware_utc(observed_at)
        except ValueError as exc:
            raise DailyCloseValidationError("observed_at must be timezone-aware") from exc
        event_at = self._event_time(trade_date)
        if observed < event_at:
            raise DailyCloseValidationError("daily-close observation precedes market close")
        request = DailyCloseSourceRequest(
            schema_version=self.config.schema_version,
            source=self.config.source,
            trade_date=trade_date,
        )
        with self._capture_lock() as waited:
            self._recover_stale_source_attempts(observed_at=observed)
            self.spool.source_descriptor(LiveChannel.DAILY_CLOSE)
            state = self._load_state()
            recovered = self._recover_pending(state)
            if recovered is not None:
                recovered_stored, recovered_published = recovered
                if recovered_stored.payload.source_request_id == request.identity_sha256:
                    return self._capture_from_stored(
                        recovered_stored,
                        published=recovered_published,
                    )
                state = self._load_state()
            previous = state.latest_by_trade_date.get(trade_date)
            if previous is not None and (
                (previous.payload.quality_status is BatchQualityStatus.PUBLISHED and not refresh)
                or waited
            ):
                return self._capture_from_stored(previous, published=False)

            quality = BatchQualityStatus.PUBLISHED
            reasons: tuple[str, ...] = ()
            raw: object
            attempt_id: str | None = None
            logical_request_id = self._quota_attempt_id(
                request=request,
                retry_ordinal=retry_ordinal,
            )
            transport_outcome = (
                None
                if self._transport_observer is None
                else self._transport_observer.request_outcome(logical_request_id)
            )
            try:
                if transport_outcome is not None:
                    recovered_outcome = transport_outcome
                else:
                    attempt_id, recovered_outcome = self._prepare_source_attempt(
                        request=request,
                        observed_at=observed,
                        retry_ordinal=retry_ordinal,
                    )
            except SourceQuotaExhaustedError:
                raw = {"quota_exhausted": True, "trade_date": trade_date}
                available_at = self._available_at(observed)
                facts = self._empty_facts()
                quality = BatchQualityStatus.STALE
                reasons = ("quota_exhausted",)
            else:
                if recovered_outcome is not None:
                    if previous is not None and recovered_outcome in {
                        SourceQuotaAttemptOutcome.FAILURE,
                        SourceQuotaAttemptOutcome.SUCCESS,
                    }:
                        return self._capture_from_stored(previous, published=False)
                    raw = {
                        "source_attempt_outcome": recovered_outcome.value,
                        "trade_date": trade_date,
                    }
                    available_at = self._available_at(observed)
                    facts = self._empty_facts()
                    quality = BatchQualityStatus.STALE
                    reasons = (
                        "source_attempt_pending"
                        if recovered_outcome is SourceQuotaAttemptOutcome.PENDING
                        else f"source_attempt_{recovered_outcome.value}",
                    )
                else:
                    try:
                        if self._transport_observer is None:
                            source_result = self._fetcher(request)
                        else:
                            with self._transport_observer.scope(
                                logical_request_id=logical_request_id,
                                observed_at=observed,
                            ):
                                source_result = self._fetcher(request)
                    except SourceQuotaExhaustedError:
                        raw = {"quota_exhausted": True, "trade_date": trade_date}
                        available_at = self._available_at(observed)
                        facts = self._empty_facts()
                        quality = BatchQualityStatus.STALE
                        reasons = ("quota_exhausted",)
                    except Exception as exc:
                        self._complete_source_attempt(
                            attempt_id,
                            outcome=SourceQuotaAttemptOutcome.FAILURE,
                            observed_at=observed,
                        )
                        raw = {
                            "source_error": type(exc).__name__,
                            "trade_date": trade_date,
                        }
                        available_at = self._available_at(observed)
                        facts = self._empty_facts()
                        quality = BatchQualityStatus.STALE
                        reasons = (f"source_error:{type(exc).__name__}",)
                    else:
                        raw, receipt_matches = self._source_result_payload(
                            source_result,
                            logical_request_id=logical_request_id,
                        )
                        if receipt_matches:
                            self._complete_source_attempt(
                                attempt_id,
                                outcome=SourceQuotaAttemptOutcome.SUCCESS,
                                observed_at=observed,
                            )
                        else:
                            self._complete_source_attempt(
                                attempt_id,
                                outcome=SourceQuotaAttemptOutcome.FAILURE,
                                observed_at=observed,
                            )
                            available_at = self._available_at(observed)
                            facts = self._empty_facts()
                            quality = BatchQualityStatus.STALE
                            reasons = ("source_call_count_mismatch",)
            if quality is BatchQualityStatus.PUBLISHED:
                available_at = self._available_at(observed)
                revision = 1 if previous is None else previous.payload.revision + 1
                try:
                    facts = self._validate_facts(raw, trade_date=trade_date)
                    quality, reasons = self._quality(facts)
                    provisional = DailyCloseRawPayload(
                        schema_version=self.config.schema_version,
                        source_request=request,
                        source_request_id=request.identity_sha256,
                        observed_at=observed,
                        available_at=available_at,
                        revision=revision,
                        revises_batch_id=(
                            None if previous is None else previous.record.envelope.batch_id
                        ),
                        content_sha256=facts.identity_sha256,
                        quality_status=quality,
                        degraded_reasons=reasons,
                        facts=facts,
                    )
                    if len(self.encode_payload(provisional)) > self.config.max_payload_bytes:
                        raise DailyCloseValidationError("byte_bound")
                except DailyCloseValidationError as exc:
                    reason = str(exc) or "invalid_payload"
                    return self._persist_quarantine(
                        request=request,
                        raw=exc.evidence_source if exc.evidence_source is not None else raw,
                        revision=revision,
                        observed_at=observed,
                        available_at=available_at,
                        reasons=(reason,),
                    )

            revision = 1 if previous is None else previous.payload.revision + 1
            parent_id = None if previous is None else previous.record.envelope.batch_id
            payload = DailyCloseRawPayload(
                schema_version=self.config.schema_version,
                source_request=request,
                source_request_id=request.identity_sha256,
                observed_at=observed,
                available_at=available_at,
                revision=revision,
                revises_batch_id=parent_id,
                content_sha256=facts.identity_sha256,
                quality_status=quality,
                degraded_reasons=reasons,
                facts=facts,
            )
            if previous is not None and (
                previous.payload.content_sha256 == payload.content_sha256
                and previous.payload.quality_status is payload.quality_status
                and previous.payload.degraded_reasons == payload.degraded_reasons
            ):
                return self._capture_from_stored(previous, published=False)

            payload_bytes = self.encode_payload(payload)
            if len(payload_bytes) > self.config.max_payload_bytes:
                return self._persist_quarantine(
                    request=request,
                    raw=raw,
                    revision=revision,
                    observed_at=observed,
                    available_at=available_at,
                    reasons=("byte_bound",),
                )
            sequence = 0 if not state.records else state.records[-1].record.envelope.sequence + 1
            batch_id = canonical_sha256(
                {
                    "channel": LiveChannel.DAILY_CLOSE,
                    "source_request_id": request.identity_sha256,
                    "sequence": sequence,
                    "revision": revision,
                    "revises_batch_id": parent_id,
                    "content_sha256": payload.content_sha256,
                    "quality_status": quality,
                    "degraded_reasons": reasons,
                }
            )
            envelope = BatchEnvelope(
                schema_version=self.config.schema_version,
                channel=LiveChannel.DAILY_CLOSE,
                dataset_id=self.config.dataset_id,
                source=self.config.source,
                source_request_id=request.identity_sha256,
                batch_id=batch_id,
                sequence=sequence,
                revision=revision,
                revises_batch_id=parent_id,
                event_time_start=event_at,
                event_time_end=event_at,
                source_time=event_at,
                received_at=observed,
                available_at=available_at,
                row_count=facts.total_rows,
                content_sha256=hashlib.sha256(payload_bytes).hexdigest(),
                quality_status=quality,
                degraded_reasons=reasons,
                producer_version=self.config.producer_version,
                producer_commit=self.config.producer_commit,
            )
            self._store_pending(envelope, payload_bytes)
            self.spool.publish(envelope, payload_bytes)
            self._clear_pending()
            published_records = self.spool.list_after(
                LiveChannel.DAILY_CLOSE,
                sequence=sequence - 1,
            )
            if len(published_records) != 1 or published_records[0].envelope != envelope:
                raise DailyCloseValidationError("published daily-close capture cannot be resolved")
            stored = _StoredDailyCloseBatch(
                record=published_records[0],
                payload=payload,
            )
            return self._capture_from_stored(stored, published=True)


__all__ = [
    "DAILY_CLOSE_DATASETS",
    "DAILY_CLOSE_SOURCE_INTERFACES",
    "AdjFactorFact",
    "DailyBarFact",
    "DailyBasicFact",
    "DailyCloseCapture",
    "DailyCloseDataset",
    "DailyCloseFacts",
    "DailyCloseFetchResult",
    "DailyCloseGateway",
    "DailyCloseGatewayConfig",
    "DailyCloseQuarantineRecord",
    "DailyCloseRawPayload",
    "DailyCloseSourceRequest",
    "DailyCloseValidationError",
    "IndexDailyFact",
    "SecurityStatusFact",
    "SuspensionStatusFact",
]
