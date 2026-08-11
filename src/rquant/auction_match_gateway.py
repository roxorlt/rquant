"""Single-fetch opening-auction authority publishing immutable live batches."""

from __future__ import annotations

import errno
import fcntl
import hashlib
import os
import re
import stat
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, time
from io import BytesIO
from typing import Annotated
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pydantic import Field, StringConstraints, model_validator

from rquant.live_contracts import (
    BatchEnvelope,
    BatchPointer,
    BatchQualityStatus,
    CurrentPointer,
    LiveChannel,
)
from rquant.live_spool import LiveBatchSpool
from rquant.runtime_contracts import (
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.source_quota_store import (
    SourceQuotaAttemptOutcome,
    SourceQuotaConflictError,
    SourceQuotaStore,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_TS_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")

AUCTION_MATCH_COLUMNS = (
    "ts_code",
    "trade_date",
    "price",
    "vol",
    "amount",
    "pre_close",
    "turnover_rate",
    "volume_ratio",
)
_REQUIRED_NUMERIC_COLUMNS = ("price", "vol", "amount", "pre_close")
_OPTIONAL_NUMERIC_COLUMNS = ("turnover_rate", "volume_ratio")


class AuctionMatchValidationError(ValueError):
    pass


class AuctionMatchGatewayConfig(RuntimeContractModel):
    source: str = Field(default="tushare.stk_auction", min_length=1)
    dataset_id: str = Field(default="auction_match", min_length=1)
    producer_version: str = Field(min_length=1)
    producer_commit: CommitSha
    min_coverage_ratio: float = Field(default=0.95, gt=0, le=1)
    quota_units_per_window: int | None = Field(default=None, gt=0)
    quota_cost_per_request: int = Field(default=1, gt=0)


class AuctionMatchCapture(RuntimeContractModel):
    pointer: CurrentPointer | BatchPointer
    published: bool
    expected_count: int = Field(gt=0)
    observed_count: int = Field(ge=0)
    coverage_ratio: float = Field(ge=0, le=1)
    missing_required_codes: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_counts(self) -> AuctionMatchCapture:
        if self.observed_count > self.expected_count:
            raise ValueError("observed_count cannot exceed expected_count")
        expected_ratio = self.observed_count / self.expected_count
        if self.coverage_ratio != expected_ratio:
            raise ValueError("coverage_ratio does not match capture counts")
        return self


class AuctionMatchGateway:
    """Shared authority for one opening-auction request and its session baseline."""

    def __init__(
        self,
        *,
        spool: LiveBatchSpool,
        fetcher: Callable[[date], pd.DataFrame],
        config: AuctionMatchGatewayConfig,
        quota_store: SourceQuotaStore | None = None,
        dispatch_clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.spool = spool
        self._fetcher = fetcher
        self.config = config
        self._quota_store = quota_store
        self._dispatch_clock = dispatch_clock
        if config.quota_units_per_window is not None and quota_store is None:
            raise ValueError("quota_store is required when quota governance is enabled")
        if config.quota_units_per_window is not None and dispatch_clock is None:
            raise ValueError("dispatch_clock is required when quota governance is enabled")

    @staticmethod
    def _normalize_universe(
        values: Iterable[str],
        *,
        label: str,
        allow_empty: bool,
    ) -> tuple[str, ...]:
        try:
            observed = tuple(values)
        except TypeError as exc:
            raise AuctionMatchValidationError(f"{label} must be iterable") from exc
        if any(not isinstance(value, str) for value in observed):
            raise AuctionMatchValidationError(f"{label} must contain strings")
        normalized = tuple(sorted(set(observed)))
        if not allow_empty and not normalized:
            raise AuctionMatchValidationError(f"{label} must be nonempty")
        if any(value != value.strip() or not _TS_CODE.fullmatch(value) for value in normalized):
            raise AuctionMatchValidationError(f"{label} contains invalid ts_code")
        return normalized

    @staticmethod
    def _event_time(trade_date: date) -> datetime:
        local = datetime.combine(trade_date, time(9, 25), tzinfo=_SHANGHAI)
        return local.astimezone(UTC)

    @staticmethod
    def _empty_frame() -> pd.DataFrame:
        return pd.DataFrame(
            {
                "ts_code": pd.Series(dtype="string"),
                "trade_date": pd.Series(dtype="object"),
                **{
                    column: pd.Series(dtype="float64")
                    for column in AUCTION_MATCH_COLUMNS
                    if column not in {"ts_code", "trade_date"}
                },
            }
        ).loc[:, AUCTION_MATCH_COLUMNS]

    @staticmethod
    def _contains_bool(series: pd.Series) -> bool:
        return any(isinstance(value, (bool, np.bool_)) for value in series.array)

    @staticmethod
    def normalize_frame(
        raw: pd.DataFrame,
        *,
        trade_date: date,
        expected_codes: Iterable[str],
    ) -> pd.DataFrame:
        if not isinstance(raw, pd.DataFrame):
            raise AuctionMatchValidationError("source result must be a DataFrame")
        missing = sorted(set(AUCTION_MATCH_COLUMNS) - set(raw.columns))
        if missing:
            raise AuctionMatchValidationError(f"missing columns: {missing}")
        expected = AuctionMatchGateway._normalize_universe(
            expected_codes,
            label="expected_codes",
            allow_empty=False,
        )
        frame = raw.loc[:, AUCTION_MATCH_COLUMNS].copy()
        if frame.empty:
            return AuctionMatchGateway._empty_frame()

        if any(
            AuctionMatchGateway._contains_bool(frame[column])
            for column in AUCTION_MATCH_COLUMNS[1:]
        ):
            raise AuctionMatchValidationError("bool values are forbidden")

        frame["ts_code"] = frame["ts_code"].astype("string").str.strip()
        if (
            frame["ts_code"].isna().any()
            or not frame["ts_code"].map(lambda value: bool(_TS_CODE.fullmatch(str(value)))).all()
        ):
            raise AuctionMatchValidationError("invalid ts_code")
        if frame["ts_code"].duplicated().any():
            raise AuctionMatchValidationError("duplicate ts_code rows")

        try:
            normalized_dates = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
        except (TypeError, ValueError) as exc:
            raise AuctionMatchValidationError("invalid trade_date") from exc
        if not normalized_dates.map(lambda value: value == trade_date).all():
            raise AuctionMatchValidationError("trade_date does not match request")
        frame["trade_date"] = normalized_dates

        try:
            for column in _REQUIRED_NUMERIC_COLUMNS + _OPTIONAL_NUMERIC_COLUMNS:
                frame[column] = pd.to_numeric(frame[column], errors="raise").astype("float64")
        except (TypeError, ValueError) as exc:
            raise AuctionMatchValidationError("invalid numeric value") from exc

        required = frame.loc[:, _REQUIRED_NUMERIC_COLUMNS].to_numpy(dtype="float64")
        if not np.isfinite(required).all():
            raise AuctionMatchValidationError("required numeric values must be finite")
        if (frame[["price", "pre_close"]] <= 0).to_numpy().any():
            raise AuctionMatchValidationError("price and pre_close must be positive")
        if (frame[["vol", "amount"]] < 0).to_numpy().any():
            raise AuctionMatchValidationError("vol and amount must be nonnegative")
        for column in _OPTIONAL_NUMERIC_COLUMNS:
            values = frame[column].dropna().to_numpy(dtype="float64")
            if not np.isfinite(values).all():
                raise AuctionMatchValidationError(
                    "optional numeric values must be finite when present"
                )
            if (values < 0).any():
                raise AuctionMatchValidationError("optional numeric values must be nonnegative")

        frame = frame.loc[frame["ts_code"].isin(expected), AUCTION_MATCH_COLUMNS]
        return frame.sort_values("ts_code", kind="stable").reset_index(drop=True)

    @staticmethod
    def encode_payload(frame: pd.DataFrame) -> bytes:
        output = BytesIO()
        frame.loc[:, AUCTION_MATCH_COLUMNS].to_parquet(output, index=False)
        return output.getvalue()

    @staticmethod
    def decode_payload(payload: bytes) -> pd.DataFrame:
        frame = pd.read_parquet(BytesIO(payload))
        if "ts_code" in frame:
            frame["ts_code"] = frame["ts_code"].astype("string")
        if "trade_date" in frame:
            frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="raise").dt.date
        return frame.loc[:, AUCTION_MATCH_COLUMNS]

    def _fetch_with_quota(
        self,
        *,
        trade_date: date,
        received: datetime,
        source_request_id: str,
        retry_ordinal: int,
    ) -> pd.DataFrame:
        if self._quota_store is None or self.config.quota_units_per_window is None:
            return self._fetcher(trade_date)
        attempt_id = canonical_sha256(
            {
                "protocol": "auction-source-attempt-v2",
                "source": self.config.source,
                "trade_date": trade_date,
                "session": "auction_match",
                "source_request_id": source_request_id,
                "retry_ordinal": retry_ordinal,
            }
        )
        if self._dispatch_clock is None:
            raise SourceQuotaConflictError("auction-match dispatch clock is required")
        attempt, created = self._quota_store.begin_transport_dispatch(
            source=self.config.source,
            owner=f"auction-match:{attempt_id}",
            attempt_id=attempt_id,
            logical_request_id=attempt_id,
            api_name="stk_auction",
            call_ordinal=1,
            units=self.config.quota_cost_per_request,
            total_units=self.config.quota_units_per_window,
            window_kind="minute",
            clock=self._dispatch_clock,
        )
        if not created:
            if attempt.outcome is SourceQuotaAttemptOutcome.PENDING:
                attempt = self._quota_store.recover_attempt(
                    attempt_id,
                    now=normalize_aware_utc(self._dispatch_clock()),
                )
            raise SourceQuotaConflictError(
                f"auction-match source attempt already exists: {attempt.outcome.value}"
            )
        try:
            result = self._fetcher(trade_date)
        except Exception:
            self._quota_store.commit_attempt(
                attempt.attempt_id,
                outcome=SourceQuotaAttemptOutcome.FAILURE,
                now=normalize_aware_utc(self._dispatch_clock()),
            )
            raise
        self._quota_store.commit_attempt(
            attempt.attempt_id,
            outcome=SourceQuotaAttemptOutcome.SUCCESS,
            now=normalize_aware_utc(self._dispatch_clock()),
        )
        return result

    @contextmanager
    def _capture_lock(self) -> Iterator[bool]:
        root = self.spool.root
        lock_name = ".auction_match.capture.lock"
        flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        nofollow = getattr(os, "O_NOFOLLOW", 0)
        directory_flag = getattr(os, "O_DIRECTORY", 0)
        if nofollow == 0 or directory_flag == 0:
            raise AuctionMatchValidationError("capture lock requires O_NOFOLLOW")
        root_descriptor = -1
        descriptor = -1
        locked = False
        try:
            try:
                root_descriptor = os.open(
                    root,
                    os.O_RDONLY | directory_flag | nofollow | getattr(os, "O_CLOEXEC", 0),
                )
            except OSError as exc:
                raise AuctionMatchValidationError("capture lock root is unsafe") from exc
            opened_root = os.fstat(root_descriptor)
            try:
                linked_root = root.lstat()
            except OSError as exc:
                raise AuctionMatchValidationError("capture lock root is unavailable") from exc
            if not self._safe_lock_root(opened_root, linked_root):
                raise AuctionMatchValidationError("capture lock root is unsafe")

            try:
                descriptor = self._open_capture_lock_file(
                    root_descriptor=root_descriptor,
                    lock_name=lock_name,
                    flags=flags,
                    nofollow=nofollow,
                )
            except OSError as exc:
                raise AuctionMatchValidationError("capture lock file is unsafe") from exc
            opened = os.fstat(descriptor)
            try:
                linked = os.stat(
                    lock_name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise AuctionMatchValidationError("capture lock file is unavailable") from exc
            if not self._safe_lock_file(opened, linked):
                raise AuctionMatchValidationError("capture lock file is unsafe")

            waited = False
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                waited = True
                fcntl.flock(descriptor, fcntl.LOCK_EX)
            locked = True
            try:
                current_root = root.lstat()
                current_link = os.stat(
                    lock_name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise AuctionMatchValidationError(
                    "capture lock path changed while waiting"
                ) from exc
            if not self._safe_lock_root(os.fstat(root_descriptor), current_root):
                raise AuctionMatchValidationError("capture lock root changed while waiting")
            if not self._safe_lock_file(os.fstat(descriptor), current_link):
                raise AuctionMatchValidationError("capture lock file changed while waiting")
            yield waited
        finally:
            if descriptor >= 0:
                if locked:
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)
            if root_descriptor >= 0:
                os.close(root_descriptor)

    @staticmethod
    def _open_capture_lock_file(
        *,
        root_descriptor: int,
        lock_name: str,
        flags: int,
        nofollow: int,
    ) -> int:
        for _ in range(3):
            try:
                return os.open(
                    lock_name,
                    flags | nofollow,
                    dir_fd=root_descriptor,
                )
            except OSError as exc:
                if exc.errno != errno.ENOENT:
                    raise
            try:
                return os.open(
                    lock_name,
                    flags | nofollow | os.O_CREAT | os.O_EXCL,
                    0o600,
                    dir_fd=root_descriptor,
                )
            except OSError as exc:
                if exc.errno != errno.EEXIST:
                    raise
        raise FileExistsError("capture lock creation did not converge")

    @staticmethod
    def _safe_lock_root(opened: os.stat_result, linked: os.stat_result) -> bool:
        return (
            stat.S_ISDIR(opened.st_mode)
            and opened.st_uid == os.getuid()
            and stat.S_IMODE(opened.st_mode) == 0o700
            and opened.st_dev == linked.st_dev
            and opened.st_ino == linked.st_ino
        )

    @staticmethod
    def _safe_lock_file(opened: os.stat_result, linked: os.stat_result) -> bool:
        return (
            stat.S_ISREG(opened.st_mode)
            and opened.st_uid == os.getuid()
            and opened.st_nlink == 1
            and stat.S_IMODE(opened.st_mode) == 0o600
            and opened.st_dev == linked.st_dev
            and opened.st_ino == linked.st_ino
        )

    def _latest_envelope(self) -> BatchEnvelope | None:
        records = self.spool.list_after(LiveChannel.AUCTION_MATCH, sequence=-1)
        if not records:
            return None
        return records[-1].envelope

    def _completed_published_capture(
        self,
        *,
        source_request_id: str,
        event_at: datetime,
        retry_ordinal: int,
        expected: tuple[str, ...],
        required: tuple[str, ...],
    ) -> AuctionMatchCapture | None:
        latest = self._latest_envelope()
        if (
            latest is None
            or retry_ordinal != 0
            or latest.source_request_id != source_request_id
            or latest.event_time_start != event_at
            or latest.event_time_end != event_at
            or latest.quality_status is not BatchQualityStatus.PUBLISHED
        ):
            return None
        current = self.spool.current(LiveChannel.AUCTION_MATCH)
        if current is None:
            raise AuctionMatchValidationError("live current pointer disappeared")
        records = self.spool.list_after(
            LiveChannel.AUCTION_MATCH,
            sequence=current.sequence - 1,
        )
        if len(records) != 1:
            raise AuctionMatchValidationError("current live batch cannot be resolved")
        frame = self.decode_payload(self.spool.read_payload(records[0]))
        if len(frame) != latest.row_count:
            raise AuctionMatchValidationError("published capture row_count is inconsistent")
        observed = tuple(frame["ts_code"].astype(str))
        missing_required = tuple(sorted(set(required) - set(observed)))
        return AuctionMatchCapture(
            pointer=current,
            published=False,
            expected_count=len(expected),
            observed_count=len(observed),
            coverage_ratio=len(observed) / len(expected),
            missing_required_codes=missing_required,
        )

    def _capture_locked(
        self,
        *,
        trade_date: date,
        received: datetime,
        expected: tuple[str, ...],
        required: tuple[str, ...],
        source_request_id: str,
        event_at: datetime,
        retry_ordinal: int,
    ) -> AuctionMatchCapture:
        quality = BatchQualityStatus.PUBLISHED
        degraded_reasons: list[str] = []
        source_failed = False
        raw_empty = False
        latest = self._latest_envelope()
        try:
            raw = self._fetch_with_quota(
                trade_date=trade_date,
                received=received,
                source_request_id=source_request_id,
                retry_ordinal=retry_ordinal,
            )
        except Exception as exc:
            source_failed = True
            frame = self._empty_frame()
            quality = BatchQualityStatus.STALE
            degraded_reasons.append(f"source_error:{type(exc).__name__}")
        else:
            raw_empty = isinstance(raw, pd.DataFrame) and raw.empty
            frame = self.normalize_frame(
                raw,
                trade_date=trade_date,
                expected_codes=expected,
            )

        observed = tuple(frame["ts_code"].astype(str))
        missing_required = tuple(sorted(set(required) - set(observed)))
        coverage_ratio = len(observed) / len(expected)
        if not source_failed:
            if frame.empty:
                degraded_reasons.append(
                    "empty_source_result" if raw_empty else "expected_universe_no_match"
                )
            if coverage_ratio < self.config.min_coverage_ratio:
                degraded_reasons.append("coverage_below_minimum")
            if missing_required:
                degraded_reasons.append("required_codes_missing")
            if degraded_reasons:
                quality = BatchQualityStatus.DEGRADED

        payload = self.encode_payload(frame)
        content_hash = hashlib.sha256(payload).hexdigest()
        reasons = tuple(degraded_reasons)
        if (
            latest is not None
            and latest.source_request_id == source_request_id
            and latest.content_sha256 == content_hash
            and latest.event_time_start == event_at
            and latest.event_time_end == event_at
            and latest.quality_status is quality
            and latest.degraded_reasons == reasons
        ):
            if latest.quality_status is BatchQualityStatus.PUBLISHED:
                pointer = self.spool.current(LiveChannel.AUCTION_MATCH)
                if pointer is None:
                    raise AuctionMatchValidationError("live current pointer disappeared")
            else:
                records = self.spool.list_after(
                    LiveChannel.AUCTION_MATCH,
                    sequence=latest.sequence - 1,
                )
                if len(records) != 1 or records[0].envelope != latest:
                    raise AuctionMatchValidationError("latest live batch cannot be resolved")
                pointer = BatchPointer(
                    channel=latest.channel,
                    source_generation_id=self.spool.source_descriptor(
                        LiveChannel.AUCTION_MATCH
                    ).generation_id,
                    batch_id=latest.batch_id,
                    sequence=latest.sequence,
                    revision=latest.revision,
                    content_sha256=latest.content_sha256,
                    quality_status=latest.quality_status,
                    published_at=latest.available_at,
                )
            return AuctionMatchCapture(
                pointer=pointer,
                published=False,
                expected_count=len(expected),
                observed_count=len(observed),
                coverage_ratio=coverage_ratio,
                missing_required_codes=missing_required,
            )

        sequence = 0 if latest is None else latest.sequence + 1
        same_trade_date = latest is not None and latest.event_time_start == event_at
        revision = latest.revision + 1 if same_trade_date and latest is not None else 1
        revises_batch_id = latest.batch_id if revision > 1 and latest is not None else None
        batch_id = canonical_sha256(
            {
                "channel": LiveChannel.AUCTION_MATCH,
                "source_request_id": source_request_id,
                "sequence": sequence,
                "revision": revision,
                "event_at": event_at,
                "content_sha256": content_hash,
                "quality_status": quality,
                "degraded_reasons": reasons,
            }
        )
        envelope = BatchEnvelope(
            schema_version=1,
            channel=LiveChannel.AUCTION_MATCH,
            dataset_id=self.config.dataset_id,
            source=self.config.source,
            source_request_id=source_request_id,
            batch_id=batch_id,
            sequence=sequence,
            revision=revision,
            revises_batch_id=revises_batch_id,
            event_time_start=event_at,
            event_time_end=event_at,
            source_time=event_at,
            received_at=received,
            available_at=received,
            row_count=len(frame),
            content_sha256=content_hash,
            quality_status=quality,
            degraded_reasons=reasons,
            producer_version=self.config.producer_version,
            producer_commit=self.config.producer_commit,
        )
        pointer = self.spool.publish(envelope, payload)
        return AuctionMatchCapture(
            pointer=pointer,
            published=True,
            expected_count=len(expected),
            observed_count=len(observed),
            coverage_ratio=coverage_ratio,
            missing_required_codes=missing_required,
        )

    def capture_once(
        self,
        *,
        trade_date: date,
        received_at: datetime,
        expected_codes: Iterable[str],
        required_codes: Iterable[str] = (),
        retry_ordinal: int = 0,
    ) -> AuctionMatchCapture:
        if type(trade_date) is not date:
            raise AuctionMatchValidationError("trade_date must be a date")
        if type(retry_ordinal) is not int or retry_ordinal < 0:
            raise AuctionMatchValidationError("retry_ordinal must be a nonnegative int")
        try:
            received = normalize_aware_utc(received_at)
        except ValueError as exc:
            raise AuctionMatchValidationError("received_at must be timezone-aware") from exc
        expected = self._normalize_universe(
            expected_codes,
            label="expected_codes",
            allow_empty=False,
        )
        required = self._normalize_universe(
            required_codes,
            label="required_codes",
            allow_empty=True,
        )
        if not set(required).issubset(expected):
            raise AuctionMatchValidationError("required_codes must be a subset of expected_codes")

        received_local = received.astimezone(_SHANGHAI)
        available_from = datetime.combine(trade_date, time(9, 26), tzinfo=_SHANGHAI)
        if received_local.date() != trade_date:
            raise AuctionMatchValidationError("received_at must be on trade_date")
        if received_local < available_from:
            raise AuctionMatchValidationError("auction data is unavailable before 09:26")

        source_request_id = canonical_sha256(
            {
                "source": self.config.source,
                "trade_date": trade_date,
                "expected_codes": expected,
                "required_codes": required,
            }
        )
        event_at = self._event_time(trade_date)
        with self._capture_lock():
            completed = self._completed_published_capture(
                source_request_id=source_request_id,
                event_at=event_at,
                retry_ordinal=retry_ordinal,
                expected=expected,
                required=required,
            )
            if completed is not None:
                return completed
            return self._capture_locked(
                trade_date=trade_date,
                received=received,
                expected=expected,
                required=required,
                source_request_id=source_request_id,
                event_at=event_at,
                retry_ordinal=retry_ordinal,
            )
