"""Point-in-time market-minute quote resolution for the paper broker."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Annotated, Literal, Self
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pydantic import Field, StrictInt, StringConstraints, field_validator, model_validator

from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    CurrentPointer,
    LiveChannel,
)
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteValidationError
from rquant.paper_broker import BrokerExecutionContext
from rquant.paper_execution_constraints import PaperExecutionConstraintAuthority
from rquant.paper_signal_worker import PaperQuoteSnapshot
from rquant.research_run_spec import InstrumentContext
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.signal_contracts import (
    CurrentSignalEnvelope,
    SignalAction,
    SignalEnvelope,
    SignalEnvelopeFamily,
    parse_signal_envelope,
)

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")


def _validated_signal_family(signal: SignalEnvelopeFamily) -> SignalEnvelopeFamily:
    if type(signal) not in {SignalEnvelope, CurrentSignalEnvelope}:
        raise TypeError("paper quote signal must be a known envelope family")
    parsed = parse_signal_envelope(signal.model_dump(mode="json"))
    if type(parsed) is not type(signal) or parsed != signal:
        raise PaperQuoteIntegrityError("paper quote signal is not canonical")
    return parsed


_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class PaperQuoteResolutionError(RuntimeError):
    """A paper quote cannot be resolved without violating PIT semantics."""


class PaperQuoteIntegrityError(PaperQuoteResolutionError):
    """Immutable input identity or filesystem safety validation failed."""


class PaperQuoteUnavailableError(PaperQuoteResolutionError):
    """No market-minute evidence was available at the observation time."""


class PaperQuoteStaleError(PaperQuoteUnavailableError):
    """The latest visible market-minute batch explicitly reported STALE."""


class PaperQuoteCandidateMissingError(PaperQuoteStaleError):
    """The latest visible batch has no usable row for the signal candidate."""


class PaperTradeCalendarError(PaperQuoteResolutionError):
    """The frozen SSE calendar cannot establish the acquisition date."""


def _require_attested_a_share_instrument_context(
    ts_code: str,
    instrument_context: InstrumentContext | None,
) -> InstrumentContext:
    """Reject quote execution unless constraints carry trusted listing evidence."""

    if instrument_context is None:
        raise PaperQuoteIntegrityError("paper quote has no trusted instrument classification")
    normalized = ts_code.strip().upper()
    if instrument_context.ts_code != normalized:
        raise PaperQuoteIntegrityError(
            "paper quote instrument classification does not match signal"
        )
    provenance = instrument_context.classification_provenance
    if provenance is None or provenance.reference_dataset != "security_listing_status":
        raise PaperQuoteIntegrityError("paper quote has no trusted instrument classification")
    if (
        instrument_context.market != "CN"
        or instrument_context.instrument_class != "EQUITY"
        or instrument_context.security_class != "A_SHARE"
    ):
        raise PaperQuoteIntegrityError("paper quote classification is not an A_SHARE")
    return instrument_context


class PaperQuoteResolverConfig(RuntimeContractModel):
    raw_spool_root: Path
    trade_calendar_path: Path
    trade_calendar_sha256: Sha256
    execution_constraint_root: Path
    expected_producer_commit: CommitSha
    timestamp_semantics: Literal["bar_end", "provider_snapshot"] = "provider_snapshot"
    quote_max_age_seconds: StrictInt = Field(default=90, gt=0, le=300)
    max_finalize_scan_batches: StrictInt = Field(default=32, gt=0, le=120)
    max_visible_scan_batches: StrictInt = Field(default=120, gt=0, le=1_000)

    @field_validator(
        "raw_spool_root",
        "trade_calendar_path",
        "execution_constraint_root",
    )
    @classmethod
    def require_absolute_normal_path(cls, value: Path) -> Path:
        if not value.is_absolute():
            raise ValueError("paper quote paths must be absolute")
        if any(part in {".", ".."} for part in value.parts):
            raise ValueError("paper quote paths must not contain dot components")
        return value

    @model_validator(mode="after")
    def validate_calendar_format(self) -> Self:
        if self.trade_calendar_path.suffix.lower() not in {".json", ".parquet"}:
            raise ValueError("trade calendar must be JSON or Parquet")
        return self


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _open_directory_no_symlinks(path: Path) -> int:
    if not path.is_absolute():
        raise PaperQuoteIntegrityError(f"directory path is not absolute: {path}")
    descriptor = os.open(path.anchor, _DIRECTORY_FLAGS)
    traversed = Path(path.anchor)
    try:
        for component in path.parts[1:]:
            traversed /= component
            try:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except OSError as exc:
                raise PaperQuoteIntegrityError(
                    f"paper quote directory is unavailable: {traversed}"
                ) from exc
            if stat.S_ISLNK(before.st_mode):
                raise PaperQuoteIntegrityError(f"paper quote path contains a symlink: {traversed}")
            if not stat.S_ISDIR(before.st_mode):
                raise PaperQuoteIntegrityError(
                    f"paper quote path component is not a directory: {traversed}"
                )
            try:
                child = os.open(component, _DIRECTORY_FLAGS, dir_fd=descriptor)
            except OSError as exc:
                raise PaperQuoteIntegrityError(
                    f"paper quote directory changed while opening: {traversed}"
                ) from exc
            active = os.fstat(child)
            if (active.st_dev, active.st_ino, active.st_mode) != (
                before.st_dev,
                before.st_ino,
                before.st_mode,
            ):
                os.close(child)
                raise PaperQuoteIntegrityError(
                    f"paper quote directory identity changed: {traversed}"
                )
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_regular_file_no_symlinks(path: Path) -> bytes:
    parent_descriptor = _open_directory_no_symlinks(path.parent)
    descriptor = -1
    try:
        try:
            before = os.stat(path.name, dir_fd=parent_descriptor, follow_symlinks=False)
        except OSError as exc:
            raise PaperQuoteIntegrityError(f"paper quote file is unavailable: {path}") from exc
        if stat.S_ISLNK(before.st_mode):
            raise PaperQuoteIntegrityError(f"paper quote file is a symlink: {path}")
        if not stat.S_ISREG(before.st_mode):
            raise PaperQuoteIntegrityError(f"paper quote file is not regular: {path}")
        try:
            descriptor = os.open(path.name, _FILE_FLAGS, dir_fd=parent_descriptor)
        except OSError as exc:
            raise PaperQuoteIntegrityError(
                f"paper quote file changed while opening: {path}"
            ) from exc
        opened = os.fstat(descriptor)
        if _identity(opened) != _identity(before):
            raise PaperQuoteIntegrityError(f"paper quote file identity changed: {path}")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if _identity(after) != _identity(opened):
            raise PaperQuoteIntegrityError(f"paper quote file changed while reading: {path}")
        return b"".join(chunks)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(parent_descriptor)


def _strict_bool(value: object) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    raise PaperTradeCalendarError("SSE calendar is_open values must be booleans")


def _next_trading_minute(value: datetime) -> datetime | None:
    local = normalize_aware_utc(value).astimezone(_SHANGHAI)
    if local.second != 0 or local.microsecond != 0:
        raise PaperQuoteIntegrityError("market-minute timestamp is not minute-aligned")
    clock = local.time().replace(tzinfo=None)
    if time(9, 30) <= clock < time(11, 30):
        return (local + timedelta(minutes=1)).astimezone(local.tzinfo)
    if clock == time(11, 30):
        return local.replace(hour=13, minute=0)
    if time(13, 0) <= clock < time(15, 0):
        return (local + timedelta(minutes=1)).astimezone(local.tzinfo)
    return None


def _calendar_frame(path: Path, content: bytes) -> pd.DataFrame:
    try:
        if path.suffix.lower() == ".parquet":
            raw = pd.read_parquet(BytesIO(content))
        else:
            decoded = json.loads(content)
            if isinstance(decoded, dict):
                decoded = decoded.get("rows", decoded.get("trade_calendar"))
            if not isinstance(decoded, list):
                raise ValueError("calendar JSON must contain a list of rows")
            raw = pd.DataFrame(decoded)
    except (OSError, TypeError, ValueError) as exc:
        raise PaperTradeCalendarError("frozen SSE calendar cannot be decoded") from exc
    required = {"exchange", "cal_date", "is_open"}
    if not required.issubset(raw.columns):
        missing = ", ".join(sorted(required - set(raw.columns)))
        raise PaperTradeCalendarError(f"frozen SSE calendar is missing columns: {missing}")
    return raw.loc[:, ["exchange", "cal_date", "is_open"]].copy()


def _load_sse_open_dates(path: Path, expected_sha256: str) -> tuple[date, ...]:
    content = _read_regular_file_no_symlinks(path)
    if hashlib.sha256(content).hexdigest() != expected_sha256:
        raise PaperQuoteIntegrityError("trade calendar content hash does not match config")
    frame = _calendar_frame(path, content)
    frame["exchange"] = frame["exchange"].astype("string").str.strip().str.upper()
    frame = frame.loc[frame["exchange"] == "SSE"].copy()
    if frame.empty:
        raise PaperTradeCalendarError("frozen trade calendar has no SSE rows")
    try:
        parsed = pd.to_datetime(frame["cal_date"], errors="raise")
    except (TypeError, ValueError) as exc:
        raise PaperTradeCalendarError("frozen SSE calendar has invalid dates") from exc
    if parsed.dt.tz is not None:
        raise PaperTradeCalendarError("frozen SSE calendar dates must not have timezones")
    frame["cal_date"] = parsed.dt.date
    frame["is_open"] = [_strict_bool(value) for value in frame["is_open"]]
    if frame["cal_date"].duplicated().any():
        raise PaperTradeCalendarError("frozen SSE calendar contains duplicate dates")
    return tuple(sorted(frame.loc[frame["is_open"], "cal_date"].tolist()))


class PaperPitQuoteResolver:
    """Resolve one executable close from the latest batch visible at ``observed_at``."""

    def __init__(self, config: PaperQuoteResolverConfig) -> None:
        self.config = config
        spool_descriptor = _open_directory_no_symlinks(config.raw_spool_root)
        os.close(spool_descriptor)
        self._sse_open_dates = _load_sse_open_dates(
            config.trade_calendar_path,
            config.trade_calendar_sha256,
        )
        self._execution_constraints = PaperExecutionConstraintAuthority(
            root=config.execution_constraint_root,
            expected_producer_commit=config.expected_producer_commit,
        )

    def __call__(
        self,
        signal: SignalEnvelopeFamily,
        observed_at: datetime,
    ) -> PaperQuoteSnapshot:
        return self.resolve(signal, observed_at=observed_at)

    def trade_date_at(self, observed_at: datetime) -> date:
        observed = normalize_aware_utc(observed_at)
        local_date = observed.astimezone(_SHANGHAI).date()
        if local_date not in self._sse_open_dates:
            raise PaperTradeCalendarError(f"{local_date.isoformat()} is not an SSE open day")
        return local_date

    def constraint_generation_at(self, observed_at: datetime) -> str:
        """Return the immutable constraint generation visible at the PIT cutoff."""

        return self._execution_constraints.load(
            observed_at=normalize_aware_utc(observed_at)
        ).content_hash

    def resolve(
        self,
        signal: SignalEnvelopeFamily,
        *,
        observed_at: datetime,
    ) -> PaperQuoteSnapshot:
        signal = _validated_signal_family(signal)
        observed = normalize_aware_utc(observed_at)
        observed_session = self._require_trade_session(observed)
        envelope, payload = self._latest_visible_batch(observed)
        if envelope.quality_status is BatchQualityStatus.STALE:
            raise PaperQuoteStaleError(f"market-minute sequence {envelope.sequence} is STALE")
        if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
            raise PaperQuoteUnavailableError(
                "latest visible market-minute batch is not published: "
                f"{envelope.quality_status.value}"
            )
        row = self._latest_candidate_row(
            envelope,
            payload,
            candidate_id=signal.candidate_id,
            observed_at=observed,
        )
        quote_available_at = envelope.available_at
        if self.config.timestamp_semantics == "provider_snapshot":
            row, finalized_envelope = self._finalized_provider_snapshot(
                candidate_id=signal.candidate_id,
                advancing_envelope=envelope,
                advancing_row=row,
                observed_at=observed,
            )
            producer_commit = finalized_envelope.producer_commit
        else:
            producer_commit = envelope.producer_commit
        event_time = row["trade_time"].to_pydatetime()
        if event_time > quote_available_at:
            raise PaperQuoteIntegrityError(
                "market-minute row event time exceeds evidence batch available_at"
            )
        event_session = self._require_trade_session(event_time)
        if event_session != observed_session:
            raise PaperTradeCalendarError(
                "paper quote cannot cross a trade date or continuous session"
            )
        age = observed - event_time
        if age < timedelta(0):
            raise PaperQuoteIntegrityError("paper quote event time is in the future")
        if age > timedelta(seconds=self.config.quote_max_age_seconds):
            raise PaperQuoteStaleError(
                f"paper quote age exceeds {self.config.quote_max_age_seconds} seconds"
            )
        acquisition_date = (
            self._next_sse_open_day(event_time.astimezone(_SHANGHAI).date())
            if signal.action is SignalAction.B_INTENT
            else None
        )
        constraint = self._execution_constraints.resolve(
            ts_code=signal.candidate_id,
            trade_date=observed_session[0],
            observed_at=observed,
            action=signal.action,
        )
        quote_available_at = max(quote_available_at, constraint.available_at)
        return PaperQuoteSnapshot(
            ts_code=signal.candidate_id,
            event_time=event_time,
            available_at=quote_available_at,
            context=BrokerExecutionContext(
                executable_price=Decimal(str(row["close"])),
                instrument_context=_require_attested_a_share_instrument_context(
                    signal.candidate_id,
                    constraint.instrument_context,
                ),
                acquisition_available_date=acquisition_date,
                suspended=constraint.suspended,
                limit_locked=constraint.limit_locked,
                risk_rejected=constraint.risk_rejected,
            ),
            producer_commit=producer_commit,
            constraint_snapshot_id=constraint.constraint_content_hash,
            constraint_batch_id=constraint.batch_content_hash,
            constraint_authority_sha256=constraint.authority_file_sha256,
            constraint_source_snapshot_ids=constraint.source_snapshot_ids,
        )

    def _require_trade_session(self, observed_at: datetime) -> tuple[date, str]:
        local = normalize_aware_utc(observed_at).astimezone(_SHANGHAI)
        if local.date() not in self._sse_open_dates:
            raise PaperTradeCalendarError(f"{local.date().isoformat()} is not an SSE open day")
        clock = local.time().replace(tzinfo=None)
        if time(9, 30) <= clock <= time(11, 30):
            return local.date(), "morning"
        if time(13, 0) <= clock <= time(15, 0):
            return local.date(), "afternoon"
        raise PaperTradeCalendarError("observation time is outside an SSE trading session")

    def _latest_candidate_row(
        self,
        envelope: BatchEnvelope,
        payload: bytes,
        *,
        candidate_id: str,
        observed_at: datetime,
    ) -> pd.Series:
        frame = self._validated_frame(envelope, payload)
        visible = frame.loc[
            (frame["ts_code"] == candidate_id) & (frame["trade_time"] <= observed_at)
        ]
        if visible.empty:
            raise PaperQuoteCandidateMissingError(
                f"{candidate_id} has no visible minute in market-minute "
                f"sequence {envelope.sequence}"
            )
        return visible.sort_values("trade_time", kind="stable").iloc[-1]

    def _finalized_provider_snapshot(
        self,
        *,
        candidate_id: str,
        advancing_envelope: BatchEnvelope,
        advancing_row: pd.Series,
        observed_at: datetime,
    ) -> tuple[pd.Series, BatchEnvelope]:
        advancing_time = advancing_row["trade_time"].to_pydatetime()
        lower_bound = max(
            -1,
            advancing_envelope.sequence - self.config.max_finalize_scan_batches - 1,
        )
        for sequence in range(advancing_envelope.sequence - 1, lower_bound, -1):
            envelope, payload = self._load_batch(sequence)
            if envelope.available_at > observed_at:
                continue
            if envelope.quality_status is BatchQualityStatus.STALE:
                raise PaperQuoteStaleError(f"market-minute sequence {envelope.sequence} is STALE")
            if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
                raise PaperQuoteUnavailableError(
                    "provider snapshot finalization crossed an unpublished batch"
                )
            row = self._latest_candidate_row(
                envelope,
                payload,
                candidate_id=candidate_id,
                observed_at=observed_at,
            )
            event_time = row["trade_time"].to_pydatetime()
            if event_time < advancing_time:
                expected_advance = _next_trading_minute(event_time)
                if expected_advance == advancing_time.astimezone(_SHANGHAI):
                    return row, envelope
                continue
            if event_time > advancing_time:
                raise PaperQuoteIntegrityError(
                    "provider snapshot timestamp regressed across sequences"
                )
        raise PaperQuoteUnavailableError(
            "provider snapshot timestamp did not advance within the bounded finalize scan"
        )

    def _latest_visible_batch(self, observed_at: datetime) -> tuple[BatchEnvelope, bytes]:
        root = self.config.raw_spool_root
        current_path = root / "current" / f"{LiveChannel.MARKET_MINUTE.value}.json"
        try:
            pointer = CurrentPointer.model_validate_json(
                _read_regular_file_no_symlinks(current_path)
            )
        except PaperQuoteIntegrityError as exc:
            if "unavailable" in str(exc):
                raise PaperQuoteUnavailableError(
                    "no current market-minute batch is available"
                ) from exc
            raise
        except ValueError as exc:
            raise PaperQuoteIntegrityError("market-minute current pointer is invalid") from exc
        if pointer.channel is not LiveChannel.MARKET_MINUTE:
            raise PaperQuoteIntegrityError("market-minute current pointer channel mismatch")

        channel_root = root / "batches" / LiveChannel.MARKET_MINUTE.value
        descriptor = _open_directory_no_symlinks(channel_root)
        try:
            sequences = [
                int(name.removesuffix(".json"))
                for name in os.listdir(descriptor)
                if name.endswith(".json") and name.removesuffix(".json").isdigit()
            ]
        finally:
            os.close(descriptor)
        if not sequences:
            raise PaperQuoteUnavailableError("no market-minute batch is available")
        high_watermark = max(sequences)
        selected: BatchEnvelope | None = None
        lower_bound = max(-1, high_watermark - self.config.max_visible_scan_batches)
        for sequence in range(high_watermark, lower_bound, -1):
            envelope, _payload = self._load_batch(sequence, load_payload=False)
            if sequence == pointer.sequence and (
                envelope.batch_id != pointer.batch_id
                or envelope.revision != pointer.revision
                or envelope.content_sha256 != pointer.content_sha256
                or envelope.quality_status is not pointer.quality_status
            ):
                raise PaperQuoteIntegrityError(
                    "market-minute current pointer does not match current manifest"
                )
            if envelope.available_at <= observed_at:
                selected = envelope
                break
        if selected is None:
            raise PaperQuoteUnavailableError(
                "no market-minute batch is available within the bounded visible scan at "
                f"{observed_at.isoformat()}"
            )
        return selected, self._load_payload(selected)

    def _load_batch(
        self,
        sequence: int,
        *,
        load_payload: bool = True,
    ) -> tuple[BatchEnvelope, bytes]:
        root = self.config.raw_spool_root
        manifest_path = root / "batches" / LiveChannel.MARKET_MINUTE.value / f"{sequence:020d}.json"
        try:
            envelope = BatchEnvelope.model_validate_json(
                _read_regular_file_no_symlinks(manifest_path)
            )
        except ValueError as exc:
            raise PaperQuoteIntegrityError(f"market-minute manifest {sequence} is invalid") from exc
        if envelope.channel is not LiveChannel.MARKET_MINUTE:
            raise PaperQuoteIntegrityError(f"market-minute manifest {sequence} channel mismatch")
        if envelope.producer_commit != self.config.expected_producer_commit:
            raise PaperQuoteIntegrityError(
                f"market-minute manifest {sequence} producer commit mismatch"
            )
        if envelope.sequence != sequence:
            raise PaperQuoteIntegrityError(
                f"market-minute manifest sequence mismatch at {sequence}"
            )
        if not load_payload:
            return envelope, b""
        return envelope, self._load_payload(envelope)

    def _load_payload(self, envelope: BatchEnvelope) -> bytes:
        payload_path = (
            self.config.raw_spool_root
            / "batches"
            / LiveChannel.MARKET_MINUTE.value
            / f"{envelope.sequence:020d}.payload"
        )
        payload = _read_regular_file_no_symlinks(payload_path)
        if hashlib.sha256(payload).hexdigest() != envelope.content_sha256:
            raise PaperQuoteIntegrityError("market-minute payload content hash mismatch")
        return payload

    @staticmethod
    def _validated_frame(envelope: BatchEnvelope, payload: bytes) -> pd.DataFrame:
        try:
            frame = MarketMinuteGateway.normalize_frame(MarketMinuteGateway.decode_payload(payload))
        except (MarketMinuteValidationError, OSError, TypeError, ValueError) as exc:
            raise PaperQuoteIntegrityError("market-minute payload cannot be decoded") from exc
        if len(frame) != envelope.row_count:
            raise PaperQuoteIntegrityError("market-minute payload row count mismatch")
        if not frame.empty:
            event_start = frame["trade_time"].min().to_pydatetime()
            event_end = frame["trade_time"].max().to_pydatetime()
            if event_start != envelope.event_time_start or event_end != envelope.event_time_end:
                raise PaperQuoteIntegrityError("market-minute payload event range mismatch")
        return frame

    def _next_sse_open_day(self, acquisition_date: date) -> date:
        for candidate in self._sse_open_dates:
            if candidate > acquisition_date:
                return candidate
        raise PaperTradeCalendarError(
            f"frozen calendar has no next SSE open day after {acquisition_date.isoformat()}"
        )


__all__ = [
    "PaperPitQuoteResolver",
    "PaperQuoteCandidateMissingError",
    "PaperQuoteIntegrityError",
    "PaperQuoteResolutionError",
    "PaperQuoteResolverConfig",
    "PaperQuoteStaleError",
    "PaperQuoteUnavailableError",
    "PaperTradeCalendarError",
]
