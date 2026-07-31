"""Point-in-time market-minute quote resolution for the paper broker."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from datetime import date, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Annotated, Self
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from pydantic import StringConstraints, field_validator, model_validator

from rquant.live_contracts import (
    BatchEnvelope,
    BatchQualityStatus,
    CurrentPointer,
    LiveChannel,
)
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteValidationError
from rquant.paper_broker import BrokerExecutionContext
from rquant.paper_signal_worker import PaperQuoteSnapshot
from rquant.runtime_contracts import RuntimeContractModel, normalize_aware_utc
from rquant.signal_contracts import SignalAction, SignalEnvelope

Sha256 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(
    os, "O_NOFOLLOW", 0
)
_FILE_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)


class PaperQuoteResolutionError(RuntimeError):
    """A paper quote cannot be resolved without violating PIT semantics."""


class PaperQuoteIntegrityError(PaperQuoteResolutionError):
    """Immutable input identity or filesystem safety validation failed."""


class PaperQuoteUnavailableError(PaperQuoteResolutionError):
    """No market-minute evidence was available at the observation time."""


class PaperQuoteStaleError(PaperQuoteUnavailableError):
    """The latest visible market-minute batch explicitly reported STALE."""


class PaperQuoteCandidateMissingError(PaperQuoteUnavailableError):
    """The latest visible batch has no usable row for the signal candidate."""


class PaperTradeCalendarError(PaperQuoteResolutionError):
    """The frozen SSE calendar cannot establish the acquisition date."""


class PaperQuoteResolverConfig(RuntimeContractModel):
    raw_spool_root: Path
    trade_calendar_path: Path
    trade_calendar_sha256: Sha256

    @field_validator("raw_spool_root", "trade_calendar_path")
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
                raise PaperQuoteIntegrityError(
                    f"paper quote path contains a symlink: {traversed}"
                )
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

    def __call__(
        self,
        signal: SignalEnvelope,
        observed_at: datetime,
    ) -> PaperQuoteSnapshot:
        return self.resolve(signal, observed_at=observed_at)

    def resolve(
        self,
        signal: SignalEnvelope,
        *,
        observed_at: datetime,
    ) -> PaperQuoteSnapshot:
        observed = normalize_aware_utc(observed_at)
        envelope, payload = self._latest_visible_batch(observed)
        if envelope.quality_status is BatchQualityStatus.STALE:
            raise PaperQuoteStaleError(
                f"market-minute sequence {envelope.sequence} is STALE"
            )
        if envelope.quality_status is not BatchQualityStatus.PUBLISHED:
            raise PaperQuoteUnavailableError(
                "latest visible market-minute batch is not published: "
                f"{envelope.quality_status.value}"
            )
        frame = self._validated_frame(envelope, payload)
        visible = frame.loc[
            (frame["ts_code"] == signal.candidate_id)
            & (frame["trade_time"] <= observed)
        ]
        if visible.empty:
            raise PaperQuoteCandidateMissingError(
                f"{signal.candidate_id} has no visible minute in market-minute "
                f"sequence {envelope.sequence}"
            )
        row = visible.sort_values("trade_time", kind="stable").iloc[-1]
        event_time = row["trade_time"].to_pydatetime()
        if event_time > envelope.available_at:
            raise PaperQuoteIntegrityError(
                "market-minute row event time exceeds evidence batch available_at"
            )
        acquisition_date = (
            self._next_sse_open_day(event_time.astimezone(_SHANGHAI).date())
            if signal.action is SignalAction.B_INTENT
            else None
        )
        return PaperQuoteSnapshot(
            ts_code=signal.candidate_id,
            event_time=event_time,
            available_at=envelope.available_at,
            context=BrokerExecutionContext(
                executable_price=Decimal(str(row["close"])),
                acquisition_available_date=acquisition_date,
            ),
            producer_commit=envelope.producer_commit,
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

        visible: list[BatchEnvelope] = []
        for sequence in range(pointer.sequence + 1):
            manifest_path = (
                root
                / "batches"
                / LiveChannel.MARKET_MINUTE.value
                / f"{sequence:020d}.json"
            )
            try:
                envelope = BatchEnvelope.model_validate_json(
                    _read_regular_file_no_symlinks(manifest_path)
                )
            except ValueError as exc:
                raise PaperQuoteIntegrityError(
                    f"market-minute manifest {sequence} is invalid"
                ) from exc
            if envelope.channel is not LiveChannel.MARKET_MINUTE:
                raise PaperQuoteIntegrityError(
                    f"market-minute manifest {sequence} channel mismatch"
                )
            if envelope.sequence != sequence:
                raise PaperQuoteIntegrityError(
                    f"market-minute manifest sequence mismatch at {sequence}"
                )
            if envelope.available_at <= observed_at:
                visible.append(envelope)
        if not visible:
            raise PaperQuoteUnavailableError(
                f"no market-minute batch is available at {observed_at.isoformat()}"
            )
        selected = max(
            visible,
            key=lambda item: (item.sequence, item.event_time_end, item.batch_id),
        )
        if selected.sequence == pointer.sequence and (
            selected.batch_id != pointer.batch_id
            or selected.revision != pointer.revision
            or selected.content_sha256 != pointer.content_sha256
            or selected.quality_status is not pointer.quality_status
        ):
            raise PaperQuoteIntegrityError(
                "market-minute current pointer does not match current manifest"
            )
        payload_path = (
            root
            / "batches"
            / LiveChannel.MARKET_MINUTE.value
            / f"{selected.sequence:020d}.payload"
        )
        payload = _read_regular_file_no_symlinks(payload_path)
        if hashlib.sha256(payload).hexdigest() != selected.content_sha256:
            raise PaperQuoteIntegrityError("market-minute payload content hash mismatch")
        return selected, payload

    @staticmethod
    def _validated_frame(envelope: BatchEnvelope, payload: bytes) -> pd.DataFrame:
        try:
            frame = MarketMinuteGateway.normalize_frame(
                MarketMinuteGateway.decode_payload(payload)
            )
        except (MarketMinuteValidationError, OSError, TypeError, ValueError) as exc:
            raise PaperQuoteIntegrityError("market-minute payload cannot be decoded") from exc
        if len(frame) != envelope.row_count:
            raise PaperQuoteIntegrityError("market-minute payload row count mismatch")
        if not frame.empty:
            event_start = frame["trade_time"].min().to_pydatetime()
            event_end = frame["trade_time"].max().to_pydatetime()
            if (
                event_start != envelope.event_time_start
                or event_end != envelope.event_time_end
            ):
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
