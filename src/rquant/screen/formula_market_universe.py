"""Capture and archive one observed, same-day A-share market list for formula runs."""

from __future__ import annotations

import fcntl
import os
import re
import stat
import uuid
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime, time
from pathlib import Path
from typing import Literal, Protocol, Self
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import Field, model_validator

from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads

Exchange = Literal["SSE", "SZSE", "BSE"]
ListStatus = Literal["L", "P"]
FORMULA_STOCK_BASIC_COLUMNS: tuple[str, ...] = (
    "ts_code",
    "symbol",
    "name",
    "area",
    "industry",
    "list_date",
    "delist_date",
    "market",
    "list_status",
)
_PARTITIONS: tuple[tuple[Exchange, ListStatus], ...] = (
    ("SSE", "L"),
    ("SSE", "P"),
    ("SZSE", "L"),
    ("SZSE", "P"),
    ("BSE", "L"),
    ("BSE", "P"),
)
_SUFFIX: dict[Exchange, str] = {"SSE": "SH", "SZSE": "SZ", "BSE": "BJ"}
_CODE = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_A_CODE = re.compile(
    r"^(?:(?:000|001|002|003|300|301)[0-9]{3}\.SZ|"
    r"(?:600|601|603|605|688|689)[0-9]{3}\.SH|"
    r"[489][0-9]{5}\.BJ)$"
)
_B_CODE = re.compile(r"^(?:900[0-9]{3}\.SH|200[0-9]{3}\.SZ)$")
_DATE = re.compile(r"^[0-9]{8}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_PARTITION_ROWS = 5999
_MAX_FILE_BYTES = 8 * 1024 * 1024
_DIR_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
_WRITE_FLAGS = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)


class FormulaMarketUniverseError(RuntimeError):
    """The market list is incomplete, inconsistent, changed or unavailable."""


class FormulaMarketUniverseAdapter(Protocol):
    def stock_basic_partition(self, *, list_status: str, exchange: str) -> pd.DataFrame: ...


class FormulaMarketEntry(RuntimeContractModel):
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    exchange: Exchange
    list_status: ListStatus
    list_date: date

    @model_validator(mode="after")
    def validate_entry(self) -> Self:
        if not self.ts_code.endswith("." + _SUFFIX[self.exchange]):
            raise ValueError("stock exchange conflicts with ts_code")
        if _A_CODE.fullmatch(self.ts_code) is None:
            raise ValueError("entry is not a supported A-share code")
        return self


class FormulaMarketPartition(RuntimeContractModel):
    exchange: Exchange
    list_status: ListStatus
    raw_rows: int = Field(ge=0, le=_MAX_PARTITION_ROWS)
    included_rows: int = Field(ge=0)
    excluded_b_shares: int = Field(ge=0)
    excluded_other: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_counts(self) -> Self:
        if self.raw_rows != self.included_rows + self.excluded_b_shares + self.excluded_other:
            raise ValueError("partition counts do not conserve source rows")
        return self


class FormulaMarketUniverseSnapshot(RuntimeContractModel):
    schema_version: Literal[1] = 1
    trade_date: date
    started_at: AwareUtcDatetime
    completed_at: AwareUtcDatetime
    calendar_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    calendar_generated_at: AwareUtcDatetime
    partitions: tuple[FormulaMarketPartition, ...] = Field(min_length=6, max_length=6)
    entries: tuple[FormulaMarketEntry, ...] = Field(max_length=6 * _MAX_PARTITION_ROWS)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_snapshot(self) -> Self:
        if self.calendar_generated_at > self.started_at:
            raise ValueError("calendar was generated after market-list capture began")
        if self.started_at > self.completed_at:
            raise ValueError("market-list capture clock moved backwards")
        for moment in (self.started_at, self.completed_at):
            local = moment.astimezone(_SHANGHAI)
            if local.date() != self.trade_date or local.time() <= time(17):
                raise ValueError("market-list capture must stay on target date after 17:00")
        keys = tuple((item.exchange, item.list_status) for item in self.partitions)
        if keys != _PARTITIONS:
            raise ValueError("market-list capture requires all six ordered partitions")
        if any(item.included_rows == 0 for item in self.partitions if item.list_status == "L"):
            raise ValueError("each current-day listed exchange requires an A-share row")
        codes = tuple(item.ts_code for item in self.entries)
        if codes != tuple(sorted(set(codes))):
            raise ValueError("market-list entries must be sorted and unique")
        counts: dict[tuple[Exchange, ListStatus], int] = {}
        for entry in self.entries:
            if entry.list_date > self.trade_date:
                raise ValueError("market-list entry has a future listing date")
            key = (entry.exchange, entry.list_status)
            counts[key] = counts.get(key, 0) + 1
        if any(
            counts.get(key, 0) != part.included_rows
            for key, part in zip(keys, self.partitions, strict=True)
        ):
            raise ValueError("market-list entries do not match partition counts")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_sha256"}))
        if self.content_sha256 != expected:
            raise ValueError("market-list content sha256 is invalid")
        return self

    @classmethod
    def create(
        cls,
        *,
        trade_date: date,
        started_at: datetime,
        completed_at: datetime,
        calendar_sha256: str,
        calendar_generated_at: datetime,
        partitions: tuple[FormulaMarketPartition, ...],
        entries: tuple[FormulaMarketEntry, ...],
    ) -> FormulaMarketUniverseSnapshot:
        identity = {
            "schema_version": 1,
            "trade_date": trade_date,
            "started_at": normalize_aware_utc(started_at),
            "completed_at": normalize_aware_utc(completed_at),
            "calendar_sha256": calendar_sha256,
            "calendar_generated_at": normalize_aware_utc(calendar_generated_at),
            "partitions": partitions,
            "entries": entries,
        }
        return cls(**identity, content_sha256=canonical_sha256(identity))


class FormulaMarketPublicationReceipt(RuntimeContractModel):
    published: bool
    generation_path: Path
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class _Pointer(RuntimeContractModel):
    schema_version: Literal[1] = 1
    trade_date: date
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_size: int = Field(gt=0, le=_MAX_FILE_BYTES)


def _date_value(value: object, *, optional: bool = False) -> date | None:
    if optional and (value is None or (isinstance(value, str) and not value.strip())):
        return None
    if optional:
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and _DATE.fullmatch(value):
        try:
            return datetime.strptime(value, "%Y%m%d").date()
        except ValueError as exc:
            raise FormulaMarketUniverseError("stock_basic contains an invalid date") from exc
    raise FormulaMarketUniverseError("stock_basic contains a missing or invalid date")


def _capture_partition(
    frame: pd.DataFrame,
    *,
    exchange: Exchange,
    list_status: ListStatus,
    trade_date: date,
    seen: set[str],
) -> tuple[FormulaMarketPartition, list[FormulaMarketEntry]]:
    if not isinstance(frame, pd.DataFrame):
        raise FormulaMarketUniverseError("stock_basic did not return a table")
    missing = set(FORMULA_STOCK_BASIC_COLUMNS) - set(frame.columns)
    if missing:
        raise FormulaMarketUniverseError(
            "stock_basic is missing columns: " + ", ".join(sorted(missing))
        )
    if len(frame) >= 6000:
        raise FormulaMarketUniverseError("stock_basic partition may be truncated at 6000 rows")
    entries: list[FormulaMarketEntry] = []
    b_shares = other = 0
    for raw_code, raw_list_date, raw_delist_date, raw_status in frame.loc[
        :, ["ts_code", "list_date", "delist_date", "list_status"]
    ].itertuples(index=False, name=None):
        if not isinstance(raw_code, str) or _CODE.fullmatch(raw_code) is None:
            raise FormulaMarketUniverseError("stock_basic contains an invalid ts_code")
        if raw_code in seen:
            raise FormulaMarketUniverseError("stock_basic contains a duplicate ts_code")
        seen.add(raw_code)
        if not raw_code.endswith("." + _SUFFIX[exchange]):
            raise FormulaMarketUniverseError("stock_basic code conflicts with exchange")
        if raw_status != list_status:
            raise FormulaMarketUniverseError("stock_basic status conflicts with partition")
        listed = _date_value(raw_list_date)
        delisted = _date_value(raw_delist_date, optional=True)
        if (
            listed is None
            or listed > trade_date
            or (delisted is not None and delisted <= trade_date)
        ):
            raise FormulaMarketUniverseError("stock_basic dates conflict with target day or status")
        if _A_CODE.fullmatch(raw_code) is not None:
            entries.append(
                FormulaMarketEntry(
                    ts_code=raw_code,
                    exchange=exchange,
                    list_status=list_status,
                    list_date=listed,
                )
            )
        elif _B_CODE.fullmatch(raw_code) is not None:
            b_shares += 1
        else:
            other += 1
    return FormulaMarketPartition(
        exchange=exchange,
        list_status=list_status,
        raw_rows=len(frame),
        included_rows=len(entries),
        excluded_b_shares=b_shares,
        excluded_other=other,
    ), entries


def capture_formula_market_universe(
    adapter: FormulaMarketUniverseAdapter,
    calendar: MarketCalendarAuthority,
    trade_date: date,
    *,
    clock: Callable[[], datetime],
) -> FormulaMarketUniverseSnapshot:
    """Observe six source partitions on one authoritative open day, after 17:00."""
    if type(trade_date) is not date:
        raise FormulaMarketUniverseError("target trade date is invalid")
    try:
        authority = MarketCalendarAuthority.model_validate(calendar)
        started = normalize_aware_utc(clock())
        if not authority.coverage_start <= trade_date <= authority.coverage_end:
            raise FormulaMarketUniverseError("calendar does not cover target date")
        if trade_date not in authority.open_dates:
            raise FormulaMarketUniverseError("target date is not an authoritative open day")
        if authority.generated_at > started:
            raise FormulaMarketUniverseError("calendar is newer than capture start")
        local = started.astimezone(_SHANGHAI)
        if local.date() != trade_date or local.time() <= time(17):
            raise FormulaMarketUniverseError("capture must start on target date after 17:00")
        seen: set[str] = set()
        partitions: list[FormulaMarketPartition] = []
        entries: list[FormulaMarketEntry] = []
        for exchange, status in _PARTITIONS:
            frame = adapter.stock_basic_partition(list_status=status, exchange=exchange)
            part, accepted = _capture_partition(
                frame,
                exchange=exchange,
                list_status=status,
                trade_date=trade_date,
                seen=seen,
            )
            partitions.append(part)
            entries.extend(accepted)
        completed = normalize_aware_utc(clock())
        if completed < started:
            raise FormulaMarketUniverseError("capture clock moved backwards")
        completed_local = completed.astimezone(_SHANGHAI)
        if completed_local.date() != trade_date:
            raise FormulaMarketUniverseError("capture completed on a different date")
        if completed_local.time() <= time(17):
            raise FormulaMarketUniverseError("capture must complete after 17:00")
        return FormulaMarketUniverseSnapshot.create(
            trade_date=trade_date,
            started_at=started,
            completed_at=completed,
            calendar_sha256=authority.content_sha256,
            calendar_generated_at=authority.generated_at,
            partitions=tuple(partitions),
            entries=tuple(sorted(entries, key=lambda item: item.ts_code)),
        )
    except FormulaMarketUniverseError:
        raise
    except Exception as exc:
        raise FormulaMarketUniverseError(
            "market-list capture failed or returned invalid evidence"
        ) from exc


def _absolute_root(root: Path) -> Path:
    candidate = Path(root)
    if not candidate.is_absolute() or candidate != Path(os.path.normpath(candidate)):
        raise ValueError("market-list root must be an absolute normalized path")
    return candidate


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _open_directory(path: Path, *, create_final: bool = False) -> int:
    descriptor = os.open(path.anchor, _DIR_FLAGS)
    try:
        parts = path.parts[1:]
        for index, component in enumerate(parts):
            final = index == len(parts) - 1
            try:
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                if not (create_final and final):
                    raise
                os.mkdir(component, 0o700, dir_fd=descriptor)
                before = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            if not stat.S_ISDIR(before.st_mode) or stat.S_ISLNK(before.st_mode):
                raise FormulaMarketUniverseError("market-list directory is unsafe")
            if final and (before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o700):
                raise FormulaMarketUniverseError("market-list directory must be private and owned")
            child = os.open(component, _DIR_FLAGS, dir_fd=descriptor)
            if _identity(os.fstat(child)) != _identity(before):
                os.close(child)
                raise FormulaMarketUniverseError("market-list directory changed while opening")
            os.close(descriptor)
            descriptor = child
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _open_child(parent: int, name: str, *, create: bool) -> int:
    if create:
        with suppress(FileExistsError):
            os.mkdir(name, 0o700, dir_fd=parent)
    before = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISDIR(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o700
    ):
        raise FormulaMarketUniverseError("market-list directory is unsafe")
    child = os.open(name, _DIR_FLAGS, dir_fd=parent)
    if _identity(os.fstat(child)) != _identity(before):
        os.close(child)
        raise FormulaMarketUniverseError("market-list directory changed while opening")
    return child


def _read_file(
    parent: int,
    name: str,
    *,
    allowed_links: tuple[int, ...] = (1,),
) -> tuple[bytes, tuple[int, ...]]:
    before = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink not in allowed_links
        or not 0 < before.st_size <= _MAX_FILE_BYTES
    ):
        raise FormulaMarketUniverseError("market-list file is unsafe")
    descriptor = os.open(name, _READ_FLAGS, dir_fd=parent)
    try:
        observed = os.fstat(descriptor)
        if _identity(observed) != _identity(before):
            raise FormulaMarketUniverseError("market-list file changed while opening")
        chunks: list[bytes] = []
        remaining = observed.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if (
            remaining
            or _identity(after) != _identity(before)
            or _identity(current) != _identity(before)
        ):
            raise FormulaMarketUniverseError("market-list file changed while reading")
        return b"".join(chunks), _identity(before)
    finally:
        os.close(descriptor)


def _write_file(parent: int, name: str, payload: bytes) -> None:
    if not 0 < len(payload) <= _MAX_FILE_BYTES:
        raise FormulaMarketUniverseError("market-list file exceeds size budget")
    descriptor = os.open(name, _WRITE_FLAGS, 0o600, dir_fd=parent)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise FormulaMarketUniverseError("market-list write made no progress")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_generation(parent: int, digest: str, payload: bytes) -> None:
    name = f"{digest}.json"
    stage = f".{digest}.stage"
    try:
        existing, generation_identity = _read_file(parent, name, allowed_links=(1, 2))
    except FileNotFoundError:
        pass
    else:
        if existing != payload:
            raise FormulaMarketUniverseError("market-list generation conflicts with identity")
        if generation_identity[4] == 2:
            staged, stage_identity = _read_file(parent, stage, allowed_links=(2,))
            if staged != payload or stage_identity[:2] != generation_identity[:2]:
                raise FormulaMarketUniverseError("market-list recovery stage conflicts")
            os.unlink(stage, dir_fd=parent)
            os.fsync(parent)
        verified, _ = _read_file(parent, name)
        if verified != payload:
            raise FormulaMarketUniverseError("market-list generation changed during recovery")
        return
    try:
        try:
            staged, _ = _read_file(parent, stage)
        except FileNotFoundError:
            _write_file(parent, stage, payload)
        else:
            if staged != payload:
                raise FormulaMarketUniverseError("market-list recovery stage conflicts")
        try:
            os.link(stage, name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
        except FileExistsError as exc:
            existing, _ = _read_file(parent, name)
            if existing != payload:
                raise FormulaMarketUniverseError(
                    "market-list generation conflicts with identity"
                ) from exc
        os.unlink(stage, dir_fd=parent)
        os.fsync(parent)
        stored, _ = _read_file(parent, name)
        if stored != payload:
            raise FormulaMarketUniverseError("market-list generation did not verify")
    finally:
        with suppress(FileNotFoundError):
            os.unlink(stage, dir_fd=parent)


def _read_current(
    day_fd: int, trade_date: date, expected_sha256: str
) -> FormulaMarketUniverseSnapshot:
    pointer_bytes, pointer_identity = _read_file(day_fd, "current.json")
    pointer = _Pointer.model_validate(strict_canonical_json_loads(pointer_bytes))
    if pointer.trade_date != trade_date:
        raise FormulaMarketUniverseError("market-list pointer has the wrong date")
    if pointer.content_sha256 != expected_sha256:
        raise FormulaMarketUniverseError("market-list pointer changed")
    generation_fd = _open_child(day_fd, "generations", create=False)
    try:
        generation_bytes, _ = _read_file(generation_fd, f"{expected_sha256}.json")
    finally:
        os.close(generation_fd)
    if len(generation_bytes) != pointer.file_size:
        raise FormulaMarketUniverseError("market-list generation size changed")
    snapshot = FormulaMarketUniverseSnapshot.model_validate(
        strict_canonical_json_loads(generation_bytes)
    )
    if snapshot.trade_date != trade_date or snapshot.content_sha256 != expected_sha256:
        raise FormulaMarketUniverseError("market-list generation has the wrong identity or date")
    if canonical_json_bytes(snapshot.model_dump(mode="json")) != generation_bytes:
        raise FormulaMarketUniverseError("market-list generation is not canonical")
    current_bytes, current_identity = _read_file(day_fd, "current.json")
    if current_bytes != pointer_bytes or current_identity != pointer_identity:
        raise FormulaMarketUniverseError("market-list pointer changed while reading")
    return snapshot


def publish_formula_market_universe(
    root: Path,
    snapshot: FormulaMarketUniverseSnapshot,
) -> FormulaMarketPublicationReceipt:
    """Publish a content generation, then atomically move only its day's pointer."""
    candidate = _absolute_root(root)
    try:
        validated = FormulaMarketUniverseSnapshot.model_validate(snapshot)
        payload = canonical_json_bytes(validated.model_dump(mode="json"))
        if len(payload) > _MAX_FILE_BYTES:
            raise FormulaMarketUniverseError("market-list generation exceeds size budget")
        root_fd = _open_directory(candidate, create_final=True)
        try:
            day_fd = _open_child(root_fd, validated.trade_date.isoformat(), create=True)
            try:
                lock_fd = os.open(
                    ".publish.lock",
                    os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=day_fd,
                )
                try:
                    lock = os.fstat(lock_fd)
                    if (
                        not stat.S_ISREG(lock.st_mode)
                        or lock.st_uid != os.geteuid()
                        or stat.S_IMODE(lock.st_mode) != 0o600
                        or lock.st_nlink != 1
                    ):
                        raise FormulaMarketUniverseError("market-list publication lock is unsafe")
                    fcntl.flock(lock_fd, fcntl.LOCK_EX)
                    if _identity(
                        os.stat(".publish.lock", dir_fd=day_fd, follow_symlinks=False)
                    ) != _identity(os.fstat(lock_fd)):
                        raise FormulaMarketUniverseError("market-list publication lock changed")
                    try:
                        pointer_bytes, _ = _read_file(day_fd, "current.json")
                    except FileNotFoundError:
                        pointer_bytes = None
                    if pointer_bytes is not None:
                        old = _Pointer.model_validate(strict_canonical_json_loads(pointer_bytes))
                        _read_current(day_fd, validated.trade_date, old.content_sha256)
                        if old.content_sha256 == validated.content_sha256:
                            return FormulaMarketPublicationReceipt(
                                published=False,
                                generation_path=candidate
                                / validated.trade_date.isoformat()
                                / "generations"
                                / f"{validated.content_sha256}.json",
                                content_sha256=validated.content_sha256,
                            )
                    generations_fd = _open_child(day_fd, "generations", create=True)
                    try:
                        _ensure_generation(generations_fd, validated.content_sha256, payload)
                    finally:
                        os.close(generations_fd)
                    pointer = _Pointer(
                        trade_date=validated.trade_date,
                        content_sha256=validated.content_sha256,
                        file_size=len(payload),
                    )
                    stage = f".current.{uuid.uuid4().hex}.tmp"
                    try:
                        _write_file(
                            day_fd, stage, canonical_json_bytes(pointer.model_dump(mode="json"))
                        )
                        os.replace(stage, "current.json", src_dir_fd=day_fd, dst_dir_fd=day_fd)
                        os.fsync(day_fd)
                    finally:
                        with suppress(FileNotFoundError):
                            os.unlink(stage, dir_fd=day_fd)
                    _read_current(day_fd, validated.trade_date, validated.content_sha256)
                    return FormulaMarketPublicationReceipt(
                        published=True,
                        generation_path=candidate
                        / validated.trade_date.isoformat()
                        / "generations"
                        / f"{validated.content_sha256}.json",
                        content_sha256=validated.content_sha256,
                    )
                finally:
                    os.close(lock_fd)
            finally:
                os.close(day_fd)
        finally:
            os.close(root_fd)
    except FormulaMarketUniverseError:
        raise
    except Exception as exc:
        raise FormulaMarketUniverseError("market-list publication failed") from exc


def load_formula_market_universe(
    root: Path,
    trade_date: date,
    *,
    expected_sha256: str,
) -> FormulaMarketUniverseSnapshot:
    """Pin one day's current generation; reject changes instead of falling back."""
    candidate = _absolute_root(root)
    if type(trade_date) is not date or _SHA256.fullmatch(expected_sha256) is None:
        raise ValueError("market-list date or expected sha256 is invalid")
    try:
        root_fd = _open_directory(candidate)
        try:
            day_fd = _open_child(root_fd, trade_date.isoformat(), create=False)
            try:
                return _read_current(day_fd, trade_date, expected_sha256)
            finally:
                os.close(day_fd)
        finally:
            os.close(root_fd)
    except FormulaMarketUniverseError:
        raise
    except Exception as exc:
        raise FormulaMarketUniverseError(
            "market-list generation is unavailable or invalid"
        ) from exc
