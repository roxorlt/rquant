"""Offline full-history RSI projection for bounded replica screening reads."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import sqlite3
import stat
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field
from ta.momentum import RSIIndicator

if TYPE_CHECKING:
    from rquant.screen.replica_source import VerifiedReplicaScreenSource

MIN_PERIOD = 2
MAX_PERIOD = 60
MAX_STOCKS = 8_000
MAX_OFFSET = 30
MAX_DATES = 60
MAX_MANIFEST_BYTES = 16 * 1024
MAX_SOURCE_BARS = 100_000_000
MAX_STOCK_BARS = 50_000
MAX_QUERY_CELLS = 1_000_000
_FIELD = re.compile(r"RSI([1-9][0-9]?)\[(0|[1-9][0-9]?)\]\Z")
_FILE = re.compile(r"[0-9a-f]{32}\.sqlite\Z")
_PERIODS = range(MIN_PERIOD, MAX_PERIOD + 1)


class DynamicRsiProjectionUnavailableError(RuntimeError):
    """The requested RSI values cannot be trusted for this replica."""


class DynamicRsiProjectionBudgetError(RuntimeError):
    """The RSI request or source exceeds its fixed budget."""


class _Manifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    method: Literal["ta-rsi-ewm-v1"] = "ta-rsi-ewm-v1"
    file_name: str = Field(pattern=r"^[0-9a-f]{32}\.sqlite$")
    file_device: int = Field(ge=0)
    file_inode: int = Field(ge=0)
    file_size: int = Field(gt=0)
    file_mtime_ns: int = Field(ge=0)
    file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_updated_at: datetime
    dates: list[date] = Field(min_length=1, max_length=MAX_DATES)
    stock_count: int = Field(ge=1, le=MAX_STOCKS)
    row_count: int = Field(ge=1, le=MAX_STOCKS * MAX_DATES)


@dataclass(frozen=True, slots=True)
class DynamicRsiCatalog:
    dates: list[date]
    source_identity: str


@dataclass(frozen=True, slots=True)
class _Pinned:
    manifest: _Manifest
    pointer_stat: tuple[int, int, int, int, int]
    file_stat: tuple[int, int, int, int, int]


def requested_dynamic_rsi(columns: set[str] | frozenset[str]) -> dict[str, tuple[int, int]]:
    selected: dict[str, tuple[int, int]] = {}
    for column in columns:
        if not isinstance(column, str) or not column.startswith("RSI"):
            continue
        match = _FIELD.fullmatch(column)
        if match is None:
            raise ValueError("unsupported dynamic RSI dependency")
        period, offset = int(match.group(1)), int(match.group(2))
        if not MIN_PERIOD <= period <= MAX_PERIOD or offset > MAX_OFFSET:
            raise ValueError("unsupported dynamic RSI dependency")
        selected[column] = period, offset
    return selected


def _identity(observed: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _regular(path: Path) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as error:
        raise DynamicRsiProjectionUnavailableError("RSI generation is unavailable") from error
    if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
        raise DynamicRsiProjectionUnavailableError("RSI generation is not independent")
    return observed


class VerifiedDynamicRsiProjection:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        if not self.root.is_absolute() or self.root != Path(os.path.abspath(self.root)):
            raise ValueError("RSI root must be absolute and canonical")
        if self.root.resolve(strict=False) != self.root:
            raise ValueError("RSI root must not follow a symbolic link")
        self._verified_digest: tuple[tuple[int, int, int, int, int], str] | None = None

    def _verify(self) -> _Pinned:
        pointer = self.root / "current.json"
        observed = _regular(pointer)
        if not 1 <= observed.st_size <= MAX_MANIFEST_BYTES:
            raise DynamicRsiProjectionUnavailableError("RSI manifest is invalid")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(pointer, flags)
            try:
                if _identity(os.fstat(descriptor)) != _identity(observed):
                    raise DynamicRsiProjectionUnavailableError("RSI manifest changed")
                payload = os.read(descriptor, MAX_MANIFEST_BYTES + 1)
                if (
                    len(payload) != observed.st_size
                    or _identity(os.fstat(descriptor)) != _identity(observed)
                    or _identity(_regular(pointer)) != _identity(observed)
                ):
                    raise DynamicRsiProjectionUnavailableError("RSI manifest changed")
            finally:
                os.close(descriptor)
            manifest = _Manifest.model_validate_json(payload)
        except (OSError, ValueError) as error:
            raise DynamicRsiProjectionUnavailableError("RSI manifest is invalid") from error
        if not _FILE.fullmatch(manifest.file_name):
            raise DynamicRsiProjectionUnavailableError("RSI filename is invalid")
        data = _regular(self.root / manifest.file_name)
        file_stat = _identity(data)
        if (
            (data.st_dev, data.st_ino, data.st_size, data.st_mtime_ns)
            != (
                manifest.file_device,
                manifest.file_inode,
                manifest.file_size,
                manifest.file_mtime_ns,
            )
            or data.st_ctime_ns > observed.st_ctime_ns
            or manifest.row_count != manifest.stock_count * len(manifest.dates)
            or manifest.dates != sorted(set(manifest.dates), reverse=True)
        ):
            raise DynamicRsiProjectionUnavailableError("RSI generation does not match")
        if self._verified_digest != (file_stat, manifest.file_sha256):
            try:
                with (self.root / manifest.file_name).open("rb") as handle:
                    digest = hashlib.file_digest(handle, "sha256").hexdigest()
            except OSError as error:
                raise DynamicRsiProjectionUnavailableError("RSI digest is unavailable") from error
            if (
                digest != manifest.file_sha256
                or _identity(_regular(self.root / manifest.file_name)) != file_stat
            ):
                raise DynamicRsiProjectionUnavailableError("RSI generation digest does not match")
            self._verified_digest = file_stat, digest
        return _Pinned(manifest, _identity(observed), file_stat)

    def catalog(self, source_identity: str) -> DynamicRsiCatalog:
        generation = self._verify()
        if generation.manifest.source_identity != source_identity:
            raise DynamicRsiProjectionUnavailableError("RSI source has changed")
        return DynamicRsiCatalog(generation.manifest.dates[:30], source_identity)

    def _open(self, generation: _Pinned) -> _PinnedConnection:
        path = self.root / generation.manifest.file_name
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
        except OSError as error:
            raise DynamicRsiProjectionUnavailableError("RSI generation is unavailable") from error
        connection: sqlite3.Connection | None = None
        try:
            if _identity(os.fstat(descriptor)) != generation.file_stat:
                raise DynamicRsiProjectionUnavailableError("RSI generation changed")
            fd_root = "/proc/self/fd" if Path("/proc/self/fd").is_dir() else "/dev/fd"
            connection = sqlite3.connect(
                f"file:{fd_root}/{descriptor}?mode=ro&immutable=1", uri=True
            )
            connection.execute("PRAGMA query_only=ON")
            if (
                self._verify() != generation
                or _identity(os.fstat(descriptor)) != generation.file_stat
            ):
                connection.close()
                raise DynamicRsiProjectionUnavailableError("RSI generation changed")
            return _PinnedConnection(connection, descriptor, self, generation)
        except (OSError, sqlite3.Error) as error:
            if connection is not None:
                connection.close()
            os.close(descriptor)
            raise DynamicRsiProjectionUnavailableError("RSI generation is unavailable") from error
        except Exception:
            if connection is not None:
                connection.close()
            os.close(descriptor)
            raise

    def values(
        self,
        source_identity: str,
        trade_date: date,
        ts_codes: list[str],
        columns: dict[str, tuple[int, int]],
    ) -> pd.DataFrame:
        if len(ts_codes) > MAX_STOCKS or len(ts_codes) != len(set(ts_codes)):
            raise DynamicRsiProjectionBudgetError("RSI stock budget exceeded")
        if len(ts_codes) * (len(columns) + 5) > MAX_QUERY_CELLS:
            raise DynamicRsiProjectionBudgetError("RSI wide budget exceeded")
        for column, parts in columns.items():
            if requested_dynamic_rsi(frozenset({column})).get(column) != parts:
                raise ValueError("unsupported dynamic RSI dependency")
        generation = self._verify()
        dates = generation.manifest.dates
        if generation.manifest.source_identity != source_identity or trade_date not in dates[:30]:
            raise DynamicRsiProjectionUnavailableError("RSI source or date has changed")
        target = dates.index(trade_date)
        output_columns: dict[str, list[float | None]] = {
            column: [None] * len(ts_codes) for column in columns
        }
        if not ts_codes or not columns:
            return pd.DataFrame({"ts_code": ts_codes, **output_columns})
        by_day: dict[date, dict[int, list[str]]] = {}
        for column, (period, offset) in columns.items():
            if target + offset < len(dates):
                by_day.setdefault(dates[target + offset], {}).setdefault(period, []).append(column)
        if not by_day:
            return pd.DataFrame({"ts_code": ts_codes, **output_columns})
        try:
            with self._open(generation) as connection:
                for day, fields in by_day.items():
                    periods = sorted(fields)
                    selected = ", ".join(f"rsi{period}" for period in periods)
                    for start in range(0, len(ts_codes), 500):
                        batch = ts_codes[start : start + 500]
                        code_slots = ",".join("?" for _ in batch)
                        rows = connection.execute(
                            f"SELECT ts_code, {selected} FROM rsi "
                            f"WHERE ts_code IN ({code_slots}) AND trade_date=?",
                            [*batch, day.isoformat()],
                        ).fetchall()
                        if len(rows) != len(batch):
                            raise DynamicRsiProjectionUnavailableError(
                                "RSI generation is incomplete"
                            )
                        positions = {code: start + index for index, code in enumerate(batch)}
                        for code, *numbers in rows:
                            position = positions[code]
                            for period, number in zip(periods, numbers, strict=True):
                                for column in fields[period]:
                                    output_columns[column][position] = number
        except sqlite3.Error as error:
            raise DynamicRsiProjectionUnavailableError("RSI lookup failed") from error
        return pd.DataFrame({"ts_code": ts_codes, **output_columns})


class _PinnedConnection:
    def __init__(
        self,
        connection: sqlite3.Connection,
        descriptor: int,
        owner: VerifiedDynamicRsiProjection,
        generation: _Pinned,
    ) -> None:
        self.connection = connection
        self.descriptor = descriptor
        self.owner = owner
        self.generation = generation

    def __enter__(self) -> sqlite3.Connection:
        return self.connection

    def __exit__(self, *_: object) -> None:
        try:
            if (
                self.owner._verify() != self.generation
                or _identity(os.fstat(self.descriptor)) != self.generation.file_stat
            ):
                raise DynamicRsiProjectionUnavailableError("RSI generation changed during read")
        finally:
            self.connection.close()
            os.close(self.descriptor)


def publish_dynamic_rsi_projection(
    source: VerifiedReplicaScreenSource,
    root: Path,
) -> DynamicRsiCatalog:
    """Build from one pinned replica; replace the pointer only after complete validation."""
    root = Path(root)
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError("RSI root must be absolute and canonical")
    root.mkdir(parents=True, exist_ok=True)
    if root.resolve() != root:
        raise ValueError("RSI root must not follow a symbolic link")
    name = f"{uuid.uuid4().hex}.sqlite"
    temporary = root / f".{name}.tmp"
    pointer_tmp = root / f".current-{uuid.uuid4().hex}.tmp"
    duck, descriptor, replica = source._open()
    try:
        max_row = duck.execute("SELECT MAX(trade_date) FROM daily_bar").fetchone()
        if max_row is None or max_row[0] is None:
            raise DynamicRsiProjectionUnavailableError("RSI source has no daily bars")
        latest = max_row[0]
        calendar = duck.execute(
            "SELECT cal_date, is_open FROM trade_calendar WHERE exchange='SSE' "
            "AND cal_date <= ? ORDER BY cal_date",
            [latest],
        ).fetchall()
        if not calendar or calendar[-1] != (latest, True):
            raise DynamicRsiProjectionUnavailableError("RSI calendar is incomplete")
        seen_days: set[date] = set()
        open_days: set[date] = set()
        for day, is_open in calendar:
            if day in seen_days or is_open is None:
                raise DynamicRsiProjectionUnavailableError("RSI calendar is ambiguous")
            seen_days.add(day)
            if is_open:
                open_days.add(day)
        dates = sorted(open_days, reverse=True)[:MAX_DATES]
        output = sqlite3.connect(temporary)
        try:
            output.execute("PRAGMA journal_mode=OFF")
            output.execute("PRAGMA synchronous=OFF")
            cols = ", ".join(f"rsi{period} REAL" for period in _PERIODS)
            output.execute(
                f"CREATE TABLE rsi (ts_code TEXT NOT NULL, trade_date TEXT NOT NULL, {cols}, "
                "PRIMARY KEY(ts_code,trade_date)) WITHOUT ROWID"
            )
            cursor = duck.execute(
                "SELECT daily.ts_code, daily.trade_date, daily.close, adj.adj_factor "
                "FROM daily_bar AS daily LEFT JOIN adj_factor AS adj "
                "ON adj.ts_code=daily.ts_code AND adj.trade_date=daily.trade_date "
                "ORDER BY daily.ts_code, daily.trade_date"
            )
            code: str | None = None
            history: list[tuple[date, float | None]] = []
            source_rows = 0
            stock_count = 0
            row_count = 0
            bar_days: set[date] = set()

            def flush_stock() -> None:
                nonlocal stock_count, row_count
                if code is None:
                    return
                stock_count += 1
                if stock_count > MAX_STOCKS:
                    raise DynamicRsiProjectionBudgetError("RSI stock budget exceeded")
                indexed = {day: index for index, (day, _) in enumerate(history)}
                first_valid = next(
                    (i for i, (_, value) in enumerate(history) if value is not None), len(history)
                )
                first_bad = next(
                    (i for i in range(first_valid, len(history)) if history[i][1] is None),
                    len(history),
                )
                valid = pd.Series(
                    [value for _, value in history[first_valid:first_bad]], dtype="float64"
                )
                calculated = {
                    period: RSIIndicator(valid, window=period).rsi().tolist() for period in _PERIODS
                }
                rows = []
                for day in dates:
                    position = indexed.get(day)
                    slot = position - first_valid if position is not None else -1
                    numbers = [
                        float(values[slot])
                        if 0 <= slot < len(values) and math.isfinite(values[slot])
                        else None
                        for values in calculated.values()
                    ]
                    rows.append((code, day.isoformat(), *numbers))
                slots = ",".join("?" for _ in range(2 + len(_PERIODS)))
                output.executemany(f"INSERT INTO rsi VALUES ({slots})", rows)
                row_count += len(rows)

            while batch := cursor.fetchmany(4096):
                for stock, day, close, factor in batch:
                    source_rows += 1
                    if source_rows > MAX_SOURCE_BARS:
                        raise DynamicRsiProjectionBudgetError("RSI source exceeds bar budget")
                    if code != stock:
                        flush_stock()
                        code, history = stock, []
                    if history and day <= history[-1][0]:
                        raise DynamicRsiProjectionUnavailableError("duplicate RSI source bar")
                    if day not in open_days:
                        raise DynamicRsiProjectionUnavailableError(
                            "RSI bar is outside the trade calendar"
                        )
                    bar_days.add(day)
                    if not isinstance(stock, str) or not stock:
                        raise DynamicRsiProjectionUnavailableError("RSI stock identity is invalid")
                    if close is None or factor is None:
                        adjusted = None
                    else:
                        price, adjustment = float(close), float(factor)
                        adjusted = (
                            price * adjustment
                            if (
                                math.isfinite(price)
                                and price > 0
                                and math.isfinite(adjustment)
                                and adjustment > 0
                                and math.isfinite(price * adjustment)
                            )
                            else None
                        )
                    history.append((day, adjusted))
                    if len(history) > MAX_STOCK_BARS:
                        raise DynamicRsiProjectionBudgetError("RSI stock history exceeds budget")
                if source_rows > MAX_SOURCE_BARS:
                    raise DynamicRsiProjectionBudgetError("RSI source exceeds bar budget")
            flush_stock()
            if not stock_count or row_count != stock_count * len(dates):
                raise DynamicRsiProjectionUnavailableError("RSI generation is incomplete")
            if not set(dates).issubset(bar_days):
                raise DynamicRsiProjectionUnavailableError("RSI recent trading days are incomplete")
            oldest = min(day for day, _ in calendar)
            newest = latest
            if len(seen_days) != (newest - oldest).days + 1 or any(
                oldest + timedelta(days=index) not in seen_days
                for index in range((newest - oldest).days + 1)
            ):
                raise DynamicRsiProjectionUnavailableError("RSI calendar coverage is incomplete")
            output.commit()
            if output.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise DynamicRsiProjectionUnavailableError("RSI generation failed integrity check")
        finally:
            output.close()
        source._finish(descriptor, replica)
        os.chmod(temporary, 0o444)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        final = root / name
        os.replace(temporary, final)
        observed = _regular(final)
        manifest = _Manifest(
            file_name=name,
            file_device=observed.st_dev,
            file_inode=observed.st_ino,
            file_size=observed.st_size,
            file_mtime_ns=observed.st_mtime_ns,
            file_sha256=digest,
            source_identity=replica.identity,
            source_updated_at=replica.updated_at,
            dates=dates,
            stock_count=stock_count,
            row_count=row_count,
        )
        payload = json.dumps(
            manifest.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        with pointer_tmp.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        source._finish(descriptor, replica)
        os.replace(pointer_tmp, root / "current.json")
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return VerifiedDynamicRsiProjection(root).catalog(replica.identity)
    finally:
        duck.close()
        os.close(descriptor)
        with suppress(FileNotFoundError):
            temporary.unlink()
        with suppress(FileNotFoundError):
            pointer_tmp.unlink()
