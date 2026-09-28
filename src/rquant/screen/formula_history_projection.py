"""Offline-published, indexed history for bounded single-stock formula reads."""

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
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from rquant.screen.replica_source import VerifiedReplicaScreenSource
from rquant.screen.tdx.evaluate import (
    MAX_BARS_PER_STOCK,
    MAX_HISTORY_SPAN_DAYS,
    MAX_STOCKS,
    HistoricalBar,
    StockHistory,
)

MAX_MANIFEST_BYTES = 16 * 1024
MAX_SQLITE_STEPS = 1_000_000
MAX_CATALOG_STOCKS = MAX_STOCKS
_FILE_NAME = re.compile(r"[0-9a-f]{32}\.sqlite\Z")
_SZ_A_PREFIXES = ("000", "001", "002", "003", "300", "301")
_SH_A_PREFIXES = ("600", "601", "603", "605", "688", "689")
_BJ_A_PREFIXES = ("4", "8", "9")
_A_SHARE_CODE = re.compile(
    r"(?:"
    rf"(?:{'|'.join(_SZ_A_PREFIXES)})[0-9]{{3}}\.SZ|"
    rf"(?:{'|'.join(_SH_A_PREFIXES)})[0-9]{{3}}\.SH|"
    rf"(?:{'|'.join(_BJ_A_PREFIXES)})[0-9]{{5}}\.BJ"
    r")\Z"
)
_A_SHARE_LISTING_SQL = (
    "SELECT ts_code,list_date FROM listing WHERE "
    "(substr(ts_code,1,3) IN (" + ",".join("?" for _ in _SZ_A_PREFIXES)
    + ") AND ts_code LIKE '%.SZ') OR "
    "(substr(ts_code,1,3) IN (" + ",".join("?" for _ in _SH_A_PREFIXES)
    + ") AND ts_code LIKE '%.SH') OR "
    "(substr(ts_code,1,1) IN (" + ",".join("?" for _ in _BJ_A_PREFIXES)
    + ") AND ts_code LIKE '%.BJ') "
    "ORDER BY ts_code LIMIT ?"
)
_BARS_SQL = (
    "SELECT trade_date, open, high, low, close, vol, amount "
    "FROM bars INDEXED BY sqlite_autoindex_bars_1 "
    "WHERE ts_code = ? AND trade_date <= ?{listing} "
    "ORDER BY trade_date DESC LIMIT ?"
)


class FormulaProjectionUnavailableError(RuntimeError):
    """The independent history generation cannot be trusted or read."""


class FormulaProjectionChangedError(FormulaProjectionUnavailableError):
    """The requested history generation has been replaced."""


class FormulaProjectionDateError(RuntimeError):
    """The selected date is not an open day in this history generation."""


class FormulaProjectionBudgetError(RuntimeError):
    """The requested single-stock history exceeds its fixed budget."""


class _ProjectionManifest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    file_name: str = Field(pattern=r"^[0-9a-f]{32}\.sqlite$")
    file_device: int = Field(ge=0)
    file_inode: int = Field(ge=0)
    file_size: int = Field(gt=0)
    file_mtime_ns: int = Field(ge=0)
    file_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_updated_at: datetime
    dates: list[date] = Field(max_length=30)
    bar_count: int = Field(ge=0)


@dataclass(frozen=True, slots=True)
class FormulaProjectionCatalog:
    identity: str
    updated_at: datetime
    dates: list[date]


@dataclass(frozen=True, slots=True)
class FormulaHistorySnapshot:
    stock: StockHistory | None
    unknown_reason: Literal[
        "missing_date", "missing_listing", "missing_calendar", "missing_history",
        "invalid_value",
    ] | None
    identity: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class FormulaCatalogEntry:
    stock_code: str
    list_date: date | None


@dataclass(frozen=True, slots=True)
class FormulaCatalogSnapshot:
    identity: str
    updated_at: datetime
    entries: tuple[FormulaCatalogEntry, ...]


@dataclass(frozen=True, slots=True)
class _PinnedGeneration:
    manifest: _ProjectionManifest
    identity: str
    manifest_stat: tuple[int, int, int, int, int]
    file_stat: tuple[int, int, int, int, int]


def _identity(observed: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        observed.st_dev, observed.st_ino, observed.st_size,
        observed.st_mtime_ns, observed.st_ctime_ns,
    )


def _regular(path: Path) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as error:
        raise FormulaProjectionUnavailableError("history file is unavailable") from error
    if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
        raise FormulaProjectionUnavailableError("history file is not independent")
    return observed


class VerifiedFormulaHistoryProjection:
    """Read one immutable SQLite generation through its pinned descriptor."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        if not self.root.is_absolute() or self.root != Path(os.path.abspath(self.root)):
            raise ValueError("history root must be absolute and canonical")
        if self.root.resolve(strict=False) != self.root:
            raise ValueError("history root must not follow a symbolic link")

    def _verify(self) -> _PinnedGeneration:
        pointer = self.root / "current.json"
        observed = _regular(pointer)
        if not 1 <= observed.st_size <= MAX_MANIFEST_BYTES:
            raise FormulaProjectionUnavailableError("history manifest is invalid")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(pointer, flags)
            try:
                if _identity(os.fstat(descriptor)) != _identity(observed):
                    raise FormulaProjectionUnavailableError("history manifest changed")
                payload = os.read(descriptor, MAX_MANIFEST_BYTES + 1)
                if (
                    len(payload) != observed.st_size
                    or _identity(os.fstat(descriptor)) != _identity(observed)
                    or _identity(_regular(pointer)) != _identity(observed)
                ):
                    raise FormulaProjectionUnavailableError("history manifest changed")
            finally:
                os.close(descriptor)
            manifest = _ProjectionManifest.model_validate_json(payload)
        except (OSError, ValueError) as error:
            raise FormulaProjectionUnavailableError("history manifest is invalid") from error
        if not _FILE_NAME.fullmatch(manifest.file_name):
            raise FormulaProjectionUnavailableError("history filename is invalid")
        data = _regular(self.root / manifest.file_name)
        if (
            (data.st_dev, data.st_ino, data.st_size, data.st_mtime_ns)
            != (
                manifest.file_device, manifest.file_inode,
                manifest.file_size, manifest.file_mtime_ns,
            )
            or data.st_ctime_ns > observed.st_ctime_ns
        ):
            raise FormulaProjectionUnavailableError("history generation does not match")
        return _PinnedGeneration(
            manifest=manifest,
            identity=hashlib.sha256(payload).hexdigest(),
            manifest_stat=_identity(observed),
            file_stat=_identity(data),
        )

    def catalog(self) -> FormulaProjectionCatalog:
        generation = self._verify()
        return FormulaProjectionCatalog(
            identity=generation.identity,
            updated_at=generation.manifest.source_updated_at,
            dates=generation.manifest.dates,
        )

    def _open(self) -> tuple[sqlite3.Connection, int, _PinnedGeneration]:
        before = self._verify()
        path = self.root / before.manifest.file_name
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(path, flags)
            if _identity(os.fstat(descriptor)) != before.file_stat:
                raise FormulaProjectionUnavailableError("history file changed")
            fd_root = "/proc/self/fd" if Path("/proc/self/fd").is_dir() else "/dev/fd"
            connection = sqlite3.connect(
                f"file:{fd_root}/{descriptor}?mode=ro&immutable=1", uri=True,
            )
            connection.execute("PRAGMA query_only=ON")
            if self._verify() != before or _identity(os.fstat(descriptor)) != before.file_stat:
                raise FormulaProjectionUnavailableError("history changed while opening")
            return connection, descriptor, before
        except (OSError, sqlite3.Error, FormulaProjectionUnavailableError) as error:
            if "connection" in locals():
                connection.close()
            if "descriptor" in locals():
                os.close(descriptor)
            raise FormulaProjectionUnavailableError("history is unavailable") from error

    def _finish(self, descriptor: int, before: _PinnedGeneration) -> None:
        if _identity(os.fstat(descriptor)) != before.file_stat or self._verify() != before:
            raise FormulaProjectionChangedError("history changed during request")

    def catalog_snapshot(
        self, trade_date: date, *, expected_identity: str,
    ) -> FormulaCatalogSnapshot:
        if type(trade_date) is not date:
            raise FormulaProjectionDateError("date is invalid")
        connection, descriptor, generation = self._open()
        try:
            if generation.identity != expected_identity:
                raise FormulaProjectionChangedError("history changed")
            calendar_day = connection.execute(
                "SELECT is_open FROM calendar WHERE exchange='SSE' AND cal_date=?",
                (trade_date.isoformat(),),
            ).fetchone()
            if calendar_day is None or calendar_day[0] != 1:
                raise FormulaProjectionDateError("date is not an open SSE day")
            steps = 0

            def count_steps() -> int:
                nonlocal steps
                steps += 1000
                return int(steps > MAX_SQLITE_STEPS)

            connection.set_progress_handler(count_steps, 1000)
            try:
                rows = connection.execute(
                    _A_SHARE_LISTING_SQL,
                    (*_SZ_A_PREFIXES, *_SH_A_PREFIXES, *_BJ_A_PREFIXES,
                     MAX_CATALOG_STOCKS + 1),
                ).fetchall()
            finally:
                connection.set_progress_handler(None, 0)
            if len(rows) > MAX_CATALOG_STOCKS:
                raise FormulaProjectionBudgetError("history catalog exceeds the allowed range")
            entries: list[FormulaCatalogEntry] = []
            seen: set[str] = set()
            for code, raw_listing in rows:
                if not isinstance(code, str) or not _A_SHARE_CODE.fullmatch(code) or code in seen:
                    raise FormulaProjectionUnavailableError("history catalog code is invalid")
                seen.add(code)
                listing = None
                if raw_listing is not None:
                    if not isinstance(raw_listing, str):
                        raise FormulaProjectionUnavailableError("history listing date is invalid")
                    try:
                        listing = date.fromisoformat(raw_listing)
                    except ValueError as error:
                        raise FormulaProjectionUnavailableError(
                            "history listing date is invalid"
                        ) from error
                    if listing.isoformat() != raw_listing:
                        raise FormulaProjectionUnavailableError("history listing date is invalid")
                entries.append(FormulaCatalogEntry(code, listing))
            self._finish(descriptor, generation)
            return FormulaCatalogSnapshot(
                identity=generation.identity,
                updated_at=generation.manifest.source_updated_at,
                entries=tuple(entries),
            )
        except sqlite3.Error as error:
            raise FormulaProjectionUnavailableError("history catalog query failed") from error
        finally:
            connection.close()
            os.close(descriptor)

    def formula_history(
        self, trade_date: date, stock_code: str, *, expected_identity: str,
        lookback: int, full_history: bool,
    ) -> FormulaHistorySnapshot:
        if type(trade_date) is not date or not 0 <= lookback <= 500:
            raise FormulaProjectionBudgetError("history exceeds the allowed range")
        connection, descriptor, generation = self._open()

        def unknown(reason: Literal[
            "missing_date", "missing_listing", "missing_calendar", "missing_history",
            "invalid_value",
        ]) -> FormulaHistorySnapshot:
            self._finish(descriptor, generation)
            return FormulaHistorySnapshot(
                stock=None, unknown_reason=reason,
                identity=generation.identity,
                updated_at=generation.manifest.source_updated_at,
            )

        try:
            if generation.identity != expected_identity:
                raise FormulaProjectionChangedError("history changed")
            calendar_day = connection.execute(
                "SELECT is_open FROM calendar WHERE exchange='SSE' AND cal_date=?",
                (trade_date.isoformat(),),
            ).fetchone()
            if calendar_day is None or calendar_day[0] != 1:
                self._finish(descriptor, generation)
                raise FormulaProjectionDateError("date is not an open SSE day")
            listing_row = connection.execute(
                "SELECT list_date FROM listing WHERE ts_code=?", (stock_code,),
            ).fetchone()
            listing = date.fromisoformat(listing_row[0]) if listing_row and listing_row[0] else None
            if full_history and listing is None:
                return unknown("missing_listing")
            if listing is not None and listing > trade_date:
                return unknown("missing_date")
            max_rows = MAX_BARS_PER_STOCK + 1 if full_history else lookback + 1
            listing_sql = " AND trade_date >= ?" if listing is not None else ""
            sql = _BARS_SQL.format(listing=listing_sql)
            parameters: list[object] = [stock_code, trade_date.isoformat()]
            if listing is not None:
                parameters.append(listing.isoformat())
            parameters.append(max_rows)
            plan = connection.execute("EXPLAIN QUERY PLAN " + sql, parameters).fetchall()
            if not any(
                "SEARCH bars USING PRIMARY KEY (ts_code=?" in row[3]
                for row in plan
            ) or any("SCAN bars" in row[3] for row in plan):
                raise FormulaProjectionUnavailableError("history lookup is not indexed")
            steps = 0

            def count_steps() -> int:
                nonlocal steps
                steps += 1000
                return int(steps > MAX_SQLITE_STEPS)

            connection.set_progress_handler(count_steps, 1000)
            rows = connection.execute(sql, parameters).fetchall()
            connection.set_progress_handler(None, 0)
            if full_history and len(rows) > MAX_BARS_PER_STOCK:
                raise FormulaProjectionBudgetError("history exceeds the allowed range")
            if not rows or rows[0][0] != trade_date.isoformat():
                return unknown("missing_date")
            rows.reverse()
            if not full_history and len(rows) < max_rows and listing is None:
                return unknown("missing_listing")
            start = (
                listing if full_history or len(rows) < max_rows
                else date.fromisoformat(rows[0][0])
            )
            if start is None or (trade_date - start).days > MAX_HISTORY_SPAN_DAYS:
                raise FormulaProjectionBudgetError("history exceeds the allowed range")
            calendar = connection.execute(
                "SELECT cal_date,is_open FROM calendar "
                "WHERE exchange='SSE' AND cal_date BETWEEN ? AND ? "
                "ORDER BY cal_date LIMIT ?",
                (start.isoformat(), trade_date.isoformat(), MAX_HISTORY_SPAN_DAYS + 2),
            ).fetchall()
            if (
                len(calendar) != (trade_date - start).days + 1
                or any(day != (start + timedelta(days=index)).isoformat()
                       for index, (day, _) in enumerate(calendar))
            ):
                return unknown("missing_calendar")
            if {day for day, is_open in calendar if is_open} != {row[0] for row in rows}:
                return unknown("missing_history")
            if any(value is not None and not math.isfinite(value)
                   for row in rows for value in row[1:]):
                return unknown("invalid_value")
            stock = StockHistory(
                stock_code=stock_code, complete_from_listing=full_history,
                bars=tuple(HistoricalBar(
                    trade_date=date.fromisoformat(row[0]), open=row[1], high=row[2],
                    low=row[3], close=row[4], vol=row[5], amount=row[6],
                ) for row in rows),
            )
            self._finish(descriptor, generation)
            return FormulaHistorySnapshot(
                stock=stock, unknown_reason=None,
                identity=generation.identity,
                updated_at=generation.manifest.source_updated_at,
            )
        except sqlite3.Error as error:
            raise FormulaProjectionUnavailableError("history query failed") from error
        finally:
            connection.close()
            os.close(descriptor)


def publish_formula_history_projection(
    source: VerifiedReplicaScreenSource, root: Path,
) -> FormulaProjectionCatalog:
    """Build outside the web process; publish one immutable file with one pointer swap."""
    root = Path(root)
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError("history root must be absolute and canonical")
    root.mkdir(parents=True, exist_ok=True)
    if root.resolve() != root:
        raise ValueError("history root must not follow a symbolic link")
    name = f"{uuid.uuid4().hex}.sqlite"
    temporary = root / f".{name}.tmp"
    final = root / name
    pointer_tmp = root / f".current-{uuid.uuid4().hex}.tmp"
    duck, descriptor, replica = source._open()
    try:
        output = sqlite3.connect(temporary)
        try:
            output.execute("PRAGMA journal_mode=OFF")
            output.execute("PRAGMA synchronous=OFF")
            output.execute(
                "CREATE TABLE bars (ts_code TEXT NOT NULL, trade_date TEXT NOT NULL, "
                "open REAL, high REAL, low REAL, close REAL, vol REAL, amount REAL, "
                "PRIMARY KEY(ts_code,trade_date)) WITHOUT ROWID"
            )
            output.execute(
                "CREATE TABLE calendar (exchange TEXT NOT NULL, cal_date TEXT NOT NULL, "
                "is_open INTEGER NOT NULL, PRIMARY KEY(exchange,cal_date)) WITHOUT ROWID"
            )
            output.execute(
                "CREATE TABLE listing (ts_code TEXT PRIMARY KEY, list_date TEXT) WITHOUT ROWID"
            )
            calendar_rows = duck.execute(
                "SELECT exchange,cal_date,is_open FROM trade_calendar"
            ).fetchall()
            output.executemany(
                "INSERT INTO calendar VALUES (?,?,?)",
                ((exchange, day.isoformat(), int(is_open))
                 for exchange, day, is_open in calendar_rows),
            )
            output.executemany(
                "INSERT INTO listing VALUES (?,?)",
                ((code, day.isoformat() if day else None)
                 for code, day in duck.execute(
                     "SELECT ts_code,list_date FROM stock_basic"
                 ).fetchall()),
            )
            cursor = duck.execute(
                "SELECT ts_code,trade_date,open,high,low,close,vol,amount FROM daily_bar"
            )
            seen_dates: set[date] = set()
            bar_count = 0
            while batch := cursor.fetchmany(4096):
                output.executemany(
                    "INSERT INTO bars VALUES (?,?,?,?,?,?,?,?)",
                    ((code, day.isoformat(), *values)
                     for code, day, *values in batch),
                )
                seen_dates.update(row[1] for row in batch)
                bar_count += len(batch)
            output.commit()
            if output.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise FormulaProjectionUnavailableError("history build failed validation")
        finally:
            output.close()
        source._finish(descriptor, replica)
        os.chmod(temporary, 0o444)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        os.replace(temporary, final)
        observed = _regular(final)
        open_dates = {day for exchange, day, is_open in calendar_rows
                      if exchange == "SSE" and is_open}
        dates = sorted(seen_dates & open_dates, reverse=True)[:30]
        manifest = _ProjectionManifest(
            file_name=name,
            file_device=observed.st_dev, file_inode=observed.st_ino,
            file_size=observed.st_size, file_mtime_ns=observed.st_mtime_ns,
            file_sha256=digest,
            source_identity=replica.identity,
            source_updated_at=replica.updated_at,
            dates=dates, bar_count=bar_count,
        )
        payload = json.dumps(
            manifest.model_dump(mode="json"), sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        with pointer_tmp.open("wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(pointer_tmp, root / "current.json")
        directory = os.open(root, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        return VerifiedFormulaHistoryProjection(root).catalog()
    finally:
        duck.close()
        os.close(descriptor)
        with suppress(FileNotFoundError):
            temporary.unlink()
        with suppress(FileNotFoundError):
            pointer_tmp.unlink()
