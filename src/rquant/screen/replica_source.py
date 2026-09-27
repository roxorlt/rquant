"""Bounded post-close screening from one verified read-only DuckDB replica."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

import duckdb
import pandas as pd

from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.replica_generation import (
    ReplicaFileWatermark,
    ReplicaGenerationMetadata,
    replica_generation_path,
)
from rquant.screen.core import _collect_aggregates, _infer_lookback
from rquant.screen.dynamic_ma import (
    MAX_DYNAMIC_MA_FACTS,
    DynamicMaFactError,
    dynamic_ma_day_count,
    requested_dynamic_ma,
)
from rquant.screen.loader import (
    ScreeningCalendarError,
    ScreeningFactError,
    _selected_sources,
    load_universe,
)
from rquant.screen.rules import Rule, required_rule_columns

if TYPE_CHECKING:
    from rquant.storage.duckdb import DuckDBStore

MAX_CONDITIONS = 26
MAX_STOCKS = 8_000
MAX_LOOKBACK = 90
MAX_AGGREGATE_WINDOW = 500
MAX_WIDE_CELLS = 1_000_000
MAX_AGGREGATE_FACTS = 8_000_000
MAX_SIDECAR_BYTES = 16 * 1024


class ScreenReplicaUnavailableError(RuntimeError):
    """The bound replica generation cannot be trusted for this read."""


class ScreenReplicaDataError(RuntimeError):
    """The verified replica lacks authoritative facts for this screen."""


class ScreenReplicaBudgetError(RuntimeError):
    """A legal looking request still exceeds the bounded screening budget."""


class ScreenReplicaChangedError(ScreenReplicaUnavailableError):
    """The requested replica generation is no longer current."""


class ScreenReplicaDateError(ScreenReplicaDataError):
    """The selected date is not an open day in the bound replica calendar."""


@dataclass(frozen=True, slots=True)
class ScreenUniverseSnapshot:
    frame: pd.DataFrame
    identity: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ScreenDatesSnapshot:
    dates: list[date]
    identity: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class _VerifiedGeneration:
    replica: tuple[int, int, int, int, int]
    sidecar: tuple[int, int, int, int, int]
    sidecar_sha256: str
    identity: str
    updated_at: datetime


@dataclass(slots=True)
class _StoreConnection:
    _conn: duckdb.DuckDBPyConnection


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _regular_stat(path: Path) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as error:
        raise ScreenReplicaUnavailableError("read-only screening data is unavailable") from error
    if not stat.S_ISREG(observed.st_mode) or stat.S_ISLNK(observed.st_mode):
        raise ScreenReplicaUnavailableError("read-only screening data is unavailable")
    return observed


def _read_sidecar(path: Path) -> tuple[ReplicaGenerationMetadata, tuple[int, ...], str]:
    observed = _regular_stat(path)
    if observed.st_size < 1 or observed.st_size > MAX_SIDECAR_BYTES:
        raise ScreenReplicaUnavailableError("screening replica metadata is invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            if _identity(os.fstat(descriptor)) != _identity(observed):
                raise ScreenReplicaUnavailableError("screening replica metadata changed")
            payload = os.read(descriptor, MAX_SIDECAR_BYTES + 1)
            if (
                len(payload) != observed.st_size
                or _identity(os.fstat(descriptor)) != _identity(observed)
                or _identity(_regular_stat(path)) != _identity(observed)
            ):
                raise ScreenReplicaUnavailableError("screening replica metadata changed")
        finally:
            os.close(descriptor)
        metadata = ReplicaGenerationMetadata.model_validate_json(payload)
    except (OSError, ValueError) as error:
        raise ScreenReplicaUnavailableError("screening replica metadata is invalid") from error
    return metadata, _identity(observed), hashlib.sha256(payload).hexdigest()


class VerifiedReplicaScreenSource:
    """Open only a sidecar-bound replica and discard reads when its generation moves."""

    def __init__(self, *, primary_path: Path, replica_path: Path) -> None:
        self.primary_path = Path(primary_path)
        self.replica_path = Path(replica_path)
        if any(
            not path.is_absolute()
            or path != Path(os.path.abspath(path))
            for path in (self.primary_path, self.replica_path)
        ) or self.replica_path.parent.resolve(strict=False) != self.replica_path.parent:
            raise ValueError("screen database paths must be absolute and canonical")
        if self.primary_path == self.replica_path:
            raise ValueError("screen replica must differ from the primary database")

    def _verify(self) -> _VerifiedGeneration:
        replica_stat = _regular_stat(self.replica_path)
        if os.path.lexists(f"{self.replica_path}.wal"):
            raise ScreenReplicaUnavailableError("screening replica has an uncheckpointed WAL")

        metadata, sidecar_identity, sidecar_hash = _read_sidecar(
            replica_generation_path(self.replica_path)
        )
        observed = ReplicaFileWatermark(
            device=replica_stat.st_dev,
            inode=replica_stat.st_ino,
            size=replica_stat.st_size,
            mtime_ns=replica_stat.st_mtime_ns,
        )
        if (
            metadata.source_database != self.primary_path
            or metadata.source_before != metadata.source_after
            or (metadata.source_before.main.device, metadata.source_before.main.inode)
            == (replica_stat.st_dev, replica_stat.st_ino)
            or metadata.replica != observed
            or replica_stat.st_ctime_ns > sidecar_identity[4]
        ):
            raise ScreenReplicaUnavailableError("screening replica generation does not match")
        replica_identity = _identity(replica_stat)
        payload = json.dumps(
            {"replica": replica_identity, "sidecar_sha256": sidecar_hash},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
        return _VerifiedGeneration(
            replica=replica_identity,
            sidecar=sidecar_identity,
            sidecar_sha256=sidecar_hash,
            identity=hashlib.sha256(payload).hexdigest(),
            updated_at=datetime.fromtimestamp(replica_stat.st_mtime_ns / 1e9, tz=UTC),
        )

    def _open(self) -> tuple[duckdb.DuckDBPyConnection, int, _VerifiedGeneration]:
        before = self._verify()
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.replica_path, flags)
        except OSError as error:
            raise ScreenReplicaUnavailableError(
                "read-only screening data is unavailable"
            ) from error
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            if _identity(os.fstat(descriptor)) != before.replica:
                raise ScreenReplicaUnavailableError("screening replica changed while opening")
            try:
                connection, _branch = connect_pinned_readonly(self.replica_path, descriptor)
            except duckdb.Error as error:
                raise ScreenReplicaUnavailableError(
                    "read-only screening data is unavailable"
                ) from error
            if self._verify() != before or _identity(os.fstat(descriptor)) != before.replica:
                raise ScreenReplicaUnavailableError("screening replica changed while opening")
            return connection, descriptor, before
        except Exception:
            if connection is not None:
                with suppress(Exception):
                    connection.close()
            os.close(descriptor)
            raise

    def _finish(self, descriptor: int, before: _VerifiedGeneration) -> None:
        if _identity(os.fstat(descriptor)) != before.replica or self._verify() != before:
            raise ScreenReplicaUnavailableError("screening replica changed during the request")

    def available_dates(self, *, limit: int = 30) -> ScreenDatesSnapshot:
        if not 1 <= limit <= 30:
            raise ScreenReplicaBudgetError("screen date limit exceeds the allowed range")
        connection, descriptor, generation = self._open()
        try:
            rows = connection.execute(
                "SELECT DISTINCT daily.trade_date "
                "FROM daily_bar AS daily "
                "JOIN trade_calendar AS calendar "
                "ON calendar.exchange = 'SSE' AND calendar.cal_date = daily.trade_date "
                "AND calendar.is_open "
                "ORDER BY daily.trade_date DESC LIMIT ?",
                [limit],
            ).fetchall()
            self._finish(descriptor, generation)
            return ScreenDatesSnapshot(
                dates=[row[0] for row in rows],
                identity=generation.identity,
                updated_at=generation.updated_at,
            )
        except duckdb.Error as error:
            raise ScreenReplicaDataError("screening dates are unavailable") from error
        finally:
            connection.close()
            os.close(descriptor)

    def load(
        self,
        trade_date: date,
        rules: Sequence[Rule],
        *,
        decision_at: datetime | None = None,
        include_columns: Sequence[str] | None = None,
    ) -> ScreenUniverseSnapshot:
        if type(trade_date) is not date:
            raise ValueError("screen trade date must be a date")
        if len(rules) > MAX_CONDITIONS:
            raise ScreenReplicaBudgetError("screen has too many conditions")
        rule_columns = required_rule_columns(rules)
        requested_columns = rule_columns | frozenset(include_columns or ())
        dynamic_ma = requested_dynamic_ma(requested_columns)
        dynamic_days = dynamic_ma_day_count(dynamic_ma)
        _, wide_columns = _selected_sources(requested_columns, MAX_LOOKBACK)
        required_offset = max(
            (int(column.split("[")[1][:-1]) for column in wide_columns),
            default=0,
        )
        lookback = max(_infer_lookback(list(rules)), required_offset)
        aggregates = _collect_aggregates(list(rules))
        if (
            lookback < 0
            or lookback > MAX_LOOKBACK
            or len(aggregates) > MAX_CONDITIONS
            or any(req.window < 1 or req.window > MAX_AGGREGATE_WINDOW for req in aggregates)
        ):
            raise ScreenReplicaBudgetError("screen history exceeds the allowed range")

        connection, descriptor, generation = self._open()
        try:
            connection.execute("SET threads=1")
            row_count = int(
                connection.execute(
                    "SELECT COUNT(*) FROM daily_bar WHERE trade_date = ?", [trade_date]
                ).fetchone()[0]
            )
            if row_count == 0:
                self._finish(descriptor, generation)
                raise ScreenReplicaDataError("screening facts are unavailable")
            if row_count * (len(wide_columns) + 5) > MAX_WIDE_CELLS:
                raise ScreenReplicaBudgetError(
                    "screen needs too many historical columns; narrow conditions or ranking"
                )
            if dynamic_days and row_count * dynamic_days > MAX_DYNAMIC_MA_FACTS:
                raise ScreenReplicaBudgetError("dynamic MA history exceeds the allowed budget")
            if (
                row_count > MAX_STOCKS
                or row_count * sum(req.window for req in aggregates) > MAX_AGGREGATE_FACTS
            ):
                raise ScreenReplicaBudgetError("screen data exceeds the allowed budget")
            frame = load_universe(
                trade_date.isoformat(),
                lookback=lookback,
                store=cast("DuckDBStore", _StoreConnection(connection)),
                aggregate_requests=aggregates,
                decision_at=decision_at,
                required_columns=requested_columns,
            )
            frame.insert(0, "trade_date", trade_date)
            self._finish(descriptor, generation)
            return ScreenUniverseSnapshot(
                frame=frame,
                identity=generation.identity,
                updated_at=generation.updated_at,
            )
        except ScreeningCalendarError as error:
            raise ScreenReplicaDataError("screening calendar is incomplete") from error
        except ScreeningFactError as error:
            raise ScreenReplicaDataError("screening facts are ambiguous") from error
        except DynamicMaFactError as error:
            raise ScreenReplicaDataError("screening MA facts are incomplete") from error
        except duckdb.Error as error:
            raise ScreenReplicaDataError("screening facts are unavailable") from error
        finally:
            connection.close()
            os.close(descriptor)
