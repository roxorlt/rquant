"""Fixed-policy factor tracking and its independent research state authority."""

from __future__ import annotations

import os
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from rquant.factor.daily_stream import FactorDailyStreamDay
from rquant.factor.evaluate import (
    CorrelationResult,
    DailyFactorResult,
    FactorEvaluation,
    FiniteFloat,
)
from rquant.factor.portfolio import _compound
from rquant.factor.registry import (
    FactorDefinitionRegistry,
    FactorHeadRef,
    FactorRegistryIdentity,
    _canonical_model_json,
    _sha256,
)
from rquant.factor.result_artifact import _open_private_root
from rquant.factor.run_request import RUN_IMMUTABLE
from rquant.factor.summary import ICSeriesSummary, summarize_factor_ic
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.strict_json import strict_model_validate_canonical_json

MAX_TRACKED_FACTORS = 512
MAX_TRACKING_DAYS = 4096
TRACKING_POLICY_LABEL = "全市场（剔除北交所、ST） · 每日 · 5组 · RankIC · 无运行后中性化"
TRACKING_BASIS_LABEL = (
    "历史回顾研究诊断；行业算子采用独立API回顾归属；累计从实际起日计算，并非实盘收益。"
)


class FactorTrackingConflict(ValueError):  # noqa: N818
    """The original operation or current tracking generation does not match."""


class FactorTrackingIntegrityError(RuntimeError):
    """An existing tracking authority is incomplete or has changed identity."""


class FactorTrackingRequest(BaseModel):
    model_config = RUN_IMMUTABLE
    command_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
    requested_at: AwareUtcDatetime
    serving_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    factor_id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,63}$")
    tracked: bool
    expected_head: FactorHeadRef
    expected_tracking_generation: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")

    @field_validator("requested_at", mode="before")
    @classmethod
    def _time(cls, value: object) -> datetime:
        if type(value) is str:
            return datetime.fromisoformat(value)
        if not isinstance(value, datetime):
            raise ValueError("请求时刻格式不正确")
        return value


class FactorTrackingIdentity(BaseModel):
    model_config = RUN_IMMUTABLE
    instance_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    path: str
    st_dev: int = Field(ge=0)
    st_ino: int = Field(gt=0)


class FactorTrackingReceipt(BaseModel):
    model_config = RUN_IMMUTABLE
    command_id: str
    factor_id: str
    tracked: bool
    tracking_generation: str = Field(pattern=r"^[0-9a-f]{32}$")
    segment_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    definition_head: FactorHeadRef


class FactorTrackingOperationResult(BaseModel):
    model_config = RUN_IMMUTABLE
    original_request: FactorTrackingRequest
    status: Literal["applied", "pending", "uncertain", "rejected"]
    receipt: FactorTrackingReceipt | None = None
    reason: str | None = Field(default=None, max_length=120)

    @model_validator(mode="after")
    def _bound(self) -> FactorTrackingOperationResult:
        r = self.receipt
        if (self.status == "applied") != (r is not None):
            raise ValueError("tracking result differs from its receipt")
        if r is not None and (
            r.command_id != self.original_request.command_id
            or r.factor_id != self.original_request.factor_id
            or r.tracked != self.original_request.tracked
            or r.definition_head != self.original_request.expected_head
        ):
            raise ValueError("tracking receipt differs from the original intent")
        return self


class FactorTrackingDay(BaseModel):
    model_config = RUN_IMMUTABLE
    trade_date: date
    rank_ic: CorrelationResult
    status: Literal["complete", "partial", "no_samples"]
    expected_count: int = Field(ge=0, le=7000)
    valid_count: int = Field(ge=0, le=7000)
    low_return: FiniteFloat | None
    high_return: FiniteFloat | None

    @model_validator(mode="after")
    def _coverage(self) -> FactorTrackingDay:
        if (
            self.valid_count > self.expected_count
            or (self.status == "complete" and self.valid_count != self.expected_count)
            or (self.status == "no_samples" and self.valid_count != 0)
        ):
            raise ValueError("tracking coverage differs from daily facts")
        if (self.low_return is None) != (self.high_return is None):
            raise ValueError("tracking sleeves must be present together")
        return self

    @classmethod
    def from_stream_day(cls, day: FactorDailyStreamDay) -> FactorTrackingDay:
        day = FactorDailyStreamDay.model_validate(day)
        grouping = next(item for item in day.portfolio_groupings if item.group_count == 5)
        available = grouping.status == "ok" and len(grouping.groups) == 5
        return cls(
            trade_date=day.trade_date,
            rank_ic=day.evaluation.rank_ic,
            status=day.status,
            expected_count=day.coverage.expected_count,
            valid_count=day.coverage.valid_count,
            low_return=grouping.groups[0].period_return if available else None,
            high_return=grouping.groups[-1].period_return if available else None,
        )


class FactorTrackingSummary(BaseModel):
    model_config = RUN_IMMUTABLE
    latest_trade_date: date | None
    yesterday_ic: FiniteFloat | None
    ic_20: ICSeriesSummary
    complete_day_count: int = Field(ge=0, le=20)
    yesterday_long_short: FiniteFloat | None
    week_long_short: FiniteFloat | None
    week_day_count: int = Field(ge=0, le=5)
    week_complete_day_count: int = Field(ge=0, le=5)
    cumulative_long_short: FiniteFloat | None
    invalidated: bool
    reason: str | None = Field(default=None, max_length=120)

    @model_validator(mode="after")
    def _coverage(self) -> FactorTrackingSummary:
        count = self.ic_20.source_day_count
        if not self.complete_day_count <= count <= 20 or self.ic_20.valid_day_count > count:
            raise ValueError("tracking IC window coverage differs")
        expected = (
            count == self.complete_day_count == self.ic_20.valid_day_count == 20
            and self.ic_20.mean is not None
            and self.ic_20.mean <= 0.0
        )
        if self.invalidated != expected or self.week_complete_day_count > self.week_day_count:
            raise ValueError("tracking invalidation or week coverage differs")
        if self.week_long_short is not None and self.week_complete_day_count != 5:
            raise ValueError("tracking week return requires all five actual dates")
        return self


def _sleeve_spread(days: tuple[FactorTrackingDay, ...]) -> float | None:
    low = high = 1.0
    for day in days:
        if day.status != "complete" or day.low_return is None or day.high_return is None:
            return None
        low, high = _compound(low, day.low_return), _compound(high, day.high_return)
    return high - low


def summarize_factor_tracking(days: tuple[FactorTrackingDay, ...]) -> FactorTrackingSummary:
    if len(days) > MAX_TRACKING_DAYS or tuple(d.trade_date for d in days) != tuple(
        sorted(set(d.trade_date for d in days))
    ):
        raise ValueError("tracking dates must be bounded, unique and ascending")
    recent, week = days[-20:], days[-5:]
    summary = summarize_factor_ic(
        FactorEvaluation(
            days=tuple(
                DailyFactorResult(
                    decision_date=d.trade_date,
                    source_sample_count=d.valid_count,
                    effective_sample_count=d.valid_count,
                    normal_ic=d.rank_ic,
                    rank_ic=d.rank_ic,
                    groupings=(),
                )
                for d in recent
            )
        )
    ).rank_ic
    complete = sum(d.status == "complete" for d in recent)
    invalidated = (
        len(recent) == complete == summary.valid_day_count == 20
        and summary.mean is not None
        and summary.mean <= 0.0
    )
    latest = days[-1] if days else None
    return FactorTrackingSummary(
        latest_trade_date=None if latest is None else latest.trade_date,
        yesterday_ic=None if latest is None else latest.rank_ic.value,
        ic_20=summary,
        complete_day_count=complete,
        yesterday_long_short=None
        if latest is None or latest.low_return is None
        else latest.high_return - latest.low_return,
        week_long_short=_sleeve_spread(week) if len(week) == 5 else None,
        week_day_count=len(week),
        week_complete_day_count=sum(d.status == "complete" for d in week),
        cumulative_long_short=_sleeve_spread(days) if days else None,
        invalidated=invalidated,
        reason="近20个成熟交易日完整覆盖，方向对齐平均 RankIC 不大于零。"
        if invalidated
        else (
            "近20日覆盖不足，暂不判断失效。"
            if len(recent) < 20 or complete < 20 or summary.valid_day_count < 20
            else None
        ),
    )


class FactorTrackingState(BaseModel):
    model_config = RUN_IMMUTABLE
    factor_id: str
    generation: str = Field(pattern=r"^[0-9a-f]{32}$")
    segment_id: str | None
    tracked: bool
    status: Literal["not_tracked", "waiting", "active", "paused"]
    registry_identity: FactorRegistryIdentity
    head: FactorHeadRef
    joined_at: AwareUtcDatetime
    cursor: date | None = None
    start_date: date | None = None
    updated_at: AwareUtcDatetime | None = None
    reason: str | None = Field(default=None, max_length=120)

    @model_validator(mode="after")
    def _consistent(self) -> FactorTrackingState:
        if self.tracked != (self.segment_id is not None) or self.tracked == (
            self.status == "not_tracked"
        ):
            raise ValueError("tracking state is inconsistent")
        if self.segment_id is not None and self.segment_id != self.generation:
            raise ValueError("tracking segment differs from generation")
        if (self.start_date is None) != (self.cursor is None) or (
            self.start_date is not None and self.start_date > self.cursor
        ):
            raise ValueError("tracking cursor is inconsistent")
        return self


class FactorTrackingPanel(BaseModel):
    model_config = RUN_IMMUTABLE
    factor_id: str
    availability: Literal["unavailable", "not_tracked", "tracked"]
    status: Literal["unavailable", "not_tracked", "waiting", "active", "paused"]
    tracked: bool = False
    tracking_generation: str | None = None
    definition_head: FactorHeadRef | None = None
    actual_start_date: date | None = None
    updated_at: AwareUtcDatetime | None = None
    summary: FactorTrackingSummary | None = None
    reason: str | None = Field(default=None, max_length=120)
    can_set_tracked: bool = False
    policy_version: Literal[1] = 1
    policy_label: str = TRACKING_POLICY_LABEL
    basis_label: str = TRACKING_BASIS_LABEL

    @model_validator(mode="after")
    def _state_binding(self) -> FactorTrackingPanel:
        if self.tracked != (self.availability == "tracked"):
            raise ValueError("tracking panel flag differs from availability")
        if self.availability in ("unavailable", "not_tracked"):
            if (
                self.status != self.availability
                or self.summary is not None
                or self.actual_start_date is not None
            ):
                raise ValueError("untracked panel carries an active result")
        elif (
            self.status not in ("waiting", "active", "paused")
            or self.tracking_generation is None
            or self.definition_head is None
        ):
            raise ValueError("tracked panel lacks its frozen identity")
        if self.summary is not None and (
            self.actual_start_date is None
            or self.updated_at is None
            or self.summary.latest_trade_date is None
            or self.summary.latest_trade_date < self.actual_start_date
        ):
            raise ValueError("tracking summary lacks its actual period")
        if self.status == "active" and self.summary is None:
            raise ValueError("active tracking panel lacks a completed result")
        return self


class FactorTrackingStore:
    """Existing SQLite identities only; original commands are never reinterpreted."""

    def __init__(
        self, path: Path, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    ) -> None:
        self.path, self.clock = Path(path).absolute(), clock

    def _file(self) -> tuple[int, int]:
        try:
            info = os.stat(self.path, follow_symlinks=False)
        except OSError as error:
            raise FactorTrackingIntegrityError("tracking authority is unavailable") from error
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o077
        ):
            raise FactorTrackingIntegrityError("tracking authority must be a private regular file")
        return info.st_dev, info.st_ino

    def initialize(self) -> FactorTrackingIdentity:
        parent = _open_private_root(self.path.parent)
        try:
            descriptor = os.open(
                self.path.name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=parent,
            )
            os.close(descriptor)
            original = self._file()
            connection = sqlite3.connect(self.path, isolation_level=None)
            try:
                connection.executescript("""
                    BEGIN IMMEDIATE;
                    PRAGMA user_version = 1;
                    CREATE TABLE tracking_identity (
                        singleton INTEGER PRIMARY KEY, instance_id TEXT NOT NULL);
                    CREATE TABLE tracking_states (
                        factor_id TEXT PRIMARY KEY, payload TEXT NOT NULL, sha256 TEXT NOT NULL);
                    CREATE TABLE tracking_commands (
                        command_id TEXT PRIMARY KEY, actor TEXT NOT NULL,
                        request_sha256 TEXT NOT NULL, receipt TEXT NOT NULL, sha256 TEXT NOT NULL);
                    CREATE TABLE tracking_days (
                        segment_id TEXT NOT NULL, trade_date TEXT NOT NULL,
                        payload TEXT NOT NULL, sha256 TEXT NOT NULL,
                        PRIMARY KEY(segment_id, trade_date));
                    CREATE TABLE tracking_runs (
                        run_id TEXT PRIMARY KEY, segment_id TEXT NOT NULL,
                        payload TEXT NOT NULL, sha256 TEXT NOT NULL);
                """)
                connection.execute("INSERT INTO tracking_identity VALUES (1, ?)", (uuid4().hex,))
                if self._file() != original:
                    raise FactorTrackingIntegrityError("tracking authority changed during creation")
                connection.execute("COMMIT")
            finally:
                connection.close()
        finally:
            os.close(parent)
        return self.identity()

    @staticmethod
    def _schema(connection: sqlite3.Connection) -> str:
        expected = {
            "tracking_identity",
            "tracking_states",
            "tracking_commands",
            "tracking_days",
            "tracking_runs",
        }
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        if connection.execute("PRAGMA user_version").fetchone()[0] != 1 or tables != expected:
            raise FactorTrackingIntegrityError("tracking schema is incomplete")
        rows = connection.execute("SELECT singleton, instance_id FROM tracking_identity").fetchall()
        if len(rows) != 1 or rows[0][0] != 1:
            raise FactorTrackingIntegrityError("tracking instance is invalid")
        value = rows[0][1]
        # A typed identity validates its format in the caller.
        return value

    def identity(self) -> FactorTrackingIdentity:
        device, inode = self._file()
        connection = sqlite3.connect(
            f"{self.path.as_uri()}?mode=ro", uri=True, isolation_level=None
        )
        try:
            instance = self._schema(connection)
        except sqlite3.DatabaseError as error:
            raise FactorTrackingIntegrityError("tracking authority cannot be read") from error
        finally:
            connection.close()
        if self._file() != (device, inode):
            raise FactorTrackingIntegrityError("tracking authority changed")
        return FactorTrackingIdentity(
            instance_id=instance, path=str(self.path), st_dev=device, st_ino=inode
        )

    def _check(self, connection: sqlite3.Connection, expected: FactorTrackingIdentity) -> None:
        if (
            expected.path != str(self.path)
            or self._file() != (expected.st_dev, expected.st_ino)
            or self._schema(connection) != expected.instance_id
        ):
            raise FactorTrackingIntegrityError("tracking authority identity changed")

    @contextmanager
    def _connection(
        self, expected: FactorTrackingIdentity, *, write: bool = False
    ) -> Iterator[sqlite3.Connection]:
        if expected.path != str(self.path) or self._file() != (expected.st_dev, expected.st_ino):
            raise FactorTrackingIntegrityError("tracking authority identity changed")
        connection = sqlite3.connect(
            f"{self.path.as_uri()}?mode={'rw' if write else 'ro'}",
            uri=True,
            timeout=10,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            self._check(connection, expected)
            yield connection
            self._check(connection, expected)
            connection.execute("COMMIT")
        except sqlite3.DatabaseError as error:
            raise FactorTrackingIntegrityError(
                "tracking authority cannot be read or committed"
            ) from error
        finally:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            connection.close()

    @staticmethod
    def _decode(model: type[BaseModel], payload: str, digest: str) -> BaseModel:
        try:
            if _sha256(payload) != digest:
                raise ValueError("digest differs")
            return strict_model_validate_canonical_json(model, payload)
        except (TypeError, ValueError) as error:
            raise FactorTrackingIntegrityError("tracking row is invalid") from error

    def _state(self, connection: sqlite3.Connection, factor_id: str) -> FactorTrackingState | None:
        row = connection.execute(
            "SELECT * FROM tracking_states WHERE factor_id=?", (factor_id,)
        ).fetchone()
        if row is None:
            return None
        state = self._decode(FactorTrackingState, row["payload"], row["sha256"])
        if state.factor_id != factor_id:
            raise FactorTrackingIntegrityError("tracking row identity differs")
        return state

    def get(
        self, factor_id: str, *, expected_identity: FactorTrackingIdentity
    ) -> FactorTrackingState | None:
        with self._connection(expected_identity) as connection:
            return self._state(connection, factor_id)

    def list_states(
        self, *, expected_identity: FactorTrackingIdentity
    ) -> tuple[FactorTrackingState, ...]:
        with self._connection(expected_identity) as connection:
            ids = connection.execute(
                "SELECT factor_id FROM tracking_states ORDER BY factor_id LIMIT ?",
                (MAX_TRACKED_FACTORS + 1,),
            ).fetchall()
            if len(ids) > MAX_TRACKED_FACTORS:
                raise FactorTrackingIntegrityError("tracking collection exceeds its capacity")
            return tuple(self._state(connection, row[0]) for row in ids)

    def days(
        self, factor_id: str, *, expected_identity: FactorTrackingIdentity
    ) -> tuple[FactorTrackingDay, ...]:
        from rquant.factor.tracking_serving import _days

        with self._connection(expected_identity) as connection:
            state = self._state(connection, factor_id)
            return () if state is None else _days(self, connection, state)

    def _lookup(
        self, connection: sqlite3.Connection, request: FactorTrackingRequest, actor_id: str
    ) -> FactorTrackingReceipt | None:
        row = connection.execute(
            "SELECT * FROM tracking_commands WHERE command_id=?", (request.command_id,)
        ).fetchone()
        if row is None:
            return None
        if row["actor"] != actor_id or row["request_sha256"] != _sha256(
            _canonical_model_json(request)
        ):
            raise FactorTrackingConflict("original tracking command differs")
        receipt = self._decode(FactorTrackingReceipt, row["receipt"], row["sha256"])
        FactorTrackingOperationResult(original_request=request, status="applied", receipt=receipt)
        return receipt

    def lookup(
        self,
        request: FactorTrackingRequest,
        *,
        actor_id: str,
        expected_identity: FactorTrackingIdentity,
    ) -> FactorTrackingReceipt | None:
        request = FactorTrackingRequest.model_validate(request)
        with self._connection(expected_identity) as connection:
            return self._lookup(connection, request, actor_id)

    def _save_state(self, connection: sqlite3.Connection, state: FactorTrackingState) -> None:
        payload = _canonical_model_json(FactorTrackingState.model_validate(state))
        connection.execute(
            "INSERT INTO tracking_states VALUES (?, ?, ?) ON CONFLICT(factor_id) "
            "DO UPDATE SET payload=excluded.payload, sha256=excluded.sha256",
            (state.factor_id, payload, _sha256(payload)),
        )

    def set_tracked(
        self,
        request: FactorTrackingRequest,
        *,
        actor_id: str,
        expected_identity: FactorTrackingIdentity,
        registry_identity: FactorRegistryIdentity,
    ) -> FactorTrackingReceipt:
        request = FactorTrackingRequest.model_validate(request)
        # Recovery precedes live head validation; the accepted command retains its original effect.
        original = self.lookup(request, actor_id=actor_id, expected_identity=expected_identity)
        if original is not None:
            return original
        registry = FactorDefinitionRegistry(Path(registry_identity.path))
        # The registry transaction orders head changes against this tracking commit.
        with registry._reader(registry_identity) as definitions:
            head, _ = registry._load_factor(definitions, request.factor_id)
            if (
                head is None
                or FactorHeadRef(version=head.version, content_sha256=head.content_sha256)
                != request.expected_head
                or (request.tracked and head.archived)
            ):
                raise FactorTrackingConflict("definition head changed or is archived")
            with self._connection(expected_identity, write=True) as connection:
                original = self._lookup(connection, request, actor_id)
                if original is not None:
                    return original
                previous = self._state(connection, request.factor_id)
                if request.expected_tracking_generation != (
                    None if previous is None else previous.generation
                ):
                    raise FactorTrackingConflict("tracking generation changed")
                if (
                    previous is None
                    and connection.execute("SELECT COUNT(*) FROM tracking_states").fetchone()[0]
                    >= MAX_TRACKED_FACTORS
                ):
                    raise FactorTrackingConflict("tracking collection exceeds its capacity")
                keep = (
                    previous is not None
                    and previous.tracked == request.tracked
                    and previous.head == request.expected_head
                    and previous.status != "paused"
                )
                generation = previous.generation if keep else uuid4().hex
                state = (
                    previous
                    if keep
                    else FactorTrackingState(
                        factor_id=request.factor_id,
                        generation=generation,
                        segment_id=generation if request.tracked else None,
                        tracked=request.tracked,
                        status="waiting" if request.tracked else "not_tracked",
                        registry_identity=registry_identity,
                        head=request.expected_head,
                        joined_at=self.clock(),
                        reason="等待可核验的成熟数据。" if request.tracked else None,
                    )
                )
                self._save_state(connection, state)
                receipt = FactorTrackingReceipt(
                    command_id=request.command_id,
                    factor_id=request.factor_id,
                    tracked=request.tracked,
                    tracking_generation=generation,
                    segment_id=state.segment_id,
                    definition_head=request.expected_head,
                )
                payload = _canonical_model_json(receipt)
                connection.execute(
                    "INSERT INTO tracking_commands VALUES (?, ?, ?, ?, ?)",
                    (
                        request.command_id,
                        actor_id,
                        _sha256(_canonical_model_json(request)),
                        payload,
                        _sha256(payload),
                    ),
                )
                registry._require_expected_identity(definitions, registry_identity)
                return receipt
