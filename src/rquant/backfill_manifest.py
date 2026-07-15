"""Typed, reproducible strategy eligibility and minute-backfill manifests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Literal, Protocol
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from rquant.data_quality import (
    DEFAULT_MINUTE_SOURCE_SESSION_SPECS,
    MinuteSourceSessionSpec,
)

EligibilityBasis = Literal["daily", "daily+auction"]
BackfillPhase = Literal["baseline", "entry", "exit"]
EstimateConfidence = Literal["low", "medium", "high"]
SHANGHAI = ZoneInfo("Asia/Shanghai")

if TYPE_CHECKING:
    from rquant.backfill_state import BackfillManifestInput
    from rquant.screen.rules import Rule
    from rquant.storage.duckdb import DuckDBStore


class ScreenRunner(Protocol):
    def __call__(
        self,
        trade_date: str,
        rules: list[Rule],
        *,
        include_columns: list[str] | None = None,
        store: DuckDBStore | None = None,
        ts_code_whitelist: list[str] | None = None,
    ) -> pd.DataFrame: ...


def _canonical_hash(payload: object) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda value: value.isoformat(),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


class StrategyWindowRequirement(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    baseline_trading_days: int = Field(default=90, ge=0)
    entry_trading_days: int = Field(default=1, ge=1)
    exit_trading_days: int = Field(default=10, ge=1, le=10)


class StrategyBackfillSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    eligibility_basis: EligibilityBasis
    minute_frequency: Literal["1min"] = "1min"
    window: StrategyWindowRequirement = Field(
        default_factory=StrategyWindowRequirement
    )


class EligibilityRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    eligibility_id: str = ""
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    ts_code: str = Field(min_length=1)
    eligibility_date: date
    entry_date: date
    decision_at: datetime
    variant: str = Field(min_length=1)

    @field_validator("decision_at")
    @classmethod
    def normalize_decision_at(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="decision_at")

    @model_validator(mode="after")
    def derive_stable_id(self) -> EligibilityRecord:
        if self.entry_date < self.eligibility_date:
            raise ValueError("entry_date must not be before eligibility_date")
        expected = _canonical_hash(
            {
                "strategy_id": self.strategy_id,
                "strategy_version": self.strategy_version,
                "ts_code": self.ts_code,
                "eligibility_date": self.eligibility_date,
                "entry_date": self.entry_date,
                "decision_at": self.decision_at,
                "variant": self.variant,
            }
        )
        if self.eligibility_id and self.eligibility_id != expected:
            raise ValueError("eligibility_id does not match record content")
        object.__setattr__(self, "eligibility_id", expected)
        return self


class BackfillManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_id: str = ""
    spec: StrategyBackfillSpec
    start_date: date
    end_date: date
    as_of_time: datetime
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    eligibilities: tuple[EligibilityRecord, ...]

    @field_validator("as_of_time")
    @classmethod
    def normalize_as_of_time(cls, value: datetime) -> datetime:
        return _aware_utc(value, field_name="as_of_time")

    @model_validator(mode="after")
    def validate_and_derive_id(self) -> BackfillManifest:
        if self.start_date > self.end_date:
            raise ValueError("manifest start_date must not be after end_date")
        ordered = tuple(
            sorted(
                {row.eligibility_id: row for row in self.eligibilities}.values(),
                key=lambda row: (
                    row.entry_date,
                    row.ts_code,
                    row.variant,
                    row.eligibility_id,
                ),
            )
        )
        for row in ordered:
            if (
                row.strategy_id != self.spec.strategy_id
                or row.strategy_version != self.spec.strategy_version
            ):
                raise ValueError("eligibility strategy does not match manifest spec")
            if not self.start_date <= row.eligibility_date <= self.end_date:
                raise ValueError("eligibility date is outside manifest range")
        object.__setattr__(self, "eligibilities", ordered)
        expected = _canonical_hash(
            {
                "spec": self.spec.model_dump(mode="json"),
                "start_date": self.start_date,
                "end_date": self.end_date,
                "as_of_time": self.as_of_time,
                "code_commit": self.code_commit,
                "eligibility_ids": [row.eligibility_id for row in ordered],
            }
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("manifest_id does not match manifest content")
        object.__setattr__(self, "manifest_id", expected)
        return self

    @classmethod
    def build(
        cls,
        *,
        spec: StrategyBackfillSpec,
        start_date: date,
        end_date: date,
        as_of_time: datetime,
        code_commit: str,
        eligibilities: Iterable[EligibilityRecord],
    ) -> BackfillManifest:
        return cls(
            spec=spec,
            start_date=start_date,
            end_date=end_date,
            as_of_time=as_of_time,
            code_commit=code_commit,
            eligibilities=tuple(eligibilities),
        )


class BackfillCalendarError(RuntimeError):
    """The authoritative calendar cannot support an exact backfill window."""


class MergedBackfillWindow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ts_code: str = Field(min_length=1)
    start_date: date
    end_date: date
    open_dates: tuple[date, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_dates(self) -> MergedBackfillWindow:
        if self.start_date != self.open_dates[0] or self.end_date != self.open_dates[-1]:
            raise ValueError("window boundaries must match open_dates")
        if tuple(sorted(set(self.open_dates))) != self.open_dates:
            raise ValueError("window open_dates must be unique and ordered")
        return self


class MinuteBackfillTask(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    ts_code: str = Field(min_length=1)
    source: str = Field(min_length=1)
    freq: str = Field(min_length=1)
    start_date: date
    end_date: date
    open_dates: tuple[date, ...] = Field(min_length=1)
    expected_rows: int = Field(gt=0)
    response_row_limit: int = Field(gt=0)
    possible_truncation: bool

    @model_validator(mode="after")
    def validate_dates_and_rows(self) -> MinuteBackfillTask:
        if self.start_date != self.open_dates[0] or self.end_date != self.open_dates[-1]:
            raise ValueError("task boundaries must match open_dates")
        if self.expected_rows > self.response_row_limit:
            raise ValueError("task expected rows exceed response row limit")
        if self.possible_truncation != (self.expected_rows == self.response_row_limit):
            raise ValueError("possible_truncation must mark an exact response limit")
        return self


class BackfillPhaseCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_sessions: int = Field(ge=0)
    complete_sessions: int = Field(ge=0)
    accepted_missing_sessions: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_counts(self) -> BackfillPhaseCoverage:
        if self.satisfied_sessions > self.expected_sessions:
            raise ValueError("satisfied sessions cannot exceed expected sessions")
        return self

    @computed_field
    @property
    def satisfied_sessions(self) -> int:
        return self.complete_sessions + self.accepted_missing_sessions

    @computed_field
    @property
    def coverage_ratio(self) -> float:
        if self.expected_sessions == 0:
            return 0.0
        return self.satisfied_sessions / self.expected_sessions


class BackfillCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    baseline: BackfillPhaseCoverage
    entry: BackfillPhaseCoverage
    exit: BackfillPhaseCoverage
    expected_unique_sessions: int = Field(ge=0)
    complete_unique_sessions: int = Field(ge=0)
    accepted_missing_unique_sessions: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_unique_counts(self) -> BackfillCoverage:
        if (
            self.complete_unique_sessions + self.accepted_missing_unique_sessions
            > self.expected_unique_sessions
        ):
            raise ValueError("satisfied unique sessions cannot exceed expected sessions")
        return self

    @computed_field
    @property
    def entry_exit_coverage_ratio(self) -> float:
        expected = self.entry.expected_sessions + self.exit.expected_sessions
        if expected == 0:
            return 0.0
        satisfied = self.entry.satisfied_sessions + self.exit.satisfied_sessions
        return satisfied / expected

    @computed_field
    @property
    def baseline_gate_passed(self) -> bool:
        return self.baseline.coverage_ratio >= 0.95

    @computed_field
    @property
    def entry_exit_gate_passed(self) -> bool:
        return self.entry_gate_passed and self.exit_gate_passed

    @computed_field
    @property
    def entry_gate_passed(self) -> bool:
        return self.entry.coverage_ratio >= 0.99

    @computed_field
    @property
    def exit_gate_passed(self) -> bool:
        return self.exit.coverage_ratio >= 0.99


class BackfillEstimate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_count: int = Field(ge=0)
    estimated_rows: int = Field(ge=0)
    estimated_disk_bytes: int = Field(ge=0)
    rate_limit_seconds: float = Field(ge=0)
    transfer_seconds: float = Field(ge=0)
    write_seconds: float = Field(ge=0)
    total_seconds: float = Field(ge=0)
    confidence: EstimateConfidence
    confidence_reasons: tuple[str, ...] = Field(min_length=1)


class MinuteBackfillPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest: BackfillManifest
    windows: tuple[MergedBackfillWindow, ...]
    tasks: tuple[MinuteBackfillTask, ...]
    coverage: BackfillCoverage
    requested_session_count: int = Field(ge=0)
    estimate: BackfillEstimate


_DEFAULT_WINDOW = StrategyWindowRequirement()

STRATEGY_BACKFILL_SPECS: dict[str, StrategyBackfillSpec] = {
    "auction_gap": StrategyBackfillSpec(
        strategy_id="auction_gap",
        strategy_version="v1",
        eligibility_basis="daily+auction",
        window=_DEFAULT_WINDOW,
    ),
    "growth_board_surge": StrategyBackfillSpec(
        strategy_id="growth_board_surge",
        strategy_version="v1",
        eligibility_basis="daily",
        window=_DEFAULT_WINDOW,
    ),
    "n_shape": StrategyBackfillSpec(
        strategy_id="n_shape",
        strategy_version="v1",
        eligibility_basis="daily",
        window=_DEFAULT_WINDOW,
    ),
}


def _as_date(value: object) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise ValueError(f"cannot convert value to date: {value!r}")


def _authoritative_open_dates(store: DuckDBStore) -> list[date]:
    rows = store._conn.execute(
        """
        SELECT cal_date
        FROM trade_calendar
        WHERE exchange = 'SSE' AND is_open = TRUE
        ORDER BY cal_date
        """
    ).fetchall()
    return [_as_date(row[0]) for row in rows]


def _target_open_dates(
    calendar: list[date],
    start_date: date,
    end_date: date,
) -> list[date]:
    if start_date > end_date:
        raise ValueError("eligibility start_date must not be after end_date")
    return [day for day in calendar if start_date <= day <= end_date]


def resolve_n_shape_eligibility(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
    screen_runner: ScreenRunner | None = None,
) -> tuple[EligibilityRecord, ...]:
    """Rebuild Pool1/Pool2 from rules without reading historical screen results."""
    from rquant.presets import PRESET_SCREENS
    from rquant.screen.core import screen

    runner = screen_runner or screen
    spec = STRATEGY_BACKFILL_SPECS["n_shape"]
    calendar = _authoritative_open_dates(store)
    targets = _target_open_dates(calendar, start_date, end_date)
    if not targets:
        return ()

    index_by_date = {day: index for index, day in enumerate(calendar)}
    first_index = index_by_date[targets[0]]
    if first_index < 2:
        raise ValueError("n_shape eligibility requires two prior open sessions")
    last_index = index_by_date[targets[-1]]
    if last_index + 1 >= len(calendar):
        raise ValueError("n_shape eligibility requires the next open session")

    pool1 = PRESET_SCREENS["n-shape-pool1"]
    pool2 = PRESET_SCREENS["n-shape-pool2"]
    pool1_hits: dict[date, tuple[str, ...]] = {}
    for trading_date in calendar[first_index - 2 : last_index + 1]:
        frame = runner(
            trading_date.isoformat(),
            pool1.rules,
            include_columns=pool1.include_columns or None,
            store=store,
        )
        pool1_hits[trading_date] = tuple(
            sorted(str(code) for code in frame.get("ts_code", pd.Series(dtype=str)))
        )

    records: list[EligibilityRecord] = []
    for trading_date in targets:
        entry_date = calendar[index_by_date[trading_date] + 1]
        decision_at = datetime.combine(
            trading_date,
            time(17, 0),
            tzinfo=SHANGHAI,
        )
        for code in pool1_hits[trading_date]:
            records.append(
                EligibilityRecord(
                    strategy_id=spec.strategy_id,
                    strategy_version=spec.strategy_version,
                    ts_code=code,
                    eligibility_date=trading_date,
                    entry_date=entry_date,
                    decision_at=decision_at,
                    variant="pool1",
                )
            )

        current_index = index_by_date[trading_date]
        parent_codes = sorted(
            {
                code
                for parent_date in calendar[current_index - 2 : current_index]
                for code in pool1_hits.get(parent_date, ())
            }
        )
        if not parent_codes:
            continue
        pool2_frame = runner(
            trading_date.isoformat(),
            pool2.rules,
            include_columns=pool2.include_columns or None,
            store=store,
            ts_code_whitelist=parent_codes,
        )
        for code in sorted(
            str(value)
            for value in pool2_frame.get("ts_code", pd.Series(dtype=str))
        ):
            records.append(
                EligibilityRecord(
                    strategy_id=spec.strategy_id,
                    strategy_version=spec.strategy_version,
                    ts_code=code,
                    eligibility_date=trading_date,
                    entry_date=entry_date,
                    decision_at=decision_at,
                    variant="pool2",
                )
            )
    return tuple(sorted(records, key=lambda row: row.eligibility_id))


def resolve_growth_board_eligibility(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
) -> tuple[EligibilityRecord, ...]:
    """Resolve the broad daily growth-board universe before any minute query."""
    import rquant.growth_board_surge_strategy as growth

    spec = STRATEGY_BACKFILL_SPECS["growth_board_surge"]
    config = growth.GrowthBoardSurgeConfig()
    calendar = _authoritative_open_dates(store)
    targets = _target_open_dates(calendar, start_date, end_date)
    index_by_date = {day: index for index, day in enumerate(calendar)}
    records: list[EligibilityRecord] = []
    for trading_date in targets:
        current_index = index_by_date[trading_date]
        if current_index == 0:
            raise ValueError("growth eligibility requires a prior open session")
        previous_date = calendar[current_index - 1]
        candidates = growth.resolve_growth_board_candidates(
            store,
            trading_date,
            previous_date,
            config.min_signal_time,
        )
        for candidate in candidates:
            records.append(
                EligibilityRecord(
                    strategy_id=spec.strategy_id,
                    strategy_version=spec.strategy_version,
                    ts_code=candidate.ts_code,
                    eligibility_date=trading_date,
                    entry_date=trading_date,
                    decision_at=datetime.combine(
                        trading_date,
                        config.min_signal_time,
                        tzinfo=SHANGHAI,
                    ),
                    variant=candidate.board_type,
                )
            )
    return tuple(sorted(records, key=lambda row: row.eligibility_id))


def resolve_auction_gap_eligibility(
    store: DuckDBStore,
    *,
    start_date: date,
    end_date: date,
) -> tuple[EligibilityRecord, ...]:
    """Resolve daily+auction candidates without consulting minute coverage."""
    import rquant.auction_gap_strategy as auction

    spec = STRATEGY_BACKFILL_SPECS["auction_gap"]
    config = auction.AuctionGapConfig(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        min_auction_vol_ratio_5d=0.0,
        max_auction_vol_ratio_5d=float("inf"),
        st_filter="none",
        require_next_day=False,
    )
    candidates = auction.run_auction_gap_replay(store, config)
    records = [
        EligibilityRecord(
            strategy_id=spec.strategy_id,
            strategy_version=spec.strategy_version,
            ts_code=str(row["ts_code"]),
            eligibility_date=_as_date(row["signal_date"]),
            entry_date=_as_date(row["signal_date"]),
            decision_at=datetime.combine(
                _as_date(row["signal_date"]),
                config.decision_time,
                tzinfo=SHANGHAI,
            ),
            variant="auction_gap",
        )
        for _, row in candidates.iterrows()
    ]
    return tuple(sorted(records, key=lambda row: row.eligibility_id))


def resolve_strategy_eligibility(
    store: DuckDBStore,
    *,
    strategy_id: str,
    start_date: date,
    end_date: date,
) -> tuple[EligibilityRecord, ...]:
    if strategy_id == "n_shape":
        return resolve_n_shape_eligibility(
            store,
            start_date=start_date,
            end_date=end_date,
        )
    if strategy_id == "growth_board_surge":
        return resolve_growth_board_eligibility(
            store,
            start_date=start_date,
            end_date=end_date,
        )
    if strategy_id == "auction_gap":
        return resolve_auction_gap_eligibility(
            store,
            start_date=start_date,
            end_date=end_date,
        )
    raise KeyError(f"unknown strategy backfill spec: {strategy_id}")


def _authoritative_calendar_facts(
    store: DuckDBStore,
) -> tuple[list[date], set[date]]:
    rows = store._conn.execute(
        """
        SELECT cal_date, is_open
        FROM trade_calendar
        WHERE exchange = 'SSE'
        ORDER BY cal_date
        """
    ).fetchall()
    if not rows:
        raise BackfillCalendarError("authoritative SSE trade calendar is empty")
    civil_dates = {_as_date(row[0]) for row in rows}
    open_dates = [_as_date(row[0]) for row in rows if bool(row[1])]
    if not open_dates:
        raise BackfillCalendarError("authoritative SSE trade calendar has no open sessions")
    return open_dates, civil_dates


def _calendar_scope_demands(
    manifest: BackfillManifest,
    open_dates: list[date],
    civil_dates: set[date],
) -> list[tuple[str, str, BackfillPhase, date]]:
    index_by_date = {value: index for index, value in enumerate(open_dates)}
    demands: list[tuple[str, str, BackfillPhase, date]] = []
    required_dates: list[date] = []
    window = manifest.spec.window
    for eligibility in manifest.eligibilities:
        entry_index = index_by_date.get(eligibility.entry_date)
        if entry_index is None:
            raise BackfillCalendarError(
                f"entry date is not an authoritative open session: "
                f"{eligibility.entry_date.isoformat()}"
            )
        baseline_start = entry_index - window.baseline_trading_days
        if baseline_start < 0:
            raise BackfillCalendarError(
                f"eligibility {eligibility.eligibility_id} lacks "
                f"{window.baseline_trading_days} prior open sessions"
            )
        entry_end = entry_index + window.entry_trading_days - 1
        exit_end = entry_end + window.exit_trading_days
        if exit_end >= len(open_dates):
            raise BackfillCalendarError(
                f"eligibility {eligibility.eligibility_id} lacks "
                f"{window.exit_trading_days} following open sessions"
            )
        phase_dates: dict[BackfillPhase, list[date]] = {
            "baseline": open_dates[baseline_start:entry_index],
            "entry": open_dates[entry_index : entry_end + 1],
            "exit": open_dates[entry_end + 1 : exit_end + 1],
        }
        for phase, dates in phase_dates.items():
            for trading_date in dates:
                demands.append(
                    (
                        eligibility.eligibility_id,
                        eligibility.ts_code,
                        phase,
                        trading_date,
                    )
                )
                required_dates.append(trading_date)

    if not required_dates:
        return demands
    current = min(required_dates)
    required_end = max(required_dates)
    missing: list[date] = []
    while current <= required_end:
        if current not in civil_dates:
            missing.append(current)
        current += timedelta(days=1)
    if missing:
        rendered = ", ".join(value.isoformat() for value in missing[:5])
        raise BackfillCalendarError(f"authoritative trade calendar gap: {rendered}")
    return demands


def _merge_windows(
    demands: list[tuple[str, str, BackfillPhase, date]],
    open_dates: list[date],
) -> tuple[MergedBackfillWindow, ...]:
    index_by_date = {value: index for index, value in enumerate(open_dates)}
    dates_by_code: dict[str, set[date]] = {}
    for _eligibility_id, ts_code, _phase, trading_date in demands:
        dates_by_code.setdefault(ts_code, set()).add(trading_date)

    windows: list[MergedBackfillWindow] = []
    for ts_code, date_values in sorted(dates_by_code.items()):
        ordered = sorted(date_values, key=index_by_date.__getitem__)
        if not ordered:
            continue
        group = [ordered[0]]
        for trading_date in ordered[1:]:
            if index_by_date[trading_date] == index_by_date[group[-1]] + 1:
                group.append(trading_date)
                continue
            windows.append(
                MergedBackfillWindow(
                    ts_code=ts_code,
                    start_date=group[0],
                    end_date=group[-1],
                    open_dates=tuple(group),
                )
            )
            group = [trading_date]
        windows.append(
            MergedBackfillWindow(
                ts_code=ts_code,
                start_date=group[0],
                end_date=group[-1],
                open_dates=tuple(group),
            )
        )
    return tuple(windows)


def _complete_minute_sessions(
    store: DuckDBStore,
    windows: tuple[MergedBackfillWindow, ...],
    session_spec: MinuteSourceSessionSpec,
) -> set[tuple[str, date]]:
    if not windows:
        return set()
    codes = tuple(sorted({window.ts_code for window in windows}))
    start_date = min(window.start_date for window in windows)
    end_date = max(window.end_date for window in windows)
    placeholders = ", ".join("?" for _ in codes)
    rows = store._conn.execute(
        f"""
        SELECT ts_code,
               CAST(trade_time AS DATE) AS trade_date,
               list(
                   DISTINCT strftime(trade_time, '%H:%M:%S')
                   ORDER BY strftime(trade_time, '%H:%M:%S')
               ) AS actual_times
        FROM minute_bar
        WHERE source = ?
          AND freq = ?
          AND CAST(trade_time AS DATE) >= ?
          AND CAST(trade_time AS DATE) <= ?
          AND ts_code IN ({placeholders})
        GROUP BY ts_code, CAST(trade_time AS DATE)
        """,
        [
            session_spec.source,
            session_spec.freq,
            start_date,
            end_date,
            *codes,
        ],
    ).fetchall()
    expected = tuple(
        value.isoformat(timespec="seconds")
        for value in session_spec.expected_times()
    )
    desired = {
        (window.ts_code, trading_date)
        for window in windows
        for trading_date in window.open_dates
    }
    return {
        (str(ts_code), _as_date(trading_date))
        for ts_code, trading_date, actual_times in rows
        if tuple(actual_times) == expected
        and (str(ts_code), _as_date(trading_date)) in desired
    }


def _coverage_from_demands(
    demands: list[tuple[str, str, BackfillPhase, date]],
    complete: set[tuple[str, date]],
    accepted_missing: set[tuple[str, date]],
) -> BackfillCoverage:
    expected_counts = {phase: 0 for phase in ("baseline", "entry", "exit")}
    complete_counts = {phase: 0 for phase in ("baseline", "entry", "exit")}
    accepted_counts = {phase: 0 for phase in ("baseline", "entry", "exit")}
    unique_sessions: set[tuple[str, date]] = set()
    for _eligibility_id, ts_code, phase, trading_date in demands:
        expected_counts[phase] += 1
        unique_sessions.add((ts_code, trading_date))
        if (ts_code, trading_date) in complete:
            complete_counts[phase] += 1
        elif (ts_code, trading_date) in accepted_missing:
            accepted_counts[phase] += 1
    return BackfillCoverage(
        baseline=BackfillPhaseCoverage(
            expected_sessions=expected_counts["baseline"],
            complete_sessions=complete_counts["baseline"],
            accepted_missing_sessions=accepted_counts["baseline"],
        ),
        entry=BackfillPhaseCoverage(
            expected_sessions=expected_counts["entry"],
            complete_sessions=complete_counts["entry"],
            accepted_missing_sessions=accepted_counts["entry"],
        ),
        exit=BackfillPhaseCoverage(
            expected_sessions=expected_counts["exit"],
            complete_sessions=complete_counts["exit"],
            accepted_missing_sessions=accepted_counts["exit"],
        ),
        expected_unique_sessions=len(unique_sessions),
        complete_unique_sessions=len(unique_sessions & complete),
        accepted_missing_unique_sessions=len(unique_sessions & accepted_missing),
    )


def _missing_task_chunks(
    windows: tuple[MergedBackfillWindow, ...],
    complete: set[tuple[str, date]],
    accepted_missing: set[tuple[str, date]],
    open_dates: list[date],
    session_spec: MinuteSourceSessionSpec,
    response_row_limit: int,
) -> tuple[MinuteBackfillTask, ...]:
    rows_per_session = len(session_spec.expected_times())
    max_sessions = response_row_limit // rows_per_session
    if max_sessions < 1:
        raise ValueError("response row limit cannot fit one complete minute session")
    index_by_date = {value: index for index, value in enumerate(open_dates)}
    tasks: list[MinuteBackfillTask] = []
    for window in windows:
        missing = [
            value
            for value in window.open_dates
            if (window.ts_code, value) not in complete
            and (window.ts_code, value) not in accepted_missing
        ]
        groups: list[list[date]] = []
        for trading_date in missing:
            if (
                not groups
                or index_by_date[trading_date] != index_by_date[groups[-1][-1]] + 1
            ):
                groups.append([trading_date])
            else:
                groups[-1].append(trading_date)
        for group in groups:
            for offset in range(0, len(group), max_sessions):
                chunk = tuple(group[offset : offset + max_sessions])
                expected_rows = len(chunk) * rows_per_session
                task_id = _canonical_hash(
                    {
                        "manifest_source": session_spec.source,
                        "freq": session_spec.freq,
                        "ts_code": window.ts_code,
                        "open_dates": chunk,
                    }
                )
                tasks.append(
                    MinuteBackfillTask(
                        task_id=task_id,
                        ts_code=window.ts_code,
                        source=session_spec.source,
                        freq=session_spec.freq,
                        start_date=chunk[0],
                        end_date=chunk[-1],
                        open_dates=chunk,
                        expected_rows=expected_rows,
                        response_row_limit=response_row_limit,
                        possible_truncation=expected_rows == response_row_limit,
                    )
                )
    return tuple(tasks)


def _backfill_estimate(
    tasks: tuple[MinuteBackfillTask, ...],
    *,
    requests_per_minute: int,
    observed_request_seconds: float | None,
    observed_bytes_per_row: float | None,
) -> BackfillEstimate:
    request_count = len(tasks)
    estimated_rows = sum(task.expected_rows for task in tasks)
    request_seconds = observed_request_seconds or 0.25
    bytes_per_row = observed_bytes_per_row or 96.0
    transfer_seconds = request_count * request_seconds
    rate_limit_seconds = request_count / requests_per_minute * 60
    write_seconds = estimated_rows / 50_000
    total_seconds = max(rate_limit_seconds, transfer_seconds) + write_seconds
    if observed_request_seconds is not None and observed_bytes_per_row is not None:
        confidence: EstimateConfidence = "high"
        reasons = ("request latency and storage density use observed values",)
    elif observed_request_seconds is not None or observed_bytes_per_row is not None:
        confidence = "medium"
        reasons = ("one estimate component uses a default value",)
    else:
        confidence = "low"
        reasons = (
            "request latency uses a conservative default",
            "disk usage uses an estimated 96 bytes per minute row",
        )
    return BackfillEstimate(
        request_count=request_count,
        estimated_rows=estimated_rows,
        estimated_disk_bytes=round(estimated_rows * bytes_per_row),
        rate_limit_seconds=rate_limit_seconds,
        transfer_seconds=transfer_seconds,
        write_seconds=write_seconds,
        total_seconds=total_seconds,
        confidence=confidence,
        confidence_reasons=reasons,
    )


def plan_minute_backfill(
    store: DuckDBStore,
    manifest: BackfillManifest,
    *,
    session_spec: MinuteSourceSessionSpec | None = None,
    response_row_limit: int = 8_000,
    requests_per_minute: int = 500,
    observed_request_seconds: float | None = None,
    observed_bytes_per_row: float | None = None,
) -> MinuteBackfillPlan:
    """Expand eligibility windows, deduct exact coverage, and estimate work."""
    if response_row_limit < 1 or requests_per_minute < 1:
        raise ValueError("response and request limits must be positive")
    selected_spec = session_spec or next(
        value
        for value in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
        if value.source == "tushare" and value.freq == manifest.spec.minute_frequency
    )
    if not selected_spec.authoritative or not selected_spec.require_full_session:
        raise ValueError("backfill planning requires an authoritative full-session spec")

    open_dates, civil_dates = _authoritative_calendar_facts(store)
    demands = _calendar_scope_demands(manifest, open_dates, civil_dates)
    windows = _merge_windows(demands, open_dates)
    complete = _complete_minute_sessions(store, windows, selected_spec)
    accepted_missing = store.known_full_day_suspensions(
        tuple(sorted({window.ts_code for window in windows})),
        min((window.start_date for window in windows), default=manifest.start_date),
        max((window.end_date for window in windows), default=manifest.end_date),
    )
    accepted_missing -= complete
    coverage = _coverage_from_demands(demands, complete, accepted_missing)
    tasks = _missing_task_chunks(
        windows,
        complete,
        accepted_missing,
        open_dates,
        selected_spec,
        response_row_limit,
    )
    requested_session_count = sum(len(task.open_dates) for task in tasks)
    return MinuteBackfillPlan(
        manifest=manifest,
        windows=windows,
        tasks=tasks,
        coverage=coverage,
        requested_session_count=requested_session_count,
        estimate=_backfill_estimate(
            tasks,
            requests_per_minute=requests_per_minute,
            observed_request_seconds=observed_request_seconds,
            observed_bytes_per_row=observed_bytes_per_row,
        ),
    )


def minute_session_spec(
    *,
    source: str = "tushare",
    freq: str = "1min",
) -> MinuteSourceSessionSpec:
    try:
        return next(
            value
            for value in DEFAULT_MINUTE_SOURCE_SESSION_SPECS
            if value.source == source and value.freq == freq
        )
    except StopIteration as exc:
        raise KeyError(f"unknown minute session spec: {source}/{freq}") from exc


def complete_minute_task_sessions(
    store: DuckDBStore,
    task: MinuteBackfillTask,
) -> set[date]:
    window = MergedBackfillWindow(
        ts_code=task.ts_code,
        start_date=task.start_date,
        end_date=task.end_date,
        open_dates=task.open_dates,
    )
    complete = _complete_minute_sessions(
        store,
        (window,),
        minute_session_spec(source=task.source, freq=task.freq),
    )
    return {trading_date for _ts_code, trading_date in complete}


def backfill_state_input(
    plan: MinuteBackfillPlan,
    *,
    max_attempts: int = 3,
) -> BackfillManifestInput:
    from rquant.backfill_state import (
        BackfillEligibilityInput,
        BackfillManifestInput,
        BackfillTaskInput,
    )

    return BackfillManifestInput(
        manifest_id=plan.manifest.manifest_id,
        payload=plan.model_dump(mode="json", exclude_computed_fields=True),
        tasks=tuple(
            BackfillTaskInput(
                task_id=task.task_id,
                payload=task.model_dump(mode="json"),
                max_attempts=max_attempts,
            )
            for task in plan.tasks
        ),
        eligibility=tuple(
            BackfillEligibilityInput(
                eligibility_id=row.eligibility_id,
                payload=row.model_dump(mode="json"),
            )
            for row in plan.manifest.eligibilities
        ),
    )
