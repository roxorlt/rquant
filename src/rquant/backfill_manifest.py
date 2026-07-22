"""Typed, reproducible strategy eligibility and minute-backfill manifests."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, date, datetime, time, timedelta
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
    model_validator,
)

from rquant.data_metadata import DatasetSnapshotArtifact
from rquant.data_quality import (
    DEFAULT_MINUTE_SOURCE_SESSION_SPECS,
    MinuteSourceSessionSpec,
)
from rquant.growth_eligibility import (
    GrowthOpeningStructure,
    classify_growth_opening_structure,
)

EligibilityBasis = Literal["daily", "daily+auction"]
BackfillPhase = Literal["baseline", "entry", "exit"]
EstimateConfidence = Literal["low", "medium", "high"]
MinuteCoverageAuthority = Literal["operational", "research_lake", "combined"]
UnavailableSessionReason = Literal[
    "known_full_day_suspension",
    "not_listed",
]
SHANGHAI = ZoneInfo("Asia/Shanghai")
MINUTE_SESSION_AVAILABLE_AT = time(15, 10)
_MINUTE_COMPLETION_BATCH_SIZE = 8_192

if TYPE_CHECKING:
    from rquant.backfill_state import BackfillManifestInput
    from rquant.research_catalog import ResearchCatalog
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
    eligibility_entry_delay_trading_days: int = Field(default=0, ge=0, le=10)
    minute_frequency: Literal["1min"] = "1min"
    window: StrategyWindowRequirement = Field(
        default_factory=StrategyWindowRequirement
    )


def _strategy_spec_identity(spec: StrategyBackfillSpec) -> dict[str, object]:
    payload = spec.model_dump(mode="json")
    if spec.eligibility_entry_delay_trading_days == 0:
        payload.pop("eligibility_entry_delay_trading_days")
    return payload


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


class EligibilityResolutionGap(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    eligibility_date: date
    reason: str = Field(min_length=1)


class EligibilityResolution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    resolution_hash: str = ""
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    requested_dates: tuple[date, ...]
    evaluated_dates: tuple[date, ...]
    complete_dates: tuple[date, ...]
    incomplete: tuple[EligibilityResolutionGap, ...] = ()
    records: tuple[EligibilityRecord, ...] = ()
    input_artifacts: tuple[DatasetSnapshotArtifact, ...] = ()

    @model_validator(mode="after")
    def validate_and_derive_hash(self) -> EligibilityResolution:
        requested = tuple(sorted(set(self.requested_dates)))
        evaluated = tuple(sorted(set(self.evaluated_dates)))
        complete = tuple(sorted(set(self.complete_dates)))
        incomplete = tuple(
            sorted(self.incomplete, key=lambda row: row.eligibility_date)
        )
        records = tuple(
            sorted(
                {row.eligibility_id: row for row in self.records}.values(),
                key=lambda row: (
                    row.entry_date,
                    row.ts_code,
                    row.variant,
                    row.eligibility_id,
                ),
            )
        )
        input_artifacts = tuple(
            sorted(self.input_artifacts, key=lambda row: row.artifact_key)
        )
        artifact_keys = [row.artifact_key for row in input_artifacts]
        if len(artifact_keys) != len(set(artifact_keys)):
            raise ValueError("eligibility input artifacts must be unique")
        if input_artifacts and self.strategy_id != "auction_gap":
            raise ValueError(
                "only auction_gap eligibility may declare lake input artifacts"
            )
        if any(
            row.artifact_type != "lake_partition"
            or row.dataset_id != "auction_bar"
            or row.table_name != "auction_bar"
            for row in input_artifacts
        ):
            raise ValueError(
                "eligibility input artifacts must be auction_bar lake partitions"
            )
        requested_set = set(requested)
        evaluated_set = set(evaluated)
        complete_set = set(complete)
        if not evaluated_set <= requested_set:
            raise ValueError("evaluated dates must be requested")
        if not complete_set <= evaluated_set:
            raise ValueError("complete dates must be evaluated")
        incomplete_dates = [row.eligibility_date for row in incomplete]
        if len(incomplete_dates) != len(set(incomplete_dates)):
            raise ValueError("incomplete eligibility dates must be unique")
        if set(incomplete_dates) != requested_set - complete_set:
            raise ValueError(
                "incomplete evidence must explain every requested date "
                "that is not complete"
            )
        for row in records:
            if (
                row.strategy_id != self.strategy_id
                or row.strategy_version != self.strategy_version
            ):
                raise ValueError("eligibility record strategy does not match resolution")
            if row.eligibility_date not in complete_set:
                raise ValueError(
                    "eligibility records may only come from complete resolution dates"
                )
        object.__setattr__(self, "requested_dates", requested)
        object.__setattr__(self, "evaluated_dates", evaluated)
        object.__setattr__(self, "complete_dates", complete)
        object.__setattr__(self, "incomplete", incomplete)
        object.__setattr__(self, "records", records)
        object.__setattr__(self, "input_artifacts", input_artifacts)
        expected = _canonical_hash(
            {
                "strategy_id": self.strategy_id,
                "strategy_version": self.strategy_version,
                "requested_dates": requested,
                "evaluated_dates": evaluated,
                "complete_dates": complete,
                "incomplete": [
                    row.model_dump(mode="json") for row in incomplete
                ],
                "eligibility_ids": [row.eligibility_id for row in records],
                "input_artifacts": [
                    {
                        "artifact_key": row.artifact_key,
                        "partition_id": row.partition_id,
                        "relative_path": row.relative_path,
                        "row_count": row.row_count,
                        "schema_hash": row.schema_hash,
                        "content_hash": row.content_hash,
                        "file_hash": row.file_hash,
                    }
                    for row in input_artifacts
                ],
            }
        )
        if self.resolution_hash and self.resolution_hash != expected:
            raise ValueError("resolution_hash does not match resolution content")
        object.__setattr__(self, "resolution_hash", expected)
        return self

    @computed_field
    @property
    def expected_count(self) -> int:
        return len(self.requested_dates)

    @computed_field
    @property
    def available_count(self) -> int:
        return len(self.complete_dates)

    @computed_field
    @property
    def coverage_ratio(self) -> float:
        if self.expected_count == 0:
            return 0.0
        return self.available_count / self.expected_count


class BackfillManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_id: str = ""
    spec: StrategyBackfillSpec
    start_date: date
    end_date: date
    as_of_time: datetime
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    eligibilities: tuple[EligibilityRecord, ...]
    eligibility_resolution: EligibilityResolution | None = None

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
        resolution = self.eligibility_resolution
        if resolution is not None:
            if (
                resolution.strategy_id != self.spec.strategy_id
                or resolution.strategy_version != self.spec.strategy_version
            ):
                raise ValueError("eligibility resolution strategy does not match spec")
            if tuple(row.eligibility_id for row in resolution.records) != tuple(
                row.eligibility_id for row in ordered
            ):
                raise ValueError(
                    "manifest eligibilities must match eligibility resolution records"
                )
            if resolution.requested_dates and (
                resolution.requested_dates[0] < self.start_date
                or resolution.requested_dates[-1] > self.end_date
            ):
                raise ValueError("eligibility resolution is outside manifest range")
        object.__setattr__(self, "eligibilities", ordered)
        identity = {
            "spec": _strategy_spec_identity(self.spec),
            "start_date": self.start_date,
            "end_date": self.end_date,
            "as_of_time": self.as_of_time,
            "code_commit": self.code_commit,
            "eligibility_ids": [row.eligibility_id for row in ordered],
        }
        if resolution is not None:
            identity["eligibility_resolution_hash"] = resolution.resolution_hash
        expected = _canonical_hash(identity)
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
        eligibility_resolution: EligibilityResolution | None = None,
    ) -> BackfillManifest:
        return cls(
            spec=spec,
            start_date=start_date,
            end_date=end_date,
            as_of_time=as_of_time,
            code_commit=code_commit,
            eligibilities=tuple(eligibilities),
            eligibility_resolution=eligibility_resolution,
        )


class BackfillCalendarError(RuntimeError):
    """The authoritative calendar cannot support an exact backfill window."""


class BackfillPlanIntegrityError(RuntimeError):
    """Persisted runner tasks disagree with the immutable embedded plan."""


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


class UnavailableMinuteSession(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ts_code: str = Field(min_length=1)
    trade_date: date
    reason: UnavailableSessionReason


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
    unavailable_sessions: tuple[UnavailableMinuteSession, ...] = ()
    minute_coverage_artifacts: tuple[DatasetSnapshotArtifact, ...] = ()


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
        eligibility_entry_delay_trading_days=1,
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


def _authoritative_open_dates(
    store: DuckDBStore,
    *,
    required_start: date,
    required_end: date,
) -> list[date]:
    open_dates, _civil_dates = _authoritative_calendar_facts(
        store,
        required_start=required_start,
        required_end=required_end,
    )
    return open_dates


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
    calendar = _authoritative_open_dates(
        store,
        required_start=start_date,
        required_end=end_date,
    )
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
    structural_facts: tuple[GrowthOpeningStructure, ...] | None = None,
) -> tuple[EligibilityRecord, ...]:
    """Resolve the broad daily growth-board universe before any minute query."""
    import rquant.growth_board_surge_strategy as growth

    spec = STRATEGY_BACKFILL_SPECS["growth_board_surge"]
    config = growth.GrowthBoardSurgeConfig()
    calendar = _authoritative_open_dates(
        store,
        required_start=start_date,
        required_end=end_date,
    )
    targets = _target_open_dates(calendar, start_date, end_date)
    index_by_date = {day: index for index, day in enumerate(calendar)}
    previous_by_target: dict[date, date] = {}
    for trading_date in targets:
        current_index = index_by_date[trading_date]
        if current_index == 0:
            raise ValueError("growth eligibility requires a prior open session")
        previous_by_target[trading_date] = calendar[current_index - 1]
    resolved_structural_facts = structural_facts
    if resolved_structural_facts is None:
        resolved_structural_facts = classify_growth_opening_structure(
            store,
            previous_by_target,
        )
    excluded_by_target: dict[date, set[str]] = {}
    for fact in resolved_structural_facts:
        excluded_by_target.setdefault(fact.target_date, set()).add(fact.ts_code)
    records: list[EligibilityRecord] = []
    for trading_date in targets:
        previous_date = previous_by_target[trading_date]
        candidates = growth.resolve_growth_board_candidates(
            store,
            trading_date,
            previous_date,
            config.min_signal_time,
            structural_excluded_codes=excluded_by_target.get(
                trading_date,
                set(),
            ),
        )
        for candidate in candidates:
            if candidate.ts_code in excluded_by_target.get(trading_date, set()):
                continue
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


_ELIGIBILITY_INPUT_COVERAGE_THRESHOLD = 0.99
_N_SHAPE_PANEL_OPEN_DAYS = 121
_GROWTH_STRUCTURE_STAGE = "_rquant_growth_opening_structure"
_A_SHARE_TS_CODE_PATTERNS = (
    "000%.SZ",
    "001%.SZ",
    "002%.SZ",
    "003%.SZ",
    "300%.SZ",
    "301%.SZ",
    "600%.SH",
    "601%.SH",
    "603%.SH",
    "605%.SH",
    "688%.SH",
    "689%.SH",
)


def _a_share_code_predicate(column: str) -> str:
    return (
        "("
        + " OR ".join(
            f"{column} LIKE '{pattern}'" for pattern in _A_SHARE_TS_CODE_PATTERNS
        )
        + ")"
    )


def _coverage_passes(expected: int, available: int) -> bool:
    return (
        expected > 0
        and available / expected >= _ELIGIBILITY_INPUT_COVERAGE_THRESHOLD
    )


def _previous_open_dates(
    requested_dates: tuple[date, ...],
    calendar: list[date],
) -> dict[date, date]:
    index_by_date = {value: index for index, value in enumerate(calendar)}
    return {
        target: calendar[index_by_date[target] - 1]
        for target in requested_dates
        if index_by_date[target] > 0
    }


def _opening_panel_counts(
    store: DuckDBStore,
    *,
    requested_dates: tuple[date, ...],
    calendar: list[date],
    strategy_id: Literal["growth_board_surge", "auction_gap"],
    growth_structure_facts: tuple[GrowthOpeningStructure, ...] | None = None,
) -> dict[date, tuple[int, int]]:
    previous_by_target = _previous_open_dates(requested_dates, calendar)
    if not previous_by_target:
        return {}
    values = ", ".join("(?, ?)" for _ in previous_by_target)
    parameters: list[object] = [
        value
        for target, previous in previous_by_target.items()
        for value in (target, previous)
    ]
    basic_is_a_share = _a_share_code_predicate("basic.ts_code")
    status_is_a_share = _a_share_code_predicate("status.ts_code")
    bar_is_a_share = _a_share_code_predicate("bar.ts_code")
    board_filter = (
        """
        AND (
            universe.ts_code LIKE '300%'
            OR universe.ts_code LIKE '301%'
            OR universe.ts_code LIKE '688%'
            OR universe.ts_code LIKE '689%'
        )
        """
        if strategy_id == "growth_board_surge"
        else ""
    )
    if strategy_id == "growth_board_surge":
        structural_facts = growth_structure_facts
        if structural_facts is None:
            structural_facts = classify_growth_opening_structure(
                store,
                previous_by_target,
            )
        structure_frame = pd.DataFrame(
            [
                {
                    "target_date": fact.target_date,
                    "ts_code": fact.ts_code,
                    "is_deterministic_non_candidate": (
                        fact.is_deterministic_non_candidate
                    ),
                }
                for fact in structural_facts
            ],
            columns=[
                "target_date",
                "ts_code",
                "is_deterministic_non_candidate",
            ],
        )
        store._conn.register(_GROWTH_STRUCTURE_STAGE, structure_frame)
        availability = """
            (
                (
                    structure.ts_code IS NULL
                    AND status.ts_code IS NOT NULL
                    AND status.conflict_reason IS NULL
                    AND status.is_st IS NOT NULL
                    AND status.available_at IS NOT NULL
                    AND status.available_at <= (
                        CAST(expected.target_date AS TIMESTAMP)
                        + INTERVAL '9 hours 30 minutes'
                    ) AT TIME ZONE 'Asia/Shanghai'
                    AND indicator.ts_code IS NOT NULL
                    AND indicator.ma5 IS NOT NULL
                    AND indicator.ma10 IS NOT NULL
                    AND indicator.ma20 IS NOT NULL
                    AND indicator.ma60 IS NOT NULL
                    AND expected.daily_ts_code IS NOT NULL
                    AND expected.close IS NOT NULL
                    AND expected.close > 0
                )
                OR structure.is_deterministic_non_candidate = TRUE
            )
        """
        joins = """
        LEFT JOIN stock_status_daily AS status
          ON status.ts_code = expected.ts_code
         AND status.trade_date = expected.target_date
        LEFT JOIN daily_indicator AS indicator
          ON indicator.ts_code = expected.ts_code
         AND indicator.trade_date = expected.previous_date
        LEFT JOIN _rquant_growth_opening_structure AS structure
          ON structure.target_date = expected.target_date
         AND structure.ts_code = expected.ts_code
        """
    else:
        availability = """
            status.ts_code IS NOT NULL
            AND status.conflict_reason IS NULL
            AND status.is_st IS NOT NULL
            AND status.available_at IS NOT NULL
            AND status.available_at <= (
                CAST(expected.target_date AS TIMESTAMP)
                + INTERVAL '9 hours 27 minutes'
            ) AT TIME ZONE 'Asia/Shanghai'
            AND auction.ts_code IS NOT NULL
            AND expected.daily_ts_code IS NOT NULL
            AND expected.close IS NOT NULL
            AND expected.close > 0
        """
        joins = """
        LEFT JOIN stock_status_daily AS status
          ON status.ts_code = expected.ts_code
         AND status.trade_date = expected.target_date
        LEFT JOIN (
            SELECT DISTINCT ts_code, trade_date
            FROM auction_bar
            WHERE auction_type = 'open_realtime'
              AND price IS NOT NULL
              AND price > 0
              AND vol IS NOT NULL
              AND vol > 0
        ) AS auction
          ON auction.ts_code = expected.ts_code
         AND auction.trade_date = expected.target_date
        """
    try:
        rows = store._conn.execute(
            f"""
            WITH requested(target_date, previous_date) AS (
                VALUES {values}
            ),
            universe AS (
            SELECT requested.target_date,
                   requested.previous_date,
                   basic.ts_code
            FROM requested
            JOIN stock_basic AS basic
              ON basic.list_date IS NOT NULL
             AND basic.list_date <= requested.previous_date
             AND {basic_is_a_share}
            UNION
            SELECT requested.target_date,
                   requested.previous_date,
                   status.ts_code
            FROM requested
            JOIN stock_status_daily AS status
              ON status.trade_date = requested.previous_date
             AND {status_is_a_share}
            UNION
            SELECT requested.target_date,
                   requested.previous_date,
                   bar.ts_code
            FROM requested
            JOIN daily_bar AS bar
              ON bar.trade_date = requested.previous_date
             AND {bar_is_a_share}
            ),
            expected AS (
            SELECT universe.target_date,
                   universe.previous_date,
                   universe.ts_code,
                   bar.ts_code AS daily_ts_code,
                   bar.close
            FROM universe
            LEFT JOIN daily_bar AS bar
              ON bar.trade_date = universe.previous_date
             AND bar.ts_code = universe.ts_code
            WHERE TRUE
              {board_filter}
            )
            SELECT expected.target_date,
                   COUNT(*) AS expected_count,
                   COUNT(*) FILTER (WHERE {availability}) AS available_count
            FROM expected
            {joins}
            GROUP BY expected.target_date
            ORDER BY expected.target_date
            """,
            parameters,
        ).fetchall()
    finally:
        if strategy_id == "growth_board_surge":
            store._conn.unregister(_GROWTH_STRUCTURE_STAGE)
    return {
        _as_date(target): (int(expected), int(available))
        for target, expected, available in rows
    }


def _n_shape_complete_dates(
    store: DuckDBStore,
    *,
    requested_dates: tuple[date, ...],
    calendar: list[date],
) -> tuple[date, ...]:
    if not requested_dates:
        return ()
    index_by_date = {value: index for index, value in enumerate(calendar)}
    supported = [
        target
        for target in requested_dates
        if index_by_date[target] >= _N_SHAPE_PANEL_OPEN_DAYS - 1
    ]
    if not supported:
        return ()
    first_index = min(
        index_by_date[target] - (_N_SHAPE_PANEL_OPEN_DAYS - 1)
        for target in supported
    )
    last_index = max(index_by_date[target] for target in supported)
    source_dates = calendar[first_index : last_index + 1]
    basic_is_a_share = _a_share_code_predicate("basic.ts_code")
    status_is_a_share = _a_share_code_predicate("status.ts_code")
    bar_is_a_share = _a_share_code_predicate("bar.ts_code")
    rows = store._conn.execute(
        f"""
        WITH deterministic_nontrading AS (
            SELECT suspension.ts_code,
                   suspension.trade_date
            FROM stock_suspend_event AS suspension
            JOIN stock_suspend_coverage AS coverage
              ON coverage.source = suspension.source
             AND coverage.trade_date = suspension.trade_date
             AND coverage.coverage_state = 'complete'
            WHERE suspension.source = 'tushare'
              AND suspension.trade_date BETWEEN ? AND ?
            GROUP BY suspension.ts_code, suspension.trade_date
            HAVING count(*) FILTER (
                       WHERE suspension.suspend_type = 'S'
                   ) > 0
               AND count(*) FILTER (
                       WHERE suspension.suspend_type = 'R'
                   ) = 0
        ),
        universe AS (
            SELECT calendar.cal_date AS trade_date,
                   basic.ts_code
            FROM trade_calendar AS calendar
            JOIN stock_basic AS basic
              ON basic.list_date IS NOT NULL
             AND basic.list_date <= calendar.cal_date
             AND {basic_is_a_share}
            WHERE calendar.exchange = 'SSE'
              AND calendar.is_open = TRUE
              AND calendar.cal_date BETWEEN ? AND ?
            UNION
            SELECT status.trade_date,
                   status.ts_code
            FROM stock_status_daily AS status
            WHERE status.trade_date BETWEEN ? AND ?
              AND {status_is_a_share}
            UNION
            SELECT bar.trade_date,
                   bar.ts_code
            FROM daily_bar AS bar
            WHERE bar.trade_date BETWEEN ? AND ?
              AND {bar_is_a_share}
        )
        SELECT universe.trade_date,
               COUNT(*) AS expected_count,
               COUNT(*) FILTER (
                   WHERE (
                       (
                           bar.ts_code IS NOT NULL
                           AND bar.open IS NOT NULL
                           AND bar.high IS NOT NULL
                           AND bar.low IS NOT NULL
                           AND bar.close IS NOT NULL
                           AND state.ts_code IS NOT NULL
                           AND state.is_limit_up IS NOT NULL
                           AND state.is_limit_down IS NOT NULL
                           AND state.is_first_limit_up IS NOT NULL
                           AND state.is_yiziban IS NOT NULL
                           AND state.consecutive_limit_ups IS NOT NULL
                           AND state.body_upper IS NOT NULL
                           AND state.body_lower IS NOT NULL
                           AND status.ts_code IS NOT NULL
                           AND status.conflict_reason IS NULL
                           AND status.is_st IS NOT NULL
                           AND status.available_at IS NOT NULL
                           AND status.available_at <= (
                               CAST(universe.trade_date AS TIMESTAMP)
                               + INTERVAL '17 hours'
                           ) AT TIME ZONE 'Asia/Shanghai'
                           AND state.is_st IS NOT DISTINCT FROM status.is_st
                       )
                       OR (
                           bar.ts_code IS NULL
                           AND nontrading.ts_code IS NOT NULL
                       )
                   )
               ) AS available_count,
               COUNT(*) FILTER (
                   WHERE (
                       (
                           basic.ts_code IS NOT NULL
                           AND basic.circ_mv IS NOT NULL
                       )
                       OR (
                           bar.ts_code IS NULL
                           AND nontrading.ts_code IS NOT NULL
                       )
                   )
               ) AS basic_available_count
        FROM universe
        LEFT JOIN daily_bar AS bar
          ON bar.ts_code = universe.ts_code
         AND bar.trade_date = universe.trade_date
        LEFT JOIN daily_state AS state
          ON state.ts_code = universe.ts_code
         AND state.trade_date = universe.trade_date
        LEFT JOIN stock_status_daily AS status
          ON status.ts_code = universe.ts_code
         AND status.trade_date = universe.trade_date
        LEFT JOIN daily_basic AS basic
          ON basic.ts_code = universe.ts_code
         AND basic.trade_date = universe.trade_date
        LEFT JOIN deterministic_nontrading AS nontrading
          ON nontrading.ts_code = universe.ts_code
         AND nontrading.trade_date = universe.trade_date
        GROUP BY universe.trade_date
        ORDER BY universe.trade_date
        """,
        [
            source_dates[0],
            source_dates[-1],
            source_dates[0],
            source_dates[-1],
            source_dates[0],
            source_dates[-1],
            source_dates[0],
            source_dates[-1],
        ],
    ).fetchall()
    panel = {
        _as_date(trade_date): (
            int(expected),
            int(available),
            int(basic_available),
        )
        for trade_date, expected, available, basic_available in rows
    }

    complete: list[date] = []
    for target in supported:
        target_index = index_by_date[target]
        window = calendar[
            target_index - (_N_SHAPE_PANEL_OPEN_DAYS - 1) : target_index + 1
        ]
        facts = [panel.get(source_date, (0, 0, 0)) for source_date in window]
        if any(expected == 0 for expected, _available, _basic in facts):
            continue
        expected = sum(item[0] for item in facts)
        available = sum(item[1] for item in facts)
        current_expected, _current_available, current_basic = facts[-1]
        if _coverage_passes(expected, available) and _coverage_passes(
            current_expected,
            current_basic,
        ):
            complete.append(target)
    return tuple(complete)


def _eligibility_input_complete_dates(
    store: DuckDBStore,
    *,
    strategy_id: str,
    requested_dates: tuple[date, ...],
    calendar: list[date],
    growth_structure_facts: tuple[GrowthOpeningStructure, ...] | None = None,
) -> tuple[date, ...]:
    if strategy_id == "n_shape":
        return _n_shape_complete_dates(
            store,
            requested_dates=requested_dates,
            calendar=calendar,
        )
    counts = _opening_panel_counts(
        store,
        requested_dates=requested_dates,
        calendar=calendar,
        strategy_id=(
            "growth_board_surge"
            if strategy_id == "growth_board_surge"
            else "auction_gap"
        ),
        growth_structure_facts=growth_structure_facts,
    )
    return tuple(
        target
        for target in requested_dates
        if _coverage_passes(*counts.get(target, (0, 0)))
    )


def resolve_strategy_eligibility(
    store: DuckDBStore,
    *,
    strategy_id: str,
    start_date: date,
    end_date: date,
) -> EligibilityResolution:
    spec = STRATEGY_BACKFILL_SPECS.get(strategy_id)
    if spec is None:
        raise KeyError(f"unknown strategy backfill spec: {strategy_id}")
    calendar = _authoritative_open_dates(
        store,
        required_start=start_date,
        required_end=end_date,
    )
    requested_dates = tuple(_target_open_dates(calendar, start_date, end_date))
    growth_structure_facts: tuple[GrowthOpeningStructure, ...] | None = None
    if strategy_id == "n_shape":
        records = resolve_n_shape_eligibility(
            store,
            start_date=start_date,
            end_date=end_date,
        )
    elif strategy_id == "growth_board_surge":
        growth_structure_facts = classify_growth_opening_structure(
            store,
            _previous_open_dates(requested_dates, calendar),
        )
        records = resolve_growth_board_eligibility(
            store,
            start_date=start_date,
            end_date=end_date,
            structural_facts=growth_structure_facts,
        )
    else:
        records = resolve_auction_gap_eligibility(
            store,
            start_date=start_date,
            end_date=end_date,
        )
    complete_dates = _eligibility_input_complete_dates(
        store,
        strategy_id=strategy_id,
        requested_dates=requested_dates,
        calendar=calendar,
        growth_structure_facts=growth_structure_facts,
    )
    records = tuple(
        row for row in records if row.eligibility_date in set(complete_dates)
    )
    complete_set = set(complete_dates)
    return EligibilityResolution(
        strategy_id=spec.strategy_id,
        strategy_version=spec.strategy_version,
        requested_dates=requested_dates,
        evaluated_dates=requested_dates,
        complete_dates=complete_dates,
        incomplete=tuple(
            EligibilityResolutionGap(
                eligibility_date=value,
                reason=(
                    "auction_input_panel_below_99pct"
                    if strategy_id == "auction_gap"
                    else "daily_input_panel_below_99pct"
                ),
            )
            for value in requested_dates
            if value not in complete_set
        ),
        records=records,
    )


def _authoritative_calendar_facts(
    store: DuckDBStore,
    *,
    required_start: date | None = None,
    required_end: date | None = None,
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
    if (required_start is None) != (required_end is None):
        raise ValueError("required calendar range must include both boundaries")
    if required_start is not None and required_end is not None:
        if required_start > required_end:
            raise ValueError("required calendar start must not be after end")
        required = {
            required_start + timedelta(days=offset)
            for offset in range((required_end - required_start).days + 1)
        }
        missing = sorted(required - civil_dates)
        if missing:
            rendered = ", ".join(value.isoformat() for value in missing[:5])
            raise BackfillCalendarError(
                "authoritative trade calendar does not cover manifest range: "
                f"{rendered}"
            )
    return open_dates, civil_dates


def _latest_closed_open_session(
    open_dates: list[date],
    *,
    as_of_time: datetime,
) -> date:
    local_as_of = _aware_utc(as_of_time, field_name="as_of_time").astimezone(
        SHANGHAI
    )
    closed_dates = [
        value
        for value in open_dates
        if value < local_as_of.date()
        or (
            value == local_as_of.date()
            and local_as_of.time() > MINUTE_SESSION_AVAILABLE_AT
        )
    ]
    if not closed_dates:
        raise BackfillCalendarError("no closed market session is observable as of time")
    return closed_dates[-1]


def latest_observable_eligibility_date(
    store: DuckDBStore,
    *,
    spec: StrategyBackfillSpec,
    as_of_time: datetime,
) -> date:
    """Return the newest candidate date with its full entry/exit window closed."""
    open_dates, _civil_dates = _authoritative_calendar_facts(store)
    latest_closed = _latest_closed_open_session(
        open_dates,
        as_of_time=as_of_time,
    )
    latest_index = open_dates.index(latest_closed)
    forward_sessions = (
        spec.eligibility_entry_delay_trading_days
        + spec.window.entry_trading_days
        - 1
        + spec.window.exit_trading_days
    )
    candidate_index = latest_index - forward_sessions
    if candidate_index < 0:
        raise BackfillCalendarError(
            f"{spec.strategy_id} requires {forward_sessions} closed sessions "
            "after eligibility"
        )
    return open_dates[candidate_index]


def resolve_requested_eligibility_end(
    *,
    requested_end: date | None,
    observable_end: date,
    start_date: date,
) -> date:
    selected = requested_end or observable_end
    if selected > observable_end:
        raise BackfillCalendarError(
            f"requested eligibility end {selected.isoformat()} exceeds "
            f"observable end {observable_end.isoformat()}"
        )
    if selected < start_date:
        raise BackfillCalendarError(
            f"eligibility start {start_date.isoformat()} exceeds "
            f"observable end {selected.isoformat()}"
        )
    return selected


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
    latest_closed = _latest_closed_open_session(
        open_dates,
        as_of_time=manifest.as_of_time,
    )
    if required_end > latest_closed:
        raise BackfillCalendarError(
            f"required minute session {required_end.isoformat()} is later than "
            f"latest closed session {latest_closed.isoformat()}"
        )
    return demands


def validate_executable_backfill_plan(
    store: DuckDBStore,
    plan: MinuteBackfillPlan,
) -> None:
    """Revalidate a persisted plan before any task can be claimed."""
    open_dates, civil_dates = _authoritative_calendar_facts(
        store,
        required_start=plan.manifest.start_date,
        required_end=plan.manifest.end_date,
    )
    demands = _calendar_scope_demands(
        plan.manifest,
        open_dates,
        civil_dates,
    )
    authoritative_open_dates = set(open_dates)
    demanded_sessions = {
        (ts_code, trading_date)
        for _eligibility_id, ts_code, _phase, trading_date in demands
    }
    for task in plan.tasks:
        for trading_date in task.open_dates:
            if trading_date not in authoritative_open_dates:
                raise BackfillCalendarError(
                    "persisted task contains a non-open session: "
                    f"{task.ts_code} {trading_date.isoformat()}"
                )
            if (task.ts_code, trading_date) not in demanded_sessions:
                raise BackfillCalendarError(
                    "persisted task is outside the manifest demand: "
                    f"{task.ts_code} {trading_date.isoformat()}"
                )


def validate_persisted_backfill_tasks(
    persisted: BackfillManifestInput,
    plan: MinuteBackfillPlan,
) -> None:
    """Ensure the runner will claim the exact tasks embedded in the plan."""
    if persisted.manifest_id != plan.manifest.manifest_id:
        raise BackfillPlanIntegrityError(
            "persisted manifest ID disagrees with the embedded plan"
        )
    embedded_tasks = {
        task.task_id: task.model_dump(mode="json") for task in plan.tasks
    }
    claim_tasks = {
        task.task_id: task.payload for task in persisted.tasks
    }
    if embedded_tasks.keys() != claim_tasks.keys():
        raise BackfillPlanIntegrityError(
            "persisted claim task IDs disagree with the embedded plan"
        )
    changed_task_ids = sorted(
        task_id
        for task_id, payload in embedded_tasks.items()
        if claim_tasks[task_id] != payload
    )
    if changed_task_ids:
        rendered = ", ".join(changed_task_ids[:5])
        raise BackfillPlanIntegrityError(
            "persisted claim task payload disagrees with the embedded plan: "
            f"{rendered}"
        )


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
    return _complete_minute_sessions_from_relation(
        store._conn,
        relation_sql="minute_bar",
        relation_parameters=(),
        desired=_desired_minute_sessions(windows),
        session_spec=session_spec,
    )


def _complete_minute_sessions_from_relation(
    connection: duckdb.DuckDBPyConnection,
    *,
    relation_sql: str,
    relation_parameters: tuple[object, ...],
    relation_parameters_by_date: dict[
        date,
        tuple[object, ...],
    ]
    | None = None,
    desired: tuple[tuple[str, date], ...],
    session_spec: MinuteSourceSessionSpec,
) -> set[tuple[str, date]]:
    expected_times = tuple(
        value.isoformat(timespec="seconds")
        for value in session_spec.expected_times()
    )
    expected_placeholders = ", ".join("?" for _ in expected_times)
    complete: set[tuple[str, date]] = set()
    ordered_desired = tuple(
        sorted(set(desired), key=lambda row: (row[1], row[0]))
    )

    def collect_batch(
        batch: tuple[tuple[str, date], ...],
        *,
        current_relation_parameters: tuple[object, ...],
        start_date: date,
        end_date: date,
    ) -> None:
        target_placeholders = ", ".join("(?, ?)" for _ in batch)
        target_parameters = [
            value
            for ts_code, target_date in batch
            for value in (ts_code, target_date)
        ]
        rows = connection.execute(
            f"""
            WITH desired_sessions(ts_code, trade_date) AS (
                VALUES {target_placeholders}
            )
            SELECT minute.ts_code,
                   CAST(minute.trade_time AS DATE) AS trade_date
            FROM {relation_sql} AS minute
            INNER JOIN desired_sessions AS desired
              ON desired.ts_code = minute.ts_code
             AND desired.trade_date = CAST(minute.trade_time AS DATE)
            WHERE minute.source = ?
              AND minute.freq = ?
              AND minute.trade_time >= ?
              AND minute.trade_time < ?
            GROUP BY minute.ts_code, CAST(minute.trade_time AS DATE)
            HAVING COUNT(
                       DISTINCT strftime(minute.trade_time, '%H:%M:%S')
                   ) = ?
               AND COUNT(
                       DISTINCT CASE
                           WHEN strftime(
                               minute.trade_time,
                               '%H:%M:%S'
                           ) IN ({expected_placeholders})
                           THEN strftime(minute.trade_time, '%H:%M:%S')
                       END
                   ) = ?
            """,
            [
                *target_parameters,
                *current_relation_parameters,
                session_spec.source,
                session_spec.freq,
                start_date,
                end_date + timedelta(days=1),
                len(expected_times),
                *expected_times,
                len(expected_times),
            ],
        ).fetchall()
        complete.update(
            (str(ts_code), _as_date(trading_date))
            for ts_code, trading_date in rows
        )

    if relation_parameters_by_date is None:
        for offset in range(
            0,
            len(ordered_desired),
            _MINUTE_COMPLETION_BATCH_SIZE,
        ):
            batch = ordered_desired[
                offset : offset + _MINUTE_COMPLETION_BATCH_SIZE
            ]
            collect_batch(
                batch,
                current_relation_parameters=relation_parameters,
                start_date=batch[0][1],
                end_date=batch[-1][1],
            )
        return complete

    for trade_date, date_rows in groupby(
        ordered_desired,
        key=lambda row: row[1],
    ):
        current_relation_parameters = relation_parameters_by_date.get(
            trade_date
        )
        if current_relation_parameters is None:
            continue
        date_desired = tuple(date_rows)
        for offset in range(
            0,
            len(date_desired),
            _MINUTE_COMPLETION_BATCH_SIZE,
        ):
            collect_batch(
                date_desired[
                    offset : offset + _MINUTE_COMPLETION_BATCH_SIZE
                ],
                current_relation_parameters=current_relation_parameters,
                start_date=trade_date,
                end_date=trade_date,
            )
    return complete


def _desired_minute_sessions(
    windows: tuple[MergedBackfillWindow, ...],
) -> tuple[tuple[str, date], ...]:
    return tuple(
        sorted(
            {
                (window.ts_code, trading_date)
                for window in windows
                for trading_date in window.open_dates
            },
            key=lambda row: (row[1], row[0]),
        )
    )


def _minute_artifact_paths_by_date(
    artifacts: tuple[DatasetSnapshotArtifact, ...],
    *,
    lake_root: Path,
) -> dict[date, tuple[object, ...]]:
    paths_by_date: dict[date, tuple[object, ...]] = {}
    prefix = "minute_bar:"
    suffix = ":1min"
    for artifact in artifacts:
        partition_id = artifact.partition_id
        if (
            partition_id is None
            or not partition_id.startswith(prefix)
            or not partition_id.endswith(suffix)
        ):
            raise ValueError("minute coverage artifact has an invalid partition id")
        trade_date = date.fromisoformat(
            partition_id[len(prefix) : -len(suffix)]
        )
        if trade_date in paths_by_date:
            raise ValueError(
                f"minute coverage has duplicate artifacts for {trade_date}"
            )
        paths_by_date[trade_date] = (
            [str(Path(lake_root) / artifact.relative_path)],
        )
    return paths_by_date


def _complete_minute_sessions_from_lake(
    windows: tuple[MergedBackfillWindow, ...],
    session_spec: MinuteSourceSessionSpec,
    *,
    catalog: ResearchCatalog,
    lake_root: Path,
    as_of_time: datetime,
    memory_only: bool = False,
    desired_sessions: tuple[tuple[str, date], ...] | None = None,
) -> tuple[
    set[tuple[str, date]],
    tuple[DatasetSnapshotArtifact, ...],
]:
    if not windows:
        return set(), ()
    from rquant.research_snapshot import SnapshotArtifactResolver

    start_date = min(window.start_date for window in windows)
    end_date = max(window.end_date for window in windows)
    artifacts = SnapshotArtifactResolver(
        catalog=catalog,
        lake_root=lake_root,
    ).resolve_lake_partitions(
        dataset="minute_bar",
        start_date=start_date,
        end_date=end_date,
        freq=session_spec.freq,
        as_of_time=as_of_time,
    )
    if not artifacts:
        return set(), ()
    parameters_by_date = _minute_artifact_paths_by_date(
        artifacts,
        lake_root=lake_root,
    )
    selected_desired = (
        _desired_minute_sessions(windows)
        if desired_sessions is None
        else tuple(
            sorted(
                set(desired_sessions),
                key=lambda row: (row[1], row[0]),
            )
        )
    )
    if not selected_desired:
        return set(), artifacts
    connect_config = {"temp_directory": ""} if memory_only else {}
    with duckdb.connect(config=connect_config) as connection:
        complete = _complete_minute_sessions_from_relation(
            connection,
            relation_sql="read_parquet(?, hive_partitioning = false)",
            relation_parameters=(),
            relation_parameters_by_date=parameters_by_date,
            desired=selected_desired,
            session_spec=session_spec,
        )
    return complete, artifacts


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


def _unavailable_minute_sessions(
    store: DuckDBStore,
    windows: tuple[MergedBackfillWindow, ...],
) -> tuple[UnavailableMinuteSession, ...]:
    if not windows:
        return ()
    codes = tuple(sorted({window.ts_code for window in windows}))
    start_date = min(window.start_date for window in windows)
    end_date = max(window.end_date for window in windows)
    desired = {
        (window.ts_code, trading_date)
        for window in windows
        for trading_date in window.open_dates
    }
    reasons: dict[tuple[str, date], UnavailableSessionReason] = {
        key: "known_full_day_suspension"
        for key in store.known_full_day_suspensions(
            codes,
            start_date,
            end_date,
        )
        if key in desired
    }
    placeholders = ", ".join("?" for _ in codes)
    listing_rows = store._conn.execute(
        f"""
        SELECT ts_code, list_date
        FROM stock_basic
        WHERE ts_code IN ({placeholders})
          AND list_date IS NOT NULL
        """,
        list(codes),
    ).fetchall()
    listing_dates = {
        str(ts_code): _as_date(list_date)
        for ts_code, list_date in listing_rows
    }
    for ts_code, trading_date in desired:
        list_date = listing_dates.get(ts_code)
        if list_date is not None and trading_date < list_date:
            reasons[(ts_code, trading_date)] = "not_listed"
    return tuple(
        UnavailableMinuteSession(
            ts_code=ts_code,
            trade_date=trading_date,
            reason=reason,
        )
        for (ts_code, trading_date), reason in sorted(reasons.items())
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
    coverage_authority: MinuteCoverageAuthority = "operational",
    research_catalog: ResearchCatalog | None = None,
    research_lake_root: Path | None = None,
    coverage_as_of_time: datetime | None = None,
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
    if coverage_authority not in {"operational", "research_lake", "combined"}:
        raise ValueError(f"unknown minute coverage authority: {coverage_authority}")
    if coverage_authority in {"research_lake", "combined"} and (
        research_catalog is None or research_lake_root is None
    ):
        raise ValueError(
            "research lake coverage requires both catalog and lake root"
        )

    open_dates, civil_dates = _authoritative_calendar_facts(
        store,
        required_start=manifest.start_date,
        required_end=manifest.end_date,
    )
    demands = _calendar_scope_demands(manifest, open_dates, civil_dates)
    windows = _merge_windows(demands, open_dates)
    desired_sessions = _desired_minute_sessions(windows)
    complete: set[tuple[str, date]] = set()
    minute_coverage_artifacts: tuple[DatasetSnapshotArtifact, ...] = ()
    if coverage_authority in {"operational", "combined"}:
        complete |= _complete_minute_sessions(store, windows, selected_spec)
    if coverage_authority in {"research_lake", "combined"}:
        lake_complete, minute_coverage_artifacts = (
            _complete_minute_sessions_from_lake(
                windows,
                selected_spec,
                catalog=research_catalog,
                lake_root=research_lake_root,
                as_of_time=coverage_as_of_time or manifest.as_of_time,
                desired_sessions=tuple(
                    row for row in desired_sessions if row not in complete
                ),
            )
        )
        complete |= lake_complete
    unavailable_sessions = tuple(
        row
        for row in _unavailable_minute_sessions(store, windows)
        if (row.ts_code, row.trade_date) not in complete
    )
    accepted_missing = {
        (row.ts_code, row.trade_date) for row in unavailable_sessions
    }
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
        unavailable_sessions=unavailable_sessions,
        minute_coverage_artifacts=minute_coverage_artifacts,
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
