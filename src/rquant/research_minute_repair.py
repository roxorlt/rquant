"""Governed historical minute research-lake repair."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import uuid
from collections import defaultdict
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.backfill_manifest import (
    MinuteBackfillPlan,
    _complete_minute_sessions,
    _complete_minute_sessions_from_lake,
    minute_session_spec,
    validate_persisted_backfill_tasks,
)
from rquant.backfill_state import BackfillStateStore
from rquant.data_metadata import DatasetSnapshotArtifact
from rquant.research_catalog import ResearchCatalog, exclusive_file_lock
from rquant.research_ingest import (
    ResearchAuthorityObservation,
    ResearchIngestPaths,
    ResearchMinuteRepairObservation,
    ResearchMinuteRepairPartitionChange,
    _copy_file_atomic,
    _file_sha256,
    _fsync_directory,
    _mkdir_durable,
    _prepare_readonly_generation,
    _query_existing_research_partition,
    _recover_interrupted_publish,
    _remove_transaction_root,
    _restore_file,
    _validate_prior_authority,
    _write_model_atomic,
)
from rquant.research_lake import (
    ResearchPartitionKey,
    ResearchPartitionManifest,
    export_research_dataset,
    partition_directory,
    partition_manifest_relative_path,
)
from rquant.storage.duckdb import DuckDBStore

_MINUTE_COLUMNS = (
    "ts_code",
    "trade_time",
    "freq",
    "open",
    "high",
    "low",
    "close",
    "vol",
    "amount",
    "source",
    "created_at",
)
_MINUTE_PHYSICAL_KEY = ("ts_code", "trade_time", "freq", "source")
_MINUTE_BUSINESS_COLUMNS = tuple(
    column for column in _MINUTE_COLUMNS if column != "created_at"
)
_MINUTE_NUMERIC_COLUMNS = ("open", "high", "low", "close", "vol", "amount")
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_ACTION_ID = "research-minute-history-repair/v1"
_CST = ZoneInfo("Asia/Shanghai")
_MARKET_PROTECTION_START = time(9, 15)
_MARKET_PROTECTION_END = time(15, 10)


class _MinuteRepairModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class MinuteRepairSession(_MinuteRepairModel):
    ts_code: str = Field(pattern=r"^\d{6}\.(?:SZ|SH|BJ)$")
    trade_date: date


class MinuteRepairScope(_MinuteRepairModel):
    required_session_count: int = Field(ge=0)
    unavailable_session_count: int = Field(ge=0)
    lake_complete_session_count: int = Field(ge=0)
    missing_sessions: tuple[MinuteRepairSession, ...]
    source_complete_session_count: int = Field(ge=0)
    minute_coverage_artifacts: tuple[DatasetSnapshotArtifact, ...] = ()


class ResearchMinuteRepairDayPlan(_MinuteRepairModel):
    trade_date: date
    target_session_count: int = Field(gt=0)
    target_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
    existing_manifest_sha256: str | None = Field(
        default=None,
        pattern=_HASH_PATTERN,
    )
    source_rows_sha256: str = Field(pattern=_HASH_PATTERN)
    merged_rows_sha256: str = Field(pattern=_HASH_PATTERN)
    existing_row_count: int = Field(ge=0)
    source_row_count: int = Field(gt=0)
    merged_row_count: int = Field(gt=0)
    changed: bool


class ResearchMinuteRepairPlan(_MinuteRepairModel):
    schema_version: Literal[1] = 1
    action_id: Literal["research-minute-history-repair/v1"] = _ACTION_ID
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    manifest_id: str = Field(pattern=_HASH_PATTERN)
    manifest_content_sha256: str = Field(pattern=_HASH_PATTERN)
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    window_scope_sha256: str = Field(pattern=_HASH_PATTERN)
    unavailable_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
    authority_current_sha256: str = Field(pattern=_HASH_PATTERN)
    catalog_sha256: str = Field(pattern=_HASH_PATTERN)
    readonly_catalog_sha256: str = Field(pattern=_HASH_PATTERN)
    required_session_count: int = Field(ge=0)
    unavailable_session_count: int = Field(ge=0)
    lake_complete_session_count: int = Field(ge=0)
    missing_session_count: int = Field(ge=0)
    source_complete_session_count: int = Field(ge=0)
    required_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
    missing_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
    lake_complete_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
    source_complete_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
    affected_ts_codes: tuple[str, ...] = ()
    days: tuple[ResearchMinuteRepairDayPlan, ...] = ()

    @model_validator(mode="after")
    def validate_scope(self) -> ResearchMinuteRepairPlan:
        dates = tuple(day.trade_date for day in self.days)
        if dates != tuple(sorted(set(dates))):
            raise ValueError("minute repair days must be strictly ordered and unique")
        if self.lake_complete_session_count + self.missing_session_count != (
            self.required_session_count
        ):
            raise ValueError("minute repair coverage counts are inconsistent")
        if self.source_complete_session_count != self.missing_session_count:
            raise ValueError("minute repair source coverage must equal missing coverage")
        if self.source_complete_sessions_sha256 != self.missing_sessions_sha256:
            raise ValueError("minute repair source session hash must equal missing hash")
        if self.affected_ts_codes != tuple(sorted(set(self.affected_ts_codes))):
            raise ValueError("minute repair affected codes must be sorted and unique")
        if sum(day.target_session_count for day in self.days) != (
            self.missing_session_count
        ):
            raise ValueError("minute repair day session counts are inconsistent")
        if self.missing_session_count and not self.days:
            raise ValueError("minute repair missing sessions require day plans")
        return self

    @property
    def plan_id(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


class ResearchMinuteRepairResult(_MinuteRepairModel):
    status: Literal["planned", "candidate", "unchanged"]
    plan: ResearchMinuteRepairPlan
    observation: ResearchMinuteRepairObservation | None = None

    @model_validator(mode="after")
    def validate_result(self) -> ResearchMinuteRepairResult:
        if self.status == "candidate" and self.observation is None:
            raise ValueError("candidate minute repair requires an observation")
        if self.status != "candidate" and self.observation is not None:
            raise ValueError("non-candidate minute repair cannot contain an observation")
        return self

    @property
    def plan_id(self) -> str:
        return self.plan.plan_id


class _ResearchMinuteRepairJournalEntry(_MinuteRepairModel):
    trade_date: date
    manifest_existed: bool
    manifest_before_sha256: str | None = Field(
        default=None,
        pattern=_HASH_PATTERN,
    )
    manifest_after_sha256: str = Field(pattern=_HASH_PATTERN)
    version_relative_path: str
    version_existed: bool
    version_sha256: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_before_hash(self) -> _ResearchMinuteRepairJournalEntry:
        if self.manifest_existed != (self.manifest_before_sha256 is not None):
            raise ValueError("minute repair manifest existence/hash mismatch")
        return self


class _ResearchMinuteRepairPublishJournal(_MinuteRepairModel):
    schema_version: Literal[1] = 1
    transaction_kind: Literal["minute_repair"] = "minute_repair"
    transaction_id: str
    observation_path: Path
    catalog_before_sha256: str = Field(pattern=_HASH_PATTERN)
    catalog_after_sha256: str = Field(pattern=_HASH_PATTERN)
    readonly_before_sha256: str = Field(pattern=_HASH_PATTERN)
    readonly_after_sha256: str = Field(pattern=_HASH_PATTERN)
    current_before_sha256: str = Field(pattern=_HASH_PATTERN)
    current_after_sha256: str = Field(pattern=_HASH_PATTERN)
    entries: tuple[_ResearchMinuteRepairJournalEntry, ...] = Field(
        min_length=1
    )


@dataclass(frozen=True)
class _PreparedMinuteRepair:
    plan: ResearchMinuteRepairPlan
    previous: ResearchAuthorityObservation
    previous_observation_sha256: str
    merged_by_date: dict[date, pd.DataFrame]
    existing_manifests: dict[date, ResearchPartitionManifest | None]


def _canonical_sha256(payload: object) -> str:
    serialized = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def hash_minute_sessions(
    sessions: tuple[MinuteRepairSession, ...],
) -> str:
    ordered = sorted(
        sessions,
        key=lambda row: (row.trade_date, row.ts_code),
    )
    return _canonical_sha256(
        [row.model_dump(mode="json") for row in ordered]
    )


def _normalized_minute_frame(
    frame: pd.DataFrame,
    *,
    trade_date: date | None = None,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=_MINUTE_COLUMNS)
    missing = set(_MINUTE_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"minute partition missing columns: {sorted(missing)}")
    normalized = frame[list(_MINUTE_COLUMNS)].copy()
    normalized["trade_time"] = pd.to_datetime(
        normalized["trade_time"],
        errors="raise",
    )
    normalized["created_at"] = pd.to_datetime(
        normalized["created_at"],
        errors="raise",
    )
    if normalized[list(_MINUTE_PHYSICAL_KEY)].isna().any(axis=None):
        raise ValueError("minute partition contains a null physical key")
    if normalized["created_at"].isna().any():
        raise ValueError("minute partition contains a null created_at")
    if normalized.duplicated(list(_MINUTE_PHYSICAL_KEY), keep=False).any():
        raise ValueError("minute partition contains a duplicate physical key")
    if trade_date is not None and set(normalized["trade_time"].dt.date) != {
        trade_date
    }:
        raise ValueError(
            f"minute partition contains rows outside target date {trade_date}"
        )
    for column in _MINUTE_NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="raise")
    return normalized


def hash_minute_rows(frame: pd.DataFrame) -> str:
    """Cryptographically bind all minute row values, including created_at."""
    normalized = _normalized_minute_frame(frame)
    ordered = normalized.sort_values(
        list(_MINUTE_PHYSICAL_KEY),
        kind="stable",
    )
    header = json.dumps(
        _MINUTE_COLUMNS,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    body = ordered.to_csv(
        index=False,
        columns=list(_MINUTE_COLUMNS),
        date_format="%Y-%m-%dT%H:%M:%S.%f",
        float_format="%.17g",
        na_rep="<NULL>",
        lineterminator="\n",
    ).encode("utf-8")
    digest = hashlib.sha256()
    digest.update(header)
    digest.update(b"\n")
    digest.update(body)
    return digest.hexdigest()


def _business_values(row: pd.Series) -> tuple[object, ...]:
    values: list[object] = []
    for column in _MINUTE_BUSINESS_COLUMNS:
        value = row[column]
        if value is None or pd.isna(value):
            values.append(None)
        elif isinstance(value, (pd.Timestamp, datetime)):
            values.append(pd.Timestamp(value).to_pydatetime())
        else:
            item = getattr(value, "item", None)
            values.append(item() if callable(item) else value)
    return tuple(values)


def _validate_operational_sessions(
    frame: pd.DataFrame,
    *,
    trade_date: date,
    target_sessions: tuple[MinuteRepairSession, ...],
) -> pd.DataFrame:
    current = _normalized_minute_frame(frame, trade_date=trade_date)
    expected_codes = {
        row.ts_code for row in target_sessions if row.trade_date == trade_date
    }
    if not expected_codes:
        raise ValueError("minute repair target sessions do not bind the trade date")
    if (
        set(current["source"].astype(str)) != {"tushare"}
        or set(current["freq"].astype(str)) != {"1min"}
    ):
        raise ValueError("minute repair source must be tushare 1min")
    if set(current["ts_code"].astype(str)) != expected_codes:
        raise ValueError("minute repair source session codes do not match the plan")
    expected_times = minute_session_spec().expected_times()
    for ts_code, group in current.groupby("ts_code", sort=True):
        actual_times = tuple(
            sorted(set(group["trade_time"].dt.time.tolist()))
        )
        if actual_times != expected_times or len(group) != len(expected_times):
            raise ValueError(
                f"minute repair source session is incomplete: {ts_code}@{trade_date}"
            )
    for column in ("open", "high", "low", "close"):
        if any(
            not math.isfinite(float(value)) or float(value) <= 0
            for value in current[column]
        ):
            raise ValueError(f"minute repair source contains invalid {column}")
    for column in ("vol", "amount"):
        if any(
            not math.isfinite(float(value)) or float(value) < 0
            for value in current[column]
        ):
            raise ValueError(f"minute repair source contains invalid {column}")
    return current


def merge_minute_partition(
    existing: pd.DataFrame,
    operational: pd.DataFrame,
    *,
    trade_date: date,
    target_sessions: tuple[MinuteRepairSession, ...],
) -> pd.DataFrame:
    """Upsert target operational sessions while preserving prior evidence time."""
    prior = _normalized_minute_frame(existing, trade_date=trade_date)
    current = _validate_operational_sessions(
        operational,
        trade_date=trade_date,
        target_sessions=target_sessions,
    )
    keyed: dict[tuple[object, ...], pd.Series] = {}
    for _, row in prior.iterrows():
        key = tuple(row[column] for column in _MINUTE_PHYSICAL_KEY)
        keyed[key] = row.copy()
    for _, row in current.iterrows():
        key = tuple(row[column] for column in _MINUTE_PHYSICAL_KEY)
        previous = keyed.get(key)
        if previous is None or _business_values(previous) != _business_values(row):
            keyed[key] = row.copy()
    merged = pd.DataFrame(
        [row.to_dict() for row in keyed.values()],
        columns=_MINUTE_COLUMNS,
    )
    merged["trade_time"] = pd.to_datetime(merged["trade_time"], errors="raise")
    merged["created_at"] = pd.to_datetime(merged["created_at"], errors="raise")
    return (
        merged.sort_values(list(_MINUTE_PHYSICAL_KEY), kind="stable")
        .reset_index(drop=True)
    )


def build_minute_repair_day_plan(
    *,
    trade_date: date,
    target_sessions: tuple[MinuteRepairSession, ...],
    existing_manifest_sha256: str | None,
    existing: pd.DataFrame,
    operational: pd.DataFrame,
) -> tuple[ResearchMinuteRepairDayPlan, pd.DataFrame]:
    targets = tuple(
        sorted(
            target_sessions,
            key=lambda row: (row.trade_date, row.ts_code),
        )
    )
    if not targets or any(row.trade_date != trade_date for row in targets):
        raise ValueError("minute repair day targets must bind the trade date")
    merged = merge_minute_partition(
        existing,
        operational,
        trade_date=trade_date,
        target_sessions=targets,
    )
    normalized_existing = _normalized_minute_frame(
        existing,
        trade_date=trade_date,
    )
    existing_hash = hash_minute_rows(normalized_existing)
    merged_hash = hash_minute_rows(merged)
    day = ResearchMinuteRepairDayPlan(
        trade_date=trade_date,
        target_session_count=len(targets),
        target_sessions_sha256=hash_minute_sessions(targets),
        existing_manifest_sha256=existing_manifest_sha256,
        source_rows_sha256=hash_minute_rows(operational),
        merged_rows_sha256=merged_hash,
        existing_row_count=len(normalized_existing),
        source_row_count=len(operational),
        merged_row_count=len(merged),
        changed=existing_hash != merged_hash,
    )
    return day, merged


def load_completed_backfill_plan(
    state: BackfillStateStore,
    manifest_id: str,
) -> MinuteBackfillPlan:
    """Load one integrity-checked, completed minute backfill plan."""
    plan, _content_hash = _load_completed_backfill_binding(state, manifest_id)
    return plan


def _load_completed_backfill_binding(
    state: BackfillStateStore,
    manifest_id: str,
) -> tuple[MinuteBackfillPlan, str]:
    persisted = state.load_manifest(manifest_id)
    if persisted is None:
        raise ValueError(f"unknown backfill manifest: {manifest_id}")
    status = state.get_manifest_status(manifest_id)
    if status.status != "completed":
        raise ValueError(
            "minute repair requires a completed backfill manifest: "
            f"{status.status}"
        )
    plan = MinuteBackfillPlan.model_validate(persisted.payload)
    validate_persisted_backfill_tasks(persisted, plan)
    embedded_eligibility = {
        row.eligibility_id: row.model_dump(mode="json")
        for row in plan.manifest.eligibilities
    }
    persisted_eligibility = {
        row.eligibility_id: row.payload for row in persisted.eligibility
    }
    if embedded_eligibility != persisted_eligibility:
        raise ValueError(
            "persisted backfill eligibility disagrees with the embedded plan"
        )
    expected_times = len(minute_session_spec().expected_times())
    embedded_task_sessions: set[tuple[str, date]] = set()
    for task in plan.tasks:
        if task.source != "tushare" or task.freq != "1min":
            raise ValueError("persisted backfill task uses an unsupported minute source")
        if task.expected_rows != len(task.open_dates) * expected_times:
            raise ValueError("persisted backfill task expected rows are inconsistent")
        for trade_date in task.open_dates:
            key = (task.ts_code, trade_date)
            if key in embedded_task_sessions:
                raise ValueError("persisted backfill tasks contain duplicate sessions")
            embedded_task_sessions.add(key)
    if len(embedded_task_sessions) != plan.requested_session_count:
        raise ValueError(
            "persisted backfill tasks disagree with requested session count"
        )
    required_minute_sessions(plan)
    return (
        plan,
        _canonical_sha256(persisted.model_dump(mode="json")),
    )


def required_minute_sessions(
    plan: MinuteBackfillPlan,
) -> tuple[MinuteRepairSession, ...]:
    """Return the exact persisted repair scope, excluding accepted absences."""
    if not plan.windows:
        raise ValueError("minute repair requires at least one persisted window")
    desired = {
        MinuteRepairSession(ts_code=window.ts_code, trade_date=trade_date)
        for window in plan.windows
        for trade_date in window.open_dates
    }
    if len(desired) != plan.coverage.expected_unique_sessions:
        raise ValueError(
            "persisted minute windows disagree with expected unique coverage"
        )
    unavailable = {
        MinuteRepairSession(ts_code=row.ts_code, trade_date=row.trade_date)
        for row in plan.unavailable_sessions
    }
    if not unavailable <= desired:
        raise ValueError("persisted unavailable sessions fall outside minute windows")
    if len(unavailable) != plan.coverage.accepted_missing_unique_sessions:
        raise ValueError(
            "persisted unavailable sessions disagree with accepted missing coverage"
        )
    return tuple(
        sorted(
            desired - unavailable,
            key=lambda row: (row.trade_date, row.ts_code),
        )
    )


def assess_minute_repair_scope(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    plan: MinuteBackfillPlan,
    as_of_time: datetime,
) -> MinuteRepairScope:
    """Compare the immutable manifest scope against lake and source coverage."""
    if as_of_time.tzinfo is None or as_of_time.utcoffset() is None:
        raise ValueError("minute repair as_of_time must be timezone-aware")
    source_database = Path(source_database)
    if not source_database.is_file() or source_database.is_symlink():
        raise ValueError(
            f"minute repair source database is invalid: {source_database}"
        )
    if not paths.catalog_path.is_file() or paths.catalog_path.is_symlink():
        raise ValueError("minute repair research catalog is invalid")

    required = required_minute_sessions(plan)
    required_keys = {(row.ts_code, row.trade_date) for row in required}
    session_spec = minute_session_spec()
    lake_complete, artifacts = _complete_minute_sessions_from_lake(
        plan.windows,
        session_spec,
        catalog=ResearchCatalog(paths.catalog_path, read_only=True),
        lake_root=paths.lake_root,
        as_of_time=as_of_time,
    )
    lake_required = required_keys & lake_complete
    missing_keys = required_keys - lake_required
    with DuckDBStore(source_database, read_only=True) as source:
        operational_complete = _complete_minute_sessions(
            source,
            plan.windows,
            session_spec,
        )
    source_complete = missing_keys & operational_complete
    incomplete = missing_keys - source_complete
    if incomplete:
        rendered = ", ".join(
            f"{ts_code}@{trade_date.isoformat()}"
            for ts_code, trade_date in sorted(
                incomplete,
                key=lambda row: (row[1], row[0]),
            )[:5]
        )
        raise ValueError(
            "minute repair operational source is incomplete: " + rendered
        )
    missing = tuple(
        MinuteRepairSession(ts_code=ts_code, trade_date=trade_date)
        for ts_code, trade_date in sorted(
            missing_keys,
            key=lambda row: (row[1], row[0]),
        )
    )
    return MinuteRepairScope(
        required_session_count=len(required),
        unavailable_session_count=len(plan.unavailable_sessions),
        lake_complete_session_count=len(lake_required),
        missing_sessions=missing,
        source_complete_session_count=len(source_complete),
        minute_coverage_artifacts=artifacts,
    )


def _minute_partition_key(trade_date: date) -> ResearchPartitionKey:
    return ResearchPartitionKey(
        dataset="minute_bar",
        trade_date=trade_date,
        freq="1min",
    )


def _load_existing_minute_manifest(
    paths: ResearchIngestPaths,
    trade_date: date,
) -> tuple[ResearchPartitionManifest | None, str | None]:
    key = _minute_partition_key(trade_date)
    manifest_path = paths.lake_root / partition_manifest_relative_path(key)
    with duckdb.connect(str(paths.catalog_path), read_only=True) as catalog:
        row = catalog.execute(
            """
            SELECT relative_path, content_hash, file_hash, manifest_json
            FROM research_partition
            WHERE partition_id = ?
            """,
            [key.partition_id],
        ).fetchone()
    if not manifest_path.exists():
        if row is not None:
            raise RuntimeError(
                f"research catalog has minute partition without manifest: {trade_date}"
            )
        return None, None
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise RuntimeError(f"invalid minute manifest path: {manifest_path}")
    manifest = ResearchPartitionManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    if manifest.partition != key or manifest.dataset != "minute_bar":
        raise RuntimeError(f"minute manifest partition mismatch: {trade_date}")
    if row is None:
        raise RuntimeError(
            f"minute manifest is missing from research catalog: {trade_date}"
        )
    catalog_manifest = ResearchPartitionManifest.model_validate_json(row[3])
    if (
        str(row[0]) != manifest.relative_path
        or str(row[1]) != manifest.content_hash
        or str(row[2]) != manifest.file_hash
        or catalog_manifest != manifest
    ):
        raise RuntimeError(f"minute catalog/manifest mismatch: {trade_date}")
    return manifest, _file_sha256(manifest_path)


def _query_operational_minute_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    trade_date: date,
    ts_codes: tuple[str, ...],
) -> pd.DataFrame:
    if not ts_codes:
        raise ValueError("minute repair source query requires target codes")
    selected = ", ".join(_MINUTE_COLUMNS)
    return connection.execute(
        f"""
        SELECT {selected}
        FROM minute_bar
        WHERE CAST(trade_time AS DATE) = ?
          AND freq = '1min'
          AND source = 'tushare'
          AND ts_code = ANY(?)
        ORDER BY ts_code, trade_time, freq, source
        """,
        [trade_date, list(ts_codes)],
    ).fetchdf()


def build_research_minute_repair_plan(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    state: BackfillStateStore,
    manifest_id: str,
    code_commit: str,
    as_of_time: datetime,
) -> ResearchMinuteRepairPlan:
    """Build a read-only content-bound minute repair plan."""
    return _build_prepared_minute_repair(
        source_database=source_database,
        paths=paths,
        state=state,
        manifest_id=manifest_id,
        code_commit=code_commit,
        as_of_time=as_of_time,
    ).plan


def _build_prepared_minute_repair(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    state: BackfillStateStore,
    manifest_id: str,
    code_commit: str,
    as_of_time: datetime,
) -> _PreparedMinuteRepair:
    if re.fullmatch(r"[0-9a-f]{40}", code_commit) is None:
        raise ValueError("minute repair requires a clean 40-character code commit")
    plan, manifest_content_sha256 = _load_completed_backfill_binding(
        state,
        manifest_id,
    )
    (
        bootstrap_snapshot_id,
        previous,
        previous_observation_sha256,
        catalog_sha256,
    ) = _validate_prior_authority(paths)
    if previous is None or previous_observation_sha256 is None:
        raise RuntimeError("minute repair requires an established current authority")
    if previous.bootstrap_snapshot_id != bootstrap_snapshot_id:
        raise RuntimeError("minute repair bootstrap lineage mismatch")
    if previous.readonly_catalog_sha256 is None:
        raise RuntimeError("minute repair authority lacks readonly catalog hash")

    scope = assess_minute_repair_scope(
        source_database=source_database,
        paths=paths,
        plan=plan,
        as_of_time=as_of_time,
    )
    if scope.missing_sessions and (
        max(row.trade_date for row in scope.missing_sessions) > previous.trade_date
    ):
        raise ValueError(
            "minute repair target is after current authority trade date"
        )
    sessions_by_date: dict[date, list[MinuteRepairSession]] = defaultdict(list)
    for session in scope.missing_sessions:
        sessions_by_date[session.trade_date].append(session)

    day_plans: list[ResearchMinuteRepairDayPlan] = []
    merged_by_date: dict[date, pd.DataFrame] = {}
    existing_manifests: dict[date, ResearchPartitionManifest | None] = {}
    source_database = Path(source_database)
    with duckdb.connect(str(source_database), read_only=True) as source:
        for trade_date in sorted(sessions_by_date):
            targets = tuple(
                sorted(
                    sessions_by_date[trade_date],
                    key=lambda row: row.ts_code,
                )
            )
            existing_manifest, existing_manifest_sha256 = (
                _load_existing_minute_manifest(paths, trade_date)
            )
            existing = _query_existing_research_partition(
                paths,
                _minute_partition_key(trade_date),
                _MINUTE_COLUMNS,
            )
            operational = _query_operational_minute_rows(
                source,
                trade_date=trade_date,
                ts_codes=tuple(row.ts_code for row in targets),
            )
            day_plan, merged = build_minute_repair_day_plan(
                trade_date=trade_date,
                target_sessions=targets,
                existing_manifest_sha256=existing_manifest_sha256,
                existing=existing,
                operational=operational,
            )
            day_plans.append(day_plan)
            merged_by_date[trade_date] = merged
            existing_manifests[trade_date] = existing_manifest

    required = required_minute_sessions(plan)
    missing_set = set(scope.missing_sessions)
    lake_complete_sessions = tuple(
        row for row in required if row not in missing_set
    )
    window_scope_sha256 = _canonical_sha256(
        [
            window.model_dump(mode="json")
            for window in sorted(
                plan.windows,
                key=lambda row: (
                    row.ts_code,
                    row.start_date,
                    row.end_date,
                ),
            )
        ]
    )
    unavailable_sessions_sha256 = _canonical_sha256(
        [
            row.model_dump(mode="json")
            for row in sorted(
                plan.unavailable_sessions,
                key=lambda item: (
                    item.trade_date,
                    item.ts_code,
                    item.reason,
                ),
            )
        ]
    )
    repair_plan = ResearchMinuteRepairPlan(
        code_commit=code_commit,
        manifest_id=manifest_id,
        manifest_content_sha256=manifest_content_sha256,
        strategy_id=plan.manifest.spec.strategy_id,
        strategy_version=plan.manifest.spec.strategy_version,
        window_scope_sha256=window_scope_sha256,
        unavailable_sessions_sha256=unavailable_sessions_sha256,
        authority_current_sha256=previous_observation_sha256,
        catalog_sha256=catalog_sha256,
        readonly_catalog_sha256=previous.readonly_catalog_sha256,
        required_session_count=scope.required_session_count,
        unavailable_session_count=scope.unavailable_session_count,
        lake_complete_session_count=scope.lake_complete_session_count,
        missing_session_count=len(scope.missing_sessions),
        source_complete_session_count=scope.source_complete_session_count,
        required_sessions_sha256=hash_minute_sessions(required),
        missing_sessions_sha256=hash_minute_sessions(scope.missing_sessions),
        lake_complete_sessions_sha256=hash_minute_sessions(
            lake_complete_sessions
        ),
        source_complete_sessions_sha256=hash_minute_sessions(
            scope.missing_sessions
        ),
        affected_ts_codes=tuple(
            sorted({row.ts_code for row in scope.missing_sessions})
        ),
        days=tuple(day_plans),
    )
    return _PreparedMinuteRepair(
        plan=repair_plan,
        previous=previous,
        previous_observation_sha256=previous_observation_sha256,
        merged_by_date=merged_by_date,
        existing_manifests=existing_manifests,
    )


def _verify_plan_baseline(
    paths: ResearchIngestPaths,
    plan: ResearchMinuteRepairPlan,
) -> None:
    current_path = paths.state_dir / "research-authority-current.json"
    observed = {
        "authority current": (
            _file_sha256(current_path) if current_path.is_file() else None
        ),
        "catalog": (
            _file_sha256(paths.catalog_path)
            if paths.catalog_path.is_file()
            else None
        ),
        "readonly catalog": (
            _file_sha256(paths.readonly_catalog_path)
            if paths.readonly_catalog_path.is_file()
            else None
        ),
    }
    expected = {
        "authority current": plan.authority_current_sha256,
        "catalog": plan.catalog_sha256,
        "readonly catalog": plan.readonly_catalog_sha256,
    }
    for label, expected_hash in expected.items():
        if observed[label] != expected_hash:
            raise RuntimeError(f"minute repair {label} changed after planning")
    for day in plan.days:
        manifest_path = paths.lake_root / partition_manifest_relative_path(
            _minute_partition_key(day.trade_date)
        )
        if manifest_path.is_symlink() or (
            manifest_path.exists() and not manifest_path.is_file()
        ):
            raise RuntimeError(
                f"invalid minute manifest path: {day.trade_date}"
            )
        manifest_hash = (
            _file_sha256(manifest_path) if manifest_path.is_file() else None
        )
        if manifest_hash != day.existing_manifest_sha256:
            raise RuntimeError(
                f"minute repair manifest changed after planning: {day.trade_date}"
            )


def _build_minute_export_source(
    merged_by_date: dict[date, pd.DataFrame],
) -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE trade_calendar (
            exchange VARCHAR NOT NULL,
            cal_date DATE NOT NULL,
            is_open BOOLEAN NOT NULL,
            PRIMARY KEY (exchange, cal_date)
        );
        CREATE TABLE minute_bar (
            ts_code VARCHAR NOT NULL,
            trade_time TIMESTAMP NOT NULL,
            freq VARCHAR NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            source VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (ts_code, trade_time, freq, source)
        );
        """
    )
    connection.executemany(
        "INSERT INTO trade_calendar VALUES ('SSE', ?, TRUE)",
        [(trade_date,) for trade_date in sorted(merged_by_date)],
    )
    combined = pd.concat(
        [
            merged_by_date[trade_date]
            for trade_date in sorted(merged_by_date)
        ],
        ignore_index=True,
    )
    connection.register("minute_repair_input", combined)
    selected = ", ".join(_MINUTE_COLUMNS)
    connection.execute(
        f"INSERT INTO minute_bar ({selected}) "
        f"SELECT {selected} FROM minute_repair_input"
    )
    connection.unregister("minute_repair_input")
    return connection


def _prepare_repair_generation(
    paths: ResearchIngestPaths,
    *,
    prepared: _PreparedMinuteRepair,
    transaction_root: Path,
    generated_at: datetime,
) -> tuple[Path, Path, dict[date, ResearchPartitionManifest]]:
    _mkdir_durable(transaction_root)
    staged_catalog = transaction_root / "catalog.next.duckdb"
    staged_lake = transaction_root / "lake.next"
    with ExitStack() as stack:
        for day in prepared.plan.days:
            stack.enter_context(
                exclusive_file_lock(
                    paths.lake_root
                    / partition_directory(_minute_partition_key(day.trade_date))
                    / ".export.lock"
                )
            )
        stack.enter_context(
            exclusive_file_lock(ResearchCatalog(paths.catalog_path).lock_path)
        )
        _verify_plan_baseline(paths, prepared.plan)
        shutil.copyfile(paths.catalog_path, staged_catalog)
        with staged_catalog.open("rb") as handle:
            os.fsync(handle.fileno())
        for day in prepared.plan.days:
            key = _minute_partition_key(day.trade_date)
            live_partition = paths.lake_root / partition_directory(key)
            staged_partition = staged_lake / partition_directory(key)
            if live_partition.is_dir():
                shutil.copytree(live_partition, staged_partition)

    export_source = _build_minute_export_source(prepared.merged_by_date)
    try:
        trade_dates = tuple(day.trade_date for day in prepared.plan.days)
        summary = export_research_dataset(
            export_source,
            catalog=ResearchCatalog(staged_catalog),
            lake_root=staged_lake,
            dataset="minute_bar",
            start_date=min(trade_dates),
            end_date=max(trade_dates),
            code_commit=prepared.plan.code_commit,
            now=lambda: generated_at.astimezone(UTC),
            as_of_date=max(trade_dates),
        )
    finally:
        export_source.close()
    manifests = {
        partition.trade_date: partition.manifest
        for partition in summary.partitions
        if partition.manifest is not None
    }
    expected_dates = {day.trade_date for day in prepared.plan.days}
    if set(manifests) != expected_dates:
        raise RuntimeError(
            "minute repair staged export did not cover every target date"
        )
    staged_readonly = transaction_root / "readonly.next.duckdb"
    _prepare_readonly_generation(staged_catalog, staged_readonly)
    return staged_catalog, staged_readonly, manifests


def _observation_id(generated_at: datetime) -> str:
    stamp = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"research-minute-repair-{stamp}-{uuid.uuid4().hex[:8]}"


def _repair_observation_path(
    paths: ResearchIngestPaths,
    observation: ResearchMinuteRepairObservation,
) -> Path:
    return (
        paths.state_dir
        / "research_observations"
        / f"trade_date={observation.trade_date.isoformat()}"
        / f"{observation.observation_id}.json"
    )


def _build_repair_observation(
    paths: ResearchIngestPaths,
    *,
    prepared: _PreparedMinuteRepair,
    staged_catalog: Path,
    staged_readonly: Path,
    manifests: dict[date, ResearchPartitionManifest],
    generated_at: datetime,
) -> ResearchMinuteRepairObservation:
    bootstrap_snapshot_id = prepared.previous.bootstrap_snapshot_id
    if bootstrap_snapshot_id is None:
        raise RuntimeError("minute repair authority lacks bootstrap lineage")
    changes: list[ResearchMinuteRepairPartitionChange] = []
    for day in prepared.plan.days:
        if not day.changed:
            continue
        manifest = manifests[day.trade_date]
        staged_manifest_path = (
            staged_catalog.parent
            / "lake.next"
            / partition_manifest_relative_path(
                _minute_partition_key(day.trade_date)
            )
        )
        existing = prepared.existing_manifests[day.trade_date]
        changes.append(
            ResearchMinuteRepairPartitionChange(
                trade_date=day.trade_date,
                before_manifest_sha256=day.existing_manifest_sha256,
                after_manifest_sha256=_file_sha256(staged_manifest_path),
                before_content_hash=(
                    None if existing is None else existing.content_hash
                ),
                before_manifest=existing,
                after_manifest=manifest,
            )
        )
    if not changes:
        raise RuntimeError("unchanged minute repair cannot publish an observation")
    return ResearchMinuteRepairObservation(
        observation_id=_observation_id(generated_at),
        bootstrap_snapshot_id=bootstrap_snapshot_id,
        trade_date=prepared.previous.trade_date,
        generated_at=generated_at,
        code_commit=prepared.plan.code_commit,
        manifest_id=prepared.plan.manifest_id,
        plan_id=prepared.plan.plan_id,
        previous_observation_sha256=prepared.previous_observation_sha256,
        catalog_before_sha256=prepared.plan.catalog_sha256,
        catalog_sha256=_file_sha256(staged_catalog),
        readonly_catalog_before_sha256=(
            prepared.plan.readonly_catalog_sha256
        ),
        readonly_catalog_sha256=_file_sha256(staged_readonly),
        repairs=tuple(changes),
    )


def _journal_path(transaction_root: Path) -> Path:
    return transaction_root / "minute-repair-journal.json"


def _model_sha256(model: BaseModel) -> str:
    payload = (model.model_dump_json(indent=2) + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _backup_file(source: Path, target: Path, expected_hash: str) -> None:
    shutil.copyfile(source, target)
    with target.open("rb") as handle:
        os.fsync(handle.fileno())
    if _file_sha256(target) != expected_hash:
        raise RuntimeError(f"minute repair backup verification failed: {source}")


def _prepare_repair_journal(
    paths: ResearchIngestPaths,
    *,
    prepared: _PreparedMinuteRepair,
    transaction_root: Path,
    staged_catalog: Path,
    staged_readonly: Path,
    observation: ResearchMinuteRepairObservation,
) -> _ResearchMinuteRepairPublishJournal:
    observation_path = _repair_observation_path(paths, observation)
    if observation_path.exists():
        raise RuntimeError("minute repair observation path already exists")
    current_path = paths.state_dir / "research-authority-current.json"
    before_files = (
        (paths.catalog_path, transaction_root / "catalog.before"),
        (paths.readonly_catalog_path, transaction_root / "readonly.before"),
        (current_path, transaction_root / "current.before"),
    )
    before_hashes = (
        prepared.plan.catalog_sha256,
        prepared.plan.readonly_catalog_sha256,
        prepared.plan.authority_current_sha256,
    )
    for (source, backup), expected_hash in zip(
        before_files,
        before_hashes,
        strict=True,
    ):
        if not source.is_file() or source.is_symlink():
            raise RuntimeError(
                f"minute repair publish target is invalid: {source}"
            )
        if _file_sha256(source) != expected_hash:
            raise RuntimeError(
                f"minute repair publish baseline changed: {source}"
            )
        _backup_file(source, backup, expected_hash)

    entries: list[_ResearchMinuteRepairJournalEntry] = []
    for change in observation.repairs:
        key = _minute_partition_key(change.trade_date)
        live_manifest = paths.lake_root / partition_manifest_relative_path(key)
        staged_manifest = (
            transaction_root
            / "lake.next"
            / partition_manifest_relative_path(key)
        )
        if live_manifest.is_symlink() or (
            live_manifest.exists() and not live_manifest.is_file()
        ):
            raise RuntimeError(
                f"invalid minute manifest path: {change.trade_date}"
            )
        manifest_existed = live_manifest.is_file()
        before_hash = (
            _file_sha256(live_manifest) if manifest_existed else None
        )
        if before_hash != change.before_manifest_sha256:
            raise RuntimeError(
                f"minute repair manifest baseline changed: {change.trade_date}"
            )
        if manifest_existed:
            _backup_file(
                live_manifest,
                transaction_root
                / f"manifest-{change.trade_date.isoformat()}.before",
                before_hash,
            )
        manifest = change.after_manifest
        live_version = paths.lake_root / manifest.relative_path
        if live_version.is_symlink() or (
            live_version.exists()
            and (
                not live_version.is_file()
                or _file_sha256(live_version) != manifest.file_hash
            )
        ):
            raise RuntimeError(
                "existing immutable minute repair version hash mismatch"
            )
        entries.append(
            _ResearchMinuteRepairJournalEntry(
                trade_date=change.trade_date,
                manifest_existed=manifest_existed,
                manifest_before_sha256=before_hash,
                manifest_after_sha256=_file_sha256(staged_manifest),
                version_relative_path=manifest.relative_path,
                version_existed=live_version.is_file(),
                version_sha256=manifest.file_hash,
            )
        )
    journal = _ResearchMinuteRepairPublishJournal(
        transaction_id=transaction_root.name,
        observation_path=observation_path,
        catalog_before_sha256=prepared.plan.catalog_sha256,
        catalog_after_sha256=_file_sha256(staged_catalog),
        readonly_before_sha256=prepared.plan.readonly_catalog_sha256,
        readonly_after_sha256=_file_sha256(staged_readonly),
        current_before_sha256=prepared.plan.authority_current_sha256,
        current_after_sha256=_model_sha256(observation),
        entries=tuple(entries),
    )
    _write_model_atomic(_journal_path(transaction_root), journal)
    return journal


def _publish_step_hook(step: str) -> None:
    del step


def _require_outside_market_protection(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("minute repair clock must be timezone-aware")
    local = value.astimezone(_CST)
    if (
        local.weekday() < 5
        and _MARKET_PROTECTION_START
        <= local.time()
        <= _MARKET_PROTECTION_END
    ):
        raise ValueError(
            "minute repair is forbidden during market protection window"
        )
    return local


def _publish_repair_generation(
    paths: ResearchIngestPaths,
    *,
    prepared: _PreparedMinuteRepair,
    transaction_root: Path,
    staged_catalog: Path,
    staged_readonly: Path,
    observation: ResearchMinuteRepairObservation,
    publish_guard: Callable[[], None],
) -> None:
    try:
        with ExitStack() as stack:
            for change in observation.repairs:
                stack.enter_context(
                    exclusive_file_lock(
                        paths.lake_root
                        / partition_directory(
                            _minute_partition_key(change.trade_date)
                        )
                        / ".export.lock"
                    )
                )
            stack.enter_context(
                exclusive_file_lock(
                    ResearchCatalog(paths.catalog_path).lock_path
                )
            )
            _verify_plan_baseline(paths, prepared.plan)
            publish_guard()
            _prepare_repair_journal(
                paths,
                prepared=prepared,
                transaction_root=transaction_root,
                staged_catalog=staged_catalog,
                staged_readonly=staged_readonly,
                observation=observation,
            )
            publish_guard()
            for change in observation.repairs:
                publish_guard()
                staged_data = (
                    transaction_root
                    / "lake.next"
                    / change.after_manifest.relative_path
                )
                live_data = (
                    paths.lake_root / change.after_manifest.relative_path
                )
                if live_data.is_symlink() or (
                    live_data.exists()
                    and (
                        not live_data.is_file()
                        or _file_sha256(live_data)
                        != change.after_manifest.file_hash
                    )
                ):
                    raise RuntimeError(
                        "existing immutable minute repair version "
                        "hash mismatch"
                    )
                if not live_data.exists():
                    _copy_file_atomic(staged_data, live_data)
            _publish_step_hook("versions_published")
            publish_guard()
            for change in observation.repairs:
                publish_guard()
                relative_manifest = partition_manifest_relative_path(
                    _minute_partition_key(change.trade_date)
                )
                _copy_file_atomic(
                    transaction_root / "lake.next" / relative_manifest,
                    paths.lake_root / relative_manifest,
                )
            _publish_step_hook("manifests_published")
            publish_guard()
            _copy_file_atomic(staged_catalog, paths.catalog_path)
            _publish_step_hook("catalog_published")
            publish_guard()
            _copy_file_atomic(
                staged_readonly,
                paths.readonly_catalog_path,
            )
            _publish_step_hook("readonly_published")
            publish_guard()
            _write_model_atomic(
                _repair_observation_path(paths, observation),
                observation,
            )
            publish_guard()
            _write_model_atomic(
                paths.state_dir / "research-authority-current.json",
                observation,
            )
            _publish_step_hook("authority_published")
        _journal_path(transaction_root).unlink()
        _fsync_directory(transaction_root)
        _remove_transaction_root(transaction_root)
    except BaseException:
        if _journal_path(transaction_root).exists():
            try:
                rollback_research_minute_repair_publish(
                    paths,
                    transaction_root,
                )
            except Exception as rollback_error:
                raise RuntimeError(
                    "minute repair publish failed and rollback is pending: "
                    f"{rollback_error}"
                ) from rollback_error
        raise


def _resolved_version_path(
    paths: ResearchIngestPaths,
    relative_path: str,
) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise RuntimeError("minute repair journal version path escaped lake root")
    root = paths.lake_root.resolve()
    if paths.lake_root.is_symlink() or not paths.lake_root.is_dir():
        raise RuntimeError("minute repair lake root is invalid")
    cursor = paths.lake_root
    for part in relative.parts[:-1]:
        cursor = cursor / part
        if cursor.is_symlink():
            raise RuntimeError(
                "minute repair journal version path contains symlink"
            )
    candidate = paths.lake_root / relative
    resolved_parent = candidate.parent.resolve()
    if resolved_parent != root and root not in resolved_parent.parents:
        raise RuntimeError("minute repair journal version path escaped lake root")
    return candidate


def rollback_research_minute_repair_publish(
    paths: ResearchIngestPaths,
    transaction_root: Path,
) -> None:
    """Rollback a journaled minute repair after preflighting every CAS target."""
    journal_path = _journal_path(transaction_root)
    if not journal_path.is_file() or journal_path.is_symlink():
        raise RuntimeError(f"minute repair journal is invalid: {journal_path}")
    journal = _ResearchMinuteRepairPublishJournal.model_validate_json(
        journal_path.read_text(encoding="utf-8")
    )
    if journal.transaction_id != transaction_root.name:
        raise RuntimeError("minute repair journal transaction mismatch")
    catalog_targets = (
        (
            paths.catalog_path,
            transaction_root / "catalog.before",
            journal.catalog_before_sha256,
            journal.catalog_after_sha256,
        ),
        (
            paths.readonly_catalog_path,
            transaction_root / "readonly.before",
            journal.readonly_before_sha256,
            journal.readonly_after_sha256,
        ),
        (
            paths.state_dir / "research-authority-current.json",
            transaction_root / "current.before",
            journal.current_before_sha256,
            journal.current_after_sha256,
        ),
    )
    observations_root = (paths.state_dir / "research_observations").resolve()
    if observations_root not in journal.observation_path.resolve().parents:
        raise RuntimeError("minute repair observation path escaped state root")

    with exclusive_file_lock(ResearchCatalog(paths.catalog_path).lock_path):
        for target, backup, before_hash, after_hash in catalog_targets:
            if target.is_symlink() or (
                target.exists() and not target.is_file()
            ):
                raise RuntimeError(
                    f"minute repair rollback CAS mismatch: {target}"
                )
            if (
                not backup.is_file()
                or backup.is_symlink()
                or _file_sha256(backup) != before_hash
            ):
                raise RuntimeError(
                    f"minute repair rollback backup mismatch: {backup}"
                )
            observed_hash = (
                _file_sha256(target) if target.is_file() else None
            )
            if observed_hash not in {before_hash, after_hash}:
                raise RuntimeError(
                    f"minute repair rollback CAS mismatch: {target}"
                )
        for entry in journal.entries:
            key = _minute_partition_key(entry.trade_date)
            manifest_path = (
                paths.lake_root / partition_manifest_relative_path(key)
            )
            if manifest_path.is_symlink() or (
                manifest_path.exists() and not manifest_path.is_file()
            ):
                raise RuntimeError(
                    "minute repair rollback manifest CAS mismatch: "
                    f"{entry.trade_date}"
                )
            observed_manifest_hash = (
                _file_sha256(manifest_path)
                if manifest_path.is_file()
                else None
            )
            allowed_manifest_hashes = (
                {
                    entry.manifest_before_sha256,
                    entry.manifest_after_sha256,
                }
                if entry.manifest_existed
                else {None, entry.manifest_after_sha256}
            )
            if observed_manifest_hash not in allowed_manifest_hashes:
                raise RuntimeError(
                    "minute repair rollback manifest CAS mismatch: "
                    f"{entry.trade_date}"
                )
            if entry.manifest_existed:
                backup = (
                    transaction_root
                    / f"manifest-{entry.trade_date.isoformat()}.before"
                )
                if (
                    not backup.is_file()
                    or backup.is_symlink()
                    or _file_sha256(backup)
                    != entry.manifest_before_sha256
                ):
                    raise RuntimeError(
                        "minute repair rollback manifest backup mismatch"
                    )
            version_path = _resolved_version_path(
                paths,
                entry.version_relative_path,
            )
            if version_path.is_symlink() or (
                version_path.exists() and not version_path.is_file()
            ):
                raise RuntimeError(
                    "minute repair rollback version CAS mismatch"
                )
            observed_version_hash = (
                _file_sha256(version_path)
                if version_path.is_file()
                else None
            )
            if entry.version_existed:
                if observed_version_hash != entry.version_sha256:
                    raise RuntimeError(
                        "minute repair rollback version CAS mismatch"
                    )
            elif observed_version_hash not in {
                None,
                entry.version_sha256,
            }:
                raise RuntimeError(
                    "minute repair rollback version CAS mismatch"
                )
        if journal.observation_path.is_symlink() or (
            journal.observation_path.exists()
            and not journal.observation_path.is_file()
        ):
            raise RuntimeError(
                "minute repair rollback observation CAS mismatch"
            )
        observation_hash = (
            _file_sha256(journal.observation_path)
            if journal.observation_path.is_file()
            else None
        )
        if observation_hash not in {None, journal.current_after_sha256}:
            raise RuntimeError(
                "minute repair rollback observation CAS mismatch"
            )

        for target, backup, before_hash, after_hash in catalog_targets:
            _restore_file(
                target,
                backup,
                True,
                expected_before_hash=before_hash,
                expected_after_hash=after_hash,
            )
        for entry in journal.entries:
            manifest_path = (
                paths.lake_root
                / partition_manifest_relative_path(
                    _minute_partition_key(entry.trade_date)
                )
            )
            _restore_file(
                manifest_path,
                transaction_root
                / f"manifest-{entry.trade_date.isoformat()}.before",
                entry.manifest_existed,
                expected_before_hash=entry.manifest_before_sha256,
                expected_after_hash=entry.manifest_after_sha256,
            )
            if not entry.version_existed:
                version_path = _resolved_version_path(
                    paths,
                    entry.version_relative_path,
                )
                if (
                    version_path.is_file()
                    and _file_sha256(version_path) != entry.version_sha256
                ):
                    raise RuntimeError(
                        "minute repair rollback version CAS mismatch"
                    )
                version_path.unlink(missing_ok=True)
                if version_path.parent.exists():
                    _fsync_directory(version_path.parent)
        journal.observation_path.unlink(missing_ok=True)
        if journal.observation_path.parent.exists():
            _fsync_directory(journal.observation_path.parent)
    _remove_transaction_root(transaction_root)


def run_research_minute_repair(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    state: BackfillStateStore,
    manifest_id: str,
    code_commit: str,
    apply: bool = False,
    plan_id: str | None = None,
    now: Callable[[], datetime] | None = None,
) -> ResearchMinuteRepairResult:
    """Plan or atomically publish a content-bound historical minute repair."""
    clock = now or (lambda: datetime.now(_CST))
    generated_at = clock()
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("minute repair clock must be timezone-aware")
    generated_at = generated_at.astimezone(_CST)
    if apply and (
        plan_id is None or re.fullmatch(_HASH_PATTERN, plan_id) is None
    ):
        raise ValueError("minute repair apply requires a 64-character plan_id")
    if not apply:
        prepared = _build_prepared_minute_repair(
            source_database=source_database,
            paths=paths,
            state=state,
            manifest_id=manifest_id,
            code_commit=code_commit,
            as_of_time=generated_at,
        )
        return ResearchMinuteRepairResult(
            status=(
                "unchanged"
                if prepared.plan.missing_session_count == 0
                else "planned"
            ),
            plan=prepared.plan,
        )

    _mkdir_durable(paths.state_dir)
    with exclusive_file_lock(paths.publish_lock_path):
        _recover_interrupted_publish(paths)

        def publish_guard() -> None:
            _require_outside_market_protection(clock())

        publish_guard()
        prepared = _build_prepared_minute_repair(
            source_database=source_database,
            paths=paths,
            state=state,
            manifest_id=manifest_id,
            code_commit=code_commit,
            as_of_time=generated_at,
        )
        if prepared.plan.plan_id != plan_id:
            raise ValueError(
                "stale minute repair plan: rerun preview and apply the new plan_id"
            )
        if (
            prepared.plan.missing_session_count == 0
            or not any(day.changed for day in prepared.plan.days)
        ):
            return ResearchMinuteRepairResult(
                status="unchanged",
                plan=prepared.plan,
            )
        _mkdir_durable(paths.transactions_root)
        transaction_root = (
            paths.transactions_root / f"minute-repair-{uuid.uuid4().hex}"
        )
        try:
            staged_catalog, staged_readonly, manifests = (
                _prepare_repair_generation(
                    paths,
                    prepared=prepared,
                    transaction_root=transaction_root,
                    generated_at=generated_at,
                )
            )
            observation = _build_repair_observation(
                paths,
                prepared=prepared,
                staged_catalog=staged_catalog,
                staged_readonly=staged_readonly,
                manifests=manifests,
                generated_at=generated_at,
            )
            _publish_repair_generation(
                paths,
                prepared=prepared,
                transaction_root=transaction_root,
                staged_catalog=staged_catalog,
                staged_readonly=staged_readonly,
                observation=observation,
                publish_guard=publish_guard,
            )
        finally:
            if (
                transaction_root.exists()
                and not _journal_path(transaction_root).exists()
            ):
                _remove_transaction_root(transaction_root)
        return ResearchMinuteRepairResult(
            status="candidate",
            plan=prepared.plan,
            observation=observation,
        )
