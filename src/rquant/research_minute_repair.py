"""Governed historical minute research-lake repair."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path

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
from rquant.research_catalog import ResearchCatalog
from rquant.research_ingest import ResearchIngestPaths
from rquant.research_ingest import (
    _file_sha256,
    _query_existing_research_partition,
    _validate_prior_authority,
)
from rquant.research_lake import (
    ResearchPartitionKey,
    ResearchPartitionManifest,
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


class _MinuteRepairModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class MinuteRepairSession(_MinuteRepairModel):
    ts_code: str = Field(pattern=r"^\d{6}\.(?:SZ|SH|BJ)$")
    trade_date: date


class MinuteRepairScope(_MinuteRepairModel):
    required_session_count: int = Field(ge=1)
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
    schema_version: int = Field(default=1, frozen=True)
    action_id: str = Field(default=_ACTION_ID, frozen=True)
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    manifest_id: str = Field(pattern=_HASH_PATTERN)
    manifest_content_sha256: str = Field(pattern=_HASH_PATTERN)
    strategy_id: str = Field(min_length=1)
    strategy_version: str = Field(min_length=1)
    authority_current_sha256: str = Field(pattern=_HASH_PATTERN)
    catalog_sha256: str = Field(pattern=_HASH_PATTERN)
    readonly_catalog_sha256: str = Field(pattern=_HASH_PATTERN)
    required_session_count: int = Field(ge=1)
    unavailable_session_count: int = Field(ge=0)
    lake_complete_session_count: int = Field(ge=0)
    missing_session_count: int = Field(ge=0)
    source_complete_session_count: int = Field(ge=0)
    required_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
    missing_sessions_sha256: str = Field(pattern=_HASH_PATTERN)
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
        catalog=ResearchCatalog(paths.catalog_path),
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
            del existing_manifest
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
            day_plan, _merged = build_minute_repair_day_plan(
                trade_date=trade_date,
                target_sessions=targets,
                existing_manifest_sha256=existing_manifest_sha256,
                existing=existing,
                operational=operational,
            )
            day_plans.append(day_plan)

    required = required_minute_sessions(plan)
    return ResearchMinuteRepairPlan(
        code_commit=code_commit,
        manifest_id=manifest_id,
        manifest_content_sha256=manifest_content_sha256,
        strategy_id=plan.manifest.spec.strategy_id,
        strategy_version=plan.manifest.spec.strategy_version,
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
        days=tuple(day_plans),
    )
