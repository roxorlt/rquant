"""Governed, atomic repair planning for historical auction partitions."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any, Literal, Protocol
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_catalog import ResearchCatalog, exclusive_file_lock
from rquant.research_ingest import (
    ResearchAuctionRepairObservation,
    ResearchAuctionRepairPartitionChange,
    ResearchAuthorityObservation,
    ResearchIngestPaths,
    _copy_file_atomic,
    _daily_bar_universe_is_complete,
    _expected_auction_codes,
    _file_sha256,
    _fsync_directory,
    _mkdir_durable,
    _prepare_readonly_generation,
    _query_existing_research_partition,
    _recover_interrupted_publish,
    _remove_transaction_root,
    _require_open_trade_date,
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

_CST = ZoneInfo("Asia/Shanghai")
_ACTION_ID = "research-auction-history-repair/v1"
_CLEAN_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_TS_CODE_PATTERN = re.compile(r"^\d{6}\.(?:SZ|SH|BJ)$")
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_MINIMUM_COVERAGE_PERCENT = 98
_MARKET_PROTECTION_START = time(9, 15)
_MARKET_PROTECTION_END = time(15, 10)

_AUCTION_COLUMNS = (
    "ts_code",
    "trade_date",
    "auction_type",
    "price",
    "vol",
    "amount",
    "turnover_rate",
    "volume_ratio",
    "source",
    "created_at",
)
_AUCTION_BUSINESS_COLUMNS = tuple(
    column for column in _AUCTION_COLUMNS if column != "created_at"
)
_AUCTION_PHYSICAL_KEY = ("ts_code", "trade_date", "auction_type", "source")
_NUMERIC_COLUMNS = (
    "price",
    "vol",
    "amount",
    "turnover_rate",
    "volume_ratio",
)


class _ResearchRepairModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResearchAuctionRepairQuality(_ResearchRepairModel):
    """Strict Tushare coverage evidence for one historical trade date."""

    expected_code_count: int = Field(gt=0)
    observed_code_count: int = Field(gt=0)
    valid_code_count: int = Field(ge=0)
    expected_valid_code_count: int = Field(ge=0)
    expected_observed_code_count: int = Field(ge=0)
    unexpected_code_count: int = Field(ge=0)
    passed: bool
    issues: tuple[str, ...]

    @model_validator(mode="after")
    def validate_quality(self) -> ResearchAuctionRepairQuality:
        if self.observed_code_count < self.valid_code_count:
            raise ValueError("valid code count cannot exceed observed code count")
        if self.expected_code_count < self.expected_valid_code_count:
            raise ValueError("expected valid count cannot exceed expected code count")
        if self.expected_code_count < self.expected_observed_code_count:
            raise ValueError("expected observed count cannot exceed expected code count")
        if self.observed_code_count < self.expected_observed_code_count:
            raise ValueError("expected observed count cannot exceed observed code count")
        if self.unexpected_code_count != (
            self.observed_code_count - self.expected_observed_code_count
        ):
            raise ValueError("unexpected code count is inconsistent")
        if self.passed != (not self.issues):
            raise ValueError("quality passed flag must agree with issues")
        return self


class ResearchAuctionRepairDayPlan(_ResearchRepairModel):
    """Content-bound repair evidence for one auction partition."""

    trade_date: date
    expected_code_count: int = Field(gt=0)
    expected_codes_sha256: str = Field(pattern=_HASH_PATTERN)
    existing_manifest_sha256: str | None = Field(default=None, pattern=_HASH_PATTERN)
    fetched_business_sha256: str = Field(pattern=_HASH_PATTERN)
    merged_business_sha256: str = Field(pattern=_HASH_PATTERN)
    existing_row_count: int = Field(ge=0)
    fetched_row_count: int = Field(gt=0)
    merged_row_count: int = Field(gt=0)
    observed_code_count: int = Field(gt=0)
    valid_code_count: int = Field(ge=0)
    expected_valid_code_count: int = Field(ge=0)
    expected_observed_code_count: int = Field(ge=0)
    unexpected_code_count: int = Field(ge=0)
    changed: bool


class ResearchAuctionRepairPlan(_ResearchRepairModel):
    """Canonical plan whose SHA256 must survive a fresh apply-time rebuild."""

    schema_version: Literal[1] = 1
    action_id: Literal["research-auction-history-repair/v1"] = _ACTION_ID
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    authority_current_sha256: str = Field(pattern=_HASH_PATTERN)
    catalog_sha256: str = Field(pattern=_HASH_PATTERN)
    readonly_catalog_sha256: str = Field(pattern=_HASH_PATTERN)
    days: tuple[ResearchAuctionRepairDayPlan, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_days(self) -> ResearchAuctionRepairPlan:
        dates = tuple(day.trade_date for day in self.days)
        if dates != tuple(sorted(set(dates))):
            raise ValueError("repair plan days must be strictly ordered and unique")
        return self

    @property
    def trade_dates(self) -> tuple[date, ...]:
        return tuple(day.trade_date for day in self.days)

    @property
    def plan_id(self) -> str:
        return _canonical_sha256(self.model_dump(mode="json"))


class ResearchAuctionRepairResult(_ResearchRepairModel):
    status: Literal["planned", "candidate", "unchanged"]
    plan: ResearchAuctionRepairPlan
    observation: ResearchAuctionRepairObservation | None = None

    @model_validator(mode="after")
    def validate_result(self) -> ResearchAuctionRepairResult:
        if self.status == "candidate" and self.observation is None:
            raise ValueError("candidate auction repair requires an observation")
        if self.status != "candidate" and self.observation is not None:
            raise ValueError("non-candidate auction repair cannot contain an observation")
        return self

    @property
    def plan_id(self) -> str:
        return self.plan.plan_id


class ResearchAuctionRepairAdapter(Protocol):
    def stk_auction(self, trade_date: date) -> pd.DataFrame: ...


class _ResearchAuctionRepairJournalEntry(_ResearchRepairModel):
    trade_date: date
    manifest_existed: bool
    manifest_before_sha256: str | None = Field(default=None, pattern=_HASH_PATTERN)
    manifest_after_sha256: str = Field(pattern=_HASH_PATTERN)
    version_relative_path: str
    version_existed: bool
    version_sha256: str = Field(pattern=_HASH_PATTERN)

    @model_validator(mode="after")
    def validate_before_hash(self) -> _ResearchAuctionRepairJournalEntry:
        if self.manifest_existed != (self.manifest_before_sha256 is not None):
            raise ValueError("repair manifest before hash does not match existence")
        return self


class _ResearchAuctionRepairPublishJournal(_ResearchRepairModel):
    schema_version: Literal[1] = 1
    transaction_kind: Literal["auction_repair"] = "auction_repair"
    transaction_id: str
    observation_path: Path
    catalog_before_sha256: str = Field(pattern=_HASH_PATTERN)
    catalog_after_sha256: str = Field(pattern=_HASH_PATTERN)
    readonly_before_sha256: str = Field(pattern=_HASH_PATTERN)
    readonly_after_sha256: str = Field(pattern=_HASH_PATTERN)
    current_before_sha256: str = Field(pattern=_HASH_PATTERN)
    current_after_sha256: str = Field(pattern=_HASH_PATTERN)
    entries: tuple[_ResearchAuctionRepairJournalEntry, ...] = Field(min_length=1)


@dataclass(frozen=True)
class _PreparedAuctionRepair:
    plan: ResearchAuctionRepairPlan
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


def _canonical_scalar(value: object) -> object:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    item = getattr(value, "item", None)
    return item() if callable(item) else value


def normalize_repair_dates(values: Iterable[date]) -> tuple[date, ...]:
    dates = tuple(sorted(set(values)))
    if not dates:
        raise ValueError("auction repair requires at least one target date")
    return dates


def normalize_tushare_auction_rows(
    frame: pd.DataFrame,
    *,
    trade_date: date,
    generated_at: datetime,
) -> pd.DataFrame:
    """Validate one Tushare response and attach the real repair observation time."""
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("auction repair generated_at must be timezone-aware")
    if frame is None or frame.empty:
        raise ValueError("Tushare auction response is empty")
    required = set(_AUCTION_BUSINESS_COLUMNS)
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"Tushare auction response missing columns: {sorted(missing)}")

    normalized = frame[list(_AUCTION_BUSINESS_COLUMNS)].copy()
    normalized["trade_date"] = pd.to_datetime(
        normalized["trade_date"],
        errors="raise",
    ).dt.date
    observed_dates = set(normalized["trade_date"])
    if observed_dates != {trade_date}:
        raise ValueError(f"Tushare auction response contains rows outside target date {trade_date}")
    if set(normalized["source"].astype(str)) != {"tushare"}:
        raise ValueError("Tushare auction response contains an invalid source")
    if set(normalized["auction_type"].astype(str)) != {"open_realtime"}:
        raise ValueError("Tushare auction response contains an invalid auction type")
    if normalized[list(_AUCTION_PHYSICAL_KEY)].isna().any(axis=None):
        raise ValueError("Tushare auction response contains a null physical key")
    if normalized.duplicated(list(_AUCTION_PHYSICAL_KEY), keep=False).any():
        raise ValueError("Tushare auction response contains a duplicate physical key")
    invalid_codes = sorted(
        {
            str(value)
            for value in normalized["ts_code"]
            if _TS_CODE_PATTERN.fullmatch(str(value)) is None
        }
    )
    if invalid_codes:
        raise ValueError(f"Tushare auction response contains invalid ts_code: {invalid_codes}")
    for column in _NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="raise")
    normalized["created_at"] = generated_at.astimezone(_CST).replace(tzinfo=None)
    return (
        normalized[list(_AUCTION_COLUMNS)]
        .sort_values(list(_AUCTION_PHYSICAL_KEY), kind="stable")
        .reset_index(drop=True)
    )


def _meets_percentage(numerator: int, denominator: int, percentage: int) -> bool:
    if denominator <= 0:
        raise ValueError("quality denominator must be positive")
    return numerator * 100 >= denominator * percentage


def assess_tushare_auction_rows(
    frame: pd.DataFrame,
    *,
    expected_codes: set[str],
) -> ResearchAuctionRepairQuality:
    """Apply the two-sided 98% gate using integer cross multiplication."""
    if not expected_codes:
        raise ValueError("auction repair expected universe is empty")
    invalid_expected = sorted(
        code for code in expected_codes if _TS_CODE_PATTERN.fullmatch(code) is None
    )
    if invalid_expected:
        raise ValueError(
            "auction repair expected universe contains invalid codes: "
            f"{invalid_expected}"
        )
    if frame.empty:
        raise ValueError("normalized Tushare auction rows are empty")

    observed_codes = set(frame["ts_code"].astype(str))
    valid_mask = frame["price"].gt(0) & frame["vol"].gt(0)
    valid_codes = set(frame.loc[valid_mask, "ts_code"].astype(str))
    expected_valid_codes = expected_codes & valid_codes
    expected_observed_codes = expected_codes & observed_codes
    issues: list[str] = []
    if not _meets_percentage(
        len(expected_valid_codes),
        len(expected_codes),
        _MINIMUM_COVERAGE_PERCENT,
    ):
        issues.append("tushare_valid_coverage_below_98pct")
    if not _meets_percentage(
        len(expected_observed_codes),
        len(observed_codes),
        _MINIMUM_COVERAGE_PERCENT,
    ):
        issues.append("tushare_observed_precision_below_98pct")
    return ResearchAuctionRepairQuality(
        expected_code_count=len(expected_codes),
        observed_code_count=len(observed_codes),
        valid_code_count=len(valid_codes),
        expected_valid_code_count=len(expected_valid_codes),
        expected_observed_code_count=len(expected_observed_codes),
        unexpected_code_count=len(observed_codes - expected_codes),
        passed=not issues,
        issues=tuple(issues),
    )


def hash_code_universe(codes: set[str]) -> str:
    if not codes:
        raise ValueError("cannot hash an empty code universe")
    return _canonical_sha256(sorted(codes))


def hash_auction_business_rows(frame: pd.DataFrame) -> str:
    """Hash business values only; repair-time created_at must not stale a plan."""
    missing = set(_AUCTION_BUSINESS_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"auction business hash missing columns: {sorted(missing)}")
    ordered = frame[list(_AUCTION_BUSINESS_COLUMNS)].sort_values(
        list(_AUCTION_PHYSICAL_KEY),
        kind="stable",
    )
    records: list[list[Any]] = [
        [_canonical_scalar(value) for value in row]
        for row in ordered.itertuples(index=False, name=None)
    ]
    return _canonical_sha256(records)


def _validated_existing_partition(
    frame: pd.DataFrame,
    *,
    trade_date: date,
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=_AUCTION_COLUMNS)
    missing = set(_AUCTION_COLUMNS) - set(frame.columns)
    if missing:
        raise ValueError(f"existing auction partition missing columns: {sorted(missing)}")
    normalized = frame[list(_AUCTION_COLUMNS)].copy()
    normalized["trade_date"] = pd.to_datetime(
        normalized["trade_date"],
        errors="raise",
    ).dt.date
    if set(normalized["trade_date"]) != {trade_date}:
        raise ValueError(
            "existing auction partition contains rows outside target date "
            f"{trade_date}"
        )
    if normalized[list(_AUCTION_PHYSICAL_KEY)].isna().any(axis=None):
        raise ValueError("existing auction partition contains a null physical key")
    if normalized.duplicated(list(_AUCTION_PHYSICAL_KEY), keep=False).any():
        raise ValueError("existing auction partition contains a duplicate physical key")
    normalized["created_at"] = pd.to_datetime(normalized["created_at"], errors="raise")
    if normalized["created_at"].isna().any():
        raise ValueError("existing auction partition contains a null created_at")
    for column in _NUMERIC_COLUMNS:
        normalized[column] = pd.to_numeric(normalized[column], errors="raise")
    return normalized


def _business_values(row: pd.Series) -> tuple[object, ...]:
    return tuple(_canonical_scalar(row[column]) for column in _AUCTION_BUSINESS_COLUMNS)


def merge_auction_partition(
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
    *,
    trade_date: date,
) -> pd.DataFrame:
    """Upsert fetched Tushare facts while preserving prior evidence timestamps."""
    prior = _validated_existing_partition(existing, trade_date=trade_date)
    current = _validated_existing_partition(fetched, trade_date=trade_date)
    if current.empty:
        raise ValueError("fetched auction partition is empty")
    if (
        set(current["source"].astype(str)) != {"tushare"}
        or set(current["auction_type"].astype(str)) != {"open_realtime"}
    ):
        raise ValueError("fetched auction partition is not normalized Tushare open auction data")

    replaced_source = (
        prior["source"].astype(str).eq("tushare")
        & prior["auction_type"].astype(str).eq("open_realtime")
    )
    replaced_rows: dict[tuple[object, ...], pd.Series] = {}
    for _, row in prior.loc[replaced_source].iterrows():
        key = tuple(row[column] for column in _AUCTION_PHYSICAL_KEY)
        replaced_rows[key] = row.copy()

    keyed: dict[tuple[object, ...], pd.Series] = {}
    for _, row in prior.loc[~replaced_source].iterrows():
        key = tuple(row[column] for column in _AUCTION_PHYSICAL_KEY)
        keyed[key] = row.copy()
    for _, row in current.iterrows():
        key = tuple(row[column] for column in _AUCTION_PHYSICAL_KEY)
        previous = replaced_rows.get(key)
        if previous is not None and _business_values(previous) == _business_values(row):
            keyed[key] = previous
        else:
            keyed[key] = row.copy()

    merged = pd.DataFrame(
        [row.to_dict() for row in keyed.values()],
        columns=_AUCTION_COLUMNS,
    )
    merged["trade_date"] = pd.to_datetime(merged["trade_date"], errors="raise").dt.date
    merged["created_at"] = pd.to_datetime(merged["created_at"], errors="raise")
    return (
        merged.sort_values(list(_AUCTION_PHYSICAL_KEY), kind="stable")
        .reset_index(drop=True)
    )


def build_auction_repair_day_plan(
    *,
    trade_date: date,
    expected_codes: set[str],
    existing_manifest_sha256: str | None,
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
) -> tuple[ResearchAuctionRepairDayPlan, pd.DataFrame]:
    """Build quality-bound plan evidence and the corresponding merged partition."""
    quality = assess_tushare_auction_rows(fetched, expected_codes=expected_codes)
    if not quality.passed:
        raise ValueError(
            "auction repair quality gate failed: " + ", ".join(quality.issues)
        )
    merged = merge_auction_partition(existing, fetched, trade_date=trade_date)
    fetched_hash = hash_auction_business_rows(fetched)
    merged_hash = hash_auction_business_rows(merged)
    existing_hash = (
        None if existing is None or existing.empty else hash_auction_business_rows(existing)
    )
    day = ResearchAuctionRepairDayPlan(
        trade_date=trade_date,
        expected_code_count=quality.expected_code_count,
        expected_codes_sha256=hash_code_universe(expected_codes),
        existing_manifest_sha256=existing_manifest_sha256,
        fetched_business_sha256=fetched_hash,
        merged_business_sha256=merged_hash,
        existing_row_count=0 if existing is None else len(existing),
        fetched_row_count=len(fetched),
        merged_row_count=len(merged),
        observed_code_count=quality.observed_code_count,
        valid_code_count=quality.valid_code_count,
        expected_valid_code_count=quality.expected_valid_code_count,
        expected_observed_code_count=quality.expected_observed_code_count,
        unexpected_code_count=quality.unexpected_code_count,
        changed=existing_hash != merged_hash,
    )
    return day, merged


def _auction_key(trade_date: date) -> ResearchPartitionKey:
    return ResearchPartitionKey(dataset="auction_bar", trade_date=trade_date)


def _load_existing_manifest(
    paths: ResearchIngestPaths,
    trade_date: date,
) -> tuple[ResearchPartitionManifest | None, str | None]:
    key = _auction_key(trade_date)
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
                f"research catalog has auction partition without manifest: {trade_date}"
            )
        return None, None
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise RuntimeError(f"invalid auction manifest path: {manifest_path}")
    manifest = ResearchPartitionManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    if manifest.partition != key:
        raise RuntimeError(f"auction manifest partition mismatch: {trade_date}")
    if row is None:
        raise RuntimeError(f"auction manifest is missing from research catalog: {trade_date}")
    catalog_manifest = ResearchPartitionManifest.model_validate_json(row[3])
    if (
        str(row[0]) != manifest.relative_path
        or str(row[1]) != manifest.content_hash
        or str(row[2]) != manifest.file_hash
        or catalog_manifest != manifest
    ):
        raise RuntimeError(f"auction catalog/manifest mismatch: {trade_date}")
    return manifest, _file_sha256(manifest_path)


def _build_prepared_repair(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    trade_dates: tuple[date, ...],
    adapter: ResearchAuctionRepairAdapter,
    code_commit: str,
    generated_at: datetime,
) -> _PreparedAuctionRepair:
    if _CLEAN_COMMIT_PATTERN.fullmatch(code_commit) is None:
        raise ValueError("auction repair requires a clean 40-character code commit")
    if any(target >= generated_at.date() for target in trade_dates):
        raise ValueError("auction repair only supports completed historical trade dates")
    source_database = Path(source_database)
    if not source_database.is_file() or source_database.is_symlink():
        raise ValueError(f"source read-only database is invalid: {source_database}")
    (
        bootstrap_snapshot_id,
        previous,
        previous_observation_sha256,
        catalog_sha256,
    ) = _validate_prior_authority(paths)
    if previous is None or previous_observation_sha256 is None:
        raise RuntimeError("auction repair requires an established current authority")
    if previous.bootstrap_snapshot_id != bootstrap_snapshot_id:
        raise RuntimeError("auction repair bootstrap lineage mismatch")
    if previous.readonly_catalog_sha256 is None:
        raise RuntimeError("auction repair current authority lacks readonly catalog hash")
    if any(target > previous.trade_date for target in trade_dates):
        raise ValueError(
            "auction repair target is after current authority trade date"
        )

    merged_by_date: dict[date, pd.DataFrame] = {}
    existing_manifests: dict[date, ResearchPartitionManifest | None] = {}
    day_plans: list[ResearchAuctionRepairDayPlan] = []
    with duckdb.connect(str(source_database), read_only=True) as source:
        for trade_date in trade_dates:
            _require_open_trade_date(source, trade_date)
            expected_codes = _expected_auction_codes(source, trade_date)
            if not _daily_bar_universe_is_complete(
                source,
                trade_date,
                expected_codes,
            ):
                raise ValueError(
                    f"daily_bar universe is incomplete for auction repair: {trade_date}"
                )
            existing_manifest, existing_manifest_sha256 = _load_existing_manifest(
                paths,
                trade_date,
            )
            existing = _query_existing_research_partition(
                paths,
                _auction_key(trade_date),
                _AUCTION_COLUMNS,
            )
            fetched = normalize_tushare_auction_rows(
                adapter.stk_auction(trade_date),
                trade_date=trade_date,
                generated_at=generated_at,
            )
            day_plan, merged = build_auction_repair_day_plan(
                trade_date=trade_date,
                expected_codes=expected_codes,
                existing_manifest_sha256=existing_manifest_sha256,
                existing=existing,
                fetched=fetched,
            )
            day_plans.append(day_plan)
            merged_by_date[trade_date] = merged
            existing_manifests[trade_date] = existing_manifest
    plan = ResearchAuctionRepairPlan(
        code_commit=code_commit,
        authority_current_sha256=previous_observation_sha256,
        catalog_sha256=catalog_sha256,
        readonly_catalog_sha256=previous.readonly_catalog_sha256,
        days=tuple(day_plans),
    )
    return _PreparedAuctionRepair(
        plan=plan,
        previous=previous,
        previous_observation_sha256=previous_observation_sha256,
        merged_by_date=merged_by_date,
        existing_manifests=existing_manifests,
    )


def _verify_plan_baseline(
    paths: ResearchIngestPaths,
    plan: ResearchAuctionRepairPlan,
) -> None:
    current_path = paths.state_dir / "research-authority-current.json"
    observed = {
        "authority current": (
            _file_sha256(current_path) if current_path.is_file() else None
        ),
        "catalog": (
            _file_sha256(paths.catalog_path) if paths.catalog_path.is_file() else None
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
            raise RuntimeError(f"auction repair {label} changed after planning")
    for day in plan.days:
        manifest_path = paths.lake_root / partition_manifest_relative_path(
            _auction_key(day.trade_date)
        )
        manifest_hash = _file_sha256(manifest_path) if manifest_path.is_file() else None
        if manifest_hash != day.existing_manifest_sha256:
            raise RuntimeError(
                f"auction repair manifest changed after planning: {day.trade_date}"
            )


def _build_auction_export_source(
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
        CREATE TABLE auction_bar (
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            auction_type VARCHAR NOT NULL,
            price DOUBLE,
            vol DOUBLE,
            amount DOUBLE,
            turnover_rate DOUBLE,
            volume_ratio DOUBLE,
            source VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (ts_code, trade_date, auction_type, source)
        );
        """
    )
    connection.executemany(
        "INSERT INTO trade_calendar VALUES ('SSE', ?, TRUE)",
        [(trade_date,) for trade_date in sorted(merged_by_date)],
    )
    combined = pd.concat(
        [merged_by_date[trade_date] for trade_date in sorted(merged_by_date)],
        ignore_index=True,
    )
    connection.register("auction_repair_input", combined)
    selected = ", ".join(_AUCTION_COLUMNS)
    connection.execute(
        f"INSERT INTO auction_bar ({selected}) "
        f"SELECT {selected} FROM auction_repair_input"
    )
    connection.unregister("auction_repair_input")
    return connection


def _prepare_repair_generation(
    paths: ResearchIngestPaths,
    *,
    prepared: _PreparedAuctionRepair,
    transaction_root: Path,
    generated_at: datetime,
) -> tuple[Path, Path, dict[date, ResearchPartitionManifest]]:
    _mkdir_durable(transaction_root)
    staged_catalog = transaction_root / "catalog.next.duckdb"
    staged_lake = transaction_root / "lake.next"
    _verify_plan_baseline(paths, prepared.plan)
    shutil.copyfile(paths.catalog_path, staged_catalog)
    with staged_catalog.open("rb") as handle:
        os.fsync(handle.fileno())
    for trade_date in prepared.plan.trade_dates:
        live_partition = paths.lake_root / partition_directory(_auction_key(trade_date))
        staged_partition = staged_lake / partition_directory(_auction_key(trade_date))
        if live_partition.is_dir():
            shutil.copytree(live_partition, staged_partition)

    export_source = _build_auction_export_source(prepared.merged_by_date)
    try:
        summary = export_research_dataset(
            export_source,
            catalog=ResearchCatalog(staged_catalog),
            lake_root=staged_lake,
            dataset="auction_bar",
            start_date=min(prepared.plan.trade_dates),
            end_date=max(prepared.plan.trade_dates),
            code_commit=prepared.plan.code_commit,
            now=lambda: generated_at.astimezone(UTC),
            as_of_date=max(prepared.plan.trade_dates),
        )
    finally:
        export_source.close()
    manifests = {
        partition.trade_date: partition.manifest
        for partition in summary.partitions
        if partition.manifest is not None
    }
    if set(manifests) != set(prepared.plan.trade_dates):
        raise RuntimeError("auction repair staged export did not cover every target date")
    staged_readonly = transaction_root / "readonly.next.duckdb"
    _prepare_readonly_generation(staged_catalog, staged_readonly)
    return staged_catalog, staged_readonly, manifests


def _observation_id(generated_at: datetime) -> str:
    stamp = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"research-auction-repair-{stamp}-{uuid.uuid4().hex[:8]}"


def _repair_observation_path(
    paths: ResearchIngestPaths,
    observation: ResearchAuctionRepairObservation,
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
    prepared: _PreparedAuctionRepair,
    staged_catalog: Path,
    staged_readonly: Path,
    manifests: dict[date, ResearchPartitionManifest],
    generated_at: datetime,
) -> ResearchAuctionRepairObservation:
    bootstrap_snapshot_id = prepared.previous.bootstrap_snapshot_id
    if bootstrap_snapshot_id is None:
        raise RuntimeError("auction repair authority lacks bootstrap snapshot lineage")
    changes: list[ResearchAuctionRepairPartitionChange] = []
    for day in prepared.plan.days:
        if not day.changed:
            continue
        manifest = manifests[day.trade_date]
        staged_manifest_path = (
            staged_catalog.parent
            / "lake.next"
            / partition_manifest_relative_path(_auction_key(day.trade_date))
        )
        existing = prepared.existing_manifests[day.trade_date]
        changes.append(
            ResearchAuctionRepairPartitionChange(
                trade_date=day.trade_date,
                before_manifest_sha256=day.existing_manifest_sha256,
                after_manifest_sha256=_file_sha256(staged_manifest_path),
                before_content_hash=None if existing is None else existing.content_hash,
                before_manifest=existing,
                after_manifest=manifest,
            )
        )
    if not changes:
        raise RuntimeError("unchanged auction repair cannot publish an observation")
    return ResearchAuctionRepairObservation(
        observation_id=_observation_id(generated_at),
        bootstrap_snapshot_id=bootstrap_snapshot_id,
        trade_date=prepared.previous.trade_date,
        generated_at=generated_at,
        code_commit=prepared.plan.code_commit,
        plan_id=prepared.plan.plan_id,
        previous_observation_sha256=prepared.previous_observation_sha256,
        catalog_before_sha256=prepared.plan.catalog_sha256,
        catalog_sha256=_file_sha256(staged_catalog),
        readonly_catalog_before_sha256=prepared.plan.readonly_catalog_sha256,
        readonly_catalog_sha256=_file_sha256(staged_readonly),
        repairs=tuple(changes),
    )


def _journal_path(transaction_root: Path) -> Path:
    return transaction_root / "auction-repair-journal.json"


def _model_sha256(model: BaseModel) -> str:
    payload = (model.model_dump_json(indent=2) + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _backup_file(source: Path, target: Path, expected_hash: str) -> None:
    shutil.copyfile(source, target)
    with target.open("rb") as handle:
        os.fsync(handle.fileno())
    if _file_sha256(target) != expected_hash:
        raise RuntimeError(f"auction repair backup verification failed: {source}")


def _prepare_repair_journal(
    paths: ResearchIngestPaths,
    *,
    prepared: _PreparedAuctionRepair,
    transaction_root: Path,
    staged_catalog: Path,
    staged_readonly: Path,
    observation: ResearchAuctionRepairObservation,
) -> _ResearchAuctionRepairPublishJournal:
    observation_path = _repair_observation_path(paths, observation)
    if observation_path.exists():
        raise RuntimeError("auction repair observation path already exists")
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
            raise RuntimeError(f"auction repair publish target is invalid: {source}")
        if _file_sha256(source) != expected_hash:
            raise RuntimeError(f"auction repair publish baseline changed: {source}")
        _backup_file(source, backup, expected_hash)

    entries: list[_ResearchAuctionRepairJournalEntry] = []
    for change in observation.repairs:
        key = _auction_key(change.trade_date)
        live_manifest = paths.lake_root / partition_manifest_relative_path(key)
        staged_manifest = (
            transaction_root / "lake.next" / partition_manifest_relative_path(key)
        )
        manifest_existed = live_manifest.is_file()
        before_hash = _file_sha256(live_manifest) if manifest_existed else None
        if before_hash != change.before_manifest_sha256:
            raise RuntimeError(
                f"auction repair manifest baseline changed: {change.trade_date}"
            )
        if manifest_existed:
            _backup_file(
                live_manifest,
                transaction_root / f"manifest-{change.trade_date.isoformat()}.before",
                before_hash,
            )
        manifest = change.after_manifest
        live_version = paths.lake_root / manifest.relative_path
        entries.append(
            _ResearchAuctionRepairJournalEntry(
                trade_date=change.trade_date,
                manifest_existed=manifest_existed,
                manifest_before_sha256=before_hash,
                manifest_after_sha256=_file_sha256(staged_manifest),
                version_relative_path=manifest.relative_path,
                version_existed=live_version.is_file(),
                version_sha256=manifest.file_hash,
            )
        )
    journal = _ResearchAuctionRepairPublishJournal(
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
        raise ValueError("auction repair clock must be timezone-aware")
    local = value.astimezone(_CST)
    if (
        local.weekday() < 5
        and _MARKET_PROTECTION_START <= local.time() <= _MARKET_PROTECTION_END
    ):
        raise ValueError("auction repair is forbidden during market protection window")
    return local


def _publish_repair_generation(
    paths: ResearchIngestPaths,
    *,
    prepared: _PreparedAuctionRepair,
    transaction_root: Path,
    staged_catalog: Path,
    staged_readonly: Path,
    observation: ResearchAuctionRepairObservation,
    publish_guard: Callable[[], None],
) -> None:
    try:
        with exclusive_file_lock(ResearchCatalog(paths.catalog_path).lock_path):
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
                staged_data = transaction_root / "lake.next" / change.after_manifest.relative_path
                live_data = paths.lake_root / change.after_manifest.relative_path
                if live_data.exists():
                    if (
                        not live_data.is_file()
                        or live_data.is_symlink()
                        or _file_sha256(live_data) != change.after_manifest.file_hash
                    ):
                        raise RuntimeError(
                            "existing immutable auction repair version hash mismatch"
                        )
                else:
                    _copy_file_atomic(staged_data, live_data)
            _publish_step_hook("versions_published")
            publish_guard()
            for change in observation.repairs:
                relative_manifest = partition_manifest_relative_path(
                    _auction_key(change.trade_date)
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
            _copy_file_atomic(staged_readonly, paths.readonly_catalog_path)
            _publish_step_hook("readonly_published")
            publish_guard()
            _write_model_atomic(
                _repair_observation_path(paths, observation),
                observation,
            )
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
                rollback_research_auction_repair_publish(paths, transaction_root)
            except Exception as rollback_error:
                raise RuntimeError(
                    "auction repair publish failed and rollback is pending: "
                    f"{rollback_error}"
                ) from rollback_error
        raise


def _resolved_version_path(paths: ResearchIngestPaths, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise RuntimeError("auction repair journal version path escaped lake root")
    root = paths.lake_root.resolve()
    if paths.lake_root.is_symlink() or not paths.lake_root.is_dir():
        raise RuntimeError("auction repair lake root is invalid")
    cursor = paths.lake_root
    for part in relative.parts[:-1]:
        cursor = cursor / part
        if cursor.is_symlink():
            raise RuntimeError("auction repair journal version path contains symlink")
    candidate = paths.lake_root / relative
    resolved_parent = candidate.parent.resolve()
    if resolved_parent != root and root not in resolved_parent.parents:
        raise RuntimeError("auction repair journal version path escaped lake root")
    return candidate


def rollback_research_auction_repair_publish(
    paths: ResearchIngestPaths,
    transaction_root: Path,
) -> None:
    """Rollback one journaled repair after fully preflighting every CAS target."""
    journal_path = _journal_path(transaction_root)
    if not journal_path.is_file() or journal_path.is_symlink():
        raise RuntimeError(f"auction repair journal is invalid: {journal_path}")
    journal = _ResearchAuctionRepairPublishJournal.model_validate_json(
        journal_path.read_text(encoding="utf-8")
    )
    if journal.transaction_id != transaction_root.name:
        raise RuntimeError("auction repair journal transaction mismatch")
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
        raise RuntimeError("auction repair observation path escaped state root")

    with exclusive_file_lock(ResearchCatalog(paths.catalog_path).lock_path):
        for target, backup, before_hash, after_hash in catalog_targets:
            if target.is_symlink() or (target.exists() and not target.is_file()):
                raise RuntimeError(f"auction repair rollback CAS mismatch: {target}")
            if (
                not backup.is_file()
                or backup.is_symlink()
                or _file_sha256(backup) != before_hash
            ):
                raise RuntimeError(f"auction repair rollback backup mismatch: {backup}")
            observed_hash = _file_sha256(target) if target.is_file() else None
            if observed_hash not in {before_hash, after_hash}:
                raise RuntimeError(f"auction repair rollback CAS mismatch: {target}")
        for entry in journal.entries:
            manifest_path = paths.lake_root / partition_manifest_relative_path(
                _auction_key(entry.trade_date)
            )
            if manifest_path.is_symlink() or (
                manifest_path.exists() and not manifest_path.is_file()
            ):
                raise RuntimeError(
                    f"auction repair rollback manifest CAS mismatch: {entry.trade_date}"
                )
            observed_manifest_hash = (
                _file_sha256(manifest_path) if manifest_path.is_file() else None
            )
            allowed_manifest_hashes = (
                {entry.manifest_before_sha256, entry.manifest_after_sha256}
                if entry.manifest_existed
                else {None, entry.manifest_after_sha256}
            )
            if observed_manifest_hash not in allowed_manifest_hashes:
                raise RuntimeError(
                    f"auction repair rollback manifest CAS mismatch: {entry.trade_date}"
                )
            if entry.manifest_existed:
                backup = (
                    transaction_root
                    / f"manifest-{entry.trade_date.isoformat()}.before"
                )
                if (
                    not backup.is_file()
                    or backup.is_symlink()
                    or _file_sha256(backup) != entry.manifest_before_sha256
                ):
                    raise RuntimeError("auction repair rollback manifest backup mismatch")
            version_path = _resolved_version_path(paths, entry.version_relative_path)
            if version_path.is_symlink() or (
                version_path.exists() and not version_path.is_file()
            ):
                raise RuntimeError("auction repair rollback version CAS mismatch")
            observed_version_hash = (
                _file_sha256(version_path) if version_path.is_file() else None
            )
            if entry.version_existed:
                if observed_version_hash != entry.version_sha256:
                    raise RuntimeError("auction repair rollback version CAS mismatch")
            elif observed_version_hash not in {None, entry.version_sha256}:
                raise RuntimeError("auction repair rollback version CAS mismatch")
        if journal.observation_path.is_symlink() or (
            journal.observation_path.exists()
            and not journal.observation_path.is_file()
        ):
            raise RuntimeError("auction repair rollback observation CAS mismatch")
        observation_hash = (
            _file_sha256(journal.observation_path)
            if journal.observation_path.is_file()
            else None
        )
        if observation_hash not in {None, journal.current_after_sha256}:
            raise RuntimeError("auction repair rollback observation CAS mismatch")

        for target, backup, before_hash, after_hash in catalog_targets:
            _restore_file(
                target,
                backup,
                True,
                expected_before_hash=before_hash,
                expected_after_hash=after_hash,
            )
        for entry in journal.entries:
            manifest_path = paths.lake_root / partition_manifest_relative_path(
                _auction_key(entry.trade_date)
            )
            _restore_file(
                manifest_path,
                transaction_root / f"manifest-{entry.trade_date.isoformat()}.before",
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
                    raise RuntimeError("auction repair rollback version CAS mismatch")
                version_path.unlink(missing_ok=True)
                if version_path.parent.exists():
                    _fsync_directory(version_path.parent)
        journal.observation_path.unlink(missing_ok=True)
        if journal.observation_path.parent.exists():
            _fsync_directory(journal.observation_path.parent)
    _remove_transaction_root(transaction_root)


def run_research_auction_repair(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    trade_dates: Iterable[date],
    adapter: ResearchAuctionRepairAdapter,
    code_commit: str,
    apply: bool = False,
    plan_id: str | None = None,
    now: Callable[[], datetime] | None = None,
) -> ResearchAuctionRepairResult:
    """Plan or atomically publish a content-bound historical auction repair."""
    clock = now or (lambda: datetime.now(_CST))
    generated_at = clock()
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("auction repair clock must be timezone-aware")
    generated_at = generated_at.astimezone(_CST)
    normalized_dates = normalize_repair_dates(trade_dates)
    if apply and (
        plan_id is None or re.fullmatch(_HASH_PATTERN, plan_id) is None
    ):
        raise ValueError("auction repair apply requires a 64-character plan_id")
    if not apply:
        prepared = _build_prepared_repair(
            source_database=source_database,
            paths=paths,
            trade_dates=normalized_dates,
            adapter=adapter,
            code_commit=code_commit,
            generated_at=generated_at,
        )
        return ResearchAuctionRepairResult(status="planned", plan=prepared.plan)

    _mkdir_durable(paths.state_dir)
    with exclusive_file_lock(paths.publish_lock_path):
        _recover_interrupted_publish(paths)

        def publish_guard() -> None:
            _require_outside_market_protection(clock())

        publish_guard()
        prepared = _build_prepared_repair(
            source_database=source_database,
            paths=paths,
            trade_dates=normalized_dates,
            adapter=adapter,
            code_commit=code_commit,
            generated_at=generated_at,
        )
        if prepared.plan.plan_id != plan_id:
            raise ValueError(
                "stale repair plan: rerun preview and apply the new plan_id"
            )
        if not any(day.changed for day in prepared.plan.days):
            return ResearchAuctionRepairResult(
                status="unchanged",
                plan=prepared.plan,
            )
        _mkdir_durable(paths.transactions_root)
        transaction_root = paths.transactions_root / f"auction-repair-{uuid.uuid4().hex}"
        try:
            staged_catalog, staged_readonly, manifests = _prepare_repair_generation(
                paths,
                prepared=prepared,
                transaction_root=transaction_root,
                generated_at=generated_at,
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
            if transaction_root.exists() and not _journal_path(transaction_root).exists():
                _remove_transaction_root(transaction_root)
        return ResearchAuctionRepairResult(
            status="candidate",
            plan=prepared.plan,
            observation=observation,
        )
