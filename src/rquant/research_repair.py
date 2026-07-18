"""Governed, atomic repair planning for historical auction partitions."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any, Literal
from zoneinfo import ZoneInfo

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

_CST = ZoneInfo("Asia/Shanghai")
_ACTION_ID = "research-auction-history-repair/v1"
_CLEAN_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_TS_CODE_PATTERN = re.compile(r"^\d{6}\.(?:SZ|SH|BJ)$")
_HASH_PATTERN = r"^[0-9a-f]{64}$"
_MINIMUM_COVERAGE_PERCENT = 98

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

    keyed: dict[tuple[object, ...], pd.Series] = {}
    for _, row in prior.iterrows():
        key = tuple(row[column] for column in _AUCTION_PHYSICAL_KEY)
        keyed[key] = row.copy()
    for _, row in current.iterrows():
        key = tuple(row[column] for column in _AUCTION_PHYSICAL_KEY)
        previous = keyed.get(key)
        if previous is not None and _business_values(previous) == _business_values(row):
            continue
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
