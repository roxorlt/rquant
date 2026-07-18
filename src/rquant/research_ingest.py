"""Daily cloud research ingestion with fail-closed candidate observations."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import uuid
from collections.abc import Callable, Sequence
from contextlib import ExitStack
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, model_validator

from rquant.research_catalog import ResearchCatalog, exclusive_file_lock
from rquant.research_lake import (
    ResearchExportSummary,
    ResearchPartitionKey,
    ResearchPartitionManifest,
    export_research_dataset,
    partition_directory,
    partition_manifest_relative_path,
)
from rquant.research_migration import ResearchAuthorityCandidate

_CST = ZoneInfo("Asia/Shanghai")
_CLEAN_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_MINIMUM_SAFE_TIME = time(15, 15)
_MARKET_PROTECTION_START = time(9, 15)
_MARKET_PROTECTION_END = time(15, 10)
_MINIMUM_AUCTION_COVERAGE = 0.98

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


class ResearchIngestAdapter(Protocol):
    def rt_min_daily(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame: ...

    def stk_mins(
        self,
        ts_code: str,
        freq: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame: ...

    def stk_auction(self, trade_date: date) -> pd.DataFrame: ...


class _ResearchModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ResearchIngestPaths(_ResearchModel):
    """Explicit research locations; no operational path is derived implicitly."""

    state_dir: Path
    catalog_path: Path
    readonly_catalog_path: Path
    lake_root: Path
    staging_root: Path

    @model_validator(mode="after")
    def validate_distinct_paths(self) -> ResearchIngestPaths:
        resolved = {
            self.catalog_path.resolve(),
            self.readonly_catalog_path.resolve(),
            self.lake_root.resolve(),
            self.staging_root.resolve(),
        }
        if len(resolved) != 4:
            raise ValueError("research ingest paths must differ from each other")
        return self

    @classmethod
    def from_data_dir(cls, data_dir: Path) -> ResearchIngestPaths:
        root = Path(data_dir)
        return cls(
            state_dir=root,
            catalog_path=root / "research.duckdb",
            readonly_catalog_path=root / "research_ro.duckdb",
            lake_root=root / "lake",
            staging_root=root / "research_staging",
        )

    @property
    def publish_lock_path(self) -> Path:
        return self.state_dir / "research-publish.lock"

    @property
    def transactions_root(self) -> Path:
        return self.state_dir / "research_transactions"


class ResearchIngestSkipResult(_ResearchModel):
    status: Literal["skipped"] = "skipped"
    trade_date: date
    reason: Literal["closed_trade_date"] = "closed_trade_date"


class ResearchIngestReadinessResult(_ResearchModel):
    status: Literal["ready", "closed", "not_ready"]
    trade_date: date
    latest_daily_bar_date: date | None
    daily_bar_code_count: int = Field(ge=0)
    issues: tuple[str, ...]


class ResearchWatchlistItem(_ResearchModel):
    ts_code: str = Field(pattern=r"^\d{6}\.(?:SZ|SH|BJ)$")
    pool: Literal["pool1", "pool2"]


class ResearchWatchlistSnapshot(_ResearchModel):
    schema_version: Literal[1] = 1
    trade_date: date
    captured_at: datetime
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    items: tuple[ResearchWatchlistItem, ...]

    @model_validator(mode="after")
    def validate_snapshot(self) -> ResearchWatchlistSnapshot:
        if self.captured_at.tzinfo is None or self.captured_at.utcoffset() is None:
            raise ValueError("watchlist captured_at must be timezone-aware")
        if self.captured_at.astimezone(_CST).date() != self.trade_date:
            raise ValueError("watchlist captured_at must belong to trade_date")
        if self.captured_at.astimezone(_CST).time() >= time(9, 30):
            raise ValueError("watchlist snapshot must be captured before 09:30 CST")
        codes = tuple(item.ts_code for item in self.items)
        if len(codes) != len(set(codes)):
            raise ValueError("watchlist snapshot contains duplicate ts_code")
        return self


class ResearchDatasetIngestAudit(_ResearchModel):
    dataset: Literal["minute_bar", "auction_bar"]
    export: ResearchExportSummary
    expected_code_count: int = Field(ge=0)
    observed_code_count: int = Field(ge=0)
    complete_code_count: int = Field(ge=0)
    unexpected_code_count: int = Field(default=0, ge=0)
    coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    observed_precision_ratio: float | None = Field(default=None, ge=0, le=1)
    earliest_time: datetime | date | None = None
    latest_time: datetime | date | None = None


class ResearchDailyIngestResult(_ResearchModel):
    schema_version: Literal[1] = 1
    status: Literal["planned", "candidate", "degraded"]
    observation_id: str
    bootstrap_snapshot_id: str | None
    trade_date: date
    generated_at: datetime
    code_commit: str
    previous_observation_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    stability_parent_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    previous_stable_trade_date: date | None = None
    catalog_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    readonly_catalog_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    stable_trading_days: int = Field(ge=0)
    minute: ResearchDatasetIngestAudit
    auction: ResearchDatasetIngestAudit
    issues: tuple[str, ...]

    @model_validator(mode="after")
    def validate_result(self) -> ResearchDailyIngestResult:
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("research ingest generated_at must be timezone-aware")
        if self.status == "planned":
            if self.catalog_sha256 is not None or self.readonly_catalog_sha256 is not None:
                raise ValueError("planned ingest must not publish catalog hashes")
        elif self.catalog_sha256 is None or self.readonly_catalog_sha256 is None:
            raise ValueError("published ingest requires both catalog hashes")
        if self.status == "candidate" and self.issues:
            raise ValueError("candidate ingest cannot contain quality issues")
        if self.status == "degraded" and not self.issues:
            raise ValueError("degraded ingest requires at least one quality issue")
        if self.stable_trading_days <= 1 and (
            self.stability_parent_sha256 is not None or self.previous_stable_trade_date is not None
        ):
            raise ValueError("stable day one cannot have a stability parent")
        if self.stable_trading_days > 1 and (
            self.stability_parent_sha256 is None or self.previous_stable_trade_date is None
        ):
            raise ValueError("multi-day candidate requires a stability parent")
        return self


def _model_payload_sha256(model: BaseModel) -> str:
    payload = (model.model_dump_json(indent=2) + "\n").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class ResearchAuctionRepairPartitionChange(_ResearchModel):
    trade_date: date
    before_manifest_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    after_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    before_content_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    before_manifest: ResearchPartitionManifest | None = None
    after_manifest: ResearchPartitionManifest

    @model_validator(mode="after")
    def validate_change(self) -> ResearchAuctionRepairPartitionChange:
        if (
            self.after_manifest.dataset != "auction_bar"
            or self.after_manifest.partition.trade_date != self.trade_date
        ):
            raise ValueError("auction repair manifest must bind its trade date")
        if _model_payload_sha256(self.after_manifest) != self.after_manifest_sha256:
            raise ValueError("repaired auction manifest physical hash is inconsistent")
        if self.before_manifest is None:
            if (
                self.before_manifest_sha256 is not None
                or self.before_content_hash is not None
                or self.after_manifest.parent_content_hash is not None
            ):
                raise ValueError("new auction partition cannot claim prior manifest evidence")
        else:
            if (
                self.before_manifest.dataset != "auction_bar"
                or self.before_manifest.partition.trade_date != self.trade_date
            ):
                raise ValueError("prior auction manifest must bind its trade date")
            if self.before_manifest_sha256 is None:
                raise ValueError("prior auction manifest requires its physical hash")
            if (
                _model_payload_sha256(self.before_manifest)
                != self.before_manifest_sha256
            ):
                raise ValueError("prior auction manifest physical hash is inconsistent")
            if self.before_content_hash != self.before_manifest.content_hash:
                raise ValueError("prior auction content hash does not match its manifest")
            if (
                self.after_manifest.parent_content_hash
                != self.before_manifest.content_hash
            ):
                raise ValueError("repaired auction manifest must link to prior content")
        return self


class ResearchAuctionRepairObservation(_ResearchModel):
    schema_version: Literal[1] = 1
    observation_kind: Literal["auction_repair"] = "auction_repair"
    status: Literal["candidate"] = "candidate"
    observation_id: str
    bootstrap_snapshot_id: str
    trade_date: date
    generated_at: datetime
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_observation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_before_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    readonly_catalog_before_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    readonly_catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stable_trading_days: Literal[0] = 0
    repairs: tuple[ResearchAuctionRepairPartitionChange, ...] = Field(min_length=1)
    issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_observation(self) -> ResearchAuctionRepairObservation:
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("auction repair generated_at must be timezone-aware")
        repair_dates = tuple(change.trade_date for change in self.repairs)
        if repair_dates != tuple(sorted(set(repair_dates))):
            raise ValueError("auction repair dates must be strictly ordered and unique")
        if any(repair_date > self.trade_date for repair_date in repair_dates):
            raise ValueError("auction repair cannot move authority trade date backwards")
        if self.issues:
            raise ValueError("auction repair authority observation cannot contain issues")
        return self


class ResearchMinuteRepairPartitionChange(_ResearchModel):
    trade_date: date
    before_manifest_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    after_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    before_content_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    before_manifest: ResearchPartitionManifest | None = None
    after_manifest: ResearchPartitionManifest

    @model_validator(mode="after")
    def validate_change(self) -> ResearchMinuteRepairPartitionChange:
        if (
            self.after_manifest.dataset != "minute_bar"
            or self.after_manifest.partition.freq != "1min"
            or self.after_manifest.partition.trade_date != self.trade_date
        ):
            raise ValueError(
                "minute repair manifest must bind its trade date and 1min frequency"
            )
        if _model_payload_sha256(self.after_manifest) != self.after_manifest_sha256:
            raise ValueError("repaired minute manifest physical hash is inconsistent")
        if self.before_manifest is None:
            if (
                self.before_manifest_sha256 is not None
                or self.before_content_hash is not None
                or self.after_manifest.parent_content_hash is not None
            ):
                raise ValueError(
                    "new minute partition cannot claim prior manifest evidence"
                )
        else:
            if (
                self.before_manifest.dataset != "minute_bar"
                or self.before_manifest.partition.freq != "1min"
                or self.before_manifest.partition.trade_date != self.trade_date
            ):
                raise ValueError(
                    "prior minute manifest must bind its trade date and frequency"
                )
            if self.before_manifest_sha256 is None:
                raise ValueError("prior minute manifest requires its physical hash")
            if (
                _model_payload_sha256(self.before_manifest)
                != self.before_manifest_sha256
            ):
                raise ValueError("prior minute manifest physical hash is inconsistent")
            if self.before_content_hash != self.before_manifest.content_hash:
                raise ValueError(
                    "prior minute content hash does not match its manifest"
                )
            if (
                self.after_manifest.parent_content_hash
                != self.before_manifest.content_hash
            ):
                raise ValueError("repaired minute manifest must link to prior content")
        return self


class ResearchMinuteRepairObservation(_ResearchModel):
    schema_version: Literal[1] = 1
    observation_kind: Literal["minute_repair"] = "minute_repair"
    status: Literal["candidate"] = "candidate"
    observation_id: str
    bootstrap_snapshot_id: str
    trade_date: date
    generated_at: datetime
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    manifest_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    plan_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    previous_observation_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_before_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    readonly_catalog_before_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    readonly_catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    stable_trading_days: Literal[0] = 0
    repairs: tuple[ResearchMinuteRepairPartitionChange, ...] = Field(
        min_length=1
    )
    issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_observation(self) -> ResearchMinuteRepairObservation:
        if self.generated_at.tzinfo is None or self.generated_at.utcoffset() is None:
            raise ValueError("minute repair generated_at must be timezone-aware")
        repair_dates = tuple(change.trade_date for change in self.repairs)
        if repair_dates != tuple(sorted(set(repair_dates))):
            raise ValueError("minute repair dates must be strictly ordered and unique")
        if any(repair_date > self.trade_date for repair_date in repair_dates):
            raise ValueError("minute repair cannot move authority trade date backwards")
        if self.issues:
            raise ValueError("minute repair authority observation cannot contain issues")
        return self


ResearchRepairObservation = (
    ResearchAuctionRepairObservation | ResearchMinuteRepairObservation
)
ResearchAuthorityObservation = ResearchDailyIngestResult | ResearchRepairObservation


class _ResearchPublishJournal(_ResearchModel):
    schema_version: Literal[1] = 1
    transaction_id: str
    trade_date: date
    created_at: datetime
    observation_path: Path
    readonly_existed: bool
    current_existed: bool
    minute_manifest_existed: bool
    auction_manifest_existed: bool
    minute_version_relative_path: str
    auction_version_relative_path: str
    minute_version_existed: bool
    auction_version_existed: bool
    catalog_before_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    readonly_before_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    current_before_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    minute_manifest_before_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    auction_manifest_before_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    catalog_after_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    readonly_after_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    minute_manifest_after_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    auction_manifest_after_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    current_after_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    minute_version_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    auction_version_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def validate_before_hashes(self) -> _ResearchPublishJournal:
        for label, existed, before_hash in (
            ("readonly", self.readonly_existed, self.readonly_before_sha256),
            ("current", self.current_existed, self.current_before_sha256),
            (
                "minute_manifest",
                self.minute_manifest_existed,
                self.minute_manifest_before_sha256,
            ),
            (
                "auction_manifest",
                self.auction_manifest_existed,
                self.auction_manifest_before_sha256,
            ),
        ):
            if existed != (before_hash is not None):
                raise ValueError(f"{label} before hash does not match existence")
        return self


class _ResearchGenerationBaseline(_ResearchModel):
    catalog_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    minute_manifest_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    auction_manifest_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")


class ResearchAuthorityStatus(_ResearchModel):
    status: Literal["missing", "bootstrap_candidate", "candidate", "degraded", "invalid"]
    bootstrap_snapshot_id: str | None
    latest_trade_date: date | None
    stable_trading_days: int = Field(ge=0)
    observation_count: int = Field(ge=0)
    catalog_hash_matches: bool
    readonly_catalog_hash_matches: bool
    eligible_for_promotion: bool
    issues: tuple[str, ...]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _mkdir_durable(path: Path) -> None:
    missing: list[Path] = []
    cursor = path
    while not cursor.exists():
        if cursor.is_symlink():
            raise RuntimeError(f"research directory must not be a symlink: {cursor}")
        missing.append(cursor)
        cursor = cursor.parent
    if not cursor.is_dir() or cursor.is_symlink():
        raise RuntimeError(f"research directory root is invalid: {cursor}")
    for directory in reversed(missing):
        directory.mkdir(exist_ok=True)
        if not directory.is_dir() or directory.is_symlink():
            raise RuntimeError(f"research directory is invalid: {directory}")
        _fsync_directory(directory.parent)


def _write_model_atomic(path: Path, model: BaseModel) -> None:
    _mkdir_durable(path.parent)
    temp_path = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(model.model_dump_json(indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        _fsync_directory(path.parent)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def watchlist_snapshot_path(staging_root: Path, trade_date: date) -> Path:
    return Path(staging_root) / "minute" / f"trade_date={trade_date.isoformat()}" / "watchlist.json"


def write_research_watchlist_snapshot(
    staging_root: Path,
    *,
    trade_date: date,
    items: Sequence[ResearchWatchlistItem],
    captured_at: datetime,
    code_commit: str,
) -> Path:
    """Publish the expected minute universe before monitor polling starts."""
    path = watchlist_snapshot_path(staging_root, trade_date)
    normalized_items = tuple(sorted(items, key=lambda item: item.ts_code))
    if path.exists():
        existing = _load_watchlist_snapshot(staging_root, trade_date)
        if existing is None:  # pragma: no cover - existence checked above
            raise RuntimeError("research watchlist snapshot disappeared")
        if existing.items == normalized_items and existing.code_commit == code_commit:
            return path
        raise RuntimeError("research watchlist snapshot is immutable after first publication")
    snapshot = ResearchWatchlistSnapshot(
        trade_date=trade_date,
        captured_at=captured_at,
        code_commit=code_commit,
        items=normalized_items,
    )
    _mkdir_durable(path.parent)
    payload = snapshot.model_dump_json(indent=2) + "\n"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        existing = _load_watchlist_snapshot(staging_root, trade_date)
        if (
            existing is not None
            and existing.items == normalized_items
            and existing.code_commit == code_commit
        ):
            return path
        raise RuntimeError(
            "research watchlist snapshot is immutable after first publication"
        ) from None
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_directory(path.parent)
    except Exception:
        path.unlink(missing_ok=True)
        raise
    return path


def _load_watchlist_snapshot(
    staging_root: Path, trade_date: date
) -> ResearchWatchlistSnapshot | None:
    path = watchlist_snapshot_path(staging_root, trade_date)
    if not path.exists():
        return None
    if not path.is_file() or path.is_symlink():
        raise RuntimeError(f"invalid research watchlist snapshot: {path}")
    snapshot = ResearchWatchlistSnapshot.model_validate_json(path.read_text(encoding="utf-8"))
    if snapshot.trade_date != trade_date:
        raise RuntimeError("research watchlist snapshot date mismatch")
    return snapshot


def _table_exists(connection: duckdb.DuckDBPyConnection, table: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main' AND table_name = ?
        """,
        [table],
    ).fetchone()
    return row is not None and int(row[0]) == 1


def _trade_date_is_open(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
) -> bool:
    if not _table_exists(connection, "trade_calendar"):
        raise ValueError("authoritative SSE trade calendar is required")
    row = connection.execute(
        """
        SELECT is_open
        FROM trade_calendar
        WHERE exchange = 'SSE' AND cal_date = ?
        """,
        [trade_date],
    ).fetchone()
    if row is None:
        raise ValueError(f"missing SSE trade date: {trade_date}")
    return bool(row[0])


def research_trade_date_is_open(source_database: Path, trade_date: date) -> bool:
    """Resolve a stored SSE calendar fact without treating missing data as closed."""
    source_database = Path(source_database)
    if not source_database.is_file() or source_database.is_symlink():
        raise ValueError(f"source read-only database is invalid: {source_database}")
    with duckdb.connect(str(source_database), read_only=True) as connection:
        return _trade_date_is_open(connection, trade_date)


def _require_open_trade_date(connection: duckdb.DuckDBPyConnection, trade_date: date) -> None:
    if not _trade_date_is_open(connection, trade_date):
        raise ValueError(f"closed SSE trade date: {trade_date}")


def _query_existing_frame(
    connection: duckdb.DuckDBPyConnection,
    *,
    table: Literal["minute_bar", "auction_bar"],
    trade_date: date,
) -> pd.DataFrame:
    if not _table_exists(connection, table):
        raise ValueError(f"source table missing: {table}")
    if table == "minute_bar":
        return connection.execute(
            "SELECT * FROM minute_bar WHERE CAST(trade_time AS DATE) = ?",
            [trade_date],
        ).fetchdf()
    return connection.execute(
        "SELECT * FROM auction_bar WHERE trade_date = ?",
        [trade_date],
    ).fetchdf()


def _query_existing_research_partition(
    paths: ResearchIngestPaths,
    key: ResearchPartitionKey,
    columns: tuple[str, ...],
) -> pd.DataFrame:
    manifest_path = paths.lake_root / partition_manifest_relative_path(key)
    if not manifest_path.exists():
        return pd.DataFrame(columns=columns)
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise RuntimeError(f"invalid research partition manifest: {manifest_path}")
    manifest = ResearchPartitionManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    if manifest.partition != key:
        raise RuntimeError("research partition manifest key mismatch")
    data_path = paths.lake_root / manifest.relative_path
    if (
        not data_path.is_file()
        or data_path.is_symlink()
        or _file_sha256(data_path) != manifest.file_hash
    ):
        raise RuntimeError("research partition data hash mismatch")
    selected = ", ".join(columns)
    with duckdb.connect() as connection:
        frame = connection.execute(
            f"SELECT {selected} FROM read_parquet(?)",
            [str(data_path)],
        ).fetchdf()
    if key.dataset == "minute_bar":
        frame["trade_time"] = pd.to_datetime(frame["trade_time"])
    else:
        frame["trade_date"] = pd.to_datetime(frame["trade_date"]).dt.date
    return frame


def _expected_auction_codes(connection: duckdb.DuckDBPyConnection, trade_date: date) -> set[str]:
    if not _table_exists(connection, "daily_bar"):
        return set()
    rows = connection.execute(
        "SELECT DISTINCT ts_code FROM daily_bar WHERE trade_date = ? ORDER BY ts_code",
        [trade_date],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _daily_bar_universe_is_complete(
    connection: duckdb.DuckDBPyConnection,
    trade_date: date,
    current_codes: set[str],
) -> bool:
    if not current_codes:
        return False
    history = connection.execute(
        """
        SELECT COUNT(DISTINCT ts_code) AS code_count
        FROM daily_bar
        WHERE trade_date < ?
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT 5
        """,
        [trade_date],
    ).fetchall()
    historical_max = max((int(row[0]) for row in history), default=0)
    if historical_max == 0 or len(current_codes) / historical_max < 0.98:
        return False
    if not _table_exists(connection, "stock_status_daily"):
        return True
    status_rows = connection.execute(
        "SELECT DISTINCT ts_code FROM stock_status_daily WHERE trade_date = ?",
        [trade_date],
    ).fetchall()
    status_codes = {str(row[0]) for row in status_rows}
    if not status_codes:
        return True
    intersection = len(current_codes & status_codes)
    return intersection / len(current_codes) >= 0.98 and intersection / len(status_codes) >= 0.98


def assess_research_ingest_readiness(
    source_database: Path,
    trade_date: date,
) -> ResearchIngestReadinessResult:
    """Verify that the refreshed replica can support a trustworthy daily ingest."""
    source_database = Path(source_database)
    if not source_database.is_file() or source_database.is_symlink():
        raise ValueError(f"source read-only database is invalid: {source_database}")
    with duckdb.connect(str(source_database), read_only=True) as connection:
        if not _trade_date_is_open(connection, trade_date):
            return ResearchIngestReadinessResult(
                status="closed",
                trade_date=trade_date,
                latest_daily_bar_date=None,
                daily_bar_code_count=0,
                issues=(),
            )
        if not _table_exists(connection, "daily_bar"):
            return ResearchIngestReadinessResult(
                status="not_ready",
                trade_date=trade_date,
                latest_daily_bar_date=None,
                daily_bar_code_count=0,
                issues=("daily_bar_missing",),
            )
        latest_row = connection.execute("SELECT MAX(trade_date) FROM daily_bar").fetchone()
        latest_daily_bar_date = None if latest_row is None else latest_row[0]
        current_codes = _expected_auction_codes(connection, trade_date)
        issues: list[str] = []
        if latest_daily_bar_date != trade_date:
            issues.append("daily_bar_not_current")
        if not _daily_bar_universe_is_complete(connection, trade_date, current_codes):
            issues.append("daily_bar_universe_incomplete")
        return ResearchIngestReadinessResult(
            status="ready" if not issues else "not_ready",
            trade_date=trade_date,
            latest_daily_bar_date=latest_daily_bar_date,
            daily_bar_code_count=len(current_codes),
            issues=tuple(issues),
        )


def _fetch_historical_minutes(
    adapter: ResearchIngestAdapter,
    *,
    ts_codes: set[str],
    trade_date: date,
) -> pd.DataFrame:
    start = datetime.combine(trade_date, time(9, 0))
    end = datetime.combine(trade_date, time(15, 30))
    frames = [
        adapter.stk_mins(ts_code, "1min", start, end)
        for ts_code in sorted(ts_codes)
    ]
    populated = [frame for frame in frames if frame is not None and not frame.empty]
    return pd.concat(populated, ignore_index=True) if populated else pd.DataFrame()


def _normalize_fetched_frame(
    frame: pd.DataFrame,
    *,
    columns: tuple[str, ...],
    trade_date: date,
    generated_at: datetime,
    dataset: Literal["minute_bar", "auction_bar"],
) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=columns)
    payload = frame.copy()
    required = set(columns) - {"created_at"}
    missing = required - set(payload.columns)
    if missing:
        raise ValueError(f"{dataset} fetch missing columns: {sorted(missing)}")
    payload["created_at"] = generated_at.astimezone(_CST).replace(tzinfo=None)
    if dataset == "minute_bar":
        payload["trade_time"] = pd.to_datetime(payload["trade_time"])
        observed_dates = set(payload["trade_time"].dt.date)
    else:
        payload["trade_date"] = pd.to_datetime(payload["trade_date"]).dt.date
        observed_dates = set(payload["trade_date"])
    if observed_dates - {trade_date}:
        raise ValueError(f"{dataset} fetch returned rows outside {trade_date}")
    return payload[list(columns)].copy()


def _merge_frames(
    existing: pd.DataFrame,
    fetched: pd.DataFrame,
    *,
    columns: tuple[str, ...],
    primary_key: tuple[str, ...],
) -> pd.DataFrame:
    frames = [frame[list(columns)] for frame in (existing, fetched) if not frame.empty]
    if not frames:
        return pd.DataFrame(columns=columns)
    combined = pd.concat(frames, ignore_index=True)
    if "trade_date" in combined.columns:
        combined["trade_date"] = pd.to_datetime(
            combined["trade_date"], errors="raise"
        ).dt.date
    if "trade_time" in combined.columns:
        combined["trade_time"] = pd.to_datetime(
            combined["trade_time"], errors="raise"
        )
    if "created_at" in combined.columns:
        combined["created_at"] = pd.to_datetime(
            combined["created_at"], errors="raise"
        )
    business_columns = [column for column in columns if column != "created_at"]
    combined["_business_hash"] = pd.util.hash_pandas_object(
        combined[business_columns],
        index=False,
    )
    business_versions = combined.groupby(list(primary_key), dropna=False)[
        "_business_hash"
    ].transform("nunique")
    ordered = combined.sort_values("created_at")
    unchanged = ordered.loc[business_versions.loc[ordered.index] == 1].drop_duplicates(
        list(primary_key), keep="first"
    )
    revised = ordered.loc[business_versions.loc[ordered.index] > 1].drop_duplicates(
        list(primary_key), keep="last"
    )
    return (
        pd.concat([unchanged, revised], ignore_index=True)[list(columns)]
        .sort_values(list(primary_key))
        .reset_index(drop=True)
    )


def _build_export_source(
    *,
    trade_date: date,
    minutes: pd.DataFrame,
    auction: pd.DataFrame,
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
    connection.execute("INSERT INTO trade_calendar VALUES ('SSE', ?, TRUE)", [trade_date])
    for table, frame, columns in (
        ("minute_bar", minutes, _MINUTE_COLUMNS),
        ("auction_bar", auction, _AUCTION_COLUMNS),
    ):
        if frame.empty:
            continue
        view_name = f"{table}_input"
        connection.register(view_name, frame)
        selected = ", ".join(columns)
        connection.execute(f"INSERT INTO {table} ({selected}) SELECT {selected} FROM {view_name}")
        connection.unregister(view_name)
    return connection


def _minute_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    expected_codes: set[str],
    export: ResearchExportSummary,
) -> ResearchDatasetIngestAudit:
    rows = connection.execute(
        """
        SELECT ts_code, trade_time
        FROM minute_bar
        WHERE freq = '1min'
        ORDER BY ts_code, trade_time
        """
    ).fetchall()
    observed_by_code: dict[str, set[time]] = {}
    for ts_code, trade_time in rows:
        observed_by_code.setdefault(str(ts_code), set()).add(trade_time.time())
    observed_codes = set(observed_by_code)
    expected_grid = {
        value.time() for value in pd.date_range("2000-01-01 09:30", "2000-01-01 11:30", freq="1min")
    } | {
        value.time() for value in pd.date_range("2000-01-01 13:01", "2000-01-01 15:00", freq="1min")
    }
    complete_codes = {
        ts_code
        for ts_code, observed_grid in observed_by_code.items()
        if ts_code in expected_codes and expected_grid == observed_grid
    }
    expected_count = len(expected_codes)
    bounds = connection.execute(
        "SELECT MIN(trade_time), MAX(trade_time) FROM minute_bar"
    ).fetchone()
    return ResearchDatasetIngestAudit(
        dataset="minute_bar",
        export=export,
        expected_code_count=expected_count,
        observed_code_count=len(observed_codes),
        complete_code_count=len(complete_codes),
        unexpected_code_count=len(observed_codes - expected_codes),
        coverage_ratio=(None if expected_count == 0 else len(complete_codes) / expected_count),
        observed_precision_ratio=(
            None
            if not observed_codes
            else len(observed_codes & expected_codes) / len(observed_codes)
        ),
        earliest_time=None if bounds is None else bounds[0],
        latest_time=None if bounds is None else bounds[1],
    )


def _auction_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    expected_codes: set[str],
    export: ResearchExportSummary,
) -> ResearchDatasetIngestAudit:
    rows = connection.execute(
        "SELECT DISTINCT ts_code FROM auction_bar ORDER BY ts_code"
    ).fetchall()
    observed_codes = {str(row[0]) for row in rows}
    complete_count = len(expected_codes & observed_codes)
    expected_count = len(expected_codes)
    bounds = connection.execute(
        "SELECT MIN(trade_date), MAX(trade_date) FROM auction_bar"
    ).fetchone()
    return ResearchDatasetIngestAudit(
        dataset="auction_bar",
        export=export,
        expected_code_count=expected_count,
        observed_code_count=len(observed_codes),
        complete_code_count=complete_count,
        unexpected_code_count=len(observed_codes - expected_codes),
        coverage_ratio=(None if expected_count == 0 else complete_count / expected_count),
        observed_precision_ratio=(
            None if not observed_codes else complete_count / len(observed_codes)
        ),
        earliest_time=None if bounds is None else bounds[0],
        latest_time=None if bounds is None else bounds[1],
    )


def _parse_research_observation(payload: bytes | str) -> ResearchAuthorityObservation:
    decoded = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    document = json.loads(decoded)
    if not isinstance(document, dict):
        raise ValueError("research observation must be a JSON object")
    kind = document.get("observation_kind", "daily_ingest")
    if kind == "daily_ingest":
        return ResearchDailyIngestResult.model_validate_json(decoded)
    if kind == "auction_repair":
        return ResearchAuctionRepairObservation.model_validate_json(decoded)
    if kind == "minute_repair":
        return ResearchMinuteRepairObservation.model_validate_json(decoded)
    raise ValueError(f"unsupported research observation kind: {kind}")


def _observation_path(
    paths: ResearchIngestPaths,
    result: ResearchAuthorityObservation,
) -> Path:
    return (
        paths.state_dir
        / "research_observations"
        / f"trade_date={result.trade_date.isoformat()}"
        / f"{result.observation_id}.json"
    )


def _observation_index(
    paths: ResearchIngestPaths,
) -> tuple[dict[str, tuple[ResearchAuthorityObservation, bytes]], int]:
    index: dict[str, tuple[ResearchAuthorityObservation, bytes]] = {}
    files = tuple(sorted((paths.state_dir / "research_observations").glob("**/*.json")))
    for path in files:
        if not path.is_file() or path.is_symlink():
            raise RuntimeError(f"invalid research observation path: {path}")
        payload = path.read_bytes()
        digest = hashlib.sha256(payload).hexdigest()
        if digest in index:
            raise RuntimeError("duplicate research observation content hash")
        index[digest] = (
            _parse_research_observation(payload),
            payload,
        )
    return index, len(files)


def _observation_chain_issues(
    paths: ResearchIngestPaths,
    current: ResearchAuthorityObservation,
    current_payload: bytes,
) -> tuple[str, ...]:
    issues: list[str] = []
    try:
        index, _ = _observation_index(paths)
    except Exception:
        return ("observation_index_invalid",)
    authoritative_path = _observation_path(paths, current)
    if (
        not authoritative_path.is_file()
        or authoritative_path.is_symlink()
        or authoritative_path.read_bytes() != current_payload
    ):
        issues.append("current_observation_mismatch")
        return tuple(issues)

    latest_repair_bindings: dict[
        str,
        tuple[ResearchPartitionManifest, str],
    ] = {}
    seen: set[str] = set()
    node = current
    while node.previous_observation_sha256 is not None:
        if isinstance(
            node,
            (ResearchAuctionRepairObservation, ResearchMinuteRepairObservation),
        ):
            for change in node.repairs:
                latest_repair_bindings.setdefault(
                    change.after_manifest.partition.partition_id,
                    (change.after_manifest, change.after_manifest_sha256),
                )
        parent_hash = node.previous_observation_sha256
        if parent_hash in seen or parent_hash not in index:
            issues.append("observation_lineage_broken")
            break
        seen.add(parent_hash)
        parent = index[parent_hash][0]
        if (
            parent.bootstrap_snapshot_id != current.bootstrap_snapshot_id
            or parent.trade_date > node.trade_date
            or (
                isinstance(
                    node,
                    (
                        ResearchAuctionRepairObservation,
                        ResearchMinuteRepairObservation,
                    ),
                )
                and (
                    parent.trade_date != node.trade_date
                    or node.catalog_before_sha256 != parent.catalog_sha256
                    or node.readonly_catalog_before_sha256
                    != parent.readonly_catalog_sha256
                )
            )
        ):
            issues.append("observation_lineage_broken")
            break
        node = parent
    if (
        not issues
        and latest_repair_bindings
        and (
            isinstance(
                current,
                (
                    ResearchAuctionRepairObservation,
                    ResearchMinuteRepairObservation,
                ),
            )
            or current.stable_trading_days < 10
        )
    ):
        repair_issues = _manifest_binding_issues(
            paths,
            tuple(latest_repair_bindings.values()),
        )
        if repair_issues:
            issues.extend(repair_issues)

    if isinstance(current, ResearchDailyIngestResult) and current.status == "candidate":
        node = current
        expected_days = current.stable_trading_days
        while expected_days > 0:
            if (
                not isinstance(node, ResearchDailyIngestResult)
                or node.status != "candidate"
                or node.stable_trading_days != expected_days
            ):
                issues.append("stability_chain_broken")
                break
            node_lake_issues = _lake_binding_issues(paths, node)
            if node_lake_issues:
                issues.extend(node_lake_issues)
                break
            if expected_days == 1:
                break
            parent_hash = node.stability_parent_sha256
            if parent_hash is None or parent_hash not in index:
                issues.append("stability_chain_broken")
                break
            parent = index[parent_hash][0]
            if (
                not isinstance(parent, ResearchDailyIngestResult)
                or node.previous_stable_trade_date != parent.trade_date
                or parent.trade_date >= node.trade_date
                or parent.bootstrap_snapshot_id != current.bootstrap_snapshot_id
            ):
                issues.append("stability_chain_broken")
                break
            node = parent
            expected_days -= 1
    return tuple(issues)


def _lake_binding_issues(
    paths: ResearchIngestPaths,
    current: ResearchAuthorityObservation,
) -> tuple[str, ...]:
    if isinstance(current, ResearchDailyIngestResult):
        bindings: tuple[tuple[ResearchPartitionManifest | None, str | None], ...] = tuple(
            (partition.manifest, None)
            for audit in (current.minute, current.auction)
            for partition in audit.export.partitions
        )
    else:
        bindings = tuple(
            (change.after_manifest, change.after_manifest_sha256)
            for change in current.repairs
        )
    return _manifest_binding_issues(paths, bindings)


def _manifest_binding_issues(
    paths: ResearchIngestPaths,
    bindings: tuple[
        tuple[ResearchPartitionManifest | None, str | None],
        ...,
    ],
) -> tuple[str, ...]:
    try:
        catalog_connection = duckdb.connect(str(paths.catalog_path), read_only=True)
    except Exception:
        return ("research_catalog_unreadable",)
    try:
        for manifest, expected_manifest_hash in bindings:
            if manifest is None:
                return ("lake_manifest_missing_from_observation",)
            manifest_path = paths.lake_root / partition_manifest_relative_path(
                manifest.partition
            )
            data_path = paths.lake_root / manifest.relative_path
            try:
                observed_manifest = ResearchPartitionManifest.model_validate_json(
                    manifest_path.read_text(encoding="utf-8")
                )
            except Exception:
                return ("lake_manifest_invalid",)
            if (
                expected_manifest_hash is not None
                and _file_sha256(manifest_path) != expected_manifest_hash
            ):
                return ("lake_manifest_hash_mismatch",)
            if observed_manifest != manifest:
                return ("lake_manifest_mismatch",)
            if (
                not data_path.is_file()
                or data_path.is_symlink()
                or data_path.stat().st_size != manifest.file_size
                or _file_sha256(data_path) != manifest.file_hash
            ):
                return ("lake_partition_hash_mismatch",)
            catalog_row = catalog_connection.execute(
                """
                SELECT relative_path, content_hash, file_hash, manifest_json
                FROM research_partition
                WHERE partition_id = ?
                """,
                [manifest.partition.partition_id],
            ).fetchone()
            if catalog_row is None:
                return ("catalog_partition_missing",)
            catalog_manifest = ResearchPartitionManifest.model_validate_json(catalog_row[3])
            if (
                str(catalog_row[0]) != manifest.relative_path
                or str(catalog_row[1]) != manifest.content_hash
                or str(catalog_row[2]) != manifest.file_hash
                or catalog_manifest != manifest
            ):
                return ("catalog_partition_mismatch",)
    finally:
        catalog_connection.close()
    return ()


def _catalog_lake_integrity_issues(paths: ResearchIngestPaths) -> tuple[str, ...]:
    """Hash every catalog partition; used at bootstrap and promotion boundaries."""
    try:
        with duckdb.connect(str(paths.catalog_path), read_only=True) as connection:
            rows = connection.execute(
                """
                SELECT relative_path, content_hash, file_hash, manifest_json
                FROM research_partition
                ORDER BY partition_id
                """
            ).fetchall()
    except Exception:
        return ("research_catalog_unreadable",)
    for relative_path, content_hash, file_hash, manifest_json in rows:
        try:
            manifest = ResearchPartitionManifest.model_validate_json(manifest_json)
            manifest_path = paths.lake_root / partition_manifest_relative_path(manifest.partition)
            data_path = paths.lake_root / manifest.relative_path
            observed_manifest = ResearchPartitionManifest.model_validate_json(
                manifest_path.read_text(encoding="utf-8")
            )
        except Exception:
            return ("catalog_lake_manifest_invalid",)
        if observed_manifest != manifest:
            return ("catalog_lake_manifest_mismatch",)
        if (
            str(relative_path) != manifest.relative_path
            or str(content_hash) != manifest.content_hash
            or str(file_hash) != manifest.file_hash
        ):
            return ("catalog_lake_record_mismatch",)
        if (
            not data_path.is_file()
            or data_path.is_symlink()
            or data_path.stat().st_size != manifest.file_size
            or _file_sha256(data_path) != manifest.file_hash
        ):
            return ("catalog_lake_partition_hash_mismatch",)
    return ()


def _validate_prior_authority(
    paths: ResearchIngestPaths,
) -> tuple[str, ResearchAuthorityObservation | None, str | None, str]:
    candidate_path = paths.state_dir / "research-authority-candidate.json"
    catalog_path = paths.catalog_path
    current_path = paths.state_dir / "research-authority-current.json"
    if not candidate_path.is_file() or candidate_path.is_symlink():
        raise RuntimeError("research bootstrap authority candidate is missing")
    if not catalog_path.is_file() or catalog_path.is_symlink():
        raise RuntimeError("research catalog is missing")
    candidate = ResearchAuthorityCandidate.model_validate_json(
        candidate_path.read_text(encoding="utf-8")
    )
    if current_path.exists():
        if not current_path.is_file() or current_path.is_symlink():
            raise RuntimeError("research authority current marker is invalid")
        current_payload = current_path.read_bytes()
        current = _parse_research_observation(current_payload)
        if current.status == "planned":
            raise RuntimeError("planned research ingest cannot be an authority marker")
        if current.bootstrap_snapshot_id != candidate.snapshot_id:
            raise RuntimeError("research authority bootstrap lineage mismatch")
        if current.catalog_sha256 != _file_sha256(catalog_path):
            raise RuntimeError("research authority current catalog hash mismatch")
        readonly_path = paths.readonly_catalog_path
        if not readonly_path.is_file() or current.readonly_catalog_sha256 != _file_sha256(
            readonly_path
        ):
            raise RuntimeError("research authority current readonly hash mismatch")
        chain_issues = _observation_chain_issues(paths, current, current_payload)
        if chain_issues:
            raise RuntimeError(f"research observation chain invalid: {', '.join(chain_issues)}")
        lake_issues = _lake_binding_issues(paths, current)
        if lake_issues:
            raise RuntimeError(f"research lake binding invalid: {', '.join(lake_issues)}")
        if (
            isinstance(current, ResearchDailyIngestResult)
            and current.status == "candidate"
            and current.stable_trading_days >= 10
        ):
            catalog_issues = _catalog_lake_integrity_issues(paths)
            if catalog_issues:
                raise RuntimeError(
                    "research catalog/lake integrity invalid: "
                    f"{', '.join(catalog_issues)}"
                )
        catalog_hash = _file_sha256(catalog_path)
        return (
            candidate.snapshot_id,
            current,
            hashlib.sha256(current_payload).hexdigest(),
            catalog_hash,
        )
    catalog_hash = _file_sha256(catalog_path)
    if candidate.catalog_sha256 != catalog_hash:
        raise RuntimeError("research bootstrap candidate catalog hash mismatch")
    bootstrap_issues = _catalog_lake_integrity_issues(paths)
    if bootstrap_issues:
        raise RuntimeError(
            f"research bootstrap lake integrity invalid: {', '.join(bootstrap_issues)}"
        )
    return candidate.snapshot_id, None, None, catalog_hash


def _partition_keys(trade_date: date) -> tuple[ResearchPartitionKey, ...]:
    return (
        ResearchPartitionKey(dataset="minute_bar", trade_date=trade_date, freq="1min"),
        ResearchPartitionKey(dataset="auction_bar", trade_date=trade_date),
    )


def _copy_file_atomic(source: Path, target: Path) -> None:
    _mkdir_durable(target.parent)
    temp_path = target.with_name(f".{target.name}.tmp-{uuid.uuid4().hex}")
    try:
        shutil.copyfile(source, temp_path)
        with temp_path.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temp_path, target)
        _fsync_directory(target.parent)
    finally:
        temp_path.unlink(missing_ok=True)


def _verify_catalog_file(path: Path) -> None:
    with duckdb.connect(str(path), read_only=True) as connection:
        tables = {
            str(row[0])
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }
    required = {
        "research_partition",
        "research_ingest_run",
        "research_dataset_coverage",
    }
    if not required.issubset(tables):
        raise RuntimeError("research catalog is missing required tables")


def _prepare_catalog_generation(
    paths: ResearchIngestPaths,
    *,
    trade_date: date,
    transaction_root: Path,
    expected_hash: str,
) -> tuple[Path, Path]:
    staged_catalog = transaction_root / "catalog.next.duckdb"
    staged_lake = transaction_root / "lake.next"
    _mkdir_durable(transaction_root)
    catalog = ResearchCatalog(paths.catalog_path)
    manifest_hashes: list[str | None] = []
    with ExitStack() as stack:
        for key in _partition_keys(trade_date):
            stack.enter_context(
                exclusive_file_lock(paths.lake_root / partition_directory(key) / ".export.lock")
            )
        stack.enter_context(exclusive_file_lock(catalog.lock_path))
        if _file_sha256(paths.catalog_path) != expected_hash:
            raise RuntimeError("research catalog changed before ingest generation copy")
        shutil.copyfile(paths.catalog_path, staged_catalog)
        with staged_catalog.open("rb") as handle:
            os.fsync(handle.fileno())
        for key in _partition_keys(trade_date):
            live_partition = paths.lake_root / partition_directory(key)
            staged_partition = staged_lake / partition_directory(key)
            manifest_path = paths.lake_root / partition_manifest_relative_path(key)
            manifest_hashes.append(_file_sha256(manifest_path) if manifest_path.is_file() else None)
            if live_partition.is_dir():
                shutil.copytree(live_partition, staged_partition)
    _write_model_atomic(
        transaction_root / "generation-baseline.json",
        _ResearchGenerationBaseline(
            catalog_sha256=expected_hash,
            minute_manifest_sha256=manifest_hashes[0],
            auction_manifest_sha256=manifest_hashes[1],
        ),
    )
    return staged_catalog, staged_lake


def _prepare_readonly_generation(catalog_path: Path, readonly_path: Path) -> str:
    shutil.copyfile(catalog_path, readonly_path)
    with readonly_path.open("rb") as handle:
        os.fsync(handle.fileno())
    _verify_catalog_file(readonly_path)
    return _file_sha256(readonly_path)


def _journal_path(transaction_root: Path) -> Path:
    return transaction_root / "publish-journal.json"


def _prepare_publish_journal(
    paths: ResearchIngestPaths,
    *,
    transaction_root: Path,
    staged_catalog: Path,
    staged_readonly: Path,
    result: ResearchDailyIngestResult,
) -> _ResearchPublishJournal:
    trade_date = result.trade_date
    observation_path = _observation_path(paths, result)
    if observation_path.exists():
        raise RuntimeError("research observation path already exists")
    minute_manifest = paths.lake_root / partition_manifest_relative_path(
        _partition_keys(trade_date)[0]
    )
    auction_manifest = paths.lake_root / partition_manifest_relative_path(
        _partition_keys(trade_date)[1]
    )
    staged_manifests = tuple(
        ResearchPartitionManifest.model_validate_json(
            (transaction_root / "lake.next" / partition_manifest_relative_path(key)).read_text(
                encoding="utf-8"
            )
        )
        for key in _partition_keys(trade_date)
    )
    version_paths = tuple(paths.lake_root / item.relative_path for item in staged_manifests)
    staged_manifest_paths = tuple(
        transaction_root / "lake.next" / partition_manifest_relative_path(key)
        for key in _partition_keys(trade_date)
    )
    result_hash = hashlib.sha256(
        (result.model_dump_json(indent=2) + "\n").encode("utf-8")
    ).hexdigest()
    current_path = paths.state_dir / "research-authority-current.json"
    targets = (
        (paths.catalog_path, transaction_root / "catalog.before", True),
        (
            paths.readonly_catalog_path,
            transaction_root / "readonly.before",
            paths.readonly_catalog_path.is_file(),
        ),
        (current_path, transaction_root / "current.before", current_path.is_file()),
        (minute_manifest, transaction_root / "minute-manifest.before", minute_manifest.is_file()),
        (
            auction_manifest,
            transaction_root / "auction-manifest.before",
            auction_manifest.is_file(),
        ),
    )
    before_hashes: list[str | None] = []
    for target, backup, existed in targets:
        if existed:
            if not target.is_file() or target.is_symlink():
                raise RuntimeError(f"research publish target is invalid: {target}")
            before_hash = _file_sha256(target)
            shutil.copyfile(target, backup)
            with backup.open("rb") as handle:
                os.fsync(handle.fileno())
            if _file_sha256(backup) != before_hash:
                raise RuntimeError(f"research publish backup verification failed: {backup}")
            before_hashes.append(before_hash)
        else:
            before_hashes.append(None)
    journal = _ResearchPublishJournal(
        transaction_id=transaction_root.name,
        trade_date=trade_date,
        created_at=result.generated_at,
        observation_path=observation_path,
        readonly_existed=paths.readonly_catalog_path.is_file(),
        current_existed=current_path.is_file(),
        minute_manifest_existed=minute_manifest.is_file(),
        auction_manifest_existed=auction_manifest.is_file(),
        minute_version_relative_path=staged_manifests[0].relative_path,
        auction_version_relative_path=staged_manifests[1].relative_path,
        minute_version_existed=version_paths[0].is_file(),
        auction_version_existed=version_paths[1].is_file(),
        catalog_before_sha256=before_hashes[0],
        readonly_before_sha256=before_hashes[1],
        current_before_sha256=before_hashes[2],
        minute_manifest_before_sha256=before_hashes[3],
        auction_manifest_before_sha256=before_hashes[4],
        catalog_after_sha256=_file_sha256(staged_catalog),
        readonly_after_sha256=_file_sha256(staged_readonly),
        minute_manifest_after_sha256=_file_sha256(staged_manifest_paths[0]),
        auction_manifest_after_sha256=_file_sha256(staged_manifest_paths[1]),
        current_after_sha256=result_hash,
        minute_version_sha256=staged_manifests[0].file_hash,
        auction_version_sha256=staged_manifests[1].file_hash,
    )
    _write_model_atomic(_journal_path(transaction_root), journal)
    return journal


def _restore_file(
    target: Path,
    backup: Path,
    existed: bool,
    *,
    expected_before_hash: str | None,
    expected_after_hash: str,
) -> None:
    if target.is_symlink():
        raise RuntimeError(f"research publish target became a symlink: {target}")
    observed_hash = _file_sha256(target) if target.is_file() else None
    if existed:
        if expected_before_hash is None:
            raise RuntimeError(f"research publish before hash is missing: {target}")
        if not backup.is_file() or backup.is_symlink():
            raise RuntimeError(f"research publish backup is missing: {backup}")
        if _file_sha256(backup) != expected_before_hash:
            raise RuntimeError(f"research publish backup hash mismatch: {backup}")
        if observed_hash == expected_before_hash:
            return
        if observed_hash != expected_after_hash:
            raise RuntimeError(f"research publish rollback CAS mismatch: {target}")
        _copy_file_atomic(backup, target)
    else:
        if expected_before_hash is not None:
            raise RuntimeError(f"unexpected research publish before hash: {target}")
        if observed_hash is None:
            return
        if observed_hash != expected_after_hash:
            raise RuntimeError(f"research publish rollback CAS mismatch: {target}")
        target.unlink(missing_ok=True)
        if target.parent.exists():
            _fsync_directory(target.parent)


def _rollback_publish_transaction(paths: ResearchIngestPaths, transaction_root: Path) -> None:
    journal_path = _journal_path(transaction_root)
    if not journal_path.is_file() or journal_path.is_symlink():
        raise RuntimeError(f"research publish journal is invalid: {journal_path}")
    journal = _ResearchPublishJournal.model_validate_json(journal_path.read_text(encoding="utf-8"))
    if journal.transaction_id != transaction_root.name:
        raise RuntimeError("research publish journal transaction mismatch")
    minute_manifest = paths.lake_root / partition_manifest_relative_path(
        _partition_keys(journal.trade_date)[0]
    )
    auction_manifest = paths.lake_root / partition_manifest_relative_path(
        _partition_keys(journal.trade_date)[1]
    )
    keys = _partition_keys(journal.trade_date)
    restore_targets = (
        (
            paths.catalog_path,
            transaction_root / "catalog.before",
            True,
            journal.catalog_before_sha256,
            journal.catalog_after_sha256,
        ),
        (
            paths.readonly_catalog_path,
            transaction_root / "readonly.before",
            journal.readonly_existed,
            journal.readonly_before_sha256,
            journal.readonly_after_sha256,
        ),
        (
            paths.state_dir / "research-authority-current.json",
            transaction_root / "current.before",
            journal.current_existed,
            journal.current_before_sha256,
            journal.current_after_sha256,
        ),
        (
            minute_manifest,
            transaction_root / "minute-manifest.before",
            journal.minute_manifest_existed,
            journal.minute_manifest_before_sha256,
            journal.minute_manifest_after_sha256,
        ),
        (
            auction_manifest,
            transaction_root / "auction-manifest.before",
            journal.auction_manifest_existed,
            journal.auction_manifest_before_sha256,
            journal.auction_manifest_after_sha256,
        ),
    )
    version_targets: list[tuple[Path, bool, str]] = []
    lake_root = paths.lake_root.resolve()
    for relative_path, existed, expected_hash in (
        (
            journal.minute_version_relative_path,
            journal.minute_version_existed,
            journal.minute_version_sha256,
        ),
        (
            journal.auction_version_relative_path,
            journal.auction_version_existed,
            journal.auction_version_sha256,
        ),
    ):
        version_path = paths.lake_root / relative_path
        resolved_version = version_path.resolve()
        if lake_root not in resolved_version.parents:
            raise RuntimeError("research publish journal version path escaped lake root")
        version_targets.append((version_path, existed, expected_hash))
    observations_root = (paths.state_dir / "research_observations").resolve()
    observation_path = journal.observation_path
    if observations_root not in observation_path.resolve().parents:
        raise RuntimeError("research publish journal observation path escaped state root")
    with ExitStack() as stack:
        for key in keys:
            stack.enter_context(
                exclusive_file_lock(paths.lake_root / partition_directory(key) / ".export.lock")
            )
        stack.enter_context(exclusive_file_lock(ResearchCatalog(paths.catalog_path).lock_path))
        for target, backup, existed, before_hash, after_hash in restore_targets:
            if existed:
                if before_hash is None:
                    raise RuntimeError(f"research publish before hash is missing: {target}")
                if not backup.is_file() or backup.is_symlink():
                    raise RuntimeError(f"research publish backup is missing: {backup}")
                if _file_sha256(backup) != before_hash:
                    raise RuntimeError(f"research publish backup hash mismatch: {backup}")
            elif before_hash is not None:
                raise RuntimeError(f"unexpected research publish before hash: {target}")
            if target.is_symlink() or (target.exists() and not target.is_file()):
                raise RuntimeError(f"research publish rollback CAS mismatch: {target}")
            observed_hash = _file_sha256(target) if target.is_file() else None
            allowed_hashes = (
                {before_hash, after_hash} if existed else {None, after_hash}
            )
            if observed_hash not in allowed_hashes:
                raise RuntimeError(f"research publish rollback CAS mismatch: {target}")
        for version_path, existed, expected_hash in version_targets:
            if version_path.is_symlink() or (
                version_path.exists() and not version_path.is_file()
            ):
                raise RuntimeError("research publish rollback version CAS mismatch")
            observed_hash = (
                _file_sha256(version_path) if version_path.is_file() else None
            )
            if existed:
                if observed_hash != expected_hash:
                    raise RuntimeError("research publish rollback version CAS mismatch")
            elif observed_hash not in {None, expected_hash}:
                raise RuntimeError("research publish rollback version CAS mismatch")
        if observation_path.is_symlink() or (
            observation_path.exists() and not observation_path.is_file()
        ):
            raise RuntimeError("research publish rollback observation CAS mismatch")
        observation_hash = (
            _file_sha256(observation_path) if observation_path.is_file() else None
        )
        if observation_hash not in {None, journal.current_after_sha256}:
            raise RuntimeError("research publish rollback observation CAS mismatch")
        for target, backup, existed, before_hash, after_hash in restore_targets:
            _restore_file(
                target,
                backup,
                existed,
                expected_before_hash=before_hash,
                expected_after_hash=after_hash,
            )
        for version_path, existed, expected_hash in version_targets:
            if not existed:
                if version_path.is_file() and _file_sha256(version_path) != expected_hash:
                    raise RuntimeError("research publish rollback version CAS mismatch")
                version_path.unlink(missing_ok=True)
                if version_path.parent.exists():
                    _fsync_directory(version_path.parent)
        if observation_path.exists() and (
            not observation_path.is_file()
            or observation_path.is_symlink()
            or _file_sha256(observation_path) != journal.current_after_sha256
        ):
            raise RuntimeError("research publish rollback observation CAS mismatch")
        observation_path.unlink(missing_ok=True)
        if observation_path.parent.exists():
            _fsync_directory(observation_path.parent)
    _remove_transaction_root(transaction_root)


def _remove_transaction_root(transaction_root: Path) -> None:
    parent = transaction_root.parent
    shutil.rmtree(transaction_root)
    if parent.exists():
        _fsync_directory(parent)


def _recover_interrupted_publish(paths: ResearchIngestPaths) -> None:
    if not paths.transactions_root.exists():
        return
    for transaction_root in sorted(paths.transactions_root.iterdir()):
        if not transaction_root.is_dir() or transaction_root.is_symlink():
            raise RuntimeError(f"invalid research transaction path: {transaction_root}")
        journal_paths = {
            "daily": _journal_path(transaction_root),
            "auction_repair": (
                transaction_root / "auction-repair-journal.json"
            ),
            "minute_repair": (
                transaction_root / "minute-repair-journal.json"
            ),
        }
        present = tuple(
            kind for kind, path in journal_paths.items() if path.exists()
        )
        if len(present) > 1:
            raise RuntimeError("research transaction contains multiple publish journals")
        if present == ("daily",):
            _rollback_publish_transaction(paths, transaction_root)
        elif present == ("auction_repair",):
            from rquant.research_repair import (
                rollback_research_auction_repair_publish,
            )

            rollback_research_auction_repair_publish(paths, transaction_root)
        elif present == ("minute_repair",):
            from rquant.research_minute_repair import (
                rollback_research_minute_repair_publish,
            )

            rollback_research_minute_repair_publish(
                paths,
                transaction_root,
            )
        else:
            _remove_transaction_root(transaction_root)


def _publish_generation(
    paths: ResearchIngestPaths,
    *,
    transaction_root: Path,
    staged_catalog: Path,
    staged_readonly: Path,
    result: ResearchDailyIngestResult,
) -> None:
    observation_path = _observation_path(paths, result)
    try:
        keys = _partition_keys(result.trade_date)
        baseline = _ResearchGenerationBaseline.model_validate_json(
            (transaction_root / "generation-baseline.json").read_text(encoding="utf-8")
        )
        with ExitStack() as stack:
            for key in keys:
                lock_path = paths.lake_root / partition_directory(key) / ".export.lock"
                stack.enter_context(exclusive_file_lock(lock_path))
            stack.enter_context(exclusive_file_lock(ResearchCatalog(paths.catalog_path).lock_path))
            if _file_sha256(paths.catalog_path) != baseline.catalog_sha256:
                raise RuntimeError("research catalog changed during daily ingest")
            for key, expected_manifest_hash in zip(
                keys,
                (
                    baseline.minute_manifest_sha256,
                    baseline.auction_manifest_sha256,
                ),
                strict=True,
            ):
                manifest_path = paths.lake_root / partition_manifest_relative_path(key)
                observed_hash = _file_sha256(manifest_path) if manifest_path.is_file() else None
                if observed_hash != expected_manifest_hash:
                    raise RuntimeError("research lake manifest changed during daily ingest")
            _prepare_publish_journal(
                paths,
                transaction_root=transaction_root,
                staged_catalog=staged_catalog,
                staged_readonly=staged_readonly,
                result=result,
            )
            for key in keys:
                relative_manifest = partition_manifest_relative_path(key)
                staged_manifest_path = transaction_root / "lake.next" / relative_manifest
                manifest = ResearchPartitionManifest.model_validate_json(
                    staged_manifest_path.read_text(encoding="utf-8")
                )
                staged_data = transaction_root / "lake.next" / manifest.relative_path
                live_data = paths.lake_root / manifest.relative_path
                if live_data.exists():
                    if not live_data.is_file() or _file_sha256(live_data) != manifest.file_hash:
                        raise RuntimeError("existing immutable research partition hash mismatch")
                else:
                    _copy_file_atomic(staged_data, live_data)
                _copy_file_atomic(
                    staged_manifest_path,
                    paths.lake_root / relative_manifest,
                )
            _copy_file_atomic(staged_catalog, paths.catalog_path)
            _copy_file_atomic(staged_readonly, paths.readonly_catalog_path)
            _write_model_atomic(observation_path, result)
            _write_model_atomic(paths.state_dir / "research-authority-current.json", result)
        _journal_path(transaction_root).unlink()
        _fsync_directory(transaction_root)
        _remove_transaction_root(transaction_root)
    except BaseException:
        if _journal_path(transaction_root).exists():
            try:
                _rollback_publish_transaction(paths, transaction_root)
            except Exception as rollback_error:
                raise RuntimeError(
                    f"research publish failed and rollback is pending: {rollback_error}"
                ) from rollback_error
        raise


def _previous_open_date(connection: duckdb.DuckDBPyConnection, trade_date: date) -> date | None:
    row = connection.execute(
        """
        SELECT MAX(cal_date)
        FROM trade_calendar
        WHERE exchange = 'SSE' AND is_open = TRUE AND cal_date < ?
        """,
        [trade_date],
    ).fetchone()
    return None if row is None else row[0]


def _latest_complete_research_date(catalog_path: Path) -> date | None:
    with duckdb.connect(str(catalog_path), read_only=True) as connection:
        row = connection.execute(
            """
            SELECT MAX(trade_date)
            FROM (
                SELECT trade_date
                FROM research_partition
                WHERE dataset IN ('minute_bar', 'auction_bar')
                GROUP BY trade_date
                HAVING SUM(
                    CASE WHEN dataset = 'minute_bar' AND freq = '1min' THEN 1 ELSE 0 END
                ) > 0
                AND SUM(CASE WHEN dataset = 'auction_bar' THEN 1 ELSE 0 END) > 0
            )
            """
        ).fetchone()
    return None if row is None else row[0]


def _require_observation_continuity(
    paths: ResearchIngestPaths,
    *,
    trade_date: date,
    previous_open_date: date | None,
    previous: ResearchAuthorityObservation | None,
) -> None:
    if previous is not None:
        if previous.trade_date in {trade_date, previous_open_date}:
            return
        raise RuntimeError(
            "research observation gap: recover the earliest missing trade date first"
        )
    bootstrap_anchor = _latest_complete_research_date(paths.catalog_path)
    if bootstrap_anchor is not None and bootstrap_anchor != previous_open_date:
        raise RuntimeError(
            "research observation gap from bootstrap catalog: "
            "recover the earliest missing trade date first"
        )


def _stability_evidence(
    *,
    status: Literal["candidate", "degraded"],
    trade_date: date,
    previous_open_date: date | None,
    previous: ResearchAuthorityObservation | None,
    previous_marker_hash: str | None,
) -> tuple[int, str | None, date | None]:
    if status != "candidate":
        return 0, None, None
    if (
        previous is None
        or not isinstance(previous, ResearchDailyIngestResult)
        or previous.status != "candidate"
    ):
        return 1, None, None
    if previous.trade_date == trade_date:
        return (
            max(1, previous.stable_trading_days),
            previous.stability_parent_sha256,
            previous.previous_stable_trade_date,
        )
    if previous.trade_date == previous_open_date and previous_marker_hash is not None:
        return previous.stable_trading_days + 1, previous_marker_hash, previous.trade_date
    return 1, None, None


def _observation_id(trade_date: date, generated_at: datetime) -> str:
    stamp = generated_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"research-daily-{trade_date.isoformat()}-{stamp}-{uuid.uuid4().hex[:8]}"


def run_daily_research_ingest(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    trade_date: date,
    adapter: ResearchIngestAdapter | None,
    code_commit: str,
    dry_run: bool = False,
    recovery: bool = False,
    now: Callable[[], datetime] | None = None,
) -> ResearchDailyIngestResult:
    """Seal one trading day's minute/auction partitions without writing production DB."""
    clock = now or (lambda: datetime.now(_CST))
    generated_at = clock()
    if generated_at.tzinfo is None or generated_at.utcoffset() is None:
        raise ValueError("research ingest clock must be timezone-aware")
    generated_at = generated_at.astimezone(_CST)
    if trade_date > generated_at.date():
        raise ValueError("research ingest cannot target a future trade date")
    if recovery and trade_date >= generated_at.date():
        raise ValueError("historical recovery requires a past trade date")
    if (
        recovery
        and not dry_run
        and generated_at.weekday() < 5
        and _MARKET_PROTECTION_START <= generated_at.time() <= _MARKET_PROTECTION_END
    ):
        raise ValueError("historical recovery is forbidden during market protection window")
    if not recovery and not dry_run and trade_date != generated_at.date():
        raise ValueError("rt_min_daily ingest only supports the current trade date")
    if not recovery and not dry_run and generated_at.time() < _MINIMUM_SAFE_TIME:
        raise ValueError("current trading day research ingest is forbidden before 15:15 CST")
    if not dry_run and _CLEAN_COMMIT_PATTERN.fullmatch(code_commit) is None:
        raise ValueError("research ingest requires a clean 40-character code commit")

    source_database = Path(source_database)
    if not source_database.is_file() or source_database.is_symlink():
        raise ValueError(f"source read-only database is invalid: {source_database}")
    if dry_run:
        return _run_daily_research_ingest_locked(
            source_database=source_database,
            paths=paths,
            trade_date=trade_date,
            adapter=adapter,
            code_commit=code_commit,
            dry_run=True,
            recovery=recovery,
            generated_at=generated_at,
        )
    _mkdir_durable(paths.state_dir)
    with exclusive_file_lock(paths.publish_lock_path):
        _recover_interrupted_publish(paths)
        return _run_daily_research_ingest_locked(
            source_database=source_database,
            paths=paths,
            trade_date=trade_date,
            adapter=adapter,
            code_commit=code_commit,
            dry_run=False,
            recovery=recovery,
            generated_at=generated_at,
        )


def _run_daily_research_ingest_locked(
    *,
    source_database: Path,
    paths: ResearchIngestPaths,
    trade_date: date,
    adapter: ResearchIngestAdapter | None,
    code_commit: str,
    dry_run: bool,
    recovery: bool,
    generated_at: datetime,
) -> ResearchDailyIngestResult:
    watchlist = _load_watchlist_snapshot(paths.staging_root, trade_date)

    with duckdb.connect(str(source_database), read_only=True) as source:
        _require_open_trade_date(source, trade_date)
        existing_minutes = _query_existing_frame(source, table="minute_bar", trade_date=trade_date)
        existing_auction = _query_existing_frame(source, table="auction_bar", trade_date=trade_date)
        expected_auction_codes = _expected_auction_codes(source, trade_date)
        auction_universe_complete = _daily_bar_universe_is_complete(
            source,
            trade_date,
            expected_auction_codes,
        )
        previous_open_date = _previous_open_date(source, trade_date)

    research_minutes = _query_existing_research_partition(
        paths,
        _partition_keys(trade_date)[0],
        _MINUTE_COLUMNS,
    )
    research_auction = _query_existing_research_partition(
        paths,
        _partition_keys(trade_date)[1],
        _AUCTION_COLUMNS,
    )
    existing_minutes = _merge_frames(
        research_minutes,
        existing_minutes,
        columns=_MINUTE_COLUMNS,
        primary_key=("ts_code", "trade_time", "freq", "source"),
    )
    existing_auction = _merge_frames(
        research_auction,
        existing_auction,
        columns=_AUCTION_COLUMNS,
        primary_key=("ts_code", "trade_date", "auction_type", "source"),
    )

    observed_minute_codes = set(existing_minutes.get("ts_code", pd.Series(dtype=str)).astype(str))
    expected_minute_codes = (
        {item.ts_code for item in watchlist.items}
        if watchlist is not None
        else observed_minute_codes
    )

    if dry_run:
        fetched_minutes = pd.DataFrame(columns=_MINUTE_COLUMNS)
        fetched_auction = pd.DataFrame(columns=_AUCTION_COLUMNS)
        bootstrap_snapshot_id = None
        previous = None
        previous_marker_hash = None
        previous_catalog_hash = None
    else:
        if adapter is None:
            raise ValueError("non-dry-run research ingest requires a data adapter")
        (
            bootstrap_snapshot_id,
            previous,
            previous_marker_hash,
            previous_catalog_hash,
        ) = _validate_prior_authority(paths)
        _require_observation_continuity(
            paths,
            trade_date=trade_date,
            previous_open_date=previous_open_date,
            previous=previous,
        )
        if recovery:
            minute_raw = _fetch_historical_minutes(
                adapter,
                ts_codes=expected_minute_codes,
                trade_date=trade_date,
            )
        else:
            minute_raw = adapter.rt_min_daily(
                sorted(expected_minute_codes), freq="1min"
            )
        auction_raw = adapter.stk_auction(trade_date)
        fetched_minutes = _normalize_fetched_frame(
            minute_raw,
            columns=_MINUTE_COLUMNS,
            trade_date=trade_date,
            generated_at=generated_at,
            dataset="minute_bar",
        )
        fetched_auction = _normalize_fetched_frame(
            auction_raw,
            columns=_AUCTION_COLUMNS,
            trade_date=trade_date,
            generated_at=generated_at,
            dataset="auction_bar",
        )

    minutes = _merge_frames(
        existing_minutes,
        fetched_minutes,
        columns=_MINUTE_COLUMNS,
        primary_key=("ts_code", "trade_time", "freq", "source"),
    )
    auction = _merge_frames(
        existing_auction,
        fetched_auction,
        columns=_AUCTION_COLUMNS,
        primary_key=("ts_code", "trade_date", "auction_type", "source"),
    )
    export_source = _build_export_source(
        trade_date=trade_date,
        minutes=minutes,
        auction=auction,
    )
    transaction_root: Path | None = None
    try:
        if dry_run:
            export_catalog_path = paths.catalog_path
            export_lake_root = paths.lake_root
        else:
            if previous_catalog_hash is None:  # pragma: no cover - guarded above
                raise RuntimeError("research catalog base hash is missing")
            _mkdir_durable(paths.transactions_root)
            transaction_root = paths.transactions_root / f"tx-{uuid.uuid4().hex}"
            export_catalog_path, export_lake_root = _prepare_catalog_generation(
                paths,
                trade_date=trade_date,
                transaction_root=transaction_root,
                expected_hash=previous_catalog_hash,
            )
        catalog = ResearchCatalog(export_catalog_path)
        minute_export = export_research_dataset(
            export_source,
            catalog=catalog,
            lake_root=export_lake_root,
            dataset="minute_bar",
            start_date=trade_date,
            end_date=trade_date,
            code_commit=code_commit,
            dry_run=dry_run,
            as_of_date=trade_date,
        )
        auction_export = export_research_dataset(
            export_source,
            catalog=catalog,
            lake_root=export_lake_root,
            dataset="auction_bar",
            start_date=trade_date,
            end_date=trade_date,
            code_commit=code_commit,
            dry_run=dry_run,
            as_of_date=trade_date,
        )
        minute_audit = _minute_audit(
            export_source,
            expected_codes=expected_minute_codes,
            export=minute_export,
        )
        auction_audit = _auction_audit(
            export_source,
            expected_codes=expected_auction_codes,
            export=auction_export,
        )
    except BaseException:
        if (
            transaction_root is not None
            and transaction_root.exists()
            and not _journal_path(transaction_root).exists()
        ):
            _remove_transaction_root(transaction_root)
        raise
    finally:
        export_source.close()

    observation_id = _observation_id(trade_date, generated_at)
    if dry_run:
        return ResearchDailyIngestResult(
            status="planned",
            observation_id=observation_id,
            bootstrap_snapshot_id=None,
            trade_date=trade_date,
            generated_at=generated_at,
            code_commit=code_commit,
            stable_trading_days=0,
            minute=minute_audit,
            auction=auction_audit,
            issues=(),
        )

    issues: list[str] = []
    if watchlist is None:
        issues.append("watchlist_snapshot_missing")
    elif watchlist.code_commit != code_commit:
        issues.append("watchlist_code_commit_mismatch")
    if minute_audit.expected_code_count == 0:
        issues.append("minute_expected_universe_empty")
    elif minute_audit.coverage_ratio != 1.0:
        issues.append("minute_watchlist_coverage_incomplete")
    if auction_audit.expected_code_count == 0:
        issues.append("auction_expected_universe_missing")
    elif not auction_universe_complete:
        issues.append("daily_bar_auction_universe_incomplete")
    elif (
        auction_audit.coverage_ratio is None
        or auction_audit.coverage_ratio < _MINIMUM_AUCTION_COVERAGE
    ):
        issues.append("auction_market_coverage_below_98pct")
    if (
        auction_audit.observed_precision_ratio is None
        or auction_audit.observed_precision_ratio < _MINIMUM_AUCTION_COVERAGE
    ):
        issues.append("auction_observed_precision_below_98pct")
    status: Literal["candidate", "degraded"] = "candidate" if not issues else "degraded"
    stable_days, stability_parent_hash, previous_stable_trade_date = _stability_evidence(
        status=status,
        trade_date=trade_date,
        previous_open_date=previous_open_date,
        previous=previous,
        previous_marker_hash=previous_marker_hash,
    )
    if transaction_root is None:  # pragma: no cover - non-dry path creates it above
        raise RuntimeError("research publish transaction was not prepared")
    staged_catalog = transaction_root / "catalog.next.duckdb"
    staged_readonly = transaction_root / "readonly.next.duckdb"
    try:
        catalog_hash = _file_sha256(staged_catalog)
        readonly_hash = _prepare_readonly_generation(staged_catalog, staged_readonly)
        result = ResearchDailyIngestResult(
            status=status,
            observation_id=observation_id,
            bootstrap_snapshot_id=bootstrap_snapshot_id,
            trade_date=trade_date,
            generated_at=generated_at,
            code_commit=code_commit,
            previous_observation_sha256=previous_marker_hash,
            stability_parent_sha256=stability_parent_hash,
            previous_stable_trade_date=previous_stable_trade_date,
            catalog_sha256=catalog_hash,
            readonly_catalog_sha256=readonly_hash,
            stable_trading_days=stable_days,
            minute=minute_audit,
            auction=auction_audit,
            issues=tuple(issues),
        )
        _publish_generation(
            paths,
            transaction_root=transaction_root,
            staged_catalog=staged_catalog,
            staged_readonly=staged_readonly,
            result=result,
        )
    finally:
        if transaction_root.exists() and not _journal_path(transaction_root).exists():
            _remove_transaction_root(transaction_root)
    return result


def inspect_research_authority(paths: ResearchIngestPaths) -> ResearchAuthorityStatus:
    """Verify bootstrap/current markers and fail closed on any catalog drift."""
    candidate_path = paths.state_dir / "research-authority-candidate.json"
    catalog_path = paths.catalog_path
    readonly_path = paths.readonly_catalog_path
    current_path = paths.state_dir / "research-authority-current.json"
    observation_count = len(tuple((paths.state_dir / "research_observations").glob("**/*.json")))
    pending_journals = (
        *paths.transactions_root.glob("*/publish-journal.json"),
        *paths.transactions_root.glob("*/auction-repair-journal.json"),
        *paths.transactions_root.glob("*/minute-repair-journal.json"),
    )
    if pending_journals:
        return ResearchAuthorityStatus(
            status="invalid",
            bootstrap_snapshot_id=None,
            latest_trade_date=None,
            stable_trading_days=0,
            observation_count=observation_count,
            catalog_hash_matches=False,
            readonly_catalog_hash_matches=False,
            eligible_for_promotion=False,
            issues=("interrupted_publish_pending_recovery",),
        )
    if (
        not candidate_path.is_file()
        or candidate_path.is_symlink()
        or not catalog_path.is_file()
        or catalog_path.is_symlink()
    ):
        return ResearchAuthorityStatus(
            status="missing",
            bootstrap_snapshot_id=None,
            latest_trade_date=None,
            stable_trading_days=0,
            observation_count=observation_count,
            catalog_hash_matches=False,
            readonly_catalog_hash_matches=False,
            eligible_for_promotion=False,
            issues=("bootstrap_candidate_or_catalog_missing",),
        )
    try:
        candidate = ResearchAuthorityCandidate.model_validate_json(
            candidate_path.read_text(encoding="utf-8")
        )
    except Exception:
        return ResearchAuthorityStatus(
            status="invalid",
            bootstrap_snapshot_id=None,
            latest_trade_date=None,
            stable_trading_days=0,
            observation_count=observation_count,
            catalog_hash_matches=False,
            readonly_catalog_hash_matches=False,
            eligible_for_promotion=False,
            issues=("bootstrap_candidate_invalid",),
        )
    if not current_path.exists():
        catalog_matches = candidate.catalog_sha256 == _file_sha256(catalog_path)
        integrity_issues = _catalog_lake_integrity_issues(paths) if catalog_matches else ()
        bootstrap_valid = catalog_matches and not integrity_issues
        return ResearchAuthorityStatus(
            status="bootstrap_candidate" if bootstrap_valid else "invalid",
            bootstrap_snapshot_id=candidate.snapshot_id,
            latest_trade_date=None,
            stable_trading_days=0,
            observation_count=observation_count,
            catalog_hash_matches=catalog_matches,
            readonly_catalog_hash_matches=False,
            eligible_for_promotion=False,
            issues=(
                integrity_issues
                if integrity_issues
                else (() if catalog_matches else ("catalog_hash_mismatch",))
            ),
        )
    try:
        if not current_path.is_file() or current_path.is_symlink():
            raise ValueError("current marker is not a regular file")
        current_payload = current_path.read_bytes()
        current = _parse_research_observation(current_payload)
    except Exception:
        return ResearchAuthorityStatus(
            status="invalid",
            bootstrap_snapshot_id=candidate.snapshot_id,
            latest_trade_date=None,
            stable_trading_days=0,
            observation_count=observation_count,
            catalog_hash_matches=False,
            readonly_catalog_hash_matches=False,
            eligible_for_promotion=False,
            issues=("current_authority_marker_invalid",),
        )
    issues: list[str] = []
    catalog_matches = current.catalog_sha256 is not None and current.catalog_sha256 == _file_sha256(
        catalog_path
    )
    readonly_matches = (
        current.readonly_catalog_sha256 is not None
        and readonly_path.is_file()
        and current.readonly_catalog_sha256 == _file_sha256(readonly_path)
    )
    if current.bootstrap_snapshot_id != candidate.snapshot_id:
        issues.append("bootstrap_lineage_mismatch")
    if not catalog_matches:
        issues.append("catalog_hash_mismatch")
    if not readonly_matches:
        issues.append("readonly_catalog_hash_mismatch")
    issues.extend(_observation_chain_issues(paths, current, current_payload))
    issues.extend(_lake_binding_issues(paths, current))
    if current.status == "planned":
        issues.append("planned_marker_cannot_be_authoritative")
    if (
        not issues
        and isinstance(current, ResearchDailyIngestResult)
        and current.status == "candidate"
        and current.stable_trading_days >= 10
    ):
        issues.extend(_catalog_lake_integrity_issues(paths))
    status: Literal["candidate", "degraded", "invalid"]
    if issues:
        status = "invalid"
    elif current.status == "candidate":
        status = "candidate"
    else:
        status = "degraded"
    eligible = (
        status == "candidate"
        and isinstance(current, ResearchDailyIngestResult)
        and current.stable_trading_days >= 10
    )
    return ResearchAuthorityStatus(
        status=status,
        bootstrap_snapshot_id=candidate.snapshot_id,
        latest_trade_date=current.trade_date,
        stable_trading_days=current.stable_trading_days,
        observation_count=observation_count,
        catalog_hash_matches=catalog_matches,
        readonly_catalog_hash_matches=readonly_matches,
        eligible_for_promotion=eligible,
        issues=tuple(issues or current.issues),
    )
