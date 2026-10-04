"""Original board auction calculations with explicit retrospective input evidence."""

from __future__ import annotations

import hashlib
import math
import os
import stat
from collections.abc import Callable
from contextlib import suppress
from datetime import date, datetime, time, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Literal

import duckdb
import pandas as pd
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from rquant.board_auction_strength import auction_amount_ratio_from_rows, gap_up_ratio_from_rows
from rquant.data_contracts import EXCHANGE_TIMEZONE
from rquant.data_metadata import DatasetSnapshotArtifact, normalize_utc_datetime, utc_now
from rquant.factor.result_artifact import _open_private_root, _require_same_root, _root_path
from rquant.factor.source_prepare import (
    FactorSourceFileIdentity,
    _check_generation,
    _generation,
    _identity,
)
from rquant.pit_visibility import VisibilityQueryScope, query_visible_rows
from rquant.readside_replica_gate import connect_pinned_readonly
from rquant.research_lake import _quoted_literal
from rquant.research_snapshot import materialize_table_dependency, verify_snapshot_artifact
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_dependencies import StrategyTableDependency

if TYPE_CHECKING:
    from rquant.factor.daily_feature_source import (
        FactorAuctionPrepareRequest,
        FactorDailyFeatureSource,
    )

_MODEL = ConfigDict(frozen=True, extra="forbid", strict=True, revalidate_instances="always")
_SHA = r"^[0-9a-f]{64}$"
AuctionColumn = Literal["board_gap_up_ratio", "board_auction_amount_ratio", "board_member_count"]
AuctionReason = Literal[
    "missing_board_membership",
    "missing_board_auction",
    "missing_previous_close",
    "missing_auction_history",
    "zero_auction_baseline",
    "auction_null",
    "auction_non_finite",
    "invalid_auction_price",
    "invalid_auction_amount",
    "invalid_previous_close",
]
AUCTION_DESCRIPTIONS = (
    (
        "board_auction_amount_ratio",
        "题材竞价金额比",
        "ratio",
        "题材竞价总额/当前成员前最多20个实际竞价日总额中位数；倍数原值，前一完整SSE开市日。",
    ),
    (
        "board_gap_up_ratio",
        "题材竞价高开占比",
        "ratio",
        "题材内有效竞价价高于昨收的成员占比；0–1比例原值，前一完整SSE开市日。",
    ),
    (
        "board_member_count",
        "题材成员数",
        "observations",
        "原题材完整成员数，不按所选股票池重算；前一完整SSE开市日。",
    ),
)
AUCTION_COLUMNS = tuple(item[0] for item in AUCTION_DESCRIPTIONS)


class FactorAuctionPolicy(BaseModel):
    model_config = _MODEL
    version: Literal["original-board-auction-v1"] = "original-board-auction-v1"
    implementation_sha256: str = Field(pattern=_SHA)
    signal_clock: Literal["09:30:00"] = "09:30:00"
    official_available_clock: Literal["09:26:00"] = "09:26:00"
    minute_fallback_available_clock: Literal["09:31:00"] = "09:31:00"
    evaluation_clock: Literal["previous_complete_sse_session_at_next_day_09:25"] = (
        "previous_complete_sse_session_at_next_day_09:25"
    )
    history_mode: Literal["retrospective_no_row_first_observed_time"] = (
        "retrospective_no_row_first_observed_time"
    )
    membership_lookback_days: Literal[30] = 30
    historical_days: Literal[20] = 20
    selection: Literal["highest_amount_ratio_first_input_board_on_tie"] = (
        "highest_amount_ratio_first_input_board_on_tie"
    )
    historical_members: Literal["fixed_signal_members_complete_board_domain"] = (
        "fixed_signal_members_complete_board_domain"
    )
    missing_policy: Literal["explicit_reason_no_zero_or_neighbor_fallback"] = (
        "explicit_reason_no_zero_or_neighbor_fallback"
    )


class FactorAuctionFileEvidence(BaseModel):
    model_config = _MODEL
    path: Path
    sha256: str = Field(pattern=_SHA)
    byte_count: int = Field(gt=0, le=256 * 1024 * 1024)

    @model_validator(mode="after")
    def _path(self) -> FactorAuctionFileEvidence:
        if _root_path(self.path) != self.path:
            raise ValueError("auction evidence path must be canonical and absolute")
        return self


class FactorAuctionLakeInput(BaseModel):
    model_config = _MODEL
    lake_root: Path
    catalog_file: FactorAuctionFileEvidence
    current_marker: FactorAuctionFileEvidence
    candidate_marker: FactorAuctionFileEvidence
    catalog_role: Literal["catalog", "readonly_catalog"] = "readonly_catalog"
    artifacts: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=1, max_length=4096)
    manifest_sha256: str = Field(pattern=_SHA)

    @model_validator(mode="after")
    def _named_input(self) -> FactorAuctionLakeInput:
        if _root_path(self.lake_root) != self.lake_root:
            raise ValueError("auction lake path must be canonical and absolute")
        keys = tuple(a.partition_id for a in self.artifacts)
        if (
            keys != tuple(sorted(set(keys)))
            or any(
                a.dataset_id != "auction_bar" or a.artifact_type != "lake_partition"
                for a in self.artifacts
            )
            or self.manifest_sha256 != canonical_sha256(self.artifacts)
        ):
            raise ValueError("auction lake input requires unique ordered named partition evidence")
        return self


class FactorAuctionFrozenFile(FactorAuctionFileEvidence):
    identity: FactorSourceFileIdentity

    @model_validator(mode="after")
    def _identity(self) -> FactorAuctionFrozenFile:
        if self.identity.size != self.byte_count:
            raise ValueError("auction frozen file identity differs from byte count")
        return self


class FactorAuctionLakeReceipt(BaseModel):
    model_config = _MODEL
    snapshot_id: str = Field(min_length=1, max_length=128)
    observation_id: str = Field(min_length=1, max_length=128)
    catalog_role: Literal["catalog", "readonly_catalog"]
    catalog_file: FactorAuctionFrozenFile
    current_marker: FactorAuctionFrozenFile
    candidate_marker: FactorAuctionFrozenFile
    catalog_sha256: str = Field(pattern=_SHA)
    current_marker_sha256: str = Field(pattern=_SHA)
    candidate_marker_sha256: str = Field(pattern=_SHA)
    manifest_sha256: str = Field(pattern=_SHA)
    catalog_status: Literal["candidate", "degraded"]
    catalog_issues: tuple[str, ...] = Field(max_length=64)
    partition_count: int = Field(gt=0, le=4096)
    verification_scope: Literal[
        "named_partitions_current_observation_not_full_authority_chain_or_pit"
    ] = "named_partitions_current_observation_not_full_authority_chain_or_pit"
    artifact: DatasetSnapshotArtifact

    @model_validator(mode="after")
    def _evidence(self) -> FactorAuctionLakeReceipt:
        if (
            (self.catalog_sha256, self.current_marker_sha256, self.candidate_marker_sha256)
            != (self.catalog_file.sha256, self.current_marker.sha256, self.candidate_marker.sha256)
            or self.artifact.row_count != self.partition_count
            or self.artifact.table_name != "auction_partition_manifest"
        ):
            raise ValueError(
                "auction lake receipt differs from frozen files or named partition manifest"
            )
        return self


class FactorAuctionDiagnostic(BaseModel):
    model_config = _MODEL
    board_code: str | None = Field(default=None, max_length=64, exclude_if=lambda v: v is None)
    board_name: str | None = Field(default=None, max_length=128, exclude_if=lambda v: v is None)
    membership_date: date | None = Field(default=None, exclude_if=lambda v: v is None)
    previous_close_date: date | None = Field(default=None, exclude_if=lambda v: v is None)
    member_count: int = Field(ge=0, le=50_000)
    valid_gap_members: int = Field(ge=0, le=50_000)
    historical_observation_days: int = Field(ge=0, le=20)
    auction_sources: tuple[Literal["tushare", "minute_0930_fallback"], ...]
    latest_available_at: AwareDatetime | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def _coherence(self) -> FactorAuctionDiagnostic:
        if (
            self.valid_gap_members > self.member_count
            or self.auction_sources != tuple(sorted(set(self.auction_sources)))
            or (bool(self.auction_sources) != (self.latest_available_at is not None))
            or (self.board_code is None) != (self.board_name is None)
            or (self.board_code is None) != (self.membership_date is None)
            or (self.board_code is None) != (self.member_count == 0)
        ):
            raise ValueError("auction diagnostic dates, members or source identity differ")
        return self


class FactorAuctionSummary(BaseModel):
    model_config = _MODEL
    policy: FactorAuctionPolicy
    input_content_sha256: str = Field(pattern=_SHA)
    output_content_sha256: str = Field(pattern=_SHA)
    input_rows: int = Field(ge=0, le=16_000_000)
    output_rows: int = Field(ge=0, le=28_672_000)
    lake: FactorAuctionLakeReceipt | None = Field(default=None, exclude_if=lambda v: v is None)


class FactorAuctionReceipt(BaseModel):
    model_config = _MODEL
    policy: FactorAuctionPolicy
    inputs: tuple[DatasetSnapshotArtifact, ...] = Field(min_length=4, max_length=4)
    artifact: DatasetSnapshotArtifact
    input_rows: int = Field(ge=0, le=16_000_000)
    output_rows: int = Field(ge=0, le=28_672_000)
    board_evaluations: int = Field(ge=0)
    max_input_rows: int = Field(gt=0, le=16_000_000)
    max_output_cells: int = Field(gt=0, le=128_000_000)
    lake: FactorAuctionLakeReceipt | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def _artifacts(self) -> FactorAuctionReceipt:
        expected = (
            "auction_membership_input",
            "auction_close_input",
            "auction_session_input",
            "auction_bar",
        )
        keys = (
            ("trade_date", "board_code", "con_code"),
            ("ts_code", "trade_date"),
            ("trade_date",),
            ("ts_code", "trade_date", "auction_type", "source"),
        )
        if (
            tuple(a.table_name for a in self.inputs) != expected
            or self.input_rows != sum(a.row_count for a in self.inputs)
            or self.input_rows > self.max_input_rows
            or self.artifact.table_name != "daily_auction_feature"
            or self.artifact.dataset_id != "factor_auction_features"
            or self.artifact.row_count != self.output_rows
            or self.artifact.primary_key != ("ts_code", "trade_date")
            or any(
                a.dataset_id != "factor_auction_input" or a.primary_key != key
                for a, key in zip(self.inputs, keys, strict=True)
            )
            or any(
                a.event_column != "trade_date"
                or a.relative_path != f"tables/{a.table_name}/versions/{a.file_hash}.parquet"
                for a in self.inputs + (self.artifact,)
            )
            or any(
                a.artifact_type != "materialized_table" or a.file_size is None
                for a in self.inputs + (self.artifact,)
            )
        ):
            raise ValueError("auction receipt differs from sealed complete input or output")
        return self

    def summary(self) -> FactorAuctionSummary:
        return FactorAuctionSummary(
            policy=self.policy,
            input_content_sha256=canonical_sha256(tuple(a.content_hash for a in self.inputs)),
            output_content_sha256=self.artifact.content_hash,
            input_rows=self.input_rows,
            output_rows=self.output_rows,
            lake=self.lake,
        )

    def causal_policy(self) -> FactorAuctionPolicy:
        return self.policy


def _copy_evidence(
    evidence: FactorAuctionFileEvidence, target: Path, *, limit: int
) -> tuple[FactorAuctionFrozenFile, bytes | None]:
    descriptor = os.open(evidence.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size != evidence.byte_count
            or before.st_size > limit
        ):
            raise ValueError("auction evidence byte count exceeds frozen bound")
        digest = hashlib.sha256()
        copied = 0
        with os.fdopen(os.dup(descriptor), "rb") as incoming, target.open("wb") as outgoing:
            for chunk in iter(lambda: incoming.read(1024 * 1024), b""):
                copied += len(chunk)
                if copied > evidence.byte_count:
                    raise ValueError("auction evidence grew beyond frozen byte bound")
                digest.update(chunk)
                outgoing.write(chunk)
        after = os.fstat(descriptor)
        if (
            _identity(before) != _identity(after)
            or _identity(before) != _identity(evidence.path.stat(follow_symlinks=False))
            or digest.hexdigest() != evidence.sha256
        ):
            raise ValueError("auction frozen evidence identity or digest changed")
        captured = FactorAuctionFrozenFile(
            **evidence.model_dump(),
            identity=FactorSourceFileIdentity(
                device=before.st_dev,
                inode=before.st_ino,
                size=before.st_size,
                mtime_ns=before.st_mtime_ns,
                ctime_ns=before.st_ctime_ns,
            ),
        )
        return captured, target.read_bytes() if limit <= 2 * 1024 * 1024 else None
    finally:
        os.close(descriptor)


def _lake_receipt(
    source: FactorAuctionLakeInput,
    connection: duckdb.DuckDBPyConnection,
    scratch: Path,
    *,
    output_root: Path,
    as_of: datetime,
) -> FactorAuctionLakeReceipt:
    from rquant.research_catalog import ResearchPartitionRecord
    from rquant.research_ingest import _parse_research_observation
    from rquant.research_migration import ResearchAuthorityCandidate
    from rquant.research_snapshot import _manifest_from_record

    catalog = scratch / "frozen-catalog.duckdb"
    catalog_file, _ = _copy_evidence(source.catalog_file, catalog, limit=256 * 1024 * 1024)
    current_file, current_bytes = _copy_evidence(
        source.current_marker, scratch / "current.json", limit=2 * 1024 * 1024
    )
    candidate_file, candidate_bytes = _copy_evidence(
        source.candidate_marker, scratch / "candidate.json", limit=2 * 1024 * 1024
    )
    current = _parse_research_observation(current_bytes)
    candidate = ResearchAuthorityCandidate.model_validate_json(candidate_bytes)
    if (
        current.status not in ("candidate", "degraded")
        or current.bootstrap_snapshot_id != candidate.snapshot_id
        or getattr(current, source.catalog_role + "_sha256") != source.catalog_file.sha256
    ):
        raise ValueError("auction current catalog hash or bootstrap lineage differs")
    connection.execute(
        "ATTACH " + _quoted_literal(str(catalog)) + " AS auction_catalog (READ_ONLY)"
    )
    connection.execute(
        "CREATE TEMP TABLE auction_partition_manifest(partition_id VARCHAR PR"
        "IMARY KEY,trade_date DATE,artifact_json VARCHAR)"
    )
    try:
        for artifact in source.artifacts:
            rows = connection.execute(
                "SELECT * FROM auction_catalog.research_partition WHERE partition_id=?",
                [artifact.partition_id],
            ).fetchmany(2)
            if len(rows) != 1:
                raise ValueError("auction named partition is absent or repeated in frozen catalog")
            names = tuple(item[0] for item in connection.description)
            record = ResearchPartitionRecord.model_validate(dict(zip(names, rows[0], strict=True)))
            manifest = _manifest_from_record(record)
            for field in (
                "relative_path",
                "row_count",
                "schema_hash",
                "content_hash",
                "file_hash",
                "file_size",
                "source",
                "primary_key",
            ):
                if getattr(manifest, field) != getattr(artifact, field):
                    raise ValueError("auction artifact differs from frozen catalog manifest")
            if (
                artifact.revision_created_at != manifest.created_at
                or artifact.catalog_updated_at != record.updated_at
            ):
                raise ValueError("auction artifact manifest identity differs")
            verify_snapshot_artifact(artifact, lake_root=source.lake_root, as_of_time=as_of)
            connection.execute(
                "INSERT INTO auction_partition_manifest VALUES(?,?,?)",
                [artifact.partition_id, manifest.partition.trade_date, artifact.model_dump_json()],
            )
        dates = connection.execute(
            "SELECT min(trade_date),max(trade_date) FROM auction_partition_manifest"
        ).fetchone()
        frozen = materialize_table_dependency(
            connection,
            dependency=StrategyTableDependency(
                dataset_id="factor_auction_manifest",
                table_name="auction_partition_manifest",
                date_column="trade_date",
            ),
            artifact_root=output_root,
            start_date=dates[0],
            end_date=dates[1],
            as_of_time=as_of,
        )
        return FactorAuctionLakeReceipt(
            snapshot_id=candidate.snapshot_id,
            observation_id=current.observation_id,
            catalog_role=source.catalog_role,
            catalog_sha256=source.catalog_file.sha256,
            catalog_file=catalog_file,
            current_marker=current_file,
            candidate_marker=candidate_file,
            current_marker_sha256=source.current_marker.sha256,
            candidate_marker_sha256=source.candidate_marker.sha256,
            manifest_sha256=source.manifest_sha256,
            catalog_status=current.status,
            catalog_issues=current.issues,
            partition_count=len(source.artifacts),
            artifact=frozen,
        )
    finally:
        connection.execute("DETACH auction_catalog")


def _stage_inputs(
    connection: duckdb.DuckDBPyConnection,
    request: FactorAuctionPrepareRequest,
    *,
    scratch: Path,
    root: Path,
) -> tuple[tuple[DatasetSnapshotArtifact, ...], FactorAuctionLakeReceipt | None]:
    scope = request.prepared_source.receipt.request.scope
    lower = scope.start_date - timedelta(days=30)
    connection.execute(
        "CREATE TEMP TABLE auction_membership_input(trade_date DATE,board_cod"
        "e VARCHAR,board_name VARCHAR,con_code VARCHAR,input_order BIGINT,PRI"
        "MARY KEY(trade_date,board_code,con_code))"
    )
    membership_rows = connection.execute(
        "SELECT count(*) FROM kpl_concept_member_daily WHERE trade_date BETWEEN ? AND ?",
        [lower, scope.end_date],
    ).fetchone()[0]
    staged_rows = membership_rows + len(request.prepared_source.receipt.calendar_open_days)
    if staged_rows > request.max_input_rows:
        raise ValueError("auction complete input row budget exceeded")
    duplicates = connection.execute(
        (
            "SELECT count(*) FROM (SELECT trade_date,board_code,con_code FROM kpl"
            "_concept_member_daily WHERE trade_date BETWEEN ? AND ? GROUP BY ALL "
            "HAVING count(*)>1)"
        ),
        [lower, scope.end_date],
    ).fetchone()[0]
    if duplicates:
        raise ValueError("duplicate auction membership input")
    connection.execute(
        (
            "INSERT INTO auction_membership_input SELECT trade_date,board_code,bo"
            "ard_name,con_code,row_number() OVER() FROM kpl_concept_member_daily "
            "WHERE trade_date BETWEEN ? AND ?"
        ),
        [lower, scope.end_date],
    )
    if (
        connection.execute(
            "SELECT count(*) FROM auction_membership_input WHERE board_code IS NU"
            "LL OR board_name IS NULL OR con_code IS NULL OR NOT regexp_full_matc"
            "h(con_code,'[0-9]{6}\\.(SZ|SH|BJ)')"
        ).fetchone()[0]
        or connection.execute(
            "SELECT count(*) FROM (SELECT trade_date,board_code FROM auction_memb"
            "ership_input GROUP BY ALL HAVING count(DISTINCT board_name)>1)"
        ).fetchone()[0]
    ):
        raise ValueError("auction membership input has invalid or conflicting identity")
    connection.execute(
        "CREATE TEMP TABLE auction_session_input(trade_date DATE PRIMARY KEY,"
        "previous_close_date DATE)"
    )
    connection.execute(
        (
            "INSERT INTO auction_session_input SELECT day,(SELECT max(trade_date)"
            " FROM daily_bar WHERE trade_date<day) FROM unnest(?) days(day)"
        ),
        [list(request.prepared_source.receipt.calendar_open_days)],
    )
    connection.execute(
        "CREATE TEMP TABLE auction_close_input(ts_code VARCHAR,trade_date DAT"
        "E,close DOUBLE,PRIMARY KEY(ts_code,trade_date))"
    )
    close_query = (
        "SELECT ts_code,trade_date,close FROM daily_bar WHERE trade_date IN ("
        "SELECT previous_close_date FROM auction_session_input) AND ts_code I"
        "N (SELECT DISTINCT con_code FROM auction_membership_input)"
    )
    staged_rows += connection.execute("SELECT count(*) FROM (" + close_query + ")").fetchone()[0]
    if staged_rows > request.max_input_rows:
        raise ValueError("auction complete input row budget exceeded")
    connection.execute("INSERT INTO auction_close_input " + close_query)
    connection.execute(
        "CREATE TEMP TABLE auction_input(ts_code VARCHAR,trade_date DATE,auct"
        "ion_type VARCHAR,price DOUBLE,amount DOUBLE,source VARCHAR,PRIMARY K"
        "EY(ts_code,trade_date,auction_type,source))"
    )
    lake = None
    if request.lake_input is not None:
        lake = _lake_receipt(
            request.lake_input, connection, scratch, output_root=root, as_of=scope.as_of_time
        )
        for artifact in request.lake_input.artifacts:
            path = verify_snapshot_artifact(
                artifact, lake_root=request.lake_input.lake_root, as_of_time=scope.as_of_time
            )
            insert = (
                "SELECT ts_code,trade_date,auction_type,price,amount,source FROM read"
                "_parquet(?,hive_partitioning=false) WHERE trade_date<=? AND ts_code "
                "IN (SELECT DISTINCT con_code FROM auction_membership_input)"
            )
            rows = connection.execute(
                "SELECT count(*) FROM (" + insert + ")", [str(path), scope.end_date]
            ).fetchone()[0]
            current = connection.execute("SELECT count(*) FROM auction_input").fetchone()[0]
            if staged_rows + current + rows > request.max_input_rows:
                raise ValueError("auction complete input row budget exceeded")
            connection.execute("INSERT INTO auction_input " + insert, [str(path), scope.end_date])
            verify_snapshot_artifact(
                artifact, lake_root=request.lake_input.lake_root, as_of_time=scope.as_of_time
            )
    else:
        count = connection.execute(
            (
                "SELECT count(*) FROM auction_bar WHERE trade_date<=? AND ts_code IN "
                "(SELECT DISTINCT con_code FROM auction_membership_input)"
            ),
            [scope.end_date],
        ).fetchone()[0]
        if staged_rows + count > request.max_input_rows:
            raise ValueError("auction complete input row budget exceeded")
        connection.execute(
            (
                "INSERT INTO auction_input SELECT ts_code,trade_date,auction_type,pri"
                "ce,amount,source FROM auction_bar WHERE trade_date<=? AND ts_code IN"
                " (SELECT DISTINCT con_code FROM auction_membership_input)"
            ),
            [scope.end_date],
        )
    tables = (
        "auction_membership_input",
        "auction_close_input",
        "auction_session_input",
        "auction_input",
    )
    count = sum(
        connection.execute("SELECT count(*) FROM " + table).fetchone()[0] for table in tables
    )
    if count > request.max_input_rows:
        raise ValueError("auction complete input row budget exceeded")
    inputs = []
    for table in tables:
        first = (
            connection.execute("SELECT min(trade_date) FROM " + table).fetchone()[0]
            or scope.start_date
        )
        exported = "auction_bar" if table == "auction_input" else table
        if table == "auction_input":
            connection.execute(
                "CREATE TEMP TABLE auction_bar(ts_code VARCHAR,trade_date DATE,auctio"
                "n_type VARCHAR,price DOUBLE,amount DOUBLE,source VARCHAR,PRIMARY KEY"
                "(ts_code,trade_date,auction_type,source))"
            )
            connection.execute("INSERT INTO auction_bar SELECT * FROM auction_input")
        inputs.append(
            materialize_table_dependency(
                connection,
                dependency=StrategyTableDependency(
                    dataset_id="factor_auction_input", table_name=exported, date_column="trade_date"
                ),
                artifact_root=root,
                start_date=first,
                end_date=scope.end_date,
                as_of_time=scope.as_of_time,
            )
        )
    return tuple(inputs), lake


def _non_finite(values: pd.Series) -> str | None:
    for value in values.dropna():
        if not math.isfinite(float(value)):
            return "NaN" if math.isnan(float(value)) else "Infinity" if value > 0 else "-Infinity"
    if any(isinstance(value, float) and math.isnan(value) for value in values):
        return "NaN"
    return None


def _board_fact(
    members: list[str],
    auctions: pd.DataFrame,
    closes: pd.DataFrame,
    day: date,
    *,
    board_code: str,
    board_name: str,
    membership_date: date,
    previous_date: date | None,
) -> tuple[dict[str, tuple[float | None, str | None, str | None]], FactorAuctionDiagnostic]:
    selected = auctions.loc[auctions.ts_code.isin(members)]
    current = selected.loc[selected.trade_date == day]
    previous = closes.loc[closes.ts_code.isin(members)]
    joined = current.merge(previous, on="ts_code", how="inner")
    history = selected.loc[selected.trade_date < day]
    # The original kernel takes dates before discarding null day totals.
    historical_dates = (
        history.groupby("trade_date").amount.sum(min_count=1).sort_index(ascending=False).head(20)
    )
    history = history.loc[history.trade_date.isin(historical_dates.index)]
    used = pd.concat([current, history], ignore_index=True)
    history_days = len(historical_dates.dropna())
    gap_reason, amount_reason, gap_tag, amount_tag = None, None, None, None
    if current.empty:
        gap_reason = amount_reason = "missing_board_auction"
    else:
        gap_tag = _non_finite(current.loc[~current.price__null, "price"])
        amount_tag = _non_finite(used.loc[~used.amount__null, "amount"])
        if gap_tag:
            gap_reason = "auction_non_finite"
        elif current.price.isna().all():
            gap_reason = "auction_null"
        elif (current.price.dropna() <= 0).any():
            gap_reason = "invalid_auction_price"
        elif previous.empty or joined.empty:
            gap_reason = "missing_previous_close"
        elif (joined.close.dropna() <= 0).any():
            gap_reason = "invalid_previous_close"
        elif _non_finite(joined.loc[~joined.close__null, "close"]):
            gap_reason, gap_tag = (
                "auction_non_finite",
                _non_finite(joined.loc[~joined.close__null, "close"]),
            )
        elif joined.close.isna().all():
            gap_reason = "missing_previous_close"
        if amount_tag:
            amount_reason = "auction_non_finite"
        elif current.amount.isna().all():
            amount_reason = "auction_null"
        elif (used.amount.dropna() < 0).any():
            amount_reason = "invalid_auction_amount"
        elif history.empty or not history.amount.notna().any():
            amount_reason = "missing_auction_history"
        elif historical_dates.dropna().median() <= 0:
            amount_reason = "zero_auction_baseline"
    gap = None if gap_reason else gap_up_ratio_from_rows(current, previous)
    amount = None if amount_reason else auction_amount_ratio_from_rows(selected, day, 20)
    if amount is not None and not math.isfinite(amount):
        amount_tag = "NaN" if math.isnan(amount) else "Infinity" if amount > 0 else "-Infinity"
        amount, amount_reason = None, "auction_non_finite"
    if gap is None and gap_reason is None:
        gap_reason = "missing_previous_close"
    if amount is None and amount_reason is None:
        amount_reason = "missing_auction_history"
    valid_gap = joined.loc[joined.price.notna() & joined.close.notna() & (joined.close > 0)]
    sources = tuple(sorted(set(current.source)))
    available = (
        datetime.combine(
            day,
            time(9, 31) if "minute_0930_fallback" in sources else time(9, 26),
            tzinfo=EXCHANGE_TIMEZONE,
        )
        if sources
        else None
    )
    diagnostic = FactorAuctionDiagnostic(
        board_code=board_code,
        board_name=board_name,
        membership_date=membership_date,
        previous_close_date=previous_date,
        member_count=len(members),
        valid_gap_members=len(valid_gap),
        historical_observation_days=history_days,
        auction_sources=sources,
        latest_available_at=available,
    )
    return {
        "board_gap_up_ratio": (gap, gap_reason, gap_tag),
        "board_auction_amount_ratio": (amount, amount_reason, amount_tag),
        "board_member_count": (float(len(members)), None, None),
    }, diagnostic


def _derive(
    connection: duckdb.DuckDBPyConnection,
    request: FactorAuctionPrepareRequest,
    *,
    inputs: tuple[DatasetSnapshotArtifact, ...],
    root: Path,
) -> tuple[DatasetSnapshotArtifact, int]:
    scope = request.prepared_source.receipt.request.scope
    with duckdb.connect(":memory:") as compute:
        compute.execute("SET threads=1")
        compute.execute("SET memory_limit='512MB'")
        compute.execute(
            "CREATE TEMP TABLE auction_scope_codes AS SELECT unnest(?) AS con_code",
            [list(scope.stock_codes)],
        )
        for artifact in inputs:
            table = (
                "all_auction_input" if artifact.table_name == "auction_bar" else artifact.table_name
            )
            compute.execute(
                "CREATE VIEW "
                + table
                + " AS SELECT * FROM read_parquet("
                + _quoted_literal(str(root / artifact.relative_path))
                + ",hive_partitioning=false)"
            )
        compute.execute(
            "CREATE VIEW kpl_concept_member_daily AS SELECT trade_date,board_code"
            ",board_name,con_code,input_order FROM auction_membership_input"
        )
        compute.execute(
            "CREATE TABLE daily_auction_feature(ts_code VARCHAR,trade_date DATE,"
            + ",".join(
                c + " DOUBLE," + c + "__reason VARCHAR," + c + "__non_finite VARCHAR"
                for c in AUCTION_COLUMNS
            )
            + ",auction_diagnostic VARCHAR,PRIMARY KEY(ts_code,trade_date))"
        )
        evaluations = 0
        for day in request.prepared_source.receipt.calendar_open_days:
            decision = datetime.combine(day, time(9, 30), tzinfo=EXCHANGE_TIMEZONE)
            # Select required full boards in SQL before converting rows to pandas.
            compute.execute(
                "CREATE OR REPLACE VIEW kpl_concept_member_daily AS "
                "SELECT full_board.* FROM auction_membership_input full_board JOIN ("
                "SELECT DISTINCT trade_date,board_code FROM auction_membership_input "
                "WHERE con_code IN (SELECT con_code FROM auction_scope_codes) "
                "AND trade_date>="
                + _quoted_literal((day - timedelta(days=30)).isoformat())
                + "::DATE "
                "AND trade_date<" + _quoted_literal(day.isoformat()) + "::DATE "
                "QUALIFY trade_date=max(trade_date) OVER(PARTITION BY con_code)"
                ") needed USING(trade_date,board_code)"
            )
            if compute.execute(
                "SELECT count(*) FROM (SELECT trade_date,board_code FROM "
                "kpl_concept_member_daily GROUP BY ALL HAVING count(*)>50000)"
            ).fetchone()[0]:
                raise ValueError("auction full board member bound exceeded")
            membership = query_visible_rows(
                compute,
                "kpl_concept_daily",
                decision,
                scope=VisibilityQueryScope(
                    start_date=day - timedelta(days=30),
                    end_date=day,
                    columns=("trade_date", "board_code", "board_name", "con_code", "input_order"),
                ),
            )
            membership["trade_date"] = pd.to_datetime(membership["trade_date"]).dt.date
            membership = membership.sort_values("input_order", kind="stable")
            latest = (
                membership.loc[membership.con_code.isin(scope.stock_codes)]
                .groupby("con_code")
                .trade_date.max()
                .to_dict()
            )
            wanted = (
                membership.loc[
                    membership.con_code.isin(latest)
                    & (membership.trade_date == membership.con_code.map(latest))
                ]
                if not membership.empty
                else membership
            )
            keys = tuple(
                dict.fromkeys(
                    (r.trade_date, r.board_code, r.board_name) for r in wanted.itertuples()
                )
            )
            board_members = {
                (stamp, board): membership.loc[
                    (membership.trade_date == stamp) & (membership.board_code == board), "con_code"
                ].tolist()
                for stamp, board, _ in keys
            }
            members = tuple(sorted({code for codes in board_members.values() for code in codes}))
            compute.execute(
                (
                    "CREATE OR REPLACE VIEW auction_bar AS SELECT ts_code,trade_date,auct"
                    "ion_type,price,amount,source,price IS NULL AS price__null,amount IS "
                    "NULL AS amount__null FROM all_auction_input WHERE auction_type='open"
                    "_realtime' AND source IN ('tushare','minute_0930_fallback') AND trad"
                    "e_date<="
                )
                + _quoted_literal(day.isoformat())
                + (
                    "::DATE QUALIFY dense_rank() OVER(PARTITION BY ts_code ORDER BY trade"
                    "_date DESC)<=21"
                )
            )
            columns = (
                "ts_code",
                "trade_date",
                "auction_type",
                "price",
                "amount",
                "source",
                "price__null",
                "amount__null",
            )
            auctions = (
                query_visible_rows(
                    compute,
                    "auction_bar",
                    decision,
                    scope=VisibilityQueryScope(ts_codes=members, end_date=day, columns=columns),
                )
                if members
                else pd.DataFrame(columns=columns)
            )
            auctions = auctions.loc[auctions.auction_type == "open_realtime"].copy()
            auctions["trade_date"] = pd.to_datetime(auctions["trade_date"]).dt.date
            previous_date = compute.execute(
                "SELECT previous_close_date FROM auction_session_input WHERE trade_date=?", [day]
            ).fetchone()[0]
            closes = compute.execute(
                (
                    "SELECT ts_code,close,close IS NULL AS close__null FROM auction_close"
                    "_input WHERE trade_date=?"
                ),
                [previous_date],
            ).fetchdf()
            metrics = {}
            auctions_by_code = {
                code: rows for code, rows in auctions.groupby("ts_code", sort=False)
            }
            for stamp, board, name in keys:
                board_codes = board_members[stamp, board]
                frames = [
                    auctions_by_code[code] for code in board_codes if code in auctions_by_code
                ]
                board_auctions = (
                    pd.concat(frames, ignore_index=True) if frames else auctions.iloc[:0]
                )
                metrics[stamp, board] = _board_fact(
                    board_codes,
                    board_auctions,
                    closes,
                    day,
                    board_code=board,
                    board_name=name,
                    membership_date=stamp,
                    previous_date=previous_date,
                )
                evaluations += 1
            output = []
            choices_by_code = {
                code: [metrics[r.trade_date, r.board_code] for r in boards.itertuples()]
                for code, boards in wanted.groupby("con_code", sort=False)
            }
            for code in scope.stock_codes:
                choices = choices_by_code.get(code, [])
                if choices:
                    values, diagnostic = max(
                        choices,
                        key=lambda item: (
                            item[0]["board_auction_amount_ratio"][0]
                            if item[0]["board_auction_amount_ratio"][0] is not None
                            else float("-inf")
                        ),
                    )
                else:
                    values = {c: (None, "missing_board_membership", None) for c in AUCTION_COLUMNS}
                    diagnostic = FactorAuctionDiagnostic(
                        member_count=0,
                        valid_gap_members=0,
                        historical_observation_days=0,
                        auction_sources=(),
                    )
                output.append(
                    (
                        code,
                        day,
                        *(v for c in AUCTION_COLUMNS for v in values[c]),
                        diagnostic.model_dump_json(),
                    )
                )
            if output:
                compute.executemany(
                    "INSERT INTO daily_auction_feature VALUES("
                    + ",".join("?" for _ in output[0])
                    + ")",
                    output,
                )
        artifact = materialize_table_dependency(
            compute,
            dependency=StrategyTableDependency(
                dataset_id="factor_auction_features",
                table_name="daily_auction_feature",
                date_column="trade_date",
                code_column="ts_code",
            ),
            artifact_root=root,
            start_date=scope.start_date,
            end_date=scope.end_date,
            as_of_time=scope.as_of_time,
            ts_codes=scope.stock_codes,
        )
    return artifact, evaluations


def prepare_factor_auction_source(
    request: FactorAuctionPrepareRequest,
    *,
    lake_root: Path,
    now: Callable[[], datetime] = utc_now,
) -> FactorDailyFeatureSource:
    from rquant.factor.daily_feature_source import (
        AUCTION_FIELDS,
        FactorAuctionPrepareRequest,
        FactorDailyFeatureSource,
        open_factor_daily_feature_source,
    )

    request = FactorAuctionPrepareRequest.model_validate(request)
    prepared, base = request.prepared_source, request.base_daily_source
    original, scope = prepared.receipt.request, prepared.receipt.request.scope
    if base is not None:
        if base.schema_version == 6:
            raise ValueError("auction source cannot extend an auction source")
        base.require_prepared(prepared)
        with open_factor_daily_feature_source(base, lake_root=lake_root):
            pass
    width = 3 + (0 if base is None else len(base.fields))
    if (
        len(scope.stock_codes) * len(prepared.receipt.calendar_open_days) * width
        > request.max_output_cells
    ):
        raise ValueError("auction output cell budget exceeded")
    generation = _generation(original)
    if generation != prepared.receipt.generation:
        raise ValueError("auction generation differs from paired prices")
    root = _root_path(lake_root)
    if root.is_relative_to(original.replica_path.parent):
        raise ValueError("auction lake must be outside replica directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    root_descriptor = _open_private_root(root)
    try:
        with TemporaryDirectory(prefix=".auction-prepare-", dir=root) as scratch:
            descriptor = os.open(original.replica_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
            connection, transaction = None, False
            try:
                _check_generation(original, generation, descriptor)
                connection, mode = connect_pinned_readonly(original.replica_path, descriptor)
                connection.execute("SET threads=1")
                connection.execute("SET memory_limit='512MB'")
                connection.execute("SET temp_directory=?", [scratch])
                connection.execute("BEGIN TRANSACTION")
                transaction = True
                observed = normalize_utc_datetime(now())
                inputs, lake = _stage_inputs(connection, request, scratch=Path(scratch), root=root)
                artifact, evaluations = _derive(connection, request, inputs=inputs, root=root)
                implementation = hashlib.sha256(
                    Path(__file__).with_name("auction_source.py").read_bytes()
                    + Path(__file__).parents[1].joinpath("board_auction_strength.py").read_bytes()
                ).hexdigest()
                receipt = FactorAuctionReceipt(
                    policy=FactorAuctionPolicy(implementation_sha256=implementation),
                    inputs=inputs,
                    artifact=artifact,
                    input_rows=sum(a.row_count for a in inputs),
                    output_rows=artifact.row_count,
                    board_evaluations=evaluations,
                    max_input_rows=request.max_input_rows,
                    max_output_cells=request.max_output_cells,
                    lake=lake,
                )
                fields = dict(
                    schema_version=6,
                    prepared_source_sha256=prepared.sha256,
                    prepared_snapshot_id=prepared.snapshot.snapshot_id,
                    prepared_binding_hash=prepared.binding.binding_hash,
                    scope_content_hash=prepared.scope_content_hash,
                    scope=scope,
                    generation=generation,
                    code_commit=original.code_commit,
                    calendar_open_days=prepared.receipt.calendar_open_days,
                    fields=tuple(
                        sorted(
                            (() if base is None else base.fields) + AUCTION_FIELDS,
                            key=lambda f: f.column,
                        )
                    ),
                    value_semantics="auction_derived",
                    price_basis="field_specific",
                    recursive_initialization="field_specific",
                    source_mode="historical_retrospective",
                    source_read_boundary="single_snapshot_transaction",
                    auction=receipt,
                    read_mode=mode,
                    observed_at=observed,
                    completed_read_at=normalize_utc_datetime(now()),
                    tables=() if base is None else base.tables,
                )
                if base is not None:
                    fields["base_daily_source"] = base
                    for key in (
                        "technical_history",
                        "stock_features",
                        "minute_features",
                        "market_temperature",
                    ):
                        if getattr(base, key) is not None:
                            fields[key] = getattr(base, key)
                source = FactorDailyFeatureSource(**fields, sha256=canonical_sha256(fields))
                source.require_prepared(prepared)
                _check_generation(original, generation, descriptor)
                _require_same_root(root, root_descriptor)
                connection.execute("COMMIT")
                transaction = False
                _check_generation(original, generation, descriptor)
                return source
            finally:
                if connection is not None:
                    if transaction:
                        with suppress(Exception):
                            connection.execute("ROLLBACK")
                    connection.close()
                os.close(descriptor)
    finally:
        os.close(root_descriptor)


def __getattr__(name: str) -> object:
    if name in ("AUCTION_FIELDS", "FactorAuctionPrepareRequest"):
        from rquant.factor import daily_feature_source

        return getattr(daily_feature_source, name)
    raise AttributeError(name)
