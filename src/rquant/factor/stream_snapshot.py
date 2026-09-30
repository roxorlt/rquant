"""Independent v2 source snapshots for bounded daily factor computation.

The scope is a requested calculation superset, not verified market membership.
Raw prices and adjustments are frozen retrospective facts, not observed PIT data.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, ConfigDict, Field

from rquant.data_metadata import (
    DatasetSnapshot,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    normalize_utc_datetime,
    utc_now,
)
from rquant.research_lake import _quoted_identifier
from rquant.research_snapshot import (
    _FACTOR_TABLE_COLUMNS,
    _FACTOR_TABLE_KEYS,
    FACTOR_STREAM_SNAPSHOT_BUILDER_VERSION,
    FACTOR_STREAM_SOURCE_CONTRACT_VERSION,
    FactorComputationScope,
    FactorStreamReadLease,
    ResearchExecutionSession,
    SnapshotMetadataStore,
    _publish_binding_manifest,
    _source_table_schema,
    materialize_table_dependency,
    verify_materialized_table_artifact,
)
from rquant.strategy_dependencies import StrategyTableDependency

_IMMUTABLE = ConfigDict(extra="forbid", frozen=True, strict=True, revalidate_instances="always")
_SCOPE_TABLE = "factor_computation_scope"
_DEPENDENCIES = (
    StrategyTableDependency(
        dataset_id="daily_bar",
        table_name="daily_bar",
        date_column="trade_date",
        code_column="ts_code",
    ),
    StrategyTableDependency(
        dataset_id="adj_factor",
        table_name="adj_factor",
        date_column="trade_date",
        code_column="ts_code",
    ),
    StrategyTableDependency(
        dataset_id="trade_calendar",
        table_name="trade_calendar",
        date_column="cal_date",
        code_column="exchange",
    ),
    StrategyTableDependency(
        dataset_id=_SCOPE_TABLE, table_name=_SCOPE_TABLE, code_column="ts_code"
    ),
)


class FactorStreamSnapshotAdmissionRequest(BaseModel):
    model_config = _IMMUTABLE

    snapshot_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    binding_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    scope: FactorComputationScope


class FactorStreamSnapshotAdmissionDecision(FactorStreamSnapshotAdmissionRequest):
    allowed: Literal[True] = True
    research_status: Literal["exploratory"] = "exploratory"
    scope_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_mode: Literal["historical_retrospective"] = "historical_retrospective"
    source_read_boundary: Literal["single_snapshot_transaction"] = "single_snapshot_transaction"


class FactorStreamSnapshotAdmissionError(PermissionError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"factor stream snapshot admission denied: {code}: {message}")


def _snapshot_range(snapshot: DatasetSnapshot, start: date, end: date) -> bool:
    try:
        first = date.fromisoformat(snapshot.table_watermarks["manifest_start_date"])
        last = date.fromisoformat(snapshot.table_watermarks["manifest_end_date"])
    except (ValueError, KeyError):
        return False
    return first <= start <= end <= last and snapshot.as_of_time.date() >= end


def build_factor_stream_snapshot_binding(
    *,
    metadata_store: SnapshotMetadataStore,
    source_connection: duckdb.DuckDBPyConnection,
    lake_root: Path,
    snapshot_id: str,
    scope: FactorComputationScope,
    now: Callable[[], datetime] = utc_now,
) -> DatasetSnapshotBinding:
    """Freeze the exact scope and three raw tables in one owned read transaction."""
    scope = FactorComputationScope.model_validate(scope)
    found = metadata_store.get_dataset_snapshot(snapshot_id)
    if found is None:
        raise KeyError(f"dataset snapshot not found: {snapshot_id}")
    snapshot = DatasetSnapshot.model_validate(found.model_dump(exclude_computed_fields=True))
    if (
        snapshot.snapshot_id != snapshot_id
        or snapshot.status != "ready"
        or snapshot.strategy_name != "factor_eval"
    ):
        raise ValueError("stream builder requires the requested ready factor_eval snapshot")
    if source_connection is getattr(metadata_store, "_conn", None):
        raise ValueError("stream source connection must differ from metadata writer")
    if snapshot.as_of_time != scope.as_of_time or not _snapshot_range(
        snapshot, scope.start_date, scope.end_date
    ):
        raise ValueError("stream computation scope differs from snapshot cutoff or range")
    temporary = f"__factor_stream_scope_{uuid.uuid4().hex}"
    quoted_scope = _quoted_identifier(temporary)
    artifacts = []
    try:
        source_connection.execute("BEGIN TRANSACTION")
    except duckdb.Error as exc:
        raise ValueError("stream builder could not start its own source transaction") from exc
    try:
        # A temporary relation freezes request-owned scope without modifying source tables.
        source_connection.execute(
            f"""CREATE TEMP TABLE {quoted_scope} (
            ts_code VARCHAR PRIMARY KEY, start_date DATE NOT NULL, end_date DATE NOT NULL,
            as_of_time TIMESTAMPTZ NOT NULL)"""
        )
        source_connection.execute(
            f"INSERT INTO {quoted_scope} SELECT unnest(?), ?, ?, ?",
            [list(scope.stock_codes), scope.start_date, scope.end_date, scope.as_of_time],
        )
        for dependency in _DEPENDENCIES:
            source_table = (
                temporary if dependency.table_name == _SCOPE_TABLE else dependency.table_name
            )
            columns, key = _source_table_schema(source_connection, source_table)
            expected_key = (
                ("ts_code",)
                if dependency.table_name == _SCOPE_TABLE
                else _FACTOR_TABLE_KEYS[dependency.table_name]
            )
            required = (
                {"ts_code", "start_date", "end_date", "as_of_time"}
                if dependency.table_name == _SCOPE_TABLE
                else _FACTOR_TABLE_COLUMNS[dependency.table_name]
            )
            if key != expected_key or not required <= {name for name, _ in columns}:
                raise ValueError(
                    "stream source required columns or business key mismatch: "
                    f"{dependency.table_name}"
                )
            artifact = materialize_table_dependency(
                source_connection,
                dependency=dependency,
                artifact_root=lake_root,
                start_date=scope.start_date,
                end_date=scope.end_date,
                as_of_time=scope.as_of_time,
                ts_codes=("SSE",)
                if dependency.table_name == "trade_calendar"
                else scope.stock_codes,
                source_table_name=source_table,
            )
            verify_materialized_table_artifact(
                artifact, lake_root=lake_root, as_of_time=scope.as_of_time
            )
            artifacts.append(artifact)
        source_connection.execute(f"DROP TABLE {quoted_scope}")
        source_connection.execute("COMMIT")
    except Exception:
        source_connection.execute("ROLLBACK")
        raise

    manifest = DatasetSnapshotBindingManifest(
        snapshot_id=snapshot_id,
        strategy_name="factor_eval",
        start_date=scope.start_date,
        end_date=scope.end_date,
        as_of_time=scope.as_of_time,
        code_commit=snapshot.code_commit,
        dependency_contract_version=FACTOR_STREAM_SOURCE_CONTRACT_VERSION,
        builder_version=FACTOR_STREAM_SNAPSHOT_BUILDER_VERSION,
        artifacts=tuple(sorted(artifacts, key=lambda a: a.artifact_key)),
    )
    built_at = normalize_utc_datetime(now())
    provisional = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path="pending/manifest.json",
        created_at=built_at,
    )
    binding = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path=f"snapshots/{snapshot_id}/{provisional.binding_hash}/manifest.json",
        created_at=built_at,
    )
    _publish_binding_manifest(lake_root=lake_root, binding=binding)
    stored = metadata_store.begin_dataset_snapshot_binding(binding)
    if stored.binding_hash != binding.binding_hash or stored.manifest != manifest:
        raise ValueError("stored stream binding differs from completed artifacts")
    if stored.status == "ready":
        return stored
    return metadata_store.finalize_dataset_snapshot_binding(
        snapshot_id, DatasetSnapshotBindingFinalization(completed_at=normalize_utc_datetime(now()))
    )


def _validated_binding(
    request: FactorStreamSnapshotAdmissionRequest,
    snapshot: DatasetSnapshot | None,
    binding: DatasetSnapshotBinding | None,
) -> tuple[DatasetSnapshot, DatasetSnapshotBinding]:
    if snapshot is None or binding is None:
        raise FactorStreamSnapshotAdmissionError("source_missing", "snapshot or binding is absent")
    try:
        snapshot = DatasetSnapshot.model_validate(snapshot.model_dump(exclude_computed_fields=True))
        binding = DatasetSnapshotBinding.model_validate(
            binding.model_dump(exclude_computed_fields=True)
        )
    except (ValueError, TypeError) as exc:
        raise FactorStreamSnapshotAdmissionError(
            "source_invalid", "source identity is invalid"
        ) from exc
    manifest = binding.manifest
    if (
        manifest.builder_version != FACTOR_STREAM_SNAPSHOT_BUILDER_VERSION
        or manifest.dependency_contract_version != FACTOR_STREAM_SOURCE_CONTRACT_VERSION
    ):
        raise FactorStreamSnapshotAdmissionError(
            "binding_version", "v2 builder and contract are required"
        )
    if snapshot.status != "ready" or binding.status != "ready":
        raise FactorStreamSnapshotAdmissionError("source_not_ready", "source is not ready")
    if (
        snapshot.snapshot_id != request.snapshot_id
        or binding.snapshot_id != request.snapshot_id
        or binding.binding_hash != request.binding_hash
        or snapshot.strategy_name != "factor_eval"
        or manifest.strategy_name != "factor_eval"
        or manifest.code_commit != snapshot.code_commit
        or manifest.as_of_time != snapshot.as_of_time
        or binding.artifact_root != "research_lake"
        or binding.manifest_relative_path
        != f"snapshots/{request.snapshot_id}/{request.binding_hash}/manifest.json"
    ):
        raise FactorStreamSnapshotAdmissionError(
            "source_identity", "requested source identity differs"
        )
    if (
        manifest.start_date != request.scope.start_date
        or manifest.end_date != request.scope.end_date
        or manifest.as_of_time != request.scope.as_of_time
        or not _snapshot_range(snapshot, manifest.start_date, manifest.end_date)
    ):
        raise FactorStreamSnapshotAdmissionError(
            "source_range", "requested scope dates or cutoff differ"
        )
    if any(
        value is not None
        for value in (
            manifest.eligibility_resolution_hash,
            manifest.eligibility_expected_dates,
            manifest.eligibility_complete_dates,
        )
    ):
        raise FactorStreamSnapshotAdmissionError(
            "binding_artifacts", "strategy eligibility is not a raw source"
        )
    expected = {item.table_name: item for item in _DEPENDENCIES}
    if len(manifest.artifacts) != 4 or {a.table_name for a in manifest.artifacts} != set(expected):
        raise FactorStreamSnapshotAdmissionError(
            "binding_artifacts", "exactly four v2 artifacts are required"
        )
    for artifact in manifest.artifacts:
        dependency = expected[artifact.table_name]
        key = (
            ("ts_code",)
            if artifact.table_name == _SCOPE_TABLE
            else _FACTOR_TABLE_KEYS[artifact.table_name]
        )
        if (
            artifact.artifact_type != "materialized_table"
            or artifact.dataset_id != dependency.dataset_id
            or artifact.artifact_key
            != (
                f"{dependency.dataset_id}:{manifest.start_date.isoformat()}:"
                f"{manifest.end_date.isoformat()}"
            )
            or artifact.primary_key != key
            or artifact.event_column != dependency.date_column
            or artifact.source != "snapshot_materialization"
            or artifact.file_size is None
            or artifact.partition_id is not None
            or artifact.revision_created_at is not None
            or artifact.catalog_updated_at is not None
            or (
                artifact.table_name == _SCOPE_TABLE
                and artifact.row_count != len(request.scope.stock_codes)
            )
        ):
            raise FactorStreamSnapshotAdmissionError(
                "binding_artifacts", "artifact differs from v2 source contract"
            )
    return snapshot, binding


@contextmanager
def open_factor_stream_snapshot_admission(
    request: FactorStreamSnapshotAdmissionRequest,
    *,
    metadata_store: SnapshotMetadataStore,
    lake_root: Path,
) -> Iterator[tuple[FactorStreamReadLease, FactorStreamSnapshotAdmissionDecision]]:
    """Validate v2 evidence, then yield one scope-limited verified execution generation."""
    checked = FactorStreamSnapshotAdmissionRequest.model_validate(request)
    snapshot, binding = _validated_binding(
        checked,
        metadata_store.get_dataset_snapshot(checked.snapshot_id),
        metadata_store.get_dataset_snapshot_binding(checked.snapshot_id),
    )
    try:
        session = ResearchExecutionSession(binding=binding, lake_root=lake_root)
    except Exception as exc:
        raise FactorStreamSnapshotAdmissionError(
            "session_verification_failed", "bound files failed verification"
        ) from exc
    with session:
        current_snapshot = metadata_store.get_dataset_snapshot(checked.snapshot_id)
        current_binding = metadata_store.get_dataset_snapshot_binding(checked.snapshot_id)
        if (
            current_snapshot != snapshot
            or current_binding != binding
            or session.snapshot_id != checked.snapshot_id
            or session.binding_hash != checked.binding_hash
        ):
            raise FactorStreamSnapshotAdmissionError(
                "generation_changed", "source changed while opening verified session"
            )
        _validated_binding(checked, current_snapshot, current_binding)
        try:
            lease = FactorStreamReadLease(
                session, start_date=checked.scope.start_date, end_date=checked.scope.end_date
            )
        except Exception as exc:
            raise FactorStreamSnapshotAdmissionError(
                "bound_scope", "verified source rows violate computation scope"
            ) from exc
        if lease.scope != checked.scope:
            raise FactorStreamSnapshotAdmissionError(
                "bound_scope", "complete bound codes differ from requested computation scope"
            )
        scope_artifact = next(a for a in binding.manifest.artifacts if a.table_name == _SCOPE_TABLE)
        yield (
            lease,
            FactorStreamSnapshotAdmissionDecision(
                snapshot_id=checked.snapshot_id,
                binding_hash=checked.binding_hash,
                scope=lease.scope,
                scope_content_hash=scope_artifact.content_hash,
            ),
        )
