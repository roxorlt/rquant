"""Versioned, bounded reads from synthetic factor computation snapshots."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pytest
from pydantic import ValidationError

from rquant.data_metadata import (
    DatasetSnapshot,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.research_snapshot import FactorReadQuery
from rquant.storage.duckdb import DuckDBStore

if TYPE_CHECKING:
    from rquant.factor.stream_snapshot import FactorStreamSnapshotAdmissionRequest

_FIRST = date(2026, 7, 1)
_AS_OF = datetime(2026, 8, 1, 8, tzinfo=UTC)


def _codes(count: int) -> tuple[str, ...]:
    return tuple(f"{index:06d}.SZ" for index in range(1, count + 1))


@contextmanager
def _source(
    tmp_path: Path,
    *,
    count: int = 3,
    days: int = 2,
    read_only: bool = True,
    coverage_start: date | None = None,
) -> Iterator[tuple[DuckDBStore, duckdb.DuckDBPyConnection, DatasetSnapshot, Path]]:
    path = tmp_path / "source.duckdb"
    with duckdb.connect(str(path)) as seed:
        seed.execute(
            """CREATE TABLE daily_bar (
            ts_code VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE,
            close DOUBLE, vol DOUBLE, amount DOUBLE, PRIMARY KEY(ts_code, trade_date));
            CREATE TABLE adj_factor (
            ts_code VARCHAR, trade_date DATE, adj_factor DOUBLE,
            PRIMARY KEY(ts_code, trade_date));
            CREATE TABLE trade_calendar (
            exchange VARCHAR, cal_date DATE, is_open BOOLEAN, pretrade_date DATE,
            PRIMARY KEY(exchange, cal_date))"""
        )
        seed.execute(
            """INSERT INTO daily_bar
            SELECT lpad(cast(i AS VARCHAR), 6, '0') || '.SZ',
            cast(? AS DATE) + cast(d AS INTEGER), 10., 12., 9.,
            10. + i / 10000. + d, 100., 1000.
            FROM range(1, ?) codes(i) CROSS JOIN range(?) days(d)""",
            [_FIRST, count + 1, days],
        )
        seed.execute("INSERT INTO adj_factor SELECT ts_code, trade_date, 1. FROM daily_bar")
        seed.execute(
            """INSERT INTO trade_calendar
            SELECT exchange, cast(? AS DATE) + cast(d AS INTEGER), TRUE,
            cast(? AS DATE) + cast(d AS INTEGER) - 1
            FROM (VALUES ('SSE'), ('SZSE')) exchanges(exchange) CROSS JOIN range(?) days(d)""",
            [_FIRST, _FIRST, days],
        )
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        building = DatasetSnapshot.create(
            strategy_name="factor_eval",
            manifest_id="m" * 64,
            as_of_time=_AS_OF,
            code_commit="a" * 40,
            origin="synthetic-stream-snapshot",
            created_at=_AS_OF,
        )
        metadata.begin_dataset_snapshot(building)
        snapshot = metadata.finalize_dataset_snapshot(
            building.snapshot_id,
            DatasetSnapshotFinalization(
                table_watermarks={
                    "manifest_start_date": (coverage_start or _FIRST).isoformat(),
                    "manifest_end_date": (_FIRST + timedelta(days=days - 1)).isoformat(),
                },
                completed_at=_AS_OF,
            ),
        )
        with duckdb.connect(str(path), read_only=read_only) as source:
            yield metadata, source, snapshot, tmp_path / "lake"


def _build(
    metadata: DuckDBStore,
    source: duckdb.DuckDBPyConnection,
    snapshot: DatasetSnapshot,
    lake: Path,
    *,
    count: int = 3,
    days: int = 2,
) -> tuple[DatasetSnapshotBinding, FactorStreamSnapshotAdmissionRequest]:
    from rquant.factor.stream_snapshot import (
        FactorComputationScope,
        FactorStreamSnapshotAdmissionRequest,
        build_factor_stream_snapshot_binding,
    )

    scope = FactorComputationScope(
        stock_codes=_codes(count),
        start_date=_FIRST,
        end_date=_FIRST + timedelta(days=days - 1),
        as_of_time=_AS_OF,
    )
    binding = build_factor_stream_snapshot_binding(
        metadata_store=metadata,
        source_connection=source,
        lake_root=lake,
        snapshot_id=snapshot.snapshot_id,
        scope=scope,
        now=lambda: _AS_OF,
    )
    return binding, FactorStreamSnapshotAdmissionRequest(
        snapshot_id=snapshot.snapshot_id, binding_hash=binding.binding_hash, scope=scope
    )


def _query(
    binding: DatasetSnapshotBinding, *, codes: tuple[str, ...] = _codes(3)
) -> FactorReadQuery:
    return FactorReadQuery(
        binding_hash=binding.binding_hash,
        stock_codes=codes,
        start_date=_FIRST,
        end_date=_FIRST + timedelta(days=1),
        row_limit=100_000,
    )


def test_v2_materializes_scope_and_reads_actual_501st_stock(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission

    with _source(tmp_path, count=501) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake, count=501)
        assert binding.manifest.builder_version == "factor-stream-snapshot-v2"
        assert binding.manifest.dependency_contract_version == "factor-stream-source-v2"
        assert len(binding.manifest.artifacts) == 4
        artifact = next(
            a for a in binding.manifest.artifacts if a.table_name == "factor_computation_scope"
        )
        assert artifact.row_count == 501
        with open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, decision):
            assert lease.scope == request.scope
            assert decision.scope_content_hash == artifact.content_hash
            bars = lease.query_daily_bars(_query(binding, codes=("000501.SZ",)))
            assert len(bars.rows) == 2
            assert bars.rows[0].ts_code == "000501.SZ"
            assert bars.rows[0].close == pytest.approx(10.0501)
            assert bars.receipt.binding_hash == binding.binding_hash
            assert bars.receipt.source_mode == "historical_retrospective"
            assert not hasattr(lease, "_conn")
            assert not hasattr(lease, "connection")
            calendar = lease.query_sse_calendar(_query(binding, codes=("000501.SZ",)))
            assert len(calendar.rows) == 2
            calendar_artifact = next(
                a for a in binding.manifest.artifacts if a.table_name == "trade_calendar"
            )
            assert calendar_artifact.row_count == 2
        with pytest.raises(RuntimeError, match="closed"):
            lease.query_daily_bars(_query(binding, codes=("000501.SZ",)))
        assert not list((lake / ".execution_sessions").iterdir())


def test_scope_distinguishes_missing_observation_from_outside_code(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission

    with _source(tmp_path, count=2) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake, count=3)
        with open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, _decision):
            assert lease.query_daily_bars(_query(binding, codes=("000003.SZ",))).rows == ()
            assert lease.query_adj_factors(_query(binding, codes=("000003.SZ",))).rows == ()
            for reader in (
                lease.query_daily_bars,
                lease.query_adj_factors,
                lease.query_sse_calendar,
            ):
                with pytest.raises(ValueError, match="scope"):
                    reader(_query(binding, codes=("000004.SZ",)))


def test_outside_scope_is_rejected_before_stock_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission
    from rquant.research_snapshot import FactorStreamReadLease

    def unexpected_read(*_args: object, **_kwargs: object) -> None:
        pytest.fail("out-of-scope query reached the raw reader")

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        with open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, _decision):
            monkeypatch.setattr(FactorStreamReadLease, "_stock_rows", unexpected_read)
            for reader in (lease.query_daily_bars, lease.query_adj_factors):
                with pytest.raises(ValueError, match="scope"):
                    reader(_query(binding, codes=("000004.SZ",)))


@pytest.mark.parametrize("codes", [("000001.SZ", "000001.SZ"), ("000001",), (), _codes(7001)])
def test_computation_scope_rejects_invalid_or_excess_codes(codes: tuple[str, ...]) -> None:
    from rquant.factor.stream_snapshot import FactorComputationScope

    with pytest.raises(ValidationError):
        FactorComputationScope(
            stock_codes=codes, start_date=_FIRST, end_date=_FIRST, as_of_time=_AS_OF
        )


def test_scope_has_independent_4096_natural_day_limit_and_is_strict() -> None:
    from rquant.factor.stream_snapshot import FactorComputationScope

    first = date(2015, 1, 1)
    last = first + timedelta(days=4095)
    scope = FactorComputationScope(
        stock_codes=("000002.SZ", "000001.SZ"), start_date=first, end_date=last, as_of_time=_AS_OF
    )
    assert scope.stock_codes == ("000001.SZ", "000002.SZ")
    with pytest.raises(ValidationError):
        FactorComputationScope(
            stock_codes=_codes(1),
            start_date=first,
            end_date=last + timedelta(days=1),
            as_of_time=_AS_OF,
        )
    with pytest.raises(ValidationError):
        scope.end_date = _FIRST
    with pytest.raises(ValidationError):
        FactorComputationScope(
            stock_codes=["000001.SZ"], start_date=_FIRST, end_date=_FIRST, as_of_time=_AS_OF
        )


def test_legacy_reader_and_admission_reject_v2(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission
    from rquant.factor_snapshot_admission import (
        FactorSnapshotAdmissionError,
        FactorSnapshotAdmissionRequest,
        open_factor_snapshot_admission,
    )
    from rquant.research_snapshot import FactorReadLease, ResearchExecutionSession

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        with (
            ResearchExecutionSession(binding=binding, lake_root=lake) as session,
            pytest.raises(ValueError, match="dedicated binding"),
        ):
            FactorReadLease(session, start_date=_FIRST, end_date=_FIRST)
        legacy = FactorSnapshotAdmissionRequest(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            start_date=_FIRST,
            end_date=_FIRST,
            source_mode="historical_retrospective",
        )
        with (
            pytest.raises(FactorSnapshotAdmissionError),
            open_factor_snapshot_admission(legacy, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("v2 entered v1 admission")
        with open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ):
            pass


def test_v2_admission_rejects_legacy_binding(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import (
        FactorComputationScope,
        FactorStreamSnapshotAdmissionError,
        FactorStreamSnapshotAdmissionRequest,
        open_factor_stream_snapshot_admission,
    )
    from rquant.research_snapshot import build_factor_snapshot_binding

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding = build_factor_snapshot_binding(
            metadata_store=metadata,
            source_connection=source,
            lake_root=lake,
            snapshot_id=snapshot.snapshot_id,
            start_date=_FIRST,
            end_date=_FIRST + timedelta(days=1),
            ts_codes=_codes(3),
            now=lambda: _AS_OF,
        )
        request = FactorStreamSnapshotAdmissionRequest(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            scope=FactorComputationScope(
                stock_codes=_codes(3),
                start_date=_FIRST,
                end_date=_FIRST + timedelta(days=1),
                as_of_time=_AS_OF,
            ),
        )
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError, match="binding_version"),
            open_factor_stream_snapshot_admission(request, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("v1 entered v2 admission")


def test_per_query_limits_stay_unchanged() -> None:
    for changes in (
        {"stock_codes": _codes(501)},
        {"end_date": _FIRST + timedelta(days=366)},
        {"row_limit": 100001},
    ):
        with pytest.raises(ValidationError):
            FactorReadQuery(
                **{
                    "binding_hash": "a" * 64,
                    "stock_codes": _codes(1),
                    "start_date": _FIRST,
                    "end_date": _FIRST,
                    "row_limit": 1,
                    **changes,
                }
            )


def _changed_binding(binding: DatasetSnapshotBinding, **changes: object) -> DatasetSnapshotBinding:
    manifest = DatasetSnapshotBindingManifest.model_validate(
        {
            **binding.manifest.model_dump(exclude_computed_fields=True),
            **changes,
        }
    )
    provisional = DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path="pending/manifest.json",
        created_at=_AS_OF,
    )
    return DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root="research_lake",
        manifest_relative_path=f"snapshots/{binding.snapshot_id}/{provisional.binding_hash}/manifest.json",
        created_at=_AS_OF,
    ).finalize(DatasetSnapshotBindingFinalization(completed_at=_AS_OF))


def _publish(lake: Path, binding: DatasetSnapshotBinding) -> None:
    path = lake / binding.manifest_relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        binding.manifest.model_dump_json(exclude_computed_fields=True), encoding="utf-8"
    )


@pytest.mark.parametrize("damage", ["delete", "replace", "tamper"])
def test_scope_file_damage_is_rejected_and_session_copies_are_cleaned(
    tmp_path: Path, damage: str
) -> None:
    from rquant.factor.stream_snapshot import (
        FactorStreamSnapshotAdmissionError,
        open_factor_stream_snapshot_admission,
    )

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        artifact = next(
            a for a in binding.manifest.artifacts if a.table_name == "factor_computation_scope"
        )
        path = lake / artifact.relative_path
        if damage == "delete":
            path.unlink()
        elif damage == "replace":
            replacement = path.with_suffix(".replacement")
            replacement.write_bytes(b"invalid parquet")
            replacement.replace(path)
        else:
            payload = bytearray(path.read_bytes())
            payload[len(payload) // 2] ^= 1
            path.write_bytes(payload)
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError, match="session_verification_failed"),
            open_factor_stream_snapshot_admission(request, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("damaged scope admitted")
        assert not list((lake / ".execution_sessions").iterdir())


@pytest.mark.parametrize("field", ["file_hash", "schema_hash", "content_hash", "row_count"])
def test_source_artifact_digest_mismatch_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    from rquant.factor.stream_snapshot import (
        FactorStreamSnapshotAdmissionError,
        open_factor_stream_snapshot_admission,
    )

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        artifacts = tuple(
            a.model_copy(update={field: a.row_count + 1 if field == "row_count" else "0" * 64})
            if a.table_name == "daily_bar"
            else a
            for a in binding.manifest.artifacts
        )
        changed = _changed_binding(binding, artifacts=artifacts)
        _publish(lake, changed)
        monkeypatch.setattr(metadata, "get_dataset_snapshot_binding", lambda _id: changed)
        request = request.model_copy(update={"binding_hash": changed.binding_hash})
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError, match="session_verification_failed"),
            open_factor_stream_snapshot_admission(request, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("incorrect artifact evidence admitted")
        assert not list((lake / ".execution_sessions").iterdir())


@pytest.mark.parametrize("change", ["codes", "start_date", "end_date", "as_of_time"])
def test_even_hash_valid_scope_fields_must_match_request_and_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from rquant.factor.stream_snapshot import (
        FactorStreamSnapshotAdmissionError,
        open_factor_stream_snapshot_admission,
    )
    from rquant.research_snapshot import materialize_table_dependency
    from rquant.strategy_dependencies import StrategyTableDependency

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        values = {
            "codes": _codes(3),
            "start_date": _FIRST,
            "end_date": _FIRST + timedelta(days=1),
            "as_of_time": _AS_OF,
        }
        values[change] = {
            "codes": ("000001.SZ", "000002.SZ", "000004.SZ"),
            "start_date": _FIRST - timedelta(days=1),
            "end_date": _FIRST + timedelta(days=2),
            "as_of_time": _AS_OF + timedelta(hours=1),
        }[change]
        with duckdb.connect() as replacement:
            replacement.execute(
                "CREATE TABLE altered (ts_code VARCHAR PRIMARY KEY, start_date DATE, "
                "end_date DATE, as_of_time TIMESTAMPTZ)"
            )
            replacement.execute(
                "INSERT INTO altered SELECT unnest(?), ?, ?, ?",
                [
                    list(values["codes"]),
                    values["start_date"],
                    values["end_date"],
                    values["as_of_time"],
                ],
            )
            scope_artifact = materialize_table_dependency(
                replacement,
                dependency=StrategyTableDependency(
                    dataset_id="factor_computation_scope",
                    table_name="factor_computation_scope",
                    code_column="ts_code",
                ),
                artifact_root=lake,
                start_date=request.scope.start_date,
                end_date=request.scope.end_date,
                as_of_time=_AS_OF,
                source_table_name="altered",
            )
        changed = _changed_binding(
            binding,
            artifacts=tuple(
                scope_artifact if a.table_name == "factor_computation_scope" else a
                for a in binding.manifest.artifacts
            ),
        )
        _publish(lake, changed)
        monkeypatch.setattr(metadata, "get_dataset_snapshot_binding", lambda _id: changed)
        request = request.model_copy(update={"binding_hash": changed.binding_hash})
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError, match="bound_scope"),
            open_factor_stream_snapshot_admission(request, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("hash-valid but mismatched scope admitted")
        assert not list((lake / ".execution_sessions").iterdir())


@pytest.mark.parametrize("change", ["hash", "codes", "start", "end", "cutoff"])
def test_request_cannot_change_bound_identity_or_complete_scope(
    tmp_path: Path, change: str
) -> None:
    from rquant.factor.stream_snapshot import (
        FactorStreamSnapshotAdmissionError,
        open_factor_stream_snapshot_admission,
    )

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        _binding, request = _build(metadata, source, snapshot, lake)
        if change == "hash":
            request = request.model_copy(update={"binding_hash": "0" * 64})
            code = "source_identity"
        else:
            field, value = {
                "codes": ("stock_codes", ("000001.SZ", "000002.SZ", "000004.SZ")),
                "start": ("start_date", _FIRST - timedelta(days=1)),
                "end": ("end_date", _FIRST + timedelta(days=2)),
                "cutoff": ("as_of_time", _AS_OF + timedelta(hours=1)),
            }[change]
            request = request.model_copy(
                update={"scope": request.scope.model_copy(update={field: value})}
            )
            code = "bound_scope" if change == "codes" else "source_range"
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError, match=code),
            open_factor_stream_snapshot_admission(request, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("changed request admitted")


@pytest.mark.parametrize(
    "change",
    ["builder", "contract", "missing_scope", "missing_prices", "dataset", "key", "event", "mode"],
)
def test_v2_rejects_inexact_contract_and_artifact_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from rquant.factor.stream_snapshot import (
        FactorStreamSnapshotAdmissionError,
        open_factor_stream_snapshot_admission,
    )

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        if change in ("builder", "contract"):
            changed = _changed_binding(
                binding,
                **{
                    "builder_version"
                    if change == "builder"
                    else "dependency_contract_version": "old-or-unknown"
                },
            )
            code = "binding_version"
        elif change.startswith("missing"):
            omitted = "factor_computation_scope" if change == "missing_scope" else "daily_bar"
            changed = _changed_binding(
                binding,
                artifacts=tuple(a for a in binding.manifest.artifacts if a.table_name != omitted),
            )
            code = "binding_artifacts"
        else:
            field, value = {
                "dataset": ("dataset_id", "other"),
                "key": ("primary_key", ("trade_date",)),
                "event": ("event_column", None),
                "mode": ("source", "observed_pit"),
            }[change]
            changed = _changed_binding(
                binding,
                artifacts=tuple(
                    a.model_copy(update={field: value}) if a.table_name == "daily_bar" else a
                    for a in binding.manifest.artifacts
                ),
            )
            code = "binding_artifacts"
        monkeypatch.setattr(metadata, "get_dataset_snapshot_binding", lambda _id: changed)
        request = request.model_copy(update={"binding_hash": changed.binding_hash})
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError, match=code),
            open_factor_stream_snapshot_admission(request, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("inexact source contract admitted")


def test_generation_change_closes_real_session_and_deletes_copies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.stream_snapshot as stream
    from rquant.research_snapshot import ResearchExecutionSession

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        calls = 0
        opened: list[ResearchExecutionSession] = []

        def current_binding(_id: str) -> DatasetSnapshotBinding | None:
            nonlocal calls
            calls += 1
            return binding if calls == 1 else None

        def record_session(**kwargs: object) -> ResearchExecutionSession:
            session = ResearchExecutionSession(**kwargs)
            opened.append(session)
            return session

        monkeypatch.setattr(metadata, "get_dataset_snapshot_binding", current_binding)
        monkeypatch.setattr(stream, "ResearchExecutionSession", record_session)
        with (
            pytest.raises(stream.FactorStreamSnapshotAdmissionError, match="generation_changed"),
            stream.open_factor_stream_snapshot_admission(
                request, metadata_store=metadata, lake_root=lake
            ),
        ):
            pytest.fail("changed generation admitted")
        assert len(opened) == 1
        with pytest.raises(duckdb.ConnectionException, match="closed"):
            opened[0].query_index_daily(_FIRST)
        assert not list((lake / ".execution_sessions").iterdir())


def test_transaction_failure_leaves_no_ready_binding_or_temporary_relation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.stream_snapshot as stream

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        original = stream.materialize_table_dependency

        def fail_second(connection: duckdb.DuckDBPyConnection, **kwargs: object) -> object:
            if kwargs["dependency"].table_name == "adj_factor":
                raise RuntimeError("synthetic middle-of-transaction failure")
            return original(connection, **kwargs)

        monkeypatch.setattr(stream, "materialize_table_dependency", fail_second)
        with pytest.raises(RuntimeError, match="middle-of-transaction"):
            _build(metadata, source, snapshot, lake)
        assert metadata.get_dataset_snapshot_binding(snapshot.snapshot_id) is None
        assert (
            source.execute(
                "SELECT table_name FROM duckdb_tables() "
                "WHERE table_name LIKE '__factor_stream_scope_%'"
            ).fetchall()
            == []
        )
        source.execute("BEGIN TRANSACTION")
        source.execute("ROLLBACK")
        assert not list(lake.rglob("*.tmp-*"))
        monkeypatch.setattr(stream, "materialize_table_dependency", original)
        binding, _request = _build(metadata, source, snapshot, lake)
        assert binding.status == "ready" and len(binding.manifest.artifacts) == 4


def test_builder_never_takes_over_existing_transaction(tmp_path: Path) -> None:
    with _source(tmp_path) as (metadata, source, snapshot, lake):
        source.execute("BEGIN TRANSACTION")
        with pytest.raises(ValueError, match="own source transaction"):
            _build(metadata, source, snapshot, lake)
        source.execute("ROLLBACK")
        assert metadata.get_dataset_snapshot_binding(snapshot.snapshot_id) is None
        assert not lake.exists()


def test_builder_missing_source_table_rolls_back(tmp_path: Path) -> None:
    with _source(tmp_path, read_only=False) as (metadata, source, snapshot, lake):
        source.execute("DROP TABLE adj_factor")
        with pytest.raises(ValueError, match="source table missing"):
            _build(metadata, source, snapshot, lake)
        assert metadata.get_dataset_snapshot_binding(snapshot.snapshot_id) is None
        source.execute("BEGIN TRANSACTION")
        source.execute("ROLLBACK")
        assert not list(lake.rglob("*.tmp-*"))


def test_all_raw_tables_share_the_same_source_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.stream_snapshot as stream

    with _source(tmp_path, read_only=False) as (metadata, source, snapshot, lake):
        original = stream.materialize_table_dependency
        with duckdb.connect(str(tmp_path / "source.duckdb")) as writer:

            def change_between_tables(
                connection: duckdb.DuckDBPyConnection, **kwargs: object
            ) -> object:
                artifact = original(connection, **kwargs)
                if kwargs["dependency"].table_name == "daily_bar":
                    writer.execute("UPDATE daily_bar SET close = 99")
                    writer.execute("UPDATE adj_factor SET adj_factor = 99")
                    writer.execute("UPDATE trade_calendar SET is_open = FALSE")
                return artifact

            monkeypatch.setattr(stream, "materialize_table_dependency", change_between_tables)
            binding, request = _build(metadata, source, snapshot, lake)
        assert source.execute("SELECT min(adj_factor) FROM adj_factor").fetchone() == (99.0,)
        with stream.open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, _decision):
            assert lease.query_daily_bars(_query(binding)).rows[0].close == pytest.approx(10.0001)
            assert {r.adj_factor for r in lease.query_adj_factors(_query(binding)).rows} == {1.0}
            assert all(r.is_open for r in lease.query_sse_calendar(_query(binding)).rows)


def test_queries_reject_actual_row_overflow_binding_and_date_mismatch(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        with open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, _decision):
            for reader in (
                lease.query_daily_bars,
                lease.query_adj_factors,
                lease.query_sse_calendar,
            ):
                with pytest.raises(ValueError, match="row limit"):
                    reader(_query(binding).model_copy(update={"row_limit": 1}))
                with pytest.raises(ValueError, match="binding_hash"):
                    reader(_query(binding).model_copy(update={"binding_hash": "0" * 64}))
                with pytest.raises(ValueError, match="admitted dates"):
                    reader(
                        _query(binding).model_copy(
                            update={"start_date": _FIRST - timedelta(days=1)}
                        )
                    )


def test_code_permutation_has_same_scope_and_binding_digest(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import build_factor_stream_snapshot_binding

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        repeated = build_factor_stream_snapshot_binding(
            metadata_store=metadata,
            source_connection=source,
            lake_root=lake,
            snapshot_id=snapshot.snapshot_id,
            scope=request.scope.model_copy(
                update={"stock_codes": tuple(reversed(request.scope.stock_codes))}
            ),
            now=lambda: _AS_OF,
        )
        assert binding.binding_hash == repeated.binding_hash
        assert binding.manifest == repeated.manifest


def test_builder_accepts_4096_natural_days_while_queries_keep_366_day_bound(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import (
        FactorComputationScope,
        FactorStreamSnapshotAdmissionRequest,
        build_factor_stream_snapshot_binding,
        open_factor_stream_snapshot_admission,
    )

    last = _FIRST + timedelta(days=1)
    first = last - timedelta(days=4095)
    with _source(tmp_path, coverage_start=first) as (metadata, source, snapshot, lake):
        scope = FactorComputationScope(
            stock_codes=_codes(3), start_date=first, end_date=last, as_of_time=_AS_OF
        )
        binding = build_factor_stream_snapshot_binding(
            metadata_store=metadata,
            source_connection=source,
            lake_root=lake,
            snapshot_id=snapshot.snapshot_id,
            scope=scope,
            now=lambda: _AS_OF,
        )
        request = FactorStreamSnapshotAdmissionRequest(
            snapshot_id=snapshot.snapshot_id, binding_hash=binding.binding_hash, scope=scope
        )
        with open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, _decision):
            query = FactorReadQuery(
                binding_hash=binding.binding_hash,
                stock_codes=_codes(3),
                start_date=last - timedelta(days=365),
                end_date=last,
                row_limit=100_000,
            )
            assert len(lease.query_daily_bars(query).rows) == 6
            with pytest.raises(ValidationError, match="bound"):
                lease.query_daily_bars(
                    query.model_copy(update={"start_date": last - timedelta(days=366)})
                )


@pytest.mark.parametrize(
    "change",
    [
        "missing_snapshot",
        "missing_binding",
        "not_ready",
        "invalid_hash",
        "snapshot_cutoff",
        "snapshot_range",
    ],
)
def test_v2_admission_requires_ready_canonical_matching_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    from rquant.factor.stream_snapshot import (
        FactorStreamSnapshotAdmissionError,
        open_factor_stream_snapshot_admission,
    )

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        if change == "missing_snapshot":
            monkeypatch.setattr(metadata, "get_dataset_snapshot", lambda _id: None)
            code = "source_missing"
        elif change == "missing_binding":
            monkeypatch.setattr(metadata, "get_dataset_snapshot_binding", lambda _id: None)
            code = "source_missing"
        elif change == "not_ready":
            monkeypatch.setattr(
                metadata,
                "get_dataset_snapshot_binding",
                lambda _id: binding.model_copy(update={"status": "building", "completed_at": None}),
            )
            code = "source_not_ready"
        elif change == "invalid_hash":
            monkeypatch.setattr(
                metadata,
                "get_dataset_snapshot_binding",
                lambda _id: binding.model_copy(update={"manifest_hash": "0" * 64}),
            )
            code = "source_invalid"
        elif change == "snapshot_cutoff":
            monkeypatch.setattr(
                metadata,
                "get_dataset_snapshot",
                lambda _id: snapshot.model_copy(update={"as_of_time": _AS_OF + timedelta(hours=1)}),
            )
            code = "source_identity"
        else:
            monkeypatch.setattr(
                metadata,
                "get_dataset_snapshot",
                lambda _id: snapshot.model_copy(
                    update={
                        "table_watermarks": {
                            "manifest_start_date": _FIRST.isoformat(),
                            "manifest_end_date": _FIRST.isoformat(),
                        }
                    }
                ),
            )
            code = "source_range"
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError, match=code),
            open_factor_stream_snapshot_admission(request, metadata_store=metadata, lake_root=lake),
        ):
            pytest.fail("invalid or mismatched metadata admitted")


def test_v2_keeps_verified_copies_after_original_price_and_scope_rewrite(tmp_path: Path) -> None:
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission

    with _source(tmp_path) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake)
        with open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, _decision):
            for artifact in binding.manifest.artifacts:
                if artifact.table_name in ("daily_bar", "factor_computation_scope"):
                    (lake / artifact.relative_path).write_bytes(
                        b"invalid replacement after admission"
                    )
            assert lease.scope == request.scope
            assert len(lease.query_daily_bars(_query(binding)).rows) == 6
            assert lease.query_daily_bars(_query(binding)).rows[0].close == pytest.approx(10.0001)
        assert not list((lake / ".execution_sessions").iterdir())


def test_7000_codes_16_days_are_read_in_one_generation_without_global_row_cap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.stream_snapshot as stream
    from rquant.research_snapshot import ResearchExecutionSession

    with _source(tmp_path, count=7000, days=16) as (metadata, source, snapshot, lake):
        binding, request = _build(metadata, source, snapshot, lake, count=7000, days=16)
        opened: list[ResearchExecutionSession] = []

        def record_session(**kwargs: object) -> ResearchExecutionSession:
            session = ResearchExecutionSession(**kwargs)
            opened.append(session)
            return session

        monkeypatch.setattr(stream, "ResearchExecutionSession", record_session)
        rows = adjustments = queries = 0
        with stream.open_factor_stream_snapshot_admission(
            request, metadata_store=metadata, lake_root=lake
        ) as (lease, decision):
            assert len(lease.scope.stock_codes) == 7000
            assert decision.scope == request.scope
            for offset in range(16):
                day = _FIRST + timedelta(days=offset)
                seen: set[str] = set()
                for start in range(0, 7000, 500):
                    codes = request.scope.stock_codes[start : start + 500]
                    query = FactorReadQuery(
                        binding_hash=binding.binding_hash,
                        stock_codes=codes,
                        start_date=day,
                        end_date=day,
                        row_limit=500,
                    )
                    bars = lease.query_daily_bars(query)
                    factors = lease.query_adj_factors(query)
                    assert len(bars.rows) == len(factors.rows) == 500
                    assert (
                        bars.receipt.binding_hash
                        == factors.receipt.binding_hash
                        == binding.binding_hash
                    )
                    assert bars.receipt.snapshot_id == snapshot.snapshot_id
                    assert bars.receipt.as_of_time == factors.receipt.as_of_time == _AS_OF
                    assert bars.rows[-1].close == pytest.approx(10 + (start + 500) / 10000 + offset)
                    seen.update(r.ts_code for r in bars.rows)
                    rows += len(bars.rows)
                    adjustments += len(factors.rows)
                    queries += 1
                assert seen == set(request.scope.stock_codes)
                calendar = lease.query_sse_calendar(
                    FactorReadQuery(
                        binding_hash=binding.binding_hash,
                        stock_codes=("007000.SZ",),
                        start_date=day,
                        end_date=day,
                        row_limit=1,
                    )
                )
                assert len(calendar.rows) == 1 and calendar.rows[0].cal_date == day
            assert len(opened) == 1
        assert rows == adjustments == 112000 and queries == 224
        with pytest.raises(duckdb.ConnectionException, match="closed"):
            opened[0].query_index_daily(_FIRST)
        assert not list((lake / ".execution_sessions").iterdir())
        print(
            f"STREAM_SNAPSHOT_SCALE codes=7000 days=16 daily_rows={rows} adj_rows={adjustments} "
            f"stock_queries={queries} sessions={len(opened)} per_query_codes=500 "
            "per_query_rows=500 closed_and_copies_removed=True"
        )
