"""Actual source observations prepare a bounded, admissible raw snapshot."""

from __future__ import annotations

import os
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest
from pydantic import ValidationError

from rquant.factor.source_prepare import FactorSourcePrepareRequest, prepare_factor_stream_source
from rquant.replica_generation import (
    ReplicaDatabaseWatermark,
    ReplicaFileWatermark,
    ReplicaGenerationMetadata,
    capture_file_watermark,
    replica_generation_path,
)
from rquant.research_snapshot import FactorComputationScope, FactorReadQuery
from rquant.storage.duckdb import DuckDBStore

_FIRST = date(2026, 7, 1)
_AS_OF = datetime(2026, 8, 1, 8, tzinfo=UTC)
_PRIMARY = Path("/not-accessed/factor-test-primary.duckdb")
_CODES = ("000001.SZ", "000002.SZ", "000003.SZ")


def _replica(tmp_path: Path, *, count: int = 3, days: int = 4) -> Path:
    path = tmp_path / "ro" / "source.duckdb"
    path.parent.mkdir()
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
            """INSERT INTO daily_bar SELECT lpad(cast(i AS VARCHAR), 6, '0') || '.SZ',
            cast(? AS DATE) + cast(d AS INTEGER), 10., 12., 9., 10. + i / 100. + d,
            100., 1000. FROM range(1, ?) codes(i) CROSS JOIN range(?) days(d)""",
            [_FIRST, count + 1, days],
        )
        seed.execute("INSERT INTO adj_factor SELECT ts_code, trade_date, 1. FROM daily_bar")
        seed.execute(
            """INSERT INTO trade_calendar SELECT 'SSE',
            cast(? AS DATE) + cast(d AS INTEGER), TRUE,
            cast(? AS DATE) + cast(d AS INTEGER) - 1 FROM range(?) days(d)""",
            [_FIRST, _FIRST, days],
        )
    _sidecar(path)
    return path


def _sidecar(path: Path) -> None:
    source = ReplicaDatabaseWatermark(
        main=ReplicaFileWatermark(device=0, inode=123, size=456, mtime_ns=1)
    )
    metadata = ReplicaGenerationMetadata(
        source_database=_PRIMARY,
        source_before=source,
        source_after=source,
        replica=capture_file_watermark(path),
    )
    replica_generation_path(path).write_text(metadata.model_dump_json())


def _request(path: Path, *, count: int = 3, days: int = 4) -> FactorSourcePrepareRequest:
    return FactorSourcePrepareRequest(
        replica_path=path,
        expected_primary_path=_PRIMARY,
        scope=FactorComputationScope(
            stock_codes=tuple(f"{i:06d}.SZ" for i in range(1, count + 1)),
            start_date=_FIRST,
            end_date=_FIRST + timedelta(days=days - 1),
            as_of_time=_AS_OF,
        ),
        code_commit="a" * 40,
    )


def test_preparation_observes_missing_rows_and_nulls_then_admits(tmp_path: Path) -> None:
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission

    path = _replica(tmp_path)
    with duckdb.connect(str(path)) as seed:
        seed.execute("DELETE FROM daily_bar WHERE ts_code='000003.SZ' AND trade_date=?", [_FIRST])
        seed.execute(
            "UPDATE adj_factor SET adj_factor=NULL WHERE ts_code='000002.SZ' AND trade_date=?",
            [_FIRST],
        )
    _sidecar(path)
    request = _request(path)
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            request, metadata_store=metadata, lake_root=lake, now=lambda: _AS_OF
        )
        assert prepared.snapshot.status == prepared.binding.status == "ready"
        assert prepared.admission_request.scope == request.scope
        bars, adjustments, calendar = prepared.receipt.tables
        assert bars.row_count == 11 and bars.structural_missing_rows == 1
        assert adjustments.row_count == 12 and adjustments.structural_missing_rows == 0
        assert (
            dict((item.column, item.count) for item in adjustments.null_counts)["adj_factor"] == 1
        )
        assert calendar.row_count == 4
        assert prepared.receipt.calendar_boundary_status == "outside_anchor_unverified"
        assert prepared.snapshot.table_watermarks["manifest_start_date"] == _FIRST.isoformat()
        assert len(prepared.binding.manifest.artifacts) == 4
        with open_factor_stream_snapshot_admission(
            prepared.admission_request, metadata_store=metadata, lake_root=lake
        ) as (lease, decision):
            assert decision.scope_content_hash == prepared.scope_content_hash
            batch = lease.query_daily_bars(
                FactorReadQuery(
                    binding_hash=prepared.binding.binding_hash,
                    stock_codes=_CODES,
                    start_date=_FIRST,
                    end_date=_FIRST + timedelta(days=3),
                    row_limit=100,
                )
            )
            assert len(batch.rows) == 11
        assert not list((lake / ".execution_sessions").iterdir())


class _ConnectionProbe:
    def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
        self.connection = connection
        self.commands: list[str] = []
        self.closed = False

    def execute(self, command: str, parameters: object = None) -> duckdb.DuckDBPyConnection:
        self.commands.append(command)
        if parameters is None:
            return self.connection.execute(command)
        return self.connection.execute(command, parameters)

    def close(self) -> None:
        self.connection.close()
        self.closed = True


def _track_connection(monkeypatch: pytest.MonkeyPatch) -> list[tuple[_ConnectionProbe, int]]:
    from rquant.factor import source_prepare

    original = source_prepare.connect_pinned_readonly
    opened: list[tuple[_ConnectionProbe, int]] = []

    def connect(path: Path, descriptor: int) -> tuple[_ConnectionProbe, str]:
        connection, mode = original(path, descriptor)
        probe = _ConnectionProbe(connection)
        opened.append((probe, descriptor))
        return probe, mode

    monkeypatch.setattr(source_prepare, "connect_pinned_readonly", connect)
    return opened


def _assert_cleaned(opened: list[tuple[_ConnectionProbe, int]], lake: Path) -> None:
    assert len(opened) == 1
    probe, descriptor = opened[0]
    assert probe.closed
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        probe.connection.execute("SELECT 1")
    with pytest.raises(OSError):
        os.fstat(descriptor)
    assert not list(lake.glob(".factor-source-prepare-*"))
    assert not list(lake.rglob("*.tmp-*"))
    sessions = lake / ".execution_sessions"
    assert not sessions.exists() or not list(sessions.iterdir())


def test_observation_and_exports_share_one_read_transaction_and_never_stat_primary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _replica(tmp_path)
    original_stat, original_lstat = Path.stat, Path.lstat

    def stat_path(path: Path, *args: object, **kwargs: object) -> os.stat_result:
        assert path != _PRIMARY, "preparation touched the primary"
        return original_stat(path, *args, **kwargs)

    def lstat_path(path: Path, *args: object, **kwargs: object) -> os.stat_result:
        assert path != _PRIMARY, "preparation touched the primary"
        return original_lstat(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", stat_path)
    monkeypatch.setattr(Path, "lstat", lstat_path)
    opened = _track_connection(monkeypatch)
    lake = tmp_path / "lake"
    before = tuple(sorted(item.name for item in path.parent.iterdir()))
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            _request(path), metadata_store=metadata, lake_root=lake, now=lambda: _AS_OF
        )
    commands = opened[0][0].commands
    assert commands.count("BEGIN TRANSACTION") == commands.count("COMMIT") == 1
    first, last = commands.index("BEGIN TRANSACTION"), commands.index("COMMIT")
    assert sum(command.lstrip().startswith("COPY") for command in commands[first:last]) == 4
    assert any("GROUP BY ts_code" in command for command in commands[first:last])
    assert any("GROUP BY trade_date" in command for command in commands[first:last])
    assert commands.index(
        next(command for command in commands if "count(*) FILTER" in command)
    ) < commands.index(next(command for command in commands if command.lstrip().startswith("COPY")))
    assert commands[0] == "SET temp_directory = ?"
    assert prepared.receipt.read_mode in ("descriptor", "in_place")
    assert tuple(sorted(item.name for item in path.parent.iterdir())) == before
    _assert_cleaned(opened, lake)
    print(
        "SOURCE_PREPARE_RESOURCES: source_transactions=1 exports=4 primary_accesses=0 "
        "connection_closed=true fd_closed=true scratch_clean=true"
    )


def test_actual_preparation_runs_formula_statistics_and_decay(tmp_path: Path) -> None:
    from rquant.factor.capability import HISTORICAL_DAILY_V1
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.formula_stream import FactorFormulaStreamRequest, FactorFormulaStreamSources
    from rquant.factor.stream_adapter import FactorStreamAdapterRequest
    from rquant.factor.stream_runner import run_factor_stream_research_with_decay
    from rquant.factor.time_series import DecisionTime
    from tests.unit.test_factor_stream_adapter import _at, _pools

    path = _replica(tmp_path, count=12, days=8)
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            _request(path, count=12, days=8),
            metadata_store=metadata,
            lake_root=lake,
            now=lambda: _AS_OF,
        )
        calculation = tuple(_FIRST + timedelta(days=offset) for offset in (1, 2, 3, 4))
        formula = FactorFormulaStreamRequest(
            definition=build_factor_definition(
                factor_id="prepared_price",
                name_zh="已准备日线",
                category="technical",
                direction="higher_is_better",
                version=1,
                earliest_available_date=None,
                expression="ts_mean(close, 2)",
                feature_catalog=HISTORICAL_DAILY_V1.feature_catalog(),
            ),
            computation_stock_codes=prepared.admission_request.scope.stock_codes,
            trading_days=calculation,
            decision_times=tuple(
                DecisionTime(trade_date=day, decision_at=_at(day, 9, 25)) for day in calculation
            ),
            as_of=_AS_OF,
            selection="all",
            sources=FactorFormulaStreamSources(
                source_mode="historical_retrospective",
                feature_source_id=prepared.admission_request.snapshot_id,
                feature_source_sha256=prepared.admission_request.binding_hash,
                security_source_id="synthetic-security-archive",
                security_source_sha256="b" * 64,
            ),
        )
        request = FactorStreamAdapterRequest(
            source=prepared.admission_request,
            scope_content_hash=prepared.scope_content_hash,
            formula=formula,
            evaluation_days=calculation[1:3],
            holding_sessions=1,
        )
        result = run_factor_stream_research_with_decay(
            request, metadata_store=metadata, lake_root=lake, universe_requests=_pools(request)
        )
        assert result.research.adapter_completion.processed_days == 4
        assert result.research.formula_completion.processed_days == 4
        assert all(day.coverage.valid_count == 12 for day in result.research.statistics.days)
        assert len(result.decay.periods) == 10
        assert result.decay.periods[0].ic_summary == result.research.statistics.ic_summary
        assert result.research.request.source.snapshot_id == prepared.snapshot.snapshot_id
        assert not list((lake / ".execution_sessions").iterdir())


@pytest.mark.parametrize("damage", ["missing_calendar", "broken_chain", "wrong_schema"])
def test_actual_bad_calendar_or_schema_cannot_prepare(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    path = _replica(tmp_path)
    with duckdb.connect(str(path)) as seed:
        if damage == "missing_calendar":
            seed.execute(
                "DELETE FROM trade_calendar WHERE cal_date=?", [_FIRST + timedelta(days=1)]
            )
        elif damage == "broken_chain":
            seed.execute(
                "UPDATE trade_calendar SET pretrade_date=? WHERE cal_date=?",
                [_FIRST, _FIRST + timedelta(days=2)],
            )
        else:
            seed.execute("ALTER TABLE daily_bar DROP COLUMN amount")
    _sidecar(path)
    opened = _track_connection(monkeypatch)
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        with pytest.raises(ValueError, match="calendar|columns"):
            prepare_factor_stream_source(_request(path), metadata_store=metadata, lake_root=lake)
        assert metadata._conn.execute("SELECT count(*) FROM dataset_snapshot").fetchone() == (0,)
    assert "ROLLBACK" in opened[0][0].commands
    _assert_cleaned(opened, lake)


@pytest.mark.parametrize("damage", ["wal", "watermark"])
def test_wal_or_sidecar_mismatch_is_rejected_before_source_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    path = _replica(tmp_path)
    if damage == "wal":
        Path(f"{path}.wal").write_bytes(b"synthetic uncheckpointed WAL")
    else:
        os.utime(path, ns=(1, 2))
    opened = _track_connection(monkeypatch)
    with (
        DuckDBStore(tmp_path / "metadata.duckdb") as metadata,
        pytest.raises(ValueError, match="WAL|generation"),
    ):
        prepare_factor_stream_source(
            _request(path), metadata_store=metadata, lake_root=tmp_path / "lake"
        )
    assert opened == []


@pytest.mark.parametrize("changed", ["replica", "sidecar"])
def test_generation_replacement_after_export_is_rejected_and_cleaned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: str
) -> None:
    from rquant.factor import source_prepare

    path = _replica(tmp_path)
    opened = _track_connection(monkeypatch)
    original = source_prepare._materialize_factor_stream_artifacts

    def materialize(**kwargs: object) -> object:
        artifacts = original(**kwargs)
        if changed == "replica":
            replacement = tmp_path / "replacement.duckdb"
            shutil.copyfile(path, replacement)
            os.replace(replacement, path)
            _sidecar(path)
        else:
            sidecar = replica_generation_path(path)
            sidecar.write_bytes(sidecar.read_bytes() + b"\n")
        return artifacts

    monkeypatch.setattr(source_prepare, "_materialize_factor_stream_artifacts", materialize)
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        with pytest.raises(ValueError, match="generation"):
            prepare_factor_stream_source(_request(path), metadata_store=metadata, lake_root=lake)
        assert metadata._conn.execute("SELECT count(*) FROM dataset_snapshot").fetchone() == (0,)
    _assert_cleaned(opened, lake)


def test_replica_refresh_after_freezing_keeps_the_verified_historical_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission

    path = _replica(tmp_path)
    original_identity = path.stat().st_ino
    opened = _track_connection(monkeypatch)
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        original_begin = metadata.begin_dataset_snapshot

        def begin(snapshot: object) -> object:
            assert opened[0][0].commands[-1] == "COMMIT"
            replacement = tmp_path / "refreshed.duckdb"
            shutil.copyfile(path, replacement)
            with duckdb.connect(str(replacement)) as newer:
                newer.execute("UPDATE daily_bar SET close=999.")
            os.replace(replacement, path)
            _sidecar(path)
            return original_begin(snapshot)

        monkeypatch.setattr(metadata, "begin_dataset_snapshot", begin)
        prepared = prepare_factor_stream_source(
            _request(path), metadata_store=metadata, lake_root=lake, now=lambda: _AS_OF
        )
        assert path.stat().st_ino != original_identity
        assert prepared.receipt.generation.replica.inode == original_identity
        with open_factor_stream_snapshot_admission(
            prepared.admission_request, metadata_store=metadata, lake_root=lake
        ) as (lease, _decision):
            rows = lease.query_daily_bars(
                FactorReadQuery(
                    binding_hash=prepared.binding.binding_hash,
                    stock_codes=(_CODES[0],),
                    start_date=_FIRST,
                    end_date=_FIRST,
                    row_limit=1,
                )
            ).rows
            assert rows[0].close == pytest.approx(10.01)
    _assert_cleaned(opened, lake)


@pytest.mark.parametrize("cancel", [False, True])
def test_materialization_failure_or_cancellation_rolls_back_and_closes_owned_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel: bool
) -> None:
    from rquant.factor import stream_snapshot

    path = _replica(tmp_path)
    untouched = path.parent / "unrelated.txt"
    untouched.write_text("keep")
    opened = _track_connection(monkeypatch)
    original = stream_snapshot.materialize_table_dependency

    def materialize(*args: object, **kwargs: object) -> object:
        if kwargs["dependency"].table_name == "adj_factor":
            if cancel:
                raise KeyboardInterrupt("synthetic preparation cancellation")
            raise RuntimeError("synthetic materialization failure")
        return original(*args, **kwargs)

    monkeypatch.setattr(stream_snapshot, "materialize_table_dependency", materialize)
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        with pytest.raises(KeyboardInterrupt if cancel else RuntimeError):
            prepare_factor_stream_source(_request(path), metadata_store=metadata, lake_root=lake)
        assert metadata._conn.execute("SELECT count(*) FROM dataset_snapshot").fetchone() == (0,)
    assert "ROLLBACK" in opened[0][0].commands
    assert list((lake / "tables" / "daily_bar" / "versions").glob("*.parquet"))
    assert untouched.read_text() == "keep"
    _assert_cleaned(opened, lake)


@pytest.mark.parametrize("failure", ["publish", "finalize"])
def test_binding_failure_leaves_ready_raw_source_but_no_admissible_prepared_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    from rquant.factor import stream_snapshot
    from rquant.factor.stream_snapshot import (
        FactorStreamSnapshotAdmissionError,
        FactorStreamSnapshotAdmissionRequest,
        open_factor_stream_snapshot_admission,
    )

    path = _replica(tmp_path)
    opened = _track_connection(monkeypatch)
    snapshots = []
    lake = tmp_path / "lake"

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("synthetic binding publication failure")

    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        original = metadata.begin_dataset_snapshot

        def begin(snapshot: object) -> object:
            snapshots.append(snapshot)
            return original(snapshot)

        monkeypatch.setattr(metadata, "begin_dataset_snapshot", begin)
        if failure == "publish":
            monkeypatch.setattr(stream_snapshot, "_publish_binding_manifest", fail)
        else:
            monkeypatch.setattr(metadata, "finalize_dataset_snapshot_binding", fail)
        request = _request(path)
        with pytest.raises(RuntimeError, match="binding"):
            prepare_factor_stream_source(
                request, metadata_store=metadata, lake_root=lake, now=lambda: _AS_OF
            )
        assert len(snapshots) == 1
        snapshot = metadata.get_dataset_snapshot(snapshots[0].snapshot_id)
        binding = metadata.get_dataset_snapshot_binding(snapshot.snapshot_id)
        assert snapshot.status == "ready"
        assert binding is None if failure == "publish" else binding.status == "building"
        admission = FactorStreamSnapshotAdmissionRequest(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash if binding else "0" * 64,
            scope=request.scope,
        )
        with (
            pytest.raises(FactorStreamSnapshotAdmissionError),
            open_factor_stream_snapshot_admission(
                admission, metadata_store=metadata, lake_root=lake
            ),
        ):
            pytest.fail("failed preparation became admissible")
    _assert_cleaned(opened, lake)


def test_prepared_models_are_strict_and_bind_actual_scope_and_observations(tmp_path: Path) -> None:
    from rquant.factor.source_prepare import (
        FactorPreparedStreamSource,
        FactorSourcePreparationReceipt,
    )
    from rquant.runtime_contracts import canonical_sha256

    path = _replica(tmp_path)
    request = _request(path)
    with pytest.raises(ValidationError):
        request.code_commit = "b" * 40
    with pytest.raises(ValidationError):
        FactorSourcePrepareRequest(**request.model_dump(), coverage_complete=True)
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            request, metadata_store=metadata, lake_root=tmp_path / "lake", now=lambda: _AS_OF
        )
    assert (
        FactorPreparedStreamSource.model_validate_json(
            prepared.model_dump_json(exclude_computed_fields=True)
        )
        == prepared
    )
    changed = prepared.receipt.tables[0].model_copy(update={"row_count": 1})
    fields = prepared.receipt.model_dump(exclude={"sha256"})
    fields["tables"] = (changed, *prepared.receipt.tables[1:])
    with pytest.raises(ValidationError, match="exported artifact"):
        FactorSourcePreparationReceipt(**fields, sha256=canonical_sha256(fields))
    fields = prepared.model_dump(exclude={"sha256"})
    fields["snapshot"] = prepared.snapshot
    fields["binding"] = prepared.binding
    fields["admission_request"] = prepared.admission_request.model_copy(
        update={"binding_hash": "0" * 64}
    )
    with pytest.raises(ValidationError, match="completion binding"):
        FactorPreparedStreamSource(**fields, sha256=canonical_sha256(fields))
