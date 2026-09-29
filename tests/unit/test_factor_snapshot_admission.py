"""Factor snapshots admit only one verified, bounded historical read generation."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.data_metadata import (
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.factor_snapshot_admission import FactorSnapshotAdmissionRequest
from rquant.storage.duckdb import DuckDBStore

_FIRST = date(2026, 7, 17)
_LAST = date(2026, 7, 20)
_AS_OF = datetime(2026, 7, 21, 8, tzinfo=UTC)
_CODES = ("000001.SZ", "000002.SZ", "000003.SZ")


def _seed_source(store: DuckDBStore) -> None:
    store._conn.executemany(
        "INSERT INTO trade_calendar VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("SSE", date(2026, 7, 17), True, date(2026, 7, 16), "test", _AS_OF),
            ("SSE", date(2026, 7, 18), False, date(2026, 7, 17), "test", _AS_OF),
            ("SSE", date(2026, 7, 19), False, date(2026, 7, 17), "test", _AS_OF),
            ("SSE", date(2026, 7, 20), True, date(2026, 7, 17), "test", _AS_OF),
        ],
    )
    for day_index, day in enumerate((_FIRST, _LAST)):
        for stock_index, code in enumerate(_CODES):
            open_price = float(10 + stock_index + day_index)
            store._conn.execute(
                """INSERT INTO daily_bar
                (ts_code, trade_date, open, high, low, close, vol, amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [
                    code,
                    day,
                    open_price,
                    open_price + 1,
                    open_price - 1,
                    open_price + 0.5,
                    100.0,
                    1000.0,
                ],
            )
            store._conn.execute(
                "INSERT INTO adj_factor VALUES (?, ?, ?)",
                [code, day, float(1 + stock_index / 10)],
            )


def _snapshot(store: DuckDBStore) -> DatasetSnapshot:
    building = DatasetSnapshot.create(
        strategy_name="factor_eval",
        manifest_id="m" * 64,
        as_of_time=_AS_OF,
        code_commit="a" * 40,
        origin="synthetic-test",
        created_at=_AS_OF,
    )
    store.begin_dataset_snapshot(building)
    return store.finalize_dataset_snapshot(
        building.snapshot_id,
        DatasetSnapshotFinalization(
            table_watermarks={
                "manifest_start_date": _FIRST.isoformat(),
                "manifest_end_date": _LAST.isoformat(),
            },
            completed_at=_AS_OF,
        ),
    )


def _binding(
    store: DuckDBStore, source: duckdb.DuckDBPyConnection, lake_root: Path
) -> tuple[DatasetSnapshot, DatasetSnapshotBinding]:
    from rquant.research_snapshot import build_factor_snapshot_binding

    snapshot = _snapshot(store)
    binding = build_factor_snapshot_binding(
        metadata_store=store,
        source_connection=source,
        lake_root=lake_root,
        snapshot_id=snapshot.snapshot_id,
        start_date=_FIRST,
        end_date=_LAST,
        ts_codes=_CODES,
        now=lambda: _AS_OF,
    )
    return snapshot, binding


def _request(
    snapshot: DatasetSnapshot, binding: DatasetSnapshotBinding
) -> FactorSnapshotAdmissionRequest:
    return FactorSnapshotAdmissionRequest(
        snapshot_id=snapshot.snapshot_id,
        binding_hash=binding.binding_hash,
        start_date=_FIRST,
        end_date=_LAST,
        source_mode="historical_retrospective",
    )


def _changed_binding(
    binding: DatasetSnapshotBinding, **manifest_changes: object
) -> DatasetSnapshotBinding:
    manifest = DatasetSnapshotBindingManifest.model_validate(
        {
            **binding.manifest.model_dump(exclude_computed_fields=True),
            **manifest_changes,
        }
    )
    return DatasetSnapshotBinding.create(
        manifest=manifest,
        artifact_root=binding.artifact_root,
        manifest_relative_path=binding.manifest_relative_path,
        created_at=binding.created_at,
    ).finalize(DatasetSnapshotBindingFinalization(completed_at=binding.completed_at))


def test_factor_dependency_is_exactly_three_tables_and_zero_lake_partitions() -> None:
    from rquant.strategy_dependencies import (
        StrategyExecutionDependencies,
        factor_execution_dependencies,
        strategy_execution_dependencies,
    )

    factor = factor_execution_dependencies()
    assert factor.strategy_id == "factor_eval"
    assert factor.contract_version == "factor-eval-v1"
    assert factor.lake_datasets == ()
    assert [(item.dataset_id, item.table_name) for item in factor.materialized_tables] == [
        ("daily_bar", "daily_bar"),
        ("adj_factor", "adj_factor"),
        ("trade_calendar", "trade_calendar"),
    ]
    assert all(
        strategy_execution_dependencies(name).lake_datasets
        for name in ("n_shape", "growth_board_surge", "auction_gap")
    )
    with pytest.raises(ValueError, match="lake_datasets"):
        StrategyExecutionDependencies(
            strategy_id="n_shape",
            contract_version="stage1-v1",
            lake_datasets=(),
            materialized_tables=factor.materialized_tables,
        )


def test_factor_admission_reads_frozen_three_table_rows_and_receipt(tmp_path: Path) -> None:
    from rquant.factor_snapshot_admission import open_factor_snapshot_admission
    from rquant.research_snapshot import FactorReadQuery

    source_path = tmp_path / "source.duckdb"
    lake_root = tmp_path / "lake"
    with DuckDBStore(source_path) as store:
        _seed_source(store)
        with duckdb.connect(str(source_path)) as source:
            snapshot, binding = _binding(store, source, lake_root)
            assert binding.status == "ready"
            assert binding.manifest.dependency_contract_version == "factor-eval-v1"
            assert binding.manifest.builder_version == "factor-single-snapshot-v1"
            assert len(binding.manifest.artifacts) == 3
            assert all(
                item.artifact_type == "materialized_table" for item in binding.manifest.artifacts
            )
            source.execute("UPDATE daily_bar SET close = 99 WHERE ts_code = '000001.SZ'")
            source.execute("UPDATE adj_factor SET adj_factor = 99 WHERE ts_code = '000001.SZ'")

        with open_factor_snapshot_admission(
            _request(snapshot, binding), metadata_store=store, lake_root=lake_root
        ) as (lease, decision):
            assert decision.allowed is True
            assert decision.research_status == "exploratory"
            assert decision.snapshot_id == snapshot.snapshot_id
            assert decision.binding_hash == binding.binding_hash
            assert decision.as_of_time == snapshot.as_of_time
            assert decision.source_read_boundary == "single_snapshot_transaction"
            query = FactorReadQuery(
                binding_hash=binding.binding_hash,
                stock_codes=("000003.SZ", "000001.SZ"),
                start_date=_FIRST,
                end_date=_LAST,
                row_limit=20,
            )
            bars = lease.query_daily_bars(query)
            adjustments = lease.query_adj_factors(query)
            calendar = lease.query_sse_calendar(query)
            assert query.stock_codes == ("000001.SZ", "000003.SZ")
            assert [(row.trade_date, row.ts_code) for row in bars.rows] == [
                (_FIRST, "000001.SZ"),
                (_FIRST, "000003.SZ"),
                (_LAST, "000001.SZ"),
                (_LAST, "000003.SZ"),
            ]
            assert bars.rows[0].close == 10.5
            assert adjustments.rows[0].adj_factor == 1.0
            assert [(row.cal_date, row.is_open, row.pretrade_date) for row in calendar.rows] == [
                (_FIRST, True, date(2026, 7, 16)),
                (date(2026, 7, 18), False, _FIRST),
                (date(2026, 7, 19), False, _FIRST),
                (_LAST, True, _FIRST),
            ]
            for batch in (bars, adjustments, calendar):
                assert batch.receipt.snapshot_id == snapshot.snapshot_id
                assert batch.receipt.binding_hash == binding.binding_hash
                assert batch.receipt.as_of_time == _AS_OF
                assert batch.receipt.source_mode == "historical_retrospective"
                assert batch.receipt.source_read_boundary == "single_snapshot_transaction"
            assert not hasattr(lease, "connection")
            assert not hasattr(lease, "_conn")
        with pytest.raises(RuntimeError, match="closed"):
            lease.query_daily_bars(query)


def test_factor_lease_keeps_frozen_rows_after_source_parquet_in_place_rewrite(
    tmp_path: Path,
) -> None:
    from rquant.factor_snapshot_admission import open_factor_snapshot_admission
    from rquant.research_snapshot import FactorReadQuery

    source_path = tmp_path / "source.duckdb"
    lake_root = tmp_path / "lake"
    with DuckDBStore(source_path) as store:
        _seed_source(store)
        with duckdb.connect(str(source_path)) as source:
            snapshot, binding = _binding(store, source, lake_root)
        artifact = next(
            item for item in binding.manifest.artifacts if item.table_name == "daily_bar"
        )
        artifact_path = lake_root / artifact.relative_path
        query = FactorReadQuery(
            binding_hash=binding.binding_hash,
            stock_codes=("000001.SZ",),
            start_date=_FIRST,
            end_date=_LAST,
            row_limit=5,
        )
        with open_factor_snapshot_admission(
            _request(snapshot, binding), metadata_store=store, lake_root=lake_root
        ) as (lease, _decision):
            inode_before = artifact_path.stat().st_ino
            changed = pd.read_parquet(artifact_path)
            changed.loc[
                (changed["ts_code"] == "000001.SZ") & (changed["trade_date"] == _FIRST),
                "close",
            ] = 999.0
            changed.to_parquet(artifact_path, index=False)
            assert artifact_path.stat().st_ino == inode_before
            assert lease.query_daily_bars(query).rows[0].close == 10.5
        assert not list((lake_root / ".execution_sessions").iterdir())


def test_factor_admission_rejects_source_mutation_during_session_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import research_snapshot
    from rquant.factor_snapshot_admission import (
        FactorSnapshotAdmissionError,
        open_factor_snapshot_admission,
    )

    source_path = tmp_path / "source.duckdb"
    lake_root = tmp_path / "lake"
    with DuckDBStore(source_path) as store:
        _seed_source(store)
        with duckdb.connect(str(source_path)) as source:
            snapshot, binding = _binding(store, source, lake_root)
        original_copy = research_snapshot.shutil.copyfile
        copies = 0

        def mutate_before_copy(source: Path, target: Path) -> Path:
            nonlocal copies
            copies += 1
            if copies == 1:
                source.write_bytes(b"changed during session copy")
            return original_copy(source, target)

        monkeypatch.setattr(research_snapshot.shutil, "copyfile", mutate_before_copy)
        with (
            pytest.raises(FactorSnapshotAdmissionError) as error,
            open_factor_snapshot_admission(
                _request(snapshot, binding), metadata_store=store, lake_root=lake_root
            ),
        ):
            pytest.fail("mutated source exposed a factor lease")
        assert copies >= 1
        assert {item.code for item in error.value.decision.failures} == {
            "session_verification_failed"
        }
        assert not list((lake_root / ".execution_sessions").iterdir())


def test_factor_builder_rejects_preopened_source_transaction(tmp_path: Path) -> None:
    from rquant.research_snapshot import build_factor_snapshot_binding

    path = tmp_path / "source.duckdb"
    with DuckDBStore(path) as store:
        _seed_source(store)
        snapshot = _snapshot(store)
        with duckdb.connect(str(path)) as source:
            source.execute("BEGIN TRANSACTION")
            try:
                with pytest.raises(ValueError, match="transaction"):
                    build_factor_snapshot_binding(
                        metadata_store=store,
                        source_connection=source,
                        lake_root=tmp_path / "lake",
                        snapshot_id=snapshot.snapshot_id,
                        start_date=_FIRST,
                        end_date=_LAST,
                        ts_codes=_CODES,
                    )
            finally:
                source.execute("ROLLBACK")


def test_factor_builder_uses_one_source_read_view_during_concurrent_update(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import research_snapshot
    from rquant.factor_snapshot_admission import open_factor_snapshot_admission
    from rquant.research_snapshot import FactorReadQuery

    path = tmp_path / "source.duckdb"
    lake_root = tmp_path / "lake"
    with DuckDBStore(path) as store:
        _seed_source(store)
        snapshot = _snapshot(store)
        original = research_snapshot.materialize_table_dependency
        seen = 0

        def change_after_first(*args: object, **kwargs: object) -> DatasetSnapshotArtifact:
            nonlocal seen
            artifact = original(*args, **kwargs)
            seen += 1
            if seen == 1:
                with duckdb.connect(str(path)) as writer:
                    writer.execute("UPDATE daily_bar SET close = 99")
                    writer.execute("UPDATE adj_factor SET adj_factor = 99")
            return artifact

        monkeypatch.setattr(research_snapshot, "materialize_table_dependency", change_after_first)
        with duckdb.connect(str(path)) as source:
            from rquant.research_snapshot import build_factor_snapshot_binding

            binding = build_factor_snapshot_binding(
                metadata_store=store,
                source_connection=source,
                lake_root=lake_root,
                snapshot_id=snapshot.snapshot_id,
                start_date=_FIRST,
                end_date=_LAST,
                ts_codes=_CODES,
            )
        assert seen == 3
        with open_factor_snapshot_admission(
            _request(snapshot, binding), metadata_store=store, lake_root=lake_root
        ) as (lease, _decision):
            query = FactorReadQuery(
                binding_hash=binding.binding_hash,
                stock_codes=("000001.SZ",),
                start_date=_FIRST,
                end_date=_LAST,
                row_limit=5,
            )
            assert lease.query_daily_bars(query).rows[0].close == 10.5
            assert lease.query_adj_factors(query).rows[0].adj_factor == 1.0


def test_factor_lease_rejects_wrong_generation_and_query_bounds(tmp_path: Path) -> None:
    from rquant.factor_snapshot_admission import open_factor_snapshot_admission
    from rquant.research_snapshot import FactorReadQuery

    path = tmp_path / "source.duckdb"
    with DuckDBStore(path) as store:
        _seed_source(store)
        with duckdb.connect(str(path)) as source:
            snapshot, binding = _binding(store, source, tmp_path / "lake")
        with open_factor_snapshot_admission(
            _request(snapshot, binding), metadata_store=store, lake_root=tmp_path / "lake"
        ) as (lease, _decision):
            with pytest.raises(ValueError, match="binding_hash"):
                lease.query_daily_bars(
                    FactorReadQuery(
                        binding_hash="f" * 64,
                        stock_codes=(_CODES[0],),
                        start_date=_FIRST,
                        end_date=_LAST,
                        row_limit=10,
                    )
                )
            with pytest.raises(ValueError, match="row limit"):
                lease.query_daily_bars(
                    FactorReadQuery(
                        binding_hash=binding.binding_hash,
                        stock_codes=_CODES,
                        start_date=_FIRST,
                        end_date=_LAST,
                        row_limit=1,
                    )
                )
            absent = lease.query_daily_bars(
                FactorReadQuery(
                    binding_hash=binding.binding_hash,
                    stock_codes=("999999.SZ",),
                    start_date=_FIRST,
                    end_date=_LAST,
                    row_limit=10,
                )
            )
            assert absent.rows == ()


@pytest.mark.parametrize(
    ("stocks", "start", "end", "row_limit"),
    [
        (("000001.SZ", "000001.SZ"), _FIRST, _LAST, 5),
        (("BAD",), _FIRST, _LAST, 5),
        ((_CODES[0],), _LAST, _FIRST, 5),
        ((_CODES[0],), _FIRST, _FIRST + timedelta(days=367), 5),
        ((_CODES[0],), _FIRST, _LAST, 100_001),
    ],
)
def test_factor_read_query_rejects_unbounded_or_duplicate_request(
    stocks: tuple[str, ...], start: date, end: date, row_limit: int
) -> None:
    from rquant.research_snapshot import FactorReadQuery

    with pytest.raises(ValidationError):
        FactorReadQuery(
            binding_hash="a" * 64,
            stock_codes=stocks,
            start_date=start,
            end_date=end,
            row_limit=row_limit,
        )


def test_legacy_builder_claim_cannot_admit_factor_binding(tmp_path: Path) -> None:
    from rquant.factor_snapshot_admission import evaluate_factor_snapshot_admission

    path = tmp_path / "source.duckdb"
    with DuckDBStore(path) as store:
        _seed_source(store)
        with duckdb.connect(str(path)) as source:
            snapshot, binding = _binding(store, source, tmp_path / "lake")
        forged = _changed_binding(binding, builder_version="snapshot-builder-v2")
        decision = evaluate_factor_snapshot_admission(
            _request(snapshot, forged), snapshot=snapshot, binding=forged
        )
        assert decision.allowed is False
        assert decision.research_status == "exploratory"
        assert decision.source_read_boundary is None
        assert "binding_builder" in {failure.code for failure in decision.failures}


@pytest.mark.parametrize(
    ("scenario", "expected"),
    [
        ("wrong_version", "binding_contract"),
        ("missing_daily_bar", "binding_artifacts"),
        ("missing_adj_factor", "binding_artifacts"),
        ("missing_trade_calendar", "binding_artifacts"),
        ("duplicate_table", "binding_artifacts"),
        ("wrong_dataset", "binding_artifacts"),
    ],
)
def test_factor_admission_rejects_wrong_contract_or_missing_artifacts(
    tmp_path: Path, scenario: str, expected: str
) -> None:
    from rquant.factor_snapshot_admission import evaluate_factor_snapshot_admission

    path = tmp_path / "source.duckdb"
    with DuckDBStore(path) as store:
        _seed_source(store)
        with duckdb.connect(str(path)) as source:
            snapshot, binding = _binding(store, source, tmp_path / "lake")
        artifacts = binding.manifest.artifacts
        if scenario == "wrong_version":
            manifest_changes: dict[str, object] = {"dependency_contract_version": "stage1-v1"}
        elif scenario.startswith("missing_"):
            missing = scenario.removeprefix("missing_")
            manifest_changes = {
                "artifacts": tuple(item for item in artifacts if item.table_name != missing)
            }
        elif scenario == "duplicate_table":
            manifest_changes = {
                "artifacts": artifacts
                + (artifacts[0].model_copy(update={"artifact_key": "adj_factor:duplicate"}),)
            }
        else:
            manifest_changes = {
                "artifacts": (
                    artifacts[0].model_copy(update={"dataset_id": "other_dataset"}),
                    *artifacts[1:],
                )
            }
        forged = _changed_binding(binding, **manifest_changes)
        decision = evaluate_factor_snapshot_admission(
            _request(snapshot, forged), snapshot=snapshot, binding=forged
        )
        assert decision.allowed is False
        assert expected in {failure.code for failure in decision.failures}


@pytest.mark.parametrize("table_name", ["daily_bar", "adj_factor", "trade_calendar"])
def test_factor_builder_refuses_missing_source_table(tmp_path: Path, table_name: str) -> None:
    from rquant.research_snapshot import build_factor_snapshot_binding

    path = tmp_path / "source.duckdb"
    with DuckDBStore(path) as store:
        _seed_source(store)
        snapshot = _snapshot(store)
        store._conn.execute(f'DROP TABLE "{table_name}"')
        with (
            duckdb.connect(str(path)) as source,
            pytest.raises(ValueError, match="source table missing"),
        ):
            build_factor_snapshot_binding(
                metadata_store=store,
                source_connection=source,
                lake_root=tmp_path / "lake",
                snapshot_id=snapshot.snapshot_id,
                start_date=_FIRST,
                end_date=_LAST,
                ts_codes=_CODES,
            )


def test_factor_admission_rejects_cross_generation_and_tampered_artifact(tmp_path: Path) -> None:
    from rquant.factor_snapshot_admission import (
        FactorSnapshotAdmissionError,
        evaluate_factor_snapshot_admission,
        open_factor_snapshot_admission,
    )

    path = tmp_path / "source.duckdb"
    lake_root = tmp_path / "lake"
    with DuckDBStore(path) as store:
        _seed_source(store)
        with duckdb.connect(str(path)) as source:
            snapshot, binding = _binding(store, source, lake_root)
        wrong = _request(snapshot, binding).model_copy(update={"binding_hash": "f" * 64})
        decision = evaluate_factor_snapshot_admission(wrong, snapshot=snapshot, binding=binding)
        assert decision.allowed is False
        assert "binding_identity" in {failure.code for failure in decision.failures}

        artifact = binding.manifest.artifacts[0]
        (lake_root / artifact.relative_path).write_bytes(b"tampered")
        with (
            pytest.raises(FactorSnapshotAdmissionError) as error,
            open_factor_snapshot_admission(
                _request(snapshot, binding), metadata_store=store, lake_root=lake_root
            ),
        ):
            pytest.fail("corrupt binding exposed a lease")
        assert {item.code for item in error.value.decision.failures} == {
            "session_verification_failed"
        }


def test_factor_builder_rejects_wrong_business_key_and_same_metadata_connection(
    tmp_path: Path,
) -> None:
    from rquant.research_snapshot import build_factor_snapshot_binding

    path = tmp_path / "source.duckdb"
    with DuckDBStore(path) as store:
        _seed_source(store)
        snapshot = _snapshot(store)
        with pytest.raises(ValueError, match="differ from metadata writer"):
            build_factor_snapshot_binding(
                metadata_store=store,
                source_connection=store._conn,
                lake_root=tmp_path / "lake",
                snapshot_id=snapshot.snapshot_id,
                start_date=_FIRST,
                end_date=_LAST,
                ts_codes=_CODES,
            )
        store._conn.execute("DROP TABLE adj_factor")
        store._conn.execute(
            "CREATE TABLE adj_factor (id INTEGER PRIMARY KEY, ts_code VARCHAR, "
            "trade_date DATE, adj_factor DOUBLE)"
        )
        store._conn.execute(
            "INSERT INTO adj_factor VALUES (1, '000001.SZ', ?, 1.0), (2, '000001.SZ', ?, 2.0)",
            [_FIRST, _FIRST],
        )
        with duckdb.connect(str(path)) as source, pytest.raises(ValueError, match="business key"):
            build_factor_snapshot_binding(
                metadata_store=store,
                source_connection=source,
                lake_root=tmp_path / "lake",
                snapshot_id=snapshot.snapshot_id,
                start_date=_FIRST,
                end_date=_LAST,
                ts_codes=_CODES,
            )


def test_formal_strategy_gate_still_rejects_factor_snapshot_without_audit_and_coverage(
    tmp_path: Path,
) -> None:
    from rquant.research_gate import ResearchGateRequest, evaluate_research_gate

    path = tmp_path / "source.duckdb"
    with DuckDBStore(path) as store:
        _seed_source(store)
        with duckdb.connect(str(path)) as source:
            snapshot, binding = _binding(store, source, tmp_path / "lake")
        decision = evaluate_research_gate(
            ResearchGateRequest(
                mode="formal",
                strategy_name="factor_eval",
                start_date=_FIRST,
                end_date=_LAST,
                dataset_snapshot_id=snapshot.snapshot_id,
                dataset_binding_hash=binding.binding_hash,
                code_commit=snapshot.code_commit,
            ),
            audit_run=None,
            snapshot=snapshot,
            binding=binding,
            binding_verified=True,
            coverages=(),
            open_p0_issues=(),
        )
        assert decision.allowed is False
        assert decision.research_status == "exploratory"
        assert "audit_missing" in {failure.code for failure in decision.failures}
        assert "coverage_eligibility_missing" in {failure.code for failure in decision.failures}
