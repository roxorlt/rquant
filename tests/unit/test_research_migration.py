"""Recoverable, fail-closed research migration tests."""

from __future__ import annotations

from contextlib import AbstractContextManager
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest

import rquant.research_migration as migration_module
from rquant.research_catalog import exclusive_file_lock
from rquant.research_migration import (
    create_recovery_snapshot,
    prepare_research_migration_bundle,
    publish_research_migration_bundle,
    verify_research_migration_bundle,
)

_COMMIT = "a" * 40


def _migration_source(path: Path) -> None:
    connection = duckdb.connect(str(path))
    connection.execute(
        """
        CREATE TABLE trade_calendar (
            exchange VARCHAR NOT NULL,
            cal_date DATE NOT NULL,
            is_open BOOLEAN NOT NULL,
            PRIMARY KEY (exchange, cal_date)
        );
        INSERT INTO trade_calendar VALUES ('SSE', '2026-07-14', TRUE);

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
        INSERT INTO minute_bar VALUES
            ('000001.SZ', '2026-07-14 09:30:00', '1min', 10, 10.2, 9.9, 10.1,
             1000, 10100, 'tushare', '2026-07-14 16:00:00'),
            ('000002.SZ', '2026-07-14 09:30:00', '1min', 20, 20.3, 19.8, 20.2,
             2000, 40400, 'tushare_rt_daily', '2026-07-14 16:00:00');

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
        INSERT INTO auction_bar VALUES
            ('000001.SZ', '2026-07-14', 'open', 10, 1000, 10000, 0.1, 1.5,
             'tushare', '2026-07-14 09:26:00');

        CREATE TABLE monitor_event (
            trade_date DATE NOT NULL,
            ts_code VARCHAR NOT NULL,
            level VARCHAR NOT NULL,
            trigger_time TIMESTAMP,
            PRIMARY KEY (trade_date, ts_code, level)
        );
        INSERT INTO monitor_event VALUES
            ('2026-07-14', '000001.SZ', 'B', '2026-07-14 09:35:00');

        CREATE TABLE intraday_feature_snapshot (
            snapshot_id VARCHAR PRIMARY KEY,
            payload JSON
        );
        CREATE TABLE paper_position (
            position_id VARCHAR PRIMARY KEY,
            status VARCHAR
        );
        CREATE TABLE paper_position_event (
            event_id VARCHAR PRIMARY KEY,
            position_id VARCHAR
        );
        CREATE TABLE dataset_snapshot (
            snapshot_id VARCHAR PRIMARY KEY,
            status VARCHAR
        );
        CREATE TABLE dataset_coverage (
            snapshot_id VARCHAR NOT NULL,
            dataset_id VARCHAR NOT NULL,
            coverage_scope VARCHAR NOT NULL,
            PRIMARY KEY (snapshot_id, dataset_id, coverage_scope)
        );
        CREATE TABLE data_quality_issue (
            issue_id VARCHAR PRIMARY KEY,
            status VARCHAR NOT NULL,
            evidence JSON NOT NULL
        );
        INSERT INTO data_quality_issue VALUES
            ('issue-1', 'resolved', '{"source":"frozen"}');

        CREATE TABLE daily_bar (
            ts_code VARCHAR NOT NULL,
            trade_date DATE NOT NULL,
            close DOUBLE,
            PRIMARY KEY (ts_code, trade_date)
        );
        INSERT INTO daily_bar VALUES ('000001.SZ', '2026-07-14', 10.1);
        """
    )
    connection.close()


def test_recovery_snapshot_is_wal_free_read_only_and_idempotent(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rquant.duckdb"
    connection = duckdb.connect(str(source))
    connection.execute("CREATE TABLE marker (value VARCHAR PRIMARY KEY)")
    connection.execute("INSERT INTO marker VALUES ('frozen')")
    connection.close()
    recovery_dir = tmp_path / "recovery"

    def now() -> datetime:
        return datetime(2026, 7, 16, 8, 0, tzinfo=UTC)

    first = create_recovery_snapshot(
        source,
        recovery_dir=recovery_dir,
        snapshot_id="research-20260716T160000Z-a1b2c3d4",
        code_commit=_COMMIT,
        now=now,
    )
    second = create_recovery_snapshot(
        source,
        recovery_dir=recovery_dir,
        snapshot_id="research-20260716T160000Z-a1b2c3d4",
        code_commit=_COMMIT,
        now=now,
    )

    assert first.status == "created"
    assert second.status == "unchanged"
    assert second.evidence == first.evidence
    assert first.database_path.stat().st_mode & 0o777 == 0o400
    assert not Path(f"{first.database_path}.wal").exists()
    assert first.evidence.table_count == 1
    assert first.evidence.file_size == first.database_path.stat().st_size
    assert len(first.evidence.sha256) == 64
    restored = duckdb.connect(str(first.database_path), read_only=True)
    try:
        assert restored.execute("SELECT value FROM marker").fetchone() == ("frozen",)
    finally:
        restored.close()


def test_recovery_snapshot_repairs_crash_after_database_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "rquant.duckdb"
    connection = duckdb.connect(str(source))
    connection.execute("CREATE TABLE marker (value INTEGER)")
    connection.execute("INSERT INTO marker VALUES (7)")
    connection.close()
    recovery_dir = tmp_path / "recovery"
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    original_replace = migration_module.os.replace

    def fail_metadata_publish(source_path: Path, target_path: Path) -> None:
        if Path(target_path).name == "snapshot.json":
            raise RuntimeError("simulated metadata publish crash")
        original_replace(source_path, target_path)

    monkeypatch.setattr(migration_module.os, "replace", fail_metadata_publish)
    with pytest.raises(RuntimeError, match="metadata publish crash"):
        create_recovery_snapshot(
            source,
            recovery_dir=recovery_dir,
            snapshot_id=snapshot_id,
            code_commit=_COMMIT,
        )

    database = recovery_dir / snapshot_id / "rquant.duckdb"
    assert database.is_file()
    assert not (recovery_dir / snapshot_id / "snapshot.json").exists()

    monkeypatch.setattr(migration_module.os, "replace", original_replace)
    repaired = create_recovery_snapshot(
        source,
        recovery_dir=recovery_dir,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )

    assert repaired.status == "unchanged"
    assert repaired.metadata_path.is_file()
    assert repaired.evidence.sha256 == migration_module._file_sha256(database)


def test_recovery_snapshot_does_not_rebind_artifacts_after_metadata_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "rquant.duckdb"
    connection = duckdb.connect(str(source))
    connection.execute("CREATE TABLE marker (value INTEGER)")
    connection.execute("INSERT INTO marker VALUES (7)")
    connection.close()
    artifacts = tmp_path / "strategy_lab_runs"
    artifacts.mkdir()
    artifact = artifacts / "run.json"
    artifact.write_text('{"generation":"A"}', encoding="utf-8")
    recovery_dir = tmp_path / "recovery"
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    original_replace = migration_module.os.replace

    def fail_metadata_publish(source_path: Path, target_path: Path) -> None:
        if Path(target_path).name == "snapshot.json":
            raise RuntimeError("simulated metadata publish crash")
        original_replace(source_path, target_path)

    monkeypatch.setattr(migration_module.os, "replace", fail_metadata_publish)
    with pytest.raises(RuntimeError, match="metadata publish crash"):
        create_recovery_snapshot(
            source,
            recovery_dir=recovery_dir,
            artifact_dir=artifacts,
            snapshot_id=snapshot_id,
            code_commit=_COMMIT,
        )

    artifact.write_text('{"generation":"B"}', encoding="utf-8")
    monkeypatch.setattr(migration_module.os, "replace", original_replace)
    with pytest.raises(RuntimeError, match="artifacts changed after recovery snapshot"):
        create_recovery_snapshot(
            source,
            recovery_dir=recovery_dir,
            artifact_dir=artifacts,
            snapshot_id=snapshot_id,
            code_commit=_COMMIT,
        )


def test_prepare_bundle_exports_only_research_state_and_lab_artifacts(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    artifacts = tmp_path / "strategy_lab_runs"
    artifacts.mkdir()
    (artifacts / "run-1.json").write_text('{"run_id":"run-1"}', encoding="utf-8")
    (artifacts / "run-1.md").write_text("# run-1\n", encoding="utf-8")
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )

    result = prepare_research_migration_bundle(
        recovery.database_path,
        bundle_dir=tmp_path / "bundles",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    )

    assert result.status == "created"
    assert result.manifest.source_snapshot_sha256 == recovery.evidence.sha256
    assert {item.dataset for item in result.manifest.datasets} == {
        "minute_bar",
        "auction_bar",
    }
    assert sum(item.row_count for item in result.manifest.datasets) == 3
    assert all(item.duplicate_key_count == 0 for item in result.manifest.datasets)
    assert all(item.sample_match_count == item.sample_size for item in result.manifest.datasets)
    assert {item.table_name for item in result.manifest.auxiliary_tables} == {
        "monitor_event",
        "intraday_feature_snapshot",
        "paper_position",
        "paper_position_event",
        "dataset_snapshot",
        "dataset_coverage",
        "data_quality_issue",
    }
    assert all(item.duplicate_key_count == 0 for item in result.manifest.auxiliary_tables)
    assert not list(result.bundle_path.rglob("*daily_bar*"))
    assert (result.bundle_path / "artifacts/strategy_lab_runs/run-1.json").is_file()
    assert (result.bundle_path / "artifacts/strategy_lab_runs/run-1.md").is_file()
    assert (result.bundle_path / "research.duckdb").is_file()
    assert result.manifest_path.is_file()
    assert all(len(item.sha256) == 64 for item in result.manifest.files)


def test_prepare_bundle_cannot_bypass_future_partition_guard(tmp_path: Path) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    connection = duckdb.connect(str(source))
    connection.execute("UPDATE trade_calendar SET cal_date = DATE '2099-01-02'")
    connection.execute(
        "UPDATE minute_bar SET trade_time = TIMESTAMP '2099-01-02 09:30:00'"
    )
    connection.execute("UPDATE auction_bar SET trade_date = DATE '2099-01-02'")
    connection.close()
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )

    with pytest.raises(ValueError, match="partition is in the future"):
        prepare_research_migration_bundle(
            recovery.database_path,
            bundle_dir=tmp_path / "bundles",
            artifact_dir=tmp_path / "strategy_lab_runs",
            snapshot_id=snapshot_id,
            code_commit=_COMMIT,
            start_date=date(2099, 1, 2),
            end_date=date(2099, 1, 2),
        )


def test_prepare_bundle_excludes_auxiliary_rows_after_scope_end(tmp_path: Path) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    connection = duckdb.connect(str(source))
    connection.execute(
        "INSERT INTO monitor_event VALUES "
        "('2099-01-02', '000002.SZ', 'B', '2099-01-02 09:35:00')"
    )
    connection.close()
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )

    prepared = prepare_research_migration_bundle(
        recovery.database_path,
        bundle_dir=tmp_path / "bundles",
        artifact_dir=tmp_path / "strategy_lab_runs",
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    )

    evidence = next(
        item
        for item in prepared.manifest.auxiliary_tables
        if item.table_name == "monitor_event"
    )
    assert evidence.row_count == 1
    auxiliary = duckdb.connect()
    try:
        assert auxiliary.execute(
            "SELECT DISTINCT trade_date FROM read_parquet(?)",
            [str(prepared.bundle_path / evidence.relative_path)],
        ).fetchall() == [(date(2026, 7, 14),)]
    finally:
        auxiliary.close()


def test_prepare_bundle_excludes_positions_with_future_exit_results(tmp_path: Path) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    connection = duckdb.connect(str(source))
    connection.execute("ALTER TABLE paper_position ADD COLUMN trade_date DATE")
    connection.execute("ALTER TABLE paper_position ADD COLUMN entry_time TIMESTAMP")
    connection.execute("ALTER TABLE paper_position ADD COLUMN exit_time TIMESTAMP")
    connection.execute(
        "INSERT INTO paper_position "
        "(position_id, status, trade_date, entry_time, exit_time) VALUES "
        "('position-1', 'closed', '2026-07-14', "
        "'2026-07-14 09:35:00', '2099-01-02 09:35:00')"
    )
    connection.close()
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )

    prepared = prepare_research_migration_bundle(
        recovery.database_path,
        bundle_dir=tmp_path / "bundles",
        artifact_dir=tmp_path / "strategy_lab_runs",
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    )

    evidence = next(
        item
        for item in prepared.manifest.auxiliary_tables
        if item.table_name == "paper_position"
    )
    assert evidence.row_count == 0


def test_verify_auxiliary_table_rejects_self_consistent_future_rows(
    tmp_path: Path,
) -> None:
    root = tmp_path / "bundle"
    data_path = root / "snapshots/dataset_id=monitor_event/snapshot_id=test/data.parquet"
    data_path.parent.mkdir(parents=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT DATE '2099-01-02' AS trade_date, '000001.SZ' AS ts_code, "
            "'B' AS level, TIMESTAMP '2099-01-02 09:35:00' AS trigger_time) "
            f"TO {migration_module._quoted_literal(str(data_path))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning = false)",
            [str(data_path)],
        ).fetchall()
        rows = connection.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning = false) "
            "ORDER BY trade_date, ts_code, level",
            [str(data_path)],
        ).fetchall()
    finally:
        connection.close()
    columns = tuple((str(row[0]), str(row[1])) for row in described)
    evidence = migration_module.AuxiliaryTableEvidence(
        table_name="monitor_event",
        relative_path=data_path.relative_to(root).as_posix(),
        row_count=1,
        duplicate_key_count=0,
        primary_key=("trade_date", "ts_code", "level"),
        scope_end_date=date(2026, 7, 14),
        schema_hash=migration_module._schema_hash(columns),
        content_hash=migration_module._canonical_rows_hash(rows),
        file_hash=migration_module._file_sha256(data_path),
    )
    data_path.with_name("manifest.json").write_text(
        evidence.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="date scope mismatch"):
        migration_module._verify_auxiliary_table(root, evidence)


def test_prepare_bundle_rejects_strategy_artifacts_that_change_during_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    artifacts = tmp_path / "strategy_lab_runs"
    artifacts.mkdir()
    run_file = artifacts / "run.json"
    run_file.write_text('{"status":"running"}', encoding="utf-8")
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )
    original_copy = migration_module.shutil.copy2

    def copy_then_change(source_path: Path, target_path: Path) -> str:
        result = original_copy(source_path, target_path)
        if Path(source_path) == run_file:
            run_file.write_text('{"status":"succeeded"}', encoding="utf-8")
        return str(result)

    monkeypatch.setattr(migration_module.shutil, "copy2", copy_then_change)

    with pytest.raises(RuntimeError, match="artifacts changed during migration copy"):
        prepare_research_migration_bundle(
            recovery.database_path,
            bundle_dir=tmp_path / "bundles",
            artifact_dir=artifacts,
            snapshot_id=snapshot_id,
            code_commit=_COMMIT,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
        )


def test_prepare_bundle_rejects_artifacts_changed_after_recovery_snapshot(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    artifacts = tmp_path / "strategy_lab_runs"
    artifacts.mkdir()
    run_file = artifacts / "run.json"
    run_file.write_text('{"status":"running"}', encoding="utf-8")
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )
    run_file.write_text('{"status":"succeeded"}', encoding="utf-8")

    with pytest.raises(RuntimeError, match="artifacts changed after recovery snapshot"):
        prepare_research_migration_bundle(
            recovery.database_path,
            bundle_dir=tmp_path / "bundles",
            artifact_dir=artifacts,
            snapshot_id=snapshot_id,
            code_commit=_COMMIT,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
        )


def test_existing_bundle_must_match_current_recovery_snapshot(tmp_path: Path) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    recovery_dir = tmp_path / "recovery"
    first_recovery = create_recovery_snapshot(
        source,
        recovery_dir=recovery_dir,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )
    prepare_research_migration_bundle(
        first_recovery.database_path,
        bundle_dir=tmp_path / "bundles",
        artifact_dir=tmp_path / "strategy_lab_runs",
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    )
    first_recovery.database_path.chmod(0o600)
    first_recovery.database_path.unlink()
    first_recovery.metadata_path.unlink()
    replacement_source = tmp_path / "replacement.duckdb"
    _migration_source(replacement_source)
    connection = duckdb.connect(str(replacement_source))
    connection.execute("UPDATE daily_bar SET close = 99")
    connection.close()
    replacement = create_recovery_snapshot(
        replacement_source,
        recovery_dir=recovery_dir,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )

    with pytest.raises(RuntimeError, match="recovery snapshot evidence mismatch"):
        prepare_research_migration_bundle(
            replacement.database_path,
            bundle_dir=tmp_path / "bundles",
            artifact_dir=tmp_path / "strategy_lab_runs",
            snapshot_id=snapshot_id,
            code_commit=_COMMIT,
            start_date=date(2026, 7, 14),
            end_date=date(2026, 7, 14),
        )


def test_verify_bundle_recomputes_evidence_and_rejects_corruption(
    tmp_path: Path,
) -> None:
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    artifacts = tmp_path / "strategy_lab_runs"
    artifacts.mkdir()
    (artifacts / "run.json").write_text('{"run_id":"run"}', encoding="utf-8")
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )
    prepared = prepare_research_migration_bundle(
        recovery.database_path,
        bundle_dir=tmp_path / "bundles",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    )

    verified = verify_research_migration_bundle(prepared.bundle_path)

    assert verified.status == "verified"
    assert verified.snapshot_id == snapshot_id
    assert verified.file_count == len(prepared.manifest.files)
    assert verified.partition_count == 2
    assert verified.row_count == 3
    assert verified.sample_match_count == verified.sample_size == 3
    assert verified.auxiliary_table_count == 7

    artifact = prepared.bundle_path / "artifacts/strategy_lab_runs/run.json"
    artifact.write_text("corrupt", encoding="utf-8")
    with pytest.raises(RuntimeError, match="file evidence mismatch"):
        verify_research_migration_bundle(prepared.bundle_path)


def _prepared_bundle(tmp_path: Path) -> tuple[str, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "rquant.duckdb"
    _migration_source(source)
    snapshot_id = "research-20260716T160000Z-a1b2c3d4"
    artifacts = tmp_path / "strategy_lab_runs"
    artifacts.mkdir()
    (artifacts / "run.json").write_text('{"run_id":"run"}', encoding="utf-8")
    recovery = create_recovery_snapshot(
        source,
        recovery_dir=tmp_path / "recovery",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
    )
    prepared = prepare_research_migration_bundle(
        recovery.database_path,
        bundle_dir=tmp_path / "bundles",
        artifact_dir=artifacts,
        snapshot_id=snapshot_id,
        code_commit=_COMMIT,
        start_date=date(2026, 7, 14),
        end_date=date(2026, 7, 14),
    )
    return snapshot_id, prepared.bundle_path


def test_publish_bundle_rebuilds_catalog_and_never_touches_production(
    tmp_path: Path,
) -> None:
    snapshot_id, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    target.mkdir()
    production = target / "rquant.duckdb"
    connection = duckdb.connect(str(production))
    connection.execute("CREATE TABLE production_marker (value VARCHAR)")
    connection.execute("INSERT INTO production_marker VALUES ('untouched')")
    connection.close()
    production_hash = migration_module._file_sha256(production)

    first = publish_research_migration_bundle(bundle_path, target_data_dir=target)
    second = publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert first.status == "published"
    assert second.status == "unchanged"
    assert first.snapshot_id == snapshot_id
    assert migration_module._file_sha256(production) == production_hash
    assert len(list((target / "lake/minute").rglob("manifest.json"))) == 1
    assert len(list((target / "lake/auction").rglob("manifest.json"))) == 1
    assert len(list((target / "lake/snapshots").rglob("data.parquet"))) == 7
    assert (
        target
        / f"research_artifacts/snapshot_id={snapshot_id}/strategy_lab_runs/run.json"
    ).is_file()

    catalog = duckdb.connect(str(target / "research.duckdb"), read_only=True)
    try:
        assert catalog.execute(
            "SELECT dataset, COUNT(*), SUM(row_count) "
            "FROM research_partition GROUP BY dataset ORDER BY dataset"
        ).fetchall() == [("auction_bar", 1, 1), ("minute_bar", 1, 2)]
        assert catalog.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'daily_bar'"
        ).fetchone() == (0,)
    finally:
        catalog.close()

    marker = migration_module.ResearchAuthorityCandidate.model_validate_json(
        (target / "research-authority-candidate.json").read_text(encoding="utf-8")
    )
    assert marker.snapshot_id == snapshot_id
    assert marker.status == "candidate"
    assert marker.local_retention_min_trading_days == 10
    assert not list(target.rglob("*.tmp-*"))


def test_publish_bundle_acquires_global_publish_lock_first(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    observed: list[Path] = []

    def observe_lock(path: Path) -> AbstractContextManager[None]:
        observed.append(path)
        return exclusive_file_lock(path)

    monkeypatch.setattr(migration_module, "exclusive_file_lock", observe_lock)

    publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert observed[:2] == [
        target / "research-publish.lock",
        target / ".research-migration.lock",
    ]


def test_publish_semantically_scans_each_raw_partition_only_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    original_verify = migration_module._verify_partition_semantics
    calls = 0

    def count_verify(
        data_path: Path,
        manifest: migration_module.ResearchPartitionManifest,
    ) -> None:
        nonlocal calls
        calls += 1
        original_verify(data_path, manifest)

    monkeypatch.setattr(migration_module, "_verify_partition_semantics", count_verify)

    publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert calls == 2


def test_publish_writes_partition_version_before_visible_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    target.mkdir()
    original_publish = migration_module._publish_immutable_file

    def publish_with_order_assertion(
        source: Path,
        destination: Path,
        evidence: migration_module.MigrationFileEvidence,
    ) -> None:
        if destination.name == "manifest.json" and any(
            part in {"minute", "auction"} for part in destination.parts
        ):
            manifest = migration_module.ResearchPartitionManifest.model_validate_json(
                source.read_text(encoding="utf-8")
            )
            assert (target / "lake" / manifest.relative_path).is_file()
        original_publish(source, destination, evidence)

    monkeypatch.setattr(
        migration_module,
        "_publish_immutable_file",
        publish_with_order_assertion,
    )

    published = publish_research_migration_bundle(
        bundle_path,
        target_data_dir=target,
    )

    assert published.status == "published"


def test_publish_bundle_resumes_after_crash_before_candidate_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot_id, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    target.mkdir()
    original_replace = migration_module.os.replace

    def fail_candidate_marker(source_path: Path, target_path: Path) -> None:
        if Path(target_path).name == "research-authority-candidate.json":
            raise RuntimeError("simulated candidate marker crash")
        original_replace(source_path, target_path)

    monkeypatch.setattr(migration_module.os, "replace", fail_candidate_marker)
    with pytest.raises(RuntimeError, match="candidate marker crash"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert not (target / "research-authority-candidate.json").exists()
    assert len(list((target / "lake").rglob("*.parquet"))) == 9
    assert (target / "research.duckdb").is_file()

    monkeypatch.setattr(migration_module.os, "replace", original_replace)
    repaired = publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert repaired.status == "published"
    assert repaired.snapshot_id == snapshot_id
    assert (target / "research-authority-candidate.json").is_file()


def test_publish_bundle_resumes_after_crash_at_every_file_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot_id, bundle_path = _prepared_bundle(tmp_path / "source")
    original_publish = migration_module._publish_immutable_file
    manifest = migration_module._read_verified_bundle(bundle_path)
    file_count = len(
        migration_module._publication_files(
            bundle_path,
            manifest,
            tmp_path / "shape-only",
        )
    )

    for fail_at in range(1, file_count + 1):
        target = tmp_path / f"cloud-data-{fail_at}"
        target.mkdir()
        publish_count = 0

        def fail_at_boundary(
            source: Path,
            destination: Path,
            evidence: migration_module.MigrationFileEvidence,
            expected_fail_at: int = fail_at,
        ) -> None:
            nonlocal publish_count
            publish_count += 1
            if publish_count == expected_fail_at:
                raise RuntimeError("simulated mid-publication crash")
            original_publish(source, destination, evidence)

        monkeypatch.setattr(
            migration_module,
            "_publish_immutable_file",
            fail_at_boundary,
        )
        with pytest.raises(RuntimeError, match="mid-publication crash"):
            publish_research_migration_bundle(bundle_path, target_data_dir=target)

        assert not (target / "research-authority-candidate.json").exists()
        assert (target / "research_migrations").is_dir()

        monkeypatch.setattr(
            migration_module,
            "_publish_immutable_file",
            original_publish,
        )
        repaired = publish_research_migration_bundle(bundle_path, target_data_dir=target)

        assert repaired.status == "published"
        assert repaired.snapshot_id == snapshot_id


def test_publish_bundle_removes_owned_stale_temp_after_hard_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot_id, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    target.mkdir()
    original_publish = migration_module._publish_immutable_file
    stale_temp: Path | None = None

    def leave_temp_and_die(
        source: Path,
        destination: Path,
        evidence: migration_module.MigrationFileEvidence,
    ) -> None:
        nonlocal stale_temp
        stale_temp = destination.with_name(f".{destination.name}.tmp-{'d' * 32}")
        stale_temp.parent.mkdir(parents=True, exist_ok=True)
        stale_temp.write_bytes(source.read_bytes()[:16])
        raise RuntimeError("simulated hard crash")

    monkeypatch.setattr(migration_module, "_publish_immutable_file", leave_temp_and_die)
    with pytest.raises(RuntimeError, match="hard crash"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert stale_temp is not None and stale_temp.is_file()
    monkeypatch.setattr(
        migration_module,
        "_publish_immutable_file",
        original_publish,
    )
    repaired = publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert repaired.status == "published"
    assert repaired.snapshot_id == snapshot_id
    assert not stale_temp.exists()


def test_publish_bundle_preserves_active_research_export_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    target.mkdir()
    original_publish = migration_module._publish_immutable_file

    def fail_after_state(
        source: Path,
        destination: Path,
        evidence: migration_module.MigrationFileEvidence,
    ) -> None:
        raise RuntimeError("simulated publication crash")

    monkeypatch.setattr(migration_module, "_publish_immutable_file", fail_after_state)
    with pytest.raises(RuntimeError, match="publication crash"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)
    monkeypatch.setattr(
        migration_module,
        "_publish_immutable_file",
        original_publish,
    )

    migration_manifest = migration_module._read_verified_bundle(bundle_path)
    publication_files = migration_module._publication_files(
        bundle_path,
        migration_manifest,
        target,
    )
    manifest_target = next(
        destination
        for _, destination, _ in publication_files
        if destination.name == "manifest.json"
    )
    active_temp = manifest_target.with_name(f".manifest.json.tmp-{'e' * 32}")
    active_temp.parent.mkdir(parents=True, exist_ok=True)
    active_temp.write_text("active export", encoding="utf-8")

    with exclusive_file_lock(manifest_target.parent / ".export.lock"):
        with pytest.raises(RuntimeError, match="active research export"):
            publish_research_migration_bundle(bundle_path, target_data_dir=target)
        assert active_temp.read_text(encoding="utf-8") == "active export"


def test_publish_bundle_rejects_symlink_export_lock_before_temp_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    target.mkdir()
    original_publish = migration_module._publish_immutable_file

    def fail_after_state(
        source: Path,
        destination: Path,
        evidence: migration_module.MigrationFileEvidence,
    ) -> None:
        raise RuntimeError("simulated publication crash")

    monkeypatch.setattr(migration_module, "_publish_immutable_file", fail_after_state)
    with pytest.raises(RuntimeError, match="publication crash"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)
    monkeypatch.setattr(
        migration_module,
        "_publish_immutable_file",
        original_publish,
    )

    migration_manifest = migration_module._read_verified_bundle(bundle_path)
    publication_files = migration_module._publication_files(
        bundle_path,
        migration_manifest,
        target,
    )
    manifest_target = next(
        destination
        for _, destination, _ in publication_files
        if destination.name == "manifest.json"
    )
    stale_temp = manifest_target.with_name(f".manifest.json.tmp-{'f' * 32}")
    stale_temp.parent.mkdir(parents=True, exist_ok=True)
    stale_temp.write_text("preserve me", encoding="utf-8")
    external_lock = tmp_path / "external-export.lock"
    external_lock.touch()
    (manifest_target.parent / ".export.lock").symlink_to(external_lock)

    with pytest.raises(RuntimeError, match="invalid research export lock"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert stale_temp.read_text(encoding="utf-8") == "preserve me"


def test_publish_bundle_refuses_conflicting_existing_partition(tmp_path: Path) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    conflict = (
        target
        / "lake/minute/freq=1min/year=2026/month=07/trade_date=2026-07-14/manifest.json"
    )
    conflict.parent.mkdir(parents=True)
    conflict.write_text("{}", encoding="utf-8")

    with pytest.raises(RuntimeError, match="conflicting research target file"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert conflict.read_text(encoding="utf-8") == "{}"
    assert not (target / "research-authority-candidate.json").exists()


def test_publish_bundle_refuses_unowned_existing_research_catalog(
    tmp_path: Path,
) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    target.mkdir()
    catalog = target / "research.duckdb"
    connection = duckdb.connect(str(catalog))
    connection.execute("CREATE TABLE owner_marker (value VARCHAR)")
    connection.execute("INSERT INTO owner_marker VALUES ('pre-existing')")
    connection.close()
    original_hash = migration_module._file_sha256(catalog)

    with pytest.raises(RuntimeError, match="not owned by this migration"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert migration_module._file_sha256(catalog) == original_hash
    assert not (target / "research-authority-candidate.json").exists()


def test_publish_bundle_refuses_existing_partitions_outside_bundle(tmp_path: Path) -> None:
    _, bundle_path = _prepared_bundle(tmp_path / "expected")
    extra_manifest_path = next((bundle_path / "lake/minute").rglob("manifest.json"))
    target = tmp_path / "cloud-data"
    extra_target = (
        target
        / "lake/minute/freq=1min/year=2026/month=07/trade_date=2026-07-15"
    )
    extra_target.mkdir(parents=True)
    (extra_target / "manifest.json").write_text(
        extra_manifest_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="outside migration bundle"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert not (target / "research-authority-candidate.json").exists()


def test_publish_bundle_refuses_orphan_raw_partition_files(tmp_path: Path) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    orphan = (
        target
        / "lake/minute/freq=1min/year=2026/month=07/"
        "trade_date=2026-07-15/versions/orphan.parquet"
    )
    orphan.parent.mkdir(parents=True)
    orphan.write_bytes(b"orphan")

    with pytest.raises(RuntimeError, match="outside migration bundle"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)

    assert not (target / "research-authority-candidate.json").exists()


def test_publish_bundle_rejects_catalog_changed_after_candidate(tmp_path: Path) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    publish_research_migration_bundle(bundle_path, target_data_dir=target)
    catalog = duckdb.connect(str(target / "research.duckdb"))
    catalog.execute("CREATE TABLE tampered (value INTEGER)")
    catalog.close()

    with pytest.raises(RuntimeError, match="candidate catalog hash mismatch"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)


def test_publish_bundle_rejects_candidate_evidence_tampering(tmp_path: Path) -> None:
    _, bundle_path = _prepared_bundle(tmp_path)
    target = tmp_path / "cloud-data"
    publish_research_migration_bundle(bundle_path, target_data_dir=target)
    candidate_path = target / "research-authority-candidate.json"
    candidate = migration_module.ResearchAuthorityCandidate.model_validate_json(
        candidate_path.read_text(encoding="utf-8")
    )
    candidate_path.write_text(
        candidate.model_copy(
            update={"source_snapshot_sha256": "b" * 64}
        ).model_dump_json(indent=2)
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="candidate evidence mismatch"):
        publish_research_migration_bundle(bundle_path, target_data_dir=target)
