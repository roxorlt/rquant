"""Independent small evidence for catalog audits, without production data."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import duckdb
import pytest

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import AuditReportSource, build_data_audit_report
from rquant.data_catalog.build import CATALOG_CONTRACTS
from rquant.storage.duckdb import initialize_schema

TZ = ZoneInfo("Asia/Shanghai")
START = date(2026, 1, 30)
END = date(2026, 2, 3)
AS_OF = datetime(2026, 2, 4, tzinfo=TZ)


def _source() -> AuditReportSource:
    return AuditReportSource(
        mode="synthetic_test",
        namespace="test",
        snapshot_label="fixed-small-source",
        replica_generation_id="test:catalog-audit",
    )


def _database(path: Path, *, all_tables: bool = True) -> Path:
    with duckdb.connect(str(path)) as conn:
        if all_tables:
            initialize_schema(conn)
        else:
            conn.execute(
                "CREATE TABLE daily_bar(ts_code VARCHAR,trade_date DATE,close DOUBLE,vol DOUBLE)"
            )
            conn.execute(
                "CREATE TABLE trade_calendar(exchange VARCHAR,cal_date DATE,is_open BOOL,"
                "source VARCHAR,updated_at TIMESTAMP)"
            )
        conn.executemany(
            "INSERT INTO trade_calendar(exchange,cal_date,is_open,source,updated_at) "
            "VALUES('SSE',?,?,'synthetic','2026-02-04')",
            [(START + timedelta(days=i), i in (0, 3, 4)) for i in range(5)],
        )
        conn.executemany(
            "INSERT INTO daily_bar(ts_code,trade_date,close,vol) VALUES(?,?,?,?)",
            [
                ("A", START, 10.0, 100.0),
                ("B", START, None, 0.0),
                ("A", date(2026, 1, 31), 8.0, 10.0),
            ],
        )
    return path


def _audit(path: Path, *, as_of: datetime = AS_OF):
    from rquant.data_audit_dataset_evidence import read_catalog_audit_from_connection

    with duckdb.connect(str(path), read_only=True) as conn:
        return read_catalog_audit_from_connection(
            conn,
            source_id="test:catalog-audit",
            audit_start=START,
            observed_through=END,
            as_of=as_of,
        )


def test_catalog_has_all_contracts_and_never_calls_empty_results_healthy(tmp_path: Path) -> None:
    results = _audit(_database(tmp_path / "catalog.duckdb"))
    assert tuple(r.dataset_id for r in results) == tuple(
        sorted(c.dataset_id for c in CATALOG_CONTRACTS)
    )
    assert len(results) == 24
    by_id = {r.dataset_id: r for r in results}
    assert by_id["adj_factor"].observed_rows == 0
    assert by_id["adj_factor"].conclusion == "not_fully_assessed"
    assert by_id["ths_member"].coverage_state == "not_applicable"
    assert by_id["ths_member"].freshness_state == "not_evaluated"
    assert by_id["stock_suspend_event"].coverage_state == "not_applicable"
    assert by_id["dc_member"].coverage_state == "not_applicable"


def test_daily_reuses_calendar_presence_and_preserves_cross_month_gap(tmp_path: Path) -> None:
    results = _audit(_database(tmp_path / "daily.duckdb"))
    daily = next(r for r in results if r.dataset_id == "daily_bar")
    assert daily.coverage_state == "measured"
    assert daily.completeness_state == "missing_expected_scope"
    assert (daily.expected_open_days, daily.covered_open_days) == (3, 1)
    assert [(m.month, m.expected_open_days, m.covered_open_days) for m in daily.monthly] == [
        (date(2026, 1, 1), 1, 1),
        (date(2026, 2, 1), 2, 0),
    ]
    assert [(g.start, g.end, g.missing_open_days) for g in daily.gaps] == [
        (date(2026, 2, 2), END, 2)
    ]
    assert [(d.day, d.row_count) for d in daily.closed_day_rows] == [(date(2026, 1, 31), 1)]
    assert next(f for f in daily.fields if f.field_name == "close").null_rows == 1
    assert [(r.day, r.row_count, r.previous_rows, r.change_rows) for r in daily.row_changes] == [
        (START, 2, None, None),
        (date(2026, 2, 2), 0, 2, -2),
        (END, 0, 0, 0),
    ]
    assert daily.freshness_lag_sessions == 2
    assert daily.freshness_state == "delayed"


def test_minute_and_auction_preserve_frequency_visibility_and_late_observation(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path / "intraday.duckdb")
    with duckdb.connect(str(path)) as conn:
        conn.executemany(
            "INSERT INTO minute_bar(ts_code,trade_time,freq,source,close,created_at) "
            "VALUES(?,?,?,?,?,?)",
            [
                (
                    "A",
                    datetime(2026, 2, 3, 9, 30),
                    "1min",
                    "tushare",
                    None,
                    datetime(2026, 2, 3, 9, 34),
                ),
                (
                    "A",
                    datetime(2026, 2, 3, 9, 35),
                    "5min",
                    "tushare",
                    1.0,
                    datetime(2026, 2, 3, 9, 35),
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO auction_bar(ts_code,trade_date,auction_type,source,price,created_at) "
            "VALUES(?,?,'open',?,?,?)",
            [
                ("A", END, "tushare", 10.0, datetime(2026, 2, 3, 9, 26)),
                ("A", END, "minute_0930_fallback", 10.0, datetime(2026, 2, 3, 9, 31)),
            ],
        )
    by_id = {r.dataset_id: r for r in _audit(path, as_of=datetime(2026, 2, 3, 9, 30, tzinfo=TZ))}
    minute = by_id["minute_bar"]
    assert (minute.observed_rows, minute.visible_rows, minute.pending_rows) == (2, 1, 1)
    assert minute.recorded_after_as_of_rows == 2
    assert [(f.frequency, f.row_count, f.visible_rows) for f in minute.frequencies] == [
        ("1min", 1, 1),
        ("5min", 1, 0),
    ]
    assert minute.completeness_state == "missing_expected_scope"
    assert minute.freshness_state == "missing_expected_scope"
    auction = by_id["auction_bar"]
    assert (auction.visible_rows, auction.pending_rows) == (1, 1)
    assert auction.recorded_after_as_of_rows == 1
    assert END not in [r.day for r in minute.row_changes]
    assert END not in [r.day for r in auction.row_changes]


def test_missing_source_and_schema_mismatch_are_distinct(tmp_path: Path) -> None:
    path = _database(tmp_path / "missing.duckdb", all_tables=False)
    with duckdb.connect(str(path)) as conn:
        conn.execute("CREATE TABLE adj_factor(ts_code VARCHAR)")
    by_id = {r.dataset_id: r for r in _audit(path)}
    assert by_id["minute_bar"].source_state == "missing_source"
    assert by_id["adj_factor"].source_state == "schema_mismatch"
    assert by_id["adj_factor"].coverage_state == "not_evaluated"
    assert by_id["minute_bar"].coverage_state == "missing_source"


def test_wrong_source_and_frequency_are_checked_without_exporting_rows(tmp_path: Path) -> None:
    path = _database(tmp_path / "unknown.duckdb")
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            "INSERT INTO minute_bar(ts_code,trade_time,freq,source) VALUES"
            "('A','2026-02-03 09:30','2min','unknown')"
        )
    minute = next(r for r in _audit(path) if r.dataset_id == "minute_bar")
    assert minute.unknown_source_rows == 1
    assert minute.unknown_frequency_rows == 1
    assert minute.conclusion == "issues_observed"
    assert '"A"' not in minute.model_dump_json()


def test_requires_readonly_complete_calendar_and_bounded_dates(tmp_path: Path) -> None:
    from rquant.data_audit_dataset_evidence import read_catalog_audit_from_connection

    path = _database(tmp_path / "guards.duckdb")
    with duckdb.connect(str(path)) as conn:
        with pytest.raises(ValueError, match="read-only"):
            read_catalog_audit_from_connection(
                conn, source_id="test:a", audit_start=START, observed_through=END, as_of=AS_OF
            )
        conn.execute("DELETE FROM trade_calendar WHERE cal_date='2026-02-01'")
    with pytest.raises(ValueError, match="calendar"):
        _audit(path)


def test_v2_keeps_v1_daily_result_and_roundtrips_both(tmp_path: Path) -> None:
    from rquant.data_audit_report import (
        build_catalog_data_audit_report,
        load_data_audit_report,
        publish_data_audit_report,
    )

    path = _database(tmp_path / "artifact.duckdb")
    kwargs = dict(
        source=_source(),
        audit_start=START,
        observed_through=END,
        null_fields=(
            DailyBarNullFieldSpec(field_name="close", max_null_numerator=0, max_null_denominator=1),
        ),
    )
    with duckdb.connect(str(path), read_only=True) as conn:
        old = build_data_audit_report(conn, **kwargs)
        new = build_catalog_data_audit_report(conn, as_of=AS_OF, **kwargs)
    assert old.schema_version == 1 and new.schema_version == 2
    assert old.rule_version == new.rule_version == "daily-bar-quality-v1"
    assert old.coverage == new.coverage
    assert old.quality_rules == new.quality_rules and old.issues == new.issues
    assert len(new.datasets) == 24
    assert new.dataset_rule_version == "catalog-dataset-audit-v1"
    assert len(new.dataset_contract_sha256) == 64
    assert new.content_hash != old.content_hash
    for report in (old, new):
        published = publish_data_audit_report(report, tmp_path / "reports")
        assert load_data_audit_report(published) == report
        assert publish_data_audit_report(report, tmp_path / "reports") == published


def test_v2_entry_reuses_one_source_and_retains_prior_report_after_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.data_audit_report as artifact

    primary = _database(tmp_path / "primary.duckdb")
    before = primary.read_bytes()
    replica = tmp_path / "replica.duckdb"
    replica.write_bytes(before)
    digest = hashlib.sha256(before).hexdigest()
    directory = tmp_path / "reports"
    kwargs = dict(
        primary_path=primary,
        replica_path=replica,
        audit_start=START,
        observed_through=END,
        null_fields=(
            DailyBarNullFieldSpec(field_name="close", max_null_numerator=0, max_null_denominator=1),
        ),
        directory=directory,
        include_catalog=True,
        expected_file_identity=artifact.capture_data_audit_replica_identity(primary, replica),
        expected_file_sha256=digest,
    )
    published = artifact.create_and_publish_data_audit_report(**kwargs)
    first = published.read_bytes()
    report = artifact.load_data_audit_report(published)
    assert report.schema_version == 2 and published.name.startswith("data-audit-v2-")
    assert report.audit_as_of == AS_OF
    assert {r.source_id for r in report.datasets} == {"sha256:" + digest}
    assert report.collection_status == "collection_unconfirmed"
    assert artifact.create_and_publish_data_audit_report(**kwargs) == published

    def fail_catalog(*args: object, **params: object) -> object:
        raise ValueError("catalog evidence is unavailable")

    monkeypatch.setattr(artifact, "build_catalog_data_audit_report", fail_catalog)
    with pytest.raises(ValueError, match="catalog evidence"):
        artifact.create_and_publish_data_audit_report(**kwargs)
    assert published.read_bytes() == first
    assert tuple(directory.iterdir()) == (published,)
    assert primary.read_bytes() == replica.read_bytes() == before
    assert not any(p.name.startswith(".audit-source-") for p in tmp_path.iterdir())


def test_cross_month_gap_and_pending_daily_are_not_filled_from_observed_population(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path / "cross-month.duckdb")
    with duckdb.connect(str(path)) as conn:
        conn.execute("DELETE FROM daily_bar WHERE trade_date='2026-01-30'")
        conn.execute(
            "INSERT INTO daily_bar(ts_code,trade_date,close,vol) VALUES('A','2026-02-03',1,1)"
        )
    daily = next(r for r in _audit(path) if r.dataset_id == "daily_bar")
    assert [(g.start, g.end, g.missing_open_days) for g in daily.gaps] == [
        (START, date(2026, 2, 2), 2)
    ]
    pending = next(
        r
        for r in _audit(path, as_of=datetime(2026, 2, 3, 16, tzinfo=TZ))
        if r.dataset_id == "daily_bar"
    )
    assert pending.pending_rows == 1
    assert pending.expected_open_days == 2 and pending.covered_open_days == 0
    assert pending.recorded_after_as_of_rows == 0
    assert next(r for r in pending.rules if r.rule_id == "known_sources").state == "not_evaluated"


def test_current_reference_counts_nulls_without_claiming_historical_or_pit_coverage(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path / "reference.duckdb")
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            "INSERT INTO ths_board_member(board_code,con_code,updated_at) "
            "VALUES('BOARD','MEMBER',NULL)"
        )
    result = next(r for r in _audit(path) if r.dataset_id == "ths_member")
    assert result.observed_rows == 1 and result.visible_rows == 0
    assert result.coverage_state == "not_applicable" and result.freshness_state == "not_evaluated"
    assert result.expected_open_days is None and result.row_changes == ()
    assert next(f for f in result.fields if f.field_name == "updated_at").null_rows == 1
    assert result.conclusion == "not_fully_assessed"


def test_stock_status_date_range_uses_trade_date_when_availability_is_null(tmp_path: Path) -> None:
    path = _database(tmp_path / "stock-status.duckdb")
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            "INSERT INTO stock_status_daily"
            "(ts_code,trade_date,name_source,available_at,ingested_at) "
            "VALUES ('OUT_BEFORE','2026-01-29','tushare',NULL,'2026-01-29T08:00:00Z'),"
            "('OUT_AFTER','2026-02-04','tushare',NULL,'2026-02-04T08:00:00Z'),"
            "('IN_UNKNOWN','2026-02-02','tushare',NULL,'2026-02-02T08:00:00Z'),"
            "('IN_VISIBLE','2026-02-03','tushare','2026-02-03T01:00:00Z','2026-02-03T08:00:00Z')"
        )
    result = next(r for r in _audit(path) if r.dataset_id == "stock_status_daily")
    assert (result.observed_rows, result.visible_rows, result.pending_rows) == (2, 1, 1)
    assert next(f for f in result.fields if f.field_name == "available_at").null_rows == 1
    assert result.latest_visible_date == END
    assert result.latest_visible_time == datetime(2026, 2, 3, 9)
    assert result.coverage_state == "not_applicable" and result.row_changes == ()


def test_projection_and_api_are_bound_to_the_same_report_with_legacy_optional(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.serving_read_models as serving
    import rquant.web.routes.data_audit_report as api
    from rquant.data_audit_report import build_catalog_data_audit_report
    from rquant.data_audit_report_projection import project_data_audit_report

    registry = dict(serving.PAGE_PROJECTION_CONTRACTS)
    registry["audit_report_dataset"] = serving.ServingProjectionContract(
        owner_dataset_id="lab_jobs",
        columns=(("report_hash", "string"), ("dataset_id", "string"), ("result_json", "string")),
        sort_keys=("report_hash", "dataset_id"),
        max_rows=24,
        max_bytes=2 * 1024 * 1024,
    )
    monkeypatch.setattr(serving, "PAGE_PROJECTION_CONTRACTS", registry)
    monkeypatch.setattr(api, "PAGE_PROJECTION_CONTRACTS", registry)
    path = _database(tmp_path / "api-source.duckdb")
    source = AuditReportSource(
        mode="production_unverified", namespace="production", snapshot_label="sha256:" + "a" * 64
    )
    with duckdb.connect(str(path), read_only=True) as conn:
        report = build_catalog_data_audit_report(
            conn,
            source=source,
            audit_start=START,
            observed_through=END,
            as_of=AS_OF,
            null_fields=(
                DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )
    available = datetime(2026, 2, 4, 1, tzinfo=ZoneInfo("UTC"))
    projections = project_data_audit_report(report, available_at=available)
    assert len(projections) == 5
    with duckdb.connect() as conn:
        conn.execute(
            "CREATE TABLE projection_status(table_name VARCHAR,available BOOL,row_count BIGINT,"
            "owner_dataset_id VARCHAR,owner_generation_id VARCHAR,available_at TIMESTAMPTZ)"
        )
        for projection in projections:
            contract = registry[projection.table_name]
            kinds = {
                "string": "VARCHAR",
                "date": "DATE",
                "timestamp": "TIMESTAMPTZ",
                "int": "BIGINT",
                "float": "DOUBLE",
                "bool": "BOOL",
            }
            conn.execute(
                f"CREATE TABLE {projection.table_name}("
                + ",".join(f"{n} {kinds[k]}" for n, k in contract.columns)
                + ")"
            )
            if projection.rows:
                conn.executemany(
                    f"INSERT INTO {projection.table_name} VALUES("
                    + ",".join("?" for _ in contract.columns)
                    + ")",
                    [list(r.values()) for r in projection.rows],
                )
            conn.execute(
                "INSERT INTO projection_status VALUES(?,TRUE,?,'lab_jobs','lab-a',?)",
                [projection.table_name, len(projection.rows), available],
            )
        borrowed = SimpleNamespace(
            cursor=conn,
            manifest=SimpleNamespace(
                row_counts={p.table_name: len(p.rows) for p in projections},
                built_at=available,
                watermarks=[SimpleNamespace(dataset_id="lab_jobs", generation_id="lab-a")],
            ),
        )
        data = api._snapshot(borrowed)
        assert data.dataset_state == "ready" and len(data.datasets) == 24
        assert data.datasets[0].name and data.datasets[0].coverage_label
        with pytest.raises(ValueError, match="rules"):
            api.AuditReportDataset.model_validate(
                {**data.datasets[0].model_dump(), "rules": data.datasets[0].rules[:-1]}
            )
        assert (
            next(d for d in data.datasets if d.dataset_id == "ths_member").conclusion_label
            == "尚未完整检查"
        )
        conn.execute(
            "UPDATE audit_report_dataset SET report_hash=? WHERE dataset_id='minute_bar'",
            ["b" * 64],
        )
        with pytest.raises(api.HTTPException) as error:
            api._snapshot(borrowed)
        assert error.value.status_code == 503


def test_named_lake_verifies_only_named_partitions_and_keeps_partial_scope(tmp_path: Path) -> None:
    from rquant.data_audit_dataset_evidence import read_audit_calendar, read_named_lake_audit
    from rquant.research_catalog import ResearchCatalog
    from rquant.research_lake import export_research_dataset

    path = _database(tmp_path / "lake-source.duckdb")
    lake = tmp_path / "lake"
    with duckdb.connect(str(path)) as conn:
        conn.execute(
            "INSERT INTO minute_bar(ts_code,trade_time,freq,source,created_at) VALUES"
            "('AUDIT_SYMBOL','2026-02-02 09:30','1min','tushare','2026-02-02 16:00'),"
            "('AUDIT_SYMBOL','2026-02-03 09:30','1min','tushare','2026-02-03 16:00')"
        )
        exported = export_research_dataset(
            conn,
            catalog=ResearchCatalog(tmp_path / "catalog.duckdb"),
            lake_root=lake,
            dataset="minute_bar",
            start_date=date(2026, 2, 2),
            end_date=END,
            code_commit="a" * 40,
            now=lambda: AS_OF,
            as_of_date=AS_OF.date(),
        )
    ignored, selected = (p.manifest for p in exported.partitions)
    assert ignored is not None and selected is not None
    ignored_path = lake / ignored.relative_path
    ignored_path.write_bytes(b"unselected corrupt partition must not be read")
    selected_path = lake / selected.relative_path
    before = selected_path.read_bytes()
    with duckdb.connect(str(path), read_only=True) as conn:
        calendar = read_audit_calendar(
            conn, source_id="test:named", audit_start=START, observed_through=END
        )
    with duckdb.connect(config={"threads": "1", "temp_directory": ""}) as conn:
        kwargs = dict(
            lake_root=lake,
            manifests=(selected,),
            source_id="test:named",
            calendar=calendar,
            audit_start=START,
            observed_through=END,
            as_of=AS_OF,
        )
        results = read_named_lake_audit(conn, **kwargs)
        assert len(results) == 1
        result = results[0]
        assert result.scope == "named_partitions" and result.source_kind == "named_lake"
        assert result.coverage_reason == "named_partitions_only"
        assert result.completeness_state == "missing_expected_scope"
        assert (result.observed_rows, result.expected_open_days, result.covered_open_days) == (
            1,
            3,
            1,
        )
        assert result.unknown_frequency_rows == result.unknown_source_rows == 0
        assert "AUDIT_SYMBOL" not in result.model_dump_json()
        assert selected_path.read_bytes() == before
        with pytest.raises(ValueError, match="unique and ordered"):
            read_named_lake_audit(conn, **{**kwargs, "manifests": (selected, selected)})
        with pytest.raises(ValueError, match="future data"):
            read_named_lake_audit(conn, **{**kwargs, "as_of": datetime(2026, 2, 3, 9, tzinfo=TZ)})
        selected_path.write_bytes(before + b"changed")
        with pytest.raises(ValueError, match="file size mismatch"):
            read_named_lake_audit(conn, **kwargs)


def test_fixed_input_driver_returns_independent_totals_and_cleans_private_directory(
    tmp_path: Path,
) -> None:
    path = _database(tmp_path / "driver-source.duckdb").resolve()
    source_bytes = path.read_bytes()
    stat = path.stat()
    inputs = {
        "schema_version": 1,
        "replica_path": str(path),
        "replica_identity": {
            "device": stat.st_dev,
            "inode": stat.st_ino,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
        },
        "audit_start": START.isoformat(),
        "observed_through": END.isoformat(),
        "as_of": AS_OF.isoformat(),
        "dataset_ids": [
            "daily_bar",
            "minute_bar",
            "auction_bar",
            "stock_status_daily",
            "ths_member",
        ],
    }
    input_path = tmp_path / "inputs.json"
    input_path.write_text(json.dumps(inputs))
    driver = (
        Path(__file__).resolve().parents[2]
        / "data/verification/data-audit-datasets-20261005/real_source_driver.py"
    )
    process = subprocess.run(
        [sys.executable, "-I", "-B", str(driver), str(input_path)],
        env=os.environ.copy(),
        capture_output=True,
        close_fds=True,
        timeout=30,
    )
    assert process.returncode == 0, process.stderr.decode()
    summary = json.loads(process.stdout)
    assert summary["independent_row_totals"] == {
        "daily_bar": 3,
        "minute_bar": 0,
        "auction_bar": 0,
        "stock_status_daily": 0,
        "ths_member": 0,
    }
    assert summary["source_id"].startswith("stat-sha256:")
    assert summary["verification_scope"] == "unchanged_file_identity_not_content_hash_or_completion"
    assert (
        summary["totals_match"]
        and summary["scratch_empty"]
        and summary["private_directory_removed"]
    )
    assert summary["identity_before"] == summary["identity_after"] == inputs["replica_identity"]
    assert b'"A"' not in process.stdout and b'"B"' not in process.stdout
    assert hashlib.sha256(path.read_bytes()).digest() == hashlib.sha256(source_bytes).digest()
    assert path.stat().st_mtime_ns == stat.st_mtime_ns
