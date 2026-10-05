"""A report is only a bounded, unconfirmed Serving observation."""

from __future__ import annotations

import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.data_audit_evidence import DailyBarNullFieldSpec
from rquant.data_audit_report import (
    AuditReportSource,
    build_data_audit_report,
    publish_data_audit_report,
)
from rquant.runtime_builder_authority import LabJobsPublisherSettings
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    LabPageProjectionSnapshot,
    PageProjectionSourceIntegrityError,
)
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.schema import DAILY_BAR_DDL, TRADE_CALENDAR_DDL
from tests.unit.test_data_audit_report import END, START, _database, _report

# Real inode/directory ctime cannot be backdated with os.utime.
OBSERVED = datetime.now(UTC) + timedelta(minutes=5)
REPORT_TABLES = {
    "audit_report_overview",
    "audit_report_month",
    "audit_report_rule",
    "audit_report_issue",
}


def _production_report_file(tmp_path: Path) -> Path:
    report = _report(
        _database(tmp_path / "daily.duckdb"),
        source=AuditReportSource(
            mode="production_unverified",
            namespace="production",
            snapshot_label="source-label-does-not-prove-replica",
        ),
    )
    path = publish_data_audit_report(report, tmp_path / "reports")
    os.utime(path, (OBSERVED.timestamp() - 60, OBSERVED.timestamp() - 60))
    return path


def _source(tmp_path: Path, *, report_path: Path | None = None) -> DuckDBLabPageProjectionSource:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    return DuckDBLabPageProjectionSource(database, audit_report_path=report_path)


def test_configured_report_projects_one_consistent_unconfirmed_generation(tmp_path: Path) -> None:
    path = _production_report_file(tmp_path)
    source = _source(tmp_path, report_path=path)

    snapshot = source(OBSERVED)

    projections = {item.table_name: item for item in snapshot.projections}
    assert projections.keys() >= REPORT_TABLES
    overview = projections["audit_report_overview"].rows[0]
    assert overview["report_hash"] == path.stem.removeprefix("data-audit-v1-")
    assert overview["schema_version"] == 1
    assert overview["run_status"] == "completed"
    assert overview["collection_status"] == "collection_unconfirmed"
    assert overview["collection_completed_through"] is None
    assert overview["coverage_conclusion"] == "unconfirmed"
    assert overview["current"] is False
    assert overview["source_mode"] == "production_unverified"
    assert overview["replica_generation_id"] is None
    assert overview["audit_start"] == START.isoformat()
    assert overview["observed_through"] == END.isoformat()
    assert overview["expected_open_days"] == 3
    assert overview["covered_open_days"] == 2
    assert overview["missing_open_days"] == 1
    assert overview["quality_issue_count"] == 1
    assert overview["indexed_issue_count"] == 1
    assert overview["omitted_issue_count"] == 0
    assert overview["unassessed_rule_days"] > 0
    month = projections["audit_report_month"].rows
    assert len(month) == 1
    assert month[0]["month"] == "2026-09-01"
    assert month[0]["expected_open_days"] == 3
    assert month[0]["covered_open_days"] == 2
    rules = projections["audit_report_rule"].rows
    assert len(rules) == 3
    assert any(
        row["rule_id"] == "daily_bar.close_limit" and row["assessed_days"] == 0 for row in rules
    )
    issues = projections["audit_report_issue"].rows
    assert len(issues) == 1
    assert issues[0]["trade_date"] == START.isoformat()
    assert issues[0]["rule_id"] == "daily_bar.field_null_ratio"
    assert all(
        row["report_hash"] == overview["report_hash"]
        for table in REPORT_TABLES - {"audit_report_overview"}
        for row in projections[table].rows
    )
    assert source(OBSERVED).content_sha256 == snapshot.content_sha256
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=OBSERVED,
            projections=tuple(
                ServingProjectionInput.bind(
                    item, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
                )
                for item in snapshot.projections
            ),
        )
    )
    assert len(tables["audit_report_month"]) == 1
    assert len(tables["audit_report_rule"]) == 3
    assert len(tables["audit_report_issue"]) == 1
    status = tables["projection_status"].set_index("table_name")
    assert all(bool(status.loc[name, "available"]) for name in REPORT_TABLES)


def test_unconfigured_or_missing_report_keeps_all_new_tables_unpublished(tmp_path: Path) -> None:
    source = _source(tmp_path)
    absent = source(OBSERVED)
    missing = DuckDBLabPageProjectionSource(
        source.database_path, audit_report_path=tmp_path / "reports" / "missing.json"
    )(OBSERVED)

    assert REPORT_TABLES.isdisjoint(item.table_name for item in absent.projections)
    assert REPORT_TABLES.isdisjoint(item.table_name for item in missing.projections)


def test_missing_report_during_directory_replacement_is_not_unpublished(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import serving_page_projection_source as page_source

    source = _source(tmp_path)
    reports = tmp_path / "reports"
    reports.mkdir()
    source = DuckDBLabPageProjectionSource(
        source.database_path, audit_report_path=reports / "missing.json"
    )
    original_read = page_source._read_bound_optional_file

    def rotate_after_missing(*args: object, **kwargs: object) -> None:
        assert original_read(*args, **kwargs) is None
        os.replace(reports, tmp_path / "old-reports")
        reports.mkdir()

    monkeypatch.setattr(page_source, "_read_bound_optional_file", rotate_after_missing)
    with pytest.raises(PageProjectionSourceIntegrityError, match="rotated"):
        source(OBSERVED)


def test_production_source_rejects_synthetic_report_even_with_matching_label(
    tmp_path: Path,
) -> None:
    report = _report(_database(tmp_path / "daily.duckdb"))
    path = publish_data_audit_report(report, tmp_path / "reports")
    os.utime(path, (OBSERVED.timestamp() - 60, OBSERVED.timestamp() - 60))
    source = _source(tmp_path, report_path=path)

    with pytest.raises(PageProjectionSourceIntegrityError, match="synthetic|test"):
        source(OBSERVED)


def test_corrupt_or_symlinked_report_rejects_whole_new_generation(tmp_path: Path) -> None:
    path = _production_report_file(tmp_path)
    source = _source(tmp_path, report_path=path)
    original = path.read_bytes()
    path.chmod(0o600)
    path.write_bytes(original.replace(b"collection_unconfirmed", b"completed", 1))

    with pytest.raises(PageProjectionSourceIntegrityError, match="invalid|digest|canonical"):
        source(OBSERVED)

    path.unlink()
    other = tmp_path / "other.json"
    other.write_bytes(original)
    path.symlink_to(other)
    with pytest.raises(PageProjectionSourceIntegrityError, match="symlink"):
        source(OBSERVED)


def test_report_replacement_during_projection_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import serving_page_projection_source as page_source

    path = _production_report_file(tmp_path)
    source = _source(tmp_path, report_path=path)
    original_read = page_source._read_bound_optional_file

    def replaced_after_read(*args: object, **kwargs: object):
        result = original_read(*args, **kwargs)
        assert result is not None
        copy = tmp_path / "replacement.json"
        copy.write_bytes(path.read_bytes())
        os.replace(copy, path)
        return result

    monkeypatch.setattr(page_source, "_read_bound_optional_file", replaced_after_read)
    with pytest.raises(PageProjectionSourceIntegrityError, match="rotated|changed"):
        source(OBSERVED)


def test_backdated_mtime_cannot_publish_report_into_past_query(tmp_path: Path) -> None:
    day = date(2026, 9, 14)
    database = tmp_path / "historical.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        connection.execute(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, TRUE, 'synthetic', ?)",
            (day, datetime(2026, 9, 14, 8, tzinfo=UTC)),
        )
        connection.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close, vol) "
            "VALUES ('600000.SH', ?, 10, 100)",
            (day,),
        )
    with duckdb.connect(str(database), read_only=True) as connection:
        report = build_data_audit_report(
            connection,
            source=AuditReportSource(
                mode="production_unverified",
                namespace="production",
                snapshot_label="historical-fixture",
            ),
            audit_start=day,
            observed_through=day,
            null_fields=(
                DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )
    path = publish_data_audit_report(report, tmp_path / "reports")
    backdated = datetime(2026, 9, 15, tzinfo=UTC)
    query = datetime(2026, 9, 20, tzinfo=UTC)
    os.utime(path, (backdated.timestamp(), backdated.timestamp()))
    assert path.stat().st_mtime < query.timestamp()
    assert path.stat().st_ctime > query.timestamp()

    source = _source(tmp_path, report_path=path)
    with pytest.raises(PageProjectionSourceIntegrityError, match="available|time|past"):
        source(query)


def test_snapshot_contract_cannot_promote_unconfirmed_report_to_healthy(tmp_path: Path) -> None:
    path = _production_report_file(tmp_path)
    report = _source(tmp_path, report_path=path)(OBSERVED)
    new_tables = tuple(item for item in report.projections if item.table_name in REPORT_TABLES)
    overview = next(item for item in new_tables if item.table_name == "audit_report_overview")
    promoted = ServingProjectionPayload(
        table_name=overview.table_name,
        available_at=overview.available_at,
        rows=({**dict(overview.rows[0]), "collection_status": "completed"},),
    )
    with pytest.raises(ValueError, match="unconfirmed|current"):
        LabPageProjectionSnapshot.create(
            available_at=OBSERVED,
            audit_report_projections=tuple(
                promoted if item.table_name == "audit_report_overview" else item
                for item in new_tables
            ),
        )


def test_report_path_requires_research_page_source(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="research_metadata_path"):
        LabJobsPublisherSettings(
            lab_jobs_path=tmp_path / "jobs.sqlite3",
            authority_root=tmp_path / "authority",
            audit_report_path=tmp_path / "report.json",
        )
