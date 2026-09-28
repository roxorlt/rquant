"""A bounded offline report must preserve what the fixed replica actually proves."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pytest

from rquant.storage.schema import DAILY_BAR_DDL, TRADE_CALENDAR_DDL

if TYPE_CHECKING:
    from rquant.data_audit_report import AuditReportSource, DataAuditReport

START = date(2026, 9, 25)
END = date(2026, 9, 29)


def _database(path: Path) -> Path:
    with duckdb.connect(str(path)) as connection:
        connection.execute(TRADE_CALENDAR_DDL)
        connection.execute(DAILY_BAR_DDL)
        connection.executemany(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, ?, 'synthetic', ?)",
            [
                (
                    day,
                    day.weekday() < 5,
                    datetime(2026, 9, 30, tzinfo=UTC),
                )
                for offset in range((END - START).days + 1)
                for day in [START + timedelta(days=offset)]
            ],
        )
        connection.executemany(
            "INSERT INTO daily_bar (ts_code, trade_date, close, vol) VALUES (?, ?, ?, ?)",
            [
                ("600000.SH", START, None, 100.0),
                ("600000.SH", END, 10.0, 100.0),
            ],
        )
    return path


def _source(*, replica: str = "test:replica-a") -> AuditReportSource:
    from rquant.data_audit_report import AuditReportSource

    return AuditReportSource(
        mode="synthetic_test",
        namespace="test",
        snapshot_label="fixture-snapshot",
        replica_generation_id=replica,
    )


def _report(path: Path, *, source: AuditReportSource | None = None) -> DataAuditReport:
    from rquant.data_audit_evidence import DailyBarNullFieldSpec
    from rquant.data_audit_report import build_data_audit_report

    with duckdb.connect(str(path), read_only=True) as connection:
        return build_data_audit_report(
            connection,
            source=_source() if source is None else source,
            audit_start=START,
            observed_through=END,
            null_fields=(
                DailyBarNullFieldSpec(
                    field_name="close", max_null_numerator=0, max_null_denominator=1
                ),
            ),
        )


def test_report_uses_one_connection_and_records_each_rule_day(tmp_path: Path) -> None:
    path = _database(tmp_path / "audit.duckdb")

    report = _report(path)

    assert report.schema_version == 1
    assert report.run_status == "completed"
    assert report.collection_status == "collection_unconfirmed"
    assert report.collection_completed_through is None
    assert report.coverage_conclusion == "unconfirmed"
    assert report.source.mode == "synthetic_test"
    assert report.source.replica_generation_id == "test:replica-a"
    assert report.coverage.observed_through == END
    assert not hasattr(report.coverage, "completed_through")
    assert report.coverage.monthly[0].expected_open_days == 3
    assert report.coverage.monthly[0].covered_open_days == 2
    assert [(gap.start, gap.end, gap.missing_open_days) for gap in report.coverage.gaps] == [
        (date(2026, 9, 28), date(2026, 9, 28), 1)
    ]
    rules = {(rule.rule_id, rule.field_name): rule for rule in report.quality_rules}
    expected = (START, date(2026, 9, 28), END)
    for rule in rules.values():
        assert rule.expected_days == expected
        assert rule.checked_days == (START, END)
        assert (date(2026, 9, 28), "no_daily_bar", 0) in {
            (item.day, item.reason, item.affected_rows) for item in rule.unassessed
        }
    assert rules[("daily_bar.close_limit", None)].assessed_days == ()
    assert rules[("daily_bar.zero_volume", None)].assessed_days == (START, END)
    assert rules[("daily_bar.field_null_ratio", "close")].assessed_days == (START, END)
    assert any(
        item.rule_id == "daily_bar.field_null_ratio" and item.trade_date == START
        for item in report.issues
    )
    assert report.quality_conclusion == "not_fully_assessed"


def test_identical_evidence_has_stable_digest_and_source_changes_it(tmp_path: Path) -> None:
    path = _database(tmp_path / "digest.duckdb")

    first = _report(path)
    same = _report(path)
    other = _report(path, source=_source(replica="test:replica-b"))

    assert first.content_hash == same.content_hash
    assert first.content_hash != other.content_hash


def test_unverified_production_cannot_claim_completion_or_test_identity(tmp_path: Path) -> None:
    from rquant.data_audit_report import AuditReportSource

    path = _database(tmp_path / "production-unverified.duckdb")
    source = AuditReportSource(
        mode="production_unverified", namespace="production", snapshot_label="unverified"
    )

    report = _report(path, source=source)

    assert report.source.replica_generation_id is None
    assert report.collection_status == "collection_unconfirmed"
    assert report.collection_completed_through is None
    with pytest.raises(ValueError):
        AuditReportSource(
            mode="synthetic_test",
            namespace="production",
            snapshot_label="x",
            replica_generation_id="test:replica-a",
        )
    with pytest.raises(ValueError):
        AuditReportSource(
            mode="production_unverified",
            namespace="production",
            snapshot_label="x",
            replica_generation_id="replica-a",
        )
    with pytest.raises(ValueError):
        AuditReportSource(
            mode="synthetic_test",
            namespace="test",
            snapshot_label="x",
            replica_generation_id="production-looking-id",
        )
    with pytest.raises(ValueError):
        AuditReportSource(
            mode="synthetic_test",
            namespace="test",
            snapshot_label="x",
            replica_generation_id="test:",
        )


def test_published_report_is_immutable_and_strictly_read(tmp_path: Path) -> None:
    from rquant.data_audit_report import (
        DataAuditReport,
        load_data_audit_report,
        publish_data_audit_report,
    )

    report = _report(_database(tmp_path / "publish.duckdb"))
    directory = tmp_path / "reports"

    published = publish_data_audit_report(report, directory)

    assert published.name == f"data-audit-v1-{report.content_hash}.json"
    assert publish_data_audit_report(report, directory) == published
    assert load_data_audit_report(published) == report
    raw = published.read_bytes()
    published.chmod(0o600)
    published.write_bytes(raw.replace(b"collection_unconfirmed", b"completed", 1))
    with pytest.raises(ValueError, match="canonical|digest|hash|invalid"):
        load_data_audit_report(published)
    with pytest.raises(ValueError, match="existing|invalid|canonical|digest|hash"):
        publish_data_audit_report(report, directory)
    assert DataAuditReport.model_validate_json(raw) == report


def test_interrupted_publish_does_not_replace_prior_report(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import data_audit_report as artifact

    path = _database(tmp_path / "interrupted.duckdb")
    previous = _report(path)
    current = _report(path, source=_source(replica="test:replica-b"))
    directory = tmp_path / "reports"
    previous_path = artifact.publish_data_audit_report(previous, directory)
    before = previous_path.read_bytes()

    def _fail_link(_source: str, _target: str) -> None:
        raise OSError("simulated interruption before publication")

    monkeypatch.setattr(artifact.os, "link", _fail_link)
    with pytest.raises(OSError, match="simulated interruption"):
        artifact.publish_data_audit_report(current, directory)

    assert previous_path.read_bytes() == before
    assert sorted(item.name for item in directory.iterdir()) == [previous_path.name]


def test_report_rejects_duplicate_dates_and_digest_mismatch(tmp_path: Path) -> None:
    from rquant.data_audit_report import DataAuditReport

    report = _report(_database(tmp_path / "contract.duckdb"))
    payload = report.model_dump(mode="json")
    first_rule = payload["quality_rules"][0]
    first_rule["expected_days"].insert(1, first_rule["expected_days"][0])
    with pytest.raises(ValueError, match="unique|increasing|digest|hash"):
        DataAuditReport.model_validate(payload)
    payload = report.model_dump(mode="json")
    payload["content_hash"] = "0" * 64
    with pytest.raises(ValueError, match="digest|hash"):
        DataAuditReport.model_validate(payload)


def test_report_requires_readonly_connection_and_complete_calendar(tmp_path: Path) -> None:
    from rquant.data_audit_evidence import DailyBarNullFieldSpec
    from rquant.data_audit_report import build_data_audit_report

    path = _database(tmp_path / "source.duckdb")
    args = {
        "source": _source(),
        "audit_start": START,
        "observed_through": END,
        "null_fields": (
            DailyBarNullFieldSpec(field_name="close", max_null_numerator=0, max_null_denominator=1),
        ),
    }
    with duckdb.connect(str(path)) as connection, pytest.raises(ValueError, match="read-only"):
        build_data_audit_report(connection, **args)
    with duckdb.connect(str(path)) as connection:
        connection.execute("DELETE FROM trade_calendar WHERE cal_date = ?", (date(2026, 9, 27),))
    with (
        duckdb.connect(str(path), read_only=True) as connection,
        pytest.raises(ValueError, match="calendar"),
    ):
        build_data_audit_report(connection, **args)


def test_maximum_date_range_may_span_122_calendar_months() -> None:
    from rquant.data_audit_coverage import MonthlyCoverage
    from rquant.data_audit_report import CoverageFacts

    start = date(2020, 1, 26)
    end = start + timedelta(days=3659)
    months = tuple(
        MonthlyCoverage(
            month=date(2020 + index // 12, index % 12 + 1, 1),
            expected_open_days=0,
            covered_open_days=0,
            coverage_ratio=None,
            status="no_expected_sessions",
        )
        for index in range(122)
    )

    facts = CoverageFacts(
        snapshot_label="test-range",
        exchange="SSE",
        audit_start=start,
        observed_through=end,
        monthly=months,
        gaps=(),
        closed_day_rows=(),
    )

    assert facts.monthly[-1].month == date(2030, 2, 1)
