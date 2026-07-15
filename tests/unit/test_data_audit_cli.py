"""CLI contract for persisted Stage-1 data audits."""

from __future__ import annotations

from argparse import Namespace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest

from rquant import cli
from rquant.data_quality import AuditFinding, AuditReport
from rquant.storage.duckdb import DuckDBStore


def test_data_audit_parser_requires_exact_as_of_date() -> None:
    parser = cli.build_parser()
    args = parser.parse_args(["data-audit", "--as-of", "2026-07-14"])

    assert args.as_of == date(2026, 7, 14)
    assert args.start_date is None
    with pytest.raises(SystemExit):
        parser.parse_args(["data-audit", "--as-of", "2026-7-14"])


def _wire_real_stores(monkeypatch: Any, db_path: Path) -> None:
    monkeypatch.setattr(
        cli,
        "DuckDBStore",
        lambda *_, **kwargs: DuckDBStore(
            db_path,
            read_only=bool(kwargs.get("read_only", False)),
        ),
    )
    monkeypatch.setattr(
        cli,
        "open_readonly_store",
        lambda: (_ for _ in ()).throw(
            AssertionError("data-audit must not read the lagging replica")
        ),
    )
    monkeypatch.setattr(
        "rquant.data_quality.build_stage1_audit_rules",
        lambda *_: (),
    )


def _args() -> Namespace:
    return Namespace(as_of=date(2026, 7, 14), start_date=date(2026, 4, 1))


def test_data_audit_persists_completed_clean_run(
    monkeypatch: Any,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    db_path = tmp_path / "audit.duckdb"
    DuckDBStore(db_path).close()
    _wire_real_stores(monkeypatch, db_path)
    report = AuditReport(
        observed_at=datetime(2026, 7, 15, tzinfo=UTC),
        rule_ids=("probe",),
    )
    monkeypatch.setattr("rquant.data_quality.run_audit", lambda *_args, **_kwargs: report)

    rc = cli.cmd_data_audit(_args())

    assert rc == 0
    with DuckDBStore(db_path, read_only=True) as store:
        run = store.latest_completed_data_audit_run(as_of_date=date(2026, 7, 14))
    assert run is not None
    assert run.p0_count == 0
    assert '"status":"completed"' in capsys.readouterr().out


def test_data_audit_returns_one_for_p0_finding(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "audit.duckdb"
    DuckDBStore(db_path).close()
    _wire_real_stores(monkeypatch, db_path)
    finding = AuditFinding(
        rule_id="future-data",
        dataset_id="minute_bar",
        severity="P0",
        scope_key="all",
        message="future rows",
    )
    report = AuditReport(
        observed_at=datetime(2026, 7, 15, tzinfo=UTC),
        rule_ids=("future-data",),
        findings=(finding,),
    )
    monkeypatch.setattr("rquant.data_quality.run_audit", lambda *_args, **_kwargs: report)

    rc = cli.cmd_data_audit(_args())

    assert rc == 1
    with DuckDBStore(db_path, read_only=True) as store:
        run = store.latest_completed_data_audit_run(as_of_date=date(2026, 7, 14))
        issues = store.list_open_data_quality_issues(severities=("P0",))
    assert run is not None and run.p0_count == 1
    assert len(issues) == 1


def test_data_audit_exception_persists_failed_evidence(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "audit.duckdb"
    DuckDBStore(db_path).close()
    _wire_real_stores(monkeypatch, db_path)

    def fail(*_: object, **__: object) -> AuditReport:
        raise RuntimeError("read failed")

    monkeypatch.setattr("rquant.data_quality.run_audit", fail)

    rc = cli.cmd_data_audit(_args())

    assert rc == 2
    with DuckDBStore(db_path, read_only=True) as store:
        row = store._conn.execute(  # noqa: SLF001
            "SELECT status, error_message FROM data_audit_run"
        ).fetchone()
    assert row is not None and row[0] == "failed"
    assert "read failed" in row[1]


def test_data_audit_only_resolves_issues_from_the_same_date_range(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "audit-scope.duckdb"
    DuckDBStore(db_path).close()
    _wire_real_stores(monkeypatch, db_path)
    observed_at = datetime(2026, 7, 14, tzinfo=UTC)
    same_range = AuditFinding(
        rule_id="stock-status-coverage",
        dataset_id="stock_status_daily",
        severity="P0",
        scope_key="missing/2026-04-01/2026-07-14",
        message="same range",
    ).to_issue(observed_at=observed_at)
    other_range = AuditFinding(
        rule_id="stock-status-coverage",
        dataset_id="stock_status_daily",
        severity="P0",
        scope_key="missing/2026-01-01/2026-03-31",
        message="other range",
    ).to_issue(observed_at=observed_at)
    with DuckDBStore(db_path) as store:
        store.record_data_quality_issue(same_range)
        store.record_data_quality_issue(other_range)
    monkeypatch.setattr(
        "rquant.data_quality.run_audit",
        lambda *_args, **_kwargs: AuditReport(
            observed_at=datetime(2026, 7, 15, tzinfo=UTC),
            rule_ids=("stock-status-coverage",),
        ),
    )

    assert cli.cmd_data_audit(_args()) == 0

    with DuckDBStore(db_path, read_only=True) as store:
        same = store.get_data_quality_issue(same_range.issue_id)
        other = store.get_data_quality_issue(other_range.issue_id)
    assert same is not None and same.status == "resolved"
    assert other is not None and other.status == "open"
