from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from rquant.data_quality import (
    AuditFinding,
    AuditReport,
    AuditRule,
    RepairAction,
    RepairReport,
    record_audit_report,
    resolve_audit_issues,
    run_audit,
    run_repair,
)
from rquant.storage.duckdb import DuckDBStore


def test_finding_reuses_metadata_stable_identity_and_marks_p0_blocking() -> None:
    finding = AuditFinding(
        rule_id="missing-trading-days",
        dataset_id="daily",
        severity="P0",
        scope_key="2026-07-13",
        message="daily data is missing",
        evidence={"missing_count": 1},
    )

    issue = finding.to_issue(observed_at=datetime(2026, 7, 14, tzinfo=UTC))

    assert finding.issue_id == issue.issue_id
    assert finding.is_blocking is True


def test_run_audit_is_read_only_and_returns_typed_report(tmp_path: Path) -> None:
    database_path = tmp_path / "quality.duckdb"
    with DuckDBStore(database_path):
        pass

    def find_missing_days(_store: DuckDBStore) -> tuple[AuditFinding, ...]:
        return (
            AuditFinding(
                rule_id="missing-trading-days",
                dataset_id="daily",
                severity="P0",
                scope_key="2026-07-13",
                message="daily data is missing",
            ),
        )

    rule = AuditRule(
        rule_id="missing-trading-days",
        dataset_id="daily",
        severity="P0",
        description="Detect missing daily bars",
        check=find_missing_days,
    )
    observed_at = datetime(2026, 7, 14, tzinfo=UTC)

    with DuckDBStore(database_path, read_only=True) as readonly_store:
        report = run_audit(readonly_store, (rule,), observed_at=observed_at)

    assert isinstance(report, AuditReport)
    assert report.observed_at == observed_at
    assert report.rule_ids == (rule.rule_id,)
    assert report.finding_count == 1
    assert report.is_blocked is True


def test_run_audit_rejects_writable_store_before_calling_checker(
    tmp_path: Path,
) -> None:
    checker_called = False

    def check(_store: DuckDBStore) -> tuple[AuditFinding, ...]:
        nonlocal checker_called
        checker_called = True
        return ()

    rule = AuditRule(
        rule_id="writable-store",
        dataset_id="daily",
        severity="P1",
        description="Must never receive a writable store",
        check=check,
    )

    with (
        DuckDBStore(tmp_path / "writable-audit.duckdb") as writable_store,
        pytest.raises(ValueError, match="read-only DuckDBStore"),
    ):
        run_audit(writable_store, (rule,))

    assert checker_called is False


def test_audit_report_rejects_duplicate_rule_ids() -> None:
    with pytest.raises(ValueError, match="duplicate rule_id"):
        AuditReport(
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
            rule_ids=("duplicate-rule", "duplicate-rule"),
        )


def test_audit_report_rejects_duplicate_finding_issue_ids() -> None:
    finding = AuditFinding(
        rule_id="duplicate-finding",
        dataset_id="daily",
        severity="P1",
        scope_key="2026-07-13",
        message="first observation",
    )
    conflicting = finding.model_copy(
        update={"severity": "P0", "message": "conflicting observation"}
    )

    with pytest.raises(ValueError, match="duplicate finding issue_id"):
        AuditReport(
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
            rule_ids=(finding.rule_id,),
            findings=(finding, conflicting),
        )


def test_audit_report_rejects_finding_for_unlisted_rule() -> None:
    finding = AuditFinding(
        rule_id="unlisted-rule",
        dataset_id="daily",
        severity="P1",
        scope_key="2026-07-13",
        message="finding does not belong to the report",
    )

    with pytest.raises(ValueError, match="finding rule_id is not in rule_ids"):
        AuditReport(
            observed_at=datetime(2026, 7, 14, tzinfo=UTC),
            rule_ids=("listed-rule",),
            findings=(finding,),
        )


def test_run_audit_rejects_duplicate_rule_ids(tmp_path: Path) -> None:
    database_path = tmp_path / "duplicate-rules.duckdb"
    with DuckDBStore(database_path):
        pass

    def check(_store: DuckDBStore) -> tuple[AuditFinding, ...]:
        return ()

    rules = tuple(
        AuditRule(
            rule_id="duplicate-rule",
            dataset_id="daily",
            severity="P1",
            description=description,
            check=check,
        )
        for description in ("First registration", "Second registration")
    )

    with (
        DuckDBStore(database_path, read_only=True) as readonly_store,
        pytest.raises(ValueError, match="duplicate rule_id"),
    ):
        run_audit(readonly_store, rules)


def test_run_audit_rejects_duplicate_finding_issue_ids(tmp_path: Path) -> None:
    database_path = tmp_path / "duplicate-findings.duckdb"
    with DuckDBStore(database_path):
        pass
    finding = AuditFinding(
        rule_id="duplicate-finding",
        dataset_id="daily",
        severity="P1",
        scope_key="2026-07-13",
        message="duplicate observation",
    )

    def check(_store: DuckDBStore) -> tuple[AuditFinding, ...]:
        return (finding, finding)

    rule = AuditRule(
        rule_id=finding.rule_id,
        dataset_id=finding.dataset_id,
        severity=finding.severity,
        description="Emit duplicate findings",
        check=check,
    )

    with (
        DuckDBStore(database_path, read_only=True) as readonly_store,
        pytest.raises(ValueError, match="duplicate finding issue_id"),
    ):
        run_audit(readonly_store, (rule,))


def test_record_report_is_idempotent_and_supports_resolve_reopen(
    tmp_path: Path,
) -> None:
    finding = AuditFinding(
        rule_id="missing-trading-days",
        dataset_id="daily",
        severity="P1",
        scope_key="2026-07-13",
        message="daily data is missing",
    )
    first_seen = datetime(2026, 7, 14, 1, tzinfo=UTC)
    resolved_at = datetime(2026, 7, 14, 2, tzinfo=UTC)
    reopened_at = datetime(2026, 7, 14, 3, tzinfo=UTC)
    first_report = AuditReport(
        observed_at=first_seen,
        rule_ids=(finding.rule_id,),
        findings=(finding,),
    )
    reopened_report = AuditReport(
        observed_at=reopened_at,
        rule_ids=(finding.rule_id,),
        findings=(finding,),
    )

    with DuckDBStore(tmp_path / "lifecycle.duckdb") as store:
        first = record_audit_report(store, first_report)
        repeated = record_audit_report(store, first_report)
        resolved = resolve_audit_issues(
            store,
            first_report.issue_ids,
            resolved_at=resolved_at,
        )
        reopened = record_audit_report(store, reopened_report)

    assert repeated == first
    assert resolved[0].status == "resolved"
    assert reopened[0].issue_id == first[0].issue_id
    assert reopened[0].status == "open"
    assert reopened[0].first_seen_at == first_seen
    assert reopened[0].last_seen_at == reopened_at
    assert reopened[0].resolved_at is None


def test_record_audit_report_rejects_readonly_store_before_write(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "readonly-record.duckdb"
    finding = AuditFinding(
        rule_id="readonly-record",
        dataset_id="daily",
        severity="P1",
        scope_key="2026-07-13",
        message="must not be written through a read-only store",
    )
    report = AuditReport(
        observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        rule_ids=(finding.rule_id,),
        findings=(finding,),
    )

    with DuckDBStore(database_path):
        pass
    with (
        DuckDBStore(database_path, read_only=True) as readonly_store,
        pytest.raises(ValueError, match="writable DuckDBStore"),
    ):
        record_audit_report(readonly_store, report)


def test_resolve_audit_issues_rejects_readonly_store_before_write(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "readonly-resolve.duckdb"
    finding = AuditFinding(
        rule_id="readonly-resolve",
        dataset_id="daily",
        severity="P1",
        scope_key="2026-07-13",
        message="must not resolve through a read-only store",
    )
    report = AuditReport(
        observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        rule_ids=(finding.rule_id,),
        findings=(finding,),
    )

    with DuckDBStore(database_path) as writable_store:
        record_audit_report(writable_store, report)
    with (
        DuckDBStore(database_path, read_only=True) as readonly_store,
        pytest.raises(ValueError, match="writable DuckDBStore"),
    ):
        resolve_audit_issues(readonly_store, report.issue_ids)


def test_repair_defaults_to_dry_run_and_records_before_after_counts(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "repair.duckdb"
    state = {"bad_rows": 2, "apply_calls": 0, "count_calls": 0}

    def count_bad_rows(_store: DuckDBStore) -> int:
        state["count_calls"] += 1
        return state["bad_rows"]

    def remove_bad_rows(_store: DuckDBStore) -> None:
        state["apply_calls"] += 1
        state["bad_rows"] = 0

    action = RepairAction(
        action_id="remove-duplicate-bars",
        description="Remove duplicate daily bars",
        count_affected=count_bad_rows,
        apply=remove_bad_rows,
    )

    with DuckDBStore(database_path):
        pass
    with DuckDBStore(database_path, read_only=True) as readonly_store:
        preview = run_repair(readonly_store, action)
    with DuckDBStore(database_path) as writable_store:
        applied = run_repair(writable_store, action, dry_run=False)

    assert isinstance(preview, RepairReport)
    assert preview.dry_run is True
    assert preview.before_count == 2
    assert preview.after_count is None
    assert preview.changed_count is None
    assert state["apply_calls"] == 1
    assert state["count_calls"] == 3
    assert applied.dry_run is False
    assert applied.before_count == 2
    assert applied.after_count == 0
    assert applied.changed_count == 2


def test_repair_dry_run_rejects_writable_store_before_count(
    tmp_path: Path,
) -> None:
    calls = {"count": 0, "apply": 0}

    def count(_store: DuckDBStore) -> int:
        calls["count"] += 1
        return 1

    def apply(_store: DuckDBStore) -> None:
        calls["apply"] += 1

    action = RepairAction(
        action_id="dry-run-access-mode",
        description="Reject writable dry-run stores",
        count_affected=count,
        apply=apply,
    )

    with (
        DuckDBStore(tmp_path / "writable-dry-run.duckdb") as writable_store,
        pytest.raises(ValueError, match="read-only DuckDBStore"),
    ):
        run_repair(writable_store, action)

    assert calls == {"count": 0, "apply": 0}


def test_repair_apply_rejects_readonly_store_before_callbacks(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "readonly-apply.duckdb"
    calls = {"count": 0, "apply": 0}

    def count(_store: DuckDBStore) -> int:
        calls["count"] += 1
        return 1

    def apply(_store: DuckDBStore) -> None:
        calls["apply"] += 1

    action = RepairAction(
        action_id="apply-access-mode",
        description="Reject read-only apply stores",
        count_affected=count,
        apply=apply,
    )

    with DuckDBStore(database_path):
        pass
    with (
        DuckDBStore(database_path, read_only=True) as readonly_store,
        pytest.raises(ValueError, match="writable DuckDBStore"),
    ):
        run_repair(readonly_store, action, dry_run=False)

    assert calls == {"count": 0, "apply": 0}


def test_repair_rejects_negative_before_count_before_apply(tmp_path: Path) -> None:
    apply_called = False

    def count(_store: DuckDBStore) -> int:
        return -1

    def apply(_store: DuckDBStore) -> None:
        nonlocal apply_called
        apply_called = True

    action = RepairAction(
        action_id="negative-before",
        description="Reject invalid candidate counts",
        count_affected=count,
        apply=apply,
    )

    with (
        DuckDBStore(tmp_path / "negative-before.duckdb") as writable_store,
        pytest.raises(ValueError, match="before_count cannot be negative"),
    ):
        run_repair(writable_store, action, dry_run=False)

    assert apply_called is False


@pytest.mark.parametrize("invalid_count", [True, 1.0, "1"])
def test_repair_rejects_non_strict_before_count_before_apply(
    tmp_path: Path,
    invalid_count: object,
) -> None:
    apply_called = False

    def count(_store: DuckDBStore) -> int:
        return cast(int, invalid_count)

    def apply(_store: DuckDBStore) -> None:
        nonlocal apply_called
        apply_called = True

    action = RepairAction(
        action_id="non-strict-before",
        description="Reject coerced candidate counts",
        count_affected=count,
        apply=apply,
    )

    with (
        DuckDBStore(tmp_path / f"non-strict-before-{type(invalid_count).__name__}.duckdb")
        as writable_store,
        pytest.raises(TypeError, match="before_count must be a strict int"),
    ):
        run_repair(writable_store, action, dry_run=False)

    assert apply_called is False


@pytest.mark.parametrize(
    "counts",
    [
        {"before_count": True, "after_count": 0},
        {"before_count": 1.0, "after_count": 0},
        {"before_count": "1", "after_count": 0},
        {"before_count": 1, "after_count": False},
        {"before_count": 1, "after_count": 0.0},
        {"before_count": 1, "after_count": "0"},
    ],
)
def test_repair_report_rejects_non_strict_counts(
    counts: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        RepairReport(
            action_id="strict-report",
            dry_run=False,
            **counts,
        )


@pytest.mark.parametrize(
    ("after_count", "message"),
    [
        (-1, "after_count cannot be negative"),
        (2, "after_count cannot exceed before_count"),
    ],
)
def test_repair_rejects_invalid_after_count(
    tmp_path: Path,
    after_count: int,
    message: str,
) -> None:
    counts = iter((1, after_count))

    def count(_store: DuckDBStore) -> int:
        return next(counts)

    def apply(_store: DuckDBStore) -> None:
        return None

    action = RepairAction(
        action_id="invalid-after",
        description="Reject invalid post-repair counts",
        count_affected=count,
        apply=apply,
    )

    with (
        DuckDBStore(
            tmp_path / f"invalid-after-{after_count}.duckdb"
        ) as writable_store,
        pytest.raises(ValueError, match=message),
    ):
        run_repair(writable_store, action, dry_run=False)


def test_repair_rolls_back_delete_when_post_count_raises(tmp_path: Path) -> None:
    count_calls = 0

    def count(store: DuckDBStore) -> int:
        nonlocal count_calls
        count_calls += 1
        if count_calls == 2:
            raise RuntimeError("post-count failed")
        row = store._conn.execute("SELECT count(*) FROM repair_target").fetchone()
        assert row is not None
        return int(row[0])

    def apply(store: DuckDBStore) -> None:
        store._conn.execute("DELETE FROM repair_target")

    action = RepairAction(
        action_id="post-count-error",
        description="Rollback when post-count raises",
        count_affected=count,
        apply=apply,
    )

    with DuckDBStore(tmp_path / "post-count-error.duckdb") as writable_store:
        writable_store._conn.execute("CREATE TABLE repair_target (id INTEGER)")
        writable_store._conn.execute("INSERT INTO repair_target VALUES (1), (2)")
        with pytest.raises(RuntimeError, match="post-count failed"):
            run_repair(writable_store, action, dry_run=False)
        remaining = writable_store._conn.execute(
            "SELECT count(*) FROM repair_target"
        ).fetchone()

    assert remaining == (2,)


@pytest.mark.parametrize("invalid_after", [True, 3])
def test_repair_rolls_back_delete_when_post_count_is_invalid(
    tmp_path: Path,
    invalid_after: object,
) -> None:
    count_calls = 0

    def count(store: DuckDBStore) -> int:
        nonlocal count_calls
        count_calls += 1
        if count_calls == 2:
            return cast(int, invalid_after)
        row = store._conn.execute("SELECT count(*) FROM repair_target").fetchone()
        assert row is not None
        return int(row[0])

    def apply(store: DuckDBStore) -> None:
        store._conn.execute("DELETE FROM repair_target")

    action = RepairAction(
        action_id="invalid-post-count",
        description="Rollback when post-count is invalid",
        count_affected=count,
        apply=apply,
    )
    expected_error = (
        "after_count must be a strict int"
        if invalid_after is True
        else "after_count cannot exceed before_count"
    )

    with DuckDBStore(
        tmp_path / f"invalid-post-count-{type(invalid_after).__name__}.duckdb"
    ) as writable_store:
        writable_store._conn.execute("CREATE TABLE repair_target (id INTEGER)")
        writable_store._conn.execute("INSERT INTO repair_target VALUES (1), (2)")
        with pytest.raises((TypeError, ValueError), match=expected_error):
            run_repair(writable_store, action, dry_run=False)
        remaining = writable_store._conn.execute(
            "SELECT count(*) FROM repair_target"
        ).fetchone()

    assert remaining == (2,)


def test_repair_rolls_back_keyboard_interrupt_and_reuses_connection(
    tmp_path: Path,
) -> None:
    def count(store: DuckDBStore) -> int:
        row = store._conn.execute("SELECT count(*) FROM repair_target").fetchone()
        assert row is not None
        return int(row[0])

    def interrupt_after_delete(store: DuckDBStore) -> None:
        store._conn.execute("DELETE FROM repair_target")
        raise KeyboardInterrupt("repair interrupted")

    interrupted_action = RepairAction(
        action_id="keyboard-interrupt",
        description="Rollback an interrupted repair",
        count_affected=count,
        apply=interrupt_after_delete,
    )

    def delete_one_row(store: DuckDBStore) -> None:
        store._conn.execute("DELETE FROM repair_target WHERE id = 1")

    successful_action = RepairAction(
        action_id="after-keyboard-interrupt",
        description="Reuse the connection after rollback",
        count_affected=count,
        apply=delete_one_row,
    )

    with DuckDBStore(tmp_path / "keyboard-interrupt.duckdb") as writable_store:
        writable_store._conn.execute("CREATE TABLE repair_target (id INTEGER)")
        writable_store._conn.execute("INSERT INTO repair_target VALUES (1), (2)")
        with pytest.raises(KeyboardInterrupt, match="repair interrupted"):
            run_repair(writable_store, interrupted_action, dry_run=False)
        remaining_after_interrupt = writable_store._conn.execute(
            "SELECT count(*) FROM repair_target"
        ).fetchone()
        report = run_repair(writable_store, successful_action, dry_run=False)
        remaining_after_reuse = writable_store._conn.execute(
            "SELECT count(*) FROM repair_target"
        ).fetchone()

    assert remaining_after_interrupt == (2,)
    assert report.before_count == 2
    assert report.after_count == 1
    assert remaining_after_reuse == (1,)


def test_repair_exposes_original_and_rollback_failure_when_state_is_uncertain(
    tmp_path: Path,
) -> None:
    class RepairApplyError(RuntimeError):
        pass

    def count(store: DuckDBStore) -> int:
        row = store._conn.execute("SELECT count(*) FROM repair_target").fetchone()
        assert row is not None
        return int(row[0])

    def close_connection_and_fail(store: DuckDBStore) -> None:
        store._conn.close()
        raise RepairApplyError("apply failed after connection closed")

    action = RepairAction(
        action_id="rollback-failure",
        description="Expose rollback failure and uncertain state",
        count_affected=count,
        apply=close_connection_and_fail,
    )
    store = DuckDBStore(tmp_path / "rollback-failure.duckdb")
    store._conn.execute("CREATE TABLE repair_target (id INTEGER)")
    store._conn.execute("INSERT INTO repair_target VALUES (1)")

    with pytest.raises(
        BaseExceptionGroup,
        match="database state uncertain",
    ) as caught:
        run_repair(store, action, dry_run=False)

    original, rollback = caught.value.exceptions
    assert isinstance(original, RepairApplyError)
    assert str(original) == "apply failed after connection closed"
    assert not isinstance(rollback, RepairApplyError)
    assert "closed" in str(rollback).lower()
