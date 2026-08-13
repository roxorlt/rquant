from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineLedgerError,
    DailyPipelineMode,
    DailyPipelineStorageBinding,
    DailyPipelineStorageProfile,
    DailyRunSpec,
    DailyRunState,
    DailyStageReceipt,
    DailyStageSpec,
    DailyStageState,
    DailyWriterLease,
    StageFailure,
    StageResult,
)
from rquant.runtime_contracts import canonical_sha256

NOW = datetime(2026, 8, 3, 9, 0, tzinfo=UTC)
SHA = "a" * 64
COMMIT = "b" * 40


def _spec(**overrides: object) -> DailyRunSpec:
    values: dict[str, object] = {
        "mode": DailyPipelineMode.SHADOW,
        "trade_date": date(2026, 8, 3),
        "source_generation_id": SHA,
        "source_content_hash": "c" * 64,
        "command_manifest_hash": "e" * 64,
        "code_commit": COMMIT,
        "profile_hash": "d" * 64,
        "stages": (
            DailyStageSpec(stage_id="capture", max_attempts=2),
            DailyStageSpec(stage_id="publish", depends_on=("capture",)),
        ),
    }
    values.update(overrides)
    return DailyRunSpec.model_validate(values)


def _profile(tmp_path: Path) -> DailyPipelineStorageProfile:
    return DailyPipelineStorageProfile.create(
        root=tmp_path.resolve(),
        mode=DailyPipelineMode.SHADOW,
        profile_hash="d" * 64,
    )


def _ledger(tmp_path: Path, *, service_owner: str = "daily-close") -> DailyPipelineLedger:
    return DailyPipelineLedger(
        storage_profile=_profile(tmp_path),
        service_owner=service_owner,
    )


def _lease(ledger: DailyPipelineLedger, now: datetime = NOW):
    return ledger.acquire_writer(owner="daily-close", now=now, lease_for=timedelta(minutes=5))


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def test_create_claim_and_complete_only_ready_stage(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(lease, _spec(), now=NOW)

    capture = ledger.claim_next(lease, now=NOW)

    assert capture is not None
    assert capture.stage_id == "capture"
    assert ledger.claim_next(lease, now=NOW) is None

    receipt = ledger.succeed(
        lease,
        capture,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW + timedelta(seconds=1),
    )
    assert receipt.stage_id == "capture"
    assert ledger.stage(run.run_id, "capture").state is DailyStageState.SUCCEEDED

    publish = ledger.claim_next(lease, now=NOW + timedelta(seconds=2))
    assert publish is not None
    assert publish.stage_id == "publish"


def test_stage_graph_rejects_unknown_dependency() -> None:
    with pytest.raises(ValueError, match="dependency is not declared"):
        DailyRunSpec.model_validate(
            {
                "trade_date": date(2026, 8, 3),
                "source_generation_id": SHA,
                "source_content_hash": "c" * 64,
                "code_commit": COMMIT,
                "profile_hash": "d" * 64,
                "stages": (DailyStageSpec(stage_id="capture", depends_on=("missing",)),),
            }
        )


def test_stage_graph_rejects_cycles() -> None:
    with pytest.raises(ValueError, match="contains a cycle"):
        DailyRunSpec.model_validate(
            {
                "trade_date": date(2026, 8, 3),
                "source_generation_id": SHA,
                "source_content_hash": "c" * 64,
                "code_commit": COMMIT,
                "profile_hash": "d" * 64,
                "stages": (
                    DailyStageSpec(stage_id="capture", depends_on=("publish",)),
                    DailyStageSpec(stage_id="publish", depends_on=("capture",)),
                ),
            }
        )


def test_conflicting_run_identity_and_writer_owner_fail_closed(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    first = _spec(run_id="daily-close-20260803")
    ledger.create_run(lease, first, now=NOW)

    with pytest.raises(DailyPipelineLedgerError, match="stable service owner"):
        _ledger(tmp_path, service_owner="other-owner")

    changed = _spec(run_id=first.run_id, profile_hash="9" * 64)
    with pytest.raises(DailyPipelineLedgerError, match="storage profile"):
        ledger.create_run(lease, changed, now=NOW)


def test_retry_backoff_deadline_and_cancel_are_durable(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(
        lease,
        _spec(
            stages=(
                DailyStageSpec(
                    stage_id="capture",
                    max_attempts=2,
                    retry_backoff_seconds=30,
                ),
            )
        ),
        now=NOW,
    )
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None

    waiting = ledger.fail(
        lease,
        attempt,
        StageFailure(error_code="upstream_timeout", message="provider timed out"),
        retryable=True,
        now=NOW + timedelta(seconds=1),
    )
    assert waiting.state is DailyStageState.RETRY_WAIT
    assert ledger.claim_next(lease, now=NOW + timedelta(seconds=30)) is None

    retry = ledger.claim_next(lease, now=NOW + timedelta(seconds=31))
    assert retry is not None
    assert retry.attempt_number == 2
    ledger.cancel_run(lease, run.run_id, reason="operator_cancel", now=NOW + timedelta(seconds=32))
    assert ledger.stage(run.run_id, "capture").state is DailyStageState.CANCELLED


def test_claim_next_exhaustion_refreshes_single_stage_run_to_failed(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(
        lease,
        _spec(stages=(DailyStageSpec(stage_id="capture", max_attempts=1),)),
        now=NOW,
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET state = ?, attempts = ?, next_attempt_at = NULL
            WHERE run_id = ? AND stage_id = ?
            """,
            (DailyStageState.RETRY_WAIT.value, 1, run.run_id, "capture"),
        )

    assert ledger.claim_next(lease, now=NOW + timedelta(seconds=1)) is None
    assert ledger.stage(run.run_id, "capture").state is DailyStageState.FAILED
    assert ledger.run(run.run_id).state is DailyRunState.FAILED


def test_success_is_receipt_first_idempotent_and_cannot_change_terminal_result(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    ledger.create_run(lease, _spec(), now=NOW)
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None

    result = StageResult(content_hash="e" * 64, evidence_hash="f" * 64)
    prepared = ledger.prepare_success(lease, attempt, result, now=NOW + timedelta(seconds=1))
    assert ledger.stage(attempt.run_id, attempt.stage_id).state is DailyStageState.RUNNING
    finalized = ledger.finalize_success(lease, prepared, now=NOW + timedelta(seconds=2))
    assert finalized == prepared
    assert ledger.succeed(lease, attempt, result, now=NOW + timedelta(seconds=3)) == prepared

    with pytest.raises(DailyPipelineLedgerError, match="conflicting terminal receipt"):
        ledger.succeed(
            lease,
            attempt,
            StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
            now=NOW + timedelta(seconds=4),
        )


def test_recover_run_is_bounded_to_the_requested_run_id(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    first = ledger.create_run(
        lease,
        _spec(run_id="daily-recover-first", stages=(DailyStageSpec(stage_id="capture"),)),
        now=NOW,
    )
    second = ledger.create_run(
        lease,
        _spec(
            run_id="daily-recover-second",
            source_content_hash="e" * 64,
            stages=(DailyStageSpec(stage_id="capture"),),
        ),
        now=NOW,
    )
    first_attempt = ledger.claim_next_for_run(lease, first.run_id, now=NOW)
    second_attempt = ledger.claim_next_for_run(lease, second.run_id, now=NOW)
    assert first_attempt is not None and second_attempt is not None
    first_receipt = ledger.prepare_success(
        lease,
        first_attempt,
        StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        now=NOW,
    )
    ledger.prepare_success(
        lease,
        second_attempt,
        StageResult(content_hash="3" * 64, evidence_hash="4" * 64),
        now=NOW,
    )

    summary = ledger.recover(lease, now=NOW + timedelta(seconds=1), run_id=first.run_id)

    assert summary.finalized_receipt_ids == (first_receipt.receipt_id,)
    assert ledger.stage(first.run_id, "capture").state is DailyStageState.SUCCEEDED
    assert ledger.stage(second.run_id, "capture").state is DailyStageState.RUNNING


def test_prepare_success_rechecks_deadline_and_fails_closed(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(
        lease,
        _spec(
            deadline_at=NOW + timedelta(seconds=1),
            stages=(DailyStageSpec(stage_id="capture"),),
        ),
        now=NOW,
    )
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None

    with pytest.raises(DailyPipelineLedgerError, match="deadline"):
        ledger.prepare_success(
            lease,
            attempt,
            StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
            now=NOW + timedelta(seconds=1),
        )

    stage = ledger.stage(run.run_id, "capture")
    assert stage.state is DailyStageState.FAILED
    assert stage.terminal_receipt_id is None
    assert ledger.run(run.run_id).state is DailyRunState.FAILED


def test_finalize_success_after_deadline_binds_durable_receipt_to_failed_stage(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(
        lease,
        _spec(stages=(DailyStageSpec(stage_id="capture", deadline_at=NOW + timedelta(seconds=2)),)),
        now=NOW,
    )
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None
    prepared = ledger.prepare_success(
        lease,
        attempt,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW + timedelta(seconds=1),
    )

    with pytest.raises(DailyPipelineLedgerError, match="deadline"):
        ledger.finalize_success(lease, prepared, now=NOW + timedelta(seconds=2))

    stage = ledger.stage(run.run_id, "capture")
    assert stage.state is DailyStageState.FAILED
    assert stage.terminal_receipt_id == prepared.receipt_id
    assert ledger.receipt(prepared.receipt_id) == prepared
    assert ledger.run(run.run_id).state is DailyRunState.FAILED


def test_fail_uses_run_deadline_and_fails_closed_instead_of_retry_wait(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(
        lease,
        _spec(
            deadline_at=NOW + timedelta(seconds=1),
            stages=(DailyStageSpec(stage_id="capture", retry_backoff_seconds=30),),
        ),
        now=NOW,
    )
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None

    failed = ledger.fail(
        lease,
        attempt,
        StageFailure(error_code="upstream_timeout", message="provider timed out"),
        retryable=True,
        now=NOW + timedelta(seconds=1),
    )

    assert failed.state is DailyStageState.FAILED
    assert failed.next_attempt_at is None
    assert failed.last_failure == StageFailure(
        error_code="deadline_expired",
        message="stage deadline expired",
    )
    assert ledger.run(run.run_id).state is DailyRunState.FAILED


def test_cancel_run_preserves_durable_receipt_and_only_cancels_remaining_work(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(lease, _spec(), now=NOW)
    capture = ledger.claim_next(lease, now=NOW)
    assert capture is not None
    prepared = ledger.prepare_success(
        lease,
        capture,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW + timedelta(seconds=1),
    )

    cancelled = ledger.cancel_run(
        lease,
        run.run_id,
        reason="operator_cancel",
        now=NOW + timedelta(seconds=2),
    )

    assert ledger.stage(run.run_id, "capture").state is DailyStageState.SUCCEEDED
    assert ledger.stage(run.run_id, "capture").terminal_receipt_id == prepared.receipt_id
    assert ledger.stage(run.run_id, "publish").state is DailyStageState.CANCELLED
    assert cancelled.state is DailyRunState.CANCELLED
    assert ledger.receipt(prepared.receipt_id) == prepared


def test_future_stage_receipt_never_promotes_stage_to_success(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(lease, _spec(), now=NOW)

    with pytest.raises(DailyPipelineLedgerError, match="not claimed"):
        ledger.prepare_stage_success(
            lease,
            run_id=run.run_id,
            stage_id="publish",
            attempt_number=1,
            result=StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
            now=NOW,
        )
    assert ledger.stage(run.run_id, "publish").state is DailyStageState.PENDING


def test_unknown_schema_and_corrupt_receipt_fail_closed(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    path = profile.state_path
    ledger = DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")
    lease = _lease(ledger)
    ledger.create_run(lease, _spec(), now=NOW)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE daily_pipeline_meta SET value = '999' WHERE key = 'schema_version'"
        )

    with pytest.raises(DailyPipelineLedgerError, match="unsupported schema"):
        DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")


def test_nonempty_legacy_schema_without_native_mode_fails_closed(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    path = profile.state_path
    DailyPipelineStorageBinding.open(profile, leaf="state").close()
    spec = _spec(stages=(DailyStageSpec(stage_id="capture"),))
    lease = DailyWriterLease(
        service_owner="daily-close",
        fencing_token=1,
        acquired_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )
    prepared = DailyStageReceipt(
        mode=DailyPipelineMode.SHADOW,
        run_id=spec.run_id,
        stage_id="capture",
        attempt_number=1,
        input_identity=spec.input_identity,
        result=StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        prepared_at=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(path) as connection:
        connection.execute("PRAGMA journal_mode = WAL")
        connection.executescript(
            """
            CREATE TABLE daily_pipeline_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE daily_pipeline_writer (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                service_owner TEXT NOT NULL,
                fencing_token INTEGER NOT NULL,
                lease_expires_at TEXT,
                acquired_at TEXT
            );
            CREATE TABLE daily_pipeline_run (
                run_id TEXT PRIMARY KEY,
                spec_json TEXT NOT NULL,
                spec_hash TEXT NOT NULL,
                input_identity TEXT NOT NULL,
                state TEXT NOT NULL,
                created_at TEXT NOT NULL,
                cancelled_reason TEXT
            );
            CREATE TABLE daily_pipeline_stage (
                run_id TEXT NOT NULL REFERENCES daily_pipeline_run(run_id),
                stage_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                max_attempts INTEGER NOT NULL,
                retry_backoff_seconds INTEGER NOT NULL,
                deadline_at TEXT,
                next_attempt_at TEXT,
                claimed_at TEXT,
                claim_fencing_token INTEGER,
                claim_lease_expires_at TEXT,
                terminal_receipt_id TEXT,
                failure_code TEXT,
                failure_message TEXT,
                PRIMARY KEY (run_id, stage_id)
            );
            CREATE TABLE daily_pipeline_dependency (
                run_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                dependency_stage_id TEXT NOT NULL,
                PRIMARY KEY (run_id, stage_id, dependency_stage_id),
                FOREIGN KEY (run_id, stage_id)
                    REFERENCES daily_pipeline_stage(run_id, stage_id)
            );
            CREATE TABLE daily_pipeline_stage_receipt (
                receipt_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                stage_id TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                receipt_json TEXT NOT NULL,
                UNIQUE (run_id, stage_id, attempt_number),
                FOREIGN KEY (run_id, stage_id)
                    REFERENCES daily_pipeline_stage(run_id, stage_id)
            );
            CREATE INDEX daily_pipeline_ready_idx
                ON daily_pipeline_stage(state, next_attempt_at, sequence);
            """
        )
        connection.executemany(
            "INSERT INTO daily_pipeline_meta(key, value) VALUES (?, ?)",
            (("schema_version", "0"), ("service_owner", "daily-close")),
        )
        connection.execute(
            """
            INSERT INTO daily_pipeline_writer(
                singleton, service_owner, fencing_token, lease_expires_at, acquired_at
            ) VALUES (1, ?, ?, ?, ?)
            """,
            (
                "daily-close",
                lease.fencing_token,
                lease.expires_at.isoformat(timespec="microseconds"),
                lease.acquired_at.isoformat(timespec="microseconds"),
            ),
        )
        connection.execute(
            """
            INSERT INTO daily_pipeline_run(
                run_id, spec_json, spec_hash, input_identity, state, created_at, cancelled_reason
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                spec.run_id,
                _canonical_json(spec.model_dump(mode="json")),
                canonical_sha256(spec),
                spec.input_identity,
                DailyRunState.RUNNING.value,
                NOW.isoformat(timespec="microseconds"),
            ),
        )
        connection.execute(
            """
            INSERT INTO daily_pipeline_stage(
                run_id, stage_id, sequence, state, attempts, max_attempts,
                retry_backoff_seconds, deadline_at, next_attempt_at, claimed_at,
                claim_fencing_token, claim_lease_expires_at, terminal_receipt_id,
                failure_code, failure_message
            ) VALUES (?, ?, 0, ?, 1, 3, 30, NULL, NULL, ?, ?, ?, NULL, NULL, NULL)
            """,
            (
                spec.run_id,
                "capture",
                DailyStageState.RUNNING.value,
                lease.acquired_at.isoformat(timespec="microseconds"),
                lease.fencing_token,
                lease.expires_at.isoformat(timespec="microseconds"),
            ),
        )
        connection.execute(
            """
            INSERT INTO daily_pipeline_stage_receipt(
                receipt_id, run_id, stage_id, attempt_number, receipt_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                prepared.receipt_id,
                prepared.run_id,
                prepared.stage_id,
                prepared.attempt_number,
                _canonical_json(prepared.model_dump(mode="json")),
            ),
        )
        path.chmod(0o600)

    with pytest.raises(DailyPipelineLedgerError, match="immutable command manifest|native mode"):
        DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")


def test_missing_required_schema_table_fails_closed_at_startup(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    path = profile.state_path
    DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TABLE daily_pipeline_stage")

    with pytest.raises(DailyPipelineLedgerError, match="schema is corrupt"):
        DailyPipelineLedger(storage_profile=profile, service_owner="daily-close")


def test_corrupt_receipt_bytes_fail_closed_on_read(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    ledger.create_run(lease, _spec(), now=NOW)
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None
    receipt = ledger.prepare_success(
        lease,
        attempt,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW,
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            "UPDATE daily_pipeline_stage_receipt SET receipt_json = '{bad' WHERE receipt_id = ?",
            (receipt.receipt_id,),
        )

    with pytest.raises(DailyPipelineLedgerError, match="receipt is corrupt"):
        ledger.receipt(receipt.receipt_id)


def test_valid_json_receipt_semantic_tamper_fails_closed(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    ledger.create_run(lease, _spec(), now=NOW)
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None
    receipt = ledger.succeed(
        lease,
        attempt,
        StageResult(content_hash="e" * 64, evidence_hash="f" * 64),
        now=NOW,
    )
    tampered = DailyStageReceipt.model_validate(
        {
            **receipt.model_dump(mode="python", exclude={"receipt_id"}),
            "result": {
                "content_hash": "1" * 64,
                "evidence_hash": "2" * 64,
            },
            "receipt_id": None,
        }
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            """
            UPDATE daily_pipeline_stage_receipt
            SET receipt_id = ?, receipt_json = ?
            WHERE receipt_id = ?
            """,
            (
                tampered.receipt_id,
                _canonical_json(tampered.model_dump(mode="json")),
                receipt.receipt_id,
            ),
        )
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET terminal_receipt_id = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (tampered.receipt_id, receipt.run_id, receipt.stage_id),
        )

    with pytest.raises(DailyPipelineLedgerError, match="binding"):
        ledger.recover(
            _lease(ledger, now=NOW + timedelta(seconds=1)),
            now=NOW + timedelta(seconds=1),
        )


def test_recover_uses_active_run_pagination_and_ignores_historical_success(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)

    historical = ledger.create_run(
        lease,
        _spec(
            run_id="daily-historical",
            stages=(DailyStageSpec(stage_id="archive"),),
        ),
        now=NOW - timedelta(minutes=2),
    )
    historical_attempt = ledger.claim_next(lease, now=NOW - timedelta(minutes=2))
    assert historical_attempt is not None
    historical_receipt = ledger.succeed(
        lease,
        historical_attempt,
        StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        now=NOW - timedelta(minutes=2) + timedelta(seconds=1),
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            "DELETE FROM daily_pipeline_stage_receipt WHERE receipt_id = ?",
            (historical_receipt.receipt_id,),
        )

    ledger.create_run(
        lease,
        _spec(
            run_id="daily-active-one",
            stages=(DailyStageSpec(stage_id="alpha", deadline_at=NOW - timedelta(seconds=1)),),
        ),
        now=NOW - timedelta(minutes=1),
    )
    ledger.create_run(
        lease,
        _spec(
            run_id="daily-active-two",
            stages=(DailyStageSpec(stage_id="beta", deadline_at=NOW - timedelta(seconds=1)),),
        ),
        now=NOW - timedelta(seconds=30),
    )

    recovery_lease = _lease(ledger, now=NOW + timedelta(minutes=1))
    first = ledger.recover(recovery_lease, now=NOW + timedelta(minutes=1), limit=1)
    second = ledger.recover(
        recovery_lease,
        now=NOW + timedelta(minutes=1),
        limit=1,
        cursor=first.next_cursor,
    )

    assert first.failed_stage_ids == ("alpha",)
    assert first.next_cursor is not None
    assert second.failed_stage_ids == ("beta",)
    assert second.next_cursor is None
    assert ledger.run(historical.run_id).state is DailyRunState.SUCCEEDED


def test_recover_rejects_out_of_bounds_limit(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)

    with pytest.raises(ValueError, match="limit"):
        ledger.recover(lease, now=NOW, limit=0)


def test_new_writer_lease_fences_another_process_with_same_service_owner(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    first = _lease(ledger)
    second = _lease(ledger, now=NOW + timedelta(seconds=1))
    assert second.fencing_token == first.fencing_token + 1

    with pytest.raises(DailyPipelineLedgerError, match="writer lease is stale"):
        ledger.create_run(first, _spec(), now=NOW + timedelta(seconds=1))


def test_same_run_cross_stage_terminal_receipt_swap_fails_closed_on_read_and_recover(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(
        lease,
        _spec(
            stages=(
                DailyStageSpec(stage_id="alpha"),
                DailyStageSpec(stage_id="beta"),
            )
        ),
        now=NOW,
    )
    alpha = ledger.claim_next(lease, now=NOW)
    assert alpha is not None
    alpha_receipt = ledger.succeed(
        lease,
        alpha,
        StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        now=NOW + timedelta(seconds=1),
    )
    beta = ledger.claim_next(lease, now=NOW + timedelta(seconds=2))
    assert beta is not None
    beta_receipt = ledger.succeed(
        lease,
        beta,
        StageResult(content_hash="3" * 64, evidence_hash="4" * 64),
        now=NOW + timedelta(seconds=3),
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET terminal_receipt_id = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (beta_receipt.receipt_id, run.run_id, "alpha"),
        )
        connection.execute(
            "UPDATE daily_pipeline_run SET state = ? WHERE run_id = ?",
            (DailyRunState.RUNNING.value, run.run_id),
        )

    with pytest.raises(DailyPipelineLedgerError, match="terminal receipt binding"):
        ledger.stage(run.run_id, "alpha")
    with pytest.raises(DailyPipelineLedgerError, match="terminal receipt binding"):
        ledger.recover(
            _lease(ledger, now=NOW + timedelta(seconds=4)),
            now=NOW + timedelta(seconds=4),
        )
    assert alpha_receipt.receipt_id != beta_receipt.receipt_id


def test_cross_run_terminal_receipt_swap_fails_closed_on_run_read_and_cancel(
    tmp_path: Path,
) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    first = ledger.create_run(
        lease,
        _spec(run_id="daily-first", stages=(DailyStageSpec(stage_id="alpha"),)),
        now=NOW,
    )
    first_attempt = ledger.claim_next(lease, now=NOW)
    assert first_attempt is not None
    ledger.succeed(
        lease,
        first_attempt,
        StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        now=NOW + timedelta(seconds=1),
    )
    second = ledger.create_run(
        lease,
        _spec(
            run_id="daily-second",
            stages=(DailyStageSpec(stage_id="alpha"), DailyStageSpec(stage_id="beta")),
        ),
        now=NOW + timedelta(seconds=2),
    )
    second_attempt = ledger.claim_next(lease, now=NOW + timedelta(seconds=2))
    assert second_attempt is not None
    second_receipt = ledger.succeed(
        lease,
        second_attempt,
        StageResult(content_hash="3" * 64, evidence_hash="4" * 64),
        now=NOW + timedelta(seconds=3),
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET terminal_receipt_id = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (second_receipt.receipt_id, first.run_id, "alpha"),
        )

    with pytest.raises(DailyPipelineLedgerError, match="terminal receipt binding"):
        ledger.run(first.run_id)
    with pytest.raises(DailyPipelineLedgerError, match="terminal receipt binding"):
        ledger.cancel_run(
            _lease(ledger, now=NOW + timedelta(seconds=4)),
            first.run_id,
            reason="operator_cancel",
            now=NOW + timedelta(seconds=4),
        )
    assert second.run_id != first.run_id


def test_old_attempt_terminal_receipt_pointer_fails_closed_on_read(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(
        lease,
        _spec(stages=(DailyStageSpec(stage_id="alpha", max_attempts=2),)),
        now=NOW,
    )
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None
    old_receipt = ledger.prepare_success(
        lease,
        attempt,
        StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        now=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET state = ?, attempts = ?, terminal_receipt_id = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (
                DailyStageState.SUCCEEDED.value,
                2,
                old_receipt.receipt_id,
                run.run_id,
                "alpha",
            ),
        )

    with pytest.raises(DailyPipelineLedgerError, match="terminal receipt binding"):
        ledger.stage(run.run_id, "alpha")


def test_finalize_rejects_swapped_terminal_receipt_pointer(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    lease = _lease(ledger)
    run = ledger.create_run(lease, _spec(), now=NOW)
    attempt = ledger.claim_next(lease, now=NOW)
    assert attempt is not None
    prepared = ledger.prepare_success(
        lease,
        attempt,
        StageResult(content_hash="1" * 64, evidence_hash="2" * 64),
        now=NOW + timedelta(seconds=1),
    )
    other = DailyStageReceipt(
        mode=DailyPipelineMode.SHADOW,
        run_id=run.run_id,
        stage_id="publish",
        attempt_number=1,
        input_identity=run.input_identity,
        result=StageResult(content_hash="3" * 64, evidence_hash="4" * 64),
        prepared_at=NOW + timedelta(seconds=1),
    )
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            """
                INSERT INTO daily_pipeline_stage_receipt(
                    receipt_id, mode, profile_hash, namespace_id, run_id, stage_id,
                    attempt_number, content_hash, evidence_hash, receipt_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                other.receipt_id,
                other.mode.value,
                ledger.storage_profile.profile_hash,
                ledger.storage_profile.namespace_id,
                other.run_id,
                other.stage_id,
                other.attempt_number,
                other.result.content_hash,
                other.result.evidence_hash,
                _canonical_json(other.model_dump(mode="json")),
            ),
        )
        connection.execute(
            """
            UPDATE daily_pipeline_stage
            SET state = ?, terminal_receipt_id = ?
            WHERE run_id = ? AND stage_id = ?
            """,
            (
                DailyStageState.SUCCEEDED.value,
                other.receipt_id,
                run.run_id,
                "capture",
            ),
        )

    with pytest.raises(DailyPipelineLedgerError, match="terminal receipt binding"):
        ledger.finalize_success(lease, prepared, now=NOW + timedelta(seconds=2))
