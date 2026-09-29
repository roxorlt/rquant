"""Persistent factor jobs fence workers and trust only verified result artifacts."""

from __future__ import annotations

import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.factor.historical_adapter import (
    HistoricalFactorAdapterRequest,
    HistoricalFactorResearch,
)
from rquant.factor.job_ledger import (
    FactorEvaluationJobLedger,
    FactorLedgerCompletionError,
    FactorLedgerConflictError,
    FactorLedgerIdentityError,
    FactorLedgerIntegrityError,
    FactorLedgerLeaseError,
)
from rquant.factor.job_runner import FactorEvaluationCompletion
from rquant.factor.job_spec import FactorEvaluationJobSpec
from rquant.factor.result import assemble_factor_research_result
from rquant.factor.result_artifact import publish_factor_research_artifact
from rquant.factor_snapshot_admission import FactorSnapshotAdmissionRequest
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_factor_result_artifact import _research

NOW = datetime(2026, 7, 17, 2, tzinfo=UTC)
REVISION = "c" * 40


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _research_and_spec() -> tuple[HistoricalFactorResearch, FactorEvaluationJobSpec]:
    original = _research()
    receipt_fields = original.receipt.model_dump(mode="python", exclude={"source_sha256"})
    receipt_fields["snapshot_id"] = "a" * 64
    source_sha256 = _digest(
        {
            **original.receipt.model_dump(mode="json", exclude={"source_sha256"}),
            "snapshot_id": "a" * 64,
        }
    )
    receipt = type(original.receipt)(**receipt_fields, source_sha256=source_sha256)
    request = original.request.model_copy(
        update={"factor_source_id": source_sha256, "return_source_id": source_sha256}
    )
    research = HistoricalFactorResearch(
        receipt=receipt, request=request, result=assemble_factor_research_result(request)
    )
    adapter = HistoricalFactorAdapterRequest(
        definition=request.factor_input.definition,
        stock_codes=receipt.stock_codes,
        pool_basis=receipt.pool_basis,
        evaluation_days=receipt.evaluation_days,
        query_start_date=receipt.query_start_date,
        query_end_date=receipt.query_end_date,
        holding_sessions=request.holding_sessions,
        as_of=request.as_of,
    )
    definition_sha256 = _digest(adapter.definition.model_dump(mode="json", round_trip=True))
    spec = FactorEvaluationJobSpec(
        code_revision=REVISION,
        admission_request=FactorSnapshotAdmissionRequest(
            snapshot_id=receipt.snapshot_id,
            binding_hash=receipt.binding_hash,
            start_date=receipt.query_start_date,
            end_date=receipt.query_end_date,
            source_mode="historical_retrospective",
        ),
        adapter_request=adapter,
        definition_content_sha256=definition_sha256,
        deadline=NOW + timedelta(hours=3),
    )
    return research, spec


def _sealed(tmp_path: Path) -> tuple[FactorEvaluationJobSpec, Path, FactorEvaluationCompletion]:
    research, spec = _research_and_spec()
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    receipt = publish_factor_research_artifact(research, REVISION, root)
    source = research.receipt
    completion = FactorEvaluationCompletion(
        spec_sha256=spec.spec_sha256,
        artifact_sha256=receipt.sha256,
        artifact_filename=receipt.filename,
        artifact_byte_count=receipt.byte_count,
        result_sha256=research.result.sha256,
        source_sha256=source.source_sha256,
        snapshot_id=source.snapshot_id,
        binding_hash=source.binding_hash,
        snapshot_as_of_time=source.snapshot_as_of_time,
        source_mode=source.source_mode,
        source_read_boundary=source.source_read_boundary,
        visibility_basis=source.visibility_basis,
        research_status=spec.research_status,
        result_kind=spec.result_kind,
        code_revision=spec.code_revision,
        completed_at=NOW,
    )
    return spec, root, completion


def _ledger(tmp_path: Path) -> FactorEvaluationJobLedger:
    ledger = FactorEvaluationJobLedger(tmp_path / "factor-jobs.sqlite3")
    ledger.initialize()
    return ledger


def test_round_trip_idempotent_submit_complete_and_reopen_with_external_identity(
    tmp_path: Path,
) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = FactorEvaluationJobLedger(tmp_path / "factor-jobs.sqlite3")
    identity = ledger.initialize()
    first = ledger.submit("request-1", spec, NOW)
    assert first.status == "queued"
    assert first.spec_sha256 == spec.spec_sha256
    assert ledger.submit("request-1", spec, NOW) == first
    assert ledger.submit("request-2", spec, NOW).job_id == first.job_id
    lease = ledger.claim(NOW, lease_seconds=60)
    assert lease is not None and lease.job.job_id == first.job_id
    assert lease.job.status == "running" and lease.job.attempts == 1
    extended = ledger.heartbeat(first.job_id, lease.lease_token, lease.version, NOW, 120)
    assert extended.expires_at == NOW + timedelta(seconds=120)
    success = ledger.complete(
        first.job_id, extended.lease_token, extended.version, completion, root, NOW
    )
    assert success.status == "succeeded"
    assert success.completion == completion
    assert ledger.submit("request-3", spec, NOW).job_id == first.job_id
    assert ledger.submit("request-4", spec, spec.deadline).job_id == first.job_id
    assert ledger.claim(NOW, lease_seconds=60) is None
    reopened = FactorEvaluationJobLedger.open_existing(identity)
    assert reopened.get(first.job_id) == success
    assert reopened.list_recent(limit=10) == (success,)
    with sqlite3.connect(identity.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM factor_jobs").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM factor_commands").fetchone()[0] == 4


def test_command_conflict_and_two_workers_claim_once(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _, spec = _research_and_spec()
    first = ledger.submit("request-1", spec, NOW)
    changed = spec.model_copy(update={"code_revision": "d" * 40})
    with pytest.raises(FactorLedgerConflictError):
        ledger.submit("request-1", changed, NOW)
    with ThreadPoolExecutor(max_workers=2) as workers:
        leases = tuple(workers.map(lambda _: ledger.claim(NOW, lease_seconds=60), range(2)))
    active = [lease for lease in leases if lease is not None]
    assert len(active) == 1
    assert active[0].job.job_id == first.job_id
    assert ledger.get(first.job_id).attempts == 1


def test_expired_lease_rotates_token_and_old_worker_cannot_finish(tmp_path: Path) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec, NOW)
    first = ledger.claim(NOW, lease_seconds=10)
    assert first is not None
    later = NOW + timedelta(seconds=11)
    second = ledger.claim(later, lease_seconds=60)
    assert second is not None
    assert second.lease_token != first.lease_token
    assert second.job.attempts == 2 and second.version > first.version
    with pytest.raises(FactorLedgerLeaseError):
        ledger.heartbeat(job.job_id, first.lease_token, first.version, later, 60)
    with pytest.raises(FactorLedgerLeaseError):
        ledger.complete(job.job_id, first.lease_token, first.version, completion, root, later)
    with pytest.raises(FactorLedgerLeaseError):
        ledger.fail(job.job_id, first.lease_token, first.version, "evaluation_failed", later)
    assert (
        ledger.complete(
            job.job_id, second.lease_token, second.version, completion, root, later
        ).status
        == "succeeded"
    )


def test_expired_spec_fails_without_claim_and_lease_is_capped(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _, spec = _research_and_spec()
    near = spec.model_copy(update={"deadline": NOW + timedelta(seconds=5)})
    with pytest.raises(FactorLedgerConflictError):
        ledger.submit("too-late", near, near.deadline)
    job = ledger.submit("near", near, NOW)
    lease = ledger.claim(NOW, lease_seconds=60)
    assert lease is not None and lease.expires_at == near.deadline
    with pytest.raises(FactorLedgerLeaseError):
        ledger.heartbeat(job.job_id, lease.lease_token, lease.version, near.deadline, 60)
    assert ledger.claim(near.deadline, lease_seconds=60) is None
    failed = ledger.get(job.job_id)
    assert failed.status == "failed" and failed.failure_code == "deadline_expired"
    assert failed.completion is None


@pytest.mark.parametrize(
    "changed",
    (
        {"spec_sha256": "f" * 64},
        {"source_sha256": "f" * 64},
        {"binding_hash": "f" * 64},
        {"snapshot_id": "f" * 64},
        {"result_sha256": "f" * 64},
        {"code_revision": "f" * 40},
        {"artifact_byte_count": 1},
        {"artifact_sha256": "f" * 64},
    ),
)
def test_completion_must_match_sealed_content_and_spec(
    tmp_path: Path, changed: dict[str, object]
) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec, NOW)
    lease = ledger.claim(NOW, lease_seconds=60)
    assert lease is not None
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(
            job.job_id,
            lease.lease_token,
            lease.version,
            completion.model_copy(update=changed),
            root,
            NOW,
        )
    assert ledger.get(job.job_id).status == "running"
    assert (root / completion.artifact_filename).is_file()


def test_missing_or_damaged_artifact_cannot_mark_success(tmp_path: Path) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec, NOW)
    lease = ledger.claim(NOW, lease_seconds=60)
    assert lease is not None
    target = root / completion.artifact_filename
    data = target.read_bytes()
    target.unlink()
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root, NOW)
    target.write_bytes(data[:20])
    target.chmod(0o600)
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root, NOW)
    assert ledger.get(job.job_id).status == "running"


def test_sealed_artifact_survives_transaction_failure_then_reclaim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module

    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec, NOW)
    first = ledger.claim(NOW, lease_seconds=5)
    assert first is not None
    original = module.FactorEvaluationJobLedger._store_job

    def fail_once(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected transaction failure")

    monkeypatch.setattr(module.FactorEvaluationJobLedger, "_store_job", fail_once)
    with pytest.raises(RuntimeError, match="injected transaction failure"):
        ledger.complete(job.job_id, first.lease_token, first.version, completion, root, NOW)
    monkeypatch.setattr(module.FactorEvaluationJobLedger, "_store_job", staticmethod(original))
    assert ledger.get(job.job_id).status == "running"
    assert (root / completion.artifact_filename).is_file()
    second = ledger.claim(NOW + timedelta(seconds=6), lease_seconds=60)
    assert second is not None and second.lease_token != first.lease_token
    assert (
        ledger.complete(
            job.job_id,
            second.lease_token,
            second.version,
            completion,
            root,
            NOW + timedelta(seconds=6),
        ).status
        == "succeeded"
    )


def test_external_identity_rejects_replaced_file_on_reopen_and_live_access(tmp_path: Path) -> None:
    path = tmp_path / "factor-jobs.sqlite3"
    ledger = FactorEvaluationJobLedger(path)
    identity = ledger.initialize()
    _, spec = _research_and_spec()
    job = ledger.submit("request-1", spec, NOW)
    path.rename(tmp_path / "old.sqlite3")
    replacement = FactorEvaluationJobLedger(path)
    replacement.initialize()
    with pytest.raises(FactorLedgerIdentityError):
        FactorEvaluationJobLedger.open_existing(identity)
    with pytest.raises(FactorLedgerIdentityError):
        ledger.get(job.job_id)
    with pytest.raises(FactorLedgerIdentityError):
        ledger.submit("request-2", spec, NOW)
    assert replacement.list_recent(limit=10) == ()


def test_initialize_rejects_file_replacement_before_schema_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module

    path = tmp_path / "factor-jobs.sqlite3"
    original = module.sqlite3.connect
    replaced = False

    def replace_before_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        nonlocal replaced
        if not replaced:
            replaced = True
            path.rename(tmp_path / "created.sqlite3")
            path.touch(mode=0o600)
        return original(*args, **kwargs)

    monkeypatch.setattr(module.sqlite3, "connect", replace_before_connect)
    with pytest.raises(FactorLedgerIdentityError):
        FactorEvaluationJobLedger(path).initialize()


def test_tampered_state_spec_or_command_is_not_trusted(tmp_path: Path) -> None:
    for column, value in (
        ("status", "succeeded"),
        ("spec_json", "{}"),
        ("status", sqlite3.Binary(b"running")),
        ("row_sha256", "0" * 64),
    ):
        root = tmp_path / f"{column}-{type(value).__name__}"
        root.mkdir(mode=0o700)
        ledger = _ledger(root)
        _, spec = _research_and_spec()
        job = ledger.submit("request-1", spec, NOW)
        with sqlite3.connect(ledger.path) as connection:
            connection.execute(
                f"UPDATE factor_jobs SET {column} = ? WHERE job_id = ?", (value, job.job_id)
            )
        with pytest.raises(FactorLedgerIntegrityError):
            ledger.get(job.job_id)
        with pytest.raises(FactorLedgerIntegrityError):
            ledger.list_recent(limit=10)
        with pytest.raises(FactorLedgerIntegrityError):
            ledger.submit("request-1", spec, NOW)


def test_tampered_command_type_is_rejected_as_integrity_failure(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _, spec = _research_and_spec()
    ledger.submit("request-1", spec, NOW)
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            "UPDATE factor_commands SET spec_sha256 = ? WHERE command_id = ?",
            (sqlite3.Binary(b"f" * 64), "request-1"),
        )
    with pytest.raises(FactorLedgerIntegrityError):
        ledger.submit("request-1", spec, NOW)


def test_failure_reason_and_list_budget_are_bounded(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _, spec = _research_and_spec()
    job = ledger.submit("request-1", spec, NOW)
    lease = ledger.claim(NOW, lease_seconds=60)
    assert lease is not None
    with pytest.raises(ValueError):
        ledger.fail(job.job_id, lease.lease_token, lease.version, "/private/secret", NOW)
    failed = ledger.fail(job.job_id, lease.lease_token, lease.version, "evaluation_failed", NOW)
    assert failed.status == "failed" and failed.failure_code == "evaluation_failed"
    assert failed.completion is None
    for limit in (0, 201):
        with pytest.raises(ValueError):
            ledger.list_recent(limit=limit)
