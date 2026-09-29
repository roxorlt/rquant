"""Persistent factor jobs fence workers and trust only verified result artifacts."""

from __future__ import annotations

import hashlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.factor.display_artifact import (
    FactorDisplayArtifactV1,
    load_factor_display_artifact,
    publish_factor_display_artifact,
)
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
from rquant.factor.result_artifact import (
    load_factor_research_artifact,
    publish_factor_research_artifact,
)
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
    display = publish_factor_display_artifact(
        load_factor_research_artifact(root, receipt.sha256), root
    )
    source = research.receipt
    completion = FactorEvaluationCompletion(
        spec_sha256=spec.spec_sha256,
        artifact_sha256=receipt.sha256,
        artifact_filename=receipt.filename,
        artifact_byte_count=receipt.byte_count,
        display_artifact_sha256=display.sha256,
        display_artifact_filename=display.filename,
        display_artifact_byte_count=display.byte_count,
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


class _Clock:
    def __init__(self, instant: datetime = NOW) -> None:
        self.instant = instant

    def __call__(self) -> datetime:
        return self.instant


def _ledger(tmp_path: Path, *, clock: _Clock | None = None) -> FactorEvaluationJobLedger:
    ledger = FactorEvaluationJobLedger(tmp_path / "factor-jobs.sqlite3", clock=clock or _Clock())
    ledger.initialize()
    return ledger


def test_round_trip_idempotent_submit_complete_and_reopen_with_external_identity(
    tmp_path: Path,
) -> None:
    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = FactorEvaluationJobLedger(tmp_path / "factor-jobs.sqlite3", clock=clock)
    identity = ledger.initialize()
    first = ledger.submit("request-1", spec)
    assert first.status == "queued"
    assert first.spec_sha256 == spec.spec_sha256
    assert ledger.submit("request-1", spec) == first
    assert ledger.submit("request-2", spec).job_id == first.job_id
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None and lease.job.job_id == first.job_id
    assert lease.job.status == "running" and lease.job.attempts == 1
    extended = ledger.heartbeat(first.job_id, lease.lease_token, lease.version, 120)
    assert extended.expires_at == NOW + timedelta(seconds=120)
    success = ledger.complete(
        first.job_id, extended.lease_token, extended.version, completion, root
    )
    assert success.status == "succeeded"
    assert success.completion == completion
    assert ledger.submit("request-3", spec).job_id == first.job_id
    assert ledger.claim(lease_seconds=60) is None
    clock.instant = spec.deadline
    reopened = FactorEvaluationJobLedger.open_existing(identity, clock=clock)
    assert reopened.submit("request-4", spec).job_id == first.job_id
    assert reopened.get(first.job_id) == success
    assert reopened.list_recent(limit=10) == (success,)
    with sqlite3.connect(identity.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM factor_jobs").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM factor_commands").fetchone()[0] == 4


def test_command_conflict_and_two_workers_claim_once(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _, spec = _research_and_spec()
    first = ledger.submit("request-1", spec)
    changed = spec.model_copy(update={"code_revision": "d" * 40})
    with pytest.raises(FactorLedgerConflictError):
        ledger.submit("request-1", changed)
    with ThreadPoolExecutor(max_workers=2) as workers:
        leases = tuple(workers.map(lambda _: ledger.claim(lease_seconds=60), range(2)))
    active = [lease for lease in leases if lease is not None]
    assert len(active) == 1
    assert active[0].job.job_id == first.job_id
    assert ledger.get(first.job_id).attempts == 1


def test_expired_lease_rotates_token_and_old_worker_cannot_finish(tmp_path: Path) -> None:
    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)
    first = ledger.claim(lease_seconds=10)
    assert first is not None
    clock.instant = NOW + timedelta(seconds=11)
    second = ledger.claim(lease_seconds=60)
    assert second is not None
    assert second.lease_token != first.lease_token
    assert second.job.attempts == 2 and second.version > first.version
    with pytest.raises(FactorLedgerLeaseError):
        ledger.heartbeat(job.job_id, first.lease_token, first.version, 60)
    with pytest.raises(FactorLedgerLeaseError):
        ledger.complete(job.job_id, first.lease_token, first.version, completion, root)
    with pytest.raises(FactorLedgerLeaseError):
        ledger.fail(job.job_id, first.lease_token, first.version, "evaluation_failed")
    assert (
        ledger.complete(job.job_id, second.lease_token, second.version, completion, root).status
        == "succeeded"
    )


def test_expired_spec_fails_without_claim_and_lease_is_capped(tmp_path: Path) -> None:
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    _, spec = _research_and_spec()
    near = spec.model_copy(update={"deadline": NOW + timedelta(seconds=5)})
    clock.instant = near.deadline
    with pytest.raises(FactorLedgerConflictError):
        ledger.submit("too-late", near)
    clock.instant = NOW
    job = ledger.submit("near", near)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None and lease.expires_at == near.deadline
    clock.instant = near.deadline
    with pytest.raises(FactorLedgerLeaseError):
        ledger.heartbeat(job.job_id, lease.lease_token, lease.version, 60)
    assert ledger.claim(lease_seconds=60) is None
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
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(
            job.job_id,
            lease.lease_token,
            lease.version,
            completion.model_copy(update=changed),
            root,
        )
    assert ledger.get(job.job_id).status == "running"
    assert (root / completion.artifact_filename).is_file()


def test_wider_admission_cannot_borrow_a_narrower_sealed_artifact(tmp_path: Path) -> None:
    spec, root, completion = _sealed(tmp_path)
    widened = spec.admission_request.model_copy(
        update={"start_date": spec.adapter_request.query_start_date - timedelta(days=1)}
    )
    forged = FactorEvaluationJobSpec.model_construct(
        **{**spec.__dict__, "admission_request": widened}
    )
    forged_completion = completion.model_copy(update={"spec_sha256": forged.spec_sha256})
    assert forged_completion.spec_sha256 != completion.spec_sha256
    assert forged_completion.artifact_sha256 == completion.artifact_sha256
    assert (root / completion.artifact_filename).is_file()
    ledger = _ledger(tmp_path)
    with pytest.raises(ValidationError, match="admission"):
        ledger.submit("wider-admission", forged)


def test_ledger_clock_fences_expired_worker_despite_old_completion_time(tmp_path: Path) -> None:
    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = FactorEvaluationJobLedger(tmp_path / "factor-jobs.sqlite3", clock=clock)
    identity = ledger.initialize()
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=5)
    assert lease is not None
    clock.instant = NOW + timedelta(seconds=6)
    reopened = FactorEvaluationJobLedger.open_existing(identity, clock=clock)
    with pytest.raises(TypeError):
        reopened.complete(job.job_id, lease.lease_token, lease.version, completion, root, now=NOW)
    with pytest.raises(FactorLedgerLeaseError):
        reopened.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    with pytest.raises(FactorLedgerLeaseError):
        reopened.heartbeat(job.job_id, lease.lease_token, lease.version, lease_seconds=30)
    with pytest.raises(FactorLedgerLeaseError):
        reopened.fail(job.job_id, lease.lease_token, lease.version, "evaluation_failed")
    replacement = reopened.claim(lease_seconds=30)
    assert replacement is not None and replacement.lease_token != lease.lease_token


def test_completion_rechecks_clock_after_artifact_binding_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module

    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=5)
    assert lease is not None
    original = module._checked_completion

    def advance_after_validation(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)
        clock.instant = NOW + timedelta(seconds=6)

    monkeypatch.setattr(module, "_checked_completion", advance_after_validation)
    with pytest.raises(FactorLedgerLeaseError):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    assert ledger.get(job.job_id).status == "running"


def test_expiry_during_full_to_display_reprojection_keeps_job_running(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module

    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=5)
    assert lease is not None
    original = module.project_factor_display_artifact

    def advance_after_projection(*args: object, **kwargs: object) -> object:
        projected = original(*args, **kwargs)
        clock.instant = NOW + timedelta(seconds=6)
        return projected

    monkeypatch.setattr(module, "project_factor_display_artifact", advance_after_projection)
    with pytest.raises(FactorLedgerLeaseError):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    assert ledger.get(job.job_id).status == "running"


def test_missing_or_damaged_artifact_cannot_mark_success(tmp_path: Path) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    target = root / completion.artifact_filename
    data = target.read_bytes()
    target.unlink()
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    target.write_bytes(data[:20])
    target.chmod(0o600)
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    assert ledger.get(job.job_id).status == "running"


@pytest.mark.parametrize("shape", ("old", "partial"))
def test_new_success_rejects_old_or_partial_display_receipt(tmp_path: Path, shape: str) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    changed = {
        "display_artifact_sha256": None,
        "display_artifact_filename": None,
        "display_artifact_byte_count": None,
    }
    if shape == "partial":
        changed["display_artifact_sha256"] = completion.display_artifact_sha256
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(
            job.job_id,
            lease.lease_token,
            lease.version,
            completion.model_copy(update=changed),
            root,
        )
    assert ledger.get(job.job_id).status == "running"


@pytest.mark.parametrize("which", ("artifact_sha256", "display_artifact_sha256"))
def test_completion_cannot_swap_full_and_compact_file_identity(tmp_path: Path, which: str) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    swapped = (
        completion.display_artifact_sha256
        if which == "artifact_sha256"
        else completion.artifact_sha256
    )
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(
            job.job_id,
            lease.lease_token,
            lease.version,
            completion.model_copy(update={which: swapped}),
            root,
        )
    assert ledger.get(job.job_id).status == "running"


@pytest.mark.parametrize("damage", ("missing", "truncated", "symlink"))
def test_missing_or_damaged_display_cannot_mark_success(tmp_path: Path, damage: str) -> None:
    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    target = root / completion.display_artifact_filename
    data = target.read_bytes()
    target.unlink()
    if damage == "truncated":
        target.write_bytes(data[:20])
        target.chmod(0o600)
    elif damage == "symlink":
        target.symlink_to(root / completion.artifact_filename)
    with pytest.raises(FactorLedgerCompletionError):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    assert ledger.get(job.job_id).status == "running"


def test_self_consistent_changed_display_is_rejected_against_complete_result(
    tmp_path: Path,
) -> None:
    spec, root, completion = _sealed(tmp_path)
    display = load_factor_display_artifact(root, completion.display_artifact_sha256)
    payload = display.model_dump(mode="json", exclude={"content_sha256"})
    payload["summary_status"] = "no_samples"
    forged = FactorDisplayArtifactV1.model_validate_json(
        canonical_json_bytes({**payload, "content_sha256": _digest(payload)}), strict=False
    )
    name = f"factor-display-v1-{forged.content_sha256}.json"
    data = canonical_json_bytes(forged.model_dump(mode="json", round_trip=True))
    target = root / name
    target.write_bytes(data)
    target.chmod(0o600)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    with pytest.raises(FactorLedgerCompletionError, match="display differs"):
        ledger.complete(
            job.job_id,
            lease.lease_token,
            lease.version,
            completion.model_copy(
                update={
                    "display_artifact_sha256": forged.content_sha256,
                    "display_artifact_filename": name,
                    "display_artifact_byte_count": len(data),
                }
            ),
            root,
        )
    assert ledger.get(job.job_id).status == "running"


@pytest.mark.parametrize("which", ("artifact", "display_artifact"))
def test_same_bytes_file_generation_change_during_verification_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, which: str
) -> None:
    from rquant.factor import job_ledger as module

    spec, root, completion = _sealed(tmp_path)
    ledger = _ledger(tmp_path)
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    target = root / getattr(completion, f"{which}_filename")
    original = module._checked_completion

    def replace_after_validation(*args: object, **kwargs: object) -> None:
        original(*args, **kwargs)
        data = target.read_bytes()
        target.unlink()
        target.write_bytes(data)
        target.chmod(0o600)

    monkeypatch.setattr(module, "_checked_completion", replace_after_validation)
    with pytest.raises(FactorLedgerCompletionError, match="changed before completion"):
        ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
    assert ledger.get(job.job_id).status == "running"


def test_historical_success_without_display_fields_stays_readable(tmp_path: Path) -> None:
    from rquant.factor.job_ledger import _COLUMNS

    spec, _root, completion = _sealed(tmp_path)
    ledger = FactorEvaluationJobLedger(tmp_path / "factor-jobs.sqlite3", clock=_Clock())
    identity = ledger.initialize()
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    old = canonical_json_bytes(
        completion.model_dump(
            mode="json",
            round_trip=True,
            exclude={
                "display_artifact_sha256",
                "display_artifact_filename",
                "display_artifact_byte_count",
            },
        )
    ).decode("utf-8")
    with sqlite3.connect(ledger.path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            "SELECT * FROM factor_jobs WHERE job_id = ?", (job.job_id,)
        ).fetchone()
        payload = {column: row[column] for column in _COLUMNS}
        payload.update(
            status="succeeded",
            version=lease.version + 1,
            updated_at=NOW.isoformat(timespec="microseconds"),
            lease_token=None,
            lease_expires_at=None,
            completion_json=old,
        )
        connection.execute(
            "UPDATE factor_jobs SET status = ?, version = ?, updated_at = ?, lease_token = NULL, "
            "lease_expires_at = NULL, completion_json = ?, row_sha256 = ? WHERE job_id = ?",
            (
                payload["status"],
                payload["version"],
                payload["updated_at"],
                old,
                _digest(payload),
                job.job_id,
            ),
        )
    reopened = FactorEvaluationJobLedger.open_existing(identity, clock=_Clock())
    historical = reopened.get(job.job_id)
    assert historical.status == "succeeded"
    assert historical.completion.display_status == "display_unavailable"
    assert historical.completion.display_artifact_sha256 is None


def test_sealed_artifact_survives_transaction_failure_then_reclaim(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger as module

    spec, root, completion = _sealed(tmp_path)
    clock = _Clock()
    ledger = _ledger(tmp_path, clock=clock)
    job = ledger.submit("request-1", spec)
    first = ledger.claim(lease_seconds=5)
    assert first is not None
    original = module.FactorEvaluationJobLedger._store_job

    def fail_once(*args: object, **kwargs: object) -> None:
        raise RuntimeError("injected transaction failure")

    monkeypatch.setattr(module.FactorEvaluationJobLedger, "_store_job", fail_once)
    with pytest.raises(RuntimeError, match="injected transaction failure"):
        ledger.complete(job.job_id, first.lease_token, first.version, completion, root)
    monkeypatch.setattr(module.FactorEvaluationJobLedger, "_store_job", staticmethod(original))
    assert ledger.get(job.job_id).status == "running"
    assert (root / completion.artifact_filename).is_file()
    clock.instant = NOW + timedelta(seconds=6)
    second = ledger.claim(lease_seconds=60)
    assert second is not None and second.lease_token != first.lease_token
    assert (
        ledger.complete(
            job.job_id,
            second.lease_token,
            second.version,
            completion,
            root,
        ).status
        == "succeeded"
    )


def test_external_identity_rejects_replaced_file_on_reopen_and_live_access(tmp_path: Path) -> None:
    path = tmp_path / "factor-jobs.sqlite3"
    ledger = FactorEvaluationJobLedger(path, clock=_Clock())
    identity = ledger.initialize()
    _, spec = _research_and_spec()
    job = ledger.submit("request-1", spec)
    path.rename(tmp_path / "old.sqlite3")
    replacement = FactorEvaluationJobLedger(path)
    replacement.initialize()
    with pytest.raises(FactorLedgerIdentityError):
        FactorEvaluationJobLedger.open_existing(identity)
    with pytest.raises(FactorLedgerIdentityError):
        ledger.get(job.job_id)
    with pytest.raises(FactorLedgerIdentityError):
        ledger.submit("request-2", spec)
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
        job = ledger.submit("request-1", spec)
        with sqlite3.connect(ledger.path) as connection:
            connection.execute(
                f"UPDATE factor_jobs SET {column} = ? WHERE job_id = ?", (value, job.job_id)
            )
        with pytest.raises(FactorLedgerIntegrityError):
            ledger.get(job.job_id)
        with pytest.raises(FactorLedgerIntegrityError):
            ledger.list_recent(limit=10)
        with pytest.raises(FactorLedgerIntegrityError):
            ledger.submit("request-1", spec)


def test_tampered_command_type_is_rejected_as_integrity_failure(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _, spec = _research_and_spec()
    ledger.submit("request-1", spec)
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            "UPDATE factor_commands SET spec_sha256 = ? WHERE command_id = ?",
            (sqlite3.Binary(b"f" * 64), "request-1"),
        )
    with pytest.raises(FactorLedgerIntegrityError):
        ledger.submit("request-1", spec)


def test_failure_reason_and_list_budget_are_bounded(tmp_path: Path) -> None:
    ledger = _ledger(tmp_path)
    _, spec = _research_and_spec()
    job = ledger.submit("request-1", spec)
    lease = ledger.claim(lease_seconds=60)
    assert lease is not None
    with pytest.raises(ValueError):
        ledger.fail(job.job_id, lease.lease_token, lease.version, "/private/secret")
    failed = ledger.fail(job.job_id, lease.lease_token, lease.version, "evaluation_failed")
    assert failed.status == "failed" and failed.failure_code == "evaluation_failed"
    assert failed.completion is None
    for limit in (0, 201):
        with pytest.raises(ValueError):
            ledger.list_recent(limit=limit)
