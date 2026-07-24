from __future__ import annotations

import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from rquant.lab_job_protocol import InvalidCommandEnvelopeError, RequestContentConflictError
from rquant.lab_shard_protocol import (
    LabClaimSpool,
    LabClaimSupersededError,
    LabReportReceipt,
    LabReportSpool,
    LabShardClaim,
    LabShardDefinition,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabWorkerReport,
    LabWorkerStopped,
)

NOW = datetime(2026, 7, 24, 2, 0, tzinfo=UTC)
PLAN_HASH = "1" * 64
SPEC_HASH = "2" * 64


def _definition(*, index: int = 0, payload_json: str = '{"hold_days":3}') -> LabShardDefinition:
    return LabShardDefinition.from_payload(
        shard_index=index,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash=PLAN_HASH,
        payload_json=payload_json,
    )


def _claim(
    *,
    definition: LabShardDefinition | None = None,
    generation: int = 1,
    fence: int = 7,
) -> LabShardClaim:
    return LabShardClaim(
        job_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        spec_hash=SPEC_HASH,
        definition=definition or _definition(),
        worker_id="worker-a",
        claim_token=uuid4(),
        claim_generation=generation,
        scheduler_fencing_token=fence,
        claimed_at=NOW,
        lease_expires_at=NOW + timedelta(minutes=5),
    )


def _report(
    claim: LabShardClaim,
    body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
    *,
    report_id: UUID | None = None,
) -> LabWorkerReport:
    return LabWorkerReport.from_claim(
        claim,
        report_id=report_id or uuid4(),
        reported_at=NOW + timedelta(seconds=5),
        body=body,
    )


def test_definition_has_deterministic_identity_and_canonical_payload() -> None:
    first = _definition(payload_json=' { "hold_days" : 3, "label" : "x" } ')
    second = _definition(payload_json='{"label":"x","hold_days":3}')

    assert first == second
    assert first.payload_json == '{"hold_days":3,"label":"x"}'
    assert first.shard_id == second.shard_id
    assert first.payload_hash == second.payload_hash
    assert _definition(index=1).shard_id != first.shard_id


@pytest.mark.parametrize(
    "payload",
    [
        '{"score":1.25}',
        '{"score":NaN}',
        '{"nested":[1,2.0]}',
    ],
)
def test_definition_rejects_raw_float_nan_and_nested_float(payload: str) -> None:
    with pytest.raises(ValidationError, match="floating-point|finite JSON"):
        _definition(payload_json=payload)


def test_definition_rejects_tampered_shard_or_payload_hash() -> None:
    original = _definition()
    raw = original.model_dump(mode="json")
    raw["payload_hash"] = "f" * 64
    with pytest.raises(ValidationError, match="payload_hash"):
        LabShardDefinition.model_validate(raw)

    raw = original.model_dump(mode="json")
    raw["shard_id"] = str(uuid4())
    with pytest.raises(ValidationError, match="shard_id"):
        LabShardDefinition.model_validate(raw)


def test_claim_is_frozen_revalidated_and_rejects_bad_lease() -> None:
    claim = _claim()
    assert claim.shard_id == claim.definition.shard_id
    with pytest.raises(ValidationError, match="lease_expires_at"):
        LabShardClaim.model_validate(
            {**claim.model_dump(mode="json"), "lease_expires_at": NOW.isoformat()}
        )
    with pytest.raises(ValidationError):
        LabShardClaim.model_validate({**claim.model_dump(mode="json"), "extra": 1})


@pytest.mark.parametrize(
    "body",
    [
        LabShardHeartbeat(lease_extension_seconds=30),
        LabShardSucceeded(result_manifest_hash="3" * 64),
        LabShardFailed(failure_json='{"code":"boom","retryable":true}'),
        LabWorkerStopped(reason="cancel observed"),
    ],
)
def test_report_union_roundtrip_and_content_hash_tamper_detection(
    body: LabShardHeartbeat | LabShardSucceeded | LabShardFailed | LabWorkerStopped,
) -> None:
    report = _report(_claim(), body)
    parsed = LabWorkerReport.model_validate_json(report.model_dump_json())
    assert parsed == report

    tampered = json.loads(report.model_dump_json())
    tampered["content_hash"] = "f" * 64
    with pytest.raises(ValidationError, match="content_hash"):
        LabWorkerReport.model_validate(tampered)


def test_failed_report_canonicalizes_failure_and_rejects_float() -> None:
    failed = LabShardFailed(failure_json=' { "retryable": true, "code": "boom" } ')
    assert failed.failure_json == '{"code":"boom","retryable":true}'
    with pytest.raises(ValidationError, match="floating-point"):
        LabShardFailed(failure_json='{"loss":0.1}')


def test_heartbeat_rejects_extension_above_strict_scheduler_bound() -> None:
    with pytest.raises(ValidationError, match="less than or equal"):
        LabShardHeartbeat(lease_extension_seconds=3_601)


def test_claim_spool_is_no_clobber_and_reader_detects_tamper(tmp_path: Path) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()

    with ThreadPoolExecutor(max_workers=8) as executor:
        entries = tuple(executor.map(spool.publish, (claim,) * 16))
    assert len({entry.path for entry in entries}) == 1
    assert spool.pending()[0].claim == claim

    replacement = entries[0].path.with_suffix(".replacement")
    replacement.write_text(entries[0].path.read_text(encoding="utf-8"), encoding="utf-8")
    os.replace(replacement, entries[0].path)
    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.consume(entries[0])


def test_claim_spool_persists_exact_high_water_across_consume_and_restart(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    old = _claim(generation=1)
    old_entry = spool.publish(old)
    current = old.model_copy(
        update={
            "worker_id": "worker-b",
            "claim_token": uuid4(),
            "claim_generation": 2,
            "claimed_at": NOW + timedelta(minutes=1),
            "lease_expires_at": NOW + timedelta(minutes=6),
        }
    )
    current_entry = spool.publish(current)

    restarted = LabClaimSpool(root)
    assert restarted.current(old.job_id, old.shard_id).claim == current
    with pytest.raises(LabClaimSupersededError):
        restarted.consume(old_entry)

    assert restarted.consume(current_entry) == current
    assert LabClaimSpool(root).current(old.job_id, old.shard_id).claim == current
    with pytest.raises(LabClaimSupersededError):
        LabClaimSpool(root).publish(old)


def test_claim_spool_rejects_same_generation_with_different_token(tmp_path: Path) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    spool.publish(claim)

    with pytest.raises(LabClaimSupersededError):
        spool.publish(claim.model_copy(update={"claim_token": uuid4()}))


def test_claim_pending_failure_never_advances_current_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()

    def fail_pending(_target: Path, _payload: bytes) -> bool:
        raise OSError("injected pending write failure")

    monkeypatch.setattr(spool, "_publish_no_clobber", fail_pending)

    with pytest.raises(OSError, match="pending write"):
        spool.publish(claim)

    assert spool.pending() == ()
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.current(claim.job_id, claim.shard_id)


def test_claim_current_failure_leaves_unconsumable_repairable_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    original_publish_current = spool._publish_current_locked
    failed = False

    def fail_current_once(marker: object) -> None:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("injected current write failure")
        original_publish_current(marker)

    monkeypatch.setattr(spool, "_publish_current_locked", fail_current_once)

    with pytest.raises(OSError, match="current write"):
        spool.publish(claim)

    pending = spool.pending()
    assert len(pending) == 1
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.current(claim.job_id, claim.shard_id)
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.consume(pending[0])

    repaired = spool.publish(claim)

    assert repaired.path == pending[0].path
    assert spool.current(claim.job_id, claim.shard_id).claim == claim
    assert spool.consume(repaired) == claim


def test_consumed_claim_republish_is_idempotent_without_second_delivery(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    entry = spool.publish(claim)
    assert spool.consume(entry) == claim

    replay = LabClaimSpool(root).publish(claim)

    assert replay.receipt.claim == claim
    assert LabClaimSpool(root).pending() == ()
    assert len(tuple(LabClaimSpool(root).ack_dir.glob("*.json"))) == 1


def test_claim_receipt_failure_keeps_pending_deliverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    entry = spool.publish(claim)
    original_publish = spool._publish_no_clobber

    def fail_receipt(target: Path, payload: bytes) -> bool:
        if target.parent == spool.ack_dir:
            raise OSError("injected receipt write failure")
        return original_publish(target, payload)

    monkeypatch.setattr(spool, "_publish_no_clobber", fail_receipt)
    with pytest.raises(OSError, match="receipt write"):
        spool.consume(entry)

    assert spool.pending() == (entry,)
    assert tuple(spool.ack_dir.glob("*.json")) == ()
    monkeypatch.setattr(spool, "_publish_no_clobber", original_publish)
    assert spool.consume(entry) == claim


def test_claim_unlink_failure_recovers_consumed_receipt_without_redelivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    entry = spool.publish(claim)
    original_unlink = spool._unlink_pending

    def fail_unlink(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected pending unlink failure")

    monkeypatch.setattr(spool, "_unlink_pending", fail_unlink)
    with pytest.raises(OSError, match="pending unlink"):
        spool.consume(entry)

    assert len(tuple(spool.ack_dir.glob("*.json"))) == 1
    assert len(spool.pending()) == 1
    restarted = LabClaimSpool(root)
    with pytest.raises(Exception, match="already consumed"):
        restarted.consume(restarted.pending()[0])

    assert restarted.pending() == ()
    replay = restarted.publish(claim)
    assert replay.receipt.claim == claim
    assert original_unlink is not None


def test_reclaim_hook_failure_never_changes_successful_delivery_semantics(
    tmp_path: Path,
) -> None:
    attempts = 0

    def flaky_hook(_claim: LabShardClaim) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected reclaim failure")

    spool = LabClaimSpool(tmp_path / "claims", claim_advance_hook=flaky_hook)
    claim = _claim()

    entry = spool.publish(claim)
    first = spool.reconcile_current()
    second = spool.reconcile_current()

    assert entry.claim == claim
    assert spool.current(claim.job_id, claim.shard_id).claim == claim
    assert first[0].status == "failed"
    assert "RuntimeError" in first[0].error
    assert second[0].status == "reconciled"
    assert attempts == 2


def test_revoke_removes_exact_pending_and_current_and_blocks_republish(
    tmp_path: Path,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    entry = spool.publish(claim)

    revoked = spool.revoke(claim, reason="lease exhausted")

    assert revoked.receipt.status == "revoked"
    assert revoked.receipt.reason == "lease exhausted"
    assert spool.pending() == ()
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.current(claim.job_id, claim.shard_id)
    replay = spool.publish(claim)
    assert replay.receipt.status == "revoked"
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.consume(entry)


def test_revoke_cleans_pending_only_after_current_publish_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    original_publish_current = spool._publish_current_locked

    def fail_current(_marker: object) -> None:
        raise OSError("injected current failure")

    monkeypatch.setattr(spool, "_publish_current_locked", fail_current)
    with pytest.raises(OSError, match="current failure"):
        spool.publish(claim)
    assert len(spool.pending()) == 1
    monkeypatch.setattr(spool, "_publish_current_locked", original_publish_current)

    revoked = spool.revoke(claim, reason="sqlite terminal")

    assert revoked.receipt.status == "revoked"
    assert spool.pending() == ()
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.current(claim.job_id, claim.shard_id)


def test_revoke_upgrades_consumed_receipt_and_fences_current(tmp_path: Path) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    assert spool.consume(spool.publish(claim)) == claim

    revoked = LabClaimSpool(root).revoke(claim, reason="scheduler takeover")

    assert revoked.receipt.status == "revoked"
    assert not LabClaimSpool(root).is_current(claim)
    assert LabClaimSpool(root).publish(claim).receipt.status == "revoked"


def test_revoke_receipt_failure_is_retryable_without_partial_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    spool.publish(claim)
    original_publish = spool._publish_no_clobber

    def fail_receipt(target: Path, payload: bytes) -> bool:
        if target.parent == spool.ack_dir:
            raise OSError("injected revoke receipt failure")
        return original_publish(target, payload)

    monkeypatch.setattr(spool, "_publish_no_clobber", fail_receipt)
    with pytest.raises(OSError, match="revoke receipt"):
        spool.revoke(claim, reason="expired")

    assert len(spool.pending()) == 1
    assert spool.is_current(claim)
    monkeypatch.setattr(spool, "_publish_no_clobber", original_publish)
    assert spool.revoke(claim, reason="expired").receipt.status == "revoked"
    assert spool.pending() == ()


def test_revoke_receipt_fences_before_unlink_and_current_removal_retries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    spool.publish(claim)
    original_unlink = spool._unlink_pending

    def fail_unlink(*_args: object, **_kwargs: object) -> None:
        raise OSError("injected revoke unlink failure")

    monkeypatch.setattr(spool, "_unlink_pending", fail_unlink)
    with pytest.raises(OSError, match="revoke unlink"):
        spool.revoke(claim, reason="expired")

    assert not spool.is_current(claim)
    assert len(spool.pending()) == 1
    assert spool.publish(claim).receipt.status == "revoked"
    monkeypatch.setattr(spool, "_unlink_pending", original_unlink)
    assert LabClaimSpool(root).revoke(claim, reason="expired").receipt.status == "revoked"
    assert LabClaimSpool(root).pending() == ()


def test_revoke_current_removal_failure_is_fenced_and_retryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    spool.publish(claim)
    original_unlink_current = spool._unlink_current_locked

    def fail_current(_claim: LabShardClaim) -> None:
        raise OSError("injected current removal failure")

    monkeypatch.setattr(spool, "_unlink_current_locked", fail_current)
    with pytest.raises(OSError, match="current removal"):
        spool.revoke(claim, reason="expired")

    assert not spool.is_current(claim)
    assert spool.current(claim.job_id, claim.shard_id).claim == claim
    assert spool.publish(claim).receipt.status == "revoked"
    monkeypatch.setattr(spool, "_unlink_current_locked", original_unlink_current)
    LabClaimSpool(root).revoke(claim, reason="expired")
    with pytest.raises(InvalidCommandEnvelopeError):
        LabClaimSpool(root).current(claim.job_id, claim.shard_id)
    assert LabClaimSpool(root).pending() == ()


def test_claim_receipt_hardlink_fails_closed_without_unlinking(tmp_path: Path) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    spool.consume(spool.publish(claim))
    receipt = spool.ack_dir / f"{claim.claim_token}.json"
    external = tmp_path / "external-receipt.json"
    os.link(receipt, external)

    with pytest.raises(InvalidCommandEnvelopeError, match="hard link"):
        spool.publish(claim)

    assert receipt.is_file()
    assert external.is_file()
    assert receipt.stat().st_nlink == 2


def test_report_spool_exactly_once_ack_restart_and_conflict(tmp_path: Path) -> None:
    root = tmp_path / "reports"
    spool = LabReportSpool(root)
    claim = _claim()
    report = _report(claim, LabShardSucceeded(result_manifest_hash="3" * 64))
    entry = spool.publish(report)
    receipt = LabReportReceipt.from_report(
        report,
        status="accepted",
        reason="shard_succeeded",
        accepted_at=NOW + timedelta(seconds=6),
    )
    acknowledged = spool.ack(entry, receipt)

    restarted = LabReportSpool(root)
    duplicate = restarted.publish(report)
    assert duplicate == acknowledged
    assert restarted.pending() == ()
    assert restarted.load_receipt(acknowledged.path) == receipt

    conflict = report.model_copy(
        update={"body": LabShardFailed(failure_json='{"code":"different"}')}
    )
    with pytest.raises((ValidationError, RequestContentConflictError)):
        restarted.publish(conflict)


def test_success_receipt_carries_attempt_and_manifest_identity() -> None:
    claim = _claim(generation=2, fence=9)
    report = _report(claim, LabShardSucceeded(result_manifest_hash="3" * 64))

    receipt = LabReportReceipt.from_report(
        report,
        status="accepted",
        reason="shard_succeeded",
        accepted_at=NOW + timedelta(seconds=6),
    )

    assert receipt.worker_id == claim.worker_id
    assert receipt.claim_token == claim.claim_token
    assert receipt.claim_generation == claim.claim_generation
    assert receipt.scheduler_fencing_token == claim.scheduler_fencing_token
    assert receipt.report_type == "shard_succeeded"
    assert receipt.result_manifest_hash == "3" * 64


def test_report_commit_before_ack_replay_keeps_same_typed_receipt(tmp_path: Path) -> None:
    spool = LabReportSpool(tmp_path / "reports")
    report = _report(_claim(), LabShardHeartbeat(lease_extension_seconds=15))
    entry = spool.publish(report)

    # Simulates scheduler commit followed by a crash before filesystem ack.
    receipt = LabReportReceipt.from_report(
        report,
        status="accepted",
        reason="heartbeat_extended",
        accepted_at=NOW + timedelta(seconds=6),
    )
    replayed_entry = LabReportSpool(spool.root).load(entry.path)
    assert replayed_entry.report == report
    assert LabReportSpool(spool.root).ack(replayed_entry, receipt).receipt == receipt


def test_malformed_and_symlink_report_do_not_block_later_report(tmp_path: Path) -> None:
    spool = LabReportSpool(tmp_path / "reports")
    bad = spool.pending_dir / f"00000000000000000001-{uuid4()}.json"
    bad.write_text("{broken", encoding="utf-8")
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")
    link = spool.pending_dir / f"00000000000000000002-{uuid4()}.json"
    link.symlink_to(outside)
    good = spool.publish(_report(_claim(), LabShardHeartbeat(lease_extension_seconds=10)))

    paths = spool.pending_paths()
    assert bad in paths and link in paths and good.path in paths
    with pytest.raises(InvalidCommandEnvelopeError) as bad_error:
        spool.load(bad)
    spool.quarantine(bad_error.value.file_identity or bad, reason="invalid_json")
    with pytest.raises(InvalidCommandEnvelopeError) as link_error:
        spool.load(link)
    assert link_error.value.file_identity is not None
    spool.quarantine(link_error.value.file_identity, reason="symlink")
    assert spool.load(good.path).report == good.report


def test_report_spool_cross_process_publish_and_restart_is_fifo(tmp_path: Path) -> None:
    root = tmp_path / "reports"
    claim = _claim()
    reports = tuple(
        _report(claim, LabShardHeartbeat(lease_extension_seconds=10 + index)) for index in range(4)
    )
    script = """
import sys
from pathlib import Path
from rquant.lab_shard_protocol import LabReportSpool, LabWorkerReport
entry = LabReportSpool(Path(sys.argv[1])).publish(
    LabWorkerReport.model_validate_json(sys.argv[2])
)
print(entry.path.name)
"""
    processes = tuple(
        subprocess.Popen(
            [sys.executable, "-c", script, str(root), report.model_dump_json()],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for report in reports
    )
    outputs = tuple(process.communicate(timeout=10) for process in processes)
    assert all(process.returncode == 0 for process in processes), outputs
    names = [stdout.strip() for stdout, _ in outputs]
    assert len({int(name.split("-", 1)[0]) for name in names}) == len(reports)
    pending = LabReportSpool(root).pending()
    assert tuple(int(entry.path.name.split("-", 1)[0]) for entry in pending) == tuple(
        sorted(int(name.split("-", 1)[0]) for name in names)
    )
