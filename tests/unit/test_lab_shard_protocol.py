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
    LabAdmittedExecution,
    LabClaimDeliveryReceipt,
    LabClaimNotConsumedError,
    LabClaimRevokedError,
    LabClaimSpool,
    LabClaimSupersededError,
    LabExecutionAdmission,
    LabReportReceipt,
    LabReportSpool,
    LabRevokedClaim,
    LabShardClaim,
    LabShardDefinition,
    LabShardFailed,
    LabShardHeartbeat,
    LabShardSucceeded,
    LabShardTelemetry,
    LabShardWorkPlan,
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


def test_definition_roundtrips_typed_work_plan_and_legacy_definition_stays_optional() -> None:
    work_plan = LabShardWorkPlan(
        phase="strategy_replay",
        work_unit_name="parameter_case",
        work_units=4,
        static_duration_ms=12_000,
    )
    definition = LabShardDefinition.from_payload(
        shard_index=0,
        adapter_id="n-shape-replay",
        adapter_version="v1",
        plan_hash=PLAN_HASH,
        payload_json='{"hold_days":3}',
        work_plan=work_plan,
    )

    assert definition.work_plan == work_plan
    assert LabShardDefinition.model_validate_json(definition.model_dump_json()) == definition
    assert _definition().work_plan is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("work_units", 0),
        ("static_duration_ms", 0),
        ("phase", ""),
        ("work_unit_name", ""),
    ],
)
def test_work_plan_rejects_nonpositive_or_empty_fields(field: str, value: object) -> None:
    payload = {
        "phase": "strategy_replay",
        "work_unit_name": "parameter_case",
        "work_units": 4,
        "static_duration_ms": 12_000,
    }
    payload[field] = value

    with pytest.raises(ValidationError):
        LabShardWorkPlan.model_validate(payload)


@pytest.mark.parametrize("duration", [0.0, -1.0, float("nan"), float("inf")])
def test_success_telemetry_fails_closed_on_invalid_monotonic_duration(duration: float) -> None:
    with pytest.raises(ValidationError):
        LabShardTelemetry(
            phase="strategy_replay",
            work_unit_name="parameter_case",
            work_units=4,
            static_duration_ms=12_000,
            duration_ms=duration,
            throughput_units_per_second=1.0,
        )


def test_success_telemetry_rejects_inconsistent_throughput() -> None:
    with pytest.raises(ValidationError, match="throughput"):
        LabShardTelemetry(
            phase="strategy_replay",
            work_unit_name="parameter_case",
            work_units=4,
            static_duration_ms=12_000,
            duration_ms=2_000,
            throughput_units_per_second=99,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("duration_ms", "2000"),
        ("throughput_units_per_second", "2"),
    ],
)
def test_success_telemetry_rejects_numeric_strings(field: str, value: str) -> None:
    payload = {
        "phase": "strategy_replay",
        "work_unit_name": "parameter_case",
        "work_units": 4,
        "static_duration_ms": 12_000,
        "duration_ms": 2_000,
        "throughput_units_per_second": 2,
    }
    payload[field] = value

    with pytest.raises(ValidationError):
        LabShardTelemetry.model_validate(payload)


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


def test_hot_claim_batches_are_fair_bounded_and_ignore_cold_consumed_history(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claims = tuple(_claim(definition=_definition(index=index)) for index in range(4))
    for claim in claims:
        spool.publish(claim)
    consumed = _claim(definition=_definition(index=10))
    spool.consume(spool.publish(consumed))
    revoked = _claim(definition=_definition(index=11))
    spool.revoke(spool.publish(revoked).claim, reason="terminal fixture")

    batches = tuple(LabClaimSpool(root).hot_delivery_batch(limit=2) for _ in range(8))
    observed_tokens = {claim.claim_token for batch in batches for claim in batch.claims}
    observed_namespaces = {namespace for batch in batches for namespace in batch.scanned_namespaces}

    assert {claim.claim_token for claim in claims}.issubset(observed_tokens)
    assert consumed.claim_token in observed_tokens
    assert revoked.claim_token in observed_tokens
    assert observed_namespaces == {"pending", "current", "revoked"}
    always_hot = len(tuple(spool.current_dir.iterdir())) + len(tuple(spool.revoked_dir.iterdir()))
    assert all(batch.inspected <= always_hot + 2 for batch in batches)


@pytest.mark.parametrize("namespace", ["current", "revoked"])
def test_uuid_hot_authority_cannot_starve_across_restart_and_continuous_insert(
    tmp_path: Path,
    namespace: str,
) -> None:
    root = tmp_path / "claims"
    old = _claim(definition=_definition(index=20_000)).model_copy(
        update={
            "job_id": UUID(int=(1 << 128) - 2),
            "claim_token": UUID(int=(1 << 128) - 2),
        }
    )
    spool = LabClaimSpool(root)
    if namespace == "current":
        spool.consume(spool.publish(old))
    else:
        spool.revoke(old, reason="old unreconciled fixture")

    observed_at: int | None = None
    for index in range(1, 121):
        fresh = _claim(definition=_definition(index=index)).model_copy(
            update={
                "job_id": UUID(int=index),
                "claim_token": UUID(int=index),
            }
        )
        restarted = LabClaimSpool(root)
        if namespace == "current":
            restarted.consume(restarted.publish(fresh))
        else:
            restarted.revoke(fresh, reason="new unreconciled fixture")
        batch = LabClaimSpool(root).hot_delivery_batch(limit=1)
        if observed_at is None and old.claim_token in {claim.claim_token for claim in batch.claims}:
            observed_at = index

    assert observed_at is not None, f"old {namespace} authority was starved by newer UUIDs"
    assert observed_at <= 3


def test_pending_hot_cursor_advances_only_by_monotonic_delivery_sequence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    for index in range(4):
        spool.publish(_claim(definition=_definition(index=index)))

    cursors = tuple(LabClaimSpool(root).hot_delivery_batch(limit=1).next_cursor for _ in range(4))

    assert [cursor.after_sequence for cursor in cursors] == [1, 2, 3, 4]
    assert {cursor.cycle_ceiling_sequence for cursor in cursors} == {4}


def test_hot_claim_scan_does_not_glob_or_parse_cold_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    for index in range(10_000):
        (spool.ack_dir / f"{UUID(int=index + 1)}.json").write_bytes(b"{}")
        (spool.archived_revoked_dir / f"{UUID(int=index + 20_000)}.json").write_bytes(b"{}")

    def reject_glob(_self: Path, _pattern: str) -> tuple[Path, ...]:
        raise AssertionError("hot authority scan must use bounded namespace scandir")

    def reject_cold_parse(_token: UUID) -> object:
        raise AssertionError("hot authority scan must not parse cold claim history")

    monkeypatch.setattr(Path, "glob", reject_glob)
    monkeypatch.setattr(spool, "_load_consumed_locked", reject_cold_parse)
    monkeypatch.setattr(spool, "_load_archived_revocation_locked", reject_cold_parse)

    batch = spool.hot_delivery_batch(limit=2)

    assert batch.claims == ()
    assert batch.inspected == 0
    assert batch.scanned_namespaces == ()


def test_retire_removes_hot_current_but_preserves_exact_high_water(tmp_path: Path) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    spool.consume(spool.publish(claim))
    spool.admit_execution(claim)

    retired = spool.retire(
        claim,
        outcome="accepted",
        reason="scheduler accepted shard success",
    )

    assert retired.claim == claim
    assert spool.pending() == ()
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.current(claim.job_id, claim.shard_id)
    assert spool.retired_high_water(claim.job_id, claim.shard_id) == retired
    assert spool.publish(claim).receipt.claim == claim
    stale = claim.model_copy(update={"claim_token": uuid4()})
    with pytest.raises(LabClaimSupersededError):
        spool.publish(stale)
    advanced = claim.model_copy(
        update={
            "claim_token": uuid4(),
            "claim_generation": claim.claim_generation + 1,
            "scheduler_fencing_token": claim.scheduler_fencing_token + 1,
        }
    )
    assert spool.publish(advanced).claim == advanced


def test_revocation_stays_hot_until_retired_then_moves_to_cold_archive(
    tmp_path: Path,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    spool.publish(claim)
    spool.revoke(claim, reason="sqlite terminal fixture")

    interrupted = spool.hot_delivery_batch(limit=8)

    assert interrupted.claims == (claim,)
    assert tuple(spool.revoked_dir.glob("*.json"))

    retired = spool.retire(
        claim,
        outcome="revoked",
        reason="sqlite terminal fixture",
    )

    assert retired.outcome == "revoked"
    assert spool.hot_delivery_batch(limit=8).claims == ()
    assert tuple(spool.revoked_dir.glob("*.json")) == ()
    assert spool.revocation(claim.claim_token).revocation.claim == claim
    assert tuple(spool.archived_revoked_dir.glob("*.json"))


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

    assert revoked.revocation.reason == "lease exhausted"
    assert spool.pending() == ()
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.current(claim.job_id, claim.shard_id)
    replay = spool.publish(claim)
    assert isinstance(replay, LabRevokedClaim)
    assert replay.revocation == revoked.revocation
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

    assert revoked.revocation.reason == "sqlite terminal"
    assert spool.pending() == ()
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.current(claim.job_id, claim.shard_id)


def test_revoke_preserves_immutable_consumed_receipt_and_fences_current(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    assert spool.consume(spool.publish(claim)) == claim
    consumed_path = spool.ack_dir / f"{claim.claim_token}.json"
    consumed_payload = consumed_path.read_bytes()

    revoked = LabClaimSpool(root).revoke(claim, reason="scheduler takeover")

    assert consumed_path.read_bytes() == consumed_payload
    assert LabClaimSpool(root)._load_consumed_locked(claim.claim_token).receipt.status == (
        "consumed"
    )
    assert revoked.revocation.reason == "scheduler takeover"
    assert LabClaimSpool(root).revocation(claim.claim_token) == revoked
    assert not LabClaimSpool(root).is_current(claim)
    assert isinstance(LabClaimSpool(root).publish(claim), LabRevokedClaim)


def test_legacy_revoked_delivery_receipt_remains_a_compatible_fence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    legacy = LabClaimDeliveryReceipt(
        status="revoked",
        claim=claim,
        reason="legacy scheduler revoke",
    )
    path = spool.ack_dir / f"{claim.claim_token}.json"
    path.write_text(legacy.model_dump_json(), encoding="utf-8")
    payload = path.read_bytes()

    replay = LabClaimSpool(root).publish(claim)
    migrated = LabClaimSpool(root).revoke(claim, reason="legacy scheduler revoke")

    assert isinstance(replay, LabRevokedClaim)
    assert replay.revocation.reason == "legacy scheduler revoke"
    assert migrated.path.parent == spool.revoked_dir
    assert path.read_bytes() == payload
    assert LabClaimSpool(root).is_revoked(claim)


def test_consume_checks_independent_revocation_before_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    entry = spool.publish(claim)
    original_unlink = spool._unlink_pending
    monkeypatch.setattr(
        spool,
        "_unlink_pending",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            OSError("leave revoked pending for admission test")
        ),
    )
    with pytest.raises(OSError, match="leave revoked pending"):
        spool.revoke(claim, reason="scheduler terminal")
    monkeypatch.setattr(spool, "_unlink_pending", original_unlink)

    with pytest.raises(LabClaimRevokedError):
        spool.consume(entry)

    assert spool.pending() == ()
    assert spool.is_revoked(claim)


def test_execution_admission_is_durable_exact_and_idempotent(tmp_path: Path) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    spool.consume(spool.publish(claim))

    admitted = spool.admit_execution(claim)
    repeated = spool.admit_execution(claim)
    restarted = LabClaimSpool(root).execution_admission(claim.claim_token)

    assert isinstance(admitted, LabAdmittedExecution)
    assert repeated == admitted
    assert restarted == admitted
    assert admitted.admission.claim == claim
    assert (
        admitted.admission.delivery_content_hash
        == spool._load_consumed_locked(claim.claim_token).receipt.content_hash
    )


def test_execution_admission_requires_consumed_current_unrevoked_claim(
    tmp_path: Path,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    unconsumed = _claim()
    spool.publish(unconsumed)

    with pytest.raises(LabClaimNotConsumedError):
        spool.admit_execution(unconsumed)

    consumed = _claim(definition=_definition(index=1))
    spool.consume(spool.publish(consumed))
    spool.revoke(consumed, reason="scheduler revoked before admission")

    with pytest.raises(LabClaimRevokedError):
        spool.admit_execution(consumed)


def test_execution_admission_and_later_revocation_are_both_immutable_audit(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    spool.consume(spool.publish(claim))
    admitted = spool.admit_execution(claim)
    admitted_payload = admitted.path.read_bytes()

    revoked = spool.revoke(claim, reason="scheduler revoked after admission")

    assert admitted.path.read_bytes() == admitted_payload
    assert LabClaimSpool(root).execution_admission(claim.claim_token) == admitted
    assert LabClaimSpool(root).revocation(claim.claim_token) == revoked
    with pytest.raises(LabClaimRevokedError):
        LabClaimSpool(root).admit_execution(claim)


def test_execution_admission_recovers_protocol_half_write_and_restart(
    tmp_path: Path,
) -> None:
    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    spool.consume(spool.publish(claim))
    temporary = (
        spool.admission_tmp_dir / f"execution-admission-v1-{claim.claim_token}-{uuid4().hex}.tmp"
    )
    temporary.write_bytes(b"half-written-admission")

    admitted = LabClaimSpool(root).admit_execution(claim)
    repeated = LabClaimSpool(root).admit_execution(claim)

    assert admitted == repeated
    assert not temporary.exists()
    assert tuple(spool.admission_tmp_dir.iterdir()) == ()


def test_execution_admission_recovers_crash_after_marker_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class AdmissionCrash(BaseException):
        pass

    root = tmp_path / "claims"
    spool = LabClaimSpool(root)
    claim = _claim()
    spool.consume(spool.publish(claim))
    original_unlink = Path.unlink

    def crash_before_temporary_unlink(path: Path, *args: object, **kwargs: object) -> None:
        if path.parent == spool.admission_tmp_dir:
            raise AdmissionCrash("crash after admission marker link")
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", crash_before_temporary_unlink)
    with pytest.raises(AdmissionCrash):
        spool.admit_execution(claim)
    marker = spool.admitted_dir / f"{claim.claim_token}.json"
    temporary = tuple(spool.admission_tmp_dir.iterdir())[0]
    assert marker.stat().st_ino == temporary.stat().st_ino
    assert marker.stat().st_nlink == 2
    monkeypatch.setattr(Path, "unlink", original_unlink)

    recovered = LabClaimSpool(root).admit_execution(claim)

    assert recovered.path == marker
    assert marker.stat().st_nlink == 1
    assert tuple(spool.admission_tmp_dir.iterdir()) == ()


def test_execution_admission_rejects_conflicting_claim_identity(tmp_path: Path) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    spool.consume(spool.publish(claim))
    spool.admit_execution(claim)
    conflicting = claim.model_copy(update={"worker_id": "worker-conflict"})

    with pytest.raises(RequestContentConflictError):
        spool.admit_execution(conflicting)


def test_execution_admission_rejects_conflicting_marker_content(tmp_path: Path) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    spool.consume(spool.publish(claim))
    admitted = spool.admit_execution(claim)
    consumed = spool._load_consumed_locked(claim.claim_token)
    conflicting = LabExecutionAdmission(
        claim=claim.model_copy(update={"worker_id": "worker-conflict"}),
        delivery_content_hash=consumed.receipt.content_hash,
    )
    replacement = admitted.path.with_suffix(".replacement")
    replacement.write_text(conflicting.model_dump_json(), encoding="utf-8")
    os.replace(replacement, admitted.path)

    with pytest.raises(RequestContentConflictError):
        spool.admit_execution(claim)


def test_revoke_receipt_failure_is_retryable_without_partial_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = LabClaimSpool(tmp_path / "claims")
    claim = _claim()
    spool.publish(claim)
    original_publish = spool._publish_no_clobber

    def fail_receipt(target: Path, payload: bytes) -> bool:
        if target.parent == spool.revoked_dir:
            raise OSError("injected revoke receipt failure")
        return original_publish(target, payload)

    monkeypatch.setattr(spool, "_publish_no_clobber", fail_receipt)
    with pytest.raises(OSError, match="revoke receipt"):
        spool.revoke(claim, reason="expired")

    assert len(spool.pending()) == 1
    assert spool.is_current(claim)
    monkeypatch.setattr(spool, "_publish_no_clobber", original_publish)
    assert spool.revoke(claim, reason="expired").revocation.reason == "expired"
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
    assert isinstance(spool.publish(claim), LabRevokedClaim)
    monkeypatch.setattr(spool, "_unlink_pending", original_unlink)
    assert LabClaimSpool(root).revoke(claim, reason="expired").revocation.reason == ("expired")
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
    assert isinstance(spool.publish(claim), LabRevokedClaim)
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
