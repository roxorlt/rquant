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
