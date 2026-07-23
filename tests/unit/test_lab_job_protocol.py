from __future__ import annotations

import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from rquant.lab_job_protocol import (
    CancelJobCommand,
    InvalidCommandEnvelopeError,
    LabCommandEnvelope,
    LabCommandReceipt,
    LabCommandSpool,
    PauseJobCommand,
    RequestContentConflictError,
    ResumeJobCommand,
    RetryJobCommand,
    SubmitJobCommand,
)
from rquant.research_run_spec import (
    DatasetSnapshotIdentity,
    ExecutionCostSpec,
    FeatureContractIdentity,
    ParameterKind,
    ResearchJobType,
    ResearchParameter,
    ResearchRunParameters,
    ResearchRunSpec,
    ResourceClass,
)


def _spec(
    *,
    threshold: Decimal = Decimal("1.5000"),
    deadline: datetime = datetime(2026, 7, 25, 2, tzinfo=UTC),
) -> ResearchRunSpec:
    return ResearchRunSpec(
        job_type=ResearchJobType.PARAMETER_SEARCH,
        parameters=ResearchRunParameters(
            strategy_name="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 14),
            arguments=(
                ResearchParameter(
                    name="threshold",
                    kind=ParameterKind.DECIMAL,
                    value=threshold,
                ),
            ),
        ),
        code_sha="1" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="a" * 64,
            binding_hash="b" * 64,
        ),
        feature_contract=FeatureContractIdentity(
            contract_id="intraday-core",
            contract_version="v1",
            contract_hash="c" * 64,
        ),
        execution_costs=ExecutionCostSpec(
            commission_bps="2.5",
            stamp_duty_bps="5",
            transfer_fee_bps="0.1",
            slippage_bps="3",
        ),
        random_seed=20260724,
        resource_class=ResourceClass.HEAVY,
        deadline=deadline,
        research_status="comparable",
    )


def _submit_envelope(
    *,
    request_id: UUID | None = None,
    job_id: UUID | None = None,
    spec: ResearchRunSpec | None = None,
) -> LabCommandEnvelope:
    return LabCommandEnvelope(
        request_id=request_id or uuid4(),
        command=SubmitJobCommand(
            job_id=job_id or uuid4(),
            spec=spec or _spec(),
            max_attempts=3,
        ),
    )


def test_protocol_roundtrips_all_command_variants() -> None:
    job_id = uuid4()
    envelopes = (
        _submit_envelope(job_id=job_id),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=PauseJobCommand(
                job_id=job_id,
                expected_version=1,
                reason="operator pause",
            ),
        ),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=ResumeJobCommand(
                job_id=job_id,
                expected_version=2,
                reason="capacity restored",
            ),
        ),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=CancelJobCommand(
                job_id=job_id,
                expected_version=2,
                reason="operator request",
            ),
        ),
        LabCommandEnvelope(
            request_id=uuid4(),
            command=RetryJobCommand(
                job_id=job_id,
                expected_version=4,
                reason="transient source failure",
            ),
        ),
    )

    for envelope in envelopes:
        restored = LabCommandEnvelope.model_validate_json(envelope.model_dump_json())
        assert type(restored.command) is type(envelope.command)
        assert restored == envelope
        assert restored.content_hash == envelope.content_hash


def test_submit_content_hash_uses_canonical_spec_hash() -> None:
    request_id = uuid4()
    job_id = uuid4()
    shanghai = timezone(timedelta(hours=8))
    first = _submit_envelope(
        request_id=request_id,
        job_id=job_id,
        spec=_spec(threshold=Decimal("1.5000")),
    )
    equivalent = _submit_envelope(
        request_id=request_id,
        job_id=job_id,
        spec=_spec(
            threshold=Decimal("1.5"),
            deadline=datetime(2026, 7, 25, 10, tzinfo=shanghai),
        ),
    )

    assert first.command.spec.spec_hash == equivalent.command.spec.spec_hash
    assert first.content_hash == equivalent.content_hash


def test_envelope_rejects_tampered_content_hash() -> None:
    envelope = _submit_envelope()
    payload = envelope.model_dump(mode="json")
    payload["content_hash"] = "f" * 64

    with pytest.raises(ValueError, match="content_hash"):
        LabCommandEnvelope.model_validate(payload)


def test_spool_publish_load_ack_is_durable_and_typed(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()

    published = spool.publish(envelope)
    assert published.envelope == envelope
    assert published.path.parent == spool.pending_dir
    assert spool.load(published.path).envelope == envelope
    assert spool.pending() == (published,)

    receipt = LabCommandReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    acknowledged = spool.ack(published, receipt)

    assert acknowledged.receipt == receipt
    assert acknowledged.path.parent == spool.ack_dir
    assert spool.pending() == ()
    assert spool.load_receipt(acknowledged.path) == receipt


def test_same_request_and_content_publish_is_idempotent(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()

    first = spool.publish(envelope)
    second = spool.publish(LabCommandEnvelope.model_validate_json(envelope.model_dump_json()))

    assert first.path == second.path
    assert spool.pending() == (first,)


def test_same_request_with_different_content_never_overwrites(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    request_id = uuid4()
    first = _submit_envelope(request_id=request_id)
    conflict = _submit_envelope(request_id=request_id)
    original = spool.publish(first)
    original_bytes = original.path.read_bytes()

    with pytest.raises(RequestContentConflictError):
        spool.publish(conflict)

    assert original.path.read_bytes() == original_bytes
    assert spool.load(original.path).envelope == first


def test_concurrent_publish_is_no_clobber(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()

    with ThreadPoolExecutor(max_workers=8) as executor:
        entries = tuple(executor.map(spool.publish, (envelope,) * 32))

    paths = {entry.path for entry in entries}
    assert len(paths) == 1
    assert next(iter(paths)).name.endswith(f"-{envelope.request_id}.json")
    assert spool.pending() == (entries[0],)


def test_bad_json_can_be_quarantined_without_bare_dict(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    bad_path = spool.pending_dir / f"{uuid4()}.json"
    bad_path.write_text("{broken", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError):
        spool.load(bad_path)

    quarantined = spool.quarantine(bad_path, reason="invalid_json")
    assert quarantined.path.parent == spool.quarantine_dir
    assert quarantined.reason == "invalid_json"
    assert not bad_path.exists()
    assert quarantined.path.read_text(encoding="utf-8") == "{broken"


def test_pending_order_is_fifo_across_reverse_request_ids_and_restart(tmp_path: Path) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root)
    first = _submit_envelope(request_id=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"))
    second = _submit_envelope(request_id=UUID("00000000-0000-0000-0000-000000000001"))

    spool.publish(first)
    spool.publish(second)

    assert tuple(entry.envelope for entry in spool.pending()) == (first, second)
    restarted = LabCommandSpool(root)
    assert tuple(entry.envelope for entry in restarted.pending()) == (first, second)


def test_concurrent_process_publish_assigns_unique_persistent_fifo_sequence(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commands"
    envelopes = tuple(_submit_envelope() for _ in range(8))
    script = """
import sys
from pathlib import Path
from rquant.lab_job_protocol import LabCommandEnvelope, LabCommandSpool
entry = LabCommandSpool(Path(sys.argv[1])).publish(
    LabCommandEnvelope.model_validate_json(sys.argv[2])
)
print(entry.path.name)
"""
    processes = tuple(
        subprocess.Popen(
            [sys.executable, "-c", script, str(root), envelope.model_dump_json()],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for envelope in envelopes
    )
    results = tuple(process.communicate(timeout=10) for process in processes)
    assert all(process.returncode == 0 for process in processes), results
    names = tuple(stdout.strip() for stdout, _stderr in results)

    sequences = {int(name.split("-", 1)[0]) for name in names}
    assert len(sequences) == len(envelopes)
    restarted = LabCommandSpool(root)
    pending_sequences = tuple(int(path.name.split("-", 1)[0]) for path in restarted.pending_paths())
    assert pending_sequences == tuple(sorted(sequences))

    later = restarted.publish(_submit_envelope())
    assert int(later.path.name.split("-", 1)[0]) > max(sequences)


def test_submit_precedes_controls_and_cancel_preempts_same_version_controls(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    job_id = uuid4()
    submit = _submit_envelope(job_id=job_id)
    pause = LabCommandEnvelope(
        request_id=uuid4(),
        command=PauseJobCommand(job_id=job_id, expected_version=0, reason="pause"),
    )
    resume = LabCommandEnvelope(
        request_id=uuid4(),
        command=ResumeJobCommand(job_id=job_id, expected_version=0, reason="resume"),
    )
    cancel = LabCommandEnvelope(
        request_id=uuid4(),
        command=CancelJobCommand(job_id=job_id, expected_version=0, reason="cancel"),
    )
    for envelope in (submit, pause, resume, cancel):
        spool.publish(envelope)

    assert tuple(entry.envelope.command.command_type for entry in spool.pending()) == (
        "submit",
        "cancel",
        "pause",
        "resume",
    )


def test_submit_precedes_earlier_controls_after_spool_restart(tmp_path: Path) -> None:
    root = tmp_path / "commands"
    spool = LabCommandSpool(root)
    job_id = UUID("11111111-1111-1111-1111-111111111111")
    pause = LabCommandEnvelope(
        request_id=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
        command=PauseJobCommand(job_id=job_id, expected_version=0, reason="pause"),
    )
    cancel = LabCommandEnvelope(
        request_id=UUID("00000000-0000-0000-0000-000000000001"),
        command=CancelJobCommand(job_id=job_id, expected_version=0, reason="cancel"),
    )
    submit = _submit_envelope(
        request_id=UUID("88888888-8888-8888-8888-888888888888"),
        job_id=job_id,
    )
    for envelope in (pause, cancel, submit):
        spool.publish(envelope)

    restarted = LabCommandSpool(root)

    assert tuple(entry.envelope.command.command_type for entry in restarted.pending()) == (
        "submit",
        "cancel",
        "pause",
    )


def test_publish_after_ack_returns_existing_receipt_and_rejects_conflict(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    request_id = uuid4()
    envelope = _submit_envelope(request_id=request_id)
    published = spool.publish(envelope)
    receipt = LabCommandReceipt(
        request_id=request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    acknowledged = spool.ack(published, receipt)

    replay = spool.publish(envelope)

    assert replay == acknowledged
    assert spool.pending() == ()
    with pytest.raises(RequestContentConflictError):
        spool.publish(_submit_envelope(request_id=request_id))
    assert spool.pending() == ()


def test_load_and_quarantine_reject_external_symlink_and_mismatched_basename(
    tmp_path: Path,
) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()
    victim = tmp_path / "victim.json"
    victim.write_text(envelope.model_dump_json(), encoding="utf-8")
    symlink = spool.pending_dir / f"{envelope.request_id}.json"
    symlink.symlink_to(victim)

    for candidate in (victim, symlink):
        with pytest.raises(InvalidCommandEnvelopeError):
            spool.load(candidate)
        with pytest.raises(InvalidCommandEnvelopeError):
            spool.quarantine(candidate, reason="unsafe")
    assert victim.exists()
    assert symlink.is_symlink()

    mismatched = spool.pending_dir / f"{uuid4()}.json"
    mismatched.write_text(envelope.model_dump_json(), encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError, match="request_id"):
        spool.load(mismatched)
    with pytest.raises(InvalidCommandEnvelopeError, match="request_id"):
        spool.quarantine(mismatched, reason="mismatch")
    assert mismatched.exists()

    pending = spool.publish(_submit_envelope())
    receipt = LabCommandReceipt(
        request_id=pending.envelope.request_id,
        content_hash=pending.envelope.content_hash,
        job_id=pending.envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    with pytest.raises(InvalidCommandEnvelopeError):
        spool.ack(pending.model_copy(update={"path": victim}), receipt)
    assert victim.exists()


def test_ack_and_quarantine_do_not_unlink_replacement_file(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    envelope = _submit_envelope()
    entry = spool.publish(envelope)
    receipt = LabCommandReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.command.job_id,
        status="applied",
        reason="submitted",
        job_version=0,
    )
    entry.path.unlink()
    replacement = _submit_envelope(request_id=envelope.request_id)
    entry.path.write_text(replacement.model_dump_json(), encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.ack(entry, receipt)
    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.quarantine(entry, reason="semantic_conflict")

    assert entry.path.exists()
    assert tuple(spool.ack_dir.glob("*.json")) == ()
    assert tuple(spool.quarantine_dir.glob("*.bad")) == ()


def test_load_rejects_non_direct_lexical_path_alias(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    entry = spool.publish(_submit_envelope())
    aliased = spool.pending_dir / "nested" / ".." / entry.path.name

    with pytest.raises(InvalidCommandEnvelopeError, match="unsafe spool path"):
        spool.load(aliased)


def test_malformed_load_identity_prevents_quarantine_of_replacement(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    malformed = spool.pending_dir / "not-a-command.json"
    malformed.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(malformed)
    identity = captured.value.file_identity
    assert identity is not None
    malformed.unlink()
    malformed.write_text("replacement", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.quarantine(identity, reason="invalid_envelope")

    assert malformed.read_text(encoding="utf-8") == "replacement"
    assert tuple(spool.quarantine_dir.glob("*.bad")) == ()


def test_symlink_load_identity_prevents_quarantine_of_replacement(tmp_path: Path) -> None:
    spool = LabCommandSpool(tmp_path / "commands")
    victim = tmp_path / "victim.json"
    victim.write_text("external", encoding="utf-8")
    symlink = spool.pending_dir / "not-a-command.json"
    symlink.symlink_to(victim)
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(symlink)
    identity = captured.value.file_identity
    assert identity is not None
    assert identity.file_type == "symlink"
    assert identity.link_target == str(victim)
    symlink.unlink()
    symlink.write_text("replacement", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
        spool.quarantine(identity, reason="invalid_symlink")

    assert symlink.read_text(encoding="utf-8") == "replacement"
    assert victim.read_text(encoding="utf-8") == "external"
    assert tuple(spool.quarantine_dir.glob("*.symlink.bad.json")) == ()
