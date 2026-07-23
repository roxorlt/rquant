from __future__ import annotations

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

    assert {entry.path for entry in entries} == {spool.pending_dir / f"{envelope.request_id}.json"}
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
