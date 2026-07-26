from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommit,
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
)
from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    RequestContentConflictError,
)
from rquant.research_run_spec import DatasetSnapshotIdentity


def _commit(tmp_path: Path) -> LabArtifactCommit:
    return LabArtifactCommit(
        job_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        spec_hash="1" * 64,
        plan_hash="2" * 64,
        adapter_id="n-shape",
        adapter_version="1",
        result_contract_version="p1.4b-complete-result-v1",
        code_sha="3" * 40,
        dataset_snapshot=DatasetSnapshotIdentity(
            snapshot_id="4" * 64,
            binding_hash="5" * 64,
            audit_run_id="6" * 64,
        ),
        manifest_hash="7" * 64,
        complete_result_hash="8" * 64,
        sealed_path=tmp_path / "artifacts" / "sealed" / ("a" * 32),
    )


def _envelope(tmp_path: Path, *, request_id: UUID | None = None) -> LabArtifactCommitEnvelope:
    return LabArtifactCommitEnvelope(
        request_id=request_id or uuid4(),
        commit=_commit(tmp_path),
    )


def test_commit_envelope_hashes_canonical_typed_content(tmp_path: Path) -> None:
    envelope = _envelope(tmp_path)
    rebuilt = LabArtifactCommitEnvelope.model_validate_json(envelope.model_dump_json())

    assert rebuilt == envelope
    assert len(envelope.content_hash) == 64
    assert envelope.content_hash == rebuilt.content_hash

    with pytest.raises(ValidationError, match="content_hash"):
        LabArtifactCommitEnvelope(
            request_id=envelope.request_id,
            commit=envelope.commit,
            content_hash="0" * 64,
        )


def test_commit_requires_absolute_sealed_path(tmp_path: Path) -> None:
    values = _commit(tmp_path).model_dump()
    values["sealed_path"] = Path("../sealed")
    with pytest.raises(ValidationError, match="sealed_path"):
        LabArtifactCommit.model_validate(values)


def test_commit_spool_is_exactly_once_through_ack(tmp_path: Path) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    envelope = _envelope(tmp_path)

    first = spool.publish(envelope)
    replay = spool.publish(envelope)

    assert isinstance(first, LabArtifactCommitSpoolEntry)
    assert replay == first
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="accepted",
        reason="artifact committed",
        accepted_at=datetime(2026, 7, 26, tzinfo=UTC),
        job_version=3,
    )
    acknowledged = spool.ack(first, receipt)

    assert isinstance(acknowledged, LabAcknowledgedArtifactCommit)
    assert spool.pending() == ()
    assert spool.publish(envelope) == acknowledged


def test_commit_spool_fails_closed_on_request_content_conflict(tmp_path: Path) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    request_id = uuid4()
    first = _envelope(tmp_path, request_id=request_id)
    spool.publish(first)
    changed = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=first.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )

    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(changed)

    assert spool.pending()[0].envelope == first
    conflict_files = tuple(spool.quarantine_dir.glob("*.conflict.bad"))
    assert len(conflict_files) == 1
    assert LabArtifactCommitEnvelope.model_validate_json(
        conflict_files[0].read_bytes()
    ) == changed


def test_commit_spool_rejects_traversal_and_quarantines_symlink(tmp_path: Path) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    outside = tmp_path / "outside.json"
    outside.write_text("{}", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError, match="outside"):
        spool.load(outside)

    request_id = uuid4()
    symlink = spool.pending_dir / f"00000000000000000001-{request_id}.json"
    os.symlink(outside, symlink)
    with pytest.raises(InvalidCommandEnvelopeError, match="symlink") as captured:
        spool.load(symlink)
    quarantined = spool.quarantine(
        captured.value.file_identity or symlink,
        reason="invalid_envelope:symlink",
    )

    assert quarantined.path.parent == spool.quarantine_dir
    assert not os.path.lexists(symlink)
