from __future__ import annotations

import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier, Thread
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
    assert LabArtifactCommitEnvelope.model_validate_json(conflict_files[0].read_bytes()) == changed


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
    assert outside.read_text(encoding="utf-8") == "{}"


@pytest.mark.parametrize("inode_type", ["directory", "fifo"])
def test_commit_spool_quarantines_nonregular_pending_without_reading(
    tmp_path: Path,
    inode_type: str,
) -> None:
    if inode_type == "fifo" and not hasattr(os, "mkfifo"):
        pytest.skip("FIFO is not supported on this platform")
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    path = spool.pending_dir / f"00000000000000000001-{uuid4()}.json"
    if inode_type == "directory":
        path.mkdir()
        (path / "evidence.txt").write_text("preserve", encoding="utf-8")
    else:
        os.mkfifo(path)

    with pytest.raises(InvalidCommandEnvelopeError, match="not regular") as captured:
        spool.load(path)
    assert captured.value.file_identity is not None
    assert captured.value.file_identity.file_type == inode_type

    quarantined = spool.quarantine(
        captured.value.file_identity,
        reason=f"invalid_inode:{inode_type}",
    )

    assert not os.path.lexists(path)
    assert spool.quarantine_dir in quarantined.path.parents
    if inode_type == "directory":
        assert quarantined.path.is_dir()
        assert (quarantined.path / "evidence.txt").read_text(encoding="utf-8") == "preserve"
    else:
        assert stat.S_ISFIFO(quarantined.path.lstat().st_mode)


def test_commit_spool_records_disappeared_pending_race(tmp_path: Path) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    path = spool.pending_dir / f"00000000000000000001-{uuid4()}.json"
    path.write_text("{}", encoding="utf-8")

    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(path)
    identity = captured.value.file_identity
    assert identity is not None
    path.unlink()

    quarantined = spool.quarantine(identity, reason="invalid_envelope:disappeared")

    assert quarantined.path.parent == spool.quarantine_dir
    assert quarantined.path.is_file()
    assert "disappeared" in quarantined.path.read_text(encoding="utf-8")


def test_commit_conflict_quarantine_is_idempotent(tmp_path: Path) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )

    for _attempt in range(5):
        with pytest.raises(RequestContentConflictError, match="different content"):
            spool.publish(conflict)

    payloads = tuple(spool.quarantine_dir.glob("*.conflict.bad"))
    metadata = tuple(spool.quarantine_dir.glob("*.conflict.bad.json"))
    assert len(payloads) == 1
    assert len(metadata) == 1
    assert LabArtifactCommitEnvelope.model_validate_json(payloads[0].read_bytes()) == conflict


def test_commit_conflict_quarantine_is_concurrent_no_clobber(tmp_path: Path) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    barrier = Barrier(5)
    outcomes: list[type[BaseException]] = []

    def publish_conflict() -> None:
        barrier.wait()
        try:
            spool.publish(conflict)
        except BaseException as exc:
            outcomes.append(type(exc))

    threads = [Thread(target=publish_conflict) for _index in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert all(not thread.is_alive() for thread in threads)
    assert outcomes == [RequestContentConflictError] * 5
    assert len(tuple(spool.quarantine_dir.glob("*.conflict.bad"))) == 1


def test_commit_conflict_retention_prunes_only_old_conflict_pairs(tmp_path: Path) -> None:
    spool = LabArtifactCommitSpool(
        tmp_path / "commits",
        max_conflict_records=2,
        max_conflict_bytes=1024 * 1024,
    )
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    unrelated = spool.quarantine_dir / "operator-note.bad"
    unrelated.write_text("keep", encoding="utf-8")

    conflicts: list[LabArtifactCommitEnvelope] = []
    for index, manifest_digit in enumerate(("8", "9", "a"), start=1):
        conflict = LabArtifactCommitEnvelope(
            request_id=request_id,
            commit=original.commit.model_copy(
                update={"manifest_hash": manifest_digit * 64},
            ),
        )
        conflicts.append(conflict)
        with pytest.raises(RequestContentConflictError, match="different content"):
            spool.publish(conflict)
        for path in spool.quarantine_dir.glob(
            f"{request_id}.{conflict.content_hash}.*.conflict.bad*"
        ):
            os.utime(path, ns=(index, index), follow_symlinks=False)

    payloads = tuple(spool.quarantine_dir.glob("*.conflict.bad"))
    archived = {
        LabArtifactCommitEnvelope.model_validate_json(path.read_bytes()).content_hash
        for path in payloads
    }
    assert len(payloads) == 2
    assert archived == {conflicts[1].content_hash, conflicts[2].content_hash}
    assert unrelated.read_text(encoding="utf-8") == "keep"


def test_commit_conflict_retention_enforces_byte_budget(tmp_path: Path) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(
        root,
        max_conflict_records=10,
        max_conflict_bytes=1024 * 1024,
    )
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    first_conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "8" * 64}),
    )
    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(first_conflict)
    first_payload = next(spool.quarantine_dir.glob("*.conflict.bad"))
    first_pair_bytes = first_payload.stat().st_size + Path(f"{first_payload}.json").stat().st_size

    bounded = LabArtifactCommitSpool(
        root,
        max_conflict_records=10,
        max_conflict_bytes=first_pair_bytes + 1,
    )
    second_conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    with pytest.raises(RequestContentConflictError, match="different content"):
        bounded.publish(second_conflict)

    retained = tuple(bounded.quarantine_dir.glob("*.conflict.bad"))
    assert len(retained) == 1
    assert (
        LabArtifactCommitEnvelope.model_validate_json(retained[0].read_bytes()).content_hash
        == second_conflict.content_hash
    )


def test_commit_conflict_retention_ignores_lookalike_operator_evidence(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(
        tmp_path / "commits",
        max_conflict_records=1,
        max_conflict_bytes=1024 * 1024,
    )
    lookalike = spool.quarantine_dir / (f"{uuid4()}.{'f' * 64}.{'e' * 16}.conflict.bad")
    lookalike_metadata = spool.quarantine_dir / f"{lookalike.name}.json"
    lookalike.write_text("operator evidence", encoding="utf-8")
    lookalike_metadata.write_text("operator metadata", encoding="utf-8")
    os.utime(lookalike, ns=(1, 1), follow_symlinks=False)
    os.utime(lookalike_metadata, ns=(1, 1), follow_symlinks=False)

    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    for manifest_digit in ("8", "9"):
        conflict = LabArtifactCommitEnvelope(
            request_id=request_id,
            commit=original.commit.model_copy(
                update={"manifest_hash": manifest_digit * 64},
            ),
        )
        with pytest.raises(RequestContentConflictError, match="different content"):
            spool.publish(conflict)

    assert lookalike.read_text(encoding="utf-8") == "operator evidence"
    assert lookalike_metadata.read_text(encoding="utf-8") == "operator metadata"
    assert len(tuple(spool.quarantine_dir.glob(f"{request_id}.*.conflict.bad"))) == 1
