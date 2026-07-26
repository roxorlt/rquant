from __future__ import annotations

import hashlib
import os
import stat
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier, Thread
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

import rquant.lab_artifact_protocol as artifact_protocol
from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommit,
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
    LabArtifactConflictEvidence,
    LabQuarantinedArtifactCommit,
)
from rquant.lab_job_protocol import (
    InvalidCommandEnvelopeError,
    LabHardLinkQuarantineArtifact,
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


class _ConflictPublishCrash(BaseException):
    pass


class _CrashableConflictSpool(LabArtifactCommitSpool):
    crash_stage: str | None = None

    def _after_conflict_evidence_stage(self, stage: str, _path: Path) -> None:
        if stage == self.crash_stage:
            raise _ConflictPublishCrash(stage)


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


def test_commit_spool_fair_scan_reaches_tail_across_restarts_and_queue_changes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    persistent_bad = tuple(
        spool.pending_dir / f"{UUID(int=index + 1)}.json" for index in range(1_000)
    )
    for path in persistent_bad:
        path.write_text("{}", encoding="utf-8")
    valid = spool.publish(_envelope(tmp_path))
    assert isinstance(valid, LabArtifactCommitSpoolEntry)
    observed: set[str] = set()
    added_later: Path | None = None

    for tick in range(20):
        spool = LabArtifactCommitSpool(root)
        batch = spool.fair_pending_paths(limit=65)
        assert 0 < len(batch) <= 65
        observed.update(path.name for path in batch)
        if tick == 4:
            added_later = spool.pending_dir / f"{UUID(int=2_000)}.json"
            added_later.write_text("{}", encoding="utf-8")
        if valid.path.name in observed and (
            added_later is not None and added_later.name in observed
        ):
            break

    assert valid.path.name in observed
    assert added_later is not None and added_later.name in observed
    assert len(observed) == 1_002


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
    conflict_files = tuple(spool.quarantine_dir.glob("*.conflict.evidence.json"))
    assert len(conflict_files) == 1
    evidence = LabArtifactConflictEvidence.model_validate_json(conflict_files[0].read_bytes())
    assert evidence.envelope == changed
    assert evidence.state == "complete"


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


def test_commit_spool_neutralizes_hardlinked_pending_without_touching_external_name(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    external = tmp_path / "external.json"
    external.write_text("external evidence", encoding="utf-8")
    pending = spool.pending_dir / f"00000000000000000001-{uuid4()}.json"
    os.link(external, pending)
    original_identity = external.stat()

    with pytest.raises(InvalidCommandEnvelopeError, match="hard link") as captured:
        spool.load(pending)
    identity = captured.value.file_identity
    assert identity is not None
    assert identity.file_type == "regular"
    assert identity.link_count == 2

    quarantined = spool.quarantine(identity, reason="invalid_inode:hardlink")

    evidence = LabHardLinkQuarantineArtifact.model_validate_json(quarantined.path.read_bytes())
    assert not os.path.lexists(pending)
    assert external.read_text(encoding="utf-8") == "external evidence"
    assert (external.stat().st_dev, external.stat().st_ino) == (
        original_identity.st_dev,
        original_identity.st_ino,
    )
    assert external.stat().st_nlink == 1
    assert evidence.original_name == pending.name
    assert (evidence.device, evidence.inode, evidence.observed_link_count) == (
        original_identity.st_dev,
        original_identity.st_ino,
        2,
    )


def test_commit_spool_hardlink_quarantine_rejects_swapped_pending_inode(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    external = tmp_path / "external.json"
    external.write_text("external evidence", encoding="utf-8")
    pending = spool.pending_dir / f"00000000000000000001-{uuid4()}.json"
    os.link(external, pending)
    with pytest.raises(InvalidCommandEnvelopeError, match="hard link") as captured:
        spool.load(pending)
    identity = captured.value.file_identity
    assert identity is not None
    pending.unlink()
    pending.write_text("replacement", encoding="utf-8")
    replacement_identity = pending.stat()

    with pytest.raises(InvalidCommandEnvelopeError, match="identity"):
        spool.quarantine(identity, reason="invalid_inode:hardlink")

    assert pending.read_text(encoding="utf-8") == "replacement"
    assert pending.stat().st_ino == replacement_identity.st_ino
    assert external.read_text(encoding="utf-8") == "external evidence"
    assert tuple(spool.quarantine_dir.iterdir()) == ()


def test_commit_spool_hardlink_evidence_is_idempotent_per_inode(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    pending = spool.pending_dir / f"00000000000000000001-{uuid4()}.json"
    reason = "invalid_inode:hardlink"
    evidence_paths: list[Path] = []

    for index in range(2):
        external = tmp_path / f"external-{index}.json"
        external.write_text(f"external evidence {index}", encoding="utf-8")
        os.link(external, pending)
        with pytest.raises(InvalidCommandEnvelopeError, match="hard link") as captured:
            spool.load(pending)
        identity = captured.value.file_identity
        assert identity is not None

        quarantined = spool.quarantine(identity, reason=reason)

        evidence_paths.append(quarantined.path)
        assert not os.path.lexists(pending)
        assert external.read_text(encoding="utf-8") == f"external evidence {index}"
        assert external.stat().st_nlink == 1

    assert evidence_paths[0] != evidence_paths[1]
    assert all(path.is_file() for path in evidence_paths)


@pytest.mark.parametrize("link_change", ["two_to_one", "two_to_three"])
def test_commit_spool_hardlink_quarantine_rejects_link_count_toctou(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    link_change: str,
) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    external = tmp_path / "external.json"
    external.write_text("external evidence", encoding="utf-8")
    pending = spool.pending_dir / f"00000000000000000001-{uuid4()}.json"
    os.link(external, pending)
    with pytest.raises(InvalidCommandEnvelopeError, match="hard link") as captured:
        spool.load(pending)
    identity = captured.value.file_identity
    assert identity is not None and identity.link_count == 2
    third = tmp_path / "third.json"

    def change_link_count(*_args: object) -> None:
        if link_change == "two_to_one":
            external.unlink()
        else:
            os.link(external, third)

    monkeypatch.setattr(
        spool,
        "_after_hardlink_quarantine_evidence",
        change_link_count,
        raising=False,
    )

    with pytest.raises(InvalidCommandEnvelopeError, match="link count"):
        spool.quarantine(identity, reason="invalid_inode:hardlink")

    assert pending.read_text(encoding="utf-8") == "external evidence"
    if link_change == "two_to_one":
        assert not external.exists()
        assert pending.stat().st_nlink == 1
    else:
        assert external.read_text(encoding="utf-8") == "external evidence"
        assert third.read_text(encoding="utf-8") == "external evidence"
        assert pending.stat().st_nlink == 3
        with pytest.raises(InvalidCommandEnvelopeError, match="hard link") as retried:
            spool.load(pending)
        retry_identity = retried.value.file_identity
        assert retry_identity is not None and retry_identity.link_count == 3
        monkeypatch.setattr(
            spool,
            "_after_hardlink_quarantine_evidence",
            lambda *_args: None,
        )
        spool.quarantine(retry_identity, reason="invalid_inode:hardlink")
        assert not os.path.lexists(pending)
        assert external.read_text(encoding="utf-8") == "external evidence"
        assert third.read_text(encoding="utf-8") == "external evidence"


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

    bundles = tuple(spool.quarantine_dir.glob("*.conflict.evidence.json"))
    assert len(bundles) == 1
    evidence = LabArtifactConflictEvidence.model_validate_json(bundles[0].read_bytes())
    assert evidence.envelope == conflict
    assert tuple(spool.quarantine_dir.glob("*.publishing.tmp")) == ()


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
    assert len(tuple(spool.quarantine_dir.glob("*.conflict.evidence.json"))) == 1
    assert tuple(spool.quarantine_dir.glob("*.publishing.tmp")) == ()


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
            f"{request_id}.{conflict.content_hash}.*.conflict.evidence.json"
        ):
            os.utime(path, ns=(index, index), follow_symlinks=False)

    payloads = tuple(spool.quarantine_dir.glob("*.conflict.evidence.json"))
    archived = {
        LabArtifactConflictEvidence.model_validate_json(path.read_bytes()).envelope.content_hash
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
    first_payload = next(spool.quarantine_dir.glob("*.conflict.evidence.json"))
    first_pair_bytes = first_payload.stat().st_size

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

    retained = tuple(bounded.quarantine_dir.glob("*.conflict.evidence.json"))
    assert len(retained) == 1
    assert (
        LabArtifactConflictEvidence.model_validate_json(
            retained[0].read_bytes()
        ).envelope.content_hash
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
    lookalike = spool.quarantine_dir / (f"{uuid4()}.{'f' * 64}.{'e' * 16}.conflict.evidence.json")
    lookalike.write_text("operator evidence", encoding="utf-8")
    os.utime(lookalike, ns=(1, 1), follow_symlinks=False)

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
    assert len(tuple(spool.quarantine_dir.glob(f"{request_id}.*.conflict.evidence.json"))) == 1


@pytest.mark.parametrize(
    "crash_stage",
    ["temporary_written", "target_linked", "temporary_unlinked"],
)
def test_commit_conflict_publish_crash_replays_to_one_atomic_bundle(
    tmp_path: Path,
    crash_stage: str,
) -> None:
    root = tmp_path / "commits"
    spool = _CrashableConflictSpool(root)
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    spool.crash_stage = crash_stage

    with pytest.raises(_ConflictPublishCrash, match=crash_stage):
        spool.publish(conflict)

    restarted = LabArtifactCommitSpool(root)
    evidence = restarted.conflict_evidence()
    assert len(evidence) == 1
    assert evidence[0].envelope == conflict
    assert tuple(restarted.quarantine_dir.glob("*.publishing.tmp")) == ()
    with pytest.raises(RequestContentConflictError, match="different content"):
        restarted.publish(conflict)
    assert restarted.conflict_evidence() == evidence


def test_commit_conflict_restart_recovers_and_prunes_owned_incomplete_bundles(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    conflicts = tuple(
        LabArtifactCommitEnvelope(
            request_id=request_id,
            commit=original.commit.model_copy(update={"manifest_hash": digit * 64}),
        )
        for digit in ("7", "8", "9")
    )
    reason = "request_id already pending with different content"
    for index, conflict in enumerate(conflicts):
        evidence = LabArtifactConflictEvidence.from_conflict(conflict, reason=reason)
        temporary = spool._conflict_temporary_path(evidence)
        temporary.write_bytes(evidence.model_dump_json().encode("utf-8"))
        os.utime(temporary, ns=(index + 1, index + 1), follow_symlinks=False)

    restarted = LabArtifactCommitSpool(
        root,
        max_conflict_records=2,
        max_conflict_bytes=1024 * 1024,
    )

    retained = restarted.conflict_evidence()
    assert len(retained) == 2
    assert {item.envelope.content_hash for item in retained} == {
        conflicts[1].content_hash,
        conflicts[2].content_hash,
    }
    assert tuple(restarted.quarantine_dir.glob("*.publishing.tmp")) == ()


def test_commit_conflict_restart_bounds_truncated_owned_temps_and_allows_republish(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    reason = "request_id already pending with different content"
    evidence = LabArtifactConflictEvidence.from_conflict(conflict, reason=reason)

    for truncation in (b"{", b'{"schema_version":1', b"not-json"):
        spool = LabArtifactCommitSpool(
            root,
            max_conflict_records=1,
            max_conflict_bytes=1,
        )
        temporary = spool._conflict_temporary_path(evidence)
        temporary.write_bytes(truncation)
        spool = LabArtifactCommitSpool(
            root,
            max_conflict_records=1,
            max_conflict_bytes=1,
        )
        assert not os.path.lexists(temporary)
        corrupt = tuple(spool.quarantine_dir.glob("*.corrupt-conflict-temp.bad.json"))
        assert len(corrupt) == 1
        record = artifact_protocol.LabCorruptConflictTempEvidence.model_validate_json(
            corrupt[0].read_bytes()
        )
        assert record.temporary_name == temporary.name
        assert record.byte_count == len(truncation)

    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)
    assert len(spool.conflict_evidence()) == 1
    assert tuple(spool.quarantine_dir.glob("*.corrupt-conflict-temp.bad.json")) == ()


def test_commit_conflict_recovery_does_not_touch_temp_lookalikes_symlinks_or_hardlinks(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    original = _envelope(tmp_path)
    spool.publish(original)
    conflicts = tuple(
        LabArtifactCommitEnvelope(
            request_id=original.request_id,
            commit=original.commit.model_copy(update={"manifest_hash": digit * 64}),
        )
        for digit in ("7", "8")
    )
    reason = "request_id already pending with different content"
    temporaries = tuple(
        spool._conflict_temporary_path(
            LabArtifactConflictEvidence.from_conflict(conflict, reason=reason)
        )
        for conflict in conflicts
    )
    outside_symlink = tmp_path / "outside-symlink"
    outside_symlink.write_text("outside symlink", encoding="utf-8")
    os.symlink(outside_symlink, temporaries[0])
    outside_hardlink = tmp_path / "outside-hardlink"
    outside_hardlink.write_text("outside hardlink", encoding="utf-8")
    os.link(outside_hardlink, temporaries[1])
    lookalike = spool.quarantine_dir / ".not-owned.publishing.tmp"
    lookalike.write_text("lookalike", encoding="utf-8")

    LabArtifactCommitSpool(root, max_conflict_records=1, max_conflict_bytes=1)

    assert os.path.islink(temporaries[0])
    assert outside_symlink.read_text(encoding="utf-8") == "outside symlink"
    assert temporaries[1].stat().st_ino == outside_hardlink.stat().st_ino
    assert outside_hardlink.read_text(encoding="utf-8") == "outside hardlink"
    assert lookalike.read_text(encoding="utf-8") == "lookalike"


def test_commit_conflict_concurrent_republish_after_truncated_temp_is_idempotent(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    original = _envelope(tmp_path)
    spool.publish(original)
    conflict = LabArtifactCommitEnvelope(
        request_id=original.request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    evidence = LabArtifactConflictEvidence.from_conflict(
        conflict,
        reason="request_id already pending with different content",
    )
    spool._conflict_temporary_path(evidence).write_bytes(b'{"schema_version":')
    barrier = Barrier(3)
    outcomes: list[str] = []

    def republish() -> None:
        barrier.wait()
        local = LabArtifactCommitSpool(root)
        try:
            local.publish(conflict)
        except RequestContentConflictError:
            outcomes.append("conflict")

    threads = [Thread(target=republish) for _index in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=5)

    assert not any(thread.is_alive() for thread in threads)
    assert outcomes == ["conflict", "conflict"]
    restarted = LabArtifactCommitSpool(root)
    assert len(restarted.conflict_evidence()) == 1
    assert len(tuple(restarted.quarantine_dir.glob("*.corrupt-conflict-temp.bad.json"))) == 1


def test_commit_conflict_retention_bounds_legacy_partial_files_without_touching_lookalikes(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    request_id = uuid4()
    original = _envelope(tmp_path, request_id=request_id)
    spool.publish(original)
    reason = "request_id already pending with different content"
    reason_hash = hashlib.sha256(reason.encode("utf-8")).hexdigest()[:16]
    payload_only_conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "7" * 64}),
    )
    payload_only = spool.quarantine_dir / (
        f"{request_id}.{payload_only_conflict.content_hash}.{reason_hash}.conflict.bad"
    )
    payload_only.write_bytes(payload_only_conflict.model_dump_json().encode("utf-8"))

    metadata_only_conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "8" * 64}),
    )
    missing_payload = spool.quarantine_dir / (
        f"{request_id}.{metadata_only_conflict.content_hash}.{reason_hash}.conflict.bad"
    )
    metadata_only = Path(f"{missing_payload}.json")
    metadata_only.write_bytes(
        LabQuarantinedArtifactCommit(
            path=missing_payload,
            reason=reason,
        )
        .model_dump_json()
        .encode("utf-8")
    )
    outside = tmp_path / "outside-conflict-evidence"
    outside.write_text("keep", encoding="utf-8")
    symlink = spool.quarantine_dir / (f"{uuid4()}.{'e' * 64}.{'d' * 16}.conflict.bad")
    os.symlink(outside, symlink)
    lookalike = spool.quarantine_dir / (f"{uuid4()}.{'f' * 64}.{'c' * 16}.conflict.bad")
    lookalike.write_text("unowned", encoding="utf-8")
    for path in (payload_only, metadata_only):
        os.utime(path, ns=(1, 1), follow_symlinks=False)

    bounded = LabArtifactCommitSpool(
        root,
        max_conflict_records=1,
        max_conflict_bytes=1024 * 1024,
    )
    newest = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    with pytest.raises(RequestContentConflictError, match="different content"):
        bounded.publish(newest)

    assert not payload_only.exists()
    assert not metadata_only.exists()
    assert os.path.lexists(symlink)
    assert outside.read_text(encoding="utf-8") == "keep"
    assert lookalike.read_text(encoding="utf-8") == "unowned"
    assert len(bounded.conflict_evidence()) == 1
