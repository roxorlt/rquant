from __future__ import annotations

import hashlib
import os
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier, Thread
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

import rquant.lab_artifact_protocol as artifact_protocol
import rquant.lab_job_protocol as job_protocol
from rquant.lab_artifact_protocol import (
    LabAcknowledgedArtifactCommit,
    LabArtifactCommit,
    LabArtifactCommitEnvelope,
    LabArtifactCommitReceipt,
    LabArtifactCommitSpool,
    LabArtifactCommitSpoolEntry,
    LabArtifactConflictEvidence,
    LabFinalizerAuthorityAuthenticationError,
    LabFinalizerAuthorityClaims,
    LabFinalizerAuthorityKey,
    LabFinalizerAuthorityShardEvidence,
    LabQuarantinedArtifactCommit,
    sign_finalizer_authority,
    verify_finalizer_authority,
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
    commit = _commit(tmp_path)
    resolved_request_id = request_id or uuid4()
    claims = LabFinalizerAuthorityClaims(
        request_id=resolved_request_id,
        commit_content_hash=hashlib.sha256(commit.canonical_json_bytes()).hexdigest(),
        job_id=commit.job_id,
        ready_event_id=17,
        ready_job_version=4,
        scheduler_fencing_token=3,
        spec_hash=commit.spec_hash,
        finalizer_code_sha=commit.code_sha,
        shards=(
            LabFinalizerAuthorityShardEvidence(
                shard_index=0,
                shard_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                payload_hash="9" * 64,
                plan_hash=commit.plan_hash,
                result_manifest_hash="a" * 64,
                accepted_report_content_hash="b" * 64,
                claim_token=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
                claim_generation=2,
                scheduler_fencing_token=3,
            ),
        ),
        artifact_manifest_hash=commit.manifest_hash,
        complete_result_hash=commit.complete_result_hash,
    )
    proof = sign_finalizer_authority(
        claims,
        key_provider=lambda: LabFinalizerAuthorityKey(
            key_id="test-key-2026-07",
            secret=b"k" * 32,
        ),
    )
    return LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=resolved_request_id,
        commit=commit,
        authority_proof=proof,
    )


def test_artifact_commit_spool_rejects_post_init_pending_replacement(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "artifact-commits")
    external = tmp_path / "external-pending"
    external.mkdir(mode=0o700)
    displaced = spool.pending_dir.with_name(f"{spool.pending_dir.name}-displaced")
    spool.pending_dir.rename(displaced)
    spool.pending_dir.symlink_to(external, target_is_directory=True)

    with pytest.raises(InvalidCommandEnvelopeError, match="identity"):
        spool.publish(_envelope(tmp_path))

    assert tuple(external.iterdir()) == ()


def test_finalizer_authority_key_is_validated_and_secret_is_not_serialized() -> None:
    key = LabFinalizerAuthorityKey(key_id="rotation-a", secret=b"s" * 32)

    assert "ssss" not in repr(key)
    assert not hasattr(key, "model_dump")
    with pytest.raises(ValueError, match="at least 32 bytes"):
        LabFinalizerAuthorityKey(key_id="rotation-a", secret=b"short")


def test_finalizer_authority_proof_verifies_mac_and_rejects_key_rotation(
    tmp_path: Path,
) -> None:
    envelope = _envelope(tmp_path)

    verified = verify_finalizer_authority(
        envelope,
        key_provider=lambda key_id: (
            LabFinalizerAuthorityKey(key_id=key_id, secret=b"k" * 32)
            if key_id == "test-key-2026-07"
            else None
        ),
    )

    assert verified == envelope.authority_proof.claims
    with pytest.raises(LabFinalizerAuthorityAuthenticationError, match="unknown key"):
        verify_finalizer_authority(
            envelope,
            key_provider=lambda _key_id: None,
        )


def test_finalizer_authority_verifier_accepts_active_and_transition_keys(
    tmp_path: Path,
) -> None:
    active = LabFinalizerAuthorityKey(key_id="test-key-2026-07", secret=b"k" * 32)
    transition = LabFinalizerAuthorityKey(key_id="test-key-2026-06", secret=b"o" * 32)
    envelope = _envelope(tmp_path)
    old_envelope = LabArtifactCommitEnvelope(
        schema_version=2,
        request_id=envelope.request_id,
        commit=envelope.commit,
        authority_proof=sign_finalizer_authority(
            envelope.authority_proof.claims,
            key_provider=lambda: transition,
        ),
    )
    keys = {active.key_id: active, transition.key_id: transition}

    assert (
        verify_finalizer_authority(
            envelope,
            key_provider=keys.get,
        )
        == envelope.authority_proof.claims
    )
    assert (
        verify_finalizer_authority(
            old_envelope,
            key_provider=keys.get,
        )
        == old_envelope.authority_proof.claims
    )


class _ConflictPublishCrash(BaseException):
    pass


class _CursorWriteCrash(BaseException):
    pass


class _ConflictCleanupCrash(BaseException):
    pass


class _CrashableConflictSpool(LabArtifactCommitSpool):
    crash_stage: str | None = None

    def _after_conflict_evidence_stage(self, stage: str, _path: Path) -> None:
        if stage == self.crash_stage:
            raise _ConflictPublishCrash(stage)


class _CrashableCursorSpool(LabArtifactCommitSpool):
    crash_stage: str | None = None

    def _after_scan_cursor_stage(self, stage: str, _path: Path) -> None:
        if stage == self.crash_stage:
            raise _CursorWriteCrash(stage)


class _CrashableConflictCleanupSpool(LabArtifactCommitSpool):
    crash_stage: str | None = None

    def _after_owned_entry_isolation_stage(
        self,
        stage: str,
        _source: Path,
        _container: Path,
    ) -> None:
        if stage == self.crash_stage:
            raise _ConflictCleanupCrash(stage)


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


def test_commit_spool_readonly_inspection_reports_exact_pending_ack_or_missing(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    envelope = _envelope(tmp_path)
    before = tuple(sorted(path.relative_to(spool.root) for path in spool.root.rglob("*")))

    assert spool.inspect(envelope.request_id) is None
    assert tuple(sorted(path.relative_to(spool.root) for path in spool.root.rglob("*"))) == before
    pending = spool.publish(envelope)
    assert isinstance(pending, LabArtifactCommitSpoolEntry)
    pending_tree = tuple(sorted(path.relative_to(spool.root) for path in spool.root.rglob("*")))
    assert spool.inspect(envelope.request_id) == pending
    assert (
        tuple(sorted(path.relative_to(spool.root) for path in spool.root.rglob("*")))
        == pending_tree
    )
    receipt = LabArtifactCommitReceipt.from_envelope(
        envelope,
        status="rejected",
        reason="test rejection",
        accepted_at=datetime(2026, 7, 26, tzinfo=UTC),
        job_version=3,
    )
    acknowledged = spool.ack(pending, receipt)
    ack_tree = tuple(sorted(path.relative_to(spool.root) for path in spool.root.rglob("*")))

    assert spool.inspect(envelope.request_id) == acknowledged
    assert tuple(sorted(path.relative_to(spool.root) for path in spool.root.rglob("*"))) == ack_tree


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


@pytest.mark.parametrize(
    "cursor_kind",
    ["malformed", "truncated", "symlink", "hardlink", "directory"],
)
def test_commit_spool_corrupt_advisory_cursor_isolated_and_queue_remains_reachable(
    tmp_path: Path,
    cursor_kind: str,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    valid = spool.publish(_envelope(tmp_path))
    assert isinstance(valid, LabArtifactCommitSpoolEntry)
    cursor = spool._scan_cursor_path
    outside = tmp_path / "outside-cursor"
    outside.write_text("outside cursor", encoding="utf-8")
    if cursor_kind == "malformed":
        cursor.write_text("{}", encoding="utf-8")
    elif cursor_kind == "truncated":
        cursor.write_text('{"schema_version":', encoding="utf-8")
    elif cursor_kind == "symlink":
        os.symlink(outside, cursor)
    elif cursor_kind == "hardlink":
        os.link(outside, cursor)
    else:
        cursor.mkdir()
        (cursor / "operator.txt").write_text("preserve", encoding="utf-8")

    restarted = LabArtifactCommitSpool(root)
    selected = restarted.fair_pending_paths(limit=1)

    assert selected == (valid.path,)
    assert restarted._scan_cursor_path.is_file()
    assert restarted._scan_cursor_path.stat().st_nlink == 1
    isolated = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(isolated) == 1
    evidence = job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        isolated[0].read_bytes()
    )
    assert evidence.source_name == cursor.name
    if cursor_kind in {"symlink", "hardlink"}:
        assert outside.read_text(encoding="utf-8") == "outside cursor"
    if cursor_kind == "directory":
        assert (isolated[0].parent / "entry" / "operator.txt").read_text(
            encoding="utf-8"
        ) == "preserve"
    assert LabArtifactCommitSpool(root).fair_pending_paths(limit=1) == (valid.path,)


def test_commit_spool_cursor_write_crash_is_advisory_across_restart(tmp_path: Path) -> None:
    root = tmp_path / "commits"
    spool = _CrashableCursorSpool(root)
    valid = spool.publish(_envelope(tmp_path))
    assert isinstance(valid, LabArtifactCommitSpoolEntry)
    spool.crash_stage = "temporary_written"

    with pytest.raises(_CursorWriteCrash, match="temporary_written"):
        spool.fair_pending_paths(limit=1)

    restarted = LabArtifactCommitSpool(root)
    assert restarted.fair_pending_paths(limit=1) == (valid.path,)
    assert tuple(root.glob(".*scan-cursor*.tmp")) == ()


def test_commit_cursor_never_publishes_into_replaced_spool_root(tmp_path: Path) -> None:
    root = tmp_path / "commits"
    displaced = tmp_path / "commits.displaced"

    class ReplacingCursorSpool(LabArtifactCommitSpool):
        def _after_scan_cursor_stage(self, stage: str, _path: Path) -> None:
            if stage == "temporary_written":
                root.rename(displaced)
                root.mkdir(mode=0o700)

    spool = ReplacingCursorSpool(root)
    valid = spool.publish(_envelope(tmp_path))
    assert isinstance(valid, LabArtifactCommitSpoolEntry)

    with pytest.raises(InvalidCommandEnvelopeError, match="identity changed"):
        spool.fair_pending_paths(limit=1)

    assert tuple(root.iterdir()) == ()
    assert not (root / ".artifact-commit-scan-cursor.json").exists()


@pytest.mark.parametrize("crash_stage", ["temporary_written", "cursor_replaced"])
def test_commit_spool_real_process_cursor_crash_reconciles_owned_temporary(
    tmp_path: Path,
    crash_stage: str,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    valid = spool.publish(_envelope(tmp_path))
    assert isinstance(valid, LabArtifactCommitSpoolEntry)
    script = """
import os
import sys
from pathlib import Path
from rquant.lab_artifact_protocol import LabArtifactCommitSpool

class CrashSpool(LabArtifactCommitSpool):
    def _after_scan_cursor_stage(self, stage, path):
        if stage == sys.argv[2]:
            os._exit(73)

CrashSpool(Path(sys.argv[1])).fair_pending_paths(limit=1)
"""

    completed = subprocess.run(
        [sys.executable, "-c", script, str(root), crash_stage],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 73, completed.stderr
    crash_temporaries = tuple(root.glob("..artifact-commit-scan-cursor.json.*.tmp"))
    assert len(crash_temporaries) == int(crash_stage == "temporary_written")

    restarted = LabArtifactCommitSpool(
        root,
        max_conflict_records=1,
        max_conflict_bytes=1,
    )
    assert restarted.fair_pending_paths(limit=1) == (valid.path,)
    assert tuple(root.glob("..artifact-commit-scan-cursor.json.*.tmp")) == ()
    isolated = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(isolated) == int(crash_stage == "temporary_written")


@pytest.mark.parametrize(
    "entry_kind",
    ["regular", "symlink", "hardlink", "fifo", "empty_directory", "nonempty_directory"],
)
def test_commit_spool_startup_isolates_every_owned_cursor_temporary_inode(
    tmp_path: Path,
    entry_kind: str,
) -> None:
    if entry_kind == "fifo" and not hasattr(os, "mkfifo"):
        pytest.skip("FIFO is not supported on this platform")
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    valid = spool.publish(_envelope(tmp_path))
    assert isinstance(valid, LabArtifactCommitSpoolEntry)
    temporary = root / f"..artifact-commit-scan-cursor.json.{uuid4().hex}.tmp"
    outside = tmp_path / "outside-cursor-temp"
    outside.write_text("outside", encoding="utf-8")
    if entry_kind == "regular":
        temporary.write_text("truncated", encoding="utf-8")
    elif entry_kind == "symlink":
        os.symlink(outside, temporary)
    elif entry_kind == "hardlink":
        os.link(outside, temporary)
    elif entry_kind == "fifo":
        os.mkfifo(temporary)
    else:
        temporary.mkdir()
        if entry_kind == "nonempty_directory":
            (temporary / "manual.txt").write_text("manual", encoding="utf-8")

    restarted = LabArtifactCommitSpool(root)

    assert restarted.fair_pending_paths(limit=1) == (valid.path,)
    assert not os.path.lexists(temporary)
    evidence_paths = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(evidence_paths) == 1
    evidence = job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        evidence_paths[0].read_bytes()
    )
    assert evidence.source_name == temporary.name
    if entry_kind in {"symlink", "hardlink"}:
        assert outside.read_text(encoding="utf-8") == "outside"
    if entry_kind == "nonempty_directory":
        assert (evidence_paths[0].parent / "entry" / "manual.txt").read_text(
            encoding="utf-8"
        ) == "manual"


@pytest.mark.parametrize("failure", ["isolation", "replace", "fsync"])
def test_commit_spool_cursor_io_failure_never_blocks_pending_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    valid = spool.publish(_envelope(tmp_path))
    assert isinstance(valid, LabArtifactCommitSpoolEntry)
    spool._scan_cursor_path.write_text("{}", encoding="utf-8")

    def fail_io(*_args: object, **_kwargs: object) -> None:
        raise OSError(f"{failure} failed")

    with monkeypatch.context() as scoped:
        if failure == "isolation":
            scoped.setattr(spool, "_isolate_scan_cursor_locked", fail_io)
        elif failure == "replace":
            scoped.setattr(artifact_protocol.os, "replace", fail_io)
        else:
            scoped.setattr(spool, "_fsync_directory", fail_io)
        assert spool.fair_pending_paths(limit=1) == (valid.path,)

    assert LabArtifactCommitSpool(root).fair_pending_paths(limit=1) == (valid.path,)


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

    assert quarantined.path.parent.parent == spool.quarantine_dir
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

    evidence = job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        (quarantined.path.parent / "evidence.json").read_bytes()
    )
    assert not os.path.lexists(pending)
    assert external.read_text(encoding="utf-8") == "external evidence"
    assert (external.stat().st_dev, external.stat().st_ino) == (
        original_identity.st_dev,
        original_identity.st_ino,
    )
    assert external.stat().st_nlink == 2
    assert evidence.source_name == pending.name
    assert (evidence.device, evidence.inode, evidence.link_count) == (
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

    with pytest.raises(InvalidCommandEnvelopeError, match="replaced"):
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
        assert external.stat().st_nlink == 2

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


@pytest.mark.parametrize(
    ("max_records", "max_bytes"),
    [(1, 1024 * 1024), (10, 1)],
)
def test_artifact_quarantine_uses_one_budget_for_conflict_and_isolation_records(
    tmp_path: Path,
    max_records: int,
    max_bytes: int,
) -> None:
    spool = LabArtifactCommitSpool(
        tmp_path / "commits",
        max_conflict_records=max_records,
        max_conflict_bytes=max_bytes,
    )
    original = _envelope(tmp_path)
    spool.publish(original)
    malformed = spool.pending_dir / f"{uuid4()}.json"
    malformed.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(malformed)
    assert captured.value.file_identity is not None
    isolated = spool.quarantine(captured.value.file_identity, reason="invalid_json")
    for path in (isolated.path.parent, isolated.path.parent / "evidence.json", isolated.path):
        os.utime(path, ns=(1, 1), follow_symlinks=False)

    conflict = LabArtifactCommitEnvelope(
        request_id=original.request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)

    conflicts = tuple(spool.quarantine_dir.glob("*.conflict.evidence.json"))
    isolations = tuple(spool.quarantine_dir.glob("owned-entry-*.dead"))
    assert len(conflicts) + len(isolations) == 1
    assert len(conflicts) == 1


def test_artifact_quarantine_aggregate_count_budget_has_no_category_off_by_one(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(
        tmp_path / "commits",
        max_conflict_records=2,
        max_conflict_bytes=1024 * 1024,
    )
    original = _envelope(tmp_path)
    spool.publish(original)
    malformed = spool.pending_dir / f"{uuid4()}.json"
    malformed.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(malformed)
    assert captured.value.file_identity is not None
    isolated = spool.quarantine(captured.value.file_identity, reason="invalid_json")
    for path in (isolated.path.parent, isolated.path.parent / "evidence.json", isolated.path):
        os.utime(path, ns=(1, 1), follow_symlinks=False)

    for digit in ("8", "9"):
        conflict = LabArtifactCommitEnvelope(
            request_id=original.request_id,
            commit=original.commit.model_copy(update={"manifest_hash": digit * 64}),
        )
        with pytest.raises(RequestContentConflictError, match="different content"):
            spool.publish(conflict)

    conflicts = tuple(spool.quarantine_dir.glob("*.conflict.evidence.json"))
    isolations = tuple(spool.quarantine_dir.glob("owned-entry-*.dead"))
    assert len(conflicts) + len(isolations) == 2
    assert len(conflicts) == 2


def test_artifact_quarantine_aggregate_byte_budget_is_exact_at_boundary(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(
        root,
        max_conflict_records=10,
        max_conflict_bytes=1024 * 1024,
    )
    original = _envelope(tmp_path)
    spool.publish(original)
    malformed = spool.pending_dir / f"{uuid4()}.json"
    malformed.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(malformed)
    assert captured.value.file_identity is not None
    isolated = spool.quarantine(captured.value.file_identity, reason="invalid_json")
    for path in (isolated.path.parent, isolated.path.parent / "evidence.json", isolated.path):
        os.utime(path, ns=(1, 1), follow_symlinks=False)
    conflict = LabArtifactCommitEnvelope(
        request_id=original.request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)
    with spool._exclusive_lock():
        exact_bytes = sum(record.size for record in spool._artifact_quarantine_records_locked())

    exact = LabArtifactCommitSpool(
        root,
        max_conflict_records=10,
        max_conflict_bytes=exact_bytes,
    )
    assert len(exact._artifact_quarantine_records_locked()) == 2
    below = LabArtifactCommitSpool(
        root,
        max_conflict_records=10,
        max_conflict_bytes=exact_bytes - 1,
    )
    assert len(below._artifact_quarantine_records_locked()) == 1
    assert len(tuple(below.quarantine_dir.glob("*.conflict.evidence.json"))) == 1


def test_artifact_quarantine_manual_dead_letter_counts_against_shared_budget(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(
        tmp_path / "commits",
        max_conflict_records=1,
        max_conflict_bytes=1024 * 1024,
    )
    original = _envelope(tmp_path)
    spool.publish(original)
    malformed = spool.pending_dir / f"{uuid4()}.json"
    malformed.mkdir()
    (malformed / "operator.txt").write_text("retain", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(malformed)
    assert captured.value.file_identity is not None
    isolated = spool.quarantine(captured.value.file_identity, reason="manual_directory")
    for path in (isolated.path.parent, isolated.path.parent / "evidence.json"):
        os.utime(path, ns=(1, 1), follow_symlinks=False)

    conflict = LabArtifactCommitEnvelope(
        request_id=original.request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "9" * 64}),
    )
    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)

    assert isolated.path.is_dir()
    assert (isolated.path / "operator.txt").read_text(encoding="utf-8") == "retain"
    assert tuple(spool.quarantine_dir.glob("*.conflict.evidence.json")) == ()


def test_artifact_quarantine_concurrent_additions_stay_within_shared_budget(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(
        root,
        max_conflict_records=2,
        max_conflict_bytes=1024 * 1024,
    )
    original = _envelope(tmp_path)
    spool.publish(original)
    malformed = spool.pending_dir / f"{uuid4()}.json"
    malformed.write_text("{broken", encoding="utf-8")
    with pytest.raises(InvalidCommandEnvelopeError) as captured:
        spool.load(malformed)
    assert captured.value.file_identity is not None
    spool.quarantine(captured.value.file_identity, reason="invalid_json")
    barrier = Barrier(5)
    outcomes: list[type[BaseException]] = []

    def add_conflict(digit: str) -> None:
        local = LabArtifactCommitSpool(
            root,
            max_conflict_records=2,
            max_conflict_bytes=1024 * 1024,
        )
        conflict = LabArtifactCommitEnvelope(
            request_id=original.request_id,
            commit=original.commit.model_copy(update={"manifest_hash": digit * 64}),
        )
        barrier.wait()
        try:
            local.publish(conflict)
        except BaseException as exc:
            outcomes.append(type(exc))

    threads = [Thread(target=add_conflict, args=(digit,)) for digit in "4568"]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join(timeout=5)

    assert all(not thread.is_alive() for thread in threads)
    assert outcomes == [RequestContentConflictError] * 4
    restarted = LabArtifactCommitSpool(
        root,
        max_conflict_records=2,
        max_conflict_bytes=1024 * 1024,
    )
    conflicts = tuple(restarted.quarantine_dir.glob("*.conflict.evidence.json"))
    isolations = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead"))
    assert len(conflicts) + len(isolations) <= 2


def test_commit_conflict_retention_ignores_lookalike_operator_evidence(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(
        tmp_path / "commits",
        max_conflict_records=1,
        max_conflict_bytes=1024 * 1024,
    )
    lookalike = spool.quarantine_dir / f"{uuid4()}.conflict.evidence.json"
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
        temporary.write_bytes(artifact_protocol.canonical_model_json_bytes(evidence))
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
        isolated = tuple(spool.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
        assert len(isolated) == 1
        record = job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
            isolated[0].read_bytes()
        )
        assert record.source_name == temporary.name
        assert record.byte_count == len(truncation)

    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)
    assert len(spool.conflict_evidence()) == 1
    assert len(tuple(spool.quarantine_dir.glob("owned-entry-*.dead"))) <= 1


def test_commit_conflict_recovery_isolates_exact_owned_temps_but_not_lookalikes(
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

    assert not os.path.lexists(temporaries[0])
    assert outside_symlink.read_text(encoding="utf-8") == "outside symlink"
    assert not os.path.lexists(temporaries[1])
    assert outside_hardlink.read_text(encoding="utf-8") == "outside hardlink"
    assert lookalike.read_text(encoding="utf-8") == "lookalike"
    isolated = tuple(spool.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(isolated) <= 1


@pytest.mark.parametrize(
    "entry_kind",
    ["symlink", "directory", "hardlink", "fifo", "different_regular"],
)
def test_commit_conflict_exact_temporary_abnormal_entry_isolated_then_republished(
    tmp_path: Path,
    entry_kind: str,
) -> None:
    if entry_kind == "fifo" and not hasattr(os, "mkfifo"):
        pytest.skip("FIFO is not supported on this platform")
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(
        root,
        max_conflict_records=8,
        max_conflict_bytes=1024 * 1024,
    )
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
    temporary = spool._conflict_temporary_path(evidence)
    outside = tmp_path / "outside-conflict-temp"
    outside.write_text("outside", encoding="utf-8")
    if entry_kind == "symlink":
        os.symlink(outside, temporary)
    elif entry_kind == "directory":
        temporary.mkdir()
        (temporary / "manual.txt").write_text("manual", encoding="utf-8")
    elif entry_kind == "hardlink":
        os.link(outside, temporary)
    elif entry_kind == "fifo":
        os.mkfifo(temporary)
    else:
        temporary.write_text("not typed conflict evidence", encoding="utf-8")

    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)

    assert not os.path.lexists(temporary)
    completed = spool._conflict_evidence_path(evidence)
    assert LabArtifactConflictEvidence.model_validate_json(completed.read_bytes()) == evidence
    isolated = tuple(spool.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(isolated) == 1
    isolation = job_protocol._LabOwnedEntryIsolationEvidence.model_validate_json(
        isolated[0].read_bytes()
    )
    assert isolation.source_name == temporary.name
    if entry_kind in {"symlink", "hardlink"}:
        assert outside.read_text(encoding="utf-8") == "outside"
    if entry_kind == "directory":
        assert (isolated[0].parent / "entry" / "manual.txt").read_text(encoding="utf-8") == "manual"
        assert isolation.manual_retention is True
    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)
    assert tuple(spool.quarantine_dir.glob("owned-entry-*.dead/evidence.json")) == isolated


def test_commit_conflict_temporary_inode_swap_never_moves_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
    temporary = spool._conflict_temporary_path(evidence)
    temporary.write_text("corrupt", encoding="utf-8")
    replacement_inode: int | None = None

    def swap_after_evidence(stage: str, source: Path, _container: Path) -> None:
        nonlocal replacement_inode
        if stage == "evidence_written" and source == temporary:
            temporary.unlink()
            temporary.write_text("replacement", encoding="utf-8")
            replacement_inode = temporary.stat().st_ino

    monkeypatch.setattr(spool, "_after_owned_entry_isolation_stage", swap_after_evidence)
    spool._recover_conflict_evidence_locked()

    assert replacement_inode is not None
    assert temporary.read_text(encoding="utf-8") == "replacement"
    assert temporary.stat().st_ino == replacement_inode
    monkeypatch.undo()
    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)
    assert (
        LabArtifactConflictEvidence.model_validate_json(
            spool._conflict_evidence_path(evidence).read_bytes()
        )
        == evidence
    )


def test_commit_conflict_corrupt_deterministic_targets_share_bounded_isolation_retention(
    tmp_path: Path,
) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(
        root,
        max_conflict_records=1,
        max_conflict_bytes=1,
    )
    original = _envelope(tmp_path)
    spool.publish(original)
    lookalike = spool.quarantine_dir / f"{uuid4()}.conflict.evidence.json"
    lookalike.write_text("operator evidence", encoding="utf-8")

    for digit in "0123456":
        conflict = LabArtifactCommitEnvelope(
            request_id=original.request_id,
            commit=original.commit.model_copy(update={"manifest_hash": digit * 64}),
        )
        evidence = LabArtifactConflictEvidence.from_conflict(
            conflict,
            reason="request_id already pending with different content",
        )
        target = spool._conflict_evidence_path(evidence)
        target.write_text(f"corrupt-{digit}", encoding="utf-8")
        with pytest.raises(RequestContentConflictError, match="different content"):
            spool.publish(conflict)
        assert LabArtifactConflictEvidence.model_validate_json(target.read_bytes()) == evidence

    restarted = LabArtifactCommitSpool(
        root,
        max_conflict_records=1,
        max_conflict_bytes=1,
    )
    assert len(tuple(restarted.quarantine_dir.glob("owned-entry-*.dead"))) <= 1
    assert len(restarted.conflict_evidence()) == 1
    assert lookalike.read_text(encoding="utf-8") == "operator evidence"


@pytest.mark.parametrize(
    "target_kind",
    ["different_inode", "symlink", "swapped_inode", "appeared_inode"],
)
def test_commit_conflict_recovery_preserves_mismatched_target_and_isolates_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target_kind: str,
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
    temporary = spool._conflict_temporary_path(evidence)
    temporary.write_bytes(evidence.model_dump_json().encode("utf-8"))
    target = spool._conflict_evidence_path(evidence)
    outside = tmp_path / "outside-target"
    outside.write_bytes(evidence.model_dump_json().encode("utf-8"))
    if target_kind == "different_inode":
        os.link(outside, target)
    elif target_kind == "symlink":
        os.symlink(outside, target)
    elif target_kind == "swapped_inode":
        os.link(temporary, target)

        def swap_target(*_args: object) -> None:
            target.unlink()
            os.link(outside, target)

        monkeypatch.setattr(
            spool,
            "_before_conflict_temp_unlink",
            swap_target,
            raising=False,
        )
    else:
        original_link = artifact_protocol.os.link

        def appear_before_link(
            source: str | bytes | Path,
            destination: str | bytes | Path,
            *args: object,
            **kwargs: object,
        ) -> None:
            if Path(source) == Path(temporary.name) and Path(destination) == Path(target.name):
                original_link(outside, target)
            original_link(source, destination, *args, **kwargs)

        monkeypatch.setattr(artifact_protocol.os, "link", appear_before_link)

    spool._recover_conflict_evidence_locked()
    monkeypatch.undo()
    with pytest.raises(RequestContentConflictError, match="different content"):
        spool.publish(conflict)

    assert outside.read_bytes() == evidence.model_dump_json().encode("utf-8")
    assert LabArtifactConflictEvidence.model_validate_json(target.read_bytes()) == evidence
    assert target.stat().st_ino != outside.stat().st_ino
    assert not os.path.lexists(temporary)
    isolated = tuple(spool.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert isolated


@pytest.mark.parametrize("with_outside_link", [False, True])
@pytest.mark.parametrize(
    "crash_stage",
    [None, "evidence_written", "entry_moved"],
)
def test_commit_conflict_corrupt_dual_link_cleanup_restarts_and_republish_converges(
    tmp_path: Path,
    with_outside_link: bool,
    crash_stage: str | None,
) -> None:
    root = tmp_path / "commits"
    spool = _CrashableConflictCleanupSpool(root)
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
    temporary = spool._conflict_temporary_path(evidence)
    target = spool._conflict_evidence_path(evidence)
    payload = b'{"truncated":'
    temporary.write_bytes(payload)
    os.link(temporary, target)
    outside = tmp_path / "outside-third-link"
    if with_outside_link:
        os.link(temporary, outside)
    spool.crash_stage = crash_stage

    if crash_stage is None:
        spool._recover_conflict_evidence_locked()
    else:
        with pytest.raises(_ConflictCleanupCrash, match=crash_stage):
            spool._recover_conflict_evidence_locked()

    restarted = LabArtifactCommitSpool(root)
    assert not os.path.lexists(temporary)
    assert not os.path.lexists(target)
    raw = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/entry"))
    metadata = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(raw) == len(metadata) == 2
    assert all(path.read_bytes() == payload for path in raw)
    if with_outside_link:
        assert outside.read_bytes() == payload
        assert outside.stat().st_ino == raw[0].stat().st_ino == raw[1].stat().st_ino
    with pytest.raises(RequestContentConflictError, match="different content"):
        restarted.publish(conflict)
    assert len(restarted.conflict_evidence()) == 1


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
    assert len(tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))) == 1


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
    payload_only.write_bytes(artifact_protocol.canonical_model_json_bytes(payload_only_conflict))

    metadata_only_conflict = LabArtifactCommitEnvelope(
        request_id=request_id,
        commit=original.commit.model_copy(update={"manifest_hash": "8" * 64}),
    )
    missing_payload = spool.quarantine_dir / (
        f"{request_id}.{metadata_only_conflict.content_hash}.{reason_hash}.conflict.bad"
    )
    metadata_only = Path(f"{missing_payload}.json")
    metadata_only.write_bytes(
        artifact_protocol.canonical_model_json_bytes(
            LabQuarantinedArtifactCommit(
                path=missing_payload,
                reason=reason,
            )
        )
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


def test_artifact_commit_spool_rejects_duplicate_keys_in_pending_and_ack(
    tmp_path: Path,
) -> None:
    spool = LabArtifactCommitSpool(tmp_path / "commits")
    envelope = _envelope(tmp_path)
    pending = spool.publish(envelope)
    assert isinstance(pending, LabArtifactCommitSpoolEntry)
    pending.path.write_bytes(
        pending.path.read_bytes().replace(
            b'"schema_version":2',
            b'"schema_version":999,"schema_version":2',
            1,
        )
    )
    with pytest.raises(InvalidCommandEnvelopeError, match="duplicate JSON key"):
        spool.load(pending.path)

    pending.path.write_bytes(artifact_protocol.canonical_model_json_bytes(envelope))
    receipt = LabArtifactCommitReceipt(
        request_id=envelope.request_id,
        content_hash=envelope.content_hash,
        job_id=envelope.commit.job_id,
        status="accepted",
        reason="complete",
        accepted_at=datetime(2026, 7, 29, tzinfo=UTC),
    )
    acknowledged = spool.ack(spool.load(pending.path), receipt)
    acknowledged.path.write_bytes(
        acknowledged.path.read_bytes().replace(
            b'"status":"accepted"',
            b'"status":"rejected","status":"accepted"',
            1,
        )
    )
    with pytest.raises(InvalidCommandEnvelopeError, match="duplicate JSON key"):
        spool.load_receipt(acknowledged.path)


def test_artifact_cursor_recovery_rejects_nested_duplicate_keys(tmp_path: Path) -> None:
    root = tmp_path / "commits"
    spool = LabArtifactCommitSpool(root)
    pending = spool.publish(_envelope(tmp_path))
    assert isinstance(pending, LabArtifactCommitSpoolEntry)
    assert spool.fair_pending_paths(limit=1) == (pending.path,)
    cursor = spool._scan_cursor_path
    cursor.write_bytes(
        cursor.read_bytes().replace(
            b'"schema_version":1',
            b'"schema_version":999,"schema_version":1',
            1,
        )
    )

    restarted = LabArtifactCommitSpool(root)

    assert restarted.fair_pending_paths(limit=1) == (pending.path,)
    isolated = tuple(restarted.quarantine_dir.glob("owned-entry-*.dead/evidence.json"))
    assert len(isolated) == 1
