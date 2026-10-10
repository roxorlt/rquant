"""Root-only physical M13 gates; synthetic inputs and peer UID are explicit."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
import multiprocessing
import os
from pathlib import Path
import stat
import tempfile
import threading
from typing import Iterator
from uuid import uuid4

import pytest


def test_tsc_09_profile_original_isolated_ack_pause_drain_resume_and_sealed_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.lab_scheduler import LabScheduler
    from rquant.lab_shard_protocol import LabClaimSpool
    from rquant.lab_worker import LabWorker
    from rquant.lab_scheduling_control import LabSchedulingCommandEnvelope, LabSchedulingMaintenanceScope, PauseSchedulingCommand, ResumeSchedulingCommand
    from rquant.strict_json import canonical_model_json_bytes
    from rquant.task_center_runtime import TaskCenterControlProfile, build_lab_scheduling_control, task_center_worker_barrier_identity
    from tests.integration import test_strategy_template_native as original

    scheduler: LabScheduler | None = None
    paused: object | None = None
    proof: list[str] = []
    start = LabClaimSpool.scheduling_execution_start
    close = LabClaimSpool.close_scheduling_execution
    children = {child.pid for child in multiprocessing.active_children()}

    def make_scheduler(**kwargs: object) -> LabScheduler:
        nonlocal scheduler
        store, claims, reports, commits, final = (kwargs[key] for key in ("store", "claim_spool", "report_spool", "artifact_commit_spool", "artifact_store"))
        root = store.path.parent
        scope = LabSchedulingMaintenanceScope(report_root=reports.root, artifact_commit_root=commits.root, final_artifact_root=final.root)
        identity = store.scheduling_identity()
        profile = TaskCenterControlProfile(producer_commit=kwargs["runtime_guard"](), runtime_root=root, enabled=True,
            allow_local_migration=True, queue_identity=identity, claim_spool_root=claims.root,
            claim_spool_generation=(claims.root.stat().st_dev, claims.root.stat().st_ino), maintenance_scope=scope,
            maintenance_generations=tuple((path.stat().st_dev, path.stat().st_ino) for path in (reports.root, commits.root, final.root)))
        path = root / "task-center-control.json"
        path.write_bytes(canonical_model_json_bytes(profile)); path.chmod(0o600)
        port = build_lab_scheduling_control(path, store=store, producer_commit=profile.producer_commit, runtime_root=root,
            claim_spool_root=claims.root, maintenance_scope=scope, production_mode=False)
        scheduler = LabScheduler(**(kwargs | {"scheduling_control": port}))
        scheduler.run_once()
        assert identity.schema_version == 16 and store.scheduling_identity().schema_version == 17
        assert identity.implementation_digest == store.scheduling_identity().implementation_digest
        proof.append("explicit-local-16-to-17-original-implementation")
        return scheduler

    def make_worker(**kwargs: object) -> LabWorker:
        assert scheduler is not None
        root = scheduler.store.path.parent
        claims = kwargs["claim_spool"]
        identity = task_center_worker_barrier_identity(root / "task-center-control.json", producer_commit=kwargs["verified_code_sha_provider"](),
            runtime_root=root, claim_spool_root=claims.root, production_mode=False)
        return LabWorker(**(kwargs | {"claim_spool": LabClaimSpool(claims.root, expected_scheduling_barrier_identity=identity)}))

    @contextmanager
    def after_real_ack(self: LabClaimSpool, claim: object, *, now: datetime) -> Iterator[None]:
        nonlocal paused
        with start(self, claim, now=now):
            yield
        assert scheduler is not None
        if paused is None and self.root == scheduler.scheduling_control.root:
            port = scheduler.scheduling_control
            envelope = LabSchedulingCommandEnvelope(request_id=uuid4(), command=PauseSchedulingCommand(
                queue_identity=scheduler.store.scheduling_identity(), expected_version=0, accepted_at=datetime.now(UTC)))
            paused = port.apply_command(envelope, lease=scheduler.lease, now=datetime.now(UTC))
            state = port.reconcile(lease=scheduler.lease, now=datetime.now(UTC))
            assert state.desired_paused and not state.applied_paused and state.draining_count == 1
            assert port.read_barrier().state == "closed"
            proof.append("real-ack-before-pause-child-drains")

    def after_real_close(self: LabClaimSpool, claim: object, *, now: datetime) -> None:
        close(self, claim, now=now)
        assert scheduler is not None
        if paused is not None and self.root == scheduler.scheduling_control.root:
            port = scheduler.scheduling_control
            state = port.reconcile(lease=scheduler.lease, now=datetime.now(UTC))
            assert state.applied_paused and state.draining_count == 0
            proof.append("real-child-close-before-applied-paused")
            resume = LabSchedulingCommandEnvelope(request_id=uuid4(), command=ResumeSchedulingCommand(
                queue_identity=state.queue_identity, expected_version=1, accepted_at=datetime.now(UTC)))
            port.apply_command(resume, lease=scheduler.lease, now=datetime.now(UTC))
            opened = port.reconcile(lease=scheduler.lease, now=datetime.now(UTC))
            assert opened.desired_version == opened.applied_version == 2 and not opened.applied_paused
            proof.append("resume-version-two-after-real-drain")

    monkeypatch.setattr(original, "LabScheduler", make_scheduler)
    monkeypatch.setattr(original, "LabWorker", make_worker)
    monkeypatch.setattr(LabClaimSpool, "scheduling_execution_start", after_real_ack)
    monkeypatch.setattr(LabClaimSpool, "close_scheduling_execution", after_real_close)
    try:
        original.test_new_template_cold_host_original_worker_and_sealed_latest(tmp_path)
        assert proof == ["explicit-local-16-to-17-original-implementation", "real-ack-before-pause-child-drains",
            "real-child-close-before-applied-paused", "resume-version-two-after-real-drain"]
        assert scheduler.store.scheduling_state().applied_version == 2
        print("M13_NATIVE_PROFILE_ACK_DRAIN_PROOF=" + "/".join(proof) + "; original-template-page-journal/worker/broker/finalizer/sealed-reader; synthetic-inputs/nonproduction-code-identity")
    finally:
        if scheduler is not None:
            scheduler.release()
        assert {child.pid for child in multiprocessing.active_children()} <= children


def test_tsc_01_real_private_task_transport_prepare_original_uuid_recovery_and_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.page_control import PageControlStatus
    from rquant.task_control_admission import TaskControlAdmission, TaskControlAdmissionClient, TaskControlAdmissionRejectedError, build_task_control_admission_server
    from rquant.task_control_commands import PrepareUnitRun, TaskUnitRunDraft
    from tests.unit.test_task_control_admission import NOW, PREPARE, REQUEST, control_service, request

    service, backend, source, _executor, starts = control_service(tmp_path, monkeypatch, writer=True)
    admission = TaskControlAdmission(service, backend=backend)
    web_uid = os.geteuid() + 1
    server = None
    worker = None
    with tempfile.TemporaryDirectory(prefix="m13-private-", dir="/private/tmp") as directory:
        root = Path(directory)
        os.chown(root, os.geteuid(), os.getegid(), follow_symlinks=False); root.chmod(0o710)
        assert (root.stat().st_uid, root.stat().st_gid, stat.S_IMODE(root.stat().st_mode)) == (os.geteuid(), os.getegid(), 0o710)
        path = root / "tasks.sock"
        try:
            server = build_task_control_admission_server(admission, socket_path=path, trusted_web_uid=web_uid,
                shared_gid=os.getegid(), peer_uid=lambda _connection: web_uid)
            assert server is not None and stat.S_IMODE(path.stat().st_mode) == 0o660
            worker = threading.Thread(target=server.serve_forever, name="m13-private-transport", daemon=False)
            worker.start()
            client = TaskControlAdmissionClient(path, expected_service_uid=os.geteuid(), shared_gid=os.getegid(), client_uid=lambda: web_uid)
            body = request()
            draft = TaskUnitRunDraft.model_validate(body.model_dump(exclude={"kind", "confirmation_id"}))
            prepare = PrepareUnitRun(command_id=PREPARE, requested_at=NOW, generation_id="generation-a", run=draft)
            assert client.lookup(prepare, authenticated_actor_id="alice") is None
            challenge = client.submit(prepare, authenticated_actor_id="alice", verified_metadata_identity=backend.journal.identity())
            assert challenge.receipt.status is PageControlStatus.SUCCEEDED and starts == []
            body = type(body).model_validate(body.model_dump() | {"confirmation_id": challenge.confirmation.confirmation_id})
            result = client.submit(body, authenticated_actor_id="alice", verified_metadata_identity=challenge.metadata_identity)
            assert result.unit_effect.stage == "unknown" and starts == [REQUEST]
            backend.enabled = False
            monkeypatch.setattr(source, "read", lambda **_: pytest.fail("original private recovery must not read fresh source"))
            assert client.lookup(body, authenticated_actor_id="alice") == result
            assert client.resume(body, authenticated_actor_id="alice").unit_effect.stage == "unknown" and starts == [REQUEST]
            with pytest.raises(TaskControlAdmissionRejectedError):
                client.lookup(body, authenticated_actor_id="bob")
            with pytest.raises(TaskControlAdmissionRejectedError):
                client.lookup(type(body).model_validate(body.model_dump() | {"generation_id": "generation-b"}), authenticated_actor_id="alice")
            print("M13_REAL_AF_UNIX_0710_0660=True; ORIGINAL_PREPARE_JOURNAL_UUID_RESUME=True; OWNER_BODY_REJECT=True; PEER_UID_INJECTED=True; DISTINCT_OS_UID=False; UNIT_START_LEAF_SYNTHETIC=True; LINUX_WITNESS_NOT_PROVEN=True")
        finally:
            if server is not None:
                server.shutdown()
                if worker is not None:
                    worker.join(timeout=3)
                    assert not worker.is_alive()
                server.server_close()
            assert not path.exists()
    assert not root.exists()
