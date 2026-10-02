"""The explicit local entry never treats a historical source as today's update."""

import subprocess
import sys
from datetime import timedelta
from pathlib import Path


def test_tracking_due_tick_requires_actual_calendar_and_scheduler_bounds(tmp_path: Path) -> None:
    from rquant.factor.tracking_entry import build_factor_tracking_scheduler
    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_tracking_runner import _AT, _generation, _joined, _sources

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = FactorTrackingRunner(
        sources[1], reference, identity, clock=lambda: _AT + timedelta(hours=11)
    )
    result = runner.due_tick()
    assert len(result) == 1 and result[0].status == "waiting"
    assert "当日" in result[0].reason and not store.days("tracked", expected_identity=identity)
    scheduler = build_factor_tracking_scheduler(runner)
    job = scheduler.get_jobs()[0]
    assert job.max_instances == 1 and job.coalesce
    assert str(job.trigger.timezone) == "Asia/Shanghai"
    assert "mon-fri" in str(job.trigger) and "18" in str(job.trigger) and "40" in str(job.trigger)
    assert not scheduler.running
    from tests.unit.test_factor_source_prepare import _FIRST

    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=20)).status == "updated"
    old = store.days("tracked", expected_identity=identity)
    assert runner.due_tick()[0].status == "waiting"
    assert store.get("tracked", expected_identity=identity).status == "waiting"
    assert store.days("tracked", expected_identity=identity) == old


def test_tracking_explicit_history_cli_uses_original_private_state(tmp_path: Path) -> None:
    from rquant.factor.tracking_runner import FactorTrackingRunOutcome
    from tests.unit.test_factor_source_prepare import _FIRST
    from tests.unit.test_factor_tracking_runner import _generation, _joined, _sources

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.factor.tracking_entry",
            "run-history",
            "--root",
            str(sources[1]),
            "--reference",
            reference.model_dump_json(),
            "--tracking-identity",
            identity.model_dump_json(),
            "--factor-id",
            "tracked",
            "--target-end",
            (_FIRST + timedelta(days=20)).isoformat(),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    outcome = FactorTrackingRunOutcome.model_validate_json(result.stdout)
    assert outcome.status == "updated" and len(outcome.evaluation_days) == 18
    assert store.get("tracked", expected_identity=identity).cursor == _FIRST + timedelta(days=20)
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))


def test_tracking_private_to_worker_projection_and_archived_head_pause(tmp_path: Path) -> None:
    import os
    from tempfile import TemporaryDirectory
    from threading import Thread
    from uuid import uuid4

    from rquant.factor.registry import ArchiveFactorRequest, FactorDefinitionRegistry, FactorHeadRef
    from rquant.factor.tracking import FactorTrackingRequest, FactorTrackingStore
    from rquant.factor.tracking_backend import FactorTrackingPageControlBackend
    from rquant.factor.tracking_runner import FactorTrackingRunner
    from rquant.factor.tracking_serving import project_factor_tracking_snapshot
    from rquant.factor_tracking_admission import (
        FactorTrackingAdmission,
        FactorTrackingAdmissionClient,
        build_factor_tracking_admission_server,
    )
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
    from tests.unit.test_factor_source_prepare import _FIRST
    from tests.unit.test_factor_tracking_runner import _AT, _generation, _sources

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store = FactorTrackingStore(tmp_path / "tracking.sqlite", clock=lambda: _AT)
    identity = store.initialize()
    registry = FactorDefinitionRegistry(Path(sources[2].path))
    head = registry.get_head("tracked", expected_identity=sources[2])
    expected = FactorHeadRef(version=1, content_sha256=head.content_sha256)
    request = FactorTrackingRequest(
        command_id=str(uuid4()),
        requested_at=_AT,
        serving_generation_id="c" * 64,
        factor_id="tracked",
        tracked=True,
        expected_head=expected,
    )
    backend = FactorTrackingPageControlBackend(
        sources[1], reference, identity, enabled=True, tracking_users=frozenset({"alice"})
    )
    outbox = PageControlOutbox(tmp_path / "control.sqlite")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path,
            log_dir=tmp_path,
            factor_tracking_backend=backend,
            clock=lambda: _AT,
        ),
    )
    with TemporaryDirectory(prefix="ft-e2e-", dir="/private/tmp") as directory:
        private = Path(directory)
        os.chown(private, os.geteuid(), os.getegid())
        private.chmod(0o710)
        socket = private / "control.sock"
        uid = os.geteuid() + 1
        server = build_factor_tracking_admission_server(
            FactorTrackingAdmission(service, enabled=True, tracking_users=frozenset({"alice"})),
            socket_path=socket,
            trusted_web_uid=uid,
            shared_gid=os.getegid(),
            peer_uid=lambda _: uid,
        )
        thread = Thread(target=server.serve_forever, daemon=True, name="synthetic-tracking-e2e")
        thread.start()
        try:
            client = FactorTrackingAdmissionClient(
                socket,
                expected_service_uid=os.geteuid(),
                shared_gid=os.getegid(),
                client_uid=lambda: uid,
            )
            receipt = client.submit(
                request,
                authenticated_actor_id="alice",
                verified_registry_instance_id=sources[2].instance_id,
            )
            assert receipt.status == "applied"
            runner = FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
            outcome = runner.run_history("tracked", target_end=_FIRST + timedelta(days=20))
            assert outcome.status == "updated" and outcome.job_id is not None
            projection = project_factor_tracking_snapshot(
                identity, registry_identity=sources[2], available_at=_AT
            )
            assert projection.panels[0].summary.latest_trade_date == _FIRST + timedelta(days=20)
            old = store.days("tracked", expected_identity=identity)
            registry.archive(
                ArchiveFactorRequest(
                    command_id="archive-tracked", factor_id="tracked", expected_head=expected
                ),
                expected_identity=sources[2],
            )
            assert (
                runner.run_history("tracked", target_end=_FIRST + timedelta(days=21)).status
                == "paused"
            )
            assert store.days("tracked", expected_identity=identity) == old
            assert client.resume(request, authenticated_actor_id="alice") == receipt
        finally:
            server.shutdown()
            thread.join(timeout=3)
            server.server_close()
            assert not thread.is_alive() and not socket.exists()
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))
