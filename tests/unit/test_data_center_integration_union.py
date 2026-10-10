"""Exercise the shared original command, read and screening paths after union."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "factory", ["build_page_control_service", "build_page_control_service_with_dependencies"]
)
def test_single_original_outbox_recovers_task_control_and_m1_pause(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    factory: str,
) -> None:
    from rquant import page_control_service
    from rquant.backfill_execute_page_backend import (
        BackfillExecutePageBackend,
        BackfillExecutePageBackendConfig,
    )
    from rquant.page_control import (
        PageControlStatus,
        PauseDataCenterExecution,
        parse_page_control_command,
    )
    from tests.unit.test_backfill_execute_admission import _state_execution
    from tests.unit.test_task_control_admission import NOW, REQUEST, control_service, request

    root = tmp_path / "data-center"
    root.mkdir(mode=0o700)
    state, spec, manifest, now = _state_execution(root, owner="alice")
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec, manifest, now=now)
    data_backend = BackfillExecutePageBackend(
        BackfillExecutePageBackendConfig(
            policy_path=root / "policy.json",
            original_state_path=state.path,
            plan_state_path=root / "plans.sqlite",
            plan_directory=root / "plans",
            audit_state_path=root / "audit.sqlite",
            audit_directory=root / "audit",
            collection_directory=root / "collection",
            allowed_owners=("alice",),
        ),
        clock=lambda: NOW,
    )
    old, tasks, source, _executor, calls = control_service(tmp_path, monkeypatch)
    control = getattr(page_control_service, factory)(
        outbox_path=old.outbox.path,
        data_dir=tmp_path / "data",
        log_dir=tmp_path / "logs",
        allowed_lab_export_roots=(),
        load_default_lab_backend=False,
        clock=lambda: NOW,
        task_control_backend=tasks,
        data_center_execution_backend=data_backend,
    )
    assert control.outbox is tasks.journal.outbox
    assert control.consumer.task_control_backend is tasks
    original_task = request()
    task_receipt = control._submit_trusted_task_control(
        original_task,
        authenticated_actor_id="alice",
        verified_metadata_identity=tasks.journal.identity(),
    )
    assert calls == [REQUEST]
    with pytest.raises(ValueError, match="trusted|private"):
        parse_page_control_command(original_task.model_dump(mode="json"))
    pause = PauseDataCenterExecution(
        command_id="m1-joint-pause",
        requested_at=NOW,
        actor_id="alice",
        execution_id=spec.execution_id,
        expected_sequence=1,
    )
    assert parse_page_control_command(pause.model_dump(mode="json")) == pause
    receipt = control.submit(pause)
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert state.get_maintenance_status(spec.execution_id, owner="alice").pause_requested
    assert state.get_maintenance_status(spec.execution_id, owner="alice").control_sequence == 2
    monkeypatch.setattr(
        data_backend.state,
        "maintenance_control",
        lambda *a, **k: pytest.fail("same M1 UUID repeated effect"),
    )
    monkeypatch.setattr(
        source, "read", lambda **k: pytest.fail("old task lookup read a newer source")
    )
    assert control.submit(pause) == receipt
    assert (
        control._resume_trusted_task_control(original_task, authenticated_actor_id="alice")
        == task_receipt
    )
    assert calls == [REQUEST]
    assert control.outbox.receipt(REQUEST) == task_receipt


def test_original_screen_transaction_and_evidence_survive_under_m1_writer_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import pandas as pd

    import rquant.pipeline as pipeline
    from rquant.storage.duckdb import DuckDBStore
    from rquant.storage.primary_writer_gate import (
        PrimaryWriterBusy,
        PrimaryWriterGate,
        PrimaryWriterGateConfig,
    )
    from tests.unit.test_daily_screen_reproducible import _daily_preset
    from tests.unit.test_screen_dynamic_ma import _world

    _, primary, _, days, _, _ = _world(tmp_path)
    lock = tmp_path / "writer.lock"
    lock.write_bytes(b"")
    lock.chmod(0o600)
    gate = PrimaryWriterGateConfig.capture(primary_path=primary, lock_path=lock)
    monkeypatch.setattr(pipeline, "PRESET_SCREENS", {"reproducible": _daily_preset()})
    with DuckDBStore(primary, primary_writer_gate=gate) as store:
        with pytest.raises(PrimaryWriterBusy):
            PrimaryWriterGate(gate).acquire()
        first = pipeline.run_daily_screen_stage(
            days[0].isoformat(),
            preset_names=["reproducible"],
            store=store,
            preset_directory=tmp_path / "presets",
        )
        assert first.errors == ()
        before_rows = store.query_screen_result(days[0].isoformat(), "reproducible")
        before_receipt = store.query_screen_run_receipt(days[0].isoformat(), "reproducible")
        before_proof = store.query_screen_run_evidence(days[0].isoformat(), "reproducible")
        assert before_proof is not None and before_receipt is not None
        assert before_proof.result_version == before_receipt.result_version
        assert store._conn.execute("SELECT count(*) FROM ingestion_commit_receipt").fetchone() == (
            0,
        )

        def fail(_proof: object) -> None:
            raise OSError("joint original evidence write failure")

        monkeypatch.setattr(store, "_upsert_screen_run_evidence", fail)
        second = pipeline.run_daily_screen_stage(
            days[0].isoformat(),
            preset_names=["reproducible"],
            store=store,
            preset_directory=tmp_path / "presets",
        )
        assert second.preset_hits == {"reproducible": -1}
        pd.testing.assert_frame_equal(
            before_rows, store.query_screen_result(days[0].isoformat(), "reproducible")
        )
        assert store.query_screen_run_receipt(days[0].isoformat(), "reproducible") == before_receipt
        assert store.query_screen_run_evidence(days[0].isoformat(), "reproducible") == before_proof
    with PrimaryWriterGate(gate).acquire():
        pass


def test_complete_original_m1_projection_group_coexists_with_task_owner_validation(
    tmp_path: Path,
) -> None:
    from rquant.backfill_execute_projection import project_data_center_execution
    from rquant.ops_status_serving import ops_status_projections
    from rquant.serving_page_projection_source import LabPageProjectionSnapshot
    from rquant.serving_read_models import ServingProjectionInput, require_projection_owner_budget
    from tests.unit.test_backfill_execute_admission import _state_execution
    from tests.unit.test_task_center_projection import _task_sample

    state, spec, manifest, now = _state_execution(tmp_path)
    state.persist_maintenance_intent(spec.intent)
    state.admit_backfill_execution(spec, manifest, now=now)
    observed = now + timedelta(seconds=1)
    group = project_data_center_execution(
        state_path=state.path, policy_path=None, observed_at=observed
    )
    snapshot = LabPageProjectionSnapshot.create(
        available_at=observed, data_center_execution_projections=group
    )
    assert any(table.table_name == "data_center_execution_event" for table in snapshot.projections)
    with pytest.raises(ValueError, match="incomplete"):
        LabPageProjectionSnapshot.create(
            available_at=observed,
            data_center_execution_projections=tuple(
                table for table in group if table.table_name != "data_center_execution_event"
            ),
        )
    combined = tuple(
        ServingProjectionInput.bind(
            table, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
        )
        for table in snapshot.projections
    ) + tuple(
        ServingProjectionInput.bind(
            table, owner_dataset_id="ops_status", owner_generation_id="b" * 64
        )
        for table in ops_status_projections(_task_sample())
    )
    require_projection_owner_budget(combined)
    execution = next(table for table in combined if table.table_name == "data_center_execution")
    with pytest.raises(ValueError, match="owner"):
        ServingProjectionInput.model_validate(
            execution.model_dump() | {"owner_dataset_id": "ops_status"}
        )


def test_private_m1_http_keeps_original_task_capabilities_csrf_and_body_caps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.testclient import TestClient

    from tests.support.web_proxy_identity import ProofTestClient
    from tests.unit.test_web_task_center_controls import PREFIX, setup

    app, _control, _backend, _source, calls, body, _clock = setup(tmp_path, monkeypatch)
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        task = client.get(PREFIX + "/control-capabilities")
        assert task.status_code == 200 and task.json()["units"][0]["unit"] == body.unit
        assert client.post("/api/v1/data/executions/commands", json={}).status_code == 403
        assert (
            client.post(
                "/api/v1/data/executions/commands",
                content=b" " * 4097,
                headers={"x-rquant-csrf": "1", "Content-Type": "application/json"},
            ).status_code
            == 413
        )
        assert (
            client.post(
                PREFIX + "/scheduling/commands",
                content=b" " * 1025,
                headers={"x-rquant-csrf": "1", "Content-Type": "application/json"},
            ).status_code
            == 413
        )
    assert calls == []
    with TestClient(app) as client:
        assert (
            client.get("/api/v1/data/executions", headers={"x-rquant-user": "alice"}).status_code
            == 401
        )
