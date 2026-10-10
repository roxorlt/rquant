"""The original ops publisher must bind the complete new material graph."""

from __future__ import annotations

import importlib
import importlib.util
import json
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import TYPE_CHECKING

import pytest

from rquant.ops_status import STATIC_TIMER_STEMS, OpsSnapshot, OpsResourceEvidence, OpsUnitEvidence
from rquant.ops_status import OpsStatusCollector
from rquant.ops_status_serving import ops_status_source_result, publish_ops_status_snapshot
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.runtime_serving_snapshot import OpsStatusPayload
from rquant.serving_read_models import ServingProjectionPayload
from tests.unit.test_task_cpu import NOW, SLICES, _observation

if TYPE_CHECKING:
    from rquant.lab_scheduling_control import LabSchedulingControlState
    from rquant.runtime_serving_snapshot import SourceReadResult
    from rquant.task_center_projection import TaskOpsSample


def _api() -> ModuleType:
    assert importlib.util.find_spec("rquant.task_center_projection") is not None, "trusted task source graph missing"
    return importlib.import_module("rquant.task_center_projection")


def _sample(*, at: datetime = NOW + timedelta(seconds=2)) -> OpsSnapshot:
    return OpsSnapshot(sampled_at=at, host_name="synthetic-test", boot_id="12345678-1234-1234-1234-123456789abc", manifest_digest="a" * 64,
        units=tuple(OpsUnitEvidence(timer=f"rquant-{stem}.timer", service=f"rquant-{stem}.service", label="定时任务", expected_enabled=True, session="all", resource_group="maintenance") for stem in STATIC_TIMER_STEMS),
        resources=tuple(OpsResourceEvidence(slice_name=name) for name in SLICES))


def _task_sample() -> TaskOpsSample:
    from rquant.task_cpu import TaskCpuPair, compute_task_cpu

    api = _api()
    cpu = compute_task_cpu(TaskCpuPair(previous=_observation(), current=_observation(later=True)), cutoff=NOW + timedelta(seconds=2))
    return api.TaskOpsSample(snapshot=_sample(), evidence=api.TaskOpsEvidence(cpu=cpu))


def test_tsc_12_exact_tables_and_full_graph() -> None:
    task = _task_sample()
    result = ops_status_source_result(task)
    assert isinstance(result.payload, OpsStatusPayload)
    assert result.payload.task_evidence == task.evidence
    tables = {table.table_name: table for table in result.payload.projections}
    assert set(tables) == {"ops_host_status", "ops_unit_status", "ops_resource_status", "ops_task_cpu", "ops_task_runs"}
    assert len(tables["ops_task_cpu"].rows) == 5
    assert tables["ops_task_cpu"].rows[2]["percent"] == "40.0"
    assert tables["ops_task_cpu"].rows[-1]["percent"] is None
    assert tables["ops_task_runs"].rows == ()


def test_tsc_12_projection_payload_swap() -> None:
    task = _task_sample()
    result = ops_status_source_result(task)
    projections = list(result.payload.projections)
    index = next(i for i, projection in enumerate(projections) if projection.table_name == "ops_task_cpu")
    rows = list(projections[index].rows)
    rows[2] = dict(rows[2]) | {"percent": "95.0"}
    projections[index] = ServingProjectionPayload(table_name="ops_task_cpu", available_at=task.snapshot.sampled_at, rows=tuple(rows))
    values = result.payload.model_dump(mode="python") | {"projections": projections}
    with pytest.raises(ValueError, match="match"):
        OpsStatusPayload.model_validate(values)


def test_tsc_03_future_material() -> None:
    task = _task_sample()
    api = _api()
    with pytest.raises(ValueError, match="cutoff"):
        api.TaskOpsSample(snapshot=_sample(at=NOW), evidence=task.evidence)
    snapshot = _sample().model_dump(mode="python") | {"host_name": "wrong-host"}
    with pytest.raises(ValueError, match="identity"):
        api.TaskOpsSample(snapshot=snapshot, evidence=task.evidence)


def test_tsc_04_total_not_group_sum() -> None:
    task = _task_sample()
    assert task.evidence.cpu.groups[0].percent == "5.0"
    assert task.evidence.cpu.groups[2].percent == "40.0"


def test_original_authority_reopens_exact_cpu_source(tmp_path: Path) -> None:
    task = _task_sample()
    root = tmp_path / "ops"
    publish_ops_status_snapshot(task, root=root, producer_commit="b" * 40, clock=lambda: NOW + timedelta(seconds=3))
    reader = ServingSourceAuthorityReader(root=root, expected_producer_commit="b" * 40, expected_dataset_id="ops_status", expected_payload_kind="ops_status", max_bytes=512 * 1024)
    result = reader(NOW + timedelta(seconds=3))
    assert result.payload.task_evidence == task.evidence
    assert result.payload.snapshot == task.snapshot
    # Full numeric result and raw facts survive the original immutable publication protocol.
    assert result.payload.task_evidence.cpu.groups[2].percent == "40.0"


def test_bad_capture_does_not_fill_cpu_zero_or_break_old_units() -> None:
    api = _api()
    task = api.TaskOpsSample(snapshot=_sample(), evidence=api.TaskOpsEvidence(cpu=None, cpu_unavailable_reason="capture_unavailable"))
    result = ops_status_source_result(task)
    rows = next(t.rows for t in result.payload.projections if t.table_name == "ops_task_cpu")
    assert len(rows) == 5 and all(row["percent"] is None for row in rows)
    assert len(result.payload.snapshot.units) == 14
    assert all(unit.last_result is None for unit in result.payload.snapshot.units)


def test_original_legacy_source_bytes_do_not_add_an_optional_null_field() -> None:
    legacy = ops_status_source_result(_sample())
    assert set(legacy.payload.model_dump(mode="json")) == {"payload_kind", "snapshot", "projections"}


def test_tsc_07_actual_original_journal_result_is_published_by_original_owner(tmp_path: Path) -> None:
    from rquant.task_center_runtime import TaskUnitRunSource
    from rquant.task_center_projection import TaskOpsEvidence, TaskOpsSample
    from tests.unit.test_task_control_admission import NOW as UNIT_NOW, persisted_started_unit

    _outbox, journal, command, _completed = persisted_started_unit(tmp_path)
    at = UNIT_NOW + timedelta(seconds=8)
    snapshot = OpsSnapshot.model_validate(_sample(at=at).model_dump() | {"host_name": command.context.host_name,
        "boot_id": command.context.boot_id, "manifest_digest": command.context.manifest_digest})
    runs = TaskUnitRunSource(identity=journal.identity()).read(host_name=snapshot.host_name, boot_id=snapshot.boot_id,
        manifest_digest=snapshot.manifest_digest, units=tuple(unit.service for unit in snapshot.units), cutoff=at)
    sample = TaskOpsSample(snapshot=snapshot, evidence=TaskOpsEvidence(cpu=None, cpu_unavailable_reason="capture_unavailable", runs=runs))
    root = tmp_path / "owner"
    publish_ops_status_snapshot(sample, root=root, producer_commit="b" * 40, clock=lambda: at)
    published = ServingSourceAuthorityReader(root=root, expected_producer_commit="b" * 40, expected_dataset_id="ops_status",
        expected_payload_kind="ops_status", max_bytes=512 * 1024)(at)
    payload = published.payload
    assert payload.task_evidence.runs == runs and runs[0].request_id == command.command_id
    run_table = next(table for table in payload.projections if table.table_name == "ops_task_runs")
    assert run_table.rows[0]["invocation_id"] == runs[0].invocation_id and run_table.rows[0]["ended_at"] is None
    assert run_table.rows[0]["result"] is None and runs[0].status == "started"


def test_tsc_07_collector_consumes_only_concrete_original_run_reader_inside_original_budget(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_center_runtime import TaskUnitRunSource
    from rquant.task_cpu import LinuxTaskCpuReader
    from tests.unit.test_task_control_admission import NOW as UNIT_NOW, persisted_started_unit
    from tests.unit.test_ops_status import _manifest

    _outbox, journal, command, _completed = persisted_started_unit(tmp_path)
    source = TaskUnitRunSource(identity=journal.identity())
    at = UNIT_NOW + timedelta(seconds=8)
    snapshot = OpsSnapshot.model_validate(_sample(at=at).model_dump() | {"host_name": command.context.host_name,
        "boot_id": command.context.boot_id, "manifest_digest": command.context.manifest_digest})
    collector = OpsStatusCollector(clock=lambda: at, monotonic=lambda: 0)
    monkeypatch.setattr(collector, "collect", lambda _: snapshot)
    monkeypatch.setattr(collector, "_show_task_cpu", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(LinuxTaskCpuReader, "capture", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("private kernel unavailable")))
    sample = collector.collect_tasks(_manifest(), previous=None, cpu_reader=LinuxTaskCpuReader(), run_source=source)
    assert sample.evidence.runs == (journal.run_effect(command).run,) and sample.evidence.cpu is None
    assert sample.snapshot.sampled_at == at
    with pytest.raises(TypeError, match="exact|reader"):
        collector.collect_tasks(_manifest(), previous=None, cpu_reader=LinuxTaskCpuReader(), run_source=object())


def test_collector_uses_original_deadline_then_binds_kernel_before_final_cutoff(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_cpu import LinuxTaskCpuReader, TaskCpuObservation
    from tests.unit.test_ops_status import BOOT, MEMINFO, _manifest

    api = _api()
    assert hasattr(OpsStatusCollector, "collect_tasks"), "actual ops collector task entry missing"
    manifest = _manifest()
    calls: list[tuple[str, ...]] = []
    capture_limits: list[float] = []

    def command(argv: tuple[str, ...], timeout: float, limit: int) -> bytes:
        calls.append(argv)
        assert timeout <= 1 and limit <= 4096
        if "ControlGroup" in argv[-1]:
            group = argv[2]
            return f"LoadState=loaded\nControlGroup=/rquant.slice{'' if group == 'rquant.slice' else '/' + group}\nInvocationID={'1' * 32}\n".encode()
        return b"LoadState=loaded\n"

    def capture(self: LinuxTaskCpuReader, *, host_name: str, boot_id: str, manifest_digest: str, properties: object, max_seconds: float, clock: object) -> TaskCpuObservation:
        capture_limits.append(max_seconds)
        sample = _observation(later=True) | {"host_name": host_name, "boot_id": boot_id, "manifest_digest": manifest_digest}
        return TaskCpuObservation.model_validate(sample)

    monkeypatch.setattr(LinuxTaskCpuReader, "capture", capture)
    collector = OpsStatusCollector(command_runner=command, proc_reader=lambda path, _: BOOT if path.endswith("boot_id") else MEMINFO, clock=lambda: NOW + timedelta(seconds=3), monotonic=lambda: 1.0, host_name=lambda: manifest.host_name)
    previous = TaskCpuObservation.model_validate(_observation() | {"host_name": manifest.host_name, "manifest_digest": manifest.digest})
    task = collector.collect_tasks(manifest, previous=previous, cpu_reader=LinuxTaskCpuReader())
    assert isinstance(task, api.TaskOpsSample)
    assert task.evidence.cpu.groups[2].percent == "40.0"
    assert task.evidence.cpu.cutoff == task.snapshot.sampled_at == NOW + timedelta(seconds=3)
    assert capture_limits == [20.0]
    assert len(calls) == 38


def test_previous_observation_is_only_from_original_verified_owner(tmp_path: Path) -> None:
    module = importlib.import_module("rquant.ops_status_serving")
    assert hasattr(module, "read_previous_task_cpu"), "original-owner CPU predecessor lookup missing"
    task = _task_sample(); root = tmp_path / "authority"
    assert module.read_previous_task_cpu(root=root, producer_commit="b" * 40, as_of=NOW) is None
    publish_ops_status_snapshot(task, root=root, producer_commit="b" * 40, clock=lambda: NOW + timedelta(seconds=3))
    actual = module.read_previous_task_cpu(root=root, producer_commit="b" * 40, as_of=NOW + timedelta(seconds=3))
    assert actual == task.evidence.cpu.pair.current
    assert module.read_previous_task_cpu(root=root, producer_commit="b" * 40, as_of=NOW + timedelta(seconds=122)) is None
    pointer = root / "current.json"
    pointer.write_bytes(b'{"corrupt":true}')
    from rquant.runtime_serving_authority import ServingSourceAuthorityIntegrityError

    with pytest.raises(ServingSourceAuthorityIntegrityError):
        module.read_previous_task_cpu(root=root, producer_commit="b" * 40, as_of=NOW + timedelta(seconds=3))
    assert pointer.read_bytes() == b'{"corrupt":true}'


def test_unsigned_manifest_cannot_collect_new_tasks(tmp_path: Path) -> None:
    from rquant.ops_status import SignedOpsInstallManifest
    from tests.unit.test_ops_status import _manifest

    module = importlib.import_module("rquant.ops_status_serving")
    assert hasattr(module, "collect_and_publish_ops_tasks"), "real task producer entry missing"
    manifest = _manifest(); path = tmp_path / "unsigned.json"
    path.write_bytes(SignedOpsInstallManifest(manifest=manifest, signature="A" * 88).canonical_bytes())
    collector = OpsStatusCollector(command_runner=lambda *_: pytest.fail("signature failure must precede any command"), host_name=lambda: manifest.host_name)
    with pytest.raises(ValueError, match="signature"):
        module.collect_and_publish_ops_tasks(manifest_path=path, manifest_public_key_pem=b"invalid", authority_root=tmp_path / "authority", producer_commit="b" * 40, collector=collector)
    assert not (tmp_path / "authority").exists()


def test_tsc_12_original_lab_source_publishes_exact_scheduler_state(tmp_path: Path) -> None:
    from rquant.lab_jobs import LabJobReader
    from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
    from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
    from tests.unit.test_lab_scheduling_control import NOW as CONTROL_NOW, command, store_and_port

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=CONTROL_NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=CONTROL_NOW)
    port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=CONTROL_NOW + timedelta(seconds=1))
    state = port.reconcile(lease=lease, now=CONTROL_NOW + timedelta(seconds=2))
    source = LabJobsServingSourceReader(reader=LabJobReader(store.path))(CONTROL_NOW + timedelta(seconds=3))
    assert source.payload.scheduling_control == state
    table = next(p for p in source.payload.projections if p.table_name == "lab_scheduler_control")
    assert table.rows[0]["state_json"] == state.model_dump_json()
    assert PAGE_PROJECTION_CONTRACTS[table.table_name].owner_dataset_id == "lab_jobs"


def test_tsc_12_scheduler_projection_swap_and_future_refused(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.lab_jobs import LabJobReader
    from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
    from rquant.runtime_serving_snapshot import LabJobsPayload
    from tests.unit.test_lab_scheduling_control import NOW as CONTROL_NOW, store_and_port

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=CONTROL_NOW)
    state = store.enable_scheduling_control(lease=lease, barrier_port=port, now=CONTROL_NOW)
    reader = LabJobReader(store.path)
    result = LabJobsServingSourceReader(reader=reader)(CONTROL_NOW + timedelta(seconds=1))
    projections = list(result.payload.projections)
    index = next(i for i, p in enumerate(projections) if p.table_name == "lab_scheduler_control")
    row = dict(projections[index].rows[0]) | {"material_hash": "a" * 64}
    projections[index] = ServingProjectionPayload(table_name="lab_scheduler_control", available_at=CONTROL_NOW + timedelta(seconds=1), rows=(row,))
    with pytest.raises(ValueError, match="scheduler|scheduling|match"):
        LabJobsPayload.model_validate(result.payload.model_dump() | {"projections": projections})
    future = type(state).model_validate(state.model_dump() | {"observed_at": CONTROL_NOW + timedelta(seconds=5)})
    monkeypatch.setattr(reader, "scheduling_state", lambda: future)
    with pytest.raises(ValueError, match="future|cutoff"):
        LabJobsServingSourceReader(reader=reader)(CONTROL_NOW + timedelta(seconds=1))


def test_tsc_12_scheduler_double_read_rejects_changed_desired_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.lab_jobs import LabJobReader
    from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
    from tests.unit.test_lab_scheduling_control import NOW as CONTROL_NOW, command, store_and_port

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler", lease_seconds=120, now=CONTROL_NOW)
    first = store.enable_scheduling_control(lease=lease, barrier_port=port, now=CONTROL_NOW)
    port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=CONTROL_NOW + timedelta(seconds=1))
    second = port.reconcile(lease=lease, now=CONTROL_NOW + timedelta(seconds=2))
    reader = LabJobReader(store.path)
    states = iter((first, second))
    monkeypatch.setattr(reader, "scheduling_state", lambda: next(states))
    with pytest.raises(RuntimeError, match="scheduling|scheduler|changed"):
        LabJobsServingSourceReader(reader=reader)(CONTROL_NOW + timedelta(seconds=3))


def test_tsc_12_run_rows_bind_full_invocation_witness_and_same_ops_identity() -> None:
    from rquant.task_unit_control import TaskSystemdRunWindow, bind_systemd_unit_run
    from tests.unit.test_task_unit_control import systemd_window

    task = _task_sample()
    window = systemd_window(host_name=task.snapshot.host_name, manifest_digest=task.snapshot.manifest_digest)
    window["invocation"] = dict(window["invocation"]) | {"host_name": task.snapshot.host_name}
    run = bind_systemd_unit_run(TaskSystemdRunWindow.model_validate(window))
    sample = _api().TaskOpsSample(snapshot=_sample(at=run.observed_at), evidence=_api().TaskOpsEvidence(cpu=None, cpu_unavailable_reason="capture_unavailable", runs=(run,)))
    result = ops_status_source_result(sample)
    projection = next(p for p in result.payload.projections if p.table_name == "ops_task_runs")
    assert projection.rows[0]["evidence_json"] == run.model_dump_json()
    assert projection.rows[0]["result"] == "success"
    values = result.payload.model_dump()
    values["projections"] = tuple(ServingProjectionPayload(table_name=p.table_name, available_at=p.available_at, rows=(dict(p.rows[0]) | {"request_id": "different"},)) if p.table_name == "ops_task_runs" else p for p in result.payload.projections)
    with pytest.raises(ValueError, match="match"):
        OpsStatusPayload.model_validate(values)
    changed = type(run).model_validate(run.model_dump() | {"manifest_digest": "c" * 64})
    with pytest.raises(ValueError, match="identity"):
        _api().TaskOpsSample(snapshot=sample.snapshot, evidence=_api().TaskOpsEvidence(cpu=None, cpu_unavailable_reason="capture_unavailable", runs=(changed,)))


def _publish_task_center(root: Path, monkeypatch: pytest.MonkeyPatch, *, task: TaskOpsSample | None = None) -> str:
    from rquant.ops_status_serving import ops_status_projections
    from rquant.serving_read_models import ServingProjectionInput
    from tests.support import web_serving_fixture as fixture

    task = _task_sample() if task is None else task
    original = fixture._projections
    monkeypatch.setattr(fixture, "_DATASETS", (*fixture._DATASETS, "ops_status"))
    monkeypatch.setattr(fixture, "fixture_built_at", lambda _: task.snapshot.sampled_at + timedelta(seconds=40))

    def projections(*args: object, **kwargs: object):
        return original(*args, **kwargs) + tuple(ServingProjectionInput.bind(p, owner_dataset_id="ops_status", owner_generation_id=kwargs["generations"]["ops_status"]) for p in ops_status_projections(task))

    monkeypatch.setattr(fixture, "_projections", projections)
    return fixture.build_web_fixture(root, "baseline").generation_id


def test_tsc_12_private_source_reads_one_real_generation_and_rejects_stale(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.task_control_admission import TaskCenterServingSource

    generation = _publish_task_center(tmp_path, monkeypatch)
    source = TaskCenterServingSource(tmp_path, clock=lambda: NOW + timedelta(seconds=42))
    view = source.read(generation_id=generation)
    assert view.generation_id == generation and view.snapshot == _task_sample().snapshot
    assert view.cpu[2].percent == "40.0" and view.cpu[-1].percent is None
    assert view.scheduling_control is None
    with pytest.raises(ValueError, match="generation"):
        source.read(generation_id="wrong")
    old = TaskCenterServingSource(tmp_path, clock=lambda: NOW + timedelta(seconds=122))
    with pytest.raises(ValueError, match="stale"):
        old.read(generation_id=generation)


def test_tsc_04_overview_consumes_published_cpu_without_recomputing_capacity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.serving_publisher import ServingReader
    from rquant.web.serving import BorrowedGeneration
    from rquant.web.task_overview import ops_sections

    _publish_task_center(tmp_path, monkeypatch)
    with ServingReader(tmp_path).acquire_generation() as lease:
        cursor = lease.connection.cursor()
        try:
            borrowed = BorrowedGeneration(manifest=lease.manifest, pointer=lease.pointer, cursor=cursor, fallback_detail=None)
            scheduled, resources = ops_sections(borrowed, now=NOW + timedelta(seconds=42), day=None)
        finally:
            cursor.close()
    assert scheduled.source_state == "ready"
    assert resources.cpu_usage_percent == 5.0
    assert resources.groups[1].cpu_usage_percent == 40.0
    assert resources.groups[-1].cpu_usage_percent is None


@pytest.mark.parametrize("current", ["a" * 32, "c" * 32])
def test_tsc_07_overview_only_same_current_invocation_can_be_this_run_result(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, current: str) -> None:
    from rquant.serving_publisher import ServingReader
    from rquant.task_unit_control import TaskSystemdRunWindow, bind_systemd_unit_run
    from rquant.web.serving import BorrowedGeneration
    from rquant.web.task_overview import ops_sections
    from tests.unit.test_task_unit_control import systemd_window

    raw = systemd_window(host_name="synthetic-test", manifest_digest="a" * 64)
    raw["invocation"] = dict(raw["invocation"]) | {"host_name": "synthetic-test"}
    run = bind_systemd_unit_run(TaskSystemdRunWindow.model_validate(raw))
    snapshot = _sample(at=run.observed_at)
    units = tuple(type(item).model_validate(item.model_dump() | {"service_invocation_id": current}) if item.service == run.unit else item for item in snapshot.units)
    snapshot = type(snapshot).model_validate(snapshot.model_dump() | {"units": units})
    task = _api().TaskOpsSample(snapshot=snapshot, evidence=_api().TaskOpsEvidence(cpu=None, cpu_unavailable_reason="capture_unavailable", runs=(run,)))
    _publish_task_center(tmp_path, monkeypatch, task=task)
    with ServingReader(tmp_path).acquire_generation() as lease:
        cursor = lease.connection.cursor()
        try:
            scheduled, _ = ops_sections(BorrowedGeneration(manifest=lease.manifest, pointer=lease.pointer, cursor=cursor, fallback_detail=None), now=run.observed_at + timedelta(seconds=42), day=None)
        finally:
            cursor.close()
    item = next(row for row in scheduled.items if row.service_unit == run.unit)
    if current == run.invocation_id:
        assert item.result_label == "成功" and item.duration_seconds == 2.0
        assert item.started_at == run.started_at and item.invocation_id == current and item.origin_label == "手动运行"
    else:
        assert item.result_label == "归属待确认" and item.duration_seconds is None
        assert item.previous_result_label == "上次手动运行成功（历史）" and item.started_at is None


def test_tsc_12_original_ops_tables_must_share_their_full_owner_generation_and_cutoff(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import duckdb
    from rquant.serving_publisher import ServingReader
    from rquant.task_control_admission import read_task_center_view
    from rquant.web.serving import BorrowedGeneration

    _publish_task_center(tmp_path, monkeypatch)
    with ServingReader(tmp_path).acquire_generation() as lease:
        memory = duckdb.connect(":memory:")
        try:
            # Copy actual published tables into a private in-memory corrupted snapshot.
            for name in ("projection_status", "ops_host_status", "ops_unit_status", "ops_resource_status", "ops_task_cpu", "ops_task_runs", "lab_scheduler_control"):
                frame = lease.connection.execute(f"SELECT * FROM {name}").fetchdf()
                memory.register("copied_rows", frame)
                memory.execute(f"CREATE TABLE {name} AS SELECT * FROM copied_rows")
                memory.unregister("copied_rows")
            for column, value in (("owner_dataset_id", "lab_jobs"), ("owner_generation_id", "b" * 64)):
                memory.execute(f"UPDATE projection_status SET {column}=? WHERE table_name='ops_unit_status'", (value,))
                with pytest.raises(ValueError, match="snapshot|owner|generation|incomplete"):
                    read_task_center_view(BorrowedGeneration(manifest=lease.manifest, pointer=lease.pointer, cursor=memory, fallback_detail=None))
                mark = next(w for w in lease.manifest.watermarks if w.dataset_id == "ops_status")
                memory.execute("UPDATE projection_status SET owner_dataset_id='ops_status',owner_generation_id=? WHERE table_name='ops_unit_status'", (mark.generation_id,))
        finally:
            memory.close()


def _scheduler_wire_source(
    tmp_path: Path, *, resume: bool = False
) -> tuple[SourceReadResult, LabSchedulingControlState]:
    from rquant.lab_jobs import LabJobReader
    from rquant.lab_jobs_serving_authority import LabJobsServingSourceReader
    from tests.unit.test_lab_scheduling_control import NOW as CONTROL_NOW, command, store_and_port

    store, port = store_and_port(tmp_path)
    lease = store.acquire_scheduler_lease(owner_id="scheduler-wire", lease_seconds=120, now=CONTROL_NOW)
    store.enable_scheduling_control(lease=lease, barrier_port=port, now=CONTROL_NOW)
    port.apply_command(command(store, paused=True, expected_version=0), lease=lease, now=CONTROL_NOW + timedelta(seconds=1))
    state = port.reconcile(lease=lease, now=CONTROL_NOW + timedelta(seconds=2))
    if resume:
        port.apply_command(command(store, paused=False, expected_version=1), lease=lease, now=CONTROL_NOW + timedelta(seconds=3))
        state = port.reconcile(lease=lease, now=CONTROL_NOW + timedelta(seconds=4))
    source = LabJobsServingSourceReader(reader=LabJobReader(store.path))(CONTROL_NOW + timedelta(seconds=5))
    return source, state


@pytest.mark.parametrize("resume", [False, True], ids=["paused", "resumed"])
def test_original_scheduler_state_roundtrips_through_frozen_lab_wire(tmp_path: Path, resume: bool) -> None:
    from rquant.runtime_serving_snapshot import SourceReadResult

    source, state = _scheduler_wire_source(tmp_path, resume=resume)
    body = json.loads(source.model_dump_json())
    assert set(body["payload"]) == {"payload_kind", "lab_jobs", "projections"}
    restored = SourceReadResult.model_validate_json(source.model_dump_json())
    assert restored == source
    assert restored.payload.scheduling_control == state
    assert state.request_id is not None
    assert state.applied_paused is not resume


def test_scheduler_state_is_read_from_the_complete_original_projection_only(tmp_path: Path) -> None:
    from rquant.runtime_serving_snapshot import LabJobsPayload

    source, state = _scheduler_wire_source(tmp_path)
    control = next(table for table in source.payload.projections if table.table_name == "lab_scheduler_control")
    payload = LabJobsPayload(projections=(control,))
    assert payload.scheduling_control == state
    restored = LabJobsPayload.model_validate_json(payload.model_dump_json())
    assert restored.scheduling_control == state


def test_legacy_lab_payload_keeps_its_three_fields_and_no_scheduler_state() -> None:
    from rquant.runtime_serving_snapshot import LabJobsPayload

    payload = LabJobsPayload()
    assert set(LabJobsPayload.model_fields) == {"payload_kind", "lab_jobs", "projections"}
    assert set(payload.model_dump(mode="json")) == {"payload_kind", "lab_jobs", "projections"}
    assert payload.scheduling_control is None
    assert LabJobsPayload.model_validate_json(payload.model_dump_json()) == payload


@pytest.mark.parametrize("damage", [
    "duplicate_table", "empty_rows", "extra_row", "bad_json", "noncanonical_json",
    "control_key", "material_hash", "observed_at", "future_state",
])
def test_lab_scheduler_wire_rejects_incomplete_or_changed_projection(tmp_path: Path, damage: str) -> None:
    from rquant.runtime_serving_snapshot import LabJobsPayload

    source, state = _scheduler_wire_source(tmp_path)
    control = next(table for table in source.payload.projections if table.table_name == "lab_scheduler_control")
    wire = control.model_dump(mode="json")
    row = wire["rows"][0]
    projections = [wire]
    if damage == "duplicate_table":
        projections.append(control.model_dump(mode="json"))
    elif damage == "empty_rows":
        wire["rows"] = []
    elif damage == "extra_row":
        wire["rows"].append(dict(row) | {"control_key": "other"})
    elif damage == "bad_json":
        row["state_json"] = "{"
    elif damage == "noncanonical_json":
        row["state_json"] = json.dumps(json.loads(row["state_json"]), indent=1)
    elif damage == "control_key":
        row["control_key"] = "other"
    elif damage == "material_hash":
        row["material_hash"] = "0" * 64
    elif damage == "observed_at":
        row["observed_at"] = (state.observed_at - timedelta(microseconds=1)).isoformat()
    elif damage == "future_state":
        future = type(state).model_validate(state.model_dump() | {"observed_at": control.available_at + timedelta(seconds=1)})
        row["state_json"] = future.model_dump_json()
        row["material_hash"] = future.state_hash
    with pytest.raises(ValueError):
        LabJobsPayload.model_validate({"projections": projections})


def test_complete_scheduler_wire_is_read_from_the_original_serving_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.serving_read_models import ServingProjectionInput
    from rquant.task_control_admission import TaskCenterServingSource
    from tests.support import web_serving_fixture as fixture

    original_root = tmp_path / "original-scheduler"
    original_root.mkdir(mode=0o700)
    source, state = _scheduler_wire_source(original_root)
    control = next(table for table in source.payload.projections if table.table_name == "lab_scheduler_control")
    original = fixture._projections

    def with_control(*args: object, **kwargs: object) -> tuple[ServingProjectionInput, ...]:
        return original(*args, **kwargs) + (
            ServingProjectionInput.bind(
                control,
                owner_dataset_id="lab_jobs",
                owner_generation_id=kwargs["generations"]["lab_jobs"],
            ),
        )

    monkeypatch.setattr(fixture, "_projections", with_control)
    root = tmp_path / "serving"
    root.mkdir(mode=0o700)
    generation = _publish_task_center(root, monkeypatch)
    view = TaskCenterServingSource(root, clock=lambda: NOW + timedelta(seconds=42)).read(generation_id=generation)
    assert view.generation_id == generation
    assert view.scheduling_control == state
