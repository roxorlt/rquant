"""Small physical fixtures; current deployment is stubbed, not an install proof."""

from __future__ import annotations

import importlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from rquant.collaboration_commands import PageControlRoleAuthority
from rquant.collaboration_roles import RoleEntry, RoleState
from rquant.experiment_registry import ExperimentRegistry
from rquant.job_center_authority import (
    JobCenterAuthorityDeploymentBinding,
    JobCenterAuthorityIntegrityError,
    JobCenterAuthorityManifest,
    _binding,
    _canonical_hash,
    _directory_identity,
    _file_identity,
)
from rquant.lab_job_protocol import InvalidCommandEnvelopeError, LabCommandSpool
from rquant.lab_jobs import LabJobStore
from rquant.minute_backtest_commands import ExportMinuteReplayZip, minute_zip_request_id
from rquant.minute_backtest_contracts import MinuteReplayModel
from rquant.minute_backtest_export import MinuteReportReader, MinuteZipExportFacade
from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService
from rquant.strict_json import canonical_json_bytes

if TYPE_CHECKING:
    from rquant.minute_backtest_native_report_runtime import (
        MinuteNativeReportCommandWriter,
        MinuteNativeReportRuntime,
    )

CODE = "a" * 40
NOW = datetime(2026, 10, 8, 0, 0, tzinfo=UTC)
JOB = UUID("41cbe9d6-2b2a-4af6-b34b-51825e6d6e8d")


def runtime_module() -> ModuleType:
    try:
        return importlib.import_module("rquant.minute_backtest_native_report_runtime")
    except ModuleNotFoundError as exc:
        if exc.name != "rquant.minute_backtest_native_report_runtime":
            raise
        pytest.fail("the native report runtime is not implemented")


def fixture_paths(root: Path) -> dict[str, Path]:
    deployment = root / "deployment"
    deployment.mkdir(mode=0o700)
    runtime = deployment / "runtime"
    runtime.mkdir(mode=0o700)
    paths = {
        "runtime_deployment_root": deployment,
        "runtime_root": runtime,
        "lab_jobs_path": runtime / "jobs.sqlite",
        "command_spool_path": runtime / "commands",
        "final_artifact_root": runtime / "artifacts",
        "definition_registry_root": runtime / "definitions",
        "experiment_registry_path": runtime / "experiments.sqlite",
        "dataset_authority_path": runtime / "dataset.json",
        "catalog_authority_root": runtime / "catalog",
        "catalog_authority_receipt_path": runtime / "catalog/current.json",
    }
    for name in ("final_artifact_root", "definition_registry_root", "catalog_authority_root"):
        paths[name].mkdir(mode=0o700)
    LabJobStore(paths["lab_jobs_path"]).initialize()
    paths["lab_jobs_path"].chmod(0o600)
    ExperimentRegistry(paths["experiment_registry_path"], managed_trust_root=runtime)
    LabCommandSpool(paths["command_spool_path"])
    for name in ("dataset_authority_path", "catalog_authority_receipt_path"):
        paths[name].write_text("{}")
        paths[name].chmod(0o600)
    return paths


def resolver_fixture(
    monkeypatch: pytest.MonkeyPatch, module: ModuleType, paths: dict[str, Path]
) -> tuple[JobCenterAuthorityDeploymentBinding, JobCenterAuthorityManifest]:
    binding = JobCenterAuthorityDeploymentBinding(
        code_sha=CODE,
        deployment_profile_id="b" * 64,
        deployment_generation_hash="c" * 64,
        runtime_mode="local-test",
        **paths,
    )
    authorities = (
        _binding(
            "artifact_catalog",
            (_directory_identity(paths["catalog_authority_root"], label="test"),),
        ),
        _binding(
            "definition_registry",
            (_directory_identity(paths["definition_registry_root"], label="test"),),
        ),
        _binding(
            "experiment_registry",
            (_file_identity(paths["experiment_registry_path"], label="test"),),
        ),
        _binding("job_center", (_file_identity(paths["lab_jobs_path"], label="test"),)),
    )
    body = binding.model_dump(mode="json", exclude={"runtime_mode", "lab_highwater"})
    body |= {"schema_version": 4, "authorities": [x.model_dump(mode="json") for x in authorities]}
    authority = JobCenterAuthorityManifest(**body, manifest_hash=_canonical_hash(body))

    def resolve(root: Path, **kwargs: object) -> JobCenterAuthorityDeploymentBinding:
        assert root == paths["runtime_deployment_root"]
        assert kwargs == {
            "expected_code_sha": CODE,
            **{
                name: paths[name]
                for name in (
                    "runtime_root",
                    "lab_jobs_path",
                    "command_spool_path",
                    "final_artifact_root",
                )
            },
        }
        return binding

    def load(path: Path, **kwargs: object) -> JobCenterAuthorityManifest:
        assert path == paths["runtime_root"] / "job-center-authority.json"
        assert kwargs == {
            "expected_code_sha": CODE,
            "expected_research_root": paths["runtime_root"],
            "expected_lab_jobs_path": paths["lab_jobs_path"],
            "expected_command_spool_path": paths["command_spool_path"],
            "expected_final_artifact_root": paths["final_artifact_root"],
            "expected_runtime_deployment_root": paths["runtime_deployment_root"],
            "expected_deployment_profile_id": binding.deployment_profile_id,
            "expected_deployment_generation_hash": binding.deployment_generation_hash,
        }
        return authority

    monkeypatch.setattr(module, "resolve_current_job_center_authority_binding", resolve)
    monkeypatch.setattr(module, "load_lab_job_center_authority_manifest", load)
    return binding, authority


def locator_file(root: Path, paths: dict[str, Path], **changes: object) -> Path:
    fields = (
        "runtime_deployment_root",
        "runtime_root",
        "lab_jobs_path",
        "command_spool_path",
        "final_artifact_root",
    )
    body: dict[str, object] = {
        "contract": "minute-native-report-runtime/v1",
        "code_sha": CODE,
        **{name: str(paths[name]) for name in fields},
        **changes,
    }
    path = root / "native-report.json"
    path.write_text(json.dumps(body))
    path.chmod(0o600)
    return path


def load_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, writable: bool = False
) -> object:
    module = runtime_module()
    paths = fixture_paths(tmp_path)
    resolver_fixture(monkeypatch, module, paths)
    path = locator_file(tmp_path, paths)
    return module.load_minute_native_report_runtime(
        path, expected_code_sha=CODE, clock=lambda: NOW, writable=writable
    )


def test_missing_locator_does_not_create_anything(tmp_path: Path) -> None:
    module = runtime_module()
    before = tuple(tmp_path.iterdir())
    with pytest.raises(FileNotFoundError):
        module.load_minute_native_report_runtime(
            tmp_path / "missing.json", expected_code_sha=CODE, clock=lambda: NOW
        )
    assert tuple(tmp_path.iterdir()) == before


@pytest.mark.parametrize(
    "change",
    [
        {"code_sha": "d" * 40},
        {"runtime_root": "relative"},
        {"runtime_root": "/private/tmp/a/../b"},
        {"trusted": True},
    ],
)
def test_locator_cannot_claim_identity_or_another_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: dict[str, object]
) -> None:
    module = runtime_module()
    paths = fixture_paths(tmp_path)
    resolver_fixture(monkeypatch, module, paths)
    path = locator_file(tmp_path, paths, **change)
    with pytest.raises((ValueError, PermissionError)):
        module.load_minute_native_report_runtime(path, expected_code_sha=CODE, clock=lambda: NOW)


@pytest.mark.parametrize("change", ["mode", "symlink", "hardlink", "oversized", "duplicate"])
def test_locator_requires_the_original_private_bounded_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    module = runtime_module()
    paths = fixture_paths(tmp_path)
    resolver_fixture(monkeypatch, module, paths)
    path = locator_file(tmp_path, paths)
    if change == "mode":
        path.chmod(0o644)
    elif change == "symlink":
        original = path.with_suffix(".original")
        path.rename(original)
        path.symlink_to(original)
    elif change == "hardlink":
        os.link(path, path.with_suffix(".copy"))
    elif change == "oversized":
        path.write_bytes(b" " * (1_048_576 + 1))
    else:
        path.write_text(
            path.read_text().replace('"code_sha":', '"code_sha":"' + CODE + '","code_sha":')
        )
    with pytest.raises((OSError, ValueError, PermissionError)):
        module.load_minute_native_report_runtime(path, expected_code_sha=CODE, clock=lambda: NOW)


@pytest.mark.parametrize("change", ["locator", "inode", "current", "manifest"])
def test_every_read_rechecks_locator_current_and_complete_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    runtime = load_runtime(tmp_path, monkeypatch)
    module = runtime_module()
    if change == "locator":
        runtime.reference.path.write_text(runtime.reference.path.read_text() + " ")
    elif change == "inode":
        replacement = runtime.reference.path.with_suffix(".replacement")
        replacement.write_bytes(runtime.reference.path.read_bytes())
        replacement.chmod(0o600)
        replacement.replace(runtime.reference.path)
    elif change == "current":

        def stale(*args: object, **kwargs: object) -> object:
            raise JobCenterAuthorityIntegrityError("current generation changed")

        monkeypatch.setattr(module, "resolve_current_job_center_authority_binding", stale)
    else:
        original = runtime.authority
        body = original.model_dump(mode="json", exclude={"manifest_hash"})
        body["deployment_generation_hash"] = "e" * 64
        changed = JobCenterAuthorityManifest(**body, manifest_hash=_canonical_hash(body))
        monkeypatch.setattr(
            module, "load_lab_job_center_authority_manifest", lambda *a, **k: changed
        )
    with pytest.raises((PermissionError, ValueError, RuntimeError)):
        runtime.verify_current()


def test_get_never_constructs_store_or_creates_export_directories(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = runtime_module()
    paths = fixture_paths(tmp_path)
    resolver_fixture(monkeypatch, module, paths)
    path = locator_file(tmp_path, paths)

    def forbidden(*args: object, **kwargs: object) -> object:
        pytest.fail("GET attempted a writer or mkdir")

    monkeypatch.setattr(module, "LabJobArtifactStore", forbidden)
    monkeypatch.setattr(Path, "mkdir", forbidden)
    monkeypatch.setattr(os, "mkdir", forbidden)
    runtime = module.load_minute_native_report_runtime(
        path, expected_code_sha=CODE, clock=lambda: NOW
    )
    assert runtime.artifact_store is None and not runtime.export_root.exists()
    with pytest.raises(LookupError):
        runtime.replay_reader(JOB)
    runtime.close()


def test_original_lab_preflight_precedes_fresh_readonly_registry_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = load_runtime(tmp_path, monkeypatch)
    module = runtime_module()
    observed: list[str] = []
    original_registry = module.ExperimentRegistryReadonlyReader

    def preflight(job: UUID) -> object:
        assert job == JOB
        observed.append("lab-preflight")
        return object()

    def capture(*args: object, **kwargs: object) -> object:
        assert observed == ["lab-preflight"]
        observed.append("registry-capture")
        return original_registry(*args, **kwargs)

    monkeypatch.setattr(runtime.reader, "get_artifact_preview_authority", preflight)
    monkeypatch.setattr(module, "ExperimentRegistryReadonlyReader", capture)
    port = runtime.replay_reader(JOB)
    assert port.reader is runtime.reader
    assert port.artifact_reader.reader is runtime.reader
    assert port.submission_facade.reader is runtime.reader
    assert port.private_authority.registry is port.submission_facade.experiment_registry
    assert observed == ["lab-preflight", "registry-capture"]
    with pytest.raises(LookupError):
        port.job(JOB, owner_id="owner")


def test_get_cannot_repair_a_missing_original_spool_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = load_runtime(tmp_path, monkeypatch)
    (runtime.authority.command_spool_path / "pending").rmdir()
    monkeypatch.setattr(runtime.reader, "get_artifact_preview_authority", lambda _: object())
    with pytest.raises(InvalidCommandEnvelopeError) as failure:
        runtime.replay_reader(JOB)
    assert isinstance(failure.value.__cause__, PermissionError)
    assert not (runtime.authority.command_spool_path / "pending").exists()


def test_writer_store_is_owned_and_closed_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = load_runtime(tmp_path, monkeypatch, writable=True)
    assert runtime.artifact_store is not None
    assert runtime.export_root.is_dir()
    assert (runtime.export_root.parent / "minute-base").is_dir()
    store = runtime.artifact_store
    calls: list[bool] = []
    original = store.close

    def close() -> None:
        calls.append(True)
        original()

    monkeypatch.setattr(store, "close", close)
    runtime.close()
    runtime.close()
    assert calls == [True]
    with pytest.raises(PermissionError):
        runtime.verify_current()


class _Receipt(MinuteReplayModel):
    """ZIP transport stand-in: these tests do not manufacture a sealed result."""

    request_id: UUID
    job_id: UUID


@dataclass
class _WriterFixture:
    runtime: MinuteNativeReportRuntime
    writer: MinuteNativeReportCommandWriter
    service: PageControlService
    report: SimpleNamespace
    reads: list[tuple[UUID, str, str]]
    exports: list[tuple[str, UUID, str, UUID, str]]
    ports: list[UUID]


def export_command(*, job_id: UUID = JOB) -> ExportMinuteReplayZip:
    return ExportMinuteReplayZip(
        command_id=str(uuid4()),
        requested_at=NOW,
        actor_id="alice",
        job_id=job_id,
        result_hash="a" * 64,
    )


def role_state(path: Path, *, revision: int = 1, role: str = "researcher") -> None:
    state = RoleState.create(
        revision=revision,
        users=(
            RoleEntry(username="admin", role="admin"),
            RoleEntry(username="alice", role=role),
        ),
    )
    path.write_bytes(canonical_json_bytes(state.model_dump(mode="json")))
    path.chmod(0o600)


def writer_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _WriterFixture:
    runtime = load_runtime(tmp_path, monkeypatch, writable=True)
    module = runtime_module()
    writer = module.MinuteNativeReportCommandWriter(runtime)
    control = tmp_path / "control"
    control.mkdir(mode=0o700)
    outbox = PageControlOutbox(control / "commands.sqlite")
    outbox.path.chmod(0o600)
    roles = control / "roles.json"
    role_state(roles)
    consumer = PageControlConsumer(
        outbox=outbox,
        data_dir=control,
        log_dir=control,
        minute_native_report_backend=writer,
        consumer_id="native-report-test",
        clock=lambda: NOW,
    )
    service = PageControlService(
        outbox=outbox,
        consumer=consumer,
        collaboration=PageControlRoleAuthority(
            mode="enforced", roles_path=roles, clock=lambda: NOW
        ),
    )
    service._defer_minute_commands = True
    report = SimpleNamespace(
        sealed=SimpleNamespace(
            full_input_hash="b" * 64, core_input_hash="c" * 64, seed_hash="d" * 64
        ),
        report=SimpleNamespace(html_sha256="e" * 64),
        owner=SimpleNamespace(content_sha256="f" * 64),
    )
    reads: list[tuple[UUID, str, str]] = []
    exports: list[tuple[str, UUID, str, UUID, str]] = []
    ports: list[UUID] = []
    original = runtime.replay_reader

    def port(job_id: UUID) -> object:
        ports.append(job_id)
        return original(job_id)

    def read(
        self: MinuteReportReader, job_id: UUID, *, owner_id: str, expected_result_hash: str
    ) -> object:
        assert self.reader is runtime.reader and self.native_reader.reader is runtime.reader
        assert self.owner_authority is service and self.installation is None
        # A real readonly Registry read also checks the assembly's physical root lifetime.
        assert (
            self.native_reader.private_authority.registry.get_submission_intent_for_job(job_id)
            is None
        )
        reads.append((job_id, owner_id, expected_result_hash))
        return report

    def export(
        self: MinuteZipExportFacade,
        job_id: UUID,
        *,
        owner_id: str,
        request_id: UUID,
        expected_result_hash: str,
    ) -> _Receipt:
        assert self.reader is runtime.reader and self.original_exports.reader is runtime.reader
        assert self.artifact_store is runtime.artifact_store
        assert self.original_exports.artifact_store is runtime.artifact_store
        exports.append(("submit", job_id, owner_id, request_id, expected_result_hash))
        return _Receipt(request_id=request_id, job_id=job_id)

    def recover(
        self: MinuteZipExportFacade,
        job_id: UUID,
        *,
        owner_id: str,
        request_id: UUID,
        expected_result_hash: str,
    ) -> None:
        exports.append(("recover", job_id, owner_id, request_id, expected_result_hash))
        return None

    monkeypatch.setattr(runtime.reader, "get_artifact_preview_authority", lambda _: object())
    monkeypatch.setattr(runtime, "replay_reader", port)
    monkeypatch.setattr(MinuteReportReader, "read", read)
    monkeypatch.setattr(MinuteZipExportFacade, "export_minute", export)
    monkeypatch.setattr(MinuteZipExportFacade, "recover_minute", recover)
    return _WriterFixture(runtime, writer, service, report, reads, exports, ports)


def activate(case: _WriterFixture, command: ExportMinuteReplayZip, *, begin: bool = True) -> None:
    proof = case.service.collaboration.issue_authorization("alice", command.model_dump(mode="json"))
    receipt = case.service.submit_authorized(command, proof)
    assert receipt.status.value == "pending"
    claims = case.service.outbox.claim_records(
        limit=1,
        owner_id=case.service.consumer.consumer_id,
        now=NOW,
        target_command_id=command.command_id,
    )
    assert len(claims) == 1 and claims[0].command == command
    if begin:
        effect, created = case.service.outbox.begin_effect(
            command,
            owner_id=claims[0].owner_id,
            claim_token=claims[0].claim_token,
            now=NOW,
        )
        assert created and effect.status.value == "started"


def test_same_service_binds_writer_and_original_marker_recover_does_not_submit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = writer_fixture(tmp_path, monkeypatch)
    command = export_command()
    activate(case, command)
    assert case.writer.owner_authority is case.service
    assert case.service.consumer.minute_native_report_backend is case.writer
    marker = case.writer.freeze(command)
    assert marker["job_id"] == str(JOB) and marker["result_hash"] == command.result_hash
    assert marker["owner_binding_hash"] == case.report.owner.content_sha256
    assert case.writer.recover(command, marker) is None
    assert case.exports == [
        ("recover", JOB, "alice", minute_zip_request_id(command), command.result_hash)
    ]
    result = case.writer.submit(command, marker)
    assert result == {"request_id": str(minute_zip_request_id(command)), "job_id": str(JOB)}
    assert case.exports[-1] == (
        "submit",
        JOB,
        "alice",
        minute_zip_request_id(command),
        command.result_hash,
    )
    assert case.reads == [(JOB, "alice", command.result_hash)] * 3
    case.writer.close()


@pytest.mark.parametrize(
    "phase", ["no_actor", "no_effect", "changed_body", "revoked_role", "taken_claim"]
)
def test_original_actor_body_role_and_current_claim_refuse_before_report_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    case = writer_fixture(tmp_path, monkeypatch)
    command = export_command()
    if phase != "no_actor":
        activate(case, command, begin=phase != "no_effect")
    if phase == "changed_body":
        command = command.model_copy(update={"result_hash": "1" * 64})
    elif phase == "revoked_role":
        role_state(case.service.collaboration.roles_path, revision=2, role="viewer")
    elif phase == "taken_claim":
        claims = case.service.outbox.claim_records(
            limit=1,
            owner_id="replacement-consumer",
            now=NOW + timedelta(hours=1),
            target_command_id=command.command_id,
        )
        assert len(claims) == 1
    with pytest.raises(PermissionError):
        case.writer.freeze(command)
    assert not case.reads and not case.exports
    case.writer.close()


def test_writer_only_accepts_original_export_and_cannot_rebind_another_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = writer_fixture(tmp_path, monkeypatch)
    with pytest.raises(TypeError):
        case.writer.freeze(SimpleNamespace(kind="export_minute_replay_zip"))
    with pytest.raises(TypeError):
        case.writer.bind_owner_authority(SimpleNamespace(consumer=case.service.consumer))
    case.service.consumer.minute_native_report_backend = None
    with pytest.raises(TypeError):
        case.writer.bind_owner_authority(case.service)
    with pytest.raises(PermissionError):
        case.writer.freeze(export_command())
    case.service.consumer.minute_native_report_backend = case.writer
    with pytest.raises(PermissionError):
        PageControlService(
            outbox=case.service.outbox,
            consumer=case.service.consumer,
            collaboration=case.service.collaboration,
        )
    assert not case.reads and not case.exports
    case.writer.close()


@pytest.mark.parametrize("field", ["job_id", "result_hash", "command_hash"])
def test_changed_marker_is_rejected_before_fresh_report_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    case = writer_fixture(tmp_path, monkeypatch)
    command = export_command()
    activate(case, command)
    marker = case.writer.freeze(command)
    marker[field] = str(uuid4()) if field == "job_id" else "1" * 64
    before = tuple(case.reads)
    with pytest.raises(PermissionError):
        case.writer.submit(command, marker)
    assert tuple(case.reads) == before and not case.exports
    case.writer.close()


@pytest.mark.parametrize("changed", ["full_input_hash", "owner", "spec_refusal", "current_role"])
def test_fresh_report_or_authority_change_cannot_publish_or_recover(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    changed: str,
) -> None:
    case = writer_fixture(tmp_path, monkeypatch)
    command = export_command()
    activate(case, command)
    marker = case.writer.freeze(command)
    if changed == "full_input_hash":
        case.report.sealed.full_input_hash = "1" * 64
    elif changed == "owner":
        case.report.owner.content_sha256 = "1" * 64
    else:
        original = MinuteReportReader.read

        def refusal(self: MinuteReportReader, job_id: UUID, **kwargs: str) -> object:
            if changed == "spec_refusal":
                raise PermissionError("original complete job/spec differs")
            result = original(self, job_id, **kwargs)
            role_state(case.service.collaboration.roles_path, revision=2, role="viewer")
            return result

        monkeypatch.setattr(MinuteReportReader, "read", refusal)
    with pytest.raises(PermissionError):
        case.writer.submit(command, marker)
    with pytest.raises(PermissionError):
        case.writer.recover(command, marker)
    assert not case.exports
    case.writer.close()


def test_different_jobs_get_their_own_original_port_not_the_first_jobs_port(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    case = writer_fixture(tmp_path, monkeypatch)
    jobs = (JOB, uuid4())
    for job_id in jobs:
        command = export_command(job_id=job_id)
        activate(case, command)
        marker = case.writer.freeze(command)
        case.writer.submit(command, marker)
    assert case.ports == [jobs[0], jobs[0], jobs[1], jobs[1]]
    assert [item[1] for item in case.exports] == list(jobs)
    case.writer.close()
