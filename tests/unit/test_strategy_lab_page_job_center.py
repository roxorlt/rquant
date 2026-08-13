from __future__ import annotations

import ast
import runpy
import stat
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from rquant.job_center_authority import JobCenterAuthorityManifest
from rquant.research_gate import (
    ResearchGateDecision,
    ResearchGateFailure,
    ResearchGateRequest,
)

ROOT = Path(__file__).parents[2]
ENTRYPOINT = ROOT / "src" / "rquant" / "dashboard" / "strategy_lab.py"
APP = ROOT / "src" / "rquant" / "dashboard" / "lab" / "app.py"


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imports(tree: ast.AST) -> set[str]:
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            imported.add(node.module or "")
    return imported


def _called_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name):
            names.add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            names.add(node.func.attr)
    return names


def _installed_job_center_authority(
    tmp_path: Path,
) -> tuple[JobCenterAuthorityManifest, dict[str, object]]:
    from rquant.artifact_retention import ArtifactReferenceStore
    from rquant.artifact_retention_catalog_authority import (
        bootstrap_retention_catalog_authority,
    )
    from rquant.experiment_registry import ExperimentRegistry
    from rquant.job_center_authority import (
        install_job_center_authority,
        load_job_center_authority,
        publish_job_center_authority_candidate,
    )
    from rquant.lab_jobs import LabJobStore
    from rquant.runtime_definition_bootstrap import (
        bootstrap_builtin_definitions,
        plan_builtin_definitions,
    )

    code_sha = "a" * 40
    research = tmp_path / "research"
    research.mkdir(mode=0o700)
    commands = research / "commands"
    commands.mkdir(mode=0o700)
    artifacts = research / "final-artifacts"
    artifacts.mkdir(mode=0o700)
    definitions = tmp_path / "definitions"
    plan = plan_builtin_definitions(producer_commit=code_sha)
    bootstrap_builtin_definitions(
        definitions,
        producer_commit=code_sha,
        registered_at=datetime(2026, 8, 1, tzinfo=UTC),
        available_at=datetime(2026, 8, 1, tzinfo=UTC),
        expected_plan_id=plan.plan_id,
    )
    jobs_path = research / "lab_jobs.sqlite3"
    LabJobStore(jobs_path).initialize()
    jobs_path.chmod(0o600)
    experiment_path = research / "experiment_registry.sqlite3"
    ExperimentRegistry(experiment_path, managed_trust_root=research)
    dataset_path = research / "research_ro.duckdb"
    dataset_path.touch(mode=0o600)
    retention_root = research / "artifact-retention"
    retention_root.mkdir(mode=0o700)
    ArtifactReferenceStore(
        retention_root / "references.sqlite3",
        managed_trust_root=retention_root,
    )
    retention_references = retention_root / "references.sqlite3"
    retention_references.chmod(0o600)
    catalog_authority = bootstrap_retention_catalog_authority(
        state_root=retention_root,
        reference_store_path=retention_references,
        producer_commit=code_sha,
    )
    staging = tmp_path / "staging"
    staging.mkdir(mode=0o700)
    paths = {
        "runtime_deployment_root": tmp_path,
        "runtime_root": research,
        "lab_jobs_path": jobs_path,
        "command_spool_path": commands,
        "final_artifact_root": artifacts,
        "definition_registry_root": definitions,
        "experiment_registry_path": experiment_path,
        "dataset_authority_path": dataset_path,
        "catalog_authority_root": catalog_authority.root,
        "catalog_authority_receipt_path": catalog_authority.current_receipt_path,
        "deployment_profile_id": "b" * 64,
        "deployment_generation_hash": "c" * 64,
    }
    candidate = publish_job_center_authority_candidate(
        staging / "candidate.json",
        code_sha=code_sha,
        **paths,
    )
    installed = install_job_center_authority(
        candidate,
        target=research / "job-center-authority.json",
        expected_code_sha=code_sha,
        expected_runtime_root=research,
        expected_runtime_deployment_root=tmp_path,
        expected_deployment_profile_id="b" * 64,
        expected_deployment_generation_hash="c" * 64,
    )
    return (
        load_job_center_authority(
            installed,
            expected_code_sha=code_sha,
            **paths,
        ),
        paths,
    )


def test_strategy_lab_entrypoint_only_delegates_to_the_job_center_app() -> None:
    tree = _tree(ENTRYPOINT)
    assert _imports(tree) == {"__future__", "rquant.dashboard.lab.app"}
    assert _called_names(tree) == {"run_strategy_lab_app"}


def test_strategy_lab_entrypoint_smoke_delegates_when_run_as_a_script(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tree = _tree(ENTRYPOINT)
    assert _imports(tree) == {"__future__", "rquant.dashboard.lab.app"}

    calls: list[None] = []
    lab_package = ModuleType("rquant.dashboard.lab")
    lab_package.__path__ = []  # type: ignore[attr-defined]
    app_module = ModuleType("rquant.dashboard.lab.app")

    def run_strategy_lab_app() -> None:
        calls.append(None)

    app_module.run_strategy_lab_app = run_strategy_lab_app
    monkeypatch.setitem(sys.modules, "rquant.dashboard.lab", lab_package)
    monkeypatch.setitem(sys.modules, "rquant.dashboard.lab.app", app_module)

    runpy.run_path(str(ENTRYPOINT), run_name="__main__")

    assert calls == [None]


def test_strategy_lab_page_has_no_legacy_execution_dependencies() -> None:
    tree = _tree(APP)
    imported = _imports(tree)
    calls = _called_names(tree)
    forbidden_modules = {
        "subprocess",
        "concurrent",
        "concurrent.futures",
        "rquant.dashboard.strategy_lab_worker",
        "rquant.strategy_compare",
        "rquant.strategy_optimizer",
        "rquant.auction_gap_strategy",
        "rquant.growth_board_surge_strategy",
        "rquant.minute_replay",
        "rquant.volume_profile",
    }
    forbidden_calls = {
        "Popen",
        "ThreadPoolExecutor",
        "launch_background_run",
        "cancel_background_run",
        "list_run_statuses",
        "run_strategy_comparison",
        "optimize_strategy_combinations",
        "run_auction_gap_replay",
        "run_growth_board_surge_replay",
        "calculate_volume_profile",
        "_run_with_countdown",
    }
    assert not (imported & forbidden_modules)
    assert not (calls & forbidden_calls)
    assert "tabs" not in calls


def test_strategy_lab_page_uses_all_typed_job_inputs_and_read_only_legacy_history() -> None:
    source = APP.read_text(encoding="utf-8")
    for symbol in (
        "StrategyLabJobCenterController",
        "NShapeComparisonRunInput",
        "NShapeOptimizationRunInput",
        "AuctionGapRunInput",
        "GrowthBoardSurgeRunInput",
        "lab_legacy_history",
        "estimate_submission",
    ):
        assert symbol in source
    assert "list_strategy_lab_runs" not in source
    assert "strategy_lab_runs" not in source
    for forbidden in (
        "build_strategy_lab_run",
        "save_strategy_lab_run",
        'session_state["compare_result"]',
        'session_state["optimize_result"]',
        'session_state["auction_result"]',
        'session_state["growth_result"]',
    ):
        assert forbidden not in source
    assert "SubmitLabCommand" in source
    assert "ExportLabArtifactZip" in source
    assert "DiscardLabArtifactZip" in source
    assert source.count('"更新时长预估"') == 1
    assert "Job Center 当前不可用" in source


def test_strategy_lab_page_never_constructs_job_or_artifact_writers() -> None:
    source = APP.read_text(encoding="utf-8")
    tree = _tree(APP)
    forbidden_symbols = {
        "ExperimentRegistry",
        "LabJobArtifactStore",
        "LabJobZipExportFacade",
        "LabCommandSubmissionFacade",
        "LabCommandSpool",
    }

    assert not (forbidden_symbols & _called_names(tree))
    for call in (
        "controller.submit(",
        "controller.pause(",
        "controller.resume(",
        "controller.cancel(",
        "controller.retry(",
        "controller.rerun(",
        "controller.export_zip(",
        "controller.discard_zip(",
    ):
        assert call not in source


def test_strategy_lab_page_keeps_mutable_job_queries_uncached() -> None:
    tree = _tree(APP)
    cached_functions: set[str] = set()
    for node in tree.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            target = decorator.func if isinstance(decorator, ast.Call) else decorator
            if (
                isinstance(target, ast.Attribute)
                and isinstance(target.value, ast.Name)
                and target.value.id == "st"
                and target.attr in {"cache_data", "cache_resource"}
            ):
                cached_functions.add(node.name)
    assert (
        not {
            "_list_jobs",
            "_get_job_detail",
            "_preview_job_artifact",
            "_export_job_zip",
        }
        & cached_functions
    )


def test_lab_ui_runtime_root_is_initialized_by_control_service(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.dashboard.lab import app
    from rquant.page_control import (
        InitializeLabExports,
        PageControlConsumer,
        PageControlOutbox,
        PageControlService,
    )

    runtime_root = tmp_path / "lab-runtime"
    runtime_root.mkdir(mode=0o755)
    outbox = PageControlOutbox(tmp_path / "control" / "outbox.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            allowed_lab_export_roots=(runtime_root,),
        ),
    )
    submitted: list[InitializeLabExports] = []

    def submit(command: InitializeLabExports):
        submitted.append(command)
        return service.submit(command)

    monkeypatch.setattr(app._page_control, "submit", submit)

    app._ensure_private_runtime_directory(runtime_root)

    assert len(submitted) == 1
    assert submitted[0].export_root == runtime_root
    assert stat.S_IMODE(runtime_root.lstat().st_mode) == 0o700


def test_lab_ui_runtime_root_rejects_a_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.dashboard.lab import app
    from rquant.page_control import PageControlReceipt, PageControlStatus

    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    alias = tmp_path / "lab-runtime"
    alias.symlink_to(target, target_is_directory=True)

    def accept(command: object) -> PageControlReceipt:
        return PageControlReceipt(
            command_id=command.command_id,
            status=PageControlStatus.SUCCEEDED,
            enqueued_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
        )

    monkeypatch.setattr(app._page_control, "submit", accept)

    with pytest.raises(RuntimeError, match="owned physical directory"):
        app._ensure_private_runtime_directory(alias)


def test_job_center_runtime_injects_all_formal_authorities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.dashboard.lab import app
    from rquant.dashboard.lab.job_center import RegistryBackedFormalExperimentResolver
    from rquant.page_control import (
        InitializeLabExports,
        PageControlConsumer,
        PageControlOutbox,
        PageControlService,
    )

    manifest, paths = _installed_job_center_authority(tmp_path)
    export_root = manifest.research_root / "exports"
    outbox = PageControlOutbox(tmp_path / "control" / "outbox.sqlite3")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            allowed_lab_export_roots=(export_root,),
        ),
    )

    def submit(command: InitializeLabExports):
        return service.submit(command)

    monkeypatch.setattr(app._page_control, "submit", submit)

    runtime = app._build_job_center_runtime(manifest)

    assert runtime.experiment_registry.path == paths["experiment_registry_path"]
    assert runtime.definition_registry.root == paths["definition_registry_root"]
    assert isinstance(
        runtime.controller._formal_experiment_resolver,
        RegistryBackedFormalExperimentResolver,
    )
    assert runtime.controller._commands is None
    assert runtime.controller._zip_exports is None
    assert not hasattr(runtime.experiment_registry, "register_formal_plan")


def test_job_center_runtime_fails_closed_when_authority_is_missing(tmp_path: Path) -> None:
    from rquant.dashboard.lab.app import (
        JobCenterRuntimeUnavailableError,
        _build_job_center_runtime,
    )

    manifest, paths = _installed_job_center_authority(tmp_path)
    paths["dataset_authority_path"].unlink()

    with pytest.raises(JobCenterRuntimeUnavailableError, match="authority|权威"):
        _build_job_center_runtime(manifest)


def test_cached_job_center_runtime_translates_manifest_failure_for_the_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.dashboard.lab import app
    from rquant.lab_daemon import LabDaemonConfigurationError

    monkeypatch.setattr(app, "_ensure_private_runtime_directory", lambda _path: None)
    monkeypatch.setattr(app, "_verified_code_sha", lambda: "a" * 40)
    monkeypatch.setenv("RQUANT_RUNTIME_ROOT", "/tmp/rquant-production-runtime")
    monkeypatch.setattr(
        app,
        "resolve_current_job_center_authority_binding",
        lambda *_args, **_kwargs: SimpleNamespace(
            runtime_deployment_root=Path("/tmp/rquant-production-runtime"),
            runtime_root=app.settings.lab_runtime_dir_resolved,
            lab_jobs_path=app.settings.lab_jobs_path_resolved,
            command_spool_path=app.settings.lab_job_command_dir_resolved,
            final_artifact_root=app.settings.lab_final_artifact_dir_resolved,
            deployment_profile_id="b" * 64,
            deployment_generation_hash="c" * 64,
        ),
    )
    monkeypatch.setattr(app, "_require_private_runtime_directory", lambda _path: None)

    def unavailable(*_args: object, **_kwargs: object) -> object:
        raise LabDaemonConfigurationError("manifest missing")

    monkeypatch.setattr(app, "load_lab_job_center_authority_manifest", unavailable)

    with pytest.raises(app.JobCenterRuntimeUnavailableError, match="authority|manifest"):
        app._job_center_runtime.__wrapped__()


def test_formal_submission_defers_artifact_verification_to_the_worker() -> None:
    from rquant.dashboard.lab import app

    request = ResearchGateRequest(
        mode="formal",
        strategy_name="n_shape",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 2),
        code_commit="1" * 40,
    )
    preliminary = ResearchGateDecision(
        allowed=False,
        research_status="exploratory",
        audit_run_id="2" * 64,
        dataset_snapshot_id="3" * 64,
        dataset_binding_hash="4" * 64,
        coverage_ratios={},
        coverage_counts={},
        failures=(
            ResearchGateFailure(
                code="snapshot_artifacts_unverified",
                message="execution verification remains",
            ),
        ),
    )
    queued = app._submission_gate(request, preliminary)

    assert queued.allowed is True
    assert queued.research_status == "comparable"
    assert queued.audit_run_id == preliminary.audit_run_id
    assert queued.dataset_snapshot_id == preliminary.dataset_snapshot_id
    assert queued.dataset_binding_hash == preliminary.dataset_binding_hash
    assert "open_gated_research_store" not in APP.read_text(encoding="utf-8")


def test_active_fragment_reruns_the_full_app_before_rendering_terminal_artifacts() -> None:
    source = APP.read_text(encoding="utf-8")
    assert 'st.rerun(scope="app")' in source
    terminal_check = source.index("if current.job.status not in _ACTIVE_JOB_STATUSES")
    artifact_render = source.index("_render_job_detail(controller, current)", terminal_check)
    assert terminal_check < artifact_render
