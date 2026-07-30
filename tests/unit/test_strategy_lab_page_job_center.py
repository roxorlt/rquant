from __future__ import annotations

import ast
import stat
from datetime import date
from pathlib import Path

import pytest

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


def test_strategy_lab_entrypoint_only_delegates_to_the_job_center_app() -> None:
    tree = _tree(ENTRYPOINT)
    assert _imports(tree) == {"__future__", "rquant.dashboard.lab.app"}
    assert _called_names(tree) == {"run_strategy_lab_app"}


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
        "list_strategy_lab_runs",
    ):
        assert symbol in source
    for forbidden in (
        "build_strategy_lab_run",
        "save_strategy_lab_run",
        'session_state["compare_result"]',
        'session_state["optimize_result"]',
        'session_state["auction_result"]',
        'session_state["growth_result"]',
    ):
        assert forbidden not in source
    assert "controller.discard_zip(receipt)" in source


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


def test_lab_ui_runtime_root_tightens_only_an_owned_physical_directory(
    tmp_path: Path,
) -> None:
    from rquant.dashboard.lab.app import _ensure_private_runtime_directory

    runtime_root = tmp_path / "lab-runtime"
    runtime_root.mkdir(mode=0o755)

    _ensure_private_runtime_directory(runtime_root)

    assert stat.S_IMODE(runtime_root.lstat().st_mode) == 0o700


def test_lab_ui_runtime_root_rejects_a_symlink(tmp_path: Path) -> None:
    from rquant.dashboard.lab.app import _ensure_private_runtime_directory

    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    alias = tmp_path / "lab-runtime"
    alias.symlink_to(target, target_is_directory=True)

    with pytest.raises(RuntimeError, match="owned physical directory"):
        _ensure_private_runtime_directory(alias)


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
