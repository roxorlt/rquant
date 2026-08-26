from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import asdict
from pathlib import Path

import pytest

from rquant.release_generation import (
    LAB_LAUNCHD_HANDOFF_LABELS,
    DeploymentIntent,
    EnvironmentSelector,
    LabHandoffRecord,
    LabInstallationIdentity,
    PathIdentity,
    ReleaseGenerationAuthority,
    ReleaseGenerationCommit,
    ReleaseGenerationError,
    ReleaseGenerationMarker,
    _read_private_json,
    _verified_interpreter,
    _write_private_json,
    commit_path_for_lock,
    environment_manifest_path_for_lock,
    environment_root_for_lock,
    environment_selector_path_for_lock,
    generation_code_root,
    initialization_path_for_lock,
    intent_path_for_lock,
    marker_path_for_lock,
    prepared_intent_path_for_lock,
    validate_lab_handoff_supersede_chain,
)
from rquant.strict_json import canonical_json_bytes

_ORIGINAL_OS_WALK = os.walk

TRUSTED_GIT = Path("/usr/bin/git")


def _handoff_record(
    *,
    operation_id: str,
    action: str,
    target_sha: str,
    supersedes_operation_id: str,
    installation: LabInstallationIdentity,
) -> LabHandoffRecord:
    payload: dict[str, object] = {
        "schema_version": 1,
        "operation_id": operation_id,
        "checkout_root": "/private/runtime/rquant",
        "labels": ["scheduler", "worker", "finalizer"],
        "loaded_labels": ["scheduler", "worker", "finalizer"],
        "stopped_labels": ["scheduler", "worker", "finalizer"],
        "restarted_labels": ["scheduler", "worker", "finalizer"],
        "target_ref": target_sha,
        "target_sha": target_sha,
        "action": action,
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": asdict(installation),
        "supersedes_operation_id": supersedes_operation_id,
        "stage": "completed",
        "updated_at": "2026-07-28T00:00:00+00:00",
        "generation_operation_id": "d" * 32,
        "environment_generation_id": "e" * 64,
        "code_sha": target_sha,
    }
    return LabHandoffRecord.from_payload(payload, completed=True)


def _partial_handoff_record(
    *,
    operation_id: str,
    action: str,
    target_sha: str,
    supersedes_operation_id: str,
    installation: LabInstallationIdentity,
    stage: str,
    stopped_labels: tuple[str, ...] = (),
    restarted_labels: tuple[str, ...] = (),
) -> LabHandoffRecord:
    labels = ("scheduler", "worker", "finalizer")
    return LabHandoffRecord.from_payload(
        {
            "schema_version": 1,
            "operation_id": operation_id,
            "checkout_root": "/private/runtime/rquant",
            "labels": list(labels),
            "loaded_labels": list(labels),
            "stopped_labels": list(stopped_labels),
            "restarted_labels": list(restarted_labels),
            "target_ref": target_sha,
            "target_sha": target_sha,
            "action": action,
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": asdict(installation),
            "supersedes_operation_id": supersedes_operation_id,
            "stage": stage,
            "updated_at": "2026-07-28T00:00:00+00:00",
        },
        completed=False,
    )


def _marker_payload() -> dict[str, object]:
    identity = asdict(PathIdentity(device=1, inode=2, mode=0o100500, owner=os.getuid(), links=1))
    return {
        "schema_version": 1,
        "operation_id": "a" * 32,
        "transaction_kind": "deployment",
        "commit": "b" * 40,
        "uv_lock_sha256": "c" * 64,
        "pyproject_sha256": "d" * 64,
        "package_version": "0.99.0",
        "python_version": "3.12.0",
        "python_abi": "cpython-312",
        "venv_path": "/private/runtime/venv",
        "venv_identity": identity,
        "pyvenv_cfg_sha256": "e" * 64,
        "python_path": "/private/runtime/venv/bin/python",
        "python_identity": identity,
        "site_packages_path": "/private/runtime/venv/lib/python3.12/site-packages",
        "site_packages_identity": identity,
        "environment_generation_id": "f" * 64,
        "previous_generation_id": "0" * 64,
        "environment_manifest_sha256": "1" * 64,
        "published_at": "2026-07-28T00:00:00+00:00",
    }


def _selector_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "operation_id": "a" * 32,
        "transaction_kind": "deployment",
        "commit": "b" * 40,
        "generation_id": "c" * 64,
        "previous_generation_id": "d" * 64,
        "environment_path": "/private/runtime/generation",
        "manifest_name": "manifest.json",
        "manifest_sha256": "e" * 64,
        "published_at": "2026-07-28T00:00:00+00:00",
    }


def _commit_payload() -> dict[str, object]:
    return {
        "schema_version": 1,
        "operation_id": "a" * 32,
        "transaction_kind": "deployment",
        "commit": "b" * 40,
        "marker_sha256": "c" * 64,
        "transaction_sha256": "d" * 64,
        "environment_generation_id": "e" * 64,
        "previous_generation_id": "f" * 64,
        "environment_manifest_sha256": "0" * 64,
        "committed_at": "2026-07-28T00:00:00+00:00",
    }


def _write_lab_installation(repo: Path, lock_path: Path) -> dict[str, object]:
    path = lock_path.with_name(f"{lock_path.stem}.lab-install.json")
    payload = {
        "schema_version": 2,
        "checkout_root": str(repo),
        "labels": ["fixture"],
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    encoded = canonical + b"\n"
    path.write_bytes(encoded)
    path.chmod(0o600)
    observed = path.stat()
    return asdict(
        LabInstallationIdentity(
            path=str(path),
            sha256=hashlib.sha256(canonical).hexdigest(),
            device=observed.st_dev,
            inode=observed.st_ino,
        )
    )


def _write_gc_lab_installation(
    tmp_path: Path,
    repo: Path,
    lock_path: Path,
    *,
    code_sha: str,
    generation_id: str,
) -> tuple[Path, Path, dict[str, object], dict[str, object]]:
    launch_agents = tmp_path / "LaunchAgents"
    launch_agents.mkdir(mode=0o700)
    runtime_root = tmp_path / "lab-runtime"
    readiness_root = runtime_root / "readiness"
    readiness_root.mkdir(parents=True, mode=0o700)
    runtime_root.chmod(0o700)
    readiness_root.chmod(0o700)
    runtime_stat = runtime_root.stat()
    installed_bindings: dict[str, dict[str, object]] = {}
    for label in LAB_LAUNCHD_HANDOFF_LABELS:
        path = launch_agents / f"{label}.plist"
        path.write_text(f"{label}:{generation_id}\n", encoding="utf-8")
        path.chmod(0o600)
        observed = path.stat()
        installed_bindings[label] = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "device": observed.st_dev,
            "inode": observed.st_ino,
        }
    handoff_operation_id = "7" * 32
    registered: dict[str, object] = {
        "schema_version": 2,
        "checkout_root": str(repo),
        "labels": list(LAB_LAUNCHD_HANDOFF_LABELS),
        "plists": installed_bindings,
        "runtime_root": str(runtime_root),
        "readiness_root": str(readiness_root),
        "registered_by_commit": code_sha,
        "prepared_authority": {
            "runtime_authority_id": "6" * 32,
            "runtime_root": str(runtime_root),
            "runtime_device": runtime_stat.st_dev,
            "runtime_inode": runtime_stat.st_ino,
        },
        "installed_at": "2026-07-29T00:00:00+00:00",
        "environment_generation_id": generation_id,
        "handoff_operation_id": handoff_operation_id,
    }
    local: dict[str, object] = {
        "schema_version": 2,
        "code_sha": code_sha,
        "environment_generation_id": generation_id,
        "handoff_operation_id": handoff_operation_id,
        "launch_agents_dir": str(launch_agents),
        "plists": {
            f"{label}.plist": installed_bindings[label] for label in LAB_LAUNCHD_HANDOFF_LABELS
        },
    }
    registered_path = lock_path.with_name(f"{lock_path.stem}.lab-install.json")
    registered_path.write_bytes(canonical_json_bytes(registered, trailing_newline=True))
    registered_path.chmod(0o600)
    local_path = lock_path.with_name(f"{lock_path.stem}.lab-local-install.json")
    local_path.write_bytes(canonical_json_bytes(local, trailing_newline=True))
    local_path.chmod(0o600)
    return local_path, registered_path, local, registered


def _deployment_intent_payload() -> dict[str, object]:
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/preflight.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
    )
    return json.loads(json.dumps(asdict(intent)))


def _advance_deployment_intent(
    authority: ReleaseGenerationAuthority,
    intent: DeploymentIntent,
    *,
    target_stage: str,
    action: str = "deploy",
) -> DeploymentIntent:
    stages = (
        "timers_stopped",
        f"{action}_checkout_ready",
        f"{action}_dependencies_ready",
        f"{action}_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
        "awaiting_readiness",
        "completed",
    )
    current = authority.read_deployment_intent()
    if current.operation_id != intent.operation_id or target_stage not in stages:
        raise AssertionError("invalid deployment intent fixture transition")
    start = (
        0 if current.stage in {"planned", "recovery_started"} else stages.index(current.stage) + 1
    )
    for stage in stages[start:]:
        current = authority.update_deployment_intent(
            operation_id=current.operation_id,
            stage=stage,
        )
        if stage == target_stage:
            return current
    raise AssertionError("fixture target stage precedes current stage")


@pytest.mark.parametrize(
    "mutation",
    [
        lambda payload: payload.update({"unexpected": "value"}),
        lambda payload: payload.pop("previous_generation_id"),
        lambda payload: payload.update({"schema_version": True}),
        lambda payload: payload.update({"schema_version": "1"}),
        lambda payload: payload.update({"changed_files": "src/rquant/preflight.py"}),
        lambda payload: payload.update({"changed_files": [1]}),
        lambda payload: payload.update({"handoff_operation_id": None}),
        lambda payload: payload["stage_history"][0].update({"timestamp": 1}),
        lambda payload: payload["stage_history"][0].update({"extra": "value"}),
    ],
    ids=(
        "extra-field",
        "missing-field",
        "bool-schema",
        "string-schema",
        "string-list",
        "non-string-list-item",
        "null-string",
        "non-string-timestamp",
        "extra-history-field",
    ),
)
def test_deployment_intent_payload_rejects_non_json_exact_types(
    mutation: Callable[[dict[str, object]], object],
) -> None:
    payload = _deployment_intent_payload()
    mutation(payload)

    with pytest.raises(ReleaseGenerationError, match="intent.*malformed|type|field"):
        DeploymentIntent.from_payload(payload)


@pytest.mark.parametrize(
    ("record_type", "payload_factory", "mutation"),
    [
        (
            ReleaseGenerationMarker,
            _marker_payload,
            lambda payload: payload.update({"unexpected": "value"}),
        ),
        (
            ReleaseGenerationMarker,
            _marker_payload,
            lambda payload: payload.pop("python_abi"),
        ),
        (
            ReleaseGenerationMarker,
            _marker_payload,
            lambda payload: payload.update({"schema_version": True}),
        ),
        (
            ReleaseGenerationMarker,
            _marker_payload,
            lambda payload: payload.update({"operation_id": 123}),
        ),
        (
            EnvironmentSelector,
            _selector_payload,
            lambda payload: payload.update({"unexpected": "value"}),
        ),
        (
            EnvironmentSelector,
            _selector_payload,
            lambda payload: payload.pop("manifest_name"),
        ),
        (
            EnvironmentSelector,
            _selector_payload,
            lambda payload: payload.update({"schema_version": True}),
        ),
        (
            EnvironmentSelector,
            _selector_payload,
            lambda payload: payload.update({"generation_id": 123}),
        ),
        (
            ReleaseGenerationCommit,
            _commit_payload,
            lambda payload: payload.update({"unexpected": "value"}),
        ),
        (
            ReleaseGenerationCommit,
            _commit_payload,
            lambda payload: payload.pop("transaction_sha256"),
        ),
        (
            ReleaseGenerationCommit,
            _commit_payload,
            lambda payload: payload.update({"schema_version": True}),
        ),
        (
            ReleaseGenerationCommit,
            _commit_payload,
            lambda payload: payload.update({"committed_at": 123}),
        ),
    ],
    ids=(
        "marker-extra",
        "marker-missing",
        "marker-bool-schema",
        "marker-string-coercion",
        "selector-extra",
        "selector-missing",
        "selector-bool-schema",
        "selector-string-coercion",
        "commit-extra",
        "commit-missing",
        "commit-bool-schema",
        "commit-string-coercion",
    ),
)
def test_generation_authority_records_require_exact_json_fields_and_types(
    record_type: type[ReleaseGenerationMarker]
    | type[EnvironmentSelector]
    | type[ReleaseGenerationCommit],
    payload_factory: Callable[[], dict[str, object]],
    mutation: Callable[[dict[str, object]], object],
) -> None:
    payload = payload_factory()
    mutation(payload)

    with pytest.raises(ReleaseGenerationError, match="malformed|invalid"):
        record_type.from_payload(payload)


def test_deployment_intent_rejects_illegal_planned_to_completed_transition() -> None:
    payload = _deployment_intent_payload()
    completed_at = "2999-01-01T00:00:00+00:00"
    payload["stage"] = "completed"
    payload["updated_at"] = completed_at
    payload["stage_history"].append({"stage": "completed", "timestamp": completed_at})

    with pytest.raises(ReleaseGenerationError, match="transition|history"):
        DeploymentIntent.from_payload(payload)

    intent = DeploymentIntent.from_payload(_deployment_intent_payload())
    with pytest.raises(ReleaseGenerationError, match="transition"):
        intent.advance(stage="completed")


def test_deployment_intent_requires_readiness_before_installed_completion() -> None:
    intent = DeploymentIntent.from_payload(_deployment_intent_payload())
    for stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
        "awaiting_readiness",
    ):
        intent = intent.advance(stage=stage)

    assert intent.stage == "awaiting_readiness"
    assert intent.advance(stage="completed").stage == "completed"


def test_installed_deployment_intent_cannot_skip_readiness_stage() -> None:
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
        handoff_operation_id="e" * 32,
        handoff_labels=("com.roxor.rquant-lab-scheduler",),
    )
    for stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
    ):
        intent = intent.advance(stage=stage)

    with pytest.raises(ReleaseGenerationError, match="readiness"):
        intent.advance(stage="completed")


def test_completed_deployment_intent_cannot_rebind_handoff() -> None:
    intent = DeploymentIntent.from_payload(_deployment_intent_payload())
    for stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
        "completed",
    ):
        intent = intent.advance(stage=stage)

    with pytest.raises(ReleaseGenerationError, match="completed.*handoff|handoff.*completed"):
        intent.rebind_handoff(
            handoff_operation_id="e" * 32,
            handoff_labels=("com.roxor.rquant-lab-scheduler",),
        )


def test_deployment_intent_persists_original_handoff_operation() -> None:
    original_operation = "1" * 32
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
        handoff_operation_id=original_operation,
        handoff_labels=("scheduler",),
    )

    assert intent.initial_handoff_operation_id == original_operation


def test_deployment_intent_rebound_history_must_anchor_original_operation() -> None:
    original_operation = "1" * 32
    recovering = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
        handoff_operation_id=original_operation,
        handoff_labels=("scheduler",),
    ).advance(stage="recovery_started")
    rebound = recovering.rebind_handoff(
        handoff_operation_id="2" * 32,
        handoff_labels=("scheduler",),
    )
    payload = json.loads(json.dumps(asdict(rebound)))
    payload["stage_history"][2]["previous_handoff_operation_id"] = "9" * 32

    with pytest.raises(ReleaseGenerationError, match="rebound history"):
        DeploymentIntent.from_payload(payload)


def test_deployment_handoff_rebind_requires_adjacent_recovery_and_new_operation() -> None:
    labels = ("com.roxor.rquant-lab-scheduler",)
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
        handoff_operation_id="e" * 32,
        handoff_labels=labels,
    )

    with pytest.raises(ReleaseGenerationError, match="recovery"):
        intent.rebind_handoff(
            handoff_operation_id="f" * 32,
            handoff_labels=labels,
        )

    recovering = intent.advance(stage="recovery_started")
    with pytest.raises(ReleaseGenerationError, match="operation"):
        recovering.rebind_handoff(
            handoff_operation_id="e" * 32,
            handoff_labels=labels,
        )

    rebound = recovering.rebind_handoff(
        handoff_operation_id="f" * 32,
        handoff_labels=labels,
    )
    assert rebound.handoff_operation_id == "f" * 32
    assert rebound.stage_history[-1] == {
        "stage": "handoff_rebound",
        "timestamp": rebound.updated_at,
        "previous_handoff_operation_id": "e" * 32,
        "handoff_operation_id": "f" * 32,
    }


def test_deployment_intent_rejects_unbound_or_nonadjacent_rebound_history() -> None:
    payload = _deployment_intent_payload()
    recovery_at = "2999-01-01T00:00:00+00:00"
    rebound_at = "2999-01-01T00:00:01+00:00"
    payload["stage"] = "recovery_started"
    payload["updated_at"] = rebound_at
    payload["handoff_operation_id"] = "f" * 32
    payload["handoff_labels"] = ["com.roxor.rquant-lab-scheduler"]
    payload["stage_history"].extend(
        [
            {"stage": "recovery_started", "timestamp": recovery_at},
            {"stage": "handoff_rebound", "timestamp": rebound_at},
        ]
    )

    with pytest.raises(ReleaseGenerationError, match="rebound|history"):
        DeploymentIntent.from_payload(payload)


def test_deployment_intent_history_does_not_hide_illegal_rebound_transition() -> None:
    intent = DeploymentIntent.from_payload(_deployment_intent_payload())
    for stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
        "completed",
    ):
        intent = intent.advance(stage=stage)
    payload = json.loads(json.dumps(asdict(intent)))
    rebound_at = "2999-01-01T00:00:00+00:00"
    payload["updated_at"] = rebound_at
    payload["stage_history"].append({"stage": "handoff_rebound", "timestamp": rebound_at})

    with pytest.raises(ReleaseGenerationError, match="transition|history"):
        DeploymentIntent.from_payload(payload)


@pytest.fixture(autouse=True)
def _remove_immutable_test_generations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    monkeypatch.setenv("RQUANT_RELEASE_GENERATION_MIN_FREE_BYTES", "0")
    try:
        yield
    finally:
        generation_roots = [
            Path(current_root) / name
            for current_root, directory_names, _file_names in _ORIGINAL_OS_WALK(tmp_path)
            for name in directory_names
            if name.endswith(".venvs")
        ]
        for root in generation_roots:
            if root.is_symlink() or not root.is_dir():
                continue
            for current_root, _directory_names, file_names in _ORIGINAL_OS_WALK(root):
                current = Path(current_root)
                if hasattr(os, "chflags"):
                    os.chflags(current, 0)
                current.chmod(0o700)
                for name in file_names:
                    path = current / name
                    if not path.is_symlink():
                        if hasattr(os, "chflags"):
                            os.chflags(path, 0)
                        path.chmod(0o600)
            shutil.rmtree(root)


def _generation(tmp_path: Path) -> tuple[Path, Path, str, Path]:
    repo = tmp_path / "rquant"
    package = repo / "src" / "rquant"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    (repo / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n',
        encoding="utf-8",
    )
    (repo / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    venv = repo / ".venv"
    python = venv / "bin" / "python"
    python.parent.mkdir(parents=True)
    shutil.copy2(sys.executable, python)
    python.chmod(0o700)
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    library = Path(sys.base_prefix) / "lib" / f"libpython{python_version}.dylib"
    if library.exists():
        (venv / "lib").mkdir()
        shutil.copy2(library, venv / "lib" / library.name)
    (venv / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {python_version}\n",
        encoding="utf-8",
    )
    site_packages = (
        venv / "lib" / f"python{sys.version_info.major}.{sys.version_info.minor}" / "site-packages"
    )
    site_packages.mkdir(parents=True)
    (repo / ".gitignore").write_text("/.venv\n", encoding="utf-8")
    subprocess.run([str(TRUSTED_GIT), "init", "-q"], cwd=repo, check=True)
    subprocess.run([str(TRUSTED_GIT), "add", "."], cwd=repo, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "generation",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    lock_path = lock_root / "rquant.lock"
    return repo, lock_path, commit, python


def _authority(
    repo: Path,
    lock_path: Path,
    lock_fd: int,
    python: Path,
    *,
    mutation_hook: object | None = None,
    gc_grace_seconds: float | None = None,
    minimum_free_bytes: int | None = None,
    uv_path: Path | None = None,
    cancellation_check: Callable[[], bool] | None = None,
) -> ReleaseGenerationAuthority:
    def copy_fixture_environment(destination: Path) -> None:
        shutil.copytree(repo / ".venv", destination, dirs_exist_ok=True, symlinks=True)

    return ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        mutation_hook=mutation_hook,
        gc_grace_seconds=gc_grace_seconds,
        minimum_free_bytes=minimum_free_bytes,
        uv_path=uv_path,
        environment_builder=(None if uv_path is not None else copy_fixture_environment),
        cancellation_check=cancellation_check,
    )


def _publish_initialized(
    authority: ReleaseGenerationAuthority,
    *,
    commit: str,
) -> object:
    initialization = authority.begin_initialization(target_sha=commit)
    marker = authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)
    authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    return marker


def test_verified_interpreter_rejection_names_every_failed_predicate(tmp_path: Path) -> None:
    candidate = tmp_path / "python"
    shutil.copy2(sys.executable, candidate)
    candidate.chmod(0o646)

    with pytest.raises(ReleaseGenerationError) as group_writable:
        _verified_interpreter(candidate, label="deployment system Python")

    assert str(group_writable.value) == (
        "deployment system Python has unsafe identity: "
        "failed no-group-or-other-write, owner-executable"
    )

    candidate.chmod(0o700)
    linked = tmp_path / "python-hardlink"
    os.link(candidate, linked)
    with pytest.raises(ReleaseGenerationError) as multi_linked:
        _verified_interpreter(candidate, label="deployment system Python")

    assert str(multi_linked.value) == (
        "deployment system Python has unsafe identity: failed single-link"
    )

    linked.unlink()
    resolved, identity, digest = _verified_interpreter(
        candidate,
        label="deployment system Python",
    )
    assert resolved == candidate.resolve(strict=True)
    assert identity.owner == os.getuid()
    assert len(digest) == 64


def test_release_generation_marker_binds_checkout_lock_python_and_venv(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    marker = _publish_initialized(authority, commit=commit)
    verified = authority.verify(expected_commit=commit)

    assert verified == marker
    assert marker.commit == commit
    assert marker.uv_lock_sha256
    assert marker.package_version == "0.99.0"
    assert marker.python_abi
    assert marker.venv_identity.inode > 0
    assert marker_path_for_lock(lock_path).is_file()
    os.close(lock_fd)


def test_runtime_identity_guard_full_verifies_once_then_uses_constant_authorities(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.release_generation as release_module
    from rquant import lab_daemon
    from rquant.lab_daemon import LabRuntimeGuard

    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    marker = _publish_initialized(authority, commit=commit)
    checkout_root = generation_code_root(Path(marker.venv_path))
    calls = {"manifest": 0, "python_facts": 0}
    original_manifest_verify = release_module._verify_environment_manifest
    original_python_facts = release_module._python_facts

    def count_manifest(*args: object, **kwargs: object) -> None:
        calls["manifest"] += 1
        original_manifest_verify(*args, **kwargs)

    def count_python_facts(*args: object, **kwargs: object) -> tuple[str, str]:
        calls["python_facts"] += 1
        return original_python_facts(*args, **kwargs)

    monkeypatch.setattr(release_module, "_verify_environment_manifest", count_manifest)
    monkeypatch.setattr(release_module, "_python_facts", count_python_facts)
    guard = LabRuntimeGuard(
        checkout_root,
        commit,
        verifier=lambda _root: authority.verify(expected_commit=commit).commit,
        deployment_generation=commit,
        deployment_lock_path=lock_path,
        deployment_generation_fd=lock_fd,
    )
    try:
        identity = guard.verify_runtime_identity()
        assert calls == {"manifest": 1, "python_facts": 1}
        assert identity.code_sha == commit
        assert identity.checkout_root.path == checkout_root
        assert identity.generation_root is not None
        assert identity.generation_root.path == Path(marker.venv_path)
        assert identity.selector is not None
        assert identity.selector.path == environment_selector_path_for_lock(lock_path)
        assert identity.manifest is not None
        assert identity.manifest.path == environment_manifest_path_for_lock(
            lock_path,
            marker.environment_generation_id,
        )
        assert identity.marker is not None
        assert identity.marker.path == marker_path_for_lock(lock_path)
        assert identity.python_path == Path(marker.python_path)
        assert identity.venv_path == Path(marker.venv_path)
        assert identity.package_root == checkout_root / "src" / "rquant"

        def forbidden_full_path(*_args: object, **_kwargs: object) -> object:
            pytest.fail("fast runtime identity guard entered the full verification path")

        monkeypatch.setattr(LabRuntimeGuard, "verify", forbidden_full_path)
        monkeypatch.setattr(lab_daemon, "require_lab_runtime_binding", forbidden_full_path)
        monkeypatch.setattr(release_module, "run_contained", forbidden_full_path)
        monkeypatch.setattr(
            release_module,
            "_verify_environment_manifest",
            forbidden_full_path,
        )
        monkeypatch.setattr(release_module, "_python_facts", forbidden_full_path)

        for _ in range(100):
            assert guard.verify_identity(identity) == commit

        assert calls == {"manifest": 1, "python_facts": 1}
    finally:
        os.close(lock_fd)


def test_runtime_identity_guard_rejects_authority_and_seal_drift(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from rquant.lab_daemon import LabDaemonConfigurationError, LabRuntimeGuard

    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    marker = _publish_initialized(authority, commit=commit)
    checkout_root = generation_code_root(Path(marker.venv_path))
    generation_root = Path(marker.venv_path)
    selector_path = environment_selector_path_for_lock(lock_path)
    manifest_path = environment_manifest_path_for_lock(
        lock_path,
        marker.environment_generation_id,
    )
    marker_path = marker_path_for_lock(lock_path)
    guard = LabRuntimeGuard(
        checkout_root,
        commit,
        verifier=lambda _root: commit,
        deployment_generation=commit,
        deployment_lock_path=lock_path,
        deployment_generation_fd=lock_fd,
    )

    def assert_file_drift(path: Path) -> None:
        nonlocal identity
        original = path.read_bytes()
        path.chmod(0o600)
        path.write_bytes(original + b" ")
        path.chmod(0o600)
        with pytest.raises(LabDaemonConfigurationError, match="runtime authority"):
            guard.verify_identity(identity)
        path.write_bytes(original)
        path.chmod(0o600)
        identity = guard.capture_verified_identity(commit)

    try:
        identity = guard.capture_verified_identity(commit)
        with pytest.raises(LabDaemonConfigurationError, match="identity binding"):
            guard.verify_identity(replace(identity, code_sha="f" * 40))
        for authority_path in (selector_path, manifest_path, marker_path):
            assert_file_drift(authority_path)

        generation_root.chmod(0o700)
        with pytest.raises(LabDaemonConfigurationError, match="runtime authority"):
            guard.verify_identity(identity)
        generation_root.chmod(0o500)
        identity = guard.capture_verified_identity(commit)

        package_root = checkout_root / "src" / "rquant"
        package_root.chmod(0o700)
        with pytest.raises(LabDaemonConfigurationError, match="runtime authority"):
            guard.verify_identity(identity)
        package_root.chmod(0o500)
        identity = guard.capture_verified_identity(commit)

        runtime_python = Path(marker.python_path)
        assert runtime_python.is_file() and not runtime_python.is_symlink()
        runtime_python.chmod(0o700)
        with pytest.raises(LabDaemonConfigurationError, match="runtime authority"):
            guard.verify_identity(identity)
        runtime_python.chmod(0o500)
        identity = guard.capture_verified_identity(commit)

        displaced = generation_root.with_name(f"{generation_root.name}.displaced")
        generation_root.chmod(0o700)
        generation_root.rename(displaced)
        generation_root.mkdir(mode=0o500)
        try:
            with pytest.raises(LabDaemonConfigurationError, match="runtime authority"):
                guard.verify_identity(identity)
        finally:
            generation_root.chmod(0o700)
            generation_root.rmdir()
            displaced.rename(generation_root)
            generation_root.chmod(0o500)
    finally:
        os.close(lock_fd)


def test_real_minimal_uv_venv_is_accepted_for_initialization_and_deployment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uv_name = shutil.which("uv")
    assert uv_name is not None
    uv_path = Path(uv_name).resolve(strict=True)
    repo, lock_path, commit, _python = _generation(tmp_path)
    package = repo / "src" / "rquant"
    (package / "cli.py").write_text(
        "def main():\n    print('tiny-rquant-ok')\n",
        encoding="utf-8",
    )
    pyproject = repo / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8")
        + (
            '\n[project.scripts]\nrquant = "rquant.cli:main"\n'
            '\n[build-system]\nrequires = []\nbuild-backend = "backend"\n'
            'backend-path = ["."]\n'
        ),
        encoding="utf-8",
    )
    (repo / "backend.py").write_text(
        "from pathlib import Path\n"
        "from zipfile import ZIP_DEFLATED, ZipFile\n"
        "def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):\n"
        "    del config_settings, metadata_directory\n"
        "    name = 'rquant-0.99.0-py3-none-any.whl'\n"
        "    target = Path(wheel_directory) / name\n"
        "    dist = 'rquant-0.99.0.dist-info'\n"
        "    with ZipFile(target, 'w', ZIP_DEFLATED) as wheel:\n"
        "        wheel.write('src/rquant/__init__.py', 'rquant/__init__.py')\n"
        "        wheel.write('src/rquant/cli.py', 'rquant/cli.py')\n"
        "        wheel.writestr(dist + '/METADATA', "
        "'Metadata-Version: 2.1\\nName: rquant\\nVersion: 0.99.0\\n')\n"
        "        wheel.writestr(dist + '/WHEEL', "
        "'Wheel-Version: 1.0\\nGenerator: rquant-test\\nRoot-Is-Purelib: true\\n'"
        "'Tag: py3-none-any\\n')\n"
        "        wheel.writestr(dist + '/entry_points.txt', "
        "'[console_scripts]\\nrquant = rquant.cli:main\\n')\n"
        "        wheel.writestr(dist + '/RECORD', '')\n"
        "    return name\n"
        "build_editable = build_wheel\n",
        encoding="utf-8",
    )
    shutil.rmtree(repo / ".venv")
    cache = tmp_path / "uv-cache"
    monkeypatch.setenv("UV_CACHE_DIR", str(cache))
    subprocess.run(
        [str(uv_path), "venv", "--python", sys.executable, str(repo / ".venv")],
        cwd=repo,
        check=True,
        env={**os.environ, "UV_CACHE_DIR": str(cache)},
        capture_output=True,
        text=True,
    )
    (repo / "uv.lock").unlink()
    subprocess.run(
        [str(uv_path), "lock", "--python", sys.executable],
        cwd=repo,
        check=True,
        env={**os.environ, "UV_CACHE_DIR": str(cache)},
        capture_output=True,
        text=True,
    )
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "add",
            "backend.py",
            "pyproject.toml",
            "src/rquant/cli.py",
            "uv.lock",
        ],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "lock real uv environment",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    python = repo / ".venv" / "bin" / "python"
    assert python.is_symlink()
    assert (repo / ".venv" / "bin" / "python3").readlink() == Path("python")
    assert sum(path.lstat().st_size for path in (repo / ".venv").rglob("*")) < 1_000_000
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python, uv_path=uv_path)

    initialized = _publish_initialized(authority, commit=commit)
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha="e" * 40,
        target_ref="e" * 40,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    _advance_deployment_intent(
        authority,
        deployment,
        target_stage="timers_restored",
    )
    deployed = authority.publish(
        expected_commit=commit,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )

    assert initialized.environment_generation_id != deployed.environment_generation_id
    assert deployed.previous_generation_id == initialized.environment_generation_id
    launcher = Path(deployed.venv_path) / "bin" / "rquant"
    executed = subprocess.run(
        [str(launcher)],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    assert executed.stdout.strip() == "tiny-rquant-ok"
    launcher_payload = launcher.read_bytes()
    assert launcher_payload.startswith(b"#!")
    assert b".building" not in launcher_payload
    assert str(repo / ".venv").encode() not in launcher_payload
    manifest = json.loads(
        environment_manifest_path_for_lock(
            lock_path,
            deployed.environment_generation_id,
        ).read_text(encoding="utf-8")
    )
    assert manifest["uv_binding"]["physical_path"] == str(uv_path)
    assert manifest["uv_binding"]["sha256"] == hashlib.sha256(uv_path.read_bytes()).hexdigest()
    os.close(lock_fd)


def test_console_entry_points_are_rebound_from_staging_to_final_generation(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def console_entry_point_environment(destination: Path) -> None:
        shutil.copytree(repo / ".venv", destination, dirs_exist_ok=True, symlinks=True)
        launcher = destination / "bin" / "rquant"
        launcher.write_text(
            f"#!{destination / 'bin' / 'python'}\nfrom rquant.cli import main\nmain()\n",
            encoding="utf-8",
        )
        launcher.chmod(0o700)

    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=console_entry_point_environment,
    )
    try:
        marker = _publish_initialized(authority, commit=commit)
    finally:
        os.close(lock_fd)

    generation = Path(marker.venv_path)
    launcher = generation / "bin" / "rquant"
    assert launcher.read_text(encoding="utf-8").splitlines()[0] == (
        f"#!{generation / 'bin' / 'python'}"
    )
    for path in (generation / "bin").iterdir():
        if path.is_file() and not path.is_symlink():
            payload = path.read_bytes()
            assert b".building" not in payload
            assert str(repo / ".venv").encode() not in payload


def test_environment_builder_rejects_non_whitelisted_symlink(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    external = tmp_path / "external-module.so"
    external.write_bytes(b"do-not-touch")
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def unsafe_builder(destination: Path) -> None:
        shutil.copytree(repo / ".venv", destination, dirs_exist_ok=True, symlinks=True)
        injected = destination / "lib" / "python3.12" / "site-packages" / "evil.so"
        injected.parent.mkdir(parents=True, exist_ok=True)
        injected.symlink_to(external)

    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=unsafe_builder,
    )
    initialization = authority.begin_initialization(target_sha=commit)
    try:
        with pytest.raises(ReleaseGenerationError, match="unsafe symlink"):
            authority.publish(
                expected_commit=commit,
                operation_id=initialization.operation_id,
                transaction_kind="initialization",
            )
    finally:
        os.close(lock_fd)

    assert external.read_bytes() == b"do-not-touch"
    assert list(environment_root_for_lock(lock_path).iterdir()) == []
    assert not lock_path.with_name(f"{lock_path.stem}.environment.json").exists()


def test_release_generation_rejects_uv_lock_but_ignores_mutable_source_venv_drift(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)

    (repo / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    with pytest.raises(ReleaseGenerationError, match="uv.lock"):
        authority.verify(expected_commit=commit)

    (repo / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    displaced = tmp_path / "old-venv"
    (repo / ".venv").rename(displaced)
    (repo / ".venv").mkdir()
    assert authority.verify(expected_commit=commit).commit == commit
    os.close(lock_fd)


def test_interrupted_atomic_marker_publication_leaves_marker_absent(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "marker_temp_fsynced":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )

    with pytest.raises(KeyboardInterrupt):
        initialization = authority.begin_initialization(target_sha=commit)
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_invalidated_generation_cannot_be_verified_until_republished(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)

    authority.invalidate()

    with pytest.raises(ReleaseGenerationError, match="marker"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_release_generation_rejects_tracked_source_drift(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    (repo / "src" / "rquant" / "__init__.py").write_text(
        "UNTRUSTED = True\n",
        encoding="utf-8",
    )

    with pytest.raises(ReleaseGenerationError, match="tracked checkout"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_release_generation_readonly_git_preserves_index_and_disables_optional_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.release_generation as module

    repo, _lock_path, commit, _python = _generation(tmp_path)
    index = repo / ".git" / "index"
    before = (index.read_bytes(), index.stat())
    original_run = module.run_contained
    environments: list[dict[str, str]] = []

    def capture_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        command = args[0]
        if isinstance(command, list) and command and command[0] == str(TRUSTED_GIT):
            environment = kwargs.get("env")
            assert isinstance(environment, dict)
            environments.append(environment)
        return original_run(*args, **kwargs)

    monkeypatch.setattr(module, "run_contained", capture_run)

    assert module._git_output(repo, TRUSTED_GIT, "rev-parse", "HEAD") == commit
    module._assert_tracked_clean(repo, TRUSTED_GIT)
    after = index.stat()
    assert environments
    assert all(environment["GIT_OPTIONAL_LOCKS"] == "0" for environment in environments)
    assert index.read_bytes() == before[0]
    assert (after.st_ino, after.st_mtime_ns) == (before[1].st_ino, before[1].st_mtime_ns)


def test_release_generation_blocking_probes_preserve_shared_absolute_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.release_generation as module

    repo, _lock_path, commit, python = _generation(tmp_path)
    destination = tmp_path / "release-code"
    original_run = module.run_contained
    expected_version, expected_abi = module._python_facts(python)
    expected_cache_tag, separator, expected_soabi = expected_abi.partition(":")
    assert separator
    observed_deadlines: list[float] = []
    requested_caps: list[float] = []
    absolute_deadline = time.monotonic() + 2

    def inherited_deadline(cap: float) -> float:
        requested_caps.append(cap)
        return absolute_deadline

    def capture_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[object]:
        observed_deadlines.append(float(kwargs["deadline_monotonic"]))
        command = args[0]
        if isinstance(command, list) and command and command[0] == str(python):
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "cache_tag": expected_cache_tag,
                        "soabi": expected_soabi,
                        "version": expected_version,
                    }
                )
                + "\n",
                "",
            )
        return original_run(*args, **kwargs)

    monkeypatch.setattr(module, "run_contained", capture_run)

    assert (
        module._git_output(
            repo,
            TRUSTED_GIT,
            "rev-parse",
            "HEAD",
            timeout_provider=inherited_deadline,
        )
        == commit
    )
    module._assert_tracked_clean(repo, TRUSTED_GIT, timeout_provider=inherited_deadline)
    module._materialize_release_code(
        repo=repo,
        git_path=TRUSTED_GIT,
        expected_commit=commit,
        destination=destination,
        checkpoint=lambda: None,
        timeout_provider=inherited_deadline,
    )
    version, abi = module._python_facts(python, timeout_provider=inherited_deadline)

    assert (version, abi) == (expected_version, expected_abi)
    assert len(observed_deadlines) == len(requested_caps) == 6
    assert observed_deadlines == [absolute_deadline] * 6


def test_release_generation_exhausted_shared_deadline_prevents_later_probe_start(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.release_generation as module

    started = False

    def forbidden_run(*_args: object, **_kwargs: object) -> object:
        nonlocal started
        started = True
        raise AssertionError("expired authority deadline must prevent process startup")

    monkeypatch.setattr(module, "run_contained", forbidden_run)

    with pytest.raises(module.ReleaseGenerationError, match="deadline"):
        module._git_output(
            tmp_path,
            TRUSTED_GIT,
            "rev-parse",
            "HEAD",
            timeout_provider=lambda _cap: time.monotonic() - 1,
        )

    assert not started


def test_release_generation_marker_handles_short_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    real_write: Callable[[int, bytes], int] = os.write
    writes: list[int] = []

    def short_write(descriptor: int, payload: bytes) -> int:
        chunk = payload[: max(1, len(payload) // 3)]
        writes.append(len(chunk))
        return real_write(descriptor, chunk)

    monkeypatch.setattr("rquant.release_generation.os.write", short_write)

    initialization = authority.begin_initialization(target_sha=commit)
    published = authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)
    authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert len(writes) > 1
    assert authority.verify(expected_commit=commit) == published
    os.close(lock_fd)


def test_release_generation_does_not_publish_unverified_temporary_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    def reject_temporary_content(*_args: object, **_kwargs: object) -> None:
        raise ReleaseGenerationError("temporary release marker content mismatch")

    monkeypatch.setattr(
        "rquant.release_generation._verify_temporary_payload",
        reject_temporary_content,
    )

    with pytest.raises(ReleaseGenerationError, match="temporary release marker"):
        initialization = authority.begin_initialization(target_sha=commit)
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_deployment_intent_pins_plan_before_marker_invalidation(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    marker = _publish_initialized(authority, commit=commit)

    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha="b" * 40,
        target_ref="v0.99.1",
        changed_files=("src/rquant/monitor.py",),
        restart_services=("rquant-monitor.service",),
        active_services=("rquant-monitor.service",),
        active_timers=("rquant-monitor.timer",),
    )
    authority.invalidate()

    persisted = authority.read_deployment_intent()
    assert persisted == intent
    assert persisted.marker_generation == marker.content_hash()
    assert persisted.stage == "planned"
    assert intent_path_for_lock(lock_path).is_file()
    assert not marker_path_for_lock(lock_path).exists()
    os.close(lock_fd)


def test_prepared_deployment_intent_is_adopted_without_replanning(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    current = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/old.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    for stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
        "marker_published",
        "completed",
    ):
        current = authority.update_deployment_intent(
            operation_id=current.operation_id,
            stage=stage,
        )
    prepared = DeploymentIntent.create(
        previous_sha="b" * 40,
        target_sha="c" * 40,
        target_ref="c" * 40,
        changed_files=("src/rquant/new.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="3" * 64,
        previous_generation_id="4" * 64,
        handoff_operation_id="5" * 32,
        handoff_labels=("scheduler", "worker", "finalizer"),
        operation_id="6" * 32,
    )
    prepared_path = prepared_intent_path_for_lock(lock_path)
    prepared_path.write_text(
        json.dumps(asdict(prepared), sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    prepared_path.chmod(0o600)

    adopted = authority.adopt_prepared_deployment_intent(
        operation_id=prepared.operation_id,
    )

    assert adopted == prepared
    assert authority.read_deployment_intent() == prepared
    assert not prepared_path.exists()
    archive = intent_path_for_lock(lock_path).with_name(
        f"{intent_path_for_lock(lock_path).stem}.{current.operation_id}.completed.json"
    )
    assert DeploymentIntent.from_payload(json.loads(archive.read_text())) == current
    assert (
        authority.adopt_prepared_deployment_intent(operation_id=prepared.operation_id) == prepared
    )
    os.close(lock_fd)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("target_sha", "f" * 40),
        ("target_ref", "f" * 40),
        ("release_profile", "linux-production"),
        ("lifecycle_mode", "uninstalled"),
        (
            "installation_identity",
            {
                "path": "/private/runtime/install.json",
                "sha256": "f" * 64,
                "device": 1,
                "inode": 2,
            },
        ),
    ),
)
def test_handoff_supersede_chain_rejects_binding_drift_at_every_ancestor(
    field: str,
    value: object,
) -> None:
    installation = LabInstallationIdentity(
        path="/private/runtime/install.json",
        sha256="1" * 64,
        device=1,
        inode=2,
    )
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="2" * 64,
        previous_generation_id="3" * 64,
        handoff_operation_id="c" * 32,
        handoff_labels=("scheduler", "worker", "finalizer"),
    )
    root = _handoff_record(
        operation_id="8" * 32,
        action="deploy",
        target_sha=intent.target_sha,
        supersedes_operation_id="",
        installation=installation,
    )
    middle_payload = asdict(
        _handoff_record(
            operation_id="9" * 32,
            action="resume",
            target_sha=intent.target_sha,
            supersedes_operation_id=root.operation_id,
            installation=installation,
        )
    )
    middle_payload["labels"] = list(middle_payload["labels"])
    middle_payload["loaded_labels"] = list(middle_payload["loaded_labels"])
    middle_payload["stopped_labels"] = list(middle_payload["stopped_labels"])
    middle_payload["restarted_labels"] = list(middle_payload["restarted_labels"])
    middle_payload["installation_identity"] = asdict(installation)
    middle_payload[field] = value
    current = _handoff_record(
        operation_id="c" * 32,
        action="rollback",
        target_sha=intent.previous_sha,
        supersedes_operation_id="9" * 32,
        installation=installation,
    )

    with pytest.raises(ReleaseGenerationError):
        middle = LabHandoffRecord.from_payload(middle_payload, completed=True)
        validate_lab_handoff_supersede_chain(
            record=current,
            ancestors=(middle, root),
            intent=intent,
            installation_identity=installation,
            checkout_root="/private/runtime/rquant",
            expected_labels=("scheduler", "worker", "finalizer"),
        )


@pytest.mark.parametrize(
    ("current_action", "ancestor_action"),
    (
        ("resume", "resume"),
        ("resume", "rollback"),
    ),
)
def test_handoff_supersede_chain_rejects_forbidden_action_edges(
    current_action: str,
    ancestor_action: str,
) -> None:
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
        handoff_operation_id="3" * 32,
        handoff_labels=("scheduler", "worker", "finalizer"),
    )
    installation = LabInstallationIdentity(
        path="/private/runtime/install.json",
        sha256="e" * 64,
        device=1,
        inode=2,
    )
    root = _handoff_record(
        operation_id="1" * 32,
        action="deploy",
        target_sha=intent.target_sha,
        supersedes_operation_id="",
        installation=installation,
    )
    ancestor = _handoff_record(
        operation_id="2" * 32,
        action=ancestor_action,
        target_sha=(intent.previous_sha if ancestor_action == "rollback" else intent.target_sha),
        supersedes_operation_id=root.operation_id,
        installation=installation,
    )
    current = _handoff_record(
        operation_id="3" * 32,
        action=current_action,
        target_sha=(intent.previous_sha if current_action == "rollback" else intent.target_sha),
        supersedes_operation_id=ancestor.operation_id,
        installation=installation,
    )

    with pytest.raises(ReleaseGenerationError, match="action|supersede chain"):
        validate_lab_handoff_supersede_chain(
            record=current,
            ancestors=(ancestor, root),
            intent=intent,
            installation_identity=installation,
            checkout_root="/private/runtime/rquant",
            expected_labels=("scheduler", "worker", "finalizer"),
        )


@pytest.mark.parametrize("ancestor_action", ("resume", "rollback"))
def test_handoff_supersede_chain_allows_valid_rollback_edges(
    ancestor_action: str,
) -> None:
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
        handoff_operation_id="1" * 32,
        handoff_labels=("scheduler", "worker", "finalizer"),
    ).advance(stage="recovery_started")
    intent = intent.rebind_handoff(
        handoff_operation_id="2" * 32,
        handoff_labels=("scheduler", "worker", "finalizer"),
    ).advance(stage="recovery_started")
    intent = intent.rebind_handoff(
        handoff_operation_id="3" * 32,
        handoff_labels=("scheduler", "worker", "finalizer"),
    )
    installation = LabInstallationIdentity(
        path="/private/runtime/install.json",
        sha256="e" * 64,
        device=1,
        inode=2,
    )
    root = _handoff_record(
        operation_id="1" * 32,
        action="deploy",
        target_sha=intent.target_sha,
        supersedes_operation_id="",
        installation=installation,
    )
    resumed = _handoff_record(
        operation_id="2" * 32,
        action=ancestor_action,
        target_sha=(intent.target_sha if ancestor_action == "resume" else intent.previous_sha),
        supersedes_operation_id=root.operation_id,
        installation=installation,
    )
    rolled_back = _handoff_record(
        operation_id="3" * 32,
        action="rollback",
        target_sha=intent.previous_sha,
        supersedes_operation_id=resumed.operation_id,
        installation=installation,
    )

    validate_lab_handoff_supersede_chain(
        record=rolled_back,
        ancestors=(resumed, root),
        intent=intent,
        installation_identity=installation,
        checkout_root="/private/runtime/rquant",
        expected_labels=("scheduler", "worker", "finalizer"),
    )


@pytest.mark.parametrize(
    ("stage", "stopped", "restarted"),
    (
        ("planned", (), ()),
        ("stopping", ("scheduler",), ()),
        ("stopped", ("scheduler", "worker", "finalizer"), ()),
        ("restarting", ("scheduler",), ("scheduler",)),
        (
            "aborted",
            ("scheduler",),
            ("scheduler", "worker", "finalizer"),
        ),
    ),
)
def test_partial_handoff_state_model_accepts_crash_recoverable_states(
    stage: str,
    stopped: tuple[str, ...],
    restarted: tuple[str, ...],
) -> None:
    installation = LabInstallationIdentity(
        path="/private/runtime/install.json",
        sha256="e" * 64,
        device=1,
        inode=2,
    )

    record = _partial_handoff_record(
        operation_id="1" * 32,
        action="deploy",
        target_sha="b" * 40,
        supersedes_operation_id="",
        installation=installation,
        stage=stage,
        stopped_labels=stopped,
        restarted_labels=restarted,
    )

    assert record.stage == stage


@pytest.mark.parametrize(
    ("stage", "stopped", "restarted"),
    (
        ("planned", ("scheduler",), ()),
        ("stopping", ("scheduler",), ("scheduler",)),
        ("stopped", ("scheduler",), ()),
        ("restarting", (), ("unknown",)),
        ("aborted", ("scheduler",), ("scheduler",)),
    ),
)
def test_partial_handoff_state_model_rejects_inconsistent_subsets(
    stage: str,
    stopped: tuple[str, ...],
    restarted: tuple[str, ...],
) -> None:
    installation = LabInstallationIdentity(
        path="/private/runtime/install.json",
        sha256="e" * 64,
        device=1,
        inode=2,
    )

    with pytest.raises(ReleaseGenerationError, match="handoff state|binding"):
        _partial_handoff_record(
            operation_id="1" * 32,
            action="deploy",
            target_sha="b" * 40,
            supersedes_operation_id="",
            installation=installation,
            stage=stage,
            stopped_labels=stopped,
            restarted_labels=restarted,
        )


@pytest.mark.parametrize("physical_middle", (None, "d" * 32))
def test_handoff_supersede_chain_exactly_matches_intent_rebound_history(
    physical_middle: str | None,
) -> None:
    labels = ("scheduler", "worker", "finalizer")
    intent = DeploymentIntent.create(
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="c" * 64,
        previous_generation_id="d" * 64,
        handoff_operation_id="1" * 32,
        handoff_labels=labels,
    ).advance(stage="recovery_started")
    intent = intent.rebind_handoff(
        handoff_operation_id="2" * 32,
        handoff_labels=labels,
    ).advance(stage="recovery_started")
    intent = intent.rebind_handoff(
        handoff_operation_id="3" * 32,
        handoff_labels=labels,
    )
    installation = LabInstallationIdentity(
        path="/private/runtime/install.json",
        sha256="e" * 64,
        device=1,
        inode=2,
    )
    root = _handoff_record(
        operation_id="1" * 32,
        action="deploy",
        target_sha=intent.target_sha,
        supersedes_operation_id="",
        installation=installation,
    )
    ancestors = (root,)
    supersedes = root.operation_id
    if physical_middle is not None:
        hidden = _handoff_record(
            operation_id=physical_middle,
            action="rollback",
            target_sha=intent.previous_sha,
            supersedes_operation_id=root.operation_id,
            installation=installation,
        )
        ancestors = (hidden, root)
        supersedes = hidden.operation_id
    current = _handoff_record(
        operation_id="3" * 32,
        action="rollback",
        target_sha=intent.previous_sha,
        supersedes_operation_id=supersedes,
        installation=installation,
    )

    with pytest.raises(ReleaseGenerationError, match="history|chain"):
        validate_lab_handoff_supersede_chain(
            record=current,
            ancestors=ancestors,
            intent=intent,
            installation_identity=installation,
            checkout_root="/private/runtime/rquant",
            expected_labels=labels,
        )


@pytest.mark.parametrize(
    "payload",
    (
        b'{"schema_version":1,"schema_version":2}',
        b'{"outer":{"operation_id":"a","operation_id":"b"}}',
    ),
)
def test_private_authority_json_rejects_duplicate_keys(
    tmp_path: Path,
    payload: bytes,
) -> None:
    root = tmp_path / "authority"
    root.mkdir(mode=0o700)
    record = root / "record.json"
    record.write_bytes(payload)
    record.chmod(0o600)
    root_fd = os.open(root, os.O_RDONLY)
    try:
        with pytest.raises(ReleaseGenerationError, match="duplicate JSON key"):
            _read_private_json(
                root_fd=root_fd,
                root_path=root,
                name=record.name,
                maximum_bytes=4096,
            )
    finally:
        os.close(root_fd)


def test_initialization_sentinel_cannot_be_recreated_by_deleting_marker(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    initialization = authority.begin_initialization(target_sha=commit)
    authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)
    authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    marker_path_for_lock(lock_path).unlink()

    with pytest.raises(ReleaseGenerationError, match="already completed"):
        authority.begin_initialization(target_sha=commit)

    sentinel = initialization_path_for_lock(lock_path)
    assert sentinel.is_file()
    assert (
        DeploymentIntent.from_payload(json.loads(sentinel.read_text(encoding="utf-8"))).stage
        == "completed"
    )
    os.close(lock_fd)


def test_marker_is_rejected_until_intent_and_commit_record_are_complete(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    initialization = authority.begin_initialization(target_sha=commit)

    marker = authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert marker.operation_id == initialization.operation_id
    assert not commit_path_for_lock(lock_path).exists()
    with pytest.raises(ReleaseGenerationError, match="transaction.*completed"):
        authority.verify(expected_commit=commit)

    authority.complete_initialization(operation_id=initialization.operation_id)
    with pytest.raises(ReleaseGenerationError, match="commit record"):
        authority.verify(expected_commit=commit)

    authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    assert authority.verify(expected_commit=commit) == marker
    os.close(lock_fd)


def test_deployment_marker_requires_completed_launchd_handoff(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    initialized = _publish_initialized(authority, commit=commit)
    labels = (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    )
    handoff_operation = "d" * 32
    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation=initialized.content_hash(),
        previous_generation_id=initialized.environment_generation_id,
        handoff_operation_id=handoff_operation,
        handoff_labels=labels,
    )
    authority.invalidate()
    _advance_deployment_intent(
        authority,
        intent,
        target_stage="timers_restored",
    )
    published = authority.publish(
        expected_commit=commit,
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    _advance_deployment_intent(authority, intent, target_stage="awaiting_readiness")
    installation_identity = _write_lab_installation(repo, lock_path)
    handoff_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.{handoff_operation}.json")
    payload = {
        "schema_version": 1,
        "operation_id": handoff_operation,
        "checkout_root": str(repo),
        "labels": list(labels),
        "loaded_labels": list(labels),
        "stopped_labels": list(labels),
        "restarted_labels": [labels[0]],
        "target_ref": commit,
        "target_sha": commit,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": installation_identity,
        "supersedes_operation_id": "",
        "stage": "restarting",
        "updated_at": "2026-07-28T00:00:00+00:00",
    }
    handoff_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    handoff_path.chmod(0o600)
    active_handoff_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.json")
    active_handoff_path.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    active_handoff_path.chmod(0o600)

    with pytest.raises(ReleaseGenerationError, match="transaction.*completed|handoff.*completed"):
        authority.verify(expected_commit=commit)
    authority.verify(expected_commit=commit, provisional_handoff_label=labels[0])

    payload = {
        **payload,
        "restarted_labels": list(labels),
        "stage": "completed",
        "generation_operation_id": intent.operation_id,
        "environment_generation_id": published.environment_generation_id,
        "code_sha": published.commit,
    }
    completed_path = handoff_path.with_name(
        f"{lock_path.stem}.lab-handoff.{handoff_operation}.completed.json"
    )
    for path in (handoff_path, completed_path, active_handoff_path):
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        path.chmod(0o600)
    authority.update_deployment_intent(
        operation_id=intent.operation_id,
        stage="completed",
    )
    authority.commit_generation(
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    authority.verify(expected_commit=commit)

    active_handoff_path.write_text(
        json.dumps(
            {
                **payload,
                "operation_id": "e" * 32,
                "restarted_labels": [],
                "stage": "stopping",
                "generation_operation_id": intent.operation_id,
                "environment_generation_id": published.environment_generation_id,
                "code_sha": published.commit,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    active_handoff_path.chmod(0o600)
    with pytest.raises(ReleaseGenerationError, match="handoff.*record|proof"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


@pytest.mark.parametrize("tamper_ancestor_proof", (False, True))
def test_provisional_handoff_validates_completed_ancestor_proof(
    tmp_path: Path,
    tamper_ancestor_proof: bool,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    initialized = _publish_initialized(authority, commit=commit)
    labels = (
        "com.roxor.rquant-lab-scheduler",
        "com.roxor.rquant-lab-worker",
        "com.roxor.rquant-lab-finalizer",
    )
    root_operation = "d" * 32
    recovery_operation = "e" * 32
    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=("src/rquant/lab_daemon.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation=initialized.content_hash(),
        previous_generation_id=initialized.environment_generation_id,
        handoff_operation_id=root_operation,
        handoff_labels=labels,
    )
    intent = authority.update_deployment_intent(
        operation_id=intent.operation_id,
        stage="recovery_started",
    )
    intent = authority.rebind_deployment_handoff(
        operation_id=intent.operation_id,
        handoff_operation_id=recovery_operation,
        handoff_labels=labels,
    )
    authority.invalidate()
    _advance_deployment_intent(authority, intent, target_stage="timers_restored")
    marker = authority.publish(
        expected_commit=commit,
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    _advance_deployment_intent(authority, intent, target_stage="awaiting_readiness")
    installation = _write_lab_installation(repo, lock_path)
    root_payload = {
        "schema_version": 1,
        "operation_id": root_operation,
        "checkout_root": str(repo),
        "labels": list(labels),
        "loaded_labels": list(labels),
        "stopped_labels": list(labels),
        "restarted_labels": list(labels),
        "target_ref": commit,
        "target_sha": commit,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": installation,
        "supersedes_operation_id": "",
        "stage": "completed",
        "updated_at": "2026-07-28T00:00:00+00:00",
        "generation_operation_id": intent.operation_id,
        "environment_generation_id": marker.environment_generation_id,
        "code_sha": commit,
    }
    proof_payload = dict(root_payload)
    if tamper_ancestor_proof:
        proof_payload["updated_at"] = "2026-07-28T00:00:01+00:00"
    recovery_payload = {
        "schema_version": 1,
        "operation_id": recovery_operation,
        "checkout_root": str(repo),
        "labels": list(labels),
        "loaded_labels": list(labels),
        "stopped_labels": list(labels),
        "restarted_labels": [labels[0]],
        "target_ref": commit,
        "target_sha": commit,
        "action": "resume",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": installation,
        "supersedes_operation_id": root_operation,
        "stage": "restarting",
        "updated_at": "2026-07-28T00:00:02+00:00",
    }
    records = {
        lock_path.with_name(f"{lock_path.stem}.lab-handoff.{root_operation}.json"): root_payload,
        lock_path.with_name(
            f"{lock_path.stem}.lab-handoff.{root_operation}.completed.json"
        ): proof_payload,
        lock_path.with_name(
            f"{lock_path.stem}.lab-handoff.{recovery_operation}.json"
        ): recovery_payload,
        lock_path.with_name(f"{lock_path.stem}.lab-handoff.json"): recovery_payload,
    }
    for path, payload in records.items():
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        path.chmod(0o600)

    if tamper_ancestor_proof:
        with pytest.raises(ReleaseGenerationError, match="proof|inconsistent"):
            authority.verify(
                expected_commit=commit,
                provisional_handoff_label=labels[0],
            )
    else:
        authority.verify(
            expected_commit=commit,
            provisional_handoff_label=labels[0],
        )
    os.close(lock_fd)


def test_generation_environment_build_timeout_kills_uv_process_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    uv_name = shutil.which("uv")
    assert uv_name is not None
    uv_path = Path(uv_name).resolve(strict=True)
    repo, lock_path, _commit, _python = _generation(tmp_path)
    uv_cache = tmp_path / "uv-cache"
    monkeypatch.setenv("UV_CACHE_DIR", str(uv_cache))
    descendant_marker = tmp_path / "uv-builder-descendant-survived"
    pyproject = repo / "pyproject.toml"
    pyproject.write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n'
        '\n[build-system]\nrequires = []\nbuild-backend = "backend"\n'
        'backend-path = ["."]\n',
        encoding="utf-8",
    )
    (repo / "backend.py").write_text(
        "import subprocess, sys, time\n"
        "def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):\n"
        "    del wheel_directory, config_settings, metadata_directory\n"
        "    subprocess.Popen([sys.executable, '-c', "
        f'"import pathlib,time;time.sleep(2);pathlib.Path({str(descendant_marker)!r})'
        ".write_text('alive')\"])\n"
        "    time.sleep(30)\n"
        "build_editable = build_wheel\n",
        encoding="utf-8",
    )
    (repo / "uv.lock").unlink()
    subprocess.run(
        [str(uv_path), "lock", "--python", sys.executable],
        cwd=repo,
        env={**os.environ, "UV_CACHE_DIR": str(uv_cache)},
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    subprocess.run(
        [str(TRUSTED_GIT), "add", "backend.py", "pyproject.toml", "uv.lock"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "add hanging tiny build backend",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    python = repo / ".venv" / "bin" / "python"
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        uv_path=uv_path,
        command_timeout_seconds=1.0,
        overall_deadline_monotonic=time.monotonic() + 1.5,
    )
    initialization = authority.begin_initialization(target_sha=commit)

    with pytest.raises(ReleaseGenerationError, match="timed out"):
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )
    time.sleep(2.2)

    assert not descendant_marker.exists()
    os.close(lock_fd)


def test_generation_authority_recovery_cannot_extend_expired_global_deadline(
    tmp_path: Path,
) -> None:
    repo, lock_path, _commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=lambda _destination: None,
        overall_deadline_monotonic=time.monotonic() - 1,
    )

    recovered = authority.for_recovery(time.monotonic() + 30)

    assert recovered.overall_deadline_monotonic < time.monotonic()
    with pytest.raises(ReleaseGenerationError, match="timed out"):
        recovered.garbage_collect_environments(reason="expired-recovery")
    os.close(lock_fd)


def test_generation_verify_cannot_refresh_expired_startup_deadline(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        overall_deadline_monotonic=time.monotonic() - 0.001,
    )

    with pytest.raises(ReleaseGenerationError, match="timed out"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_environment_generation_is_immutable_and_content_bound(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)

    marker = _publish_initialized(authority, commit=commit)
    selected_python = Path(marker.python_path)

    assert selected_python.is_file()
    assert not selected_python.is_symlink()
    assert not selected_python.is_relative_to(repo / ".venv")
    assert environment_selector_path_for_lock(lock_path).is_file()

    selected_python.chmod(0o700)
    with selected_python.open("ab") as handle:
        handle.write(b"environment-drift")
    selected_python.chmod(0o500)
    with pytest.raises(ReleaseGenerationError, match="environment generation"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_generation_code_authority_survives_checkout_removal(tmp_path: Path) -> None:
    repo, lock_path, _commit, python = _generation(tmp_path)
    (repo / "scripts").mkdir()
    shutil.copy2(
        Path(__file__).resolve().parents[2] / "scripts" / "strict_json.py", repo / "scripts"
    )
    shutil.copy2(
        Path(__file__).resolve().parents[2] / "src" / "rquant" / "release_generation.py",
        repo / "src" / "rquant" / "release_generation.py",
    )
    shutil.copy2(
        Path(__file__).resolve().parents[2] / "src" / "rquant" / "strict_json.py",
        repo / "src" / "rquant" / "strict_json.py",
    )
    shutil.copy2(
        Path(__file__).resolve().parents[2] / "src" / "rquant" / "contained_subprocess.py",
        repo / "src" / "rquant" / "contained_subprocess.py",
    )
    subprocess.run([str(TRUSTED_GIT), "add", "."], cwd=repo, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "runtime authority",
        ],
        cwd=repo,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    marker = _publish_initialized(_authority(repo, lock_path, lock_fd, python), commit=commit)
    code_root = generation_code_root(Path(marker.venv_path))
    authority_path = code_root / "src" / "rquant" / "release_generation.py"
    original_repo = repo.with_name("removed-checkout")
    repo.rename(original_repo)

    spec = importlib.util.spec_from_file_location("_immutable_release_authority", authority_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    verified = module.ReleaseGenerationAuthority(
        repo=code_root,
        immutable_code_root=code_root,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=Path(marker.python_path),
        git_path=TRUSTED_GIT,
    ).verify(expected_commit=commit)

    assert verified.commit == commit
    assert authority_path.is_file()
    assert not repo.exists()
    os.close(lock_fd)


def test_environment_selector_is_not_switched_when_generation_copy_is_interrupted(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    first = _publish_initialized(authority, commit=commit)
    selector = environment_selector_path_for_lock(lock_path)
    before = selector.read_bytes()

    authority.invalidate()
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation=first.content_hash(),
        previous_generation_id=first.environment_generation_id,
    )
    for stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
    ):
        deployment = authority.update_deployment_intent(
            operation_id=deployment.operation_id,
            stage=stage,
        )

    def interrupt(stage: str) -> None:
        if stage == "environment_staged":
            raise KeyboardInterrupt

    interrupted = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    with pytest.raises(KeyboardInterrupt):
        interrupted.publish(
            expected_commit=commit,
            operation_id=deployment.operation_id,
            transaction_kind="deployment",
        )

    assert selector.read_bytes() == before
    os.close(lock_fd)


def test_environment_generation_can_resume_after_rename_before_manifest(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "environment_generation_ready":
            raise KeyboardInterrupt

    interrupted = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = interrupted.begin_initialization(target_sha=commit)
    with pytest.raises(KeyboardInterrupt):
        interrupted.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    generations = [
        path
        for path in environment_root_for_lock(lock_path).iterdir()
        if not path.name.startswith(".")
    ]
    assert len(generations) == 1
    assert not environment_selector_path_for_lock(lock_path).exists()

    recovered = _authority(repo, lock_path, lock_fd, python)
    marker = recovered.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    recovered.complete_initialization(operation_id=initialization.operation_id)
    recovered.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert recovered.verify(expected_commit=commit) == marker
    os.close(lock_fd)


@pytest.mark.parametrize("cancelled", [False, True])
def test_environment_tree_publication_checks_shared_deadline_and_cancellation(
    tmp_path: Path,
    cancelled: bool,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    cancellation = False
    authority: ReleaseGenerationAuthority

    def build(destination: Path) -> None:
        nonlocal cancellation
        shutil.copytree(repo / ".venv", destination, dirs_exist_ok=True, symlinks=True)
        for index in range(40):
            payload = destination / "lib" / f"payload-{index}.txt"
            payload.write_text(str(index), encoding="utf-8")
            payload.chmod(0o600)
        if cancelled:
            cancellation = True
        else:
            authority.overall_deadline_monotonic = time.monotonic() - 1

    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=build,
        cancellation_check=lambda: cancellation,
        minimum_free_bytes=0,
    )
    initialization = authority.begin_initialization(target_sha=commit)

    with pytest.raises(ReleaseGenerationError, match="cancelled|timed out"):
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    environment_root = environment_root_for_lock(lock_path)
    assert not environment_selector_path_for_lock(lock_path).exists()
    assert not [path for path in environment_root.iterdir() if path.name.endswith(".building")]
    assert not [path for path in environment_root.iterdir() if not path.name.startswith(".")]
    os.close(lock_fd)


def test_real_uv_wait_poll_cancellation_kills_process_group(
    tmp_path: Path,
) -> None:
    repo, lock_path, _commit, python = _generation(tmp_path)
    child_marker = tmp_path / "uv-child-survived"
    uv = tmp_path / "uv"
    uv.write_text(
        f"#!{sys.executable}\n"
        "import subprocess, sys, time\n"
        f"subprocess.Popen([sys.executable, '-c', \"import time; "
        f"time.sleep(0.8); open({str(child_marker)!r}, 'w').write('alive')\"])\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    uv.chmod(0o700)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    started = time.monotonic()
    authority = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock_path,
        lock_fd=lock_fd,
        python_path=python,
        git_path=TRUSTED_GIT,
        writable=True,
        uv_path=uv,
        minimum_free_bytes=0,
        command_timeout_seconds=10,
        cancellation_check=lambda: time.monotonic() - started >= 0.15,
    )
    destination = tmp_path / "destination"
    destination.mkdir()

    with pytest.raises(ReleaseGenerationError, match="cancelled"):
        authority._build_environment(destination, system_python=Path(sys.executable).resolve())

    time.sleep(1)
    assert not child_marker.exists()
    os.close(lock_fd)


def test_large_manifest_serialization_is_cancellable_before_publish(
    tmp_path: Path,
) -> None:
    repo, lock_path, _commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    checks = 0

    def cancelled() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 8

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        cancellation_check=cancelled,
    )
    manifest = {
        "schema_version": 1,
        "entries": [
            {"path": f"lib/payload-{index}.bin", "sha256": "a" * 64} for index in range(20_000)
        ],
    }
    root_fd = os.open(lock_path.parent, os.O_RDONLY)
    try:
        with pytest.raises(ReleaseGenerationError, match="cancelled"):
            _write_private_json(
                root_fd=root_fd,
                root_path=lock_path.parent,
                name="large.manifest.json",
                payload=manifest,
                require_absent=True,
                maximum_bytes=64 * 1024 * 1024,
                checkpoint=authority._checkpoint,
            )
    finally:
        os.close(root_fd)

    assert not (lock_path.parent / "large.manifest.json").exists()
    assert not list(lock_path.parent.glob(".large.manifest.json.*.tmp"))
    os.close(lock_fd)


def test_manifest_publish_checks_cancellation_after_directory_fsync(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _repo, lock_path, _commit, _python = _generation(tmp_path)
    root_fd = os.open(lock_path.parent, os.O_RDONLY)
    root_identity = os.fstat(root_fd)
    cancelled = False
    original_fsync = os.fsync

    def checkpoint() -> None:
        if cancelled:
            raise ReleaseGenerationError("manifest publish cancelled")

    def cancelling_fsync(descriptor: int) -> None:
        nonlocal cancelled
        original_fsync(descriptor)
        observed = os.fstat(descriptor)
        if (observed.st_dev, observed.st_ino) == (
            root_identity.st_dev,
            root_identity.st_ino,
        ):
            cancelled = True

    monkeypatch.setattr(os, "fsync", cancelling_fsync)
    payload = {"schema_version": 1, "entries": [{"path": "payload", "sha256": "a" * 64}]}
    try:
        with pytest.raises(ReleaseGenerationError, match="cancelled"):
            _write_private_json(
                root_fd=root_fd,
                root_path=lock_path.parent,
                name="durable.manifest.json",
                payload=payload,
                require_absent=True,
                maximum_bytes=1024 * 1024,
                checkpoint=checkpoint,
            )
    finally:
        os.close(root_fd)

    published = lock_path.parent / "durable.manifest.json"
    assert json.loads(published.read_text(encoding="utf-8")) == payload
    assert not list(lock_path.parent.glob(".durable.manifest.json.*.tmp"))


def test_selector_publish_inherits_checkpoint_and_recovers_after_boundary_cancellation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    cancelled = False
    selector_boundary_armed = False

    def mutation_hook(stage: str) -> None:
        nonlocal selector_boundary_armed
        if stage == "environment_sealed":
            selector_boundary_armed = True

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=mutation_hook,
        cancellation_check=lambda: cancelled,
    )
    initialization = authority.begin_initialization(target_sha=commit)
    root_identity = lock_path.parent.stat()
    original_fsync = os.fsync

    def cancelling_selector_fsync(descriptor: int) -> None:
        nonlocal cancelled, selector_boundary_armed
        original_fsync(descriptor)
        observed = os.fstat(descriptor)
        if selector_boundary_armed and (observed.st_dev, observed.st_ino) == (
            root_identity.st_dev,
            root_identity.st_ino,
        ):
            selector_boundary_armed = False
            cancelled = True

    monkeypatch.setattr(os, "fsync", cancelling_selector_fsync)

    with pytest.raises(ReleaseGenerationError, match="cancelled"):
        authority._publish_environment(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
            previous_generation_id="",
        )

    selector_path = environment_selector_path_for_lock(lock_path)
    selector = json.loads(selector_path.read_text(encoding="utf-8"))
    manifest = environment_manifest_path_for_lock(lock_path, selector["generation_id"])
    assert manifest.is_file()
    assert not marker_path_for_lock(lock_path).exists()

    cancelled = False
    recovered_selector, recovered_manifest = authority._publish_environment(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
        previous_generation_id="",
    )
    assert recovered_selector.generation_id == selector["generation_id"]
    assert recovered_manifest["generation_id"] == selector["generation_id"]
    os.close(lock_fd)


def test_generation_gc_cancellation_brackets_orphan_manifest_read(
    tmp_path: Path,
) -> None:
    repo, lock_path, _commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    root = environment_root_for_lock(lock_path)
    root.mkdir(mode=0o700)
    orphan = "f" * 64
    candidate = root / orphan
    candidate.mkdir(mode=0o700)
    payload = candidate / "payload"
    payload.write_text("orphan", encoding="utf-8")
    payload.chmod(0o400)
    candidate.chmod(0o500)
    manifest = environment_manifest_path_for_lock(lock_path, orphan)
    manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generation_id": orphan,
                "environment_path": str(candidate),
                "entries": [{"path": "."}],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    manifest.chmod(0o600)
    old = time.time() - 3600
    os.utime(candidate, (old, old))
    cancel = False

    def interrupt_after_manifest_read(stage: str) -> None:
        nonlocal cancel
        if stage == "before_environment_gc_manifest_delete":
            cancel = True

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt_after_manifest_read,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
        cancellation_check=lambda: cancel,
    )

    with pytest.raises(ReleaseGenerationError, match="cancelled"):
        authority.garbage_collect_environments(reason="cancel-orphan-read")

    assert candidate.is_dir()
    assert manifest.is_file()
    os.close(lock_fd)


def test_commit_record_is_the_only_generation_acceptance_boundary(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "before_generation_commit":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = authority.begin_initialization(target_sha=commit)
    authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)

    with pytest.raises(KeyboardInterrupt):
        authority.commit_generation(
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    with pytest.raises(ReleaseGenerationError, match="commit record"):
        authority.verify(expected_commit=commit)
    recovered = _authority(repo, lock_path, lock_fd, python)
    recovered.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    assert recovered.verify(expected_commit=commit).operation_id == initialization.operation_id
    os.close(lock_fd)


def test_crash_after_commit_record_publication_leaves_an_accepted_generation(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "generation_committed":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = authority.begin_initialization(target_sha=commit)
    authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)

    with pytest.raises(KeyboardInterrupt):
        authority.commit_generation(
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    verifier = _authority(repo, lock_path, lock_fd, python)
    assert verifier.verify(expected_commit=commit).operation_id == initialization.operation_id
    os.close(lock_fd)


def test_generation_commit_retry_is_idempotent(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    initialization = authority.begin_initialization(target_sha=commit)
    authority.publish(
        expected_commit=commit,
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    authority.complete_initialization(operation_id=initialization.operation_id)

    first = authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )
    before = commit_path_for_lock(lock_path).read_bytes()
    second = authority.commit_generation(
        operation_id=initialization.operation_id,
        transaction_kind="initialization",
    )

    assert second == first
    assert commit_path_for_lock(lock_path).read_bytes() == before
    os.close(lock_fd)


def test_marker_publication_before_transaction_completion_is_never_accepted(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

    def interrupt(stage: str) -> None:
        if stage == "marker_published":
            raise KeyboardInterrupt

    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    initialization = authority.begin_initialization(target_sha=commit)

    with pytest.raises(KeyboardInterrupt):
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    assert marker_path_for_lock(lock_path).is_file()
    assert not commit_path_for_lock(lock_path).exists()
    with pytest.raises(ReleaseGenerationError, match="transaction.*completed"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_selector_switch_without_marker_never_accepts_the_new_environment(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    original = _publish_initialized(authority, commit=commit)
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    deployment = _advance_deployment_intent(
        authority,
        deployment,
        target_stage="timers_restored",
    )

    def interrupt(stage: str) -> None:
        if stage == "environment_selector_published":
            raise KeyboardInterrupt

    interrupted = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        mutation_hook=interrupt,
    )
    with pytest.raises(KeyboardInterrupt):
        interrupted.publish(
            expected_commit=commit,
            operation_id=deployment.operation_id,
            transaction_kind="deployment",
        )

    selector = json.loads(environment_selector_path_for_lock(lock_path).read_text(encoding="utf-8"))
    assert selector["generation_id"] != original.environment_generation_id
    with pytest.raises(ReleaseGenerationError, match="marker"):
        authority.verify(expected_commit=commit)

    marker = authority.publish(
        expected_commit=commit,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    _advance_deployment_intent(authority, deployment, target_stage="completed")
    authority.commit_generation(
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    assert authority.verify(expected_commit=commit) == marker
    os.close(lock_fd)


def _archive_completed_deployment(
    authority: ReleaseGenerationAuthority,
    lock_path: Path,
    *,
    commit: str,
) -> tuple[str, Path]:
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    _advance_deployment_intent(
        authority,
        deployment,
        target_stage="timers_restored",
    )
    authority.publish(
        expected_commit=commit,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    _advance_deployment_intent(authority, deployment, target_stage="completed")
    authority.commit_generation(
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    active = intent_path_for_lock(lock_path)
    archive = active.with_name(f"{active.stem}.{deployment.operation_id}.completed.json")
    active.replace(archive)
    return deployment.operation_id, archive


def test_completed_intent_archive_is_used_only_when_active_intent_is_absent(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    _operation_id, archive = _archive_completed_deployment(
        authority,
        lock_path,
        commit=commit,
    )

    assert archive.is_file()
    assert authority.verify(expected_commit=commit).commit == commit
    os.close(lock_fd)


@pytest.mark.parametrize("active_kind", ["corrupt", "loose", "symlink"])
def test_unsafe_active_intent_never_falls_back_to_valid_archive(
    tmp_path: Path,
    active_kind: str,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    _operation_id, archive = _archive_completed_deployment(
        authority,
        lock_path,
        commit=commit,
    )
    active = intent_path_for_lock(lock_path)
    if active_kind == "corrupt":
        active.write_text("{not-json\n", encoding="utf-8")
        active.chmod(0o600)
    elif active_kind == "loose":
        active.write_bytes(archive.read_bytes())
        active.chmod(0o644)
    else:
        active.symlink_to(archive)

    with pytest.raises(ReleaseGenerationError, match="deployment record"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


@pytest.mark.parametrize("active_kind", ["corrupt", "loose", "valid"])
def test_initialization_generation_is_blocked_by_any_active_deployment_intent(
    tmp_path: Path,
    active_kind: str,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    _publish_initialized(authority, commit=commit)
    active = intent_path_for_lock(lock_path)
    if active_kind == "valid":
        authority.begin_deployment_intent(
            previous_sha=commit,
            target_sha="e" * 40,
            target_ref="e" * 40,
            changed_files=(),
            restart_services=(),
            active_services=(),
            active_timers=(),
        )
    else:
        active.write_text("{not-json\n" if active_kind == "corrupt" else "{}\n")
        active.chmod(0o600 if active_kind == "corrupt" else 0o644)

    with pytest.raises(ReleaseGenerationError, match="deployment|record"):
        authority.verify(expected_commit=commit)
    os.close(lock_fd)


def test_generation_gc_retains_authority_references_and_removes_only_old_orphans(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    marker = _publish_initialized(authority, commit=commit)
    intent = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha="e" * 40,
        target_ref="e" * 40,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
        handoff_operation_id="8" * 32,
        handoff_labels=("scheduler", "worker", "finalizer"),
    )
    partial = _partial_handoff_record(
        operation_id=intent.handoff_operation_id,
        action="deploy",
        target_sha=intent.target_sha,
        supersedes_operation_id="",
        installation=LabInstallationIdentity(
            path=str(lock_path.with_name("fixture-install.json")),
            sha256="9" * 64,
            device=1,
            inode=2,
        ),
        stage="planned",
    )
    partial_path = lock_path.with_name(f"{lock_path.stem}.lab-handoff.{partial.operation_id}.json")
    partial_payload = asdict(partial)
    for completion_field in (
        "generation_operation_id",
        "environment_generation_id",
        "code_sha",
    ):
        partial_payload.pop(completion_field)
    partial_path.write_text(
        json.dumps(partial_payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    partial_path.chmod(0o600)
    referenced = {
        hashlib.sha256(f"{intent.operation_id}:{sha}".encode()).hexdigest()
        for sha in (intent.previous_sha, intent.target_sha)
    }
    root = environment_root_for_lock(lock_path)
    previous = marker.environment_generation_id
    orphan = "c" * 64
    failed = f".{('d' * 64)}.0123456789abcdef.building"
    for name in (orphan, failed, *referenced):
        candidate = root / name
        candidate.mkdir(mode=0o700)
        payload = candidate / "payload"
        payload.write_text(name, encoding="utf-8")
        payload.chmod(0o400)
        candidate.chmod(0o500)
    manifests: dict[str, Path] = {}
    for generation_id in (orphan,):
        manifest = lock_path.with_name(f"{lock_path.stem}.venv-{generation_id}.manifest.json")
        manifest.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "generation_id": generation_id,
                    "environment_path": str(root / generation_id),
                    "entries": [{"path": "."}],
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        manifest.chmod(0o600)
        manifests[generation_id] = manifest
    now = time.time()
    os.utime(manifests[orphan], (now + 1_000, now + 1_000), follow_symlinks=False)
    os.utime(root / orphan, (now - 200, now - 200), follow_symlinks=False)
    os.utime(root / failed, (now - 300, now - 300), follow_symlinks=False)
    for generation_id in referenced:
        os.utime(root / generation_id, (now - 400, now - 400), follow_symlinks=False)

    metrics = authority.garbage_collect_environments(reason="unit-test")

    assert marker.environment_generation_id in metrics.retained_generation_ids
    assert previous in metrics.retained_generation_ids
    assert referenced <= set(metrics.retained_generation_ids)
    assert not (root / orphan).exists()
    assert not manifests[orphan].exists()
    assert not (root / failed).exists()
    assert metrics.deleted_generations == 2
    audit = lock_path.with_name(f"{lock_path.stem}.generation-gc.jsonl")
    assert audit.stat().st_mode & 0o777 == 0o600
    assert '"reason":"unit-test"' in audit.read_text(encoding="utf-8")
    os.close(lock_fd)


def test_generation_gc_retains_generation_referenced_only_by_completed_intent_archive(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    _publish_initialized(authority, commit=commit)
    deployment = authority.begin_deployment_intent(
        previous_sha=commit,
        target_sha=commit,
        target_ref=commit,
        changed_files=(),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    _advance_deployment_intent(authority, deployment, target_stage="completed")
    active = intent_path_for_lock(lock_path)
    archive = active.with_name(f"{active.stem}.{deployment.operation_id}.completed.json")
    active.replace(archive)
    operation_id = deployment.operation_id
    archived_generation = hashlib.sha256(f"{operation_id}:{commit}".encode()).hexdigest()
    candidate = environment_root_for_lock(lock_path) / archived_generation
    candidate.mkdir(mode=0o700)
    payload = candidate / "payload"
    payload.write_text("archive-only", encoding="utf-8")
    payload.chmod(0o400)
    candidate.chmod(0o500)
    os.utime(candidate, (time.time() - 200, time.time() - 200), follow_symlinks=False)

    metrics = authority.garbage_collect_environments(reason="completed-archive")

    assert archive.is_file()
    assert archived_generation in metrics.retained_generation_ids
    assert candidate.is_dir()
    os.close(lock_fd)


@pytest.mark.parametrize("archive_kind", ["corrupt", "foreign-name", "symlink"])
def test_generation_gc_fails_closed_on_unsafe_completed_intent_archive(
    tmp_path: Path,
    archive_kind: str,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    _publish_initialized(authority, commit=commit)
    operation_id, archive = _archive_completed_deployment(
        authority,
        lock_path,
        commit=commit,
    )
    if archive_kind == "corrupt":
        archive.write_text("{not-json\n", encoding="utf-8")
    elif archive_kind == "foreign-name":
        archive.rename(
            archive.with_name(f"{intent_path_for_lock(lock_path).stem}.foreign.completed.json")
        )
    else:
        archived = archive.read_bytes()
        archive.unlink()
        external = tmp_path / "external-intent.json"
        external.write_bytes(archived)
        external.chmod(0o600)
        archive.symlink_to(external)

    orphan = environment_root_for_lock(lock_path) / ("9" * 64)
    orphan.mkdir(mode=0o700)
    payload = orphan / "payload"
    payload.write_text(operation_id, encoding="utf-8")
    payload.chmod(0o400)
    orphan.chmod(0o500)

    with pytest.raises(ReleaseGenerationError, match="intent|archive|unsafe|JSON"):
        authority.garbage_collect_environments(reason="unsafe-archive")

    assert orphan.is_dir()
    os.close(lock_fd)


@pytest.mark.parametrize("authority_kind", ["install-transaction", "corrupt-install"])
def test_generation_gc_fails_closed_on_unresolved_installation_authority(
    tmp_path: Path,
    authority_kind: str,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    _publish_initialized(authority, commit=commit)
    orphan = environment_root_for_lock(lock_path) / ("9" * 64)
    orphan.mkdir(mode=0o700)
    (orphan / "payload").write_text("retain", encoding="utf-8")
    orphan.chmod(0o500)
    if authority_kind == "install-transaction":
        control = lock_path.with_name(f"{lock_path.stem}.lab-install-transaction.json")
        control.write_text("{}\n", encoding="utf-8")
    else:
        control = lock_path.with_name(f"{lock_path.stem}.lab-install.json")
        control.write_text('{"schema_version":2}\n', encoding="utf-8")
    control.chmod(0o600)

    with pytest.raises(ReleaseGenerationError, match="installation|registered"):
        authority.garbage_collect_environments(reason="blocked-authority")

    assert orphan.exists()
    os.close(lock_fd)


def test_generation_gc_retains_fully_bound_installed_generation(tmp_path: Path) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    _publish_initialized(authority, commit=commit)
    installed_generation = "9" * 64
    candidate = environment_root_for_lock(lock_path) / installed_generation
    candidate.mkdir(mode=0o700)
    (candidate / "payload").write_text("installed", encoding="utf-8")
    candidate.chmod(0o500)
    os.utime(candidate, (time.time() - 300, time.time() - 300), follow_symlinks=False)
    _write_gc_lab_installation(
        tmp_path,
        repo,
        lock_path,
        code_sha=commit,
        generation_id=installed_generation,
    )

    metrics = authority.garbage_collect_environments(reason="installed-generation")

    assert candidate.is_dir()
    assert installed_generation in metrics.retained_generation_ids
    os.close(lock_fd)


@pytest.mark.parametrize(
    ("corruption", "expected"),
    [
        ("missing-field", "registered Lab installation authority"),
        ("wrong-type", "registered Lab installation authority"),
        ("plist-binding", "plist.*binding|installation.*binding"),
        ("local-divergence", "authorit.*diverged|installation.*diverged"),
    ],
)
def test_generation_gc_rejects_incomplete_or_divergent_typed_installation_authority(
    tmp_path: Path,
    corruption: str,
    expected: str,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    _publish_initialized(authority, commit=commit)
    generation_id = "9" * 64
    local_path, registered_path, local, registered = _write_gc_lab_installation(
        tmp_path,
        repo,
        lock_path,
        code_sha=commit,
        generation_id=generation_id,
    )
    if corruption == "missing-field":
        registered.pop("prepared_authority")
        target_path, target = registered_path, registered
    elif corruption == "wrong-type":
        registered["registered_by_commit"] = 42
        target_path, target = registered_path, registered
    elif corruption == "plist-binding":
        label = LAB_LAUNCHD_HANDOFF_LABELS[0]
        registered["plists"][label]["sha256"] = "0" * 64
        target_path, target = registered_path, registered
    else:
        local["code_sha"] = "0" * 40
        target_path, target = local_path, local
    target_path.write_bytes(canonical_json_bytes(target, trailing_newline=True))
    target_path.chmod(0o600)

    with pytest.raises(ReleaseGenerationError, match=expected):
        authority.garbage_collect_environments(reason=f"invalid-{corruption}")

    os.close(lock_fd)


def test_generation_gc_uses_exact_previous_id_not_newer_orphan_mtime(tmp_path: Path) -> None:
    repo, lock_path, first_commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    first = _publish_initialized(authority, commit=first_commit)
    (repo / "README.md").write_text("next generation\n", encoding="utf-8")
    subprocess.run([str(TRUSTED_GIT), "add", "README.md"], cwd=repo, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "next generation",
        ],
        cwd=repo,
        check=True,
    )
    second_commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [str(TRUSTED_GIT), "reset", "--hard", first_commit],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    intent = authority.begin_deployment_intent(
        previous_sha=first_commit,
        target_sha=second_commit,
        target_ref=second_commit,
        changed_files=("README.md",),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    subprocess.run(
        [str(TRUSTED_GIT), "reset", "--hard", second_commit],
        cwd=repo,
        check=True,
        capture_output=True,
    )
    _advance_deployment_intent(authority, intent, target_stage="timers_restored")
    second = authority.publish(
        expected_commit=second_commit,
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    _advance_deployment_intent(authority, intent, target_stage="completed")
    authority.commit_generation(
        operation_id=intent.operation_id,
        transaction_kind="deployment",
    )
    assert second.previous_generation_id == first.environment_generation_id

    root = environment_root_for_lock(lock_path)
    orphan = "f" * 64
    candidate = root / orphan
    candidate.mkdir(mode=0o700)
    payload = candidate / "payload"
    payload.write_text("orphan", encoding="utf-8")
    payload.chmod(0o400)
    candidate.chmod(0o500)
    orphan_manifest = lock_path.with_name(f"{lock_path.stem}.venv-{orphan}.manifest.json")
    orphan_manifest.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "generation_id": orphan,
                "environment_path": str(candidate),
                "entries": [{"path": "."}],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    orphan_manifest.chmod(0o600)
    now = time.time()
    os.utime(orphan_manifest, (now + 10_000, now + 10_000), follow_symlinks=False)
    os.utime(candidate, (now - 100, now - 100), follow_symlinks=False)

    metrics = authority.garbage_collect_environments(reason="clock-skew-test")

    assert first.environment_generation_id in metrics.retained_generation_ids
    assert second.environment_generation_id in metrics.retained_generation_ids
    assert not candidate.exists()
    assert not orphan_manifest.exists()
    os.close(lock_fd)


def test_generation_publish_fails_before_copy_when_disk_budget_is_insufficient(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=1,
    )
    monkeypatch.setattr(
        "rquant.release_generation.shutil.disk_usage",
        lambda _path: shutil._ntuple_diskusage(total=100, used=100, free=0),
    )
    initialization = authority.begin_initialization(target_sha=commit)

    with pytest.raises(ReleaseGenerationError, match="disk budget"):
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )

    root = environment_root_for_lock(lock_path)
    assert not any(path.name.endswith(".building") for path in root.iterdir())
    os.close(lock_fd)


def test_generation_gc_rejects_symlink_candidate_without_touching_external_data(
    tmp_path: Path,
) -> None:
    repo, lock_path, commit, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(
        repo,
        lock_path,
        lock_fd,
        python,
        gc_grace_seconds=0,
        minimum_free_bytes=0,
    )
    _publish_initialized(authority, commit=commit)
    external = tmp_path / "external"
    external.mkdir()
    payload = external / "keep.txt"
    payload.write_text("keep", encoding="utf-8")
    candidate = environment_root_for_lock(lock_path) / ("f" * 64)
    candidate.symlink_to(external, target_is_directory=True)

    with pytest.raises(ReleaseGenerationError, match="generation is unsafe"):
        authority.garbage_collect_environments(reason="symlink-test")

    assert payload.read_text(encoding="utf-8") == "keep"
    assert candidate.is_symlink()
    os.close(lock_fd)


def test_writable_generation_authority_requires_exclusive_lock(tmp_path: Path) -> None:
    repo, lock_path, _commit, python = _generation(tmp_path)
    first = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    second = os.open(lock_path, os.O_RDWR)
    fcntl.flock(first, fcntl.LOCK_SH | fcntl.LOCK_NB)
    fcntl.flock(second, fcntl.LOCK_SH | fcntl.LOCK_NB)

    with pytest.raises(ReleaseGenerationError, match="exclusive"):
        _authority(repo, lock_path, first, python)

    os.close(second)
    os.close(first)


def test_rollback_selects_a_verified_immutable_previous_environment(
    tmp_path: Path,
) -> None:
    repo, lock_path, previous, python = _generation(tmp_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    authority = _authority(repo, lock_path, lock_fd, python)
    original = _publish_initialized(authority, commit=previous)

    (repo / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.1"\n',
        encoding="utf-8",
    )
    (repo / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    subprocess.run([str(TRUSTED_GIT), "add", "pyproject.toml", "uv.lock"], cwd=repo, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "target generation",
        ],
        cwd=repo,
        check=True,
    )
    target = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [str(TRUSTED_GIT), "reset", "--hard", previous],
        cwd=repo,
        check=True,
        capture_output=True,
    )

    deployment = authority.begin_deployment_intent(
        previous_sha=previous,
        target_sha=target,
        target_ref=target,
        changed_files=("pyproject.toml", "uv.lock"),
        restart_services=(),
        active_services=(),
        active_timers=(),
    )
    authority.invalidate()
    authority.update_deployment_intent(
        operation_id=deployment.operation_id,
        stage="recovery_started",
    )
    _advance_deployment_intent(
        authority,
        deployment,
        target_stage="timers_restored",
        action="rollback",
    )
    marker = authority.publish(
        expected_commit=previous,
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )
    _advance_deployment_intent(
        authority,
        deployment,
        target_stage="completed",
        action="rollback",
    )
    authority.commit_generation(
        operation_id=deployment.operation_id,
        transaction_kind="deployment",
    )

    selector_payload = json.loads(
        environment_selector_path_for_lock(lock_path).read_text(encoding="utf-8")
    )
    commit_payload = json.loads(commit_path_for_lock(lock_path).read_text(encoding="utf-8"))

    assert marker.commit == previous
    assert marker.environment_generation_id != original.environment_generation_id
    assert marker.previous_generation_id == original.environment_generation_id
    assert selector_payload["previous_generation_id"] == original.environment_generation_id
    assert commit_payload["previous_generation_id"] == original.environment_generation_id
    assert authority.verify(expected_commit=previous) == marker
    os.close(lock_fd)
