from __future__ import annotations

import fcntl
import hashlib
import importlib.util
import json
import os
import plistlib
import shlex
import shutil
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Iterator
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo

import pytest

from rquant.release_generation import (
    DeploymentIntent,
    EnvironmentSelector,
    PathIdentity,
    ReleaseGenerationAuthority,
    ReleaseGenerationCommit,
    ReleaseGenerationMarker,
    commit_path_for_lock,
    environment_selector_path_for_lock,
    intent_path_for_lock,
    marker_path_for_lock,
)

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = ROOT / "scripts" / "bootstrap-production-deploy.py"
AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"
STRICT_JSON = ROOT / "scripts" / "strict_json.py"
PRODUCTION_DEPLOYER = ROOT / "src" / "rquant" / "ops" / "production_deploy.py"
TRUSTED_GIT = Path("/usr/bin/git")
_ORIGINAL_OS_WALK = os.walk
_READINESS_A = ("a" * 32, "b" * 64, "a" * 40)


def _handoff_deployment_intent(
    module: ModuleType,
    *,
    handoff_operation_id: str,
    operation_id: str,
    previous_sha: str,
    target_sha: str,
    target_ref: str,
    stage: str,
) -> DeploymentIntent:
    intent = DeploymentIntent.create(
        previous_sha=previous_sha,
        target_sha=target_sha,
        target_ref=target_ref,
        changed_files=("src/rquant/preflight.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="7" * 64,
        previous_generation_id="8" * 64,
        handoff_operation_id=handoff_operation_id,
        handoff_labels=tuple(module.LAB_LAUNCHD_LABELS),
        operation_id=operation_id,
    )
    if stage == "planned":
        return intent
    stages = (
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
        "completed",
    )
    if stage not in stages:
        raise AssertionError(f"unsupported fixture deployment stage: {stage}")
    for next_stage in stages:
        intent = intent.advance(stage=next_stage)
        if next_stage == stage:
            return intent
    raise AssertionError("unreachable")


def _publish_handoff_generation_authority(
    module: ModuleType,
    lock_path: Path,
    *,
    handoff_operation_id: str,
    generation: tuple[str, str, str] = _READINESS_A,
    target_ref: str | None = None,
    action: str = "deploy",
) -> DeploymentIntent:
    generation_operation_id, environment_generation_id, code_sha = generation
    previous_sha = "9" * 40
    previous_generation_id = "8" * 64
    selected_target = code_sha if action != "rollback" else previous_sha
    intent = _handoff_deployment_intent(
        module,
        handoff_operation_id=handoff_operation_id,
        operation_id=generation_operation_id,
        previous_sha=previous_sha,
        target_sha=code_sha,
        target_ref=target_ref or code_sha,
        stage="completed",
    )
    identity = PathIdentity(device=1, inode=2, mode=0o40700, owner=os.getuid(), links=1)
    marker = ReleaseGenerationMarker(
        schema_version=1,
        operation_id=generation_operation_id,
        transaction_kind="deployment",
        commit=selected_target,
        uv_lock_sha256="1" * 64,
        pyproject_sha256="2" * 64,
        package_version="0.99.0",
        python_version="3.12.0",
        python_abi="cpython-test",
        venv_path="/private/tmp/rquant-test-venv",
        venv_identity=identity,
        pyvenv_cfg_sha256="3" * 64,
        python_path="/private/tmp/rquant-test-venv/bin/python",
        python_identity=identity,
        site_packages_path="/private/tmp/rquant-test-venv/lib/python3.12/site-packages",
        site_packages_identity=identity,
        environment_generation_id=environment_generation_id,
        previous_generation_id=previous_generation_id,
        environment_manifest_sha256="4" * 64,
        published_at="2026-07-28T00:00:00+00:00",
    )
    selector = EnvironmentSelector(
        schema_version=1,
        operation_id=generation_operation_id,
        transaction_kind="deployment",
        commit=selected_target,
        generation_id=environment_generation_id,
        previous_generation_id=previous_generation_id,
        environment_path=marker.venv_path,
        manifest_name=f"rquant.lock.venv-{environment_generation_id}.manifest.json",
        manifest_sha256=marker.environment_manifest_sha256,
        published_at=marker.published_at,
    )
    committed = ReleaseGenerationCommit(
        schema_version=1,
        operation_id=generation_operation_id,
        transaction_kind="deployment",
        commit=selected_target,
        marker_sha256=marker.content_hash(),
        transaction_sha256=intent.content_hash(),
        environment_generation_id=environment_generation_id,
        previous_generation_id=previous_generation_id,
        environment_manifest_sha256=marker.environment_manifest_sha256,
        committed_at="2026-07-28T00:00:01+00:00",
    )
    module._atomic_private_json(marker_path_for_lock(lock_path), asdict(marker))
    module._atomic_private_json(environment_selector_path_for_lock(lock_path), asdict(selector))
    module._atomic_private_json(intent_path_for_lock(lock_path), asdict(intent))
    module._atomic_private_json(commit_path_for_lock(lock_path), asdict(committed))
    return intent


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


def _bootstrap_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("_test_production_deploy_bootstrap", BOOTSTRAP)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _tiny_test_venv(checkout: Path) -> Path:
    venv_root = checkout / ".venv"
    python = venv_root / "bin" / "python"
    python.parent.mkdir(parents=True)
    system_python = checkout / ".test-system-python"
    _write_test_interpreter(system_python, system_python)
    _write_test_interpreter(python, system_python)
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    (venv_root / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {version}\n",
        encoding="utf-8",
    )
    (venv_root / "lib" / f"python{version}" / "site-packages").mkdir(parents=True)
    return python


def _write_test_interpreter(path: Path, system_python: Path) -> None:
    path.write_text(
        "#!/bin/sh\n"
        "if [ \"${1:-}\" = '-I' ] && [ \"${2:-}\" = '-S' ] && "
        "[ \"${3:-}\" = '-c' ] && "
        "[ \"${4:-}\" = 'import sys; print(sys._base_executable)' ]; then\n"
        f"    printf '%s\\n' {shlex.quote(str(system_python))}\n"
        "    exit 0\n"
        "fi\n"
        f'exec {shlex.quote(sys.executable)} "$@"\n',
        encoding="utf-8",
    )
    path.chmod(0o700)


def _git(checkout: Path, *arguments: str) -> str:
    return subprocess.run(
        [str(TRUSTED_GIT), *arguments],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _tree_snapshot(root: Path) -> tuple[tuple[object, ...], ...]:
    if not root.exists():
        return ()
    snapshot: list[tuple[object, ...]] = []
    for candidate in sorted(root.rglob("*")):
        observed = candidate.lstat()
        payload = candidate.read_bytes() if candidate.is_file() else b""
        snapshot.append(
            (
                str(candidate.relative_to(root)),
                observed.st_dev,
                observed.st_ino,
                observed.st_mode,
                observed.st_mtime_ns,
                observed.st_size,
                payload,
            )
        )
    return tuple(snapshot)


def _checkout(
    tmp_path: Path,
    *,
    publish_marker: bool = True,
    real_deployer: bool = False,
    install_lab: bool = True,
    install_state: bool | None = None,
) -> tuple[Path, Path, Path, str]:
    checkout = tmp_path / "rquant"
    package = checkout / "src" / "rquant"
    ops = package / "ops"
    scripts = checkout / "scripts"
    ops.mkdir(parents=True)
    scripts.mkdir()
    shutil.copy2(BOOTSTRAP, scripts / BOOTSTRAP.name)
    shutil.copy2(STRICT_JSON, scripts / STRICT_JSON.name)
    shutil.copy2(AUTHORITY, package / AUTHORITY.name)
    shutil.copy2(
        ROOT / "src" / "rquant" / "contained_subprocess.py",
        package / "contained_subprocess.py",
    )
    shutil.copy2(ROOT / "src" / "rquant" / "strict_json.py", package / "strict_json.py")
    shutil.copy2(ROOT / "src" / "rquant" / "private_fs.py", package / "private_fs.py")
    shutil.copy2(
        ROOT / "src" / "rquant" / "lab_launchd_install.py",
        package / "lab_launchd_install.py",
    )
    shutil.copy2(
        ROOT / "src" / "rquant" / "research_manifest.py",
        package / "research_manifest.py",
    )
    (package / "__init__.py").write_text("", encoding="utf-8")
    (ops / "__init__.py").write_text("", encoding="utf-8")
    if real_deployer:
        shutil.copy2(PRODUCTION_DEPLOYER, ops / PRODUCTION_DEPLOYER.name)
    else:
        (ops / "production_deploy.py").write_text(
            "from __future__ import annotations\n"
            "import fcntl, os, time\n"
            "from pathlib import Path\n"
            "if os.environ.get('DEPLOY_LOCK'):\n"
            "    lock_fd = os.open(os.environ['DEPLOY_LOCK'], os.O_RDONLY)\n"
            "    try:\n"
            "        fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)\n"
            "    except BlockingIOError:\n"
            "        state = 'locked'\n"
            "    else:\n"
            "        state = 'unlocked'\n"
            "    finally:\n"
            "        os.close(lock_fd)\n"
            "    if os.environ.get('IMPORT_MARKER'):\n"
            "        Path(os.environ['IMPORT_MARKER']).write_text(state, encoding='utf-8')\n"
            "def main(argv=None):\n"
            "    if os.environ.get('RUN_MARKER'):\n"
            "        Path(os.environ['RUN_MARKER']).write_text('ran', encoding='utf-8')\n"
            "    time.sleep(float(os.environ.get('DEPLOY_HOLD_SECONDS', '0')))\n"
            "    return int(os.environ.get('DEPLOY_EXIT', '0'))\n",
            encoding="utf-8",
        )
    (checkout / ".gitignore").write_text("/.venv\n", encoding="utf-8")
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.0"\n',
        encoding="utf-8",
    )
    (checkout / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    if install_lab:
        launchd = checkout / "deploy" / "launchd"
        launchd.mkdir(parents=True)
        for label in _bootstrap_module().LAB_LAUNCHD_LABELS:
            plist = launchd / f"{label}.plist"
            shutil.copy2(ROOT / "deploy" / "launchd" / plist.name, plist)
            plist.chmod(0o600)
    python = _tiny_test_venv(checkout)
    rquant = checkout / ".venv" / "bin" / "rquant"
    rquant.write_text(
        "#!/bin/sh\n"
        'if [ "$#" -ne 1 ] || [ "$1" != \'preflight\' ]; then\n'
        "    exit 64\n"
        "fi\n"
        'exit "${PREFLIGHT_EXIT:-0}"\n',
        encoding="utf-8",
    )
    rquant.chmod(0o700)
    console_script = checkout / ".venv" / "bin" / "rquant-test-console"
    console_script.write_text(
        f"#!{python}\nraise SystemExit(0)\n",
        encoding="utf-8",
    )
    console_script.chmod(0o700)
    uv = checkout / ".venv" / "bin" / "uv"
    uv.write_text(
        "#!/bin/sh\n"
        'for argument in "$@"; do\n'
        '    target="${argument}"\n'
        "done\n"
        "if [ \"${1:-}\" = 'venv' ]; then\n"
        '    mkdir -p "${target}"\n'
        '    /bin/cp -R "$(dirname "$0")/.."/. "${target}"/\n'
        "elif [ \"${1:-}\" = 'sync' ] && [ \"${2:-}\" = '--frozen' ]; then\n"
        '    if [ -n "${UV_PROJECT_ENVIRONMENT:-}" ] && [ ! -e '
        '"${UV_PROJECT_ENVIRONMENT}/pyvenv.cfg" ]; then\n'
        '        mkdir -p "${UV_PROJECT_ENVIRONMENT}"\n'
        '        /bin/cp -R "$(dirname "$0")/.."/. "${UV_PROJECT_ENVIRONMENT}"/\n'
        "    fi\n"
        "else\n"
        "    exit 64\n"
        "fi\n"
        'exit "${UV_SYNC_EXIT:-0}"\n',
        encoding="utf-8",
    )
    uv.chmod(0o700)
    subprocess.run([str(TRUSTED_GIT), "init", "-q", "-b", "main"], cwd=checkout, check=True)
    subprocess.run([str(TRUSTED_GIT), "add", "."], cwd=checkout, check=True)
    subprocess.run(
        [
            str(TRUSTED_GIT),
            "-c",
            "user.name=rQuant Tests",
            "-c",
            "user.email=tests@rquant.invalid",
            "commit",
            "-qm",
            "bootstrap generation",
        ],
        cwd=checkout,
        check=True,
    )
    commit = subprocess.run(
        [str(TRUSTED_GIT), "rev-parse", "HEAD"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    remote = tmp_path / "origin.git"
    subprocess.run([str(TRUSTED_GIT), "init", "-q", "--bare", str(remote)], check=True)
    _git(checkout, "remote", "add", "origin", str(remote))
    _git(checkout, "push", "-q", "-u", "origin", "main")
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    lock_path = lock_root / "rquant.lock"
    if install_state is None:
        install_state = install_lab
    if install_state:
        _install_lab_handoff(_bootstrap_module(), checkout, lock_path)
    if publish_marker:
        lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            authority = ReleaseGenerationAuthority(
                repo=checkout,
                lock_path=lock_path,
                lock_fd=lock_fd,
                python_path=python,
                git_path=TRUSTED_GIT,
                writable=True,
                environment_builder=lambda destination: shutil.copytree(
                    checkout / ".venv",
                    destination,
                    dirs_exist_ok=True,
                    symlinks=True,
                ),
            )
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
        finally:
            os.close(lock_fd)
    if publish_marker and install_state:
        from rquant.lab_launchd_install import LabLaunchdInstaller

        launch_agents_dir = tmp_path / "LaunchAgents"
        launch_agents_dir.mkdir(mode=0o700)
        LabLaunchdInstaller(
            checkout_root=checkout,
            deployment_lock_path=lock_path,
            launch_agents_dir=launch_agents_dir,
            trusted_git_path=TRUSTED_GIT,
        ).install(activate=False)
    return checkout, python, lock_path, commit


def _commit_next_release(checkout: Path) -> str:
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "0.99.1"\n',
        encoding="utf-8",
    )
    (checkout / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    _git(checkout, "add", "pyproject.toml", "uv.lock")
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "next generation",
    )
    commit = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "push", "-q", "origin", "main")
    return commit


def _begin_intent(
    checkout: Path,
    python: Path,
    lock_path: Path,
    *,
    previous: str,
    target: str,
    target_ref: str | None = None,
    handoff_operation_id: str = "",
    handoff_labels: tuple[str, ...] = (),
) -> str:
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        authority = ReleaseGenerationAuthority(
            repo=checkout,
            lock_path=lock_path,
            lock_fd=lock_fd,
            python_path=python,
            git_path=TRUSTED_GIT,
            writable=True,
            environment_builder=lambda destination: shutil.copytree(
                checkout / ".venv",
                destination,
                dirs_exist_ok=True,
                symlinks=True,
            ),
        )
        intent = authority.begin_deployment_intent(
            previous_sha=previous,
            target_sha=target,
            target_ref=target_ref or target,
            changed_files=("src/rquant/preflight.py",),
            restart_services=(),
            active_services=(),
            active_timers=(),
            handoff_operation_id=handoff_operation_id,
            handoff_labels=handoff_labels,
        )
        return intent.operation_id
    finally:
        os.close(lock_fd)


def _advance_generation_intent(
    authority: ReleaseGenerationAuthority,
    *,
    operation_id: str,
    target_stage: str,
) -> None:
    stages = (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
        "services_ready",
        "post_restart_preflight_ready",
        "timers_restored",
    )
    if target_stage not in stages:
        raise AssertionError("unsupported generation fixture stage")
    for stage in stages:
        authority.update_deployment_intent(operation_id=operation_id, stage=stage)
        if stage == target_stage:
            return


def _command(
    checkout: Path,
    python: Path,
    lock_path: Path,
    *,
    target: str | None = None,
    mode: str = "deploy",
    recovery_action: str | None = None,
    operation_id: str | None = None,
    inherited_lock_fd: int | None = None,
    inherited_handoff_lock_fd: int | None = None,
    finalize_phase: str = "publish",
    lifecycle_mode: str = "uninstalled",
) -> list[str]:
    try:
        release_profile, host_platform = {
            "darwin": ("macos-lab", "darwin"),
            "linux": ("linux-production", "linux"),
        }[sys.platform]
    except KeyError as exc:
        raise AssertionError(f"unsupported test host platform: {sys.platform}") from exc
    command = [
        str(python),
        "-I",
        "-S",
        str(checkout / "scripts" / BOOTSTRAP.name),
        "--expected-checkout-root",
        str(checkout),
        "--trusted-git-path",
        str(TRUSTED_GIT),
        "--deployment-lock-path",
        str(lock_path),
        "--python-path",
        str(python),
        "--uv-path",
        str(checkout / ".venv" / "bin" / "uv"),
        "--release-profile",
        release_profile,
        "--host-platform",
        host_platform,
        "--lab-lifecycle-mode",
        lifecycle_mode,
    ]
    if mode == "initialize":
        command.append("--initialize-generation")
    elif mode == "register":
        command.append("--register-lab-installation")
    elif mode == "recover":
        command.append("--recover-generation")
        command.extend(["--recovery-action", str(recovery_action)])
    elif mode == "finalize":
        command.append("--finalize-generation")
        command.extend(
            [
                "--finalize-action",
                str(recovery_action),
                "--finalize-phase",
                finalize_phase,
                "--operation-id",
                str(operation_id),
                "--inherited-lock-fd",
                str(inherited_lock_fd),
            ]
        )
        if inherited_handoff_lock_fd is not None:
            command.extend(["--inherited-handoff-lock-fd", str(inherited_handoff_lock_fd)])
    command.extend(["--", "--target", target or _git(checkout, "rev-parse", "HEAD")])
    return command


def _handoff_fixture(tmp_path: Path) -> tuple[ModuleType, Path, Path]:
    module = _bootstrap_module()
    root = tmp_path / "rquant"
    authority = root / "src" / "rquant" / "release_generation.py"
    authority.parent.mkdir(parents=True)
    shutil.copy2(AUTHORITY, authority)
    shutil.copy2(
        ROOT / "src" / "rquant" / "contained_subprocess.py",
        authority.parent / "contained_subprocess.py",
    )
    shutil.copy2(ROOT / "src" / "rquant" / "strict_json.py", authority.parent)
    scripts = root / "scripts"
    scripts.mkdir()
    shutil.copy2(STRICT_JSON, scripts / STRICT_JSON.name)
    launchd = root / "deploy" / "launchd"
    launchd.mkdir(parents=True)
    for label in module.LAB_LAUNCHD_LABELS:
        path = launchd / f"{label}.plist"
        path.write_text("<?xml version='1.0'?><plist version='1.0'><dict/></plist>\n")
        path.chmod(0o600)
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    return module, root, lock_root / "rquant.lock"


def _install_lab_handoff(module: ModuleType, root: Path, lock_path: Path) -> None:
    from rquant.lab_daemon import (
        prepare_lab_runtime_layout,
        prepare_private_sqlite_path,
        register_lab_runtime_managed_file,
    )

    runtime_root = root / "data" / "lab-runtime"
    directories = {
        "lab command spool": runtime_root / "commands",
        "lab claim spool": runtime_root / "claims",
        "lab report spool": runtime_root / "reports",
        "lab worker artifact root": runtime_root / "worker-artifacts",
        "lab final artifact root": runtime_root / "final-artifacts",
        "lab artifact commit spool": runtime_root / "artifact-commits",
        "lab daemon lock root": runtime_root / "locks",
        "lab finalizer state root": runtime_root / "finalizer-state",
        "lab readiness root": runtime_root / "readiness",
    }
    files = {"lab jobs SQLite": runtime_root / "lab_jobs.sqlite3"}
    prepare_lab_runtime_layout(
        runtime_root,
        checkout_root=root,
        managed_directories=directories,
        managed_files=files,
        legacy_paths={},
        mutation_guard=lambda: "a" * 40,
    )
    authority = prepare_private_sqlite_path(
        files["lab jobs SQLite"],
        label="lab jobs SQLite",
        create=True,
        mutation_guard=lambda: "a" * 40,
    )
    try:
        register_lab_runtime_managed_file(
            runtime_root,
            label="lab jobs SQLite",
            path=files["lab jobs SQLite"],
            mutation_guard=lambda: "a" * 40,
        )
    finally:
        authority.close()
    module._write_lab_installation_state(
        root=root,
        lock_path=lock_path,
        runtime_root=runtime_root,
        readiness_root=runtime_root / "readiness",
        expected_commit="a" * 40,
    )


def test_lab_handoff_dry_run_models_labels_without_stopping_daemons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    calls: list[list[str]] = []
    monkeypatch.setattr(module.sys, "platform", "darwin")

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(arguments)
        return subprocess.CompletedProcess(arguments, 0, stdout="state = running\n", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
    )

    handoff.prepare(
        dry_run=True,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
    )
    handoff.restore()

    assert calls == [["print", f"gui/{os.getuid()}/{label}"] for label in module.LAB_LAUNCHD_LABELS]
    payload = json.loads(capsys.readouterr().err)
    assert payload == {
        "lab_daemon_handoff": "planned",
        "labels": list(module.LAB_LAUNCHD_LABELS),
        "stopped": False,
    }


def test_handoff_rebinds_committed_generation_plists_after_transition_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    now = datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    first = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    first.prepare(
        dry_run=False,
        target_ref="b" * 40,
        target_sha="b" * 40,
        action="deploy",
        now=now,
    )
    first.close()

    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    rebound = dict(installation)
    rebound["registered_by_commit"] = "b" * 40
    rebound["environment_generation_id"] = "c" * 64
    rebound["handoff_operation_id"] = first.operation_id
    rebound_plists: dict[str, object] = {}
    for label in module.LAB_LAUNCHD_LABELS:
        path = root / "deploy" / "launchd" / f"{label}.plist"
        replacement = path.with_suffix(".next")
        replacement.write_text(f"<plist><dict><key>{label}-B</key></dict></plist>\n")
        replacement.chmod(0o600)
        os.replace(replacement, path)
        observed = path.lstat()
        rebound_plists[label] = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "device": observed.st_dev,
            "inode": observed.st_ino,
        }
    rebound["plists"] = rebound_plists
    module._atomic_private_json(module._stable_record_path(lock_path, "lab-install"), rebound)
    module._atomic_private_json(
        marker_path_for_lock(lock_path),
        {
            "commit": "b" * 40,
            "environment_generation_id": "c" * 64,
        },
    )

    resumed = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    resumed.prepare(
        dry_run=False,
        target_ref="b" * 40,
        target_sha="b" * 40,
        action="deploy",
        now=now,
    )

    active = module._private_json(resumed.record_path, label="Lab handoff state")
    assert active is not None
    assert active["operation_id"] == first.operation_id
    assert active["installation_identity"] == module._lab_installation_identity(
        lock_path,
        rebound,
    )
    resumed.close()


def test_lab_handoff_dry_run_requires_every_installed_label_loaded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    calls: list[tuple[str, ...]] = []

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(arguments))
        label = arguments[-1].rsplit("/", 1)[-1]
        return subprocess.CompletedProcess(
            arguments,
            0 if label != module.LAB_LAUNCHD_LABELS[-1] else 113,
            stdout="state = running\n" if label != module.LAB_LAUNCHD_LABELS[-1] else "",
            stderr="",
        )

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    try:
        with pytest.raises(module.DeployBootstrapError, match="all installed"):
            handoff.prepare(
                dry_run=True,
                target_ref="a" * 40,
                target_sha="a" * 40,
                action="deploy",
            )
    finally:
        handoff.close()

    assert not [call for call in calls if call[0] in {"bootout", "bootstrap"}]


def test_lab_handoff_rejects_invalid_or_changed_target_before_bootout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    calls: list[tuple[str, ...]] = []

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(arguments))
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            durable = json.loads(
                module._stable_record_path(lock_path, "lab-handoff").read_text(encoding="utf-8")
            )
            assert durable["target_ref"] == durable["target_sha"] == "a" * 40
            assert durable["action"] == "deploy"
            assert durable["release_profile"] == "macos-lab"
            assert durable["lifecycle_mode"] == "installed"
            assert durable["installation_identity"]["sha256"]
            loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    invalid = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    try:
        with pytest.raises(module.DeployBootstrapError, match="exact target"):
            invalid.prepare(
                dry_run=False,
                target_ref="not-a-target",
                target_sha="short",
                action="deploy",
                now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            )
    finally:
        invalid.close()
    assert not [call for call in calls if call[0] == "bootout"]

    first = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    first.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    first.close()
    bootouts = sum(call[0] == "bootout" for call in calls)
    resumed = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    try:
        with pytest.raises(module.DeployBootstrapError, match="binding changed"):
            resumed.prepare(
                dry_run=False,
                target_ref="b" * 40,
                target_sha="b" * 40,
                action="deploy",
                now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            )
    finally:
        resumed.close()
    assert sum(call[0] == "bootout" for call in calls) == bootouts


def test_lab_handoff_persists_typed_intent_before_first_bootout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    loaded = set(module.LAB_LAUNCHD_LABELS)
    prepared_operation = "7" * 32
    prepared_path = lock_path.with_name(f"{lock_path.stem}.intent.prepared.json")

    def persist(operation_id: str, labels: tuple[str, ...]) -> tuple[str, str]:
        assert operation_id
        assert labels == tuple(module.LAB_LAUNCHD_LABELS)
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
            handoff_operation_id=operation_id,
            handoff_labels=labels,
            operation_id=prepared_operation,
        )
        module._atomic_private_json(prepared_path, asdict(intent), absent=True)
        return prepared_operation, operation_id

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        label = arguments[-1].rsplit("/", 1)[-1]
        if arguments[0] == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if arguments[0] == "bootout":
            assert prepared_path.is_file()
            prepared = DeploymentIntent.from_payload(json.loads(prepared_path.read_text()))
            assert prepared.operation_id == prepared_operation
            assert prepared.handoff_operation_id == handoff.operation_id
            loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    try:
        handoff.prepare(
            dry_run=False,
            target_ref="b" * 40,
            target_sha="b" * 40,
            action="deploy",
            prepare_intent=persist,
            now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    finally:
        handoff.close()

    assert handoff.prepared_intent_operation_id == prepared_operation


@pytest.mark.parametrize("recovery_action", ("resume", "rollback"))
def test_prepared_only_transaction_is_detected_and_materialized_for_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    recovery_action: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    original_operation = "1" * 32
    prepared = _handoff_deployment_intent(
        module,
        handoff_operation_id=original_operation,
        operation_id="2" * 32,
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        stage="planned",
    )
    prepared_path = lock_path.with_name(f"{lock_path.stem}.intent.prepared.json")
    module._atomic_private_json(prepared_path, asdict(prepared), absent=True)
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    pending = module._incomplete_handoff_payload(root=root, lock_path=lock_path)
    assert pending is not None
    assert pending["stage"] == "prepared"
    assert pending["operation_id"] == original_operation

    recovery = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=original_operation,
    )
    try:
        recovery_target = (
            prepared.target_sha if recovery_action == "resume" else prepared.previous_sha
        )
        recovery.prepare(
            dry_run=False,
            target_ref=(prepared.target_ref if recovery_action == "resume" else recovery_target),
            target_sha=recovery_target,
            action=recovery_action,
            now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    finally:
        recovery.close()

    root_record = module._private_json(
        module._operation_handoff_path(lock_path, original_operation),
        label="materialized prepared handoff",
    )
    assert root_record is not None
    assert root_record["action"] == "deploy"
    assert root_record["stage"] == "planned"
    assert recovery.prepared_intent_operation_id == prepared.operation_id


def test_prepared_only_recovery_preserves_prior_completed_active_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            loaded.remove(label)
        elif command == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", lambda **_kwargs: _READINESS_A)
    prior = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    prior.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=prior.operation_id,
        generation=_READINESS_A,
    )
    prior.restore()
    prior_proof = module._completed_handoff_path(lock_path, prior.operation_id)
    prior_bytes = prior_proof.read_bytes()

    original_operation = "1" * 32
    prepared = _handoff_deployment_intent(
        module,
        handoff_operation_id=original_operation,
        operation_id="2" * 32,
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        stage="planned",
    )
    module._atomic_private_json(
        lock_path.with_name(f"{lock_path.stem}.intent.prepared.json"),
        asdict(prepared),
        absent=True,
    )

    pending = module._incomplete_handoff_payload(root=root, lock_path=lock_path)
    assert pending is not None and pending["stage"] == "prepared"
    recovery = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=original_operation,
    )
    try:
        recovery.prepare(
            dry_run=False,
            target_ref=prepared.target_ref,
            target_sha=prepared.target_sha,
            action="resume",
            now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    finally:
        recovery.close()

    assert prior_proof.read_bytes() == prior_bytes
    root_record = module._private_json(
        module._operation_handoff_path(lock_path, original_operation),
        label="materialized prepared handoff",
    )
    assert root_record is not None and root_record["stage"] == "planned"


def test_pre_deployer_abort_restores_previous_without_completed_target_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    loaded = set(module.LAB_LAUNCHD_LABELS)
    previous_sha = "a" * 40
    target_sha = "b" * 40

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            loaded.remove(label)
        elif command == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)

    def persist(
        handoff_operation_id: str,
        handoff_labels: tuple[str, ...],
    ) -> tuple[str, str]:
        intent = _handoff_deployment_intent(
            module,
            handoff_operation_id=handoff_operation_id,
            operation_id="3" * 32,
            previous_sha=previous_sha,
            target_sha=target_sha,
            target_ref=target_sha,
            stage="planned",
        )
        assert handoff_labels == tuple(module.LAB_LAUNCHD_LABELS)
        module._atomic_private_json(
            lock_path.with_name(f"{lock_path.stem}.intent.prepared.json"),
            asdict(intent),
            absent=True,
        )
        return intent.operation_id, intent.handoff_operation_id

    handoff.prepare(
        dry_run=False,
        target_ref=target_sha,
        target_sha=target_sha,
        action="deploy",
        prepare_intent=persist,
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    monkeypatch.setattr(
        module,
        "_wait_for_lab_readiness",
        lambda **_kwargs: ("4" * 32, "8" * 64, previous_sha),
    )

    handoff.abort_prepared()

    active = module._private_json(handoff.record_path, label="aborted handoff")
    assert active is not None
    assert active["stage"] == "aborted"
    assert active["action"] == "deploy"
    assert loaded == set(module.LAB_LAUNCHD_LABELS)
    assert not module._completed_handoff_path(lock_path, handoff.operation_id).exists()


def test_prepared_abort_restores_partial_stop_after_prepare_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    loaded = set(module.LAB_LAUNCHD_LABELS)
    bootouts = 0

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal bootouts
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            bootouts += 1
            if bootouts == 2:
                raise module.DeployBootstrapError("injected partial stop")
            loaded.remove(label)
        elif command == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)

    def persist(
        handoff_operation_id: str,
        _handoff_labels: tuple[str, ...],
    ) -> tuple[str, str]:
        intent = _handoff_deployment_intent(
            module,
            handoff_operation_id=handoff_operation_id,
            operation_id="3" * 32,
            previous_sha="a" * 40,
            target_sha="b" * 40,
            target_ref="b" * 40,
            stage="planned",
        )
        module._atomic_private_json(
            lock_path.with_name(f"{lock_path.stem}.intent.prepared.json"),
            asdict(intent),
            absent=True,
        )
        return intent.operation_id, intent.handoff_operation_id

    with pytest.raises(module.DeployBootstrapError, match="partial stop"):
        handoff.prepare(
            dry_run=False,
            target_ref="b" * 40,
            target_sha="b" * 40,
            action="deploy",
            prepare_intent=persist,
            now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    monkeypatch.setattr(
        module,
        "_wait_for_lab_readiness",
        lambda **_kwargs: ("4" * 32, "8" * 64, "a" * 40),
    )

    handoff.abort_prepared()

    assert loaded == set(module.LAB_LAUNCHD_LABELS)
    active = module._private_json(handoff.record_path, label="aborted partial handoff")
    assert active is not None and active["stage"] == "aborted"
    module._validate_handoff_record_shape(
        root=root,
        lock_path=lock_path,
        payload=active,
        operation_id=handoff.operation_id,
        completed=False,
    )
    assert not module._completed_handoff_path(lock_path, handoff.operation_id).exists()


def test_handoff_never_completes_with_readiness_from_a_different_code_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            loaded.remove(label)
        elif command == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    handoff.prepare(
        dry_run=False,
        target_ref="b" * 40,
        target_sha="b" * 40,
        action="deploy",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    monkeypatch.setattr(
        module,
        "_wait_for_lab_readiness",
        lambda **_kwargs: ("4" * 32, "8" * 64, "a" * 40),
    )

    with pytest.raises(module.DeployBootstrapError, match="different code generation"):
        handoff.restore()

    assert not module._completed_handoff_path(lock_path, handoff.operation_id).exists()


def test_incomplete_handoff_resume_is_deferred_without_writes_in_protected_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    calls: list[tuple[str, ...]] = []

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(tuple(arguments))
        label = arguments[-1].rsplit("/", 1)[-1]
        if arguments[0] == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if arguments[0] == "bootout":
            loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    first = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    first.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    first.close()
    calls_before = list(calls)
    durable_before = module._stable_record_path(lock_path, "lab-handoff").read_bytes()

    resumed = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    try:
        with pytest.raises(module.DeployDeferredError, match="protected") as deferred:
            resumed.prepare(
                dry_run=False,
                target_ref="a" * 40,
                target_sha="a" * 40,
                action="deploy",
                now=datetime(2026, 7, 29, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            )
    finally:
        resumed.close()

    assert calls == calls_before
    assert deferred.value.exit_code == 75
    assert module._stable_record_path(lock_path, "lab-handoff").read_bytes() == durable_before


@pytest.mark.parametrize("dry_run", (False, True))
def test_protected_incomplete_handoff_defers_before_fetch_or_ref_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    dry_run: bool,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path)
    module = _bootstrap_module()
    handoff = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": "a" * 32,
        "checkout_root": str(checkout),
        "stage": "stopped",
        "labels": list(module.LAB_LAUNCHD_LABELS),
        "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
        "stopped_labels": list(module.LAB_LAUNCHD_LABELS),
        "restarted_labels": [],
        "updated_at": "2026-07-28T00:00:00+00:00",
        "target_ref": commit,
        "target_sha": commit,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": module._lab_installation_identity(
            lock_path,
            module._read_lab_installation_state(root=checkout, lock_path=lock_path),
        ),
        "supersedes_operation_id": "",
    }
    module._atomic_private_json(module._stable_record_path(lock_path, "lab-handoff"), handoff)
    fetch_head = checkout / ".git" / "FETCH_HEAD"
    refs = checkout / ".git" / "refs"

    def snapshot(path: Path) -> tuple[tuple[str, bytes, int, int, int], ...]:
        return tuple(
            sorted(
                (
                    str(candidate.relative_to(path)),
                    candidate.read_bytes(),
                    candidate.stat().st_ino,
                    candidate.stat().st_mtime_ns,
                    candidate.stat().st_size,
                )
                for candidate in path.rglob("*")
                if candidate.is_file()
            )
        )

    refs_before = snapshot(refs)
    fetch_before = None if not fetch_head.exists() else (fetch_head.read_bytes(), fetch_head.stat())
    authority_before = _tree_snapshot(lock_path.parent)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: True)
    monkeypatch.chdir(checkout)
    command = _command(checkout, python, lock_path, lifecycle_mode="installed")[4:]
    if dry_run:
        command.append("--dry-run")

    result = module.main(command)

    assert result == 75
    assert snapshot(refs) == refs_before
    assert _tree_snapshot(lock_path.parent) == authority_before
    if fetch_before is None:
        assert not fetch_head.exists()
    else:
        payload, observed = fetch_before
        after = fetch_head.stat()
        assert fetch_head.read_bytes() == payload
        assert (after.st_ino, after.st_mtime_ns, after.st_size) == (
            observed.st_ino,
            observed.st_mtime_ns,
            observed.st_size,
        )


def test_installed_bootstrap_missing_installation_fails_before_any_namespace_or_git_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, python, lock_path, _commit = _checkout(
        tmp_path,
        publish_marker=False,
        install_state=False,
    )
    shutil.rmtree(lock_path.parent)
    module = _bootstrap_module()
    index = checkout / ".git" / "index"
    index_before = (index.read_bytes(), index.stat())
    git_before = _tree_snapshot(checkout / ".git")
    tree_before = _tree_snapshot(tmp_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.chdir(checkout)

    result = module.main(_command(checkout, python, lock_path, lifecycle_mode="installed")[4:])

    assert result == 2
    assert not lock_path.parent.exists()
    assert _tree_snapshot(checkout / ".git") == git_before
    assert _tree_snapshot(tmp_path) == tree_before
    index_after = index.stat()
    assert index.read_bytes() == index_before[0]
    assert (index_after.st_ino, index_after.st_mtime_ns) == (
        index_before[1].st_ino,
        index_before[1].st_mtime_ns,
    )


def test_installed_bootstrap_tampered_prepared_sentinel_fails_before_fetch_or_handoff_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    module = _bootstrap_module()
    installation = module._read_lab_installation_state(root=checkout, lock_path=lock_path)
    runtime_root = Path(str(installation["runtime_root"]))
    sentinel = runtime_root / module.LAB_RUNTIME_PREPARED_FILENAME
    sentinel.write_text("{}\n", encoding="utf-8")
    sentinel.chmod(0o600)
    git_before = _tree_snapshot(checkout / ".git")
    authority_before = _tree_snapshot(lock_path.parent)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.chdir(checkout)

    result = module.main(_command(checkout, python, lock_path, lifecycle_mode="installed")[4:])

    assert result == 2
    assert _tree_snapshot(checkout / ".git") == git_before
    assert _tree_snapshot(lock_path.parent) == authority_before


def test_installed_target_policy_rejects_privileged_launchd_diff_before_bootout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    module = _bootstrap_module()
    plist = checkout / "deploy" / "launchd" / f"{module.LAB_LAUNCHD_LABELS[0]}.plist"
    plist.write_text(
        "<?xml version='1.0'?><plist version='1.0'><dict><key>Changed</key>"
        "<true/></dict></plist>\n",
        encoding="utf-8",
    )
    _git(checkout, "add", str(plist.relative_to(checkout)))
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "change launchd infrastructure",
    )
    target = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "push", "-q", "origin", "main")
    _git(checkout, "reset", "--hard", previous)
    installation = module._private_json(
        module._stable_record_path(lock_path, "lab-install"),
        label="Lab launchd installation state",
    )
    assert installation is not None
    module._write_lab_installation_state(
        root=checkout,
        lock_path=lock_path,
        runtime_root=Path(str(installation["runtime_root"])),
        readiness_root=Path(str(installation["readiness_root"])),
        expected_commit=previous,
    )
    launchctl_calls: list[list[str]] = []
    run_marker = tmp_path / "deployer-ran"
    monkeypatch.setenv("RUN_MARKER", str(run_marker))
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: (
            launchctl_calls.append(arguments)
            or subprocess.CompletedProcess(arguments, 0, stdout="state = running\n", stderr="")
        ),
    )
    monkeypatch.chdir(checkout)

    result = module.main(
        _command(
            checkout,
            python,
            lock_path,
            target=target,
            lifecycle_mode="installed",
        )[4:]
    )

    assert result == 2
    assert launchctl_calls == []
    assert not run_marker.exists()
    assert _git(checkout, "rev-parse", "HEAD") == previous


def test_installed_already_current_returns_before_handoff_or_launchd_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checkout, python, lock_path, current = _checkout(tmp_path)
    module = _bootstrap_module()
    launchctl_calls: list[list[str]] = []
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: (
            launchctl_calls.append(arguments)
            or subprocess.CompletedProcess(arguments, 0, stdout="state = running\n", stderr="")
        ),
    )
    monkeypatch.chdir(checkout)

    result = module.main(
        _command(
            checkout,
            python,
            lock_path,
            target=current,
            lifecycle_mode="installed",
        )[4:]
    )

    assert result == 0
    assert not [call for call in launchctl_calls if call[0] in {"bootout", "bootstrap"}]
    assert not module._stable_record_path(lock_path, "lab-handoff").exists()
    assert not list(lock_path.parent.glob(f"{lock_path.stem}.lab-handoff.*"))
    assert '"status": "already_current"' in capsys.readouterr().out


def test_installed_already_current_with_incomplete_handoff_requires_explicit_recovery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checkout, python, lock_path, current = _checkout(tmp_path)
    module = _bootstrap_module()
    installation = module._read_lab_installation_state(root=checkout, lock_path=lock_path)
    handoff_path = module._stable_record_path(lock_path, "lab-handoff")
    payload = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": "a" * 32,
        "checkout_root": str(checkout),
        "stage": "stopped",
        "labels": list(module.LAB_LAUNCHD_LABELS),
        "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
        "stopped_labels": list(module.LAB_LAUNCHD_LABELS),
        "restarted_labels": [],
        "updated_at": "2026-07-28T00:00:00+00:00",
        "target_ref": current,
        "target_sha": current,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": module._lab_installation_identity(lock_path, installation),
        "supersedes_operation_id": "",
    }
    module._atomic_private_json(handoff_path, payload)
    before = handoff_path.read_bytes()
    launchctl_calls: list[list[str]] = []
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: (
            launchctl_calls.append(arguments)
            or subprocess.CompletedProcess(arguments, 0, stdout="state = running\n", stderr="")
        ),
    )
    monkeypatch.chdir(checkout)

    result = module.main(
        _command(
            checkout,
            python,
            lock_path,
            target=current,
            lifecycle_mode="installed",
        )[4:]
    )

    assert result == 2
    assert handoff_path.read_bytes() == before
    assert not [call for call in launchctl_calls if call[0] in {"bootout", "bootstrap"}]
    status = json.loads(capsys.readouterr().out)
    assert status == {
        "allowed_actions": ["resume", "rollback"],
        "handoff_operation_id": "a" * 32,
        "handoff_stage": "stopped",
        "status": "recovery_required",
    }


def test_installed_empty_change_plan_returns_before_handoff_or_intent_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "--allow-empty",
        "-qm",
        "empty release",
    )
    target = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "push", "-q", "origin", "main")
    _git(checkout, "reset", "--hard", previous)
    module = _bootstrap_module()
    launchctl_calls: list[list[str]] = []
    intent_path = intent_path_for_lock(lock_path)
    intent_before = intent_path.read_bytes() if intent_path.exists() else None
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: (
            launchctl_calls.append(arguments)
            or subprocess.CompletedProcess(arguments, 0, stdout="state = running\n", stderr="")
        ),
    )
    monkeypatch.chdir(checkout)

    result = module.main(
        _command(
            checkout,
            python,
            lock_path,
            target=target,
            lifecycle_mode="installed",
        )[4:]
    )

    assert result == 0
    assert _git(checkout, "rev-parse", "HEAD") == previous
    assert not [call for call in launchctl_calls if call[0] in {"bootout", "bootstrap"}]
    assert (intent_path.read_bytes() if intent_path.exists() else None) == intent_before
    assert not list(lock_path.parent.glob(f"{lock_path.stem}.lab-handoff*"))


def test_installed_rollout_commits_only_after_generation_bound_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    module = _bootstrap_module()
    loaded = set(module.LAB_LAUNCHD_LABELS)
    observed_stages: list[str] = []

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            prepared_path = lock_path.with_name(f"{lock_path.stem}.intent.prepared.json")
            assert prepared_path.is_file()
            prepared = DeploymentIntent.from_payload(json.loads(prepared_path.read_text()))
            assert prepared.previous_sha == previous
            assert prepared.target_sha == target
            assert prepared.handoff_operation_id
            loaded.remove(label)
        elif action == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    def ready(**_kwargs: object) -> tuple[str, str, str]:
        payload = module._private_json(
            intent_path_for_lock(lock_path),
            label="deployment intent before Lab readiness",
        )
        assert payload is not None
        observed_stages.append(str(payload["stage"]))
        assert not commit_path_for_lock(lock_path).exists()
        return module._release_readiness_expectation(lock_path)

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", ready)
    monkeypatch.chdir(checkout)
    prior_modules = {
        name: value
        for name, value in sys.modules.items()
        if name == "rquant" or name.startswith("rquant.")
    }
    for name in prior_modules:
        sys.modules.pop(name, None)
    checkout_src = str(checkout / "src")
    sys.path.insert(0, checkout_src)
    try:
        from rquant.ops import production_deploy as checkout_deployer

        monkeypatch.setattr(checkout_deployer, "is_protected_market_window", lambda _now: False)
        result = module.main(
            _command(
                checkout,
                python,
                lock_path,
                target=target,
                lifecycle_mode="installed",
            )[4:]
        )
    finally:
        sys.path.remove(checkout_src)
        for name in tuple(sys.modules):
            if name == "rquant" or name.startswith("rquant."):
                sys.modules.pop(name, None)
        sys.modules.update(prior_modules)

    assert result == 0
    assert observed_stages == ["awaiting_readiness"]
    completed = DeploymentIntent.from_payload(
        json.loads(intent_path_for_lock(lock_path).read_text(encoding="utf-8"))
    )
    committed = ReleaseGenerationCommit.from_payload(
        json.loads(commit_path_for_lock(lock_path).read_text(encoding="utf-8"))
    )
    assert completed.stage == "completed"
    assert committed.transaction_sha256 == completed.content_hash()
    assert not lock_path.with_name(f"{lock_path.stem}.intent.prepared.json").exists()
    assert loaded == set(module.LAB_LAUNCHD_LABELS)
    local_install = json.loads(
        lock_path.with_name(f"{lock_path.stem}.lab-local-install.json").read_text(encoding="utf-8")
    )
    assert local_install["code_sha"] == target
    generation = local_install["environment_generation_id"]
    for label in module.LAB_LAUNCHD_LABELS:
        plist_path = Path(local_install["launch_agents_dir"]) / f"{label}.plist"
        plist = plistlib.loads(plist_path.read_bytes())
        arguments = plist["ProgramArguments"]
        assert str(lock_path.parent / f"{lock_path.stem}.venvs" / generation) in arguments[0]
        assert target in arguments


@pytest.mark.parametrize(
    ("interrupted_stage", "handoff_boundary"),
    (
        ("awaiting_readiness", "proof"),
        ("awaiting_readiness", "operation"),
        ("awaiting_readiness", "stable"),
        ("completed", "stable"),
    ),
)
def test_explicit_resume_converges_readiness_commit_crash_windows(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    interrupted_stage: str,
    handoff_boundary: str,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    module = _bootstrap_module()
    loaded = set(module.LAB_LAUNCHD_LABELS)
    launchctl_mutations: list[tuple[str, ...]] = []

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        launchctl_mutations.append(tuple(arguments))
        if command == "bootout":
            loaded.remove(label)
        elif command == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(
        module,
        "_wait_for_lab_readiness",
        lambda **_kwargs: module._release_readiness_expectation(lock_path),
    )
    monkeypatch.chdir(checkout)
    prior_modules = {
        name: value
        for name, value in sys.modules.items()
        if name == "rquant" or name.startswith("rquant.")
    }
    for name in prior_modules:
        sys.modules.pop(name, None)
    checkout_src = str(checkout / "src")
    sys.path.insert(0, checkout_src)
    try:
        from rquant.ops import production_deploy as checkout_deployer

        monkeypatch.setattr(checkout_deployer, "is_protected_market_window", lambda _now: False)
        assert (
            module.main(
                _command(
                    checkout,
                    python,
                    lock_path,
                    target=target,
                    lifecycle_mode="installed",
                )[4:]
            )
            == 0
        )
        intent_path = intent_path_for_lock(lock_path)
        completed = DeploymentIntent.from_payload(json.loads(intent_path.read_text()))
        payload = asdict(completed)
        if interrupted_stage == "awaiting_readiness":
            payload["stage"] = "awaiting_readiness"
            payload["stage_history"] = payload["stage_history"][:-1]
            payload["updated_at"] = payload["stage_history"][-1]["timestamp"]
        module._atomic_private_json(intent_path, payload)
        commit_path_for_lock(lock_path).unlink()
        stable_path = module._stable_record_path(lock_path, "lab-handoff")
        completed_handoff = module._private_json(
            stable_path,
            label="completed Lab handoff",
        )
        assert completed_handoff is not None
        operation_path = module._operation_handoff_path(
            lock_path,
            str(completed_handoff["operation_id"]),
        )
        partial_handoff = {
            key: value
            for key, value in completed_handoff.items()
            if key not in {"generation_operation_id", "environment_generation_id", "code_sha"}
        }
        partial_handoff["stage"] = "restarting"
        if handoff_boundary == "proof":
            module._atomic_private_json(operation_path, partial_handoff)
            module._atomic_private_json(stable_path, partial_handoff)
        elif handoff_boundary == "operation":
            module._atomic_private_json(stable_path, partial_handoff)
        launchctl_mutations.clear()

        result = module.main(
            _command(
                checkout,
                python,
                lock_path,
                target=target,
                mode="recover",
                recovery_action="resume",
                lifecycle_mode="installed",
            )[4:]
        )
    finally:
        sys.path.remove(checkout_src)
        for name in tuple(sys.modules):
            if name == "rquant" or name.startswith("rquant."):
                sys.modules.pop(name, None)
        sys.modules.update(prior_modules)

    assert result == 0
    recovered = DeploymentIntent.from_payload(
        json.loads(intent_path_for_lock(lock_path).read_text())
    )
    committed = ReleaseGenerationCommit.from_payload(
        json.loads(commit_path_for_lock(lock_path).read_text())
    )
    assert recovered.stage == "completed"
    assert committed.transaction_sha256 == recovered.content_hash()
    assert launchctl_mutations == []
    final_handoff = module._private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        label="converged Lab handoff",
    )
    assert final_handoff is not None and final_handoff["stage"] == "completed"


def test_installed_readiness_failure_rolls_back_previous_generation_and_labels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    module = _bootstrap_module()
    loaded = set(module.LAB_LAUNCHD_LABELS)
    readiness_calls = 0

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            loaded.remove(label)
        elif action == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    def ready(**_kwargs: object) -> tuple[str, str, str]:
        nonlocal readiness_calls
        readiness_calls += 1
        intent = module._private_json(
            intent_path_for_lock(lock_path),
            label="deployment intent before Lab readiness",
        )
        assert intent is not None and intent["stage"] == "awaiting_readiness"
        expected = module._release_readiness_expectation(lock_path)
        if readiness_calls == 1:
            assert expected[2] == target
            raise module.DeployBootstrapError("target daemon readiness failed")
        assert expected[2] == previous
        return expected

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_is_protected_handoff_window", lambda _now=None: False)
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", ready)
    monkeypatch.chdir(checkout)
    prior_modules = {
        name: value
        for name, value in sys.modules.items()
        if name == "rquant" or name.startswith("rquant.")
    }
    for name in prior_modules:
        sys.modules.pop(name, None)
    checkout_src = str(checkout / "src")
    sys.path.insert(0, checkout_src)
    try:
        from rquant.ops import production_deploy as checkout_deployer

        monkeypatch.setattr(checkout_deployer, "is_protected_market_window", lambda _now: False)
        result = module.main(
            _command(
                checkout,
                python,
                lock_path,
                target=target,
                lifecycle_mode="installed",
            )[4:]
        )
    finally:
        sys.path.remove(checkout_src)
        for name in tuple(sys.modules):
            if name == "rquant" or name.startswith("rquant."):
                sys.modules.pop(name, None)
        sys.modules.update(prior_modules)

    assert result == 1
    assert readiness_calls == 2
    assert _git(checkout, "rev-parse", "HEAD") == previous
    marker = ReleaseGenerationMarker.from_payload(
        json.loads(marker_path_for_lock(lock_path).read_text(encoding="utf-8"))
    )
    completed = DeploymentIntent.from_payload(
        json.loads(intent_path_for_lock(lock_path).read_text(encoding="utf-8"))
    )
    committed = ReleaseGenerationCommit.from_payload(
        json.loads(commit_path_for_lock(lock_path).read_text(encoding="utf-8"))
    )
    assert marker.commit == previous
    assert completed.stage == "completed"
    assert any(item["stage"] == "recovery_started" for item in completed.stage_history)
    assert any(item["stage"] == "handoff_rebound" for item in completed.stage_history)
    assert committed.transaction_sha256 == completed.content_hash()
    assert loaded == set(module.LAB_LAUNCHD_LABELS)
    local_install = json.loads(
        lock_path.with_name(f"{lock_path.stem}.lab-local-install.json").read_text(encoding="utf-8")
    )
    assert local_install["code_sha"] == previous
    for label in module.LAB_LAUNCHD_LABELS:
        plist_path = Path(local_install["launch_agents_dir"]) / f"{label}.plist"
        arguments = plistlib.loads(plist_path.read_bytes())["ProgramArguments"]
        assert previous in arguments


def test_recovery_target_binding_is_verified_before_launchd_handoff(
    tmp_path: Path,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    target = "b" * 40
    previous = "a" * 40
    module._atomic_private_json(
        lock_path.with_name(f"{lock_path.stem}.intent.json"),
        asdict(
            _handoff_deployment_intent(
                module,
                handoff_operation_id="d" * 32,
                operation_id="c" * 32,
                previous_sha=previous,
                target_sha=target,
                target_ref="v0.99.1",
                stage="timers_stopped",
            )
        ),
        absent=True,
    )

    module._verify_recovery_target_binding(
        root=root,
        lock_path=lock_path,
        target_ref="v0.99.1",
        target_sha=target,
        action="resume",
    )
    module._verify_recovery_target_binding(
        root=root,
        lock_path=lock_path,
        target_ref=previous,
        target_sha=previous,
        action="rollback",
    )

    with pytest.raises(module.DeployBootstrapError, match="recorded deployment intent"):
        module._verify_recovery_target_binding(
            root=root,
            lock_path=lock_path,
            target_ref="d" * 40,
            target_sha="d" * 40,
            action="resume",
        )


def test_lab_handoff_restores_all_managed_daemons_and_verifies_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    initially_loaded = set(module.LAB_LAUNCHD_LABELS)
    loaded = set(initially_loaded)
    calls: list[tuple[str, ...]] = []

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del timeout_seconds
        calls.append(tuple(arguments))
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            returncode = 0 if label in loaded else 113
        elif action == "bootout":
            loaded.remove(label)
            returncode = 0
        else:
            label = Path(arguments[-1]).stem
            loaded.add(label)
            returncode = 0
        if check and returncode:
            raise subprocess.CalledProcessError(returncode, arguments)
        stdout = "state = running\n" if action == "print" and returncode == 0 else ""
        return subprocess.CompletedProcess(arguments, returncode, stdout=stdout, stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", lambda **_kwargs: _READINESS_A)
    handoff = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
    )

    handoff.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert loaded == set()
    handoff.restore()

    assert loaded == initially_loaded
    assert handoff.stopped == list(module.LAB_LAUNCHD_LABELS)
    assert sum(call[0] == "bootstrap" for call in calls) == 3


def test_lab_handoff_prepares_exact_target_before_first_bootout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    events: list[str] = []

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, timeout_seconds
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(arguments, 0 if label in loaded else 113)
        assert action == "bootout"
        assert events == ["target-ready"]
        loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0)

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
    )

    def prepare_intent(operation_id: str, labels: tuple[str, ...]) -> tuple[str, str]:
        assert labels == tuple(module.LAB_LAUNCHD_LABELS)
        return "1" * 32, operation_id

    def prepare_target(operation_id: str, target_sha: str) -> None:
        assert operation_id == "1" * 32
        assert target_sha == "b" * 40
        assert loaded == set(module.LAB_LAUNCHD_LABELS)
        events.append("target-ready")

    handoff.prepare(
        dry_run=False,
        target_ref="b" * 40,
        target_sha="b" * 40,
        action="deploy",
        prepare_intent=prepare_intent,
        prepare_target=prepare_target,
        now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert loaded == set()


def test_lab_handoff_target_candidate_failure_leaves_current_daemons_loaded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    mutations: list[str] = []

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(arguments, 0 if label in loaded else 113)
        mutations.append(action)
        return subprocess.CompletedProcess(arguments, 0)

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)

    def reject_candidate(_operation_id: str, _target: str) -> None:
        raise module.DeployBootstrapError("candidate failed")

    try:
        with pytest.raises(module.DeployBootstrapError, match="candidate failed"):
            handoff.prepare(
                dry_run=False,
                target_ref="b" * 40,
                target_sha="b" * 40,
                action="deploy",
                prepare_intent=lambda operation_id, _labels: ("1" * 32, operation_id),
                prepare_target=reject_candidate,
                now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            )
    finally:
        handoff.close()

    assert loaded == set(module.LAB_LAUNCHD_LABELS)
    assert mutations == []


def test_lab_handoff_fails_before_bootout_when_any_managed_daemon_is_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS[:2])
    calls: list[tuple[str, ...]] = []

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, timeout_seconds
        calls.append(tuple(arguments))
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "bootout":
            loaded.discard(label)
            return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")
        if action == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
            return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(
            arguments,
            0 if label in loaded else 113,
            stdout="state = running\n" if label in loaded else "",
            stderr="",
        )

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    try:
        with pytest.raises(module.DeployBootstrapError, match="all installed"):
            handoff.prepare(
                dry_run=False,
                target_ref="a" * 40,
                target_sha="a" * 40,
                action="deploy",
                now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
            )
    finally:
        handoff.restore()

    assert loaded == set(module.LAB_LAUNCHD_LABELS[:2])
    assert not [call for call in calls if call[0] == "bootout"]


def test_lab_handoff_failure_path_restarts_prior_daemons_and_has_bounded_lock_wait(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, timeout_seconds
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "bootout":
            loaded.remove(label)
            returncode = 0
        elif action == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
            returncode = 0
        else:
            returncode = 0 if label in loaded else 113
        stdout = "state = running\n" if action == "print" and returncode == 0 else ""
        return subprocess.CompletedProcess(arguments, returncode, stdout=stdout, stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(
        module,
        "_wait_for_lab_readiness",
        lambda **_kwargs: (_ for _ in ()).throw(
            module.DeployBootstrapError("did not reacquire generation-bound readiness")
        ),
    )
    handoff = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=0.01,
    )
    handoff.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    with pytest.raises(module.DeployBootstrapError, match="did not reacquire"):
        handoff.restore()

    assert loaded == set(module.LAB_LAUNCHD_LABELS)
    assert handoff.lock_fd == -1
    assert not module._completed_handoff_path(lock_path, handoff.operation_id).exists()
    persisted = json.loads(handoff.record_path.read_text(encoding="utf-8"))
    assert persisted["stage"] == "restarting"


def test_lab_handoff_command_timeout_restores_already_stopped_daemons(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    bootout_count = 0

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal bootout_count
        del check, timeout_seconds
        action = arguments[0]
        if action == "print":
            label = arguments[-1].rsplit("/", 1)[-1]
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            bootout_count += 1
            if bootout_count == 2:
                raise module.DeployBootstrapError("Lab launchd handoff command timed out")
            loaded.remove(arguments[-1].rsplit("/", 1)[-1])
        else:
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", lambda **_kwargs: _READINESS_A)
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=0.1)

    with pytest.raises(module.DeployBootstrapError, match="timed out"):
        handoff.prepare(
            dry_run=False,
            target_ref="a" * 40,
            target_sha="a" * 40,
            action="deploy",
            now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    handoff.restore()

    assert loaded == set(module.LAB_LAUNCHD_LABELS)


@pytest.mark.parametrize("label_index", range(3))
@pytest.mark.parametrize(
    "crash_stage",
    ("before_intent", "intent_published", "bootout_complete", "state_recorded"),
)
def test_lab_handoff_bootout_transition_is_crash_recoverable_at_every_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    label_index: int,
    crash_stage: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    target_label = module.LAB_LAUNCHD_LABELS[label_index]

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            loaded.discard(label)
        elif action == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    class CrashHandoff(module._LabLaunchdHandoff):
        crashed = False

        def _after_label_transition_stage(self, stage: str, label: str) -> None:
            if not self.crashed and stage == crash_stage and label == target_label:
                self.crashed = True
                raise RuntimeError(f"crash:{stage}:{label}")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", lambda **_kwargs: _READINESS_A)
    first = CrashHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    with pytest.raises(RuntimeError, match=f"crash:{crash_stage}"):
        first.prepare(
            dry_run=False,
            target_ref="a" * 40,
            target_sha="a" * 40,
            action="deploy",
            now=datetime(2026, 7, 29, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    first.close()

    resumed = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    resumed.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 29, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert loaded == set()
    assert set(resumed.stopped) == set(module.LAB_LAUNCHD_LABELS)
    resumed.restore()
    assert loaded == set(module.LAB_LAUNCHD_LABELS)


def test_lab_handoff_readiness_verifies_every_label_and_stable_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
    operation_id = "a" * 32
    generation_id = "b" * 64
    code_sha = "c" * 40
    for suffix, payload in (
        (
            "complete.json",
            {
                "operation_id": operation_id,
                "environment_generation_id": generation_id,
                "commit": code_sha,
                "transaction_kind": "deployment",
            },
        ),
        (
            "commit.json",
            {
                "operation_id": operation_id,
                "environment_generation_id": generation_id,
                "commit": code_sha,
            },
        ),
        ("intent.json", {"operation_id": operation_id, "stage": "completed"}),
    ):
        path = lock_path.with_name(f"{lock_path.stem}.{suffix}")
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        path.chmod(0o600)
    lock_identity = lock_path.lstat()
    counts = {label: 0 for label in module.LAB_LAUNCHD_LABELS}
    pids = {label: 1000 + index for index, label in enumerate(module.LAB_LAUNCHD_LABELS)}

    def readiness(
        _lock_path: Path,
        label: str,
        **_kwargs: object,
    ) -> dict[str, object]:
        counts[label] += 1
        return {
            "label": label,
            "pid": pids[label],
            "operation_id": operation_id,
            "environment_generation_id": generation_id,
            "code_sha": code_sha,
            "started_at": "2026-07-28T00:00:00+00:00",
            "heartbeat_at": "2026-07-28T00:00:01+00:00",
            "heartbeat_monotonic": float(counts[label]),
            "generation_lock_device": lock_identity.st_dev,
            "generation_lock_inode": lock_identity.st_ino,
        }

    monkeypatch.setattr(module, "_lab_readiness_payload", readiness)
    monkeypatch.setattr(module.os, "kill", lambda _pid, _signal: None)
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: subprocess.CompletedProcess(
            arguments,
            0,
            stdout=(f"state = running\npid = {pids[arguments[-1].rsplit('/', 1)[-1]]}\n"),
            stderr="",
        ),
    )
    try:
        module._wait_for_lab_readiness(
            root=root,
            domain=f"gui/{os.getuid()}",
            labels=list(module.LAB_LAUNCHD_LABELS),
            lock_path=lock_path,
            timeout_seconds=1,
            stability_seconds=0,
        )
    finally:
        os.close(lock_fd)

    assert all(count >= 2 for count in counts.values())


def test_lab_readiness_launchctl_timeout_uses_current_remaining_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    lock_path.write_bytes(b"")
    lock_path.chmod(0o600)
    observed_timeouts: list[float] = []
    monotonic_values = iter((100.0, 100.1, 100.25))

    class StopReadinessError(RuntimeError):
        pass

    def stop_after_first_print(
        _arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del check
        observed_timeouts.append(timeout_seconds)
        raise StopReadinessError

    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module, "_release_readiness_expectation", lambda _path: _READINESS_A)
    monkeypatch.setattr(module, "_launchctl", stop_after_first_print)

    with pytest.raises(StopReadinessError):
        module._wait_for_lab_readiness(
            root=root,
            domain=f"gui/{os.getuid()}",
            labels=[module.LAB_LAUNCHD_LABELS[0]],
            lock_path=lock_path,
            timeout_seconds=1.0,
            stability_seconds=0,
        )

    assert observed_timeouts == [pytest.approx(0.75)]


def test_generation_lock_wait_never_sleeps_past_remaining_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _bootstrap_module()
    root = tmp_path / "rquant"
    root.mkdir()
    lock_root = tmp_path / ".rquant-deploy"
    lock_root.mkdir(mode=0o700)
    lock_path = lock_root / "rquant.lock"
    held = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    monotonic_values = iter((100.0, 100.001, 100.011))
    sleeps: list[float] = []

    monkeypatch.setattr(module.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(module.time, "sleep", sleeps.append)

    try:
        with pytest.raises(module.DeployBootstrapError, match="generation is active"):
            module._acquire_lock(root, lock_path, timeout_seconds=0.01)
    finally:
        os.close(held)

    assert sleeps == [pytest.approx(0.009)]


def test_generation_lock_expired_inherited_deadline_has_no_side_effect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _bootstrap_module()
    root = tmp_path / "rquant"
    root.mkdir()
    lock_path = tmp_path / ".rquant-deploy" / "rquant.lock"
    monkeypatch.setattr(module.time, "monotonic", lambda: 100.0)

    with pytest.raises(module.DeployBootstrapError, match="deadline"):
        module._acquire_lock(
            root,
            lock_path,
            timeout_seconds=30,
            deadline_monotonic=99.9,
        )

    assert not lock_path.parent.exists()


def test_lab_handoff_refuses_to_stop_daemons_in_protected_window(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    calls: list[list[str]] = []
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: calls.append(arguments),
    )
    handoff = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
    )

    with pytest.raises(module.DeployBootstrapError, match="protected"):
        handoff.prepare(
            dry_run=False,
            target_ref="a" * 40,
            target_sha="a" * 40,
            action="deploy",
            now=datetime(2026, 7, 27, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    handoff.restore()

    assert calls == []


def test_lab_handoff_requires_explicit_installation_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)

    with pytest.raises(module.DeployBootstrapError, match="installation state"):
        handoff.prepare(
            dry_run=True,
            target_ref="a" * 40,
            target_sha="a" * 40,
            action="deploy",
        )


def test_lab_handoff_recovery_accepts_partial_loaded_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = {module.LAB_LAUNCHD_LABELS[0]}
    bootstrapped: list[str] = []

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, timeout_seconds
        action = arguments[0]
        if action == "print":
            label = arguments[-1].rsplit("/", 1)[-1]
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            label = arguments[-1].rsplit("/", 1)[-1]
            loaded.remove(label)
            return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")
        label = Path(arguments[-1]).stem
        loaded.add(label)
        bootstrapped.append(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", lambda **_kwargs: _READINESS_A)
    operation_id = "e" * 32
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": operation_id,
            "checkout_root": str(root),
            "stage": "restarting",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": list(module.LAB_LAUNCHD_LABELS),
            "restarted_labels": [module.LAB_LAUNCHD_LABELS[0]],
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": "a" * 40,
            "target_sha": "a" * 40,
            "action": "deploy",
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": module._lab_installation_identity(
                lock_path,
                installation,
            ),
            "supersedes_operation_id": "",
        },
    )
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)

    handoff.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert loaded == set()

    handoff.restore()

    assert loaded == set(module.LAB_LAUNCHD_LABELS)
    assert bootstrapped == list(module.LAB_LAUNCHD_LABELS)
    persisted = json.loads(handoff.record_path.read_text(encoding="utf-8"))
    assert persisted["operation_id"] == operation_id
    assert persisted["stage"] == "completed"
    assert persisted["restarted_labels"] == list(module.LAB_LAUNCHD_LABELS)


@pytest.mark.parametrize(
    ("action", "recovery_ref", "recovery_sha"),
    (
        ("resume", "v0.99.1", "b" * 40),
        ("rollback", "a" * 40, "a" * 40),
    ),
)
def test_recovery_supersedes_recorded_deploy_handoff_from_partial_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
    recovery_ref: str,
    recovery_sha: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    old_operation = "b" * 32
    loaded = {module.LAB_LAUNCHD_LABELS[1]}
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    installation_identity = module._lab_installation_identity(lock_path, installation)
    module._atomic_private_json(
        lock_path.with_name(f"{lock_path.stem}.intent.json"),
        asdict(
            _handoff_deployment_intent(
                module,
                handoff_operation_id=old_operation,
                operation_id="c" * 32,
                previous_sha="a" * 40,
                target_sha="b" * 40,
                target_ref="v0.99.1",
                stage="services_transitioning",
            )
        ),
        absent=True,
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": old_operation,
            "checkout_root": str(root),
            "stage": "stopping",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": [
                module.LAB_LAUNCHD_LABELS[0],
                module.LAB_LAUNCHD_LABELS[2],
            ],
            "restarted_labels": [],
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": "v0.99.1",
            "target_sha": "b" * 40,
            "action": "deploy",
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": installation_identity,
            "supersedes_operation_id": "",
        },
    )

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    supersedes = module._superseding_handoff_operation_id(
        root=root,
        lock_path=lock_path,
        recovery_action=action,
        release_profile="macos-lab",
        lifecycle_mode="installed",
    )
    assert supersedes == old_operation
    recovery = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=supersedes,
    )
    try:
        recovery.prepare(
            dry_run=False,
            target_ref=recovery_ref,
            target_sha=recovery_sha,
            action=action,
            now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    finally:
        recovery.close()

    assert recovery.operation_id != old_operation
    assert loaded == set()
    persisted = json.loads(recovery.record_path.read_text(encoding="utf-8"))
    assert persisted["action"] == action
    assert persisted["supersedes_operation_id"] == old_operation


@pytest.mark.parametrize("ancestor_state", ("missing", "corrupt"))
def test_recovery_validates_active_physical_supersede_chain_before_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ancestor_state: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    operation_a = "a" * 32
    operation_b = "b" * 32
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    installation_identity = module._lab_installation_identity(lock_path, installation)
    intent = (
        _handoff_deployment_intent(
            module,
            handoff_operation_id=operation_a,
            operation_id="c" * 32,
            previous_sha="8" * 40,
            target_sha="9" * 40,
            target_ref="v0.99.1",
            stage="services_transitioning",
        )
        .advance(stage="recovery_started")
        .rebind_handoff(
            handoff_operation_id=operation_b,
            handoff_labels=tuple(module.LAB_LAUNCHD_LABELS),
        )
    )
    module._atomic_private_json(intent_path_for_lock(lock_path), asdict(intent), absent=True)
    active = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": operation_b,
        "checkout_root": str(root),
        "stage": "stopping",
        "labels": list(module.LAB_LAUNCHD_LABELS),
        "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
        "stopped_labels": [module.LAB_LAUNCHD_LABELS[0]],
        "restarted_labels": [],
        "updated_at": "2026-07-29T00:00:00+00:00",
        "target_ref": "v0.99.1",
        "target_sha": "9" * 40,
        "action": "resume",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": installation_identity,
        "supersedes_operation_id": operation_a,
    }
    module._atomic_private_json(
        module._operation_handoff_path(lock_path, operation_b),
        active,
        absent=True,
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        active,
        absent=True,
    )
    if ancestor_state == "corrupt":
        ancestor = {**active, "operation_id": operation_a, "action": "deploy"}
        ancestor["supersedes_operation_id"] = "d" * 32
        module._atomic_private_json(
            module._operation_handoff_path(lock_path, operation_a),
            ancestor,
            absent=True,
        )
    launchctl_calls: list[list[str]] = []
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: launchctl_calls.append(arguments),
    )

    with pytest.raises(module.DeployBootstrapError, match="handoff|supersede"):
        module._superseding_handoff_operation_id(
            root=root,
            lock_path=lock_path,
            recovery_action="rollback",
            release_profile="macos-lab",
            lifecycle_mode="installed",
        )

    assert launchctl_calls == []


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("target_sha", "d" * 40),
        ("target_ref", "v0.99.2"),
        ("release_profile", "linux-production"),
        ("installation_identity", {"path": "/tampered"}),
    ),
)
def test_recovery_rejects_superseded_deploy_handoff_binding_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    replacement: object,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    old_operation = "b" * 32
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    module._atomic_private_json(
        lock_path.with_name(f"{lock_path.stem}.intent.json"),
        asdict(
            _handoff_deployment_intent(
                module,
                handoff_operation_id=old_operation,
                operation_id="c" * 32,
                previous_sha="a" * 40,
                target_sha="b" * 40,
                target_ref="v0.99.1",
                stage="services_transitioning",
            )
        ),
        absent=True,
    )
    payload: dict[str, object] = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": old_operation,
        "checkout_root": str(root),
        "stage": "stopping",
        "labels": list(module.LAB_LAUNCHD_LABELS),
        "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
        "stopped_labels": [module.LAB_LAUNCHD_LABELS[0]],
        "restarted_labels": [],
        "updated_at": "2026-07-28T00:00:00+00:00",
        "target_ref": "v0.99.1",
        "target_sha": "b" * 40,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": module._lab_installation_identity(lock_path, installation),
        "supersedes_operation_id": "",
    }
    payload[field] = replacement
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        payload,
    )
    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: subprocess.CompletedProcess(
            arguments,
            113,
            stdout="",
            stderr="",
        ),
    )
    with pytest.raises(
        module.DeployBootstrapError,
        match="superseded.*binding|handoff record is malformed",
    ):
        module._superseding_handoff_operation_id(
            root=root,
            lock_path=lock_path,
            recovery_action="resume",
            release_profile="macos-lab",
            lifecycle_mode="installed",
        )


@pytest.mark.parametrize("intent_state", ["missing", "different-operation"])
def test_supersede_requires_matching_immutable_intent_before_launchd_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    intent_state: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    old_operation = "7" * 32
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": old_operation,
            "checkout_root": str(root),
            "stage": "stopping",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": [module.LAB_LAUNCHD_LABELS[0]],
            "restarted_labels": [],
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": "v0.99.1",
            "target_sha": "b" * 40,
            "action": "deploy",
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": module._lab_installation_identity(lock_path, installation),
            "supersedes_operation_id": "",
        },
    )
    if intent_state != "missing":
        module._atomic_private_json(
            lock_path.with_name(f"{lock_path.stem}.intent.json"),
            asdict(
                _handoff_deployment_intent(
                    module,
                    handoff_operation_id="9" * 32,
                    operation_id="8" * 32,
                    previous_sha="a" * 40,
                    target_sha="b" * 40,
                    target_ref="v0.99.1",
                    stage="services_transitioning",
                )
            ),
            absent=True,
        )
    launchctl_calls: list[list[str]] = []
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: launchctl_calls.append(arguments),
    )

    with pytest.raises(module.DeployBootstrapError, match="deployment intent.*handoff"):
        module._superseding_handoff_operation_id(
            root=root,
            lock_path=lock_path,
            recovery_action="resume",
            release_profile="macos-lab",
            lifecycle_mode="installed",
        )

    assert launchctl_calls == []


@pytest.mark.parametrize("action", ("resume", "rollback"))
def test_same_action_recovery_validates_intent_and_preserves_supersede_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    action: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    current_operation = "6" * 32
    previous_operation = "5" * 32
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    installation_identity = module._lab_installation_identity(lock_path, installation)
    previous_sha = "a" * 40
    target_sha = "b" * 40
    target_ref = target_sha if action == "resume" else previous_sha
    ancestor = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": previous_operation,
        "checkout_root": str(root),
        "stage": "stopping",
        "labels": list(module.LAB_LAUNCHD_LABELS),
        "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
        "stopped_labels": [module.LAB_LAUNCHD_LABELS[0]],
        "restarted_labels": [],
        "updated_at": "2026-07-28T00:00:00+00:00",
        "target_ref": target_sha,
        "target_sha": target_sha,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": installation_identity,
        "supersedes_operation_id": "",
    }
    module._atomic_private_json(
        module._operation_handoff_path(lock_path, previous_operation),
        ancestor,
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": current_operation,
            "checkout_root": str(root),
            "stage": "stopping",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": [module.LAB_LAUNCHD_LABELS[0]],
            "restarted_labels": [],
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": target_ref,
            "target_sha": target_ref,
            "action": action,
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": installation_identity,
            "supersedes_operation_id": previous_operation,
        },
    )
    intent = DeploymentIntent.create(
        previous_sha=previous_sha,
        target_sha=target_sha,
        target_ref=target_sha,
        changed_files=("src/rquant/preflight.py",),
        restart_services=(),
        active_services=(),
        active_timers=(),
        marker_generation="7" * 64,
        previous_generation_id="8" * 64,
        handoff_operation_id=previous_operation,
        handoff_labels=tuple(module.LAB_LAUNCHD_LABELS),
        operation_id="c" * 32,
    )
    intent = intent.advance(stage="recovery_started")
    intent = intent.rebind_handoff(
        handoff_operation_id=current_operation,
        handoff_labels=tuple(module.LAB_LAUNCHD_LABELS),
    )
    for next_stage in (
        "timers_stopped",
        "deploy_checkout_ready",
        "deploy_dependencies_ready",
        "deploy_preflight_ready",
        "services_transitioning",
    ):
        intent = intent.advance(stage=next_stage)
    module._atomic_private_json(intent_path_for_lock(lock_path), asdict(intent))
    monkeypatch.setattr(module.sys, "platform", "darwin")

    supersedes = module._superseding_handoff_operation_id(
        root=root,
        lock_path=lock_path,
        recovery_action=action,
        release_profile="macos-lab",
        lifecycle_mode="installed",
    )

    assert supersedes == previous_operation


def test_same_action_recovery_rejects_invalid_typed_intent_before_launchd_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    operation_id = "6" * 32
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": operation_id,
            "checkout_root": str(root),
            "stage": "stopping",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": [],
            "restarted_labels": [],
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": "b" * 40,
            "target_sha": "b" * 40,
            "action": "resume",
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": module._lab_installation_identity(lock_path, installation),
            "supersedes_operation_id": "5" * 32,
        },
    )
    intent = asdict(
        _handoff_deployment_intent(
            module,
            handoff_operation_id=operation_id,
            operation_id="c" * 32,
            previous_sha="a" * 40,
            target_sha="b" * 40,
            target_ref="b" * 40,
            stage="services_transitioning",
        )
    )
    intent["changed_files"] = ["deploy/launchd/com.roxor.rquant-lab-worker.plist"]
    module._atomic_private_json(intent_path_for_lock(lock_path), intent)
    launchctl_calls: list[list[str]] = []
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: launchctl_calls.append(arguments),
    )

    with pytest.raises(module.DeployBootstrapError, match="intent|policy|privileged"):
        module._superseding_handoff_operation_id(
            root=root,
            lock_path=lock_path,
            recovery_action="resume",
            release_profile="macos-lab",
            lifecycle_mode="installed",
        )

    assert launchctl_calls == []


def test_completed_recovery_intent_is_rejected_without_mutating_generation_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    operation_id = "6" * 32
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=operation_id,
    )
    marker = marker_path_for_lock(lock_path)
    committed = commit_path_for_lock(lock_path)
    before = (marker.read_bytes(), committed.read_bytes())
    launchctl_calls: list[list[str]] = []
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: launchctl_calls.append(arguments),
    )

    with pytest.raises(module.DeployBootstrapError, match="completed|already"):
        module._superseding_handoff_operation_id(
            root=root,
            lock_path=lock_path,
            recovery_action="resume",
            release_profile="macos-lab",
            lifecycle_mode="installed",
        )

    assert (marker.read_bytes(), committed.read_bytes()) == before
    assert launchctl_calls == []


def test_superseding_rollback_stops_partial_target_labels_before_previous_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    target_operation = "b" * 32
    loaded = {module.LAB_LAUNCHD_LABELS[1]}
    calls: list[tuple[str, str]] = []

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        calls.append((action, label))
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            loaded.remove(label)
        elif action == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    module._atomic_private_json(
        lock_path.with_name(f"{lock_path.stem}.intent.json"),
        asdict(
            _handoff_deployment_intent(
                module,
                handoff_operation_id=target_operation,
                operation_id="c" * 32,
                previous_sha="a" * 40,
                target_sha="b" * 40,
                target_ref="b" * 40,
                stage="services_transitioning",
            )
        ),
        absent=True,
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": target_operation,
            "checkout_root": str(root),
            "stage": "restarting",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": list(module.LAB_LAUNCHD_LABELS),
            "restarted_labels": [module.LAB_LAUNCHD_LABELS[1]],
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": "b" * 40,
            "target_sha": "b" * 40,
            "action": "deploy",
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": module._lab_installation_identity(
                lock_path,
                installation,
            ),
            "supersedes_operation_id": "",
        },
    )
    rollback = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=target_operation,
    )

    rollback.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="rollback",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert loaded == set()
    assert ("bootout", module.LAB_LAUNCHD_LABELS[1]) in calls
    persisted = json.loads(rollback.record_path.read_text(encoding="utf-8"))
    assert persisted["supersedes_operation_id"] == target_operation
    assert persisted["loaded_labels"] == list(module.LAB_LAUNCHD_LABELS)


def test_recovery_retry_recognizes_persisted_successor_before_intent_rebind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    original_operation = "a" * 32
    recovery_operation = "b" * 32
    intent = _handoff_deployment_intent(
        module,
        handoff_operation_id=original_operation,
        operation_id="c" * 32,
        previous_sha="1" * 40,
        target_sha="2" * 40,
        target_ref="2" * 40,
        stage="planned",
    ).advance(stage="recovery_started")
    module._atomic_private_json(intent_path_for_lock(lock_path), asdict(intent), absent=True)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    installation_identity = module._lab_installation_identity(lock_path, installation)
    labels = list(module.LAB_LAUNCHD_LABELS)
    root_payload = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": original_operation,
        "checkout_root": str(root),
        "stage": "stopping",
        "labels": labels,
        "loaded_labels": labels,
        "stopped_labels": labels[:1],
        "restarted_labels": [],
        "updated_at": "2026-07-28T00:00:00+00:00",
        "target_ref": intent.target_ref,
        "target_sha": intent.target_sha,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": installation_identity,
        "supersedes_operation_id": "",
    }
    recovery_payload = {
        **root_payload,
        "operation_id": recovery_operation,
        "stage": "planned",
        "stopped_labels": [],
        "action": "resume",
        "supersedes_operation_id": original_operation,
    }
    module._atomic_private_json(
        module._operation_handoff_path(lock_path, original_operation),
        root_payload,
        absent=True,
    )
    module._atomic_private_json(
        module._operation_handoff_path(lock_path, recovery_operation),
        recovery_payload,
        absent=True,
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        recovery_payload,
        absent=True,
    )

    supersedes = module._superseding_handoff_operation_id(
        root=root,
        lock_path=lock_path,
        recovery_action="resume",
        release_profile="macos-lab",
        lifecycle_mode="installed",
    )

    assert supersedes == original_operation
    rebound = DeploymentIntent.from_payload(
        json.loads(intent_path_for_lock(lock_path).read_text(encoding="utf-8"))
    )
    assert rebound.handoff_operation_id == recovery_operation
    assert [
        item["handoff_operation_id"]
        for item in rebound.stage_history
        if item["stage"] == "handoff_rebound"
    ] == [recovery_operation]
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            loaded.remove(label)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    recovery = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=supersedes,
    )
    recovery.prepare(
        dry_run=False,
        target_ref=intent.target_ref,
        target_sha=intent.target_sha,
        action="resume",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert recovery.operation_id == recovery_operation
    assert recovery.supersedes_operation_id == original_operation


def test_opposite_recovery_persists_exact_a_b_c_chain_before_first_bootout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    operation_a = "a" * 32
    operation_b = "b" * 32
    intent = _handoff_deployment_intent(
        module,
        handoff_operation_id=operation_a,
        operation_id="d" * 32,
        previous_sha="1" * 40,
        target_sha="2" * 40,
        target_ref="2" * 40,
        stage="planned",
    ).advance(stage="recovery_started")
    module._atomic_private_json(intent_path_for_lock(lock_path), asdict(intent), absent=True)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    labels = list(module.LAB_LAUNCHD_LABELS)
    operation_a_payload = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": operation_a,
        "checkout_root": str(root),
        "stage": "stopping",
        "labels": labels,
        "loaded_labels": labels,
        "stopped_labels": labels[:1],
        "restarted_labels": [],
        "updated_at": "2026-07-28T00:00:00+00:00",
        "target_ref": intent.target_ref,
        "target_sha": intent.target_sha,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": module._lab_installation_identity(lock_path, installation),
        "supersedes_operation_id": "",
    }
    operation_b_payload = {
        **operation_a_payload,
        "operation_id": operation_b,
        "stage": "planned",
        "stopped_labels": [],
        "action": "resume",
        "supersedes_operation_id": operation_a,
    }
    module._atomic_private_json(
        module._operation_handoff_path(lock_path, operation_a), operation_a_payload
    )
    module._atomic_private_json(
        module._operation_handoff_path(lock_path, operation_b), operation_b_payload
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"), operation_b_payload
    )

    supersedes = module._superseding_handoff_operation_id(
        root=root,
        lock_path=lock_path,
        recovery_action="rollback",
        release_profile="macos-lab",
        lifecycle_mode="installed",
    )
    assert supersedes == operation_b
    after_b = DeploymentIntent.from_payload(
        json.loads(intent_path_for_lock(lock_path).read_text(encoding="utf-8"))
    )
    assert after_b.handoff_operation_id == operation_b

    loaded = set(module.LAB_LAUNCHD_LABELS)
    observed_before_bootout: list[DeploymentIntent] = []

    def fake_launchctl(arguments: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(arguments, 0, "state = running\n", "")
        if action == "bootout":
            observed_before_bootout.append(
                DeploymentIntent.from_payload(
                    json.loads(intent_path_for_lock(lock_path).read_text(encoding="utf-8"))
                )
            )
            loaded.discard(label)
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    rollback = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=supersedes,
    )
    try:
        rollback.prepare(
            dry_run=False,
            target_ref=intent.previous_sha,
            target_sha=intent.previous_sha,
            action="rollback",
            now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    finally:
        rollback.close()

    assert observed_before_bootout
    operation_c = observed_before_bootout[0].handoff_operation_id
    assert operation_c not in {operation_a, operation_b}
    rebounds = [
        item["handoff_operation_id"]
        for item in observed_before_bootout[0].stage_history
        if item["stage"] == "handoff_rebound"
    ]
    assert rebounds == [operation_b, operation_c]


def test_opposite_retry_recovers_physical_successor_before_intent_rebind(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    operation_a = "a" * 32
    intent = _handoff_deployment_intent(
        module,
        handoff_operation_id=operation_a,
        operation_id="d" * 32,
        previous_sha="1" * 40,
        target_sha="2" * 40,
        target_ref="2" * 40,
        stage="services_transitioning",
    )
    module._atomic_private_json(intent_path_for_lock(lock_path), asdict(intent), absent=True)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    labels = list(module.LAB_LAUNCHD_LABELS)
    operation_a_payload = {
        "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
        "operation_id": operation_a,
        "checkout_root": str(root),
        "stage": "stopping",
        "labels": labels,
        "loaded_labels": labels,
        "stopped_labels": [],
        "restarted_labels": [],
        "updated_at": "2026-07-29T00:00:00+00:00",
        "target_ref": intent.target_ref,
        "target_sha": intent.target_sha,
        "action": "deploy",
        "release_profile": "macos-lab",
        "lifecycle_mode": "installed",
        "installation_identity": module._lab_installation_identity(lock_path, installation),
        "supersedes_operation_id": "",
    }
    module._atomic_private_json(
        module._operation_handoff_path(lock_path, operation_a), operation_a_payload
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"), operation_a_payload
    )
    loaded = set(labels)

    def fake_launchctl(arguments: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                "state = running\n" if label in loaded else "",
                "",
            )
        if action == "bootout":
            loaded.discard(label)
        return subprocess.CompletedProcess(arguments, 0, "", "")

    class CrashAfterSuccessor(module._LabLaunchdHandoff):
        def _after_handoff_successor_published(self) -> None:
            raise RuntimeError("crash after physical successor")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    first = CrashAfterSuccessor(root=root, lock_path=lock_path, timeout_seconds=1)
    with pytest.raises(RuntimeError, match="physical successor"):
        first.prepare(
            dry_run=False,
            target_ref=intent.target_ref,
            target_sha=intent.target_sha,
            action="resume",
            now=datetime(2026, 7, 29, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    first.close()
    operation_b = str(
        json.loads(
            module._stable_record_path(lock_path, "lab-handoff").read_text(encoding="utf-8")
        )["operation_id"]
    )
    assert operation_b != operation_a
    assert (
        DeploymentIntent.from_payload(
            json.loads(intent_path_for_lock(lock_path).read_text(encoding="utf-8"))
        ).handoff_operation_id
        == operation_a
    )

    observed_before_bootout: list[DeploymentIntent] = []

    def observe_launchctl(
        arguments: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        if arguments[0] == "bootout":
            observed_before_bootout.append(
                DeploymentIntent.from_payload(
                    json.loads(intent_path_for_lock(lock_path).read_text(encoding="utf-8"))
                )
            )
        return fake_launchctl(arguments, **kwargs)

    monkeypatch.setattr(module, "_launchctl", observe_launchctl)
    rollback = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    try:
        rollback.prepare(
            dry_run=False,
            target_ref=intent.previous_sha,
            target_sha=intent.previous_sha,
            action="rollback",
            now=datetime(2026, 7, 29, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    finally:
        rollback.close()

    assert observed_before_bootout
    rebound = observed_before_bootout[0]
    assert rebound.handoff_operation_id not in {operation_a, operation_b}
    assert [
        event["handoff_operation_id"]
        for event in rebound.stage_history
        if event["stage"] == "handoff_rebound"
    ] == [operation_b, rebound.handoff_operation_id]


def test_handoff_writer_rejects_invalid_partial_state_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    handoff.installation_identity = module._lab_installation_identity(lock_path, installation)
    handoff.operation_id = "d" * 32
    handoff.loaded = list(module.LAB_LAUNCHD_LABELS)
    handoff.stopped = [module.LAB_LAUNCHD_LABELS[0]]
    handoff.restarted = [module.LAB_LAUNCHD_LABELS[0]]
    handoff.target_ref = "e" * 40
    handoff.target_sha = "e" * 40
    handoff.action = "deploy"

    with pytest.raises(module.DeployBootstrapError, match="handoff.*state|record"):
        handoff._record("stopping")

    assert not handoff.record_path.exists()
    assert not module._operation_handoff_path(lock_path, handoff.operation_id).exists()


def test_completed_handoff_proof_survives_consecutive_installed_releases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    first_generation = ("a" * 32, "b" * 64, "c" * 40)
    second_generation = ("d" * 32, "e" * 64, "f" * 40)
    expectations = iter([first_generation, second_generation])

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, timeout_seconds
        action = arguments[0]
        if action == "print":
            label = arguments[-1].rsplit("/", 1)[-1]
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            loaded.remove(arguments[-1].rsplit("/", 1)[-1])
        else:
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(
        module,
        "_wait_for_lab_readiness",
        lambda **_kwargs: next(expectations),
    )
    now = datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))

    first = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    first.prepare(
        dry_run=False,
        target_ref="c" * 40,
        target_sha="c" * 40,
        action="deploy",
        now=now,
    )
    first_operation = first.operation_id
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=first_operation,
        generation=first_generation,
    )
    first.restore()
    first_proof = module._completed_handoff_path(lock_path, first_operation)
    first_payload = json.loads(first_proof.read_text(encoding="utf-8"))
    interrupted_active = dict(first_payload)
    interrupted_active["stage"] = "restarting"
    for field in ("generation_operation_id", "environment_generation_id", "code_sha"):
        interrupted_active.pop(field)
    module._atomic_private_json(first.record_path, interrupted_active)

    second = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    second.prepare(
        dry_run=False,
        target_ref="f" * 40,
        target_sha="f" * 40,
        action="deploy",
        now=now,
    )
    assert second.operation_id != first_operation
    assert json.loads(first_proof.read_text(encoding="utf-8")) == first_payload
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=second.operation_id,
        generation=second_generation,
    )
    second.restore()

    assert first_payload["generation_operation_id"] == "a" * 32
    assert first_payload["environment_generation_id"] == "b" * 64
    assert first_payload["code_sha"] == "c" * 40
    assert module._completed_handoff_path(lock_path, second.operation_id).is_file()


@pytest.mark.parametrize("crash_after_write", [1, 2, 3])
def test_completed_handoff_crash_boundaries_converge_idempotently(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crash_after_write: int,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    handoff.installation = installation
    handoff.installation_identity = module._lab_installation_identity(lock_path, installation)
    handoff.operation_id = "1" * 32
    handoff.loaded = list(module.LAB_LAUNCHD_LABELS)
    handoff.stopped = list(module.LAB_LAUNCHD_LABELS)
    handoff.restarted = list(module.LAB_LAUNCHD_LABELS)
    handoff.target_ref = _READINESS_A[2]
    handoff.target_sha = _READINESS_A[2]
    handoff.action = "deploy"
    handoff._record("restarting")
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=handoff.operation_id,
    )
    completed_paths = {
        module._completed_handoff_path(lock_path, handoff.operation_id),
        module._operation_handoff_path(lock_path, handoff.operation_id),
        handoff.record_path,
    }
    real_atomic = module._atomic_private_json
    completed_writes = 0

    class SimulatedCrashError(RuntimeError):
        pass

    def crash_after_boundary(path: Path, payload: dict[str, object], **kwargs: object) -> None:
        nonlocal completed_writes
        real_atomic(path, payload, **kwargs)
        if path in completed_paths and payload.get("stage") == "completed":
            completed_writes += 1
            if completed_writes == crash_after_write:
                raise SimulatedCrashError

    monkeypatch.setattr(module, "_atomic_private_json", crash_after_boundary)
    with pytest.raises(SimulatedCrashError):
        handoff._record("completed", generation=_READINESS_A)
    monkeypatch.setattr(module, "_atomic_private_json", real_atomic)

    module._converge_completed_handoff_state(root=root, lock_path=lock_path)
    proof = module._private_json(
        module._completed_handoff_path(lock_path, handoff.operation_id),
        label="completed Lab launchd handoff proof",
    )
    assert proof is not None
    assert module._private_json(handoff.record_path, label="Lab handoff state") == proof
    assert (
        module._private_json(
            module._operation_handoff_path(lock_path, handoff.operation_id),
            label="Lab handoff operation",
        )
        == proof
    )


def test_completed_handoff_convergence_rejects_forged_proof_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    handoff.installation_identity = module._lab_installation_identity(lock_path, installation)
    handoff.operation_id = "2" * 32
    handoff.loaded = list(module.LAB_LAUNCHD_LABELS)
    handoff.stopped = list(module.LAB_LAUNCHD_LABELS)
    handoff.restarted = list(module.LAB_LAUNCHD_LABELS)
    handoff.target_ref = _READINESS_A[2]
    handoff.target_sha = _READINESS_A[2]
    handoff.action = "deploy"
    handoff._record("restarting")
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=handoff.operation_id,
    )
    stable_before = handoff.record_path.read_bytes()
    operation_path = module._operation_handoff_path(lock_path, handoff.operation_id)
    operation_before = operation_path.read_bytes()
    forged = module._private_json(operation_path, label="Lab handoff operation")
    assert forged is not None
    forged.update(
        {
            "stage": "completed",
            "target_sha": "f" * 40,
            "generation_operation_id": _READINESS_A[0],
            "environment_generation_id": _READINESS_A[1],
            "code_sha": _READINESS_A[2],
        }
    )
    module._atomic_private_json(
        module._completed_handoff_path(lock_path, handoff.operation_id),
        forged,
    )

    with pytest.raises(module.DeployBootstrapError, match="completed.*binding"):
        module._converge_completed_handoff_state(root=root, lock_path=lock_path)

    assert handoff.record_path.read_bytes() == stable_before
    assert operation_path.read_bytes() == operation_before


@pytest.mark.parametrize(
    ("field", "forged"),
    [
        ("generation_operation_id", "d" * 32),
        ("environment_generation_id", "e" * 64),
        ("code_sha", "f" * 40),
    ],
)
def test_completed_handoff_convergence_cross_checks_generation_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    forged: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    handoff.installation_identity = module._lab_installation_identity(lock_path, installation)
    handoff.operation_id = "2" * 32
    handoff.loaded = list(module.LAB_LAUNCHD_LABELS)
    handoff.stopped = list(module.LAB_LAUNCHD_LABELS)
    handoff.restarted = list(module.LAB_LAUNCHD_LABELS)
    handoff.target_ref = _READINESS_A[2]
    handoff.target_sha = _READINESS_A[2]
    handoff.action = "deploy"
    handoff._record("restarting")
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=handoff.operation_id,
    )
    stable_before = handoff.record_path.read_bytes()
    operation_path = module._operation_handoff_path(lock_path, handoff.operation_id)
    operation_before = operation_path.read_bytes()
    proof = module._private_json(operation_path, label="Lab handoff operation")
    assert proof is not None
    proof.update(
        {
            "stage": "completed",
            "generation_operation_id": _READINESS_A[0],
            "environment_generation_id": _READINESS_A[1],
            "code_sha": _READINESS_A[2],
        }
    )
    proof[field] = forged
    module._atomic_private_json(
        module._completed_handoff_path(lock_path, handoff.operation_id),
        proof,
    )

    with pytest.raises(module.DeployBootstrapError, match="generation|authority|binding"):
        module._converge_completed_handoff_state(root=root, lock_path=lock_path)

    assert handoff.record_path.read_bytes() == stable_before
    assert operation_path.read_bytes() == operation_before


@pytest.mark.parametrize(
    "mutation",
    (
        lambda payload: payload.update({"unexpected": "value"}),
        lambda payload: payload.update({"schema_version": True}),
        lambda payload: payload.update({"stopped_labels": []}),
        lambda payload: payload.update(
            {
                "installation_identity": {
                    **payload["installation_identity"],
                    "sha256": "f" * 64,
                }
            }
        ),
        lambda payload: payload.update({"supersedes_operation_id": "e" * 32}),
        lambda payload: payload.update(
            {
                "action": "rollback",
                "target_ref": "9" * 40,
                "target_sha": "9" * 40,
                "code_sha": "9" * 40,
                "supersedes_operation_id": "e" * 32,
            }
        ),
    ),
    ids=(
        "extra-field",
        "bool-schema",
        "incomplete-stopped-labels",
        "stale-installation-identity",
        "deploy-with-supersede",
        "rollback-with-missing-chain",
    ),
)
def test_completed_handoff_proof_is_exact_and_bound_to_current_installation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Callable[[dict[str, object]], None],
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    handoff.installation_identity = module._lab_installation_identity(lock_path, installation)
    handoff.operation_id = "2" * 32
    handoff.loaded = list(module.LAB_LAUNCHD_LABELS)
    handoff.stopped = list(module.LAB_LAUNCHD_LABELS)
    handoff.restarted = list(module.LAB_LAUNCHD_LABELS)
    handoff.target_ref = _READINESS_A[2]
    handoff.target_sha = _READINESS_A[2]
    handoff.action = "deploy"
    _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=handoff.operation_id,
    )
    handoff._record("completed", generation=_READINESS_A)
    proof_path = module._completed_handoff_path(lock_path, handoff.operation_id)
    proof = module._private_json(proof_path, label="completed Lab handoff proof")
    assert proof is not None
    mutation(proof)
    for path in (
        proof_path,
        module._operation_handoff_path(lock_path, handoff.operation_id),
        handoff.record_path,
    ):
        module._atomic_private_json(path, proof)

    with pytest.raises(module.DeployBootstrapError, match="handoff|installation|binding"):
        module._converge_completed_handoff_state(root=root, lock_path=lock_path)


@pytest.mark.parametrize(
    "intent_mutation",
    [
        {"changed_files": ["deploy/systemd/rquant-monitor.service"]},
        {"restart_services": ["rquant-monitor.service"]},
        {"active_services": ["rquant-monitor.service"]},
        {"marker_generation": "not-a-generation"},
        {"stage_history": [{"stage": "completed", "timestamp": "not-a-date"}]},
    ],
)
def test_handoff_supersede_rejects_invalid_typed_intent_before_launchd_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    intent_mutation: dict[str, object],
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    old_operation = "6" * 32
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": old_operation,
            "checkout_root": str(root),
            "stage": "stopping",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": [module.LAB_LAUNCHD_LABELS[0]],
            "restarted_labels": [],
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": _READINESS_A[2],
            "target_sha": _READINESS_A[2],
            "action": "deploy",
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": module._lab_installation_identity(lock_path, installation),
            "supersedes_operation_id": "",
        },
    )
    intent = _publish_handoff_generation_authority(
        module,
        lock_path,
        handoff_operation_id=old_operation,
    )
    intent_payload = asdict(intent)
    intent_payload.update(intent_mutation)
    module._atomic_private_json(intent_path_for_lock(lock_path), intent_payload)
    launchctl_calls: list[list[str]] = []
    monkeypatch.setattr(
        module,
        "_launchctl",
        lambda arguments, **_kwargs: launchctl_calls.append(arguments),
    )

    with pytest.raises(module.DeployBootstrapError, match="intent|policy|classification"):
        module._superseding_handoff_operation_id(
            root=root,
            lock_path=lock_path,
            recovery_action="resume",
            release_profile="macos-lab",
            lifecycle_mode="installed",
        )

    assert launchctl_calls == []


def test_successful_lab_handoff_restore_uses_original_overall_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        *,
        check: bool,
        timeout_seconds: float,
    ) -> subprocess.CompletedProcess[str]:
        del check, timeout_seconds
        action = arguments[0]
        if action == "print":
            label = arguments[-1].rsplit("/", 1)[-1]
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            loaded.remove(arguments[-1].rsplit("/", 1)[-1])
        else:
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module.sys, "platform", "darwin")
    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", lambda **_kwargs: _READINESS_A)
    handoff = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1.0,
        overall_timeout_seconds=1.0,
    )
    handoff.prepare(
        dry_run=False,
        target_ref="a" * 40,
        target_sha="a" * 40,
        action="deploy",
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    handoff.deadline = time.monotonic() - 1

    with pytest.raises(module.DeployBootstrapError, match="overall timeout"):
        handoff.restore()

    assert loaded == set()


def test_readiness_failure_stops_target_then_rolls_back_and_restores_previous(
    tmp_path: Path,
) -> None:
    module = _bootstrap_module()
    events: list[str] = []

    class TargetHandoff:
        def restore(self) -> None:
            events.append("target-readiness-failed")
            raise module.DeployBootstrapError("target readiness failed")

    class RecoveryHandoff:
        def prepare(
            self,
            *,
            dry_run: bool,
            target_ref: str,
            target_sha: str,
            action: str,
            now: datetime | None = None,
        ) -> None:
            assert target_ref == target_sha == "f" * 40
            assert action == "rollback"
            del dry_run, now
            events.append("target-daemons-stopped")

        def restore(self) -> None:
            events.append("previous-daemons-ready")

        def close(self) -> None:
            events.append("recovery-closed")

    recovery_handoff = RecoveryHandoff()

    def rollback(_handoff: object) -> int:
        assert _handoff is recovery_handoff
        assert events[-1] == "target-daemons-stopped"
        events.append("previous-generation-restored")
        return 0

    def finalize(_handoff: object) -> None:
        assert _handoff is recovery_handoff
        events.append("rollback-committed")

    result = module._complete_installed_rollout(
        target_handoff=TargetHandoff(),
        deploy_code=0,
        recovery_handoff_factory=lambda: recovery_handoff,
        rollback=rollback,
        finalize_readiness=finalize,
        recovery_target_sha="f" * 40,
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert result == 1
    assert events == [
        "target-readiness-failed",
        "target-daemons-stopped",
        "previous-generation-restored",
        "previous-daemons-ready",
        "rollback-committed",
    ]


def test_successful_target_readiness_is_finalized_before_rollout_returns() -> None:
    module = _bootstrap_module()
    events: list[str] = []

    class TargetHandoff:
        def restore(self) -> None:
            events.append("target-ready")

    target = TargetHandoff()
    result = module._complete_installed_rollout(
        target_handoff=target,
        deploy_code=0,
        recovery_handoff_factory=lambda: pytest.fail("recovery must not run"),
        rollback=lambda _handoff: pytest.fail("rollback must not run"),
        finalize_readiness=lambda handoff: events.append(
            "transaction-completed" if handoff is target else "wrong-target"
        ),
        recovery_target_sha="f" * 40,
        transition_installation=lambda handoff: events.append(
            "target-plists-installed" if handoff is target else "wrong-target"
        ),
    )

    assert result == 0
    assert events == ["target-plists-installed", "target-ready", "transaction-completed"]


def test_readiness_failure_transitions_plists_back_before_previous_bootstrap() -> None:
    module = _bootstrap_module()
    events: list[str] = []

    class Target:
        def restore(self) -> None:
            events.append("bootstrap-b")
            raise module.DeployBootstrapError("B is unhealthy")

    class Recovery:
        def prepare(self, **_kwargs: object) -> None:
            events.append("stop-b")

        def restore(self) -> None:
            events.append("bootstrap-a")

        def close(self) -> None:
            events.append("close-recovery")

    recovery = Recovery()

    def transition(handoff: object) -> None:
        events.append("install-a-plists" if handoff is recovery else "install-b-plists")

    result = module._complete_installed_rollout(
        target_handoff=Target(),
        deploy_code=0,
        recovery_handoff_factory=lambda: recovery,
        rollback=lambda _handoff: events.append("restore-a-generation") or 0,
        finalize_readiness=lambda _handoff: events.append("commit-a"),
        transition_installation=transition,
        recovery_target_sha="a" * 40,
    )

    assert result == 1
    assert events == [
        "install-b-plists",
        "bootstrap-b",
        "stop-b",
        "restore-a-generation",
        "install-a-plists",
        "bootstrap-a",
        "commit-a",
    ]


def test_nonzero_deployer_exit_uses_formal_rollback_before_previous_readiness() -> None:
    module = _bootstrap_module()
    events: list[str] = []

    class TargetHandoff:
        def restore(self) -> None:
            pytest.fail("failed deploy must not restore or complete the target handoff")

        def close(self) -> None:
            events.append("target-closed")

    class RecoveryHandoff:
        def prepare(self, **kwargs: object) -> None:
            assert kwargs["target_ref"] == kwargs["target_sha"] == "f" * 40
            assert kwargs["action"] == "rollback"
            events.append("recovery-prepare")

        def restore(self) -> None:
            events.append("previous-ready")

        def close(self) -> None:
            events.append("recovery-closed")

    recovery = RecoveryHandoff()

    def rollback(handoff: object) -> int:
        assert handoff is recovery
        events.append("formal-rollback")
        return 0

    def finalize(handoff: object) -> None:
        assert handoff is recovery
        events.append("rollback-committed")

    result = module._complete_installed_rollout(
        target_handoff=TargetHandoff(),
        deploy_code=1,
        recovery_handoff_factory=lambda: recovery,
        rollback=rollback,
        finalize_readiness=finalize,
        recovery_target_sha="f" * 40,
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert result == 1
    assert events == [
        "target-closed",
        "recovery-prepare",
        "formal-rollback",
        "previous-ready",
        "rollback-committed",
    ]


def test_nonzero_deployer_uses_real_superseding_handoff_recovery_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    loaded = set(module.LAB_LAUNCHD_LABELS)

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        action = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if action == "bootout":
            loaded.remove(label)
        elif action == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    now = datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    target = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    target.prepare(
        dry_run=False,
        target_ref="b" * 40,
        target_sha="b" * 40,
        action="deploy",
        now=now,
    )
    deployment_operation = "7" * 32
    intent = _handoff_deployment_intent(
        module,
        handoff_operation_id=target.operation_id,
        operation_id=deployment_operation,
        previous_sha="a" * 40,
        target_sha="b" * 40,
        target_ref="b" * 40,
        stage="awaiting_readiness",
    )
    module._atomic_private_json(intent_path_for_lock(lock_path), asdict(intent), absent=True)
    recovery = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=target.operation_id,
    )
    monkeypatch.setattr(
        module,
        "_wait_for_lab_readiness",
        lambda **_kwargs: (deployment_operation, "8" * 64, "a" * 40),
    )

    def rollback(active: object) -> int:
        assert active is recovery
        recovering = intent.advance(stage="recovery_started")
        rebound = recovering.rebind_handoff(
            handoff_operation_id=recovery.operation_id,
            handoff_labels=tuple(module.LAB_LAUNCHD_LABELS),
        )
        module._atomic_private_json(intent_path_for_lock(lock_path), asdict(rebound))
        return 0

    def finalize(active: object) -> None:
        assert active is recovery
        proof = module._private_json(
            module._completed_handoff_path(lock_path, recovery.operation_id),
            label="completed rollback handoff proof",
        )
        assert proof is not None
        assert proof["action"] == "rollback"
        assert proof["supersedes_operation_id"] == target.operation_id

    result = module._complete_installed_rollout(
        target_handoff=target,
        deploy_code=1,
        recovery_handoff_factory=lambda: recovery,
        rollback=rollback,
        finalize_readiness=finalize,
        recovery_target_sha="a" * 40,
        now=now,
    )

    assert result == 1
    assert loaded == set(module.LAB_LAUNCHD_LABELS)


@pytest.mark.parametrize("source_action", ["resume", "rollback"])
def test_recovery_readiness_failure_rolls_back_through_superseded_handoff_chain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source_action: str,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    monkeypatch.setattr(module.sys, "platform", "darwin")
    source_operation = "4" * 32
    prior_operation = "3" * 32
    loaded = set(module.LAB_LAUNCHD_LABELS)
    installation = module._read_lab_installation_state(root=root, lock_path=lock_path)
    source_ref = "v0.99.1" if source_action == "resume" else "a" * 40
    source_sha = "b" * 40 if source_action == "resume" else "a" * 40
    module._atomic_private_json(
        lock_path.with_name(f"{lock_path.stem}.intent.json"),
        asdict(
            _handoff_deployment_intent(
                module,
                handoff_operation_id=source_operation,
                operation_id="5" * 32,
                previous_sha="a" * 40,
                target_sha="b" * 40,
                target_ref="v0.99.1",
                stage="services_transitioning",
            )
        ),
        absent=True,
    )
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {
            "schema_version": module.LAB_HANDOFF_SCHEMA_VERSION,
            "operation_id": source_operation,
            "checkout_root": str(root),
            "stage": "restarting",
            "labels": list(module.LAB_LAUNCHD_LABELS),
            "loaded_labels": list(module.LAB_LAUNCHD_LABELS),
            "stopped_labels": list(module.LAB_LAUNCHD_LABELS),
            "restarted_labels": list(module.LAB_LAUNCHD_LABELS),
            "updated_at": "2026-07-28T00:00:00+00:00",
            "target_ref": source_ref,
            "target_sha": source_sha,
            "action": source_action,
            "release_profile": "macos-lab",
            "lifecycle_mode": "installed",
            "installation_identity": module._lab_installation_identity(lock_path, installation),
            "supersedes_operation_id": prior_operation,
        },
    )

    def fake_launchctl(
        arguments: list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = arguments[0]
        label = arguments[-1].rsplit("/", 1)[-1]
        if command == "print":
            return subprocess.CompletedProcess(
                arguments,
                0 if label in loaded else 113,
                stdout="state = running\n" if label in loaded else "",
                stderr="",
            )
        if command == "bootout":
            loaded.remove(label)
        elif command == "bootstrap":
            loaded.add(Path(arguments[-1]).stem)
        return subprocess.CompletedProcess(arguments, 0, stdout="", stderr="")

    monkeypatch.setattr(module, "_launchctl", fake_launchctl)
    monkeypatch.setattr(module, "_wait_for_lab_readiness", lambda **_kwargs: _READINESS_A)

    class FailedTargetHandoff:
        def restore(self) -> None:
            raise module.DeployBootstrapError("target readiness failed")

    rollback_handoff = module._LabLaunchdHandoff(
        root=root,
        lock_path=lock_path,
        timeout_seconds=1,
        supersedes_operation_id=source_operation,
    )

    result = module._complete_installed_rollout(
        target_handoff=FailedTargetHandoff(),
        deploy_code=0,
        recovery_handoff_factory=lambda: rollback_handoff,
        rollback=lambda _handoff: 0,
        finalize_readiness=lambda _handoff: None,
        recovery_target_sha="a" * 40,
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert result == 1
    persisted = module._private_json(rollback_handoff.record_path, label="Lab handoff state")
    assert persisted is not None
    assert persisted["stage"] == "completed"
    assert persisted["action"] == "rollback"
    assert persisted["supersedes_operation_id"] == source_operation
    assert loaded == set(module.LAB_LAUNCHD_LABELS)


def test_deploy_control_dotenv_reader_is_allowlisted_and_never_evaluates_shell(
    tmp_path: Path,
) -> None:
    module = _bootstrap_module()
    marker = tmp_path / "must-not-exist"
    env_path = tmp_path / ".env"
    env_path.write_text(
        "TUSHARE_TOKEN='secret'\n"
        "RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS=17\n"
        "RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS='91'\n"
        "RQUANT_LAB_LIFECYCLE_MODE=uninstalled\n"
        "RQUANT_DEPLOY_UV=/opt/homebrew/bin/uv\n"
        "LAB_TRUSTED_GIT_PATH=/usr/bin/git\n"
        f"UNRELATED=$({marker})\n",
        encoding="utf-8",
    )
    env_path.chmod(0o600)

    controls = module._read_deploy_controls(env_path)

    assert controls == {
        "LAB_TRUSTED_GIT_PATH": "/usr/bin/git",
        "RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS": "17",
        "RQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS": "91",
        "RQUANT_DEPLOY_UV": "/opt/homebrew/bin/uv",
        "RQUANT_LAB_LIFECYCLE_MODE": "uninstalled",
    }
    assert not marker.exists()


@pytest.mark.parametrize(
    "payload",
    (
        '{"schema_version":1,"schema_version":2}',
        '{"outer":{"operation_id":"a","operation_id":"b"}}',
    ),
)
def test_bootstrap_private_json_rejects_duplicate_keys(
    tmp_path: Path,
    payload: str,
) -> None:
    module = _bootstrap_module()
    record = tmp_path / "record.json"
    record.write_text(payload, encoding="utf-8")
    record.chmod(0o600)

    with pytest.raises(module.DeployBootstrapError, match="duplicate JSON key"):
        module._private_json(record, label="duplicate record")


def test_bootstrap_dotenv_and_prepared_sentinel_reads_use_openat_bound_descriptors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checkout, _python, lock_path, _commit = _checkout(tmp_path)
    module = _bootstrap_module()
    dotenv = checkout / ".env"
    dotenv.write_text("RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS=30\n", encoding="utf-8")
    dotenv.chmod(0o600)
    installation = module._read_lab_installation_state(root=checkout, lock_path=lock_path)
    sentinel = Path(str(installation["runtime_root"])) / module.LAB_RUNTIME_PREPARED_FILENAME
    original_open = os.open
    observed: list[tuple[object, int | None]] = []

    def capture_open(
        path: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        if path in {dotenv, dotenv.name, sentinel, sentinel.name}:
            observed.append((path, dir_fd))
        if dir_fd is None:
            return original_open(path, flags, mode)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(os, "open", capture_open)

    assert module._read_deploy_controls(dotenv)["RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS"] == "30"
    assert module._private_json(sentinel, label="Lab runtime prepared sentinel")
    assert any(path == dotenv.name and dir_fd is not None for path, dir_fd in observed)
    assert any(path == sentinel.name and dir_fd is not None for path, dir_fd in observed)
    assert all(path not in {dotenv, sentinel} for path, _dir_fd in observed)


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ("RQUANT_DEPLOY_COMMAND_TIMEOUT_SECOND=30\n", "unknown deployment dotenv key"),
        ("RQUANT_LAB_LIFECYCLE_MOD=installed\n", "unknown deployment dotenv key"),
        ("RQUANT_RELEASE_GENERATION_MIN_FREE_BYTE=1\n", "unknown deployment dotenv key"),
        ("LAB_TRUSTED_GIT_PAT=/usr/bin/git\n", "unknown deployment dotenv key"),
        (
            "RQUANT_DAILY_RECEIPT_ACTIVE_KEY_ID=daily-v1\n",
            "Daily receipt authority cannot be configured",
        ),
        (
            "RQ_DAILY_SHADOW_RECEIPT_SIGNER_COMMAND=/tmp/signer\n",
            "Daily receipt authority cannot be configured",
        ),
        ("RQUANT_DEPLOY_UV\n", "requires '='"),
        ("RQUANT_DEPLOY_UV='/opt/homebrew/bin/uv\n", "value is invalid"),
        (
            "RQUANT_DEPLOY_UV=/opt/homebrew/bin/uv\nRQUANT_DEPLOY_UV=/usr/local/bin/uv\n",
            "duplicate deployment dotenv key",
        ),
    ],
)
def test_deploy_control_dotenv_fails_closed_for_namespaced_typos_and_malformed_lines(
    tmp_path: Path,
    payload: str,
    message: str,
) -> None:
    module = _bootstrap_module()
    env_path = tmp_path / ".env"
    env_path.write_text("TUSHARE_TOKEN\n" + payload, encoding="utf-8")
    env_path.chmod(0o600)

    with pytest.raises(module.DeployBootstrapError, match=message):
        module._read_deploy_controls(env_path)


def test_bootstrap_applies_repo_dotenv_deploy_timeout_controls(tmp_path: Path) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    dotenv = checkout / ".env"
    dotenv.write_text(
        "RQUANT_DEPLOY_COMMAND_TIMEOUT_SECONDS=0\nRQUANT_DEPLOY_OVERALL_TIMEOUT_SECONDS=60\n",
        encoding="utf-8",
    )
    dotenv.chmod(0o600)

    result = subprocess.run(
        _command(checkout, python, lock_path),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )

    assert result.returncode == 2
    assert "deployment timeout configuration is invalid" in result.stderr


def test_bootstrap_frozen_sync_timeout_terminates_uv_process_group(tmp_path: Path) -> None:
    module = _bootstrap_module()
    marker = tmp_path / "uv-descendant-survived"
    fake_uv = tmp_path / "uv"
    fake_uv.write_text(
        f"#!{sys.executable}\n"
        "import subprocess, sys, time\n"
        "subprocess.Popen([sys.executable, '-c', "
        f'"import pathlib,time;time.sleep(0.4);'
        f"pathlib.Path({str(marker)!r}).write_text('alive')\"])\n"
        "time.sleep(5)\n",
        encoding="utf-8",
    )
    fake_uv.chmod(0o700)

    with pytest.raises(module.DeployBootstrapError, match="could not run"):
        module._run_frozen_sync(tmp_path, fake_uv, timeout_seconds=0.1)
    time.sleep(0.6)

    assert not marker.exists()


def test_bootstrap_runner_timeout_contains_detached_grandchild(tmp_path: Path) -> None:
    module = _bootstrap_module()
    marker = tmp_path / "detached-grandchild-survived"
    grandchild = (
        "import sys,time; from pathlib import Path; "
        "time.sleep(1); Path(sys.argv[1]).write_text('late')"
    )
    child = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable,'-c',{grandchild!r},sys.argv[1]],"
        "start_new_session=True); time.sleep(5)"
    )

    started = time.monotonic()
    with pytest.raises(subprocess.TimeoutExpired):
        module._run_process_group(
            [sys.executable, "-c", child, str(marker)],
            cwd=tmp_path,
            timeout_seconds=0.5,
        )
    assert time.monotonic() - started < 1
    time.sleep(1.1)

    assert not marker.exists()


def test_generation_preflight_is_clipped_by_end_to_end_deadline(tmp_path: Path) -> None:
    module = _bootstrap_module()
    launcher = tmp_path / ".venv" / "bin" / "rquant"
    launcher.parent.mkdir(parents=True)
    launcher.write_text(f"#!{sys.executable}\nimport time\ntime.sleep(5)\n", encoding="utf-8")
    launcher.chmod(0o700)

    started = time.monotonic()
    with pytest.raises(module.DeployBootstrapError, match="preflight.*timeout"):
        module._run_generation_preflight(
            tmp_path,
            timeout_seconds=5,
            overall_deadline_monotonic=time.monotonic() + 0.1,
        )

    assert time.monotonic() - started < 1


def test_env_example_is_strictly_parseable_for_mac_and_linux_profiles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _bootstrap_module()
    env_path = tmp_path / ".env"
    env_path.write_bytes((ROOT / ".env.example").read_bytes())
    env_path.chmod(0o600)

    controls = module._read_deploy_controls(env_path)
    module._validate_profile_controls(
        controls,
        release_profile="macos-lab",
        host_platform="darwin",
    )
    module._validate_profile_controls(
        controls,
        release_profile="linux-production",
        host_platform="linux",
    )

    assert controls.get("RQUANT_RELEASE_PROFILE", "") == ""
    for host_platform, release_profile in (
        ("darwin", "macos-lab"),
        ("linux", "linux-production"),
    ):
        monkeypatch.setattr(sys, "platform", host_platform)
        command = _command(
            ROOT,
            Path(sys.executable),
            tmp_path / "deploy.lock",
            target="a" * 40,
        )
        assert command[command.index("--release-profile") + 1] == release_profile
        assert command[command.index("--host-platform") + 1] == host_platform


def test_generation_docs_describe_rebuilt_venv_and_initialize_restart_contract() -> None:
    lab_doc = (ROOT / "docs" / "lab-daemon-release-generation.md").read_text(encoding="utf-8")
    production_doc = (ROOT / "docs" / "production-release.md").read_text(encoding="utf-8")

    assert "uv venv --relocatable" in production_doc
    assert "uv sync --frozen --active" in production_doc
    assert "把实际环境复制到" not in lab_doc
    assert "把实际环境复制到" not in production_doc
    assert "--initialize-generation --target <the-same-recorded-exact-target>" in lab_doc
    assert "不得改用 `--recover-generation`" in production_doc


def test_production_runbook_uses_the_controlled_job_authority_prepare_chain() -> None:
    production_doc = (ROOT / "docs" / "production-release.md").read_text(encoding="utf-8")

    for control in (
        "RQUANT_RUNTIME_PRODUCTION_INPUTS",
        "RQUANT_RUNTIME_PROFILE_OUTPUT_DIR",
        "RQUANT_RUNTIME_ROOT",
    ):
        assert f"export {control}=" in production_doc
    assert '"${ROOT}/scripts/run-lab-daemon.py"' in production_doc
    assert '-- "${ROOT}/.venv/bin/rquant" lab-runtime-prepare' in production_doc
    assert "调用方不能覆盖" in production_doc
    assert "任何 target scheduler/daemon restart 前进入 rollback" in production_doc


def test_deploy_bootstrap_holds_exclusive_generation_before_project_import(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    first_import = tmp_path / "first-import"
    first_run = tmp_path / "first-run"
    second_import = tmp_path / "second-import"
    second_run = tmp_path / "second-run"
    first_env = {
        **os.environ,
        "DEPLOY_LOCK": str(lock_path),
        "IMPORT_MARKER": str(first_import),
        "RUN_MARKER": str(first_run),
        "DEPLOY_HOLD_SECONDS": "1.0",
    }
    first = subprocess.Popen(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env=first_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 5
    while not first_import.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert first_import.read_text(encoding="utf-8") == "locked"
    second = subprocess.run(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env={
            **os.environ,
            "DEPLOY_LOCK": str(lock_path),
            "IMPORT_MARKER": str(second_import),
            "RUN_MARKER": str(second_run),
        },
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    first_stdout, first_stderr = first.communicate(timeout=5)

    assert first.returncode == 0, first_stdout + first_stderr
    assert first_run.read_text(encoding="utf-8") == "ran"
    assert second.returncode == 2
    assert "generation is active" in second.stderr
    assert not second_import.exists()
    assert not second_run.exists()


def test_normal_deploy_fetches_before_resolving_first_seen_annotated_tag(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    (checkout / "pyproject.toml").write_text(
        '[project]\nname = "rquant"\nversion = "1.0.0"\n',
        encoding="utf-8",
    )
    (checkout / "uv.lock").write_text("version = 2\n", encoding="utf-8")
    _git(checkout, "add", "pyproject.toml", "uv.lock")
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "tagged release",
    )
    target = _git(checkout, "rev-parse", "HEAD")
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "tag",
        "-a",
        "v1.0.0",
        "-m",
        "v1.0.0",
    )
    _git(checkout, "push", "origin", "main", "refs/tags/v1.0.0")
    _git(checkout, "reset", "--hard", previous)
    _git(checkout, "tag", "-d", "v1.0.0")
    _git(checkout, "update-ref", "refs/remotes/origin/main", previous)

    result = subprocess.run(
        _command(checkout, python, lock_path, target="v1.0.0"),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "v1.0.0^{commit}") == target


def test_installed_already_current_dry_run_does_not_require_loaded_labels(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    daemon_lock = os.open(lock_path, os.O_RDONLY)
    fcntl.flock(daemon_lock, fcntl.LOCK_SH | fcntl.LOCK_NB)
    command = _command(checkout, python, lock_path, lifecycle_mode="installed")
    command.append("--dry-run")
    try:
        result = subprocess.run(
            command,
            cwd=checkout,
            env=os.environ,
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
    finally:
        os.close(daemon_lock)

    assert result.returncode == 0
    assert '"status": "already_current"' in result.stdout


def test_uninstalled_deploy_dry_run_leaves_git_and_authority_trees_byte_identical(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        lifecycle_mode="uninstalled",
    )
    command.append("--dry-run")
    git_before = _tree_snapshot(checkout / ".git")
    authority_before = _tree_snapshot(lock_path.parent)

    result = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert _tree_snapshot(checkout / ".git") == git_before
    assert _tree_snapshot(lock_path.parent) == authority_before


def test_uninstalled_deploy_dry_run_never_creates_missing_lock_parent(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        lifecycle_mode="uninstalled",
    )
    command.append("--dry-run")
    missing_lock = tmp_path / "absent-coordination" / "production.lock"
    command[command.index("--deployment-lock-path") + 1] = str(missing_lock)

    result = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert not missing_lock.parent.exists()


def test_initialize_generation_publishes_first_marker_without_importing_deployer(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    imported = tmp_path / "imported"
    ran = tmp_path / "ran"

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="initialize",
        ),
        cwd=checkout,
        env={
            **os.environ,
            "DEPLOY_LOCK": str(lock_path),
            "IMPORT_MARKER": str(imported),
            "RUN_MARKER": str(ran),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    marker_path = marker_path_for_lock(lock_path)
    assert marker_path.is_file()
    assert not imported.exists()
    assert not ran.exists()
    marker = ReleaseGenerationMarker.from_payload(
        json.loads(marker_path.read_text(encoding="utf-8"))
    )
    source_launcher = checkout / ".venv" / "bin" / "rquant-test-console"
    generated_launcher = Path(marker.venv_path) / "bin" / "rquant-test-console"
    assert source_launcher.read_bytes().splitlines()[0] == f"#!{python}".encode()
    assert generated_launcher.read_bytes().splitlines()[0] == (
        f"#!{marker.venv_path}/bin/python".encode()
    )
    assert str(checkout / ".venv").encode() not in generated_launcher.read_bytes().splitlines()[0]


def test_initialize_generation_does_not_require_launchd_installation(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(
        tmp_path,
        publish_marker=False,
        install_lab=False,
    )

    result = subprocess.run(
        _command(checkout, python, lock_path, target=commit, mode="initialize"),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert marker_path_for_lock(lock_path).is_file()


def test_register_lab_installation_requires_explicit_prepared_runtime(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, install_state=False)
    runtime_root = checkout / "data" / "lab-runtime"
    readiness_root = runtime_root / "readiness"
    readiness_root.mkdir(parents=True, mode=0o700)
    runtime_root.chmod(0o700)
    readiness_root.chmod(0o700)
    command = _command(checkout, python, lock_path, target=commit, mode="register")
    separator = command.index("--")
    command[separator:separator] = [
        "--lab-runtime-root",
        str(runtime_root),
        "--lab-readiness-root",
        str(readiness_root),
    ]

    result = subprocess.run(
        command,
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 2
    assert "prepared sentinel" in result.stderr
    installation = lock_path.with_name(f"{lock_path.stem}.lab-install.json")
    assert not installation.exists()

    from rquant.lab_daemon import (
        prepare_lab_runtime_layout,
        prepare_private_sqlite_path,
        register_lab_runtime_managed_file,
    )

    directories = {
        "lab command spool": runtime_root / "commands",
        "lab claim spool": runtime_root / "claims",
        "lab report spool": runtime_root / "reports",
        "lab worker artifact root": runtime_root / "worker-artifacts",
        "lab final artifact root": runtime_root / "final-artifacts",
        "lab artifact commit spool": runtime_root / "artifact-commits",
        "lab daemon lock root": runtime_root / "locks",
        "lab finalizer state root": runtime_root / "finalizer-state",
        "lab readiness root": readiness_root,
    }
    prepare_lab_runtime_layout(
        runtime_root,
        checkout_root=checkout,
        managed_directories=directories,
        managed_files={"lab jobs SQLite": runtime_root / "lab_jobs.sqlite3"},
        legacy_paths={},
        mutation_guard=lambda: commit,
    )
    database = runtime_root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(
        database,
        label="lab jobs SQLite",
        create=True,
        mutation_guard=lambda: commit,
    )
    try:
        register_lab_runtime_managed_file(
            runtime_root,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: commit,
        )
    finally:
        authority.close()
    accepted = subprocess.run(
        command,
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert accepted.returncode == 0, accepted.stderr
    installation_payload = json.loads(installation.read_text(encoding="utf-8"))
    assert installation_payload["registered_by_commit"] == commit
    assert installation_payload["prepared_authority"]["runtime_authority_id"]


def test_register_lab_installation_dry_run_never_rewrites_installation_state(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, install_state=False)
    runtime_root = checkout / "data" / "lab-runtime"
    readiness_root = runtime_root / "readiness"
    from rquant.lab_daemon import (
        prepare_lab_runtime_layout,
        prepare_private_sqlite_path,
        register_lab_runtime_managed_file,
    )

    directories = {
        "lab command spool": runtime_root / "commands",
        "lab claim spool": runtime_root / "claims",
        "lab report spool": runtime_root / "reports",
        "lab worker artifact root": runtime_root / "worker-artifacts",
        "lab final artifact root": runtime_root / "final-artifacts",
        "lab artifact commit spool": runtime_root / "artifact-commits",
        "lab daemon lock root": runtime_root / "locks",
        "lab finalizer state root": runtime_root / "finalizer-state",
        "lab readiness root": readiness_root,
    }
    prepare_lab_runtime_layout(
        runtime_root,
        checkout_root=checkout,
        managed_directories=directories,
        managed_files={"lab jobs SQLite": runtime_root / "lab_jobs.sqlite3"},
        legacy_paths={},
        mutation_guard=lambda: commit,
    )
    database = runtime_root / "lab_jobs.sqlite3"
    authority = prepare_private_sqlite_path(
        database,
        label="lab jobs SQLite",
        create=True,
        mutation_guard=lambda: commit,
    )
    try:
        register_lab_runtime_managed_file(
            runtime_root,
            label="lab jobs SQLite",
            path=database,
            mutation_guard=lambda: commit,
        )
    finally:
        authority.close()
    command = _command(checkout, python, lock_path, target=commit, mode="register")
    separator = command.index("--")
    command[separator:separator] = [
        "--lab-runtime-root",
        str(runtime_root),
        "--lab-readiness-root",
        str(readiness_root),
    ]
    created = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )
    assert created.returncode == 0, created.stderr
    installation = lock_path.with_name(f"{lock_path.stem}.lab-install.json")
    before = installation.read_bytes()
    before_stat = installation.stat()
    dry_run_command = [*command, "--dry-run"]

    preview = subprocess.run(
        dry_run_command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    after_stat = installation.stat()
    assert preview.returncode == 0, preview.stderr
    assert installation.read_bytes() == before
    assert (after_stat.st_ino, after_stat.st_mtime_ns) == (
        before_stat.st_ino,
        before_stat.st_mtime_ns,
    )


def test_identical_lab_installation_reregistration_preserves_authority_identity(
    tmp_path: Path,
) -> None:
    checkout, _python, lock_path, commit = _checkout(tmp_path)
    module = _bootstrap_module()
    path = module._stable_record_path(lock_path, "lab-install")
    existing = module._read_lab_installation_state(root=checkout, lock_path=lock_path)
    before = path.read_bytes()
    before_stat = path.stat()

    returned = module._write_lab_installation_state(
        root=checkout,
        lock_path=lock_path,
        runtime_root=Path(str(existing["runtime_root"])),
        readiness_root=Path(str(existing["readiness_root"])),
        expected_commit=commit,
    )

    after_stat = path.stat()
    assert returned == existing
    assert path.read_bytes() == before
    assert (after_stat.st_ino, after_stat.st_mtime_ns) == (
        before_stat.st_ino,
        before_stat.st_mtime_ns,
    )


def test_changed_lab_installation_requires_separate_migration_without_staling_authority(
    tmp_path: Path,
) -> None:
    checkout, _python, lock_path, commit = _checkout(tmp_path)
    module = _bootstrap_module()
    path = module._stable_record_path(lock_path, "lab-install")
    existing = module._read_lab_installation_state(root=checkout, lock_path=lock_path)
    before = path.read_bytes()
    module._atomic_private_json(
        module._stable_record_path(lock_path, "lab-handoff"),
        {"authority": "completed-deployment"},
        absent=True,
    )
    plist = checkout / "deploy" / "launchd" / f"{module.LAB_LAUNCHD_LABELS[0]}.plist"
    plist.write_text("<plist><dict><key>Changed</key><true/></dict></plist>", encoding="utf-8")

    with pytest.raises(module.DeployBootstrapError, match="separate.*migration"):
        module._write_lab_installation_state(
            root=checkout,
            lock_path=lock_path,
            runtime_root=Path(str(existing["runtime_root"])),
            readiness_root=Path(str(existing["readiness_root"])),
            expected_commit=commit,
        )

    assert path.read_bytes() == before


def test_register_lab_installation_dry_run_never_creates_missing_lock(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, install_state=False)
    runtime_root = checkout / "data" / "lab-runtime"
    readiness_root = runtime_root / "readiness"
    from rquant.lab_daemon import prepare_lab_runtime_layout

    directories = {
        "lab command spool": runtime_root / "commands",
        "lab claim spool": runtime_root / "claims",
        "lab report spool": runtime_root / "reports",
        "lab worker artifact root": runtime_root / "worker-artifacts",
        "lab final artifact root": runtime_root / "final-artifacts",
        "lab artifact commit spool": runtime_root / "artifact-commits",
        "lab daemon lock root": runtime_root / "locks",
        "lab finalizer state root": runtime_root / "finalizer-state",
        "lab readiness root": readiness_root,
    }
    prepare_lab_runtime_layout(
        runtime_root,
        checkout_root=checkout,
        managed_directories=directories,
        managed_files={"lab jobs SQLite": runtime_root / "lab_jobs.sqlite3"},
        legacy_paths={},
        mutation_guard=lambda: commit,
    )
    command = _command(checkout, python, lock_path, target=commit, mode="register")
    separator = command.index("--")
    command[separator:separator] = [
        "--lab-runtime-root",
        str(runtime_root),
        "--lab-readiness-root",
        str(readiness_root),
    ]
    command.append("--dry-run")
    lock_path.unlink()
    before = {
        path.relative_to(lock_path.parent).as_posix(): (
            path.lstat().st_ino,
            path.lstat().st_mtime_ns,
            path.read_bytes() if path.is_file() else b"",
        )
        for path in lock_path.parent.rglob("*")
    }

    result = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    after = {
        path.relative_to(lock_path.parent).as_posix(): (
            path.lstat().st_ino,
            path.lstat().st_mtime_ns,
            path.read_bytes() if path.is_file() else b"",
        )
        for path in lock_path.parent.rglob("*")
    }
    assert result.returncode == 2
    assert "lock" in result.stderr.lower()
    assert not lock_path.exists()
    assert after == before


@pytest.mark.parametrize(
    ("mode", "command"),
    [("resume", "merge"), ("rollback", "reset")],
)
def test_generation_checkout_mutations_use_process_group_and_git_write_locking(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    command: str,
) -> None:
    module = _bootstrap_module()
    repo = tmp_path / "rquant"
    repo.mkdir()
    current = "a" * 40 if mode == "resume" else "b" * 40
    target = "b" * 40 if mode == "resume" else "a" * 40
    process_calls: list[tuple[list[str], dict[str, str]]] = []
    monkeypatch.setattr(module, "_git_head", lambda *_args, **_kwargs: current)
    monkeypatch.setattr(
        module,
        "_git_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, "", ""),
    )

    def fake_process_group(
        arguments: list[str],
        *,
        cwd: Path,
        timeout_seconds: float,
        env: dict[str, str],
    ) -> subprocess.CompletedProcess[str]:
        assert cwd == repo
        assert timeout_seconds > 0
        process_calls.append((arguments, env))
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr(module, "_run_process_group", fake_process_group)

    module._prepare_generation_checkout(
        root=repo,
        git_path=Path("/usr/bin/git"),
        target_commit=target,
        mode=mode,
        overall_deadline_monotonic=time.monotonic() + 10,
    )

    assert len(process_calls) == 1
    arguments, environment = process_calls[0]
    assert arguments[1] == command
    assert environment["GIT_OPTIONAL_LOCKS"] == "1"


def test_read_only_git_verification_disables_optional_locks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _bootstrap_module()
    captured: list[dict[str, str]] = []

    def fake_run(*_args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        captured.append(dict(kwargs["env"]))
        return subprocess.CompletedProcess([], 0, "a" * 40 + "\n", "")

    monkeypatch.setattr(module, "_run_process_group", fake_run)

    module._git_run(tmp_path, Path("/usr/bin/git"), "rev-parse", "HEAD")

    assert captured == [{**os.environ, "GIT_OPTIONAL_LOCKS": "0", "GIT_TERMINAL_PROMPT": "0"}]


def test_lab_installation_registration_rejects_tampered_prepared_sentinel(
    tmp_path: Path,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    runtime_root = root / "data" / "lab-runtime"
    readiness_root = runtime_root / "readiness"
    runtime_root.mkdir(parents=True, mode=0o700)
    runtime_root.chmod(0o700)
    readiness_root.mkdir(mode=0o700)
    readiness_root.chmod(0o700)
    sentinel = runtime_root / ".prepared.json"
    sentinel.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "checkout_root": str(root),
                "runtime_root": str(runtime_root),
                "runtime_authority_id": "a" * 32,
                "prepared_by_commit": "a" * 40,
                "managed_directories": {},
                "managed_files": {},
                "migration_sources": {},
                "runtime_device": runtime_root.stat().st_dev,
                "runtime_inode": runtime_root.stat().st_ino + 1,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    sentinel.chmod(0o600)

    with pytest.raises(module.DeployBootstrapError, match="prepared sentinel"):
        module._write_lab_installation_state(
            root=root,
            lock_path=lock_path,
            runtime_root=runtime_root,
            readiness_root=readiness_root,
            expected_commit="a" * 40,
        )


def test_initialize_generation_accepts_uv_style_symlinked_python(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    python.unlink()
    python.symlink_to(checkout / ".test-system-python")

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="initialize",
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert marker_path_for_lock(lock_path).is_file()


def test_uv_resolution_accepts_verified_homebrew_symlink_chain(
    tmp_path: Path,
) -> None:
    module = _bootstrap_module()
    physical = tmp_path / "Cellar" / "uv" / "1.0" / "bin" / "uv"
    physical.parent.mkdir(parents=True)
    physical.write_bytes(Path("/usr/bin/true").read_bytes())
    physical.chmod(0o700)
    homebrew_bin = tmp_path / "homebrew" / "bin"
    homebrew_bin.mkdir(parents=True)
    candidate = homebrew_bin / "uv"
    candidate.symlink_to(Path("../../Cellar/uv/1.0/bin/uv"))

    resolved, binding = module._resolve_uv_path(str(candidate))

    assert resolved == physical
    assert binding["configured_path"] == str(candidate)
    assert binding["physical_path"] == str(physical)
    assert binding["sha256"] == hashlib.sha256(physical.read_bytes()).hexdigest()
    assert int(binding["device"]) == physical.stat().st_dev
    assert int(binding["inode"]) == physical.stat().st_ino


def test_uv_resolution_never_uses_path_only_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _bootstrap_module()
    fake = tmp_path / "path-only" / "uv"
    fake.parent.mkdir()
    fake.write_bytes(Path("/usr/bin/true").read_bytes())
    fake.chmod(0o700)
    monkeypatch.setenv("PATH", str(fake.parent))
    monkeypatch.setattr(module, "UV_CANDIDATES", (tmp_path / "missing-uv",))

    with pytest.raises(module.DeployBootstrapError, match="absolute uv path"):
        module._resolve_uv_path("")


@pytest.mark.parametrize("failure_env", [{"UV_SYNC_EXIT": "1"}, {"PREFLIGHT_EXIT": "1"}])
def test_initialize_generation_interruption_can_restart_without_partial_marker(
    tmp_path: Path,
    failure_env: dict[str, str],
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="initialize",
    )

    failed = subprocess.run(
        command,
        cwd=checkout,
        env={**os.environ, **failure_env},
        capture_output=True,
        text=True,
        check=False,
    )
    assert failed.returncode == 2
    assert "Traceback" not in failed.stderr
    assert not marker_path_for_lock(lock_path).exists()

    recovered = subprocess.run(
        command,
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert marker_path_for_lock(lock_path).is_file()


def test_initialize_generation_cannot_be_replayed_after_marker_deletion(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="initialize",
    )
    first = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    assert first.returncode == 0, first.stderr
    marker_path_for_lock(lock_path).unlink()

    replay = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert replay.returncode == 2
    assert "already completed" in replay.stderr
    assert not marker_path_for_lock(lock_path).exists()


def test_initialize_generation_recovers_completed_transaction_before_commit_record(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    lock_fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        authority = ReleaseGenerationAuthority(
            repo=checkout,
            lock_path=lock_path,
            lock_fd=lock_fd,
            python_path=python,
            git_path=TRUSTED_GIT,
            writable=True,
            environment_builder=lambda destination: shutil.copytree(
                checkout / ".venv",
                destination,
                dirs_exist_ok=True,
                symlinks=True,
            ),
        )
        initialization = authority.begin_initialization(target_sha=commit)
        authority.publish(
            expected_commit=commit,
            operation_id=initialization.operation_id,
            transaction_kind="initialization",
        )
        authority.complete_initialization(operation_id=initialization.operation_id)
    finally:
        os.close(lock_fd)

    recovered = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="initialize",
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert commit_path_for_lock(lock_path).is_file()

    replay = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="initialize",
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    assert replay.returncode == 2
    assert "already completed" in replay.stderr


def test_initialize_generation_refuses_replaying_a_committed_initialization(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path)
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="initialize",
    )

    migrated = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )
    replay = subprocess.run(
        command,
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert migrated.returncode == 2
    assert "already completed" in migrated.stderr
    assert replay.returncode == 2
    assert "already completed" in replay.stderr
    assert marker_path_for_lock(lock_path).is_file()


@pytest.mark.parametrize("recovery_action", ["resume", "rollback"])
def test_recover_generation_republishes_only_exact_verified_target(
    tmp_path: Path,
    recovery_action: str,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, real_deployer=True)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=commit,
        target=commit,
    )
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=commit,
            mode="recover",
            recovery_action=recovery_action,
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert marker_path_for_lock(lock_path).is_file()


@pytest.mark.parametrize("failure_env", [{"UV_SYNC_EXIT": "1"}, {"PREFLIGHT_EXIT": "1"}])
def test_recover_generation_failure_stays_unpublished_and_can_restart(
    tmp_path: Path,
    failure_env: dict[str, str],
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    commit = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=commit,
    )
    marker = marker_path_for_lock(lock_path)
    marker.unlink()
    command = _command(
        checkout,
        python,
        lock_path,
        target=commit,
        mode="recover",
        recovery_action="resume",
    )

    failed = subprocess.run(
        command,
        cwd=checkout,
        env={**os.environ, **failure_env},
        capture_output=True,
        text=True,
        check=False,
    )
    assert failed.returncode == 1
    assert "Traceback" not in failed.stderr
    assert _git(checkout, "rev-parse", "HEAD") == commit
    assert not marker.exists()

    recovered = subprocess.run(
        command,
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert recovered.returncode == 0, recovered.stderr
    assert marker.is_file()


def test_recover_generation_resumes_fast_forward_target_after_interruption(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=target,
            mode="recover",
            recovery_action="resume",
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == target
    assert marker_path_for_lock(lock_path).is_file()


def test_recover_generation_rolls_back_to_verified_previous_release(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=previous,
            mode="recover",
            recovery_action="rollback",
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == previous
    assert marker_path_for_lock(lock_path).is_file()


def test_recovery_target_remains_pinned_when_origin_main_advances(tmp_path: Path) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    _git(checkout, "reset", "--hard", target)
    (checkout / "uv.lock").write_text("version = 3\n", encoding="utf-8")
    _git(checkout, "add", "uv.lock")
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "later origin generation",
    )
    later = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "update-ref", "refs/remotes/origin/main", later)
    _git(checkout, "reset", "--hard", previous)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=target,
            mode="recover",
            recovery_action="resume",
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == target


def test_recovery_uses_recorded_commit_after_tag_deleted_and_origin_rewritten(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    target = _commit_next_release(checkout)
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "tag",
        "-a",
        "v0.99.1",
        "-m",
        "v0.99.1",
    )
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
        target_ref="v0.99.1",
    )
    _git(checkout, "tag", "-d", "v0.99.1")
    _git(checkout, "update-ref", "refs/remotes/origin/main", previous)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target="v0.99.1",
            mode="recover",
            recovery_action="resume",
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == target


def test_target_checkout_authority_publishes_its_marker_schema(tmp_path: Path) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path)
    authority_path = checkout / "src" / "rquant" / "release_generation.py"
    authority_path.write_text(
        authority_path.read_text(encoding="utf-8").replace(
            "MARKER_SCHEMA_VERSION = 1",
            "MARKER_SCHEMA_VERSION = 2",
        ),
        encoding="utf-8",
    )
    _git(checkout, "add", str(authority_path.relative_to(checkout)))
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "marker schema v2",
    )
    target = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "update-ref", "refs/remotes/origin/main", target)
    _git(checkout, "reset", "--hard", previous)
    operation_id = _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    update_fd = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(update_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        update_authority = ReleaseGenerationAuthority(
            repo=checkout,
            lock_path=lock_path,
            lock_fd=update_fd,
            python_path=python,
            git_path=TRUSTED_GIT,
            writable=True,
        )
        _advance_generation_intent(
            update_authority,
            operation_id=operation_id,
            target_stage="timers_restored",
        )
    finally:
        os.close(update_fd)
    marker_path_for_lock(lock_path).unlink()
    _git(checkout, "merge", "--ff-only", target)
    lock_fd = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(
            _command(
                checkout,
                python,
                lock_path,
                target=target,
                mode="finalize",
                recovery_action="resume",
                operation_id=operation_id,
                inherited_lock_fd=lock_fd,
            ),
            cwd=checkout,
            pass_fds=(lock_fd,),
            capture_output=True,
            text=True,
            check=False,
        )
    finally:
        os.close(lock_fd)

    assert result.returncode == 0, result.stderr
    marker = json.loads(marker_path_for_lock(lock_path).read_text(encoding="utf-8"))
    assert marker["schema_version"] == 2


def test_installed_publish_finalizer_inherits_outer_handoff_without_relocking(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path)
    labels = tuple(_bootstrap_module().LAB_LAUNCHD_LABELS)
    handoff_operation_id = "d" * 32
    operation_id = _begin_intent(
        checkout,
        python,
        lock_path,
        previous=commit,
        target=commit,
        handoff_operation_id=handoff_operation_id,
        handoff_labels=labels,
    )
    update_fd = os.open(lock_path, os.O_RDWR)
    try:
        fcntl.flock(update_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        authority = ReleaseGenerationAuthority(
            repo=checkout,
            lock_path=lock_path,
            lock_fd=update_fd,
            python_path=python,
            git_path=TRUSTED_GIT,
            writable=True,
        )
        _advance_generation_intent(
            authority,
            operation_id=operation_id,
            target_stage="timers_restored",
        )
    finally:
        os.close(update_fd)
    marker_path_for_lock(lock_path).unlink()
    generation_fd = os.open(lock_path, os.O_RDWR)
    handoff_path = lock_path.with_name(f"{lock_path.stem}.handoff.lock")
    handoff_fd = os.open(handoff_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        fcntl.flock(generation_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(handoff_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(
            _command(
                checkout,
                python,
                lock_path,
                target=commit,
                mode="finalize",
                recovery_action="deploy",
                operation_id=operation_id,
                inherited_lock_fd=generation_fd,
                inherited_handoff_lock_fd=handoff_fd,
                lifecycle_mode="installed",
            ),
            cwd=checkout,
            pass_fds=(generation_fd, handoff_fd),
            capture_output=True,
            text=True,
            check=False,
            timeout=20,
        )
    finally:
        os.close(handoff_fd)
        os.close(generation_fd)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "generation_publish"


def test_rollback_uses_previous_checkout_marker_schema(tmp_path: Path) -> None:
    checkout, python, lock_path, previous = _checkout(tmp_path, real_deployer=True)
    authority_path = checkout / "src" / "rquant" / "release_generation.py"
    authority_path.write_text(
        authority_path.read_text(encoding="utf-8").replace(
            "MARKER_SCHEMA_VERSION = 1",
            "MARKER_SCHEMA_VERSION = 2",
        ),
        encoding="utf-8",
    )
    _git(checkout, "add", str(authority_path.relative_to(checkout)))
    _git(
        checkout,
        "-c",
        "user.name=rQuant Tests",
        "-c",
        "user.email=tests@rquant.invalid",
        "commit",
        "-qm",
        "marker schema v2",
    )
    target = _git(checkout, "rev-parse", "HEAD")
    _git(checkout, "update-ref", "refs/remotes/origin/main", target)
    _git(checkout, "reset", "--hard", previous)
    _begin_intent(
        checkout,
        python,
        lock_path,
        previous=previous,
        target=target,
    )
    _git(checkout, "merge", "--ff-only", target)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target=previous,
            mode="recover",
            recovery_action="rollback",
        ),
        cwd=checkout,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert _git(checkout, "rev-parse", "HEAD") == previous
    marker = json.loads(marker_path_for_lock(lock_path).read_text(encoding="utf-8"))
    assert marker["schema_version"] == 1


def test_generation_mode_rejects_target_that_is_not_current_head(tmp_path: Path) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path, publish_marker=False)

    result = subprocess.run(
        _command(
            checkout,
            python,
            lock_path,
            target="f" * 40,
            mode="initialize",
        ),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "Traceback" not in result.stderr
    assert not marker_path_for_lock(lock_path).exists()


def test_missing_generation_is_controlled_exit_without_traceback(tmp_path: Path) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    marker_path_for_lock(lock_path).unlink()

    result = subprocess.run(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env=os.environ,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "marker is missing" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize("signum", (signal.SIGTERM, signal.SIGINT))
def test_bootstrap_process_runner_reaps_group_before_signal_exit(
    tmp_path: Path,
    signum: signal.Signals,
) -> None:
    ready = tmp_path / "ready"
    late_mutation = tmp_path / "late-mutation"
    child_program = (
        "import subprocess,sys,time; from pathlib import Path; "
        "subprocess.Popen([sys.executable,'-c',"
        '"import sys,time; from pathlib import Path; time.sleep(.25); '
        "Path(sys.argv[1]).write_text('late')\",sys.argv[2]],start_new_session=True); "
        "Path(sys.argv[1]).write_text('ready'); time.sleep(.6)"
    )
    harness = f"""
import importlib.util
import sys
from pathlib import Path
spec = importlib.util.spec_from_file_location("bootstrap", {str(BOOTSTRAP)!r})
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
module._run_process_group(
    [sys.executable, "-c", {child_program!r}, sys.argv[1], sys.argv[2]],
    cwd=Path(sys.argv[3]),
    timeout_seconds=10,
)
"""
    process = subprocess.Popen(
        [sys.executable, "-c", harness, str(ready), str(late_mutation), str(tmp_path)],
        cwd=ROOT,
    )
    deadline = time.monotonic() + 5
    while not ready.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert ready.exists()

    os.kill(process.pid, signum)
    process.wait(timeout=5)
    time.sleep(0.8)

    assert not late_mutation.exists()


def test_bootstrap_runner_base_exception_contains_detached_grandchild(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _bootstrap_module()
    marker = tmp_path / "base-exception-grandchild-survived"
    grandchild = (
        "import sys,time; from pathlib import Path; "
        "time.sleep(.3); Path(sys.argv[1]).write_text('late')"
    )
    child = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable,'-c',{grandchild!r},sys.argv[1]],"
        "start_new_session=True); time.sleep(5)"
    )
    real_popen = subprocess.Popen

    class SimulatedRunnerCrash(BaseException):
        pass

    class CrashProxy:
        def __init__(self, process: subprocess.Popen[str]) -> None:
            self._process = process
            self.pid = process.pid
            self.returncode: int | None = None
            self.crashed = False

        def __getattr__(self, name: str) -> object:
            return getattr(self._process, name)

        def communicate(self, *args: object, **kwargs: object) -> tuple[str, str]:
            if not self.crashed:
                self.crashed = True
                time.sleep(0.08)
                raise SimulatedRunnerCrash
            result = self._process.communicate(*args, **kwargs)
            self.returncode = self._process.returncode
            return result

    monkeypatch.setattr(
        module.subprocess,
        "Popen",
        lambda *args, **kwargs: CrashProxy(real_popen(*args, **kwargs)),
    )

    with pytest.raises(SimulatedRunnerCrash):
        module._run_process_group(
            [sys.executable, "-c", child, str(marker)],
            cwd=tmp_path,
            timeout_seconds=0.5,
        )
    time.sleep(0.5)

    assert not marker.exists()


def test_dirty_release_authority_is_rejected_before_project_import(tmp_path: Path) -> None:
    checkout, python, lock_path, _commit = _checkout(tmp_path)
    imported = tmp_path / "imported"
    ran = tmp_path / "ran"
    authority = checkout / "src" / "rquant" / "release_generation.py"
    authority.write_text(
        authority.read_text(encoding="utf-8") + "\nDIRTY = True\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        _command(checkout, python, lock_path),
        cwd=checkout,
        env={
            **os.environ,
            "DEPLOY_LOCK": str(lock_path),
            "IMPORT_MARKER": str(imported),
            "RUN_MARKER": str(ran),
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 2
    assert "checkout is dirty" in result.stderr
    assert "Traceback" not in result.stderr
    assert not imported.exists()
    assert not ran.exists()


def test_bootstrap_binds_interpreter_before_loading_project_runner() -> None:
    source = BOOTSTRAP.read_text(encoding="utf-8")

    assert source.index("def _bind_bootstrap_interpreter") < source.index(
        "def _load_contained_runner"
    )
    assert "run_contained = _load_contained_runner()" not in source
    assert source.index("interpreter_binding = _bind_bootstrap_interpreter") < source.index(
        "uv_path, _uv_binding = _resolve_uv_path"
    )
