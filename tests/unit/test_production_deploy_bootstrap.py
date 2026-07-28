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
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from types import ModuleType
from zoneinfo import ZoneInfo

import pytest

from rquant.release_generation import (
    ReleaseGenerationAuthority,
    commit_path_for_lock,
    marker_path_for_lock,
)

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = ROOT / "scripts" / "bootstrap-production-deploy.py"
AUTHORITY = ROOT / "src" / "rquant" / "release_generation.py"
PRODUCTION_DEPLOYER = ROOT / "src" / "rquant" / "ops" / "production_deploy.py"
TRUSTED_GIT = Path("/usr/bin/git")
_ORIGINAL_OS_WALK = os.walk
_READINESS_A = ("a" * 32, "b" * 64, "c" * 40)


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
    shutil.copy2(sys.executable, python)
    python.chmod(0o700)
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    (venv_root / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {version}\n",
        encoding="utf-8",
    )
    (venv_root / "lib" / f"python{version}" / "site-packages").mkdir(parents=True)
    python_library = Path(sys.base_prefix) / "lib" / f"libpython{version}.dylib"
    if python_library.exists():
        shutil.copy2(python_library, venv_root / "lib" / python_library.name)
    return python


def _git(checkout: Path, *arguments: str) -> str:
    return subprocess.run(
        [str(TRUSTED_GIT), *arguments],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


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
    shutil.copy2(AUTHORITY, package / AUTHORITY.name)
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
            plist.write_text(
                "<?xml version='1.0'?><plist version='1.0'><dict/></plist>\n",
                encoding="utf-8",
            )
            plist.chmod(0o600)
    python = _tiny_test_venv(checkout)
    rquant = checkout / ".venv" / "bin" / "rquant"
    rquant.write_text(
        f"#!{python}\n"
        "import os, sys\n"
        "if sys.argv[1:] != ['preflight']:\n"
        "    raise SystemExit(64)\n"
        "raise SystemExit(int(os.environ.get('PREFLIGHT_EXIT', '0')))\n",
        encoding="utf-8",
    )
    rquant.chmod(0o700)
    uv = checkout / ".venv" / "bin" / "uv"
    uv.write_text(
        f"#!{python}\n"
        "import os, shutil, sys\n"
        "from pathlib import Path\n"
        "if sys.argv[1] == 'venv':\n"
        "    shutil.copytree(\n"
        "        Path(sys.prefix), Path(sys.argv[-1]), dirs_exist_ok=True, symlinks=True\n"
        "    )\n"
        "elif sys.argv[1:3] == ['sync', '--frozen']:\n"
        "    if target_value := os.environ.get('UV_PROJECT_ENVIRONMENT'):\n"
        "        target = Path(target_value)\n"
        "        if not (target / 'pyvenv.cfg').exists():\n"
        "            shutil.copytree(Path(sys.prefix), target, dirs_exist_ok=True, symlinks=True)\n"
        "else:\n"
        "    raise SystemExit(64)\n"
        "raise SystemExit(int(os.environ.get('UV_SYNC_EXIT', '0')))\n",
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
    subprocess.run(
        [str(TRUSTED_GIT), "update-ref", "refs/remotes/origin/main", commit],
        cwd=checkout,
        check=True,
    )
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
    _git(checkout, "update-ref", "refs/remotes/origin/main", commit)
    return commit


def _begin_intent(
    checkout: Path,
    python: Path,
    lock_path: Path,
    *,
    previous: str,
    target: str,
    target_ref: str | None = None,
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
        )
        return intent.operation_id
    finally:
        os.close(lock_fd)


def _command(
    checkout: Path,
    python: Path,
    lock_path: Path,
    *,
    target: str = "v0.99.0",
    mode: str = "deploy",
    recovery_action: str | None = None,
    operation_id: str | None = None,
    inherited_lock_fd: int | None = None,
    finalize_phase: str = "publish",
    lifecycle_mode: str = "uninstalled",
) -> list[str]:
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
        "macos-lab",
        "--host-platform",
        "darwin",
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
    command.extend(["--", "--target", target])
    return command


def _handoff_fixture(tmp_path: Path) -> tuple[ModuleType, Path, Path]:
    module = _bootstrap_module()
    root = tmp_path / "rquant"
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
    runtime_root = root / "data" / "lab-runtime"
    runtime_root.mkdir(parents=True, mode=0o700, exist_ok=True)
    runtime_root.chmod(0o700)
    module._write_lab_installation_state(
        root=root,
        lock_path=lock_path,
        runtime_root=runtime_root,
        readiness_root=runtime_root / "readiness",
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

    handoff.prepare(dry_run=True)
    handoff.restore()

    assert calls == []
    payload = json.loads(capsys.readouterr().err)
    assert payload == {
        "lab_daemon_handoff": "planned",
        "labels": list(module.LAB_LAUNCHD_LABELS),
        "stopped": False,
    }


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
        now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    assert loaded == set()
    handoff.restore()

    assert loaded == initially_loaded
    assert handoff.stopped == list(module.LAB_LAUNCHD_LABELS)
    assert sum(call[0] == "bootstrap" for call in calls) == 3


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
            now=datetime(2026, 7, 27, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
        )
    handoff.restore()

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
        path.write_text(json.dumps(payload), encoding="utf-8")
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
        handoff.prepare(dry_run=True)


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
        },
    )
    handoff = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)

    handoff.prepare(
        dry_run=False,
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


def test_completed_handoff_proof_survives_consecutive_installed_releases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module, root, lock_path = _handoff_fixture(tmp_path)
    _install_lab_handoff(module, root, lock_path)
    loaded = set(module.LAB_LAUNCHD_LABELS)
    expectations = iter(
        [
            ("a" * 32, "b" * 64, "c" * 40),
            ("d" * 32, "e" * 64, "f" * 40),
        ]
    )

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
    first.prepare(dry_run=False, now=now)
    first_operation = first.operation_id
    first.restore()
    first_proof = module._completed_handoff_path(lock_path, first_operation)
    first_payload = json.loads(first_proof.read_text(encoding="utf-8"))
    interrupted_active = dict(first_payload)
    interrupted_active["stage"] = "restarting"
    interrupted_active["restarted_labels"] = []
    module._atomic_private_json(first.record_path, interrupted_active)

    second = module._LabLaunchdHandoff(root=root, lock_path=lock_path, timeout_seconds=1)
    second.prepare(dry_run=False, now=now)
    assert second.operation_id != first_operation
    assert json.loads(first_proof.read_text(encoding="utf-8")) == first_payload
    second.restore()

    assert first_payload["generation_operation_id"] == "a" * 32
    assert first_payload["environment_generation_id"] == "b" * 64
    assert first_payload["code_sha"] == "c" * 40
    assert module._completed_handoff_path(lock_path, second.operation_id).is_file()


def test_lab_handoff_restore_gets_fresh_overall_budget(
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
        timeout_seconds=0.1,
        overall_timeout_seconds=0.1,
    )
    handoff.prepare(
        dry_run=False,
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    handoff.deadline = time.monotonic() - 1

    handoff.restore()

    assert loaded == set(module.LAB_LAUNCHD_LABELS)


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
        def prepare(self, *, dry_run: bool, now: datetime | None = None) -> None:
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

    result = module._complete_installed_rollout(
        target_handoff=TargetHandoff(),
        deploy_code=0,
        recovery_handoff_factory=lambda: recovery_handoff,
        rollback=rollback,
        now=datetime(2026, 7, 28, 16, 0, tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert result == 1
    assert events == [
        "target-readiness-failed",
        "target-daemons-stopped",
        "previous-generation-restored",
        "previous-daemons-ready",
    ]


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


def test_deploy_bootstrap_dry_run_uses_shared_generation_without_stopping(
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

    assert result.returncode == 0, result.stderr
    plan = json.loads(result.stderr)
    assert plan["lab_daemon_handoff"] == "planned"
    assert plan["stopped"] is False


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
    assert marker_path_for_lock(lock_path).is_file()
    assert not imported.exists()
    assert not ran.exists()


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

    assert result.returncode == 0, result.stderr
    installation = lock_path.with_name(f"{lock_path.stem}.lab-install.json")
    assert json.loads(installation.read_text(encoding="utf-8"))["runtime_root"] == str(runtime_root)


def test_initialize_generation_accepts_uv_style_symlinked_python(
    tmp_path: Path,
) -> None:
    checkout, python, lock_path, commit = _checkout(tmp_path, publish_marker=False)
    python.unlink()
    python.symlink_to(Path(sys.executable).resolve(strict=True))

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
        update_authority.update_deployment_intent(
            operation_id=operation_id,
            stage="timers_restored",
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
