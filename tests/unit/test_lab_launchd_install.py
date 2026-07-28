from __future__ import annotations

import fcntl
import json
import os
import plistlib
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from rquant.lab_launchd_install import (
    LAB_LAUNCHD_LABELS,
    LabLaunchdInstaller,
    LabLaunchdInstallError,
)
from rquant.release_generation import ReleaseGenerationAuthority, marker_path_for_lock

ROOT = Path(__file__).resolve().parents[2]
TRUSTED_GIT = Path("/usr/bin/git")


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, str]:
    repo = tmp_path / "repo"
    (repo / "src" / "rquant").mkdir(parents=True)
    (repo / "scripts").mkdir()
    (repo / "deploy" / "launchd").mkdir(parents=True)
    for relative in (
        "scripts/run-lab-daemon.py",
        "scripts/bootstrap-lab-daemon.py",
        "scripts/preflight-lab-runtime.py",
        "scripts/strict_json.py",
        "src/rquant/release_generation.py",
        "src/rquant/strict_json.py",
    ):
        source = ROOT / relative
        target = repo / relative
        shutil.copy2(source, target)
    for label in LAB_LAUNCHD_LABELS:
        shutil.copy2(
            ROOT / "deploy" / "launchd" / f"{label}.plist",
            repo / "deploy" / "launchd" / f"{label}.plist",
        )
    (repo / "src" / "rquant" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "pyproject.toml").write_text(
        '[project]\nname="rquant"\nversion="0.99.0"\n', encoding="utf-8"
    )
    (repo / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (repo / ".env").write_text(f"DATA_DIR='{tmp_path / 'data'}'\n", encoding="utf-8")
    (repo / ".env").chmod(0o600)
    venv = repo / ".venv"
    (venv / "bin").mkdir(parents=True)
    shutil.copy2(sys.executable, venv / "bin" / "python")
    (venv / "bin" / "python").chmod(0o700)
    (venv / "bin" / "rquant").write_text(f"#!{venv / 'bin' / 'python'}\n", encoding="utf-8")
    (venv / "bin" / "rquant").chmod(0o700)
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    (venv / "pyvenv.cfg").write_text(
        f"home = {Path(sys.base_prefix) / 'bin'}\nversion = {version}\n", encoding="utf-8"
    )
    (venv / "lib" / f"python{version}" / "site-packages").mkdir(parents=True)
    python_library = Path(sys.base_prefix) / "lib" / f"libpython{version}.dylib"
    if python_library.exists():
        shutil.copy2(python_library, venv / "lib" / python_library.name)
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
            "fixture",
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
    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    lock = authority / "rquant.lock"
    lock_fd = os.open(lock, os.O_RDWR | os.O_CREAT, 0o600)
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    release = ReleaseGenerationAuthority(
        repo=repo,
        lock_path=lock,
        lock_fd=lock_fd,
        python_path=venv / "bin" / "python",
        git_path=TRUSTED_GIT,
        writable=True,
        environment_builder=lambda destination: shutil.copytree(
            venv, destination, dirs_exist_ok=True
        ),
        minimum_free_bytes=0,
    )
    intent = release.begin_initialization(target_sha=commit)
    release.publish(
        expected_commit=commit,
        operation_id=intent.operation_id,
        transaction_kind="initialization",
    )
    release.complete_initialization(operation_id=intent.operation_id)
    release.commit_generation(
        operation_id=intent.operation_id,
        transaction_kind="initialization",
    )
    os.close(lock_fd)
    launch_agents = tmp_path / "LaunchAgents"
    launch_agents.mkdir(mode=0o700)
    return repo, lock, launch_agents, commit


class _Runner:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail_on = fail_on

    def __call__(self, command: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        del timeout
        self.calls.append(tuple(command))
        if self.fail_on is not None and self.fail_on in " ".join(command):
            return subprocess.CompletedProcess(command, 1, "", "failed")
        return subprocess.CompletedProcess(command, 0, "", "")


class _FailFirstKickstartRunner(_Runner):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    def __call__(self, command: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        del timeout
        self.calls.append(tuple(command))
        if command[1:2] == ["kickstart"] and not self.failed:
            self.failed = True
            return subprocess.CompletedProcess(command, 1, "", "failed")
        return subprocess.CompletedProcess(command, 0, "", "")


def test_installer_materializes_generation_bound_plists_and_is_idempotent(tmp_path: Path) -> None:
    repo, lock, launch_agents, commit = _fixture(tmp_path)
    runner = _Runner()
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=runner,
    )

    first = installer.install(activate=True)
    before = {
        path.name: (path.read_bytes(), path.stat().st_ino) for path in launch_agents.glob("*.plist")
    }
    second = installer.install(activate=True)

    assert first.code_sha == second.code_sha == commit
    assert len(before) == 3
    marker = json.loads(marker_path_for_lock(lock).read_text(encoding="utf-8"))
    generation = Path(marker["venv_path"])
    for label in LAB_LAUNCHD_LABELS:
        path = launch_agents / f"{label}.plist"
        with path.open("rb") as stream:
            document = plistlib.load(stream)
        serialized = path.read_text(encoding="utf-8")
        assert str(generation / "release") in serialized
        assert str(repo / "scripts") not in serialized
        assert document["WorkingDirectory"] == str(generation / "release")
        assert path.stat().st_mode & 0o777 == 0o600
    assert {
        path.name: (path.read_bytes(), path.stat().st_ino) for path in launch_agents.glob("*.plist")
    } == before
    assert any(call[:2] == ("/bin/launchctl", "bootstrap") for call in runner.calls)


def test_installer_rejects_symlink_destination_without_touching_external(tmp_path: Path) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    external = tmp_path / "external.plist"
    external.write_text("external", encoding="utf-8")
    target = launch_agents / f"{LAB_LAUNCHD_LABELS[0]}.plist"
    target.symlink_to(external)

    with pytest.raises(LabLaunchdInstallError, match="symlink|physical"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(),
        ).install(activate=False)

    assert external.read_text(encoding="utf-8") == "external"


def test_installer_activation_failure_restores_previous_plists(tmp_path: Path) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    initial = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )
    initial.install(activate=False)
    before = {path.name: path.read_bytes() for path in launch_agents.glob("*.plist")}

    with pytest.raises(LabLaunchdInstallError, match="launchctl"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(fail_on="kickstart"),
        ).install(activate=True)

    assert {path.name: path.read_bytes() for path in launch_agents.glob("*.plist")} == before


def test_installer_activation_failure_restores_previously_loaded_labels(tmp_path: Path) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)
    runner = _FailFirstKickstartRunner()

    with pytest.raises(LabLaunchdInstallError, match="kickstart"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=runner,
        ).install(activate=True)

    failure_index = next(
        index
        for index, call in enumerate(runner.calls)
        if call[1:2] == ("kickstart",) and call[-1].endswith(LAB_LAUNCHD_LABELS[0])
    )
    recovery_calls = runner.calls[failure_index + 1 :]
    assert {
        call[-1].rsplit("/", 1)[-1] for call in recovery_calls if call[1:2] == ("kickstart",)
    } == set(LAB_LAUNCHD_LABELS)


def test_uninstall_refuses_modified_plist_and_removes_exact_installation(tmp_path: Path) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )
    installer.install(activate=False)
    changed = launch_agents / f"{LAB_LAUNCHD_LABELS[0]}.plist"
    changed.chmod(0o600)
    changed.write_bytes(changed.read_bytes() + b"\n")
    with pytest.raises(LabLaunchdInstallError, match="changed"):
        installer.uninstall(deactivate=False)
    installer.install(activate=False)

    installer.uninstall(deactivate=True)

    assert not list(launch_agents.glob("*.plist"))
