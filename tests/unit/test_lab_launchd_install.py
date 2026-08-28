from __future__ import annotations

import fcntl
import hashlib
import json
import os
import plistlib
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

import pytest

import rquant.lab_launchd_install as install_module
from rquant.lab_launchd_install import (
    LAB_LAUNCHD_LABELS,
    LabLaunchdInstaller,
    LabLaunchdInstallError,
)
from rquant.release_generation import ReleaseGenerationAuthority, marker_path_for_lock
from rquant.strict_json import canonical_json_bytes
from tests.support.verified_system_interpreter import materialize_system_interpreter

ROOT = Path(__file__).resolve().parents[2]
TRUSTED_GIT = Path("/usr/bin/git")


def _binding(path: Path) -> dict[str, object]:
    observed = path.lstat()
    return {
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "device": observed.st_dev,
        "inode": observed.st_ino,
    }


def test_validate_binding_rejects_same_content_inode_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "agent.plist"
    replacement = tmp_path / "replacement.plist"
    path.write_bytes(b"same")
    replacement.write_bytes(b"same")
    path.chmod(0o600)
    replacement.chmod(0o600)
    binding = _binding(path)
    replaced = False
    original_read_bytes = Path.read_bytes
    original_os_read = install_module.os.read

    def replace_once() -> None:
        nonlocal replaced
        if not replaced:
            replaced = True
            os.replace(replacement, path)

    def racing_path_read(candidate: Path) -> bytes:
        if candidate == path:
            replace_once()
        return original_read_bytes(candidate)

    def racing_os_read(descriptor: int, size: int) -> bytes:
        replace_once()
        return original_os_read(descriptor, size)

    monkeypatch.setattr(Path, "read_bytes", racing_path_read)
    monkeypatch.setattr(install_module.os, "read", racing_os_read)

    with pytest.raises(LabLaunchdInstallError, match="changed"):
        LabLaunchdInstaller._validate_binding(binding, path)

    assert replaced


def test_read_existing_rejects_same_content_inode_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "com.roxor.rquant-lab-worker.plist"
    path = tmp_path / name
    replacement = tmp_path / "replacement.plist"
    path.write_bytes(b"same")
    replacement.write_bytes(b"same")
    path.chmod(0o600)
    replacement.chmod(0o600)
    replaced = False
    original_read_bytes = Path.read_bytes
    original_os_read = install_module.os.read

    def replace_once() -> None:
        nonlocal replaced
        if not replaced:
            replaced = True
            os.replace(replacement, path)

    def racing_path_read(candidate: Path) -> bytes:
        if candidate == path:
            replace_once()
        return original_read_bytes(candidate)

    def racing_os_read(descriptor: int, size: int) -> bytes:
        replace_once()
        return original_os_read(descriptor, size)

    monkeypatch.setattr(Path, "read_bytes", racing_path_read)
    monkeypatch.setattr(install_module.os, "read", racing_os_read)
    installer = object.__new__(LabLaunchdInstaller)
    installer.launch_agents_dir = tmp_path

    with pytest.raises(LabLaunchdInstallError, match="changed"):
        installer._read_existing(name)

    assert replaced


def test_active_generation_rejects_same_content_marker_swap_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    lock = authority / "rquant.lock"
    lock.write_bytes(b"")
    lock.chmod(0o600)
    marker = authority / "rquant.complete.json"
    replacement = authority / "replacement.json"
    payload = b'{"commit":"' + b"a" * 40 + b'"}\n'
    marker.write_bytes(payload)
    replacement.write_bytes(payload)
    marker.chmod(0o600)
    replacement.chmod(0o600)
    replaced = False
    original_read_bytes = Path.read_bytes
    original_os_read = install_module.os.read

    def racing_path_read(candidate: Path) -> bytes:
        nonlocal replaced
        if candidate == marker and not replaced:
            replaced = True
            os.replace(replacement, marker)
        return original_read_bytes(candidate)

    def racing_os_read(descriptor: int, size: int) -> bytes:
        nonlocal replaced
        if not replaced:
            replaced = True
            os.replace(replacement, marker)
        return original_os_read(descriptor, size)

    monkeypatch.setattr(Path, "read_bytes", racing_path_read)
    monkeypatch.setattr(install_module.os, "read", racing_os_read)
    installer = object.__new__(LabLaunchdInstaller)
    installer.trusted_git_path = TRUSTED_GIT
    installer.lock_path = lock

    with pytest.raises(LabLaunchdInstallError, match="changed"):
        installer._active_generation(-1)

    assert replaced


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
    system_home = materialize_system_interpreter(tmp_path / "system-python")
    (venv / "pyvenv.cfg").write_text(
        f"home = {system_home}\nversion = {version}\n", encoding="utf-8"
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
    runtime_root = tmp_path / "data" / "lab-runtime"
    readiness_root = runtime_root / "readiness"
    readiness_root.mkdir(parents=True, mode=0o700)
    runtime_root.chmod(0o700)
    readiness_root.chmod(0o700)
    registered = {
        "schema_version": 2,
        "checkout_root": str(repo),
        "labels": list(LAB_LAUNCHD_LABELS),
        "plists": {},
        "runtime_root": str(runtime_root),
        "readiness_root": str(readiness_root),
        "registered_by_commit": commit,
        "prepared_authority": {
            "runtime_authority_id": "a" * 32,
            "runtime_root": str(runtime_root),
            "runtime_device": runtime_root.stat().st_dev,
            "runtime_inode": runtime_root.stat().st_ino,
        },
        "installed_at": "2026-07-29T00:00:00+00:00",
    }
    for label in LAB_LAUNCHD_LABELS:
        path = repo / "deploy" / "launchd" / f"{label}.plist"
        observed = path.stat()
        registered["plists"][label] = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "device": observed.st_dev,
            "inode": observed.st_ino,
        }
    registration = lock.with_name(f"{lock.stem}.lab-install.json")
    registration.write_bytes(canonical_json_bytes(registered, trailing_newline=True))
    registration.chmod(0o600)
    return repo, lock, launch_agents, commit


class _Runner:
    def __init__(self, *, fail_on: str | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.fail_on = fail_on
        self.loaded: set[str] = set()

    def __call__(self, command: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        del timeout
        self.calls.append(tuple(command))
        if self.fail_on is not None and self.fail_on in " ".join(command):
            return subprocess.CompletedProcess(command, 1, "", "failed")
        action = command[1] if len(command) > 1 else ""
        label = command[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(command, 0 if label in self.loaded else 113, "", "")
        if action == "bootout":
            self.loaded.discard(label)
        elif action == "bootstrap":
            self.loaded.add(Path(command[-1]).stem)
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
        return super().__call__(command, timeout=1)


class _LaunchdStateRunner(_Runner):
    def __init__(
        self,
        *,
        loaded: set[str],
        fail_bootout: str | None = None,
        after_bootout: dict[str, Callable[[], None]] | None = None,
    ) -> None:
        super().__init__()
        self.loaded = loaded
        self.fail_bootout = fail_bootout
        self.after_bootout = after_bootout or {}

    def __call__(self, command: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        assert timeout > 0
        self.calls.append(tuple(command))
        action = command[1] if len(command) > 1 else ""
        label = command[-1].rsplit("/", 1)[-1]
        if action == "print":
            return subprocess.CompletedProcess(command, 0 if label in self.loaded else 113, "", "")
        if action == "bootout":
            if label == self.fail_bootout:
                return subprocess.CompletedProcess(command, 1, "", "busy")
            self.loaded.discard(label)
            callback = self.after_bootout.get(label)
            if callback is not None:
                callback()
        elif action in {"bootstrap", "kickstart"}:
            self.loaded.add(Path(command[-1]).stem if action == "bootstrap" else label)
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
    local_state = json.loads(
        lock.with_name(f"{lock.stem}.lab-local-install.json").read_text(encoding="utf-8")
    )
    registered = json.loads(
        lock.with_name(f"{lock.stem}.lab-install.json").read_text(encoding="utf-8")
    )
    for label in LAB_LAUNCHD_LABELS:
        name = f"{label}.plist"
        path = launch_agents / name
        identity = {
            "path": str(path),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "device": path.stat().st_dev,
            "inode": path.stat().st_ino,
        }
        assert local_state["plists"][name] == identity
        assert registered["plists"][label] == identity


def test_installer_rejects_symlink_destination_without_touching_external(tmp_path: Path) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    external = tmp_path / "external.plist"
    external.write_text("external", encoding="utf-8")
    target = launch_agents / f"{LAB_LAUNCHD_LABELS[0]}.plist"
    target.symlink_to(external)

    with pytest.raises(LabLaunchdInstallError, match="symlink|physical|foreign"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(),
        ).install(activate=False)

    assert external.read_text(encoding="utf-8") == "external"


@pytest.mark.parametrize("label", LAB_LAUNCHD_LABELS)
def test_first_install_rejects_foreign_regular_plist_without_replacing_identity(
    tmp_path: Path,
    label: str,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    foreign = launch_agents / f"{label}.plist"
    foreign.write_bytes(b"foreign\n")
    foreign.chmod(0o600)
    before = (foreign.read_bytes(), foreign.stat().st_ino)

    with pytest.raises(LabLaunchdInstallError, match="foreign|registered|installation"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(),
        ).install(activate=False)

    assert (foreign.read_bytes(), foreign.stat().st_ino) == before


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


def test_installer_failure_restores_exact_plist_and_state_inodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)
    local_state = lock.with_name(f"{lock.stem}.lab-local-install.json")
    managed = [
        *launch_agents.glob("*.plist"),
        local_state,
        lock.with_name(f"{lock.stem}.lab-install.json"),
    ]
    before = {path: (path.read_bytes(), path.stat().st_ino) for path in managed}
    original_payload = LabLaunchdInstaller._plist_payload

    def changed_payload(
        self: LabLaunchdInstaller,
        marker: object,
        code_root: Path,
        label: str,
    ) -> bytes:
        return original_payload(self, marker, code_root, label) + b"\n"

    monkeypatch.setattr(LabLaunchdInstaller, "_plist_payload", changed_payload)
    runner = _FailFirstKickstartRunner()
    runner.loaded = set(LAB_LAUNCHD_LABELS)

    with pytest.raises(LabLaunchdInstallError, match="kickstart"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=runner,
        ).install(activate=True)

    assert {path: (path.read_bytes(), path.stat().st_ino) for path in managed} == before


def test_foreign_destination_after_quarantine_is_never_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)
    name = f"{LAB_LAUNCHD_LABELS[0]}.plist"
    destination = launch_agents / name
    original_payload = LabLaunchdInstaller._plist_payload

    def changed_payload(
        self: LabLaunchdInstaller,
        marker: object,
        code_root: Path,
        label: str,
    ) -> bytes:
        return original_payload(self, marker, code_root, label) + b"\n"

    def inject(stage: str) -> None:
        if stage == f"after-quarantine:{name}":
            destination.write_bytes(b"foreign-after-quarantine")
            destination.chmod(0o600)

    monkeypatch.setattr(LabLaunchdInstaller, "_plist_payload", changed_payload)
    with pytest.raises(LabLaunchdInstallError, match="foreign|appeared"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(),
            mutation_hook=inject,
        ).install(activate=False)

    assert destination.read_bytes() == b"foreign-after-quarantine"
    assert list(launch_agents.glob(f".{name}.*.rollback"))
    assert lock.with_name(f"{lock.stem}.lab-install-transaction.json").exists()


def test_foreign_backup_winning_exact_quarantine_race_is_never_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)
    name = f"{LAB_LAUNCHD_LABELS[0]}.plist"
    destination = launch_agents / name
    original = (destination.read_bytes(), destination.stat().st_ino)
    original_payload = LabLaunchdInstaller._plist_payload
    real_rename = install_module.rename_noreplace_at
    injected_backup: Path | None = None

    def changed_payload(
        self: LabLaunchdInstaller,
        marker: object,
        code_root: Path,
        label: str,
    ) -> bytes:
        return original_payload(self, marker, code_root, label) + b"\n"

    def inject_rename(
        source_dir_fd: int,
        source_name: str,
        destination_dir_fd: int,
        destination_name: str,
    ) -> None:
        nonlocal injected_backup
        if source_name == name and destination_name.endswith(".rollback"):
            descriptor = os.open(
                destination_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=destination_dir_fd,
            )
            os.write(descriptor, b"foreign-backup")
            os.close(descriptor)
            injected_backup = launch_agents / destination_name
        real_rename(
            source_dir_fd,
            source_name,
            destination_dir_fd,
            destination_name,
        )

    monkeypatch.setattr(LabLaunchdInstaller, "_plist_payload", changed_payload)
    monkeypatch.setattr(install_module, "rename_noreplace_at", inject_rename)

    with pytest.raises(LabLaunchdInstallError, match="backup (appeared|changed)"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(),
        ).install(activate=False)

    assert (destination.read_bytes(), destination.stat().st_ino) == original
    assert injected_backup is not None
    assert injected_backup.read_bytes() == b"foreign-backup"


def test_forged_quarantine_backup_blocks_recovery_and_preserves_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)
    name = f"{LAB_LAUNCHD_LABELS[0]}.plist"
    original_payload = LabLaunchdInstaller._plist_payload

    class SimulatedCrash(BaseException):
        pass

    def changed_payload(
        self: LabLaunchdInstaller,
        marker: object,
        code_root: Path,
        label: str,
    ) -> bytes:
        return original_payload(self, marker, code_root, label) + b"\n"

    def forge(stage: str) -> None:
        if stage == f"after-quarantine:{name}":
            backup = next(launch_agents.glob(f".{name}.*.rollback"))
            backup.write_bytes(b"forged-backup")
            backup.chmod(0o600)
            raise SimulatedCrash

    monkeypatch.setattr(LabLaunchdInstaller, "_plist_payload", changed_payload)
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
        mutation_hook=forge,
    )
    with pytest.raises(LabLaunchdInstallError, match="backup changed"):
        installer.install(activate=False)

    backup = next(launch_agents.glob(f".{name}.*.rollback"))
    assert backup.read_bytes() == b"forged-backup"
    assert not (launch_agents / name).exists()
    assert lock.with_name(f"{lock.stem}.lab-install-transaction.json").exists()


def test_foreign_destination_after_replacement_arm_is_not_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)
    name = f"{LAB_LAUNCHD_LABELS[0]}.plist"
    destination = launch_agents / name
    original_payload = LabLaunchdInstaller._plist_payload

    def changed_payload(
        self: LabLaunchdInstaller,
        marker: object,
        code_root: Path,
        label: str,
    ) -> bytes:
        return original_payload(self, marker, code_root, label) + b"\n"

    def inject(stage: str) -> None:
        if stage == f"replacement-armed:{name}":
            destination.write_bytes(b"foreign-after-arm")
            destination.chmod(0o600)

    monkeypatch.setattr(LabLaunchdInstaller, "_plist_payload", changed_payload)
    with pytest.raises(LabLaunchdInstallError, match="foreign|appeared"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(),
            mutation_hook=inject,
        ).install(activate=False)

    assert destination.read_bytes() == b"foreign-after-arm"


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
    runner.loaded = set(LAB_LAUNCHD_LABELS)

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
    original = changed.read_bytes()
    changed.chmod(0o600)
    changed.write_bytes(changed.read_bytes() + b"\n")
    with pytest.raises(LabLaunchdInstallError, match="changed"):
        installer.uninstall(deactivate=False)
    changed.write_bytes(original)
    installer.install(activate=False)

    installer.uninstall(deactivate=True)

    assert not list(launch_agents.glob("*.plist"))

    before = {
        path: (
            path.read_bytes() if path.is_file() else None,
            path.stat().st_ino,
            path.stat().st_mtime_ns,
        )
        for path in lock.parent.iterdir()
    }
    installer.uninstall(deactivate=True)
    after = {
        path: (
            path.read_bytes() if path.is_file() else None,
            path.stat().st_ino,
            path.stat().st_mtime_ns,
        )
        for path in lock.parent.iterdir()
    }
    assert after == before


def test_clean_uninstall_refuses_foreign_plist_after_prior_removal(tmp_path: Path) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )
    installer.install(activate=False)
    installer.uninstall(deactivate=True)
    foreign = launch_agents / f"{LAB_LAUNCHD_LABELS[1]}.plist"
    foreign.write_bytes(b"foreign")
    foreign.chmod(0o600)
    before = (foreign.read_bytes(), foreign.stat().st_ino)

    with pytest.raises(LabLaunchdInstallError, match="foreign|authority|installation"):
        installer.uninstall(deactivate=True)

    assert (foreign.read_bytes(), foreign.stat().st_ino) == before


def test_installation_authorities_use_utf8_canonical_paths_and_reject_escaped_form(
    tmp_path: Path,
) -> None:
    unicode_root = tmp_path / "研究环境"
    repo, lock, launch_agents, _commit = _fixture(unicode_root)
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )
    installer.install(activate=False)
    local_state = lock.with_name(f"{lock.stem}.lab-local-install.json")
    payload = local_state.read_bytes()

    assert "研究环境".encode() in payload
    escaped = payload.replace("研究环境".encode(), b"\\u7814\\u7a76\\u73af\\u5883")
    assert escaped != payload
    local_state.write_bytes(escaped)

    with pytest.raises(LabLaunchdInstallError, match="invalid|canonical|authority"):
        installer.install(activate=False)


def test_uninstall_bootout_failure_preserves_files_states_and_loaded_labels(
    tmp_path: Path,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )
    installer.install(activate=False)
    state_paths = (
        lock.with_name(f"{lock.stem}.lab-local-install.json"),
        lock.with_name(f"{lock.stem}.lab-install.json"),
    )
    before_files = {
        path.name: (path.read_bytes(), path.stat().st_ino) for path in launch_agents.glob("*.plist")
    }
    before_states = {
        path: (path.read_bytes(), path.stat().st_ino) for path in state_paths if path.exists()
    }
    loaded = set(LAB_LAUNCHD_LABELS)
    failing = _LaunchdStateRunner(
        loaded=loaded,
        fail_bootout=LAB_LAUNCHD_LABELS[1],
    )

    with pytest.raises(LabLaunchdInstallError, match="bootout"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=failing,
        ).uninstall(deactivate=True)

    assert {
        path.name: (path.read_bytes(), path.stat().st_ino) for path in launch_agents.glob("*.plist")
    } == before_files
    assert {
        path: (path.read_bytes(), path.stat().st_ino) for path in state_paths if path.exists()
    } == before_states
    assert loaded == set(LAB_LAUNCHD_LABELS)


def test_install_requires_registered_authority_before_creating_plists(tmp_path: Path) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    registration = lock.with_name(f"{lock.stem}.lab-install.json")
    registration.unlink()

    with pytest.raises(LabLaunchdInstallError, match="registered|authority"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=_Runner(),
        ).install(activate=False)

    assert not list(launch_agents.glob("*.plist"))


@pytest.mark.parametrize(
    "fault_stage",
    [
        "transaction-prepared",
        *(f"plist-installed:{label}.plist" for label in LAB_LAUNCHD_LABELS),
        "registered-state-installed",
        "local-state-installed",
    ],
)
def test_interrupted_install_journal_restores_exact_authority_on_next_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault_stage: str,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    registration = lock.with_name(f"{lock.stem}.lab-install.json")
    registered_before = (registration.read_bytes(), registration.stat().st_ino)

    class SimulatedCrash(BaseException):
        pass

    def crash(stage: str) -> None:
        if stage == fault_stage:
            raise SimulatedCrash

    interrupted = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
        mutation_hook=crash,
    )
    monkeypatch.setattr(interrupted, "_recover_transaction", lambda: None)
    with pytest.raises(SimulatedCrash):
        interrupted.install(activate=False)

    journal = lock.with_name(f"{lock.stem}.lab-install-transaction.json")
    assert journal.is_file()

    completed = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)

    assert completed.code_sha
    assert not journal.exists()
    assert registered_before != (registration.read_bytes(), registration.stat().st_ino)
    registered = json.loads(registration.read_text(encoding="utf-8"))
    for label in LAB_LAUNCHD_LABELS:
        path = launch_agents / f"{label}.plist"
        assert registered["plists"][label]["inode"] == path.stat().st_ino


def test_transaction_update_cas_never_overwrites_concurrent_successor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )
    transaction = installer._begin_transaction(action="install", previously_loaded=set())
    journal = lock.with_name(f"{lock.stem}.lab-install-transaction.json")
    concurrent = {**transaction, "stage": "committed"}
    concurrent_bytes = canonical_json_bytes(concurrent, trailing_newline=True)
    real_rename = install_module.rename_noreplace_at
    injected = False

    def race(
        source_dir_fd: int,
        source_name: str,
        destination_dir_fd: int,
        destination_name: str,
    ) -> None:
        nonlocal injected
        if (
            not injected
            and source_name == journal.name
            and destination_name.endswith(".update-backup")
        ):
            injected = True
            os.unlink(source_name, dir_fd=source_dir_fd)
            descriptor = os.open(
                source_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=source_dir_fd,
            )
            os.write(descriptor, concurrent_bytes)
            os.fsync(descriptor)
            os.close(descriptor)
        real_rename(source_dir_fd, source_name, destination_dir_fd, destination_name)

    monkeypatch.setattr(install_module, "rename_noreplace_at", race)

    with pytest.raises(LabLaunchdInstallError, match="changed|CAS"):
        installer._save_transaction(transaction, stage="mutating")

    assert journal.read_bytes() == concurrent_bytes


def test_transaction_successor_accepts_one_time_replacement_arm() -> None:
    original = {
        "path": "/tmp/example",
        "backup": ".example.rollback",
        "existed": True,
        "sha256": "a" * 64,
        "device": 1,
        "inode": 2,
        "replacement_sha256": None,
        "replacement_device": None,
        "replacement_inode": None,
    }
    previous = {
        "schema_version": 1,
        "operation_id": "a" * 32,
        "action": "install",
        "stage": "mutating",
        "checkout_root": "/tmp/repo",
        "launch_agents_dir": "/tmp/agents",
        "previously_loaded": [],
        "files": [original],
    }
    current = {
        **previous,
        "files": [
            {
                **original,
                "replacement_sha256": "b" * 64,
                "replacement_device": 3,
                "replacement_inode": 4,
            }
        ],
    }

    assert LabLaunchdInstaller._transaction_successor(previous, current)


@pytest.mark.parametrize(
    "fault_stage",
    ("transaction-authority-quarantined", "transaction-authority-published"),
)
def test_transaction_authority_update_crash_is_reconciled(
    tmp_path: Path,
    fault_stage: str,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)

    class SimulatedCrash(BaseException):
        pass

    armed = False

    def crash(stage: str) -> None:
        nonlocal armed
        if armed and stage == fault_stage:
            raise SimulatedCrash

    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
        mutation_hook=crash,
    )
    transaction = installer._begin_transaction(action="install", previously_loaded=set())
    armed = True
    with pytest.raises(SimulatedCrash):
        installer._save_transaction(transaction, stage="mutating")

    recovered = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )._transaction()

    assert recovered["operation_id"] == transaction["operation_id"]
    assert not lock.with_name(f".{lock.stem}.lab-install-transaction.json.update-backup").exists()


def test_installation_transaction_lock_serializes_installer_and_handoff(
    tmp_path: Path,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    owner = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    )
    held = owner._acquire_installation_lock()
    try:
        with pytest.raises(LabLaunchdInstallError, match="transaction.*active"):
            LabLaunchdInstaller(
                checkout_root=repo,
                deployment_lock_path=lock,
                launch_agents_dir=launch_agents,
                trusted_git_path=TRUSTED_GIT,
                runner=_Runner(),
                command_timeout_seconds=0.05,
                overall_timeout_seconds=0.05,
            ).install(activate=False)
        assert not list(launch_agents.glob("*.plist"))
    finally:
        os.close(held)

    owner.install(activate=False)
    assert len(list(launch_agents.glob("*.plist"))) == len(LAB_LAUNCHD_LABELS)


def test_uninstall_removes_local_and_registered_authority_only_after_unload(
    tmp_path: Path,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    runner = _Runner()
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=runner,
    )
    installer.install(activate=True)

    installer.uninstall(deactivate=True)

    assert not list(launch_agents.glob("*.plist"))
    assert not lock.with_name(f"{lock.stem}.lab-local-install.json").exists()
    assert not lock.with_name(f"{lock.stem}.lab-install.json").exists()
    assert not lock.with_name(f"{lock.stem}.lab-install-transaction.json").exists()
    assert not runner.loaded


def test_rerun_stops_daemons_before_waiting_for_generation_exclusive_lock(
    tmp_path: Path,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=_Runner(),
    ).install(activate=False)
    held: dict[str, int] = {}
    for label in LAB_LAUNCHD_LABELS:
        descriptor = os.open(lock, os.O_RDONLY)
        fcntl.flock(descriptor, fcntl.LOCK_SH)
        held[label] = descriptor
    loaded = set(LAB_LAUNCHD_LABELS)
    runner = _LaunchdStateRunner(
        loaded=loaded,
        after_bootout={
            label: (lambda item=label: os.close(held.pop(item))) for label in LAB_LAUNCHD_LABELS
        },
    )
    try:
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=runner,
            command_timeout_seconds=1,
        ).install(activate=True)
    finally:
        for descriptor in held.values():
            os.close(descriptor)

    first_bootout = next(i for i, call in enumerate(runner.calls) if call[1:2] == ("bootout",))
    assert first_bootout >= 0
    assert loaded == set(LAB_LAUNCHD_LABELS)


def test_installed_generation_plists_converge_a_to_b_rollback_then_b(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, lock, launch_agents, _commit = _fixture(tmp_path)
    first_runner = _Runner()
    installer = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=first_runner,
    )
    installed_a = installer.install(activate=True)
    a_payloads = {path.name: path.read_bytes() for path in launch_agents.glob("*.plist")}

    generation_b = tmp_path / "generation-b"
    code_b = generation_b / "release"
    (code_b / "deploy" / "launchd").mkdir(parents=True)
    for label in LAB_LAUNCHD_LABELS:
        shutil.copy2(
            ROOT / "deploy" / "launchd" / f"{label}.plist",
            code_b / "deploy" / "launchd" / f"{label}.plist",
        )
    marker_b = SimpleNamespace(
        commit="b" * 40,
        environment_generation_id="c" * 64,
        venv_path=str(generation_b),
    )
    monkeypatch.setattr(
        LabLaunchdInstaller,
        "_active_generation",
        lambda *_args, **_kwargs: (marker_b, code_b),
    )
    failing_runner = _Runner(fail_on="kickstart")
    failing_runner.loaded = set(LAB_LAUNCHD_LABELS)
    with pytest.raises(LabLaunchdInstallError, match="launchctl"):
        LabLaunchdInstaller(
            checkout_root=repo,
            deployment_lock_path=lock,
            launch_agents_dir=launch_agents,
            trusted_git_path=TRUSTED_GIT,
            runner=failing_runner,
        ).install(activate=True)

    assert {path.name: path.read_bytes() for path in launch_agents.glob("*.plist")} == a_payloads

    healthy_runner = _Runner()
    healthy_runner.loaded = set(LAB_LAUNCHD_LABELS)
    installed_b = LabLaunchdInstaller(
        checkout_root=repo,
        deployment_lock_path=lock,
        launch_agents_dir=launch_agents,
        trusted_git_path=TRUSTED_GIT,
        runner=healthy_runner,
    ).install(activate=True)

    assert installed_a.environment_generation_id != installed_b.environment_generation_id
    assert installed_b == type(installed_b)(
        code_sha="b" * 40,
        environment_generation_id="c" * 64,
        launch_agents_dir=launch_agents,
    )
    for path in launch_agents.glob("*.plist"):
        assert str(generation_b) in path.read_text(encoding="utf-8")
    assert healthy_runner.loaded == set(LAB_LAUNCHD_LABELS)
