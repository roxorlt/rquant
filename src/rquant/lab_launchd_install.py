"""Generation-bound local launchd installation for Strategy Lab daemons."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import plistlib
import secrets
import stat
import subprocess
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from rquant.release_generation import (
    ReleaseGenerationAuthority,
    generation_code_root,
)
from rquant.research_manifest import bind_trusted_git_executable
from rquant.strict_json import StrictJsonError, strict_json_loads

LAB_LAUNCHD_LABELS = (
    "com.roxor.rquant-lab-scheduler",
    "com.roxor.rquant-lab-worker",
    "com.roxor.rquant-lab-finalizer",
)
_STATE_SCHEMA_VERSION = 1


class LabLaunchdInstallError(RuntimeError):
    pass


@dataclass(frozen=True)
class LabLaunchdInstallation:
    code_sha: str
    environment_generation_id: str
    launch_agents_dir: Path


Runner = Callable[[list[str]], subprocess.CompletedProcess[str]]


def _canonical(path: Path, *, label: str) -> Path:
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise LabLaunchdInstallError(f"{label} must be an absolute canonical path")
    return path


def _private_directory(path: Path, *, label: str) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise LabLaunchdInstallError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
        or path.resolve(strict=True) != path
    ):
        raise LabLaunchdInstallError(f"{label} must be an owned physical private directory")
    return observed


def _regular_identity(path: Path, *, label: str) -> os.stat_result:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise LabLaunchdInstallError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_nlink != 1
        or stat.S_IMODE(observed.st_mode) != 0o600
    ):
        raise LabLaunchdInstallError(f"{label} must be an owned physical 0600 file")
    return observed


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise LabLaunchdInstallError("launchd installation write was incomplete")
        offset += written


class LabLaunchdInstaller:
    def __init__(
        self,
        *,
        checkout_root: Path,
        deployment_lock_path: Path,
        launch_agents_dir: Path,
        trusted_git_path: Path,
        runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
        worker_id: str = "rquant-mac-primary",
        command_timeout_seconds: float = 30,
    ) -> None:
        self.checkout_root = _canonical(checkout_root, label="checkout root")
        self.lock_path = _canonical(deployment_lock_path, label="deployment lock")
        self.launch_agents_dir = _canonical(launch_agents_dir, label="LaunchAgents root")
        self.trusted_git_path = _canonical(trusted_git_path, label="trusted Git")
        self.worker_id = worker_id
        self.command_timeout_seconds = command_timeout_seconds
        self._runner = runner or self._default_runner
        if not worker_id or any(character.isspace() for character in worker_id):
            raise LabLaunchdInstallError("worker id is invalid")

    @staticmethod
    def _default_runner(command: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )

    @property
    def _state_path(self) -> Path:
        return self.lock_path.with_name(f"{self.lock_path.stem}.lab-local-install.json")

    def _run(self, command: list[str], *, label: str) -> subprocess.CompletedProcess[str]:
        try:
            result = self._runner(command, timeout=self.command_timeout_seconds)
        except (OSError, subprocess.SubprocessError) as exc:
            raise LabLaunchdInstallError(f"{label} failed") from exc
        if result.returncode != 0:
            raise LabLaunchdInstallError(f"{label} failed: {(result.stderr or '').strip()}")
        return result

    def _launchctl_loaded(self, label: str) -> bool:
        result = self._runner(
            ["/bin/launchctl", "print", f"gui/{os.getuid()}/{label}"],
            timeout=self.command_timeout_seconds,
        )
        if result.returncode == 0:
            return True
        if result.returncode in {3, 113}:
            return False
        raise LabLaunchdInstallError(f"launchctl state failed: {(result.stderr or '').strip()}")

    def _bootout_if_loaded(self, label: str) -> None:
        if not self._launchctl_loaded(label):
            return
        self._run(
            ["/bin/launchctl", "bootout", f"gui/{os.getuid()}/{label}"],
            label="launchctl bootout",
        )

    def _bootstrap(self, label: str) -> None:
        domain = f"gui/{os.getuid()}"
        self._run(
            [
                "/bin/launchctl",
                "bootstrap",
                domain,
                str(self.launch_agents_dir / f"{label}.plist"),
            ],
            label="launchctl bootstrap",
        )
        self._run(
            ["/bin/launchctl", "kickstart", f"{domain}/{label}"],
            label="launchctl kickstart",
        )

    def _ensure_launch_agents(self) -> None:
        if os.path.lexists(self.launch_agents_dir):
            _private_directory(self.launch_agents_dir, label="LaunchAgents root")
            return
        parent = self.launch_agents_dir.parent
        _private_directory(parent, label="LaunchAgents parent")
        parent_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.mkdir(self.launch_agents_dir.name, 0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
        except FileExistsError as exc:
            raise LabLaunchdInstallError("LaunchAgents root appeared concurrently") from exc
        finally:
            os.close(parent_fd)
        _private_directory(self.launch_agents_dir, label="LaunchAgents root")

    def _acquire_lock(self) -> int:
        _private_directory(self.lock_path.parent, label="deployment authority root")
        parent_fd = os.open(
            self.lock_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            descriptor = os.open(
                self.lock_path.name,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(self.lock_path.name, dir_fd=parent_fd, follow_symlinks=False)
        finally:
            os.close(parent_fd)
        if (
            (opened.st_dev, opened.st_ino) != (active.st_dev, active.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise LabLaunchdInstallError("deployment lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(descriptor)
            raise LabLaunchdInstallError("release generation is active") from exc
        return descriptor

    def _active_generation(self, lock_fd: int) -> tuple[object, Path]:
        trusted_git = bind_trusted_git_executable(self.trusted_git_path)
        marker_payload = strict_json_loads(
            self.lock_path.with_name(f"{self.lock_path.stem}.complete.json").read_bytes()
        )
        if not isinstance(marker_payload, dict) or not isinstance(
            marker_payload.get("commit"), str
        ):
            raise LabLaunchdInstallError("release generation marker is invalid")
        environment = Path(str(marker_payload.get("venv_path", "")))
        code_root = generation_code_root(environment)
        try:
            marker = ReleaseGenerationAuthority(
                repo=code_root,
                immutable_code_root=code_root,
                lock_path=self.lock_path,
                lock_fd=lock_fd,
                python_path=environment / "bin" / "python",
                git_path=trusted_git.path,
            ).verify(expected_commit=marker_payload["commit"])
        except Exception as exc:
            raise LabLaunchdInstallError("active release generation is invalid") from exc
        return marker, code_root

    def _plist_payload(self, marker: object, code_root: Path, label: str) -> bytes:
        generation = Path(str(marker.venv_path))
        template = code_root / "deploy" / "launchd" / f"{label}.plist"
        try:
            with template.open("rb") as stream:
                document = plistlib.load(stream)
        except (OSError, plistlib.InvalidFileException) as exc:
            raise LabLaunchdInstallError("immutable launchd plist template is invalid") from exc
        replacements = {
            "__RQUANT_GENERATION_PYTHON__": str(generation / "bin" / "python"),
            "__RQUANT_CODE_ROOT__": str(code_root),
            "__RQUANT_COMMIT__": str(marker.commit),
            "__RQUANT_TRUSTED_GIT__": str(self.trusted_git_path),
            "__RQUANT_DEPLOYMENT_LOCK__": str(self.lock_path),
            "__RQUANT_LAUNCHER__": str(generation / "bin" / "rquant"),
            "__RQUANT_WORKER_ID__": self.worker_id,
            "__RQUANT_STDOUT__": str(self.launch_agents_dir / f"{label}.stdout.log"),
            "__RQUANT_STDERR__": str(self.launch_agents_dir / f"{label}.stderr.log"),
        }

        def substitute(value: object) -> object:
            if isinstance(value, str):
                for token, replacement in replacements.items():
                    value = value.replace(token, replacement)
                if "__RQUANT_" in value:
                    raise LabLaunchdInstallError("launchd plist contains an unresolved token")
                return value
            if isinstance(value, list):
                return [substitute(item) for item in value]
            if isinstance(value, dict):
                return {key: substitute(item) for key, item in value.items()}
            return value

        document = substitute(document)
        if not isinstance(document, dict) or document.get("Label") != label:
            raise LabLaunchdInstallError("immutable launchd plist label is invalid")
        return plistlib.dumps(document, fmt=plistlib.FMT_XML, sort_keys=True)

    def _read_existing(self, name: str) -> bytes | None:
        path = self.launch_agents_dir / name
        if not os.path.lexists(path):
            return None
        _regular_identity(path, label=f"installed launchd plist {name}")
        return path.read_bytes()

    def _replace(self, name: str, payload: bytes) -> None:
        current = self._read_existing(name)
        if current == payload:
            return
        root_fd = os.open(
            self.launch_agents_dir,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        temporary = f".{name}.{secrets.token_hex(8)}.tmp"
        descriptor = -1
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_fd,
            )
            _write_all(descriptor, payload)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            temporary_path = self.launch_agents_dir / temporary
            with temporary_path.open("rb") as stream:
                plistlib.load(stream)
            self._run(["/usr/bin/plutil", "-lint", str(temporary_path)], label="plutil lint")
            if os.path.lexists(self.launch_agents_dir / name):
                _regular_identity(
                    self.launch_agents_dir / name,
                    label=f"installed launchd plist {name}",
                )
            os.replace(temporary, name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
            os.fsync(root_fd)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=root_fd)
            os.close(root_fd)

    def _write_state(self, payload: dict[str, object], *, path: Path | None = None) -> None:
        state_path = self._state_path if path is None else path
        encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        root_fd = os.open(
            self.lock_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        temporary = f".{state_path.name}.{secrets.token_hex(8)}.tmp"
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=root_fd,
        )
        try:
            _write_all(descriptor, encoded)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, state_path.name, src_dir_fd=root_fd, dst_dir_fd=root_fd)
            os.fsync(root_fd)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=root_fd)
            os.close(root_fd)

    def _state(self) -> dict[str, object]:
        try:
            payload = strict_json_loads(self._state_path.read_bytes())
        except (OSError, StrictJsonError) as exc:
            raise LabLaunchdInstallError("Lab launchd installation state is unavailable") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != _STATE_SCHEMA_VERSION:
            raise LabLaunchdInstallError("Lab launchd installation state is invalid")
        return payload

    def install(self, *, activate: bool) -> LabLaunchdInstallation:
        self._ensure_launch_agents()
        lock_fd = self._acquire_lock()
        snapshots: dict[str, bytes | None] = {}
        previously_loaded: set[str] = set()
        activation_started = False
        try:
            marker, code_root = self._active_generation(lock_fd)
            payloads = {
                f"{label}.plist": self._plist_payload(marker, code_root, label)
                for label in LAB_LAUNCHD_LABELS
            }
            snapshots = {name: self._read_existing(name) for name in payloads}
            if activate:
                previously_loaded = {
                    label
                    for label in LAB_LAUNCHD_LABELS
                    if snapshots[f"{label}.plist"] is not None and self._launchctl_loaded(label)
                }
            for name, payload in payloads.items():
                self._replace(name, payload)
            if activate:
                activation_started = True
                for label in LAB_LAUNCHD_LABELS:
                    self._bootout_if_loaded(label)
                    self._bootstrap(label)
            state = {
                "schema_version": _STATE_SCHEMA_VERSION,
                "code_sha": marker.commit,
                "environment_generation_id": marker.environment_generation_id,
                "launch_agents_dir": str(self.launch_agents_dir),
                "plists": {
                    name: {
                        "path": str(self.launch_agents_dir / name),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                    for name, payload in payloads.items()
                },
            }
            handoff_installation = self.lock_path.with_name(
                f"{self.lock_path.stem}.lab-install.json"
            )
            if os.path.lexists(handoff_installation):
                try:
                    registered = strict_json_loads(handoff_installation.read_bytes())
                except (OSError, StrictJsonError) as exc:
                    raise LabLaunchdInstallError(
                        "Lab handoff installation state is invalid"
                    ) from exc
                if (
                    not isinstance(registered, dict)
                    or registered.get("schema_version") != 2
                    or registered.get("checkout_root") != str(self.checkout_root)
                    or registered.get("labels") != list(LAB_LAUNCHD_LABELS)
                ):
                    raise LabLaunchdInstallError("Lab handoff installation state is invalid")
                registered = {
                    **registered,
                    "registered_by_commit": marker.commit,
                    "plists": {
                        label: {
                            "path": str(self.launch_agents_dir / f"{label}.plist"),
                            "sha256": hashlib.sha256(payloads[f"{label}.plist"]).hexdigest(),
                            "device": (self.launch_agents_dir / f"{label}.plist").stat().st_dev,
                            "inode": (self.launch_agents_dir / f"{label}.plist").stat().st_ino,
                        }
                        for label in LAB_LAUNCHD_LABELS
                    },
                }
                self._write_state(registered, path=handoff_installation)
            self._write_state(state)
            return LabLaunchdInstallation(
                code_sha=marker.commit,
                environment_generation_id=marker.environment_generation_id,
                launch_agents_dir=self.launch_agents_dir,
            )
        except BaseException as exc:
            for name, payload in snapshots.items():
                path = self.launch_agents_dir / name
                if payload is None:
                    with suppress(FileNotFoundError):
                        path.unlink()
                else:
                    self._replace(name, payload)
            rollback_errors: list[str] = []
            if activation_started:
                for label in LAB_LAUNCHD_LABELS:
                    try:
                        self._bootout_if_loaded(label)
                    except LabLaunchdInstallError as rollback_exc:
                        rollback_errors.append(str(rollback_exc))
                for label in LAB_LAUNCHD_LABELS:
                    if label not in previously_loaded:
                        continue
                    try:
                        self._bootstrap(label)
                    except LabLaunchdInstallError as rollback_exc:
                        rollback_errors.append(str(rollback_exc))
            if rollback_errors:
                raise LabLaunchdInstallError(
                    f"{exc}; launchd rollback failed: {'; '.join(rollback_errors)}"
                ) from exc
            raise
        finally:
            os.close(lock_fd)

    def uninstall(self, *, deactivate: bool) -> None:
        _private_directory(self.launch_agents_dir, label="LaunchAgents root")
        lock_fd = self._acquire_lock()
        try:
            state = self._state()
            plists = state.get("plists")
            if not isinstance(plists, dict) or set(plists) != {
                f"{label}.plist" for label in LAB_LAUNCHD_LABELS
            }:
                raise LabLaunchdInstallError("Lab launchd installation state is invalid")
            for name, binding in plists.items():
                path = self.launch_agents_dir / name
                current = self._read_existing(name)
                if (
                    not isinstance(binding, dict)
                    or binding.get("path") != str(path)
                    or current is None
                    or binding.get("sha256") != hashlib.sha256(current).hexdigest()
                ):
                    raise LabLaunchdInstallError("installed launchd plist changed")
            if deactivate:
                domain = f"gui/{os.getuid()}"
                for label in LAB_LAUNCHD_LABELS:
                    self._runner(
                        ["/bin/launchctl", "bootout", f"{domain}/{label}"],
                        timeout=self.command_timeout_seconds,
                    )
            root_fd = os.open(
                self.launch_agents_dir,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                for name in plists:
                    os.unlink(name, dir_fd=root_fd)
                os.fsync(root_fd)
            finally:
                os.close(root_fd)
            self._state_path.unlink()
        finally:
            os.close(lock_fd)
