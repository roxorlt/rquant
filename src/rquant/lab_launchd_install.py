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
import time
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
_STATE_SCHEMA_VERSION = 2
_TRANSACTION_SCHEMA_VERSION = 1


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
        overall_timeout_seconds: float = 120,
        mutation_hook: Callable[[str], None] | None = None,
    ) -> None:
        self.checkout_root = _canonical(checkout_root, label="checkout root")
        self.lock_path = _canonical(deployment_lock_path, label="deployment lock")
        self.launch_agents_dir = _canonical(launch_agents_dir, label="LaunchAgents root")
        self.trusted_git_path = _canonical(trusted_git_path, label="trusted Git")
        self.worker_id = worker_id
        self.command_timeout_seconds = command_timeout_seconds
        if not 0 < command_timeout_seconds <= overall_timeout_seconds <= 600:
            raise LabLaunchdInstallError("launchd installation timeout is invalid")
        started = time.monotonic()
        self._hard_deadline = started + overall_timeout_seconds
        cleanup_reserve = min(
            5.0,
            max(0.01, overall_timeout_seconds * 0.2),
            overall_timeout_seconds * 0.4,
        )
        self._deadline = self._hard_deadline - cleanup_reserve
        self._in_recovery = False
        self._runner = runner or self._default_runner
        self._mutation_hook = mutation_hook or (lambda _stage: None)
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

    @property
    def _registered_state_path(self) -> Path:
        return self.lock_path.with_name(f"{self.lock_path.stem}.lab-install.json")

    @property
    def _transaction_path(self) -> Path:
        return self.lock_path.with_name(f"{self.lock_path.stem}.lab-install-transaction.json")

    def _run(self, command: list[str], *, label: str) -> subprocess.CompletedProcess[str]:
        try:
            result = self._runner(command, timeout=self._remaining())
        except (OSError, subprocess.SubprocessError) as exc:
            raise LabLaunchdInstallError(f"{label} failed") from exc
        if result.returncode != 0:
            raise LabLaunchdInstallError(f"{label} failed: {(result.stderr or '').strip()}")
        return result

    def _launchctl_loaded(self, label: str) -> bool:
        try:
            result = self._runner(
                ["/bin/launchctl", "print", f"gui/{os.getuid()}/{label}"],
                timeout=self._remaining(),
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise LabLaunchdInstallError("launchctl state failed") from exc
        if result.returncode == 0:
            return True
        if result.returncode in {3, 113}:
            return False
        raise LabLaunchdInstallError(f"launchctl state failed: {(result.stderr or '').strip()}")

    def _remaining(self) -> float:
        deadline = self._hard_deadline if self._in_recovery else self._deadline
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise LabLaunchdInstallError("launchd installation deadline expired")
        return min(self.command_timeout_seconds, remaining)

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

    def _acquire_named_lock(self, path: Path, *, label: str) -> int:
        _private_directory(self.lock_path.parent, label="deployment authority root")
        parent_fd = os.open(
            self.lock_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            descriptor = os.open(
                path.name,
                os.O_RDWR | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
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
            raise LabLaunchdInstallError(f"{label} is unsafe")
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as exc:
                if self._deadline - time.monotonic() <= 0:
                    os.close(descriptor)
                    raise LabLaunchdInstallError(f"{label} remains active") from exc
                time.sleep(min(0.02, self._remaining()))
        return descriptor

    def _acquire_generation_lock(self) -> int:
        return self._acquire_named_lock(self.lock_path, label="release generation")

    def _acquire_installation_lock(self) -> int:
        self._remaining()
        path = self.lock_path.with_name(f"{self.lock_path.stem}.handoff.lock")
        if not os.path.lexists(path):
            parent_fd = os.open(
                self.lock_path.parent,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                descriptor = os.open(
                    path.name,
                    os.O_RDWR | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                    0o600,
                    dir_fd=parent_fd,
                )
                os.fsync(descriptor)
                os.close(descriptor)
                os.fsync(parent_fd)
            except FileExistsError:
                pass
            finally:
                os.close(parent_fd)
        return self._acquire_named_lock(path, label="Lab installation transaction")

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
                command_timeout_seconds=self._remaining(),
                overall_deadline_monotonic=self._deadline,
            ).verify(expected_commit=marker_payload["commit"])
        except Exception as exc:
            raise LabLaunchdInstallError("active release generation is invalid") from exc
        return marker, code_root

    def _plist_payload(self, marker: object, code_root: Path, label: str) -> bytes:
        generation = Path(str(marker.venv_path))
        template = code_root / "deploy" / "launchd" / f"{label}.plist"
        try:
            self._remaining()
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
        self._remaining()
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
        self._remaining()
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
            self._remaining()
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

    @staticmethod
    def _file_binding(path: Path, payload: bytes | None = None) -> dict[str, object]:
        observed = _regular_identity(path, label=f"managed install file {path.name}")
        content = path.read_bytes() if payload is None else payload
        return {
            "path": str(path),
            "sha256": hashlib.sha256(content).hexdigest(),
            "device": observed.st_dev,
            "inode": observed.st_ino,
        }

    def _transaction(self) -> dict[str, object]:
        try:
            _regular_identity(self._transaction_path, label="Lab installation transaction")
            payload = strict_json_loads(self._transaction_path.read_bytes())
        except (OSError, StrictJsonError) as exc:
            raise LabLaunchdInstallError("Lab installation transaction is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {
                "schema_version",
                "operation_id",
                "action",
                "stage",
                "checkout_root",
                "launch_agents_dir",
                "previously_loaded",
                "files",
            }
            or payload.get("schema_version") != _TRANSACTION_SCHEMA_VERSION
            or payload.get("action") not in {"install", "uninstall"}
            or payload.get("stage") not in {"prepared", "mutating", "committed"}
            or payload.get("checkout_root") != str(self.checkout_root)
            or payload.get("launch_agents_dir") != str(self.launch_agents_dir)
            or not isinstance(payload.get("operation_id"), str)
            or not isinstance(payload.get("previously_loaded"), list)
            or not isinstance(payload.get("files"), list)
        ):
            raise LabLaunchdInstallError("Lab installation transaction is invalid")
        loaded = payload["previously_loaded"]
        if any(type(label) is not str or label not in LAB_LAUNCHD_LABELS for label in loaded):
            raise LabLaunchdInstallError("Lab installation transaction labels are invalid")
        for item in payload["files"]:
            if (
                not isinstance(item, dict)
                or set(item)
                != {
                    "path",
                    "backup",
                    "existed",
                    "sha256",
                    "device",
                    "inode",
                }
                or type(item.get("existed")) is not bool
                or not isinstance(item.get("path"), str)
                or not isinstance(item.get("backup"), str)
            ):
                raise LabLaunchdInstallError("Lab installation transaction file is invalid")
            if item["existed"]:
                if (
                    not isinstance(item.get("sha256"), str)
                    or type(item.get("device")) is not int
                    or type(item.get("inode")) is not int
                ):
                    raise LabLaunchdInstallError("Lab installation transaction identity is invalid")
            elif any(item.get(key) is not None for key in ("sha256", "device", "inode")):
                raise LabLaunchdInstallError("Lab installation transaction identity is invalid")
            self._validated_managed_path(Path(item["path"]))
        return payload

    def _validated_managed_path(self, path: Path) -> Path:
        allowed = {
            *(self.launch_agents_dir / f"{label}.plist" for label in LAB_LAUNCHD_LABELS),
            self._state_path,
            self._registered_state_path,
        }
        if path not in allowed or not path.is_absolute() or path != Path(os.path.abspath(path)):
            raise LabLaunchdInstallError("Lab installation transaction path escaped authority")
        return path

    def _begin_transaction(self, *, action: str, previously_loaded: set[str]) -> dict[str, object]:
        self._remaining()
        if os.path.lexists(self._transaction_path):
            raise LabLaunchdInstallError("unfinished Lab installation transaction remains")
        payload: dict[str, object] = {
            "schema_version": _TRANSACTION_SCHEMA_VERSION,
            "operation_id": secrets.token_hex(16),
            "action": action,
            "stage": "prepared",
            "checkout_root": str(self.checkout_root),
            "launch_agents_dir": str(self.launch_agents_dir),
            "previously_loaded": sorted(previously_loaded),
            "files": [],
        }
        self._write_state(payload, path=self._transaction_path)
        self._mutation_hook("transaction-prepared")
        return payload

    def _save_transaction(self, payload: dict[str, object], *, stage: str) -> None:
        self._remaining()
        updated = {**payload, "stage": stage}
        self._write_state(updated, path=self._transaction_path)
        payload.clear()
        payload.update(updated)

    def _record_transaction_file(self, payload: dict[str, object], path: Path) -> str:
        path = self._validated_managed_path(path)
        operation_id = str(payload["operation_id"])
        backup = f".{path.name}.{operation_id}.rollback"
        if any(item.get("path") == str(path) for item in payload["files"]):
            raise LabLaunchdInstallError("managed install file was staged twice")
        if os.path.lexists(path.parent / backup):
            raise LabLaunchdInstallError("stale launchd installation rollback exists")
        if os.path.lexists(path):
            binding = self._file_binding(path)
            item = {
                "path": str(path),
                "backup": backup,
                "existed": True,
                "sha256": binding["sha256"],
                "device": binding["device"],
                "inode": binding["inode"],
            }
        else:
            item = {
                "path": str(path),
                "backup": backup,
                "existed": False,
                "sha256": None,
                "device": None,
                "inode": None,
            }
        payload["files"].append(item)
        self._save_transaction(payload, stage="mutating")
        return backup

    def _rename_original_to_backup(
        self,
        path: Path,
        backup: str,
        *,
        existed: bool,
    ) -> None:
        if not existed:
            return
        root_fd = os.open(
            path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.rename(path.name, backup, src_dir_fd=root_fd, dst_dir_fd=root_fd)
            os.fsync(root_fd)
        finally:
            os.close(root_fd)

    def _stage_replacement(
        self,
        transaction: dict[str, object],
        path: Path,
        payload: bytes,
    ) -> bool:
        if os.path.lexists(path) and path.read_bytes() == payload:
            return False
        backup = self._record_transaction_file(transaction, path)
        self._rename_original_to_backup(
            path,
            backup,
            existed=bool(transaction["files"][-1]["existed"]),
        )
        return True

    def _stage_removal(self, transaction: dict[str, object], path: Path) -> None:
        backup = self._record_transaction_file(transaction, path)
        self._rename_original_to_backup(path, backup, existed=True)

    def _remove_transaction(self) -> None:
        root_fd = os.open(
            self.lock_path.parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            os.unlink(self._transaction_path.name, dir_fd=root_fd)
            os.fsync(root_fd)
        finally:
            os.close(root_fd)

    def _recover_transaction(self) -> None:
        if not os.path.lexists(self._transaction_path):
            return
        prior_recovery = self._in_recovery
        self._in_recovery = True
        try:
            payload = self._transaction()
            committed = payload["stage"] == "committed"
            if not committed:
                for item in reversed(payload["files"]):
                    path = Path(item["path"])
                    backup = path.parent / str(item["backup"])
                    root_fd = os.open(
                        path.parent,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                    )
                    try:
                        if os.path.lexists(backup):
                            with suppress(FileNotFoundError):
                                os.unlink(path.name, dir_fd=root_fd)
                            os.rename(
                                backup.name,
                                path.name,
                                src_dir_fd=root_fd,
                                dst_dir_fd=root_fd,
                            )
                        elif item["existed"]:
                            binding = self._file_binding(path)
                            if any(
                                binding[key] != item[key] for key in ("sha256", "device", "inode")
                            ):
                                raise LabLaunchdInstallError(
                                    "Lab installation rollback identity changed"
                                )
                        else:
                            with suppress(FileNotFoundError):
                                os.unlink(path.name, dir_fd=root_fd)
                        os.fsync(root_fd)
                    finally:
                        os.close(root_fd)
                rollback_errors: list[str] = []
                for label in LAB_LAUNCHD_LABELS:
                    try:
                        self._bootout_if_loaded(label)
                    except LabLaunchdInstallError as exc:
                        rollback_errors.append(str(exc))
                for label in payload["previously_loaded"]:
                    try:
                        self._bootstrap(label)
                    except LabLaunchdInstallError as exc:
                        rollback_errors.append(str(exc))
                if rollback_errors:
                    raise LabLaunchdInstallError(
                        "Lab installation rollback failed: " + "; ".join(rollback_errors)
                    )
            for item in payload["files"]:
                path = Path(item["path"])
                backup = path.parent / str(item["backup"])
                if os.path.lexists(backup):
                    _regular_identity(backup, label="Lab installation rollback file")
                    root_fd = os.open(
                        path.parent,
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
                    )
                    try:
                        os.unlink(backup.name, dir_fd=root_fd)
                        os.fsync(root_fd)
                    finally:
                        os.close(root_fd)
            self._remove_transaction()
        finally:
            self._in_recovery = prior_recovery

    def _state(self) -> dict[str, object]:
        try:
            payload = strict_json_loads(self._state_path.read_bytes())
        except (OSError, StrictJsonError) as exc:
            raise LabLaunchdInstallError("Lab launchd installation state is unavailable") from exc
        if not isinstance(payload, dict) or payload.get("schema_version") != _STATE_SCHEMA_VERSION:
            raise LabLaunchdInstallError("Lab launchd installation state is invalid")
        return payload

    def _registered_state(self) -> dict[str, object]:
        try:
            _regular_identity(
                self._registered_state_path,
                label="Lab handoff installation state",
            )
            payload = strict_json_loads(self._registered_state_path.read_bytes())
        except (OSError, StrictJsonError, LabLaunchdInstallError) as exc:
            raise LabLaunchdInstallError(
                "registered Lab installation authority is unavailable"
            ) from exc
        if (
            not isinstance(payload, dict)
            or payload.get("schema_version") != 2
            or payload.get("checkout_root") != str(self.checkout_root)
            or payload.get("labels") != list(LAB_LAUNCHD_LABELS)
            or not isinstance(payload.get("plists"), dict)
            or set(payload["plists"]) != set(LAB_LAUNCHD_LABELS)
        ):
            raise LabLaunchdInstallError("registered Lab installation authority is invalid")
        return payload

    @staticmethod
    def _validate_binding(
        binding: object,
        path: Path,
        *,
        require_private: bool = True,
    ) -> dict[str, object]:
        if not isinstance(binding, dict) or set(binding) != {
            "path",
            "sha256",
            "device",
            "inode",
        }:
            raise LabLaunchdInstallError("installed launchd plist binding is invalid")
        try:
            observed = path.lstat()
        except OSError as exc:
            raise LabLaunchdInstallError("registered launchd plist is unavailable") from exc
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != os.getuid()
            or observed.st_nlink != 1
            or (require_private and stat.S_IMODE(observed.st_mode) != 0o600)
        ):
            raise LabLaunchdInstallError("registered launchd plist identity is unsafe")
        if (
            binding.get("path") != str(path)
            or binding.get("sha256") != hashlib.sha256(path.read_bytes()).hexdigest()
            or type(binding.get("device")) is not int
            or type(binding.get("inode")) is not int
            or binding.get("device") != observed.st_dev
            or binding.get("inode") != observed.st_ino
        ):
            raise LabLaunchdInstallError("installed launchd plist changed")
        return binding

    def _validate_existing_installation(self) -> dict[str, object] | None:
        registered = self._registered_state()
        existing_paths = [self.launch_agents_dir / f"{label}.plist" for label in LAB_LAUNCHD_LABELS]
        if not os.path.lexists(self._state_path):
            if any(os.path.lexists(path) for path in existing_paths):
                raise LabLaunchdInstallError("foreign unregistered launchd plist exists")
            for label in LAB_LAUNCHD_LABELS:
                source = self.checkout_root / "deploy" / "launchd" / f"{label}.plist"
                self._validate_binding(
                    registered["plists"][label],
                    source,
                    require_private=False,
                )
            return None
        state = self._state()
        bindings = state.get("plists")
        if not isinstance(bindings, dict) or set(bindings) != {
            f"{label}.plist" for label in LAB_LAUNCHD_LABELS
        }:
            raise LabLaunchdInstallError("Lab launchd installation state is invalid")
        for name, binding in bindings.items():
            path = self.launch_agents_dir / name
            local = self._validate_binding(binding, path)
            label = name.removesuffix(".plist")
            if registered["plists"].get(label) != local:
                raise LabLaunchdInstallError(
                    "local and registered Lab installation authority diverged"
                )
        return state

    def install(self, *, activate: bool) -> LabLaunchdInstallation:
        self._remaining()
        self._ensure_launch_agents()
        installation_lock_fd = self._acquire_installation_lock()
        lock_fd = -1
        transaction: dict[str, object] | None = None
        try:
            self._recover_transaction()
            self._validate_existing_installation()
            previously_loaded: set[str] = set()
            if activate:
                previously_loaded = {
                    label for label in LAB_LAUNCHD_LABELS if self._launchctl_loaded(label)
                }
            transaction = self._begin_transaction(
                action="install",
                previously_loaded=previously_loaded,
            )
            if activate:
                for label in LAB_LAUNCHD_LABELS:
                    self._bootout_if_loaded(label)
                if any(self._launchctl_loaded(label) for label in LAB_LAUNCHD_LABELS):
                    raise LabLaunchdInstallError("Lab daemons did not unload")
            lock_fd = self._acquire_generation_lock()
            marker, code_root = self._active_generation(lock_fd)
            payloads = {
                f"{label}.plist": self._plist_payload(marker, code_root, label)
                for label in LAB_LAUNCHD_LABELS
            }
            for name, payload in payloads.items():
                self._remaining()
                path = self.launch_agents_dir / name
                changed = self._stage_replacement(
                    transaction,
                    path,
                    payload,
                )
                if changed:
                    self._replace(name, payload)
                    self._mutation_hook(f"plist-installed:{name}")
            if activate:
                for label in LAB_LAUNCHD_LABELS:
                    self._remaining()
                    self._bootstrap(label)
            plist_bindings = {
                name: self._file_binding(self.launch_agents_dir / name) for name in payloads
            }
            state: dict[str, object] = {
                "schema_version": _STATE_SCHEMA_VERSION,
                "code_sha": marker.commit,
                "environment_generation_id": marker.environment_generation_id,
                "launch_agents_dir": str(self.launch_agents_dir),
                "plists": plist_bindings,
            }
            registered = {
                **self._registered_state(),
                "registered_by_commit": marker.commit,
                "plists": {label: plist_bindings[f"{label}.plist"] for label in LAB_LAUNCHD_LABELS},
            }
            encoded_registered = (
                json.dumps(registered, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode()
            if self._stage_replacement(
                transaction,
                self._registered_state_path,
                encoded_registered,
            ):
                self._write_state(registered, path=self._registered_state_path)
                self._mutation_hook("registered-state-installed")
            encoded_state = (
                json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n"
            ).encode()
            if self._stage_replacement(
                transaction,
                self._state_path,
                encoded_state,
            ):
                self._write_state(state)
                self._mutation_hook("local-state-installed")
            self._save_transaction(transaction, stage="committed")
            self._recover_transaction()
            transaction = None
            return LabLaunchdInstallation(
                code_sha=marker.commit,
                environment_generation_id=marker.environment_generation_id,
                launch_agents_dir=self.launch_agents_dir,
            )
        except BaseException:
            if transaction is not None:
                self._recover_transaction()
            raise
        finally:
            if lock_fd >= 0:
                os.close(lock_fd)
            os.close(installation_lock_fd)

    def uninstall(self, *, deactivate: bool) -> None:
        self._remaining()
        _private_directory(self.launch_agents_dir, label="LaunchAgents root")
        installation_lock_fd = self._acquire_installation_lock()
        transaction: dict[str, object] | None = None
        try:
            self._recover_transaction()
            state = self._validate_existing_installation()
            if state is None:
                raise LabLaunchdInstallError("Lab launchd installation state is unavailable")
            plists = state.get("plists")
            if not isinstance(plists, dict) or set(plists) != {
                f"{label}.plist" for label in LAB_LAUNCHD_LABELS
            }:
                raise LabLaunchdInstallError("Lab launchd installation state is invalid")
            previously_loaded: set[str] = set()
            if deactivate:
                previously_loaded = {
                    label for label in LAB_LAUNCHD_LABELS if self._launchctl_loaded(label)
                }
            transaction = self._begin_transaction(
                action="uninstall",
                previously_loaded=previously_loaded,
            )
            if deactivate:
                for label in LAB_LAUNCHD_LABELS:
                    try:
                        self._bootout_if_loaded(label)
                    except LabLaunchdInstallError:
                        for restore in previously_loaded:
                            if not self._launchctl_loaded(restore):
                                self._bootstrap(restore)
                        raise
                if any(self._launchctl_loaded(label) for label in LAB_LAUNCHD_LABELS):
                    raise LabLaunchdInstallError("Lab daemons did not unload")
            for name in plists:
                self._stage_removal(transaction, self.launch_agents_dir / name)
            self._stage_removal(transaction, self._state_path)
            self._stage_removal(transaction, self._registered_state_path)
            self._save_transaction(transaction, stage="committed")
            self._recover_transaction()
            transaction = None
        except BaseException:
            if transaction is not None:
                self._recover_transaction()
            raise
        finally:
            os.close(installation_lock_fd)
