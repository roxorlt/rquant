"""Git-free execution binding for an attested immutable runtime generation."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping
from contextlib import suppress
from pathlib import Path, PurePosixPath
from typing import Literal

from pydantic import Field

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    SecureRegularFileLease,
    open_secure_regular_file_lease,
)
from rquant.fd_exec import DescriptorExecutionError, exec_verified_descriptor
from rquant.runtime_code_attestation import CodeTrustEvidence
from rquant.runtime_code_generation import (
    RuntimeCodeGenerationCapability,
    RuntimeCodeGenerationError,
)
from rquant.runtime_contracts import RuntimeContractModel

_ROUTING_PREFIXES = ("GIT_", "PYTHON", "DYLD_", "LD_")
_REQUEST_FD_PLACEHOLDER = "{rquant-request-fd}"
_RECEIPT_FD_PLACEHOLDER = "{rquant-receipt-fd}"
_LAUNCHER_FD_PLACEHOLDER = "{rquant-launcher-fd}"
_FD_LAUNCH_BOOTSTRAP = "\n".join(
    (
        "import json, runpy, sys",
        "roots = json.loads(sys.argv[1])",
        "launcher = sys.argv[2]",
        "launcher_fd = sys.argv[3]",
        "arguments = sys.argv[4:]",
        "sys.path[:0] = roots",
        "sys.argv = [launcher, *arguments]",
        "runpy.run_path('/dev/fd/' + launcher_fd, run_name='__main__')",
    )
)
FORMAL_SMOKE_BOOTSTRAP_SHA256 = hashlib.sha256(_FD_LAUNCH_BOOTSTRAP.encode("utf-8")).hexdigest()


class FormalRuntimeError(RuntimeError):
    """A formal runtime cannot be bound to its attested execution contract."""


def _require_live_generation(capability: RuntimeCodeGenerationCapability) -> None:
    try:
        capability.require_live()
    except (AuthorityPathSecurityError, RuntimeCodeGenerationError) as exc:
        raise FormalRuntimeError("formal runtime generation validation failed") from exc


class FormalRuntimeAudit(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-runtime-audit/v1"] = "rquant-formal-runtime-audit/v1"
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    attestation_verified: Literal[True] = True
    promotion_current_verified: Literal[True] = True
    generation_verified: Literal[True] = True
    execution_binding_verified: Literal[True] = True
    target_package_imported_before_verification: Literal[False] = False
    git_process_spawned: Literal[False] = False
    git_metadata_opened: Literal[False] = False
    events: tuple[str, ...] = (
        "current-verified",
        "attestation-verified",
        "promotion-current-verified",
        "bundle-tree-verified",
        "execution-binding-verified",
    )


class FormalRuntimeLaunchPlan(RuntimeContractModel):
    schema_version: Literal[1] = 1
    contract: Literal["rquant-formal-runtime-launch-plan/v1"] = (
        "rquant-formal-runtime-launch-plan/v1"
    )
    evidence: CodeTrustEvidence
    interpreter: Path
    launcher: Path
    launcher_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    launcher_via_descriptor: bool = False
    bootstrap_sha256: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    argv: tuple[str, ...] = Field(min_length=4)
    working_directory: Path
    import_roots: tuple[Path, ...] = Field(min_length=1)
    environment: dict[str, str]
    audit: FormalRuntimeAudit


class FormalRuntimeSession:
    """Retain code and interpreter identities until the final exec boundary."""

    def __init__(
        self,
        *,
        capability: RuntimeCodeGenerationCapability,
        interpreter_lease: SecureRegularFileLease,
        launcher_lease: SecureRegularFileLease,
        plan: FormalRuntimeLaunchPlan,
    ) -> None:
        self.capability = capability
        self._interpreter_lease = interpreter_lease
        self._launcher_lease = launcher_lease
        self.plan = plan
        self._closed = False

    def require_live(self) -> CodeTrustEvidence:
        if self._closed:
            raise FormalRuntimeError("formal runtime session is closed")
        _require_live_generation(self.capability)
        try:
            self._interpreter_lease.require_unchanged()
            self._launcher_lease.require_unchanged()
        except AuthorityPathSecurityError as exc:
            raise FormalRuntimeError("formal runtime generation validation failed") from exc
        return self.plan.evidence

    def require_interpreter_descriptor(self) -> int:
        self.require_live()
        try:
            return self._interpreter_lease.fileno()
        except AuthorityPathSecurityError as exc:
            raise FormalRuntimeError("formal runtime generation validation failed") from exc

    def require_launcher_descriptor(self) -> int:
        self.require_live()
        try:
            return self._launcher_lease.fileno()
        except AuthorityPathSecurityError as exc:
            raise FormalRuntimeError("formal runtime generation validation failed") from exc

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._launcher_lease.close()
        self._interpreter_lease.close()
        self.capability.close()

    def __enter__(self) -> FormalRuntimeSession:
        self.require_live()
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class FormalRuntimeCodeAuthority:
    """Capability-only code identity provider for formal daemon consumers."""

    def __init__(self, capability: RuntimeCodeGenerationCapability) -> None:
        if not isinstance(capability, RuntimeCodeGenerationCapability):
            raise FormalRuntimeError(
                "formal code authority requires an attested generation capability"
            )
        _require_live_generation(capability)
        self._capability = capability
        self._startup_evidence = capability.evidence

    def require_evidence(self) -> CodeTrustEvidence:
        _require_live_generation(self._capability)
        if self._capability.evidence != self._startup_evidence:
            raise FormalRuntimeError("formal runtime code evidence changed")
        return self._startup_evidence

    def require_code_sha(self) -> str:
        return self.require_evidence().provenance_commit


def bind_formal_runtime(
    capability: RuntimeCodeGenerationCapability,
    *,
    daemon_argv: tuple[str, ...],
    environment_source: Mapping[str, str],
    expected_python_abi: str,
) -> FormalRuntimeSession:
    """Create one literal, isolated launch plan from attested generation data."""

    return _bind_formal_runtime(
        capability,
        daemon_argv=daemon_argv,
        environment_source=environment_source,
        expected_python_abi=expected_python_abi,
        launcher_via_descriptor=False,
    )


def _bind_formal_runtime(
    capability: RuntimeCodeGenerationCapability,
    *,
    daemon_argv: tuple[str, ...],
    environment_source: Mapping[str, str],
    expected_python_abi: str,
    launcher_via_descriptor: bool,
) -> FormalRuntimeSession:

    if not isinstance(capability, RuntimeCodeGenerationCapability):
        raise FormalRuntimeError("formal runtime requires an attested generation capability")
    if not daemon_argv or any(not value or "\0" in value for value in daemon_argv):
        raise FormalRuntimeError("formal runtime command is missing or invalid")
    _require_live_generation(capability)
    loaded = capability.loaded
    spec = loaded.attestation.execution_spec
    release_root = loaded.release_root
    files = {file.path: file for file in loaded.attestation.files}
    interpreter = loaded.generation_root.joinpath(*PurePosixPath(spec.interpreter_path).parts)
    interpreter_file = files.get(spec.interpreter_path)
    if interpreter_file is None or interpreter_file.mode != 0o555:
        raise FormalRuntimeError("formal interpreter is not an attested executable")
    if interpreter_file.sha256 != spec.interpreter_sha256 or spec.python_abi != expected_python_abi:
        raise FormalRuntimeError("formal interpreter hash or ABI binding is invalid")
    launcher = loaded.generation_root.joinpath(*PurePosixPath(spec.launcher_path).parts)
    launcher_file = files.get(spec.launcher_path)
    if launcher_file is None or launcher_file.mode != 0o555:
        raise FormalRuntimeError("formal launcher is not an attested executable")
    working_directory = loaded.generation_root.joinpath(
        *PurePosixPath(spec.working_directory).parts
    )
    import_roots = tuple(
        loaded.generation_root.joinpath(*PurePosixPath(path).parts) for path in spec.import_roots
    )
    if (
        launcher
        != release_root.joinpath(*PurePosixPath(spec.launcher_path).relative_to("release").parts)
        or not working_directory.is_relative_to(release_root)
        or any(not path.is_relative_to(release_root) for path in import_roots)
    ):
        raise FormalRuntimeError("formal execution paths escape the release root")
    try:
        if (
            working_directory.resolve(strict=True) != working_directory
            or not working_directory.is_dir()
            or any(path.resolve(strict=True) != path or not path.is_dir() for path in import_roots)
        ):
            raise FormalRuntimeError("formal execution directories are not physical")
    except OSError as exc:
        raise FormalRuntimeError("formal execution directories are unavailable") from exc
    environment = {
        name: environment_source[name]
        for name in spec.environment_allowlist
        if name in environment_source
    }
    if any(name.startswith(_ROUTING_PREFIXES) for name in environment):
        raise FormalRuntimeError("formal environment contains a routing variable")
    interpreter_lease: SecureRegularFileLease | None = None
    launcher_lease: SecureRegularFileLease | None = None
    try:
        interpreter_lease = open_secure_regular_file_lease(
            interpreter,
            trusted_root=loaded.generation_root,
            expected_uid=loaded.material_uid,
            expected_gid=loaded.material_gid,
            allowed_modes=frozenset({0o555}),
            max_bytes=max(1, interpreter_file.size + 1),
        )
        interpreter_bytes = interpreter_lease.read_all(max_bytes=max(1, interpreter_file.size + 1))
        if (
            len(interpreter_bytes) != interpreter_file.size
            or hashlib.sha256(interpreter_bytes).hexdigest() != interpreter_file.sha256
        ):
            raise FormalRuntimeError("formal interpreter bytes are not attested")
        launcher_lease = open_secure_regular_file_lease(
            launcher,
            trusted_root=loaded.generation_root,
            expected_uid=loaded.material_uid,
            expected_gid=loaded.material_gid,
            allowed_modes=frozenset({0o555}),
            max_bytes=max(1, launcher_file.size + 1),
        )
        launcher_bytes = launcher_lease.read_all(max_bytes=max(1, launcher_file.size + 1))
        if (
            len(launcher_bytes) != launcher_file.size
            or hashlib.sha256(launcher_bytes).hexdigest() != launcher_file.sha256
        ):
            raise FormalRuntimeError("formal launcher bytes are not attested")
        if launcher_via_descriptor:
            argv = (
                str(interpreter),
                "-I",
                "-S",
                "-c",
                _FD_LAUNCH_BOOTSTRAP,
                json.dumps([str(path) for path in import_roots], separators=(",", ":")),
                str(launcher),
                _LAUNCHER_FD_PLACEHOLDER,
                *daemon_argv,
            )
        else:
            argv = (str(interpreter), "-I", "-S", str(launcher), *daemon_argv)
        plan = FormalRuntimeLaunchPlan(
            evidence=capability.evidence,
            interpreter=interpreter,
            launcher=launcher,
            launcher_sha256=launcher_file.sha256,
            launcher_via_descriptor=launcher_via_descriptor,
            bootstrap_sha256=(FORMAL_SMOKE_BOOTSTRAP_SHA256 if launcher_via_descriptor else None),
            argv=argv,
            working_directory=working_directory,
            import_roots=import_roots,
            environment=environment,
            audit=FormalRuntimeAudit(generation_id=capability.evidence.generation_id),
        )
        session = FormalRuntimeSession(
            capability=capability,
            interpreter_lease=interpreter_lease,
            launcher_lease=launcher_lease,
            plan=plan,
        )
        session.require_live()
        return session
    except (AuthorityPathSecurityError, RuntimeCodeGenerationError) as exc:
        if launcher_lease is not None:
            launcher_lease.close()
        if interpreter_lease is not None:
            interpreter_lease.close()
        raise FormalRuntimeError("formal runtime execution binding is unsafe") from exc
    except Exception:
        if launcher_lease is not None:
            launcher_lease.close()
        if interpreter_lease is not None:
            interpreter_lease.close()
        raise


def bind_formal_smoke_runtime(
    capability: RuntimeCodeGenerationCapability,
    *,
    environment_source: Mapping[str, str],
) -> FormalRuntimeSession:
    """Bind the internal smoke entry to exact interpreter and launcher descriptors."""

    return _bind_formal_runtime(
        capability,
        daemon_argv=(
            "formal-smoke-runtime-execute",
            "--request-fd",
            _REQUEST_FD_PLACEHOLDER,
            "--receipt-fd",
            _RECEIPT_FD_PLACEHOLDER,
        ),
        environment_source=environment_source,
        expected_python_abi=capability.loaded.attestation.execution_spec.python_abi,
        launcher_via_descriptor=True,
    )


def _exec_verified_descriptor(
    descriptor: int,
    argv: tuple[str, ...],
    environment: Mapping[str, str],
) -> object:
    try:
        return exec_verified_descriptor(descriptor, argv, environment)
    except DescriptorExecutionError as exc:
        raise FormalRuntimeError(str(exc)) from exc


def exec_formal_runtime(
    session: FormalRuntimeSession,
    *,
    executor: Callable[[int, tuple[str, ...], Mapping[str, str]], object] = (
        _exec_verified_descriptor
    ),
) -> object:
    """Consume the session and execute only its retained interpreter descriptor."""

    try:
        session.require_live()
        plan = session.plan
        os.chdir(plan.working_directory)
        descriptor = session.require_interpreter_descriptor()
        return executor(descriptor, plan.argv, plan.environment)
    finally:
        session.close()


def exec_formal_smoke_child(
    session: FormalRuntimeSession,
    *,
    request_descriptor: int,
    receipt_descriptor: int,
    executor: Callable[[int, tuple[str, ...], Mapping[str, str]], object] = (
        _exec_verified_descriptor
    ),
) -> object:
    """Execute the attested launcher with private request/receipt descriptors."""

    sources: tuple[int, ...] = ()
    try:
        session.require_live()
        if not session.plan.launcher_via_descriptor:
            raise FormalRuntimeError("formal smoke launcher is not descriptor-bound")
        launcher_descriptor = session.require_launcher_descriptor()
        sources = tuple(
            os.dup(descriptor)
            for descriptor in (request_descriptor, receipt_descriptor, launcher_descriptor)
        )
        for descriptor in sources:
            os.set_inheritable(descriptor, True)
        request_source, receipt_source, launcher_source = sources
        replacements = {
            _REQUEST_FD_PLACEHOLDER: str(request_source),
            _RECEIPT_FD_PLACEHOLDER: str(receipt_source),
            _LAUNCHER_FD_PLACEHOLDER: str(launcher_source),
        }
        argv = tuple(replacements.get(value, value) for value in session.plan.argv)
        os.lseek(launcher_source, 0, os.SEEK_SET)
        os.chdir(session.plan.working_directory)
        return executor(
            session.require_interpreter_descriptor(),
            argv,
            session.plan.environment,
        )
    finally:
        for descriptor in sources:
            with suppress(OSError):
                os.close(descriptor)
        session.close()


__all__ = [
    "FormalRuntimeAudit",
    "FormalRuntimeError",
    "FormalRuntimeLaunchPlan",
    "FormalRuntimeCodeAuthority",
    "FormalRuntimeSession",
    "FORMAL_SMOKE_BOOTSTRAP_SHA256",
    "_exec_verified_descriptor",
    "bind_formal_runtime",
    "bind_formal_smoke_runtime",
    "exec_formal_smoke_child",
    "exec_formal_runtime",
]
