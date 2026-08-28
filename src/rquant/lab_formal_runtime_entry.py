"""The lab claim finalizer's production entry, executed from a verified generation.

Amended per Codex round-3 verdict 2026-08-28, item RQ-WI-R2-P1-02.

`deploy/systemd/rquant-lab-claim-finalizer.service` used to start this work twice out of the
checkout: an `ExecStartPre` running `.venv/bin/rquant runtime-code dry-run`, then an
`ExecStart` running `.venv/bin/python scripts/run-lab-daemon.py formal`. Both are lighthouse
-writable trees, and `.venv` is an editable install whose `rquant.pth` points straight back
at `<checkout>/src`, so the code deciding whether the deployment was trustworthy *was* the
code the decision was about. Verification was real and unbypassed; it simply asked the
suspect.

The unit now names a role and nothing else. The root-owned wrapper
(`/usr/local/libexec/rquant-runtime-exec.pyz`) verifies one whole generation file by file
against a root-owned full manifest, then execs this module out of that generation with an
argv frozen in the root-owned profile. Two trust chains run in series and neither absorbs
the other:

1. *runtime-authority* — the wrapper proves the code identity of everything that is about to
   run, before a byte of it runs.
2. *runtime-code* — this module, now known-good, opens the root-owned bootstrap document,
   verifies the finalizer's own ed25519 attestation and promotion receipt, and execs the
   selected immutable generation. Its semantics are unchanged from `_formal_main`; only the
   provenance of the code performing them has changed.

The `ExecStartPre` migration gate did not survive as a unit line, because a checkout dry-run
proves nothing. It survives as `assert_migration_request_is_satisfiable`, run here between
the capability and the bind, so an unsatisfiable migration request still stops the daemon —
now at the first instant of the service rather than in a start-phase helper.

Nothing here is loaded dynamically. `scripts/run-lab-daemon.py` reached this logic through
three `importlib.import_module` calls that resolved through `rquant.pth` into the checkout;
here they are ordinary top-level imports, and by then `sys.path` has already been narrowed
to the verified generation by the wrapper's `child_import_paths`.
"""

from __future__ import annotations

import fcntl
import os
import stat
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from rquant.formal_runtime import (
    FormalRuntimeError,
    bind_formal_runtime,
    exec_formal_runtime,
)
from rquant.formal_runtime_command import (
    RUNTIME_CODE_MIGRATION_REQUEST_PATH,
    FormalRuntimeCommandError,
    FormalRuntimeWrapperBinding,
    compose_formal_daemon_argv,
    compose_formal_wrapper_argv,
    parse_formal_wrapper_argv,
)
from rquant.formal_runtime_composition import (
    FormalRuntimeCompositionError,
    open_formal_runtime_capability,
)
from rquant.runtime_code_operations import (
    RuntimeCodeMigrationRequest,
    RuntimeCodeOperationError,
    compose_runtime_code_generation_operator,
    load_runtime_code_bootstrap_configuration,
    load_runtime_code_operation_request,
)

#: The one daemon entry this module exists to start. The role policy names it and the
#: parser accepts four other formal commands, so the agreement is asserted rather than
#: assumed: a profile that froze `lab-worker` here must fail, not start a worker.
FINALIZER_COMMAND = "lab-claim-finalizer"

#: The same 60 second budget `_formal_main` gave itself, and the reason the unit can keep
#: `TimeoutStartSec=30s`: this deadline bounds the work, not systemd's start phase.
STARTUP_BUDGET_SECONDS = 60.0


class LabFormalRuntimeEntryError(RuntimeError):
    """The finalizer's own startup preconditions were not met."""


@dataclass(frozen=True)
class _PathIdentity:
    device: int
    inode: int
    mode: int
    owner: int
    links: int

    @classmethod
    def capture(cls, observed: os.stat_result) -> _PathIdentity:
        return cls(
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=observed.st_mode,
            owner=observed.st_uid,
            links=observed.st_nlink,
        )


def _canonical_absolute(raw: str | Path, *, label: str) -> Path:
    path = Path(raw)
    if not path.is_absolute() or path != Path(os.path.abspath(path)):
        raise LabFormalRuntimeEntryError(f"{label} must be an absolute canonical path")
    return path


def _require_owned_directory(path: Path, *, label: str) -> _PathIdentity:
    try:
        observed = path.lstat()
    except OSError as exc:
        raise LabFormalRuntimeEntryError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
        or path.resolve(strict=True) != path
    ):
        raise LabFormalRuntimeEntryError(f"{label} must be an owned physical directory")
    return _PathIdentity.capture(observed)


def acquire_formal_deployment_lock(path: Path) -> int:
    """Take the shared deployment lock, exactly as `_formal_main` did.

    A deployment holds this exclusively while it swaps generations, so a shared acquisition
    that fails means "a deployment is in flight" rather than "something is broken" — the
    unit's `SuccessExitStatus=0 75` and `Restart=on-failure` carry that distinction. The
    descriptor is deliberately inheritable: it is handed to the daemon as
    `--deployment-generation-fd`, which is how the running daemon keeps one whole generation
    pinned against a concurrent deployment.
    """

    path = _canonical_absolute(path, label="formal deployment lock")
    parent = path.parent
    try:
        parent_identity = _require_owned_directory(parent, label="formal deployment lock root")
        parent_fd = os.open(
            parent,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            descriptor = os.open(
                path.name,
                os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=parent_fd,
            )
            opened = os.fstat(descriptor)
            active = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        finally:
            os.close(parent_fd)
        if (
            _require_owned_directory(parent, label="formal deployment lock root")
            != parent_identity
            or _PathIdentity.capture(opened) != _PathIdentity.capture(active)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise LabFormalRuntimeEntryError("formal deployment lock is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        os.set_inheritable(descriptor, True)
        return descriptor
    except BlockingIOError as exc:
        raise LabFormalRuntimeEntryError("formal deployment generation is being updated") from exc
    except OSError as exc:
        raise LabFormalRuntimeEntryError("formal deployment lock is unavailable") from exc


def assert_migration_request_is_satisfiable(
    binding: FormalRuntimeWrapperBinding,
) -> tuple[str, ...]:
    """The retired `ExecStartPre` dry-run, moved inside the verified generation.

    The unit line was `rquant runtime-code dry-run --request
    /etc/rquant/runtime-code-migration.json`; this is the same call with the same root-owned
    inputs and the same fail-closed outcome, minus the checkout interpreter. The request path
    is a frozen constant rather than an argument because the unit is no longer allowed to
    name files: everything a protected unit contributes is a role literal.
    """

    configuration = load_runtime_code_bootstrap_configuration(
        binding.bootstrap.configuration_path,
        trusted_base=binding.bootstrap.trusted_base,
        expected_uid=binding.bootstrap.authority_uid,
        expected_gid=binding.bootstrap.authority_gid,
    )
    request = load_runtime_code_operation_request(
        RUNTIME_CODE_MIGRATION_REQUEST_PATH,
        RuntimeCodeMigrationRequest,
        trusted_base=binding.bootstrap.trusted_base,
        expected_uid=binding.bootstrap.authority_uid,
        expected_gid=binding.bootstrap.authority_gid,
    )
    if request.expected_configuration_path != binding.bootstrap.configuration_path:
        raise RuntimeCodeOperationError(
            "runtime code migration configuration path does not match the frozen binding"
        )
    if request.expected_trusted_base != binding.bootstrap.trusted_base:
        raise RuntimeCodeOperationError(
            "runtime code migration trusted base does not match the frozen binding"
        )
    if (
        request.expected_authority_uid != binding.bootstrap.authority_uid
        or request.expected_authority_gid != binding.bootstrap.authority_gid
    ):
        raise RuntimeCodeOperationError(
            "runtime code migration authority identity does not match the frozen binding"
        )
    return tuple(compose_runtime_code_generation_operator(configuration).dry_run(request).checks)


def run_formal_finalizer_session(binding: FormalRuntimeWrapperBinding) -> int:
    """Hold the deployment lock, verify the runtime-code chain, and exec the daemon.

    The order is the load-bearing part and it is the order `_formal_main` already used: lock,
    then capability, then the migration gate, then the bind, then the exec. Nothing reads the
    root-owned bootstrap document before the lock is held, and nothing composes a daemon argv
    before the capability has verified the attestation and the promotion receipt.
    """

    startup_deadline = time.monotonic() + STARTUP_BUDGET_SECONDS
    lock_fd = -1
    capability = None
    try:
        lock_fd = acquire_formal_deployment_lock(binding.deployment_lock_path)
        capability = open_formal_runtime_capability(
            configuration_path=binding.bootstrap.configuration_path,
            trusted_base=binding.bootstrap.trusted_base,
            expected_authority_uid=binding.bootstrap.authority_uid,
            expected_authority_gid=binding.bootstrap.authority_gid,
            startup_deadline_monotonic=startup_deadline,
        )
        assert_migration_request_is_satisfiable(binding)
        daemon_argv = compose_formal_daemon_argv(
            binding,
            deployment_generation=capability.evidence.provenance_commit,
            deployment_generation_fd=lock_fd,
            startup_deadline_monotonic=startup_deadline,
        )
        session = bind_formal_runtime(
            capability,
            daemon_argv=daemon_argv,
            environment_source=os.environ,
            expected_python_abi=capability.loaded.attestation.execution_spec.python_abi,
        )
        capability = None
        sys.stdout.flush()
        sys.stderr.flush()
        exec_formal_runtime(session)
    except (
        FormalRuntimeCommandError,
        FormalRuntimeCompositionError,
        FormalRuntimeError,
        LabFormalRuntimeEntryError,
        OSError,
        RuntimeCodeOperationError,
    ) as exc:
        print(f"Lab formal daemon wrapper failed: {exc}", file=sys.stderr)
        return 1
    finally:
        if capability is not None:
            capability.close()
        if lock_fd >= 0:
            os.close(lock_fd)
    # `exec_formal_runtime` replaces this process. Reaching here means it returned, which is
    # a failure however quiet it looked.
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    """The wrapper's entry point: the frozen `module_arguments` of the root-owned profile.

    The profile freezes the daemon entry; `compose_formal_wrapper_argv` puts the immutable
    bootstrap binding in front of it, and `parse_formal_wrapper_argv` — the same typed parser
    the unit's own tail went through before — accepts exactly one shape. A profile that froze
    the wrong literals therefore fails loudly at the first instruction instead of starting a
    differently-configured finalizer.
    """

    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        binding = parse_formal_wrapper_argv(compose_formal_wrapper_argv(arguments))
        if binding.command != FINALIZER_COMMAND:
            raise FormalRuntimeCommandError(
                "formal runtime role selected an entry this module does not serve"
            )
    except FormalRuntimeCommandError as exc:
        print(f"Lab formal daemon wrapper failed: {exc}", file=sys.stderr)
        return 1
    return run_formal_finalizer_session(binding)


__all__ = [
    "FINALIZER_COMMAND",
    "STARTUP_BUDGET_SECONDS",
    "LabFormalRuntimeEntryError",
    "acquire_formal_deployment_lock",
    "assert_migration_request_is_satisfiable",
    "main",
    "run_formal_finalizer_session",
]


if __name__ == "__main__":
    raise SystemExit(main())
