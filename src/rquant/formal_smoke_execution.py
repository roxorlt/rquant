"""Outer verifier and descriptor launcher for formal smoke execution."""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import selectors
import signal
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Protocol, cast

from pydantic import Field, ValidationError

from rquant.authority_path_security import (
    AuthorityPathSecurityError,
    SecureRegularFileLease,
    open_secure_regular_file_lease,
)
from rquant.formal_runtime import (
    FORMAL_SMOKE_BOOTSTRAP_SHA256,
    FormalRuntimeError,
    FormalRuntimeSession,
    bind_formal_smoke_runtime,
    prepare_formal_smoke_launch,
)
from rquant.formal_smoke_protocol import (
    FormalSmokeArtifactReceipt,
    FormalSmokeAttestedReplayResult,
    FormalSmokeBootstrapReference,
    FormalSmokeExecutionIdentity,
    FormalSmokeExecutionReceipt,
    FormalSmokeExecutionRequest,
    FormalSmokeStrategy,
    formal_smoke_receipt_digest,
    formal_smoke_request_digest,
)
from rquant.runtime_code_attestation import RuntimeCodeTrustError
from rquant.runtime_code_generation import (
    RuntimeCodeGenerationCapability,
    RuntimeCodeGenerationError,
)
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import (
    StrictJsonError,
    canonical_model_json_bytes,
    strict_model_validate_canonical_json,
)

_MAX_RECEIPT_BYTES = 8 * 1024 * 1024
_SUPERVISOR_CLEANUP_SECONDS = 1.25
_SUPERVISOR_OWNED_GRACE_SECONDS = 0.5
_SUPERVISOR_TEARDOWN_REQUEST = b"T"
_SUPERVISOR_TEARDOWN_READY = b"K"
_TEARDOWN_SELECTOR_FACTORY = selectors.DefaultSelector
_DESCRIPTOR_HANDOFF_BOOTSTRAP = "\n".join(
    (
        "import json, os, selectors, signal, sys, time",
        "status_descriptor = -1",
        "def teardown_group():",
        "    signal.signal(signal.SIGTERM, signal.SIG_IGN)",
        "    try:",
        "        os.killpg(0, signal.SIGTERM)",
        "    except ProcessLookupError:",
        "        pass",
        "    time.sleep(0.25)",
        "    if status_descriptor >= 0:",
        "        try:",
        "            os.write(status_descriptor, b'K')",
        "        except OSError:",
        "            pass",
        "    time.sleep(0.75)",
        "    try:",
        "        os.killpg(0, signal.SIGKILL)",
        "    except ProcessLookupError:",
        "        os._exit(126)",
        "    os._exit(126)",
        "try:",
        "    descriptor = int(sys.argv[1])",
        "    working_directory = sys.argv[2]",
        "    arguments = json.loads(sys.argv[3])",
        "    environment = json.loads(sys.argv[4])",
        "    inherited = json.loads(sys.argv[5])",
        "    lifetime_descriptor = int(sys.argv[6])",
        "    status_descriptor = int(sys.argv[7])",
        "    signal.signal(signal.SIGTERM, signal.SIG_IGN)",
        "    generation_pid = os.fork()",
        "    if generation_pid == 0:",
        "        os.close(lifetime_descriptor)",
        "        os.close(status_descriptor)",
        "        signal.signal(signal.SIGTERM, signal.SIG_DFL)",
        "        os.chdir(working_directory)",
        "        os.execve(descriptor, arguments, environment)",
        "    lifetime_selector = selectors.DefaultSelector()",
        "    lifetime_selector.register(lifetime_descriptor, selectors.EVENT_READ)",
        "    for inherited_descriptor in inherited:",
        "        os.close(inherited_descriptor)",
        "    while True:",
        "        waited, status = os.waitpid(generation_pid, os.WNOHANG)",
        "        if waited == generation_pid:",
        "            code = os.WEXITSTATUS(status) if os.WIFEXITED(status) else 255",
        "            break",
        "        if lifetime_selector.select(0.05):",
        "            os.read(lifetime_descriptor, 1)",
        "            teardown_group()",
        "    lifetime_selector.close()",
        "    os.write(status_descriptor, bytes((code,)))",
        "    os.read(lifetime_descriptor, 1)",
        "    teardown_group()",
        "except BaseException:",
        "    teardown_group()",
    )
)


class FormalSmokeExecutionError(RuntimeError):
    """The attested generation did not produce a verifiable formal smoke result."""


class FormalSmokeCleanupError(FormalSmokeExecutionError):
    """The isolated supervisor could not prove that its process group was removed."""


class FormalSmokeChildProcessResult(RuntimeContractModel):
    exit_code: int = Field(strict=True, ge=0, le=255)
    receipt_bytes: bytes = Field(max_length=_MAX_RECEIPT_BYTES)


class _FormalSmokeOuterPhase(StrEnum):
    VALIDATE_CAPABILITY = "validate_capability"
    PREPARE_OUTPUT = "prepare_output"
    PREPARE_STAGING = "prepare_staging"
    BUILD_REQUEST = "build_request"
    BIND_RUNTIME = "bind_runtime"
    EXCHANGE_CHILD = "exchange_child"
    VALIDATE_RECEIPT = "validate_receipt"
    POST_CHILD_LIVENESS = "post_child_liveness"
    VERIFY_STAGED_ARTIFACTS = "verify_staged_artifacts"
    PRE_PUBLISH_LIVENESS = "pre_publish_liveness"
    BUILD_ACCEPTED_RESULT = "build_accepted_result"
    PUBLISH_ARTIFACTS = "publish_artifacts"
    POST_PUBLISH_LIVENESS = "post_publish_liveness"
    VERIFY_PUBLISHED_ARTIFACTS = "verify_published_artifacts"
    BIND_EXECUTION_RECEIPT = "bind_execution_receipt"
    CLEANUP_STAGING = "cleanup_staging"


class _FormalSmokeExchangeSubphase(StrEnum):
    VALIDATE_DEADLINE = "validate_deadline"
    SETUP_REQUEST_PIPE = "setup_request_pipe"
    SETUP_RECEIPT_PIPE = "setup_receipt_pipe"
    SPAWN = "spawn"
    CLOSE_CHILD_ENDPOINTS = "close_child_endpoints"
    SET_NONBLOCKING_REQUEST = "set_nonblocking_request"
    SET_NONBLOCKING_RECEIPT = "set_nonblocking_receipt"
    SET_NONBLOCKING_STATUS = "set_nonblocking_status"
    CREATE_SELECTOR = "create_selector"
    REGISTER_REQUEST = "register_request"
    REGISTER_RECEIPT = "register_receipt"
    REGISTER_STATUS = "register_status"
    SELECT_EVENTS = "select_events"
    WRITE_REQUEST = "write_request"
    UNREGISTER_REQUEST = "unregister_request"
    READ_STATUS = "read_status"
    UNREGISTER_STATUS = "unregister_status"
    CLOSE_STATUS = "close_status"
    READ_RECEIPT = "read_receipt"
    UNREGISTER_RECEIPT = "unregister_receipt"
    BUILD_RESULT = "build_result"
    CLOSE_SELECTOR = "close_selector"
    CLEANUP_VALIDATE_ANCHOR = "cleanup_validate_anchor"
    CLEANUP_TEARDOWN_REQUEST = "cleanup_teardown_request"
    CLEANUP_TEARDOWN_WAIT = "cleanup_teardown_wait"
    CLEANUP_WAITID = "cleanup_waitid"
    CLEANUP_KILL = "cleanup_kill"
    CLEANUP_REAP = "cleanup_reap"
    CLEANUP_CLOSE_DESCRIPTORS = "cleanup_close_descriptors"


def _exchange_value_error_reason(subphase: _FormalSmokeExchangeSubphase) -> str:
    if subphase in {
        _FormalSmokeExchangeSubphase.SETUP_REQUEST_PIPE,
        _FormalSmokeExchangeSubphase.SETUP_RECEIPT_PIPE,
    }:
        return "pipe_contract"
    if subphase in {
        _FormalSmokeExchangeSubphase.CREATE_SELECTOR,
        _FormalSmokeExchangeSubphase.REGISTER_REQUEST,
        _FormalSmokeExchangeSubphase.REGISTER_RECEIPT,
        _FormalSmokeExchangeSubphase.REGISTER_STATUS,
        _FormalSmokeExchangeSubphase.SELECT_EVENTS,
        _FormalSmokeExchangeSubphase.UNREGISTER_REQUEST,
        _FormalSmokeExchangeSubphase.UNREGISTER_STATUS,
        _FormalSmokeExchangeSubphase.UNREGISTER_RECEIPT,
        _FormalSmokeExchangeSubphase.CLOSE_SELECTOR,
        _FormalSmokeExchangeSubphase.CLEANUP_TEARDOWN_WAIT,
    }:
        return "selector_contract"
    if subphase is _FormalSmokeExchangeSubphase.SPAWN:
        return "spawn_contract"
    if subphase is _FormalSmokeExchangeSubphase.BUILD_RESULT:
        return "result_contract"
    if subphase is _FormalSmokeExchangeSubphase.CLEANUP_WAITID:
        return "waitid_contract"
    if subphase is _FormalSmokeExchangeSubphase.CLEANUP_KILL:
        return "process_group_contract"
    if subphase is _FormalSmokeExchangeSubphase.CLEANUP_REAP:
        return "process_reap_contract"
    return "descriptor_contract"


def _formal_smoke_failure_component(exc: BaseException) -> str | None:
    if isinstance(exc, AuthorityPathSecurityError):
        return "authority_path"
    if isinstance(exc, FormalRuntimeError):
        return "formal_runtime"
    if isinstance(exc, RuntimeCodeGenerationError):
        return "runtime_generation"
    if isinstance(exc, RuntimeCodeTrustError):
        return "runtime_trust"
    if isinstance(exc, StrictJsonError):
        return "strict_json"
    if isinstance(exc, ValidationError):
        return "validation_error"
    if isinstance(exc, OSError):
        error_name = errno.errorcode.get(exc.errno)
        return "os_error" if error_name is None else f"os_error_{error_name.lower()}"
    if isinstance(exc, TypeError):
        return "type_error"
    if isinstance(exc, ValueError):
        return "value_error"
    return None


def _formal_smoke_failure_reason(exc: BaseException) -> str:
    components: list[str] = []
    observed: set[int] = set()
    current: BaseException | None = exc
    while current is not None and len(components) < 4 and id(current) not in observed:
        observed.add(id(current))
        component = _formal_smoke_failure_component(current)
        if component is None:
            break
        components.append(component)
        current = current.__cause__
    return "_".join(components)


@dataclass
class _FormalSmokeExchangeProgress:
    subphase: _FormalSmokeExchangeSubphase = _FormalSmokeExchangeSubphase.VALIDATE_DEADLINE
    request_sent: bool = False
    receipt_bytes: int = 0
    receipt_eof: bool = False
    status_reported: bool = False

    def value_error(self, exc: ValueError) -> FormalSmokeExecutionError:
        reason = (
            "validation_error"
            if isinstance(exc, ValidationError)
            else _exchange_value_error_reason(self.subphase)
        )
        return FormalSmokeExecutionError(
            f"formal smoke child exchange failed (subphase={self.subphase.value} reason={reason})"
        )

    def redacted_error(self, exc: BaseException) -> FormalSmokeExecutionError:
        if isinstance(exc, ValueError):
            return self.value_error(exc)
        reason = _formal_smoke_failure_component(exc) or "internal_error"
        return FormalSmokeExecutionError(
            f"formal smoke child exchange failed (subphase={self.subphase.value} reason={reason})"
        )

    def cleanup_value_error(self, exc: ValueError) -> FormalSmokeCleanupError:
        reason = (
            "validation_error"
            if isinstance(exc, ValidationError)
            else _exchange_value_error_reason(self.subphase)
        )
        return FormalSmokeCleanupError(
            "formal smoke supervisor cleanup failed "
            f"(subphase={self.subphase.value} reason={reason})"
        )

    def deadline_error(self) -> FormalSmokeExecutionError:
        if not self.request_sent:
            phase = "sending_request"
        elif self.status_reported and not self.receipt_eof:
            phase = "awaiting_receipt_eof"
        elif self.receipt_bytes and not self.receipt_eof:
            phase = "receiving_receipt"
        elif self.receipt_eof and not self.status_reported:
            phase = "awaiting_child_status"
        else:
            phase = "awaiting_generation_result"
        facts = (
            f"phase={phase}",
            f"request_sent={str(self.request_sent).lower()}",
            f"receipt_bytes={self.receipt_bytes}",
            f"receipt_eof={str(self.receipt_eof).lower()}",
            f"status_reported={str(self.status_reported).lower()}",
        )
        return FormalSmokeExecutionError(f"formal smoke child deadline expired ({' '.join(facts)})")


FormalSmokeExchange = Callable[
    [FormalRuntimeSession, bytes],
    FormalSmokeChildProcessResult,
]


def _exit_code(return_code: int) -> int:
    if return_code < 0 or return_code > 255:
        return 255
    return return_code


class _SupervisorLifecycleState(StrEnum):
    """The group identity remains anchored until the sole transition to REAPED."""

    RUNNING = "running"
    STATUS_REPORTED = "status_reported"
    TEARDOWN_REQUESTED = "teardown_requested"
    TEARDOWN_READY = "teardown_ready"
    FINAL_GROUP_KILL_SENT = "final_group_kill_sent"
    GROUP_IDENTITY_LOST = "group_identity_lost"
    GROUP_SIGNAL_DENIED = "group_signal_denied"
    REAPED = "reaped"


class _WaitidObservation(Protocol):
    si_pid: int
    si_code: int
    si_status: int


@dataclass
class _FormalSmokeChildProcess:
    process: subprocess.Popen[bytes]
    lifetime_descriptor: int
    status_descriptor: int
    state: _SupervisorLifecycleState = _SupervisorLifecycleState.RUNNING

    @property
    def pid(self) -> int:
        return self.process.pid

    def wait(self, *, timeout: float | None = None) -> int:
        return self.process.wait(timeout=timeout)

    def close_lifetime(self) -> None:
        if self.lifetime_descriptor == -1:
            return
        os.close(self.lifetime_descriptor)
        self.lifetime_descriptor = -1

    def close_status(self) -> None:
        if self.status_descriptor == -1:
            return
        os.close(self.status_descriptor)
        self.status_descriptor = -1


def _require_nonreaping_wait_support() -> None:
    required = (
        "P_PID",
        "WEXITED",
        "WNOHANG",
        "WNOWAIT",
        "waitid",
        "CLD_EXITED",
        "CLD_KILLED",
        "CLD_DUMPED",
    )
    if os.name != "posix" or any(not hasattr(os, name) for name in required):
        raise FormalSmokeExecutionError(
            "formal smoke supervisor requires POSIX waitid WNOWAIT support"
        )


def _waitid_return_code(observation: _WaitidObservation) -> int:
    code = int(observation.si_code)
    status = int(observation.si_status)
    if code == os.CLD_EXITED:
        return status
    if code in (os.CLD_KILLED, os.CLD_DUMPED):
        return -status
    raise FormalSmokeCleanupError(f"formal smoke supervisor returned unexpected wait state {code}")


def _observe_supervisor_without_reaping(
    process: _FormalSmokeChildProcess,
) -> int | None:
    if process.process.returncode is not None:
        process.state = _SupervisorLifecycleState.REAPED
        raise FormalSmokeCleanupError("formal smoke supervisor was already reaped before cleanup")
    try:
        observation = os.waitid(
            os.P_PID,
            process.pid,
            os.WEXITED | os.WNOHANG | os.WNOWAIT,
        )
    except ChildProcessError as exc:
        process.state = _SupervisorLifecycleState.REAPED
        raise FormalSmokeCleanupError(
            "formal smoke supervisor identity was already reaped"
        ) from exc
    if observation is None:
        return None
    observed = cast(_WaitidObservation, observation)
    if observed.si_pid == 0:
        return None
    if observed.si_pid != process.pid:
        raise FormalSmokeCleanupError("formal smoke supervisor wait identity mismatch")
    return _waitid_return_code(observed)


def _reap_known_supervisor(
    process: _FormalSmokeChildProcess,
    *,
    deadline_monotonic: float,
) -> int:
    remaining = max(0.0, deadline_monotonic - time.monotonic())
    try:
        return_code = process.wait(timeout=remaining)
    except subprocess.TimeoutExpired as exc:
        raise FormalSmokeCleanupError(
            "formal smoke supervisor did not exit before cleanup deadline"
        ) from exc
    process.state = _SupervisorLifecycleState.REAPED
    return return_code


def _final_kill_supervisor_group(
    process: _FormalSmokeChildProcess,
) -> FormalSmokeCleanupError | None:
    if process.state is _SupervisorLifecycleState.REAPED or process.process.returncode is not None:
        process.state = _SupervisorLifecycleState.REAPED
        return FormalSmokeCleanupError(
            "formal smoke supervisor was reaped before final group cleanup"
        )
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        process.state = _SupervisorLifecycleState.GROUP_IDENTITY_LOST
        return None
    except PermissionError:
        process.state = _SupervisorLifecycleState.GROUP_SIGNAL_DENIED
        return FormalSmokeCleanupError("formal smoke supervisor group cleanup permission denied")
    process.state = _SupervisorLifecycleState.FINAL_GROUP_KILL_SENT
    return None


def _await_supervisor_teardown_ready(
    process: _FormalSmokeChildProcess,
    *,
    deadline_monotonic: float,
) -> bool:
    if process.status_descriptor == -1:
        return False
    remaining = max(0.0, deadline_monotonic - time.monotonic())
    if remaining == 0:
        return False
    selector: selectors.BaseSelector | None = None
    try:
        selector = _TEARDOWN_SELECTOR_FACTORY()
        selector.register(process.status_descriptor, selectors.EVENT_READ)
        if not selector.select(remaining):
            return False
        ready = os.read(process.status_descriptor, 1) == _SUPERVISOR_TEARDOWN_READY
        if ready:
            process.state = _SupervisorLifecycleState.TEARDOWN_READY
        return ready
    except OSError:
        return False
    finally:
        if selector is not None:
            selector.close()


def _cleanup_formal_smoke_supervisor(
    process: _FormalSmokeChildProcess,
    *,
    progress: _FormalSmokeExchangeProgress | None = None,
) -> None:
    cleanup_deadline = time.monotonic() + _SUPERVISOR_CLEANUP_SECONDS
    cleanup_error: FormalSmokeCleanupError | None = None
    cleanup_diagnostic: FormalSmokeExecutionError | None = None
    cleanup_diagnostic_error: FormalSmokeCleanupError | None = None
    request_delivered = False
    observed_return_code: int | None = None
    reaped_return_code: int | None = None
    cleanup_progress = progress or _FormalSmokeExchangeProgress()

    def set_subphase(subphase: _FormalSmokeExchangeSubphase) -> None:
        cleanup_progress.subphase = subphase

    def record_value_error(exc: ValueError) -> None:
        nonlocal cleanup_diagnostic, cleanup_diagnostic_error
        if cleanup_diagnostic is None:
            cleanup_diagnostic = cleanup_progress.value_error(exc)
            cleanup_diagnostic_error = cleanup_progress.cleanup_value_error(exc)

    try:
        set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_VALIDATE_ANCHOR)
        if process.process.returncode is not None:
            process.state = _SupervisorLifecycleState.REAPED
            cleanup_error = FormalSmokeCleanupError(
                "formal smoke supervisor was already reaped before cleanup"
            )
        else:
            try:
                set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_TEARDOWN_REQUEST)
                request_delivered = (
                    os.write(process.lifetime_descriptor, _SUPERVISOR_TEARDOWN_REQUEST) == 1
                )
            except OSError:
                request_delivered = False
            except ValueError as exc:
                record_value_error(exc)
            finally:
                try:
                    process.close_lifetime()
                except OSError:
                    pass
                except ValueError as exc:
                    set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_CLOSE_DESCRIPTORS)
                    record_value_error(exc)
            if request_delivered:
                process.state = _SupervisorLifecycleState.TEARDOWN_REQUESTED

            owned_deadline = time.monotonic()
            if request_delivered:
                owned_deadline = min(
                    cleanup_deadline,
                    owned_deadline + _SUPERVISOR_OWNED_GRACE_SECONDS,
                )
                set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_TEARDOWN_WAIT)
                try:
                    _await_supervisor_teardown_ready(
                        process,
                        deadline_monotonic=owned_deadline,
                    )
                except ValueError as exc:
                    record_value_error(exc)

            set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_WAITID)
            try:
                observed_return_code = _observe_supervisor_without_reaping(process)
            except ValueError as exc:
                record_value_error(exc)
            except FormalSmokeCleanupError as exc:
                cleanup_error = exc

            if process.state is not _SupervisorLifecycleState.REAPED:
                set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_KILL)
                # The non-reaped leader remains the process-group identity anchor.
                for _attempt in range(2):
                    try:
                        group_error = _final_kill_supervisor_group(process)
                    except ValueError as exc:
                        record_value_error(exc)
                        continue
                    if group_error is not None and cleanup_error is None:
                        cleanup_error = group_error
                    break
                else:
                    if cleanup_error is None:
                        cleanup_error = FormalSmokeCleanupError(
                            "formal smoke supervisor final group cleanup contract failed"
                        )

                set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_REAP)
                for _attempt in range(2):
                    try:
                        reaped_return_code = _reap_known_supervisor(
                            process,
                            deadline_monotonic=cleanup_deadline,
                        )
                    except ValueError as exc:
                        record_value_error(exc)
                        continue
                    except FormalSmokeCleanupError as exc:
                        if cleanup_error is None:
                            cleanup_error = exc
                    break
                else:
                    if cleanup_error is None:
                        cleanup_error = FormalSmokeCleanupError(
                            "formal smoke supervisor reap contract failed"
                        )

            if (
                cleanup_error is None
                and observed_return_code is not None
                and reaped_return_code != observed_return_code
            ):
                cleanup_error = FormalSmokeCleanupError(
                    "formal smoke supervisor changed status while being reaped"
                )
    finally:
        for close_descriptor in (process.close_lifetime, process.close_status):
            try:
                close_descriptor()
            except OSError:
                pass
            except ValueError as exc:
                set_subphase(_FormalSmokeExchangeSubphase.CLEANUP_CLOSE_DESCRIPTORS)
                record_value_error(exc)

    final_error = cleanup_error or cleanup_diagnostic_error
    if final_error is not None:
        if cleanup_diagnostic is not None:
            raise final_error from cleanup_diagnostic
        raise final_error


def _spawn_formal_smoke_child(
    session: FormalRuntimeSession,
    *,
    request_descriptor: int,
    receipt_descriptor: int,
) -> _FormalSmokeChildProcess:
    _require_nonreaping_wait_support()
    with prepare_formal_smoke_launch(
        session,
        request_descriptor=request_descriptor,
        receipt_descriptor=receipt_descriptor,
    ) as launch:
        lifetime_read, lifetime_write = os.pipe()
        try:
            status_read, status_write = os.pipe()
        except BaseException:
            os.close(lifetime_read)
            os.close(lifetime_write)
            raise
        try:
            command = (
                sys.executable,
                "-I",
                "-S",
                "-c",
                _DESCRIPTOR_HANDOFF_BOOTSTRAP,
                str(launch.interpreter_descriptor),
                os.fspath(launch.working_directory),
                json.dumps(launch.argv, ensure_ascii=True, separators=(",", ":")),
                json.dumps(launch.environment, ensure_ascii=True, separators=(",", ":")),
                json.dumps(launch.inherited_descriptors, separators=(",", ":")),
                str(lifetime_read),
                str(status_write),
            )
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                pass_fds=(
                    *launch.inherited_descriptors,
                    lifetime_read,
                    status_write,
                ),
                start_new_session=True,
                env={},
            )
        except BaseException:
            os.close(lifetime_write)
            os.close(status_read)
            raise
        finally:
            os.close(lifetime_read)
            os.close(status_write)
        return _FormalSmokeChildProcess(
            process=process,
            lifetime_descriptor=lifetime_write,
            status_descriptor=status_read,
        )


def _exchange_formal_smoke_child_impl(
    session: FormalRuntimeSession,
    request_bytes: bytes,
    *,
    deadline_monotonic: float,
    progress: _FormalSmokeExchangeProgress,
) -> FormalSmokeChildProcessResult:
    if not math.isfinite(deadline_monotonic) or time.monotonic() >= deadline_monotonic:
        raise progress.deadline_error()
    progress.subphase = _FormalSmokeExchangeSubphase.SETUP_REQUEST_PIPE
    request_read, request_write = os.pipe()
    try:
        progress.subphase = _FormalSmokeExchangeSubphase.SETUP_RECEIPT_PIPE
        receipt_read, receipt_write = os.pipe()
    except BaseException:
        os.close(request_read)
        os.close(request_write)
        raise
    try:
        progress.subphase = _FormalSmokeExchangeSubphase.SPAWN
        process = _spawn_formal_smoke_child(
            session,
            request_descriptor=request_read,
            receipt_descriptor=receipt_write,
        )
    except BaseException:
        for descriptor in (request_read, request_write, receipt_read, receipt_write):
            os.close(descriptor)
        raise

    selector: selectors.BaseSelector | None = None
    result: FormalSmokeChildProcessResult | None = None
    try:
        try:
            progress.subphase = _FormalSmokeExchangeSubphase.CLOSE_CHILD_ENDPOINTS
            os.close(request_read)
            request_read = -1
            os.close(receipt_write)
            receipt_write = -1
            progress.subphase = _FormalSmokeExchangeSubphase.SET_NONBLOCKING_REQUEST
            os.set_blocking(request_write, False)
            progress.subphase = _FormalSmokeExchangeSubphase.SET_NONBLOCKING_RECEIPT
            os.set_blocking(receipt_read, False)
            progress.subphase = _FormalSmokeExchangeSubphase.SET_NONBLOCKING_STATUS
            os.set_blocking(process.status_descriptor, False)
            progress.subphase = _FormalSmokeExchangeSubphase.CREATE_SELECTOR
            selector = selectors.DefaultSelector()
            progress.subphase = _FormalSmokeExchangeSubphase.REGISTER_REQUEST
            selector.register(request_write, selectors.EVENT_WRITE, "request")
            progress.subphase = _FormalSmokeExchangeSubphase.REGISTER_RECEIPT
            selector.register(receipt_read, selectors.EVENT_READ, "receipt")
            progress.subphase = _FormalSmokeExchangeSubphase.REGISTER_STATUS
            selector.register(process.status_descriptor, selectors.EVENT_READ, "status")
            request_view = memoryview(request_bytes)
            receipt = bytearray()
            receipt_eof = False
            status_bytes = bytearray()
            child_exit_code: int | None = None
            while child_exit_code is None or not receipt_eof or request_write != -1:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0:
                    raise progress.deadline_error()
                progress.subphase = _FormalSmokeExchangeSubphase.SELECT_EVENTS
                for key, _events in selector.select(min(remaining, 0.05)):
                    if key.data == "request":
                        try:
                            progress.subphase = _FormalSmokeExchangeSubphase.WRITE_REQUEST
                            written = os.write(request_write, request_view)
                        except BlockingIOError:
                            continue
                        except BrokenPipeError as exc:
                            raise FormalSmokeExecutionError(
                                "formal smoke request pipe closed"
                            ) from exc
                        if written <= 0:
                            raise FormalSmokeExecutionError("formal smoke request write failed")
                        request_view = request_view[written:]
                        if not request_view:
                            progress.subphase = _FormalSmokeExchangeSubphase.UNREGISTER_REQUEST
                            selector.unregister(request_write)
                            os.close(request_write)
                            request_write = -1
                            progress.request_sent = True
                        continue
                    if key.data == "status":
                        try:
                            progress.subphase = _FormalSmokeExchangeSubphase.READ_STATUS
                            chunk = os.read(process.status_descriptor, 2)
                        except BlockingIOError:
                            continue
                        if chunk:
                            status_bytes.extend(chunk)
                            if len(status_bytes) != 1:
                                raise FormalSmokeExecutionError(
                                    "formal smoke supervisor status is invalid"
                                )
                            progress.subphase = _FormalSmokeExchangeSubphase.UNREGISTER_STATUS
                            selector.unregister(process.status_descriptor)
                            child_exit_code = status_bytes[0]
                            progress.status_reported = True
                            process.state = _SupervisorLifecycleState.STATUS_REPORTED
                            continue
                        progress.subphase = _FormalSmokeExchangeSubphase.UNREGISTER_STATUS
                        selector.unregister(process.status_descriptor)
                        progress.subphase = _FormalSmokeExchangeSubphase.CLOSE_STATUS
                        process.close_status()
                        raise FormalSmokeExecutionError(
                            "formal smoke supervisor returned no status"
                        )
                    while True:
                        try:
                            progress.subphase = _FormalSmokeExchangeSubphase.READ_RECEIPT
                            chunk = os.read(
                                receipt_read,
                                min(64 * 1024, _MAX_RECEIPT_BYTES + 1 - len(receipt)),
                            )
                        except BlockingIOError:
                            break
                        if not chunk:
                            progress.subphase = _FormalSmokeExchangeSubphase.UNREGISTER_RECEIPT
                            selector.unregister(receipt_read)
                            os.close(receipt_read)
                            receipt_read = -1
                            receipt_eof = True
                            progress.receipt_eof = True
                            break
                        receipt.extend(chunk)
                        progress.receipt_bytes = len(receipt)
                        if len(receipt) > _MAX_RECEIPT_BYTES:
                            raise FormalSmokeExecutionError(
                                "formal smoke receipt exceeds the limit"
                            )
            assert child_exit_code is not None
            progress.subphase = _FormalSmokeExchangeSubphase.BUILD_RESULT
            result = FormalSmokeChildProcessResult(
                exit_code=_exit_code(child_exit_code),
                receipt_bytes=bytes(receipt),
            )
        finally:
            try:
                if selector is not None:
                    previous_subphase = progress.subphase
                    try:
                        progress.subphase = _FormalSmokeExchangeSubphase.CLOSE_SELECTOR
                        selector.close()
                    except BaseException:
                        raise
                    else:
                        progress.subphase = previous_subphase
            finally:
                for descriptor in (
                    request_read,
                    request_write,
                    receipt_read,
                    receipt_write,
                ):
                    if descriptor != -1:
                        with suppress(OSError):
                            os.close(descriptor)
    except BaseException as exc:
        failure_subphase = progress.subphase
        redacted_failure = progress.redacted_error(exc)
        try:
            _cleanup_formal_smoke_supervisor(process, progress=progress)
        except FormalSmokeCleanupError as cleanup_exc:
            raise cleanup_exc from redacted_failure
        progress.subphase = failure_subphase
        raise
    _cleanup_formal_smoke_supervisor(process, progress=progress)
    assert result is not None
    return result


def _exchange_formal_smoke_child(
    session: FormalRuntimeSession,
    request_bytes: bytes,
    *,
    deadline_monotonic: float,
) -> FormalSmokeChildProcessResult:
    progress = _FormalSmokeExchangeProgress()
    try:
        return _exchange_formal_smoke_child_impl(
            session,
            request_bytes,
            deadline_monotonic=deadline_monotonic,
            progress=progress,
        )
    except FormalSmokeExecutionError:
        raise
    except ValueError as exc:
        raise progress.value_error(exc) from None


def _require_output_root(output_dir: Path) -> Path:
    output = Path(output_dir)
    if not output.is_absolute() or output != Path(os.path.abspath(output)):
        raise FormalSmokeExecutionError("formal smoke output directory must be canonical absolute")
    if not output.exists():
        output.mkdir(mode=0o700, parents=True)
    try:
        observed = output.lstat()
        physical = output.resolve(strict=True)
    except OSError as exc:
        raise FormalSmokeExecutionError("formal smoke output directory is unavailable") from exc
    if (
        physical != output
        or not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or observed.st_mode & 0o022
    ):
        raise FormalSmokeExecutionError("formal smoke output directory is unsafe")
    return output


def _execution_identity(
    capability: RuntimeCodeGenerationCapability,
) -> FormalSmokeExecutionIdentity:
    capability.require_live()
    loaded = capability.loaded
    spec = loaded.attestation.execution_spec
    files = tuple(sorted(loaded.attestation.files, key=lambda item: item.path))
    by_path = {file.path: file for file in files}
    try:
        launcher = by_path[spec.launcher_path]
        interpreter = by_path[spec.interpreter_path]
    except KeyError as exc:  # signed attestation validation should already reject this
        raise FormalSmokeExecutionError("formal smoke execution file is missing") from exc
    return FormalSmokeExecutionIdentity(
        generation_id=capability.evidence.generation_id,
        generation_root=loaded.generation_root,
        material_uid=loaded.material_uid,
        material_gid=loaded.material_gid,
        launcher=launcher,
        interpreter=interpreter,
        working_directory=spec.working_directory,
        import_roots=spec.import_roots,
        python_abi=spec.python_abi,
        bootstrap_sha256=FORMAL_SMOKE_BOOTSTRAP_SHA256,
        code_files=files,
    )


def _validate_receipt(
    request: FormalSmokeExecutionRequest,
    process_result: FormalSmokeChildProcessResult,
) -> FormalSmokeExecutionReceipt:
    if process_result.exit_code != 0:
        raise FormalSmokeExecutionError(
            f"formal smoke child failed with exit code {process_result.exit_code}"
        )
    if not process_result.receipt_bytes:
        raise FormalSmokeExecutionError("formal smoke child returned no receipt")
    try:
        receipt = strict_model_validate_canonical_json(
            FormalSmokeExecutionReceipt,
            process_result.receipt_bytes,
        )
    except (StrictJsonError, ValidationError, ValueError, TypeError) as exc:
        raise FormalSmokeExecutionError("formal smoke receipt is invalid") from exc
    if receipt.request_digest != formal_smoke_request_digest(request):
        raise FormalSmokeExecutionError("formal smoke receipt request digest mismatch")
    if receipt.code_trust_evidence != request.code_trust_evidence:
        raise FormalSmokeExecutionError("formal smoke receipt evidence mismatch")
    if receipt.execution_identity != request.execution_identity:
        raise FormalSmokeExecutionError("formal smoke receipt execution identity mismatch")
    expected_result = {
        "strategy": request.strategy,
        "audit_run_id": request.audit_run_id,
        "dataset_snapshot_id": request.dataset_snapshot_id,
        "dataset_binding_hash": request.dataset_binding_hash,
        "code_commit": request.code_commit,
        "missing_evidence": (),
    }
    for field_name, expected in expected_result.items():
        if getattr(receipt.result, field_name) != expected:
            raise FormalSmokeExecutionError(f"formal smoke receipt {field_name} mismatch")
    return receipt


def _mark_verified_execution(
    capability: RuntimeCodeGenerationCapability,
    binding_digest: str,
) -> None:
    capability._mark_verified_execution(binding_digest)


def _require_live_session(session: FormalRuntimeSession) -> None:
    session.require_live()


def _verify_staged_artifacts(
    request: FormalSmokeExecutionRequest,
    receipt: FormalSmokeExecutionReceipt,
) -> tuple[tuple[Path, SecureRegularFileLease], ...]:
    verified: list[tuple[Path, SecureRegularFileLease]] = []
    try:
        for artifact in receipt.artifacts:
            relative = PurePosixPath(artifact.relative_path)
            staged = request.staging_root.joinpath(*relative.parts)
            lease: SecureRegularFileLease | None = None
            try:
                lease = open_secure_regular_file_lease(
                    staged,
                    trusted_root=request.staging_root,
                    expected_uid=os.getuid(),
                    expected_gid=os.getgid(),
                    allowed_modes=frozenset({0o600, 0o640, 0o644}),
                    max_bytes=max(1, artifact.size + 1),
                )
                payload = lease.read_all(max_bytes=max(1, artifact.size + 1))
                if (
                    len(payload) != artifact.size
                    or hashlib.sha256(payload).hexdigest() != artifact.sha256
                ):
                    raise FormalSmokeExecutionError("formal smoke artifact digest mismatch")
                verified.append((staged, lease))
                lease = None
            finally:
                if lease is not None:
                    lease.close()
        return tuple(verified)
    except BaseException:
        for _path, opened in reversed(verified):
            opened.close()
        raise


@dataclass(frozen=True)
class _DirectoryIdentity:
    device: int
    inode: int
    mode: int
    uid: int
    gid: int

    @classmethod
    def from_stat(cls, observed: os.stat_result) -> _DirectoryIdentity:
        return cls(
            device=observed.st_dev,
            inode=observed.st_ino,
            mode=observed.st_mode,
            uid=observed.st_uid,
            gid=observed.st_gid,
        )


class _PublicationDirectories:
    def __init__(
        self,
        *,
        artifact_root: Path,
        staging_root: Path,
        artifact_root_fd: int,
        staging_root_fd: int,
        destination_fd: int,
        source_fd: int,
    ) -> None:
        self.artifact_root = artifact_root
        self.staging_root = staging_root
        self.artifact_root_fd = artifact_root_fd
        self.staging_root_fd = staging_root_fd
        self.destination_fd = destination_fd
        self.source_fd = source_fd
        self._identities = tuple(
            _DirectoryIdentity.from_stat(os.fstat(descriptor))
            for descriptor in (
                artifact_root_fd,
                staging_root_fd,
                destination_fd,
                source_fd,
            )
        )

    def require_unchanged(self) -> None:
        named = (
            os.stat(self.artifact_root, follow_symlinks=False),
            os.stat(
                self.staging_root.name,
                dir_fd=self.artifact_root_fd,
                follow_symlinks=False,
            ),
            os.stat(
                "strategy_lab_runs",
                dir_fd=self.artifact_root_fd,
                follow_symlinks=False,
            ),
            os.stat(
                "strategy_lab_runs",
                dir_fd=self.staging_root_fd,
                follow_symlinks=False,
            ),
        )
        descriptors = (
            self.artifact_root_fd,
            self.staging_root_fd,
            self.destination_fd,
            self.source_fd,
        )
        for expected, path_observed, descriptor in zip(
            self._identities,
            named,
            descriptors,
            strict=True,
        ):
            opened = os.fstat(descriptor)
            if (
                _DirectoryIdentity.from_stat(path_observed) != expected
                or _DirectoryIdentity.from_stat(opened) != expected
                or not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != os.getuid()
                or opened.st_gid != os.getgid()
                or opened.st_mode & 0o022
            ):
                raise FormalSmokeExecutionError(
                    "formal smoke publication directory identity changed"
                )

    def close(self) -> None:
        for descriptor in (
            self.source_fd,
            self.destination_fd,
            self.staging_root_fd,
            self.artifact_root_fd,
        ):
            with suppress(OSError):
                os.close(descriptor)


def _open_publication_directories(request: FormalSmokeExecutionRequest) -> _PublicationDirectories:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    opened: list[int] = []
    try:
        artifact_root_fd = os.open(request.artifact_root, flags)
        opened.append(artifact_root_fd)
        with suppress(FileExistsError):
            os.mkdir("strategy_lab_runs", mode=0o755, dir_fd=artifact_root_fd)
        staging_root_fd = os.open(request.staging_root.name, flags, dir_fd=artifact_root_fd)
        opened.append(staging_root_fd)
        destination_fd = os.open("strategy_lab_runs", flags, dir_fd=artifact_root_fd)
        opened.append(destination_fd)
        source_fd = os.open("strategy_lab_runs", flags, dir_fd=staging_root_fd)
        opened.append(source_fd)
        directories = _PublicationDirectories(
            artifact_root=request.artifact_root,
            staging_root=request.staging_root,
            artifact_root_fd=artifact_root_fd,
            staging_root_fd=staging_root_fd,
            destination_fd=destination_fd,
            source_fd=source_fd,
        )
        directories.require_unchanged()
        return directories
    except BaseException:
        for descriptor in reversed(opened):
            with suppress(OSError):
                os.close(descriptor)
        raise


class _PublishedArtifacts:
    def __init__(
        self,
        *,
        directories: _PublicationDirectories,
        names: tuple[str, str],
    ) -> None:
        self.directories = directories
        self.names = names

    def require_unchanged(self) -> None:
        self.directories.require_unchanged()

    def cleanup(self) -> None:
        for name in reversed(self.names):
            with suppress(FileNotFoundError):
                os.unlink(name, dir_fd=self.directories.destination_fd)

    def close(self) -> None:
        self.directories.close()


def _publish_artifacts(
    request: FormalSmokeExecutionRequest,
    receipt: FormalSmokeExecutionReceipt,
    staged: tuple[tuple[Path, SecureRegularFileLease], ...],
) -> _PublishedArtifacts:
    directories: _PublicationDirectories | None = None
    names = tuple(PurePosixPath(artifact.relative_path).name for artifact in receipt.artifacts)
    published: list[str] = []
    try:
        directories = _open_publication_directories(request)
        for (source, source_lease), name, artifact in zip(
            staged,
            names,
            receipt.artifacts,
            strict=True,
        ):
            directories.require_unchanged()
            source_lease.require_unchanged()
            source_descriptor = source_lease.fileno()
            source_identity = os.fstat(source_descriptor)
            os.link(
                source.name,
                name,
                src_dir_fd=directories.source_fd,
                dst_dir_fd=directories.destination_fd,
                follow_symlinks=False,
            )
            published.append(name)
            directories.require_unchanged()
            _verify_linked_artifact(
                name,
                directories.destination_fd,
                source_descriptor,
                source_identity,
                artifact,
            )
            _require_linked_source(
                source.name,
                directories.source_fd,
                source_descriptor,
                source_identity,
            )
        directories.require_unchanged()
        for source, _lease in staged:
            os.unlink(source.name, dir_fd=directories.source_fd)
        directories.require_unchanged()
    except BaseException:
        if directories is not None:
            for name in reversed(published):
                with suppress(FileNotFoundError):
                    os.unlink(name, dir_fd=directories.destination_fd)
            directories.close()
        raise
    finally:
        for _source, lease in reversed(staged):
            lease.close()
    assert directories is not None
    return _PublishedArtifacts(
        directories=directories,
        names=(names[0], names[1]),
    )


def _verify_linked_artifact(
    destination: str,
    destination_dir_fd: int,
    source_descriptor: int,
    source_identity: os.stat_result,
    artifact: FormalSmokeArtifactReceipt,
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(destination, flags, dir_fd=destination_dir_fd)
    try:
        source = os.fstat(source_descriptor)
        linked = os.fstat(descriptor)
        if (
            not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(linked.st_mode) not in {0o600, 0o640, 0o644}
            or linked.st_uid != os.getuid()
            or linked.st_gid != os.getgid()
            or linked.st_nlink != 2
            or (linked.st_dev, linked.st_ino) != (source_identity.st_dev, source_identity.st_ino)
            or (source.st_dev, source.st_ino) != (source_identity.st_dev, source_identity.st_ino)
            or linked.st_size != artifact.size
        ):
            raise FormalSmokeExecutionError("formal smoke published artifact identity mismatch")
        payload = bytearray()
        while len(payload) <= artifact.size:
            chunk = os.read(descriptor, min(64 * 1024, artifact.size + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) != artifact.size or hashlib.sha256(payload).hexdigest() != artifact.sha256:
            raise FormalSmokeExecutionError("formal smoke published artifact digest mismatch")
    finally:
        os.close(descriptor)


def _require_linked_source(
    source: str,
    source_dir_fd: int,
    descriptor: int,
    expected: os.stat_result,
) -> None:
    named = os.stat(source, dir_fd=source_dir_fd, follow_symlinks=False)
    opened = os.fstat(descriptor)
    expected_stable = (
        expected.st_dev,
        expected.st_ino,
        expected.st_mode,
        expected.st_uid,
        expected.st_gid,
        expected.st_size,
        expected.st_mtime_ns,
    )
    for observed in (named, opened):
        if (
            observed.st_dev,
            observed.st_ino,
            observed.st_mode,
            observed.st_uid,
            observed.st_gid,
            observed.st_size,
            observed.st_mtime_ns,
        ) != expected_stable or observed.st_nlink != 2:
            raise FormalSmokeExecutionError("formal smoke staged artifact changed during publish")


def _run_attested_formal_smoke(
    capability: RuntimeCodeGenerationCapability,
    *,
    strategy: FormalSmokeStrategy,
    start_date: date,
    end_date: date,
    audit_run_id: str,
    dataset_snapshot_id: str,
    dataset_binding_hash: str,
    output_dir: Path,
    bootstrap_reference: FormalSmokeBootstrapReference,
    environment_source: Mapping[str, str],
    exchange: FormalSmokeExchange = _exchange_formal_smoke_child,
) -> FormalSmokeAttestedReplayResult:
    """Launch generation A and accept only its fully bound canonical receipt."""

    session: FormalRuntimeSession | None = None
    phase = _FormalSmokeOuterPhase.VALIDATE_CAPABILITY
    try:
        if not isinstance(capability, RuntimeCodeGenerationCapability):
            raise FormalSmokeExecutionError(
                "formal smoke requires an attested generation capability"
            )
        capability.require_live()
        phase = _FormalSmokeOuterPhase.PREPARE_OUTPUT
        output = _require_output_root(output_dir)
        phase = _FormalSmokeOuterPhase.PREPARE_STAGING
        with tempfile.TemporaryDirectory(prefix=".formal-smoke-", dir=output) as raw_staging:
            staging = Path(raw_staging)
            staging.chmod(0o700)
            phase = _FormalSmokeOuterPhase.BUILD_REQUEST
            request = FormalSmokeExecutionRequest(
                strategy=strategy,
                start_date=start_date,
                end_date=end_date,
                audit_run_id=audit_run_id,
                dataset_snapshot_id=dataset_snapshot_id,
                dataset_binding_hash=dataset_binding_hash,
                code_commit=capability.evidence.provenance_commit,
                code_trust_evidence=capability.evidence,
                execution_identity=_execution_identity(capability),
                bootstrap_reference=bootstrap_reference,
                artifact_root=output,
                staging_root=staging,
            )
            phase = _FormalSmokeOuterPhase.BIND_RUNTIME
            session = bind_formal_smoke_runtime(
                capability,
                environment_source=environment_source,
            )
            phase = _FormalSmokeOuterPhase.EXCHANGE_CHILD
            process_result = exchange(session, canonical_model_json_bytes(request))
            phase = _FormalSmokeOuterPhase.VALIDATE_RECEIPT
            receipt = _validate_receipt(request, process_result)
            phase = _FormalSmokeOuterPhase.POST_CHILD_LIVENESS
            _require_live_session(session)
            phase = _FormalSmokeOuterPhase.VERIFY_STAGED_ARTIFACTS
            staged = _verify_staged_artifacts(request, receipt)
            phase = _FormalSmokeOuterPhase.PRE_PUBLISH_LIVENESS
            _require_live_session(session)
            phase = _FormalSmokeOuterPhase.BUILD_ACCEPTED_RESULT
            binding_digest = formal_smoke_receipt_digest(receipt)
            result_values = receipt.result.model_dump(mode="python")
            expected_paths = tuple(
                output.joinpath(*PurePosixPath(artifact.relative_path).parts)
                for artifact in receipt.artifacts
            )
            accepted = FormalSmokeAttestedReplayResult(
                **result_values,
                json_path=expected_paths[0],
                markdown_path=expected_paths[1],
                execution_receipt=receipt,
                execution_receipt_digest=binding_digest,
            )
            phase = _FormalSmokeOuterPhase.PUBLISH_ARTIFACTS
            published = _publish_artifacts(request, receipt, staged)
            try:
                phase = _FormalSmokeOuterPhase.POST_PUBLISH_LIVENESS
                _require_live_session(session)
                phase = _FormalSmokeOuterPhase.VERIFY_PUBLISHED_ARTIFACTS
                published.require_unchanged()
                phase = _FormalSmokeOuterPhase.BIND_EXECUTION_RECEIPT
                _mark_verified_execution(capability, binding_digest)
            except BaseException:
                published.cleanup()
                raise
            finally:
                published.close()
            phase = _FormalSmokeOuterPhase.CLEANUP_STAGING
            return accepted
    except FormalSmokeExecutionError:
        raise
    except (
        AuthorityPathSecurityError,
        FormalRuntimeError,
        OSError,
        RuntimeCodeGenerationError,
        StrictJsonError,
        TypeError,
        ValidationError,
        ValueError,
    ) as exc:
        reason = _formal_smoke_failure_reason(exc)
        raise FormalSmokeExecutionError(
            f"formal smoke attested execution failed (phase={phase.value} reason={reason})"
        ) from None
    finally:
        if session is not None:
            session.close()
        else:
            capability.close()


def run_attested_formal_smoke(
    capability: RuntimeCodeGenerationCapability,
    *,
    strategy: FormalSmokeStrategy,
    start_date: date,
    end_date: date,
    audit_run_id: str,
    dataset_snapshot_id: str,
    dataset_binding_hash: str,
    output_dir: Path,
    bootstrap_reference: FormalSmokeBootstrapReference,
    environment_source: Mapping[str, str],
    execution_deadline_monotonic: float | None = None,
) -> FormalSmokeAttestedReplayResult:
    """Launch only through the built-in verified descriptor transport."""

    deadline = (
        time.monotonic() + 60 * 60
        if execution_deadline_monotonic is None
        else execution_deadline_monotonic
    )

    def exchange(
        session: FormalRuntimeSession,
        request_bytes: bytes,
    ) -> FormalSmokeChildProcessResult:
        return _exchange_formal_smoke_child(
            session,
            request_bytes,
            deadline_monotonic=deadline,
        )

    return _run_attested_formal_smoke(
        capability,
        strategy=strategy,
        start_date=start_date,
        end_date=end_date,
        audit_run_id=audit_run_id,
        dataset_snapshot_id=dataset_snapshot_id,
        dataset_binding_hash=dataset_binding_hash,
        output_dir=output_dir,
        bootstrap_reference=bootstrap_reference,
        environment_source=environment_source,
        exchange=exchange,
    )


__all__ = [
    "FormalSmokeChildProcessResult",
    "FormalSmokeExecutionError",
    "run_attested_formal_smoke",
]
