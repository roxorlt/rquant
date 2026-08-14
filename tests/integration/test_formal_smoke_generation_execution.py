from __future__ import annotations

import errno
import fcntl
import hashlib
import inspect
import json
import os
import selectors
import signal
import subprocess
import sys
import threading
import time
import traceback
from collections.abc import Callable
from contextlib import suppress
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.runtime_code_e2e_support import (
    LAUNCHER_BYTES,
    build_test_package,
    install_test_package,
    open_test_capability,
)


def _artifact_digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _runtime_generation_failure(message: str) -> BaseException:
    from rquant.runtime_code_generation import RuntimeCodeGenerationError

    return RuntimeCodeGenerationError(message)


def _formal_runtime_generation_failure(message: str) -> BaseException:
    from rquant.formal_runtime import FormalRuntimeError

    failure = FormalRuntimeError(message)
    failure.__cause__ = _runtime_generation_failure(message)
    return failure


def _cross_device_failure(message: str) -> BaseException:
    return OSError(errno.EXDEV, message)


def _duplicate_high_descriptor(descriptor: int, *, minimum: int = 2_048) -> int:
    return int(fcntl.fcntl(descriptor, fcntl.F_DUPFD, minimum))


def _read_ready_descriptor(descriptor: int, size: int, *, timeout: float = 3) -> bytes:
    selector = selectors.DefaultSelector()
    try:
        selector.register(descriptor, selectors.EVENT_READ)
        if not selector.select(timeout):
            pytest.fail("formal smoke high-descriptor subprocess did not become readable")
        return os.read(descriptor, size)
    finally:
        selector.close()


def _success_exchange(*, mutate: str | None = None):
    def exchange(session: object, request_bytes: bytes):
        from rquant.formal_smoke_execution import FormalSmokeChildProcessResult
        from rquant.formal_smoke_protocol import (
            FormalSmokeArtifactReceipt,
            FormalSmokeExecutionReceipt,
            FormalSmokeExecutionRequest,
            FormalSmokeReplayPayload,
            formal_smoke_request_digest,
            formal_smoke_result_digest,
        )
        from rquant.strict_json import canonical_model_json_bytes

        assert b"PYTHONPATH" not in request_bytes
        assert b"RQUANT_ALLOWED" not in request_bytes
        assert b"synthetic-token" not in request_bytes
        request = FormalSmokeExecutionRequest.model_validate_json(request_bytes)
        assert "execution-binding-pending" in session.capability.audit_events
        assert "execution-binding-verified" not in session.capability.audit_events
        launcher_fd = session.require_launcher_descriptor()
        os.lseek(launcher_fd, 0, os.SEEK_SET)
        assert os.read(launcher_fd, len(LAUNCHER_BYTES) + 32) == LAUNCHER_BYTES

        run_id = "generation-a-formal-run"
        json_relative = Path("strategy_lab_runs") / f"{run_id}.json"
        markdown_relative = Path("strategy_lab_runs") / f"{run_id}.md"
        json_payload = b'{"generation_marker":"A"}\n'
        markdown_payload = b"# generation A\n"
        for relative, payload in (
            (json_relative, json_payload),
            (markdown_relative, markdown_payload),
        ):
            path = request.staging_root / relative
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            path.write_bytes(payload)

        result = FormalSmokeReplayPayload(
            strategy=request.strategy,
            fixed_spec_version="stage1-smoke-v1",
            run_id=run_id,
            audit_run_id=request.audit_run_id,
            dataset_snapshot_id=request.dataset_snapshot_id,
            dataset_binding_hash=request.dataset_binding_hash,
            code_commit=request.code_commit,
            strategy_spec_hash="7" * 64,
            result_hash="8" * 64,
            sample_count=1,
            metrics={"generation_marker": "A"},
            missing_evidence=(),
        )
        artifacts = (
            FormalSmokeArtifactReceipt(
                kind="json",
                relative_path=json_relative.as_posix(),
                size=len(json_payload),
                sha256=_artifact_digest(json_payload),
            ),
            FormalSmokeArtifactReceipt(
                kind="markdown",
                relative_path=markdown_relative.as_posix(),
                size=len(markdown_payload),
                sha256=_artifact_digest(markdown_payload),
            ),
        )
        values: dict[str, object] = {
            "code_trust_evidence": request.code_trust_evidence,
            "request_digest": formal_smoke_request_digest(request),
            "execution_identity": request.execution_identity,
            "result": result,
            "artifacts": artifacts,
            "result_digest": formal_smoke_result_digest(result, artifacts),
        }
        if mutate == "wrong_request":
            values["request_digest"] = "9" * 64
        elif mutate == "wrong_evidence":
            values["code_trust_evidence"] = request.code_trust_evidence.model_copy(
                update={"provenance_commit": "9" * 40}
            )
        elif mutate == "wrong_generation":
            values["execution_identity"] = request.execution_identity.model_copy(
                update={"generation_id": "9" * 64}
            )
        elif mutate == "wrong_launcher":
            values["execution_identity"] = request.execution_identity.model_copy(
                update={
                    "launcher": request.execution_identity.launcher.model_copy(
                        update={"sha256": "9" * 64}
                    )
                }
            )
        elif mutate == "wrong_interpreter":
            values["execution_identity"] = request.execution_identity.model_copy(
                update={
                    "interpreter": request.execution_identity.interpreter.model_copy(
                        update={"sha256": "9" * 64}
                    )
                }
            )
        elif mutate == "wrong_import_root":
            values["execution_identity"] = request.execution_identity.model_copy(
                update={"import_roots": ("release/other",)}
            )
        receipt = FormalSmokeExecutionReceipt(**values)
        payload = canonical_model_json_bytes(receipt)
        if mutate == "tampered_receipt":
            payload = payload.replace(b'"sample_count":1', b'"sample_count":2')
        elif mutate == "artifact_tamper":
            (request.staging_root / json_relative).write_bytes(b"tampered")
        elif mutate == "missing_artifact":
            (request.staging_root / markdown_relative).unlink()
        return FormalSmokeChildProcessResult(exit_code=0, receipt_bytes=payload)

    return exchange


def _run(
    root: Path,
    *,
    exchange: object,
):
    from rquant.formal_smoke_execution import _run_attested_formal_smoke
    from rquant.formal_smoke_protocol import FormalSmokeBootstrapReference

    package = build_test_package(root / "package")
    trusted_base, runtime_root, _installer = install_test_package(root, package)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    output = root / "output"
    output.mkdir(mode=0o700)
    result = _run_attested_formal_smoke(
        capability,
        strategy="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
        output_dir=output,
        bootstrap_reference=FormalSmokeBootstrapReference(
            configuration_path=(root / "authority/bootstrap.json").absolute(),
            trusted_base=(root / "authority").absolute(),
            expected_authority_uid=os.getuid(),
            expected_authority_gid=os.getgid(),
        ),
        environment_source={"PYTHONPATH": "/checkout-b", "RQUANT_ALLOWED": "yes"},
        exchange=exchange,
    )
    return capability, output, result


def _pid_is_present(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _assert_pid_disappears(pid: int, *, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not _pid_is_present(pid):
            return
        time.sleep(0.01)
    pytest.fail(f"formal smoke process {pid} survived lifecycle cleanup")


def _emergency_reap_test_processes(
    supervisor: subprocess.Popen[bytes] | None,
    descendant_pid: int | None,
) -> None:
    # This runs only after assertions, so a product leak still fails the test.
    if descendant_pid is not None:
        with suppress(ProcessLookupError, PermissionError):
            os.kill(descendant_pid, signal.SIGKILL)
    if supervisor is not None:
        with suppress(ProcessLookupError, PermissionError):
            os.kill(supervisor.pid, signal.SIGKILL)
        with suppress(ChildProcessError, subprocess.TimeoutExpired):
            supervisor.wait(timeout=1)


def _portable_lifecycle_spawn_factory(
    execution_module: object,
    *,
    identity_path: Path,
    tracked_descriptors: list[int],
    supervisor_kills_only_itself: bool,
):
    real_pipe = os.pipe
    real_close = os.close
    spawned: list[subprocess.Popen[bytes]] = []
    supervisor_final_signal = (
        "os.kill(os.getpid(), signal.SIGKILL)"
        if supervisor_kills_only_itself
        else "os.killpg(0, signal.SIGKILL)"
    )
    supervisor_script = (
        "import os, select, signal, subprocess, sys, time\n"
        "identity_path = sys.argv[1]\n"
        "control_descriptor = int(sys.argv[2])\n"
        "status_descriptor = int(sys.argv[3])\n"
        "request_descriptor = int(sys.argv[4])\n"
        "receipt_descriptor = int(sys.argv[5])\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "descendant = subprocess.Popen((\n"
        "    sys.executable, '-c',\n"
        "    'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)',\n"
        "))\n"
        "with open(identity_path, 'w', encoding='ascii') as target:\n"
        "    target.write(f'{os.getpid()}:{os.getpgrp()}\\n')\n"
        "    target.write(f'{descendant.pid}:{os.getpgid(descendant.pid)}\\n')\n"
        "def teardown_group():\n"
        "    try:\n"
        "        os.killpg(0, signal.SIGTERM)\n"
        "    except ProcessLookupError:\n"
        "        pass\n"
        "    time.sleep(0.1)\n"
        "    try:\n"
        "        os.write(status_descriptor, b'K')\n"
        "    except OSError:\n"
        "        pass\n"
        "    time.sleep(0.2)\n"
        f"    {supervisor_final_signal}\n"
        "request = bytearray()\n"
        "while True:\n"
        "    readable, _, _ = select.select([control_descriptor, request_descriptor], [], [], 1)\n"
        "    if control_descriptor in readable:\n"
        "        os.read(control_descriptor, 1)\n"
        "        teardown_group()\n"
        "    if request_descriptor in readable:\n"
        "        chunk = os.read(request_descriptor, 65536)\n"
        "        if not chunk:\n"
        "            break\n"
        "        request.extend(chunk)\n"
        "try:\n"
        "    os.write(receipt_descriptor, b'receipt:' + bytes(request))\n"
        "except BrokenPipeError:\n"
        "    pass\n"
        "os.close(receipt_descriptor)\n"
        "try:\n"
        "    os.write(status_descriptor, bytes((0,)))\n"
        "except BrokenPipeError:\n"
        "    pass\n"
        "os.read(control_descriptor, 1)\n"
        "teardown_group()\n"
    )

    def spawn(
        _session: object,
        *,
        request_descriptor: int,
        receipt_descriptor: int,
    ) -> object:
        control_read, control_write = real_pipe()
        status_read, status_write = real_pipe()
        tracked_descriptors.extend((control_read, control_write, status_read, status_write))
        try:
            process = subprocess.Popen(
                (
                    sys.executable,
                    "-c",
                    supervisor_script,
                    os.fspath(identity_path),
                    str(control_read),
                    str(status_write),
                    str(request_descriptor),
                    str(receipt_descriptor),
                ),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                pass_fds=(
                    request_descriptor,
                    receipt_descriptor,
                    control_read,
                    status_write,
                ),
                start_new_session=True,
            )
            spawned.append(process)
        except BaseException:
            real_close(control_write)
            real_close(status_read)
            raise
        finally:
            real_close(control_read)
            real_close(status_write)
        identity_deadline = time.monotonic() + 1
        while not identity_path.exists() and time.monotonic() < identity_deadline:
            time.sleep(0.01)
        assert identity_path.exists()
        return execution_module._FormalSmokeChildProcess(
            process=process,
            lifetime_descriptor=control_write,
            status_descriptor=status_read,
        )

    return spawn, spawned


def test_outer_command_is_only_a_verifier_and_launcher() -> None:
    from rquant.cli import cmd_formal_smoke_replay
    from rquant.formal_smoke_execution import run_attested_formal_smoke

    source = inspect.getsource(cmd_formal_smoke_replay)
    assert "from rquant.formal_smoke_replay import" not in source
    assert "run_formal_smoke_replay" not in source
    assert "formal_smoke_execution" in source
    assert "exchange" not in inspect.signature(run_attested_formal_smoke).parameters


def test_formal_smoke_child_uses_dynamic_private_fds_and_attested_launcher_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_smoke_runtime, exec_formal_smoke_child

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    session = bind_formal_smoke_runtime(
        capability,
        environment_source={"PYTHONPATH": "/checkout-b", "RQUANT_ALLOWED": "yes"},
    )
    request_read, request_write = os.pipe()
    receipt_read, receipt_write = os.pipe()
    captured: dict[str, object] = {}
    monkeypatch.setattr(os, "chdir", lambda _path: None)

    def executor(
        interpreter_fd: int,
        argv: tuple[str, ...],
        environment: object,
    ) -> None:
        command_index = argv.index("formal-smoke-runtime-execute")
        launcher_fd = int(argv[command_index - 1])
        request_fd = int(argv[argv.index("--request-fd") + 1])
        receipt_fd = int(argv[argv.index("--receipt-fd") + 1])
        os.lseek(launcher_fd, 0, os.SEEK_SET)
        captured.update(
            {
                "interpreter_fd": interpreter_fd,
                "launcher": os.read(launcher_fd, len(LAUNCHER_BYTES) + 1),
                "request_fd": request_fd,
                "receipt_fd": receipt_fd,
                "argv": argv,
                "environment": environment,
            }
        )

    try:
        exec_formal_smoke_child(
            session,
            request_descriptor=request_read,
            receipt_descriptor=receipt_write,
            executor=executor,
        )
    finally:
        for descriptor in (request_read, request_write, receipt_read, receipt_write):
            with suppress(OSError):
                os.close(descriptor)

    argv = captured["argv"]
    assert isinstance(argv, tuple)
    assert argv[1:4] == ("-I", "-S", "-c")
    assert "/dev/fd/" in argv[4]
    assert captured["launcher"] == LAUNCHER_BYTES
    assert captured["request_fd"] not in {request_read, receipt_write}
    assert captured["receipt_fd"] not in {request_read, receipt_write}
    assert captured["environment"] == {"RQUANT_ALLOWED": "yes"}


def test_generation_a_launcher_receipt_and_artifacts_are_the_only_success_identity(
    tmp_path: Path,
) -> None:
    capability, output, result = _run(tmp_path, exchange=_success_exchange())

    assert result.metrics == {"generation_marker": "A"}
    assert result.json_path.read_bytes() == b'{"generation_marker":"A"}\n'
    assert result.markdown_path.read_bytes() == b"# generation A\n"
    assert result.json_path.parent == output / "strategy_lab_runs"
    assert result.execution_receipt.code_trust_evidence == capability.evidence
    assert result.execution_receipt_digest == capability.execution_binding_digest
    assert "execution-binding-pending" not in capability.audit_events
    assert "execution-binding-verified" in capability.audit_events


@pytest.mark.parametrize(
    ("target", "failure_factory", "expected_phase", "expected_reason"),
    (
        (
            "_require_live_session",
            _formal_runtime_generation_failure,
            "post_child_liveness",
            "formal_runtime_runtime_generation",
        ),
        (
            "_verify_staged_artifacts",
            OSError,
            "verify_staged_artifacts",
            "os_error",
        ),
        (
            "_publish_artifacts",
            _cross_device_failure,
            "publish_artifacts",
            "os_error_exdev",
        ),
    ),
)
def test_wrapped_outer_failure_reports_only_stable_redacted_phase_and_reason(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    failure_factory: Callable[[str], BaseException],
    expected_phase: str,
    expected_reason: str,
) -> None:
    from rquant import formal_smoke_execution as execution_module
    from rquant.formal_smoke_execution import FormalSmokeExecutionError

    secret = "secret argv=/private/checkout-b token=synthetic-token"

    def fail(*_args: object, **_kwargs: object) -> object:
        raise failure_factory(secret)

    monkeypatch.setattr(execution_module, target, fail)

    with pytest.raises(FormalSmokeExecutionError) as raised:
        _run(tmp_path, exchange=_success_exchange())

    assert str(raised.value) == (
        f"formal smoke attested execution failed (phase={expected_phase} reason={expected_reason})"
    )
    assert secret not in str(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    assert raised.value.__context__ is not None
    assert secret in str(raised.value.__context__)
    rendered_traceback = "".join(traceback.format_exception(raised.value))
    assert secret not in rendered_traceback
    assert "/private/checkout-b" not in rendered_traceback
    assert "argv=" not in rendered_traceback
    assert "synthetic-token" not in rendered_traceback
    assert not list((tmp_path / "output").glob("strategy_lab_runs/*"))


def test_exchange_value_error_reports_redacted_fixed_subphase(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module
    from rquant.formal_smoke_execution import FormalSmokeExecutionError

    secret = "secret path=/private/checkout-b argv=--token synthetic-token"

    def fail_pipe() -> tuple[int, int]:
        raise ValueError(secret)

    monkeypatch.setattr(execution_module.os, "pipe", fail_pipe)

    with pytest.raises(FormalSmokeExecutionError) as raised:
        execution_module._exchange_formal_smoke_child(
            object(),  # type: ignore[arg-type]
            b"request",
            deadline_monotonic=time.monotonic() + 1,
        )

    assert str(raised.value) == (
        "formal smoke child exchange failed (subphase=setup_request_pipe reason=pipe_contract)"
    )
    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    assert raised.value.__context__ is not None
    assert secret in str(raised.value.__context__)
    assert secret not in "".join(traceback.format_exception(raised.value))


@pytest.mark.parametrize(
    "mutation",
    (
        "wrong_request",
        "wrong_evidence",
        "wrong_generation",
        "wrong_launcher",
        "wrong_interpreter",
        "wrong_import_root",
        "tampered_receipt",
        "artifact_tamper",
        "missing_artifact",
    ),
)
def test_receipt_mismatch_matrix_fails_closed_without_saved_run(
    tmp_path: Path,
    mutation: str,
) -> None:
    from rquant.formal_smoke_execution import FormalSmokeExecutionError

    with pytest.raises(FormalSmokeExecutionError):
        _run(tmp_path, exchange=_success_exchange(mutate=mutation))
    assert not list((tmp_path / "output").glob("strategy_lab_runs/*"))


def test_missing_receipt_and_child_failure_leave_no_saved_run(tmp_path: Path) -> None:
    from rquant.formal_smoke_execution import (
        FormalSmokeChildProcessResult,
        FormalSmokeExecutionError,
    )

    def fail(_session: object, request_bytes: bytes) -> FormalSmokeChildProcessResult:
        from rquant.formal_smoke_protocol import FormalSmokeExecutionRequest

        request = FormalSmokeExecutionRequest.model_validate_json(request_bytes)
        partial = request.staging_root / "strategy_lab_runs/partial.json"
        partial.parent.mkdir(mode=0o700, parents=True)
        partial.write_bytes(b"partial")
        return FormalSmokeChildProcessResult(exit_code=23, receipt_bytes=b"")

    with pytest.raises(FormalSmokeExecutionError, match="child"):
        _run(tmp_path, exchange=fail)
    assert not list((tmp_path / "output").glob("strategy_lab_runs/*"))
    assert not list((tmp_path / "output").glob(".formal-smoke-*"))


def test_receipt_fd_holder_times_out_reaps_process_group_and_cleans_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module
    from rquant.formal_smoke_execution import FormalSmokeExecutionError

    identity_path = tmp_path / "child-process-group.txt"
    script = (
        "import os, signal, subprocess, sys\n"
        "request_descriptor = int(sys.argv[1])\n"
        "receipt_descriptor = int(sys.argv[2])\n"
        "while os.read(request_descriptor, 65536):\n"
        "    pass\n"
        "os.close(request_descriptor)\n"
        "descendant = subprocess.Popen((\n"
        "    sys.executable, '-c',\n"
        "    'import signal,time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(10)',\n"
        "), pass_fds=(receipt_descriptor,))\n"
        "os.close(receipt_descriptor)\n"
        "with open(sys.argv[3], 'w', encoding='ascii') as target:\n"
        "    target.write(f'{os.getppid()}:{os.getpgrp()}\\n')\n"
        "    target.write(f'{descendant.pid}:{os.getpgid(descendant.pid)}\\n')\n"
    )

    class ReceiptHolderLaunch:
        def __init__(self, request_descriptor: int, receipt_descriptor: int) -> None:
            self.inherited_descriptors = (
                os.open(sys.executable, os.O_RDONLY),
                os.dup(request_descriptor),
                os.dup(receipt_descriptor),
            )
            self.interpreter_descriptor = self.inherited_descriptors[0]
            self.argv = (
                sys.executable,
                "-c",
                script,
                str(self.inherited_descriptors[1]),
                str(self.inherited_descriptors[2]),
                os.fspath(identity_path),
            )
            self.environment = {"PYTHONUNBUFFERED": "1"}
            self.working_directory = tmp_path

        def __enter__(self) -> ReceiptHolderLaunch:
            return self

        def __exit__(self, *_args: object) -> None:
            for descriptor in self.inherited_descriptors:
                os.close(descriptor)

    def prepare_receipt_holder_launch(
        _session: object,
        *,
        request_descriptor: int,
        receipt_descriptor: int,
    ) -> ReceiptHolderLaunch:
        return ReceiptHolderLaunch(request_descriptor, receipt_descriptor)

    if os.execve in os.supports_fd:
        monkeypatch.setattr(
            execution_module,
            "prepare_formal_smoke_launch",
            prepare_receipt_holder_launch,
        )
    else:

        def spawn_portable_supervisor(
            _session: object,
            *,
            request_descriptor: int,
            receipt_descriptor: int,
        ) -> object:
            supervisor_script = (
                "import os, signal, subprocess, sys, time\n"
                "control_descriptor = int(sys.argv[2])\n"
                "status_descriptor = int(sys.argv[3])\n"
                "request_descriptor = int(sys.argv[4])\n"
                "receipt_descriptor = int(sys.argv[5])\n"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                "while os.read(request_descriptor, 65536):\n"
                "    pass\n"
                "os.close(request_descriptor)\n"
                "descendant = subprocess.Popen((\n"
                "    sys.executable, '-c',\n"
                "    'import signal,time; "
                "signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(10)',\n"
                "), pass_fds=(receipt_descriptor,))\n"
                "os.close(receipt_descriptor)\n"
                "with open(sys.argv[1], 'w', encoding='ascii') as target:\n"
                "    target.write(f'{os.getpid()}:{os.getpgrp()}\\n')\n"
                "    target.write(f'{descendant.pid}:{os.getpgid(descendant.pid)}\\n')\n"
                "os.write(status_descriptor, bytes((0,)))\n"
                "os.close(status_descriptor)\n"
                "os.read(control_descriptor, 1)\n"
                "os.killpg(0, signal.SIGTERM)\n"
                "time.sleep(0.25)\n"
                "os.killpg(0, signal.SIGKILL)\n"
            )
            control_read, control_write = os.pipe()
            status_read, status_write = os.pipe()
            try:
                process = subprocess.Popen(
                    (
                        sys.executable,
                        "-c",
                        supervisor_script,
                        os.fspath(identity_path),
                        str(control_read),
                        str(status_write),
                        str(request_descriptor),
                        str(receipt_descriptor),
                    ),
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    close_fds=True,
                    pass_fds=(
                        request_descriptor,
                        receipt_descriptor,
                        control_read,
                        status_write,
                    ),
                    start_new_session=True,
                )
            except BaseException:
                os.close(control_write)
                os.close(status_read)
                raise
            finally:
                os.close(control_read)
                os.close(status_write)
            return execution_module._FormalSmokeChildProcess(
                process=process,
                lifetime_descriptor=control_write,
                status_descriptor=status_read,
            )

        monkeypatch.setattr(
            execution_module,
            "_spawn_formal_smoke_child",
            spawn_portable_supervisor,
        )

    def exchange(session: object, request_bytes: bytes):
        return execution_module._exchange_formal_smoke_child(
            session,
            request_bytes,
            deadline_monotonic=time.monotonic() + 0.75,
        )

    started = time.monotonic()
    with pytest.raises(FormalSmokeExecutionError, match="deadline") as raised:
        _run(tmp_path, exchange=exchange)
    elapsed = time.monotonic() - started

    assert elapsed < 2
    assert str(raised.value) == (
        "formal smoke child deadline expired "
        "(phase=awaiting_receipt_eof request_sent=true receipt_bytes=0 "
        "receipt_eof=false status_reported=true)"
    )
    identities = tuple(
        tuple(int(value) for value in line.split(":"))
        for line in identity_path.read_text(encoding="ascii").splitlines()
    )
    assert all(process_id == process_group for process_id, process_group in identities[:1])
    supervisor_pid = identities[0][0]
    assert identities[1][1] == supervisor_pid
    descendant_pid = identities[1][0]
    descendant_deadline = time.monotonic() + 3
    while time.monotonic() < descendant_deadline:
        try:
            os.kill(descendant_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.01)
    else:
        pytest.fail(f"formal smoke descendant {descendant_pid} survived group cleanup")
    with pytest.raises(ChildProcessError):
        os.waitpid(supervisor_pid, os.WNOHANG)
    assert not list((tmp_path / "output").glob("strategy_lab_runs/*"))
    assert not list((tmp_path / "output").glob(".formal-smoke-*"))


def test_parent_final_group_kill_removes_descendant_after_supervisor_self_kill_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    identity_path = tmp_path / "success-process-group.txt"
    tracked_descriptors: list[int] = []
    spawn, spawned = _portable_lifecycle_spawn_factory(
        execution_module,
        identity_path=identity_path,
        tracked_descriptors=tracked_descriptors,
        supervisor_kills_only_itself=True,
    )
    monkeypatch.setattr(execution_module, "_spawn_formal_smoke_child", spawn)

    supervisor: subprocess.Popen[bytes] | None = None
    descendant_pid: int | None = None
    try:
        result = execution_module._exchange_formal_smoke_child(
            object(),  # type: ignore[arg-type]
            b"request",
            deadline_monotonic=time.monotonic() + 2,
        )
        identities = tuple(
            tuple(int(value) for value in line.split(":"))
            for line in identity_path.read_text(encoding="ascii").splitlines()
        )
        supervisor = spawned[0]
        descendant_pid = identities[1][0]

        assert result.exit_code == 0
        assert result.receipt_bytes == b"receipt:request"
        assert identities[0] == (supervisor.pid, supervisor.pid)
        assert identities[1][1] == supervisor.pid
        _assert_pid_disappears(descendant_pid)
        with pytest.raises(ChildProcessError):
            os.waitpid(supervisor.pid, os.WNOHANG)
    finally:
        _emergency_reap_test_processes(supervisor, descendant_pid)


def test_supervisor_cleanup_state_matrix_preserves_or_loses_group_anchor_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    scenarios = (
        ("leader-running", None, None, False),
        ("leader-exited-unreaped", 126, None, False),
        ("group-esrch", None, ProcessLookupError(), False),
        ("group-eperm", None, PermissionError(), True),
    )

    class FakePopen:
        pid = 41_041

        def __init__(
            self,
            event_log: list[tuple[str, float | int]],
            observed_return_code: int | None,
        ) -> None:
            self.returncode: int | None = None
            self._event_log = event_log
            self._observed_return_code = observed_return_code

        def wait(self, *, timeout: float | None = None) -> int:
            assert timeout is not None
            self._event_log.append(("wait", timeout))
            self.returncode = self._observed_return_code or -signal.SIGKILL
            return self.returncode

    for name, observed_return_code, group_error, expect_error in scenarios:
        events: list[tuple[str, float | int]] = []
        control_read, control_write = os.pipe()
        status_read, status_write = os.pipe()
        os.close(control_read)
        os.close(status_write)

        process = execution_module._FormalSmokeChildProcess(
            process=FakePopen(events, observed_return_code),  # type: ignore[arg-type]
            lifetime_descriptor=control_write,
            status_descriptor=status_read,
        )
        if observed_return_code is not None:
            wait_observations = iter(
                (
                    SimpleNamespace(
                        si_pid=process.pid,
                        si_code=os.CLD_EXITED,
                        si_status=observed_return_code,
                    ),
                )
            )
        else:
            wait_observations = iter((None,))

        def waitid(
            *_args: object,
            observations: object = wait_observations,
        ) -> object | None:
            return next(observations, None)  # type: ignore[call-overload]

        def killpg(
            _process_group: int,
            signum: int,
            *,
            event_log: list[tuple[str, float | int]] = events,
            error: BaseException | None = group_error,
        ) -> None:
            event_log.append(("killpg", signum))
            if error is not None:
                raise error

        def kill(
            _pid: int,
            signum: int,
            *,
            event_log: list[tuple[str, float | int]] = events,
        ) -> None:
            event_log.append(("kill", signum))

        with monkeypatch.context() as scenario_patch:
            scenario_patch.setattr(execution_module.os, "waitid", waitid)
            scenario_patch.setattr(execution_module.os, "killpg", killpg)
            scenario_patch.setattr(
                execution_module.os,
                "kill",
                kill,
            )
            scenario_patch.setattr(execution_module.time, "sleep", lambda _seconds: None)

            if expect_error:
                with pytest.raises(execution_module.FormalSmokeCleanupError, match="permission"):
                    execution_module._cleanup_formal_smoke_supervisor(process)
            else:
                execution_module._cleanup_formal_smoke_supervisor(process)

        assert events.count(("killpg", signal.SIGKILL)) == 1, name
        assert next(index for index, event in enumerate(events) if event[0] == "killpg") < next(
            index for index, event in enumerate(events) if event[0] == "wait"
        ), name
        if isinstance(group_error, ProcessLookupError):
            assert events.count(("killpg", signal.SIGKILL)) == 1
            assert ("kill", signal.SIGKILL) not in events
        if isinstance(group_error, PermissionError):
            assert ("kill", signal.SIGKILL) not in events
        assert process.state is execution_module._SupervisorLifecycleState.REAPED


def test_already_reaped_supervisor_fails_without_signaling_reused_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    control_read, control_write = os.pipe()
    status_read, status_write = os.pipe()
    os.close(control_read)
    os.close(status_write)

    class ReapedPopen:
        pid = 41_042
        returncode = 0

    process = execution_module._FormalSmokeChildProcess(
        process=ReapedPopen(),  # type: ignore[arg-type]
        lifetime_descriptor=control_write,
        status_descriptor=status_read,
    )
    killpg = pytest.fail
    monkeypatch.setattr(execution_module.os, "killpg", killpg)

    with pytest.raises(execution_module.FormalSmokeCleanupError, match="reaped"):
        execution_module._cleanup_formal_smoke_supervisor(process)

    assert process.state is execution_module._SupervisorLifecycleState.REAPED


def test_supervisor_owned_teardown_still_gets_parent_final_group_kill_before_reap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    control_read, control_write = os.pipe()
    status_read, status_write = os.pipe()
    os.close(status_write)

    class KilledPopen:
        pid = 41_044
        returncode: int | None = None

        def wait(self, *, timeout: float | None = None) -> int:
            assert timeout is not None
            self.returncode = -signal.SIGKILL
            return self.returncode

    process = execution_module._FormalSmokeChildProcess(
        process=KilledPopen(),  # type: ignore[arg-type]
        lifetime_descriptor=control_write,
        status_descriptor=status_read,
        state=execution_module._SupervisorLifecycleState.STATUS_REPORTED,
    )
    monkeypatch.setattr(
        execution_module.os,
        "waitid",
        lambda *_args: SimpleNamespace(
            si_pid=process.pid,
            si_code=os.CLD_KILLED,
            si_status=signal.SIGKILL,
        ),
    )
    events: list[tuple[str, int]] = []
    monkeypatch.setattr(
        execution_module.os,
        "killpg",
        lambda process_group, signum: events.append(("killpg", signum)),
    )

    try:
        execution_module._cleanup_formal_smoke_supervisor(process)
        assert os.read(control_read, 1) == b"T"
    finally:
        os.close(control_read)

    assert events == [("killpg", signal.SIGKILL)]
    assert process.state is execution_module._SupervisorLifecycleState.REAPED


def test_teardown_ready_wait_supports_descriptor_above_select_fd_limit() -> None:
    from rquant import formal_smoke_execution as execution_module

    status_read, status_write = os.pipe()
    high_status_read = _duplicate_high_descriptor(status_read)
    os.close(status_read)

    class FakePopen:
        pid = 41_045
        returncode: int | None = None

    process = execution_module._FormalSmokeChildProcess(
        process=FakePopen(),  # type: ignore[arg-type]
        lifetime_descriptor=-1,
        status_descriptor=high_status_read,
    )
    try:
        os.write(status_write, b"K")
        assert execution_module._await_supervisor_teardown_ready(
            process,
            deadline_monotonic=time.monotonic() + 1,
        )
        assert process.state is execution_module._SupervisorLifecycleState.TEARDOWN_READY
    finally:
        os.close(status_write)
        process.close_status()


@pytest.mark.parametrize(
    ("fault", "expected_subphase", "expected_reason"),
    (
        ("teardown-wait", "cleanup_teardown_wait", "selector_contract"),
        ("waitid", "cleanup_waitid", "waitid_contract"),
        ("kill", "cleanup_kill", "process_group_contract"),
        ("reap", "cleanup_reap", "process_reap_contract"),
    ),
)
def test_cleanup_value_error_still_kills_real_group_and_reaps_supervisor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
    expected_subphase: str,
    expected_reason: str,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    secret = f"secret cleanup path=/private/{fault} argv=--token"
    identity_path = tmp_path / f"{fault}-cleanup-process-group.txt"
    tracked_descriptors: list[int] = []
    spawn, spawned = _portable_lifecycle_spawn_factory(
        execution_module,
        identity_path=identity_path,
        tracked_descriptors=tracked_descriptors,
        supervisor_kills_only_itself=False,
    )
    monkeypatch.setattr(execution_module, "_spawn_formal_smoke_child", spawn)

    target_name = {
        "teardown-wait": "_await_supervisor_teardown_ready",
        "waitid": "_observe_supervisor_without_reaping",
        "kill": "_final_kill_supervisor_group",
        "reap": "_reap_known_supervisor",
    }[fault]
    original = getattr(execution_module, target_name)
    injected = False

    def fail_once(*args: object, **kwargs: object) -> object:
        nonlocal injected
        if not injected:
            injected = True
            raise ValueError(secret)
        return original(*args, **kwargs)

    monkeypatch.setattr(execution_module, target_name, fail_once)

    supervisor: subprocess.Popen[bytes] | None = None
    descendant_pid: int | None = None
    try:
        with pytest.raises(
            execution_module.FormalSmokeCleanupError,
            match=(
                "formal smoke supervisor cleanup failed "
                f"\\(subphase={expected_subphase} reason={expected_reason}\\)"
            ),
        ) as raised:
            execution_module._exchange_formal_smoke_child(
                object(),  # type: ignore[arg-type]
                b"request",
                deadline_monotonic=time.monotonic() + 2,
            )

        identities = tuple(
            tuple(int(value) for value in line.split(":"))
            for line in identity_path.read_text(encoding="ascii").splitlines()
        )
        supervisor = spawned[0]
        descendant_pid = identities[1][0]

        assert injected
        assert str(raised.value.__cause__) == (
            "formal smoke child exchange failed "
            f"(subphase={expected_subphase} reason={expected_reason})"
        )
        assert secret not in "".join(traceback.format_exception(raised.value))
        _assert_pid_disappears(descendant_pid)
        with pytest.raises(ChildProcessError):
            os.waitpid(supervisor.pid, os.WNOHANG)
        for descriptor in set(tracked_descriptors):
            with pytest.raises(OSError):
                os.fstat(descriptor)
    finally:
        _emergency_reap_test_processes(supervisor, descendant_pid)


@pytest.mark.skipif(
    os.execve not in os.supports_fd,
    reason="descriptor exec is required to exercise the real supervisor bootstrap",
)
def test_real_supervisor_bootstrap_handles_high_lifetime_descriptor() -> None:
    from rquant import formal_smoke_execution as execution_module

    descriptors: list[int] = []

    def high_pipe() -> tuple[int, int]:
        read_descriptor, write_descriptor = os.pipe()
        high_read = _duplicate_high_descriptor(read_descriptor)
        descriptors.append(high_read)
        high_write = _duplicate_high_descriptor(write_descriptor, minimum=high_read + 1)
        descriptors.append(high_write)
        os.close(read_descriptor)
        os.close(write_descriptor)
        return high_read, high_write

    receipt_read, receipt_write = high_pipe()
    lifetime_read, lifetime_write = high_pipe()
    status_read, status_write = high_pipe()
    interpreter_source = os.open(sys.executable, os.O_RDONLY)
    interpreter = _duplicate_high_descriptor(interpreter_source)
    os.close(interpreter_source)
    descriptors.append(interpreter)
    child_code = (
        "import os,sys; descriptor=int(sys.argv[1]); "
        "os.write(descriptor,b'receipt'); os.close(descriptor)"
    )
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            (
                sys.executable,
                "-I",
                "-S",
                "-c",
                execution_module._DESCRIPTOR_HANDOFF_BOOTSTRAP,
                str(interpreter),
                os.getcwd(),
                json.dumps(
                    (sys.executable, "-c", child_code, str(receipt_write)),
                    separators=(",", ":"),
                ),
                "{}",
                json.dumps((interpreter, receipt_write), separators=(",", ":")),
                str(lifetime_read),
                str(status_write),
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            pass_fds=(interpreter, receipt_write, lifetime_read, status_write),
            start_new_session=True,
        )
        for descriptor in (interpreter, receipt_write, lifetime_read, status_write):
            os.close(descriptor)
            descriptors.remove(descriptor)

        assert _read_ready_descriptor(receipt_read, len(b"receipt") + 1) == b"receipt"
        assert _read_ready_descriptor(status_read, 1) == b"\x00"
        os.write(lifetime_write, b"T")
        assert _read_ready_descriptor(status_read, 1) == b"K"
        assert process.wait(timeout=3) == -signal.SIGKILL
    finally:
        if process is not None and process.returncode is None:
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            with suppress(subprocess.TimeoutExpired):
                process.wait(timeout=1)
        for descriptor in descriptors:
            with suppress(OSError):
                os.close(descriptor)


@pytest.mark.parametrize(
    "fault",
    (
        "selector-constructor",
        "register-request",
        "register-receipt",
        "register-status",
        "set-blocking",
        "close-child-end",
    ),
)
def test_post_spawn_initialization_failure_cleans_group_and_all_descriptors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    class InjectedInitializationError(RuntimeError):
        pass

    identity_path = tmp_path / f"{fault}-process-group.txt"
    tracked_descriptors: list[int] = []
    real_pipe = os.pipe
    real_close = os.close
    real_set_blocking = os.set_blocking
    real_selector = execution_module.selectors.DefaultSelector

    def recording_pipe() -> tuple[int, int]:
        descriptors = real_pipe()
        tracked_descriptors.extend(descriptors)
        return descriptors

    spawn, spawned = _portable_lifecycle_spawn_factory(
        execution_module,
        identity_path=identity_path,
        tracked_descriptors=tracked_descriptors,
        supervisor_kills_only_itself=False,
    )
    monkeypatch.setattr(execution_module.os, "pipe", recording_pipe)
    monkeypatch.setattr(execution_module, "_spawn_formal_smoke_child", spawn)

    if fault == "selector-constructor":

        def fail_selector_constructor() -> object:
            raise InjectedInitializationError(fault)

        monkeypatch.setattr(
            execution_module.selectors,
            "DefaultSelector",
            fail_selector_constructor,
        )
    elif fault.startswith("register-"):
        failing_registration = {
            "register-request": 1,
            "register-receipt": 2,
            "register-status": 3,
        }[fault]

        class FaultingSelector:
            def __init__(self) -> None:
                self.delegate = real_selector()
                self.register_count = 0

            def register(self, *args: object) -> object:
                self.register_count += 1
                if self.register_count == failing_registration:
                    raise InjectedInitializationError(fault)
                return self.delegate.register(*args)

            def unregister(self, *args: object) -> object:
                return self.delegate.unregister(*args)

            def select(self, *args: object) -> object:
                return self.delegate.select(*args)

            def close(self) -> None:
                self.delegate.close()

        monkeypatch.setattr(
            execution_module.selectors,
            "DefaultSelector",
            FaultingSelector,
        )
    elif fault == "set-blocking":
        set_blocking_failed = False

        def fail_set_blocking(descriptor: int, blocking: bool) -> None:
            nonlocal set_blocking_failed
            real_set_blocking(descriptor, blocking)
            if not set_blocking_failed:
                set_blocking_failed = True
                raise InjectedInitializationError(fault)

        monkeypatch.setattr(execution_module.os, "set_blocking", fail_set_blocking)
    else:
        close_failed = False

        def fail_close(descriptor: int) -> None:
            nonlocal close_failed
            real_close(descriptor)
            if not close_failed and tracked_descriptors and descriptor == tracked_descriptors[0]:
                close_failed = True
                raise InjectedInitializationError(fault)

        monkeypatch.setattr(execution_module.os, "close", fail_close)

    supervisor: subprocess.Popen[bytes] | None = None
    descendant_pid: int | None = None
    try:
        with pytest.raises(InjectedInitializationError, match=fault):
            execution_module._exchange_formal_smoke_child(
                object(),  # type: ignore[arg-type]
                b"request",
                deadline_monotonic=time.monotonic() + 2,
            )
        identities = tuple(
            tuple(int(value) for value in line.split(":"))
            for line in identity_path.read_text(encoding="ascii").splitlines()
        )
        supervisor = spawned[0]
        descendant_pid = identities[1][0]

        _assert_pid_disappears(descendant_pid)
        with pytest.raises(ChildProcessError):
            os.waitpid(supervisor.pid, os.WNOHANG)
        for descriptor in set(tracked_descriptors):
            with pytest.raises(OSError):
                os.fstat(descriptor)
    finally:
        _emergency_reap_test_processes(supervisor, descendant_pid)
        for descriptor in set(tracked_descriptors):
            with suppress(OSError):
                real_close(descriptor)


def test_cleanup_failure_links_only_redacted_initialization_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    held_descriptors: list[int] = []
    secret_path = "/private/formal-smoke-secret.py"
    secret_argv = "--authority-token=argv-secret"
    secret_token = "credential-token-secret"

    class FakePopen:
        pid = 41_043
        returncode: int | None = None

    def spawn_for_initialization_failure(
        _session: object,
        *,
        request_descriptor: int,
        receipt_descriptor: int,
    ) -> object:
        assert request_descriptor >= 0
        assert receipt_descriptor >= 0
        control_read, control_write = os.pipe()
        status_read, status_write = os.pipe()
        held_descriptors.extend((control_read, status_write))
        return execution_module._FormalSmokeChildProcess(
            process=FakePopen(),  # type: ignore[arg-type]
            lifetime_descriptor=control_write,
            status_descriptor=status_read,
        )

    def fail_cleanup(process: object, *, progress: object | None = None) -> None:
        assert progress is not None
        child = process
        child.close_lifetime()  # type: ignore[attr-defined]
        child.close_status()  # type: ignore[attr-defined]
        raise execution_module.FormalSmokeCleanupError("forced cleanup failure")

    def fail_selector_constructor() -> object:
        raise ValueError(f"{secret_path} {secret_argv} {secret_token}")

    monkeypatch.setattr(
        execution_module,
        "_spawn_formal_smoke_child",
        spawn_for_initialization_failure,
    )
    monkeypatch.setattr(
        execution_module.selectors,
        "DefaultSelector",
        fail_selector_constructor,
    )
    monkeypatch.setattr(
        execution_module,
        "_cleanup_formal_smoke_supervisor",
        fail_cleanup,
    )

    try:
        with pytest.raises(
            execution_module.FormalSmokeCleanupError,
            match="forced cleanup failure",
        ) as captured:
            execution_module._exchange_formal_smoke_child(
                object(),  # type: ignore[arg-type]
                b"request",
                deadline_monotonic=time.monotonic() + 1,
            )
    finally:
        for descriptor in held_descriptors:
            with suppress(OSError):
                os.close(descriptor)

    assert isinstance(captured.value.__cause__, execution_module.FormalSmokeExecutionError)
    assert str(captured.value.__cause__) == (
        "formal smoke child exchange failed (subphase=create_selector reason=selector_contract)"
    )
    assert captured.value.__suppress_context__ is True
    assert isinstance(captured.value.__context__, ValueError)
    rendered = "".join(traceback.format_exception(captured.value))
    for secret in (secret_path, secret_argv, secret_token):
        assert secret not in rendered


def test_formal_smoke_child_launch_has_no_fork_window_when_thread_starts_after_pipe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module

    late_stop = threading.Event()
    late_ready = threading.Event()
    child_done = threading.Event()
    child_threads: list[threading.Thread] = []
    real_pipe = os.pipe
    pipe_count = 0

    def hold_late_thread() -> None:
        late_ready.set()
        late_stop.wait(timeout=5)

    late_thread = threading.Thread(target=hold_late_thread, name="formal-smoke-late-thread")

    def racing_pipe() -> tuple[int, int]:
        nonlocal pipe_count
        descriptors = real_pipe()
        pipe_count += 1
        if pipe_count == 1:
            late_thread.start()
            assert late_ready.wait(timeout=1)
        return descriptors

    class FakeProcess:
        pid = 999_999
        returncode: int | None = None

    class FakeLaunch:
        def __init__(self, request_descriptor: int, receipt_descriptor: int) -> None:
            self.inherited_descriptors = (
                os.open(os.devnull, os.O_RDONLY),
                os.dup(request_descriptor),
                os.dup(receipt_descriptor),
                os.open(os.devnull, os.O_RDONLY),
            )
            self.interpreter_descriptor = self.inherited_descriptors[0]
            self.argv = ("verified-python", "formal-smoke-runtime-execute")
            self.environment: dict[str, str] = {}
            self.working_directory = Path("/")

        def __enter__(self) -> FakeLaunch:
            return self

        def __exit__(self, *_args: object) -> None:
            for descriptor in self.inherited_descriptors:
                os.close(descriptor)

    def prepare_launch(
        _session: object,
        *,
        request_descriptor: int,
        receipt_descriptor: int,
    ) -> FakeLaunch:
        assert late_ready.is_set()
        return FakeLaunch(request_descriptor, receipt_descriptor)

    def start_supervisor(command: tuple[str, ...], **kwargs: object) -> FakeProcess:
        assert late_ready.is_set()
        assert command[:4] == (sys.executable, "-I", "-S", "-c")
        assert kwargs["start_new_session"] is True
        assert kwargs["env"] == {}
        assert "preexec_fn" not in kwargs
        pass_fds = kwargs["pass_fds"]
        assert isinstance(pass_fds, tuple)
        child_request = os.dup(pass_fds[1])
        child_receipt = os.dup(pass_fds[2])
        child_status = os.dup(pass_fds[-1])

        def exchange_bytes() -> None:
            try:
                request = bytearray()
                while True:
                    chunk = os.read(child_request, 64 * 1024)
                    if not chunk:
                        break
                    request.extend(chunk)
                os.write(child_receipt, b"receipt:" + bytes(request))
                os.write(child_status, bytes((0,)))
            finally:
                os.close(child_request)
                os.close(child_receipt)
                os.close(child_status)
                child_done.set()

        child_thread = threading.Thread(target=exchange_bytes, name="formal-smoke-fake-child")
        child_threads.append(child_thread)
        child_thread.start()
        return FakeProcess()

    monkeypatch.setattr(execution_module.os, "pipe", racing_pipe)
    monkeypatch.setattr(
        execution_module.os,
        "fork",
        lambda: pytest.fail("formal smoke launch used Python os.fork after the thread race"),
    )
    monkeypatch.setattr(
        execution_module,
        "prepare_formal_smoke_launch",
        prepare_launch,
    )
    monkeypatch.setattr(execution_module.subprocess, "Popen", start_supervisor)

    def finish_fake_supervisor(process: object, *, progress: object | None = None) -> None:
        assert progress is not None
        child = process
        child.close_lifetime()  # type: ignore[attr-defined]
        child.close_status()  # type: ignore[attr-defined]
        child.process.returncode = -signal.SIGKILL  # type: ignore[attr-defined]
        child.state = execution_module._SupervisorLifecycleState.REAPED  # type: ignore[attr-defined]

    monkeypatch.setattr(
        execution_module,
        "_cleanup_formal_smoke_supervisor",
        finish_fake_supervisor,
    )

    try:
        result = execution_module._exchange_formal_smoke_child(
            object(),  # type: ignore[arg-type]
            b"request",
            deadline_monotonic=time.monotonic() + 2,
        )
    finally:
        late_stop.set()
        late_thread.join(timeout=1)
        for child_thread in child_threads:
            child_thread.join(timeout=1)

    assert result.exit_code == 0
    assert result.receipt_bytes == b"receipt:request"
    assert not late_thread.is_alive()
    assert all(not child_thread.is_alive() for child_thread in child_threads)


def test_publication_directory_swap_before_link_fails_closed_in_both_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_smoke_execution as execution_module
    from rquant.formal_smoke_execution import FormalSmokeExecutionError

    success = _success_exchange()
    output = tmp_path / "output"
    displaced = output / "validated-strategy-lab-runs"
    current = output / "strategy_lab_runs"
    original_link = os.link
    swapped = False
    marked_verified = False

    def swapping_link(
        source: object,
        destination: object,
        *args: object,
        **kwargs: object,
    ) -> None:
        nonlocal swapped
        if not swapped:
            swapped = True
            current.rename(displaced)
            current.mkdir(mode=0o755)
        original_link(source, destination, *args, **kwargs)

    def exchange(session: object, request_bytes: bytes):
        process_result = success(session, request_bytes)
        monkeypatch.setattr(execution_module.os, "link", swapping_link)
        return process_result

    def mark_verified(_capability: object, _binding_digest: str) -> None:
        nonlocal marked_verified
        marked_verified = True

    monkeypatch.setattr(execution_module, "_mark_verified_execution", mark_verified)

    with pytest.raises(FormalSmokeExecutionError):
        _run(tmp_path, exchange=exchange)

    assert swapped
    assert not marked_verified
    assert not list(current.glob("*"))
    assert not list(displaced.glob("*"))


def test_closed_or_tampered_generation_fails_before_child_execution(tmp_path: Path) -> None:
    from rquant.formal_smoke_execution import (
        FormalSmokeExecutionError,
        _run_attested_formal_smoke,
    )
    from rquant.formal_smoke_protocol import FormalSmokeBootstrapReference

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    capability.close()
    output = tmp_path / "output"
    output.mkdir(mode=0o700)
    child_started = False

    def forbidden(_session: object, _request_bytes: bytes) -> object:
        nonlocal child_started
        child_started = True
        raise AssertionError("child must not start")

    with pytest.raises(FormalSmokeExecutionError):
        _run_attested_formal_smoke(
            capability,
            strategy="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 2),
            audit_run_id="a" * 64,
            dataset_snapshot_id="b" * 64,
            dataset_binding_hash="c" * 64,
            output_dir=output,
            bootstrap_reference=FormalSmokeBootstrapReference(
                configuration_path=(tmp_path / "authority/bootstrap.json").absolute(),
                trusted_base=(tmp_path / "authority").absolute(),
                expected_authority_uid=os.getuid(),
                expected_authority_gid=os.getgid(),
            ),
            environment_source={},
            exchange=forbidden,
        )
    assert not child_started
    assert not list(output.glob("strategy_lab_runs/*"))


def test_launcher_path_swap_is_detected_before_artifact_publication(tmp_path: Path) -> None:
    from rquant.formal_smoke_execution import FormalSmokeExecutionError

    def swap(session: object, request_bytes: bytes):
        launcher = session.plan.launcher
        generation = session.capability.loaded.generation_root
        generation.parent.chmod(0o755)
        generation.chmod(0o755)
        launcher.parent.parent.chmod(0o755)
        launcher.parent.chmod(0o755)
        replacement = launcher.with_name("checkout-b-launcher")
        replacement.write_bytes(b"B_LAUNCHER = True\n")
        replacement.chmod(0o555)
        os.replace(replacement, launcher)
        return _success_exchange()(session, request_bytes)

    with pytest.raises(FormalSmokeExecutionError):
        _run(tmp_path, exchange=swap)
    assert not list((tmp_path / "output").glob("strategy_lab_runs/*"))


def test_generation_entry_reopens_and_rejects_a_different_signed_current_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import formal_runtime_composition as composition
    from rquant import formal_smoke_runtime_entry as entry
    from rquant.formal_smoke_execution import (
        FormalSmokeChildProcessResult,
        FormalSmokeExecutionError,
    )

    package_b = build_test_package(
        tmp_path / "generation-b/package",
        provenance_commit="e" * 40,
    )
    trusted_b, runtime_b, _installer = install_test_package(
        tmp_path / "generation-b",
        package_b,
    )
    business_started = False

    def exchange(session: object, request_bytes: bytes) -> FormalSmokeChildProcessResult:
        nonlocal business_started
        capability_b = open_test_capability(
            trusted_base=trusted_b,
            runtime_root=runtime_b,
            package=package_b,
        )
        monkeypatch.setattr(entry, "_read_private_pipe", lambda _fd: request_bytes)
        monkeypatch.setattr(entry, "_validate_execution_context", lambda _request: None)
        monkeypatch.setattr(
            composition,
            "open_formal_runtime_capability",
            lambda **_kwargs: capability_b,
        )

        def forbidden(*_args: object, **_kwargs: object) -> object:
            nonlocal business_started
            business_started = True
            raise AssertionError("business must not run")

        import rquant.formal_smoke_replay as replay

        monkeypatch.setattr(replay, "run_formal_smoke_replay", forbidden)
        with pytest.raises(entry.FormalSmokeGenerationEntryError, match="does not match"):
            entry.run_formal_smoke_generation_entry(request_fd=17, receipt_fd=18)
        return FormalSmokeChildProcessResult(exit_code=70, receipt_bytes=b"")

    with pytest.raises(FormalSmokeExecutionError, match="child"):
        _run(tmp_path / "generation-a", exchange=exchange)
    assert not business_started
    assert not list((tmp_path / "generation-a/output").glob("strategy_lab_runs/*"))
