from __future__ import annotations

import hashlib
import inspect
import os
import subprocess
import sys
import threading
import time
from contextlib import suppress
from datetime import date
from pathlib import Path

import pytest

from tests.runtime_code_e2e_support import (
    LAUNCHER_BYTES,
    build_test_package,
    install_test_package,
    open_test_capability,
)


def _artifact_digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


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
    child_processes: list[subprocess.Popen[bytes]] = []

    def retain_receipt_fd_forever(
        _session: object,
        *,
        request_descriptor: int,
        receipt_descriptor: int,
    ) -> object:
        script = (
            "import os, subprocess, sys, time\n"
            "descendant = subprocess.Popen((sys.executable, '-c', 'import time; time.sleep(30)'))\n"
            "with open(sys.argv[1], 'w', encoding='ascii') as target:\n"
            "    target.write(f'{os.getpid()}:{os.getpgrp()}\\n')\n"
            "    target.write(f'{descendant.pid}:{os.getpgid(descendant.pid)}\\n')\n"
            "time.sleep(30)\n"
        )
        process = subprocess.Popen(
            (
                sys.executable,
                "-c",
                script,
                os.fspath(identity_path),
            ),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            pass_fds=(request_descriptor, receipt_descriptor),
            start_new_session=True,
        )
        child_processes.append(process)
        return execution_module._FormalSmokeChildProcess(
            process=process,
            lifetime_descriptor=-1,
        )

    monkeypatch.setattr(
        execution_module,
        "_spawn_formal_smoke_child",
        retain_receipt_fd_forever,
    )

    def exchange(session: object, request_bytes: bytes):
        return execution_module._exchange_formal_smoke_child(
            session,
            request_bytes,
            deadline_monotonic=time.monotonic() + 0.25,
        )

    started = time.monotonic()
    with pytest.raises(FormalSmokeExecutionError, match="deadline"):
        _run(tmp_path, exchange=exchange)
    elapsed = time.monotonic() - started

    assert elapsed < 2
    assert len(child_processes) == 1
    assert child_processes[0].poll() is not None
    identities = tuple(
        tuple(int(value) for value in line.split(":"))
        for line in identity_path.read_text(encoding="ascii").splitlines()
    )
    assert identities[0][0] == child_processes[0].pid
    assert all(process_id == process_group for process_id, process_group in identities[:1])
    assert identities[1][1] == child_processes[0].pid
    assert not list((tmp_path / "output").glob("strategy_lab_runs/*"))
    assert not list((tmp_path / "output").glob(".formal-smoke-*"))


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

        def poll(self) -> int | None:
            return 0 if child_done.is_set() else None

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

        def exchange_bytes() -> None:
            try:
                request = bytearray()
                while True:
                    chunk = os.read(child_request, 64 * 1024)
                    if not chunk:
                        break
                    request.extend(chunk)
                os.write(child_receipt, b"receipt:" + bytes(request))
            finally:
                os.close(child_request)
                os.close(child_receipt)
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
