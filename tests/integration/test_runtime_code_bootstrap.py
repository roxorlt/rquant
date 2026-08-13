from __future__ import annotations

import os
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest


def test_p0_07_post_verify_replacement_fails_before_target_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.authority_path_security import AuthorityPathSecurityError
    from rquant.formal_runtime import (
        FormalRuntimeError,
        bind_formal_runtime,
        exec_formal_runtime,
    )
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
        open_test_capability,
    )

    monkeypatch.setattr(os, "chdir", lambda _path: None)
    for index, selected in enumerate(("pointer", "generation", "archive", "release")):
        root = tmp_path / f"case-{index}"
        package = build_test_package(root / "package")
        trusted_base, runtime_root, _installer = install_test_package(root, package)
        capability = open_test_capability(
            trusted_base=trusted_base,
            runtime_root=runtime_root,
            package=package,
        )
        session = bind_formal_runtime(
            capability,
            daemon_argv=("lab-finalizer",),
            environment_source={"RQUANT_ALLOWED": "yes"},
            expected_python_abi="test-abi",
        )
        generation = runtime_root / "generations" / package.receipt.generation_id
        if selected == "pointer":
            changed = runtime_root / ".changed-current"
            changed.write_text("f" * 64 + "\n", encoding="ascii")
            changed.chmod(0o444)
            os.replace(changed, runtime_root / "current")
        elif selected == "generation":
            generation.parent.chmod(0o755)
            generation.chmod(0o755)
            moved = generation.with_name(generation.name + ".moved")
            generation.rename(moved)
            generation.symlink_to(moved)
        elif selected == "archive":
            generation.chmod(0o755)
            archive = generation / "runtime-code.bundle"
            changed = archive.with_name("changed.bundle")
            changed.write_bytes(b"X" * archive.stat().st_size)
            changed.chmod(0o444)
            os.replace(changed, archive)
        else:
            generation.chmod(0o755)
            release = generation / "release"
            release.chmod(0o755)
            moved = generation / "release.moved"
            release.rename(moved)
            release.symlink_to(moved)
        target_started: list[bool] = []

        def target(*_args: object, started: list[bool] = target_started) -> None:
            started.append(True)

        with pytest.raises(
            FormalRuntimeError,
            match="^formal runtime generation validation failed$",
        ) as raised:
            exec_formal_runtime(session, executor=target)
        assert isinstance(raised.value.__cause__, AuthorityPathSecurityError)
        assert not target_started
        session.close()


def test_verified_interpreter_fd_cannot_be_hijacked_by_a_final_path_swap(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_runtime, exec_formal_runtime
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
        open_test_capability,
    )

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    session = bind_formal_runtime(
        capability,
        daemon_argv=("lab-worker",),
        environment_source={},
        expected_python_abi="test-abi",
    )
    interpreter = session.plan.interpreter
    original_identity = interpreter.stat().st_ino
    captured_fd = -1
    monkeypatch.setattr(os, "chdir", lambda _path: None)

    def swap_then_execute(
        descriptor: int,
        _argv: tuple[str, ...],
        _environment: object,
    ) -> bytes:
        nonlocal captured_fd
        captured_fd = descriptor
        interpreter.parent.parent.parent.chmod(0o755)
        interpreter.parent.parent.chmod(0o755)
        interpreter.parent.chmod(0o755)
        replacement = interpreter.with_name("replacement-python")
        replacement.write_bytes(b"HIJACKED\n")
        replacement.chmod(0o555)
        os.replace(replacement, interpreter)
        assert interpreter.stat().st_ino != original_identity
        assert os.fstat(descriptor).st_ino == original_identity
        os.lseek(descriptor, 0, os.SEEK_SET)
        return os.read(descriptor, 4096)

    assert exec_formal_runtime(session, executor=swap_then_execute) == b"RQUANT-TEST-INTERPRETER\n"
    with pytest.raises(OSError):
        os.fstat(captured_fd)


def test_verified_interpreter_fd_is_closed_when_executor_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_runtime, exec_formal_runtime
    from rquant.runtime_code_generation import RuntimeCodeGenerationError
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
        open_test_capability,
    )

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    session = bind_formal_runtime(
        capability,
        daemon_argv=("lab-finalizer",),
        environment_source={},
        expected_python_abi="test-abi",
    )
    captured_fd = -1
    monkeypatch.setattr(os, "chdir", lambda _path: None)

    def fail(descriptor: int, _argv: tuple[str, ...], _environment: object) -> None:
        nonlocal captured_fd
        captured_fd = descriptor
        raise RuntimeError("exec failed")

    with pytest.raises(RuntimeError, match="exec failed"):
        exec_formal_runtime(session, executor=fail)
    with pytest.raises(OSError):
        os.fstat(captured_fd)
    with pytest.raises(RuntimeCodeGenerationError, match="closed"):
        capability.require_live()


def test_default_executor_uses_execve_descriptor_when_supported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import fd_exec
    from rquant.formal_runtime import _exec_verified_descriptor

    calls: list[tuple[int, tuple[str, ...], dict[str, str]]] = []

    def capture(descriptor: int, argv: tuple[str, ...], environment: dict[str, str]) -> None:
        calls.append((descriptor, argv, environment))

    monkeypatch.setattr(fd_exec.os, "execve", capture)
    monkeypatch.setattr(fd_exec.os, "supports_fd", frozenset({capture}))
    _exec_verified_descriptor(17, ("python", "-I", "-S", "launcher"), {"SAFE": "1"})
    assert calls == [(17, ("python", "-I", "-S", "launcher"), {"SAFE": "1"})]


@pytest.mark.skipif(
    os.execve in os.supports_fd,
    reason="platform supports descriptor-based os.execve",
)
def test_default_executor_fails_closed_without_fd_exec_support() -> None:
    from rquant.formal_runtime import FormalRuntimeError, _exec_verified_descriptor

    with pytest.raises(
        FormalRuntimeError,
        match="^formal descriptor execution is unavailable on this platform$",
    ):
        _exec_verified_descriptor(17, ("python", "-I", "-S", "launcher"), {"SAFE": "1"})


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux FD exec exact gate")
def test_linux_default_executor_executes_verified_descriptor_after_path_swap(
    tmp_path: Path,
) -> None:
    from rquant.fd_exec import exec_verified_descriptor

    assert os.execve in os.supports_fd
    candidate = tmp_path / "verified-sh"
    shutil.copyfile(Path("/bin/sh").resolve(strict=True), candidate)
    candidate.chmod(0o555)
    descriptor = os.open(candidate, os.O_RDONLY)
    verified_identity = os.fstat(descriptor)
    replacement = tmp_path / "replacement-sh"
    replacement.write_text("#!/bin/sh\nprintf 'HIJACKED\\n'\nexit 91\n", encoding="ascii")
    replacement.chmod(0o555)
    os.replace(replacement, candidate)
    assert candidate.stat().st_ino != verified_identity.st_ino

    read_fd, write_fd = os.pipe()
    child = os.fork()
    if child == 0:  # pragma: no cover - asserted through parent output/status
        try:
            os.close(read_fd)
            os.dup2(write_fd, 1)
            os.close(write_fd)
            exec_verified_descriptor(
                descriptor,
                (str(candidate), "-c", "printf 'VERIFIED-FD\\n'; exit 23"),
                {"PATH": "/usr/bin:/bin"},
            )
        except BaseException as exc:
            os.write(1, f"EXEC-ERROR:{type(exc).__name__}:{exc}\n".encode("ascii"))
            os._exit(127)
    os.close(write_fd)
    os.close(descriptor)
    output = b""
    try:
        while chunk := os.read(read_fd, 4096):
            output += chunk
    finally:
        os.close(read_fd)
    waited, status = os.waitpid(child, 0)

    assert waited == child
    assert os.WIFEXITED(status)
    assert os.WEXITSTATUS(status) == 23
    assert output == b"VERIFIED-FD\n"


def test_formal_runtime_does_not_wrap_unexpected_liveness_errors(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_runtime
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
        open_test_capability,
    )

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    session = bind_formal_runtime(
        capability,
        daemon_argv=("lab-finalizer",),
        environment_source={},
        expected_python_abi="test-abi",
    )

    def programmer_error() -> None:
        raise TypeError("programmer defect")

    monkeypatch.setattr(capability, "require_live", programmer_error)
    with pytest.raises(TypeError, match="programmer defect"):
        session.require_live()
    session.close()


def test_p0_10_inherited_python_user_site_loader_and_import_roots_are_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import (
        FormalRuntimeError,
        bind_formal_runtime,
        exec_formal_runtime,
    )
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
        open_test_capability,
    )

    external = tmp_path / "external"
    external.mkdir()
    external.joinpath("sitecustomize.py").write_text("INJECTED = True\n", encoding="utf-8")
    poisoned = {
        "PYTHONPATH": str(external),
        "PYTHONHOME": str(external),
        "PYTHONUSERBASE": str(external),
        "DYLD_INSERT_LIBRARIES": str(external / "inject.dylib"),
        "LD_PRELOAD": str(external / "inject.so"),
        "RQUANT_ALLOWED": "kept",
    }
    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    with pytest.raises(FormalRuntimeError, match="ABI"):
        bind_formal_runtime(
            capability,
            daemon_argv=("lab-worker",),
            environment_source=poisoned,
            expected_python_abi="wrong-abi",
        )
    session = bind_formal_runtime(
        capability,
        daemon_argv=("lab-worker",),
        environment_source=poisoned,
        expected_python_abi="test-abi",
    )
    try:
        plan = session.plan
        assert plan.argv[1:3] == ("-I", "-S")
        assert plan.environment == {"RQUANT_ALLOWED": "kept"}
        assert plan.interpreter == session.capability.loaded.generation_root / "release/bin/python"
        assert plan.working_directory == session.capability.release_root
        assert plan.import_roots == (session.capability.release_root / "src",)
        assert all(not path.is_relative_to(external) for path in plan.import_roots)
        launched: list[tuple[str, ...]] = []
        monkeypatch.setattr(os, "chdir", lambda _path: None)
        exec_formal_runtime(
            session,
            executor=lambda _executable, argv, _environment: launched.append(argv),
        )
        assert launched == [plan.argv]
    finally:
        session.close()


def test_p0_12_target_import_and_subprocess_are_after_all_trust_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_runtime, exec_formal_runtime
    from rquant.lab_daemon import require_lab_runtime_binding
    from rquant.lab_finalizer import LabFinalizer
    from rquant.research_manifest import ResearchManifest, require_formal_research_manifest
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
        open_test_capability,
    )

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)

    def forbid_subprocess(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("Git or another subprocess ran before target execution")

    monkeypatch.setattr(subprocess, "run", forbid_subprocess)
    monkeypatch.setattr(subprocess, "Popen", forbid_subprocess)
    assert "rquant.app" not in sys.modules
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    evidence = require_lab_runtime_binding(capability)
    manifest = ResearchManifest(
        schema_version=3,
        research_status="comparable",
        status_reason="immutable generation verified",
        code_trust_evidence=evidence,
        dataset_snapshot_id="snapshot-a",
        dataset_binding_hash="3" * 64,
        strategy_spec_hash="4" * 64,
        result_hash="5" * 64,
        coverage_numerator=100,
        coverage_denominator=100,
        data_start_date=date(2025, 1, 1),
        data_end_date=date(2026, 1, 1),
        universe_definition="formal-universe-v1",
        execution_model_version="execution-v1",
        cost_model_version="cost-v1",
    )
    assert require_formal_research_manifest(manifest, capability=capability) == manifest

    captured: dict[str, object] = {}

    def fake_finalizer_init(self: object, **values: object) -> None:
        captured.update(values)

    monkeypatch.setattr(LabFinalizer, "__init__", fake_finalizer_init)
    formal_finalizer = LabFinalizer.for_formal_runtime(
        runtime_capability=capability,
        reader=object(),  # type: ignore[arg-type]
        shard_artifact_root=tmp_path,
        artifact_store=object(),  # type: ignore[arg-type]
        commit_spool=object(),  # type: ignore[arg-type]
        finalizer_authority_key_provider=lambda: object(),  # type: ignore[arg-type]
    )
    assert formal_finalizer is not None
    provider = captured["verified_code_sha_provider"]
    assert callable(provider) and provider() == evidence.provenance_commit

    session = bind_formal_runtime(
        capability,
        daemon_argv=("lab-finalizer",),
        environment_source={"RQUANT_ALLOWED": "yes"},
        expected_python_abi="test-abi",
    )
    expected_events = (
        "current-verified",
        "attestation-verified",
        "promotion-current-verified",
        "bundle-tree-verified",
        "execution-binding-verified",
    )
    assert session.plan.audit.events == expected_events
    audit = list(expected_events)
    monkeypatch.setattr(os, "chdir", lambda _path: None)

    def target(_executable: str, _argv: tuple[str, ...], _environment: object) -> None:
        audit.append("target-exec")

    exec_formal_runtime(session, executor=target)
    assert audit == [*expected_events, "target-exec"]
    assert "rquant.app" not in sys.modules
    session.close()


def test_missing_invalid_or_tampered_evidence_blocks_before_target_execution(
    tmp_path: Path,
) -> None:
    from rquant.runtime_code_generation import (
        RuntimeCodeGenerationError,
        open_attested_runtime_generation,
    )
    from tests.runtime_code_e2e_support import (
        NOW,
        build_test_package,
        install_test_package,
    )

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    (runtime_root / "current").unlink()
    target_started = False
    with pytest.raises(RuntimeCodeGenerationError):
        open_attested_runtime_generation(
            runtime_root=runtime_root,
            trusted_base=trusted_base,
            root_keyring=package.root_keyring,
            runtime_keyring=package.runtime_keyring,
            promotion_trust=package.promotion_trust,
            expected_uid=os.getuid(),
            expected_gid=os.getgid(),
            expected_audience="formal-lab",
            expected_installation_id="installation-a",
            expected_target_platform="test-platform",
            now=NOW,
        )
    assert not target_started
