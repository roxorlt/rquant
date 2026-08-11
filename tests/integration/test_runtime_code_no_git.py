from __future__ import annotations

import inspect
import os
import subprocess
from pathlib import Path

import pytest

from tests.runtime_code_e2e_support import (
    build_test_package,
    install_test_package,
    open_test_capability,
)


def test_p0_01_git_index_and_objects_alternate_cannot_change_formal_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_runtime

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    checkout = tmp_path / "checkout"
    metadata = checkout / ".git"
    alternate = tmp_path / "alternate" / ".git"
    metadata.mkdir(parents=True)
    alternate.joinpath("objects").mkdir(parents=True)
    alternate.joinpath("index").write_bytes(b"alternate-index")
    metadata.joinpath("index").symlink_to(alternate / "index")
    metadata.joinpath("objects").symlink_to(alternate / "objects")
    expected = package.receipt.generation_id
    opened_paths: list[str] = []
    original_open = os.open

    def audited_open(path: object, *args: object, **kwargs: object) -> int:
        opened_paths.append(
            os.fsdecode(path) if isinstance(path, (str, bytes, os.PathLike)) else ""
        )
        return original_open(path, *args, **kwargs)  # type: ignore[arg-type]

    def forbid_subprocess(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("formal runtime attempted to spawn a subprocess")

    monkeypatch.setattr(os, "open", audited_open)
    monkeypatch.setattr(subprocess, "run", forbid_subprocess)
    monkeypatch.setattr(subprocess, "Popen", forbid_subprocess)
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
    try:
        assert session.require_live().generation_id == expected
        assert not any("/.git" in path or path.endswith(".git") for path in opened_paths)
    finally:
        session.close()


def test_p0_02_all_known_and_unknown_git_environment_variables_are_ignored(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_runtime

    package = build_test_package(tmp_path / "package")
    trusted_base, runtime_root, _installer = install_test_package(tmp_path, package)
    poisoned = {
        "GIT_DIR": "/alternate/repo.git",
        "GIT_WORK_TREE": "/alternate/worktree",
        "GIT_INDEX_FILE": "/alternate/index",
        "GIT_OBJECT_DIRECTORY": "/alternate/objects",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES": "/alternate/other-objects",
        "GIT_COMMON_DIR": "/alternate/common",
        "GIT_CONFIG_GLOBAL": "/alternate/config",
        "GIT_CONFIG_SYSTEM": "/alternate/system-config",
        "GIT_CONFIG_NOSYSTEM": "0",
        "GIT_CEILING_DIRECTORIES": "/",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM": "1",
        "GIT_EXEC_PATH": "/alternate/exec",
        "GIT_REPLACE_REF_BASE": "refs/replace/poison",
        "GIT_FUTURE_ROUTER": "/alternate/future",
        "PYTHONPATH": "/alternate/python",
        "PYTHONUSERBASE": "/alternate/user-site",
        "DYLD_LIBRARY_PATH": "/alternate/dylib",
        "LD_LIBRARY_PATH": "/alternate/loader",
        "RQUANT_ALLOWED": "kept",
        "UNAPPROVED_SECRET": "dropped",
    }
    for name, value in poisoned.items():
        monkeypatch.setenv(name, value)
    capability = open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )
    session = bind_formal_runtime(
        capability,
        daemon_argv=("lab-worker",),
        environment_source=os.environ,
        expected_python_abi="test-abi",
    )
    try:
        assert session.plan.environment == {"RQUANT_ALLOWED": "kept"}
        assert session.require_live().generation_id == package.receipt.generation_id
    finally:
        session.close()


def test_formal_runtime_static_call_graph_has_no_git_dependencies() -> None:
    from rquant.formal_runtime import (
        FormalRuntimeCodeAuthority,
        bind_formal_runtime,
        exec_formal_runtime,
    )
    from rquant.lab_daemon import require_lab_runtime_binding
    from rquant.lab_finalizer import LabFinalizer
    from rquant.research_manifest import require_formal_research_manifest

    formal_consumers = (
        bind_formal_runtime,
        exec_formal_runtime,
        FormalRuntimeCodeAuthority,
        require_lab_runtime_binding,
        require_formal_research_manifest,
        LabFinalizer.for_formal_runtime,
    )
    forbidden = (
        "subprocess",
        "rev-parse",
        "git archive",
        "detect_verified_code_commit",
        "_run_trusted_git",
        '".git"',
        "'.git'",
    )
    for consumer in formal_consumers:
        source = inspect.getsource(consumer).casefold()
        assert not any(token in source for token in forbidden), consumer
