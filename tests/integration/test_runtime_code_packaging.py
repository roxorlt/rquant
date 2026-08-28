from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from tests.runtime_code_e2e_support import (
    build_test_package,
    install_test_package,
    open_test_capability,
)


def test_p0_11_normal_checkout_and_linked_worktree_inputs_are_attested_but_not_runtime_roots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.formal_runtime import bind_formal_runtime, exec_formal_runtime
    from rquant.lab_daemon import LabDaemonConfigurationError, require_lab_runtime_binding
    from rquant.runtime_code_generation import RuntimeCodeCollectFile, collect_runtime_code_bundle

    normal = tmp_path / "normal"
    linked = tmp_path / "linked"
    normal.mkdir()
    subprocess.run(("git", "init", "-q"), cwd=normal, check=True)
    source = normal / "src/rquant/app.py"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"VALUE = 1\n")
    subprocess.run(("git", "add", "src/rquant/app.py"), cwd=normal, check=True)
    subprocess.run(
        (
            "git",
            "-c",
            "user.name=rquant-test",
            "-c",
            "user.email=rquant@example.invalid",
            "commit",
            "-qm",
            "runtime input",
        ),
        cwd=normal,
        check=True,
    )
    commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        cwd=normal,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ("git", "worktree", "add", "--detach", "-q", str(linked), "HEAD"),
        cwd=normal,
        check=True,
    )
    files = (
        RuntimeCodeCollectFile(
            source_path="src/rquant/app.py",
            bundle_path="release/src/rquant/app.py",
            mode=0o444,
        ),
    )
    normal_bundle = collect_runtime_code_bundle(
        normal,
        files,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    linked_bundle = collect_runtime_code_bundle(
        linked,
        files,
        expected_uid=os.getuid(),
        expected_gid=os.getgid(),
    )
    assert normal_bundle.content_root_sha256 == linked_bundle.content_root_sha256

    normal_package = build_test_package(
        tmp_path / "normal-package",
        source=source.read_bytes(),
        provenance_commit=commit,
    )
    linked_package = build_test_package(
        tmp_path / "linked-package",
        source=(linked / "src/rquant/app.py").read_bytes(),
        provenance_commit=commit,
    )
    normal_base, normal_runtime, _normal_installer = install_test_package(
        tmp_path / "normal-install",
        normal_package,
    )
    linked_base, linked_runtime, _linked_installer = install_test_package(
        tmp_path / "linked-install",
        linked_package,
    )
    normal.joinpath(".git").rename(normal / ".git-hidden")
    linked.joinpath(".git").rename(linked / ".git-hidden")

    with pytest.raises(LabDaemonConfigurationError, match="capability"):
        require_lab_runtime_binding(normal)
    with pytest.raises(LabDaemonConfigurationError, match="capability"):
        require_lab_runtime_binding(linked)

    monkeypatch.setattr(os, "chdir", lambda _path: None)
    launched: list[str] = []
    for trusted_base, runtime_root, package in (
        (normal_base, normal_runtime, normal_package),
        (linked_base, linked_runtime, linked_package),
    ):
        capability = open_test_capability(
            trusted_base=trusted_base,
            runtime_root=runtime_root,
            package=package,
        )
        session = bind_formal_runtime(
            capability,
            daemon_argv=("lab-worker",),
            environment_source={"RQUANT_ALLOWED": "yes"},
            expected_python_abi="test-abi",
        )
        expected_commit = session.plan.evidence.provenance_commit
        exec_formal_runtime(
            session,
            executor=lambda _executable, _argv, _environment, commit=expected_commit: (
                launched.append(commit)
            ),
        )
        session.close()
    assert launched == [commit, commit]
