from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest


def _policy(root: Path, target: Path, *, sha256: str | None = None):
    from rquant.interpreter_trust import InterpreterTrustPolicy

    return InterpreterTrustPolicy(
        profile="test",
        canonical_interpreter=target,
        trusted_anchor=root,
        owner_uid=os.getuid(),
        allowed_mode=0o700,
        sha256=sha256,
    )


def _interpreter(root: Path, name: str = "python") -> Path:
    target = root / name
    target.write_bytes(b"#!/bin/sh\nexit 0\n")
    target.chmod(0o700)
    return target


def test_policy_requires_one_explicit_owner_and_exact_mode(tmp_path: Path) -> None:
    from rquant.interpreter_trust import InterpreterTrustPolicy

    target = _interpreter(tmp_path)

    with pytest.raises(ValueError, match="owner UID"):
        InterpreterTrustPolicy(
            profile="test",
            canonical_interpreter=target,
            trusted_anchor=tmp_path,
            owner_uid={0, os.getuid()},
            allowed_mode=0o700,
        )
    with pytest.raises(ValueError, match="mode"):
        InterpreterTrustPolicy(
            profile="test",
            canonical_interpreter=target,
            trusted_anchor=tmp_path,
            owner_uid=os.getuid(),
            allowed_mode={0o700, 0o755},
        )


@pytest.mark.parametrize("unsafe", ("symlink", "group-writable", "wrong-owner"))
def test_bind_rejects_unsafe_ancestor(tmp_path: Path, unsafe: str) -> None:
    from rquant.interpreter_trust import InterpreterTrustError, bind_interpreter

    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    parent = root / "bin"
    parent.mkdir(mode=0o700)
    target = _interpreter(parent)
    policy = _policy(root, target)
    if unsafe == "symlink":
        parent.rename(root / "real-bin")
        parent.symlink_to("real-bin", target_is_directory=True)
    elif unsafe == "group-writable":
        parent.chmod(0o770)
    else:
        policy = replace(policy, owner_uid=os.getuid() + 1)

    with pytest.raises(InterpreterTrustError, match="unsafe"):
        bind_interpreter(policy)


def test_attestation_and_preexec_reject_target_replacement_and_close_fd(tmp_path: Path) -> None:
    from rquant.interpreter_trust import (
        InterpreterTrustError,
        InterpreterTrustState,
        bind_interpreter,
    )

    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    target = _interpreter(root)
    policy = _policy(root, target, sha256=hashlib.sha256(target.read_bytes()).hexdigest())
    binding = bind_interpreter(policy)
    descriptor = binding.descriptor
    binding.attest()
    replacement = root / "replacement"
    replacement.write_bytes(b"#!/bin/sh\nexit 1\n")
    replacement.chmod(0o700)
    replacement.replace(target)

    with pytest.raises(InterpreterTrustError, match="identity changed"):
        binding.prepare_exec()

    assert binding.state is InterpreterTrustState.REJECTED
    with pytest.raises(OSError):
        os.fstat(descriptor)


def test_attestation_rejects_fd_hash_mismatch(tmp_path: Path) -> None:
    from rquant.interpreter_trust import InterpreterTrustError, bind_interpreter

    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    target = _interpreter(root)
    binding = bind_interpreter(_policy(root, target, sha256="0" * 64))

    with pytest.raises(InterpreterTrustError, match="SHA256"):
        binding.attest()

    assert binding.closed


@pytest.mark.parametrize("replace_parent", (False, True))
def test_contained_launch_executes_attested_descriptor_after_path_replacement(
    tmp_path: Path,
    replace_parent: bool,
) -> None:
    from rquant.contained_subprocess import ContainedProcessError, run_contained
    from rquant.fd_exec import descriptor_execution_supported
    from rquant.interpreter_trust import bind_interpreter

    root = tmp_path / "root"
    root.mkdir(mode=0o700)
    target = root / "python"
    shutil.copy2(Path(sys.executable).resolve(strict=True), target)
    target.chmod(0o700)
    marker = tmp_path / "executed"
    replacement_root = tmp_path / "replacement-root"
    replacement_root.mkdir(mode=0o700)
    replacement = replacement_root / "python"
    replacement.write_text(
        f"#!/bin/sh\nprintf replacement > {marker!s}\n",
        encoding="utf-8",
    )
    replacement.chmod(0o700)
    binding = bind_interpreter(
        _policy(root, target, sha256=hashlib.sha256(target.read_bytes()).hexdigest())
    )
    binding.attest()
    command = [
        str(target),
        "-I",
        "-S",
        "-c",
        f"from pathlib import Path; Path({str(marker)!r}).write_text('trusted')",
    ]

    def launch(arguments: tuple[str, ...], **kwargs: object) -> subprocess.CompletedProcess[str]:
        if replace_parent:
            root.rename(tmp_path / "replaced-root")
            replacement_root.rename(root)
        else:
            replacement.replace(target)
        return run_contained(
            arguments,
            cwd=root,
            deadline_monotonic=time.monotonic() + 10,
            may_spawn_background_descendants=False,
            **kwargs,
        )

    try:
        if descriptor_execution_supported():
            result = binding.launch(launch, tuple(command))
            assert isinstance(result, subprocess.CompletedProcess)
            assert result.returncode == 0, result.stderr
            assert marker.read_text(encoding="utf-8") == "trusted"
        else:
            with pytest.raises(ContainedProcessError, match="descriptor execution"):
                binding.launch(launch, tuple(command))
            assert not marker.exists()
    finally:
        binding.close()
