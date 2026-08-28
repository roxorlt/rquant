"""Regression coverage for the fixed-production-runtime absence guard.

The guard must fail closed on every non-``ENOENT-final`` observation: a
symlink anywhere on the walk (platforms report ELOOP/ENOTDIR/ENOENT
inconsistently for ``O_NOFOLLOW``), a missing ancestor, and a root that was
absent when bound but appeared before a side effect.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from rquant.daily_pipeline_control import (
    DailyPipelineProductionProfileError,
    _assert_component_verified_absent,
    _FixedProductionRuntimeBinding,
    assert_daily_dag_dev_allowed,
)


def _patch_runtime_root(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    from rquant import runtime_deployment_profile as deployment_module

    monkeypatch.setattr(deployment_module, "LINUX_PRODUCTION_RUNTIME_ROOT", root)


def test_ancestor_symlink_with_missing_target_final_component_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    missing_target = base / "real-parent"
    parent = base / "fixed-production-parent"
    parent.symlink_to(missing_target, target_is_directory=True)
    _patch_runtime_root(monkeypatch, parent / "runtime")

    with pytest.raises(DailyPipelineProductionProfileError, match="symlink"):
        assert_daily_dag_dev_allowed()

    assert not missing_target.exists()
    assert not (parent / "runtime").exists()


def test_ancestor_symlink_to_existing_directory_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    real_parent = base / "real-parent"
    real_parent.mkdir(mode=0o700)
    parent = base / "fixed-production-parent"
    parent.symlink_to(real_parent, target_is_directory=True)
    _patch_runtime_root(monkeypatch, parent / "runtime")

    with pytest.raises(DailyPipelineProductionProfileError, match="symlink"):
        assert_daily_dag_dev_allowed()

    assert list(real_parent.iterdir()) == []


def test_final_dangling_symlink_is_not_treated_as_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    parent = base / "fixed-production-parent"
    parent.mkdir(mode=0o700)
    (parent / "runtime").symlink_to(parent / "missing-target")
    _patch_runtime_root(monkeypatch, parent / "runtime")

    with pytest.raises(DailyPipelineProductionProfileError, match="symlink"):
        assert_daily_dag_dev_allowed()


def test_missing_ancestor_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    _patch_runtime_root(monkeypatch, base / "absent-parent" / "runtime")

    with pytest.raises(DailyPipelineProductionProfileError, match="ancestor is missing"):
        assert_daily_dag_dev_allowed()

    assert not (base / "absent-parent").exists()


def test_absent_to_present_race_is_rejected_by_revalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    parent = base / "fixed-production-parent"
    parent.mkdir(mode=0o700)
    runtime_root = parent / "runtime"
    _patch_runtime_root(monkeypatch, runtime_root)

    guard = assert_daily_dag_dev_allowed()
    try:
        guard.assert_still_absent()
        runtime_root.mkdir(mode=0o700)
        with pytest.raises(DailyPipelineProductionProfileError, match="appeared"):
            guard.assert_still_absent()
    finally:
        guard.close()


def test_absent_to_present_dangling_symlink_race_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    parent = base / "fixed-production-parent"
    parent.mkdir(mode=0o700)
    runtime_root = parent / "runtime"
    _patch_runtime_root(monkeypatch, runtime_root)

    guard = assert_daily_dag_dev_allowed()
    try:
        runtime_root.symlink_to(parent / "missing-target")
        with pytest.raises(DailyPipelineProductionProfileError, match="appeared"):
            guard.assert_still_absent()
    finally:
        guard.close()


def test_parent_replacement_after_binding_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    parent = base / "fixed-production-parent"
    parent.mkdir(mode=0o700)
    _patch_runtime_root(monkeypatch, parent / "runtime")

    guard = assert_daily_dag_dev_allowed()
    try:
        parent.rmdir()
        parent.mkdir(mode=0o700)
        with pytest.raises(DailyPipelineProductionProfileError, match="replaced|changed"):
            guard.assert_still_absent()
    finally:
        guard.close()


def test_closed_binding_refuses_further_absence_claims(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    parent = base / "fixed-production-parent"
    parent.mkdir(mode=0o700)
    _patch_runtime_root(monkeypatch, parent / "runtime")

    guard = assert_daily_dag_dev_allowed()
    guard.assert_still_absent()
    guard.close()
    with pytest.raises(DailyPipelineProductionProfileError, match="closed"):
        guard.assert_still_absent()


def test_component_open_error_with_present_directory_is_rejected(tmp_path: Path) -> None:
    base = tmp_path.resolve()
    (base / "present").mkdir(mode=0o700)
    dir_fd = os.open(base, os.O_RDONLY)
    try:
        with pytest.raises(DailyPipelineProductionProfileError, match="unsafe or was replaced"):
            _assert_component_verified_absent(
                "present",
                dir_fd=dir_fd,
                open_error=FileNotFoundError(),
                final=True,
            )
        with pytest.raises(DailyPipelineProductionProfileError, match="changed while validating"):
            _assert_component_verified_absent(
                "truly-absent",
                dir_fd=dir_fd,
                open_error=PermissionError(),
                final=True,
            )
        (base / "plain-file").write_bytes(b"x")
        with pytest.raises(DailyPipelineProductionProfileError, match="non-directory"):
            _assert_component_verified_absent(
                "plain-file",
                dir_fd=dir_fd,
                open_error=NotADirectoryError(),
                final=True,
            )
    finally:
        os.close(dir_fd)


def test_binding_walk_pins_parent_identity_for_absent_final(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path.resolve()
    parent = base / "fixed-production-parent"
    parent.mkdir(mode=0o700)
    _patch_runtime_root(monkeypatch, parent / "runtime")

    binding = _FixedProductionRuntimeBinding.open(parent / "runtime")
    try:
        assert binding.final_absent is True
        assert binding.final_name == "runtime"
        pinned = binding.identities[-1]
        observed = os.fstat(pinned.descriptor)
        expected = parent.stat()
        assert (observed.st_dev, observed.st_ino) == (expected.st_dev, expected.st_ino)
        binding.assert_current()
    finally:
        binding.close()
