"""Complete input evidence uses the original reader over real private files."""

from __future__ import annotations

import hashlib
import errno
import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import FrameType
from typing import Any

import pytest

from rquant import minute_backtest_parameter_producer as owner


def _capabilities() -> tuple[type[Any], Callable[..., Any], Callable[..., None]]:
    path_type = getattr(owner, "_MinuteStudyInputPath", None)
    capture = getattr(owner, "_capture_minute_study_input_files", None)
    verify = getattr(owner, "_verify_minute_study_input_files", None)
    assert path_type is not None and callable(capture) and callable(verify), (
        "complete typed input-file capture and verification capability is unavailable"
    )
    return path_type, capture, verify


def _write(path: Path, value: bytes) -> Path:
    path.write_bytes(value)
    path.chmod(0o600)
    return path


def _assert_no_payload(value: object) -> None:
    assert not isinstance(value, (bytes, bytearray, memoryview))
    if isinstance(value, owner.MinuteReplayModel):
        _assert_no_payload(value.__dict__)
        _assert_no_payload(value.__pydantic_private__)
        _assert_no_payload(value.__pydantic_extra__)
    elif isinstance(value, dict):
        for child in value.values():
            _assert_no_payload(child)
    elif isinstance(value, (tuple, list)):
        for child in value:
            _assert_no_payload(child)


def test_complete_input_file_proof_uses_full_bytes_and_every_identity_field(tmp_path: Path) -> None:
    content = bytes(range(256)) * 257 + b"complete-final-input-tail"
    path = _write(tmp_path / "input.bin", content)
    path_type, capture, verify = _capabilities()
    paths = (path_type(role="prepared_source", path=path),)
    with _observe_original_reader() as captured:
        evidence = capture(paths)
    assert captured.paths == [path]
    assert type(evidence) is tuple and len(evidence) == 1
    item = evidence[0]
    assert type(item).__name__ == "_MinuteStudyInputFileEvidence"
    before = path.stat(follow_symlinks=False)
    expected = {
        "role": "prepared_source", "path": path,
        "device": before.st_dev, "inode": before.st_ino, "owner_uid": before.st_uid,
        "mode": before.st_mode, "link_count": before.st_nlink, "size_bytes": before.st_size,
        "mtime_ns": before.st_mtime_ns, "ctime_ns": before.st_ctime_ns,
        "content_sha256": hashlib.sha256(content).hexdigest(),
    }
    assert {key: getattr(item, key) for key in expected} == expected
    parent_paths = tuple(reversed(path.parents))
    assert tuple(parent.path for parent in item.parents) == parent_paths
    for parent, parent_path in zip(item.parents, parent_paths, strict=True):
        actual = parent_path.stat(follow_symlinks=False)
        assert parent.model_dump() == {
            "path": parent_path, "device": actual.st_dev, "inode": actual.st_ino,
            "owner_uid": actual.st_uid, "mode": actual.st_mode,
        }
    assert set(item.model_dump()) == {*expected, "parents"}
    _assert_no_payload(item)
    with _observe_original_reader() as verified:
        assert verify(paths, expected=evidence) is None
    assert verified.paths == [path]


@dataclass
class _ReadObservation:
    paths: list[Path] = field(default_factory=list)
    handles: set[int] = field(default_factory=set)
    acted: bool = False


@contextmanager
def _observe_original_reader(
    action_after_chunk: Callable[[], None] | None = None,
) -> Iterator[_ReadObservation]:
    observed = _ReadObservation()
    code = owner._resolved_read_file.__code__
    previous = sys.gettrace()

    def trace(frame: FrameType, event: str, arg: object) -> Any:
        if frame.f_code is not code:
            return previous(frame, event, arg) if previous is not None else None
        if event == "call":
            observed.paths.append(frame.f_locals["path"])
        if event == "line":
            values = frame.f_locals
            for name in ("fd", "directory", "child"):
                descriptor = values.get(name)
                if type(descriptor) is int and descriptor >= 0:
                    observed.handles.add(descriptor)
            for _, descriptor, _ in values.get("opened", ()):
                observed.handles.add(descriptor)
            if action_after_chunk is not None and not observed.acted and values.get("chunk"):
                observed.acted = True
                action_after_chunk()
        if previous is not None:
            previous(frame, event, arg)
        return trace

    sys.settrace(trace)
    try:
        yield observed
    finally:
        sys.settrace(previous)
        assert sys.gettrace() is previous
        for descriptor in observed.handles:
            with pytest.raises(OSError) as closed:
                os.fstat(descriptor)
            assert closed.value.errno == errno.EBADF


def _two_files(tmp_path: Path) -> tuple[tuple[Any, ...], tuple[Any, ...]]:
    path_type, capture, _ = _capabilities()
    paths = (
        path_type(role="prepared_source", path=_write(tmp_path / "source.bin", b"source-input")),
        path_type(role="prepared_receipt", path=_write(tmp_path / "receipt.bin", b"receipt-input")),
    )
    return paths, capture(paths)


@pytest.mark.parametrize("change", ("missing", "extra", "role", "order", "path"))
def test_complete_ordered_role_path_list_is_checked_before_any_read(
    tmp_path: Path, change: str,
) -> None:
    path_type, _, verify = _capabilities()
    paths, expected = _two_files(tmp_path)
    if change == "missing":
        changed = paths[:-1]
    elif change == "extra":
        changed = (*paths, path_type(role="prepared_manifest", path=tmp_path / "absent-manifest"))
    elif change == "role":
        changed = (path_type(role="prepared_metadata", path=paths[0].path), paths[1])
    elif change == "order":
        changed = tuple(reversed(paths))
    else:
        changed = (path_type(role=paths[0].role, path=tmp_path / "absent-source"), paths[1])
    with _observe_original_reader() as observed:
        with pytest.raises(PermissionError):
            verify(changed, expected=expected)
    assert observed.paths == []


def test_same_path_keeps_every_distinct_role_and_rereads_each_item(tmp_path: Path) -> None:
    path_type, capture, verify = _capabilities()
    path = _write(tmp_path / "shared.bin", b"shared-input")
    roles = (
        "prepared_source", "prepared_receipt", "prepared_metadata", "prepared_manifest",
        "prepared_artifact", "baseline_source", "baseline_receipt", "baseline_metadata",
        "baseline_manifest", "baseline_artifact",
    )
    paths = tuple(path_type(role=role, path=path) for role in roles)
    with _observe_original_reader() as observed:
        evidence = capture(paths)
        assert verify(paths, expected=evidence) is None
    assert observed.paths == [path] * (2 * len(roles))
    assert tuple(item.role for item in evidence) == roles
    assert tuple(item.path for item in evidence) == (path,) * len(roles)
    physical = [item.model_dump(exclude={"role"}) for item in evidence]
    assert all(item == physical[0] for item in physical)
    for item in evidence:
        _assert_no_payload(item)


@pytest.mark.parametrize(
    "field_name",
    ("device", "inode", "owner_uid", "mode", "link_count", "size_bytes", "mtime_ns",
     "ctime_ns", "content_sha256", "parents"),
)
def test_verification_compares_each_stored_physical_field(tmp_path: Path, field_name: str) -> None:
    _, _, verify = _capabilities()
    paths, expected = _two_files(tmp_path)
    item = expected[0]
    if field_name == "content_sha256":
        changed_value = "0" * 64 if item.content_sha256 != "0" * 64 else "1" * 64
    elif field_name == "parents":
        parent = item.parents[-1]
        changed_value = (*item.parents[:-1], parent.model_copy(update={"mode": parent.mode ^ 0o100}))
    else:
        changed_value = getattr(item, field_name) + 1
    altered = (item.model_copy(update={field_name: changed_value}), *expected[1:])
    with _observe_original_reader() as observed:
        with pytest.raises(PermissionError):
            verify(paths, expected=altered)
    assert observed.paths


@pytest.mark.parametrize(
    "change", ("bytes", "same_bytes_new_inode", "safe_mode", "unsafe_mode", "hardlink", "symlink"),
)
def test_current_file_changes_reject_original_evidence(tmp_path: Path, change: str) -> None:
    path_type, capture, verify = _capabilities()
    content = b"original-input-body"
    path = _write(tmp_path / "input.bin", content)
    paths = (path_type(role="prepared_source", path=path),)
    expected = capture(paths)
    if change == "bytes":
        path.write_bytes(b"X" + content[1:])
    elif change == "same_bytes_new_inode":
        replacement = _write(tmp_path / "replacement.bin", content)
        assert replacement.stat().st_ino != expected[0].inode
        os.replace(replacement, path)
    elif change == "safe_mode":
        path.chmod(0o400)
    elif change == "unsafe_mode":
        path.chmod(0o622)
    elif change == "hardlink":
        os.link(path, tmp_path / "alias.bin")
    else:
        target = _write(tmp_path / "target.bin", content)
        path.unlink()
        path.symlink_to(target)
    with _observe_original_reader() as observed:
        with pytest.raises(OSError):
            verify(paths, expected=expected)
    assert observed.paths == [path]


def test_parent_directory_mode_change_rejects_with_file_identity_unchanged(tmp_path: Path) -> None:
    path_type, capture, verify = _capabilities()
    parent = tmp_path / "private-parent"
    parent.mkdir(mode=0o700)
    path = _write(parent / "input.bin", b"directory-input")
    paths = (path_type(role="prepared_source", path=path),)
    expected = capture(paths)
    before = path.stat()
    parent.chmod(0o500)
    after = path.stat()
    assert (before.st_dev, before.st_ino, before.st_mode, before.st_nlink,
            before.st_size, before.st_mtime_ns, before.st_ctime_ns) == (
        after.st_dev, after.st_ino, after.st_mode, after.st_nlink,
        after.st_size, after.st_mtime_ns, after.st_ctime_ns,
    )
    try:
        with _observe_original_reader() as observed:
            with pytest.raises(PermissionError):
                verify(paths, expected=expected)
        assert observed.paths == [path]
    finally:
        parent.chmod(0o700)


@pytest.mark.parametrize("target", ("file", "parent"))
def test_replacement_after_real_chunk_rejects_and_closes_all_fds(
    tmp_path: Path, target: str,
) -> None:
    path_type, capture, _ = _capabilities()
    parent = tmp_path / "private-parent"
    parent.mkdir(mode=0o700)
    content = b"R" * 70_000
    path = _write(parent / "input.bin", content)
    paths = (path_type(role="prepared_source", path=path),)

    def replace_after_chunk() -> None:
        if target == "file":
            replacement = _write(parent / "replacement.bin", content)
            os.replace(replacement, path)
        else:
            parent.rename(tmp_path / "retained-old-parent")
            parent.mkdir(mode=0o700)
            _write(path, content)

    with _observe_original_reader(replace_after_chunk) as observed:
        with pytest.raises(PermissionError, match="changed during"):
            capture(paths)
    assert observed.acted
    assert observed.paths == [path]
    assert observed.handles


def test_original_max_input_bytes_is_accepted_with_full_digest(tmp_path: Path) -> None:
    path_type, capture, verify = _capabilities()
    path = tmp_path / "at-original-limit.bin"
    with path.open("wb") as stream:
        stream.truncate(owner.MAX_INPUT_BYTES)
    path.chmod(0o600)
    paths = (path_type(role="prepared_source", path=path),)
    with _observe_original_reader() as observed:
        evidence = capture(paths)
        assert verify(paths, expected=evidence) is None
    assert evidence[0].size_bytes == owner.MAX_INPUT_BYTES == 16_777_216
    assert evidence[0].content_sha256 == hashlib.sha256(b"\0" * owner.MAX_INPUT_BYTES).hexdigest()
    assert observed.paths == [path, path]
    _assert_no_payload(evidence[0])


@pytest.mark.parametrize("size_bytes", (0, 16_777_217), ids=("empty", "over-original-limit"))
def test_original_file_size_failures_close_opened_fds(tmp_path: Path, size_bytes: int) -> None:
    path_type, capture, _ = _capabilities()
    path = tmp_path / "invalid-size.bin"
    with path.open("wb") as stream:
        stream.truncate(size_bytes)
    path.chmod(0o600)
    paths = (path_type(role="prepared_source", path=path),)
    with _observe_original_reader() as observed:
        with pytest.raises(PermissionError, match="bounded file"):
            capture(paths)
    assert observed.handles


def test_exception_after_real_read_closes_every_directory_and_file_fd(tmp_path: Path) -> None:
    path_type, capture, _ = _capabilities()
    path = _write(tmp_path / "input.bin", b"exception-input")
    paths = (path_type(role="prepared_source", path=path),)

    def fail_after_chunk() -> None:
        raise OSError(errno.EIO, "synthetic-after-real-read")

    with _observe_original_reader(fail_after_chunk) as observed:
        with pytest.raises(OSError, match="synthetic-after-real-read"):
            capture(paths)
    assert observed.acted
    assert observed.handles
    assert capture(paths)[0].path == path


@pytest.mark.parametrize("kind", ("relative", "dotdot", "ancestor_symlink"))
def test_noncanonical_or_symlinked_input_paths_reject(tmp_path: Path, kind: str) -> None:
    path_type, capture, _ = _capabilities()
    parent = tmp_path / "real-parent"
    parent.mkdir(mode=0o700)
    path = _write(parent / "input.bin", b"path-input")
    if kind == "relative":
        changed = Path("relative-input.bin")
    elif kind == "dotdot":
        changed = parent / ".." / parent.name / path.name
    else:
        alias = tmp_path / "parent-alias"
        alias.symlink_to(parent, target_is_directory=True)
        changed = alias / path.name
    paths = (path_type(role="prepared_source", path=changed),)
    with _observe_original_reader():
        with pytest.raises(OSError):
            capture(paths)


def test_directory_cannot_be_captured_as_input_file(tmp_path: Path) -> None:
    path_type, capture, _ = _capabilities()
    paths = (path_type(role="prepared_source", path=tmp_path),)
    with _observe_original_reader() as observed:
        with pytest.raises(PermissionError, match="bounded file"):
            capture(paths)
    assert observed.handles
