"""#193: the two `os.open()` flag defects on the TP1 publish path.

G-2 — `ldd` prints the path it was given, and on RHEL 9 a shared library's last segment is
routinely a versioned symlink (`libcrypt.so.1 -> libcrypt.so.1.1.0`). `O_NOFOLLOW` constrains
exactly that last segment, so staging a closure member under the printed name fails closed
with `ELOOP` in a window where the operator cannot legally work around it. The stage now
resolves closure members to their real paths, and `O_NOFOLLOW` stays: a profile that names
the real file is also what the wrapper can open later.

N-6 — the staging tree is `lighthouse`-writable. A planned member replaced by a FIFO used to
block `open()` forever on the read side, after `publish` had taken `deployment.lock`, because
`O_NONBLOCK` was missing and `S_ISREG` is checked on the descriptor. `O_NONBLOCK` is a no-op
for a regular file and makes the FIFO return immediately, so the existing `S_ISREG` check is
reached and refuses it.
"""

from __future__ import annotations

import hashlib
import os
import threading
from collections.abc import Callable
from pathlib import Path

import pytest

from rquant.runtime_authority_publish import (
    RuntimeAuthorityPublishError,
    RuntimeAuthorityStageError,
    _copy_new_file,
    _read_staged,
    sha256_file,
)
from rquant.runtime_authority_stage import file_policy, shared_libraries_from_ldd

BOUND_SECONDS = 10.0
LOADER = "/usr/lib64/ld-linux-x86-64.so.2"


def _call_within(seconds: float, function: Callable[[], object]) -> BaseException | None:
    """Run `function` on a thread and insist it finishes; a blocked `open()` never does."""

    outcome: list[BaseException | None] = []

    def target() -> None:
        try:
            function()
        except BaseException as exc:  # noqa: BLE001 - the exception is the assertion
            outcome.append(exc)
        else:
            outcome.append(None)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    assert not thread.is_alive(), f"the call blocked for more than {seconds}s"
    return outcome[0]


# ---------------------------------------------------------------------------------------
# G-2: the closure members `ldd` prints
# ---------------------------------------------------------------------------------------


def _versioned_library(root: Path, name: str, version: str, payload: bytes) -> tuple[Path, Path]:
    """The RHEL 9 shape: a real `libfoo.so.1.2.3` and the `libfoo.so.1` symlink beside it."""

    real = root / f"{name}.{version}"
    real.write_bytes(payload)
    link = root / name
    link.symlink_to(real.name)
    return link, real


def test_a_versioned_library_symlink_is_staged_as_its_real_file(tmp_path: Path) -> None:
    link, real = _versioned_library(tmp_path, "libcrypt.so.1", "1.0", b"crypt")

    resolved = shared_libraries_from_ldd(
        f"\tlibcrypt.so.1 => {link} (0x00007f0000000000)\n", elf_loader=LOADER
    )

    assert resolved == (str(real),)
    assert link.is_symlink()


def test_o_nofollow_still_refuses_the_symlink_the_resolution_removed(tmp_path: Path) -> None:
    """The fix is the resolution, not a relaxation: the printed name is still unopenable."""

    link, real = _versioned_library(tmp_path, "libz.so.1", "1.2.13", b"zlib")

    with pytest.raises(OSError) as failure:
        file_policy(link)

    assert failure.value.errno in {getattr(os, "ELOOP", 62), 40, 62}, failure.value
    policy = file_policy(real)
    assert policy.path == real
    assert policy.sha256 == hashlib.sha256(b"zlib").hexdigest()


def test_the_loader_is_dropped_however_ldd_spells_it(tmp_path: Path) -> None:
    """The dedup rule (B-5a / M-4) has to hold after resolution, or the closure gets the
    loader twice under two names and the profile carries a path the wrapper cannot open."""

    real_loader = tmp_path / "ld-2.34.so"
    real_loader.write_bytes(b"loader")
    printed = tmp_path / "ld-linux-x86-64.so.2"
    printed.symlink_to(real_loader.name)
    link, real = _versioned_library(tmp_path, "libm.so.6", "2.34", b"libm")

    resolved = shared_libraries_from_ldd(
        f"\t{printed.name} => {printed} (0x1)\n\tlibm.so.6 => {link} (0x2)\n",
        elf_loader=str(printed),
    )

    assert resolved == (str(real),)


def test_a_regular_library_path_is_left_exactly_as_printed(tmp_path: Path) -> None:
    plain = tmp_path / "libc.so.6"
    plain.write_bytes(b"libc")

    assert shared_libraries_from_ldd(f"\tlibc.so.6 => {plain} (0x1)\n", elf_loader=LOADER) == (
        str(plain),
    )
    assert shared_libraries_from_ldd("\tlinux-vdso.so.1 (0x1)\n", elf_loader=LOADER) == ()


# ---------------------------------------------------------------------------------------
# N-6: a FIFO where a planned regular file was
# ---------------------------------------------------------------------------------------


def test_hashing_a_fifo_is_refused_in_bounded_time(tmp_path: Path) -> None:
    fifo = tmp_path / "member.py"
    os.mkfifo(fifo)

    failure = _call_within(BOUND_SECONDS, lambda: sha256_file(fifo))

    assert isinstance(failure, RuntimeAuthorityStageError), failure
    assert "not a regular file" in str(failure)


def test_copying_a_fifo_is_refused_in_bounded_time(tmp_path: Path) -> None:
    fifo = tmp_path / "member.py"
    os.mkfifo(fifo)
    destination = tmp_path / "copied.py"

    failure = _call_within(BOUND_SECONDS, lambda: _copy_new_file(fifo, destination))

    assert isinstance(failure, RuntimeAuthorityStageError), failure
    assert "not a regular file" in str(failure)
    assert not destination.exists()


def test_reading_a_staged_fifo_is_refused_in_bounded_time(tmp_path: Path) -> None:
    os.mkfifo(tmp_path / "plan.json")

    failure = _call_within(
        BOUND_SECONDS, lambda: _read_staged(tmp_path, "plan.json", max_bytes=4096)
    )

    assert isinstance(failure, RuntimeAuthorityPublishError), failure
    assert "plan.json" in str(failure)


def test_regular_files_still_hash_copy_and_read_unchanged(tmp_path: Path) -> None:
    """`O_NONBLOCK` is a no-op here: the same digest, the same bytes, the same size."""

    payload = b"x" * (3 * 1024 * 1024 + 7)
    source = tmp_path / "member.py"
    source.write_bytes(payload)
    destination = tmp_path / "copied.py"

    digest, size = sha256_file(source)
    copied_digest, copied_size = _copy_new_file(source, destination)

    assert digest == hashlib.sha256(payload).hexdigest()
    assert (size, copied_size) == (len(payload), len(payload))
    assert copied_digest == digest
    assert destination.read_bytes() == payload
    assert _read_staged(tmp_path, "member.py", max_bytes=len(payload)) == payload
