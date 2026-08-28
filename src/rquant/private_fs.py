"""Small fail-closed filesystem primitives shared by durable authorities."""

from __future__ import annotations

import ctypes
import errno
import os
import sys
from collections.abc import Callable

_RENAME_NOREPLACE_MAX_ATTEMPTS = 8


def rename_noreplace_capability() -> str:
    """Resolve the required platform primitive without mutating the filesystem."""

    _function, _flags, capability = _rename_noreplace_function()
    return capability


def _rename_noreplace_function() -> tuple[ctypes._CFuncPtr, int, str]:
    libc = ctypes.CDLL(None, use_errno=True)
    if sys.platform == "darwin":
        try:
            function = libc.renameatx_np
        except AttributeError as exc:
            raise OSError(errno.ENOTSUP, "renameatx_np is unavailable") from exc
        flags = 0x00000004  # RENAME_EXCL
        capability = "renameatx_np/RENAME_EXCL"
    elif sys.platform.startswith("linux"):
        try:
            function = libc.renameat2
        except AttributeError as exc:
            raise OSError(errno.ENOTSUP, "renameat2 is unavailable") from exc
        flags = 0x00000001  # RENAME_NOREPLACE
        capability = "renameat2/RENAME_NOREPLACE"
    else:
        raise OSError(errno.ENOTSUP, "atomic no-clobber rename is unsupported")
    function.argtypes = (
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    )
    function.restype = ctypes.c_int
    return function, flags, capability


def rename_noreplace_at(
    source_dir_fd: int,
    source_name: str,
    destination_dir_fd: int,
    destination_name: str,
    *,
    on_success: Callable[[], None] | None = None,
) -> None:
    """Atomically rename an entry while refusing to replace any destination."""

    function, flags, _capability = _rename_noreplace_function()
    error_number = 0
    for _attempt in range(_RENAME_NOREPLACE_MAX_ATTEMPTS):
        if (
            function(
                source_dir_fd,
                os.fsencode(source_name),
                destination_dir_fd,
                os.fsencode(destination_name),
                flags,
            )
            == 0
        ):
            if on_success is not None:
                on_success()
            return
        error_number = ctypes.get_errno()
        if error_number != errno.EINTR:
            break
    if error_number == errno.EEXIST:
        raise FileExistsError(error_number, os.strerror(error_number), destination_name)
    raise OSError(error_number, os.strerror(error_number), source_name)
