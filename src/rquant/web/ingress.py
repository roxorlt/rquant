"""Bind the Web API to a private nginx-accessible Unix socket."""

from __future__ import annotations

import os
import socket
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def private_web_ingress_socket(
    path: Path, *, nginx_group_gid: int
) -> Iterator[socket.socket]:
    """Prebind securely so Uvicorn cannot apply its permissive UDS mode."""

    path = Path(path)
    if not path.is_absolute() or ".." in path.parts or len(os.fsencode(path)) >= 100:
        raise ValueError("private Web ingress socket path is unsafe")
    directory = path.parent.lstat()
    if (
        not stat.S_ISDIR(directory.st_mode)
        or directory.st_uid != os.geteuid()
        or directory.st_gid != nginx_group_gid
        or stat.S_IMODE(directory.st_mode) != 0o710
    ):
        raise ValueError("private Web ingress directory must be owned and mode 0710")
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("private Web ingress socket path already exists")

    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    identity: tuple[int, int] | None = None
    try:
        old_umask = os.umask(0o177)
        try:
            listener.bind(str(path))
        finally:
            os.umask(old_umask)
        bound = path.lstat()
        identity = (bound.st_dev, bound.st_ino)
        if not stat.S_ISSOCK(bound.st_mode) or bound.st_uid != os.geteuid():
            raise ValueError("private Web ingress socket owner is unsafe")
        os.chown(path, -1, nginx_group_gid, follow_symlinks=False)
        os.chmod(path, 0o660, follow_symlinks=False)
        ready = path.lstat()
        if (
            not stat.S_ISSOCK(ready.st_mode)
            or (ready.st_dev, ready.st_ino) != identity
            or ready.st_uid != os.geteuid()
            or ready.st_gid != nginx_group_gid
            or stat.S_IMODE(ready.st_mode) != 0o660
        ):
            raise ValueError("private Web ingress socket permissions are unsafe")
        listener.listen(128)
        yield listener
    finally:
        listener.close()
        if identity is not None:
            try:
                current = path.lstat()
            except FileNotFoundError:
                pass
            else:
                if stat.S_ISSOCK(current.st_mode) and (current.st_dev, current.st_ino) == identity:
                    path.unlink()
