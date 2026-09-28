"""Explicit bootstrap for the optional private systemd journal service."""

from __future__ import annotations

import argparse
import os
import re
import signal
import socket
import stat
import sys
import threading
from collections.abc import Sequence
from pathlib import Path
from types import FrameType
from typing import Literal

from rquant.ops_status import load_signed_ops_manifest
from rquant.unit_log_reader import JournalLogReader
from rquant.unit_log_service import UnitLogService, _private_directory

_MAX_CURSOR_KEY_BYTES = 4096
_MAX_PUBLIC_KEY_BYTES = 8192
_IDENTITY_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
    "st_gid",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> None:
        raise ValueError("unit log daemon arguments are invalid")


def _parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(allow_abbrev=False)
    parser.add_argument("--socket-path", required=True)
    parser.add_argument("--web-uid", required=True)
    parser.add_argument("--web-group-gid", required=True)
    parser.add_argument("--manifest-path", required=True)
    parser.add_argument("--public-key-path", required=True)
    parser.add_argument("--expected-host", required=True)
    parser.add_argument("--cursor-key-file", required=True)
    parser.add_argument("--self-check", action="store_true")
    return parser


def _absolute_no_symlink_path(raw: str, *, existing: bool = True) -> Path:
    if not raw.startswith("/") or os.path.normpath(raw) != raw or ".." in Path(raw).parts:
        raise ValueError("unit log daemon path is invalid")
    path = Path(raw)
    current = Path("/")
    for index, part in enumerate(path.parts[1:], start=1):
        current /= part
        if not existing and index == len(path.parts) - 1:
            break
        state = current.lstat()
        if stat.S_ISLNK(state.st_mode):
            raise ValueError("unit log daemon path contains a symbolic link")
        if index < len(path.parts) - 1 and not stat.S_ISDIR(state.st_mode):
            raise ValueError("unit log daemon path ancestor is not a directory")
    return path


def _identity(state: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(state, name) for name in _IDENTITY_FIELDS)


def _validate_material(state: os.stat_result, *, max_bytes: int, kind: str) -> None:
    if not stat.S_ISREG(state.st_mode) or not 1 <= state.st_size <= max_bytes:
        raise ValueError("unit log daemon trust file is invalid")
    if kind == "key":
        if (
            state.st_uid != os.geteuid()
            or state.st_nlink != 1
            or stat.S_IMODE(state.st_mode) not in {0o400, 0o600}
            or state.st_size < 32
        ):
            raise ValueError("unit log daemon cursor key is invalid")
    elif kind == "public":
        if stat.S_IMODE(state.st_mode) & 0o022:
            raise ValueError("unit log daemon public key is writable")
    else:
        raise ValueError("unit log daemon material kind is invalid")


def _read_stable_file(path: Path, *, max_bytes: int, kind: Literal["key", "public"]) -> bytes:
    candidate = _absolute_no_symlink_path(str(path))
    before = candidate.lstat()
    _validate_material(before, max_bytes=max_bytes, kind=kind)
    if not hasattr(os, "O_NOFOLLOW"):
        raise ValueError("unit log daemon requires no-follow file opens")
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(candidate, flags)
    try:
        opened = os.fstat(descriptor)
        _validate_material(opened, max_bytes=max_bytes, kind=kind)
        if _identity(opened) != _identity(before):
            raise ValueError("unit log daemon trust file changed")
        chunks = bytearray()
        while len(chunks) <= max_bytes:
            chunk = os.read(descriptor, max_bytes + 1 - len(chunks))
            if not chunk:
                break
            chunks.extend(chunk)
        after = os.fstat(descriptor)
        current = candidate.lstat()
        if (
            len(chunks) != opened.st_size
            or _identity(after) != _identity(opened)
            or _identity(current) != _identity(opened)
        ):
            raise ValueError("unit log daemon trust file changed")
        return bytes(chunks)
    finally:
        os.close(descriptor)


def _unsigned_id(raw: str) -> int:
    if re.fullmatch(r"(?:0|[1-9][0-9]{0,9})", raw) is None:
        raise ValueError("unit log daemon identity is invalid")
    value = int(raw)
    if value > 2**32 - 1:
        raise ValueError("unit log daemon identity is invalid")
    return value


def _preflight(arguments: argparse.Namespace) -> tuple[Path, int, int, JournalLogReader]:
    socket_path = _absolute_no_symlink_path(arguments.socket_path, existing=False)
    manifest_path = _absolute_no_symlink_path(arguments.manifest_path)
    public_key_path = _absolute_no_symlink_path(arguments.public_key_path)
    cursor_key_path = _absolute_no_symlink_path(arguments.cursor_key_file)
    web_uid = _unsigned_id(arguments.web_uid)
    web_group_gid = _unsigned_id(arguments.web_group_gid)
    if web_uid == os.geteuid():
        raise ValueError("unit log daemon and Web require different UIDs")
    host = arguments.expected_host
    if type(host) is not str or not 1 <= len(host) <= 253 or host != socket.gethostname():
        raise ValueError("unit log daemon host is invalid")
    _private_directory(socket_path, owner_uid=os.geteuid(), web_group_gid=web_group_gid)
    try:
        socket_path.lstat()
    except FileNotFoundError:
        pass
    else:
        raise ValueError("unit log daemon socket path is occupied")
    cursor_key_before = cursor_key_path.lstat()
    public_key_before = public_key_path.lstat()
    cursor_secret = _read_stable_file(cursor_key_path, max_bytes=_MAX_CURSOR_KEY_BYTES, kind="key")
    public_key_pem = _read_stable_file(
        public_key_path, max_bytes=_MAX_PUBLIC_KEY_BYTES, kind="public"
    )
    manifest_before = manifest_path.lstat()
    if not stat.S_ISREG(manifest_before.st_mode):
        raise ValueError("unit log daemon manifest is not a regular file")
    load_signed_ops_manifest(
        manifest_path,
        public_key_pem=public_key_pem,
        expected_host=host,
    )
    if (
        _identity(manifest_path.lstat()) != _identity(manifest_before)
        or _identity(public_key_path.lstat()) != _identity(public_key_before)
        or _identity(cursor_key_path.lstat()) != _identity(cursor_key_before)
    ):
        raise ValueError("unit log daemon trust material changed")
    reader = JournalLogReader(
        manifest_path=manifest_path,
        public_key_pem=public_key_pem,
        expected_host=host,
        cursor_secret=cursor_secret,
    )
    return socket_path, web_uid, web_group_gid, reader


def main(argv: Sequence[str] | None = None) -> int:
    try:
        arguments = _parser().parse_args(argv)
    except ValueError:
        print("unit log daemon arguments are invalid", file=sys.stderr)
        return 2
    try:
        socket_path, web_uid, web_group_gid, reader = _preflight(arguments)
        if arguments.self_check:
            return 0
        service = UnitLogService(
            socket_path=socket_path,
            web_uid=web_uid,
            web_group_gid=web_group_gid,
            reader=reader,
        )
        stop = threading.Event()

        def request_stop(_signum: int, _frame: FrameType | None) -> None:
            stop.set()

        previous = {signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)}
        try:
            for signum in previous:
                signal.signal(signum, request_stop)
            service.serve(stop=stop)
        finally:
            for signum, handler in previous.items():
                signal.signal(signum, handler)
        return 0
    except Exception:
        print("unit log daemon preflight or service failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
