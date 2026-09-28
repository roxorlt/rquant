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
from collections.abc import Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Literal

from rquant.ops_status import SignedOpsInstallManifest, verify_ops_manifest
from rquant.strict_json import strict_canonical_json_loads
from rquant.unit_log_reader import JournalLogReader
from rquant.unit_log_service import UnitLogService, _private_directory

_MAX_CURSOR_KEY_BYTES = 4096
_MAX_PUBLIC_KEY_BYTES = 8192
_MAX_MANIFEST_BYTES = 64 * 1024
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


def _absolute_path(raw: str) -> Path:
    if (
        not raw.startswith("/")
        or raw.startswith("//")
        or os.path.normpath(raw) != raw
        or ".." in Path(raw).parts
    ):
        raise ValueError("unit log daemon path is invalid")
    return Path(raw)


def _identity(state: os.stat_result) -> tuple[int, ...]:
    return tuple(getattr(state, name) for name in _IDENTITY_FIELDS)


def _trusted_directory(state: os.stat_result, *, web_uid: int) -> None:
    if (
        not stat.S_ISDIR(state.st_mode)
        or state.st_uid not in {0, os.geteuid()}
        or state.st_uid == web_uid
        or (stat.S_IMODE(state.st_mode) & 0o022 and not state.st_mode & stat.S_ISVTX)
    ):
        raise ValueError("unit log daemon directory is replaceable")


def _trusted_child(state: os.stat_result, *, web_uid: int) -> None:
    if state.st_uid not in {0, os.geteuid()} or state.st_uid == web_uid:
        raise ValueError("unit log daemon path entry is replaceable")


@dataclass(frozen=True)
class _AnchoredParent:
    path: Path
    parent_fd: int
    links: tuple[tuple[int, str, tuple[int, ...]], ...]
    web_uid: int

    @property
    def name(self) -> str:
        return self.path.name

    def recheck(self) -> None:
        for parent_fd, name, identity in self.links:
            current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            if _identity(current) != identity:
                raise ValueError("unit log daemon path ancestor changed")
        _trusted_directory(os.fstat(self.parent_fd), web_uid=self.web_uid)

    def leaf(self) -> os.stat_result:
        result = os.stat(self.name, dir_fd=self.parent_fd, follow_symlinks=False)
        _trusted_child(result, web_uid=self.web_uid)
        return result


@contextmanager
def _anchored_parent(path: Path, *, web_uid: int) -> Iterator[_AnchoredParent]:
    candidate = _absolute_path(str(path))
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise ValueError("unit log daemon requires anchored directory opens")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    with ExitStack() as stack:
        parent_fd = os.open("/", flags)
        stack.callback(os.close, parent_fd)
        links: list[tuple[int, str, tuple[int, ...]]] = []
        parent_state = os.fstat(parent_fd)
        for name in candidate.parts[1:-1]:
            _trusted_directory(parent_state, web_uid=web_uid)
            before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            _trusted_child(before, web_uid=web_uid)
            if not stat.S_ISDIR(before.st_mode):
                raise ValueError("unit log daemon path ancestor is not a directory")
            child_fd = os.open(name, flags, dir_fd=parent_fd)
            stack.callback(os.close, child_fd)
            opened = os.fstat(child_fd)
            if _identity(opened) != _identity(before):
                raise ValueError("unit log daemon path ancestor changed")
            links.append((parent_fd, name, _identity(opened)))
            parent_fd, parent_state = child_fd, opened
        _trusted_directory(parent_state, web_uid=web_uid)
        anchor = _AnchoredParent(candidate, parent_fd, tuple(links), web_uid)
        anchor.recheck()
        try:
            yield anchor
        finally:
            anchor.recheck()


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
        if state.st_uid not in {0, os.geteuid()} or stat.S_IMODE(state.st_mode) & 0o022:
            raise ValueError("unit log daemon public key ownership or mode is invalid")
    elif kind != "manifest":
        raise ValueError("unit log daemon material kind is invalid")


def _read_stable_file(
    path: Path,
    *,
    max_bytes: int,
    kind: Literal["key", "public", "manifest"],
    web_uid: int,
) -> bytes:
    with _anchored_parent(path, web_uid=web_uid) as anchor:
        before = anchor.leaf()
        _validate_material(before, max_bytes=max_bytes, kind=kind)
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
        descriptor = os.open(anchor.name, flags, dir_fd=anchor.parent_fd)
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
            if (
                len(chunks) != opened.st_size
                or _identity(after) != _identity(opened)
                or _identity(anchor.leaf()) != _identity(opened)
            ):
                raise ValueError("unit log daemon trust file changed")
            return bytes(chunks)
        finally:
            os.close(descriptor)


def _leaf_identity(path: Path, *, web_uid: int) -> tuple[int, ...]:
    with _anchored_parent(path, web_uid=web_uid) as anchor:
        return _identity(anchor.leaf())


def _load_anchored_manifest(
    path: Path, *, public_key_pem: bytes, expected_host: str, web_uid: int
) -> None:
    payload = _read_stable_file(
        path, max_bytes=_MAX_MANIFEST_BYTES, kind="manifest", web_uid=web_uid
    )
    signed = SignedOpsInstallManifest.model_validate(strict_canonical_json_loads(payload))
    if signed.canonical_bytes() != payload:
        raise ValueError("unit log daemon manifest is not canonical")
    verify_ops_manifest(signed, public_key_pem=public_key_pem, expected_host=expected_host)


def _unsigned_id(raw: str) -> int:
    if re.fullmatch(r"(?:0|[1-9][0-9]{0,9})", raw) is None:
        raise ValueError("unit log daemon identity is invalid")
    value = int(raw)
    if value > 2**32 - 1:
        raise ValueError("unit log daemon identity is invalid")
    return value


def _check_socket_anchor(anchor: _AnchoredParent, *, web_group_gid: int) -> None:
    anchor.recheck()
    directory = os.fstat(anchor.parent_fd)
    if (
        directory.st_uid != os.geteuid()
        or directory.st_gid != web_group_gid
        or stat.S_IMODE(directory.st_mode) != 0o710
    ):
        raise ValueError("unit log daemon socket directory is invalid")
    _private_directory(anchor.path, owner_uid=os.geteuid(), web_group_gid=web_group_gid)
    anchor.recheck()


def _require_socket_absent(anchor: _AnchoredParent) -> None:
    try:
        os.stat(anchor.name, dir_fd=anchor.parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    raise ValueError("unit log daemon socket path is occupied")


def _preflight(arguments: argparse.Namespace) -> tuple[Path, int, int, JournalLogReader]:
    web_uid = _unsigned_id(arguments.web_uid)
    web_group_gid = _unsigned_id(arguments.web_group_gid)
    if web_uid == os.geteuid():
        raise ValueError("unit log daemon and Web require different UIDs")
    socket_path = _absolute_path(arguments.socket_path)
    manifest_path = _absolute_path(arguments.manifest_path)
    public_key_path = _absolute_path(arguments.public_key_path)
    cursor_key_path = _absolute_path(arguments.cursor_key_file)
    host = arguments.expected_host
    if type(host) is not str or not 1 <= len(host) <= 253 or host != socket.gethostname():
        raise ValueError("unit log daemon host is invalid")
    with _anchored_parent(socket_path, web_uid=web_uid) as socket_anchor:
        _check_socket_anchor(socket_anchor, web_group_gid=web_group_gid)
        _require_socket_absent(socket_anchor)
    cursor_key_before = _leaf_identity(cursor_key_path, web_uid=web_uid)
    public_key_before = _leaf_identity(public_key_path, web_uid=web_uid)
    manifest_before = _leaf_identity(manifest_path, web_uid=web_uid)
    cursor_secret = _read_stable_file(
        cursor_key_path, max_bytes=_MAX_CURSOR_KEY_BYTES, kind="key", web_uid=web_uid
    )
    public_key_pem = _read_stable_file(
        public_key_path, max_bytes=_MAX_PUBLIC_KEY_BYTES, kind="public", web_uid=web_uid
    )
    _load_anchored_manifest(
        manifest_path,
        public_key_pem=public_key_pem,
        expected_host=host,
        web_uid=web_uid,
    )
    if (
        _leaf_identity(manifest_path, web_uid=web_uid) != manifest_before
        or _leaf_identity(public_key_path, web_uid=web_uid) != public_key_before
        or _leaf_identity(cursor_key_path, web_uid=web_uid) != cursor_key_before
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

        with _anchored_parent(socket_path, web_uid=web_uid) as socket_anchor:
            _check_socket_anchor(socket_anchor, web_group_gid=web_group_gid)
            _require_socket_absent(socket_anchor)
            previous = {
                signum: signal.getsignal(signum) for signum in (signal.SIGTERM, signal.SIGINT)
            }
            try:
                for signum in previous:
                    signal.signal(signum, request_stop)
                socket_anchor.recheck()
                service.serve(stop=stop)
                socket_anchor.recheck()
            finally:
                for signum, handler in previous.items():
                    signal.signal(signum, handler)
        return 0
    except Exception:
        print("unit log daemon preflight or service failed", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
