"""Verify one restart-bound proof that the private ingress authenticated a request."""

from __future__ import annotations

import hmac
import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path

PROXY_PROOF_HEADER = b"x-rquant-proxy-proof"
_TOKEN = re.compile(rb"[0-9a-f]{64}\Z")


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
        value.st_uid,
        stat.S_IMODE(value.st_mode),
    )


def _require_private_parents(path: Path) -> None:
    for parent in path.parents:
        node = os.lstat(parent)
        if (
            not stat.S_ISDIR(node.st_mode)
            or node.st_uid not in {0, os.geteuid()}
            or stat.S_IMODE(node.st_mode) & 0o022
        ):
            raise ValueError("proxy proof parent directory is not private")


@dataclass(frozen=True)
class ProxyIdentityVerifier:
    path: Path
    _expected: bytes = field(repr=False)
    _file_identity: tuple[int, int, int, int, int, int, int] = field(repr=False)

    @classmethod
    def load(cls, path: Path) -> ProxyIdentityVerifier | None:
        """Return no verifier when the explicit credential cannot be read safely."""
        try:
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("proxy proof path must be absolute and canonical")
            _require_private_parents(path)
            before = os.lstat(path)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) != 0o400
                or before.st_nlink != 1
                or before.st_size not in (64, 65)
            ):
                raise ValueError("proxy proof file is not private")
            descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                opened = os.fstat(descriptor)
                if _identity(opened) != _identity(before):
                    raise ValueError("proxy proof file changed while opening")
                raw = os.read(descriptor, 66)
                after = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            if _identity(after) != _identity(before) or _identity(os.lstat(path)) != _identity(
                before
            ):
                raise ValueError("proxy proof file changed while reading")
            token = raw[:-1] if raw.endswith(b"\n") else raw
            if _TOKEN.fullmatch(token) is None:
                raise ValueError("proxy proof value is invalid")
            return cls(path=path, _expected=token, _file_identity=_identity(before))
        except (OSError, ValueError):
            return None

    def verify(self, presented: bytes) -> bool:
        if len(presented) != 64 or _TOKEN.fullmatch(presented) is None:
            return False
        try:
            _require_private_parents(self.path)
            if _identity(os.lstat(self.path)) != self._file_identity:
                return False
        except (OSError, ValueError):
            return False
        return hmac.compare_digest(presented, self._expected)
