"""Lay a credential out the way `LoadCredentialEncrypted=` really lays one out.

Package E's fixtures wrote the credential as "a file this process owns, mode 0400, in a
private directory", and that shape passed every check the reader had. It is not the shape
systemd produces for a `User=lighthouse` unit, which is what the first Route A window met:
`/run/credentials/<unit>` is a root-owned directory on systemd's own memory-backed mount,
and `capabilities.json` inside it is **root-owned 0440** with a POSIX ACL admitting the
service user. Five units refused a credential that had been sealed, delivered and decrypted.

The reader now checks the real contract, so every fixture has to produce the real shape.
Three of its facts a non-root test on any platform cannot produce, and each is a named seam
of `rquant.runtime_capabilities` that this module moves and nothing else does:

* `_SYSTEMD_CREDENTIALS_ROOT` — the test's root stands in for `/run/credentials`;
* `_SYSTEMD_DELIVERY_OWNER` — the uid/gid systemd delivers as, moved from root to the
  running user, so a directory this process can create is one the reader accepts;
* `_SYSTEMD_MOUNT_TABLE` — a mountinfo file describing that root as a `nosuid,nodev,noexec`
  tmpfs. The device number in it is the directory's **real** `st_dev`, so the parser, the
  option check and the device cross-check all run for real against a synthetic table.

The Linux gate in `tests/integration/test_systemd_credential_delivery_linux.py` moves none
of the three: it runs as root, mounts a real tmpfs at a real `/run/credentials/<unit>`, and
reads it back from a real non-root process.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import pytest

import rquant.runtime_capabilities as capabilities_module
from rquant.runtime_capabilities import RUNTIME_CAPABILITY_CREDENTIAL_NAME

#: A unit name of the shape the seven credstore templates instantiate to.
DEFAULT_UNIT = "rquant-runtime-daily-close@svc-" + "a" * 64 + ".service"
#: What the production host reports for `/run/credentials/<unit>`: systemd mounts the
#: workspace `MS_NODEV|MS_NOEXEC|MS_NOSUID` and remounts the finished directory read-only.
TMPFS_OPTIONS = "ro,nosuid,nodev,noexec,relatime"
#: What Docker's own `--tmpfs` reports, and what a root-owned credential is read off in the
#: Linux gate: the read-only remount is systemd's, and `--tmpfs` does not do it.
WRITABLE_TMPFS_OPTIONS = "rw,nosuid,nodev,noexec,relatime"


@dataclass(frozen=True)
class Delivery:
    """One credential, laid out the way systemd would have laid it out."""

    root: Path
    unit: str
    directory: Path
    path: Path

    @property
    def environ(self) -> dict[str, str]:
        """The one environment variable systemd exports for it."""

        return {"CREDENTIALS_DIRECTORY": str(self.directory)}


def write_mount_table(
    table: Path,
    *,
    mount_point: Path,
    filesystem: str = "tmpfs",
    options: str = TMPFS_OPTIONS,
) -> None:
    """A mountinfo naming `mount_point` as its own mount, with its real device number.

    Only the file is synthetic. The parser, the longest-prefix choice, the filesystem check,
    the option check and the device cross-check all run against it for real, and the device
    is the one the kernel gave the directory, so a table that describes some other mount
    cannot pass for this one.
    """

    device = os.stat(mount_point).st_dev
    table.write_text(
        "21 1 0:1 / / rw,relatime shared:1 - ext4 /dev/vda1 rw\n"
        f"36 21 {os.major(device)}:{os.minor(device)} / {mount_point} {options} shared:2 - "
        f"{filesystem} {filesystem} rw,mode=700\n",
        encoding="utf-8",
    )


def install_delivery(
    monkeypatch: pytest.MonkeyPatch,
    *,
    root: Path,
    filesystem: str = "tmpfs",
    mount_options: str = TMPFS_OPTIONS,
    owner: tuple[int, int] | None = None,
) -> Path:
    """Make `root` stand in for `/run/credentials`, and return it."""

    root.mkdir(mode=0o755, parents=True, exist_ok=True)
    table = root.parent / f"{root.name}.mountinfo"
    write_mount_table(table, mount_point=root, filesystem=filesystem, options=mount_options)
    monkeypatch.setattr(capabilities_module, "_SYSTEMD_CREDENTIALS_ROOT", root)
    monkeypatch.setattr(capabilities_module, "_SYSTEMD_MOUNT_TABLE", table)
    monkeypatch.setattr(
        capabilities_module,
        "_SYSTEMD_DELIVERY_OWNER",
        owner if owner is not None else (os.geteuid(), os.stat(root).st_gid),
    )
    return root


def deliver(
    monkeypatch: pytest.MonkeyPatch,
    *,
    root: Path,
    payload: bytes,
    unit: str = DEFAULT_UNIT,
    mode: int = 0o400,
    directory_mode: int = 0o550,
    name: str = RUNTIME_CAPABILITY_CREDENTIAL_NAME,
    filesystem: str = "tmpfs",
    mount_options: str = TMPFS_OPTIONS,
    owner: tuple[int, int] | None = None,
) -> Delivery:
    """Deliver `payload` for `unit` under `root`, installing the seams on first use.

    Every keyword is exactly one clause of the contract, so a reverse case breaks the one
    clause it is about and leaves the rest of the delivery real — `mode=0o444` is a
    world-readable credential in an otherwise perfect directory, `filesystem="ext4"` is a
    perfect file on a mount systemd would never have made.
    """

    install_delivery(
        monkeypatch,
        root=root,
        filesystem=filesystem,
        mount_options=mount_options,
        owner=owner,
    )
    directory = root / unit
    directory.mkdir(mode=0o700, exist_ok=True)
    directory.chmod(0o700)
    path = directory / name
    # systemd writes each credential into a directory it has just made; a redelivery here
    # would otherwise meet the read-only file the last one left behind.
    path.unlink(missing_ok=True)
    path.write_bytes(payload)
    path.chmod(mode)
    directory.chmod(directory_mode)
    return Delivery(root=root, unit=unit, directory=directory, path=path)


def empty_directory(
    monkeypatch: pytest.MonkeyPatch,
    *,
    root: Path,
    unit: str = DEFAULT_UNIT,
    directory_mode: int = 0o550,
) -> Delivery:
    """A credential directory systemd made, with no `capabilities.json` in it."""

    install_delivery(monkeypatch, root=root)
    directory = root / unit
    directory.mkdir(mode=0o700, exist_ok=True)
    directory.chmod(directory_mode)
    return Delivery(
        root=root,
        unit=unit,
        directory=directory,
        path=directory / RUNTIME_CAPABILITY_CREDENTIAL_NAME,
    )
