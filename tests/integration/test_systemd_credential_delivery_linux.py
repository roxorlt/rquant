"""#215 third break, on Linux: the shape systemd itself delivers, read by a non-root process.

Package E's acceptance ran the credstore roles against a credential this process owned, mode
0400, in a private directory, and called that the delivery. systemd does something else, and
the difference is the whole of the third break: it decrypts as **root**, writes the file
0400, then admits the unit's `User=` through a POSIX ACL, so what the service actually finds
is a root-owned `0440` file in a root-owned `0550` directory on a `nosuid,nodev,noexec`
memory-backed mount at `/run/credentials/<unit>`. Five units refused to start on a credential
that had been sealed, delivered and decrypted, and no test on macOS could have seen it,
because there the delivering uid and the reading uid are the same process.

Here they are not. This gate runs as root, mounts a real tmpfs where systemd mounts one,
lays the credential down as root, and reads it back from a real child process running as an
unprivileged uid with no group in common. It moves none of the seams
`tests/support/systemd_credential_delivery.py` moves, with the one named exception below.

    docker run --rm --cap-add SYS_ADMIN -v <repo>:/repo:ro -w /repo \
        -e PYTHONPATH=/repo/src:/repo python:3.11-slim \
        sh -c 'apt-get install -y acl && pip install pytest pydantic pydantic-settings \
               python-dotenv && python -m pytest \
               tests/integration/test_systemd_credential_delivery_linux.py -m linux_exact'

`--cap-add SYS_ADMIN` is what lets this process mount and remount; on the production host
root has it already. Two degradations are forced by Docker Desktop's kernel, which is built
without `CONFIG_TMPFS_POSIX_ACL`, so `setfacl` on a tmpfs answers `Operation not supported`:

* the tmpfs cases admit the reader through **group 0** rather than an ACL. Everything the
  reader checks is identical — a root-owned 0440 file it does not own, on systemd's mount —
  and only the kernel mechanism behind the grant differs;
* the ACL cases therefore run on the container's ordinary filesystem, where `setfacl` works,
  and inject a mount table for that directory (`RQ_TEST_MOUNT_TABLE`, the same seam the
  macOS suite uses). The path, the ownership, the modes and the grant are all real there;
  only the mount verification is fed a synthetic table.

On a host whose tmpfs carries ACLs — the production host's does, which is how its credentials
are delivered at all — both sets collapse into the single real case.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

import rquant.runtime_capabilities as capabilities_module
from rquant.runtime_capabilities import (
    RUNTIME_CAPABILITY_CREDENTIAL_NAME,
    serialize_runtime_credential,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from tests.support.systemd_credential_delivery import write_mount_table

pytestmark = [
    pytest.mark.integration,
    pytest.mark.linux_exact,
    pytest.mark.skipif(
        sys.platform != "linux" or os.geteuid() != 0,
        reason="needs root on Linux, where systemd's own delivery can be reproduced",
    ),
]

#: An unprivileged uid to read as. Nothing needs a passwd entry: the child only setuids.
READER_UID = 1000
#: The gid the ACL cases read under — its own, so that only the ACL can admit them.
READER_GID = 1000

SERVICE_ID = "source.daily-close"
INSTANCE = "svc-" + "a" * 64
KIND = RuntimeServiceKind.DAILY_CLOSE_SOURCE
GENERATION = "b" * 64
VALUES = {"TUSHARE_TOKEN_MAIN": "sealed-by-the-root-helper"}
UNIT = f"rquant-runtime-daily-close@{INSTANCE}.service"
PAYLOAD = serialize_runtime_credential(
    service_id=SERVICE_ID,
    service_kind=KIND,
    instance_name=INSTANCE,
    bundle_generation=GENERATION,
    values=VALUES,
)
EXPECT = json.dumps(
    {
        "kind": KIND.value,
        "service_id": SERVICE_ID,
        "instance": INSTANCE,
        "generation": GENERATION,
    },
    sort_keys=True,
)
#: systemd's own mount for the credential workspace, flag for flag: `mount_nofollow_verbose(
#: LOG_DEBUG, "ramfs", workspace, "ramfs", MS_NODEV|MS_NOEXEC|MS_NOSUID, "mode=0700")`
#: (systemd 252, src/core/execute.c, setup_credentials_internal).
MOUNT_OPTIONS = "nosuid,nodev,noexec,mode=0700"

#: The reader. It calls the real loader with the environment systemd would have set, and
#: reports what came back — including its own uid and groups, so a case that claims to read
#: as an unprivileged process without the delivering uid's groups can be held to it.
CHILD = """
import json, os, sys
from pathlib import Path

import rquant.runtime_capabilities as capabilities
from rquant.runtime_service_entrypoint import RuntimeServiceKind

table = os.environ.get("RQ_TEST_MOUNT_TABLE", "")
if table:
    capabilities._SYSTEMD_MOUNT_TABLE = Path(table)
expect = json.loads(os.environ["RQ_TEST_EXPECT"])
report = {"uid": os.geteuid(), "gid": os.getegid(), "groups": sorted(os.getgroups())}
try:
    loaded = capabilities.load_systemd_runtime_capabilities(
        RuntimeServiceKind(expect["kind"]),
        expected_service_id=expect["service_id"],
        expected_instance=expect["instance"],
        expected_generation=expect["generation"],
    )
except Exception as error:
    report["ok"] = False
    report["error"] = "%s: %s" % (type(error).__name__, error)
else:
    report["ok"] = True
    report["values"] = dict(loaded)
print(json.dumps(report))
"""


def _read_as(
    directory: Path,
    *,
    uid: int = READER_UID,
    gid: int = READER_GID,
    mount_table: Path | None = None,
) -> dict[str, Any]:
    """`load_systemd_runtime_capabilities` in a child process running as `uid`:`gid`."""

    package = Path(capabilities_module.__file__).resolve().parent
    environment = {
        "PATH": "/usr/bin:/bin",
        "PYTHONPATH": os.pathsep.join((str(package.parent), str(package.parents[2]))),
        "PYTHONDONTWRITEBYTECODE": "1",
        "CREDENTIALS_DIRECTORY": str(directory),
        "RQ_TEST_EXPECT": EXPECT,
    }
    if mount_table is not None:
        environment["RQ_TEST_MOUNT_TABLE"] = str(mount_table)
    completed = subprocess.run(
        (sys.executable, "-c", CHILD),
        capture_output=True,
        text=True,
        env=environment,
        cwd="/",
        user=uid,
        group=gid,
        extra_groups=[],
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert (report["uid"], report["gid"]) == (uid, gid)
    assert report["groups"] == [], report["groups"]
    return report


@pytest.fixture
def injected_mount_table() -> Iterator[Callable[[Path], Path]]:
    """A mountinfo the unprivileged child can actually read.

    pytest's own `tmp_path` is 0700 under root, so a table written there would fail to open
    in the child and the case would pass for the wrong reason.
    """

    written: list[Path] = []

    def make(mount_point: Path) -> Path:
        table = Path("/run") / f"rquant-mountinfo-{len(written)}"
        write_mount_table(table, mount_point=mount_point)
        table.chmod(0o644)
        written.append(table)
        return table

    yield make

    for table in written:
        table.unlink(missing_ok=True)


@pytest.fixture
def credential_directory() -> Iterator[Callable[..., Path]]:
    """Make `/run/credentials/<unit>`, on a tmpfs mounted the way systemd mounts it."""

    root = Path("/run/credentials")
    root.mkdir(mode=0o755, parents=True, exist_ok=True)
    mounted: list[Path] = []
    made: list[Path] = []

    def make(unit: str = UNIT, *, tmpfs: bool = True, options: str = MOUNT_OPTIONS) -> Path:
        directory = root / unit
        directory.mkdir(mode=0o700, exist_ok=True)
        made.append(directory)
        if tmpfs:
            completed = subprocess.run(
                ("mount", "-t", "tmpfs", "-o", options, "tmpfs", str(directory)),
                capture_output=True,
                text=True,
                check=False,
            )
            if completed.returncode != 0:
                pytest.skip(f"cannot mount a tmpfs here: {completed.stderr.strip()}")
            mounted.append(directory)
        return directory

    yield make

    for directory in reversed(mounted):
        subprocess.run(("umount", str(directory)), capture_output=True, check=False)
    for directory in reversed(made):
        shutil.rmtree(directory, ignore_errors=True)


def _write(
    directory: Path,
    *,
    payload: bytes = PAYLOAD,
    uid: int = 0,
    gid: int = 0,
    mode: int = 0o440,
    directory_mode: int = 0o550,
    name: str = RUNTIME_CAPABILITY_CREDENTIAL_NAME,
) -> Path:
    """Lay the credential down as root, the way systemd's `write_credential` does."""

    path = directory / name
    path.write_bytes(payload)
    os.chown(path, uid, gid)
    path.chmod(mode)
    directory.chmod(directory_mode)
    return path


def _setfacl(target: Path, entry: str) -> None:
    if shutil.which("setfacl") is None:
        pytest.skip("needs setfacl, which is how systemd admits the unit's User=")
    completed = subprocess.run(
        ("setfacl", "-m", entry, str(target)), capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        pytest.skip(f"this filesystem cannot hold the delivery ACL: {completed.stderr.strip()}")


# ---------------------------------------------------------------------------------------
# The delivery systemd makes, read by the unprivileged process it is made for
# ---------------------------------------------------------------------------------------


def test_a_root_owned_credential_on_systemds_own_mount_reaches_the_service_user(
    credential_directory: Callable[..., Path],
) -> None:
    """The shape that refused to start: root:root 0440, read by uid 1000, and now accepted."""

    directory = credential_directory()
    path = _write(directory)

    report = _read_as(directory, gid=0)

    assert report["ok"], report.get("error")
    assert report["values"] == VALUES
    observed = path.stat()
    assert (observed.st_uid, observed.st_gid) == (0, 0)
    assert observed.st_mode & 0o7777 == 0o440


def test_the_delivery_acl_is_what_admits_the_reader(
    credential_directory: Callable[..., Path],
    injected_mount_table: Callable[[Path], Path],
) -> None:
    """The production grant itself: no group in common, only `u:1000:r` on a root-owned file."""

    directory = credential_directory(tmpfs=False)
    path = _write(directory)
    _setfacl(directory, f"u:{READER_UID}:rx")
    _setfacl(path, f"u:{READER_UID}:r")

    report = _read_as(directory, mount_table=injected_mount_table(directory))

    assert report["ok"], report.get("error")
    assert report["values"] == VALUES


def test_without_the_acl_the_open_itself_fails_and_says_which_uid(
    credential_directory: Callable[..., Path],
) -> None:
    """A wrong `User=` produces exactly this, and "capability is required" used to hide it.

    The directory admits the reader (group 0 here, an ACL on the host) and the credential
    does not, which is the state a unit whose `User=` changed after sealing wakes up in.
    """

    directory = credential_directory()
    _write(directory, mode=0o400)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert f"not readable by uid {READER_UID}" in report["error"]
    assert "0:0 mode 0o0400" in report["error"]
    assert "ACL" in report["error"]
    assert "carries no" not in report["error"]


def test_the_ownership_fallback_is_accepted_on_a_read_only_mount(
    credential_directory: Callable[..., Path],
) -> None:
    """systemd's other shape: where the fs holds no ACL it chowns, and remounts read-only."""

    directory = credential_directory()
    _write(directory, uid=READER_UID, gid=READER_GID, mode=0o400)
    subprocess.run(("mount", "-o", "remount,ro", str(directory)), capture_output=True, check=True)

    report = _read_as(directory, gid=0)

    assert report["ok"], report.get("error")
    assert report["values"] == VALUES


def test_the_ownership_fallback_is_refused_on_a_writable_mount(
    credential_directory: Callable[..., Path],
) -> None:
    """Without the read-only remount the owner could chmod its way to write access."""

    directory = credential_directory()
    _write(directory, uid=READER_UID, gid=READER_GID, mode=0o400)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "ownership fallback" in report["error"]
    assert "not read-only" in report["error"]


# ---------------------------------------------------------------------------------------
# Reverse: nothing systemd would not have produced gets in
# ---------------------------------------------------------------------------------------


def test_a_credential_owned_by_a_third_party_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """Neither root nor the reader: no delivery systemd makes could have left this."""

    directory = credential_directory()
    _write(directory, uid=65534, gid=0, mode=0o440)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "must be owned by uid 0 or by the runtime uid" in report["error"]
    assert "observed 65534" in report["error"]


def test_a_world_readable_credential_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """0444 is readable by every account on the host, which a sealed secret may never be."""

    directory = credential_directory()
    _write(directory, mode=0o444)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "must not be world accessible, observed 0o0444" in report["error"]


def test_a_hardlinked_credential_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """A second link is a copy that would outlive the unit's credential directory."""

    directory = credential_directory()
    path = _write(directory)
    directory.chmod(0o700)
    os.link(path, directory / "kept.json")
    directory.chmod(0o550)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "hardlink count must be one, observed 2" in report["error"]


def test_a_directory_the_service_user_owns_is_refused(
    credential_directory: Callable[..., Path],
    injected_mount_table: Callable[[Path], Path],
) -> None:
    """If the reader owns the directory it can put anything in it, so nothing in it is evidence."""

    directory = credential_directory(tmpfs=False)
    path = _write(directory)
    _setfacl(path, f"u:{READER_UID}:r")
    os.chown(directory, READER_UID, READER_GID)

    report = _read_as(directory, mount_table=injected_mount_table(directory))

    assert not report["ok"]
    assert "credential directory as 0:0" in report["error"]
    assert f"observed {READER_UID}:{READER_GID}" in report["error"]


def test_a_credential_on_an_ordinary_filesystem_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """The real mount check over the real mount table: nothing is injected in this one."""

    directory = credential_directory(tmpfs=False)
    path = _write(directory)
    _setfacl(directory, f"u:{READER_UID}:rx")
    _setfacl(path, f"u:{READER_UID}:r")

    report = _read_as(directory)

    assert not report["ok"]
    assert "memory-backed mount that never reaches a disk" in report["error"]


def test_a_mount_without_the_three_flags_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """A tmpfs is not enough: systemd's is `nosuid,nodev,noexec` and this one is not."""

    directory = credential_directory(options="mode=0700")
    _write(directory)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "is missing nosuid, nodev, noexec" in report["error"]


def test_a_credential_outside_the_credentials_root_is_refused(tmp_path: Path) -> None:
    """`CREDENTIALS_DIRECTORY` has to name where systemd puts credentials, not any place."""

    directory = tmp_path / UNIT
    directory.mkdir(mode=0o755, parents=True)
    _write(directory, directory_mode=0o555)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "a systemd credential directory is /run/credentials/<unit>.service" in report["error"]
