"""#215 third break, on Linux: the shape systemd itself delivers, read by a non-root process.

Package E's acceptance ran the credstore roles against a credential this process owned, mode
0400, in a private directory, and called that the delivery. systemd does something else, and
the difference is the whole of the third break: it decrypts as **root**, writes the file
0400, then admits the unit's `User=` through a POSIX ACL — on the file and on the directory
together — so what the service actually finds is a root-owned `0440` file in a root-owned
`0550` directory on a `nosuid,nodev,noexec` memory-backed mount at `/run/credentials/<unit>`
that systemd has remounted **read-only**. Five units refused to start on a credential that
had been sealed, delivered and decrypted, and no test on macOS could have seen it, because
there the delivering uid and the reading uid are the same process.

Here they are not. This gate runs as root, mounts a real tmpfs where systemd mounts one,
lays the credential down as root, remounts read-only the way systemd does, and reads it back
from a real child process running as an unprivileged uid. It moves none of the seams
`tests/support/systemd_credential_delivery.py` moves, except where a host cannot hold a
POSIX ACL on tmpfs — and that is decided by **trying it**, not by assumption, so a host that
can (a GitHub `ubuntu-latest` runner, the production host) runs the production shape itself.

    docker build -t rquant-credshape:1 <dir with the Dockerfile from the report>
    docker run --rm --cap-add SYS_ADMIN -v <repo>:/repo:ro -w /repo \
      -e PYTHONPATH=/repo/src:/repo \
      -e TUSHARE_TOKEN_MAIN=00000000000000000000000000000000placeholder \
      -e DATA_DIR=/tmp/rq/data -e DUCKDB_PATH=/tmp/rq/data/rquant.duckdb \
      -e PARQUET_DIR=/tmp/rq/data/parquet -e LOG_DIR=/tmp/rq/logs \
      rquant-credshape:1 \
      python -m pytest tests/integration/test_systemd_credential_delivery_linux.py \
        -m linux_exact -q -p no:cacheprovider

The five `Settings` variables are not optional: `tests/conftest.py` has an autouse fixture
that imports `rquant.config`, and a checkout without a `.env` cannot build `Settings` — the
whole file errors out before a single case runs. `--cap-add SYS_ADMIN` is what lets this
process mount and remount; on the production host root has it already.

**Nothing here skips.** A host that cannot mount a tmpfs, or holds no ACL anywhere, fails
the gate rather than passing it empty — a green run with zero assertions is exactly what let
the third break through in the first place. Wire it with the repository's usual JUnit
contract (`tests/support/assert_junit_contract.py … --skipped 0`).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Iterator, Sequence
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
#: A third party that is neither systemd nor the service user.
STRANGER_UID = 65534
STRANGER_GID = 65534

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
#: systemd's own mount for the credential workspace, flag for flag: `credentials_fs_mount_flags()`
#: is `MS_NODEV|MS_NOEXEC|MS_NOSUID|ms_nosymfollow_supported()|(ro ? MS_RDONLY : 0)`
#: (systemd 255 `src/shared/mount-util.c:1643-1645`), and the workspace is mounted writable
#: (`exec-credential.c:808`) then remounted read-only before the service sees it (`:869`).
MOUNT_OPTIONS = "nosuid,nodev,noexec,mode=0700"

#: The reader. It calls the real loader with the environment systemd would have set, and
#: reports what came back — including its own uid, gid and groups, so a case that claims to
#: read as an unprivileged process without the delivering uid's groups can be held to it.
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


def _run(command: Sequence[str], *, what: str) -> subprocess.CompletedProcess[str]:
    """A privileged step that has to work here. If it cannot, the gate is red, never green."""

    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    if completed.returncode != 0:
        pytest.fail(f"{what} failed ({' '.join(command)}): {completed.stderr.strip()}")
    return completed


def _read_as(
    directory: Path,
    *,
    uid: int = READER_UID,
    gid: int = READER_GID,
    extra_groups: Sequence[int] = (),
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
        extra_groups=list(extra_groups),
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    report = json.loads(completed.stdout)
    assert (report["uid"], report["gid"]) == (uid, gid)
    assert report["groups"] == sorted(extra_groups), report["groups"]
    return report


def _seal(directory: Path) -> None:
    """systemd's last step: remount the finished credential directory read-only (`:869`)."""

    _run(("mount", "-o", "remount,ro", str(directory)), what="read-only remount")


def _tmpfs_holds_acls() -> bool:
    """Whether a tmpfs on *this* host can carry the delivery ACL — decided by trying it.

    Docker Desktop's LinuxKit kernel is built without `CONFIG_TMPFS_POSIX_ACL`, so `setfacl`
    there answers `Operation not supported` and the ACL cases have to run on an ordinary
    filesystem with an injected mount table. A GitHub `ubuntu-latest` runner and the
    production host both do carry them, and there this probe keeps the ACL cases on the real
    thing. Assuming either way would make the file lie about what it covers.
    """

    probe = Path("/run/rquant-acl-probe")
    probe.mkdir(mode=0o700, exist_ok=True)
    _run(("mount", "-t", "tmpfs", "-o", "mode=0700", "tmpfs", str(probe)), what="probe tmpfs")
    try:
        target = probe / "probe"
        target.write_bytes(b"probe")
        held = subprocess.run(
            ("setfacl", "-m", f"u:{READER_UID}:r", str(target)),
            capture_output=True,
            text=True,
            check=False,
        )
        return held.returncode == 0
    finally:
        subprocess.run(("umount", str(probe)), capture_output=True, check=False)
        probe.rmdir()


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
    """Make `/run/credentials/<unit>`, on a tmpfs mounted the way systemd mounts one."""

    root = Path("/run/credentials")
    root.mkdir(mode=0o755, parents=True, exist_ok=True)
    mounted: list[Path] = []
    made: list[Path] = []

    def make(unit: str = UNIT, *, tmpfs: bool = True, options: str = MOUNT_OPTIONS) -> Path:
        directory = root / unit
        directory.mkdir(mode=0o700, exist_ok=True)
        made.append(directory)
        if tmpfs:
            _run(
                ("mount", "-t", "tmpfs", "-o", options, "tmpfs", str(directory)),
                what="credential tmpfs mount",
            )
            mounted.append(directory)
        return directory

    yield make

    for directory in reversed(mounted):
        subprocess.run(("umount", "-l", str(directory)), capture_output=True, check=False)
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
    directory_owner: tuple[int, int] = (0, 0),
    name: str = RUNTIME_CAPABILITY_CREDENTIAL_NAME,
) -> Path:
    """Lay the credential down as root, the way systemd's `write_credential` does."""

    path = directory / name
    path.write_bytes(payload)
    os.chown(path, uid, gid)
    path.chmod(mode)
    directory.chmod(directory_mode)
    os.chown(directory, *directory_owner)
    return path


def _setfacl(target: Path, entry: str) -> None:
    """The grant systemd uses. If this host cannot hold it anywhere, the gate is red."""

    if shutil.which("setfacl") is None:
        pytest.fail("needs setfacl (package `acl`), which is how systemd admits the unit's User=")
    _run(("setfacl", "-m", entry, str(target)), what=f"delivery ACL {entry}")


# ---------------------------------------------------------------------------------------
# The delivery systemd makes, read by the unprivileged process it is made for
# ---------------------------------------------------------------------------------------


def test_a_root_owned_credential_on_systemds_own_mount_reaches_the_service_user(
    credential_directory: Callable[..., Path],
) -> None:
    """The shape that refused to start: root:root 0440, read by uid 1000, and now accepted."""

    directory = credential_directory()
    path = _write(directory)
    _seal(directory)

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
    """The production grant itself: no group in common, only `u:1000:r` on a root-owned file.

    On a host whose tmpfs carries ACLs this is the whole production shape with nothing
    injected at all; where it does not, the grant is still real and only the mount table is
    synthetic. Which one ran is asserted, so neither can be mistaken for the other.
    """

    on_tmpfs = _tmpfs_holds_acls()
    directory = credential_directory(tmpfs=on_tmpfs)
    path = _write(directory)
    _setfacl(directory, f"u:{READER_UID}:rx")
    _setfacl(path, f"u:{READER_UID}:r")
    table = None
    if on_tmpfs:
        _seal(directory)
    else:
        table = injected_mount_table(directory)

    report = _read_as(directory, mount_table=table)

    assert report["ok"], report.get("error")
    assert report["values"] == VALUES
    assert (table is None) is on_tmpfs


def test_the_full_ownership_fallback_is_accepted_on_a_read_only_mount(
    credential_directory: Callable[..., Path],
) -> None:
    """systemd's other delivery, whole: where no ACL can be held it chowns **both**.

    `exec-credential.c:206` chowns the file and `:738` chowns the directory, and systemd 255
    reaches that branch on any host whose kernel is older than 6.3, because it then mounts
    `ramfs`, which carries no POSIX ACLs at all (`mount-util.c:1655-1657`). Refusing this
    shape would be #230 again, one kernel away.
    """

    directory = credential_directory()
    _write(
        directory,
        uid=READER_UID,
        gid=READER_GID,
        mode=0o400,
        directory_mode=0o500,
        directory_owner=(READER_UID, READER_GID),
    )
    _seal(directory)

    report = _read_as(directory)

    assert report["ok"], report.get("error")
    assert report["values"] == VALUES


def test_the_ownership_fallback_is_refused_on_a_writable_mount(
    credential_directory: Callable[..., Path],
) -> None:
    """What makes that fallback safe is the read-only remount, so it is not optional."""

    directory = credential_directory()
    _write(
        directory,
        uid=READER_UID,
        gid=READER_GID,
        mode=0o400,
        directory_mode=0o500,
        directory_owner=(READER_UID, READER_GID),
    )

    report = _read_as(directory)

    assert not report["ok"]
    assert "remounts the credential directory read-only" in report["error"]
    assert "not read-only" in report["error"]


def test_a_writable_mount_is_refused_even_for_the_acl_delivery(
    credential_directory: Callable[..., Path],
) -> None:
    """systemd always remounts read-only, so a writable credential mount is nobody's."""

    directory = credential_directory()
    _write(directory)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "not read-only" in report["error"]


# ---------------------------------------------------------------------------------------
# Reverse: nothing systemd would not have produced gets in
# ---------------------------------------------------------------------------------------


def test_without_the_acl_on_the_credential_the_open_says_which_uid(
    credential_directory: Callable[..., Path],
) -> None:
    """Directory reachable, credential not: the file's own ACL is what is missing."""

    directory = credential_directory()
    _write(directory, mode=0o400)
    _seal(directory)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert f"not readable by uid {READER_UID}" in report["error"]
    assert "0:0 mode 0o0400" in report["error"]
    assert "ACL" in report["error"]
    assert "carries no" not in report["error"]


def test_without_the_acl_on_the_directory_the_refusal_names_the_directory(
    credential_directory: Callable[..., Path],
) -> None:
    """The real `User=` mistake: systemd adds both ACLs together, so both are missing.

    `acquire_credentials` writes every credential with its own ACL and then adds the
    directory's (`exec-credential.c:722-731`), so a unit whose `User=` does not match what
    the credential was loaded for cannot even walk into the directory — `lstat` of the file
    is denied too. That used to degrade into `systemd credential is unavailable or unsafe`,
    which names nothing and points nowhere.
    """

    directory = credential_directory()
    _write(directory, mode=0o440, directory_mode=0o500)
    _seal(directory)

    report = _read_as(directory)

    assert not report["ok"]
    assert f"cannot be entered by {READER_UID}:{READER_GID}" in report["error"]
    assert "0:0 mode 0o0500" in report["error"]
    assert "User=" in report["error"]
    assert "unavailable or unsafe" not in report["error"]


def test_a_group_readable_credential_outside_the_delivery_group_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """0440 is only ever the ACL mask over a root-owned file, never a shared group.

    Without this, "root wrote a 0440 whose group is the service's" reads as a delivery, and
    every account in that group can read the sealed capability.
    """

    directory = credential_directory()
    _write(directory, uid=0, gid=STRANGER_GID, mode=0o440)
    _seal(directory)

    report = _read_as(directory, gid=STRANGER_GID, extra_groups=(0,))

    assert not report["ok"]
    assert f"which is 0:0; observed 0:{STRANGER_GID}" in report["error"]


def test_a_credential_owned_by_a_third_party_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """Neither root nor the reader: no delivery systemd makes could have left this."""

    directory = credential_directory()
    _write(directory, uid=STRANGER_UID, gid=0, mode=0o440)
    _seal(directory)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "must be owned by uid 0 or by the runtime uid" in report["error"]
    assert f"observed {STRANGER_UID}" in report["error"]


def test_a_directory_owned_by_a_third_party_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """The directory has exactly two owners systemd ever gives it; this is neither."""

    directory = credential_directory()
    _write(directory, directory_owner=(STRANGER_UID, STRANGER_GID))
    _seal(directory)

    report = _read_as(directory, gid=0, extra_groups=(STRANGER_GID,))

    assert not report["ok"]
    assert "creates the credential directory as 0:0" in report["error"]
    assert f"observed {STRANGER_UID}:{STRANGER_GID}" in report["error"]


def test_a_world_readable_credential_is_refused(
    credential_directory: Callable[..., Path],
) -> None:
    """0444 is readable by every account on the host, which a sealed secret may never be."""

    directory = credential_directory()
    _write(directory, mode=0o444)
    _seal(directory)

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
    _seal(directory)

    report = _read_as(directory, gid=0)

    assert not report["ok"]
    assert "hardlink count must be one, observed 2" in report["error"]


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
    _seal(directory)

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
