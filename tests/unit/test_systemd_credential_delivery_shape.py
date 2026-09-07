"""#215, third break: the shape systemd really delivers a credential in.

Two breaks were repaired before this one. The wrapper dropped `CREDENTIALS_DIRECTORY` out of
the role child's environment, so a sealed credential never reached the role at all; and the
reader checked the credential against the authority chain's generation instead of the
deployment bundle's, the namespace it is actually sealed in. With both repaired, the first
window's five credstore units still refused to start, and this is why:

    systemd credential must be owned by the runtime uid

The unit runs `User=lighthouse` (uid 1001). systemd decrypts `LoadCredentialEncrypted=` as
**root** and admits the service user through a POSIX ACL, so the delivered file is
`root:root 0440` in a `root:root 0550` directory on systemd's own read-only, non-swappable
mount. The reader demanded `st_uid == os.geteuid()` and `mode & 0o077 == 0`, which describes
only the shape a unit running as root gets. Package E's e2e never caught it because its
fixture wrote the credential as "a file this process owns, mode 0400" — it modelled the
reader, not systemd.

Widening this to "any readable file" would have thrown away what the ownership rule was
carrying: the assurance that only systemd can have put that file there. So the rule is not
relaxed, it is replaced by systemd's own documented contract, clause by clause
(systemd.exec(5), CREDENTIALS and `$CREDENTIALS_DIRECTORY`):

* "An absolute path to the **per-unit** directory" — the directory is
  `/run/credentials/<unit>` and, when this process can tell which unit it is, that unit;
* "is placed in **unswappable memory**" — the mount under it is `tmpfs`/`ramfs`, and the
  mount table entry is cross-checked against the directory's own `st_dev`;
* "The directory is marked **read-only** … only accessible to the UID associated with the
  unit … (and the superuser)" — the directory belongs to root and grants nothing to others;
  the file belongs to root or to this process, carries no world bit, is 0400 or 0440, and
  may only be group-readable in the root-owned ACL delivery;
* "an accumulated credential size limit of 1 MB per unit" — the size bound, unchanged;
* "When loading from a directory, symlinks will be ignored" — `O_NOFOLLOW`, unchanged.

The three facts a non-root test cannot produce are named seams of the module, and
`tests/support/systemd_credential_delivery.py` is the only thing that moves them. The Linux
gate moves none of them.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

import rquant.runtime_capabilities as capabilities_module
from rquant.runtime_capabilities import (
    LoadedRuntimeCapabilities,
    load_systemd_runtime_capabilities,
    serialize_runtime_credential,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from tests.support.systemd_credential_delivery import (
    DEFAULT_UNIT,
    Delivery,
    deliver,
    empty_directory,
)

GENERATION = "b" * 64
SERVICE_ID = "source.daily-close"
INSTANCE = "svc-" + "a" * 64
KIND = RuntimeServiceKind.DAILY_CLOSE_SOURCE
VALUES = {"TUSHARE_TOKEN_MAIN": "sealed-token"}
PAYLOAD = serialize_runtime_credential(
    service_id=SERVICE_ID,
    service_kind=KIND,
    instance_name=INSTANCE,
    bundle_generation=GENERATION,
    values=VALUES,
)


def _load(directory: Path, environ: dict[str, str] | None = None):
    target = dict(environ or {})
    target["CREDENTIALS_DIRECTORY"] = str(directory)
    return load_systemd_runtime_capabilities(
        KIND,
        expected_service_id=SERVICE_ID,
        expected_instance=INSTANCE,
        expected_generation=GENERATION,
        environ=target,
    )


def _delivered(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, **overrides) -> Delivery:
    return deliver(monkeypatch, root=tmp_path / "run-credentials", payload=PAYLOAD, **overrides)


# ---------------------------------------------------------------------------------------
# The two shapes systemd actually produces
# ---------------------------------------------------------------------------------------


def test_the_acl_delivery_a_user_service_gets_is_accepted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """root-owned 0440 in a root-owned 0550 directory: the exact shape that refused to start."""

    delivery = _delivered(monkeypatch, tmp_path, mode=0o440, directory_mode=0o550)

    loaded = _load(delivery.directory)

    assert isinstance(loaded, LoadedRuntimeCapabilities)
    assert dict(loaded) == VALUES
    assert "sealed-token" not in repr(loaded)


def test_the_self_owned_delivery_a_root_service_gets_is_still_accepted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """0400 owned by the process: what the old rule described, and it did not stop being valid."""

    delivery = _delivered(monkeypatch, tmp_path, mode=0o400)

    assert dict(_load(delivery.directory)) == VALUES


@pytest.mark.parametrize("directory_mode", (0o500, 0o550, 0o700))
def test_every_directory_mode_systemd_uses_is_accepted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    directory_mode: int,
) -> None:
    """0700 before an ACL is needed, 0500/0550 once one is; nothing wider than those."""

    delivery = _delivered(monkeypatch, tmp_path, directory_mode=directory_mode)

    assert dict(_load(delivery.directory)) == VALUES


def test_a_ramfs_credential_mount_is_accepted_like_a_tmpfs_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both are the "unswappable memory" the manual promises; systemd has shipped both."""

    delivery = _delivered(monkeypatch, tmp_path, filesystem="ramfs")

    assert dict(_load(delivery.directory)) == VALUES


def test_the_unit_the_directory_names_may_be_this_process_own_unit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the cgroup does say which unit this is, agreeing with it is what passes."""

    cgroup = tmp_path / "cgroup"
    cgroup.write_text(f"0::/system.slice/system-rquant.slice/{DEFAULT_UNIT}\n", encoding="utf-8")
    monkeypatch.setattr(capabilities_module, "_SYSTEMD_CGROUP_PATH", cgroup)
    delivery = _delivered(monkeypatch, tmp_path, mode=0o440)

    assert dict(_load(delivery.directory)) == VALUES


# ---------------------------------------------------------------------------------------
# Reverse: one clause broken at a time, everything else a real delivery
# ---------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param(
            {"mode": 0o444},
            r"must not be world accessible, observed 0o0444",
            id="world-readable-file",
        ),
        pytest.param(
            {"mode": 0o600},
            r"mode must be 0o0400 or 0o0440, observed 0o0600",
            id="writable-file",
        ),
        pytest.param(
            {"directory_mode": 0o755},
            r"directory mode must be 0o0500 or 0o0550 or 0o0700, observed 0o0755",
            id="searchable-directory",
        ),
        pytest.param(
            {"directory_mode": 0o770},
            r"directory mode must be .*, observed 0o0770",
            id="group-writable-directory",
        ),
        pytest.param(
            {"filesystem": "ext4"},
            r"memory-backed mount that never reaches a disk; the mount at .* is ext4",
            id="disk-backed-mount",
        ),
        pytest.param(
            {"mount_options": "rw,nosuid,nodev,relatime"},
            r"is missing noexec: systemd mounts it nosuid,nodev,noexec",
            id="executable-mount",
        ),
        pytest.param(
            {"mount_options": "rw,noexec,relatime"},
            r"is missing nosuid, nodev",
            id="privilege-bearing-mount",
        ),
        pytest.param(
            {"unit": "rquant-runtime-daily-close@svc-" + "a" * 64},
            r"a systemd credential directory is .*/<unit>\.service, observed ",
            id="directory-is-not-a-unit",
        ),
    ],
)
def test_a_delivery_that_systemd_would_not_have_made_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    overrides: dict[str, object],
    expected: str,
) -> None:
    """Each case breaks one clause and leaves the other seven as a real delivery."""

    delivery = _delivered(monkeypatch, tmp_path, **overrides)

    with pytest.raises(ValueError, match=expected):
        _load(delivery.directory)


def test_a_directory_systemd_does_not_own_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The directory belongs to root on every host; a user-owned one is somebody's copy."""

    foreign = (os.geteuid() + 1, os.stat(tmp_path).st_gid)
    delivery = _delivered(monkeypatch, tmp_path, owner=foreign)

    with pytest.raises(ValueError, match=r"credential directory as \d+:\d+, observed \d+:\d+"):
        _load(delivery.directory)


def test_a_credential_outside_the_credentials_root_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`CREDENTIALS_DIRECTORY` is an address, not an authorisation: it has to be systemd's."""

    _delivered(monkeypatch, tmp_path)
    elsewhere = tmp_path / "elsewhere" / DEFAULT_UNIT
    elsewhere.mkdir(parents=True)

    with pytest.raises(ValueError, match=r"a systemd credential directory is "):
        _load(elsewhere)


def test_another_units_credential_directory_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Per-unit means per-unit: the cgroup says which one this process is."""

    cgroup = tmp_path / "cgroup"
    cgroup.write_text(
        "0::/system.slice/system-rquant.slice/rquant-runtime-notifier@svc-"
        + "b" * 64
        + ".service\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(capabilities_module, "_SYSTEMD_CGROUP_PATH", cgroup)
    delivery = _delivered(monkeypatch, tmp_path)

    with pytest.raises(ValueError, match=r"belongs to unit .* while this process runs under "):
        _load(delivery.directory)


def test_a_mount_table_that_names_another_device_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The entry has to be the directory's own mount, not a line that merely looks right."""

    delivery = _delivered(monkeypatch, tmp_path)
    table = capabilities_module._SYSTEMD_MOUNT_TABLE
    table.write_text(
        table.read_text(encoding="utf-8").replace(
            f"{os.major(os.stat(delivery.root).st_dev)}:"
            f"{os.minor(os.stat(delivery.root).st_dev)} /",
            "0:999 /",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"mount table disagrees with the credential directory"):
        _load(delivery.directory)


def test_an_unreadable_mount_table_refuses_rather_than_assumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fail closed: an unverifiable mount is not a verified one."""

    delivery = _delivered(monkeypatch, tmp_path)
    monkeypatch.setattr(capabilities_module, "_SYSTEMD_MOUNT_TABLE", tmp_path / "absent")

    with pytest.raises(ValueError, match=r"credential mount cannot be read from "):
        _load(delivery.directory)


def test_a_hardlinked_credential_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """systemd writes one link; a second is a copy somebody kept elsewhere."""

    delivery = _delivered(monkeypatch, tmp_path)
    delivery.directory.chmod(0o700)
    os.link(delivery.path, delivery.directory / "kept.json")
    delivery.directory.chmod(0o550)

    with pytest.raises(ValueError, match=r"hardlink count must be one, observed 2"):
        _load(delivery.directory)


def test_a_symlinked_credential_is_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`O_NOFOLLOW`, and the manual's own "symlinks will be ignored"."""

    delivery = _delivered(monkeypatch, tmp_path)
    delivery.directory.chmod(0o700)
    real = delivery.directory / "real.json"
    delivery.path.replace(real)
    delivery.path.symlink_to(real)
    delivery.directory.chmod(0o550)

    with pytest.raises(ValueError, match=r"unavailable or unsafe"):
        _load(delivery.directory)


@pytest.mark.skipif(os.geteuid() == 0, reason="root is never refused by a file mode")
def test_a_credential_no_access_control_entry_admits_names_the_acl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure a wrong `User=` produces is in the open, and it has to say so.

    On the host the grant is a POSIX ACL, which no test on macOS can write; a mode with no
    owner bit produces the same `EACCES` from the same `os.open`, and the Linux gate does it
    with a real missing ACL.
    """

    delivery = _delivered(monkeypatch, tmp_path)
    delivery.directory.chmod(0o700)
    delivery.path.chmod(0o000)
    delivery.directory.chmod(0o550)

    with pytest.raises(ValueError, match=r"is not readable by uid \d+: it is \d+:\d+ mode 0o0000"):
        _load(delivery.directory)


# ---------------------------------------------------------------------------------------
# The message that only applies when the directory itself is sound
# ---------------------------------------------------------------------------------------


def test_a_sound_directory_without_the_named_credential_still_accuses_the_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """systemd made the directory and loaded some other id into it."""

    delivery = empty_directory(monkeypatch, root=tmp_path / "run-credentials")

    with pytest.raises(ValueError, match=r"carries no capabilities\.json"):
        _load(delivery.directory)


def test_an_unsound_directory_reports_itself_rather_than_the_missing_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Otherwise the wrong repair gets attempted: the name is fine, the directory is not."""

    delivery = _delivered(monkeypatch, tmp_path, name="other.json", filesystem="ext4")

    with pytest.raises(ValueError, match=r"memory-backed mount"):
        _load(delivery.directory)
