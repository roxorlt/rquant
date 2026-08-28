"""The narrow root-owned launcher that replaces every production `preexec_fn`.

Codex round-2 P1-5 removes the Python callable that ran between `fork` and `exec`. The
replacement is `/usr/bin/setpriv`: the identity change, the supplementary-group clear and
`no-new-privs` are performed by a root-owned util-linux binary that this repository does
not write, and the descriptor closure is left to CPython's own C-level sweep.

The binary is TCB, so it is identified the way every other TCB file is: regular, not a
symlink, single link, owned by the expected owner, no group or world write bit, executable
by its owner, and reached through a non-writable ancestor chain. macOS ships no `setpriv`,
so every test here materialises its own launcher file and injects the expected owner.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest

from rquant import privilege_launcher as launcher


def _launcher_file(root: Path, *, name: str = "setpriv", mode: int = 0o555) -> Path:
    path = root / name
    path.write_text("#!/bin/sh\nexec \"$@\"\n", encoding="utf-8")
    path.chmod(mode)
    return path


@pytest.fixture()
def trusted_root(tmp_path: Path) -> Path:
    root = tmp_path / "usr"
    root.mkdir(mode=0o755)
    root.chmod(0o755)
    return root


class TestProductionConstants:
    def test_the_production_launcher_is_the_fixed_util_linux_binary(self) -> None:
        assert Path("/usr/bin/setpriv") == launcher.PRODUCTION_PRIVILEGE_LAUNCHER

    def test_the_launcher_flags_are_frozen_and_ordered(self) -> None:
        assert launcher.PRIVILEGE_DROP_FLAGS == (
            "--reuid",
            "--regid",
            "--clear-groups",
            "--no-new-privs",
        )

    def test_the_launcher_is_declared_a_trusted_computing_base_entry(self) -> None:
        entry = launcher.PRIVILEGE_LAUNCHER_TCB_ENTRY

        assert entry.path == Path("/usr/bin/setpriv")
        assert entry.owner_uid == 0
        assert entry.owner_gid == 0
        assert entry.forbidden_mode_bits == 0o022
        assert entry.required_mode_bits == 0o100


class TestArgvConstruction:
    def test_the_drop_argv_is_the_exact_frozen_sequence(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)

        argv = launcher.build_privilege_drop_argv(
            launcher_path=path,
            target_uid=1000,
            target_gid=1001,
            command=("/gen/bin/python", "-I", "/usr/local/libexec/harness.pyz"),
        )

        assert argv == (
            str(path),
            "--reuid",
            "1000",
            "--regid",
            "1001",
            "--clear-groups",
            "--no-new-privs",
            "--",
            "/gen/bin/python",
            "-I",
            "/usr/local/libexec/harness.pyz",
        )

    def test_the_drop_argv_refuses_a_root_target(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)

        for uid, gid in ((0, 1000), (1000, 0), (0, 0)):
            with pytest.raises(launcher.PrivilegeLauncherError, match="never run as root"):
                launcher.build_privilege_drop_argv(
                    launcher_path=path,
                    target_uid=uid,
                    target_gid=gid,
                    command=("/gen/bin/python",),
                )

    def test_the_drop_argv_refuses_an_empty_or_relative_command(
        self,
        trusted_root: Path,
    ) -> None:
        path = _launcher_file(trusted_root)

        with pytest.raises(launcher.PrivilegeLauncherError, match="one absolute command"):
            launcher.build_privilege_drop_argv(
                launcher_path=path,
                target_uid=1000,
                target_gid=1000,
                command=(),
            )
        with pytest.raises(launcher.PrivilegeLauncherError, match="one absolute command"):
            launcher.build_privilege_drop_argv(
                launcher_path=path,
                target_uid=1000,
                target_gid=1000,
                command=("python",),
            )

    def test_the_drop_argv_refuses_a_relative_launcher(self) -> None:
        with pytest.raises(launcher.PrivilegeLauncherError, match="absolute"):
            launcher.build_privilege_drop_argv(
                launcher_path=Path("setpriv"),
                target_uid=1000,
                target_gid=1000,
                command=("/gen/bin/python",),
            )

    def test_the_parent_death_argv_is_the_exact_frozen_sequence(
        self,
        trusted_root: Path,
    ) -> None:
        path = _launcher_file(trusted_root)

        argv = launcher.build_parent_death_argv(
            launcher_path=path,
            command=("/home/lighthouse/rquant/.venv/bin/python", "-m", "rquant.x"),
        )

        assert argv == (
            str(path),
            "--pdeathsig",
            "SIGKILL",
            "--",
            "/home/lighthouse/rquant/.venv/bin/python",
            "-m",
            "rquant.x",
        )

    def test_the_parent_death_signal_is_frozen_at_sigkill(self) -> None:
        assert launcher.PARENT_DEATH_SIGNAL_NAME == "SIGKILL"


class TestLauncherIdentity:
    def test_a_root_owned_launcher_is_accepted_and_hashed(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)

        identity = launcher.verify_privilege_launcher(
            path,
            expected_owner_uid=os.getuid(),
            expected_owner_gid=path.stat().st_gid,
            trusted_root=trusted_root.parent,
        )

        assert identity.path == path
        assert identity.mode == 0o555
        assert identity.owner_uid == os.getuid()
        assert len(identity.sha256) == 64
        assert identity.size == path.stat().st_size

    def test_a_matching_hash_pin_is_accepted(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)
        identity = launcher.verify_privilege_launcher(
            path,
            expected_owner_uid=os.getuid(),
            expected_owner_gid=path.stat().st_gid,
            trusted_root=trusted_root.parent,
        )

        again = launcher.verify_privilege_launcher(
            path,
            expected_owner_uid=os.getuid(),
            expected_owner_gid=path.stat().st_gid,
            trusted_root=trusted_root.parent,
            expected_sha256=identity.sha256,
        )

        assert again.sha256 == identity.sha256

    def test_a_mismatched_hash_pin_rejects(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)

        with pytest.raises(launcher.PrivilegeLauncherError, match="hash"):
            launcher.verify_privilege_launcher(
                path,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=path.stat().st_gid,
                trusted_root=trusted_root.parent,
                expected_sha256="0" * 64,
            )

    def test_a_missing_launcher_rejects(self, trusted_root: Path) -> None:
        with pytest.raises(launcher.PrivilegeLauncherError, match="not present"):
            launcher.verify_privilege_launcher(
                trusted_root / "setpriv",
                expected_owner_uid=os.getuid(),
                expected_owner_gid=os.getgid(),
                trusted_root=trusted_root.parent,
            )

    def test_a_symlinked_launcher_rejects(self, trusted_root: Path) -> None:
        real = _launcher_file(trusted_root, name="setpriv.real")
        link = trusted_root / "setpriv"
        link.symlink_to(real)

        with pytest.raises(launcher.PrivilegeLauncherError, match="regular file"):
            launcher.verify_privilege_launcher(
                link,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=real.stat().st_gid,
                trusted_root=trusted_root.parent,
            )

    def test_a_hardlinked_launcher_rejects(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)
        os.link(path, trusted_root / "setpriv.alias")

        with pytest.raises(launcher.PrivilegeLauncherError, match="single link"):
            launcher.verify_privilege_launcher(
                path,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=path.stat().st_gid,
                trusted_root=trusted_root.parent,
            )

    def test_a_group_or_world_writable_launcher_rejects(self, trusted_root: Path) -> None:
        for mode in (0o575, 0o557):
            path = _launcher_file(trusted_root, name=f"setpriv{mode:o}", mode=mode)
            with pytest.raises(launcher.PrivilegeLauncherError, match="writable"):
                launcher.verify_privilege_launcher(
                    path,
                    expected_owner_uid=os.getuid(),
                    expected_owner_gid=path.stat().st_gid,
                    trusted_root=trusted_root.parent,
                )

    def test_a_non_executable_launcher_rejects(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root, mode=0o444)

        with pytest.raises(launcher.PrivilegeLauncherError, match="executable"):
            launcher.verify_privilege_launcher(
                path,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=path.stat().st_gid,
                trusted_root=trusted_root.parent,
            )

    def test_a_foreign_owner_rejects(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)

        with pytest.raises(launcher.PrivilegeLauncherError, match="owner"):
            launcher.verify_privilege_launcher(
                path,
                expected_owner_uid=os.getuid() + 4242,
                expected_owner_gid=path.stat().st_gid,
                trusted_root=trusted_root.parent,
            )

    def test_a_group_writable_ancestor_rejects(self, trusted_root: Path) -> None:
        path = _launcher_file(trusted_root)
        trusted_root.chmod(0o775)

        with pytest.raises(launcher.PrivilegeLauncherError, match="writable"):
            launcher.verify_privilege_launcher(
                path,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=path.stat().st_gid,
                trusted_root=trusted_root.parent,
            )

    def test_a_launcher_outside_the_trusted_root_rejects(self, tmp_path: Path) -> None:
        elsewhere = tmp_path / "elsewhere"
        elsewhere.mkdir(mode=0o755)
        path = _launcher_file(elsewhere)

        with pytest.raises(launcher.PrivilegeLauncherError, match="trusted root"):
            launcher.verify_privilege_launcher(
                path,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=path.stat().st_gid,
                trusted_root=tmp_path / "usr",
            )


class TestDescriptorClosure:
    """`close_fds=True` is now the only descriptor sweep; the assertion replaces the code."""

    def test_the_retained_set_is_exactly_the_standard_streams_and_the_pipes(self) -> None:
        assert launcher.retained_descriptors((7, 9)) == (0, 1, 2, 7, 9)
        assert launcher.retained_descriptors(()) == (0, 1, 2)

    def test_the_retained_set_refuses_a_standard_stream(self) -> None:
        with pytest.raises(launcher.PrivilegeLauncherError, match="standard stream"):
            launcher.retained_descriptors((2, 5))

    def test_the_closure_assertion_accepts_a_consistent_sweep(self) -> None:
        launcher.assert_descriptor_closure(pass_fds=(7, 9), limit=20)

    def test_the_closure_assertion_refuses_a_bound_below_a_retained_descriptor(self) -> None:
        with pytest.raises(launcher.PrivilegeLauncherError, match="bound"):
            launcher.assert_descriptor_closure(pass_fds=(7, 9), limit=8)


class TestNoPreexecRemains:
    def test_the_launcher_module_never_names_preexec(self) -> None:
        source = Path(launcher.__file__).read_text(encoding="utf-8")

        assert "preexec_fn=" not in source

    def test_the_module_performs_no_identity_syscall_of_its_own(self) -> None:
        source = Path(launcher.__file__).read_text(encoding="utf-8")

        code = "\n".join(
            line for line in source.splitlines() if not line.lstrip().startswith("#")
        )
        body = code.split('"""', 2)[2]

        for forbidden in ("os.setresuid", "os.setresgid", "os.setgroups", "ctypes"):
            assert forbidden not in body


def test_the_mode_predicates_are_expressed_as_stat_bits() -> None:
    entry = launcher.PRIVILEGE_LAUNCHER_TCB_ENTRY

    assert entry.forbidden_mode_bits == stat.S_IWGRP | stat.S_IWOTH
    assert entry.required_mode_bits == stat.S_IXUSR
