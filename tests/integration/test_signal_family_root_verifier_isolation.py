"""`RESET-REG-P0-01`: the OS separation between the root verifier and its child.

authority.md L1425-1443 gives the unprivileged child a sanitized fixed environment, fixed
argv and cwd, no supplementary privilege, and closed inherited descriptors except the two
canonical IPC pipes. It receives no store descriptor, store path, store capability,
verifier-module import path, root module object, or caller-supplied Python path, and its
one bounded canonical response is the only thing the root reads back.

The child here is the WP4-b stub harness. It writes an introspection report to a location
baked into its own zipapp at build time — never to a path the root hands it — so the
assertions below observe what the child could actually reach, not what the root claims.
"""

from __future__ import annotations

import os
import sqlite3
import stat
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from rquant import signal_family_root_verifier as root_verifier
from rquant import signal_family_verification as verification
from tests.integration.signal_family_verifier_world import (
    VerifierWorld,
    build_world,
    digest,
)
from tests.support import signal_family_private_root as _private_root

# The Phase C ancestry walks refuse a group- or world-writable ancestor, and pytest's own
# `tmp_path` is rooted at `TMPDIR`, which defaults to a sticky `/tmp` on Linux. Rebinding both
# fixture names here roots every temporary directory in a verified-private `$HOME` root for
# this module only, and fails loudly with the offending directory if that root is not private.
signal_family_private_root = _private_root.signal_family_private_root
tmp_path = _private_root.tmp_path
pytestmark = pytest.mark.integration

VERIFIED_AT = datetime(2026, 8, 24, 7, 30, 15, 250000, tzinfo=UTC)


def _verifier(world: VerifierWorld, **overrides: Any) -> root_verifier.RootVerifier:
    return root_verifier.RootVerifier(
        anchors=overrides.pop("anchors", world.anchors),
        authority_gateway=world.gateway,
        clock=lambda: VERIFIED_AT,
        **overrides,
    )


def _rows(world: VerifierWorld, table: str) -> list[tuple[Any, ...]]:
    if not world.store_database.exists():
        return []
    connection = sqlite3.connect(f"file:{world.store_database}?mode=ro", uri=True)
    try:
        return list(connection.execute(f"SELECT * FROM {table}"))
    finally:
        connection.close()


# ---------------------------------------------------------------------------------------
# What the child actually receives
# ---------------------------------------------------------------------------------------


class TestChildContainment:
    #: CoreFoundation stamps this into every process it initializes on macOS. The root
    #: never passes it; it appears inside the child after exec and is outside any
    #: launcher's control, so it is excluded from the exactness assertion by name.
    PLATFORM_INJECTED_ENV_KEYS = frozenset({"__CF_USER_TEXT_ENCODING"})

    def test_the_child_environment_is_exactly_the_frozen_allowlist(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        passed = root_verifier.child_environment(
            cwd=tmp_path,
            request_fd=3,
            result_fd=4,
        )
        assert tuple(sorted(passed)) == root_verifier.SIGNAL_FAMILY_CHILD_ENV_KEYS

        _verifier(world).run()
        report = world.read_report()
        observed = set(report["environ"]) - self.PLATFORM_INJECTED_ENV_KEYS

        assert tuple(sorted(observed)) == root_verifier.SIGNAL_FAMILY_CHILD_ENV_KEYS

    def test_the_child_configuration_environment_is_root_fixed_and_cwd_anchored(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """WP4-c round 1: `rquant.config` builds a `Settings` at import, so the child needs
        those five keys to construct any notifier or serving builder. The root chooses the
        values; the caller's own environment can never reach the child."""

        for key in root_verifier.CHILD_CONFIGURATION_ENV_KEYS:
            monkeypatch.setenv(key, f"/leaked/{key}")
        cwd = tmp_path / "child"
        cwd.mkdir()

        passed = root_verifier.child_environment(cwd=cwd, request_fd=3, result_fd=4)

        assert set(root_verifier.CHILD_CONFIGURATION_ENV_KEYS).issubset(passed)
        for value in passed.values():
            assert "/leaked/" not in value
        for key in root_verifier.CHILD_CONFIGURATION_PATH_ENV_KEYS:
            assert Path(passed[key]).is_relative_to(cwd)
        assert passed["TUSHARE_TOKEN_MAIN"] == root_verifier.CHILD_ABSENT_CREDENTIAL_VALUE

    def test_the_child_receives_no_python_path_and_runs_isolated(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        assert "PYTHONPATH" not in report["environ"]
        assert "PYTHONHOME" not in report["environ"]
        assert report["flags_isolated"] is True

    def test_the_child_argv_is_the_fixed_interpreter_isolation_harness_triple(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        interpreter = world.gateway.snapshots[0].slot.roles["router"].python_path
        assert root_verifier.build_child_argv(interpreter, world.harness_path) == (
            str(interpreter),
            "-I",
            str(world.harness_path),
        )
        _verifier(world).run()
        report = world.read_report()

        assert report["argv"] == [str(world.harness_path)]

    def test_the_child_cwd_is_an_empty_private_directory_removed_afterwards(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        cwd = Path(report["cwd"])
        assert report["cwd_entries"] == []
        assert cwd.exists() is False
        assert world.store_root not in cwd.parents
        assert world.generation_path not in cwd.parents

    def test_the_child_cwd_has_no_group_or_world_writable_ancestor(
        self,
        tmp_path: Path,
    ) -> None:
        """WP4-c round 1: a sticky ancestor is what blocks the serving-authority readers.

        `runtime_serving_authority` walks an authority root's whole ancestry and refuses any
        group- or world-writable node, so a child cwd created by a bare `tempfile.mkdtemp()`
        makes three reader surfaces unreachable the moment `TMPDIR` is unset and the default
        temp root is a sticky `1777` `/tmp`. macOS hides this — its per-user temp root is
        `0700` — so the property is asserted on the reported ancestry rather than inferred
        from a run that happened to succeed.
        """

        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        writable = [
            (path, oct(mode))
            for path, mode, _uid in report["cwd_ancestor_modes"]
            if mode & (stat.S_IWGRP | stat.S_IWOTH)
        ]
        assert writable == []

    def test_a_group_writable_workspace_ancestor_stops_the_run_before_the_child(
        self,
        tmp_path: Path,
    ) -> None:
        """The rejection itself, not the property of a fixture that happened to be private.

        `test_the_child_cwd_has_no_group_or_world_writable_ancestor` reads the ancestry the
        child reported, so it can only observe a run that already succeeded. Removing the
        ancestry rule from `open_child_workspace_root` leaves that assertion green. This
        drives the real verifier at a workspace under a `0775` parent and requires the run to
        stop with `CHILD_LAUNCH_FAILED` before any child process exists.
        """

        world = build_world(tmp_path)
        loose = tmp_path / "loose"
        loose.mkdir(mode=0o755)
        loose.chmod(0o775)  # `mkdir` would have masked this with the umask
        anchors = replace(world.anchors, child_workspace_root=loose / "workspace")

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_LAUNCH_FAILED",
        ) as raised:
            _verifier(world, anchors=anchors).run()

        assert "group or world writable" in str(raised.value)
        assert world.report_path.exists() is False
        assert list((loose / "workspace").iterdir()) == []

    def test_the_child_cwd_lives_in_the_verifier_owned_workspace(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        cwd = Path(report["cwd"])
        workspace = world.anchors.child_workspace_root

        assert workspace in cwd.parents
        assert workspace.is_dir()
        assert stat.S_IMODE(workspace.stat().st_mode) == root_verifier.CHILD_WORKSPACE_MODE
        assert list(workspace.iterdir()) == []

    def test_the_workspace_admits_a_dropped_privilege_child(self, tmp_path: Path) -> None:
        """The production shape, read off the privilege-plan seam rather than assumed.

        Locally the verifier and the child share a uid, so `child_privilege_plan` returns
        `None` and the drop never happens — the failure mode this guards is structurally
        invisible in any same-uid test. The seam is driven with the production identities
        (root verifier, `lighthouse` child) so the mode is judged against the class of bits
        that will actually apply: `other`.
        """

        plan = root_verifier.child_privilege_plan(
            current_uid=0,
            current_gid=0,
            target_uid=1000,
            target_gid=1000,
        )
        assert plan is not None, "production drops privilege; the mode must survive that"

        other = root_verifier.CHILD_WORKSPACE_MODE & 0o007
        assert other == stat.S_IROTH | stat.S_IXOTH

    def test_the_workspace_mode_grants_read_and_traversal_without_write(self) -> None:
        """What the two ancestry walks actually need: `O_RDONLY | O_DIRECTORY` on the way in.

        Execute alone is not enough — `0711` passed every bit-level check in fix round 1 and
        still failed all eight surfaces, because opening a directory read-only requires the
        read bit. What must hold is read plus execute for the child's class, and no write bit
        for anyone but the owner, which is what both ancestry rules refuse.
        """

        mode = root_verifier.CHILD_WORKSPACE_MODE

        assert mode & stat.S_IROTH
        assert mode & stat.S_IXOTH
        assert not mode & (stat.S_IWGRP | stat.S_IWOTH)
        assert mode & stat.S_IRWXU == stat.S_IRWXU

    def test_a_workspace_without_the_traversal_bit_denies_absolute_paths(
        self,
        tmp_path: Path,
    ) -> None:
        """The mechanism itself, demonstrated with real syscalls under one uid.

        A uid switch is not available here, but the kernel rule that bites in production is
        not about identity — it is that resolving an absolute path needs the execute bit on
        every component. Removing the owner's own execute bit reproduces exactly that, and
        shows what the child would hit under `0700` once it is no longer root.
        """

        workspace = tmp_path / "workspace"
        workspace.mkdir(mode=0o711)
        child_cwd = workspace / "child"
        child_cwd.mkdir(mode=0o700)
        (child_cwd / "state.txt").write_text("hi", encoding="utf-8")

        assert (child_cwd / "state.txt").read_text(encoding="utf-8") == "hi"

        workspace.chmod(0o600)
        try:
            with pytest.raises(PermissionError):
                (child_cwd / "state.txt").read_text(encoding="utf-8")
        finally:
            workspace.chmod(0o711)

    def test_the_child_holds_only_the_standard_streams_and_the_two_ipc_pipes(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        expected = sorted({0, 1, 2, report["request_fd"], report["result_fd"]})
        assert report["open_descriptors"] == expected
        assert report["request_fd"] not in (0, 1, 2)
        assert report["result_fd"] not in (0, 1, 2)

    def test_no_child_input_names_the_store_root_or_the_verifier_module(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        reachable = [
            *report["argv"],
            report["cwd"],
            *report["environ"].values(),
            *report["sys_path"],
        ]
        needles = (
            str(world.store_root),
            root_verifier.STORE_DATABASE_NAME,
            "signal_family_root_verifier",
        )
        for value in reachable:
            for needle in needles:
                assert needle not in value

    def test_the_child_cannot_import_the_privileged_verifier_module(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        assert report["imports"]["rquant"] == "ModuleNotFoundError"
        assert report["imports"]["verifier"] == "ModuleNotFoundError"

    def test_the_request_the_child_reads_carries_no_expected_result(
        self,
        tmp_path: Path,
    ) -> None:
        import hashlib
        import json

        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        request = root_verifier.build_child_request(
            run_id=root_verifier.derive_run_id(
                authority_epoch_key=world.authority_epoch_key,
                overlay_content_hash=world.overlay_content_hash,
                test_manifest_hash=world.test_manifest_sha256,
                vector_set_hash=world.entry.vector_set_hash,
            ),
            test_manifest_hash=world.test_manifest_sha256,
            vectors=world.test_manifest.vectors,
        )
        assert hashlib.sha256(request).hexdigest() == report["request_sha256"]
        decoded = json.loads(request)
        assert set(decoded) == {"schema_version", "run_id", "test_manifest_hash", "vectors"}
        for vector in decoded["vectors"]:
            assert set(vector) == {
                "vector_id",
                "pair_id",
                "family_id",
                "surface_id",
                "input_json",
            }
        assert b"canonical_result_sha256" not in request
        assert len(request) <= root_verifier.MAX_REQUEST_BYTES

    def test_the_child_privilege_plan_always_drops_root(self) -> None:
        plan = root_verifier.child_privilege_plan(
            current_uid=0,
            current_gid=0,
            target_uid=1000,
            target_gid=1000,
        )
        assert plan is not None
        assert plan.setresuid == (1000, 1000, 1000)
        assert plan.setresgid == (1000, 1000, 1000)
        assert plan.clear_supplementary_groups is True

    def test_the_child_privilege_plan_refuses_to_keep_root(self) -> None:
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_LAUNCH_FAILED",
        ):
            root_verifier.child_privilege_plan(
                current_uid=0,
                current_gid=0,
                target_uid=0,
                target_gid=0,
            )

    def test_an_unprivileged_verifier_needs_no_plan_for_its_own_identity(self) -> None:
        assert (
            root_verifier.child_privilege_plan(
                current_uid=501,
                current_gid=20,
                target_uid=501,
                target_gid=20,
            )
            is None
        )

    def test_an_unprivileged_verifier_cannot_reach_another_identity(self) -> None:
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_LAUNCH_FAILED",
        ):
            root_verifier.child_privilege_plan(
                current_uid=501,
                current_gid=20,
                target_uid=1000,
                target_gid=1000,
            )


# ---------------------------------------------------------------------------------------
# The bounded IPC contract
# ---------------------------------------------------------------------------------------


class TestBoundedIpc:
    @pytest.mark.parametrize(
        ("mode", "reason"),
        (
            ("extra_output", "CHILD_EXTRA_OUTPUT"),
            ("stderr_output", "CHILD_EXTRA_OUTPUT"),
            ("nonzero", "CHILD_NONZERO_EXIT"),
            ("signal", "CHILD_SIGNAL_DEATH"),
            ("trailing", "CHILD_RESULT_NONCANONICAL"),
            ("noncanonical", "CHILD_RESULT_NONCANONICAL"),
            ("oversized", "CHILD_RESULT_OVERSIZED"),
            ("forged_hash", "CHILD_RESULT_NONCANONICAL"),
            ("unsorted", "CHILD_RESULT_NONCANONICAL"),
            ("missing_vector", "CHILD_RESULT_IDENTITY_MISMATCH"),
            ("unknown_vector", "CHILD_RESULT_IDENTITY_MISMATCH"),
            ("wrong_result", "RESULT_SET_HASH_MISMATCH"),
        ),
    )
    def test_a_child_response_outside_the_frozen_contract_rejects(
        self,
        tmp_path: Path,
        mode: str,
        reason: str,
    ) -> None:
        world = build_world(tmp_path, harness_mode=mode)
        with pytest.raises(root_verifier.SignalFamilyRootVerifierError, match=reason):
            _verifier(world).run()
        assert _rows(world, "receipts") == []
        assert _rows(world, "decisions") == []

    def test_a_child_that_exceeds_its_deadline_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path, harness_mode="timeout")
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_TIMEOUT",
        ):
            _verifier(world, child_timeout_seconds=2.0).run()
        assert _rows(world, "receipts") == []

    def test_a_child_that_leaves_the_result_pipe_open_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path, harness_mode="open_pipe")
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_DESCRIPTOR_MISMATCH",
        ):
            _verifier(world, child_timeout_seconds=3.0).run()
        assert _rows(world, "receipts") == []

    def test_a_child_run_id_the_root_did_not_derive_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path, run_id_override=digest("forged-run"))
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_RESULT_IDENTITY_MISMATCH",
        ):
            _verifier(world).run()

    def test_a_child_test_manifest_hash_the_root_did_not_derive_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path, test_manifest_hash_override=digest("forged-manifest"))
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_RESULT_IDENTITY_MISMATCH",
        ):
            _verifier(world).run()

    def test_the_root_bounds_the_response_at_one_mebibyte(self) -> None:
        assert verification.MAX_IPC_RESPONSE_BYTES == 1_048_576
        assert root_verifier.MAX_REQUEST_BYTES == 1_048_576

    def test_the_default_child_deadline_is_the_frozen_verifier_constant(self) -> None:
        assert root_verifier.CHILD_TIMEOUT_SECONDS == 600


# ---------------------------------------------------------------------------------------
# The launch wiring itself, not just the plan objects
# ---------------------------------------------------------------------------------------


class TestChildLaunchWiring:
    """P1-5: the launch carries a `setpriv` launcher, and no `preexec_fn` at all."""

    def test_the_retained_descriptor_set_is_the_streams_plus_the_two_pipes(self) -> None:
        assert root_verifier.child_descriptor_sweep((7, 9), limit=20) == (0, 1, 2, 7, 9)
        assert root_verifier.child_descriptor_sweep((3, 4), limit=12) == (0, 1, 2, 3, 4)
        assert root_verifier.child_descriptor_sweep((), limit=8) == (0, 1, 2)

    def test_the_retained_descriptor_set_rejects_a_standard_stream_or_a_bad_bound(
        self,
    ) -> None:
        for keep, limit in (((2, 5), 20), ((5,), 5), ((-1,), 20)):
            with pytest.raises(
                root_verifier.SignalFamilyRootVerifierError,
                match="CHILD_LAUNCH_FAILED",
            ):
                root_verifier.child_descriptor_sweep(keep, limit=limit)

    def test_the_child_is_launched_with_the_exact_containment_wiring(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """No `preexec_fn` reaches `Popen`, and `close_fds` plus `pass_fds` do the sweep.

        The Python callable that used to run between `fork` and `exec` is gone (Codex
        round-2 P1-5). What remains observable at the launch seam is the argv, the fixed
        environment, the two retained descriptors, and the absence of the callable.
        """

        world = build_world(tmp_path)
        captured: dict[str, Any] = {}
        original = root_verifier.subprocess.Popen

        def recording_popen(*args: Any, **kwargs: Any) -> Any:
            captured["args"] = args
            captured["kwargs"] = kwargs
            return original(*args, **kwargs)

        monkeypatch.setattr(root_verifier.subprocess, "Popen", recording_popen)
        _verifier(world).run()

        kwargs = captured["kwargs"]
        assert "preexec_fn" not in kwargs
        assert kwargs["close_fds"] is True
        assert kwargs["stdin"] is root_verifier.subprocess.DEVNULL
        assert kwargs["stdout"] is root_verifier.subprocess.PIPE
        assert kwargs["stderr"] is root_verifier.subprocess.PIPE
        assert tuple(sorted(kwargs["env"])) == root_verifier.SIGNAL_FAMILY_CHILD_ENV_KEYS
        assert set(kwargs["pass_fds"]) == {
            int(kwargs["env"][root_verifier.CHILD_REQUEST_ENV_KEY]),
            int(kwargs["env"][root_verifier.CHILD_RESULT_ENV_KEY]),
        }
        assert captured["args"][0] == root_verifier.build_child_argv(
            world.gateway.snapshots[0].slot.roles["router"].python_path,
            world.harness_path,
        )
        assert root_verifier.child_descriptor_sweep(
            tuple(kwargs["pass_fds"]),
            limit=root_verifier.child_descriptor_limit(),
        ) == (0, 1, 2, *sorted(kwargs["pass_fds"]))

    def test_the_launch_argv_is_wrapped_in_the_launcher_when_the_verifier_is_root(
        self,
        tmp_path: Path,
    ) -> None:
        """The production shape: `setpriv` performs the drop, not a Python callable.

        Locally the verifier and the child share a uid, so the real launch never drops.
        The seam is driven with the production identities and a launcher this test owns.
        """

        world = build_world(tmp_path)
        binary = world.root / "usr" / "bin" / "setpriv"
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text('#!/bin/sh\nexec "$@"\n', encoding="utf-8")
        binary.chmod(0o555)
        plan = root_verifier.child_privilege_plan(
            current_uid=0,
            current_gid=0,
            target_uid=1000,
            target_gid=1000,
        )
        assert plan is not None

        argv = root_verifier.build_launch_argv(
            plan,
            command=("/gen/bin/python", "-I", "/opt/harness.pyz"),
            launcher_path=binary,
            expected_owner_uid=os.getuid(),
            expected_owner_gid=binary.stat().st_gid,
            trusted_root=world.root,
        )

        assert argv == (
            str(binary),
            "--reuid",
            "1000",
            "--regid",
            "1000",
            "--clear-groups",
            "--no-new-privs",
            "--",
            "/gen/bin/python",
            "-I",
            "/opt/harness.pyz",
        )

    def test_a_launch_without_a_plan_executes_the_command_unwrapped(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)

        argv = root_verifier.build_launch_argv(
            None,
            command=("/gen/bin/python", "-I", "/opt/harness.pyz"),
            launcher_path=world.root / "usr" / "bin" / "setpriv",
            expected_owner_uid=os.getuid(),
            expected_owner_gid=os.getgid(),
            trusted_root=world.root,
        )

        assert argv == ("/gen/bin/python", "-I", "/opt/harness.pyz")

    def test_a_root_launch_without_a_usable_launcher_refuses(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        plan = root_verifier.child_privilege_plan(
            current_uid=0,
            current_gid=0,
            target_uid=1000,
            target_gid=1000,
        )

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_LAUNCH_FAILED",
        ):
            root_verifier.build_launch_argv(
                plan,
                command=("/gen/bin/python",),
                launcher_path=world.root / "usr" / "bin" / "setpriv",
                expected_owner_uid=os.getuid(),
                expected_owner_gid=os.getgid(),
                trusted_root=world.root,
            )

    def test_the_privilege_launcher_anchor_is_the_fixed_util_linux_binary(self) -> None:
        assert Path("/usr/bin/setpriv") == root_verifier.PRODUCTION_PRIVILEGE_LAUNCHER

    def test_no_production_path_still_carries_a_python_preexec_callable(self) -> None:
        """P1-5, structurally: the symbol and the keyword are gone from the module."""

        source = Path(root_verifier.__file__).read_text(encoding="utf-8")

        assert "preexec_fn=" not in source
        assert "build_child_preexec" not in source
        assert not hasattr(root_verifier, "build_child_preexec")
        assert not hasattr(root_verifier, "child_privilege_applier")


# ---------------------------------------------------------------------------------------
# The root's own import closure
# ---------------------------------------------------------------------------------------


class TestRootImportClosure:
    #: Everything `import rquant.signal_family_root_verifier` is permitted to pull in.
    #: Amended per Codex round-2 order 2026-08-25, ruling 2: `rquant.signal_contracts` used
    #: to appear here because `ACCEPTED_FAMILY_IDS` read `CURRENT_ENVELOPE_SCHEMA` from the
    #: contract module at import time. Those constants now live in the leaf module
    #: `rquant.signal_family_constants`, which imports nothing from `rquant`, so the
    #: contract module leaves the root's closure and the leaf module takes its place.
    ALLOWED_MODULES = (
        "rquant",
        "rquant.authority_path_security",
        "rquant.privilege_launcher",
        "rquant.runtime_authority",
        "rquant.runtime_contracts",
        "rquant.runtime_service_control",
        "rquant.runtime_service_entrypoint",
        "rquant.signal_family_constants",
        "rquant.signal_family_root_verifier",
        "rquant.signal_family_successor_registry",
        "rquant.signal_family_verification",
        "rquant.strict_json",
    )

    #: Nothing here may ever appear: the twelve pair-to-surface modules, the builder and
    #: harness layers, and the production profile that would drag a builder in with it.
    FORBIDDEN_MODULES = (
        "rquant.notification_state",
        "rquant.paper_signal_consumer",
        "rquant.paper_signal_worker",
        "rquant.runtime_builder_daily_orchestrator",
        "rquant.runtime_builder_paper",
        "rquant.runtime_builder_serving",
        "rquant.runtime_builder_shadow",
        "rquant.runtime_builder_signal",
        "rquant.runtime_builder_strategy",
        "rquant.runtime_definition_bootstrap",
        "rquant.runtime_production_profile",
        "rquant.runtime_serving_authority",
        "rquant.runtime_serving_snapshot",
        "rquant.runtime_service_builtin",
        "rquant.runtime_shadow_sources",
        "rquant.serving_read_models",
        "rquant.signal_route_spool",
        "rquant.signal_router_runtime",
        "rquant.strategy_runner",
    )

    @staticmethod
    def _observed_closure() -> tuple[str, ...]:
        import json
        import subprocess

        program = (
            "import json, sys\n"
            "import rquant.signal_family_root_verifier as verifier\n"
            "assert verifier is not None\n"
            "print(json.dumps(sorted(n for n in sys.modules if n.split('.')[0] == 'rquant')))\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", program],
            check=True,
            capture_output=True,
            text=True,
        )
        return tuple(json.loads(completed.stdout))

    def test_the_root_import_closure_is_exactly_the_allowed_module_set(self) -> None:
        assert self._observed_closure() == self.ALLOWED_MODULES

    def test_no_forbidden_module_reaches_the_root_interpreter(self) -> None:
        observed = set(self._observed_closure())

        assert observed.isdisjoint(self.FORBIDDEN_MODULES)

    def test_every_pair_to_surface_module_is_on_the_forbidden_list(self) -> None:
        declared = {
            surface.value.rsplit(".", 1)[0]
            for surface in verification.SurfaceId
        }
        modules = {
            name if name in self.FORBIDDEN_MODULES else name.rsplit(".", 1)[0]
            for name in declared
        }

        assert modules <= set(self.FORBIDDEN_MODULES)


# ---------------------------------------------------------------------------------------
# No caller-reachable append authority
# ---------------------------------------------------------------------------------------


class TestNoCallerAppendAuthority:
    def test_the_store_refuses_a_caller_built_receipt_mapping(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        result = _verifier(world).run()
        store = root_verifier.SignalFamilyVerificationStore.open(
            world.store_root,
            owner_uid=os.getuid(),
        )
        try:
            with pytest.raises(TypeError):
                store.finalize(
                    receipts=(result.receipts[0].model_dump(mode="json"),),  # type: ignore[arg-type]
                    decision=result.decision,
                    recorded_at=VERIFIED_AT,
                )
        finally:
            store.close()

    def test_the_store_refuses_a_receipt_set_that_is_not_the_five_frozen_pairs(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        result = _verifier(world).run()
        store = root_verifier.SignalFamilyVerificationStore.open(
            world.store_root,
            owner_uid=os.getuid(),
        )
        try:
            with pytest.raises(
                root_verifier.SignalFamilyRootVerifierError,
                match="PAIR_SET_INCOMPLETE",
            ):
                store.finalize(
                    receipts=result.receipts[:4],
                    decision=result.decision,
                    recorded_at=VERIFIED_AT,
                )
        finally:
            store.close()

    def test_the_verification_model_module_exposes_no_persistence_entry_point(self) -> None:
        forbidden = ("append", "persist", "store", "commit", "write")
        offenders = [
            name
            for name in dir(verification)
            if not name.startswith("_") and any(word in name.lower() for word in forbidden)
        ]
        assert offenders == []

    def test_the_store_root_is_created_private_to_its_owner(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()

        assert stat.S_IMODE(world.store_root.stat().st_mode) == 0o700
        assert stat.S_IMODE(world.store_database.stat().st_mode) == 0o600

    def test_a_store_root_owned_by_another_identity_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="STORE_ANCHOR_INVALID",
        ):
            root_verifier.SignalFamilyVerificationStore.open(
                world.store_root,
                owner_uid=os.getuid() + 4242,
            )
        assert world.store_root.exists() is False

    def test_a_group_writable_store_root_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        world.store_root.mkdir(parents=True)
        os.chmod(world.store_root, 0o770)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="STORE_ANCHOR_INVALID",
        ):
            root_verifier.SignalFamilyVerificationStore.open(
                world.store_root,
                owner_uid=os.getuid(),
            )

    def test_a_symlinked_store_root_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        real = tmp_path / "elsewhere-store"
        real.mkdir(mode=0o700)
        world.store_root.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
        os.symlink(real, world.store_root)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="STORE_ANCHOR_INVALID",
        ):
            root_verifier.SignalFamilyVerificationStore.open(
                world.store_root,
                owner_uid=os.getuid(),
            )


# ---------------------------------------------------------------------------------------
# The production entry point wires the fixed anchors and nothing else
# ---------------------------------------------------------------------------------------


class TestProductionEntryPoint:
    """The entry point moved into the installed artifact; the checkout copy refuses.

    Codex round-2 P1-4. `scripts/signal-family-root-verifier.py` used to insert the
    checkout's `src` at `sys.path[0]`; the production entry is now
    `rquant.signal_family_verifier_entry._cli`, which the build packages into the fixed
    root-owned pyz and which is reached only after the installed tree has been verified.
    """

    @staticmethod
    def _entry_module() -> Any:
        from rquant.signal_family_verifier_entry import _cli

        return _cli

    @staticmethod
    def _checkout_script() -> Path:
        return Path(root_verifier.__file__).resolve().parents[2] / "scripts" / (
            "signal-family-root-verifier.py"
        )

    def test_the_entry_point_hardcodes_the_four_production_anchors(self) -> None:
        module = self._entry_module()
        anchors = module.production_anchors(child_uid=1000, child_gid=1000)

        assert anchors.policy_trusted_root == Path("/")
        assert anchors.policy_path == Path(verification.VERIFIER_POLICY_PATH)
        assert anchors.harness_path == Path(verification.HARNESS_IDENTITY)
        assert anchors.store_root == Path("/var/lib/rquant/signal-family-verification")
        assert anchors.expected_owner_uid == 0
        assert anchors.expected_owner_gid == 0
        assert anchors.privilege_launcher_path == Path("/usr/bin/setpriv")

    def test_the_entry_point_reads_no_environment_override(self) -> None:
        module = self._entry_module()
        source = Path(module.__file__).read_text(encoding="utf-8")

        assert "os.environ" not in source
        assert "getenv" not in source
        assert "--policy" not in source
        assert "--harness" not in source
        assert "--store" not in source

    def test_the_checkout_entry_script_refuses_to_run_the_verifier(self) -> None:
        import subprocess

        completed = subprocess.run(
            [sys.executable, str(self._checkout_script()), "verify"],
            capture_output=True,
            text=True,
            check=False,
        )

        assert completed.returncode != 0
        assert "mutable checkout" in completed.stderr

    def test_the_checkout_entry_script_imports_nothing_from_the_checkout(self) -> None:
        source = self._checkout_script().read_text(encoding="utf-8")

        assert "sys.path.insert" not in source
        assert "from rquant." not in source

    def test_the_entry_point_offers_exactly_verify_revoke_and_rollback(self) -> None:
        module = self._entry_module()

        assert module.SUBCOMMANDS == ("verify", "revoke", "rollback")
