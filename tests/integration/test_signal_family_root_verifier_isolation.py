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

pytestmark = pytest.mark.integration

VERIFIED_AT = datetime(2026, 8, 24, 7, 30, 15, 250000, tzinfo=UTC)


def _verifier(world: VerifierWorld, **overrides: Any) -> root_verifier.RootVerifier:
    return root_verifier.RootVerifier(
        anchors=world.anchors,
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
    def test_the_child_environment_is_exactly_the_frozen_allowlist(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        report = world.read_report()

        assert tuple(sorted(report["environ"])) == root_verifier.SIGNAL_FAMILY_CHILD_ENV_KEYS

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
        world.store_root.mkdir(mode=0o770, parents=True)
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
    @staticmethod
    def _entry_module() -> Any:
        import importlib.util

        path = Path(root_verifier.__file__).resolve().parents[2]
        script = path / "scripts" / "signal-family-root-verifier.py"
        spec = importlib.util.spec_from_file_location("signal_family_root_verifier_cli", script)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        try:
            spec.loader.exec_module(module)
        finally:
            del sys.modules[spec.name]
        return module

    def test_the_entry_point_hardcodes_the_four_production_anchors(self) -> None:
        module = self._entry_module()
        anchors = module.production_anchors(child_uid=1000, child_gid=1000)

        assert anchors.policy_trusted_root == Path("/")
        assert anchors.policy_path == Path(verification.VERIFIER_POLICY_PATH)
        assert anchors.harness_path == Path(verification.HARNESS_IDENTITY)
        assert anchors.store_root == Path("/var/lib/rquant/signal-family-verification")
        assert anchors.expected_owner_uid == 0
        assert anchors.expected_owner_gid == 0

    def test_the_entry_point_reads_no_environment_override(self) -> None:
        module = self._entry_module()
        source = Path(module.__file__).read_text(encoding="utf-8")

        assert "os.environ" not in source
        assert "getenv" not in source
        assert "--policy" not in source
        assert "--harness" not in source
        assert "--store" not in source

    def test_the_entry_point_offers_exactly_verify_revoke_and_rollback(self) -> None:
        module = self._entry_module()

        assert module.SUBCOMMANDS == ("verify", "revoke", "rollback")
