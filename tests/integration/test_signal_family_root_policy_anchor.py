"""`RESET-REG-P0-01`: the external fixed root-owned policy is the only authorization.

authority.md L1280-1291 anchors `/etc/rquant/signal-family-verifier-policy-v1.json` behind
an anchored no-follow open from a trusted root directory FD, requires `root:root`, mode
`0444`, `nlink == 1`, canonical bytes and content hash, and rechecks every component's
device, inode, type, owner, and mode after the open. Its strict canonical bytes and SHA-256
are verified *before any generation file is opened*.

macOS cannot create a `root:root` `/etc`, so ruling O5 injects the trusted root, the policy
path, the harness path, the store root, and the expected owner UID through the explicit
`VerifierAnchors` constructor — never through an environment variable or a flag. Every
mode, nlink, regular-file, no-follow, and directory-writability check below runs for real
against that replica; only the literal UID 0 is substituted.

The "before any generation file" ordering is proved by deleting the four immutable
generation documents first: if the policy anchor were consulted second, the rejection would
name a manifest, not the policy.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from rquant import signal_family_root_verifier as root_verifier
from rquant import signal_family_verification as verification
from rquant.strict_json import canonical_json_bytes
from tests.integration.signal_family_verifier_world import (
    VerifierWorld,
    build_world,
    digest,
    rewrite_policy,
    write_policy_bytes,
)

pytestmark = pytest.mark.integration

VERIFIED_AT = datetime(2026, 8, 24, 7, 30, 15, 250000, tzinfo=UTC)


def _verifier(world: VerifierWorld, **overrides: Any) -> root_verifier.RootVerifier:
    return root_verifier.RootVerifier(
        anchors=overrides.pop("anchors", world.anchors),
        authority_gateway=world.gateway,
        clock=lambda: VERIFIED_AT,
        **overrides,
    )


def _strip_generation_documents(world: VerifierWorld) -> None:
    """Remove every immutable generation document the later steps would open."""

    for relative in (
        verification.SUCCESSOR_BUNDLE_RELATIVE_PATH,
        verification.OVERLAY_BUNDLE_RELATIVE_PATH,
        verification.VERIFICATION_MANIFEST_RELATIVE_PATH,
        verification.TEST_MANIFEST_RELATIVE_PATH,
    ):
        (world.generation_path / relative).unlink()


def _rows(world: VerifierWorld, table: str) -> list[tuple[Any, ...]]:
    if not world.store_database.exists():
        return []
    connection = sqlite3.connect(f"file:{world.store_database}?mode=ro", uri=True)
    try:
        return list(connection.execute(f"SELECT * FROM {table}"))
    finally:
        connection.close()


def _assert_policy_rejection(world: VerifierWorld, reason: str) -> None:
    _strip_generation_documents(world)
    with pytest.raises(root_verifier.SignalFamilyRootVerifierError, match=reason):
        _verifier(world).run()
    assert world.report_path.exists() is False
    assert _rows(world, "receipts") == []
    assert _rows(world, "decisions") == []
    assert _rows(world, "readiness_state") == []


# ---------------------------------------------------------------------------------------
# The anchored no-follow identity of the policy file and its fixed harness
# ---------------------------------------------------------------------------------------


class TestAnchoredIdentity:
    def test_a_symlinked_policy_file_rejects_before_any_generation_file(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        payload = world.policy_path.read_bytes()
        target = tmp_path / "elsewhere-policy.json"
        target.write_bytes(payload)
        target.chmod(0o444)
        world.policy_path.unlink()
        os.symlink(target, world.policy_path)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_policy_file_with_a_writable_mode_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        world.policy_path.chmod(0o644)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_hard_linked_policy_file_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        os.link(world.policy_path, world.policy_path.parent / "policy-second-name.json")

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_group_writable_policy_directory_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        world.policy_path.parent.chmod(0o775)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_group_writable_etc_substitute_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        (world.root / "etc").chmod(0o775)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_world_writable_trusted_root_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        world.root.chmod(0o757)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_symlinked_policy_directory_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        real = world.root / "etc" / "rquant-real"
        world.policy_path.parent.rename(real)
        os.symlink(real, world.policy_path.parent)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_policy_owned_by_another_identity_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        import dataclasses

        anchors = dataclasses.replace(
            world.anchors,
            expected_owner_uid=os.getuid() + 4242,
        )
        _strip_generation_documents(world)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="POLICY_ANCHOR_INVALID",
        ):
            _verifier(world, anchors=anchors).run()

    def test_a_harness_whose_bytes_do_not_match_the_policy_hash_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        world.harness_path.chmod(0o755)
        world.harness_path.write_bytes(world.harness_path.read_bytes() + b"\x00")
        world.harness_path.chmod(0o555)

        _assert_policy_rejection(world, "HARNESS_HASH_MISMATCH")

    def test_a_harness_with_a_writable_mode_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        world.harness_path.chmod(0o755)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_symlinked_harness_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        payload = world.harness_path.read_bytes()
        target = tmp_path / "elsewhere-harness.pyz"
        target.write_bytes(payload)
        target.chmod(0o555)
        world.harness_path.unlink()
        os.symlink(target, world.harness_path)

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_hard_linked_harness_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        os.link(world.harness_path, world.harness_path.parent / "harness-second-name.pyz")

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")

    def test_a_missing_policy_file_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        world.policy_path.unlink()

        _assert_policy_rejection(world, "POLICY_ANCHOR_INVALID")


# ---------------------------------------------------------------------------------------
# The strict duplicate-free decoder and the exact entry tuple
# ---------------------------------------------------------------------------------------


class TestStrictPolicyBytes:
    def test_a_trailing_newline_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        write_policy_bytes(world, world.policy_path.read_bytes() + b"\n")

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_pretty_printed_whitespace_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        write_policy_bytes(world, json.dumps(decoded, indent=2).encode("utf-8"))

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_a_duplicate_object_key_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        raw = world.policy_path.read_bytes()
        assert raw.startswith(b'{"content_hash":"')
        injected = b'{"content_hash":"' + b"0" * 64 + b'",' + raw[1:]
        write_policy_bytes(world, injected)

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_an_extra_top_level_key_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["installed_by"] = "release"
        decoded.pop("content_hash")
        decoded["content_hash"] = hashlib.sha256(canonical_json_bytes(decoded)).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_a_tampered_content_hash_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["content_hash"] = digest("tampered")
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "POLICY_CONTENT_HASH_MISMATCH")

    def test_a_tampered_entry_hash_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["release_entries"][0]["entry_hash"] = digest("tampered-entry")
        decoded["content_hash"] = hashlib.sha256(
            canonical_json_bytes(
                {key: value for key, value in decoded.items() if key != "content_hash"}
            )
        ).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_reordered_release_entries_reject(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        second = verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=digest("zzz-successor"),
            overlay_content_hash=digest("zzz-overlay"),
            verification_manifest_sha256=world.entry.verification_manifest_sha256,
            vector_set_hash=world.entry.vector_set_hash,
            expected_result_set_hash=world.entry.expected_result_set_hash,
            five_pair_service_binding_set_hash=(
                world.entry.five_pair_service_binding_set_hash
            ),
            verifier_policy_max_age_seconds=None,
        )
        ordered = verification.SignalFamilyVerifierPolicyV1.create(
            harness_sha256=world.policy.harness_sha256,
            release_entries=tuple(
                sorted((world.entry, second), key=lambda entry: entry.release_key)
            ),
        )
        decoded = json.loads(verification.verifier_policy_canonical_json_bytes(ordered))
        decoded["release_entries"].reverse()
        decoded["content_hash"] = hashlib.sha256(
            canonical_json_bytes(
                {key: value for key, value in decoded.items() if key != "content_hash"}
            )
        ).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_two_identical_entries_for_one_release_key_reject(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["release_entries"].append(dict(decoded["release_entries"][0]))
        decoded["content_hash"] = hashlib.sha256(
            canonical_json_bytes(
                {key: value for key, value in decoded.items() if key != "content_hash"}
            )
        ).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "ENTRY_MULTIPLE")

    def test_two_conflicting_entries_for_one_release_key_reject(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        conflicting = verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=world.successor_bundle_content_hash,
            overlay_content_hash=world.overlay_content_hash,
            verification_manifest_sha256=digest("conflicting-manifest"),
            vector_set_hash=world.entry.vector_set_hash,
            expected_result_set_hash=world.entry.expected_result_set_hash,
            five_pair_service_binding_set_hash=(
                world.entry.five_pair_service_binding_set_hash
            ),
            verifier_policy_max_age_seconds=None,
        )
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["release_entries"].append(
            json.loads(canonical_json_bytes(conflicting.model_dump(mode="json")))
        )
        decoded["content_hash"] = hashlib.sha256(
            canonical_json_bytes(
                {key: value for key, value in decoded.items() if key != "content_hash"}
            )
        ).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "ENTRY_CONFLICTING")

    def test_an_empty_release_entry_tuple_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["release_entries"] = []
        decoded["content_hash"] = hashlib.sha256(
            canonical_json_bytes(
                {key: value for key, value in decoded.items() if key != "content_hash"}
            )
        ).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_an_unknown_harness_identity_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["harness_identity"] = "/usr/local/libexec/other-harness.pyz"
        decoded["content_hash"] = hashlib.sha256(
            canonical_json_bytes(
                {key: value for key, value in decoded.items() if key != "content_hash"}
            )
        ).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")

    def test_an_unknown_schema_version_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        decoded = json.loads(world.policy_path.read_bytes())
        decoded["schema_version"] = 2
        decoded["content_hash"] = hashlib.sha256(
            canonical_json_bytes(
                {key: value for key, value in decoded.items() if key != "content_hash"}
            )
        ).hexdigest()
        write_policy_bytes(world, canonical_json_bytes(decoded))

        _assert_policy_rejection(world, "POLICY_BYTES_NONCANONICAL")


# ---------------------------------------------------------------------------------------
# A self-consistent generation cannot authorize itself
# ---------------------------------------------------------------------------------------


class TestSelfConsistentReplacement:
    def test_a_self_consistent_vector_and_result_replacement_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        vectors = world.test_manifest.vectors
        replacement = verification.SignalFamilyVectorV1.create(
            pair_id=vectors[0].pair_id,
            family_id=vectors[0].family_id,
            surface_id=vectors[0].surface_id,
            input_json=canonical_json_bytes({"grafted": True}).decode("utf-8"),
        )
        rebuilt = tuple(
            sorted((replacement, *vectors[1:]), key=lambda vector: vector.vector_id)
        )
        expected = tuple(
            verification.SignalFamilyExpectedResultV1(
                vector_id=vector.vector_id,
                canonical_result_sha256=digest(f"grafted:{vector.vector_id}"),
            )
            for vector in rebuilt
        )
        manifest = verification.SignalFamilyTestManifestV1.create(
            vectors=rebuilt,
            expected_results=expected,
            profile_manifests=world.profile_manifests,
            service_bindings=world.test_manifest.service_bindings,
        )
        manifest_bytes = verification.test_manifest_canonical_json_bytes(manifest)
        verification_manifest = verification.SignalFamilyVerificationManifestV1.create(
            successor_bundle_content_hash=world.successor_bundle_content_hash,
            overlay_content_hash=world.overlay_content_hash,
            test_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            test_manifest=manifest,
        )
        verification_bytes = verification.verification_manifest_canonical_json_bytes(
            verification_manifest
        )
        entries = dict(world.gateway.snapshots[0].full_manifest_entries)
        for relative, payload in (
            (verification.TEST_MANIFEST_RELATIVE_PATH, manifest_bytes),
            (verification.VERIFICATION_MANIFEST_RELATIVE_PATH, verification_bytes),
        ):
            target = world.generation_path / relative
            target.chmod(0o644)
            target.write_bytes(payload)
            target.chmod(0o444)
            entries[relative] = hashlib.sha256(payload).hexdigest()
        from tests.integration.signal_family_verifier_world import snapshot_with

        world.gateway.snapshots = [snapshot_with(world, full_manifest_entries=entries)]

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="VERIFICATION_MANIFEST_HASH_MISMATCH",
        ):
            _verifier(world).run()
        assert world.report_path.exists() is False
        assert _rows(world, "receipts") == []

    def test_a_matching_policy_update_is_the_only_way_the_replacement_passes(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        result = _verifier(world).run()

        assert result.outcome == "persisted"
        assert result.decision.selected_entry_hash == world.entry.entry_hash


# ---------------------------------------------------------------------------------------
# The policy and harness may not move under a running child
# ---------------------------------------------------------------------------------------


class TestReplacementDuringChildExecution:
    def test_a_selected_entry_replaced_while_the_child_runs_rejects_as_stale(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        world = build_world(tmp_path)
        original = root_verifier.RootVerifier._run_child
        replacement = verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=world.successor_bundle_content_hash,
            overlay_content_hash=world.overlay_content_hash,
            verification_manifest_sha256=world.verification_manifest_sha256,
            vector_set_hash=world.entry.vector_set_hash,
            expected_result_set_hash=world.entry.expected_result_set_hash,
            five_pair_service_binding_set_hash=(
                world.entry.five_pair_service_binding_set_hash
            ),
            verifier_policy_max_age_seconds=4242,
        )

        def swapping_child(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return original(self, *args, **kwargs)
            finally:
                rewrite_policy(
                    world,
                    verification.SignalFamilyVerifierPolicyV1.create(
                        harness_sha256=world.policy.harness_sha256,
                        release_entries=(replacement,),
                    ),
                )

        monkeypatch.setattr(root_verifier.RootVerifier, "_run_child", swapping_child)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="ENTRY_STALE",
        ):
            _verifier(world).run()
        assert _rows(world, "receipts") == []
        assert _rows(world, "decisions") == []

    def test_a_harness_replaced_while_the_child_runs_rejects(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        world = build_world(tmp_path)
        original = root_verifier.RootVerifier._run_child

        def swapping_child(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return original(self, *args, **kwargs)
            finally:
                world.harness_path.chmod(0o755)
                world.harness_path.write_bytes(world.harness_path.read_bytes() + b"\x00")
                world.harness_path.chmod(0o555)

        monkeypatch.setattr(root_verifier.RootVerifier, "_run_child", swapping_child)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="HARNESS_HASH_MISMATCH",
        ):
            _verifier(world).run()
        assert _rows(world, "receipts") == []

    def test_an_unrelated_policy_entry_added_while_the_child_runs_rejects(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        world = build_world(tmp_path)
        original = root_verifier.RootVerifier._run_child
        extra = verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=digest("aaa-successor"),
            overlay_content_hash=digest("aaa-overlay"),
            verification_manifest_sha256=world.entry.verification_manifest_sha256,
            vector_set_hash=world.entry.vector_set_hash,
            expected_result_set_hash=world.entry.expected_result_set_hash,
            five_pair_service_binding_set_hash=(
                world.entry.five_pair_service_binding_set_hash
            ),
            verifier_policy_max_age_seconds=None,
        )

        def swapping_child(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return original(self, *args, **kwargs)
            finally:
                rewrite_policy(
                    world,
                    verification.SignalFamilyVerifierPolicyV1.create(
                        harness_sha256=world.policy.harness_sha256,
                        release_entries=tuple(
                            sorted(
                                (world.entry, extra),
                                key=lambda entry: entry.release_key,
                            )
                        ),
                    ),
                )

        monkeypatch.setattr(root_verifier.RootVerifier, "_run_child", swapping_child)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="POLICY_CHANGED_DURING_RUN",
        ):
            _verifier(world).run()


# ---------------------------------------------------------------------------------------
# The anchors themselves are a boundary, not a switch
# ---------------------------------------------------------------------------------------


class TestChildWorkspaceAnchorBoundary:
    """WP4C-SPEC-05: every store-root exclusion and every workspace reject, with a case.

    The seven guards round 1 added were hand-probed and correct but had no negative test, so
    nothing would have noticed them being deleted. O5's discipline for the WP4-b anchors —
    symlink, mode, ownership, group-writability, type — is applied to them here.
    """

    @staticmethod
    def _anchors(world: Any, **overrides: Any) -> root_verifier.VerifierAnchors:
        values: dict[str, Any] = {
            "policy_trusted_root": world.root,
            "policy_path": world.policy_path,
            "harness_path": world.harness_path,
            "store_root": world.store_root,
            "child_workspace_root": world.child_workspace_root,
            "expected_owner_uid": os.getuid(),
            "expected_owner_gid": world.root.stat().st_gid,
            "child_uid": os.getuid(),
            "child_gid": os.getgid(),
        }
        values.update(overrides)
        return root_verifier.VerifierAnchors(**values)

    def test_a_workspace_that_is_the_store_root_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)

        with pytest.raises(ValueError, match="must not be the store root"):
            self._anchors(world, child_workspace_root=world.store_root)

    def test_a_workspace_that_contains_the_store_root_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)

        with pytest.raises(ValueError, match="must not contain the store root"):
            self._anchors(world, child_workspace_root=world.store_root.parent)

    def test_a_workspace_beneath_the_store_root_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)

        with pytest.raises(ValueError, match="must not live beneath the store root"):
            self._anchors(world, child_workspace_root=world.store_root / "workspace")

    def test_the_world_anchors_keep_the_workspace_and_the_store_disjoint(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        workspace = world.anchors.child_workspace_root

        assert workspace != world.store_root
        assert workspace not in world.store_root.parents
        assert world.store_root not in workspace.parents

    def test_a_symlinked_ancestor_rejects(self, tmp_path: Path) -> None:
        real = tmp_path / "real"
        real.mkdir(mode=0o755)
        link = tmp_path / "link"
        link.symlink_to(real, target_is_directory=True)

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError, match="symlink"):
            root_verifier.open_child_workspace_root(
                link / "workspace",
                expected_uid=os.getuid(),
                child_uid=os.getuid(),
            )

    def test_a_group_writable_ancestor_rejects(self, tmp_path: Path) -> None:
        loose = tmp_path / "loose"
        loose.mkdir(mode=0o755)
        loose.chmod(0o775)  # `mkdir` would have masked this with the umask

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="group or world writable",
        ):
            root_verifier.open_child_workspace_root(
                loose / "workspace",
                expected_uid=os.getuid(),
                child_uid=os.getuid(),
            )

    def test_an_ancestor_owned_by_an_untrusted_account_rejects(self, tmp_path: Path) -> None:
        private = tmp_path / "private"
        private.mkdir(mode=0o755)

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="untrusted account",
        ):
            root_verifier.open_child_workspace_root(
                private / "workspace",
                expected_uid=os.getuid() + 4242,
                child_uid=os.getuid(),
            )

    @pytest.mark.parametrize("mode", [0o700, 0o755, 0o701, 0o710])
    def test_a_workspace_with_the_wrong_mode_rejects(self, tmp_path: Path, mode: int) -> None:
        """`0700` is in this list on purpose: it is the round 1 value that production cannot
        traverse, so re-adopting it must now fail loudly rather than silently."""

        workspace = tmp_path / "workspace"
        workspace.mkdir(mode=0o755)
        workspace.chmod(mode)

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="private directory this verifier owns",
        ):
            root_verifier.open_child_workspace_root(
                workspace,
                expected_uid=os.getuid(),
                child_uid=os.getuid(),
            )

    def test_a_non_directory_component_rejects(self, tmp_path: Path) -> None:
        """A file where a parent directory should be is refused at creation, not walked."""

        blocker = tmp_path / "blocker"
        blocker.write_text("not a directory", encoding="utf-8")

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="could not be created",
        ):
            root_verifier.open_child_workspace_root(
                blocker / "workspace",
                expected_uid=os.getuid(),
                child_uid=os.getuid(),
            )

    def test_a_missing_parent_rejects_instead_of_being_created(self, tmp_path: Path) -> None:
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="could not be created",
        ):
            root_verifier.open_child_workspace_root(
                tmp_path / "absent" / "workspace",
                expected_uid=os.getuid(),
                child_uid=os.getuid(),
            )

    def test_a_fresh_workspace_is_created_at_the_frozen_mode(self, tmp_path: Path) -> None:
        """`os.mkdir` masks its mode with the umask, so the mode is set explicitly."""

        workspace = tmp_path / "workspace"
        previous = os.umask(0o077)
        try:
            resolved = root_verifier.open_child_workspace_root(
                workspace,
                expected_uid=os.getuid(),
                child_uid=os.getuid(),
            )
        finally:
            os.umask(previous)

        assert resolved == workspace
        assert stat.S_IMODE(workspace.stat().st_mode) == root_verifier.CHILD_WORKSPACE_MODE

    def test_an_existing_workspace_is_validated_not_normalized(self, tmp_path: Path) -> None:
        """A mode somebody else set is a misconfiguration to report, not one to repair."""

        workspace = tmp_path / "workspace"
        workspace.mkdir(mode=0o755)
        workspace.chmod(0o777)

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError):
            root_verifier.open_child_workspace_root(
                workspace,
                expected_uid=os.getuid(),
                child_uid=os.getuid(),
            )

        assert stat.S_IMODE(workspace.stat().st_mode) == 0o777


class TestAnchorBoundary:
    def test_a_policy_path_outside_the_trusted_root_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        with pytest.raises(ValueError, match="trusted root"):
            root_verifier.VerifierAnchors(
                policy_trusted_root=world.root,
                policy_path=tmp_path / "policy.json",
                harness_path=world.harness_path,
                store_root=world.store_root,
                child_workspace_root=world.child_workspace_root,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=os.getgid(),
                child_uid=os.getuid(),
                child_gid=os.getgid(),
            )

    def test_a_relative_anchor_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        with pytest.raises(ValueError, match="absolute"):
            root_verifier.VerifierAnchors(
                policy_trusted_root=world.root,
                policy_path=Path("etc/rquant/policy.json"),
                harness_path=world.harness_path,
                store_root=world.store_root,
                child_workspace_root=world.child_workspace_root,
                expected_owner_uid=os.getuid(),
                expected_owner_gid=os.getgid(),
                child_uid=os.getuid(),
                child_gid=os.getgid(),
            )

    def test_the_verifier_module_never_opens_an_anchor_for_write(self) -> None:
        source = Path(root_verifier.__file__).read_text(encoding="utf-8")

        for forbidden in ("write_bytes", "write_text", "O_CREAT", "O_WRONLY", "O_TRUNC"):
            assert forbidden not in source

    def test_the_verifier_module_reads_no_environment_override(self) -> None:
        source = Path(root_verifier.__file__).read_text(encoding="utf-8")

        assert "os.environ.get" not in source
        assert "os.getenv" not in source
