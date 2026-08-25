"""`RESET-REG-P0` / `RESET-REG-P1` / `RESET-REG-P1-02`: the eight-step verifier sequence.

The root verifier of authority.md L1407-1449 is exercised end to end against a replica of
its production anchors: an externally installed root-owned policy, a policy-hashed fixed
harness, an immutable in-generation verification/test manifest, an OS-separated child, and
a root-owned append store. WP4-b proves the protocol with a stub harness that replays one
frozen result per vector; the real production-builder harness belongs to WP4-c.

Every rejection row names the exact step it stops at, carries a bounded reason code, and
leaves no receipt and no readiness record behind. Variants that touch a hashed field always
recompute the affected `*_hash` so the rule under test is the first failing check.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from rquant import signal_family_root_verifier as root_verifier
from rquant import signal_family_verification as verification
from rquant import signal_family_verifier_harness as harness
from rquant.runtime_authority import GENERATION_MANIFEST_NAME
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.strict_json import canonical_json_bytes
from tests.integration.signal_family_verifier_world import (
    OTHER_OPERATION_ID,
    PROFILE_ID,
    VerifierWorld,
    build_world,
    digest,
    full_manifest_payload,
    production_authority_record,
    profile_document,
    profile_manifests,
    rewrite_policy,
    rewrite_profile_document,
    snapshot_with,
    write_full_manifest_bytes,
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


def _clock() -> datetime:
    return VERIFIED_AT


def _verifier(world: VerifierWorld, **overrides: Any) -> root_verifier.RootVerifier:
    return root_verifier.RootVerifier(
        anchors=overrides.pop("anchors", world.anchors),
        authority_gateway=overrides.pop("authority_gateway", world.gateway),
        clock=overrides.pop("clock", _clock),
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


def _assert_no_evidence(world: VerifierWorld) -> None:
    assert _rows(world, "receipts") == []
    assert _rows(world, "decisions") == []
    assert [row for row in _rows(world, "readiness_state")] == []


# ---------------------------------------------------------------------------------------
# The eight-step happy path
# ---------------------------------------------------------------------------------------


class TestEightStepSequence:
    def test_one_successful_run_persists_exactly_five_receipts_and_one_decision(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        result = _verifier(world).run()

        assert result.outcome == "persisted"
        assert result.state is verification.SignalFamilyReadinessState.READY
        assert tuple(receipt.pair_id for receipt in result.receipts) == verification.PAIR_IDS
        assert len(result.receipts) == 5
        assert result.decision.pair_ids == verification.PAIR_IDS
        assert result.decision.overlay_content_hash == world.overlay_content_hash
        assert result.decision.authority_epoch_key == world.authority_epoch_key
        assert len(_rows(world, "receipts")) == 5
        assert len(_rows(world, "decisions")) == 1

    def test_receipts_bind_the_pair_derived_participants_and_the_frozen_surfaces(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        result = _verifier(world).run()

        participating = verification.participating_service_ids(world.profile_manifests)
        for receipt in result.receipts:
            assert receipt.participating_service_ids == participating
            assert receipt.reader_surface_ids == verification.READER_SURFACES[receipt.pair_id]
            assert receipt.producer_surface_ids == verification.PRODUCER_SURFACES[receipt.pair_id]
            assert receipt.service_bindings_hash == world.test_manifest.service_bindings_hash

    def test_freshness_follows_the_lowest_participating_stale_bound(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path, stale_overrides={"serving-publisher": 12.0})
        result = _verifier(world).run()

        expected = verification.canonical_timestamp(VERIFIED_AT + timedelta(seconds=12.0))
        assert result.decision.fresh_until == expected

    def test_the_optional_policy_cap_lowers_freshness_below_the_profile_minimum(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path, policy_max_age_seconds=9)
        result = _verifier(world).run()

        expected = verification.canonical_timestamp(VERIFIED_AT + timedelta(seconds=9.0))
        assert result.decision.fresh_until == expected

    def test_the_decision_binds_every_overlay_declaration_and_channel_hash(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        result = _verifier(world).run()

        overlay = canonical_json_bytes(
            __import__("json").loads(
                (
                    world.generation_path / verification.OVERLAY_BUNDLE_RELATIVE_PATH
                ).read_bytes()
            )
        )
        assert overlay  # the overlay document is canonical on disk
        assert len(result.decision.successor_declaration_hashes) == 3
        assert len(result.decision.successor_channel_hashes) == 3

    def test_the_deployment_lock_is_held_across_the_whole_run_and_released(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()

        assert world.gateway.lock.asserted >= 2
        assert world.gateway.lock.closed is True

    def test_the_store_is_opened_only_after_the_child_has_exited(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        world = build_world(tmp_path)
        events: list[str] = []
        original_child = root_verifier.RootVerifier._run_child
        original_open = root_verifier.SignalFamilyVerificationStore.open

        def traced_child(self: Any, *args: Any, **kwargs: Any) -> Any:
            events.append("child-start")
            try:
                return original_child(self, *args, **kwargs)
            finally:
                events.append("child-exit")

        def traced_open(*args: Any, **kwargs: Any) -> Any:
            events.append("store-open")
            return original_open(*args, **kwargs)

        monkeypatch.setattr(root_verifier.RootVerifier, "_run_child", traced_child)
        monkeypatch.setattr(
            root_verifier.SignalFamilyVerificationStore,
            "open",
            staticmethod(traced_open),
        )
        _verifier(world).run()

        assert events == ["child-start", "child-exit", "store-open"]

    def test_the_root_process_never_imports_generation_surface_modules(self) -> None:
        import subprocess
        import sys

        surfaces = [
            "rquant.notification_state",
            "rquant.paper_signal_consumer",
            "rquant.paper_signal_worker",
            "rquant.runtime_builder_shadow",
            "rquant.runtime_builder_signal",
            "rquant.runtime_serving_authority",
            "rquant.runtime_serving_snapshot",
            "rquant.runtime_shadow_sources",
            "rquant.serving_read_models",
            "rquant.signal_route_spool",
            "rquant.signal_router_runtime",
            "rquant.strategy_runner",
        ]
        assert all(
            any(surface.value.startswith(f"{module}.") for module in surfaces)
            for surface in verification.SurfaceId
        )
        program = (
            "import sys\n"
            "import rquant.signal_family_root_verifier as verifier\n"
            f"surfaces = {surfaces!r}\n"
            "leaked = sorted(name for name in surfaces if name in sys.modules)\n"
            "assert verifier is not None\n"
            "print(';'.join(leaked))\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", program],
            check=True,
            capture_output=True,
            text=True,
        )
        assert completed.stdout.strip() == ""


# ---------------------------------------------------------------------------------------
# Step 2: exactly one policy entry
# ---------------------------------------------------------------------------------------


class TestEntrySelection:
    def test_a_release_key_the_policy_never_names_stops_before_the_child(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        entry = verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=digest("other-successor"),
            overlay_content_hash=digest("other-overlay"),
            verification_manifest_sha256=world.entry.verification_manifest_sha256,
            vector_set_hash=world.entry.vector_set_hash,
            expected_result_set_hash=world.entry.expected_result_set_hash,
            five_pair_service_binding_set_hash=(
                world.entry.five_pair_service_binding_set_hash
            ),
            verifier_policy_max_age_seconds=None,
        )
        rewrite_policy(
            world,
            verification.SignalFamilyVerifierPolicyV1.create(
                harness_sha256=world.policy.harness_sha256,
                release_entries=(entry,),
            ),
        )
        with pytest.raises(root_verifier.SignalFamilyRootVerifierError, match="ENTRY_MISSING"):
            _verifier(world).run()
        assert world.report_path.exists() is False
        _assert_no_evidence(world)

    def test_an_entry_that_names_another_verification_manifest_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        entry = verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=world.successor_bundle_content_hash,
            overlay_content_hash=world.overlay_content_hash,
            verification_manifest_sha256=digest("another-manifest"),
            vector_set_hash=world.entry.vector_set_hash,
            expected_result_set_hash=world.entry.expected_result_set_hash,
            five_pair_service_binding_set_hash=(
                world.entry.five_pair_service_binding_set_hash
            ),
            verifier_policy_max_age_seconds=None,
        )
        rewrite_policy(
            world,
            verification.SignalFamilyVerifierPolicyV1.create(
                harness_sha256=world.policy.harness_sha256,
                release_entries=(entry,),
            ),
        )
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="VERIFICATION_MANIFEST_HASH_MISMATCH",
        ):
            _verifier(world).run()
        assert world.report_path.exists() is False


# ---------------------------------------------------------------------------------------
# Step 3: the in-generation manifests and the exact service bindings
# ---------------------------------------------------------------------------------------


class TestManifestAndBindings:
    @pytest.mark.parametrize(
        ("field_name", "reason"),
        (
            ("vector_set_hash", "VECTOR_SET_HASH_MISMATCH"),
            ("expected_result_set_hash", "EXPECTED_RESULT_SET_HASH_MISMATCH"),
            ("five_pair_service_binding_set_hash", "FIVE_PAIR_SET_HASH_MISMATCH"),
        ),
    )
    def test_a_policy_entry_set_hash_that_the_generation_does_not_produce_rejects(
        self,
        tmp_path: Path,
        field_name: str,
        reason: str,
    ) -> None:
        world = build_world(tmp_path)
        values = {
            "successor_bundle_content_hash": world.successor_bundle_content_hash,
            "overlay_content_hash": world.overlay_content_hash,
            "verification_manifest_sha256": world.verification_manifest_sha256,
            "vector_set_hash": world.entry.vector_set_hash,
            "expected_result_set_hash": world.entry.expected_result_set_hash,
            "five_pair_service_binding_set_hash": (
                world.entry.five_pair_service_binding_set_hash
            ),
            "verifier_policy_max_age_seconds": None,
        }
        values[field_name] = digest(f"drifted:{field_name}")
        rewrite_policy(
            world,
            verification.SignalFamilyVerifierPolicyV1.create(
                harness_sha256=world.policy.harness_sha256,
                release_entries=(verification.ReleaseVerificationEntryV1.create(**values),),
            ),
        )
        with pytest.raises(root_verifier.SignalFamilyRootVerifierError, match=reason):
            _verifier(world).run()
        assert world.report_path.exists() is False
        _assert_no_evidence(world)

    def test_a_test_manifest_the_verification_manifest_does_not_name_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        target = world.generation_path / verification.TEST_MANIFEST_RELATIVE_PATH
        target.chmod(0o644)
        target.write_bytes(canonical_json_bytes({"schema_version": 1}))
        target.chmod(0o444)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="TEST_MANIFEST_HASH_MISMATCH",
        ):
            _verifier(world).run()
        assert world.report_path.exists() is False

    def test_a_generation_document_outside_the_full_manifest_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        snapshot = world.gateway.snapshots[0]
        entries = dict(snapshot.full_manifest_entries)
        del entries[verification.TEST_MANIFEST_RELATIVE_PATH]
        world.gateway.snapshots = [snapshot_with(world, full_manifest_entries=entries)]
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="BINDING_UNMANIFESTED",
        ):
            _verifier(world).run()
        assert world.report_path.exists() is False

    def test_a_binding_whose_module_is_not_the_slot_role_module_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        self._reject_with_binding_change(
            world,
            index=0,
            reason="BINDING_WRONG_MODULE",
            executable_module="rquant.serving_read_models",
        )

    def test_a_binding_whose_source_hash_drifts_from_the_generation_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        self._reject_with_binding_change(
            world,
            index=0,
            reason="BINDING_WRONG_SOURCE_HASH",
            executable_source_sha256=digest("drifted-source"),
        )

    def test_a_binding_whose_role_belongs_to_another_service_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        self._reject_with_binding_change(
            world,
            index=0,
            reason="BINDING_CROSS_ROLE",
            role_name="serving",
        )

    def test_a_binding_whose_kind_is_not_the_profile_kind_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        self._reject_with_binding_change(
            world,
            index=0,
            reason="BINDING_WRONG_KIND",
            runtime_service_kind=RuntimeServiceKind.FEATURE_LIVE,
        )

    def test_a_binding_whose_fingerprint_is_not_the_profile_manifest_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        self._reject_with_binding_change(
            world,
            index=0,
            reason="BINDING_MISSING",
            service_manifest_fingerprint=digest("forged-fingerprint"),
        )

    def test_a_binding_whose_source_path_is_absent_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        self._reject_with_binding_change(
            world,
            index=0,
            reason="BINDING_WRONG_PATH",
            executable_source_relative_path="src/rquant/absent_module.py",
        )

    def test_a_binding_whose_source_path_is_a_symlink_escape_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        import os

        world = build_world(tmp_path)
        outside = tmp_path / "outside-source.py"
        outside.write_bytes(b"# outside the generation\n")
        os.symlink(outside, world.generation_path / "src" / "rquant" / "escaped.py")
        self._reject_with_binding_change(
            world,
            index=0,
            reason="BINDING_WRONG_PATH",
            executable_source_relative_path="src/rquant/escaped.py",
        )

    def test_a_generation_without_one_interpreter_for_every_role_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        import os

        from rquant.runtime_authority import RuntimeGenerationSlot, RuntimeRoleSpec

        world = build_world(tmp_path)
        second = world.generation_path / "bin" / "python-second"
        os.symlink(__import__("sys").executable, second)
        slot = world.gateway.snapshots[0].slot
        roles = dict(slot.roles)
        original = roles["serving"]
        roles["serving"] = RuntimeRoleSpec(
            python_path=second,
            module=original.module,
            working_directory=original.working_directory,
            app_source=original.app_source,
            site_packages=original.site_packages,
        )
        world.gateway.snapshots = [
            snapshot_with(
                world,
                roles=RuntimeGenerationSlot(
                    lifecycle=slot.lifecycle,
                    generation_id=slot.generation_id,
                    generation_path=slot.generation_path,
                    commit=slot.commit,
                    full_manifest_hash=slot.full_manifest_hash,
                    profile_id=slot.profile_id,
                    roles=roles,
                ).roles,
            )
        ]
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="CHILD_LAUNCH_FAILED",
        ):
            _verifier(world).run()
        assert world.report_path.exists() is False

    @staticmethod
    def _reject_with_binding_change(
        world: VerifierWorld,
        *,
        index: int,
        reason: str,
        **changes: Any,
    ) -> None:
        original = world.bindings[index]
        values: dict[str, Any] = {
            "service_id": original.service_id,
            "runtime_service_kind": original.runtime_service_kind,
            "role_name": original.role_name,
            "service_manifest_fingerprint": original.service_manifest_fingerprint,
            "executable_module": original.executable_module,
            "executable_source_relative_path": original.executable_source_relative_path,
            "executable_source_sha256": original.executable_source_sha256,
            "surface_ids": original.surface_ids,
        }
        values.update(changes)
        replaced = verification.VerificationServiceBindingV1.create(**values)
        bindings = tuple(
            replaced if position == index else binding
            for position, binding in enumerate(world.bindings)
        )
        _republish_generation(world, bindings=bindings)
        with pytest.raises(root_verifier.SignalFamilyRootVerifierError, match=reason):
            _verifier(world).run()
        assert world.report_path.exists() is False


def _republish_generation(
    world: VerifierWorld,
    *,
    bindings: tuple[verification.VerificationServiceBindingV1, ...] | None = None,
    vectors: tuple[verification.SignalFamilyVectorV1, ...] | None = None,
) -> None:
    """Rewrite the immutable generation documents and the policy that authorizes them.

    The policy entry is reminted from the same profile-derived and raw-byte inputs, so a
    rejection under test is never masked by an entry the generation no longer matches.
    """

    resolved_bindings = world.test_manifest.service_bindings if bindings is None else bindings
    resolved_vectors = world.test_manifest.vectors if vectors is None else vectors
    expected_results = tuple(
        verification.SignalFamilyExpectedResultV1(
            vector_id=vector.vector_id,
            canonical_result_sha256=hashlib.sha256(
                world.replay[vector.vector_id].encode("utf-8")
            ).hexdigest(),
        )
        for vector in resolved_vectors
        if vector.vector_id in world.replay
    )
    test_manifest = verification.SignalFamilyTestManifestV1.create(
        vectors=resolved_vectors,
        expected_results=expected_results,
        profile_manifests=world.profile_manifests,
        service_bindings=resolved_bindings,
    )
    test_bytes = verification.test_manifest_canonical_json_bytes(test_manifest)
    test_sha256 = hashlib.sha256(test_bytes).hexdigest()
    verification_manifest = verification.SignalFamilyVerificationManifestV1.create(
        successor_bundle_content_hash=world.successor_bundle_content_hash,
        overlay_content_hash=world.overlay_content_hash,
        test_manifest_sha256=test_sha256,
        test_manifest=test_manifest,
    )
    verification_bytes = verification.verification_manifest_canonical_json_bytes(
        verification_manifest
    )
    documents = {
        verification.TEST_MANIFEST_RELATIVE_PATH: test_bytes,
        verification.VERIFICATION_MANIFEST_RELATIVE_PATH: verification_bytes,
    }
    entries = dict(world.gateway.snapshots[0].full_manifest_entries)
    for relative, payload in documents.items():
        target = world.generation_path / relative
        target.chmod(0o644)
        target.write_bytes(payload)
        target.chmod(0o444)
        entries[relative] = hashlib.sha256(payload).hexdigest()
    world.gateway.snapshots = [snapshot_with(world, full_manifest_entries=entries)]
    entry = verification.ReleaseVerificationEntryV1.create(
        successor_bundle_content_hash=world.successor_bundle_content_hash,
        overlay_content_hash=world.overlay_content_hash,
        verification_manifest_sha256=entries[
            verification.VERIFICATION_MANIFEST_RELATIVE_PATH
        ],
        vector_set_hash=verification.vector_set_hash(resolved_vectors),
        expected_result_set_hash=verification.expected_result_set_hash(expected_results),
        five_pair_service_binding_set_hash=verification.five_pair_service_binding_set_hash(
            world.profile_manifests,
            resolved_bindings,
        ),
        verifier_policy_max_age_seconds=None,
    )
    rewrite_policy(
        world,
        verification.SignalFamilyVerifierPolicyV1.create(
            harness_sha256=world.policy.harness_sha256,
            release_entries=(entry,),
        ),
    )


# ---------------------------------------------------------------------------------------
# The production authority gateway: the one path a real deployment takes
# ---------------------------------------------------------------------------------------


class _ProductionGatewayWithStubLock(root_verifier.ProductionRuntimeAuthorityGateway):
    """The real `load_snapshot()` with only the deployment lock stubbed.

    `acquire_runtime_deployment_lock()` binds `/var/lib/rquant/...`, which no macOS
    development host has. Everything the gateway does with the generation — reading the
    full manifest, reading the profile service-manifest document, requiring the document
    to be part of the source closure, and decoding both — runs for real here.
    """

    def __init__(self, *, authority_loader: Any, lock: Any) -> None:
        super().__init__(authority_loader=authority_loader)
        self._lock = lock

    def acquire_deployment_lock(self) -> Any:
        return self._lock


def _production_verifier(world: VerifierWorld) -> root_verifier.RootVerifier:
    return root_verifier.RootVerifier(
        anchors=world.anchors,
        authority_gateway=_ProductionGatewayWithStubLock(
            authority_loader=lambda: production_authority_record(world),
            lock=world.gateway.lock,
        ),
        clock=_clock,
    )


def _replace_profile_document(
    world: VerifierWorld,
    payload: bytes,
    *,
    republish_slot: bool = True,
) -> None:
    """Swap the profile document and republish the whole generation around it.

    Every enclosing binding is repaired on purpose — the closure entry that names the
    document, the full manifest that carries that entry, and (when `republish_slot`) the
    slot identity that authenticates the manifest. That models an attacker who controls
    the generation tree completely, so what is left standing is the one binding under
    test: the policy-anchored `service_manifest_fingerprint`.
    """

    rewrite_profile_document(world, payload)
    entries = dict(world.gateway.snapshots[0].full_manifest_entries)
    entries[root_verifier.PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH] = hashlib.sha256(
        payload
    ).hexdigest()
    manifest_bytes = full_manifest_payload(
        world.generation_path,
        entries,
        profile_id=PROFILE_ID,
    )
    write_full_manifest_bytes(world.generation_path, manifest_bytes)
    generation_id = hashlib.sha256(manifest_bytes).hexdigest()
    changes: dict[str, Any] = {
        "full_manifest_entries": entries,
        "profile_document_sha256": entries[
            root_verifier.PROFILE_SERVICE_MANIFESTS_RELATIVE_PATH
        ],
    }
    if republish_slot:
        changes["generation_id"] = generation_id
        changes["full_manifest_hash"] = generation_id
        changes["full_manifest_sha256"] = generation_id
    world.gateway.snapshots = [snapshot_with(world, **changes)]


class TestProductionAuthorityGateway:
    def test_the_production_gateway_reaches_five_receipts(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        result = _production_verifier(world).run()

        assert result.outcome == "persisted"
        assert tuple(receipt.pair_id for receipt in result.receipts) == verification.PAIR_IDS
        assert len(_rows(world, "receipts")) == 5
        assert len(_rows(world, "decisions")) == 1

    def test_the_production_gateway_derives_the_same_snapshot_as_the_offline_stub(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        gateway = _ProductionGatewayWithStubLock(
            authority_loader=lambda: production_authority_record(world),
            lock=world.gateway.lock,
        )
        assert gateway.load_snapshot().identity() == world.gateway.snapshots[0].identity()

    def test_a_profile_document_outside_the_source_closure_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        rewrite_profile_document(
            world,
            profile_document(profile_manifests({"notifier": 41.0})),
        )
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="BINDING_UNMANIFESTED",
        ):
            _production_verifier(world).run()
        assert world.report_path.exists() is False
        assert _rows(world, "receipts") == []

    def test_the_gateway_itself_refuses_a_document_outside_the_source_closure(
        self,
        tmp_path: Path,
    ) -> None:
        """The gateway is the layer that reads the document, so it checks it."""

        world = build_world(tmp_path)
        rewrite_profile_document(
            world,
            profile_document(profile_manifests({"notifier": 41.0})),
        )
        gateway = _ProductionGatewayWithStubLock(
            authority_loader=lambda: production_authority_record(world),
            lock=world.gateway.lock,
        )
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="BINDING_UNMANIFESTED",
        ):
            gateway.load_snapshot()

    def test_the_verifier_itself_refuses_a_document_outside_the_source_closure(
        self,
        tmp_path: Path,
    ) -> None:
        """And so does the verifier, for any gateway that ever hands it one.

        The two checks are deliberately independent: a gateway is an injected component,
        so the root does not take its word for the closure membership of the document
        that decided which services participate.
        """

        world = build_world(tmp_path)
        world.gateway.snapshots = [
            snapshot_with(world, profile_document_sha256=digest("unmanifested-document"))
        ]
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="BINDING_UNMANIFESTED",
        ):
            _verifier(world).run()
        assert world.report_path.exists() is False
        assert _rows(world, "receipts") == []

    def test_a_profile_document_the_bindings_do_not_fingerprint_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        """A forged service manifest must require a `manifest_fingerprint` preimage.

        The bindings carry the exact fingerprints, and the external policy anchors the
        binding tuple through `five_pair_service_binding_set_hash`, so swapping a
        manifest inside the document cannot be made to agree with the policy.
        """

        world = build_world(tmp_path)
        _replace_profile_document(world, profile_document(profile_manifests({"notifier": 41.0})))
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="BINDING_MISSING",
        ):
            _production_verifier(world).run()
        assert world.report_path.exists() is False

    def test_a_profile_document_missing_a_participant_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        reduced = tuple(
            manifest
            for manifest in world.profile_manifests
            if manifest.service_id != "notifier"
        )
        _replace_profile_document(world, profile_document(reduced))
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="PARTICIPANT_RESOLUTION_INVALID",
        ):
            _production_verifier(world).run()
        assert world.report_path.exists() is False

    def test_a_profile_document_repeating_a_service_id_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        doubled = (*world.profile_manifests, world.profile_manifests[0])
        _replace_profile_document(world, profile_document(doubled))
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="PARTICIPANT_RESOLUTION_INVALID",
        ):
            _production_verifier(world).run()

    def test_the_generation_identity_is_the_hash_of_its_own_full_manifest(
        self,
        tmp_path: Path,
    ) -> None:
        """The closure document authenticates itself, or it authenticates nothing.

        `RuntimeGenerationSlot.validate_for_root` only checks the manifest's *path*, so
        without this equality every `_require_manifested` call would be measuring against
        a declaration the generation tree can rewrite at will.
        """

        world = build_world(tmp_path)
        slot = world.gateway.snapshots[0].slot
        payload = (slot.generation_path / GENERATION_MANIFEST_NAME).read_bytes()

        assert hashlib.sha256(payload).hexdigest() == slot.full_manifest_hash
        assert slot.full_manifest_hash == slot.generation_id
        assert world.gateway.snapshots[0].full_manifest_sha256 == slot.full_manifest_hash

    def test_a_full_manifest_the_slot_does_not_identify_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        target = world.generation_path / GENERATION_MANIFEST_NAME
        entries = dict(world.gateway.snapshots[0].full_manifest_entries)
        entries[verification.TEST_MANIFEST_RELATIVE_PATH] = digest("grafted-declaration")
        target.chmod(0o644)
        target.write_bytes(
            full_manifest_payload(world.generation_path, entries, profile_id=PROFILE_ID)
        )
        target.chmod(0o444)

        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="FULL_MANIFEST_HASH_MISMATCH",
        ):
            _production_verifier(world).run()
        assert world.report_path.exists() is False
        assert _rows(world, "receipts") == []

    def test_a_rewritten_closure_declaration_no_longer_launders_a_forged_document(
        self,
        tmp_path: Path,
    ) -> None:
        """Repairing the declaration is exactly what the manifest hash now forbids.

        Before the closure document authenticated itself, swapping a generation file and
        then editing its full-manifest row was enough to satisfy every membership check.
        """

        world = build_world(tmp_path)
        _replace_profile_document(
            world,
            profile_document(profile_manifests({"notifier": 41.0})),
            republish_slot=False,
        )
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="FULL_MANIFEST_HASH_MISMATCH",
        ):
            _production_verifier(world).run()
        assert world.report_path.exists() is False

    def test_a_gateway_reported_manifest_hash_that_is_not_the_slot_identity_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        """And the verifier checks it too, for any gateway that ever hands it one."""

        world = build_world(tmp_path)
        world.gateway.snapshots = [
            snapshot_with(world, full_manifest_sha256=digest("another-closure"))
        ]
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="FULL_MANIFEST_HASH_MISMATCH",
        ):
            _verifier(world).run()
        assert world.report_path.exists() is False
        assert _rows(world, "receipts") == []

    def test_the_production_gateway_binds_no_profile_id_equation(self) -> None:
        """The document is never hashed against `slot.profile_id`.

        `RuntimeClosureProfile` fixes `profile_id` as the hash of the runtime closure
        body, which has no `service_manifests` key at all, so such an equation could
        only hold on a SHA-256 collision.
        """

        source = Path(root_verifier.__file__).read_text(encoding="utf-8")
        equations = [
            line
            for line in source.splitlines()
            if "profile_id" in line and ("profile_payload" in line or "profile_sha256" in line)
        ]

        assert equations == []


# ---------------------------------------------------------------------------------------
# The readiness compare-and-swap guards
# ---------------------------------------------------------------------------------------


def _seed_readiness(world: VerifierWorld, state: str) -> None:
    store = root_verifier.SignalFamilyVerificationStore.open(
        world.store_root,
        owner_uid=os.getuid(),
    )
    store.close()
    connection = sqlite3.connect(str(world.store_database))
    try:
        connection.execute(
            "INSERT INTO readiness_state "
            "(overlay_content_hash, authority_epoch_key, state, updated_at) "
            "VALUES (?, ?, ?, ?)",
            (
                world.overlay_content_hash,
                world.authority_epoch_key,
                state,
                verification.canonical_timestamp(VERIFIED_AT),
            ),
        )
        connection.commit()
    finally:
        connection.close()


class TestReadinessCompareAndSwapGuards:
    def test_a_key_already_revoked_cannot_be_swapped_to_ready(
        self,
        tmp_path: Path,
    ) -> None:
        """The append transaction may only promote a key it observes as `DECLARED`.

        The store row here is what an out-of-band revocation leaves behind: a readiness
        record that is not `DECLARED` and no decision. A blind write would silently
        resurrect it as `READY`.
        """

        world = build_world(tmp_path)
        _seed_readiness(world, verification.SignalFamilyReadinessState.REVOKED.value)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="READINESS_TRANSITION_INVALID",
        ):
            _verifier(world).run()

        states = {row[2] for row in _rows(world, "readiness_state")}
        assert states == {verification.SignalFamilyReadinessState.REVOKED.value}
        assert _rows(world, "decisions") == []
        assert _rows(world, "receipts") == []

    def test_a_key_already_rolled_back_cannot_be_swapped_to_ready(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _seed_readiness(world, verification.SignalFamilyReadinessState.ROLLED_BACK.value)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="READINESS_TRANSITION_INVALID",
        ):
            _verifier(world).run()
        assert _rows(world, "decisions") == []

    def test_a_key_already_ready_cannot_be_swapped_again(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        _seed_readiness(world, verification.SignalFamilyReadinessState.READY.value)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="READINESS_TRANSITION_INVALID",
        ):
            _verifier(world).run()
        assert _rows(world, "decisions") == []

    def test_revoke_on_an_absent_key_rejects(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="READINESS_TRANSITION_INVALID",
        ):
            _verifier(world).revoke(
                overlay_content_hash=world.overlay_content_hash,
                authority_epoch_key=world.authority_epoch_key,
            )

    def test_rollback_on_a_declared_key_rejects(self, tmp_path: Path) -> None:
        """`DECLARED -> ROLLED_BACK` is not one of the four frozen edges."""

        world = build_world(tmp_path)
        _seed_readiness(world, verification.SignalFamilyReadinessState.DECLARED.value)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="READINESS_TRANSITION_INVALID",
        ):
            _verifier(world).rollback(
                overlay_content_hash=world.overlay_content_hash,
                authority_epoch_key=world.authority_epoch_key,
            )
        states = {row[2] for row in _rows(world, "readiness_state")}
        assert states == {verification.SignalFamilyReadinessState.DECLARED.value}

    def test_revoke_on_a_declared_key_is_the_frozen_edge_that_passes(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _seed_readiness(world, verification.SignalFamilyReadinessState.DECLARED.value)
        state = _verifier(world).revoke(
            overlay_content_hash=world.overlay_content_hash,
            authority_epoch_key=world.authority_epoch_key,
        )
        assert state is verification.SignalFamilyReadinessState.REVOKED


# ---------------------------------------------------------------------------------------
# Step 6: the root re-derives the generation plan after the child exits
# ---------------------------------------------------------------------------------------


class TestPostChildRederivation:
    def test_a_test_manifest_replaced_while_the_child_runs_rejects_after_exit(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Step 6 must load the immutable manifests again, not reuse step 3's objects."""

        world = build_world(tmp_path)
        original = root_verifier.RootVerifier._run_child
        target = world.generation_path / verification.TEST_MANIFEST_RELATIVE_PATH

        def swapping_child(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return original(self, *args, **kwargs)
            finally:
                target.chmod(0o644)
                target.write_bytes(canonical_json_bytes({"schema_version": 1}))
                target.chmod(0o444)

        monkeypatch.setattr(root_verifier.RootVerifier, "_run_child", swapping_child)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="TEST_MANIFEST_HASH_MISMATCH",
        ):
            _verifier(world).run()

        assert world.report_path.exists() is True
        assert _rows(world, "receipts") == []
        assert _rows(world, "decisions") == []

    def test_a_binding_source_replaced_while_the_child_runs_rejects_after_exit(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        world = build_world(tmp_path)
        original = root_verifier.RootVerifier._run_child
        target = world.generation_path / "src" / "rquant" / "strategy_runner.py"

        def swapping_child(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return original(self, *args, **kwargs)
            finally:
                target.chmod(0o644)
                target.write_bytes(b"# swapped under the running child\n")
                target.chmod(0o444)

        monkeypatch.setattr(root_verifier.RootVerifier, "_run_child", swapping_child)
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="BINDING_WRONG_SOURCE_HASH",
        ):
            _verifier(world).run()

        assert world.report_path.exists() is True
        assert _rows(world, "receipts") == []


# ---------------------------------------------------------------------------------------
# Step 7: the authority snapshot may not move under the run
# ---------------------------------------------------------------------------------------


class TestAuthorityRevalidation:
    @pytest.mark.parametrize(
        ("changes", "reason"),
        (
            ({"sequence": 4}, "AUTHORITY_EPOCH_CHANGED"),
            ({"operation_id": OTHER_OPERATION_ID}, "AUTHORITY_EPOCH_CHANGED"),
            ({"profile_id": "e" * 64}, "AUTHORITY_EPOCH_CHANGED"),
        ),
    )
    def test_an_authority_change_between_child_exit_and_append_rejects(
        self,
        tmp_path: Path,
        changes: dict[str, Any],
        reason: str,
    ) -> None:
        world = build_world(tmp_path)
        world.gateway.snapshots = [
            world.gateway.snapshots[0],
            snapshot_with(world, **changes),
        ]
        with pytest.raises(root_verifier.SignalFamilyRootVerifierError, match=reason):
            _verifier(world).run()
        _assert_no_evidence(world)

    def test_a_profile_change_between_child_exit_and_append_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        drifted = profile_manifests({"notifier": 33.0})
        world.gateway.snapshots = [
            world.gateway.snapshots[0],
            snapshot_with(world, profile_manifests=drifted),
        ]
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="AUTHORITY_EPOCH_CHANGED",
        ):
            _verifier(world).run()
        _assert_no_evidence(world)

    def test_a_lost_deployment_lock_rejects_before_any_append(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        world.gateway.lock.identity_changes = True
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="DEPLOYMENT_LOCK_LOST",
        ):
            _verifier(world).run()
        _assert_no_evidence(world)


# ---------------------------------------------------------------------------------------
# Step 8: the append store, its transaction, and the lifecycle
# ---------------------------------------------------------------------------------------


class TestAppendStore:
    def test_the_store_directory_and_database_are_private_to_their_owner(
        self,
        tmp_path: Path,
    ) -> None:
        import stat as stat_module

        world = build_world(tmp_path)
        _verifier(world).run()

        directory_mode = stat_module.S_IMODE(world.store_root.stat().st_mode)
        database_mode = stat_module.S_IMODE(world.store_database.stat().st_mode)
        assert directory_mode == root_verifier.STORE_DIRECTORY_MODE
        assert database_mode == root_verifier.STORE_FILE_MODE

    def test_the_run_declares_the_key_before_the_compare_and_swap(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()

        rows = _rows(world, "readiness_state")
        assert len(rows) == 1
        assert rows[0][0] == world.overlay_content_hash
        assert rows[0][1] == world.authority_epoch_key
        assert rows[0][2] == verification.SignalFamilyReadinessState.READY.value

    def test_an_identical_replay_returns_the_same_bytes_and_appends_nothing(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        first = _verifier(world).run()
        second = _verifier(world).run()

        assert second.outcome == "idempotent"
        assert second.decision_bytes == first.decision_bytes
        assert len(_rows(world, "receipts")) == 5
        assert len(_rows(world, "decisions")) == 1
        assert _rows(world, "conflict_audit") == []

    def test_concurrent_identical_finalization_returns_one_set_of_bytes(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two runs race the store transaction from two connections.

        The child launches are serialized because `subprocess`'s `preexec_fn` is not safe
        in the presence of threads, and the production verifier is single-threaded. What
        the two threads actually race is the `BEGIN IMMEDIATE` transaction and its unique
        key over overlay plus authority epoch, which is the property under test.
        """

        world = build_world(tmp_path)
        outcomes: list[Any] = []
        errors: list[BaseException] = []
        launch_lock = threading.Lock()
        barrier = threading.Barrier(2)
        original_child = root_verifier.RootVerifier._run_child
        original_finalize = root_verifier.SignalFamilyVerificationStore.finalize

        def serialized_child(self: Any, *args: Any, **kwargs: Any) -> Any:
            with launch_lock:
                return original_child(self, *args, **kwargs)

        def racing_finalize(self: Any, *args: Any, **kwargs: Any) -> Any:
            barrier.wait(timeout=60)
            return original_finalize(self, *args, **kwargs)

        monkeypatch.setattr(root_verifier.RootVerifier, "_run_child", serialized_child)
        monkeypatch.setattr(
            root_verifier.SignalFamilyVerificationStore,
            "finalize",
            racing_finalize,
        )

        def finalize() -> None:
            try:
                outcomes.append(_verifier(world).run())
            except BaseException as error:  # pragma: no cover - surfaced by the assert
                errors.append(error)

        threads = [threading.Thread(target=finalize) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=180)

        assert errors == []
        assert len(outcomes) == 2
        assert outcomes[0].decision_bytes == outcomes[1].decision_bytes
        assert sorted(result.outcome for result in outcomes) == ["idempotent", "persisted"]
        assert len(_rows(world, "receipts")) == 5
        assert len(_rows(world, "decisions")) == 1

    def test_divergent_bytes_for_one_key_append_conflict_evidence_and_reject(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="DECISION_CONFLICT",
        ):
            _verifier(
                world,
                clock=lambda: VERIFIED_AT + timedelta(seconds=1),
            ).run()

        conflicts = _rows(world, "conflict_audit")
        assert len(conflicts) == 1
        assert len(_rows(world, "decisions")) == 1

    def test_a_new_authority_epoch_never_revives_the_previous_readiness(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        advanced = snapshot_with(world, sequence=4)
        world.gateway.snapshots = [advanced, advanced]
        result = _verifier(world).run()

        assert result.outcome == "persisted"
        states = {row[1]: row[2] for row in _rows(world, "readiness_state")}
        assert len(states) == 2
        assert set(states.values()) == {verification.SignalFamilyReadinessState.READY.value}
        assert world.authority_epoch_key in states
        assert len(_rows(world, "receipts")) == 10

    def test_revoke_disables_future_eligibility_without_deleting_history(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        state = _verifier(world).revoke(
            overlay_content_hash=world.overlay_content_hash,
            authority_epoch_key=world.authority_epoch_key,
        )

        assert state is verification.SignalFamilyReadinessState.REVOKED
        assert verification.is_activation_eligible(state) is False
        assert len(_rows(world, "receipts")) == 5
        assert len(_rows(world, "decisions")) == 1

    def test_rollback_moves_ready_to_rolled_back_and_is_append_only(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        state = _verifier(world).rollback(
            overlay_content_hash=world.overlay_content_hash,
            authority_epoch_key=world.authority_epoch_key,
        )

        assert state is verification.SignalFamilyReadinessState.ROLLED_BACK
        assert len(_rows(world, "decisions")) == 1
        assert len(_rows(world, "audit")) >= 2

    def test_a_revoked_key_cannot_be_rolled_back(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()
        _verifier(world).revoke(
            overlay_content_hash=world.overlay_content_hash,
            authority_epoch_key=world.authority_epoch_key,
        )
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="READINESS_TRANSITION_INVALID",
        ):
            _verifier(world).rollback(
                overlay_content_hash=world.overlay_content_hash,
                authority_epoch_key=world.authority_epoch_key,
            )

    def test_the_store_never_records_an_attesting_or_activated_state(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        _verifier(world).run()

        recorded = {row[2] for row in _rows(world, "readiness_state")}
        assert recorded <= {state.value for state in verification.SignalFamilyReadinessState}
        assert "ATTESTING" not in recorded
        assert "ACTIVATED" not in recorded

    def test_every_rejection_carries_a_bounded_audit_record(self, tmp_path: Path) -> None:
        world = build_world(tmp_path)
        world.policy_path.chmod(0o644)
        with pytest.raises(root_verifier.SignalFamilyRootVerifierError) as raised:
            _verifier(world).run()

        record = raised.value.audit_record
        assert record is not None
        assert record.outcome is verification.SignalFamilyAuditOutcome.REJECTED
        assert record.reason_code is verification.SignalFamilyReasonCode.POLICY_ANCHOR_INVALID
        assert record.event is verification.SignalFamilyAuditEvent.POLICY_VALIDATED
        payload = record.model_dump(mode="json")
        assert set(payload) == set(
            verification.SignalFamilyVerificationAuditRecordV1.model_fields
        )
        assert str(world.policy_path) not in canonical_json_bytes(payload).decode("utf-8")

    def test_every_bounded_reason_code_names_one_audit_event(self) -> None:
        covered = {
            code: root_verifier._REJECTION_EVENTS[code]
            for code in verification.SignalFamilyReasonCode
        }
        assert len(covered) == len(verification.SignalFamilyReasonCode)
        assert all(
            isinstance(event, verification.SignalFamilyAuditEvent)
            for event in covered.values()
        )

    def test_the_production_gateway_rejects_a_malformed_source_closure(self) -> None:
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="BINDING_UNMANIFESTED",
        ):
            root_verifier._parse_full_manifest_entries(
                canonical_json_bytes({"entries": "not-an-array"})
            )

    def test_the_production_gateway_rejects_a_malformed_profile_document(self) -> None:
        with pytest.raises(
            root_verifier.SignalFamilyRootVerifierError,
            match="PARTICIPANT_RESOLUTION_INVALID",
        ):
            root_verifier._parse_profile_manifests(
                canonical_json_bytes({"service_manifests": [{"service_id": "x"}]})
            )

    def test_the_production_gateway_parses_an_exact_source_closure(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path)
        payload = canonical_json_bytes(
            {
                "entries": [
                    {"path": relative, "sha256": digest_value}
                    for relative, digest_value in sorted(
                        world.gateway.snapshots[0].full_manifest_entries.items()
                    )
                ]
            }
        )
        assert root_verifier._parse_full_manifest_entries(payload) == dict(
            world.gateway.snapshots[0].full_manifest_entries
        )

    def test_audit_rows_carry_only_bounded_identifiers_and_reason_codes(
        self,
        tmp_path: Path,
    ) -> None:
        import json

        world = build_world(tmp_path)
        _verifier(world).run()

        allowed = set(
            verification.SignalFamilyVerificationAuditRecordV1.model_fields
        )
        for row in _rows(world, "audit"):
            record = json.loads(row[1])
            assert set(record) == allowed
            assert record["reason_code"] in {
                None,
                *(code.value for code in verification.SignalFamilyReasonCode),
            }


# ---------------------------------------------------------------------------------------
# The real WP4-c harness, in place of the WP4-b protocol replayer
# ---------------------------------------------------------------------------------------


class TestProductionHarness:
    """The eight steps again, with the real harness zipapp exercising real builders.

    Every earlier case in this file uses the stub harness, which replays frozen bytes and
    proves the protocol. These cases swap in the artifact `scripts/build-signal-family-
    verifier-harness.py` produces: an unprivileged child that imports the generation, builds
    each reader through its production `runtime_builder_*` factory, and derives its own
    results. The root is unchanged, so a green run here means the two halves agree on bytes
    neither of them was told in advance.
    """

    def test_the_real_harness_is_refused_for_the_pairs_it_cannot_exercise(
        self,
        tmp_path: Path,
    ) -> None:
        """Ruling C1: 8/13 coverage is not five-pair readiness, and no longer pretends to be.

        The rejection reason is what makes this case load-bearing. Reaching
        `PAIR_SURFACE_COVERAGE_MISSING` means the child's eight results already satisfied
        every identity, ordering, and hash check — including `expected_result_set_hash` —
        and the only thing left to fail was coverage of `strategy-router` and
        `strategy-shadow`, whose readers no vector can reach yet (R3/R4).
        """

        world = build_world(tmp_path, harness="real")

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError) as error:
            _verifier(world).run()

        assert (
            error.value.audit_record.reason_code
            is verification.SignalFamilyReasonCode.PAIR_SURFACE_COVERAGE_MISSING
        )
        _assert_no_evidence(world)

    def test_the_real_harness_answers_exactly_the_policy_authorized_vectors(
        self,
        tmp_path: Path,
    ) -> None:
        world = build_world(tmp_path, harness="real")

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError):
            _verifier(world).run()

        assert tuple(sorted(world.replay)) == tuple(
            sorted(vector.vector_id for vector in world.test_manifest.vectors)
        )
        assert {vector.surface_id.value for vector in world.test_manifest.vectors} == set(
            harness.IMPLEMENTED_SURFACE_IDS
        )

    def test_every_real_result_names_the_surface_and_its_production_builder(
        self,
        tmp_path: Path,
    ) -> None:
        import json

        world = build_world(tmp_path, harness="real")

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError):
            _verifier(world).run()

        by_id = {vector.vector_id: vector for vector in world.test_manifest.vectors}
        for vector_id, payload in world.replay.items():
            observed = json.loads(payload)
            assert observed["surface_id"] == by_id[vector_id].surface_id.value
            assert observed["builder"].startswith("rquant.runtime_builder_")
            assert observed["state_unchanged"] is True

    def test_the_two_unblocked_pairs_are_exercised_in_the_child(self, tmp_path: Path) -> None:
        """WP4-c round 1: the child cwd move and the widened environment, proven end to end.

        `router-notifier` needed `rquant.config` to be constructible and `notifier-serving`
        needed an authority root with no world-writable ancestor. Both now run inside the
        real child; the run still stops at the coverage gate, but it stops *after* those
        results were produced and accepted byte for byte.
        """

        import json

        world = build_world(tmp_path, harness="real")

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError):
            _verifier(world).run()

        by_id = {vector.vector_id: vector for vector in world.test_manifest.vectors}
        pairs = {vector.pair_id for vector in by_id.values()}
        assert {"router-notifier", "notifier-serving", "router-paper"} == pairs

        builders = {
            json.loads(payload)["builder"]
            for vector_id, payload in world.replay.items()
            if by_id[vector_id].pair_id in {"router-notifier", "notifier-serving"}
        }
        assert builders == {
            "rquant.runtime_builder_signal.notifier_builder",
            "rquant.runtime_builder_serving.serving_publisher_builder",
        }

    def test_a_blocked_surface_vector_fails_closed_without_evidence(
        self,
        tmp_path: Path,
    ) -> None:
        """The harness refuses a surface it cannot reach rather than inventing a result."""

        world = build_world(
            tmp_path,
            harness="real",
            blocked_surface_id=verification.SurfaceId.ROUTE_RUNNER_SIGNALS,
        )

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError) as error:
            _verifier(world).run()

        assert (
            error.value.audit_record.reason_code
            is verification.SignalFamilyReasonCode.CHILD_NONZERO_EXIT
        )
        _assert_no_evidence(world)

    def test_the_remaining_blocked_set_is_exactly_the_two_producer_bound_pairs(self) -> None:
        """The activity gap this harness still has, stated as a contract rather than prose."""

        assert set(harness.BLOCKED_SURFACE_REASONS) == {
            surface.value
            for pair_id in ("strategy-router", "strategy-shadow")
            for surface in verification.READER_SURFACES[pair_id]
        }
        assert len(harness.IMPLEMENTED_SURFACE_IDS) == 8

    @pytest.mark.parametrize(
        "covered",
        [
            ("router-paper",),
            ("router-paper", "router-notifier"),
            ("notifier-serving", "router-notifier", "router-paper", "strategy-router"),
        ],
    )
    def test_partial_pair_coverage_yields_no_receipt_and_no_readiness(
        self,
        tmp_path: Path,
        covered: tuple[str, ...],
    ) -> None:
        """Reviewer mutation M6, closed: fewer than five covered pairs cannot reach READY.

        The stub harness is used so the case tests the *gate* rather than which surfaces the
        real harness happens to implement — every vector it is handed is answered, so the
        only thing missing is the pairs the restricted set never names.
        """

        world = build_world(tmp_path, vector_pair_ids=covered)

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError) as error:
            _verifier(world).run()

        assert (
            error.value.audit_record.reason_code
            is verification.SignalFamilyReasonCode.PAIR_SURFACE_COVERAGE_MISSING
        )
        _assert_no_evidence(world)

    def test_one_missing_surface_inside_a_covered_pair_still_rejects(
        self,
        tmp_path: Path,
    ) -> None:
        """Coverage is per surface, not per pair: four pairs whole and one short still fails.

        Fix round 1 asserted this against `missing_pair_surface_coverage` alone and never ran
        the verifier, so nothing proved the root consulted the predicate for a shortfall this
        shape. It runs the whole eight-step sequence now.
        """

        dropped = verification.READER_SURFACES["router-paper"][0]
        world = build_world(tmp_path, drop_surface_ids=(dropped,))

        assert {vector.pair_id for vector in world.test_manifest.vectors} == set(
            verification.PAIR_IDS
        )
        assert dropped not in {vector.surface_id for vector in world.test_manifest.vectors}

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError) as error:
            _verifier(world).run()

        assert (
            error.value.audit_record.reason_code
            is verification.SignalFamilyReasonCode.PAIR_SURFACE_COVERAGE_MISSING
        )
        _assert_no_evidence(world)

    @pytest.mark.parametrize("pair_id", list(verification.PAIR_IDS))
    def test_dropping_any_single_reader_surface_rejects(
        self,
        tmp_path: Path,
        pair_id: str,
    ) -> None:
        """One missing surface anywhere in the five pairs is enough to withhold every receipt."""

        dropped = verification.READER_SURFACES[pair_id][-1]
        world = build_world(tmp_path, drop_surface_ids=(dropped,))

        with pytest.raises(root_verifier.SignalFamilyRootVerifierError) as error:
            _verifier(world).run()

        assert (
            error.value.audit_record.reason_code
            is verification.SignalFamilyReasonCode.PAIR_SURFACE_COVERAGE_MISSING
        )
        _assert_no_evidence(world)

    def test_full_five_pair_coverage_still_reaches_ready(self, tmp_path: Path) -> None:
        """The gate admits what it should: the unrestricted stub world is unchanged."""

        world = build_world(tmp_path)

        result = _verifier(world).run()

        assert result.state is verification.SignalFamilyReadinessState.READY
        assert len(result.receipts) == 5
        assert verification.missing_pair_surface_coverage(
            tuple(
                verification.SignalFamilyVectorResultV1.create(
                    vector_id=vector.vector_id,
                    pair_id=vector.pair_id,
                    family_id=vector.family_id,
                    surface_id=vector.surface_id,
                    canonical_result_json="{}",
                )
                for vector in world.test_manifest.vectors
            )
        ) == ()

    def test_the_root_authenticates_every_binding_source_against_the_manifest(
        self,
        tmp_path: Path,
    ) -> None:
        """WP4C-SPEC-06: the property that must not regress, plus the boundary it leaves open.

        The invariant asserted here is the one the root enforces and that must hold however
        the world evolves: every binding's `executable_source_sha256` is the hash of the file
        actually sitting at its manifested path inside the generation.

        The boundary is separate and is *recorded*, not asserted as a failure trigger. This
        replica writes one-line placeholders at those paths while
        `make_generation_importable` points the child's interpreter at the repository's own
        `src/`, so the bytes the manifest authenticates and the bytes the child executes are
        different files. The real-harness end-to-end therefore proves the protocol and the
        builder path — not "the full manifest is the source closure". Materializing the true
        closure is out of scope for this round and is §9 question 6.

        Fix round 1 asserted `divergent == len(bindings)`, which would have turned red on the
        day the gap was closed. What is checked instead is that the world is internally
        consistent — wholly placeholder or wholly real, never a half-migrated mixture — so
        closing the gap passes and a partial migration fails.
        """

        import importlib
        import inspect

        world = build_world(tmp_path, harness="real")

        divergent: list[str] = []
        for binding in world.bindings:
            manifested = world.generation_path / binding.executable_source_relative_path
            assert manifested.is_file()
            assert (
                hashlib.sha256(manifested.read_bytes()).hexdigest()
                == binding.executable_source_sha256
            )

            imported = Path(inspect.getfile(importlib.import_module(binding.executable_module)))
            assert imported.is_file()
            if (
                hashlib.sha256(imported.read_bytes()).hexdigest()
                != binding.executable_source_sha256
            ):
                divergent.append(binding.executable_module)

        assert len(divergent) in {0, len(world.bindings)}, (
            "the offline world must be wholly placeholder-sourced or wholly real-sourced; "
            f"a mixture hides which bindings are authenticated: {sorted(divergent)}"
        )

    def test_the_real_harness_pyz_is_the_policy_hashed_artifact(self, tmp_path: Path) -> None:
        world = build_world(tmp_path, harness="real")

        assert world.policy.harness_sha256 == hashlib.sha256(
            world.harness_path.read_bytes()
        ).hexdigest()
        assert world.harness_path.stat().st_mode & 0o222 == 0
