"""Phase C readiness, receipt, and set-hash red tests.

Covers `RESET-REG-P0`, `RESET-REG-P2`, and `RESET-REG-P2-01` at the pure model and
arithmetic layer: the frozen Phase C schemas and their strict rejection matrices, the four
policy-bound set-hash preimages, the authority epoch key, the execution-evidence preimage,
the receipt fingerprint and its uniqueness key, profile-derived freshness, the exact
five-pair resolution, and the minimal lifecycle with no `ATTESTING`/`ACTIVATED` state.

Every rejection case makes its own rule the first failing check — a variant that changes a
hashed field recomputes the affected `*_hash` / `*_fingerprint` so a hash mismatch can never
stand in for the rule under test — and pins the exact rejection reason with `match=`.

The root verifier process, its IPC transport, the append store, and the fixed harness are
deliberately absent here; this module owns only the models and the arithmetic they bind.
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from rquant import signal_family_successor_registry as successor
from rquant import signal_family_verification as verification
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.strict_json import canonical_json_bytes

REJECTIONS = (ValueError, TypeError)

PRODUCER_COMMIT = "a" * 40
OPERATION_ID = "0" * 32
OTHER_OPERATION_ID = "1" * 32
GENERATION_ID = "c" * 64
PROFILE_ID = "d" * 64
VERIFIED_AT = datetime(2026, 8, 24, 7, 30, 15, 250000, tzinfo=UTC)

# The exact pair-to-surface table of authority.md L1211-1217, transcribed row by row.
EXPECTED_PAIR_SURFACE_TABLE: tuple[tuple[str, tuple[str, ...], tuple[str, ...]], ...] = (
    (
        "strategy-router",
        ("rquant.strategy_runner.StrategyRunnerStore.process_batch",),
        (
            "rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource.read_batch",
            "rquant.signal_router_runtime.route_runner_signals",
        ),
    ),
    (
        "strategy-shadow",
        (
            "rquant.strategy_runner.StrategyRunnerStore.process_batch",
            "rquant.strategy_runner.StrategyRunnerStore.publish_session_close_receipt",
        ),
        (
            "rquant.runtime_builder_shadow._FilesystemRunnerSource.read_completed_batch",
            "rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot",
            "rquant.runtime_shadow_sources.isolated_signal_observations",
        ),
    ),
    (
        "router-notifier",
        (
            "rquant.signal_route_spool.SignalRouteSpool.publish",
            "rquant.signal_route_spool.publish_signal_bus_prefix",
        ),
        (
            "rquant.signal_route_spool.ReadonlySignalRouteSpool.routed_after_global_sequence",
            "rquant.notification_state.NotificationStateStore.replicate",
        ),
    ),
    (
        "router-paper",
        (
            "rquant.signal_route_spool.SignalRouteSpool.publish",
            "rquant.signal_route_spool.publish_signal_bus_prefix",
        ),
        (
            "rquant.signal_route_spool.ReadonlySignalRouteSpool.signals_after_global_sequence",
            "rquant.paper_signal_consumer.consume_signal_bus_to_paper",
            "rquant.paper_signal_worker.PaperSignalQueueStore.ingest",
        ),
    ),
    (
        "notifier-serving",
        (
            "rquant.runtime_builder_signal._publish_signal_authority",
            "rquant.runtime_serving_authority.ServingSourceAuthorityPublisher.publish",
        ),
        (
            "rquant.runtime_serving_authority.ServingSourceAuthorityReader.__call__",
            "rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble",
            "rquant.serving_read_models.build_serving_read_models",
        ),
    ),
)

_PARTICIPANTS: tuple[tuple[str, RuntimeServiceKind, float], ...] = (
    ("strategy.alpha.v1", RuntimeServiceKind.STRATEGY_LIVE, 90.0),
    ("strategy.beta.v1", RuntimeServiceKind.STRATEGY_LIVE, 120.0),
    ("signal-router", RuntimeServiceKind.SIGNAL_ROUTER, 60.0),
    ("shadow-session", RuntimeServiceKind.SHADOW_SESSION, 45.0),
    ("notifier", RuntimeServiceKind.NOTIFIER, 75.0),
    ("paper-broker", RuntimeServiceKind.PAPER_BROKER, 180.0),
    ("serving-publisher", RuntimeServiceKind.SERVING_PUBLISHER, 50.0),
)
_BYSTANDERS: tuple[tuple[str, RuntimeServiceKind, float], ...] = (
    ("feature-live", RuntimeServiceKind.FEATURE_LIVE, 5.0),
    ("paper-consumer", RuntimeServiceKind.PAPER_CONSUMER, 7.0),
)
_ROLE_BY_KIND = {
    RuntimeServiceKind.STRATEGY_LIVE: "strategy",
    RuntimeServiceKind.SIGNAL_ROUTER: "router",
    RuntimeServiceKind.SHADOW_SESSION: "shadow",
    RuntimeServiceKind.NOTIFIER: "notifier",
    RuntimeServiceKind.PAPER_BROKER: "paper-broker",
    RuntimeServiceKind.SERVING_PUBLISHER: "serving",
}
_MODULE_BY_KIND = {
    RuntimeServiceKind.STRATEGY_LIVE: "rquant.strategy_runner",
    RuntimeServiceKind.SIGNAL_ROUTER: "rquant.signal_router_runtime",
    RuntimeServiceKind.SHADOW_SESSION: "rquant.runtime_shadow_sources",
    RuntimeServiceKind.NOTIFIER: "rquant.notification_state",
    RuntimeServiceKind.PAPER_BROKER: "rquant.paper_signal_worker",
    RuntimeServiceKind.SERVING_PUBLISHER: "rquant.serving_read_models",
}
_SOURCE_BY_KIND = {
    kind: f"src/{module.replace('.', '/')}.py" for kind, module in _MODULE_BY_KIND.items()
}


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _manifest(
    service_id: str,
    kind: RuntimeServiceKind,
    stale_after_seconds: float,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1.0,
        stale_after_seconds=stale_after_seconds,
        producer_commit=PRODUCER_COMMIT,
        settings={},
    )


def _profile_manifests(
    overrides: dict[str, float] | None = None,
    *,
    participants: tuple[tuple[str, RuntimeServiceKind, float], ...] = _PARTICIPANTS,
    with_bystanders: bool = True,
) -> tuple[RuntimeServiceManifest, ...]:
    stale = dict(overrides or {})
    rows = list(participants)
    if with_bystanders:
        rows.extend(_BYSTANDERS)
    return tuple(
        _manifest(service_id, kind, stale.get(service_id, default_stale))
        for service_id, kind, default_stale in rows
    )


def _pairs() -> tuple[verification.PairBindingV1, ...]:
    return verification.resolve_pair_bindings(_profile_manifests())


def _binding(
    manifest: RuntimeServiceManifest,
    pairs: tuple[verification.PairBindingV1, ...],
) -> verification.VerificationServiceBindingV1:
    kind = manifest.service_kind
    return verification.VerificationServiceBindingV1.create(
        service_id=manifest.service_id,
        runtime_service_kind=kind,
        role_name=_ROLE_BY_KIND[kind],
        service_manifest_fingerprint=manifest.manifest_fingerprint,
        executable_module=_MODULE_BY_KIND[kind],
        executable_source_relative_path=_SOURCE_BY_KIND[kind],
        executable_source_sha256=_digest(f"source:{_SOURCE_BY_KIND[kind]}"),
        surface_ids=verification.expected_surface_ids(pairs)[manifest.service_id],
    )


def _bindings(
    pairs: tuple[verification.PairBindingV1, ...] | None = None,
) -> tuple[verification.VerificationServiceBindingV1, ...]:
    resolved = _pairs() if pairs is None else pairs
    participating = verification.participating_service_ids(resolved)
    by_id = {manifest.service_id: manifest for manifest in _profile_manifests()}
    return tuple(_binding(by_id[service_id], resolved) for service_id in participating)


def _vectors() -> tuple[verification.SignalFamilyVectorV1, ...]:
    family_id = successor.ACCEPTED_FAMILY_IDS[0]
    built = [
        verification.SignalFamilyVectorV1.create(
            pair_id=pair.pair_id,
            family_id=family_id,
            surface_id=surface_id,
            input_json=canonical_json_bytes(
                {"pair": pair.pair_id, "surface": surface_id.value}
            ).decode("utf-8"),
        )
        for pair in _pairs()
        for surface_id in verification.READER_SURFACES[pair.pair_id]
    ]
    return tuple(sorted(built, key=lambda vector: vector.vector_id))


def _expected_results(
    vectors: tuple[verification.SignalFamilyVectorV1, ...] | None = None,
) -> tuple[verification.SignalFamilyExpectedResultV1, ...]:
    selected = _vectors() if vectors is None else vectors
    return tuple(
        verification.SignalFamilyExpectedResultV1(
            vector_id=vector.vector_id,
            canonical_result_sha256=_digest(f"result:{vector.vector_id}"),
        )
        for vector in selected
    )


def _test_manifest() -> verification.SignalFamilyTestManifestV1:
    return verification.SignalFamilyTestManifestV1.create(
        vectors=_vectors(),
        expected_results=_expected_results(),
        pairs=_pairs(),
        service_bindings=_bindings(),
    )


def _authority(
    *,
    operation_id: str = OPERATION_ID,
    sequence: int = 3,
    generation_id: str = GENERATION_ID,
    profile_id: str = PROFILE_ID,
) -> verification.AuthoritySnapshotV1:
    return verification.AuthoritySnapshotV1.create(
        operation_id=operation_id,
        sequence=sequence,
        authority_state="active",
        generation_id=generation_id,
        generation_lifecycle="active",
        full_manifest_hash=generation_id,
        profile_id=profile_id,
        role_names=("notifier", "paper-broker", "router", "serving", "shadow", "strategy"),
    )


def _child_result(
    vectors: tuple[verification.SignalFamilyVectorV1, ...] | None = None,
) -> verification.SignalFamilyChildResultV1:
    selected = _vectors() if vectors is None else vectors
    results = tuple(
        verification.SignalFamilyVectorResultV1.create(
            vector_id=vector.vector_id,
            pair_id=vector.pair_id,
            family_id=vector.family_id,
            surface_id=vector.surface_id,
            canonical_result_json=canonical_json_bytes(
                {"observed": vector.vector_id}
            ).decode("utf-8"),
        )
        for vector in selected
    )
    return verification.SignalFamilyChildResultV1.create(
        run_id=_digest("run"),
        test_manifest_hash=_digest("test-manifest"),
        vector_results=results,
    )


def _entry(
    *,
    max_age: int | None = None,
    manifest: verification.SignalFamilyTestManifestV1 | None = None,
) -> verification.ReleaseVerificationEntryV1:
    resolved = _test_manifest() if manifest is None else manifest
    return verification.ReleaseVerificationEntryV1.create(
        successor_bundle_content_hash=_digest("successor-bundle"),
        overlay_content_hash=_digest("overlay-bundle"),
        verification_manifest_sha256=_digest("verification-manifest"),
        vector_set_hash=verification.vector_set_hash(resolved.vectors),
        expected_result_set_hash=verification.expected_result_set_hash(resolved.expected_results),
        five_pair_service_binding_set_hash=verification.five_pair_service_binding_set_hash(
            resolved.pairs,
            resolved.service_bindings,
        ),
        verifier_policy_max_age_seconds=max_age,
    )


def _policy(
    entries: tuple[verification.ReleaseVerificationEntryV1, ...] | None = None,
) -> verification.SignalFamilyVerifierPolicyV1:
    return verification.SignalFamilyVerifierPolicyV1.create(
        harness_sha256=_digest("harness"),
        release_entries=(_entry(),) if entries is None else entries,
    )


def _snapshot(
    *,
    authority: verification.AuthoritySnapshotV1 | None = None,
    max_age: int | None = None,
    stale_overrides: dict[str, float] | None = None,
    verified_at: datetime = VERIFIED_AT,
) -> verification.SignalFamilyVerificationSnapshotV1:
    manifests = _profile_manifests(stale_overrides)
    pairs = verification.resolve_pair_bindings(manifests)
    bindings = _bindings(pairs)
    test_manifest = verification.SignalFamilyTestManifestV1.create(
        vectors=_vectors(),
        expected_results=_expected_results(),
        pairs=pairs,
        service_bindings=bindings,
    )
    entry = _entry(max_age=max_age, manifest=test_manifest)
    policy = _policy((entry,))
    resolved_authority = _authority() if authority is None else authority
    child = _child_result()
    participating = verification.participating_service_ids(pairs)
    service_manifests = verification.resolve_participating_service_manifests(
        manifests,
        participating,
    )
    freshness = verification.freshness_seconds(
        verification.service_freshness_seconds(service_manifests),
        entry.verifier_policy_max_age_seconds,
    )
    return verification.SignalFamilyVerificationSnapshotV1.create(
        authority=resolved_authority,
        overlay_content_hash=entry.overlay_content_hash,
        successor_bundle_content_hash=entry.successor_bundle_content_hash,
        successor_declaration_hashes=(_digest("declaration-a"), _digest("declaration-b")),
        successor_channel_hashes=(_digest("channel-a"), _digest("channel-b")),
        verification_manifest_sha256=entry.verification_manifest_sha256,
        test_manifest_hash=child.test_manifest_hash,
        pairs=pairs,
        test_manifest=test_manifest,
        child_result=child,
        policy=policy,
        selected_entry=entry,
        service_manifests=service_manifests,
        verified_at=verified_at,
        freshness_seconds=freshness,
    )


# --------------------------------------------------------------------------------------
# Frozen constants: the callable allowlist, the pair map, and the R-O3 manifest paths
# --------------------------------------------------------------------------------------


def test_canonical_sha256_is_the_phase_b_definition() -> None:
    assert verification.canonical_sha256 is successor._canonical_sha256
    value = {"note": "现", "pair": "router-paper"}
    assert (
        verification.canonical_sha256(value)
        == hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    )


def test_pair_ids_are_the_frozen_phase_b_five() -> None:
    assert verification.PAIR_IDS is successor.PAIR_IDS
    assert verification.PAIR_IDS == (
        "notifier-serving",
        "router-notifier",
        "router-paper",
        "strategy-router",
        "strategy-shadow",
    )


def test_surface_enum_is_closed_over_the_exact_pair_to_surface_table() -> None:
    declared = {
        qualname
        for _, producers, readers in EXPECTED_PAIR_SURFACE_TABLE
        for qualname in (*producers, *readers)
    }
    assert {member.value for member in verification.SurfaceId} == declared
    assert len(tuple(verification.SurfaceId)) == 19


@pytest.mark.parametrize(
    ("pair_id", "producers", "readers"),
    EXPECTED_PAIR_SURFACE_TABLE,
    ids=[row[0] for row in EXPECTED_PAIR_SURFACE_TABLE],
)
def test_each_pair_row_binds_its_exact_producer_and_reader_surfaces(
    pair_id: str,
    producers: tuple[str, ...],
    readers: tuple[str, ...],
) -> None:
    assert tuple(
        surface.value for surface in verification.PRODUCER_SURFACES[pair_id]
    ) == producers
    assert tuple(surface.value for surface in verification.READER_SURFACES[pair_id]) == readers
    assert set(verification.PRODUCER_SURFACES[pair_id]).isdisjoint(
        verification.READER_SURFACES[pair_id]
    )


def test_producer_surfaces_never_count_as_reader_receipt_surfaces() -> None:
    producers = {
        surface for pair_id in verification.PAIR_IDS
        for surface in verification.PRODUCER_SURFACES[pair_id]
    }
    readers = {
        surface for pair_id in verification.PAIR_IDS
        for surface in verification.READER_SURFACES[pair_id]
    }
    assert producers.isdisjoint(readers)
    assert len(producers) == 6
    assert len(readers) == 13


def test_frozen_generation_relative_paths_are_the_ruled_v1_locations() -> None:
    assert verification.SUCCESSOR_BUNDLE_RELATIVE_PATH == (
        "signal-family/successor-bundle-v1.json"
    )
    assert verification.OVERLAY_BUNDLE_RELATIVE_PATH == "signal-family/overlay-bundle-v1.json"
    assert verification.VERIFICATION_MANIFEST_RELATIVE_PATH == (
        "signal-family/verification-manifest-v1.json"
    )
    assert verification.TEST_MANIFEST_RELATIVE_PATH == "signal-family/test-manifest-v1.json"
    assert verification.VERIFIER_POLICY_PATH == (
        "/etc/rquant/signal-family-verifier-policy-v1.json"
    )
    assert verification.HARNESS_IDENTITY == (
        "/usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"
    )


def test_frozen_size_bounds_are_the_spec_values() -> None:
    assert verification.MAX_VECTOR_INPUT_BYTES == 65_536
    assert verification.MAX_CANONICAL_RESULT_BYTES == 65_536
    assert verification.MAX_IPC_RESPONSE_BYTES == 1_048_576


# --------------------------------------------------------------------------------------
# Exact frozen field sets and order
# --------------------------------------------------------------------------------------


def test_release_entry_and_policy_have_exactly_the_frozen_fields_in_order() -> None:
    assert tuple(verification.ReleaseVerificationEntryV1.model_fields) == (
        "successor_bundle_content_hash",
        "overlay_content_hash",
        "verification_manifest_sha256",
        "vector_set_hash",
        "expected_result_set_hash",
        "five_pair_service_binding_set_hash",
        "verifier_policy_max_age_seconds",
        "entry_hash",
    )
    assert tuple(verification.SignalFamilyVerifierPolicyV1.model_fields) == (
        "schema_version",
        "verifier_policy_id",
        "harness_identity",
        "harness_sha256",
        "release_entries",
        "content_hash",
    )


def test_service_binding_has_exactly_the_frozen_fields_in_order() -> None:
    assert tuple(verification.VerificationServiceBindingV1.model_fields) == (
        "service_id",
        "runtime_service_kind",
        "role_name",
        "service_manifest_fingerprint",
        "executable_module",
        "executable_source_relative_path",
        "executable_source_sha256",
        "surface_ids",
        "binding_hash",
    )


def test_child_result_schemas_have_exactly_the_frozen_fields_in_order() -> None:
    assert tuple(verification.SignalFamilyVectorResultV1.model_fields) == (
        "vector_id",
        "pair_id",
        "family_id",
        "surface_id",
        "canonical_result_json",
        "canonical_result_sha256",
    )
    assert tuple(verification.SignalFamilyChildResultV1.model_fields) == (
        "schema_version",
        "run_id",
        "test_manifest_hash",
        "vector_results",
        "result_hash",
    )


def test_vector_declaration_carries_no_expected_result_field() -> None:
    fields = tuple(verification.SignalFamilyVectorV1.model_fields)
    assert fields == ("vector_id", "pair_id", "family_id", "surface_id", "input_json")
    assert not [name for name in fields if "result" in name]
    assert tuple(verification.SignalFamilyExpectedResultV1.model_fields) == (
        "vector_id",
        "canonical_result_sha256",
    )


# --------------------------------------------------------------------------------------
# The four policy-bound set-hash preimages, transcribed from authority.md L1364-1395
# --------------------------------------------------------------------------------------


def test_vector_set_hash_matches_the_literal_spec_preimage() -> None:
    vectors = _vectors()
    expected = hashlib.sha256(
        canonical_json_bytes(
            {"vectors": [vector.model_dump(mode="json") for vector in vectors]}
        )
    ).hexdigest()
    assert verification.vector_set_hash(vectors) == expected


def test_expected_result_set_hash_matches_the_literal_spec_preimage() -> None:
    results = _expected_results()
    expected = hashlib.sha256(
        canonical_json_bytes(
            {
                "expected_results": [
                    {
                        "vector_id": result.vector_id,
                        "canonical_result_sha256": result.canonical_result_sha256,
                    }
                    for result in results
                ]
            }
        )
    ).hexdigest()
    assert verification.expected_result_set_hash(results) == expected


def test_five_pair_service_binding_set_hash_matches_the_literal_spec_preimage() -> None:
    pairs = _pairs()
    bindings = _bindings(pairs)
    expected = hashlib.sha256(
        canonical_json_bytes(
            {
                "pairs": [
                    {
                        "pair_id": pair.pair_id,
                        "producer_service_ids": list(pair.producer_service_ids),
                        "consumer_service_ids": list(pair.consumer_service_ids),
                    }
                    for pair in pairs
                ],
                "service_bindings": [
                    binding.model_dump(mode="json") for binding in bindings
                ],
            }
        )
    ).hexdigest()
    assert verification.five_pair_service_binding_set_hash(pairs, bindings) == expected


def test_service_bindings_hash_is_the_full_model_dump_of_the_sorted_tuple() -> None:
    bindings = _bindings()
    expected = hashlib.sha256(
        canonical_json_bytes([binding.model_dump(mode="json") for binding in bindings])
    ).hexdigest()
    assert verification.service_bindings_hash(bindings) == expected


def test_observed_result_set_hash_reuses_the_expected_result_preimage_shape() -> None:
    child = _child_result()
    expected = hashlib.sha256(
        canonical_json_bytes(
            {
                "expected_results": [
                    {
                        "vector_id": result.vector_id,
                        "canonical_result_sha256": result.canonical_result_sha256,
                    }
                    for result in sorted(
                        child.vector_results, key=lambda item: item.vector_id
                    )
                ]
            }
        )
    ).hexdigest()
    assert verification.observed_result_set_hash(child.vector_results) == expected


@pytest.mark.parametrize(
    ("preimage", "reason"),
    (
        ("vectors", "vectors must be sorted by vector_id and duplicate-free"),
        ("expected_results", "expected results must be sorted by vector_id"),
    ),
)
def test_set_hash_preimages_reject_unsorted_input(preimage: str, reason: str) -> None:
    if preimage == "vectors":
        vectors = _vectors()
        with pytest.raises(REJECTIONS, match=reason):
            verification.vector_set_hash(tuple(reversed(vectors)))
    else:
        results = _expected_results()
        with pytest.raises(REJECTIONS, match=reason):
            verification.expected_result_set_hash(tuple(reversed(results)))


def test_five_pair_set_hash_rejects_a_pair_tuple_that_is_not_the_exact_five() -> None:
    pairs = _pairs()
    bindings = _bindings(pairs)
    with pytest.raises(REJECTIONS, match="exactly the five frozen pair ids"):
        verification.five_pair_service_binding_set_hash(pairs[:4], bindings)


# --------------------------------------------------------------------------------------
# Authority epoch key and execution-evidence preimage
# --------------------------------------------------------------------------------------


def test_authority_epoch_key_matches_the_literal_spec_preimage() -> None:
    expected = hashlib.sha256(
        canonical_json_bytes(
            {
                "operation_id": OPERATION_ID,
                "sequence": 3,
                "generation_id": GENERATION_ID,
                "full_manifest_hash": GENERATION_ID,
                "profile_id": PROFILE_ID,
            }
        )
    ).hexdigest()
    assert (
        verification.authority_epoch_key(
            operation_id=OPERATION_ID,
            sequence=3,
            generation_id=GENERATION_ID,
            full_manifest_hash=GENERATION_ID,
            profile_id=PROFILE_ID,
        )
        == expected
    )
    assert _authority().authority_epoch_key == expected


def test_authority_epoch_key_changes_with_every_epoch_component() -> None:
    base = _authority().authority_epoch_key
    assert _authority(operation_id=OTHER_OPERATION_ID).authority_epoch_key != base
    assert _authority(sequence=4).authority_epoch_key != base
    assert _authority(generation_id="e" * 64).authority_epoch_key != base
    assert _authority(profile_id="f" * 64).authority_epoch_key != base


def test_execution_evidence_hash_is_not_an_input_to_itself() -> None:
    snapshot = _snapshot()
    preimage = verification.execution_evidence_preimage(
        authority=snapshot.authority,
        verification_manifest_sha256=snapshot.verification_manifest_sha256,
        test_manifest_hash=snapshot.test_manifest_hash,
        vector_set_hash=snapshot.vector_set_hash,
        expected_result_set_hash=snapshot.expected_result_set_hash,
        five_pair_service_binding_set_hash=snapshot.five_pair_service_binding_set_hash,
        child_result_hash=snapshot.child_result_hash,
        verifier_policy_id=snapshot.verifier_policy_id,
        verifier_policy_content_hash=snapshot.verifier_policy_content_hash,
        selected_entry_hash=snapshot.selected_entry_hash,
        harness_identity=snapshot.harness_identity,
        harness_sha256=snapshot.harness_sha256,
        observed_family_ids=snapshot.observed_family_ids,
        observed_surface_ids=snapshot.observed_surface_ids,
        service_manifest_fingerprints=snapshot.service_manifest_fingerprints,
    )
    serialized = canonical_json_bytes(preimage).decode("utf-8")
    assert snapshot.execution_evidence_hash not in serialized
    assert "execution_evidence_hash" not in preimage
    assert (
        hashlib.sha256(canonical_json_bytes(preimage)).hexdigest()
        == snapshot.execution_evidence_hash
    )


def test_execution_evidence_preimage_binds_every_required_input() -> None:
    snapshot = _snapshot()
    preimage = verification.execution_evidence_preimage(
        authority=snapshot.authority,
        verification_manifest_sha256=snapshot.verification_manifest_sha256,
        test_manifest_hash=snapshot.test_manifest_hash,
        vector_set_hash=snapshot.vector_set_hash,
        expected_result_set_hash=snapshot.expected_result_set_hash,
        five_pair_service_binding_set_hash=snapshot.five_pair_service_binding_set_hash,
        child_result_hash=snapshot.child_result_hash,
        verifier_policy_id=snapshot.verifier_policy_id,
        verifier_policy_content_hash=snapshot.verifier_policy_content_hash,
        selected_entry_hash=snapshot.selected_entry_hash,
        harness_identity=snapshot.harness_identity,
        harness_sha256=snapshot.harness_sha256,
        observed_family_ids=snapshot.observed_family_ids,
        observed_surface_ids=snapshot.observed_surface_ids,
        service_manifest_fingerprints=snapshot.service_manifest_fingerprints,
    )
    assert set(preimage) == {
        "authority_epoch_key",
        "authority_snapshot",
        "child_result_hash",
        "expected_result_set_hash",
        "five_pair_service_binding_set_hash",
        "full_manifest_hash",
        "harness_identity",
        "harness_sha256",
        "observed_family_ids",
        "observed_surface_ids",
        "selected_entry_hash",
        "service_manifest_fingerprints",
        "test_manifest_hash",
        "vector_set_hash",
        "verification_manifest_sha256",
        "verifier_policy_content_hash",
        "verifier_policy_id",
    }
    assert preimage["full_manifest_hash"] == snapshot.authority.full_manifest_hash


def test_observed_family_and_surface_sets_come_from_the_child_result() -> None:
    child = _child_result()
    assert verification.observed_family_ids(child.vector_results) == (
        successor.ACCEPTED_FAMILY_IDS[0],
    )
    observed = verification.observed_surface_ids(child.vector_results)
    expected = tuple(
        sorted(
            {
                surface
                for pair_id in verification.PAIR_IDS
                for surface in verification.READER_SURFACES[pair_id]
            },
            key=lambda surface: surface.value,
        )
    )
    assert observed == expected


# --------------------------------------------------------------------------------------
# Exact five-pair resolution and the pair-derived participating-service union
# --------------------------------------------------------------------------------------


def test_pair_map_resolves_exactly_the_five_frozen_rows_from_the_profile() -> None:
    pairs = _pairs()
    assert tuple(pair.pair_id for pair in pairs) == verification.PAIR_IDS
    by_id = {pair.pair_id: pair for pair in pairs}
    strategies = ("strategy.alpha.v1", "strategy.beta.v1")
    assert by_id["strategy-router"].producer_service_ids == strategies
    assert by_id["strategy-router"].consumer_service_ids == ("signal-router",)
    assert by_id["strategy-shadow"].producer_service_ids == strategies
    assert by_id["strategy-shadow"].consumer_service_ids == ("shadow-session",)
    assert by_id["router-notifier"].producer_service_ids == ("signal-router",)
    assert by_id["router-notifier"].consumer_service_ids == ("notifier",)
    assert by_id["router-paper"].producer_service_ids == ("signal-router",)
    assert by_id["router-paper"].consumer_service_ids == ("paper-broker",)
    assert by_id["notifier-serving"].producer_service_ids == ("notifier",)
    assert by_id["notifier-serving"].consumer_service_ids == ("serving-publisher",)


def test_participating_service_ids_is_the_sorted_unique_union_of_the_five_rows() -> None:
    pairs = _pairs()
    assert verification.participating_service_ids(pairs) == (
        "notifier",
        "paper-broker",
        "serving-publisher",
        "shadow-session",
        "signal-router",
        "strategy.alpha.v1",
        "strategy.beta.v1",
    )
    union = sorted(
        {
            service_id
            for pair in pairs
            for service_id in (*pair.producer_service_ids, *pair.consumer_service_ids)
        }
    )
    assert list(verification.participating_service_ids(pairs)) == union
    assert "feature-live" not in union
    assert "paper-consumer" not in union


def test_pair_resolution_rejects_an_empty_strategy_tuple() -> None:
    manifests = _profile_manifests(
        participants=tuple(
            row for row in _PARTICIPANTS if row[1] is not RuntimeServiceKind.STRATEGY_LIVE
        )
    )
    with pytest.raises(REJECTIONS, match="strategy_live services must be nonempty"):
        verification.resolve_pair_bindings(manifests)


@pytest.mark.parametrize(
    "kind",
    (
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.SHADOW_SESSION,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_BROKER,
        RuntimeServiceKind.SERVING_PUBLISHER,
    ),
    ids=lambda kind: kind.value,
)
def test_pair_resolution_rejects_a_missing_singleton_kind(kind: RuntimeServiceKind) -> None:
    manifests = _profile_manifests(
        participants=tuple(row for row in _PARTICIPANTS if row[1] is not kind)
    )
    with pytest.raises(REJECTIONS, match=f"exactly one {kind.value} service"):
        verification.resolve_pair_bindings(manifests)


@pytest.mark.parametrize(
    "kind",
    (
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.SHADOW_SESSION,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_BROKER,
        RuntimeServiceKind.SERVING_PUBLISHER,
    ),
    ids=lambda kind: kind.value,
)
def test_pair_resolution_rejects_a_duplicate_singleton_kind(kind: RuntimeServiceKind) -> None:
    manifests = (*_profile_manifests(), _manifest(f"second-{kind.value}", kind, 30.0))
    with pytest.raises(REJECTIONS, match=f"exactly one {kind.value} service"):
        verification.resolve_pair_bindings(manifests)


def test_pair_resolution_rejects_a_duplicate_service_id() -> None:
    manifests = (*_profile_manifests(), _manifest("notifier", RuntimeServiceKind.FEATURE_LIVE, 9.0))
    with pytest.raises(REJECTIONS, match="duplicate service id"):
        verification.resolve_pair_bindings(manifests)


@pytest.mark.parametrize(
    ("substitute", "reason"),
    (
        (("signal-router", "notifier"), "exact RuntimeServiceManifest"),
        ((RuntimeServiceKind.SIGNAL_ROUTER,), "exact RuntimeServiceManifest"),
        ((7,), "exact RuntimeServiceManifest"),
    ),
    ids=("handwritten-id-list", "service-kind", "service-count"),
)
def test_pair_resolution_rejects_substitutes_for_resolved_manifests(
    substitute: tuple[Any, ...],
    reason: str,
) -> None:
    with pytest.raises(REJECTIONS, match=reason):
        verification.resolve_pair_bindings(substitute)


def test_participating_union_rejects_a_handwritten_subset() -> None:
    pairs = _pairs()
    with pytest.raises(REJECTIONS, match="participating service ids are not pair-derived"):
        verification.require_pair_derived_participants(pairs, ("notifier", "signal-router"))
    assert (
        verification.require_pair_derived_participants(
            pairs,
            verification.participating_service_ids(pairs),
        )
        == verification.participating_service_ids(pairs)
    )


# --------------------------------------------------------------------------------------
# `VerificationServiceBindingV1` structure and the complete binding tuple
# --------------------------------------------------------------------------------------


def test_binding_hash_matches_the_literal_spec_preimage() -> None:
    binding = _bindings()[0]
    assert binding.binding_hash == hashlib.sha256(
        canonical_json_bytes(binding.model_dump(mode="json", exclude={"binding_hash"}))
    ).hexdigest()


def test_binding_surface_ids_are_a_nonempty_sorted_unique_enum_tuple() -> None:
    for binding in _bindings():
        assert binding.surface_ids
        values = tuple(surface.value for surface in binding.surface_ids)
        assert values == tuple(sorted(set(values)))
        assert all(isinstance(surface, verification.SurfaceId) for surface in binding.surface_ids)


def test_dynamic_strategy_services_may_share_a_role_and_surface_tuple_but_not_a_binding() -> None:
    bindings = {binding.service_id: binding for binding in _bindings()}
    alpha = bindings["strategy.alpha.v1"]
    beta = bindings["strategy.beta.v1"]
    assert alpha.role_name == beta.role_name
    assert alpha.surface_ids == beta.surface_ids
    assert alpha.binding_hash != beta.binding_hash
    assert alpha.service_manifest_fingerprint != beta.service_manifest_fingerprint


@pytest.mark.parametrize(
    "path",
    (
        "",
        "/src/rquant/strategy_runner.py",
        "src/../rquant/strategy_runner.py",
        "src/./rquant/strategy_runner.py",
        "src//rquant/strategy_runner.py",
        "src\\rquant\\strategy_runner.py",
        "src/rquant/strategy_runner.py/",
        "..",
    ),
    ids=(
        "empty",
        "absolute",
        "parent",
        "dot",
        "empty-component",
        "alternate-separator",
        "trailing-separator",
        "bare-parent",
    ),
)
def test_binding_rejects_an_escaping_executable_source_path(path: str) -> None:
    pairs = _pairs()
    with pytest.raises(REJECTIONS, match="executable source path"):
        verification.VerificationServiceBindingV1.create(
            service_id="signal-router",
            runtime_service_kind=RuntimeServiceKind.SIGNAL_ROUTER,
            role_name="router",
            service_manifest_fingerprint=_digest("fingerprint"),
            executable_module="rquant.signal_router_runtime",
            executable_source_relative_path=path,
            executable_source_sha256=_digest("source"),
            surface_ids=verification.expected_surface_ids(pairs)["signal-router"],
        )


def test_binding_tuple_covers_exactly_the_participating_service_ids() -> None:
    pairs = _pairs()
    bindings = _bindings(pairs)
    participating = verification.participating_service_ids(pairs)
    assert verification.validate_service_bindings(bindings, participating) == bindings
    assert tuple(binding.service_id for binding in bindings) == participating


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        ("drop", "service bindings must cover exactly the participating service ids"),
        ("extra", "service bindings must cover exactly the participating service ids"),
        ("unsorted", "service bindings must be sorted by service_id"),
        ("duplicate-binding-hash", "duplicate service binding hash"),
    ),
)
def test_binding_tuple_rejects_structural_drift(mutation: str, reason: str) -> None:
    pairs = _pairs()
    bindings = _bindings(pairs)
    participating = verification.participating_service_ids(pairs)
    if mutation == "drop":
        candidate = bindings[1:]
    elif mutation == "extra":
        extra = verification.VerificationServiceBindingV1.create(
            service_id="zz-extra",
            runtime_service_kind=RuntimeServiceKind.FEATURE_LIVE,
            role_name="strategy",
            service_manifest_fingerprint=_digest("extra"),
            executable_module="rquant.strategy_runner",
            executable_source_relative_path="src/rquant/strategy_runner.py",
            executable_source_sha256=_digest("extra-source"),
            surface_ids=(verification.SurfaceId.STRATEGY_RUNNER_PROCESS_BATCH,),
        )
        candidate = (*bindings, extra)
    elif mutation == "unsorted":
        candidate = tuple(reversed(bindings))
    else:
        candidate = (*bindings[:-1], bindings[-2])
    with pytest.raises(REJECTIONS, match=reason):
        verification.validate_service_bindings(candidate, participating)


def test_expected_surface_ids_reject_an_omitted_or_additional_surface() -> None:
    pairs = _pairs()
    expected = verification.expected_surface_ids(pairs)
    router = expected["signal-router"]
    assert len(router) == 4
    with pytest.raises(REJECTIONS, match="surface ids are not the exact pair-derived set"):
        verification.require_pair_derived_surfaces(pairs, "signal-router", router[:-1])
    with pytest.raises(REJECTIONS, match="surface ids are not the exact pair-derived set"):
        verification.require_pair_derived_surfaces(
            pairs,
            "signal-router",
            tuple(
                sorted(
                    (*router, verification.SurfaceId.BUILD_SERVING_READ_MODELS),
                    key=lambda surface: surface.value,
                )
            ),
        )
    assert verification.require_pair_derived_surfaces(pairs, "signal-router", router) == router


def test_every_frozen_surface_is_assigned_to_exactly_one_participating_service() -> None:
    pairs = _pairs()
    expected = verification.expected_surface_ids(pairs)
    seen: list[verification.SurfaceId] = []
    for surfaces in expected.values():
        seen.extend(surfaces)
    assert sorted({surface.value for surface in seen}) == sorted(
        member.value for member in verification.SurfaceId
    )


# --------------------------------------------------------------------------------------
# Release entry, policy, and their strict rejection matrix
# --------------------------------------------------------------------------------------


def test_entry_and_policy_hashes_match_their_literal_preimages() -> None:
    entry = _entry()
    assert entry.entry_hash == hashlib.sha256(
        canonical_json_bytes(entry.model_dump(mode="json", exclude={"entry_hash"}))
    ).hexdigest()
    policy = _policy((entry,))
    assert policy.content_hash == hashlib.sha256(
        canonical_json_bytes(policy.model_dump(mode="json", exclude={"content_hash"}))
    ).hexdigest()
    assert policy.verifier_policy_id == "signal-family-verifier-policy-v1"
    assert policy.harness_identity == verification.HARNESS_IDENTITY
    assert policy.schema_version == 1


def test_policy_raw_bytes_round_trip_without_a_newline() -> None:
    policy = _policy()
    raw = verification.verifier_policy_canonical_json_bytes(policy)
    assert raw == canonical_json_bytes(policy.model_dump(mode="json"))
    assert not raw.endswith(b"\n")
    assert verification.SignalFamilyVerifierPolicyV1.from_canonical_json(raw) == policy


@pytest.mark.parametrize(
    ("mutator", "reason"),
    (
        (lambda raw: raw + b"\n", "persistent JSON is not canonical"),
        (lambda raw: raw.replace(b":", b": ", 1), "persistent JSON is not canonical"),
        (
            lambda raw: b'{"schema_version":1,' + raw[1:],
            "duplicate JSON key",
        ),
        (lambda raw: raw[:-1] + b',"extra":1}', "Extra inputs are not permitted"),
    ),
    ids=("newline", "whitespace", "duplicate-key", "extra-key"),
)
def test_policy_decoder_rejects_noncanonical_bytes(mutator: Any, reason: str) -> None:
    raw = verification.verifier_policy_canonical_json_bytes(_policy())
    with pytest.raises(REJECTIONS, match=re.escape(reason)):
        verification.SignalFamilyVerifierPolicyV1.from_canonical_json(mutator(raw))


def test_policy_rejects_an_empty_or_unsorted_entry_tuple() -> None:
    entry = _entry()
    other = verification.ReleaseVerificationEntryV1.create(
        successor_bundle_content_hash=_digest("zzz-bundle"),
        overlay_content_hash=_digest("zzz-overlay"),
        verification_manifest_sha256=_digest("zzz-manifest"),
        vector_set_hash=_digest("zzz-vectors"),
        expected_result_set_hash=_digest("zzz-results"),
        five_pair_service_binding_set_hash=_digest("zzz-pairs"),
        verifier_policy_max_age_seconds=None,
    )
    ordered = tuple(
        sorted(
            (entry, other),
            key=lambda item: (item.successor_bundle_content_hash, item.overlay_content_hash),
        )
    )
    assert verification.SignalFamilyVerifierPolicyV1.create(
        harness_sha256=_digest("harness"),
        release_entries=ordered,
    ).release_entries == ordered
    with pytest.raises(REJECTIONS, match="release entries must be nonempty"):
        verification.SignalFamilyVerifierPolicyV1.create(
            harness_sha256=_digest("harness"),
            release_entries=(),
        )
    with pytest.raises(REJECTIONS, match="release entries must be sorted"):
        verification.SignalFamilyVerifierPolicyV1(
            schema_version=1,
            verifier_policy_id="signal-family-verifier-policy-v1",
            harness_identity=verification.HARNESS_IDENTITY,
            harness_sha256=_digest("harness"),
            release_entries=tuple(reversed(ordered)),
            content_hash=verification.canonical_sha256(
                {
                    "schema_version": 1,
                    "verifier_policy_id": "signal-family-verifier-policy-v1",
                    "harness_identity": verification.HARNESS_IDENTITY,
                    "harness_sha256": _digest("harness"),
                    "release_entries": [
                        item.model_dump(mode="json") for item in reversed(ordered)
                    ],
                }
            ),
        )


def test_policy_rejects_a_duplicate_release_key() -> None:
    entry = _entry()
    with pytest.raises(REJECTIONS, match="duplicate release key"):
        verification.SignalFamilyVerifierPolicyV1.create(
            harness_sha256=_digest("harness"),
            release_entries=(entry, entry),
        )


def test_policy_selects_exactly_one_entry_for_a_release_key() -> None:
    entry = _entry()
    policy = _policy((entry,))
    assert (
        policy.select_entry(
            successor_bundle_content_hash=entry.successor_bundle_content_hash,
            overlay_content_hash=entry.overlay_content_hash,
        )
        is entry
    )
    with pytest.raises(REJECTIONS, match="no release entry matches"):
        policy.select_entry(
            successor_bundle_content_hash=_digest("absent"),
            overlay_content_hash=entry.overlay_content_hash,
        )


@pytest.mark.parametrize(
    ("value", "reason"),
    (
        (0, "verifier_policy_max_age_seconds must be a positive integer"),
        (-1, "verifier_policy_max_age_seconds must be a positive integer"),
        (True, "Input should be a valid integer"),
        (30.0, "Input should be a valid integer"),
        ("30", "Input should be a valid integer"),
    ),
    ids=("zero", "negative", "bool", "float", "string"),
)
def test_entry_rejects_a_nonpositive_or_coerced_age_cap(value: Any, reason: str) -> None:
    with pytest.raises(REJECTIONS, match=reason):
        verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=_digest("bundle"),
            overlay_content_hash=_digest("overlay"),
            verification_manifest_sha256=_digest("manifest"),
            vector_set_hash=_digest("vectors"),
            expected_result_set_hash=_digest("results"),
            five_pair_service_binding_set_hash=_digest("pairs"),
            verifier_policy_max_age_seconds=value,
        )


@pytest.mark.parametrize(
    "bad_hash",
    ("A" * 64, "0" * 63, "0" * 65, "g" * 64),
    ids=("uppercase", "short", "long", "nonhex"),
)
def test_entry_rejects_a_malformed_hash(bad_hash: str) -> None:
    with pytest.raises(REJECTIONS, match="String should match pattern"):
        verification.ReleaseVerificationEntryV1.create(
            successor_bundle_content_hash=bad_hash,
            overlay_content_hash=_digest("overlay"),
            verification_manifest_sha256=_digest("manifest"),
            vector_set_hash=_digest("vectors"),
            expected_result_set_hash=_digest("results"),
            five_pair_service_binding_set_hash=_digest("pairs"),
            verifier_policy_max_age_seconds=None,
        )


def test_entry_rejects_a_tampered_entry_hash() -> None:
    entry = _entry()
    payload = entry.model_dump(mode="json")
    payload["entry_hash"] = _digest("forged")
    with pytest.raises(REJECTIONS, match="entry hash does not match its canonical content"):
        verification.ReleaseVerificationEntryV1.model_validate(payload)


# --------------------------------------------------------------------------------------
# Vectors, expected results, and the immutable test manifest
# --------------------------------------------------------------------------------------


def test_vector_id_is_the_canonical_hash_of_its_own_declaration() -> None:
    vector = _vectors()[0]
    assert vector.vector_id == hashlib.sha256(
        canonical_json_bytes(vector.model_dump(mode="json", exclude={"vector_id"}))
    ).hexdigest()


def test_vector_rejects_noncanonical_oversized_or_empty_input_json() -> None:
    pair_id = "router-paper"
    surface = verification.READER_SURFACES[pair_id][0]
    family_id = successor.ACCEPTED_FAMILY_IDS[0]
    with pytest.raises(REJECTIONS, match="persistent JSON is not canonical"):
        verification.SignalFamilyVectorV1.create(
            pair_id=pair_id,
            family_id=family_id,
            surface_id=surface,
            input_json='{"a": 1}',
        )
    with pytest.raises(REJECTIONS, match="input_json must be nonempty"):
        verification.SignalFamilyVectorV1.create(
            pair_id=pair_id,
            family_id=family_id,
            surface_id=surface,
            input_json="",
        )
    oversized = canonical_json_bytes({"a": "b" * 70_000}).decode("utf-8")
    with pytest.raises(REJECTIONS, match="input_json exceeds 65536 bytes"):
        verification.SignalFamilyVectorV1.create(
            pair_id=pair_id,
            family_id=family_id,
            surface_id=surface,
            input_json=oversized,
        )


def test_vector_rejects_a_surface_outside_its_own_pair() -> None:
    with pytest.raises(REJECTIONS, match="surface is not bound to that pair"):
        verification.SignalFamilyVectorV1.create(
            pair_id="router-paper",
            family_id=successor.ACCEPTED_FAMILY_IDS[0],
            surface_id=verification.SurfaceId.BUILD_SERVING_READ_MODELS,
            input_json=canonical_json_bytes({"a": 1}).decode("utf-8"),
        )


def test_vector_rejects_a_family_outside_the_current_family() -> None:
    with pytest.raises(REJECTIONS, match="family id is not a current-family id"):
        verification.SignalFamilyVectorV1.create(
            pair_id="router-paper",
            family_id="signal-envelope/v2",
            surface_id=verification.READER_SURFACES["router-paper"][0],
            input_json=canonical_json_bytes({"a": 1}).decode("utf-8"),
        )


def test_test_manifest_binds_vectors_expected_results_pairs_and_bindings() -> None:
    manifest = _test_manifest()
    assert tuple(manifest.model_fields) == (
        "schema_version",
        "vectors",
        "expected_results",
        "pairs",
        "service_bindings",
        "service_bindings_hash",
        "content_hash",
    )
    assert manifest.schema_version == 1
    assert len(manifest.vectors) == 13
    assert len(manifest.expected_results) == 13
    assert manifest.service_bindings_hash == verification.service_bindings_hash(
        manifest.service_bindings
    )
    assert manifest.content_hash == hashlib.sha256(
        canonical_json_bytes(manifest.model_dump(mode="json", exclude={"content_hash"}))
    ).hexdigest()


def test_test_manifest_rejects_expected_results_that_do_not_pair_with_vectors() -> None:
    vectors = _vectors()
    results = _expected_results(vectors)
    with pytest.raises(REJECTIONS, match="expected results must pair one to one with vectors"):
        verification.SignalFamilyTestManifestV1.create(
            vectors=vectors,
            expected_results=results[:-1],
            pairs=_pairs(),
            service_bindings=_bindings(),
        )


def test_verification_manifest_binds_the_policy_bound_hashes() -> None:
    manifest = _test_manifest()
    verification_manifest = verification.SignalFamilyVerificationManifestV1.create(
        successor_bundle_content_hash=_digest("successor-bundle"),
        overlay_content_hash=_digest("overlay-bundle"),
        test_manifest_sha256=hashlib.sha256(
            verification.test_manifest_canonical_json_bytes(manifest)
        ).hexdigest(),
        test_manifest=manifest,
    )
    assert tuple(verification_manifest.model_fields) == (
        "schema_version",
        "successor_bundle_content_hash",
        "overlay_content_hash",
        "test_manifest_sha256",
        "vector_set_hash",
        "expected_result_set_hash",
        "five_pair_service_binding_set_hash",
        "content_hash",
    )
    assert verification_manifest.vector_set_hash == verification.vector_set_hash(manifest.vectors)
    assert verification_manifest.expected_result_set_hash == (
        verification.expected_result_set_hash(manifest.expected_results)
    )
    assert verification_manifest.five_pair_service_binding_set_hash == (
        verification.five_pair_service_binding_set_hash(
            manifest.pairs,
            manifest.service_bindings,
        )
    )


# --------------------------------------------------------------------------------------
# Bounded child result
# --------------------------------------------------------------------------------------


def test_child_result_hash_and_sort_order_are_exact() -> None:
    child = _child_result()
    assert child.result_hash == hashlib.sha256(
        canonical_json_bytes(child.model_dump(mode="json", exclude={"result_hash"}))
    ).hexdigest()
    keys = [
        (result.pair_id, result.family_id, result.surface_id.value, result.vector_id)
        for result in child.vector_results
    ]
    assert keys == sorted(keys)


def test_child_result_rejects_unsorted_or_duplicate_vector_results() -> None:
    child = _child_result()
    with pytest.raises(REJECTIONS, match="vector results must be sorted"):
        verification.SignalFamilyChildResultV1(
            schema_version=1,
            run_id=child.run_id,
            test_manifest_hash=child.test_manifest_hash,
            vector_results=tuple(reversed(child.vector_results)),
            result_hash=verification.canonical_sha256(
                {
                    "schema_version": 1,
                    "run_id": child.run_id,
                    "test_manifest_hash": child.test_manifest_hash,
                    "vector_results": [
                        result.model_dump(mode="json")
                        for result in reversed(child.vector_results)
                    ],
                }
            ),
        )
    duplicated = (*child.vector_results, child.vector_results[-1])
    with pytest.raises(REJECTIONS, match="vector results must be sorted"):
        verification.SignalFamilyChildResultV1(
            schema_version=1,
            run_id=child.run_id,
            test_manifest_hash=child.test_manifest_hash,
            vector_results=duplicated,
            result_hash=verification.canonical_sha256(
                {
                    "schema_version": 1,
                    "run_id": child.run_id,
                    "test_manifest_hash": child.test_manifest_hash,
                    "vector_results": [
                        result.model_dump(mode="json") for result in duplicated
                    ],
                }
            ),
        )


def test_child_result_ipc_bytes_are_bounded_canonical_and_trailing_free() -> None:
    child = _child_result()
    raw = verification.child_result_canonical_json_bytes(child)
    assert len(raw) <= verification.MAX_IPC_RESPONSE_BYTES
    assert not raw.endswith(b"\n")
    assert (
        verification.SignalFamilyChildResultV1.from_canonical_ipc_bytes(
            raw,
            max_vector_count=len(child.vector_results),
        )
        == child
    )
    with pytest.raises(REJECTIONS, match="persistent JSON is not canonical"):
        verification.SignalFamilyChildResultV1.from_canonical_ipc_bytes(
            raw + b"\n",
            max_vector_count=len(child.vector_results),
        )
    with pytest.raises(REJECTIONS, match="vector results exceed the test manifest vector count"):
        verification.SignalFamilyChildResultV1.from_canonical_ipc_bytes(
            raw,
            max_vector_count=len(child.vector_results) - 1,
        )
    with pytest.raises(REJECTIONS, match="IPC response exceeds 1048576 bytes"):
        verification.SignalFamilyChildResultV1.from_canonical_ipc_bytes(
            b" " * (verification.MAX_IPC_RESPONSE_BYTES + 1),
            max_vector_count=len(child.vector_results),
        )


def test_vector_result_rejects_an_oversized_or_mismatched_canonical_result() -> None:
    vector = _vectors()[0]
    with pytest.raises(REJECTIONS, match="canonical_result_json exceeds 65536 bytes"):
        verification.SignalFamilyVectorResultV1.create(
            vector_id=vector.vector_id,
            pair_id=vector.pair_id,
            family_id=vector.family_id,
            surface_id=vector.surface_id,
            canonical_result_json=canonical_json_bytes({"a": "b" * 70_000}).decode("utf-8"),
        )
    result = verification.SignalFamilyVectorResultV1.create(
        vector_id=vector.vector_id,
        pair_id=vector.pair_id,
        family_id=vector.family_id,
        surface_id=vector.surface_id,
        canonical_result_json=canonical_json_bytes({"a": 1}).decode("utf-8"),
    )
    payload = result.model_dump(mode="json")
    payload["canonical_result_sha256"] = _digest("forged")
    with pytest.raises(REJECTIONS, match="canonical_result_sha256 does not hash its own bytes"):
        verification.SignalFamilyVectorResultV1.model_validate(payload)


# --------------------------------------------------------------------------------------
# `RESET-REG-P2-01`: profile-derived freshness
# --------------------------------------------------------------------------------------


def test_freshness_is_the_minimum_stale_bound_of_the_participating_manifests() -> None:
    manifests = _profile_manifests()
    pairs = verification.resolve_pair_bindings(manifests)
    participating = verification.participating_service_ids(pairs)
    resolved = verification.resolve_participating_service_manifests(manifests, participating)
    assert tuple(manifest.service_id for manifest in resolved) == participating
    assert verification.service_freshness_seconds(resolved) == 45.0
    assert verification.freshness_seconds(45.0, None) == 45.0
    assert isinstance(verification.freshness_seconds(45.0, None), float)


@pytest.mark.parametrize(
    ("service_id", "stale", "expected"),
    (
        ("shadow-session", 12.5, 12.5),
        ("serving-publisher", 9.0, 9.0),
        ("strategy.beta.v1", 8.0, 8.0),
        ("signal-router", 7.5, 7.5),
        ("notifier", 6.0, 6.0),
        ("paper-broker", 5.0, 5.0),
    ),
)
def test_the_lowest_participant_bound_controls_freshness(
    service_id: str,
    stale: float,
    expected: float,
) -> None:
    manifests = _profile_manifests({service_id: stale})
    pairs = verification.resolve_pair_bindings(manifests)
    resolved = verification.resolve_participating_service_manifests(
        manifests,
        verification.participating_service_ids(pairs),
    )
    assert verification.service_freshness_seconds(resolved) == expected


def test_a_nonparticipating_service_never_controls_freshness() -> None:
    manifests = _profile_manifests({"feature-live": 0.5, "paper-consumer": 0.25})
    pairs = verification.resolve_pair_bindings(manifests)
    resolved = verification.resolve_participating_service_manifests(
        manifests,
        verification.participating_service_ids(pairs),
    )
    assert verification.service_freshness_seconds(resolved) == 45.0


def test_the_optional_policy_cap_lowers_but_never_raises_freshness() -> None:
    assert verification.freshness_seconds(45.0, 30) == 30.0
    assert verification.freshness_seconds(45.0, 600) == 45.0
    assert verification.freshness_seconds(45.0, None) == 45.0


def test_there_is_no_fixed_thirty_second_freshness_assumption() -> None:
    manifests = _profile_manifests({service_id: 300.0 for service_id, _, _ in _PARTICIPANTS})
    pairs = verification.resolve_pair_bindings(manifests)
    resolved = verification.resolve_participating_service_manifests(
        manifests,
        verification.participating_service_ids(pairs),
    )
    assert verification.service_freshness_seconds(resolved) == 300.0
    assert verification.freshness_seconds(300.0, None) == 300.0


def test_participating_manifest_resolution_rejects_missing_duplicate_or_extra() -> None:
    manifests = _profile_manifests()
    pairs = verification.resolve_pair_bindings(manifests)
    participating = verification.participating_service_ids(pairs)
    with pytest.raises(REJECTIONS, match="no manifest resolves service id"):
        verification.resolve_participating_service_manifests(
            tuple(m for m in manifests if m.service_id != "notifier"),
            participating,
        )
    duplicated = (*manifests, _manifest("notifier", RuntimeServiceKind.NOTIFIER, 11.0))
    with pytest.raises(REJECTIONS, match="duplicate manifest resolves service id"):
        verification.resolve_participating_service_manifests(duplicated, participating)
    with pytest.raises(REJECTIONS, match="participating service ids must be sorted"):
        verification.resolve_participating_service_manifests(
            manifests,
            tuple(reversed(participating)),
        )


def test_fresh_until_adds_the_float_freshness_to_verified_at() -> None:
    assert verification.fresh_until(VERIFIED_AT, 12.5) == VERIFIED_AT + timedelta(seconds=12.5)
    with pytest.raises(REJECTIONS, match="verified_at must be timezone-aware UTC"):
        verification.fresh_until(VERIFIED_AT.replace(tzinfo=None), 12.5)
    with pytest.raises(REJECTIONS, match="freshness_seconds must be positive"):
        verification.fresh_until(VERIFIED_AT, 0.0)


def test_canonical_timestamps_are_iso_8601_microsecond_utc_strings() -> None:
    assert verification.canonical_timestamp(VERIFIED_AT) == "2026-08-24T07:30:15.250000Z"
    whole = datetime(2026, 8, 24, 7, 30, 15, tzinfo=UTC)
    assert verification.canonical_timestamp(whole) == "2026-08-24T07:30:15.000000Z"
    assert verification.parse_canonical_timestamp("2026-08-24T07:30:15.250000Z") == VERIFIED_AT
    with pytest.raises(REJECTIONS, match="timestamp must be timezone-aware UTC"):
        verification.canonical_timestamp(VERIFIED_AT.replace(tzinfo=None))


def test_receipt_preimage_carries_timestamp_strings_and_no_float_seconds() -> None:
    receipt = verification.build_pair_receipts(_snapshot())[0]
    payload = receipt.model_dump(mode="json")
    assert payload["verified_at"] == "2026-08-24T07:30:15.250000Z"
    assert payload["fresh_until"] == "2026-08-24T07:31:00.250000Z"
    assert not [value for value in payload.values() if isinstance(value, float)]


def test_a_policy_cap_lowers_fresh_until_on_every_receipt() -> None:
    receipts = verification.build_pair_receipts(_snapshot(max_age=10))
    assert {receipt.fresh_until for receipt in receipts} == {"2026-08-24T07:30:25.250000Z"}


# --------------------------------------------------------------------------------------
# Atomic five-receipt emission, uniqueness key, replay, and conflict
# --------------------------------------------------------------------------------------


def test_one_snapshot_emits_exactly_five_receipts_one_per_frozen_pair() -> None:
    receipts = verification.build_pair_receipts(_snapshot())
    assert len(receipts) == 5
    assert tuple(receipt.pair_id for receipt in receipts) == verification.PAIR_IDS
    assert len({receipt.receipt_fingerprint for receipt in receipts}) == 5


def test_receipt_unique_key_is_overlay_epoch_and_pair() -> None:
    snapshot = _snapshot()
    receipt = verification.build_pair_receipts(snapshot)[0]
    assert verification.receipt_unique_key(receipt) == (
        snapshot.overlay_content_hash,
        snapshot.authority.authority_epoch_key,
        receipt.pair_id,
    )


def test_receipt_fingerprint_matches_its_literal_preimage_and_binds_the_pair() -> None:
    receipt = verification.build_pair_receipts(_snapshot())[0]
    assert receipt.receipt_fingerprint == hashlib.sha256(
        canonical_json_bytes(receipt.model_dump(mode="json", exclude={"receipt_fingerprint"}))
    ).hexdigest()
    assert receipt.producer_surface_ids == tuple(
        surface.value for surface in verification.PRODUCER_SURFACES[receipt.pair_id]
    )
    assert receipt.reader_surface_ids == tuple(
        surface.value for surface in verification.READER_SURFACES[receipt.pair_id]
    )


def test_an_identical_replay_is_idempotent_and_byte_identical() -> None:
    first = verification.build_pair_receipts(_snapshot())
    second = verification.build_pair_receipts(_snapshot())
    assert [receipt.model_dump(mode="json") for receipt in first] == [
        receipt.model_dump(mode="json") for receipt in second
    ]
    for existing, candidate in zip(first, second, strict=True):
        assert verification.resolve_receipt_replay(existing, candidate) is existing


def test_divergent_bytes_for_one_receipt_key_reject_with_conflict_evidence() -> None:
    existing = verification.build_pair_receipts(_snapshot())[0]
    candidate = verification.build_pair_receipts(
        _snapshot(stale_overrides={"shadow-session": 30.0})
    )[0]
    assert verification.receipt_unique_key(existing) == verification.receipt_unique_key(candidate)
    assert existing.receipt_fingerprint != candidate.receipt_fingerprint
    with pytest.raises(
        verification.SignalFamilyVerificationConflictError,
        match="receipt bytes diverge for one unique key",
    ) as excinfo:
        verification.resolve_receipt_replay(existing, candidate)
    record = excinfo.value.audit_record
    assert record.reason_code is verification.SignalFamilyReasonCode.RECEIPT_CONFLICT
    assert record.existing_hash == existing.receipt_fingerprint
    assert record.attempted_hash == candidate.receipt_fingerprint


def test_receipt_replay_rejects_a_different_unique_key() -> None:
    receipts = verification.build_pair_receipts(_snapshot())
    with pytest.raises(REJECTIONS, match="receipt replay requires one unique key"):
        verification.resolve_receipt_replay(receipts[0], receipts[1])


# --------------------------------------------------------------------------------------
# `READY` decision: exact set equality, concurrency, and epoch binding
# --------------------------------------------------------------------------------------


def test_ready_requires_exact_five_pair_set_equality_not_a_count() -> None:
    snapshot = _snapshot()
    receipts = verification.build_pair_receipts(snapshot)
    decision = verification.build_readiness_decision(snapshot, receipts)
    assert decision.pair_ids == verification.PAIR_IDS
    duplicated = (receipts[0], receipts[0], *receipts[2:])
    assert len(duplicated) == 5
    with pytest.raises(REJECTIONS, match="receipts must cover exactly the five frozen pairs"):
        verification.build_readiness_decision(snapshot, duplicated)
    with pytest.raises(REJECTIONS, match="receipts must cover exactly the five frozen pairs"):
        verification.build_readiness_decision(snapshot, receipts[:4])


def test_decision_binds_the_overlay_declarations_channels_and_receipt_aggregate() -> None:
    snapshot = _snapshot()
    receipts = verification.build_pair_receipts(snapshot)
    decision = verification.build_readiness_decision(snapshot, receipts)
    assert decision.successor_declaration_hashes == snapshot.successor_declaration_hashes
    assert decision.successor_channel_hashes == snapshot.successor_channel_hashes
    assert decision.receipt_fingerprints == tuple(
        sorted(receipt.receipt_fingerprint for receipt in receipts)
    )
    assert decision.receipt_fingerprint_set_hash == hashlib.sha256(
        canonical_json_bytes({"receipt_fingerprints": list(decision.receipt_fingerprints)})
    ).hexdigest()
    assert decision.decision_hash == hashlib.sha256(
        canonical_json_bytes(decision.model_dump(mode="json", exclude={"decision_hash"}))
    ).hexdigest()
    assert decision.authority_epoch_key == snapshot.authority.authority_epoch_key


def test_concurrent_identical_finalization_returns_the_same_bytes() -> None:
    left = verification.build_readiness_decision(
        _snapshot(),
        verification.build_pair_receipts(_snapshot()),
    )
    right = verification.build_readiness_decision(
        _snapshot(),
        verification.build_pair_receipts(_snapshot()),
    )
    assert verification.decision_unique_key(left) == verification.decision_unique_key(right)
    assert canonical_json_bytes(left.model_dump(mode="json")) == canonical_json_bytes(
        right.model_dump(mode="json")
    )
    assert verification.resolve_decision_replay(left, right) is left


def test_a_divergent_concurrent_decision_rejects_with_conflict_evidence() -> None:
    left_snapshot = _snapshot()
    right_snapshot = _snapshot(stale_overrides={"shadow-session": 30.0})
    left = verification.build_readiness_decision(
        left_snapshot,
        verification.build_pair_receipts(left_snapshot),
    )
    right = verification.build_readiness_decision(
        right_snapshot,
        verification.build_pair_receipts(right_snapshot),
    )
    assert verification.decision_unique_key(left) == verification.decision_unique_key(right)
    with pytest.raises(
        verification.SignalFamilyVerificationConflictError,
        match="decision bytes diverge for one unique key",
    ) as excinfo:
        verification.resolve_decision_replay(left, right)
    assert excinfo.value.audit_record.reason_code is (
        verification.SignalFamilyReasonCode.DECISION_CONFLICT
    )


def test_a_receipt_from_another_snapshot_cannot_join_a_decision() -> None:
    snapshot = _snapshot()
    other = _snapshot(authority=_authority(sequence=4))
    receipts = verification.build_pair_receipts(snapshot)
    intruder = verification.build_pair_receipts(other)[0]
    mixed = (intruder, *receipts[1:])
    with pytest.raises(REJECTIONS, match="receipt does not bind the decision snapshot"):
        verification.build_readiness_decision(snapshot, mixed)


def test_authority_advance_produces_a_new_epoch_and_never_revives_old_readiness() -> None:
    first = _snapshot()
    advanced = _snapshot(authority=_authority(sequence=4))
    assert first.authority.generation_id == advanced.authority.generation_id
    assert first.authority.authority_epoch_key != advanced.authority.authority_epoch_key
    first_decision = verification.build_readiness_decision(
        first,
        verification.build_pair_receipts(first),
    )
    advanced_decision = verification.build_readiness_decision(
        advanced,
        verification.build_pair_receipts(advanced),
    )
    assert verification.decision_unique_key(first_decision) != verification.decision_unique_key(
        advanced_decision
    )


def test_returning_to_the_same_generation_under_a_new_operation_is_a_new_epoch() -> None:
    original = _snapshot()
    returned = _snapshot(authority=_authority(operation_id=OTHER_OPERATION_ID, sequence=1))
    assert original.authority.generation_id == returned.authority.generation_id
    assert original.authority.full_manifest_hash == returned.authority.full_manifest_hash
    assert original.authority.authority_epoch_key != returned.authority.authority_epoch_key
    assert verification.receipt_unique_key(
        verification.build_pair_receipts(original)[0]
    ) != verification.receipt_unique_key(verification.build_pair_receipts(returned)[0])


# --------------------------------------------------------------------------------------
# Minimal lifecycle
# --------------------------------------------------------------------------------------


def test_the_lifecycle_has_no_attesting_or_activated_state() -> None:
    assert {state.value for state in verification.SignalFamilyReadinessState} == {
        "DECLARED",
        "READY",
        "REVOKED",
        "ROLLED_BACK",
    }
    assert not [
        name
        for name in dir(verification)
        if "ATTESTING" in name.upper() or "ACTIVATED" in name.upper()
    ]


def test_the_four_allowed_transitions_are_exactly_the_spec_edges() -> None:
    state = verification.SignalFamilyReadinessState
    assert verification.ALLOWED_READINESS_TRANSITIONS == frozenset(
        {
            (state.DECLARED, state.READY),
            (state.DECLARED, state.REVOKED),
            (state.READY, state.REVOKED),
            (state.READY, state.ROLLED_BACK),
        }
    )
    for current, target in verification.ALLOWED_READINESS_TRANSITIONS:
        assert verification.require_readiness_transition(current, target) is target


@pytest.mark.parametrize(
    ("current", "target"),
    (
        ("DECLARED", "ROLLED_BACK"),
        ("DECLARED", "DECLARED"),
        ("READY", "DECLARED"),
        ("READY", "READY"),
        ("REVOKED", "READY"),
        ("REVOKED", "ROLLED_BACK"),
        ("ROLLED_BACK", "READY"),
        ("ROLLED_BACK", "REVOKED"),
    ),
)
def test_every_other_transition_rejects(current: str, target: str) -> None:
    state = verification.SignalFamilyReadinessState
    with pytest.raises(REJECTIONS, match="readiness transition is not allowed"):
        verification.require_readiness_transition(state(current), state(target))


def test_revocation_and_rollback_only_disable_future_eligibility() -> None:
    state = verification.SignalFamilyReadinessState
    assert verification.is_activation_eligible(state.READY) is True
    assert verification.is_activation_eligible(state.DECLARED) is False
    assert verification.is_activation_eligible(state.REVOKED) is False
    assert verification.is_activation_eligible(state.ROLLED_BACK) is False


def test_expiry_disables_eligibility_without_deleting_the_decision() -> None:
    snapshot = _snapshot()
    decision = verification.build_readiness_decision(
        snapshot,
        verification.build_pair_receipts(snapshot),
    )
    fresh_until = verification.parse_canonical_timestamp(decision.fresh_until)
    assert verification.is_decision_fresh(decision, fresh_until - timedelta(microseconds=1))
    assert not verification.is_decision_fresh(decision, fresh_until)
    assert not verification.is_decision_fresh(decision, fresh_until + timedelta(seconds=1))
    assert decision.decision_hash


# --------------------------------------------------------------------------------------
# No caller-created persistence surface exists in this module
# --------------------------------------------------------------------------------------


def test_the_model_layer_exposes_no_append_persist_or_store_entry_point() -> None:
    banned = ("append", "persist", "store", "write", "commit_receipt")
    exported = [name for name in dir(verification) if not name.startswith("_")]
    assert not [
        name for name in exported if any(token in name.lower() for token in banned)
    ]
