from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

import rquant.runtime_schema_registry as runtime_schema_registry
from rquant.auction_universe_authority import AuctionUniverseAuthority
from rquant.live_contracts import BatchEnvelope
from rquant.reference_slow_publisher import ReferenceSlowSourceSnapshot
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_schema_registry import (
    RuntimePhysicalTableSchema,
    RuntimeSchemaChannelContract,
    RuntimeSchemaCompatibilityError,
    RuntimeSchemaConsumerBinding,
    RuntimeSchemaContractBundle,
    RuntimeSchemaProducerBinding,
    RuntimeSchemaV1LifecycleReview,
    build_runtime_schema_contract_bundle,
    build_runtime_schema_v1_migration_audit,
    parse_runtime_schema_contract_bundle,
    validate_runtime_schema_transition,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.schema_compatibility import (
    ConsumerFieldCapability,
    ConsumerSchemaRequirement,
    ProducerSchemaCapability,
    SchemaDeclaration,
    SchemaField,
    UnknownFieldPolicy,
)
from rquant.serving_read_models import serving_physical_table_specs_fingerprint

OLD_COMMIT = "a" * 40
NEW_COMMIT = "b" * 40


def _manifest(
    *,
    service_id: str,
    kind: RuntimeServiceKind,
    commit: str,
) -> RuntimeServiceManifest:
    plane = (
        RuntimeServicePlane.SERVING
        if kind
        in {
            RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
            RuntimeServiceKind.SERVING_PUBLISHER,
        }
        else RuntimeServicePlane.RESEARCH
        if kind
        in {
            RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        }
        else RuntimeServicePlane.LIVE
    )
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=plane,
        interval_seconds=1,
        stale_after_seconds=30,
        producer_commit=commit,
        settings={},
    )


def _bundle(
    commit: str,
    *,
    include_serving_publisher: bool = False,
) -> RuntimeSchemaContractBundle:
    manifests = (
        _manifest(
            service_id="reference-slow-source",
            kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
            commit=commit,
        ),
        _manifest(
            service_id="reference-slow-publisher",
            kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
            commit=commit,
        ),
        _manifest(
            service_id="auction-universe-publisher",
            kind=RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
            commit=commit,
        ),
        _manifest(
            service_id="auction-source",
            kind=RuntimeServiceKind.AUCTION_MATCH_SOURCE,
            commit=commit,
        ),
        _manifest(
            service_id="minute-source",
            kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            commit=commit,
        ),
        _manifest(
            service_id="watchlist-quote-source",
            kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
            commit=commit,
        ),
        _manifest(
            service_id="feature-live",
            kind=RuntimeServiceKind.FEATURE_LIVE,
            commit=commit,
        ),
        _manifest(
            service_id="candidate-publisher",
            kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
            commit=commit,
        ),
        _manifest(
            service_id="strategy-live",
            kind=RuntimeServiceKind.STRATEGY_LIVE,
            commit=commit,
        ),
        _manifest(
            service_id="signal-router",
            kind=RuntimeServiceKind.SIGNAL_ROUTER,
            commit=commit,
        ),
        _manifest(
            service_id="notifier",
            kind=RuntimeServiceKind.NOTIFIER,
            commit=commit,
        ),
    )
    if include_serving_publisher:
        manifests += (
            _manifest(
                service_id="serving-publisher",
                kind=RuntimeServiceKind.SERVING_PUBLISHER,
                commit=commit,
            ),
        )
    return build_runtime_schema_contract_bundle(manifests, producer_commit=commit)


def _replace_channel(
    bundle: RuntimeSchemaContractBundle,
    replacement: RuntimeSchemaChannelContract,
) -> RuntimeSchemaContractBundle:
    channels = tuple(
        replacement if channel.channel_id == replacement.channel_id else channel
        for channel in bundle.channels
    )
    return RuntimeSchemaContractBundle.create(
        producer_commit=bundle.producer_commit,
        manifest_fingerprints=bundle.manifest_fingerprints,
        channels=channels,
    )


def _replace_declaration(
    channel: RuntimeSchemaChannelContract,
    *,
    commit: str,
    fields: tuple[SchemaField, ...],
    version: int,
) -> RuntimeSchemaChannelContract:
    declaration = SchemaDeclaration(
        dataset_id=channel.channel_id,
        schema_name=channel.declaration.schema_name,
        min_reader_version=1,
        current_version=version,
        fields=fields,
        producer_commit=commit,
    )
    fields_by_name = declaration.available_fields()
    producers = tuple(
        RuntimeSchemaProducerBinding(
            service_id=binding.service_id,
            capability=ProducerSchemaCapability(
                producer_id=binding.service_id,
                dataset_id=declaration.dataset_id,
                min_writable_version=declaration.min_reader_version,
                max_writable_version=declaration.current_version,
                writable_fields=tuple(sorted(fields_by_name)),
                field_capabilities=tuple(
                    ConsumerFieldCapability(
                        name=field.name,
                        type_name=field.type_name,
                        nullable=field.nullable,
                        semantic_fingerprint=field.effective_semantic_fingerprint,
                    )
                    for field in sorted(fields_by_name.values(), key=lambda item: item.name)
                ),
            ),
        )
        for binding in channel.producers
    )
    return channel.model_copy(
        update={
            "declaration": declaration,
            "physical_schema": RuntimePhysicalTableSchema.create(
                object_name=channel.payload_model,
                declaration=declaration,
            ),
            "producers": producers,
        }
    )


def test_registry_binds_market_minute_channel_to_actual_payload_model() -> None:
    bundle = _bundle(OLD_COMMIT)

    channel = bundle.channel("runtime.market_minute.batch-envelope")

    assert channel.payload_model == "rquant.live_contracts.BatchEnvelope"
    assert {field.name for field in channel.declaration.fields} == set(BatchEnvelope.model_fields)
    assert channel.producer_service_ids == ("minute-source",)
    assert tuple(binding.service_id for binding in channel.consumers) == ("feature-live",)
    assert all(
        field.type_name.startswith("json-schema-sha256:") for field in channel.declaration.fields
    )


def test_registry_tracks_independent_auction_match_channel() -> None:
    bundle = _bundle(OLD_COMMIT)

    channel = bundle.channel("runtime.auction_match.batch-envelope")

    assert channel.payload_model == "rquant.live_contracts.BatchEnvelope"
    assert channel.producer_service_ids == ("auction-source",)
    assert tuple(binding.service_id for binding in channel.consumers) == ("candidate-publisher",)
    assert {field.name for field in channel.declaration.fields} == set(BatchEnvelope.model_fields)


def test_registry_binds_auction_universe_authority_to_publisher_and_source() -> None:
    bundle = _bundle(OLD_COMMIT)

    channel = bundle.channel("runtime.auction_universe.authority")

    assert channel.payload_model == ("rquant.auction_universe_authority.AuctionUniverseAuthority")
    assert channel.producer_service_ids == ("auction-universe-publisher",)
    assert tuple(binding.service_id for binding in channel.consumers) == ("auction-source",)
    assert {field.name for field in channel.declaration.fields} == set(
        AuctionUniverseAuthority.model_fields
    )


def test_registry_binds_reference_snapshot_semantics_to_source_and_publisher() -> None:
    bundle = _bundle(OLD_COMMIT)

    channel = bundle.channel("runtime.reference_slow.source-snapshot")

    assert channel.payload_model == ("rquant.reference_slow_publisher.ReferenceSlowSourceSnapshot")
    assert channel.producer_service_ids == ("reference-slow-source",)
    assert tuple(binding.service_id for binding in channel.consumers) == (
        "reference-slow-publisher",
    )
    assert {field.name for field in channel.declaration.fields} == set(
        ReferenceSlowSourceSnapshot.model_fields
    )

    generation = bundle.channel("runtime.reference_slow.generation-manifest")
    assert generation.declaration.current_version == 2


def test_registry_tracks_independent_watchlist_quote_channel() -> None:
    bundle = _bundle(OLD_COMMIT)

    channel = bundle.channel("runtime.watchlist_quote.batch-envelope")

    assert channel.payload_model == "rquant.live_contracts.BatchEnvelope"
    assert channel.producer_service_ids == ("watchlist-quote-source",)
    assert {field.name for field in channel.declaration.fields} == set(BatchEnvelope.model_fields)


class TestCoreCqP101PersistedRegistryIdentity:
    def test_signal_declarations_match_the_pre_family_split_registry_identity(self) -> None:
        bundle = _bundle(OLD_COMMIT, include_serving_publisher=True)
        strategy_signal = bundle.channel("runtime.strategy_signal.envelope")

        assert strategy_signal.payload_model == "rquant.signal_contracts.SignalEnvelope"
        assert strategy_signal.declaration.schema_name == "rquant.signal_contracts.SignalEnvelope"
        assert strategy_signal.physical_schema.object_name == (
            "rquant.signal_contracts.SignalEnvelope"
        )
        assert strategy_signal.declaration.schema_fingerprint == (
            "1d5b90cf4800f634427abf9aaa4f5cc8072cb045e0a7affe90dd700672e0c198"
        )

        nested = bundle.channel("runtime.serving.signals")
        assert nested.declaration.schema_fingerprint == (
            "992b0b6441c7e77e426645c09d0298fef7f44c7b25c21c15a852fc5b797a4e45"
        )
        nested_signal_field = next(
            field for field in nested.declaration.fields if field.name == "signals"
        )
        expected_nested_type = (
            "json-schema-sha256:a01914d026c58fbf2effb0d853a829bdf8123d6fb9190d189ae5737a65a44b14"
        )
        assert nested_signal_field.type_name == expected_nested_type
        serving_consumer = nested.consumers[0].requirement
        embedded_consumer_field = next(
            field for field in serving_consumer.field_capabilities if field.name == "signals"
        )
        assert embedded_consumer_field.type_name == expected_nested_type


def test_registry_contract_is_hash_bound_and_rejects_tampering() -> None:
    bundle = _bundle(OLD_COMMIT)
    payload = bundle.model_dump(mode="json")
    payload["producer_commit"] = NEW_COMMIT

    with pytest.raises(ValidationError, match="content_hash"):
        RuntimeSchemaContractBundle.model_validate(payload)


def test_registry_rejects_missing_and_unknown_channels() -> None:
    bundle = _bundle(OLD_COMMIT)
    missing = bundle.model_dump(mode="json")
    missing["channels"] = missing["channels"][:-1]
    missing["content_hash"] = "0" * 64

    with pytest.raises(ValidationError, match="channel catalog"):
        RuntimeSchemaContractBundle.model_validate(missing)

    unknown = bundle.model_dump(mode="json")
    unknown["channels"][0]["channel_id"] = "runtime.unknown.payload"
    unknown["channels"][0]["declaration"]["dataset_id"] = "runtime.unknown.payload"
    for producer in unknown["channels"][0]["producers"]:
        producer["capability"]["dataset_id"] = "runtime.unknown.payload"
    for consumer in unknown["channels"][0]["consumers"]:
        consumer["requirement"]["dataset_id"] = "runtime.unknown.payload"
    unknown["content_hash"] = "0" * 64
    with pytest.raises(ValidationError, match="channel catalog"):
        RuntimeSchemaContractBundle.model_validate(unknown)


def test_parser_rejects_noncanonical_or_hash_tampered_contract() -> None:
    bundle = _bundle(OLD_COMMIT)
    canonical = bundle.model_dump_json().encode("utf-8")

    assert parse_runtime_schema_contract_bundle(canonical) == bundle
    with pytest.raises(RuntimeSchemaCompatibilityError, match="canonical|hash|contract"):
        parse_runtime_schema_contract_bundle(canonical.replace(b"minute-source", b"minute-tamper"))


def test_transition_rejects_new_producer_that_old_consumer_cannot_decode() -> None:
    previous = _bundle(OLD_COMMIT)
    candidate = _bundle(NEW_COMMIT)
    channel = candidate.channel("runtime.market_minute.batch-envelope")
    required = SchemaField(
        name="new_required",
        type_name="json-schema-sha256:" + "f" * 64,
        required=True,
        introduced_in=2,
        nullable=False,
    )
    changed = _replace_declaration(
        channel,
        commit=NEW_COMMIT,
        fields=(*channel.declaration.fields, required),
        version=2,
    )
    candidate = _replace_channel(candidate, changed)

    with pytest.raises(
        RuntimeSchemaCompatibilityError,
        match="new producer.*old consumer.*new_required",
    ):
        validate_runtime_schema_transition(previous=previous, candidate=candidate)


def test_transition_rejects_new_consumer_that_old_producer_cannot_satisfy() -> None:
    previous = _bundle(OLD_COMMIT)
    candidate = _bundle(NEW_COMMIT)
    channel = candidate.channel("runtime.market_minute.batch-envelope")
    binding = channel.consumers[0]
    required = (*binding.requirement.required_fields, "future_required")
    capabilities = (
        *binding.requirement.field_capabilities,
        ConsumerFieldCapability(
            name="future_required",
            type_name="json-schema-sha256:" + "f" * 64,
            nullable=False,
        ),
    )
    changed_binding = RuntimeSchemaConsumerBinding(
        service_id=binding.service_id,
        requirement=ConsumerSchemaRequirement(
            consumer_id=binding.requirement.consumer_id,
            dataset_id=binding.requirement.dataset_id,
            min_version=1,
            max_version=2,
            required_fields=required,
            optional_fields=binding.requirement.optional_fields,
            field_capabilities=capabilities,
            unknown_field_policy=UnknownFieldPolicy.FORBID,
        ),
    )
    changed_channel = channel.model_copy(update={"consumers": (changed_binding,)})
    candidate = _replace_channel(candidate, changed_channel)

    with pytest.raises(
        RuntimeSchemaCompatibilityError,
        match="old producer.*new consumer.*future_required",
    ):
        validate_runtime_schema_transition(previous=previous, candidate=candidate)


def test_transition_rejects_schema_history_rewrite_without_mixed_participants() -> None:
    previous = _bundle(OLD_COMMIT)
    candidate = _bundle(NEW_COMMIT)
    channel = candidate.channel("runtime.market_minute.batch-envelope")
    rewritten = SchemaField(
        name="silently_backfilled",
        type_name="json-schema-sha256:" + "f" * 64,
        required=False,
        introduced_in=1,
        nullable=False,
    )
    changed = _replace_declaration(
        channel,
        commit=NEW_COMMIT,
        fields=(*channel.declaration.fields, rewritten),
        version=1,
    ).model_copy(update={"producer_service_ids": (), "producers": (), "consumers": ()})
    candidate = _replace_channel(candidate, changed)

    with pytest.raises(
        RuntimeSchemaCompatibilityError,
        match="schema history.*same schema version",
    ):
        validate_runtime_schema_transition(previous=previous, candidate=candidate)


def test_unchanged_payload_contract_is_compatible_across_code_commits() -> None:
    previous = _bundle(OLD_COMMIT)
    candidate = _bundle(NEW_COMMIT)

    validate_runtime_schema_transition(previous=previous, candidate=candidate)


def test_consumer_requirements_are_bound_to_real_fields_and_forbid_unknowns() -> None:
    bundle = _bundle(OLD_COMMIT)
    channel = bundle.channel("runtime.market_minute.batch-envelope")
    requirement = channel.consumers[0].requirement

    assert set(requirement.required_fields) == {
        name for name, field in BatchEnvelope.model_fields.items() if field.is_required()
    }
    assert set(requirement.optional_fields) == {
        name for name, field in BatchEnvelope.model_fields.items() if not field.is_required()
    }
    assert requirement.unknown_field_policy is UnknownFieldPolicy.FORBID


def test_registry_v2_records_real_field_lifecycle_instead_of_backfilling_v1() -> None:
    bundle = _bundle(OLD_COMMIT)
    market = bundle.channel("runtime.market_minute.batch-envelope")
    generation = bundle.channel("runtime.reference_slow.generation-manifest")

    assert bundle.schema_version == 2
    assert market.declaration.current_version == 1
    assert {field.introduced_in for field in market.declaration.fields} == {1}
    assert generation.declaration.current_version == 2
    assert generation.declaration.min_reader_version == 2
    assert {field.introduced_in for field in generation.declaration.fields} == {2}


def test_bundle_fingerprint_covers_physical_schema_and_producer_capabilities() -> None:
    bundle = _bundle(OLD_COMMIT)
    channel = bundle.channel("runtime.market_minute.batch-envelope")

    assert channel.physical_schema.storage_format == "pydantic-json/v1"
    assert channel.physical_schema.object_name == channel.payload_model
    assert tuple(column.ordinal for column in channel.physical_schema.columns) == tuple(
        range(len(channel.physical_schema.columns))
    )
    assert channel.physical_schema.physical_schema_fingerprint
    assert tuple(binding.service_id for binding in channel.producers) == ("minute-source",)
    producer = channel.producers[0].capability
    assert producer.dataset_id == channel.channel_id
    assert producer.min_writable_version == 1
    assert producer.max_writable_version == channel.declaration.current_version
    assert set(producer.writable_fields) == set(BatchEnvelope.model_fields)

    payload = bundle.model_dump(mode="json")
    payload["channels"][0]["physical_schema"]["storage_format"] = "tampered/v1"
    with pytest.raises(ValidationError, match="content_hash|physical.schema|pydantic-json"):
        RuntimeSchemaContractBundle.model_validate(payload)


def test_bundle_binds_canonical_serving_physical_schema() -> None:
    bundle = _bundle(OLD_COMMIT)

    assert bundle.serving_physical_schema_fingerprint == serving_physical_table_specs_fingerprint()

    payload = bundle.model_dump(mode="json")
    payload["serving_physical_schema_fingerprint"] = "f" * 64
    with pytest.raises(ValidationError, match="content_hash"):
        RuntimeSchemaContractBundle.model_validate(payload)


def test_parser_fails_closed_for_legacy_v1_registry() -> None:
    payload = _bundle(OLD_COMMIT).model_dump(mode="json")
    payload["schema_version"] = 1
    payload["content_hash"] = "0" * 64
    serialized = (
        __import__("json")
        .dumps(
            payload,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        .encode("utf-8")
    )

    with pytest.raises(RuntimeSchemaCompatibilityError, match="legacy v1"):
        parse_runtime_schema_contract_bundle(serialized)


def test_legacy_v1_migration_requires_explicit_complete_field_lifecycle_review() -> None:
    candidate = _bundle(NEW_COMMIT)
    legacy = json.dumps(
        {"schema_version": 1, "channels": []},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    reviewed = tuple(
        RuntimeSchemaV1LifecycleReview(
            channel_id=channel.channel_id,
            field_name=field.name,
            introduced_in=field.introduced_in,
            deprecated_in=field.deprecated_in,
            removed_in=field.removed_in,
        )
        for channel in candidate.channels
        for field in channel.declaration.fields
    )

    with pytest.raises(RuntimeSchemaCompatibilityError, match="complete.*lifecycle"):
        build_runtime_schema_v1_migration_audit(
            legacy_payload=legacy,
            candidate=candidate,
            reason="reviewed historic contracts",
            previous_generation_id="3" * 64,
            reviewed_lifecycles=reviewed[:-1],
            migrated_at=datetime(2026, 8, 2, 1, 0, tzinfo=UTC),
        )

    audit = build_runtime_schema_v1_migration_audit(
        legacy_payload=legacy,
        candidate=candidate,
        reason="reviewed historic contracts",
        previous_generation_id="3" * 64,
        reviewed_lifecycles=reviewed,
        migrated_at=datetime(2026, 8, 2, 1, 0, tzinfo=UTC),
    )

    assert audit.status == "explicit_v1_migration"
    assert audit.legacy_payload_sha256 == hashlib.sha256(legacy).hexdigest()
    assert audit.candidate_content_hash == candidate.content_hash
    assert audit.reviewed_lifecycles == tuple(
        sorted(reviewed, key=lambda item: (item.channel_id, item.field_name))
    )
    assert audit.content_hash == canonical_sha256(
        audit.model_dump(mode="python", exclude={"content_hash"})
    )


def test_rollout_plan_and_trusted_consumers_are_derived_from_hash_bound_bundle() -> None:
    previous = _bundle(OLD_COMMIT)
    candidate = _bundle(NEW_COMMIT)
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)

    plan, registry = runtime_schema_registry.build_runtime_schema_rollout(
        previous=previous,
        candidate=candidate,
        channel_id="runtime.market_minute.batch-envelope",
        target_generation_id="4" * 64,
        started_at=started_at,
        deadline=started_at + timedelta(hours=1),
        consumer_ack_max_age_seconds=300,
    )

    channel = candidate.channel("runtime.market_minute.batch-envelope")
    assert (
        plan.old_declaration_fingerprint
        == previous.channel(channel.channel_id).declaration.schema_fingerprint
    )
    assert plan.new_declaration_fingerprint == channel.declaration.schema_fingerprint
    assert plan.production_consumer_registry_fingerprint == registry.registry_fingerprint
    assert plan.serving_physical_schema_fingerprint == (
        channel.physical_schema.physical_schema_fingerprint
    )
    assert tuple(consumer.consumer_id for consumer in registry.consumers) == ("feature-live",)
    consumer = registry.consumers[0]
    assert consumer.code_commit == NEW_COMMIT
    assert consumer.contract_fingerprint == candidate.manifest_fingerprints["feature-live"]
    assert consumer.required_fields == tuple(
        sorted(channel.consumers[0].requirement.required_fields)
    )
