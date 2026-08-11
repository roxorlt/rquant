from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant import runtime_deployment_bundle as deployment_module
from rquant.live_spool import LiveBatchSpool
from rquant.market_minute_gateway import MarketMinuteGateway, MarketMinuteGatewayConfig
from rquant.runtime_builder_serving import serving_publisher_builder
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_definition_bootstrap import plan_builtin_definitions
from rquant.runtime_deployment_bundle import (
    advance_runtime_schema_rollout,
    install_runtime_deployment_bundle,
    load_runtime_schema_rollout,
    load_runtime_schema_service_bindings,
    prepare_runtime_schema_rollout,
    rollback_runtime_schema_rollout,
)
from rquant.runtime_deployment_profile import (
    RuntimeDeploymentProfile,
    install_runtime_deployment_profile,
)
from rquant.runtime_deployment_rollout import (
    preview_runtime_schema_retirement,
    retire_runtime_schema_plan,
    rollout_runtime_deployment,
)
from rquant.runtime_production_profile import (
    build_production_runtime_profile,
)
from rquant.runtime_schema_registry import (
    RuntimeSchemaContractBundle,
    build_runtime_schema_contract_bundle,
    build_runtime_schema_rollout,
    runtime_schema_dual_write_context,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.runtime_serving_authority import ServingSourceAuthorityPublisher
from rquant.runtime_serving_snapshot import (
    LAB_JOBS_DATASET_ID,
    PAPER_ACCOUNTS_DATASET_ID,
    PROMOTIONS_DATASET_ID,
    REFERENCE_SLOW_AUTHORITY_DATASET_ID,
    RUNTIME_HEALTH_DATASET_ID,
    SIGNALS_DATASET_ID,
    LabJobsPayload,
    PaperAccountsPayload,
    PromotionsPayload,
    ReferenceSlowPayload,
    RuntimeHealthPayload,
    SignalDeliveryPayload,
    SourceReadResult,
)
from rquant.schema_compatibility import (
    ConsumerCapabilityReceipt,
    RolloutPhase,
    SchemaRolloutStore,
    validate_dual_write_values,
)
from rquant.serving_contracts import FreshnessStatus
from tests.unit.test_runtime_production_profile import (
    _inputs as _production_inputs_fixture,
)
from tests.unit.test_runtime_production_profile import (
    _retention_writer_capability,
)


def _manifest(
    *,
    service_id: str,
    kind: RuntimeServiceKind,
    commit: str,
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=RuntimeServicePlane.LIVE,
        interval_seconds=1,
        stale_after_seconds=30,
        producer_commit=commit,
        settings={},
    )


def _bundle(commit: str) -> RuntimeSchemaContractBundle:
    return build_runtime_schema_contract_bundle(
        (
            _manifest(
                service_id="minute-source",
                kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
                commit=commit,
            ),
            _manifest(
                service_id="feature-live",
                kind=RuntimeServiceKind.FEATURE_LIVE,
                commit=commit,
            ),
        ),
        producer_commit=commit,
    )


def test_hash_bound_bundle_drives_persistent_schema_rollout(tmp_path: Path) -> None:
    old_commit = "a" * 40
    new_commit = "b" * 40
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    previous = _bundle(old_commit)
    candidate = _bundle(new_commit)
    channel_id = "runtime.market_minute.batch-envelope"
    plan, registry = build_runtime_schema_rollout(
        previous=previous,
        candidate=candidate,
        channel_id=channel_id,
        target_generation_id="4" * 64,
        started_at=started_at,
        deadline=started_at + timedelta(hours=1),
        consumer_ack_max_age_seconds=300,
    )
    store = SchemaRolloutStore(
        tmp_path / "schema-rollout.sqlite3",
        production_consumer_registry=registry,
    )

    state = store.create_plan(plan, now=started_at, operation_id="create")
    state = store.acknowledge(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        phase=RolloutPhase.PREPARE,
        participant_id="minute-source",
        participant_fingerprint=candidate.manifest_fingerprints["minute-source"],
        declaration_fingerprint=plan.new_declaration_fingerprint,
        now=started_at + timedelta(seconds=1),
        operation_id="producer-prepare",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.DUAL_WRITE,
        now=started_at + timedelta(seconds=2),
        operation_id="dual-write",
    )
    envelope_values = {
        "schema_version": 1,
        "channel": "market_minute",
        "dataset_id": "minute-bar",
        "source": "tushare",
        "source_request_id": "request-1",
        "batch_id": "batch-1",
        "sequence": 1,
        "revision": 1,
        "event_time_start": started_at,
        "event_time_end": started_at,
        "source_time": started_at,
        "received_at": started_at,
        "available_at": started_at,
        "row_count": 1,
        "content_sha256": "6" * 64,
        "quality_status": "published",
        "producer_version": "v-next",
        "producer_commit": new_commit,
    }
    evidence = validate_dual_write_values(
        old_declaration=previous.channel(channel_id).declaration,
        new_declaration=candidate.channel(channel_id).declaration,
        old_values=envelope_values,
        new_values=envelope_values,
        generation_id=plan.target_generation_id,
        observed_at=started_at + timedelta(seconds=3),
    )
    state = store.record_dual_write_evidence(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        evidence=evidence,
        operation_id="dual-write-evidence",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CONSUMER_ACK,
        now=started_at + timedelta(seconds=4),
        operation_id="consumer-ack",
    )
    consumer = registry.consumers[0]
    state = store.acknowledge_consumer(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        receipt=ConsumerCapabilityReceipt(
            consumer_id=consumer.consumer_id,
            service_id=consumer.service_id,
            code_commit=consumer.code_commit,
            dataset_id=consumer.dataset_id,
            min_readable_schema_version=consumer.min_readable_schema_version,
            max_readable_schema_version=consumer.max_readable_schema_version,
            required_fields=consumer.required_fields,
            serving_physical_schema_fingerprint=(plan.serving_physical_schema_fingerprint),
            observed_generation_id=plan.target_generation_id,
            available_at=started_at + timedelta(seconds=5),
        ),
        now=started_at + timedelta(seconds=5),
        operation_id="feature-live-ack",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CUTOVER,
        now=started_at + timedelta(seconds=6),
        operation_id="cutover",
    )
    state = store.acknowledge(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        phase=RolloutPhase.CUTOVER,
        participant_id="minute-source",
        participant_fingerprint=candidate.manifest_fingerprints["minute-source"],
        declaration_fingerprint=plan.new_declaration_fingerprint,
        now=started_at + timedelta(seconds=7),
        operation_id="producer-cutover",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.RETIRE,
        now=started_at + timedelta(seconds=8),
        operation_id="retire",
    )

    reopened = SchemaRolloutStore(
        tmp_path / "schema-rollout.sqlite3",
        production_consumer_registry=registry,
    )
    assert reopened.get_state(plan.plan_id) == state
    assert state.phase is RolloutPhase.RETIRE
    assert state.authority_declaration_fingerprint == plan.new_declaration_fingerprint
    assert reopened.dual_write_evidence(plan.plan_id) == (evidence,)


def test_real_bundle_producer_consumer_cutover_and_rollback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _Transaction:
        sealed_instances: tuple[str, ...] = ()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    class _Recovery:
        outcome = "none"
        transaction_id = None

    monkeypatch.setattr(
        deployment_module,
        "_seal_runtime_credentials",
        lambda _items: _Transaction(),
    )
    monkeypatch.setattr(
        deployment_module,
        "_recover_runtime_credentials",
        lambda **_kwargs: _Recovery(),
    )
    root = tmp_path / "runtime"
    old_commit = "a" * 40
    new_commit = "b" * 40
    calendar_hash = "c" * 64
    calendar_path = (
        root / "authorities" / "market-calendar" / "generations" / f"{calendar_hash}.json"
    )

    def manifests(commit: str) -> tuple[RuntimeServiceManifest, ...]:
        return (
            RuntimeServiceManifest(
                service_id="minute-source",
                service_kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
                plane=RuntimeServicePlane.LIVE,
                interval_seconds=1,
                stale_after_seconds=30,
                producer_commit=commit,
                settings={
                    "spool_root": str(root / "live" / "market-minute"),
                    "quota_path": str(root / "live" / "market-minute" / "quota.sqlite3"),
                    "calendar_path": str(calendar_path),
                    "calendar_expected_commit": old_commit,
                    "calendar_content_sha256": calendar_hash,
                },
            ),
            RuntimeServiceManifest(
                service_id="feature-live",
                service_kind=RuntimeServiceKind.FEATURE_LIVE,
                plane=RuntimeServicePlane.LIVE,
                interval_seconds=1,
                stale_after_seconds=30,
                producer_commit=commit,
                settings={
                    "raw_spool_root": str(root / "live" / "market-minute"),
                    "feature_spool_root": str(root / "live" / "features"),
                    "historical_minutes_snapshot_path": str(root / "research" / "minute.parquet"),
                },
            ),
        )

    capabilities = {
        "minute-source": {"TUSHARE_TOKEN_MAIN": "secret"},
        "feature-live": {},
    }
    first = install_runtime_deployment_bundle(
        root,
        producer_commit=old_commit,
        manifests=manifests(old_commit),
        capability_env=capabilities,
        schema_bootstrap_reason="reviewed integration bootstrap",
    )
    second = install_runtime_deployment_bundle(
        root,
        producer_commit=new_commit,
        manifests=manifests(new_commit),
        capability_env=capabilities,
    )
    started = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    authority = prepare_runtime_schema_rollout(
        root,
        previous_generation_id=first.generation_hash,
        target_generation_id=second.generation_hash,
        channel_id="runtime.market_minute.batch-envelope",
        started_at=started,
        deadline=started + timedelta(hours=1),
        consumer_ack_max_age_seconds=300,
    )
    candidate_manifests = manifests(new_commit)
    producer_bindings = load_runtime_schema_service_bindings(
        root,
        manifest=candidate_manifests[0],
        generation_id=second.generation_hash,
        observed_at=started + timedelta(seconds=1),
    )
    assert len(producer_bindings) == 1

    frame = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": started,
                "open": 10.0,
                "high": 10.2,
                "low": 9.9,
                "close": 10.1,
                "vol": 1000.0,
                "amount": 10100.0,
            }
        ]
    )
    with runtime_schema_dual_write_context(producer_bindings):
        MarketMinuteGateway(
            spool=LiveBatchSpool(root / "live" / "market-minute"),
            fetcher=lambda: frame,
            config=MarketMinuteGatewayConfig(
                producer_version="integration-v2",
                producer_commit=new_commit,
            ),
        ).capture_once(received_at=started + timedelta(seconds=2))

    _, store = load_runtime_schema_rollout(root, plan_id=authority.plan_id)
    assert len(store.dual_write_records(authority.plan_id)) == 1
    state = store.get_state(authority.plan_id)
    state = advance_runtime_schema_rollout(
        root,
        plan_id=authority.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CONSUMER_ACK,
        now=started + timedelta(seconds=3),
        operation_id="integration-consumer-ack",
    )
    assert state.phase is RolloutPhase.CONSUMER_ACK

    assert (
        load_runtime_schema_service_bindings(
            root,
            manifest=candidate_manifests[1],
            generation_id=second.generation_hash,
            observed_at=started + timedelta(seconds=4),
        )
        == ()
    )
    receipts = store.consumer_capability_receipts(authority.plan_id)
    assert len(receipts) == 1
    assert receipts[0].service_id == "feature-live"
    assert receipts[0].code_commit == new_commit
    assert receipts[0].observed_generation_id == second.generation_hash
    assert receipts[0].serving_physical_schema_fingerprint == (
        authority.plan.serving_physical_schema_fingerprint
    )
    state = store.get_state(authority.plan_id)
    state = advance_runtime_schema_rollout(
        root,
        plan_id=authority.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CUTOVER,
        now=started + timedelta(seconds=5),
        operation_id="integration-cutover",
    )
    assert state.phase is RolloutPhase.CUTOVER
    assert (
        load_runtime_schema_service_bindings(
            root,
            manifest=candidate_manifests[0],
            generation_id=second.generation_hash,
            observed_at=started + timedelta(seconds=6),
        )
        == ()
    )

    state = store.get_state(authority.plan_id)
    rolled_back = rollback_runtime_schema_rollout(
        root,
        plan_id=authority.plan_id,
        expected_revision=state.revision,
        reason="post-cutover verification failed",
        now=started + timedelta(seconds=7),
        operation_id="integration-rollback",
    )
    assert rolled_back.phase is RolloutPhase.ROLLBACK
    assert (root / "current").readlink() == Path("generations") / first.generation_hash
    assert len(store.dual_write_records(authority.plan_id)) == 1


def _production_profile(
    tmp_path: Path,
    *,
    commit: str,
) -> RuntimeDeploymentProfile:
    fixture = _production_inputs_fixture(tmp_path)
    inputs_payload = fixture.model_dump(mode="python")
    inputs_payload["producer_commit"] = commit
    inputs_payload["strategies"] = tuple(
        binding.model_dump(mode="python")
        for binding in plan_builtin_definitions(producer_commit=commit).strategies
    )
    full = build_production_runtime_profile(inputs_payload)
    payload = full.model_dump(mode="python", exclude={"profile_id"})
    payload["schema_rollout_policies"] = tuple(
        policy
        for policy in full.schema_rollout_policies
        if policy.channel_id == "runtime.serving.signals"
    )
    return RuntimeDeploymentProfile.model_validate(payload)


def _production_environment() -> dict[str, str]:
    return {
        "TUSHARE_TOKEN_MAIN": "tushare-secret",
        "PUSHDEER_KEYS": "pushdeer-secret",
        "PUSHPLUS_TOKENS": "pushplus-secret",
        "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _retention_writer_capability(),
        "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": "cHJpdmF0ZS1rZXk=",
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY": "ssh-ed25519 AAAAtest reference-source",
    }


def _publish_serving_owners(
    manifest: RuntimeServiceManifest,
    *,
    observed_at: datetime,
) -> None:
    payloads = {
        SIGNALS_DATASET_ID: SignalDeliveryPayload(),
        PAPER_ACCOUNTS_DATASET_ID: PaperAccountsPayload(),
        RUNTIME_HEALTH_DATASET_ID: RuntimeHealthPayload(),
        LAB_JOBS_DATASET_ID: LabJobsPayload(),
        PROMOTIONS_DATASET_ID: PromotionsPayload(),
        REFERENCE_SLOW_AUTHORITY_DATASET_ID: ReferenceSlowPayload(
            reference_generation_id="d" * 64,
            revision=1,
            price_basis="raw_session",
            adjustment_basis="tushare_adj_factor",
            available_at=observed_at,
        ),
    }
    authorities = manifest.settings["source_authorities"]
    assert isinstance(authorities, tuple)
    assert len(authorities) == 6
    for sequence, authority in enumerate(authorities, start=1):
        assert isinstance(authority, Mapping)
        dataset_id = str(authority["dataset_id"])
        payload = payloads[dataset_id]
        result_values = {
            "dataset_id": dataset_id,
            "sequence": sequence,
            "event_time": observed_at - timedelta(seconds=1),
            "published_at": observed_at,
            "status": FreshnessStatus.FRESH,
            "reason": None,
            "payload": payload,
        }
        result = SourceReadResult.model_validate(
            {
                **result_values,
                "generation_id": canonical_sha256(result_values),
            }
        )
        ServingSourceAuthorityPublisher(
            root=Path(str(authority["root"])),
            producer_commit=manifest.producer_commit,
            dataset_id=dataset_id,
            payload_kind=payload.payload_kind,
            clock=lambda: observed_at,
        ).publish(result)


def test_production_profile_six_owner_serving_ack_cutover_and_rollback(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _Transaction:
        sealed_instances: tuple[str, ...] = ()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    class _Recovery:
        outcome = "none"
        transaction_id = None

    monkeypatch.setattr(
        deployment_module,
        "_seal_runtime_credentials",
        lambda _items: _Transaction(),
    )
    monkeypatch.setattr(
        deployment_module,
        "_recover_runtime_credentials",
        lambda **_kwargs: _Recovery(),
    )
    root = tmp_path / "source" / "runtime"
    old_profile = _production_profile(tmp_path, commit="a" * 40)
    new_profile = _production_profile(tmp_path, commit="b" * 40)
    first = install_runtime_deployment_profile(
        old_profile,
        runtime_root=root,
        environ=_production_environment(),
        schema_bootstrap_reason="reviewed six-owner production bootstrap",
    )
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    candidate = install_runtime_deployment_profile(
        new_profile,
        runtime_root=root,
        environ=_production_environment(),
        schema_rollout_started_at=started_at,
    )
    assert len(candidate.schema_rollout_plan_ids) == 1
    plan_id = candidate.schema_rollout_plan_ids[0]
    notifier = next(
        manifest
        for manifest in new_profile.manifests
        if manifest.service_kind is RuntimeServiceKind.NOTIFIER
    )
    serving = next(
        manifest
        for manifest in new_profile.manifests
        if manifest.service_kind is RuntimeServiceKind.SERVING_PUBLISHER
    )
    authorities = serving.settings["source_authorities"]
    assert isinstance(authorities, tuple)
    signals_owner = next(
        Path(str(authority["root"]))
        for authority in authorities
        if isinstance(authority, Mapping) and authority["dataset_id"] == SIGNALS_DATASET_ID
    )
    assert signals_owner == (
        root
        / "live"
        / "notifications"
        / candidate.instance_mapping[notifier.service_id]
        / "serving-authority"
    )

    producer_bindings = load_runtime_schema_service_bindings(
        root,
        manifest=notifier,
        generation_id=candidate.generation_hash,
        observed_at=started_at + timedelta(seconds=1),
    )
    assert len(producer_bindings) == 1
    prepared = producer_bindings[0].prepare_payload(
        SignalDeliveryPayload().model_dump(mode="json"),
        observed_at=started_at + timedelta(seconds=2),
    )
    assert prepared is not None
    producer_bindings[0].commit_payload(
        prepared,
        operation_id="production-signals-dual-write",
    )
    _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id)
    state = store.get_state(plan_id)
    state = advance_runtime_schema_rollout(
        root,
        plan_id=plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CONSUMER_ACK,
        now=started_at + timedelta(seconds=3),
        operation_id="production-signals-consumer-ack",
    )
    _publish_serving_owners(serving, observed_at=started_at + timedelta(seconds=4))
    consumer_bindings = load_runtime_schema_service_bindings(
        root,
        manifest=serving,
        generation_id=candidate.generation_hash,
        observed_at=started_at + timedelta(seconds=5),
    )
    assert len(consumer_bindings) == 1
    with runtime_schema_dual_write_context(consumer_bindings):
        serving_result = serving_publisher_builder(
            snapshot_loader=None,
            clock=lambda: started_at + timedelta(seconds=5),
        )(serving)()
    consumer_receipts = store.consumer_capability_receipts(plan_id)
    assert len(consumer_receipts) == 1
    assert (
        consumer_receipts[0].serving_generation_id
        == (serving_result.source_generations["serving_generation"])
    )
    state = store.get_state(plan_id)
    state = advance_runtime_schema_rollout(
        root,
        plan_id=plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CUTOVER,
        now=started_at + timedelta(seconds=6),
        operation_id="production-signals-cutover",
    )
    assert (
        load_runtime_schema_service_bindings(
            root,
            manifest=notifier,
            generation_id=candidate.generation_hash,
            observed_at=started_at + timedelta(seconds=7),
        )
        == ()
    )
    rolled_back = rollback_runtime_schema_rollout(
        root,
        plan_id=plan_id,
        expected_revision=store.get_state(plan_id).revision,
        reason="post-cutover serving audit failed",
        now=started_at + timedelta(seconds=8),
        operation_id="production-signals-rollback",
    )
    assert rolled_back.phase is RolloutPhase.ROLLBACK
    assert rolled_back.new_data_preserved is True
    assert (root / "current").readlink() == Path("generations") / first.generation_hash
    assert len(store.dual_write_records(plan_id)) == 1


def test_production_rollout_orchestrates_schema_cutover_and_binds_audit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _Transaction:
        sealed_instances: tuple[str, ...] = ()

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    class _Recovery:
        outcome = "none"
        transaction_id = None

    monkeypatch.setattr(
        deployment_module,
        "_seal_runtime_credentials",
        lambda _items: _Transaction(),
    )
    monkeypatch.setattr(
        deployment_module,
        "_recover_runtime_credentials",
        lambda **_kwargs: _Recovery(),
    )
    root = tmp_path / "source" / "runtime"
    old_profile = _production_profile(tmp_path, commit="a" * 40)
    new_profile = _production_profile(tmp_path, commit="b" * 40)
    first = install_runtime_deployment_profile(
        old_profile,
        runtime_root=root,
        environ=_production_environment(),
        schema_bootstrap_reason="reviewed production rollout bootstrap",
    )
    started_at = datetime(2026, 8, 2, 2, 0, tzinfo=UTC)
    candidate = install_runtime_deployment_profile(
        new_profile,
        runtime_root=root,
        environ=_production_environment(),
        schema_rollout_started_at=started_at,
    )
    plan_id = candidate.schema_rollout_plan_ids[0]
    serving = next(
        manifest
        for manifest in new_profile.manifests
        if manifest.service_kind is RuntimeServiceKind.SERVING_PUBLISHER
    )
    _publish_serving_owners(serving, observed_at=started_at + timedelta(seconds=1))
    manifests_by_unit = {
        candidate.unit_mapping[manifest.service_id]: manifest for manifest in new_profile.manifests
    }
    ticks = 1

    def clock() -> datetime:
        nonlocal ticks
        ticks += 1
        return started_at + timedelta(seconds=ticks)

    class Controller:
        def daemon_reload(self) -> None:
            pass

        def restart(self, unit: str) -> None:
            manifest = manifests_by_unit[unit]
            observed_at = clock()
            bindings = load_runtime_schema_service_bindings(
                root,
                manifest=manifest,
                generation_id=candidate.generation_hash,
                observed_at=observed_at,
            )
            if manifest.service_kind is RuntimeServiceKind.NOTIFIER:
                for binding in bindings:
                    prepared = binding.prepare_payload(
                        SignalDeliveryPayload().model_dump(mode="json"),
                        observed_at=observed_at,
                    )
                    if prepared is not None:
                        binding.commit_payload(
                            prepared,
                            operation_id=f"rollout-notifier:{ticks}",
                        )
            elif manifest.service_kind is RuntimeServiceKind.SERVING_PUBLISHER:
                with runtime_schema_dual_write_context(bindings):
                    serving_publisher_builder(
                        snapshot_loader=None,
                        clock=lambda: observed_at,
                    )(manifest)()

        def stop(self, _unit: str) -> None:
            pass

        def wait_healthy(
            self,
            _unit: str,
            *,
            receipt,
            timeout_seconds: float,
        ) -> None:
            assert receipt.generation_hash == candidate.generation_hash
            assert timeout_seconds > 0

    audit = rollout_runtime_deployment(
        candidate,
        controller=Controller(),
        current_receipt_loader=lambda: candidate,
        previous_receipt_loader=lambda generation: (
            first if generation == first.generation_hash else None
        ),
        previous_generation_activator=lambda _receipt: None,
        audit_root=root / "control" / "deployment-rollouts",
        health_timeout_seconds=30,
        clock=clock,
    )

    _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id)
    assert audit.status == "succeeded"
    assert audit.schema_rollout_plan_ids == (plan_id,)
    assert audit.schema_receipt_hashes == (store.receipts(plan_id)[-1].event_hash,)
    assert store.get_state(plan_id).phase is RolloutPhase.CUTOVER
    assert store.consumer_capability_receipts(plan_id)[0].serving_generation_id is not None
    assert len(audit.schema_retire_observations) == 1
    observation = audit.schema_retire_observations[0]
    assert observation.plan_id == plan_id
    assert observation.cutover_receipt_hash == audit.schema_receipt_hashes[0]
    assert observation.retire_eligible_at == (observation.cutover_observed_at + timedelta(days=1))

    before = preview_runtime_schema_retirement(
        candidate,
        rollout_audit=audit,
        now=observation.retire_eligible_at - timedelta(seconds=1),
    )
    assert len(before) == 1
    assert before[0].phase is RolloutPhase.CUTOVER
    assert before[0].eligible is False
    with pytest.raises(ValueError, match="observation window"):
        retire_runtime_schema_plan(
            candidate,
            rollout_audit=audit,
            plan_id=plan_id,
            now=observation.retire_eligible_at - timedelta(seconds=1),
            operation_id="explicit-retire-too-early",
        )

    eligible = preview_runtime_schema_retirement(
        candidate,
        rollout_audit=audit,
        now=observation.retire_eligible_at,
    )
    assert eligible[0].eligible is True
    retired = retire_runtime_schema_plan(
        candidate,
        rollout_audit=audit,
        plan_id=plan_id,
        now=observation.retire_eligible_at,
        operation_id="operator-approved-retire",
    )
    assert retired.phase is RolloutPhase.RETIRE
    assert store.get_state(plan_id).phase is RolloutPhase.RETIRE
