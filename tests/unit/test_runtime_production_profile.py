from __future__ import annotations

import base64
import fcntl
import hashlib
import json
import os
import stat
import subprocess
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

import rquant.runtime_deployment_bundle as deployment_bundle_module
import rquant.runtime_production_profile as production_profile_module
from rquant.lab_highwater_authority import PRODUCTION_LAB_HIGHWATER_COMMAND
from rquant.legacy_shadow_export import LegacyShadowRunnerManifestBinding
from rquant.runtime_builder_artifact_catalog import ArtifactCatalogSettings
from rquant.runtime_builder_authority import (
    LabJobsPublisherSettings,
    PaperConstraintRuntimeSettings,
    PromotionsPublisherSettings,
    RuntimeHealthPublisherSettings,
)
from rquant.runtime_builder_candidate import CandidatePublisherRuntimeSettings
from rquant.runtime_builder_daily import DailyCloseSourceSettings
from rquant.runtime_builder_daily_orchestrator import DailyPipelineOrchestratorSettings
from rquant.runtime_builder_feature import FeatureLiveRuntimeSettings
from rquant.runtime_builder_paper import PaperBrokerSettings
from rquant.runtime_builder_retention import ArtifactRetentionSettings
from rquant.runtime_builder_serving import ServingRuntimeSettings
from rquant.runtime_builder_shadow import ShadowSessionSettings
from rquant.runtime_builder_signal import NotifierSettings, SignalRouterSettings
from rquant.runtime_builder_strategy import StrategyLiveRuntimeSettings
from rquant.runtime_definition_bootstrap import plan_builtin_definitions
from rquant.runtime_deployment_profile import (
    LINUX_PRODUCTION_RUNTIME_ROOT,
    RuntimeDeploymentProfile,
    RuntimeRecoveryArtifactRoleBinding,
    RuntimeRecoveryProductionConfig,
    build_runtime_recovery_preflight_config,
    install_runtime_deployment_profile,
    preview_runtime_deployment_profile,
    validate_runtime_recovery_backup_config,
)
from rquant.runtime_market_calendar_generation import market_calendar_generation_path
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_production_profile import (
    ProductionRuntimeProfileInputs,
    ProductionStrategyBinding,
    build_production_runtime_profile,
    install_production_runtime_prerequisites,
    load_production_runtime_profile_inputs,
    publish_production_runtime_profile,
)
from rquant.runtime_recovery_artifacts import (
    RealRecoveryArtifactKind,
    RealRecoveryArtifactSpec,
)
from rquant.runtime_recovery_backup import RecoveryBackupConfig
from rquant.runtime_service_builtin import (
    AuctionMatchSourceSettings,
    AuctionUniversePublisherSettings,
    MarketMinuteSourceSettings,
    ReferenceSlowPublisherSettings,
    ReferenceSlowSourceSettings,
    WatchlistQuoteSourceSettings,
    build_builtin_registry,
)
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.signal_route_spool import SignalRouteSpool
from rquant.strict_json import canonical_json_bytes, canonical_model_json_bytes
from tests.highwater_ed25519_support import resolve_openssl

COMMIT = "a" * 40
STRATEGY_IDS = ("auction_gap", "growth_board_surge", "n_shape")
STRATEGY_COMPLETION_FIELDS = (
    "calendar_path",
    "calendar_expected_commit",
    "calendar_content_sha256",
    "signal_bus_path",
    "routing_policy_fingerprint",
    "producer_instance_id",
    "producer_version",
    "strategy_spec_fingerprint",
    "evaluator_contract_fingerprint",
)


def _retention_writer_capability() -> str:
    return json.dumps(
        {
            "key_id": "artifact-retention-test",
            "sequence": 1,
            "secret_hex": "1" * 64,
            "not_before": "2026-01-01T00:00:00+00:00",
            "expires_at": "2027-01-01T00:00:00+00:00",
            "revoked_at": None,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


EXTERNAL_OWNER_BUILDER_SMOKE_DEFERRALS = {
    RuntimeServiceKind.SHADOW_SESSION: (
        "root-owned Shadow report signer and legacy evidence exports"
    ),
    RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: (
        "post-close shadow spool and stage ledger authority"
    ),
    RuntimeServiceKind.STRATEGY_LIVE: "shadow completion-attestation signer injection",
    RuntimeServiceKind.SIGNAL_ROUTER: "shadow strategy runner authority startup",
    RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: "shadow owner authority",
    RuntimeServiceKind.PAPER_BROKER: "shadow broker authority",
    RuntimeServiceKind.LAB_JOBS_PUBLISHER: "job authority",
    RuntimeServiceKind.LAB_ARTIFACT_CATALOG: "job artifact authority",
    RuntimeServiceKind.ARTIFACT_RETENTION: "fixed recovery deletion-gate evidence",
    RuntimeServiceKind.PROMOTIONS_PUBLISHER: "job promotion authority",
}


def _builtin_bindings() -> tuple[ProductionStrategyBinding, ...]:
    return tuple(
        ProductionStrategyBinding.model_validate(binding.model_dump(mode="python"))
        for binding in plan_builtin_definitions(producer_commit=COMMIT).strategies
    )


def _inputs(tmp_path: Path) -> ProductionRuntimeProfileInputs:
    source_root = tmp_path / "source"
    external = source_root / "external"
    runtime_root = source_root / "runtime"
    broker_id = "paper-broker.shadow-main.v1"
    broker_instance = "svc-" + hashlib.sha256(broker_id.encode("utf-8")).hexdigest()
    recovery = RuntimeRecoveryProductionConfig(
        backup_source_root=source_root,
        backup_publication_root=Path("/var/lib/rquant/runtime-recovery/backups"),
        isolated_restore_root=Path("/var/lib/rquant/runtime-recovery/restores"),
        service_state_path=runtime_root / "control" / "recovery" / "service.sqlite3",
        service_receipt_root=runtime_root / "control" / "recovery" / "receipts",
        backup_config_path=external / "config" / "runtime-recovery-backup.json",
        credential_file=external / "secrets" / "runtime-recovery.json",
        artifact_roles=(
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="paper_ledger",
                kind=RealRecoveryArtifactKind.STATE_SQLITE,
                source_path=(f"runtime/live/paper-brokers/{broker_instance}/broker.sqlite3"),
                restore_path="state/paper.sqlite3",
                schema_version="paper-ledger-v5",
                relations=(
                    "paper_ledger_attestation",
                    "paper_ledger_head_marker",
                    "paper_ledger_schema",
                ),
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="production",
                kind=RealRecoveryArtifactKind.PRODUCTION_DUCKDB,
                source_path="external/rquant_ro.duckdb",
                restore_path="production/rquant.duckdb",
                schema_version="v1",
                relations=("auction_bar", "daily_bar", "minute_bar"),
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="runtime_state",
                kind=RealRecoveryArtifactKind.STATE_SQLITE,
                source_path="runtime/control/recovery/service.sqlite3",
                restore_path="state/runtime-recovery.sqlite3",
                schema_version="runtime-recovery-service-v1",
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="research_catalog",
                kind=RealRecoveryArtifactKind.RESEARCH_CATALOG,
                source_path="external/research.duckdb",
                restore_path="research/research.duckdb",
                schema_version="research-catalog-v1",
                references={"lake:current": "research_lake_manifest"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="research_catalog_readonly",
                kind=RealRecoveryArtifactKind.RESEARCH_CATALOG_READONLY,
                source_path="external/research_ro.duckdb",
                restore_path="research/research_ro.duckdb",
                schema_version="research-catalog-v1",
                references={"authority": "research_catalog"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="research_lake_manifest",
                kind=RealRecoveryArtifactKind.RESEARCH_LAKE_MANIFEST,
                source_path="external/research-lake/current.json",
                restore_path="research-lake/current.json",
                schema_version="research-lake-manifest-v1",
                references={"parquet": "research_lake_object"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="research_lake_object",
                kind=RealRecoveryArtifactKind.RESEARCH_LAKE_OBJECT,
                source_path="external/research-lake/objects/current.parquet",
                restore_path="research-lake/objects/current.parquet",
                schema_version="parquet-v1",
                references={"manifest": "research_lake_manifest"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="lab_artifact_manifest",
                kind=RealRecoveryArtifactKind.LAB_ARTIFACT_MANIFEST,
                source_path="runtime/research/final-artifacts/current.json",
                restore_path="lab-artifacts/current.json",
                schema_version="lab-artifact-manifest-v1",
                references={"file:current.json": "lab_artifact_object"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="lab_artifact_object",
                kind=RealRecoveryArtifactKind.LAB_ARTIFACT_OBJECT,
                source_path="runtime/research/final-artifacts/objects/current.json",
                restore_path="lab-artifacts/objects/current.json",
                schema_version="lab-artifact-object-v1",
                references={"manifest": "lab_artifact_manifest"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="serving_current",
                kind=RealRecoveryArtifactKind.SERVING_CURRENT,
                source_path="runtime/serving/current.json",
                restore_path="serving/current.json",
                schema_version="serving-current-v1",
                references={"manifest": "serving_manifest"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="serving_manifest",
                kind=RealRecoveryArtifactKind.SERVING_MANIFEST,
                source_path="runtime/serving/current/manifest.json",
                restore_path="serving/current/manifest.json",
                schema_version="serving-manifest-v3",
                references={
                    "database": "serving_database",
                    "reference": "reference_slow",
                },
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="serving_database",
                kind=RealRecoveryArtifactKind.SERVING_DATABASE,
                source_path="runtime/serving/current/serving.duckdb",
                restore_path="serving/current/serving.duckdb",
                schema_version="serving-v3",
                references={"manifest": "serving_manifest"},
            ),
            RuntimeRecoveryArtifactRoleBinding(
                logical_role="reference_slow",
                kind=RealRecoveryArtifactKind.REFERENCE_SLOW_SQLITE,
                source_path="runtime/authorities/reference-slow/reference.sqlite3",
                restore_path="reference/reference.sqlite3",
                schema_version="reference-slow-v1",
            ),
        ),
        production_artifact_role="production",
        paper_ledger_artifact_role="paper_ledger",
        signer_key_id="production-recovery-v1",
        max_rpo_seconds=1800,
        max_rto_seconds=900,
        max_rehearsal_age_seconds=604800,
        recovery_lease_seconds=300,
        recovery_max_attempts=3,
        recovery_retry_delay_seconds=60,
        recovery_deadline_seconds=3600,
        rehearsal_interval_seconds=604800,
    )
    return ProductionRuntimeProfileInputs(
        producer_commit=COMMIT,
        runtime_root=runtime_root,
        operational_database_path=external / "rquant_ro.duckdb",
        definition_registry_root=external / "definitions",
        n_shape_candidate_input_path=external / "candidates" / "n-shape.json",
        growth_board_candidate_input_path=(external / "candidates" / "growth-board-surge.json"),
        historical_minutes_snapshot_path=external / "snapshots" / "minute.parquet",
        historical_minutes_snapshot_id="b" * 64,
        market_calendar_authority_path=external / "calendar" / "market-calendar.json",
        market_calendar_content_sha256="e" * 64,
        market_calendar_producer_commit=COMMIT,
        trade_calendar_path=external / "calendar" / "trade-calendar.json",
        trade_calendar_sha256="c" * 64,
        routing_policy_path=external / "policies" / "signal-routing.json",
        routing_policy_fingerprint="d" * 64,
        strategies=_builtin_bindings(),
        artifact_location_id="tencent-primary",
        artifact_failure_domain="tencent-shanghai",
        artifact_retention_schema_authority_path=(
            external / "authorities" / "artifact-descriptor-schema.json"
        ),
        artifact_retention_schema_authority_sha256="7" * 64,
        artifact_retention_recovery_target_manifest_id="8" * 64,
        artifact_retention_full_recovery_receipt_id="9" * 64,
        shadow_completion_active_key_id="shadow-completion-v1",
        shadow_completion_active_public_key_pem="shadow-completion-public-test-key",
        shadow_report_active_key_id="shadow-report-v1",
        shadow_report_active_public_key_pem="shadow-report-public-test-key",
        recovery=recovery,
    )


def _by_kind(inputs: ProductionRuntimeProfileInputs):
    profile = build_production_runtime_profile(inputs)
    return profile, {
        kind: tuple(item for item in profile.manifests if item.service_kind is kind)
        for kind in RuntimeServiceKind
    }


def _openssl() -> str:
    return resolve_openssl()


def _daily_private_key(root: Path, *, key_id: str) -> tuple[Path, str]:
    root.mkdir(parents=True, mode=0o700, exist_ok=True)
    private_key = root / f"{key_id}.private.pem"
    subprocess.run(
        [_openssl(), "genpkey", "-algorithm", "ED25519", "-out", str(private_key)],
        check=True,
        capture_output=True,
    )
    private_key.chmod(0o600)
    public_key = subprocess.run(
        [_openssl(), "pkey", "-in", str(private_key), "-pubout"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return private_key, public_key


def _daily_signature(private_key: Path, payload: bytes) -> str:
    payload_path = private_key.parent / "daily-keyring-manifest.hash"
    payload_path.write_bytes(payload)
    payload_path.chmod(0o600)
    result = subprocess.run(
        [
            _openssl(),
            "pkeyutl",
            "-sign",
            "-rawin",
            "-inkey",
            str(private_key),
            "-in",
            str(payload_path),
        ],
        check=True,
        capture_output=True,
    )
    return base64.b64encode(result.stdout).decode("ascii")


def _daily_keyring_document(
    private_key: Path,
    *,
    active_key_id: str,
    active_public_key: str,
    previous_public_keys: dict[str, str] | None = None,
    generation: int = 1,
    previous_manifest_hash: str = "0" * 64,
) -> dict[str, object]:
    body = {
        "schema_version": 2,
        "generation": generation,
        "previous_manifest_hash": previous_manifest_hash,
        "active_key_id": active_key_id,
        "active_public_key": active_public_key,
        "previous_public_keys": previous_public_keys or {},
    }
    manifest_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    return {
        **body,
        "manifest_hash": manifest_hash,
        "signature": _daily_signature(private_key, manifest_hash.encode("ascii")),
    }


def _stat_result_with(
    observed: os.stat_result,
    *,
    uid: int | None = None,
    mode: int | None = None,
) -> os.stat_result:
    values = list(tuple(observed))
    if uid is not None:
        values[stat.ST_UID] = uid
    if mode is not None:
        values[stat.ST_MODE] = (observed.st_mode & ~0o777) | mode
    return os.stat_result(values)


def _mark_daily_keyring_root_owned(
    monkeypatch: pytest.MonkeyPatch,
    keyring: Path,
    *,
    file_root_owned: bool = True,
) -> None:
    keyring_path = Path(os.path.abspath(keyring))
    root_owned_parents = {Path(os.path.abspath(parent)) for parent in keyring_path.parents}
    real_stat = production_profile_module.os.stat
    real_fstat = production_profile_module.os.fstat
    keyring_stat = real_stat(keyring_path, follow_symlinks=False)
    keyring_identity = (keyring_stat.st_dev, keyring_stat.st_ino)

    def fake_stat(
        target: object,
        *args: object,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        observed = real_stat(
            target,
            *args,
            dir_fd=dir_fd,
            follow_symlinks=follow_symlinks,
        )
        if dir_fd is not None:
            return observed
        try:
            candidate = Path(os.path.abspath(Path(target)))
        except TypeError:
            return observed
        if candidate in root_owned_parents:
            return _stat_result_with(observed, uid=0, mode=0o755)
        if file_root_owned and candidate == keyring_path:
            return _stat_result_with(observed, uid=0)
        return observed

    def fake_fstat(descriptor: int) -> os.stat_result:
        observed = real_fstat(descriptor)
        if file_root_owned and (observed.st_dev, observed.st_ino) == keyring_identity:
            return _stat_result_with(observed, uid=0)
        return observed

    monkeypatch.setattr(production_profile_module.os, "stat", fake_stat)
    monkeypatch.setattr(production_profile_module.os, "fstat", fake_fstat)


def _publish_profile(profile: RuntimeDeploymentProfile, output: Path) -> Path:
    assert profile.production_runtime_root is not None
    return publish_production_runtime_profile(
        profile,
        output,
        production_runtime_root=Path(profile.production_runtime_root),
    )


def _relocate_runtime_value(value: object, *, source: Path, target: Path) -> object:
    if isinstance(value, Path):
        relocated = _relocate_runtime_value(str(value), source=source, target=target)
        return Path(relocated) if isinstance(relocated, str) else relocated
    if isinstance(value, str) and (value == str(source) or value.startswith(f"{source}/")):
        return str(target) + value[len(str(source)) :]
    if isinstance(value, dict):
        return {
            key: _relocate_runtime_value(item, source=source, target=target)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_relocate_runtime_value(item, source=source, target=target) for item in value]
    if isinstance(value, tuple):
        return tuple(_relocate_runtime_value(item, source=source, target=target) for item in value)
    return value


def _retarget_schema_policy_consumer(
    payload: dict[str, object],
    *,
    old_id: str,
    new_id: str,
) -> None:
    policies = payload["schema_rollout_policies"]
    assert isinstance(policies, (list, tuple))
    for policy in policies:
        assert isinstance(policy, dict)
        consumers = tuple(policy["required_consumers"])
        policy["required_consumers"] = tuple(
            sorted(new_id if consumer == old_id else consumer for consumer in consumers)
        )


def test_production_profile_declares_the_complete_isolated_runtime(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile, by_kind = _by_kind(inputs)

    assert profile.production_runtime_root == str(inputs.runtime_root)
    assert profile.lab_highwater is not None
    assert profile.lab_highwater.app_env == "prod"
    assert profile.lab_highwater.disable_dotenv is True
    assert profile.lab_highwater.production_mode is True
    assert profile.lab_highwater.authority_command == PRODUCTION_LAB_HIGHWATER_COMMAND
    assert profile.lab_highwater.trusted_keyring_path == (inputs.lab_highwater_trusted_keyring_path)
    assert profile.page_control is not None
    assert profile.page_control.endpoint == "http://127.0.0.1:8767/v1/commands"
    assert profile.page_control.outbox_path == (
        inputs.runtime_root / "control" / "page-control.sqlite3"
    )
    assert profile.page_control.data_dir == inputs.runtime_root / "serving" / "page-control"
    assert profile.page_control.log_dir == inputs.runtime_root / "control" / "page-control-logs"
    assert profile.page_control.page_projection_canvas_catalog_root == (
        inputs.runtime_root / "serving" / "page-control" / "canvases"
    )
    assert profile.schema_rollout_policies
    assert {policy.channel_id for policy in profile.schema_rollout_policies} >= {
        "runtime.market_minute.batch-envelope",
        "runtime.serving.signals",
        "runtime.serving.paper-accounts",
        "runtime.serving.runtime-health",
        "runtime.serving.lab-jobs",
        "runtime.serving.promotions",
    }
    assert {policy.state_path for policy in profile.schema_rollout_policies} == {
        inputs.runtime_root / "control" / "schema-rollouts"
    }
    signals_policy = next(
        policy
        for policy in profile.schema_rollout_policies
        if policy.channel_id == "runtime.serving.signals"
    )
    assert signals_policy.required_consumers == ("serving.publisher.v1",)
    assert len(profile.manifests) == 26
    assert Counter(manifest.service_kind for manifest in profile.manifests) == {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE: 1,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: 1,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: 1,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: 1,
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: 1,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: 1,
        RuntimeServiceKind.DAILY_CLOSE_SOURCE: 1,
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: 1,
        RuntimeServiceKind.SHADOW_SESSION: 1,
        RuntimeServiceKind.CANDIDATE_PUBLISHER: 3,
        RuntimeServiceKind.FEATURE_LIVE: 1,
        RuntimeServiceKind.STRATEGY_LIVE: 3,
        RuntimeServiceKind.SIGNAL_ROUTER: 1,
        RuntimeServiceKind.NOTIFIER: 1,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: 1,
        RuntimeServiceKind.PAPER_BROKER: 1,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: 1,
        RuntimeServiceKind.LAB_JOBS_PUBLISHER: 1,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG: 1,
        RuntimeServiceKind.ARTIFACT_RETENTION: 1,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER: 1,
        RuntimeServiceKind.SERVING_PUBLISHER: 1,
    }
    assert set(profile.capability_environment) == {
        manifest.service_id for manifest in profile.manifests
    }
    assert profile.capability_environment[
        by_kind[RuntimeServiceKind.REFERENCE_SLOW_SOURCE][0].service_id
    ] == (
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64",
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
        "TUSHARE_TOKEN_MAIN",
    )
    assert profile.capability_environment[
        by_kind[RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER][0].service_id
    ] == (
        "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX",
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
    )
    assert profile.capability_environment[
        by_kind[RuntimeServiceKind.AUCTION_MATCH_SOURCE][0].service_id
    ] == ("TUSHARE_TOKEN_MAIN",)
    assert profile.capability_environment[
        by_kind[RuntimeServiceKind.MARKET_MINUTE_SOURCE][0].service_id
    ] == ("TUSHARE_TOKEN_MAIN",)
    assert (
        profile.capability_environment[
            by_kind[RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE][0].service_id
        ]
        == ()
    )
    assert profile.capability_environment[
        by_kind[RuntimeServiceKind.DAILY_CLOSE_SOURCE][0].service_id
    ] == ("TUSHARE_TOKEN_MAIN",)
    assert profile.capability_environment[by_kind[RuntimeServiceKind.NOTIFIER][0].service_id] == (
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
    )
    notifier_settings = NotifierSettings.model_validate(
        dict(by_kind[RuntimeServiceKind.NOTIFIER][0].settings)
    )
    assert notifier_settings.page_projection_surge_live_root == (
        inputs.operational_database_path.parent / "surge_live"
    )
    assert profile.capability_environment[
        by_kind[RuntimeServiceKind.ARTIFACT_RETENTION][0].service_id
    ] == ("RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL",)
    assert all(
        not profile.capability_environment[manifest.service_id]
        for manifest in profile.manifests
        if manifest.service_kind
        not in {
            RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
            RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
            RuntimeServiceKind.AUCTION_MATCH_SOURCE,
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            RuntimeServiceKind.DAILY_CLOSE_SOURCE,
            RuntimeServiceKind.NOTIFIER,
            RuntimeServiceKind.ARTIFACT_RETENTION,
        }
    )


def test_production_profile_hash_binds_current_recovery_configuration(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    assert profile.recovery == inputs.recovery
    assert profile.recovery is not None
    assert profile.recovery.profile_generation is not None
    preview = preview_runtime_deployment_profile(
        profile,
        runtime_root=inputs.runtime_root,
        environ={
            "TUSHARE_TOKEN_MAIN": "tushare-secret-token",
            "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-key",
            "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": "cHJpdmF0ZS1rZXk=",
            "RQ_REFERENCE_SOURCE_PUBLIC_KEY": "reference-public-key",
            "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-key",
            "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
            "PUSHDEER_KEYS": "pushdeer-secret",
            "PUSHPLUS_TOKENS": "pushplus-secret",
            "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _retention_writer_capability(),
        },
        schema_bootstrap_reason="first audited production profile",
    )
    assert preview.recovery_profile_generation == profile.recovery.profile_generation

    changed_recovery_payload = inputs.recovery.model_dump(mode="python")
    changed_recovery_payload.pop("profile_generation")
    changed_recovery_payload["max_rpo_seconds"] = inputs.recovery.max_rpo_seconds + 1
    changed_recovery = RuntimeRecoveryProductionConfig.model_validate(changed_recovery_payload)
    changed_payload = inputs.model_dump(mode="python")
    changed_payload["recovery"] = changed_recovery
    changed = ProductionRuntimeProfileInputs.model_validate(changed_payload)
    changed_profile = build_production_runtime_profile(changed)

    assert changed.recovery.profile_generation != inputs.recovery.profile_generation
    assert changed_profile.profile_id != profile.profile_id


def test_recovery_profile_uses_canonical_artifact_graph_validator(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.runtime_deployment_profile as deployment_profile_module
    import rquant.runtime_recovery_artifacts as recovery_artifacts_module

    recovery = _inputs(tmp_path).recovery
    assert recovery is not None
    observed: list[tuple[str, str, tuple[str, ...]]] = []
    canonical_validator = recovery_artifacts_module.validate_complete_recovery_artifact_graph

    def validate(
        artifacts: object,
        *,
        production_artifact_role: str,
        paper_ledger_artifact_role: str,
    ) -> None:
        roles = tuple(item.logical_role for item in artifacts)  # type: ignore[union-attr]
        observed.append((production_artifact_role, paper_ledger_artifact_role, roles))
        canonical_validator(
            artifacts,  # type: ignore[arg-type]
            production_artifact_role=production_artifact_role,
            paper_ledger_artifact_role=paper_ledger_artifact_role,
        )

    monkeypatch.setattr(
        deployment_profile_module,
        "validate_complete_recovery_artifact_graph",
        validate,
    )
    payload = recovery.model_dump(mode="python", exclude={"profile_generation"})

    rebuilt = RuntimeRecoveryProductionConfig.model_validate(payload)

    assert rebuilt.profile_generation is not None
    assert observed == [
        (
            recovery.production_artifact_role,
            recovery.paper_ledger_artifact_role,
            tuple(item.logical_role for item in recovery.artifact_roles),
        )
    ]
    assert (
        deployment_profile_module.REQUIRED_RECOVERY_ARTIFACT_KINDS
        is recovery_artifacts_module.REQUIRED_RECOVERY_ARTIFACT_KINDS
    )


def test_production_profile_rejects_missing_or_untrusted_recovery_configuration(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    payload = inputs.model_dump(mode="python")
    payload.pop("recovery")

    with pytest.raises(ValidationError):
        ProductionRuntimeProfileInputs.model_validate(payload)

    assert inputs.recovery is not None
    forged_payload = inputs.recovery.model_dump(mode="python")
    forged_payload.pop("profile_generation")
    forged_payload["artifact_roles"] = tuple(
        role.model_copy(update={"source_path": "forged.sqlite3"})
        if role.logical_role == "paper_ledger"
        else role
        for role in inputs.recovery.artifact_roles
    )
    forged = RuntimeRecoveryProductionConfig.model_validate(forged_payload)
    with pytest.raises(ValueError, match="paper ledger"):
        build_production_runtime_profile(inputs.model_copy(update={"recovery": forged}))


@pytest.mark.parametrize(
    ("field", "invalid_path"),
    (
        ("backup_source_root", "other-source"),
        ("backup_publication_root", "recovery-backups"),
        ("isolated_restore_root", "recovery-restore"),
        ("backup_config_path", "outside-backup-config.json"),
        ("credential_file", "outside-recovery-credential.json"),
    ),
)
def test_recovery_profile_rejects_paths_outside_the_systemd_sandbox(
    tmp_path: Path,
    field: str,
    invalid_path: str,
) -> None:
    inputs = _inputs(tmp_path)
    recovery = inputs.recovery
    assert recovery is not None
    runtime_root = Path(inputs.runtime_root)
    recovery.validate_trusted_runtime_root(runtime_root)
    invalid = recovery.model_copy(update={field: tmp_path / invalid_path})

    with pytest.raises(ValueError, match="recovery systemd sandbox"):
        invalid.validate_trusted_runtime_root(runtime_root)


def test_production_recovery_config_drives_preflight_backup_and_rejects_old_generation(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    recovery = profile.recovery
    assert recovery is not None and recovery.profile_generation is not None
    artifacts = tuple(
        RealRecoveryArtifactSpec(
            logical_role=role.logical_role,
            kind=role.kind,
            source_path=role.source_path,
            restore_path=role.restore_path,
            generation_id=f"{role.logical_role}-generation-1",
            schema_version=role.schema_version,
            relations=role.relations,
            references=role.references,
        )
        for role in recovery.artifact_roles
    )
    backup = RecoveryBackupConfig(
        source_root=recovery.backup_source_root,
        publication_root=recovery.backup_publication_root,
        target_commit=profile.producer_commit,
        target_profile_generation=recovery.profile_generation,
        verifier_commit=profile.producer_commit,
        signer_key_id=recovery.signer_key_id,
        as_of=datetime(2026, 8, 2, 8, 0, tzinfo=UTC),
        replay_start_date=date(2026, 7, 1),
        replay_end_date=date(2026, 7, 31),
        production_artifact_role=recovery.production_artifact_role,
        paper_ledger_artifact_role=recovery.paper_ledger_artifact_role,
        strategy_bindings=inputs.strategies,
        artifacts=artifacts,
        deadline_seconds=recovery.recovery_deadline_seconds,
    )

    validate_runtime_recovery_backup_config(profile, backup)
    preflight = build_runtime_recovery_preflight_config(profile)
    assert preflight.expected_profile_generation == recovery.profile_generation
    assert preflight.expected_manifest_id is None
    assert dict(recovery.backup_environment()) == {
        "RQUANT_RECOVERY_BACKUP_ENABLED": "true",
        "RQUANT_RECOVERY_BACKUP_CONFIG": str(recovery.backup_config_path),
        "RQUANT_RECOVERY_CREDENTIAL_FILE": str(recovery.credential_file),
        "RQUANT_RECOVERY_PROFILE_GENERATION": recovery.profile_generation,
        "RQUANT_RECOVERY_SIGNER_KEY_ID": recovery.signer_key_id,
    }

    stale_payload = backup.model_dump(mode="python")
    stale_payload.pop("config_id")
    stale_payload["target_profile_generation"] = "f" * 64
    stale = RecoveryBackupConfig.model_validate(stale_payload)
    with pytest.raises(ValueError, match="differs"):
        validate_runtime_recovery_backup_config(profile, stale)


def test_production_profile_identity_binds_schema_rollout_policy(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    baseline = build_production_runtime_profile(inputs)
    assert baseline.runtime_mode == "local-test"
    changed = build_production_runtime_profile(
        inputs.model_copy(
            update={
                "schema_rollout_stage_timeout_seconds": 601,
                "schema_retire_observation_seconds": 90_000,
            }
        )
    )

    assert baseline.profile_id != changed.profile_id
    assert {policy.stage_timeout_seconds for policy in changed.schema_rollout_policies} == {601}
    assert {policy.retire_observation_seconds for policy in changed.schema_rollout_policies} == {
        90_000
    }


def test_production_profile_settings_are_accepted_by_every_real_builder(
    tmp_path: Path,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    models = {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE: ReferenceSlowSourceSettings,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: ReferenceSlowPublisherSettings,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: AuctionUniversePublisherSettings,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: AuctionMatchSourceSettings,
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: MarketMinuteSourceSettings,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: WatchlistQuoteSourceSettings,
        RuntimeServiceKind.DAILY_CLOSE_SOURCE: DailyCloseSourceSettings,
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: DailyPipelineOrchestratorSettings,
        RuntimeServiceKind.SHADOW_SESSION: ShadowSessionSettings,
        RuntimeServiceKind.CANDIDATE_PUBLISHER: CandidatePublisherRuntimeSettings,
        RuntimeServiceKind.FEATURE_LIVE: FeatureLiveRuntimeSettings,
        RuntimeServiceKind.STRATEGY_LIVE: StrategyLiveRuntimeSettings,
        RuntimeServiceKind.SIGNAL_ROUTER: SignalRouterSettings,
        RuntimeServiceKind.NOTIFIER: NotifierSettings,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: PaperConstraintRuntimeSettings,
        RuntimeServiceKind.PAPER_BROKER: PaperBrokerSettings,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: RuntimeHealthPublisherSettings,
        RuntimeServiceKind.LAB_JOBS_PUBLISHER: LabJobsPublisherSettings,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG: ArtifactCatalogSettings,
        RuntimeServiceKind.ARTIFACT_RETENTION: ArtifactRetentionSettings,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER: PromotionsPublisherSettings,
        RuntimeServiceKind.SERVING_PUBLISHER: ServingRuntimeSettings,
    }

    for manifest in profile.manifests:
        models[manifest.service_kind].model_validate(dict(manifest.settings))


def test_shadow_profile_keeps_calendar_commit_independent_from_export_manifest_commit(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path).model_copy(update={"market_calendar_producer_commit": "b" * 40})
    profile = build_production_runtime_profile(inputs)
    manifest = next(
        item for item in profile.manifests if item.service_kind is RuntimeServiceKind.SHADOW_SESSION
    )
    settings = ShadowSessionSettings.model_validate(dict(manifest.settings))

    assert settings.calendar_expected_commit == "b" * 40
    assert manifest.producer_commit == inputs.producer_commit
    assert manifest.producer_commit != settings.calendar_expected_commit


def test_shadow_profile_binds_the_exact_two_strategy_live_manifests(
    tmp_path: Path,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    shadow_manifest = next(
        item for item in profile.manifests if item.service_kind is RuntimeServiceKind.SHADOW_SESSION
    )
    settings = ShadowSessionSettings.model_validate(dict(shadow_manifest.settings))
    runners = {
        manifest.settings["strategy_id"]: manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE
        and manifest.settings["strategy_id"] in {"n_shape", "growth_board_surge"}
    }

    assert len(settings.runner_manifest_bindings) == 2
    assert {binding.strategy_id for binding in settings.runner_manifest_bindings} == set(runners)
    for binding in settings.runner_manifest_bindings:
        runner = runners[binding.strategy_id]
        expected = LegacyShadowRunnerManifestBinding.create(
            strategy_id=binding.strategy_id,
            strategy_version=runner.settings["strategy_version"],
            producer_manifest_fingerprint=runner.manifest_fingerprint,
            producer_commit=runner.producer_commit,
            producer_service_id=runner.service_id,
            producer_instance_id=runner.settings["producer_instance_id"],
            producer_version=runner.settings["producer_version"],
            strategy_registration_fingerprint=(
                runner.settings["strategy_registration_fingerprint"]
            ),
            strategy_spec_fingerprint=runner.settings["strategy_spec_fingerprint"],
            evaluator_contract_fingerprint=(runner.settings["evaluator_contract_fingerprint"]),
            executable_fingerprint=runner.settings["strategy_executable_fingerprint"],
        )
        assert binding == expected

    assert len({binding.producer_version for binding in settings.runner_manifest_bindings}) == 2
    assert shadow_manifest.settings["producer_version"] not in {
        binding.producer_version for binding in settings.runner_manifest_bindings
    }


def test_every_owned_production_manifest_builds_through_the_builtin_registry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CredentialRecovery:
        outcome = "none"
        transaction_id = None

    class CredentialTransaction:
        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            pass

    class AuctionAdapter:
        def stk_auction(self, _trade_date: date) -> object:
            raise AssertionError("builder smoke must not fetch auction data")

    class MinuteAdapter:
        def rt_min(self, _codes: list[str], _freq: str = "1min") -> object:
            raise AssertionError("builder smoke must not fetch minute data")

    class ReferenceAdapter:
        def bind_transport_observer(self, _observer: object) -> None:
            pass

    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        open_dates=(date(2026, 8, 3),),
        generated_at=datetime(2025, 12, 31, 8, tzinfo=UTC),
    )
    inputs = _inputs(tmp_path)
    inputs.historical_minutes_snapshot_path.parent.mkdir(parents=True)
    pd.DataFrame(
        columns=(
            "ts_code",
            "trade_time",
            "available_at",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "amount",
        )
    ).to_parquet(inputs.historical_minutes_snapshot_path, index=False)
    snapshot_id = hashlib.sha256(inputs.historical_minutes_snapshot_path.read_bytes()).hexdigest()
    inputs = inputs.model_copy(
        update={
            "historical_minutes_snapshot_id": snapshot_id,
            "market_calendar_content_sha256": authority.content_sha256,
        }
    )
    inputs.market_calendar_authority_path.parent.mkdir(parents=True)
    inputs.market_calendar_authority_path.write_text(
        authority.model_dump_json(),
        encoding="utf-8",
    )
    inputs.market_calendar_authority_path.chmod(0o600)
    install_production_runtime_prerequisites(inputs)
    profile = build_production_runtime_profile(inputs)
    private_key = tmp_path / "reference-source-ed25519"
    subprocess.run(
        ("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private_key)),
        check=True,
    )
    capabilities = {
        "TUSHARE_TOKEN_MAIN": "test-token",
        "PUSHDEER_KEYS": "pushdeer-test",
        "PUSHPLUS_TOKENS": "pushplus-test",
        "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": base64.b64encode(private_key.read_bytes()).decode(
            "ascii"
        ),
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY": private_key.with_suffix(".pub")
        .read_text(encoding="ascii")
        .strip(),
        "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _retention_writer_capability(),
    }
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._recover_runtime_credentials",
        lambda **_kwargs: CredentialRecovery(),
    )
    monkeypatch.setattr(
        "rquant.runtime_deployment_bundle._seal_runtime_credentials",
        lambda _credentials: CredentialTransaction(),
    )
    install_runtime_deployment_profile(
        profile,
        runtime_root=inputs.runtime_root,
        environ=capabilities,
        schema_bootstrap_reason="production builder smoke",
    )
    SignalRouteSpool(inputs.runtime_root / "live" / "signal-bus" / "spool")
    registry = build_builtin_registry(
        runtime_capabilities=capabilities,
        reference_adapter_factory=ReferenceAdapter,  # type: ignore[arg-type]
        auction_adapter_factory=AuctionAdapter,  # type: ignore[arg-type]
        adapter_factory=MinuteAdapter,  # type: ignore[arg-type]
        watchlist_quote_provider_factory=lambda: lambda *_args, **_kwargs: pd.DataFrame(),
        clock=lambda: datetime(2026, 8, 3, tzinfo=UTC),
    )
    startup_order = {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE: 0,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: 0,
        RuntimeServiceKind.CANDIDATE_PUBLISHER: 0,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: 1,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: 1,
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: 1,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: 1,
        RuntimeServiceKind.DAILY_CLOSE_SOURCE: 1,
        RuntimeServiceKind.FEATURE_LIVE: 2,
        RuntimeServiceKind.NOTIFIER: 3,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: 3,
        RuntimeServiceKind.SERVING_PUBLISHER: 4,
    }

    built = {}
    for manifest in sorted(
        (
            item
            for item in profile.manifests
            if item.service_kind not in EXTERNAL_OWNER_BUILDER_SMOKE_DEFERRALS
        ),
        key=lambda item: (startup_order[item.service_kind], item.service_id),
    ):
        built[manifest.service_id] = registry.build(manifest)

    assert set(built) == {
        manifest.service_id
        for manifest in profile.manifests
        if manifest.service_kind not in EXTERNAL_OWNER_BUILDER_SMOKE_DEFERRALS
    }
    assert all(callable(step) for step in built.values())
    assert RuntimeServiceKind.PAPER_CONSUMER not in {
        manifest.service_kind for manifest in profile.manifests
    }


def test_external_owner_builder_smoke_deferrals_are_explicit_and_bounded(
    tmp_path: Path,
) -> None:
    profile_kinds = {
        manifest.service_kind
        for manifest in build_production_runtime_profile(_inputs(tmp_path)).manifests
    }

    assert set(EXTERNAL_OWNER_BUILDER_SMOKE_DEFERRALS) < profile_kinds
    assert all(reason.strip() for reason in EXTERNAL_OWNER_BUILDER_SMOKE_DEFERRALS.values())


@pytest.mark.parametrize(
    ("kind", "model", "field"),
    (
        (
            RuntimeServiceKind.CANDIDATE_PUBLISHER,
            CandidatePublisherRuntimeSettings,
            "definition_fingerprint",
        ),
        (
            RuntimeServiceKind.CANDIDATE_PUBLISHER,
            CandidatePublisherRuntimeSettings,
            "executable_fingerprint",
        ),
        (
            RuntimeServiceKind.CANDIDATE_PUBLISHER,
            CandidatePublisherRuntimeSettings,
            "candidate_schema_fingerprint",
        ),
        (
            RuntimeServiceKind.STRATEGY_LIVE,
            StrategyLiveRuntimeSettings,
            "strategy_registration_fingerprint",
        ),
        (
            RuntimeServiceKind.STRATEGY_LIVE,
            StrategyLiveRuntimeSettings,
            "strategy_executable_fingerprint",
        ),
        (
            RuntimeServiceKind.STRATEGY_LIVE,
            StrategyLiveRuntimeSettings,
            "candidate_schema_fingerprint",
        ),
    ),
)
def test_production_strategy_authorities_require_every_exact_fingerprint(
    tmp_path: Path,
    kind: RuntimeServiceKind,
    model: type[CandidatePublisherRuntimeSettings] | type[StrategyLiveRuntimeSettings],
    field: str,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    manifest = next(item for item in profile.manifests if item.service_kind is kind)
    settings = dict(manifest.settings)
    settings.pop(field, None)

    with pytest.raises(ValidationError, match=field):
        model.model_validate(settings)


def test_reference_slow_source_profile_declares_exact_capture_budgets(
    tmp_path: Path,
) -> None:
    _profile, by_kind = _by_kind(_inputs(tmp_path))
    manifest = by_kind[RuntimeServiceKind.REFERENCE_SLOW_SOURCE][0]

    assert manifest.settings["limits"] == {
        "snapshot_max_bytes": 8 * 1024**3,
        "snapshot_min_free_bytes": 2 * 1024**3,
        "snapshot_copy_timeout_seconds": 45.0,
        "query_chunk_rows": 512,
        "max_response_rows": 10_000,
        "max_response_bytes": 8 * 1024**2,
    }
    assert manifest.settings["history_page_size"] == 64
    assert manifest.settings["retention_hot_batches"] == 128
    assert manifest.settings["retention_page_size"] == 32
    assert manifest.settings["retention_consumer_id"] == "reference-slow-publisher"
    assert manifest.settings["pending_recovery_min_age_seconds"] == 60
    assert manifest.settings["quota_accounting_mode"] == "transport"
    assert manifest.settings["quota_cost_per_capture"] is None

    publisher = by_kind[RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER][0]
    assert manifest.settings["consumer_cursor_root"] == publisher.settings["cursor_root"]
    assert publisher.settings["page_size"] == 16


def test_production_profile_passes_the_real_bundle_preview_without_writes(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    preview = preview_runtime_deployment_profile(
        profile,
        runtime_root=inputs.runtime_root,
        environ={
            "TUSHARE_TOKEN_MAIN": "tushare-secret",
            "PUSHDEER_KEYS": "pushdeer-secret",
            "PUSHPLUS_TOKENS": "pushplus-secret",
            "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _retention_writer_capability(),
            "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
            "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
            "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
            "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": "cHJpdmF0ZS1rZXk=",
            "RQ_REFERENCE_SOURCE_PUBLIC_KEY": "ssh-ed25519 AAAAtest reference-source",
        },
        schema_bootstrap_reason="production profile contract test",
    )

    assert preview.profile_id == profile.profile_id
    assert preview.service_ids == tuple(
        sorted(manifest.service_id for manifest in profile.manifests)
    )
    assert len(preview.service_ids) == 26
    assert not inputs.runtime_root.exists()


def test_production_profile_preview_binds_three_strategy_completion_authorities(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    runners = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE
    )

    assert len(runners) == 3
    assert len({runner.settings["producer_instance_id"] for runner in runners}) == 3
    for runner in runners:
        settings = runner.settings
        assert all(field in settings for field in STRATEGY_COMPLETION_FIELDS)
        assert settings["calendar_path"] == str(
            market_calendar_generation_path(
                inputs.runtime_root,
                inputs.market_calendar_content_sha256,
            )
        )
        assert settings["calendar_expected_commit"] == inputs.market_calendar_producer_commit
        assert settings["calendar_content_sha256"] == inputs.market_calendar_content_sha256
        assert settings["signal_bus_path"] == str(
            inputs.runtime_root / "live" / "signal-bus" / "signal_bus.sqlite3"
        )
        assert settings["routing_policy_fingerprint"] == inputs.routing_policy_fingerprint
        expected_instance = "svc-" + hashlib.sha256(runner.service_id.encode()).hexdigest()
        assert settings["producer_instance_id"] == expected_instance
        assert settings["producer_version"] == (
            f"strategy-live/{runner.service_id}/"
            f"strategy-v{settings['strategy_version']}/commit-{COMMIT}"
        )

    preview = preview_runtime_deployment_profile(
        profile,
        runtime_root=inputs.runtime_root,
        environ={
            "TUSHARE_TOKEN_MAIN": "tushare-secret",
            "PUSHDEER_KEYS": "pushdeer-secret",
            "PUSHPLUS_TOKENS": "pushplus-secret",
            "RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL": _retention_writer_capability(),
            "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID": "reference-publication-v1",
            "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX": "ab" * 32,
            "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID": "reference-source-v1",
            "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64": "cHJpdmF0ZS1rZXk=",
            "RQ_REFERENCE_SOURCE_PUBLIC_KEY": "ssh-ed25519 AAAAtest reference-source",
        },
        schema_bootstrap_reason="strategy completion authority preview",
    )

    assert set(preview.strategy_completion_manifest_fingerprints) == {
        runner.service_id for runner in runners
    }
    assert preview.strategy_completion_manifest_fingerprints == {
        runner.service_id: runner.manifest_fingerprint for runner in runners
    }


@pytest.mark.parametrize("field", STRATEGY_COMPLETION_FIELDS)
def test_production_profile_preview_rejects_incomplete_strategy_completion_authority(
    tmp_path: Path,
    field: str,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    runner = next(
        manifest
        for manifest in payload["manifests"]
        if manifest["service_kind"] is RuntimeServiceKind.STRATEGY_LIVE
    )
    runner["settings"].pop(field)

    with pytest.raises((ValidationError, ValueError), match=field.replace("_", ".?")):
        forged = RuntimeDeploymentProfile.model_validate(payload)
        preview_runtime_deployment_profile(
            forged,
            runtime_root=inputs.runtime_root,
            environ={service_id: "unused" for service_id in ()},
            schema_bootstrap_reason="must fail before capability resolution",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (("producer_instance_id", "svc-" + "f" * 64), ("producer_version", "mutable-latest")),
)
def test_production_profile_rejects_forged_strategy_release_identity(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    runner = next(
        manifest
        for manifest in payload["manifests"]
        if manifest["service_kind"] is RuntimeServiceKind.STRATEGY_LIVE
    )
    runner["settings"][field] = value

    with pytest.raises(ValidationError, match=field.replace("_", ".?")):
        RuntimeDeploymentProfile.model_validate(payload)


def test_production_profile_binds_three_strategy_authorities_end_to_end(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile, by_kind = _by_kind(inputs)
    root = inputs.runtime_root
    candidates = {
        str(manifest.settings["strategy_id"]): manifest
        for manifest in by_kind[RuntimeServiceKind.CANDIDATE_PUBLISHER]
    }
    runners = {
        str(manifest.settings["strategy_id"]): manifest
        for manifest in by_kind[RuntimeServiceKind.STRATEGY_LIVE]
    }
    router = SignalRouterSettings.model_validate(
        dict(by_kind[RuntimeServiceKind.SIGNAL_ROUTER][0].settings)
    )
    minute = MarketMinuteSourceSettings.model_validate(
        dict(by_kind[RuntimeServiceKind.MARKET_MINUTE_SOURCE][0].settings)
    )
    quote = WatchlistQuoteSourceSettings.model_validate(
        dict(by_kind[RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE][0].settings)
    )
    daily_close = DailyCloseSourceSettings.model_validate(
        dict(by_kind[RuntimeServiceKind.DAILY_CLOSE_SOURCE][0].settings)
    )
    broker = by_kind[RuntimeServiceKind.PAPER_BROKER][0]
    broker_path = Path(str(broker.settings["broker_path"]))

    assert set(candidates) == set(STRATEGY_IDS)
    assert set(runners) == set(STRATEGY_IDS)
    assert {authority.strategy_id for authority in minute.candidate_authorities} == set(
        STRATEGY_IDS
    )
    assert {authority.strategy_id for authority in quote.candidate_authorities} == set(STRATEGY_IDS)
    assert quote.rollout_mode == "candidate"
    assert quote.schema_version == 2
    assert quote.spool_root == root / "live" / "watchlist-quote"
    assert quote.quota_path == quote.spool_root / "quota.sqlite3"
    assert daily_close.quota_path == daily_close.spool_root / "quota.sqlite3"
    assert daily_close.quota_units_per_window == 20
    assert daily_close.quota_accounting_mode == "transport"
    assert daily_close.quota_cost_per_request is None
    assert daily_close.pending_recovery_min_age_seconds == 300
    assert minute.pending_recovery_min_age_seconds == 60
    minute_authorities = {
        authority.strategy_id: authority for authority in minute.candidate_authorities
    }
    assert router.routing_policy_path == inputs.routing_policy_path
    assert router.routing_policy_fingerprint == inputs.routing_policy_fingerprint
    sources = {source.source_id: source for source in router.sources}
    bindings = {item.strategy_id: item for item in inputs.strategies}
    for strategy_id in STRATEGY_IDS:
        candidate_root = Path(str(candidates[strategy_id].settings["snapshot_root"]))
        runner = runners[strategy_id]
        binding = bindings[strategy_id]
        assert Path(str(runner.settings["candidate_snapshot_root"])) == candidate_root
        assert Path(str(runner.settings["paper_broker_path"])) == broker_path
        assert runner.settings["strategy_registration_fingerprint"] == (
            binding.registration_fingerprint
        )
        assert candidates[strategy_id].settings["definition_fingerprint"] == (
            binding.registration_fingerprint
        )
        assert candidates[strategy_id].settings["executable_fingerprint"] == (
            binding.executable_fingerprint
        )
        assert candidates[strategy_id].settings["candidate_schema_fingerprint"] == (
            binding.candidate_schema_fingerprint
        )
        assert runner.settings["strategy_executable_fingerprint"] == (
            binding.executable_fingerprint
        )
        minute_authority = minute_authorities[strategy_id]
        assert minute_authority.definition_fingerprint == binding.registration_fingerprint
        assert minute_authority.executable_fingerprint == binding.executable_fingerprint
        assert minute_authority.candidate_schema_fingerprint == (
            binding.candidate_schema_fingerprint
        )
        source = sources[f"strategy.{strategy_id}.v1"]
        assert source.runner_state_path == Path(str(runner.settings["runner_state_path"]))
        assert source.expected_strategy_spec_fingerprint == (binding.strategy_spec_fingerprint)
        assert source.expected_evaluator_contract_fingerprint == (binding.executable_fingerprint)
        assert candidate_root.is_relative_to(root / "live" / "candidates")

    health = RuntimeHealthPublisherSettings.model_validate(
        dict(by_kind[RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER][0].settings)
    )
    monitored_manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind
        not in {
            RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
            RuntimeServiceKind.SERVING_PUBLISHER,
        }
    )
    assert {source.service_id for source in health.sources} == {
        manifest.service_id for manifest in monitored_manifests
    }
    assert len({source.control_root for source in health.sources}) == len(health.sources)
    assert by_kind[RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE][0].service_id in {
        source.service_id for source in health.sources
    }
    assert (
        sum(
            source.service_id == by_kind[RuntimeServiceKind.ARTIFACT_RETENTION][0].service_id
            for source in health.sources
        )
        == 1
    )

    serving = ServingRuntimeSettings.model_validate(
        dict(by_kind[RuntimeServiceKind.SERVING_PUBLISHER][0].settings)
    )
    assert {source.dataset_id for source in serving.source_authorities} == {
        "signals",
        "paper_accounts",
        "runtime_health",
        "lab_jobs",
        "promotions",
        "reference_slow_authority",
    }

    calendar_path = market_calendar_generation_path(
        inputs.runtime_root,
        inputs.market_calendar_content_sha256,
    )
    calendar_settings = tuple(
        manifest.settings for manifest in profile.manifests if "calendar_path" in manifest.settings
    )
    assert calendar_settings
    assert all(
        Path(str(settings["calendar_path"])) == calendar_path for settings in calendar_settings
    )
    assert all(
        settings["calendar_expected_commit"] == inputs.market_calendar_producer_commit
        for settings in calendar_settings
    )


def test_production_profile_paper_broker_binds_explicit_v3_cost_provenance(
    tmp_path: Path,
) -> None:
    _, by_kind = _by_kind(_inputs(tmp_path))
    manifest = by_kind[RuntimeServiceKind.PAPER_BROKER][0]
    settings = PaperBrokerSettings.model_validate(dict(manifest.settings))
    spec = settings.execution_cost_spec
    policy = settings.cost_policy()

    assert spec.schema_version == 3
    assert spec.is_alignment_eligible
    assert spec.cost_spec_id == policy.cost_spec_id
    assert spec.cost_engine_version is not None
    assert spec.fee_notional_basis == "EXECUTED_NOTIONAL"
    assert spec.assessment_unit == "FILL"
    assert spec.slippage is not None
    assert spec.slippage.owner == "shared_cost_engine"
    assert spec.money is not None
    assert len(spec.instrument_selectors) == 2
    assert len(spec.commission_rules) == len(spec.instrument_selectors)
    assert len(spec.transfer_fee_rules) == len(spec.instrument_selectors)
    assert len(spec.stamp_duty_rules) == len(spec.instrument_selectors)


def test_production_profile_binds_daily_close_to_its_immutable_live_spool(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile, by_kind = _by_kind(inputs)

    daily = by_kind[RuntimeServiceKind.DAILY_CLOSE_SOURCE]
    assert len(daily) == 1
    manifest = daily[0]
    assert manifest.service_id == "daily-close.source.v1"
    assert manifest.plane == "live"
    assert manifest.settings["spool_root"] == str(inputs.runtime_root / "live" / "daily-close")
    assert manifest.settings["calendar_path"] == str(
        market_calendar_generation_path(
            inputs.runtime_root,
            inputs.market_calendar_content_sha256,
        )
    )
    assert profile.capability_environment[manifest.service_id] == ("TUSHARE_TOKEN_MAIN",)


def test_production_profile_binds_daily_orchestrator_to_shadow_fan_in_dag(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile, by_kind = _by_kind(inputs)

    orchestrators = by_kind[RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR]
    assert len(orchestrators) == 1
    manifest = orchestrators[0]
    assert manifest.service_id == "daily.pipeline.orchestrator.shadow.v1"
    assert manifest.plane == "research"
    assert manifest.settings["mode"] == "shadow"
    assert manifest.settings["source_spool_root"] == str(
        inputs.runtime_root / "live" / "daily-close"
    )
    assert manifest.settings["storage_root"] == str(
        inputs.runtime_root / "research" / "daily-pipeline"
    )
    assert manifest.settings["deployment_profile_path"] == str(
        inputs.runtime_root / "current" / "deployment-profile.json"
    )
    assert manifest.settings["stages"] == (
        "raw_capture",
        "validate_candidate",
        "canonical_publish",
        "screen",
        "pool",
        "summary",
        "serving_refresh",
        "replica_sync",
        "research_ingest",
        "backup",
    )


def test_production_profile_binds_shadow_to_public_only_authority(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile, by_kind = _by_kind(inputs)

    shadow = by_kind[RuntimeServiceKind.SHADOW_SESSION]
    assert len(shadow) == 1
    manifest = shadow[0]
    assert manifest.service_id == "shadow.session.production.v1"
    assert manifest.plane == "research"
    assert manifest.settings["mode"] == "shadow"
    assert manifest.settings["report_root"] == str(
        inputs.runtime_root / "research" / "shadow-reports"
    )
    assert manifest.settings["calendar_path"] == str(
        market_calendar_generation_path(
            inputs.runtime_root,
            inputs.market_calendar_content_sha256,
        )
    )
    assert manifest.settings["signer_command"] == (
        "/usr/bin/sudo",
        "-n",
        "/usr/local/libexec/rquant-shadow-report-signer",
    )
    assert profile.capability_environment[manifest.service_id] == ()
    assert profile.shadow is not None
    assert profile.shadow.completion_active_key_id == "shadow-completion-v1"
    assert profile.shadow.completion_previous_public_key_pems == {}
    assert profile.shadow.report_producer_service_id == manifest.service_id
    assert profile.shadow.report_producer_instance_id == "shadow-session-primary"
    assert all(
        profile.capability_environment[item.service_id] == ()
        for item in by_kind[RuntimeServiceKind.STRATEGY_LIVE]
    )


def test_production_profile_installs_its_exact_market_calendar_prerequisite(
    tmp_path: Path,
) -> None:
    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        open_dates=(date(2026, 1, 5),),
        generated_at=datetime(2025, 12, 31, 8, tzinfo=UTC),
    )
    inputs = _inputs(tmp_path).model_copy(
        update={
            "market_calendar_content_sha256": authority.content_sha256,
            "strategies": _builtin_bindings(),
        }
    )
    inputs.market_calendar_authority_path.parent.mkdir(parents=True)
    inputs.market_calendar_authority_path.write_text(
        authority.model_dump_json(),
        encoding="utf-8",
    )
    inputs.market_calendar_authority_path.chmod(0o600)

    installed = install_production_runtime_prerequisites(inputs)

    assert installed == (
        market_calendar_generation_path(inputs.runtime_root, authority.content_sha256),
        inputs.definition_registry_root,
        inputs.runtime_root
        / "research"
        / "artifact-retention"
        / ("svc-" + hashlib.sha256(b"artifact-retention.primary.v1").hexdigest())
        / "catalog-authority"
        / "current.json",
    )
    assert inputs.definition_registry_root.is_dir()
    assert installed[-1].is_file()


def test_production_prerequisites_reject_definition_mismatch_before_writing(
    tmp_path: Path,
) -> None:
    authority = MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 1, 1),
        coverage_end=date(2026, 12, 31),
        open_dates=(date(2026, 1, 5),),
        generated_at=datetime(2025, 12, 31, 8, tzinfo=UTC),
    )
    payload = _inputs(tmp_path).model_dump(mode="python")
    payload["market_calendar_content_sha256"] = authority.content_sha256
    payload["strategies"][0]["registration_fingerprint"] = "0" * 64
    inputs = ProductionRuntimeProfileInputs.model_validate(payload)
    inputs.market_calendar_authority_path.parent.mkdir(parents=True)
    inputs.market_calendar_authority_path.write_text(
        authority.model_dump_json(),
        encoding="utf-8",
    )
    inputs.market_calendar_authority_path.chmod(0o600)

    with pytest.raises(ValueError, match="definition.*binding"):
        install_production_runtime_prerequisites(inputs)

    assert not inputs.runtime_root.exists()
    assert not inputs.definition_registry_root.exists()


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("historical_minutes_snapshot_id", "e" * 64),
        ("market_calendar_content_sha256", "9" * 64),
        ("trade_calendar_sha256", "f" * 64),
        ("routing_policy_fingerprint", "0" * 64),
    ],
)
def test_production_profile_identity_changes_with_external_authority_fingerprints(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    inputs = _inputs(tmp_path)
    baseline = build_production_runtime_profile(inputs)
    changed = ProductionRuntimeProfileInputs.model_validate(
        {**inputs.model_dump(mode="python"), field: replacement}
    )

    assert build_production_runtime_profile(changed).profile_id != baseline.profile_id


def test_local_test_profile_root_evidence_changes_profile_identity(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    assert inputs.runtime_mode == "local-test"
    baseline = build_production_runtime_profile(inputs)
    relocated_root = inputs.runtime_root.parent / "other-runtime"
    recovery_payload = inputs.recovery.model_dump(mode="python")
    recovery_payload.pop("profile_generation")
    recovery_payload["service_state_path"] = (
        relocated_root / "control" / "recovery" / "service.sqlite3"
    )
    recovery_payload["service_receipt_root"] = relocated_root / "control" / "recovery" / "receipts"
    recovery_payload["artifact_roles"] = tuple(
        {
            **role,
            "source_path": (
                "other-runtime/" + role["source_path"].removeprefix("runtime/")
                if role["source_path"].startswith("runtime/")
                else role["source_path"]
            ),
        }
        for role in recovery_payload["artifact_roles"]
    )
    relocated_recovery = RuntimeRecoveryProductionConfig.model_validate(recovery_payload)
    relocated = ProductionRuntimeProfileInputs.model_validate(
        {
            **inputs.model_dump(mode="python"),
            "runtime_root": relocated_root,
            "recovery": relocated_recovery,
        }
    )

    changed = build_production_runtime_profile(relocated)

    assert changed.production_runtime_root == str(relocated.runtime_root)
    assert changed.profile_id != baseline.profile_id


def test_linux_production_profile_rejects_whole_srv_runtime_relocation(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    relocated_root = Path("/srv/rquant/data/runtime")
    relocated = _relocate_runtime_value(
        payload,
        source=inputs.runtime_root,
        target=relocated_root,
    )
    assert isinstance(relocated, dict)
    relocated["runtime_mode"] = "linux-production"
    relocated["production_runtime_root"] = str(relocated_root)
    relocated["recovery"]["backup_source_root"] = str(relocated_root.parent)
    relocated["recovery"].pop("profile_generation")

    with pytest.raises(ValidationError, match="Linux production runtime root"):
        RuntimeDeploymentProfile.model_validate(relocated)


def test_linux_production_inputs_reject_local_parameterized_runtime_root(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    payload = inputs.model_dump(mode="python")
    payload["runtime_mode"] = "linux-production"

    with pytest.raises(ValidationError, match="Linux production runtime root"):
        ProductionRuntimeProfileInputs.model_validate(payload)


def test_linux_production_inputs_accept_only_canonical_host_layout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _inputs(tmp_path)
    payload = _relocate_runtime_value(
        inputs.model_dump(mode="json"),
        source=inputs.runtime_root.parent,
        target=LINUX_PRODUCTION_RUNTIME_ROOT.parent,
    )
    assert isinstance(payload, dict)
    payload["runtime_mode"] = "linux-production"
    payload["recovery"].pop("profile_generation")
    payload["canvas_publication_active_key_id"] = "canvas-v1"
    payload["canvas_publication_active_public_key_pem"] = "active-public-key"
    payload["daily_receipt_active_key_id"] = None
    payload["daily_receipt_active_public_key_pem"] = None
    payload["daily_receipt_previous_public_key_pems"] = {}

    active_private, active_public = _daily_private_key(
        tmp_path / "daily-receipt" / "active",
        key_id="daily-v1",
    )
    keyring = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    keyring.parent.mkdir(parents=True, mode=0o700)
    keyring.write_bytes(
        canonical_json_bytes(
            _daily_keyring_document(
                active_private,
                active_key_id="daily-v1",
                active_public_key=active_public,
            )
        )
    )
    keyring.chmod(0o444)
    _mark_daily_keyring_root_owned(monkeypatch, keyring)
    monkeypatch.setattr(
        production_profile_module,
        "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
        keyring,
    )
    path = tmp_path / "linux-profile-inputs.json"
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)

    production_inputs = load_production_runtime_profile_inputs(
        path,
        expected_commit=COMMIT,
        expected_runtime_mode="linux-production",
    )

    assert production_inputs.runtime_mode == "linux-production"
    assert production_inputs.runtime_root == LINUX_PRODUCTION_RUNTIME_ROOT
    assert production_inputs.recovery.backup_source_root == Path("/home/lighthouse/rquant/data")
    assert production_inputs.recovery.service_state_path == (
        LINUX_PRODUCTION_RUNTIME_ROOT / "control" / "recovery" / "service.sqlite3"
    )
    assert production_inputs.recovery.service_receipt_root == (
        LINUX_PRODUCTION_RUNTIME_ROOT / "control" / "recovery" / "receipts"
    )


def test_linux_production_inputs_load_daily_receipt_authority_from_fixed_keyring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _inputs(tmp_path)
    payload = _relocate_runtime_value(
        inputs.model_dump(mode="json"),
        source=inputs.runtime_root.parent,
        target=LINUX_PRODUCTION_RUNTIME_ROOT.parent,
    )
    assert isinstance(payload, dict)
    payload["runtime_mode"] = "linux-production"
    payload["recovery"].pop("profile_generation")
    payload["canvas_publication_active_key_id"] = "canvas-v1"
    payload["canvas_publication_active_public_key_pem"] = "active-public-key"
    payload["daily_receipt_active_key_id"] = None
    payload["daily_receipt_active_public_key_pem"] = None
    payload["daily_receipt_previous_public_key_pems"] = {}

    keyring = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    keyring.parent.mkdir(parents=True, mode=0o700)
    active_private, active_public = _daily_private_key(
        tmp_path / "daily-receipt" / "active",
        key_id="daily-v2",
    )
    _previous_private, previous_public = _daily_private_key(
        tmp_path / "daily-receipt" / "previous",
        key_id="daily-v1",
    )
    keyring.write_bytes(
        canonical_json_bytes(
            _daily_keyring_document(
                active_private,
                active_key_id="daily-v2",
                active_public_key=active_public,
                previous_public_keys={"daily-v1": previous_public},
                generation=2,
                previous_manifest_hash="1" * 64,
            )
        )
    )
    keyring.chmod(0o444)
    _mark_daily_keyring_root_owned(monkeypatch, keyring)
    monkeypatch.setattr(
        production_profile_module,
        "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
        keyring,
    )
    path = tmp_path / "linux-profile-inputs.json"
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)

    loaded = load_production_runtime_profile_inputs(
        path,
        expected_commit=COMMIT,
        expected_runtime_mode="linux-production",
    )

    assert loaded.daily_receipt_active_key_id == "daily-v2"
    assert loaded.daily_receipt_active_public_key_pem == active_public.strip()
    assert loaded.daily_receipt_previous_public_key_pems == {"daily-v1": previous_public.strip()}


def test_linux_production_inputs_reject_runner_owned_daily_receipt_keyring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _inputs(tmp_path)
    payload = _relocate_runtime_value(
        inputs.model_dump(mode="json"),
        source=inputs.runtime_root.parent,
        target=LINUX_PRODUCTION_RUNTIME_ROOT.parent,
    )
    assert isinstance(payload, dict)
    payload["runtime_mode"] = "linux-production"
    payload["recovery"].pop("profile_generation")
    payload["canvas_publication_active_key_id"] = "canvas-v1"
    payload["canvas_publication_active_public_key_pem"] = "active-public-key"
    payload["daily_receipt_active_key_id"] = None
    payload["daily_receipt_active_public_key_pem"] = None
    payload["daily_receipt_previous_public_key_pems"] = {}

    active_private, active_public = _daily_private_key(
        tmp_path / "daily-receipt" / "active",
        key_id="daily-v1",
    )
    keyring = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    keyring.parent.mkdir(parents=True, mode=0o700)
    keyring.write_bytes(
        canonical_json_bytes(
            _daily_keyring_document(
                active_private,
                active_key_id="daily-v1",
                active_public_key=active_public,
            )
        )
    )
    keyring.chmod(0o444)
    _mark_daily_keyring_root_owned(monkeypatch, keyring, file_root_owned=False)
    monkeypatch.setattr(
        production_profile_module,
        "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
        keyring,
    )
    path = tmp_path / "linux-profile-inputs.json"
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)

    with pytest.raises(ValueError, match="root-owned|unsafe"):
        load_production_runtime_profile_inputs(
            path,
            expected_commit=COMMIT,
            expected_runtime_mode="linux-production",
        )


def test_linux_production_daily_receipt_keyring_rejects_post_open_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _inputs(tmp_path)
    payload = _relocate_runtime_value(
        inputs.model_dump(mode="json"),
        source=inputs.runtime_root.parent,
        target=LINUX_PRODUCTION_RUNTIME_ROOT.parent,
    )
    assert isinstance(payload, dict)
    payload["runtime_mode"] = "linux-production"
    payload["recovery"].pop("profile_generation")
    payload["canvas_publication_active_key_id"] = "canvas-v1"
    payload["canvas_publication_active_public_key_pem"] = "active-public-key"
    payload["daily_receipt_active_key_id"] = None
    payload["daily_receipt_active_public_key_pem"] = None
    payload["daily_receipt_previous_public_key_pems"] = {}

    active_private, active_public = _daily_private_key(
        tmp_path / "daily-receipt" / "active",
        key_id="daily-v1",
    )
    keyring = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    keyring.parent.mkdir(parents=True, mode=0o700)
    keyring.write_bytes(
        canonical_json_bytes(
            _daily_keyring_document(
                active_private,
                active_key_id="daily-v1",
                active_public_key=active_public,
            )
        )
    )
    keyring.chmod(0o444)
    _mark_daily_keyring_root_owned(monkeypatch, keyring)
    monkeypatch.setattr(
        production_profile_module,
        "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
        keyring,
    )
    keyring_identity = (
        keyring.stat(follow_symlinks=False).st_dev,
        keyring.stat(follow_symlinks=False).st_ino,
    )
    real_read = production_profile_module.os.read
    changed = False

    def chmod_after_open(descriptor: int, count: int) -> bytes:
        nonlocal changed
        observed = production_profile_module.os.fstat(descriptor)
        if not changed and (observed.st_dev, observed.st_ino) == keyring_identity:
            keyring.chmod(0o644)
            changed = True
        return real_read(descriptor, count)

    monkeypatch.setattr(production_profile_module.os, "read", chmod_after_open)
    path = tmp_path / "linux-profile-inputs.json"
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)

    with pytest.raises(ValueError, match="changed|unsafe"):
        load_production_runtime_profile_inputs(
            path,
            expected_commit=COMMIT,
            expected_runtime_mode="linux-production",
        )


def test_linux_production_inputs_reject_daily_receipt_keyring_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _inputs(tmp_path)
    payload = _relocate_runtime_value(
        inputs.model_dump(mode="json"),
        source=inputs.runtime_root.parent,
        target=LINUX_PRODUCTION_RUNTIME_ROOT.parent,
    )
    assert isinstance(payload, dict)
    payload["runtime_mode"] = "linux-production"
    payload["recovery"].pop("profile_generation")
    payload["canvas_publication_active_key_id"] = "canvas-v1"
    payload["canvas_publication_active_public_key_pem"] = "active-public-key"
    payload["daily_receipt_active_key_id"] = None
    payload["daily_receipt_active_public_key_pem"] = None
    payload["daily_receipt_previous_public_key_pems"] = {}

    active_private, active_public = _daily_private_key(
        tmp_path / "daily-receipt" / "active",
        key_id="daily-v2",
    )
    keyring = tmp_path / "etc/rquant/daily-receipt-trusted-keys.json"
    keyring.parent.mkdir(parents=True, mode=0o700)
    document = _daily_keyring_document(
        active_private,
        active_key_id="daily-v2",
        active_public_key=active_public,
    )
    document["manifest_hash"] = "f" * 64
    keyring.write_bytes(canonical_json_bytes(document))
    keyring.chmod(0o444)
    _mark_daily_keyring_root_owned(monkeypatch, keyring)
    monkeypatch.setattr(
        production_profile_module,
        "DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
        keyring,
    )
    path = tmp_path / "linux-profile-inputs.json"
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o600)

    with pytest.raises(ValueError, match="manifest hash|signature"):
        load_production_runtime_profile_inputs(
            path,
            expected_commit=COMMIT,
            expected_runtime_mode="linux-production",
        )


def test_production_profile_binds_complete_page_control_canvas_authority(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path).model_copy(
        update={
            "canvas_publication_active_key_id": "canvas-v2",
            "canvas_publication_active_public_key_pem": "active-public-key",
            "canvas_publication_previous_public_key_pems": {"canvas-v1": "previous-public-key"},
        }
    )

    profile = build_production_runtime_profile(inputs)

    assert profile.page_control is not None
    assert profile.page_control.data_dir == inputs.runtime_root / "serving" / "page-control"
    assert profile.page_control.log_dir == inputs.runtime_root / "control" / "page-control-logs"
    authority = profile.page_control.canvas_publication
    assert authority is not None
    assert authority.active_key_id == "canvas-v2"
    assert authority.previous_public_key_pems == {"canvas-v1": "previous-public-key"}
    assert authority.signer_command == (
        "/usr/bin/sudo",
        "-n",
        "/usr/local/libexec/rquant-canvas-publication-signer",
    )
    assert authority.consumer_service_id == "page-control.production.v1"
    assert authority.consumer_instance_id == "page-control-primary"
    notifier = next(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.NOTIFIER
    )
    settings = NotifierSettings.model_validate(dict(notifier.settings))
    authority_profile_root = inputs.runtime_root / "serving" / "page-control" / "canvases"
    assert settings.page_projection_canvas_catalog_root == authority_profile_root
    assert settings.page_projection_canvas_receipt_root == (
        authority_profile_root.parent / "canvas-publication-receipts"
    )
    assert settings.page_projection_page_control_outbox_path == (
        inputs.runtime_root / "control" / "page-control.sqlite3"
    )
    assert settings.page_projection_canvas_active_key_id == authority.active_key_id
    assert settings.page_projection_canvas_previous_public_key_pems == (
        authority.previous_public_key_pems
    )


def test_profile_preview_rejects_caller_runtime_root_mismatch(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)

    with pytest.raises(ValueError, match="production runtime root.*trusted runtime root"):
        preview_runtime_deployment_profile(
            profile,
            runtime_root=tmp_path / "other-runtime",
            environ={},
        )


def test_production_profile_requires_exactly_the_three_builtin_strategies(
    tmp_path: Path,
) -> None:
    payload = _inputs(tmp_path).model_dump(mode="python")
    payload["strategies"] = tuple(payload["strategies"][:2])

    with pytest.raises(ValueError, match="three|strategy"):
        ProductionRuntimeProfileInputs.model_validate(payload)


@pytest.mark.parametrize(
    "field",
    ("strategy_spec_fingerprint", "evaluator_contract_fingerprint"),
)
def test_strategy_completion_identity_rejects_malformed_sha_before_path_resolution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    manifest = next(
        item for item in profile.manifests if item.service_kind is RuntimeServiceKind.STRATEGY_LIVE
    )
    settings = dict(manifest.settings)
    settings[field] = "not-a-sha256"
    forged = manifest.model_copy(update={"settings": settings})

    def forbidden_path_resolution(*_args: object, **_kwargs: object) -> Path:
        raise AssertionError("path/capability resolution ran before structural identity checks")

    monkeypatch.setattr(
        deployment_bundle_module,
        "_expected_market_calendar_generation",
        forbidden_path_resolution,
    )

    with pytest.raises(ValueError, match=field):
        deployment_bundle_module.validate_strategy_live_completion_manifest(
            forged,
            runtime_root=inputs.runtime_root,
        )


@pytest.mark.parametrize(
    "field",
    (
        "registration_fingerprint",
        "strategy_spec_fingerprint",
        "executable_fingerprint",
        "candidate_schema_fingerprint",
    ),
)
def test_production_profile_build_rejects_forged_strategy_binding(
    tmp_path: Path,
    field: str,
) -> None:
    inputs = _inputs(tmp_path).model_copy(update={"strategies": _builtin_bindings()})
    payload = inputs.model_dump(mode="python")
    payload["strategies"][0][field] = "0" * 64
    forged = ProductionRuntimeProfileInputs.model_validate(payload)

    with pytest.raises(ValueError, match="definition.*binding|trusted|built-in"):
        build_production_runtime_profile(forged)


@pytest.mark.parametrize(
    "field",
    (
        "definition_fingerprint",
        "executable_fingerprint",
        "candidate_schema_fingerprint",
    ),
)
def test_production_profile_publish_revalidates_trusted_strategy_binding(
    tmp_path: Path,
    field: str,
) -> None:
    inputs = _inputs(tmp_path).model_copy(update={"strategies": _builtin_bindings()})
    profile = build_production_runtime_profile(inputs)
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    for manifest in payload["manifests"]:
        if manifest["service_kind"] == RuntimeServiceKind.CANDIDATE_PUBLISHER:
            manifest["settings"][field] = "0" * 64
            break
    forged = RuntimeDeploymentProfile.model_validate(payload)
    assert forged.profile_id is not None
    output = tmp_path / "profiles" / f"{forged.profile_id}.json"
    output.parent.mkdir()

    with pytest.raises(ValueError, match="definition.*binding|trusted|built-in"):
        _publish_profile(forged, output)


def test_loads_owned_canonical_production_inputs_bound_to_commit(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    path = tmp_path / "profile-inputs.json"
    path.write_bytes(canonical_model_json_bytes(inputs))
    path.chmod(0o600)

    assert load_production_runtime_profile_inputs(path, expected_commit=COMMIT) == inputs

    with pytest.raises(ValueError, match="commit"):
        load_production_runtime_profile_inputs(path, expected_commit="b" * 40)
    with pytest.raises(ValueError, match="runtime mode"):
        load_production_runtime_profile_inputs(
            path,
            expected_commit=COMMIT,
            expected_runtime_mode="linux-production",
        )


def test_production_inputs_reject_symlink_and_noncanonical_json(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    source = tmp_path / "source.json"
    source.write_bytes(canonical_model_json_bytes(inputs))
    source.chmod(0o600)
    link = tmp_path / "link.json"
    link.symlink_to(source)

    with pytest.raises(ValueError, match="unsafe|unavailable"):
        load_production_runtime_profile_inputs(link, expected_commit=COMMIT)

    source.write_text(inputs.model_dump_json(indent=2))
    with pytest.raises(ValueError, match="canonical"):
        load_production_runtime_profile_inputs(source, expected_commit=COMMIT)


def test_production_inputs_fail_if_opened_parent_is_replaced_by_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _inputs(tmp_path)
    trusted_parent = tmp_path / "trusted-input"
    trusted_parent.mkdir(mode=0o700)
    path = trusted_parent / "profile-inputs.json"
    path.write_bytes(canonical_model_json_bytes(inputs))
    path.chmod(0o600)
    displaced = tmp_path / "displaced-input"
    attacker = tmp_path / "attacker-input"
    attacker.mkdir(mode=0o700)
    real_open = production_profile_module.os.open
    swapped = False

    def replacing_open(
        target: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        descriptor = real_open(target, flags, mode, dir_fd=dir_fd)
        if not swapped and Path(str(target)).name == path.name:
            trusted_parent.rename(displaced)
            trusted_parent.symlink_to(attacker, target_is_directory=True)
            swapped = True
        return descriptor

    monkeypatch.setattr(production_profile_module.os, "open", replacing_open)

    with pytest.raises(ValueError, match="parent|changed|unsafe"):
        load_production_runtime_profile_inputs(path, expected_commit=COMMIT)


def test_profile_publication_fails_if_opened_parent_is_replaced_by_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    assert profile.profile_id is not None
    trusted_parent = tmp_path / "trusted-output"
    trusted_parent.mkdir(mode=0o700)
    output = trusted_parent / f"{profile.profile_id}.json"
    displaced = tmp_path / "displaced-output"
    attacker = tmp_path / "attacker-output"
    attacker.mkdir(mode=0o700)
    real_open = production_profile_module.os.open
    swapped = False

    def replacing_open(
        target: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        descriptor = real_open(target, flags, mode, dir_fd=dir_fd)
        target_name = Path(str(target)).name
        if not swapped and target_name in {trusted_parent.name, f".{profile.profile_id}.stage"}:
            trusted_parent.rename(displaced)
            trusted_parent.symlink_to(attacker, target_is_directory=True)
            swapped = True
        return descriptor

    monkeypatch.setattr(production_profile_module.os, "open", replacing_open)

    with pytest.raises(ValueError, match="parent|changed|unsafe|safely"):
        _publish_profile(profile, output)

    assert not (attacker / output.name).exists()


def test_profile_publication_rejects_stage_inode_swap_before_final_link(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    assert profile.profile_id is not None
    output_dir = tmp_path / "profiles"
    output_dir.mkdir(mode=0o700)
    output = output_dir / f"{profile.profile_id}.json"
    stage = output_dir / f".{profile.profile_id}.stage"
    stage.write_bytes(canonical_model_json_bytes(profile))
    stage.chmod(0o600)
    real_open = production_profile_module.os.open
    swapped = False

    def swapping_open(
        target: object,
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        nonlocal swapped
        if not swapped and target == output.name and flags & production_profile_module.os.O_CREAT:
            stage.unlink()
            stage.write_bytes(b"{}")
            stage.chmod(0o600)
            swapped = True
        return real_open(target, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(production_profile_module.os, "open", swapping_open)

    with pytest.raises(ValueError, match="stage.*changed|publication.*changed|source.*changed"):
        _publish_profile(profile, output)

    assert swapped is True
    assert not output.exists()


def test_profile_publication_recovers_incomplete_private_stage(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()
    stage = output.parent / f".{profile.profile_id}.stage"
    stage.write_bytes(b"{")
    stage.chmod(0o200)

    assert (
        publish_production_runtime_profile(
            profile,
            output,
            production_runtime_root=inputs.runtime_root,
        )
        == output
    )
    assert output.read_bytes() == canonical_model_json_bytes(profile)
    assert output.stat().st_mode & 0o777 == 0o600
    assert not stage.exists()


def test_profile_publication_promotes_private_entries_from_incomplete_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()
    observed_modes: list[int] = []
    real_fchmod = production_profile_module.os.fchmod

    def record_promotion(descriptor: int, mode: int) -> None:
        if mode == 0o600:
            current = production_profile_module.os.fstat(descriptor)
            observed_modes.append(production_profile_module.stat.S_IMODE(current.st_mode))
        real_fchmod(descriptor, mode)

    monkeypatch.setattr(production_profile_module.os, "fchmod", record_promotion)

    assert _publish_profile(profile, output) == output
    assert observed_modes == [0o200, 0o200]
    assert output.stat().st_mode & 0o777 == 0o600


def test_profile_publication_rejects_complete_stage_with_different_content(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()
    stage = output.parent / f".{profile.profile_id}.stage"
    stage.write_bytes(b"{}")
    stage.chmod(0o600)

    with pytest.raises(ValueError, match="stage is invalid"):
        publish_production_runtime_profile(
            profile,
            output,
            production_runtime_root=inputs.runtime_root,
        )

    assert not output.exists()
    assert stage.read_bytes() == b"{}"
    assert stage.stat().st_mode & 0o777 == 0o600


def test_profile_publication_fails_closed_while_another_publisher_holds_directory_lock(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    profile = build_production_runtime_profile(inputs)
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()
    descriptor = production_profile_module.os.open(
        output.parent, production_profile_module._DIRECTORY_FLAGS
    )
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="publication.*progress|concurrent|lock"):
            publish_production_runtime_profile(
                profile,
                output,
                production_runtime_root=inputs.runtime_root,
            )
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        production_profile_module.os.close(descriptor)

    assert not output.exists()


def test_profile_publication_rejects_coherent_strategy_roots_outside_runtime_root(
    tmp_path: Path,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    strategy_id = "n_shape"
    candidate_service_id = f"candidate.{strategy_id}.v1"
    forged_root = (
        tmp_path
        / "other-runtime"
        / "live"
        / "candidates"
        / production_profile_module._instance_name(candidate_service_id)
    ).resolve()
    for manifest in payload["manifests"]:
        if (
            manifest["service_kind"] is RuntimeServiceKind.CANDIDATE_PUBLISHER
            and manifest["settings"]["strategy_id"] == strategy_id
        ):
            manifest["settings"]["snapshot_root"] = str(forged_root)
        elif (
            manifest["service_kind"] is RuntimeServiceKind.STRATEGY_LIVE
            and manifest["settings"]["strategy_id"] == strategy_id
        ):
            manifest["settings"]["candidate_snapshot_root"] = str(forged_root)
        elif manifest["service_kind"] is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
            authority = next(
                item
                for item in manifest["settings"]["candidate_authorities"]
                if item["strategy_id"] == strategy_id
            )
            authority["snapshot_root"] = str(forged_root)
    forged = RuntimeDeploymentProfile.model_validate(payload)
    assert forged.profile_id != profile.profile_id
    output = tmp_path / "profiles" / f"{forged.profile_id}.json"
    output.parent.mkdir()

    with pytest.raises(ValueError, match="runtime root|managed|topology|owned"):
        _publish_profile(forged, output)


def test_profile_publication_rejects_forged_minute_service_id(tmp_path: Path) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    minute = next(
        manifest
        for manifest in payload["manifests"]
        if manifest["service_kind"] is RuntimeServiceKind.MARKET_MINUTE_SOURCE
    )
    old_id = minute["service_id"]
    minute["service_id"] = "forged.market-minute.source.v1"
    payload["capability_environment"][minute["service_id"]] = payload["capability_environment"].pop(
        old_id
    )
    forged = RuntimeDeploymentProfile.model_validate(payload)
    assert forged.profile_id != profile.profile_id
    output = tmp_path / "profiles" / f"{forged.profile_id}.json"
    output.parent.mkdir()

    with pytest.raises(ValueError, match="minute.*service|service.*topology"):
        _publish_profile(forged, output)


@pytest.mark.parametrize(
    "service_kind",
    (
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        RuntimeServiceKind.FEATURE_LIVE,
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        RuntimeServiceKind.PAPER_BROKER,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        RuntimeServiceKind.ARTIFACT_RETENTION,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
        RuntimeServiceKind.SERVING_PUBLISHER,
    ),
)
def test_profile_publication_rejects_forged_singleton_service_ids(
    tmp_path: Path,
    service_kind: RuntimeServiceKind,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    manifest = next(item for item in payload["manifests"] if item["service_kind"] is service_kind)
    old_id = manifest["service_id"]
    manifest["service_id"] = f"forged.{old_id}"
    payload["capability_environment"][manifest["service_id"]] = payload[
        "capability_environment"
    ].pop(old_id)
    _retarget_schema_policy_consumer(
        payload,
        old_id=old_id,
        new_id=manifest["service_id"],
    )
    forged = RuntimeDeploymentProfile.model_validate(payload)
    output = tmp_path / "profiles" / f"{forged.profile_id}.json"
    output.parent.mkdir()

    with pytest.raises(
        ValueError,
        match="service.*topology|systemd.*path|instance root|owner service|exclusive",
    ):
        _publish_profile(forged, output)


@pytest.mark.parametrize(
    "target",
    (
        "candidate_root",
        "candidate_service_id",
        "minute_root",
        "runner_root",
        "runner_service_id",
    ),
)
def test_profile_publication_rejects_recomputed_id_with_forged_strategy_topology(
    tmp_path: Path,
    target: str,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    payload = profile.model_dump(mode="python")
    payload.pop("profile_id")
    strategy_id = "n_shape"
    candidate = next(
        manifest
        for manifest in payload["manifests"]
        if manifest["service_kind"] == RuntimeServiceKind.CANDIDATE_PUBLISHER
        and manifest["settings"]["strategy_id"] == strategy_id
    )
    runner = next(
        manifest
        for manifest in payload["manifests"]
        if manifest["service_kind"] == RuntimeServiceKind.STRATEGY_LIVE
        and manifest["settings"]["strategy_id"] == strategy_id
    )
    minute = next(
        manifest
        for manifest in payload["manifests"]
        if manifest["service_kind"] == RuntimeServiceKind.MARKET_MINUTE_SOURCE
    )
    minute_authority = next(
        authority
        for authority in minute["settings"]["candidate_authorities"]
        if authority["strategy_id"] == strategy_id
    )
    forged_root = str((tmp_path / "forged-candidate-root").resolve())
    if target == "candidate_root":
        candidate["settings"]["snapshot_root"] = forged_root
    elif target == "minute_root":
        minute_authority["snapshot_root"] = forged_root
    elif target == "runner_root":
        runner["settings"]["candidate_snapshot_root"] = forged_root
    else:
        manifest = candidate if target == "candidate_service_id" else runner
        old_id = manifest["service_id"]
        new_id = f"forged.{old_id}"
        manifest["service_id"] = new_id
        payload["capability_environment"][new_id] = payload["capability_environment"].pop(old_id)
        _retarget_schema_policy_consumer(payload, old_id=old_id, new_id=new_id)
    if target == "runner_service_id":
        with pytest.raises(ValidationError, match="stable service instance"):
            RuntimeDeploymentProfile.model_validate(payload)
        return
    forged = RuntimeDeploymentProfile.model_validate(payload)
    assert forged.profile_id != profile.profile_id
    output = tmp_path / "profiles" / f"{forged.profile_id}.json"
    output.parent.mkdir()

    with pytest.raises(
        ValueError,
        match="topology|service|snapshot.?root|strategy|owned",
    ):
        _publish_profile(forged, output)


def test_publishes_content_addressed_canonical_production_profile(tmp_path: Path) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()

    published = _publish_profile(profile, output)

    assert published == output
    assert output.read_bytes() == canonical_model_json_bytes(profile)
    assert output.stat().st_mode & 0o777 == 0o600
    assert output.stat().st_nlink == 1
    assert _publish_profile(profile, output) == output


def test_profile_publication_recovers_its_post_link_stage(tmp_path: Path) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()
    stage = output.parent / f".{profile.profile_id}.stage"
    stage.write_bytes(canonical_model_json_bytes(profile))
    stage.chmod(0o600)
    output.hardlink_to(stage)
    assert output.stat().st_nlink == 2

    assert _publish_profile(profile, output) == output
    assert not stage.exists()
    assert output.stat().st_nlink == 1


def test_profile_publication_recovers_its_post_link_publication_source(
    tmp_path: Path,
) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()
    payload = canonical_model_json_bytes(profile)
    stage = output.parent / f".{profile.profile_id}.stage"
    stage.write_bytes(payload)
    stage.chmod(0o600)
    publication = output.parent / f".{profile.profile_id}.publish"
    publication.write_bytes(payload)
    publication.chmod(0o600)
    output.hardlink_to(publication)
    assert output.stat().st_nlink == 2

    assert _publish_profile(profile, output) == output
    assert output.read_bytes() == payload
    assert output.stat().st_mode & 0o777 == 0o600
    assert output.stat().st_nlink == 1
    assert not stage.exists()
    assert not publication.exists()


def test_profile_publication_recovers_incomplete_private_output(tmp_path: Path) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    assert profile.profile_id is not None
    output = tmp_path / "profiles" / f"{profile.profile_id}.json"
    output.parent.mkdir()
    output.write_bytes(b"{")
    output.chmod(0o200)

    assert _publish_profile(profile, output) == output
    assert output.read_bytes() == canonical_model_json_bytes(profile)
    assert output.stat().st_mode & 0o777 == 0o600
    assert output.stat().st_nlink == 1


def test_profile_publication_rejects_wrong_name_or_existing_content(tmp_path: Path) -> None:
    profile = build_production_runtime_profile(_inputs(tmp_path))
    output_dir = tmp_path / "profiles"
    output_dir.mkdir()

    with pytest.raises(ValueError, match="profile id"):
        _publish_profile(profile, output_dir / "profile.json")

    assert profile.profile_id is not None
    output = output_dir / f"{profile.profile_id}.json"
    output.write_text("{}")
    output.chmod(0o600)
    with pytest.raises(ValueError, match="existing|canonical"):
        _publish_profile(profile, output)
