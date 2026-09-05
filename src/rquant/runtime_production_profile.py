"""Deterministic production profile for the complete isolated rQuant runtime."""

from __future__ import annotations

import fcntl
import hashlib
import os
import re
import stat
import sys
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal

from pydantic import Field, ValidationInfo, field_validator, model_validator

from rquant.legacy_shadow_export import LegacyShadowRunnerManifestBinding
from rquant.runtime_contracts import RuntimeContractModel
from rquant.runtime_definition_bootstrap import (
    bootstrap_builtin_definitions,
    plan_builtin_definitions,
)
from rquant.runtime_deployment_bundle import (
    RuntimeSchemaV1MigrationAuthorization,
    strategy_live_producer_version,
    validate_runtime_deployment_topology,
)
from rquant.runtime_deployment_profile import (
    LINUX_PRODUCTION_RUNTIME_ROOT,
    PRODUCTION_CANVAS_SIGNER_COMMAND,
    PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
    PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT,
    PRODUCTION_PAGE_CONTROL_INSTANCE_ID,
    PRODUCTION_PAGE_CONTROL_SERVICE_ID,
    PRODUCTION_SHADOW_INSTANCE_ID,
    PRODUCTION_SHADOW_SERVICE_ID,
    PRODUCTION_SHADOW_SIGNER_COMMAND,
    CanvasPublicationRuntimeProfile,
    LabHighWaterRuntimeProfile,
    PageControlRuntimeProfile,
    RuntimeDeploymentProfile,
    RuntimeMode,
    RuntimeRecoveryProductionConfig,
    RuntimeSchemaRolloutPolicy,
    ShadowRuntimeProfile,
)
from rquant.runtime_market_calendar_generation import (
    install_market_calendar_generation,
    market_calendar_generation_path,
)
from rquant.runtime_market_session import load_market_calendar_authority
from rquant.runtime_schema_registry import build_runtime_schema_contract_bundle
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.strict_json import (
    canonical_json_bytes,
    canonical_model_json_bytes,
    strict_canonical_json_loads,
    strict_model_validate_canonical_json,
)

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
StrategyId = Literal["n_shape", "growth_board_surge", "auction_gap"]
PositiveSeconds = Annotated[int, Field(strict=True, ge=1, le=86_400)]
ObservationSeconds = Annotated[int, Field(strict=True, ge=60, le=31 * 86_400)]

_REQUIRED_STRATEGIES = frozenset({"n_shape", "growth_board_surge", "auction_gap"})
_MAX_PRODUCTION_INPUT_BYTES = 16 * 1024 * 1024
_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_READ_FLAGS = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
_DIRECTORY_FLAGS = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
_SINGLETON_SERVICE_IDS = {
    RuntimeServiceKind.REFERENCE_SLOW_SOURCE: "reference-slow.source.v1",
    RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: "reference-slow.publisher.v1",
    RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: "auction-universe.publisher.v1",
    RuntimeServiceKind.AUCTION_MATCH_SOURCE: "auction-match.source.v1",
    RuntimeServiceKind.MARKET_MINUTE_SOURCE: "market-minute.source.v1",
    RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: "watchlist-quote.source.v1",
    RuntimeServiceKind.DAILY_CLOSE_SOURCE: "daily-close.source.v1",
    RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: "daily.pipeline.orchestrator.shadow.v1",
    RuntimeServiceKind.SHADOW_SESSION: "shadow.session.production.v1",
    RuntimeServiceKind.FEATURE_LIVE: "feature.intraday-pit.v1",
    RuntimeServiceKind.SIGNAL_ROUTER: "signal-router.all-strategies.v1",
    RuntimeServiceKind.NOTIFIER: "notifier.admin.shadow.v1",
    RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: "paper-constraint.market.v1",
    RuntimeServiceKind.PAPER_BROKER: "paper-broker.shadow-main.v1",
    RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: "runtime-health.all.v1",
    RuntimeServiceKind.LAB_JOBS_PUBLISHER: "lab-jobs.serving.v1",
    RuntimeServiceKind.LAB_ARTIFACT_CATALOG: "artifact-catalog.primary.v1",
    RuntimeServiceKind.ARTIFACT_RETENTION: "artifact-retention.primary.v1",
    RuntimeServiceKind.PROMOTIONS_PUBLISHER: "promotions.serving.v1",
    RuntimeServiceKind.SERVING_PUBLISHER: "serving.publisher.v1",
}
DAILY_RECEIPT_TRUSTED_KEYRING_PATH = Path("/etc/rquant/daily-receipt-trusted-keys.json")


class ProductionStrategyBinding(RuntimeContractModel):
    strategy_id: StrategyId
    strategy_version: Literal[1] = 1
    registration_fingerprint: Sha256
    candidate_schema_fingerprint: Sha256
    strategy_spec_fingerprint: Sha256
    executable_fingerprint: Sha256


class ProductionRuntimeProfileInputs(RuntimeContractModel):
    producer_commit: CommitSha
    runtime_mode: RuntimeMode = "local-test"
    runtime_root: Path
    operational_database_path: Path
    definition_registry_root: Path
    n_shape_candidate_input_path: Path
    growth_board_candidate_input_path: Path
    historical_minutes_snapshot_path: Path
    historical_minutes_snapshot_id: Sha256
    market_calendar_authority_path: Path
    market_calendar_content_sha256: Sha256
    market_calendar_producer_commit: CommitSha
    trade_calendar_path: Path
    trade_calendar_sha256: Sha256
    routing_policy_path: Path
    routing_policy_fingerprint: Sha256
    strategies: tuple[ProductionStrategyBinding, ...]
    artifact_location_id: str = Field(min_length=1)
    artifact_failure_domain: str = Field(min_length=1)
    artifact_retention_schema_authority_path: Path
    artifact_retention_schema_authority_sha256: Sha256
    artifact_retention_recovery_target_manifest_id: Sha256
    artifact_retention_full_recovery_receipt_id: Sha256
    schema_rollout_stage_timeout_seconds: PositiveSeconds = 600
    schema_consumer_ack_max_age_seconds: PositiveSeconds = 300
    schema_retire_observation_seconds: ObservationSeconds = 86_400
    schema_v1_migration_authority: RuntimeSchemaV1MigrationAuthorization | None = None
    recovery: RuntimeRecoveryProductionConfig
    lab_highwater_stable_identity: str = Field(
        default="rquant-lab-job-center-production-v1",
        min_length=1,
        max_length=512,
    )
    lab_highwater_trusted_keyring_path: Path = Path("/etc/rquant/lab-highwater-trusted-keys.json")
    lab_highwater_timeout_seconds: float = Field(default=10.0, ge=0.1, le=300)
    lab_highwater_allow_identity_rotation: bool = True
    canvas_publication_active_key_id: str | None = Field(default=None, min_length=1)
    canvas_publication_active_public_key_pem: str | None = Field(
        default=None,
        min_length=1,
        max_length=16_384,
    )
    canvas_publication_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    canvas_publication_timeout_seconds: float = Field(default=5.0, ge=0.1, le=30.0)
    shadow_completion_active_key_id: str | None = Field(default=None, min_length=1)
    shadow_completion_active_public_key_pem: str | None = Field(
        default=None,
        min_length=1,
        max_length=16_384,
    )
    shadow_completion_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    shadow_report_active_key_id: str | None = Field(default=None, min_length=1)
    shadow_report_active_public_key_pem: str | None = Field(
        default=None,
        min_length=1,
        max_length=16_384,
    )
    shadow_report_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    shadow_signer_timeout_seconds: float = Field(default=5.0, ge=0.1, le=30.0)
    daily_receipt_active_key_id: str | None = Field(default=None, min_length=1)
    daily_receipt_active_public_key_pem: str | None = Field(
        default=None,
        min_length=1,
        max_length=16_384,
    )
    daily_receipt_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    daily_receipt_signer_socket_endpoint: Path = Path(PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT)
    daily_receipt_trusted_keyring_path: Path = PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH
    daily_receipt_signer_timeout_seconds: float = Field(default=5.0, ge=0.1, le=30.0)

    @field_validator(
        "runtime_root",
        "operational_database_path",
        "definition_registry_root",
        "n_shape_candidate_input_path",
        "growth_board_candidate_input_path",
        "historical_minutes_snapshot_path",
        "market_calendar_authority_path",
        "trade_calendar_path",
        "routing_policy_path",
        "artifact_retention_schema_authority_path",
        "lab_highwater_trusted_keyring_path",
        "daily_receipt_signer_socket_endpoint",
        "daily_receipt_trusted_keyring_path",
    )
    @classmethod
    def require_normalized_absolute_path(cls, value: Path) -> Path:
        normalized = Path(os.path.abspath(value))
        if not value.is_absolute() or value != normalized:
            raise ValueError("production runtime paths must be absolute and normalized")
        return value

    @field_validator("historical_minutes_snapshot_path")
    @classmethod
    def require_historical_parquet(cls, value: Path) -> Path:
        if value.suffix.lower() != ".parquet":
            raise ValueError("historical minute snapshot must be parquet")
        return value

    @field_validator("strategies")
    @classmethod
    def canonicalize_strategies(
        cls,
        value: tuple[ProductionStrategyBinding, ...],
    ) -> tuple[ProductionStrategyBinding, ...]:
        return tuple(sorted(value, key=lambda item: item.strategy_id))

    @model_validator(mode="after")
    def validate_complete_authority_set(
        self,
        info: ValidationInfo,
    ) -> ProductionRuntimeProfileInputs:
        if self.runtime_mode == "linux-production" and self.runtime_root != (
            LINUX_PRODUCTION_RUNTIME_ROOT
        ):
            raise ValueError(
                f"Linux production runtime root must be exactly {LINUX_PRODUCTION_RUNTIME_ROOT}"
            )
        if self.runtime_mode == "linux-production" and not (
            isinstance(info.context, dict)
            and info.context.get("daily_receipt_authority_hydrated") is True
        ):
            raise ValueError(
                "Linux production Daily receipt authority must be hydrated from fixed keyring"
            )
        if self.runtime_mode == "linux-production" and (
            self.canvas_publication_active_key_id is None
            or self.canvas_publication_active_public_key_pem is None
        ):
            raise ValueError(
                "Linux production profile requires an active Canvas publication public key"
            )
        if (
            self.canvas_publication_active_key_id is not None
            and self.canvas_publication_active_key_id
            in self.canvas_publication_previous_public_key_pems
        ):
            raise ValueError("Canvas publication active key cannot also be previous")
        shadow_key_parts = (
            self.shadow_completion_active_key_id,
            self.shadow_completion_active_public_key_pem,
            self.shadow_report_active_key_id,
            self.shadow_report_active_public_key_pem,
        )
        if any(value is None for value in shadow_key_parts):
            raise ValueError("production profile requires complete Shadow public keyrings")
        if self.shadow_completion_active_key_id in self.shadow_completion_previous_public_key_pems:
            raise ValueError("Shadow completion active key cannot also be previous")
        if self.shadow_report_active_key_id in self.shadow_report_previous_public_key_pems:
            raise ValueError("Shadow report active key cannot also be previous")
        if self.runtime_mode == "linux-production" and (
            self.daily_receipt_active_key_id is None
            or self.daily_receipt_active_public_key_pem is None
            or self.daily_receipt_signer_socket_endpoint
            != Path(PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT)
            or self.daily_receipt_trusted_keyring_path
            != PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH
        ):
            raise ValueError(
                "Linux production profile requires the fixed Daily receipt socket authority"
            )
        if (
            self.daily_receipt_active_key_id is not None
            and self.daily_receipt_active_key_id in self.daily_receipt_previous_public_key_pems
        ):
            raise ValueError("Daily receipt active key cannot also be previous")
        strategy_ids = tuple(item.strategy_id for item in self.strategies)
        if len(strategy_ids) != 3 or set(strategy_ids) != _REQUIRED_STRATEGIES:
            raise ValueError("production profile requires exactly the three built-in strategies")
        for attribute in (
            "registration_fingerprint",
            "strategy_spec_fingerprint",
            "executable_fingerprint",
        ):
            values = tuple(getattr(item, attribute) for item in self.strategies)
            if len(values) != len(set(values)):
                raise ValueError(f"production strategy {attribute} values must be unique")
        immutable_inputs = (
            self.operational_database_path,
            self.definition_registry_root,
            self.n_shape_candidate_input_path,
            self.growth_board_candidate_input_path,
            self.historical_minutes_snapshot_path,
            self.market_calendar_authority_path,
            self.trade_calendar_path,
            self.routing_policy_path,
            self.artifact_retention_schema_authority_path,
        )
        if any(path.is_relative_to(self.runtime_root) for path in immutable_inputs):
            raise ValueError("production immutable inputs must be outside the runtime owner root")
        self.recovery.validate_trusted_runtime_root(self.runtime_root)
        if self.recovery.backup_config_path.is_relative_to(self.runtime_root):
            raise ValueError("recovery backup config must be outside the runtime owner root")
        if self.recovery.credential_file.is_relative_to(self.runtime_root):
            raise ValueError("recovery credential must be outside the runtime owner root")
        return self


def _daily_receipt_keyring_identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _verify_daily_receipt_trusted_keyring_parent_chain(path: Path) -> None:
    try:
        for parent in path.parents:
            observed = os.stat(parent, follow_symlinks=False)
            if (
                not stat.S_ISDIR(observed.st_mode)
                or observed.st_uid != 0
                or observed.st_mode & 0o022
            ):
                raise ValueError(
                    "Daily receipt trusted keyring parent must be root-owned "
                    "and not group/world writable"
                )
    except OSError as exc:
        raise ValueError("Daily receipt trusted keyring parent is unavailable") from exc


def _load_daily_receipt_trusted_keyring(path: Path) -> tuple[str, str, dict[str, str]]:
    keyring_path = _require_absolute_normalized_path(
        path,
        label="Daily receipt trusted keyring",
    )
    _verify_daily_receipt_trusted_keyring_parent_chain(keyring_path)
    descriptor = -1
    try:
        descriptor = os.open(keyring_path, _READ_FLAGS)
        observed = os.fstat(descriptor)
        named = os.stat(keyring_path, follow_symlinks=False)
        if (
            not stat.S_ISREG(observed.st_mode)
            or (observed.st_dev, observed.st_ino) != (named.st_dev, named.st_ino)
            or observed.st_uid != 0
            or observed.st_nlink != 1
            or stat.S_IMODE(observed.st_mode) != 0o444
            or not 0 < observed.st_size <= 64 * 1024
        ):
            raise ValueError("Daily receipt trusted keyring file is unsafe")
        payload = os.read(descriptor, 64 * 1024 + 1)
        after = os.fstat(descriptor)
        active = os.stat(keyring_path, follow_symlinks=False)
        if len(payload) != observed.st_size or (
            _daily_receipt_keyring_identity(after) != _daily_receipt_keyring_identity(observed)
            or _daily_receipt_keyring_identity(active) != _daily_receipt_keyring_identity(observed)
        ):
            raise ValueError("Daily receipt trusted keyring changed while reading")
        _verify_daily_receipt_trusted_keyring_parent_chain(keyring_path)
    except OSError as exc:
        raise ValueError("Daily receipt trusted keyring is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    document = strict_canonical_json_loads(payload)
    signed_fields = {
        "schema_version",
        "generation",
        "previous_manifest_hash",
        "active_key_id",
        "active_public_key",
        "previous_public_keys",
        "manifest_hash",
        "signature",
    }
    if (
        not isinstance(document, dict)
        or set(document) != signed_fields
        or document.get("schema_version") != 2
        or type(document.get("generation")) is not int
        or document["generation"] < 1
        or not isinstance(document.get("previous_manifest_hash"), str)
        or re.fullmatch(r"[0-9a-f]{64}", document["previous_manifest_hash"]) is None
        or not isinstance(document.get("active_key_id"), str)
        or not document["active_key_id"]
        or not isinstance(document.get("active_public_key"), str)
        or not document["active_public_key"]
        or not isinstance(document.get("previous_public_keys"), dict)
    ):
        raise ValueError("Daily receipt trusted keyring shape is invalid")
    if (
        not isinstance(document.get("manifest_hash"), str)
        or not re.fullmatch(r"[0-9a-f]{64}", document["manifest_hash"])
        or not isinstance(document.get("signature"), str)
        or not document["signature"]
    ):
        raise ValueError("Daily receipt trusted keyring signature is invalid")
    generation = document["generation"]
    previous_manifest_hash = document["previous_manifest_hash"]
    previous: dict[str, str] = {}
    for key_id, public_key in document["previous_public_keys"].items():
        if (
            not isinstance(key_id, str)
            or not key_id
            or key_id == document["active_key_id"]
            or not isinstance(public_key, str)
            or not public_key
        ):
            raise ValueError("Daily receipt trusted keyring previous keys are invalid")
        previous[key_id] = public_key
    if generation == 1 and (previous_manifest_hash != "0" * 64 or previous):
        raise ValueError("Daily receipt trusted keyring genesis binding is invalid")
    if generation > 1 and (previous_manifest_hash == "0" * 64 or not previous):
        raise ValueError("Daily receipt trusted keyring rotation binding is invalid")
    from rquant.runtime_builder_daily_orchestrator import (
        Ed25519DailyShadowStageReceiptKeyring,
    )
    from rquant.runtime_shadow_validation import _verify_ed25519_signature

    Ed25519DailyShadowStageReceiptKeyring(
        active_key_id=document["active_key_id"],
        active_public_key=document["active_public_key"].encode("utf-8"),
        previous_public_keys={
            key_id: public_key.encode("utf-8") for key_id, public_key in previous.items()
        },
    )
    body = {
        key: document[key] for key in signed_fields if key not in {"manifest_hash", "signature"}
    }
    expected_hash = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
    if document["manifest_hash"] != expected_hash:
        raise ValueError("Daily receipt trusted keyring manifest hash is invalid")
    if not _verify_ed25519_signature(
        public_key=document["active_public_key"].encode("utf-8"),
        payload=expected_hash.encode("ascii"),
        signature=document["signature"],
    ):
        raise ValueError("Daily receipt trusted keyring signature is invalid")
    return document["active_key_id"], document["active_public_key"], dict(sorted(previous.items()))


def _hydrate_daily_receipt_authority_from_fixed_keyring(
    payload: object,
) -> object:
    if not isinstance(payload, dict) or payload.get("runtime_mode") != "linux-production":
        return payload
    active_key_id, active_public_key, previous = _load_daily_receipt_trusted_keyring(
        DAILY_RECEIPT_TRUSTED_KEYRING_PATH
    )
    observed = {
        "daily_receipt_active_key_id": payload.get("daily_receipt_active_key_id"),
        "daily_receipt_active_public_key_pem": payload.get("daily_receipt_active_public_key_pem"),
        "daily_receipt_previous_public_key_pems": payload.get(
            "daily_receipt_previous_public_key_pems",
            {},
        ),
    }
    expected = {
        "daily_receipt_active_key_id": active_key_id,
        "daily_receipt_active_public_key_pem": active_public_key,
        "daily_receipt_previous_public_key_pems": previous,
    }
    for value in observed.values():
        if value is not None and value != {}:
            raise ValueError(
                "Linux production Daily receipt authority must be hydrated from fixed keyring"
            )
    hydrated = dict(payload)
    hydrated.update(expected)
    return hydrated


def _trusted_strategy_contracts(
    *,
    producer_commit: str,
) -> dict[str, tuple[ProductionStrategyBinding, dict[str, dict[str, str]]]]:
    plan = plan_builtin_definitions(producer_commit=producer_commit)
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=producer_commit)
    return {
        binding.strategy_id: (
            ProductionStrategyBinding.model_validate(binding.model_dump(mode="python")),
            {
                name: semantic.contract_payload()
                for name, semantic in registry.load_definition(
                    binding.strategy_id,
                    binding.strategy_version,
                ).static_feature_schema.items()
            },
        )
        for binding in plan.strategies
    }


def _validate_input_strategy_bindings(config: ProductionRuntimeProfileInputs) -> None:
    trusted = _trusted_strategy_contracts(producer_commit=config.producer_commit)
    observed = {binding.strategy_id: binding for binding in config.strategies}
    expected = {strategy_id: binding for strategy_id, (binding, _schema) in trusted.items()}
    if observed != expected:
        raise ValueError("production definition bindings do not match trusted built-in content")


def _validate_profile_strategy_bindings(
    profile: RuntimeDeploymentProfile,
    *,
    production_runtime_root: Path,
) -> None:
    runtime_root = _require_absolute_normalized_path(
        production_runtime_root,
        label="trusted production runtime root",
    )
    if profile.production_runtime_root is None:
        raise ValueError("production profile is missing trusted runtime root evidence")
    if Path(profile.production_runtime_root) != runtime_root:
        raise ValueError("production profile trusted runtime root mismatch")
    if profile.recovery is None:
        raise ValueError("production runtime profile requires recovery configuration")
    profile.recovery.validate_trusted_runtime_root(runtime_root)
    validate_runtime_deployment_topology(
        runtime_root,
        producer_commit=profile.producer_commit,
        manifests=profile.manifests,
    )
    trusted = _trusted_strategy_contracts(producer_commit=profile.producer_commit)
    expected_ids = set(trusted)
    manifests_by_kind: dict[RuntimeServiceKind, list[RuntimeServiceManifest]] = {}
    for manifest in profile.manifests:
        manifests_by_kind.setdefault(manifest.service_kind, []).append(manifest)
    for service_kind, service_id in _SINGLETON_SERVICE_IDS.items():
        manifests = manifests_by_kind.get(service_kind, [])
        if len(manifests) != 1 or manifests[0].service_id != service_id:
            raise ValueError(f"production {service_kind.value} service topology is invalid")

    expected_service_ids = set(_SINGLETON_SERVICE_IDS.values())
    expected_service_ids.update(_candidate_service_id(item) for item in expected_ids)
    expected_service_ids.update(_strategy_service_id(item) for item in expected_ids)
    if {manifest.service_id for manifest in profile.manifests} != expected_service_ids:
        raise ValueError("production service topology is incomplete or contains unknown services")

    candidate_ids: set[str] = set()
    strategy_ids: set[str] = set()
    minute_ids: set[str] = set()
    quote_ids: set[str] = set()
    candidate_roots: dict[str, str] = {}
    runner_roots: dict[str, str] = {}
    runner_manifests: dict[str, RuntimeServiceManifest] = {}
    minute_roots: dict[str, str] = {}
    quote_roots: dict[str, str] = {}

    for manifest in profile.manifests:
        settings = manifest.settings
        if manifest.service_kind is RuntimeServiceKind.CANDIDATE_PUBLISHER:
            strategy_id = str(settings.get("strategy_id", ""))
            if strategy_id not in trusted:
                raise ValueError("production candidate publisher has an untrusted strategy")
            if strategy_id in candidate_ids:
                raise ValueError("production candidate publisher strategy binding is duplicated")
            if manifest.service_id != _candidate_service_id(strategy_id):
                raise ValueError("production candidate publisher service topology is invalid")
            binding, schema = trusted[strategy_id]
            if (
                settings.get("strategy_version") != binding.strategy_version
                or settings.get("definition_fingerprint") != binding.registration_fingerprint
                or settings.get("executable_fingerprint") != binding.executable_fingerprint
                or settings.get("candidate_schema_fingerprint")
                != binding.candidate_schema_fingerprint
                or settings.get("static_feature_schema") != schema
            ):
                raise ValueError("production candidate definition binding is not trusted")
            candidate_ids.add(strategy_id)
            candidate_roots[strategy_id] = str(settings.get("snapshot_root", ""))
        elif manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE:
            strategy_id = str(settings.get("strategy_id", ""))
            if strategy_id not in trusted:
                raise ValueError("production strategy runner has an untrusted strategy")
            if strategy_id in strategy_ids:
                raise ValueError("production strategy runner binding is duplicated")
            if manifest.service_id != _strategy_service_id(strategy_id):
                raise ValueError("production strategy runner service topology is invalid")
            binding, _schema = trusted[strategy_id]
            if (
                settings.get("strategy_version") != binding.strategy_version
                or settings.get("strategy_registration_fingerprint")
                != binding.registration_fingerprint
                or settings.get("strategy_executable_fingerprint") != binding.executable_fingerprint
                or settings.get("strategy_spec_fingerprint") != binding.strategy_spec_fingerprint
                or settings.get("evaluator_contract_fingerprint") != binding.executable_fingerprint
                or settings.get("candidate_schema_fingerprint")
                != binding.candidate_schema_fingerprint
            ):
                raise ValueError("production strategy definition binding is not trusted")
            strategy_ids.add(strategy_id)
            runner_roots[strategy_id] = str(settings.get("candidate_snapshot_root", ""))
            runner_manifests[strategy_id] = manifest
        elif manifest.service_kind in {
            RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
            RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        }:
            source_label = (
                "minute"
                if manifest.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE
                else (
                    "watchlist quote"
                    if manifest.service_kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE
                    else "daily close"
                )
            )
            expected_source_root = (
                runtime_root / "live" / "market-minute"
                if manifest.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE
                else (
                    runtime_root / "live" / "watchlist-quote"
                    if manifest.service_kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE
                    else runtime_root / "live" / "daily-close"
                )
            )
            source_root = _require_absolute_normalized_path(
                Path(str(settings.get("spool_root", ""))),
                label=f"production {source_label} source spool root",
            )
            if source_root != expected_source_root:
                raise ValueError(f"production {source_label} source managed topology is invalid")
            expected_quota = source_root / "quota.sqlite3"
            if Path(str(settings.get("quota_path", ""))) != expected_quota:
                raise ValueError(f"production {source_label} source managed topology is invalid")
            if manifest.service_kind is RuntimeServiceKind.DAILY_CLOSE_SOURCE:
                if not isinstance(settings.get("quota_units_per_window"), int) or (
                    isinstance(settings.get("quota_units_per_window"), bool)
                    or int(settings["quota_units_per_window"]) < 1
                ):
                    raise ValueError("production daily close source quota is invalid")
                if settings.get("quota_accounting_mode") != "transport":
                    raise ValueError("production daily close source must use transport quota")
                if settings.get("quota_cost_per_request") is not None:
                    raise ValueError("production daily close source cannot use a fixed quota cost")
                continue
            if (
                manifest.service_kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE
                and settings.get("rollout_mode") != "candidate"
            ):
                raise ValueError("production watchlist quote source must remain candidate")
            authorities = settings.get("candidate_authorities")
            if not isinstance(authorities, (list, tuple)):
                raise ValueError(
                    f"production {source_label} source candidate authorities are invalid"
                )
            for authority in authorities:
                if not isinstance(authority, Mapping):
                    raise ValueError(
                        f"production {source_label} source candidate authority is invalid"
                    )
                strategy_id = str(authority.get("strategy_id", ""))
                if strategy_id not in trusted:
                    raise ValueError(f"production {source_label} source has an untrusted strategy")
                source_ids = (
                    minute_ids
                    if manifest.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE
                    else quote_ids
                )
                if strategy_id in source_ids:
                    raise ValueError(
                        f"production {source_label} source strategy binding is duplicated"
                    )
                binding, schema = trusted[strategy_id]
                if (
                    authority.get("strategy_version") != str(binding.strategy_version)
                    or authority.get("definition_fingerprint") != binding.registration_fingerprint
                    or authority.get("executable_fingerprint") != binding.executable_fingerprint
                    or authority.get("candidate_schema_fingerprint")
                    != binding.candidate_schema_fingerprint
                    or tuple(authority.get("static_feature_names", ())) != tuple(sorted(schema))
                    or authority.get("static_feature_schema") != schema
                ):
                    raise ValueError(
                        f"production {source_label} source definition binding is not trusted"
                    )
                source_ids.add(strategy_id)
                if manifest.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
                    minute_roots[strategy_id] = str(authority.get("snapshot_root", ""))
                else:
                    quote_roots[strategy_id] = str(authority.get("snapshot_root", ""))

    if (
        candidate_ids != expected_ids
        or strategy_ids != expected_ids
        or minute_ids != expected_ids
        or quote_ids != expected_ids
    ):
        raise ValueError(
            "production profile must bind every trusted built-in strategy exactly once"
        )
    from rquant.runtime_builder_shadow import ShadowSessionSettings

    shadow_manifest = next(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.SHADOW_SESSION
    )
    shadow_settings = ShadowSessionSettings.model_validate(dict(shadow_manifest.settings))
    expected_shadow_bindings = {
        strategy_id: LegacyShadowRunnerManifestBinding.create(
            strategy_id=strategy_id,
            strategy_version=int(runner_manifests[strategy_id].settings["strategy_version"]),
            producer_manifest_fingerprint=(runner_manifests[strategy_id].manifest_fingerprint),
            producer_commit=runner_manifests[strategy_id].producer_commit,
            producer_service_id=runner_manifests[strategy_id].service_id,
            producer_instance_id=str(
                runner_manifests[strategy_id].settings["producer_instance_id"]
            ),
            producer_version=str(runner_manifests[strategy_id].settings["producer_version"]),
            strategy_registration_fingerprint=str(
                runner_manifests[strategy_id].settings["strategy_registration_fingerprint"]
            ),
            strategy_spec_fingerprint=str(
                runner_manifests[strategy_id].settings["strategy_spec_fingerprint"]
            ),
            evaluator_contract_fingerprint=str(
                runner_manifests[strategy_id].settings["evaluator_contract_fingerprint"]
            ),
            executable_fingerprint=str(
                runner_manifests[strategy_id].settings["strategy_executable_fingerprint"]
            ),
        )
        for strategy_id in {"n_shape", "growth_board_surge"}
    }
    if {
        binding.strategy_id: binding for binding in shadow_settings.runner_manifest_bindings
    } != expected_shadow_bindings:
        raise ValueError("Shadow runner bindings differ from strategy live manifests")
    for strategy_id in sorted(expected_ids):
        roots = {
            candidate_roots[strategy_id],
            runner_roots[strategy_id],
            minute_roots[strategy_id],
            quote_roots[strategy_id],
        }
        if len(roots) != 1 or not next(iter(roots)):
            raise ValueError("production strategy snapshot root topology is inconsistent")
        root = _require_absolute_normalized_path(
            Path(next(iter(roots))),
            label="production strategy snapshot root",
        )
        if root != _candidate_root(runtime_root, strategy_id):
            raise ValueError("production strategy snapshot root topology is invalid")


def _instance_name(service_id: str) -> str:
    return "svc-" + hashlib.sha256(service_id.encode("utf-8")).hexdigest()


def _manifest(
    inputs: ProductionRuntimeProfileInputs,
    *,
    service_id: str,
    kind: RuntimeServiceKind,
    plane: RuntimeServicePlane,
    interval_seconds: float,
    stale_after_seconds: float,
    settings: dict[str, object],
) -> RuntimeServiceManifest:
    return RuntimeServiceManifest(
        service_id=service_id,
        service_kind=kind,
        plane=plane,
        interval_seconds=interval_seconds,
        stale_after_seconds=stale_after_seconds,
        producer_commit=inputs.producer_commit,
        settings=settings,
    )


def _candidate_service_id(strategy_id: str) -> str:
    return f"candidate.{strategy_id}.v1"


def _strategy_service_id(strategy_id: str) -> str:
    return f"strategy.{strategy_id}.v1"


def _candidate_root(root: Path, strategy_id: str) -> Path:
    return root / "live" / "candidates" / _instance_name(_candidate_service_id(strategy_id))


def _strategy_state_path(root: Path, strategy_id: str) -> Path:
    return (
        root
        / "live"
        / "strategies"
        / _instance_name(_strategy_service_id(strategy_id))
        / "runner.sqlite3"
    )


def _recovery_source_path(
    recovery: RuntimeRecoveryProductionConfig,
    logical_role: str,
) -> Path:
    role = next(
        (item for item in recovery.artifact_roles if item.logical_role == logical_role),
        None,
    )
    if role is None:  # pragma: no cover - recovery model invariant
        raise ValueError("recovery artifact role is missing")
    return recovery.backup_source_root.joinpath(*PurePosixPath(role.source_path).parts)


def _validate_recovery_artifact_bindings(
    config: ProductionRuntimeProfileInputs,
    *,
    broker_path: Path,
) -> None:
    recovery = config.recovery
    production = _recovery_source_path(recovery, recovery.production_artifact_role)
    paper = _recovery_source_path(recovery, recovery.paper_ledger_artifact_role)
    if production != config.operational_database_path:
        raise ValueError("recovery production DuckDB role differs from production profile")
    if paper != broker_path:
        raise ValueError("recovery paper ledger role differs from production broker")


def _control_bucket(kind: RuntimeServiceKind) -> str:
    return {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE: "reference-slow-sources",
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: "reference-slow-publishers",
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: "auction-universe-publishers",
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: "auction-match-sources",
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: "market-minute-sources",
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: "watchlist-quote-sources",
        RuntimeServiceKind.DAILY_CLOSE_SOURCE: "daily-close-sources",
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: "daily-orchestrators",
        RuntimeServiceKind.SHADOW_SESSION: "shadow-sessions",
        RuntimeServiceKind.CANDIDATE_PUBLISHER: "candidates",
        RuntimeServiceKind.FEATURE_LIVE: "features",
        RuntimeServiceKind.STRATEGY_LIVE: "strategies",
        RuntimeServiceKind.SIGNAL_ROUTER: "signal-routers",
        RuntimeServiceKind.NOTIFIER: "notifiers",
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: "paper-constraints",
        RuntimeServiceKind.PAPER_BROKER: "paper-brokers",
        RuntimeServiceKind.LAB_JOBS_PUBLISHER: "lab-jobs-publishers",
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG: "artifact-catalogs",
        RuntimeServiceKind.ARTIFACT_RETENTION: "artifact-retention",
        RuntimeServiceKind.PROMOTIONS_PUBLISHER: "promotions-publishers",
    }[kind]


def _revalidate_production_inputs(
    inputs: ProductionRuntimeProfileInputs,
) -> ProductionRuntimeProfileInputs:
    """Re-run the input validators on an already validated contract.

    `RuntimeContractModel` sets `revalidate_instances="always"`, so handing a model
    instance to `model_validate` runs every validator again — including the
    linux-production gate that demands `context["daily_receipt_authority_hydrated"]`.
    That context is a statement about *how a raw payload was loaded*
    (`load_production_runtime_profile_inputs` sets it after
    `_hydrate_daily_receipt_authority_from_fixed_keyring` has filled the three Daily
    receipt fields from the fixed keyring), so an instance that already exists has by
    construction passed through it: the only way to build one in linux-production mode
    is that loader. Re-asserting the flag for an instance therefore restates a fact,
    while a raw mapping still has to come through the loader to get it.

    Without this, every linux-production call died at the first line of
    `build_production_runtime_profile` — which is why the route A command chain stopped
    before printing its first target (found by pkgB review B-1).
    """

    if isinstance(inputs, ProductionRuntimeProfileInputs):
        return ProductionRuntimeProfileInputs.model_validate(
            inputs,
            context={"daily_receipt_authority_hydrated": True},
        )
    return ProductionRuntimeProfileInputs.model_validate(inputs)


def build_production_runtime_profile(
    inputs: ProductionRuntimeProfileInputs,
) -> RuntimeDeploymentProfile:
    """Build one content-addressed, least-privilege runtime profile."""

    config = _revalidate_production_inputs(inputs)
    _validate_input_strategy_bindings(config)
    builtin_registry = BuiltinStrategyEvaluatorRegistry(producer_commit=config.producer_commit)
    root = config.runtime_root
    calendar = market_calendar_generation_path(
        root,
        config.market_calendar_content_sha256,
    )
    reference_registry = root / "authorities" / "reference-slow" / "reference.sqlite3"
    minute_root = root / "live" / "market-minute"
    quote_root = root / "live" / "watchlist-quote"
    signal_root = root / "live" / "signal-bus"
    legacy_shadow_root = root.parent / "legacy-shadow"
    from rquant.runtime_builder_daily_orchestrator import build_daily_shadow_stage_commands

    daily_shadow_stage_commands = [
        command.model_dump(mode="json")
        for command in build_daily_shadow_stage_commands(
            python_executable=(
                Path("/home/lighthouse/rquant/.venv/bin/python")
                if config.runtime_mode == "linux-production"
                else Path(sys.executable)
            ),
            working_directory=(
                Path("/home/lighthouse/rquant")
                if config.runtime_mode == "linux-production"
                else None
            ),
        )
    ]

    reference_source_id = "reference-slow.source.v1"
    reference_publisher_id = "reference-slow.publisher.v1"
    auction_universe_id = "auction-universe.publisher.v1"
    auction_source_id = "auction-match.source.v1"
    minute_source_id = "market-minute.source.v1"
    quote_source_id = "watchlist-quote.source.v1"
    daily_close_source_id = "daily-close.source.v1"
    daily_orchestrator_id = "daily.pipeline.orchestrator.shadow.v1"
    shadow_session_id = PRODUCTION_SHADOW_SERVICE_ID
    feature_id = "feature.intraday-pit.v1"
    router_id = "signal-router.all-strategies.v1"
    notifier_id = "notifier.admin.shadow.v1"
    constraint_id = "paper-constraint.market.v1"
    broker_id = "paper-broker.shadow-main.v1"
    health_id = "runtime-health.all.v1"
    lab_jobs_id = "lab-jobs.serving.v1"
    artifact_id = "artifact-catalog.primary.v1"
    retention_id = "artifact-retention.primary.v1"
    promotions_id = "promotions.serving.v1"
    serving_id = "serving.publisher.v1"

    reference_publisher_instance = _instance_name(reference_publisher_id)
    reference_publisher_cursor_root = (
        root / "control" / "reference-slow-publishers" / reference_publisher_instance / "cursors"
    )
    broker_instance = _instance_name(broker_id)
    broker_root = root / "live" / "paper-brokers" / broker_instance
    notifier_instance = _instance_name(notifier_id)
    notifier_root = root / "live" / "notifications" / notifier_instance
    artifact_instance = _instance_name(artifact_id)
    retention_instance = _instance_name(retention_id)
    _validate_recovery_artifact_bindings(
        config,
        broker_path=broker_root / "broker.sqlite3",
    )
    strategy_live_manifests = tuple(
        _manifest(
            config,
            service_id=_strategy_service_id(strategy.strategy_id),
            kind=RuntimeServiceKind.STRATEGY_LIVE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=2,
            stale_after_seconds=30,
            settings={
                "feature_spool_root": str(root / "live" / "features"),
                "runner_state_path": str(_strategy_state_path(root, strategy.strategy_id)),
                "definition_registry_root": str(config.definition_registry_root),
                "strategy_registration_fingerprint": strategy.registration_fingerprint,
                "strategy_spec_fingerprint": strategy.strategy_spec_fingerprint,
                "evaluator_contract_fingerprint": strategy.executable_fingerprint,
                "strategy_executable_fingerprint": strategy.executable_fingerprint,
                "candidate_schema_fingerprint": strategy.candidate_schema_fingerprint,
                "candidate_snapshot_root": str(_candidate_root(root, strategy.strategy_id)),
                "paper_broker_path": str(broker_root / "broker.sqlite3"),
                "paper_account_id": "shadow-main",
                "candidate_max_age_seconds": 7 * 24 * 60 * 60,
                "strategy_id": strategy.strategy_id,
                "strategy_version": strategy.strategy_version,
                "batch_limit": 128,
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "signal_bus_path": str(signal_root / "signal_bus.sqlite3"),
                "routing_policy_fingerprint": config.routing_policy_fingerprint,
                "producer_instance_id": _instance_name(_strategy_service_id(strategy.strategy_id)),
                "producer_version": strategy_live_producer_version(
                    service_id=_strategy_service_id(strategy.strategy_id),
                    strategy_version=strategy.strategy_version,
                    producer_commit=config.producer_commit,
                ),
            },
        )
        for strategy in config.strategies
    )

    manifests: list[RuntimeServiceManifest] = [
        _manifest(
            config,
            service_id=reference_source_id,
            kind=RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=30,
            stale_after_seconds=180,
            settings={
                "database_path": str(config.operational_database_path),
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "spool_root": str(root / "live" / "reference-slow"),
                "quota_path": str(root / "live" / "reference-slow" / "quota.sqlite3"),
                "quota_units_per_window": 500,
                "quota_accounting_mode": "transport",
                "quota_cost_per_capture": None,
                "retry_ordinal": 0,
                "pending_recovery_min_age_seconds": 60,
                "revision_lookback_sessions": 5,
                "history_page_size": 64,
                "limits": {
                    "snapshot_max_bytes": 8 * 1024**3,
                    "snapshot_min_free_bytes": 2 * 1024**3,
                    "snapshot_copy_timeout_seconds": 45.0,
                    "query_chunk_rows": 512,
                    "max_response_rows": 10_000,
                    "max_response_bytes": 8 * 1024**2,
                },
                "consumer_cursor_root": str(reference_publisher_cursor_root),
                "retention_consumer_id": "reference-slow-publisher",
                "retention_hot_batches": 128,
                "retention_page_size": 32,
                "producer_version": "reference-slow-source-v1",
            },
        ),
        _manifest(
            config,
            service_id=reference_publisher_id,
            kind=RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=5,
            stale_after_seconds=180,
            settings={
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "spool_root": str(root / "live" / "reference-slow"),
                "registry_path": str(reference_registry),
                "cursor_root": str(reference_publisher_cursor_root),
                "consumer_id": "reference-slow-publisher",
                "page_size": 16,
            },
        ),
        _manifest(
            config,
            service_id=auction_universe_id,
            kind=RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=30,
            stale_after_seconds=180,
            settings={
                "database_path": str(config.operational_database_path),
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "authority_root": str(root / "authorities" / "auction-universe"),
            },
        ),
        _manifest(
            config,
            service_id=auction_source_id,
            kind=RuntimeServiceKind.AUCTION_MATCH_SOURCE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=2,
            stale_after_seconds=30,
            settings={
                "spool_root": str(root / "live" / "auction-match"),
                "quota_path": str(root / "live" / "auction-match" / "quota.sqlite3"),
                "quota_units_per_window": 500,
                "quota_cost_per_request": 1,
                "producer_version": "auction-match-source-v1",
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "universe_path": str(root / "authorities" / "auction-universe" / "current.json"),
                "max_attempts": 3,
            },
        ),
    ]

    candidate_authorities = [
        {
            "strategy_id": strategy.strategy_id,
            "strategy_version": str(strategy.strategy_version),
            "snapshot_root": str(_candidate_root(root, strategy.strategy_id)),
            "required": True,
            "max_age_seconds": 7 * 24 * 60 * 60,
            "definition_fingerprint": strategy.registration_fingerprint,
            "executable_fingerprint": strategy.executable_fingerprint,
            "candidate_schema_fingerprint": strategy.candidate_schema_fingerprint,
            "static_feature_names": list(
                sorted(
                    builtin_registry.load_definition(
                        strategy.strategy_id,
                        strategy.strategy_version,
                    ).static_feature_schema
                )
            ),
            "static_feature_schema": {
                name: semantic.contract_payload()
                for name, semantic in builtin_registry.load_definition(
                    strategy.strategy_id,
                    strategy.strategy_version,
                ).static_feature_schema.items()
            },
        }
        for strategy in config.strategies
    ]
    manifests.append(
        _manifest(
            config,
            service_id=quote_source_id,
            kind=RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=5,
            stale_after_seconds=30,
            settings={
                "spool_root": str(quote_root),
                "quota_path": str(quote_root / "quota.sqlite3"),
                "quota_units_per_window": 12,
                "quota_cost_per_request": 1,
                "producer_version": "watchlist-quote-source-v1",
                "schema_version": 2,
                "rollout_mode": "candidate",
                "minimum_cadence_seconds": 5.0,
                "request_timeout_seconds": 2.5,
                "failure_threshold": 3,
                "circuit_cooldown_seconds": 30.0,
                "max_backoff_seconds": 60.0,
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "candidate_authorities": candidate_authorities,
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=minute_source_id,
            kind=RuntimeServiceKind.MARKET_MINUTE_SOURCE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=5,
            stale_after_seconds=30,
            settings={
                "spool_root": str(minute_root),
                "quota_path": str(minute_root / "quota.sqlite3"),
                "quota_units_per_window": 500,
                "quota_cost_per_request": 20,
                "pending_recovery_min_age_seconds": 60,
                "max_codes_per_source_call": 300,
                "producer_version": "market-minute-source-v1",
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "candidate_authorities": candidate_authorities,
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=daily_orchestrator_id,
            kind=RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=60,
            stale_after_seconds=172_800,
            settings={
                "storage_root": str(root / "research" / "daily-pipeline"),
                "source_spool_root": str(root / "live" / "daily-close"),
                "deployment_profile_path": str(root / "current" / "deployment-profile.json"),
                "mode": "shadow",
                "service_owner": daily_orchestrator_id,
                "stages": [
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
                ],
                "stage_commands": daily_shadow_stage_commands,
                "receipt_active_key_id": (
                    config.daily_receipt_active_key_id or config.shadow_completion_active_key_id
                ),
                "receipt_active_public_key_pem": (
                    config.daily_receipt_active_public_key_pem
                    or config.shadow_completion_active_public_key_pem
                ),
                "receipt_previous_public_key_pems": (config.daily_receipt_previous_public_key_pems),
                "receipt_signer_socket_endpoint": str(config.daily_receipt_signer_socket_endpoint),
                "receipt_trusted_keyring_path": str(config.daily_receipt_trusted_keyring_path),
                "receipt_signer_timeout_seconds": (config.daily_receipt_signer_timeout_seconds),
                "receipt_signer_test_mode": (
                    config.runtime_mode == "local-test"
                    and config.daily_receipt_signer_socket_endpoint
                    != Path(PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT)
                ),
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=shadow_session_id,
            kind=RuntimeServiceKind.SHADOW_SESSION,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=300,
            stale_after_seconds=172_800,
            settings={
                "report_root": str(root / "research" / "shadow-reports"),
                "legacy_monitor_root": str(legacy_shadow_root / "monitor"),
                "legacy_surge_root": str(legacy_shadow_root / "surge"),
                "isolated_runner_root": str(legacy_shadow_root / "isolated-runners"),
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
                "completion_active_key_id": config.shadow_completion_active_key_id,
                "completion_active_public_key_pem": (
                    config.shadow_completion_active_public_key_pem
                ),
                "completion_previous_public_key_pems": (
                    config.shadow_completion_previous_public_key_pems
                ),
                "report_active_key_id": config.shadow_report_active_key_id,
                "report_active_public_key_pem": config.shadow_report_active_public_key_pem,
                "report_previous_public_key_pems": (config.shadow_report_previous_public_key_pems),
                "signer_command": list(PRODUCTION_SHADOW_SIGNER_COMMAND),
                "report_producer_service_id": PRODUCTION_SHADOW_SERVICE_ID,
                "report_producer_instance_id": PRODUCTION_SHADOW_INSTANCE_ID,
                "signer_timeout_seconds": config.shadow_signer_timeout_seconds,
                "producer_version": "shadow-session-production-v1",
                "match_tolerance_microseconds": 60_000_000,
                "mode": "shadow",
                "strategy_bindings": [
                    {
                        "strategy_id": strategy.strategy_id,
                        "strategy_version": strategy.strategy_version,
                        "definition_fingerprint": strategy.registration_fingerprint,
                        "executable_fingerprint": strategy.executable_fingerprint,
                    }
                    for strategy in config.strategies
                    if strategy.strategy_id in {"n_shape", "growth_board_surge"}
                ],
                "runner_manifest_bindings": [
                    LegacyShadowRunnerManifestBinding.create(
                        strategy_id=str(manifest.settings["strategy_id"]),
                        strategy_version=int(manifest.settings["strategy_version"]),
                        producer_manifest_fingerprint=manifest.manifest_fingerprint,
                        producer_commit=manifest.producer_commit,
                        producer_service_id=manifest.service_id,
                        producer_instance_id=str(manifest.settings["producer_instance_id"]),
                        producer_version=str(manifest.settings["producer_version"]),
                        strategy_registration_fingerprint=str(
                            manifest.settings["strategy_registration_fingerprint"]
                        ),
                        strategy_spec_fingerprint=str(
                            manifest.settings["strategy_spec_fingerprint"]
                        ),
                        evaluator_contract_fingerprint=str(
                            manifest.settings["evaluator_contract_fingerprint"]
                        ),
                        executable_fingerprint=str(
                            manifest.settings["strategy_executable_fingerprint"]
                        ),
                    ).model_dump(mode="json")
                    for manifest in strategy_live_manifests
                    if manifest.settings["strategy_id"] in {"n_shape", "growth_board_surge"}
                ],
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=daily_close_source_id,
            kind=RuntimeServiceKind.DAILY_CLOSE_SOURCE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=60,
            stale_after_seconds=3_600,
            settings={
                "spool_root": str(root / "live" / "daily-close"),
                "quota_path": str(root / "live" / "daily-close" / "quota.sqlite3"),
                "quota_units_per_window": 20,
                "quota_accounting_mode": "transport",
                "quota_cost_per_request": None,
                "pending_recovery_min_age_seconds": 300,
                "producer_version": "daily-close-source-v1",
                "calendar_path": str(calendar),
                "calendar_expected_commit": config.market_calendar_producer_commit,
                "calendar_content_sha256": config.market_calendar_content_sha256,
            },
        )
    )
    retention_state_root = root / "research" / "artifact-retention" / retention_instance
    manifests.append(
        _manifest(
            config,
            service_id=retention_id,
            kind=RuntimeServiceKind.ARTIFACT_RETENTION,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=300,
            stale_after_seconds=900,
            settings={
                "managed_root": str(root / "research" / "final-artifacts"),
                "state_root": str(retention_state_root),
                "reference_store_path": str(retention_state_root / "references.sqlite3"),
                "catalog_authority_root": str(retention_state_root / "catalog-authority"),
                "recovery_publication_root": str(config.recovery.backup_publication_root),
                "recovery_restore_root": str(config.recovery.isolated_restore_root),
                "recovery_target_manifest_id": (
                    config.artifact_retention_recovery_target_manifest_id
                ),
                "recovery_profile_generation": config.recovery.profile_generation,
                "full_recovery_receipt_id": config.artifact_retention_full_recovery_receipt_id,
                "max_recovery_age": "P30D",
                "schema_authority_root": str(
                    config.artifact_retention_schema_authority_path.parent
                ),
                "schema_authority_path": str(config.artifact_retention_schema_authority_path),
                "schema_authority_sha256": config.artifact_retention_schema_authority_sha256,
                "migration": {
                    "warm_root": str(root / "research" / "final-artifacts" / "warm"),
                    "cold_root": str(root / "research" / "final-artifacts" / "cold"),
                    "warm_failure_domain": f"{config.artifact_failure_domain}-warm",
                    "cold_failure_domain": f"{config.artifact_failure_domain}-cold",
                    "batch_items": 16,
                    "batch_bytes": 1073741824,
                    "max_runtime": "PT60S",
                    "query_page_items": 64,
                },
                "max_bundle_items": 128,
                "max_bundle_bytes": 8589934592,
                "retention_policy": {
                    "hot_min_age": "P7D",
                    "warm_min_age": "P30D",
                    "cold_min_age": "P90D",
                    "minimum_verified_copies": 1,
                    "verification_max_age": "P1D",
                    "plan_ttl": "PT1H",
                    "claim_ttl": "PT10M",
                    "rules": [],
                },
                "worker": {
                    "batch_items": 16,
                    "batch_bytes": 1073741824,
                    "max_runtime": "PT60S",
                    "lease_ttl": "PT5M",
                    "max_attempts": 3,
                    "retry_delay": "PT1M",
                },
            },
        )
    )

    sealed_candidate_inputs = {
        "n_shape": config.n_shape_candidate_input_path,
        "growth_board_surge": config.growth_board_candidate_input_path,
    }
    for strategy in config.strategies:
        settings: dict[str, object] = {
            "strategy_id": strategy.strategy_id,
            "strategy_version": strategy.strategy_version,
            "definition_fingerprint": strategy.registration_fingerprint,
            "executable_fingerprint": strategy.executable_fingerprint,
            "candidate_schema_fingerprint": strategy.candidate_schema_fingerprint,
            "static_feature_schema": {
                name: semantic.contract_payload()
                for name, semantic in builtin_registry.load_definition(
                    strategy.strategy_id,
                    strategy.strategy_version,
                ).static_feature_schema.items()
            },
            "snapshot_root": str(_candidate_root(root, strategy.strategy_id)),
        }
        if strategy.strategy_id == "auction_gap":
            settings.update(
                input_mode="auction_live",
                auction_spool_root=str(root / "live" / "auction-match"),
                daily_database_path=str(config.operational_database_path),
                reference_registry_path=str(reference_registry),
                calendar_path=str(calendar),
                calendar_expected_commit=config.market_calendar_producer_commit,
                calendar_content_sha256=config.market_calendar_content_sha256,
            )
        else:
            settings.update(
                input_mode="sealed_document",
                candidate_input_path=str(sealed_candidate_inputs[strategy.strategy_id]),
            )
        manifests.append(
            _manifest(
                config,
                service_id=_candidate_service_id(strategy.strategy_id),
                kind=RuntimeServiceKind.CANDIDATE_PUBLISHER,
                plane=RuntimeServicePlane.LIVE,
                interval_seconds=5,
                stale_after_seconds=180,
                settings=settings,
            )
        )

    manifests.append(
        _manifest(
            config,
            service_id=feature_id,
            kind=RuntimeServiceKind.FEATURE_LIVE,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=2,
            stale_after_seconds=30,
            settings={
                "raw_spool_root": str(minute_root),
                "feature_spool_root": str(root / "live" / "features"),
                "historical_minutes_snapshot_path": str(config.historical_minutes_snapshot_path),
                "historical_snapshot_id": config.historical_minutes_snapshot_id,
                "limit": 128,
                "consumer_id": "feature-live",
                "feature_config": {
                    "lookback_sessions": 20,
                    "opening_acceleration_block_minutes": 3,
                    "bar_timestamp_semantics": "bar_end",
                    "contract_id": "intraday-pit",
                    "contract_version": 3,
                    "schema_version": 2,
                },
            },
        )
    )

    manifests.extend(strategy_live_manifests)

    manifests.append(
        _manifest(
            config,
            service_id=router_id,
            kind=RuntimeServiceKind.SIGNAL_ROUTER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=2,
            stale_after_seconds=30,
            settings={
                "signal_bus_path": str(signal_root / "signal_bus.sqlite3"),
                "signal_spool_root": str(signal_root / "spool"),
                "sources": [
                    {
                        "source_id": _strategy_service_id(strategy.strategy_id),
                        "runner_state_path": str(_strategy_state_path(root, strategy.strategy_id)),
                        "expected_strategy_registration_fingerprint": (
                            strategy.registration_fingerprint
                        ),
                        "expected_strategy_spec_fingerprint": (strategy.strategy_spec_fingerprint),
                        "expected_evaluator_contract_fingerprint": (
                            strategy.executable_fingerprint
                        ),
                    }
                    for strategy in config.strategies
                ],
                "routing_policy_fingerprint": config.routing_policy_fingerprint,
                "routing_policy_path": str(config.routing_policy_path),
                "batch_limit": 256,
                "paused": False,
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=notifier_id,
            kind=RuntimeServiceKind.NOTIFIER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=2,
            stale_after_seconds=30,
            settings={
                "signal_spool_root": str(signal_root / "spool"),
                "notification_state_path": str(notifier_root / "notification_state.sqlite3"),
                "worker_id": "notifier-admin-shadow",
                "batch_limit": 128,
                "lease_seconds": 30,
                "serving_authority_root": str(notifier_root / "serving-authority"),
                "page_projection_database_path": str(config.operational_database_path),
                "page_projection_surge_live_root": str(
                    config.operational_database_path.parent / "surge_live"
                ),
                **(
                    {
                        "page_projection_canvas_catalog_root": str(
                            root / "serving" / "page-control" / "canvases"
                        ),
                        "page_projection_canvas_receipt_root": str(
                            root / "serving" / "page-control" / "canvas-publication-receipts"
                        ),
                        "page_projection_page_control_outbox_path": str(
                            root / "control" / "page-control.sqlite3"
                        ),
                        "page_projection_canvas_active_key_id": (
                            config.canvas_publication_active_key_id
                        ),
                        "page_projection_canvas_active_public_key_pem": (
                            config.canvas_publication_active_public_key_pem
                        ),
                        "page_projection_canvas_previous_public_key_pems": (
                            config.canvas_publication_previous_public_key_pems
                        ),
                    }
                    if (
                        config.canvas_publication_active_key_id is not None
                        and config.canvas_publication_active_public_key_pem is not None
                    )
                    else {}
                ),
                "paused": True,
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=constraint_id,
            kind=RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=2,
            stale_after_seconds=30,
            settings={
                "minute_spool_root": str(minute_root),
                "reference_registry_path": str(reference_registry),
                "authority_root": str(root / "authorities" / "paper-execution"),
                "quote_ttl_seconds": 120,
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=broker_id,
            kind=RuntimeServiceKind.PAPER_BROKER,
            plane=RuntimeServicePlane.LIVE,
            interval_seconds=2,
            stale_after_seconds=30,
            settings={
                "account_id": "shadow-main",
                "execution_lag_seconds": 60,
                "buy_quantity": 100,
                "reduce_quantity": 100,
                "sell_quantity": 100,
                "signal_spool_root": str(signal_root / "spool"),
                "queue_path": str(broker_root / "queue.sqlite3"),
                "consumer_state_path": str(broker_root / "consumer.sqlite3"),
                "broker_path": str(broker_root / "broker.sqlite3"),
                "initial_cash": "100000",
                "execution_cost_spec": {
                    "schema_version": 3,
                    "cost_engine_version": "rquant-paper-cost-engine-v3",
                    "instrument_selectors": [
                        {
                            "selector_id": "cn-sse-a-share",
                            "market": "CN",
                            "exchange": "SSE",
                            "instrument_class": "EQUITY",
                            "security_class": "A_SHARE",
                        },
                        {
                            "selector_id": "cn-szse-a-share",
                            "market": "CN",
                            "exchange": "SZSE",
                            "instrument_class": "EQUITY",
                            "security_class": "A_SHARE",
                        },
                    ],
                    "commission_rules": [
                        {
                            "rule_id": "commission-cn-sse-a-share",
                            "selector_id": "cn-sse-a-share",
                            "rate_bps": "3",
                            "minimum_amount": "5",
                            "applies_to": "BOTH",
                        },
                        {
                            "rule_id": "commission-cn-szse-a-share",
                            "selector_id": "cn-szse-a-share",
                            "rate_bps": "3",
                            "minimum_amount": "5",
                            "applies_to": "BOTH",
                        },
                    ],
                    "transfer_fee_rules": [
                        {
                            "rule_id": "transfer-cn-sse-a-share",
                            "selector_id": "cn-sse-a-share",
                            "rate_bps": "0",
                            "minimum_amount": "0",
                            "applies_to": "BOTH",
                        },
                        {
                            "rule_id": "transfer-cn-szse-a-share",
                            "selector_id": "cn-szse-a-share",
                            "rate_bps": "0",
                            "minimum_amount": "0",
                            "applies_to": "BOTH",
                        },
                    ],
                    "stamp_duty_rules": [
                        {
                            "rule_id": "stamp-cn-sse-a-share",
                            "selector_id": "cn-sse-a-share",
                            "rate_bps": "10",
                            "minimum_amount": "0",
                            "applies_to": "SELL",
                        },
                        {
                            "rule_id": "stamp-cn-szse-a-share",
                            "selector_id": "cn-szse-a-share",
                            "rate_bps": "10",
                            "minimum_amount": "0",
                            "applies_to": "SELL",
                        },
                    ],
                    "fee_notional_basis": "EXECUTED_NOTIONAL",
                    "assessment_unit": "FILL",
                    "slippage": {
                        "owner": "shared_cost_engine",
                        "buy_bps": "5",
                        "sell_bps": "5",
                        "price_tick": "0.0001",
                        "price_rounding": "HALF_UP",
                    },
                    "money": {"quantum": "0.01", "rounding": "HALF_UP"},
                },
                "limit": 128,
                "raw_spool_root": str(minute_root),
                "trade_calendar_path": str(config.trade_calendar_path),
                "trade_calendar_sha256": config.trade_calendar_sha256,
                "execution_constraint_root": str(root / "authorities" / "paper-execution"),
                "timestamp_semantics": "bar_end",
                "quote_max_age_seconds": 90,
                "serving_authority_root": str(broker_root / "serving-authority"),
                "paused": False,
            },
        )
    )

    health_source_manifests = tuple(manifests)
    health_sources = [
        {
            "control_root": str(
                root
                / "control"
                / _control_bucket(manifest.service_kind)
                / _instance_name(manifest.service_id)
            ),
            "service_id": manifest.service_id,
            "plane": manifest.plane.value,
            "stale_after_seconds": manifest.stale_after_seconds,
            "producer_commit": config.producer_commit,
        }
        for manifest in health_source_manifests
    ]
    manifests.append(
        _manifest(
            config,
            service_id=lab_jobs_id,
            kind=RuntimeServiceKind.LAB_JOBS_PUBLISHER,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=30,
            stale_after_seconds=120,
            settings={
                "lab_jobs_path": str(root / "research" / "lab_jobs.sqlite3"),
                "research_metadata_path": str(root / "research" / "research_ro.duckdb"),
                "authority_root": str(root / "research" / "serving-authorities" / "lab-jobs"),
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=artifact_id,
            kind=RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=30,
            stale_after_seconds=120,
            settings={
                "research_root": str(root / "research"),
                "artifact_root": str(root / "research" / "final-artifacts"),
                "state_root": str(root / "research" / "artifact-catalogs" / artifact_instance),
                "lab_jobs_path": str(root / "research" / "lab_jobs.sqlite3"),
                "dataset_authority_path": str(root / "research" / "research_ro.duckdb"),
                "experiment_registry_path": str(root / "research" / "experiment_registry.sqlite3"),
                "location_id": config.artifact_location_id,
                "failure_domain": config.artifact_failure_domain,
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=promotions_id,
            kind=RuntimeServiceKind.PROMOTIONS_PUBLISHER,
            plane=RuntimeServicePlane.RESEARCH,
            interval_seconds=60,
            stale_after_seconds=180,
            settings={
                "experiment_registry_path": str(root / "research" / "experiment_registry.sqlite3"),
                "experiment_registry_managed_trust_root": str(root / "research"),
                "authority_root": str(root / "research" / "serving-authorities" / "promotions"),
            },
        )
    )
    for manifest in manifests[len(health_source_manifests) :]:
        health_sources.append(
            {
                "control_root": str(
                    root
                    / "control"
                    / _control_bucket(manifest.service_kind)
                    / _instance_name(manifest.service_id)
                ),
                "service_id": manifest.service_id,
                "plane": manifest.plane.value,
                "stale_after_seconds": manifest.stale_after_seconds,
                "producer_commit": config.producer_commit,
            }
        )
    manifests.append(
        _manifest(
            config,
            service_id=health_id,
            kind=RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
            plane=RuntimeServicePlane.SERVING,
            interval_seconds=10,
            stale_after_seconds=60,
            settings={
                "authority_root": str(root / "control" / "authority-runtime-health"),
                "sources": health_sources,
            },
        )
    )
    manifests.append(
        _manifest(
            config,
            service_id=serving_id,
            kind=RuntimeServiceKind.SERVING_PUBLISHER,
            plane=RuntimeServicePlane.SERVING,
            interval_seconds=30,
            stale_after_seconds=120,
            settings={
                "serving_root": str(root / "serving"),
                "schema_version": 3,
                "source_authorities": [
                    {
                        "dataset_id": "signals",
                        "root": str(notifier_root / "serving-authority"),
                    },
                    {
                        "dataset_id": "paper_accounts",
                        "root": str(broker_root / "serving-authority"),
                    },
                    {
                        "dataset_id": "runtime_health",
                        "root": str(root / "control" / "authority-runtime-health"),
                    },
                    {
                        "dataset_id": "lab_jobs",
                        "root": str(root / "research" / "serving-authorities" / "lab-jobs"),
                    },
                    {
                        "dataset_id": "promotions",
                        "root": str(root / "research" / "serving-authorities" / "promotions"),
                    },
                    {
                        "dataset_id": "reference_slow_authority",
                        "root": str(root / "live" / "reference-slow" / "serving-authority"),
                    },
                ],
            },
        )
    )

    capabilities = {manifest.service_id: () for manifest in manifests}
    capabilities[reference_source_id] = (
        "TUSHARE_TOKEN_MAIN",
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
        "RQ_REFERENCE_SOURCE_PRIVATE_KEY_BASE64",
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
    )
    for service_id in (auction_source_id, minute_source_id, daily_close_source_id):
        capabilities[service_id] = ("TUSHARE_TOKEN_MAIN",)
    capabilities[reference_publisher_id] = (
        "RQ_REFERENCE_PUBLICATION_HMAC_KEY_ID",
        "RQ_REFERENCE_PUBLICATION_HMAC_SECRET_HEX",
        "RQ_REFERENCE_SOURCE_SIGNING_KEY_ID",
        "RQ_REFERENCE_SOURCE_PUBLIC_KEY",
    )
    capabilities[notifier_id] = ("PUSHDEER_KEYS", "PUSHPLUS_TOKENS")
    capabilities[retention_id] = ("RQ_ARTIFACT_RETENTION_WRITER_CREDENTIAL",)
    schema_bundle = build_runtime_schema_contract_bundle(
        tuple(manifests),
        producer_commit=config.producer_commit,
    )
    schema_state_path = root / "control" / "schema-rollouts"
    schema_rollout_policies = tuple(
        RuntimeSchemaRolloutPolicy(
            channel_id=channel.channel_id,
            state_path=schema_state_path,
            stage_timeout_seconds=config.schema_rollout_stage_timeout_seconds,
            consumer_ack_max_age_seconds=(config.schema_consumer_ack_max_age_seconds),
            retire_observation_seconds=config.schema_retire_observation_seconds,
            required_consumers=tuple(
                binding.requirement.consumer_id for binding in channel.consumers
            ),
        )
        for channel in schema_bundle.channels
        if channel.producers and channel.consumers
    )
    profile = RuntimeDeploymentProfile(
        producer_commit=config.producer_commit,
        runtime_mode=config.runtime_mode,
        production_runtime_root=str(root),
        manifests=tuple(manifests),
        capability_environment=capabilities,
        schema_rollout_policies=schema_rollout_policies,
        schema_v1_migration_authority=config.schema_v1_migration_authority,
        page_control=PageControlRuntimeProfile(
            endpoint="http://127.0.0.1:8767/v1/commands",
            outbox_path=root / "control" / "page-control.sqlite3",
            data_dir=root / "serving" / "page-control",
            log_dir=root / "control" / "page-control-logs",
            page_projection_canvas_catalog_root=(root / "serving" / "page-control" / "canvases"),
            canvas_publication=(
                CanvasPublicationRuntimeProfile(
                    active_key_id=config.canvas_publication_active_key_id,
                    active_public_key_pem=config.canvas_publication_active_public_key_pem,
                    previous_public_key_pems=(config.canvas_publication_previous_public_key_pems),
                    signer_command=PRODUCTION_CANVAS_SIGNER_COMMAND,
                    consumer_service_id=PRODUCTION_PAGE_CONTROL_SERVICE_ID,
                    consumer_instance_id=PRODUCTION_PAGE_CONTROL_INSTANCE_ID,
                    timeout_seconds=config.canvas_publication_timeout_seconds,
                )
                if (
                    config.canvas_publication_active_key_id is not None
                    and config.canvas_publication_active_public_key_pem is not None
                )
                else None
            ),
        ),
        shadow=ShadowRuntimeProfile(
            completion_active_key_id=config.shadow_completion_active_key_id,
            completion_active_public_key_pem=config.shadow_completion_active_public_key_pem,
            completion_previous_public_key_pems=(config.shadow_completion_previous_public_key_pems),
            report_active_key_id=config.shadow_report_active_key_id,
            report_active_public_key_pem=config.shadow_report_active_public_key_pem,
            report_previous_public_key_pems=config.shadow_report_previous_public_key_pems,
            signer_command=PRODUCTION_SHADOW_SIGNER_COMMAND,
            report_producer_service_id=PRODUCTION_SHADOW_SERVICE_ID,
            report_producer_instance_id=PRODUCTION_SHADOW_INSTANCE_ID,
            timeout_seconds=config.shadow_signer_timeout_seconds,
        ),
        recovery=config.recovery,
        lab_highwater=LabHighWaterRuntimeProfile(
            authority_command=(
                "/usr/bin/sudo",
                "-n",
                "/usr/local/libexec/rquant-lab-highwater-authority",
            ),
            stable_identity=config.lab_highwater_stable_identity,
            trusted_keyring_path=config.lab_highwater_trusted_keyring_path,
            timeout_seconds=config.lab_highwater_timeout_seconds,
            allow_identity_rotation=config.lab_highwater_allow_identity_rotation,
            production_mode=True,
        ),
    )
    _validate_profile_strategy_bindings(
        profile,
        production_runtime_root=root,
    )
    return profile


def install_production_runtime_prerequisites(
    inputs: ProductionRuntimeProfileInputs,
) -> tuple[Path, ...]:
    """Install immutable authority generations required before service rollout."""

    config = _revalidate_production_inputs(inputs)
    definition_plan = plan_builtin_definitions(producer_commit=config.producer_commit)
    planned_bindings = tuple(
        ProductionStrategyBinding.model_validate(binding.model_dump(mode="python"))
        for binding in definition_plan.strategies
    )
    if config.strategies != planned_bindings:
        raise ValueError("production definition bindings do not match built-in executable content")
    calendar_authority = load_market_calendar_authority(
        config.market_calendar_authority_path,
        expected_commit=config.market_calendar_producer_commit,
    )
    if calendar_authority.content_sha256 != config.market_calendar_content_sha256:
        raise ValueError("production market calendar content identity changed")

    profile = build_production_runtime_profile(config)
    retention_manifests = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION
    )
    if len(retention_manifests) != 1:
        raise ValueError("production profile must contain exactly one retention owner")
    retention_settings = retention_manifests[0].settings
    retention_state_root = Path(str(retention_settings["state_root"]))
    retention_reference_store = Path(str(retention_settings["reference_store_path"]))
    retention_catalog_root = Path(str(retention_settings["catalog_authority_root"]))
    if retention_catalog_root != retention_state_root / "catalog-authority":
        raise ValueError("production retention catalog authority path is not profile-owned")

    calendar = install_market_calendar_generation(
        config.market_calendar_authority_path,
        runtime_root=config.runtime_root,
        expected_commit=config.market_calendar_producer_commit,
        expected_content_sha256=config.market_calendar_content_sha256,
    )
    bootstrap_builtin_definitions(
        config.definition_registry_root,
        producer_commit=config.producer_commit,
        registered_at=calendar_authority.generated_at,
        available_at=calendar_authority.generated_at,
        expected_plan_id=definition_plan.plan_id,
    )
    from rquant.artifact_retention_catalog_authority import (
        initialize_retention_catalog_authority,
    )

    authority = initialize_retention_catalog_authority(
        state_root=retention_state_root,
        reference_store_path=retention_reference_store,
        producer_commit=config.producer_commit,
    )
    return calendar, config.definition_registry_root, authority.current_receipt_path


def _require_absolute_normalized_path(path: Path, *, label: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"{label} path must be absolute and normalized")
    return candidate


@dataclass(frozen=True)
class _DirectoryHandle:
    descriptor: int
    parent_descriptor: int | None
    name: str
    identity: tuple[int, int, int, int]


def _directory_identity(observed: os.stat_result) -> tuple[int, int, int, int]:
    return observed.st_dev, observed.st_ino, observed.st_mode, observed.st_uid


def _close_directory_chain(chain: list[_DirectoryHandle]) -> None:
    for handle in reversed(chain):
        with suppress(OSError):
            os.close(handle.descriptor)


def _open_directory_chain(path: Path, *, label: str) -> list[_DirectoryHandle]:
    chain: list[_DirectoryHandle] = []
    try:
        anchor_fd = os.open(path.anchor, _DIRECTORY_FLAGS)
        anchor_stat = os.fstat(anchor_fd)
        chain.append(
            _DirectoryHandle(
                descriptor=anchor_fd,
                parent_descriptor=None,
                name=path.anchor,
                identity=_directory_identity(anchor_stat),
            )
        )
        for component in path.parts[1:]:
            parent_fd = chain[-1].descriptor
            descriptor = os.open(component, _DIRECTORY_FLAGS, dir_fd=parent_fd)
            try:
                opened = os.fstat(descriptor)
                active = os.stat(component, dir_fd=parent_fd, follow_symlinks=False)
                identity = _directory_identity(opened)
                if not stat.S_ISDIR(opened.st_mode) or _directory_identity(active) != identity:
                    raise ValueError(f"{label} parent changed while opening")
                chain.append(
                    _DirectoryHandle(
                        descriptor=descriptor,
                        parent_descriptor=parent_fd,
                        name=component,
                        identity=identity,
                    )
                )
                descriptor = -1
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
        _verify_directory_chain(chain, label=label)
        return chain
    except ValueError:
        _close_directory_chain(chain)
        raise
    except OSError as exc:
        _close_directory_chain(chain)
        raise ValueError(f"{label} parent is unavailable or unsafe") from exc


def _verify_directory_chain(chain: list[_DirectoryHandle], *, label: str) -> None:
    try:
        for handle in chain:
            if _directory_identity(os.fstat(handle.descriptor)) != handle.identity:
                raise ValueError(f"{label} parent changed after opening")
            if handle.parent_descriptor is None:
                continue
            active = os.stat(
                handle.name,
                dir_fd=handle.parent_descriptor,
                follow_symlinks=False,
            )
            if _directory_identity(active) != handle.identity:
                raise ValueError(f"{label} parent changed after opening")
    except OSError as exc:
        raise ValueError(f"{label} parent changed after opening") from exc


def _entry_stat(parent_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None


def _read_owned_regular_at(
    parent_fd: int,
    name: str,
    *,
    chain: list[_DirectoryHandle],
    label: str,
) -> bytes:
    descriptor, payload, _identity = _open_and_read_owned_regular_at(
        parent_fd,
        name,
        chain=chain,
        label=label,
    )
    os.close(descriptor)
    return payload


def _open_and_read_owned_regular_at(
    parent_fd: int,
    name: str,
    *,
    chain: list[_DirectoryHandle],
    label: str,
) -> tuple[int, bytes, tuple[int, ...]]:
    _verify_directory_chain(chain, label=label)
    descriptor = -1
    try:
        entry_before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        descriptor = os.open(name, _READ_FLAGS, dir_fd=parent_fd)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid():
            raise ValueError(f"{label} must be an owned regular file")
        if before.st_nlink != 1 or before.st_mode & 0o022:
            raise ValueError(f"{label} permissions are unsafe")
        if before.st_size <= 0 or before.st_size > _MAX_PRODUCTION_INPUT_BYTES:
            raise ValueError(f"{label} size is unsafe")
        if _file_identity(entry_before) != _file_identity(before):
            raise ValueError(f"{label} changed while opening")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        payload = b"".join(chunks)
        if (
            remaining
            or len(payload) != before.st_size
            or _file_identity(after) != _file_identity(before)
            or _file_identity(active) != _file_identity(before)
        ):
            raise ValueError(f"{label} changed while reading")
        _verify_directory_chain(chain, label=label)
        identity = _file_identity(before)
        result = descriptor, payload, identity
        descriptor = -1
        return result
    except OSError as exc:
        raise ValueError(f"{label} is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_bound_descriptor(
    descriptor: int,
    *,
    expected_size: int,
    label: str,
) -> bytes:
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        remaining = expected_size
        chunks: list[bytes] = []
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if remaining or len(payload) != expected_size:
            raise ValueError(f"{label} changed while reading")
        return payload
    except OSError as exc:
        raise ValueError(f"{label} changed while reading") from exc


def _verify_bound_regular_entry(
    parent_fd: int,
    name: str,
    descriptor: int,
    *,
    chain: list[_DirectoryHandle],
    label: str,
    expected_payload: bytes,
    expected_identity: tuple[int, ...] | None = None,
    expected_nlink: int,
) -> os.stat_result:
    _verify_directory_chain(chain, label=label)
    try:
        opened_before = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened_before.st_mode)
            or opened_before.st_uid != os.geteuid()
            or opened_before.st_nlink != expected_nlink
            or opened_before.st_size != len(expected_payload)
            or opened_before.st_mode & 0o077
            or (active.st_dev, active.st_ino) != (opened_before.st_dev, opened_before.st_ino)
        ):
            raise ValueError(f"{label} changed before publication completed")
        if expected_identity is not None and _file_identity(opened_before) != expected_identity:
            raise ValueError(f"{label} changed before publication completed")
        payload = _read_bound_descriptor(
            descriptor,
            expected_size=len(expected_payload),
            label=label,
        )
        opened_after = os.fstat(descriptor)
        active_after = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            payload != expected_payload
            or _file_identity(opened_after) != _file_identity(opened_before)
            or (active_after.st_dev, active_after.st_ino)
            != (opened_after.st_dev, opened_after.st_ino)
        ):
            raise ValueError(f"{label} changed before publication completed")
        _verify_directory_chain(chain, label=label)
        return opened_after
    except OSError as exc:
        raise ValueError(f"{label} changed before publication completed") from exc


def _unlink_bound_entry(
    parent_fd: int,
    name: str,
    descriptor: int,
    *,
    chain: list[_DirectoryHandle],
    label: str,
) -> None:
    _verify_directory_chain(chain, label=label)
    try:
        opened = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (active.st_dev, active.st_ino) != (opened.st_dev, opened.st_ino):
            raise ValueError(f"{label} changed before cleanup")
        os.unlink(name, dir_fd=parent_fd)
        _fsync_directory(parent_fd)
        _verify_directory_chain(chain, label=label)
    except OSError as exc:
        raise ValueError(f"{label} changed before cleanup") from exc


def _file_identity(observed: os.stat_result) -> tuple[int, ...]:
    return (
        observed.st_dev,
        observed.st_ino,
        observed.st_mode,
        observed.st_uid,
        observed.st_nlink,
        observed.st_size,
        observed.st_mtime_ns,
        observed.st_ctime_ns,
    )


def _read_owned_canonical_inputs(path: Path) -> bytes:
    candidate = _require_absolute_normalized_path(path, label="production profile inputs")
    chain = _open_directory_chain(candidate.parent, label="production profile inputs")
    try:
        return _read_owned_regular_at(
            chain[-1].descriptor,
            candidate.name,
            chain=chain,
            label="production profile inputs",
        )
    finally:
        _close_directory_chain(chain)


def load_production_runtime_profile_inputs(
    path: Path,
    *,
    expected_commit: str,
    expected_runtime_mode: RuntimeMode | None = None,
) -> ProductionRuntimeProfileInputs:
    """Load one canonical production input contract without following links."""

    if _COMMIT_PATTERN.fullmatch(expected_commit) is None:
        raise ValueError("expected production profile commit is invalid")
    payload = strict_canonical_json_loads(_read_owned_canonical_inputs(path))
    hydrated = _hydrate_daily_receipt_authority_from_fixed_keyring(payload)
    inputs = ProductionRuntimeProfileInputs.model_validate(
        hydrated,
        context={"daily_receipt_authority_hydrated": True},
    )
    if inputs.producer_commit != expected_commit:
        raise ValueError("production profile inputs commit mismatch")
    if expected_runtime_mode is not None and inputs.runtime_mode != expected_runtime_mode:
        raise ValueError("production profile inputs runtime mode mismatch")
    return inputs


def _fsync_directory(descriptor: int) -> None:
    os.fsync(descriptor)


def _create_private_payload_at(
    parent_fd: int,
    name: str,
    payload: bytes,
    *,
    chain: list[_DirectoryHandle],
    label: str,
) -> None:
    descriptor = -1
    try:
        _verify_directory_chain(chain, label=label)
        descriptor = os.open(
            name,
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o200,
            dir_fd=parent_fd,
        )
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("short production profile write")
            view = view[written:]
        os.fsync(descriptor)
        opened = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o200
            or _file_identity(active) != _file_identity(opened)
            or _read_bound_descriptor(
                descriptor,
                expected_size=len(payload),
                label=label,
            )
            != payload
        ):
            raise ValueError(f"{label} changed while writing")
        os.fchmod(descriptor, 0o600)
        os.fsync(descriptor)
        published = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            stat.S_IMODE(published.st_mode) != 0o600
            or published.st_nlink != 1
            or (active.st_dev, active.st_ino) != (published.st_dev, published.st_ino)
        ):
            raise ValueError(f"{label} changed while publishing")
        _verify_directory_chain(chain, label=label)
        _fsync_directory(parent_fd)
    except BaseException:
        if descriptor >= 0:
            with suppress(OSError):
                opened = os.fstat(descriptor)
                active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                if (active.st_dev, active.st_ino) == (opened.st_dev, opened.st_ino):
                    os.unlink(name, dir_fd=parent_fd)
                    _fsync_directory(parent_fd)
        raise
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _open_expected_profile_entry(
    parent_fd: int,
    name: str,
    *,
    chain: list[_DirectoryHandle],
    label: str,
    expected: RuntimeDeploymentProfile,
    expected_payload: bytes,
) -> tuple[int, tuple[int, ...]]:
    descriptor, payload, identity = _open_and_read_owned_regular_at(
        parent_fd,
        name,
        chain=chain,
        label=label,
    )
    try:
        observed = strict_model_validate_canonical_json(RuntimeDeploymentProfile, payload)
        if payload != expected_payload or observed != expected:
            raise ValueError(f"{label} content differs")
        if stat.S_IMODE(identity[2]) != 0o600:
            raise ValueError(f"{label} permissions are unsafe")
        return descriptor, identity
    except BaseException:
        os.close(descriptor)
        raise


def _recover_publication_alias(
    parent_fd: int,
    output_name: str,
    alias_name: str,
    *,
    chain: list[_DirectoryHandle],
    expected_payload: bytes,
) -> None:
    output_fd = -1
    alias_fd = -1
    label = "existing production runtime profile publication"
    try:
        _verify_directory_chain(chain, label=label)
        output_fd = os.open(output_name, _READ_FLAGS, dir_fd=parent_fd)
        alias_fd = os.open(alias_name, _READ_FLAGS, dir_fd=parent_fd)
        output_stat = os.fstat(output_fd)
        alias_stat = os.fstat(alias_fd)
        output_active = os.stat(output_name, dir_fd=parent_fd, follow_symlinks=False)
        alias_active = os.stat(alias_name, dir_fd=parent_fd, follow_symlinks=False)
        inode = (output_stat.st_dev, output_stat.st_ino)
        observed_payload = _read_bound_descriptor(
            output_fd,
            expected_size=len(expected_payload),
            label=label,
        )
        output_after = os.fstat(output_fd)
        alias_after = os.fstat(alias_fd)
        output_active_after = os.stat(
            output_name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        alias_active_after = os.stat(
            alias_name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        if (
            inode != (alias_stat.st_dev, alias_stat.st_ino)
            or inode != (output_active.st_dev, output_active.st_ino)
            or inode != (alias_active.st_dev, alias_active.st_ino)
            or not stat.S_ISREG(output_stat.st_mode)
            or output_stat.st_uid != os.geteuid()
            or output_stat.st_nlink != 2
            or output_stat.st_size != len(expected_payload)
            or output_stat.st_mode & 0o077
            or observed_payload != expected_payload
            or _file_identity(output_after) != _file_identity(output_stat)
            or _file_identity(alias_after) != _file_identity(alias_stat)
            or inode != (output_active_after.st_dev, output_active_after.st_ino)
            or inode != (alias_active_after.st_dev, alias_active_after.st_ino)
        ):
            raise ValueError(f"{label} is unsafe")
        os.unlink(alias_name, dir_fd=parent_fd)
        _fsync_directory(parent_fd)
        remaining = os.fstat(output_fd)
        output_active = os.stat(output_name, dir_fd=parent_fd, follow_symlinks=False)
        if remaining.st_nlink != 1 or (remaining.st_dev, remaining.st_ino) != (
            output_active.st_dev,
            output_active.st_ino,
        ):
            raise ValueError(f"{label} changed during recovery")
        _verify_directory_chain(chain, label=label)
    except OSError as exc:
        raise ValueError(f"{label} is unavailable or unsafe") from exc
    finally:
        if alias_fd >= 0:
            os.close(alias_fd)
        if output_fd >= 0:
            os.close(output_fd)


def _remove_incomplete_private_entry(
    parent_fd: int,
    name: str,
    *,
    chain: list[_DirectoryHandle],
    label: str,
) -> bool:
    descriptor = -1
    try:
        _verify_directory_chain(chain, label=label)
        descriptor = os.open(name, os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), dir_fd=parent_fd)
        opened = os.fstat(descriptor)
        active = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.geteuid()
            or opened.st_nlink != 1
            or stat.S_IMODE(opened.st_mode) != 0o200
            or opened.st_size > _MAX_PRODUCTION_INPUT_BYTES
            or (active.st_dev, active.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            return False
        os.unlink(name, dir_fd=parent_fd)
        _fsync_directory(parent_fd)
        _verify_directory_chain(chain, label=label)
        return True
    except OSError:
        return False
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _copy_bound_stage_to_output(
    parent_fd: int,
    output_name: str,
    stage_name: str,
    stage_descriptor: int,
    *,
    stage_identity: tuple[int, ...],
    chain: list[_DirectoryHandle],
    expected_payload: bytes,
) -> int:
    label = "production runtime profile publication"
    output_descriptor = -1
    try:
        _verify_bound_regular_entry(
            parent_fd,
            stage_name,
            stage_descriptor,
            chain=chain,
            label="production runtime profile stage",
            expected_payload=expected_payload,
            expected_identity=stage_identity,
            expected_nlink=1,
        )
        output_descriptor = os.open(
            output_name,
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_CLOEXEC", 0),
            0o200,
            dir_fd=parent_fd,
        )
        source_payload = _read_bound_descriptor(
            stage_descriptor,
            expected_size=len(expected_payload),
            label="production runtime profile stage",
        )
        if source_payload != expected_payload:
            raise ValueError("production runtime profile stage changed while copying")
        view = memoryview(source_payload)
        while view:
            written = os.write(output_descriptor, view)
            if written <= 0:
                raise OSError("short production profile write")
            view = view[written:]
        os.fsync(output_descriptor)
        unpublished = _verify_bound_regular_entry(
            parent_fd,
            output_name,
            output_descriptor,
            chain=chain,
            label=label,
            expected_payload=expected_payload,
            expected_nlink=1,
        )
        if stat.S_IMODE(unpublished.st_mode) != 0o200:
            raise ValueError("production runtime profile publication became visible early")
        _verify_bound_regular_entry(
            parent_fd,
            stage_name,
            stage_descriptor,
            chain=chain,
            label="production runtime profile stage",
            expected_payload=expected_payload,
            expected_identity=stage_identity,
            expected_nlink=1,
        )
        os.fchmod(output_descriptor, 0o600)
        os.fsync(output_descriptor)
        published = _verify_bound_regular_entry(
            parent_fd,
            output_name,
            output_descriptor,
            chain=chain,
            label=label,
            expected_payload=expected_payload,
            expected_nlink=1,
        )
        if stat.S_IMODE(published.st_mode) != 0o600:
            raise ValueError("production runtime profile publication permissions are unsafe")
        _fsync_directory(parent_fd)
        result = output_descriptor
        output_descriptor = -1
        return result
    except BaseException:
        if output_descriptor >= 0:
            with suppress(ValueError, FileNotFoundError):
                _unlink_bound_entry(
                    parent_fd,
                    output_name,
                    output_descriptor,
                    chain=chain,
                    label="failed production runtime profile publication",
                )
        raise
    finally:
        if output_descriptor >= 0:
            os.close(output_descriptor)


def publish_production_runtime_profile(
    profile: RuntimeDeploymentProfile,
    output_path: Path,
    *,
    production_runtime_root: Path,
) -> Path:
    """Publish one immutable content-addressed deployment profile."""

    validated = RuntimeDeploymentProfile.model_validate(profile)
    _validate_profile_strategy_bindings(
        validated,
        production_runtime_root=production_runtime_root,
    )
    if validated.profile_id is None:  # pragma: no cover - model invariant
        raise ValueError("production runtime profile id is missing")
    output = _require_absolute_normalized_path(
        output_path,
        label="production runtime profile output",
    )
    if output.name != f"{validated.profile_id}.json":
        raise ValueError("production runtime profile output must use its profile id")
    stage_name = f".{validated.profile_id}.stage"
    publication_name = f".{validated.profile_id}.publish"
    payload = canonical_model_json_bytes(validated)
    chain = _open_directory_chain(output.parent, label="production runtime profile output")
    directory_descriptor = chain[-1].descriptor
    stage_descriptor = -1
    output_descriptor = -1
    output_created = False
    directory_lock_acquired = False
    try:
        directory_stat = os.fstat(directory_descriptor)
        if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_uid != os.geteuid():
            raise ValueError("production runtime profile output parent must be owned")
        if directory_stat.st_mode & 0o022:
            raise ValueError("production runtime profile output parent permissions are unsafe")
        try:
            fcntl.flock(directory_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError(
                "production runtime profile publication is already in progress"
            ) from exc
        directory_lock_acquired = True

        output_stat = _entry_stat(directory_descriptor, output.name)
        for alias_name in (stage_name, publication_name):
            alias_stat = _entry_stat(directory_descriptor, alias_name)
            if output_stat is None or alias_stat is None:
                continue
            if (output_stat.st_dev, output_stat.st_ino) == (
                alias_stat.st_dev,
                alias_stat.st_ino,
            ):
                _recover_publication_alias(
                    directory_descriptor,
                    output.name,
                    alias_name,
                    chain=chain,
                    expected_payload=payload,
                )
                output_stat = _entry_stat(directory_descriptor, output.name)

        if output_stat is not None:
            existing_descriptor = -1
            try:
                existing_descriptor, _existing_identity = _open_expected_profile_entry(
                    directory_descriptor,
                    output.name,
                    chain=chain,
                    label="existing production runtime profile",
                    expected=validated,
                    expected_payload=payload,
                )
                for alias_name in (stage_name, publication_name):
                    if _entry_stat(directory_descriptor, alias_name) is None:
                        continue
                    alias_descriptor, alias_identity = _open_expected_profile_entry(
                        directory_descriptor,
                        alias_name,
                        chain=chain,
                        label="existing production runtime profile residue",
                        expected=validated,
                        expected_payload=payload,
                    )
                    try:
                        _verify_bound_regular_entry(
                            directory_descriptor,
                            alias_name,
                            alias_descriptor,
                            chain=chain,
                            label="existing production runtime profile residue",
                            expected_payload=payload,
                            expected_identity=alias_identity,
                            expected_nlink=1,
                        )
                        _unlink_bound_entry(
                            directory_descriptor,
                            alias_name,
                            alias_descriptor,
                            chain=chain,
                            label="existing production runtime profile residue",
                        )
                    finally:
                        os.close(alias_descriptor)
                _verify_bound_regular_entry(
                    directory_descriptor,
                    output.name,
                    existing_descriptor,
                    chain=chain,
                    label="existing production runtime profile",
                    expected_payload=payload,
                    expected_identity=_existing_identity,
                    expected_nlink=1,
                )
                return output
            except ValueError as exc:
                if not _remove_incomplete_private_entry(
                    directory_descriptor,
                    output.name,
                    chain=chain,
                    label="incomplete production runtime profile publication",
                ):
                    raise ValueError(
                        "existing production runtime profile is invalid or noncanonical"
                    ) from exc
                output_stat = None
            finally:
                if existing_descriptor >= 0:
                    os.close(existing_descriptor)

        stage_stat = _entry_stat(directory_descriptor, stage_name)
        if stage_stat is not None:
            try:
                stage_descriptor, stage_identity = _open_expected_profile_entry(
                    directory_descriptor,
                    stage_name,
                    chain=chain,
                    label="production runtime profile stage",
                    expected=validated,
                    expected_payload=payload,
                )
            except ValueError as exc:
                if not _remove_incomplete_private_entry(
                    directory_descriptor,
                    stage_name,
                    chain=chain,
                    label="incomplete production runtime profile stage",
                ):
                    raise ValueError("production runtime profile stage is invalid") from exc
                stage_stat = None
        if stage_stat is None:
            _create_private_payload_at(
                directory_descriptor,
                stage_name,
                payload,
                chain=chain,
                label="production runtime profile stage",
            )
            stage_descriptor, stage_identity = _open_expected_profile_entry(
                directory_descriptor,
                stage_name,
                chain=chain,
                label="production runtime profile stage",
                expected=validated,
                expected_payload=payload,
            )
        _verify_bound_regular_entry(
            directory_descriptor,
            stage_name,
            stage_descriptor,
            chain=chain,
            label="production runtime profile stage",
            expected_payload=payload,
            expected_identity=stage_identity,
            expected_nlink=1,
        )

        if _entry_stat(directory_descriptor, publication_name) is not None:
            legacy_descriptor, legacy_identity = _open_expected_profile_entry(
                directory_descriptor,
                publication_name,
                chain=chain,
                label="legacy production runtime profile publication residue",
                expected=validated,
                expected_payload=payload,
            )
            try:
                _verify_bound_regular_entry(
                    directory_descriptor,
                    publication_name,
                    legacy_descriptor,
                    chain=chain,
                    label="legacy production runtime profile publication residue",
                    expected_payload=payload,
                    expected_identity=legacy_identity,
                    expected_nlink=1,
                )
                _unlink_bound_entry(
                    directory_descriptor,
                    publication_name,
                    legacy_descriptor,
                    chain=chain,
                    label="legacy production runtime profile publication residue",
                )
            finally:
                os.close(legacy_descriptor)

        output_descriptor = _copy_bound_stage_to_output(
            directory_descriptor,
            output.name,
            stage_name,
            stage_descriptor,
            stage_identity=stage_identity,
            chain=chain,
            expected_payload=payload,
        )
        output_created = True
        _verify_bound_regular_entry(
            directory_descriptor,
            stage_name,
            stage_descriptor,
            chain=chain,
            label="production runtime profile stage",
            expected_payload=payload,
            expected_identity=stage_identity,
            expected_nlink=1,
        )
        _unlink_bound_entry(
            directory_descriptor,
            stage_name,
            stage_descriptor,
            chain=chain,
            label="production runtime profile stage",
        )
        final_stat = _verify_bound_regular_entry(
            directory_descriptor,
            output.name,
            output_descriptor,
            chain=chain,
            label="production runtime profile publication",
            expected_payload=payload,
            expected_nlink=1,
        )
        if stat.S_IMODE(final_stat.st_mode) != 0o600:
            raise ValueError("production runtime profile publication permissions are unsafe")
        output_created = False
        return output
    except BaseException as exc:
        if output_created and output_descriptor >= 0:
            with suppress(ValueError, FileNotFoundError):
                _unlink_bound_entry(
                    directory_descriptor,
                    output.name,
                    output_descriptor,
                    chain=chain,
                    label="failed production runtime profile publication",
                )
            output_created = False
        if isinstance(exc, OSError):
            raise ValueError("production runtime profile publication failed safely") from exc
        raise
    finally:
        if output_descriptor >= 0:
            os.close(output_descriptor)
        if stage_descriptor >= 0:
            os.close(stage_descriptor)
        if directory_lock_acquired:
            fcntl.flock(directory_descriptor, fcntl.LOCK_UN)
        _close_directory_chain(chain)


__all__ = [
    "ProductionRuntimeProfileInputs",
    "ProductionStrategyBinding",
    "build_production_runtime_profile",
    "install_production_runtime_prerequisites",
    "load_production_runtime_profile_inputs",
    "publish_production_runtime_profile",
]
