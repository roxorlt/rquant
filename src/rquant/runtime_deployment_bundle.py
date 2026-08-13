"""Atomic, least-privilege deployment bundles for isolated runtime services."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
import time
from base64 import b64encode, urlsafe_b64encode
from collections.abc import Mapping
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Literal

from pydantic import StringConstraints, field_serializer, field_validator, model_validator

from rquant.runtime_artifact_terminal_lifecycle import artifact_retention_state_root
from rquant.runtime_capabilities import (
    CAPABILITY_KEYS,
    SECRET_CAPABILITY_KEYS,
    serialize_runtime_capabilities,
    serialize_runtime_credential,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.runtime_credential_sealer_client import (
    recover_runtime_credentials as _recover_runtime_credentials,
)
from rquant.runtime_credential_sealer_client import (
    seal_runtime_credentials as _seal_runtime_credentials,
)
from rquant.runtime_schema_registry import (
    RuntimeSchemaCompatibilityError,
    RuntimeSchemaConsumerAckBinding,
    RuntimeSchemaContractBundle,
    RuntimeSchemaDualWriteBinding,
    RuntimeSchemaServiceBinding,
    RuntimeSchemaV1LifecycleReview,
    RuntimeSchemaV1MigrationAudit,
    build_runtime_schema_rollout,
    build_runtime_schema_v1_migration_audit,
    parse_runtime_schema_contract_bundle,
)
from rquant.runtime_schema_registry import (
    build_runtime_schema_contract_bundle as _build_runtime_schema_contract_bundle,
)
from rquant.runtime_schema_registry import (
    validate_runtime_schema_transition as _validate_runtime_schema_transition,
)
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import (
    RuntimeServiceKind,
    RuntimeServiceManifest,
)
from rquant.schema_compatibility import (
    ConsumerCapabilityReceipt,
    LiveSchemaRolloutPlan,
    ProductionConsumerRegistry,
    RolloutPhase,
    SchemaRolloutState,
    SchemaRolloutStore,
)

CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
GenerationHash = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
InstanceName = Annotated[
    str,
    StringConstraints(pattern=r"^svc-[0-9a-f]{64}$"),
]
SystemdUnitName = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^rquant-runtime-(?:reference-slow-source|reference-slow-publisher|auction-universe|auction-match|market-minute|watchlist-quote|daily-close|daily-orchestrator|shadow|feature|serving|research|candidate|strategy|"
            r"signal-router|notifier|paper-broker|paper-constraint|runtime-health|lab-jobs|"
            r"artifact-catalog|promotions)@svc-[0-9a-f]{64}\.service$|"
            r"^rquant-artifact-retention\.service$"
        )
    ),
]

_COMMIT_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_GENERATION_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_LIVE_KINDS = frozenset(
    {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        RuntimeServiceKind.CANDIDATE_PUBLISHER,
        RuntimeServiceKind.FEATURE_LIVE,
        RuntimeServiceKind.STRATEGY_LIVE,
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        RuntimeServiceKind.PAPER_BROKER,
    }
)
_EXPECTED_PLANE = {
    **{kind: RuntimeServicePlane.LIVE for kind in _LIVE_KINDS},
    RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: RuntimeServicePlane.SERVING,
    RuntimeServiceKind.LAB_JOBS_PUBLISHER: RuntimeServicePlane.RESEARCH,
    RuntimeServiceKind.LAB_ARTIFACT_CATALOG: RuntimeServicePlane.RESEARCH,
    RuntimeServiceKind.ARTIFACT_RETENTION: RuntimeServicePlane.RESEARCH,
    RuntimeServiceKind.PROMOTIONS_PUBLISHER: RuntimeServicePlane.RESEARCH,
    RuntimeServiceKind.SERVING_PUBLISHER: RuntimeServicePlane.SERVING,
    RuntimeServiceKind.SHADOW_SESSION: RuntimeServicePlane.RESEARCH,
    RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: RuntimeServicePlane.RESEARCH,
}
_WRITABLE_PATH_SETTINGS = {
    RuntimeServiceKind.REFERENCE_SLOW_SOURCE: ("spool_root", "quota_path"),
    RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: (),
    RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: (),
    RuntimeServiceKind.AUCTION_MATCH_SOURCE: ("spool_root", "quota_path"),
    RuntimeServiceKind.MARKET_MINUTE_SOURCE: ("spool_root", "quota_path"),
    RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: ("spool_root", "quota_path"),
    RuntimeServiceKind.DAILY_CLOSE_SOURCE: ("spool_root",),
    RuntimeServiceKind.SHADOW_SESSION: ("report_root",),
    RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: ("storage_root",),
    RuntimeServiceKind.CANDIDATE_PUBLISHER: ("snapshot_root",),
    RuntimeServiceKind.FEATURE_LIVE: ("feature_spool_root",),
    RuntimeServiceKind.STRATEGY_LIVE: ("runner_state_path",),
    RuntimeServiceKind.SIGNAL_ROUTER: ("signal_bus_path", "signal_spool_root"),
    RuntimeServiceKind.NOTIFIER: (
        "notification_state_path",
        "serving_authority_root",
    ),
    RuntimeServiceKind.PAPER_CONSUMER: (
        "signal_bus_path",
        "queue_path",
        "consumer_state_path",
    ),
    RuntimeServiceKind.PAPER_BROKER: (
        "queue_path",
        "consumer_state_path",
        "broker_path",
        "serving_authority_root",
    ),
    RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: (),
    RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: (),
    RuntimeServiceKind.LAB_JOBS_PUBLISHER: ("authority_root",),
    RuntimeServiceKind.LAB_ARTIFACT_CATALOG: ("state_root",),
    RuntimeServiceKind.ARTIFACT_RETENTION: ("state_root", "managed_root"),
    RuntimeServiceKind.PROMOTIONS_PUBLISHER: ("authority_root",),
    RuntimeServiceKind.SERVING_PUBLISHER: ("serving_root",),
}
_READONLY_PATH_SETTINGS = {
    RuntimeServiceKind.REFERENCE_SLOW_SOURCE: (
        "database_path",
        "calendar_path",
    ),
    RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: ("calendar_path", "spool_root"),
    RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: (
        "database_path",
        "calendar_path",
    ),
    RuntimeServiceKind.AUCTION_MATCH_SOURCE: ("calendar_path", "universe_path"),
    RuntimeServiceKind.MARKET_MINUTE_SOURCE: ("calendar_path",),
    RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: ("calendar_path",),
    RuntimeServiceKind.DAILY_CLOSE_SOURCE: ("calendar_path",),
    RuntimeServiceKind.SHADOW_SESSION: (
        "calendar_path",
        "legacy_monitor_root",
        "legacy_surge_root",
        "isolated_runner_root",
    ),
    RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: (),
    RuntimeServiceKind.FEATURE_LIVE: (
        "raw_spool_root",
        "historical_minutes_snapshot_path",
    ),
    RuntimeServiceKind.CANDIDATE_PUBLISHER: (
        "candidate_input_path",
        "auction_spool_root",
        "daily_database_path",
        "reference_registry_path",
        "calendar_path",
    ),
    RuntimeServiceKind.STRATEGY_LIVE: (
        "feature_spool_root",
        "definition_registry_root",
        "candidate_snapshot_root",
        "paper_broker_path",
        "calendar_path",
        "signal_bus_path",
    ),
    RuntimeServiceKind.SIGNAL_ROUTER: (
        "runner_state_path",
        "routing_policy_path",
    ),
    RuntimeServiceKind.NOTIFIER: ("signal_spool_root",),
    RuntimeServiceKind.PAPER_BROKER: (
        "signal_spool_root",
        "raw_spool_root",
        "trade_calendar_path",
        "execution_constraint_root",
    ),
    RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: (
        "minute_spool_root",
        "reference_registry_path",
    ),
    RuntimeServiceKind.LAB_JOBS_PUBLISHER: ("lab_jobs_path",),
    RuntimeServiceKind.LAB_ARTIFACT_CATALOG: (
        "artifact_root",
        "lab_jobs_path",
        "dataset_authority_path",
        "experiment_registry_path",
    ),
    RuntimeServiceKind.ARTIFACT_RETENTION: (
        "schema_authority_root",
        "schema_authority_path",
    ),
    RuntimeServiceKind.PROMOTIONS_PUBLISHER: ("experiment_registry_path",),
}
_DEDICATED_NO_CAPABILITY_KINDS = frozenset(
    {
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        RuntimeServiceKind.CANDIDATE_PUBLISHER,
        RuntimeServiceKind.FEATURE_LIVE,
        RuntimeServiceKind.STRATEGY_LIVE,
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        RuntimeServiceKind.PAPER_BROKER,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
    }
)
_MANIFEST_V2_KINDS = frozenset(
    {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        RuntimeServiceKind.SHADOW_SESSION,
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
        RuntimeServiceKind.SIGNAL_ROUTER,
        RuntimeServiceKind.NOTIFIER,
        RuntimeServiceKind.PAPER_BROKER,
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER,
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER,
        RuntimeServiceKind.LAB_JOBS_PUBLISHER,
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG,
        RuntimeServiceKind.PROMOTIONS_PUBLISHER,
    }
)
_SERVING_SOURCE_OWNER_SERVICE_IDS = {
    "signals": "notifier.admin.shadow.v1",
    "paper_accounts": "paper-broker.shadow-main.v1",
    "runtime_health": "runtime-health.all.v1",
    "lab_jobs": "lab-jobs.serving.v1",
    "promotions": "promotions.serving.v1",
    "reference_slow_authority": "reference-slow.publisher.v1",
}


def _serving_source_owner_root(runtime_root: Path, dataset_id: str) -> Path | None:
    owner_service_id = _SERVING_SOURCE_OWNER_SERVICE_IDS.get(dataset_id)
    if owner_service_id is None:
        return None
    if dataset_id == "signals":
        return (
            runtime_root
            / "live"
            / "notifications"
            / _instance_name(owner_service_id)
            / "serving-authority"
        )
    if dataset_id == "paper_accounts":
        return (
            runtime_root
            / "live"
            / "paper-brokers"
            / _instance_name(owner_service_id)
            / "serving-authority"
        )
    if dataset_id == "runtime_health":
        return runtime_root / "control" / "authority-runtime-health"
    if dataset_id == "lab_jobs":
        return runtime_root / "research" / "serving-authorities" / "lab-jobs"
    if dataset_id == "promotions":
        return runtime_root / "research" / "serving-authorities" / "promotions"
    if dataset_id == "reference_slow_authority":
        return runtime_root / "live" / "reference-slow" / "serving-authority"
    return None  # pragma: no cover - exhaustive contract map


_RUNTIME_HEALTH_SOURCE_BUCKETS = frozenset(
    {
        "candidates",
        "reference-slow-sources",
        "reference-slow-publishers",
        "auction-universe-publishers",
        "auction-match-sources",
        "features",
        "market-minute-sources",
        "watchlist-quote-sources",
        "daily-close-sources",
        "daily-orchestrators",
        "shadow-sessions",
        "notifiers",
        "paper-brokers",
        "paper-constraints",
        "promotions-publishers",
        "lab-jobs-publishers",
        "artifact-catalogs",
        "artifact-retention",
        "signal-routers",
        "strategies",
    }
)


class RuntimeDeploymentReceipt(RuntimeContractModel):
    runtime_root: Path
    producer_commit: CommitSha
    generation_hash: GenerationHash
    deployment_profile_id: GenerationHash | None = None
    previous_generation_hash: GenerationHash | None = None
    schema_rollout_plan_ids: tuple[GenerationHash, ...] = ()
    instance_mapping: Mapping[str, InstanceName]
    unit_mapping: Mapping[str, SystemdUnitName]

    @field_validator("instance_mapping")
    @classmethod
    def freeze_instance_mapping(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if len(value) != len(set(value.values())):
            raise ValueError("runtime instance names must be unique")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("instance_mapping")
    def serialize_instance_mapping(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @field_validator("unit_mapping")
    @classmethod
    def freeze_unit_mapping(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if len(value) != len(set(value.values())):
            raise ValueError("runtime systemd unit names must be unique")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("unit_mapping")
    def serialize_unit_mapping(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_mapping_keys(self) -> RuntimeDeploymentReceipt:
        if set(self.instance_mapping) != set(self.unit_mapping):
            raise ValueError("runtime instance and unit mappings must have identical service ids")
        if tuple(sorted(set(self.schema_rollout_plan_ids))) != self.schema_rollout_plan_ids:
            raise ValueError("runtime schema rollout plan ids must be unique and sorted")
        return self


class _RuntimeGenerationBasis(RuntimeContractModel):
    schema_version: Literal[1]
    producer_commit: CommitSha
    deployment_profile_id: GenerationHash | None = None
    manifest_sha256: Mapping[str, GenerationHash]
    capability_sha256: Mapping[str, GenerationHash]
    instance_mapping: Mapping[str, InstanceName]
    unit_mapping: Mapping[str, SystemdUnitName]
    schema_contract_sha256: GenerationHash
    schema_bootstrap_sha256: GenerationHash | None

    @field_validator(
        "manifest_sha256",
        "capability_sha256",
        "instance_mapping",
        "unit_mapping",
    )
    @classmethod
    def freeze_mappings(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if any(not key for key in value):
            raise ValueError("runtime generation basis keys cannot be empty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer(
        "manifest_sha256",
        "capability_sha256",
        "instance_mapping",
        "unit_mapping",
    )
    def serialize_mappings(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)

    @model_validator(mode="after")
    def validate_mappings(self) -> _RuntimeGenerationBasis:
        service_ids = set(self.manifest_sha256)
        if service_ids != set(self.instance_mapping) or service_ids != set(self.unit_mapping):
            raise ValueError("runtime generation basis service mappings do not match")
        if not set(self.capability_sha256) <= set(self.instance_mapping.values()):
            raise ValueError("runtime generation basis capability instance is unknown")
        return self


class RuntimeDeploymentRollbackError(RuntimeError):
    """A deployment failed and at least one authoritative pointer could not be restored."""


class RuntimeDeploymentRecoveryError(RuntimeError):
    """A prior deployment could not be reconciled, so runtime visibility was removed."""


class RuntimeSchemaBootstrapRequiredError(RuntimeError):
    """The first runtime schema registry install lacks an explicit audit reason."""


class RuntimeSchemaV1MigrationAuthorization(RuntimeContractModel):
    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
    reviewed_lifecycles: tuple[RuntimeSchemaV1LifecycleReview, ...]
    migrated_at: AwareUtcDatetime


class RuntimeSchemaRolloutAuthority(RuntimeContractModel):
    schema_version: Literal[1]
    previous_generation_id: GenerationHash
    target_generation_id: GenerationHash
    previous_bundle_content_hash: GenerationHash
    target_bundle_content_hash: GenerationHash
    plan: LiveSchemaRolloutPlan
    registry: ProductionConsumerRegistry
    content_hash: GenerationHash

    @property
    def plan_id(self) -> str:
        return self.plan.plan_id

    @model_validator(mode="after")
    def validate_identity(self) -> RuntimeSchemaRolloutAuthority:
        if self.plan.target_generation_id != self.target_generation_id:
            raise ValueError("schema rollout authority target generation mismatch")
        if self.plan.production_consumer_registry_fingerprint != self.registry.registry_fingerprint:
            raise ValueError("schema rollout authority registry mismatch")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"content_hash"}))
        if self.content_hash != expected:
            raise ValueError("schema rollout authority content hash mismatch")
        return self

    @classmethod
    def create(
        cls,
        *,
        previous_generation_id: str,
        target_generation_id: str,
        previous_bundle: RuntimeSchemaContractBundle,
        target_bundle: RuntimeSchemaContractBundle,
        plan: LiveSchemaRolloutPlan,
        registry: ProductionConsumerRegistry,
    ) -> RuntimeSchemaRolloutAuthority:
        values = {
            "schema_version": 1,
            "previous_generation_id": previous_generation_id,
            "target_generation_id": target_generation_id,
            "previous_bundle_content_hash": previous_bundle.content_hash,
            "target_bundle_content_hash": target_bundle.content_hash,
            "plan": plan,
            "registry": registry,
        }
        return cls(**values, content_hash=canonical_sha256(values))


def _instance_name(service_id: str) -> str:
    digest = hashlib.sha256(service_id.encode("utf-8")).hexdigest()
    return f"svc-{digest}"


_STRATEGY_COMPLETION_AUTHORITY_FIELDS = (
    "calendar_path",
    "calendar_expected_commit",
    "calendar_content_sha256",
    "signal_bus_path",
    "routing_policy_fingerprint",
    "strategy_spec_fingerprint",
    "evaluator_contract_fingerprint",
    "producer_instance_id",
    "producer_version",
)


def strategy_live_producer_version(
    *,
    service_id: str,
    strategy_version: int,
    producer_commit: str,
) -> str:
    """Return the immutable release identity admitted for one stable strategy instance."""

    if not service_id:
        raise ValueError("strategy producer service_id cannot be empty")
    if (
        not isinstance(strategy_version, int)
        or isinstance(strategy_version, bool)
        or strategy_version < 1
    ):
        raise ValueError("strategy producer strategy_version must be positive")
    if _COMMIT_PATTERN.fullmatch(producer_commit) is None:
        raise ValueError("strategy producer commit must be a full lowercase Git SHA")
    return f"strategy-live/{service_id}/strategy-v{strategy_version}/commit-{producer_commit}"


def validate_strategy_live_completion_manifest(
    manifest: RuntimeServiceManifest,
    *,
    runtime_root: Path,
) -> None:
    """Validate the hash-bound completion authority used by Shadow evidence."""

    if manifest.service_kind is not RuntimeServiceKind.STRATEGY_LIVE:
        raise ValueError("strategy completion validation requires a strategy_live manifest")
    settings = manifest.settings
    missing = tuple(
        field for field in _STRATEGY_COMPLETION_AUTHORITY_FIELDS if field not in settings
    )
    if missing:
        raise ValueError("strategy completion authority is missing " + ", ".join(missing))
    for field in (
        "strategy_spec_fingerprint",
        "evaluator_contract_fingerprint",
    ):
        value = settings[field]
        if not isinstance(value, str) or _GENERATION_PATTERN.fullmatch(value) is None:
            raise ValueError(f"{field} must be a full lowercase SHA-256")
    expected_calendar = _expected_market_calendar_generation(
        manifest,
        runtime_root=runtime_root,
    )
    if Path(str(settings["calendar_path"])) != expected_calendar:
        raise ValueError("calendar_path must equal the trusted market calendar generation")
    if _COMMIT_PATTERN.fullmatch(str(settings["calendar_expected_commit"])) is None:
        raise ValueError("calendar_expected_commit must be a full lowercase Git SHA")
    if _GENERATION_PATTERN.fullmatch(str(settings["calendar_content_sha256"])) is None:
        raise ValueError("calendar_content_sha256 must be a full lowercase SHA-256")
    expected_signal_bus = runtime_root / "live" / "signal-bus" / "signal_bus.sqlite3"
    if Path(str(settings["signal_bus_path"])) != expected_signal_bus:
        raise ValueError("signal_bus_path must equal the shared isolated signal bus path")
    if _GENERATION_PATTERN.fullmatch(str(settings["routing_policy_fingerprint"])) is None:
        raise ValueError("routing_policy_fingerprint must be a full lowercase SHA-256")
    expected_instance = _instance_name(manifest.service_id)
    if settings["producer_instance_id"] != expected_instance:
        raise ValueError("producer_instance_id must equal the stable service instance")
    expected_version = strategy_live_producer_version(
        service_id=manifest.service_id,
        strategy_version=settings.get("strategy_version"),  # type: ignore[arg-type]
        producer_commit=manifest.producer_commit,
    )
    if settings["producer_version"] != expected_version:
        raise ValueError("producer_version must bind the immutable strategy release")


def _systemd_unit_name(manifest: RuntimeServiceManifest, instance: str) -> str:
    dedicated_templates = {
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE: "reference-slow-source",
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER: "reference-slow-publisher",
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER: "auction-universe",
        RuntimeServiceKind.AUCTION_MATCH_SOURCE: "auction-match",
        RuntimeServiceKind.MARKET_MINUTE_SOURCE: "market-minute",
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE: "watchlist-quote",
        RuntimeServiceKind.DAILY_CLOSE_SOURCE: "daily-close",
        RuntimeServiceKind.SHADOW_SESSION: "shadow",
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR: "daily-orchestrator",
        RuntimeServiceKind.FEATURE_LIVE: "feature",
        RuntimeServiceKind.CANDIDATE_PUBLISHER: "candidate",
        RuntimeServiceKind.STRATEGY_LIVE: "strategy",
        RuntimeServiceKind.SIGNAL_ROUTER: "signal-router",
        RuntimeServiceKind.NOTIFIER: "notifier",
        RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER: "paper-constraint",
        RuntimeServiceKind.PAPER_BROKER: "paper-broker",
        RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER: "runtime-health",
        RuntimeServiceKind.LAB_JOBS_PUBLISHER: "lab-jobs",
        RuntimeServiceKind.LAB_ARTIFACT_CATALOG: "artifact-catalog",
        RuntimeServiceKind.ARTIFACT_RETENTION: "artifact-retention",
        RuntimeServiceKind.PROMOTIONS_PUBLISHER: "promotions",
    }
    if manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
        return "rquant-artifact-retention.service"
    template = dedicated_templates.get(manifest.service_kind, manifest.plane.value)
    return f"rquant-runtime-{template}@{instance}.service"


def _canonical_manifest(manifest: RuntimeServiceManifest) -> tuple[RuntimeServiceManifest, bytes]:
    if not isinstance(manifest, RuntimeServiceManifest):
        raise TypeError("manifests must contain RuntimeServiceManifest values")
    payload = json.dumps(
        manifest.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    try:
        validated = RuntimeServiceManifest.model_validate_json(payload)
    except ValueError as exc:
        raise ValueError("runtime service manifest is invalid") from exc
    return validated, payload


def _absolute_runtime_root(root: Path) -> Path:
    candidate = Path(root)
    if not candidate.is_absolute():
        raise ValueError("runtime root must be absolute")
    normalized = Path(os.path.abspath(candidate))
    if candidate != normalized:
        raise ValueError("runtime root must not contain path traversal")
    current = Path(candidate.anchor)
    for component in candidate.parts[1:]:
        current /= component
        try:
            observed = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"runtime root contains a symlink parent: {current}")
    return candidate


def _ensure_owned_directory(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    observed = path.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
    ):
        raise ValueError(f"runtime deployment directory is not safely owned: {path}")
    path.chmod(0o700)


def _ensure_owned_descendant(root: Path, path: Path) -> None:
    relative = path.relative_to(root)
    current = root
    for component in relative.parts:
        current /= component
        current.mkdir(mode=0o700, exist_ok=True)
        observed = current.lstat()
        if (
            not stat.S_ISDIR(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != os.getuid()
        ):
            raise ValueError(f"runtime deployment directory is not safely owned: {current}")
        current.chmod(0o700)


def _require_owned_plane_path(
    value: object,
    *,
    runtime_root: Path,
    plane: RuntimeServicePlane,
    setting_name: str,
) -> None:
    if not isinstance(value, str):
        raise ValueError(f"runtime path setting {setting_name} must be a string")
    candidate = Path(value)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"runtime path setting {setting_name} must be absolute and normalized")
    owner_root = runtime_root / plane.value
    try:
        relative = candidate.relative_to(owner_root)
    except ValueError as exc:
        raise ValueError(
            f"runtime path setting {setting_name} must be owned by the {plane.value} plane"
        ) from exc
    current = runtime_root
    for component in (plane.value, *relative.parts):
        current /= component
        try:
            observed = current.lstat()
        except FileNotFoundError:
            break
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"runtime path setting {setting_name} contains a symlink: {current}")


def _require_external_readonly_path(
    value: object,
    *,
    writable_roots: Mapping[str, Path],
    setting_name: str,
) -> None:
    if not isinstance(value, str):
        raise ValueError(f"runtime path setting {setting_name} must be a string")
    candidate = Path(value)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError(f"runtime path setting {setting_name} must be absolute and normalized")
    current = Path(candidate.anchor)
    for component in candidate.parts[1:]:
        current /= component
        try:
            observed = current.lstat()
        except FileNotFoundError:
            break
        if stat.S_ISLNK(observed.st_mode):
            raise ValueError(f"runtime path setting {setting_name} contains a symlink: {current}")
    for root_name, writable_root in writable_roots.items():
        try:
            candidate.relative_to(writable_root)
        except ValueError:
            pass
        else:
            raise ValueError(
                f"read-only runtime path setting {setting_name} must not be inside "
                f"the {root_name} writable owner root"
            )
        try:
            writable_root.relative_to(candidate)
        except ValueError:
            pass
        else:
            raise ValueError(
                f"read-only runtime path setting {setting_name} must not contain "
                f"the {root_name} writable owner root"
            )


def _expected_market_calendar_generation(
    manifest: RuntimeServiceManifest,
    *,
    runtime_root: Path,
) -> Path:
    expected_commit = manifest.settings.get("calendar_expected_commit")
    content_sha256 = manifest.settings.get("calendar_content_sha256")
    if not isinstance(expected_commit, str) or _COMMIT_PATTERN.fullmatch(expected_commit) is None:
        raise ValueError("calendar_expected_commit must be a lowercase 40-character Git SHA")
    if not isinstance(content_sha256, str) or _GENERATION_PATTERN.fullmatch(content_sha256) is None:
        raise ValueError("calendar_content_sha256 must be a lowercase SHA-256")
    return (
        runtime_root / "authorities" / "market-calendar" / "generations" / f"{content_sha256}.json"
    )


def _validate_manifest_authority(
    manifest: RuntimeServiceManifest,
    *,
    producer_commit: str,
    runtime_root: Path,
) -> None:
    if manifest.service_kind is RuntimeServiceKind.PAPER_CONSUMER:
        raise ValueError(
            "paper consumer is an internal component of the single-writer paper broker"
        )
    if manifest.service_kind in _MANIFEST_V2_KINDS and manifest.schema_version != 2:
        raise ValueError(f"runtime service {manifest.service_id} requires manifest schema v2")
    if manifest.producer_commit != producer_commit:
        raise ValueError(
            f"runtime manifest {manifest.service_id} producer commit does not match bundle"
        )
    expected_plane = _EXPECTED_PLANE[manifest.service_kind]
    if manifest.plane is not expected_plane:
        raise ValueError(
            f"runtime service {manifest.service_id} must use the {expected_plane.value} plane"
        )
    for setting_name in _WRITABLE_PATH_SETTINGS[manifest.service_kind]:
        if setting_name not in manifest.settings:
            continue
        _require_owned_plane_path(
            manifest.settings[setting_name],
            runtime_root=runtime_root,
            plane=manifest.plane,
            setting_name=setting_name,
        )
    instance = _instance_name(manifest.service_id)
    if manifest.service_kind is RuntimeServiceKind.REFERENCE_SLOW_SOURCE:
        readonly_exclusions = {
            "reference slow source": runtime_root / "live" / "reference-slow",
            "reference slow source control": (
                runtime_root / "control" / "reference-slow-sources" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER:
        readonly_exclusions = {
            "reference slow authority": runtime_root / "authorities" / "reference-slow",
            "reference slow control": (
                runtime_root / "control" / "reference-slow-publishers" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER:
        readonly_exclusions = {
            "auction universe authority": (runtime_root / "authorities" / "auction-universe"),
            "auction universe control": (
                runtime_root / "control" / "auction-universe-publishers" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.AUCTION_MATCH_SOURCE:
        readonly_exclusions = {
            "auction match state": runtime_root / "live" / "auction-match",
            "auction match control": (
                runtime_root / "control" / "auction-match-sources" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
        readonly_exclusions = {
            "market minute state": runtime_root / "live" / "market-minute",
            "market minute control": (
                runtime_root / "control" / "market-minute-sources" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE:
        readonly_exclusions = {
            "watchlist quote state": runtime_root / "live" / "watchlist-quote",
            "watchlist quote control": (
                runtime_root / "control" / "watchlist-quote-sources" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.DAILY_CLOSE_SOURCE:
        readonly_exclusions = {
            "daily close state": runtime_root / "live" / "daily-close",
            "daily close control": (runtime_root / "control" / "daily-close-sources" / instance),
        }
    elif manifest.service_kind is RuntimeServiceKind.SHADOW_SESSION:
        readonly_exclusions = {
            "shadow reports": runtime_root / "research" / "shadow-reports",
            "shadow control": runtime_root / "control" / "shadow-sessions" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR:
        readonly_exclusions = {
            "daily pipeline state": runtime_root / "research" / "daily-pipeline",
            "daily orchestrator control": (
                runtime_root / "control" / "daily-orchestrators" / instance
            ),
            "daily close source": runtime_root / "live" / "daily-close",
            "current immutable profile": runtime_root / "current",
        }
    elif manifest.service_kind is RuntimeServiceKind.FEATURE_LIVE:
        readonly_exclusions = {
            "feature state": runtime_root / "live" / "features",
            "feature control": runtime_root / "control" / "features" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.CANDIDATE_PUBLISHER:
        if manifest.settings.get("input_mode", "sealed_document") == "auction_live":
            readonly_exclusions = {
                "candidate state": runtime_root / "live" / "candidates" / instance,
                "candidate control": runtime_root / "control" / "candidates" / instance,
            }
        else:
            readonly_exclusions = {
                "live": runtime_root / "live",
                "control": runtime_root / "control",
            }
    elif manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE:
        validate_strategy_live_completion_manifest(
            manifest,
            runtime_root=runtime_root,
        )
        readonly_exclusions = {
            "strategy state": runtime_root / "live" / "strategies" / instance,
            "strategy control": runtime_root / "control" / "strategies" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.SIGNAL_ROUTER:
        readonly_exclusions = {
            "signal state": runtime_root / "live" / "signal-bus",
            "signal control": runtime_root / "control" / "signal-routers" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.NOTIFIER:
        readonly_exclusions = {
            "notification state": runtime_root / "live" / "notifications" / instance,
            "notification control": runtime_root / "control" / "notifiers" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.PAPER_BROKER:
        readonly_exclusions = {
            "paper state": runtime_root / "live" / "paper-brokers" / instance,
            "paper control": runtime_root / "control" / "paper-brokers" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER:
        readonly_exclusions = {
            "paper constraint authority": runtime_root / "authorities" / "paper-execution",
            "paper constraint control": (runtime_root / "control" / "paper-constraints" / instance),
        }
    elif manifest.service_kind is RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER:
        readonly_exclusions = {
            "runtime health authority": runtime_root / "control" / "authority-runtime-health",
            "runtime health control": (
                runtime_root / "control" / "runtime-health-publishers" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.LAB_JOBS_PUBLISHER:
        readonly_exclusions = {
            "lab jobs authority": (runtime_root / "research" / "serving-authorities" / "lab-jobs"),
            "lab jobs control": runtime_root / "control" / "lab-jobs-publishers" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.LAB_ARTIFACT_CATALOG:
        readonly_exclusions = {
            "artifact catalog state": (runtime_root / "research" / "artifact-catalogs" / instance),
            "artifact catalog control": runtime_root / "control" / "artifact-catalogs" / instance,
        }
    elif manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
        readonly_exclusions = {
            "artifact retention state": runtime_root / "research" / "artifact-retention" / instance,
            "artifact retention artifacts": runtime_root / "research" / "final-artifacts",
            "artifact retention control": (
                runtime_root / "control" / "artifact-retention" / instance
            ),
        }
    elif manifest.service_kind is RuntimeServiceKind.PROMOTIONS_PUBLISHER:
        readonly_exclusions = {
            "promotions authority": (
                runtime_root / "research" / "serving-authorities" / "promotions"
            ),
            "promotions control": (runtime_root / "control" / "promotions-publishers" / instance),
        }
    elif manifest.service_kind is RuntimeServiceKind.SERVING_PUBLISHER:
        readonly_exclusions = {
            "serving state": runtime_root / "serving",
            "serving control": (runtime_root / "control" / "serving-publishers" / instance),
        }
    else:
        readonly_exclusions = {
            manifest.plane.value: runtime_root / manifest.plane.value,
            "control": runtime_root / "control",
        }
    for setting_name in _READONLY_PATH_SETTINGS.get(manifest.service_kind, ()):
        if setting_name not in manifest.settings:
            continue
        _require_external_readonly_path(
            manifest.settings[setting_name],
            writable_roots=readonly_exclusions,
            setting_name=setting_name,
        )
    if manifest.service_kind is RuntimeServiceKind.SIGNAL_ROUTER:
        raw_sources = manifest.settings.get("sources", ())
        if not isinstance(raw_sources, (list, tuple)):
            raise ValueError("signal router sources must be a sequence")
        for index, raw_source in enumerate(raw_sources):
            if not isinstance(raw_source, Mapping):
                raise ValueError(f"signal router sources[{index}] must be a mapping")
            if "runner_state_path" not in raw_source:
                continue
            _require_external_readonly_path(
                raw_source["runner_state_path"],
                writable_roots=readonly_exclusions,
                setting_name=f"sources[{index}].runner_state_path",
            )
    if manifest.service_kind is RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER:
        expected_paths = {
            "minute_spool_root": runtime_root / "live" / "market-minute",
            "reference_registry_path": (
                runtime_root / "authorities" / "reference-slow" / "reference.sqlite3"
            ),
            "authority_root": runtime_root / "authorities" / "paper-execution",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the paper constraint systemd path")
    if manifest.service_kind is RuntimeServiceKind.REFERENCE_SLOW_SOURCE:
        expected_paths = {
            "calendar_path": _expected_market_calendar_generation(
                manifest,
                runtime_root=runtime_root,
            ),
            "spool_root": runtime_root / "live" / "reference-slow",
            "quota_path": runtime_root / "live" / "reference-slow" / "quota.sqlite3",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(
                    f"{setting_name} must equal the reference-slow source systemd path"
                )
        database_path = Path(str(manifest.settings.get("database_path", "")))
        if not database_path.is_absolute() or database_path.is_relative_to(runtime_root):
            raise ValueError("database_path must be an external absolute read-only snapshot")
    if manifest.service_kind is RuntimeServiceKind.DAILY_CLOSE_SOURCE:
        expected_paths = {
            "calendar_path": _expected_market_calendar_generation(
                manifest,
                runtime_root=runtime_root,
            ),
            "spool_root": runtime_root / "live" / "daily-close",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the daily-close source systemd path")
    if manifest.service_kind is RuntimeServiceKind.SHADOW_SESSION:
        expected_paths = {
            "calendar_path": _expected_market_calendar_generation(
                manifest,
                runtime_root=runtime_root,
            ),
            "report_root": runtime_root / "research" / "shadow-reports",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the Shadow systemd path")
        expected_legacy_root = runtime_root.parent / "legacy-shadow"
        for setting_name, expected_path in {
            "legacy_monitor_root": expected_legacy_root / "monitor",
            "legacy_surge_root": expected_legacy_root / "surge",
            "isolated_runner_root": expected_legacy_root / "isolated-runners",
        }.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the external Shadow input root")
        if manifest.settings.get("mode") != "shadow":
            raise ValueError("Shadow runtime session must remain in shadow mode")
        if tuple(manifest.settings.get("signer_command", ())) != (
            "/usr/bin/sudo",
            "-n",
            "/usr/local/libexec/rquant-shadow-report-signer",
        ):
            raise ValueError("Shadow runtime signer must use the fixed protected helper")
    if manifest.service_kind is RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR:
        expected_paths = {
            "storage_root": runtime_root / "research" / "daily-pipeline",
            "source_spool_root": runtime_root / "live" / "daily-close",
            "deployment_profile_path": runtime_root / "current" / "deployment-profile.json",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the daily orchestrator systemd path")
        if manifest.settings.get("mode") != "shadow":
            raise ValueError("daily orchestrator must remain in shadow mode")
    if manifest.service_kind is RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER:
        instance = _instance_name(manifest.service_id)
        expected_paths = {
            "calendar_path": _expected_market_calendar_generation(
                manifest,
                runtime_root=runtime_root,
            ),
            "spool_root": runtime_root / "live" / "reference-slow",
            "registry_path": runtime_root / "authorities" / "reference-slow" / "reference.sqlite3",
            "cursor_root": runtime_root
            / "control"
            / "reference-slow-publishers"
            / instance
            / "cursors",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(
                    f"{setting_name} must equal the reference-slow publisher systemd path"
                )
    if manifest.service_kind is RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER:
        expected_authority = runtime_root / "control" / "authority-runtime-health"
        if Path(str(manifest.settings.get("authority_root", ""))) != expected_authority:
            raise ValueError("authority_root must equal the runtime health authority path")
        raw_sources = manifest.settings.get("sources", ())
        if not isinstance(raw_sources, (list, tuple)) or not raw_sources:
            raise ValueError("runtime health sources must be a non-empty sequence")
        source_roots: list[Path] = []
        for index, raw_source in enumerate(raw_sources):
            if not isinstance(raw_source, Mapping):
                raise ValueError(f"runtime health sources[{index}] must be a mapping")
            control_root = raw_source.get("control_root")
            _require_external_readonly_path(
                control_root,
                writable_roots=readonly_exclusions,
                setting_name=f"sources[{index}].control_root",
            )
            candidate = Path(str(control_root))
            try:
                relative = candidate.relative_to(runtime_root / "control")
            except ValueError as exc:
                raise ValueError(
                    f"runtime health sources[{index}].control_root must use a known control root"
                ) from exc
            if (
                len(relative.parts) != 2
                or relative.parts[0] not in _RUNTIME_HEALTH_SOURCE_BUCKETS
                or not relative.parts[1]
            ):
                raise ValueError(
                    f"runtime health sources[{index}].control_root must use a known control root"
                )
            source_roots.append(candidate)
        if len(source_roots) != len(set(source_roots)):
            raise ValueError("runtime health sources contain duplicate control roots")
    if manifest.service_kind is RuntimeServiceKind.LAB_JOBS_PUBLISHER:
        expected_paths = {
            "lab_jobs_path": runtime_root / "research" / "lab_jobs.sqlite3",
            "authority_root": runtime_root / "research" / "serving-authorities" / "lab-jobs",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the lab jobs systemd path")
        if (
            "research_metadata_path" in manifest.settings
            and Path(str(manifest.settings["research_metadata_path"]))
            != runtime_root / "research" / "research_ro.duckdb"
        ):
            raise ValueError("research_metadata_path must equal the lab jobs systemd path")
    if manifest.service_kind is RuntimeServiceKind.LAB_ARTIFACT_CATALOG:
        instance = _instance_name(manifest.service_id)
        expected_paths = {
            "research_root": runtime_root / "research",
            "artifact_root": runtime_root / "research" / "final-artifacts",
            "state_root": runtime_root / "research" / "artifact-catalogs" / instance,
            "lab_jobs_path": runtime_root / "research" / "lab_jobs.sqlite3",
            "dataset_authority_path": runtime_root / "research" / "research_ro.duckdb",
            "experiment_registry_path": runtime_root / "research" / "experiment_registry.sqlite3",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the artifact catalog systemd path")
    if manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
        instance = _instance_name(manifest.service_id)
        expected_paths = {
            "managed_root": runtime_root / "research" / "final-artifacts",
            "state_root": runtime_root / "research" / "artifact-retention" / instance,
            "reference_store_path": (
                runtime_root / "research" / "artifact-retention" / instance / "references.sqlite3"
            ),
            "catalog_authority_root": (
                runtime_root / "research" / "artifact-retention" / instance / "catalog-authority"
            ),
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the artifact retention systemd path")
        for setting_name in ("recovery_publication_root", "recovery_restore_root"):
            recovery_path = Path(str(manifest.settings.get(setting_name, "")))
            if not recovery_path.is_absolute() or recovery_path != Path(
                os.path.abspath(recovery_path)
            ):
                raise ValueError(f"{setting_name} must be an absolute normalized recovery root")
    if manifest.service_kind is RuntimeServiceKind.PROMOTIONS_PUBLISHER:
        expected_paths = {
            "experiment_registry_path": runtime_root / "research" / "experiment_registry.sqlite3",
            "experiment_registry_managed_trust_root": runtime_root / "research",
            "authority_root": runtime_root / "research" / "serving-authorities" / "promotions",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the promotions systemd path")
    if manifest.service_kind is RuntimeServiceKind.SERVING_PUBLISHER:
        raw_authorities = manifest.settings.get("source_authorities", ())
        if not isinstance(raw_authorities, (list, tuple)):
            raise ValueError("serving source authorities must be a sequence")
        dataset_ids: list[str] = []
        for index, raw_authority in enumerate(raw_authorities):
            if not isinstance(raw_authority, Mapping):
                raise ValueError(f"serving source authorities[{index}] must be a mapping")
            dataset_id = raw_authority.get("dataset_id")
            authority_root = raw_authority.get("root")
            if not isinstance(dataset_id, str) or not isinstance(authority_root, str):
                raise ValueError("serving source authority dataset_id and root must be strings")
            dataset_ids.append(dataset_id)
            _require_external_readonly_path(
                authority_root,
                writable_roots=readonly_exclusions,
                setting_name=f"source_authorities[{index}].root",
            )
            candidate = Path(authority_root)
            owner_service_id = _SERVING_SOURCE_OWNER_SERVICE_IDS.get(dataset_id)
            expected_root = (
                _serving_source_owner_root(runtime_root, dataset_id)
                if owner_service_id is not None
                else None
            )
            if expected_root is None or candidate != expected_root:
                raise ValueError(
                    f"serving source authority {dataset_id!r} must use the exact owner service "
                    f"identity and read-only root"
                )
        if len(dataset_ids) != 6 or set(dataset_ids) != set(_SERVING_SOURCE_OWNER_SERVICE_IDS):
            raise ValueError("serving source authorities require exactly six owner datasets")
    if manifest.service_kind is RuntimeServiceKind.CANDIDATE_PUBLISHER:
        expected_root = (
            runtime_root
            / RuntimeServicePlane.LIVE.value
            / "candidates"
            / _instance_name(manifest.service_id)
        )
        if Path(str(manifest.settings.get("snapshot_root", ""))) != expected_root:
            raise ValueError(
                "candidate snapshot_root must equal its exclusive systemd instance root"
            )
        if manifest.settings.get("input_mode", "sealed_document") == "auction_live":
            expected_readonly = {
                "auction_spool_root": runtime_root / "live" / "auction-match",
                "reference_registry_path": (
                    runtime_root / "authorities" / "reference-slow" / "reference.sqlite3"
                ),
                "calendar_path": _expected_market_calendar_generation(
                    manifest,
                    runtime_root=runtime_root,
                ),
            }
            for setting_name, expected_path in expected_readonly.items():
                if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                    raise ValueError(
                        f"{setting_name} must equal the auction candidate shared authority path"
                    )
            database_path = Path(str(manifest.settings.get("daily_database_path", "")))
            if not database_path.is_absolute() or database_path.is_relative_to(runtime_root):
                raise ValueError(
                    "daily_database_path must be an external absolute read-only snapshot"
                )
            if "candidate_input_path" in manifest.settings:
                raise ValueError("candidate_input_path is forbidden for auction_live")
    if manifest.service_kind is RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER:
        expected_paths = {
            "calendar_path": _expected_market_calendar_generation(
                manifest,
                runtime_root=runtime_root,
            ),
            "authority_root": runtime_root / "authorities" / "auction-universe",
        }
        for setting_name, expected_path in expected_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(
                    f"{setting_name} must equal the auction-universe publisher systemd path"
                )
        database_path = Path(str(manifest.settings.get("database_path", "")))
        if not database_path.is_absolute() or database_path.is_relative_to(runtime_root):
            raise ValueError("database_path must be an external absolute read-only snapshot")
    if manifest.service_kind is RuntimeServiceKind.AUCTION_MATCH_SOURCE:
        expected_spool = runtime_root / "live" / "auction-match"
        expected_quota = expected_spool / "quota.sqlite3"
        expected_readonly = {
            "calendar_path": _expected_market_calendar_generation(
                manifest,
                runtime_root=runtime_root,
            ),
            "universe_path": runtime_root / "authorities" / "auction-universe" / "current.json",
        }
        if Path(str(manifest.settings.get("spool_root", ""))) != expected_spool:
            raise ValueError("spool_root must equal the auction-match owner root")
        if Path(str(manifest.settings.get("quota_path", ""))) != expected_quota:
            raise ValueError("quota_path must equal the auction-match owner quota path")
        for setting_name, expected_path in expected_readonly.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the auction-match authority path")
    if manifest.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
        expected_spool = runtime_root / "live" / "market-minute"
        expected_quota = expected_spool / "quota.sqlite3"
        expected_calendar = _expected_market_calendar_generation(
            manifest,
            runtime_root=runtime_root,
        )
        if Path(str(manifest.settings.get("spool_root", ""))) != expected_spool:
            raise ValueError("spool_root must equal the market-minute owner root")
        if Path(str(manifest.settings.get("quota_path", ""))) != expected_quota:
            raise ValueError("quota_path must equal the market-minute owner quota path")
        if Path(str(manifest.settings.get("calendar_path", ""))) != expected_calendar:
            raise ValueError("calendar_path must equal the market-minute authority path")
    if manifest.service_kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE:
        expected_spool = runtime_root / "live" / "watchlist-quote"
        expected_quota = expected_spool / "quota.sqlite3"
        expected_calendar = _expected_market_calendar_generation(
            manifest,
            runtime_root=runtime_root,
        )
        if Path(str(manifest.settings.get("spool_root", ""))) != expected_spool:
            raise ValueError("spool_root must equal the watchlist-quote owner root")
        if Path(str(manifest.settings.get("quota_path", ""))) != expected_quota:
            raise ValueError("quota_path must equal the watchlist-quote owner quota path")
        if Path(str(manifest.settings.get("calendar_path", ""))) != expected_calendar:
            raise ValueError("calendar_path must equal the watchlist-quote authority path")
        if manifest.settings.get("rollout_mode") != "candidate":
            raise ValueError("watchlist-quote production rollout_mode must be candidate")
    if manifest.service_kind is RuntimeServiceKind.FEATURE_LIVE:
        expected_features = runtime_root / "live" / "features"
        expected_source = runtime_root / "live" / "market-minute"
        if Path(str(manifest.settings.get("feature_spool_root", ""))) != expected_features:
            raise ValueError("feature_spool_root must equal the feature owner root")
        if Path(str(manifest.settings.get("raw_spool_root", ""))) != expected_source:
            raise ValueError("raw_spool_root must equal the read-only market-minute root")
    if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE:
        expected_state = runtime_root / "live" / "strategies" / instance / "runner.sqlite3"
        if Path(str(manifest.settings.get("runner_state_path", ""))) != expected_state:
            raise ValueError(
                "strategy runner_state_path must equal its exclusive systemd instance path"
            )
        broker_path = Path(str(manifest.settings.get("paper_broker_path", "")))
        broker_root = runtime_root / "live" / "paper-brokers"
        if (
            broker_path.name != "broker.sqlite3"
            or broker_path.parent.parent != broker_root
            or re.fullmatch(r"svc-[0-9a-f]{64}", broker_path.parent.name) is None
        ):
            raise ValueError("paper_broker_path must use a paper-broker owner read-only database")
    shared_signal_path = runtime_root / "live" / "signal-bus" / "signal_bus.sqlite3"
    if (
        manifest.service_kind is RuntimeServiceKind.SIGNAL_ROUTER
        and Path(str(manifest.settings.get("signal_bus_path", ""))) != shared_signal_path
    ):
        raise ValueError("signal_bus_path must equal the shared isolated signal bus path")
    shared_signal_spool = runtime_root / "live" / "signal-bus" / "spool"
    if (
        manifest.service_kind
        in {
            RuntimeServiceKind.SIGNAL_ROUTER,
            RuntimeServiceKind.NOTIFIER,
            RuntimeServiceKind.PAPER_BROKER,
        }
        and Path(str(manifest.settings.get("signal_spool_root", ""))) != shared_signal_spool
    ):
        raise ValueError("signal_spool_root must equal the immutable signal spool path")
    if manifest.service_kind is RuntimeServiceKind.NOTIFIER:
        notification_root = runtime_root / "live" / "notifications" / instance
        expected_state = notification_root / "notification_state.sqlite3"
        if Path(str(manifest.settings.get("notification_state_path", ""))) != expected_state:
            raise ValueError(
                "notification_state_path must equal its exclusive systemd instance path"
            )
        if Path(str(manifest.settings.get("serving_authority_root", ""))) != (
            notification_root / "serving-authority"
        ):
            raise ValueError(
                "serving_authority_root must equal the notifier's exclusive instance path"
            )
        if "page_projection_database_path" in manifest.settings:
            projection_database = Path(str(manifest.settings["page_projection_database_path"]))
            if not projection_database.is_absolute() or projection_database.is_relative_to(
                runtime_root
            ):
                raise ValueError(
                    "page_projection_database_path must be an external absolute read-only snapshot"
                )
            if Path(str(manifest.settings.get("page_projection_surge_live_root", ""))) != (
                projection_database.parent / "surge_live"
            ):
                raise ValueError(
                    "page_projection_surge_live_root must equal the operational data "
                    "surge_live source"
                )
        if "page_projection_canvas_catalog_root" in manifest.settings:
            expected_catalog_root = runtime_root / "serving" / "page-control" / "canvases"
            if (
                Path(str(manifest.settings["page_projection_canvas_catalog_root"]))
                != expected_catalog_root
            ):
                raise ValueError(
                    "page_projection_canvas_catalog_root must equal the PageControl public catalog"
                )
            expected_authority_paths = {
                "page_projection_canvas_receipt_root": (
                    runtime_root / "serving" / "page-control" / "canvas-publication-receipts"
                ),
                "page_projection_page_control_outbox_path": (
                    runtime_root / "control" / "page-control.sqlite3"
                ),
            }
            for setting_name, expected_path in expected_authority_paths.items():
                if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                    raise ValueError(
                        f"{setting_name} must equal the PageControl public authority path"
                    )
            required_public_authority = {
                "page_projection_canvas_active_key_id",
                "page_projection_canvas_active_public_key_pem",
                "page_projection_canvas_previous_public_key_pems",
            }
            if not required_public_authority <= set(manifest.settings):
                raise ValueError("canvas projection manifest lacks public key authority")
    if manifest.service_kind is RuntimeServiceKind.PAPER_BROKER:
        paper_root = runtime_root / "live" / "paper-brokers" / instance
        expected_paper_paths = {
            "queue_path": paper_root / "queue.sqlite3",
            "consumer_state_path": paper_root / "consumer.sqlite3",
            "broker_path": paper_root / "broker.sqlite3",
        }
        for setting_name, expected_path in expected_paper_paths.items():
            if Path(str(manifest.settings.get(setting_name, ""))) != expected_path:
                raise ValueError(f"{setting_name} must equal the broker's exclusive paper path")
        if Path(str(manifest.settings.get("serving_authority_root", ""))) != (
            paper_root / "serving-authority"
        ):
            raise ValueError(
                "serving_authority_root must equal the broker's exclusive instance path"
            )
        constraint_path = Path(str(manifest.settings.get("execution_constraint_root", "")))
        constraint_root = runtime_root / "authorities" / "paper-execution"
        if constraint_path != constraint_root:
            raise ValueError(
                "execution_constraint_root must equal the paper-execution authority root"
            )


def _validate_capability_value(name: str, value: object) -> str:
    if not isinstance(value, str):
        raise ValueError(f"capability environment {name} must be a string")
    if not value or any(character in value for character in ("\x00", "\n", "\r")):
        raise ValueError(f"capability environment {name} has an unsafe value")
    return value


def _validate_capabilities(
    manifests: tuple[RuntimeServiceManifest, ...],
    capability_env: Mapping[str, Mapping[str, str]],
) -> dict[str, dict[str, str]]:
    if not isinstance(capability_env, Mapping):
        raise TypeError("capability_env must be a mapping")
    service_ids = {manifest.service_id for manifest in manifests}
    if set(capability_env) != service_ids:
        raise ValueError("capability environment service ids must exactly match manifests")
    validated: dict[str, dict[str, str]] = {}
    for manifest in manifests:
        raw = capability_env[manifest.service_id]
        if not isinstance(raw, Mapping):
            raise ValueError("each capability environment must be a mapping")
        if (
            manifest.plane is not RuntimeServicePlane.LIVE
            and manifest.service_kind is not RuntimeServiceKind.ARTIFACT_RETENTION
            and raw
        ):
            raise ValueError(
                f"{manifest.plane.value} services cannot receive capability environment"
            )
        allowed = CAPABILITY_KEYS.get(manifest.service_kind, frozenset())
        unknown = set(raw) - allowed
        if unknown:
            names = ", ".join(sorted(str(name) for name in unknown))
            raise ValueError(f"unknown capability environment for {manifest.service_id}: {names}")
        validated[manifest.service_id] = {
            str(name): _validate_capability_value(str(name), value)
            for name, value in sorted(raw.items())
        }
    return validated


def _validate_runtime_topology(
    manifests: tuple[RuntimeServiceManifest, ...],
) -> None:
    by_kind: dict[RuntimeServiceKind, list[RuntimeServiceManifest]] = {}
    for manifest in manifests:
        by_kind.setdefault(manifest.service_kind, []).append(manifest)
    for kind in (
        RuntimeServiceKind.REFERENCE_SLOW_SOURCE,
        RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER,
        RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER,
        RuntimeServiceKind.AUCTION_MATCH_SOURCE,
        RuntimeServiceKind.MARKET_MINUTE_SOURCE,
        RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE,
        RuntimeServiceKind.DAILY_CLOSE_SOURCE,
        RuntimeServiceKind.SHADOW_SESSION,
        RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR,
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
    ):
        if len(by_kind.get(kind, ())) > 1:
            raise ValueError(f"runtime kind {kind.value} is a singleton mutable-store writer")
    strategies = tuple(by_kind.get(RuntimeServiceKind.STRATEGY_LIVE, ()))
    instances = tuple(
        str(manifest.settings.get("producer_instance_id", "")) for manifest in strategies
    )
    if len(instances) != len(set(instances)):
        raise ValueError("strategy producer_instance_id values must be unique")
    routers = tuple(by_kind.get(RuntimeServiceKind.SIGNAL_ROUTER, ()))
    if strategies and routers:
        router = routers[0]
        expected_signal_bus = router.settings.get("signal_bus_path")
        expected_policy = router.settings.get("routing_policy_fingerprint")
        for manifest in strategies:
            if manifest.settings.get("signal_bus_path") != expected_signal_bus:
                raise ValueError("strategy signal_bus_path differs from the signal router")
            if manifest.settings.get("routing_policy_fingerprint") != expected_policy:
                raise ValueError(
                    "strategy routing_policy_fingerprint differs from the signal router"
                )


def validate_runtime_deployment_topology(
    runtime_root: Path,
    *,
    producer_commit: str,
    manifests: tuple[RuntimeServiceManifest, ...],
) -> tuple[RuntimeServiceManifest, ...]:
    """Validate canonical manifests against one caller-trusted runtime root without writes."""

    if _COMMIT_PATTERN.fullmatch(producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    root = _absolute_runtime_root(runtime_root)
    if not isinstance(manifests, tuple) or not manifests:
        raise ValueError("manifests must be a non-empty tuple")
    canonical = tuple(_canonical_manifest(manifest)[0] for manifest in manifests)
    service_ids = tuple(manifest.service_id for manifest in canonical)
    if len(service_ids) != len(set(service_ids)):
        raise ValueError("runtime bundle contains duplicate service_id values")
    ordered = tuple(sorted(canonical, key=lambda manifest: manifest.service_id))
    _validate_runtime_topology(ordered)
    for manifest in ordered:
        _validate_manifest_authority(
            manifest,
            producer_commit=producer_commit,
            runtime_root=root,
        )
    return ordered


def _json_string_values(value: object) -> set[str]:
    if isinstance(value, str):
        return {value}
    if isinstance(value, Mapping):
        values: set[str] = set()
        for nested in value.values():
            values.update(_json_string_values(nested))
        return values
    if isinstance(value, list):
        values: set[str] = set()
        for nested in value:
            values.update(_json_string_values(nested))
        return values
    return set()


def _secret_representations(value: str) -> set[str]:
    encoded = value.encode("utf-8")
    standard_base64 = b64encode(encoded).decode("ascii")
    urlsafe_base64 = urlsafe_b64encode(encoded).decode("ascii")
    return {
        value,
        encoded.hex(),
        standard_base64,
        standard_base64.rstrip("="),
        urlsafe_base64,
        urlsafe_base64.rstrip("="),
    }


def _runtime_secret_values(capabilities: Mapping[str, Mapping[str, str]]) -> set[str]:
    values: set[str] = set()
    for environment in capabilities.values():
        for name, raw_value in environment.items():
            if name not in SECRET_CAPABILITY_KEYS:
                continue
            try:
                decoded = json.loads(raw_value)
            except json.JSONDecodeError:
                decoded = raw_value
            for value in _json_string_values(decoded):
                values.update(_secret_representations(value))
    return values


def _reject_plaintext_secrets(
    manifest_payloads: Mapping[str, bytes],
    capabilities: Mapping[str, Mapping[str, str]],
    *,
    deployment_profile_payload: bytes | None = None,
) -> None:
    forbidden_values = _runtime_secret_values(capabilities)
    structured_payloads = {
        **manifest_payloads,
        **(
            {"deployment profile": deployment_profile_payload}
            if deployment_profile_payload is not None
            else {}
        ),
    }
    for label, payload in structured_payloads.items():
        try:
            parsed = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"runtime {label} is not valid structured JSON") from exc
        if _json_string_values(parsed) & forbidden_values:
            raise ValueError(f"runtime {label} contains a plaintext capability value")
        if label == "deployment profile":
            continue
        if not isinstance(parsed, dict):
            raise ValueError(f"runtime manifest {label} is not a structured object")
        settings = parsed.get("settings", {})
        if not isinstance(settings, Mapping):
            raise ValueError(f"runtime manifest {label} settings are not structured")
        setting_values = _json_string_values(settings)
        if any(
            name in setting_values
            for name in CAPABILITY_KEYS.get(RuntimeServiceKind(parsed["service_kind"]), frozenset())
        ):
            raise ValueError(f"runtime manifest {label} contains a capability environment name")


def _environment_payload(producer_commit: str, generation_hash: str) -> bytes:
    return (
        "APP_ENV=prod\n"
        "RQUANT_DISABLE_DOTENV=1\n"
        f"RQUANT_RUNTIME_COMMIT={producer_commit}\n"
        f"RQUANT_RUNTIME_GENERATION={generation_hash}\n"
    ).encode()


def _canonical_model_payload(model: RuntimeContractModel) -> bytes:
    return model.model_dump_json().encode("utf-8")


def _read_owned_generation_file(path: Path, *, label: str) -> bytes:
    try:
        observed = path.lstat()
    except FileNotFoundError as exc:
        raise RuntimeSchemaCompatibilityError(f"previous {label} is missing") from exc
    if (
        not stat.S_ISREG(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o600
    ):
        raise RuntimeSchemaCompatibilityError(f"previous {label} has an unsafe identity")
    try:
        return path.read_bytes()
    except OSError as exc:
        raise RuntimeSchemaCompatibilityError(f"previous {label} is unreadable") from exc


def _parse_generation_basis(payload: bytes) -> _RuntimeGenerationBasis:
    try:
        basis = _RuntimeGenerationBasis.model_validate_json(payload)
    except (ValueError, TypeError) as exc:
        raise RuntimeSchemaCompatibilityError(
            "previous runtime generation hash-bound basis is invalid"
        ) from exc
    if payload != _canonical_model_payload(basis):
        raise RuntimeSchemaCompatibilityError(
            "previous runtime generation hash-bound basis is not canonical"
        )
    return basis


def _read_current_schema_contract(
    root: Path,
    *,
    allow_missing_contract: bool = False,
) -> tuple[str | None, RuntimeSchemaContractBundle | None, bytes | None]:
    target = _current_target(root)
    if target is None:
        return None, None, None
    generation = root / target
    try:
        observed = generation.lstat()
    except FileNotFoundError as exc:
        raise RuntimeSchemaCompatibilityError(
            "current runtime generation for schema preflight is missing"
        ) from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise RuntimeSchemaCompatibilityError(
            "current runtime generation for schema preflight is unsafe"
        )
    try:
        contract_payload = _read_owned_generation_file(
            generation / "schema-contracts.json",
            label="runtime schema contract",
        )
    except RuntimeSchemaCompatibilityError:
        contract_path = generation / "schema-contracts.json"
        if allow_missing_contract and not contract_path.exists() and not contract_path.is_symlink():
            basis_path = generation / "generation-basis.json"
            if basis_path.exists() or basis_path.is_symlink():
                raise RuntimeSchemaCompatibilityError(
                    "previous runtime generation hash-bound schema contract is missing"
                ) from None
            return target, None, None
        raise
    contract = parse_runtime_schema_contract_bundle(contract_payload)
    basis_payload = _read_owned_generation_file(
        generation / "generation-basis.json",
        label="runtime generation hash-bound basis",
    )
    basis = _parse_generation_basis(basis_payload)
    expected_generation_hash = Path(target).name
    if canonical_sha256(basis.model_dump(mode="python")) != expected_generation_hash:
        raise RuntimeSchemaCompatibilityError(
            "previous runtime generation is not hash-bound to its generation basis"
        )
    if (
        basis.producer_commit != contract.producer_commit
        or set(basis.manifest_sha256) != set(contract.manifest_fingerprints)
        or basis.schema_contract_sha256 != hashlib.sha256(contract_payload).hexdigest()
    ):
        raise RuntimeSchemaCompatibilityError(
            "previous runtime schema contract is not hash-bound to its generation"
        )
    bootstrap_path = generation / "schema-bootstrap.json"
    bootstrap_payload = None
    if basis.schema_bootstrap_sha256 is None:
        if bootstrap_path.exists() or bootstrap_path.is_symlink():
            raise RuntimeSchemaCompatibilityError(
                "previous runtime schema bootstrap audit is not hash-bound to its generation"
            )
    else:
        bootstrap_payload = _read_owned_generation_file(
            bootstrap_path,
            label="runtime schema bootstrap audit",
        )
        if basis.schema_bootstrap_sha256 != hashlib.sha256(bootstrap_payload).hexdigest():
            raise RuntimeSchemaCompatibilityError(
                "previous runtime schema bootstrap audit is not hash-bound to its generation"
            )
        _validate_schema_bootstrap_payload(
            bootstrap_payload,
            expected_contract_hash=contract.content_hash,
        )
    return target, contract, bootstrap_payload


def _read_current_legacy_v1_schema_contract(root: Path) -> tuple[str, bytes]:
    target = _current_target(root)
    if target is None:
        raise RuntimeSchemaCompatibilityError("runtime schema migration source is not legacy v1")
    generation = root / target
    try:
        observed = generation.lstat()
    except FileNotFoundError as exc:
        raise RuntimeSchemaCompatibilityError(
            "current runtime generation for schema migration is missing"
        ) from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise RuntimeSchemaCompatibilityError(
            "current runtime generation for schema migration is unsafe"
        )
    legacy_payload = _read_owned_generation_file(
        generation / "schema-contracts.json",
        label="runtime schema contract",
    )
    try:
        decoded = json.loads(legacy_payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeSchemaCompatibilityError(
            "legacy v1 runtime schema contract is invalid"
        ) from exc
    if not isinstance(decoded, dict) or decoded.get("schema_version") != 1:
        raise RuntimeSchemaCompatibilityError("runtime schema migration source is not legacy v1")
    basis_payload = _read_owned_generation_file(
        generation / "generation-basis.json",
        label="runtime generation hash-bound basis",
    )
    basis = _parse_generation_basis(basis_payload)
    if canonical_sha256(basis.model_dump(mode="python")) != Path(target).name:
        raise RuntimeSchemaCompatibilityError(
            "previous runtime generation is not hash-bound to its generation basis"
        )
    if basis.schema_contract_sha256 != hashlib.sha256(legacy_payload).hexdigest():
        raise RuntimeSchemaCompatibilityError(
            "legacy v1 runtime schema contract is not hash-bound to its generation"
        )
    return target, legacy_payload


def _load_runtime_deployment_receipt_at_generation(
    root: Path,
    *,
    generation_hash: str,
    expected_commit: str,
    expected_profile_id: str,
) -> RuntimeDeploymentReceipt:
    if _COMMIT_PATTERN.fullmatch(expected_commit) is None:
        raise ValueError("expected commit must be a full lowercase Git SHA")
    if _GENERATION_PATTERN.fullmatch(expected_profile_id) is None:
        raise ValueError("expected profile id must be a full lowercase SHA-256")
    if _GENERATION_PATTERN.fullmatch(generation_hash) is None:
        raise ValueError("runtime generation must be a full lowercase SHA-256")
    generation = root / "generations" / generation_hash
    try:
        observed_generation = generation.lstat()
    except FileNotFoundError as exc:
        raise ValueError("runtime deployment generation is missing") from exc
    if (
        not stat.S_ISDIR(observed_generation.st_mode)
        or stat.S_ISLNK(observed_generation.st_mode)
        or observed_generation.st_uid != os.getuid()
        or stat.S_IMODE(observed_generation.st_mode) != 0o700
    ):
        raise ValueError("runtime deployment generation is unsafe")
    basis_payload = _read_owned_generation_file(
        generation / "generation-basis.json",
        label="runtime generation hash-bound basis",
    )
    basis = _parse_generation_basis(basis_payload)
    if canonical_sha256(basis.model_dump(mode="python")) != generation_hash:
        raise ValueError("runtime deployment generation hash mismatch")
    if basis.producer_commit != expected_commit:
        raise ValueError("runtime deployment commit mismatch")
    if basis.deployment_profile_id != expected_profile_id:
        raise ValueError("runtime deployment profile mismatch")
    schema_contract_payload = _read_owned_generation_file(
        generation / "schema-contracts.json",
        label="runtime schema contract",
    )
    schema_contract = parse_runtime_schema_contract_bundle(schema_contract_payload)
    if (
        basis.schema_contract_sha256 != hashlib.sha256(schema_contract_payload).hexdigest()
        or schema_contract.producer_commit != expected_commit
        or set(schema_contract.manifest_fingerprints) != set(basis.manifest_sha256)
    ):
        raise ValueError("runtime deployment schema contract mismatch")
    manifests = generation / "manifests"
    observed_manifests = manifests.lstat()
    if (
        not stat.S_ISDIR(observed_manifests.st_mode)
        or stat.S_ISLNK(observed_manifests.st_mode)
        or observed_manifests.st_uid != os.getuid()
        or stat.S_IMODE(observed_manifests.st_mode) != 0o700
    ):
        raise ValueError("runtime deployment manifest directory is unsafe")
    expected_manifest_names: set[str] = set()
    for service_id, expected_sha256 in basis.manifest_sha256.items():
        instance = basis.instance_mapping[service_id]
        expected_manifest_names.add(f"{instance}.json")
        payload = _read_owned_generation_file(
            manifests / f"{instance}.json",
            label=f"runtime manifest {service_id}",
        )
        if hashlib.sha256(payload).hexdigest() != expected_sha256:
            raise ValueError(f"runtime deployment manifest hash mismatch: {service_id}")
        try:
            manifest = RuntimeServiceManifest.model_validate_json(payload)
        except ValueError as exc:
            raise ValueError(f"runtime deployment manifest is invalid: {service_id}") from exc
        if (
            manifest.service_id != service_id
            or manifest.producer_commit != expected_commit
            or _instance_name(service_id) != instance
            or _systemd_unit_name(manifest, instance) != basis.unit_mapping[service_id]
        ):
            raise ValueError(f"runtime deployment manifest identity mismatch: {service_id}")
    actual_manifest_names = {path.name for path in manifests.iterdir() if path.name != ".DS_Store"}
    if actual_manifest_names != expected_manifest_names:
        raise ValueError("runtime deployment manifest inventory mismatch")
    return RuntimeDeploymentReceipt(
        runtime_root=root,
        producer_commit=expected_commit,
        generation_hash=generation_hash,
        deployment_profile_id=expected_profile_id,
        schema_rollout_plan_ids=runtime_schema_rollout_plan_ids_for_generation(
            root,
            generation_id=generation_hash,
        ),
        instance_mapping=basis.instance_mapping,
        unit_mapping=basis.unit_mapping,
    )


def load_runtime_deployment_generation_receipt(
    runtime_root: Path,
    *,
    generation_hash: str,
) -> RuntimeDeploymentReceipt:
    """Load one content-addressed generation without trusting caller-supplied identity."""
    if _GENERATION_PATTERN.fullmatch(generation_hash) is None:
        raise ValueError("runtime generation must be a full lowercase SHA-256")
    root = _absolute_runtime_root(runtime_root)
    basis_payload = _read_owned_generation_file(
        root / "generations" / generation_hash / "generation-basis.json",
        label="runtime generation hash-bound basis",
    )
    basis = _parse_generation_basis(basis_payload)
    if canonical_sha256(basis.model_dump(mode="python")) != generation_hash:
        raise ValueError("runtime deployment generation hash mismatch")
    if basis.deployment_profile_id is None:
        raise ValueError("runtime deployment generation lacks a profile identity")
    return _load_runtime_deployment_receipt_at_generation(
        root,
        generation_hash=generation_hash,
        expected_commit=basis.producer_commit,
        expected_profile_id=basis.deployment_profile_id,
    )


def load_current_runtime_deployment_receipt(
    runtime_root: Path,
    *,
    expected_commit: str,
    expected_profile_id: str,
) -> RuntimeDeploymentReceipt:
    """Verify the immutable current generation and reconstruct its deployment receipt."""
    root = _absolute_runtime_root(runtime_root)
    target = _current_target(root)
    if target is None:
        raise ValueError("runtime current deployment is missing")
    receipt = _load_runtime_deployment_receipt_at_generation(
        root,
        generation_hash=Path(target).name,
        expected_commit=expected_commit,
        expected_profile_id=expected_profile_id,
    )
    if _current_target(root) != target:
        raise ValueError("runtime current deployment changed while validating")
    return receipt


def load_current_runtime_deployment_receipt_unbound(
    runtime_root: Path,
) -> RuntimeDeploymentReceipt:
    """Verify current from its hash-bound generation without caller-supplied identity."""

    root = _absolute_runtime_root(runtime_root)
    target = _current_target(root)
    if target is None:
        raise ValueError("runtime current deployment is missing")
    receipt = load_runtime_deployment_generation_receipt(
        root,
        generation_hash=Path(target).name,
    )
    if _current_target(root) != target:
        raise ValueError("runtime current deployment changed while validating")
    return receipt


def activate_runtime_deployment_generation(
    runtime_root: Path,
    *,
    generation_hash: str,
    expected_commit: str,
    expected_profile_id: str,
) -> RuntimeDeploymentReceipt:
    """Atomically activate one previously verified immutable runtime generation."""
    root = _absolute_runtime_root(runtime_root)
    receipt = _load_runtime_deployment_receipt_at_generation(
        root,
        generation_hash=generation_hash,
        expected_commit=expected_commit,
        expected_profile_id=expected_profile_id,
    )
    _replace_current(root, target=f"generations/{generation_hash}")
    current = load_current_runtime_deployment_receipt(
        root,
        expected_commit=expected_commit,
        expected_profile_id=expected_profile_id,
    )
    if current.generation_hash != receipt.generation_hash:
        raise RuntimeDeploymentRollbackError(
            "runtime generation activation did not publish the verified generation"
        )
    return current


def _load_generation_schema_bundle(
    root: Path,
    *,
    generation_id: str,
) -> RuntimeSchemaContractBundle:
    if _GENERATION_PATTERN.fullmatch(generation_id) is None:
        raise ValueError("runtime schema generation must be a full lowercase SHA-256")
    generation = root / "generations" / generation_id
    try:
        observed = generation.lstat()
    except FileNotFoundError as exc:
        raise ValueError("runtime schema generation is missing") from exc
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise ValueError("runtime schema generation is unsafe")
    basis_payload = _read_owned_generation_file(
        generation / "generation-basis.json",
        label="runtime generation hash-bound basis",
    )
    basis = _parse_generation_basis(basis_payload)
    if canonical_sha256(basis.model_dump(mode="python")) != generation_id:
        raise ValueError("runtime schema generation hash mismatch")
    contract_payload = _read_owned_generation_file(
        generation / "schema-contracts.json",
        label="runtime schema contract",
    )
    if basis.schema_contract_sha256 != hashlib.sha256(contract_payload).hexdigest():
        raise ValueError("runtime schema contract is not hash-bound to generation")
    contract = parse_runtime_schema_contract_bundle(contract_payload)
    if contract.producer_commit != basis.producer_commit or set(
        contract.manifest_fingerprints
    ) != set(basis.manifest_sha256):
        raise ValueError("runtime schema contract identity differs from generation")
    return contract


def _schema_rollout_root(root: Path, plan_id: str) -> Path:
    if _GENERATION_PATTERN.fullmatch(plan_id) is None:
        raise ValueError("runtime schema rollout plan id must be a full lowercase SHA-256")
    return root / "control" / "schema-rollouts" / plan_id


def changed_runtime_schema_channels(
    runtime_root: Path,
    *,
    previous_generation_id: str,
    target_generation_id: str,
) -> tuple[str, ...]:
    root = _absolute_runtime_root(runtime_root)
    previous = _load_generation_schema_bundle(
        root,
        generation_id=previous_generation_id,
    )
    target = _load_generation_schema_bundle(root, generation_id=target_generation_id)
    return tuple(
        channel.channel_id
        for channel in target.channels
        if (
            channel.declaration.schema_fingerprint
            != previous.channel(channel.channel_id).declaration.schema_fingerprint
        )
    )


def runtime_schema_rollout_plan_ids_for_generation(
    runtime_root: Path,
    *,
    generation_id: str,
) -> tuple[str, ...]:
    root = _absolute_runtime_root(runtime_root)
    if _GENERATION_PATTERN.fullmatch(generation_id) is None:
        raise ValueError("runtime schema generation must be a full lowercase SHA-256")
    matching: list[str] = []
    for plan_id in _schema_rollout_plan_ids(root):
        authority = _read_schema_rollout_authority(
            _schema_rollout_root(root, plan_id) / "authority.json"
        )
        if authority.target_generation_id == generation_id:
            matching.append(plan_id)
    return tuple(sorted(matching))


def _read_schema_rollout_authority(path: Path) -> RuntimeSchemaRolloutAuthority:
    payload = _read_owned_generation_file(path, label="runtime schema rollout authority")
    try:
        authority = RuntimeSchemaRolloutAuthority.model_validate_json(payload)
    except ValueError as exc:
        raise RuntimeSchemaCompatibilityError(
            "runtime schema rollout authority is invalid"
        ) from exc
    if payload != _canonical_model_payload(authority):
        raise RuntimeSchemaCompatibilityError("runtime schema rollout authority is not canonical")
    return authority


def prepare_runtime_schema_rollout(
    runtime_root: Path,
    *,
    previous_generation_id: str,
    target_generation_id: str,
    channel_id: str,
    started_at: datetime,
    deadline: datetime,
    consumer_ack_max_age_seconds: int,
    retire_observation_seconds: int = 86_400,
) -> RuntimeSchemaRolloutAuthority:
    """Create or reopen one generation-bound persistent rollout transaction."""

    root = _absolute_runtime_root(runtime_root)
    if _current_target(root) != f"generations/{target_generation_id}":
        raise ValueError("runtime schema rollout target is not the current generation")
    previous = _load_generation_schema_bundle(
        root,
        generation_id=previous_generation_id,
    )
    target = _load_generation_schema_bundle(root, generation_id=target_generation_id)
    plan, registry = build_runtime_schema_rollout(
        previous=previous,
        candidate=target,
        channel_id=channel_id,
        target_generation_id=target_generation_id,
        started_at=started_at,
        deadline=deadline,
        consumer_ack_max_age_seconds=consumer_ack_max_age_seconds,
        retire_observation_seconds=retire_observation_seconds,
    )
    authority = RuntimeSchemaRolloutAuthority.create(
        previous_generation_id=previous_generation_id,
        target_generation_id=target_generation_id,
        previous_bundle=previous,
        target_bundle=target,
        plan=plan,
        registry=registry,
    )
    rollout_root = _schema_rollout_root(root, plan.plan_id)
    _ensure_owned_descendant(root, rollout_root)
    authority_path = rollout_root / "authority.json"
    payload = _canonical_model_payload(authority)
    if authority_path.exists() or authority_path.is_symlink():
        if _read_schema_rollout_authority(authority_path) != authority:
            raise RuntimeSchemaCompatibilityError(
                "conflicting runtime schema rollout authority already exists"
            )
    else:
        _write_secure_file(authority_path, payload)
        _fsync_directory(rollout_root)
    store = SchemaRolloutStore(
        rollout_root / "state.sqlite3",
        production_consumer_registry=registry,
    )
    store.create_plan(
        plan,
        now=started_at,
        operation_id=f"deployment-prepare:{plan.plan_id}",
    )
    return authority


def load_runtime_schema_rollout(
    runtime_root: Path,
    *,
    plan_id: str,
) -> tuple[RuntimeSchemaRolloutAuthority, SchemaRolloutStore]:
    """Load a rollout only after re-deriving its immutable bundle authority."""

    root = _absolute_runtime_root(runtime_root)
    rollout_root = _schema_rollout_root(root, plan_id)
    authority = _read_schema_rollout_authority(rollout_root / "authority.json")
    previous = _load_generation_schema_bundle(
        root,
        generation_id=authority.previous_generation_id,
    )
    target = _load_generation_schema_bundle(
        root,
        generation_id=authority.target_generation_id,
    )
    expected_plan, expected_registry = build_runtime_schema_rollout(
        previous=previous,
        candidate=target,
        channel_id=authority.plan.dataset_id,
        target_generation_id=authority.target_generation_id,
        started_at=authority.plan.started_at,
        deadline=authority.plan.deadline,
        consumer_ack_max_age_seconds=authority.plan.consumer_ack_max_age_seconds,
        retire_observation_seconds=authority.plan.retire_observation_seconds,
    )
    if (
        authority.previous_bundle_content_hash != previous.content_hash
        or authority.target_bundle_content_hash != target.content_hash
        or authority.plan != expected_plan
        or authority.registry != expected_registry
    ):
        raise RuntimeSchemaCompatibilityError(
            "runtime schema rollout authority differs from immutable bundles"
        )
    store = SchemaRolloutStore(
        rollout_root / "state.sqlite3",
        production_consumer_registry=expected_registry,
    )
    store.get_state(plan_id)
    return authority, store


def _rollout_event_payloads(store: SchemaRolloutStore, plan_id: str) -> tuple[dict, ...]:
    return tuple(json.loads(receipt.payload_json) for receipt in store.receipts(plan_id))


def _participant_has_ack(
    store: SchemaRolloutStore,
    *,
    plan_id: str,
    phase: RolloutPhase,
    participant_id: str,
) -> bool:
    return any(
        payload.get("action") == "participant_ack"
        and payload.get("phase") == phase.value
        and payload.get("participant_id") == participant_id
        for payload in _rollout_event_payloads(store, plan_id)
    )


def _consumer_has_receipt(
    store: SchemaRolloutStore,
    *,
    plan_id: str,
    consumer_id: str,
) -> bool:
    return any(
        payload.get("action") == "consumer_capability" and payload.get("consumer_id") == consumer_id
        for payload in _rollout_event_payloads(store, plan_id)
    )


def _schema_rollout_plan_ids(root: Path) -> tuple[str, ...]:
    rollouts = root / "control" / "schema-rollouts"
    if not rollouts.exists():
        return ()
    observed = rollouts.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
    ):
        raise ValueError("runtime schema rollout root is unsafe")
    plan_ids = tuple(sorted(path.name for path in rollouts.iterdir()))
    if any(_GENERATION_PATTERN.fullmatch(plan_id) is None for plan_id in plan_ids):
        raise ValueError("runtime schema rollout root contains an unknown entry")
    return plan_ids


def load_runtime_schema_service_bindings(
    runtime_root: Path,
    *,
    manifest: RuntimeServiceManifest,
    generation_id: str,
    observed_at: datetime,
) -> tuple[RuntimeSchemaServiceBinding, ...]:
    """Admit one service from trusted bundles and persist any required startup ACK."""

    root = _absolute_runtime_root(runtime_root)
    if _current_target(root) != f"generations/{generation_id}":
        raise ValueError("runtime schema service generation is not current")
    candidate = _load_generation_schema_bundle(root, generation_id=generation_id)
    expected_manifest = candidate.manifest_fingerprints.get(manifest.service_id)
    if (
        expected_manifest is None
        or expected_manifest != manifest.manifest_fingerprint
        or manifest.producer_commit != candidate.producer_commit
    ):
        raise RuntimeSchemaCompatibilityError(
            "runtime schema service is not hash-bound to the current bundle"
        )
    bindings: list[RuntimeSchemaServiceBinding] = []
    for plan_id in _schema_rollout_plan_ids(root):
        authority, store = load_runtime_schema_rollout(root, plan_id=plan_id)
        if authority.target_generation_id != generation_id:
            continue
        state = store.get_state(plan_id)
        producer = next(
            (
                participant
                for participant in authority.plan.producers
                if participant.participant_id == manifest.service_id
            ),
            None,
        )
        if producer is not None and state.phase is RolloutPhase.PREPARE:
            if not _participant_has_ack(
                store,
                plan_id=plan_id,
                phase=RolloutPhase.PREPARE,
                participant_id=manifest.service_id,
            ):
                state = store.acknowledge(
                    plan_id=plan_id,
                    expected_revision=state.revision,
                    phase=RolloutPhase.PREPARE,
                    participant_id=manifest.service_id,
                    participant_fingerprint=producer.contract_fingerprint,
                    declaration_fingerprint=authority.plan.new_declaration_fingerprint,
                    now=observed_at,
                    operation_id=f"service-prepare:{generation_id}:{manifest.service_id}",
                )
            acknowledged = {
                payload.get("participant_id")
                for payload in _rollout_event_payloads(store, plan_id)
                if payload.get("action") == "participant_ack"
                and payload.get("phase") == RolloutPhase.PREPARE.value
            }
            required = {participant.participant_id for participant in authority.plan.producers}
            if required <= acknowledged:
                state = store.advance(
                    plan_id=plan_id,
                    expected_revision=state.revision,
                    target_phase=RolloutPhase.DUAL_WRITE,
                    now=observed_at,
                    operation_id=f"service-dual-write:{generation_id}",
                )
            else:
                raise RuntimeSchemaCompatibilityError(
                    "schema producer startup is waiting for every producer PREPARE ACK"
                )
        if producer is not None and state.phase in {
            RolloutPhase.DUAL_WRITE,
            RolloutPhase.CONSUMER_ACK,
        }:
            previous = _load_generation_schema_bundle(
                root,
                generation_id=authority.previous_generation_id,
            )
            bindings.append(
                RuntimeSchemaDualWriteBinding(
                    service_id=manifest.service_id,
                    plan=authority.plan,
                    registry=authority.registry,
                    store_path=_schema_rollout_root(root, plan_id) / "state.sqlite3",
                    old_declaration=previous.channel(authority.plan.dataset_id).declaration,
                    new_declaration=candidate.channel(authority.plan.dataset_id).declaration,
                )
            )
        if (
            producer is not None
            and state.phase is RolloutPhase.CUTOVER
            and not _participant_has_ack(
                store,
                plan_id=plan_id,
                phase=RolloutPhase.CUTOVER,
                participant_id=manifest.service_id,
            )
        ):
            store.acknowledge(
                plan_id=plan_id,
                expected_revision=state.revision,
                phase=RolloutPhase.CUTOVER,
                participant_id=manifest.service_id,
                participant_fingerprint=producer.contract_fingerprint,
                declaration_fingerprint=authority.plan.new_declaration_fingerprint,
                now=observed_at,
                operation_id=f"service-cutover:{generation_id}:{manifest.service_id}",
            )
        if state.phase is RolloutPhase.CONSUMER_ACK:
            for consumer in authority.registry.consumers:
                if consumer.service_id != manifest.service_id or _consumer_has_receipt(
                    store,
                    plan_id=plan_id,
                    consumer_id=consumer.consumer_id,
                ):
                    continue
                if consumer.requires_serving_generation_ack:
                    bindings.append(
                        RuntimeSchemaConsumerAckBinding(
                            service_id=manifest.service_id,
                            consumer=consumer,
                            plan=authority.plan,
                            registry=authority.registry,
                            store_path=(_schema_rollout_root(root, plan_id) / "state.sqlite3"),
                        )
                    )
                    continue
                receipt = ConsumerCapabilityReceipt(
                    consumer_id=consumer.consumer_id,
                    service_id=consumer.service_id,
                    code_commit=consumer.code_commit,
                    dataset_id=consumer.dataset_id,
                    min_readable_schema_version=consumer.min_readable_schema_version,
                    max_readable_schema_version=consumer.max_readable_schema_version,
                    required_fields=consumer.required_fields,
                    serving_physical_schema_fingerprint=(
                        authority.plan.serving_physical_schema_fingerprint
                    ),
                    observed_generation_id=generation_id,
                    serving_generation_id=None,
                    available_at=observed_at,
                )
                store.acknowledge_consumer(
                    plan_id=plan_id,
                    expected_revision=store.get_state(plan_id).revision,
                    receipt=receipt,
                    now=observed_at,
                    operation_id=(f"service-capability:{generation_id}:{consumer.consumer_id}"),
                )
    return tuple(bindings)


def advance_runtime_schema_rollout(
    runtime_root: Path,
    *,
    plan_id: str,
    expected_revision: int,
    target_phase: RolloutPhase,
    now: datetime,
    operation_id: str,
) -> SchemaRolloutState:
    root = _absolute_runtime_root(runtime_root)
    authority, store = load_runtime_schema_rollout(root, plan_id=plan_id)
    if _current_target(root) != f"generations/{authority.target_generation_id}":
        raise RuntimeSchemaCompatibilityError(
            "runtime schema rollout target is no longer the current generation"
        )
    return store.advance(
        plan_id=plan_id,
        expected_revision=expected_revision,
        target_phase=target_phase,
        now=now,
        operation_id=operation_id,
    )


def rollback_runtime_schema_rollout(
    runtime_root: Path,
    *,
    plan_id: str,
    expected_revision: int,
    reason: str,
    now: datetime,
    operation_id: str,
) -> SchemaRolloutState:
    """Restore the old generation after durably recording rollback intent."""

    root = _absolute_runtime_root(runtime_root)
    authority, store = load_runtime_schema_rollout(root, plan_id=plan_id)
    state = store.rollback(
        plan_id=plan_id,
        expected_revision=expected_revision,
        reason=reason,
        now=now,
        operation_id=operation_id,
    )
    _replace_current(
        root,
        target=f"generations/{authority.previous_generation_id}",
    )
    return state


def _schema_bootstrap_payload(
    *,
    producer_commit: str,
    contract_content_hash: str,
    reason: str,
    previous_generation: str | None,
) -> bytes:
    values = {
        "schema_version": 1,
        "status": "explicit_bootstrap",
        "producer_commit": producer_commit,
        "contract_content_hash": contract_content_hash,
        "reason": reason,
        "previous_generation": previous_generation,
    }
    payload = {**values, "content_hash": canonical_sha256(values)}
    return json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _validate_schema_bootstrap_payload(
    payload: bytes,
    *,
    expected_contract_hash: str,
) -> None:
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeSchemaCompatibilityError(
            "previous runtime schema bootstrap audit is invalid"
        ) from exc
    if not isinstance(decoded, dict):
        raise RuntimeSchemaCompatibilityError("previous runtime schema bootstrap audit is invalid")
    expected_keys = {
        "schema_version",
        "status",
        "producer_commit",
        "contract_content_hash",
        "reason",
        "previous_generation",
        "content_hash",
    }
    if set(decoded) != expected_keys:
        raise RuntimeSchemaCompatibilityError("previous runtime schema bootstrap audit is invalid")
    content_hash = decoded.pop("content_hash")
    if (
        decoded["schema_version"] != 1
        or decoded["status"] != "explicit_bootstrap"
        or not isinstance(decoded["reason"], str)
        or not decoded["reason"].strip()
        or decoded["contract_content_hash"] != expected_contract_hash
        or content_hash != canonical_sha256(decoded)
    ):
        raise RuntimeSchemaCompatibilityError(
            "previous runtime schema bootstrap audit hash or identity is invalid"
        )
    canonical = json.dumps(
        {**decoded, "content_hash": content_hash},
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if canonical != payload:
        raise RuntimeSchemaCompatibilityError(
            "previous runtime schema bootstrap audit is not canonical"
        )


def _write_secure_file(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _persist_schema_v1_migration_audit(
    root: Path,
    audit: RuntimeSchemaV1MigrationAudit,
) -> Path:
    audit_root = root / "control" / "schema-migrations"
    _ensure_owned_descendant(root, audit_root)
    payload = _canonical_model_payload(audit)
    target = audit_root / f"{audit.content_hash}.json"
    if target.exists() or target.is_symlink():
        observed = target.lstat()
        if (
            not stat.S_ISREG(observed.st_mode)
            or stat.S_ISLNK(observed.st_mode)
            or observed.st_uid != os.getuid()
            or stat.S_IMODE(observed.st_mode) != 0o600
            or target.read_bytes() != payload
        ):
            raise RuntimeSchemaCompatibilityError(
                "runtime schema v1 migration audit conflicts with immutable history"
            )
        return target
    _write_secure_file(target, payload)
    _fsync_directory(audit_root)
    return target


def _validate_existing_generation(
    generation: Path,
    expected_files: Mapping[str, bytes],
) -> None:
    observed = generation.lstat()
    if (
        not stat.S_ISDIR(observed.st_mode)
        or stat.S_ISLNK(observed.st_mode)
        or observed.st_uid != os.getuid()
        or stat.S_IMODE(observed.st_mode) != 0o700
    ):
        raise ValueError("existing runtime generation is not a safely owned directory")
    actual_files = {
        path.relative_to(generation).as_posix() for path in generation.rglob("*") if path.is_file()
    }
    if actual_files != set(expected_files):
        raise ValueError("existing runtime generation contents do not match bundle")
    for relative, payload in expected_files.items():
        path = generation / relative
        file_state = path.lstat()
        if (
            not stat.S_ISREG(file_state.st_mode)
            or stat.S_ISLNK(file_state.st_mode)
            or file_state.st_uid != os.getuid()
            or stat.S_IMODE(file_state.st_mode) != 0o600
            or path.read_bytes() != payload
        ):
            raise ValueError("existing runtime generation file does not match bundle")


def _validate_deployment_profile_payload(
    payload: bytes | None,
    *,
    deployment_profile_id: str | None,
    producer_commit: str,
) -> bytes | None:
    if payload is None:
        return None
    if deployment_profile_id is None:
        raise ValueError("deployment profile payload requires a profile identity")
    if not isinstance(payload, bytes) or not payload or len(payload) > 16 * 1024 * 1024:
        raise ValueError("deployment profile payload is invalid")
    try:
        decoded = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("deployment profile payload is invalid") from exc
    if not isinstance(decoded, dict):
        raise ValueError("deployment profile payload must be an object")
    canonical = json.dumps(
        decoded,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    identity_payload = dict(decoded)
    observed_profile_id = identity_payload.pop("profile_id", None)
    if (
        canonical != payload
        or observed_profile_id != deployment_profile_id
        or decoded.get("producer_commit") != producer_commit
        or canonical_sha256(identity_payload) != deployment_profile_id
    ):
        raise ValueError("deployment profile payload identity differs")
    return payload


def _reject_legacy_plaintext_credentials(root: Path) -> None:
    generations = root / "generations"
    if not generations.exists():
        return
    if generations.is_symlink() or not generations.is_dir():
        raise ValueError("runtime generations path is unsafe")
    for generation in generations.iterdir():
        secrets = generation / "secrets"
        if secrets.exists() or secrets.is_symlink():
            raise ValueError("legacy plaintext runtime secrets must be removed before deployment")


def _validate_current_pointer(current: Path) -> None:
    if not current.is_symlink() and not current.exists():
        return
    if not current.is_symlink():
        raise ValueError("runtime current pointer must be a symlink")
    target = os.readlink(current)
    parts = Path(target).parts
    if (
        len(parts) != 2
        or parts[0] != "generations"
        or _GENERATION_PATTERN.fullmatch(parts[1]) is None
    ):
        raise ValueError("runtime current pointer escapes the generation directory")


def _current_target(root: Path) -> str | None:
    current = root / "current"
    _validate_current_pointer(current)
    return os.readlink(current) if current.is_symlink() else None


def _replace_current(root: Path, *, target: str | None) -> None:
    current = root / "current"
    _validate_current_pointer(current)
    if target is not None:
        parts = Path(target).parts
        if (
            len(parts) != 2
            or parts[0] != "generations"
            or _GENERATION_PATTERN.fullmatch(parts[1]) is None
        ):
            raise ValueError("runtime replacement pointer escapes the generation directory")
    if current.is_symlink() and os.readlink(current) == target:
        return
    if target is None:
        if current.is_symlink():
            current.unlink()
            _fsync_directory(root)
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=".current-", dir=root)
    os.close(descriptor)
    os.unlink(temporary_name)
    temporary = Path(temporary_name)
    try:
        os.symlink(target, temporary)
        _fsync_directory(root)
        os.replace(temporary, current)
        _fsync_directory(root)
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


def _publish_current(root: Path, *, generation_hash: str) -> None:
    _replace_current(root, target=f"generations/{generation_hash}")


def _record_rollback_failure(
    root: Path,
    *,
    generation_hash: str,
    previous_runtime_target: str | None,
    deployment_error: BaseException,
    runtime_rollback_error: BaseException | None,
    credential_rollback_error: BaseException | None,
    fail_closed_error: BaseException | None,
) -> Path:
    audit_root = root / "failed-deployments"
    _ensure_owned_descendant(root, audit_root)
    try:
        runtime_current: str | None = _current_target(root)
    except BaseException as exc:
        runtime_current = f"invalid:{type(exc).__name__}"
    payload = json.dumps(
        {
            "schema_version": 1,
            "status": "rollback_failed",
            "generation_hash": generation_hash,
            "deployment_error": type(deployment_error).__name__,
            "previous_runtime_target": previous_runtime_target,
            "runtime_current": runtime_current,
            "runtime_rollback": ("failed" if runtime_rollback_error is not None else "restored"),
            "runtime_rollback_error": (
                type(runtime_rollback_error).__name__
                if runtime_rollback_error is not None
                else None
            ),
            "credential_rollback": (
                "failed" if credential_rollback_error is not None else "restored"
            ),
            "credential_rollback_error": (
                type(credential_rollback_error).__name__
                if credential_rollback_error is not None
                else None
            ),
            "fail_closed": "failed" if fail_closed_error is not None else "enforced",
            "fail_closed_error": (
                type(fail_closed_error).__name__ if fail_closed_error is not None else None
            ),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    descriptor, temporary_name = tempfile.mkstemp(prefix=".rollback-", dir=audit_root)
    temporary = Path(temporary_name)
    target = audit_root / f"{generation_hash}-{time.time_ns()}.json"
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        _fsync_directory(audit_root)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            temporary.unlink()
    return target


def _record_recovery_failure(
    root: Path,
    *,
    generation_hash: str,
    previous_runtime_target: str | None,
    recovery_action: str,
    recovery_error: BaseException,
    fail_closed_error: BaseException | None,
) -> Path:
    audit_root = root / "failed-deployments"
    _ensure_owned_descendant(root, audit_root)
    try:
        runtime_current: str | None = _current_target(root)
    except BaseException as exc:
        runtime_current = f"invalid:{type(exc).__name__}"
    payload = json.dumps(
        {
            "schema_version": 1,
            "status": "recovery_failed",
            "generation_hash": generation_hash,
            "previous_runtime_target": previous_runtime_target,
            "recovery_action": recovery_action,
            "recovery_error": type(recovery_error).__name__,
            "runtime_current": runtime_current,
            "fail_closed": "failed" if fail_closed_error is not None else "enforced",
            "fail_closed_error": (
                type(fail_closed_error).__name__ if fail_closed_error is not None else None
            ),
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    descriptor, temporary_name = tempfile.mkstemp(prefix=".recovery-", dir=audit_root)
    temporary = Path(temporary_name)
    target = audit_root / f"{generation_hash}-{time.time_ns()}.json"
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = -1
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        _fsync_directory(audit_root)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            temporary.unlink()
    return target


def validate_runtime_deployment_bundle(
    runtime_root: Path,
    *,
    producer_commit: str,
    manifests: tuple[RuntimeServiceManifest, ...],
    capability_env: Mapping[str, Mapping[str, str]],
    deployment_profile_id: str | None = None,
    deployment_profile_payload: bytes | None = None,
    schema_bootstrap_reason: str | None = None,
    schema_v1_migration: RuntimeSchemaV1MigrationAuthorization | None = None,
) -> tuple[RuntimeServiceManifest, ...]:
    """Run the installer's topology, authority, secret, and schema checks without writes."""
    if _COMMIT_PATTERN.fullmatch(producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    if (
        deployment_profile_id is not None
        and _GENERATION_PATTERN.fullmatch(deployment_profile_id) is None
    ):
        raise ValueError("deployment profile id must be a full lowercase SHA-256")
    profile_payload = _validate_deployment_profile_payload(
        deployment_profile_payload,
        deployment_profile_id=deployment_profile_id,
        producer_commit=producer_commit,
    )
    root = _absolute_runtime_root(runtime_root)
    if not isinstance(manifests, tuple) or not manifests:
        raise ValueError("manifests must be a non-empty tuple")
    canonical = [_canonical_manifest(manifest) for manifest in manifests]
    service_ids = [manifest.service_id for manifest, _ in canonical]
    if len(service_ids) != len(set(service_ids)):
        raise ValueError("runtime bundle contains duplicate service_id values")
    ordered = tuple(sorted(canonical, key=lambda item: item[0].service_id))
    validated_manifests = tuple(manifest for manifest, _ in ordered)
    _validate_runtime_topology(validated_manifests)
    for manifest in validated_manifests:
        _validate_manifest_authority(
            manifest,
            producer_commit=producer_commit,
            runtime_root=root,
        )
    capabilities = _validate_capabilities(validated_manifests, capability_env)
    payload_by_service = {manifest.service_id: payload for manifest, payload in ordered}
    _reject_plaintext_secrets(
        payload_by_service,
        capabilities,
        deployment_profile_payload=profile_payload,
    )
    if schema_bootstrap_reason is not None and schema_v1_migration is not None:
        raise RuntimeSchemaBootstrapRequiredError(
            "runtime schema bootstrap and v1 migration are mutually exclusive"
        )
    if schema_bootstrap_reason is not None:
        if not isinstance(schema_bootstrap_reason, str):
            raise TypeError("schema bootstrap reason must be a string")
        schema_bootstrap_reason = schema_bootstrap_reason.strip()
        if not schema_bootstrap_reason:
            raise RuntimeSchemaBootstrapRequiredError(
                "explicit runtime schema bootstrap requires a non-empty audit reason"
            )
    schema_contract = _build_runtime_schema_contract_bundle(
        validated_manifests,
        producer_commit=producer_commit,
    )
    if schema_v1_migration is not None:
        if not isinstance(schema_v1_migration, RuntimeSchemaV1MigrationAuthorization):
            raise TypeError("schema v1 migration must be an explicit authorization")
        previous_schema_target, legacy_payload = _read_current_legacy_v1_schema_contract(root)
        build_runtime_schema_v1_migration_audit(
            legacy_payload=legacy_payload,
            candidate=schema_contract,
            reason=schema_v1_migration.reason,
            previous_generation_id=Path(previous_schema_target).name,
            reviewed_lifecycles=schema_v1_migration.reviewed_lifecycles,
            migrated_at=schema_v1_migration.migrated_at,
        )
        return validated_manifests
    _, previous_schema_contract, _ = _read_current_schema_contract(
        root,
        allow_missing_contract=schema_bootstrap_reason is not None,
    )
    if previous_schema_contract is None:
        if schema_bootstrap_reason is None:
            raise RuntimeSchemaBootstrapRequiredError(
                "first runtime schema generation requires explicit audited bootstrap"
            )
    else:
        if schema_bootstrap_reason is not None:
            raise RuntimeSchemaBootstrapRequiredError(
                "runtime schema registry is already bootstrapped; bootstrap cannot bypass preflight"
            )
        _validate_runtime_schema_transition(
            previous=previous_schema_contract,
            candidate=schema_contract,
        )
    return validated_manifests


def install_runtime_deployment_bundle(
    runtime_root: Path,
    *,
    producer_commit: str,
    manifests: tuple[RuntimeServiceManifest, ...],
    capability_env: Mapping[str, Mapping[str, str]],
    deployment_profile_id: str | None = None,
    deployment_profile_payload: bytes | None = None,
    schema_bootstrap_reason: str | None = None,
    schema_v1_migration: RuntimeSchemaV1MigrationAuthorization | None = None,
) -> RuntimeDeploymentReceipt:
    """Install one coherent runtime generation without starting any service."""

    if _COMMIT_PATTERN.fullmatch(producer_commit) is None:
        raise ValueError("producer commit must be a full lowercase Git SHA")
    if (
        deployment_profile_id is not None
        and _GENERATION_PATTERN.fullmatch(deployment_profile_id) is None
    ):
        raise ValueError("deployment profile id must be a full lowercase SHA-256")
    profile_payload = _validate_deployment_profile_payload(
        deployment_profile_payload,
        deployment_profile_id=deployment_profile_id,
        producer_commit=producer_commit,
    )
    root = _absolute_runtime_root(runtime_root)
    if not isinstance(manifests, tuple) or not manifests:
        raise ValueError("manifests must be a non-empty tuple")

    canonical: list[tuple[RuntimeServiceManifest, bytes]] = [
        _canonical_manifest(manifest) for manifest in manifests
    ]
    service_ids = [manifest.service_id for manifest, _ in canonical]
    if len(service_ids) != len(set(service_ids)):
        raise ValueError("runtime bundle contains duplicate service_id values")
    _validate_runtime_topology(tuple(manifest for manifest, _ in canonical))
    for manifest, _ in canonical:
        _validate_manifest_authority(
            manifest,
            producer_commit=producer_commit,
            runtime_root=root,
        )
    ordered = tuple(sorted(canonical, key=lambda item: item[0].service_id))
    validated_manifests = tuple(manifest for manifest, _ in ordered)
    capabilities = _validate_capabilities(validated_manifests, capability_env)
    payload_by_service = {manifest.service_id: payload for manifest, payload in ordered}
    _reject_plaintext_secrets(
        payload_by_service,
        capabilities,
        deployment_profile_payload=profile_payload,
    )

    if schema_bootstrap_reason is not None and schema_v1_migration is not None:
        raise RuntimeSchemaBootstrapRequiredError(
            "runtime schema bootstrap and v1 migration are mutually exclusive"
        )
    if schema_bootstrap_reason is not None:
        if not isinstance(schema_bootstrap_reason, str):
            raise TypeError("schema bootstrap reason must be a string")
        schema_bootstrap_reason = schema_bootstrap_reason.strip()
        if not schema_bootstrap_reason:
            raise RuntimeSchemaBootstrapRequiredError(
                "explicit runtime schema bootstrap requires a non-empty audit reason"
            )
    schema_contract = _build_runtime_schema_contract_bundle(
        validated_manifests,
        producer_commit=producer_commit,
    )
    schema_contract_payload = _canonical_model_payload(schema_contract)
    migration_audit: RuntimeSchemaV1MigrationAudit | None = None
    if schema_v1_migration is not None:
        if not isinstance(schema_v1_migration, RuntimeSchemaV1MigrationAuthorization):
            raise TypeError("schema v1 migration must be an explicit authorization")
        previous_schema_target, legacy_payload = _read_current_legacy_v1_schema_contract(root)
        previous_schema_contract = None
        previous_bootstrap_payload = None
        migration_audit = build_runtime_schema_v1_migration_audit(
            legacy_payload=legacy_payload,
            candidate=schema_contract,
            reason=schema_v1_migration.reason,
            previous_generation_id=Path(previous_schema_target).name,
            reviewed_lifecycles=schema_v1_migration.reviewed_lifecycles,
            migrated_at=schema_v1_migration.migrated_at,
        )
    else:
        previous_schema_target, previous_schema_contract, previous_bootstrap_payload = (
            _read_current_schema_contract(
                root,
                allow_missing_contract=schema_bootstrap_reason is not None,
            )
        )
    schema_bootstrap_payload: bytes | None = None
    if migration_audit is not None:
        pass
    elif previous_schema_contract is None:
        if schema_bootstrap_reason is None:
            raise RuntimeSchemaBootstrapRequiredError(
                "first runtime schema generation requires explicit audited bootstrap"
            )
        schema_bootstrap_payload = _schema_bootstrap_payload(
            producer_commit=producer_commit,
            contract_content_hash=schema_contract.content_hash,
            reason=schema_bootstrap_reason,
            previous_generation=previous_schema_target,
        )
    else:
        if schema_bootstrap_reason is not None:
            raise RuntimeSchemaBootstrapRequiredError(
                "runtime schema registry is already bootstrapped; bootstrap cannot bypass preflight"
            )
        _validate_runtime_schema_transition(
            previous=previous_schema_contract,
            candidate=schema_contract,
        )
        if previous_schema_contract == schema_contract:
            schema_bootstrap_payload = previous_bootstrap_payload

    instance_mapping = {
        manifest.service_id: _instance_name(manifest.service_id) for manifest in validated_manifests
    }
    unit_mapping = {
        manifest.service_id: _systemd_unit_name(
            manifest,
            instance_mapping[manifest.service_id],
        )
        for manifest in validated_manifests
    }
    capability_payloads: dict[str, bytes] = {}
    for manifest in validated_manifests:
        instance = instance_mapping[manifest.service_id]
        if (
            manifest.plane is RuntimeServicePlane.LIVE
            and manifest.service_kind not in _DEDICATED_NO_CAPABILITY_KINDS
        ) or manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
            capability_payloads[instance] = serialize_runtime_capabilities(
                capabilities[manifest.service_id]
            )
    generation_basis = _RuntimeGenerationBasis(
        schema_version=1,
        producer_commit=producer_commit,
        deployment_profile_id=deployment_profile_id,
        manifest_sha256={
            service_id: hashlib.sha256(payload).hexdigest()
            for service_id, payload in sorted(payload_by_service.items())
        },
        capability_sha256={
            instance: hashlib.sha256(payload).hexdigest()
            for instance, payload in sorted(capability_payloads.items())
        },
        instance_mapping=instance_mapping,
        unit_mapping=unit_mapping,
        schema_contract_sha256=hashlib.sha256(schema_contract_payload).hexdigest(),
        schema_bootstrap_sha256=(
            hashlib.sha256(schema_bootstrap_payload).hexdigest()
            if schema_bootstrap_payload is not None
            else None
        ),
    )
    generation_hash = canonical_sha256(generation_basis.model_dump(mode="python"))
    files: dict[str, bytes] = {
        "generation-basis.json": _canonical_model_payload(generation_basis),
        "runtime.env": _environment_payload(producer_commit, generation_hash),
        "schema-contracts.json": schema_contract_payload,
        **{
            f"manifests/{instance_mapping[service_id]}.json": payload
            for service_id, payload in sorted(payload_by_service.items())
        },
    }
    if profile_payload is not None:
        files["deployment-profile.json"] = profile_payload
    if schema_bootstrap_payload is not None:
        files["schema-bootstrap.json"] = schema_bootstrap_payload
    credential_plaintexts = {
        instance: serialize_runtime_credential(
            service_id=manifest.service_id,
            service_kind=manifest.service_kind,
            instance_name=instance,
            bundle_generation=generation_hash,
            values=capabilities[manifest.service_id],
        )
        for manifest in validated_manifests
        if (
            (
                manifest.plane is RuntimeServicePlane.LIVE
                and manifest.service_kind not in _DEDICATED_NO_CAPABILITY_KINDS
            )
            or manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION
        )
        for instance in (instance_mapping[manifest.service_id],)
    }

    _ensure_owned_directory(root)
    _reject_legacy_plaintext_credentials(root)
    _ensure_owned_descendant(root, root / "control")
    for plane in {manifest.plane for manifest in validated_manifests}:
        _ensure_owned_descendant(root, root / plane.value)
    for manifest in validated_manifests:
        instance = instance_mapping[manifest.service_id]
        if manifest.service_kind is RuntimeServiceKind.REFERENCE_SLOW_SOURCE:
            _ensure_owned_descendant(root, root / "live" / "reference-slow")
            _ensure_owned_descendant(
                root,
                root / "control" / "reference-slow-sources" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.REFERENCE_SLOW_PUBLISHER:
            _ensure_owned_descendant(root, root / "authorities" / "reference-slow")
            _ensure_owned_descendant(
                root,
                root / "control" / "reference-slow-publishers" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.AUCTION_UNIVERSE_PUBLISHER:
            _ensure_owned_descendant(
                root,
                root / "authorities" / "auction-universe",
            )
            _ensure_owned_descendant(
                root,
                root / "control" / "auction-universe-publishers" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.AUCTION_MATCH_SOURCE:
            _ensure_owned_descendant(root, root / "live" / "auction-match")
            _ensure_owned_descendant(
                root,
                root / "control" / "auction-match-sources" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.MARKET_MINUTE_SOURCE:
            _ensure_owned_descendant(root, root / "live" / "market-minute")
            _ensure_owned_descendant(
                root,
                root / "control" / "market-minute-sources" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.WATCHLIST_QUOTE_SOURCE:
            _ensure_owned_descendant(root, root / "live" / "watchlist-quote")
            _ensure_owned_descendant(
                root,
                root / "control" / "watchlist-quote-sources" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.DAILY_CLOSE_SOURCE:
            _ensure_owned_descendant(root, root / "live" / "daily-close")
            _ensure_owned_descendant(
                root,
                root / "control" / "daily-close-sources" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.SHADOW_SESSION:
            _ensure_owned_descendant(root, root / "research" / "shadow-reports")
            _ensure_owned_descendant(
                root,
                root / "control" / "shadow-sessions" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR:
            _ensure_owned_descendant(root, root / "research" / "daily-pipeline")
            _ensure_owned_descendant(
                root,
                root / "control" / "daily-orchestrators" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.FEATURE_LIVE:
            _ensure_owned_descendant(root, root / "live" / "features")
            _ensure_owned_descendant(root, root / "control" / "features" / instance)
        elif manifest.service_kind is RuntimeServiceKind.CANDIDATE_PUBLISHER:
            _ensure_owned_descendant(root, Path(str(manifest.settings["snapshot_root"])))
            _ensure_owned_descendant(root, root / "control" / "candidates" / instance)
        elif manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE:
            state_parent = Path(str(manifest.settings["runner_state_path"])).parent
            _ensure_owned_descendant(root, state_parent)
            _ensure_owned_descendant(root, root / "control" / "strategies" / instance)
        elif manifest.service_kind is RuntimeServiceKind.SIGNAL_ROUTER:
            _ensure_owned_descendant(root, root / "live" / "signal-bus")
            _ensure_owned_descendant(root, root / "control" / "signal-routers" / instance)
        elif manifest.service_kind is RuntimeServiceKind.NOTIFIER:
            _ensure_owned_descendant(root, root / "live" / "notifications" / instance)
            _ensure_owned_descendant(root, root / "control" / "notifiers" / instance)
        elif manifest.service_kind is RuntimeServiceKind.PAPER_BROKER:
            _ensure_owned_descendant(root, root / "live" / "paper-brokers" / instance)
            _ensure_owned_descendant(root, root / "control" / "paper-brokers" / instance)
        elif manifest.service_kind is RuntimeServiceKind.PAPER_CONSTRAINT_PUBLISHER:
            _ensure_owned_descendant(root, root / "authorities" / "paper-execution")
            _ensure_owned_descendant(root, root / "control" / "paper-constraints" / instance)
        elif manifest.service_kind is RuntimeServiceKind.RUNTIME_HEALTH_PUBLISHER:
            _ensure_owned_descendant(root, root / "control" / "authority-runtime-health")
            _ensure_owned_descendant(
                root,
                root / "control" / "runtime-health-publishers" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.LAB_JOBS_PUBLISHER:
            _ensure_owned_descendant(
                root,
                root / "research" / "serving-authorities" / "lab-jobs",
            )
            _ensure_owned_descendant(
                root,
                root / "control" / "lab-jobs-publishers" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.LAB_ARTIFACT_CATALOG:
            _ensure_owned_descendant(
                root,
                root / "research" / "artifact-catalogs" / instance,
            )
            _ensure_owned_descendant(
                root,
                artifact_retention_state_root(root) / "catalog-registration-outbox",
            )
            _ensure_owned_descendant(
                root,
                root / "control" / "artifact-catalogs" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.ARTIFACT_RETENTION:
            _ensure_owned_descendant(
                root,
                root / "research" / "artifact-retention" / instance,
            )
            _ensure_owned_descendant(root, root / "research" / "final-artifacts")
            _ensure_owned_descendant(
                root,
                root / "control" / "artifact-retention" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.PROMOTIONS_PUBLISHER:
            _ensure_owned_descendant(
                root,
                root / "research" / "serving-authorities" / "promotions",
            )
            _ensure_owned_descendant(
                root,
                root / "control" / "promotions-publishers" / instance,
            )
        elif manifest.service_kind is RuntimeServiceKind.SERVING_PUBLISHER:
            _ensure_owned_descendant(
                root,
                root / "control" / "serving-publishers" / instance,
            )
    generations = root / "generations"
    _ensure_owned_directory(generations)
    target = generations / generation_hash
    staging = Path(tempfile.mkdtemp(prefix=".staging-", dir=root))
    staging.chmod(0o700)
    created_target = False
    credential_transaction = None
    previous_runtime_target = _current_target(root)
    target_runtime_pointer = f"generations/{generation_hash}"
    credential_recovery_in_progress = False
    credential_recovery_action = (
        "commit" if previous_runtime_target == target_runtime_pointer else "rollback"
    )
    try:
        manifest_directory = staging / "manifests"
        manifest_directory.mkdir(mode=0o700)
        manifest_directory.chmod(0o700)
        for relative, payload in sorted(files.items()):
            _write_secure_file(staging / relative, payload)
        _fsync_directory(staging / "manifests")
        _fsync_directory(staging)

        if target.exists() or target.is_symlink():
            _validate_existing_generation(target, files)
        else:
            os.replace(staging, target)
            created_target = True
            _fsync_directory(generations)
        if migration_audit is not None:
            _persist_schema_v1_migration_audit(root, migration_audit)
        if credential_plaintexts:
            credential_recovery_in_progress = True
            recovery = _recover_runtime_credentials(
                bundle_generation=generation_hash,
                instances=tuple(sorted(credential_plaintexts)),
                action=credential_recovery_action,
            )
            expected_recovery_outcome = (
                "committed" if credential_recovery_action == "commit" else "rolled_back"
            )
            if recovery.outcome not in {"none", expected_recovery_outcome}:
                raise RuntimeError("credential recovery returned an incoherent outcome")
            credential_recovery_in_progress = False
            if recovery.outcome != "committed":
                credential_transaction = _seal_runtime_credentials(credential_plaintexts)
        _publish_current(root, generation_hash=generation_hash)
        if credential_transaction is not None:
            credential_transaction.commit()
    except BaseException as deployment_error:
        if credential_recovery_in_progress:
            fail_closed_error: BaseException | None = None
            try:
                _replace_current(root, target=None)
            except BaseException as exc:
                fail_closed_error = exc
            audit_path = _record_recovery_failure(
                root,
                generation_hash=generation_hash,
                previous_runtime_target=previous_runtime_target,
                recovery_action=credential_recovery_action,
                recovery_error=deployment_error,
                fail_closed_error=fail_closed_error,
            )
            raise RuntimeDeploymentRecoveryError(
                f"runtime credential recovery failed closed; audit preserved at {audit_path}"
            ) from deployment_error
        runtime_rollback_error: BaseException | None = None
        credential_rollback_error: BaseException | None = None
        try:
            _replace_current(root, target=previous_runtime_target)
        except BaseException as exc:
            runtime_rollback_error = exc
        if credential_transaction is not None:
            try:
                credential_transaction.rollback()
            except BaseException as exc:
                credential_rollback_error = exc
        if runtime_rollback_error is not None or credential_rollback_error is not None:
            fail_closed_error: BaseException | None = None
            try:
                _replace_current(root, target=None)
            except BaseException as exc:
                fail_closed_error = exc
            audit_path = _record_rollback_failure(
                root,
                generation_hash=generation_hash,
                previous_runtime_target=previous_runtime_target,
                deployment_error=deployment_error,
                runtime_rollback_error=runtime_rollback_error,
                credential_rollback_error=credential_rollback_error,
                fail_closed_error=fail_closed_error,
            )
            raise RuntimeDeploymentRollbackError(
                f"runtime deployment rollback failed closed; audit preserved at {audit_path}"
            ) from deployment_error
        if created_target:
            shutil.rmtree(target)
            _fsync_directory(generations)
        raise
    finally:
        if staging.exists():
            shutil.rmtree(staging)

    return RuntimeDeploymentReceipt(
        runtime_root=root,
        producer_commit=producer_commit,
        generation_hash=generation_hash,
        deployment_profile_id=deployment_profile_id,
        previous_generation_hash=(
            Path(previous_runtime_target).name
            if previous_runtime_target is not None
            and Path(previous_runtime_target).name != generation_hash
            else None
        ),
        instance_mapping=instance_mapping,
        unit_mapping=unit_mapping,
    )


__all__ = [
    "RuntimeDeploymentReceipt",
    "RuntimeDeploymentRecoveryError",
    "RuntimeDeploymentRollbackError",
    "RuntimeSchemaBootstrapRequiredError",
    "RuntimeSchemaRolloutAuthority",
    "RuntimeSchemaV1MigrationAuthorization",
    "activate_runtime_deployment_generation",
    "advance_runtime_schema_rollout",
    "changed_runtime_schema_channels",
    "install_runtime_deployment_bundle",
    "load_runtime_schema_rollout",
    "load_runtime_schema_service_bindings",
    "load_current_runtime_deployment_receipt",
    "load_current_runtime_deployment_receipt_unbound",
    "load_runtime_deployment_generation_receipt",
    "prepare_runtime_schema_rollout",
    "rollback_runtime_schema_rollout",
    "runtime_schema_rollout_plan_ids_for_generation",
    "strategy_live_producer_version",
    "validate_runtime_deployment_bundle",
    "validate_runtime_deployment_topology",
    "validate_strategy_live_completion_manifest",
]
