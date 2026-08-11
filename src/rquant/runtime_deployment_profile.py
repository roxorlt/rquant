"""Immutable declarative profile for one isolated runtime deployment."""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import TYPE_CHECKING, Annotated, Literal
from urllib.parse import urlsplit

from pydantic import Field, field_serializer, field_validator, model_validator

from rquant.daily_receipt_socket_authority import (
    DAILY_RECEIPT_SOCKET_ENDPOINT,
    DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
)
from rquant.runtime_capabilities import CAPABILITY_KEYS, LoadedRuntimeCapabilities
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_deployment_bundle import (
    RuntimeDeploymentReceipt,
    RuntimeSchemaV1MigrationAuthorization,
    activate_runtime_deployment_generation,
    changed_runtime_schema_channels,
    load_current_runtime_deployment_receipt_unbound,
    load_runtime_deployment_generation_receipt,
    load_runtime_schema_rollout,
    prepare_runtime_schema_rollout,
    rollback_runtime_schema_rollout,
    runtime_schema_rollout_plan_ids_for_generation,
    validate_strategy_live_completion_manifest,
)
from rquant.runtime_deployment_bundle import (
    install_runtime_deployment_bundle as _install_runtime_deployment_bundle,
)
from rquant.runtime_deployment_bundle import (
    validate_runtime_deployment_bundle as _validate_runtime_deployment_bundle,
)
from rquant.runtime_recovery_artifacts import (
    REQUIRED_RECOVERY_ARTIFACT_KINDS,
    RealRecoveryArtifactKind,
    validate_complete_recovery_artifact_graph,
)
from rquant.runtime_schema_registry import build_runtime_schema_contract_bundle
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.schema_compatibility import RolloutPhase
from rquant.strict_json import canonical_model_json_bytes, strict_model_validate_canonical_json

if TYPE_CHECKING:
    from rquant.preflight import RuntimeRecoveryPreflightConfig

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
CommitSha = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
_REQUIRED_CAPABILITIES = {
    RuntimeServiceKind.REFERENCE_SLOW_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
    RuntimeServiceKind.AUCTION_MATCH_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
    RuntimeServiceKind.MARKET_MINUTE_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
    RuntimeServiceKind.DAILY_CLOSE_SOURCE: frozenset({"TUSHARE_TOKEN_MAIN"}),
}
_NOTIFIER_DELIVERY_CAPABILITIES = frozenset({"PUSHDEER_KEYS", "PUSHPLUS_TOKENS"})
_MAX_PROFILE_BYTES = 16 * 1024 * 1024
PositiveSeconds = Annotated[int, Field(strict=True, ge=1, le=86_400)]
ObservationSeconds = Annotated[int, Field(strict=True, ge=60, le=31 * 86_400)]
RecoverySeconds = Annotated[int, Field(strict=True, ge=1, le=365 * 86_400)]
RuntimeMode = Literal["local-test", "linux-production"]
LINUX_PRODUCTION_RUNTIME_ROOT = Path("/home/lighthouse/rquant/data/runtime")
PRODUCTION_CANVAS_SIGNER_COMMAND = (
    "/usr/bin/sudo",
    "-n",
    "/usr/local/libexec/rquant-canvas-publication-signer",
)
PRODUCTION_PAGE_CONTROL_SERVICE_ID = "page-control.production.v1"
PRODUCTION_PAGE_CONTROL_INSTANCE_ID = "page-control-primary"
PRODUCTION_SHADOW_SIGNER_COMMAND = (
    "/usr/bin/sudo",
    "-n",
    "/usr/local/libexec/rquant-shadow-report-signer",
)
PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT = str(DAILY_RECEIPT_SOCKET_ENDPOINT)
PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH = DAILY_RECEIPT_TRUSTED_KEYRING_PATH
PRODUCTION_SHADOW_SERVICE_ID = "shadow.session.production.v1"
PRODUCTION_SHADOW_INSTANCE_ID = "shadow-session-primary"


class CanvasPublicationRuntimeProfile(RuntimeContractModel):
    """Public authority plus one opaque signer capability for PageControl."""

    schema_version: Literal[1] = 1
    active_key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    active_public_key_pem: str = Field(min_length=1, max_length=16_384)
    previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    signer_command: tuple[str, ...]
    consumer_service_id: str = Field(min_length=1, max_length=128)
    consumer_instance_id: str = Field(min_length=1, max_length=256)
    timeout_seconds: float = Field(default=5.0, ge=0.1, le=30.0)

    @field_validator("previous_public_key_pems")
    @classmethod
    def freeze_previous_public_keys(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        previous = dict(sorted(value.items()))
        if any(
            re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,127}", key_id) is None or not public_key
            for key_id, public_key in previous.items()
        ):
            raise ValueError("Canvas publication previous public keyring is invalid")
        return MappingProxyType(previous)

    @field_serializer("previous_public_key_pems")
    def serialize_previous_public_keys(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @field_validator("signer_command")
    @classmethod
    def require_absolute_signer_command(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not item for item in value):
            raise ValueError("Canvas publication signer command must be nonempty")
        executable = Path(value[0])
        if not executable.is_absolute() or executable != Path(os.path.abspath(executable)):
            raise ValueError("Canvas publication signer command must be absolute")
        return value

    @model_validator(mode="after")
    def reject_active_key_as_previous(self) -> CanvasPublicationRuntimeProfile:
        if self.active_key_id in self.previous_public_key_pems:
            raise ValueError("Canvas publication active key cannot also be previous")
        return self


class PageControlRuntimeProfile(RuntimeContractModel):
    """Non-secret authority binding for the loopback page command service."""

    endpoint: str
    outbox_path: Path
    data_dir: Path | None = None
    log_dir: Path | None = None
    page_projection_canvas_catalog_root: Path | None = None
    canvas_publication: CanvasPublicationRuntimeProfile | None = None

    @field_validator("endpoint")
    @classmethod
    def require_explicit_loopback_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("page control endpoint port is invalid") from exc
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "::1", "localhost"}
            or port is None
            or not 1 <= port <= 65_535
            or parsed.path != "/v1/commands"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("page control endpoint must be an explicit loopback command URL")
        return value

    @field_validator(
        "outbox_path",
        "data_dir",
        "log_dir",
        "page_projection_canvas_catalog_root",
    )
    @classmethod
    def require_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        normalized = Path(os.path.abspath(value))
        if not value.is_absolute() or value != normalized:
            raise ValueError("page control outbox path must be absolute and normalized")
        return value

    @field_serializer(
        "outbox_path",
        "data_dir",
        "log_dir",
        "page_projection_canvas_catalog_root",
    )
    def serialize_paths(self, value: Path | None) -> str | None:
        return None if value is None else str(value)


class ShadowRuntimeProfile(RuntimeContractModel):
    """Public verification authority and one opaque report signer for Shadow."""

    schema_version: Literal[1] = 1
    completion_active_key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    completion_active_public_key_pem: str = Field(min_length=1, max_length=16_384)
    completion_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    report_active_key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    report_active_public_key_pem: str = Field(min_length=1, max_length=16_384)
    report_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    signer_command: tuple[str, ...]
    report_producer_service_id: str = Field(min_length=1, max_length=128)
    report_producer_instance_id: str = Field(min_length=1, max_length=256)
    timeout_seconds: float = Field(default=5.0, ge=0.1, le=30.0)

    @field_validator(
        "completion_previous_public_key_pems",
        "report_previous_public_key_pems",
    )
    @classmethod
    def freeze_previous_public_keys(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        previous = dict(sorted(value.items()))
        if any(
            re.fullmatch(r"[a-z0-9][a-z0-9_.-]{0,127}", key_id) is None or not public_key
            for key_id, public_key in previous.items()
        ):
            raise ValueError("Shadow previous public keyring is invalid")
        return MappingProxyType(previous)

    @field_serializer(
        "completion_previous_public_key_pems",
        "report_previous_public_key_pems",
    )
    def serialize_previous_public_keys(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @field_validator("signer_command")
    @classmethod
    def require_absolute_signer_command(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not item for item in value):
            raise ValueError("Shadow signer command must be nonempty")
        executable = Path(value[0])
        if not executable.is_absolute() or executable != Path(os.path.abspath(executable)):
            raise ValueError("Shadow signer command must be absolute")
        return value

    @model_validator(mode="after")
    def reject_active_key_as_previous(self) -> ShadowRuntimeProfile:
        if self.completion_active_key_id in self.completion_previous_public_key_pems:
            raise ValueError("Shadow completion active key cannot also be previous")
        if self.report_active_key_id in self.report_previous_public_key_pems:
            raise ValueError("Shadow report active key cannot also be previous")
        return self


class LabHighWaterRuntimeProfile(RuntimeContractModel):
    """Immutable, public-only binding for the external Job Center authority."""

    schema_version: Literal[1] = 1
    app_env: Literal["prod"] = "prod"
    disable_dotenv: Literal[True] = True
    authority_command: tuple[str, ...]
    stable_identity: str = Field(min_length=1, max_length=512)
    trusted_keyring_path: Path
    timeout_seconds: float = Field(default=10.0, ge=0.1, le=300)
    allow_identity_rotation: bool = False
    production_mode: bool = False

    @field_validator("authority_command")
    @classmethod
    def require_nonempty_command(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or any(not part for part in value):
            raise ValueError("Lab high-water authority command must be nonempty")
        return value

    @field_validator("trusted_keyring_path")
    @classmethod
    def require_absolute_keyring_path(cls, value: Path) -> Path:
        normalized = Path(os.path.abspath(value))
        if not value.is_absolute() or value != normalized:
            raise ValueError("Lab high-water trusted keyring path must be absolute and normalized")
        return value

    @field_serializer("trusted_keyring_path")
    def serialize_keyring_path(self, value: Path) -> str:
        return str(value)

    @model_validator(mode="after")
    def require_fixed_production_capability(self) -> LabHighWaterRuntimeProfile:
        if self.production_mode:
            from rquant.lab_highwater_authority import PRODUCTION_LAB_HIGHWATER_COMMAND

            if self.authority_command != PRODUCTION_LAB_HIGHWATER_COMMAND:
                raise ValueError("production Lab high-water command must be the fixed sudo helper")
        return self


class RuntimeSchemaRolloutPolicy(RuntimeContractModel):
    channel_id: str = Field(min_length=1)
    state_path: Path
    stage_timeout_seconds: PositiveSeconds
    consumer_ack_max_age_seconds: PositiveSeconds
    retire_observation_seconds: ObservationSeconds = 86_400
    required_consumers: tuple[str, ...]

    @field_validator("state_path")
    @classmethod
    def require_absolute_state_path(cls, value: Path) -> Path:
        normalized = Path(os.path.abspath(value))
        if not value.is_absolute() or value != normalized:
            raise ValueError("schema rollout state path must be absolute and normalized")
        return value

    @field_serializer("state_path")
    def serialize_state_path(self, value: Path) -> str:
        return str(value)

    @field_validator("required_consumers")
    @classmethod
    def require_sorted_consumers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or tuple(sorted(set(value))) != value:
            raise ValueError("schema rollout required consumers must be unique and sorted")
        return value


class RuntimeRecoveryArtifactRoleBinding(RuntimeContractModel):
    logical_role: str = Field(min_length=1, max_length=256)
    kind: RealRecoveryArtifactKind
    source_path: str
    restore_path: str
    schema_version: str = Field(min_length=1, max_length=128)
    relations: tuple[str, ...] = ()
    references: Mapping[str, str] = Field(default_factory=dict)

    @field_validator("source_path", "restore_path")
    @classmethod
    def require_canonical_relative_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if (
            "\\" in value
            or not value
            or path.is_absolute()
            or str(path) != value
            or any(part in {"", ".", ".."} for part in path.parts)
        ):
            raise ValueError("recovery artifact path must be canonical relative POSIX")
        return value

    @field_validator("relations")
    @classmethod
    def require_canonical_relations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not relation for relation in value) or len(value) != len(set(value)):
            raise ValueError("recovery artifact relations must be unique and nonempty")
        return tuple(sorted(value))

    @field_validator("references")
    @classmethod
    def require_canonical_references(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if any(not key or not role for key, role in value.items()):
            raise ValueError("recovery artifact references must be nonempty")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("references")
    def serialize_references(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)


class RuntimeRecoveryProductionConfig(RuntimeContractModel):
    schema_version: Literal[1] = 1
    profile_generation: Sha256 | None = None
    backup_source_root: Path
    backup_publication_root: Path
    isolated_restore_root: Path
    service_state_path: Path
    service_receipt_root: Path
    backup_config_path: Path
    credential_file: Path
    artifact_roles: tuple[RuntimeRecoveryArtifactRoleBinding, ...]
    production_artifact_role: str = Field(min_length=1, max_length=256)
    paper_ledger_artifact_role: str = Field(min_length=1, max_length=256)
    signer_key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    max_rpo_seconds: RecoverySeconds
    max_rto_seconds: RecoverySeconds
    max_rehearsal_age_seconds: RecoverySeconds
    recovery_lease_seconds: RecoverySeconds
    recovery_max_attempts: Annotated[int, Field(strict=True, ge=1, le=100)]
    recovery_retry_delay_seconds: RecoverySeconds
    recovery_deadline_seconds: RecoverySeconds
    rehearsal_interval_seconds: RecoverySeconds

    @field_validator(
        "backup_source_root",
        "backup_publication_root",
        "isolated_restore_root",
        "service_state_path",
        "service_receipt_root",
        "backup_config_path",
        "credential_file",
    )
    @classmethod
    def require_canonical_absolute_path(cls, value: Path) -> Path:
        normalized = Path(os.path.abspath(value))
        if not value.is_absolute() or value != normalized:
            raise ValueError("recovery production paths must be absolute and normalized")
        return value

    @field_serializer(
        "backup_source_root",
        "backup_publication_root",
        "isolated_restore_root",
        "service_state_path",
        "service_receipt_root",
        "backup_config_path",
        "credential_file",
    )
    def serialize_paths(self, value: Path) -> str:
        return str(value)

    @field_validator("artifact_roles")
    @classmethod
    def require_canonical_artifact_roles(
        cls,
        value: tuple[RuntimeRecoveryArtifactRoleBinding, ...],
    ) -> tuple[RuntimeRecoveryArtifactRoleBinding, ...]:
        ordered = tuple(sorted(value, key=lambda role: role.logical_role))
        names = tuple(role.logical_role for role in ordered)
        if not names or len(names) != len(set(names)):
            raise ValueError("recovery artifact roles must be unique and nonempty")
        return ordered

    @staticmethod
    def _overlap(left: Path, right: Path) -> bool:
        return left == right or left.is_relative_to(right) or right.is_relative_to(left)

    @model_validator(mode="after")
    def validate_identity_and_policy(self) -> RuntimeRecoveryProductionConfig:
        if self._overlap(self.backup_publication_root, self.isolated_restore_root):
            raise ValueError("recovery backup publication and restore roots must be isolated")
        validate_complete_recovery_artifact_graph(
            self.artifact_roles,
            production_artifact_role=self.production_artifact_role,
            paper_ledger_artifact_role=self.paper_ledger_artifact_role,
        )
        if self.max_rto_seconds > self.recovery_deadline_seconds:
            raise ValueError("recovery RTO cannot exceed the execution deadline")
        if self.recovery_lease_seconds >= self.recovery_deadline_seconds:
            raise ValueError("recovery lease must be shorter than the execution deadline")
        if self.recovery_retry_delay_seconds >= self.recovery_deadline_seconds:
            raise ValueError("recovery retry delay must be shorter than the execution deadline")
        if self.max_rehearsal_age_seconds < self.rehearsal_interval_seconds:
            raise ValueError("recovery rehearsal freshness cannot be shorter than its interval")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"profile_generation"}))
        if self.profile_generation is not None and self.profile_generation != expected:
            raise ValueError("recovery production profile generation differs from content")
        object.__setattr__(self, "profile_generation", expected)
        return self

    def validate_trusted_runtime_root(self, runtime_root: Path) -> None:
        root = Path(runtime_root)
        expected_control = root / "control" / "recovery"
        expected_source = root.parent
        recovery_sandbox = Path("/var/lib/rquant/runtime-recovery")
        expected_publication = recovery_sandbox / "backups"
        expected_restore = recovery_sandbox / "restores"
        exact_paths = (
            (self.backup_source_root, expected_source),
            (self.backup_publication_root, expected_publication),
            (self.isolated_restore_root, expected_restore),
            (self.service_state_path, expected_control / "service.sqlite3"),
            (self.service_receipt_root, expected_control / "receipts"),
        )
        if any(observed != expected for observed, expected in exact_paths):
            raise ValueError("recovery systemd sandbox path differs from trusted runtime root")
        readonly_inputs = (self.backup_config_path, self.credential_file)
        if any(
            not path.is_relative_to(expected_source) or path.is_relative_to(expected_control)
            for path in readonly_inputs
        ):
            raise ValueError("recovery systemd sandbox read path is outside backup source root")

    def backup_environment(self) -> Mapping[str, str]:
        if self.profile_generation is None:  # pragma: no cover - model invariant
            raise ValueError("recovery production profile generation is missing")
        return MappingProxyType(
            {
                "RQUANT_RECOVERY_BACKUP_ENABLED": "true",
                "RQUANT_RECOVERY_BACKUP_CONFIG": str(self.backup_config_path),
                "RQUANT_RECOVERY_CREDENTIAL_FILE": str(self.credential_file),
                "RQUANT_RECOVERY_PROFILE_GENERATION": self.profile_generation,
                "RQUANT_RECOVERY_SIGNER_KEY_ID": self.signer_key_id,
            }
        )

    def recovery_service_arguments(self) -> Mapping[str, str]:
        """Return nonsecret CLI arguments bound by this immutable profile."""

        return MappingProxyType(
            {
                "publication_root": str(self.backup_publication_root),
                "state_path": str(self.service_state_path),
                "receipt_root": str(self.service_receipt_root),
                "restore_root": str(self.isolated_restore_root),
                "credential_file": str(self.credential_file),
                "lease_seconds": str(self.recovery_lease_seconds),
                "max_attempts": str(self.recovery_max_attempts),
                "retry_delay_seconds": str(self.recovery_retry_delay_seconds),
                "deadline_seconds": str(self.recovery_deadline_seconds),
                "rehearsal_interval_seconds": str(self.rehearsal_interval_seconds),
            }
        )


class RuntimeDeploymentProfile(RuntimeContractModel):
    profile_id: Sha256 | None = None
    producer_commit: CommitSha
    runtime_mode: RuntimeMode = "local-test"
    production_runtime_root: str | None = None
    manifests: tuple[RuntimeServiceManifest, ...]
    capability_environment: Mapping[str, tuple[str, ...]]
    schema_rollout_policies: tuple[RuntimeSchemaRolloutPolicy, ...] = ()
    schema_v1_migration_authority: RuntimeSchemaV1MigrationAuthorization | None = None
    recovery: RuntimeRecoveryProductionConfig | None = None
    page_control: PageControlRuntimeProfile | None = None
    shadow: ShadowRuntimeProfile | None = None
    lab_highwater: LabHighWaterRuntimeProfile | None = None

    @field_validator("production_runtime_root")
    @classmethod
    def require_canonical_production_root(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = Path(value)
        normalized = Path(os.path.abspath(path))
        if not path.is_absolute() or path != normalized or value != str(normalized):
            raise ValueError("production runtime root must be absolute and normalized")
        return value

    @field_validator("manifests", mode="before")
    @classmethod
    def thaw_prevalidated_manifests(
        cls,
        value: object,
    ) -> object:
        if not isinstance(value, (list, tuple)):
            return value
        return tuple(
            item.model_dump(mode="json") if isinstance(item, RuntimeServiceManifest) else item
            for item in value
        )

    @field_validator("manifests")
    @classmethod
    def order_manifests(
        cls,
        value: tuple[RuntimeServiceManifest, ...],
    ) -> tuple[RuntimeServiceManifest, ...]:
        return tuple(sorted(value, key=lambda manifest: manifest.service_id))

    @field_validator("capability_environment")
    @classmethod
    def freeze_capability_environment(
        cls,
        value: Mapping[str, tuple[str, ...]],
    ) -> Mapping[str, tuple[str, ...]]:
        normalized = {
            str(service_id): tuple(sorted(names)) for service_id, names in sorted(value.items())
        }
        return MappingProxyType(normalized)

    @field_validator("schema_rollout_policies")
    @classmethod
    def order_schema_rollout_policies(
        cls,
        value: tuple[RuntimeSchemaRolloutPolicy, ...],
    ) -> tuple[RuntimeSchemaRolloutPolicy, ...]:
        return tuple(sorted(value, key=lambda policy: policy.channel_id))

    @field_serializer("capability_environment")
    def serialize_capability_environment(
        self,
        value: Mapping[str, tuple[str, ...]],
    ) -> dict[str, list[str]]:
        return {service_id: list(names) for service_id, names in value.items()}

    @model_validator(mode="after")
    def validate_identity(self) -> RuntimeDeploymentProfile:
        if self.runtime_mode == "linux-production" and self.production_runtime_root != str(
            LINUX_PRODUCTION_RUNTIME_ROOT
        ):
            raise ValueError(
                f"Linux production runtime root must be exactly {LINUX_PRODUCTION_RUNTIME_ROOT}"
            )
        if self.runtime_mode == "linux-production" and (
            self.lab_highwater is None or not self.lab_highwater.production_mode
        ):
            raise ValueError(
                "Linux production profile requires production Lab high-water authority"
            )
        if not self.manifests:
            raise ValueError("runtime deployment profile requires manifests")
        service_ids = tuple(manifest.service_id for manifest in self.manifests)
        if len(service_ids) != len(set(service_ids)):
            raise ValueError("runtime deployment profile service ids must be unique")
        if set(service_ids) != set(self.capability_environment):
            raise ValueError("runtime deployment profile capability bindings must match manifests")
        if any(manifest.producer_commit != self.producer_commit for manifest in self.manifests):
            raise ValueError("runtime deployment profile manifest commit mismatch")
        if self.production_runtime_root is not None:
            production_root = Path(self.production_runtime_root)
            if (
                self.lab_highwater is not None
                and self.lab_highwater.production_mode
                and self.lab_highwater.trusted_keyring_path.is_relative_to(production_root)
            ):
                raise ValueError(
                    "production Lab high-water credential must be outside the runtime owner root"
                )
            if self.runtime_mode == "linux-production":
                if self.page_control is None:
                    raise ValueError("Linux production profile requires page control")
                if self.page_control.endpoint != "http://127.0.0.1:8767/v1/commands":
                    raise ValueError("production page control endpoint must be fixed")
                if self.page_control.outbox_path != (
                    production_root / "control" / "page-control.sqlite3"
                ):
                    raise ValueError("page control outbox differs from trusted runtime root")
                if self.page_control.data_dir != (production_root / "serving" / "page-control"):
                    raise ValueError(
                        "page control data directory differs from trusted runtime root"
                    )
                if self.page_control.log_dir != (production_root / "control" / "page-control-logs"):
                    raise ValueError("page control log directory differs from trusted runtime root")
                if self.page_control.page_projection_canvas_catalog_root != (
                    self.page_control.data_dir / "canvases"
                ):
                    raise ValueError(
                        "page projection Canvas catalog differs from trusted PageControl data root"
                    )
                canvas_authority = self.page_control.canvas_publication
                if canvas_authority is None:
                    raise ValueError(
                        "Linux production profile requires Canvas publication authority"
                    )
                if canvas_authority.signer_command != PRODUCTION_CANVAS_SIGNER_COMMAND:
                    raise ValueError(
                        "production Canvas signer must use the fixed protected capability"
                    )
                if (
                    canvas_authority.consumer_service_id != PRODUCTION_PAGE_CONTROL_SERVICE_ID
                    or canvas_authority.consumer_instance_id != PRODUCTION_PAGE_CONTROL_INSTANCE_ID
                ):
                    raise ValueError("production Canvas consumer identity must be fixed")
                if self.shadow is None:
                    raise ValueError("Linux production profile requires Shadow authority")
                if self.shadow.signer_command != PRODUCTION_SHADOW_SIGNER_COMMAND:
                    raise ValueError(
                        "production Shadow signer must use the fixed protected capability"
                    )
                if (
                    self.shadow.report_producer_service_id != PRODUCTION_SHADOW_SERVICE_ID
                    or self.shadow.report_producer_instance_id != PRODUCTION_SHADOW_INSTANCE_ID
                ):
                    raise ValueError("production Shadow report producer identity must be fixed")
                daily_orchestrators = tuple(
                    manifest
                    for manifest in self.manifests
                    if manifest.service_kind is RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR
                )
                if len(daily_orchestrators) != 1:
                    raise ValueError("Linux production profile requires one Daily orchestrator")
                daily_settings = daily_orchestrators[0].settings
                if "receipt_signer_command" in daily_settings:
                    raise ValueError("production Daily signer must use the fixed socket authority")
                if daily_settings.get("receipt_signer_socket_endpoint") != (
                    PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT
                ):
                    raise ValueError("production Daily signer must use the fixed socket authority")
                if daily_settings.get("receipt_trusted_keyring_path") != str(
                    PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH
                ):
                    raise ValueError(
                        "production Daily trusted keyring must use the fixed root keyring"
                    )
                active_key_id = daily_settings.get("receipt_active_key_id")
                active_public_key = daily_settings.get("receipt_active_public_key_pem")
                previous_keys = daily_settings.get("receipt_previous_public_key_pems")
                if (
                    not isinstance(active_key_id, str)
                    or not active_key_id
                    or not isinstance(active_public_key, str)
                    or not active_public_key
                    or not isinstance(previous_keys, Mapping)
                    or active_key_id in previous_keys
                ):
                    raise ValueError("production Daily receipt authority is incomplete")
                forbidden_daily_fields = (
                    "private",
                    "helper",
                    "argv",
                    "env",
                    "state",
                    "keys_file",
                    "key_path",
                )
                if any(
                    any(fragment in str(key).lower() for fragment in forbidden_daily_fields)
                    for key in daily_settings
                ):
                    raise ValueError("production Daily receipt authority must be public-only")
            if self.recovery is not None:
                self.recovery.validate_trusted_runtime_root(production_root)
            for manifest in self.manifests:
                if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE:
                    validate_strategy_live_completion_manifest(
                        manifest,
                        runtime_root=production_root,
                    )
        elif self.recovery is not None:
            raise ValueError("recovery production configuration requires a trusted runtime root")
        for manifest in self.manifests:
            names = self.capability_environment[manifest.service_id]
            if any(not name for name in names) or len(names) != len(set(names)):
                raise ValueError("runtime capability environment names must be unique and nonempty")
            unknown = set(names) - CAPABILITY_KEYS.get(manifest.service_kind, frozenset())
            if unknown:
                raise ValueError(
                    f"runtime capability environment is not allowed for {manifest.service_id}"
                )
            missing = _REQUIRED_CAPABILITIES.get(
                manifest.service_kind,
                frozenset(),
            ) - set(names)
            if missing:
                raise ValueError(
                    f"runtime profile {manifest.service_id} requires {', '.join(sorted(missing))}"
                )
            if (
                manifest.service_kind is RuntimeServiceKind.NOTIFIER
                and not _NOTIFIER_DELIVERY_CAPABILITIES.intersection(names)
            ):
                raise ValueError(
                    f"runtime profile {manifest.service_id} requires "
                    "PUSHDEER_KEYS or PUSHPLUS_TOKENS"
                )
        if self.schema_rollout_policies or self.schema_v1_migration_authority is not None:
            if self.production_runtime_root is None:
                raise ValueError("schema rollout policy requires a trusted production root")
            expected_state_path = Path(self.production_runtime_root) / "control" / "schema-rollouts"
            channel_ids = tuple(policy.channel_id for policy in self.schema_rollout_policies)
            if len(channel_ids) != len(set(channel_ids)):
                raise ValueError("schema rollout policy channels must be unique")
            bundle = build_runtime_schema_contract_bundle(
                self.manifests,
                producer_commit=self.producer_commit,
            )
            for policy in self.schema_rollout_policies:
                if policy.state_path != expected_state_path:
                    raise ValueError(
                        "schema rollout state path differs from trusted production root"
                    )
                try:
                    channel = bundle.channel(policy.channel_id)
                except KeyError as exc:
                    raise ValueError("schema rollout policy channel is unknown") from exc
                expected_consumers = tuple(
                    binding.requirement.consumer_id for binding in channel.consumers
                )
                if not channel.producers or policy.required_consumers != expected_consumers:
                    raise ValueError("schema rollout required consumers differ from trusted bundle")
            if self.schema_v1_migration_authority is not None:
                expected_lifecycles = {
                    (channel.channel_id, field.name): (
                        field.introduced_in,
                        field.deprecated_in,
                        field.removed_in,
                    )
                    for channel in bundle.channels
                    for field in channel.declaration.fields
                }
                reviewed_lifecycles = {
                    (review.channel_id, review.field_name): (
                        review.introduced_in,
                        review.deprecated_in,
                        review.removed_in,
                    )
                    for review in self.schema_v1_migration_authority.reviewed_lifecycles
                }
                if expected_lifecycles != reviewed_lifecycles:
                    raise ValueError(
                        "schema v1 migration authority does not review the trusted bundle"
                    )
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"profile_id"}))
        if self.profile_id is None:
            object.__setattr__(self, "profile_id", expected)
        elif self.profile_id != expected:
            raise ValueError("runtime deployment profile id does not match content")
        return self


class ResolvedRuntimeCapabilityEnvironment(Mapping[str, Mapping[str, str]]):
    __slots__ = ("_values",)

    def __init__(self, values: Mapping[str, Mapping[str, str]]) -> None:
        self._values = MappingProxyType(
            {
                service_id: LoadedRuntimeCapabilities(capabilities)
                for service_id, capabilities in sorted(values.items())
            }
        )

    def __getitem__(self, service_id: str) -> Mapping[str, str]:
        return self._values[service_id]

    def __iter__(self) -> Iterator[str]:
        return iter(self._values)

    def __len__(self) -> int:
        return len(self._values)

    def __repr__(self) -> str:
        bindings = {
            service_id: tuple(capabilities) for service_id, capabilities in self._values.items()
        }
        return f"ResolvedRuntimeCapabilityEnvironment(bindings={bindings}, values=<redacted>)"


class RuntimeDeploymentProfilePreview(RuntimeContractModel):
    profile_id: Sha256
    producer_commit: CommitSha
    runtime_root: Path
    service_ids: tuple[str, ...]
    capability_names: Mapping[str, tuple[str, ...]]
    strategy_completion_manifest_fingerprints: Mapping[str, Sha256] = Field(default_factory=dict)
    recovery_profile_generation: Sha256 | None = None
    recovery_backup_environment: Mapping[str, str] = Field(default_factory=dict)

    @field_validator("capability_names")
    @classmethod
    def freeze_capability_names(
        cls,
        value: Mapping[str, tuple[str, ...]],
    ) -> Mapping[str, tuple[str, ...]]:
        return MappingProxyType(
            {service_id: tuple(names) for service_id, names in sorted(value.items())}
        )

    @field_serializer("capability_names")
    def serialize_capability_names(
        self,
        value: Mapping[str, tuple[str, ...]],
    ) -> dict[str, list[str]]:
        return {service_id: list(names) for service_id, names in value.items()}

    @field_validator("strategy_completion_manifest_fingerprints")
    @classmethod
    def freeze_strategy_completion_fingerprints(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("strategy_completion_manifest_fingerprints")
    def serialize_strategy_completion_fingerprints(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)

    @field_validator("recovery_backup_environment")
    @classmethod
    def freeze_recovery_backup_environment(
        cls,
        value: Mapping[str, str],
    ) -> Mapping[str, str]:
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("recovery_backup_environment")
    def serialize_recovery_backup_environment(
        self,
        value: Mapping[str, str],
    ) -> dict[str, str]:
        return dict(value)


def resolve_profile_capabilities(
    profile: RuntimeDeploymentProfile,
    *,
    environ: Mapping[str, str],
) -> ResolvedRuntimeCapabilityEnvironment:
    if not isinstance(profile, RuntimeDeploymentProfile):
        raise TypeError("profile must be a RuntimeDeploymentProfile")
    if not isinstance(environ, Mapping):
        raise TypeError("environ must be a mapping")
    resolved: dict[str, dict[str, str]] = {}
    for service_id, names in profile.capability_environment.items():
        values: dict[str, str] = {}
        for name in names:
            value = environ.get(name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"runtime capability environment {name} is missing")
            if any(character in value for character in ("\x00", "\n", "\r")):
                raise ValueError(f"runtime capability environment {name} is unsafe")
            values[name] = value
        resolved[service_id] = values
    return ResolvedRuntimeCapabilityEnvironment(resolved)


def preview_runtime_deployment_profile(
    profile: RuntimeDeploymentProfile,
    *,
    runtime_root: Path,
    environ: Mapping[str, str],
    schema_bootstrap_reason: str | None = None,
) -> RuntimeDeploymentProfilePreview:
    root = Path(runtime_root)
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError("runtime root must be absolute and normalized")
    if (
        profile.production_runtime_root is not None
        and Path(profile.production_runtime_root) != root
    ):
        raise ValueError("profile production runtime root does not match trusted runtime root")
    resolved = resolve_profile_capabilities(profile, environ=environ)
    if profile.profile_id is None:  # pragma: no cover - guarded by the profile validator
        raise ValueError("runtime deployment profile id is missing")
    _validate_runtime_deployment_bundle(
        root,
        producer_commit=profile.producer_commit,
        manifests=profile.manifests,
        capability_env={
            service_id: dict(capabilities) for service_id, capabilities in resolved.items()
        },
        deployment_profile_id=profile.profile_id,
        deployment_profile_payload=canonical_model_json_bytes(profile),
        schema_bootstrap_reason=schema_bootstrap_reason,
        schema_v1_migration=profile.schema_v1_migration_authority,
    )
    return RuntimeDeploymentProfilePreview(
        profile_id=profile.profile_id,
        producer_commit=profile.producer_commit,
        runtime_root=root,
        service_ids=tuple(sorted(resolved)),
        capability_names={
            service_id: tuple(capabilities) for service_id, capabilities in resolved.items()
        },
        strategy_completion_manifest_fingerprints={
            manifest.service_id: manifest.manifest_fingerprint
            for manifest in profile.manifests
            if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE
        },
        recovery_profile_generation=(
            None if profile.recovery is None else profile.recovery.profile_generation
        ),
        recovery_backup_environment=(
            {} if profile.recovery is None else dict(profile.recovery.backup_environment())
        ),
    )


def load_runtime_deployment_profile(
    path: Path,
    *,
    expected_commit: str,
) -> RuntimeDeploymentProfile:
    if re.fullmatch(r"[0-9a-f]{40}", expected_commit) is None:
        raise ValueError("expected runtime profile commit is invalid")
    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError("runtime deployment profile path must be absolute and normalized")
    current = Path(candidate.anchor)
    for component in candidate.parts[1:-1]:
        current /= component
        try:
            observed_parent = current.lstat()
        except FileNotFoundError as exc:
            raise ValueError("runtime deployment profile parent is unavailable") from exc
        if stat.S_ISLNK(observed_parent.st_mode):
            raise ValueError("runtime deployment profile parent contains a symlink")
    descriptor = -1
    try:
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid():
            raise ValueError("runtime deployment profile must be an owned regular file")
        if before.st_nlink != 1 or before.st_mode & 0o022:
            raise ValueError("runtime deployment profile permissions are unsafe")
        if before.st_size <= 0 or before.st_size > _MAX_PROFILE_BYTES:
            raise ValueError("runtime deployment profile size is unsafe")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(remaining, 1024 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        identity_before = (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        identity_after = (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        payload = b"".join(chunks)
        if remaining or len(payload) != before.st_size or identity_after != identity_before:
            raise ValueError("runtime deployment profile changed while reading")
    except OSError as exc:
        raise ValueError("runtime deployment profile is unavailable or unsafe") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    profile = strict_model_validate_canonical_json(RuntimeDeploymentProfile, payload)
    if profile.producer_commit != expected_commit:
        raise ValueError("runtime deployment profile commit mismatch")
    return profile


def load_runtime_schema_v1_migration_authorization(
    path: Path,
) -> RuntimeSchemaV1MigrationAuthorization:
    """Load one explicit operator authorization from an owned canonical private file."""

    candidate = Path(path)
    if not candidate.is_absolute() or candidate != Path(os.path.abspath(candidate)):
        raise ValueError("schema v1 migration authority path must be absolute and normalized")
    descriptor = -1
    try:
        descriptor = os.open(candidate, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        before = os.fstat(descriptor)
        named = candidate.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size <= 0
            or before.st_size > _MAX_PROFILE_BYTES
            or (before.st_dev, before.st_ino) != (named.st_dev, named.st_ino)
        ):
            raise ValueError("schema v1 migration authority is unsafe")
        payload = b""
        while chunk := os.read(descriptor, 1024 * 1024):
            payload += chunk
            if len(payload) > _MAX_PROFILE_BYTES:
                raise ValueError("schema v1 migration authority is oversized")
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ):
            raise ValueError("schema v1 migration authority changed while reading")
    except OSError as exc:
        raise ValueError("schema v1 migration authority is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return strict_model_validate_canonical_json(
        RuntimeSchemaV1MigrationAuthorization,
        payload,
    )


def load_current_runtime_deployment_profile(
    runtime_root: Path,
) -> RuntimeDeploymentProfile:
    """Load the profile cryptographically named by the current runtime receipt."""

    root = Path(runtime_root)
    receipt = load_current_runtime_deployment_receipt_unbound(root)
    if receipt.deployment_profile_id is None:  # pragma: no cover - receipt invariant
        raise ValueError("current runtime generation lacks a deployment profile identity")
    path = root / "generations" / receipt.generation_hash / "deployment-profile.json"
    profile = load_runtime_deployment_profile(
        path,
        expected_commit=receipt.producer_commit,
    )
    if profile.profile_id != receipt.deployment_profile_id:
        raise ValueError("current runtime deployment profile identity differs")
    if (
        profile.production_runtime_root is not None
        and Path(profile.production_runtime_root) != root
    ):
        raise ValueError("current runtime deployment profile root differs")
    return profile


def validate_runtime_recovery_backup_config(
    profile: RuntimeDeploymentProfile,
    backup_config: object,
) -> None:
    """Bind one dynamic daily backup plan to the immutable production policy."""

    from rquant.runtime_recovery_backup import RecoveryBackupConfig

    if profile.recovery is None:
        raise ValueError("runtime profile has no recovery production configuration")
    config = RecoveryBackupConfig.model_validate(backup_config)
    recovery = profile.recovery
    if recovery.profile_generation is None:  # pragma: no cover - model invariant
        raise ValueError("recovery production profile generation is missing")
    if (
        Path(config.source_root) != recovery.backup_source_root
        or Path(config.publication_root) != recovery.backup_publication_root
        or config.target_commit != profile.producer_commit
        or config.target_profile_generation != recovery.profile_generation
        or config.signer_key_id != recovery.signer_key_id
        or config.production_artifact_role != recovery.production_artifact_role
        or config.paper_ledger_artifact_role != recovery.paper_ledger_artifact_role
        or config.deadline_seconds != recovery.recovery_deadline_seconds
    ):
        raise ValueError("recovery backup config differs from the production profile")
    expected_roles = {
        (
            role.logical_role,
            role.kind,
            role.source_path,
            role.restore_path,
            role.schema_version,
            role.relations,
            tuple(role.references.items()),
        )
        for role in recovery.artifact_roles
    }
    observed_roles = {
        (
            role.logical_role,
            role.kind,
            role.source_path,
            role.restore_path,
            role.schema_version,
            role.relations,
            tuple(role.references.items()),
        )
        for role in config.artifacts
    }
    if observed_roles != expected_roles:
        raise ValueError("recovery backup artifact role mapping differs from production")


def build_runtime_recovery_preflight_config(
    profile: RuntimeDeploymentProfile,
    *,
    expected_manifest_id: str | None = None,
) -> RuntimeRecoveryPreflightConfig:
    from rquant.preflight import RuntimeRecoveryPreflightConfig

    recovery = profile.recovery
    if recovery is None or recovery.profile_generation is None:
        raise ValueError("runtime profile has no recovery production configuration")
    return RuntimeRecoveryPreflightConfig(
        publication_root=recovery.backup_publication_root,
        service_state_path=recovery.service_state_path,
        service_receipt_root=recovery.service_receipt_root,
        restore_root=recovery.isolated_restore_root,
        expected_profile_generation=recovery.profile_generation,
        expected_manifest_id=expected_manifest_id,
        max_rpo=timedelta(seconds=recovery.max_rpo_seconds),
        max_rehearsal_age=timedelta(seconds=recovery.max_rehearsal_age_seconds),
        max_rto=timedelta(seconds=recovery.max_rto_seconds),
    )


def install_runtime_deployment_profile(
    profile: RuntimeDeploymentProfile,
    *,
    runtime_root: Path,
    environ: Mapping[str, str],
    schema_bootstrap_reason: str | None = None,
    schema_rollout_started_at: datetime | None = None,
) -> RuntimeDeploymentReceipt:
    root = Path(runtime_root)
    if not root.is_absolute() or root != Path(os.path.abspath(root)):
        raise ValueError("runtime root must be absolute and normalized")
    if (
        profile.production_runtime_root is not None
        and Path(profile.production_runtime_root) != root
    ):
        raise ValueError("profile production runtime root does not match trusted runtime root")
    resolved = resolve_profile_capabilities(profile, environ=environ)
    capability_env = {
        service_id: dict(capabilities) for service_id, capabilities in resolved.items()
    }
    receipt = _install_runtime_deployment_bundle(
        root,
        producer_commit=profile.producer_commit,
        manifests=profile.manifests,
        capability_env=capability_env,
        deployment_profile_id=profile.profile_id,
        deployment_profile_payload=canonical_model_json_bytes(profile),
        schema_bootstrap_reason=schema_bootstrap_reason,
        schema_v1_migration=profile.schema_v1_migration_authority,
    )
    if not isinstance(receipt, RuntimeDeploymentReceipt):
        return receipt
    existing_plan_ids = runtime_schema_rollout_plan_ids_for_generation(
        root,
        generation_id=receipt.generation_hash,
    )
    active_plan_ids = tuple(
        plan_id
        for plan_id in existing_plan_ids
        if load_runtime_schema_rollout(root, plan_id=plan_id)[1].get_state(plan_id).phase
        not in {RolloutPhase.RETIRE, RolloutPhase.ROLLBACK}
    )
    if receipt.previous_generation_hash is None or not profile.schema_rollout_policies:
        return receipt.model_copy(update={"schema_rollout_plan_ids": active_plan_ids})
    if profile.schema_v1_migration_authority is not None:
        return receipt.model_copy(update={"schema_rollout_plan_ids": active_plan_ids})
    started_at = schema_rollout_started_at or datetime.now(UTC)
    changed_channels = set(
        changed_runtime_schema_channels(
            root,
            previous_generation_id=receipt.previous_generation_hash,
            target_generation_id=receipt.generation_hash,
        )
    )
    prepared_plan_ids = list(active_plan_ids)
    try:
        for policy in profile.schema_rollout_policies:
            if policy.channel_id not in changed_channels:
                continue
            authority = prepare_runtime_schema_rollout(
                root,
                previous_generation_id=receipt.previous_generation_hash,
                target_generation_id=receipt.generation_hash,
                channel_id=policy.channel_id,
                started_at=started_at,
                deadline=started_at + timedelta(seconds=policy.stage_timeout_seconds),
                consumer_ack_max_age_seconds=(policy.consumer_ack_max_age_seconds),
                retire_observation_seconds=policy.retire_observation_seconds,
            )
            observed_consumers = tuple(
                consumer.consumer_id for consumer in authority.registry.consumers
            )
            if observed_consumers != policy.required_consumers:
                raise ValueError("schema rollout authority consumers differ from production policy")
            prepared_plan_ids.append(authority.plan_id)
    except BaseException:
        rollback_error: BaseException | None = None
        for plan_id in reversed(tuple(dict.fromkeys(prepared_plan_ids))):
            try:
                _authority, store = load_runtime_schema_rollout(root, plan_id=plan_id)
                state = store.get_state(plan_id)
                rollback_runtime_schema_rollout(
                    root,
                    plan_id=plan_id,
                    expected_revision=state.revision,
                    reason="deployment profile schema preparation failed",
                    now=started_at,
                    operation_id=f"profile-prepare-rollback:{receipt.generation_hash}:{plan_id}",
                )
            except BaseException as exc:
                rollback_error = rollback_error or exc
        if rollback_error is not None or not prepared_plan_ids:
            try:
                previous = load_runtime_deployment_generation_receipt(
                    root,
                    generation_hash=receipt.previous_generation_hash,
                )
                if previous.deployment_profile_id is None:
                    raise ValueError("previous deployment profile identity is missing")
                activate_runtime_deployment_generation(
                    root,
                    generation_hash=previous.generation_hash,
                    expected_commit=previous.producer_commit,
                    expected_profile_id=previous.deployment_profile_id,
                )
            except BaseException as exc:
                rollback_error = rollback_error or exc
        if rollback_error is not None:
            raise RuntimeError(
                "schema rollout preparation failed and previous generation restore failed"
            ) from rollback_error
        raise
    return receipt.model_copy(
        update={"schema_rollout_plan_ids": tuple(sorted(set(prepared_plan_ids)))}
    )


__all__ = [
    "CanvasPublicationRuntimeProfile",
    "REQUIRED_RECOVERY_ARTIFACT_KINDS",
    "LabHighWaterRuntimeProfile",
    "LINUX_PRODUCTION_RUNTIME_ROOT",
    "PageControlRuntimeProfile",
    "PRODUCTION_CANVAS_SIGNER_COMMAND",
    "PRODUCTION_DAILY_SIGNER_SOCKET_ENDPOINT",
    "PRODUCTION_DAILY_RECEIPT_TRUSTED_KEYRING_PATH",
    "PRODUCTION_PAGE_CONTROL_INSTANCE_ID",
    "PRODUCTION_PAGE_CONTROL_SERVICE_ID",
    "PRODUCTION_SHADOW_INSTANCE_ID",
    "PRODUCTION_SHADOW_SERVICE_ID",
    "PRODUCTION_SHADOW_SIGNER_COMMAND",
    "ResolvedRuntimeCapabilityEnvironment",
    "RuntimeDeploymentProfile",
    "RuntimeDeploymentProfilePreview",
    "RuntimeRecoveryArtifactRoleBinding",
    "RuntimeRecoveryProductionConfig",
    "RuntimeSchemaRolloutPolicy",
    "ShadowRuntimeProfile",
    "build_runtime_recovery_preflight_config",
    "install_runtime_deployment_profile",
    "load_current_runtime_deployment_profile",
    "load_runtime_deployment_profile",
    "load_runtime_schema_v1_migration_authorization",
    "preview_runtime_deployment_profile",
    "resolve_profile_capabilities",
    "validate_runtime_recovery_backup_config",
]
