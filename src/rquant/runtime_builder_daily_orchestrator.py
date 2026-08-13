"""Profile-bound Shadow daily-DAG coordinator.

The runtime service executes only the isolated Shadow DAG declared by the
immutable deployment profile.  Legacy ``rquant-daily`` remains authoritative;
this builder never opens its DuckDB, outbox, service, or timer state.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, Protocol, Self

from pydantic import Field, field_validator, model_validator

from rquant.daily_pipeline_ledger import (
    DailyPipelineLedger,
    DailyPipelineMode,
    DailyPipelineStorageProfile,
    DailyRunState,
    DailyStageState,
    StageResult,
)
from rquant.daily_receipt_socket_authority import (
    DAILY_RECEIPT_SOCKET_ENDPOINT,
    DAILY_RECEIPT_TRUSTED_KEYRING_PATH,
    DailyReceiptSocketSigningClient,
    load_daily_receipt_trusted_keyring,
)
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeStepResult
from rquant.runtime_service_entrypoint import (
    RuntimeServiceBuilder,
    RuntimeServiceKind,
    RuntimeServiceManifest,
    RuntimeServiceStep,
)
from rquant.runtime_shadow_validation import (
    Ed25519CompletionAttestationKeyring,
    _ed25519_signing_payload,
    _valid_ed25519_signature,
    _verify_ed25519_signature,
)
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_json_loads

_FAN_IN_STAGES = (
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
_MAX_STAGE_COMPLETION_BYTES = 64 * 1024
_SHADOW_STAGE_ADAPTER_FLAG = "--run-shadow-adapter"
_ALLOWED_STAGE_ENVIRONMENT_KEYS = frozenset({"PYTHONPATH"})
_SHADOW_STAGE_ACTIONS = {
    "raw_capture": "shadow_raw_capture",
    "validate_candidate": "shadow_validate_candidate",
    "canonical_publish": "shadow_canonical_publish",
    "screen": "shadow_screen",
    "pool": "shadow_pool",
    "summary": "shadow_summary",
    "serving_refresh": "shadow_serving_refresh",
    "replica_sync": "shadow_replica_sync",
    "research_ingest": "shadow_research_ingest",
    "backup": "shadow_backup",
}
_PHYSICAL_STAGE_ROOTS = {
    "serving_refresh": "serving-refresh",
    "replica_sync": "replica-sync",
    "research_ingest": "research-ingest",
    "backup": "backup",
}
_DAILY_STAGE_RECEIPT_NAMESPACE = "rquant-daily-shadow-stage-completion-receipt"
_DAILY_RUN_RECEIPT_NAMESPACE = "rquant-daily-shadow-run-completion-receipt"
_MAX_SIGNING_PAYLOAD_BYTES = 1024 * 1024
_FORBIDDEN_SIGNER_ARG_NAMES = ("private", "secret")
_PRODUCTION_DAILY_SIGNER_HELPER_NAME = "rquant-daily-receipt-signer"
_PRIVILEGE_ESCALATING_EXECUTABLES = frozenset({"sudo", "doas", "su", "pkexec", "run0"})


def _shadow_stage_command_identity(stage_id: str) -> str:
    return f"daily-shadow-stage/{stage_id}/v1"


def _shadow_stage_order(stage_id: str) -> int:
    try:
        return _FAN_IN_STAGES.index(stage_id)
    except ValueError as exc:
        raise ValueError("daily shadow stage is not part of the fixed DAG") from exc


def _shadow_stage_output_identity(
    *,
    mode: DailyPipelineMode,
    run_id: str,
    stage_id: str,
    action: str,
    effect_id: str,
    idempotency_key: str,
    input_identity: str,
    source_generation_id: str,
    source_content_hash: str,
    command_manifest_hash: str,
    profile_hash: str,
    command_identity: str,
    stage_order: int,
    exit_status: int,
    dependency_receipt_ids: tuple[str, ...],
) -> str:
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "contract": "daily-shadow-stage-output-identity/v1",
                "mode": mode,
                "run_id": run_id,
                "stage_id": stage_id,
                "action": action,
                "effect_id": effect_id,
                "idempotency_key": idempotency_key,
                "input_identity": input_identity,
                "source_generation_id": source_generation_id,
                "source_content_hash": source_content_hash,
                "command_manifest_hash": command_manifest_hash,
                "profile_hash": profile_hash,
                "command_identity": command_identity,
                "stage_order": stage_order,
                "exit_status": exit_status,
                "dependency_receipt_ids": dependency_receipt_ids,
            }
        )
    ).hexdigest()


def _shadow_stage_receipt_identity_payload(
    *,
    mode: DailyPipelineMode,
    run_id: str,
    stage_id: str,
    effect_id: str,
    idempotency_key: str,
    input_identity: str,
    source_generation_id: str,
    source_content_hash: str,
    command_manifest_hash: str,
    profile_hash: str,
    command_identity: str,
    stage_order: int,
    exit_status: int,
    output_identity: str,
    dependency_receipt_ids: tuple[str, ...],
    previous_receipt_hash: str,
    key_id: str,
    signature_algorithm: Literal["ed25519"],
    result: StageResult,
    issued_at: datetime,
) -> dict[str, object]:
    issued_at_utc = issued_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return {
        "contract": "daily-shadow-stage-completion-receipt/v2",
        "mode": mode,
        "run_id": run_id,
        "stage_id": stage_id,
        "effect_id": effect_id,
        "idempotency_key": idempotency_key,
        "input_identity": input_identity,
        "source_generation_id": source_generation_id,
        "source_content_hash": source_content_hash,
        "command_manifest_hash": command_manifest_hash,
        "profile_hash": profile_hash,
        "command_identity": command_identity,
        "stage_order": stage_order,
        "exit_status": exit_status,
        "output_identity": output_identity,
        "dependency_receipt_ids": dependency_receipt_ids,
        "previous_receipt_hash": previous_receipt_hash,
        "key_id": key_id,
        "signature_algorithm": signature_algorithm,
        "result": result.model_dump(mode="json"),
        "issued_at": issued_at_utc,
    }


def _shadow_stage_receipt_signing_payload(
    *,
    receipt_id: str,
    identity_payload: Mapping[str, object],
) -> bytes:
    return canonical_json_bytes(
        {
            "contract": "daily-shadow-stage-completion-signing-payload/v1",
            "receipt_id": receipt_id,
            **dict(identity_payload),
        }
    )


def _shadow_run_receipt_identity_payload(
    *,
    mode: DailyPipelineMode,
    run_id: str,
    trade_date: date,
    input_identity: str,
    source_generation_id: str,
    source_content_hash: str,
    command_manifest_hash: str,
    profile_hash: str,
    code_commit: str,
    stage_order: tuple[str, ...],
    stage_receipt_ids: tuple[str, ...],
    previous_receipt_hash: str,
    final_backup_stage_id: Literal["backup"],
    final_backup_receipt_id: str,
    final_backup_output_identity: str,
    final_backup_result: StageResult,
    exit_status: int,
    key_id: str,
    signature_algorithm: Literal["ed25519"],
    issued_at: datetime,
) -> dict[str, object]:
    issued_at_utc = issued_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return {
        "contract": "daily-shadow-run-completion-receipt/v1",
        "mode": mode,
        "run_id": run_id,
        "trade_date": trade_date.isoformat(),
        "input_identity": input_identity,
        "source_generation_id": source_generation_id,
        "source_content_hash": source_content_hash,
        "command_manifest_hash": command_manifest_hash,
        "profile_hash": profile_hash,
        "code_commit": code_commit,
        "stage_order": stage_order,
        "stage_receipt_ids": stage_receipt_ids,
        "previous_receipt_hash": previous_receipt_hash,
        "final_backup_stage_id": final_backup_stage_id,
        "final_backup_receipt_id": final_backup_receipt_id,
        "final_backup_output_identity": final_backup_output_identity,
        "final_backup_result": final_backup_result.model_dump(mode="json"),
        "exit_status": exit_status,
        "key_id": key_id,
        "signature_algorithm": signature_algorithm,
        "issued_at": issued_at_utc,
    }


def _shadow_run_receipt_signing_payload(
    *,
    receipt_id: str,
    identity_payload: Mapping[str, object],
) -> bytes:
    return canonical_json_bytes(
        {
            "contract": "daily-shadow-run-completion-signing-payload/v1",
            "receipt_id": receipt_id,
            **dict(identity_payload),
        }
    )


class DailyShadowStageReceiptSigningClient(Protocol):
    def sign(self, *, namespace: str, payload: bytes) -> str: ...


class DailyShadowStageReceiptSigningRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
    operation: Literal["sign"] = "sign"
    request_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=128)
    namespace: str = Field(min_length=1, max_length=256)
    payload_base64: str = Field(min_length=1, max_length=2 * 1024 * 1024)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class DailyShadowStageReceiptSigningResponse(RuntimeContractModel):
    schema_version: Literal[1] = 1
    operation: Literal["sign"] = "sign"
    request_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(min_length=1, max_length=128)
    namespace: str = Field(min_length=1, max_length=256)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    signature: str = Field(min_length=1, max_length=16_384)


class SecureDailyShadowStageReceiptSigningClient:
    """Invoke one protected Daily receipt signer without exposing private key material."""

    def __init__(
        self,
        *,
        command: tuple[str, ...],
        key_id: str,
        timeout_seconds: float,
    ) -> None:
        if not command or any(not item for item in command):
            raise ValueError("Daily stage receipt signer command is invalid")
        _reject_private_key_material_args(command)
        _reject_privileged_signer_command(command)
        if _daily_production_signer_host():
            raise ValueError(
                "daily offline test receipt signer is not available on a production host"
            )
        executable = Path(command[0])
        if not executable.is_absolute() or executable != Path(os.path.abspath(executable)):
            raise ValueError("Daily stage receipt signer command must be absolute")
        if not key_id.strip() or any(character.isspace() for character in key_id):
            raise ValueError("Daily stage receipt signer key id is invalid")
        self._command = tuple(command)
        self.key_id = key_id
        self.timeout_seconds = timeout_seconds

    def sign(self, *, namespace: str, payload: bytes) -> str:
        if namespace not in {
            _DAILY_STAGE_RECEIPT_NAMESPACE,
            _DAILY_RUN_RECEIPT_NAMESPACE,
        }:
            raise ValueError("Daily stage receipt signer namespace is not allowed")
        if not payload or len(payload) > _MAX_SIGNING_PAYLOAD_BYTES:
            raise ValueError("Daily stage receipt signer payload is invalid")
        payload_sha256 = hashlib.sha256(payload).hexdigest()
        request_id = canonical_sha256(
            {
                "contract": "daily-shadow-stage-receipt-signing-request/v1",
                "key_id": self.key_id,
                "namespace": namespace,
                "payload_sha256": payload_sha256,
            }
        )
        request = DailyShadowStageReceiptSigningRequest(
            request_id=request_id,
            key_id=self.key_id,
            namespace=namespace,
            payload_base64=base64.b64encode(payload).decode("ascii"),
            payload_sha256=payload_sha256,
        )
        try:
            completed = subprocess.run(
                self._command,
                input=canonical_json_bytes(request.model_dump(mode="json")),
                check=False,
                capture_output=True,
                timeout=self.timeout_seconds,
                env={"LANG": "C", "PATH": "/usr/bin:/bin"},
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise RuntimeError("Daily stage receipt signer capability is unavailable") from exc
        if completed.returncode != 0 or not completed.stdout:
            raise RuntimeError("Daily stage receipt signer capability failed")
        try:
            response = DailyShadowStageReceiptSigningResponse.model_validate(
                strict_json_loads(completed.stdout)
            )
        except (StrictJsonError, UnicodeDecodeError, ValueError) as exc:
            raise RuntimeError(
                "Daily stage receipt signer capability returned an invalid response"
            ) from exc
        if (
            response.request_id != request.request_id
            or response.key_id != request.key_id
            or response.namespace != request.namespace
            or response.payload_sha256 != request.payload_sha256
            or not _valid_ed25519_signature(response.signature)
        ):
            raise RuntimeError("Daily stage receipt signer capability response does not match")
        return response.signature


class Ed25519DailyShadowStageReceiptSigner:
    """Opaque Ed25519 signer adapter for Daily stage completion receipts."""

    def __init__(self, *, key_id: str, client: DailyShadowStageReceiptSigningClient) -> None:
        if not key_id.strip() or any(character.isspace() for character in key_id):
            raise ValueError("Daily stage receipt signing key id is invalid")
        self.key_id = key_id
        self._client = client

    def sign_receipt(
        self,
        *,
        receipt_id: str,
        identity_payload: Mapping[str, object],
    ) -> str:
        signature = self._client.sign(
            namespace=_DAILY_STAGE_RECEIPT_NAMESPACE,
            payload=_ed25519_signing_payload(
                namespace=_DAILY_STAGE_RECEIPT_NAMESPACE,
                payload=_shadow_stage_receipt_signing_payload(
                    receipt_id=receipt_id,
                    identity_payload=identity_payload,
                ),
            ),
        )
        if not _valid_ed25519_signature(signature):
            raise ValueError("daily shadow stage receipt signer returned an invalid signature")
        return signature

    def sign_run_completion(
        self,
        *,
        receipt_id: str,
        identity_payload: Mapping[str, object],
    ) -> str:
        signature = self._client.sign(
            namespace=_DAILY_RUN_RECEIPT_NAMESPACE,
            payload=_ed25519_signing_payload(
                namespace=_DAILY_RUN_RECEIPT_NAMESPACE,
                payload=_shadow_run_receipt_signing_payload(
                    receipt_id=receipt_id,
                    identity_payload=identity_payload,
                ),
            ),
        )
        if not _valid_ed25519_signature(signature):
            raise ValueError("daily run completion signer returned an invalid signature")
        return signature


class Ed25519DailyShadowStageReceiptKeyring(Ed25519CompletionAttestationKeyring):
    """Public-only active plus previous verifier for Daily stage receipts."""

    def verify_stage_receipt(
        self,
        receipt: DailyShadowStageCompletionReceipt,
        *,
        require_active: bool = False,
    ) -> bool:
        try:
            verified = DailyShadowStageCompletionReceipt.model_validate(receipt)
        except (TypeError, ValueError):
            return False
        if verified.signature_algorithm != "ed25519":
            return False
        if require_active and verified.key_id != self.active_key_id:
            return False
        public_key = self._keys.get(verified.key_id)
        if public_key is None:
            return False
        return _verify_ed25519_signature(
            public_key=public_key,
            payload=_ed25519_signing_payload(
                namespace=_DAILY_STAGE_RECEIPT_NAMESPACE,
                payload=_shadow_stage_receipt_signing_payload(
                    receipt_id=str(verified.receipt_id),
                    identity_payload=_shadow_stage_receipt_identity_payload(
                        mode=verified.mode,
                        run_id=verified.run_id,
                        stage_id=verified.stage_id,
                        effect_id=verified.effect_id,
                        idempotency_key=verified.idempotency_key,
                        input_identity=verified.input_identity,
                        source_generation_id=verified.source_generation_id,
                        source_content_hash=verified.source_content_hash,
                        command_manifest_hash=verified.command_manifest_hash,
                        profile_hash=verified.profile_hash,
                        command_identity=verified.command_identity,
                        stage_order=verified.stage_order,
                        exit_status=verified.exit_status,
                        output_identity=verified.output_identity,
                        dependency_receipt_ids=verified.dependency_receipt_ids,
                        previous_receipt_hash=verified.previous_receipt_hash,
                        key_id=verified.key_id,
                        signature_algorithm=verified.signature_algorithm,
                        result=verified.result,
                        issued_at=verified.issued_at,
                    ),
                ),
            ),
            signature=verified.signature,
        )

    def verify_run_completion_receipt(
        self,
        receipt: DailyShadowRunCompletionReceipt,
        *,
        require_active: bool = False,
    ) -> bool:
        try:
            verified = DailyShadowRunCompletionReceipt.model_validate(receipt)
        except (TypeError, ValueError):
            return False
        if verified.signature_algorithm != "ed25519":
            return False
        if require_active and verified.key_id != self.active_key_id:
            return False
        public_key = self._keys.get(verified.key_id)
        if public_key is None:
            return False
        return _verify_ed25519_signature(
            public_key=public_key,
            payload=_ed25519_signing_payload(
                namespace=_DAILY_RUN_RECEIPT_NAMESPACE,
                payload=_shadow_run_receipt_signing_payload(
                    receipt_id=str(verified.receipt_id),
                    identity_payload=_shadow_run_receipt_identity_payload(
                        mode=verified.mode,
                        run_id=verified.run_id,
                        trade_date=verified.trade_date,
                        input_identity=verified.input_identity,
                        source_generation_id=verified.source_generation_id,
                        source_content_hash=verified.source_content_hash,
                        command_manifest_hash=verified.command_manifest_hash,
                        profile_hash=verified.profile_hash,
                        code_commit=verified.code_commit,
                        stage_order=verified.stage_order,
                        stage_receipt_ids=verified.stage_receipt_ids,
                        previous_receipt_hash=verified.previous_receipt_hash,
                        final_backup_stage_id=verified.final_backup_stage_id,
                        final_backup_receipt_id=verified.final_backup_receipt_id,
                        final_backup_output_identity=verified.final_backup_output_identity,
                        final_backup_result=verified.final_backup_result,
                        exit_status=verified.exit_status,
                        key_id=verified.key_id,
                        signature_algorithm=verified.signature_algorithm,
                        issued_at=verified.issued_at,
                    ),
                ),
            ),
            signature=verified.signature,
        )


class DailyShadowStageCommandSettings(RuntimeContractModel):
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    adapter_identity: str = Field(min_length=1, max_length=256)
    argv: tuple[str, ...] = Field(min_length=1, max_length=128)
    environment: dict[str, str] = Field(default_factory=dict, max_length=64)
    working_directory: str | None = Field(default=None, min_length=1, max_length=4_096)
    estimated_memory_mb: int = Field(default=64, ge=1, le=1_048_576)
    estimated_io_bytes: int = Field(default=0, ge=0, le=1 << 50)

    @field_validator("argv")
    @classmethod
    def require_controlled_argv(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not item for item in value):
            raise ValueError("daily shadow stage argv cannot contain empty values")
        executable = Path(value[0])
        if not executable.is_absolute() or executable != Path(os.path.abspath(executable)):
            raise ValueError("daily shadow stage executable must be absolute and normalized")
        return value

    @field_validator("environment")
    @classmethod
    def reject_reserved_environment(cls, value: dict[str, str]) -> dict[str, str]:
        reserved_prefixes = ("RQUANT_DAILY_",)
        normalized = dict(sorted(value.items()))
        for key, item in normalized.items():
            if key not in _ALLOWED_STAGE_ENVIRONMENT_KEYS:
                raise ValueError("daily shadow stage command environment is not allowlisted")
            if not key or any(key.startswith(prefix) for prefix in reserved_prefixes):
                raise ValueError("daily shadow stage environment overrides reserved state")
            if "\x00" in key or "\x00" in item:
                raise ValueError("daily shadow stage environment contains NUL bytes")
        return normalized

    @field_validator("working_directory")
    @classmethod
    def require_absolute_working_directory(cls, value: str | None) -> str | None:
        if value is None:
            return None
        path = Path(value)
        if not path.is_absolute() or path != Path(os.path.abspath(path)):
            raise ValueError("daily shadow stage working directory must be absolute")
        return value

    @model_validator(mode="after")
    def require_allowlisted_identity_command(self) -> Self:
        expected_identity = _shadow_stage_command_identity(self.stage_id)
        expected_tail = (
            "-m",
            "rquant.runtime_builder_daily_orchestrator",
            _SHADOW_STAGE_ADAPTER_FLAG,
            self.stage_id,
        )
        if self.adapter_identity != expected_identity or self.argv[1:] != expected_tail:
            raise ValueError("daily shadow stage command is not allowlisted")
        return self


class DailyPipelineOrchestratorSettings(RuntimeContractModel):
    storage_root: Path
    source_spool_root: Path
    deployment_profile_path: Path
    mode: Literal["shadow"] = "shadow"
    service_owner: str = Field(min_length=1)
    stages: tuple[str, ...] = _FAN_IN_STAGES
    stage_commands: tuple[DailyShadowStageCommandSettings, ...]
    receipt_active_key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    receipt_active_public_key_pem: str = Field(min_length=1, max_length=16_384)
    receipt_previous_public_key_pems: Mapping[str, str] = Field(default_factory=dict)
    receipt_signer_command: tuple[str, ...] | None = None
    offline_test_receipt_signer_command: tuple[str, ...] | None = None
    receipt_signer_socket_endpoint: Path | None = None
    receipt_trusted_keyring_path: Path | None = None
    receipt_signer_timeout_seconds: float = Field(default=5.0, ge=0.1, le=30.0)
    receipt_signer_test_mode: bool = False

    @field_validator(
        "storage_root",
        "source_spool_root",
        "deployment_profile_path",
        "receipt_signer_socket_endpoint",
        "receipt_trusted_keyring_path",
    )
    @classmethod
    def require_normalized_absolute_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("daily orchestrator paths must be absolute and normalized")
        return value

    @field_validator("stages")
    @classmethod
    def require_exact_fan_in_dag(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != _FAN_IN_STAGES:
            raise ValueError("daily orchestrator must declare the fixed fan-in DAG")
        return value

    @field_validator("stage_commands")
    @classmethod
    def order_stage_commands(
        cls,
        value: tuple[DailyShadowStageCommandSettings, ...],
    ) -> tuple[DailyShadowStageCommandSettings, ...]:
        stage_ids = tuple(command.stage_id for command in value)
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("daily orchestrator stage_commands must be unique")
        if set(stage_ids) != set(_FAN_IN_STAGES):
            raise ValueError("daily orchestrator stage_commands must cover the fixed DAG")
        commands = {command.stage_id: command for command in value}
        return tuple(commands[stage_id] for stage_id in _FAN_IN_STAGES)

    @field_validator("receipt_previous_public_key_pems")
    @classmethod
    def validate_previous_receipt_keys(cls, value: Mapping[str, str]) -> dict[str, str]:
        previous = dict(sorted(value.items()))
        if any(
            not public_key or not key_id or any(character.isspace() for character in key_id)
            for key_id, public_key in previous.items()
        ):
            raise ValueError("daily receipt previous public keyring is invalid")
        return previous

    @field_validator("receipt_signer_command")
    @classmethod
    def reject_production_signer_command(
        cls,
        value: tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        if value is None:
            return None
        raise ValueError("daily receipt signer command fallback is not allowed; use socket")

    @field_validator("offline_test_receipt_signer_command")
    @classmethod
    def require_offline_test_signer_command(
        cls,
        value: tuple[str, ...] | None,
    ) -> tuple[str, ...] | None:
        if value is None:
            return None
        if not value or any(not item for item in value):
            raise ValueError("daily offline test receipt signer command must be nonempty")
        _reject_private_key_material_args(value)
        _reject_privileged_signer_command(value)
        executable = Path(value[0])
        if not executable.is_absolute() or executable != Path(os.path.abspath(executable)):
            raise ValueError("daily offline test receipt signer command must be absolute")
        return value

    @model_validator(mode="after")
    def require_exact_stage_commands(self) -> Self:
        stage_ids = tuple(command.stage_id for command in self.stage_commands)
        if stage_ids != self.stages:
            raise ValueError("daily orchestrator stage_commands must cover the fixed DAG")
        if self.receipt_active_key_id in self.receipt_previous_public_key_pems:
            raise ValueError("daily receipt active key cannot also be previous")
        has_command = self.offline_test_receipt_signer_command is not None
        has_socket = self.receipt_signer_socket_endpoint is not None
        if has_command == has_socket:
            raise ValueError("daily receipt signer authority must be exactly one capability")
        if has_socket:
            if self.receipt_signer_test_mode:
                if (
                    self.receipt_signer_socket_endpoint == DAILY_RECEIPT_SOCKET_ENDPOINT
                    or self.receipt_trusted_keyring_path == DAILY_RECEIPT_TRUSTED_KEYRING_PATH
                ):
                    raise ValueError(
                        "daily receipt test socket must not select a production authority path"
                    )
            else:
                if self.receipt_signer_socket_endpoint != DAILY_RECEIPT_SOCKET_ENDPOINT:
                    raise ValueError("daily receipt signer socket endpoint must be fixed")
                if self.receipt_trusted_keyring_path != DAILY_RECEIPT_TRUSTED_KEYRING_PATH:
                    raise ValueError("daily receipt trusted keyring path must be fixed")
        elif self.receipt_signer_test_mode:
            raise ValueError("daily receipt test mode requires a socket authority")
        return self


def _reject_private_key_material_args(command: tuple[str, ...]) -> None:
    for argument in command[1:]:
        name = Path(argument).name.lower()
        if any(fragment in name for fragment in _FORBIDDEN_SIGNER_ARG_NAMES):
            raise ValueError("daily receipt signer command must not include private key material")


def _reject_privileged_signer_command(command: tuple[str, ...]) -> None:
    """The offline test signer stays offline: no sudo, no root-owned production helper."""

    if Path(command[0]).name.lower() in _PRIVILEGE_ESCALATING_EXECUTABLES:
        raise ValueError("daily receipt signer command must not escalate privilege")
    if any(Path(argument).name == _PRODUCTION_DAILY_SIGNER_HELPER_NAME for argument in command):
        raise ValueError(
            "daily receipt signer command must not select the production signer helper"
        )


def _daily_production_signer_host() -> bool:
    """A host carrying the fixed Daily socket or root keyring is a production host."""

    for path in (DAILY_RECEIPT_SOCKET_ENDPOINT, DAILY_RECEIPT_TRUSTED_KEYRING_PATH):
        try:
            if path.exists():
                return True
        except OSError:
            return True
    return False


class DailyShadowStageCompletionReceipt(RuntimeContractModel):
    contract: Literal["daily-shadow-stage-completion-receipt/v2"] = (
        "daily-shadow-stage-completion-receipt/v2"
    )
    receipt_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    mode: DailyPipelineMode
    run_id: str
    stage_id: str
    effect_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    idempotency_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_identity: str = Field(min_length=1, max_length=256)
    stage_order: int = Field(ge=0, lt=len(_FAN_IN_STAGES))
    exit_status: int
    output_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    dependency_receipt_ids: tuple[str, ...] = ()
    previous_receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str = Field(min_length=1, max_length=16_384)
    result: StageResult
    issued_at: datetime

    @field_validator("dependency_receipt_ids")
    @classmethod
    def validate_dependency_receipt_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(
            len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
            for item in value
        ):
            raise ValueError("daily shadow dependency receipt ids must be sha256 hex digests")
        return value

    @model_validator(mode="after")
    def bind_identity(self) -> Self:
        object.__setattr__(self, "issued_at", self.issued_at.astimezone(UTC))
        expected_command_identity = _shadow_stage_command_identity(self.stage_id)
        if self.command_identity != expected_command_identity:
            raise ValueError("daily shadow stage completion receipt command identity mismatch")
        if self.stage_order != _shadow_stage_order(self.stage_id):
            raise ValueError("daily shadow stage completion receipt stage order mismatch")
        if self.previous_receipt_hash != _previous_receipt_hash(self.dependency_receipt_ids):
            raise ValueError("daily shadow stage completion receipt previous hash mismatch")
        if not _valid_ed25519_signature(self.signature):
            raise ValueError("daily shadow stage completion receipt signature is not Ed25519")
        identity_payload = _shadow_stage_receipt_identity_payload(
            mode=self.mode,
            run_id=self.run_id,
            stage_id=self.stage_id,
            effect_id=self.effect_id,
            idempotency_key=self.idempotency_key,
            input_identity=self.input_identity,
            source_generation_id=self.source_generation_id,
            source_content_hash=self.source_content_hash,
            command_manifest_hash=self.command_manifest_hash,
            profile_hash=self.profile_hash,
            command_identity=self.command_identity,
            stage_order=self.stage_order,
            exit_status=self.exit_status,
            output_identity=self.output_identity,
            dependency_receipt_ids=self.dependency_receipt_ids,
            previous_receipt_hash=self.previous_receipt_hash,
            key_id=self.key_id,
            signature_algorithm=self.signature_algorithm,
            result=self.result,
            issued_at=self.issued_at,
        )
        expected = canonical_sha256(identity_payload)
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("daily shadow stage completion receipt identity mismatch")
        return self


class DailyShadowRunCompletionReceipt(RuntimeContractModel):
    """Immutable signed completion evidence for one full Shadow daily run."""

    contract: Literal["daily-shadow-run-completion-receipt/v1"] = (
        "daily-shadow-run-completion-receipt/v1"
    )
    receipt_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    mode: DailyPipelineMode
    run_id: str
    trade_date: date
    input_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    stage_order: tuple[str, ...] = _FAN_IN_STAGES
    stage_receipt_ids: tuple[str, ...] = Field(min_length=len(_FAN_IN_STAGES))
    previous_receipt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    final_backup_stage_id: Literal["backup"] = "backup"
    final_backup_receipt_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    final_backup_output_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    final_backup_result: StageResult
    exit_status: int
    key_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,127}$")
    signature_algorithm: Literal["ed25519"] = "ed25519"
    signature: str = Field(min_length=1, max_length=16_384)
    issued_at: datetime

    @field_validator("stage_receipt_ids")
    @classmethod
    def validate_stage_receipt_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(_FAN_IN_STAGES) or any(
            len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
            for item in value
        ):
            raise ValueError("daily run completion stage receipt ids are invalid")
        return value

    @model_validator(mode="after")
    def bind_identity(self) -> Self:
        object.__setattr__(self, "issued_at", self.issued_at.astimezone(UTC))
        if self.mode is not DailyPipelineMode.SHADOW:
            raise ValueError("daily run completion receipt must be shadow mode")
        if self.stage_order != _FAN_IN_STAGES:
            raise ValueError("daily run completion receipt stage order is invalid")
        if self.exit_status != 0:
            raise ValueError("daily run completion receipt cannot attest a failed run")
        if self.final_backup_receipt_id != self.stage_receipt_ids[-1]:
            raise ValueError("daily run completion backup receipt is not final")
        expected_previous = _previous_receipt_hash(self.stage_receipt_ids)
        if self.previous_receipt_hash != expected_previous:
            raise ValueError("daily run completion receipt previous hash mismatch")
        if not _valid_ed25519_signature(self.signature):
            raise ValueError("daily run completion receipt signature is not Ed25519")
        identity_payload = _shadow_run_receipt_identity_payload(
            mode=self.mode,
            run_id=self.run_id,
            trade_date=self.trade_date,
            input_identity=self.input_identity,
            source_generation_id=self.source_generation_id,
            source_content_hash=self.source_content_hash,
            command_manifest_hash=self.command_manifest_hash,
            profile_hash=self.profile_hash,
            code_commit=self.code_commit,
            stage_order=self.stage_order,
            stage_receipt_ids=self.stage_receipt_ids,
            previous_receipt_hash=self.previous_receipt_hash,
            final_backup_stage_id=self.final_backup_stage_id,
            final_backup_receipt_id=self.final_backup_receipt_id,
            final_backup_output_identity=self.final_backup_output_identity,
            final_backup_result=self.final_backup_result,
            exit_status=self.exit_status,
            key_id=self.key_id,
            signature_algorithm=self.signature_algorithm,
            issued_at=self.issued_at,
        )
        expected = canonical_sha256(identity_payload)
        if self.receipt_id is None:
            object.__setattr__(self, "receipt_id", expected)
        elif self.receipt_id != expected:
            raise ValueError("daily run completion receipt identity mismatch")
        return self


def _previous_receipt_hash(dependency_receipt_ids: tuple[str, ...]) -> str:
    return canonical_sha256(
        {
            "contract": "daily-shadow-stage-previous-receipt-chain/v1",
            "dependency_receipt_ids": dependency_receipt_ids,
        }
    )


def daily_shadow_stage_receipt_keyring(
    settings: DailyPipelineOrchestratorSettings,
) -> Ed25519DailyShadowStageReceiptKeyring:
    verified = DailyPipelineOrchestratorSettings.model_validate(settings)
    return Ed25519DailyShadowStageReceiptKeyring(
        active_key_id=verified.receipt_active_key_id,
        active_public_key=verified.receipt_active_public_key_pem.encode("utf-8"),
        previous_public_keys={
            key_id: public_key.encode("utf-8")
            for key_id, public_key in verified.receipt_previous_public_key_pems.items()
        },
    )


def daily_shadow_stage_receipt_signer(
    settings: DailyPipelineOrchestratorSettings,
) -> Ed25519DailyShadowStageReceiptSigner:
    verified = DailyPipelineOrchestratorSettings.model_validate(settings)
    if verified.receipt_signer_socket_endpoint is not None:
        if verified.receipt_signer_test_mode:
            return Ed25519DailyShadowStageReceiptSigner(
                key_id=verified.receipt_active_key_id,
                client=DailyReceiptSocketSigningClient.for_test_endpoint(
                    socket_path=verified.receipt_signer_socket_endpoint,
                    active_key_id=verified.receipt_active_key_id,
                    active_public_key_pem=verified.receipt_active_public_key_pem,
                    previous_public_key_pems=dict(verified.receipt_previous_public_key_pems),
                    timeout_seconds=verified.receipt_signer_timeout_seconds,
                ),
            )
        return Ed25519DailyShadowStageReceiptSigner(
            key_id=verified.receipt_active_key_id,
            client=DailyReceiptSocketSigningClient.production(
                active_key_id=verified.receipt_active_key_id,
                active_public_key_pem=verified.receipt_active_public_key_pem,
                previous_public_key_pems=dict(verified.receipt_previous_public_key_pems),
                timeout_seconds=verified.receipt_signer_timeout_seconds,
            ),
        )
    if verified.offline_test_receipt_signer_command is None:
        raise ValueError("daily receipt signer authority is missing")
    return Ed25519DailyShadowStageReceiptSigner(
        key_id=verified.receipt_active_key_id,
        client=SecureDailyShadowStageReceiptSigningClient(
            command=verified.offline_test_receipt_signer_command,
            key_id=verified.receipt_active_key_id,
            timeout_seconds=verified.receipt_signer_timeout_seconds,
        ),
    )


def issue_daily_shadow_stage_completion_receipt(
    *,
    signer: Ed25519DailyShadowStageReceiptSigner,
    mode: DailyPipelineMode,
    run_id: str,
    stage_id: str,
    effect_id: str,
    idempotency_key: str,
    input_identity: str,
    source_generation_id: str,
    source_content_hash: str,
    command_manifest_hash: str,
    profile_hash: str,
    command_identity: str,
    stage_order: int,
    exit_status: int,
    output_identity: str,
    dependency_receipt_ids: tuple[str, ...],
    result: StageResult,
    issued_at: datetime,
) -> DailyShadowStageCompletionReceipt:
    verified_result = StageResult.model_validate(result)
    issued_at_utc = issued_at.astimezone(UTC)
    previous_receipt_hash = _previous_receipt_hash(dependency_receipt_ids)
    identity_payload = _shadow_stage_receipt_identity_payload(
        mode=mode,
        run_id=run_id,
        stage_id=stage_id,
        effect_id=effect_id,
        idempotency_key=idempotency_key,
        input_identity=input_identity,
        source_generation_id=source_generation_id,
        source_content_hash=source_content_hash,
        command_manifest_hash=command_manifest_hash,
        profile_hash=profile_hash,
        command_identity=command_identity,
        stage_order=stage_order,
        exit_status=exit_status,
        output_identity=output_identity,
        dependency_receipt_ids=dependency_receipt_ids,
        previous_receipt_hash=previous_receipt_hash,
        key_id=signer.key_id,
        signature_algorithm="ed25519",
        result=verified_result,
        issued_at=issued_at_utc,
    )
    receipt_id = canonical_sha256(identity_payload)
    signature = signer.sign_receipt(
        receipt_id=receipt_id,
        identity_payload=identity_payload,
    )
    return DailyShadowStageCompletionReceipt(
        **identity_payload,
        receipt_id=receipt_id,
        signature=signature,
    )


def issue_daily_shadow_run_completion_receipt(
    *,
    signer: Ed25519DailyShadowStageReceiptSigner,
    run: object,
    stage_receipts: tuple[DailyShadowStageCompletionReceipt, ...],
    issued_at: datetime,
) -> DailyShadowRunCompletionReceipt:
    from rquant.daily_pipeline_ledger import DailyRunRecord

    verified_run = DailyRunRecord.model_validate(run)
    receipts = tuple(
        DailyShadowStageCompletionReceipt.model_validate(item) for item in stage_receipts
    )
    if verified_run.state.value != "succeeded":
        raise ValueError("daily run completion receipt requires a succeeded run")
    if (
        len(receipts) != len(_FAN_IN_STAGES)
        or tuple(item.stage_id for item in receipts) != _FAN_IN_STAGES
    ):
        raise ValueError("daily run completion receipt requires the complete ordered stage chain")
    for order, receipt in enumerate(receipts):
        if (
            receipt.mode is not DailyPipelineMode.SHADOW
            or receipt.run_id != verified_run.run_id
            or receipt.input_identity != verified_run.input_identity
            or receipt.source_generation_id != verified_run.spec.source_generation_id
            or receipt.source_content_hash != verified_run.spec.source_content_hash
            or receipt.command_manifest_hash != verified_run.spec.command_manifest_hash
            or receipt.profile_hash != verified_run.spec.profile_hash
            or receipt.stage_order != order
            or receipt.exit_status != 0
        ):
            raise ValueError("daily run completion receipt stage binding is invalid")
    backup = receipts[-1]
    stage_receipt_ids = tuple(str(item.receipt_id) for item in receipts)
    identity_payload = _shadow_run_receipt_identity_payload(
        mode=verified_run.spec.mode,
        run_id=verified_run.run_id,
        trade_date=verified_run.spec.trade_date,
        input_identity=verified_run.input_identity,
        source_generation_id=verified_run.spec.source_generation_id,
        source_content_hash=verified_run.spec.source_content_hash,
        command_manifest_hash=verified_run.spec.command_manifest_hash,
        profile_hash=verified_run.spec.profile_hash,
        code_commit=verified_run.spec.code_commit,
        stage_order=_FAN_IN_STAGES,
        stage_receipt_ids=stage_receipt_ids,
        previous_receipt_hash=_previous_receipt_hash(stage_receipt_ids),
        final_backup_stage_id="backup",
        final_backup_receipt_id=str(backup.receipt_id),
        final_backup_output_identity=backup.output_identity,
        final_backup_result=backup.result,
        exit_status=0,
        key_id=signer.key_id,
        signature_algorithm="ed25519",
        issued_at=issued_at.astimezone(UTC),
    )
    receipt_id = canonical_sha256(identity_payload)
    signature = signer.sign_run_completion(
        receipt_id=receipt_id,
        identity_payload=identity_payload,
    )
    return DailyShadowRunCompletionReceipt(
        **identity_payload,
        receipt_id=receipt_id,
        signature=signature,
    )


def _daily_run_completion_receipt_path(
    storage_profile: DailyPipelineStorageProfile,
    receipt_id: str,
) -> Path:
    return storage_profile.report_root / f"{receipt_id}.json"


def persist_daily_shadow_run_completion_receipt(
    storage_profile: DailyPipelineStorageProfile,
    receipt: DailyShadowRunCompletionReceipt,
) -> Path:
    from rquant.daily_pipeline_ledger import DailyPipelineStorageBinding

    verified = DailyShadowRunCompletionReceipt.model_validate(receipt)
    DailyPipelineStorageBinding.open(storage_profile, leaf="reports").close()
    path = _daily_run_completion_receipt_path(storage_profile, str(verified.receipt_id))
    _safe_write_once(
        path,
        canonical_json_bytes(verified.model_dump(mode="json")),
        label="daily shadow run completion receipt",
    )
    return path


class DailyShadowStageArtifact(RuntimeContractModel):
    contract: Literal["daily-shadow-stage-artifact/v1"] = "daily-shadow-stage-artifact/v1"
    artifact_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    mode: DailyPipelineMode
    run_id: str
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    action: str = Field(min_length=1, max_length=128)
    effect_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    idempotency_key: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_manifest_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    profile_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    command_identity: str = Field(min_length=1, max_length=256)
    stage_order: int = Field(ge=0, lt=len(_FAN_IN_STAGES))
    exit_status: int
    output_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    output_files: tuple[DailyShadowStageOutputFile, ...] = Field(min_length=1, max_length=16)
    dependency_receipt_ids: tuple[str, ...] = ()
    artifact_path: Path
    created_at: datetime

    @field_validator("dependency_receipt_ids")
    @classmethod
    def validate_dependency_receipt_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(
            len(item) != 64 or any(character not in "0123456789abcdef" for character in item)
            for item in value
        ):
            raise ValueError("daily shadow dependency receipt ids must be sha256 hex digests")
        return value

    @field_validator("artifact_path")
    @classmethod
    def require_absolute_artifact_path(cls, value: Path) -> Path:
        if not value.is_absolute() or value != Path(os.path.abspath(value)):
            raise ValueError("daily shadow stage artifact path must be absolute and normalized")
        return value

    @model_validator(mode="after")
    def bind_artifact_identity(self) -> Self:
        object.__setattr__(self, "created_at", self.created_at.astimezone(UTC))
        expected_action = _SHADOW_STAGE_ACTIONS.get(self.stage_id)
        if expected_action is None or self.action != expected_action:
            raise ValueError("daily shadow stage action is not allowlisted")
        if self.command_identity != _shadow_stage_command_identity(self.stage_id):
            raise ValueError("daily shadow stage artifact command identity mismatch")
        if self.stage_order != _shadow_stage_order(self.stage_id):
            raise ValueError("daily shadow stage artifact stage order mismatch")
        if self.exit_status != 0:
            raise ValueError("daily shadow stage artifact must persist a successful exit status")
        output_hashes = {item.sha256 for item in self.output_files}
        if self.output_identity not in output_hashes:
            raise ValueError(
                "daily shadow stage artifact output identity must match a physical output"
            )
        expected = canonical_sha256(
            self.model_dump(
                mode="json",
                exclude={"artifact_id", "created_at"},
            )
        )
        if self.artifact_id is None:
            object.__setattr__(self, "artifact_id", expected)
        elif self.artifact_id != expected:
            raise ValueError("daily shadow stage artifact identity mismatch")
        return self


class DailyShadowStageOutputFile(RuntimeContractModel):
    logical_name: str = Field(min_length=1, max_length=64)
    relative_path: str = Field(min_length=1, max_length=512)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        candidate = Path(value)
        if candidate.is_absolute():
            raise ValueError("daily shadow stage output file path must be relative")
        normalized = Path(os.path.normpath(value))
        if normalized != candidate or ".." in normalized.parts or "." in normalized.parts:
            raise ValueError("daily shadow stage output file path must be normalized")
        return value


@dataclass(frozen=True)
class _ShadowStagePublication:
    generation_id: str
    output_identity: str
    output_files: tuple[DailyShadowStageOutputFile, ...]


class _DailyCloseSourceIdentity(RuntimeContractModel):
    trade_date: date
    source_generation_id: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def _load_profile(path: Path) -> object:
    from rquant.runtime_deployment_profile import RuntimeDeploymentProfile

    try:
        payload = path.read_bytes()
        return RuntimeDeploymentProfile.model_validate(strict_json_loads(payload))
    except (OSError, StrictJsonError, UnicodeDecodeError, ValueError) as exc:
        raise ValueError("daily orchestrator deployment profile is unavailable or invalid") from exc


def _daily_shadow_profile_path(storage_profile: DailyPipelineStorageProfile) -> Path:
    root = storage_profile.root
    if len(root.parts) < 3 or root.name != "daily-pipeline" or root.parent.name != "research":
        raise ValueError("daily shadow storage root cannot resolve its deployment profile")
    return root.parent.parent / "current" / "deployment-profile.json"


def _daily_shadow_orchestrator_settings(
    storage_profile: DailyPipelineStorageProfile,
) -> tuple[object, RuntimeServiceManifest, DailyPipelineOrchestratorSettings]:
    from rquant.runtime_deployment_profile import RuntimeDeploymentProfile

    profile = _load_profile(_daily_shadow_profile_path(storage_profile))
    if not isinstance(profile, RuntimeDeploymentProfile) or profile.profile_id is None:
        raise ValueError("daily shadow deployment profile is unavailable or invalid")
    if profile.profile_id != storage_profile.profile_hash:
        raise ValueError("daily shadow storage profile does not match deployment profile")
    matches = tuple(
        manifest
        for manifest in profile.manifests
        if manifest.service_kind is RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR
    )
    for manifest in matches:
        settings = DailyPipelineOrchestratorSettings.model_validate(dict(manifest.settings))
        if (
            settings.mode == "shadow"
            and settings.storage_root == storage_profile.root
            and storage_profile.namespace_id
            == DailyPipelineStorageProfile.create(
                root=settings.storage_root,
                mode=DailyPipelineMode.SHADOW,
                profile_hash=profile.profile_id,
            ).namespace_id
        ):
            return profile, manifest, settings
    raise ValueError("daily shadow orchestrator authority is missing")


def load_daily_shadow_run_completion_receipt(
    storage_profile: DailyPipelineStorageProfile,
    receipt_id: str,
) -> DailyShadowRunCompletionReceipt:
    verified_storage = DailyPipelineStorageProfile.model_validate(storage_profile)
    if len(receipt_id) != 64 or any(
        character not in "0123456789abcdef" for character in receipt_id
    ):
        raise ValueError("daily run completion receipt id is invalid")
    _profile, _manifest, settings = _daily_shadow_orchestrator_settings(verified_storage)
    receipt_keyring = daily_shadow_stage_receipt_keyring(settings)
    completion_receipt = DailyShadowRunCompletionReceipt.model_validate(
        strict_json_loads(
            _read_bounded_regular(
                _daily_run_completion_receipt_path(verified_storage, receipt_id),
                label="daily shadow run completion receipt",
            )
        )
    )
    if not receipt_keyring.verify_run_completion_receipt(completion_receipt, require_active=True):
        raise ValueError("daily shadow run completion receipt verification failed")
    return completion_receipt


def _safe_write_once(path: Path, payload: bytes, *, label: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        os.write(descriptor, payload)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
    except FileExistsError:
        if _read_bounded_regular(path, label=label) != payload:
            raise ValueError(f"{label} conflicts with an existing immutable file") from None
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_bounded_regular(path: Path, *, label: str) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
        )
        observed = os.fstat(descriptor)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise ValueError(f"{label} is unavailable") from exc
    try:
        if not stat.S_ISREG(observed.st_mode):
            raise ValueError(f"{label} must be a regular file")
        if observed.st_size > _MAX_STAGE_COMPLETION_BYTES:
            raise ValueError(f"{label} exceeds the bounded receipt size")
        return os.read(descriptor, _MAX_STAGE_COMPLETION_BYTES + 1)
    finally:
        os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path, *, label: str) -> str:
    return _sha256_bytes(_read_bounded_regular(path, label=label))


def _physical_stage_root(
    storage_profile: DailyPipelineStorageProfile,
    *,
    stage_id: str,
) -> Path:
    directory = _PHYSICAL_STAGE_ROOTS.get(stage_id)
    if directory is None:
        raise RuntimeError("daily shadow stage has no physical publication root")
    root = storage_profile.namespace_root / directory
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root


def _write_temp_file(path: Path, payload: bytes) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        os.write(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _safe_publish_immutable(path: Path, payload: bytes, *, label: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists():
        if _read_bounded_regular(path, label=label) != payload:
            raise RuntimeError(f"{label} conflicts with an existing immutable file")
        return
    temp_path = path.parent / f".{path.name}.tmp-{os.getpid()}"
    counter = 0
    while temp_path.exists():
        counter += 1
        temp_path = path.parent / f".{path.name}.tmp-{os.getpid()}-{counter}"
    _write_temp_file(temp_path, payload)
    try:
        os.replace(temp_path, path)
    except FileExistsError:
        temp_path.unlink(missing_ok=True)
        if _read_bounded_regular(path, label=label) != payload:
            raise RuntimeError(f"{label} conflicts with an existing immutable file") from None
    _fsync_directory(path.parent)


def _atomic_replace_file(path: Path, payload: bytes, *, label: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    if path.exists():
        try:
            if _read_bounded_regular(path, label=label) == payload:
                return
        except FileNotFoundError:
            pass
    temp_path = path.parent / f".{path.name}.tmp-{os.getpid()}"
    counter = 0
    while temp_path.exists():
        counter += 1
        temp_path = path.parent / f".{path.name}.tmp-{os.getpid()}-{counter}"
    _write_temp_file(temp_path, payload)
    os.replace(temp_path, path)
    _fsync_directory(path.parent)


def _publish_generation_directory(
    *,
    stage_root: Path,
    generation_id: str,
    files: Mapping[str, bytes],
    label: str,
) -> None:
    generations_root = stage_root / "generations"
    generations_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = generations_root / generation_id
    if target.exists():
        for relative_path, payload in files.items():
            observed = _read_bounded_regular(target / relative_path, label=label)
            if observed != payload:
                raise RuntimeError(
                    f"{label} generation conflicts with an existing immutable output"
                )
        return
    temp_root = generations_root / f".tmp-{generation_id}-{os.getpid()}"
    counter = 0
    while temp_root.exists():
        counter += 1
        temp_root = generations_root / f".tmp-{generation_id}-{os.getpid()}-{counter}"
    temp_root.mkdir(mode=0o700, parents=True)
    for relative_path, payload in files.items():
        destination = temp_root / relative_path
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        _write_temp_file(destination, payload)
        _fsync_directory(destination.parent)
    _fsync_directory(temp_root)
    os.replace(temp_root, target)
    _fsync_directory(generations_root)


def _record_output_files(
    stage_root: Path,
    *,
    outputs: Mapping[str, bytes],
) -> tuple[DailyShadowStageOutputFile, ...]:
    records: list[DailyShadowStageOutputFile] = []
    for logical_name, payload in outputs.items():
        if logical_name == "payload":
            relative_path = f"payloads/{_sha256_bytes(payload)}.bin"
        elif logical_name == "manifest":
            relative_path = f"manifests/{_sha256_bytes(payload)}.json"
        elif logical_name == "catalog":
            relative_path = f"catalogs/{_sha256_bytes(payload)}.json"
        elif logical_name == "lake":
            relative_path = f"lake/{_sha256_bytes(payload)}.json"
        else:
            raise RuntimeError("daily shadow stage output logical name is not allowlisted")
        path = stage_root / relative_path
        records.append(
            DailyShadowStageOutputFile(
                logical_name=logical_name,
                relative_path=str(path.relative_to(stage_root.parent)),
                sha256=_sha256_bytes(payload),
                size_bytes=len(payload),
            )
        )
    return tuple(sorted(records, key=lambda item: item.logical_name))


def _publish_stage_outputs(
    *,
    storage_profile: DailyPipelineStorageProfile,
    stage_id: str,
    outputs: Mapping[str, bytes],
    current_payload: Mapping[str, object],
    label: str,
    output_identity_logical_name: str = "manifest",
) -> _ShadowStagePublication:
    stage_root = _physical_stage_root(storage_profile, stage_id=stage_id)
    output_files = _record_output_files(stage_root, outputs=outputs)
    output_map = {entry.logical_name: entry for entry in output_files}
    if output_identity_logical_name not in output_map:
        raise RuntimeError("daily shadow stage output identity logical name is missing")
    for entry in output_files:
        _safe_publish_immutable(
            storage_profile.namespace_root / entry.relative_path,
            outputs[entry.logical_name],
            label=f"{label} {entry.logical_name}",
        )
    generation_id = canonical_sha256(
        {
            "contract": "daily-shadow-stage-generation/v1",
            "stage_id": stage_id,
            "current": dict(current_payload),
            "outputs": [entry.model_dump(mode="json") for entry in output_files],
        }
    )
    _publish_generation_directory(
        stage_root=stage_root,
        generation_id=generation_id,
        files={
            "current.json": canonical_json_bytes(dict(current_payload)),
            **{
                entry.relative_path.removeprefix(f"{stage_root.parent.name}/"): outputs[
                    entry.logical_name
                ]
                for entry in output_files
            },
        },
        label=label,
    )
    _atomic_replace_file(
        stage_root / "current.json",
        canonical_json_bytes(dict(current_payload)),
        label=f"{label} current pointer",
    )
    return _ShadowStagePublication(
        generation_id=generation_id,
        output_identity=output_map[output_identity_logical_name].sha256,
        output_files=output_files,
    )


def _publish_generic_stage_artifact(
    *,
    storage_profile: DailyPipelineStorageProfile,
    stage_id: str,
    values: Mapping[str, object],
    dependency_receipt_ids: tuple[str, ...],
) -> _ShadowStagePublication:
    stage_root = storage_profile.namespace_root / "effects" / stage_id / "outputs"
    payload = canonical_json_bytes(
        {
            "contract": "daily-shadow-stage-generic-output/v1",
            "stage_id": stage_id,
            "run_id": values["run_id"],
            "effect_id": values["effect_id"],
            "input_identity": values["input_identity"],
            "source_generation_id": values["source_generation_id"],
            "source_content_hash": values["source_content_hash"],
            "dependency_receipt_ids": dependency_receipt_ids,
        }
    )
    stage_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    digest = _sha256_bytes(payload)
    path = stage_root / f"{digest}.json"
    _safe_publish_immutable(path, payload, label=f"daily shadow {stage_id} output")
    return _ShadowStagePublication(
        generation_id=canonical_sha256(
            {
                "contract": "daily-shadow-stage-generation/v1",
                "stage_id": stage_id,
                "sha256": digest,
            }
        ),
        output_identity=digest,
        output_files=(
            DailyShadowStageOutputFile(
                logical_name="manifest",
                relative_path=str(path.relative_to(storage_profile.namespace_root)),
                sha256=digest,
                size_bytes=len(payload),
            ),
        ),
    )


def _publish_serving_refresh(
    *,
    storage_profile: DailyPipelineStorageProfile,
    values: Mapping[str, object],
) -> _ShadowStagePublication:
    payload = canonical_json_bytes(
        {
            "contract": "daily-shadow-serving-payload/v1",
            "stage_id": "serving_refresh",
            "run_id": values["run_id"],
            "input_identity": values["input_identity"],
            "source_generation_id": values["source_generation_id"],
            "source_content_hash": values["source_content_hash"],
            "profile_hash": values["profile_hash"],
            "command_manifest_hash": values["command_manifest_hash"],
        }
    )
    payload_sha256 = _sha256_bytes(payload)
    manifest = canonical_json_bytes(
        {
            "contract": "daily-shadow-serving-manifest/v1",
            "stage_id": "serving_refresh",
            "generation_hint": canonical_sha256(
                {
                    "contract": "daily-shadow-serving-generation-hint/v1",
                    "payload_sha256": payload_sha256,
                    "profile_hash": values["profile_hash"],
                    "run_id": values["run_id"],
                }
            ),
            "payload_sha256": payload_sha256,
            "source_generation_id": values["source_generation_id"],
            "source_content_hash": values["source_content_hash"],
        }
    )
    manifest_sha256 = _sha256_bytes(manifest)
    current = {
        "contract": "daily-shadow-serving-current/v1",
        "payload_sha256": payload_sha256,
        "manifest_sha256": manifest_sha256,
    }
    return _publish_stage_outputs(
        storage_profile=storage_profile,
        stage_id="serving_refresh",
        outputs={"payload": payload, "manifest": manifest},
        current_payload=current,
        label="daily shadow serving refresh",
    )


def _publish_replica_sync(storage_profile: DailyPipelineStorageProfile) -> _ShadowStagePublication:
    serving_root = _physical_stage_root(storage_profile, stage_id="serving_refresh")
    _serving_current, serving_manifest, serving_payload = _verified_document_binding(
        serving_root,
        label="daily shadow serving refresh",
        current_contract="daily-shadow-serving-current/v1",
        manifest_contract="daily-shadow-serving-manifest/v1",
    )
    payload = bytes(serving_payload)
    payload_sha256 = _sha256_bytes(payload)
    manifest = canonical_json_bytes(
        {
            "contract": "daily-shadow-replica-manifest/v1",
            "stage_id": "replica_sync",
            "payload_sha256": payload_sha256,
            "upstream_manifest_sha256": _sha256_bytes(canonical_json_bytes(serving_manifest)),
        }
    )
    manifest_sha256 = _sha256_bytes(manifest)
    current = {
        "contract": "daily-shadow-replica-current/v1",
        "payload_sha256": payload_sha256,
        "manifest_sha256": manifest_sha256,
    }
    return _publish_stage_outputs(
        storage_profile=storage_profile,
        stage_id="replica_sync",
        outputs={"payload": payload, "manifest": manifest},
        current_payload=current,
        label="daily shadow replica sync",
    )


def _publish_research_ingest(
    storage_profile: DailyPipelineStorageProfile,
) -> _ShadowStagePublication:
    replica_root = _physical_stage_root(storage_profile, stage_id="replica_sync")
    _replica_current, replica_manifest, replica_payload = _verified_document_binding(
        replica_root,
        label="daily shadow replica sync",
        current_contract="daily-shadow-replica-current/v1",
        manifest_contract="daily-shadow-replica-manifest/v1",
    )
    payload_sha256 = _sha256_bytes(replica_payload)
    catalog = canonical_json_bytes(
        {
            "contract": "daily-shadow-research-catalog/v1",
            "stage_id": "research_ingest",
            "replica_payload_sha256": payload_sha256,
            "replica_manifest_sha256": _sha256_bytes(canonical_json_bytes(replica_manifest)),
        }
    )
    lake = canonical_json_bytes(
        {
            "contract": "daily-shadow-research-lake/v1",
            "stage_id": "research_ingest",
            "replica_payload_sha256": payload_sha256,
        }
    )
    catalog_sha256 = _sha256_bytes(catalog)
    lake_sha256 = _sha256_bytes(lake)
    manifest = canonical_json_bytes(
        {
            "contract": "daily-shadow-research-manifest/v1",
            "stage_id": "research_ingest",
            "catalog_sha256": catalog_sha256,
            "lake_sha256": lake_sha256,
        }
    )
    manifest_sha256 = _sha256_bytes(manifest)
    current = {
        "contract": "daily-shadow-research-current/v1",
        "manifest_sha256": manifest_sha256,
        "catalog_sha256": catalog_sha256,
        "lake_sha256": lake_sha256,
    }
    return _publish_stage_outputs(
        storage_profile=storage_profile,
        stage_id="research_ingest",
        outputs={"manifest": manifest, "catalog": catalog, "lake": lake},
        current_payload=current,
        label="daily shadow research ingest",
    )


def _publish_backup(storage_profile: DailyPipelineStorageProfile) -> _ShadowStagePublication:
    research_root = _physical_stage_root(storage_profile, stage_id="research_ingest")
    (
        _research_current,
        research_manifest,
        research_catalog,
        research_lake,
    ) = _verified_research_binding(
        research_root,
        label="daily shadow research ingest",
        current_contract="daily-shadow-research-current/v1",
        manifest_contract="daily-shadow-research-manifest/v1",
    )
    catalog = bytes(research_catalog)
    lake = bytes(research_lake)
    catalog_sha256 = _sha256_bytes(catalog)
    lake_sha256 = _sha256_bytes(lake)
    manifest = canonical_json_bytes(
        {
            "contract": "daily-shadow-backup-manifest/v1",
            "stage_id": "backup",
            "catalog_sha256": catalog_sha256,
            "lake_sha256": lake_sha256,
            "upstream_manifest_sha256": _sha256_bytes(canonical_json_bytes(research_manifest)),
        }
    )
    manifest_sha256 = _sha256_bytes(manifest)
    current = {
        "contract": "daily-shadow-backup-current/v1",
        "manifest_sha256": manifest_sha256,
        "catalog_sha256": catalog_sha256,
        "lake_sha256": lake_sha256,
    }
    return _publish_stage_outputs(
        storage_profile=storage_profile,
        stage_id="backup",
        outputs={"manifest": manifest, "catalog": catalog, "lake": lake},
        current_payload=current,
        label="daily shadow backup",
    )


def _publish_stage_artifact(
    *,
    storage_profile: DailyPipelineStorageProfile,
    stage_id: str,
    values: Mapping[str, object],
    dependency_receipt_ids: tuple[str, ...],
) -> _ShadowStagePublication:
    if stage_id == "serving_refresh":
        return _publish_serving_refresh(storage_profile=storage_profile, values=values)
    if stage_id == "replica_sync":
        return _publish_replica_sync(storage_profile)
    if stage_id == "research_ingest":
        return _publish_research_ingest(storage_profile)
    if stage_id == "backup":
        return _publish_backup(storage_profile)
    return _publish_generic_stage_artifact(
        storage_profile=storage_profile,
        stage_id=stage_id,
        values=values,
        dependency_receipt_ids=dependency_receipt_ids,
    )


def _verify_artifact_outputs(
    artifact: DailyShadowStageArtifact,
    *,
    storage_profile: DailyPipelineStorageProfile,
) -> None:
    observed_hashes: set[str] = set()
    for entry in artifact.output_files:
        path = storage_profile.namespace_root / entry.relative_path
        observed = _read_bounded_regular(path, label="daily shadow physical output")
        if len(observed) != entry.size_bytes:
            raise RuntimeError("daily shadow stage output size mismatch")
        digest = _sha256_bytes(observed)
        if digest != entry.sha256:
            raise RuntimeError("daily shadow stage output hash mismatch")
        observed_hashes.add(digest)
    if artifact.output_identity not in observed_hashes:
        raise RuntimeError("daily shadow stage output identity does not bind a physical output")


def _verified_document_binding(
    stage_root: Path,
    *,
    label: str,
    current_contract: str,
    manifest_contract: str,
) -> tuple[dict[str, object], dict[str, object], bytes]:
    current_path = stage_root / "current.json"
    current_bytes = _read_bounded_regular(current_path, label=f"{label} current pointer")
    current = strict_json_loads(current_bytes)
    if not isinstance(current, dict) or current.get("contract") != current_contract:
        raise RuntimeError(f"{label} current pointer is invalid")
    manifest_sha256 = str(current.get("manifest_sha256") or "")
    payload_sha256 = str(current.get("payload_sha256") or "")
    manifest_path = stage_root / "manifests" / f"{manifest_sha256}.json"
    payload_path = stage_root / "payloads" / f"{payload_sha256}.bin"
    manifest_bytes = _read_bounded_regular(manifest_path, label=f"{label} manifest")
    payload_bytes = _read_bounded_regular(payload_path, label=f"{label} payload")
    if _sha256_bytes(manifest_bytes) != manifest_sha256:
        raise RuntimeError(f"{label} manifest hash mismatch")
    if _sha256_bytes(payload_bytes) != payload_sha256:
        raise RuntimeError(f"{label} payload hash mismatch")
    manifest = strict_json_loads(manifest_bytes)
    if not isinstance(manifest, dict) or manifest.get("contract") != manifest_contract:
        raise RuntimeError(f"{label} manifest is invalid")
    if str(manifest.get("payload_sha256") or "") != payload_sha256:
        raise RuntimeError(f"{label} manifest binding mismatch")
    return current, manifest, payload_bytes


def _verified_research_binding(
    stage_root: Path,
    *,
    label: str,
    current_contract: str,
    manifest_contract: str,
) -> tuple[dict[str, object], dict[str, object], bytes, bytes]:
    current_path = stage_root / "current.json"
    current_bytes = _read_bounded_regular(current_path, label=f"{label} current pointer")
    current = strict_json_loads(current_bytes)
    if not isinstance(current, dict) or current.get("contract") != current_contract:
        raise RuntimeError(f"{label} current pointer is invalid")
    manifest_sha256 = str(current.get("manifest_sha256") or "")
    catalog_sha256 = str(current.get("catalog_sha256") or "")
    lake_sha256 = str(current.get("lake_sha256") or "")
    manifest_path = stage_root / "manifests" / f"{manifest_sha256}.json"
    catalog_path = stage_root / "catalogs" / f"{catalog_sha256}.json"
    lake_path = stage_root / "lake" / f"{lake_sha256}.json"
    manifest_bytes = _read_bounded_regular(manifest_path, label=f"{label} manifest")
    catalog_bytes = _read_bounded_regular(catalog_path, label=f"{label} catalog")
    lake_bytes = _read_bounded_regular(lake_path, label=f"{label} lake")
    if _sha256_bytes(manifest_bytes) != manifest_sha256:
        raise RuntimeError(f"{label} manifest hash mismatch")
    if _sha256_bytes(catalog_bytes) != catalog_sha256:
        raise RuntimeError(f"{label} catalog hash mismatch")
    if _sha256_bytes(lake_bytes) != lake_sha256:
        raise RuntimeError(f"{label} lake hash mismatch")
    manifest = strict_json_loads(manifest_bytes)
    if not isinstance(manifest, dict) or manifest.get("contract") != manifest_contract:
        raise RuntimeError(f"{label} manifest is invalid")
    if (
        str(manifest.get("catalog_sha256") or "") != catalog_sha256
        or str(manifest.get("lake_sha256") or "") != lake_sha256
    ):
        raise RuntimeError(f"{label} manifest binding mismatch")
    return current, manifest, catalog_bytes, lake_bytes


def _shadow_stage_result(*, artifact: DailyShadowStageArtifact) -> StageResult:
    return StageResult(
        content_hash=str(artifact.artifact_id),
        evidence_hash=canonical_sha256(
            {
                "contract": "daily-shadow-stage-evidence/v1",
                "artifact_id": artifact.artifact_id,
                "stage_id": artifact.stage_id,
                "action": artifact.action,
                "effect_id": artifact.effect_id,
                "command_identity": artifact.command_identity,
                "stage_order": artifact.stage_order,
                "exit_status": artifact.exit_status,
                "output_identity": artifact.output_identity,
            }
        ),
    )


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"daily shadow stage environment is missing {name}")
    return value


def daily_shadow_ledger_owner(service_owner: str) -> str:
    if not service_owner.strip():
        raise ValueError("daily shadow service owner cannot be empty")
    return (
        "daily-"
        + canonical_sha256(
            {
                "contract": "daily-shadow-ledger-owner/v1",
                "service_owner": service_owner,
            }
        )[:40]
    )


def _shadow_effect_artifact_path(
    storage_profile: DailyPipelineStorageProfile,
    *,
    stage_id: str,
    effect_id: str,
) -> Path:
    root = storage_profile.namespace_root / "effects" / stage_id
    path = root / f"{effect_id}.json"
    try:
        path.relative_to(storage_profile.namespace_root)
    except ValueError as exc:
        raise RuntimeError("daily shadow stage artifact escapes its storage profile") from exc
    return path


def _load_shadow_stage_artifact(path: Path) -> DailyShadowStageArtifact:
    return DailyShadowStageArtifact.model_validate(
        strict_json_loads(
            _read_bounded_regular(
                path,
                label="daily shadow stage artifact",
            )
        )
    )


def _dependency_receipt_ids_from_environment() -> tuple[str, ...]:
    raw = os.environ.get("RQ_DAILY_SHADOW_DEPENDENCY_RECEIPT_IDS", "[]")
    try:
        decoded = strict_json_loads(raw)
    except StrictJsonError as exc:
        raise RuntimeError("daily shadow dependency receipt ids are invalid") from exc
    if not isinstance(decoded, list):
        raise RuntimeError("daily shadow dependency receipt ids are invalid")
    return tuple(str(item) for item in decoded)


def _daily_test_receipt_signer_from_environment(
    *,
    socket_endpoint: Path,
    timeout_seconds: float,
) -> Ed25519DailyShadowStageReceiptSigner:
    key_id = _required_environment("RQ_DAILY_SHADOW_RECEIPT_SIGNER_KEY_ID")
    public_key = _required_environment("RQ_DAILY_SHADOW_RECEIPT_SIGNER_TEST_PUBLIC_KEY_PEM")
    return Ed25519DailyShadowStageReceiptSigner(
        key_id=key_id,
        client=DailyReceiptSocketSigningClient.for_test_endpoint(
            socket_path=socket_endpoint,
            active_key_id=key_id,
            active_public_key_pem=public_key,
            timeout_seconds=timeout_seconds,
        ),
    )


def _daily_receipt_signer_from_environment() -> Ed25519DailyShadowStageReceiptSigner:
    raw_socket_endpoint = os.environ.get("RQ_DAILY_SHADOW_RECEIPT_SIGNER_SOCKET_ENDPOINT")
    if raw_socket_endpoint is not None:
        socket_endpoint = Path(raw_socket_endpoint.strip())
        test_mode = os.environ.get("RQ_DAILY_SHADOW_RECEIPT_SIGNER_TEST_MODE") == "1"
        if not test_mode and socket_endpoint != DAILY_RECEIPT_SOCKET_ENDPOINT:
            raise RuntimeError("daily shadow receipt signer socket endpoint is invalid")
        raw_keyring = _required_environment("RQ_DAILY_SHADOW_RECEIPT_TRUSTED_KEYRING_PATH")
        keyring_path = Path(raw_keyring)
        if not test_mode and keyring_path != DAILY_RECEIPT_TRUSTED_KEYRING_PATH:
            raise RuntimeError("daily shadow receipt trusted keyring path is invalid")
        try:
            timeout_seconds = float(
                _required_environment("RQ_DAILY_SHADOW_RECEIPT_SIGNER_TIMEOUT_SECONDS")
            )
        except ValueError as exc:
            raise RuntimeError("daily shadow receipt signer timeout is invalid") from exc
        if test_mode:
            return _daily_test_receipt_signer_from_environment(
                socket_endpoint=socket_endpoint,
                timeout_seconds=timeout_seconds,
            )
        keyring = load_daily_receipt_trusted_keyring(keyring_path)
        return Ed25519DailyShadowStageReceiptSigner(
            key_id=keyring.active_key_id,
            client=DailyReceiptSocketSigningClient.production(
                active_key_id=keyring.active_key_id,
                active_public_key_pem=keyring.active_public_key_pem,
                previous_public_key_pems=keyring.previous_public_key_pems,
                timeout_seconds=timeout_seconds,
            ),
        )

    if "RQ_DAILY_SHADOW_RECEIPT_SIGNER_COMMAND" in os.environ:
        raise RuntimeError("daily shadow receipt signer command fallback is not allowed")
    # Forging the offline-test environment on a production host must not buy a signer:
    # production always carries the fixed socket endpoint and the root-owned keyring.
    if _daily_production_signer_host():
        raise RuntimeError(
            "daily shadow offline test receipt signer is not allowed on a production host"
        )
    raw_command = _required_environment("RQ_DAILY_SHADOW_OFFLINE_TEST_RECEIPT_SIGNER_COMMAND")
    try:
        decoded = json.loads(raw_command)
    except json.JSONDecodeError as exc:
        raise RuntimeError("daily shadow receipt signer command is invalid") from exc
    if (
        not isinstance(decoded, list)
        or not decoded
        or any(not isinstance(item, str) or not item for item in decoded)
    ):
        raise RuntimeError("daily shadow receipt signer command is invalid")
    key_id = _required_environment("RQ_DAILY_SHADOW_RECEIPT_SIGNER_KEY_ID")
    try:
        timeout_seconds = float(
            _required_environment("RQ_DAILY_SHADOW_RECEIPT_SIGNER_TIMEOUT_SECONDS")
        )
    except ValueError as exc:
        raise RuntimeError("daily shadow receipt signer timeout is invalid") from exc
    return Ed25519DailyShadowStageReceiptSigner(
        key_id=key_id,
        client=SecureDailyShadowStageReceiptSigningClient(
            command=tuple(decoded),
            key_id=key_id,
            timeout_seconds=timeout_seconds,
        ),
    )


def run_daily_shadow_stage_adapter_from_environment(
    stage_id: str,
    *,
    issued_at: datetime | None = None,
) -> DailyShadowStageCompletionReceipt:
    if stage_id not in _SHADOW_STAGE_ACTIONS:
        raise RuntimeError("daily shadow stage adapter is not allowlisted")
    if _required_environment("RQUANT_DAILY_STAGE_ID") != stage_id:
        raise RuntimeError("daily shadow stage adapter identity mismatch")
    if os.environ.get("RQ_DAILY_SHADOW_ADAPTER_STAGE", "").strip() != stage_id:
        raise RuntimeError("daily shadow stage adapter profile binding is missing")
    expected_identity = _shadow_stage_command_identity(stage_id)
    if os.environ.get("RQ_DAILY_SHADOW_ADAPTER_IDENTITY", "").strip() != expected_identity:
        raise RuntimeError("daily shadow stage adapter profile identity mismatch")
    mode = DailyPipelineMode(_required_environment("RQUANT_DAILY_RUN_MODE"))
    if mode is not DailyPipelineMode.SHADOW:
        raise RuntimeError("daily shadow stage adapter only accepts shadow mode")
    profile_hash = _required_environment("RQUANT_DAILY_PROFILE_HASH")
    storage_profile = DailyPipelineStorageProfile.create(
        root=Path(_required_environment("RQUANT_DAILY_STORAGE_ROOT")),
        mode=mode,
        profile_hash=profile_hash,
    )
    namespace = _required_environment("RQUANT_DAILY_STORAGE_NAMESPACE_ID")
    if storage_profile.namespace_id != namespace:
        raise RuntimeError("daily shadow stage storage namespace is stale")
    locator = Path(_required_environment("RQUANT_DAILY_EFFECT_RECEIPT_LOCATOR"))
    if not locator.is_absolute() or locator != Path(os.path.abspath(locator)):
        raise RuntimeError("daily shadow stage receipt path must be absolute")
    if locator.parent != storage_profile.receipt_root:
        raise RuntimeError("daily shadow stage receipt escapes its storage profile")
    values = {
        "mode": mode,
        "run_id": _required_environment("RQUANT_DAILY_RUN_ID"),
        "stage_id": stage_id,
        "effect_id": _required_environment("RQUANT_DAILY_EFFECT_ID"),
        "idempotency_key": _required_environment("RQUANT_DAILY_EFFECT_IDEMPOTENCY_KEY"),
        "input_identity": _required_environment("RQUANT_DAILY_INPUT_IDENTITY"),
        "source_generation_id": _required_environment("RQUANT_DAILY_SOURCE_GENERATION_ID"),
        "source_content_hash": _required_environment("RQUANT_DAILY_SOURCE_CONTENT_HASH"),
        "command_manifest_hash": _required_environment("RQUANT_DAILY_COMMAND_MANIFEST_HASH"),
        "profile_hash": profile_hash,
        "command_identity": expected_identity,
        "stage_order": _shadow_stage_order(stage_id),
        "exit_status": 0,
    }
    dependency_receipt_ids = _dependency_receipt_ids_from_environment()
    publication = _publish_stage_artifact(
        storage_profile=storage_profile,
        stage_id=stage_id,
        values=values,
        dependency_receipt_ids=dependency_receipt_ids,
    )
    values["output_identity"] = publication.output_identity
    artifact_path = _shadow_effect_artifact_path(
        storage_profile,
        stage_id=stage_id,
        effect_id=str(values["effect_id"]),
    )
    try:
        artifact = _load_shadow_stage_artifact(artifact_path)
    except FileNotFoundError:
        artifact = DailyShadowStageArtifact(
            **values,
            action=_SHADOW_STAGE_ACTIONS[stage_id],
            generation_id=publication.generation_id,
            output_files=publication.output_files,
            dependency_receipt_ids=dependency_receipt_ids,
            artifact_path=artifact_path,
            created_at=issued_at or datetime.now(UTC),
        )
        _safe_write_once(
            artifact_path,
            canonical_json_bytes(artifact.model_dump(mode="json")),
            label="daily shadow stage artifact",
        )
    expected_artifact = DailyShadowStageArtifact(
        **values,
        action=_SHADOW_STAGE_ACTIONS[stage_id],
        generation_id=publication.generation_id,
        output_files=publication.output_files,
        dependency_receipt_ids=dependency_receipt_ids,
        artifact_path=artifact_path,
        created_at=artifact.created_at,
    )
    if artifact.model_dump(mode="python", exclude={"created_at"}) != expected_artifact.model_dump(
        mode="python",
        exclude={"created_at"},
    ):
        raise RuntimeError("daily shadow stage artifact identity mismatch")
    result = _shadow_stage_result(artifact=artifact)
    receipt_issued_at = (issued_at or datetime.now(UTC)).astimezone(UTC)
    receipt = issue_daily_shadow_stage_completion_receipt(
        signer=_daily_receipt_signer_from_environment(),
        mode=values["mode"],
        run_id=str(values["run_id"]),
        stage_id=str(values["stage_id"]),
        effect_id=str(values["effect_id"]),
        idempotency_key=str(values["idempotency_key"]),
        input_identity=str(values["input_identity"]),
        source_generation_id=str(values["source_generation_id"]),
        source_content_hash=str(values["source_content_hash"]),
        command_manifest_hash=str(values["command_manifest_hash"]),
        profile_hash=str(values["profile_hash"]),
        command_identity=str(values["command_identity"]),
        stage_order=int(values["stage_order"]),
        exit_status=int(values["exit_status"]),
        output_identity=str(values["output_identity"]),
        dependency_receipt_ids=dependency_receipt_ids,
        result=result,
        issued_at=receipt_issued_at,
    )
    _safe_write_once(
        locator,
        canonical_json_bytes(receipt.model_dump(mode="json")),
        label="daily shadow stage completion receipt",
    )
    return receipt


def build_daily_shadow_stage_commands(
    *,
    python_executable: Path,
    working_directory: Path | None = None,
    environment: Mapping[str, str] | None = None,
) -> tuple[DailyShadowStageCommandSettings, ...]:
    executable = Path(os.path.abspath(python_executable))
    if not executable.is_absolute():
        raise ValueError("daily shadow stage executable must be absolute")
    resolved_working_directory: str | None = None
    if working_directory is not None:
        resolved = Path(os.path.abspath(working_directory))
        if not resolved.is_absolute():
            raise ValueError("daily shadow stage working directory must be absolute")
        resolved_working_directory = str(resolved)
    normalized_environment = dict(sorted((environment or {}).items()))
    return tuple(
        DailyShadowStageCommandSettings(
            stage_id=stage_id,
            adapter_identity=_shadow_stage_command_identity(stage_id),
            argv=(
                str(executable),
                "-m",
                "rquant.runtime_builder_daily_orchestrator",
                _SHADOW_STAGE_ADAPTER_FLAG,
                stage_id,
            ),
            environment=normalized_environment,
            working_directory=resolved_working_directory,
        )
        for stage_id in _FAN_IN_STAGES
    )


class DailyShadowStageCommandAdapter:
    def __init__(
        self,
        *,
        command: DailyShadowStageCommandSettings,
        storage_profile: DailyPipelineStorageProfile,
        command_manifest_hash: str,
        receipt_keyring: Ed25519DailyShadowStageReceiptKeyring,
        receipt_signer_command: tuple[str, ...],
        receipt_signer_socket_endpoint: Path | None = None,
        receipt_trusted_keyring_path: Path | None = None,
        receipt_signer_key_id: str,
        receipt_signer_timeout_seconds: float,
        receipt_signer_test_mode: bool = False,
        receipt_signer_test_public_key_pem: str | None = None,
    ) -> None:
        self._command = DailyShadowStageCommandSettings.model_validate(command)
        self._storage_profile = DailyPipelineStorageProfile.model_validate(storage_profile)
        self._command_manifest_hash = command_manifest_hash
        self._receipt_keyring = receipt_keyring
        self._receipt_signer_command = tuple(receipt_signer_command)
        self._receipt_signer_socket_endpoint = receipt_signer_socket_endpoint
        self._receipt_trusted_keyring_path = receipt_trusted_keyring_path
        self._receipt_signer_key_id = receipt_signer_key_id
        self._receipt_signer_timeout_seconds = receipt_signer_timeout_seconds
        self._receipt_signer_test_mode = receipt_signer_test_mode
        self._receipt_signer_test_public_key_pem = receipt_signer_test_public_key_pem
        self.stage_id = self._command.stage_id

    def health(self, _context) -> object:
        from rquant.daily_pipeline_orchestrator import DailyStageHealth

        return DailyStageHealth(
            ready=True,
            detail="daily_shadow_stage_command",
            estimated_memory_mb=self._command.estimated_memory_mb,
            estimated_io_bytes=self._command.estimated_io_bytes,
        )

    def prepare(self, context) -> object:
        from rquant.daily_pipeline_ledger import DailyStageEffectIntent

        idempotency_key = canonical_sha256(
            {
                "contract": "daily-stage-idempotency-key/v3",
                "mode": context.run.spec.mode,
                "run_id": context.run.run_id,
                "stage_id": context.attempt.stage_id,
                "input_identity": context.run.input_identity,
                "command_manifest_hash": context.run.spec.command_manifest_hash,
            }
        )
        effect_id = canonical_sha256(
            {
                "contract": "daily-shadow-stage-effect/v1",
                "stage_id": context.attempt.stage_id,
                "idempotency_key": idempotency_key,
                "adapter_identity": self._command.adapter_identity,
                "command_manifest_hash": self._command_manifest_hash,
                "receipt_root": str(self._storage_profile.receipt_root),
            }
        )
        return DailyStageEffectIntent(
            mode=self._storage_profile.mode,
            idempotency_key=idempotency_key,
            command_manifest_hash=self._command_manifest_hash,
            adapter_identity=self._command.adapter_identity,
            receipt_locator=str(self._storage_profile.receipt_root / f"{effect_id}.json"),
        )

    def command(self, _context, _effect, /) -> object:
        from rquant.daily_pipeline_orchestrator import DailyStageProcessSpec

        dependency_receipt_ids = tuple(
            receipt.receipt_id for receipt in _context.dependency_receipts
        )
        environment = {
            **self._command.environment,
            "RQ_DAILY_SHADOW_ADAPTER_IDENTITY": self._command.adapter_identity,
            "RQ_DAILY_SHADOW_ADAPTER_STAGE": self._command.stage_id,
            "RQ_DAILY_SHADOW_DEPENDENCY_RECEIPT_IDS": canonical_json_bytes(
                dependency_receipt_ids
            ).decode("utf-8"),
            "RQ_DAILY_SHADOW_RECEIPT_SIGNER_KEY_ID": self._receipt_signer_key_id,
            "RQ_DAILY_SHADOW_RECEIPT_SIGNER_TIMEOUT_SECONDS": str(
                self._receipt_signer_timeout_seconds
            ),
        }
        if self._receipt_signer_socket_endpoint is not None:
            if self._receipt_trusted_keyring_path is None:
                raise RuntimeError("daily shadow receipt trusted keyring path is missing")
            if self._receipt_signer_test_mode and not self._receipt_signer_test_public_key_pem:
                raise RuntimeError("daily shadow test receipt signer public key is missing")
            environment.update(
                {
                    "RQ_DAILY_SHADOW_RECEIPT_SIGNER_SOCKET_ENDPOINT": str(
                        self._receipt_signer_socket_endpoint
                    ),
                    "RQ_DAILY_SHADOW_RECEIPT_TRUSTED_KEYRING_PATH": str(
                        self._receipt_trusted_keyring_path
                    ),
                    "RQ_DAILY_SHADOW_RECEIPT_SIGNER_TEST_MODE": (
                        "1" if self._receipt_signer_test_mode else "0"
                    ),
                }
            )
            if self._receipt_signer_test_mode:
                environment["RQ_DAILY_SHADOW_RECEIPT_SIGNER_TEST_PUBLIC_KEY_PEM"] = (
                    self._receipt_signer_test_public_key_pem
                )
        else:
            environment.update(
                {
                    "RQ_DAILY_SHADOW_OFFLINE_TEST_RECEIPT_SIGNER_COMMAND": json.dumps(
                        self._receipt_signer_command,
                        separators=(",", ":"),
                    ),
                }
            )
        return DailyStageProcessSpec(
            argv=self._command.argv,
            environment=environment,
            working_directory=self._command.working_directory,
        )

    def reconcile(self, context, effect, /) -> object | None:
        try:
            payload = _read_bounded_regular(
                Path(effect.receipt_locator),
                label="daily shadow stage completion receipt",
            )
        except FileNotFoundError:
            return None
        receipt = DailyShadowStageCompletionReceipt.model_validate(strict_json_loads(payload))
        expected_values = {
            "mode": context.run.spec.mode,
            "run_id": context.run.run_id,
            "stage_id": context.attempt.stage_id,
            "effect_id": effect.effect_id,
            "idempotency_key": effect.idempotency_key,
            "input_identity": context.run.input_identity,
            "source_generation_id": context.run.spec.source_generation_id,
            "source_content_hash": context.run.spec.source_content_hash,
            "command_manifest_hash": context.run.spec.command_manifest_hash,
            "profile_hash": context.run.spec.profile_hash,
            "command_identity": self._command.adapter_identity,
            "stage_order": _shadow_stage_order(context.attempt.stage_id),
            "exit_status": 0,
            "dependency_receipt_ids": tuple(
                item.receipt_id for item in context.dependency_receipts
            ),
            "previous_receipt_hash": _previous_receipt_hash(
                tuple(item.receipt_id for item in context.dependency_receipts)
            ),
            "key_id": self._receipt_keyring.active_key_id,
            "signature_algorithm": "ed25519",
        }
        observed_values = {key: getattr(receipt, key) for key in expected_values}
        if observed_values != expected_values:
            raise ValueError("daily shadow stage completion receipt identity mismatch")
        if not self._receipt_keyring.verify_stage_receipt(receipt, require_active=True):
            raise ValueError("daily shadow stage completion receipt signature verification failed")
        artifact_path = _shadow_effect_artifact_path(
            self._storage_profile,
            stage_id=context.attempt.stage_id,
            effect_id=effect.effect_id,
        )
        artifact = _load_shadow_stage_artifact(artifact_path)
        if artifact.dependency_receipt_ids != tuple(
            item.receipt_id for item in context.dependency_receipts
        ):
            raise ValueError("daily shadow stage artifact dependency mismatch")
        _verify_artifact_outputs(artifact, storage_profile=self._storage_profile)
        if receipt.output_identity != artifact.output_identity:
            raise ValueError("daily shadow stage completion receipt output identity mismatch")
        artifact_values = {
            "mode": artifact.mode,
            "run_id": artifact.run_id,
            "stage_id": artifact.stage_id,
            "effect_id": artifact.effect_id,
            "idempotency_key": artifact.idempotency_key,
            "input_identity": artifact.input_identity,
            "source_generation_id": artifact.source_generation_id,
            "source_content_hash": artifact.source_content_hash,
            "command_manifest_hash": artifact.command_manifest_hash,
            "profile_hash": artifact.profile_hash,
            "command_identity": artifact.command_identity,
            "stage_order": artifact.stage_order,
            "exit_status": artifact.exit_status,
            "output_identity": artifact.output_identity,
            "generation_id": artifact.generation_id,
            "output_files": artifact.output_files,
            "dependency_receipt_ids": artifact.dependency_receipt_ids,
            "artifact_path": artifact.artifact_path,
        }
        expected_artifact_values = {
            "mode": context.run.spec.mode,
            "run_id": context.run.run_id,
            "stage_id": context.attempt.stage_id,
            "effect_id": effect.effect_id,
            "idempotency_key": effect.idempotency_key,
            "input_identity": context.run.input_identity,
            "source_generation_id": context.run.spec.source_generation_id,
            "source_content_hash": context.run.spec.source_content_hash,
            "command_manifest_hash": context.run.spec.command_manifest_hash,
            "profile_hash": context.run.spec.profile_hash,
            "command_identity": self._command.adapter_identity,
            "stage_order": _shadow_stage_order(context.attempt.stage_id),
            "exit_status": 0,
            "output_identity": artifact.output_identity,
            "generation_id": artifact.generation_id,
            "output_files": artifact.output_files,
            "dependency_receipt_ids": tuple(
                item.receipt_id for item in context.dependency_receipts
            ),
            "artifact_path": artifact_path,
        }
        if artifact_values != expected_artifact_values:
            raise ValueError("daily shadow stage artifact identity mismatch")
        expected_result = _shadow_stage_result(artifact=artifact)
        if receipt.result != expected_result:
            raise ValueError("daily shadow stage completion receipt result mismatch")
        return receipt.result


def _command_manifest_payload(
    *,
    settings: DailyPipelineOrchestratorSettings,
    storage_profile: DailyPipelineStorageProfile,
) -> dict[str, object]:
    return {
        "contract": "daily-shadow-command-manifest/v1",
        "mode": settings.mode,
        "storage_profile": storage_profile.model_dump(mode="json"),
        "stages": [command.model_dump(mode="json") for command in settings.stage_commands],
    }


def _persist_command_manifest(
    *,
    settings: DailyPipelineOrchestratorSettings,
    storage_profile: DailyPipelineStorageProfile,
) -> str:
    from rquant.daily_pipeline_ledger import DailyPipelineStorageBinding

    payload = _command_manifest_payload(settings=settings, storage_profile=storage_profile)
    command_manifest_hash = canonical_sha256(payload)
    DailyPipelineStorageBinding.open(storage_profile, leaf="control").close()
    _safe_write_once(
        storage_profile.command_manifest_path,
        canonical_json_bytes(payload),
        label="daily shadow command manifest",
    )
    observed_payload = strict_json_loads(storage_profile.command_manifest_path.read_bytes())
    observed_hash = canonical_sha256(observed_payload)
    if observed_hash != command_manifest_hash:
        raise ValueError("daily shadow command manifest failed verification")
    return command_manifest_hash


def _current_daily_close_source(spool_root: Path) -> _DailyCloseSourceIdentity:
    from rquant.daily_close_gateway import DailyCloseGateway
    from rquant.live_contracts import LiveChannel
    from rquant.live_spool import LiveBatchSpool

    spool = LiveBatchSpool(spool_root, read_only=True, source_read_only=True)
    pointer = spool.current(LiveChannel.DAILY_CLOSE)
    if pointer is None:
        raise ValueError("daily orchestrator daily-close source pointer is missing")
    records = tuple(
        record
        for record in spool.list_after(
            LiveChannel.DAILY_CLOSE,
            sequence=pointer.sequence - 1,
            limit=1,
        )
        if record.envelope.sequence == pointer.sequence
    )
    if len(records) != 1:
        raise ValueError("daily orchestrator daily-close source record is missing")
    record = records[0]
    payload = DailyCloseGateway.decode_payload(spool.read_payload(record))
    return _DailyCloseSourceIdentity(
        trade_date=payload.source_request.trade_date,
        source_generation_id=pointer.source_generation_id,
        source_content_hash=payload.content_sha256,
    )


def _profile_manifest(
    profile: object,
    manifest: RuntimeServiceManifest,
) -> RuntimeServiceManifest:
    from rquant.runtime_deployment_profile import RuntimeDeploymentProfile

    if not isinstance(profile, RuntimeDeploymentProfile):
        raise TypeError("daily orchestrator profile type is invalid")
    matches = tuple(item for item in profile.manifests if item.service_id == manifest.service_id)
    if len(matches) != 1:
        raise ValueError("daily orchestrator profile authority is missing")
    candidate = matches[0]
    if candidate.manifest_fingerprint != manifest.manifest_fingerprint:
        raise ValueError("daily orchestrator manifest differs from profile authority")
    return candidate


def daily_pipeline_orchestrator_builder(
    *,
    clock: Callable[[], datetime],
) -> RuntimeServiceBuilder:
    """Build a Shadow DAG coordinator from immutable profile data."""

    def build(manifest: RuntimeServiceManifest) -> RuntimeServiceStep:
        if manifest.service_kind is not RuntimeServiceKind.DAILY_PIPELINE_ORCHESTRATOR:
            raise ValueError("runtime service kind must be daily_pipeline_orchestrator")
        if manifest.plane is not RuntimeServicePlane.RESEARCH:
            raise ValueError("daily orchestrator must run on the research plane")
        settings = DailyPipelineOrchestratorSettings.model_validate(dict(manifest.settings))
        from rquant.runtime_deployment_profile import RuntimeDeploymentProfile

        profile = _load_profile(settings.deployment_profile_path)
        if not isinstance(profile, RuntimeDeploymentProfile):
            raise TypeError("daily orchestrator profile type is invalid")
        profile_manifest = _profile_manifest(profile, manifest)
        if profile.producer_commit != manifest.producer_commit or profile.profile_id is None:
            raise ValueError("daily orchestrator profile identity does not match the manifest")
        if settings.service_owner != manifest.service_id:
            raise ValueError("daily orchestrator service owner must equal its profile authority")
        if profile.production_runtime_root is None:
            raise ValueError("daily orchestrator requires an immutable production profile root")
        production_root = Path(profile.production_runtime_root)
        if settings.deployment_profile_path != (
            production_root / "current" / "deployment-profile.json"
        ):
            raise ValueError("daily orchestrator profile path differs from trusted root")
        if settings.storage_root != production_root / "research" / "daily-pipeline":
            raise ValueError("daily orchestrator storage root differs from trusted root")
        daily_sources = tuple(
            item
            for item in profile.manifests
            if item.service_kind is RuntimeServiceKind.DAILY_CLOSE_SOURCE
        )
        if len(daily_sources) != 1:
            raise ValueError("daily orchestrator requires one daily-close source authority")
        if settings.source_spool_root != Path(str(daily_sources[0].settings.get("spool_root"))):
            raise ValueError("daily orchestrator source spool differs from the immutable profile")

        settings = DailyPipelineOrchestratorSettings.model_validate(dict(profile_manifest.settings))
        settings.storage_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        storage = DailyPipelineStorageProfile.create(
            root=settings.storage_root,
            mode=DailyPipelineMode.SHADOW,
            profile_hash=profile.profile_id,
        )
        receipt_keyring = daily_shadow_stage_receipt_keyring(settings)
        command_manifest_hash = _persist_command_manifest(
            settings=settings,
            storage_profile=storage,
        )

        def step() -> RuntimeStepResult:
            from rquant.daily_pipeline_orchestrator import (
                DEFAULT_DAILY_CLOSE_PIPELINE,
                DailyCloseSpoolSourceResolver,
                DailyPipelineOrchestrator,
            )
            from rquant.live_spool import LiveBatchSpool

            observed = clock()
            source = _current_daily_close_source(settings.source_spool_root)
            ledger_owner = daily_shadow_ledger_owner(settings.service_owner)
            ledger = DailyPipelineLedger(
                storage_profile=storage,
                service_owner=ledger_owner,
            )
            orchestrator = DailyPipelineOrchestrator(
                ledger=ledger,
                service_owner=ledger_owner,
                definition=DEFAULT_DAILY_CLOSE_PIPELINE,
                adapters=tuple(
                    DailyShadowStageCommandAdapter(
                        command=command,
                        storage_profile=storage,
                        command_manifest_hash=command_manifest_hash,
                        receipt_keyring=receipt_keyring,
                        receipt_signer_command=(settings.offline_test_receipt_signer_command or ()),
                        receipt_signer_socket_endpoint=settings.receipt_signer_socket_endpoint,
                        receipt_trusted_keyring_path=settings.receipt_trusted_keyring_path,
                        receipt_signer_key_id=settings.receipt_active_key_id,
                        receipt_signer_timeout_seconds=settings.receipt_signer_timeout_seconds,
                        receipt_signer_test_mode=settings.receipt_signer_test_mode,
                        receipt_signer_test_public_key_pem=(
                            settings.receipt_active_public_key_pem
                            if settings.receipt_signer_test_mode
                            else None
                        ),
                    )
                    for command in settings.stage_commands
                ),
                source_resolver=DailyCloseSpoolSourceResolver(
                    LiveBatchSpool(
                        settings.source_spool_root,
                        read_only=True,
                        source_read_only=True,
                    )
                ),
                clock=clock,
            )
            run = orchestrator.create_run(
                mode=DailyPipelineMode.SHADOW,
                trade_date=source.trade_date,
                source_generation_id=source.source_generation_id,
                source_content_hash=source.source_content_hash,
                command_manifest_hash=command_manifest_hash,
                code_commit=manifest.producer_commit,
                profile_hash=profile.profile_id,
                now=observed,
            )
            orchestrator.recover(run_id=run.run_id, now=clock())
            while orchestrator.advance(run.run_id, now=clock()) is not None:
                pass
            status = orchestrator.status(run.run_id)
            if status.state is not DailyRunState.SUCCEEDED:
                raise RuntimeError("daily orchestrator shadow DAG did not complete")
            completed = {
                stage_id
                for stage_id, state in status.stage_states.items()
                if state is DailyStageState.SUCCEEDED
            }
            final_stage = ledger.stage(run.run_id, "backup")
            if final_stage.terminal_receipt_id is None:
                raise RuntimeError("daily orchestrator backup stage receipt is missing")
            final_receipt = ledger.receipt(final_stage.terminal_receipt_id)
            if final_receipt is None or final_receipt.stage_id != "backup":
                raise RuntimeError("daily orchestrator completion receipt failed verification")
            stage_receipts: list[DailyShadowStageCompletionReceipt] = []
            for stage_id in _FAN_IN_STAGES:
                effect = ledger.effect_for_stage(run.run_id, stage_id)
                if effect is None:
                    raise RuntimeError("daily orchestrator stage receipt is missing")
                stage_receipt = DailyShadowStageCompletionReceipt.model_validate(
                    strict_json_loads(Path(effect.intent.receipt_locator).read_bytes())
                )
                if not receipt_keyring.verify_stage_receipt(stage_receipt, require_active=True):
                    raise RuntimeError("daily orchestrator stage receipt failed verification")
                stage_receipts.append(stage_receipt)
            run_completion_receipt = issue_daily_shadow_run_completion_receipt(
                signer=daily_shadow_stage_receipt_signer(settings),
                run=run.model_copy(update={"state": DailyRunState.SUCCEEDED}),
                stage_receipts=tuple(stage_receipts),
                issued_at=stage_receipts[-1].issued_at,
            )
            persist_daily_shadow_run_completion_receipt(storage, run_completion_receipt)
            return RuntimeStepResult(
                processed_count=len(completed),
                source_generations={
                    "daily_pipeline_profile": profile.profile_id,
                    "daily_pipeline_namespace": str(storage.namespace_id),
                    "daily_pipeline_command_manifest": command_manifest_hash,
                    "daily_pipeline_completion_receipt": run_completion_receipt.receipt_id,
                },
            )

        return step

    return build


__all__ = [
    "DailyShadowStageCommandAdapter",
    "DailyShadowStageArtifact",
    "DailyShadowStageCommandSettings",
    "DailyShadowStageCompletionReceipt",
    "DailyShadowRunCompletionReceipt",
    "DailyPipelineOrchestratorSettings",
    "build_daily_shadow_stage_commands",
    "daily_pipeline_orchestrator_builder",
    "daily_shadow_ledger_owner",
    "load_daily_shadow_run_completion_receipt",
    "run_daily_shadow_stage_adapter_from_environment",
]


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) == 2 and arguments[0] == _SHADOW_STAGE_ADAPTER_FLAG:
        run_daily_shadow_stage_adapter_from_environment(arguments[1])
        return 0
    raise SystemExit(
        "usage: python -m rquant.runtime_builder_daily_orchestrator "
        f"{_SHADOW_STAGE_ADAPTER_FLAG} <stage-id>"
    )


if __name__ == "__main__":
    raise SystemExit(main())
