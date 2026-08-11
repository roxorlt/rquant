"""Least-privilege client for the root-owned systemd credential sealer."""

from __future__ import annotations

import base64
import re
import subprocess
from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal

from pydantic import field_serializer, field_validator, model_validator

from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

_INSTANCE_PATTERN = re.compile(r"^svc-[0-9a-f]{64}$")
_HELPER = "/usr/local/libexec/rquant-runtime-credential-sealer"


class RuntimeCredentialSealRequest(RuntimeContractModel):
    schema_version: Literal[2] = 2
    operation: Literal["begin"] = "begin"
    credentials: Mapping[str, str]

    @field_validator("credentials")
    @classmethod
    def validate_credentials(cls, value: Mapping[str, str]) -> Mapping[str, str]:
        if not value:
            raise ValueError("credential seal request cannot be empty")
        if any(_INSTANCE_PATTERN.fullmatch(name) is None for name in value):
            raise ValueError("credential seal request contains an invalid instance")
        return MappingProxyType(dict(sorted(value.items())))

    @field_serializer("credentials")
    def serialize_credentials(self, value: Mapping[str, str]) -> dict[str, str]:
        return dict(value)


class RuntimeCredentialSealReceipt(RuntimeContractModel):
    schema_version: Literal[2] = 2
    operation: Literal["begin", "commit", "rollback"]
    transaction_id: str
    sealed_instances: tuple[str, ...]

    @field_validator("transaction_id")
    @classmethod
    def validate_transaction_id(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("credential seal receipt contains an invalid transaction id")
        return value

    @field_validator("sealed_instances")
    @classmethod
    def validate_instances(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(_INSTANCE_PATTERN.fullmatch(name) is None for name in value):
            raise ValueError("credential seal receipt contains an invalid instance")
        return tuple(sorted(value))


class RuntimeCredentialControlRequest(RuntimeContractModel):
    schema_version: Literal[2] = 2
    operation: Literal["commit", "rollback"]
    transaction_id: str

    @field_validator("transaction_id")
    @classmethod
    def validate_transaction_id(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("credential transaction id is invalid")
        return value


class RuntimeCredentialRecoveryRequest(RuntimeContractModel):
    schema_version: Literal[2] = 2
    operation: Literal["recover"] = "recover"
    bundle_generation: str
    instances: tuple[str, ...]
    action: Literal["commit", "rollback"]

    @field_validator("bundle_generation")
    @classmethod
    def validate_generation(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("credential recovery generation is invalid")
        return value

    @field_validator("instances")
    @classmethod
    def validate_instances(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("credential recovery instances must be non-empty and unique")
        if any(_INSTANCE_PATTERN.fullmatch(name) is None for name in value):
            raise ValueError("credential recovery contains an invalid instance")
        return tuple(sorted(value))


class RuntimeCredentialRecoveryReceipt(RuntimeContractModel):
    schema_version: Literal[2] = 2
    operation: Literal["recover"] = "recover"
    bundle_generation: str
    sealed_instances: tuple[str, ...]
    outcome: Literal["none", "committed", "rolled_back"]
    transaction_id: str | None

    @field_validator("bundle_generation")
    @classmethod
    def validate_generation(cls, value: str) -> str:
        if re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("credential recovery receipt generation is invalid")
        return value

    @field_validator("sealed_instances")
    @classmethod
    def validate_instances(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("credential recovery receipt instances are invalid")
        if any(_INSTANCE_PATTERN.fullmatch(name) is None for name in value):
            raise ValueError("credential recovery receipt contains an invalid instance")
        return tuple(sorted(value))

    @field_validator("transaction_id")
    @classmethod
    def validate_transaction_id(cls, value: str | None) -> str | None:
        if value is not None and re.fullmatch(r"[0-9a-f]{64}", value) is None:
            raise ValueError("credential recovery receipt transaction id is invalid")
        return value

    @model_validator(mode="after")
    def validate_outcome_transaction(self) -> RuntimeCredentialRecoveryReceipt:
        if (self.outcome == "none") != (self.transaction_id is None):
            raise ValueError("credential recovery receipt outcome is inconsistent")
        return self


def _run_helper(request: RuntimeContractModel) -> bytes:
    try:
        result = subprocess.run(
            ["/usr/bin/sudo", "-n", _HELPER],
            input=canonical_json_bytes(request.model_dump(mode="json")),
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise RuntimeError("root runtime credential sealer is unavailable") from exc
    if result.returncode != 0 or not result.stdout:
        raise RuntimeError("root runtime credential sealing failed")
    return result.stdout


def _invoke_helper(
    request: RuntimeCredentialSealRequest | RuntimeCredentialControlRequest,
    *,
    expected_instances: tuple[str, ...],
) -> RuntimeCredentialSealReceipt:
    try:
        receipt = strict_model_validate_json(RuntimeCredentialSealReceipt, _run_helper(request))
    except ValueError as exc:
        raise RuntimeError("root runtime credential sealer returned an invalid receipt") from exc
    if receipt.operation != request.operation:
        raise RuntimeError("root runtime credential sealer receipt operation does not match")
    if set(receipt.sealed_instances) != set(expected_instances):
        raise RuntimeError("root runtime credential sealer receipt is incomplete")
    if (
        isinstance(request, RuntimeCredentialControlRequest)
        and receipt.transaction_id != request.transaction_id
    ):
        raise RuntimeError("root runtime credential sealer receipt transaction does not match")
    return receipt


class RuntimeCredentialTransaction:
    def __init__(self, receipt: RuntimeCredentialSealReceipt) -> None:
        self.transaction_id = receipt.transaction_id
        self.sealed_instances = receipt.sealed_instances
        self._state: Literal["active", "committed", "rolled_back"] = "active"

    def commit(self) -> None:
        if self._state == "committed":
            return
        if self._state != "active":
            raise RuntimeError("rolled-back credential transaction cannot be committed")
        _invoke_helper(
            RuntimeCredentialControlRequest(
                operation="commit",
                transaction_id=self.transaction_id,
            ),
            expected_instances=self.sealed_instances,
        )
        self._state = "committed"

    def rollback(self) -> None:
        if self._state == "rolled_back":
            return
        if self._state != "active":
            raise RuntimeError("committed credential transaction cannot be rolled back")
        _invoke_helper(
            RuntimeCredentialControlRequest(
                operation="rollback",
                transaction_id=self.transaction_id,
            ),
            expected_instances=self.sealed_instances,
        )
        self._state = "rolled_back"


def seal_runtime_credentials(
    credentials: Mapping[str, bytes],
) -> RuntimeCredentialTransaction:
    if not isinstance(credentials, Mapping) or not credentials:
        raise ValueError("credentials must be a non-empty mapping")
    request = RuntimeCredentialSealRequest(
        credentials={
            name: base64.b64encode(payload).decode("ascii")
            for name, payload in sorted(credentials.items())
        }
    )
    receipt = _invoke_helper(request, expected_instances=tuple(sorted(credentials)))
    return RuntimeCredentialTransaction(receipt)


def recover_runtime_credentials(
    *,
    bundle_generation: str,
    instances: tuple[str, ...],
    action: Literal["commit", "rollback"],
) -> RuntimeCredentialRecoveryReceipt:
    request = RuntimeCredentialRecoveryRequest(
        bundle_generation=bundle_generation,
        instances=instances,
        action=action,
    )
    try:
        receipt = strict_model_validate_json(
            RuntimeCredentialRecoveryReceipt,
            _run_helper(request),
        )
    except ValueError as exc:
        raise RuntimeError("root runtime credential sealer returned an invalid receipt") from exc
    if receipt.bundle_generation != request.bundle_generation:
        raise RuntimeError("root runtime credential recovery receipt generation does not match")
    if receipt.sealed_instances != request.instances:
        raise RuntimeError("root runtime credential recovery receipt instances do not match")
    expected_outcome = "committed" if action == "commit" else "rolled_back"
    if receipt.outcome not in {"none", expected_outcome}:
        raise RuntimeError("root runtime credential recovery receipt action does not match")
    return receipt


__all__ = [
    "RuntimeCredentialSealReceipt",
    "RuntimeCredentialSealRequest",
    "RuntimeCredentialRecoveryReceipt",
    "RuntimeCredentialRecoveryRequest",
    "RuntimeCredentialTransaction",
    "recover_runtime_credentials",
    "seal_runtime_credentials",
]
