"""Least-privilege client for the root-owned systemd credential sealer."""

from __future__ import annotations

import base64
import re
import subprocess
from collections.abc import Mapping
from types import MappingProxyType
from typing import Literal

from pydantic import field_serializer, field_validator

from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

_INSTANCE_PATTERN = re.compile(r"^svc-[0-9a-f]{64}$")
_HELPER = "/usr/local/libexec/rquant-runtime-credential-sealer"


class RuntimeCredentialSealRequest(RuntimeContractModel):
    schema_version: Literal[1] = 1
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
    sealed_instances: tuple[str, ...]

    @field_validator("sealed_instances")
    @classmethod
    def validate_instances(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(_INSTANCE_PATTERN.fullmatch(name) is None for name in value):
            raise ValueError("credential seal receipt contains an invalid instance")
        return tuple(sorted(value))


def seal_runtime_credentials(credentials: Mapping[str, bytes]) -> None:
    if not isinstance(credentials, Mapping) or not credentials:
        raise ValueError("credentials must be a non-empty mapping")
    request = RuntimeCredentialSealRequest(
        credentials={
            name: base64.b64encode(payload).decode("ascii")
            for name, payload in sorted(credentials.items())
        }
    )
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
    try:
        receipt = strict_model_validate_json(RuntimeCredentialSealReceipt, result.stdout)
    except ValueError as exc:
        raise RuntimeError("root runtime credential sealer returned an invalid receipt") from exc
    if set(receipt.sealed_instances) != set(credentials):
        raise RuntimeError("root runtime credential sealer receipt is incomplete")


__all__ = [
    "RuntimeCredentialSealReceipt",
    "RuntimeCredentialSealRequest",
    "seal_runtime_credentials",
]
