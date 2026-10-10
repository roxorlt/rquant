"""Least-privilege secret capabilities for isolated runtime services."""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType
from typing import Self

from pydantic import Field, field_validator, model_validator

from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


class SecretScopeError(RuntimeError):
    pass


class ServiceSecretPolicy(RuntimeContractModel):
    service_id: str = Field(min_length=1)
    allowed_keys: tuple[str, ...] = ()
    required_keys: tuple[str, ...] = ()

    @field_validator("allowed_keys", "required_keys")
    @classmethod
    def canonicalize_keys(cls, values: tuple[str, ...]) -> tuple[str, ...]:
        if any(not value or not value.isupper() for value in values):
            raise ValueError("secret keys must be nonempty uppercase names")
        if len(values) != len(set(values)):
            raise ValueError("secret keys must be unique")
        return tuple(sorted(values))

    @model_validator(mode="after")
    def validate_required_keys(self) -> Self:
        if not set(self.required_keys).issubset(self.allowed_keys):
            raise ValueError("required secret keys must be allowed")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self)


KNOWN_SECRET_KEYS = frozenset(
    {
        "DEEPSEEK_API_KEY",
        "PUSHDEER_KEYS",
        "PUSHPLUS_TOKENS",
        "RQUANT_BACKUP_TOKEN",
        "RQUANT_CLOUD_FEED_PASS",
        "RQUANT_PANORAMA_COOKIE_SECRET",
        "RQUANT_UPLOAD_TOKEN",
        "TUSHARE_COOKIE",
        "TUSHARE_LOGIN_PASSWORD",
        "TUSHARE_LOGIN_USERNAME",
        "TUSHARE_TOKEN_BACKUP",
        "TUSHARE_TOKEN_MAIN",
    }
)
_SECRET_LIKE = re.compile(r"(?:^|_)(?:API_?KEY|COOKIE|KEYS?|PASSWORD|SECRET|TOKENS?)(?:_|$)")


SOURCE_GATEWAY_SECRET_POLICY = ServiceSecretPolicy(
    service_id="source-gateway",
    allowed_keys=("TUSHARE_TOKEN_MAIN", "TUSHARE_TOKEN_BACKUP"),
    required_keys=("TUSHARE_TOKEN_MAIN",),
)
NOTIFIER_SECRET_POLICY = ServiceSecretPolicy(
    service_id="notifier",
    allowed_keys=("PUSHDEER_KEYS", "PUSHPLUS_TOKENS"),
)
STRATEGY_SECRET_POLICY = ServiceSecretPolicy(service_id="strategy-runner")
PAPER_SECRET_POLICY = ServiceSecretPolicy(service_id="paper-broker")
SERVING_SECRET_POLICY = ServiceSecretPolicy(service_id="serving-publisher")
RESEARCH_WORKER_SECRET_POLICY = ServiceSecretPolicy(service_id="research-worker")


class ScopedSecrets:
    """Non-serializable in-memory capability; repr and audit data are value-free."""

    __slots__ = ("_values", "policy")

    def __init__(self, policy: ServiceSecretPolicy, values: Mapping[str, str]) -> None:
        self.policy = policy
        self._values = MappingProxyType(dict(sorted(values.items())))

    @property
    def policy_fingerprint(self) -> str:
        return self.policy.fingerprint

    @property
    def present_keys(self) -> tuple[str, ...]:
        return tuple(self._values)

    def reveal(self, key: str) -> str:
        if key not in self.policy.allowed_keys or key not in self._values:
            raise KeyError(key)
        return self._values[key]

    def audit_payload(self) -> dict[str, object]:
        return {
            "service_id": self.policy.service_id,
            "policy_fingerprint": self.policy_fingerprint,
            "allowed_keys": self.policy.allowed_keys,
            "required_keys": self.policy.required_keys,
            "present_keys": self.present_keys,
        }

    def __repr__(self) -> str:
        return (
            f"ScopedSecrets(service_id={self.policy.service_id!r}, "
            f"present_keys={self.present_keys!r}, values=<redacted>)"
        )


def load_scoped_secrets(
    policy: ServiceSecretPolicy,
    environment: Mapping[str, str],
) -> ScopedSecrets:
    unknown = sorted(
        key
        for key in environment
        if _SECRET_LIKE.search(key) is not None and key not in KNOWN_SECRET_KEYS
    )
    if unknown:
        raise SecretScopeError(f"unregistered secret-like keys: {', '.join(unknown)}")

    values = {
        key: environment[key].strip()
        for key in policy.allowed_keys
        if key in environment and environment[key].strip()
    }
    missing = sorted(set(policy.required_keys) - set(values))
    if missing:
        raise SecretScopeError(f"missing required secrets: {', '.join(missing)}")
    return ScopedSecrets(policy, values)


__all__ = [
    "KNOWN_SECRET_KEYS",
    "NOTIFIER_SECRET_POLICY",
    "PAPER_SECRET_POLICY",
    "RESEARCH_WORKER_SECRET_POLICY",
    "SERVING_SECRET_POLICY",
    "SOURCE_GATEWAY_SECRET_POLICY",
    "STRATEGY_SECRET_POLICY",
    "ScopedSecrets",
    "SecretScopeError",
    "ServiceSecretPolicy",
    "load_scoped_secrets",
]
