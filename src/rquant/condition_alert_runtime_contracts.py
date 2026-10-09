"""Exact condition events and capabilities for the original alert chain."""

from __future__ import annotations

from datetime import date, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Literal, Self
from weakref import WeakKeyDictionary
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, StrictStr, model_validator

from rquant.delivery_contracts import DeliveryChannel
from rquant.manual_watchlist import OwnerId, TsCode
from rquant.monitor_builtin_contracts import (
    BuiltinConditionAlertEventEnvelope,
    parse_builtin_condition_alert_event,
)
from rquant.price_alert_runtime_contracts import PriceCommit, PriceSha256, _activation_bytes
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads, strict_json_loads


class ConditionRuntimeModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    def wire_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def sha256(self) -> str:
        return sha256(self.wire_bytes()).hexdigest()


class ConditionAlertEventFacts(ConditionRuntimeModel):
    envelope_schema: Literal["rquant.condition-alert-event/v1"] = "rquant.condition-alert-event/v1"
    owner_id: OwnerId
    rule_id: StrictStr = Field(min_length=1, max_length=128)
    rule_name: StrictStr = Field(min_length=1, max_length=80)
    priority: Literal["P0", "P1", "P2", "P3"]
    channels: tuple[DeliveryChannel, ...] = Field(min_length=1, max_length=2)
    rule_version: StrictInt = Field(ge=1)
    rule_body_hash: PriceSha256
    scope_version: PriceSha256
    member_digest: PriceSha256
    ts_code: TsCode
    stock_name: StrictStr = Field(min_length=1, max_length=80)
    trigger_kind: Literal["matched", "recovered"]
    previous_truth: Literal["true", "false", "unknown"]
    truth: Literal["true", "false"]
    source_identity: PriceSha256
    raw_batch_id: PriceSha256
    feature_snapshot_id: PriceSha256
    daily_anchor_date: date
    event_time: AwareUtcDatetime
    decision_time: AwareUtcDatetime
    available_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    evaluation_contract_sha256: PriceSha256
    frequency_policy_sha256: PriceSha256
    frequency_bucket: StrictStr = Field(min_length=1, max_length=80)
    producer_manifest_sha256: PriceSha256
    producer_commit: PriceCommit
    source_epoch: PriceSha256

    @model_validator(mode="after")
    def sealed_visibility(self) -> Self:
        if len(set(self.channels)) != len(self.channels):
            raise ValueError("condition event channels must be unique")
        if not self.event_time <= self.decision_time <= self.available_at < self.expires_at:
            raise ValueError("condition event facts are future or out of order")
        if self.expires_at - self.available_at > timedelta(minutes=15):
            raise ValueError("condition event expiry exceeds its bounded lifetime")
        if (
            self.daily_anchor_date
            >= self.decision_time.astimezone(ZoneInfo("Asia/Shanghai")).date()
        ):
            raise ValueError("condition daily anchor must precede the actual session")
        if (self.trigger_kind == "matched") != (self.truth == "true"):
            raise ValueError("condition trigger differs from the actual truth")
        if self.trigger_kind == "recovered" and self.previous_truth != "true":
            raise ValueError("unknown condition truth cannot produce a recovery")
        return self


class ConditionAlertEventEnvelope(ConditionAlertEventFacts):
    event_id: PriceSha256

    @model_validator(mode="after")
    def sealed_identity(self) -> Self:
        expected = sha256(
            canonical_json_bytes(self.model_dump(mode="json", exclude={"event_id"}))
        ).hexdigest()
        if self.event_id != expected:
            raise ValueError("condition event identity differs from its exact sealed facts")
        return self

    @classmethod
    def create(cls, **facts: object) -> ConditionAlertEventEnvelope:
        validated = ConditionAlertEventFacts(**facts)
        return cls(**validated.model_dump(mode="python"), event_id=validated.sha256)


def parse_condition_alert_event(value: object) -> ConditionAlertEventEnvelope | BuiltinConditionAlertEventEnvelope:
    if type(value) is BuiltinConditionAlertEventEnvelope:
        return parse_builtin_condition_alert_event(value)
    if type(value) is ConditionAlertEventEnvelope:
        payload = value.wire_bytes()
    elif type(value) is bytes:
        payload = value
    elif type(value) is str:
        payload = value.encode()
    else:
        raise TypeError("condition event requires exact event or canonical bytes")
    if len(payload) > 16 * 1024:
        raise ValueError("condition event exceeds its input budget")
    raw = strict_canonical_json_loads(payload)
    if isinstance(raw, dict) and raw.get("envelope_schema") == "rquant.builtin-condition-alert-event/v1":
        return parse_builtin_condition_alert_event(payload)
    event = ConditionAlertEventEnvelope.model_validate_json(payload)
    if event.wire_bytes() != payload:
        raise ValueError("condition event bytes are not canonical")
    return event


class ConditionAlertSourceDescriptor(ConditionRuntimeModel):
    source_id: StrictStr = Field(min_length=1, max_length=128)
    ledger_id: PriceSha256
    source_epoch: PriceSha256
    generation_id: PriceSha256
    producer_manifest_sha256: PriceSha256
    evaluation_contract_sha256: PriceSha256
    frequency_policy_sha256: PriceSha256
    routing_policy_sha256: PriceSha256
    first_sequence: Literal[1] = 1
    high_watermark: StrictInt = Field(ge=0)


class ConditionAlertActivationSettings(ConditionRuntimeModel):
    protocol: Literal["condition-alert-runtime/v1"] = "condition-alert-runtime/v1"
    source_id: StrictStr = Field(min_length=1, max_length=128)
    source_epoch: PriceSha256
    ledger_id: PriceSha256
    generation_id: PriceSha256
    evaluation_contract_sha256: PriceSha256
    frequency_policy_sha256: PriceSha256
    routing_policy_sha256: PriceSha256
    recipient_policy_sha256: PriceSha256
    evaluation_enabled: StrictBool = False
    event_write_enabled: StrictBool = False
    routing_enabled: StrictBool = False
    delivery_enabled: StrictBool = False


class ConditionAlertActivationBinding(ConditionAlertActivationSettings):
    producer_manifest_sha256: PriceSha256
    producer_commit: PriceCommit
    service_kind: Literal[
        "condition_alert_runtime", "price_alert_runtime", "signal_router", "notifier"
    ]


class ConditionAlertRuntimeActivation:
    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("condition activation requires the original owned manifest verifier")


_ACTIVATIONS: WeakKeyDictionary[
    ConditionAlertRuntimeActivation, tuple[ConditionAlertActivationBinding, Path, Path, bytes]
] = WeakKeyDictionary()
_STAGE_ROLES = {
    "evaluation": "condition_alert_runtime",
    "event_write": "condition_alert_runtime",
    "routing": "signal_router",
    "delivery": "notifier",
}


def verify_condition_alert_activation(
    manifest_path: Path,
    *,
    runtime_root: Path,
    expected_manifest_sha256: str,
    expected_commit: str,
    expected_kind: object,
) -> ConditionAlertRuntimeActivation:
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, load_runtime_service_manifest

    if type(expected_kind) is not RuntimeServiceKind or expected_kind.value not in set(
        _STAGE_ROLES.values()
    ) | {"price_alert_runtime"}:
        raise TypeError("condition activation requires the actual original allowlisted role")
    path, root = Path(manifest_path), Path(runtime_root)
    payload = _activation_bytes(path, root)
    if sha256(payload).hexdigest() != expected_manifest_sha256:
        raise ValueError("condition activation manifest content changed")
    manifest = load_runtime_service_manifest(path, expected_commit=expected_commit)
    if manifest.service_kind is not expected_kind or _activation_bytes(path, root) != payload:
        raise ValueError("condition activation actual role or manifest changed")
    settings = manifest.model_dump(mode="json")["settings"].get("condition_alert_runtime")
    if settings is None:
        raise ValueError("condition activation is not installed")
    spec = ConditionAlertActivationSettings.model_validate_json(canonical_json_bytes(settings))
    binding = ConditionAlertActivationBinding(
        **spec.model_dump(mode="python"),
        producer_manifest_sha256=expected_manifest_sha256,
        producer_commit=expected_commit,
        service_kind=expected_kind.value,
    )
    for stage, role in _STAGE_ROLES.items():
        if (
            getattr(binding, stage + "_enabled")
            and binding.service_kind != role
            and not (
                stage in {"evaluation", "event_write"}
                and binding.service_kind == "price_alert_runtime"
            )
        ):
            raise ValueError("condition activation grants a stage outside the actual role")
    capability = object.__new__(ConditionAlertRuntimeActivation)
    _ACTIVATIONS[capability] = binding, path, root, payload
    return capability


def require_condition_alert_activation(
    value: object, stage: str
) -> ConditionAlertActivationBinding:
    if type(value) is not ConditionAlertRuntimeActivation or value not in _ACTIVATIONS:
        raise TypeError("condition stage requires a verified original role activation")
    if stage not in _STAGE_ROLES:
        raise ValueError("unknown condition runtime stage")
    binding, path, root, original = _ACTIVATIONS[value]
    if not getattr(binding, stage + "_enabled") or (
        binding.service_kind != _STAGE_ROLES[stage]
        and not (
            stage in {"evaluation", "event_write"} and binding.service_kind == "price_alert_runtime"
        )
    ):
        raise ValueError("condition runtime stage is disabled")
    if _activation_bytes(path, root) != original:
        raise ValueError("condition activation changed after verification")
    return binding


class ConditionAlertProducerEventRecord(ConditionRuntimeModel):
    sequence: StrictInt = Field(ge=1)
    event: ConditionAlertEventEnvelope | BuiltinConditionAlertEventEnvelope
    payload_json: StrictStr = Field(min_length=1, max_length=16 * 1024)
    payload_sha256: PriceSha256

    @model_validator(mode="after")
    def exact_payload(self) -> Self:
        if (
            type(self.event) not in {ConditionAlertEventEnvelope, BuiltinConditionAlertEventEnvelope}
            or parse_condition_alert_event(self.payload_json) != self.event
            or self.payload_sha256 != self.event.sha256
        ):
            raise ValueError("condition producer record differs from its sealed event")
        return self


def require_verified_condition_activation(
    value: object, role: str
) -> ConditionAlertActivationBinding:
    if type(value) is not ConditionAlertRuntimeActivation or value not in _ACTIVATIONS:
        raise TypeError("condition role requires its original actual verifier")
    binding, path, root, original = _ACTIVATIONS[value]
    if binding.service_kind != role or _activation_bytes(path, root) != original:
        raise ValueError("condition actual role or manifest changed")
    return binding


def read_condition_activation_setting(value: object, name: str) -> object:
    """Read an additive setting from the same original verified manifest bytes."""
    if type(value) is not ConditionAlertRuntimeActivation or value not in _ACTIVATIONS:
        raise TypeError("condition settings require the actual original activation")
    _, path, root, original = _ACTIVATIONS[value]
    if _activation_bytes(path, root) != original:
        raise ValueError("condition activation changed before reading its settings")
    raw = strict_json_loads(original)
    # Original manifests may be formatted JSON; their verifier pins exact bytes.
    return raw["settings"].get(name)


def condition_activation_runtime_root(value: object) -> Path:
    if type(value) is not ConditionAlertRuntimeActivation or value not in _ACTIVATIONS:
        raise TypeError("condition runtime root requires the original actual verifier")
    _, path, root, original = _ACTIVATIONS[value]
    if _activation_bytes(path, root) != original:
        raise ValueError("condition original installation changed")
    return root
