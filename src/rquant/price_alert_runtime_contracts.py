"""Exact price-event facts; independent of the frozen strategy envelopes."""

from __future__ import annotations

import os
import stat
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal, Self
from weakref import WeakKeyDictionary

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    StringConstraints,
    field_serializer,
    field_validator,
    model_validator,
)

from rquant.manual_watchlist import OwnerId, TsCode
from rquant.runtime_contracts import AwareUtcDatetime
from rquant.strict_json import canonical_json_bytes, strict_canonical_json_loads


def _nonzero_sha(value: str) -> str:
    if value == "0" * 64:
        raise ValueError("price runtime SHA must identify actual content")
    return value


PriceSha256 = Annotated[
    str, StringConstraints(pattern=r"^[0-9a-f]{64}$"), AfterValidator(_nonzero_sha)
]
PriceCommit = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
PriceRuleId = Annotated[str, StringConstraints(min_length=1, max_length=128)]
PriceTimestampProvenance = Literal["provider_source_timestamp", "response_received_at_fallback"]


class PriceAlertCapacityExceeded(ValueError):  # noqa: N818 - Keep the typed v1 outcome name.
    """The complete price domain cannot fit its frozen input or storage budget."""


def utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("price runtime time must be aware")
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def decimal_text(value: str | Decimal) -> str:
    if not isinstance(value, (str, Decimal)):
        raise TypeError("price must use exact decimal text")
    if isinstance(value, str) and len(value) > 128:
        raise ValueError("price decimal representation is too large")
    try:
        decimal = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("price is not a decimal") from exc
    if (
        not decimal.is_finite()
        or decimal <= 0
        or len(decimal.as_tuple().digits) > 64
        or abs(decimal.as_tuple().exponent) > 64
        or abs(decimal.adjusted()) > 64
    ):
        raise ValueError("price decimal is outside the representation budget")
    result = format(decimal, "f")
    if len(result) > 128:
        raise ValueError("price decimal representation is too large")
    return result


class PriceRuntimeModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    def wire_bytes(self) -> bytes:
        return canonical_json_bytes(self.model_dump(mode="json"))

    @property
    def sha256(self) -> str:
        return sha256(self.wire_bytes()).hexdigest()


class PriceAlertFrequencyPolicy(PriceRuntimeModel):
    policy_schema: Literal["price-alert-frequency/v1"] = "price-alert-frequency/v1"
    mode: Literal["per_rule_cooldown"] = "per_rule_cooldown"
    cooldown_seconds: StrictInt = Field(default=300, ge=60, le=3600)


class PriceAlertEventEnvelope(PriceRuntimeModel):
    envelope_schema: Literal["rquant.price-alert-event/v1"] = "rquant.price-alert-event/v1"
    event_id: PriceSha256
    owner_id: OwnerId
    rule_id: PriceRuleId
    rule_version: StrictInt = Field(ge=1)
    membership_version: StrictInt = Field(ge=1)
    ts_code: TsCode
    trade_date: date
    kind: Literal["threshold_reached"] = "threshold_reached"
    rule_body_sha256: PriceSha256
    member_binding_sha256: PriceSha256
    frequency_policy_sha256: PriceSha256
    comparison: Literal["gte", "lte"]
    threshold: StrictStr
    price: StrictStr
    rule_name: StrictStr = Field(min_length=1, max_length=80)
    priority: Literal["P0", "P1", "P2", "P3"]
    scope_generation_id: PriceSha256
    scope_manifest_sha256: PriceSha256
    calendar_content_sha256: PriceSha256
    quote_source_generation_id: PriceSha256
    quote_batch_id: PriceSha256
    quote_sequence: StrictInt = Field(ge=0)
    quote_revision: StrictInt = Field(ge=1)
    quote_payload_sha256: PriceSha256
    quote_request_binding_sha256: PriceSha256
    quote_observed_at: AwareUtcDatetime
    source_timestamp_provenance: PriceTimestampProvenance
    quote_available_at: AwareUtcDatetime
    evaluated_at: AwareUtcDatetime
    available_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    producer_manifest_sha256: PriceSha256
    producer_commit: PriceCommit
    source_epoch: PriceSha256

    @field_validator("threshold", "price")
    @classmethod
    def exact_decimal(cls, value: str) -> str:
        if decimal_text(value) != value:
            raise ValueError("price must use canonical fixed decimal text")
        return value

    @field_serializer(
        "quote_observed_at", "quote_available_at", "evaluated_at", "available_at", "expires_at"
    )
    def time_text(self, value: datetime) -> str:
        return utc_text(value)

    @property
    def observation_key(self) -> str:
        body = {
            name: self.model_dump(mode="json")[name]
            for name in (
                "quote_source_generation_id",
                "ts_code",
                "trade_date",
                "quote_observed_at",
                "price",
                "source_timestamp_provenance",
            )
        }
        return sha256(canonical_json_bytes(body)).hexdigest()

    @property
    def expected_event_id(self) -> str:
        body = {
            name: self.model_dump(mode="json")[name]
            for name in (
                "envelope_schema",
                "owner_id",
                "rule_id",
                "rule_version",
                "membership_version",
                "ts_code",
                "trade_date",
                "frequency_policy_sha256",
                "rule_body_sha256",
            )
        }
        body["observation_key"] = self.observation_key
        return sha256(canonical_json_bytes(body)).hexdigest()

    @model_validator(mode="after")
    def validate_facts(self) -> Self:
        if self.event_id != self.expected_event_id:
            raise ValueError("price event identity differs from its facts")
        if not (
            self.quote_observed_at
            <= self.quote_available_at
            <= self.evaluated_at
            <= self.available_at
            < self.expires_at
            <= self.evaluated_at + timedelta(seconds=120)
        ):
            raise ValueError("price event visibility or TTL is invalid")
        if self.evaluated_at - self.quote_observed_at > timedelta(seconds=15):
            raise ValueError("price event quote is stale")
        if len(self.wire_bytes()) > 4096:
            raise PriceAlertCapacityExceeded("price event exceeds 4 KiB")
        return self

    @classmethod
    def create(cls, **facts: object) -> PriceAlertEventEnvelope:
        # Compute identity without normalizing Decimal under the active context.
        facts = dict(facts)
        if "event_id" in facts:
            raise ValueError("event identity is computed from facts")
        model = cls.model_construct(event_id="1" * 64, **facts)
        facts["event_id"] = model.expected_event_id
        return cls.model_validate(facts)


def parse_price_alert_event(value: object) -> PriceAlertEventEnvelope:
    if type(value) is PriceAlertEventEnvelope:
        return PriceAlertEventEnvelope.model_validate(value)
    if not isinstance(value, (bytes, str)):
        raise TypeError("price event must be its exact model or canonical JSON")
    payload = value.encode() if isinstance(value, str) else value
    if len(payload) > 4096:
        raise ValueError("price event exceeds 4 KiB")
    strict_canonical_json_loads(payload)
    event = PriceAlertEventEnvelope.model_validate_json(payload)
    if event.wire_bytes() != payload:
        raise ValueError("price event JSON is not canonical")
    return event


class PriceAlertSourceDescriptor(PriceRuntimeModel):
    source_id: StrictStr = Field(min_length=1, max_length=128)
    ledger_id: PriceSha256
    source_epoch: PriceSha256
    generation_id: PriceSha256
    producer_manifest_sha256: PriceSha256
    evaluation_contract_sha256: PriceSha256
    frequency_policy_sha256: PriceSha256
    routing_policy_sha256: PriceSha256
    first_sequence: StrictInt = Field(ge=1)
    high_watermark: StrictInt = Field(ge=0)

    @model_validator(mode="after")
    def continuous_range(self) -> Self:
        if self.first_sequence != 1 or self.high_watermark < 0:
            raise ValueError("price source must retain its complete event ledger")
        return self


class PriceAlertActivationSettings(PriceRuntimeModel):
    protocol: Literal["price-alert-runtime/v1"] = "price-alert-runtime/v1"
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


class PriceAlertActivationBinding(PriceAlertActivationSettings):
    producer_manifest_sha256: PriceSha256
    producer_commit: PriceCommit
    service_kind: Literal["price_alert_runtime", "signal_router", "notifier"]


class PriceAlertRuntimeActivation:
    """An opaque capability issued only after reading an actual owned manifest."""

    __slots__ = ("__weakref__",)

    def __init__(self) -> None:
        raise TypeError("price activation must come from the trusted manifest verifier")


_ACTIVATIONS: WeakKeyDictionary[
    PriceAlertRuntimeActivation,
    tuple[
        PriceAlertActivationBinding,
        Path,
        Path,
        bytes,
    ],
] = WeakKeyDictionary()
_STAGE_ROLES = {
    "evaluation": "price_alert_runtime",
    "event_write": "price_alert_runtime",
    "routing": "signal_router",
    "delivery": "notifier",
}


def _activation_bytes(path: Path, root: Path) -> bytes:
    from rquant.live_spool import LiveSpoolIntegrityError, _secure_read_regular_file

    if (
        not path.is_absolute()
        or path != Path(os.path.abspath(path))
        or not root.is_absolute()
        or root != Path(os.path.abspath(root))
        or not path.is_relative_to(root)
    ):
        raise ValueError("price activation paths must belong to the normalized runtime root")
    current = Path(path.anchor)
    identities = []
    for part in path.parts[1:-1]:
        current /= part
        info = current.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("price activation path contains an unsafe component")
        identities.append((current, info.st_dev, info.st_ino))
        if current.is_relative_to(root) and (
            info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700
        ):
            raise ValueError("price activation role directory must be owned and private")
    try:
        payload = _secure_read_regular_file(
            path, label="price activation manifest", max_bytes=64 * 1024
        )
    except LiveSpoolIntegrityError as exc:
        raise ValueError(str(exc)) from exc
    for directory, device, inode in identities:
        after = directory.lstat()
        if not stat.S_ISDIR(after.st_mode) or (after.st_dev, after.st_ino) != (device, inode):
            raise ValueError("price activation role directory changed while reading")
    return payload


def verify_price_alert_activation(
    manifest_path: Path,
    *,
    runtime_root: Path,
    expected_manifest_sha256: str,
    expected_commit: str,
    expected_kind: object,
) -> PriceAlertRuntimeActivation:
    from rquant.runtime_service_entrypoint import RuntimeServiceKind, load_runtime_service_manifest

    if type(expected_kind) is not RuntimeServiceKind or expected_kind.value not in {
        "price_alert_runtime",
        "signal_router",
        "notifier",
    }:
        raise TypeError("price activation requires an actual allowlisted runtime role")
    path, root = Path(manifest_path), Path(runtime_root)
    payload = _activation_bytes(path, root)
    if sha256(payload).hexdigest() != expected_manifest_sha256:
        raise ValueError("price activation manifest content changed")
    manifest = load_runtime_service_manifest(path, expected_commit=expected_commit)
    if manifest.service_kind is not expected_kind:
        raise ValueError("price activation role differs from the actual runtime role")
    if _activation_bytes(path, root) != payload:
        raise ValueError("price activation changed during verification")
    settings = manifest.settings.get("price_alert_runtime")
    # The original loader freezes dict/list values. Decode its actual JSON representation
    # through the strict JSON validator, retaining exact scalar and closed-field checks.
    settings_json = manifest.model_dump(mode="json")["settings"].get("price_alert_runtime")
    if settings is None:
        raise ValueError("price activation is not installed")
    spec = PriceAlertActivationSettings.model_validate_json(canonical_json_bytes(settings_json))
    binding = PriceAlertActivationBinding(
        **spec.model_dump(mode="python"),
        producer_manifest_sha256=expected_manifest_sha256,
        producer_commit=expected_commit,
        service_kind=expected_kind.value,
    )
    for stage, role in _STAGE_ROLES.items():
        if getattr(binding, stage + "_enabled") and binding.service_kind != role:
            raise ValueError("price activation grants a stage outside the actual role")
    capability = object.__new__(PriceAlertRuntimeActivation)
    _ACTIVATIONS[capability] = binding, path, root, payload
    return capability


def require_price_alert_activation(value: object, stage: str) -> PriceAlertActivationBinding:
    if type(value) is not PriceAlertRuntimeActivation or value not in _ACTIVATIONS:
        raise TypeError("price stage requires a verified runtime activation")
    if stage not in _STAGE_ROLES:
        raise ValueError("unknown price runtime stage")
    binding, path, root, original = _ACTIVATIONS[value]
    if not getattr(binding, stage + "_enabled") or binding.service_kind != _STAGE_ROLES[stage]:
        raise ValueError("price runtime stage is disabled")
    if _activation_bytes(path, root) != original:
        raise ValueError("price runtime activation changed")
    return binding


def require_verified_price_alert_activation(
    value: object, role: str
) -> PriceAlertActivationBinding:
    if type(value) is not PriceAlertRuntimeActivation or value not in _ACTIVATIONS:
        raise TypeError("price runtime requires an actual verified activation")
    binding, path, root, original = _ACTIVATIONS[value]
    if binding.service_kind != role or _activation_bytes(path, root) != original:
        raise ValueError("price runtime role or manifest changed")
    return binding
