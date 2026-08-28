"""Frozen Phase B successor base contracts and their subordinate staged overlay.

`RESET-REG-P1-01` freezes four exact schemas — `SuccessorChannelV1`,
`SuccessorBundleV1`, `OverlayDeclarationV1` and `OverlayBundleV1` — over the
current-family transport models that actually exist. The v2
`RuntimeSchemaContractBundle` remains the sole old authority and is neither
imported into these declarations nor overlaid by them.

This module is declaration-only. It has no durable authority: nothing here writes a
file, names a storage location, opens a database, or grants transport authority. The
readiness decision belongs to the later root-derived verification phase, so no state
here can reach it.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
from collections.abc import Mapping, Sequence
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol, Self, runtime_checkable

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictInt,
    StrictStr,
    model_validator,
)

from rquant.signal_family_constants import (
    ACCEPTED_FAMILY_IDS,
    OVERLAY_NAMESPACE,
    PAIR_IDS,
    SUCCESSOR_BUNDLE_NAMESPACE,
    SUCCESSOR_CHANNEL_BINDINGS,
    SuccessorChannelId,
    require_channel_role_domain,
)
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_canonical_json_loads

_SHA256 = r"^[0-9a-f]{64}$"
_SHA1 = r"^[0-9a-f]{40}$"

# Amended per Codex round-2 order 2026-08-25, ruling 2. The channel closed set, the single
# accepted family, both namespaces, the pair IDs, and the participant service-ID domain now
# live in the leaf module `rquant.signal_family_constants` so `Literal` annotations can name
# them at module scope without the import cycle the old deferred import worked around. These
# re-exports are the historical spelling and stay part of this module's public surface.
__all__ = [
    "ACCEPTED_FAMILY_IDS",
    "OVERLAY_NAMESPACE",
    "PAIR_IDS",
    "SUCCESSOR_BUNDLE_NAMESPACE",
    "SUCCESSOR_CHANNEL_BINDINGS",
    "ConflictAuditSink",
    "OverlayBundleV1",
    "OverlayDeclarationV1",
    "StagedOverlayStateV1",
    "SuccessorBundleV1",
    "SuccessorChannelV1",
    "SuccessorConflictAuditRecordV1",
    "SuccessorConflictError",
    "SuccessorGenerationSourceClosureV1",
    "SuccessorModelDescriptorV1",
    "SuccessorRegistry",
    "SuccessorRegistryError",
    "model_descriptor_hash",
    "overlay_bundle_canonical_json_bytes",
    "overlay_declaration_canonical_json_bytes",
    "resolve_payload_model",
    "resolve_successor_channel_descriptor",
    "successor_bundle_canonical_json_bytes",
    "successor_channel_canonical_json_bytes",
    "verify_successor_channel_binding",
]


class SuccessorRegistryError(ValueError):
    """A successor or overlay declaration violates its frozen contract."""


class SuccessorConflictError(SuccessorRegistryError):
    """A bundle, channel, overlay, or declaration identity was reused with new bytes."""


def _canonical_sha256(value: Any) -> str:
    """`SHA256(rquant.strict_json.canonical_json_bytes(value))`, per Phase B."""

    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


class _SuccessorStrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=False,
        revalidate_instances="always",
    )


def _require_exact_instance(value: object, expected: type, *, field: str) -> object:
    if type(value) is not expected:
        raise TypeError(f"{field} requires an exact {expected.__name__} object")
    return value


def _require_exact_items(value: object, expected: type, *, field: str) -> None:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"{field} requires an exact tuple of {expected.__name__} objects")
    for item in value:
        _require_exact_instance(item, expected, field=field)


def _require_known_channel(value: object) -> None:
    """Reject an out-of-set `channel_id` with its own message, ahead of `Literal`.

    The `Literal` annotation is the frozen closed set at the type layer; this keeps the
    rejection reason naming the offending channel instead of listing the members.
    """

    if isinstance(value, Mapping):
        channel_id = value.get("channel_id")
    else:
        channel_id = getattr(value, "channel_id", None)
    if isinstance(channel_id, str) and channel_id not in SUCCESSOR_CHANNEL_BINDINGS:
        raise ValueError(f"unknown successor transport channel: {channel_id}")


def _require_sorted_unique(values: tuple[str, ...], *, field: str) -> tuple[str, ...]:
    if not values:
        raise ValueError(f"{field} must be nonempty")
    if any(not item for item in values):
        raise ValueError(f"{field} cannot contain an empty identifier")
    if tuple(sorted(set(values))) != values:
        raise ValueError(f"{field} must be sorted and duplicate-free")
    return values


def _decode_object(payload: bytes | str) -> dict[str, Any]:
    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    try:
        decoded = strict_canonical_json_loads(raw)
    except StrictJsonError as exc:
        raise SuccessorRegistryError(str(exc)) from exc
    if type(decoded) is not dict:
        raise SuccessorRegistryError("successor authority payload must be a JSON object")
    return decoded


def _exact_tuples(decoded: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    """Map the JSON array form of the declared tuple fields, coercing nothing else."""

    values = dict(decoded)
    for field in fields:
        if field in values:
            if type(values[field]) is not list:
                raise SuccessorRegistryError(f"{field} must be a JSON array")
            values[field] = tuple(values[field])
    return values


class SuccessorGenerationSourceClosureV1(_SuccessorStrictModel):
    """The caller-injected generation source closure a payload model must live in."""

    producer_commit: StrictStr = Field(pattern=_SHA1)
    source_closure: frozenset[StrictStr]

    @model_validator(mode="after")
    def validate_closure(self) -> Self:
        if not self.source_closure:
            raise ValueError("generation source closure must be nonempty")
        for entry in self.source_closure:
            if not entry or PurePosixPath(entry).is_absolute():
                raise ValueError("source closure entries must be nonempty relative paths")
        return self


class SuccessorModelDescriptorV1(_SuccessorStrictModel):
    """The descriptor an actual resolved class produces, never a qualname alone."""

    payload_model: StrictStr = Field(min_length=1)
    declaration_schema_fingerprint: StrictStr = Field(pattern=_SHA256)
    physical_schema_fingerprint: StrictStr = Field(pattern=_SHA256)
    model_descriptor_hash: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="after")
    def validate_descriptor_hash(self) -> Self:
        if self.model_descriptor_hash != model_descriptor_hash(
            payload_model=self.payload_model,
            declaration_schema_fingerprint=self.declaration_schema_fingerprint,
            physical_schema_fingerprint=self.physical_schema_fingerprint,
        ):
            raise ValueError("model descriptor hash does not match its exact preimage")
        return self


def model_descriptor_hash(
    *,
    payload_model: str,
    declaration_schema_fingerprint: str,
    physical_schema_fingerprint: str,
) -> str:
    return _canonical_sha256(
        {
            "payload_model": payload_model,
            "declaration_schema_fingerprint": declaration_schema_fingerprint,
            "physical_schema_fingerprint": physical_schema_fingerprint,
        }
    )


def _repository_root() -> PurePosixPath:
    import rquant

    package = inspect.getsourcefile(rquant)
    if package is None:  # pragma: no cover - the package always has a source file
        raise SuccessorRegistryError("the rquant package has no source file")
    return PurePosixPath(package).parent.parent.parent


def _repository_relative_source(model: type[BaseModel]) -> str:
    source = inspect.getsourcefile(model)
    if source is None:
        raise SuccessorRegistryError("payload model has no importable source file")
    candidate = PurePosixPath(source)
    root = _repository_root()
    try:
        return str(candidate.relative_to(root))
    except ValueError as exc:
        raise SuccessorRegistryError(
            "payload model source is outside the generation repository"
        ) from exc


def resolve_payload_model(
    payload_model: str,
    *,
    closure: SuccessorGenerationSourceClosureV1,
) -> type[BaseModel]:
    """Resolve one authoritative qualified model string to its actual class."""

    _require_exact_instance(closure, SuccessorGenerationSourceClosureV1, field="closure")
    if type(payload_model) is not str or not payload_model:
        raise SuccessorRegistryError("payload_model must be a nonempty string")
    parts = payload_model.split(".")
    if len(parts) < 2 or any(not part for part in parts):
        raise SuccessorRegistryError(f"payload_model is not a qualified name: {payload_model}")
    module = None
    boundary = 0
    for index in range(len(parts) - 1, 0, -1):
        try:
            module = importlib.import_module(".".join(parts[:index]))
        except ImportError:
            continue
        boundary = index
        break
    if module is None:
        raise SuccessorRegistryError(f"payload_model module does not exist: {payload_model}")
    resolved: object = module
    for attribute in parts[boundary:]:
        try:
            resolved = getattr(resolved, attribute)
        except AttributeError as exc:
            raise SuccessorRegistryError(
                f"payload_model does not resolve to a declared class: {payload_model}"
            ) from exc
    if not inspect.isclass(resolved) or not issubclass(resolved, BaseModel):
        raise SuccessorRegistryError(
            f"payload_model must name a declared payload class: {payload_model}"
        )
    if f"{resolved.__module__}.{resolved.__qualname__}" != payload_model:
        raise SuccessorRegistryError(
            f"payload_model is an alias for {resolved.__module__}.{resolved.__qualname__}"
        )
    relative = _repository_relative_source(resolved)
    if relative not in closure.source_closure:
        raise SuccessorRegistryError(
            f"payload_model is not covered by the generation source closure: {relative}"
        )
    return resolved


def resolve_successor_channel_descriptor(
    channel_id: str,
    payload_model: str,
    *,
    closure: SuccessorGenerationSourceClosureV1,
) -> SuccessorModelDescriptorV1:
    """Recompute one channel descriptor from the actual class behind `payload_model`."""

    expected = SUCCESSOR_CHANNEL_BINDINGS.get(channel_id)
    if expected is None:
        raise SuccessorRegistryError(f"unknown successor transport channel: {channel_id}")
    if payload_model != expected:
        raise SuccessorRegistryError(
            f"channel {channel_id} is bound to {expected}, not {payload_model}"
        )
    model = resolve_payload_model(payload_model, closure=closure)
    # The declaration and physical fingerprints come from the repository's existing
    # authoritative computations; this module never restates their formulas.
    from rquant.runtime_schema_registry import (
        RuntimePhysicalTableSchema,
        _ChannelSpec,
        _declaration,
    )

    declaration = _declaration(
        _ChannelSpec(channel_id, model, (), ()),
        producer_commit=closure.producer_commit,
    )
    physical = RuntimePhysicalTableSchema.create(
        object_name=payload_model,
        declaration=declaration,
    )
    return SuccessorModelDescriptorV1(
        payload_model=payload_model,
        declaration_schema_fingerprint=declaration.schema_fingerprint,
        physical_schema_fingerprint=physical.physical_schema_fingerprint,
        model_descriptor_hash=model_descriptor_hash(
            payload_model=payload_model,
            declaration_schema_fingerprint=declaration.schema_fingerprint,
            physical_schema_fingerprint=physical.physical_schema_fingerprint,
        ),
    )


class SuccessorChannelV1(_SuccessorStrictModel):
    channel_id: SuccessorChannelId
    payload_model: StrictStr = Field(min_length=1)
    declaration_schema_fingerprint: StrictStr = Field(pattern=_SHA256)
    physical_schema_fingerprint: StrictStr = Field(pattern=_SHA256)
    model_descriptor_hash: StrictStr = Field(pattern=_SHA256)
    producer_service_ids: tuple[StrictStr, ...]
    consumer_service_ids: tuple[StrictStr, ...]
    channel_hash: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="before")
    @classmethod
    def reject_unknown_channel(cls, value: object) -> object:
        """Name an out-of-set channel before the `Literal` reports a bare type error."""

        _require_known_channel(value)
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        expected_model = SUCCESSOR_CHANNEL_BINDINGS.get(self.channel_id)
        if expected_model is None:
            raise ValueError(f"unknown successor transport channel: {self.channel_id}")
        if self.payload_model != expected_model:
            raise ValueError("successor channel payload model is not its frozen binding")
        _require_sorted_unique(self.producer_service_ids, field="producer_service_ids")
        _require_sorted_unique(self.consumer_service_ids, field="consumer_service_ids")
        require_channel_role_domain(
            self.channel_id,
            self.producer_service_ids,
            field="producer_service_ids",
            direction="produce",
        )
        require_channel_role_domain(
            self.channel_id,
            self.consumer_service_ids,
            field="consumer_service_ids",
            direction="consume",
        )
        if self.model_descriptor_hash != model_descriptor_hash(
            payload_model=self.payload_model,
            declaration_schema_fingerprint=self.declaration_schema_fingerprint,
            physical_schema_fingerprint=self.physical_schema_fingerprint,
        ):
            raise ValueError("successor channel descriptor hash does not match its preimage")
        if self.channel_hash != _canonical_sha256(
            self.model_dump(mode="json", exclude={"channel_hash"})
        ):
            raise ValueError("successor channel hash does not match its canonical content")
        return self

    @property
    def descriptor(self) -> SuccessorModelDescriptorV1:
        return SuccessorModelDescriptorV1(
            payload_model=self.payload_model,
            declaration_schema_fingerprint=self.declaration_schema_fingerprint,
            physical_schema_fingerprint=self.physical_schema_fingerprint,
            model_descriptor_hash=self.model_descriptor_hash,
        )

    @classmethod
    def create(
        cls,
        channel_id: str,
        *,
        producer_service_ids: tuple[str, ...],
        consumer_service_ids: tuple[str, ...],
        closure: SuccessorGenerationSourceClosureV1,
    ) -> Self:
        expected = SUCCESSOR_CHANNEL_BINDINGS.get(channel_id)
        if expected is None:
            raise SuccessorRegistryError(f"unknown successor transport channel: {channel_id}")
        descriptor = resolve_successor_channel_descriptor(channel_id, expected, closure=closure)
        values: dict[str, Any] = {
            "channel_id": channel_id,
            "payload_model": descriptor.payload_model,
            "declaration_schema_fingerprint": descriptor.declaration_schema_fingerprint,
            "physical_schema_fingerprint": descriptor.physical_schema_fingerprint,
            "model_descriptor_hash": descriptor.model_descriptor_hash,
            "producer_service_ids": tuple(producer_service_ids),
            "consumer_service_ids": tuple(consumer_service_ids),
        }
        return cls(**values, channel_hash=_canonical_sha256(values))

    @classmethod
    def from_canonical_json(
        cls,
        payload: bytes | str,
        *,
        closure: SuccessorGenerationSourceClosureV1,
    ) -> Self:
        channel = cls._from_decoded(_decode_object(payload))
        verify_successor_channel_binding(channel, closure=closure)
        return channel

    @classmethod
    def _from_decoded(cls, decoded: dict[str, Any]) -> Self:
        return cls.model_validate(
            _exact_tuples(decoded, ("producer_service_ids", "consumer_service_ids"))
        )


def verify_successor_channel_binding(
    channel: SuccessorChannelV1,
    *,
    closure: SuccessorGenerationSourceClosureV1,
) -> SuccessorModelDescriptorV1:
    """Reject any descriptor that the actual class does not reproduce exactly."""

    _require_exact_instance(channel, SuccessorChannelV1, field="channel")
    descriptor = resolve_successor_channel_descriptor(
        channel.channel_id,
        channel.payload_model,
        closure=closure,
    )
    if descriptor != channel.descriptor:
        raise SuccessorRegistryError(
            f"successor channel descriptor does not match the actual model: {channel.channel_id}"
        )
    return descriptor


class SuccessorBundleV1(_SuccessorStrictModel):
    schema_version: StrictInt
    bundle_namespace: Literal["rquant.signal-family.successor"]
    channels: tuple[SuccessorChannelV1, ...]
    content_hash: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="before")
    @classmethod
    def reject_substituted_channels(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise TypeError("successor bundle must be an object")
        if "channels" in value:
            _require_exact_items(value["channels"], SuccessorChannelV1, field="channels")
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.schema_version != 1:
            raise ValueError("unsupported successor bundle schema version")
        if not self.channels:
            raise ValueError("successor bundle must declare at least one channel")
        channel_ids = tuple(channel.channel_id for channel in self.channels)
        if tuple(sorted(set(channel_ids))) != channel_ids:
            raise ValueError("successor bundle channels must be sorted and duplicate-free")
        channel_hashes = tuple(channel.channel_hash for channel in self.channels)
        if len(set(channel_hashes)) != len(channel_hashes):
            raise ValueError("successor bundle contains a duplicate channel hash")
        if self.content_hash != _canonical_sha256(
            self.model_dump(mode="json", exclude={"content_hash"})
        ):
            raise ValueError("successor bundle content hash does not match its canonical content")
        return self

    @property
    def identity(self) -> str:
        """The bundle identity is its namespace, and nothing else.

        Amended per Codex round-2 order 2026-08-25, ruling 2. Exactly one successor base may
        be registered, so the namespace alone names the slot a second bundle would collide
        with; neither the content hash nor the channel set participates.
        """

        return self.bundle_namespace

    def channel(self, channel_id: str) -> SuccessorChannelV1:
        for channel in self.channels:
            if channel.channel_id == channel_id:
                return channel
        raise SuccessorRegistryError(f"channel is absent from the successor base: {channel_id}")

    @classmethod
    def create(cls, *, channels: tuple[SuccessorChannelV1, ...]) -> Self:
        _require_exact_items(channels, SuccessorChannelV1, field="channels")
        ordered = tuple(sorted(channels, key=lambda channel: channel.channel_id))
        values: dict[str, Any] = {
            "schema_version": 1,
            "bundle_namespace": SUCCESSOR_BUNDLE_NAMESPACE,
            "channels": ordered,
        }
        preimage = dict(values)
        preimage["channels"] = [channel.model_dump(mode="json") for channel in ordered]
        return cls(**values, content_hash=_canonical_sha256(preimage))

    @classmethod
    def from_canonical_json(
        cls,
        payload: bytes | str,
        *,
        closure: SuccessorGenerationSourceClosureV1,
    ) -> Self:
        decoded = _decode_object(payload)
        values = dict(decoded)
        raw_channels = values.get("channels")
        if raw_channels is not None:
            if type(raw_channels) is not list:
                raise SuccessorRegistryError("channels must be a JSON array")
            channels = []
            for item in raw_channels:
                if type(item) is not dict:
                    raise SuccessorRegistryError("channels must contain exact channel objects")
                channels.append(SuccessorChannelV1._from_decoded(item))
            values["channels"] = tuple(channels)
        bundle = cls.model_validate(values)
        for channel in bundle.channels:
            verify_successor_channel_binding(channel, closure=closure)
        return bundle


class OverlayDeclarationV1(_SuccessorStrictModel):
    channel_id: SuccessorChannelId
    base_bundle_content_hash: StrictStr = Field(pattern=_SHA256)
    base_declaration_fingerprint: StrictStr = Field(pattern=_SHA256)
    base_physical_fingerprint: StrictStr = Field(pattern=_SHA256)
    model_descriptor_hash: StrictStr = Field(pattern=_SHA256)
    accepted_family_ids: tuple[StrictStr, ...]
    pair_ids: tuple[StrictStr, ...]
    declaration_hash: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="before")
    @classmethod
    def reject_unknown_channel(cls, value: object) -> object:
        """Name an out-of-set channel before the `Literal` reports a bare type error."""

        _require_known_channel(value)
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.channel_id not in SUCCESSOR_CHANNEL_BINDINGS:
            raise ValueError(f"unknown successor transport channel: {self.channel_id}")
        _require_sorted_unique(self.accepted_family_ids, field="accepted_family_ids")
        _require_sorted_unique(self.pair_ids, field="pair_ids")
        if not set(self.accepted_family_ids) <= set(ACCEPTED_FAMILY_IDS):
            raise ValueError("overlay declaration accepts a family absent from the current family")
        if not set(self.pair_ids) <= set(PAIR_IDS):
            raise ValueError("overlay declaration binds an unknown pair id")
        if self.model_descriptor_hash != model_descriptor_hash(
            payload_model=SUCCESSOR_CHANNEL_BINDINGS[self.channel_id],
            declaration_schema_fingerprint=self.base_declaration_fingerprint,
            physical_schema_fingerprint=self.base_physical_fingerprint,
        ):
            raise ValueError("overlay declaration descriptor hash does not match its base")
        if self.declaration_hash != _canonical_sha256(
            self.model_dump(mode="json", exclude={"declaration_hash"})
        ):
            raise ValueError("overlay declaration hash does not match its canonical content")
        return self

    @classmethod
    def create(
        cls,
        *,
        channel: SuccessorChannelV1,
        base_bundle_content_hash: str,
        accepted_family_ids: tuple[str, ...],
        pair_ids: tuple[str, ...],
    ) -> Self:
        _require_exact_instance(channel, SuccessorChannelV1, field="channel")
        values: dict[str, Any] = {
            "channel_id": channel.channel_id,
            "base_bundle_content_hash": base_bundle_content_hash,
            "base_declaration_fingerprint": channel.declaration_schema_fingerprint,
            "base_physical_fingerprint": channel.physical_schema_fingerprint,
            "model_descriptor_hash": channel.model_descriptor_hash,
            "accepted_family_ids": tuple(accepted_family_ids),
            "pair_ids": tuple(pair_ids),
        }
        return cls(**values, declaration_hash=_canonical_sha256(values))

    @classmethod
    def from_canonical_json(cls, payload: bytes | str) -> Self:
        return cls._from_decoded(_decode_object(payload))

    @classmethod
    def _from_decoded(cls, decoded: dict[str, Any]) -> Self:
        return cls.model_validate(_exact_tuples(decoded, ("accepted_family_ids", "pair_ids")))


class OverlayBundleV1(_SuccessorStrictModel):
    overlay_namespace: Literal["rquant.signal-family.overlay"]
    overlay_version: StrictInt
    base_bundle_content_hash: StrictStr = Field(pattern=_SHA256)
    declarations: tuple[OverlayDeclarationV1, ...]
    content_hash: StrictStr = Field(pattern=_SHA256)

    @model_validator(mode="before")
    @classmethod
    def reject_substituted_declarations(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise TypeError("overlay bundle must be an object")
        if "declarations" in value:
            _require_exact_items(
                value["declarations"],
                OverlayDeclarationV1,
                field="declarations",
            )
        return value

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.overlay_version != 1:
            raise ValueError("unsupported overlay bundle version")
        if not self.declarations:
            raise ValueError("overlay bundle must declare at least one channel")
        channel_ids = tuple(declaration.channel_id for declaration in self.declarations)
        if tuple(sorted(set(channel_ids))) != channel_ids:
            raise ValueError("overlay declarations must be sorted and duplicate-free")
        hashes = tuple(declaration.declaration_hash for declaration in self.declarations)
        if len(set(hashes)) != len(hashes):
            raise ValueError("overlay bundle contains a duplicate declaration hash")
        for declaration in self.declarations:
            if declaration.base_bundle_content_hash != self.base_bundle_content_hash:
                raise ValueError("overlay declaration names a different successor base bundle")
        if self.identity != (OVERLAY_NAMESPACE, self.base_bundle_content_hash):
            raise ValueError("overlay identity is not its namespace and base bundle hash")
        if self.content_hash != _canonical_sha256(
            self.model_dump(mode="json", exclude={"content_hash"})
        ):
            raise ValueError("overlay bundle content hash does not match its canonical content")
        return self

    @property
    def identity(self) -> tuple[str, str]:
        """The overlay identity is `(overlay_namespace, base_bundle_content_hash)`.

        Amended per Codex round-2 order 2026-08-25, ruling 2. One overlay may be staged per
        successor base, so the same namespace over a different base is a *different*
        identity, not a conflict, and the registry's conflict decision uses this tuple.
        """

        return (self.overlay_namespace, self.base_bundle_content_hash)

    @classmethod
    def create(
        cls,
        *,
        base_bundle_content_hash: str,
        declarations: tuple[OverlayDeclarationV1, ...],
    ) -> Self:
        _require_exact_items(declarations, OverlayDeclarationV1, field="declarations")
        ordered = tuple(sorted(declarations, key=lambda item: item.channel_id))
        values: dict[str, Any] = {
            "overlay_namespace": OVERLAY_NAMESPACE,
            "overlay_version": 1,
            "base_bundle_content_hash": base_bundle_content_hash,
            "declarations": ordered,
        }
        preimage = dict(values)
        preimage["declarations"] = [item.model_dump(mode="json") for item in ordered]
        return cls(**values, content_hash=_canonical_sha256(preimage))

    @classmethod
    def from_canonical_json(cls, payload: bytes | str) -> Self:
        decoded = _decode_object(payload)
        values = dict(decoded)
        raw_declarations = values.get("declarations")
        if raw_declarations is not None:
            if type(raw_declarations) is not list:
                raise SuccessorRegistryError("declarations must be a JSON array")
            declarations = []
            for item in raw_declarations:
                if type(item) is not dict:
                    raise SuccessorRegistryError(
                        "declarations must contain exact declaration objects"
                    )
                declarations.append(OverlayDeclarationV1._from_decoded(item))
            values["declarations"] = tuple(declarations)
        return cls.model_validate(values)


def _canonical_model_bytes(model: BaseModel, expected: type[BaseModel]) -> bytes:
    _require_exact_instance(model, expected, field=expected.__name__)
    validated = expected.model_validate(model)
    return canonical_json_bytes(validated.model_dump(mode="json"))


def successor_channel_canonical_json_bytes(channel: SuccessorChannelV1) -> bytes:
    return _canonical_model_bytes(channel, SuccessorChannelV1)


def successor_bundle_canonical_json_bytes(bundle: SuccessorBundleV1) -> bytes:
    return _canonical_model_bytes(bundle, SuccessorBundleV1)


def overlay_declaration_canonical_json_bytes(declaration: OverlayDeclarationV1) -> bytes:
    return _canonical_model_bytes(declaration, OverlayDeclarationV1)


def overlay_bundle_canonical_json_bytes(overlay: OverlayBundleV1) -> bytes:
    return _canonical_model_bytes(overlay, OverlayBundleV1)


class SuccessorConflictAuditRecordV1(_SuccessorStrictModel):
    """Bounded evidence that one frozen identity was reused with different bytes."""

    identity_kind: Literal["bundle", "channel", "overlay", "declaration"]
    identity: StrictStr = Field(min_length=1)
    existing_hash: StrictStr = Field(pattern=_SHA256)
    attempted_hash: StrictStr = Field(pattern=_SHA256)


@runtime_checkable
class ConflictAuditSink(Protocol):
    """Append-only conflict evidence receiver; the durable store is a later phase."""

    def append(self, record: SuccessorConflictAuditRecordV1) -> None: ...


class StagedOverlayStateV1(_SuccessorStrictModel):
    """What a staged overlay covers. Coverage is not, and never becomes, readiness."""

    base_bundle_content_hash: StrictStr = Field(pattern=_SHA256)
    overlay_content_hash: StrictStr = Field(pattern=_SHA256)
    covered_channel_ids: tuple[StrictStr, ...]
    missing_channel_ids: tuple[StrictStr, ...]
    complete: StrictBool

    @model_validator(mode="after")
    def validate_coverage(self) -> Self:
        _require_sorted_unique(self.covered_channel_ids, field="covered_channel_ids")
        if self.missing_channel_ids:
            _require_sorted_unique(self.missing_channel_ids, field="missing_channel_ids")
        if set(self.covered_channel_ids) & set(self.missing_channel_ids):
            raise ValueError("a channel cannot be both covered and missing")
        if self.complete is not (not self.missing_channel_ids):
            raise ValueError("overlay completeness must match its missing channels")
        return self


class SuccessorRegistry:
    """In-memory Phase B registry: successor base first, then a subordinate overlay.

    It holds declarations only. It never persists anything, and it emits no readiness
    decision — a complete overlay is still only a staged overlay.
    """

    def __init__(
        self,
        *,
        closure: SuccessorGenerationSourceClosureV1,
        audit_sink: ConflictAuditSink,
    ) -> None:
        _require_exact_instance(closure, SuccessorGenerationSourceClosureV1, field="closure")
        if not isinstance(audit_sink, ConflictAuditSink):
            raise TypeError("audit_sink must implement the conflict audit sink protocol")
        self._closure = closure
        self._audit_sink = audit_sink
        self._bundle: SuccessorBundleV1 | None = None
        self._bundle_bytes: bytes | None = None
        self._overlay: OverlayBundleV1 | None = None
        self._overlay_bytes: bytes | None = None
        self._overlay_state: StagedOverlayStateV1 | None = None

    def successor_bundle(self) -> SuccessorBundleV1 | None:
        return self._bundle

    def staged_overlay(self) -> StagedOverlayStateV1 | None:
        return self._overlay_state

    def register_successor_bundle(self, payload: bytes | str) -> SuccessorBundleV1:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        candidate = SuccessorBundleV1.from_canonical_json(raw, closure=self._closure)
        existing = self._bundle
        if existing is not None:
            if raw == self._bundle_bytes:
                return existing
            self._audit_sink.append(
                SuccessorConflictAuditRecordV1(
                    identity_kind="bundle",
                    identity=existing.identity,
                    existing_hash=existing.content_hash,
                    attempted_hash=candidate.content_hash,
                )
            )
            existing_channels = {
                channel.channel_id: channel.channel_hash for channel in existing.channels
            }
            for channel in candidate.channels:
                previous = existing_channels.get(channel.channel_id)
                if previous is not None and previous != channel.channel_hash:
                    self._audit_sink.append(
                        SuccessorConflictAuditRecordV1(
                            identity_kind="channel",
                            identity=channel.channel_id,
                            existing_hash=previous,
                            attempted_hash=channel.channel_hash,
                        )
                    )
            raise SuccessorConflictError(
                "a different successor bundle is already registered for this identity"
            )
        self._bundle = candidate
        self._bundle_bytes = raw
        return candidate

    def stage_overlay(self, payload: bytes | str) -> StagedOverlayStateV1:
        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        base = self._bundle
        if base is None:
            raise SuccessorRegistryError(
                "an overlay cannot be staged before its successor bundle is registered"
            )
        candidate = OverlayBundleV1.from_canonical_json(raw)
        if candidate.base_bundle_content_hash != base.content_hash:
            raise SuccessorRegistryError(
                "the staged overlay does not name the registered successor bundle"
            )
        for declaration in candidate.declarations:
            channel = base.channel(declaration.channel_id)
            if (
                declaration.base_declaration_fingerprint != channel.declaration_schema_fingerprint
                or declaration.base_physical_fingerprint != channel.physical_schema_fingerprint
                or declaration.model_descriptor_hash != channel.model_descriptor_hash
            ):
                raise SuccessorRegistryError(
                    f"overlay declaration does not bind its base channel: {channel.channel_id}"
                )
        existing = self._overlay
        if existing is not None and existing.identity != candidate.identity:
            # A different successor base is a different overlay identity, so it can never be
            # the same slot. The base equality above already rejects that case, and this
            # keeps the conflict decision spelled in terms of the frozen identity tuple.
            raise SuccessorRegistryError(
                "the staged overlay does not name the registered successor bundle"
            )
        if existing is not None:
            if raw == self._overlay_bytes:
                state = self._overlay_state
                if state is None:  # pragma: no cover - set together with the overlay
                    raise SuccessorRegistryError("staged overlay state is missing")
                return state
            self._audit_sink.append(
                SuccessorConflictAuditRecordV1(
                    identity_kind="overlay",
                    identity=":".join(candidate.identity),
                    existing_hash=existing.content_hash,
                    attempted_hash=candidate.content_hash,
                )
            )
            existing_declarations = {
                declaration.channel_id: declaration.declaration_hash
                for declaration in existing.declarations
            }
            for declaration in candidate.declarations:
                previous = existing_declarations.get(declaration.channel_id)
                if previous is not None and previous != declaration.declaration_hash:
                    self._audit_sink.append(
                        SuccessorConflictAuditRecordV1(
                            identity_kind="declaration",
                            identity=declaration.channel_id,
                            existing_hash=previous,
                            attempted_hash=declaration.declaration_hash,
                        )
                    )
            raise SuccessorConflictError(
                "a different overlay is already staged for this successor base"
            )
        covered = tuple(declaration.channel_id for declaration in candidate.declarations)
        missing = tuple(
            channel.channel_id for channel in base.channels if channel.channel_id not in covered
        )
        state = StagedOverlayStateV1(
            base_bundle_content_hash=base.content_hash,
            overlay_content_hash=candidate.content_hash,
            covered_channel_ids=covered,
            missing_channel_ids=missing,
            complete=not missing,
        )
        self._overlay = candidate
        self._overlay_bytes = raw
        self._overlay_state = state
        return state
