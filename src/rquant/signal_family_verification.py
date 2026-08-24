"""Frozen Phase C verification models and the arithmetic that binds them.

`RESET-REG-P0`, `RESET-REG-P0-01`, `RESET-REG-P1-02`, and `RESET-REG-P2-01` freeze the
release-verification policy, the in-generation verification/test manifest, the exact
service bindings, the bounded child result, the five pair receipts, the readiness decision,
and the bounded audit record. This module owns those schemas, the four policy-bound
set-hash preimages, the authority epoch key, the execution-evidence preimage, the receipt
fingerprint and its uniqueness key, profile-derived freshness, and the minimal lifecycle.

It owns nothing else. There is no verifier process, no fixed harness, no inter-process
transport, and no append store here — those are root-owned and live outside this module by
design, so nothing a caller can reach from here grants append authority. The declarations
below become eligible only through one exact external root-policy match performed by the
separate root verifier.
"""

from __future__ import annotations

import hashlib
import types
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from enum import Enum, StrEnum
from pathlib import PurePosixPath
from types import MappingProxyType
from typing import Annotated, Any, Literal, Self, Union, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, model_validator

from rquant.runtime_authority import RuntimeAuthorityState, RuntimeGenerationLifecycle
from rquant.runtime_service_entrypoint import RuntimeServiceKind, RuntimeServiceManifest
from rquant.signal_family_successor_registry import ACCEPTED_FAMILY_IDS, PAIR_IDS
from rquant.signal_family_successor_registry import _canonical_sha256 as canonical_sha256
from rquant.strict_json import StrictJsonError, canonical_json_bytes, strict_canonical_json_loads

_SHA256 = r"^[0-9a-f]{64}$"
_TIMESTAMP = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$"
_ROLE_NAME = r"^[a-z][a-z0-9_-]{0,63}$"
_MODULE_NAME = r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
_OPERATION_ID = r"^[0-9a-f]{32}$"

Sha256 = Annotated[StrictStr, Field(pattern=_SHA256)]
CanonicalTimestamp = Annotated[StrictStr, Field(pattern=_TIMESTAMP)]
PairIdField = Literal[
    "notifier-serving",
    "router-notifier",
    "router-paper",
    "strategy-router",
    "strategy-shadow",
]

# The fixed external anchors. They are declared here for the root verifier to compare
# against; nothing in this module opens, creates, or amends either path.
VERIFIER_POLICY_PATH = "/etc/rquant/signal-family-verifier-policy-v1.json"
VERIFIER_POLICY_ID = "signal-family-verifier-policy-v1"
HARNESS_IDENTITY = "/usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"

# Frozen generation-relative locations of the immutable Phase B/C declarations. Each one
# must also be an entry of the full generation manifest; that check belongs to the root.
SUCCESSOR_BUNDLE_RELATIVE_PATH = "signal-family/successor-bundle-v1.json"
OVERLAY_BUNDLE_RELATIVE_PATH = "signal-family/overlay-bundle-v1.json"
VERIFICATION_MANIFEST_RELATIVE_PATH = "signal-family/verification-manifest-v1.json"
TEST_MANIFEST_RELATIVE_PATH = "signal-family/test-manifest-v1.json"

MAX_VECTOR_INPUT_BYTES = 65_536
MAX_CANONICAL_RESULT_BYTES = 65_536
MAX_IPC_RESPONSE_BYTES = 1_048_576


class SignalFamilyVerificationError(ValueError):
    """A Phase C declaration, preimage, or transition violated its frozen contract."""


class SignalFamilyVerificationConflictError(SignalFamilyVerificationError):
    """One frozen uniqueness key was reused with divergent bytes."""

    def __init__(self, message: str, *, audit_record: SignalFamilyVerificationAuditRecordV1):
        super().__init__(message)
        self.audit_record = audit_record


class SurfaceId(StrEnum):
    """The closed callable-object allowlist of authority.md L1211-1217.

    Each value is the exact `object.__module__ + "." + object.__qualname__` the fixed
    harness derives inside the unprivileged child. The root process never imports them.
    """

    STRATEGY_RUNNER_PROCESS_BATCH = "rquant.strategy_runner.StrategyRunnerStore.process_batch"
    STRATEGY_RUNNER_PUBLISH_SESSION_CLOSE_RECEIPT = (
        "rquant.strategy_runner.StrategyRunnerStore.publish_session_close_receipt"
    )
    SIGNAL_ROUTE_SPOOL_PUBLISH = "rquant.signal_route_spool.SignalRouteSpool.publish"
    PUBLISH_SIGNAL_BUS_PREFIX = "rquant.signal_route_spool.publish_signal_bus_prefix"
    PUBLISH_SIGNAL_AUTHORITY = "rquant.runtime_builder_signal._publish_signal_authority"
    SERVING_SOURCE_AUTHORITY_PUBLISHER_PUBLISH = (
        "rquant.runtime_serving_authority.ServingSourceAuthorityPublisher.publish"
    )
    READONLY_STRATEGY_RUNNER_SIGNAL_SOURCE_READ_BATCH = (
        "rquant.signal_router_runtime.ReadonlyStrategyRunnerSignalSource.read_batch"
    )
    ROUTE_RUNNER_SIGNALS = "rquant.signal_router_runtime.route_runner_signals"
    FILESYSTEM_RUNNER_SOURCE_READ_COMPLETED_BATCH = (
        "rquant.runtime_builder_shadow._FilesystemRunnerSource.read_completed_batch"
    )
    READ_ISOLATED_RUNNER_SHADOW_SNAPSHOT = (
        "rquant.runtime_shadow_sources.read_isolated_runner_shadow_snapshot"
    )
    ISOLATED_SIGNAL_OBSERVATIONS = "rquant.runtime_shadow_sources.isolated_signal_observations"
    READONLY_SIGNAL_ROUTE_SPOOL_ROUTED_AFTER_GLOBAL_SEQUENCE = (
        "rquant.signal_route_spool.ReadonlySignalRouteSpool.routed_after_global_sequence"
    )
    NOTIFICATION_STATE_STORE_REPLICATE = (
        "rquant.notification_state.NotificationStateStore.replicate"
    )
    READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE = (
        "rquant.signal_route_spool.ReadonlySignalRouteSpool.signals_after_global_sequence"
    )
    CONSUME_SIGNAL_BUS_TO_PAPER = "rquant.paper_signal_consumer.consume_signal_bus_to_paper"
    PAPER_SIGNAL_QUEUE_STORE_INGEST = "rquant.paper_signal_worker.PaperSignalQueueStore.ingest"
    SERVING_SOURCE_AUTHORITY_READER_CALL = (
        "rquant.runtime_serving_authority.ServingSourceAuthorityReader.__call__"
    )
    SERVING_SNAPSHOT_ASSEMBLER_ASSEMBLE = (
        "rquant.runtime_serving_snapshot.ServingSnapshotAssembler.assemble"
    )
    BUILD_SERVING_READ_MODELS = "rquant.serving_read_models.build_serving_read_models"


# Producer surfaces prove transport production. They never count as a reader receipt.
PRODUCER_SURFACES: Mapping[str, tuple[SurfaceId, ...]] = MappingProxyType(
    {
        "strategy-router": (SurfaceId.STRATEGY_RUNNER_PROCESS_BATCH,),
        "strategy-shadow": (
            SurfaceId.STRATEGY_RUNNER_PROCESS_BATCH,
            SurfaceId.STRATEGY_RUNNER_PUBLISH_SESSION_CLOSE_RECEIPT,
        ),
        "router-notifier": (
            SurfaceId.SIGNAL_ROUTE_SPOOL_PUBLISH,
            SurfaceId.PUBLISH_SIGNAL_BUS_PREFIX,
        ),
        "router-paper": (
            SurfaceId.SIGNAL_ROUTE_SPOOL_PUBLISH,
            SurfaceId.PUBLISH_SIGNAL_BUS_PREFIX,
        ),
        "notifier-serving": (
            SurfaceId.PUBLISH_SIGNAL_AUTHORITY,
            SurfaceId.SERVING_SOURCE_AUTHORITY_PUBLISHER_PUBLISH,
        ),
    }
)

# Reader surfaces are the code exercised for that pair's one verifier-issued receipt.
READER_SURFACES: Mapping[str, tuple[SurfaceId, ...]] = MappingProxyType(
    {
        "strategy-router": (
            SurfaceId.READONLY_STRATEGY_RUNNER_SIGNAL_SOURCE_READ_BATCH,
            SurfaceId.ROUTE_RUNNER_SIGNALS,
        ),
        "strategy-shadow": (
            SurfaceId.FILESYSTEM_RUNNER_SOURCE_READ_COMPLETED_BATCH,
            SurfaceId.READ_ISOLATED_RUNNER_SHADOW_SNAPSHOT,
            SurfaceId.ISOLATED_SIGNAL_OBSERVATIONS,
        ),
        "router-notifier": (
            SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_ROUTED_AFTER_GLOBAL_SEQUENCE,
            SurfaceId.NOTIFICATION_STATE_STORE_REPLICATE,
        ),
        "router-paper": (
            SurfaceId.READONLY_SIGNAL_ROUTE_SPOOL_SIGNALS_AFTER_GLOBAL_SEQUENCE,
            SurfaceId.CONSUME_SIGNAL_BUS_TO_PAPER,
            SurfaceId.PAPER_SIGNAL_QUEUE_STORE_INGEST,
        ),
        "notifier-serving": (
            SurfaceId.SERVING_SOURCE_AUTHORITY_READER_CALL,
            SurfaceId.SERVING_SNAPSHOT_ASSEMBLER_ASSEMBLE,
            SurfaceId.BUILD_SERVING_READ_MODELS,
        ),
    }
)

PAIR_SURFACES: Mapping[str, tuple[SurfaceId, ...]] = MappingProxyType(
    {
        pair_id: tuple(
            sorted(
                {*PRODUCER_SURFACES[pair_id], *READER_SURFACES[pair_id]},
                key=lambda surface: surface.value,
            )
        )
        for pair_id in PAIR_IDS
    }
)

# The five singleton kinds of the pair map. Missing or duplicate resolutions reject.
SINGLETON_PAIR_KINDS: tuple[RuntimeServiceKind, ...] = (
    RuntimeServiceKind.SIGNAL_ROUTER,
    RuntimeServiceKind.SHADOW_SESSION,
    RuntimeServiceKind.NOTIFIER,
    RuntimeServiceKind.PAPER_BROKER,
    RuntimeServiceKind.SERVING_PUBLISHER,
)


def _require_exact_instance(value: object, expected: type, *, field: str) -> Any:
    if type(value) is not expected:
        raise TypeError(f"{field} requires an exact {expected.__name__} object")
    return value


def _require_exact_items(value: object, expected: type, *, field: str) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(value, Sequence):
        raise TypeError(f"{field} requires an exact tuple of {expected.__name__} objects")
    for item in value:
        _require_exact_instance(item, expected, field=field)
    return tuple(value)


def _require_sorted_unique(values: tuple[str, ...], *, field: str) -> tuple[str, ...]:
    if not values:
        raise ValueError(f"{field} must be nonempty")
    if any(type(item) is not str or not item for item in values):
        raise ValueError(f"{field} cannot contain an empty identifier")
    if tuple(sorted(set(values))) != values:
        raise ValueError(f"{field} must be sorted and duplicate-free")
    return values


def _decode_object(payload: bytes | str) -> dict[str, Any]:
    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    try:
        decoded = strict_canonical_json_loads(raw)
    except StrictJsonError as exc:
        raise SignalFamilyVerificationError(str(exc)) from exc
    if type(decoded) is not dict:
        raise SignalFamilyVerificationError("verification payload must be a JSON object")
    return decoded


def _tuple_item_type(annotation: Any) -> Any:
    args = get_args(annotation)
    return args[0] if args else None


def _enum_member(annotation: Any, value: Any) -> Any:
    """Map an exact JSON string onto its closed enum member; coerce nothing else."""

    candidates: tuple[Any, ...]
    if get_origin(annotation) in (Union, types.UnionType):
        candidates = get_args(annotation)
    else:
        candidates = (annotation,)
    for candidate in candidates:
        if isinstance(candidate, type) and issubclass(candidate, Enum) and type(value) is str:
            try:
                return candidate(value)
            except ValueError:
                return value
    return value


class _PhaseCStrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=False,
        revalidate_instances="always",
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_frozen_shapes(cls, value: object) -> object:
        """Accept the JSON array/string forms of tuples and closed enums, nothing else."""

        if not isinstance(value, dict):
            raise TypeError(f"{cls.__name__} must be built from an object")
        values = dict(value)
        for name, field in cls.model_fields.items():
            if name not in values:
                continue
            annotation = field.annotation
            item = values[name]
            if get_origin(annotation) is tuple:
                member = _tuple_item_type(annotation)
                if type(item) is list:
                    item = tuple(item)
                if isinstance(member, type) and issubclass(member, BaseModel):
                    item = _require_exact_items(item, member, field=name)
                elif isinstance(member, type) and issubclass(member, Enum):
                    item = tuple(_enum_member(member, entry) for entry in item)
                values[name] = item
                continue
            values[name] = _enum_member(annotation, item)
        return values


def canonical_timestamp(value: datetime) -> str:
    """One ISO-8601 UTC microsecond string; hash preimages never carry raw seconds."""

    if type(value) is not datetime:
        raise TypeError("timestamp must be an exact datetime object")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise SignalFamilyVerificationError("timestamp must be timezone-aware UTC")
    return f"{value.strftime('%Y-%m-%dT%H:%M:%S')}.{value.microsecond:06d}Z"


def parse_canonical_timestamp(value: str) -> datetime:
    if type(value) is not str:
        raise TypeError("timestamp must be an exact string")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as exc:
        raise SignalFamilyVerificationError("timestamp is not a canonical UTC string") from exc
    return parsed.replace(tzinfo=UTC)


# --------------------------------------------------------------------------------------
# Pair map resolved from the validated target production profile
# --------------------------------------------------------------------------------------


class PairBindingV1(_PhaseCStrictModel):
    """One frozen pair row with its exact resolved producer and consumer service IDs."""

    pair_id: PairIdField
    producer_service_ids: tuple[StrictStr, ...]
    consumer_service_ids: tuple[StrictStr, ...]

    @model_validator(mode="after")
    def validate_sides(self) -> Self:
        _require_sorted_unique(self.producer_service_ids, field="producer_service_ids")
        _require_sorted_unique(self.consumer_service_ids, field="consumer_service_ids")
        return self

    @property
    def producer_surface_ids(self) -> tuple[SurfaceId, ...]:
        return PRODUCER_SURFACES[self.pair_id]

    @property
    def reader_surface_ids(self) -> tuple[SurfaceId, ...]:
        return READER_SURFACES[self.pair_id]


def resolve_pair_bindings(
    manifests: Sequence[RuntimeServiceManifest],
) -> tuple[PairBindingV1, ...]:
    """Derive the exact five pair rows from the validated target production profile."""

    resolved = _require_exact_items(manifests, RuntimeServiceManifest, field="profile manifests")
    service_ids = [manifest.service_id for manifest in resolved]
    if len(set(service_ids)) != len(service_ids):
        raise SignalFamilyVerificationError("profile declares a duplicate service id")
    strategies = tuple(
        sorted(
            manifest.service_id
            for manifest in resolved
            if manifest.service_kind is RuntimeServiceKind.STRATEGY_LIVE
        )
    )
    if not strategies:
        raise SignalFamilyVerificationError("profile strategy_live services must be nonempty")
    singletons: dict[RuntimeServiceKind, str] = {}
    for kind in SINGLETON_PAIR_KINDS:
        matches = tuple(
            manifest.service_id for manifest in resolved if manifest.service_kind is kind
        )
        if len(matches) != 1:
            raise SignalFamilyVerificationError(
                f"profile must declare exactly one {kind.value} service"
            )
        singletons[kind] = matches[0]
    rows = {
        "strategy-router": (strategies, (singletons[RuntimeServiceKind.SIGNAL_ROUTER],)),
        "strategy-shadow": (strategies, (singletons[RuntimeServiceKind.SHADOW_SESSION],)),
        "router-notifier": (
            (singletons[RuntimeServiceKind.SIGNAL_ROUTER],),
            (singletons[RuntimeServiceKind.NOTIFIER],),
        ),
        "router-paper": (
            (singletons[RuntimeServiceKind.SIGNAL_ROUTER],),
            (singletons[RuntimeServiceKind.PAPER_BROKER],),
        ),
        "notifier-serving": (
            (singletons[RuntimeServiceKind.NOTIFIER],),
            (singletons[RuntimeServiceKind.SERVING_PUBLISHER],),
        ),
    }
    return tuple(
        PairBindingV1(
            pair_id=pair_id,
            producer_service_ids=rows[pair_id][0],
            consumer_service_ids=rows[pair_id][1],
        )
        for pair_id in PAIR_IDS
    )


def require_exact_five_pairs(pairs: Sequence[PairBindingV1]) -> tuple[PairBindingV1, ...]:
    resolved = _require_exact_items(pairs, PairBindingV1, field="pairs")
    if tuple(pair.pair_id for pair in resolved) != PAIR_IDS:
        raise SignalFamilyVerificationError(
            "pairs must be exactly the five frozen pair ids in sorted order"
        )
    return resolved


def participating_service_ids(pairs: Sequence[PairBindingV1]) -> tuple[str, ...]:
    """The sorted unique union of every producer and consumer ID in the five rows."""

    resolved = require_exact_five_pairs(pairs)
    union: set[str] = set()
    for pair in resolved:
        union.update(pair.producer_service_ids)
        union.update(pair.consumer_service_ids)
    return tuple(sorted(union))


def require_pair_derived_participants(
    pairs: Sequence[PairBindingV1],
    claimed: tuple[str, ...],
) -> tuple[str, ...]:
    """Reject a handwritten subset, static list, count, or kind in place of the union."""

    derived = participating_service_ids(pairs)
    if type(claimed) is not tuple or claimed != derived:
        raise SignalFamilyVerificationError("participating service ids are not pair-derived")
    return derived


def expected_surface_ids(
    pairs: Sequence[PairBindingV1],
) -> Mapping[str, tuple[SurfaceId, ...]]:
    """The exact surface tuple each participating service owns under the frozen pair map."""

    resolved = require_exact_five_pairs(pairs)
    assigned: dict[str, set[SurfaceId]] = {}
    for pair in resolved:
        for service_id in pair.producer_service_ids:
            assigned.setdefault(service_id, set()).update(PRODUCER_SURFACES[pair.pair_id])
        for service_id in pair.consumer_service_ids:
            assigned.setdefault(service_id, set()).update(READER_SURFACES[pair.pair_id])
    return MappingProxyType(
        {
            service_id: tuple(sorted(surfaces, key=lambda surface: surface.value))
            for service_id, surfaces in sorted(assigned.items())
        }
    )


def require_pair_derived_surfaces(
    pairs: Sequence[PairBindingV1],
    service_id: str,
    claimed: tuple[SurfaceId, ...],
) -> tuple[SurfaceId, ...]:
    """Reject an omitted or additional surface for one participating service."""

    expected = expected_surface_ids(pairs).get(service_id)
    if expected is None:
        raise SignalFamilyVerificationError(
            "surface ids are not the exact pair-derived set for a nonparticipating service"
        )
    if type(claimed) is not tuple or tuple(claimed) != expected:
        raise SignalFamilyVerificationError("surface ids are not the exact pair-derived set")
    return expected


# --------------------------------------------------------------------------------------
# `VerificationServiceBindingV1`
# --------------------------------------------------------------------------------------


def _validate_relative_source_path(value: str) -> str:
    if type(value) is not str or not value:
        raise SignalFamilyVerificationError("executable source path must be a nonempty string")
    if "\\" in value:
        raise SignalFamilyVerificationError(
            "executable source path cannot use an alternate separator"
        )
    path = PurePosixPath(value)
    if path.is_absolute():
        raise SignalFamilyVerificationError("executable source path cannot be absolute")
    parts = value.split("/")
    if any(part in ("", ".", "..") for part in parts):
        raise SignalFamilyVerificationError(
            "executable source path cannot contain an empty, dot, or parent component"
        )
    if "/".join(parts) != value:  # pragma: no cover - split/join is total for str
        raise SignalFamilyVerificationError("executable source path is not normalized")
    return value


class VerificationServiceBindingV1(_PhaseCStrictModel):
    """`RESET-REG-P1-02`: one root-policy-approved service-to-runtime-role binding."""

    service_id: StrictStr = Field(min_length=1)
    runtime_service_kind: RuntimeServiceKind
    role_name: StrictStr = Field(pattern=_ROLE_NAME)
    service_manifest_fingerprint: Sha256
    executable_module: StrictStr = Field(pattern=_MODULE_NAME)
    executable_source_relative_path: StrictStr
    executable_source_sha256: Sha256
    surface_ids: tuple[SurfaceId, ...]
    binding_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        _validate_relative_source_path(self.executable_source_relative_path)
        if not self.surface_ids:
            raise SignalFamilyVerificationError("surface_ids must be nonempty")
        values = tuple(surface.value for surface in self.surface_ids)
        if values != tuple(sorted(set(values))):
            raise SignalFamilyVerificationError("surface_ids must be sorted and duplicate-free")
        if self.binding_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"binding_hash"})
        ):
            raise SignalFamilyVerificationError(
                "binding hash does not match its canonical content"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        service_id: str,
        runtime_service_kind: RuntimeServiceKind,
        role_name: str,
        service_manifest_fingerprint: str,
        executable_module: str,
        executable_source_relative_path: str,
        executable_source_sha256: str,
        surface_ids: tuple[SurfaceId, ...],
    ) -> Self:
        values: dict[str, Any] = {
            "service_id": service_id,
            "runtime_service_kind": runtime_service_kind,
            "role_name": role_name,
            "service_manifest_fingerprint": service_manifest_fingerprint,
            "executable_module": executable_module,
            "executable_source_relative_path": executable_source_relative_path,
            "executable_source_sha256": executable_source_sha256,
            "surface_ids": tuple(surface_ids),
        }
        preimage = dict(values)
        preimage["runtime_service_kind"] = (
            runtime_service_kind.value
            if isinstance(runtime_service_kind, RuntimeServiceKind)
            else runtime_service_kind
        )
        preimage["surface_ids"] = [
            surface.value if isinstance(surface, SurfaceId) else surface
            for surface in values["surface_ids"]
        ]
        return cls(**values, binding_hash=canonical_sha256(preimage))


def service_bindings_hash(bindings: Sequence[VerificationServiceBindingV1]) -> str:
    """`canonical_sha256` of the complete full-model dump of the sorted binding tuple."""

    resolved = _require_exact_items(
        bindings,
        VerificationServiceBindingV1,
        field="service_bindings",
    )
    return canonical_sha256([binding.model_dump(mode="json") for binding in resolved])


def validate_service_bindings(
    bindings: Sequence[VerificationServiceBindingV1],
    participating: tuple[str, ...],
) -> tuple[VerificationServiceBindingV1, ...]:
    """Sorted, duplicate-free, and covering exactly the pair-derived participant set."""

    resolved = _require_exact_items(
        bindings,
        VerificationServiceBindingV1,
        field="service_bindings",
    )
    if not resolved:
        raise SignalFamilyVerificationError("service bindings must be nonempty")
    service_ids = tuple(binding.service_id for binding in resolved)
    if list(service_ids) != sorted(service_ids):
        raise SignalFamilyVerificationError("service bindings must be sorted by service_id")
    hashes = tuple(binding.binding_hash for binding in resolved)
    if len(set(hashes)) != len(hashes):
        raise SignalFamilyVerificationError(
            "service bindings contain a duplicate service binding hash"
        )
    if len(set(service_ids)) != len(service_ids):
        raise SignalFamilyVerificationError("service bindings contain a duplicate service id")
    if service_ids != tuple(participating):
        raise SignalFamilyVerificationError(
            "service bindings must cover exactly the participating service ids"
        )
    return resolved


# --------------------------------------------------------------------------------------
# Fixed root-owned release verification policy
# --------------------------------------------------------------------------------------


class ReleaseVerificationEntryV1(_PhaseCStrictModel):
    """One exact release the external root policy authorizes."""

    successor_bundle_content_hash: Sha256
    overlay_content_hash: Sha256
    verification_manifest_sha256: Sha256
    vector_set_hash: Sha256
    expected_result_set_hash: Sha256
    five_pair_service_binding_set_hash: Sha256
    verifier_policy_max_age_seconds: StrictInt | None
    entry_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        cap = self.verifier_policy_max_age_seconds
        if cap is not None and cap < 1:
            raise SignalFamilyVerificationError(
                "verifier_policy_max_age_seconds must be a positive integer"
            )
        if self.entry_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"entry_hash"})
        ):
            raise SignalFamilyVerificationError("entry hash does not match its canonical content")
        return self

    @property
    def release_key(self) -> tuple[str, str]:
        return (self.successor_bundle_content_hash, self.overlay_content_hash)

    @classmethod
    def create(
        cls,
        *,
        successor_bundle_content_hash: str,
        overlay_content_hash: str,
        verification_manifest_sha256: str,
        vector_set_hash: str,
        expected_result_set_hash: str,
        five_pair_service_binding_set_hash: str,
        verifier_policy_max_age_seconds: int | None,
    ) -> Self:
        values: dict[str, Any] = {
            "successor_bundle_content_hash": successor_bundle_content_hash,
            "overlay_content_hash": overlay_content_hash,
            "verification_manifest_sha256": verification_manifest_sha256,
            "vector_set_hash": vector_set_hash,
            "expected_result_set_hash": expected_result_set_hash,
            "five_pair_service_binding_set_hash": five_pair_service_binding_set_hash,
            "verifier_policy_max_age_seconds": verifier_policy_max_age_seconds,
        }
        return cls(**values, entry_hash=canonical_sha256(values))

    @classmethod
    def _from_decoded(cls, decoded: dict[str, Any]) -> Self:
        return cls.model_validate(decoded)


class SignalFamilyVerifierPolicyV1(_PhaseCStrictModel):
    """`RESET-REG-P0-01`: the externally installed fixed root-owned release policy."""

    schema_version: StrictInt
    verifier_policy_id: Literal["signal-family-verifier-policy-v1"]
    harness_identity: Literal[
        "/usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"
    ]
    harness_sha256: Sha256
    release_entries: tuple[ReleaseVerificationEntryV1, ...]
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.schema_version != 1:
            raise SignalFamilyVerificationError("unsupported verifier policy schema version")
        if not self.release_entries:
            raise SignalFamilyVerificationError("release entries must be nonempty")
        keys = tuple(entry.release_key for entry in self.release_entries)
        if list(keys) != sorted(keys):
            raise SignalFamilyVerificationError(
                "release entries must be sorted by successor bundle and overlay hash"
            )
        if len(set(keys)) != len(keys):
            raise SignalFamilyVerificationError("release entries contain a duplicate release key")
        hashes = tuple(entry.entry_hash for entry in self.release_entries)
        if len(set(hashes)) != len(hashes):
            raise SignalFamilyVerificationError("release entries contain a duplicate entry hash")
        if self.content_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"content_hash"})
        ):
            raise SignalFamilyVerificationError(
                "policy content hash does not match its canonical content"
            )
        return self

    def select_entry(
        self,
        *,
        successor_bundle_content_hash: str,
        overlay_content_hash: str,
    ) -> ReleaseVerificationEntryV1:
        key = (successor_bundle_content_hash, overlay_content_hash)
        matches = tuple(entry for entry in self.release_entries if entry.release_key == key)
        if not matches:
            raise SignalFamilyVerificationError("no release entry matches the exact release key")
        if len(matches) != 1:  # pragma: no cover - duplicate keys reject at construction
            raise SignalFamilyVerificationError("multiple release entries match one release key")
        return matches[0]

    @classmethod
    def create(
        cls,
        *,
        harness_sha256: str,
        release_entries: tuple[ReleaseVerificationEntryV1, ...],
    ) -> Self:
        _require_exact_items(release_entries, ReleaseVerificationEntryV1, field="release_entries")
        values: dict[str, Any] = {
            "schema_version": 1,
            "verifier_policy_id": VERIFIER_POLICY_ID,
            "harness_identity": HARNESS_IDENTITY,
            "harness_sha256": harness_sha256,
            "release_entries": tuple(release_entries),
        }
        preimage = dict(values)
        preimage["release_entries"] = [
            entry.model_dump(mode="json") for entry in release_entries
        ]
        return cls(**values, content_hash=canonical_sha256(preimage))

    @classmethod
    def from_canonical_json(cls, payload: bytes | str) -> Self:
        decoded = _decode_object(payload)
        values = dict(decoded)
        raw_entries = values.get("release_entries")
        if raw_entries is not None:
            if type(raw_entries) is not list:
                raise SignalFamilyVerificationError("release_entries must be a JSON array")
            entries = []
            for item in raw_entries:
                if type(item) is not dict:
                    raise SignalFamilyVerificationError(
                        "release_entries must contain exact entry objects"
                    )
                entries.append(ReleaseVerificationEntryV1._from_decoded(item))
            values["release_entries"] = tuple(entries)
        return cls.model_validate(values)


def verifier_policy_canonical_json_bytes(policy: SignalFamilyVerifierPolicyV1) -> bytes:
    _require_exact_instance(policy, SignalFamilyVerifierPolicyV1, field="policy")
    return canonical_json_bytes(policy.model_dump(mode="json"))


# --------------------------------------------------------------------------------------
# Immutable in-generation verification and test manifests
# --------------------------------------------------------------------------------------


class SignalFamilyVectorV1(_PhaseCStrictModel):
    """A frozen vector identity plus its input bytes. Expected results are forbidden."""

    vector_id: Sha256
    pair_id: PairIdField
    family_id: StrictStr = Field(min_length=1)
    surface_id: SurfaceId
    input_json: StrictStr

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if not self.input_json:
            raise SignalFamilyVerificationError("input_json must be nonempty")
        raw = self.input_json.encode("utf-8")
        if len(raw) > MAX_VECTOR_INPUT_BYTES:
            raise SignalFamilyVerificationError(
                f"input_json exceeds {MAX_VECTOR_INPUT_BYTES} bytes"
            )
        try:
            strict_canonical_json_loads(raw)
        except StrictJsonError as exc:
            raise SignalFamilyVerificationError(str(exc)) from exc
        if self.family_id not in ACCEPTED_FAMILY_IDS:
            raise SignalFamilyVerificationError("family id is not a current-family id")
        if self.surface_id not in PAIR_SURFACES[self.pair_id]:
            raise SignalFamilyVerificationError("vector surface is not bound to that pair")
        if self.vector_id != canonical_sha256(
            self.model_dump(mode="json", exclude={"vector_id"})
        ):
            raise SignalFamilyVerificationError("vector id does not match its canonical content")
        return self

    @classmethod
    def create(
        cls,
        *,
        pair_id: str,
        family_id: str,
        surface_id: SurfaceId,
        input_json: str,
    ) -> Self:
        values: dict[str, Any] = {
            "pair_id": pair_id,
            "family_id": family_id,
            "surface_id": surface_id,
            "input_json": input_json,
        }
        preimage = dict(values)
        preimage["surface_id"] = (
            surface_id.value if isinstance(surface_id, SurfaceId) else surface_id
        )
        return cls(**values, vector_id=canonical_sha256(preimage))

    @classmethod
    def _from_decoded(cls, decoded: dict[str, Any]) -> Self:
        return cls.model_validate(decoded)


class SignalFamilyExpectedResultV1(_PhaseCStrictModel):
    """The expected result of one vector. It never travels to the child."""

    vector_id: Sha256
    canonical_result_sha256: Sha256

    @classmethod
    def _from_decoded(cls, decoded: dict[str, Any]) -> Self:
        return cls.model_validate(decoded)


def vector_set_hash(vectors: Sequence[SignalFamilyVectorV1]) -> str:
    resolved = _require_exact_items(vectors, SignalFamilyVectorV1, field="vectors")
    if not resolved:
        raise SignalFamilyVerificationError("vectors must be nonempty")
    ids = tuple(vector.vector_id for vector in resolved)
    if list(ids) != sorted(set(ids)):
        raise SignalFamilyVerificationError(
            "vectors must be sorted by vector_id and duplicate-free"
        )
    return canonical_sha256(
        {"vectors": [vector.model_dump(mode="json") for vector in resolved]}
    )


def _expected_result_preimage(rows: Sequence[Any]) -> dict[str, Any]:
    return {
        "expected_results": [
            {
                "vector_id": row.vector_id,
                "canonical_result_sha256": row.canonical_result_sha256,
            }
            for row in rows
        ]
    }


def expected_result_set_hash(results: Sequence[SignalFamilyExpectedResultV1]) -> str:
    resolved = _require_exact_items(
        results,
        SignalFamilyExpectedResultV1,
        field="expected_results",
    )
    if not resolved:
        raise SignalFamilyVerificationError("expected results must be nonempty")
    ids = tuple(result.vector_id for result in resolved)
    if list(ids) != sorted(set(ids)):
        raise SignalFamilyVerificationError(
            "expected results must be sorted by vector_id and duplicate-free"
        )
    return canonical_sha256(_expected_result_preimage(resolved))


def five_pair_service_binding_set_hash(
    pairs: Sequence[PairBindingV1],
    bindings: Sequence[VerificationServiceBindingV1],
) -> str:
    resolved_pairs = require_exact_five_pairs(pairs)
    resolved_bindings = _require_exact_items(
        bindings,
        VerificationServiceBindingV1,
        field="service_bindings",
    )
    return canonical_sha256(
        {
            "pairs": [
                {
                    "pair_id": pair.pair_id,
                    "producer_service_ids": list(pair.producer_service_ids),
                    "consumer_service_ids": list(pair.consumer_service_ids),
                }
                for pair in resolved_pairs
            ],
            "service_bindings": [
                binding.model_dump(mode="json") for binding in resolved_bindings
            ],
        }
    )


class SignalFamilyTestManifestV1(_PhaseCStrictModel):
    """The immutable in-generation test manifest the root policy authorizes by hash."""

    schema_version: StrictInt
    vectors: tuple[SignalFamilyVectorV1, ...]
    expected_results: tuple[SignalFamilyExpectedResultV1, ...]
    pairs: tuple[PairBindingV1, ...]
    service_bindings: tuple[VerificationServiceBindingV1, ...]
    service_bindings_hash: Sha256
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.schema_version != 1:
            raise SignalFamilyVerificationError("unsupported test manifest schema version")
        vector_ids = tuple(vector.vector_id for vector in self.vectors)
        if not vector_ids:
            raise SignalFamilyVerificationError("vectors must be nonempty")
        if list(vector_ids) != sorted(set(vector_ids)):
            raise SignalFamilyVerificationError(
                "vectors must be sorted by vector_id and duplicate-free"
            )
        result_ids = tuple(result.vector_id for result in self.expected_results)
        if list(result_ids) != sorted(set(result_ids)):
            raise SignalFamilyVerificationError(
                "expected results must be sorted by vector_id and duplicate-free"
            )
        if result_ids != vector_ids:
            raise SignalFamilyVerificationError(
                "expected results must pair one to one with vectors"
            )
        pairs = require_exact_five_pairs(self.pairs)
        participating = participating_service_ids(pairs)
        validate_service_bindings(self.service_bindings, participating)
        for binding in self.service_bindings:
            require_pair_derived_surfaces(pairs, binding.service_id, binding.surface_ids)
        if self.service_bindings_hash != service_bindings_hash(self.service_bindings):
            raise SignalFamilyVerificationError(
                "service bindings hash does not match its canonical content"
            )
        if self.content_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"content_hash"})
        ):
            raise SignalFamilyVerificationError(
                "test manifest content hash does not match its canonical content"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        vectors: tuple[SignalFamilyVectorV1, ...],
        expected_results: tuple[SignalFamilyExpectedResultV1, ...],
        pairs: tuple[PairBindingV1, ...],
        service_bindings: tuple[VerificationServiceBindingV1, ...],
    ) -> Self:
        values: dict[str, Any] = {
            "schema_version": 1,
            "vectors": tuple(vectors),
            "expected_results": tuple(expected_results),
            "pairs": tuple(pairs),
            "service_bindings": tuple(service_bindings),
            "service_bindings_hash": service_bindings_hash(service_bindings),
        }
        preimage = dict(values)
        for name in ("vectors", "expected_results", "pairs", "service_bindings"):
            preimage[name] = [item.model_dump(mode="json") for item in values[name]]
        return cls(**values, content_hash=canonical_sha256(preimage))


def test_manifest_canonical_json_bytes(manifest: SignalFamilyTestManifestV1) -> bytes:
    _require_exact_instance(manifest, SignalFamilyTestManifestV1, field="test manifest")
    return canonical_json_bytes(manifest.model_dump(mode="json"))


class SignalFamilyVerificationManifestV1(_PhaseCStrictModel):
    """The immutable in-generation verification manifest matched by the external policy."""

    schema_version: StrictInt
    successor_bundle_content_hash: Sha256
    overlay_content_hash: Sha256
    test_manifest_sha256: Sha256
    vector_set_hash: Sha256
    expected_result_set_hash: Sha256
    five_pair_service_binding_set_hash: Sha256
    content_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.schema_version != 1:
            raise SignalFamilyVerificationError(
                "unsupported verification manifest schema version"
            )
        if self.content_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"content_hash"})
        ):
            raise SignalFamilyVerificationError(
                "verification manifest content hash does not match its canonical content"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        successor_bundle_content_hash: str,
        overlay_content_hash: str,
        test_manifest_sha256: str,
        test_manifest: SignalFamilyTestManifestV1,
    ) -> Self:
        _require_exact_instance(test_manifest, SignalFamilyTestManifestV1, field="test manifest")
        values: dict[str, Any] = {
            "schema_version": 1,
            "successor_bundle_content_hash": successor_bundle_content_hash,
            "overlay_content_hash": overlay_content_hash,
            "test_manifest_sha256": test_manifest_sha256,
            "vector_set_hash": vector_set_hash(test_manifest.vectors),
            "expected_result_set_hash": expected_result_set_hash(test_manifest.expected_results),
            "five_pair_service_binding_set_hash": five_pair_service_binding_set_hash(
                test_manifest.pairs,
                test_manifest.service_bindings,
            ),
        }
        return cls(**values, content_hash=canonical_sha256(values))


def verification_manifest_canonical_json_bytes(
    manifest: SignalFamilyVerificationManifestV1,
) -> bytes:
    _require_exact_instance(
        manifest,
        SignalFamilyVerificationManifestV1,
        field="verification manifest",
    )
    return canonical_json_bytes(manifest.model_dump(mode="json"))


# --------------------------------------------------------------------------------------
# Bounded child result
# --------------------------------------------------------------------------------------


class SignalFamilyVectorResultV1(_PhaseCStrictModel):
    """One bounded canonical result the child returns under a frozen surface ID."""

    vector_id: Sha256
    pair_id: PairIdField
    family_id: StrictStr = Field(min_length=1)
    surface_id: SurfaceId
    canonical_result_json: StrictStr
    canonical_result_sha256: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if not self.canonical_result_json:
            raise SignalFamilyVerificationError("canonical_result_json must be nonempty")
        raw = self.canonical_result_json.encode("utf-8")
        if len(raw) > MAX_CANONICAL_RESULT_BYTES:
            raise SignalFamilyVerificationError(
                f"canonical_result_json exceeds {MAX_CANONICAL_RESULT_BYTES} bytes"
            )
        try:
            strict_canonical_json_loads(raw)
        except StrictJsonError as exc:
            raise SignalFamilyVerificationError(str(exc)) from exc
        if self.family_id not in ACCEPTED_FAMILY_IDS:
            raise SignalFamilyVerificationError("family id is not a current-family id")
        if self.surface_id not in PAIR_SURFACES[self.pair_id]:
            raise SignalFamilyVerificationError("result surface is not bound to that pair")
        if self.canonical_result_sha256 != hashlib.sha256(raw).hexdigest():
            raise SignalFamilyVerificationError(
                "canonical_result_sha256 does not hash its own bytes"
            )
        return self

    @property
    def result_sort_key(self) -> tuple[str, str, str, str]:
        return (self.pair_id, self.family_id, self.surface_id.value, self.vector_id)

    @classmethod
    def create(
        cls,
        *,
        vector_id: str,
        pair_id: str,
        family_id: str,
        surface_id: SurfaceId,
        canonical_result_json: str,
    ) -> Self:
        return cls(
            vector_id=vector_id,
            pair_id=pair_id,
            family_id=family_id,
            surface_id=surface_id,
            canonical_result_json=canonical_result_json,
            canonical_result_sha256=hashlib.sha256(
                canonical_result_json.encode("utf-8")
            ).hexdigest(),
        )

    @classmethod
    def _from_decoded(cls, decoded: dict[str, Any]) -> Self:
        return cls.model_validate(decoded)


class SignalFamilyChildResultV1(_PhaseCStrictModel):
    """The one bounded canonical IPC response the unprivileged child emits."""

    schema_version: StrictInt
    run_id: Sha256
    test_manifest_hash: Sha256
    vector_results: tuple[SignalFamilyVectorResultV1, ...]
    result_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.schema_version != 1:
            raise SignalFamilyVerificationError("unsupported child result schema version")
        if not self.vector_results:
            raise SignalFamilyVerificationError("vector results must be nonempty")
        keys = [result.result_sort_key for result in self.vector_results]
        if keys != sorted(set(keys)):
            raise SignalFamilyVerificationError(
                "vector results must be sorted by pair, family, surface, vector and duplicate-free"
            )
        if self.result_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"result_hash"})
        ):
            raise SignalFamilyVerificationError(
                "child result hash does not match its canonical content"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        test_manifest_hash: str,
        vector_results: Sequence[SignalFamilyVectorResultV1],
    ) -> Self:
        resolved = _require_exact_items(
            vector_results,
            SignalFamilyVectorResultV1,
            field="vector_results",
        )
        ordered = tuple(sorted(resolved, key=lambda result: result.result_sort_key))
        values: dict[str, Any] = {
            "schema_version": 1,
            "run_id": run_id,
            "test_manifest_hash": test_manifest_hash,
            "vector_results": ordered,
        }
        preimage = dict(values)
        preimage["vector_results"] = [result.model_dump(mode="json") for result in ordered]
        return cls(**values, result_hash=canonical_sha256(preimage))

    @classmethod
    def from_canonical_ipc_bytes(cls, payload: bytes, *, max_vector_count: int) -> Self:
        if type(payload) is not bytes:
            raise TypeError("IPC response must be exact bytes")
        if type(max_vector_count) is not int or max_vector_count < 1:
            raise SignalFamilyVerificationError("max_vector_count must be a positive integer")
        if len(payload) > MAX_IPC_RESPONSE_BYTES:
            raise SignalFamilyVerificationError(
                f"IPC response exceeds {MAX_IPC_RESPONSE_BYTES} bytes"
            )
        decoded = _decode_object(payload)
        values = dict(decoded)
        raw_results = values.get("vector_results")
        if raw_results is not None:
            if type(raw_results) is not list:
                raise SignalFamilyVerificationError("vector_results must be a JSON array")
            if len(raw_results) > max_vector_count:
                raise SignalFamilyVerificationError(
                    "vector results exceed the test manifest vector count"
                )
            results = []
            for item in raw_results:
                if type(item) is not dict:
                    raise SignalFamilyVerificationError(
                        "vector_results must contain exact result objects"
                    )
                results.append(SignalFamilyVectorResultV1._from_decoded(item))
            values["vector_results"] = tuple(results)
        return cls.model_validate(values)


def child_result_canonical_json_bytes(result: SignalFamilyChildResultV1) -> bytes:
    _require_exact_instance(result, SignalFamilyChildResultV1, field="child result")
    return canonical_json_bytes(result.model_dump(mode="json"))


def observed_result_set_hash(results: Sequence[SignalFamilyVectorResultV1]) -> str:
    """The actual result-set hash, recomputed with the expected-result preimage shape."""

    resolved = _require_exact_items(
        results,
        SignalFamilyVectorResultV1,
        field="vector_results",
    )
    ordered = tuple(sorted(resolved, key=lambda result: result.vector_id))
    ids = tuple(result.vector_id for result in ordered)
    if len(set(ids)) != len(ids):
        raise SignalFamilyVerificationError("vector results contain a duplicate vector_id")
    return canonical_sha256(_expected_result_preimage(ordered))


def observed_family_ids(results: Sequence[SignalFamilyVectorResultV1]) -> tuple[str, ...]:
    resolved = _require_exact_items(
        results,
        SignalFamilyVectorResultV1,
        field="vector_results",
    )
    return tuple(sorted({result.family_id for result in resolved}))


def observed_surface_ids(
    results: Sequence[SignalFamilyVectorResultV1],
) -> tuple[SurfaceId, ...]:
    resolved = _require_exact_items(
        results,
        SignalFamilyVectorResultV1,
        field="vector_results",
    )
    return tuple(
        sorted({result.surface_id for result in resolved}, key=lambda surface: surface.value)
    )


# --------------------------------------------------------------------------------------
# Authority epoch and execution evidence
# --------------------------------------------------------------------------------------


def authority_epoch_key(
    *,
    operation_id: str,
    sequence: int,
    generation_id: str,
    full_manifest_hash: str,
    profile_id: str,
) -> str:
    """The exact epoch preimage of authority.md L1479-1485."""

    return hashlib.sha256(
        canonical_json_bytes(
            {
                "operation_id": operation_id,
                "sequence": sequence,
                "generation_id": generation_id,
                "full_manifest_hash": full_manifest_hash,
                "profile_id": profile_id,
            }
        )
    ).hexdigest()


class AuthoritySnapshotV1(_PhaseCStrictModel):
    """The complete authority snapshot one verifier run binds. Identifiers only."""

    operation_id: StrictStr = Field(pattern=_OPERATION_ID)
    sequence: StrictInt
    authority_state: RuntimeAuthorityState
    generation_id: Sha256
    generation_lifecycle: RuntimeGenerationLifecycle
    full_manifest_hash: Sha256
    profile_id: Sha256
    role_names: tuple[StrictStr, ...]
    authority_epoch_key: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.sequence < 1:
            raise SignalFamilyVerificationError("authority sequence must be a positive integer")
        if self.generation_id != self.full_manifest_hash:
            raise SignalFamilyVerificationError(
                "generation identity differs from the full manifest hash"
            )
        _require_sorted_unique(self.role_names, field="role_names")
        if self.authority_epoch_key != authority_epoch_key(
            operation_id=self.operation_id,
            sequence=self.sequence,
            generation_id=self.generation_id,
            full_manifest_hash=self.full_manifest_hash,
            profile_id=self.profile_id,
        ):
            raise SignalFamilyVerificationError(
                "authority epoch key does not match its canonical preimage"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        operation_id: str,
        sequence: int,
        authority_state: RuntimeAuthorityState | str,
        generation_id: str,
        generation_lifecycle: RuntimeGenerationLifecycle | str,
        full_manifest_hash: str,
        profile_id: str,
        role_names: tuple[str, ...],
    ) -> Self:
        return cls(
            operation_id=operation_id,
            sequence=sequence,
            authority_state=authority_state,
            generation_id=generation_id,
            generation_lifecycle=generation_lifecycle,
            full_manifest_hash=full_manifest_hash,
            profile_id=profile_id,
            role_names=tuple(role_names),
            authority_epoch_key=authority_epoch_key(
                operation_id=operation_id,
                sequence=sequence,
                generation_id=generation_id,
                full_manifest_hash=full_manifest_hash,
                profile_id=profile_id,
            ),
        )


def execution_evidence_preimage(
    *,
    authority: AuthoritySnapshotV1,
    verification_manifest_sha256: str,
    test_manifest_hash: str,
    vector_set_hash: str,
    expected_result_set_hash: str,
    five_pair_service_binding_set_hash: str,
    child_result_hash: str,
    verifier_policy_id: str,
    verifier_policy_content_hash: str,
    selected_entry_hash: str,
    harness_identity: str,
    harness_sha256: str,
    observed_family_ids: tuple[str, ...],
    observed_surface_ids: tuple[SurfaceId, ...],
    service_manifest_fingerprints: tuple[str, ...],
) -> dict[str, Any]:
    """The canonical execution-evidence preimage; its own hash is never an input."""

    _require_exact_instance(authority, AuthoritySnapshotV1, field="authority")
    return {
        "authority_epoch_key": authority.authority_epoch_key,
        "authority_snapshot": authority.model_dump(mode="json"),
        "child_result_hash": child_result_hash,
        "expected_result_set_hash": expected_result_set_hash,
        "five_pair_service_binding_set_hash": five_pair_service_binding_set_hash,
        "full_manifest_hash": authority.full_manifest_hash,
        "harness_identity": harness_identity,
        "harness_sha256": harness_sha256,
        "observed_family_ids": list(observed_family_ids),
        "observed_surface_ids": [
            surface.value if isinstance(surface, SurfaceId) else surface
            for surface in observed_surface_ids
        ],
        "selected_entry_hash": selected_entry_hash,
        "service_manifest_fingerprints": list(service_manifest_fingerprints),
        "test_manifest_hash": test_manifest_hash,
        "vector_set_hash": vector_set_hash,
        "verification_manifest_sha256": verification_manifest_sha256,
        "verifier_policy_content_hash": verifier_policy_content_hash,
        "verifier_policy_id": verifier_policy_id,
    }


def execution_evidence_hash(**inputs: Any) -> str:
    return hashlib.sha256(
        canonical_json_bytes(execution_evidence_preimage(**inputs))
    ).hexdigest()


# --------------------------------------------------------------------------------------
# `RESET-REG-P2-01`: profile-derived freshness
# --------------------------------------------------------------------------------------


def resolve_participating_service_manifests(
    manifests: Sequence[RuntimeServiceManifest],
    participating: tuple[str, ...],
) -> tuple[RuntimeServiceManifest, ...]:
    """Exactly one manifest per participating ID; missing, duplicate, or extra rejects."""

    resolved = _require_exact_items(manifests, RuntimeServiceManifest, field="profile manifests")
    if type(participating) is not tuple or tuple(sorted(set(participating))) != participating:
        raise SignalFamilyVerificationError(
            "participating service ids must be sorted and duplicate-free"
        )
    if not participating:
        raise SignalFamilyVerificationError("participating service ids must be nonempty")
    selected: list[RuntimeServiceManifest] = []
    for service_id in participating:
        matches = tuple(
            manifest for manifest in resolved if manifest.service_id == service_id
        )
        if not matches:
            raise SignalFamilyVerificationError(
                f"no manifest resolves service id: {service_id}"
            )
        if len(matches) != 1:
            raise SignalFamilyVerificationError(
                f"duplicate manifest resolves service id: {service_id}"
            )
        selected.append(matches[0])
    return tuple(selected)


def service_freshness_seconds(manifests: Sequence[RuntimeServiceManifest]) -> float:
    resolved = _require_exact_items(
        manifests,
        RuntimeServiceManifest,
        field="participating service manifests",
    )
    if not resolved:
        raise SignalFamilyVerificationError("participating service manifests must be nonempty")
    return float(min(manifest.stale_after_seconds for manifest in resolved))


def freshness_seconds(
    service_freshness: float,
    verifier_policy_max_age_seconds: int | None,
) -> float:
    """The profile minimum, optionally capped by the frozen policy maximum."""

    if type(service_freshness) is not float or service_freshness <= 0:
        raise SignalFamilyVerificationError("service freshness must be a positive float")
    if verifier_policy_max_age_seconds is None:
        return service_freshness
    if (
        type(verifier_policy_max_age_seconds) is not int
        or verifier_policy_max_age_seconds < 1
    ):
        raise SignalFamilyVerificationError(
            "verifier_policy_max_age_seconds must be a positive integer"
        )
    return float(min(service_freshness, verifier_policy_max_age_seconds))


def fresh_until(verified_at: datetime, seconds: float) -> datetime:
    if type(verified_at) is not datetime:
        raise TypeError("verified_at must be an exact datetime object")
    if verified_at.tzinfo is None or verified_at.utcoffset() != timedelta(0):
        raise SignalFamilyVerificationError("verified_at must be timezone-aware UTC")
    if type(seconds) is not float or seconds <= 0:
        raise SignalFamilyVerificationError("freshness_seconds must be positive")
    return verified_at + timedelta(seconds=seconds)


# --------------------------------------------------------------------------------------
# One consistent snapshot, five receipts, one immutable decision
# --------------------------------------------------------------------------------------


class SignalFamilyVerificationSnapshotV1(_PhaseCStrictModel):
    """One consistent root-derived snapshot; every receipt and the decision bind it."""

    authority: AuthoritySnapshotV1
    overlay_content_hash: Sha256
    successor_bundle_content_hash: Sha256
    successor_declaration_hashes: tuple[Sha256, ...]
    successor_channel_hashes: tuple[Sha256, ...]
    verification_manifest_sha256: Sha256
    test_manifest_hash: Sha256
    vector_set_hash: Sha256
    expected_result_set_hash: Sha256
    five_pair_service_binding_set_hash: Sha256
    child_result_hash: Sha256
    verifier_policy_id: Literal["signal-family-verifier-policy-v1"]
    verifier_policy_content_hash: Sha256
    selected_entry_hash: Sha256
    harness_identity: Literal[
        "/usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"
    ]
    harness_sha256: Sha256
    observed_family_ids: tuple[StrictStr, ...]
    observed_surface_ids: tuple[SurfaceId, ...]
    pairs: tuple[PairBindingV1, ...]
    participating_service_ids: tuple[StrictStr, ...]
    service_binding_hashes: tuple[Sha256, ...]
    service_bindings_hash: Sha256
    service_manifest_fingerprints: tuple[Sha256, ...]
    verified_at: CanonicalTimestamp
    fresh_until: CanonicalTimestamp
    execution_evidence_hash: Sha256
    snapshot_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        require_exact_five_pairs(self.pairs)
        require_pair_derived_participants(self.pairs, self.participating_service_ids)
        if len(self.service_binding_hashes) != len(self.participating_service_ids):
            raise SignalFamilyVerificationError(
                "service binding hashes must cover every participating service"
            )
        if len(self.service_manifest_fingerprints) != len(self.participating_service_ids):
            raise SignalFamilyVerificationError(
                "service manifest fingerprints must cover every participating service"
            )
        _require_sorted_unique(
            self.successor_declaration_hashes,
            field="successor_declaration_hashes",
        )
        _require_sorted_unique(self.successor_channel_hashes, field="successor_channel_hashes")
        if parse_canonical_timestamp(self.fresh_until) <= parse_canonical_timestamp(
            self.verified_at
        ):
            raise SignalFamilyVerificationError("fresh_until must follow verified_at")
        if self.execution_evidence_hash != execution_evidence_hash(
            authority=self.authority,
            verification_manifest_sha256=self.verification_manifest_sha256,
            test_manifest_hash=self.test_manifest_hash,
            vector_set_hash=self.vector_set_hash,
            expected_result_set_hash=self.expected_result_set_hash,
            five_pair_service_binding_set_hash=self.five_pair_service_binding_set_hash,
            child_result_hash=self.child_result_hash,
            verifier_policy_id=self.verifier_policy_id,
            verifier_policy_content_hash=self.verifier_policy_content_hash,
            selected_entry_hash=self.selected_entry_hash,
            harness_identity=self.harness_identity,
            harness_sha256=self.harness_sha256,
            observed_family_ids=self.observed_family_ids,
            observed_surface_ids=self.observed_surface_ids,
            service_manifest_fingerprints=self.service_manifest_fingerprints,
        ):
            raise SignalFamilyVerificationError(
                "execution evidence hash does not match its canonical preimage"
            )
        if self.snapshot_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"snapshot_hash"})
        ):
            raise SignalFamilyVerificationError(
                "snapshot hash does not match its canonical content"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        authority: AuthoritySnapshotV1,
        overlay_content_hash: str,
        successor_bundle_content_hash: str,
        successor_declaration_hashes: tuple[str, ...],
        successor_channel_hashes: tuple[str, ...],
        verification_manifest_sha256: str,
        test_manifest_hash: str,
        pairs: tuple[PairBindingV1, ...],
        test_manifest: SignalFamilyTestManifestV1,
        child_result: SignalFamilyChildResultV1,
        policy: SignalFamilyVerifierPolicyV1,
        selected_entry: ReleaseVerificationEntryV1,
        service_manifests: Sequence[RuntimeServiceManifest],
        verified_at: datetime,
        freshness_seconds: float,
    ) -> Self:
        _require_exact_instance(authority, AuthoritySnapshotV1, field="authority")
        _require_exact_instance(test_manifest, SignalFamilyTestManifestV1, field="test manifest")
        _require_exact_instance(child_result, SignalFamilyChildResultV1, field="child result")
        _require_exact_instance(policy, SignalFamilyVerifierPolicyV1, field="policy")
        _require_exact_instance(
            selected_entry,
            ReleaseVerificationEntryV1,
            field="selected entry",
        )
        if selected_entry not in policy.release_entries:
            raise SignalFamilyVerificationError("selected entry is absent from the policy")
        if selected_entry.release_key != (
            successor_bundle_content_hash,
            overlay_content_hash,
        ):
            raise SignalFamilyVerificationError(
                "selected entry does not name the exact release key"
            )
        if selected_entry.verification_manifest_sha256 != verification_manifest_sha256:
            raise SignalFamilyVerificationError(
                "selected entry does not name the verification manifest hash"
            )
        if child_result.test_manifest_hash != test_manifest_hash:
            raise SignalFamilyVerificationError(
                "child result does not name the immutable test manifest"
            )
        resolved_pairs = require_exact_five_pairs(pairs)
        if test_manifest.pairs != resolved_pairs:
            raise SignalFamilyVerificationError(
                "test manifest pairs differ from the resolved pair map"
            )
        derived_vector_set = vector_set_hash(test_manifest.vectors)
        derived_expected_set = expected_result_set_hash(test_manifest.expected_results)
        derived_pair_set = five_pair_service_binding_set_hash(
            resolved_pairs,
            test_manifest.service_bindings,
        )
        if (
            selected_entry.vector_set_hash != derived_vector_set
            or selected_entry.expected_result_set_hash != derived_expected_set
            or selected_entry.five_pair_service_binding_set_hash != derived_pair_set
        ):
            raise SignalFamilyVerificationError(
                "selected entry does not authorize the recomputed manifest set hashes"
            )
        participating = participating_service_ids(resolved_pairs)
        resolved_manifests = resolve_participating_service_manifests(
            service_manifests,
            participating,
        )
        expiry = fresh_until(verified_at, freshness_seconds)
        values: dict[str, Any] = {
            "authority": authority,
            "overlay_content_hash": overlay_content_hash,
            "successor_bundle_content_hash": successor_bundle_content_hash,
            "successor_declaration_hashes": tuple(successor_declaration_hashes),
            "successor_channel_hashes": tuple(successor_channel_hashes),
            "verification_manifest_sha256": verification_manifest_sha256,
            "test_manifest_hash": test_manifest_hash,
            "vector_set_hash": derived_vector_set,
            "expected_result_set_hash": derived_expected_set,
            "five_pair_service_binding_set_hash": derived_pair_set,
            "child_result_hash": child_result.result_hash,
            "verifier_policy_id": policy.verifier_policy_id,
            "verifier_policy_content_hash": policy.content_hash,
            "selected_entry_hash": selected_entry.entry_hash,
            "harness_identity": policy.harness_identity,
            "harness_sha256": policy.harness_sha256,
            "observed_family_ids": observed_family_ids(child_result.vector_results),
            "observed_surface_ids": observed_surface_ids(child_result.vector_results),
            "pairs": resolved_pairs,
            "participating_service_ids": participating,
            "service_binding_hashes": tuple(
                binding.binding_hash for binding in test_manifest.service_bindings
            ),
            "service_bindings_hash": test_manifest.service_bindings_hash,
            "service_manifest_fingerprints": tuple(
                manifest.manifest_fingerprint for manifest in resolved_manifests
            ),
            "verified_at": canonical_timestamp(verified_at),
            "fresh_until": canonical_timestamp(expiry),
        }
        values["execution_evidence_hash"] = execution_evidence_hash(
            authority=authority,
            verification_manifest_sha256=verification_manifest_sha256,
            test_manifest_hash=test_manifest_hash,
            vector_set_hash=derived_vector_set,
            expected_result_set_hash=derived_expected_set,
            five_pair_service_binding_set_hash=derived_pair_set,
            child_result_hash=child_result.result_hash,
            verifier_policy_id=policy.verifier_policy_id,
            verifier_policy_content_hash=policy.content_hash,
            selected_entry_hash=selected_entry.entry_hash,
            harness_identity=policy.harness_identity,
            harness_sha256=policy.harness_sha256,
            observed_family_ids=values["observed_family_ids"],
            observed_surface_ids=values["observed_surface_ids"],
            service_manifest_fingerprints=values["service_manifest_fingerprints"],
        )
        preimage = dict(values)
        preimage["authority"] = authority.model_dump(mode="json")
        preimage["pairs"] = [pair.model_dump(mode="json") for pair in resolved_pairs]
        preimage["observed_surface_ids"] = [
            surface.value for surface in values["observed_surface_ids"]
        ]
        preimage["successor_declaration_hashes"] = list(values["successor_declaration_hashes"])
        preimage["successor_channel_hashes"] = list(values["successor_channel_hashes"])
        preimage["observed_family_ids"] = list(values["observed_family_ids"])
        preimage["participating_service_ids"] = list(participating)
        preimage["service_binding_hashes"] = list(values["service_binding_hashes"])
        preimage["service_manifest_fingerprints"] = list(
            values["service_manifest_fingerprints"]
        )
        return cls(**values, snapshot_hash=canonical_sha256(preimage))


class SignalFamilyReceiptV1(_PhaseCStrictModel):
    """One pair receipt. Only the root verifier ever emits or persists one."""

    pair_id: PairIdField
    producer_service_ids: tuple[StrictStr, ...]
    consumer_service_ids: tuple[StrictStr, ...]
    producer_surface_ids: tuple[SurfaceId, ...]
    reader_surface_ids: tuple[SurfaceId, ...]
    successor_declaration_hashes: tuple[Sha256, ...]
    successor_channel_hashes: tuple[Sha256, ...]
    overlay_content_hash: Sha256
    authority_epoch_key: Sha256
    operation_id: StrictStr = Field(pattern=_OPERATION_ID)
    authority_sequence: StrictInt
    generation_id: Sha256
    full_manifest_hash: Sha256
    profile_id: Sha256
    verification_manifest_sha256: Sha256
    test_manifest_hash: Sha256
    child_result_hash: Sha256
    execution_evidence_hash: Sha256
    verifier_policy_id: Literal["signal-family-verifier-policy-v1"]
    verifier_policy_content_hash: Sha256
    selected_entry_hash: Sha256
    harness_identity: Literal[
        "/usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"
    ]
    harness_sha256: Sha256
    participating_service_ids: tuple[StrictStr, ...]
    service_binding_hashes: tuple[Sha256, ...]
    service_bindings_hash: Sha256
    service_manifest_fingerprints: tuple[Sha256, ...]
    verified_at: CanonicalTimestamp
    fresh_until: CanonicalTimestamp
    receipt_fingerprint: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.producer_surface_ids != PRODUCER_SURFACES[self.pair_id]:
            raise SignalFamilyVerificationError(
                "receipt producer surfaces are not the frozen pair evidence"
            )
        if self.reader_surface_ids != READER_SURFACES[self.pair_id]:
            raise SignalFamilyVerificationError(
                "receipt reader surfaces are not the frozen pair surfaces"
            )
        _require_sorted_unique(self.producer_service_ids, field="producer_service_ids")
        _require_sorted_unique(self.consumer_service_ids, field="consumer_service_ids")
        if self.receipt_fingerprint != canonical_sha256(
            self.model_dump(mode="json", exclude={"receipt_fingerprint"})
        ):
            raise SignalFamilyVerificationError(
                "receipt fingerprint does not match its canonical content"
            )
        return self


def _build_receipt(
    snapshot: SignalFamilyVerificationSnapshotV1,
    pair: PairBindingV1,
) -> SignalFamilyReceiptV1:
    values: dict[str, Any] = {
        "pair_id": pair.pair_id,
        "producer_service_ids": pair.producer_service_ids,
        "consumer_service_ids": pair.consumer_service_ids,
        "producer_surface_ids": PRODUCER_SURFACES[pair.pair_id],
        "reader_surface_ids": READER_SURFACES[pair.pair_id],
        "successor_declaration_hashes": snapshot.successor_declaration_hashes,
        "successor_channel_hashes": snapshot.successor_channel_hashes,
        "overlay_content_hash": snapshot.overlay_content_hash,
        "authority_epoch_key": snapshot.authority.authority_epoch_key,
        "operation_id": snapshot.authority.operation_id,
        "authority_sequence": snapshot.authority.sequence,
        "generation_id": snapshot.authority.generation_id,
        "full_manifest_hash": snapshot.authority.full_manifest_hash,
        "profile_id": snapshot.authority.profile_id,
        "verification_manifest_sha256": snapshot.verification_manifest_sha256,
        "test_manifest_hash": snapshot.test_manifest_hash,
        "child_result_hash": snapshot.child_result_hash,
        "execution_evidence_hash": snapshot.execution_evidence_hash,
        "verifier_policy_id": snapshot.verifier_policy_id,
        "verifier_policy_content_hash": snapshot.verifier_policy_content_hash,
        "selected_entry_hash": snapshot.selected_entry_hash,
        "harness_identity": snapshot.harness_identity,
        "harness_sha256": snapshot.harness_sha256,
        "participating_service_ids": snapshot.participating_service_ids,
        "service_binding_hashes": snapshot.service_binding_hashes,
        "service_bindings_hash": snapshot.service_bindings_hash,
        "service_manifest_fingerprints": snapshot.service_manifest_fingerprints,
        "verified_at": snapshot.verified_at,
        "fresh_until": snapshot.fresh_until,
    }
    preimage = dict(values)
    for name in ("producer_surface_ids", "reader_surface_ids"):
        preimage[name] = [surface.value for surface in values[name]]
    for name in (
        "producer_service_ids",
        "consumer_service_ids",
        "successor_declaration_hashes",
        "successor_channel_hashes",
        "participating_service_ids",
        "service_binding_hashes",
        "service_manifest_fingerprints",
    ):
        preimage[name] = list(values[name])
    return SignalFamilyReceiptV1(**values, receipt_fingerprint=canonical_sha256(preimage))


def build_pair_receipts(
    snapshot: SignalFamilyVerificationSnapshotV1,
) -> tuple[SignalFamilyReceiptV1, ...]:
    """One successful immutable run yields exactly five receipts, one per frozen pair."""

    _require_exact_instance(
        snapshot,
        SignalFamilyVerificationSnapshotV1,
        field="verification snapshot",
    )
    return tuple(_build_receipt(snapshot, pair) for pair in snapshot.pairs)


def receipt_unique_key(receipt: SignalFamilyReceiptV1) -> tuple[str, str, str]:
    _require_exact_instance(receipt, SignalFamilyReceiptV1, field="receipt")
    return (receipt.overlay_content_hash, receipt.authority_epoch_key, receipt.pair_id)


def receipt_fingerprint_set_hash(fingerprints: Sequence[str]) -> str:
    values = tuple(fingerprints)
    if list(values) != sorted(set(values)):
        raise SignalFamilyVerificationError(
            "receipt fingerprints must be sorted and duplicate-free"
        )
    return canonical_sha256({"receipt_fingerprints": list(values)})


class SignalFamilyReadinessDecisionV1(_PhaseCStrictModel):
    """The immutable `READY` decision of one verifier run. Set equality, never a count."""

    overlay_content_hash: Sha256
    successor_declaration_hashes: tuple[Sha256, ...]
    successor_channel_hashes: tuple[Sha256, ...]
    pair_ids: tuple[PairIdField, ...]
    receipt_fingerprints: tuple[Sha256, ...]
    receipt_fingerprint_set_hash: Sha256
    authority_epoch_key: Sha256
    operation_id: StrictStr = Field(pattern=_OPERATION_ID)
    authority_sequence: StrictInt
    generation_id: Sha256
    full_manifest_hash: Sha256
    profile_id: Sha256
    participating_service_ids: tuple[StrictStr, ...]
    service_binding_hashes: tuple[Sha256, ...]
    service_bindings_hash: Sha256
    verifier_policy_id: Literal["signal-family-verifier-policy-v1"]
    verifier_policy_content_hash: Sha256
    selected_entry_hash: Sha256
    harness_identity: Literal[
        "/usr/local/libexec/rquant-signal-family-verifier-harness-v1.pyz"
    ]
    harness_sha256: Sha256
    verified_at: CanonicalTimestamp
    fresh_until: CanonicalTimestamp
    decision_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.pair_ids != PAIR_IDS:
            raise SignalFamilyVerificationError(
                "a READY decision requires exactly the five frozen pair ids"
            )
        if self.receipt_fingerprint_set_hash != receipt_fingerprint_set_hash(
            self.receipt_fingerprints
        ):
            raise SignalFamilyVerificationError(
                "receipt fingerprint aggregate does not match its canonical content"
            )
        if self.decision_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"decision_hash"})
        ):
            raise SignalFamilyVerificationError(
                "decision hash does not match its canonical content"
            )
        return self


def build_readiness_decision(
    snapshot: SignalFamilyVerificationSnapshotV1,
    receipts: Sequence[SignalFamilyReceiptV1],
) -> SignalFamilyReadinessDecisionV1:
    """`READY` requires exact five-pair set equality from one consistent snapshot."""

    _require_exact_instance(
        snapshot,
        SignalFamilyVerificationSnapshotV1,
        field="verification snapshot",
    )
    resolved = _require_exact_items(receipts, SignalFamilyReceiptV1, field="receipts")
    observed = tuple(sorted({receipt.pair_id for receipt in resolved}))
    if len(resolved) != len(PAIR_IDS) or observed != PAIR_IDS:
        raise SignalFamilyVerificationError(
            "receipts must cover exactly the five frozen pairs"
        )
    expected = {receipt.pair_id: receipt for receipt in build_pair_receipts(snapshot)}
    for receipt in resolved:
        if receipt != expected[receipt.pair_id]:
            raise SignalFamilyVerificationError("receipt does not bind the decision snapshot")
    fingerprints = tuple(sorted(receipt.receipt_fingerprint for receipt in resolved))
    values: dict[str, Any] = {
        "overlay_content_hash": snapshot.overlay_content_hash,
        "successor_declaration_hashes": snapshot.successor_declaration_hashes,
        "successor_channel_hashes": snapshot.successor_channel_hashes,
        "pair_ids": PAIR_IDS,
        "receipt_fingerprints": fingerprints,
        "receipt_fingerprint_set_hash": receipt_fingerprint_set_hash(fingerprints),
        "authority_epoch_key": snapshot.authority.authority_epoch_key,
        "operation_id": snapshot.authority.operation_id,
        "authority_sequence": snapshot.authority.sequence,
        "generation_id": snapshot.authority.generation_id,
        "full_manifest_hash": snapshot.authority.full_manifest_hash,
        "profile_id": snapshot.authority.profile_id,
        "participating_service_ids": snapshot.participating_service_ids,
        "service_binding_hashes": snapshot.service_binding_hashes,
        "service_bindings_hash": snapshot.service_bindings_hash,
        "verifier_policy_id": snapshot.verifier_policy_id,
        "verifier_policy_content_hash": snapshot.verifier_policy_content_hash,
        "selected_entry_hash": snapshot.selected_entry_hash,
        "harness_identity": snapshot.harness_identity,
        "harness_sha256": snapshot.harness_sha256,
        "verified_at": snapshot.verified_at,
        "fresh_until": snapshot.fresh_until,
    }
    preimage = dict(values)
    for name in (
        "successor_declaration_hashes",
        "successor_channel_hashes",
        "pair_ids",
        "receipt_fingerprints",
        "participating_service_ids",
        "service_binding_hashes",
    ):
        preimage[name] = list(values[name])
    return SignalFamilyReadinessDecisionV1(
        **values,
        decision_hash=canonical_sha256(preimage),
    )


def decision_unique_key(decision: SignalFamilyReadinessDecisionV1) -> tuple[str, str]:
    _require_exact_instance(decision, SignalFamilyReadinessDecisionV1, field="decision")
    return (decision.overlay_content_hash, decision.authority_epoch_key)


def is_decision_fresh(
    decision: SignalFamilyReadinessDecisionV1,
    now: datetime,
) -> bool:
    _require_exact_instance(decision, SignalFamilyReadinessDecisionV1, field="decision")
    if type(now) is not datetime or now.tzinfo is None or now.utcoffset() != timedelta(0):
        raise SignalFamilyVerificationError("now must be timezone-aware UTC")
    return now < parse_canonical_timestamp(decision.fresh_until)


# --------------------------------------------------------------------------------------
# Bounded audit evidence
# --------------------------------------------------------------------------------------


class SignalFamilyAuditOutcome(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class SignalFamilyAuditEvent(StrEnum):
    POLICY_VALIDATED = "policy_validated"
    ENTRY_SELECTED = "entry_selected"
    MANIFEST_VALIDATED = "manifest_validated"
    BINDING_VALIDATED = "binding_validated"
    CHILD_LAUNCHED = "child_launched"
    CHILD_RESULT_VALIDATED = "child_result_validated"
    AUTHORITY_REVALIDATED = "authority_revalidated"
    RECEIPT_APPENDED = "receipt_appended"
    DECISION_FINALIZED = "decision_finalized"
    READINESS_DECLARED = "readiness_declared"
    READINESS_REVOKED = "readiness_revoked"
    READINESS_ROLLED_BACK = "readiness_rolled_back"


class SignalFamilyReasonCode(StrEnum):
    """Every rejection reason the verifier may record. No free-form text exists."""

    POLICY_ANCHOR_INVALID = "POLICY_ANCHOR_INVALID"
    POLICY_BYTES_NONCANONICAL = "POLICY_BYTES_NONCANONICAL"
    POLICY_CONTENT_HASH_MISMATCH = "POLICY_CONTENT_HASH_MISMATCH"
    POLICY_CHANGED_DURING_RUN = "POLICY_CHANGED_DURING_RUN"
    HARNESS_IDENTITY_MISMATCH = "HARNESS_IDENTITY_MISMATCH"
    HARNESS_HASH_MISMATCH = "HARNESS_HASH_MISMATCH"
    ENTRY_MISSING = "ENTRY_MISSING"
    ENTRY_MULTIPLE = "ENTRY_MULTIPLE"
    ENTRY_CONFLICTING = "ENTRY_CONFLICTING"
    ENTRY_STALE = "ENTRY_STALE"
    VERIFICATION_MANIFEST_HASH_MISMATCH = "VERIFICATION_MANIFEST_HASH_MISMATCH"
    TEST_MANIFEST_HASH_MISMATCH = "TEST_MANIFEST_HASH_MISMATCH"
    VECTOR_SET_HASH_MISMATCH = "VECTOR_SET_HASH_MISMATCH"
    EXPECTED_RESULT_SET_HASH_MISMATCH = "EXPECTED_RESULT_SET_HASH_MISMATCH"
    FIVE_PAIR_SET_HASH_MISMATCH = "FIVE_PAIR_SET_HASH_MISMATCH"
    BINDING_MISSING = "BINDING_MISSING"
    BINDING_DUPLICATE = "BINDING_DUPLICATE"
    BINDING_CROSS_ROLE = "BINDING_CROSS_ROLE"
    BINDING_WRONG_KIND = "BINDING_WRONG_KIND"
    BINDING_WRONG_MODULE = "BINDING_WRONG_MODULE"
    BINDING_WRONG_PATH = "BINDING_WRONG_PATH"
    BINDING_WRONG_SOURCE_HASH = "BINDING_WRONG_SOURCE_HASH"
    BINDING_UNMANIFESTED = "BINDING_UNMANIFESTED"
    BINDING_SURFACE_MISMATCH = "BINDING_SURFACE_MISMATCH"
    PAIR_SET_INCOMPLETE = "PAIR_SET_INCOMPLETE"
    PARTICIPANT_RESOLUTION_INVALID = "PARTICIPANT_RESOLUTION_INVALID"
    CHILD_LAUNCH_FAILED = "CHILD_LAUNCH_FAILED"
    CHILD_TIMEOUT = "CHILD_TIMEOUT"
    CHILD_SIGNAL_DEATH = "CHILD_SIGNAL_DEATH"
    CHILD_NONZERO_EXIT = "CHILD_NONZERO_EXIT"
    CHILD_EXTRA_OUTPUT = "CHILD_EXTRA_OUTPUT"
    CHILD_DESCRIPTOR_MISMATCH = "CHILD_DESCRIPTOR_MISMATCH"
    CHILD_RESULT_OVERSIZED = "CHILD_RESULT_OVERSIZED"
    CHILD_RESULT_NONCANONICAL = "CHILD_RESULT_NONCANONICAL"
    CHILD_RESULT_IDENTITY_MISMATCH = "CHILD_RESULT_IDENTITY_MISMATCH"
    RESULT_SET_HASH_MISMATCH = "RESULT_SET_HASH_MISMATCH"
    AUTHORITY_EPOCH_CHANGED = "AUTHORITY_EPOCH_CHANGED"
    DEPLOYMENT_LOCK_LOST = "DEPLOYMENT_LOCK_LOST"
    RECEIPT_CONFLICT = "RECEIPT_CONFLICT"
    DECISION_CONFLICT = "DECISION_CONFLICT"
    READINESS_TRANSITION_INVALID = "READINESS_TRANSITION_INVALID"
    READINESS_EXPIRED = "READINESS_EXPIRED"


class SignalFamilyVerificationAuditRecordV1(_PhaseCStrictModel):
    """Identifiers, hashes, timestamps, outcomes, and bounded reason codes only."""

    schema_version: StrictInt
    event: SignalFamilyAuditEvent
    outcome: SignalFamilyAuditOutcome
    reason_code: SignalFamilyReasonCode | None
    recorded_at: CanonicalTimestamp
    pair_id: PairIdField | None
    overlay_content_hash: Sha256 | None
    authority_epoch_key: Sha256 | None
    verifier_policy_content_hash: Sha256 | None
    selected_entry_hash: Sha256 | None
    subject_hash: Sha256 | None
    existing_hash: Sha256 | None
    attempted_hash: Sha256 | None
    record_hash: Sha256

    @model_validator(mode="after")
    def validate_identity(self) -> Self:
        if self.schema_version != 1:
            raise SignalFamilyVerificationError("unsupported audit record schema version")
        if self.outcome is SignalFamilyAuditOutcome.REJECTED and self.reason_code is None:
            raise SignalFamilyVerificationError("a rejected audit record requires a reason code")
        if self.outcome is SignalFamilyAuditOutcome.ACCEPTED and self.reason_code is not None:
            raise SignalFamilyVerificationError("an accepted audit record carries no reason code")
        if self.record_hash != canonical_sha256(
            self.model_dump(mode="json", exclude={"record_hash"})
        ):
            raise SignalFamilyVerificationError(
                "audit record hash does not match its canonical content"
            )
        return self

    @classmethod
    def create(
        cls,
        *,
        event: SignalFamilyAuditEvent,
        outcome: SignalFamilyAuditOutcome,
        reason_code: SignalFamilyReasonCode | None,
        recorded_at: datetime,
        pair_id: str | None = None,
        overlay_content_hash: str | None = None,
        authority_epoch_key: str | None = None,
        verifier_policy_content_hash: str | None = None,
        selected_entry_hash: str | None = None,
        subject_hash: str | None = None,
        existing_hash: str | None = None,
        attempted_hash: str | None = None,
    ) -> Self:
        values: dict[str, Any] = {
            "schema_version": 1,
            "event": event,
            "outcome": outcome,
            "reason_code": reason_code,
            "recorded_at": canonical_timestamp(recorded_at),
            "pair_id": pair_id,
            "overlay_content_hash": overlay_content_hash,
            "authority_epoch_key": authority_epoch_key,
            "verifier_policy_content_hash": verifier_policy_content_hash,
            "selected_entry_hash": selected_entry_hash,
            "subject_hash": subject_hash,
            "existing_hash": existing_hash,
            "attempted_hash": attempted_hash,
        }
        preimage = dict(values)
        preimage["event"] = event.value if isinstance(event, SignalFamilyAuditEvent) else event
        preimage["outcome"] = (
            outcome.value if isinstance(outcome, SignalFamilyAuditOutcome) else outcome
        )
        preimage["reason_code"] = (
            reason_code.value
            if isinstance(reason_code, SignalFamilyReasonCode)
            else reason_code
        )
        return cls(**values, record_hash=canonical_sha256(preimage))


def build_conflict_audit_record(
    *,
    event: SignalFamilyAuditEvent,
    reason_code: SignalFamilyReasonCode,
    recorded_at: datetime,
    existing_hash: str,
    attempted_hash: str,
    pair_id: str | None = None,
    overlay_content_hash: str | None = None,
    authority_epoch_key: str | None = None,
    verifier_policy_content_hash: str | None = None,
    selected_entry_hash: str | None = None,
) -> SignalFamilyVerificationAuditRecordV1:
    """Bounded conflict evidence: two fingerprints and their frozen identifiers."""

    return SignalFamilyVerificationAuditRecordV1.create(
        event=event,
        outcome=SignalFamilyAuditOutcome.REJECTED,
        reason_code=reason_code,
        recorded_at=recorded_at,
        pair_id=pair_id,
        overlay_content_hash=overlay_content_hash,
        authority_epoch_key=authority_epoch_key,
        verifier_policy_content_hash=verifier_policy_content_hash,
        selected_entry_hash=selected_entry_hash,
        existing_hash=existing_hash,
        attempted_hash=attempted_hash,
    )


def resolve_receipt_replay(
    existing: SignalFamilyReceiptV1,
    candidate: SignalFamilyReceiptV1,
    *,
    recorded_at: datetime | None = None,
) -> SignalFamilyReceiptV1:
    """An identical replay is idempotent; divergent bytes for one key reject."""

    _require_exact_instance(existing, SignalFamilyReceiptV1, field="existing receipt")
    _require_exact_instance(candidate, SignalFamilyReceiptV1, field="candidate receipt")
    if receipt_unique_key(existing) != receipt_unique_key(candidate):
        raise SignalFamilyVerificationError("receipt replay requires one unique key")
    if canonical_json_bytes(existing.model_dump(mode="json")) == canonical_json_bytes(
        candidate.model_dump(mode="json")
    ):
        return existing
    raise SignalFamilyVerificationConflictError(
        "receipt bytes diverge for one unique key",
        audit_record=build_conflict_audit_record(
            event=SignalFamilyAuditEvent.RECEIPT_APPENDED,
            reason_code=SignalFamilyReasonCode.RECEIPT_CONFLICT,
            recorded_at=datetime.now(UTC) if recorded_at is None else recorded_at,
            pair_id=existing.pair_id,
            overlay_content_hash=existing.overlay_content_hash,
            authority_epoch_key=existing.authority_epoch_key,
            verifier_policy_content_hash=existing.verifier_policy_content_hash,
            selected_entry_hash=existing.selected_entry_hash,
            existing_hash=existing.receipt_fingerprint,
            attempted_hash=candidate.receipt_fingerprint,
        ),
    )


def resolve_decision_replay(
    existing: SignalFamilyReadinessDecisionV1,
    candidate: SignalFamilyReadinessDecisionV1,
    *,
    recorded_at: datetime | None = None,
) -> SignalFamilyReadinessDecisionV1:
    """Concurrent identical finalization returns the same bytes; divergence rejects."""

    _require_exact_instance(
        existing,
        SignalFamilyReadinessDecisionV1,
        field="existing decision",
    )
    _require_exact_instance(
        candidate,
        SignalFamilyReadinessDecisionV1,
        field="candidate decision",
    )
    if decision_unique_key(existing) != decision_unique_key(candidate):
        raise SignalFamilyVerificationError("decision replay requires one unique key")
    if canonical_json_bytes(existing.model_dump(mode="json")) == canonical_json_bytes(
        candidate.model_dump(mode="json")
    ):
        return existing
    raise SignalFamilyVerificationConflictError(
        "decision bytes diverge for one unique key",
        audit_record=build_conflict_audit_record(
            event=SignalFamilyAuditEvent.DECISION_FINALIZED,
            reason_code=SignalFamilyReasonCode.DECISION_CONFLICT,
            recorded_at=datetime.now(UTC) if recorded_at is None else recorded_at,
            overlay_content_hash=existing.overlay_content_hash,
            authority_epoch_key=existing.authority_epoch_key,
            verifier_policy_content_hash=existing.verifier_policy_content_hash,
            selected_entry_hash=existing.selected_entry_hash,
            existing_hash=existing.decision_hash,
            attempted_hash=candidate.decision_hash,
        ),
    )


# --------------------------------------------------------------------------------------
# Minimal lifecycle
# --------------------------------------------------------------------------------------


class SignalFamilyReadinessState(StrEnum):
    """The whole lifecycle. There is no `ATTESTING` and no post-`READY` promotion."""

    DECLARED = "DECLARED"
    READY = "READY"
    REVOKED = "REVOKED"
    ROLLED_BACK = "ROLLED_BACK"


ALLOWED_READINESS_TRANSITIONS: frozenset[
    tuple[SignalFamilyReadinessState, SignalFamilyReadinessState]
] = frozenset(
    {
        (SignalFamilyReadinessState.DECLARED, SignalFamilyReadinessState.READY),
        (SignalFamilyReadinessState.DECLARED, SignalFamilyReadinessState.REVOKED),
        (SignalFamilyReadinessState.READY, SignalFamilyReadinessState.REVOKED),
        (SignalFamilyReadinessState.READY, SignalFamilyReadinessState.ROLLED_BACK),
    }
)


def is_allowed_readiness_transition(
    current: SignalFamilyReadinessState,
    target: SignalFamilyReadinessState,
) -> bool:
    return (current, target) in ALLOWED_READINESS_TRANSITIONS


def require_readiness_transition(
    current: SignalFamilyReadinessState,
    target: SignalFamilyReadinessState,
) -> SignalFamilyReadinessState:
    if not isinstance(current, SignalFamilyReadinessState) or not isinstance(
        target, SignalFamilyReadinessState
    ):
        raise TypeError("readiness transitions require exact SignalFamilyReadinessState values")
    if not is_allowed_readiness_transition(current, target):
        raise SignalFamilyVerificationError(
            f"readiness transition is not allowed: {current.value} -> {target.value}"
        )
    return target


def is_activation_eligible(state: SignalFamilyReadinessState) -> bool:
    """Rollback and revocation are append-only; they only disable future eligibility."""

    if not isinstance(state, SignalFamilyReadinessState):
        raise TypeError("activation eligibility requires an exact SignalFamilyReadinessState")
    return state is SignalFamilyReadinessState.READY
