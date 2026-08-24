"""Phase B successor base registry and staged overlay red tests.

Covers `RESET-REG-P1` and `RESET-REG-P1-01`: the four frozen schemas, their canonical
preimages and raw bytes, the strict structural rejection matrix, the actual-model
prerequisite, and the fact that a partial, absent, or conflicting overlay never becomes
ready while v2 stays untouched.
"""

from __future__ import annotations

import hashlib
import importlib
import inspect
import json
import subprocess
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from pydantic import BaseModel

from rquant import runtime_contracts
from rquant import signal_family_successor_registry as successor
from rquant.runtime_schema_registry import _CHANNEL_SPEC_BY_ID, RuntimeSchemaContractBundle
from rquant.signal_contracts import CURRENT_ENVELOPE_SCHEMA
from rquant.strict_json import canonical_json_bytes

REPO_ROOT = Path(__file__).resolve().parents[2]
REJECTIONS = (ValueError, TypeError)

CLOSURE_FILES = frozenset(
    {
        "src/rquant/signal_contracts.py",
        "src/rquant/signal_route_spool.py",
    }
)
PRODUCER_COMMIT = "a" * 40

_CHANNEL_SERVICES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "signal-bus-routed-record/current": (
        ("signal-router", "signal-router-shadow"),
        ("notifier", "paper-broker"),
    ),
    "signal-envelope/current": (
        ("strategy-live-a", "strategy-live-b"),
        ("shadow-session", "signal-router"),
    ),
    "signal-route-spool-record/current": (
        ("signal-router", "signal-router-shadow"),
        ("notifier", "paper-broker"),
    ),
}


def _closure(
    files: frozenset[str] = CLOSURE_FILES,
) -> successor.SuccessorGenerationSourceClosureV1:
    return successor.SuccessorGenerationSourceClosureV1(
        producer_commit=PRODUCER_COMMIT,
        source_closure=files,
    )


def _channel(
    channel_id: str,
    *,
    producers: tuple[str, ...] | None = None,
    consumers: tuple[str, ...] | None = None,
) -> successor.SuccessorChannelV1:
    declared_producers, declared_consumers = _CHANNEL_SERVICES[channel_id]
    return successor.SuccessorChannelV1.create(
        channel_id,
        producer_service_ids=declared_producers if producers is None else producers,
        consumer_service_ids=declared_consumers if consumers is None else consumers,
        closure=_closure(),
    )


def _channels() -> tuple[successor.SuccessorChannelV1, ...]:
    return tuple(_channel(channel_id) for channel_id in sorted(_CHANNEL_SERVICES))


def _bundle(
    channels: tuple[successor.SuccessorChannelV1, ...] | None = None,
) -> successor.SuccessorBundleV1:
    return successor.SuccessorBundleV1.create(
        channels=_channels() if channels is None else channels
    )


def _declaration(
    bundle: successor.SuccessorBundleV1,
    channel_id: str,
    *,
    pair_ids: tuple[str, ...] = ("router-notifier", "strategy-router"),
    accepted_family_ids: tuple[str, ...] | None = None,
) -> successor.OverlayDeclarationV1:
    return successor.OverlayDeclarationV1.create(
        channel=bundle.channel(channel_id),
        base_bundle_content_hash=bundle.content_hash,
        accepted_family_ids=(
            successor.ACCEPTED_FAMILY_IDS if accepted_family_ids is None else accepted_family_ids
        ),
        pair_ids=pair_ids,
    )


def _overlay(
    bundle: successor.SuccessorBundleV1,
    channel_ids: tuple[str, ...] | None = None,
) -> successor.OverlayBundleV1:
    selected = tuple(channel.channel_id for channel in bundle.channels)
    if channel_ids is not None:
        selected = channel_ids
    return successor.OverlayBundleV1.create(
        base_bundle_content_hash=bundle.content_hash,
        declarations=tuple(_declaration(bundle, channel_id) for channel_id in selected),
    )


def _registry() -> tuple[
    successor.SuccessorRegistry,
    list[successor.SuccessorConflictAuditRecordV1],
]:
    records: list[successor.SuccessorConflictAuditRecordV1] = []

    class _ListSink:
        def append(self, record: successor.SuccessorConflictAuditRecordV1) -> None:
            records.append(record)

    return successor.SuccessorRegistry(closure=_closure(), audit_sink=_ListSink()), records


def _decoded(payload: bytes) -> dict[str, Any]:
    value = json.loads(payload)
    assert isinstance(value, dict)
    return value


def _recanonical(value: dict[str, Any]) -> bytes:
    return canonical_json_bytes(value)


def _with_space(payload: bytes) -> bytes:
    return payload.replace(b":", b": ", 1)


def _with_unicode_escape(payload: bytes) -> bytes:
    index = payload.index(b'"') + 1
    escaped = b"\\u00" + format(payload[index], "02x").encode("ascii")
    return payload[:index] + escaped + payload[index + 1 :]


def _with_reordered_keys(payload: bytes) -> bytes:
    decoded = _decoded(payload)
    reordered = {key: decoded[key] for key in reversed(list(decoded))}
    return json.dumps(reordered, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _with_trailing_newline(payload: bytes) -> bytes:
    return payload + b"\n"


def _with_duplicate_key(payload: bytes) -> bytes:
    decoded = _decoded(payload)
    first = next(iter(decoded))
    duplicate = json.dumps({first: decoded[first]}, ensure_ascii=False, separators=(",", ":"))
    return b"{" + duplicate.encode("utf-8")[1:-1] + b"," + payload[1:]


NONCANONICAL_MUTATORS = (
    ("whitespace", _with_space),
    ("unicode-escape", _with_unicode_escape),
    ("key-order", _with_reordered_keys),
    ("newline", _with_trailing_newline),
    ("duplicate-key", _with_duplicate_key),
)


def _raw_forms() -> dict[str, tuple[bytes, Any]]:
    bundle = _bundle()
    overlay = _overlay(bundle)
    return {
        "channel": (
            successor.successor_channel_canonical_json_bytes(bundle.channels[0]),
            lambda payload: successor.SuccessorChannelV1.from_canonical_json(
                payload, closure=_closure()
            ),
        ),
        "bundle": (
            successor.successor_bundle_canonical_json_bytes(bundle),
            lambda payload: successor.SuccessorBundleV1.from_canonical_json(
                payload, closure=_closure()
            ),
        ),
        "declaration": (
            successor.overlay_declaration_canonical_json_bytes(overlay.declarations[0]),
            successor.OverlayDeclarationV1.from_canonical_json,
        ),
        "overlay": (
            successor.overlay_bundle_canonical_json_bytes(overlay),
            successor.OverlayBundleV1.from_canonical_json,
        ),
    }


# --------------------------------------------------------------------------------------
# R1: canonical_sha256 is the ensure_ascii=False strict_json definition
# --------------------------------------------------------------------------------------


def test_canonical_sha256_is_the_strict_json_definition() -> None:
    value = {"payload_model": "rquant.signal_contracts.CurrentSignalEnvelope", "note": "现"}
    assert (
        successor._canonical_sha256(value)
        == hashlib.sha256(canonical_json_bytes(value)).hexdigest()
    )


def test_canonical_sha256_differs_from_the_frozen_v2_helper_on_non_ascii() -> None:
    value = {"note": "现"}
    assert successor._canonical_sha256(value) != runtime_contracts.canonical_sha256(value)
    ascii_only = {"note": "plain"}
    assert successor._canonical_sha256(ascii_only) == runtime_contracts.canonical_sha256(ascii_only)


def test_module_never_reuses_the_v2_canonical_sha256_helper() -> None:
    source = Path(inspect.getsourcefile(successor) or "").read_text(encoding="utf-8")
    assert "runtime_contracts" not in source
    assert "ensure_ascii" not in source


# --------------------------------------------------------------------------------------
# Exact four-schema field sets and order
# --------------------------------------------------------------------------------------


def test_the_four_schemas_have_exactly_the_frozen_fields_in_order() -> None:
    assert tuple(successor.SuccessorChannelV1.model_fields) == (
        "channel_id",
        "payload_model",
        "declaration_schema_fingerprint",
        "physical_schema_fingerprint",
        "model_descriptor_hash",
        "producer_service_ids",
        "consumer_service_ids",
        "channel_hash",
    )
    assert tuple(successor.SuccessorBundleV1.model_fields) == (
        "schema_version",
        "bundle_namespace",
        "channels",
        "content_hash",
    )
    assert tuple(successor.OverlayDeclarationV1.model_fields) == (
        "channel_id",
        "base_bundle_content_hash",
        "base_declaration_fingerprint",
        "base_physical_fingerprint",
        "model_descriptor_hash",
        "accepted_family_ids",
        "pair_ids",
        "declaration_hash",
    )
    assert tuple(successor.OverlayBundleV1.model_fields) == (
        "overlay_namespace",
        "overlay_version",
        "base_bundle_content_hash",
        "declarations",
        "content_hash",
    )


def test_frozen_namespaces_and_versions_are_exact() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)
    assert bundle.bundle_namespace == "rquant.signal-family.successor"
    assert bundle.schema_version == 1
    assert overlay.overlay_namespace == "rquant.signal-family.overlay"
    assert overlay.overlay_version == 1


@pytest.mark.parametrize(
    ("form", "field", "value"),
    (
        ("bundle", "bundle_namespace", "rquant.signal-family.overlay"),
        ("bundle", "schema_version", 2),
        ("overlay", "overlay_namespace", "rquant.signal-family.successor"),
        ("overlay", "overlay_version", 2),
    ),
)
def test_wrong_namespace_or_version_rejects(form: str, field: str, value: Any) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = value
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


# --------------------------------------------------------------------------------------
# Exact hash preimages
# --------------------------------------------------------------------------------------


def test_model_descriptor_hash_preimage_is_exact() -> None:
    channel = _channel("signal-envelope/current")
    assert channel.model_descriptor_hash == successor._canonical_sha256(
        {
            "payload_model": channel.payload_model,
            "declaration_schema_fingerprint": channel.declaration_schema_fingerprint,
            "physical_schema_fingerprint": channel.physical_schema_fingerprint,
        }
    )


def test_channel_bundle_declaration_and_overlay_hash_preimages_are_exact() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)
    for channel in bundle.channels:
        assert channel.channel_hash == successor._canonical_sha256(
            channel.model_dump(mode="json", exclude={"channel_hash"})
        )
    assert bundle.content_hash == successor._canonical_sha256(
        bundle.model_dump(mode="json", exclude={"content_hash"})
    )
    for declaration in overlay.declarations:
        assert declaration.declaration_hash == successor._canonical_sha256(
            declaration.model_dump(mode="json", exclude={"declaration_hash"})
        )
    assert overlay.content_hash == successor._canonical_sha256(
        overlay.model_dump(mode="json", exclude={"content_hash"})
    )


# --------------------------------------------------------------------------------------
# Raw canonical authority bytes
# --------------------------------------------------------------------------------------


def test_raw_authority_bytes_equal_full_model_canonical_bytes() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)
    assert successor.successor_bundle_canonical_json_bytes(bundle) == canonical_json_bytes(
        bundle.model_dump(mode="json")
    )
    assert successor.overlay_bundle_canonical_json_bytes(overlay) == canonical_json_bytes(
        overlay.model_dump(mode="json")
    )
    for channel in bundle.channels:
        assert successor.successor_channel_canonical_json_bytes(channel) == canonical_json_bytes(
            channel.model_dump(mode="json")
        )
    for declaration in overlay.declarations:
        assert successor.overlay_declaration_canonical_json_bytes(
            declaration
        ) == canonical_json_bytes(declaration.model_dump(mode="json"))


def test_canonical_bytes_round_trip_through_the_strict_decoders() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)
    raw_bundle = successor.successor_bundle_canonical_json_bytes(bundle)
    assert successor.SuccessorBundleV1.from_canonical_json(raw_bundle, closure=_closure()) == bundle
    raw_overlay = successor.overlay_bundle_canonical_json_bytes(overlay)
    assert successor.OverlayBundleV1.from_canonical_json(raw_overlay) == overlay
    channel = bundle.channels[0]
    raw_channel = successor.successor_channel_canonical_json_bytes(channel)
    assert (
        successor.SuccessorChannelV1.from_canonical_json(raw_channel, closure=_closure()) == channel
    )
    declaration = overlay.declarations[0]
    raw_declaration = successor.overlay_declaration_canonical_json_bytes(declaration)
    assert successor.OverlayDeclarationV1.from_canonical_json(raw_declaration) == declaration


def test_canonical_bytes_serializers_require_the_exact_model() -> None:
    bundle = _bundle()

    class _BundleSubclass(successor.SuccessorBundleV1):
        pass

    subclass = _BundleSubclass.model_construct(**dict(bundle))
    with pytest.raises(REJECTIONS):
        successor.successor_bundle_canonical_json_bytes(subclass)
    with pytest.raises(REJECTIONS):
        successor.successor_channel_canonical_json_bytes(bundle)  # type: ignore[arg-type]
    with pytest.raises(REJECTIONS):
        successor.overlay_bundle_canonical_json_bytes(bundle)  # type: ignore[arg-type]


# --------------------------------------------------------------------------------------
# Strict structural rejection matrix (authority.md L1162-1172)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
@pytest.mark.parametrize("mutator", NONCANONICAL_MUTATORS, ids=lambda item: item[0])
def test_noncanonical_representations_reject(form: str, mutator: tuple[str, Any]) -> None:
    payload, decode = _raw_forms()[form]
    with pytest.raises(REJECTIONS):
        decode(mutator[1](payload))


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
def test_extra_keys_reject(form: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded["unexpected_field"] = "x"
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
def test_aliased_field_names_reject(form: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    first = sorted(decoded)[0]
    decoded["".join(part.capitalize() for part in first.split("_"))] = decoded.pop(first)
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
def test_non_object_payloads_reject(form: str) -> None:
    _, decode = _raw_forms()[form]
    for payload in (b"[]", b'"x"', b"1", b"null"):
        with pytest.raises(REJECTIONS):
            decode(payload)


@pytest.mark.parametrize(
    ("form", "field", "value"),
    (
        ("channel", "channel_id", 1),
        ("channel", "declaration_schema_fingerprint", 0),
        ("channel", "producer_service_ids", "signal-router"),
        ("bundle", "schema_version", "1"),
        ("bundle", "schema_version", 1.0),
        ("bundle", "channels", {}),
        ("declaration", "pair_ids", "strategy-router"),
        ("declaration", "base_bundle_content_hash", 0),
        ("overlay", "overlay_version", "1"),
        ("overlay", "declarations", {}),
    ),
)
def test_coerced_scalar_types_reject(form: str, field: str, value: Any) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = value
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


@pytest.mark.parametrize(
    ("form", "field"),
    (("bundle", "schema_version"), ("overlay", "overlay_version")),
)
def test_booleans_are_not_accepted_as_integers(form: str, field: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = True
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


def test_mappings_or_subclasses_cannot_replace_the_exact_nested_models() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)

    class _ChannelSubclass(successor.SuccessorChannelV1):
        pass

    class _DeclarationSubclass(successor.OverlayDeclarationV1):
        pass

    channel_mapping = MappingProxyType(bundle.channels[0].model_dump(mode="json"))
    with pytest.raises(REJECTIONS):
        successor.SuccessorBundleV1.create(channels=(channel_mapping,))  # type: ignore[arg-type]
    with pytest.raises(REJECTIONS):
        successor.SuccessorBundleV1.create(
            channels=(_ChannelSubclass.model_construct(**dict(bundle.channels[0])),)
        )
    declaration_mapping = MappingProxyType(overlay.declarations[0].model_dump(mode="json"))
    with pytest.raises(REJECTIONS):
        successor.OverlayBundleV1.create(
            base_bundle_content_hash=bundle.content_hash,
            declarations=(declaration_mapping,),  # type: ignore[arg-type]
        )
    with pytest.raises(REJECTIONS):
        successor.OverlayBundleV1.create(
            base_bundle_content_hash=bundle.content_hash,
            declarations=(_DeclarationSubclass.model_construct(**dict(overlay.declarations[0])),),
        )


@pytest.mark.parametrize(
    ("form", "field"),
    (
        ("channel", "producer_service_ids"),
        ("channel", "consumer_service_ids"),
        ("declaration", "pair_ids"),
        ("bundle", "channels"),
        ("overlay", "declarations"),
    ),
)
def test_unsorted_and_duplicated_tuple_variants_reject(form: str, field: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    items = decoded[field]
    assert len(items) >= 2, f"{form}.{field} fixture must carry at least two entries"
    decoded[field] = list(reversed(items))
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))
    decoded[field] = [items[0], items[0]]
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


def test_duplicate_accepted_family_ids_reject() -> None:
    payload, decode = _raw_forms()["declaration"]
    decoded = _decoded(payload)
    decoded["accepted_family_ids"] = [CURRENT_ENVELOPE_SCHEMA, CURRENT_ENVELOPE_SCHEMA]
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


@pytest.mark.parametrize(
    ("form", "field"),
    (
        ("channel", "producer_service_ids"),
        ("channel", "consumer_service_ids"),
        ("declaration", "pair_ids"),
        ("declaration", "accepted_family_ids"),
        ("bundle", "channels"),
        ("overlay", "declarations"),
    ),
)
def test_empty_tuple_fields_reject(form: str, field: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = []
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


@pytest.mark.parametrize(
    ("form", "field"),
    (
        ("channel", "model_descriptor_hash"),
        ("channel", "channel_hash"),
        ("bundle", "content_hash"),
        ("declaration", "declaration_hash"),
        ("overlay", "content_hash"),
    ),
)
def test_hash_mismatch_rejects(form: str, field: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = "f" * 64
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


@pytest.mark.parametrize(
    ("form", "field"),
    (
        ("channel", "channel_hash"),
        ("bundle", "content_hash"),
        ("declaration", "declaration_hash"),
        ("overlay", "content_hash"),
    ),
)
def test_non_lowercase_hex_digests_reject(form: str, field: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    for value in ("F" * 64, "f" * 63, "f" * 65, ""):
        decoded[field] = value
        with pytest.raises(REJECTIONS):
            decode(_recanonical(decoded))


@pytest.mark.parametrize("form", ("channel", "declaration"))
def test_unknown_channel_rejects(form: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded["channel_id"] = "runtime.strategy_signal.envelope"
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


def test_channel_payload_model_must_match_its_frozen_binding() -> None:
    payload, decode = _raw_forms()["channel"]
    decoded = _decoded(payload)
    decoded["payload_model"] = "rquant.signal_contracts.CurrentSignalEnvelope"
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


def test_bundle_and_declaration_schemas_do_not_accept_each_other_fields() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)
    bundle_payload = _decoded(successor.successor_bundle_canonical_json_bytes(bundle))
    declaration_payload = _decoded(
        successor.overlay_declaration_canonical_json_bytes(overlay.declarations[0])
    )
    with pytest.raises(REJECTIONS):
        successor.OverlayDeclarationV1.from_canonical_json(_recanonical(bundle_payload))
    with pytest.raises(REJECTIONS):
        successor.SuccessorBundleV1.from_canonical_json(
            _recanonical(declaration_payload), closure=_closure()
        )
    with pytest.raises(REJECTIONS):
        successor.OverlayBundleV1.from_canonical_json(_recanonical(bundle_payload))
    with pytest.raises(REJECTIONS):
        successor.SuccessorChannelV1.from_canonical_json(
            _recanonical(declaration_payload), closure=_closure()
        )
    merged = dict(declaration_payload)
    merged["schema_version"] = 1
    with pytest.raises(REJECTIONS):
        successor.OverlayDeclarationV1.from_canonical_json(_recanonical(merged))


# --------------------------------------------------------------------------------------
# Overlay may not add anything absent from its successor base
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    (
        "payload_model",
        "producer_service_ids",
        "consumer_service_ids",
        "physical_schema_fingerprint",
        "declaration_schema_fingerprint",
        "channel_hash",
        "extra_family_field",
    ),
)
def test_overlay_declaration_cannot_carry_base_only_or_new_fields(field: str) -> None:
    payload, decode = _raw_forms()["declaration"]
    decoded = _decoded(payload)
    decoded[field] = ["x"] if field.endswith("_ids") else "x"
    with pytest.raises(REJECTIONS):
        decode(_recanonical(decoded))


def test_overlay_cannot_bind_a_channel_absent_from_its_base() -> None:
    registry, records = _registry()
    partial = _bundle(channels=(_channel("signal-envelope/current"),))
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(partial))
    full = _bundle()
    stray = successor.OverlayBundleV1.create(
        base_bundle_content_hash=partial.content_hash,
        declarations=(
            successor.OverlayDeclarationV1.create(
                channel=full.channel("signal-route-spool-record/current"),
                base_bundle_content_hash=partial.content_hash,
                accepted_family_ids=successor.ACCEPTED_FAMILY_IDS,
                pair_ids=("router-notifier",),
            ),
        ),
    )
    with pytest.raises(REJECTIONS):
        registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(stray))
    assert registry.staged_overlay() is None
    assert records == []


@pytest.mark.parametrize(
    "field",
    (
        "base_bundle_content_hash",
        "base_declaration_fingerprint",
        "base_physical_fingerprint",
        "model_descriptor_hash",
    ),
)
def test_overlay_declaration_rebinding_a_base_hash_rejects(field: str) -> None:
    registry, _ = _registry()
    bundle = _bundle()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    overlay = _overlay(bundle)
    decoded = _decoded(successor.overlay_bundle_canonical_json_bytes(overlay))
    declaration = dict(decoded["declarations"][0])
    declaration[field] = "b" * 64
    declaration.pop("declaration_hash")
    declaration["declaration_hash"] = successor._canonical_sha256(declaration)
    decoded["declarations"] = [declaration, *decoded["declarations"][1:]]
    decoded.pop("content_hash")
    decoded["content_hash"] = successor._canonical_sha256(decoded)
    with pytest.raises(REJECTIONS):
        registry.stage_overlay(_recanonical(decoded))
    assert registry.staged_overlay() is None


def test_overlay_cannot_accept_a_legacy_or_unknown_family() -> None:
    bundle = _bundle()
    for family in ("rquant.signal-envelope/v0", "legacy", "runtime.strategy_signal.envelope"):
        with pytest.raises(REJECTIONS):
            _declaration(bundle, "signal-envelope/current", accepted_family_ids=(family,))


def test_accepted_family_ids_domain_is_the_current_envelope_literal() -> None:
    assert successor.ACCEPTED_FAMILY_IDS == (CURRENT_ENVELOPE_SCHEMA,)
    assert successor.ACCEPTED_FAMILY_IDS == ("rquant.signal-envelope/v1",)


def test_pair_ids_are_the_frozen_five_and_accept_only_sorted_subsets() -> None:
    assert successor.PAIR_IDS == (
        "notifier-serving",
        "router-notifier",
        "router-paper",
        "strategy-router",
        "strategy-shadow",
    )
    bundle = _bundle()
    assert _declaration(bundle, "signal-envelope/current", pair_ids=("strategy-router",))
    assert _declaration(bundle, "signal-envelope/current", pair_ids=successor.PAIR_IDS)
    with pytest.raises(REJECTIONS):
        _declaration(bundle, "signal-envelope/current", pair_ids=("strategy-live",))
    with pytest.raises(REJECTIONS):
        _declaration(
            bundle,
            "signal-envelope/current",
            pair_ids=("strategy-router", "router-notifier"),
        )


# --------------------------------------------------------------------------------------
# Actual manifest-covered payload models (Explicit Phase Blocker 2)
# --------------------------------------------------------------------------------------


def test_successor_channel_bindings_are_a_closed_current_family_set() -> None:
    assert dict(successor.SUCCESSOR_CHANNEL_BINDINGS) == {
        "signal-bus-routed-record/current": (
            "rquant.signal_route_spool.CurrentSignalBusRoutedRecord"
        ),
        "signal-envelope/current": "rquant.signal_contracts.CurrentSignalEnvelope",
        "signal-route-spool-record/current": (
            "rquant.signal_route_spool.CurrentSignalRouteSpoolRecord"
        ),
    }
    assert tuple(successor.SUCCESSOR_CHANNEL_BINDINGS) == tuple(
        sorted(successor.SUCCESSOR_CHANNEL_BINDINGS)
    )


def test_successor_channel_ids_are_disjoint_from_the_v2_catalog() -> None:
    assert not set(successor.SUCCESSOR_CHANNEL_BINDINGS) & set(_CHANNEL_SPEC_BY_ID)
    v2_models = {spec.payload_model for spec in _CHANNEL_SPEC_BY_ID.values()}
    resolved = {
        successor.resolve_payload_model(qualname, closure=_closure())
        for qualname in successor.SUCCESSOR_CHANNEL_BINDINGS.values()
    }
    assert not resolved & v2_models


def test_channel_id_and_payload_model_must_match_the_frozen_binding() -> None:
    with pytest.raises(REJECTIONS):
        successor.resolve_successor_channel_descriptor(
            "signal-envelope/current",
            "rquant.signal_route_spool.CurrentSignalRouteSpoolRecord",
            closure=_closure(),
        )
    with pytest.raises(REJECTIONS):
        successor.resolve_successor_channel_descriptor(
            "runtime.strategy_signal.envelope",
            "rquant.signal_contracts.SignalEnvelope",
            closure=_closure(),
        )


@pytest.mark.parametrize(
    "qualname",
    (
        "rquant.signal_contracts.FutureCurrentSignalEnvelope",
        "rquant.signal_family_future_module.CurrentSignalEnvelope",
        "rquant.signal_contracts.CurrentSignalEnvelope.Future",
        "rquant.signal_contracts.parse_signal_envelope",
        "rquant.signal_contracts.CURRENT_ENVELOPE_SCHEMA",
        "rquant.signal_contracts.LegacySignalEnvelope",
        "CurrentSignalEnvelope",
        "",
    ),
)
def test_missing_future_or_unresolvable_payload_models_reject(qualname: str) -> None:
    with pytest.raises(REJECTIONS):
        successor.resolve_payload_model(qualname, closure=_closure())


def test_payload_model_outside_the_generation_source_closure_rejects() -> None:
    narrow = _closure(frozenset({"src/rquant/signal_contracts.py"}))
    assert successor.resolve_payload_model(
        "rquant.signal_contracts.CurrentSignalEnvelope", closure=narrow
    )
    with pytest.raises(REJECTIONS):
        successor.resolve_payload_model(
            "rquant.signal_route_spool.CurrentSignalBusRoutedRecord", closure=narrow
        )
    with pytest.raises(REJECTIONS):
        successor.SuccessorChannelV1.create(
            "signal-route-spool-record/current",
            producer_service_ids=("signal-router",),
            consumer_service_ids=("notifier",),
            closure=narrow,
        )


def test_a_descriptor_computed_from_a_qualname_alone_rejects() -> None:
    channel = _channel("signal-envelope/current")
    forged_declaration = hashlib.sha256(channel.payload_model.encode()).hexdigest()
    forged_physical = hashlib.sha256(channel.payload_model.encode() + b"physical").hexdigest()
    values: dict[str, Any] = {
        "channel_id": channel.channel_id,
        "payload_model": channel.payload_model,
        "declaration_schema_fingerprint": forged_declaration,
        "physical_schema_fingerprint": forged_physical,
        "producer_service_ids": list(channel.producer_service_ids),
        "consumer_service_ids": list(channel.consumer_service_ids),
    }
    values["model_descriptor_hash"] = successor._canonical_sha256(
        {
            "payload_model": channel.payload_model,
            "declaration_schema_fingerprint": forged_declaration,
            "physical_schema_fingerprint": forged_physical,
        }
    )
    values["channel_hash"] = successor._canonical_sha256(values)
    with pytest.raises(REJECTIONS):
        successor.SuccessorChannelV1.from_canonical_json(_recanonical(values), closure=_closure())


def test_fingerprints_come_from_the_repository_authorities() -> None:
    registry_module = importlib.import_module("rquant.runtime_schema_registry")
    channel_id = "signal-envelope/current"
    qualname = successor.SUCCESSOR_CHANNEL_BINDINGS[channel_id]
    model = successor.resolve_payload_model(qualname, closure=_closure())
    declaration = registry_module._declaration(
        registry_module._ChannelSpec(channel_id, model, (), ()),
        producer_commit=PRODUCER_COMMIT,
    )
    physical = registry_module.RuntimePhysicalTableSchema.create(
        object_name=qualname, declaration=declaration
    )
    channel = _channel(channel_id)
    assert channel.declaration_schema_fingerprint == declaration.schema_fingerprint
    assert channel.physical_schema_fingerprint == physical.physical_schema_fingerprint


def test_a_different_producer_commit_changes_the_declaration_fingerprint() -> None:
    first = successor.resolve_successor_channel_descriptor(
        "signal-envelope/current",
        successor.SUCCESSOR_CHANNEL_BINDINGS["signal-envelope/current"],
        closure=_closure(),
    )
    other = successor.resolve_successor_channel_descriptor(
        "signal-envelope/current",
        successor.SUCCESSOR_CHANNEL_BINDINGS["signal-envelope/current"],
        closure=successor.SuccessorGenerationSourceClosureV1(
            producer_commit="b" * 40,
            source_closure=CLOSURE_FILES,
        ),
    )
    assert first.declaration_schema_fingerprint != other.declaration_schema_fingerprint
    assert first.physical_schema_fingerprint == other.physical_schema_fingerprint
    assert first.model_descriptor_hash != other.model_descriptor_hash


# --------------------------------------------------------------------------------------
# Registry: successor before overlay, idempotent replay, conflict audit
# --------------------------------------------------------------------------------------


def test_absent_overlay_grants_nothing() -> None:
    registry, records = _registry()
    bundle = _bundle()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    assert registry.successor_bundle() == bundle
    assert registry.staged_overlay() is None
    assert records == []


def test_overlay_before_its_successor_bundle_rejects() -> None:
    registry, records = _registry()
    overlay = _overlay(_bundle())
    with pytest.raises(REJECTIONS):
        registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(overlay))
    assert registry.staged_overlay() is None
    assert records == []


def test_byte_identical_replay_is_idempotent() -> None:
    registry, records = _registry()
    bundle = _bundle()
    raw_bundle = successor.successor_bundle_canonical_json_bytes(bundle)
    first = registry.register_successor_bundle(raw_bundle)
    second = registry.register_successor_bundle(bytes(raw_bundle))
    assert first == second == bundle
    overlay = _overlay(bundle)
    raw_overlay = successor.overlay_bundle_canonical_json_bytes(overlay)
    staged = registry.stage_overlay(raw_overlay)
    assert registry.stage_overlay(bytes(raw_overlay)) == staged
    assert records == []


def test_partial_overlay_stages_but_never_reports_completion() -> None:
    registry, records = _registry()
    bundle = _bundle()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    partial = _overlay(bundle, channel_ids=("signal-envelope/current",))
    staged = registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(partial))
    assert staged.complete is False
    assert staged.covered_channel_ids == ("signal-envelope/current",)
    assert staged.missing_channel_ids == (
        "signal-bus-routed-record/current",
        "signal-route-spool-record/current",
    )
    assert records == []
    assert not hasattr(staged, "ready")


def test_complete_overlay_reports_coverage_without_readiness() -> None:
    registry, _ = _registry()
    bundle = _bundle()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    staged = registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(_overlay(bundle)))
    assert staged.complete is True
    assert staged.missing_channel_ids == ()
    assert "READY" not in repr(staged)
    assert set(successor.StagedOverlayStateV1.model_fields) == {
        "base_bundle_content_hash",
        "overlay_content_hash",
        "covered_channel_ids",
        "missing_channel_ids",
        "complete",
    }


def test_conflicting_bundle_identity_appends_audit_and_rejects() -> None:
    registry, records = _registry()
    first = _bundle()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(first))
    divergent_channels = (
        _channel("signal-bus-routed-record/current"),
        _channel("signal-envelope/current", producers=("strategy-live-c",)),
        _channel("signal-route-spool-record/current"),
    )
    second = _bundle(channels=divergent_channels)
    with pytest.raises(REJECTIONS):
        registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(second))
    assert registry.successor_bundle() == first
    assert [record.identity_kind for record in records] == ["bundle", "channel"]
    assert records[0].identity == "rquant.signal-family.successor"
    assert records[0].existing_hash == first.content_hash
    assert records[0].attempted_hash == second.content_hash
    assert records[1].identity == "signal-envelope/current"
    assert records[1].existing_hash == first.channel("signal-envelope/current").channel_hash
    assert records[1].attempted_hash == second.channel("signal-envelope/current").channel_hash


def test_conflicting_overlay_identity_appends_audit_and_rejects() -> None:
    registry, records = _registry()
    bundle = _bundle()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    first = _overlay(bundle)
    staged = registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(first))
    divergent = successor.OverlayBundleV1.create(
        base_bundle_content_hash=bundle.content_hash,
        declarations=tuple(
            _declaration(bundle, channel.channel_id, pair_ids=("router-paper",))
            for channel in bundle.channels
        ),
    )
    with pytest.raises(REJECTIONS):
        registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(divergent))
    assert registry.staged_overlay() == staged
    assert [record.identity_kind for record in records] == [
        "overlay",
        "declaration",
        "declaration",
        "declaration",
    ]
    assert records[0].existing_hash == first.content_hash
    assert records[0].attempted_hash == divergent.content_hash
    assert [record.identity for record in records[1:]] == [
        channel.channel_id for channel in bundle.channels
    ]


def test_conflict_audit_records_are_bounded_identifiers_only() -> None:
    assert set(successor.SuccessorConflictAuditRecordV1.model_fields) == {
        "identity_kind",
        "identity",
        "existing_hash",
        "attempted_hash",
    }
    record = successor.SuccessorConflictAuditRecordV1(
        identity_kind="bundle",
        identity="rquant.signal-family.successor",
        existing_hash="a" * 64,
        attempted_hash="b" * 64,
    )
    dumped = record.model_dump(mode="json")
    assert set(dumped) == {"identity_kind", "identity", "existing_hash", "attempted_hash"}
    assert all(isinstance(value, str) for value in dumped.values())
    with pytest.raises(REJECTIONS):
        successor.SuccessorConflictAuditRecordV1(
            identity_kind="payload",
            identity="x",
            existing_hash="a" * 64,
            attempted_hash="b" * 64,
        )


def test_registry_rejects_an_unknown_base_bundle_hash() -> None:
    registry, records = _registry()
    bundle = _bundle()
    other = _bundle(channels=(_channel("signal-envelope/current"),))
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    with pytest.raises(REJECTIONS):
        registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(_overlay(other)))
    assert registry.staged_overlay() is None
    assert records == []


def test_registry_rejects_a_bundle_whose_model_is_outside_the_injected_closure() -> None:
    records: list[successor.SuccessorConflictAuditRecordV1] = []

    class _ListSink:
        def append(self, record: successor.SuccessorConflictAuditRecordV1) -> None:
            records.append(record)

    narrow = successor.SuccessorRegistry(
        closure=_closure(frozenset({"src/rquant/signal_contracts.py"})),
        audit_sink=_ListSink(),
    )
    with pytest.raises(REJECTIONS):
        narrow.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(_bundle()))
    assert narrow.successor_bundle() is None
    assert records == []


# --------------------------------------------------------------------------------------
# v2 stays the sole old authority, and Phase B never activates anything
# --------------------------------------------------------------------------------------


def test_v2_contract_bundle_fields_and_catalog_are_unchanged_and_overlay_free() -> None:
    assert tuple(RuntimeSchemaContractBundle.model_fields) == (
        "schema_version",
        "producer_commit",
        "manifest_fingerprints",
        "serving_physical_schema_fingerprint",
        "channels",
        "content_hash",
    )
    assert len(_CHANNEL_SPEC_BY_ID) == 21
    v2_source = Path(
        inspect.getsourcefile(importlib.import_module("rquant.runtime_schema_registry")) or ""
    ).read_text(encoding="utf-8")
    assert "successor" not in v2_source.lower()
    assert "overlay" not in v2_source.lower()


def test_successor_module_is_absent_from_production_import_closures() -> None:
    modules = (
        "rquant.runtime_builder_daily_orchestrator",
        "rquant.runtime_builder_paper",
        "rquant.runtime_builder_serving",
        "rquant.runtime_builder_shadow",
        "rquant.runtime_builder_signal",
        "rquant.runtime_builder_strategy",
        "rquant.runtime_service_builtin",
        "rquant.runtime_service_main",
        "rquant.signal_route_spool",
        "rquant.runtime_schema_registry",
    )
    script = (
        "import importlib, sys\n"
        f"for name in {modules!r}:\n"
        "    importlib.import_module(name)\n"
        "from rquant.runtime_service_main import build_builtin_registry\n"
        "assert callable(build_builtin_registry)\n"
        "print('rquant.signal_family_successor_registry' in sys.modules)\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        check=True,
    )
    assert completed.stdout.strip().splitlines()[-1] == "False"


def test_module_declares_no_activation_readiness_or_durable_writer() -> None:
    source = Path(inspect.getsourcefile(successor) or "").read_text(encoding="utf-8")
    for token in (
        "READY",
        "ATTESTING",
        "ACTIVATED",
        "r07_overlay",
        "v3_overlay",
        "v3_writer",
        "publish_v3",
        "cutover",
        "drain",
        "cursor",
        "high_watermark",
        "os.replace",
        "open(",
        "write_bytes",
        "write_text",
        "mkdir",
        "duckdb",
        "sqlite",
    ):
        assert token not in source, token


def test_public_surface_exposes_no_writer_or_activation_callable() -> None:
    exported = {
        name
        for name in dir(successor)
        if not name.startswith("_") and callable(getattr(successor, name))
    }
    assert not {name for name in exported if "writ" in name.lower()}
    assert not {name for name in exported if "activ" in name.lower()}
    assert not {name for name in exported if "ready" in name.lower()}
    assert successor.SuccessorRegistry.__mro__[1:] == (object,)
    assert issubclass(successor.SuccessorChannelV1, BaseModel)
