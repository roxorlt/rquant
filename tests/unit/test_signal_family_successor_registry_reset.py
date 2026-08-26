"""Phase B successor base registry and staged overlay red tests.

Covers `RESET-REG-P1` and `RESET-REG-P1-01`: the four frozen schemas, their canonical
preimages and raw bytes, the strict structural rejection matrix, the actual-model
prerequisite, and the fact that a partial, absent, or conflicting overlay never becomes
ready while v2 stays untouched.

Every rejection case makes its own rule the first failing check — variants that change a
hashed field recompute the affected `channel_hash` / `content_hash` / `declaration_hash`
so a hash mismatch cannot stand in for the rule under test — and pins the exact rejection
reason with `match=`, so two different rules can never satisfy one another's assertion.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import inspect
import json
import re
import subprocess
import sys
import typing
from pathlib import Path
from types import MappingProxyType
from typing import Any

import pytest
from pydantic import BaseModel

from rquant import runtime_contracts
from rquant import signal_family_constants as constants
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
OTHER_PRODUCER_COMMIT = "b" * 40

# The frozen role domain of `rquant.signal_family_constants`: strategy services produce
# envelopes, the router produces routed and spool records, and the notifier, paper broker
# and shadow session only ever consume. `signal-router-shadow` used to sit in two producer
# tuples; it names no role at all, so it is not a legal participant.
_CHANNEL_SERVICES: dict[str, tuple[tuple[str, ...], tuple[str, ...]]] = {
    "signal-bus-routed-record/current": (
        ("signal-router",),
        ("notifier", "paper-broker"),
    ),
    "signal-envelope/current": (
        ("strategy-live-a", "strategy-live-b"),
        ("shadow-session", "signal-router"),
    ),
    "signal-route-spool-record/current": (
        ("signal-router",),
        ("notifier", "paper-broker"),
    ),
}

# The hash field each frozen schema derives from the rest of its own canonical content.
_HASH_FIELD = {
    "channel": "channel_hash",
    "bundle": "content_hash",
    "declaration": "declaration_hash",
    "overlay": "content_hash",
}


def _closure(
    files: frozenset[str] = CLOSURE_FILES,
    *,
    producer_commit: str = PRODUCER_COMMIT,
) -> successor.SuccessorGenerationSourceClosureV1:
    return successor.SuccessorGenerationSourceClosureV1(
        producer_commit=producer_commit,
        source_closure=files,
    )


def _channel(
    channel_id: str,
    *,
    producers: tuple[str, ...] | None = None,
    consumers: tuple[str, ...] | None = None,
    closure: successor.SuccessorGenerationSourceClosureV1 | None = None,
) -> successor.SuccessorChannelV1:
    declared_producers, declared_consumers = _CHANNEL_SERVICES[channel_id]
    return successor.SuccessorChannelV1.create(
        channel_id,
        producer_service_ids=declared_producers if producers is None else producers,
        consumer_service_ids=declared_consumers if consumers is None else consumers,
        closure=_closure() if closure is None else closure,
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


def _rehashed(form: str, value: dict[str, Any]) -> bytes:
    """Re-derive the schema's own hash so it can never mask the rule under test."""

    field = _HASH_FIELD[form]
    preimage = {key: item for key, item in value.items() if key != field}
    return canonical_json_bytes({**preimage, field: successor._canonical_sha256(preimage)})


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
    ("whitespace", _with_space, "persistent JSON is not canonical"),
    ("unicode-escape", _with_unicode_escape, "persistent JSON is not canonical"),
    ("key-order", _with_reordered_keys, "persistent JSON is not canonical"),
    ("newline", _with_trailing_newline, "persistent JSON is not canonical"),
    ("duplicate-key", _with_duplicate_key, "duplicate JSON key"),
)

_EXTRA_KEY = "Extra inputs are not permitted"
_NOT_AN_OBJECT = "successor authority payload must be a JSON object"


def _raw_forms() -> dict[str, tuple[bytes, Any]]:
    bundle = _bundle()
    overlay = _overlay(bundle)
    return {
        # The envelope channel is the raw "channel" form because it is the only one whose
        # producer and consumer tuples both have two members, which the sort/duplicate
        # mutators need in order to fail their own rule rather than pass vacuously.
        "channel": (
            successor.successor_channel_canonical_json_bytes(
                bundle.channel("signal-envelope/current")
            ),
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
    ("form", "field", "value", "reason"),
    (
        (
            "bundle",
            "bundle_namespace",
            "rquant.signal-family.overlay",
            "Input should be 'rquant.signal-family.successor'",
        ),
        ("bundle", "schema_version", 2, "unsupported successor bundle schema version"),
        (
            "overlay",
            "overlay_namespace",
            "rquant.signal-family.successor",
            "Input should be 'rquant.signal-family.overlay'",
        ),
        ("overlay", "overlay_version", 2, "unsupported overlay bundle version"),
    ),
)
def test_wrong_namespace_or_version_rejects(
    form: str,
    field: str,
    value: Any,
    reason: str,
) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = value
    with pytest.raises(REJECTIONS, match=reason):
        decode(_rehashed(form, decoded))


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
    with pytest.raises(TypeError, match="requires an exact SuccessorBundleV1 object"):
        successor.successor_bundle_canonical_json_bytes(subclass)
    with pytest.raises(TypeError, match="requires an exact SuccessorChannelV1 object"):
        successor.successor_channel_canonical_json_bytes(bundle)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="requires an exact OverlayBundleV1 object"):
        successor.overlay_bundle_canonical_json_bytes(bundle)  # type: ignore[arg-type]


# --------------------------------------------------------------------------------------
# Strict structural rejection matrix (authority.md L1162-1172)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
@pytest.mark.parametrize("mutator", NONCANONICAL_MUTATORS, ids=lambda item: item[0])
def test_noncanonical_representations_reject(form: str, mutator: tuple[str, Any, str]) -> None:
    payload, decode = _raw_forms()[form]
    with pytest.raises(REJECTIONS, match=mutator[2]):
        decode(mutator[1](payload))


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
def test_extra_keys_reject(form: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded["unexpected_field"] = "x"
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
        decode(_rehashed(form, decoded))


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
def test_aliased_field_names_reject(form: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    first = sorted(decoded)[0]
    decoded["".join(part.capitalize() for part in first.split("_"))] = decoded.pop(first)
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
        decode(_recanonical(decoded))


@pytest.mark.parametrize("form", ("channel", "bundle", "declaration", "overlay"))
@pytest.mark.parametrize("payload", (b"[]", b'"x"', b"1", b"null"))
def test_non_object_payloads_reject(form: str, payload: bytes) -> None:
    _, decode = _raw_forms()[form]
    with pytest.raises(REJECTIONS, match=_NOT_AN_OBJECT):
        decode(payload)


@pytest.mark.parametrize(
    ("form", "field", "value", "reason"),
    (
        # `channel_id` is a `Literal` closed set, so a non-member of any type fails the
        # frozen membership rule at the type layer rather than a generic string check.
        ("channel", "channel_id", 1, "Input should be 'signal-bus-routed-record/current'"),
        ("channel", "declaration_schema_fingerprint", 0, "Input should be a valid string"),
        ("channel", "producer_service_ids", "signal-router", "must be a JSON array"),
        ("bundle", "schema_version", "1", "Input should be a valid integer"),
        ("bundle", "schema_version", 1.0, "Input should be a valid integer"),
        ("bundle", "channels", {}, "channels must be a JSON array"),
        ("declaration", "pair_ids", "strategy-router", "must be a JSON array"),
        ("declaration", "base_bundle_content_hash", 0, "Input should be a valid string"),
        ("overlay", "overlay_version", "1", "Input should be a valid integer"),
        ("overlay", "declarations", {}, "declarations must be a JSON array"),
    ),
)
def test_coerced_scalar_types_reject(form: str, field: str, value: Any, reason: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = value
    with pytest.raises(REJECTIONS, match=reason):
        decode(_rehashed(form, decoded))


@pytest.mark.parametrize(
    ("form", "field"),
    (("bundle", "schema_version"), ("overlay", "overlay_version")),
)
def test_booleans_are_not_accepted_as_integers(form: str, field: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = True
    with pytest.raises(REJECTIONS, match="Input should be a valid integer"):
        decode(_rehashed(form, decoded))


def test_mappings_or_subclasses_cannot_replace_the_exact_nested_models() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)

    class _ChannelSubclass(successor.SuccessorChannelV1):
        pass

    class _DeclarationSubclass(successor.OverlayDeclarationV1):
        pass

    channel_reason = "channels requires an exact SuccessorChannelV1 object"
    declaration_reason = "declarations requires an exact OverlayDeclarationV1 object"
    channel_mapping = MappingProxyType(bundle.channels[0].model_dump(mode="json"))
    with pytest.raises(TypeError, match=channel_reason):
        successor.SuccessorBundleV1.create(channels=(channel_mapping,))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match=channel_reason):
        successor.SuccessorBundleV1.create(
            channels=(_ChannelSubclass.model_construct(**dict(bundle.channels[0])),)
        )
    declaration_mapping = MappingProxyType(overlay.declarations[0].model_dump(mode="json"))
    with pytest.raises(TypeError, match=declaration_reason):
        successor.OverlayBundleV1.create(
            base_bundle_content_hash=bundle.content_hash,
            declarations=(declaration_mapping,),  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError, match=declaration_reason):
        successor.OverlayBundleV1.create(
            base_bundle_content_hash=bundle.content_hash,
            declarations=(_DeclarationSubclass.model_construct(**dict(overlay.declarations[0])),),
        )


@pytest.mark.parametrize(
    ("form", "field", "reason"),
    (
        (
            "channel",
            "producer_service_ids",
            "producer_service_ids must be sorted and duplicate-free",
        ),
        (
            "channel",
            "consumer_service_ids",
            "consumer_service_ids must be sorted and duplicate-free",
        ),
        ("declaration", "pair_ids", "pair_ids must be sorted and duplicate-free"),
        (
            "declaration",
            "accepted_family_ids",
            "accepted_family_ids must be sorted and duplicate-free",
        ),
        ("bundle", "channels", "successor bundle channels must be sorted and duplicate-free"),
        ("overlay", "declarations", "overlay declarations must be sorted and duplicate-free"),
    ),
)
def test_duplicated_tuple_variants_reject_for_the_duplicate_rule(
    form: str,
    field: str,
    reason: str,
) -> None:
    """A repeated entry must fail the sort/duplicate rule, not a stale hash."""

    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    items = decoded[field]
    decoded[field] = [items[0], items[0]]
    with pytest.raises(REJECTIONS, match=reason):
        decode(_rehashed(form, decoded))


@pytest.mark.parametrize(
    ("form", "field", "reason"),
    (
        (
            "channel",
            "producer_service_ids",
            "producer_service_ids must be sorted and duplicate-free",
        ),
        (
            "channel",
            "consumer_service_ids",
            "consumer_service_ids must be sorted and duplicate-free",
        ),
        ("declaration", "pair_ids", "pair_ids must be sorted and duplicate-free"),
        ("bundle", "channels", "successor bundle channels must be sorted and duplicate-free"),
        ("overlay", "declarations", "overlay declarations must be sorted and duplicate-free"),
    ),
)
def test_unsorted_tuple_variants_reject_for_the_order_rule(
    form: str,
    field: str,
    reason: str,
) -> None:
    """A reversed but otherwise self-consistent payload must fail on order alone."""

    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    items = decoded[field]
    assert len(items) >= 2, f"{form}.{field} fixture must carry at least two entries"
    decoded[field] = list(reversed(items))
    reversed_bytes = _rehashed(form, decoded)
    # The reversed payload is byte-canonical and hash-consistent: only order is wrong.
    assert json.loads(reversed_bytes)[_HASH_FIELD[form]] != _decoded(payload)[_HASH_FIELD[form]]
    with pytest.raises(REJECTIONS, match=reason):
        decode(reversed_bytes)


@pytest.mark.parametrize(
    ("form", "field", "reason"),
    (
        ("channel", "producer_service_ids", "producer_service_ids must be nonempty"),
        ("channel", "consumer_service_ids", "consumer_service_ids must be nonempty"),
        ("declaration", "pair_ids", "pair_ids must be nonempty"),
        ("declaration", "accepted_family_ids", "accepted_family_ids must be nonempty"),
        ("bundle", "channels", "successor bundle must declare at least one channel"),
        ("overlay", "declarations", "overlay bundle must declare at least one channel"),
    ),
)
def test_empty_tuple_fields_reject(form: str, field: str, reason: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = []
    with pytest.raises(REJECTIONS, match=reason):
        decode(_rehashed(form, decoded))


@pytest.mark.parametrize(
    ("form", "field", "reason"),
    (
        (
            "channel",
            "model_descriptor_hash",
            "successor channel descriptor hash does not match its preimage",
        ),
        ("channel", "channel_hash", "successor channel hash does not match its canonical content"),
        (
            "bundle",
            "content_hash",
            "successor bundle content hash does not match its canonical content",
        ),
        (
            "declaration",
            "declaration_hash",
            "overlay declaration hash does not match its canonical content",
        ),
        (
            "overlay",
            "content_hash",
            "overlay bundle content hash does not match its canonical content",
        ),
    ),
)
def test_hash_mismatch_rejects(form: str, field: str, reason: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = "f" * 64
    with pytest.raises(REJECTIONS, match=reason):
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
@pytest.mark.parametrize("value", ("F" * 64, "f" * 63, "f" * 65, ""))
def test_non_lowercase_hex_digests_reject(form: str, field: str, value: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded[field] = value
    with pytest.raises(REJECTIONS, match="String should match pattern"):
        decode(_recanonical(decoded))


@pytest.mark.parametrize("form", ("channel", "declaration"))
def test_unknown_channel_rejects(form: str) -> None:
    payload, decode = _raw_forms()[form]
    decoded = _decoded(payload)
    decoded["channel_id"] = "runtime.strategy_signal.envelope"
    with pytest.raises(
        REJECTIONS,
        match="unknown successor transport channel: runtime.strategy_signal.envelope",
    ):
        decode(_rehashed(form, decoded))


def test_channel_payload_model_must_match_its_frozen_binding() -> None:
    payload, decode = _raw_forms()["channel"]
    decoded = _decoded(payload)
    decoded["payload_model"] = "rquant.signal_route_spool.CurrentSignalBusRoutedRecord"
    with pytest.raises(
        REJECTIONS,
        match="successor channel payload model is not its frozen binding",
    ):
        decode(_rehashed("channel", decoded))


def test_bundle_and_declaration_schemas_do_not_accept_each_other_fields() -> None:
    bundle = _bundle()
    overlay = _overlay(bundle)
    bundle_payload = _decoded(successor.successor_bundle_canonical_json_bytes(bundle))
    declaration_payload = _decoded(
        successor.overlay_declaration_canonical_json_bytes(overlay.declarations[0])
    )
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
        successor.OverlayDeclarationV1.from_canonical_json(_recanonical(bundle_payload))
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
        successor.SuccessorBundleV1.from_canonical_json(
            _recanonical(declaration_payload), closure=_closure()
        )
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
        successor.OverlayBundleV1.from_canonical_json(_recanonical(bundle_payload))
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
        successor.SuccessorChannelV1.from_canonical_json(
            _recanonical(declaration_payload), closure=_closure()
        )
    merged = dict(declaration_payload)
    merged["schema_version"] = 1
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
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
    with pytest.raises(REJECTIONS, match=_EXTRA_KEY):
        decode(_rehashed("declaration", decoded))


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
    with pytest.raises(
        REJECTIONS,
        match="channel is absent from the successor base: signal-route-spool-record/current",
    ):
        registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(stray))
    assert registry.staged_overlay() is None
    assert records == []


@pytest.mark.parametrize(
    ("field", "reason"),
    (
        (
            "base_bundle_content_hash",
            "overlay declaration names a different successor base bundle",
        ),
        (
            "base_declaration_fingerprint",
            "overlay declaration descriptor hash does not match its base",
        ),
        (
            "base_physical_fingerprint",
            "overlay declaration descriptor hash does not match its base",
        ),
        ("model_descriptor_hash", "overlay declaration descriptor hash does not match its base"),
    ),
)
def test_overlay_declaration_rebinding_a_base_hash_rejects(field: str, reason: str) -> None:
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
    with pytest.raises(REJECTIONS, match=reason):
        registry.stage_overlay(_rehashed("overlay", decoded))
    assert registry.staged_overlay() is None


def test_a_self_consistent_declaration_for_another_generation_fails_the_base_binding() -> None:
    """Only the registry can catch a declaration that is valid but binds other bytes."""

    registry, records = _registry()
    bundle = _bundle()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    foreign_channel = _channel(
        "signal-envelope/current",
        closure=_closure(producer_commit=OTHER_PRODUCER_COMMIT),
    )
    assert (
        foreign_channel.declaration_schema_fingerprint
        != bundle.channel("signal-envelope/current").declaration_schema_fingerprint
    )
    foreign = successor.OverlayBundleV1.create(
        base_bundle_content_hash=bundle.content_hash,
        declarations=(
            successor.OverlayDeclarationV1.create(
                channel=foreign_channel,
                base_bundle_content_hash=bundle.content_hash,
                accepted_family_ids=successor.ACCEPTED_FAMILY_IDS,
                pair_ids=("strategy-router",),
            ),
        ),
    )
    raw = successor.overlay_bundle_canonical_json_bytes(foreign)
    # The declaration decodes on its own; only the base binding rejects it.
    assert successor.OverlayBundleV1.from_canonical_json(raw) == foreign
    with pytest.raises(
        REJECTIONS,
        match="overlay declaration does not bind its base channel: signal-envelope/current",
    ):
        registry.stage_overlay(raw)
    assert registry.staged_overlay() is None
    assert records == []


def test_overlay_cannot_accept_a_legacy_or_unknown_family() -> None:
    bundle = _bundle()
    for family in ("rquant.signal-envelope/v0", "legacy", "runtime.strategy_signal.envelope"):
        with pytest.raises(
            REJECTIONS,
            match="overlay declaration accepts a family absent from the current family",
        ):
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
    with pytest.raises(REJECTIONS, match="overlay declaration binds an unknown pair id"):
        _declaration(bundle, "signal-envelope/current", pair_ids=("strategy-live",))
    with pytest.raises(REJECTIONS, match="pair_ids must be sorted and duplicate-free"):
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
    with pytest.raises(
        REJECTIONS,
        match="channel signal-envelope/current is bound to "
        "rquant.signal_contracts.CurrentSignalEnvelope, not "
        "rquant.signal_route_spool.CurrentSignalRouteSpoolRecord",
    ):
        successor.resolve_successor_channel_descriptor(
            "signal-envelope/current",
            "rquant.signal_route_spool.CurrentSignalRouteSpoolRecord",
            closure=_closure(),
        )
    with pytest.raises(
        REJECTIONS,
        match="unknown successor transport channel: runtime.strategy_signal.envelope",
    ):
        successor.resolve_successor_channel_descriptor(
            "runtime.strategy_signal.envelope",
            "rquant.signal_contracts.SignalEnvelope",
            closure=_closure(),
        )


@pytest.mark.parametrize(
    ("qualname", "reason"),
    (
        (
            "rquant.signal_contracts.FutureCurrentSignalEnvelope",
            "payload_model does not resolve to a declared class",
        ),
        (
            "rquant.signal_family_future_module.CurrentSignalEnvelope",
            "payload_model does not resolve to a declared class",
        ),
        (
            "rquant.signal_contracts.CurrentSignalEnvelope.Future",
            "payload_model does not resolve to a declared class",
        ),
        (
            "rquant.signal_contracts.parse_signal_envelope",
            "payload_model must name a declared payload class",
        ),
        (
            "rquant.signal_contracts.CURRENT_ENVELOPE_SCHEMA",
            "payload_model must name a declared payload class",
        ),
        (
            "rquant.signal_contracts.LegacySignalEnvelope",
            "payload_model is an alias for rquant.signal_contracts.SignalEnvelope",
        ),
        ("CurrentSignalEnvelope", "payload_model is not a qualified name"),
        ("", "payload_model must be a nonempty string"),
    ),
)
def test_missing_future_or_unresolvable_payload_models_reject(qualname: str, reason: str) -> None:
    with pytest.raises(REJECTIONS, match=reason):
        successor.resolve_payload_model(qualname, closure=_closure())


def test_payload_model_outside_the_generation_source_closure_rejects() -> None:
    narrow = _closure(frozenset({"src/rquant/signal_contracts.py"}))
    reason = (
        "payload_model is not covered by the generation source closure: "
        "src/rquant/signal_route_spool.py"
    )
    assert successor.resolve_payload_model(
        "rquant.signal_contracts.CurrentSignalEnvelope", closure=narrow
    )
    with pytest.raises(REJECTIONS, match=reason):
        successor.resolve_payload_model(
            "rquant.signal_route_spool.CurrentSignalBusRoutedRecord", closure=narrow
        )
    with pytest.raises(REJECTIONS, match=reason):
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
    with pytest.raises(
        REJECTIONS,
        match="successor channel descriptor does not match the actual model: "
        "signal-envelope/current",
    ):
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
        closure=_closure(producer_commit=OTHER_PRODUCER_COMMIT),
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
    with pytest.raises(
        REJECTIONS,
        match="an overlay cannot be staged before its successor bundle is registered",
    ):
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
    with pytest.raises(
        successor.SuccessorConflictError,
        match="a different successor bundle is already registered for this identity",
    ):
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
    with pytest.raises(
        successor.SuccessorConflictError,
        match="a different overlay is already staged for this successor base",
    ):
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
    with pytest.raises(REJECTIONS, match="Input should be 'bundle', 'channel'"):
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
    with pytest.raises(
        REJECTIONS,
        match="the staged overlay does not name the registered successor bundle",
    ):
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
    with pytest.raises(
        successor.SuccessorRegistryError,
        match="payload_model is not covered by the generation source closure",
    ):
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


# --------------------------------------------------------------------------------------
# Ruling 2: the frozen channel set, family, identities and participant service-ID domain
# --------------------------------------------------------------------------------------


def test_the_constants_module_is_a_leaf_and_imports_nothing_from_rquant() -> None:
    """The whole reason the module exists: `Literal` needs module-level constants.

    The registry used to defer `from rquant.signal_contracts import ...` into a function
    body to dodge an import cycle. A leaf module removes the cycle instead of dodging it,
    so it must stay import-free of `rquant`.
    """

    source = Path(inspect.getsourcefile(constants) or "").read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imported.append(node.module)
    assert imported
    assert not [name for name in imported if name == "rquant" or name.startswith("rquant.")]


def test_the_registry_re_exports_every_lowered_constant_under_its_historical_name() -> None:
    """Downstream modules import these names from the registry and must keep working."""

    for name in (
        "ACCEPTED_FAMILY_IDS",
        "OVERLAY_NAMESPACE",
        "PAIR_IDS",
        "SUCCESSOR_BUNDLE_NAMESPACE",
        "SUCCESSOR_CHANNEL_BINDINGS",
    ):
        assert getattr(successor, name) is getattr(constants, name), name
        assert name in successor.__all__


def test_the_accepted_family_is_exactly_the_one_current_family() -> None:
    assert constants.ACCEPTED_FAMILY_IDS == (CURRENT_ENVELOPE_SCHEMA,)
    assert len(constants.ACCEPTED_FAMILY_IDS) == 1
    assert constants.CURRENT_ENVELOPE_FAMILY_ID == CURRENT_ENVELOPE_SCHEMA
    assert typing.get_args(constants.AcceptedFamilyId) == (CURRENT_ENVELOPE_SCHEMA,)


def test_a_declaration_accepting_a_second_family_rejects() -> None:
    bundle = _bundle()
    with pytest.raises(
        REJECTIONS,
        match="overlay declaration accepts a family absent from the current family",
    ):
        _declaration(
            bundle,
            "signal-envelope/current",
            accepted_family_ids=tuple(sorted((*constants.ACCEPTED_FAMILY_IDS, "other/v1"))),
        )


def test_the_channel_set_is_a_frozen_closed_set_at_the_type_layer() -> None:
    assert isinstance(constants.SUCCESSOR_CHANNEL_BINDINGS, MappingProxyType)
    assert tuple(sorted(constants.SUCCESSOR_CHANNEL_BINDINGS)) == (
        "signal-bus-routed-record/current",
        "signal-envelope/current",
        "signal-route-spool-record/current",
    )
    assert typing.get_args(constants.SuccessorChannelId) == tuple(
        sorted(constants.SUCCESSOR_CHANNEL_BINDINGS)
    )
    assert (
        successor.SuccessorChannelV1.model_fields["channel_id"].annotation
        is constants.SuccessorChannelId
    )
    assert (
        successor.OverlayDeclarationV1.model_fields["channel_id"].annotation
        is constants.SuccessorChannelId
    )
    with pytest.raises(TypeError):
        constants.SUCCESSOR_CHANNEL_BINDINGS["forged/current"] = "rquant.forged.Model"  # type: ignore[index]


def test_the_bundle_identity_is_its_namespace_and_nothing_else() -> None:
    """One successor base per process, so the namespace alone names the slot."""

    bundle = _bundle()
    other = successor.SuccessorBundleV1.create(
        channels=(_channel("signal-envelope/current"),)
    )
    assert bundle.identity == constants.SUCCESSOR_BUNDLE_NAMESPACE
    assert other.identity == bundle.identity
    assert other.content_hash != bundle.content_hash
    assert "identity" not in successor.SuccessorBundleV1.model_fields

    registry, records = _registry()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    with pytest.raises(successor.SuccessorConflictError):
        registry.register_successor_bundle(
            successor.successor_bundle_canonical_json_bytes(other)
        )
    assert records[0].identity_kind == "bundle"
    assert records[0].identity == constants.SUCCESSOR_BUNDLE_NAMESPACE


def test_the_overlay_identity_is_its_namespace_and_its_base_bundle_hash() -> None:
    bundle = _bundle()
    other_bundle = successor.SuccessorBundleV1.create(
        channels=(_channel("signal-envelope/current"),)
    )
    overlay = _overlay(bundle)
    other_overlay = _overlay(other_bundle, channel_ids=("signal-envelope/current",))

    assert overlay.identity == (constants.OVERLAY_NAMESPACE, bundle.content_hash)
    assert other_overlay.identity == (constants.OVERLAY_NAMESPACE, other_bundle.content_hash)
    assert overlay.identity != other_overlay.identity
    assert "identity" not in successor.OverlayBundleV1.model_fields

    registry, records = _registry()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(overlay))
    conflicting = _overlay(bundle, channel_ids=("signal-envelope/current",))
    with pytest.raises(successor.SuccessorConflictError):
        registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(conflicting))
    assert records[0].identity_kind == "overlay"
    assert records[0].identity == f"{constants.OVERLAY_NAMESPACE}:{bundle.content_hash}"


def test_an_overlay_naming_another_base_is_not_the_same_identity_and_rejects() -> None:
    bundle = _bundle()
    other_bundle = successor.SuccessorBundleV1.create(
        channels=(_channel("signal-envelope/current"),)
    )
    registry, _records = _registry()
    registry.register_successor_bundle(successor.successor_bundle_canonical_json_bytes(bundle))
    foreign = _overlay(other_bundle, channel_ids=("signal-envelope/current",))
    with pytest.raises(
        successor.SuccessorRegistryError,
        match="the staged overlay does not name the registered successor bundle",
    ):
        registry.stage_overlay(successor.overlay_bundle_canonical_json_bytes(foreign))


_LEGAL_STRATEGY_ID_AT_LIMIT = constants.STRATEGY_SERVICE_ID_PREFIX + "a" * (
    constants.SERVICE_ID_MAX_LENGTH - len(constants.STRATEGY_SERVICE_ID_PREFIX)
)


@pytest.mark.parametrize(
    ("service_id", "reason"),
    (
        ("Signal-Router", "outside the frozen grammar"),
        ("signal_router", "outside the frozen grammar"),
        ("signal-router.v1", "outside the frozen grammar"),
        ("1signal-router", "outside the frozen grammar"),
        ("signal-router-", "outside the frozen grammar"),
        ("signal--router", "outside the frozen grammar"),
        ("-signal-router", "outside the frozen grammar"),
        ("strategy-live-", "outside the frozen grammar"),
        (_LEGAL_STRATEGY_ID_AT_LIMIT + "a", "exceeds 64 characters"),
    ),
)
def test_a_participant_service_id_outside_the_frozen_grammar_rejects(
    service_id: str,
    reason: str,
) -> None:
    """`producer_service_ids` used to accept any nonempty string."""

    with pytest.raises(REJECTIONS, match=reason):
        _channel("signal-envelope/current", producers=(service_id,))


def test_a_strategy_service_id_at_the_exact_length_bound_is_accepted() -> None:
    assert len(_LEGAL_STRATEGY_ID_AT_LIMIT) == constants.SERVICE_ID_MAX_LENGTH
    channel = _channel("signal-envelope/current", producers=(_LEGAL_STRATEGY_ID_AT_LIMIT,))
    assert channel.producer_service_ids == (_LEGAL_STRATEGY_ID_AT_LIMIT,)


@pytest.mark.parametrize(
    ("channel_id", "producers", "consumers", "reason"),
    (
        # A consumer-only role standing in the producer tuple.
        (
            "signal-envelope/current",
            ("notifier",),
            None,
            "producer_service_ids role notifier cannot produce signal-envelope/current",
        ),
        (
            "signal-bus-routed-record/current",
            ("paper-broker",),
            None,
            "producer_service_ids role paper_broker cannot produce",
        ),
        # A producer-only role standing in the consumer tuple.
        (
            "signal-envelope/current",
            None,
            ("strategy-live-a",),
            "consumer_service_ids role strategy_live cannot consume signal-envelope/current",
        ),
        # A legal role on the wrong channel.
        (
            "signal-bus-routed-record/current",
            ("strategy-live-a",),
            None,
            "producer_service_ids role strategy_live cannot produce",
        ),
        (
            "signal-envelope/current",
            ("signal-router",),
            None,
            "producer_service_ids role signal_router cannot produce",
        ),
        (
            "signal-route-spool-record/current",
            None,
            ("shadow-session",),
            "consumer_service_ids role shadow_session cannot consume",
        ),
        # A grammar-legal identifier that names no role at all.
        (
            "signal-bus-routed-record/current",
            ("signal-router-shadow",),
            None,
            "producer_service_ids service id is outside the frozen role domain",
        ),
        (
            "signal-envelope/current",
            None,
            ("serving-publisher",),
            "consumer_service_ids role serving_publisher cannot consume",
        ),
    ),
)
def test_a_cross_domain_participant_rejects(
    channel_id: str,
    producers: tuple[str, ...] | None,
    consumers: tuple[str, ...] | None,
    reason: str,
) -> None:
    with pytest.raises(REJECTIONS, match=re.escape(reason)):
        _channel(channel_id, producers=producers, consumers=consumers)


def test_the_role_domain_is_exactly_the_five_phase_c_roles_plus_the_strategy_prefix() -> None:
    assert tuple(sorted(constants.PHASE_C_ROLE_SERVICE_IDS)) == (
        "notifier",
        "paper_broker",
        "serving_publisher",
        "shadow_session",
        "signal_router",
    )
    for role, service_id in constants.PHASE_C_ROLE_SERVICE_IDS.items():
        assert constants.service_id_role(service_id) == role
    assert constants.service_id_role("strategy-live-a") == constants.STRATEGY_LIVE_ROLE
    assert constants.service_id_role("strategy-live-") is None
    assert constants.service_id_role("strategy-live") is None
    assert constants.service_id_role("signal-router-shadow") is None
    assert constants.service_id_role("Signal-Router") is None
    assert constants.service_id_role("a" * (constants.SERVICE_ID_MAX_LENGTH + 1)) is None


def test_every_channel_role_domain_is_a_subset_of_the_frozen_role_names() -> None:
    """No channel may name a role that is not one of the six frozen roles."""

    known = {*constants.PHASE_C_ROLE_SERVICE_IDS, constants.STRATEGY_LIVE_ROLE}
    for mapping in (constants.CHANNEL_PRODUCER_ROLES, constants.CHANNEL_CONSUMER_ROLES):
        assert tuple(sorted(mapping)) == tuple(sorted(constants.SUCCESSOR_CHANNEL_BINDINGS))
        for channel_id, roles in mapping.items():
            assert roles, channel_id
            assert tuple(sorted(set(roles))) == tuple(sorted(roles)), channel_id
            assert set(roles) <= known, channel_id
