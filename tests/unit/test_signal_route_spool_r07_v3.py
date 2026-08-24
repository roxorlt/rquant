"""Phase-A R07 v3 decoder contracts; no durable v3 publication exists here."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

import rquant.signal_route_spool as spool
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.signal_bus import (
    LegacySignalWriteActivationError,
    RouteReceiptDisposition,
    SignalBusRoutedRecord,
    SignalBusSourceDescriptor,
    SignalBusStore,
    SignalRouteReceipt,
)
from rquant.signal_contracts import (
    CURRENT_ENVELOPE_SCHEMA,
    CurrentSignalEnvelope,
    GitCommitClaimIdentity,
    SignalAction,
    SignalEnvelope,
    current_signal_envelope_json_bytes,
)
from rquant.strict_json import canonical_json_bytes
from tests.support.signal_route_spool_crash_matrix import (
    CRASH_POINTS,
    CURRENT_SPEC_DIALECT,
    FROZEN_LEGACY_DIALECT,
    SPOOL_DIALECTS,
    SpoolConflictAuditEntry,
    SpoolPointerImage,
    SpoolRecordImage,
    SyntheticSpoolDurabilityModel,
    SyntheticSpoolRecoveryError,
    drive_publication,
    record_name,
)

V2_VALID_RECORD = b'{"global_sequence":1,"payload_hash":"24c8bc94babd7f16e1ecaa34294c7f03606be37fbd07941ebfffbab098f6d0f1","previous_record_hash":null,"record":{"global_sequence":1,"payload_hash":"85e0362c7ccb4a1c8c7891700b7d40a29c799437856d6363efdf7a5b800a2a65","payload_json":"{\\"action\\":\\"watch\\",\\"available_at\\":\\"2026-07-31T02:30:00Z\\",\\"candidate_id\\":\\"600001.SH\\",\\"dataset_snapshot_id\\":\\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\\",\\"event_time\\":\\"2026-07-31T02:29:59Z\\",\\"evidence\\":{},\\"expires_at\\":\\"2026-07-31T02:35:00Z\\",\\"feature_snapshot_id\\":\\"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\\",\\"parameter_fingerprint\\":\\"1111111111111111111111111111111111111111111111111111111111111111\\",\\"producer_commit\\":\\"ffffffffffffffffffffffffffffffffffffffff\\",\\"reason_codes\\":[\\"spool-test\\"],\\"schema_version\\":1,\\"signal_id\\":\\"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f\\",\\"strategy_id\\":\\"n-shape\\",\\"strategy_version\\":\\"1\\"}","receipt":{"decision_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","disposition":"routed","reason_code":null,"routed_at":"2026-07-31T02:30:00Z","signal_id":"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f","source_id":"n-shape-v1","source_sequence":1,"target_count":1,"target_manifest_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","targets":[{"channel":"pushdeer","recipient_id":"admin"}]},"received_at":"2026-07-31T02:30:00Z","signal":{"action":"watch","available_at":"2026-07-31T02:30:00Z","candidate_id":"600001.SH","dataset_snapshot_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","event_time":"2026-07-31T02:29:59Z","evidence":{},"expires_at":"2026-07-31T02:35:00Z","feature_snapshot_id":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","parameter_fingerprint":"1111111111111111111111111111111111111111111111111111111111111111","producer_commit":"ffffffffffffffffffffffffffffffffffffffff","reason_codes":["spool-test"],"schema_version":1,"signal_id":"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f","strategy_id":"n-shape","strategy_version":"1"},"signal_id":"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f"},"record_hash":"7a9068acf63507646e22bf53e48fc7bf899dfffd2cb3b8d635b5549addf7b5b8","schema_version":2}'  # noqa: E501


def test_r07_exposes_a_strict_current_v3_decoder() -> None:
    assert callable(spool.decode_current_signal_route_spool_record)


NOW = datetime(2026, 8, 16, 2, 30, tzinfo=UTC)


def _current_record_bytes(*, sequence: int, previous_record_hash: str | None) -> bytes:
    envelope = CurrentSignalEnvelope(
        envelope_schema=CURRENT_ENVELOPE_SCHEMA,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint="1" * 64,
        dataset_snapshot_id="d" * 64,
        feature_snapshot_id="e" * 64,
        event_time=NOW - timedelta(seconds=1),
        available_at=NOW,
        candidate_id="600001.SH",
        action=SignalAction.WATCH,
        reason_codes=("spool-test",),
        evidence={"label": "current"},
        expires_at=NOW + timedelta(minutes=5),
        producer_identity=GitCommitClaimIdentity(
            kind="git-commit-claim-sha1/v1",
            producer_commit="c" * 40,
        ),
    )
    envelope_bytes = current_signal_envelope_json_bytes(envelope)
    receipt = SignalRouteReceipt(
        source_id="n-shape-v1",
        source_sequence=sequence,
        signal_id=envelope.signal_id,
        decision_fingerprint="a" * 64,
        disposition=RouteReceiptDisposition.ROUTED,
        target_manifest_hash="b" * 64,
        targets=(DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
        target_count=1,
        routed_at=NOW,
    )
    routed = {
        "global_sequence": sequence,
        "signal_id": envelope.signal_id,
        "envelope_hash": hashlib.sha256(envelope_bytes).hexdigest(),
        "payload_json": envelope_bytes.decode("utf-8"),
        "envelope": envelope.model_dump(mode="json"),
        "received_at": NOW.isoformat().replace("+00:00", "Z"),
        "receipt": receipt.model_dump(mode="json"),
    }
    routed_bytes = canonical_json_bytes(routed)
    outer = {
        "schema_version": 3,
        "global_sequence": sequence,
        "previous_record_hash": previous_record_hash,
        "envelope_hash": routed["envelope_hash"],
        "routed_record_hash": hashlib.sha256(routed_bytes).hexdigest(),
        "record": routed,
    }
    outer["record_hash"] = hashlib.sha256(canonical_json_bytes(outer)).hexdigest()
    return canonical_json_bytes(outer)


def _current_bus_record(*, sequence: int) -> SignalBusRoutedRecord:
    parsed = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=sequence, previous_record_hash=None)
    )
    return SignalBusRoutedRecord(
        global_sequence=parsed.global_sequence,
        signal_id=parsed.record.signal_id,
        payload_hash=parsed.record.envelope_hash,
        payload_json=parsed.record.payload_json,
        signal=parsed.record.envelope,
        received_at=parsed.record.received_at,
        receipt=parsed.record.receipt,
    )


def _tree_bytes(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_current_v3_decoder_binds_exact_canonical_preimages() -> None:
    raw = _current_record_bytes(sequence=1, previous_record_hash=None)

    decoded = spool.decode_current_signal_route_spool_record(raw)

    assert type(decoded) is spool.CurrentSignalRouteSpoolRecord
    assert type(decoded.record) is spool.CurrentSignalBusRoutedRecord
    assert spool.current_signal_route_spool_record_json_bytes(decoded) == raw
    routed_bytes = spool.current_signal_bus_routed_record_json_bytes(decoded.record)
    assert routed_bytes == canonical_json_bytes(decoded.record.model_dump(mode="json"))
    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalBusRoutedRecord.model_validate(decoded.record.model_dump(mode="json"))
    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalRouteSpoolRecord.model_validate(decoded.model_dump(mode="json"))


def _direct_routed_values(
    record: spool.CurrentSignalBusRoutedRecord,
) -> dict[str, object]:
    return {name: getattr(record, name) for name in type(record).model_fields}


def _direct_outer_values(
    record: spool.CurrentSignalRouteSpoolRecord,
) -> dict[str, object]:
    return {name: getattr(record, name) for name in type(record).model_fields}


@pytest.mark.parametrize("missing", ("schema_version", "previous_record_hash"))
def test_current_outer_model_requires_explicit_schema_and_previous_hash(missing: str) -> None:
    decoded = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=1, previous_record_hash=None)
    )
    values = _direct_outer_values(decoded)
    del values[missing]

    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalRouteSpoolRecord.model_validate(values)


@pytest.mark.parametrize("field", ("signal_id", "envelope_hash", "payload_json"))
@pytest.mark.parametrize("side", ("leading", "trailing"))
def test_current_routed_model_rejects_surrounding_text_whitespace(
    field: str,
    side: str,
) -> None:
    decoded = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=1, previous_record_hash=None)
    )
    values = _direct_routed_values(decoded.record)
    original = values[field]
    assert isinstance(original, str)
    values[field] = f" {original}" if side == "leading" else f"{original} "

    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalBusRoutedRecord.model_validate(values)


@pytest.mark.parametrize(
    "field",
    (
        "previous_record_hash",
        "envelope_hash",
        "routed_record_hash",
        "record_hash",
    ),
)
@pytest.mark.parametrize("side", ("leading", "trailing"))
def test_current_outer_model_rejects_surrounding_hash_whitespace(
    field: str,
    side: str,
) -> None:
    decoded = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=2, previous_record_hash="f" * 64)
    )
    values = _direct_outer_values(decoded)
    original = values[field]
    assert isinstance(original, str)
    values[field] = f" {original}" if side == "leading" else f"{original} "

    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalRouteSpoolRecord.model_validate(values)


@pytest.mark.parametrize("value", (True, 1.0, "1"))
def test_current_routed_model_rejects_noncanonical_sequence_scalars(value: object) -> None:
    decoded = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=1, previous_record_hash=None)
    )
    values = _direct_routed_values(decoded.record)
    values["global_sequence"] = value

    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalBusRoutedRecord.model_validate(values)


@pytest.mark.parametrize("value", (True, 3.0, "3"))
def test_current_outer_model_rejects_noncanonical_schema_scalars(value: object) -> None:
    decoded = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=1, previous_record_hash=None)
    )
    values = _direct_outer_values(decoded)
    values["schema_version"] = value

    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalRouteSpoolRecord.model_validate(values)


@pytest.mark.parametrize(
    "value",
    (
        NOW.replace(tzinfo=None),
        NOW.astimezone(timezone(timedelta(hours=8))),
    ),
)
def test_current_routed_model_rejects_non_utc_datetimes(value: datetime) -> None:
    decoded = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=1, previous_record_hash=None)
    )
    values = _direct_routed_values(decoded.record)
    values["received_at"] = value

    with pytest.raises((TypeError, ValueError)):
        spool.CurrentSignalBusRoutedRecord.model_validate(values)


def test_current_byte_helpers_revalidate_forged_exact_model_instances() -> None:
    decoded = spool.decode_current_signal_route_spool_record(
        _current_record_bytes(sequence=1, previous_record_hash=None)
    )
    routed_values = _direct_routed_values(decoded.record)
    routed_values["payload_json"] = f" {decoded.record.payload_json}"
    forged_routed = spool.CurrentSignalBusRoutedRecord.model_construct(**routed_values)
    outer_values = _direct_outer_values(decoded)
    outer_values["schema_version"] = 3.0
    forged_outer = spool.CurrentSignalRouteSpoolRecord.model_construct(**outer_values)

    with pytest.raises((TypeError, ValueError)):
        spool.current_signal_bus_routed_record_json_bytes(forged_routed)
    with pytest.raises((TypeError, ValueError)):
        spool.current_signal_route_spool_record_json_bytes(forged_outer)


@pytest.mark.parametrize(
    "mutation",
    (
        lambda raw: b" " + raw,
        lambda raw: b'{"envelope_hash":"' + b"0" * 64 + b'",' + raw[1:],
        lambda raw: raw.replace(b'"global_sequence":1', b'"global_sequence":1.0', 1),
        lambda raw: raw + b"\n",
    ),
)
def test_current_v3_decoder_rejects_noncanonical_or_ambiguous_bytes(
    mutation: object,
) -> None:
    raw = _current_record_bytes(sequence=1, previous_record_hash=None)

    with pytest.raises(spool.SignalRouteSpoolIntegrityError):
        spool.decode_current_signal_route_spool_record(mutation(raw))  # type: ignore[operator]


def test_fixture_verifier_allows_only_a_v2_to_v3_transition() -> None:
    current_raw = _current_record_bytes(
        sequence=2,
        previous_record_hash="7a9068acf63507646e22bf53e48fc7bf899dfffd2cb3b8d635b5549addf7b5b8",
    )

    verified = spool.verify_current_signal_route_spool_fixture((V2_VALID_RECORD, current_raw))

    assert [type(record) for record in verified] == [
        spool.SignalRouteSpoolRecord,
        spool.CurrentSignalRouteSpoolRecord,
    ]
    with pytest.raises(spool.SignalRouteSpoolIntegrityError, match="cannot start with v3"):
        spool.verify_current_signal_route_spool_fixture(
            (_current_record_bytes(sequence=1, previous_record_hash=None),)
        )
    isolated = spool.verify_current_signal_route_spool_fixture(
        (_current_record_bytes(sequence=1, previous_record_hash=None),),
        allow_isolated_current_fixture=True,
    )
    assert type(isolated[0]) is spool.CurrentSignalRouteSpoolRecord


def test_legacy_spool_rejects_current_records_before_mutation(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    route_spool = spool.SignalRouteSpool(root)
    before = _tree_bytes(root)
    source = SignalBusSourceDescriptor(generation_id="f" * 64, high_watermark=1)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        route_spool.publish(source=source, records=(_current_bus_record(sequence=1),))

    assert _tree_bytes(root) == before


def test_bus_prefix_preflights_current_records_before_initial_publish(tmp_path: Path) -> None:
    root = tmp_path / "spool"
    route_spool = spool.SignalRouteSpool(root)
    before = _tree_bytes(root)
    source = SignalBusSourceDescriptor(generation_id="f" * 64, high_watermark=1)
    current = _current_bus_record(sequence=1)

    class _Bus:
        def source_descriptor(self) -> SignalBusSourceDescriptor:
            return source

        def routed_signals_after_global_sequence(
            self,
            *,
            after_sequence: int,
            through_sequence: int,
            limit: int,
        ) -> tuple[SignalBusRoutedRecord, ...]:
            assert (after_sequence, through_sequence, limit) == (0, 1, 1)
            return (current,)

    with pytest.raises(LegacySignalWriteActivationError, match="legacy-only"):
        spool.publish_signal_bus_prefix(bus=_Bus(), spool=route_spool, limit=1)  # type: ignore[arg-type]

    assert _tree_bytes(root) == before


V2_VALID_RECORD_HASH = "7a9068acf63507646e22bf53e48fc7bf899dfffd2cb3b8d635b5549addf7b5b8"
LEGACY_GENERATION = "9" * 64


def _matrix_images() -> tuple[SpoolRecordImage, ...]:
    """Build the crash-matrix images from a real, verified one-way mixed chain."""

    payloads = (
        V2_VALID_RECORD,
        _current_record_bytes(sequence=2, previous_record_hash=V2_VALID_RECORD_HASH),
    )
    payloads += (
        _current_record_bytes(
            sequence=3,
            previous_record_hash=spool.decode_current_signal_route_spool_record(
                payloads[1]
            ).record_hash,
        ),
    )
    verified = spool.verify_current_signal_route_spool_fixture(payloads)
    return tuple(
        SpoolRecordImage(
            sequence=record.global_sequence,
            payload=payload,
            previous_record_hash=record.previous_record_hash,
            record_hash=record.record_hash,
        )
        for record, payload in zip(verified, payloads, strict=True)
    )


MATRIX_IMAGES = _matrix_images()
POINTER_AT_FIRST = SpoolPointerImage(
    first_sequence=1,
    high_watermark=1,
    last_record_hash=MATRIX_IMAGES[0].record_hash,
)
POINTER_AT_SECOND = SpoolPointerImage(
    first_sequence=1,
    high_watermark=2,
    last_record_hash=MATRIX_IMAGES[1].record_hash,
)

# One expectation row per frozen durability boundary (authority.md L558-570). The frozen
# v2 dialect and the frozen later-writer dialect agree on recovery for a first-attempt
# publication; they diverge on retry and conflict, which the dedicated cases below pin.
CRASH_MATRIX_EXPECTATIONS: dict[str, dict[str, object]] = {
    "record-temporary-write": {
        "watermarks": (1,),
        "prefixes": ((1,),),
        "orphans": ((),),
        "temporaries": 1,
        "pointer_temp_staged": False,
        "durable": (1,),
    },
    "record-temporary-fsync": {
        "watermarks": (1,),
        "prefixes": ((1,),),
        "orphans": ((),),
        "temporaries": 1,
        "pointer_temp_staged": False,
        "durable": (1,),
    },
    "immutable-record-link": {
        "watermarks": (1,),
        "prefixes": ((1,),),
        "orphans": ((2,),),
        "temporaries": 0,
        "pointer_temp_staged": False,
        "durable": (1,),
    },
    "records-directory-fsync": {
        "watermarks": (1,),
        "prefixes": ((1,),),
        "orphans": ((2,),),
        "temporaries": 0,
        "pointer_temp_staged": False,
        "durable": (1, 2),
    },
    "pointer-temporary-write": {
        "watermarks": (1,),
        "prefixes": ((1,),),
        "orphans": ((2,),),
        "temporaries": 0,
        "pointer_temp_staged": True,
        "durable": (1, 2),
    },
    "pointer-temporary-fsync": {
        "watermarks": (1,),
        "prefixes": ((1,),),
        "orphans": ((2,),),
        "temporaries": 0,
        "pointer_temp_staged": True,
        "durable": (1, 2),
    },
    "pointer-replace": {
        "watermarks": (1, 2),
        "prefixes": ((1,), (1, 2)),
        "orphans": ((2,), ()),
        "temporaries": 0,
        "pointer_temp_staged": False,
        "durable": (1, 2),
    },
    "root-directory-fsync": {
        "watermarks": (2,),
        "prefixes": ((1, 2),),
        "orphans": ((),),
        "temporaries": 0,
        "pointer_temp_staged": False,
        "durable": (1, 2),
    },
}


def _model_at_first_pointer(dialect: str) -> SyntheticSpoolDurabilityModel:
    model = SyntheticSpoolDurabilityModel(dialect=dialect)
    drive_publication(model, images=MATRIX_IMAGES[:1], pointer=POINTER_AT_FIRST)
    assert model.pointer_visible == POINTER_AT_FIRST
    assert model.root_fsynced
    return model


def test_synthetic_matrix_mirrors_the_production_record_name() -> None:
    for sequence in (1, 2, 42, 10**19 - 1):
        assert record_name(sequence) == spool._SignalRouteSpoolPaths.record_name(sequence)
    assert MATRIX_IMAGES[1].name == "00000000000000000002.json"


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
@pytest.mark.parametrize("crash_point", CRASH_POINTS)
def test_synthetic_crash_matrix_recovers_every_frozen_durability_boundary(
    crash_point: str,
    dialect: str,
) -> None:
    expected = CRASH_MATRIX_EXPECTATIONS[crash_point]
    model = _model_at_first_pointer(dialect)

    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point=crash_point,
    )
    outcome = model.recover()

    assert tuple(
        0 if pointer is None else pointer.high_watermark
        for pointer in outcome.admissible_pointers
    ) == expected["watermarks"]
    assert tuple(
        observation.verified_prefix for observation in outcome.observations
    ) == expected["prefixes"]
    assert tuple(
        observation.orphan_sequences for observation in outcome.observations
    ) == expected["orphans"]
    assert len(outcome.ignored_temporary_names) == expected["temporaries"]
    assert (model.pointer_temp is not None) is expected["pointer_temp_staged"]
    assert outcome.durable_sequences == expected["durable"]
    assert outcome.conflict_audit == ()
    # Universal rules from L571-574 that hold at every boundary.
    for observation in outcome.observations:
        assert observation.verified_prefix == tuple(
            range(1, len(observation.verified_prefix) + 1)
        )
        assert not set(observation.verified_prefix) & set(observation.orphan_sequences)
    assert all(name.startswith(".") for name in outcome.ignored_temporary_names)
    if model.pointer_temp is not None:
        assert model.pointer_temp not in outcome.admissible_pointers


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
def test_the_pointer_replace_window_admits_only_the_old_or_the_new_pointer(
    dialect: str,
) -> None:
    model = _model_at_first_pointer(dialect)

    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point="pointer-replace",
    )
    window = model.recover()

    assert window.admissible_pointers == (POINTER_AT_FIRST, POINTER_AT_SECOND)
    assert [observation.verified_prefix for observation in window.observations] == [(1,), (1, 2)]
    assert model.root_fsynced is False

    model.fsync_root_directory()
    settled = model.recover()

    assert settled.admissible_pointers == (POINTER_AT_SECOND,)
    assert settled.observations[0].verified_prefix == (1, 2)
    assert settled.observations[0].orphan_sequences == ()


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
def test_temporaries_orphans_listings_and_the_highest_sequence_are_never_authority(
    dialect: str,
) -> None:
    model = _model_at_first_pointer(dialect)
    drive_publication(
        model,
        images=MATRIX_IMAGES[1:3],
        pointer=POINTER_AT_SECOND,
        crash_point="records-directory-fsync",
    )
    staged = model.stage_record_temporary(MATRIX_IMAGES[0])

    outcome = model.recover()

    assert sorted(model.linked_names) == [
        record_name(1),
        record_name(2),
        record_name(3),
    ]
    assert outcome.durable_sequences == (1, 2, 3)
    assert outcome.admissible_pointers == (POINTER_AT_FIRST,)
    assert outcome.observations[0].verified_prefix == (1,)
    assert outcome.observations[0].orphan_sequences == (2, 3)
    assert outcome.ignored_temporary_names == (staged,)
    assert list(model.linked_names)[-1] == record_name(3)


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
def test_a_byte_identical_record_retry_is_idempotent(dialect: str) -> None:
    model = _model_at_first_pointer(dialect)
    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point="immutable-record-link",
    )
    linked_before = dict(model.linked_names)

    model.stage_record_temporary(MATRIX_IMAGES[1])
    model.link_record(2)

    assert model.linked_names == linked_before
    assert model.conflict_audit == []
    assert model.temporaries == {}
    assert model.recover().observations[0].orphan_sequences == (2,)


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
def test_a_record_temporary_retry_reuses_only_byte_identical_content(dialect: str) -> None:
    model = _model_at_first_pointer(dialect)
    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point="record-temporary-fsync",
    )
    first_temporary = tuple(model.temporaries)[0]
    forged = replace(MATRIX_IMAGES[1], payload=MATRIX_IMAGES[1].payload + b" ")

    if dialect == CURRENT_SPEC_DIALECT:
        assert model.retry_record_temporary(MATRIX_IMAGES[1]) == first_temporary
        assert tuple(model.temporaries) == (first_temporary,)
        with pytest.raises(SyntheticSpoolRecoveryError, match="byte-identical"):
            model.retry_record_temporary(forged)
        assert tuple(model.temporaries) == (first_temporary,)
    else:
        assert model.retry_record_temporary(MATRIX_IMAGES[1]) != first_temporary
        assert len(model.temporaries) == 2
    assert model.recover().admissible_pointers == (POINTER_AT_FIRST,)
    assert model.recover().observations[0].verified_prefix == (1,)


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
def test_conflicting_record_bytes_reject_before_any_pointer_mutation(dialect: str) -> None:
    model = _model_at_first_pointer(dialect)
    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point="records-directory-fsync",
    )
    forged = replace(MATRIX_IMAGES[1], payload=MATRIX_IMAGES[1].payload + b" ")
    model.stage_record_temporary(forged)

    with pytest.raises(SyntheticSpoolRecoveryError, match="immutable routed-signal record changed"):
        model.link_record(2)

    assert model.linked_names[record_name(2)] == MATRIX_IMAGES[1]
    assert model.pointer_temp is None
    assert model.pointer_visible == POINTER_AT_FIRST
    assert model.root_fsynced is True
    expected_audit = (
        SpoolConflictAuditEntry(
            sequence=2,
            existing_hash=MATRIX_IMAGES[1].content_digest,
            attempted_hash=forged.content_digest,
        ),
    )
    assert tuple(model.conflict_audit) == (
        expected_audit if dialect == CURRENT_SPEC_DIALECT else ()
    )
    assert model.recover().conflict_audit == tuple(model.conflict_audit)


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
def test_a_visible_pointer_must_name_a_complete_verified_prefix(dialect: str) -> None:
    model = _model_at_first_pointer(dialect)
    drive_publication(model, images=MATRIX_IMAGES[1:2], pointer=POINTER_AT_SECOND)
    assert model.recover().observations[0].verified_prefix == (1, 2)

    del model.linked_names[record_name(1)]
    with pytest.raises(SyntheticSpoolRecoveryError, match="sequence is missing: 1"):
        model.recover()

    model.linked_names[record_name(1)] = replace(MATRIX_IMAGES[0], record_hash="0" * 64)
    with pytest.raises(SyntheticSpoolRecoveryError, match="hash chain mismatch at 2"):
        model.recover()


@pytest.mark.parametrize("dialect", SPOOL_DIALECTS)
def test_pointer_publication_refuses_an_incomplete_prefix(dialect: str) -> None:
    model = _model_at_first_pointer(dialect)
    model.stage_record_temporary(MATRIX_IMAGES[1])
    model.fsync_record_temporary(2)

    with pytest.raises(SyntheticSpoolRecoveryError, match="sequence is missing: 2"):
        model.stage_pointer_temporary(POINTER_AT_SECOND)

    assert model.pointer_temp is None
    assert model.recover().admissible_pointers == (POINTER_AT_FIRST,)

    model.link_record(2)
    with pytest.raises(SyntheticSpoolRecoveryError, match="head hash mismatch"):
        model.stage_pointer_temporary(replace(POINTER_AT_SECOND, last_record_hash="0" * 64))
    assert model.pointer_temp is None


def test_hardened_publication_contract_refsyncs_records_before_pointer_work() -> None:
    model = _model_at_first_pointer(CURRENT_SPEC_DIALECT)
    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point="records-directory-fsync",
    )
    assert record_name(2) in model.records_dir_fsynced

    model.stage_record_temporary(MATRIX_IMAGES[1])
    model.link_record(2)

    assert record_name(2) not in model.records_dir_fsynced
    with pytest.raises(SyntheticSpoolRecoveryError, match="records directory must be fsynced"):
        model.stage_pointer_temporary(POINTER_AT_SECOND)
    assert model.pointer_temp is None
    assert model.pointer_visible == POINTER_AT_FIRST

    model.fsync_records_directory()
    drive_publication(model, images=(), pointer=POINTER_AT_SECOND)

    assert model.pointer_visible == POINTER_AT_SECOND
    assert model.root_fsynced is True
    assert model.recover().observations[0].verified_prefix == (1, 2)


def test_hardened_publication_contract_rejects_differing_bytes_before_pointer_work() -> None:
    model = _model_at_first_pointer(CURRENT_SPEC_DIALECT)
    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point="immutable-record-link",
    )
    forged = replace(MATRIX_IMAGES[1], payload=MATRIX_IMAGES[1].payload + b"\n")
    model.stage_record_temporary(forged)

    with pytest.raises(SyntheticSpoolRecoveryError, match="immutable routed-signal record changed"):
        model.link_record(2)

    assert [entry.sequence for entry in model.conflict_audit] == [2]
    assert model.conflict_audit[0].existing_hash == MATRIX_IMAGES[1].content_digest
    assert model.conflict_audit[0].attempted_hash == forged.content_digest
    assert model.pointer_temp is None
    assert model.pointer_visible == POINTER_AT_FIRST
    assert model.root_fsynced is True


def test_frozen_legacy_immutable_write_skips_the_second_records_directory_fsync() -> None:
    model = _model_at_first_pointer(FROZEN_LEGACY_DIALECT)
    drive_publication(
        model,
        images=MATRIX_IMAGES[1:2],
        pointer=POINTER_AT_SECOND,
        crash_point="records-directory-fsync",
    )

    model.stage_record_temporary(MATRIX_IMAGES[1])
    model.link_record(2)
    model.stage_pointer_temporary(POINTER_AT_SECOND)

    # Frozen v2 gap: an identical pre-existing link keeps its durability claim and the
    # pointer publication starts without a second records-directory fsync, and a byte
    # conflict raises without recording audit evidence.
    assert record_name(2) in model.records_dir_fsynced
    assert model.pointer_temp == POINTER_AT_SECOND
    assert model.conflict_audit == []


def test_frozen_v2_immutable_write_matches_the_observed_dialect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bind the frozen-v2-observed dialect to the untouched v2 primitive itself."""

    records = tmp_path / "records"
    records.mkdir()
    descriptor = os.open(records, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    fsynced: list[int] = []
    real_fsync = os.fsync

    def counting_fsync(target: int) -> None:
        fsynced.append(target)
        real_fsync(target)

    try:
        monkeypatch.setattr(os, "fsync", counting_fsync)
        payload = b'{"probe":1}'
        spool._immutable_write_at(descriptor, "x.json", payload, label="probe", max_bytes=64)
        fresh_link = list(fsynced)
        fsynced.clear()
        spool._immutable_write_at(descriptor, "x.json", payload, label="probe", max_bytes=64)
        identical_retry = list(fsynced)
        before = sorted(path.name for path in records.iterdir())
        with pytest.raises(spool.SignalRouteSpoolIntegrityError, match="immutable probe changed"):
            spool._immutable_write_at(
                descriptor,
                "x.json",
                payload + b" ",
                label="probe",
                max_bytes=64,
            )
    finally:
        os.close(descriptor)

    assert descriptor in fresh_link
    assert descriptor not in identical_retry
    assert sorted(path.name for path in records.iterdir()) == before == ["x.json"]
    assert (records / "x.json").read_bytes() == payload


def _legacy_envelope(seed: str) -> SignalEnvelope:
    return SignalEnvelope(
        schema_version=1,
        strategy_id="n-shape",
        strategy_version="1",
        parameter_fingerprint=seed * 64,
        dataset_snapshot_id="d" * 64,
        feature_snapshot_id="e" * 64,
        event_time=NOW - timedelta(seconds=1),
        available_at=NOW,
        candidate_id=f"60000{ord(seed) % 10}.SH",
        action=SignalAction.WATCH,
        reason_codes=("spool-test",),
        evidence={},
        expires_at=NOW + timedelta(minutes=5),
        producer_commit="f" * 40,
    )


def _legacy_bus_record(*, sequence: int) -> SignalBusRoutedRecord:
    envelope = _legacy_envelope(str(sequence))
    payload_json = json.dumps(
        envelope.model_dump(mode="json"),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return SignalBusRoutedRecord(
        global_sequence=sequence,
        signal_id=envelope.signal_id,
        payload_hash=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        payload_json=payload_json,
        signal=envelope,
        received_at=NOW,
        receipt=SignalRouteReceipt(
            source_id="n-shape-v1",
            source_sequence=sequence,
            signal_id=envelope.signal_id,
            decision_fingerprint="a" * 64,
            disposition=RouteReceiptDisposition.ROUTED,
            target_manifest_hash="b" * 64,
            targets=(DeliveryTarget(recipient_id="admin", channel=DeliveryChannel.PUSHDEER),),
            target_count=1,
            routed_at=NOW,
        ),
    )


def _publish_legacy_records(root: Path, *, first: int, last: int) -> None:
    spool.SignalRouteSpool(root).publish(
        source=SignalBusSourceDescriptor(generation_id=LEGACY_GENERATION, high_watermark=last),
        records=tuple(_legacy_bus_record(sequence=sequence) for sequence in range(first, last + 1)),
    )


def test_readonly_spool_never_promotes_a_temporary_or_an_orphan_above_the_pointer(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    donor = tmp_path / "donor"
    _publish_legacy_records(donor, first=1, last=2)
    _publish_legacy_records(root, first=1, last=1)
    records = root / "records"
    orphan_name = spool._SignalRouteSpoolPaths.record_name(2)
    orphan_bytes = (donor / "records" / orphan_name).read_bytes()
    temporary_name = f".{spool._SignalRouteSpoolPaths.record_name(3)}.{'a' * 32}"
    (records / orphan_name).write_bytes(orphan_bytes)
    (records / temporary_name).write_bytes(orphan_bytes)
    newest = time.time() + 60
    os.utime(records / orphan_name, (newest, newest))
    os.utime(records / temporary_name, (newest, newest))
    before = _tree_bytes(root)

    reader = spool.ReadonlySignalRouteSpool(root)
    descriptor = reader.source_descriptor()
    visible = reader.routed_after_global_sequence(
        after_sequence=0,
        through_sequence=descriptor.high_watermark,
        limit=10,
    )

    assert sorted(path.name for path in records.iterdir()) == [
        temporary_name,
        spool._SignalRouteSpoolPaths.record_name(1),
        orphan_name,
    ]
    assert descriptor.high_watermark == 1
    assert [record.global_sequence for record in visible] == [1]
    with pytest.raises(spool.SignalRouteSpoolIntegrityError, match="exceeds the published"):
        reader.routed_after_global_sequence(after_sequence=0, through_sequence=2, limit=10)
    assert _tree_bytes(root) == before


def test_readonly_spool_rejects_a_pointer_backed_only_by_a_record_temporary(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    _publish_legacy_records(root, first=1, last=2)
    name = spool._SignalRouteSpoolPaths.record_name(2)
    (root / "records" / name).rename(root / "records" / f".{name}.{'b' * 32}")

    reader = spool.ReadonlySignalRouteSpool(root)

    with pytest.raises(spool.SignalRouteSpoolIntegrityError, match="sequence is missing: 2"):
        reader.source_descriptor()


def test_route_spool_source_identity_binds_the_exact_generation() -> None:
    identity = SignalBusSourceDescriptor(generation_id="a" * 64, high_watermark=0)
    advanced = spool.SignalRouteSpoolPointer(
        source=identity.model_copy(update={"high_watermark": 3}),
        last_record_hash="c" * 64,
    )

    spool._validate_source_identity(identity, advanced)

    for change in (
        {"generation_id": "b" * 64},
        {"source_id": "signal-bus/other/v1"},
        {"first_global_sequence": 2},
    ):
        drifted = spool.SignalRouteSpoolPointer(
            source=identity.model_copy(update={"high_watermark": 3, **change}),
            last_record_hash="c" * 64,
        )
        with pytest.raises(
            spool.SignalRouteSpoolIntegrityError,
            match="route spool generation changed",
        ):
            spool._validate_source_identity(identity, drifted)


def test_readonly_spool_rejects_a_generation_change_and_a_watermark_regression(
    tmp_path: Path,
) -> None:
    root = tmp_path / "spool"
    _publish_legacy_records(root, first=1, last=1)
    reader = spool.ReadonlySignalRouteSpool(root)
    assert reader.source_descriptor().high_watermark == 1
    rolled_back = (root / "current.json").read_bytes()

    _publish_legacy_records(root, first=2, last=2)
    assert reader.source_descriptor().high_watermark == 2

    (root / "current.json").write_bytes(rolled_back)
    with pytest.raises(
        spool.SignalRouteSpoolIntegrityError,
        match="high watermark regressed",
    ):
        reader.source_descriptor()

    source_path = root / "source.json"
    source = json.loads(source_path.read_text(encoding="utf-8"))
    source["generation_id"] = "0" * 64
    source_path.write_text(
        json.dumps(source, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )
    with pytest.raises(
        spool.SignalRouteSpoolIntegrityError,
        match="route spool generation changed",
    ):
        spool.ReadonlySignalRouteSpool(root).source_descriptor()


def test_signal_bus_generation_and_high_watermark_stay_continuous(tmp_path: Path) -> None:
    path = tmp_path / "signal-bus.sqlite3"
    bus = SignalBusStore(path)
    bus.ingest(_legacy_envelope("1"), received_at=NOW)
    bus.ingest(_legacy_envelope("2"), received_at=NOW)
    descriptor = bus.source_descriptor()

    assert descriptor.high_watermark == 2
    assert SignalBusStore(path).source_descriptor() == descriptor

    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "UPDATE signal_bus_metadata SET metadata_value = '9' "
            "WHERE metadata_key = 'signal_high_watermark'"
        )
        connection.commit()
    finally:
        connection.close()
    bus.ingest(_legacy_envelope("3"), received_at=NOW)
    ahead = bus.source_descriptor()

    assert ahead.high_watermark == 9
    assert ahead.generation_id == descriptor.generation_id

    path.unlink()
    rebuilt = SignalBusStore(path).source_descriptor()

    assert rebuilt.generation_id != descriptor.generation_id
    assert rebuilt.high_watermark == 0
