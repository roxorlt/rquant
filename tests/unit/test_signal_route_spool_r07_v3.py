"""Phase-A R07 v3 decoder contracts; no durable v3 publication exists here."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.signal_route_spool as spool
from rquant.delivery_contracts import DeliveryChannel, DeliveryTarget
from rquant.signal_bus import (
    LegacySignalWriteActivationError,
    RouteReceiptDisposition,
    SignalBusRoutedRecord,
    SignalBusSourceDescriptor,
    SignalRouteReceipt,
)
from rquant.signal_contracts import (
    CURRENT_ENVELOPE_SCHEMA,
    CurrentSignalEnvelope,
    GitCommitClaimIdentity,
    SignalAction,
    current_signal_envelope_json_bytes,
)
from rquant.strict_json import canonical_json_bytes

V2_VALID_RECORD = b'{"global_sequence":1,"payload_hash":"24c8bc94babd7f16e1ecaa34294c7f03606be37fbd07941ebfffbab098f6d0f1","previous_record_hash":null,"record":{"global_sequence":1,"payload_hash":"85e0362c7ccb4a1c8c7891700b7d40a29c799437856d6363efdf7a5b800a2a65","payload_json":"{\\"action\\":\\"watch\\",\\"available_at\\":\\"2026-07-31T02:30:00Z\\",\\"candidate_id\\":\\"600001.SH\\",\\"dataset_snapshot_id\\":\\"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\\",\\"event_time\\":\\"2026-07-31T02:29:59Z\\",\\"evidence\\":{},\\"expires_at\\":\\"2026-07-31T02:35:00Z\\",\\"feature_snapshot_id\\":\\"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee\\",\\"parameter_fingerprint\\":\\"1111111111111111111111111111111111111111111111111111111111111111\\",\\"producer_commit\\":\\"ffffffffffffffffffffffffffffffffffffffff\\",\\"reason_codes\\":[\\"spool-test\\"],\\"schema_version\\":1,\\"signal_id\\":\\"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f\\",\\"strategy_id\\":\\"n-shape\\",\\"strategy_version\\":\\"1\\"}","receipt":{"decision_fingerprint":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","disposition":"routed","reason_code":null,"routed_at":"2026-07-31T02:30:00Z","signal_id":"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f","source_id":"n-shape-v1","source_sequence":1,"target_count":1,"target_manifest_hash":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","targets":[{"channel":"pushdeer","recipient_id":"admin"}]},"received_at":"2026-07-31T02:30:00Z","signal":{"action":"watch","available_at":"2026-07-31T02:30:00Z","candidate_id":"600001.SH","dataset_snapshot_id":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd","event_time":"2026-07-31T02:29:59Z","evidence":{},"expires_at":"2026-07-31T02:35:00Z","feature_snapshot_id":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","parameter_fingerprint":"1111111111111111111111111111111111111111111111111111111111111111","producer_commit":"ffffffffffffffffffffffffffffffffffffffff","reason_codes":["spool-test"],"schema_version":1,"signal_id":"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f","strategy_id":"n-shape","strategy_version":"1"},"signal_id":"7d8274438e1da562d20e5e5a547414e5c06ef04aa540a421755c6e6a5a0e4e4f"},"record_hash":"7a9068acf63507646e22bf53e48fc7bf899dfffd2cb3b8d635b5549addf7b5b8","schema_version":2}'  # noqa: E501

V2_INVALID_CORPUS = (
    b"{}",
    b'{"schema_version":3}',
    b'{"schema_version":"2"}',
    b'{"schema_version":true}',
    b'{"schema_version":2,"schema_version":2}',
    b'{"schema_version":2,"record_hash":"NaN"}',
    b'{"schema_version":2}\n',
    b'{"schema_version":2',
)


@pytest.mark.parametrize(
    "payload",
    (
        V2_VALID_RECORD,
        *V2_INVALID_CORPUS,
    ),
)
def test_r07_dispatcher_preserves_frozen_v2_parser_results(payload: bytes) -> None:
    try:
        legacy = spool._parse_record(payload, sequence=1)
    except spool.SignalRouteSpoolIntegrityError as legacy_error:
        with pytest.raises(type(legacy_error), match=str(legacy_error)):
            spool._parse_r07_record(payload, sequence=1)
    else:
        dispatched = spool._parse_r07_record(payload, sequence=1)
        assert type(dispatched) is type(legacy)
        assert spool._canonical_bytes(dispatched) == spool._canonical_bytes(legacy)


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
