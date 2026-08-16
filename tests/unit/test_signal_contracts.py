from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant import signal_contracts
from rquant.runtime_contracts import RuntimeContractModel
from rquant.signal_contracts import SignalAction, SignalEnvelope
from rquant.strict_json import canonical_json_bytes

_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_COMMIT = "d" * 40
_ZERO_COMMIT = "0" * 40

_LEGACY_CANONICAL_FIXTURES = (
    (
        1,
        _ZERO_COMMIT,
        "f9c779c01399c7c6554778335bed19107206b7114c1f54c9e04929296e4e4da2",
        (
            b'{"action":"b_intent","available_at":"2026-07-31T01:32:00Z",'
            b'"candidate_id":"600000.SH","dataset_snapshot_id":"bbbbbbbbbbbbbbbbbbbbbbbb'
            b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","event_time":"2026-07-31T01:31:00Z",'
            b'"evidence":{"levels":{"resistance":10.2},"volume_ratio":2.5},'
            b'"expires_at":"2026-07-31T02:00:00Z","feature_snapshot_id":"cccccccccccccccc'
            b'cccccccccccccccccccccccccccccccccccccccccccccccc","parameter_fingerprint":"'
            b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            b'"producer_commit":"0000000000000000000000000000000000000000",'
            b'"reason_codes":["above_vwap","same_minute_volume"],"schema_version":1,'
            b'"signal_id":"f9c779c01399c7c6554778335bed19107206b7114c1f54c9e04929296e4e4da2",'
            b'"strategy_id":"n-shape","strategy_version":"2.1.0"}'
        ),
    ),
    (
        1,
        _COMMIT,
        "cd7afd6b503b390c268d07ccc10c782ec5c181c97deb39927298d04227e58f4c",
        (
            b'{"action":"b_intent","available_at":"2026-07-31T01:32:00Z",'
            b'"candidate_id":"600000.SH","dataset_snapshot_id":"bbbbbbbbbbbbbbbbbbbbbbbb'
            b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","event_time":"2026-07-31T01:31:00Z",'
            b'"evidence":{"levels":{"resistance":10.2},"volume_ratio":2.5},'
            b'"expires_at":"2026-07-31T02:00:00Z","feature_snapshot_id":"cccccccccccccccc'
            b'cccccccccccccccccccccccccccccccccccccccccccccccc","parameter_fingerprint":"'
            b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            b'"producer_commit":"dddddddddddddddddddddddddddddddddddddddd",'
            b'"reason_codes":["above_vwap","same_minute_volume"],"schema_version":1,'
            b'"signal_id":"cd7afd6b503b390c268d07ccc10c782ec5c181c97deb39927298d04227e58f4c",'
            b'"strategy_id":"n-shape","strategy_version":"2.1.0"}'
        ),
    ),
    (
        2,
        _ZERO_COMMIT,
        "58776536fa077ad048c31d0cd930e48844ea233715eb3962192ddb149d02e157",
        (
            b'{"action":"b_intent","available_at":"2026-07-31T01:32:00Z",'
            b'"candidate_id":"600000.SH","dataset_snapshot_id":"bbbbbbbbbbbbbbbbbbbbbbbb'
            b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","event_time":"2026-07-31T01:31:00Z",'
            b'"evidence":{"levels":{"resistance":10.2},"volume_ratio":2.5},'
            b'"expires_at":"2026-07-31T02:00:00Z","feature_snapshot_id":"cccccccccccccccc'
            b'cccccccccccccccccccccccccccccccccccccccccccccccc","parameter_fingerprint":"'
            b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            b'"producer_commit":"0000000000000000000000000000000000000000",'
            b'"reason_codes":["above_vwap","same_minute_volume"],"schema_version":2,'
            b'"signal_id":"58776536fa077ad048c31d0cd930e48844ea233715eb3962192ddb149d02e157",'
            b'"strategy_id":"n-shape","strategy_version":"2.1.0"}'
        ),
    ),
    (
        2,
        _COMMIT,
        "60281eb0d1c2fa8ab0d3a04d6dc385d45905e458f20ed6ed208656e4718635b0",
        (
            b'{"action":"b_intent","available_at":"2026-07-31T01:32:00Z",'
            b'"candidate_id":"600000.SH","dataset_snapshot_id":"bbbbbbbbbbbbbbbbbbbbbbbb'
            b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","event_time":"2026-07-31T01:31:00Z",'
            b'"evidence":{"levels":{"resistance":10.2},"volume_ratio":2.5},'
            b'"expires_at":"2026-07-31T02:00:00Z","feature_snapshot_id":"cccccccccccccccc'
            b'cccccccccccccccccccccccccccccccccccccccccccccccc","parameter_fingerprint":"'
            b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            b'"producer_commit":"dddddddddddddddddddddddddddddddddddddddd",'
            b'"reason_codes":["above_vwap","same_minute_volume"],"schema_version":2,'
            b'"signal_id":"60281eb0d1c2fa8ab0d3a04d6dc385d45905e458f20ed6ed208656e4718635b0",'
            b'"strategy_id":"n-shape","strategy_version":"2.1.0"}'
        ),
    ),
    (
        3,
        _ZERO_COMMIT,
        "95f72b6d9c7233438c3b97726aeff7da19b6b9ece2dc7138f1ff520197158744",
        (
            b'{"action":"b_intent","available_at":"2026-07-31T01:32:00Z",'
            b'"candidate_id":"600000.SH","dataset_snapshot_id":"bbbbbbbbbbbbbbbbbbbbbbbb'
            b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","event_time":"2026-07-31T01:31:00Z",'
            b'"evidence":{"levels":{"resistance":10.2},"volume_ratio":2.5},'
            b'"expires_at":"2026-07-31T02:00:00Z","feature_snapshot_id":"cccccccccccccccc'
            b'cccccccccccccccccccccccccccccccccccccccccccccccc","parameter_fingerprint":"'
            b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            b'"producer_commit":"0000000000000000000000000000000000000000",'
            b'"reason_codes":["above_vwap","same_minute_volume"],"schema_version":3,'
            b'"signal_id":"95f72b6d9c7233438c3b97726aeff7da19b6b9ece2dc7138f1ff520197158744",'
            b'"strategy_id":"n-shape","strategy_version":"2.1.0"}'
        ),
    ),
    (
        3,
        _COMMIT,
        "28fdb6371fce685ebefbd43699ee761645280ed80d8ef01dcf8d16f205874c43",
        (
            b'{"action":"b_intent","available_at":"2026-07-31T01:32:00Z",'
            b'"candidate_id":"600000.SH","dataset_snapshot_id":"bbbbbbbbbbbbbbbbbbbbbbbb'
            b'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","event_time":"2026-07-31T01:31:00Z",'
            b'"evidence":{"levels":{"resistance":10.2},"volume_ratio":2.5},'
            b'"expires_at":"2026-07-31T02:00:00Z","feature_snapshot_id":"cccccccccccccccc'
            b'cccccccccccccccccccccccccccccccccccccccccccccccc","parameter_fingerprint":"'
            b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
            b'"producer_commit":"dddddddddddddddddddddddddddddddddddddddd",'
            b'"reason_codes":["above_vwap","same_minute_volume"],"schema_version":3,'
            b'"signal_id":"28fdb6371fce685ebefbd43699ee761645280ed80d8ef01dcf8d16f205874c43",'
            b'"strategy_id":"n-shape","strategy_version":"2.1.0"}'
        ),
    ),
)
_LEGACY_FIXTURE_IDS = (
    "v1-zero",
    "v1-commit",
    "v2-zero",
    "v2-commit",
    "v3-zero",
    "v3-commit",
)
_CURRENT_ONLY_CANONICAL_WRITERS: tuple[tuple[str, Callable[[object], bytes]], ...] = (
    (
        "current_signal_envelope_json_bytes",
        signal_contracts.current_signal_envelope_json_bytes,
    ),
)


def _signal_kwargs() -> dict[str, object]:
    return {
        "schema_version": 1,
        "strategy_id": "n-shape",
        "strategy_version": "2.1.0",
        "parameter_fingerprint": _HASH_A,
        "dataset_snapshot_id": _HASH_B,
        "feature_snapshot_id": _HASH_C,
        "event_time": datetime(2026, 7, 31, 1, 31, tzinfo=UTC),
        "available_at": datetime(2026, 7, 31, 1, 32, tzinfo=UTC),
        "candidate_id": "600000.SH",
        "action": SignalAction.B_INTENT,
        "reason_codes": ("same_minute_volume", "above_vwap"),
        "evidence": {"volume_ratio": 2.5, "levels": {"resistance": 10.2}},
        "expires_at": datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
        "producer_commit": _COMMIT,
    }


def _current_kwargs(producer_identity: dict[str, object]) -> dict[str, object]:
    kwargs = _signal_kwargs()
    kwargs.pop("schema_version")
    kwargs.pop("producer_commit")
    kwargs["envelope_schema"] = "rquant.signal-envelope/v1"
    kwargs["producer_identity"] = producer_identity
    return kwargs


class TestR01LegacyFamily:
    @pytest.mark.parametrize(
        ("schema_version", "producer_commit", "expected_id", "original"),
        _LEGACY_CANONICAL_FIXTURES,
        ids=_LEGACY_FIXTURE_IDS,
    )
    def test_roundtrips_historical_ids_and_canonical_bytes(
        self,
        schema_version: int,
        producer_commit: str,
        expected_id: str,
        original: bytes,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        legacy_type = signal_contracts.LegacySignalEnvelope

        def fail_if_serialized(*_args: object, **_kwargs: object) -> bytes:
            raise AssertionError("legacy read path invoked serialization")

        with monkeypatch.context() as serialization_guard:
            serialization_guard.setattr(
                signal_contracts,
                "canonical_json_bytes",
                fail_if_serialized,
            )
            serialization_guard.setattr(
                signal_contracts,
                "current_signal_envelope_json_bytes",
                fail_if_serialized,
            )
            serialization_guard.setattr(legacy_type, "model_dump", fail_if_serialized)
            serialization_guard.setattr(legacy_type, "model_dump_json", fail_if_serialized)
            parsed = signal_contracts.parse_signal_envelope(original)

        expected_kwargs = _signal_kwargs()
        expected_kwargs.update(
            schema_version=schema_version,
            producer_commit=producer_commit,
            signal_id=expected_id,
        )
        expected = legacy_type(**expected_kwargs)
        mapped = signal_contracts.parse_signal_envelope(parsed.model_dump(mode="python"))
        serialized = signal_contracts.canonical_json_bytes(parsed.model_dump(mode="json"))

        assert type(parsed) is legacy_type
        assert type(mapped) is legacy_type
        assert mapped == parsed == expected
        assert parsed.signal_id == expected_id
        assert serialized == original
        assert legacy_type.model_validate_json(original) == parsed
        assert legacy_type.model_validate(parsed.model_dump(mode="python")) == parsed
        assert parsed.legacy_read_status is (
            signal_contracts.LegacySignalReadStatus.LEGACY_ZERO_SENTINEL
            if producer_commit == _ZERO_COMMIT
            else signal_contracts.LegacySignalReadStatus.LEGACY_COMMIT_CLAIM
        )
        assert "legacy_read_status" not in parsed.model_dump(mode="python")

    def test_pretty_json_control_cannot_satisfy_exact_r01_byte_evidence(self) -> None:
        original = _LEGACY_CANONICAL_FIXTURES[1][3]
        pretty = json.dumps(
            json.loads(original),
            ensure_ascii=True,
            indent=2,
            sort_keys=True,
        ).encode("utf-8")

        assert b"\n" in pretty
        assert pretty != original
        assert signal_contracts.parse_signal_envelope(pretty) == (
            signal_contracts.parse_signal_envelope(original)
        )

    def test_compatibility_name_is_the_explicit_legacy_type(self) -> None:
        assert SignalEnvelope is signal_contracts.LegacySignalEnvelope

    @pytest.mark.parametrize("schema_version", [True, 1.0, "1"])
    def test_requires_a_native_integer_schema_version(self, schema_version: object) -> None:
        kwargs = _signal_kwargs()
        kwargs["schema_version"] = schema_version

        with pytest.raises(ValidationError, match="schema_version"):
            signal_contracts.LegacySignalEnvelope(**kwargs)

    @pytest.mark.parametrize(
        "extra",
        [
            {"envelope_schema": "rquant.signal-envelope/v1"},
            {"producer_identity": {"kind": "git-commit-claim-sha1/v1"}},
            {"producer_generation_id": _HASH_A},
        ],
    )
    def test_rejects_current_family_root_fields(self, extra: dict[str, object]) -> None:
        kwargs = _signal_kwargs()
        kwargs.update(extra)

        with pytest.raises(ValidationError, match="extra_forbidden"):
            signal_contracts.LegacySignalEnvelope(**kwargs)


class TestR02WriteBoundary:
    def test_rejects_a_mismatched_stored_legacy_signal_id(self) -> None:
        kwargs = _signal_kwargs()
        kwargs["signal_id"] = "0" * 64

        with pytest.raises(ValidationError, match="signal_id"):
            signal_contracts.parse_signal_envelope(kwargs)

    @pytest.mark.parametrize(
        ("schema_version", "producer_commit", "expected_id", "original"),
        _LEGACY_CANONICAL_FIXTURES,
        ids=_LEGACY_FIXTURE_IDS,
    )
    @pytest.mark.parametrize(
        "writer",
        [writer for _name, writer in _CURRENT_ONLY_CANONICAL_WRITERS],
        ids=[name for name, _writer in _CURRENT_ONLY_CANONICAL_WRITERS],
    )
    def test_current_canonical_writers_reject_every_legacy_family_case(
        self,
        schema_version: int,
        producer_commit: str,
        expected_id: str,
        original: bytes,
        writer: Callable[[object], bytes],
    ) -> None:
        legacy = signal_contracts.parse_signal_envelope(original)

        assert legacy.schema_version == schema_version
        assert legacy.producer_commit == producer_commit
        assert legacy.signal_id == expected_id
        with pytest.raises(TypeError, match="CurrentSignalEnvelope") as exc_info:
            writer(legacy)
        assert exc_info.type is TypeError


class TestR03CurrentProducerIdentity:
    @pytest.mark.parametrize(
        ("identity", "expected_type_name"),
        [
            (
                {
                    "kind": "git-commit-claim-sha1/v1",
                    "producer_commit": _COMMIT,
                },
                "GitCommitClaimIdentity",
            ),
            (
                {
                    "kind": "full-manifest-sha256/v1",
                    "producer_generation_id": _HASH_A,
                },
                "FullManifestIdentity",
            ),
        ],
        ids=["git-commit-claim", "full-manifest"],
    )
    def test_accepts_each_exact_identity_variant(
        self,
        identity: dict[str, object],
        expected_type_name: str,
    ) -> None:
        current = signal_contracts.CurrentSignalEnvelope(**_current_kwargs(identity))

        assert type(current.producer_identity).__name__ == expected_type_name
        assert current.signal_id is not None
        assert len(current.signal_id) == 64

    @pytest.mark.parametrize(
        ("kind", "field", "value"),
        [
            ("git-commit-claim-sha1/v1", "producer_commit", "0" * 40),
            ("git-commit-claim-sha1/v1", "producer_commit", "D" * 40),
            ("git-commit-claim-sha1/v1", "producer_commit", "d" * 39),
            ("git-commit-claim-sha1/v1", "producer_commit", "g" * 40),
            ("git-commit-claim-sha1/v1", "producer_commit", None),
            ("full-manifest-sha256/v1", "producer_generation_id", "0" * 64),
            ("full-manifest-sha256/v1", "producer_generation_id", "A" * 64),
            ("full-manifest-sha256/v1", "producer_generation_id", "a" * 63),
            ("full-manifest-sha256/v1", "producer_generation_id", "g" * 64),
            ("full-manifest-sha256/v1", "producer_generation_id", None),
        ],
        ids=[
            "git-zero",
            "git-uppercase",
            "git-short",
            "git-nonhex",
            "git-null",
            "manifest-zero",
            "manifest-uppercase",
            "manifest-short",
            "manifest-nonhex",
            "manifest-null",
        ],
    )
    def test_rejects_noncanonical_or_zero_active_identity_values(
        self,
        kind: str,
        field: str,
        value: object,
    ) -> None:
        identity = {"kind": kind, field: value}

        with pytest.raises(ValidationError, match=field):
            signal_contracts.CurrentSignalEnvelope(**_current_kwargs(identity))

    @pytest.mark.parametrize(
        "identity",
        [
            {"kind": "git-commit-claim-sha1/v1"},
            {
                "kind": "git-commit-claim-sha1/v1",
                "producer_commit": _COMMIT,
                "producer_generation_id": _HASH_A,
            },
            {
                "kind": "git-commit-claim-sha1/v1",
                "producer_commit": _COMMIT,
                "producer_generation_id": None,
            },
            {"kind": "full-manifest-sha256/v1"},
            {
                "kind": "full-manifest-sha256/v1",
                "producer_generation_id": _HASH_A,
                "producer_commit": _COMMIT,
            },
            {
                "kind": "full-manifest-sha256/v1",
                "producer_generation_id": _HASH_A,
                "producer_commit": None,
            },
            {
                "kind": "full-manifest-sha256/v1",
                "producer_generation_id": _HASH_A,
                "unexpected": "field",
            },
        ],
        ids=[
            "git-neither",
            "git-both",
            "git-inactive-null",
            "manifest-neither",
            "manifest-both",
            "manifest-inactive-null",
            "extra-field",
        ],
    )
    def test_rejects_both_neither_inactive_null_or_extra_identity_fields(
        self,
        identity: dict[str, object],
    ) -> None:
        with pytest.raises(ValidationError, match="missing|extra_forbidden"):
            signal_contracts.CurrentSignalEnvelope(**_current_kwargs(identity))

    def test_reuses_common_validation_and_deep_evidence_freezing(self) -> None:
        kwargs = _current_kwargs({"kind": "git-commit-claim-sha1/v1", "producer_commit": _COMMIT})
        kwargs["reason_codes"] = ("same_minute_volume", "above_vwap")
        current = signal_contracts.CurrentSignalEnvelope(**kwargs)

        assert current.reason_codes == ("above_vwap", "same_minute_volume")
        with pytest.raises(TypeError):
            current.evidence["levels"]["resistance"] = 11.0  # type: ignore[index]

        invalid_time = dict(kwargs)
        invalid_time["event_time"] = invalid_time["expires_at"]
        with pytest.raises(ValidationError, match="event_time"):
            signal_contracts.CurrentSignalEnvelope(**invalid_time)


class TestR04StructuralDispatcher:
    @pytest.mark.parametrize(
        "identity",
        [
            {"kind": "git-commit-claim-sha1/v1", "producer_commit": _COMMIT},
            {
                "kind": "full-manifest-sha256/v1",
                "producer_generation_id": _HASH_A,
            },
        ],
        ids=["mapping-and-json-git", "mapping-and-json-manifest"],
    )
    def test_dispatches_mapping_and_json_to_the_exact_current_family(
        self,
        identity: dict[str, object],
    ) -> None:
        current = signal_contracts.CurrentSignalEnvelope(**_current_kwargs(identity))
        mapping_payload = current.model_dump(mode="python")
        json_payload = canonical_json_bytes(current.model_dump(mode="json"))

        mapped = signal_contracts.parse_signal_envelope(mapping_payload)
        decoded = signal_contracts.parse_signal_envelope(json_payload)

        assert type(mapped) is signal_contracts.CurrentSignalEnvelope
        assert type(decoded) is signal_contracts.CurrentSignalEnvelope
        assert mapped == decoded == current

    @pytest.mark.parametrize(
        "envelope_schema",
        ["rquant.signal-envelope/v2", None, 1],
        ids=["unknown", "null", "non-string"],
    )
    def test_rejects_unknown_or_nonexact_family_discriminators(
        self,
        envelope_schema: object,
    ) -> None:
        payload = _current_kwargs({"kind": "git-commit-claim-sha1/v1", "producer_commit": _COMMIT})
        payload["envelope_schema"] = envelope_schema

        with pytest.raises(
            (TypeError, ValueError, ValidationError),
            match="envelope_schema|schema",
        ):
            signal_contracts.parse_signal_envelope(payload)

    @pytest.mark.parametrize(
        "mutation",
        [
            {"schema_version": 1},
            {"producer_commit": _COMMIT},
            {"producer_generation_id": _HASH_A},
            {"unexpected": True},
        ],
        ids=["legacy-version", "legacy-commit", "root-generation", "extra-root"],
    )
    def test_rejects_mixed_or_extra_current_root_keys(
        self,
        mutation: dict[str, object],
    ) -> None:
        payload = _current_kwargs({"kind": "git-commit-claim-sha1/v1", "producer_commit": _COMMIT})
        payload.update(mutation)

        with pytest.raises(ValidationError, match="extra_forbidden"):
            signal_contracts.parse_signal_envelope(payload)

    @pytest.mark.parametrize(
        "mutation",
        [
            {"producer_identity": None},
            {"producer_identity": {"kind": "unknown/v1"}},
        ],
        ids=["neither-family", "unknown-identity-kind"],
    )
    def test_rejects_objects_satisfying_no_exact_family(
        self,
        mutation: dict[str, object],
    ) -> None:
        payload = _current_kwargs({"kind": "git-commit-claim-sha1/v1", "producer_commit": _COMMIT})
        payload.update(mutation)

        with pytest.raises(ValidationError):
            signal_contracts.parse_signal_envelope(payload)

    @pytest.mark.parametrize(
        "payload",
        [
            b'{"envelope_schema":',
            b"[]",
            (
                b'{"envelope_schema":"rquant.signal-envelope/v1",'
                b'"envelope_schema":"rquant.signal-envelope/v1"}'
            ),
        ],
        ids=["malformed-json", "non-object-json", "duplicate-discriminator"],
    )
    def test_json_dispatch_rejects_malformed_or_ambiguous_inputs(self, payload: bytes) -> None:
        with pytest.raises((TypeError, ValueError)):
            signal_contracts.parse_signal_envelope(payload)


class TestR05CurrentIdentitySeparation:
    def test_rejects_a_mismatched_current_signal_id(self) -> None:
        kwargs = _current_kwargs({"kind": "git-commit-claim-sha1/v1", "producer_commit": _COMMIT})
        kwargs["signal_id"] = "0" * 64

        with pytest.raises(ValidationError, match="signal_id"):
            signal_contracts.CurrentSignalEnvelope(**kwargs)

    def test_separates_legacy_current_kind_and_active_identity_values(self) -> None:
        legacy = signal_contracts.LegacySignalEnvelope(**_signal_kwargs())
        git_d = signal_contracts.CurrentSignalEnvelope(
            **_current_kwargs({"kind": "git-commit-claim-sha1/v1", "producer_commit": "d" * 40})
        )
        git_e = signal_contracts.CurrentSignalEnvelope(
            **_current_kwargs({"kind": "git-commit-claim-sha1/v1", "producer_commit": "e" * 40})
        )
        manifest_d = signal_contracts.CurrentSignalEnvelope(
            **_current_kwargs(
                {
                    "kind": "full-manifest-sha256/v1",
                    "producer_generation_id": "d" * 64,
                }
            )
        )
        manifest_e = signal_contracts.CurrentSignalEnvelope(
            **_current_kwargs(
                {
                    "kind": "full-manifest-sha256/v1",
                    "producer_generation_id": "e" * 64,
                }
            )
        )

        assert (
            len(
                {
                    legacy.signal_id,
                    git_d.signal_id,
                    git_e.signal_id,
                    manifest_d.signal_id,
                    manifest_e.signal_id,
                }
            )
            == 5
        )

    def test_current_canonical_writer_roundtrips_only_the_current_family(self) -> None:
        current = signal_contracts.CurrentSignalEnvelope(
            **_current_kwargs(
                {
                    "kind": "full-manifest-sha256/v1",
                    "producer_generation_id": _HASH_A,
                }
            )
        )

        payload = signal_contracts.current_signal_envelope_json_bytes(current)

        assert payload == canonical_json_bytes(current.model_dump(mode="json"))
        assert signal_contracts.parse_signal_envelope(payload) == current


def test_signal_id_is_stable_for_evidence_order_and_equivalent_timezones() -> None:
    left = SignalEnvelope(**_signal_kwargs())
    right_kwargs = _signal_kwargs()
    right_kwargs.update(
        event_time=datetime(
            2026,
            7,
            31,
            9,
            31,
            tzinfo=timezone(timedelta(hours=8)),
        ),
        available_at=datetime(
            2026,
            7,
            31,
            9,
            32,
            tzinfo=timezone(timedelta(hours=8)),
        ),
        expires_at=datetime(
            2026,
            7,
            31,
            10,
            0,
            tzinfo=timezone(timedelta(hours=8)),
        ),
        evidence={"levels": {"resistance": 10.2}, "volume_ratio": 2.5},
    )
    right = SignalEnvelope(**right_kwargs)

    assert left.signal_id == right.signal_id
    assert left.signal_id is not None
    assert len(left.signal_id) == 64
    assert left.event_time.tzinfo is UTC


def test_signal_id_is_verified_when_supplied() -> None:
    generated = SignalEnvelope(**_signal_kwargs())

    assert SignalEnvelope(signal_id=generated.signal_id, **_signal_kwargs()) == generated
    with pytest.raises(ValidationError, match="signal_id"):
        SignalEnvelope(signal_id="0" * 64, **_signal_kwargs())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("parameter_fingerprint", "A" * 64),
        ("dataset_snapshot_id", "b" * 63),
        ("feature_snapshot_id", "g" * 64),
    ],
)
def test_signal_content_hashes_require_lowercase_sha256(field: str, value: str) -> None:
    kwargs = _signal_kwargs()
    kwargs[field] = value

    with pytest.raises(ValidationError):
        SignalEnvelope(**kwargs)


def test_signal_requires_unique_nonempty_reason_codes() -> None:
    duplicate = _signal_kwargs()
    duplicate["reason_codes"] = ("above_vwap", "above_vwap")
    empty = _signal_kwargs()
    empty["reason_codes"] = ("above_vwap", "")

    with pytest.raises(ValidationError, match="reason_codes"):
        SignalEnvelope(**duplicate)
    with pytest.raises(ValidationError, match="reason_codes"):
        SignalEnvelope(**empty)


def test_signal_identity_treats_reason_codes_as_a_set_and_deep_freezes_evidence() -> None:
    left = SignalEnvelope(**_signal_kwargs())
    reversed_reasons = _signal_kwargs()
    reversed_reasons["reason_codes"] = tuple(reversed(left.reason_codes))
    right = SignalEnvelope(**reversed_reasons)

    assert left.signal_id == right.signal_id
    with pytest.raises(TypeError):
        left.evidence["volume_ratio"] = 3.0
    restored = SignalEnvelope.model_validate_json(left.model_dump_json())
    assert restored == left


def test_signal_can_cross_a_revalidating_contract_boundary_after_deep_freeze() -> None:
    signal = SignalEnvelope(**_signal_kwargs())

    revalidated = SignalEnvelope.model_validate(signal)

    assert revalidated == signal
    with pytest.raises(TypeError):
        revalidated.evidence["levels"]["resistance"] = 11.0  # type: ignore[index]


@pytest.mark.parametrize(
    ("event_offset", "available_offset", "expires_offset"),
    [
        (2, 1, 30),
        (0, 30, 30),
        (0, 31, 30),
    ],
)
def test_signal_enforces_visibility_and_expiry_order(
    event_offset: int,
    available_offset: int,
    expires_offset: int,
) -> None:
    kwargs = _signal_kwargs()
    anchor = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
    kwargs.update(
        event_time=anchor + timedelta(minutes=event_offset),
        available_at=anchor + timedelta(minutes=available_offset),
        expires_at=anchor + timedelta(minutes=expires_offset),
    )

    with pytest.raises(ValidationError, match="event_time|available_at|expires_at"):
        SignalEnvelope(**kwargs)


def test_signal_allows_event_to_become_available_at_the_same_instant() -> None:
    kwargs = _signal_kwargs()
    kwargs["available_at"] = kwargs["event_time"]

    assert SignalEnvelope(**kwargs).available_at == kwargs["event_time"]


def test_signal_contract_is_frozen_and_rejects_unknown_or_naive_values() -> None:
    signal = SignalEnvelope(**_signal_kwargs())
    assert isinstance(signal, RuntimeContractModel)

    with pytest.raises(ValidationError):
        signal.candidate_id = "changed"
    with pytest.raises(ValidationError):
        SignalEnvelope(unexpected=True, **_signal_kwargs())

    kwargs = _signal_kwargs()
    kwargs["event_time"] = datetime(2026, 7, 31, 9, 31)
    with pytest.raises(ValidationError, match="timezone-aware"):
        SignalEnvelope(**kwargs)

    bad_commit = _signal_kwargs()
    bad_commit["producer_commit"] = "release-main"
    with pytest.raises(ValidationError, match="producer_commit"):
        SignalEnvelope(**bad_commit)


def test_signal_action_values_are_stable() -> None:
    assert [action.value for action in SignalAction] == [
        "watch",
        "b_intent",
        "reduce",
        "s_intent",
        "cancel",
    ]
