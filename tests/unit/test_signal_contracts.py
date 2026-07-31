from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from rquant.runtime_contracts import RuntimeContractModel
from rquant.signal_contracts import SignalAction, SignalEnvelope

_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_COMMIT = "d" * 40


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
