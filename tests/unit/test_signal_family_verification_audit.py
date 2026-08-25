"""Phase C audit-schema red tests.

Covers `RESET-REG-P0` and `RESET-REG-P2`: the bounded audit record contains identifiers,
hashes, timestamps, outcomes, and bounded reason codes only. Signal payloads, verification
vector inputs, environment values, secrets, credentials, and raw exception text are not
merely filtered — they are unrepresentable, because every field of the record is an enum, a
frozen literal, a SHA-256 pattern, a microsecond UTC timestamp pattern, or a frozen pair ID.

The decoy inputs below are deliberately shaped like the things that must never leak.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import Enum
from typing import Any

import pytest

from rquant import signal_family_verification as verification
from rquant.strict_json import canonical_json_bytes

REJECTIONS = (ValueError, TypeError)

RECORDED_AT = datetime(2026, 8, 24, 7, 30, 15, 250000, tzinfo=UTC)
HASH_PATTERN = r"^[0-9a-f]{64}$"
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$"

# Every decoy is exactly the kind of value authority.md L1495-1497 forbids in audit records.
# Hosts and addresses here stay inside RFC 5737 documentation space: a decoy must never
# name a real production asset.
DECOYS: tuple[tuple[str, str], ...] = (
    ("raw-exception-text", "Traceback (most recent call last): KeyError: 'tushare_token'"),
    ("environment-value", "PUSHDEER_KEYS=pdxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"),
    ("secret", "sk-live-51H9dEadBeEfCafeBabe0123456789"),
    ("credential", "lighthouse:hunter2@203.0.113.5"),
    ("signal-payload", '{"ts_code":"600519.SH","side":"buy","qty":100}'),
    ("vector-input", '{"pair":"router-paper","surface":"consume_signal_bus_to_paper"}'),
    ("free-form-reason", "the notifier reader surface raised while reading the spool"),
)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _record(
    **overrides: Any,
) -> verification.SignalFamilyVerificationAuditRecordV1:
    values: dict[str, Any] = {
        "event": verification.SignalFamilyAuditEvent.CHILD_RESULT_VALIDATED,
        "outcome": verification.SignalFamilyAuditOutcome.REJECTED,
        "reason_code": verification.SignalFamilyReasonCode.CHILD_RESULT_IDENTITY_MISMATCH,
        "recorded_at": RECORDED_AT,
        "pair_id": "router-paper",
        "overlay_content_hash": _digest("overlay"),
        "authority_epoch_key": _digest("epoch"),
        "verifier_policy_content_hash": _digest("policy"),
        "selected_entry_hash": _digest("entry"),
        "subject_hash": _digest("subject"),
        "existing_hash": None,
        "attempted_hash": None,
    }
    values.update(overrides)
    return verification.SignalFamilyVerificationAuditRecordV1.create(**values)


def _schema() -> dict[str, Any]:
    return verification.SignalFamilyVerificationAuditRecordV1.model_json_schema()


def _accepted_string_forms(prop: dict[str, Any], schema: dict[str, Any]) -> list[str]:
    """Classify every non-null branch a string field will accept."""

    branches = prop.get("anyOf", [prop])
    forms: list[str] = []
    for branch in branches:
        if branch.get("type") == "null":
            continue
        if "$ref" in branch:
            target = schema["$defs"][branch["$ref"].rsplit("/", 1)[-1]]
            forms.append("enum" if "enum" in target else "unbounded")
            continue
        if "enum" in branch or "const" in branch:
            forms.append("enum")
            continue
        if branch.get("type") == "integer":
            forms.append("integer")
            continue
        if branch.get("pattern"):
            forms.append("pattern:" + branch["pattern"])
            continue
        forms.append("unbounded")
    return forms


# --------------------------------------------------------------------------------------
# The bounded audit schema
# --------------------------------------------------------------------------------------


def test_audit_record_has_exactly_the_bounded_fields_in_order() -> None:
    assert tuple(verification.SignalFamilyVerificationAuditRecordV1.model_fields) == (
        "schema_version",
        "event",
        "outcome",
        "reason_code",
        "recorded_at",
        "pair_id",
        "overlay_content_hash",
        "authority_epoch_key",
        "verifier_policy_content_hash",
        "selected_entry_hash",
        "subject_hash",
        "existing_hash",
        "attempted_hash",
        "record_hash",
    )


def test_every_audit_field_is_an_enum_literal_hash_or_timestamp() -> None:
    schema = _schema()
    allowed = {
        "schema_version": {"integer"},
        "event": {"enum"},
        "outcome": {"enum"},
        "reason_code": {"enum"},
        "recorded_at": {"pattern:" + TIMESTAMP_PATTERN},
        "pair_id": {"enum"},
        "overlay_content_hash": {"pattern:" + HASH_PATTERN},
        "authority_epoch_key": {"pattern:" + HASH_PATTERN},
        "verifier_policy_content_hash": {"pattern:" + HASH_PATTERN},
        "selected_entry_hash": {"pattern:" + HASH_PATTERN},
        "subject_hash": {"pattern:" + HASH_PATTERN},
        "existing_hash": {"pattern:" + HASH_PATTERN},
        "attempted_hash": {"pattern:" + HASH_PATTERN},
        "record_hash": {"pattern:" + HASH_PATTERN},
    }
    for name, prop in schema["properties"].items():
        forms = set(_accepted_string_forms(prop, schema))
        assert forms == allowed[name], name
    assert set(schema["properties"]) == set(allowed)


def test_no_audit_field_is_named_after_a_forbidden_payload() -> None:
    forbidden = (
        "payload",
        "input",
        "vector_input",
        "environment",
        "env",
        "secret",
        "credential",
        "token",
        "message",
        "detail",
        "traceback",
        "exception",
        "error_text",
        "note",
    )
    for name in verification.SignalFamilyVerificationAuditRecordV1.model_fields:
        assert not [token for token in forbidden if token in name], name


def test_reason_codes_are_a_closed_bounded_enum() -> None:
    codes = tuple(verification.SignalFamilyReasonCode)
    assert issubclass(verification.SignalFamilyReasonCode, Enum)
    assert 1 <= len(codes) <= 64
    assert len({code.value for code in codes}) == len(codes)
    for code in codes:
        assert code.value == code.name
        assert code.value.isupper()
        assert code.value.replace("_", "").isalnum()
        assert len(code.value) <= 48


def test_audit_events_and_outcomes_are_closed_enums() -> None:
    assert {outcome.value for outcome in verification.SignalFamilyAuditOutcome} == {
        "accepted",
        "rejected",
    }
    events = tuple(verification.SignalFamilyAuditEvent)
    assert 1 <= len(events) <= 32
    for event in events:
        assert event.value.replace("_", "").isalnum()
        assert event.value.islower()


def test_record_hash_matches_its_canonical_preimage() -> None:
    record = _record()
    assert record.record_hash == hashlib.sha256(
        canonical_json_bytes(record.model_dump(mode="json", exclude={"record_hash"}))
    ).hexdigest()
    assert record.recorded_at == "2026-08-24T07:30:15.250000Z"


def test_an_accepted_outcome_carries_no_reason_code_and_a_rejection_requires_one() -> None:
    accepted = _record(
        event=verification.SignalFamilyAuditEvent.POLICY_VALIDATED,
        outcome=verification.SignalFamilyAuditOutcome.ACCEPTED,
        reason_code=None,
    )
    assert accepted.reason_code is None
    with pytest.raises(REJECTIONS, match="an accepted audit record carries no reason code"):
        _record(outcome=verification.SignalFamilyAuditOutcome.ACCEPTED)
    with pytest.raises(REJECTIONS, match="a rejected audit record requires a reason code"):
        _record(outcome=verification.SignalFamilyAuditOutcome.REJECTED, reason_code=None)


def test_audit_record_rejects_an_extra_field() -> None:
    payload = _record().model_dump(mode="json")
    payload["reason_text"] = "anything"
    with pytest.raises(REJECTIONS, match="Extra inputs are not permitted"):
        verification.SignalFamilyVerificationAuditRecordV1.model_validate(payload)


def test_audit_record_rejects_a_pair_outside_the_frozen_five() -> None:
    with pytest.raises(REJECTIONS, match="Input should be"):
        _record(pair_id="strategy-serving")


# --------------------------------------------------------------------------------------
# Decoys are unrepresentable, not merely filtered
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(("label", "decoy"), DECOYS, ids=[label for label, _ in DECOYS])
def test_no_audit_field_accepts_a_forbidden_decoy_value(label: str, decoy: str) -> None:
    for name in verification.SignalFamilyVerificationAuditRecordV1.model_fields:
        if name == "record_hash":
            continue
        with pytest.raises(REJECTIONS):
            _record(**{name: decoy})


@pytest.mark.parametrize(("label", "decoy"), DECOYS, ids=[label for label, _ in DECOYS])
def test_a_serialized_audit_record_never_carries_a_decoy(label: str, decoy: str) -> None:
    serialized = canonical_json_bytes(_record().model_dump(mode="json")).decode("utf-8")
    assert decoy not in serialized
    for fragment in decoy.split():
        assert fragment not in serialized


def test_a_rejection_raised_from_a_decoy_bearing_exception_logs_only_a_reason_code() -> None:
    decoy = "KeyError: 'PUSHDEER_KEYS=pd-secret'"
    try:
        raise RuntimeError(decoy)
    except RuntimeError as exc:
        record = verification.SignalFamilyVerificationAuditRecordV1.create(
            event=verification.SignalFamilyAuditEvent.CHILD_LAUNCHED,
            outcome=verification.SignalFamilyAuditOutcome.REJECTED,
            reason_code=verification.SignalFamilyReasonCode.CHILD_NONZERO_EXIT,
            recorded_at=RECORDED_AT,
            pair_id=None,
            overlay_content_hash=_digest("overlay"),
            authority_epoch_key=_digest("epoch"),
            verifier_policy_content_hash=_digest("policy"),
            selected_entry_hash=_digest("entry"),
            subject_hash=None,
            existing_hash=None,
            attempted_hash=None,
        )
        assert str(exc) == decoy
    serialized = canonical_json_bytes(record.model_dump(mode="json")).decode("utf-8")
    assert "KeyError" not in serialized
    assert "PUSHDEER_KEYS" not in serialized
    assert "pd-secret" not in serialized
    assert record.reason_code is verification.SignalFamilyReasonCode.CHILD_NONZERO_EXIT


def test_conflict_evidence_from_a_divergent_receipt_is_hash_only() -> None:
    existing = _digest("existing-receipt")
    attempted = _digest("attempted-receipt")
    record = verification.build_conflict_audit_record(
        event=verification.SignalFamilyAuditEvent.RECEIPT_APPENDED,
        reason_code=verification.SignalFamilyReasonCode.RECEIPT_CONFLICT,
        recorded_at=RECORDED_AT,
        pair_id="notifier-serving",
        overlay_content_hash=_digest("overlay"),
        authority_epoch_key=_digest("epoch"),
        existing_hash=existing,
        attempted_hash=attempted,
    )
    payload = record.model_dump(mode="json")
    assert payload["existing_hash"] == existing
    assert payload["attempted_hash"] == attempted
    assert payload["outcome"] == "rejected"
    for key, value in payload.items():
        if value is None or key in {"event", "outcome", "reason_code", "pair_id"}:
            continue
        if key == "schema_version":
            assert value == 1
            continue
        if key == "recorded_at":
            assert value == "2026-08-24T07:30:15.250000Z"
            continue
        assert isinstance(value, str)
        assert len(value) == 64
        assert set(value) <= set("0123456789abcdef")


def test_the_module_exposes_no_free_text_audit_constructor() -> None:
    exported = [name for name in dir(verification) if not name.startswith("_")]
    assert not [
        name
        for name in exported
        if any(token in name.lower() for token in ("log_", "_text", "message", "traceback"))
    ]
