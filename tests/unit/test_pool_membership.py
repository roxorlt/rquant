"""A pool entry is a proven transition between adjacent successful trading days."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from rquant.pool_membership import (
    PoolDayEvidence,
    PoolMemberClose,
    PoolMembershipMember,
    PoolMembershipProjection,
    compute_pool_membership,
)
from rquant.pool_result_receipt import ScreenRunReceipt, member_set_digest

_POOL = "user/watch"
_VERSION = "a" * 64
_NEXT_VERSION = "b" * 64
_FRIDAY = date(2026, 8, 7)
_MONDAY = date(2026, 8, 10)
_TUESDAY = date(2026, 8, 11)
_WEDNESDAY = date(2026, 8, 12)


def _run(
    trade_date: date,
    prices: dict[str, float | None],
    *,
    version: str = _VERSION,
    minute: int = 0,
    lineage_complete: bool = True,
) -> PoolDayEvidence:
    receipt = ScreenRunReceipt(
        trade_date=trade_date,
        preset_name=_POOL,
        definition_version=version,
        hit_count=len(prices),
        member_digest=member_set_digest(list(prices)),
        lineage_complete=lineage_complete,
        completed_at=datetime(2026, 8, 12, 8, tzinfo=UTC) + timedelta(minutes=minute),
    )
    return PoolDayEvidence(
        trade_date=trade_date,
        receipt=receipt,
        members=tuple(PoolMemberClose(ts_code=code, close=close) for code, close in prices.items()),
    )


def _missing(trade_date: date, *, legacy: bool = False) -> PoolDayEvidence:
    return PoolDayEvidence(
        trade_date=trade_date,
        receipt=None,
        missing_receipt_reason="legacy_unproven" if legacy else "not_run",
    )


def _compute(
    calendar: tuple[date, ...],
    *days: PoolDayEvidence,
    version: str = _VERSION,
    calendar_complete: bool = True,
) -> PoolMembershipProjection:
    return compute_pool_membership(
        pool_name=_POOL,
        published_definition_version=version,
        trading_days=calendar,
        days=days,
        calendar_complete=calendar_complete,
    )


def test_adjacent_successful_runs_preserve_entry_across_weekend() -> None:
    result = _compute(
        (_FRIDAY, _MONDAY, _TUESDAY),
        _run(_FRIDAY, {}),
        _run(_MONDAY, {"A": 10.0}),
        _run(_TUESDAY, {"A": 12.0}),
    )

    assert result.status == "verified"
    assert result.trade_date == _TUESDAY
    assert result.result_version is not None
    assert len(result.members) == 1
    member = result.members[0]
    assert member.ts_code == "A"
    assert member.entry_trade_date == _MONDAY
    assert member.entry_close == 10.0
    assert member.entry_result_version == _run(_MONDAY, {"A": 10.0}).receipt.result_version
    assert member.unknown_reason is None


def test_verified_zero_hits_end_period_and_reentry_uses_new_close() -> None:
    result = _compute(
        (_FRIDAY, _MONDAY, _TUESDAY, _WEDNESDAY),
        _run(_FRIDAY, {}),
        _run(_MONDAY, {"A": 10.0}),
        _run(_TUESDAY, {}),
        _run(_WEDNESDAY, {"A": 15.0}),
    )

    assert result.status == "verified"
    assert result.members[0].entry_trade_date == _WEDNESDAY
    assert result.members[0].entry_close == 15.0


@pytest.mark.parametrize(
    ("missing", "reason"),
    [
        (_missing(_MONDAY), "not_run"),
        (_missing(_MONDAY, legacy=True), "legacy_unproven"),
    ],
)
def test_unproved_day_breaks_continuity_without_becoming_zero_hit(
    missing: PoolDayEvidence, reason: str
) -> None:
    result = _compute(
        (_FRIDAY, _MONDAY, _TUESDAY),
        _run(_FRIDAY, {}),
        missing,
        _run(_TUESDAY, {"A": 12.0}),
    )

    member = result.members[0]
    assert member.entry_trade_date is None
    assert member.entry_close is None
    assert member.unknown_reason == reason


def test_window_first_member_is_not_assumed_newly_entered() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {"A": 10.0}),
        _run(_TUESDAY, {"A": 12.0}),
    )

    assert result.members[0].entry_trade_date is None
    assert result.members[0].entry_close is None
    assert result.members[0].unknown_reason == "window_truncated"
    assert result.members[0].entry_result_version is None


def test_new_member_can_have_proven_entry_while_older_member_remains_unknown() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {"A": 10.0}),
        _run(_TUESDAY, {"A": 11.0, "B": 20.0}),
    )

    older, newer = result.members
    assert older.ts_code == "A"
    assert older.entry_trade_date is None
    assert older.unknown_reason == "window_truncated"
    assert newer.ts_code == "B"
    assert newer.entry_trade_date == _TUESDAY
    assert newer.entry_close == 20.0
    assert newer.entry_result_version == result.result_version


def test_latest_same_day_rerun_replaces_old_membership_and_zero_is_proof() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {"A": 10.0}),
        _run(_TUESDAY, {"A": 11.0}, minute=1),
        _run(_TUESDAY, {}, minute=2),
    )

    assert result.status == "verified"
    assert result.members == ()
    assert result.result_version == _run(_TUESDAY, {}, minute=2).receipt.result_version


def test_latest_historical_rerun_is_used_for_subsequent_entry() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY, _WEDNESDAY),
        _run(_MONDAY, {}),
        _run(_TUESDAY, {"A": 10.0}, minute=1),
        _run(_TUESDAY, {}, minute=2),
        _run(_WEDNESDAY, {"A": 13.0}),
    )

    assert result.members[0].entry_trade_date == _WEDNESDAY
    assert result.members[0].entry_close == 13.0


@pytest.mark.parametrize("bad_close", [None, float("nan"), float("inf"), 0.0])
def test_missing_or_invalid_entry_close_does_not_fabricate_entry(
    bad_close: float | None,
) -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {}),
        _run(_TUESDAY, {"A": bad_close}),
    )

    member = result.members[0]
    assert member.entry_trade_date == _TUESDAY
    assert member.entry_result_version == result.result_version
    assert member.entry_close is None
    assert member.unknown_reason == "entry_price_missing"


def test_missing_current_close_does_not_erase_proven_entry() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY, _WEDNESDAY),
        _run(_MONDAY, {}),
        _run(_TUESDAY, {"A": 10.0}),
        _run(_WEDNESDAY, {"A": None}),
    )

    member = result.members[0]
    assert member.entry_trade_date == _TUESDAY
    assert member.entry_close == 10.0
    assert member.unknown_reason is None


def test_incomplete_calendar_suppresses_all_entry_claims() -> None:
    result = _compute(
        (_MONDAY, _WEDNESDAY),
        _run(_MONDAY, {}),
        _run(_WEDNESDAY, {"A": 10.0}),
        calendar_complete=False,
    )

    assert result.status == "calendar_incomplete"
    assert result.members[0].entry_trade_date is None
    assert result.members[0].unknown_reason == "calendar_incomplete"


def test_latest_result_must_match_published_definition_version() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {}),
        _run(_TUESDAY, {"A": 10.0}, version=_NEXT_VERSION),
    )

    assert result.status == "definition_mismatch"
    assert result.members == ()
    assert result.result_version is not None


def test_old_definition_cannot_prove_transition_for_new_definition() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {}, version=_NEXT_VERSION),
        _run(_TUESDAY, {"A": 10.0}),
    )

    assert result.status == "verified"
    assert result.members[0].entry_trade_date is None
    assert result.members[0].unknown_reason == "definition_mismatch"


def test_current_unrun_or_legacy_day_does_not_expose_unproved_members() -> None:
    for legacy, status in ((False, "not_run"), (True, "legacy_unproven")):
        result = _compute(
            (_MONDAY, _TUESDAY),
            _run(_MONDAY, {"A": 10.0}),
            _missing(_TUESDAY, legacy=legacy),
        )
        assert result.status == status
        assert result.result_version is None
        assert result.members == ()


def test_incomplete_parent_lineage_never_proves_membership_entry() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {}, lineage_complete=False),
        _run(_TUESDAY, {"A": 10.0}),
    )
    assert result.members[0].entry_trade_date is None
    assert result.members[0].unknown_reason == "lineage_incomplete"


def test_receipt_member_mismatch_never_proves_entry() -> None:
    valid = _run(_MONDAY, {"A": 10.0})
    corrupt = PoolDayEvidence(
        trade_date=_MONDAY,
        receipt=valid.receipt,
        members=(PoolMemberClose(ts_code="B", close=10.0),),
    )
    result = _compute((_MONDAY, _TUESDAY), corrupt, _run(_TUESDAY, {"A": 12.0}))

    assert result.members[0].entry_trade_date is None
    assert result.members[0].unknown_reason == "receipt_mismatch"


def test_same_timestamp_with_conflicting_reruns_is_ambiguous() -> None:
    result = _compute(
        (_MONDAY, _TUESDAY),
        _run(_MONDAY, {}),
        _run(_TUESDAY, {"A": 10.0}, minute=1),
        _run(_TUESDAY, {"B": 20.0}, minute=1),
    )
    assert result.status == "ambiguous_rerun"
    assert result.members == ()


def test_receipt_for_a_different_pool_is_rejected() -> None:
    wrong = _run(_TUESDAY, {"A": 10.0})
    assert wrong.receipt is not None
    receipt_fields = wrong.receipt.model_dump(exclude={"result_version"})
    receipt_fields["preset_name"] = "other"
    wrong = PoolDayEvidence(
        trade_date=_TUESDAY,
        receipt=ScreenRunReceipt.model_validate(receipt_fields),
        members=wrong.members,
    )
    with pytest.raises(ValueError, match="pool"):
        _compute((_TUESDAY,), wrong)


def test_output_can_keep_proven_day_when_price_evidence_is_missing() -> None:
    member = PoolMembershipMember(
        ts_code="A",
        entry_trade_date=_TUESDAY,
        entry_result_version="c" * 64,
        unknown_reason="entry_price_missing",
    )
    assert member.entry_trade_date == _TUESDAY
    assert member.entry_close is None


def test_output_cannot_claim_entry_without_result_proof_or_price_reason() -> None:
    with pytest.raises(ValueError, match="entry evidence"):
        PoolMembershipMember(
            ts_code="A",
            entry_trade_date=_TUESDAY,
            unknown_reason="entry_price_missing",
        )
    with pytest.raises(ValueError, match="entry evidence"):
        PoolMembershipMember(ts_code="A", entry_trade_date=_TUESDAY, entry_result_version="c" * 64)
    with pytest.raises(ValueError, match="entry evidence"):
        PoolMembershipMember(
            ts_code="A",
            entry_trade_date=_TUESDAY,
            entry_close=10.0,
            entry_result_version="c" * 64,
            unknown_reason="window_truncated",
        )
