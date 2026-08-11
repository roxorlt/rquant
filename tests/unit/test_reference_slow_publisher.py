from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.reference_data_registry import ReferenceDataset, ReferenceRegistry
from rquant.reference_slow_publisher import (
    ReferenceDailyFact,
    ReferenceSecurityFact,
    ReferenceSlowPublicationError,
    ReferenceSlowSourceSnapshot,
    build_reference_slow_serving_result,
    publish_reference_slow_snapshot,
)
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.runtime_serving_snapshot import ReferenceSlowPayload

COMMIT = "a" * 40
TARGET_DATE = date(2026, 7, 31)
PRIOR_DATE = date(2026, 7, 30)
CAPTURED_AT = datetime(2026, 7, 31, 1, 20, tzinfo=UTC)
VISIBLE_AT = datetime(2026, 7, 31, 1, 24, 40, tzinfo=UTC)
AVAILABLE_AT = VISIBLE_AT + timedelta(seconds=5)


def _calendar() -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit=COMMIT,
        coverage_start=date(2026, 7, 20),
        coverage_end=date(2026, 8, 3),
        open_dates=(
            date(2026, 7, 20),
            date(2026, 7, 21),
            date(2026, 7, 22),
            date(2026, 7, 23),
            date(2026, 7, 24),
            date(2026, 7, 27),
            date(2026, 7, 28),
            date(2026, 7, 29),
            PRIOR_DATE,
            TARGET_DATE,
            date(2026, 8, 3),
        ),
        generated_at=datetime(2026, 7, 19, tzinfo=UTC),
    )


def _snapshot() -> ReferenceSlowSourceSnapshot:
    return ReferenceSlowSourceSnapshot.create(
        target_trade_date=TARGET_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT,
        source_snapshot_ids={
            "daily": "1" * 64,
            "security": "2" * 64,
            "suspension": "3" * 64,
            "calendar": _calendar().content_sha256,
        },
        daily_facts=(
            ReferenceDailyFact(
                ts_code="300001.SZ",
                trade_date=PRIOR_DATE,
                close_raw=20.0,
                prior_adj_factor=1.0,
                adj_factor=2.0,
            ),
            ReferenceDailyFact(
                ts_code="600000.SH",
                trade_date=PRIOR_DATE,
                close_raw=10.0,
                prior_adj_factor=1.0,
                adj_factor=1.0,
            ),
        ),
        security_facts=(
            ReferenceSecurityFact(
                ts_code="300001.SZ",
                name="成长样本",
                list_date=date(2020, 1, 2),
                market="创业板",
            ),
            ReferenceSecurityFact(
                ts_code="600000.SH",
                name="*ST样本",
                list_date=date(1999, 11, 10),
                market="主板",
            ),
        ),
        suspended_codes=("600000.SH",),
    )


def test_publishes_six_point_in_time_reference_domains_per_security(
    tmp_path: Path,
) -> None:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")

    receipt = publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=_snapshot(),
        completion_clock=lambda: VISIBLE_AT,
    )

    assert receipt.inserted_record_count == 12
    assert receipt.security_count == 2
    assert receipt.target_trade_date == TARGET_DATE
    assert receipt.generation_id == registry.current_pointer().generation_id
    event_time = datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
    common = {
        "event_time": event_time,
        "decision_time": AVAILABLE_AT,
        "generation_id": receipt.generation_id,
    }
    st = registry.as_of(
        dataset_id=ReferenceDataset.ST_STATUS,
        key="600000.SH",
        **common,
    )
    suspension = registry.as_of(
        dataset_id=ReferenceDataset.SUSPENSION_STATUS,
        key="600000.SH",
        **common,
    )
    listing = registry.as_of(
        dataset_id=ReferenceDataset.LISTING_STATUS,
        key="300001.SZ",
        **common,
    )
    board = registry.as_of(
        dataset_id=ReferenceDataset.BOARD_MEMBERSHIP,
        key="300001.SZ",
        **common,
    )
    adjustment = registry.as_of(
        dataset_id=ReferenceDataset.ADJUSTMENT_FACTOR,
        key="300001.SZ",
        **common,
    )
    price_limit = registry.as_of(
        dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
        key="300001.SZ",
        **common,
    )

    assert st.record.payload == {"is_st": True, "name": "*ST样本"}
    assert suspension.record.payload == {"is_suspended": True}
    assert listing.record.payload["status"] == "listed"
    assert board.record.payload == {"board_type": "gem", "market": "创业板"}
    assert adjustment.record.payload == {
        "adj_factor": 2.0,
        "price_basis": "raw_session",
    }
    assert price_limit.record.payload == {
        "limit_down_price": 8.0,
        "limit_eligible": True,
        "limit_percent": 0.2,
        "limit_up_price": 12.0,
        "session_pre_close_raw": 10.0,
    }
    assert registry.current_manifest().published_at == AVAILABLE_AT
    assert all(
        record.first_available_at == AVAILABLE_AT
        for record in registry.records(
            dataset_id=ReferenceDataset.ST_STATUS,
            key="600000.SH",
        )
    )


def test_builds_complete_bounded_reference_serving_projection_contract(
    tmp_path: Path,
) -> None:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    snapshot = _snapshot()
    receipt = publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=snapshot,
        completion_clock=lambda: VISIBLE_AT,
    )

    result = build_reference_slow_serving_result(
        snapshot=snapshot,
        receipt=receipt,
    )

    assert isinstance(result.payload, ReferenceSlowPayload)
    assert result.payload.reference_generation_id == receipt.generation_id
    assert result.payload.revision == receipt.revision
    assert result.payload.available_at == receipt.available_at
    assert result.payload.price_basis == "raw_session"
    assert result.payload.adjustment_basis == "tushare_adj_factor"
    assert {projection.table_name for projection in result.payload.projections} == {
        "stock_basic",
        "risk_blacklist",
        "dc_board",
        "dc_board_member",
        "kpl_concept_member",
        "market_liquidity",
        "daily_bar",
        "trade_calendar",
        "nl_screen_universe",
    }
    stock = next(
        projection
        for projection in result.payload.projections
        if projection.table_name == "stock_basic"
    )
    assert {row["ts_code"] for row in stock.rows} == {"300001.SZ", "600000.SH"}
    assert all(
        projection.available_at <= receipt.available_at for projection in result.payload.projections
    )


def test_rejects_snapshot_discovered_before_the_publication_session(tmp_path: Path) -> None:
    original = _snapshot()
    previous_session = ReferenceSlowSourceSnapshot.create(
        target_trade_date=original.target_trade_date,
        captured_at=datetime(2026, 7, 30, 10, 30, tzinfo=UTC),
        producer_commit=original.producer_commit,
        source_snapshot_ids=original.source_snapshot_ids,
        daily_facts=original.daily_facts,
        security_facts=original.security_facts,
        suspended_codes=original.suspended_codes,
    )

    with pytest.raises(ReferenceSlowPublicationError, match="discovery session"):
        publish_reference_slow_snapshot(
            registry=ReferenceRegistry(tmp_path / "reference.sqlite3"),
            calendar=_calendar(),
            snapshot=previous_session,
            completion_clock=lambda: VISIBLE_AT,
        )


def test_daily_fact_requires_exact_prior_session_adjustment_factor() -> None:
    with pytest.raises(ValidationError, match="prior_adj_factor"):
        ReferenceDailyFact(
            ts_code="300001.SZ",
            trade_date=PRIOR_DATE,
            close_raw=20.0,
            adj_factor=2.0,
        )


def test_republishing_same_sealed_snapshot_is_idempotent(tmp_path: Path) -> None:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")

    first = publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=_snapshot(),
        completion_clock=lambda: CAPTURED_AT,
    )
    second = publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=_snapshot(),
        completion_clock=lambda: CAPTURED_AT,
    )

    assert second.inserted_record_count == 0
    assert second.generation_id == first.generation_id
    assert registry.current_manifest().row_count == 12


def test_full_market_publication_uses_two_connections_and_one_delta_membership(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    security_facts = tuple(
        ReferenceSecurityFact(
            ts_code=f"{300000 + index:06d}.SZ",
            name=f"规模样本{index}",
            list_date=date(2020, 1, 2),
            market="创业板",
        )
        for index in range(500)
    )
    snapshot = ReferenceSlowSourceSnapshot.create(
        target_trade_date=TARGET_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT,
        source_snapshot_ids={
            "daily": "4" * 64,
            "security": "5" * 64,
            "suspension": "6" * 64,
            "calendar": _calendar().content_sha256,
        },
        daily_facts=tuple(
            ReferenceDailyFact(
                ts_code=fact.ts_code,
                trade_date=PRIOR_DATE,
                close_raw=10.0,
                prior_adj_factor=1.0,
                adj_factor=1.0,
            )
            for fact in security_facts
        ),
        security_facts=security_facts,
    )
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    original_connect = registry._connect
    original_append_many = registry._append_many_in_connection
    connect_count = 0
    bulk_append_count = 0

    def counted_connect():
        nonlocal connect_count
        connect_count += 1
        return original_connect()

    def counted_append_many(connection, records, *, persist=True):
        nonlocal bulk_append_count
        bulk_append_count += 1
        return original_append_many(connection, records, persist=persist)

    monkeypatch.setattr(registry, "_connect", counted_connect)
    monkeypatch.setattr(registry, "_append_many_in_connection", counted_append_many)

    receipt = publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=snapshot,
        completion_clock=lambda: CAPTURED_AT,
    )

    assert receipt.inserted_record_count == 3_000
    assert connect_count == 2
    assert bulk_append_count == 1
    assert registry.current_manifest().added_record_ids


def test_later_correction_keeps_old_point_in_time_decision_visible(tmp_path: Path) -> None:
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")
    original = _snapshot()
    publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=original,
        completion_clock=lambda: original.captured_at,
    )
    corrected_daily = tuple(
        fact.model_copy(update={"close_raw": 21.0}) if fact.ts_code == "300001.SZ" else fact
        for fact in original.daily_facts
    )
    corrected = ReferenceSlowSourceSnapshot.create(
        target_trade_date=TARGET_DATE,
        captured_at=CAPTURED_AT + timedelta(minutes=2),
        producer_commit=COMMIT,
        source_snapshot_ids={**original.source_snapshot_ids, "daily": "9" * 64},
        daily_facts=corrected_daily,
        security_facts=original.security_facts,
        suspended_codes=original.suspended_codes,
    )

    receipt = publish_reference_slow_snapshot(
        registry=registry,
        calendar=_calendar(),
        snapshot=corrected,
        completion_clock=lambda: corrected.captured_at,
    )

    assert receipt.inserted_record_count == 1
    event_time = datetime(2026, 7, 31, 1, 25, tzinfo=UTC)
    old = registry.as_of(
        dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
        key="300001.SZ",
        event_time=event_time,
        decision_time=CAPTURED_AT + timedelta(minutes=1),
        generation_id=receipt.generation_id,
    )
    new = registry.as_of(
        dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
        key="300001.SZ",
        event_time=event_time,
        decision_time=corrected.captured_at + timedelta(seconds=5),
        generation_id=receipt.generation_id,
    )
    assert old.record.payload["limit_up_price"] == 12.0
    assert new.record.payload["limit_up_price"] == 12.6
    assert old.record.revision == 1
    assert new.record.revision == 2


def test_first_five_listing_sessions_are_not_limit_eligible(tmp_path: Path) -> None:
    calendar = _calendar()
    security = ReferenceSecurityFact(
        ts_code="301001.SZ",
        name="上市样本",
        list_date=date(2026, 7, 27),
        market="创业板",
    )
    snapshot = ReferenceSlowSourceSnapshot.create(
        target_trade_date=TARGET_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT,
        source_snapshot_ids={
            "daily": "1" * 64,
            "security": "2" * 64,
            "suspension": "3" * 64,
            "calendar": calendar.content_sha256,
        },
        daily_facts=(
            ReferenceDailyFact(
                ts_code=security.ts_code,
                trade_date=PRIOR_DATE,
                close_raw=10.0,
                prior_adj_factor=1.0,
                adj_factor=1.0,
            ),
        ),
        security_facts=(security,),
    )
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")

    receipt = publish_reference_slow_snapshot(
        registry=registry,
        calendar=calendar,
        snapshot=snapshot,
        completion_clock=lambda: snapshot.captured_at,
    )

    price_limit = registry.as_of(
        dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
        key=security.ts_code,
        event_time=datetime(2026, 7, 31, 1, 25, tzinfo=UTC),
        decision_time=CAPTURED_AT + timedelta(seconds=5),
        generation_id=receipt.generation_id,
    )
    assert price_limit.record.payload["limit_eligible"] is False


def test_rejects_snapshot_that_was_not_visible_by_target_decision(tmp_path: Path) -> None:
    original = _snapshot()
    late = ReferenceSlowSourceSnapshot.create(
        target_trade_date=original.target_trade_date,
        captured_at=datetime(2026, 7, 31, 2, 0, tzinfo=UTC),
        producer_commit=original.producer_commit,
        source_snapshot_ids=original.source_snapshot_ids,
        daily_facts=original.daily_facts,
        security_facts=original.security_facts,
        suspended_codes=original.suspended_codes,
    )

    with pytest.raises(ReferenceSlowPublicationError, match="after 09:25"):
        publish_reference_slow_snapshot(
            registry=ReferenceRegistry(tmp_path / "reference.sqlite3"),
            calendar=_calendar(),
            snapshot=late,
            completion_clock=lambda: late.captured_at,
        )


def test_rejects_mismatched_or_duplicate_source_universe() -> None:
    original = _snapshot()
    duplicate = original.daily_facts + (original.daily_facts[0],)

    with pytest.raises(ValueError, match="duplicate"):
        ReferenceSlowSourceSnapshot.create(
            target_trade_date=TARGET_DATE,
            captured_at=CAPTURED_AT,
            producer_commit=COMMIT,
            source_snapshot_ids=original.source_snapshot_ids,
            daily_facts=duplicate,
            security_facts=original.security_facts,
            suspended_codes=original.suspended_codes,
        )


def test_publish_boundary_revalidates_tampered_snapshot_hash(tmp_path: Path) -> None:
    original = _snapshot()
    tampered_daily = tuple(
        fact.model_copy(update={"close_raw": 99.0}) if fact.ts_code == "300001.SZ" else fact
        for fact in original.daily_facts
    )
    tampered = original.model_copy(update={"daily_facts": tampered_daily})

    with pytest.raises(ValueError, match="content_sha256"):
        publish_reference_slow_snapshot(
            registry=ReferenceRegistry(tmp_path / "reference.sqlite3"),
            calendar=_calendar(),
            snapshot=tampered,
            completion_clock=lambda: tampered.captured_at,
        )


def test_price_limits_use_target_session_adjusted_reference_close(tmp_path: Path) -> None:
    calendar = _calendar()
    code = "600001.SH"
    snapshot = ReferenceSlowSourceSnapshot.create(
        target_trade_date=TARGET_DATE,
        captured_at=CAPTURED_AT,
        producer_commit=COMMIT,
        source_snapshot_ids={
            "daily": "1" * 64,
            "security": "2" * 64,
            "suspension": "3" * 64,
            "calendar": calendar.content_sha256,
        },
        daily_facts=(
            ReferenceDailyFact(
                ts_code=code,
                trade_date=PRIOR_DATE,
                close_raw=10.0,
                prior_adj_factor=1.0,
                adj_factor=2.0,
            ),
        ),
        security_facts=(
            ReferenceSecurityFact(
                ts_code=code,
                name="除权样本",
                list_date=date(2000, 1, 1),
                market="主板",
            ),
        ),
    )
    registry = ReferenceRegistry(tmp_path / "reference.sqlite3")

    receipt = publish_reference_slow_snapshot(
        registry=registry,
        calendar=calendar,
        snapshot=snapshot,
        completion_clock=lambda: snapshot.captured_at,
    )
    price_limit = registry.as_of(
        dataset_id=ReferenceDataset.PRICE_LIMIT_REGIME,
        key=code,
        event_time=datetime(2026, 7, 31, 1, 25, tzinfo=UTC),
        decision_time=CAPTURED_AT + timedelta(seconds=5),
        generation_id=receipt.generation_id,
    )

    assert price_limit.record.payload == {
        "limit_down_price": 4.5,
        "limit_eligible": True,
        "limit_percent": 0.1,
        "limit_up_price": 5.5,
        "session_pre_close_raw": 5.0,
    }
