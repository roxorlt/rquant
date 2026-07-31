from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest
from pydantic import ValidationError

from rquant.auction_gap_strategy import AuctionGapConfig
from rquant.live_contracts import BatchQualityStatus
from rquant.strategy_candidate_producers import (
    AUCTION_GAP_REQUIRED_LINEAGE_KEYS,
    DAILY_VOLUME_LOT_SIZE_SHARES,
    GROWTH_BOARD_REQUIRED_LINEAGE_KEYS,
    N_SHAPE_REQUIRED_LINEAGE_KEYS,
    AuctionMatchFact,
    CandidateAuthorityQualityError,
    CandidateInputConflictError,
    CandidatePointInTimeError,
    GrowthBoardFact,
    NShapePoolFact,
    PriorDailyVolumeFact,
    PublishedCandidateInputAuthority,
    produce_auction_gap_candidates,
    produce_growth_board_surge_candidates,
    produce_n_shape_candidates,
)
from rquant.strategy_candidate_snapshot import (
    StrategyCandidatePriceBasis,
    StrategyCandidateRecord,
)
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry

TRADE_DATE = date(2026, 7, 31)
REFERENCE_DATE = date(2026, 7, 30)
CAPTURED_AT = datetime(2026, 7, 31, 1, 30, tzinfo=UTC)
AUCTION_AVAILABLE_AT = datetime(2026, 7, 31, 1, 26, tzinfo=UTC)
SESSION_AVAILABLE_AT = AUCTION_AVAILABLE_AT
COMMIT = "a" * 40
AUTHORITY_SNAPSHOT = "1" * 64
POOL_SNAPSHOT = "2" * 64
SESSION_SNAPSHOT = "3" * 64
STATUS_SNAPSHOT = "4" * 64
LIMIT_SNAPSHOT = "5" * 64
AUCTION_SNAPSHOT = "6" * 64
DAILY_SNAPSHOTS = tuple(str(index) * 64 for index in range(7, 10)) + (
    "b" * 64,
    "c" * 64,
)
CALENDAR_SNAPSHOT = "f" * 64
ADJ_FACTOR_SNAPSHOT = "a" * 64
DAILY_SNAPSHOT = "d" * 64
MONEYFLOW_SNAPSHOT = "e" * 64
PRIOR5_TRADE_DATES = (
    date(2026, 7, 24),
    date(2026, 7, 27),
    date(2026, 7, 28),
    date(2026, 7, 29),
    REFERENCE_DATE,
)
N_LINEAGE = {
    "pool": POOL_SNAPSHOT,
    "daily": DAILY_SNAPSHOT,
    "adj_factor": ADJ_FACTOR_SNAPSHOT,
    "session": SESSION_SNAPSHOT,
    "status": STATUS_SNAPSHOT,
    "limit": LIMIT_SNAPSHOT,
    "trade_calendar": CALENDAR_SNAPSHOT,
}
GROWTH_LINEAGE = {
    "daily": DAILY_SNAPSHOT,
    "moneyflow_t1": MONEYFLOW_SNAPSHOT,
    "session": SESSION_SNAPSHOT,
    "status": STATUS_SNAPSHOT,
    "limit": LIMIT_SNAPSHOT,
    "trade_calendar": CALENDAR_SNAPSHOT,
}
AUCTION_LINEAGE = {
    "session": SESSION_SNAPSHOT,
    "status": STATUS_SNAPSHOT,
    "limit": LIMIT_SNAPSHOT,
}


def _authority(
    *,
    quality_status: BatchQualityStatus = BatchQualityStatus.PUBLISHED,
    captured_at: datetime = CAPTURED_AT,
) -> PublishedCandidateInputAuthority:
    return PublishedCandidateInputAuthority(
        trade_date=TRADE_DATE,
        captured_at=captured_at,
        quality_status=quality_status,
        authority_snapshot_id=AUTHORITY_SNAPSHOT,
        producer_commit=COMMIT,
    )


def _n_fact(
    ts_code: str = "300001.SZ",
    **changes: object,
) -> NShapePoolFact:
    values: dict[str, object] = {
        "ts_code": ts_code,
        "variant": "pool1",
        "reference_trade_date": date(2026, 7, 29),
        "t_close_raw": 20.0,
        "t_high_raw": 25.0,
        "reference_adj_factor": 1.0,
        "prior_session_trade_date": REFERENCE_DATE,
        "expected_prior_session_trade_date": REFERENCE_DATE,
        "prior_session_close_raw": 10.0,
        "prior_session_adj_factor": 2.0,
        "available_at": SESSION_AVAILABLE_AT,
        "reference_snapshot_ids": N_LINEAGE,
        "session_pre_close_raw": 8.0,
        "limit_pct": 0.2,
        "limit_up_price_session_raw": 9.6,
        "is_st": False,
        "is_suspended": False,
        "is_listed": True,
        "limit_eligible": True,
    }
    values.update(changes)
    return NShapePoolFact(**values)


def _growth_fact(
    ts_code: str = "300001.SZ",
    **changes: object,
) -> GrowthBoardFact:
    values: dict[str, object] = {
        "ts_code": ts_code,
        "board_type": "gem",
        "reference_trade_date": REFERENCE_DATE,
        "prior_session_trade_date": REFERENCE_DATE,
        "expected_prior_session_trade_date": REFERENCE_DATE,
        "ma5": 14.0,
        "ma10": 13.0,
        "ma20": 12.0,
        "ma60": 11.0,
        "large_net_vol_t1": 1_000_000.0,
        "historical_sessions": 5,
        "available_at": SESSION_AVAILABLE_AT,
        "reference_snapshot_ids": GROWTH_LINEAGE,
        "session_pre_close_raw": 10.0,
        "limit_pct": 0.2,
        "limit_up_price_session_raw": 12.0,
        "is_st": False,
        "is_suspended": False,
        "is_listed": True,
        "limit_eligible": True,
    }
    values.update(changes)
    return GrowthBoardFact(**values)


def _daily_volumes(
    *,
    volumes_lots: tuple[float, ...] = (1_000.0,) * 5,
) -> tuple[PriorDailyVolumeFact, ...]:
    return tuple(
        PriorDailyVolumeFact(
            trade_date=trade_date,
            daily_volume_lots=volume_lots,
            available_at=datetime.combine(
                trade_date,
                datetime.min.time(),
                tzinfo=UTC,
            )
            + timedelta(hours=8),
            source_snapshot_id=snapshot_id,
        )
        for trade_date, volume_lots, snapshot_id in zip(
            PRIOR5_TRADE_DATES,
            volumes_lots,
            DAILY_SNAPSHOTS,
            strict=True,
        )
    )


def _auction_fact(
    ts_code: str = "300001.SZ",
    **changes: object,
) -> AuctionMatchFact:
    values: dict[str, object] = {
        "ts_code": ts_code,
        "trade_date": TRADE_DATE,
        "auction_price_raw": 10.5,
        "auction_vol_shares": 20_000.0,
        "session_pre_close_raw": 10.0,
        "limit_pct": 0.2,
        "limit_up_price_session_raw": 12.0,
        "is_st": False,
        "is_suspended": False,
        "is_listed": True,
        "limit_eligible": True,
        "available_at": AUCTION_AVAILABLE_AT,
        "source_snapshot_id": AUCTION_SNAPSHOT,
        "source_volume_ratio": 999.0,
        "expected_prior5_trade_dates": PRIOR5_TRADE_DATES,
        "calendar_available_at": AUCTION_AVAILABLE_AT,
        "calendar_snapshot_id": CALENDAR_SNAPSHOT,
        "reference_snapshot_ids": AUCTION_LINEAGE,
        "prior5_daily_volumes": _daily_volumes(),
    }
    values.update(changes)
    return AuctionMatchFact(**values)


def _assert_schema_exact(record: StrategyCandidateRecord) -> None:
    registry = BuiltinStrategyEvaluatorRegistry(producer_commit=COMMIT)
    definition = registry.load_definition(record.strategy_id, int(record.strategy_version))
    assert set(record.static_features) == set(definition.static_feature_schema)


def test_n_shape_rebases_cross_adjustment_prices_and_emits_exact_contract() -> None:
    records = produce_n_shape_candidates(
        authority=_authority(),
        facts=(_n_fact(),),
    )

    assert len(records) == 1
    record = records[0]
    assert isinstance(record, StrategyCandidateRecord)
    assert record.strategy_id == "n_shape"
    assert record.strategy_version == "1"
    assert record.candidate_id == "300001.SZ"
    assert record.variant == "pool1"
    assert record.price_basis is StrategyCandidatePriceBasis.RAW
    assert record.effective_trade_date == TRADE_DATE
    assert record.reference_trade_date == date(2026, 7, 29)
    assert record.decision_at == CAPTURED_AT
    assert record.available_at == CAPTURED_AT
    assert dict(record.static_features) == {
        "candidate_price_basis": "raw_session",
        "limit_pct": 0.2,
        "limit_up_price_session_raw": 9.6,
        "t_close_session_raw": pytest.approx(8.0),
        "t_high_session_raw": pytest.approx(10.0),
    }
    _assert_schema_exact(record)
    StrategyCandidateRecord.model_validate(record.model_dump(mode="python"))


def test_n_shape_pool2_wins_regardless_of_input_order_and_output_is_canonical() -> None:
    pool1 = _n_fact("300002.SZ", variant="pool1", t_high_raw=24.0)
    pool2 = _n_fact("300002.SZ", variant="pool2", t_high_raw=25.0)
    first = _n_fact("000001.SZ", variant="pool1")

    records = produce_n_shape_candidates(
        authority=_authority(),
        facts=(pool1, pool2, first),
    )
    repeated = produce_n_shape_candidates(
        authority=_authority(),
        facts=(first, pool2, pool1),
    )

    assert tuple(record.candidate_id for record in records) == (
        "000001.SZ",
        "300002.SZ",
    )
    assert records[1].variant == "pool2"
    assert records == repeated


def test_n_shape_accepts_a_pool2_only_membership() -> None:
    records = produce_n_shape_candidates(
        authority=_authority(),
        facts=(_n_fact(variant="pool2"),),
    )

    assert len(records) == 1
    assert records[0].variant == "pool2"


def test_n_shape_same_priority_conflict_fails_closed() -> None:
    with pytest.raises(CandidateInputConflictError, match="300001.SZ.*pool1"):
        produce_n_shape_candidates(
            authority=_authority(),
            facts=(_n_fact(t_high_raw=24.0), _n_fact(t_high_raw=25.0)),
        )


def test_n_shape_rejects_a_stale_prior_session_against_authoritative_calendar() -> None:
    with pytest.raises(ValidationError, match="authoritative prior session"):
        _n_fact(prior_session_trade_date=date(2026, 7, 29))


@pytest.mark.parametrize(
    ("changes", "match"),
    [
        ({"t_high_raw": 19.0}, ""),
        ({"t_close_raw": 0.0}, ""),
        ({"t_high_raw": -1.0}, ""),
        ({"reference_adj_factor": 0.0}, ""),
        ({"prior_session_adj_factor": -1.0}, ""),
        ({"prior_session_close_raw": 0.0}, ""),
    ],
)
def test_n_shape_invalid_static_geometry_is_a_published_empty_result(
    changes: dict[str, object],
    match: str,
) -> None:
    assert (
        produce_n_shape_candidates(
            authority=_authority(),
            facts=(_n_fact(**changes),),
        )
        == ()
    )


def test_growth_board_emits_exact_static_contract_and_canonical_sort() -> None:
    records = produce_growth_board_surge_candidates(
        authority=_authority(),
        facts=(_growth_fact("688001.SH", board_type="star"), _growth_fact("300001.SZ")),
    )

    assert tuple(record.candidate_id for record in records) == (
        "300001.SZ",
        "688001.SH",
    )
    assert records[0].variant == "gem"
    assert dict(records[0].static_features) == {
        "board_type": "gem",
        "candidate_price_basis": "raw_session",
        "large_net_vol_t1": 1_000_000.0,
        "limit_up_price_session_raw": 12.0,
        "ma_alignment": True,
        "session_pre_close_raw": 10.0,
    }
    _assert_schema_exact(records[0])


@pytest.mark.parametrize(
    "changes",
    [
        {"ma5": 13.0, "ma10": 13.0},
        {"ma10": 12.0, "ma20": 12.0},
        {"ma20": 11.0, "ma60": 11.0},
        {"large_net_vol_t1": 0.0},
        {"large_net_vol_t1": -1.0},
        {"historical_sessions": 4},
    ],
)
def test_growth_board_static_filters_return_published_empty(
    changes: dict[str, object],
) -> None:
    assert (
        produce_growth_board_surge_candidates(
            authority=_authority(),
            facts=(_growth_fact(**changes),),
        )
        == ()
    )


def test_growth_board_only_accepts_gem_or_star() -> None:
    with pytest.raises(ValidationError, match="board_type"):
        _growth_fact(board_type="main")


@pytest.mark.parametrize(
    ("ts_code", "board_type"),
    [
        ("600001.SH", "gem"),
        ("300001.SZ", "star"),
        ("688001.SH", "gem"),
        ("830001.BJ", "gem"),
        ("830001.BJ", "star"),
    ],
)
def test_growth_board_rejects_board_type_disguises(
    ts_code: str,
    board_type: str,
) -> None:
    with pytest.raises(ValidationError, match="board_type.*ts_code"):
        _growth_fact(ts_code, board_type=board_type)


def test_growth_board_rejects_stale_moneyflow_masquerading_as_t_minus_1() -> None:
    stale_date = date(2026, 7, 29)

    with pytest.raises(ValidationError, match="authoritative prior session"):
        _growth_fact(
            reference_trade_date=stale_date,
            prior_session_trade_date=stale_date,
        )


@pytest.mark.parametrize(
    ("auction_vol_shares", "expected_ratio", "expected_count"),
    [
        (15_000.0, 0.15, 1),
        (14_999.0, None, 0),
        (500_000.0, 5.0, 1),
        (500_001.0, None, 0),
    ],
)
def test_auction_gap_converts_daily_lots_to_shares_for_ratio_bounds(
    auction_vol_shares: float,
    expected_ratio: float | None,
    expected_count: int,
) -> None:
    records = produce_auction_gap_candidates(
        authority=_authority(),
        facts=(
            _auction_fact(
                auction_vol_shares=auction_vol_shares,
                source_volume_ratio=999.0,
            ),
        ),
    )

    assert len(records) == expected_count
    if expected_ratio is not None:
        assert records[0].static_features["auction_vol_ratio_5d"] == pytest.approx(expected_ratio)
        assert records[0].static_features["auction_vol_ratio_5d"] != 999.0


def test_daily_lot_size_matches_existing_auction_gap_strategy_contract() -> None:
    config = AuctionGapConfig(start_date="20260701", end_date="20260731")

    assert DAILY_VOLUME_LOT_SIZE_SHARES == config.daily_vol_unit_factor == 100.0


def test_auction_gap_emits_exact_contract_and_computes_gap_from_raw_pre_close() -> None:
    records = produce_auction_gap_candidates(
        authority=_authority(),
        facts=(
            _auction_fact("688001.SH", auction_price_raw=11.0),
            _auction_fact("300001.SZ", auction_price_raw=10.5),
        ),
    )

    assert tuple(record.candidate_id for record in records) == (
        "300001.SZ",
        "688001.SH",
    )
    record = records[0]
    assert record.strategy_id == "auction_gap"
    assert record.strategy_version == "1"
    assert record.variant == "auction_gap"
    assert record.reference_trade_date == TRADE_DATE
    assert record.decision_at == CAPTURED_AT
    assert record.available_at == CAPTURED_AT
    assert dict(record.static_features) == {
        "auction_price_raw": 10.5,
        "auction_vol_ratio_5d": 0.2,
        "candidate_price_basis": "raw_session",
        "gap_pct_close": pytest.approx(5.0),
        "limit_up_price_session_raw": 12.0,
    }
    _assert_schema_exact(record)


@pytest.mark.parametrize(
    "changes",
    [
        {"auction_price_raw": 10.0},
        {"auction_price_raw": 9.99},
        {"auction_price_raw": 12.0},
        {"auction_price_raw": 12.01},
    ],
)
def test_auction_gap_requires_positive_gap_and_price_strictly_below_limit(
    changes: dict[str, object],
) -> None:
    assert (
        produce_auction_gap_candidates(
            authority=_authority(),
            facts=(_auction_fact(**changes),),
        )
        == ()
    )


def test_auction_gap_requires_exactly_five_strictly_increasing_completed_sessions() -> None:
    with pytest.raises(ValidationError, match="exactly five"):
        _auction_fact(prior5_daily_volumes=_daily_volumes()[:-1])

    unordered = list(_daily_volumes())
    unordered[1], unordered[2] = unordered[2], unordered[1]
    with pytest.raises(ValidationError, match="strictly increasing"):
        _auction_fact(prior5_daily_volumes=tuple(unordered))

    current = PriorDailyVolumeFact(
        trade_date=TRADE_DATE,
        daily_volume_lots=1_000.0,
        available_at=AUCTION_AVAILABLE_AT,
        source_snapshot_id="d" * 64,
    )
    with pytest.raises(ValidationError, match="completed"):
        _auction_fact(prior5_daily_volumes=(*_daily_volumes()[:-1], current))


def test_auction_gap_rejects_five_rows_that_skip_an_authoritative_recent_session() -> None:
    skipped_recent = (
        PriorDailyVolumeFact(
            trade_date=date(2026, 7, 23),
            daily_volume_lots=1_000.0,
            available_at=datetime(2026, 7, 23, 8, 0, tzinfo=UTC),
            source_snapshot_id="a" * 64,
        ),
        *_daily_volumes()[:-1],
    )

    with pytest.raises(ValidationError, match="authoritative prior-five calendar"):
        _auction_fact(prior5_daily_volumes=skipped_recent)


def test_auction_gap_zero_prior_average_is_published_empty() -> None:
    assert (
        produce_auction_gap_candidates(
            authority=_authority(),
            facts=(_auction_fact(prior5_daily_volumes=_daily_volumes(volumes_lots=(0.0,) * 5)),),
        )
        == ()
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("is_st", True),
        ("is_suspended", True),
        ("is_listed", False),
        ("limit_eligible", False),
    ],
)
@pytest.mark.parametrize("strategy", ["n", "growth", "auction"])
def test_status_and_limit_eligibility_fail_closed(
    strategy: str,
    field: str,
    value: bool,
) -> None:
    authority = _authority()
    if strategy == "n":
        result = produce_n_shape_candidates(
            authority=authority,
            facts=(_n_fact(**{field: value}),),
        )
    elif strategy == "growth":
        result = produce_growth_board_surge_candidates(
            authority=authority,
            facts=(_growth_fact(**{field: value}),),
        )
    else:
        result = produce_auction_gap_candidates(
            authority=authority,
            facts=(_auction_fact(**{field: value}),),
        )
    assert result == ()


@pytest.mark.parametrize(
    "quality_status",
    [
        BatchQualityStatus.DEGRADED,
        BatchQualityStatus.STALE,
        BatchQualityStatus.CANDIDATE,
        BatchQualityStatus.QUARANTINED,
    ],
)
@pytest.mark.parametrize("strategy", ["n", "growth", "auction"])
def test_nonpublished_authority_raises_instead_of_forging_empty(
    strategy: str,
    quality_status: BatchQualityStatus,
) -> None:
    authority = _authority(quality_status=quality_status)
    with pytest.raises(CandidateAuthorityQualityError, match=quality_status.value):
        if strategy == "n":
            produce_n_shape_candidates(authority=authority, facts=())
        elif strategy == "growth":
            produce_growth_board_surge_candidates(authority=authority, facts=())
        else:
            produce_auction_gap_candidates(authority=authority, facts=())


def test_available_at_cannot_exceed_authority_capture() -> None:
    future_available = CAPTURED_AT + timedelta(seconds=1)
    with pytest.raises(CandidatePointInTimeError, match="available_at.*captured_at"):
        produce_n_shape_candidates(
            authority=_authority(),
            facts=(_n_fact(available_at=future_available),),
        )
    with pytest.raises(CandidatePointInTimeError, match="available_at.*captured_at"):
        produce_growth_board_surge_candidates(
            authority=_authority(),
            facts=(_growth_fact(available_at=future_available),),
        )
    with pytest.raises(CandidatePointInTimeError, match="available_at.*captured_at"):
        produce_auction_gap_candidates(
            authority=_authority(),
            facts=(_auction_fact(available_at=future_available),),
        )


def test_reference_dates_cannot_reach_or_exceed_current_session() -> None:
    with pytest.raises(CandidatePointInTimeError, match="reference_trade_date"):
        produce_n_shape_candidates(
            authority=_authority(),
            facts=(
                _n_fact(
                    reference_trade_date=TRADE_DATE,
                    prior_session_trade_date=TRADE_DATE,
                    expected_prior_session_trade_date=TRADE_DATE,
                    available_at=AUCTION_AVAILABLE_AT,
                ),
            ),
        )
    with pytest.raises(CandidatePointInTimeError, match="reference_trade_date"):
        produce_growth_board_surge_candidates(
            authority=_authority(),
            facts=(
                _growth_fact(
                    reference_trade_date=TRADE_DATE,
                    prior_session_trade_date=TRADE_DATE,
                    expected_prior_session_trade_date=TRADE_DATE,
                    available_at=AUCTION_AVAILABLE_AT,
                ),
            ),
        )


@pytest.mark.parametrize("strategy", ["n", "growth", "auction"])
def test_authority_capture_is_included_in_latest_required_availability(
    strategy: str,
) -> None:
    authority = _authority()
    if strategy == "n":
        record = produce_n_shape_candidates(
            authority=authority,
            facts=(_n_fact(available_at=SESSION_AVAILABLE_AT),),
        )[0]
    elif strategy == "growth":
        record = produce_growth_board_surge_candidates(
            authority=authority,
            facts=(_growth_fact(available_at=SESSION_AVAILABLE_AT),),
        )[0]
    else:
        record = produce_auction_gap_candidates(
            authority=authority,
            facts=(_auction_fact(available_at=AUCTION_AVAILABLE_AT),),
        )[0]

    assert authority.captured_at > SESSION_AVAILABLE_AT
    assert record.decision_at == authority.captured_at
    assert record.available_at == authority.captured_at


def test_auction_decision_includes_prior_daily_availability_and_authority_capture() -> None:
    later_daily = list(_daily_volumes())
    later_daily[-1] = PriorDailyVolumeFact(
        trade_date=REFERENCE_DATE,
        daily_volume_lots=1_000.0,
        available_at=AUCTION_AVAILABLE_AT + timedelta(seconds=1),
        source_snapshot_id=DAILY_SNAPSHOTS[-1],
    )
    authority = _authority(captured_at=CAPTURED_AT + timedelta(seconds=2))
    record = produce_auction_gap_candidates(
        authority=authority,
        facts=(_auction_fact(prior5_daily_volumes=tuple(later_daily)),),
    )[0]

    assert record.decision_at == authority.captured_at
    assert record.available_at == record.decision_at


@pytest.mark.parametrize(
    ("factory", "changes"),
    [
        (_n_fact, {"t_close_raw": True}),
        (_n_fact, {"t_high_raw": float("nan")}),
        (_growth_fact, {"ma5": float("inf")}),
        (_growth_fact, {"historical_sessions": True}),
        (_auction_fact, {"auction_vol_shares": True}),
        (_auction_fact, {"auction_price_raw": float("-inf")}),
    ],
)
def test_numeric_contracts_reject_bool_nan_and_infinity(
    factory: object,
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        factory(**changes)  # type: ignore[operator]


@pytest.mark.parametrize("factory", [_n_fact, _growth_fact, _auction_fact])
def test_fact_contracts_reject_invalid_security_codes(factory: object) -> None:
    with pytest.raises(ValidationError, match="ts_code"):
        factory("300001")  # type: ignore[operator]


def test_reference_lineage_binds_every_authority_snapshot() -> None:
    n_lineage = dict(
        produce_n_shape_candidates(authority=_authority(), facts=(_n_fact(),))[
            0
        ].reference_snapshot_ids
    )
    growth_lineage = dict(
        produce_growth_board_surge_candidates(
            authority=_authority(),
            facts=(_growth_fact(),),
        )[0].reference_snapshot_ids
    )
    auction_lineage = dict(
        produce_auction_gap_candidates(
            authority=_authority(),
            facts=(_auction_fact(),),
        )[0].reference_snapshot_ids
    )

    assert n_lineage == {
        "adj_factor": ADJ_FACTOR_SNAPSHOT,
        "candidate_authority": AUTHORITY_SNAPSHOT,
        "daily": DAILY_SNAPSHOT,
        "limit": LIMIT_SNAPSHOT,
        "pool": POOL_SNAPSHOT,
        "session": SESSION_SNAPSHOT,
        "status": STATUS_SNAPSHOT,
        "trade_calendar": CALENDAR_SNAPSHOT,
    }
    assert growth_lineage == {
        "candidate_authority": AUTHORITY_SNAPSHOT,
        "daily": DAILY_SNAPSHOT,
        "limit": LIMIT_SNAPSHOT,
        "moneyflow_t1": MONEYFLOW_SNAPSHOT,
        "session": SESSION_SNAPSHOT,
        "status": STATUS_SNAPSHOT,
        "trade_calendar": CALENDAR_SNAPSHOT,
    }
    assert auction_lineage == {
        "auction_match": AUCTION_SNAPSHOT,
        "candidate_authority": AUTHORITY_SNAPSHOT,
        "daily_volume:2026-07-24": DAILY_SNAPSHOTS[0],
        "daily_volume:2026-07-27": DAILY_SNAPSHOTS[1],
        "daily_volume:2026-07-28": DAILY_SNAPSHOTS[2],
        "daily_volume:2026-07-29": DAILY_SNAPSHOTS[3],
        "daily_volume:2026-07-30": DAILY_SNAPSHOTS[4],
        "limit": LIMIT_SNAPSHOT,
        "session": SESSION_SNAPSHOT,
        "status": STATUS_SNAPSHOT,
        "trade_calendar": CALENDAR_SNAPSHOT,
    }


def test_strategy_lineage_constants_cover_every_required_field_authority() -> None:
    assert frozenset(N_LINEAGE) == N_SHAPE_REQUIRED_LINEAGE_KEYS
    assert frozenset(GROWTH_LINEAGE) == GROWTH_BOARD_REQUIRED_LINEAGE_KEYS
    assert frozenset(AUCTION_LINEAGE) == AUCTION_GAP_REQUIRED_LINEAGE_KEYS


@pytest.mark.parametrize(
    ("factory", "lineage", "required_key"),
    [
        *((_n_fact, N_LINEAGE, required_key) for required_key in sorted(N_LINEAGE)),
        *((_growth_fact, GROWTH_LINEAGE, required_key) for required_key in sorted(GROWTH_LINEAGE)),
        *(
            (_auction_fact, AUCTION_LINEAGE, required_key)
            for required_key in sorted(AUCTION_LINEAGE)
        ),
    ],
)
def test_strategy_facts_reject_each_missing_required_lineage_key(
    factory: object,
    lineage: dict[str, str],
    required_key: str,
) -> None:
    incomplete = dict(lineage)
    incomplete.pop(required_key)

    with pytest.raises(ValidationError, match=required_key):
        factory(reference_snapshot_ids=incomplete)  # type: ignore[operator]


def test_reference_snapshot_ids_are_sha256_and_immutable() -> None:
    with pytest.raises(ValidationError, match="reference_snapshot_ids"):
        _n_fact(reference_snapshot_ids={"pool": "bad"})
    with pytest.raises(ValidationError, match="source_snapshot_id"):
        _auction_fact(source_snapshot_id="bad")
    with pytest.raises(ValidationError, match="reference_snapshot_ids"):
        _auction_fact(reference_snapshot_ids={})

    references = dict(N_LINEAGE)
    fact = _n_fact(reference_snapshot_ids=references)
    references["pool"] = STATUS_SNAPSHOT
    assert fact.reference_snapshot_ids["pool"] == POOL_SNAPSHOT
    with pytest.raises(TypeError):
        fact.reference_snapshot_ids["pool"] = STATUS_SNAPSHOT  # type: ignore[index]


def test_shared_daily_snapshot_authority_is_supported() -> None:
    daily = tuple(
        PriorDailyVolumeFact(
            trade_date=item.trade_date,
            daily_volume_lots=item.daily_volume_lots,
            available_at=item.available_at,
        )
        for item in _daily_volumes()
    )
    fact = _auction_fact(
        prior5_daily_volumes=daily,
        daily_snapshot_id="e" * 64,
    )

    lineage = dict(
        produce_auction_gap_candidates(authority=_authority(), facts=(fact,))[
            0
        ].reference_snapshot_ids
    )
    assert lineage["daily_volume"] == "e" * 64
    assert not any(key.startswith("daily_volume:") for key in lineage)


def test_auction_deduplication_ignores_untrusted_source_volume_ratio() -> None:
    low_source_ratio = _auction_fact(source_volume_ratio=0.01)
    high_source_ratio = _auction_fact(source_volume_ratio=999.0)

    combined = produce_auction_gap_candidates(
        authority=_authority(),
        facts=(low_source_ratio, high_source_ratio),
    )
    expected = produce_auction_gap_candidates(
        authority=_authority(),
        facts=(low_source_ratio,),
    )

    assert combined == expected
    assert combined[0].static_features["auction_vol_ratio_5d"] == pytest.approx(0.2)


def test_duplicate_growth_and_auction_conflicts_fail_closed() -> None:
    with pytest.raises(CandidateInputConflictError, match="growth_board_surge"):
        produce_growth_board_surge_candidates(
            authority=_authority(),
            facts=(_growth_fact(ma5=14.0), _growth_fact(ma5=15.0)),
        )
    with pytest.raises(CandidateInputConflictError, match="auction_gap"):
        produce_auction_gap_candidates(
            authority=_authority(),
            facts=(
                _auction_fact(auction_vol_shares=20_000.0),
                _auction_fact(auction_vol_shares=21_000.0),
            ),
        )
