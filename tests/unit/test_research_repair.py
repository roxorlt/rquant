"""Governed historical auction repair tests."""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.research_repair import (
    ResearchAuctionRepairDayPlan,
    ResearchAuctionRepairPlan,
    assess_tushare_auction_rows,
    build_auction_repair_day_plan,
    hash_auction_business_rows,
    hash_code_universe,
    merge_auction_partition,
    normalize_repair_dates,
    normalize_tushare_auction_rows,
)

_CST = ZoneInfo("Asia/Shanghai")
_COMMIT = "a" * 40
_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_TRADE_DATE = date(2026, 7, 14)
_GENERATED_AT = datetime(2026, 7, 18, 15, 20, tzinfo=_CST)


def _auction_frame(
    codes: list[str],
    *,
    trade_date: date = _TRADE_DATE,
    source: str = "tushare",
    auction_type: str = "open_realtime",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_date": trade_date,
                "auction_type": auction_type,
                "price": 10.0 + index / 100,
                "vol": 1_000.0 + index,
                "amount": 10_000.0 + index,
                "turnover_rate": 0.1,
                "volume_ratio": 1.5,
                "source": source,
            }
            for index, code in enumerate(codes)
        ]
    )


def _codes(count: int, *, prefix: str = "0") -> list[str]:
    return [f"{prefix}{index:05d}.SZ" for index in range(count)]


def _day_plan(trade_date: date) -> ResearchAuctionRepairDayPlan:
    return ResearchAuctionRepairDayPlan(
        trade_date=trade_date,
        expected_code_count=100,
        expected_codes_sha256=_HASH_A,
        existing_manifest_sha256=_HASH_B,
        fetched_business_sha256=_HASH_C,
        merged_business_sha256="d" * 64,
        existing_row_count=90,
        fetched_row_count=100,
        merged_row_count=100,
        observed_code_count=100,
        valid_code_count=100,
        expected_valid_code_count=100,
        expected_observed_code_count=100,
        unexpected_code_count=0,
        changed=True,
    )


def test_normalize_repair_dates_sorts_deduplicates_and_rejects_empty() -> None:
    assert normalize_repair_dates(
        [
            date(2026, 7, 14),
            date(2026, 4, 20),
            date(2026, 7, 14),
        ]
    ) == (date(2026, 4, 20), date(2026, 7, 14))

    with pytest.raises(ValueError, match="at least one"):
        normalize_repair_dates([])


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda frame: frame.drop(columns=["vol"]), "missing columns"),
        (
            lambda frame: frame.assign(trade_date=date(2026, 7, 15)),
            "outside target date",
        ),
        (lambda frame: frame.assign(source="minute_0930_fallback"), "source"),
        (lambda frame: frame.assign(auction_type="close"), "auction type"),
        (
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "duplicate physical key",
        ),
    ],
)
def test_normalize_tushare_auction_rows_rejects_structural_corruption(
    mutate: object,
    message: str,
) -> None:
    frame = _auction_frame(["000001.SZ", "600000.SH"])

    with pytest.raises(ValueError, match=message):
        normalize_tushare_auction_rows(
            mutate(frame),  # type: ignore[operator]
            trade_date=_TRADE_DATE,
            generated_at=_GENERATED_AT,
        )


def test_normalize_tushare_auction_rows_rejects_empty_and_na_keys() -> None:
    with pytest.raises(ValueError, match="empty"):
        normalize_tushare_auction_rows(
            pd.DataFrame(),
            trade_date=_TRADE_DATE,
            generated_at=_GENERATED_AT,
        )

    frame = _auction_frame(["000001.SZ"])
    frame.loc[0, "ts_code"] = None
    with pytest.raises(ValueError, match="null physical key"):
        normalize_tushare_auction_rows(
            frame,
            trade_date=_TRADE_DATE,
            generated_at=_GENERATED_AT,
        )


def test_normalized_rows_use_real_repair_time_and_stable_sorting() -> None:
    frame = _auction_frame(["600000.SH", "000001.SZ"])

    normalized = normalize_tushare_auction_rows(
        frame,
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    assert normalized["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    assert normalized["created_at"].tolist() == [
        _GENERATED_AT.replace(tzinfo=None),
        _GENERATED_AT.replace(tzinfo=None),
    ]


def test_tushare_quality_accepts_exact_98_percent_boundary() -> None:
    expected = set(_codes(100))
    observed = _codes(98) + _codes(2, prefix="9")
    normalized = normalize_tushare_auction_rows(
        _auction_frame(observed),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    audit = assess_tushare_auction_rows(normalized, expected_codes=expected)

    assert audit.passed is True
    assert audit.expected_code_count == 100
    assert audit.observed_code_count == 100
    assert audit.valid_code_count == 100
    assert audit.expected_valid_code_count == 98
    assert audit.expected_observed_code_count == 98
    assert audit.unexpected_code_count == 2
    assert audit.issues == ()


def test_tushare_quality_rejects_97_percent_and_nonpositive_values() -> None:
    expected = set(_codes(100))
    observed = _codes(97) + _codes(3, prefix="9")
    frame = _auction_frame(observed)
    frame.loc[0, "price"] = 0
    frame.loc[1, "vol"] = -1
    normalized = normalize_tushare_auction_rows(
        frame,
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    audit = assess_tushare_auction_rows(normalized, expected_codes=expected)

    assert audit.passed is False
    assert audit.valid_code_count == 98
    assert audit.expected_valid_code_count == 95
    assert set(audit.issues) == {
        "tushare_valid_coverage_below_98pct",
        "tushare_observed_precision_below_98pct",
    }


def test_tushare_quality_rejects_empty_expected_universe() -> None:
    normalized = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ"]),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    with pytest.raises(ValueError, match="expected universe"):
        assess_tushare_auction_rows(normalized, expected_codes=set())


def test_business_hash_excludes_created_at_but_binds_business_values() -> None:
    first = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ", "600000.SH"]),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )
    second = first.iloc[::-1].copy()
    second["created_at"] = datetime(2026, 7, 18, 16, 0)

    assert hash_auction_business_rows(first) == hash_auction_business_rows(second)

    second.loc[second["ts_code"] == "000001.SZ", "price"] = 10.5
    assert hash_auction_business_rows(first) != hash_auction_business_rows(second)


def test_universe_hash_is_order_independent_and_binds_membership() -> None:
    assert hash_code_universe({"600000.SH", "000001.SZ"}) == hash_code_universe(
        {"000001.SZ", "600000.SH"}
    )
    assert hash_code_universe({"000001.SZ"}) != hash_code_universe(
        {"000001.SZ", "600000.SH"}
    )


def test_plan_id_is_canonical_and_binds_authority_and_day_content() -> None:
    days = (_day_plan(date(2026, 4, 20)), _day_plan(date(2026, 7, 14)))
    first = ResearchAuctionRepairPlan(
        code_commit=_COMMIT,
        authority_current_sha256=_HASH_A,
        catalog_sha256=_HASH_B,
        readonly_catalog_sha256=_HASH_C,
        days=days,
    )
    second = ResearchAuctionRepairPlan.model_validate(first.model_dump())

    assert first.plan_id == second.plan_id
    assert len(first.plan_id) == 64
    assert first.trade_dates == (date(2026, 4, 20), date(2026, 7, 14))

    changed = ResearchAuctionRepairPlan(
        code_commit=_COMMIT,
        authority_current_sha256="f" * 64,
        catalog_sha256=_HASH_B,
        readonly_catalog_sha256=_HASH_C,
        days=days,
    )
    assert changed.plan_id != first.plan_id


def test_plan_rejects_unsorted_or_duplicate_days() -> None:
    with pytest.raises(ValueError, match="strictly ordered"):
        ResearchAuctionRepairPlan(
            code_commit=_COMMIT,
            authority_current_sha256=_HASH_A,
            catalog_sha256=_HASH_B,
            readonly_catalog_sha256=_HASH_C,
            days=(
                _day_plan(date(2026, 7, 14)),
                _day_plan(date(2026, 4, 20)),
            ),
        )

    with pytest.raises(ValueError, match="strictly ordered"):
        ResearchAuctionRepairPlan(
            code_commit=_COMMIT,
            authority_current_sha256=_HASH_A,
            catalog_sha256=_HASH_B,
            readonly_catalog_sha256=_HASH_C,
            days=(
                _day_plan(date(2026, 7, 14)),
                _day_plan(date(2026, 7, 14)),
            ),
        )


def test_merge_preserves_fallback_and_unchanged_tushare_created_at() -> None:
    old_time = datetime(2026, 7, 14, 9, 26)
    fallback_time = datetime(2026, 7, 14, 9, 31)
    existing = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ", "000002.SZ", "000003.SZ"]),
        trade_date=_TRADE_DATE,
        generated_at=old_time.replace(tzinfo=_CST),
    )
    fallback = existing.iloc[[0]].copy()
    fallback["ts_code"] = "000004.SZ"
    fallback["source"] = "minute_0930_fallback"
    fallback["created_at"] = fallback_time
    existing = pd.concat([existing, fallback], ignore_index=True)

    fetched_frame = _auction_frame(["000001.SZ", "000002.SZ", "000005.SZ"])
    fetched_frame.loc[fetched_frame["ts_code"] == "000002.SZ", "price"] = 88.0
    fetched = normalize_tushare_auction_rows(
        fetched_frame,
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    merged = merge_auction_partition(existing, fetched, trade_date=_TRADE_DATE)

    keyed = merged.set_index(["ts_code", "source"])
    assert keyed.loc[("000001.SZ", "tushare"), "created_at"] == old_time
    assert keyed.loc[("000002.SZ", "tushare"), "created_at"] == _GENERATED_AT.replace(
        tzinfo=None
    )
    assert keyed.loc[("000002.SZ", "tushare"), "price"] == 88.0
    assert keyed.loc[("000003.SZ", "tushare"), "created_at"] == old_time
    assert keyed.loc[("000004.SZ", "minute_0930_fallback"), "created_at"] == fallback_time
    assert keyed.loc[("000005.SZ", "tushare"), "created_at"] == _GENERATED_AT.replace(
        tzinfo=None
    )
    assert not merged.duplicated(
        ["ts_code", "trade_date", "auction_type", "source"]
    ).any()
    assert merged[["ts_code", "source"]].values.tolist() == sorted(
        merged[["ts_code", "source"]].values.tolist()
    )


def test_merge_rejects_existing_rows_from_another_date_or_duplicate_key() -> None:
    existing = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ"]),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )
    fetched = existing.copy()

    wrong_date = existing.copy()
    wrong_date["trade_date"] = date(2026, 7, 15)
    with pytest.raises(ValueError, match="outside target date"):
        merge_auction_partition(wrong_date, fetched, trade_date=_TRADE_DATE)

    duplicate = pd.concat([existing, existing], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate physical key"):
        merge_auction_partition(duplicate, fetched, trade_date=_TRADE_DATE)


def test_build_day_plan_reports_unchanged_business_content() -> None:
    existing = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ", "600000.SH"]),
        trade_date=_TRADE_DATE,
        generated_at=datetime(2026, 7, 14, 9, 26, tzinfo=_CST),
    )
    fetched = normalize_tushare_auction_rows(
        existing.drop(columns=["created_at"]).iloc[::-1],
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    day, merged = build_auction_repair_day_plan(
        trade_date=_TRADE_DATE,
        expected_codes={"000001.SZ", "600000.SH"},
        existing_manifest_sha256=_HASH_A,
        existing=existing,
        fetched=fetched,
    )

    assert day.changed is False
    assert day.existing_row_count == 2
    assert day.fetched_row_count == 2
    assert day.merged_row_count == 2
    assert day.fetched_business_sha256 == day.merged_business_sha256
    assert hash_auction_business_rows(existing) == hash_auction_business_rows(merged)
    assert merged["created_at"].tolist() == existing["created_at"].tolist()


def test_build_day_plan_rejects_quality_failure() -> None:
    expected = set(_codes(100))
    fetched = normalize_tushare_auction_rows(
        _auction_frame(_codes(97) + _codes(3, prefix="9")),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    with pytest.raises(ValueError, match="quality gate"):
        build_auction_repair_day_plan(
            trade_date=_TRADE_DATE,
            expected_codes=expected,
            existing_manifest_sha256=None,
            existing=pd.DataFrame(),
            fetched=fetched,
        )
