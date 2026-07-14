"""PIT-safe adjusted-price contract tests."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.storage.duckdb import DuckDBStore


@pytest.fixture()
def store(tmp_path: Path) -> DuckDBStore:
    result = DuckDBStore(tmp_path / "price-basis-pit.duckdb")
    yield result
    result.close()


def _daily_rows(
    ts_code: str = "600000.SH",
    *,
    periods: int = 3,
) -> pd.DataFrame:
    dates = [item.date() for item in pd.bdate_range("2026-06-01", periods=periods)]
    return pd.DataFrame([
        {
            "ts_code": ts_code,
            "trade_date": trade_date,
            "open": 10.0 + index,
            "high": 10.5 + index,
            "low": 9.5 + index,
            "close": 10.2 + index,
            "pre_close": 9.8 + index,
            "change": 0.4,
            "pct_chg": 4.08,
            "vol": 100.0 + index,
            "amount": 1000.0 + index * 100,
        }
        for index, trade_date in enumerate(dates)
    ])


def _factor_rows(
    values: list[float | None],
    ts_code: str = "600000.SH",
) -> pd.DataFrame:
    dates = [item.date() for item in pd.bdate_range("2026-06-01", periods=len(values))]
    return pd.DataFrame([
        {"ts_code": ts_code, "trade_date": trade_date, "adj_factor": value}
        for trade_date, value in zip(dates, values, strict=True)
    ])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("factor", 0.0),
        ("factor", -1.0),
        ("factor", float("nan")),
        ("factor", float("inf")),
        ("ratio", 0.0),
        ("ratio", -1.0),
        ("ratio", float("nan")),
        ("ratio", float("inf")),
    ],
)
def test_price_factor_ratio_requires_finite_positive_values(
    field: str,
    value: float,
) -> None:
    from rquant.price_adjustment import PriceFactorRatio

    payload = {
        "trade_date": date(2026, 6, 1),
        "factor": 1.0,
        "ratio": 1.0,
    }
    payload[field] = value

    with pytest.raises(ValidationError):
        PriceFactorRatio.model_validate(payload)


def test_available_price_factor_basis_enforces_consistent_state() -> None:
    from rquant.price_adjustment import PriceFactorBasis, PriceFactorRatio

    ratio = PriceFactorRatio(
        trade_date=date(2026, 6, 1),
        factor=1.0,
        ratio=1.0,
    )
    valid = PriceFactorBasis(
        available=True,
        reference_date=date(2026, 6, 1),
        reference_factor=1.0,
        ratios=(ratio,),
    )
    assert valid.available is True

    invalid_payloads = [
        {"reference_factor": None, "ratios": (ratio,)},
        {"reference_factor": 0.0, "ratios": (ratio,)},
        {"reference_factor": float("inf"), "ratios": (ratio,)},
        {"reference_factor": 1.0, "ratios": ()},
        {"reference_factor": 1.0, "ratios": (ratio, ratio)},
        {
            "reference_factor": 1.0,
            "ratios": (ratio,),
            "unavailable_reason": "missing_required_factor",
        },
        {
            "reference_factor": 1.0,
            "ratios": (ratio,),
            "unavailable_dates": (date(2026, 6, 1),),
        },
    ]
    for payload in invalid_payloads:
        with pytest.raises(ValidationError):
            PriceFactorBasis(
                available=True,
                reference_date=date(2026, 6, 1),
                **payload,
            )


def test_unavailable_price_factor_basis_enforces_consistent_state() -> None:
    from rquant.price_adjustment import PriceFactorBasis, PriceFactorRatio

    diagnostic = PriceFactorBasis(
        available=False,
        reference_date=date(2026, 6, 1),
        reference_factor=float("inf"),
        unavailable_reason="non_finite_reference_factor",
        unavailable_dates=(date(2026, 6, 1),),
    )
    assert diagnostic.reference_factor == float("inf")

    with pytest.raises(ValidationError):
        PriceFactorBasis(
            available=False,
            reference_date=date(2026, 6, 1),
        )
    with pytest.raises(ValidationError):
        PriceFactorBasis(
            available=False,
            reference_date=date(2026, 6, 1),
            unavailable_reason="missing_required_factor",
            ratios=(
                PriceFactorRatio(
                    trade_date=date(2026, 6, 1),
                    factor=1.0,
                    ratio=1.0,
                ),
            ),
        )


def test_resolve_price_factor_basis_rejects_empty_required_dates() -> None:
    from rquant.price_adjustment import resolve_price_factor_basis

    with pytest.raises(ValueError, match="required_dates"):
        resolve_price_factor_basis(
            required_dates=(),
            factor_by_date={date(2026, 6, 1): 1.0},
            reference_date=date(2026, 6, 1),
        )


def test_get_daily_qfq_end_anchor_ignores_future_factor(
    store: DuckDBStore,
) -> None:
    store.upsert_daily(_daily_rows(periods=4))
    store.upsert_adj_factor(_factor_rows([1.0, 1.0, 2.0, 99.0]))

    result = store.get_daily_qfq("600000.SH", end="2026-06-03")

    assert result["price_basis_available"].tolist() == [True, True, True]
    assert result["ref_factor"].tolist() == [2.0, 2.0, 2.0]
    assert result.iloc[0]["qfq_close"] == pytest.approx(5.1)


def test_get_daily_qfq_without_end_uses_explicit_latest_anchor(
    store: DuckDBStore,
) -> None:
    store.upsert_daily(_daily_rows(periods=4))
    store.upsert_adj_factor(_factor_rows([1.0, 1.0, 2.0, 4.0]))

    result = store.get_daily_qfq("600000.SH")

    assert result["price_basis_available"].all()
    assert result["ref_factor"].tolist() == [4.0] * 4
    assert result["ref_trade_date"].tolist() == ["2026-06-04"] * 4


def test_get_daily_qfq_validates_only_the_requested_date_range(
    store: DuckDBStore,
) -> None:
    store.upsert_daily(_daily_rows(periods=3))
    store.upsert_adj_factor(_factor_rows([1.0, 1.0, 2.0]).drop(index=0))

    result = store.get_daily_qfq(
        "600000.SH",
        start="2026-06-02",
        end="2026-06-03",
    )

    assert result["trade_date"].tolist() == ["2026-06-02", "2026-06-03"]
    assert result["price_basis_available"].all()
    assert result["ref_factor"].tolist() == [2.0, 2.0]


def test_get_daily_qfq_missing_reference_factor_is_explicitly_unavailable(
    store: DuckDBStore,
) -> None:
    store.upsert_daily(_daily_rows(periods=2))

    result = store.get_daily_qfq("600000.SH", end="2026-06-02")

    assert result["price_basis_available"].tolist() == [False, False]
    assert set(result["price_basis_reason"]) == {"missing_reference_factor"}
    assert result["qfq_close"].isna().all()


def test_get_daily_qfq_missing_intermediate_factor_invalidates_whole_range(
    store: DuckDBStore,
) -> None:
    store.upsert_daily(_daily_rows(periods=3))
    factors = _factor_rows([1.0, 1.0, 2.0]).drop(index=1)
    store.upsert_adj_factor(factors)

    result = store.get_daily_qfq("600000.SH", end="2026-06-03")

    assert result["price_basis_available"].tolist() == [False, False, False]
    assert set(result["price_basis_reason"]) == {"missing_required_factor"}
    assert result["qfq_open"].isna().all()


@pytest.mark.parametrize(
    ("values", "reason"),
    [
        ([1.0, 0.0, 2.0], "non_positive_required_factor"),
        ([1.0, 1.0, 0.0], "non_positive_reference_factor"),
    ],
)
def test_get_daily_qfq_non_positive_factor_is_unavailable(
    store: DuckDBStore,
    values: list[float],
    reason: str,
) -> None:
    store.upsert_daily(_daily_rows(periods=3))
    store.upsert_adj_factor(_factor_rows(values))

    result = store.get_daily_qfq("600000.SH", end="2026-06-03")

    assert not result["price_basis_available"].any()
    assert set(result["price_basis_reason"]) == {reason}
    assert result["qfq_close"].isna().all()


@pytest.mark.parametrize(
    ("factor", "at_reference", "expected_reasons"),
    [
        (float("nan"), False, {"missing_required_factor"}),
        (float("inf"), True, {"non_finite_reference_factor"}),
        (float("-inf"), False, {"non_finite_required_factor"}),
    ],
)
def test_get_daily_qfq_non_finite_factor_is_unavailable(
    store: DuckDBStore,
    factor: float,
    at_reference: bool,
    expected_reasons: set[str],
) -> None:
    store.upsert_daily(_daily_rows(periods=3))
    values = [1.0, 1.0, factor] if at_reference else [1.0, factor, 2.0]
    factors = _factor_rows(values)
    if pd.isna(factor):
        with pytest.raises(duckdb.ConstraintException, match="NOT NULL"):
            store.upsert_adj_factor(factors)
        store.upsert_adj_factor(factors.dropna(subset=["adj_factor"]))
    else:
        store.upsert_adj_factor(factors)

    result = store.get_daily_qfq("600000.SH", end="2026-06-03")

    assert not result["price_basis_available"].any()
    assert set(result["price_basis_reason"]) <= expected_reasons
    assert set(result["price_basis_reason"])
    assert result["qfq_close"].isna().all()


def test_daily_qfq_preserves_raw_pre_close_semantics(
    store: DuckDBStore,
) -> None:
    daily = _daily_rows(periods=2)
    store.upsert_daily(daily)
    store.upsert_adj_factor(_factor_rows([1.0, 2.0]))

    result = store.get_daily_qfq("600000.SH", end="2026-06-02")

    assert result["raw_pre_close"].tolist() == daily["pre_close"].tolist()
    assert "qfq_pre_close" not in result.columns


def test_stock_features_do_not_mix_raw_prices_when_factor_window_is_incomplete(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_daily_stock_feature_result

    daily = _daily_rows(periods=65)
    daily["pct_chg"] = [1.0 if index % 2 == 0 else -1.0 for index in range(65)]
    store.upsert_daily(daily)
    factors = _factor_rows([1.0] * 65).drop(index=55)
    store.upsert_adj_factor(factors)
    reference_date = daily.iloc[-1]["trade_date"]

    outcome = build_daily_stock_feature_result(
        store,
        "600000.SH",
        reference_date,
        price_lookbacks=(60,),
        accumulation_lookback=20,
    )
    result = outcome.features

    assert outcome.status == "partial"
    assert outcome.reason == "price_basis_unavailable"
    assert result["price_position_60d_pct"] is None
    assert result["ma_alignment"] is None
    assert result["accum_obv_change_20d_pct"] is None
    assert result["accum_window_days_20d"] == 20
    assert result["accum_ad_flow_20d_pct"] is not None
    assert result["accum_up_down_amount_ratio_20d"] is not None
    assert result["accum_heavy_no_drop_days_20d"] is not None
    assert result["accum_close_position_avg_20d_pct"] is not None
    scale_invariant = outcome.diagnostics["accumulation_scale_invariant_20d"]
    assert scale_invariant.available is True
    assert scale_invariant.basis is None
    assert scale_invariant.reason is None
    assert (
        outcome.diagnostics["accumulation_obv_20d"].reason
        == "missing_required_factor"
    )


def test_stock_features_use_complete_adjusted_price_basis(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_daily_stock_features

    daily = _daily_rows(periods=65)
    store.upsert_daily(daily)
    store.upsert_adj_factor(_factor_rows([1.0] * 64 + [2.0]))
    reference_date = daily.iloc[-1]["trade_date"]

    result = build_daily_stock_features(
        store,
        "600000.SH",
        reference_date,
        price_lookbacks=(60,),
        accumulation_lookback=20,
    )

    assert result["price_position_60d_pct"] is not None
    assert result["ma_alignment"] in {0, 1}
    assert result["accum_obv_change_20d_pct"] is not None


def test_stock_features_validate_each_actual_window_independently(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_daily_stock_feature_result

    daily = _daily_rows(periods=260)
    daily["pct_chg"] = [1.0 if index % 2 == 0 else -1.0 for index in range(260)]
    store.upsert_daily(daily)
    factors = _factor_rows([1.0] * 260).drop(index=10)
    store.upsert_adj_factor(factors)
    reference_date = daily.iloc[-1]["trade_date"]

    outcome = build_daily_stock_feature_result(
        store,
        "600000.SH",
        reference_date,
        price_lookbacks=(90, 250),
        accumulation_lookback=20,
    )
    features = outcome.features

    assert outcome.status == "partial"
    assert outcome.reason == "price_basis_unavailable"
    assert features["price_position_90d_pct"] is not None
    assert features["price_position_250d_pct"] is None
    assert features["ma_alignment"] == 1
    assert features["price_percentile_250d"] is None
    assert features["accum_obv_change_20d_pct"] is not None
    assert features["accum_ad_flow_20d_pct"] is not None
    assert features["accum_up_down_amount_ratio_20d"] is not None
    assert outcome.diagnostics["price_position_90d"].available is True
    assert outcome.diagnostics["ma_alignment_60d"].available is True
    assert outcome.diagnostics["accumulation_obv_20d"].available is True
    assert (
        outcome.diagnostics["price_position_250d"].reason
        == "missing_required_factor"
    )
    assert (
        outcome.diagnostics["price_percentile_250d"].reason
        == "missing_required_factor"
    )


@pytest.mark.parametrize(
    ("factor", "drop_factor", "expected_reason"),
    [
        (1.0, True, "missing_required_factor"),
        (float("inf"), False, "non_finite_required_factor"),
        (0.0, False, "non_positive_required_factor"),
    ],
)
def test_stock_feature_outcome_preserves_price_basis_reason(
    store: DuckDBStore,
    factor: float,
    drop_factor: bool,
    expected_reason: str,
) -> None:
    from rquant.stock_features import build_daily_stock_feature_result

    daily = _daily_rows(periods=65)
    store.upsert_daily(daily)
    values = [1.0] * 65
    values[30] = factor
    factors = _factor_rows(values)
    if drop_factor:
        factors = factors.drop(index=30)
    store.upsert_adj_factor(factors)

    outcome = build_daily_stock_feature_result(
        store,
        "600000.SH",
        daily.iloc[-1]["trade_date"],
        price_lookbacks=(60,),
        accumulation_lookback=20,
    )

    diagnostic = outcome.diagnostics["price_position_60d"]
    assert diagnostic.available is False
    assert diagnostic.reason == expected_reason
    assert diagnostic.basis is not None
    assert diagnostic.basis.unavailable_reason == expected_reason


def test_stock_feature_outcome_reports_missing_daily_data(
    store: DuckDBStore,
) -> None:
    from rquant.stock_features import build_daily_stock_feature_result

    outcome = build_daily_stock_feature_result(
        store,
        "600000.SH",
        date(2026, 6, 24),
    )

    assert outcome.status == "no_data"
    assert outcome.reason == "missing_daily_data"
    assert outcome.features == {}
    assert outcome.diagnostics == {}


def _seed_profile_minutes(store: DuckDBStore) -> None:
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime(2026, 6, day, 9, 30),
            "freq": "1min",
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "vol": 100.0,
            "amount": price * 100,
            "source": "tushare",
        }
        for day, price in [(1, 20.0), (2, 10.0), (3, 11.0)]
    ]))


def test_volume_profile_is_unavailable_when_any_minute_day_factor_is_missing(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import (
        calculate_volume_profile,
        calculate_volume_profile_outcome,
    )

    store.upsert_daily(_daily_rows(periods=4))
    store.upsert_adj_factor(_factor_rows([0.5, 1.0, 1.0, 1.0]).drop(index=1))
    _seed_profile_minutes(store)

    outcome = calculate_volume_profile_outcome(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )

    assert outcome.profile is None
    assert outcome.status == "price_basis_unavailable"
    assert outcome.reason == "missing_required_factor"
    assert outcome.price_basis is not None
    assert outcome.price_basis.unavailable_reason == "missing_required_factor"
    assert calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    ) is None


def test_volume_profile_uses_complete_factor_window(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile

    daily = _daily_rows(periods=4)
    daily["close"] = 10.0
    store.upsert_daily(daily)
    store.upsert_adj_factor(_factor_rows([0.5, 1.0, 1.0, 1.0]))
    _seed_profile_minutes(store)

    result = calculate_volume_profile(
        store,
        "600000.SH",
        reference_date=date(2026, 6, 4),
        lookback_days=3,
    )

    assert result is not None
    assert result.weight_basis == "adjusted_share_volume"
    assert result.total_vol == pytest.approx(400.0)
    assert result.vwap == pytest.approx(10.25)
    assert result.poc_price == pytest.approx(10.0)
    assert result.value_area_low == pytest.approx(10.0)
    assert result.value_area_high == pytest.approx(10.0)
    assert result.above_reference_volume_pct == pytest.approx(25.0)


def test_volume_profile_uses_one_adjusted_share_weight_for_all_chip_metrics(
    store: DuckDBStore,
) -> None:
    from rquant.volume_profile import calculate_volume_profile_outcome

    trading_dates = [
        item.date() for item in pd.bdate_range("2026-06-01", periods=7)
    ]
    history_dates = trading_dates[:6]
    reference_date = trading_dates[-1]
    store.upsert_daily(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "pre_close": 10.0,
            "change": 0.0,
            "pct_chg": 0.0,
            "vol": 1.0,
            "amount": 1.0,
        }
        for trade_date in trading_dates
    ]))
    store.upsert_adj_factor(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_date": trade_date,
            "adj_factor": factor,
        }
        for trade_date, factor in zip(
            trading_dates,
            [0.1, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            strict=True,
        )
    ]))
    raw_prices = [100.0, 36.0, 20.0, 21.0, 22.0, 23.0]
    raw_volumes = [350.0, 1000.0, 100.0, 100.0, 100.0, 100.0]
    store.upsert_minute_bars(pd.DataFrame([
        {
            "ts_code": "600000.SH",
            "trade_time": datetime.combine(trade_date, datetime.min.time()).replace(
                hour=9,
                minute=30,
            ),
            "freq": "1min",
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "vol": volume,
            "amount": price * volume,
            "source": "tushare",
        }
        for trade_date, price, volume in zip(
            history_dates,
            raw_prices,
            raw_volumes,
            strict=True,
        )
    ]))

    outcome = calculate_volume_profile_outcome(
        store,
        "600000.SH",
        reference_date=reference_date,
        lookback_days=6,
    )

    assert outcome.status == "available"
    assert outcome.reason is None
    assert outcome.price_basis is not None
    assert outcome.price_basis.available is True
    assert outcome.profile is not None
    profile = outcome.profile
    # 10 元档 raw amount=35,000，小于 36 元档的 36,000；但可比股数
    # 3,500 大于 1,000，因此核心筹码指标必须选择 10 元档。
    assert profile.weight_basis == "adjusted_share_volume"
    assert profile.total_vol == pytest.approx(4900.0)
    assert profile.total_amount == pytest.approx(79600.0)
    assert profile.vwap == pytest.approx(79600.0 / 4900.0)
    assert profile.poc_price == pytest.approx(10.0)
    assert profile.value_area_low == pytest.approx(10.0)
    assert profile.value_area_high == pytest.approx(10.0)
    assert profile.concentration_top5_pct == pytest.approx(4800 / 4900 * 100)
    assert profile.above_reference_amount_pct == pytest.approx(44600 / 79600 * 100)
    assert profile.above_reference_volume_pct == pytest.approx(1400 / 4900 * 100)
    assert profile.below_reference_amount_pct == 0.0
    assert profile.below_reference_volume_pct == 0.0
