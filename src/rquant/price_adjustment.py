"""除权/复权导致的价格基准调整工具。"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date
from math import isfinite
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

PriceFactorUnavailableReason = Literal[
    "missing_reference_factor",
    "non_finite_reference_factor",
    "non_positive_reference_factor",
    "missing_required_factor",
    "non_finite_required_factor",
    "non_positive_required_factor",
]


class PriceFactorRatio(BaseModel):
    """一个交易日相对参考日的复权比例。"""

    model_config = ConfigDict(frozen=True)

    trade_date: date
    factor: float = Field(gt=0, allow_inf_nan=False)
    ratio: float = Field(gt=0, allow_inf_nan=False)


class PriceFactorBasis(BaseModel):
    """一段价格窗口的统一复权基准及其可用性。"""

    model_config = ConfigDict(frozen=True)

    available: bool
    reference_date: date
    reference_factor: float | None = None
    ratios: tuple[PriceFactorRatio, ...] = ()
    unavailable_reason: PriceFactorUnavailableReason | None = None
    unavailable_dates: tuple[date, ...] = ()

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        """可用和不可用结果不能携带互相矛盾的字段。"""
        if self.available:
            if (
                self.reference_factor is None
                or not isfinite(self.reference_factor)
                or self.reference_factor <= 0
            ):
                raise ValueError("available basis requires a finite positive reference_factor")
            if not self.ratios:
                raise ValueError("available basis requires at least one ratio")
            ratio_dates = [item.trade_date for item in self.ratios]
            if len(ratio_dates) != len(set(ratio_dates)):
                raise ValueError("available basis requires unique ratio dates")
            if self.unavailable_reason is not None or self.unavailable_dates:
                raise ValueError("available basis cannot carry unavailable diagnostics")
            return self

        if self.unavailable_reason is None:
            raise ValueError("unavailable basis requires unavailable_reason")
        if self.ratios:
            raise ValueError("unavailable basis cannot carry ratios")
        return self

    def ratio_by_date(self) -> dict[date, float]:
        """返回交易日到复权比例的映射。"""
        return {item.trade_date: item.ratio for item in self.ratios}


def resolve_price_factor_basis(
    *,
    required_dates: Iterable[date],
    factor_by_date: Mapping[date, float | None],
    reference_date: date,
) -> PriceFactorBasis:
    """验证整段因子完整性，并统一到 reference_date 价格基准。"""
    required = tuple(sorted(set(required_dates)))
    if not required:
        raise ValueError("required_dates must not be empty")
    reference_factor = factor_by_date.get(reference_date)
    if reference_factor is None:
        return PriceFactorBasis(
            available=False,
            reference_date=reference_date,
            unavailable_reason="missing_reference_factor",
            unavailable_dates=(reference_date,),
        )
    reference_factor = float(reference_factor)
    if not isfinite(reference_factor):
        return PriceFactorBasis(
            available=False,
            reference_date=reference_date,
            reference_factor=reference_factor,
            unavailable_reason="non_finite_reference_factor",
            unavailable_dates=(reference_date,),
        )
    if reference_factor <= 0:
        return PriceFactorBasis(
            available=False,
            reference_date=reference_date,
            reference_factor=reference_factor,
            unavailable_reason="non_positive_reference_factor",
            unavailable_dates=(reference_date,),
        )

    missing_dates = tuple(
        trade_date
        for trade_date in required
        if factor_by_date.get(trade_date) is None
    )
    if missing_dates:
        return PriceFactorBasis(
            available=False,
            reference_date=reference_date,
            reference_factor=reference_factor,
            unavailable_reason="missing_required_factor",
            unavailable_dates=missing_dates,
        )

    non_finite_dates = tuple(
        trade_date
        for trade_date in required
        if not isfinite(float(factor_by_date[trade_date]))
    )
    if non_finite_dates:
        return PriceFactorBasis(
            available=False,
            reference_date=reference_date,
            reference_factor=reference_factor,
            unavailable_reason="non_finite_required_factor",
            unavailable_dates=non_finite_dates,
        )

    non_positive_dates = tuple(
        trade_date
        for trade_date in required
        if float(factor_by_date[trade_date]) <= 0
    )
    if non_positive_dates:
        return PriceFactorBasis(
            available=False,
            reference_date=reference_date,
            reference_factor=reference_factor,
            unavailable_reason="non_positive_required_factor",
            unavailable_dates=non_positive_dates,
        )

    return PriceFactorBasis(
        available=True,
        reference_date=reference_date,
        reference_factor=reference_factor,
        ratios=tuple(
            PriceFactorRatio(
                trade_date=trade_date,
                factor=float(factor_by_date[trade_date]),
                ratio=float(factor_by_date[trade_date]) / reference_factor,
            )
            for trade_date in required
        ),
    )


class PriceBasisAdjustment(BaseModel):
    """前后交易日价格基准调整。"""

    model_config = ConfigDict(frozen=True)

    previous_close: float | None = None
    current_pre_close: float | None = None
    ratio: float = 1.0
    adjusted: bool = False
    reason: str = "same_basis"

    def adjust(self, price: float | None) -> float | None:
        """把旧价格基准上的价格缩放到当前交易日基准。"""
        if price is None:
            return None
        return float(price) * self.ratio


def resolve_price_basis_adjustment(
    previous_close: float | None,
    current_pre_close: float | None,
    *,
    threshold_pct: float = 0.01,
) -> PriceBasisAdjustment:
    """根据 `今日pre_close / 昨日close` 判断是否发生价格基准断点。"""
    if (
        previous_close is None
        or current_pre_close is None
        or previous_close <= 0
        or current_pre_close <= 0
    ):
        return PriceBasisAdjustment(
            previous_close=previous_close,
            current_pre_close=current_pre_close,
            reason="missing_price",
        )

    ratio = current_pre_close / previous_close
    if abs(ratio - 1) <= threshold_pct:
        return PriceBasisAdjustment(
            previous_close=previous_close,
            current_pre_close=current_pre_close,
        )

    return PriceBasisAdjustment(
        previous_close=previous_close,
        current_pre_close=current_pre_close,
        ratio=ratio,
        adjusted=True,
        reason="price_basis_discontinuity",
    )
