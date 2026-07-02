"""除权/复权导致的价格基准调整工具。"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


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
