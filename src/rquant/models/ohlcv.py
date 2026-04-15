"""K 线数据模型。跨层传递时统一用这些模型，禁止裸 dict。"""

from datetime import date

from pydantic import BaseModel, ConfigDict, Field


class DailyBar(BaseModel):
    """日线 OHLCV + 基础指标。"""

    model_config = ConfigDict(frozen=True)

    ts_code: str = Field(..., description="Tushare 股票代码，如 000001.SZ")
    trade_date: date = Field(..., description="交易日期")
    open: float
    high: float
    low: float
    close: float
    pre_close: float | None = None
    change: float | None = None
    pct_chg: float | None = None
    vol: float = Field(..., description="成交量（手）")
    amount: float = Field(..., description="成交额（千元）")
