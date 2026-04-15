"""Tushare Pro adapter：历史日线 / 股票基础信息。

只做取数 + 归一化。落库由 storage 层负责，不在这里耦合 DB。
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import tushare as ts
from loguru import logger

from rquant.config import settings


class TushareAdapter:
    """Tushare Pro 封装。主 token 失败时自动切备用 token 重试一次。"""

    def __init__(self, token: str | None = None) -> None:
        self._primary_token = token or settings.tushare_token_main
        self._backup_token = settings.tushare_token_backup
        self._pro = ts.pro_api(self._primary_token)
        self._using_backup = False

    def _switch_to_backup(self) -> bool:
        if self._backup_token and not self._using_backup:
            logger.warning("Tushare 主 token 失败，切换到备用 token")
            self._pro = ts.pro_api(self._backup_token)
            self._using_backup = True
            return True
        return False

    def daily(
        self,
        ts_codes: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """拉取日线 OHLCV。

        返回字段（对齐 Tushare `daily` 接口）：
            ts_code, trade_date, open, high, low, close,
            pre_close, change, pct_chg, vol, amount
        """
        codes_str = ",".join(ts_codes)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        logger.info(
            f"Tushare daily 请求：codes={codes_str} start={start_str} end={end_str}"
        )

        try:
            df = self._pro.daily(
                ts_code=codes_str,
                start_date=start_str,
                end_date=end_str,
            )
        except Exception as e:
            if self._switch_to_backup():
                df = self._pro.daily(
                    ts_code=codes_str,
                    start_date=start_str,
                    end_date=end_str,
                )
            else:
                raise RuntimeError(f"Tushare daily 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(
                f"Tushare daily 返回空：codes={codes_str} {start_str}-{end_str}"
            )
            return pd.DataFrame()

        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
        df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        logger.info(f"Tushare daily 返回 {len(df)} 行")
        return df

    def stock_basic(self, list_status: str = "L") -> pd.DataFrame:
        """股票基础信息（代码 / 名称 / 行业 / 上市日期等）。

        list_status: L=上市, D=退市, P=暂停上市
        """
        logger.info(f"Tushare stock_basic 请求：list_status={list_status}")
        df = self._pro.stock_basic(
            exchange="",
            list_status=list_status,
            fields="ts_code,symbol,name,area,industry,list_date,market",
        )
        logger.info(f"Tushare stock_basic 返回 {len(df)} 行")
        return df
