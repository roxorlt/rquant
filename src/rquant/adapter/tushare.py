"""Tushare Pro adapter：历史日线 / 股票基础信息。

只做取数 + 归一化。落库由 storage 层负责，不在这里耦合 DB。
"""

from __future__ import annotations

from datetime import date, datetime

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

    def adj_factor(
        self,
        ts_codes: list[str],
        start: date,
        end: date,
    ) -> pd.DataFrame:
        """拉取复权因子。

        Tushare `adj_factor` 接口返回：ts_code, trade_date, adj_factor
        因子累计递增，最新交易日的因子最大。前复权公式：
            qfq[t] = raw[t] * adj_factor[t] / adj_factor[latest]
        """
        codes_str = ",".join(ts_codes)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        logger.info(
            f"Tushare adj_factor 请求：codes={codes_str} start={start_str} end={end_str}"
        )

        try:
            df = self._pro.adj_factor(
                ts_code=codes_str,
                start_date=start_str,
                end_date=end_str,
            )
        except Exception as e:
            if self._switch_to_backup():
                df = self._pro.adj_factor(
                    ts_code=codes_str,
                    start_date=start_str,
                    end_date=end_str,
                )
            else:
                raise RuntimeError(f"Tushare adj_factor 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(
                f"Tushare adj_factor 返回空：codes={codes_str} {start_str}-{end_str}"
            )
            return pd.DataFrame()

        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
        df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        logger.info(f"Tushare adj_factor 返回 {len(df)} 行")
        return df

    def stk_mins(
        self,
        ts_code: str,
        freq: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        """拉取 A 股历史分钟行情。

        Tushare `stk_mins` 接口：
        - freq: 1min / 5min / 15min / 30min / 60min
        - 单次最大 8000 行；更长区间由调用方分段循环
        - vol 单位：股；amount 单位：元
        """
        allowed = {"1min", "5min", "15min", "30min", "60min"}
        if freq not in allowed:
            raise ValueError(f"unsupported stk_mins freq: {freq}")

        start_str = start.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(
            f"Tushare stk_mins 请求：code={ts_code} freq={freq} "
            f"start={start_str} end={end_str}"
        )

        try:
            df = self._pro.stk_mins(
                ts_code=ts_code,
                freq=freq,
                start_date=start_str,
                end_date=end_str,
            )
        except Exception as e:
            raise RuntimeError(f"Tushare stk_mins 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(
                f"Tushare stk_mins 返回空：code={ts_code} {freq} "
                f"{start_str}-{end_str}"
            )
            return pd.DataFrame()

        required = ["ts_code", "trade_time", "open", "high", "low", "close", "vol", "amount"]
        missing = set(required) - set(df.columns)
        if missing:
            raise RuntimeError(f"Tushare stk_mins 返回缺字段：{sorted(missing)}")

        out = df[required].copy()
        out["trade_time"] = pd.to_datetime(out["trade_time"])
        out["freq"] = freq
        out["source"] = "tushare"
        out = out.sort_values(["ts_code", "trade_time"]).reset_index(drop=True)

        logger.info(f"Tushare stk_mins 返回 {len(out)} 行")
        return out

    def rt_min(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
        """拉取 A 股实时分钟最新 K 线。

        Tushare `rt_min` 接口返回当前最新一根分钟 K，可批量传多个股票代码。
        归一化后直接对齐 `minute_bar` 表结构。
        """
        freq_map = {
            "1min": "1MIN",
            "5min": "5MIN",
            "15min": "15MIN",
            "30min": "30MIN",
            "60min": "60MIN",
        }
        if freq not in freq_map:
            raise ValueError(f"unsupported rt_min freq: {freq}")
        if not ts_codes:
            return pd.DataFrame(
                columns=[
                    "ts_code", "trade_time", "freq", "open", "high",
                    "low", "close", "vol", "amount", "source",
                ]
            )

        codes_str = ",".join(ts_codes)
        tushare_freq = freq_map[freq]
        logger.info(
            f"Tushare rt_min 请求：codes={codes_str} freq={tushare_freq}"
        )
        try:
            df = self._pro.rt_min(ts_code=codes_str, freq=tushare_freq)
        except Exception as e:
            raise RuntimeError(f"Tushare rt_min 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(f"Tushare rt_min 返回空：codes={codes_str} freq={tushare_freq}")
            return pd.DataFrame()

        required = [
            "ts_code",
            "time",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "amount",
        ]
        missing = set(required) - set(df.columns)
        if missing:
            raise RuntimeError(f"Tushare rt_min 返回缺字段：{sorted(missing)}")

        out = df[required].copy()
        out["trade_time"] = pd.to_datetime(out["time"])
        out["freq"] = freq
        out["source"] = "tushare_rt"
        out = out[
            ["ts_code", "trade_time", "freq", "open", "high", "low", "close",
             "vol", "amount", "source"]
        ]
        out = out.sort_values(["ts_code", "trade_time"]).reset_index(drop=True)
        logger.info(f"Tushare rt_min 返回 {len(out)} 行")
        return out

    def rt_min_daily(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
        """拉取单只或多只股票当日开盘以来分钟 K 线。

        Tushare `rt_min_daily` 单次面向单只股票；这里为了业务侧方便，支持
        多个 ts_code 输入并逐只合并。它用于盘中补齐缺失分钟，不用于历史回测。
        """
        freq_map = {
            "1min": "1MIN",
            "5min": "5MIN",
            "15min": "15MIN",
            "30min": "30MIN",
            "60min": "60MIN",
        }
        if freq not in freq_map:
            raise ValueError(f"unsupported rt_min_daily freq: {freq}")
        if not ts_codes:
            return pd.DataFrame(
                columns=[
                    "ts_code", "trade_time", "freq", "open", "high",
                    "low", "close", "vol", "amount", "source",
                ]
            )

        tushare_freq = freq_map[freq]
        frames: list[pd.DataFrame] = []
        for ts_code in ts_codes:
            logger.info(
                f"Tushare rt_min_daily 请求：code={ts_code} freq={tushare_freq}"
            )
            try:
                df = self._pro.rt_min_daily(ts_code=ts_code, freq=tushare_freq)
            except Exception as e:
                raise RuntimeError(
                    f"Tushare rt_min_daily 调用失败：{ts_code} {e}"
                ) from e

            if df is None or df.empty:
                logger.warning(
                    f"Tushare rt_min_daily 返回空：code={ts_code} freq={tushare_freq}"
                )
                continue

            if "ts_code" not in df.columns and "code" in df.columns:
                df = df.rename(columns={"code": "ts_code"})
            required = [
                "ts_code",
                "time",
                "open",
                "high",
                "low",
                "close",
                "vol",
                "amount",
            ]
            missing = set(required) - set(df.columns)
            if missing:
                raise RuntimeError(
                    f"Tushare rt_min_daily 返回缺字段：{sorted(missing)}"
                )

            out = df[required].copy()
            out["trade_time"] = pd.to_datetime(out["time"])
            out["freq"] = freq
            out["source"] = "tushare_rt_daily"
            frames.append(out[
                ["ts_code", "trade_time", "freq", "open", "high", "low",
                 "close", "vol", "amount", "source"]
            ])

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        result = result.sort_values(["ts_code", "trade_time"]).reset_index(drop=True)
        logger.info(f"Tushare rt_min_daily 返回 {len(result)} 行")
        return result

    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        """拉取 A 股当日集合竞价成交情况。

        Tushare `stk_auction` 独立权限接口，历史从 2025-01-01 起提供。
        不切备用 token：备用 token 未必开通同一付费权限，失败应直接暴露。
        """
        trade_date_str = trade_date.strftime("%Y%m%d")
        fields = "ts_code,trade_date,vol,price,amount,turnover_rate,volume_ratio"
        logger.info(f"Tushare stk_auction 请求：date={trade_date_str}")

        try:
            df = self._pro.stk_auction(trade_date=trade_date_str, fields=fields)
        except Exception as e:
            raise RuntimeError(f"Tushare stk_auction 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(f"Tushare stk_auction 返回空：date={trade_date_str}")
            return pd.DataFrame()

        required = [
            "ts_code",
            "trade_date",
            "vol",
            "price",
            "amount",
            "turnover_rate",
            "volume_ratio",
        ]
        missing = set(required) - set(df.columns)
        if missing:
            raise RuntimeError(f"Tushare stk_auction 返回缺字段：{sorted(missing)}")

        out = df[required].copy()
        out["trade_date"] = pd.to_datetime(
            out["trade_date"], format="%Y%m%d"
        ).dt.date
        out["auction_type"] = "open_realtime"
        out["source"] = "tushare"
        out = out.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        logger.info(f"Tushare stk_auction 返回 {len(out)} 行")
        return out

    def moneyflow(self, trade_date: date) -> pd.DataFrame:
        """拉取日级个股资金流。

        这是盘后日级数据，只适合复盘、标签分析或次日过滤；不能作为同日盘中
        B 信号的输入。
        """
        trade_date_str = trade_date.strftime("%Y%m%d")
        fields = (
            "ts_code,trade_date,buy_lg_vol,sell_lg_vol,"
            "buy_elg_vol,sell_elg_vol,net_mf_vol,net_mf_amount"
        )
        logger.info(f"Tushare moneyflow 请求：date={trade_date_str}")
        try:
            df = self._pro.moneyflow(trade_date=trade_date_str, fields=fields)
        except Exception as e:
            if self._switch_to_backup():
                df = self._pro.moneyflow(trade_date=trade_date_str, fields=fields)
            else:
                raise RuntimeError(f"Tushare moneyflow 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(f"Tushare moneyflow 返回空：date={trade_date_str}")
            return pd.DataFrame()

        required = [
            "ts_code",
            "trade_date",
            "buy_lg_vol",
            "sell_lg_vol",
            "buy_elg_vol",
            "sell_elg_vol",
            "net_mf_vol",
            "net_mf_amount",
        ]
        missing = set(required) - set(df.columns)
        if missing:
            raise RuntimeError(f"Tushare moneyflow 返回缺字段：{sorted(missing)}")

        out = df[required].copy()
        out["trade_date"] = pd.to_datetime(
            out["trade_date"], format="%Y%m%d"
        ).dt.date
        out = out.rename(
            columns={
                "net_mf_vol": "large_net_vol",
                "net_mf_amount": "large_net_amount",
            }
        )
        out["source"] = "tushare"
        out = out.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)
        logger.info(f"Tushare moneyflow 返回 {len(out)} 行")
        return out

    def daily_basic(
        self,
        ts_codes: list[str],
        trade_date: date,
    ) -> pd.DataFrame:
        """拉取每日基本面指标（换手率、量比、市值等）。

        注意：Tushare daily_basic 接口只支持按单日查询（trade_date），
        不支持 start_date/end_date 范围。
        """
        codes_str = ",".join(ts_codes)
        trade_date_str = trade_date.strftime("%Y%m%d")

        logger.info(
            f"Tushare daily_basic 请求：codes={codes_str} trade_date={trade_date_str}"
        )

        try:
            df = self._pro.daily_basic(
                ts_code=codes_str,
                trade_date=trade_date_str,
                fields="ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
            )
        except Exception as e:
            if self._switch_to_backup():
                df = self._pro.daily_basic(
                    ts_code=codes_str,
                    trade_date=trade_date_str,
                    fields="ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv",
                )
            else:
                raise RuntimeError(f"Tushare daily_basic 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(
                f"Tushare daily_basic 返回空：codes={codes_str} date={trade_date_str}"
            )
            return pd.DataFrame()

        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
        df = df.sort_values(["ts_code", "trade_date"]).reset_index(drop=True)

        logger.info(f"Tushare daily_basic 返回 {len(df)} 行")
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
