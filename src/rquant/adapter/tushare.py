"""Tushare Pro adapter：历史日线 / 股票基础信息。

只做取数 + 归一化。落库由 storage 层负责，不在这里耦合 DB。
"""

from __future__ import annotations

import time
from datetime import date, datetime

import pandas as pd
import tushare as ts
from loguru import logger

from rquant.config import settings

# 分页取数 / 多指数循环时相邻请求间隔（对齐 dataset_backfill._API_SLEEP）
_PAGE_SLEEP = 0.35


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


    def _call_with_backoff(
        self, api_name: str, call, max_retries: int = 6
    ) -> pd.DataFrame | None:
        """回补类调用的限频退避重试（by_date / 分页 / 快照通用）。

        限频错误（"频率超限"）绝不切备用 token：备用是免费档，daily_basic
        限频 1 次/分钟，切换等于把整个回补判死（2026-07-02 真实事故——主 token
        瞬时超限触发粘性切换，960 个日期全灭）。原地 sleep 后用主 token 重试。
        """
        for attempt in range(1, max_retries + 1):
            try:
                return call()
            except Exception as e:
                msg = str(e)
                if attempt >= max_retries:
                    raise RuntimeError(f"Tushare {api_name} 调用失败：{e}") from e
                wait = 25.0 if ("频率" in msg or "超限" in msg) else 5.0
                logger.warning(
                    f"Tushare {api_name} 第 {attempt}/{max_retries} 次失败"
                    f"（{wait}s 后重试）: {msg[:120]}"
                )
                time.sleep(wait)
        return None

    def trade_cal(self, start: date, end: date) -> list[date]:
        """交易日历（只返回开市日，升序）。

        历史回补的日历必须走 trade_cal：本地 daily_bar 覆盖不到未入库的
        早期区间（如 2020 年），从库里推日历会漏。
        """
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        logger.info(f"Tushare trade_cal 请求：start={start_str} end={end_str}")

        try:
            df = self._pro.trade_cal(
                exchange="SSE", start_date=start_str, end_date=end_str, is_open="1"
            )
        except Exception as e:
            if self._switch_to_backup():
                df = self._pro.trade_cal(
                    exchange="SSE", start_date=start_str, end_date=end_str, is_open="1"
                )
            else:
                raise RuntimeError(f"Tushare trade_cal 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(f"Tushare trade_cal 返回空：{start_str}-{end_str}")
            return []

        dates = pd.to_datetime(df["cal_date"], format="%Y%m%d").dt.date.tolist()
        logger.info(f"Tushare trade_cal 返回 {len(dates)} 个交易日")
        return sorted(dates)

    def daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """按交易日拉全市场日线（历史回补用，字段对齐 daily_bar 表）。"""
        ds = trade_date.strftime("%Y%m%d")
        logger.info(f"Tushare daily(by_date) 请求：trade_date={ds}")

        df = self._call_with_backoff(
            "daily", lambda: self._pro.daily(trade_date=ds)
        )

        if df is None or df.empty:
            logger.warning(f"Tushare daily 返回空：trade_date={ds}")
            return pd.DataFrame()

        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
        df = df.sort_values("ts_code").reset_index(drop=True)
        logger.info(f"Tushare daily(by_date) 返回 {len(df)} 行")
        return df

    def daily_basic_by_date(self, trade_date: date) -> pd.DataFrame:
        """按交易日拉全市场每日基本面指标（历史回补用，字段对齐 daily_basic 表）。"""
        ds = trade_date.strftime("%Y%m%d")
        fields = "ts_code,trade_date,turnover_rate,volume_ratio,total_mv,circ_mv"
        logger.info(f"Tushare daily_basic(by_date) 请求：trade_date={ds}")

        df = self._call_with_backoff(
            "daily_basic", lambda: self._pro.daily_basic(trade_date=ds, fields=fields)
        )

        if df is None or df.empty:
            logger.warning(f"Tushare daily_basic 返回空：trade_date={ds}")
            return pd.DataFrame()

        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d").dt.date
        df = df.sort_values("ts_code").reset_index(drop=True)
        logger.info(f"Tushare daily_basic(by_date) 返回 {len(df)} 行")
        return df

    def adj_factor_by_date(self, trade_date: date) -> pd.DataFrame:
        """按交易日拉全市场复权因子（历史回补用，归一化对齐 adj_factor 方法）。"""
        ds = trade_date.strftime("%Y%m%d")
        logger.info(f"Tushare adj_factor(by_date) 请求：trade_date={ds}")

        df = self._call_with_backoff(
            "adj_factor", lambda: self._pro.adj_factor(trade_date=ds)
        )

        if df is None or df.empty:
            logger.warning(f"Tushare adj_factor 返回空：trade_date={ds}")
            return pd.DataFrame()

        cols = ["ts_code", "trade_date", "adj_factor"]
        missing = set(cols) - set(df.columns)
        if missing:
            raise RuntimeError(f"Tushare adj_factor 返回缺字段：{sorted(missing)}")

        out = df[cols].copy()
        out["trade_date"] = pd.to_datetime(out["trade_date"], format="%Y%m%d").dt.date
        out = out.sort_values("ts_code").reset_index(drop=True)
        logger.info(f"Tushare adj_factor(by_date) 返回 {len(out)} 行")
        return out

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

    def limit_list_by_date(
        self, trade_date: date, limit_type: str = ""
    ) -> pd.DataFrame:
        """按交易日拉涨跌停/炸板榜（limit_list_d，5000 积分，历史从 2020 起）。

        limit_type 空串一次拿齐 U(涨停)/D(跌停)/Z(炸板)，省请求；单次上限
        2500 行，A 股单日涨跌停+炸板远够不到，不需分页。不含 ST 股。
        不切备用 token：备用 token 无此积分权限时会掩盖真实错误（同 stk_auction）。
        """
        ds = trade_date.strftime("%Y%m%d")
        fields = (
            "trade_date,ts_code,industry,name,close,pct_chg,amount,limit_amount,"
            "float_mv,total_mv,turnover_ratio,fd_amount,first_time,last_time,"
            "open_times,up_stat,limit_times,limit"
        )
        logger.info(
            f"Tushare limit_list_d 请求：date={ds} limit_type={limit_type or 'ALL'}"
        )

        try:
            df = self._pro.limit_list_d(
                trade_date=ds, limit_type=limit_type, fields=fields
            )
        except Exception as e:
            raise RuntimeError(f"Tushare limit_list_d 调用失败：{e}") from e

        if df is None or df.empty:
            logger.warning(f"Tushare limit_list_d 返回空：date={ds}")
            return pd.DataFrame()

        required = [
            "ts_code", "trade_date", "name", "industry", "close", "pct_chg",
            "amount", "limit_amount", "float_mv", "total_mv", "turnover_ratio",
            "fd_amount", "first_time", "last_time", "open_times", "up_stat",
            "limit_times", "limit",
        ]
        missing = set(required) - set(df.columns)
        if missing:
            raise RuntimeError(f"Tushare limit_list_d 返回缺字段：{sorted(missing)}")

        out = df[required].copy()
        out["trade_date"] = pd.to_datetime(
            out["trade_date"], format="%Y%m%d"
        ).dt.date
        out = out.sort_values(["limit", "ts_code"]).reset_index(drop=True)
        logger.info(f"Tushare limit_list_d 返回 {len(out)} 行")
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

    # ══ 统一数据集回补层薄方法（dataset_backfill 注册表用） ══════════════════
    # 各方法 docstring 里的字段清单为 2026-07-01 trade_date=20260701 实测返回。
    # 共同约束：fields 显式、trade_date 归一化为 date、空返回容错、
    # 全部经 _call_with_backoff 限频退避，不切备用 token（付费权限未必对齐）。

    def _finalize_dataset(self, api_name: str, df: pd.DataFrame | None) -> pd.DataFrame:
        """数据集取数收尾：空容错 + trade_date 归一化。"""
        if df is None or df.empty:
            logger.warning(f"Tushare {api_name} 返回空")
            return pd.DataFrame()
        df = df.copy()
        if "trade_date" in df.columns:
            df["trade_date"] = pd.to_datetime(
                df["trade_date"], format="%Y%m%d"
            ).dt.date
        logger.info(f"Tushare {api_name} 返回 {len(df)} 行")
        return df

    def _dataset_by_date(
        self, api_name: str, trade_date: date, fields: str
    ) -> pd.DataFrame:
        """按 trade_date 全市场取数的数据集接口共用入口。"""
        ds = trade_date.strftime("%Y%m%d")
        api = getattr(self._pro, api_name)
        df = self._call_with_backoff(
            api_name, lambda: api(trade_date=ds, fields=fields)
        )
        return self._finalize_dataset(api_name, df)

    def _dataset_paged(
        self,
        api_name: str,
        *,
        fields: str,
        page_size: int,
        max_pages: int = 60,
        **params: str,
    ) -> pd.DataFrame:
        """offset 分页全量拉取（dc_member 单页 8000 / ths_member 单页 6000 实测封顶）。

        page_size 必须 ≤ 服务端单页上限：终止条件是「本页行数 < page_size」，
        page_size 大于服务端上限会把被截断的整页误判为最后一页而提前结束。
        """
        api = getattr(self._pro, api_name)
        frames: list[pd.DataFrame] = []
        for page in range(max_pages):
            offset = page * page_size
            df = self._call_with_backoff(
                api_name,
                lambda off=offset: api(
                    offset=off, limit=page_size, fields=fields, **params
                ),
            )
            if df is None or df.empty:
                break
            frames.append(df)
            if len(df) < page_size:
                break
            time.sleep(_PAGE_SLEEP)
        else:
            raise RuntimeError(
                f"Tushare {api_name} 分页超过 {max_pages} 页仍未拉完，疑似异常"
            )
        if not frames:
            return self._finalize_dataset(api_name, None)
        return self._finalize_dataset(
            api_name, pd.concat(frames, ignore_index=True)
        )

    # ── 板块日行情 ──

    def ths_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """同花顺概念/行业指数日行情。实测 12 字段：ts_code, trade_date, open,
        high, low, close, pre_close, avg_price, change, pct_change, vol,
        turnover_rate（无 amount）。"""
        return self._dataset_by_date(
            "ths_daily", trade_date,
            fields="ts_code,trade_date,open,high,low,close,pre_close,"
                   "avg_price,change,pct_change,vol,turnover_rate",
        )

    def dc_daily_by_date(self, trade_date: date) -> pd.DataFrame:
        """东财板块日行情（2020 起）。实测 13 字段：ts_code, trade_date, close,
        open, high, low, change, pct_change, vol, amount, swing,
        turnover_rate, category。"""
        return self._dataset_by_date(
            "dc_daily", trade_date,
            fields="ts_code,trade_date,open,high,low,close,change,"
                   "pct_change,vol,amount,swing,turnover_rate,category",
        )

    # ── 板块静态 / 成分快照 ──

    def ths_index_snapshot(self) -> pd.DataFrame:
        """同花顺板块列表全量。实测 6 字段：ts_code, name, count, exchange,
        list_date, type（count/type 落库前由 normalize 改名）。"""
        return self._dataset_paged(
            "ths_index",
            fields="ts_code,name,count,exchange,list_date,type",
            page_size=5000,
        )

    def ths_member_snapshot(self) -> pd.DataFrame:
        """同花顺板块成分全量（实测无参可分页拉全量，单页 6000 封顶）。
        实测 3 字段：ts_code(板块代码), con_code, con_name。"""
        return self._dataset_paged(
            "ths_member",
            fields="ts_code,con_code,con_name",
            page_size=6000,
        )

    def dc_index_snapshot(self, trade_date: date) -> pd.DataFrame:
        """东财板块列表（源按 trade_date 出数）。实测 13 字段：ts_code,
        trade_date, name, leading, leading_code, pct_change, leading_pct,
        total_mv, turnover_rate, up_num, down_num, idx_type, level。"""
        return self._dataset_by_date(
            "dc_index", trade_date,
            fields="ts_code,trade_date,name,leading,leading_code,pct_change,"
                   "leading_pct,total_mv,turnover_rate,up_num,down_num,"
                   "idx_type,level",
        )

    def dc_member_snapshot(self, trade_date: date) -> pd.DataFrame:
        """东财板块成分（按日全量，实测单页 8000 封顶需分页）。
        实测 4 字段：trade_date, ts_code(板块代码), con_code, name。"""
        return self._dataset_paged(
            "dc_member",
            fields="trade_date,ts_code,con_code,name",
            page_size=8000,
            trade_date=trade_date.strftime("%Y%m%d"),
        )

    def hm_list_snapshot(self) -> pd.DataFrame:
        """游资名录（静态快照，实测 110 行）。实测 3 字段：name, desc, orgs
        （desc 落库前由 normalize 改名 description）。"""
        return self._dataset_paged(
            "hm_list", fields="name,desc,orgs", page_size=2000
        )

    # ── 资金流 ──

    def moneyflow_full_by_date(self, trade_date: date) -> pd.DataFrame:
        """个股资金流全 20 字段（2010 起）。实测：ts_code, trade_date,
        buy_sm_vol, buy_sm_amount, sell_sm_vol, sell_sm_amount, buy_md_vol,
        buy_md_amount, sell_md_vol, sell_md_amount, buy_lg_vol, buy_lg_amount,
        sell_lg_vol, sell_lg_amount, buy_elg_vol, buy_elg_amount, sell_elg_vol,
        sell_elg_amount, net_mf_vol, net_mf_amount。

        与 moneyflow()（monitor 用的 8 字段窄口径）并存：本方法给
        dataset_backfill 落 moneyflow_daily 全字段。"""
        return self._dataset_by_date(
            "moneyflow", trade_date,
            fields="ts_code,trade_date,buy_sm_vol,buy_sm_amount,sell_sm_vol,"
                   "sell_sm_amount,buy_md_vol,buy_md_amount,sell_md_vol,"
                   "sell_md_amount,buy_lg_vol,buy_lg_amount,sell_lg_vol,"
                   "sell_lg_amount,buy_elg_vol,buy_elg_amount,sell_elg_vol,"
                   "sell_elg_amount,net_mf_vol,net_mf_amount",
        )

    def moneyflow_dc_by_date(self, trade_date: date) -> pd.DataFrame:
        """东财个股资金流（2023-09 起）。实测 15 字段：trade_date, ts_code,
        name, pct_change, close, net_amount, net_amount_rate, buy_elg_amount,
        buy_elg_amount_rate, buy_lg_amount, buy_lg_amount_rate, buy_md_amount,
        buy_md_amount_rate, buy_sm_amount, buy_sm_amount_rate。"""
        return self._dataset_by_date(
            "moneyflow_dc", trade_date,
            fields="trade_date,ts_code,name,pct_change,close,net_amount,"
                   "net_amount_rate,buy_elg_amount,buy_elg_amount_rate,"
                   "buy_lg_amount,buy_lg_amount_rate,buy_md_amount,"
                   "buy_md_amount_rate,buy_sm_amount,buy_sm_amount_rate",
        )

    def moneyflow_ths_by_date(self, trade_date: date) -> pd.DataFrame:
        """同花顺个股资金流。实测 13 字段：trade_date, ts_code, name,
        pct_change, latest, net_amount, net_d5_amount, buy_lg_amount,
        buy_lg_amount_rate, buy_md_amount, buy_md_amount_rate, buy_sm_amount,
        buy_sm_amount_rate。"""
        return self._dataset_by_date(
            "moneyflow_ths", trade_date,
            fields="trade_date,ts_code,name,pct_change,latest,net_amount,"
                   "net_d5_amount,buy_lg_amount,buy_lg_amount_rate,"
                   "buy_md_amount,buy_md_amount_rate,buy_sm_amount,"
                   "buy_sm_amount_rate",
        )

    def moneyflow_ind_ths_by_date(self, trade_date: date) -> pd.DataFrame:
        """同花顺行业资金流。实测 12 字段：trade_date, ts_code, industry,
        lead_stock, close, pct_change, company_num, pct_change_stock,
        close_price, net_buy_amount, net_sell_amount, net_amount。"""
        return self._dataset_by_date(
            "moneyflow_ind_ths", trade_date,
            fields="trade_date,ts_code,industry,lead_stock,close,pct_change,"
                   "company_num,pct_change_stock,close_price,net_buy_amount,"
                   "net_sell_amount,net_amount",
        )

    def moneyflow_ind_dc_by_date(self, trade_date: date) -> pd.DataFrame:
        """东财板块/概念资金流。实测 18 字段：trade_date, content_type,
        ts_code, name, pct_change, close, net_amount, net_amount_rate,
        buy_elg_amount, buy_elg_amount_rate, buy_lg_amount, buy_lg_amount_rate,
        buy_md_amount, buy_md_amount_rate, buy_sm_amount, buy_sm_amount_rate,
        buy_sm_amount_stock, rank（rank 落库前由 normalize 改名）。"""
        return self._dataset_by_date(
            "moneyflow_ind_dc", trade_date,
            fields="trade_date,content_type,ts_code,name,pct_change,close,"
                   "net_amount,net_amount_rate,buy_elg_amount,"
                   "buy_elg_amount_rate,buy_lg_amount,buy_lg_amount_rate,"
                   "buy_md_amount,buy_md_amount_rate,buy_sm_amount,"
                   "buy_sm_amount_rate,buy_sm_amount_stock,rank",
        )

    def moneyflow_cnt_ths_by_date(self, trade_date: date) -> pd.DataFrame:
        """同花顺概念资金流。实测 12 字段：trade_date, ts_code, name,
        lead_stock, close_price, pct_change, industry_index, company_num,
        pct_change_stock, net_buy_amount, net_sell_amount, net_amount。"""
        return self._dataset_by_date(
            "moneyflow_cnt_ths", trade_date,
            fields="trade_date,ts_code,name,lead_stock,close_price,pct_change,"
                   "industry_index,company_num,pct_change_stock,"
                   "net_buy_amount,net_sell_amount,net_amount",
        )

    def moneyflow_mkt_dc_by_date(self, trade_date: date) -> pd.DataFrame:
        """东财大盘资金流（1 行/日）。实测 15 字段：trade_date, close_sh,
        pct_change_sh, close_sz, pct_change_sz, net_amount, net_amount_rate,
        buy_elg_amount, buy_elg_amount_rate, buy_lg_amount, buy_lg_amount_rate,
        buy_md_amount, buy_md_amount_rate, buy_sm_amount, buy_sm_amount_rate。"""
        return self._dataset_by_date(
            "moneyflow_mkt_dc", trade_date,
            fields="trade_date,close_sh,pct_change_sh,close_sz,pct_change_sz,"
                   "net_amount,net_amount_rate,buy_elg_amount,"
                   "buy_elg_amount_rate,buy_lg_amount,buy_lg_amount_rate,"
                   "buy_md_amount,buy_md_amount_rate,buy_sm_amount,"
                   "buy_sm_amount_rate",
        )

    # ── 龙虎榜 ──

    def top_list_by_date(self, trade_date: date) -> pd.DataFrame:
        """龙虎榜每日明细（2005 起）。实测 15 字段：trade_date, ts_code, name,
        close, pct_change, turnover_rate, amount, l_sell, l_buy, l_amount,
        net_amount, net_rate, amount_rate, float_values, reason。"""
        return self._dataset_by_date(
            "top_list", trade_date,
            fields="trade_date,ts_code,name,close,pct_change,turnover_rate,"
                   "amount,l_sell,l_buy,l_amount,net_amount,net_rate,"
                   "amount_rate,float_values,reason",
        )

    def top_inst_by_date(self, trade_date: date) -> pd.DataFrame:
        """龙虎榜机构席位明细。实测 10 字段：trade_date, ts_code, exalter,
        buy, buy_rate, sell, sell_rate, net_buy, side, reason。"""
        return self._dataset_by_date(
            "top_inst", trade_date,
            fields="trade_date,ts_code,exalter,buy,buy_rate,sell,sell_rate,"
                   "net_buy,side,reason",
        )

    # ── 其他榜单 / 统计 ──

    def kpl_list_by_date(self, trade_date: date) -> pd.DataFrame:
        """开盘啦榜单。实测 24 字段：ts_code, name, trade_date, lu_time,
        ld_time, open_time, last_time, lu_desc, tag, theme, net_change,
        bid_amount, status, bid_change, bid_turnover, lu_bid_vol, pct_chg,
        bid_pct_chg, rt_pct_chg, limit_order, amount, turnover_rate,
        free_float, lu_limit_order。"""
        return self._dataset_by_date(
            "kpl_list", trade_date,
            fields="ts_code,name,trade_date,lu_time,ld_time,open_time,"
                   "last_time,lu_desc,tag,theme,net_change,bid_amount,status,"
                   "bid_change,bid_turnover,lu_bid_vol,pct_chg,bid_pct_chg,"
                   "rt_pct_chg,limit_order,amount,turnover_rate,free_float,"
                   "lu_limit_order",
        )

    def daily_info_by_date(self, trade_date: date) -> pd.DataFrame:
        """市场交易统计（每市场分段 1 行/日）。实测 14 字段：trade_date,
        ts_code, ts_name, com_count, total_share, float_share, total_mv,
        float_mv, amount, vol, trans_count, pe, tr, exchange。"""
        return self._dataset_by_date(
            "daily_info", trade_date,
            fields="trade_date,ts_code,ts_name,com_count,total_share,"
                   "float_share,total_mv,float_mv,amount,vol,trans_count,"
                   "pe,tr,exchange",
        )

    # 主要指数白名单：上证/深成/创业板/科创50/北证50
    MAJOR_INDEXES: tuple[str, ...] = (
        "000001.SH", "399001.SZ", "399006.SZ", "000688.SH", "899050.BJ",
    )

    def index_daily_major_by_date(self, trade_date: date) -> pd.DataFrame:
        """主要指数日线（写现有 index_daily_bar 表）。实测 11 字段：ts_code,
        trade_date, close, open, high, low, pre_close, change, pct_chg, vol,
        amount。实测不支持多代码合并请求，逐指数各打 1 次（5 次/日）；
        单指数为空容错跳过（如 899050.BJ 在 2022-11 上市前）。"""
        ds = trade_date.strftime("%Y%m%d")
        fields = (
            "ts_code,trade_date,open,high,low,close,pre_close,"
            "change,pct_chg,vol,amount"
        )
        frames: list[pd.DataFrame] = []
        for code in self.MAJOR_INDEXES:
            df = self._call_with_backoff(
                "index_daily",
                lambda c=code: self._pro.index_daily(
                    ts_code=c, trade_date=ds, fields=fields
                ),
            )
            if df is not None and not df.empty:
                frames.append(df)
            time.sleep(_PAGE_SLEEP)
        if not frames:
            return self._finalize_dataset("index_daily", None)
        return self._finalize_dataset(
            "index_daily", pd.concat(frames, ignore_index=True)
        )
