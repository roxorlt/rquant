"""rQuant 健康检查：一条命令看全链路状态。

    uv run python scripts/status.py

人类可读输出，不懂代码也能看懂。
"""

from __future__ import annotations

from datetime import datetime

from rquant.config import settings
from rquant.storage import DuckDBStore


def _banner(title: str) -> None:
    bar = "═" * 60
    print(f"\n{bar}\n  {title}\n{bar}")


def _line(label: str, value: str, mark: str = "") -> None:
    suffix = f"  {mark}" if mark else ""
    print(f"  {label:<18} {value}{suffix}")


def main() -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _banner(f"rQuant 健康检查 — {now}")

    print("\n【配置】")
    _line("Tushare 主 token", settings.tushare_token_main[:8] + "…（2000 积分）", "✓")
    backup = "已配置" if settings.tushare_token_backup else "未配置"
    _line("Tushare 备 token", backup)
    _line("数据目录", str(settings.data_dir))
    _line("环境", settings.app_env)

    if not settings.duckdb_path.exists():
        print(f"\n⚠ DuckDB 未创建：{settings.duckdb_path}")
        print("  先跑：uv run python scripts/ingest_daily.py --codes 000001.SZ --start 2024-12-01 --end 2024-12-31")
        return

    size_mb = settings.duckdb_path.stat().st_size / 1024 / 1024
    _line("DuckDB 文件", f"{settings.duckdb_path.name} ({size_mb:.2f} MB)", "✓")

    print("\n【数据库】")
    with DuckDBStore() as s:
        tables = s.query("SHOW TABLES")
        _line("表", ", ".join(tables["name"].tolist()))

        daily_count = s.count_daily()
        basic_count = s.query("SELECT COUNT(*) AS c FROM stock_basic")["c"].iloc[0]
        factor_count = s.count_adj_factor()
        ind_count = s.count_indicators()
        _line("日线 daily_bar", f"{daily_count:,} 行")
        _line("因子 adj_factor", f"{factor_count:,} 行")
        _line("指标 daily_indicator", f"{ind_count:,} 行")
        _line("基础 stock_basic", f"{int(basic_count):,} 行")

        if daily_count == 0:
            print("\n⚠ 尚无日线数据，跑一次 ingest_daily.py 试试")
            return

        codes = s.query(
            """
            SELECT ts_code, COUNT(*) AS n,
                   strftime(MIN(trade_date), '%Y-%m-%d') AS first_day,
                   strftime(MAX(trade_date), '%Y-%m-%d') AS last_day
            FROM daily_bar
            GROUP BY ts_code
            ORDER BY ts_code
            """
        )
        print("\n【已入库股票】")
        for _, row in codes.iterrows():
            _line(str(row["ts_code"]), f"{row['n']} 日  {row['first_day']} ~ {row['last_day']}")

        latest = s.query(
            """
            SELECT ts_code,
                   strftime(trade_date, '%Y-%m-%d') AS trade_date,
                   close, pct_chg, vol, amount
            FROM daily_bar
            WHERE trade_date = (SELECT MAX(trade_date) FROM daily_bar)
            ORDER BY ts_code
            """
        )
        print("\n【最新收盘样本 — 原始价】")
        for _, row in latest.iterrows():
            amount_yi = row["amount"] / 1e5
            print(
                f"  {row['ts_code']}  {row['trade_date']}  "
                f"收盘 {row['close']:>8.2f}  "
                f"涨跌 {row['pct_chg']:>+6.2f}%  "
                f"成交额 {amount_yi:>6.2f} 亿元"
            )

        if factor_count > 0:
            print("\n【前复权对比】（东方财富默认显示前复权 → 用 qfq_close 比对）")
            print("  最新日 factor == ref，qfq 必等原始；取首日看真实效果：\n")
            code_list = codes["ts_code"].tolist()
            for code in code_list:
                df_qfq = s.get_daily_qfq(code)
                if df_qfq.empty:
                    continue
                first, last = df_qfq.iloc[0], df_qfq.iloc[-1]
                print(
                    f"  {code}  {first['trade_date']}  "
                    f"原始 {first['raw_close']:>8.2f}  →  "
                    f"前复权 {first['qfq_close']:>8.2f}  "
                    f"(factor={first['adj_factor']:.4f} / ref={first['ref_factor']:.4f})"
                )
                print(
                    f"  {code}  {last['trade_date']}  "
                    f"原始 {last['raw_close']:>8.2f}  →  "
                    f"前复权 {last['qfq_close']:>8.2f}  "
                    f"(factor={last['adj_factor']:.4f} / ref={last['ref_factor']:.4f})"
                )
                print()

        if ind_count > 0:
            print("\n【最新技术指标】（基于前复权价）")
            latest_ind = s.query(
                """
                SELECT ts_code,
                       strftime(trade_date,'%Y-%m-%d') AS trade_date,
                       ma5, ma10, ma20, ma60,
                       rsi6, rsi14,
                       macd, macd_signal, macd_hist,
                       kdj_k, kdj_d, kdj_j
                FROM daily_indicator
                WHERE trade_date = (SELECT MAX(trade_date) FROM daily_indicator)
                ORDER BY ts_code
                """
            )
            for _, r in latest_ind.iterrows():
                print(f"\n  {r['ts_code']}  {r['trade_date']}")
                print(
                    f"    MA:   5={r['ma5']:>8.2f}  10={r['ma10']:>8.2f}  "
                    f"20={r['ma20']:>8.2f}  60={r['ma60']:>8.2f}"
                )
                print(
                    f"    RSI:  6={r['rsi6']:>6.2f}   14={r['rsi14']:>6.2f}    "
                    f"(>70 超买 / <30 超卖)"
                )
                print(
                    f"    MACD: {r['macd']:>7.2f}  signal={r['macd_signal']:>7.2f}  "
                    f"hist={r['macd_hist']:>+7.2f}  "
                    f"({'多头' if r['macd_hist'] > 0 else '空头'})"
                )
                print(
                    f"    KDJ:  K={r['kdj_k']:>6.2f}  D={r['kdj_d']:>6.2f}  "
                    f"J={r['kdj_j']:>6.2f}  "
                    f"({'金叉(K>D)' if r['kdj_k'] > r['kdj_d'] else '死叉(K<D)'})"
                )

    print("\n✓ 所有基础设施就绪\n")


if __name__ == "__main__":
    main()
