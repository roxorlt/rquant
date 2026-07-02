"""集合竞价跳空策略特征归因（roadmap #10）。

输入：`rquant auction-gap-minute-replay --output <csv>` 的交易明细。
输出：markdown 归因报告（结局拆解 / 单变量分层 / 过滤规则搜索 + 时间外验证）。

方法约束：
- 训练/验证按时间切分（默认 2025-12-31），过滤阈值只在训练段选取，
  验证段一次性评估、全量汇报（不挑好看的）——防止把噪声当规律。
- 上板(b_hit_limit_up_today)是结局不是特征，只用于归因解释，
  不允许进过滤规则。

用法：
    .venv/bin/python scripts/analyze_auction_gap_attribution.py <replay.csv> [--split 2025-12-31]
"""

from __future__ import annotations

import argparse
import itertools
from pathlib import Path

import numpy as np
import pandas as pd

# 入场时点已知的候选特征（结局类字段绝不能出现在这里）
ENTRY_FEATURES: list[str] = [
    "auction_vol_ratio_5d",
    "auction_amount",
    "auction_turnover_rate",
    "auction_gap_pct_close",
    "auction_entry_to_limit_up_pct",
    "entry_signal_limit_progress",
    "entry_signal_minute_amount",
    "entry_signal_cum_amount_asof",
    "entry_signal_rel_amount_same_minute_20d",
    "entry_signal_rel_cum_amount_asof_20d",
    "entry_signal_opening_segment_amount",
    "entry_signal_amount_accel_5m",
    "entry_signal_amount_accel_10m",
    "entry_minute",
    # T-1 日市场温度（join market_sentiment_daily，入场时点可知）
    "mkt_high_60d_ratio_pct",
    "mkt_above_ma20_ratio_pct",
    "mkt_limit_up_count",
    "mkt_up_ratio_pct",
]

MIN_TRAIN_TRADES = 120  # 过滤后训练段最少笔数，低于此视为样本不足
MIN_VALID_COVERAGE = 500  # 特征非空样本低于此不参与规则搜索


def load(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df["buy_date"] = pd.to_datetime(df["buy_date"])
    df["win"] = df["ret_pct"] > 0
    df["sealed"] = (df["b_hit_limit_up_today"] == True) & (  # noqa: E712
        df["b_close_at_limit_up"] == True  # noqa: E712
    )
    df["touched_not_sealed"] = (df["b_hit_limit_up_today"] == True) & ~df["sealed"]  # noqa: E712
    et = pd.to_datetime(df["entry_signal_time"])
    df["entry_minute"] = et.dt.hour * 60 + et.dt.minute
    return df


def spearman(a: pd.Series, b: pd.Series) -> float:
    return float(np.corrcoef(a.rank(), b.rank())[0, 1])


def outcome_table(df: pd.DataFrame) -> pd.DataFrame:
    def bucket(r: pd.Series) -> str:
        if r["sealed"]:
            return "封住涨停"
        if r["touched_not_sealed"]:
            return "触板未封(炸板)"
        return "未触板"

    g = df.assign(outcome=df.apply(bucket, axis=1)).groupby("outcome")["ret_pct"]
    out = g.agg(
        trades="count",
        mean_ret="mean",
        median_ret="median",
        win_pct=lambda s: (s > 0).mean() * 100,
    ).round(2)
    return out


def univariate_table(train: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for f in ENTRY_FEATURES:
        if f not in train.columns:
            rows.append((f, 0, np.nan, np.nan, np.nan, "列不存在"))
            continue
        s = pd.to_numeric(train[f], errors="coerce")
        valid = s.notna()
        n = int(valid.sum())
        if n < MIN_VALID_COVERAGE:
            rows.append((f, n, np.nan, np.nan, np.nan, "覆盖不足"))
            continue
        sub = train[valid].copy()
        sub["q"] = pd.qcut(s[valid], 5, labels=False, duplicates="drop")
        byq = sub.groupby("q").agg(
            ret=("ret_pct", "mean"), sealed=("sealed", "mean")
        )
        rows.append(
            (
                f,
                n,
                round(spearman(s[valid], sub["ret_pct"]), 3),
                round(byq["ret"].iloc[-1] - byq["ret"].iloc[0], 2),
                round((byq["sealed"].iloc[-1] - byq["sealed"].iloc[0]) * 100, 1),
                "",
            )
        )
    return pd.DataFrame(
        rows,
        columns=["feature", "n", "spearman_ret", "Q5-Q1收益差", "Q5-Q1封板率差pp", "备注"],
    ).sort_values("spearman_ret", key=lambda c: c.abs(), ascending=False)


def _apply_rule(df: pd.DataFrame, rule: list[tuple[str, str, float]]) -> pd.Series:
    mask = pd.Series(True, index=df.index)
    for feat, op, thr in rule:
        s = pd.to_numeric(df[feat], errors="coerce")
        mask &= (s >= thr) if op == ">=" else (s <= thr)
    return mask


def search_rules(
    train: pd.DataFrame, valid: pd.DataFrame, top_k: int = 5
) -> pd.DataFrame:
    """单特征 + 双特征阈值网格，训练段选优、验证段全量汇报。"""
    usable = [
        f
        for f in ENTRY_FEATURES
        if f in train.columns
        and pd.to_numeric(train[f], errors="coerce").notna().sum() >= MIN_VALID_COVERAGE
    ]
    quantiles = [0.2, 0.4, 0.6, 0.8]
    candidates: list[list[tuple[str, str, float]]] = []
    for f in usable:
        s = pd.to_numeric(train[f], errors="coerce")
        for q in quantiles:
            thr = float(s.quantile(q))
            candidates.append([(f, ">=", thr)])
            candidates.append([(f, "<=", thr)])
    for f1, f2 in itertools.combinations(usable, 2):
        s1 = pd.to_numeric(train[f1], errors="coerce")
        s2 = pd.to_numeric(train[f2], errors="coerce")
        for q1, q2 in itertools.product([0.4, 0.6], repeat=2):
            for op1, op2 in itertools.product([">=", "<="], repeat=2):
                candidates.append(
                    [
                        (f1, op1, float(s1.quantile(q1))),
                        (f2, op2, float(s2.quantile(q2))),
                    ]
                )

    scored = []
    for rule in candidates:
        m = _apply_rule(train, rule)
        n = int(m.sum())
        if n < MIN_TRAIN_TRADES:
            continue
        scored.append((rule, n, train.loc[m, "ret_pct"].mean()))
    scored.sort(key=lambda x: x[2], reverse=True)

    picked: list[tuple] = []
    seen_feats: set[frozenset] = set()
    for rule, n, ret in scored:
        key = frozenset(f for f, _, _ in rule)
        if key in seen_feats:
            continue
        seen_feats.add(key)
        picked.append((rule, n, ret))
        if len(picked) >= top_k:
            break

    rows = []
    for rule, n_train, ret_train in picked:
        mv = _apply_rule(valid, rule)
        nv = int(mv.sum())
        rows.append(
            {
                "规则": " 且 ".join(f"{f} {op} {thr:.3g}" for f, op, thr in rule),
                "训练笔数": n_train,
                "训练均收益%": round(ret_train, 2),
                "训练胜率%": round(
                    train.loc[_apply_rule(train, rule), "win"].mean() * 100, 1
                ),
                "验证笔数": nv,
                "验证均收益%": round(valid.loc[mv, "ret_pct"].mean(), 2) if nv else np.nan,
                "验证胜率%": round(valid.loc[mv, "win"].mean() * 100, 1) if nv else np.nan,
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("csv", type=Path)
    ap.add_argument("--split", type=str, default="2025-12-31")
    args = ap.parse_args()

    df = load(args.csv)
    train = df[df["buy_date"] <= args.split]
    valid = df[df["buy_date"] > args.split]

    print(f"## 数据集\n\n共 {len(df)} 笔；训练 {len(train)}（≤{args.split}）/ 验证 {len(valid)}\n")
    print(
        f"基准：均收益 {df.ret_pct.mean():.2f}% / 胜率 {df.win.mean()*100:.1f}% / "
        f"中位 {df.ret_pct.median():.2f}%\n"
    )
    print("## B 日结局拆解（全样本）\n")
    print(outcome_table(df).to_markdown(), "\n")
    print("## 单变量分层（训练段，按 |spearman| 排序）\n")
    print(univariate_table(train).to_markdown(index=False), "\n")
    print("## 过滤规则搜索（训练段选优 → 验证段一次性评估）\n")
    baseline_v = valid.ret_pct.mean()
    win_v = valid.win.mean() * 100
    print(f"验证段基准：{len(valid)} 笔 / 均收益 {baseline_v:.2f}% / 胜率 {win_v:.1f}%\n")
    print(search_rules(train, valid).to_markdown(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
