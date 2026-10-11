"""Machine-readable daily summary of every macro indicator (for the daily report).

Written to ``<research_root>/macro/daily/YYYY-MM-DD.json`` (as-of trade date) and
``<research_root>/macro/daily/latest.json``. Schema (version 1)::

    {"schema": "rquant.macro.summary/1", "as_of": "YYYY-MM-DD", "generated_at": ISO-8601,
     "indicators": [{"key", "label", "group", "date", "value", "unit",
                     "chg_1d", "chg_5d", "chg_20d", "pct", "regime", "note"}],
     "studies": [{"study", "horizon", "n", "mean", "median", "win", "base_mean", "base_win"}]}

``pct`` is the full-history percentile of ``value`` (0–1, in-sample, descriptive only);
``regime`` is a short Chinese label; changes are in the indicator's own unit.
"""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

SCHEMA = "rquant.macro.summary/1"


def _f(v: Any) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(x) else round(x, 6)


def _ind(
    key: str,
    label: str,
    group: str,
    s: pd.Series,
    unit: str = "",
    regime: Any = None,
    note: str = "",
) -> dict | None:
    s = s.dropna()
    if s.empty:
        return None
    v = s.iloc[-1]

    def chg(n: int) -> float | None:
        return _f(v - s.iloc[-1 - n]) if len(s) > n else None

    pct = float((s <= v).mean())
    reg = regime(v, pct) if callable(regime) else regime
    return dict(
        key=key,
        label=label,
        group=group,
        date=str(s.index[-1]),
        value=_f(v),
        unit=unit,
        chg_1d=chg(1),
        chg_5d=chg(5),
        chg_20d=chg(20),
        pct=_f(pct),
        regime=reg,
        note=note,
    )


def _band(lo_txt: str, mid_txt: str, hi_txt: str):  # noqa: ANN202
    return lambda v, p: lo_txt if p < 0.2 else hi_txt if p > 0.8 else mid_txt


def build(derived: dict[str, pd.DataFrame]) -> dict:
    out: list[dict | None] = []
    d = derived["macro_daily"].set_index("trade_date")
    b = (
        derived["macro_buffett"]
        .assign(trade_date=lambda x: pd.to_datetime(x.trade_date).dt.strftime("%Y%m%d"))
        .set_index("trade_date")
    )
    out.append(
        _ind(
            "buffett",
            "巴菲特指标(总市值/GDP)",
            "估值",
            b.ratio,
            "ratio",
            _band("低估", "中性", "高估"),
        )
    )
    out.append(
        _ind(
            "erp",
            "股债利差(沪深300)",
            "估值",
            d.erp,
            "%",
            _band("股票相对贵", "中性", "股票相对便宜"),
        )
    )
    out.append(_ind("pe_ttm", "沪深300 PE_TTM", "估值", d.pe_ttm, "x"))
    out.append(_ind("cn10y", "10年国债收益率", "利率", d.cn10y, "%"))
    out.append(_ind("rzrqye", "两融余额", "资金", d.rzrqye / 1e8, "亿元"))
    out.append(
        _ind(
            "fin_buy_share",
            "融资买入占成交",
            "资金",
            d.fin_buy_share,
            "ratio",
            _band("杠杆情绪低", "中性", "杠杆情绪高"),
        )
    )
    out.append(
        _ind(
            "north_turnover",
            "北向成交额(2024-08后口径)",
            "资金",
            d.north_turnover / 100,
            "亿元",
            note="2024-08-19 起不再披露净买入",
        )
    )
    e = derived["macro_etf_share"]
    for code in ["510300.SH", "510050.SH", "510500.SH", "159915.SZ", "588000.SH"]:
        g = e[e.ts_code == code].set_index("trade_date").fd_share / 1e4
        out.append(_ind(f"etf_share_{code}", f"ETF份额 {code}", "国家队代理", g, "亿份"))
    if "macro_breadth" in derived:
        br = derived["macro_breadth"].set_index("trade_date")
        out.append(
            _ind(
                "mcclellan",
                "麦克莱伦振荡指标",
                "宽度",
                br.mcclellan,
                "",
                lambda v, p: "偏强" if v > 0 else "偏弱",
            )
        )
        out.append(
            _ind(
                "pct_ma20",
                "MA20上方占比",
                "宽度",
                br.pct_ma20,
                "ratio",
                lambda v, p: "超卖" if v < 0.15 else "超买" if v > 0.85 else "中性",
            )
        )
        out.append(_ind("pct_ma60", "MA60上方占比", "宽度", br.pct_ma60, "ratio"))
        out.append(_ind("nh_nl", "250日新高-新低家数", "宽度", br.nh_nl, "家"))
    o = derived["macro_option_daily"]
    for und in ["50ETF", "沪300ETF", "沪500ETF", "创业板ETF"]:
        g = o[o.und == und].set_index("trade_date")
        if g.empty:
            continue
        last = g.iloc[-1]
        pos = (
            "跌破下轨"
            if last.pcr_vol < last.pcr_vol_lo
            else "突破上轨"
            if last.pcr_vol > last.pcr_vol_up
            else "带内"
        )
        out.append(_ind(f"pcr_vol_{und}", f"{und} PCR(量)", "期权", g.pcr_vol, "", pos))
        out.append(_ind(f"pcr_oi_{und}", f"{und} PCR(持仓)", "期权", g.pcr_oi, ""))
        out.append(
            _ind(
                f"atm_iv_{und}",
                f"{und} 平值IV",
                "期权",
                g.atm_iv,
                "ratio",
                _band("低波", "中性", "高波"),
            )
        )
        out.append(_ind(f"vol_surge_{und}", f"{und} 成交放量倍数", "期权", g.vol_surge, "x"))
    f = derived.get("macro_futures")
    if f is not None:
        for sym in ["IF", "IH", "IC", "IM"]:
            g = f[f.sym == sym].set_index("trade_date")
            out.append(
                _ind(
                    f"basis_{sym}",
                    f"{sym}主力基差",
                    "期指",
                    g.basis_pct,
                    "ratio",
                    lambda v, p: "升水" if v > 0 else "贴水",
                )
            )
            out.append(_ind(f"net_{sym}", f"{sym} 前20会员净多", "期指", g.net, "手"))
    hk = derived["macro_hk"].set_index("trade_date")
    out.append(
        _ind(
            "hk_smart_gap",
            "SMARTMONEY−MA20(恒生科技,近似)",
            "港股",
            hk.smart - hk.smart_ma20,
            "",
            lambda v, p: "SM在MA20上" if v > 0 else "SM在MA20下",
        )
    )
    out.append(
        _ind(
            "hk_signal",
            "定价权C近似信号",
            "港股",
            hk.signal.astype(float),
            "",
            lambda v, p: "触发" if v else "未触发",
        )
    )
    inds = [i for i in out if i]
    as_of = max(i["date"] for i in inds)
    st = derived.get("macro_study", pd.DataFrame())
    studies = [
        {k: (_f(v) if not isinstance(v, str) else v) for k, v in r.items()}
        for r in st.to_dict("records")
    ]
    return dict(
        schema=SCHEMA,
        as_of=f"{as_of[:4]}-{as_of[4:6]}-{as_of[6:8]}",
        generated_at=datetime.now(UTC).isoformat(timespec="seconds"),
        indicators=inds,
        studies=studies,
    )


def write_summary(root: Path, summary: dict) -> Path:
    d = root / "macro" / "daily"
    d.mkdir(parents=True, exist_ok=True)
    text = json.dumps(summary, ensure_ascii=False, indent=1)
    p = d / f"{summary['as_of']}.json"
    for target in (p, d / "latest.json"):
        tmp = target.with_suffix(".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(target)
    return p
