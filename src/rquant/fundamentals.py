"""Financial indicators (Tushare fina_indicator), latest report per stock.

Lean: one CLI fetches the latest reported period for a code list and writes one JSON
file under $RQUANT_RESEARCH_ROOT/fundamentals/latest.json; the web API reads it.
    python -m rquant.fundamentals --codes 600519.SH,000001.SZ
"""

from __future__ import annotations

import argparse
import json
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from pydantic import BaseModel

FIELDS = {
    "roe": "ROE %",
    "grossprofit_margin": "毛利率 %",
    "netprofit_margin": "净利率 %",
    "netprofit_yoy": "净利润同比 %",
    "or_yoy": "营收同比 %",
    "debt_to_assets": "资产负债率 %",
    "eps": "每股收益",
    "bps": "每股净资产",
}


class FundamentalRow(BaseModel):
    code: str
    end_date: str
    ann_date: str | None = None
    values: dict[str, float | None]


class FundamentalsDoc(BaseModel):
    fetched_at: datetime
    fields: dict[str, str]
    rows: list[FundamentalRow]


class Pro(Protocol):
    def fina_indicator(self, **kwargs: Any) -> Any: ...


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def fetch(pro: Pro, codes: list[str], pause: float = 0.0) -> list[FundamentalRow]:
    rows: list[FundamentalRow] = []
    for code in codes:
        fields = "ts_code,ann_date,end_date," + ",".join(FIELDS)
        df = pro.fina_indicator(ts_code=code, fields=fields)
        if df is None or len(df) == 0:
            continue
        rec = df.sort_values("end_date", ascending=False).iloc[0].to_dict()
        rows.append(FundamentalRow(
            code=code, end_date=str(rec["end_date"]),
            ann_date=str(rec["ann_date"]) if rec.get("ann_date") else None,
            values={k: _num(rec.get(k)) for k in FIELDS}))
        if pause:
            time.sleep(pause)
    return rows


def path(root: Path) -> Path:
    return root / "fundamentals" / "latest.json"


def save(root: Path, rows: list[FundamentalRow]) -> Path:
    p = path(root)
    p.parent.mkdir(parents=True, exist_ok=True)
    doc = FundamentalsDoc(fetched_at=datetime.now(UTC), fields=FIELDS, rows=rows)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(doc.model_dump_json(indent=1), encoding="utf-8")
    tmp.replace(p)
    return p


def read(root: Path) -> FundamentalsDoc | None:
    p = path(root)
    if not p.exists():
        return None
    return FundamentalsDoc.model_validate(json.loads(p.read_text(encoding="utf-8")))


def main(argv: list[str] | None = None) -> int:
    from rquant.backtest.store import research_root

    ap = argparse.ArgumentParser(prog="python -m rquant.fundamentals")
    ap.add_argument("--codes", default="")
    ap.add_argument("--pause", type=float, default=0.3, help="seconds between calls")
    args = ap.parse_args(argv)
    codes = [c.strip() for c in args.codes.split(",") if c.strip()]
    if not codes:
        ap.error("--codes is required")
    import tushare as ts

    token = os.environ.get("TUSHARE_TOKEN_MAIN") or os.environ.get("TUSHARE_TOKEN", "")
    rows = fetch(ts.pro_api(token), codes, pause=args.pause)
    print(save(research_root(), rows), len(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
