"""Weekly 研报因子 tracking (Sat morning, off-hours).

1. discover: public fxbaogao search for recent broker quant factor reports (last 14 days),
   skip periodic tracking/weekly reports and ones already catalogued, classify replicability by
   keywords, link to an implemented factor family when the title names one; append to
   ``report_factors/catalog_extra.json``.
2. re-run the replication (``report_factors.run``) and diff against last week's snapshot:
   newly passing factors, factors that lost validation or got the decay flag.
3. write ``report_factors/weekly_summary.json`` (stable path, read by /api/v1/report-factors/summary
   and the daily market report) + ``report_factors/weekly/YYYY-MM-DD.json`` history.

Reports that need a new formula are queued as ``pending_impl`` — the formula has to be coded by
hand (one function in report_factors.py); everything else is automatic.
"""

from __future__ import annotations

import json
import re
import shutil
import urllib.request
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rquant.research import report_factors as rf

SEARCH_URL = "https://api.fxbaogao.com/mofoun/report/searchReport/searchNoAuth"
KEYWORDS = ("高频因子", "金工 选股因子", "分钟 因子", "量价因子", "筹码 因子", "资金流 因子",
            "多因子 选股 金工")
SKIP = ("跟踪", "周报", "月报", "日报", "绩效", "精选", "汇总", "年度成果")
QUANT_HINT = ("因子", "选股", "alpha", "Alpha")
FAMILIES = {"聪明钱": "ky_smart_money", "价量相关": "dw_cpv", "CPV": "dw_cpv",
            "均匀度": "dw_utd", "偏度": "ht_skew", "下行波动": "ht_downvol", "APM": "gj_apm",
            "理想振幅": "ky_ideal_amp", "大单": "ht_bigorder_net", "尾盘": "tail_return"}


def classify(title: str) -> str:
    if re.search(r"逐笔|Level-?2|L2|委托|订单|挂单|撤单", title, re.I):
        return "needs_tick_infeasible"
    if re.search(r"成交笔数|单笔|快照|Level-?1", title, re.I):
        return "3s_snapshot_approx"
    if re.search(r"分钟|高频|日内|筹码|尾盘|开盘|集合竞价", title):
        return "5min"
    if re.search(r"资金流|换手|振幅|反转|动量|波动", title):
        return "daily"
    return "unclassified"


def _search(keyword: str, size: int = 20) -> list[dict]:
    body = json.dumps({"keywords": keyword, "pageSize": size, "pageNum": 1, "order": 2}).encode()
    req = urllib.request.Request(SEARCH_URL, data=body, headers={
        "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as r:  # noqa: S310 - fixed https URL
        return json.loads(r.read())["data"]["dataList"] or []


def discover(known: list[dict], days: int = 14,
             search=_search) -> list[dict]:  # noqa: ANN001 - injectable for tests
    cutoff = datetime.now(UTC) - timedelta(days=days)
    titles = {re.sub(r"\W", "", c.get("title", "")) for c in known}
    ids = {c.get("id") for c in known}
    found: dict[str, dict] = {}
    for kw in KEYWORDS:
        try:
            hits = search(kw)
        except Exception as e:  # noqa: BLE001 - one bad keyword must not stop the run
            print("search failed", kw, e)
            continue
        for h in hits:
            title = re.sub(r"<[^>]+>", "", h.get("title") or "")
            pub = datetime.fromtimestamp(int(h.get("pubTime") or 0), UTC)
            fid = f"fxb_{h.get('docId')}"
            norm = re.sub(r"\W", "", title)
            if (pub < cutoff or fid in ids or fid in found or norm in titles
                    or any(s in title for s in SKIP)
                    or not any(q in title for q in QUANT_HINT)):
                continue
            titles.add(norm)
            fam = next((v for k, v in FAMILIES.items() if k in title), None)
            repl = classify(title)
            status = ("linked" if fam else "pending_impl"
                      if repl in ("5min", "daily") else "registered")
            found[fid] = {
                "id": fid, "title": title, "broker": h.get("orgName") or "未知机构",
                "date": str(pub.date()), "url": f"https://www.fxbaogao.com/detail/{h.get('docId')}",
                "formula": "（自动收录，待阅读）", "data": "", "replicability": repl,
                "classic": False, "auto": True, "status": status, "family": fam,
                "note": {"linked": f"同族已实现因子 {fam}，随周度复评跟踪",
                         "pending_impl": "我们数据可做，待实现公式",
                         "registered": "已登记"}[status]}
    return list(found.values())


def diff(prev: dict | None, cur: dict) -> dict:
    p = (prev or {}).get("factors", {})
    out: dict[str, list] = {"new_passing": [], "lost_passing": [], "decayed": [], "changes": []}
    for k, r in cur.get("factors", {}).items():
        o = p.get(k)
        te = r["test"]["rank_ic"]["mean"]
        row = {"id": k, "name": r["name"], "test_rank_ic": te,
               "prev_test_rank_ic": o["test"]["rank_ic"]["mean"] if o else None,
               "validated": r["validated"], "decay": r["decay"]}
        out["changes"].append(row)
        if r["validated"] and not (o and o["validated"]):
            out["new_passing"].append(row)
        if o and o["validated"] and not r["validated"]:
            out["lost_passing"].append(row)
        if r["decay"] and not (o and o["decay"]):
            out["decayed"].append(row)
    return out


def weekly(database: Path, root: Path | None = None, search=_search) -> dict:  # noqa: ANN001
    base = (root or rf.research_root()) / rf.KIND
    base.mkdir(parents=True, exist_ok=True)
    extra_p = base / "catalog_extra.json"
    extra = json.loads(extra_p.read_text()) if extra_p.exists() else []
    new = discover(rf.load_catalog(root), search=search)
    if new:
        tmp = extra_p.with_suffix(".tmp")
        tmp.write_text(json.dumps(extra + new, ensure_ascii=False, indent=1))
        tmp.replace(extra_p)
    snap = base / "results.lastweek.json"
    prev = json.loads(snap.read_text()) if snap.exists() else None
    minute, daily, basic = rf._load_db(database)
    rf.write(rf.run(minute, daily, basic), root)
    cur = rf.read_results(root) or {}
    d = diff(prev, cur)
    summary = {
        "generated_at": datetime.now(UTC).isoformat(), "week_of": str(date.today()),
        "previous_snapshot": prev.get("generated_at") if prev else None,
        "new_reports": [{k: n[k] for k in ("id", "title", "broker", "date", "url",
                                            "replicability", "status", "family")} for n in new],
        "new_passing": d["new_passing"], "lost_passing": d["lost_passing"],
        "decayed": d["decayed"],
        "validated_now": [r["name"] for r in cur.get("factors", {}).values() if r["validated"]],
        "changes": d["changes"],
    }
    summary["headline"] = (
        f"研报挖掘周报：新收录 {len(new)} 篇；新通过 {len(d['new_passing'])}，"
        f"失效 {len(d['lost_passing'])}，新衰减 {len(d['decayed'])}；"
        f"当前有效 {len(summary['validated_now'])} 个")
    (base / "weekly").mkdir(exist_ok=True)
    for p in (base / "weekly" / f"{date.today()}.json", base / "weekly_summary.json"):
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=1))
        tmp.replace(p)
    shutil.copyfile(base / "results.json", snap)
    return summary


def read_summary(root: Path | None = None) -> dict | None:
    p = (root or rf.research_root()) / rf.KIND / "weekly_summary.json"
    try:
        return json.loads(p.read_text()) if p.exists() else None
    except ValueError:
        return None
