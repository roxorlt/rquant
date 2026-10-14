from datetime import date

import pandas as pd

from rquant.research import auction_pool as ap
from rquant.research.surge import format_push


def test_filter_candidates():
    df = pd.DataFrame({"ts_code": ["A.SZ", "B.SZ", "C.SZ", "D.BJ", "E.SH"],
                       "high": [11, 11, 11, 11, 11], "low": [10, 11, 10, 10, 10],
                       "fd_amount": [5e6, 1e6, 5e8, 1e6, 1e6], "circ_mv": [1e5] * 5,
                       "is_st": [False, False, False, False, True]})
    # no seal filter any more: only 一字 (B), BJ (D), ST (E) are dropped
    assert list(ap.filter_candidates(df)["ts_code"]) == ["A.SZ", "C.SZ"]


def test_rank_picks_and_store(tmp_path):
    cands = [{"ts_code": c, "name": c, "y_close": 10.0} for c in "ABCDE"]
    q = pd.DataFrame({"ts_code": list("ABCDE"), "price": [10.6, 10.7, 10.3, 10.8, 11.0],
                      "pre_close": [10.0] * 5, "volume": [100, 300, 900, 200, 50]})
    picks = ap.rank_picks(cands, q)  # gap > 4%, all kept, by auction volume
    assert [(p["rank"], p["ts_code"]) for p in picks] == [(1, "B"), (2, "D"), (3, "A"), (4, "E")]
    day = date(2026, 10, 12)
    ap._write(tmp_path, day, {"date": "2026-10-12", "candidates": cands, "picks": picks})
    assert ap.priority_codes(tmp_path, day)[:3] == ["B", "D", "A"]
    assert ap.read_pool(tmp_path)["date"] == "2026-10-12"


def test_push_priority(monkeypatch):
    from rquant.research.surge import Pusher

    assert Pusher.priority() == set()  # auction-pick push paused by default
    evs = [{"ts_code": c, "name": c, "ratio": r, "time": "09:40:00", "acc5": 1.0, "pct": 0.01,
            "amount": 1e8} for c, r in (("X", 9.0), ("P", 3.0))]
    _, body = format_push(evs, {"P"})
    assert body.index("★P") < body.index("X")


def test_limit_open_kept_flagged_and_ths_screen():
    assert ap.limit_price(15.13, "002242.SZ") == 16.64
    assert ap.limit_price(10.0, "300001.SZ") == 12.0
    base = [{"ts_code": c, "name": c, "y_close": 10.0, "seal_ratio": s}
            for c, s in (("A.SZ", 0.001), ("B.SZ", 0.002), ("C.SH", 0.05), ("D.SH", 0.003))]
    cands = base
    q = pd.DataFrame({"ts_code": ["A.SZ", "B.SZ", "C.SH", "D.SH"],
                      "price": [11.0, 10.6, 10.45, 10.6], "pre_close": [10.0] * 4,
                      "volume": [900, 300, 800, 100]})
    picks = ap.rank_picks(cands, q)
    # A opened at limit-up: kept (skip removed) and flagged
    assert [(p["ts_code"], p["limit_open"]) for p in picks] == [
        ("A.SZ", True), ("C.SH", False), ("B.SZ", False), ("D.SH", False)]
    assert "竞价涨停" in ap.RULE_TEXT and "非买入信号" in ap.POOL_LABEL
    ths = ap.ths_screen(base, q)
    assert [(t["ts_code"], t["limit_open"]) for t in ths] == [
        ("A.SZ", True), ("C.SH", False), ("B.SZ", False), ("D.SH", False)]


def test_attribution_offline(monkeypatch):
    from rquant.research import auction_attrib as aa

    monkeypatch.setattr(aa, "ths_concepts", lambda codes: {
        "A": ["影视概念", "AIGC"], "B": ["影视概念", "短剧"], "C": ["化纤"]})
    monkeypatch.setattr(aa, "major_news", lambda s, u: [
        {"title": "A公司AI电影定档", "pub_time": "2026-10-12 08:00:00", "src": "x"}])
    monkeypatch.setattr(aa, "em_news", lambda *a, **k: [])
    monkeypatch.setattr(aa, "em_anns", lambda *a, **k: [])
    pool = {"date": "2026-10-12", "prior_date": "2026-10-09", "rule": "r",
            "picks": [{"ts_code": c, "name": c, "gap": 0.07, "rank": i, "auction_vol": 1e6}
                      for i, c in enumerate("ABC", 1)],
            "ths_screen": [{"ts_code": "D", "name": "D", "gap": 0.1, "limit_open": True}]}
    seen = {}

    def fake(msgs):
        seen["prompt"] = msgs[1]["content"]
        return "AI电影消息带动影视传媒"

    att = aa.attribute(pool, llm=(fake, "m"))
    assert att["text"] == "AI电影消息带动影视传媒" and att["common_concepts"] == ["影视概念"]
    assert "A公司AI电影定档" in seen["prompt"] and len(att["stocks"]) == 4
    fallback = aa.attribute(pool, llm=False)
    assert fallback["text"] == "共同概念：影视概念"
    title, body = aa.format_push({**pool, "attribution": att})
    assert title.endswith("A/B/C") and "D(一字)" in body


def _final_pool(picks):
    return {"date": "2026-10-14", "stage": "final", "candidates": picks + [
        {"ts_code": "Z.SZ", "name": "Z", "y_close": 10.0}], "picks": picks}


def test_lock_after_open_persists_and_never_reranks(tmp_path, monkeypatch):
    from datetime import datetime

    day = date(2026, 10, 14)
    picks = [{"ts_code": c, "name": c, "y_close": 10.0, "rank": i + 1}
             for i, c in enumerate(["B", "A"])]
    ap._write(tmp_path, day, _final_pool(picks))
    pre = datetime(2026, 10, 14, 9, 29, 59, tzinfo=ap.TZ)
    post = datetime(2026, 10, 14, 9, 30, 0, tzinfo=ap.TZ)
    assert not ap.lock_due(day, pre) and ap.lock_due(day, post)
    assert not ap.is_locked(ap.ensure_locked(tmp_path, day, pre))  # 09:29:59: still open
    pool = ap.ensure_locked(tmp_path, day, post)
    assert pool["frozen_members"] == ["B", "A"] and pool["locked_at"].startswith("2026-10-14T09:30")
    on_disk = ap.read_pool(tmp_path, day)
    assert on_disk["frozen_members"] == ["B", "A"] and on_disk["frozen"] is True
    # a later pick run (e.g. after a restart) must not touch membership
    monkeypatch.setattr(ap, "fetch_auction", lambda codes: (_ for _ in ()).throw(AssertionError))
    again = ap.make_picks(tmp_path, day, now=datetime(2026, 10, 14, 10, 0, tzinfo=ap.TZ))
    assert [p["ts_code"] for p in ap.watch_members(again)] == ["B", "A"]
    assert again["locked_at"] == on_disk["locked_at"]


def test_rerank_before_open_then_lock_on_first_post_open_pick(tmp_path, monkeypatch):
    from datetime import datetime

    day = date(2026, 10, 14)
    cands = [{"ts_code": c, "name": c, "y_close": 10.0} for c in "ABC"]
    ap._write(tmp_path, day, {"date": "2026-10-14", "stage": "candidates",
                              "candidates": cands, "base": cands, "picks": []})
    vols = {"v": [100, 300, 200]}
    monkeypatch.setattr(ap, "fetch_auction", lambda codes: pd.DataFrame(
        {"ts_code": list("ABC"), "price": [10.6] * 3, "pre_close": [10.0] * 3,
         "volume": vols["v"]}))
    t1 = datetime(2026, 10, 14, 9, 26, 30, tzinfo=ap.TZ)
    p1 = ap.make_picks(tmp_path, day, now=t1)
    assert not ap.is_locked(p1) and p1["picks"][0]["ts_code"] == "B"
    ap._write(tmp_path, day, p1)
    vols["v"] = [900, 300, 200]  # 09:29 auction data changes the order: allowed
    p2 = ap.make_picks(tmp_path, day, now=datetime(2026, 10, 14, 9, 29, 30, tzinfo=ap.TZ))
    assert not ap.is_locked(p2) and p2["picks"][0]["ts_code"] == "A"
    ap._write(tmp_path, day, p2)
    vols["v"] = [1, 1, 999]  # post-open volume must not matter
    p3 = ap.make_picks(tmp_path, day, now=datetime(2026, 10, 14, 9, 30, 5, tzinfo=ap.TZ))
    assert ap.is_locked(p3) and p3["frozen_members"] == ["A", "B", "C"]


def test_empty_final_not_locked_until_pick_or_day_end(tmp_path):
    from datetime import datetime

    day = date(2026, 10, 14)
    ap._write(tmp_path, day, _final_pool([]))
    assert not ap.is_locked(ap.ensure_locked(
        tmp_path, day, datetime(2026, 10, 14, 9, 31, tzinfo=ap.TZ)))
    assert ap.is_locked(ap.ensure_locked(
        tmp_path, day, datetime(2026, 10, 15, 8, 0, tzinfo=ap.TZ)))


def test_web_lists_only_watch_members(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from rquant.web import routes

    day = date(2026, 10, 14)
    picks = [{"ts_code": f"{i}.SZ", "name": str(i), "y_close": 10.0, "rank": i + 1}
             for i in range(10)]
    pool = _final_pool(picks)
    pool["candidates"] = pool["candidates"] + [
        {"ts_code": f"C{i}.SZ", "name": "c", "y_close": 10.0} for i in range(50)]
    ap._write(tmp_path, day, pool)
    req = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(research_root=tmp_path)))
    item = routes._auction_pool_item(req)
    assert [m.code for m in item.members] == [p["ts_code"] for p in picks]
    assert all(m.detail["stage"] == "pick" for m in item.members)
    assert item.members[0].detail["n_candidates"] == 61
    assert ap.read_pool(tmp_path, day)["frozen_members"] == [p["ts_code"] for p in picks]
