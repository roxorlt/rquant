import time

from rquant.research import report_tracking as rt


def _hit(doc, title, days_ago=1):
    pub = int(time.time()) - days_ago * 86400
    return {"docId": doc, "title": title, "orgName": "某证券", "pubTime": pub}


def test_discover_filters_and_classifies():
    hits = [_hit(1, "<em>高频因子</em>（二十）：分钟级聪明钱改进"),
            _hit(2, "高频因子跟踪：上周斜率凸性因子表现优异"),  # tracking report -> skip
            _hit(3, "基于逐笔委托的订单流因子"),
            _hit(4, "日内收益分布选股因子", days_ago=40),  # too old
            _hit(5, "宏观经济周期研究")]  # not quant factor
    out = rt.discover([{"id": "x", "title": "已有"}], search=lambda kw: hits)
    by = {o["id"]: o for o in out}
    assert set(by) == {"fxb_1", "fxb_3"}
    assert by["fxb_1"]["status"] == "linked" and by["fxb_1"]["family"] == "ky_smart_money"
    assert by["fxb_3"]["replicability"] == "needs_tick_infeasible"


def test_diff_flags():
    def r(v, d, ic):
        return {"name": "n", "validated": v, "decay": d, "test": {"rank_ic": {"mean": ic}}}
    prev = {"factors": {"a": r(True, False, 0.05), "b": r(False, False, 0.0)}}
    cur = {"factors": {"a": r(False, True, 0.0), "b": r(True, False, 0.06),
                       "c": r(True, False, 0.1)}}
    d = rt.diff(prev, cur)
    assert [x["id"] for x in d["lost_passing"]] == ["a"]
    assert [x["id"] for x in d["decayed"]] == ["a"]
    assert sorted(x["id"] for x in d["new_passing"]) == ["b", "c"]
