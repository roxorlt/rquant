# ruff: noqa: E501
from datetime import date

import numpy as np
import pandas as pd

from rquant.risk import abnormal as ab
from rquant.risk import neg_filter as nf


def _frames(rets: dict[str, list[float]], n: int):
    days = pd.bdate_range("2026-09-01", periods=n)
    sr = pd.DataFrame(rets, index=days)
    ir = pd.DataFrame({ix: np.zeros(n) for ix in ab.BOARD_INDEX.values()}, index=days)
    return sr, ir


def test_main_board_abnormal_trigger_and_reset() -> None:
    # +10%/day vs flat index: day1 dev 10, day2 21 -> trigger (>=20), reset; day3 window restarts
    sr, ir = _frames({"600001.SH": [0.0] * 5 + [0.1, 0.1, 0.1, 0.0, 0.0]}, 10)
    r = ab.compute(sr, ir)
    a = r.abnormal["600001.SH"].tolist()
    assert a[6] == 1 and a[7] == 0  # day after trigger: counter restarted (10% only)
    assert round(r.dev3["600001.SH"].iloc[7], 6) == 10.0
    assert r.count10_up["600001.SH"].iloc[-1] == 1


def test_gem_threshold_30_and_near_flag() -> None:
    sr, ir = _frames({"300001.SZ": [0.0] * 8 + [0.05, 0.10]}, 10)
    r = ab.compute(sr, ir)
    assert r.abnormal["300001.SZ"].iloc[-1] == 0          # 15.5% < 30
    assert bool(r.near_abn_up["300001.SZ"].iloc[-1])      # +20% tomorrow -> ≥30


def test_announcement_resets_counter() -> None:
    sr, ir = _frames({"600002.SH": [0.0] * 7 + [0.09, 0.09, 0.0]}, 10)
    ann = pd.DataFrame(False, index=sr.index, columns=sr.columns)
    ann.iloc[7, 0] = True
    r = ab.compute(sr, ir, announced=ann)
    assert r.abnormal["600002.SH"].iloc[8] == 0  # 9% (window restarted after the announcement)


def test_same_direction_count_severe_gem() -> None:
    # three separate +30% bursts within 10 days on GEM -> severe (3 same-direction abnormal)
    rets = [0.0] * 3 + [0.2, 0.15, 0.0, 0.2, 0.15, 0.0, 0.2, 0.15]
    sr, ir = _frames({"300002.SZ": rets}, len(rets))
    r = ab.compute(sr, ir)
    assert int((r.abnormal["300002.SZ"] == 1).sum()) == 3
    assert bool(r.severe["300002.SZ"].iloc[-1])


def test_apply_policy_and_hard_filter() -> None:
    cands = pd.DataFrame({"ts_code": ["A", "B", "C"]})
    flags = pd.DataFrame({"ts_code": ["A", "B", "B"], "flag": ["st", "abn_triggered", "holder_reduce"],
                          "severity": ["hard", "soft", "info"]})
    d = nf.apply(cands, flags)
    assert d["risk_hard"].tolist() == [True, False, False]
    assert d["risk_score"].tolist() == [0.0, 1.0, 0.0]
    s = nf.apply(cands, flags, policy="short_term")
    assert s["risk_score"].tolist() == [0.0, 0.0, 0.0]
    assert nf.filter_hard(cands, flags)["ts_code"].tolist() == ["B", "C"]


def test_calendar_tags() -> None:
    td = [d.date() for d in pd.bdate_range("2026-09-21", "2026-10-31")
          if not (date(2026, 10, 1) <= d.date() <= date(2026, 10, 8))]
    assert "pre_holiday_1d" in nf.calendar_tags(date(2026, 9, 30), td)
    assert "month_end_3d" in nf.calendar_tags(date(2026, 9, 30), td)
    assert "quarter_end_3d" in nf.calendar_tags(date(2026, 9, 29), td)
    assert "pre_disclosure_deadline" in nf.calendar_tags(date(2026, 10, 28), td)


def test_load_flags_point_in_time() -> None:
    import duckdb

    con = duckdb.connect()
    df = pd.DataFrame({"trade_date": [date(2026, 10, 13), date(2026, 10, 14)], "ts_code": ["A", "B"],
                       "flag": ["st", "st"], "severity": ["hard", "hard"], "detail": ["", ""],
                       "source": ["", ""], "valid_until": [date(2026, 10, 20), date(2026, 10, 20)]})
    nf.store(con, df)
    got = nf.load_risk_flags(con, before=date(2026, 10, 14))
    assert got["ts_code"].tolist() == ["A"]  # flags built on D are not visible for decisions on D
