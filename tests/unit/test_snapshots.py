from __future__ import annotations

import json
from datetime import time as dtime

import pandas as pd

from rquant.research import snapshots as s


def _frame(codes, src="sina", vol=100.0):
    rows = [{"TS_CODE": c, "NAME": c, "DATE": "20261009", "TIME": "15:00:00", "PRICE": 10.0,
             "VOLUME": vol, "AMOUNT": vol * 10, "B1_P": 9.99, "B1_V": 5, "A1_P": 10.01,
             "A1_V": 7} for c in codes]
    return s._normalize(pd.DataFrame(rows), src)


def test_sessions():
    assert s.in_session(dtime(9, 15)) and s.in_session(dtime(14, 59))
    assert not s.in_session(dtime(12, 0)) and not s.in_session(dtime(9, 0))
    assert s.next_session_start(dtime(12, 0)) == dtime(13, 0)
    assert s.next_session_start(dtime(15, 5)) is None


def test_universe_priority_and_cap(tmp_path, monkeypatch):
    cmd = tmp_path / "cmd" / "watchlist"
    cmd.mkdir(parents=True)
    (cmd / "items.jsonl").write_text(
        json.dumps({"ts_code": "000001.SZ"}) + "\nbad\n"
        + json.dumps({"ts_code": "600000.SH"}) + "\n")
    cond = tmp_path / "research" / "condition" / "x"
    cond.mkdir(parents=True)
    (cond / "result.json").write_text(json.dumps(
        {"trade_date": "2026-10-09", "hits": [{"code": "300750.SZ"}, {"code": "000001.SZ"}]}))
    monkeypatch.setenv("RQUANT_WEB_COMMANDS_DIR", str(tmp_path / "cmd"))
    monkeypatch.setenv("RQUANT_RESEARCH_ROOT", str(tmp_path / "research"))
    monkeypatch.delenv("RQUANT_SERVING_ROOT", raising=False)
    monkeypatch.setenv("RQUANT_SNAPSHOT_EXTRA", "688981.SH,junk")
    u = s.build_universe(cap=3)
    assert u["codes"] == ["688981.SH", "600000.SH", "000001.SZ"]
    assert u["counts"]["condition"] == 2


def test_poller_falls_back_and_counts_failures(monkeypatch):
    def bad(codes):
        raise RuntimeError("boom")

    p = s.Poller(["600000.SH", "000001.SZ"], sina=bad, dc=lambda c: _frame(c, "dc"))
    df = p.cycle()
    assert set(df["src"]) == {"dc"} and len(df) == 2 and p.failed_cycles == 0
    p2 = s.Poller(["600000.SH"], sina=bad, dc=bad)
    p2.cycle()
    p2.cycle()
    assert p2.failed_cycles == 2 and p2.backoff() > s.INTERVAL


def test_parts_compact_dedup(tmp_path):
    d = tmp_path / "2026-10-09"
    p = s.Poller(["600000.SH"], sina=lambda c: _frame(c))
    s.write_part(d, [p.cycle(), p.cycle()])  # identical snapshots
    s.write_part(d, [s.Poller(["600000.SH"], sina=lambda c: _frame(c, vol=200.0)).cycle()])
    info = s.compact(d)
    assert info["raw_rows"] == 3 and info["rows"] == 2
    assert not list((d / "quotes").glob("part-*"))
    assert len(pd.read_parquet(d / "quotes.parquet")) == 2


def test_ticks(tmp_path):
    d = tmp_path / "2026-10-09"
    d.mkdir()
    df = pd.DataFrame({"TIME": ["09:30:00"], "PRICE": [1.0], "VOLUME": [1], "TYPE": ["买盘"]})
    info = s.fetch_ticks(d, ["600000.SH"], fetch=lambda c: df, pause=0)
    assert info["rows"] == 1 and (d / "ticks.parquet").is_file()
