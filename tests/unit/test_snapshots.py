from __future__ import annotations

import json
import time
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
    assert p.token is None
    df = p.cycle()
    assert set(df["src"]) == {"dc"} and len(df) == 2 and p.failed_cycles == 0
    p2 = s.Poller(["600000.SH"], sina=bad, dc=bad)
    p2.cycle()
    p2.cycle()
    assert p2.failed_cycles == 2 and p2.backoff() > s.INTERVAL


def test_parts_compact_dedup(tmp_path):
    d = tmp_path / "2026-10-09"
    dedup = s.Deduper()
    p = s.Poller(["600000.SH"], sina=lambda c: _frame(c))
    s.write_part(d, [dedup(p.cycle()), dedup(p.cycle())])  # identical snapshots
    s.write_part(d, [dedup(s.Poller(["600000.SH"], sina=lambda c: _frame(c, vol=200.0)).cycle())])
    info = s.compact(d)
    assert info["rows"] == 2
    assert not list((d / "quotes").glob("part-*"))
    assert len(pd.read_parquet(d / "quotes.parquet")) == 2


def test_ticks(tmp_path):
    d = tmp_path / "2026-10-09"
    d.mkdir()
    df = pd.DataFrame({"TIME": ["09:30:00"], "PRICE": [1.0], "VOLUME": [1], "TYPE": ["买盘"]})
    info = s.fetch_ticks(d, ["600000.SH"], fetch=lambda c: df, pause=0)
    assert info["rows"] == 1 and (d / "ticks.parquet").is_file()


def _ts_items(api):
    if api == "rt_tick":
        row = ["600000.SH", "浦发", "2026-10-09 15:00:03", 9.54, 9.7, 9.74, 9.7, 9.54, 1e6, 9e6,
               100.0] + [9.55, 100.0, 9.54, 200.0] * 5
        return pd.DataFrame([row], columns=s.TS_TICK_FIELDS)
    row = ["600000.SH", "浦发", "2026-10-09 15:00:03", 9.54, 9.7, 9.74, 9.7, 9.54, 1e6, 9e6,
           100.0, 9.55, 100.0, 9.54, 200.0]
    return pd.DataFrame([row], columns=s.TS_K_FIELDS)


def test_tushare_first_alternates_tick_and_k(tmp_path, monkeypatch):
    calls = []

    def ts_call(api, params, fields, token):
        calls.append(api)
        assert params["ts_code"] == s.FULL_MARKET and token == "t"
        return _ts_items(api)

    def never(codes):
        raise AssertionError("fallback must not run")

    p = s.Poller(["600000.SH"], token="t", ts_call=ts_call, sina=never, dc=never)
    p.collect_wait = 2.0
    both = p.cycle()
    while len(both) < 2:
        both = pd.concat([both, p.cycle()], ignore_index=True)
    assert sorted(calls) == ["rt_k", "rt_tick"]
    a, b = both[both.src == "ts_tick"], both[both.src == "ts_k"]
    assert a.iloc[0]["src"] == "ts_tick" and a.iloc[0]["a5_p"] == 9.55
    assert a.iloc[0]["date"] == "20261009" and a.iloc[0]["time"] == "15:00:03"
    assert b.iloc[0]["src"] == "ts_k" and pd.isna(b.iloc[0]["a5_p"])
    s.write_part(tmp_path, [a, b])
    assert s.compact(tmp_path)["rows"] == 2


def test_tushare_failure_falls_back_to_sina():
    def ts_call(*a, **k):
        raise s.TushareError("rate")

    p = s.Poller(["600000.SH"], token="t", ts_call=ts_call, sina=lambda c: _frame(c))
    p.collect_wait = 2.0
    df = p.cycle()
    while not len(df):
        df = p.cycle()
    assert set(df["src"]) == {"sina"} and p.failed_cycles == 0


def test_token_from_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN_MAIN", raising=False)
    f = tmp_path / ".env"
    f.write_text("# c\nTUSHARE_TOKEN_MAIN=abc123\nOTHER=1\n")
    monkeypatch.setenv("RQUANT_TUSHARE_ENV_FILE", str(f))
    assert s.tushare_token() == "abc123"


def test_rt_tick_hourly_limit_never_blocks_rt_k():
    calls = []

    def ts_call(api, params, fields, token):
        calls.append(api)
        if api == "rt_tick":
            raise s.TushareError("rt_tick code=40203 抱歉，您访问接口(rt_tick)频率超限(100次/小时)")
        return _ts_items(api)

    def never(codes):
        raise AssertionError("fallback must not run")

    p = s.Poller(["600000.SH"], token="t", ts_call=ts_call, sina=never, dc=never)
    p.collect_wait = 2.0
    frames = []
    for _ in range(3):
        p.last_k_call = 0.0  # skip the stagger
        f = p.cycle()
        while not len(f):
            f = p.cycle()
        frames.append(f)
    assert calls.count("rt_tick") == 1 and calls.count("rt_k") >= 3
    assert all(set(f["src"]) == {"ts_k"} for f in frames)
    assert p.tick_gap >= 36.0 and p.failed_cycles == 0 and p.backoff() == 0.0


def test_rt_k_rate_limit_paces_k_and_uses_sina():
    def ts_call(api, params, fields, token):
        raise s.TushareError(f"{api} code=40203 频率超限(1000次/小时)")

    p = s.Poller(["600000.SH"], token="t", ts_call=ts_call, sina=lambda c: _frame(c))
    p.collect_wait = 2.0
    df = p.cycle()
    while not len(df) or p._inflight:
        df = p.cycle()
    assert set(df["src"]) == {"sina"} and p.k_gap >= 3.6 and p.failed_cycles == 0


def test_backoff_is_capped():
    p = s.Poller(["600000.SH"])
    p.failed_cycles = 10
    assert p.backoff() <= s.MAX_BACKOFF


def test_two_rt_k_calls_overlap():
    import threading

    live, peak, lock = [0], [0], threading.Lock()

    def ts_call(api, params, fields, token):
        if api == "rt_tick":
            raise s.TushareError("rt_tick code=40203 (100次/小时)")
        with lock:
            live[0] += 1
            peak[0] = max(peak[0], live[0])
        time.sleep(0.4)
        with lock:
            live[0] -= 1
        return _ts_items(api)

    p = s.Poller(["600000.SH"], token="t", ts_call=ts_call)
    for _ in range(2):
        p.last_k_call = 0.0
        assert p.cycle().empty  # in flight: no fallback, no failure
    time.sleep(0.6)
    got = p.cycle()
    p.close()
    assert peak[0] == 2 and len(got) == 2 and p.failed_cycles == 0


# ----------------------------------------------------------------- 3s delta stream
def test_delta_stream_lifecycle(tmp_path):
    import pyarrow.parquet as pq

    d = tmp_path / "2026-10-15"
    (d / "stream").mkdir(parents=True)
    (d / "stream" / "d-old-00000007.parquet").write_bytes(b"stale")  # crashed run leftover
    st = s.DeltaStream(d, run="r1")
    assert list((d / "stream").iterdir()) == []
    assert st.write(s._normalize(None, "ts_k")) is None  # empty round: no seq
    fr = _frame(["600000.SH", "000001.SZ"])
    fr["local_ts"] = pd.Timestamp("2026-10-15 09:30:00")
    assert st.write(fr) == 1 and st.write(fr) == 2
    files = sorted(p.name for p in (d / "stream").iterdir())
    assert files == ["d-r1-00000001.parquet", "d-r1-00000002.parquet"]
    t = pq.read_table(d / "stream" / files[0])
    assert t.column_names == s.DELTA_COLS and t.num_rows == 2
    part = s.write_part(d, [fr, fr], st.meta())
    md = pq.read_metadata(part).metadata
    assert json.loads(md[s.STREAM_META_KEY]) == {"run": "r1", "seq": 2}
    st.shard_landed(2)
    assert list((d / "stream").iterdir()) == []
    st.write(fr)
    st.close()
    assert not (d / "stream").exists()
    assert len(pd.read_parquet(part)) == 4  # shards unaffected


def test_delta_write_errors_never_raise(tmp_path):
    st = s.DeltaStream(tmp_path / "d", run="r1")
    st.dir = tmp_path / "missing" / "stream"  # every write fails
    fr = _frame(["600000.SH"])
    for i in range(s.DELTA_MAX_ERRORS + 3):
        assert st.write(fr) == i + 1  # seq keeps counting: shards still record coverage
    assert not st.enabled and st.errors == s.DELTA_MAX_ERRORS


def test_delta_stream_can_be_disabled(tmp_path, monkeypatch):
    monkeypatch.setenv("SNAPSHOT_DELTA", "0")
    st = s.DeltaStream(tmp_path / "d", run="r1")
    assert st.write(_frame(["600000.SH"])) == 1
    assert not (tmp_path / "d" / "stream").exists()


def test_run_writes_deltas_each_round_and_shards_cover_them(tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    seen = []

    class FakePoller:
        failed_cycles = 0

        def __init__(self, codes, token=None):
            self.n = 0

        def set_codes(self, codes):
            pass

        def cycle(self):
            seen.append(sorted(p.name for p in (tmp_path / "out").rglob("d-*.parquet")))
            self.n += 1
            fr = _frame(["600000.SH"], src="ts_k", vol=100.0 * self.n)
            fr["local_ts"] = pd.Timestamp("2026-10-15 09:30:00")
            return fr

        def backoff(self):
            return 0.0

        def close(self):
            pass

    monkeypatch.setattr(s, "Poller", FakePoller)
    monkeypatch.setattr(s, "build_universe", lambda cap=300: {"codes": ["600000.SH"],
                                                              "counts": {}})
    monkeypatch.setattr(s, "tushare_token", lambda: "tok")
    monkeypatch.setattr(s, "FLUSH_EVERY", 1e9)  # no mid-run shard: deltas accumulate
    monkeypatch.setattr(s, "LOOP_TICK", 0.0)
    monkeypatch.setattr(s.time, "sleep", lambda x: None)
    monkeypatch.setenv("SURGE_SCAN", "0")
    out = tmp_path / "out"
    assert s.run(out, force=True, max_cycles=3, ticks=False) == 0
    # before round k the stream held k-1 delta files (one per finished round)
    assert [len(x) for x in seen] == [0, 1, 2]
    day = next(out.glob("20??-??-??"))
    assert not (day / "stream").exists()  # cleaned at the end of the run
    assert pq.read_metadata(day / "quotes.parquet").num_rows == 3


def test_restart_after_close_touches_nothing(tmp_path, monkeypatch):
    from datetime import datetime

    out = tmp_path / "snapshots"
    d = out / "2026-10-15"
    (d / "stream").mkdir(parents=True)
    (d / "stream" / "d-x-00000001.parquet").write_bytes(b"x")
    (d / "universe.json").write_text('{"codes": ["600000.SH"]}')
    pd.DataFrame({"a": [1]}).to_parquet(d / "ticks.parquet")
    monkeypatch.setattr(s, "now", lambda: datetime(2026, 10, 15, 15, 40, tzinfo=s.TZ))
    monkeypatch.setattr(s, "is_trading_day", lambda day: True)

    def boom(*a, **k):
        raise AssertionError("must not run after the close")

    monkeypatch.setattr(s, "build_universe", boom)
    monkeypatch.setattr(s, "fetch_ticks", boom)
    monkeypatch.setattr(s, "Poller", boom)
    assert s.run(out) == 0
    assert (d / "universe.json").read_text() == '{"codes": ["600000.SH"]}'
    assert not (d / "stream").exists()
