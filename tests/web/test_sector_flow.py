from pathlib import Path

import duckdb
import pandas as pd

from rquant.web import sector_flow as sf


def _row(code, t, price, amount, bid, ask, pre=10.0, name="A"):
    return dict(ts_code=code, name=name, time=t, price=price, pre_close=pre, amount=amount,
                b1_p=bid, a1_p=ask)


def test_tick_rule_and_auction(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "market.duckdb"))
    con.execute("CREATE TABLE stock_basic (ts_code VARCHAR, industry VARCHAR)")
    con.execute("INSERT INTO stock_basic VALUES ('000001.SZ','银行'), ('000002.SZ','地产')")
    con.execute("CREATE TABLE daily_basic (ts_code VARCHAR, trade_date DATE, circ_mv DOUBLE)")
    con.execute("INSERT INTO daily_basic VALUES ('000001.SZ', DATE '2026-10-13', 5e5),"
                "('000002.SZ', DATE '2026-10-13', 5e5)")
    con.close()
    q = tmp_path / "snapshots" / "2026-10-14" / "quotes"
    q.mkdir(parents=True)
    pd.DataFrame([
        _row("000001.SZ", "09:20:00", 0.0, 0.0, 10.1, 10.1),
        _row("000001.SZ", "09:25:03", 10.1, 1e8, 10.1, 10.1),   # auction, above pre -> +1e8
        _row("000002.SZ", "09:25:03", 9.9, 2e8, 9.9, 9.9),     # auction, below pre -> -2e8
    ]).to_parquet(q / "part-1.parquet")
    pd.DataFrame([
        _row("000001.SZ", "09:30:03", 10.2, 1.5e8, 10.1, 10.2),  # >= prev ask 10.1 -> buy
        _row("000001.SZ", "09:30:06", 10.1, 1.7e8, 10.1, 10.2),  # <= prev bid 10.1 -> sell
        _row("000002.SZ", "09:31:00", 9.9, 2e8, 9.8, 9.9),       # no turnover change
    ]).to_parquet(q / "part-2.parquet")
    out = sf.load_sector_flow(tmp_path, "2026-10-14", [], wait=True)
    nets = {s.name: s.net for s in out.sectors}
    assert nets["银行"] == 1e8 + 0.5e8 - 0.2e8
    assert nets["地产"] == -2e8
    assert out.t[0] == "09:25" and out.t[-1] == "09:31"
    assert out.sectors[0].series[0] == 1.0  # 亿, the auction point
    assert out.summary is not None and out.summary.amount == 1.7e8 + 2e8
    again = sf.load_sector_flow(tmp_path, "2026-10-14", [], known=out.version)
    assert again.unchanged


def test_leaders_as_of_minute(tmp_path: Path) -> None:
    test_tick_rule_and_auction(tmp_path)
    # float cap 50亿 passes; turnover 1.7亿 at the end but only 1亿 at 09:25 (>= floor)
    lead = sf.load_leaders_at(tmp_path, "2026-10-14", 0)
    assert lead and lead[0].name == "银行"
    assert lead[0].stocks[0].net == 1e8
    assert len(lead[0].stocks[0].spark) == 1


# ----------------------------------------------------------------- 3s delta stream overlay
import os  # noqa: E402
import time  # noqa: E402

import pytest  # noqa: E402

from rquant.research import snapshots as snap  # noqa: E402

DAY = "2026-10-15"


def _clear() -> None:
    import threading

    for t in threading.enumerate():  # background refreshes started by load_sector_flow
        if t.name.startswith("sector-flow-2"):
            t.join(10)
    for c in (sf._DAYS, sf._RESULT, sf._FOOTER, sf._SECT, sf._CIRC, sf._TURN, sf._PREV):
        c.clear()


@pytest.fixture(autouse=True)
def _fresh_caches():
    _clear()
    yield
    _clear()


def _market(root: Path) -> None:
    con = duckdb.connect(str(root / "market.duckdb"))
    con.execute("CREATE TABLE stock_basic (ts_code VARCHAR, industry VARCHAR)")
    con.execute("INSERT INTO stock_basic VALUES ('000001.SZ','银行'), ('000002.SZ','地产'),"
                "('600000.SH','银行')")
    con.execute("CREATE TABLE daily_basic (ts_code VARCHAR, trade_date DATE, circ_mv DOUBLE)")
    con.close()


def _round(i: int) -> pd.DataFrame:
    """Collector round i (3 s apart from 09:25:03): every code trades, alternating up/down."""
    sec = 25 * 60 + 3 + 3 * i
    t = f"09:{sec // 60:02d}:{sec % 60:02d}"
    rows = []
    for k, code in enumerate(("000001.SZ", "000002.SZ", "600000.SH")):
        px = 10.0 + (0.01 * ((i + k) % 3))
        rows.append({"ts_code": code, "name": code[:6], "date": "20261015", "time": t,
                     "price": px, "pre_close": 10.0, "amount": 1e6 * (i + 1) * (k + 1),
                     "b1_p": px - 0.01, "a1_p": px, "volume": 1e5 * (i + 1)})
    fr = snap._normalize(pd.DataFrame(rows), "ts_k")
    fr["local_ts"] = pd.Timestamp("2026-10-15 09:25:00") + pd.Timedelta(seconds=3 * i)
    return fr


def _nets(out) -> dict[str, float]:
    return {s.name: round(s.net, 4) for s in out.sectors}


def _reference(tmp: Path, rounds: list[pd.DataFrame]) -> dict[str, float]:
    """Everything in one official shard, no stream."""
    tmp.mkdir(parents=True)
    _market(tmp)
    snap.write_part(tmp / "snapshots" / DAY, rounds)
    for c in (sf._DAYS, sf._RESULT):
        c.clear()
    out = sf._refresh(tmp, DAY)
    for c in (sf._DAYS, sf._RESULT):
        c.clear()
    return _nets(out)


def test_deltas_update_every_round_and_reconcile_without_double_count(tmp_path: Path) -> None:
    frames = [_round(i) for i in range(9)]
    refs = [_reference(tmp_path / f"ref{i}", frames[: i + 1]) for i in range(9)]
    root = tmp_path / "live"
    root.mkdir()
    _market(root)
    d = root / "snapshots" / DAY
    stream = snap.DeltaStream(d, run="r1")
    buf: list[pd.DataFrame] = []
    versions = []
    for i, fr in enumerate(frames):  # live state is folded incrementally across refreshes
        buf.append(fr)
        assert stream.write(fr) == i + 1
        if i in (3, 7):  # a 30 s shard lands (covers seq 1..i+1), its deltas are dropped
            meta = stream.meta()
            snap.write_part(d, buf, meta)
            buf = []
            stream.shard_landed(meta["seq"])
        out = sf._refresh(root, DAY)
        assert out is not None
        versions.append(out.version)
        assert _nets(out) == refs[i], f"round {i}"
        assert out.time == fr["time"].iloc[0]
    assert len(set(versions)) == len(versions)  # a new data version every round
    assert versions[3] == "1-09:25:12" and versions[4].startswith("1+r1.5-")
    assert versions[8].startswith("2+r1.9-")
    assert sorted(p.name for p in (d / "stream").iterdir()) == ["d-r1-00000009.parquet"]
    assert len(sf._DAYS[DAY].keys) == 2  # official state holds only the two shards
    stream.close()
    assert not (d / "stream").exists()


def test_landed_shard_drops_lingering_deltas(tmp_path: Path) -> None:
    """Race: the shard is on disk but the collector has not deleted its deltas yet."""
    _market(tmp_path)
    d = tmp_path / "snapshots" / DAY
    stream = snap.DeltaStream(d, run="r1")
    frames = [_round(i) for i in range(4)]
    for fr in frames:
        stream.write(fr)
    snap.write_part(d, frames[:3], {"run": "r1", "seq": 3})  # deltas 1-3 still present
    out = sf._refresh(tmp_path, DAY)
    assert _nets(out) == _reference(tmp_path / "ref", frames)
    assert out.version.startswith("1+r1.4-")


def test_deltas_before_first_shard_and_stale_ignored(tmp_path: Path) -> None:
    _market(tmp_path)
    d = tmp_path / "snapshots" / DAY
    stream = snap.DeltaStream(d, run="r1")
    stream.write(_round(0))
    out = sf._refresh(tmp_path, DAY)
    assert out is not None and out.t == ["09:25"] and out.version.startswith("0+r1.1-")
    old = time.time() - sf.DELTA_MAX_AGE - 10
    for p in (d / "stream").iterdir():
        os.utime(p, (old, old))
    sf._RESULT.clear()
    assert sf._refresh(tmp_path, DAY) is None  # collector stalled: provisional data expires


def test_vanished_delta_refolds_shard(tmp_path: Path, monkeypatch) -> None:
    """Shard lands and its deltas are deleted between listing and reading."""
    _market(tmp_path)
    d = tmp_path / "snapshots" / DAY
    stream = snap.DeltaStream(d, run="r1")
    frames = [_round(i) for i in range(3)]
    for fr in frames:
        stream.write(fr)
    real = sf._update
    calls = {"n": 0}

    def update(root, day):
        calls["n"] += 1
        if calls["n"] == 2:  # after the first fold: shard lands, deltas removed
            snap.write_part(d, frames, stream.meta())
            stream.shard_landed(3)
        return real(root, day)

    monkeypatch.setattr(sf, "_update", update)
    sf._refresh(tmp_path, DAY)  # first call folds nothing official, overlay from deltas
    sf._RESULT.clear()
    out = sf._refresh(tmp_path, DAY)
    assert _nets(out) == _reference(tmp_path / "ref", frames)
    assert out.version.startswith("1-")


# ------------------------------------------------------- flowfix 2026-10-19: late snapshots
def _db(tmp_path: Path) -> None:
    con = duckdb.connect(str(tmp_path / "market.duckdb"))
    con.execute("CREATE TABLE stock_basic (ts_code VARCHAR, industry VARCHAR)")
    con.execute("INSERT INTO stock_basic VALUES ('000001.SZ','银行'), ('000002.SZ','地产')")
    con.execute("CREATE TABLE daily_basic (ts_code VARCHAR, trade_date DATE, circ_mv DOUBLE)")
    con.execute("INSERT INTO daily_basic VALUES ('000001.SZ', DATE '2026-10-13', 5e5),"
                "('000002.SZ', DATE '2026-10-13', 5e5)")
    con.close()


def test_late_snapshot_in_later_chunk_is_not_recounted(tmp_path: Path) -> None:
    """A row older than the code's last folded row (other source, later chunk) must add nothing.
    Before the fix it sorted ahead of the carry row, became the 'first row of the day' and added
    the whole day-to-date turnover signed by price vs pre_close (10-19: +491亿)."""
    _db(tmp_path)
    q = tmp_path / "snapshots" / "2026-10-14" / "quotes"
    q.mkdir(parents=True)
    pd.DataFrame([
        _row("000001.SZ", "09:25:03", 10.1, 1e8, 10.1, 10.1),    # auction +1e8
        _row("000001.SZ", "11:30:00", 10.2, 5e8, 10.1, 10.2),    # >= prev ask -> +4e8
    ]).to_parquet(q / "part-1.parquet")
    pd.DataFrame([
        _row("000001.SZ", "11:29:59", 10.3, 4.9e8, 10.2, 10.3),  # LATE (other source): ignore
        _row("000001.SZ", "11:30:00", 10.3, 5e8, 10.2, 10.3),    # same time as carry: ignore
        _row("000001.SZ", "13:00:03", 10.1, 6e8, 10.1, 10.2),    # <= prev bid 10.1 -> -1e8
    ]).to_parquet(q / "part-2.parquet")
    out = sf.load_sector_flow(tmp_path, "2026-10-14", [], wait=True)
    s = {x.name: x for x in out.sectors}["银行"]
    assert s.net == 1e8 + 4e8 - 1e8
    assert s.amount == 6e8  # Σ increments == final cumulative turnover
    assert out.summary is not None and out.summary.amount == 6e8
    assert sf._DAYS["2026-10-14"].late_dropped == 2


def test_stale_source_inside_chunk_does_not_recount(tmp_path: Path) -> None:
    """Within one chunk a stale source (lower cumulative amount at a later time) must not make
    the next row re-count the difference."""
    _db(tmp_path)
    q = tmp_path / "snapshots" / "2026-10-14" / "quotes"
    q.mkdir(parents=True)
    pd.DataFrame([
        _row("000002.SZ", "09:25:03", 9.9, 2e8, 9.9, 9.9),       # auction -2e8
        _row("000002.SZ", "09:30:03", 10.0, 3e8, 9.9, 10.0),     # >= prev ask 9.9 -> +1e8
        _row("000002.SZ", "09:30:04", 10.0, 2.5e8, 9.9, 10.0),   # stale source: +0
        _row("000002.SZ", "09:30:06", 10.0, 3.2e8, 9.9, 10.0),   # +0.2e8, not +0.7e8
    ]).to_parquet(q / "part-1.parquet")
    out = sf.load_sector_flow(tmp_path, "2026-10-14", [], wait=True)
    s = {x.name: x for x in out.sectors}["地产"]
    assert abs(s.net - (-2e8 + 1e8 + 0.2e8)) < 1
    assert abs(s.amount - 3.2e8) < 1


def test_late_rows_in_delta_overlay_are_ignored(tmp_path: Path) -> None:
    """The provisional 3s-delta overlay folds onto a clone with the same rule."""
    _db(tmp_path)
    q = tmp_path / "snapshots" / "2026-10-14" / "quotes"
    q.mkdir(parents=True)
    pd.DataFrame([
        _row("000001.SZ", "09:25:03", 10.1, 1e8, 10.1, 10.1),
        _row("000001.SZ", "10:00:00", 10.2, 3e8, 10.1, 10.2),
    ]).to_parquet(q / "part-1.parquet")
    st = sf._update(tmp_path, "2026-10-14")
    assert st is not None
    view = st.clone()
    view.fold(pd.DataFrame([_row("000001.SZ", "09:59:58", 10.3, 2.9e8, 10.2, 10.3)]))
    assert float(view.net.sum()) == float(st.net.sum()) == 1e8 + 2e8
    assert view.late_dropped == 1 and st.late_dropped == 0


# ------------------------------------------- 2026-10-20: 两市成交额 card replaces the market net
TDAY = "2026-10-19"  # Monday; previous open day is Friday 10-16 (calendar, not "yesterday")


def _turnover_root(tmp_path: Path, *, today: bool, calendar: bool = True) -> Path:
    _db(tmp_path)
    con = duckdb.connect(str(tmp_path / "market.duckdb"))
    if calendar:
        con.execute("CREATE TABLE trade_calendar (cal_date DATE, is_open BOOLEAN)")
        con.execute("INSERT INTO trade_calendar VALUES (DATE '2026-10-15', true),"
                    "(DATE '2026-10-16', true), (DATE '2026-10-17', false),"
                    "(DATE '2026-10-18', false), (DATE '2026-10-19', true)")
    con.execute("CREATE TABLE daily_bar (ts_code VARCHAR, trade_date DATE, amount DOUBLE)")
    # amount in 千元 like Tushare daily; 北交所 counted (same total as turnover_daily.csv)
    con.execute("INSERT INTO daily_bar VALUES ('000001.SZ', DATE '2026-10-16', 1.5e9),"
                "('920001.BJ', DATE '2026-10-16', 6.9066e7), ('000001.SZ', DATE '2026-10-15', 9e9)")
    if today:
        con.execute("INSERT INTO daily_bar VALUES ('000001.SZ', DATE '2026-10-19', 1.4e9),"
                    "('920001.BJ', DATE '2026-10-19', 4.8791854e7)")
    con.close()
    q = tmp_path / "snapshots" / TDAY / "quotes"
    q.mkdir(parents=True)
    pd.DataFrame([
        _row("000001.SZ", "09:25:03", 10.1, 1e8, 10.1, 10.1),
        _row("000001.SZ", "10:00:00", 10.2, 3e8, 10.1, 10.2),
    ]).to_parquet(q / "part-1.parquet")
    return tmp_path


def test_turnover_from_tushare_daily_vs_previous_open_day(tmp_path: Path) -> None:
    root = _turnover_root(tmp_path, today=True)
    sm = sf.load_sector_flow(root, TDAY, [], wait=True).summary
    assert sm is not None
    assert sm.turnover_source == "tushare_daily"
    assert sm.turnover == 14487.919  # (1.4e9 + 4.8791854e7) 千元 -> 亿
    assert sm.prev_date == "2026-10-16"  # skips the weekend via trade_calendar
    assert sm.prev_turnover == 15690.66
    assert sm.turnover_chg == -1202.741
    assert sm.turnover_chg_pct == -7.67
    d = sm.model_dump()
    assert "net" not in d  # the market-wide estimated net inflow is no longer served


def test_turnover_falls_back_to_snapshot_until_ingested(tmp_path: Path, monkeypatch) -> None:
    root = _turnover_root(tmp_path, today=False)
    out = sf.load_sector_flow(root, TDAY, [], wait=True)
    sm = out.summary
    assert sm is not None
    assert sm.turnover_source == "snapshot_last" and sm.turnover == 3.0  # last cum amount 3e8
    assert sm.prev_turnover == 15690.66 and sm.turnover_chg == round(3.0 - 15690.66, 3)
    # Tushare lands later: next refresh swaps the source and the version, no restart needed
    con = duckdb.connect(str(root / "market.duckdb"))
    con.execute("INSERT INTO daily_bar VALUES ('000001.SZ', DATE '2026-10-19', 1.4e9)")
    con.close()
    monkeypatch.setattr(sf, "TURN_RETRY", 0.0)
    again = sf._refresh(root, TDAY)
    assert again is not None and again.version != out.version and again.version.endswith("-td")
    assert again.summary is not None and again.summary.turnover_source == "tushare_daily"
    assert again.summary.turnover == 14000.0


def test_turnover_without_calendar_has_no_change(tmp_path: Path) -> None:
    root = _turnover_root(tmp_path, today=True, calendar=False)
    sm = sf.load_sector_flow(root, TDAY, [], wait=True).summary
    assert sm is not None and sm.turnover == 14487.919
    assert sm.prev_date is None and sm.prev_turnover is None
    assert sm.turnover_chg is None and sm.turnover_chg_pct is None


def test_turnover_fields_empty_day(tmp_path: Path) -> None:
    _db(tmp_path)
    f = sf.turnover_fields(tmp_path, TDAY, 0.0)
    assert f == dict(turnover=None, turnover_source=None, prev_date=None, prev_turnover=None,
                     turnover_chg=None, turnover_chg_pct=None)
