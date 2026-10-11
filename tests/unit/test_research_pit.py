from datetime import date

import duckdb
import pandas as pd

from rquant.research.pit import adjust_volume, is_st, st_mask


def _con():
    con = duckdb.connect()
    con.execute("CREATE TABLE stock_basic(ts_code VARCHAR, name VARCHAR)")
    con.execute("INSERT INTO stock_basic VALUES ('A.SZ','退市甲'),('B.SZ','乙')")
    con.execute("CREATE TABLE namechange(ts_code VARCHAR, name VARCHAR, start_date DATE,"
                " end_date DATE)")
    con.execute("INSERT INTO namechange VALUES ('A.SZ','甲',DATE '2020-01-01',DATE '2023-12-31'),"
                "('A.SZ','*ST甲',DATE '2024-01-01',NULL),"
                "('B.SZ','ST乙',DATE '2021-01-01',DATE '2021-06-30'),"
                "('B.SZ','乙',DATE '2021-07-01',NULL)")
    con.execute("CREATE TABLE adj_factor(trade_date DATE, ts_code VARCHAR, adj_factor DOUBLE)")
    con.execute("INSERT INTO adj_factor VALUES (DATE '2022-01-03','A.SZ',1),"
                "(DATE '2022-01-04','A.SZ',2)")
    return con


def test_point_in_time_st():
    con = _con()
    idx = pd.to_datetime(["2021-03-01", "2022-03-01", "2024-03-01"])
    m = st_mask(con, idx, pd.Index(["A.SZ", "B.SZ"]))
    assert m["A.SZ"].tolist() == [False, False, True]
    assert m["B.SZ"].tolist() == [True, False, False]
    f = is_st(con, pd.Series(["A.SZ", "B.SZ"]), pd.Series([date(2022, 3, 1), date(2021, 3, 1)]))
    assert f.tolist() == [False, True]


def test_volume_adjusted_for_bonus_shares():
    con = _con()
    idx = pd.to_datetime(["2022-01-03", "2022-01-04"])
    panel = {"vol": pd.DataFrame({"A.SZ": [100.0, 200.0]}, index=idx)}
    adjust_volume(con, panel)
    assert panel["vol"]["A.SZ"].tolist() == [100.0, 100.0]
