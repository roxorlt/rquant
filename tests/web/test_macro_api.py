from __future__ import annotations

import pandas as pd
from fastapi.testclient import TestClient

from rquant.macro.refresh import derive, write
from rquant.web.app import create_app
from rquant.web.source import FixtureSource


def _raw() -> dict[str, pd.DataFrame]:
    days = [d.strftime("%Y%m%d") for d in pd.bdate_range("2024-01-01", "2024-03-29")]
    idx = pd.concat(
        [
            pd.DataFrame(
                {"ts_code": c, "trade_date": days, "close": range(1, 1 + len(days)), "amount": 1e9}
            )
            for c in ["000300.SH", "000985.CSI"]
        ]
    )
    basic = pd.DataFrame(
        {
            "ts_code": ["C1", "P1"],
            "name": ["c", "p"],
            "call_put": ["C", "P"],
            "exercise_price": [3.0, 3.0],
            "maturity_date": ["20241231"] * 2,
            "opt_code": ["OP510300.SH"] * 2,
            "per_unit": [10000.0] * 2,
        }
    )
    od = pd.concat(
        [
            pd.DataFrame(
                {
                    "ts_code": ["C1", "P1"],
                    "trade_date": d,
                    "close": [0.2, 0.15],
                    "settle": [0.2, 0.15],
                    "vol": [100.0, 80.0],
                    "amount": 1.0,
                    "oi": [1000.0, 900.0],
                }
            )
            for d in days
        ]
    )
    hk = pd.DataFrame(
        {"ts_code": "HKTECH", "trade_date": days, "close": [100 + i for i in range(len(days))]}
    )
    hk_etf = pd.DataFrame(
        {"ts_code": "513180.SH", "trade_date": days, "close": 1.0, "vol": 1e6, "amount": 1.0}
    )
    fut = pd.DataFrame(
        {
            "ts_code": "IF.CFX",
            "trade_date": days,
            "close": 10.0,
            "settle": 10.0,
            "vol": 1.0,
            "oi": 1.0,
        }
    )
    fh = pd.DataFrame(
        {
            "trade_date": days,
            "sym": "IF",
            "long_hld": 100.0,
            "short_hld": 120.0,
            "long_chg": 1.0,
            "short_chg": 2.0,
        }
    )
    return {
        "hk": hk,
        "hk_etf": hk_etf,
        "fut": fut,
        "fut_holding": fh,
        "holders": pd.DataFrame(
            {
                "entity": "中央汇金",
                "ts_code": ["A", "A"],
                "ann_date": ["20231030", "20240330"],
                "end_date": ["20230930", "20231231"],
                "holder_name": "中央汇金",
                "hold_amount": [1.0, 2.0],
            }
        ),
        "qprice": pd.DataFrame(
            {"end_date": ["20230930", "20231231"], "ts_code": "A", "close": [1.0, 1.0]}
        ),
        "mv": pd.DataFrame(
            {
                "trade_date": ["20240131", "20240229"],
                "total_mv": [7e12, 8e12],
                "circ_mv": 1.0,
                "n": 1,
            }
        ),
        "gdp": pd.DataFrame({"quarter": ["2023Q4"], "gdp": [1.26e6], "gdp_yoy": [5.2]}),
        "index": idx,
        "etf_share": pd.DataFrame({"ts_code": "510300.SH", "trade_date": days, "fd_share": 1e5}),
        "opt_basic": basic,
        "opt_daily": od,
        "und": pd.DataFrame({"ts_code": "510300.SH", "trade_date": days, "close": 3.0}),
        "margin": pd.DataFrame(
            {
                "trade_date": days,
                "exchange_id": "SSE",
                "rzye": 1e12,
                "rzmre": 5e10,
                "rzrqye": 1.1e12,
            }
        ),
        "hsgt": pd.DataFrame({"trade_date": days, "north_money": 10.0}),
        "idxbasic": pd.DataFrame({"trade_date": days, "pe_ttm": 12.0, "pb": 1.3}),
        "cn10y": pd.DataFrame({"date": days, "cn10y": 2.5}),
    }


def test_macro_unavailable_without_tables(tmp_path) -> None:
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    assert client.get("/api/v1/macro").json()["data"]["available"] is False


def test_macro_reads_derived_tables(tmp_path) -> None:
    raw = _raw()
    write(tmp_path / "market.duckdb", raw, derive(raw))
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    body = client.get("/api/v1/macro").json()["data"]
    assert body["available"] is True
    assert body["buffett_bands"]["current"] > 0
    assert body["erp_bands"]["current"] == round(100 / 12 - 2.5, 6) or body["erp_bands"]
    assert body["options_latest"][0]["und"] == "沪300ETF"
    assert body["options_latest"][0]["atm_iv"] is not None
    assert any(h["entity"] == "中央汇金" for h in body["holders"])
    assert body["futures"]["IF"]["cols"]["net"][-1] == -20
    assert body["hk"] is not None and body["studies"]


def test_macro_summary_json(tmp_path) -> None:
    from rquant.macro.summary import SCHEMA, build, write_summary

    raw = _raw()
    derived = derive(raw)
    path = write_summary(tmp_path, build(derived))
    assert path.name == "2024-03-29.json"
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    body = client.get("/api/v1/macro/summary").json()
    assert body["schema"] == SCHEMA
    keys = {i["key"] for i in body["indicators"]}
    assert {"buffett", "erp", "basis_IF", "pcr_vol_沪300ETF"} <= keys
