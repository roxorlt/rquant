import pandas as pd
import pytest

from rquant.macro import compute


def test_holder_quarterly_new_exit_and_flow() -> None:
    holders = pd.DataFrame(
        [
            ("中央汇金", "600001.SH", "20240425", "20240331", 100.0),
            ("中央汇金", "600002.SH", "20240425", "20240331", 50.0),
            ("中央汇金", "600001.SH", "20240820", "20240630", 150.0),
            ("中央汇金", "600003.SH", "20240820", "20240630", 10.0),
        ],
        columns=["entity", "ts_code", "ann_date", "end_date", "hold_amount"],
    )
    prices = pd.DataFrame(
        [
            ("20240331", "600001.SH", 1.0),
            ("20240331", "600002.SH", 2.0),
            ("20240630", "600001.SH", 2.0),
            ("20240630", "600003.SH", 3.0),
        ],
        columns=["end_date", "ts_code", "close"],
    )
    out = compute.holder_quarterly(holders, prices)
    rep = out[(out.basis == "report") & (out.quarter == "2024Q2")].iloc[0]
    assert (rep.n_hold, rep.n_new, rep.n_exit, rep.n_add, rep.n_cut) == (2, 1, 1, 2, 1)
    assert rep.flow == pytest.approx(50 * 2 + 10 * 3)
    ann = out[(out.basis == "announce") & (out.quarter == "2024Q3")].iloc[0]
    assert ann.flow == pytest.approx(130)


def test_gdp_ttm_and_buffett_no_lookahead() -> None:
    gdp = pd.DataFrame({"quarter": ["2023Q1", "2023Q4", "2024Q1"], "gdp": [10.0, 50.0, 12.0]})
    t = compute.gdp_ttm(gdp).set_index("quarter").gdp_ttm
    assert t["2024Q1"] == 52.0
    mv = pd.DataFrame({"trade_date": ["20240410", "20240430"], "total_mv": [52e4, 104e4]})
    b = compute.buffett(mv, gdp)
    assert list(b.quarter) == ["2023Q4", "2024Q1"]
    assert b.ratio.tolist() == pytest.approx([52 / 50, 2.0])


def test_implied_vol_roundtrip() -> None:
    p = compute._bs(3.0, 3.0, 0.1, 0.015, 0.2, True)
    assert compute.implied_vol(p, 3.0, 3.0, 0.1, True) == pytest.approx(0.2, abs=1e-4)


def test_complete_dates_drops_partial_trailing_margin_date():
    import pandas as pd

    from rquant.macro.compute import complete_dates, daily_panel

    rows = []
    for d in ["20261008", "20261009"]:
        for ex in ["SSE", "SZSE", "BSE"]:
            if d == "20261009" and ex != "SSE":
                continue
            rows.append(dict(trade_date=d, exchange_id=ex, rzye=1.0, rzmre=1.0, rzrqye=1.0))
    m = pd.DataFrame(rows)
    assert sorted(complete_dates(m, "exchange_id").trade_date.unique()) == ["20261008"]
    # an exchange added later (BSE 2021) does not invalidate the earlier two-exchange history
    early = pd.DataFrame(
        [dict(trade_date="20210101", exchange_id=e) for e in ["SSE", "SZSE"]]
        + [dict(trade_date="20210102", exchange_id=e) for e in ["SSE", "SZSE", "BSE"]]
    )
    assert len(complete_dates(early, "exchange_id")) == 5
    idx = pd.DataFrame(
        [
            dict(trade_date=d, ts_code=c, close=1.0, amount=1e6)
            for d in ["20261008", "20261009"]
            for c in ["000300.SH", "000985.CSI"]
        ]
    )
    p = daily_panel(
        idx,
        m,
        pd.DataFrame(dict(trade_date=["20261008"], north_money=[1.0])),
        pd.DataFrame(dict(trade_date=["20261008"], pe_ttm=[10.0])),
        pd.DataFrame(dict(date=["2026-10-08"], cn10y=[2.0])),
    ).set_index("trade_date")
    assert p.loc["20261008", "rzye"] == 3.0
    assert pd.isna(p.loc["20261009", "rzye"])
