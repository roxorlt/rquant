"""#277 的第一、第二个缺陷：`stk_auction` 要 `pre_close`，空结果也要带齐列。

2026-09-21 生产实测：09:26 三次 `stk_auction(date=20260921)` 都「返回空」，适配器回了一张
**无列**的 `pd.DataFrame()`；15:10 同一个主 token 同一天已经有 6,073 行，列里就有 `pre_close`。
两件事合起来，网关那边一个批次也发不出来：空表被当成「缺八个字段」，非空表被当成「缺
`pre_close`」。这里钉住的是适配器这一半——要的字段里有 `pre_close`，空结果的列集与列序和
`AUCTION_MATCH_COLUMNS` 逐字相同。
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest

from rquant.adapter.tushare import (
    STK_AUCTION_COLUMNS,
    TushareAdapter,
)
from rquant.auction_match_gateway import AUCTION_MATCH_COLUMNS, AuctionMatchGateway

TRADE_DATE = date(2026, 9, 21)


def _adapter(responder: object) -> TushareAdapter:
    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = SimpleNamespace(stk_auction=responder)  # type: ignore[attr-defined]
    adapter._using_backup = False  # type: ignore[attr-defined]
    adapter._backup_token = None  # type: ignore[attr-defined]
    adapter._transport_observer = None  # type: ignore[attr-defined]
    return adapter


def test_the_adapter_and_the_gateway_name_the_same_eight_columns_in_the_same_order() -> None:
    """两份列表不许各写各的。

    适配器不 import 网关（那会把 parquet / spool 一串依赖拖进一个只做 HTTP 的模块），所以
    「两边一致」这件事没有编译期保证，只有这一条用例。#277 的第二个缺陷就是这条保证缺席
    的后果：网关的 `_REQUIRED_NUMERIC_COLUMNS` 里有 `pre_close`，而适配器的 `fields` 里
    从来没有过它。
    """

    assert STK_AUCTION_COLUMNS == AUCTION_MATCH_COLUMNS


def test_the_request_asks_tushare_for_pre_close() -> None:
    asked: list[dict[str, str]] = []

    def stk_auction(**kwargs: str) -> pd.DataFrame:
        asked.append(kwargs)
        return pd.DataFrame(
            [
                {
                    "ts_code": "600000.SH",
                    "trade_date": "20260921",
                    "price": 10.2,
                    "vol": 100_000.0,
                    "amount": 1_020_000.0,
                    "pre_close": 10.0,
                    "turnover_rate": 0.1,
                    "volume_ratio": 1.5,
                }
            ]
        )

    frame = _adapter(stk_auction).stk_auction(TRADE_DATE)

    assert len(asked) == 1
    assert asked[0]["trade_date"] == "20260921"
    assert "pre_close" in asked[0]["fields"].split(",")
    assert tuple(asked[0]["fields"].split(",")) == STK_AUCTION_COLUMNS
    assert set(AUCTION_MATCH_COLUMNS) <= set(frame.columns)
    assert frame.loc[0, "pre_close"] == 10.0
    assert frame.loc[0, "trade_date"] == TRADE_DATE


@pytest.mark.parametrize("empty", [None, "frame"])
def test_an_empty_response_keeps_every_expected_column(empty: str | None) -> None:
    """09:26 的那三次调用，逐字复现：返回空，但下游拿到的是一张八列的零行表。"""

    response = None if empty is None else pd.DataFrame()
    frame = _adapter(lambda **_: response).stk_auction(TRADE_DATE)

    assert frame.empty
    assert tuple(frame.columns)[: len(AUCTION_MATCH_COLUMNS)] == AUCTION_MATCH_COLUMNS
    assert set(AUCTION_MATCH_COLUMNS) <= set(frame.columns)


def test_the_gateway_accepts_the_adapters_empty_frame_as_empty() -> None:
    """两个模块接在一起跑一遍：空结果走到网关，是「空」而不是「缺列」。"""

    frame = _adapter(lambda **_: pd.DataFrame()).stk_auction(TRADE_DATE)

    normalized = AuctionMatchGateway.normalize_frame(
        frame,
        trade_date=TRADE_DATE,
        expected_codes=("600000.SH",),
    )

    assert normalized.empty
    assert tuple(normalized.columns) == AUCTION_MATCH_COLUMNS


def test_a_response_missing_pre_close_is_still_refused_by_name() -> None:
    """接口哪天不给 `pre_close` 了，适配器当场报缺字段，不把半张表递下去。"""

    response = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_date": "20260921",
                "price": 10.2,
                "vol": 100_000.0,
                "amount": 1_020_000.0,
                "turnover_rate": 0.1,
                "volume_ratio": 1.5,
            }
        ]
    )

    with pytest.raises(RuntimeError, match="pre_close"):
        _adapter(lambda **_: response).stk_auction(TRADE_DATE)


def test_a_transport_failure_never_falls_back_to_the_backup_token() -> None:
    """备用 token 没有 `stk_auction` 权限（2026-09-21 实测），所以失败必须直接冒出来。"""

    def stk_auction(**_: str) -> pd.DataFrame:
        raise RuntimeError("抱歉，您没有接口(stk_auction)访问权限")

    adapter = _adapter(stk_auction)
    adapter._backup_token = "b" * 32  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="stk_auction 调用失败"):
        adapter.stk_auction(TRADE_DATE)

    assert adapter._using_backup is False  # type: ignore[attr-defined]
