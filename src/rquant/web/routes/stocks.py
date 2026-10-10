"""``/api/v1/stocks/*``: bounded lookups on the current read-only Serving generation."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Annotated, Any, TypeVar

from fastapi import APIRouter, Depends, Path, Query, Request, Response

from rquant.web import readers
from rquant.web.envelope import Envelope
from rquant.web.labels import PRESET_LABELS
from rquant.web.models.stocks import StockSearchData, StockSearchRow, StockSummaryData
from rquant.web.security import current_user
from rquant.web.serving import serving_meta

router = APIRouter(prefix="/stocks")
_DataT = TypeVar("_DataT")
_TS_CODE = r"^[0-9A-Z]{6}\.(SH|SZ|BJ)$"
_SEARCH_LIMIT = 20


def _readable(tables: Mapping[str, readers.TableState], name: str) -> bool:
    state = tables.get(name)
    return state is not None and state.available


def _serve(
    request: Request,
    response: Response,
    build: Callable[[Any, Mapping[str, readers.TableState]], _DataT],
    empty: Callable[[], _DataT],
) -> Envelope[_DataT]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        data = (
            empty()
            if borrowed is None
            else build(borrowed.cursor, readers.table_states(borrowed.cursor))
        )
    if meta.generation_id is not None:
        response.headers["X-Rquant-Generation"] = meta.generation_id
    return Envelope[_DataT](data=data, serving=meta)


@router.get("/search", response_model=Envelope[StockSearchData], summary="搜索股票")
def search_stocks(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    q: Annotated[str, Query(min_length=1, max_length=32)],
) -> Envelope[StockSearchData]:
    query = q.strip()

    def build(cursor: Any, tables: Mapping[str, readers.TableState]) -> StockSearchData:
        if not _readable(tables, "stock_basic"):
            return StockSearchData(query=query, available=False, rows=[], truncated=False)
        if not query:
            return StockSearchData(query=query, available=True, rows=[], truncated=False)
        needle = query.casefold()
        found = cursor.execute(
            "SELECT ts_code, name FROM stock_basic "
            "WHERE strpos(lower(ts_code), ?) > 0 OR strpos(lower(coalesce(name, '')), ?) > 0 "
            "ORDER BY CASE "
            "WHEN lower(ts_code) = ? THEN 0 "
            "WHEN starts_with(lower(ts_code), ?) THEN 1 "
            "WHEN lower(name) = ? THEN 2 "
            "WHEN starts_with(lower(coalesce(name, '')), ?) THEN 3 ELSE 4 END, ts_code "
            "LIMIT ?",
            (needle, needle, needle, needle, needle, needle, _SEARCH_LIMIT + 1),
        ).fetchall()
        return StockSearchData(
            query=query,
            available=True,
            rows=[
                StockSearchRow(ts_code=str(code), name=str(name) if name else str(code))
                for code, name in found[:_SEARCH_LIMIT]
            ],
            truncated=len(found) > _SEARCH_LIMIT,
        )

    return _serve(
        request,
        response,
        build,
        lambda: StockSearchData(query=query, available=False, rows=[], truncated=False),
    )


@router.get(
    "/{ts_code}/summary",
    response_model=Envelope[StockSummaryData],
    summary="个股概览",
)
def get_stock_summary(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
    ts_code: Annotated[str, Path(pattern=_TS_CODE)],
) -> Envelope[StockSummaryData]:
    def build(cursor: Any, tables: Mapping[str, readers.TableState]) -> StockSummaryData:
        name = None
        if _readable(tables, "stock_basic"):
            row = cursor.execute(
                "SELECT name FROM stock_basic WHERE ts_code = ? LIMIT 1", (ts_code,)
            ).fetchone()
            name = str(row[0]) if row is not None and row[0] else None
        price = None
        as_of = None
        if _readable(tables, "market_snapshot"):
            row = cursor.execute(
                "SELECT price, as_of, name FROM market_snapshot WHERE ts_code = ? "
                "AND as_of = (SELECT max(as_of) FROM market_snapshot) LIMIT 1",
                (ts_code,),
            ).fetchone()
            if row is not None:
                price = float(row[0]) if row[0] is not None else None
                as_of = row[1]
                name = name or (str(row[2]) if row[2] else None)
        pools: list[str] = []
        if _readable(tables, "screen_result"):
            rows = cursor.execute(
                "SELECT preset_name FROM screen_result WHERE ts_code = ? "
                "AND trade_date = (SELECT max(trade_date) FROM screen_result) "
                "ORDER BY preset_name LIMIT 16",
                (ts_code,),
            ).fetchall()
            pools.extend(PRESET_LABELS.get(str(row[0]), "选股结果") for row in rows)
        if _readable(tables, "pool2_watch"):
            row = cursor.execute(
                "SELECT 1 FROM pool2_watch WHERE ts_code = ? AND status = 'active' LIMIT 1",
                (ts_code,),
            ).fetchone()
            if row is not None:
                pools.append("二池盯盘")
        return StockSummaryData(
            ts_code=ts_code, name=name, price=price, as_of=as_of, pools=list(dict.fromkeys(pools))
        )

    return _serve(
        request,
        response,
        build,
        lambda: StockSummaryData(ts_code=ts_code, name=None, price=None, as_of=None, pools=[]),
    )
