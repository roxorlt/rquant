"""Read-only, reconciled paper accounts from one verified Serving generation."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import Annotated, Any, Literal

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import ValidationError

from rquant.paper_contracts import PaperAccountSnapshot, PaperHolding
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.web import readers
from rquant.web.envelope import Envelope
from rquant.web.models.paper import PaperAccountItem, PaperAccountsData, PaperHoldingItem
from rquant.web.security import current_user
from rquant.web.serving import BorrowedGeneration, serving_meta
from rquant.web.status import PAPER_VALUATION_NOTE

router = APIRouter(prefix="/paper")

_MAX_ACCOUNTS = 20
_MAX_HOLDINGS = 500
_UNREADABLE = "模拟账户数据暂时无法读取，请稍后重试。"
_ACCOUNT_SQL = (
    "SELECT account_id, snapshot_id, as_of_time, cash, available_cash, frozen_cash, "
    "realized_pnl, unrealized_pnl, nav FROM paper_accounts "
    "ORDER BY as_of_time DESC, account_id LIMIT ?"
)
_HOLDING_SQL = (
    "SELECT account_id, ts_code, quantity, available_quantity, frozen_quantity, "
    "average_cost, market_price, market_value, unrealized_pnl, as_of_time "
    "FROM paper_holdings ORDER BY account_id, market_value DESC, ts_code LIMIT ?"
)


def _empty(state: Literal["not_published", "unavailable"]) -> PaperAccountsData:
    return PaperAccountsData(
        source_state=state,
        source_updated_at=None,
        source_note=None,
        valuation_note=None,
        accounts=[],
    )


def _watermark(borrowed: BorrowedGeneration) -> ServingDatasetWatermark | None:
    return next(
        (mark for mark in borrowed.manifest.watermarks if mark.dataset_id == "paper_accounts"),
        None,
    )


def _holding(row: tuple[Any, ...]) -> PaperHolding:
    return PaperHolding(
        code=row[1],
        quantity=row[2],
        available_quantity=row[3],
        frozen_quantity=row[4],
        average_cost=row[5],
        market_price=row[6],
    )


def _read(borrowed: BorrowedGeneration, mark: ServingDatasetWatermark) -> PaperAccountsData:
    try:
        expected_accounts = borrowed.manifest.row_counts["paper_accounts"]
        expected_holdings = borrowed.manifest.row_counts["paper_holdings"]
        if (
            not 0 <= expected_accounts <= _MAX_ACCOUNTS
            or not 0 <= expected_holdings <= _MAX_HOLDINGS
        ):
            raise ValueError("paper row budget exceeded")
        account_rows = borrowed.cursor.execute(_ACCOUNT_SQL, (_MAX_ACCOUNTS + 1,)).fetchall()
        holding_rows = borrowed.cursor.execute(_HOLDING_SQL, (_MAX_HOLDINGS + 1,)).fetchall()
        if len(account_rows) != expected_accounts or len(holding_rows) != expected_holdings:
            raise ValueError("paper rows differ from manifest")

        by_account: dict[str, list[tuple[Any, ...]]] = defaultdict(list)
        seen_holdings: set[tuple[str, str]] = set()
        account_ids = {row[0] for row in account_rows}
        if len(account_ids) != len(account_rows):
            raise ValueError("duplicate paper account")
        for row in holding_rows:
            identity = (row[0], row[1])
            if row[0] not in account_ids or identity in seen_holdings:
                raise ValueError("orphan or duplicate paper holding")
            seen_holdings.add(identity)
            by_account[row[0]].append(row)

        codes = (row[1] for row in holding_rows)
        try:
            names = readers.stock_names(
                borrowed.cursor, readers.table_states(borrowed.cursor), codes
            )
        except duckdb.Error:
            names = {}

        accounts: list[PaperAccountItem] = []
        for row in account_rows:
            account_id = row[0]
            rows = by_account[account_id]
            if any(holding[9] != row[2] for holding in rows):
                raise ValueError("paper holding timestamps differ from account")
            holdings = [_holding(holding) for holding in rows]
            snapshot = PaperAccountSnapshot(
                snapshot_id=row[1],
                account_id=account_id,
                as_of_time=row[2],
                cash=row[3],
                available_cash=row[4],
                frozen_cash=row[5],
                realized_pnl=row[6],
                unrealized_pnl=row[7],
                nav=row[8],
                holdings=tuple(holdings),
            )
            items: list[PaperHoldingItem] = []
            for holding_row, holding in zip(rows, holdings, strict=True):
                value = holding.market_price * holding.quantity
                pnl = (holding.market_price - holding.average_cost) * holding.quantity
                if holding_row[7] != value or holding_row[8] != pnl:
                    raise ValueError("paper holding amount differs from snapshot")
                cost = holding.average_cost * holding.quantity
                items.append(
                    PaperHoldingItem(
                        code=holding.code,
                        name=names.get(holding.code),
                        quantity=holding.quantity,
                        available_quantity=holding.available_quantity,
                        average_cost=float(holding.average_cost),
                        market_price=float(holding.market_price),
                        market_value=float(value),
                        unrealized_pnl=float(pnl),
                        unrealized_pct=float(pnl / cost * Decimal("100")) if cost else None,
                    )
                )
            market_value = snapshot.nav - snapshot.cash
            accounts.append(
                PaperAccountItem(
                    account_id=account_id,
                    as_of=snapshot.as_of_time,
                    nav=float(snapshot.nav),
                    cash=float(snapshot.cash),
                    market_value=float(market_value),
                    unrealized_pnl=float(snapshot.unrealized_pnl),
                    holdings=items,
                )
            )
    except (duckdb.Error, ValidationError, ValueError, TypeError, KeyError, OverflowError) as error:
        raise HTTPException(status_code=503, detail=_UNREADABLE) from error

    note = (
        "模拟账户更新延迟，以下金额可能不是最新的。"
        if mark.status is FreshnessStatus.STALE
        else "模拟账户数据暂不完整，请稍后刷新。"
        if mark.status is FreshnessStatus.DEGRADED
        else None
    )
    valuation = (
        PAPER_VALUATION_NOTE
        if "last execution price" in (mark.reason or "").lower()
        else "持仓采用已发布估值，可能与实时价格不同"
    )
    return PaperAccountsData(
        source_state="ready" if accounts else "empty",
        source_updated_at=mark.event_time,
        source_note=note,
        valuation_note=valuation,
        accounts=accounts,
    )


@router.get("/accounts", response_model=Envelope[PaperAccountsData], summary="模拟账户与持仓")
def get_accounts(
    request: Request,
    response: Response,
    _viewer: Annotated[str | None, Depends(current_user)],
) -> Envelope[PaperAccountsData]:
    web = request.app.state.web
    now = web.clock()
    with web.tracker.borrow() as borrowed:
        meta = serving_meta(
            borrowed, now=now, stale_after=web.settings.stale_after, failure=web.tracker.failure
        )
        if meta.generation_id is not None:
            response.headers["X-Rquant-Generation"] = meta.generation_id
        if borrowed is None or meta.state == "unavailable":
            data = _empty("unavailable")
        else:
            mark = _watermark(borrowed)
            data = (
                _empty("not_published")
                if mark is None or mark.status is FreshnessStatus.UNAVAILABLE
                else _read(borrowed, mark)
            )
    return Envelope[PaperAccountsData](data=data, serving=meta)
