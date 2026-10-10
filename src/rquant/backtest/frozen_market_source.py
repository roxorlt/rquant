"""Bind a captured screen receipt to retrospective market facts in one frozen read."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal, Self

import duckdb
from pydantic import ValidationError, model_validator

from rquant.backtest.daily_price_source import (
    MAX_REQUEST_CODES,
    RetrospectiveDailyPriceSnapshot,
    verify_retrospective_daily_prices,
)
from rquant.backtest.screen_calendar_source import (
    VerifiedScreenCalendarCandidates,
    verify_screen_calendar_candidates,
)
from rquant.pool_result_receipt import member_price_digest
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256


class FrozenMarketSourceError(ValueError):
    """Frozen candidate and retrospective daily price facts cannot be bound."""


class VerifiedFrozenMarketSnapshot(RuntimeContractModel):
    """Captured candidate availability plus retrospective calendar and daily prices."""

    source_mode: Literal["screen_receipt_with_retrospective_market"] = (
        "screen_receipt_with_retrospective_market"
    )
    candidates: VerifiedScreenCalendarCandidates
    daily_prices: RetrospectiveDailyPriceSnapshot | None

    @model_validator(mode="after")
    def validate_binding(self) -> Self:
        screen = self.candidates.screen
        if screen.receipt.contract != "screen-run-receipt/v2" or screen.receipt.price_digest != (
            member_price_digest(
                [(candidate.ts_code, candidate.previous_close) for candidate in screen.candidates]
            )
        ):
            raise ValueError("screen receipt v2 price proof does not match candidates")
        if len(screen.candidates) > MAX_REQUEST_CODES:
            raise ValueError(f"frozen market source exceeds {MAX_REQUEST_CODES} candidate codes")
        if not screen.candidates:
            if self.daily_prices is not None:
                raise ValueError("empty screen result cannot claim daily price rows")
            return self
        if self.daily_prices is None:
            raise ValueError("every screen candidate requires a source-day daily price")
        requested = tuple(
            (pair.trade_date, pair.ts_code) for pair in self.daily_prices.requested_pairs
        )
        expected = tuple(
            (screen.source_trade_date, candidate.ts_code) for candidate in screen.candidates
        )
        if requested != expected:
            raise ValueError("daily price pairs differ from exact screen candidates")
        for candidate, price in zip(screen.candidates, self.daily_prices.rows, strict=True):
            if price.close != candidate.previous_close:
                raise ValueError(
                    f"daily close differs from captured screen price for {candidate.ts_code}"
                )
        return self

    @property
    def source_identity(self) -> str:
        return canonical_sha256(
            {
                "source_mode": self.source_mode,
                "candidates": self.candidates.source_identity,
                "daily_prices": (
                    self.daily_prices.source_identity if self.daily_prices is not None else None
                ),
            }
        )


def verify_frozen_market_source(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
) -> VerifiedFrozenMarketSnapshot:
    """Use the caller's transaction for the receipt, SSE days, and exact price pairs."""
    candidates = verify_screen_calendar_candidates(
        connection,
        source_trade_date=source_trade_date,
        decision_trade_date=decision_trade_date,
        preset_name=preset_name,
    )
    screen = candidates.screen
    if len(screen.candidates) > MAX_REQUEST_CODES:
        raise FrozenMarketSourceError(
            f"frozen market source exceeds {MAX_REQUEST_CODES} candidate codes"
        )
    daily_prices = (
        verify_retrospective_daily_prices(
            connection,
            tuple((screen.source_trade_date, row.ts_code) for row in screen.candidates),
        )
        if screen.candidates
        else None
    )
    try:
        return VerifiedFrozenMarketSnapshot(candidates=candidates, daily_prices=daily_prices)
    except ValidationError as exc:
        raise FrozenMarketSourceError(str(exc)) from exc


def load_frozen_market_source(
    frozen_path: Path,
    *,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
) -> VerifiedFrozenMarketSnapshot:
    """Open one frozen file read-only and commit one transaction after all checks."""
    if not frozen_path.is_file():
        raise FrozenMarketSourceError("an existing frozen DuckDB file is required")
    try:
        connection = duckdb.connect(str(frozen_path), read_only=True)
    except duckdb.Error as exc:
        raise FrozenMarketSourceError("frozen DuckDB cannot be opened read-only") from exc
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            snapshot = verify_frozen_market_source(
                connection,
                source_trade_date=source_trade_date,
                decision_trade_date=decision_trade_date,
                preset_name=preset_name,
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return snapshot
    except duckdb.Error as exc:
        raise FrozenMarketSourceError("frozen market tables are unavailable") from exc
    finally:
        connection.close()
