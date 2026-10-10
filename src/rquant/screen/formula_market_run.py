"""Offline formula run over one observed A-share list and one verified history generation."""

from __future__ import annotations

import time
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.screen.formula_catalog_run import _evaluate_candidates
from rquant.screen.formula_history_projection import (
    FormulaProjectionUnavailableError,
    VerifiedFormulaHistoryProjection,
)
from rquant.screen.formula_market_universe import load_formula_market_universe
from rquant.screen.tdx.evaluate import (
    MAX_STOCKS,
    EvaluationRejectedError,
    FormulaEvaluationInput,
    compile_formula,
    evaluate_compiled_formula,
)

MAX_MARKET_RUN_STOCKS = MAX_STOCKS
MAX_MARKET_RUN_SECONDS = 120.0


class FormulaMarketRunBudgetError(RuntimeError):
    """The captured market is too large for one bounded formula run."""


class FormulaMarketRunTimeoutError(RuntimeError):
    """The entire offline run exceeded its wall-clock budget."""


class FormulaMarketRunSummary(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    trade_date: date
    decision_at: datetime
    scope: Literal["captured_a_share_market"] = "captured_a_share_market"
    universe_identity: str
    universe_completed_at: datetime
    projection_identity: str
    projection_updated_at: datetime
    market_total: int
    listed_count: int
    paused_count: int
    match_count: int
    no_match_count: int
    unknown_count: int
    unknown_reasons: dict[str, int]
    match_codes: tuple[str, ...]


def run_formula_market(
    universe_root: Path,
    projection_root: Path,
    formula: str,
    trade_date: date,
    decision_at: datetime,
    *,
    expected_universe_sha256: str,
    expected_projection_identity: str,
) -> FormulaMarketRunSummary:
    """Evaluate only archived members; refuse a partial answer or source generation switch."""
    started_at = time.monotonic()

    def check_deadline() -> None:
        if time.monotonic() - started_at > MAX_MARKET_RUN_SECONDS:
            raise FormulaMarketRunTimeoutError("formula market run timed out")

    compiled = compile_formula(formula)
    evaluate_compiled_formula(
        FormulaEvaluationInput(
            formula=formula,
            decision_date=trade_date,
            decision_at=decision_at,
            stocks=(),
        ),
        compiled,
    )
    universe = load_formula_market_universe(
        universe_root,
        trade_date,
        expected_sha256=expected_universe_sha256,
    )
    check_deadline()
    if len(universe.entries) > MAX_MARKET_RUN_STOCKS:
        raise FormulaMarketRunBudgetError("captured market exceeds formula run capacity")

    projection = VerifiedFormulaHistoryProjection(projection_root)
    catalog = projection.listing_snapshot_for_codes(
        trade_date,
        tuple(entry.ts_code for entry in universe.entries),
        expected_identity=expected_projection_identity,
        check_deadline=check_deadline,
    )
    check_deadline()
    if catalog.updated_at.tzinfo is None or catalog.updated_at.utcoffset() is None:
        raise FormulaProjectionUnavailableError("history source time is not timezone-aware")
    if decision_at < universe.completed_at or decision_at < catalog.updated_at:
        raise EvaluationRejectedError("time", "运行时点早于名单或行情资料可用时间。")

    listing_by_code = {entry.stock_code: entry.list_date for entry in catalog.entries}
    unknown_reasons: dict[str, int] = {}
    candidate_codes: list[str] = []

    def unknown(reason: str) -> None:
        unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1

    for entry in universe.entries:
        check_deadline()
        if entry.ts_code not in listing_by_code:
            unknown("missing_projection_code")
        elif listing_by_code[entry.ts_code] != entry.list_date:
            unknown("listing_conflict")
        else:
            candidate_codes.append(entry.ts_code)

    match_codes, no_match_count = _evaluate_candidates(
        projection,
        tuple(candidate_codes),
        compiled,
        formula,
        trade_date,
        decision_at,
        expected_identity=expected_projection_identity,
        check_deadline=check_deadline,
        unknown_reasons=unknown_reasons,
    )
    check_deadline()
    if projection.catalog().identity != expected_projection_identity:
        raise FormulaProjectionUnavailableError("history changed during market run")
    load_formula_market_universe(
        universe_root,
        trade_date,
        expected_sha256=expected_universe_sha256,
    )
    check_deadline()

    listed_count = sum(entry.list_status == "L" for entry in universe.entries)
    paused_count = len(universe.entries) - listed_count
    unknown_count = sum(unknown_reasons.values())
    if listed_count + paused_count != len(universe.entries) or len(
        match_codes
    ) + no_match_count + unknown_count != len(universe.entries):
        raise FormulaProjectionUnavailableError("formula market counts are inconsistent")
    return FormulaMarketRunSummary(
        trade_date=trade_date,
        decision_at=decision_at,
        universe_identity=expected_universe_sha256,
        universe_completed_at=universe.completed_at,
        projection_identity=expected_projection_identity,
        projection_updated_at=catalog.updated_at,
        market_total=len(universe.entries),
        listed_count=listed_count,
        paused_count=paused_count,
        match_count=len(match_codes),
        no_match_count=no_match_count,
        unknown_count=unknown_count,
        unknown_reasons=unknown_reasons,
        match_codes=tuple(sorted(match_codes)),
    )
