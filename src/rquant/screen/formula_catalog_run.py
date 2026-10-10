"""Offline, bounded formula run over the history projection's recorded catalog."""

from __future__ import annotations

import time
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict

from rquant.screen.formula_history_projection import (
    FormulaProjectionBudgetError,
    FormulaProjectionChangedError,
    FormulaProjectionUnavailableError,
    VerifiedFormulaHistoryProjection,
)
from rquant.screen.tdx.evaluate import (
    CompiledFormula,
    EvaluationRejectedError,
    FormulaEvaluationInput,
    compile_formula,
    evaluate_compiled_formula,
)

MAX_CATALOG_RUN_SECONDS = 120.0


class FormulaCatalogRunTimeoutError(RuntimeError):
    """The entire offline run exceeded its fixed wall-clock budget."""


class FormulaCatalogRunSummary(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    trade_date: date
    scope: Literal["historical_projection_catalog"] = "historical_projection_catalog"
    identity: str
    source_updated_at: datetime
    catalog_total: int
    candidate_total: int
    future_listing_excluded: int
    match_count: int
    no_match_count: int
    unknown_count: int
    unknown_reasons: dict[str, int]
    match_codes: tuple[str, ...]


def _evaluate_candidates(
    projection: VerifiedFormulaHistoryProjection,
    codes: tuple[str, ...],
    compiled: CompiledFormula,
    formula: str,
    trade_date: date,
    decision_at: datetime,
    *,
    expected_identity: str,
    check_deadline: Callable[[], None],
    unknown_reasons: dict[str, int],
) -> tuple[tuple[str, ...], int]:
    """Use the same verified single-stock read and pure evaluator for either universe."""
    matches: list[str] = []
    no_match_count = 0

    def unknown(reason: str) -> None:
        unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1

    for code in codes:
        check_deadline()
        try:
            snapshot = projection.formula_history(
                trade_date,
                code,
                expected_identity=expected_identity,
                lookback=compiled.parsed.translation.window_lookback_bars,
                full_history=compiled.parsed.translation.requires_full_history,
            )
        except FormulaProjectionBudgetError:
            unknown("history_budget")
            continue
        if snapshot.stock is None:
            if snapshot.unknown_reason is None:
                raise FormulaProjectionUnavailableError("history answer has no reason")
            unknown(snapshot.unknown_reason)
            continue
        try:
            decision = evaluate_compiled_formula(
                FormulaEvaluationInput(
                    formula=formula,
                    decision_date=trade_date,
                    decision_at=decision_at,
                    stocks=(snapshot.stock,),
                ),
                compiled,
            ).decisions[0]
        except EvaluationRejectedError as error:
            if error.code != "limit":
                raise
            unknown("evaluation_budget")
            continue
        if decision.status == "match":
            matches.append(code)
        elif decision.status == "no_match":
            no_match_count += 1
        elif decision.reason is not None:
            unknown(decision.reason)
        else:
            raise FormulaProjectionUnavailableError("formula answer has no reason")
    return tuple(matches), no_match_count


def run_formula_catalog(
    root: Path,
    formula: str,
    trade_date: date,
    decision_at: datetime,
    *,
    expected_identity: str,
) -> FormulaCatalogRunSummary:
    """Return a result only after every candidate and the final generation check pass."""
    started_at = time.monotonic()

    def check_deadline() -> None:
        if time.monotonic() - started_at > MAX_CATALOG_RUN_SECONDS:
            raise FormulaCatalogRunTimeoutError("formula catalog run timed out")

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
    projection = VerifiedFormulaHistoryProjection(root)
    catalog = projection.catalog_snapshot(trade_date, expected_identity=expected_identity)
    check_deadline()

    future_excluded = 0
    candidate_codes: list[str] = []
    unknown_reasons: dict[str, int] = {}

    def unknown(reason: str) -> None:
        unknown_reasons[reason] = unknown_reasons.get(reason, 0) + 1

    for entry in catalog.entries:
        if entry.list_date is not None and entry.list_date > trade_date:
            future_excluded += 1
            continue
        if entry.list_date is None:
            unknown("missing_listing")
            continue
        candidate_codes.append(entry.stock_code)

    match_codes, no_match_count = _evaluate_candidates(
        projection,
        tuple(candidate_codes),
        compiled,
        formula,
        trade_date,
        decision_at,
        expected_identity=expected_identity,
        check_deadline=check_deadline,
        unknown_reasons=unknown_reasons,
    )

    check_deadline()
    if projection.catalog().identity != expected_identity:
        raise FormulaProjectionChangedError("history changed during catalog run")
    check_deadline()
    candidate_total = len(catalog.entries) - future_excluded
    unknown_count = sum(unknown_reasons.values())
    if len(match_codes) + no_match_count + unknown_count != candidate_total:
        raise FormulaProjectionUnavailableError("formula catalog counts are inconsistent")
    return FormulaCatalogRunSummary(
        trade_date=trade_date,
        identity=expected_identity,
        source_updated_at=catalog.updated_at,
        catalog_total=len(catalog.entries),
        candidate_total=candidate_total,
        future_listing_excluded=future_excluded,
        match_count=len(match_codes),
        no_match_count=no_match_count,
        unknown_count=unknown_count,
        unknown_reasons=unknown_reasons,
        match_codes=match_codes,
    )
