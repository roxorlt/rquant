"""PIT candidate selection and training-only ranking using the original owners."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Self

import pandas as pd
from pydantic import Field, model_validator

from rquant import strategy_optimizer, topn_selection
from rquant.minute_backtest_contracts import MAX_CODES, MAX_WORK_UNITS
from rquant.minute_backtest_parameter_study import (
    MinuteParameterStudyBinding,
    MinuteStudyExecutionPartition,
)
from rquant.minute_backtest_study_protocols import (
    SHANGHAI,
    MinuteStudyCandidate,
    MinuteStudyHead,
    MinuteStudyProtocol,
    MinuteStudySource,
    StudyDefinitionId,
    StudyHash,
    StudyPartition,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc


class MinuteStudySelection(RuntimeContractModel):
    study_id: StudyHash
    source_hash: StudyHash
    candidate_id: StudyDefinitionId
    ts_code: str
    trade_date: date
    event_time: AwareUtcDatetime
    decision_cutoff: AwareUtcDatetime
    partition: StudyPartition
    feature_score: float = Field(allow_inf_nan=False)
    feature_rank: int = Field(strict=True, ge=1)


class MinuteStudyThreePartSelection(RuntimeContractModel):
    study_id: StudyHash
    execution_binding_hash: StudyHash
    source_hash: StudyHash
    candidate_id: StudyDefinitionId
    ts_code: str
    trade_date: date
    event_time: AwareUtcDatetime
    decision_cutoff: AwareUtcDatetime
    partition: MinuteStudyExecutionPartition
    feature_score: float = Field(allow_inf_nan=False)
    feature_rank: int = Field(strict=True, ge=1)


def select_minute_study_candidates(
    protocol: MinuteStudyProtocol,
    candidates: Sequence[MinuteStudyCandidate],
    *,
    decision_cutoff: datetime,
) -> tuple[MinuteStudySelection, ...]:
    """Score only the current, verified prefix; this function never reads trades."""
    protocol = MinuteStudyProtocol.model_validate(protocol)
    cutoff = normalize_aware_utc(decision_cutoff)
    if cutoff > protocol.requested_at:
        raise ValueError("decision clock exceeds the study request clock")
    current_date = cutoff.astimezone(SHANGHAI).date()
    partition = protocol.split.partition(current_date)
    return tuple(
        MinuteStudySelection(
            study_id=protocol.study_id,
            source_hash=protocol.source.full_input_hash,
            candidate_id=fact.candidate_id,
            ts_code=fact.ts_code,
            trade_date=fact.trade_date,
            event_time=fact.event_time,
            decision_cutoff=cutoff,
            partition=partition,
            feature_score=score,
            feature_rank=rank,
        )
        for fact, score, rank in _select_minute_study_prefix(protocol, candidates, cutoff=cutoff)
    )


def select_minute_three_part_study_candidates(
    binding: MinuteParameterStudyBinding,
    candidates: Sequence[MinuteStudyCandidate],
    *,
    decision_cutoff: datetime,
) -> tuple[MinuteStudyThreePartSelection, ...]:
    """Apply the same owner to a separately bound formal validation partition."""
    binding = MinuteParameterStudyBinding.model_validate(binding)
    protocol = binding.protocol
    cutoff = normalize_aware_utc(decision_cutoff)
    if cutoff > protocol.requested_at:
        raise ValueError("decision clock exceeds the study request clock")
    partition = binding.partition(cutoff.astimezone(SHANGHAI).date())
    return tuple(
        MinuteStudyThreePartSelection(
            study_id=protocol.study_id,
            execution_binding_hash=binding.binding_hash,
            source_hash=protocol.source.full_input_hash,
            candidate_id=fact.candidate_id,
            ts_code=fact.ts_code,
            trade_date=fact.trade_date,
            event_time=fact.event_time,
            decision_cutoff=cutoff,
            partition=partition,
            feature_score=score,
            feature_rank=rank,
        )
        for fact, score, rank in _select_minute_study_prefix(protocol, candidates, cutoff=cutoff)
    )


def _select_minute_study_prefix(
    protocol: MinuteStudyProtocol,
    candidates: Sequence[MinuteStudyCandidate],
    *,
    cutoff: datetime,
) -> tuple[tuple[MinuteStudyCandidate, float, int], ...]:
    """The sole PIT/TopN kernel for both immutable split contracts."""
    current_date = cutoff.astimezone(SHANGHAI).date()
    if len(candidates) > MAX_WORK_UNITS:
        raise ValueError("candidate prefix exceeds the original minute work budget")
    profile = topn_selection.resolve_score_profiles([protocol.score_profile])[0]
    required = {term.name for term in profile.terms}
    if profile.env_gate is not None:
        required.add(profile.env_gate.feature)
    facts: list[MinuteStudyCandidate] = []
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for value in candidates:
        fact = MinuteStudyCandidate.model_validate(value)
        if (
            fact.source != protocol.source
            or fact.head != protocol.head
            or fact.parameter_fingerprint != protocol.parameters.fingerprint
        ):
            raise ValueError("candidate does not belong to this source, head and full recipe")
        if (
            fact.trade_date != current_date
            or fact.event_time > cutoff
            or fact.available_at > cutoff
        ):
            raise ValueError("candidate is not available at the current decision")
        if any(item.available_at > cutoff for item in fact.features):
            raise ValueError("score feature is not available at the current decision")
        if not required.issubset({item.name for item in fact.features}):
            raise ValueError("required feature observation is unknown")
        if fact.candidate_id in seen:
            raise ValueError("duplicate candidate in the PIT prefix")
        seen.add(fact.candidate_id)
        facts.append(fact)
        rows.append(
            {
                **{item.name: item.value for item in fact.features},
                "candidate_id": fact.candidate_id,
                "ts_code": fact.ts_code,
                "buy_date": fact.trade_date,
                "entry_time": fact.event_time,
            }
        )
    if len({fact.ts_code for fact in facts}) > MAX_CODES:
        raise ValueError("candidate universe exceeds the original minute code budget")
    if not rows:
        return ()
    selected = topn_selection.select_topn_by_feature_score(
        pd.DataFrame(rows),
        top_n=protocol.top_n,
        score_profile=profile,
    )
    original = {fact.candidate_id: fact for fact in facts}
    return tuple(
        (original[row.candidate_id], row.feature_score, int(row.feature_rank))
        for row in selected.itertuples()
    )


class MinuteStudyTrainingSummary(RuntimeContractModel):
    trades: int = Field(strict=True, ge=0)
    mean_ret_pct: float | None = Field(strict=True, allow_inf_nan=False)
    win_rate_pct: float | None = Field(strict=True, ge=0, le=100, allow_inf_nan=False)
    worst_ret_pct: float | None = Field(strict=True, allow_inf_nan=False)
    gap_stop_rate_pct: float | None = Field(strict=True, ge=0, le=100, allow_inf_nan=False)

    @model_validator(mode="after")
    def complete_nonempty_summary(self) -> Self:
        if self.trades > 0 and any(
            value is None
            for value in (
                self.mean_ret_pct,
                self.win_rate_pct,
                self.worst_ret_pct,
                self.gap_stop_rate_pct,
            )
        ):
            raise ValueError("nonempty training summary has unavailable metrics")
        return self


class MinuteStudyTrainingObservation(RuntimeContractModel):
    study_id: StudyHash
    source: MinuteStudySource
    head: MinuteStudyHead
    parameter_fingerprint: StudyHash
    train_start: date
    train_end: date
    result_hash: StudyHash
    available_at: AwareUtcDatetime
    summary: MinuteStudyTrainingSummary

    @model_validator(mode="after")
    def ordered_training_window(self) -> Self:
        if not self.source.start_date <= self.train_start <= self.train_end <= self.source.end_date:
            raise ValueError("training result window is outside its source")
        if self.parameter_fingerprint != self.head.parameter_fingerprint:
            raise ValueError("training result recipe differs from its head")
        return self


class MinuteStudyTrainingRank(RuntimeContractModel):
    study_id: StudyHash
    result_hash: StudyHash
    available_at: AwareUtcDatetime
    selection_cutoff: AwareUtcDatetime
    training_score: float = Field(allow_inf_nan=False)
    rank: int = Field(strict=True, ge=1)


def rank_minute_study_training(
    protocols: Sequence[MinuteStudyProtocol],
    observations: Sequence[MinuteStudyTrainingObservation],
    *,
    selection_cutoff: datetime,
) -> tuple[MinuteStudyTrainingRank, ...]:
    """Use verified training summaries only; test outcomes have no input slot."""
    cutoff = normalize_aware_utc(selection_cutoff)
    if len(protocols) > MAX_WORK_UNITS or len(observations) > MAX_WORK_UNITS:
        raise ValueError("study grid exceeds the original minute work budget")
    bound: dict[str, MinuteStudyProtocol] = {}
    for value in protocols:
        protocol = MinuteStudyProtocol.model_validate(value)
        if protocol.study_id in bound:
            raise ValueError("duplicate study protocol")
        if bound:
            first = next(iter(bound.values()))
            if protocol.source != first.source or protocol.split != first.split:
                raise ValueError("training comparison mixes sources or windows")
        bound[protocol.study_id] = protocol
    verified: dict[str, MinuteStudyTrainingObservation] = {}
    scores: dict[str, float] = {}
    for value in observations:
        fact = MinuteStudyTrainingObservation.model_validate(value)
        protocol = bound.get(fact.study_id)
        if protocol is None or fact.study_id in verified:
            raise ValueError("unknown or duplicate training result")
        if (
            fact.source != protocol.source
            or fact.head != protocol.head
            or fact.parameter_fingerprint != protocol.parameters.fingerprint
            or fact.train_start != protocol.split.train_start
            or fact.train_end != protocol.split.train_end
        ):
            raise ValueError("training result does not bind the declared source, recipe and window")
        if fact.available_at > cutoff or fact.train_end > cutoff.astimezone(SHANGHAI).date():
            raise ValueError("training evidence is not yet available")
        verified[fact.study_id] = fact
        score = strategy_optimizer._score_row(
            pd.Series(fact.summary.model_dump()), min_trades=protocol.min_trades
        )
        if fact.summary.trades >= protocol.min_trades:
            scores[fact.study_id] = score
    if verified.keys() != bound.keys():
        raise ValueError("training evidence does not cover the declared study grid")
    ordered = sorted(scores, key=lambda study_id: (-scores[study_id], study_id))
    return tuple(
        MinuteStudyTrainingRank(
            study_id=study_id,
            result_hash=verified[study_id].result_hash,
            available_at=verified[study_id].available_at,
            selection_cutoff=cutoff,
            training_score=scores[study_id],
            rank=rank,
        )
        for rank, study_id in enumerate(ordered, start=1)
    )
