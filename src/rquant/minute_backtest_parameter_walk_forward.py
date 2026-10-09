"""Immutable minute WF plans; execution and sealing remain with the original worker."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import Literal, Self

from pydantic import Field, model_validator

from rquant import minute_backtest_parameter_optimizer, topn_walk_forward
from rquant.minute_backtest_contracts import MAX_DATE_SPAN, MAX_INPUT_BYTES, MAX_WORK_UNITS
from rquant.minute_backtest_parameter_optimizer import (
    MinuteStudyTrainingObservation,
    MinuteStudyTrainingRank,
)
from rquant.minute_backtest_study_protocols import MinuteStudyProtocol, MinuteStudySource
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

UnavailableReason = Literal["incomplete_calendar", "insufficient_fold_dates"]


class MinuteParameterWalkForwardRequest(RuntimeContractModel):
    """Verified calendar plus full recipe templates; WF replaces their initial split."""

    templates: tuple[MinuteStudyProtocol, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    calendar_source: MinuteStudySource
    calendar_dates: tuple[date, ...] = Field(max_length=MAX_DATE_SPAN)
    calendar_complete: bool = Field(strict=True)
    fold_count: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)
    min_train_dates: int | None = Field(default=None, strict=True, ge=1)

    @model_validator(mode="after")
    def validate_calendar_binding(self) -> Self:
        if len(set(self.calendar_dates)) != len(self.calendar_dates):
            raise ValueError("calendar contains duplicate dates")
        if len({template.study_id for template in self.templates}) != len(self.templates):
            raise ValueError("duplicate study template")
        for template in self.templates:
            if template.source != self.calendar_source:
                raise ValueError("calendar and template source identities differ")
            if any(
                not template.split.train_start <= day <= template.split.test_end
                for day in self.calendar_dates
            ):
                raise ValueError("calendar date is outside a template's declared study window")
        if len(self.model_dump_json().encode("utf-8")) > MAX_INPUT_BYTES:
            raise ValueError("study request exceeds the original minute input byte budget")
        return self

    @property
    def effective_min_train_dates(self) -> int:
        if self.min_train_dates is not None:
            return self.min_train_dates
        return max(2, len(self.calendar_dates) // 3)


class MinuteParameterWalkForwardFold(RuntimeContractModel):
    fold: int = Field(strict=True, ge=1)
    train_dates: tuple[date, ...] = Field(min_length=1, max_length=MAX_DATE_SPAN)
    test_dates: tuple[date, ...] = Field(min_length=1, max_length=MAX_DATE_SPAN)
    protocols: tuple[MinuteStudyProtocol, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)

    @model_validator(mode="after")
    def validate_declared_window(self) -> Self:
        if tuple(sorted(set(self.train_dates))) != self.train_dates:
            raise ValueError("training dates must be unique and ordered")
        if tuple(sorted(set(self.test_dates))) != self.test_dates:
            raise ValueError("test dates must be unique and ordered")
        if self.train_dates[-1] >= self.test_dates[0]:
            raise ValueError("training must be strictly earlier than testing")
        for protocol in self.protocols:
            if (
                protocol.split.train_start != self.train_dates[0]
                or protocol.split.train_end != self.train_dates[-1]
                or protocol.split.test_start != self.test_dates[0]
                or protocol.split.test_end != self.test_dates[-1]
            ):
                raise ValueError("fold protocol does not bind its complete date window")
        return self


class MinuteParameterWalkForwardPlan(RuntimeContractModel):
    request: MinuteParameterWalkForwardRequest
    effective_min_train_dates: int = Field(strict=True, ge=1)
    folds: tuple[MinuteParameterWalkForwardFold, ...] = Field(max_length=MAX_DATE_SPAN)
    state: Literal["ready", "unavailable"]
    unavailable_reasons: tuple[UnavailableReason, ...] = ()
    results_state: Literal["pending"] = "pending"

    @model_validator(mode="after")
    def validate_plan_bindings(self) -> Self:
        if self.effective_min_train_dates != self.request.effective_min_train_dates:
            raise ValueError("plan changed the requested training minimum")
        if self.state == "ready" and (
            not self.request.calendar_complete
            or len(self.folds) != self.request.fold_count
            or self.unavailable_reasons
        ):
            raise ValueError("ready plan lacks the complete requested folds")
        if self.state == "unavailable" and not self.unavailable_reasons:
            raise ValueError("unavailable plan requires its actual reason")
        if len(self.folds) * len(self.request.templates) > MAX_WORK_UNITS:
            raise ValueError("fold study grid exceeds the original minute work budget")
        calendar = set(self.request.calendar_dates)
        for index, fold in enumerate(self.folds, start=1):
            if fold.fold != index or not set(fold.train_dates + fold.test_dates).issubset(calendar):
                raise ValueError("fold is detached from the actual calendar")
            if len(fold.protocols) != len(self.request.templates):
                raise ValueError("fold does not cover the complete recipe grid")
            for template, protocol in zip(self.request.templates, fold.protocols, strict=True):
                if template.model_dump(mode="json", exclude={"split"}) != protocol.model_dump(
                    mode="json",
                    exclude={"split"},
                ):
                    raise ValueError("fold changed a full recipe, head or selection field")
        if len(self.model_dump_json().encode("utf-8")) > MAX_INPUT_BYTES:
            raise ValueError("plan exceeds the original minute input byte budget")
        return self

    @property
    def plan_id(self) -> str:
        return canonical_sha256(
            {
                "plan": self.model_dump(mode="json"),
                "template_study_ids": [template.study_id for template in self.request.templates],
            }
        )


def build_minute_parameter_walk_forward(
    request: MinuteParameterWalkForwardRequest,
) -> MinuteParameterWalkForwardPlan:
    request = MinuteParameterWalkForwardRequest.model_validate(request)
    if not request.calendar_complete:
        return MinuteParameterWalkForwardPlan(
            request=request,
            effective_min_train_dates=request.effective_min_train_dates,
            folds=(),
            state="unavailable",
            unavailable_reasons=("incomplete_calendar",),
        )
    windows = topn_walk_forward.build_expanding_folds(
        list(request.calendar_dates),
        fold_count=request.fold_count,
        min_train_dates=request.effective_min_train_dates,
    )
    if len(windows) * len(request.templates) > MAX_WORK_UNITS:
        raise ValueError("fold study grid exceeds the original minute work budget")
    folds: list[MinuteParameterWalkForwardFold] = []
    for window in windows:
        protocols: list[MinuteStudyProtocol] = []
        for template in request.templates:
            body = template.model_dump(mode="python")
            body["split"] = {
                "train_start": window.train_dates[0],
                "train_end": window.train_dates[-1],
                "test_start": window.test_dates[0],
                "test_end": window.test_dates[-1],
            }
            protocols.append(MinuteStudyProtocol.model_validate(body))
        folds.append(
            MinuteParameterWalkForwardFold(
                fold=window.fold,
                train_dates=tuple(window.train_dates),
                test_dates=tuple(window.test_dates),
                protocols=tuple(protocols),
            )
        )
    ready = len(folds) == request.fold_count
    return MinuteParameterWalkForwardPlan(
        request=request,
        effective_min_train_dates=request.effective_min_train_dates,
        folds=tuple(folds),
        state="ready" if ready else "unavailable",
        unavailable_reasons=() if ready else ("insufficient_fold_dates",),
    )


def select_minute_walk_forward_training(
    plan: MinuteParameterWalkForwardPlan,
    *,
    fold: int,
    observations: Sequence[MinuteStudyTrainingObservation],
    selection_cutoff: datetime,
) -> tuple[MinuteStudyTrainingRank, ...]:
    plan = MinuteParameterWalkForwardPlan.model_validate(plan)
    if plan != build_minute_parameter_walk_forward(plan.request):
        raise ValueError("plan does not match the original expanding-window owner")
    if plan.state != "ready" or isinstance(fold, bool) or not 1 <= fold <= len(plan.folds):
        raise ValueError("requested complete fold is unavailable")
    return minute_backtest_parameter_optimizer.rank_minute_study_training(
        plan.folds[fold - 1].protocols,
        observations,
        selection_cutoff=selection_cutoff,
    )
