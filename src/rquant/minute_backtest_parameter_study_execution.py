"""Formal study observations using the original complete result and metric owners."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from typing import TYPE_CHECKING, Literal, Self
from uuid import UUID, uuid5

import pandas as pd
from pydantic import Field, model_validator

from rquant import minute_backtest_parameter_optimizer, strategy_compare, topn_walk_forward
from rquant.experiment_registry import DateRange
from rquant.lab_job_center import CommandSubmissionReceipt
from rquant.lab_job_protocol import LabCommandEnvelope, LabSpoolEntry
from rquant.lab_jobs import JobStatus
from rquant.minute_backtest_commands import (
    MinuteCommandWriter,
    MinuteParameterRunConfig,
    MinuteRunEffect,
    SubmitMinuteReplay,
    minute_job_id,
)
from rquant.minute_backtest_contracts import (
    MAX_DATE_SPAN,
    MAX_INPUT_BYTES,
    MAX_RESULT_WIRE_BYTES,
    MAX_WORK_UNITS,
    Sha256,
)
from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_parameter_ablation import growth_board_parameter_ablation
from rquant.minute_backtest_parameter_adapter import (
    MinuteParameterFormalReplayAdapter,
    MinuteParameterFormalReplayResult,
)
from rquant.minute_backtest_parameter_artifact import (
    MinuteParameterSealedReplayReader,
    MinuteParameterSealedReplayResult,
)
from rquant.minute_backtest_parameter_heatmap import MinuteStudyHeatmap, build_minute_study_heatmap
from rquant.minute_backtest_parameter_optimizer import (
    MinuteStudyTrainingObservation,
    MinuteStudyTrainingRank,
    MinuteStudyTrainingSummary,
)
from rquant.minute_backtest_parameter_producer import (
    MinuteParameterFactSourceReference,
    MinuteParameterReplayCatalog,
)
from rquant.minute_backtest_parameter_runner import MinuteParameterReplayResult
from rquant.minute_backtest_parameter_search import (
    build_minute_parameter_search_plan,
)
from rquant.minute_backtest_parameter_study import (
    MinuteParameterStudyBinding,
)
from rquant.minute_backtest_parameter_study_commands import (
    MinuteParameterStudyExecutionRequest,
    SubmitMinuteParameterStudy,
    _study_control_size,
)
from rquant.minute_backtest_parameter_study_commands import (
    MinuteParameterStudyWindowSettings as MinuteParameterStudyWindowSettings,
)
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.minute_backtest_performance import MinutePerformanceDay, build_minute_performance
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.minute_backtest_study_protocols import (
    SHANGHAI,
    MinuteStudyHead,
    MinuteStudyProtocol,
    MinuteStudySource,
)
from rquant.paper_contracts import PaperFill, PaperSide
from rquant.paper_signal_worker import PaperSignalQueueStatus
from rquant.perf.trades import RoundTrip
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)
from rquant.signal_contracts import SignalAction
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

if TYPE_CHECKING:
    from rquant.minute_backtest_parameter_study_projection import InstalledMinuteStudyProjection


class MinuteParameterStudyWindowObservation(RuntimeContractModel):
    """A metric projection, not a statement of sealed or score-selection authority."""

    contract: Literal["minute-parameter-study-window/v1"] = "minute-parameter-study-window/v1"
    full_input_hash: Sha256
    parameter_hash: Sha256
    profile_hash: Sha256
    window: DateRange
    status: Literal["complete", "unavailable"]
    summary: MinuteStudyTrainingSummary | None = None
    daily: tuple[MinutePerformanceDay, ...]
    cross_window_trades: int = Field(strict=True, ge=0)
    unavailable_reasons: tuple[str, ...] = ()

    @model_validator(mode="after")
    def complete_window(self) -> Self:
        if self.status == "complete":
            if (
                self.summary is None
                or self.unavailable_reasons
                or not self.daily
                or any(day.status != "complete" or day.daily_return is None for day in self.daily)
            ):
                raise ValueError("complete study metrics require every original window NAV")
        elif self.summary is not None or not self.unavailable_reasons:
            raise ValueError("unavailable study metrics cannot invent a training summary")
        if any(
            not self.window.start_date <= day.trade_date <= self.window.end_date
            for day in self.daily
        ):
            raise ValueError("study NAV is outside the exact observation window")
        return self


def _window_contains(window: DateRange, day: date) -> bool:
    return window.start_date <= day <= window.end_date


def _closed_sell_bindings(
    replay: MinuteParameterReplayResult, trips: tuple[RoundTrip, ...]
) -> tuple[tuple[RoundTrip, PaperFill], ...]:
    """Attach identities to original FIFO output; do not match lots or calculate fees."""
    orders = {order.order_id: order for order in replay.orders}
    if len(orders) != len(replay.orders):
        raise ValueError("duplicate original order identity")
    bindings: list[tuple[RoundTrip, PaperFill]] = []
    index = 0
    for fill in sorted(
        replay.fills, key=lambda item: (item.executed_at, item.sequence, item.fill_id)
    ):
        order = orders[fill.order_id]
        if order.side is not PaperSide.SELL:
            continue
        quantity = fill.quantity
        while quantity > 0:
            if index == len(trips):
                raise ValueError("original sell has no complete FIFO output")
            trip = trips[index]
            if (
                trip.ts_code != order.ts_code
                or trip.exit_date != fill.executed_at.astimezone(SHANGHAI).date()
                or not 0 < trip.quantity <= quantity
            ):
                raise ValueError("original FIFO output differs from its exact sell fill")
            bindings.append((trip, fill))
            quantity -= trip.quantity
            index += 1
    if index != len(trips):
        raise ValueError("original closed trip has no complete sell fill")
    return tuple(bindings)


def _original_exit_reason(replay: MinuteParameterReplayResult, fill: PaperFill) -> str:
    (order,) = (item for item in replay.orders if item.order_id == fill.order_id)
    matches = tuple(
        record
        for record in replay.queue_records
        if record.order is not None and record.order.order_id == fill.order_id
    )
    if len(matches) != 1:
        raise ValueError("sell requires its single original queue execution")
    (record,) = matches
    signal = record.signal
    signals = tuple(item for item in replay.signals if item.signal_id == signal.signal_id)
    if (
        record.status is not PaperSignalQueueStatus.COMPLETED
        or record.order != order
        or record.execution_id != fill.execution_id
        or record.intent is None
        or record.quote is None
        or record.intent.intent_id != order.intent_id
        or record.intent.signal_id != signal.signal_id
        or record.intent.account_id != order.account_id
        or order.account_id != replay.account.account_id
        or record.intent.ts_code != order.ts_code
        or record.quote.ts_code != order.ts_code
        or record.intent.side is not PaperSide.SELL
        or record.quote.snapshot_id != fill.price_snapshot_id
        or record.intent.price_snapshot_id != fill.price_snapshot_id
        or signal.action is not SignalAction.S_INTENT
        or signals != (signal,)
    ):
        raise ValueError("sell fill differs from its original signal/intent/order/quote")
    reason = signal.evidence.get("exit_reason")
    if not isinstance(reason, str) or not reason or reason not in signal.reason_codes:
        raise ValueError("original sell exit reason is unavailable or inconsistent")
    return reason


def project_minute_parameter_study_window(
    result: MinuteParameterFormalReplayResult, *, window: DateRange
) -> MinuteParameterStudyWindowObservation:
    """Project facts after full reading; callers still bind owner/head/study controls."""
    window = DateRange.model_validate(window)
    frozen = result.publication.frozen
    runtime = frozen.runtime
    if not runtime.start_date <= window.start_date <= window.end_date <= runtime.end_date:
        raise ValueError("study window is outside its complete frozen input")
    if (result.replay.parameters, result.replay.parameter_work) != (
        runtime.parameters,
        runtime.parameter_work,
    ):
        raise ValueError("study metrics differ from their complete parameter input")
    performance = build_minute_performance(result.replay, runtime=runtime)
    daily = tuple(day for day in performance.daily if _window_contains(window, day.trade_date))
    reasons = list(performance.unavailable_reasons)
    if not daily:
        reasons.append("window_has_no_original_trading_dates")
    rows: list[dict[str, object]] = []
    cross_window_trades = 0
    if performance.metrics is not None:
        bindings = _closed_sell_bindings(result.replay, performance.metrics.round_trips)
        for trip, fill in bindings:
            inside = _window_contains(window, trip.entry_date) and _window_contains(
                window, trip.exit_date
            )
            if not inside:
                if trip.entry_date <= window.end_date and trip.exit_date >= window.start_date:
                    cross_window_trades += 1
                continue
            try:
                reason = _original_exit_reason(result.replay, fill)
            except ValueError:
                reasons.append("sell_execution_identity_unavailable")
                break
            rows.append({"ret_pct": trip.return_rate * 100, "exit_reason": reason})
    summary = None
    if not reasons:
        # These two discarded labels do not affect any metric or execute a strategy.
        original = strategy_compare._summary_row("first_break", "baseline", pd.DataFrame(rows), 0)
        summary = MinuteStudyTrainingSummary.model_validate(
            {name: original[name] for name in MinuteStudyTrainingSummary.model_fields}
        )
    return MinuteParameterStudyWindowObservation(
        full_input_hash=result.full_input_hash,
        parameter_hash=runtime.parameters.fingerprint,
        profile_hash=result.replay.profile_hash,
        window=window,
        status="unavailable" if reasons else "complete",
        summary=summary,
        daily=daily,
        cross_window_trades=cross_window_trades,
        unavailable_reasons=tuple(dict.fromkeys(reasons)),
    )


class MinuteParameterStudySealedWindows(RuntimeContractModel):
    """Full sealed facts and three separate metrics; score controls are not attested here."""

    sealed: MinuteParameterSealedReplayResult
    read_at: AwareUtcDatetime
    training: MinuteParameterStudyWindowObservation
    validation: MinuteParameterStudyWindowObservation
    independent_test: MinuteParameterStudyWindowObservation

    @model_validator(mode="after")
    def retain_exact_formal_partitions(self) -> Self:
        if self.read_at < self.sealed.completed_at:
            raise ValueError("study metrics cannot predate the original visible seal")
        experiment = self.sealed.accepted_spec.experiment
        if experiment is None:
            raise ValueError("study metrics require the original complete formal experiment")
        for observation, window in (
            (self.training, experiment.spec.train_range),
            (self.validation, experiment.spec.validation_range),
            (self.independent_test, experiment.spec.frozen_outer_test_range),
        ):
            if (
                observation.window != window
                or observation.full_input_hash != self.sealed.full_input_hash
            ):
                raise ValueError("study metrics differ from their actual sealed partition/input")
        return self


def read_minute_parameter_study_windows(
    reader: MinuteParameterSealedReplayReader,
    *,
    job_id: UUID,
    owner_id: str,
    parameters: MinuteParameterSet,
    as_of: datetime,
) -> MinuteParameterStudySealedWindows | None:
    """Use the existing full reader; training, validation and outer facts remain separate."""
    parameters = MinuteParameterSet.model_validate(parameters)
    read_at = normalize_aware_utc(as_of)
    sealed = reader.read(
        job_id,
        owner_id=owner_id,
        native_id=parameters.definition_id,
        native_version=parameters.definition_version,
        as_of=read_at,
    )
    if sealed is None:
        return None
    if sealed.result.replay.parameters != parameters:
        raise PermissionError("sealed study result differs from its complete expected recipe")
    return project_minute_parameter_study_sealed_windows(sealed, as_of=read_at)


def project_minute_parameter_study_sealed_windows(
    sealed: MinuteParameterSealedReplayResult,
    *,
    as_of: datetime,
) -> MinuteParameterStudySealedWindows:
    """Project a full reader output; a caller-supplied wire is not read authority."""
    sealed = MinuteParameterSealedReplayResult.model_validate(sealed)
    read_at = normalize_aware_utc(as_of)
    experiment = sealed.accepted_spec.experiment
    if experiment is None:
        raise PermissionError("sealed study result has no original formal experiment")
    return MinuteParameterStudySealedWindows(
        sealed=sealed,
        read_at=read_at,
        training=project_minute_parameter_study_window(
            sealed.result, window=experiment.spec.train_range
        ),
        validation=project_minute_parameter_study_window(
            sealed.result, window=experiment.spec.validation_range
        ),
        independent_test=project_minute_parameter_study_window(
            sealed.result, window=experiment.spec.frozen_outer_test_range
        ),
    )


class MinuteParameterThreePartWalkForwardRequest(RuntimeContractModel):
    """Explicit validation tail; source and installed head authority remain external."""

    contract: Literal["minute-parameter-three-part-walk-forward/v1"] = (
        "minute-parameter-three-part-walk-forward/v1"
    )
    templates: tuple[MinuteStudyProtocol, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    calendar_source: MinuteStudySource
    calendar_dates: tuple[date, ...] = Field(max_length=MAX_DATE_SPAN)
    calendar_complete: bool = Field(strict=True)
    fold_count: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)
    min_training_dates: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)
    validation_date_count: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)

    @model_validator(mode="after")
    def full_calendar_binding(self) -> Self:
        if len(set(self.calendar_dates)) != len(self.calendar_dates):
            raise ValueError("calendar contains duplicate dates")
        if self.fold_count * len(self.templates) > MAX_WORK_UNITS:
            raise ValueError("fold study grid exceeds the original minute work budget")
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
    def owner_min_train_dates(self) -> int:
        return self.min_training_dates + self.validation_date_count


class MinuteParameterThreePartWalkForwardFold(RuntimeContractModel):
    fold: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)
    train_dates: tuple[date, ...] = Field(min_length=1, max_length=MAX_DATE_SPAN)
    validation_dates: tuple[date, ...] = Field(min_length=1, max_length=MAX_DATE_SPAN)
    test_dates: tuple[date, ...] = Field(min_length=1, max_length=MAX_DATE_SPAN)
    protocols: tuple[MinuteStudyProtocol, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)

    @model_validator(mode="after")
    def three_separate_ranges(self) -> Self:
        for dates in (self.train_dates, self.validation_dates, self.test_dates):
            if tuple(sorted(set(dates))) != dates:
                raise ValueError("fold dates must be unique and ordered")
        if (
            not self.train_dates[-1]
            < self.validation_dates[0]
            <= (self.validation_dates[-1])
            < self.test_dates[0]
        ):
            raise ValueError("training, validation and independent test must be ordered")
        for protocol in self.protocols:
            if (
                protocol.split.train_start != self.train_dates[0]
                or protocol.split.train_end != self.train_dates[-1]
                or protocol.split.test_start != self.test_dates[0]
                or protocol.split.test_end != self.test_dates[-1]
            ):
                raise ValueError("fold protocol differs from the actual training/test windows")
        return self

    @property
    def train_range(self) -> DateRange:
        return DateRange(start_date=self.train_dates[0], end_date=self.train_dates[-1])

    @property
    def validation_range(self) -> DateRange:
        return DateRange(start_date=self.validation_dates[0], end_date=self.validation_dates[-1])

    @property
    def out_of_sample_range(self) -> DateRange:
        return DateRange(start_date=self.test_dates[0], end_date=self.test_dates[-1])

    @property
    def formal_protocol(self) -> MinuteExperimentProtocol:
        return MinuteExperimentProtocol(
            train_range=self.train_range,
            validation_range=self.validation_range,
            frozen_outer_test_range=self.out_of_sample_range,
        )


class MinuteParameterThreePartWalkForwardPlan(RuntimeContractModel):
    request: MinuteParameterThreePartWalkForwardRequest
    owner_min_train_dates: int = Field(strict=True, ge=2)
    folds: tuple[MinuteParameterThreePartWalkForwardFold, ...] = Field(max_length=MAX_DATE_SPAN)
    state: Literal["ready", "unavailable"]
    unavailable_reasons: tuple[Literal["incomplete_calendar", "insufficient_fold_dates"], ...] = ()
    results_state: Literal["pending"] = "pending"

    @model_validator(mode="after")
    def full_request_and_date_binding(self) -> Self:
        if self.owner_min_train_dates != self.request.owner_min_train_dates:
            raise ValueError("plan changed the requested training/validation minima")
        if not self.request.calendar_complete:
            if (
                self.folds
                or self.state != "unavailable"
                or self.unavailable_reasons != ("incomplete_calendar",)
            ):
                raise ValueError("incomplete calendar cannot produce a ready fold")
        elif len(self.folds) == self.request.fold_count:
            if self.state != "ready" or self.unavailable_reasons:
                raise ValueError("complete requested folds require a ready plan")
        elif (
            len(self.folds) > self.request.fold_count
            or self.state != "unavailable"
            or self.unavailable_reasons != ("insufficient_fold_dates",)
        ):
            raise ValueError("incomplete folds cannot be presented as a ready plan")
        calendar = tuple(sorted(self.request.calendar_dates))
        for index, fold in enumerate(self.folds, start=1):
            history = fold.train_dates + fold.validation_dates
            if (
                fold.fold != index
                or len(fold.train_dates) < self.request.min_training_dates
                or len(fold.validation_dates) != self.request.validation_date_count
                or history != calendar[: len(history)]
                or fold.test_dates != calendar[len(history) : len(history) + len(fold.test_dates)]
            ):
                raise ValueError("fold changed the original calendar prefix/validation tail/test")
            if len(fold.protocols) != len(self.request.templates):
                raise ValueError("fold does not cover the full recipe grid")
            for template, protocol in zip(self.request.templates, fold.protocols, strict=True):
                if template.model_dump(mode="json", exclude={"split"}) != protocol.model_dump(
                    mode="json", exclude={"split"}
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


def build_minute_parameter_three_part_walk_forward(
    request: MinuteParameterThreePartWalkForwardRequest,
) -> MinuteParameterThreePartWalkForwardPlan:
    request = MinuteParameterThreePartWalkForwardRequest.model_validate(request)
    if not request.calendar_complete:
        return MinuteParameterThreePartWalkForwardPlan(
            request=request,
            owner_min_train_dates=request.owner_min_train_dates,
            folds=(),
            state="unavailable",
            unavailable_reasons=("incomplete_calendar",),
        )
    windows = topn_walk_forward.build_expanding_folds(
        list(request.calendar_dates),
        fold_count=request.fold_count,
        min_train_dates=request.owner_min_train_dates,
    )
    folds: list[MinuteParameterThreePartWalkForwardFold] = []
    for window in windows:
        history = tuple(window.train_dates)
        training = history[: -request.validation_date_count]
        validation = history[-request.validation_date_count :]
        protocols: list[MinuteStudyProtocol] = []
        for template in request.templates:
            body = template.model_dump(mode="python")
            body["split"] = {
                "train_start": training[0],
                "train_end": training[-1],
                "test_start": window.test_dates[0],
                "test_end": window.test_dates[-1],
            }
            protocols.append(MinuteStudyProtocol.model_validate(body))
        folds.append(
            MinuteParameterThreePartWalkForwardFold(
                fold=window.fold,
                train_dates=training,
                validation_dates=validation,
                test_dates=tuple(window.test_dates),
                protocols=tuple(protocols),
            )
        )
    ready = len(folds) == request.fold_count
    return MinuteParameterThreePartWalkForwardPlan(
        request=request,
        owner_min_train_dates=request.owner_min_train_dates,
        folds=tuple(folds),
        state="ready" if ready else "unavailable",
        unavailable_reasons=() if ready else ("insufficient_fold_dates",),
    )


def select_minute_parameter_three_part_training(
    plan: MinuteParameterThreePartWalkForwardPlan,
    *,
    fold: int,
    observations: Sequence[MinuteStudyTrainingObservation],
    selection_cutoff: datetime,
) -> tuple[MinuteStudyTrainingRank, ...]:
    plan = MinuteParameterThreePartWalkForwardPlan.model_validate(plan)
    if plan != build_minute_parameter_three_part_walk_forward(plan.request):
        raise ValueError("plan does not match the original expanding-window owner")
    if plan.state != "ready" or type(fold) is not int or not 1 <= fold <= len(plan.folds):
        raise ValueError("requested complete three-part fold is unavailable")
    return minute_backtest_parameter_optimizer.rank_minute_study_training(
        plan.folds[fold - 1].protocols,
        observations,
        selection_cutoff=selection_cutoff,
    )


def bind_minute_parameter_three_part_walk_forward(
    plan: MinuteParameterThreePartWalkForwardPlan,
) -> tuple[tuple[MinuteParameterStudyBinding, ...], ...]:
    """Plan values only; the original prepare owner builds each actual config binding."""
    plan = MinuteParameterThreePartWalkForwardPlan.model_validate(plan)
    if plan != build_minute_parameter_three_part_walk_forward(plan.request):
        raise ValueError("plan does not match the original expanding-window owner")
    if plan.state != "ready":
        raise ValueError("complete three-part study folds are unavailable")
    request_hash = plan.plan_id
    return tuple(
        tuple(
            MinuteParameterStudyBinding.from_formal_protocol(
                protocol=protocol,
                formal_protocol=fold.formal_protocol,
                request_hash=request_hash,
            )
            for protocol in fold.protocols
        )
        for fold in plan.folds
    )


class MinuteParameterStudyTrial(RuntimeContractModel):
    index: int = Field(strict=True, ge=0, lt=MAX_WORK_UNITS)
    variant_key: str = Field(min_length=1, max_length=128)
    label: str = Field(min_length=1, max_length=128)
    fold: int = Field(strict=True, ge=1, le=MAX_DATE_SPAN)
    command: SubmitMinuteReplay

    @model_validator(mode="after")
    def actual_parameter_branch(self) -> Self:
        if not isinstance(self.command.config, MinuteParameterRunConfig):
            raise ValueError("study trials require the original complete parameter command")
        if self.command.config.study is None:
            raise ValueError("study trials require actual selection controls")
        return self

    @property
    def job_id(self) -> UUID:
        return minute_job_id(self.command.actor_id, self.command.command_id)


def _study_recipes(
    request: MinuteParameterStudyExecutionRequest,
) -> tuple[tuple[str, str, MinuteParameterSet], ...]:
    if request.mode == "ablation":
        return tuple(
            (item.key, item.label, item.parameters)
            for item in growth_board_parameter_ablation(request.parameters)
        )
    if request.search is not None:
        recipes = build_minute_parameter_search_plan(request.search).trials
        return tuple((f"recipe:{i}", f"参数 {i + 1}", recipe) for i, recipe in enumerate(recipes))
    return (("baseline", "当前参数", request.parameters),)


def _study_windows(
    request: MinuteParameterStudyExecutionRequest, calendar_dates: tuple[date, ...]
) -> tuple[MinuteExperimentProtocol, ...]:
    settings = request.walk_forward
    if settings is None:
        return (request.formal_protocol,)
    folds = topn_walk_forward.build_expanding_folds(
        list(calendar_dates),
        fold_count=settings.fold_count,
        min_train_dates=settings.min_training_dates + settings.validation_date_count,
    )
    if len(folds) != settings.fold_count:
        return ()
    return tuple(
        MinuteExperimentProtocol(
            train_range=DateRange(
                start_date=fold.train_dates[0],
                end_date=fold.train_dates[-settings.validation_date_count - 1],
            ),
            validation_range=DateRange(
                start_date=fold.train_dates[-settings.validation_date_count],
                end_date=fold.train_dates[-1],
            ),
            frozen_outer_test_range=DateRange(
                start_date=fold.test_dates[0], end_date=fold.test_dates[-1]
            ),
        )
        for fold in folds
    )


def _study_trials(
    request: MinuteParameterStudyExecutionRequest, calendar_dates: tuple[date, ...]
) -> tuple[MinuteParameterStudyTrial, ...]:
    recipes = _study_recipes(request)
    requested_folds = request.walk_forward.fold_count if request.walk_forward else 1
    if len(recipes) * len(request.settings) * requested_folds > MAX_WORK_UNITS:
        raise ValueError("study trial count exceeds the original 20,000 finite budget")
    windows = _study_windows(request, calendar_dates)
    trials: list[MinuteParameterStudyTrial] = []
    size = 0
    for fold, formal in enumerate(windows, start=1):
        for variant_key, label, recipe in recipes:
            for settings in request.settings:
                index = len(trials)
                trial = MinuteParameterStudyTrial(
                    index=index,
                    variant_key=variant_key,
                    label=label,
                    fold=fold,
                    command=SubmitMinuteReplay(
                        command_id=str(uuid5(request.request_id, f"rquant.minute-study:{index}")),
                        actor_id=request.owner_id,
                        requested_at=request.requested_at,
                        config=MinuteParameterRunConfig(
                            source_key=request.source_key,
                            source_version=request.source_version,
                            full_input_hash=request.full_input_hash,
                            parameters=recipe,
                            protocol=formal,
                            random_seed=request.random_seed,
                            deadline=request.deadline,
                            study=settings,
                        ),
                    ),
                )
                size += len(trial.model_dump_json().encode("utf-8"))
                if size > MAX_MINUTE_CONTROL_BYTES:
                    raise ValueError("study trials exceed the original minute control byte budget")
                trials.append(trial)
    return tuple(trials)


class MinuteParameterStudyExecutionPlan(RuntimeContractModel):
    """Exact trial commands; neither this identity nor a DTO is installation authority."""

    request: MinuteParameterStudyExecutionRequest
    baseline_source: MinuteStudySource
    baseline_reference: MinuteParameterFactSourceReference
    baseline_head: MinuteStudyHead
    calendar_dates: tuple[date, ...] = Field(min_length=1, max_length=MAX_DATE_SPAN)
    trials: tuple[MinuteParameterStudyTrial, ...] = Field(max_length=MAX_WORK_UNITS)
    state: Literal["ready", "unavailable"]
    unavailable_reasons: tuple[Literal["insufficient_fold_dates"], ...] = ()

    @model_validator(mode="after")
    def full_request_binding(self) -> Self:
        source, request = self.baseline_source, self.request
        reference = self.baseline_reference
        if (
            reference.source_key,
            reference.source_version,
            reference.owner_id,
            reference.full_input_hash,
        ) != (source.source_key, source.source_version, source.owner_id, source.full_input_hash):
            raise ValueError("study plan changed its complete physical baseline reference")
        if (
            source.source_key,
            source.source_version,
            source.owner_id,
            source.full_input_hash,
            source.frequency,
        ) != (
            request.source_key,
            request.source_version,
            request.owner_id,
            request.full_input_hash,
            request.parameters.parameters.freq,
        ):
            raise ValueError("study plan changed the complete original source selection")
        if source.published_at > request.requested_at:
            raise ValueError("study plan source was unavailable at the request")
        if tuple(sorted(set(self.calendar_dates))) != self.calendar_dates:
            raise ValueError("study plan calendar must preserve unique ordered original dates")
        for window in (
            request.formal_protocol.train_range,
            request.formal_protocol.validation_range,
            request.formal_protocol.frozen_outer_test_range,
        ):
            if not source.start_date <= window.start_date <= window.end_date <= source.end_date:
                raise ValueError("study plan window is outside its complete installed source")
        if any(
            not request.formal_protocol.train_range.start_date
            <= day
            <= request.formal_protocol.frozen_outer_test_range.end_date
            for day in self.calendar_dates
        ):
            raise ValueError("study calendar is outside the declared full research window")
        expected = _study_trials(request, self.calendar_dates)
        if self.trials != expected:
            raise ValueError("study plan changed the original recipe/window/selection/UUID grid")
        if bool(expected) != (self.state == "ready") or self.unavailable_reasons != (
            () if expected else ("insufficient_fold_dates",)
        ):
            raise ValueError("study plan cannot present missing folds as executable trials")
        _study_control_size(self)
        return self

    @property
    def plan_id(self) -> str:
        return canonical_sha256(self.model_dump(mode="json", exclude_computed_fields=True))

    @property
    def trial_count(self) -> int:
        return len(self.trials)

    @property
    def random_seed(self) -> int:
        return self.request.random_seed


def build_minute_parameter_study_execution(
    request: MinuteParameterStudyExecutionRequest,
    *,
    catalog: MinuteParameterReplayCatalog,
    as_of: datetime,
) -> MinuteParameterStudyExecutionPlan:
    request = MinuteParameterStudyExecutionRequest.model_validate(request)
    now = normalize_aware_utc(as_of)
    receipt = catalog.resolve_fact(
        source_key=request.source_key,
        source_version=request.source_version,
        owner_id=request.owner_id,
        full_input_hash=request.full_input_hash,
    )
    runtime, provenance = receipt.frozen.runtime, receipt.frozen.provenance
    if not provenance.published_at <= request.requested_at <= now < request.deadline:
        raise PermissionError("study original source/request/deadline is unavailable")
    if (runtime.parameters.parameters.family, runtime.source_frequency) != (
        request.parameters.parameters.family,
        request.parameters.parameters.freq,
    ):
        raise PermissionError("study recipe family/frequency differs from complete installed facts")
    source = MinuteStudySource(
        source_key=runtime.source_key,
        source_version=runtime.source_version,
        owner_id=runtime.owner_id,
        full_input_hash=receipt.frozen.full_input_hash,
        dataset_snapshot_id=runtime.dataset_snapshot_id,
        frequency=runtime.source_frequency,
        start_date=runtime.start_date,
        end_date=runtime.end_date,
        published_at=provenance.published_at,
    )
    reference = next(
        item
        for item in catalog.fact_sources
        if (item.source_key, item.source_version, item.owner_id, item.full_input_hash)
        == (request.source_key, request.source_version, request.owner_id, request.full_input_hash)
    )
    native = receipt.frozen.native_registration
    head = MinuteStudyHead(
        definition_id=native.logical_id,
        definition_version=native.version,
        evaluator_semantic_version=runtime.parameters.evaluator_semantic_version,
        parameter_fingerprint=runtime.parameters.fingerprint,
        registration_fingerprint=native.fingerprint,
        spec_fingerprint=native.spec.spec_fingerprint,
        executable_fingerprint=native.executable_fingerprint,
        producer_commit=runtime.producer_commit,
    )
    dates = tuple(
        day
        for day in runtime.daily_trade_dates
        if request.formal_protocol.train_range.start_date
        <= day
        <= request.formal_protocol.frozen_outer_test_range.end_date
    )
    trials = _study_trials(request, dates)
    return MinuteParameterStudyExecutionPlan(
        request=request,
        baseline_source=source,
        baseline_reference=reference,
        baseline_head=head,
        calendar_dates=dates,
        trials=trials,
        state="ready" if trials else "unavailable",
        unavailable_reasons=() if trials else ("insufficient_fold_dates",),
    )


class MinuteParameterPreparedStudyTrial(RuntimeContractModel):
    plan_id: Sha256
    trial: MinuteParameterStudyTrial
    marker: MinuteRunEffect
    binding: MinuteParameterStudyBinding
    full_input_hash: Sha256
    core_input_hash: Sha256
    seed_hash: Sha256
    profile_hash: Sha256
    work_units: int = Field(strict=True, ge=1, le=MAX_WORK_UNITS)

    @model_validator(mode="after")
    def retain_actual_preparation(self) -> Self:
        config = self.trial.command.config
        if not isinstance(config, MinuteParameterRunConfig):
            raise ValueError("prepared study needs its original parameter command")
        self.binding.verify_request(config)
        if (self.marker.command.job_id, self.marker.config_hash) != (
            self.trial.job_id,
            self.binding.request_hash,
        ):
            raise ValueError("prepared study differs from its original task or config hash")
        _study_control_size(self)
        return self


def _bound_study_preparations(
    plan: MinuteParameterStudyExecutionPlan,
    prepared: Sequence[MinuteParameterPreparedStudyTrial],
) -> dict[int, MinuteParameterPreparedStudyTrial]:
    if len(prepared) > plan.trial_count:
        raise ValueError("study preparations exceed the complete requested grid")
    refs: dict[int, MinuteParameterPreparedStudyTrial] = {}
    for value in prepared:
        ref = MinuteParameterPreparedStudyTrial.model_validate(value)
        index = ref.trial.index
        if (
            index in refs
            or not 0 <= index < plan.trial_count
            or ref.plan_id != plan.plan_id
            or ref.trial != plan.trials[index]
            or ref.binding.protocol.source != plan.baseline_source
        ):
            raise PermissionError("prepared study is duplicate or differs from its complete plan")
        refs[index] = ref
    return refs


class MinuteParameterStudyExecutionEffect(RuntimeContractModel):
    """Private original Outbox payload; it supplies no new queue or actor authority."""

    contract: Literal["minute-parameter-study-effect/v1"] = "minute-parameter-study-effect/v1"
    command: SubmitMinuteParameterStudy
    plan: MinuteParameterStudyExecutionPlan
    prepared: tuple[MinuteParameterPreparedStudyTrial, ...] = Field(max_length=MAX_WORK_UNITS)

    @model_validator(mode="after")
    def exact_original_plan_and_children(self) -> Self:
        if self.command.request != self.plan.request:
            raise ValueError("study effect changed its complete original request")
        refs = _bound_study_preparations(self.plan, self.prepared)
        if tuple(refs) != tuple(sorted(refs)):
            raise ValueError("study effect must retain ordered original prepared trials")
        _study_control_size(self)
        return self


def prepare_minute_parameter_study_trial(
    plan: MinuteParameterStudyExecutionPlan,
    *,
    trial_index: int,
    writer: MinuteCommandWriter,
) -> MinuteParameterPreparedStudyTrial:
    plan = MinuteParameterStudyExecutionPlan.model_validate(plan)
    if (
        plan.state != "ready"
        or type(trial_index) is not int
        or not 0 <= trial_index < plan.trial_count
    ):
        raise ValueError("complete requested study trial is unavailable")
    trial = plan.trials[trial_index]
    catalog = writer.installation.profile.parameter_catalog
    if catalog is None:
        raise PermissionError("study preparation requires the original installed parameter source")
    if plan.baseline_reference not in catalog.fact_sources:
        raise PermissionError("study original installed physical baseline changed")
    marker = strict_model_validate_json(
        MinuteRunEffect, canonical_json_bytes(writer.freeze(trial.command))
    )
    adapter = MinuteParameterFormalReplayAdapter(catalog)
    parameters = adapter.parameters(marker.command.spec)
    expected = adapter.expected(parameters)
    runtime = expected.frozen.runtime
    binding = runtime.study_binding
    if binding is None or binding.protocol.source != plan.baseline_source:
        raise PermissionError(
            "actual prepared study lacks the complete requested baseline/selection"
        )
    binding.verify_request(trial.command.config)
    return MinuteParameterPreparedStudyTrial(
        plan_id=plan.plan_id,
        trial=trial,
        marker=marker,
        binding=binding,
        full_input_hash=expected.frozen.full_input_hash,
        core_input_hash=expected.frozen.core_input_hash,
        seed_hash=expected.frozen.source_content_seed.seed_hash,
        profile_hash=runtime.execution_profile.profile_hash,
        work_units=parameters.work_units,
    )


def submit_minute_parameter_study_trial(
    prepared: MinuteParameterPreparedStudyTrial,
    *,
    writer: MinuteCommandWriter,
) -> CommandSubmissionReceipt:
    """Original spool receipt only; the writer also handles exact UUID recovery."""
    prepared = MinuteParameterPreparedStudyTrial.model_validate(prepared)
    return strict_model_validate_json(
        CommandSubmissionReceipt,
        canonical_json_bytes(
            writer.submit(prepared.trial.command, prepared.marker.model_dump(mode="json"))
        ),
    )


class MinuteParameterStudyTrialResult(RuntimeContractModel):
    prepared: MinuteParameterPreparedStudyTrial
    spec_hash: Sha256
    manifest_hash: Sha256
    complete_result_hash: Sha256
    result_hash: Sha256
    completed_at: AwareUtcDatetime
    read_at: AwareUtcDatetime
    training: MinuteParameterStudyWindowObservation
    validation: MinuteParameterStudyWindowObservation
    independent_test: MinuteParameterStudyWindowObservation

    @model_validator(mode="after")
    def exact_original_observation(self) -> Self:
        if (
            self.read_at < self.completed_at
            or self.spec_hash != self.prepared.marker.command.spec.spec_hash
        ):
            raise ValueError("study result differs from its visible original spec/seal")
        binding = self.prepared.binding
        for observation, window in (
            (self.training, binding.train_range),
            (self.validation, binding.validation_range),
            (self.independent_test, binding.frozen_outer_test_range),
        ):
            if (
                observation.window,
                observation.full_input_hash,
                observation.parameter_hash,
                observation.profile_hash,
            ) != (
                window,
                self.prepared.full_input_hash,
                binding.protocol.parameters.fingerprint,
                self.prepared.profile_hash,
            ):
                raise ValueError("study result changed its original partition/input/recipe/profile")
        return self

    @property
    def training_observation(self) -> MinuteStudyTrainingObservation | None:
        if self.training.summary is None:
            return None
        protocol = self.prepared.binding.protocol
        return MinuteStudyTrainingObservation(
            study_id=protocol.study_id,
            source=protocol.source,
            head=protocol.head,
            parameter_fingerprint=protocol.parameters.fingerprint,
            train_start=self.training.window.start_date,
            train_end=self.training.window.end_date,
            result_hash=self.result_hash,
            available_at=self.read_at,
            summary=self.training.summary,
        )


class MinuteParameterStudyTrialState(RuntimeContractModel):
    index: int = Field(strict=True, ge=0, lt=MAX_WORK_UNITS)
    job_id: UUID
    state: Literal[
        "not_prepared",
        "awaiting_submission_receipt",
        "pending",
        "queued",
        "running",
        "checkpointed",
        "pending_seal",
        "sealed",
        "failed",
        "cancelled",
        "rejected",
        "unknown",
    ]
    reason: str | None = None


def _unsealed_study_state(
    reader: MinuteParameterSealedReplayReader,
    ref: MinuteParameterPreparedStudyTrial,
    now: datetime,
) -> MinuteParameterStudyTrialState:
    job = reader.reader.get_job(ref.trial.job_id)
    if job is not None:
        if job.spec != ref.marker.command.spec:
            raise PermissionError("study job differs from its complete original prepared task")
        state = (
            "unknown"
            if job.updated_at > now
            else ("pending_seal" if job.status is JobStatus.SUCCEEDED else job.status.value)
        )
        return MinuteParameterStudyTrialState(
            index=ref.trial.index, job_id=ref.trial.job_id, state=state
        )
    facade = reader.submission_facade
    registry = facade.experiment_registry
    if registry is None:
        raise PermissionError("study state lacks the original experiment registry")
    intent = registry.get_submission_intent_for_job(ref.trial.job_id)
    state, reason = "awaiting_submission_receipt", None
    if intent is not None:
        envelope = strict_model_validate_json(LabCommandEnvelope, intent.envelope_json)
        if envelope.command != ref.marker.command:
            raise PermissionError("study original submission intent has another complete task")
        observed = facade.spool.find(envelope.request_id)
        state = "unknown"
        if isinstance(observed, LabSpoolEntry):
            if observed.envelope != envelope:
                raise PermissionError("study original pending body changed")
            state = "pending"
        elif observed is not None:
            if (observed.receipt.content_hash, observed.receipt.job_id) != (
                envelope.content_hash,
                ref.trial.job_id,
            ):
                raise PermissionError("study original queue receipt changed")
            if observed.receipt.status == "rejected":
                state, reason = "rejected", observed.receipt.reason
    return MinuteParameterStudyTrialState(
        index=ref.trial.index, job_id=ref.trial.job_id, state=state, reason=reason
    )


class MinuteParameterStudyExecutionResult(RuntimeContractModel):
    plan: MinuteParameterStudyExecutionPlan
    read_at: AwareUtcDatetime
    state: Literal["pending", "complete", "unavailable"]
    results: tuple[MinuteParameterStudyTrialResult, ...] = Field(max_length=MAX_WORK_UNITS)
    trial_states: tuple[MinuteParameterStudyTrialState, ...] = Field(max_length=MAX_WORK_UNITS)
    missing_trial_indices: tuple[int, ...] = Field(max_length=MAX_WORK_UNITS)
    training_ranks: tuple[MinuteStudyTrainingRank, ...] = Field(max_length=MAX_WORK_UNITS)
    unavailable_reasons: tuple[str, ...] = ()

    @model_validator(mode="after")
    def complete_trial_coverage(self) -> Self:
        if tuple(item.index for item in self.trial_states) != tuple(range(self.plan.trial_count)):
            raise ValueError("study state does not cover every original trial exactly once")
        indices = tuple(item.prepared.trial.index for item in self.results)
        if tuple(sorted(set(indices))) != indices:
            raise ValueError("study has duplicate or unordered result identities")
        if self.missing_trial_indices != tuple(
            i for i in range(self.plan.trial_count) if i not in indices
        ):
            raise ValueError("study omitted an original missing result")
        if any(
            item.prepared.plan_id != self.plan.plan_id
            or item.prepared.trial != self.plan.trials[item.prepared.trial.index]
            or item.read_at != self.read_at
            for item in self.results
        ):
            raise ValueError("study result belongs to another complete request or read")
        if self.missing_trial_indices and self.training_ranks:
            raise ValueError(
                "partial submitted trials cannot produce a complete-grid training rank"
            )
        if len(self.model_dump_json().encode("utf-8")) > MAX_RESULT_WIRE_BYTES:
            raise ValueError("study aggregate exceeds the original complete result byte budget")
        return self


def read_minute_parameter_study_execution(
    plan: MinuteParameterStudyExecutionPlan,
    *,
    prepared: Sequence[MinuteParameterPreparedStudyTrial],
    reader: MinuteParameterSealedReplayReader,
    as_of: datetime,
    projection: InstalledMinuteStudyProjection | None = None,
) -> MinuteParameterStudyExecutionResult:
    """Fresh authorized reads; validation/test have no ranking input slot."""
    if projection is not None:
        from rquant.minute_backtest_parameter_study_projection import InstalledMinuteStudyProjection

        if type(projection) is not InstalledMinuteStudyProjection or projection.installation.reader is not reader.reader:
            raise TypeError("study projection requires the same original installed reader")
    plan = MinuteParameterStudyExecutionPlan.model_validate(plan)
    now = normalize_aware_utc(as_of)
    refs = _bound_study_preparations(plan, prepared)
    results: list[MinuteParameterStudyTrialResult] = []
    missing: list[int] = []
    states: list[MinuteParameterStudyTrialState] = []
    for trial in plan.trials:
        ref = refs.get(trial.index)
        derived = None if ref is None or projection is None else projection.read_trial(ref, as_of=now)
        if derived is not None:
            if type(derived) is not MinuteParameterStudyTrialResult or derived.prepared != ref or derived.read_at != now:
                raise PermissionError("derived study result differs from actual preparation/read")
            results.append(derived)
            states.append(MinuteParameterStudyTrialState(index=trial.index, job_id=trial.job_id, state="sealed"))
            continue
        windows = (
            None
            if ref is None
            else read_minute_parameter_study_windows(
                reader,
                job_id=trial.job_id,
                owner_id=plan.request.owner_id,
                parameters=trial.command.config.parameters,
                as_of=now,
            )
        )
        if windows is None:
            missing.append(trial.index)
            states.append(
                MinuteParameterStudyTrialState(
                    index=trial.index, job_id=trial.job_id, state="not_prepared"
                )
                if ref is None
                else _unsealed_study_state(reader, ref, now)
            )
            continue
        assert ref is not None
        sealed = windows.sealed
        if (
            sealed.job_id,
            sealed.owner_id,
            sealed.accepted_spec,
            sealed.full_input_hash,
            sealed.core_input_hash,
            sealed.seed_hash,
            sealed.result.replay.profile_hash,
            sealed.result.replay.study_binding,
        ) != (
            trial.job_id,
            plan.request.owner_id,
            ref.marker.command.spec,
            ref.full_input_hash,
            ref.core_input_hash,
            ref.seed_hash,
            ref.profile_hash,
            ref.binding,
        ):
            raise PermissionError(
                "sealed study result differs from actual task/spec/source/profile/selection"
            )
        results.append(
            MinuteParameterStudyTrialResult(
                prepared=ref,
                spec_hash=sealed.spec_hash,
                manifest_hash=sealed.manifest_hash,
                complete_result_hash=sealed.complete_result_hash,
                result_hash=sealed.result_hash,
                completed_at=sealed.completed_at,
                read_at=windows.read_at,
                training=windows.training,
                validation=windows.validation,
                independent_test=windows.independent_test,
            )
        )
        states.append(
            MinuteParameterStudyTrialState(index=trial.index, job_id=trial.job_id, state="sealed")
        )
    reasons = list(plan.unavailable_reasons)
    if any(item.state in {"failed", "cancelled", "rejected"} for item in states):
        reasons.append("original_trial_failed_or_cancelled")
    ranks: list[MinuteStudyTrainingRank] = []
    if not missing and results:
        observations = [item.training_observation for item in results]
        if any(item is None for item in observations):
            reasons.append("training_metrics_unavailable")
        else:
            for fold in dict.fromkeys(item.prepared.trial.fold for item in results):
                group = tuple(item for item in results if item.prepared.trial.fold == fold)
                protocols = tuple(item.prepared.binding.protocol for item in group)
                facts = tuple(
                    fact for item in group if (fact := item.training_observation) is not None
                )
                ranks.extend(
                    minute_backtest_parameter_optimizer.rank_minute_study_training(
                        protocols, facts, selection_cutoff=now
                    )
                )
            if not ranks:
                reasons.append("insufficient_training_trades")
        if any(
            item.validation.status != "complete" or item.independent_test.status != "complete"
            for item in results
        ):
            reasons.append("validation_or_independent_test_metrics_unavailable")
    return MinuteParameterStudyExecutionResult(
        plan=plan,
        read_at=now,
        state="unavailable" if reasons else ("pending" if missing else "complete"),
        trial_states=tuple(states),
        results=tuple(results),
        missing_trial_indices=tuple(missing),
        training_ranks=tuple(ranks),
        unavailable_reasons=tuple(dict.fromkeys(reasons)),
    )


def build_minute_parameter_study_execution_heatmap(
    result: MinuteParameterStudyExecutionResult,
    *,
    current_trial_index: int,
    x_parameter: str,
    y_parameter: str,
) -> MinuteStudyHeatmap:
    """Called with a fresh owner read; supplied public DTOs cannot attest full results."""
    result = MinuteParameterStudyExecutionResult.model_validate(result)
    if result.missing_trial_indices:
        raise ValueError("heatmap needs every original trial result")
    current = next(
        (item for item in result.results if item.prepared.trial.index == current_trial_index), None
    )
    if current is None:
        raise ValueError("heatmap current point is not an original completed trial")
    selected = current.prepared.binding
    group = tuple(
        item
        for item in result.results
        if item.prepared.trial.fold == current.prepared.trial.fold
        and item.prepared.binding.settings == selected.settings
    )
    facts = tuple(fact for item in group if (fact := item.training_observation) is not None)
    if len(facts) != len(group):
        raise ValueError("heatmap original training metrics are unavailable")
    return build_minute_study_heatmap(
        tuple(item.prepared.binding.protocol for item in group),
        facts,
        selection_cutoff=result.read_at,
        x_parameter=x_parameter,
        y_parameter=y_parameter,
        current_study_id=selected.study_id,
    )
