"""Fixed-version folds submitted to the existing template preparer and Lab."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Literal, Self
from uuid import UUID, uuid5

import pandas as pd
from pydantic import Field, model_validator

from rquant.experiment_registry import ExperimentRegistry
from rquant.experiment_platform import experiment_family_job, stable_experiment_interaction
from rquant.experiment_platform_commands import ExperimentCommandWriter, RegisterExperimentFamily
from rquant.lab_job_center import CommandSubmissionReceipt, LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandEnvelope
from rquant.perf import performance_summary
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_promotion_commands import RunStrategyWalkForward
from rquant.strategy_promotion_contracts import (
    BoundValidationPromotionEvidence,
    BoundWalkForwardFold,
    SealedPromotionResult,
    StrategyPromotionTarget,
    NativeMinuteConfiguration,
    NativeMinuteSelection,
)
from rquant.strategy_template_artifact import StrategyTemplateSealedResultReader
from rquant.strategy_template_run_commands import RunStrategyTemplate, StrategyTemplateRunReceipt
from rquant.strategy_template_submission import StrategyTemplateRunBackend
from rquant.topn_walk_forward import build_expanding_folds
from rquant.minute_backtest_formal import MinuteExperimentProtocol

if TYPE_CHECKING:
    from rquant.experiment_platform import NativeMinuteExperimentRequest
    from rquant.minute_backtest_artifact import MinuteSealedReplayReader


class StrategyPromotionFoldPlan(RuntimeContractModel):
    index: int = Field(strict=True, ge=1, le=6)
    train_dates: tuple[date, ...] = Field(min_length=1, max_length=2520)
    test_dates: tuple[date, ...] = Field(min_length=1, max_length=2520)
    request: RunStrategyTemplate

    @model_validator(mode="after")
    def original_past_only(self) -> Self:
        if any(dates != tuple(sorted(set(dates))) for dates in (self.train_dates, self.test_dates)):
            raise ValueError("fold dates must retain the full original calendar")
        if self.train_dates[-1] >= self.test_dates[0]:
            raise ValueError("fold training includes future test dates")
        if (self.request.start_date, self.request.end_date) != (
            self.train_dates[0],
            self.test_dates[-1],
        ):
            raise ValueError("child run differs from its full training/test interval")
        return self


class StrategyPromotionWalkForwardPlan(RuntimeContractModel):
    contract: Literal["strategy-fixed-walk-forward/v1"] = "strategy-fixed-walk-forward/v1"
    request: RunStrategyWalkForward
    metadata_identity: StrategyAuthoringIdentity
    parent: BoundValidationPromotionEvidence
    calendar_source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    dates: tuple[date, ...] = Field(min_length=2, max_length=2520)
    folds: tuple[StrategyPromotionFoldPlan, ...] = Field(max_length=6)

    @model_validator(mode="after")
    def fixed_original_children(self) -> Self:
        target = self.request.target
        if target != self.parent.target or (
            self.request.selection.family_id,
            self.request.selection.experiment_id,
        ) != (self.parent.parent_family, self.parent.experiment_id):
            raise ValueError("WF target/selection differs from the original parent")
        if self.dates != tuple(sorted(set(self.dates))):
            raise ValueError("WF dates must be complete and ordered")
        if any(
            not self.parent.train_window.start_date <= day <= self.parent.window.end_date
            or self.parent.train_window.end_date < day < self.parent.window.start_date
            for day in self.dates
        ):
            raise ValueError("WF dates exceed the fixed train/validation interval")
        training = tuple(day for day in self.dates if day <= self.parent.train_window.end_date)
        validation = tuple(day for day in self.dates if day >= self.parent.window.start_date)
        if not training or not validation:
            raise ValueError("WF requires original training and validation dates")
        original = build_expanding_folds(
            list(self.dates), fold_count=self.request.fold_count, min_train_dates=len(training)
        )
        if len(self.folds) != len(original):
            raise ValueError("WF cannot invent missing original folds")
        for fold, expected in zip(self.folds, original, strict=True):
            child = fold.request
            if (fold.index, fold.train_dates, fold.test_dates) != (
                expected.fold,
                tuple(expected.train_dates),
                tuple(expected.test_dates),
            ):
                raise ValueError("WF fold differs from the original expanding builder")
            if (
                child.command_id,
                child.requested_at,
                child.generation_id,
                child.strategy_id,
                child.head,
                child.expected_head,
            ) != (
                str(uuid5(UUID(self.request.command_id), f"strategy-fixed-wf:{fold.index}")),
                self.request.requested_at,
                self.request.generation_id,
                target.strategy_id,
                target.head,
                target.head,
            ):
                raise ValueError("WF child differs from its fixed version and original UUID")
        if len({fold.request.initial_cash for fold in self.folds}) > 1:
            raise ValueError("WF initial capital changes between folds")
        if len(self.model_dump_json().encode()) > 128 * 1024:
            raise ValueError("WF reference plan exceeds its bounded domain budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self)

    @property
    def complete_for_promotion(self) -> bool:
        return len(self.folds) == 6


def build_walk_forward_plan(
    request: RunStrategyWalkForward,
    *,
    metadata_identity: StrategyAuthoringIdentity,
    parent: BoundValidationPromotionEvidence,
    dates: tuple[date, ...],
    calendar_source_identity: str,
    initial_cash: Decimal,
) -> StrategyPromotionWalkForwardPlan:
    if request.target != parent.target or request.target.source_kind != "template":
        raise ValueError("WF target differs from its original template parent")
    training = tuple(day for day in dates if day <= parent.train_window.end_date)
    original = build_expanding_folds(
        list(dates), fold_count=request.fold_count, min_train_dates=len(training)
    )
    folds = tuple(
        StrategyPromotionFoldPlan(
            index=fold.fold,
            train_dates=tuple(fold.train_dates),
            test_dates=tuple(fold.test_dates),
            request=RunStrategyTemplate(
                command_id=str(uuid5(UUID(request.command_id), f"strategy-fixed-wf:{fold.fold}")),
                requested_at=request.requested_at,
                generation_id=request.generation_id,
                strategy_id=request.target.strategy_id,
                head=request.target.head,
                expected_head=request.target.head,
                start_date=fold.train_dates[0],
                end_date=fold.test_dates[-1],
                initial_cash=initial_cash,
            ),
        )
        for fold in original
    )
    return StrategyPromotionWalkForwardPlan(
        request=request,
        metadata_identity=metadata_identity,
        parent=parent,
        dates=dates,
        calendar_source_identity=calendar_source_identity,
        folds=folds,
    )


class StrategyPromotionWalkForwardSubmission(RuntimeContractModel):
    command_id: UUID
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipts: tuple[StrategyTemplateRunReceipt, ...] = Field(max_length=6)


class NativeMinuteFoldPlan(RuntimeContractModel):
    index: int = Field(strict=True, ge=1, le=6)
    train_dates: tuple[date, ...] = Field(min_length=1, max_length=2520)
    test_dates: tuple[date, ...] = Field(min_length=1, max_length=2520)
    job_id: UUID
    configuration: NativeMinuteConfiguration


class NativeStrategyPromotionWalkForwardPlan(RuntimeContractModel):
    contract: Literal["strategy-native-minute-walk-forward/v1"] = "strategy-native-minute-walk-forward/v1"
    request: RunStrategyWalkForward
    metadata_identity: StrategyAuthoringIdentity
    parent: BoundValidationPromotionEvidence
    selection: NativeMinuteSelection
    protocol: MinuteExperimentProtocol
    calendar_source_identity: str = Field(pattern=r"^[0-9a-f]{64}$")
    dates: tuple[date, ...] = Field(min_length=2, max_length=2520)
    folds: tuple[NativeMinuteFoldPlan, ...] = Field(max_length=6)

    @model_validator(mode="after")
    def fixed_native_children(self) -> Self:
        if self.request.target != self.parent.target or self.selection.target != self.parent.target:
            raise ValueError("native WF definition/profile differs from its fixed parent")
        if (self.request.selection.family_id, self.request.selection.experiment_id) != (
            self.parent.parent_family, self.parent.experiment_id
        ) or (self.protocol.train_range, self.protocol.validation_range) != (
            self.parent.train_window, self.parent.window
        ):
            raise ValueError("native WF selection/protocol differs from its complete parent")
        if self.dates != tuple(sorted(set(self.dates))) or any(
            not self.parent.train_window.start_date <= d <= self.parent.window.end_date
            or self.parent.train_window.end_date < d < self.parent.window.start_date for d in self.dates
        ):
            raise ValueError("native WF calendar includes outer or unordered dates")
        training = tuple(d for d in self.dates if d <= self.parent.train_window.end_date)
        validation = tuple(d for d in self.dates if d >= self.parent.window.start_date)
        if not training or not validation or (training[0], training[-1], validation[0], validation[-1]) != (
            self.parent.train_window.start_date, self.parent.train_window.end_date,
            self.parent.window.start_date, self.parent.window.end_date
        ):
            raise ValueError("native WF calendar lacks complete train/validation boundaries")
        expected = build_expanding_folds(list(self.dates), fold_count=self.request.fold_count, min_train_dates=len(training))
        if len(self.folds) != len(expected):
            raise ValueError("native WF cannot invent missing original folds")
        for child, fold in zip(self.folds, expected, strict=True):
            if (child.index, child.train_dates, child.test_dates, child.job_id, child.configuration) != (
                fold.fold, tuple(fold.train_dates), tuple(fold.test_dates),
                uuid5(UUID(self.request.command_id), f"strategy-fixed-wf:{fold.fold}"),
                NativeMinuteConfiguration(selection=self.selection, start_date=fold.train_dates[0], end_date=fold.test_dates[-1]),
            ):
                raise ValueError("native WF child differs from original dates/UUID/fixed parameters/profile")
        if len(self.model_dump_json().encode()) > 128 * 1024:
            raise ValueError("native WF reference plan exceeds its bounded domain budget")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self)

    @property
    def complete_for_promotion(self) -> bool:
        return len(self.folds) == 6

    def family_request(self) -> NativeMinuteExperimentRequest:
        from rquant.experiment_platform import NativeMinuteExperimentRequest

        return NativeMinuteExperimentRequest(name=self.request.target.name,
            configurations=tuple(f.configuration for f in self.folds), protocol=self.protocol,
            walk_forward_plan_hash=self.fingerprint, walk_forward_command_id=UUID(self.request.command_id))


def build_native_walk_forward_plan(
    request: RunStrategyWalkForward, *, metadata_identity: StrategyAuthoringIdentity,
    parent: BoundValidationPromotionEvidence, selection: NativeMinuteSelection,
    dates: tuple[date, ...], calendar_source_identity: str, protocol: MinuteExperimentProtocol,
) -> NativeStrategyPromotionWalkForwardPlan:
    original = build_expanding_folds(list(dates), fold_count=request.fold_count,
        min_train_dates=len(tuple(d for d in dates if d <= parent.train_window.end_date)))
    return NativeStrategyPromotionWalkForwardPlan(request=request, metadata_identity=metadata_identity,
        parent=parent, selection=selection, protocol=protocol, dates=dates,
        calendar_source_identity=calendar_source_identity, folds=tuple(
            NativeMinuteFoldPlan(index=f.fold, train_dates=tuple(f.train_dates), test_dates=tuple(f.test_dates),
                job_id=uuid5(UUID(request.command_id), f"strategy-fixed-wf:{f.fold}"),
                configuration=NativeMinuteConfiguration(selection=selection,
                    start_date=f.train_dates[0], end_date=f.test_dates[-1])) for f in original))


PromotionWalkForwardPlan = StrategyPromotionWalkForwardPlan | NativeStrategyPromotionWalkForwardPlan


class NativeStrategyPromotionWalkForwardSubmission(RuntimeContractModel):
    command_id: UUID
    plan_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipts: tuple[CommandSubmissionReceipt, ...] = Field(min_length=1, max_length=6)


PromotionWalkForwardSubmission = StrategyPromotionWalkForwardSubmission | NativeStrategyPromotionWalkForwardSubmission


@dataclass(frozen=True)
class NativeStrategyPromotionWalkForwardBinding:
    store: StrategyAuthoringStore
    expected_identity: StrategyAuthoringIdentity
    writer: ExperimentCommandWriter
    results: MinuteSealedReplayReader | None = None

    def __post_init__(self) -> None:
        if (
            type(self.store) is not StrategyAuthoringStore
            or type(self.writer) is not ExperimentCommandWriter
            or self.store.identity() != self.expected_identity
            or self.writer.commands.experiment_registry is not self.writer.store.registry
        ):
            raise TypeError("native WF requires its original installed metadata, Registry and Lab spool")
        if self.results is not None:
            from rquant.minute_backtest_artifact import MinuteSealedReplayReader

            if type(self.results) is not MinuteSealedReplayReader or self.results.submission_facade is not self.writer.commands:
                raise TypeError("native WF complete reader differs from its original admitted Lab authority")


class StrategyPromotionWalkForwardBackend:
    def __init__(
        self,
        *,
        registry: ExperimentRegistry,
        runs: StrategyTemplateRunBackend | None = None,
        results: StrategyTemplateSealedResultReader | None = None,
        native: NativeStrategyPromotionWalkForwardBinding | None = None,
    ) -> None:
        if (
            type(registry) is not ExperimentRegistry
            or (runs is None) != (results is None)
            or runs is not None and (
                type(runs) is not StrategyTemplateRunBackend
                or type(results) is not StrategyTemplateSealedResultReader
                or runs.preparer.experiments is not registry
                or results.reader is not runs.facade.reader
            )
            or native is not None and (
                type(native) is not NativeStrategyPromotionWalkForwardBinding
                or native.writer.store.registry is not registry
            )
            or runs is None and native is None
        ):
            raise TypeError(
                "WF requires the same concrete original preparer, Registry and Lab reader"
            )
        self.registry, self.runs, self.results = registry, runs, results
        self.native = native

    def _native_command(
        self, plan: NativeStrategyPromotionWalkForwardPlan, *, actor_id: str
    ) -> RegisterExperimentFamily:
        binding = self.native
        if binding is None:
            raise ValueError("native original WF admission is not installed")
        if (
            plan.request.target.owner_id != actor_id
            or binding.store.identity() != plan.metadata_identity
            or plan.metadata_identity != binding.expected_identity
        ):
            raise PermissionError("native WF original owner or metadata identity differs")
        return RegisterExperimentFamily(command_id=plan.request.command_id,
            requested_at=plan.request.requested_at, actor_id=actor_id, request=plan.family_request())

    def _native_completed(
        self, plan: NativeStrategyPromotionWalkForwardPlan, *, actor_id: str
    ) -> NativeStrategyPromotionWalkForwardSubmission | None:
        command = self._native_command(plan, actor_id=actor_id)
        writer = self.native.writer
        record = writer.store.get_request(actor_id, UUID(command.command_id))
        if record is None:
            return None
        if (record.request, record.body_hash) != (command.request, canonical_sha256(command)):
            raise ValueError("native WF original family body differs")
        if record.state != "ready":
            return None
        receipts = []
        for index, fold in enumerate(plan.folds):
            job_id = experiment_family_job(record, index)
            prepared = writer.store.preparation(actor_id, record.family_id, index)
            intent = self.registry.get_submission_intent_for_job(job_id)
            child = writer.store.child(job_id)
            if prepared is None or intent is None or child is None:
                raise ValueError("native ready WF lost its original complete preparation/intent")
            if (
                job_id != fold.job_id
                or prepared.configuration != fold.configuration
                or (child.owner, child.family_id, child.experiment_id) != (
                    actor_id, record.family_id, prepared.prepared.formal_plan.spec.experiment_id)
            ):
                raise PermissionError("native WF child differs from its owner/full original plan")
            envelope = LabCommandEnvelope(request_id=LabCommandSubmissionFacade._request_id(
                stable_experiment_interaction(actor_id, record.request_id, index)),
                command=prepared.prepared.submission(job_id=job_id).command)
            if intent != LabCommandSubmissionFacade._experiment_submission_intent(envelope):
                raise ValueError("native WF original intent/body/spec differs")
            receipt = writer.commands._existing(envelope)
            if receipt is None:
                return None
            if not isinstance(receipt, CommandSubmissionReceipt) or receipt.job_id != job_id:
                raise ValueError("native WF original spool receipt conflicts")
            receipts.append(receipt)
        return NativeStrategyPromotionWalkForwardSubmission(command_id=UUID(plan.request.command_id),
            plan_hash=plan.fingerprint, receipts=tuple(receipts))

    def lookup(
        self, request: RunStrategyWalkForward, *, actor_id: str
    ) -> PromotionWalkForwardPlan | None:
        return self.registry.walk_forward_plan(request, actor_id=actor_id)

    def completed_submission(
        self, plan: PromotionWalkForwardPlan, *, actor_id: str
    ) -> PromotionWalkForwardSubmission | None:
        if isinstance(plan, NativeStrategyPromotionWalkForwardPlan):
            return self._native_completed(plan, actor_id=actor_id)
        if self.runs is None:
            raise ValueError("original template WF admission is not installed")
        if (
            plan.request.target.owner_id != actor_id
            or plan.metadata_identity != self.runs.expected_identity
        ):
            raise PermissionError("WF original owner or metadata identity differs")
        receipts = []
        for fold in plan.folds:
            receipt = self.runs.store.lookup_command(
                fold.request, owner_id=actor_id, expected_identity=plan.metadata_identity
            )
            if receipt is None:
                return None
            if type(receipt) is not StrategyTemplateRunReceipt or receipt.job_id != UUID(
                fold.request.command_id
            ):
                raise ValueError("WF original child receipt differs")
            receipts.append(receipt)
        return StrategyPromotionWalkForwardSubmission(
            command_id=UUID(plan.request.command_id),
            plan_hash=plan.fingerprint,
            receipts=tuple(receipts),
        )

    def submit(
        self, plan: PromotionWalkForwardPlan, *, actor_id: str
    ) -> PromotionWalkForwardSubmission:
        if isinstance(plan, NativeStrategyPromotionWalkForwardPlan):
            command = self._native_command(plan, actor_id=actor_id)
            plan = self.registry.record_walk_forward_plan(plan, actor_id=actor_id)
            writer = self.native.writer
            writer.submit(command, writer.freeze(command))
            completed = self._native_completed(plan, actor_id=actor_id)
            if completed is None:
                raise ValueError("native original WF submission requires recovery")
            return completed
        if self.runs is None:
            raise ValueError("original template WF admission is not installed")
        # Freeze only references before any original input publication/prepare.
        plan = self.registry.record_walk_forward_plan(plan, actor_id=actor_id)
        if plan.metadata_identity != self.runs.expected_identity:
            raise PermissionError("WF original metadata authority changed")
        receipts = []
        for fold in plan.folds:
            command = self.runs.compile(
                fold.request, owner_id=actor_id, expected_identity=plan.metadata_identity
            )
            if (
                canonical_sha256(command.accepted.spec.execution_costs)
                != plan.request.target.cost_fingerprint
            ):
                raise ValueError("WF original child cost differs from the fixed parent")
            receipt = self.runs.submit(command)
            if receipt.job_id != UUID(fold.request.command_id):
                raise ValueError("WF child original job differs")
            receipts.append(receipt)
        return StrategyPromotionWalkForwardSubmission(
            command_id=UUID(plan.request.command_id),
            plan_hash=plan.fingerprint,
            receipts=tuple(receipts),
        )

    def read_folds(
        self, request_id: UUID, *, target: StrategyPromotionTarget, as_of: datetime
    ) -> tuple[BoundWalkForwardFold, ...]:
        plan = self.registry.walk_forward_plan_by_id(request_id, actor_id=target.owner_id)
        if plan is None:
            return ()
        if isinstance(plan, NativeStrategyPromotionWalkForwardPlan):
            return self._read_native_folds(plan, target=target, as_of=as_of)
        if plan.request.target != target or plan.metadata_identity != self.runs.expected_identity:
            raise PermissionError(
                "WF reference belongs to another owner/version or original metadata"
            )
        recent = self.results.recent_runs(
            self.runs.store, expected_identity=plan.metadata_identity, as_of=as_of
        )
        indexed = {value.job_id: value for value in recent}
        folds = []
        for fold in plan.folds:
            job_id = UUID(fold.request.command_id)
            reference = indexed.get(job_id)
            if reference is None:
                break
            read = self.results.read_run(
                job_id,
                store=self.runs.store,
                expected_identity=plan.metadata_identity,
                private_owner=target.owner_id,
                expected_result_hash=reference.complete_result_hash,
            )
            result = read.result
            job = self.results.reader.get_job(job_id)
            if job is None or canonical_sha256(job.spec.execution_costs) != target.cost_fingerprint:
                raise ValueError("WF complete original result differs from fixed execution costs")
            if result.calendar_source_identity != plan.calendar_source_identity:
                raise ValueError("WF original full result differs from its fixed calendar")
            dates = fold.train_dates + fold.test_dates
            if tuple(day.trade_date for day in result.days) != dates or any(
                day.daily_return is None or day.account is None for day in result.days
            ):
                raise ValueError("WF original full result lacks exact training/test dates")
            selected = tuple(day for day in result.days if day.trade_date in fold.test_dates)
            summary = performance_summary(
                pd.Series(
                    [float(day.daily_return) for day in selected],
                    index=pd.to_datetime(fold.test_dates),
                    dtype=float,
                )
            )
            if summary.total_return is None:
                raise ValueError("WF full test return is unavailable")
            sealed = SealedPromotionResult(
                job_id=job_id,
                spec_hash=read.spec_hash,
                manifest_hash=read.manifest_hash,
                result_hash=read.result_hash,
                input_hash=result.input_hash,
                content_hash=result.content_hash,
                available_at=reference.completed_at,
            )
            if sealed.available_at > as_of:
                raise ValueError("WF original seal is not yet visible")
            folds.append(
                BoundWalkForwardFold(
                    target=target,
                    index=fold.index,
                    train_dates=fold.train_dates,
                    test_dates=fold.test_dates,
                    reference=sealed,
                    net_return=Decimal(str(summary.total_return)),
                )
            )
        return tuple(folds)

    def _read_native_folds(
        self, plan: NativeStrategyPromotionWalkForwardPlan, *,
        target: StrategyPromotionTarget, as_of: datetime,
    ) -> tuple[BoundWalkForwardFold, ...]:
        from rquant.experiment_platform_evidence import native_curve_and_performance, read_native_preparation_result

        binding = self.native
        if binding is None or binding.results is None:
            raise ValueError("native complete WF reader is not installed")
        self._native_command(plan, actor_id=target.owner_id)
        if plan.request.target != target:
            raise PermissionError("native WF belongs to another exact owner/version/profile")
        record = binding.writer.store.get_request(target.owner_id, UUID(plan.request.command_id))
        if record is None:
            return ()
        if record.request != plan.family_request():
            raise PermissionError("native WF original full family differs")
        folds = []
        for index, fold in enumerate(plan.folds):
            receipt = binding.writer.store.preparation(target.owner_id, record.family_id, index)
            if receipt is None:
                break
            if receipt.configuration != fold.configuration or experiment_family_job(record, index) != fold.job_id:
                raise PermissionError("native WF original child/configuration differs")
            sealed = read_native_preparation_result(binding.results, receipt.prepared,
                job_id=fold.job_id, configuration=fold.configuration, as_of=as_of)
            if sealed is None:
                break
            if sealed.result.publication != receipt.prepared.published.receipt or sealed.formal_plan != receipt.prepared.formal_plan:
                raise PermissionError("native WF complete seal differs from its original preparation")
            runtime = receipt.prepared.frozen.runtime
            if runtime.market_calendar.content_sha256 != plan.calendar_source_identity:
                raise ValueError("native WF original complete calendar changed")
            points, _, _ = native_curve_and_performance(sealed.result.replay, fold.train_dates + fold.test_dates)
            selected = tuple(point for point in points if point.trade_date in fold.test_dates)
            summary = performance_summary(pd.Series([point.daily_return for point in selected],
                index=pd.to_datetime(fold.test_dates), dtype=float))
            if summary.total_return is None:
                raise ValueError("native WF complete test return is unavailable")
            folds.append(BoundWalkForwardFold(target=target, index=fold.index,
                train_dates=fold.train_dates, test_dates=fold.test_dates,
                reference=SealedPromotionResult(job_id=fold.job_id, spec_hash=sealed.spec_hash,
                    manifest_hash=sealed.manifest_hash, result_hash=sealed.complete_result_hash,
                    input_hash=sealed.full_input_hash, content_hash=sealed.result_hash,
                    available_at=sealed.completed_at), net_return=Decimal(str(summary.total_return))))
        return tuple(folds)
