"""Version-bound manual facts; automatic registry policy remains a separate history."""

from __future__ import annotations

from datetime import date, time, timedelta
from decimal import Decimal
from typing import Annotated, Literal, Self
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from rquant.experiment_registry import (
    DateRange,
    ExperimentOutcome,
    ExperimentSpec,
    ExperimentStatus,
    ForwardArtifactEvidence,
    HypothesisFamilyManifest,
    PromotionStage,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256
from rquant.research_run_spec import ExecutionCostSpec
from rquant.strategy_authoring_commands import ExperimentTemplateSelection, StrategyAuthoringIdentity, StrategyTemplateHead

from rquant.minute_backtest_contracts import MinuteReplayExecutionProfile, MinuteRuntimeDailyValuation
from rquant.live_contracts import BatchEnvelope, LiveChannel

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
Owner = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[a-zA-Z0-9_.:@-]+$")]
Finite = Annotated[Decimal, Field(allow_inf_nan=False)]
MAX_PROMOTION_REVIEWS = 4096
MAX_REVIEW_BYTES = 32 * 1024
STAGES = tuple(PromotionStage)


def next_stage(stage: PromotionStage) -> PromotionStage:
    index = STAGES.index(PromotionStage(stage))
    if index == len(STAGES) - 1:
        raise ValueError("strategy is already monitor approved")
    return STAGES[index + 1]


class ManualPromotionPolicy(RuntimeContractModel):
    contract: Literal["strategy-manual-promotion/prototype-v1"] = (
        "strategy-manual-promotion/prototype-v1"
    )
    validation_trades: Literal[30] = 30
    validation_sharpe: Finite = Decimal("0.8")
    adjusted_p_limit: Finite = Decimal("0.05")
    p_is_strict: Literal[True] = True
    outer_net_return_is_strict_positive: Literal[True] = True
    fold_count: Literal[6] = 6
    positive_folds: Literal[4] = 4
    forward_open_days: Literal[20] = 20
    band_algorithm: Literal["paper-bootstrap-splitmix64-day-major-nearest-rank-v1"] = (
        "paper-bootstrap-splitmix64-day-major-nearest-rank-v1"
    )
    band_paths: Literal[2048] = 2048
    band_seed: Literal[20261005] = 20261005

    @model_validator(mode="after")
    def fixed_decimal_boundaries(self) -> Self:
        if (self.validation_sharpe, self.adjusted_p_limit) != (Decimal("0.8"), Decimal("0.05")):
            raise ValueError("manual policy thresholds are immutable")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self)


class StrategyPromotionTarget(RuntimeContractModel):
    source_kind: Literal["template", "builtin"]
    owner_id: Owner
    strategy_id: str = Field(min_length=1, max_length=128)
    name: str = Field(min_length=1, max_length=80)
    head: StrategyTemplateHead
    parameter_fingerprint: Sha256
    cost_fingerprint: Sha256

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self)

    @property
    def version_key(self) -> str:
        return canonical_sha256(self.model_dump(exclude={"name"}))


class NativeMinuteSelection(RuntimeContractModel):
    """Native definition and fixed execution profile; the Lab wrapper is separate."""

    target: StrategyPromotionTarget
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(strict=True, ge=1)
    profile_hash: Sha256

    @model_validator(mode="after")
    def native_definition(self) -> Self:
        if self.target.source_kind != "builtin" or (
            self.target.strategy_id not in {"n_shape", "auction_gap", "growth_board_surge"}
            or self.target.head.version != 1
        ):
            raise ValueError("native minute selection requires its exact native@1 definition")
        return self


class NativeMinuteConfiguration(RuntimeContractModel):
    kind: Literal["native_minute"] = "native_minute"
    selection: NativeMinuteSelection
    start_date: date
    end_date: date

    @model_validator(mode="after")
    def ordered_interval(self) -> Self:
        if self.start_date > self.end_date:
            raise ValueError("native minute interval is reversed")
        return self

    @property
    def config_hash(self) -> str:
        return canonical_sha256(self)

    @property
    def source_key(self) -> str:
        return self.selection.source_key

    @property
    def source_version(self) -> int:
        return self.selection.source_version


class NativeMinuteForwardBinding(RuntimeContractModel):
    role_id: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    account_id: str = Field(min_length=1, max_length=128)
    owner_id: Owner
    strategy_id: Literal["n_shape", "auction_gap", "growth_board_surge"]
    strategy_version: Literal["1"] = "1"
    parameter_fingerprint: Sha256
    cost_spec_id: Sha256
    ledger_id: str = Field(min_length=1, max_length=128)
    manifest_fingerprint: Sha256


class NativeMinuteForwardConfiguration(RuntimeContractModel):
    """Native paper execution is bound to the original manual strategy owner."""

    contract: Literal["native-minute-forward/v1"] = "native-minute-forward/v1"
    target: StrategyPromotionTarget
    binding: NativeMinuteForwardBinding
    metadata_identity: StrategyAuthoringIdentity
    source_key: str = Field(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")
    source_version: int = Field(strict=True, ge=1)
    execution_profile: MinuteReplayExecutionProfile
    version: int = Field(strict=True, ge=1, le=4096)
    configured_at: AwareUtcDatetime
    paper_approval_hash: Sha256
    paper_approved_at: AwareUtcDatetime

    @model_validator(mode="after")
    def exact_native_owner_and_profile(self) -> Self:
        target, binding, profile = self.target, self.binding, self.execution_profile
        if (
            target.source_kind != "builtin" or target.head.version != 1
            or (binding.owner_id, binding.strategy_id, binding.strategy_version,
                binding.parameter_fingerprint, binding.cost_spec_id)
            != (target.owner_id, target.strategy_id, str(target.head.version),
                target.parameter_fingerprint, profile.execution_costs.cost_spec_id)
            or binding.account_id != profile.paper_policy.account_id
            or target.cost_fingerprint != canonical_sha256(profile.execution_costs)
            or not profile.execution_costs.is_alignment_eligible
            or self.configured_at < self.paper_approved_at
        ):
            raise ValueError("native forward differs from its exact native owner/profile/cost/manual paper start")
        if len(self.model_dump_json().encode()) > 32 * 1024:
            raise ValueError("native forward configuration exceeds the original command budget")
        return self

    @property
    def execution_cost_spec(self) -> ExecutionCostSpec:
        return self.execution_profile.execution_costs

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class NativeMinuteForwardValuation(MinuteRuntimeDailyValuation):
    """Actual forward observation is separate from the fixed close cutoff."""

    contract: Literal["native-forward-valuation/v1"] = "native-forward-valuation/v1"
    observed_at: AwareUtcDatetime
    market_envelope: BatchEnvelope | None = None

    @model_validator(mode="after")
    def explicit_original_valuation(self) -> Self:
        local = self.as_of.astimezone(ZoneInfo("Asia/Shanghai"))
        if (local.date() != self.trade_date or local.time().replace(tzinfo=None) != time(15)
            or self.observed_at < self.as_of or self.observed_at > self.as_of + timedelta(hours=6)
            or self.observed_at.astimezone(ZoneInfo("Asia/Shanghai")).date() != self.trade_date):
            raise ValueError("native close requires its actual same-day observation within six hours")
        for pointer in (self.market_pointer, self.constraint_pointer):
            if pointer is not None and pointer.published_at > self.as_of:
                raise ValueError("native close publication is later than its original cutoff")
        pointer, envelope = self.market_pointer, self.market_envelope
        if (pointer is None) != (envelope is None):
            raise ValueError("native close publication requires its full original manifest")
        if pointer is not None and (
            envelope.channel is not LiveChannel.MARKET_MINUTE or
            (pointer.channel, pointer.batch_id, pointer.sequence, pointer.revision,
                pointer.content_sha256, pointer.quality_status, pointer.published_at) !=
            (envelope.channel, envelope.batch_id, envelope.sequence, envelope.revision,
                envelope.content_sha256, envelope.quality_status, envelope.available_at)
            or envelope.event_time_end > self.as_of or envelope.source_time > self.as_of
            or envelope.received_at > self.as_of):
            raise ValueError("native close publication differs from its exact original manifest or cutoff")
        codes = tuple(proof.quote.ts_code for proof in self.price_proofs)
        if codes != tuple(sorted(set(codes))) or any(
            proof.quote.available_at > self.as_of or proof.quote.event_time > self.as_of
            for proof in self.price_proofs):
            raise ValueError("native close requires ordered complete PIT prices at its original cutoff")
        if self.status == "unavailable":
            if self.account is not None or not self.unavailable_reasons:
                raise ValueError("unavailable native close cannot supply a substituted account")
        elif (self.account is None or pointer is None or self.unavailable_reasons
            or self.account.as_of_time != self.as_of):
            raise ValueError("complete native close requires its full original publication and account")
        elif tuple(holding.code for holding in self.account.holdings) != codes or any(
            holding.market_price != proof.quote.context.executable_price
            for holding, proof in zip(self.account.holdings, self.price_proofs, strict=True)):
            raise ValueError("native close holdings differ from their full original PIT proofs")
        return self


class SealedPromotionResult(RuntimeContractModel):
    job_id: UUID
    spec_hash: Sha256
    manifest_hash: Sha256
    result_hash: Sha256
    input_hash: Sha256
    content_hash: Sha256
    available_at: AwareUtcDatetime


class BoundValidationPromotionEvidence(RuntimeContractModel):
    target: StrategyPromotionTarget
    parent_family: str = Field(min_length=1, max_length=128)
    parent_manifest_hash: Sha256
    parent_count: int = Field(strict=True, ge=1, le=64)
    experiment_id: Sha256
    reference: SealedPromotionResult
    train_window: DateRange
    window: DateRange
    full_dates_hash: Sha256
    returns_hash: Sha256
    trades_hash: Sha256
    closed_trades: int = Field(strict=True, ge=0)
    net_return: Finite
    max_drawdown: Annotated[Decimal, Field(ge=0, le=1, allow_inf_nan=False)]
    win_rate: Annotated[Decimal, Field(ge=0, le=1, allow_inf_nan=False)]
    sharpe: Finite | None
    full_costs: bool = Field(strict=True)

    @model_validator(mode="after")
    def ordered_ranges(self) -> Self:
        if self.train_window.end_date >= self.window.start_date:
            raise ValueError("validation must follow fixed training")
        return self


class BoundOuterPromotionEvidence(RuntimeContractModel):
    target: StrategyPromotionTarget
    parent_experiment_id: Sha256
    outer_experiment_id: Sha256
    grant_hash: Sha256
    reference: SealedPromotionResult
    window: DateRange
    net_return: Finite

    @property
    def passed(self) -> bool:
        return self.net_return > 0


class SealedFamilyAttemptOutcome(RuntimeContractModel):
    spec: ExperimentSpec
    original_status: Literal[
        ExperimentStatus.EXECUTED,
        ExperimentStatus.SUCCEEDED,
        ExperimentStatus.FAILED,
        ExperimentStatus.CANCELLED,
    ]
    execution_completed_at: AwareUtcDatetime
    reference: SealedPromotionResult | None = None
    outcome: ExperimentOutcome | None = None

    @model_validator(mode="after")
    def actual_statistical_domain(self) -> Self:
        successful = self.original_status in (ExperimentStatus.EXECUTED, ExperimentStatus.SUCCEEDED)
        if successful != (self.reference is not None and self.outcome is not None):
            raise ValueError(
                "complete terminal candidate requires its full seal and actual Outcome"
            )
        if not successful and (self.reference is not None or self.outcome is not None):
            raise ValueError("failed or cancelled candidate cannot have an invented Outcome")
        if self.outcome is not None:
            if (
                self.outcome.experiment_id != self.spec.experiment_id
                or self.outcome.adjusted_p_value is not None
                or self.outcome.outer_test_completed
                or self.outcome.outer_evidence is not None
                or self.outcome.artifact_hash != self.reference.result_hash
            ):
                raise ValueError(
                    "parent Outcome must bind its unchanged validation seal without outer"
                )
            if self.reference.available_at < self.execution_completed_at:
                raise ValueError("seal availability precedes original completion")
        return self


class SealedFamilyOutcomeReceipt(RuntimeContractModel):
    contract: Literal["strategy-sealed-family-validation-outcomes/v1"] = (
        "strategy-sealed-family-validation-outcomes/v1"
    )
    manifest: HypothesisFamilyManifest
    attempts: tuple[SealedFamilyAttemptOutcome, ...] = Field(min_length=1, max_length=64)
    overfit_evidence_hash: Sha256
    method: Literal["original-psr-validation+paper-bootstrap90/v1"] = (
        "original-psr-validation+paper-bootstrap90/v1"
    )
    recorded_at: AwareUtcDatetime

    @model_validator(mode="after")
    def complete_original_parent(self) -> Self:
        ids = tuple(a.spec.experiment_id for a in self.attempts)
        if len(set(ids)) != len(ids) or set(ids) != set(self.manifest.experiment_ids):
            raise ValueError("sealed receipt must cover every original parent attempt")
        ranks = []
        for item in self.attempts:
            if (
                item.spec.hypothesis_family != self.manifest.hypothesis_family
                or item.spec.metric_definition_fingerprint
                != self.manifest.metric_definition_fingerprint
                or item.execution_completed_at > self.recorded_at
            ):
                raise ValueError("parent family identity or statistical time differs")
            if item.outcome is not None:
                if (
                    item.outcome.attempted_configuration_count != self.manifest.hypothesis_count
                    or item.reference.available_at > self.recorded_at
                ):
                    raise ValueError("Outcome search count or availability differs")
                ranks.append(item.outcome.selected_rank)
        if not ranks or len(ranks) != len(set(ranks)):
            raise ValueError("family needs real uniquely ranked Outcomes")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self)


class BoundWalkForwardFold(RuntimeContractModel):
    target: StrategyPromotionTarget
    index: int = Field(strict=True, ge=1, le=6)
    train_dates: tuple[date, ...] = Field(min_length=1, max_length=2520)
    test_dates: tuple[date, ...] = Field(min_length=1, max_length=2520)
    reference: SealedPromotionResult
    net_return: Finite

    @model_validator(mode="after")
    def past_train_only(self) -> Self:
        for dates in (self.train_dates, self.test_dates):
            if dates != tuple(sorted(set(dates))):
                raise ValueError("fold dates must be complete ordered original dates")
        if self.train_dates[-1] >= self.test_dates[0]:
            raise ValueError("fold training cannot include future test data")
        return self


class BoundForwardPromotionEvidence(RuntimeContractModel):
    target: StrategyPromotionTarget
    paper_approval_hash: Sha256
    configuration_fingerprint: Sha256
    ledger_generation: Sha256
    ledger_head: Sha256
    ledger_revision: int = Field(strict=True, ge=1)
    nav_source_hash: Sha256
    calendar_source_hash: Sha256
    band_source_hash: Sha256
    band_job: UUID
    full_open_days: int = Field(strict=True, ge=1, le=2520)
    first_date: date
    last_date: date
    reconciliation_hash: Sha256
    all_inside_original_band: bool = Field(strict=True)
    original: ForwardArtifactEvidence

    @model_validator(mode="after")
    def full_date_domain(self) -> Self:
        if (
            self.first_date > self.last_date
            or self.original.observation_range
            != DateRange(start_date=self.first_date, end_date=self.last_date)
            or self.original.trading_days != self.full_open_days
        ):
            raise ValueError("forward dates differ from full original evidence")
        return self


class PromotionEvidenceBundle(RuntimeContractModel):
    target: StrategyPromotionTarget
    validation: BoundValidationPromotionEvidence | None = None
    family_receipt: SealedFamilyOutcomeReceipt | None = None
    adjusted_p: Annotated[Decimal, Field(ge=0, le=1, allow_inf_nan=False)] | None = None
    adjustment_hash: Sha256 | None = None
    outer: BoundOuterPromotionEvidence | None = None
    folds: tuple[BoundWalkForwardFold, ...] = Field(default=(), max_length=6)
    forward: BoundForwardPromotionEvidence | None = None
    observed_at: AwareUtcDatetime
    missing: tuple[str, ...] = Field(default=(), max_length=32)

    @model_validator(mode="after")
    def exact_target_and_visibility(self) -> Self:
        for item in (self.validation, self.outer, self.forward, *self.folds):
            if item is not None and item.target != self.target:
                raise ValueError("promotion bundle mixes owner/version/parameters/cost")
        for item in (self.validation, self.outer, *self.folds):
            if item is not None and item.reference.available_at > self.observed_at:
                raise ValueError("promotion evidence not yet visible")
        if self.family_receipt is not None and self.family_receipt.recorded_at > self.observed_at:
            raise ValueError("statistical evidence not yet visible")
        if self.forward is not None and self.forward.original.available_at > self.observed_at:
            raise ValueError("forward evidence not yet visible")
        if (self.adjusted_p is None) != (self.adjustment_hash is None):
            raise ValueError("adjusted p requires original immutable adjustment")
        if (
            self.outer
            and self.validation
            and self.outer.parent_experiment_id != self.validation.experiment_id
        ):
            raise ValueError("outer belongs to another selected parent")
        if tuple(f.index for f in self.folds) != tuple(range(1, len(self.folds) + 1)):
            raise ValueError("folds are missing or duplicated")
        return self

    @property
    def fingerprint(self) -> str:
        return canonical_sha256(self.model_dump(mode="python", exclude={"observed_at"}))


class PromotionEvidenceSelection(RuntimeContractModel):
    family_id: str = Field(min_length=1, max_length=128)
    experiment_id: Sha256
    walk_forward_id: UUID | None = None
    paper_account_id: str | None = Field(default=None, min_length=1, max_length=128)
    band_job_id: UUID | None = None


class StrategyPromotionCandidateReference(RuntimeContractModel):
    target: StrategyPromotionTarget
    selection: PromotionEvidenceSelection
    family_name: str = Field(min_length=1, max_length=128)
    job_id: UUID
    parent_count: int = Field(strict=True, ge=1, le=64)
    train_window: DateRange
    validation_window: DateRange
    has_sealed_reference: bool = Field(strict=True)
    input_hash: Sha256
    spec_hash: Sha256
    manifest_hash: Sha256 | None
    result_hash: Sha256 | None
    template_parent: ExperimentTemplateSelection | None = None
    is_current: bool = Field(strict=True)

    @model_validator(mode="after")
    def complete_reference(self) -> Self:
        if self.has_sealed_reference != (
            self.manifest_hash is not None and self.result_hash is not None
        ):
            raise ValueError("sealed choice differs from original full references")
        return self


class StrategyPromotionWalkForwardReference(RuntimeContractModel):
    command_id: UUID
    target_key: Sha256
    family_id: str
    experiment_id: Sha256
    fold_count: int = Field(strict=True, ge=1, le=6)
    submitted: bool = Field(strict=True)


class StrategyPromotionPaperReference(RuntimeContractModel):
    target_key: Sha256
    account_id: str = Field(min_length=1, max_length=128)
    band_jobs: tuple[UUID, ...] = Field(max_length=20)


class StrategyPromotionContext(RuntimeContractModel):
    owner_id: Owner
    metadata_identity: StrategyAuthoringIdentity
    source_kind: Literal["template", "builtin"]
    requested_strategy_id: str = Field(min_length=1, max_length=128)
    requested_head: StrategyTemplateHead
    original_metadata_identity: StrategyAuthoringIdentity | None = None
    candidates: tuple[StrategyPromotionCandidateReference, ...] = Field(max_length=64)
    walk_forward: tuple[StrategyPromotionWalkForwardReference, ...] = Field(max_length=64)
    paper_accounts: tuple[StrategyPromotionPaperReference, ...] = Field(max_length=64)
    can_evaluate: bool = Field(strict=True)
    can_approve: bool = Field(strict=True)
    can_run_walk_forward: bool = Field(strict=True)

    @model_validator(mode="after")
    def private_original_references(self) -> Self:
        if any(item.target.owner_id != self.owner_id for item in self.candidates):
            raise ValueError("manual choices differ from original actor")
        for item in self.candidates:
            if item.target.source_kind != self.source_kind:
                raise ValueError("manual choice differs from original strategy kind")
            direct = (item.target.strategy_id, item.target.head) == (
                self.requested_strategy_id,
                self.requested_head,
            )
            variant = (
                self.original_metadata_identity is not None
                and item.template_parent is not None
                and (item.template_parent.strategy_id, item.template_parent.head)
                == (self.requested_strategy_id, self.requested_head)
            )
            if not (direct or variant):
                raise ValueError(
                    "manual choice differs from exact requested version or original parent"
                )
        keys = {item.target.version_key for item in self.candidates}
        if any(item.target_key not in keys for item in (*self.walk_forward, *self.paper_accounts)):
            raise ValueError("forward references lack their exact original candidate")
        if len(self.model_dump_json().encode()) > 64 * 1024:
            raise ValueError("manual choices exceed original response capacity")
        return self


class PromotionGate(RuntimeContractModel):
    key: str = Field(min_length=1, max_length=64)
    status: Literal["satisfied", "failed", "missing"]
    message: str = Field(min_length=1, max_length=256)
    value: Finite | None = None


class StrategyPromotionState(RuntimeContractModel):
    target: StrategyPromotionTarget
    stage: PromotionStage = PromotionStage.EXPLORATORY
    revision: int = Field(default=0, strict=True, ge=0, le=3)
    latest_approval_hash: Sha256 | None = None
    paper_approval_hash: Sha256 | None = None
    paper_approved_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def exact_stage_revision(self) -> Self:
        if STAGES.index(self.stage) != self.revision:
            raise ValueError("manual stage revision cannot skip a stage")
        if (self.latest_approval_hash is None) != (self.revision == 0):
            raise ValueError("manual stage requires actual approval")
        if (self.paper_approval_hash is None) != (self.paper_approved_at is None):
            raise ValueError("paper start requires its actual human approval")
        if self.revision >= 2 and self.paper_approval_hash is None:
            raise ValueError("paper stage lacks human approval")
        return self


class StrategyPromotionReview(RuntimeContractModel):
    review_id: Sha256 | None = None
    command_id: UUID
    actor_id: Owner
    target: StrategyPromotionTarget
    metadata_identity: StrategyAuthoringIdentity
    from_stage: PromotionStage
    to_stage: PromotionStage
    expected_revision: int = Field(strict=True, ge=0, le=2)
    policy_hash: Sha256
    evidence_hash: Sha256
    selection: PromotionEvidenceSelection
    observed_at: AwareUtcDatetime
    gates: tuple[PromotionGate, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def exact_review(self) -> Self:
        if (
            next_stage(self.from_stage) != self.to_stage
            or STAGES.index(self.from_stage) != self.expected_revision
        ):
            raise ValueError("review skips a manual stage")
        if self.actor_id != self.target.owner_id:
            raise ValueError("private review actor differs from strategy owner")
        expected = canonical_sha256(self.model_dump(mode="python", exclude={"review_id"}))
        if self.review_id is None:
            object.__setattr__(self, "review_id", expected)
        elif self.review_id != expected:
            raise ValueError("review identity differs")
        if len(self.model_dump_json().encode()) > MAX_REVIEW_BYTES:
            raise ValueError("promotion review exceeds its bounded record")
        return self

    @property
    def eligible(self) -> bool:
        return all(g.status == "satisfied" for g in self.gates)


class PreparedPromotionApproval(RuntimeContractModel):
    preparation_id: UUID
    actor_id: Owner
    role_revision: int = Field(strict=True, ge=1)
    role_state_hash: Sha256
    review: StrategyPromotionReview
    issued_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime
    issuance_proof: Sha256

    @model_validator(mode="after")
    def confirmation_bounds(self) -> Self:
        if (
            not self.review.eligible
            or self.actor_id != self.review.actor_id
            or self.issued_at < self.review.observed_at
            or (self.expires_at - self.issued_at).total_seconds() != 120
        ):
            raise ValueError("approval preparation differs from eligible exact review")
        return self


class StrategyPromotionApproval(RuntimeContractModel):
    approval_id: Sha256 | None = None
    command_id: UUID
    effect_id: UUID
    actor_id: Owner
    original_request_hash: Sha256
    review: StrategyPromotionReview
    applied_at: AwareUtcDatetime
    after: StrategyPromotionState

    @model_validator(mode="after")
    def exact_approval(self) -> Self:
        if (
            not self.review.eligible
            or self.actor_id != self.review.actor_id
            or self.after.target != self.review.target
            or self.after.stage != self.review.to_stage
            or self.after.revision != self.review.expected_revision + 1
            or self.applied_at < self.review.observed_at
        ):
            raise ValueError("approval differs from its original eligible review")
        # The state references this content hash; exclude the self-reference.
        body = self.model_dump(mode="python", exclude={"approval_id"})
        body["after"]["latest_approval_hash"] = None
        if self.after.stage is PromotionStage.PAPER_CANDIDATE:
            body["after"]["paper_approval_hash"] = None
        expected = canonical_sha256(body)
        if self.approval_id is None:
            object.__setattr__(self, "approval_id", expected)
        elif self.approval_id != expected:
            raise ValueError("approval content identity differs")
        if self.after.latest_approval_hash != expected:
            raise ValueError("stage is not bound to its exact approval")
        return self
