"""Evidence review and manual stage CAS on the original private metadata owner."""

from __future__ import annotations

import hmac
import re
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import JsonValue

from rquant.collaboration_commands import PageControlRoleAuthority
from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_registry import PromotionStage
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.strategy_authoring import StrategyAuthoringConflict, StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity, StrategyTemplateHead
from rquant.strategy_promotion_commands import (
    PROMOTION_OWNED_TYPES,
    PROMOTION_PUBLIC_TYPES,
    ApprovePromotion,
    OwnedStrategyPromotionCommand,
    PreparePromotionApproval,
    RequestPromotionReview,
    RunStrategyWalkForward,
    StrategyPromotionCommand,
    own_strategy_promotion,
)
from rquant.strategy_promotion_contracts import (
    ManualPromotionPolicy,
    PreparedPromotionApproval,
    PromotionEvidenceBundle,
    PromotionEvidenceSelection,
    PromotionGate,
    StrategyPromotionApproval,
    StrategyPromotionCandidateReference,
    StrategyPromotionContext,
    StrategyPromotionPaperReference,
    StrategyPromotionReview,
    StrategyPromotionState,
    StrategyPromotionTarget,
    StrategyPromotionWalkForwardReference,
    next_stage,
)
from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource
from rquant.strategy_promotion_walk_forward import (
    StrategyPromotionWalkForwardPlan,
    StrategyPromotionWalkForwardSubmission,
    NativeStrategyPromotionWalkForwardPlan,
    PromotionWalkForwardPlan,
    PromotionWalkForwardSubmission,
)


def evaluate_review(
    request: RequestPromotionReview,
    *,
    state: StrategyPromotionState,
    bundle: PromotionEvidenceBundle,
    actor_id: str,
    metadata_identity: StrategyAuthoringIdentity,
) -> StrategyPromotionReview:
    if (
        state.target != request.target
        or bundle.target != request.target
        or actor_id != request.target.owner_id
    ):
        raise PermissionError("review does not bind the exact private owner/version")
    if state.revision != request.expected_revision:
        raise StrategyAuthoringConflict("manual stage revision changed")
    policy = ManualPromotionPolicy()
    stage = next_stage(state.stage)
    gates = []

    def gate(key: str, ok: bool | None, message: str, value: Decimal | None = None) -> None:
        gates.append(
            PromotionGate(
                key=key,
                status="missing" if ok is None else ("satisfied" if ok else "failed"),
                message=message,
                value=value,
            )
        )

    validation = bundle.validation
    if stage is PromotionStage.COMPARABLE:
        gate(
            "preregistered",
            None if validation is None else validation.parent_count >= 1,
            "预登记假设族",
        )
        gate(
            "fixed_validation",
            None
            if validation is None
            else validation.experiment_id == request.selection.experiment_id,
            "固定训练与验证区间",
        )
        gate(
            "validation_trades",
            None if validation is None else validation.closed_trades >= policy.validation_trades,
            "验证期完整交易至少30笔",
            None if validation is None else Decimal(validation.closed_trades),
        )
        gate("full_costs", None if validation is None else validation.full_costs, "完整计入成本")
    elif stage is PromotionStage.PAPER_CANDIDATE:
        gate(
            "manual_comparable",
            state.revision == 1 and state.latest_approval_hash is not None,
            "已人工批准为可比",
        )
        gate(
            "validation_sharpe",
            None
            if validation is None or validation.sharpe is None
            else validation.sharpe >= policy.validation_sharpe,
            "验证期夏普至少0.8",
            None if validation is None else validation.sharpe,
        )
        receipt = bundle.family_receipt
        full_parent = (
            receipt is not None
            and validation is not None
            and receipt.manifest.manifest_id == validation.parent_manifest_hash
            and receipt.manifest.hypothesis_count == validation.parent_count
            and any(
                a.spec.experiment_id == validation.experiment_id and a.outcome is not None
                for a in receipt.attempts
            )
        )
        gate(
            "full_parent_bh",
            None
            if not full_parent or bundle.adjusted_p is None
            else bundle.adjusted_p < policy.adjusted_p_limit,
            "全父搜索校正后p小于0.05",
            bundle.adjusted_p,
        )
        gate(
            "unique_outer",
            None if bundle.outer is None else bundle.outer.passed,
            "唯一完整样本外净收益为正",
            None if bundle.outer is None else bundle.outer.net_return,
        )
        folds = bundle.folds
        complete = (
            len(folds) == 6
            and validation is not None
            and all(
                validation.train_window.start_date <= f.train_dates[0]
                and f.test_dates[-1] <= validation.window.end_date
                for f in folds
            )
        )
        positives = sum(f.net_return > 0 for f in folds)
        gate(
            "six_folds",
            None if not complete else positives >= 4,
            "完整六折至少四折收益为正",
            Decimal(positives),
        )
    else:
        gate(
            "manual_paper",
            state.paper_approval_hash is not None and state.paper_approved_at is not None,
            "已人工批准为模拟候选",
        )
        forward = bundle.forward
        bound = forward is not None and forward.paper_approval_hash == state.paper_approval_hash
        if bound and state.paper_approved_at is not None:
            bound = (
                forward.first_date
                > state.paper_approved_at.astimezone(ZoneInfo("Asia/Shanghai")).date()
            )
        gate(
            "forward_open_days",
            None if not bound else forward.full_open_days >= 20,
            "批准后至少20个完整开市日",
            None if forward is None else Decimal(forward.full_open_days),
        )
        gate(
            "original_band",
            None if not bound else forward.all_inside_original_band,
            "模拟净值在原回测区间内",
        )
        gate("original_reconciliation", None if not bound else True, "原完整账本对账无差异")
    for index, message in enumerate(bundle.missing):
        gate(f"source_gap_{index}", None, message)
    return StrategyPromotionReview(
        command_id=UUID(request.command_id),
        actor_id=actor_id,
        target=request.target,
        metadata_identity=metadata_identity,
        from_stage=state.stage,
        to_stage=stage,
        expected_revision=state.revision,
        policy_hash=policy.fingerprint,
        evidence_hash=bundle.fingerprint,
        selection=request.selection,
        observed_at=bundle.observed_at,
        gates=tuple(gates),
    )


def build_approval(
    request: ApprovePromotion,
    state: StrategyPromotionState,
    *,
    effect_id: UUID,
    applied_at: datetime,
) -> StrategyPromotionApproval:
    review = request.preparation.review
    after = state.model_copy(
        update={
            "stage": review.to_stage,
            "revision": state.revision + 1,
            "latest_approval_hash": "0" * 64,
        }
    )
    if review.to_stage is PromotionStage.PAPER_CANDIDATE:
        after = after.model_copy(
            update={"paper_approval_hash": "0" * 64, "paper_approved_at": applied_at}
        )
    body = {
        "command_id": UUID(request.command_id),
        "effect_id": effect_id,
        "actor_id": review.actor_id,
        "original_request_hash": request.request_hash,
        "review": review.model_dump(mode="python"),
        "applied_at": applied_at,
        "after": after.model_dump(mode="python"),
    }
    body["after"]["latest_approval_hash"] = None
    if review.to_stage is PromotionStage.PAPER_CANDIDATE:
        body["after"]["paper_approval_hash"] = None
    digest = canonical_sha256(body)
    changes = {"latest_approval_hash": digest}
    if review.to_stage is PromotionStage.PAPER_CANDIDATE:
        changes["paper_approval_hash"] = digest
    after = StrategyPromotionState.model_validate(
        after.model_copy(update=changes).model_dump(mode="python")
    )
    return StrategyPromotionApproval(
        command_id=UUID(request.command_id),
        effect_id=effect_id,
        actor_id=review.actor_id,
        original_request_hash=request.request_hash,
        review=review,
        applied_at=applied_at,
        after=after,
    )


class StrategyPromotionBackend:
    def __init__(
        self,
        store: StrategyAuthoringStore,
        *,
        roles: PageControlRoleAuthority,
        source: StrategyPromotionEvidenceSource,
        enabled: bool = False,
        builtin_definitions: tuple[
            tuple[StrategyPromotionTarget, ImmutableDefinitionRegistry], ...
        ] = (),
    ) -> None:
        if (
            type(store) is not StrategyAuthoringStore
            or type(roles) is not PageControlRoleAuthority
            or type(source) is not StrategyPromotionEvidenceSource
        ):
            raise TypeError("manual promotion requires original concrete installed authorities")
        if type(enabled) is not bool:
            raise TypeError("manual promotion opt-in must be bool")
        binding = getattr(source, "template_binding", None)
        if binding is not None and store is not binding.private:
            raise TypeError("manual candidate facts belong to the installed original private store")
        self.store, self.roles, self.source, self.enabled = store, roles, source, enabled
        self.builtin_definitions = builtin_definitions

    def _builtin(self, target: StrategyPromotionTarget) -> None:
        matches = [registry for binding, registry in self.builtin_definitions if binding == target]
        if len(matches) != 1 or type(matches[0]) is not ImmutableDefinitionRegistry:
            raise PermissionError("exact builtin owner binding is not installed")
        registration = matches[0].read_strategy_spec(target.head.registration_fingerprint)
        if registration is None or (
            registration.logical_id,
            registration.version,
            registration.record_hash,
            registration.spec.spec_fingerprint,
            registration.spec.parameter_fingerprint,
        ) != (
            target.strategy_id,
            target.head.version,
            target.head.record_hash,
            target.head.spec_fingerprint,
            target.parameter_fingerprint,
        ):
            raise PermissionError("builtin installation lost its exact immutable definition")
        if matches[0].latest_strategy_spec(target.strategy_id,
            as_of=normalize_aware_utc(self.roles.clock())) != registration:
            raise PermissionError("builtin installation no longer has this exact current definition")

    def _authorize(
        self,
        actor_id: str,
        target: StrategyPromotionTarget,
        *,
        approval: bool = False,
        read: bool = False,
    ) -> None:
        if actor_id != target.owner_id:
            raise PermissionError("private strategy belongs to another actor")
        role = self.roles.current_role(actor_id)
        allowed = (
            {"admin", "researcher", "viewer"}
            if read
            else ({"admin"} if approval else {"admin", "researcher"})
        )
        if role not in allowed or (not read and not self.enabled):
            raise PermissionError("manual promotion is not enabled for current role")

    def lookup(
        self, request: StrategyPromotionCommand, *, actor_id: str
    ) -> (
        StrategyPromotionReview
        | PreparedPromotionApproval
        | StrategyPromotionApproval
        | PromotionWalkForwardPlan
        | None
    ):
        self._authorize(actor_id, request.target, read=True)
        original = self.store.lookup_promotion_command(request, actor_id=actor_id)
        if original is not None:
            return original
        wf = self.source.walk_forward
        if wf is None:
            return None
        if type(request) is RunStrategyWalkForward:
            return wf.lookup(request, actor_id=actor_id)
        if (
            wf.registry.walk_forward_plan_by_id(UUID(request.command_id), actor_id=actor_id)
            is not None
        ):
            raise StrategyAuthoringConflict("original UUID has another WF request kind")
        return None

    def run_walk_forward(
        self, request: RunStrategyWalkForward, *, actor_id: str
    ) -> PromotionWalkForwardSubmission:
        with self.roles.locked():
            old = self.lookup(request, actor_id=actor_id)
            wf = self.source.walk_forward
            if wf is None:
                raise ValueError("original fixed WF is not installed")
            if old is not None:
                if not isinstance(old, (StrategyPromotionWalkForwardPlan, NativeStrategyPromotionWalkForwardPlan)):
                    raise StrategyAuthoringConflict("original WF request kind differs")
                completed = wf.completed_submission(old, actor_id=actor_id)
                if completed is not None:
                    return completed
            self._authorize(actor_id, request.target)
            self.store.promotion_state(request.target, verify_builtin=self._builtin)
            plan = (
                old
                if old is not None
                else self.source.plan_walk_forward(
                    request,
                    metadata_identity=self.store.identity(),
                    as_of=normalize_aware_utc(self.roles.clock()),
                )
            )
            return wf.submit(plan, actor_id=actor_id)

    def review(self, request: RequestPromotionReview, *, actor_id: str) -> StrategyPromotionReview:
        with self.roles.locked():
            old = self.lookup(request, actor_id=actor_id)
            if old is not None:
                if type(old) is not StrategyPromotionReview:
                    raise ValueError("original kind differs")
                return old
            self._authorize(actor_id, request.target)
            state = self.store.promotion_state(request.target, verify_builtin=self._builtin)
            if state.revision > 0:
                original = next(
                    (
                        value
                        for value in self.store.promotion_approvals(owner_id=actor_id)
                        if value.approval_id == state.latest_approval_hash
                    ),
                    None,
                )
                if original is None or original.after != state:
                    raise StrategyAuthoringConflict("stage lacks its original human approval")
                if (
                    original.review.selection.family_id,
                    original.review.selection.experiment_id,
                ) != (request.selection.family_id, request.selection.experiment_id):
                    raise StrategyAuthoringConflict(
                        "approved fixed candidate cannot change between stages"
                    )
            now = normalize_aware_utc(self.roles.clock())
            bundle = self.source.read_selection(
                request.target,
                request.selection,
                state=state,
                as_of=now,
                register_statistics=state.stage is PromotionStage.COMPARABLE,
            )
            review = evaluate_review(
                request,
                state=state,
                bundle=bundle,
                actor_id=actor_id,
                metadata_identity=self.store.identity(),
            )
            return self.store.record_promotion_review(request, review, verify_builtin=self._builtin)

    def prepare(
        self, request: PreparePromotionApproval, *, actor_id: str
    ) -> PreparedPromotionApproval:
        with self.roles.locked():
            old = self.lookup(request, actor_id=actor_id)
            if old is not None:
                if type(old) is not PreparedPromotionApproval:
                    raise ValueError("original kind differs")
                return old
            self._authorize(actor_id, request.target, approval=True)
            review = self.store.promotion_review(request.review_id, owner_id=actor_id)
            if review.target != request.target:
                raise ValueError("prepared target differs from review")
            state = self.store.promotion_state(request.target, verify_builtin=self._builtin)
            if state.revision != review.expected_revision or not review.eligible:
                raise StrategyAuthoringConflict("review is not eligible at current revision")
            roles = self.roles.read_state()
            now = normalize_aware_utc(self.roles.clock())
            prepared = PreparedPromotionApproval(
                preparation_id=UUID(request.command_id),
                actor_id=actor_id,
                role_revision=roles.revision,
                role_state_hash=roles.content_sha256,
                review=review,
                issued_at=now,
                expires_at=now + timedelta(seconds=120),
                issuance_proof="0" * 64,
            )
            proof = self.roles._mac(
                "strategy-promotion-approval",
                prepared.model_dump(mode="python", exclude={"issuance_proof"}),
            )
            prepared = prepared.model_copy(update={"issuance_proof": proof})
            return self.store.record_promotion_preparation(
                request, prepared, verify_builtin=self._builtin
            )

    def approve(
        self, request: ApprovePromotion, *, actor_id: str, effect_id: UUID
    ) -> StrategyPromotionApproval:
        with self.roles.locked():
            old = self.lookup(request, actor_id=actor_id)
            if old is not None:
                if type(old) is not StrategyPromotionApproval or old.effect_id != effect_id:
                    raise StrategyAuthoringConflict("original approval effect differs")
                return old
            self._authorize(actor_id, request.target, approval=True)
            prepared = request.preparation
            review = prepared.review

            def verify(state: StrategyPromotionState) -> None:
                self._authorize(actor_id, request.target, approval=True)
                roles = self.roles.read_state()
                now = normalize_aware_utc(self.roles.clock())
                proof = self.roles._mac(
                    "strategy-promotion-approval",
                    prepared.model_dump(mode="python", exclude={"issuance_proof"}),
                )
                if (
                    not hmac.compare_digest(proof, prepared.issuance_proof)
                    or (roles.revision, roles.content_sha256)
                    != (prepared.role_revision, prepared.role_state_hash)
                    or not prepared.issued_at <= now < prepared.expires_at
                    or request.entered_name != request.target.name
                    or review.policy_hash != ManualPromotionPolicy().fingerprint
                ):
                    raise StrategyAuthoringConflict("promotion confirmation expired or changed")
                current = self.source.read_selection(
                    request.target,
                    review.selection,
                    state=state,
                    as_of=review.observed_at,
                    register_statistics=False,
                )
                if current.fingerprint != review.evidence_hash:
                    raise StrategyAuthoringConflict("promotion evidence changed after confirmation")

            return self.store.apply_promotion_approval(
                request, effect_id=effect_id, verify=verify, verify_builtin=self._builtin
            )


class StrategyPromotionPageControlBackend:
    """One installed domain owner used by the original command/effect journal."""

    def __init__(self, domain: StrategyPromotionBackend, *, operator_users: frozenset[str]) -> None:
        if type(domain) is not StrategyPromotionBackend or type(operator_users) is not frozenset:
            raise TypeError("promotion requires the original concrete domain and exact operators")
        if len(operator_users) > 64 or any(
            not isinstance(name, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@-]{0,127}", name) is None
            for name in operator_users
        ):
            raise ValueError("promotion operator allowlist exceeds capacity")
        self.domain, self.operator_users = domain, operator_users

    def context(
        self,
        *,
        actor_id: str,
        source_kind: Literal["template", "builtin"],
        strategy_id: str,
        head: StrategyTemplateHead | None = None,
    ) -> StrategyPromotionContext:
        role = self.domain.roles.current_role(actor_id)
        if role not in {"admin", "researcher", "viewer"}:
            raise PermissionError("current private actor is unavailable")
        identity = self.domain.store.identity()
        observed = normalize_aware_utc(self.domain.roles.clock())
        source = self.domain.source
        binding = getattr(source, "template_binding", None)
        original_identity = None
        parent_scope = False
        current = True
        if source_kind == "template":
            try:
                latest = self.domain.store.get_current(strategy_id, owner_id=actor_id)
            except KeyError as exc:
                if binding is None or head is None:
                    raise
                latest = binding.original.get_current(strategy_id, owner_id=actor_id)
                version = binding.original.get_version(strategy_id, head.version, owner_id=actor_id)
                original_identity = binding.original.identity()
                if original_identity != binding.expected_original_identity:
                    raise PermissionError("original parent metadata identity changed") from exc
                parent_scope = True
            else:
                head = latest.head if head is None else head
                version = self.domain.store.get_version(
                    strategy_id, head.version, owner_id=actor_id
                )
            if version.head != head:
                raise ValueError("manual context differs from immutable version")
            current = latest.head == head and not latest.archived
        else:
            installed = tuple(
                target
                for target, _ in self.domain.builtin_definitions
                if (target.owner_id, target.strategy_id) == (actor_id, strategy_id)
                and (head is None or target.head == head)
            )
            if len(installed) != 1:
                raise PermissionError("exact builtin private owner is not installed")
            self.domain._builtin(installed[0])
            head = installed[0].head
        choices = []
        with source.context_read(observed) as original:
            snapshot = original.snapshot
            if snapshot is not None and actor_id in snapshot.truncated_owners:
                raise ValueError("full original parent references exceed read capacity")
            for family in () if snapshot is None else snapshot.families:
                if (family.owner, family.phase, family.preparation_state) != (
                    actor_id,
                    "search",
                    "ready",
                ):
                    continue
                if getattr(family.request, "walk_forward_plan_hash", None) is not None:
                    continue
                selected = family.request.template
                if source_kind == "template":
                    if selected is None:
                        continue
                    if parent_scope and (selected.strategy_id, selected.head) != (
                        strategy_id, head
                    ):
                        continue
                    if binding is not None:
                        record = source.platform.get_family(actor_id, family.family_id)
                        baseline = record.template_baseline
                        if (
                            baseline is None
                            or baseline.metadata_identity != binding.expected_original_identity
                        ):
                            raise ValueError(
                                "private candidate lost its exact original baseline identity"
                            )
                facts = tuple(
                    fact
                    for fact in snapshot.attempts
                    if (fact.owner, fact.family_id) == (actor_id, family.family_id)
                )
                if len(facts) != family.planned_count or sorted(f.index for f in facts) != list(
                    range(family.planned_count)
                ):
                    raise ValueError("full original parent references are incomplete")
                for fact in facts:
                    registration = original.registration(fact)
                    if source_kind == "template":
                        if not parent_scope and registration.logical_id != strategy_id:
                            continue
                        metadata = self.domain.store.get_version(
                            registration.logical_id, registration.version, owner_id=actor_id
                        )
                        actual_head = metadata.head
                        if (
                            registration.fingerprint,
                            registration.record_hash,
                            registration.spec.spec_fingerprint,
                        ) != (
                            actual_head.registration_fingerprint,
                            actual_head.record_hash,
                            actual_head.spec_fingerprint,
                        ):
                            raise ValueError(
                                "candidate definition differs from its original saved version"
                            )
                        if not parent_scope and actual_head != head:
                            continue
                        latest_candidate = self.domain.store.get_current(
                            registration.logical_id, owner_id=actor_id
                        )
                        target = StrategyPromotionTarget(
                            source_kind=source_kind,
                            owner_id=actor_id,
                            strategy_id=metadata.strategy_id,
                            name=metadata.name,
                            head=actual_head,
                            parameter_fingerprint=registration.spec.parameter_fingerprint,
                            cost_fingerprint=fact.attempt.spec.cost_model_fingerprint,
                        )
                        is_current = (
                            current
                            and latest_candidate.head == actual_head
                            and not latest_candidate.archived
                        )
                    else:
                        if (
                            registration.logical_id,
                            registration.version,
                            registration.fingerprint,
                            registration.record_hash,
                            registration.spec.spec_fingerprint,
                        ) != (
                            strategy_id,
                            head.version,
                            head.registration_fingerprint,
                            head.record_hash,
                            head.spec_fingerprint,
                        ):
                            continue
                        target = installed[0]
                        if (
                            registration.spec.parameter_fingerprint,
                            fact.attempt.spec.cost_model_fingerprint,
                        ) != (target.parameter_fingerprint, target.cost_fingerprint):
                            continue
                        is_current = True
                    choices.append(
                        StrategyPromotionCandidateReference(
                            target=target,
                            selection=PromotionEvidenceSelection(
                                family_id=family.family_id,
                                experiment_id=fact.attempt.spec.experiment_id,
                            ),
                            family_name=family.name,
                            job_id=fact.child.job_id,
                            parent_count=family.planned_count,
                            train_window=fact.attempt.spec.train_range,
                            validation_window=fact.attempt.spec.validation_range,
                            has_sealed_reference=fact.result_hash is not None
                            and fact.manifest_hash is not None,
                            input_hash=fact.input_hash,
                            spec_hash=fact.spec_hash,
                            manifest_hash=fact.manifest_hash,
                            result_hash=fact.result_hash,
                            template_parent=selected if source_kind == "template" else None,
                            is_current=is_current,
                        )
                    )
        if len(choices) > 64:
            raise ValueError("original candidate references exceed response capacity")
        keys = {item.target.version_key for item in choices}
        wf = source.walk_forward
        plans = (
            ()
            if wf is None
            else tuple(
                plan
                for selected_id in sorted({item.target.strategy_id for item in choices})
                for plan in source.registry.walk_forward_plans_for_strategy(
                    selected_id, actor_id=actor_id
                )
            )
        )
        forward = tuple(
            StrategyPromotionWalkForwardReference(
                command_id=UUID(plan.request.command_id),
                target_key=plan.request.target.version_key,
                family_id=plan.request.selection.family_id,
                experiment_id=plan.request.selection.experiment_id,
                fold_count=len(plan.folds),
                submitted=wf.completed_submission(plan, actor_id=actor_id) is not None,
            )
            for plan in plans
            if plan.request.target.version_key in keys and plan.metadata_identity == identity
        )
        paper = []
        for paper_source in source.paper_sources:
            configuration = paper_source.runtime.state.refresh_configuration()
            binding_account = configuration.binding
            for key, target in {
                choice.target.version_key: choice.target for choice in choices
            }.items():
                if (
                    binding_account.owner_id,
                    binding_account.strategy_id,
                    binding_account.strategy_version,
                    binding_account.parameter_fingerprint,
                    canonical_sha256(configuration.execution_cost_spec),
                ) != (
                    actor_id,
                    target.strategy_id,
                    str(target.head.version),
                    target.parameter_fingerprint,
                    target.cost_fingerprint,
                ):
                    continue
                with paper_source.runtime.state._connection() as connection:
                    exists = connection.execute(
                        "SELECT 1 FROM sqlite_master "
                        "WHERE type='table' AND name='paper_research_admissions'"
                    ).fetchone()
                    rows = (
                        ()
                        if exists is None
                        else connection.execute(
                            "SELECT command_id FROM paper_research_admissions "
                            "WHERE json_extract(owned_body,'$.owner_id')=? "
                            "ORDER BY command_id LIMIT 21",
                            (actor_id,),
                        ).fetchall()
                    )
                if len(rows) > 20:
                    raise ValueError("original band references exceed read capacity")
                paper.append(
                    StrategyPromotionPaperReference(
                        target_key=key,
                        account_id=binding_account.account_id,
                        band_jobs=tuple(UUID(row[0]) for row in rows),
                    )
                )
        if (
            self.domain.store.identity() != identity
            or self.domain.roles.current_role(actor_id) != role
        ):
            raise ValueError("original metadata or role changed during private read")
        if original_identity is not None and binding.original.identity() != original_identity:
            raise ValueError("original parent identity changed during private read")
        enabled = (
            self.domain.enabled
            and current
            and actor_id in self.operator_users
            and any(item.is_current for item in choices)
        )
        native_wf = None if wf is None else wf.native
        native_wf_installed = (
            source_kind == "builtin"
            and native_wf is not None
            and wf.registry is source.registry
            and native_wf.store is self.domain.store
            and native_wf.expected_identity == identity
            and native_wf.writer.store is source.platform
            and native_wf.results is not None
            and native_wf.results is source.native_results
            and native_wf.writer.native_results is source.native_results
            and native_wf.writer.enabled
            and actor_id in native_wf.writer.owners
        )
        return StrategyPromotionContext(
            owner_id=actor_id,
            metadata_identity=identity,
            source_kind=source_kind,
            requested_strategy_id=strategy_id,
            requested_head=head,
            original_metadata_identity=original_identity,
            candidates=tuple(choices),
            walk_forward=forward,
            paper_accounts=tuple(paper),
            can_evaluate=enabled and role in {"admin", "researcher"},
            can_approve=enabled and role == "admin",
            can_run_walk_forward=enabled
            and role in {"admin", "researcher"}
            and wf is not None
            and (source_kind == "template" or native_wf_installed),
        )

    def authorize(
        self, actor_id: str, request: StrategyPromotionCommand, *, read: bool = False
    ) -> None:
        if type(request) not in PROMOTION_PUBLIC_TYPES:
            raise TypeError("original promotion request required")
        self.domain._authorize(
            actor_id,
            request.target,
            approval=type(request) in (PreparePromotionApproval, ApprovePromotion),
            read=read,
        )
        if not read and actor_id not in self.operator_users:
            raise PermissionError("current actor is not an installed promotion operator")

    def compile(
        self,
        request: StrategyPromotionCommand,
        *,
        actor_id: str,
        expected_identity: StrategyAuthoringIdentity,
    ) -> OwnedStrategyPromotionCommand:
        self.authorize(actor_id, request)
        if self.domain.store.identity() != expected_identity:
            raise ValueError("original promotion metadata identity changed")
        self.domain.store.promotion_state(request.target, verify_builtin=self.domain._builtin)
        return own_strategy_promotion(
            request,
            actor_id=actor_id,
            metadata_identity=expected_identity,
            accepted_at=normalize_aware_utc(self.domain.roles.clock()),
        )

    def validate(self, command: OwnedStrategyPromotionCommand) -> None:
        if type(command) not in PROMOTION_OWNED_TYPES:
            raise TypeError("private original promotion command required")
        self.authorize(command.owner_id, command.original(), read=True)
        if self.domain.store.identity() != command.metadata_identity:
            raise ValueError("original promotion metadata physical identity differs")

    def recover(self, command: OwnedStrategyPromotionCommand) -> JsonValue | None:
        self.validate(command)
        original = command.original()
        result = self.domain.lookup(original, actor_id=command.owner_id)
        if result is None:
            return None
        if type(original) is RunStrategyWalkForward:
            wf = self.domain.source.walk_forward
            if wf is None:
                raise ValueError("original WF authority is unavailable")
            if result.metadata_identity != command.metadata_identity:
                raise ValueError("original WF metadata identity differs")
            result = wf.completed_submission(result, actor_id=command.owner_id)
            if result is None:
                return None
        else:
            review = result if type(result) is StrategyPromotionReview else result.review
            if review.metadata_identity != command.metadata_identity:
                raise ValueError("original manual result metadata identity differs")
            if type(result) is StrategyPromotionApproval and result.effect_id != UUID(
                command.command_id
            ):
                raise ValueError("original approval effect identity differs")
        return result.model_dump(mode="json")

    def submit(self, command: OwnedStrategyPromotionCommand) -> JsonValue:
        old = self.recover(command)
        if old is not None:
            return old
        request = command.original()
        self.authorize(command.owner_id, request)
        if type(request) is RequestPromotionReview:
            result = self.domain.review(request, actor_id=command.owner_id)
        elif type(request) is PreparePromotionApproval:
            result = self.domain.prepare(request, actor_id=command.owner_id)
        elif type(request) is ApprovePromotion:
            result = self.domain.approve(
                request, actor_id=command.owner_id, effect_id=UUID(command.command_id)
            )
        else:
            result = self.domain.run_walk_forward(request, actor_id=command.owner_id)
        return result.model_dump(mode="json")

    def recover_partial_walk_forward(
        self, command: OwnedStrategyPromotionCommand
    ) -> JsonValue | None:
        self.validate(command)
        request = command.original()
        if type(request) is not RunStrategyWalkForward:
            return None
        plan = self.domain.lookup(request, actor_id=command.owner_id)
        if not isinstance(plan, (StrategyPromotionWalkForwardPlan, NativeStrategyPromotionWalkForwardPlan)):
            return None
        if plan.metadata_identity != command.metadata_identity:
            raise ValueError("original WF plan metadata differs")
        try:
            self.authorize(command.owner_id, request)
        except PermissionError:
            return None
        wf = self.domain.source.walk_forward
        if isinstance(plan, NativeStrategyPromotionWalkForwardPlan):
            self.domain._builtin(request.target)
            owner = None if wf is None or wf.native is None else wf.native.store
        else:
            current = self.domain.store.get_current(request.target.strategy_id, owner_id=command.owner_id)
            if current.head != request.target.head or current.archived:
                raise ValueError("original WF current version is no longer writable")
            owner = None if wf is None or wf.runs is None else wf.runs.store
        if owner is not self.domain.store:
            raise ValueError("original WF child metadata owner differs")
        # Each original compile/submit reads its accepted body and original journal/spool
        # before preparing or publishing. A missing receipt alone grants no fresh UUID.
        result = wf.submit(plan, actor_id=command.owner_id)
        return result.model_dump(mode="json")
