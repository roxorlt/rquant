"""Study submission through the original PageControl effect and minute writer."""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Self

from pydantic import Field, JsonValue, model_validator

from rquant.lab_job_center import CommandSubmissionReceipt
from rquant.lab_job_protocol import LabCommandEnvelope
from rquant.minute_backtest_commands import MinuteCommandWriter, MinuteRunEffect
from rquant.minute_backtest_contracts import MAX_WORK_UNITS, Sha256
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayAdapter
from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
from rquant.minute_backtest_parameter_study_commands import (
    SubmitMinuteParameterStudy,
    _study_control_size,
)
from rquant.minute_backtest_parameter_study_execution import (
    MinuteParameterPreparedStudyTrial,
    MinuteParameterStudyExecutionEffect,
    MinuteParameterStudyExecutionPlan,
    build_minute_parameter_study_execution,
    prepare_minute_parameter_study_trial,
)
from rquant.minute_backtest_publication_contracts import MAX_MINUTE_CONTROL_BYTES
from rquant.runtime_contracts import RuntimeContractModel
from rquant.strict_json import canonical_json_bytes, strict_model_validate_json

if TYPE_CHECKING:
    from rquant.page_control import PageControlEffectRecord, PageControlService


class MinuteParameterStudySubmissionUncertainError(RuntimeError):
    """An original child submission may exist; preserve the parent effect for recovery."""


class MinuteParameterStudySubmissionReceipt(RuntimeContractModel):
    parent_command_id: str = Field(pattern=r"^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
    plan_id: Sha256
    state: Literal["submitted", "unavailable"]
    receipts: tuple[CommandSubmissionReceipt, ...] = Field(max_length=MAX_WORK_UNITS)
    unavailable_reasons: tuple[Literal["insufficient_fold_dates"], ...] = ()

    @model_validator(mode="after")
    def retain_original_submission(self) -> Self:
        if len({value.request_id for value in self.receipts}) != len(self.receipts) or len(
            {value.job_id for value in self.receipts}
        ) != len(self.receipts):
            raise ValueError("duplicate child submission identity")
        if self.state == "submitted":
            if not self.receipts or self.unavailable_reasons:
                raise ValueError("submitted study requires original child receipts")
        elif self.receipts or not self.unavailable_reasons:
            raise ValueError("unavailable study requires its original reason without child jobs")
        _study_control_size(self)
        return self


class MinuteParameterStudyCommandWriter:
    def __init__(self, minute_writer: MinuteCommandWriter) -> None:
        if type(minute_writer) is not MinuteCommandWriter:
            raise TypeError("study requires the original MinuteCommandWriter")
        self.minute_writer = minute_writer
        self.owner_authority: PageControlService | None = None

    def bind_owner_authority(self, authority: PageControlService) -> None:
        from rquant.page_control import PageControlService

        if type(authority) is not PageControlService:
            raise TypeError("study requires the original PageControlService")
        if (
            getattr(authority.consumer, "minute_study_backend", None) is not self
            or authority.consumer.minute_backend is not self.minute_writer
            or self.minute_writer.owner_authority is not authority
            or authority.outbox is not authority.consumer.outbox
            or authority.outbox.collaboration is not authority.collaboration
            or authority.collaboration.mode != "enforced"
        ):
            raise PermissionError("study backends are not bound to one original enforced owner")
        if self.owner_authority is not None and self.owner_authority is not authority:
            raise PermissionError("study original owner authority cannot be rebound")
        self.owner_authority = authority

    def _authority(self) -> PageControlService:
        authority = self.owner_authority
        if authority is None:
            raise PermissionError("study original owner authority is not bound")
        self.bind_owner_authority(authority)
        return authority

    def _guard(
        self,
        command: SubmitMinuteParameterStudy,
        *,
        claim: PageControlEffectRecord | None = None,
        saved: MinuteParameterStudyExecutionEffect | None = None,
    ) -> PageControlEffectRecord:
        from rquant.page_control import (
            PageControlEffectStatus,
            PageControlStatus,
            _command_hash,
        )

        authority = self._authority()
        if type(command) is not SubmitMinuteParameterStudy:
            raise TypeError("study requires the complete original typed parent command")
        original = strict_model_validate_json(
            SubmitMinuteParameterStudy,
            authority.outbox.original_command_bytes(command.command_id),
        )
        digest = _command_hash(command)
        if original != command:
            raise PermissionError("study original parent UUID/body changed")
        actor = authority.outbox.trusted_command_actor(command.command_id, digest)
        if (
            actor != command.actor_id
            or authority.outbox.require_current_command_role(command) != actor
        ):
            raise PermissionError("study original current command actor differs")
        audit = authority.outbox.audit(command.command_id)
        effect = authority.outbox.effect(command.command_id)
        if (
            audit is None
            or effect is None
            or (audit.command_id, audit.command_kind, audit.command_hash, audit.status)
            != (command.command_id, command.kind, digest, PageControlStatus.PROCESSING)
            or (
                effect.command_id,
                effect.effect_kind,
                effect.command_hash,
                effect.status,
                effect.owner_id,
            )
            != (
                command.command_id,
                command.kind,
                digest,
                PageControlEffectStatus.STARTED,
                authority.consumer.consumer_id,
            )
            or not effect.claim_token
        ):
            raise PermissionError("study original active command/effect claim is unavailable")
        if claim is not None and (effect.owner_id, effect.claim_token) != (
            claim.owner_id,
            claim.claim_token,
        ):
            raise PermissionError("study original claim owner/token changed")
        authority.outbox.require_active_claim(
            command, owner_id=effect.owner_id, claim_token=effect.claim_token
        )
        if saved is not None and (
            effect.result is None or self._decode_effect(command, effect.result) != saved
        ):
            raise PermissionError("study complete original effect was not durably saved")
        return effect

    @staticmethod
    def _decode_effect(
        command: SubmitMinuteParameterStudy, marker: JsonValue
    ) -> MinuteParameterStudyExecutionEffect:
        raw = canonical_json_bytes(marker)
        if len(raw) > MAX_MINUTE_CONTROL_BYTES:
            raise ValueError("complete study effect exceeds control capacity")
        effect = strict_model_validate_json(MinuteParameterStudyExecutionEffect, raw)
        if effect.command != command:
            raise PermissionError("study effect differs from its complete original parent")
        if len(effect.prepared) != effect.plan.trial_count:
            raise PermissionError("study requires every original prepared trial before submission")
        return effect

    def _verify_baseline(self, plan: MinuteParameterStudyExecutionPlan) -> None:
        installation = self.minute_writer.installation
        installation.verify_current()
        catalog = installation.profile.parameter_catalog
        if catalog is None or plan.baseline_reference not in catalog.fact_sources:
            raise PermissionError("study complete installed baseline is unavailable")
        request = plan.request
        receipt = catalog.resolve_fact(
            source_key=request.source_key,
            source_version=request.source_version,
            owner_id=request.owner_id,
            full_input_hash=request.full_input_hash,
        )
        frozen = receipt.frozen
        runtime, provenance, native = frozen.runtime, frozen.provenance, frozen.native_registration
        source = plan.baseline_source
        if (
            source.source_key,
            source.source_version,
            source.owner_id,
            source.full_input_hash,
            source.dataset_snapshot_id,
            source.frequency,
            source.start_date,
            source.end_date,
            source.published_at,
        ) != (
            runtime.source_key,
            runtime.source_version,
            runtime.owner_id,
            frozen.full_input_hash,
            runtime.dataset_snapshot_id,
            runtime.source_frequency,
            runtime.start_date,
            runtime.end_date,
            provenance.published_at,
        ):
            raise PermissionError("study baseline source differs from actual installed facts")
        head = plan.baseline_head
        if (
            head.definition_id,
            head.definition_version,
            head.evaluator_semantic_version,
            head.parameter_fingerprint,
            head.registration_fingerprint,
            head.spec_fingerprint,
            head.executable_fingerprint,
            head.producer_commit,
        ) != (
            native.logical_id,
            native.version,
            runtime.parameters.evaluator_semantic_version,
            runtime.parameters.fingerprint,
            native.fingerprint,
            native.spec.spec_fingerprint,
            native.executable_fingerprint,
            runtime.producer_commit,
        ):
            raise PermissionError("study original baseline head differs from actual registration")
        dates = tuple(
            day
            for day in runtime.daily_trade_dates
            if request.formal_protocol.train_range.start_date
            <= day
            <= request.formal_protocol.frozen_outer_test_range.end_date
        )
        if (
            dates != plan.calendar_dates
            or not provenance.published_at <= request.requested_at <= installation.clock()
            or (runtime.parameters.parameters.family, runtime.source_frequency)
            != (request.parameters.parameters.family, request.parameters.parameters.freq)
        ):
            raise PermissionError("study original source calendar/time/family differs")
        installation.verify_current()

    def _verify_prepared(
        self, plan: MinuteParameterStudyExecutionPlan, prepared: MinuteParameterPreparedStudyTrial
    ) -> None:
        writer = self.minute_writer
        checked = writer._marker(
            prepared.trial.command, prepared.marker.model_dump(mode="json")
        )
        if type(checked) is not MinuteRunEffect or checked != prepared.marker:
            raise PermissionError("study original child marker differs")
        catalog = writer.installation.profile.parameter_catalog
        if catalog is None:
            raise PermissionError("study original installed parameter source is unavailable")
        adapter = MinuteParameterFormalReplayAdapter(catalog)
        parameters = adapter.parameters(checked.command.spec)
        expected = adapter.expected(parameters)
        runtime = expected.frozen.runtime
        if (
            prepared.binding,
            prepared.full_input_hash,
            prepared.core_input_hash,
            prepared.seed_hash,
            prepared.profile_hash,
            prepared.work_units,
        ) != (
            runtime.study_binding,
            expected.frozen.full_input_hash,
            expected.frozen.core_input_hash,
            expected.frozen.source_content_seed.seed_hash,
            runtime.execution_profile.profile_hash,
            parameters.work_units,
        ) or prepared.binding.protocol.source != plan.baseline_source:
            raise PermissionError("study child complete binding/input/profile/work differs")
        writer.installation.verify_current()

    def freeze(self, command: SubmitMinuteParameterStudy) -> JsonValue:
        claim = self._guard(command)
        with minute_parameter_validation_scope(), self._authority().outbox.command_fence(command):
            return self._freeze_bound(command, claim)

    def _freeze_bound(
        self, command: SubmitMinuteParameterStudy, claim: PageControlEffectRecord
    ) -> JsonValue:
        self._guard(command, claim=claim)
        if claim.result is not None:
            effect = self._decode_effect(command, claim.result)
            self._verify_baseline(effect.plan)
            self._guard(command, claim=claim, saved=effect)
            for prepared in effect.prepared:
                self._verify_prepared(effect.plan, prepared)
                self._guard(command, claim=claim, saved=effect)
            return effect.model_dump(mode="json", exclude_computed_fields=True)
        catalog = self.minute_writer.installation.profile.parameter_catalog
        if catalog is None:
            raise PermissionError("study original installed parameter source is unavailable")
        self.minute_writer.installation.verify_current()
        plan = build_minute_parameter_study_execution(
            command.request, catalog=catalog, as_of=self.minute_writer.installation.clock()
        )
        self._guard(command, claim=claim)
        prepared: list[MinuteParameterPreparedStudyTrial] = []
        for trial in plan.trials:
            self._guard(command, claim=claim)
            prepared.append(
                prepare_minute_parameter_study_trial(
                    plan, trial_index=trial.index, writer=self.minute_writer
                )
            )
            self._guard(command, claim=claim)
        effect = MinuteParameterStudyExecutionEffect(
            command=command, plan=plan, prepared=tuple(prepared)
        )
        marker = effect.model_dump(mode="json", exclude_computed_fields=True)
        self._decode_effect(command, marker)
        self._guard(command, claim=claim)
        return marker

    def _receipt(
        self, prepared: MinuteParameterPreparedStudyTrial, value: JsonValue
    ) -> CommandSubmissionReceipt:
        receipt = strict_model_validate_json(CommandSubmissionReceipt, canonical_json_bytes(value))
        registry = self.minute_writer.installation.commands.experiment_registry
        if registry is None:
            raise PermissionError("study original child submission registry is unavailable")
        intent = registry.get_submission_intent_for_job(prepared.marker.command.job_id)
        if intent is None:
            raise PermissionError("study original child submission intent is unavailable")
        envelope = strict_model_validate_json(LabCommandEnvelope, intent.envelope_json)
        if envelope.command != prepared.marker.command or (
            receipt.request_id,
            receipt.job_id,
            receipt.command_type,
            receipt.expected_version,
            receipt.spool.content_hash,
        ) != (
            envelope.request_id,
            prepared.trial.job_id,
            "submit",
            None,
            envelope.content_hash,
        ):
            raise PermissionError("study receipt differs from its actual original child submission")
        return receipt

    def _submit(
        self, command: SubmitMinuteParameterStudy, marker: JsonValue, *, recovery: bool
    ) -> JsonValue | None:
        claim = self._guard(command)
        with minute_parameter_validation_scope(), self._authority().outbox.command_fence(command):
            return self._submit_bound(command, marker, claim=claim, recovery=recovery)

    def _submit_bound(
        self,
        command: SubmitMinuteParameterStudy,
        marker: JsonValue,
        *,
        claim: PageControlEffectRecord,
        recovery: bool,
    ) -> JsonValue | None:
        effect = self._decode_effect(command, marker)
        self._guard(command, claim=claim, saved=effect)
        self._verify_baseline(effect.plan)
        self._guard(command, claim=claim, saved=effect)
        # Validate the complete saved tuple before the first possible submission.
        for prepared in effect.prepared:
            self._verify_prepared(effect.plan, prepared)
            self._guard(command, claim=claim, saved=effect)
        receipts: list[CommandSubmissionReceipt] = []
        delegated = False
        try:
            for prepared in effect.prepared:
                self._verify_prepared(effect.plan, prepared)
                self._guard(command, claim=claim, saved=effect)
                child_marker = prepared.marker.model_dump(mode="json")
                delegated = True
                value = (
                    self.minute_writer.recover(prepared.trial.command, child_marker)
                    if recovery
                    else self.minute_writer.submit(prepared.trial.command, child_marker)
                )
                self._guard(command, claim=claim, saved=effect)
                if value is None:
                    return None
                receipts.append(self._receipt(prepared, value))
                self._guard(command, claim=claim, saved=effect)
            result = MinuteParameterStudySubmissionReceipt(
                parent_command_id=command.command_id,
                plan_id=effect.plan.plan_id,
                state="submitted" if effect.plan.state == "ready" else "unavailable",
                receipts=tuple(receipts),
                unavailable_reasons=effect.plan.unavailable_reasons,
            )
            self._guard(command, claim=claim, saved=effect)
            return result.model_dump(mode="json")
        except Exception as error:
            if delegated:
                raise MinuteParameterStudySubmissionUncertainError(
                    "original study child submission must recover its saved effect"
                ) from error
            raise

    def submit(self, command: SubmitMinuteParameterStudy, marker: JsonValue) -> JsonValue:
        result = self._submit(command, marker, recovery=False)
        if result is None:
            raise MinuteParameterStudySubmissionUncertainError(
                "study original child submission has no receipt"
            )
        return result

    def recover(self, command: SubmitMinuteParameterStudy, marker: JsonValue) -> JsonValue | None:
        return self._submit(command, marker, recovery=True)
