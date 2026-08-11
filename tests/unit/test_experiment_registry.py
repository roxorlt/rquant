from __future__ import annotations

import os
import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

import rquant.artifact_retention as artifact_retention_module
import rquant.experiment_registry as registry_module
from rquant.experiment_registry import (
    DateRange,
    ExperimentIdentityConflictError,
    ExperimentOutcome,
    ExperimentRegistry,
    ExperimentRegistryError,
    ExperimentRegistryReadonlyReader,
    ExperimentSpec,
    ExperimentStatus,
    ExperimentSubmissionIntent,
    FormalExperimentPlan,
    IncompleteHypothesisFamilyError,
    PromotionDecision,
    PromotionStage,
    TerminalExperimentError,
)

_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_HASH_D = "d" * 64
_HASH_E = "e" * 64
_HASH_F = "f" * 64
_EXECUTABLE_HASH = "1" * 64
_CANDIDATE_SCHEMA_HASH = "2" * 64
_COMMIT = "e" * 40
_NOW = datetime(2026, 7, 31, 1, 0, tzinfo=UTC)
_FORWARD_AVAILABLE = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)


def _exception_leaves(error: BaseException) -> tuple[BaseException, ...]:
    if isinstance(error, BaseExceptionGroup):
        return tuple(leaf for nested in error.exceptions for leaf in _exception_leaves(nested))
    return (error,)


def _assert_identity_failure_group(error: BaseExceptionGroup) -> None:
    leaves = _exception_leaves(error)
    assert leaves
    assert all(isinstance(leaf, ValueError) for leaf in leaves)
    assert any(
        any(marker in str(leaf) for marker in ("parent", "path identity", "changed"))
        for leaf in leaves
    )


def _spec(
    *,
    parameter_fingerprint: str = _HASH_B,
    family: str = "n-shape-v1",
    seed: int = 7,
) -> ExperimentSpec:
    return ExperimentSpec(
        strategy_spec_fingerprint=_HASH_A,
        strategy_executable_fingerprint=_EXECUTABLE_HASH,
        candidate_schema_fingerprint=_CANDIDATE_SCHEMA_HASH,
        dataset_snapshot_id=_HASH_C,
        code_commit=_COMMIT,
        parameter_fingerprint=parameter_fingerprint,
        hypothesis_family=family,
        metric_definition_fingerprint=_HASH_E,
        train_range=DateRange(start_date=date(2024, 1, 2), end_date=date(2024, 12, 31)),
        validation_range=DateRange(start_date=date(2025, 1, 2), end_date=date(2025, 6, 30)),
        frozen_outer_test_range=DateRange(start_date=date(2025, 7, 1), end_date=date(2025, 12, 31)),
        cost_model_fingerprint=_HASH_D,
        execution_model_fingerprint=_HASH_F,
        seed=seed,
    )


def test_experiment_identity_binds_the_trusted_strategy_executable() -> None:
    first = _spec()
    changed = first.model_copy(
        update={
            "experiment_id": None,
            "strategy_executable_fingerprint": "2" * 64,
        }
    )

    rebound = ExperimentSpec.model_validate(changed.model_dump(mode="python"))

    assert rebound.experiment_id != first.experiment_id


def test_experiment_identity_binds_candidate_schema() -> None:
    first = _spec()
    changed = first.model_copy(
        update={
            "experiment_id": None,
            "candidate_schema_fingerprint": "3" * 64,
        }
    )

    rebound = ExperimentSpec.model_validate(changed.model_dump(mode="python"))

    assert rebound.experiment_id != first.experiment_id


def _manifest(
    specs: list[ExperimentSpec] | tuple[ExperimentSpec, ...],
    *,
    preregistered_at: datetime = _NOW - timedelta(minutes=1),
):
    return registry_module.HypothesisFamilyManifest(
        hypothesis_family=specs[0].hypothesis_family,
        experiment_ids=tuple(spec.experiment_id for spec in specs),
        search_space_fingerprint="1" * 64,
        metric_definition_fingerprint=_HASH_E,
        preregistered_at=preregistered_at,
    )


def _outer_evidence(
    spec: ExperimentSpec,
    *,
    available_at: datetime = _NOW + timedelta(minutes=1, seconds=30),
    net_return: str = "0.12",
    max_drawdown: str = "0.08",
    trade_count: int = 80,
):
    return registry_module.EvaluationArtifactEvidence(
        artifact_hash=_HASH_A,
        metric_definition_fingerprint=_HASH_E,
        evaluation_range=spec.frozen_outer_test_range,
        available_at=available_at,
        trade_count=trade_count,
        net_return=Decimal(net_return),
        max_drawdown=Decimal(max_drawdown),
    )


def _forward_evidence(
    *,
    available_at: datetime = _FORWARD_AVAILABLE,
    trading_days: int = 5,
    fills: int = 12,
    net_return: str = "0.03",
    max_drawdown: str = "0.06",
):
    return registry_module.ForwardArtifactEvidence(
        artifact_hash=_HASH_D,
        metric_definition_fingerprint=_HASH_E,
        observation_range=DateRange(
            start_date=date(2026, 8, 3),
            end_date=date(2026, 8, 14),
        ),
        available_at=available_at,
        trading_days=trading_days,
        fill_count=fills,
        net_return=Decimal(net_return),
        max_drawdown=Decimal(max_drawdown),
    )


def _outcome(
    spec: ExperimentSpec,
    *,
    attempted: int,
    rank: int,
    raw_p: str,
    net_return: str = "0.12",
    confidence_lower: str = "0.03",
    outer_complete: bool = True,
    trades: int = 80,
) -> ExperimentOutcome:
    evidence = (
        _outer_evidence(spec, net_return=net_return, trade_count=trades) if outer_complete else None
    )
    return ExperimentOutcome(
        experiment_id=spec.experiment_id,
        trade_count=trades,
        net_return=Decimal(net_return),
        max_drawdown=Decimal("0.08"),
        win_rate=Decimal("0.58"),
        confidence_lower=Decimal(confidence_lower),
        confidence_upper=Decimal("0.21"),
        attempted_configuration_count=attempted,
        selected_rank=rank,
        raw_p_value=Decimal(raw_p),
        artifact_hash=_HASH_A,
        outer_test_completed=outer_complete,
        outer_evidence=evidence,
    )


def _register_family(registry: ExperimentRegistry, specs: list[ExperimentSpec]) -> None:
    registry.register_hypothesis_family(_manifest(specs))


def _current_formal_plan(
    spec: ExperimentSpec,
    *,
    variant: str,
) -> FormalExperimentPlan:
    return FormalExperimentPlan(
        schema_version=2,
        spec=spec,
        hypothesis_variant=variant,
        strategy_definition_fingerprint="3" * 64,
        definition_registration_record_hash="4" * 64,
        preregistered_at=_NOW - timedelta(minutes=1),
    )


def _succeed(
    registry: ExperimentRegistry,
    spec: ExperimentSpec,
    outcome: ExperimentOutcome,
    *,
    offset: int = 0,
) -> ExperimentOutcome:
    registry.register_attempt(spec, registered_at=_NOW + timedelta(seconds=offset))
    registry.start_attempt(
        spec.experiment_id,
        started_at=_NOW + timedelta(minutes=1, seconds=offset),
    )
    return registry.record_success(
        outcome,
        completed_at=_NOW + timedelta(minutes=2, seconds=offset),
    )


def test_experiment_real_terminal_transition_invokes_artifact_outbox_hook(
    tmp_path: Path,
) -> None:
    events: list[tuple[str, str, datetime]] = []
    registry = ExperimentRegistry(
        tmp_path / "experiment-hook.sqlite3",
        managed_trust_root=tmp_path,
        artifact_terminal_hook=lambda owner_type, owner_id, observed_at: events.append(
            (owner_type, owner_id, observed_at)
        ),
    )
    spec = _spec()
    _register_family(registry, [spec])
    registry.register_attempt(spec, registered_at=_NOW)
    registry.start_attempt(spec.experiment_id, started_at=_NOW + timedelta(minutes=1))
    assert events == []
    completed_at = _NOW + timedelta(minutes=2)
    registry.record_failure(
        spec.experiment_id,
        first_error="bounded worker failure",
        completed_at=completed_at,
    )

    assert events == [("experiment", spec.experiment_id, completed_at)]


def test_family_must_be_preregistered_with_complete_immutable_search_space(tmp_path) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    specs = [_spec(parameter_fingerprint=value * 64) for value in ("1", "2")]

    with pytest.raises(IncompleteHypothesisFamilyError, match="preregister"):
        registry.register_attempt(specs[0], registered_at=_NOW)

    manifest = _manifest(specs)
    assert registry.register_hypothesis_family(manifest) == manifest
    assert registry.get_hypothesis_family("n-shape-v1") == manifest
    assert registry.register_hypothesis_family(manifest) == manifest

    changed = manifest.model_copy(update={"search_space_fingerprint": "9" * 64})
    with pytest.raises(ExperimentIdentityConflictError):
        registry.register_hypothesis_family(changed)

    unlisted = _spec(parameter_fingerprint="3" * 64)
    with pytest.raises(IncompleteHypothesisFamilyError, match="not preregistered"):
        registry.register_attempt(unlisted, registered_at=_NOW)


def test_register_attempt_atomically_prepares_recoverable_job_submission_outbox(
    tmp_path: Path,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    spec = _spec()
    plan = _current_formal_plan(spec, variant="baseline")
    registry.register_formal_plan(plan, family_manifest=_manifest([spec]))
    assert spec.experiment_id is not None
    intent = ExperimentSubmissionIntent(
        schema_version=2,
        request_id=UUID(int=1),
        job_id=UUID(int=2),
        experiment_id=spec.experiment_id,
        attempt_identity=registry_module.canonical_sha256(
            {
                "contract": "research-experiment-attempt/v1",
                "experiment_id": spec.experiment_id,
                "hypothesis_family": spec.hypothesis_family,
                "hypothesis_variant": "baseline",
            }
        ),
        hypothesis_variant="baseline",
        formal_plan_id=plan.plan_id,
        strategy_definition_fingerprint=plan.strategy_definition_fingerprint,
        definition_registration_record_hash=(plan.definition_registration_record_hash),
        command_content_hash="3" * 64,
        envelope_json='{"schema_version":1}',
        envelope_sha256=registry_module.canonical_sha256(
            {"canonical_envelope_json": '{"schema_version":1}'}
        ),
    )

    registered = registry.register_attempt(spec, registered_at=_NOW, submission=intent)
    repeated = registry.register_attempt(spec, registered_at=_NOW, submission=intent)
    pending = registry.list_pending_submissions(limit=10)

    assert registered == repeated
    assert pending == (intent,)
    registry.mark_submission_published(
        intent.request_id,
        command_content_hash=intent.command_content_hash,
        published_at=_NOW + timedelta(seconds=1),
    )
    assert registry.list_pending_submissions(limit=10) == ()


def test_execution_completion_is_a_distinct_immutable_terminal_state(tmp_path: Path) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    spec = _spec()
    _register_family(registry, [spec])
    assert spec.experiment_id is not None
    registry.register_attempt(spec, registered_at=_NOW)
    registry.ensure_attempt_started(
        spec.experiment_id,
        started_at=_NOW + timedelta(minutes=1),
    )

    completed = registry.record_execution_completed(
        spec.experiment_id,
        completed_at=_NOW + timedelta(minutes=2),
    )
    repeated = registry.record_execution_completed(
        spec.experiment_id,
        completed_at=_NOW + timedelta(minutes=2),
    )

    assert completed == repeated
    assert completed.status is ExperimentStatus.EXECUTED
    assert completed.completed_at == _NOW + timedelta(minutes=2)
    assert completed.first_error is None
    assert completed.outcome is None
    with pytest.raises(TerminalExperimentError, match="immutable|terminal"):
        registry.record_execution_completed(
            spec.experiment_id,
            completed_at=_NOW + timedelta(minutes=3),
        )


def test_lifecycle_recovery_ignores_terminal_history_before_applying_its_budget(
    tmp_path: Path,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    completed_spec = _spec(parameter_fingerprint="1" * 64)
    active_spec = _spec(parameter_fingerprint="2" * 64)
    manifest = _manifest([completed_spec, active_spec])
    plans = {
        1: _current_formal_plan(completed_spec, variant="variant-1"),
        2: _current_formal_plan(active_spec, variant="variant-2"),
    }
    for plan in plans.values():
        registry.register_formal_plan(plan, family_manifest=manifest)

    def intent(spec: ExperimentSpec, identity: int) -> ExperimentSubmissionIntent:
        assert spec.experiment_id is not None
        variant = f"variant-{identity}"
        envelope_json = f'{{"identity":{identity}}}'
        return ExperimentSubmissionIntent(
            schema_version=2,
            request_id=UUID(int=identity),
            job_id=UUID(int=identity + 10),
            experiment_id=spec.experiment_id,
            attempt_identity=registry_module.canonical_sha256(
                {
                    "contract": "research-experiment-attempt/v1",
                    "experiment_id": spec.experiment_id,
                    "hypothesis_family": spec.hypothesis_family,
                    "hypothesis_variant": variant,
                }
            ),
            hypothesis_variant=variant,
            formal_plan_id=plans[identity].plan_id,
            strategy_definition_fingerprint=(plans[identity].strategy_definition_fingerprint),
            definition_registration_record_hash=(
                plans[identity].definition_registration_record_hash
            ),
            command_content_hash=f"{identity}" * 64,
            envelope_json=envelope_json,
            envelope_sha256=registry_module.canonical_sha256(
                {"canonical_envelope_json": envelope_json}
            ),
        )

    completed_intent = intent(completed_spec, 1)
    active_intent = intent(active_spec, 2)
    registry.register_attempt(
        completed_spec,
        registered_at=_NOW,
        submission=completed_intent,
    )
    registry.mark_submission_published(
        completed_intent.request_id,
        command_content_hash=completed_intent.command_content_hash,
        published_at=_NOW + timedelta(seconds=1),
    )
    assert completed_spec.experiment_id is not None
    registry.ensure_attempt_started(
        completed_spec.experiment_id,
        started_at=_NOW + timedelta(seconds=2),
    )
    registry.record_execution_completed(
        completed_spec.experiment_id,
        completed_at=_NOW + timedelta(seconds=3),
    )
    registry.register_attempt(
        active_spec,
        registered_at=_NOW + timedelta(seconds=4),
        submission=active_intent,
    )

    assert registry.list_recoverable_submission_intents(limit=1) == (active_intent,)


def test_formal_plan_resolution_requires_one_exact_preregistered_identity(
    tmp_path: Path,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    spec = _spec()
    manifest = _manifest([spec])
    plan = FormalExperimentPlan(
        spec=spec,
        hypothesis_variant="baseline",
        preregistered_at=manifest.preregistered_at,
    )
    registry.register_formal_plan(plan, family_manifest=manifest)

    resolved = registry.resolve_formal_plan(
        strategy_spec_fingerprint=spec.strategy_spec_fingerprint,
        strategy_executable_fingerprint=spec.strategy_executable_fingerprint,
        candidate_schema_fingerprint=spec.candidate_schema_fingerprint,
        dataset_snapshot_id=spec.dataset_snapshot_id,
        code_commit=spec.code_commit,
        parameter_fingerprint=spec.parameter_fingerprint,
        cost_model_fingerprint=spec.cost_model_fingerprint,
        execution_model_fingerprint=spec.execution_model_fingerprint,
        seed=spec.seed,
        as_of=_NOW,
    )

    assert resolved == plan
    assert registry.register_formal_plan(plan, family_manifest=manifest) == plan
    with pytest.raises(IncompleteHypothesisFamilyError, match="preregistered formal plan"):
        registry.resolve_formal_plan(
            strategy_spec_fingerprint=spec.strategy_spec_fingerprint,
            strategy_executable_fingerprint=spec.strategy_executable_fingerprint,
            candidate_schema_fingerprint=spec.candidate_schema_fingerprint,
            dataset_snapshot_id="9" * 64,
            code_commit=spec.code_commit,
            parameter_fingerprint=spec.parameter_fingerprint,
            cost_model_fingerprint=spec.cost_model_fingerprint,
            execution_model_fingerprint=spec.execution_model_fingerprint,
            seed=spec.seed,
            as_of=_NOW,
        )


def test_readonly_registry_resolves_formal_plan_without_write_capability(
    tmp_path: Path,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    spec = _spec()
    manifest = _manifest([spec])
    plan = FormalExperimentPlan(
        spec=spec,
        hypothesis_variant="baseline",
        preregistered_at=manifest.preregistered_at,
    )
    registry.register_formal_plan(plan, family_manifest=manifest)

    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)

    assert (
        reader.resolve_formal_plan(
            strategy_spec_fingerprint=spec.strategy_spec_fingerprint,
            strategy_executable_fingerprint=spec.strategy_executable_fingerprint,
            candidate_schema_fingerprint=spec.candidate_schema_fingerprint,
            dataset_snapshot_id=spec.dataset_snapshot_id,
            code_commit=spec.code_commit,
            parameter_fingerprint=spec.parameter_fingerprint,
            cost_model_fingerprint=spec.cost_model_fingerprint,
            execution_model_fingerprint=spec.execution_model_fingerprint,
            seed=spec.seed,
            as_of=_NOW,
        )
        == plan
    )
    assert reader.resolve_formal_plan_by_id(plan.plan_id, as_of=_NOW) == plan
    assert not hasattr(reader, "register_formal_plan")


def test_manifest_identity_covers_metric_search_space_and_exact_experiment_ids() -> None:
    specs = [_spec(parameter_fingerprint=value * 64) for value in ("1", "2")]
    first = _manifest(specs)
    reordered = _manifest(list(reversed(specs)))

    assert first.experiment_ids == tuple(sorted(spec.experiment_id for spec in specs))
    assert first.manifest_id == reordered.manifest_id
    assert first.hypothesis_count == 2
    assert len(first.manifest_id) == 64
    with pytest.raises(ValidationError, match="experiment"):
        registry_module.HypothesisFamilyManifest(
            hypothesis_family="n-shape-v1",
            experiment_ids=(specs[0].experiment_id, specs[0].experiment_id),
            search_space_fingerprint="1" * 64,
            metric_definition_fingerprint=_HASH_E,
            preregistered_at=_NOW,
        )


def test_outcome_requires_outer_evidence_bound_to_frozen_range_metric_and_artifact() -> None:
    spec = _spec()
    with pytest.raises(ValidationError, match="outer_evidence"):
        ExperimentOutcome(
            **{
                **_outcome(spec, attempted=1, rank=1, raw_p="0.01").model_dump(),
                "outer_evidence": None,
            }
        )


def test_registry_rejects_outer_evidence_mismatch_or_future_availability(tmp_path) -> None:
    spec = _spec()
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    _register_family(registry, [spec])
    registry.register_attempt(spec, registered_at=_NOW)
    registry.start_attempt(spec.experiment_id, started_at=_NOW + timedelta(minutes=1))

    wrong_range = _outer_evidence(spec).model_copy(
        update={
            "evaluation_range": DateRange(start_date=date(2025, 8, 1), end_date=date(2025, 12, 31))
        }
    )
    bad_range = _outcome(spec, attempted=1, rank=1, raw_p="0.01").model_copy(
        update={"outer_evidence": wrong_range}
    )
    with pytest.raises(ExperimentRegistryError, match="outer.*range"):
        registry.record_success(bad_range, completed_at=_NOW + timedelta(minutes=2))

    future = _outer_evidence(spec, available_at=_NOW + timedelta(minutes=3))
    future_outcome = _outcome(spec, attempted=1, rank=1, raw_p="0.01").model_copy(
        update={"outer_evidence": future}
    )
    with pytest.raises(ExperimentRegistryError, match="available"):
        registry.record_success(future_outcome, completed_at=_NOW + timedelta(minutes=2))


def test_result_count_is_taken_from_manifest_not_declared_after_search(tmp_path) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    specs = [_spec(parameter_fingerprint=value * 64) for value in ("1", "2", "3")]
    _register_family(registry, specs)

    with pytest.raises(IncompleteHypothesisFamilyError, match="manifest"):
        _succeed(registry, specs[0], _outcome(specs[0], attempted=1, rank=1, raw_p="0.01"))


def test_bh_adjustment_uses_preregistered_family_and_waits_for_every_attempt(tmp_path) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    specs = [_spec(parameter_fingerprint=value * 64) for value in ("1", "2", "3", "4")]
    _register_family(registry, specs)
    for index, (spec, raw_p) in enumerate(
        zip(specs, ("0.01", "0.01", "0.03", "0.20"), strict=True)
    ):
        _succeed(
            registry,
            spec,
            _outcome(spec, attempted=4, rank=index + 1, raw_p=raw_p),
            offset=index,
        )

    adjusted = registry.adjust_hypothesis_family(
        "n-shape-v1", adjusted_at=_NOW + timedelta(hours=1)
    )
    by_id = {item.experiment_id: item for item in adjusted}
    assert by_id[specs[0].experiment_id].adjusted_p_value == Decimal("0.02")
    assert by_id[specs[1].experiment_id].adjusted_p_value == Decimal("0.02")
    assert by_id[specs[2].experiment_id].adjusted_p_value == Decimal("0.04")
    assert by_id[specs[3].experiment_id].adjusted_p_value == Decimal("0.20")


def test_adjustment_and_promotion_timestamps_cannot_be_backdated(tmp_path) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
        minimum_comparable_trades=10,
    )
    spec = _spec()
    _register_family(registry, [spec])
    _succeed(registry, spec, _outcome(spec, attempted=1, rank=1, raw_p="0.01"))

    with pytest.raises(ValueError, match="adjusted_at"):
        registry.adjust_hypothesis_family(
            "n-shape-v1", adjusted_at=_NOW + timedelta(minutes=1, seconds=30)
        )
    registry.adjust_hypothesis_family("n-shape-v1", adjusted_at=_NOW + timedelta(hours=1))
    with pytest.raises(ValueError, match="decided_at"):
        registry.evaluate_promotion(
            PromotionStage.COMPARABLE,
            experiment_ids=(spec.experiment_id,),
            evidence_artifact_hash=_HASH_A,
            decided_at=_NOW + timedelta(minutes=1),
        )

    assert registry.evaluate_promotion(
        PromotionStage.COMPARABLE,
        experiment_ids=(spec.experiment_id,),
        evidence_artifact_hash=_HASH_A,
        decided_at=_NOW + timedelta(hours=2),
    ).approved
    assert registry.evaluate_promotion(
        PromotionStage.PAPER_CANDIDATE,
        experiment_ids=(spec.experiment_id,),
        evidence_artifact_hash=_HASH_B,
        decided_at=_NOW + timedelta(hours=3),
    ).approved
    with pytest.raises(ValueError, match="decided_at"):
        registry.evaluate_promotion(
            PromotionStage.MONITOR_APPROVED,
            experiment_ids=(spec.experiment_id,),
            evidence_artifact_hash=_HASH_D,
            decided_at=_NOW + timedelta(hours=2, minutes=30),
        )


def test_monitor_requires_immutable_profitable_forward_artifact_within_risk_budget(
    tmp_path,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
        minimum_comparable_trades=10,
        minimum_forward_days=5,
        minimum_forward_fills=12,
        maximum_forward_drawdown=Decimal("0.10"),
    )
    spec = _spec()
    _register_family(registry, [spec])
    _succeed(registry, spec, _outcome(spec, attempted=1, rank=1, raw_p="0.01"))
    registry.adjust_hypothesis_family("n-shape-v1", adjusted_at=_NOW + timedelta(hours=1))
    ids = (spec.experiment_id,)
    registry.evaluate_promotion(
        PromotionStage.COMPARABLE,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_A,
        decided_at=_NOW + timedelta(hours=2),
    )
    registry.evaluate_promotion(
        PromotionStage.PAPER_CANDIDATE,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_B,
        decided_at=_NOW + timedelta(hours=3),
    )

    missing = registry.evaluate_promotion(
        PromotionStage.MONITOR_APPROVED,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_D,
        decided_at=_FORWARD_AVAILABLE + timedelta(hours=1),
    )
    losing = registry.evaluate_promotion(
        PromotionStage.MONITOR_APPROVED,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_D,
        decided_at=_FORWARD_AVAILABLE + timedelta(hours=2),
        forward_evidence=_forward_evidence(net_return="-0.01"),
    )
    risky = registry.evaluate_promotion(
        PromotionStage.MONITOR_APPROVED,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_D,
        decided_at=_FORWARD_AVAILABLE + timedelta(hours=3),
        forward_evidence=_forward_evidence(max_drawdown="0.11"),
    )
    approved = registry.evaluate_promotion(
        PromotionStage.MONITOR_APPROVED,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_D,
        decided_at=_FORWARD_AVAILABLE + timedelta(hours=4),
        forward_evidence=_forward_evidence(),
    )

    assert "forward_evidence_missing" in missing.gate_failures
    assert "non_positive_forward_return" in losing.gate_failures
    assert "forward_drawdown_budget_exceeded" in risky.gate_failures
    assert approved.approved
    assert approved.forward_evidence_artifact_hash == _HASH_D
    assert approved.forward_net_return == Decimal("0.03")


def test_forward_evidence_must_match_metric_artifact_and_be_visible_at_decision(tmp_path) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
        minimum_comparable_trades=10,
        minimum_forward_days=1,
        minimum_forward_fills=1,
    )
    spec = _spec()
    _register_family(registry, [spec])
    _succeed(registry, spec, _outcome(spec, attempted=1, rank=1, raw_p="0.01"))
    registry.adjust_hypothesis_family("n-shape-v1", adjusted_at=_NOW + timedelta(hours=1))
    ids = (spec.experiment_id,)
    for stage, hour, artifact in (
        (PromotionStage.COMPARABLE, 2, _HASH_A),
        (PromotionStage.PAPER_CANDIDATE, 3, _HASH_B),
    ):
        assert registry.evaluate_promotion(
            stage,
            experiment_ids=ids,
            evidence_artifact_hash=artifact,
            decided_at=_NOW + timedelta(hours=hour),
        ).approved

    future = _forward_evidence(available_at=_FORWARD_AVAILABLE + timedelta(hours=5))
    with pytest.raises(ValueError, match="available"):
        registry.evaluate_promotion(
            PromotionStage.MONITOR_APPROVED,
            experiment_ids=ids,
            evidence_artifact_hash=_HASH_D,
            decided_at=_FORWARD_AVAILABLE + timedelta(hours=4),
            forward_evidence=future,
        )

    wrong_metric = _forward_evidence().model_copy(
        update={"metric_definition_fingerprint": "9" * 64}
    )
    with pytest.raises(ExperimentRegistryError, match="metric"):
        registry.evaluate_promotion(
            PromotionStage.MONITOR_APPROVED,
            experiment_ids=ids,
            evidence_artifact_hash=_HASH_D,
            decided_at=_FORWARD_AVAILABLE + timedelta(hours=5),
            forward_evidence=wrong_metric,
        )


def test_forward_observation_must_start_after_paper_candidate_selection(tmp_path) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
        minimum_comparable_trades=10,
        minimum_forward_days=1,
        minimum_forward_fills=1,
    )
    spec = _spec()
    _register_family(registry, [spec])
    _succeed(registry, spec, _outcome(spec, attempted=1, rank=1, raw_p="0.01"))
    registry.adjust_hypothesis_family("n-shape-v1", adjusted_at=_NOW + timedelta(hours=1))
    ids = (spec.experiment_id,)
    registry.evaluate_promotion(
        PromotionStage.COMPARABLE,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_A,
        decided_at=_NOW + timedelta(hours=2),
    )
    registry.evaluate_promotion(
        PromotionStage.PAPER_CANDIDATE,
        experiment_ids=ids,
        evidence_artifact_hash=_HASH_B,
        decided_at=_NOW + timedelta(hours=3),
    )
    preselected_history = _forward_evidence().model_copy(
        update={
            "observation_range": DateRange(
                start_date=date(2026, 7, 1),
                end_date=date(2026, 7, 30),
            )
        }
    )

    with pytest.raises(ExperimentRegistryError, match="after paper candidate"):
        registry.evaluate_promotion(
            PromotionStage.MONITOR_APPROVED,
            experiment_ids=ids,
            evidence_artifact_hash=_HASH_D,
            decided_at=_FORWARD_AVAILABLE + timedelta(hours=1),
            forward_evidence=preselected_history,
        )


def test_artifact_availability_cannot_predate_its_observation_range() -> None:
    with pytest.raises(ValidationError, match="observation range"):
        _forward_evidence(available_at=datetime(2026, 8, 13, 8, 0, tzinfo=UTC))


def test_promotion_policy_is_fingerprinted_and_database_rejects_drift(tmp_path) -> None:
    path = tmp_path / "experiments.sqlite3"
    first = ExperimentRegistry(
        path,
        managed_trust_root=tmp_path,
        maximum_forward_drawdown=Decimal("0.10"),
    )
    reopened = ExperimentRegistry(
        path,
        managed_trust_root=tmp_path,
        maximum_forward_drawdown=Decimal("0.10"),
    )

    assert first.policy.policy_fingerprint == reopened.policy.policy_fingerprint
    with pytest.raises(ExperimentRegistryError, match="policy"):
        ExperimentRegistry(
            path,
            managed_trust_root=tmp_path,
            maximum_forward_drawdown=Decimal("0.20"),
        )


def test_registry_uses_wal_and_terminal_evidence_remains_immutable(tmp_path) -> None:
    path = tmp_path / "experiments.sqlite3"
    spec = _spec()
    outcome = _outcome(spec, attempted=1, rank=1, raw_p="0.01")
    first = ExperimentRegistry(path, managed_trust_root=tmp_path)
    _register_family(first, [spec])

    original = _succeed(first, spec, outcome)
    reopened = ExperimentRegistry(path, managed_trust_root=tmp_path)
    assert reopened.record_success(outcome, completed_at=_NOW + timedelta(minutes=2)) == original
    with pytest.raises(TerminalExperimentError):
        reopened.record_success(
            outcome.model_copy(update={"raw_p_value": Decimal("0.02")}),
            completed_at=_NOW + timedelta(minutes=2),
        )

    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        manifest_count = connection.execute(
            "SELECT COUNT(*) FROM hypothesis_family_manifest"
        ).fetchone()[0]
        assert manifest_count == 1


def _promotion_decision(*, decided_at: datetime, marker: str) -> PromotionDecision:
    return PromotionDecision(
        stage=PromotionStage.EXPLORATORY,
        experiment_ids=(marker * 64,),
        evidence_artifact_hash=marker * 64,
        decided_at=decided_at,
        approved=True,
        minimum_trade_count=30,
        significance_level=Decimal("0.05"),
        forward_trading_days=0,
        forward_fills=0,
        minimum_forward_days=10,
        minimum_forward_fills=20,
        maximum_forward_drawdown=Decimal("0.10"),
        policy_fingerprint=(marker.upper() if marker != "a" else "9") * 64,
    )


def _insert_promotion_decision(path: Path, decision: PromotionDecision) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO promotion_decision(
                decision_id, stage, approved, decided_at, payload_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                decision.decision_id,
                decision.stage.value,
                int(decision.approved),
                registry_module._utc_iso(decision.decided_at),
                registry_module._json_payload(decision),
            ),
        )


def test_readonly_reader_does_not_initialize_missing_database_or_mutate_registry(
    tmp_path: Path,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    missing = private / "experiments.sqlite3"
    reader = ExperimentRegistryReadonlyReader(missing, managed_trust_root=tmp_path)

    with pytest.raises(ExperimentRegistryError, match="does not exist"):
        reader.list_promotion_decisions(observed_at=_NOW)
    assert not missing.exists()

    path = private / "existing.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    path.chmod(0o600)
    with sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True) as connection:
        schema_version = connection.execute("PRAGMA schema_version").fetchone()[0]
    before = {
        item.name: (item.stat().st_size, item.stat().st_mtime_ns) for item in path.parent.iterdir()
    }

    assert (
        ExperimentRegistryReadonlyReader(
            path,
            managed_trust_root=tmp_path,
        ).list_promotion_decisions(observed_at=_NOW)
        == ()
    )

    with sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True) as connection:
        assert connection.execute("PRAGMA schema_version").fetchone()[0] == schema_version
    after = {
        item.name: (item.stat().st_size, item.stat().st_mtime_ns) for item in path.parent.iterdir()
    }
    assert after == before


def test_readonly_reader_requires_explicit_managed_trust_root(tmp_path: Path) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)

    with pytest.raises(TypeError, match="managed_trust_root"):
        ExperimentRegistryReadonlyReader(path)


def test_readonly_reader_retries_only_transient_managed_root_ctime_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    actual_authority = registry_module.PrivateSqlitePathAuthority
    calls = 0

    def transient_authority(
        authority_path: Path,
        *,
        label: str,
        create_if_missing: bool,
        managed_trust_root: Path,
        create_parent_if_missing: bool = False,
    ) -> artifact_retention_module.PrivateSqlitePathAuthority:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("experiment registry managed trust root identity changed")
        return actual_authority(
            authority_path,
            label=label,
            create_if_missing=create_if_missing,
            managed_trust_root=managed_trust_root,
            create_parent_if_missing=create_parent_if_missing,
        )

    monkeypatch.setattr(
        registry_module,
        "PrivateSqlitePathAuthority",
        transient_authority,
    )

    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)

    assert calls == 2
    assert reader.list_promotion_decisions(observed_at=_NOW) == ()


def test_readonly_reader_verifies_identity_after_native_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    original_connect = registry_module.sqlite3.connect
    opened: list[sqlite3.Connection] = []
    closed: set[int] = set()

    class TrackingConnection(sqlite3.Connection):
        def close(self) -> None:
            super().close()
            closed.add(id(self))

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = TrackingConnection
        connection = original_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    original_assert_current = reader._path_authority.assert_current

    def fail_after_close() -> None:
        original_assert_current()
        if opened and id(opened[-1]) in closed:
            raise ValueError("readonly identity changed after close")

    monkeypatch.setattr(registry_module.sqlite3, "connect", connect)
    monkeypatch.setattr(reader._path_authority, "assert_current", fail_after_close)

    with (
        pytest.raises(ValueError, match="readonly identity changed after close"),
        reader._read_snapshot(),
    ):
        pass


def test_readonly_reader_preserves_close_and_postcheck_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    original_connect = registry_module.sqlite3.connect
    opened: list[sqlite3.Connection] = []
    closed: set[int] = set()
    close_error = OSError("readonly connection close failed")
    postcheck_error = ValueError("readonly identity changed after close")

    class FailingCloseConnection(sqlite3.Connection):
        def close(self) -> None:
            super().close()
            closed.add(id(self))
            raise close_error

    def connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        kwargs["factory"] = FailingCloseConnection
        connection = original_connect(*args, **kwargs)
        opened.append(connection)
        return connection

    original_assert_current = reader._path_authority.assert_current

    def fail_after_close() -> None:
        original_assert_current()
        if opened and id(opened[-1]) in closed:
            raise postcheck_error

    monkeypatch.setattr(registry_module.sqlite3, "connect", connect)
    monkeypatch.setattr(reader._path_authority, "assert_current", fail_after_close)

    with pytest.raises(BaseExceptionGroup) as captured, reader._read_snapshot():
        pass

    assert captured.value.exceptions == (close_error, postcheck_error)


def test_readonly_reader_applies_pit_cutoff_and_deterministic_latest_limit(
    tmp_path: Path,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    decisions = tuple(
        _promotion_decision(decided_at=_NOW + timedelta(minutes=index), marker=marker)
        for index, marker in enumerate(("1", "2", "3", "4"), start=1)
    )
    for decision in decisions:
        _insert_promotion_decision(path, decision)
    path.chmod(0o600)

    snapshot = ExperimentRegistryReadonlyReader(
        path,
        managed_trust_root=tmp_path,
    ).read_promotion_decisions(
        observed_at=_NOW + timedelta(minutes=3),
        limit=2,
    )

    assert snapshot.decisions == decisions[1:3]
    assert snapshot.sequence == 3
    assert snapshot.event_time == decisions[2].decided_at
    assert (
        ExperimentRegistryReadonlyReader(
            path,
            managed_trust_root=tmp_path,
        ).list_promotion_decisions(
            observed_at=_NOW + timedelta(minutes=3),
            limit=2,
        )
        == decisions[1:3]
    )


def test_readonly_reader_rejects_denormalized_or_future_promotion_evidence(
    tmp_path: Path,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    future = _promotion_decision(
        decided_at=_NOW + timedelta(minutes=2),
        marker="5",
    )
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            INSERT INTO promotion_decision(
                decision_id, stage, approved, decided_at, payload_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                future.decision_id,
                future.stage.value,
                int(future.approved),
                registry_module._utc_iso(_NOW),
                registry_module._json_payload(future),
            ),
        )
    path.chmod(0o600)

    with pytest.raises(ExperimentRegistryError, match="future|does not match"):
        ExperimentRegistryReadonlyReader(
            path,
            managed_trust_root=tmp_path,
        ).read_promotion_decisions(
            observed_at=_NOW,
            limit=10,
        )


@pytest.mark.parametrize(
    "hazard",
    [
        "relative",
        "non-normalized",
        "parent-symlink",
        "parent-mode",
        "final-symlink",
        "hardlink",
        "mode",
        "not-0600",
    ],
)
def test_readonly_reader_rejects_unsafe_registry_path(tmp_path: Path, hazard: str) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    path.chmod(0o600)

    if hazard == "relative":
        unsafe = Path("relative/experiments.sqlite3")
    elif hazard == "non-normalized":
        unsafe = Path(f"{private}/../private/experiments.sqlite3")
    elif hazard == "parent-symlink":
        linked = tmp_path / "linked"
        linked.symlink_to(private, target_is_directory=True)
        unsafe = linked / path.name
    elif hazard == "parent-mode":
        private.chmod(0o755)
        unsafe = path
    elif hazard == "final-symlink":
        unsafe = private / "linked.sqlite3"
        unsafe.symlink_to(path)
    elif hazard == "hardlink":
        unsafe = private / "hardlinked.sqlite3"
        os.link(path, unsafe)
    elif hazard == "mode":
        path.chmod(0o640)
        unsafe = path
    else:
        path.chmod(0o400)
        unsafe = path

    with pytest.raises(
        (ValueError, ExperimentRegistryError), match="absolute|symlink|hard link|mode"
    ):
        ExperimentRegistryReadonlyReader(unsafe, managed_trust_root=tmp_path)


def test_readonly_reader_rejects_registry_not_owned_by_current_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    path.chmod(0o600)
    monkeypatch.setattr(
        artifact_retention_module.os,
        "geteuid",
        lambda: path.stat().st_uid + 1,
    )

    with pytest.raises(ValueError, match="owner"):
        ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)


def test_readonly_reader_rejects_private_parent_generation_swap(tmp_path: Path) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    path.chmod(0o600)
    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    retired = tmp_path / "retired"

    private.rename(retired)
    private.mkdir(mode=0o700)
    (retired / path.name).rename(path)

    with pytest.raises(ExperimentRegistryError, match="path|identity|changed"):
        reader.list_promotion_decisions(observed_at=_NOW)


def test_readonly_reader_revalidates_parent_identity_immediately_after_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    path.chmod(0o600)
    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    original_connect = registry_module.sqlite3.connect
    retired = tmp_path / "retired"

    def swap_after_connect(*args: object, **kwargs: object):
        connection = original_connect(*args, **kwargs)
        private.rename(retired)
        private.mkdir(mode=0o700)
        (retired / path.name).rename(path)
        return connection

    monkeypatch.setattr(registry_module.sqlite3, "connect", swap_after_connect)

    with pytest.raises(BaseExceptionGroup) as captured:
        reader.list_promotion_decisions(observed_at=_NOW)
    _assert_identity_failure_group(captured.value)


@pytest.mark.parametrize(
    "hazard",
    [
        "relative",
        "non-normalized",
        "parent-symlink",
        "final-symlink",
        "hardlink",
        "file-mode",
        "parent-mode",
    ],
)
def test_writable_registry_rejects_unsafe_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hazard: str,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    if hazard == "relative":
        monkeypatch.chdir(tmp_path)
        unsafe = Path("relative/experiments.sqlite3")
    elif hazard == "non-normalized":
        (private / "nested").mkdir(mode=0o700)
        unsafe = Path(f"{private}/nested/../experiments.sqlite3")
    elif hazard == "parent-symlink":
        linked = tmp_path / "linked"
        linked.symlink_to(private, target_is_directory=True)
        unsafe = linked / path.name
    elif hazard in {"final-symlink", "hardlink"}:
        seed = private / "seed.sqlite3"
        seed.touch(mode=0o600)
        unsafe = path
        if hazard == "final-symlink":
            unsafe.symlink_to(seed)
        else:
            os.link(seed, unsafe)
    elif hazard == "file-mode":
        path.touch(mode=0o600)
        path.chmod(0o644)
        unsafe = path
    else:
        private.chmod(0o755)
        unsafe = path

    with pytest.raises(ValueError, match="absolute|symlink|hard link|mode|unsafe"):
        ExperimentRegistry(unsafe, managed_trust_root=tmp_path)


def test_writable_registry_requires_current_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    path.touch(mode=0o600)
    current_uid = os.geteuid()
    monkeypatch.setattr(artifact_retention_module.os, "geteuid", lambda: current_uid + 1)

    with pytest.raises(ValueError, match="owner"):
        ExperimentRegistry(path, managed_trust_root=tmp_path)


def test_writable_registry_safely_creates_missing_private_parent(tmp_path: Path) -> None:
    parent = tmp_path / "research"
    path = parent / "experiments.sqlite3"

    ExperimentRegistry(path, managed_trust_root=tmp_path)

    assert parent.stat().st_mode & 0o777 == 0o700
    assert path.stat().st_mode & 0o777 == 0o600


def test_writable_registry_durably_syncs_created_directories_and_initialized_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    managed_root = tmp_path / "managed"
    managed_root.mkdir(mode=0o700)
    parent = managed_root / "nested"
    path = parent / "experiments.sqlite3"
    real_fsync = os.fsync
    synced: list[tuple[int, int, int]] = []

    def record_fsync(descriptor: int) -> None:
        observed = os.fstat(descriptor)
        synced.append((observed.st_dev, observed.st_ino, observed.st_mode))
        real_fsync(descriptor)

    monkeypatch.setattr(artifact_retention_module.os, "fsync", record_fsync)

    ExperimentRegistry(path, managed_trust_root=managed_root)

    synced_generations = {(dev, ino) for dev, ino, _mode in synced}
    assert (managed_root.stat().st_dev, managed_root.stat().st_ino) in synced_generations
    assert (parent.stat().st_dev, parent.stat().st_ino) in synced_generations
    assert (path.stat().st_dev, path.stat().st_ino) in synced_generations


def test_writable_registry_rejects_non_private_intermediate_managed_parent(
    tmp_path: Path,
) -> None:
    managed_root = tmp_path / "managed"
    managed_root.mkdir(mode=0o700)
    shared = managed_root / "shared"
    shared.mkdir(mode=0o755)
    private = shared / "private"
    private.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="parent owner or mode is unsafe"):
        ExperimentRegistry(
            private / "experiments.sqlite3",
            managed_trust_root=managed_root,
        )


def test_writable_registry_checks_every_descendant_of_explicit_trust_root(
    tmp_path: Path,
) -> None:
    managed_root = tmp_path / "managed"
    managed_root.mkdir(mode=0o700)
    unsafe = managed_root / "unsafe"
    unsafe.mkdir(mode=0o755)
    private = unsafe / "private"
    private.mkdir(mode=0o700)

    with pytest.raises(ValueError, match="parent owner or mode is unsafe"):
        ExperimentRegistry(
            private / "experiments.sqlite3",
            managed_trust_root=managed_root,
        )


def test_writable_connection_revalidates_executemany_and_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    authority = registry._path_authority
    original_assert_current = authority.assert_current
    calls = 0

    def record_assert_current() -> None:
        nonlocal calls
        calls += 1
        original_assert_current()

    monkeypatch.setattr(authority, "assert_current", record_assert_current)
    connection = registry._connect()
    try:
        connection.execute("CREATE TEMP TABLE identity_probe(value INTEGER NOT NULL)")
        calls = 0
        connection.executemany(
            "INSERT INTO identity_probe(value) VALUES (?)",
            ((1,), (2,)),
        )
        assert calls == 2

        calls = 0
        connection.rollback()
        assert calls == 2
    finally:
        connection.close()


def test_writable_connection_revalidates_identity_after_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    connection = registry._connect()
    authority = registry._path_authority
    calls = 0
    underlying_closed = False
    original_close = connection._close_underlying

    def fail_after_close() -> None:
        nonlocal calls
        calls += 1
        if underlying_closed:
            raise ValueError("post-close identity changed")

    def mark_underlying_closed() -> None:
        nonlocal underlying_closed
        original_close()
        underlying_closed = True

    monkeypatch.setattr(authority, "assert_current", fail_after_close)
    monkeypatch.setattr(connection, "_close_underlying", mark_underlying_closed)

    with pytest.raises(ValueError, match="post-close identity changed"):
        connection.close()

    assert calls == 2
    assert underlying_closed


def test_writable_connection_preserves_close_error_with_postcheck_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    connection = registry._connect()
    authority = registry._path_authority
    calls = 0
    close_error = OSError("database close failed")
    postcheck_error = ValueError("post-close identity changed")

    def fail_after_close() -> None:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise postcheck_error

    def fail_close(_connection: registry_module._ExperimentRegistryConnection) -> None:
        raise close_error

    monkeypatch.setattr(authority, "assert_current", fail_after_close)
    monkeypatch.setattr(
        registry_module._ExperimentRegistryConnection,
        "_close_underlying",
        fail_close,
        raising=False,
    )

    with pytest.raises(BaseExceptionGroup) as captured:
        connection.close()

    assert captured.value.exceptions == (close_error, postcheck_error)
    assert calls == 2


def test_writable_connection_preserves_preclose_and_postclose_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    connection = registry._connect()
    precheck_error = ValueError("pre-close identity changed")
    postcheck_error = ValueError("post-close identity changed")
    calls = 0

    def fail_both_checks() -> None:
        nonlocal calls
        calls += 1
        raise precheck_error if calls == 1 else postcheck_error

    monkeypatch.setattr(registry._path_authority, "assert_current", fail_both_checks)

    with pytest.raises(BaseExceptionGroup) as captured:
        connection.close()

    assert captured.value.exceptions == (precheck_error, postcheck_error)
    assert calls == 2


def test_writable_connection_preserves_business_error_and_all_close_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    connection = registry._connect()
    business_error = RuntimeError("registry business failed")
    precheck_error = ValueError("pre-close identity changed")
    close_error = OSError("registry close failed")
    postcheck_error = ValueError("post-close identity changed")
    original_close = connection._close_underlying
    underlying_closed = False

    def fail_checks() -> None:
        raise postcheck_error if underlying_closed else precheck_error

    def close_then_fail() -> None:
        nonlocal underlying_closed
        original_close()
        underlying_closed = True
        raise close_error

    monkeypatch.setattr(registry._path_authority, "assert_current", fail_checks)
    monkeypatch.setattr(connection, "_close_underlying", close_then_fail)

    with pytest.raises(BaseExceptionGroup) as captured:
        connection.close(primary_error=business_error)

    assert captured.value.exceptions == (
        business_error,
        precheck_error,
        close_error,
        postcheck_error,
    )


def test_writable_registry_connect_closes_when_row_factory_setup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    setup_error = RuntimeError("registry row factory failed")

    class FailingSetupConnection:
        def __init__(self) -> None:
            self.closed = False

        @property
        def row_factory(self) -> object | None:
            return None

        @row_factory.setter
        def row_factory(self, _value: object) -> None:
            raise setup_error

        def close(self) -> None:
            self.closed = True

    connection = FailingSetupConnection()
    monkeypatch.setattr(
        registry._path_authority,
        "open_verified_connection",
        lambda _opener: connection,
    )

    with pytest.raises(RuntimeError, match="row factory"):
        registry._connect()

    assert connection.closed


def test_writable_connection_exit_routes_business_error_into_verified_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = ExperimentRegistry(
        tmp_path / "experiments.sqlite3",
        managed_trust_root=tmp_path,
    )
    connection = registry._connect()
    business_error = RuntimeError("transaction body failed")
    observed_primary: list[BaseException | None] = []

    def record_close(
        candidate: registry_module._ExperimentRegistryConnection,
        *,
        primary_error: BaseException | None = None,
    ) -> None:
        observed_primary.append(primary_error)
        candidate._close_underlying()

    monkeypatch.setattr(
        registry_module._ExperimentRegistryConnection,
        "rollback",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        registry_module._ExperimentRegistryConnection,
        "close",
        record_close,
    )

    assert connection.__exit__(RuntimeError, business_error, None) is False
    assert observed_primary == [business_error]


def test_readonly_reader_routes_business_error_into_verified_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    reader = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    business_error = RuntimeError("readonly consumer failed")
    observed_primary: list[BaseException | None] = []

    def record_close(
        connection: sqlite3.Connection,
        _authority: artifact_retention_module.PrivateSqlitePathAuthority,
        *,
        primary_error: BaseException | None = None,
        known_identity_failure: bool = False,
    ) -> None:
        del known_identity_failure
        observed_primary.append(primary_error)
        connection.close()

    monkeypatch.setattr(
        artifact_retention_module,
        "close_verified_sqlite_connection",
        record_close,
    )

    with pytest.raises(RuntimeError, match="readonly consumer"), reader._read_snapshot():
        raise business_error

    assert observed_primary == [business_error]


def test_writable_registry_safe_create_rejects_parent_replacement_without_leak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    retired = tmp_path / "retired"
    real_lexists = artifact_retention_module.os.path.lexists
    swapped = False

    def replace_parent_after_authority_snapshot(candidate: object) -> bool:
        nonlocal swapped
        if Path(candidate) == path and not swapped:
            swapped = True
            private.rename(retired)
            private.mkdir(mode=0o700)
            return False
        return real_lexists(candidate)

    monkeypatch.setattr(
        artifact_retention_module.os.path,
        "lexists",
        replace_parent_after_authority_snapshot,
    )

    with pytest.raises(ValueError, match="parent|identity|changed"):
        ExperimentRegistry(path, managed_trust_root=tmp_path)

    assert not path.exists()


def test_writable_registry_rolls_back_when_parent_is_replaced_during_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    registry = ExperimentRegistry(path, managed_trust_root=tmp_path)
    spec = _spec()
    manifest = _manifest([spec])
    retired = tmp_path / "retired"
    original_execute = registry_module._ExperimentRegistryConnection.execute
    swapped = False

    def replace_parent_after_insert(
        connection: registry_module._ExperimentRegistryConnection,
        sql: str,
        parameters: tuple[object, ...] = (),
        /,
    ) -> sqlite3.Cursor:
        nonlocal swapped
        result = original_execute(connection, sql, parameters)
        if "INSERT INTO hypothesis_family_manifest" in sql and not swapped:
            swapped = True
            private.rename(retired)
            private.mkdir(mode=0o700)
            for suffix in ("", "-wal", "-shm"):
                source = Path(f"{retired / path.name}{suffix}")
                if source.exists():
                    source.rename(Path(f"{path}{suffix}"))
        return result

    monkeypatch.setattr(
        registry_module._ExperimentRegistryConnection,
        "execute",
        replace_parent_after_insert,
    )

    with pytest.raises(BaseExceptionGroup) as captured:
        registry.register_hypothesis_family(manifest)
    _assert_identity_failure_group(captured.value)

    with sqlite3.connect(path) as connection:
        count = connection.execute("SELECT COUNT(*) FROM hypothesis_family_manifest").fetchone()[0]
    assert count == 0


def test_writable_registry_fences_implicit_initialization_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    retired = tmp_path / "retired"
    original_execute = registry_module._ExperimentRegistryConnection.execute
    swapped = False

    def replace_parent_after_policy_insert(
        connection: registry_module._ExperimentRegistryConnection,
        sql: str,
        parameters: tuple[object, ...] = (),
        /,
    ) -> sqlite3.Cursor:
        nonlocal swapped
        result = original_execute(connection, sql, parameters)
        if "INSERT INTO registry_metadata" in sql and not swapped:
            swapped = True
            private.rename(retired)
            private.mkdir(mode=0o700)
            for suffix in ("", "-wal", "-shm"):
                source = Path(f"{retired / path.name}{suffix}")
                if source.exists():
                    source.rename(Path(f"{path}{suffix}"))
        return result

    monkeypatch.setattr(
        registry_module._ExperimentRegistryConnection,
        "execute",
        replace_parent_after_policy_insert,
    )

    with pytest.raises(BaseExceptionGroup) as captured:
        ExperimentRegistry(path, managed_trust_root=tmp_path)
    _assert_identity_failure_group(captured.value)

    with sqlite3.connect(path) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE name = 'registry_metadata'"
        ).fetchall()
    assert tables == []


def test_writable_registry_rolls_back_schema_when_parent_changes_during_initialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private = tmp_path / "private"
    private.mkdir(mode=0o700)
    path = private / "experiments.sqlite3"
    retired = tmp_path / "retired"
    original_executescript = registry_module._ExperimentRegistryConnection.executescript

    def replace_parent_after_schema(
        connection: registry_module._ExperimentRegistryConnection,
        sql_script: str,
        /,
    ) -> sqlite3.Cursor:
        result = original_executescript(connection, sql_script)
        private.rename(retired)
        private.mkdir(mode=0o700)
        for suffix in ("", "-wal", "-shm"):
            source = Path(f"{retired / path.name}{suffix}")
            if source.exists():
                source.rename(Path(f"{path}{suffix}"))
        return result

    monkeypatch.setattr(
        registry_module._ExperimentRegistryConnection,
        "executescript",
        replace_parent_after_schema,
    )

    with pytest.raises(BaseExceptionGroup) as captured:
        ExperimentRegistry(path, managed_trust_root=tmp_path)
    _assert_identity_failure_group(captured.value)

    with sqlite3.connect(path) as connection:
        tables = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    assert tables == []
