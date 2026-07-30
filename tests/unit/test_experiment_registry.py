from __future__ import annotations

import sqlite3
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError

import rquant.experiment_registry as registry_module
from rquant.experiment_registry import (
    DateRange,
    ExperimentIdentityConflictError,
    ExperimentOutcome,
    ExperimentRegistry,
    ExperimentRegistryError,
    ExperimentSpec,
    IncompleteHypothesisFamilyError,
    PromotionStage,
    TerminalExperimentError,
)

_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_HASH_D = "d" * 64
_HASH_E = "e" * 64
_HASH_F = "f" * 64
_COMMIT = "e" * 40
_NOW = datetime(2026, 7, 31, 1, 0, tzinfo=UTC)
_FORWARD_AVAILABLE = datetime(2026, 8, 14, 8, 0, tzinfo=UTC)


def _spec(
    *,
    parameter_fingerprint: str = _HASH_B,
    family: str = "n-shape-v1",
    seed: int = 7,
) -> ExperimentSpec:
    return ExperimentSpec(
        strategy_spec_fingerprint=_HASH_A,
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


def test_family_must_be_preregistered_with_complete_immutable_search_space(tmp_path) -> None:
    registry = ExperimentRegistry(tmp_path / "experiments.sqlite3")
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
    registry = ExperimentRegistry(tmp_path / "experiments.sqlite3")
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
    registry = ExperimentRegistry(tmp_path / "experiments.sqlite3")
    specs = [_spec(parameter_fingerprint=value * 64) for value in ("1", "2", "3")]
    _register_family(registry, specs)

    with pytest.raises(IncompleteHypothesisFamilyError, match="manifest"):
        _succeed(registry, specs[0], _outcome(specs[0], attempted=1, rank=1, raw_p="0.01"))


def test_bh_adjustment_uses_preregistered_family_and_waits_for_every_attempt(tmp_path) -> None:
    registry = ExperimentRegistry(tmp_path / "experiments.sqlite3")
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
    registry = ExperimentRegistry(tmp_path / "experiments.sqlite3", minimum_comparable_trades=10)
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
    first = ExperimentRegistry(path, maximum_forward_drawdown=Decimal("0.10"))
    reopened = ExperimentRegistry(path, maximum_forward_drawdown=Decimal("0.10"))

    assert first.policy.policy_fingerprint == reopened.policy.policy_fingerprint
    with pytest.raises(ExperimentRegistryError, match="policy"):
        ExperimentRegistry(path, maximum_forward_drawdown=Decimal("0.20"))


def test_registry_uses_wal_and_terminal_evidence_remains_immutable(tmp_path) -> None:
    path = tmp_path / "experiments.sqlite3"
    spec = _spec()
    outcome = _outcome(spec, attempted=1, rank=1, raw_p="0.01")
    first = ExperimentRegistry(path)
    _register_family(first, [spec])

    original = _succeed(first, spec, outcome)
    reopened = ExperimentRegistry(path)
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
