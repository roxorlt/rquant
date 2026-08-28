from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.schema_compatibility as schema_compatibility
from rquant.runtime_contracts import RuntimeContractModel
from rquant.schema_compatibility import (
    CompatibilityOutcome,
    ConsumerFieldCapability,
    ConsumerSchemaRequirement,
    LiveSchemaRolloutPlan,
    RolloutPhase,
    SchemaDeclaration,
    SchemaField,
    SchemaParticipant,
    SchemaRequiredTransition,
    SchemaRolloutStore,
    UnknownFieldPolicy,
    evaluate_schema_compatibility,
)


def _field(
    name: str,
    *,
    type_name: str = "float64",
    required: bool = True,
    introduced_in: int = 1,
    deprecated_in: int | None = None,
    removed_in: int | None = None,
    nullable: bool = False,
    required_history: tuple[SchemaRequiredTransition, ...] = (),
) -> SchemaField:
    return SchemaField(
        name=name,
        type_name=type_name,
        required=required,
        introduced_in=introduced_in,
        deprecated_in=deprecated_in,
        removed_in=removed_in,
        nullable=nullable,
        required_history=required_history,
    )


def _declaration(
    *,
    current_version: int,
    fields: tuple[SchemaField, ...],
    min_reader_version: int = 1,
) -> SchemaDeclaration:
    return SchemaDeclaration(
        dataset_id="market-minute",
        schema_name="market_minute_batch",
        min_reader_version=min_reader_version,
        current_version=current_version,
        fields=fields,
        producer_commit="a" * 40,
    )


def _consumer(
    *,
    min_version: int = 1,
    max_version: int = 4,
    required_fields: tuple[str, ...] = ("ts_code", "close"),
    optional_fields: tuple[str, ...] = (),
    unknown_field_policy: UnknownFieldPolicy = UnknownFieldPolicy.ALLOW,
) -> ConsumerSchemaRequirement:
    type_by_name = {"ts_code": "string"}
    return ConsumerSchemaRequirement(
        consumer_id="intraday-feature-live",
        dataset_id="market-minute",
        min_version=min_version,
        max_version=max_version,
        required_fields=required_fields,
        optional_fields=optional_fields,
        field_capabilities=tuple(
            ConsumerFieldCapability(
                name=name,
                type_name=type_by_name.get(name, "float64"),
                nullable=False,
            )
            for name in (*required_fields, *optional_fields)
        ),
        unknown_field_policy=unknown_field_policy,
    )


def _base_fields() -> tuple[SchemaField, ...]:
    return (_field("ts_code", type_name="string"), _field("close"))


@pytest.mark.parametrize(
    "changes",
    [
        {"introduced_in": 0},
        {"introduced_in": 2, "deprecated_in": 1},
        {"introduced_in": 2, "removed_in": 2},
        {"introduced_in": 1, "deprecated_in": 3, "removed_in": 3},
        {"introduced_in": 1, "deprecated_in": 4, "removed_in": 3},
    ],
)
def test_schema_field_rejects_invalid_version_chronology(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        _field("amount", **changes)  # type: ignore[arg-type]


def test_schema_declaration_requires_unique_fields_and_valid_version_bounds() -> None:
    with pytest.raises(ValidationError, match="unique"):
        _declaration(current_version=1, fields=(_field("close"), _field("close")))
    with pytest.raises(ValidationError, match="min_reader_version"):
        _declaration(current_version=1, min_reader_version=2, fields=_base_fields())
    with pytest.raises(ValidationError, match="introduced_in"):
        _declaration(
            current_version=1,
            fields=(*_base_fields(), _field("amount", introduced_in=2)),
        )


def test_schema_declaration_fingerprint_uses_semantic_field_order() -> None:
    left = _declaration(current_version=1, fields=_base_fields())
    right = _declaration(current_version=1, fields=tuple(reversed(_base_fields())))

    assert left.schema_fingerprint == right.schema_fingerprint
    assert len(left.schema_fingerprint) == 64


def test_consumer_requirement_requires_unique_disjoint_fields() -> None:
    with pytest.raises(ValidationError, match="unique"):
        _consumer(required_fields=("close", "close"))
    with pytest.raises(ValidationError, match="disjoint"):
        _consumer(required_fields=("close",), optional_fields=("close",))
    with pytest.raises(ValidationError, match="max_version"):
        _consumer(min_version=3, max_version=2)


def test_consumer_declares_type_nullability_and_unknown_field_policy() -> None:
    with pytest.raises(ValidationError, match="capability"):
        ConsumerSchemaRequirement(
            consumer_id="strict-reader",
            dataset_id="market-minute",
            min_version=1,
            max_version=2,
            required_fields=("close",),
            optional_fields=(),
            field_capabilities=(),
            unknown_field_policy=UnknownFieldPolicy.FORBID,
        )

    old = _declaration(current_version=1, fields=_base_fields())
    nullable = _declaration(
        current_version=2,
        fields=(
            _field("ts_code", type_name="string"),
            _field("close", nullable=True),
        ),
    )
    wrong_type = _consumer(required_fields=("ts_code", "close"))
    strict = _consumer(unknown_field_policy=UnknownFieldPolicy.FORBID)
    extra = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )

    nullable_decision = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=nullable,
        consumer=wrong_type,
        phase=RolloutPhase.DUAL_WRITE,
    )
    strict_decision = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=extra,
        consumer=strict,
        phase=RolloutPhase.DUAL_WRITE,
    )

    assert nullable_decision.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("nullable" in reason for reason in nullable_decision.reasons)
    assert strict_decision.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("unknown field amount" in reason for reason in strict_decision.reasons)


def test_optional_addition_sequence_moves_from_degraded_to_compatible() -> None:
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )

    before_support = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(optional_fields=("amount",)),
        phase=RolloutPhase.PREPARE_OPTIONAL,
    )
    after_support = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(),
        phase=RolloutPhase.DUAL_WRITE,
    )

    assert before_support.outcome is CompatibilityOutcome.COMPATIBLE
    assert before_support.readable_version == 2
    assert after_support.outcome is CompatibilityOutcome.COMPATIBLE

    old_only = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=old,
        consumer=_consumer(optional_fields=("amount",)),
        phase=RolloutPhase.PREPARE_OPTIONAL,
    )
    assert old_only.outcome is CompatibilityOutcome.DEGRADED
    assert "optional field amount is unavailable" in old_only.reasons


def test_required_field_promotion_waits_for_phase_and_consumer_support() -> None:
    old = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    new = _declaration(
        current_version=3,
        fields=(
            *_base_fields(),
            _field(
                "amount",
                introduced_in=2,
                required_history=(
                    SchemaRequiredTransition(version=2, required=False),
                    SchemaRequiredTransition(version=3, required=True),
                ),
            ),
        ),
    )

    premature = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(optional_fields=("amount",)),
        phase=RolloutPhase.DUAL_READ,
    )
    unsupported = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(),
        phase=RolloutPhase.REQUIRE_NEW,
    )
    promoted = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(required_fields=("ts_code", "close", "amount")),
        phase=RolloutPhase.REQUIRE_NEW,
    )

    assert premature.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("before require_new" in reason for reason in premature.reasons)
    assert unsupported.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("does not explicitly support" in reason for reason in unsupported.reasons)
    assert promoted.outcome is CompatibilityOutcome.COMPATIBLE
    assert promoted.readable_version == 3


def test_dual_read_accepts_old_and_new_optional_shapes() -> None:
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    consumer = _consumer(optional_fields=("amount",))

    old_decision = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=old,
        consumer=consumer,
        phase=RolloutPhase.DUAL_READ,
    )
    new_decision = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=consumer,
        phase=RolloutPhase.DUAL_READ,
    )

    assert old_decision.outcome is CompatibilityOutcome.DEGRADED
    assert old_decision.readable_version == 1
    assert new_decision.outcome is CompatibilityOutcome.COMPATIBLE
    assert new_decision.readable_version == 2


def test_field_removal_is_forbidden_during_dual_read_and_explicit_at_retirement() -> None:
    old = _declaration(
        current_version=2,
        fields=(
            *_base_fields(),
            _field("legacy_volume", required=False, deprecated_in=2),
        ),
    )
    new = _declaration(
        current_version=3,
        fields=(
            *_base_fields(),
            _field(
                "legacy_volume",
                required=False,
                deprecated_in=2,
                removed_in=3,
            ),
        ),
    )

    dual_read = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(),
        phase=RolloutPhase.DUAL_READ,
    )
    retired = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(),
        phase=RolloutPhase.RETIRE_OLD,
    )
    still_required = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=new,
        consumer=_consumer(
            required_fields=("ts_code", "close", "legacy_volume"),
        ),
        phase=RolloutPhase.RETIRE_OLD,
    )

    assert dual_read.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("cannot be removed during dual_read" in reason for reason in dual_read.reasons)
    assert retired.outcome is CompatibilityOutcome.COMPATIBLE
    assert still_required.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("required field legacy_volume" in reason for reason in still_required.reasons)


def test_type_change_and_version_range_mismatch_fail_closed() -> None:
    old = _declaration(current_version=1, fields=_base_fields())
    changed = _declaration(
        current_version=2,
        fields=(_field("ts_code", type_name="string"), _field("close", type_name="decimal")),
    )

    changed_decision = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=changed,
        consumer=_consumer(),
        phase=RolloutPhase.DUAL_WRITE,
    )
    version_decision = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=changed,
        consumer=_consumer(max_version=1),
        phase=RolloutPhase.DUAL_WRITE,
    )

    assert changed_decision.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("type changed" in reason for reason in changed_decision.reasons)
    assert version_decision.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert version_decision.readable_version is None
    assert any("outside consumer range" in reason for reason in version_decision.reasons)


def test_same_version_semantic_change_and_field_history_rewrite_fail_closed() -> None:
    old = _declaration(current_version=2, fields=_base_fields())
    same_version_changed = _declaration(
        current_version=2,
        fields=(_field("ts_code", type_name="string"), _field("close", nullable=True)),
    )
    rewritten_introduction = _declaration(
        current_version=3,
        fields=(
            _field("ts_code", type_name="string"),
            _field("close", introduced_in=2),
        ),
    )
    old_required_history = (
        SchemaRequiredTransition(version=1, required=False),
        SchemaRequiredTransition(version=2, required=True),
    )
    new_required_history = (SchemaRequiredTransition(version=1, required=True),)
    required_old = _declaration(
        current_version=2,
        fields=(
            _field("ts_code", type_name="string"),
            _field("close", required=True, required_history=old_required_history),
        ),
    )
    required_rewritten = _declaration(
        current_version=3,
        fields=(
            _field("ts_code", type_name="string"),
            _field("close", required=True, required_history=new_required_history),
        ),
    )

    decisions = (
        evaluate_schema_compatibility(
            old_declaration=old,
            new_declaration=same_version_changed,
            consumer=_consumer(),
            phase=RolloutPhase.DUAL_WRITE,
        ),
        evaluate_schema_compatibility(
            old_declaration=old,
            new_declaration=rewritten_introduction,
            consumer=_consumer(),
            phase=RolloutPhase.DUAL_WRITE,
        ),
        evaluate_schema_compatibility(
            old_declaration=required_old,
            new_declaration=required_rewritten,
            consumer=_consumer(),
            phase=RolloutPhase.REQUIRE_NEW,
        ),
    )

    assert all(item.outcome is CompatibilityOutcome.INCOMPATIBLE for item in decisions)
    assert any("same schema version" in reason for reason in decisions[0].reasons)
    assert any("introduced_in history" in reason for reason in decisions[1].reasons)
    assert any("required history" in reason for reason in decisions[2].reasons)


def test_already_removed_field_history_cannot_be_rewritten_or_backfilled() -> None:
    old = _declaration(
        current_version=2,
        fields=(
            *_base_fields(),
            _field(
                "legacy",
                required=False,
                introduced_in=1,
                deprecated_in=1,
                removed_in=2,
            ),
        ),
    )
    rewritten = _declaration(
        current_version=3,
        fields=(
            *_base_fields(),
            _field(
                "legacy",
                required=False,
                introduced_in=1,
                deprecated_in=2,
                removed_in=3,
            ),
        ),
    )
    backfilled = _declaration(
        current_version=3,
        fields=(
            *_base_fields(),
            _field("amount", required=False, introduced_in=1, deprecated_in=2),
        ),
    )

    rewritten_decision = evaluate_schema_compatibility(
        old_declaration=old,
        new_declaration=rewritten,
        consumer=_consumer(),
        phase=RolloutPhase.RETIRE_OLD,
    )
    backfilled_decision = evaluate_schema_compatibility(
        old_declaration=_declaration(current_version=2, fields=_base_fields()),
        new_declaration=backfilled,
        consumer=_consumer(),
        phase=RolloutPhase.DUAL_WRITE,
    )

    assert rewritten_decision.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any("deprecated_in history" in reason for reason in rewritten_decision.reasons)
    assert any("removed_in history" in reason for reason in rewritten_decision.reasons)
    assert backfilled_decision.outcome is CompatibilityOutcome.INCOMPATIBLE
    assert any(
        "introduced_in" in reason or "backfilled" in reason
        for reason in backfilled_decision.reasons
    )


def test_rollout_plan_has_stable_registry_bound_identity() -> None:
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    started_at = datetime(2026, 7, 31, 1, 0, tzinfo=UTC)
    payload = {
        "dataset_id": "market-minute",
        "old_declaration_fingerprint": old.schema_fingerprint,
        "new_declaration_fingerprint": new.schema_fingerprint,
        "producers": (
            SchemaParticipant(
                participant_id="market-minute-gateway",
                contract_fingerprint="1" * 64,
            ),
        ),
        "consumers": (
            SchemaParticipant(participant_id="feature-live", contract_fingerprint="2" * 64),
            SchemaParticipant(participant_id="paper-runner", contract_fingerprint="3" * 64),
        ),
        "started_at": started_at,
        "deadline": started_at + timedelta(hours=2),
    }
    left = LiveSchemaRolloutPlan(**payload)
    right = LiveSchemaRolloutPlan(
        **{
            **payload,
            "consumers": tuple(reversed(payload["consumers"])),
        }
    )

    assert left.plan_id == right.plan_id
    assert len(left.plan_id) == 64
    with pytest.raises(ValidationError, match="producer registry"):
        LiveSchemaRolloutPlan(
            **{
                **payload,
                "producers": (),
            }
        )
    with pytest.raises(ValidationError, match="consumer registry"):
        LiveSchemaRolloutPlan(
            **{
                **payload,
                "consumers": (),
            }
        )


def test_rollout_store_requires_complete_registry_and_consecutive_cas_phases(
    tmp_path: Path,
) -> None:
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    started_at = datetime(2026, 7, 31, 1, 0, tzinfo=UTC)
    plan = LiveSchemaRolloutPlan(
        dataset_id="market-minute",
        old_declaration_fingerprint=old.schema_fingerprint,
        new_declaration_fingerprint=new.schema_fingerprint,
        producers=(SchemaParticipant(participant_id="gateway", contract_fingerprint="1" * 64),),
        consumers=(
            SchemaParticipant(participant_id="feature", contract_fingerprint="2" * 64),
            SchemaParticipant(participant_id="paper", contract_fingerprint="3" * 64),
        ),
        started_at=started_at,
        deadline=started_at + timedelta(hours=2),
    )
    store = SchemaRolloutStore(tmp_path / "rollout.sqlite3")
    state = store.create_plan(plan, now=started_at)

    with pytest.raises(ValueError, match="consecutive"):
        store.advance(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            target_phase=RolloutPhase.RETIRE_OLD,
            now=started_at + timedelta(minutes=1),
        )

    for participant in (*plan.producers, *plan.consumers):
        state = store.acknowledge(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            phase=RolloutPhase.PREPARE_OPTIONAL,
            participant_id=participant.participant_id,
            participant_fingerprint=participant.contract_fingerprint,
            declaration_fingerprint=new.schema_fingerprint,
            now=started_at + timedelta(minutes=2),
        )

    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.DUAL_WRITE,
        now=started_at + timedelta(minutes=3),
    )
    assert state.phase is RolloutPhase.DUAL_WRITE

    with pytest.raises(ValueError, match="CAS"):
        store.acknowledge(
            plan_id=plan.plan_id,
            expected_revision=0,
            phase=RolloutPhase.DUAL_WRITE,
            participant_id="gateway",
            participant_fingerprint="1" * 64,
            declaration_fingerprint=new.schema_fingerprint,
            now=started_at + timedelta(minutes=4),
        )
    with pytest.raises(ValueError, match="fingerprint"):
        store.acknowledge(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            phase=RolloutPhase.DUAL_WRITE,
            participant_id="gateway",
            participant_fingerprint="f" * 64,
            declaration_fingerprint=new.schema_fingerprint,
            now=started_at + timedelta(minutes=4),
        )
    with pytest.raises(ValueError, match="deadline"):
        store.acknowledge(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            phase=RolloutPhase.DUAL_WRITE,
            participant_id="gateway",
            participant_fingerprint="1" * 64,
            declaration_fingerprint=new.schema_fingerprint,
            now=plan.deadline + timedelta(seconds=1),
        )
    with pytest.raises(ValueError, match="time cannot precede"):
        store.acknowledge(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            phase=RolloutPhase.DUAL_WRITE,
            participant_id="gateway",
            participant_fingerprint="1" * 64,
            declaration_fingerprint=new.schema_fingerprint,
            now=started_at - timedelta(seconds=1),
        )


def test_contracts_round_trip_json_as_frozen_runtime_models() -> None:
    local = timezone(timedelta(hours=8))
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    plan = LiveSchemaRolloutPlan(
        dataset_id="market-minute",
        old_declaration_fingerprint=old.schema_fingerprint,
        new_declaration_fingerprint=new.schema_fingerprint,
        producers=(
            SchemaParticipant(
                participant_id="market-minute-gateway",
                contract_fingerprint="1" * 64,
            ),
        ),
        consumers=(
            SchemaParticipant(participant_id="feature-live", contract_fingerprint="2" * 64),
        ),
        started_at=datetime(2026, 7, 31, 9, 0, tzinfo=local),
        deadline=datetime(2026, 7, 31, 10, 0, tzinfo=local),
    )

    restored = LiveSchemaRolloutPlan.model_validate_json(plan.model_dump_json())

    assert restored == plan
    assert restored.started_at == datetime(2026, 7, 31, 1, 0, tzinfo=UTC)
    assert restored.plan_id == plan.plan_id
    assert isinstance(restored, RuntimeContractModel)
    with pytest.raises(ValidationError):
        restored.deadline = restored.deadline + timedelta(hours=1)


@pytest.mark.parametrize("invalid", [True, "2", 2**31])
def test_schema_versions_are_strict_bounded_integers(invalid: object) -> None:
    with pytest.raises(ValidationError):
        SchemaField(
            name="amount",
            type_name="float64",
            required=False,
            introduced_in=invalid,
        )


def test_removed_field_requires_a_prior_deprecation_and_bounded_history() -> None:
    with pytest.raises(ValidationError, match="deprecated"):
        _field("legacy", required=False, introduced_in=1, removed_in=3)
    with pytest.raises(ValidationError, match="removed"):
        _field(
            "legacy",
            required=False,
            introduced_in=1,
            deprecated_in=2,
            removed_in=3,
            required_history=(
                SchemaRequiredTransition(version=1, required=False),
                SchemaRequiredTransition(version=3, required=False),
            ),
        )


def test_dual_write_contract_rejects_shared_value_drift_and_binds_new_data() -> None:
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    validate = schema_compatibility.validate_dual_write_values

    evidence = validate(
        old_declaration=old,
        new_declaration=new,
        old_values={"ts_code": "000001.SZ", "close": 10.5},
        new_values={"ts_code": "000001.SZ", "close": 10.5, "amount": 1_000_000.0},
        generation_id="4" * 64,
        observed_at=datetime(2026, 8, 2, 1, 1, tzinfo=UTC),
    )

    assert evidence.old_declaration_fingerprint == old.schema_fingerprint
    assert evidence.new_declaration_fingerprint == new.schema_fingerprint
    assert evidence.new_values_fingerprint != evidence.shared_values_fingerprint
    with pytest.raises(ValueError, match="shared field close"):
        validate(
            old_declaration=old,
            new_declaration=new,
            old_values={"ts_code": "000001.SZ", "close": 10.5},
            new_values={"ts_code": "000001.SZ", "close": 10.6, "amount": 1_000_000.0},
            generation_id="4" * 64,
            observed_at=datetime(2026, 8, 2, 1, 1, tzinfo=UTC),
        )


def _trusted_registry() -> object:
    registry_type = schema_compatibility.ProductionConsumerRegistry
    consumer_type = schema_compatibility.ProductionConsumerCapability
    return registry_type(
        registry_id="production-runtime-consumers",
        consumers=(
            consumer_type(
                consumer_id="feature-reader",
                service_id="rquant-runtime-feature@n-shape.service",
                dataset_id="market-minute",
                contract_fingerprint="2" * 64,
                code_commit="a" * 40,
                min_readable_schema_version=1,
                max_readable_schema_version=2,
                required_fields=("close", "ts_code"),
            ),
            consumer_type(
                consumer_id="paper-reader",
                service_id="rquant-runtime-paper-broker@n-shape.service",
                dataset_id="market-minute",
                contract_fingerprint="3" * 64,
                code_commit="a" * 40,
                min_readable_schema_version=1,
                max_readable_schema_version=2,
                required_fields=("close", "ts_code"),
            ),
        ),
    )


def _strict_rollout_plan(*, started_at: datetime) -> LiveSchemaRolloutPlan:
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    registry = _trusted_registry()
    return LiveSchemaRolloutPlan(
        dataset_id="market-minute",
        old_declaration_fingerprint=old.schema_fingerprint,
        new_declaration_fingerprint=new.schema_fingerprint,
        producers=(SchemaParticipant(participant_id="gateway", contract_fingerprint="1" * 64),),
        consumers=(
            SchemaParticipant(participant_id="feature-reader", contract_fingerprint="2" * 64),
            SchemaParticipant(participant_id="paper-reader", contract_fingerprint="3" * 64),
        ),
        production_consumer_registry_fingerprint=registry.registry_fingerprint,
        serving_physical_schema_fingerprint="5" * 64,
        target_generation_id="4" * 64,
        target_schema_version=2,
        consumer_ack_max_age_seconds=300,
        started_at=started_at,
        deadline=started_at + timedelta(hours=2),
    )


def _consumer_receipt(
    *,
    consumer_id: str,
    service_id: str,
    available_at: datetime,
) -> object:
    receipt_type = schema_compatibility.ConsumerCapabilityReceipt
    return receipt_type(
        consumer_id=consumer_id,
        service_id=service_id,
        code_commit="a" * 40,
        dataset_id="market-minute",
        min_readable_schema_version=1,
        max_readable_schema_version=2,
        required_fields=("close", "ts_code"),
        serving_physical_schema_fingerprint="5" * 64,
        observed_generation_id="4" * 64,
        available_at=available_at,
    )


def test_rollout_state_machine_requires_trusted_fresh_consumer_capabilities(
    tmp_path: Path,
) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    registry = _trusted_registry()
    plan = _strict_rollout_plan(started_at=started_at)
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=registry,
    )
    state = store.create_plan(plan, now=started_at, operation_id="create")
    assert state.phase is RolloutPhase.PREPARE
    assert state.authority_declaration_fingerprint == plan.old_declaration_fingerprint

    with pytest.raises(ValueError, match="consecutive"):
        store.advance(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            target_phase=RolloutPhase.CUTOVER,
            now=started_at + timedelta(seconds=1),
            operation_id="skip-to-cutover",
        )

    state = store.acknowledge(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        phase=RolloutPhase.PREPARE,
        participant_id="gateway",
        participant_fingerprint="1" * 64,
        declaration_fingerprint=plan.new_declaration_fingerprint,
        now=started_at + timedelta(seconds=2),
        operation_id="producer-prepare",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.DUAL_WRITE,
        now=started_at + timedelta(seconds=3),
        operation_id="dual-write",
    )
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )
    evidence = schema_compatibility.validate_dual_write_values(
        old_declaration=old,
        new_declaration=new,
        old_values={"ts_code": "000001.SZ", "close": 10.5},
        new_values={"ts_code": "000001.SZ", "close": 10.5, "amount": 1.0},
        generation_id="4" * 64,
        observed_at=started_at + timedelta(seconds=4),
    )
    state = store.record_dual_write_evidence(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        evidence=evidence,
        operation_id="dual-write-evidence",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CONSUMER_ACK,
        now=started_at + timedelta(seconds=5),
        operation_id="consumer-ack-phase",
    )
    state = store.acknowledge_consumer(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        receipt=_consumer_receipt(
            consumer_id="feature-reader",
            service_id="rquant-runtime-feature@n-shape.service",
            available_at=started_at + timedelta(seconds=6),
        ),
        now=started_at + timedelta(seconds=6),
        operation_id="feature-ack",
    )
    state = store.acknowledge_consumer(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        receipt=_consumer_receipt(
            consumer_id="paper-reader",
            service_id="rquant-runtime-paper-broker@n-shape.service",
            available_at=started_at + timedelta(seconds=7),
        ),
        now=started_at + timedelta(seconds=7),
        operation_id="paper-ack",
    )
    with pytest.raises(ValueError, match="stale"):
        store.advance(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            target_phase=RolloutPhase.CUTOVER,
            now=started_at + timedelta(seconds=400),
            operation_id="stale-cutover",
        )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CUTOVER,
        now=started_at + timedelta(seconds=8),
        operation_id="cutover",
    )

    assert state.phase is RolloutPhase.CUTOVER
    assert state.authority_declaration_fingerprint == plan.new_declaration_fingerprint
    with pytest.raises(ValueError, match="producer acknowledgement"):
        store.advance(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            target_phase=RolloutPhase.RETIRE,
            now=started_at + timedelta(seconds=9),
            operation_id="premature-retire",
        )
    state = store.acknowledge(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        phase=RolloutPhase.CUTOVER,
        participant_id="gateway",
        participant_fingerprint="1" * 64,
        declaration_fingerprint=plan.new_declaration_fingerprint,
        now=started_at + timedelta(seconds=10),
        operation_id="producer-cutover",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.RETIRE,
        now=started_at + timedelta(seconds=11),
        operation_id="retire",
    )
    assert state.phase is RolloutPhase.RETIRE
    with pytest.raises(ValueError, match="terminal"):
        store.rollback(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            reason="too late",
            now=started_at + timedelta(seconds=12),
            operation_id="rollback-retired",
        )
    events = store.receipts(plan.plan_id)
    assert tuple(event.revision for event in events) == tuple(range(len(events)))
    assert all(
        event.previous_hash == events[index - 1].event_hash
        for index, event in enumerate(events)
        if index
    )


def test_rollout_retry_is_idempotent_but_conflicting_operation_fails(tmp_path: Path) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=_trusted_registry(),
    )
    first = store.create_plan(plan, now=started_at, operation_id="same-create")
    retried = store.create_plan(plan, now=started_at, operation_id="same-create")
    assert retried == first
    assert len(store.receipts(plan.plan_id)) == 1

    with pytest.raises(ValueError, match="operation"):
        store.create_plan(
            plan,
            now=started_at + timedelta(seconds=1),
            operation_id="same-create",
        )


def test_rollout_hash_tamper_and_legacy_v1_registry_fail_closed(tmp_path: Path) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    path = tmp_path / "rollout.sqlite3"
    plan = _strict_rollout_plan(started_at=started_at)
    store = SchemaRolloutStore(path, production_consumer_registry=_trusted_registry())
    store.create_plan(plan, now=started_at, operation_id="create")
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER schema_rollout_event_no_update")
        connection.execute(
            "UPDATE schema_rollout_event SET event_hash = ? WHERE plan_id = ? AND revision = 0",
            ("f" * 64, plan.plan_id),
        )
    with pytest.raises(RuntimeError, match="hash chain"):
        store.get_state(plan.plan_id)

    legacy = tmp_path / "legacy.sqlite3"
    with sqlite3.connect(legacy) as connection:
        connection.execute(
            "CREATE TABLE schema_rollout (plan_id TEXT PRIMARY KEY, plan_json TEXT NOT NULL)"
        )
    with pytest.raises(RuntimeError, match="legacy v1"):
        SchemaRolloutStore(legacy)


def test_timeout_and_consumer_reject_roll_back_without_erasing_new_data(
    tmp_path: Path,
) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=_trusted_registry(),
    )
    state = store.create_plan(plan, now=started_at, operation_id="create")
    rolled_back = store.expire(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        now=plan.deadline + timedelta(seconds=1),
        operation_id="deadline-expired",
    )

    assert rolled_back.phase is RolloutPhase.ROLLBACK
    assert rolled_back.authority_declaration_fingerprint == plan.old_declaration_fingerprint
    assert rolled_back.new_data_preserved is True
    assert any("deadline" in event.payload_json for event in store.receipts(plan.plan_id))


def test_cutover_rejects_stale_or_untrusted_consumer_receipt(tmp_path: Path) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=_trusted_registry(),
    )
    state = store.create_plan(plan, now=started_at, operation_id="create")

    with pytest.raises(ValueError, match="trusted production consumer"):
        store.acknowledge_consumer(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            receipt=_consumer_receipt(
                consumer_id="invented-reader",
                service_id="invented.service",
                available_at=started_at,
            ),
            now=started_at,
            operation_id="invented-ack",
        )


def test_plan_cannot_replace_the_trusted_production_consumer_set(tmp_path: Path) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    forged = plan.model_copy(update={"consumers": plan.consumers[:1]})
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=_trusted_registry(),
    )

    with pytest.raises(ValueError, match="trusted production registry"):
        store.create_plan(forged, now=started_at, operation_id="forged-plan")


def _enter_dual_write(
    store: SchemaRolloutStore,
    plan: LiveSchemaRolloutPlan,
    *,
    started_at: datetime,
) -> object:
    state = store.create_plan(plan, now=started_at, operation_id="create")
    state = store.acknowledge(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        phase=RolloutPhase.PREPARE,
        participant_id="gateway",
        participant_fingerprint="1" * 64,
        declaration_fingerprint=plan.new_declaration_fingerprint,
        now=started_at + timedelta(seconds=1),
        operation_id="producer-prepare",
    )
    return store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.DUAL_WRITE,
        now=started_at + timedelta(seconds=2),
        operation_id="dual-write",
    )


def _dual_write_evidence(*, started_at: datetime) -> object:
    return schema_compatibility.validate_dual_write_values(
        old_declaration=_declaration(current_version=1, fields=_base_fields()),
        new_declaration=_declaration(
            current_version=2,
            fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
        ),
        old_values={"ts_code": "000001.SZ", "close": 10.5},
        new_values={"ts_code": "000001.SZ", "close": 10.5, "amount": 1.0},
        generation_id="4" * 64,
        observed_at=started_at + timedelta(seconds=3),
    )


def test_dual_write_crash_rolls_back_evidence_and_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=_trusted_registry(),
    )
    state = _enter_dual_write(store, plan, started_at=started_at)

    def crash(*args: object, **kwargs: object) -> object:
        raise RuntimeError("simulated crash after evidence insert")

    monkeypatch.setattr(store, "_append_mutation", crash)
    with pytest.raises(RuntimeError, match="simulated crash"):
        store.record_dual_write_evidence(
            plan_id=plan.plan_id,
            expected_revision=state.revision,
            evidence=_dual_write_evidence(started_at=started_at),
            operation_id="evidence-crash",
        )

    reopened = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=_trusted_registry(),
    )
    assert reopened.get_state(plan.plan_id) == state
    assert reopened.dual_write_evidence(plan.plan_id) == ()


def test_dual_write_persists_both_canonical_shapes_and_replays_idempotently(
    tmp_path: Path,
) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    registry = _trusted_registry()
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=registry,
    )
    state = _enter_dual_write(store, plan, started_at=started_at)
    old = _declaration(current_version=1, fields=_base_fields())
    new = _declaration(
        current_version=2,
        fields=(*_base_fields(), _field("amount", required=False, introduced_in=2)),
    )

    state = store.record_dual_write_values(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        old_declaration=old,
        new_declaration=new,
        old_values={"ts_code": "000001.SZ", "close": 10.5},
        new_values={"ts_code": "000001.SZ", "close": 10.5, "amount": 1.0},
        generation_id="4" * 64,
        observed_at=started_at + timedelta(seconds=3),
        operation_id="batch:000001",
    )
    replay = store.record_dual_write_values(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        old_declaration=old,
        new_declaration=new,
        old_values={"close": 10.5, "ts_code": "000001.SZ"},
        new_values={"amount": 1.0, "close": 10.5, "ts_code": "000001.SZ"},
        generation_id="4" * 64,
        observed_at=started_at + timedelta(seconds=3),
        operation_id="batch:000001",
    )

    records = store.dual_write_records(plan.plan_id)
    assert replay == state
    assert len(records) == 1
    assert records[0].old_values == {"close": 10.5, "ts_code": "000001.SZ"}
    assert records[0].new_values == {
        "amount": 1.0,
        "close": 10.5,
        "ts_code": "000001.SZ",
    }
    assert records[0].evidence.generation_id == "4" * 64

    reopened = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=registry,
    )
    assert reopened.dual_write_records(plan.plan_id) == records


def test_rollback_retains_new_field_evidence_and_consumer_reject_history(
    tmp_path: Path,
) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    store = SchemaRolloutStore(
        tmp_path / "rollout.sqlite3",
        production_consumer_registry=_trusted_registry(),
    )
    state = _enter_dual_write(store, plan, started_at=started_at)
    evidence = _dual_write_evidence(started_at=started_at)
    state = store.record_dual_write_evidence(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        evidence=evidence,
        operation_id="evidence",
    )
    state = store.advance(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        target_phase=RolloutPhase.CONSUMER_ACK,
        now=started_at + timedelta(seconds=4),
        operation_id="consumer-ack",
    )
    state = store.reject_consumer(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        consumer_id="feature-reader",
        reason="cannot decode observed generation",
        now=started_at + timedelta(seconds=5),
        operation_id="consumer-reject",
    )

    assert state.phase is RolloutPhase.ROLLBACK
    assert store.dual_write_evidence(plan.plan_id) == (evidence,)
    assert any(
        "consumer_reject" in receipt.payload_json for receipt in store.receipts(plan.plan_id)
    )


def test_deleting_bound_evidence_is_detected_even_if_sqlite_trigger_is_removed(
    tmp_path: Path,
) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    path = tmp_path / "rollout.sqlite3"
    store = SchemaRolloutStore(
        path,
        production_consumer_registry=_trusted_registry(),
    )
    state = _enter_dual_write(store, plan, started_at=started_at)
    state = store.record_dual_write_evidence(
        plan_id=plan.plan_id,
        expected_revision=state.revision,
        evidence=_dual_write_evidence(started_at=started_at),
        operation_id="evidence",
    )
    assert state.phase is RolloutPhase.DUAL_WRITE

    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER schema_dual_write_evidence_no_delete")
        connection.execute(
            "DELETE FROM schema_dual_write_evidence WHERE plan_id = ?",
            (plan.plan_id,),
        )

    with pytest.raises(RuntimeError, match="evidence.*hash chain"):
        store.get_state(plan.plan_id)


def test_strict_rollout_reopen_requires_the_same_trusted_registry(tmp_path: Path) -> None:
    started_at = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)
    plan = _strict_rollout_plan(started_at=started_at)
    path = tmp_path / "rollout.sqlite3"
    store = SchemaRolloutStore(
        path,
        production_consumer_registry=_trusted_registry(),
    )
    store.create_plan(plan, now=started_at, operation_id="create")

    without_registry = SchemaRolloutStore(path)
    with pytest.raises(RuntimeError, match="trusted production consumer registry"):
        without_registry.get_state(plan.plan_id)

    registry_type = schema_compatibility.ProductionConsumerRegistry
    wrong_registry = registry_type(
        registry_id="different-production-registry",
        consumers=_trusted_registry().consumers,
    )
    with pytest.raises(ValueError, match="trusted production registry"):
        SchemaRolloutStore(
            path,
            production_consumer_registry=wrong_registry,
        ).get_state(plan.plan_id)
