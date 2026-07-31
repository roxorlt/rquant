from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

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
        fields=(*_base_fields(), _field("legacy_volume", required=False)),
    )
    new = _declaration(
        current_version=3,
        fields=(
            *_base_fields(),
            _field("legacy_volume", required=False, removed_in=3),
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
