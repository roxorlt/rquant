"""Public runs cannot supply ownership or replace an accepted Lab plan."""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from rquant.experiment_registry import FormalExperimentPlan
from rquant.strategy_template_run_commands import (
    AcceptedStrategyTemplateRun,
    OwnedRunStrategyTemplate,
    RunStrategyTemplate,
)
from tests.unit.test_strategy_template_adapter import adapter_fixture, template_spec


def accepted_run(tmp_path):
    target, value, catalog, _ = adapter_fixture(tmp_path)
    spec = template_spec(value)
    parameters = {item.name: item.value for item in spec.parameters.arguments}
    request = RunStrategyTemplate(
        command_id=parameters["request_id"],
        requested_at=value.definition.available_at - timedelta(days=30),
        generation_id="generation-a",
        strategy_id=value.definition.logical_id,
        head=catalog.versions[0].head,
        expected_head=catalog.versions[0].head,
        start_date=spec.parameters.start_date,
        end_date=spec.parameters.end_date,
        initial_cash=value.request.initial_cash,
    )
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=spec.experiment.spec,
        hypothesis_variant=spec.experiment.hypothesis_variant,
        strategy_definition_fingerprint=value.definition.fingerprint,
        definition_registration_record_hash=value.definition.record_hash,
        preregistered_at=value.definition.available_at,
    )
    accepted = AcceptedStrategyTemplateRun(
        owner_id="alice",
        request=request,
        metadata_identity=target.identity(),
        accepted_at=value.definition.available_at,
        spec=spec,
        plan=plan,
    )
    return target, value, accepted


@pytest.mark.parametrize("cash", ["1e-1000000000", "1e1000000000", "0.001", True, "1000000000001"])
def test_public_run_cash_is_rejected_before_unsafe_math(tmp_path, cash) -> None:
    _, _, accepted = accepted_run(tmp_path)
    with pytest.raises(ValueError):
        RunStrategyTemplate.model_validate(
            {**accepted.request.model_dump(mode="python"), "initial_cash": cash}
        )


def test_run_public_owner_injection_and_changed_accepted_spec_are_rejected(tmp_path) -> None:
    _, _, accepted = accepted_run(tmp_path)
    with pytest.raises(ValueError, match="owner_id"):
        RunStrategyTemplate.model_validate(
            {**accepted.request.model_dump(mode="python"), "owner_id": "bob"}
        )
    with pytest.raises(ValueError, match="owner"):
        AcceptedStrategyTemplateRun.model_validate(
            {**accepted.model_dump(mode="python"), "owner_id": "bob"}
        )
    owned = OwnedRunStrategyTemplate(
        **accepted.request.model_dump(mode="python"),
        owner_id="alice",
        metadata_identity=accepted.metadata_identity,
        accepted=accepted,
    )
    assert owned.accepted.request.requested_at < owned.accepted.accepted_at
    with pytest.raises(ValueError, match="original accepted"):
        OwnedRunStrategyTemplate.model_validate(
            {**owned.model_dump(mode="python"), "initial_cash": Decimal("3000.010000")}
        )
