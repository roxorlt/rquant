from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from rquant.definition_registry import StrategySpecRegistration
from rquant.experiment_registry import ExperimentSpec, ExperimentStatus
from rquant.research_run_spec import ExecutionCostSpec
from rquant.strategy_promotion_contracts import NativeMinuteConfiguration
from rquant.strategy_promotion_evidence import StrategyPromotionEvidenceSource


@pytest.fixture(scope="module")
def original_bindings() -> tuple[SimpleNamespace, ...]:
    # Metadata extracted from the accepted complete 112 seals. This test does not
    # replace their physical reader, or claim a new journal/owner installation.
    path = (
        Path(__file__).resolve().parents[2]
        / "data/verification/strategy-promotion-20261006"
        / "native-statistical-target-fix-117/actual-target-bindings.json"
    )
    raw = path.read_bytes()
    assert hashlib.sha256(raw).hexdigest() == (
        "6fba288d96b18bcc4a1e9aa20cf3a54b9a96f252a165302273b7667eb24e88d4"
    )
    return tuple(
        SimpleNamespace(
            configuration=NativeMinuteConfiguration.model_validate(item["configuration"]),
            registration=StrategySpecRegistration.model_validate(item["registration"]),
            spec=ExperimentSpec.model_validate(item["spec"]),
            job_id=UUID(item["job_id"]),
            result_hash=item["result_hash"],
            execution_costs=ExecutionCostSpec.model_validate(item["execution_costs"]),
        )
        for item in json.loads(raw)["bindings"]
    )


def original_facts(bindings: tuple[SimpleNamespace, ...]) -> tuple[SimpleNamespace, ...]:
    return tuple(
        SimpleNamespace(
            configuration=item.configuration,
            owner=item.configuration.selection.target.owner_id,
            attempt=SimpleNamespace(status=ExperimentStatus.EXECUTED, spec=item.spec),
            child=SimpleNamespace(job_id=item.job_id),
            result_hash=item.result_hash,
        )
        for item in bindings
    )


def binding_source(bindings: tuple[SimpleNamespace, ...]) -> StrategyPromotionEvidenceSource:
    source = object.__new__(StrategyPromotionEvidenceSource)
    by_id = {item.spec.experiment_id: item for item in bindings}
    source._registration = lambda fact: by_id[fact.attempt.spec.experiment_id].registration
    # Only the original identity guard is exercised below. No finance phase or
    # complete result is fabricated by this minimal refusal fixture.
    source._read = lambda fact, family: SimpleNamespace(
        native=SimpleNamespace(
            target=by_id[fact.attempt.spec.experiment_id].configuration.selection.target,
            execution_costs=by_id[fact.attempt.spec.experiment_id].execution_costs,
        )
    )
    return source


@pytest.mark.parametrize("selected_index", [0, 1, 2])
def test_each_native_parent_uses_its_complete_original_target_in_full_n(
    original_bindings: tuple[SimpleNamespace, ...], selected_index: int
) -> None:
    source = binding_source(original_bindings)
    facts = original_facts(original_bindings)
    calls = []

    def observe_exact_target(target: object, family: object, fact: SimpleNamespace) -> tuple:
        assert target == fact.configuration.selection.target
        calls.append(target)
        return SimpleNamespace(target=target), None, ()

    source.validation = observe_exact_target
    selected = original_bindings[selected_index].configuration.selection.target
    values = source._statistics_inputs(selected, object(), facts)
    assert len(values) == len(original_bindings) == 3
    assert calls == [item.configuration.selection.target for item in original_bindings]
    assert [value[0] for value in values] == [item.spec.experiment_id for item in original_bindings]
    assert len({target.name for target in calls}) == 3


def test_exact_selected_result_is_reused_without_changing_the_other_native_targets(
    original_bindings: tuple[SimpleNamespace, ...],
) -> None:
    source = binding_source(original_bindings)
    facts = original_facts(original_bindings)
    selected = original_bindings[1]
    bound = SimpleNamespace(
        target=selected.configuration.selection.target,
        reference=SimpleNamespace(job_id=selected.job_id, result_hash=selected.result_hash),
    )
    view = object()
    calls = []

    def observe_exact_target(target: object, family: object, fact: SimpleNamespace) -> tuple:
        assert target == fact.configuration.selection.target
        calls.append(target.strategy_id)
        return SimpleNamespace(target=target), None, ()

    source.validation = observe_exact_target
    values = source._statistics_inputs(
        original_bindings[0].configuration.selection.target,
        object(),
        facts,
        selected_result=(selected.spec.experiment_id, bound, view, ()),
    )
    assert calls == ["n_shape", "growth_board_surge"]
    assert values[1] == (selected.spec.experiment_id, bound, view, ())


@pytest.mark.parametrize("index", [1, 2])
@pytest.mark.parametrize("change", ["name", "owner", "parameters", "cost", "record", "version"])
def test_native_configuration_cannot_bypass_the_existing_complete_target_guard(
    original_bindings: tuple[SimpleNamespace, ...], index: int, change: str
) -> None:
    source = binding_source(original_bindings)
    fact = original_facts(original_bindings)[index]
    target = fact.configuration.selection.target
    if change == "name":
        changed = target.model_copy(update={"name": "N 字形态"})
    elif change == "owner":
        changed = target.model_copy(update={"owner_id": "another-owner"})
    elif change == "parameters":
        changed = target.model_copy(update={"parameter_fingerprint": "f" * 64})
    elif change == "cost":
        changed = target.model_copy(update={"cost_fingerprint": "f" * 64})
    else:
        head = target.head.model_copy(
            update={"record_hash": "f" * 64} if change == "record" else {"version": 2}
        )
        changed = target.model_copy(update={"head": head})
    selection = fact.configuration.selection.model_copy(update={"target": changed})
    fact.configuration = fact.configuration.model_copy(update={"selection": selection})
    with pytest.raises(ValueError, match="candidate|target|cost"):
        source._statistics_inputs(
            original_bindings[0].configuration.selection.target, object(), (fact,)
        )
