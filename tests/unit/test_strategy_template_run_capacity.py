"""The bounded recent-result reader stays publishable after run admission."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from pathlib import Path
from threading import Barrier
from uuid import uuid4

import pytest

from rquant import strategy_authoring as authoring
from rquant.experiment_registry import FormalExperimentPlan
from rquant.strategy_authoring import StrategyAuthoringConflict, StrategyAuthoringStore
from rquant.strategy_template_adapter import StrategyTemplateAdapterCatalog
from rquant.strategy_template_run import FrozenStrategyTemplateInput
from rquant.strategy_template_run_commands import AcceptedStrategyTemplateRun, RunStrategyTemplate
from tests.unit.test_strategy_template_adapter import adapter_fixture, template_spec
from tests.unit.test_strategy_template_submission import run_service


def make_run(
    target: StrategyAuthoringStore,
    value: FrozenStrategyTemplateInput,
    catalog: StrategyTemplateAdapterCatalog,
) -> AcceptedStrategyTemplateRun:
    head = catalog.versions[0].head
    command_id = str(uuid4())
    now = value.definition.available_at + timedelta(seconds=1)
    request = RunStrategyTemplate(
        command_id=command_id,
        requested_at=now,
        generation_id="generation-a",
        strategy_id=value.definition.logical_id,
        head=head,
        expected_head=head,
        start_date=value.request.days[0].trade_date,
        end_date=value.request.days[-1].trade_date,
        initial_cash=value.request.initial_cash,
    )
    spec = template_spec(value, request_id=command_id)
    plan = FormalExperimentPlan(
        schema_version=2,
        spec=spec.experiment.spec,
        hypothesis_variant=spec.experiment.hypothesis_variant,
        strategy_definition_fingerprint=head.registration_fingerprint,
        definition_registration_record_hash=head.record_hash,
        preregistered_at=value.definition.available_at,
    )
    return AcceptedStrategyTemplateRun(
        owner_id="alice",
        request=request,
        metadata_identity=target.identity(),
        accepted_at=now,
        spec=spec,
        plan=plan,
    )


@pytest.mark.parametrize("writer", ("typed", "legacy"))
def test_new_runs_refuse_global_capacity_but_original_retry_precedes_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    writer: str,
) -> None:
    monkeypatch.setattr(authoring, "MAX_TEMPLATE_RUN_ADMISSIONS", 2, raising=False)
    target, value, catalog, _ = adapter_fixture(tmp_path)
    first, second, third = (make_run(target, value, catalog) for _ in range(3))

    def commit(accepted: AcceptedStrategyTemplateRun) -> object:
        if writer == "typed":
            return target.commit_run_admission(accepted)
        return target.admit_run(
            accepted.request.strategy_id,
            accepted.request.head,
            owner_id=accepted.owner_id,
            command_id=accepted.request.command_id,
            request_hash=accepted.request.request_hash,
            spec_hash=accepted.spec.spec_hash,
        )

    original = commit(first)
    commit(second)
    with pytest.raises(StrategyAuthoringConflict, match="run.*budget"):
        target.verify_new_run(third.request, owner_id="alice", expected_identity=target.identity())
    with pytest.raises(StrategyAuthoringConflict, match="run.*budget"):
        commit(third)
    assert commit(first) == original
    with target._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM run_admissions").fetchone()[0] == 2


def test_budget_refuses_before_source_preparation_but_recovers_original_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(authoring, "MAX_TEMPLATE_RUN_ADMISSIONS", 2, raising=False)
    target, _, backend, request, _ = run_service(tmp_path)
    owned = backend.compile(request, owner_id="alice", expected_identity=target.identity())
    target.admit_run(
        request.strategy_id,
        request.head,
        owner_id="alice",
        command_id=str(uuid4()),
        request_hash="b" * 64,
        spec_hash=owned.accepted.spec.spec_hash,
    )

    def no_new_source(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("capacity refusal must precede trusted source preparation")

    monkeypatch.setattr(backend.preparer, "prepare", no_new_source)
    new = request.model_copy(update={"command_id": str(uuid4())})
    with pytest.raises(StrategyAuthoringConflict, match="run.*budget"):
        backend.compile(new, owner_id="alice", expected_identity=target.identity())
    assert backend.compile(request, owner_id="alice", expected_identity=target.identity()) == owned


def test_two_preflighted_runs_share_one_transactional_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(authoring, "MAX_TEMPLATE_RUN_ADMISSIONS", 2, raising=False)
    target, value, catalog, _ = adapter_fixture(tmp_path)
    first, second, third = (make_run(target, value, catalog) for _ in range(3))
    target.commit_run_admission(first)
    barrier = Barrier(2)

    def attempt(accepted: AcceptedStrategyTemplateRun) -> str:
        target.verify_new_run(
            accepted.request, owner_id="alice", expected_identity=target.identity()
        )
        barrier.wait(timeout=5)
        try:
            target.commit_run_admission(accepted)
        except StrategyAuthoringConflict as error:
            assert "budget" in str(error)
            return "refused"
        return "accepted"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = tuple(executor.map(attempt, (second, third)))
    assert sorted(results) == ["accepted", "refused"]
    assert target.commit_run_admission(first) == first
    with target._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM run_admissions").fetchone()[0] == 2
