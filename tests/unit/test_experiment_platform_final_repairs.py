from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from uuid import UUID

import pytest

from rquant.definition_registry import ImmutableDefinitionRegistry
from rquant.experiment_platform_commands import (
    CancelExperimentFamily,
    ExperimentCommandResult,
    ExperimentCommandWriter,
    ExperimentFamilyPreparer,
)
from rquant.experiment_platform_evidence import (
    _project_complete_result,
    compare_experiment_results,
    read_experiment_result,
)
from rquant.experiment_registry import ExperimentStatus
from rquant.lab_job_center import LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.portfolio_backtest_models import FrozenPortfolioInput
from rquant.portfolio_backtest_product import execute_portfolio_input
from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
from rquant.web.experiment_platform_service import ExperimentWebService
from rquant.web.lab_control_gateway import LabControlGateway
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, create_private_test_app
from tests.unit.test_experiment_platform import NOW, prepared_family
from tests.unit.test_experiment_platform_results import complete_family as complete_family


def test_m8_final01_original_c6_normalized_values_and_continuous_phases(complete_family) -> None:
    _, projection, reader, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    fact = snapshot.attempts[0]
    sealed = reader.read(
        fact.child.job_id,
        expected_result_hash=fact.result_hash,
        private_owner="alice",
        private_authority=projection.authority,
    )
    public = read_experiment_result(fact, family, results=reader, authority=projection.authority)
    assert tuple(p.nav for p in public.curves) == tuple(
        float(day.normalized_nav) for day in sealed.bundle.result.days
    )
    assert tuple(p.trade_date for p in public.curves) == tuple(
        day.trade_date for day in sealed.bundle.result.days
    )
    assert tuple(p for phase in public.phases for p in phase.curves) == public.curves
    assert public.phases[1].curves[0].nav == public.curves[2].nav


def test_m8_final02_rows_keep_full_original_metrics_and_null_terminal_rows(complete_family) -> None:
    _, projection, reader, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    facts = sorted(snapshot.attempts, key=lambda f: f.index)
    service = ExperimentWebService(results=reader, private_authority=projection.authority)
    row = service._row(facts[0], family)
    result = read_experiment_result(
        facts[0], family, results=reader, authority=projection.authority
    )
    assert row.metrics == result.metrics
    assert row.strategy_name == "组合回测" and row.strategy_version == 1
    if any(metric.value is None for metric in row.metrics):
        assert row.message == "部分指标暂不可计算。"
    for status in ("failed", "cancelled", "executed"):
        fact = facts[1].model_copy(
            update={
                "attempt": facts[1].attempt.model_copy(update={"status": ExperimentStatus(status)}),
                "result_hash": None,
            }
        )
        missing = service._row(fact, family)
        assert missing.index == facts[1].index and missing.message
        assert tuple(m.key for m in missing.metrics) == tuple(m.key for m in result.metrics)
        assert all(m.value is None for m in missing.metrics)
    with pytest.raises((ValueError, PermissionError)):
        service._row(facts[0].model_copy(update={"result_hash": "0" * 64}), family)
    with pytest.raises((ValueError, PermissionError)):
        service._row(facts[0].model_copy(update={"owner": "bob"}), family)


def test_m8_final01_different_cash_preserves_original_normalization_and_missing_breaks(
    complete_family, tmp_path: Path
) -> None:
    store, projection, reader, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    facts = sorted(snapshot.attempts, key=lambda f: f.index)
    original = store.preparation("alice", family.family_id, 0).prepared.frozen
    projected = []
    for fact, cash in zip(facts, (Decimal("3000"), Decimal("6000")), strict=False):
        frozen = FrozenPortfolioInput.model_validate(
            original.model_dump(mode="python")
            | {
                "config": original.config.model_copy(update={"initial_cash": cash}),
                "request": original.request.model_copy(update={"initial_cash": cash}),
                "input_hash": None,
            }
        )
        bundle = execute_portfolio_input(frozen, research_root=tmp_path)
        sealed = reader.read(
            fact.child.job_id,
            expected_result_hash=fact.result_hash,
            private_owner="alice",
            private_authority=projection.authority,
        )
        # The two new cash runs prove projection units. Their wrapper IDs are fixtures,
        # not a claim of two additional native jobs or newly sealed artifacts.
        args = dict(
            config=frozen.config,
            request=frozen.request,
            performance=bundle.performance,
            benchmark_series=bundle.benchmark,
            sources=frozen.sources,
            sealed=sealed,
            execution="portfolio-backtest@1",
        )
        public = _project_complete_result(fact, family, days=bundle.result.days, **args)
        assert tuple(p.nav for p in public.curves) == tuple(
            float(d.normalized_nav) for d in bundle.result.days
        )
        assert all(p.nav < 2 for p in public.curves)
        assert tuple(p for phase in public.phases for p in phase.curves) == public.curves
        for missing in (None, Decimal("NaN")):
            broken = (
                bundle.result.days[0].model_copy(update={"normalized_nav": missing}),
                *bundle.result.days[1:],
            )
            with pytest.raises(ValueError, match="normalized NAV is unavailable"):
                _project_complete_result(fact, family, days=broken, **args)
        projected.append(public)
    comparison = compare_experiment_results(*projected)
    assert not comparison.comparable
    assert any(d.path == "initial_cash" for d in comparison.differences)
    assert comparison.a.curves == projected[0].curves
    assert comparison.b.curves == projected[1].curves


def test_m8_final03_cancel_before_publication_public_receipt(tmp_path: Path) -> None:
    store, record, children, definitions = prepared_family(tmp_path)
    store.register_family_submission(
        owner=record.owner, request_id=record.request_id, children=children
    )
    jobs = LabJobStore(tmp_path / "jobs.sqlite")
    jobs.initialize()
    spool = LabCommandSpool(tmp_path / "cancel-spool")
    facade = LabCommandSubmissionFacade(
        reader=LabJobReader(jobs.path),
        spool=spool,
        experiment_registry=store.registry,
        definition_registry=definitions,
        clock=lambda: NOW,
    )
    writer = ExperimentCommandWriter(
        store=store,
        commands=facade,
        prepare=cast(ExperimentFamilyPreparer, SimpleNamespace(clock=lambda: NOW)),
        enabled=True,
        owners=frozenset({"alice"}),
    )
    command = CancelExperimentFamily(
        command_id=str(UUID(int=780032)),
        requested_at=NOW,
        actor_id="alice",
        family_id=record.family_id,
    )
    marker = writer.freeze(command)
    receipt = ExperimentCommandResult.model_validate(writer.submit(command, marker))
    assert receipt.status == "cancelled" and receipt.planned_count == 4
    assert all(
        store.child(child.intent.job_id).cancel_state == "before_publication" for child in children
    )
    assert spool.pending() == () and LabJobReader(jobs.path).list_jobs().items == ()
    assert len(store.registry.list_family_attempts(record.family_id)) == 4
    assert ExperimentCommandResult.model_validate(writer.recover(command, marker)) == receipt
    ExperimentWebService.verify_receipt(command, receipt)


@pytest.mark.parametrize(
    ("status", "message"),
    (
        ("already_completed", "实验已完成，结果仍保留。"),
        ("cancelled", "未完成项已取消，已有结果仍保留。"),
        ("cancellation_pending", "正在核对取消结果。"),
        ("already_finished", "实验已结束，历史记录仍保留。"),
    ),
)
def test_m8_final03_public_web_distinguishes_verified_terminal_and_pending_receipts(
    tmp_path: Path, status: str, message: str
) -> None:
    def transport(payload: dict[str, object]) -> dict[str, object]:
        assert payload["actor_id"] == "alice"
        result = ExperimentCommandResult.model_validate(
            dict(
                command_id=payload["command_id"],
                owner="alice",
                action=payload["kind"],
                family_id=payload["family_id"],
                status=status,
                planned_count=4,
            )
        )
        return dict(
            command_id=payload["command_id"],
            status="succeeded",
            enqueued_at=NOW,
            completed_at=NOW,
            result=result.model_dump(mode="json"),
            error=None,
        )

    app = create_private_test_app(
        WebSettings(serving_root=tmp_path / "serving"),
        clock=lambda: NOW,
        background=False,
        experiment_platform=ExperimentWebService(
            gateway=LabControlGateway(transport=transport), owners=frozenset({"alice"})
        ),
    )
    with ProofTestClient(app, headers={"x-rquant-user": "alice"}) as client:
        response = client.post(
            "/api/v1/experiments/commands",
            json=dict(
                kind="cancel_experiment_family",
                command_id=str(UUID(int=780033)),
                requested_at=NOW.isoformat(),
                family_id="experiment-search:message-fixture",
            ),
            headers={"x-rquant-csrf": "1", "origin": "http://testserver"},
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == status and response.json()["message"] == message


def test_m8_final03_original_completed_before_cancel_public_receipt(
    complete_family, tmp_path: Path
) -> None:
    store, projection, reader, _ = complete_family
    snapshot = projection.snapshot(NOW + timedelta(seconds=3))
    family = snapshot.families[0]
    spool = LabCommandSpool(tmp_path / "cancel-spool")
    facade = LabCommandSubmissionFacade(
        reader=reader.reader,
        spool=spool,
        experiment_registry=store.registry,
        definition_registry=ImmutableDefinitionRegistry(
            tmp_path / "definitions",
            execution_registry=BuiltinStrategyEvaluatorRegistry(
                producer_commit=store.preparation(
                    "alice", family.family_id, 0
                ).prepared.frozen.request.producer_commit
            ).trusted_executable_registry(),
        ),
        clock=lambda: NOW + timedelta(seconds=3),
    )
    prepare = cast(
        ExperimentFamilyPreparer, SimpleNamespace(clock=lambda: NOW + timedelta(seconds=3))
    )
    writer = ExperimentCommandWriter(
        store=store, commands=facade, prepare=prepare, enabled=True, owners=frozenset({"alice"})
    )
    command = CancelExperimentFamily(
        command_id=str(UUID(int=780031)),
        requested_at=NOW + timedelta(seconds=3),
        actor_id="alice",
        family_id=family.family_id,
    )
    marker = writer.freeze(command)
    receipt = ExperimentCommandResult.model_validate(writer.submit(command, marker))
    assert receipt.status == "already_completed"
    assert all(
        store.child(f.child.job_id).cancel_state == "already_completed" for f in snapshot.attempts
    )
    assert all(
        a.status.value == "executed" for a in store.registry.list_family_attempts(family.family_id)
    )
    assert spool.pending() == ()
    assert ExperimentCommandResult.model_validate(writer.recover(command, marker)) == receipt
    ExperimentWebService.verify_receipt(command, receipt)
