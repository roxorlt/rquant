"""Focused proofs for the remaining exact input and original recovery boundaries."""

from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import duckdb
import pytest

from rquant.lab_job_center import ExperimentLifecycleCoordinator
from rquant.lab_scheduler import LabScheduler
from rquant.page_control import PageControlStatus
from rquant.portfolio.drawdown import DrawdownRule
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_template_adapter import write_strategy_template_input
from rquant.strategy_template_run import (
    FrozenStrategyTemplateInput,
    execute_strategy_template_input,
)
from rquant.strategy_template_source import verify_template_snapshot_source
from tests.unit.test_portfolio_backtest import _CODES, _at, _request
from tests.unit.test_strategy_authoring_page_control import submit
from tests.unit.test_strategy_template_adapter import adapter_fixture
from tests.unit.test_strategy_template_run import frozen
from tests.unit.test_strategy_template_submission import run_service, source_catalog


def test_disjoint_original_ranking_and_raw_facts_share_one_code_budget(tmp_path: Path) -> None:
    value = frozen(tmp_path)
    data = value.model_dump(mode="python")
    ranking = tuple(f"{600000 + index}.SH" for index in range(250))
    rows = tuple(f"{601000 + index}.SH" for index in range(251))
    assert len(set(ranking) | set(rows)) == 501
    data["request"]["days"][0]["ranking"]["candidates"] = tuple(
        {"ts_code": code, "rank_score": "0"} for code in ranking
    )
    data["days"][0]["entry"]["evidence"]["rows"] = tuple(
        {"ts_code": code, "is_st": True} for code in rows
    )
    data["input_hash"] = None
    with pytest.raises(ValueError, match="code.*budget"):
        FrozenStrategyTemplateInput.model_validate(data)


def test_snapshot_cutoff_rejects_future_close_after_definition_was_available(
    tmp_path: Path,
) -> None:
    request = _request((_CODES[0],))
    registered_at = _at(request.days[0].trade_date, 9, 0) - timedelta(days=1)
    target = StrategyAuthoringStore(
        tmp_path / "metadata.sqlite",
        definition_root=tmp_path / "definitions",
        producer_commit=request.producer_commit,
        clock=lambda: registered_at,
    )
    target.initialize()
    _, value, catalog, _ = adapter_fixture(tmp_path, existing_store=target)
    cutoff = _at(value.request.days[-1].trade_date, 15, 0) - timedelta(seconds=1)
    assert value.definition.available_at < cutoff
    with duckdb.connect(":memory:") as connection:
        write_strategy_template_input(connection, value)
        with pytest.raises(ValueError, match="future"):
            verify_template_snapshot_source(
                connection,
                version=catalog.versions[0],
                code_sha=request.producer_commit,
                start_date=value.request.days[0].trade_date,
                end_date=value.request.days[-1].trade_date,
                input_hash=value.input_hash,
                as_of=cutoff,
            )


def test_original_risk_cap_reduces_the_actual_broker_position(tmp_path: Path) -> None:
    value = frozen(tmp_path, prices=("10", "8", "8"))
    request = value.request.model_copy(
        update={
            "drawdown_rule": DrawdownRule(
                trigger_drawdown="0.05",
                action="cap_total_risk_weight",
                total_risk_weight_cap="0",
            )
        }
    )
    value = FrozenStrategyTemplateInput.model_validate(
        {
            **value.model_dump(mode="python"),
            "request": request,
            "input_hash": None,
        }
    )
    result = execute_strategy_template_input(value, research_root=tmp_path)
    assert result.days[2].risk.state.active
    assert result.days[2].risk.max_total_risk_weight == Decimal("0")
    assert [order.intent.side.value for order in result.days[2].orders] == ["SELL"]
    # 3000 - 100*10 - 5 + 100*8 - 5 - 0.8 = 2789.20.
    assert result.days[2].account.cash == Decimal("2789.20")
    assert result.days[2].account.holdings == ()


def test_original_lab_consumed_spool_recovers_without_republishing_or_new_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, service, backend, request, jobs = run_service(tmp_path)
    complete = target.complete_run_receipt

    def interrupt(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("after original Lab publication before metadata receipt")

    monkeypatch.setattr(target, "complete_run_receipt", interrupt)
    receipt = submit(service, target, request)
    assert receipt.status is PageControlStatus.PENDING
    assert len(backend.facade.spool.pending()) == 1
    scheduler = LabScheduler(
        store=jobs,
        spool=backend.facade.spool,
        owner_id="synthetic-template-recovery",
        lease_seconds=60,
        heartbeat_seconds=10,
        poll_interval_ms=5,
        template_directory=backend.facade.template_directory,
        clock=target.clock,
        lifecycle_synchronizer=ExperimentLifecycleCoordinator(backend.facade),
    )
    try:
        tick = scheduler.run_once()
        assert tick.plans_created == 1 and tick.plans_failed == 0
        assert backend.facade.reader.get_job(UUID(request.command_id)) is not None
        assert backend.facade.spool.pending() == ()
        monkeypatch.setattr(target, "complete_run_receipt", complete)
        monkeypatch.setattr(
            backend.preparer,
            "prepare",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("original retry read fresh source")
            ),
        )
        recovered = submit(service, target, request, sources=source_catalog(generation="changed"))
        assert recovered.status is PageControlStatus.SUCCEEDED
        assert recovered.result["job_id"] == request.command_id
        assert backend.facade.spool.pending() == ()
    finally:
        scheduler.release()
