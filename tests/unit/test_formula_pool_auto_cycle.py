"""A short owner-private cycle only trusts complete daily pool evidence."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path
from threading import Barrier

import pytest

import rquant.formula_pool_auto_cycle as cycle_module
import rquant.screen.formula_market_jobs as jobs_module
from rquant.formula_pool_batch import FormulaPoolBatchPrivateConfig
from rquant.formula_pool_daily import _run_identity
from rquant.screen.formula_market_jobs import FormulaMarketJobRequest
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_pool_batch import _expand_catalog, _fixture
from tests.unit.test_formula_pool_daily_recalc import DAY, NOW, _publish_market_day


def _cycle(
    config: FormulaPoolBatchPrivateConfig,
    *,
    clock: Callable[[], datetime] = lambda: NOW,
) -> cycle_module.FormulaPoolAutoCycle:
    return cycle_module.FormulaPoolAutoCycle(config=config, clock=clock)


def test_auto_cycle_runs_two_real_pool_tasks_and_rechecks_each(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    result = cycle.run(DAY, max_worker_calls=2)
    assert result.worker_calls == 2
    assert result.day.trade_date == DAY
    assert result.day.total_count == result.day.completed_count == 2
    assert result.day.all_complete
    assert all(item.daily is not None for item in result.day.pools)
    assert result.day == cycle.coordinator.run_day(DAY)
    retry = cycle.run(DAY, max_worker_calls=1)
    assert retry.worker_calls == 0
    assert retry.day == result.day


def test_auto_cycle_call_limit_and_next_invocation_converge(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    first = cycle.run(DAY, max_worker_calls=1)
    assert first.worker_calls == 1
    assert first.day.completed_count == first.day.waiting_count == 1
    assert not first.day.all_complete
    second = cycle.run(DAY, max_worker_calls=1)
    assert second.worker_calls == 1
    assert second.day.completed_count == 2 and second.day.all_complete
    for invalid in (0, 9, True):
        with pytest.raises(ValueError, match="worker calls"):
            cycle.run(DAY, max_worker_calls=invalid)


def test_auto_cycle_stops_at_eight_real_worker_calls(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    _expand_catalog(batch, 10)
    cycle = _cycle(batch.config)
    first = cycle.run(DAY, max_worker_calls=8)
    assert first.worker_calls == first.day.completed_count == 8
    assert first.day.waiting_count == 2 and not first.day.all_complete
    final = cycle.run(DAY, max_worker_calls=2)
    assert final.worker_calls == 2
    assert final.day.completed_count == 10 and final.day.all_complete


def test_auto_cycle_empty_catalog_and_unclosed_day_never_run_worker(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    for path in batch.config.definition_root.iterdir():
        path.unlink()
    cycle = _cycle(batch.config)
    assert (
        cycle.worker.store.state_path,
        cycle.worker.store.artifact_directory,
        cycle.worker.trusted_source_roots,
    ) == (
        batch.config.market.state_path,
        batch.config.market.artifact_directory,
        (batch.config.market.universe_root, batch.config.market.projection_root),
    )
    result = cycle.run(DAY)
    assert result.worker_calls == 0 and result.day.all_complete
    assert result.day.total_count == 0
    previous = admission.store.latest()
    with pytest.raises(ValueError, match="closed"):
        cycle.run(date(2026, 4, 18))
    assert admission.store.latest() == previous
    cycle.worker.trusted_source_roots = (batch.config.market.universe_root, tmp_path)
    with pytest.raises(ValueError, match="worker|config"):
        cycle.run(DAY)


def test_auto_cycle_rechecks_after_worker_none_exhausts_lease(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    now = [NOW]
    cycle = _cycle(batch.config, clock=lambda: now[0])
    assert cycle.coordinator.run_day(DAY).waiting_count == 2
    store = cycle.coordinator.runner.task_store
    for _ in range(3):
        assert store._claim() is not None
        now[0] += timedelta(seconds=store.lease_seconds + 1)
    result = cycle.run(DAY, max_worker_calls=1)
    assert result.worker_calls == 1
    assert result.day.failed_count == result.day.waiting_count == 1
    assert result.day.pools[0].error_code == "lease_exhausted"
    assert not result.day.all_complete


def test_auto_cycle_other_research_task_does_not_count_as_pool_work(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    universe, projection, _ = cycle.coordinator.runner._sources(DAY)
    unrelated = cycle.coordinator.runner.task_store.submit(
        FormulaMarketJobRequest(
            idempotency_key="research-unrelated-0001",
            formula="CLOSE>2",
            trade_date=DAY,
            decision_at=NOW,
            universe_root=batch.config.market.universe_root,
            projection_root=batch.config.market.projection_root,
            expected_universe_sha256=universe,
            expected_projection_identity=projection,
        )
    )
    first = cycle.run(DAY, max_worker_calls=1)
    assert first.worker_calls == 1
    assert first.day.completed_count == 0 and first.day.waiting_count == 2
    assert not first.day.all_complete
    assert cycle.coordinator.runner.task_store.status(unrelated.task_id).status == "succeeded"
    assert cycle.run(DAY, max_worker_calls=2).day.all_complete


def test_auto_cycle_worker_failure_remains_failed_not_complete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    def fail_task(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("synthetic task failure")

    monkeypatch.setattr(jobs_module, "run_formula_market", fail_task)
    result = cycle.run(DAY, max_worker_calls=1)
    assert result.worker_calls == 1
    assert result.day.failed_count == result.day.waiting_count == 1
    assert result.day.pools[0].error_code == "internal_error"
    assert not result.day.all_complete


def test_auto_cycle_rejects_observed_source_shift_after_worker(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    original = cycle.worker.run_one

    def run_then_shift() -> object:
        receipt = original()
        _publish_market_day(batch.config.market.universe_root, DAY, minute=6)
        return receipt

    cycle.worker.run_one = run_then_shift  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="identity|sources"):
        cycle.run(DAY, max_worker_calls=1)


def test_auto_cycle_rejects_worker_window_source_change_even_after_return(
    tmp_path: Path,
) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    original = cycle.worker.run_one

    def run_during_shift() -> object:
        _publish_market_day(batch.config.market.universe_root, DAY, minute=6)
        receipt = original()
        _publish_market_day(batch.config.market.universe_root, DAY, minute=5)
        return receipt

    cycle.worker.run_one = run_during_shift  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="source|identity"):
        cycle.run(DAY, max_worker_calls=1)


def test_auto_cycle_rejects_formula_pool_task_from_other_source(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    definition = batch.definitions.read("alpha")
    cycle.coordinator.runner.task_store.submit(
        FormulaMarketJobRequest(
            idempotency_key=_run_identity(
                definition.pool_name, definition.version, DAY, "b" * 64, "c" * 64
            ),
            formula=definition.formula,
            trade_date=DAY,
            decision_at=NOW,
            universe_root=batch.config.market.universe_root,
            projection_root=batch.config.market.projection_root,
            expected_universe_sha256="b" * 64,
            expected_projection_identity="c" * 64,
        )
    )
    with pytest.raises(ValueError, match="source|identity"):
        cycle.run(DAY, max_worker_calls=1)


def test_auto_cycle_rejects_pool_key_with_different_saved_formula(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    definition = batch.definitions.read("alpha")
    universe, projection, _ = cycle.coordinator.runner._sources(DAY)
    forged = cycle.coordinator.runner.task_store.submit(
        FormulaMarketJobRequest(
            idempotency_key=_run_identity(
                definition.pool_name, definition.version, DAY, universe, projection
            ),
            formula="CLOSE>999",
            trade_date=DAY,
            decision_at=NOW,
            universe_root=batch.config.market.universe_root,
            projection_root=batch.config.market.projection_root,
            expected_universe_sha256=universe,
            expected_projection_identity=projection,
        )
    )
    with pytest.raises(ValueError, match="formula|identity"):
        cycle.run(DAY, max_worker_calls=1)
    assert cycle.coordinator.runner.task_store.status(forged.task_id).status == "succeeded"


def test_auto_cycle_rejects_pool_key_with_untrusted_source_roots(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycle = _cycle(batch.config)
    definition = batch.definitions.read("alpha")
    universe, projection, _ = cycle.coordinator.runner._sources(DAY)
    cycle.coordinator.runner.task_store.submit(
        FormulaMarketJobRequest(
            idempotency_key=_run_identity(
                definition.pool_name, definition.version, DAY, universe, projection
            ),
            formula=definition.formula,
            trade_date=DAY,
            decision_at=NOW,
            universe_root=tmp_path / "untrusted-universe",
            projection_root=batch.config.market.projection_root,
            expected_universe_sha256=universe,
            expected_projection_identity=projection,
        )
    )
    with pytest.raises(ValueError, match="identity"):
        cycle.run(DAY, max_worker_calls=1)


def test_auto_cycle_concurrent_calls_keep_canonical_daily_results(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    cycles = (_cycle(batch.config), _cycle(batch.config))
    barrier = Barrier(2)

    def run(cycle: cycle_module.FormulaPoolAutoCycle) -> cycle_module.FormulaPoolAutoCycleResult:
        barrier.wait(timeout=10)
        return cycle.run(DAY, max_worker_calls=4)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = tuple(pool.map(run, cycles))
    assert all(
        not item.day.all_complete or item.day.completed_count == item.day.total_count == 2
        for item in outcomes
    )
    final = _cycle(batch.config).run(DAY, max_worker_calls=4)
    assert final.day.all_complete
    assert final.day == _cycle(batch.config).coordinator.run_day(DAY)
    assert all(
        not item.day.all_complete or item.day == final.day
        for item in outcomes
    )


def test_auto_cycle_cli_outputs_only_final_day_and_safe_exit_codes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    batch, _, _ = _fixture(tmp_path)
    config_path = tmp_path / "batch.json"
    config_path.write_bytes(canonical_json_bytes(batch.config.model_dump(mode="json")))
    os.chmod(config_path, 0o600)
    args = ["--config", str(config_path), "--trade-date", DAY.isoformat()]
    assert cycle_module.main([*args, "--max-worker-calls", "1"]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["worker_calls"] == 1
    assert output["day"]["trade_date"] == DAY.isoformat()
    assert output["day"]["all_complete"] is False
    assert cycle_module.main([*args, "--max-worker-calls", "0"]) == 2
    assert not capsys.readouterr().out
    assert cycle_module.main(["--config", str(config_path), "--trade-date", "2026-04-18"]) == 1
    assert not capsys.readouterr().out
