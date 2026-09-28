"""Durable formula market tasks use one source pair and one verifiable result."""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from rquant.screen.formula_history_projection import FormulaProjectionBudgetError
from rquant.screen.formula_market_jobs import (
    FormulaMarketArtifactUnavailableError,
    FormulaMarketJobRequest,
    FormulaMarketJobResult,
    FormulaMarketJobStore,
    FormulaMarketJobWorker,
)
from rquant.screen.formula_market_run import (
    FormulaMarketRunBudgetError,
    FormulaMarketRunTimeoutError,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_market_run import DAY, DECISION_AT, _history, _market, _run


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 4, 16, 9, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def _request(
    market: tuple[Path, str],
    history: tuple[Path, str],
    *,
    key: str = "formula-market-0001",
    formula: str = "CLOSE>2",
) -> FormulaMarketJobRequest:
    return FormulaMarketJobRequest(
        idempotency_key=key,
        formula=formula,
        trade_date=DAY,
        decision_at=DECISION_AT,
        universe_root=market[0],
        projection_root=history[0],
        expected_universe_sha256=market[1],
        expected_projection_identity=history[1],
    )


def _store(tmp_path: Path, clock: Clock, *, lease_seconds: int = 180) -> FormulaMarketJobStore:
    return FormulaMarketJobStore(
        state_path=tmp_path / "state" / "formula-jobs.sqlite",
        artifact_directory=tmp_path / "results",
        clock=clock,
        lease_seconds=lease_seconds,
    )


def test_real_worker_persists_complete_result_across_restart(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    clock = Clock()
    store = _store(tmp_path, clock)
    queued = store.submit(_request(market, history))

    assert (queued.status, queued.attempts, queued.result_sha256) == ("queued", 0, None)
    completed = FormulaMarketJobWorker(store).run_one()

    assert completed is not None
    assert (completed.task_id, completed.status, completed.attempts) == (
        queued.task_id,
        "succeeded",
        1,
    )
    restarted = _store(tmp_path, clock)
    assert restarted.status(queued.task_id) == completed
    result = restarted.read_result(queued.task_id)
    assert result.task_id == queued.task_id
    assert result.summary.market_total == 4
    counts = (
        result.summary.match_count,
        result.summary.no_match_count,
        result.summary.unknown_count,
    )
    assert counts == (
        2,
        1,
        1,
    )
    assert result.summary.match_codes == ("000001.SZ", "830001.BJ")
    assert result.summary.universe_identity == market[1]
    assert result.summary.projection_identity == history[1]
    assert completed.result_sha256 == result.content_sha256
    assert os.stat(tmp_path / "results").st_mode & 0o777 == 0o700
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in (tmp_path / "results").iterdir())


def test_historical_trade_date_can_run_after_sources_become_available(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    store = _store(tmp_path, Clock())
    request = _request(market, history).model_copy(
        update={"decision_at": DECISION_AT + timedelta(days=1)}
    )

    task = store.submit(request)
    receipt = FormulaMarketJobWorker(store).run_one()

    assert receipt is not None and receipt.status == "succeeded"
    assert store.read_result(task.task_id).summary.decision_at == request.decision_at


def test_submit_rejects_decision_before_daily_bar_is_visible(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    request = _request(market, history)
    values = request.model_dump(mode="python") | {
        "decision_at": datetime(2026, 4, 15, 16, tzinfo=ZoneInfo("Asia/Shanghai"))
    }

    with pytest.raises(ValueError, match="日线|17:00|收盘"):
        FormulaMarketJobRequest.model_validate(values)


def test_same_key_is_idempotent_but_changed_request_and_second_active_are_rejected(
    tmp_path: Path,
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    store = _store(tmp_path, Clock())
    first = _request(market, history)
    receipt = store.submit(first)

    assert store.submit(first) == receipt
    with pytest.raises(ValueError, match="idempotency"):
        store.submit(_request(market, history, formula="CLOSE>1"))
    with pytest.raises(ValueError, match="active"):
        store.submit(_request(market, history, key="formula-market-0002"))
    assert store.latest() == receipt


def test_concurrent_submit_allows_only_one_active_task(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    clock = Clock()
    first, second = _store(tmp_path, clock), _store(tmp_path, clock)
    requests = (
        (first, _request(market, history, key="formula-market-0001")),
        (second, _request(market, history, key="formula-market-0002")),
    )

    def submit(item: tuple[FormulaMarketJobStore, FormulaMarketJobRequest]) -> object:
        try:
            return item[0].submit(item[1])
        except ValueError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(submit, requests))

    assert sum(not isinstance(item, Exception) for item in results) == 1
    assert sum(isinstance(item, ValueError) and "active" in str(item) for item in results) == 1


def test_expired_claim_retries_and_fences_stale_worker(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    clock = Clock()
    store = _store(tmp_path, clock, lease_seconds=3)
    task = store.submit(_request(market, history))
    stale = store._claim()
    assert stale is not None

    clock.now += timedelta(seconds=4)
    current = store._claim()
    assert current is not None and current.token != stale.token
    assert current.attempts == 2
    with pytest.raises(RuntimeError, match="lease"):
        store._finish_failure(stale, "internal_error")
    assert store.status(task.task_id).status == "running"

    assert store._renew(current)
    assert store._finish_failure(current, "internal_error").status == "failed"
    assert store.status(task.task_id).attempts == 2


def test_restart_recovers_linked_result_left_before_commit(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    clock = Clock()
    store = _store(tmp_path, clock, lease_seconds=3)
    request = _request(market, history)
    task = store.submit(request)
    stale = store._claim()
    assert stale is not None
    result = FormulaMarketJobResult.create(
        task_id=task.task_id,
        request_sha256=stale.request_sha256,
        formula_sha256=hashlib.sha256(request.formula.encode()).hexdigest(),
        summary=_run(market, history),
    )
    result_dir = tmp_path / "results"
    stage = result_dir / f".{stale.task_id}.{stale.token}.stage"
    stage.write_bytes(canonical_json_bytes(result.model_dump(mode="json")))
    stage.chmod(0o600)
    artifact = result_dir / store._artifact_name(task.task_id, result.content_sha256)
    os.link(stage, artifact)
    assert artifact.stat().st_nlink == 2

    clock.now += timedelta(seconds=4)
    restarted = _store(tmp_path, clock, lease_seconds=3)
    receipt = FormulaMarketJobWorker(restarted).run_one()

    assert receipt is not None and receipt.status == "succeeded"
    assert receipt.attempts == 2
    assert not stage.exists()
    assert artifact.stat().st_nlink == 1
    assert restarted.read_result(task.task_id).content_sha256 == result.content_sha256


def test_source_generation_change_fails_without_result(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    clock = Clock()
    store = _store(tmp_path, clock)
    task = store.submit(_request(market, history))
    _history(tmp_path, name="b" * 32)

    receipt = FormulaMarketJobWorker(store).run_one()

    assert receipt is not None
    assert (receipt.status, receipt.error_code, receipt.result_sha256) == (
        "failed",
        "source_changed",
        None,
    )
    with pytest.raises(ValueError, match="succeeded"):
        store.read_result(task.task_id)
    assert not list((tmp_path / "results").glob("*.json"))


def test_timeout_fails_whole_task_without_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    store = _store(tmp_path, Clock())
    task = store.submit(_request(market, history))

    def timed_out(*args: object, **kwargs: object) -> None:
        raise FormulaMarketRunTimeoutError("budget elapsed")

    monkeypatch.setattr("rquant.screen.formula_market_jobs.run_formula_market", timed_out)
    receipt = FormulaMarketJobWorker(store).run_one()

    assert receipt is not None
    assert (receipt.status, receipt.error_code) == ("failed", "timeout")
    assert store.status(task.task_id).result_sha256 is None
    assert not list((tmp_path / "results").glob("*.json"))


@pytest.mark.parametrize("failure", [FormulaProjectionBudgetError, FormulaMarketRunBudgetError])
def test_capacity_failure_is_finite_and_has_no_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: type[Exception]
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    store = _store(tmp_path, Clock())
    task = store.submit(_request(market, history))

    def out_of_capacity(*args: object, **kwargs: object) -> None:
        raise failure("source capacity exceeded")

    monkeypatch.setattr("rquant.screen.formula_market_jobs.run_formula_market", out_of_capacity)
    receipt = FormulaMarketJobWorker(store).run_one()

    assert receipt is not None
    assert (receipt.status, receipt.error_code) == ("failed", "capacity")
    assert store.status(task.task_id).result_sha256 is None
    assert not list((tmp_path / "results").glob("*.json"))


@pytest.mark.parametrize("damage", ["delete", "mutate", "symlink", "hardlink"])
def test_success_status_and_result_fail_closed_on_artifact_damage(
    tmp_path: Path, damage: str
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    store = _store(tmp_path, Clock())
    task = store.submit(_request(market, history))
    assert FormulaMarketJobWorker(store).run_one() is not None
    artifact = next((tmp_path / "results").glob("*.json"))
    if damage == "delete":
        artifact.unlink()
    elif damage == "mutate":
        artifact.write_bytes(artifact.read_bytes().replace(b"000001.SZ", b"000002.SZ"))
    elif damage == "symlink":
        original = artifact.with_suffix(".copy")
        artifact.rename(original)
        artifact.symlink_to(original)
    else:
        os.link(artifact, artifact.with_suffix(".copy"))

    with pytest.raises(FormulaMarketArtifactUnavailableError):
        store.status(task.task_id)
    with pytest.raises(FormulaMarketArtifactUnavailableError):
        store.read_result(task.task_id)


@pytest.mark.parametrize(
    "change",
    [
        {"decision_at": datetime(2026, 4, 15, 17, 15)},
        {"trade_date": date(2026, 4, 16)},
        {"expected_universe_sha256": "BAD"},
        {"idempotency_key": "short"},
        {"formula": "CLOSE>2;DROP TABLE"},
    ],
)
def test_request_rejects_unbounded_or_invalid_input(
    tmp_path: Path, change: dict[str, object]
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    values = _request(market, history).model_dump(mode="python") | change
    with pytest.raises(ValueError):
        FormulaMarketJobRequest.model_validate(values)
