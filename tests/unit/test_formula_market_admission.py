"""Only trusted PageControl may bind and enqueue a historical formula market run."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

import rquant.formula_market_page_backend as admission_module
from rquant.formula_market_page_backend import (
    FormulaMarketPageBackend,
    FormulaMarketPageBackendConfig,
)
from rquant.page_control import (
    PageControlCommandConflictError,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SubmitFormulaMarketRun,
    parse_page_control_command,
)
from rquant.page_control_service import build_page_control_service
from rquant.screen.formula_market_universe import (
    FormulaMarketUniverseError,
    peek_formula_market_universe_identity,
)
from tests.unit.test_formula_market_run import DAY, SHANGHAI, _history, _market

NOW = datetime(2026, 4, 16, 18, tzinfo=SHANGHAI)


def _command(
    command_id: str = "formula-market-0001", *, formula: str = "CLOSE>2"
) -> SubmitFormulaMarketRun:
    return SubmitFormulaMarketRun(
        command_id=command_id,
        requested_at=NOW,
        actor_id="researcher",
        formula=formula,
        trade_date=DAY,
    )


def _backend(
    tmp_path: Path,
    market: tuple[Path, str],
    history: tuple[Path, str],
    *,
    now: datetime = NOW,
) -> FormulaMarketPageBackend:
    return FormulaMarketPageBackend(
        FormulaMarketPageBackendConfig(
            universe_root=market[0],
            projection_root=history[0],
            state_path=tmp_path / "state" / "formula-jobs.sqlite",
            artifact_directory=tmp_path / "results",
        ),
        clock=lambda: now,
    )


def _service(
    tmp_path: Path, backend: FormulaMarketPageBackend | None, *, now: datetime = NOW
) -> PageControlService:
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "page-data",
            log_dir=tmp_path / "page-logs",
            formula_market_backend=backend,
            clock=lambda: now,
            lease_seconds=1,
        ),
    )


def test_pointer_probe_is_bounded_and_does_not_open_full_generation(tmp_path: Path) -> None:
    market = _market(tmp_path)
    assert peek_formula_market_universe_identity(market[0], DAY) == market[1]
    generation = market[0] / DAY.isoformat() / "generations" / f"{market[1]}.json"
    generation.unlink()
    assert peek_formula_market_universe_identity(market[0], DAY) == market[1]

    pointer = market[0] / DAY.isoformat() / "current.json"
    pointer.write_bytes(b"x" * 2048)
    with pytest.raises(FormulaMarketUniverseError):
        peek_formula_market_universe_identity(market[0], DAY)


def test_command_accepts_only_browser_input_and_server_actor(tmp_path: Path) -> None:
    command = _command()
    payload = command.model_dump(mode="json")
    assert parse_page_control_command(payload) == command
    for field, value in (
        ("universe_root", str(tmp_path / "market")),
        ("projection_root", str(tmp_path / "history")),
        ("state_path", str(tmp_path / "state.sqlite")),
        ("expected_universe_sha256", "a" * 64),
        ("decision_at", NOW.isoformat()),
    ):
        with pytest.raises(ValidationError):
            parse_page_control_command({**payload, field: value})


def test_page_control_binds_two_sources_and_audits_actor_without_running_formula(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    backend = _backend(tmp_path, market, history)

    def must_not_run(*_args: object, **_kwargs: object) -> None:
        pytest.fail("admission ran the market formula")

    monkeypatch.setattr("rquant.screen.formula_market_jobs.run_formula_market", must_not_run)
    first = _service(tmp_path, backend).submit(_command())
    replay = _service(tmp_path, backend).submit(_command())

    assert first == replay
    assert first.status is PageControlStatus.SUCCEEDED
    assert first.result is not None and first.result["outcome"] == "task_queued"
    task_id = first.result["task_id"]
    assert backend.store.status(task_id).status == "queued"
    admitted = backend.store.admission_by_key(backend.idempotency_key(_command()))
    assert admitted is not None
    request, recovered_id = admitted
    assert recovered_id == task_id
    assert request.formula == "CLOSE>2"
    assert request.trade_date == DAY
    assert request.decision_at == NOW.astimezone(UTC)
    assert (request.universe_root, request.projection_root) == (market[0], history[0])
    assert (request.expected_universe_sha256, request.expected_projection_identity) == (
        market[1], history[1]
    )
    with sqlite3.connect(tmp_path / "page-control.sqlite") as connection:
        stored = connection.execute(
            "SELECT payload_json FROM page_control_command WHERE command_id = ?",
            (_command().command_id,),
        ).fetchone()
    assert stored is not None and json.loads(stored[0])["actor_id"] == "researcher"


def test_historical_open_day_need_not_be_in_recent_projection_dates(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    pointer = history[0] / "current.json"
    manifest = json.loads(pointer.read_text())
    manifest["dates"] = []
    pointer.write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")))

    backend = _backend(tmp_path, market, history)
    receipt = _service(tmp_path, backend).submit(_command())

    assert receipt.status is PageControlStatus.SUCCEEDED
    assert receipt.result is not None and receipt.result["outcome"] == "task_queued"


def test_lost_receipt_recovers_original_task_after_both_sources_rotate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    backend = _backend(tmp_path, market, history)
    actual_submit = backend.submit

    def lose_receipt(command: SubmitFormulaMarketRun) -> object:
        actual_submit(command)
        raise KeyboardInterrupt("after queue")

    with monkeypatch.context() as patch:
        patch.setattr(backend, "submit", lose_receipt)
        with pytest.raises(KeyboardInterrupt, match="after queue"):
            _service(tmp_path, backend).submit(_command())

    admitted = backend.store.admission_by_key(backend.idempotency_key(_command()))
    assert admitted is not None
    original_task_id = admitted[1]
    _market(tmp_path, completed_at=NOW - timedelta(days=1, minutes=50))
    _history(tmp_path, name="b" * 32)
    recovered = _service(
        tmp_path,
        _backend(tmp_path, market, history, now=NOW + timedelta(seconds=2)),
        now=NOW + timedelta(seconds=2),
    ).submit(_command())
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result == {"outcome": "task_queued", "task_id": original_task_id}


def test_missing_or_closed_source_never_queues_task(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path, open_day=False)
    closed = _service(tmp_path, _backend(tmp_path, market, history)).submit(_command())
    assert closed.status is PageControlStatus.FAILED
    assert _backend(tmp_path, market, history).store.latest() is None

    market_pointer = market[0] / DAY.isoformat() / "current.json"
    market_pointer.unlink()
    missing = _service(tmp_path, _backend(tmp_path, market, history)).submit(
        _command("formula-market-0002")
    )
    assert missing.status is PageControlStatus.FAILED


def test_source_switch_during_admission_never_queues_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    backend = _backend(tmp_path, market, history)
    original = admission_module.peek_formula_market_universe_identity
    calls = 0

    def switch_after_first(root: Path, trade_date: date) -> str:
        nonlocal calls
        calls += 1
        identity = original(root, trade_date)
        if calls == 1:
            _market(tmp_path, completed_at=NOW - timedelta(days=1, minutes=50))
        return identity

    monkeypatch.setattr(
        admission_module, "peek_formula_market_universe_identity", switch_after_first
    )
    receipt = _service(tmp_path, backend).submit(_command())
    assert receipt.status is PageControlStatus.FAILED
    assert backend.store.latest() is None


def test_reusing_command_id_with_a_different_actor_is_a_conflict(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    service = _service(tmp_path, _backend(tmp_path, market, history))
    assert service.submit(_command()).status is PageControlStatus.SUCCEEDED
    with pytest.raises(PageControlCommandConflictError):
        service.submit(_command().model_copy(update={"actor_id": "another-user"}))


def test_second_command_reports_active_task_conflict_without_queuing(
    tmp_path: Path,
) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    backend = _backend(tmp_path, market, history)
    service = _service(tmp_path, backend)
    first = service.submit(_command())
    second = service.submit(_command("formula-market-0002"))

    assert first.status is PageControlStatus.SUCCEEDED
    assert second.status is PageControlStatus.SUCCEEDED
    assert second.result == {"outcome": "task_conflict", "reason": "task_active"}
    assert backend.store.latest() is not None
    assert backend.store.latest().task_id == first.result["task_id"]


def test_service_builder_accepts_explicit_formula_backend(tmp_path: Path) -> None:
    market, history = _market(tmp_path), _history(tmp_path)
    backend = _backend(tmp_path, market, history)
    service = build_page_control_service(
        outbox_path=tmp_path / "builder-outbox.sqlite",
        data_dir=tmp_path / "builder-data",
        log_dir=tmp_path / "builder-logs",
        allowed_lab_export_roots=(tmp_path / "builder-exports",),
        formula_market_backend=backend,
        load_default_lab_backend=False,
        clock=lambda: NOW,
    )
    assert service.submit(_command("formula-market-builder")).status is PageControlStatus.SUCCEEDED
