"""RQ-07: real PageControl CAS, original-command recovery and atomicity."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest

import rquant.page_control as control

NOW = datetime(2026, 10, 5, tzinfo=UTC)


def _service(tmp_path: Path):
    assert hasattr(control, "SaveResearchQuery"), "typed query save is not implemented"
    outbox = control.PageControlOutbox(tmp_path / "control.sqlite3")
    service = control.PageControlService(
        outbox=outbox,
        consumer=control.PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "state",
            log_dir=tmp_path / "events",
            clock=lambda: NOW,
        ),
    )
    return service


def _command(command_id: str = "original", **changes: object):
    values = {
        "command_id": command_id,
        "requested_at": NOW,
        "query_id": "query-1",
        "name": "最新收盘价",
        "sql": "SELECT ts_code,close FROM daily_bar",
    }
    values.update(changes)
    return control.SaveResearchQuery(**values)


def test_save_and_recover_bind_exact_actor_original_command_and_version(tmp_path: Path) -> None:
    service = _service(tmp_path)
    command = _command()
    with pytest.raises(ValueError):
        service.submit(command)
    with pytest.raises(ValueError):
        control.parse_page_control_command(command.model_dump(mode="json"))
    receipt = service._submit_trusted_research_query(command, authenticated_actor_id="alice")
    assert receipt.status == control.PageControlStatus.SUCCEEDED
    assert receipt.result["version"] == 1
    assert (
        service._resume_trusted_research_query(command, authenticated_actor_id="alice") == receipt
    )
    for changed, actor in ((_command(sql="SELECT 2"), "alice"), (command, "bob")):
        with pytest.raises(control.PageControlCommandConflictError):
            service._resume_trusted_research_query(changed, authenticated_actor_id=actor)
    assert len(service.outbox.list_research_queries("alice")) == 1
    assert service.outbox.list_research_queries("bob") == ()
    conflict = service._submit_trusted_research_query(
        _command("stale"), authenticated_actor_id="alice"
    )
    assert conflict.status == control.PageControlStatus.FAILED
    changed = service._submit_trusted_research_query(
        _command("version-2", expected_version=1, sql="SELECT 2"), authenticated_actor_id="alice"
    )
    assert changed.status == control.PageControlStatus.SUCCEEDED and changed.result["version"] == 2
    assert service.outbox.list_research_queries("alice")[0].sql == "SELECT 2"


def test_capacity_and_database_failure_preserve_effect_and_terminal_receipt(tmp_path: Path) -> None:
    service = _service(tmp_path)
    for index in range(100):
        receipt = service._submit_trusted_research_query(
            _command(f"create-{index}", query_id=f"q-{index}"), authenticated_actor_id="alice"
        )
        assert receipt.status == control.PageControlStatus.SUCCEEDED
    receipt = service._submit_trusted_research_query(
        _command("full", query_id="new"), authenticated_actor_id="alice"
    )
    assert receipt.status == control.PageControlStatus.FAILED
    assert len(service.outbox.list_research_queries("alice")) == 100
    with sqlite3.connect(service.outbox.path) as connection:
        connection.execute(
            "CREATE TRIGGER reject_update BEFORE UPDATE ON research_query_saved "
            "BEGIN SELECT RAISE(ABORT, 'injected transaction failure'); END"
        )
    command = _command("db-failure", query_id="q-0", expected_version=1, sql="SELECT 9")
    with pytest.raises(sqlite3.DatabaseError):
        service._submit_trusted_research_query(command, authenticated_actor_id="alice")
    assert service.outbox.list_research_queries("alice")[0].sql != "SELECT 9"
    with sqlite3.connect(service.outbox.path) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM page_control_effect WHERE command_id='db-failure'"
            ).fetchone()[0]
            == 0
        )
        connection.execute("DROP TRIGGER reject_update")
    # An expired claim can resume; no effect committed with the failed transaction.
    service.consumer.clock = lambda: NOW.replace(minute=2)
    resumed = service._resume_trusted_research_query(command, authenticated_actor_id="alice")
    assert resumed.status == control.PageControlStatus.SUCCEEDED
    assert resumed.result["version"] == 2


def test_saved_name_sql_bounds_and_browser_owner_are_rejected(tmp_path: Path) -> None:
    _service(tmp_path)
    for changes in ({"name": "x" * 61}, {"sql": "DELETE FROM daily_bar"}, {"owner_id": "bob"}):
        with pytest.raises(ValueError):
            _command(**changes)
