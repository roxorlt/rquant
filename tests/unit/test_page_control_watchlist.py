from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.manual_watchlist import (
    ManualWatchlistKey,
    ManualWatchlistRepository,
    ManualWatchlistUpsert,
)
from rquant.page_control import (
    AddWatchlistItem,
    PageControlClient,
    PageControlCommandConflictError,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    RemoveWatchlistItem,
    parse_page_control_command,
)

NOW = datetime(2026, 9, 28, 2, 0, tzinfo=UTC)
OWNER = "alice"
CODE = "600000.SH"


def _add(
    command_id: str,
    *,
    owner_id: str = OWNER,
    code: str = CODE,
    expected_version: int | None = None,
    price: Decimal = Decimal("10.00"),
    requested_at: datetime = NOW,
) -> AddWatchlistItem:
    return AddWatchlistItem(
        command_id=command_id,
        requested_at=requested_at,
        item=ManualWatchlistUpsert(
            owner_id=owner_id,
            ts_code=code,
            expected_version=expected_version,
            source="detail",
            price_levels=(price,),
        ),
    )


def _remove(command_id: str, *, expected_version: int) -> RemoveWatchlistItem:
    return RemoveWatchlistItem(
        command_id=command_id,
        requested_at=NOW,
        item={"owner_id": OWNER, "ts_code": CODE, "expected_version": expected_version},
    )


def _service(
    outbox: PageControlOutbox, tmp_path: Path, *, now: datetime = NOW
) -> PageControlService:
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=lambda: now,
            consumer_id="watchlist-consumer",
        ),
    )


def _entry(path: Path, *, owner_id: str = OWNER, code: str = CODE, now: datetime = NOW):
    with sqlite3.connect(path) as connection:
        return ManualWatchlistRepository(connection).get(
            ManualWatchlistKey(owner_id=owner_id, ts_code=code), now=now
        )


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }


def test_watchlist_requires_explicit_activation_and_all_generic_submission_paths_reject(
    tmp_path: Path,
) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    service = _service(outbox, tmp_path)
    command = _add("watch-1")
    assert "manual_watchlist" not in _tables(path)
    assert outbox.manual_watchlist_activated_at() is None
    assert isinstance(parse_page_control_command(command.model_dump(mode="json")), AddWatchlistItem)
    with pytest.raises(ValueError, match="trusted|watchlist"):
        service.submit(command)
    with pytest.raises(ValueError, match="trusted|watchlist"):
        outbox.enqueue(command)
    with pytest.raises(ValueError, match="activated"):
        service._submit_trusted_watchlist(command, authenticated_owner_id=OWNER)
    assert outbox.receipt(command.command_id) is None
    assert "manual_watchlist" not in _tables(path)

    submitted: list[dict[str, object]] = []
    client = PageControlClient(transport=lambda payload: submitted.append(payload) or {})
    with pytest.raises(ValueError, match="trusted|watchlist"):
        client.submit(command)
    assert submitted == []

    assert outbox.activate_manual_watchlist(NOW) == NOW
    assert outbox.activate_manual_watchlist(NOW) == NOW
    assert outbox.manual_watchlist_activated_at() == NOW
    assert "manual_watchlist" in _tables(path)
    with pytest.raises(ValueError, match="trusted|watchlist"):
        service.submit(command)
    with pytest.raises(ValueError, match="owner"):
        service._submit_trusted_watchlist(command, authenticated_owner_id="bob")
    assert outbox.receipt(command.command_id) is None


def test_trusted_command_add_update_remove_recreate_and_exact_retry(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    service = _service(outbox, tmp_path)
    add = _add("watch-add")
    first = service._submit_trusted_watchlist(add, authenticated_owner_id=OWNER)
    assert first.status is PageControlStatus.SUCCEEDED
    assert first.result == {
        "ts_code": CODE,
        "action": "add",
        "version": 1,
        "state": "active",
    }
    assert _entry(path).version == 1
    effect = outbox.effect(add.command_id)
    assert effect is not None and effect.status == "succeeded"
    assert effect.result == first.result
    assert service._submit_trusted_watchlist(add, authenticated_owner_id=OWNER) == first
    with pytest.raises(PageControlCommandConflictError):
        service._submit_trusted_watchlist(
            _add("watch-add", price=Decimal("10.25")), authenticated_owner_id=OWNER
        )

    updated = service._submit_trusted_watchlist(
        _add("watch-update", expected_version=1, price=Decimal("10.25")),
        authenticated_owner_id=OWNER,
    )
    assert (updated.status, updated.result["version"]) == (PageControlStatus.SUCCEEDED, 2)
    removed = service._submit_trusted_watchlist(
        _remove("watch-remove", expected_version=2), authenticated_owner_id=OWNER
    )
    assert removed.result == {
        "ts_code": CODE,
        "action": "remove",
        "version": 3,
        "state": "deleted",
    }
    assert _entry(path).status == "deleted"
    recreated = service._submit_trusted_watchlist(
        _add("watch-recreate", expected_version=3), authenticated_owner_id=OWNER
    )
    assert (recreated.status, recreated.result["version"]) == (PageControlStatus.SUCCEEDED, 4)
    assert _entry(path).version == 4


def test_same_code_is_isolated_by_authenticated_owner(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    service = _service(outbox, tmp_path)
    assert (
        service._submit_trusted_watchlist(_add("alice-add"), authenticated_owner_id=OWNER).status
        is PageControlStatus.SUCCEEDED
    )
    assert (
        service._submit_trusted_watchlist(
            _add("bob-add", owner_id="bob"), authenticated_owner_id="bob"
        ).status
        is PageControlStatus.SUCCEEDED
    )
    assert _entry(path, owner_id=OWNER).version == 1
    assert _entry(path, owner_id="bob").version == 1


def test_stale_or_expired_claim_cannot_mutate_watchlist(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    command = _add("watch-lease")
    outbox.enqueue_trusted_watchlist(command)
    claim = outbox.claim_records(limit=1, owner_id="worker-a", lease_seconds=1, now=NOW)[0]
    with pytest.raises(RuntimeError, match="stale|expired"):
        outbox.complete_watchlist(claim, now=NOW + timedelta(seconds=1))
    assert _entry(path) is None
    assert outbox.effect(command.command_id) is None
    replacement = outbox.claim_records(
        limit=1, owner_id="worker-b", lease_seconds=30, now=NOW + timedelta(seconds=1)
    )[0]
    with pytest.raises(RuntimeError, match="stale|expired"):
        outbox.complete_watchlist(claim, now=NOW + timedelta(seconds=1))
    receipt = outbox.complete_watchlist(replacement, now=NOW + timedelta(seconds=1))
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert _entry(path).version == 1


def test_generic_complete_cannot_finalize_a_watchlist_claim(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    command = _add("watch-generic")
    outbox.enqueue_trusted_watchlist(command)
    claim = outbox.claim_records(limit=1, owner_id="worker-a", now=NOW)[0]
    with pytest.raises(ValueError, match="watchlist"):
        outbox.complete(
            command.command_id,
            result={"state": "active"},
            owner_id=claim.owner_id,
            claim_token=claim.claim_token,
        )
    assert _entry(path) is None
    assert outbox.receipt(command.command_id).status is PageControlStatus.PROCESSING


def test_version_conflict_and_capacity_fail_with_durable_distinct_results(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    service = _service(outbox, tmp_path)
    first = service._submit_trusted_watchlist(_add("first"), authenticated_owner_id=OWNER)
    conflict = service._submit_trusted_watchlist(_add("stale"), authenticated_owner_id=OWNER)
    assert conflict.status is PageControlStatus.FAILED
    assert conflict.result["code"] == "version_conflict"
    assert outbox.effect("stale").status == "failed"
    assert _entry(path).version == first.result["version"] == 1

    with sqlite3.connect(path) as connection:
        connection.execute("BEGIN IMMEDIATE")
        store = ManualWatchlistRepository(connection)
        for number in range(499):
            store.upsert(
                ManualWatchlistUpsert(
                    owner_id=OWNER,
                    ts_code=f"{number:06d}.SH",
                    source="detail",
                ),
                now=NOW,
            )
    capacity = service._submit_trusted_watchlist(
        _add("full", code="600001.SH"), authenticated_owner_id=OWNER
    )
    assert capacity.status is PageControlStatus.FAILED
    assert capacity.result["code"] == "capacity_exceeded"
    assert outbox.effect("full").status == "failed"
    assert _entry(path, code="600001.SH") is None
    assert _entry(path).version == 1


def test_effect_insert_failure_rolls_back_list_and_terminal_receipt(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    command = _add("watch-rollback")
    outbox.enqueue_trusted_watchlist(command)
    claim = outbox.claim_records(limit=1, owner_id="worker-a", now=NOW)[0]
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_watchlist_effect BEFORE INSERT ON page_control_effect "
            "BEGIN SELECT RAISE(ABORT, 'injected effect failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="injected effect failure"):
        outbox.complete_watchlist(claim, now=NOW)
    assert _entry(path) is None
    assert outbox.effect(command.command_id) is None
    assert outbox.receipt(command.command_id).status is PageControlStatus.PROCESSING
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER fail_watchlist_effect")
    assert outbox.complete_watchlist(claim, now=NOW).status is PageControlStatus.SUCCEEDED
    assert _entry(path).version == 1


def test_response_lost_after_commit_recovers_exact_persisted_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    service = _service(outbox, tmp_path)
    command = _add("watch-response-loss")
    original_complete = outbox.complete_watchlist
    calls = 0

    def complete_then_lose_response(*args: object, **kwargs: object):
        nonlocal calls
        receipt = original_complete(*args, **kwargs)
        calls += 1
        if calls == 1:
            raise ConnectionResetError("response lost after commit")
        return receipt

    monkeypatch.setattr(outbox, "complete_watchlist", complete_then_lose_response)
    with pytest.raises(ConnectionResetError, match="response lost"):
        service._submit_trusted_watchlist(command, authenticated_owner_id=OWNER)
    assert _entry(path).version == 1
    restarted = _service(PageControlOutbox(path), tmp_path)
    recovered = restarted._submit_trusted_watchlist(command, authenticated_owner_id=OWNER)
    assert recovered.status is PageControlStatus.SUCCEEDED
    assert recovered.result["version"] == 1
    assert _entry(path).version == 1


def test_future_requested_at_fails_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    service = _service(outbox, tmp_path)
    command = _add("watch-future", requested_at=NOW + timedelta(minutes=6))
    receipt = service._submit_trusted_watchlist(command, authenticated_owner_id=OWNER)
    assert receipt.status is PageControlStatus.FAILED
    assert receipt.result["code"] == "future_request"
    assert _entry(path) is None
