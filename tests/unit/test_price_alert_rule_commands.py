from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import (
    ManualWatchlistDelete,
    ManualWatchlistRepository,
    ManualWatchlistUpsert,
)
from rquant.price_alert_rule_store import (
    PriceAlertRuleKey,
    PriceAlertRuleRepository,
    PriceAlertRuleUpsert,
)
from rquant.runtime_contracts import canonical_sha256

NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)
CODE = "600001.SH"


def _api() -> Any:
    return import_module("rquant.page_control")


def _service(outbox: Any, tmp_path: Path, *, now: datetime = NOW) -> Any:
    api = _api()
    return api.PageControlService(
        outbox=outbox,
        consumer=api.PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=lambda: now,
            consumer_id="price-rule-consumer",
        ),
    )


def _rule(rule_id: str = "threshold-a", *, enabled: bool = True) -> PriceAlertRule:
    return PriceAlertRule(
        rule_id=rule_id,
        name="到价提醒",
        priority="P2",
        enabled=enabled,
        comparison="gte",
        threshold=Decimal("10.00"),
        valid_from=time(9, 30),
        valid_until=time(14, 57),
    )


def _save(
    command_id: str,
    *,
    membership_version: int = 1,
    expected_version: int | None = None,
    rule_id: str = "threshold-a",
    enabled: bool = True,
    requested_at: datetime = NOW,
) -> Any:
    api = _api()
    return api.SavePriceAlertRule(
        command_id=command_id,
        requested_at=requested_at,
        ts_code=CODE,
        membership_version=membership_version,
        expected_version=expected_version,
        rule=_rule(rule_id, enabled=enabled),
    )


def _set_enabled(command_id: str, *, version: int, enabled: bool) -> Any:
    return _api().SetPriceAlertRuleEnabled(
        command_id=command_id,
        requested_at=NOW,
        rule_id="threshold-a",
        expected_version=version,
        enabled=enabled,
    )


def _delete(command_id: str, *, version: int) -> Any:
    return _api().DeletePriceAlertRule(
        command_id=command_id,
        requested_at=NOW,
        rule_id="threshold-a",
        expected_version=version,
    )


def _member(
    path: Path,
    owner: str,
    *,
    expected_version: int | None = None,
    expires_at: datetime | None = None,
) -> int:
    with sqlite3.connect(path, isolation_level=None) as connection:
        connection.execute("BEGIN IMMEDIATE")
        entry = ManualWatchlistRepository(connection).upsert(
            ManualWatchlistUpsert(
                owner_id=owner,
                ts_code=CODE,
                expected_version=expected_version,
                source="detail",
                expires_at=expires_at,
            ),
            now=NOW,
        )
        connection.commit()
        return entry.version


def _entry(path: Path, owner: str, rule_id: str = "threshold-a") -> Any:
    with sqlite3.connect(path) as connection:
        return PriceAlertRuleRepository(connection).get(
            PriceAlertRuleKey(owner_id=owner, rule_id=rule_id)
        )


def _remove_member(path: Path, owner: str, *, version: int) -> None:
    with sqlite3.connect(path, isolation_level=None) as connection:
        connection.execute("BEGIN IMMEDIATE")
        ManualWatchlistRepository(connection).delete(
            ManualWatchlistDelete(owner_id=owner, ts_code=CODE, expected_version=version),
            now=NOW,
        )
        connection.commit()


def _tables(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }


def _activated(tmp_path: Path) -> tuple[Any, Any, Path]:
    api = _api()
    path = tmp_path / "control.sqlite3"
    outbox = api.PageControlOutbox(path)
    outbox.activate_manual_watchlist(NOW)
    outbox.activate_price_alert_rules(NOW)
    return outbox, _service(outbox, tmp_path), path


def test_default_closed_activation_is_atomic_and_all_generic_paths_reject(tmp_path: Path) -> None:
    api = _api()
    path = tmp_path / "control.sqlite3"
    outbox = api.PageControlOutbox(path)
    service = _service(outbox, tmp_path)
    request = _save("save-1")
    assert "price_alert_rule" not in _tables(path)
    with pytest.raises(ValidationError):
        api.SavePriceAlertRule.model_validate({**request.model_dump(), "owner_id": "alice"})
    with pytest.raises(ValueError, match="price rule|trusted"):
        api.parse_page_control_command({**request.model_dump(mode="json"), "owner_id": "alice"})
    with pytest.raises(ValidationError):
        api.parse_page_control_command({"kind": [], "command_id": "bad", "requested_at": NOW})
    with pytest.raises(ValueError, match="price rule|trusted"):
        service.submit(request)
    with pytest.raises(ValueError, match="price rule|trusted"):
        outbox.enqueue(request)
    calls: list[dict[str, object]] = []
    client = api.PageControlClient(transport=lambda payload: calls.append(payload) or {})
    with pytest.raises(ValueError, match="price rule|trusted"):
        client.submit(request)
    assert calls == []
    with pytest.raises(ValueError, match="activated"):
        service._submit_trusted_price_rule(request, authenticated_owner_id="alice")
    assert outbox.receipt("save-1") is None
    assert "price_alert_rule" not in _tables(path)

    assert outbox.activate_price_alert_rules(NOW) == NOW
    assert outbox.activate_price_alert_rules(NOW) == NOW
    assert "price_alert_rule" in _tables(path)
    with pytest.raises(ValueError, match="price rule|trusted"):
        service.submit(request)


def test_failed_activation_rolls_back_schema_and_marker_together(tmp_path: Path) -> None:
    api = _api()
    path = tmp_path / "control.sqlite3"
    outbox = api.PageControlOutbox(path)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_price_activation BEFORE INSERT "
            "ON page_control_protocol_activation "
            "WHEN NEW.marker_name = 'price-alert-rule/v1' "
            "BEGIN SELECT RAISE(ABORT, 'injected activation failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="injected activation failure"):
        outbox.activate_price_alert_rules(NOW)
    assert "price_alert_rule" not in _tables(path)
    assert outbox.price_alert_rules_activated_at() is None
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER fail_price_activation")
    assert outbox.activate_price_alert_rules(NOW) == NOW


def test_owner_is_in_persisted_hash_and_exact_lookup_is_read_only(tmp_path: Path) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    _member(path, "alice")
    _member(path, "bob")
    request = _save("same-id")
    first = service._submit_trusted_price_rule(request, authenticated_owner_id="alice")
    assert first.status is api.PageControlStatus.SUCCEEDED
    assert first.result == {
        "rule_id": "threshold-a",
        "action": "save",
        "version": 1,
        "deleted": False,
        "enabled": True,
    }
    assert _entry(path, "alice").version == 1
    assert _entry(path, "bob") is None
    with sqlite3.connect(path) as connection:
        digest, body = connection.execute(
            "SELECT command_hash, payload_json FROM page_control_command WHERE command_id = ?",
            ("same-id",),
        ).fetchone()
    persisted = json.loads(body)
    assert persisted["owner_id"] == "alice"
    assert digest == canonical_sha256(persisted)
    assert service._lookup_trusted_price_rule(request, authenticated_owner_id="alice") == first
    assert service._submit_trusted_price_rule(request, authenticated_owner_id="alice") == first
    with pytest.raises(api.PageControlCommandConflictError):
        service._submit_trusted_price_rule(request, authenticated_owner_id="bob")
    with pytest.raises(api.PageControlCommandConflictError):
        service._lookup_trusted_price_rule(request, authenticated_owner_id="bob")
    with pytest.raises(api.PageControlCommandConflictError):
        service._resume_trusted_price_rule(request, authenticated_owner_id="bob")
    changed = _save("same-id", membership_version=2)
    with pytest.raises(api.PageControlCommandConflictError):
        service._lookup_trusted_price_rule(changed, authenticated_owner_id="alice")
    with pytest.raises(api.PageControlCommandConflictError):
        service._resume_trusted_price_rule(changed, authenticated_owner_id="alice")
    assert (
        service._lookup_trusted_price_rule(_save("missing"), authenticated_owner_id="alice") is None
    )
    with pytest.raises(KeyError):
        service._resume_trusted_price_rule(_save("missing"), authenticated_owner_id="alice")
    assert outbox.receipt("missing") is None
    bob = service._submit_trusted_price_rule(_save("bob-own-id"), authenticated_owner_id="bob")
    assert bob.status is api.PageControlStatus.SUCCEEDED
    assert _entry(path, "bob").version == 1
    assert _entry(path, "alice").version == 1


def test_pending_original_command_resumes_without_reenqueue_and_response_loss_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    _member(path, "alice")
    request = _save("recover")
    monkeypatch.setattr(
        service.consumer,
        "drain_price_rule_command",
        lambda command: (_ for _ in ()).throw(ConnectionResetError("lost before drain")),
    )
    with pytest.raises(ConnectionResetError, match="before drain"):
        service._submit_trusted_price_rule(request, authenticated_owner_id="alice")
    assert _entry(path, "alice") is None
    assert outbox.receipt("recover").status is api.PageControlStatus.PENDING
    restarted = _service(api.PageControlOutbox(path), tmp_path)
    assert restarted._lookup_trusted_price_rule(request, authenticated_owner_id="alice").status is (
        api.PageControlStatus.PENDING
    )
    assert _entry(path, "alice") is None
    recovered = restarted._resume_trusted_price_rule(request, authenticated_owner_id="alice")
    assert recovered.status is api.PageControlStatus.SUCCEEDED
    assert recovered.result["version"] == 1
    assert (
        restarted._resume_trusted_price_rule(request, authenticated_owner_id="alice") == recovered
    )
    assert _entry(path, "alice").version == 1


def test_resuming_one_rule_command_does_not_execute_another_pending_command(tmp_path: Path) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    _member(path, "alice")
    first = _save("first-pending")
    second = _save("second-pending", rule_id="threshold-b")
    outbox.enqueue_trusted_price_rule(
        api._owned_price_rule_command(first, authenticated_owner_id="alice")
    )
    outbox.enqueue_trusted_price_rule(
        api._owned_price_rule_command(second, authenticated_owner_id="alice")
    )
    resumed = service._resume_trusted_price_rule(first, authenticated_owner_id="alice")
    assert resumed.status is api.PageControlStatus.SUCCEEDED
    assert outbox.receipt("second-pending").status is api.PageControlStatus.PENDING
    assert _entry(path, "alice", "threshold-b") is None


def test_future_request_is_terminal_failure_without_rule_write(tmp_path: Path) -> None:
    api = _api()
    _, service, path = _activated(tmp_path)
    _member(path, "alice")
    request = _save("future", requested_at=NOW + timedelta(minutes=6))
    receipt = service._submit_trusted_price_rule(request, authenticated_owner_id="alice")
    assert receipt.status is api.PageControlStatus.FAILED
    assert receipt.result["code"] == "future_request"
    assert _entry(path, "alice") is None


def test_disabled_price_protocol_does_not_starve_existing_watchlist_claims(tmp_path: Path) -> None:
    api = _api()
    outbox, _, path = _activated(tmp_path)
    _member(path, "alice")
    price_command = api._owned_price_rule_command(
        _save("price-first"), authenticated_owner_id="alice"
    )
    outbox.enqueue_trusted_price_rule(price_command)
    watch_command = api.AddWatchlistItem(
        command_id="watch-second",
        requested_at=NOW,
        item=ManualWatchlistUpsert(owner_id="alice", ts_code="600002.SH", source="detail"),
    )
    outbox.enqueue_trusted_watchlist(watch_command)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "DELETE FROM page_control_protocol_activation WHERE marker_name = ?",
            ("price-alert-rule/v1",),
        )
    claims = outbox.claim_records(limit=1, owner_id="worker", now=NOW)
    assert len(claims) == 1 and claims[0].command == watch_command
    assert outbox.receipt("price-first").status is api.PageControlStatus.PENDING


def test_mismatched_price_kind_does_not_stop_the_next_watchlist_command(tmp_path: Path) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    owned = api._owned_price_rule_command(_save("wrong-kind"), authenticated_owner_id="alice")
    outbox.enqueue_trusted_price_rule(owned)
    watch_command = api.AddWatchlistItem(
        command_id="watch-after-wrong-kind",
        requested_at=NOW,
        item=ManualWatchlistUpsert(owner_id="alice", ts_code=CODE, source="detail"),
    )
    outbox.enqueue_trusted_watchlist(watch_command)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE page_control_command SET command_kind = ? WHERE command_id = ?",
            ("save_price_alert_rule_typo", "wrong-kind"),
        )
    drained = service.consumer.drain(limit=1)
    assert len(drained) == 1
    assert drained[0].command_id == watch_command.command_id
    assert drained[0].status is api.PageControlStatus.SUCCEEDED
    assert outbox.receipt("wrong-kind").status is api.PageControlStatus.PENDING
    assert outbox.effect("wrong-kind") is None


@pytest.mark.parametrize("corruption", ["payload", "digest"])
def test_corrupt_price_command_is_not_claimed_or_allowed_to_starve_other_work(
    tmp_path: Path, corruption: str
) -> None:
    api = _api()
    outbox, _, path = _activated(tmp_path)
    owned = api._owned_price_rule_command(_save("bad-price"), authenticated_owner_id="alice")
    outbox.enqueue_trusted_price_rule(owned)
    watch_command = api.AddWatchlistItem(
        command_id="good-watch",
        requested_at=NOW,
        item=ManualWatchlistUpsert(owner_id="alice", ts_code=CODE, source="detail"),
    )
    outbox.enqueue_trusted_watchlist(watch_command)
    with sqlite3.connect(path) as connection:
        if corruption == "payload":
            connection.execute(
                "UPDATE page_control_command SET payload_json = '{}' WHERE command_id = ?",
                ("bad-price",),
            )
        else:
            connection.execute(
                "UPDATE page_control_command SET command_hash = ? WHERE command_id = ?",
                ("0" * 64, "bad-price"),
            )
    claims = outbox.claim_records(limit=1, owner_id="worker", now=NOW)
    assert len(claims) == 1 and claims[0].command == watch_command
    assert outbox.receipt("bad-price").status is api.PageControlStatus.PENDING


def test_scope_time_cas_disable_delete_and_tombstone_rebuild(tmp_path: Path) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    _member(path, "bob")
    only_bob = service._submit_trusted_price_rule(_save("only-bob"), authenticated_owner_id="alice")
    assert only_bob.status is api.PageControlStatus.FAILED
    assert only_bob.result["code"] == "scope_invalid"
    assert _entry(path, "alice") is None

    _member(path, "alice", expires_at=NOW)
    expired = service._submit_trusted_price_rule(_save("expired"), authenticated_owner_id="alice")
    assert expired.result["code"] == "scope_invalid"
    assert _entry(path, "alice") is None
    _member(path, "alice", expected_version=1)
    stale = service._submit_trusted_price_rule(_save("stale"), authenticated_owner_id="alice")
    assert stale.result["code"] == "scope_invalid"
    created = service._submit_trusted_price_rule(
        _save("created", membership_version=2), authenticated_owner_id="alice"
    )
    assert created.status is api.PageControlStatus.SUCCEEDED
    assert created.result["version"] == 1
    assert _entry(path, "alice").membership_version == 2
    bob_toggle = service._submit_trusted_price_rule(
        _set_enabled("bob-toggle", version=1, enabled=False), authenticated_owner_id="bob"
    )
    bob_delete = service._submit_trusted_price_rule(
        _delete("bob-delete", version=1), authenticated_owner_id="bob"
    )
    assert bob_toggle.result["code"] == bob_delete.result["code"] == "version_conflict"
    assert _entry(path, "bob") is None
    assert _entry(path, "alice").version == 1

    _remove_member(path, "alice", version=2)
    disabled = service._submit_trusted_price_rule(
        _set_enabled("disabled", version=1, enabled=False), authenticated_owner_id="alice"
    )
    assert disabled.status is api.PageControlStatus.SUCCEEDED
    assert disabled.result["version"] == 2 and disabled.result["enabled"] is False
    reenabled = service._submit_trusted_price_rule(
        _set_enabled("reenabled", version=2, enabled=True), authenticated_owner_id="alice"
    )
    assert reenabled.status is api.PageControlStatus.FAILED
    assert reenabled.result["code"] == "scope_invalid"
    assert _entry(path, "alice").version == 2
    tombstone = service._submit_trusted_price_rule(
        _delete("deleted", version=2), authenticated_owner_id="alice"
    )
    assert tombstone.status is api.PageControlStatus.SUCCEEDED
    assert tombstone.result["version"] == 3 and tombstone.result["deleted"] is True
    assert _entry(path, "alice").deleted is True

    _member(path, "alice", expected_version=3)
    old_binding = service._submit_trusted_price_rule(
        _save("old-binding", membership_version=2, expected_version=3),
        authenticated_owner_id="alice",
    )
    assert old_binding.result["code"] == "scope_invalid"
    rebuilt = service._submit_trusted_price_rule(
        _save("rebuilt", membership_version=4, expected_version=3),
        authenticated_owner_id="alice",
    )
    assert rebuilt.status is api.PageControlStatus.SUCCEEDED
    assert rebuilt.result["version"] == 4
    assert _entry(path, "alice").membership_version == 4
    assert outbox.effect("old-binding").status is api.PageControlEffectStatus.FAILED


def test_corrupt_marker_or_rule_schema_blocks_submit_claim_and_complete(tmp_path: Path) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    _member(path, "alice")
    owned = api._owned_price_rule_command(_save("pending"), authenticated_owner_id="alice")
    outbox.enqueue_trusted_price_rule(owned)
    claim = outbox.claim_records(limit=1, owner_id="worker", now=NOW)[0]
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE page_control_protocol_activation SET protocol_version = 99 "
            "WHERE marker_name = ?",
            ("price-alert-rule/v1",),
        )
    with pytest.raises(ValueError, match="activated"):
        service._submit_trusted_price_rule(_save("new"), authenticated_owner_id="alice")
    with pytest.raises(ValueError, match="activated"):
        outbox.complete_price_rule(claim, now=NOW)
    assert _entry(path, "alice") is None
    assert outbox.receipt("pending").status is api.PageControlStatus.PROCESSING
    assert outbox.effect("pending") is None
    assert outbox.claim_records(limit=1, owner_id="worker", now=NOW + timedelta(seconds=31)) == ()
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE page_control_protocol_activation SET protocol_version = 1 "
            "WHERE marker_name = ?",
            ("price-alert-rule/v1",),
        )
        connection.execute("DROP TABLE price_alert_rule")
        connection.execute("CREATE TABLE price_alert_rule (bad TEXT)")
    with pytest.raises(RuntimeError, match="schema"):
        service._submit_trusted_price_rule(_save("bad-schema"), authenticated_owner_id="alice")
    assert outbox.receipt("bad-schema") is None
    assert outbox.claim_records(limit=1, owner_id="worker", now=NOW + timedelta(seconds=31)) == ()


def test_malformed_activation_timestamp_cannot_admit_a_rule_command(tmp_path: Path) -> None:
    _, service, path = _activated(tmp_path)
    _member(path, "alice")
    with sqlite3.connect(path) as connection:
        connection.execute(
            "UPDATE page_control_protocol_activation SET activated_at = ? WHERE marker_name = ?",
            ("not-a-timestamp", "price-alert-rule/v1"),
        )
    with pytest.raises(ValueError, match="activation"):
        service._submit_trusted_price_rule(
            _save("malformed-marker"), authenticated_owner_id="alice"
        )


def test_failed_effect_insert_rolls_back_rule_and_terminal_receipt(tmp_path: Path) -> None:
    api = _api()
    outbox, _, path = _activated(tmp_path)
    _member(path, "alice")
    owned = api._owned_price_rule_command(_save("atomic"), authenticated_owner_id="alice")
    outbox.enqueue_trusted_price_rule(owned)
    claim = outbox.claim_records(limit=1, owner_id="worker", now=NOW)[0]
    with pytest.raises(ValueError, match="atomic"):
        outbox.complete("atomic", owner_id=claim.owner_id, claim_token=claim.claim_token)
    with pytest.raises(ValueError, match="atomic"):
        outbox.begin_effect(owned, owner_id=claim.owner_id, claim_token=claim.claim_token)
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TRIGGER fail_price_effect BEFORE INSERT ON page_control_effect "
            "BEGIN SELECT RAISE(ABORT, 'injected effect failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="injected effect failure"):
        outbox.complete_price_rule(claim, now=NOW)
    assert _entry(path, "alice") is None
    assert outbox.effect("atomic") is None
    assert outbox.receipt("atomic").status is api.PageControlStatus.PROCESSING
    with sqlite3.connect(path) as connection:
        connection.execute("DROP TRIGGER fail_price_effect")
    restarted = api.PageControlOutbox(path)
    replacement = restarted.claim_records(
        limit=1, owner_id="replacement", now=NOW + timedelta(seconds=31)
    )[0]
    with pytest.raises(RuntimeError, match="stale"):
        restarted.complete_price_rule(claim, now=NOW + timedelta(seconds=31))
    completed = restarted.complete_price_rule(replacement, now=NOW + timedelta(seconds=31))
    assert completed.status is api.PageControlStatus.SUCCEEDED
    assert completed.result["version"] == _entry(path, "alice").version == 1
    assert restarted.effect("atomic").status is api.PageControlEffectStatus.SUCCEEDED


def test_response_lost_after_commit_returns_same_effect_on_exact_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    _member(path, "alice")
    request = _save("lost-response")
    original_complete = outbox.complete_price_rule

    def complete_then_lose_response(*args: object, **kwargs: object) -> Any:
        original_complete(*args, **kwargs)
        raise ConnectionResetError("response lost after commit")

    monkeypatch.setattr(outbox, "complete_price_rule", complete_then_lose_response)
    with pytest.raises(ConnectionResetError, match="response lost"):
        service._submit_trusted_price_rule(request, authenticated_owner_id="alice")
    restarted = _service(api.PageControlOutbox(path), tmp_path)
    receipt = restarted._lookup_trusted_price_rule(request, authenticated_owner_id="alice")
    assert receipt.status is api.PageControlStatus.SUCCEEDED
    assert restarted._resume_trusted_price_rule(request, authenticated_owner_id="alice") == receipt
    assert _entry(path, "alice").version == 1


def test_concurrent_rule_commands_obey_one_cas_head(tmp_path: Path) -> None:
    api = _api()
    _, service, path = _activated(tmp_path)
    _member(path, "alice")
    assert (
        service._submit_trusted_price_rule(_save("first"), authenticated_owner_id="alice").status
        is api.PageControlStatus.SUCCEEDED
    )

    def submit(request: Any) -> Any:
        local = _service(api.PageControlOutbox(path), tmp_path)
        return local._submit_trusted_price_rule(request, authenticated_owner_id="alice")

    requests = (
        _set_enabled("disable", version=1, enabled=False),
        _delete("delete", version=1),
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        initial = tuple(pool.map(submit, requests))
    receipts = tuple(
        service._resume_trusted_price_rule(request, authenticated_owner_id="alice")
        if receipt.status is api.PageControlStatus.PENDING
        else receipt
        for request, receipt in zip(requests, initial, strict=True)
    )
    assert sorted(receipt.status for receipt in receipts) == [
        api.PageControlStatus.FAILED,
        api.PageControlStatus.SUCCEEDED,
    ]
    assert (
        next(
            receipt for receipt in receipts if receipt.status is api.PageControlStatus.FAILED
        ).result["code"]
        == "version_conflict"
    )
    assert _entry(path, "alice").version == 2


def test_capacity_failure_records_terminal_effect_without_mutation(tmp_path: Path) -> None:
    api = _api()
    outbox, service, path = _activated(tmp_path)
    _member(path, "alice")
    with sqlite3.connect(path, isolation_level=None) as connection:
        connection.execute("BEGIN IMMEDIATE")
        repository = PriceAlertRuleRepository(connection)
        for index in range(100):
            repository.upsert(
                PriceAlertRuleUpsert(
                    owner_id="alice",
                    ts_code=CODE,
                    membership_version=1,
                    rule=_rule(f"seed-{index:03d}"),
                ),
                now=NOW,
            )
        connection.commit()
    failed = service._submit_trusted_price_rule(
        _save("over-capacity"), authenticated_owner_id="alice"
    )
    assert failed.status is api.PageControlStatus.FAILED
    assert failed.result["code"] == "capacity_exceeded"
    assert _entry(path, "alice") is None
    assert outbox.effect("over-capacity").status is api.PageControlEffectStatus.FAILED
