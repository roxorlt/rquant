from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from rquant.manual_watchlist import (
    ManualWatchlistDelete,
    ManualWatchlistRepository,
    ManualWatchlistUpsert,
)
from rquant.page_control import (
    PageControlConsumer,
    PageControlOutbox,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
)
from rquant.price_alert_admission import (
    PriceAlertAdmission,
    PriceAlertAdmissionUnavailableError,
    PriceRuleCommand,
)
from rquant.web.price_alert_read import PriceAlertRuleView
from rquant.web.serving import BorrowedGeneration
from tests.unit.test_web_price_alert_rules import (
    HEADERS,
    NOW,
    PATH,
    WRITE_HEADERS,
    client_for,
    member,
    publish,
    rule,
)


class LogicalAdmission:
    """Real SQLite/CAS logic only. This does not prove socket identity installation."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.outbox = PageControlOutbox(path)
        self.outbox.activate_manual_watchlist(NOW - timedelta(days=1))
        self.outbox.activate_price_alert_rules(NOW - timedelta(days=1))
        self.inner = PriceAlertAdmission(
            PageControlService(
                outbox=self.outbox,
                consumer=PageControlConsumer(
                    outbox=self.outbox,
                    data_dir=path.parent / "data",
                    log_dir=path.parent / "logs",
                    clock=lambda: NOW,
                    consumer_id="web-price-test",
                ),
            )
        )
        self.submitted: list[PriceRuleCommand] = []
        self.lookup_count = 0
        self.lose_reply = False
        self.lookup_unavailable = False
        self.before_submit: Callable[[], None] | None = None
        with sqlite3.connect(path, isolation_level=None) as connection:
            connection.execute("BEGIN IMMEDIATE")
            for owner in ("alice", "bob"):
                ManualWatchlistRepository(connection).upsert(
                    ManualWatchlistUpsert(owner_id=owner, ts_code="600001.SH", source="detail"),
                    now=NOW,
                )
            connection.commit()

    def lookup(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        self.lookup_count += 1
        if self.lookup_unavailable:
            raise PriceAlertAdmissionUnavailableError("synthetic lookup I/O")
        return self.inner.lookup(command, authenticated_owner_id=authenticated_owner_id)

    def submit(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        self.submitted.append(command)
        if self.before_submit is not None:
            self.before_submit()
        result = self.inner.submit(command, authenticated_owner_id=authenticated_owner_id)
        if self.lose_reply:
            raise PriceAlertAdmissionUnavailableError("synthetic lost reply")
        return result

    def resume(
        self, command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt:
        return self.inner.resume(command, authenticated_owner_id=authenticated_owner_id)

    def counts(self) -> tuple[int, ...]:
        with sqlite3.connect(self.path) as connection:
            return tuple(
                connection.execute("SELECT COUNT(*) FROM " + name).fetchone()[0]
                for name in ("page_control_command", "page_control_effect", "price_alert_rule")
            )


def body(
    generation: str,
    *,
    command_id: str = "web-price-1",
    action: str = "save",
    expected: int | None = None,
    enabled: bool = True,
    threshold: str = "10.123456",
) -> dict[str, object]:
    value = {
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "generation_id": generation,
        "rule_id": "rule/a",
        "action": action,
        "expected_version": expected,
    }
    if action == "save":
        value.update(
            ts_code="600001.SH",
            membership_version=1,
            rule={
                "name": "到价提醒",
                "priority": "P2",
                "enabled": enabled,
                "comparison": "gte",
                "threshold": threshold,
                "valid_from": "09:30:01.123456",
                "valid_until": "14:57:02",
            },
        )
    elif action == "set_enabled":
        value["enabled"] = enabled
    return value


@pytest.mark.parametrize(
    "threshold", ["1e65", "1e-65", "1." + "1" * 64, "0." + "0" * 63 + "1" * 64, "1e64"]
)
def test_unpublishable_new_values_never_admit(threshold: str, tmp_path: Path) -> None:
    root = tmp_path / "serving"
    gen = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    clock = [NOW]
    with client_for(root, admission=admission, clock=lambda: clock[0]) as client:
        response = client.post(
            PATH + "/commands", json=body(gen, threshold=threshold), headers=WRITE_HEADERS
        )
        assert response.status_code == 422
        assert admission.lookup_count >= 1
        assert admission.submitted == [] and admission.counts() == (0, 0, 0)


@pytest.mark.parametrize("threshold", ["10.123456", "9" * 64, "1e-64", "0." + "0" * 63 + "1" * 63])
def test_precise_valid_prices_roundtrip_without_rounding(threshold: str, tmp_path: Path) -> None:
    from decimal import Decimal

    root = tmp_path / "serving"
    gen = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    clock = [NOW]
    with client_for(root, admission=admission, clock=lambda: clock[0]) as client:
        response = client.post(
            PATH + "/commands", json=body(gen, threshold=threshold), headers=WRITE_HEADERS
        )
        assert response.status_code == 200
        assert response.json()["status"] == "saved_syncing"
        assert admission.submitted[0].rule.threshold == Decimal(threshold)
        publish(root, sequence=6, rules=(rule(threshold=threshold, updated=NOW),))
        clock[0] = NOW + timedelta(minutes=2)
        data = client.post(
            PATH + "/commands/resume", json=body(gen, threshold=threshold), headers=WRITE_HEADERS
        ).json()
        assert data["status"] == "published"
        assert admission.counts() == (1, 1, 1)


@pytest.mark.parametrize("ahead_seconds, expected_status", [(1, "saved_syncing"), (360, "failed")])
def test_original_clock_skew_result_recovers_without_changing_request(
    ahead_seconds: int,
    expected_status: str,
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    generation = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    value = body(generation)
    value["requested_at"] = (NOW + timedelta(seconds=ahead_seconds)).isoformat()
    clock = [NOW]
    with client_for(root, admission=admission, clock=lambda: clock[0]) as client:
        response = client.post(PATH + "/commands", json=value, headers=WRITE_HEADERS)
        assert response.status_code == 200
        assert response.json()["status"] == expected_status
        assert admission.counts() == (1, 1, int(expected_status != "failed"))
        publish(root, sequence=6, rules=(rule(updated=NOW),) if ahead_seconds == 1 else ())
        clock[0] = NOW + timedelta(minutes=2)
        recovered = client.post(PATH + "/commands/resume", json=value, headers=WRITE_HEADERS)
        assert recovered.status_code == 200
        assert recovered.json()["status"] == ("published" if ahead_seconds == 1 else "failed")
        assert len(admission.submitted) == 1
        assert admission.submitted[0].requested_at.isoformat() == value["requested_at"]
        assert admission.counts() == (1, 1, int(expected_status != "failed"))


def test_all_actions_complete_only_on_exact_later_publication(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    gen = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    clock = [NOW]
    with client_for(root, admission=admission, clock=lambda: clock[0]) as client:
        save = body(gen)
        assert (
            client.post(PATH + "/commands", json=save, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
        later = publish(root, sequence=6, rules=(rule(updated=NOW),))
        clock[0] = NOW + timedelta(minutes=2)
        assert (
            client.post(PATH + "/commands/resume", json=save, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "published"
        )
        disable = body(later, command_id="disable", action="set_enabled", expected=1, enabled=False)
        assert (
            client.post(PATH + "/commands", json=disable, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
        newer = publish(root, sequence=7, rules=(rule(version=2, enabled=False, updated=NOW),))
        assert (
            client.post(PATH + "/commands/resume", json=disable, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "published"
        )
        delete = body(newer, command_id="delete", action="delete", expected=2)
        assert (
            client.post(PATH + "/commands", json=delete, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
        publish(root, sequence=8, rules=())
        clock[0] = NOW + timedelta(minutes=4)
        assert (
            client.post(PATH + "/commands/resume", json=delete, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "saved_syncing"
        )
        tombstone = publish(root, sequence=9, rules=(rule(version=3, deleted=True, updated=NOW),))
        assert (
            client.post(PATH + "/commands/resume", json=delete, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "published"
        )
        assert admission.counts() == (3, 3, 1)
        assert (
            client.post(
                PATH + "/commands",
                json=body(tombstone, command_id="recreate-without-version"),
                headers=WRITE_HEADERS,
            ).status_code
            == 409
        )
        recreated = client.post(
            PATH + "/commands",
            json=body(tombstone, command_id="recreate", expected=3),
            headers=WRITE_HEADERS,
        )
        assert recreated.json()["status"] == "saved_syncing" and recreated.json()["version"] == 4
        assert admission.counts() == (4, 4, 1)


def test_original_lookup_and_loss_recovery_precede_changed_source(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    gen = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    admission.lose_reply = True
    original = body(gen)
    with client_for(root, admission=admission) as client:
        response = client.post(PATH + "/commands", json=original, headers=WRITE_HEADERS)
        assert response.json()["status"] == "saved_syncing"
        publish(root, sequence=1, activated=False, watchlist_ready=False)
        assert (
            client.post(PATH + "/commands", json=original, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
        assert len(admission.submitted) == 1 and admission.counts() == (1, 1, 1)
        admission.lookup_unavailable = True
        assert (
            client.post(PATH + "/commands/resume", json=original, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "uncertain"
        )
        assert len(admission.submitted) == 1


def test_unknown_resume_and_preflight_conflict_never_create(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    gen = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    with client_for(root, admission=admission) as client:
        assert (
            client.post(PATH + "/commands/resume", json=body(gen), headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "not_found"
        )
        assert admission.counts() == (0, 0, 0)
        response = client.post(
            PATH + "/commands", json=body(gen, expected=7), headers=WRITE_HEADERS
        )
        assert response.status_code == 409 and admission.counts() == (0, 0, 0)


def test_same_id_exact_content_is_one_effect_and_changes_conflict(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    generation = publish(root, members=(member(), member("bob")))
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    original = body(generation, threshold="10.1234567890123456789012345678901")
    with client_for(root, admission=admission) as client:
        for _ in range(2):
            assert (
                client.post(PATH + "/commands", json=original, headers=WRITE_HEADERS).status_code
                == 200
            )
        changed = body(generation, threshold="10.1234567890123456789012345678902")
        assert (
            client.post(PATH + "/commands", json=changed, headers=WRITE_HEADERS).status_code == 409
        )
        changed_action = body(generation, action="delete", expected=1)
        assert (
            client.post(PATH + "/commands", json=changed_action, headers=WRITE_HEADERS).status_code
            == 409
        )
        assert (
            client.post(
                PATH + "/commands/resume",
                json=original,
                headers={**WRITE_HEADERS, "x-rquant-user": "bob"},
            ).status_code
            == 409
        )
        assert admission.counts() == (1, 1, 1)
        second = body(generation, command_id="bob-own")
        assert (
            client.post(
                PATH + "/commands", json=second, headers={**WRITE_HEADERS, "x-rquant-user": "bob"}
            ).status_code
            == 200
        )
        assert admission.counts() == (2, 2, 2)


def test_transaction_cas_rejects_a_second_action_even_with_same_read_head(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    generation = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    with client_for(root, admission=admission) as client:
        client.post(PATH + "/commands", json=body(generation), headers=WRITE_HEADERS)
        current = publish(root, sequence=1, rules=(rule(),))
        disable = body(
            current, command_id="disable-cas", action="set_enabled", expected=1, enabled=False
        )
        delete = body(current, command_id="delete-cas", action="delete", expected=1)
        assert (
            client.post(PATH + "/commands", json=disable, headers=WRITE_HEADERS).status_code == 200
        )
        conflict = client.post(PATH + "/commands", json=delete, headers=WRITE_HEADERS)
        assert conflict.status_code == 409 and conflict.json()["status"] == "conflict"
        assert admission.counts() == (3, 3, 1)
        with sqlite3.connect(admission.path) as connection:
            assert connection.execute(
                "SELECT version, deleted FROM price_alert_rule"
            ).fetchone() == (2, 0)
            assert (
                connection.execute(
                    "SELECT COUNT(*) FROM page_control_effect WHERE status='succeeded'"
                ).fetchone()[0]
                == 2
            )


def remove_member(
    admission: LogicalAdmission, *, readd: bool = False, expire: bool = False
) -> None:
    with sqlite3.connect(admission.path, isolation_level=None) as connection:
        connection.execute("BEGIN IMMEDIATE")
        repository = ManualWatchlistRepository(connection)
        if expire:
            repository.upsert(
                ManualWatchlistUpsert(
                    owner_id="alice",
                    ts_code="600001.SH",
                    expected_version=1,
                    source="detail",
                    expires_at=NOW,
                ),
                now=NOW,
            )
        else:
            repository.delete(
                ManualWatchlistDelete(owner_id="alice", ts_code="600001.SH", expected_version=1),
                now=NOW,
            )
            if readd:
                repository.upsert(
                    ManualWatchlistUpsert(
                        owner_id="alice", ts_code="600001.SH", expected_version=2, source="detail"
                    ),
                    now=NOW,
                )
        connection.commit()


@pytest.mark.parametrize("change", ["remove", "expire", "readd"])
@pytest.mark.parametrize("action", ["new", "enable"])
def test_scope_change_between_preflight_and_transaction_rejects_new_rule(
    change: str,
    action: str,
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    generation = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    if action == "enable":
        from rquant.web.models.price_alert_rules import PriceAlertRuleCommandRequest
        from rquant.web.price_alert_commands import domain_command

        admission.inner.submit(
            domain_command(
                PriceAlertRuleCommandRequest.model_validate(
                    body(generation, command_id="seed-disabled", enabled=False)
                )
            ),
            authenticated_owner_id="alice",
        )
        generation = publish(root, sequence=5, rules=(rule(enabled=False, updated=NOW),))
    admission.before_submit = lambda: remove_member(
        admission, expire=change == "expire", readd=change == "readd"
    )
    with client_for(root, admission=admission) as client:
        value = (
            body(generation)
            if action == "new"
            else body(generation, action="set_enabled", expected=1)
        )
        response = client.post(PATH + "/commands", json=value, headers=WRITE_HEADERS)
        assert response.status_code == 409 and response.json()["status"] == "scope_invalid"
        assert admission.counts() == ((1, 1, 0) if action == "new" else (2, 2, 1))


def test_disable_and_delete_remain_available_after_scope_loss(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    generation = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    with client_for(root, admission=admission) as client:
        client.post(PATH + "/commands", json=body(generation), headers=WRITE_HEADERS)
        remove_member(admission)
        current = publish(
            root, sequence=1, rules=(rule(),), members=(member(version=2, deleted=True),)
        )
        disabled = client.post(
            PATH + "/commands",
            json=body(
                current, command_id="disable-lost", action="set_enabled", expected=1, enabled=False
            ),
            headers=WRITE_HEADERS,
        )
        assert disabled.status_code == 200
        current = publish(root, sequence=2, rules=(rule(version=2, enabled=False),), members=())
        deleted = client.post(
            PATH + "/commands",
            json=body(current, command_id="delete-lost", action="delete", expected=2),
            headers=WRITE_HEADERS,
        )
        assert deleted.status_code == 200 and admission.counts() == (3, 3, 1)


def test_existing_unpublishable_original_still_recovers_before_preflight(tmp_path: Path) -> None:
    from rquant.web.models.price_alert_rules import PriceAlertRuleCommandRequest
    from rquant.web.price_alert_commands import domain_command

    root = tmp_path / "serving"
    generation = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    original = body(generation, threshold="1e65")
    admission.inner.submit(
        domain_command(PriceAlertRuleCommandRequest.model_validate(original)),
        authenticated_owner_id="alice",
    )
    publish(root, sequence=1, activated=False, watchlist_ready=False)
    with client_for(root, admission=admission) as client:
        result = client.post(PATH + "/commands", json=original, headers=WRITE_HEADERS)
        assert result.status_code == 200 and result.json()["status"] == "saved_syncing"
        assert admission.submitted == [] and admission.counts() == (1, 1, 1)


def test_raced_existing_original_is_found_after_preflight_rejects(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    original = body("f" * 64)
    original_lookup = admission.lookup

    def racing_lookup(
        command: PriceRuleCommand, *, authenticated_owner_id: str
    ) -> PageControlReceipt | None:
        if admission.lookup_count == 1:
            admission.inner.submit(command, authenticated_owner_id=authenticated_owner_id)
        return original_lookup(command, authenticated_owner_id=authenticated_owner_id)

    admission.lookup = racing_lookup
    with client_for(root, admission=admission) as client:
        response = client.post(PATH + "/commands", json=original, headers=WRITE_HEADERS)
        assert response.status_code == 200 and response.json()["status"] == "saved_syncing"
        assert admission.submitted == [] and admission.counts() == (1, 1, 1)


@pytest.mark.parametrize(
    "mismatch", ["id", "time", "rule", "action", "version", "deleted", "enabled", "completed"]
)
def test_forged_receipt_never_claims_completion(mismatch: str, tmp_path: Path) -> None:
    root = tmp_path / "serving"
    generation = publish(root)
    value = body(generation)
    result = {
        "rule_id": "rule/a",
        "action": "save",
        "version": 1,
        "deleted": False,
        "enabled": True,
    }
    if mismatch in {"rule", "action", "version", "deleted", "enabled"}:
        result[{"rule": "rule_id"}.get(mismatch, mismatch)] = {
            "rule": "other",
            "action": "delete",
            "version": 2,
            "deleted": True,
            "enabled": False,
        }[mismatch]
    forged = PageControlReceipt(
        command_id="other" if mismatch == "id" else value["command_id"],
        enqueued_at=NOW + timedelta(seconds=1) if mismatch == "time" else NOW,
        completed_at=None if mismatch == "completed" else NOW + timedelta(seconds=2),
        status=PageControlStatus.SUCCEEDED,
        result=result,
    )

    class ForgedAdmission:
        def lookup(
            self, command: PriceRuleCommand, *, authenticated_owner_id: str
        ) -> PageControlReceipt:
            return forged

    with client_for(root, admission=ForgedAdmission()) as client:
        response = client.post(PATH + "/commands/resume", json=value, headers=WRITE_HEADERS)
        assert response.status_code == 502 and response.json()["status"] == "uncertain"
        assert "other" not in response.text


def test_old_or_different_content_and_higher_heads_have_precise_results(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    generation = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    clock = [NOW]
    original = body(generation)
    with client_for(root, admission=admission, clock=lambda: clock[0]) as client:
        client.post(PATH + "/commands", json=original, headers=WRITE_HEADERS)
        publish(root, sequence=1, rules=(rule(),))
        assert (
            client.post(PATH + "/commands/resume", json=original, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "saved_syncing"
        )
        publish(root, sequence=6, rules=(rule(threshold="10.123457", updated=NOW),))
        clock[0] = NOW + timedelta(minutes=2)
        assert (
            client.post(PATH + "/commands/resume", json=original, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "saved_syncing"
        )
        publish(root, sequence=7, rules=(rule(version=3, updated=NOW),))
        clock[0] = NOW + timedelta(minutes=3)
        assert (
            client.post(PATH + "/commands/resume", json=original, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "superseded"
        )


def test_strict_json_csrf_and_identity_prevent_admission(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    gen = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    value = body(gen)
    with client_for(root, admission=admission) as client:
        assert client.post(PATH + "/commands", json=value, headers=HEADERS).status_code == 403
        assert (
            client.post(PATH + "/commands", json=value, headers={"x-rquant-csrf": "1"}).status_code
            == 401
        )
        for extra in ({"owner_id": "bob"}, {"path": "/tmp/a"}, {"enabled": False}):
            assert (
                client.post(
                    PATH + "/commands", json={**value, **extra}, headers=WRITE_HEADERS
                ).status_code
                == 422
            )
        for private_headers in (
            {**WRITE_HEADERS, "x-rquant-proxy-proof": "bad"},
            [*WRITE_HEADERS.items(), ("x-rquant-user", "bob")],
        ):
            assert (
                client.post(PATH + "/commands", json=value, headers=private_headers).status_code
                == 401
            )
        for invalid_threshold in ("NaN", "Infinity", True, None):
            malformed = {**value, "rule": {**value["rule"], "threshold": invalid_threshold}}
            assert (
                client.post(PATH + "/commands", json=malformed, headers=WRITE_HEADERS).status_code
                == 422
            )
        nonfinite = json.dumps(value).replace('"threshold": "10.123456"', '"threshold": NaN')
        assert (
            client.post(
                PATH + "/commands",
                content=nonfinite,
                headers={**WRITE_HEADERS, "content-type": "application/json"},
            ).status_code
            == 422
        )
        raw = json.dumps(value)[:-1] + ', "rule_id": "other"}'
        assert (
            client.post(
                PATH + "/commands",
                content=raw,
                headers={**WRITE_HEADERS, "content-type": "application/json"},
            ).status_code
            == 422
        )
        raw = json.dumps(value)[:-1] + ', "padding": "' + "x" * 8200 + '"}'
        assert (
            client.post(
                PATH + "/commands",
                content=raw,
                headers={**WRITE_HEADERS, "content-type": "application/json"},
            ).status_code
            == 413
        )
        assert admission.counts() == (0, 0, 0)


def test_pointer_changes_during_publication_never_confirm_or_admit_new_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.web.price_alert_commands as commands

    root = tmp_path / "serving"
    generation = publish(root)
    admission = LogicalAdmission(tmp_path / "control.sqlite3")
    original = body(generation)
    clock = [NOW]
    with client_for(root, admission=admission, clock=lambda: clock[0]) as client:
        assert (
            client.post(PATH + "/commands", json=original, headers=WRITE_HEADERS).status_code == 200
        )
        publish(root, sequence=6, rules=(rule(updated=NOW),))
        clock[0] = NOW + timedelta(minutes=3)
        reader = commands.read_price_alert_rules
        sequence = [7]

        def switch_after_read(
            borrowed: BorrowedGeneration, *, owner_id: str, now: datetime
        ) -> PriceAlertRuleView:
            view = reader(borrowed, owner_id=owner_id, now=now)
            publish(root, sequence=sequence[0], rules=(rule(updated=NOW),))
            sequence[0] += 1
            return view

        monkeypatch.setattr(commands, "read_price_alert_rules", switch_after_read)
        assert (
            client.post(PATH + "/commands/resume", json=original, headers=WRITE_HEADERS).json()[
                "status"
            ]
            == "saved_syncing"
        )
        current = client.app.state.web.tracker
        current.refresh()
        with current.borrow() as borrowed:
            assert borrowed is not None
            generation = borrowed.manifest.generation_id
        rejected = client.post(
            PATH + "/commands",
            json=body(generation, command_id="new-after-switch", expected=1),
            headers=WRITE_HEADERS,
        )
        assert rejected.status_code == 409 and admission.counts() == (1, 1, 1)


def test_client_constructor_rejects_same_uid_configuration(tmp_path: Path) -> None:
    import os

    from rquant.price_alert_admission import PriceAlertAdmissionClient

    with pytest.raises(ValueError, match="distinct"):
        PriceAlertAdmissionClient(
            Path("/private/tmp/synthetic-price.sock"),
            expected_service_uid=os.geteuid(),
            shared_gid=os.getegid(),
        )


def test_real_uds_endpoint_with_wrong_identity_is_refused(tmp_path: Path) -> None:
    import os
    import socket
    from tempfile import TemporaryDirectory

    from rquant.price_alert_admission import PriceAlertAdmissionClient
    from rquant.web.models.price_alert_rules import PriceAlertRuleCommandRequest
    from rquant.web.price_alert_commands import domain_command

    short_tmp = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"
    with TemporaryDirectory(prefix="prcfg-", dir=short_tmp) as directory:
        parent = Path(directory)
        endpoint = parent / "price.sock"
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            try:
                listener.bind(str(endpoint))
            except PermissionError:
                pytest.skip("sandbox refused synthetic Unix socket bind; no UDS evidence")
            listener.listen(1)
            parent.chmod(0o710)
            endpoint.chmod(0o660)
            client = PriceAlertAdmissionClient(
                endpoint, expected_service_uid=os.geteuid() + 1, shared_gid=os.getegid()
            )
            with pytest.raises(PriceAlertAdmissionUnavailableError):
                client.lookup(
                    domain_command(PriceAlertRuleCommandRequest.model_validate(body("a" * 64))),
                    authenticated_owner_id="alice",
                )
