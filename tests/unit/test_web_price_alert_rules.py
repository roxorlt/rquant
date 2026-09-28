from __future__ import annotations

import os
from datetime import datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fastapi.testclient import TestClient as RawTestClient

from rquant.alert_price_rule import PriceAlertRule
from rquant.page_control import (
    DeletePriceAlertRule,
    PageControlReceipt,
    PageControlStatus,
    SavePriceAlertRule,
    SetPriceAlertRuleEnabled,
)
from rquant.price_alert_admission import (
    PriceAlertAdmissionRejectedError,
    PriceAlertAdmissionUnavailableError,
)
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
    build_manual_watchlist_projections,
)
from rquant.serving_price_alert_rule_projection import (
    PriceAlertRuleAuthoritySnapshot,
    PriceAlertRuleProjectionRow,
    build_price_alert_rule_projections,
)
from rquant.web.price_alert_rule_read import read_price_alert_rules
from rquant.web.serving import BorrowedGeneration, GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient, create_proof_test_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

PATH = "/api/v1/monitor/rules"
COMMAND_PATH = f"{PATH}/commands"
NOW = FIXTURE_BUILT_AT + timedelta(minutes=5)
HEADERS = {"x-rquant-user": "alice"}
WRITE_HEADERS = {**HEADERS, "x-rquant-csrf": "1", "origin": "http://testserver"}


def _rule(rule_id: str, *, enabled: bool = True) -> PriceAlertRule:
    return PriceAlertRule(
        rule_id=rule_id,
        name="到价提醒",
        priority="P2",
        enabled=enabled,
        comparison="gte",
        threshold=Decimal("10.50"),
        valid_from=time(9, 30),
        valid_until=time(14, 57),
    )


def _member(
    owner: str, code: str, version: int, *, expiry: datetime | None = None
) -> ManualWatchlistProjectionRow:
    return ManualWatchlistProjectionRow(
        owner_id=owner,
        ts_code=code,
        version=version,
        deleted=False,
        source="detail",
        price_levels_json="[]",
        expires_at=expiry,
        updated_at=FIXTURE_BUILT_AT - timedelta(minutes=1),
    )


def _head(
    owner: str,
    rule_id: str,
    code: str | None,
    membership_version: int | None,
    *,
    version: int = 1,
    deleted: bool = False,
    enabled: bool = True,
    updated_at: datetime = FIXTURE_BUILT_AT - timedelta(minutes=1),
) -> PriceAlertRuleProjectionRow:
    return PriceAlertRuleProjectionRow(
        owner_id=owner,
        rule_id=rule_id,
        version=version,
        deleted=deleted,
        ts_code=code,
        membership_version=membership_version,
        name=None if deleted else "到价提醒",
        priority=None if deleted else "P2",
        enabled=None if deleted else enabled,
        comparison=None if deleted else "gte",
        threshold=None if deleted else "10.50",
        valid_from=None if deleted else "09:30:00",
        valid_until=None if deleted else "14:57:00",
        updated_at=updated_at,
    )


def _publish(
    root: Path,
    *,
    sequence: int = 0,
    rules: tuple[PriceAlertRuleProjectionRow, ...] = (),
    members: tuple[ManualWatchlistProjectionRow, ...] = (),
    activated: bool = True,
) -> None:
    activated_at = FIXTURE_BUILT_AT - timedelta(days=1)
    rule_snapshot = (
        PriceAlertRuleAuthoritySnapshot.create(activated_at=activated_at, rows=rules)
        if activated
        else None
    )
    member_snapshot = ManualWatchlistAuthoritySnapshot.create(
        activated_at=activated_at, rows=members
    )
    build_web_fixture(
        root,
        "baseline",
        sequence=sequence,
        signal_projections=(
            *build_manual_watchlist_projections(
                member_snapshot, observed_at=FIXTURE_BUILT_AT + timedelta(minutes=sequence)
            ),
            *build_price_alert_rule_projections(
                rule_snapshot, observed_at=FIXTURE_BUILT_AT + timedelta(minutes=sequence)
            ),
        ),
    )


class _Admission:
    def __init__(
        self, receipt: PageControlReceipt | None = None, *, fail_after_persist: bool = False
    ) -> None:
        self.initial_receipt = receipt
        self.fail_after_persist = fail_after_persist
        self.originals: dict[
            str, tuple[SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule, str]
        ] = {}
        self.receipts: dict[str, PageControlReceipt] = {}
        self.submitted: list[
            tuple[SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule, str]
        ] = []
        self.resumed: list[
            tuple[SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule, str]
        ] = []

    def lookup(
        self,
        command: SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule,
        *,
        authenticated_owner_id: str,
    ) -> PageControlReceipt | None:
        original = self.originals.get(command.command_id)
        if original is None:
            return None
        if original != (command, authenticated_owner_id):
            raise PriceAlertAdmissionRejectedError("command_conflict")
        return self.receipts[command.command_id]

    def submit(
        self,
        command: SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule,
        *,
        authenticated_owner_id: str,
    ) -> PageControlReceipt:
        original = (command, authenticated_owner_id)
        self.originals[command.command_id] = original
        self.submitted.append(original)
        receipt = self.initial_receipt
        if receipt is None:
            action = (
                "save"
                if isinstance(command, SavePriceAlertRule)
                else "set_enabled"
                if isinstance(command, SetPriceAlertRuleEnabled)
                else "delete"
            )
            rule_id = (
                command.rule.rule_id if isinstance(command, SavePriceAlertRule) else command.rule_id
            )
            version = command.expected_version or 0
            receipt = PageControlReceipt(
                command_id=command.command_id,
                status=PageControlStatus.SUCCEEDED,
                enqueued_at=command.requested_at,
                completed_at=NOW,
                result={
                    "rule_id": rule_id,
                    "action": action,
                    "version": version + 1,
                    "deleted": action == "delete",
                    "enabled": None
                    if action == "delete"
                    else (
                        command.rule.enabled
                        if isinstance(command, SavePriceAlertRule)
                        else command.enabled
                    ),
                },
            )
        self.receipts[command.command_id] = receipt
        if self.fail_after_persist:
            raise PriceAlertAdmissionUnavailableError("lost response")
        return receipt

    def resume(
        self,
        command: SavePriceAlertRule | SetPriceAlertRuleEnabled | DeletePriceAlertRule,
        *,
        authenticated_owner_id: str,
    ) -> PageControlReceipt:
        self.resumed.append((command, authenticated_owner_id))
        receipt = self.lookup(command, authenticated_owner_id=authenticated_owner_id)
        if receipt is None:
            raise PriceAlertAdmissionRejectedError("not_found")
        return receipt


def _client(
    root: Path, admission: _Admission | None = None, *, now: datetime = NOW
) -> ProofTestClient:
    settings = WebSettings(
        serving_root=root,
        ingress_socket_path=root.parent / "web-private" / "web.sock",
        price_rule_admission_socket_path=(root.parent / "price-private" / "price.sock")
        if admission is not None
        else None,
        price_rule_admission_service_uid=os.geteuid() + 1 if admission is not None else None,
        price_rule_admission_shared_gid=os.getegid() + 1 if admission is not None else None,
    )
    return ProofTestClient(
        create_proof_test_app(
            settings,
            clock=lambda: now,
            background=False,
            price_rule_admission_client=admission,
        )
    )


def _save_body(generation_id: str, *, command_id: str = "price-first") -> dict[str, object]:
    return {
        "kind": "save_price_alert_rule",
        "command_id": command_id,
        "requested_at": NOW.isoformat(),
        "generation_id": generation_id,
        "ts_code": "600001.SH",
        "membership_version": 1,
        "expected_version": None,
        "rule": _rule("threshold-a").model_dump(mode="json"),
    }


def test_private_owner_read_reports_binding_without_claiming_evaluation(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(
            _head("alice", "active", "600001.SH", 1),
            _head("alice", "expired", "600002.SH", 1),
            _head("alice", "changed", "600003.SH", 1),
            _head("alice", "removed", "600004.SH", 1),
            _head("alice", "deleted", None, None, version=2, deleted=True),
            _head("bob", "secret", "600001.SH", 1),
        ),
        members=(
            _member("alice", "600001.SH", 1),
            _member("alice", "600002.SH", 1, expiry=NOW),
            _member("alice", "600003.SH", 2),
            _member("bob", "600001.SH", 1),
        ),
    )
    with _client(root) as client:
        response = client.get(PATH, headers=HEADERS)
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["availability"] == "ready"
    assert data["evaluation_running"] is False
    assert {item["rule_id"]: item["scope_status"] for item in data["items"]} == {
        "active": "valid",
        "expired": "expired",
        "changed": "changed",
        "removed": "removed",
        "deleted": "deleted",
    }
    assert data["items"][0]["threshold"] == "10.50"
    assert "secret" not in response.text
    assert "bob" not in response.text


def test_private_read_requires_proof_and_never_guesses_empty_from_unknown(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, activated=False)
    with _client(root) as client:
        assert client.get(PATH).status_code == 401
        not_ready = client.get(PATH, headers=HEADERS).json()["data"]
        assert not_ready["availability"] == "not_ready"
        assert not_ready["items"] == []
        assert client.get(f"{PATH}?owner_id=bob", headers=HEADERS).status_code == 422
    with RawTestClient(
        create_proof_test_app(
            WebSettings(
                serving_root=root, ingress_socket_path=tmp_path / "web-private" / "web.sock"
            ),
            clock=lambda: NOW,
            background=False,
        )
    ) as client:
        assert client.get(PATH, headers=HEADERS).status_code == 401
    with ProofTestClient(
        create_proof_test_app(WebSettings(serving_root=root), clock=lambda: NOW, background=False)
    ) as client:
        assert client.get(PATH, headers=HEADERS).status_code == 503


def test_price_rule_socket_requires_explicit_distinct_private_identity(tmp_path: Path) -> None:
    ingress = tmp_path / "web-private" / "web.sock"
    admission = tmp_path / "price-private" / "price.sock"
    assert WebSettings.from_env({}).price_rule_admission_socket_path is None
    with pytest.raises(ValueError, match="configured together"):
        WebSettings(
            serving_root=tmp_path,
            ingress_socket_path=ingress,
            price_rule_admission_socket_path=admission,
        )
    with pytest.raises(ValueError, match="private Web ingress"):
        WebSettings(
            serving_root=tmp_path,
            price_rule_admission_socket_path=admission,
            price_rule_admission_service_uid=os.geteuid() + 1,
            price_rule_admission_shared_gid=os.getegid() + 1,
        )
    with pytest.raises(ValueError, match="separate directory"):
        WebSettings(
            serving_root=tmp_path,
            ingress_socket_path=ingress,
            price_rule_admission_socket_path=ingress.parent / "price.sock",
            price_rule_admission_service_uid=os.geteuid() + 1,
            price_rule_admission_shared_gid=os.getegid() + 1,
        )
    with pytest.raises(ValueError, match="differ from Web"):
        WebSettings(
            serving_root=tmp_path,
            ingress_socket_path=ingress,
            price_rule_admission_socket_path=admission,
            price_rule_admission_service_uid=os.geteuid(),
            price_rule_admission_shared_gid=os.getegid() + 1,
        )
    configured = WebSettings.from_env(
        {
            "RQUANT_WEB_INGRESS_SOCKET": str(ingress),
            "RQUANT_WEB_PRICE_RULE_ADMISSION_SOCKET": str(admission),
            "RQUANT_WEB_PRICE_RULE_ADMISSION_SERVICE_UID": str(os.geteuid() + 1),
            "RQUANT_WEB_PRICE_RULE_ADMISSION_SHARED_GID": str(os.getegid() + 1),
        }
    )
    assert configured.price_rule_admission_socket_path == admission


def test_save_lookup_first_and_current_generation_preflight(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, members=(_member("alice", "600001.SH", 1),))
    admission = _Admission()
    with _client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _save_body(generation)
        response = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert response.status_code == 200
        assert response.json()["status"] == "saved_syncing"
        command, owner = admission.submitted[0]
        assert isinstance(command, SavePriceAlertRule)
        assert owner == "alice"
        assert command.membership_version == 1
        assert (
            client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
        assert len(admission.submitted) == 1
        other_owner = client.post(
            COMMAND_PATH, json=body, headers={**WRITE_HEADERS, "x-rquant-user": "bob"}
        )
        assert other_owner.status_code == 409
        assert other_owner.json()["reason"] == "command_conflict"
        changed_body = client.post(
            COMMAND_PATH,
            json=body | {"membership_version": 2},
            headers=WRITE_HEADERS,
        )
        assert changed_body.status_code == 409
        assert changed_body.json()["reason"] == "command_conflict"
        assert (
            client.post(
                COMMAND_PATH, json=body | {"owner_id": "bob"}, headers=WRITE_HEADERS
            ).status_code
            == 422
        )
    _publish(root, sequence=6, members=(_member("alice", "600001.SH", 2),))
    with _client(root, _Admission(), now=NOW + timedelta(minutes=2)) as client:
        stale = client.post(
            COMMAND_PATH, json=_save_body(generation, command_id="stale"), headers=WRITE_HEADERS
        )
        assert stale.status_code == 409
        assert stale.json()["reason"] == "generation_changed"


def test_write_needs_login_csrf_private_socket_and_valid_body(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, members=(_member("alice", "600001.SH", 1),))
    admission = _Admission()
    with _client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _save_body(generation)
        assert client.post(COMMAND_PATH, json=body).status_code == 401
        assert client.post(COMMAND_PATH, json=body, headers=HEADERS).status_code == 403
        assert (
            client.post(COMMAND_PATH, json=body | {"rule": {}}, headers=WRITE_HEADERS).status_code
            == 422
        )
        assert (
            client.post(
                COMMAND_PATH, json=body | {"owner_id": "bob"}, headers=WRITE_HEADERS
            ).status_code
            == 422
        )
        assert admission.submitted == []
    with _client(root) as client:
        assert client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).status_code == 503


def test_toggle_delete_cas_and_old_pending_command_recovery(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(_head("alice", "threshold-a", "600001.SH", 1, version=2),),
        members=(_member("alice", "600001.SH", 1),),
    )
    pending = _Admission()
    with _client(root, pending) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        toggle = {
            "kind": "set_price_alert_rule_enabled",
            "command_id": "toggle-first",
            "requested_at": NOW.isoformat(),
            "generation_id": generation,
            "rule_id": "threshold-a",
            "expected_version": 2,
            "enabled": False,
        }
        assert (
            client.post(COMMAND_PATH, json=toggle, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
        assert isinstance(pending.submitted[0][0], SetPriceAlertRuleEnabled)
        wrong = toggle | {"command_id": "wrong-cas", "expected_version": 1}
        wrong_response = client.post(COMMAND_PATH, json=wrong, headers=WRITE_HEADERS)
        assert wrong_response.status_code == 409
        assert wrong_response.json()["reason"] == "version_conflict"
        delete = toggle | {"kind": "delete_price_alert_rule", "command_id": "delete-first"}
        delete.pop("enabled")
        assert (
            client.post(COMMAND_PATH, json=delete, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
        assert isinstance(pending.submitted[-1][0], DeletePriceAlertRule)


def test_response_loss_and_pending_retry_keep_the_original_command(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, members=(_member("alice", "600001.SH", 1),))
    lost = _Admission(fail_after_persist=True)
    with _client(root, lost) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _save_body(generation)
        recovered = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert recovered.json()["status"] == "saved_syncing"
        assert len(lost.submitted) == 1

    pending = _Admission(
        PageControlReceipt(
            command_id="pending-price",
            status=PageControlStatus.PENDING,
            enqueued_at=NOW,
        )
    )
    with _client(root, pending) as client:
        body = _save_body(generation, command_id="pending-price")
        assert (
            client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).json()["status"]
            == "pending"
        )
    _publish(root, sequence=6, members=(_member("alice", "600001.SH", 2),))
    with _client(root, pending, now=NOW + timedelta(minutes=2)) as client:
        retry = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert retry.json()["status"] == "pending"
    assert len(pending.submitted) == 1
    assert len(pending.resumed) == 1


def test_published_requires_new_exact_owner_version_and_facts(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, members=(_member("alice", "600001.SH", 1),))
    admission = _Admission()
    with _client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _save_body(generation)
        assert (
            client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
    _publish(
        root,
        sequence=6,
        rules=(_head("alice", "threshold-a", "600001.SH", 1, updated_at=NOW),),
        members=(_member("alice", "600001.SH", 1),),
    )
    with _client(root, admission, now=NOW + timedelta(minutes=2)) as client:
        published = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert published.json()["status"] == "published"
        assert len(admission.submitted) == 1
    _publish(
        root,
        sequence=7,
        rules=(
            _head(
                "alice",
                "threshold-a",
                "600001.SH",
                1,
                version=2,
                updated_at=NOW + timedelta(minutes=1),
            ),
        ),
        members=(_member("alice", "600001.SH", 1),),
    )
    with _client(root, admission, now=NOW + timedelta(minutes=3)) as client:
        superseded = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert superseded.json()["status"] == "saved_syncing"


def test_delete_publication_requires_tombstone(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(_head("alice", "threshold-a", "600001.SH", 1),),
        members=(_member("alice", "600001.SH", 1),),
    )
    admission = _Admission()
    with _client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = {
            "kind": "delete_price_alert_rule",
            "command_id": "delete-price",
            "requested_at": NOW.isoformat(),
            "generation_id": generation,
            "rule_id": "threshold-a",
            "expected_version": 1,
        }
        assert (
            client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).json()["status"]
            == "saved_syncing"
        )
    _publish(
        root,
        sequence=6,
        rules=(_head("alice", "threshold-a", None, None, version=2, deleted=True, updated_at=NOW),),
        members=(_member("alice", "600001.SH", 1),),
    )
    with _client(root, admission, now=NOW + timedelta(minutes=2)) as client:
        assert (
            client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).json()["status"]
            == "published"
        )


@pytest.mark.parametrize(
    ("code", "status", "reason"),
    (
        ("version_conflict", "conflict", "version_conflict"),
        ("scope_invalid", "conflict", "membership_changed"),
        ("capacity_exceeded", "capacity", "capacity_exceeded"),
    ),
)
def test_durable_failures_have_specific_safe_status(
    tmp_path: Path, code: str, status: str, reason: str
) -> None:
    root = tmp_path / "serving"
    _publish(root, members=(_member("alice", "600001.SH", 1),))
    failed = PageControlReceipt(
        command_id="price-first",
        status=PageControlStatus.FAILED,
        enqueued_at=NOW,
        completed_at=NOW,
        result={"rule_id": "threshold-a", "action": "save", "code": code},
        error="private internal error",
    )
    with _client(root, _Admission(failed)) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        response = client.post(COMMAND_PATH, json=_save_body(generation), headers=WRITE_HEADERS)
    assert response.status_code == 409
    assert response.json()["status"] == status
    assert response.json()["reason"] == reason
    assert "private internal error" not in response.text


def test_current_member_mismatch_has_distinct_conflict_reason(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, members=(_member("alice", "600001.SH", 2),))
    admission = _Admission()
    with _client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        response = client.post(COMMAND_PATH, json=_save_body(generation), headers=WRITE_HEADERS)
    assert response.status_code == 409
    assert response.json()["reason"] == "membership_changed"
    assert admission.submitted == []


def test_bad_projection_pair_and_stale_generation_are_unavailable(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(_head("alice", "threshold-a", "600001.SH", 1),),
        members=(_member("alice", "600001.SH", 1),),
    )
    tracker = GenerationTracker(root)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            counts = dict(borrowed.manifest.row_counts)
            counts["price_alert_rule"] += 1
            broken = BorrowedGeneration(
                manifest=borrowed.manifest.model_copy(update={"row_counts": counts}),
                pointer=borrowed.pointer,
                cursor=borrowed.cursor,
                fallback_detail=None,
            )
            with pytest.raises(ValueError, match="manifest and status differ"):
                read_price_alert_rules(broken, now=NOW)
    finally:
        tracker.close()
    with _client(root, now=NOW + timedelta(hours=2)) as client:
        stale = client.get(PATH, headers=HEADERS).json()["data"]
        assert stale["availability"] == "unavailable"
        assert stale["items"] == []


@pytest.mark.parametrize("state_table", ("price_alert_rule_state", "manual_watchlist_state"))
def test_read_rejects_corrupt_authority_digest(tmp_path: Path, state_table: str) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rules=(_head("alice", "threshold-a", "600001.SH", 1),),
        members=(_member("alice", "600001.SH", 1),),
    )
    tracker = GenerationTracker(root)
    tracker.refresh()

    class CorruptStateCursor:
        def __init__(self, actual: object) -> None:
            self.actual = actual
            self.corrupt = False

        def execute(self, query: str, parameters: object = None) -> CorruptStateCursor:
            self.corrupt = f"FROM {state_table} " in query
            if parameters is None:
                self.actual.execute(query)
            else:
                self.actual.execute(query, parameters)
            return self

        def fetchall(self) -> list[tuple[object, ...]]:
            rows = self.actual.fetchall()
            if not self.corrupt:
                return rows
            return [(*row[:4], "0" * 64) for row in rows]

    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            broken = BorrowedGeneration(
                manifest=borrowed.manifest,
                pointer=borrowed.pointer,
                cursor=CorruptStateCursor(borrowed.cursor),
                fallback_detail=None,
            )
            with pytest.raises(ValueError, match="digest mismatch"):
                read_price_alert_rules(broken, now=NOW)
    finally:
        tracker.close()
