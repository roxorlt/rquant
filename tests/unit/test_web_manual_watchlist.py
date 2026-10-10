from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from rquant.page_control import (
    AddWatchlistItem,
    PageControlConsumer,
    PageControlOutbox,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
    RemoveWatchlistItem,
)
from rquant.serving_manual_watchlist_projection import (
    ManualWatchlistAuthoritySnapshot,
    ManualWatchlistProjectionRow,
    build_manual_watchlist_projections,
)
from rquant.watchlist_admission import (
    WatchlistAdmission,
    WatchlistAdmissionClient,
    WatchlistAdmissionRejectedError,
    WatchlistAdmissionUnavailableError,
    build_watchlist_admission_server,
)
from rquant.web.manual_watchlist_read import read_manual_watchlist, read_manual_watchlist_item
from rquant.web.serving import BorrowedGeneration, GenerationTracker
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient as TestClient
from tests.support.web_proxy_identity import create_proof_test_app as create_app
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

PATH = "/api/v1/watchlist"
COMMAND_PATH = f"{PATH}/commands"
NOW = FIXTURE_BUILT_AT + timedelta(minutes=5)
HEADERS = {"x-rquant-user": "alice"}
WRITE_HEADERS = {**HEADERS, "x-rquant-csrf": "1", "origin": "http://testserver"}
SHORT_TMP = "/private/tmp" if Path("/private/tmp").is_dir() else "/tmp"


def _client(root: Path, *, private: bool = True, now: datetime = NOW) -> TestClient:
    settings = WebSettings(
        serving_root=root,
        ingress_socket_path=root.parent / "private.sock" if private else None,
    )
    return TestClient(create_app(settings, clock=lambda: now, background=False))


def _publish(
    root: Path,
    *,
    sequence: int = 0,
    rows: tuple[ManualWatchlistProjectionRow, ...] = (),
    activated: bool = True,
) -> None:
    built_at = FIXTURE_BUILT_AT + timedelta(minutes=sequence)
    snapshot = (
        ManualWatchlistAuthoritySnapshot.create(
            activated_at=FIXTURE_BUILT_AT - timedelta(days=1), rows=rows
        )
        if activated
        else None
    )
    build_web_fixture(
        root,
        "baseline",
        sequence=sequence,
        signal_projections=build_manual_watchlist_projections(snapshot, observed_at=built_at),
    )


def _row(
    owner: str,
    code: str,
    version: int,
    *,
    deleted: bool = False,
    expiry: datetime | None = None,
    updated: datetime = FIXTURE_BUILT_AT - timedelta(minutes=1),
) -> ManualWatchlistProjectionRow:
    return ManualWatchlistProjectionRow(
        owner_id=owner,
        ts_code=code,
        version=version,
        deleted=deleted,
        source=None if deleted else "detail",
        price_levels_json="[]" if deleted else '["10.00","12.345"]',
        expires_at=expiry,
        updated_at=None if deleted else updated,
    )


def test_private_ingress_and_authenticated_user_are_required(tmp_path: Path) -> None:
    _publish(tmp_path / "serving")
    with _client(tmp_path / "serving", private=False) as client:
        assert client.get(PATH, headers=HEADERS).status_code == 503
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).status_code == 503
    with _client(tmp_path / "serving") as client:
        assert client.get(PATH).status_code == 401
        assert client.get(f"{PATH}/600001.SH").status_code == 401
        assert client.get(f"{PATH}/bad").status_code == 401
        assert client.get(PATH, headers={"x-rquant-user": "bad name"}).status_code == 401


def test_list_and_exact_status_are_isolated_and_keep_decimal_strings(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(
        root,
        rows=(
            _row("alice", "600001.SH", 1),
            _row("alice", "600002.SH", 2, expiry=NOW),
            _row("alice", "600003.SH", 3, deleted=True),
            _row("bob", "600004.SH", 4),
        ),
    )
    with _client(root) as client:
        listed = client.get(PATH, headers=HEADERS)
        assert listed.status_code == 200
        data = listed.json()["data"]
        assert data["availability"] == "ready"
        assert [item["ts_code"] for item in data["items"]] == ["600001.SH"]
        assert data["items"][0]["price_levels"] == ["10.00", "12.345"]
        for code, status, version in (
            ("600001.SH", "active", 1),
            ("600002.SH", "expired", 2),
            ("600003.SH", "deleted", 3),
            ("600004.SH", "absent", None),
            ("600005.SH", "absent", None),
        ):
            exact = client.get(f"{PATH}/{code}", headers=HEADERS)
            assert exact.status_code == 200
            assert (exact.json()["data"]["status"], exact.json()["data"]["version"]) == (
                status,
                version,
            )
        bob = client.get(PATH, headers={"x-rquant-user": "bob"}).json()["data"]
        assert [item["ts_code"] for item in bob["items"]] == ["600004.SH"]


def test_request_clock_drives_expiry_and_trusted_empty_is_distinct(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 7, expiry=NOW + timedelta(seconds=1)),))
    with _client(root) as client:
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] == "active"
    with _client(root, now=NOW + timedelta(seconds=1)) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["items"] == []
        assert (
            client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] == "expired"
        )
    _publish(root, sequence=1)
    with _client(root, now=NOW + timedelta(minutes=1)) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "ready"
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] == "absent"


def test_old_unactivated_and_stale_generations_never_mean_absent(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    with _client(root) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "unavailable"
    build_web_fixture(root, "baseline")
    with _client(root) as client:
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] is None
    _publish(root, sequence=1, activated=False)
    with _client(root, now=NOW + timedelta(minutes=1)) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "unavailable"
    _publish(root, sequence=2)
    with _client(root, now=NOW + timedelta(hours=2)) as client:
        assert client.get(f"{PATH}/600001.SH", headers=HEADERS).json()["data"]["status"] is None


def test_owner_query_and_invalid_code_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    with _client(root) as client:
        assert client.get(f"{PATH}?owner_id=bob", headers=HEADERS).status_code == 422
        assert client.get(f"{PATH}/600001.SH?owner=bob", headers=HEADERS).status_code == 422
        invalid = client.get(f"{PATH}/bad", headers=HEADERS)
        assert invalid.status_code == 422
        assert invalid.json() == {"detail": "股票代码有误，请检查后重试。"}


def test_degraded_generation_is_unavailable_even_when_old_members_exist(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    tracker = GenerationTracker(root, pointer_check_seconds=3600)
    settings = WebSettings(serving_root=root, ingress_socket_path=tmp_path / "private.sock")
    app = create_app(settings, tracker=tracker, clock=lambda: NOW, background=False)
    with TestClient(app) as client:
        assert client.get(PATH, headers=HEADERS).json()["data"]["availability"] == "ready"
        tracker._record_failure("new generation verification failed")
        unavailable = client.get(PATH, headers=HEADERS).json()
        assert unavailable["data"]["availability"] == "unavailable"
        assert unavailable["data"]["items"] == []
        assert unavailable["serving"]["state"] == "degraded"
        assert "new generation verification failed" not in str(unavailable)


def test_manifest_count_mismatch_blocks_positive_membership(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    tracker = GenerationTracker(root)
    tracker.refresh()
    try:
        with tracker.borrow() as borrowed:
            assert borrowed is not None
            counts = dict(borrowed.manifest.row_counts)
            counts["manual_watchlist"] += 1
            broken = BorrowedGeneration(
                manifest=borrowed.manifest.model_copy(update={"row_counts": counts}),
                pointer=borrowed.pointer,
                cursor=borrowed.cursor,
                fallback_detail=None,
            )
            with pytest.raises(ValueError, match="manifest and status differ"):
                read_manual_watchlist(broken, owner_id="alice", now=NOW)
    finally:
        tracker.close()


def test_list_refuses_more_than_five_hundred_active_members(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    rows = tuple(_row("alice", f"{600000 + number:06d}.SH", 1) for number in range(501))
    _publish(root, rows=rows)
    with _client(root) as client:
        listed = client.get(PATH, headers=HEADERS).json()["data"]
        assert listed["availability"] == "unavailable"
        assert listed["items"] == []
        assert client.get(f"{PATH}/600000.SH", headers=HEADERS).json()["data"]["status"] == "active"


def test_watchlist_command_requires_private_ingress_and_independent_socket(
    tmp_path: Path,
) -> None:
    assert WebSettings.from_env({}).watchlist_admission_socket_path is None
    socket_path = tmp_path / "watchlist-admission" / "watchlist.sock"
    ingress_path = tmp_path / "web-private" / "web.sock"
    with pytest.raises(ValueError, match="private Web ingress"):
        WebSettings(serving_root=tmp_path, watchlist_admission_socket_path=socket_path)
    with pytest.raises(ValueError, match="separate|independent"):
        WebSettings(
            serving_root=tmp_path,
            ingress_socket_path=ingress_path,
            watchlist_admission_socket_path=ingress_path.parent / "watchlist.sock",
        )
    configured = WebSettings.from_env(
        {
            "RQUANT_WEB_INGRESS_SOCKET": str(ingress_path),
            "RQUANT_WEB_WATCHLIST_ADMISSION_SOCKET": str(socket_path),
        }
    )
    assert configured.watchlist_admission_socket_path == socket_path


def test_optional_price_levels_have_no_array_default_in_openapi() -> None:
    app = create_app(WebSettings(serving_root=Path("data/runtime/serving")), background=False)
    schema = app.openapi()["components"]["schemas"]["ManualWatchlistCommandRequest"]
    field = schema["properties"]["price_levels"]
    assert "price_levels" not in schema["required"]
    assert "default" not in field
    assert {variant.get("type") for variant in field["anyOf"]} == {"array", "null"}


class _RecordingAdmission:
    def __init__(
        self,
        receipt: PageControlReceipt | None = None,
        *,
        fail_after_persist: bool = False,
    ) -> None:
        self.receipt = receipt
        self.fail_after_persist = fail_after_persist
        self.original: tuple[AddWatchlistItem | RemoveWatchlistItem, str] | None = None
        self.submitted: list[tuple[AddWatchlistItem | RemoveWatchlistItem, str]] = []
        self.resumed: list[tuple[AddWatchlistItem | RemoveWatchlistItem, str]] = []
        self.looked_up: list[tuple[AddWatchlistItem | RemoveWatchlistItem, str]] = []

    def lookup(
        self,
        command: AddWatchlistItem | RemoveWatchlistItem,
        *,
        authenticated_owner_id: str,
    ) -> PageControlReceipt | None:
        self.looked_up.append((command, authenticated_owner_id))
        if self.original is None:
            return None
        if self.original != (command, authenticated_owner_id):
            raise WatchlistAdmissionRejectedError("command_conflict")
        return self.receipt

    def submit(
        self,
        command: AddWatchlistItem | RemoveWatchlistItem,
        *,
        authenticated_owner_id: str,
    ) -> PageControlReceipt:
        self.submitted.append((command, authenticated_owner_id))
        self.original = (command, authenticated_owner_id)
        if self.receipt is None:
            self.receipt = PageControlReceipt(
                command_id=command.command_id,
                status=PageControlStatus.SUCCEEDED,
                enqueued_at=command.requested_at,
                completed_at=NOW,
                result={
                    "ts_code": command.item.ts_code,
                    "action": "add" if isinstance(command, AddWatchlistItem) else "remove",
                    "version": (command.item.expected_version or 0) + 1,
                    "state": "active" if isinstance(command, AddWatchlistItem) else "deleted",
                },
            )
        if self.fail_after_persist:
            raise WatchlistAdmissionUnavailableError("response lost")
        return self.receipt

    def resume(
        self,
        command: AddWatchlistItem | RemoveWatchlistItem,
        *,
        authenticated_owner_id: str,
    ) -> PageControlReceipt:
        self.resumed.append((command, authenticated_owner_id))
        result = self.lookup(command, authenticated_owner_id=authenticated_owner_id)
        if result is None:
            raise WatchlistAdmissionRejectedError("not_found")
        return result


def _command_body(generation_id: str) -> dict[str, object]:
    return {
        "command_id": "watchlist-first",
        "requested_at": NOW.isoformat(),
        "generation_id": generation_id,
        "ts_code": "600001.SH",
        "action": "add",
        "expected_version": None,
        "source": "detail",
        "price_levels": ["10.00", "12.35"],
    }


def _command_client(
    root: Path,
    admission: _RecordingAdmission | None,
    *,
    private: bool = True,
    now: datetime = NOW,
) -> TestClient:
    settings = WebSettings(
        serving_root=root,
        ingress_socket_path=root.parent / "web-private" / "web.sock" if private else None,
        watchlist_admission_socket_path=(root.parent / "watchlist-private" / "watchlist.sock")
        if admission is not None
        else None,
    )
    return TestClient(
        create_app(
            settings,
            clock=lambda: now,
            background=False,
            watchlist_admission_client=admission,
        )
    )


def test_new_command_injects_authenticated_owner_and_reports_saved_syncing(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    _publish(root)
    admission = _RecordingAdmission()
    settings = WebSettings(
        serving_root=root,
        ingress_socket_path=tmp_path / "web-private" / "web.sock",
        watchlist_admission_socket_path=tmp_path / "watchlist-private" / "watchlist.sock",
    )
    with TestClient(
        create_app(
            settings,
            clock=lambda: NOW,
            background=False,
            watchlist_admission_client=admission,
        )
    ) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        response = client.post(COMMAND_PATH, json=_command_body(generation), headers=WRITE_HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == "saved_syncing"
    assert response.json()["version"] == 1
    assert "已保存，正在同步" in response.json()["message"]
    command, owner = admission.submitted[0]
    assert owner == command.item.owner_id == "alice"
    assert tuple(str(level) for level in command.item.price_levels) == ("10.00", "12.35")


def test_add_omitted_levels_are_empty_and_remove_forbids_explicit_levels(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    add_admission = _RecordingAdmission()
    with _command_client(root, add_admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        add = _command_body(generation)
        add.pop("price_levels")
        assert client.post(COMMAND_PATH, json=add, headers=WRITE_HEADERS).status_code == 200
        assert (
            client.post(
                COMMAND_PATH, json=add | {"price_levels": None}, headers=WRITE_HEADERS
            ).status_code
            == 422
        )
    assert add_admission.submitted[0][0].item.price_levels == ()

    _publish(root, sequence=1, rows=(_row("alice", "600001.SH", 1),))
    remove_admission = _RecordingAdmission()
    with _command_client(root, remove_admission, now=NOW + timedelta(minutes=1)) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        remove = _command_body(generation) | {
            "command_id": "watchlist-remove",
            "action": "remove",
            "expected_version": 1,
        }
        remove.pop("source")
        remove.pop("price_levels")
        assert client.post(COMMAND_PATH, json=remove, headers=WRITE_HEADERS).status_code == 200
        for levels in ([], None):
            assert (
                client.post(
                    COMMAND_PATH,
                    json=remove | {"price_levels": levels},
                    headers=WRITE_HEADERS,
                ).status_code
                == 422
            )
    assert len(remove_admission.submitted) == 1


def test_watchlist_post_requires_private_socket_login_csrf_and_bounded_json(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    admission = _RecordingAdmission()
    with _command_client(root, None, private=False) as client:
        assert (
            client.post(
                COMMAND_PATH, json=_command_body("a" * 64), headers=WRITE_HEADERS
            ).status_code
            == 503
        )
    with _command_client(root, None) as client:
        assert (
            client.post(
                COMMAND_PATH, json=_command_body("a" * 64), headers=WRITE_HEADERS
            ).status_code
            == 503
        )
    with _command_client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation)
        assert client.post(COMMAND_PATH, json=body).status_code == 401
        assert client.post(COMMAND_PATH, json=body, headers=HEADERS).status_code == 403
        assert (
            client.post(
                COMMAND_PATH,
                json=body,
                headers={**WRITE_HEADERS, "origin": "https://outside.example"},
            ).status_code
            == 403
        )
        assert (
            client.post(
                COMMAND_PATH, json=body | {"owner_id": "bob"}, headers=WRITE_HEADERS
            ).status_code
            == 422
        )
        assert (
            client.post(
                COMMAND_PATH,
                content=b"{",
                headers={**WRITE_HEADERS, "content-type": "application/json"},
            ).status_code
            == 422
        )
        assert (
            client.post(
                COMMAND_PATH,
                content=b"{}" + b" " * 4096,
                headers={**WRITE_HEADERS, "content-type": "application/json"},
            ).status_code
            == 413
        )
        assert (
            client.post(
                COMMAND_PATH, content=b"{}", headers={**WRITE_HEADERS, "content-type": "text/plain"}
            ).status_code
            == 415
        )
    assert admission.submitted == []


def test_new_commands_require_current_ready_generation_and_exact_cas(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, activated=False)
    admission = _RecordingAdmission()
    with _command_client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        unavailable = client.post(
            COMMAND_PATH, json=_command_body(generation), headers=WRITE_HEADERS
        )
        assert unavailable.status_code == 503
        assert unavailable.json()["status"] == "uncertain"
    _publish(root, sequence=1, rows=(_row("alice", "600001.SH", 2),))
    with _command_client(root, admission, now=NOW + timedelta(minutes=1)) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        old_generation = client.post(
            COMMAND_PATH, json=_command_body("a" * 64), headers=WRITE_HEADERS
        )
        assert old_generation.status_code == 409
        assert old_generation.json()["status"] == "conflict"
        old_version = client.post(
            COMMAND_PATH, json=_command_body(generation), headers=WRITE_HEADERS
        )
        assert old_version.status_code == 409
        assert old_version.json()["status"] == "conflict"
        remove = _command_body(generation) | {"action": "remove", "expected_version": 2}
        remove.pop("source")
        remove.pop("price_levels")
        assert client.post(COMMAND_PATH, json=remove, headers=WRITE_HEADERS).status_code == 200
    assert len(admission.submitted) == 1
    assert isinstance(admission.submitted[0][0], RemoveWatchlistItem)


def test_pointer_change_during_exact_read_refuses_new_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    _publish(root)
    admission = _RecordingAdmission()

    def change_pointer_during_read(
        borrowed: BorrowedGeneration, *, owner_id: str, ts_code: str
    ) -> tuple[datetime | None, ManualWatchlistProjectionRow | None]:
        answer = read_manual_watchlist_item(borrowed, owner_id=owner_id, ts_code=ts_code)
        _publish(root, sequence=1)
        return answer

    with _command_client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        monkeypatch.setattr(
            "rquant.web.routes.manual_watchlist.read_manual_watchlist_item",
            change_pointer_during_read,
        )
        response = client.post(COMMAND_PATH, json=_command_body(generation), headers=WRITE_HEADERS)
    assert response.status_code == 409
    assert response.json()["status"] == "conflict"
    assert admission.submitted == []


def test_preflight_rejection_rechecks_a_racing_original_command(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    with _client(root) as client:
        old_generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
    _publish(root, sequence=1)

    class OriginalAppearsOnSecondLookup(_RecordingAdmission):
        def lookup(
            self,
            command: AddWatchlistItem | RemoveWatchlistItem,
            *,
            authenticated_owner_id: str,
        ) -> PageControlReceipt | None:
            if not self.looked_up:
                self.looked_up.append((command, authenticated_owner_id))
                return None
            self.original = (command, authenticated_owner_id)
            return super().lookup(command, authenticated_owner_id=authenticated_owner_id)

    admission = OriginalAppearsOnSecondLookup(
        receipt=PageControlReceipt(
            command_id="watchlist-first",
            status=PageControlStatus.SUCCEEDED,
            enqueued_at=NOW,
            completed_at=NOW,
            result={"ts_code": "600001.SH", "action": "add", "version": 1, "state": "active"},
        )
    )
    with _command_client(root, admission, now=NOW + timedelta(minutes=1)) as client:
        response = client.post(
            COMMAND_PATH, json=_command_body(old_generation), headers=WRITE_HEADERS
        )
    assert response.status_code == 200
    assert response.json()["status"] == "saved_syncing"
    assert len(admission.looked_up) == 2
    assert admission.submitted == []


def test_original_command_survives_generation_change_and_changed_content_conflicts(
    tmp_path: Path,
) -> None:
    root = tmp_path / "serving"
    _publish(root)
    admission = _RecordingAdmission()
    with _command_client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation)
        first = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert first.status_code == 200
        assert first.json()["status"] == "saved_syncing"
    _publish(
        root,
        sequence=6,
        rows=(_row("alice", "600001.SH", 1, updated=NOW + timedelta(minutes=1)),),
    )
    with _command_client(root, admission, now=NOW + timedelta(minutes=7)) as client:
        published = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert published.status_code == 200
        assert published.json()["status"] == "published"
        assert "已加入盯盘" in published.json()["message"]
        conflict = client.post(
            COMMAND_PATH, json=body | {"source": "screen_result"}, headers=WRITE_HEADERS
        )
        assert conflict.status_code == 409
        assert conflict.json()["status"] == "conflict"
    assert len(admission.submitted) == 1


def test_published_removal_uses_following_generation_and_exact_tombstone(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root, rows=(_row("alice", "600001.SH", 1),))
    admission = _RecordingAdmission()
    with _command_client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation) | {"action": "remove", "expected_version": 1}
        body.pop("source")
        body.pop("price_levels")
        saved = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert saved.status_code == 200
        assert saved.json()["status"] == "saved_syncing"
    _publish(root, sequence=6, rows=(_row("alice", "600001.SH", 2, deleted=True),))
    with _command_client(root, admission, now=NOW + timedelta(minutes=7)) as client:
        published = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert published.status_code == 200
        assert published.json()["status"] == "published"
        assert "已移出盯盘" in published.json()["message"]


def test_published_label_requires_the_checked_generation_to_remain_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "serving"
    _publish(root)
    admission = _RecordingAdmission()
    with _command_client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation)
        assert client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).status_code == 200
    _publish(
        root,
        sequence=6,
        rows=(_row("alice", "600001.SH", 1, updated=NOW + timedelta(minutes=1)),),
    )

    def replace_current_after_read(
        borrowed: BorrowedGeneration, *, owner_id: str, ts_code: str
    ) -> tuple[datetime | None, ManualWatchlistProjectionRow | None]:
        answer = read_manual_watchlist_item(borrowed, owner_id=owner_id, ts_code=ts_code)
        _publish(root, sequence=7, rows=(_row("alice", "600001.SH", 2, deleted=True),))
        return answer

    with _command_client(root, admission, now=NOW + timedelta(minutes=7)) as client:
        monkeypatch.setattr(
            "rquant.web.routes.manual_watchlist.read_manual_watchlist_item",
            replace_current_after_read,
        )
        response = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
    assert response.status_code == 200
    assert response.json()["status"] == "saved_syncing"


def test_lost_response_recovers_original_and_pending_retry_resumes(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    lost = _RecordingAdmission(fail_after_persist=True)
    with _command_client(root, lost) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation)
        recovered = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert recovered.status_code == 200
        assert recovered.json()["status"] == "saved_syncing"
    assert len(lost.submitted) == 1

    pending = _RecordingAdmission(
        receipt=PageControlReceipt(
            command_id="watchlist-first",
            status=PageControlStatus.PENDING,
            enqueued_at=NOW,
        )
    )
    with _command_client(root, pending) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation)
        first = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert first.status_code == 200
        assert first.json()["status"] == "pending"
    _publish(root, sequence=1)
    with _command_client(root, pending, now=NOW + timedelta(minutes=1)) as client:
        retry = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
        assert retry.status_code == 200
        assert retry.json()["status"] == "pending"
    assert len(pending.submitted) == 1
    assert len(pending.resumed) == 1


@pytest.mark.parametrize(
    ("code", "expected_status", "expected_text"),
    (
        ("version_conflict", "conflict", "版本"),
        ("capacity_exceeded", "capacity", "已满"),
        ("future_request", "failed", "失败"),
    ),
)
def test_durable_watchlist_failure_keeps_specific_user_state(
    tmp_path: Path, code: str, expected_status: str, expected_text: str
) -> None:
    root = tmp_path / "serving"
    _publish(root)
    admission = _RecordingAdmission(
        receipt=PageControlReceipt(
            command_id="watchlist-first",
            status=PageControlStatus.FAILED,
            enqueued_at=NOW,
            completed_at=NOW,
            result={"ts_code": "600001.SH", "action": "add", "code": code},
            error="internal technical detail",
        )
    )
    with _command_client(root, admission) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        response = client.post(COMMAND_PATH, json=_command_body(generation), headers=WRITE_HEADERS)
    assert response.json()["status"] == expected_status
    assert expected_text in response.json()["message"]
    assert "internal technical detail" not in response.text


def test_stale_serving_and_unavailable_lookup_never_start_new_command(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    admission = _RecordingAdmission()
    with _command_client(root, admission, now=NOW + timedelta(hours=2)) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        response = client.post(COMMAND_PATH, json=_command_body(generation), headers=WRITE_HEADERS)
    assert response.status_code == 503
    assert admission.submitted == []

    class LookupUnavailable(_RecordingAdmission):
        def lookup(
            self,
            command: AddWatchlistItem | RemoveWatchlistItem,
            *,
            authenticated_owner_id: str,
        ) -> PageControlReceipt | None:
            raise WatchlistAdmissionUnavailableError("private transport detail")

    unavailable = LookupUnavailable()
    with _command_client(root, unavailable) as client:
        response = client.post(COMMAND_PATH, json=_command_body(generation), headers=WRITE_HEADERS)
    assert response.status_code == 503
    assert response.json()["status"] == "uncertain"
    assert "private transport detail" not in response.text
    assert unavailable.submitted == []


def test_generic_admission_rejection_is_uncertain_and_keeps_original_id(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)

    class RejectedAfterUnknownEffect(_RecordingAdmission):
        def submit(
            self,
            command: AddWatchlistItem | RemoveWatchlistItem,
            *,
            authenticated_owner_id: str,
        ) -> PageControlReceipt:
            raise WatchlistAdmissionRejectedError("rejected")

    with _command_client(root, RejectedAfterUnknownEffect()) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation)
        response = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
    assert response.status_code == 503
    assert response.json()["status"] == "uncertain"
    assert response.json()["command_id"] == body["command_id"]


def test_unknown_delivery_and_cross_owner_retry_keep_original_identity(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)

    class LostBeforePersist(_RecordingAdmission):
        def submit(
            self,
            command: AddWatchlistItem | RemoveWatchlistItem,
            *,
            authenticated_owner_id: str,
        ) -> PageControlReceipt:
            raise WatchlistAdmissionUnavailableError("connection dropped")

    unavailable = LostBeforePersist()
    with _command_client(root, unavailable) as client:
        generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
        body = _command_body(generation)
        response = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
    assert response.status_code == 503
    assert response.json()["status"] == "uncertain"
    assert response.json()["command_id"] == body["command_id"]

    admission = _RecordingAdmission()
    with _command_client(root, admission) as client:
        assert client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS).status_code == 200
        cross_owner = client.post(
            COMMAND_PATH,
            json=body,
            headers={**WRITE_HEADERS, "x-rquant-user": "bob"},
        )
    assert cross_owner.status_code == 409
    assert cross_owner.json()["status"] == "conflict"
    assert len(admission.submitted) == 1


def test_real_private_socket_checks_permissions_and_uid_before_web_write(tmp_path: Path) -> None:
    root = tmp_path / "serving"
    _publish(root)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_manual_watchlist(NOW)
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path / "data",
            log_dir=tmp_path / "logs",
            clock=lambda: NOW,
            consumer_id="web-watchlist-test",
        ),
    )
    with TemporaryDirectory(prefix="rqw-", dir=SHORT_TMP) as directory:
        socket_path = Path(directory) / "watchlist.sock"
        server = build_watchlist_admission_server(
            WatchlistAdmission(service), socket_path=socket_path
        )
        assert server is not None
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        settings = WebSettings(
            serving_root=root,
            ingress_socket_path=tmp_path / "web-private" / "web.sock",
            watchlist_admission_socket_path=socket_path,
        )
        try:
            with TestClient(create_app(settings, clock=lambda: NOW, background=False)) as client:
                generation = client.get(PATH, headers=HEADERS).json()["serving"]["generation_id"]
                body = _command_body(generation)
                os.chmod(socket_path, 0o666)
                wrong_mode = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
                assert wrong_mode.status_code == 503
                assert wrong_mode.json()["status"] == "uncertain"
                assert outbox.receipt("watchlist-first") is None
                os.chmod(socket_path, 0o600)

            wrong_uid = WatchlistAdmissionClient(socket_path, expected_service_uid=os.geteuid() + 1)
            with TestClient(
                create_app(
                    settings,
                    clock=lambda: NOW,
                    background=False,
                    watchlist_admission_client=wrong_uid,
                )
            ) as client:
                rejected = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
                assert rejected.status_code == 503
                assert rejected.json()["status"] == "uncertain"
                assert outbox.receipt("watchlist-first") is None

            with TestClient(create_app(settings, clock=lambda: NOW, background=False)) as client:
                accepted = client.post(COMMAND_PATH, json=body, headers=WRITE_HEADERS)
                assert accepted.status_code == 200
                assert accepted.json()["status"] == "saved_syncing"
                assert outbox.receipt("watchlist-first").status is PageControlStatus.SUCCEEDED
        finally:
            server.shutdown()
            server.server_close()
            worker.join(timeout=2)
            assert not worker.is_alive()
