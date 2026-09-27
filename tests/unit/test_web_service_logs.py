"""The Web service-log boundary stays closed unless every authority is present."""

from __future__ import annotations

import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from rquant.unit_log_reader import JournalEntry, JournalPage
from rquant.unit_log_service import UnitLogServiceError
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.unit.test_ops_status import _manifest, _signed

NOW = datetime(2026, 9, 28, 4, 0, tzinfo=UTC)
SINCE = NOW - timedelta(days=1)
UNIT = "rquant-daily.service"
URL = f"/api/v1/tasks/services/{UNIT}/logs"
ADMIN = {"X-Rquant-User": "liutong"}


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.error: Exception | None = None

    def read(self, **kwargs: object) -> JournalPage:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return JournalPage(
            service_label="每日任务",
            entries=(JournalEntry(at=NOW, level="信息", text="任务已完成"),),
            next_cursor=None,
        )


def _configured(tmp_path: Path, *, accepted: frozenset[str] = frozenset({UNIT})) -> WebSettings:
    signed, public = _signed(
        _manifest().model_copy(update={"host_name": socket.gethostname()}), tmp_path
    )
    manifest_path = tmp_path / "manifest.json"
    public_path = tmp_path / "public.pem"
    manifest_path.write_bytes(signed.canonical_bytes())
    public_path.write_bytes(public)
    return WebSettings(
        serving_root=tmp_path / "serving",
        ingress_socket_path=tmp_path / "ingress" / "web.sock",
        log_admin_users=frozenset({"liutong"}),
        unit_log_socket_path=tmp_path / "ops" / "unit-logs.sock",
        unit_log_service_uid=os.geteuid() + 1,
        unit_log_web_group_gid=os.getegid(),
        unit_log_manifest_path=manifest_path,
        unit_log_public_key_path=public_path,
        unit_log_expected_host=socket.gethostname(),
        unit_log_verified_units=accepted,
    )


def test_admin_reads_only_a_typed_bounded_service_page(tmp_path: Path) -> None:
    fake = FakeClient()
    app = create_app(
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
    )
    with TestClient(app) as client:
        response = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})

    assert response.status_code == 200, response.text
    assert response.json() == {
        "service_label": "每日任务",
        "scope": "本机本次开机以来的服务日志（含手动运行）",
        "entries": [
            {
                "at": NOW.isoformat().replace("+00:00", "Z"),
                "level": "信息",
                "text": "任务已完成",
            }
        ],
        "next_cursor": None,
    }
    assert fake.calls == [
        {"unit": UNIT, "since": SINCE, "level": None, "page_size": 100, "cursor": None}
    ]


def test_default_and_unaccepted_service_logs_are_unavailable(tmp_path: Path) -> None:
    fake = FakeClient()
    disabled = create_app(WebSettings(serving_root=tmp_path), clock=lambda: NOW, background=False)
    with TestClient(disabled) as client:
        response = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
    assert response.status_code == 503

    app = create_app(
        _configured(tmp_path, accepted=frozenset()),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
    )
    with TestClient(app) as client:
        response = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
    assert response.status_code == 503
    assert fake.calls == []


def test_spoofed_header_needs_private_ingress_and_exact_admin(tmp_path: Path) -> None:
    fake = FakeClient()
    disabled = create_app(WebSettings(serving_root=tmp_path), clock=lambda: NOW, background=False)
    with TestClient(disabled) as client:
        forged = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
    assert forged.status_code == 503

    app = create_app(
        _configured(tmp_path), clock=lambda: NOW, background=False, unit_log_client=fake
    )
    with TestClient(app) as client:
        missing = client.get(URL, params={"since": SINCE.isoformat()})
        other = client.get(
            URL,
            headers={"X-Rquant-User": "other"},
            params={"since": SINCE.isoformat()},
        )
        invalid = client.get(
            URL,
            headers={"X-Rquant-User": "liutong, other"},
            params={"since": SINCE.isoformat()},
        )
    assert (missing.status_code, other.status_code, invalid.status_code) == (401, 403, 401)
    assert fake.calls == []


def test_only_exact_accepted_and_signed_service_can_reach_client(tmp_path: Path) -> None:
    fake = FakeClient()
    settings = _configured(tmp_path)
    app = create_app(settings, clock=lambda: NOW, background=False, unit_log_client=fake)
    with TestClient(app) as client:
        for unit in ("rquant-monitor.service", "ssh.service", "rquant-daily.service;id"):
            denied = client.get(
                f"/api/v1/tasks/services/{unit}/logs",
                headers=ADMIN,
                params={"since": SINCE.isoformat()},
            )
            assert denied.status_code == 403
        settings.unit_log_manifest_path.write_bytes(b"invalid manifest")
        damaged = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
    assert damaged.status_code == 503
    assert "invalid manifest" not in damaged.text
    assert fake.calls == []


@pytest.mark.parametrize(
    "params",
    (
        {},
        {"since": "2026-09-27T04:00:00"},
        {"since": "Bearer secret"},
        {"since": (NOW - timedelta(days=7, seconds=1)).isoformat()},
        {"since": (NOW + timedelta(seconds=1)).isoformat()},
        {"since": SINCE.isoformat(), "level": "verbose"},
        {"since": SINCE.isoformat(), "page_size": "0"},
        {"since": SINCE.isoformat(), "page_size": "499"},
        {"since": SINCE.isoformat(), "cursor": "x" * 4097},
        {"since": SINCE.isoformat(), "_SYSTEMD_UNIT": "ssh.service"},
        [("since", SINCE.isoformat()), ("since", NOW.isoformat())],
    ),
)
def test_invalid_or_extra_log_filters_do_not_reach_client(tmp_path: Path, params: object) -> None:
    fake = FakeClient()
    app = create_app(
        _configured(tmp_path), clock=lambda: NOW, background=False, unit_log_client=fake
    )
    with TestClient(app) as client:
        response = client.get(URL, headers=ADMIN, params=params)
    assert response.status_code == 422
    assert "Bearer secret" not in response.text
    assert "ssh.service" not in response.text
    assert fake.calls == []


def test_valid_filters_keep_exact_timezone_and_page_binding(tmp_path: Path) -> None:
    fake = FakeClient()
    app = create_app(
        _configured(tmp_path), clock=lambda: NOW, background=False, unit_log_client=fake
    )
    with TestClient(app) as client:
        response = client.get(
            URL,
            headers=ADMIN,
            params={
                "since": "2026-09-27T12:00:00+08:00",
                "level": "warning",
                "page_size": "498",
                "cursor": "signed.cursor",
            },
        )
    assert response.status_code == 200
    assert fake.calls == [
        {
            "unit": UNIT,
            "since": SINCE,
            "level": "warning",
            "page_size": 498,
            "cursor": "signed.cursor",
        }
    ]


@pytest.mark.parametrize(
    ("error", "status_code"),
    (
        (UnitLogServiceError("cursor_changed"), 409),
        (UnitLogServiceError("busy"), 429),
        (UnitLogServiceError("unavailable"), 503),
        (UnitLogServiceError("forbidden"), 403),
        (RuntimeError("Bearer secret token in journal MESSAGE"), 503),
    ),
)
def test_transport_failures_are_closed_and_do_not_expose_source_data(
    tmp_path: Path, error: Exception, status_code: int
) -> None:
    fake = FakeClient()
    fake.error = error
    app = create_app(
        _configured(tmp_path), clock=lambda: NOW, background=False, unit_log_client=fake
    )
    with TestClient(app) as client:
        response = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
        overview = client.get("/api/v1/tasks/overview", headers=ADMIN)
        health = client.get("/api/v1/health", headers=ADMIN)
    assert response.status_code == status_code
    assert overview.status_code == 200
    assert health.status_code == 200
    assert "Bearer" not in response.text
    assert "journal" not in response.text
    if status_code == 429:
        assert response.headers["Retry-After"] == "1"


def test_service_log_config_is_atomic_and_separates_web_identity(tmp_path: Path) -> None:
    settings = _configured(tmp_path)
    assert WebSettings.from_env({}).unit_log_verified_units == frozenset()
    values = settings.model_dump()
    for field in (
        "unit_log_socket_path",
        "unit_log_service_uid",
        "unit_log_web_group_gid",
        "unit_log_manifest_path",
        "unit_log_public_key_path",
        "unit_log_expected_host",
    ):
        with pytest.raises(ValidationError):
            WebSettings.model_validate(values | {field: None})
    with pytest.raises(ValidationError, match="differ from Web"):
        WebSettings.model_validate(values | {"unit_log_service_uid": os.geteuid()})
    with pytest.raises(ValidationError):
        WebSettings.model_validate(values | {"unit_log_service_uid": True})
    with pytest.raises(ValidationError, match="private Web ingress"):
        WebSettings.model_validate(values | {"ingress_socket_path": None})
    with pytest.raises(ValidationError):
        WebSettings.model_validate(values | {"unit_log_verified_units": {"rquant-monitor.service"}})
    with pytest.raises(ValidationError):
        WebSettings.model_validate(values | {"unit_log_socket_path": Path("relative.sock")})


def test_service_log_environment_is_explicit_and_defaults_to_no_verified_units(
    tmp_path: Path,
) -> None:
    configured = _configured(tmp_path)
    source = {
        "RQUANT_WEB_INGRESS_SOCKET": str(configured.ingress_socket_path),
        "RQUANT_WEB_LOG_ADMIN_USERS": "liutong",
        "RQUANT_WEB_UNIT_LOG_SOCKET": str(configured.unit_log_socket_path),
        "RQUANT_WEB_UNIT_LOG_SERVICE_UID": str(configured.unit_log_service_uid),
        "RQUANT_WEB_UNIT_LOG_WEB_GROUP_GID": str(configured.unit_log_web_group_gid),
        "RQUANT_WEB_UNIT_LOG_MANIFEST": str(configured.unit_log_manifest_path),
        "RQUANT_WEB_UNIT_LOG_PUBLIC_KEY": str(configured.unit_log_public_key_path),
        "RQUANT_WEB_UNIT_LOG_EXPECTED_HOST": str(configured.unit_log_expected_host),
    }
    disabled = WebSettings.from_env(source)
    assert disabled.unit_log_verified_units == frozenset()
    accepted = WebSettings.from_env(
        source | {"RQUANT_WEB_UNIT_LOG_VERIFIED_UNITS": "rquant-daily.service"}
    )
    assert accepted.unit_log_verified_units == frozenset({UNIT})
    with pytest.raises(ValidationError):
        WebSettings.from_env({"RQUANT_WEB_UNIT_LOG_SOCKET": str(configured.unit_log_socket_path)})
    with pytest.raises(ValueError):
        WebSettings.from_env(
            source
            | {"RQUANT_WEB_UNIT_LOG_VERIFIED_UNITS": "rquant-daily.service,rquant-daily.service"}
        )


def test_untrusted_or_oversized_public_key_file_never_reaches_transport(tmp_path: Path) -> None:
    fake = FakeClient()
    settings = _configured(tmp_path)
    assert settings.unit_log_public_key_path is not None
    public_path = settings.unit_log_public_key_path
    public_path.write_bytes(b"x" * 8193)
    app = create_app(settings, clock=lambda: NOW, background=False, unit_log_client=fake)
    with TestClient(app) as client:
        oversized = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
        public_path.unlink()
        public_path.symlink_to(settings.unit_log_manifest_path)
        symlink = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
    assert (oversized.status_code, symlink.status_code) == (503, 503)
    assert fake.calls == []


def test_openapi_describes_the_bounded_log_filters(tmp_path: Path) -> None:
    app = create_app(WebSettings(serving_root=tmp_path), background=False)
    operation = app.openapi()["paths"]["/api/v1/tasks/services/{unit}/logs"]["get"]
    parameters = {item["name"]: item for item in operation["parameters"]}
    assert set(parameters) == {"unit", "since", "level", "page_size", "cursor"}
    assert parameters["since"]["required"] is True
    assert parameters["since"]["schema"]["format"] == "date-time"
    assert parameters["page_size"]["schema"]["maximum"] == 498


def test_inflight_log_read_rejects_second_request_without_blocking_health(tmp_path: Path) -> None:
    class BlockingClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()

        def read(self, **kwargs: object) -> JournalPage:
            self.entered.set()
            if not self.release.wait(3):
                raise RuntimeError("test read was not released")
            return super().read(**kwargs)

    fake = BlockingClient()
    app = create_app(
        _configured(tmp_path), clock=lambda: NOW, background=False, unit_log_client=fake
    )
    with TestClient(app) as client, ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(client.get, URL, headers=ADMIN, params={"since": SINCE.isoformat()})
        assert fake.entered.wait(2)
        try:
            second = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
            health = client.get("/api/v1/health", headers=ADMIN)
        finally:
            fake.release.set()
        assert first.result(timeout=3).status_code == 200
    assert second.status_code == 429
    assert health.status_code == 200
    assert len(fake.calls) == 1
