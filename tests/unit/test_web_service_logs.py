"""The Web service-log boundary stays closed unless every authority is present."""

from __future__ import annotations

import json
import os
import socket
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import uvicorn
from pydantic import ValidationError

from rquant.unit_log_reader import JournalEntry, JournalPage
from rquant.unit_log_service import UnitLogServiceError
from rquant.web import app as app_module
from rquant.web import cli as web_cli
from rquant.web import ingress as ingress_module
from rquant.web.service_log_access_audit import (
    AUDIT_FILE_NAME,
    JsonlServiceLogAccessAudit,
    ServiceLogAccessRecord,
)
from rquant.web.settings import WebSettings
from tests.support.web_proxy_identity import ProofTestClient as TestClient
from tests.support.web_proxy_identity import create_proof_test_app as create_app
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
        self.available = True

    def read(self, **kwargs: object) -> JournalPage:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return JournalPage(
            service_label="每日任务",
            entries=(JournalEntry(at=NOW, level="信息", text="任务已完成"),),
            next_cursor=None,
        )

    def preflight(self) -> bool:
        return self.available


class FakeAudit:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.records: list[ServiceLogAccessRecord] = []
        self.error = error

    def record(self, event: ServiceLogAccessRecord) -> None:
        self.records.append(event)
        if self.error is not None:
            raise self.error

    def preflight(self) -> bool:
        return True


def _configured(tmp_path: Path, *, accepted: frozenset[str] = frozenset({UNIT})) -> WebSettings:
    signed, public = _signed(
        _manifest().model_copy(update={"host_name": socket.gethostname()}), tmp_path
    )
    manifest_path = tmp_path / "manifest.json"
    public_path = tmp_path / "public.pem"
    manifest_path.write_bytes(signed.canonical_bytes())
    public_path.write_bytes(public)
    ingress_dir = tmp_path / "ingress"
    ingress_dir.mkdir(mode=0o710, exist_ok=True)
    ingress_dir.chmod(0o710)
    ingress_path = ingress_dir / "web.sock"
    if ingress_path.exists():
        ingress_path.unlink()
    original_directory = Path.cwd()
    try:
        os.chdir(tmp_path)
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
            listener.bind("ingress/web.sock")
    finally:
        os.chdir(original_directory)
    ingress_path.chmod(0o660)
    return WebSettings(
        serving_root=tmp_path / "serving",
        ingress_socket_path=ingress_path,
        log_admin_users=frozenset({"liutong"}),
        unit_log_socket_path=tmp_path / "ops" / "unit-logs.sock",
        unit_log_service_uid=os.geteuid() + 1,
        unit_log_web_group_gid=os.getegid(),
        unit_log_manifest_path=manifest_path,
        unit_log_public_key_path=public_path,
        unit_log_expected_host=socket.gethostname(),
        unit_log_verified_units=accepted,
    )


def test_capability_only_lists_currently_admitted_exact_units_without_side_effects(
    tmp_path: Path,
) -> None:
    fake = FakeClient()
    audit = FakeAudit()
    settings = _configured(tmp_path)
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=audit,
    )
    capability_url = "/api/v1/tasks/services/log-capabilities"
    with TestClient(app) as client:
        available = client.get(capability_url, headers=ADMIN)
        anonymous = client.get(capability_url)
        other = client.get(capability_url, headers={"X-Rquant-User": "other"})
        assert available.status_code == 200
        assert available.json() == {"units": [UNIT]}
        assert anonymous.json() == {"units": []}
        assert other.json() == {"units": []}
        assert app.state.web.unit_log_gate.acquire(blocking=False)
        app.state.web.unit_log_gate.release()
        assert fake.calls == []
        assert audit.records == []

        assert settings.ingress_socket_path is not None
        settings.ingress_socket_path.chmod(0o666)
        assert client.get(capability_url, headers=ADMIN).json() == {"units": []}
        assert (
            client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()}).status_code == 503
        )
        settings.ingress_socket_path.chmod(0o660)
        fake.available = False
        assert client.get(capability_url, headers=ADMIN).json() == {"units": []}
        assert (
            client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()}).status_code == 503
        )
        assert fake.calls == []
        fake.available = True
        app.state.web = replace(
            app.state.web,
            settings=settings.model_copy(update={"unit_log_verified_units": frozenset()}),
        )
        assert client.get(capability_url, headers=ADMIN).json() == {"units": []}


def test_capability_rechecks_current_audit_directory_without_side_effects(
    tmp_path: Path,
) -> None:
    fake = FakeClient()
    settings = _configured(tmp_path)
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    audit = JsonlServiceLogAccessAudit(directory)
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=audit,
    )
    capability_url = "/api/v1/tasks/services/log-capabilities"
    audit_path = directory / AUDIT_FILE_NAME
    with TestClient(app) as client:
        assert client.get(capability_url, headers=ADMIN).json() == {"units": [UNIT]}
        assert not audit_path.exists()
        directory.chmod(0o755)
        assert client.get(capability_url, headers=ADMIN).json() == {"units": []}
        assert (
            client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()}).status_code == 503
        )
        assert not audit_path.exists()
        assert fake.calls == []
        assert app.state.web.unit_log_gate.acquire(blocking=False)
        app.state.web.unit_log_gate.release()


def test_capability_fails_closed_on_missing_client_audit_and_manifest(tmp_path: Path) -> None:
    capability_url = "/api/v1/tasks/services/log-capabilities"
    settings = _configured(tmp_path)
    fake = FakeClient()
    for missing in ("audit", "client"):
        app = create_app(
            settings,
            clock=lambda: NOW,
            background=False,
            unit_log_client=fake,
            unit_log_access_audit=FakeAudit(),
        )
        if missing == "audit":
            app.state.web = replace(app.state.web, unit_log_access_audit=None)
        else:
            app.state.web = replace(app.state.web, unit_log_client=None)
        with TestClient(app) as client:
            assert client.get(capability_url, headers=ADMIN).json() == {"units": []}
    assert settings.unit_log_manifest_path is not None
    settings.unit_log_manifest_path.write_bytes(b"invalid manifest")
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
    )
    with TestClient(app) as client:
        assert client.get(capability_url, headers=ADMIN).json() == {"units": []}

    disabled = create_app(WebSettings(serving_root=tmp_path), background=False)
    with TestClient(disabled) as client:
        assert client.get(capability_url, headers=ADMIN).json() == {"units": []}


def test_admin_reads_only_a_typed_bounded_service_page(tmp_path: Path) -> None:
    fake = FakeClient()
    audit = FakeAudit()
    app = create_app(
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=audit,
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
    assert [record.model_dump(mode="json") for record in audit.records] == [
        {
            "operator": "liutong",
            "unit": UNIT,
            "result_class": "admitted",
            "at": NOW.isoformat().replace("+00:00", "Z"),
        }
    ]


def test_verified_unit_stays_closed_without_access_audit(tmp_path: Path) -> None:
    fake = FakeClient()
    app = create_app(
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
    )
    with TestClient(app) as client:
        response = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
    assert response.status_code == 503
    assert fake.calls == []


def test_audit_write_failure_refuses_journal_read_and_hides_error(tmp_path: Path) -> None:
    fake = FakeClient()
    audit = FakeAudit(error=RuntimeError("Bearer secret in audit backend"))
    app = create_app(
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=audit,
    )
    with TestClient(app) as client:
        response = client.get(
            URL,
            headers=ADMIN,
            params={"since": SINCE.isoformat(), "cursor": "secret.cursor"},
        )
    assert response.status_code == 503
    assert "Bearer" not in response.text
    assert "secret.cursor" not in response.text
    assert fake.calls == []
    assert len(audit.records) == 1
    assert set(audit.records[0].model_dump()) == {"operator", "unit", "result_class", "at"}
    serialized = audit.records[0].model_dump_json()
    assert "secret.cursor" not in serialized
    assert "Bearer" not in serialized


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
        unit_log_access_audit=FakeAudit(),
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
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
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
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
    )
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
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
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
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
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
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
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


def test_audit_dir_setting_requires_full_private_service_log_configuration(tmp_path: Path) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    values = _configured(tmp_path).model_dump()
    configured = WebSettings.model_validate(values | {"unit_log_audit_dir": directory})
    assert configured.unit_log_audit_dir == directory
    with pytest.raises(ValidationError):
        WebSettings.model_validate(values | {"unit_log_audit_dir": Path("relative")})
    with pytest.raises(ValidationError):
        WebSettings(serving_root=tmp_path, unit_log_audit_dir=directory)
    with pytest.raises(ValidationError):
        WebSettings.model_validate(
            values | {"ingress_socket_path": None, "unit_log_audit_dir": directory}
        )
    source = {
        "RQUANT_WEB_INGRESS_SOCKET": str(values["ingress_socket_path"]),
        "RQUANT_WEB_LOG_ADMIN_USERS": "liutong",
        "RQUANT_WEB_UNIT_LOG_SOCKET": str(values["unit_log_socket_path"]),
        "RQUANT_WEB_UNIT_LOG_SERVICE_UID": str(values["unit_log_service_uid"]),
        "RQUANT_WEB_UNIT_LOG_WEB_GROUP_GID": str(values["unit_log_web_group_gid"]),
        "RQUANT_WEB_UNIT_LOG_MANIFEST": str(values["unit_log_manifest_path"]),
        "RQUANT_WEB_UNIT_LOG_PUBLIC_KEY": str(values["unit_log_public_key_path"]),
        "RQUANT_WEB_UNIT_LOG_EXPECTED_HOST": str(values["unit_log_expected_host"]),
        "RQUANT_WEB_UNIT_LOG_AUDIT_DIR": str(directory),
    }
    assert WebSettings.from_env(source).unit_log_audit_dir == directory


def test_web_serve_alone_injects_real_audit_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    settings = WebSettings.model_validate(
        _configured(tmp_path).model_dump() | {"unit_log_audit_dir": directory}
    )
    observed: list[object] = []
    from_env_calls: list[object] = []

    def settings_from_env(_cls: type[WebSettings], *, bind: str | None = None) -> WebSettings:
        from_env_calls.append(bind)
        return settings

    def capture_app(_settings: WebSettings, **kwargs: object) -> object:
        observed.append(kwargs.get("unit_log_access_audit"))
        return object()

    @contextmanager
    def fake_ingress(_path: Path, *, nginx_group_gid: int) -> Iterator[SimpleNamespace]:
        assert nginx_group_gid == os.getegid()
        yield SimpleNamespace(fileno=lambda: 123)

    monkeypatch.setattr(WebSettings, "from_env", classmethod(settings_from_env))
    monkeypatch.setattr(app_module, "create_app", capture_app)
    monkeypatch.setattr(app_module, "openapi_document", lambda _app: "{}")
    monkeypatch.setattr(ingress_module, "private_web_ingress_socket", fake_ingress)
    monkeypatch.setattr(web_cli.grp, "getgrnam", lambda _name: SimpleNamespace(gr_gid=os.getegid()))
    monkeypatch.setattr(uvicorn, "run", lambda _app, **_options: None)

    assert web_cli.main(["web-serve"]) == 0
    assert type(observed.pop()) is JsonlServiceLogAccessAudit
    assert from_env_calls == [None]
    assert not (directory / AUDIT_FILE_NAME).exists()

    assert web_cli.main(["web-openapi"]) == 0
    assert observed.pop() is None
    assert from_env_calls == [None]
    assert capsys.readouterr().out == "{}"

    disabled = WebSettings(serving_root=tmp_path)
    monkeypatch.setattr(WebSettings, "from_env", classmethod(lambda _cls, *, bind=None: disabled))
    assert web_cli.main(["web-serve"]) == 0
    assert observed.pop() is None


def test_real_audit_is_durable_before_client_read(tmp_path: Path) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    path = directory / AUDIT_FILE_NAME

    class CheckingClient(FakeClient):
        def read(self, **kwargs: object) -> JournalPage:
            rows = path.read_bytes().splitlines()
            assert len(rows) == 1
            assert json.loads(rows[0]) == {
                "operator": "liutong",
                "unit": UNIT,
                "result_class": "admitted",
                "at": NOW.isoformat().replace("+00:00", "Z"),
            }
            return super().read(**kwargs)

    fake = CheckingClient()
    settings = WebSettings.model_validate(
        _configured(tmp_path).model_dump() | {"unit_log_audit_dir": directory}
    )
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=JsonlServiceLogAccessAudit(directory),
    )
    with TestClient(app) as client:
        response = client.get(
            URL, headers=ADMIN, params={"since": SINCE.isoformat(), "cursor": "secret.cursor"}
        )
    assert response.status_code == 200
    assert len(fake.calls) == 1
    assert b"secret.cursor" not in path.read_bytes()


def test_real_audit_sync_failure_returns_503_before_client_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = tmp_path / "audit"
    directory.mkdir(mode=0o700)
    fake = FakeClient()
    settings = WebSettings.model_validate(
        _configured(tmp_path).model_dump() | {"unit_log_audit_dir": directory}
    )
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=JsonlServiceLogAccessAudit(directory),
    )

    def fail_sync(_descriptor: int) -> None:
        raise OSError("Bearer secret from storage")

    monkeypatch.setattr(os, "fsync", fail_sync)
    with TestClient(app) as client:
        response = client.get(URL, headers=ADMIN, params={"since": SINCE.isoformat()})
    assert response.status_code == 503
    assert "Bearer" not in response.text
    assert fake.calls == []


def test_untrusted_or_oversized_public_key_file_never_reaches_transport(tmp_path: Path) -> None:
    fake = FakeClient()
    settings = _configured(tmp_path)
    assert settings.unit_log_public_key_path is not None
    public_path = settings.unit_log_public_key_path
    public_path.write_bytes(b"x" * 8193)
    app = create_app(
        settings,
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
    )
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
        _configured(tmp_path),
        clock=lambda: NOW,
        background=False,
        unit_log_client=fake,
        unit_log_access_audit=FakeAudit(),
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
