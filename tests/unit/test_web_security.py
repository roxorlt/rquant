"""Loopback-only bind, the nginx user header, and the write-endpoint cross-site guard."""

from __future__ import annotations

from typing import Annotated

import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from rquant.web.security import current_user, require_csrf
from rquant.web.settings import WebSettings, parse_bind


@pytest.mark.parametrize("value", ("127.0.0.1:8768", "localhost:8768", "[::1]:8768"))
def test_loopback_binds_are_accepted(value: str) -> None:
    host, port = parse_bind(value)
    assert port == 8768
    assert host in {"127.0.0.1", "localhost", "::1"}


@pytest.mark.parametrize(
    "value", ("0.0.0.0:8768", "10.0.0.5:8768", "example.com:80", "8768", ":80")
)
def test_non_loopback_or_malformed_binds_are_refused(value: str) -> None:
    with pytest.raises(ValueError):
        parse_bind(value)


def test_settings_come_from_the_environment_only() -> None:
    settings = WebSettings.from_env(
        {
            "RQUANT_SERVING_ROOT": "/srv/serving",
            "RQUANT_WEB_BIND": "127.0.0.1:9000",
            "RQUANT_WEB_STALE_AFTER_SECONDS": "30",
        }
    )
    assert str(settings.serving_root) == "/srv/serving"
    assert (settings.bind_host, settings.bind_port) == ("127.0.0.1", 9000)
    assert settings.stale_after.total_seconds() == 30
    assert str(WebSettings.from_env({}).serving_root) == "data/runtime/serving"
    with pytest.raises(ValueError):
        WebSettings.from_env({"RQUANT_WEB_BIND": "0.0.0.0:8768"})


def _app() -> FastAPI:
    app = FastAPI()

    @app.post("/write", dependencies=[Depends(require_csrf)])
    def write(user: Annotated[str | None, Depends(current_user)]) -> dict[str, str | None]:
        return {"user": user}

    return app


_GOOD = {
    "Content-Type": "application/json",
    "X-Rquant-Csrf": "1",
    "Sec-Fetch-Site": "same-origin",
    # nginx forwards `Host $host:$server_port`, so the API sees the browser's port.
    "Host": "82.156.0.68:8081",
    "Origin": "http://82.156.0.68:8081",
    "X-Rquant-User": "liutong",
}


def test_a_same_site_json_write_with_the_csrf_header_passes() -> None:
    with TestClient(_app()) as client:
        response = client.post("/write", headers=_GOOD, content="{}")
    assert response.status_code == 200
    assert response.json() == {"user": "liutong"}


@pytest.mark.parametrize(
    ("change", "status"),
    (
        ({"Content-Type": "text/plain"}, 415),
        ({"X-Rquant-Csrf": "0"}, 403),
        ({"Sec-Fetch-Site": "cross-site"}, 403),
        ({"Origin": "http://evil.example"}, 403),
        # Same host, another port: another origin (e.g. a Streamlit page on :8501).
        ({"Origin": "http://82.156.0.68:8501"}, 403),
        ({"Origin": "null"}, 403),
        # nginx without the port (`Host $host`) must not pass as :8081.
        ({"Host": "82.156.0.68"}, 403),
    ),
)
def test_writes_without_the_guard_conditions_are_refused(
    change: dict[str, str], status: int
) -> None:
    headers = {**_GOOD, **change}
    with TestClient(_app()) as client:
        response = client.post("/write", headers=headers, content="{}")
    assert response.status_code == status


def test_default_ports_match_an_origin_without_an_explicit_port() -> None:
    headers = {**_GOOD, "Host": "rquant.example", "Origin": "http://rquant.example"}
    with TestClient(_app()) as client:
        assert client.post("/write", headers=headers, content="{}").status_code == 200
        explicit = {**headers, "Host": "rquant.example:80"}
        assert client.post("/write", headers=explicit, content="{}").status_code == 200
