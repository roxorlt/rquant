"""Explicit private configuration only; no socket installation claim."""

from __future__ import annotations

import os
from pathlib import Path
from typing import cast

import pytest

from rquant.price_alert_admission import PriceAlertAdmissionClient
from rquant.web.app import create_app
from rquant.web.settings import WebSettings


def private_values() -> dict[str, object]:
    return {
        "serving_root": Path("/synthetic/serving"),
        "ingress_socket_path": Path("/synthetic/web/web.sock"),
        "proxy_proof_file": Path("/synthetic/proof"),
        "price_alert_admission_socket_path": Path("/synthetic/price/price.sock"),
        "price_alert_admission_service_uid": os.geteuid() + 1,
        "price_alert_admission_shared_gid": os.getegid(),
    }


def test_price_admission_is_disabled_by_default_even_with_injected_client(tmp_path: Path) -> None:
    settings = WebSettings(serving_root=tmp_path)
    assert settings.price_alert_admission_socket_path is None
    app = create_app(
        settings,
        background=False,
        price_alert_admission_client=cast(PriceAlertAdmissionClient, object()),
    )
    assert app.state.web.price_alert_admission is None


def test_price_admission_environment_has_one_explicit_private_endpoint() -> None:
    settings = WebSettings.from_env(
        {
            "RQUANT_SERVING_ROOT": "/synthetic/serving",
            "RQUANT_WEB_INGRESS_SOCKET": "/synthetic/web/web.sock",
            "RQUANT_WEB_PROXY_PROOF_FILE": "/synthetic/proof",
            "RQUANT_WEB_PRICE_ALERT_ADMISSION_SOCKET": "/synthetic/price/price.sock",
            "RQUANT_WEB_PRICE_ALERT_ADMISSION_SERVICE_UID": str(os.geteuid() + 1),
            "RQUANT_WEB_PRICE_ALERT_ADMISSION_SHARED_GID": str(os.getegid()),
        }
    )
    assert settings.price_alert_admission_socket_path == Path("/synthetic/price/price.sock")
    assert settings.price_alert_admission_service_uid == os.geteuid() + 1
    assert settings.price_alert_admission_shared_gid == os.getegid()


@pytest.mark.parametrize(
    "missing",
    [
        "ingress_socket_path",
        "proxy_proof_file",
        "price_alert_admission_socket_path",
        "price_alert_admission_service_uid",
        "price_alert_admission_shared_gid",
    ],
)
def test_incomplete_price_admission_cannot_start(missing: str) -> None:
    values = private_values()
    del values[missing]
    with pytest.raises(ValueError, match="price rules require"):
        WebSettings.model_validate(values)


@pytest.mark.parametrize(
    "field,value",
    [
        ("price_alert_admission_socket_path", "relative.sock"),
        ("price_alert_admission_socket_path", "/synthetic/price/../price.sock"),
        ("price_alert_admission_socket_path", "/" + "x" * 100),
        ("price_alert_admission_service_uid", -1),
        ("price_alert_admission_shared_gid", -1),
        ("price_alert_admission_service_uid", True),
        ("price_alert_admission_shared_gid", True),
        ("price_alert_admission_service_uid", os.geteuid()),
    ],
    ids=(None, None, None, None, None, None, None, "same-service-uid"),
)
def test_price_admission_rejects_unsafe_path_or_identity(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        WebSettings.model_validate({**private_values(), field: value})


@pytest.mark.parametrize(
    "other",
    [
        "ingress_socket_path",
        "ack_admission_socket_path",
        "watchlist_admission_socket_path",
        "factor_admission_socket_path",
        "factor_run_admission_socket_path",
        "factor_tracking_admission_socket_path",
        "research_query_socket_path",
        "research_query_save_socket_path",
        "unit_log_socket_path",
    ],
)
def test_price_endpoint_cannot_share_another_private_directory(other: str) -> None:
    with pytest.raises(ValueError, match="separate private directory"):
        WebSettings.model_validate({**private_values(), other: "/synthetic/price/other.sock"})
