"""Synthetic, private proxy proof for Web route tests only."""

from __future__ import annotations

import secrets
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rquant.web.app import create_app
from rquant.web.settings import WebSettings

TEST_PROXY_PROOF = secrets.token_hex(32)
PROXY_HEADERS = {"x-rquant-proxy-proof": TEST_PROXY_PROOF}


def with_test_proxy_identity(settings: WebSettings) -> WebSettings:
    parent = settings.serving_root.parent
    path = parent / "synthetic-web-proxy-proof"
    if not path.exists():
        path.write_text(TEST_PROXY_PROOF, encoding="ascii")
        path.chmod(0o400)
    return WebSettings.model_validate(
        {
            **settings.model_dump(),
            "ingress_socket_path": settings.ingress_socket_path or parent / "synthetic-web.sock",
            "proxy_proof_file": path,
        }
    )


def create_private_test_app(settings: WebSettings, **kwargs: Any) -> FastAPI:
    return create_app(with_test_proxy_identity(settings), **kwargs)


def create_proof_test_app(settings: WebSettings, **kwargs: Any) -> FastAPI:
    configured = (
        with_test_proxy_identity(settings) if settings.ingress_socket_path is not None else settings
    )
    return create_app(configured, **kwargs)


class ProofTestClient(TestClient):
    def __init__(self, app: FastAPI, **kwargs: Any) -> None:
        headers = kwargs.pop("headers", None) or {}
        super().__init__(app, headers={**PROXY_HEADERS, **headers}, **kwargs)


class ResearcherTestClient(ProofTestClient):
    def __init__(self, app: FastAPI, **kwargs: Any) -> None:
        headers = kwargs.pop("headers", None) or {}
        super().__init__(app, headers={"x-rquant-user": "researcher", **headers}, **kwargs)
