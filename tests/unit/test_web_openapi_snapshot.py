"""The committed OpenAPI snapshot is exactly what the API generates.

The front end's TypeScript types (``web/src/api/schema.d.ts``) are generated from
``web/src/api/openapi.json``; this test is the Python half of the chain, ``web.yml`` checks
the TypeScript half. Regenerate with ``uv run rquant web-openapi > web/src/api/openapi.json``.
"""

from __future__ import annotations

from pathlib import Path

from rquant.web.app import create_app, openapi_document
from rquant.web.settings import WebSettings

SNAPSHOT = Path(__file__).resolve().parents[2] / "web" / "src" / "api" / "openapi.json"


def test_openapi_snapshot_matches_the_app_byte_for_byte() -> None:
    app = create_app(WebSettings(serving_root=Path("data/runtime/serving")), background=False)
    assert SNAPSHOT.read_text(encoding="utf-8") == openapi_document(app)


def test_every_api_path_is_versioned_and_read_only() -> None:
    app = create_app(WebSettings(serving_root=Path("data/runtime/serving")), background=False)
    paths = app.openapi()["paths"]
    assert paths
    for path, operations in paths.items():
        assert path.startswith("/api/v1/")
        assert set(operations) == {"get"}
