"""One-sentence screening drafts are typed, source-bound and never run automatically."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from rquant.web.app import create_app
from rquant.web.models.screen import ScreenCatalogData, ScreenSourceInfo
from rquant.web.nl_parser import (
    NlClarificationNeededError,
    NlParserUnavailableError,
    OpenAiScreenPlanParser,
)
from rquant.web.screen_catalog import screen_blocks
from rquant.web.screen_nl_preview import (
    InvalidScreenDraftError,
    ScreenDateMismatchError,
    validate_screen_draft,
)
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

NOW = FIXTURE_BUILT_AT + timedelta(seconds=90)
HEADERS = {
    "x-rquant-user": "researcher",
    "x-rquant-csrf": "1",
    "origin": "http://testserver",
}


class FakeParser:
    def __init__(self, result: dict[str, object] | Exception) -> None:
        self.result = result
        self.calls = 0
        self.hook: Callable[[], None] | None = None

    def parse_new(self, instruction: str, trade_date: str) -> dict[str, object]:
        self.calls += 1
        assert instruction == "排除 ST，流通市值低于 100 亿"
        assert trade_date == "2026-09-24"
        if self.hook is not None:
            self.hook()
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def _raw(*rules: dict[str, object], trade_date: str = "") -> dict[str, object]:
    return {
        "trade_date": trade_date,
        "stages": [{"label": "条件", "rules": list(rules)}],
    }


def _app(root: Path, parser: FakeParser | None):
    build_web_fixture(root, "baseline")
    return create_app(
        WebSettings(serving_root=root, stale_after_seconds=600),
        clock=lambda: NOW,
        background=False,
        nl_parser=parser,
    )


def _body(source: dict[str, object], **updates: object) -> dict[str, object]:
    return {
        "source_kind": "serving",
        "source_identity": source["identity"],
        "trade_date": "2026-09-24",
        "instruction": "排除 ST，流通市值低于 100 亿",
        **updates,
    }


def test_preview_apply_then_serving_rotation_requires_fresh_run(tmp_path: Path) -> None:
    parser = FakeParser(
        _raw(
            {"name": "not_st", "args": {}},
            {"name": "circ_mv_lt", "args": {"threshold_yi": "100"}},
        )
    )
    root = tmp_path / "serving"
    app = _app(root, parser)
    with TestClient(app) as client:
        catalog = client.get("/api/v1/screen/blocks").json()["data"]
        assert catalog["nl_generate_available"] is True
        preview = client.post(
            "/api/v1/screen/nl-preview", json=_body(catalog["source"]), headers=HEADERS
        )
        assert preview.status_code == 200, preview.text
        data = preview.json()
        assert data == {
            "source_kind": "serving",
            "source_identity": catalog["source"]["identity"],
            "trade_date": "2026-09-24",
            "conditions": [
                {"key": "not_st", "args": {}},
                {"key": "circ_mv_lt", "args": {"threshold_yi": 100.0, "offset": 0}},
            ],
        }
        run_body = {
            "source_identity": data["source_identity"],
            "trade_date": data["trade_date"],
            "conditions": data["conditions"],
        }
        first = client.post(
            "/api/v1/screen/run", json=run_body, headers={"x-rquant-csrf": "1"}
        )
        assert first.status_code == 200, first.text
        assert first.json()["data"]["status"] == "ready"
        assert parser.calls == 1

        build_web_fixture(root, "baseline", sequence=1)
        app.state.web.tracker.refresh()
        stale = client.post(
            "/api/v1/screen/run", json=run_body, headers={"x-rquant-csrf": "1"}
        )
        fresh_catalog = client.get("/api/v1/screen/blocks").json()["data"]
        fresh = client.post(
            "/api/v1/screen/run",
            json={**run_body, "source_identity": fresh_catalog["source"]["identity"]},
            headers={"x-rquant-csrf": "1"},
        )
    assert stale.status_code == 409
    assert fresh.status_code == 200
    assert fresh.json()["data"]["source"]["identity"] == fresh_catalog["source"]["identity"]


def test_unconfigured_model_is_a_visible_capability_and_cannot_generate(tmp_path: Path) -> None:
    app = _app(tmp_path / "serving", None)
    with TestClient(app) as client:
        catalog = client.get("/api/v1/screen/blocks").json()["data"]
        response = client.post(
            "/api/v1/screen/nl-preview", json=_body(catalog["source"]), headers=HEADERS
        )
    assert catalog["nl_generate_available"] is False
    assert response.status_code == 503
    assert "手动" in response.json()["detail"]


def test_admission_guards_run_before_model_and_limit_paid_calls(tmp_path: Path) -> None:
    parser = FakeParser(_raw({"name": "not_st", "args": {}}))
    app = _app(tmp_path / "serving", parser)
    with TestClient(app) as client:
        source = client.get("/api/v1/screen/blocks").json()["data"]["source"]
        body = _body(source)
        assert client.post(
            "/api/v1/screen/nl-preview",
            json=body,
            headers={key: value for key, value in HEADERS.items() if key != "x-rquant-user"},
        ).status_code == 401
        assert client.post(
            "/api/v1/screen/nl-preview", json=body, headers={"x-rquant-user": "researcher"}
        ).status_code == 403
        assert client.post(
            "/api/v1/screen/nl-preview", json=body,
            headers={**HEADERS, "origin": "http://other.example"},
        ).status_code == 403
        assert client.post(
            "/api/v1/screen/nl-preview",
            json=_body(source, source_identity="b" * 64),
            headers=HEADERS,
        ).status_code == 409
        assert client.post(
            "/api/v1/screen/nl-preview",
            json=_body(source, trade_date="2026-09-23"),
            headers=HEADERS,
        ).status_code == 409
        assert client.post(
            "/api/v1/screen/nl-preview",
            json=_body(source, instruction="x" * 501),
            headers=HEADERS,
        ).status_code == 422
        assert client.post(
            "/api/v1/screen/nl-preview",
            json={**body, "payload": "SECRET_MARKER" * 500},
            headers=HEADERS,
        ).status_code == 413
        assert parser.calls == 0
        assert app.state.web.nl_gate.acquire(blocking=False)
        try:
            assert client.post(
                "/api/v1/screen/nl-preview", json=body, headers=HEADERS
            ).status_code == 429
        finally:
            app.state.web.nl_gate.release()
        for _ in range(3):
            assert client.post(
                "/api/v1/screen/nl-preview", json=body, headers=HEADERS
            ).status_code == 200
        limited = client.post("/api/v1/screen/nl-preview", json=body, headers=HEADERS)
    assert limited.status_code == 429
    assert limited.headers["retry-after"] == "60"
    assert parser.calls == 3


def test_parser_failure_and_rotation_during_parse_keep_candidate_unpublished(
    tmp_path: Path,
) -> None:
    for index, failure in enumerate(
        (
            NlClarificationNeededError("RAW_SECRET"),
            NlParserUnavailableError("RAW_SECRET"),
            TimeoutError("RAW_SECRET"),
        )
    ):
        parser = FakeParser(failure)
        app = _app(tmp_path / f"failure-{index}", parser)
        with TestClient(app) as client:
            source = client.get("/api/v1/screen/blocks").json()["data"]["source"]
            response = client.post(
                "/api/v1/screen/nl-preview", json=_body(source), headers=HEADERS
            )
        assert response.status_code == (422 if index == 0 else 503)
        assert "RAW_SECRET" not in response.text

    root = tmp_path / "rotation"
    parser = FakeParser(_raw({"name": "not_st", "args": {}}))
    app = _app(root, parser)

    def rotate() -> None:
        build_web_fixture(root, "baseline", sequence=1)
        app.state.web.tracker.refresh()

    parser.hook = rotate
    with TestClient(app) as client:
        source = client.get("/api/v1/screen/blocks").json()["data"]["source"]
        response = client.post(
            "/api/v1/screen/nl-preview", json=_body(source), headers=HEADERS
        )
    assert response.status_code == 409
    assert parser.calls == 1


def _catalog(*, replica: bool = False) -> ScreenCatalogData:
    return ScreenCatalogData(
        source_kind="replica" if replica else "serving",
        blocks=(
            screen_blocks(dynamic_ma=True, dynamic_rsi=True, fundamental_fields={"pe_ttm"})
            if replica else screen_blocks()
        ),
        dates=[NOW.date()],
        available=True,
        ranking_metrics=[],
        source=ScreenSourceInfo(identity="a" * 64, updated_at=NOW),
    )


def test_dynamic_replica_fields_follow_current_catalog_and_normalize_controls() -> None:
    raw = _raw(
        {"name": "cross_above", "args": {"fast": "MA37", "slow": "MA20"}},
        {"name": "gt", "args": {"left": "PE_TTM[0]", "right": "8"}},
        {"name": "rsi_oversold", "args": {"period": "14", "threshold": "30"}},
    )
    conditions = validate_screen_draft(raw, _catalog(replica=True), NOW.date())
    assert [(condition.key, condition.args) for condition in conditions] == [
        ("cross_above", {"fast": 37, "slow": 20, "offset": 0}),
        ("gt", {"left": "PE_TTM[0]", "right": 8.0}),
        ("rsi_oversold", {"period": 14, "threshold": 30.0, "offset": 0}),
    ]
    with pytest.raises(InvalidScreenDraftError):
        validate_screen_draft(raw, _catalog(), NOW.date())


@pytest.mark.parametrize(
    "raw",
    [
        _raw(),
        _raw({"name": "invented_rule", "args": {}}),
        _raw({"name": "circ_mv_lt", "args": {"threshold_yi": "10001"}}),
        _raw({"name": "gt", "args": {"left": "UNPUBLISHED[0]", "right": 8}}),
        _raw({"name": "board_in", "args": {"boards": ["invalid"]}}),
        _raw(*({"name": "not_st", "args": {}} for _ in range(27))),
        {**_raw({"name": "not_st", "args": {}}), "include_columns": ["CLOSE[0]"]},
    ],
)
def test_invalid_model_candidate_is_rejected_whole(raw: dict[str, object]) -> None:
    with pytest.raises(InvalidScreenDraftError):
        validate_screen_draft(raw, _catalog(), NOW.date())


def test_model_cannot_silently_change_selected_date() -> None:
    with pytest.raises(ScreenDateMismatchError):
        validate_screen_draft(
            _raw({"name": "not_st", "args": {}}, trade_date="2026-09-23"),
            _catalog(), NOW.date(),
        )


def test_new_parser_mode_uses_bounded_official_adapter_and_selected_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    result = _raw({"name": "not_st", "args": {}})

    def fake_openai(**options: object) -> object:
        captured.update(options)

        def create(**request: object) -> object:
            captured["request"] = request
            return SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        message=SimpleNamespace(
                            tool_calls=[
                                SimpleNamespace(
                                    function=SimpleNamespace(
                                        name="build_screen", arguments=json.dumps(result)
                                    )
                                )
                            ]
                        )
                    )
                ]
            )

        return SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=create)))

    monkeypatch.setattr("rquant.web.nl_parser.OpenAI", fake_openai)
    parser = OpenAiScreenPlanParser(api_key=SecretStr("secret-token"), model="configured-model")
    assert parser.parse_new("排除 ST", "2026-09-24") == result
    assert captured["base_url"] == "https://api.openai.com/v1"
    assert captured["timeout"] == 12.0
    assert captured["max_retries"] == 0
    request = captured["request"]
    assert isinstance(request, dict)
    assert request["max_completion_tokens"] == 4096
    assert "2026-09-24" in request["messages"][0]["content"]
    assert "include_columns" not in request["tools"][0]["function"]["parameters"]["properties"]
