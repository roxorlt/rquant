"""Natural-language pool drafts stay bounded and never become saved commands."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from rquant.pool_definition_projection import build_pool_definition_rows
from rquant.serving_read_models import ServingProjectionPayload
from rquant.web.app import create_app
from rquant.web.models.pool_editor import EditablePool, EditorRuleCall
from rquant.web.nl_parser import (
    NlClarificationNeededError,
    NlParserUnavailableError,
    OpenAiScreenPlanParser,
)
from rquant.web.pool_nl_preview import (
    InvalidPoolDraftError,
    NoPoolRuleChangeError,
    validate_pool_draft,
)
from rquant.web.settings import WebSettings
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture

NOW = FIXTURE_BUILT_AT + timedelta(seconds=30)
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

    def parse_edit(
        self, instruction: str, current_rules: list[EditorRuleCall]
    ) -> dict[str, object]:
        self.calls += 1
        assert instruction == "流通市值低于 200 亿"
        assert current_rules == [EditorRuleCall(name="not_st", args={})]
        if isinstance(self.result, Exception):
            raise self.result
        if self.hook is not None:
            self.hook()
        return self.result


def _raw(*rules: dict[str, object]) -> dict[str, object]:
    return {
        "trade_date": "",
        "stages": [{"label": "筛选条件", "rules": list(rules)}],
    }


def _app(
    root: Path, parser: FakeParser | None, *, include_columns: list[str] | None = None
):
    user_row = {
        "pool_name": "user/样本池",
        "display_name": "样本池",
        "description": "日终观察",
        "source_kind": "user",
        "state": "available",
        "reason": None,
        "version": "a" * 64,
        "command_id": "save-seed",
        "command_hash": "f" * 64,
        "depends_on": "n-shape-pool1",
        "delay_mode": "exact",
        "delay_days": 2,
        "rules_json": json.dumps([{"name": "not_st", "args": {}}]),
        "include_columns_json": json.dumps(include_columns or ["CLOSE[0]"]),
        "can_edit": True,
    }
    projections = (
        ServingProjectionPayload(
            table_name="pool_definition",
            available_at=FIXTURE_BUILT_AT - timedelta(seconds=30),
            rows=tuple(build_pool_definition_rows({}, {}, root_path="/synthetic")) + (user_row,),
        ),
    )
    build_web_fixture(root, "baseline", signal_projections=projections)
    return create_app(
        WebSettings(serving_root=root, stale_after_seconds=600),
        clock=lambda: NOW,
        background=False,
        nl_parser=parser,
    )


def _body(generation_id: str, **changes: Any) -> dict[str, object]:
    return {
        "pool_key": "user/样本池",
        "generation_id": generation_id,
        "expected_version": "a" * 64,
        "instruction": "流通市值低于 200 亿",
        **changes,
    }


def _base(*rules: EditorRuleCall) -> EditablePool:
    return EditablePool(
        key="user/样本池",
        display_name="样本池",
        description="",
        version="a" * 64,
        depends_on=None,
        delay_days=0,
        rule_calls=list(rules),
        include_columns=["CLOSE[0]"],
    )


def test_api_returns_complete_preview_without_saving(tmp_path: Path) -> None:
    parser = FakeParser(
        _raw(
            {"name": "not_st", "args": {}},
            {"name": "circ_mv_lt", "args": {"threshold_yi": "200"}},
        )
    )
    app = _app(tmp_path / "serving", parser)
    with TestClient(app) as client:
        editor = client.get("/api/v1/pools/editor").json()
        response = client.post(
            "/api/v1/pools/editor/nl-preview",
            json=_body(editor["serving"]["generation_id"]),
            headers=HEADERS,
        )
    assert editor["data"]["nl_preview_available"] is True
    assert response.status_code == 200
    data = response.json()
    assert data["base_generation_id"] == editor["serving"]["generation_id"]
    assert data["base_version"] == "a" * 64
    assert data["rule_calls"] == [
        {"name": "not_st", "args": {}},
        {"name": "circ_mv_lt", "args": {"threshold_yi": 200.0, "offset": 0}},
    ]
    assert data["changes"] == [
        {
            "kind": "added",
            "label": "流通市值低于",
            "before": None,
            "after": {"name": "circ_mv_lt", "args": {"threshold_yi": 200.0, "offset": 0}},
        }
    ]
    assert data["message"] is None
    assert parser.calls == 1
    assert not list(tmp_path.rglob("*control*"))


def test_api_admission_rejects_auth_csrf_stale_builtin_and_rate(tmp_path: Path) -> None:
    parser = FakeParser(_raw({"name": "not_st", "args": {}}, {"name": "not_bj", "args": {}}))
    app = _app(tmp_path / "serving", parser)
    with TestClient(app) as client:
        editor = client.get("/api/v1/pools/editor").json()
        generation = editor["serving"]["generation_id"]
        body = _body(generation)
        assert client.post(
            "/api/v1/pools/editor/nl-preview",
            json=body,
            headers={key: value for key, value in HEADERS.items() if key != "x-rquant-user"},
        ).status_code == 401
        assert client.post(
            "/api/v1/pools/editor/nl-preview", json=body, headers={"x-rquant-user": "researcher"}
        ).status_code == 403
        assert client.post(
            "/api/v1/pools/editor/nl-preview", json=body, headers={**HEADERS, "origin": "http://bad.example"}
        ).status_code == 403
        assert client.post(
            "/api/v1/pools/editor/nl-preview", json=_body("other"), headers=HEADERS
        ).status_code == 409
        assert client.post(
            "/api/v1/pools/editor/nl-preview",
            json=_body(generation, expected_version="b" * 64),
            headers=HEADERS,
        ).status_code == 409
        builtin = editor["data"]["copy_sources"][0]["key"]
        assert client.post(
            "/api/v1/pools/editor/nl-preview",
            json=_body(generation, pool_key=builtin),
            headers=HEADERS,
        ).status_code == 409
        assert client.post(
            "/api/v1/pools/editor/nl-preview",
            json=_body(generation, instruction="x" * 501),
            headers=HEADERS,
        ).status_code == 422
        assert parser.calls == 0
        for _ in range(3):
            assert client.post(
                "/api/v1/pools/editor/nl-preview", json=body, headers=HEADERS
            ).status_code == 200
        limited = client.post("/api/v1/pools/editor/nl-preview", json=body, headers=HEADERS)
        assert limited.status_code == 429
        assert limited.headers["retry-after"] == "60"
    assert parser.calls == 3


def test_unconfigured_and_parser_failures_do_not_expose_internal_error(tmp_path: Path) -> None:
    unavailable = _app(tmp_path / "none", None)
    with TestClient(unavailable) as client:
        editor = client.get("/api/v1/pools/editor").json()
        assert editor["data"]["nl_preview_available"] is False
        response = client.post(
            "/api/v1/pools/editor/nl-preview",
            json=_body(editor["serving"]["generation_id"]),
            headers=HEADERS,
        )
        assert response.status_code == 503
    failures = (
        NlParserUnavailableError("secret-token"),
        NlClarificationNeededError("raw-model-text"),
    )
    for index, error in enumerate(failures):
        parser = FakeParser(error)
        app = _app(tmp_path / f"failure-{index}", parser)
        with TestClient(app) as client:
            generation = client.get("/api/v1/pools/editor").json()["serving"]["generation_id"]
            response = client.post(
                "/api/v1/pools/editor/nl-preview", json=_body(generation), headers=HEADERS
            )
        assert response.status_code == (503 if index == 0 else 422)
        assert "secret-token" not in response.text
        assert "raw-model-text" not in response.text


def test_preview_rechecks_serving_after_parser_and_rejects_busy_gate(tmp_path: Path) -> None:
    parser = FakeParser(_raw({"name": "not_st", "args": {}}, {"name": "not_bj", "args": {}}))
    root = tmp_path / "serving"
    app = _app(root, parser)
    with TestClient(app) as client:
        generation = client.get("/api/v1/pools/editor").json()["serving"]["generation_id"]
        assert app.state.web.nl_gate.acquire(blocking=False)
        try:
            busy = client.post(
                "/api/v1/pools/editor/nl-preview", json=_body(generation), headers=HEADERS
            )
        finally:
            app.state.web.nl_gate.release()
        assert busy.status_code == 429
        assert parser.calls == 0

        def replace_generation() -> None:
            build_web_fixture(root, "baseline", sequence=1)
            app.state.web.tracker.refresh()

        parser.hook = replace_generation
        stale = client.post(
            "/api/v1/pools/editor/nl-preview", json=_body(generation), headers=HEADERS
        )
    assert stale.status_code == 409
    assert parser.calls == 1


def test_preview_refuses_legacy_unpublished_pool_columns_before_model(tmp_path: Path) -> None:
    parser = FakeParser(_raw({"name": "not_st", "args": {}}, {"name": "not_bj", "args": {}}))
    app = _app(tmp_path / "serving", parser, include_columns=["PE_TTM[0]"])
    with TestClient(app) as client:
        generation = client.get("/api/v1/pools/editor").json()["serving"]["generation_id"]
        response = client.post(
            "/api/v1/pools/editor/nl-preview", json=_body(generation), headers=HEADERS
        )
    assert response.status_code == 409
    assert parser.calls == 0


def test_diff_handles_duplicates_parameters_and_reordering() -> None:
    old = _base(
        EditorRuleCall(name="circ_mv_lt", args={"threshold_yi": 100}),
        EditorRuleCall(name="circ_mv_lt", args={"threshold_yi": 200}),
        EditorRuleCall(name="not_st", args={}),
    )
    calls, changes = validate_pool_draft(
        _raw(
            {"name": "not_st", "args": {}},
            {"name": "circ_mv_lt", "args": {"threshold_yi": 200}},
            {"name": "circ_mv_lt", "args": {"threshold_yi": 300}},
        ),
        old,
    )
    assert len(calls) == 3
    assert [(change.kind, change.before, change.after) for change in changes] == [
        (
            "parameter_changed",
            EditorRuleCall(name="circ_mv_lt", args={"threshold_yi": 100.0, "offset": 0}),
            EditorRuleCall(name="circ_mv_lt", args={"threshold_yi": 300.0, "offset": 0}),
        )
    ]
    with pytest.raises(NoPoolRuleChangeError):
        validate_pool_draft(
            _raw(
                {"name": "not_st", "args": {}},
                {"name": "circ_mv_lt", "args": {"threshold_yi": 200}},
                {"name": "circ_mv_lt", "args": {"threshold_yi": 100}},
            ),
            old,
        )


@pytest.mark.parametrize(
    ("candidate", "normalized"),
    [
        (
            {"name": "circ_mv_lt", "args": {"threshold_yi": "200", "offset": "1"}},
            {"name": "circ_mv_lt", "args": {"threshold_yi": 200.0, "offset": 1}},
        ),
        (
            {"name": "no_consec_ups_in_window", "args": {"threshold": "3", "window": "8"}},
            {"name": "no_consec_ups_in_window", "args": {"threshold": 3, "window": 8}},
        ),
        (
            {"name": "board_in", "args": {"boards": ["main", "gem"]}},
            {"name": "board_in", "args": {"boards": ["main", "gem"]}},
        ),
        (
            {"name": "gt", "args": {"left": "200", "right": "CLOSE[0]"}},
            {"name": "gt", "args": {"left": 200.0, "right": "CLOSE[0]"}},
        ),
    ],
)
def test_candidate_and_diff_return_typed_normalized_args(
    candidate: dict[str, object], normalized: dict[str, object]
) -> None:
    calls, changes = validate_pool_draft(
        _raw({"name": "not_st", "args": {}}, candidate),
        _base(EditorRuleCall(name="not_st", args={})),
    )
    assert calls[1].model_dump(mode="json") == normalized
    assert changes[0].after is not None
    assert changes[0].after.model_dump(mode="json") == normalized


@pytest.mark.parametrize(
    "raw",
    [
        _raw({"name": "invented_rule", "args": {}}),
        _raw({"name": "circ_mv_lt", "args": {"threshold_yi": 10_001}}),
        _raw({"name": "circ_mv_lt", "args": {"threshold_yi": "10001"}}),
        _raw({"name": "board_in", "args": {"boards": ["main", "unknown"]}}),
        _raw({"name": "gt", "args": {"left": "1e309", "right": "CLOSE[0]"}}),
        _raw({"name": "gt", "args": {"left": "PE_TTM[0]", "right": 9}}),
        {**_raw({"name": "not_st", "args": {}}), "depends_on": "user/other"},
        {**_raw({"name": "not_st", "args": {}}), "include_columns": ["VOL[0]"]},
        _raw(*({"name": "not_st", "args": {}} for _ in range(33))),
    ],
)
def test_model_cannot_expand_authority_or_return_unsaveable_rules(raw: dict[str, object]) -> None:
    with pytest.raises(InvalidPoolDraftError):
        validate_pool_draft(raw, _base(EditorRuleCall(name="not_bj", args={})))


def test_paid_parser_needs_explicit_private_ingress_and_secret_is_redacted(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="private Web ingress"):
        WebSettings(
            serving_root=tmp_path,
            nl_openai_api_key=SecretStr("secret-token"),
            nl_openai_model="configured-model",
        )
    settings = WebSettings.from_env(
        {
            "RQUANT_WEB_NL_OPENAI_API_KEY": "secret-token",
            "RQUANT_WEB_NL_OPENAI_MODEL": "configured-model",
            "RQUANT_WEB_INGRESS_SOCKET": str(tmp_path / "private" / "web.sock"),
        }
    )
    assert settings.nl_openai_api_key is not None
    assert settings.nl_openai_api_key.get_secret_value() == "secret-token"
    assert "secret-token" not in repr(settings)


def test_model_adapter_is_bounded_and_uses_only_official_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    result = _raw({"name": "not_st", "args": {}}, {"name": "not_bj", "args": {}})

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
    assert parser.parse_edit(
        "流通市值低于 200 亿", [EditorRuleCall(name="not_st", args={})]
    ) == result
    assert captured["base_url"] == "https://api.openai.com/v1"
    assert captured["timeout"] == 12.0
    assert captured["max_retries"] == 0
    request = captured["request"]
    assert isinstance(request, dict)
    assert request["model"] == "configured-model"
    assert request["max_completion_tokens"] == 4096
    tools = request["tools"]
    assert isinstance(tools, list)
    assert "include_columns" not in tools[0]["function"]["parameters"]["properties"]
