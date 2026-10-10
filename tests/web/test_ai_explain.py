"""AI explanation: facts in, every number in the answer checked."""

from __future__ import annotations

from fastapi.testclient import TestClient

from rquant.web.ai_explain import unverified_numbers
from rquant.web.app import create_app
from rquant.web.source import FixtureSource, write_demo_research


def test_numbers_not_in_facts_are_flagged() -> None:
    facts = {"总收益": "12.34%", "夏普": "1.50", "持股数上限": "10"}
    assert unverified_numbers("1. 总收益 12.34%，夏普 1.50，持股 10 只", facts) == []
    assert unverified_numbers("总收益约 12.3%，回撤 -8.00%", facts) == ["-8.00%", "12.3%"]


def test_explain_endpoint_uses_injected_model(tmp_path) -> None:
    write_demo_research(tmp_path)
    app = create_app(FixtureSource(), dist=None, research_root=tmp_path)
    seen: list[list[dict[str, str]]] = []

    def fake(messages: list[dict[str, str]]) -> str:
        seen.append(messages)
        total = next(line for line in messages[1]["content"].splitlines() if "总收益" in line)
        return f"1. {total.split('：')[1]} 的收益；2. 编造的 99.99% 胜率"

    app.state.llm = (fake, "fake-model")
    client = TestClient(app)
    run_id = client.get("/api/v1/portfolio-backtests").json()["data"]["runs"][0]["run_id"]
    body = client.post(f"/api/v1/portfolio-backtests/{run_id}/explain").json()
    assert body["unverified"] == ["99.99%"] and body["model"] == "fake-model"
    assert "完全一致" in seen[0][0]["content"]


def test_explain_without_key_is_503(tmp_path, monkeypatch) -> None:
    from rquant.config import settings

    monkeypatch.setattr(settings, "deepseek_api_key", "")
    write_demo_research(tmp_path)
    client = TestClient(create_app(FixtureSource(), dist=None, research_root=tmp_path))
    run_id = client.get("/api/v1/portfolio-backtests").json()["data"]["runs"][0]["run_id"]
    assert client.post(f"/api/v1/portfolio-backtests/{run_id}/explain").status_code == 503
