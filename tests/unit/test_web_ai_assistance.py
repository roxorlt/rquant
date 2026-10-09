"""ASGI uses the actual private identity, owner SQLite and original Serving fixture."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest

from tests.support.web_proxy_identity import ProofTestClient, with_test_proxy_identity
from tests.support.web_serving_fixture import FIXTURE_BUILT_AT
from tests.unit.test_ai_assistance import _owner, _response
from tests.unit.test_ai_interpretation import sealed

if TYPE_CHECKING:
    from rquant.portfolio_backtest_artifact import PortfolioReadResult


class _Gateway:
    def __init__(self, admission):
        self.admission = admission

    def request(self, action, *, authenticated_actor_id):
        return self.admission.dispatch(action, authenticated_actor_id=authenticated_actor_id)


def ai_web_fixture(tmp_path: Path, *, limit: int = 4):
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    calls = []
    def handle(request):
        calls.append(request)
        raw = {"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]}
        return httpx.Response(200, json=_response(usage={"prompt_tokens": 11, "completion_tokens": 4}, arguments=json.dumps(raw)))
    owner, original, adapter = _owner(tmp_path, handle, limit=limit)
    admission = AIAssistanceAdmission(owner=owner, allowed_users=frozenset({"researcher", "other"}))
    settings = with_test_proxy_identity(WebSettings(serving_root=tmp_path / "serving", stale_after_seconds=600))
    settings = WebSettings.model_validate({**settings.model_dump(), "ai_users": frozenset({"researcher", "other"})})
    app = create_app(settings, clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=90), background=False, ai_assistance_gateway=_Gateway(admission))
    return app, owner, original, adapter, calls


HEADERS = {"x-rquant-user": "researcher", "x-rquant-csrf": "1", "origin": "http://testserver"}


def test_ai_routes_require_private_user_csrf_and_owner_lookup(tmp_path: Path) -> None:
    app, owner, original, adapter, calls = ai_web_fixture(tmp_path)
    with ProofTestClient(app) as client:
        body = original.model_dump(mode="json")
        assert client.post("/api/v1/ai/requests", json=body).status_code == 401
        assert client.post("/api/v1/ai/requests", json=body, headers={"x-rquant-user": "researcher"}).status_code == 403
        assert client.post("/api/v1/ai/requests", json=body, headers={**HEADERS, "x-rquant-user": "outsider"}).status_code == 403
        response = client.post("/api/v1/ai/requests", json=body, headers=HEADERS)
        assert response.status_code == 200, response.text
        assert response.json()["data"]["state"] == "completed"
        assert response.headers["cache-control"] == "no-store"
        owner.account = owner.account.model_copy(update={"daily_limit": 0})
        recovered = client.post("/api/v1/ai/requests/lookup", json=body, headers=HEADERS)
        assert recovered.json()["data"] == response.json()["data"]
        assert client.post("/api/v1/ai/requests/lookup", json=body, headers={**HEADERS, "x-rquant-user": "other"}).status_code == 404
        assert client.post("/api/v1/ai/requests", json={**body, "instruction": "changed"}, headers=HEADERS).status_code == 409
        usage = client.get("/api/v1/ai/usage?start_date=2026-09-01&end_date=2026-09-30", headers=HEADERS)
        assert usage.json()["data"]["summary"]["input_tokens"] == 11
        other = client.get("/api/v1/ai/usage?start_date=2026-09-01&end_date=2026-09-30", headers={**HEADERS, "x-rquant-user": "other"})
        assert other.json()["data"]["summary"]["calls"] == 0
        assert len(calls) == 1
    adapter.close()


def test_actual_generate_route_continues_only_proven_reserved_original(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.ai_assistance import AIAssistanceOwner
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.page_control import PageControlOutbox
    from tests.unit.test_ai_assistance import _reserve_with_interruption
    app, owner, original, adapter, calls = ai_web_fixture(tmp_path, limit=1)
    try:
        waiting = _reserve_with_interruption(owner, original, monkeypatch)
        restarted = AIAssistanceOwner(
            outbox=PageControlOutbox(owner.outbox.path), account=owner.account,
            provider=adapter, contexts=owner.contexts, clock=owner.clock,
        )
        app.state.web.ai_assistance_gateway.admission = AIAssistanceAdmission(owner=restarted, allowed_users=frozenset({"researcher", "other"}))
        body = original.model_dump(mode="json")
        with ProofTestClient(app) as client:
            looked = client.post("/api/v1/ai/requests/lookup", json=body, headers=HEADERS)
            assert looked.status_code == 200 and looked.json()["data"]["state"] == "reserved"
            assert calls == []
            response = client.post("/api/v1/ai/requests", json=body, headers=HEADERS)
            assert response.status_code == 200, response.text
            assert response.json()["data"]["state"] == "completed"
            assert len(calls) == 1
            assert restarted.outbox.ai_usage_lookup("researcher", original.request_id).binding == waiting.binding
            old_body = original.model_dump(mode="json", exclude={"purpose", "request_id", "include_ranking"})
            assert client.post("/api/v1/screen/nl-preview", json=old_body, headers={**HEADERS, "x-rquant-ai-request-id": str(original.request_id)}).status_code == 200
            assert client.post("/api/v1/ai/requests", json={**body, "instruction": "different body"}, headers=HEADERS).status_code == 409
            assert client.post("/api/v1/ai/requests/lookup", json=body, headers={**HEADERS, "x-rquant-user": "other"}).status_code == 404
            assert len(calls) == 1
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


def test_actual_interpretation_route_reuses_cache_without_new_provider_usage(
    tmp_path: Path, sealed: PortfolioReadResult, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from uuid import UUID, uuid4
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.unit.test_ai_assistance import _interpretation_owner
    owner, original, adapter, calls, reads, _ = _interpretation_owner(tmp_path, sealed, monkeypatch)
    settings = with_test_proxy_identity(WebSettings(serving_root=tmp_path / "serving"))
    settings = WebSettings.model_validate(settings.model_dump() | {"ai_users": frozenset({"alice", "other"})})
    app = create_app(settings, clock=owner.clock, background=False,
        ai_assistance_gateway=_Gateway(AIAssistanceAdmission(owner=owner, allowed_users=frozenset({"alice", "other"}))))
    actor = {**HEADERS, "x-rquant-user": "alice"}
    try:
        with ProofTestClient(app) as client:
            first = client.post("/api/v1/ai/requests", json=original.model_dump(mode="json"), headers=actor)
            assert first.status_code == 200, first.text
            body = original.model_copy(update={"request_id": uuid4()}).model_dump(mode="json")
            before_reads = len(reads)
            reused = client.post("/api/v1/ai/requests", json=body, headers=actor)
            assert reused.status_code == 200, reused.text
            assert reused.json()["data"]["state"] == "completed"
            assert reused.json()["data"]["result"] == first.json()["data"]["result"]
            assert len(reads) > before_reads and len(calls) == 1
            record = owner.outbox.ai_usage_lookup("alice", UUID(body["request_id"]))
            assert record.state == "not_dispatched" and record.error_code == "cache_reused"
            assert record.dispatch_token is None and record.usage.total_tokens is None
            owner.account = owner.account.model_copy(update={"daily_limit": 0})
            owner.provider = None
            recovered = client.post("/api/v1/ai/requests/lookup", json=body, headers=actor)
            assert recovered.json()["data"] == reused.json()["data"]
            assert client.post("/api/v1/ai/requests/lookup", json=body, headers={**actor, "x-rquant-user": "other"}).status_code == 404
            used = client.get("/api/v1/ai/usage?start_date=2026-09-01&end_date=2026-09-30", headers=actor)
            assert used.json()["data"]["summary"]["calls"] == 1
            assert used.json()["data"]["summary"]["input_tokens"] == 11
            assert len(calls) == 1
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


def test_old_screen_entry_shares_original_uuid_and_account_budget(tmp_path: Path) -> None:
    app, owner, original, adapter, calls = ai_web_fixture(tmp_path, limit=1)
    old_body = original.model_dump(mode="json", exclude={"purpose", "request_id", "include_ranking"})
    with ProofTestClient(app) as client:
        assert client.post("/api/v1/screen/nl-preview", json=old_body, headers=HEADERS).status_code == 422
        response = client.post("/api/v1/screen/nl-preview", json=old_body, headers={**HEADERS, "x-rquant-ai-request-id": str(original.request_id)})
        assert response.status_code == 200, response.text
        assert response.json()["conditions"][0]["key"] == "not_st"
        recovered = client.post("/api/v1/ai/requests/lookup", json=original.model_dump(mode="json"), headers=HEADERS)
        assert recovered.status_code == 200
        assert owner.outbox.ai_usage_lookup("researcher", original.request_id).usage.total_tokens == 15
        assert len(calls) == 1
    adapter.close()


def test_original_auxiliary_gate_and_rate_preserve_original_lookup_first(tmp_path: Path) -> None:
    from uuid import uuid4
    app, owner, original, adapter, calls = ai_web_fixture(tmp_path, limit=5)
    body = original.model_dump(mode="json")
    old_body = original.model_dump(mode="json", exclude={"purpose", "request_id", "include_ranking"})
    try:
        with ProofTestClient(app) as client:
            assert app.state.web.nl_gate.acquire(blocking=False)
            try:
                busy = client.post("/api/v1/screen/nl-preview", json=old_body,
                    headers={**HEADERS, "x-rquant-ai-request-id": str(original.request_id)})
                assert busy.status_code == 429, busy.text
                assert not calls
            finally:
                app.state.web.nl_gate.release()
            for index in range(3):
                current = body if index == 0 else {**body, "request_id": str(uuid4())}
                response = client.post("/api/v1/ai/requests", json=current, headers=HEADERS)
                assert response.status_code == 200, response.text
            limited = client.post("/api/v1/ai/requests", json={**body, "request_id": str(uuid4())}, headers=HEADERS)
            assert limited.status_code == 429
            recovered = client.post("/api/v1/screen/nl-preview", json=old_body,
                headers={**HEADERS, "x-rquant-ai-request-id": str(original.request_id)})
            assert recovered.status_code == 200, recovered.text
        assert len(calls) == 3
        assert owner.outbox.ai_usage_lookup("researcher", original.request_id).usage.total_tokens == 15
    finally:
        adapter.close()


def test_backtest_preparation_routes_use_original_private_history_and_explicit_confirmation(tmp_path: Path) -> None:
    from uuid import uuid4
    from tests.unit.test_ai_screen_backtest_source import prepared_history_fixture
    from rquant.ai_screen_backtest_source import AIScreenBacktestPipeline, AIHistoricalScreenSource, AIScreenBacktestArtifacts
    from rquant.ai_assistance import AIAssistanceOwner, AIAccountConfig, AIAssistanceContexts
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.web.app import create_app
    from rquant.web.settings import WebSettings
    from tests.support.ai_assistance_fixture import DirectAIGateway
    screen, base, config, control, history, command, now = prepared_history_fixture(tmp_path)
    owner = AIAssistanceOwner(outbox=control.outbox, account=AIAccountConfig(account_id="shared", model_id="gpt-test"),
        provider=None, contexts=AIAssistanceContexts(screen=screen), clock=lambda: now)
    owner.backtests = AIScreenBacktestPipeline(history=history,
        source=AIHistoricalScreenSource(screen=screen, base=base, default_config=config),
        artifacts=AIScreenBacktestArtifacts(tmp_path / "private" / "prepared"), clock=lambda: now)
    owner.control = control
    settings = with_test_proxy_identity(WebSettings(serving_root=tmp_path / "absent-serving"))
    settings = WebSettings.model_validate(settings.model_dump() | {"ai_users": frozenset({"researcher", "other"})})
    app = create_app(settings, background=False, clock=lambda: now,
        ai_assistance_gateway=DirectAIGateway(AIAssistanceAdmission(owner=owner, allowed_users=frozenset({"researcher", "other"}))))
    body = {"request_id": str(uuid4()), "execution_id": command.command_id,
        "start_date": config.start_date.isoformat(), "end_date": config.end_date.isoformat()}
    with ProofTestClient(app) as client:
        response = client.post("/api/v1/ai/backtests/prepare", json=body, headers=HEADERS)
        assert response.status_code == 200, response.text
        view = response.json()["data"]
        assert view["complete"] and view["candidate_count"] == 2
        assert client.post("/api/v1/ai/backtests/prepare/lookup", json=body, headers={**HEADERS, "x-rquant-user": "other"}).status_code == 404
        assert client.post("/api/v1/ai/backtests/prepare/lookup", json=body, headers=HEADERS).json()["data"] == view
        assert client.post("/api/v1/ai/backtests/prepare", json=body, headers={"x-rquant-user": "researcher"}).status_code == 403
        with control.outbox._connect() as connection:
            assert connection.execute("SELECT COUNT(*) FROM page_control_command WHERE command_kind='submit_portfolio_backtest'").fetchone()[0] == 0


def test_news_cache_route_preserves_private_identity_dates_and_no_model_calls(tmp_path:Path) -> None:
    from tests.unit.test_stock_news_digest import collection,draft
    from rquant.stock_news_digest import validate_stock_news_digest
    from rquant.stock_news_sources import StockNewsArtifactStore
    app,owner,_,adapter,calls=ai_web_fixture(tmp_path)
    store=StockNewsArtifactStore(tmp_path/'private-news',outbox=owner.outbox)
    facts=collection().model_copy(update={'owner_uid':'researcher'})
    facts=facts.model_copy(update={'documents':tuple(d.model_copy(update={'first_collected_at':FIXTURE_BUILT_AT}) for d in facts.documents),
        'coverage':tuple(c.model_copy(update={'start_date':FIXTURE_BUILT_AT.date(),'end_date':FIXTURE_BUILT_AT.date(),'collected_at':FIXTURE_BUILT_AT}) for c in facts.coverage)})
    store.put_facts(facts)
    store.put_digest(facts,validate_stock_news_digest(facts,draft=draft(facts)),model_id=owner.account.model_id,template_version=owner.account.template_version)
    owner.contexts.news=store
    from tests.support.ai_assistance_fixture import publish_original_ai_views
    publish_original_ai_views(owner,tmp_path/'serving')
    app.state.web.tracker.refresh()
    with ProofTestClient(app) as client:
        assert client.get('/api/v1/ai/news/000001.SZ').status_code==401
        response=client.get('/api/v1/ai/news/000001.SZ',headers=HEADERS)
        assert response.status_code==200,response.text
        data=response.json()['data']
        assert data['state']=='ready' and data['content']['sources'][0]['first_collected_at']
        assert response.headers['cache-control']=='no-store'
        other=client.get('/api/v1/ai/news/000001.SZ',headers={**HEADERS,'x-rquant-user':'other'})
        assert other.json()['data']['state']=='missing'
        assert calls==[]
    adapter.close()


def test_original_integration_fixture_wires_screen_pool_news_and_full_preparer(tmp_path: Path) -> None:
    from uuid import uuid4
    from tests.support.ai_assistance_fixture import build_ai_integration_fixture
    fixture = build_ai_integration_fixture(tmp_path)
    try:
        with ProofTestClient(fixture.app) as client:
            catalog = client.get('/api/v1/screen/blocks?mode=daily', headers=HEADERS).json()['data']
            request = {'purpose': 'screen', 'request_id': str(uuid4()), 'instruction': '排除 ST，市值从小到大',
                'trade_date': catalog['dates'][0], 'source_kind': catalog['source_kind'],
                'source_identity': catalog['source']['identity'], 'include_ranking': True}
            generated = client.post('/api/v1/ai/requests', json=request, headers=HEADERS)
            assert generated.status_code == 200, generated.text
            definition = generated.json()['data']['result']['definition']
            assert definition['ranking']['top_n'] == 1
            command = {'kind': 'execute_screen_query', 'command_id': str(uuid4()),
                'requested_at': fixture.now.isoformat(), 'definition': definition, 'page_size': 20}
            executed = client.post('/api/v1/screen/query/execute', json=command, headers=HEADERS)
            assert executed.status_code == 200, executed.text
            assert executed.json()['receipt']['status'] == 'succeeded'
            results = client.get('/api/v1/screen/query/executions/' + command['command_id'] + '/results', headers=HEADERS)
            assert len(results.json()['results']['rows']) == 1
            editor = client.get('/api/v1/pools/editor', headers=HEADERS).json()
            base = next(p for p in editor['data']['pools'] if p['key'] == 'user/ai-owned-pool')
            pool_request = {'purpose': 'pool_edit', 'request_id': str(uuid4()), 'instruction': '添加小市值条件',
                'generation_id': editor['serving']['generation_id'], 'pool_key': base['key'], 'expected_version': base['version']}
            edited = client.post('/api/v1/ai/requests', json=pool_request, headers=HEADERS)
            assert edited.status_code == 200, edited.text
            content = edited.json()['data']['result']
            assert content['base']['ranking'] == base['ranking']
            assert content['base']['depends_on'] == base['depends_on']
            assert content['base']['include_columns'] == base['include_columns']
            assert len(content['changes']) == 1
            news = client.get('/api/v1/ai/news/600001.SH', headers=HEADERS)
            assert news.json()['data']['state'] == 'ready', news.text
            assert {s['nature'] for s in news.json()['data']['content']['digest']['statements']} == {'actual', 'forecast'}
            original = {'request_id': str(uuid4()), 'execution_id': command['command_id'],
                'start_date': fixture.config.start_date.isoformat(), 'end_date': fixture.config.end_date.isoformat()}
            prepared = client.post('/api/v1/ai/backtests/prepare', json=original, headers=HEADERS)
            assert prepared.status_code == 200, prepared.text
            proof = prepared.json()['data']
            assert proof['complete'] and proof['candidate_count'] == 2
            assert proof['config']['weight_rule'] == fixture.config.weight_rule.model_dump(mode='json')
            confirm = {'command_id': str(uuid4()), 'requested_at': fixture.now.isoformat(),
                'prepared_request_id': original['request_id'], 'config_sha256': proof['config_sha256'], 'proof_sha256': proof['proof_sha256']}
            accepted = client.post('/api/v1/ai/backtests/confirm', json=confirm, headers=HEADERS)
            assert accepted.status_code == 200, accepted.text
            assert accepted.json()['data']['receipt']['status'] == 'succeeded'
            assert len(fixture.foundation.commands.spool.pending()) == 1
            assert len(tuple(fixture.foundation.input_root.iterdir())) == 1
            from zoneinfo import ZoneInfo
            budget_day = fixture.now.astimezone(ZoneInfo("Asia/Shanghai")).date()
            usage = fixture.owner.usage('researcher', start_date=budget_day, end_date=budget_day)
            assert usage.summary.calls == 3
            assert fixture.model.calls == 3 and len(fixture.source_calls) == 4
    finally:
        fixture.close()
