"""Real bounded model transport with only external HTTP replaced by MockTransport."""

from __future__ import annotations

import importlib
import json
from collections.abc import Iterator
from typing import TYPE_CHECKING

import httpx
import pytest
from pydantic import SecretStr
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
from tests.unit.test_ai_interpretation import sealed

if TYPE_CHECKING:
    from rquant.ai_assistance import AIAssistanceOwner
    from rquant.ai_interpretation import SealedInterpretationFacts
    from rquant.ai_usage import AIUsageRecord
    from rquant.portfolio_backtest_artifact import PortfolioReadResult
    from rquant.web.models.ai_assistance import AIGenerateRequest, AIInterpretationRequest
    from rquant.web.nl_parser import OpenAiScreenPlanParser


def _modules():
    return importlib.import_module("rquant.ai_assistance"), importlib.import_module("rquant.web.nl_parser")


def _prompt():
    domain, _ = _modules()
    return domain.AIModelPrompt(model_id="gpt-test", system="只生成草稿。", instruction="市值较小", tool_name="build_screen", tool_schema={"type": "object", "properties": {}})


def _response(*, usage: object = None, arguments: str = '{"stages": []}', name: str = "build_screen") -> dict:
    return {"usage": usage, "choices": [{"message": {"tool_calls": [{"type": "function", "function": {"name": name, "arguments": arguments}}]}}]}


def _adapter(handler, *, clock=None):
    _, module = _modules()
    options = {"api_key": SecretStr("offline-placeholder"), "model": "gpt-test", "transport": httpx.MockTransport(handler)}
    if clock is not None:
        options["monotonic"] = clock
    return module.OpenAiScreenPlanParser(**options)


def test_official_endpoint_bounded_settings_and_measured_usage() -> None:
    seen = []
    def handle(request):
        seen.append(request)
        assert str(request.url) == "https://api.openai.com/v1/chat/completions"
        body = json.loads(request.content)
        assert body["model"] == "gpt-test"
        assert body["max_completion_tokens"] == 4096
        assert body["temperature"] == 0
        assert body["tool_choice"] == {"type": "function", "function": {"name": "build_screen"}}
        assert set(request.extensions["timeout"].values()) == {12.0}
        return httpx.Response(200, json=_response(usage={"prompt_tokens": 17, "completion_tokens": 4, "total_tokens": 21}))
    adapter = _adapter(handle)
    reply = adapter.generate(_prompt())
    assert len(seen) == 1
    assert reply.draft == {"stages": []}
    assert reply.usage.input_tokens == 17 and reply.usage.output_tokens == 4
    adapter.close()


@pytest.mark.parametrize("usage", [None, {}, {"prompt_tokens": True, "completion_tokens": 2}, {"prompt_tokens": -1, "completion_tokens": 2}, {"prompt_tokens": 2, "completion_tokens": 3, "total_tokens": 6}, {"prompt_tokens": 1.0, "completion_tokens": 2}, {"prompt_tokens": 2**63, "completion_tokens": 2}])
def test_missing_or_invalid_provider_usage_is_unknown(usage: object) -> None:
    adapter = _adapter(lambda request: httpx.Response(200, json=_response(usage=usage)))
    result = adapter.generate(_prompt())
    assert result.usage.input_tokens is None and result.usage.output_tokens is None
    adapter.close()


@pytest.mark.parametrize(
    "arguments,name",
    [("bad json", "build_screen"), ("[]", "build_screen"), ('{"bad": "' + "x" * 16384 + '"}', "build_screen"), ("{}", "execute_shell")],
    ids=["invalid-json", "wrong-shape", "oversized", "wrong-tool"],
)
def test_invalid_draft_retains_real_usage(arguments: str, name: str) -> None:
    adapter = _adapter(lambda request: httpx.Response(200, json=_response(usage={"prompt_tokens": 8, "completion_tokens": 3}, arguments=arguments, name=name)))
    result = adapter.generate(_prompt())
    assert result.error_code == "invalid_model_output" and result.draft is None
    assert result.usage.total_tokens == 11
    adapter.close()


def test_input_rejected_before_any_transport_request() -> None:
    domain, _ = _modules()
    seen = []
    adapter = _adapter(lambda request: seen.append(request))
    prompt = _prompt().model_copy(update={"instruction": "量" * 23000})
    with pytest.raises(domain.AIInputTooLarge):
        adapter.prepare(prompt)
    assert seen == []
    adapter.close()


class _Chunks(httpx.SyncByteStream):
    def __init__(self, chunks: list[bytes]):
        self.chunks = chunks
        self.consumed = 0
        self.closed = False

    def __iter__(self) -> Iterator[bytes]:
        for chunk in self.chunks:
            self.consumed += 1
            yield chunk

    def close(self) -> None:
        self.closed = True


def test_response_stops_at_transport_cap_not_post_parse() -> None:
    domain, _ = _modules()
    stream = _Chunks([b" " * 131072, b" " * 131073, b"never-consume"])
    calls = []
    def handle(request):
        calls.append(request)
        return httpx.Response(200, stream=stream)
    adapter = _adapter(handle)
    with pytest.raises(domain.AIResponseUnknown):
        adapter.generate(_prompt())
    assert len(calls) == 1 and stream.consumed == 2 and stream.closed
    adapter.close()


def test_stream_deadline_is_total_elapsed_time() -> None:
    domain, _ = _modules()
    ticks = iter([0.0, 0.0, 13.0])
    stream = _Chunks([b"{", b"}", b"never-consume"])
    adapter = _adapter(lambda request: httpx.Response(200, stream=stream), clock=lambda: next(ticks))
    with pytest.raises(domain.AIResponseUnknown):
        adapter.generate(_prompt())
    assert stream.closed and stream.consumed < 3
    adapter.close()


def test_redirect_or_timeout_is_not_retried() -> None:
    domain, _ = _modules()
    for timeout in (False, True):
        calls = []
        def handle(request):
            calls.append(request)
            if timeout:
                raise httpx.ReadTimeout("offline timeout")
            return httpx.Response(307, headers={"location": "https://example.invalid/redirect"})
        adapter = _adapter(handle)
        with pytest.raises(domain.AIResponseUnknown):
            adapter.generate(_prompt())
        assert len(calls) == 1
        adapter.close()


def _owner(tmp_path: Path, handler, *, limit: int = 3):
    from rquant.page_control import PageControlOutbox
    from rquant.screen.query_admission import ScreenQueryExecutor, ScreenQueryPrivateConfig
    import os
    domain, _ = _modules()
    root = tmp_path / "serving"
    build_web_fixture(root, "baseline")
    executor = ScreenQueryExecutor(ScreenQueryPrivateConfig(socket_path=Path("/private/tmp/offline-ai-unused.sock"), trusted_web_uid=os.geteuid() + 1, shared_gid=os.getegid(), allowed_users=frozenset({"researcher", "other"}), serving_root=root), clock=lambda: FIXTURE_BUILT_AT + timedelta(seconds=90))
    adapter = _adapter(handler)
    outbox = PageControlOutbox(tmp_path / "original.sqlite3")
    owner = domain.AIAssistanceOwner(outbox=outbox, account=domain.AIAccountConfig(account_id="shared", model_id="gpt-test", daily_limit=limit), provider=adapter, contexts=domain.AIAssistanceContexts(screen=executor), clock=executor.clock)
    from rquant.web.models.ai_assistance import AIScreenRequest
    catalog = owner.contexts.screen_catalog()
    request = AIScreenRequest(request_id=uuid4(), source_kind=catalog.source_kind, source_identity=catalog.source.identity, trade_date=catalog.dates[0], instruction="排除ST", include_ranking=False)
    return owner, request, adapter


def test_owner_dispatches_original_journal_before_real_http_and_recovers_old_body(tmp_path: Path) -> None:
    calls = []
    holder = []
    def handle(request):
        calls.append(request)
        original = holder[0].outbox.ai_usage_lookup("researcher", holder[1].request_id)
        assert original.state == "dispatched"
        return httpx.Response(200, json=_response(usage={"prompt_tokens": 8, "completion_tokens": 3}, arguments=json.dumps({"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]})))
    owner, request, adapter = _owner(tmp_path, handle, limit=1)
    holder.extend([owner, request])
    result = owner.generate("researcher", request)
    assert result.state == "completed" and result.result.definition.conditions[0].name == "not_st"
    owner.account = owner.account.model_copy(update={"daily_limit": 0})
    assert owner.generate("researcher", request) == result
    assert len(calls) == 1
    from rquant.ai_usage import AIRequestConflict, AIRequestNotFound
    with pytest.raises(AIRequestConflict):
        owner.generate("researcher", request.model_copy(update={"instruction": "改掉原请求"}))
    with pytest.raises(AIRequestNotFound):
        owner.generate("other", request)
    adapter.close()


def test_owner_unknown_request_never_reissues_and_default_zero_closes_new_calls(tmp_path: Path) -> None:
    calls = []
    def handle(request):
        calls.append(request)
        raise httpx.ReadTimeout("offline")
    owner, request, adapter = _owner(tmp_path, handle, limit=1)
    assert owner.generate("researcher", request).state == "unknown"
    assert owner.generate("researcher", request).state == "unknown"
    assert len(calls) == 1
    from rquant.ai_usage import AIBudgetExceeded
    owner.account = owner.account.model_copy(update={"daily_limit": 0})
    with pytest.raises(AIBudgetExceeded):
        owner.generate("researcher", request.model_copy(update={"request_id": uuid4()}))
    assert len(calls) == 1
    adapter.close()


def test_domain_rejected_result_still_completes_with_measured_usage(tmp_path: Path) -> None:
    raw = {"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "execute_shell", "args": {}}]}]}
    owner, request, adapter = _owner(tmp_path, lambda request: httpx.Response(200, json=_response(usage={"prompt_tokens": 6, "completion_tokens": 3}, arguments=json.dumps(raw))))
    view = owner.generate("researcher", request)
    assert view.state == "completed" and view.result is None
    assert owner.outbox.ai_usage_lookup("researcher", request.request_id).usage.total_tokens == 9
    assert "execute_shell" not in view.model_dump_json()
    adapter.close()


def test_source_rotation_while_model_runs_rejects_draft_but_counts_usage(tmp_path: Path) -> None:
    holder = []
    def handle(request):
        build_web_fixture(tmp_path / "serving", "baseline", sequence=1)
        holder[0].contexts.screen.tracker.refresh()
        return httpx.Response(200, json=_response(usage={"prompt_tokens": 4, "completion_tokens": 2}, arguments=json.dumps({"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]})))
    owner, request, adapter = _owner(tmp_path, handle)
    holder.append(owner)
    assert owner.generate("researcher", request).result is None
    assert owner.outbox.ai_usage_lookup("researcher", request.request_id).usage.total_tokens == 6
    adapter.close()


def test_new_ranking_uses_original_catalog_and_keeps_zero_individual_weight(tmp_path: Path) -> None:
    holder = []
    def handle(request):
        metric = holder[0].contexts.screen_catalog().ranking_metrics[0].value
        raw = {"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}], "ranking": {"conditions": [{"metric": metric, "ascending": True, "weight": 1.0}], "top_n": 10}}
        return httpx.Response(200, json=_response(usage={"prompt_tokens": 3, "completion_tokens": 3}, arguments=json.dumps(raw)))
    owner, request, adapter = _owner(tmp_path, handle)
    holder.append(owner)
    request = request.model_copy(update={"include_ranking": True})
    view = owner.generate("researcher", request)
    assert view.result.definition.ranking.top_n == 10
    assert owner.outbox.ai_usage_lookup("researcher", request.request_id).binding.purpose == "screen"
    adapter.close()


def test_news_generation_persists_private_original_cache_and_read_never_calls_model(tmp_path:Path) -> None:
    from tests.unit.test_stock_news_digest import collection,draft
    from rquant.stock_news_sources import StockNewsArtifactStore
    from rquant.web.models.ai_assistance import AINewsRequest
    facts=collection()
    calls=[]
    def handle(request):
        calls.append(request)
        return httpx.Response(200,json=_response(usage={'prompt_tokens':12,'completion_tokens':5},arguments=draft(facts).model_dump_json(),name='summarize_stock'))
    owner,_,adapter=_owner(tmp_path,handle)
    store=StockNewsArtifactStore(tmp_path/'private-news',outbox=owner.outbox)
    store.put_facts(facts)
    owner.contexts.news=store
    body=AINewsRequest(request_id=uuid4(),stock_code=facts.stock_code,context_sha256=facts.context_sha256)
    view=owner.generate('alice',body)
    assert view.state=='completed' and view.result is not None
    assert store.read_digest('alice',facts.stock_code,model_id=owner.account.model_id,template_version=owner.account.template_version)==view.result
    assert owner.stock_news('alice',facts.stock_code).content==view.result
    assert owner.stock_news('other',facts.stock_code).state=='missing'
    citations = {item.fact_id: item for item in view.result.citations}
    for statement in view.result.digest.statements:
        for identifier in statement.content.citations:
            item = citations[identifier]
            document = next(doc for doc in facts.documents if doc.document_id == item.document_id)
            assert document.body[item.body_start:item.body_end] == item.quote
            assert item.body_sha256 == document.body_sha256
    assert len(calls)==1
    adapter.close()


def test_interpretation_read_contract_returns_exact_sealed_binding() -> None:
    from rquant.web.models.ai_assistance import AIInterpretationContextRequest, AIInterpretationView
    from rquant.ai_assistance_contracts import AISealedResultBinding
    body = AIInterpretationContextRequest(source_kind='portfolio',job_id=uuid4(),result_sha256='f'*64)
    binding = AISealedResultBinding(owner_uid='alice',source_kind=body.source_kind,job_id=body.job_id,spec_sha256='d'*64,manifest_sha256='e'*64,result_sha256=body.result_sha256)
    view = AIInterpretationView(binding=binding,cache_key='a'*64)
    assert view.binding == binding


def test_interpretation_cache_is_bound_to_complete_original_facts_model_and_owner(tmp_path:Path, sealed) -> None:
    from tests.unit.test_ai_interpretation import packet, context, draft
    from rquant.ai_interpretation import validate_interpretation
    from rquant.page_control import PageControlOutbox
    domain,_=_modules()
    facts=packet(sealed);bound=context(facts)
    content=validate_interpretation(facts,binding=bound,draft=draft(facts))
    store=domain.AIInterpretationCache(PageControlOutbox(tmp_path/'original.sqlite3'))
    store.put(facts,content)
    assert store.read(facts,bound)==content
    with pytest.raises(LookupError):
        store.read(facts,bound.model_copy(update={'model_id':'another'}))
    with pytest.raises(ValueError):
        store.put(facts,content.model_copy(update={'content_sha256':'a'*64}))


def _reserve_with_interruption(
    owner: AIAssistanceOwner, request: AIGenerateRequest, monkeypatch: pytest.MonkeyPatch,
    *, actor: str = "researcher",
) -> AIUsageRecord:
    def interrupt(*_args: object, **_kwargs: object) -> None:
        raise OSError("synthetic interruption after committed reserve before dispatch")
    with monkeypatch.context() as patch:
        patch.setattr(owner.outbox, "ai_usage_dispatch", interrupt)
        with pytest.raises(OSError, match="before dispatch"):
            owner.generate(actor, request)
    record = owner.outbox.ai_usage_lookup(actor, request.request_id)
    assert record.state == "reserved" and record.dispatch_token is None
    assert record.usage.input_tokens is None and record.usage.output_tokens is None
    return record


@pytest.mark.parametrize("days", [0, 1])
def test_original_owner_restart_resumes_reserved_once_and_keeps_original_budget_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, days: int,
) -> None:
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.page_control import PageControlOutbox
    from rquant.web.models.ai_assistance import AIGenerateAction
    domain, _ = _modules()
    calls = []
    raw = {"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]}
    owner, request, adapter = _owner(tmp_path, lambda sent: calls.append(sent) or httpx.Response(200, json=_response(arguments=json.dumps(raw))), limit=1)
    try:
        waiting = _reserve_with_interruption(owner, request, monkeypatch)
        now = owner.clock() + timedelta(days=days)
        restarted = domain.AIAssistanceOwner(
            outbox=PageControlOutbox(owner.outbox.path), account=owner.account,
            provider=adapter, contexts=owner.contexts, clock=lambda: now,
        )
        admission = AIAssistanceAdmission(owner=restarted, allowed_users=frozenset({"researcher"}))
        result = admission.dispatch(AIGenerateAction(request=request), authenticated_actor_id="researcher").request
        assert result.state == "completed" and result.result is not None
        assert len(calls) == 1
        assert restarted.generate("researcher", request) == result
        finished = restarted.outbox.ai_usage_lookup("researcher", request.request_id)
        assert finished.binding == waiting.binding
        assert restarted.outbox.ai_usage_account_calls("shared", waiting.binding.budget_date) == 1
        if days:
            assert restarted.outbox.ai_usage_account_calls("shared", now.date()) == 0
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


def test_concurrent_original_owner_resumes_claim_only_one_model_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from rquant.page_control import PageControlOutbox
    domain, _ = _modules()
    calls = []
    raw = {"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]}
    owner, request, adapter = _owner(tmp_path, lambda sent: calls.append(sent) or httpx.Response(200, json=_response(arguments=json.dumps(raw))), limit=1)
    try:
        waiting = _reserve_with_interruption(owner, request, monkeypatch)
        workers = [domain.AIAssistanceOwner(outbox=PageControlOutbox(owner.outbox.path), account=owner.account, provider=adapter, contexts=owner.contexts, clock=owner.clock) for _ in range(2)]
        gate = Barrier(2)
        def resume(current: AIAssistanceOwner) -> object:
            return current.generate("researcher", request, submission_gate=lambda: gate.wait(timeout=8))
        with ThreadPoolExecutor(max_workers=2) as executor:
            views = list(executor.map(resume, workers))
        assert len(calls) == 1
        assert all(view.state in {"dispatched", "completed"} for view in views)
        record = owner.outbox.ai_usage_lookup("researcher", request.request_id)
        assert record.binding == waiting.binding and record.state == "completed"
        assert owner.outbox.ai_usage_account_calls("shared", waiting.binding.budget_date) == 1
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


@pytest.mark.parametrize("changed", ["account_id", "model_id", "template_version", "context", "disabled", "no_provider"])
def test_original_reserved_request_changed_premise_is_proven_unsent_without_rebinding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed: str,
) -> None:
    calls = []
    owner, request, adapter = _owner(tmp_path, lambda sent: calls.append(sent))
    try:
        waiting = _reserve_with_interruption(owner, request, monkeypatch)
        if changed in {"account_id", "model_id", "template_version"}:
            owner.account = owner.account.model_copy(update={changed: "changed-original-premise"})
        elif changed == "disabled":
            owner.account = owner.account.model_copy(update={"daily_limit": 0})
        elif changed == "no_provider":
            owner.provider = None
        else:
            build_web_fixture(tmp_path / "serving", "baseline", sequence=1)
        view = owner.generate("researcher", request)
        assert view.state == "not_dispatched"
        assert view.result is None and calls == []
        original = owner.outbox.ai_usage_lookup("researcher", request.request_id)
        assert original.binding == waiting.binding and original.dispatch_token is None
        assert original.usage.input_tokens is None and original.usage.output_tokens is None
        assert owner.outbox.ai_usage_account_calls(waiting.binding.account_id, waiting.binding.budget_date) == 0
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


def test_reserved_resume_requires_current_write_role_and_exact_owner_body(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.ai_usage import AIRequestConflict, AIRequestNotFound
    from rquant.collaboration_commands import PageControlRoleAuthority
    from rquant.collaboration_roles import RoleEntry, RoleState
    calls = []
    owner, request, adapter = _owner(tmp_path, lambda sent: calls.append(sent))
    try:
        waiting = _reserve_with_interruption(owner, request, monkeypatch)
        with pytest.raises(AIRequestConflict):
            owner.generate("researcher", request.model_copy(update={"instruction": "different body"}))
        with pytest.raises(AIRequestNotFound):
            owner.generate("other", request)
        role_path = owner.outbox.path.parent / "roles.json"
        state = RoleState.create(revision=1, users=(RoleEntry(username="admin", role="admin"), RoleEntry(username="researcher", role="viewer")))
        role_path.write_text(state.model_dump_json())
        role_path.chmod(0o600)
        owner.outbox.collaboration = PageControlRoleAuthority(mode="enforced", roles_path=role_path, clock=owner.clock)
        with pytest.raises(PermissionError):
            owner.generate("researcher", request)
        assert owner.outbox.ai_usage_lookup("researcher", request.request_id) == waiting
        assert calls == []
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


def test_reserved_source_change_at_submission_fence_does_not_dispatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    raw = {"trade_date": "", "stages": [{"label": "条件", "rules": [{"name": "not_st", "args": {}}]}]}
    owner, request, adapter = _owner(tmp_path, lambda sent: calls.append(sent) or httpx.Response(200, json=_response(arguments=json.dumps(raw))))
    try:
        waiting = _reserve_with_interruption(owner, request, monkeypatch)
        passes = 0
        def rotate_before_dispatch() -> None:
            nonlocal passes
            passes += 1
            if passes == 2:
                build_web_fixture(tmp_path / "serving", "baseline", sequence=1)
        view = owner.generate("researcher", request, submission_gate=rotate_before_dispatch)
        assert view.state == "not_dispatched" and calls == []
        record = owner.outbox.ai_usage_lookup("researcher", request.request_id)
        assert record.binding == waiting.binding and record.dispatch_token is None
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


def _interpretation_owner(
    tmp_path: Path, sealed: PortfolioReadResult, monkeypatch: pytest.MonkeyPatch,
) -> tuple[AIAssistanceOwner, AIInterpretationRequest, OpenAiScreenPlanParser, list[httpx.Request], list[AIInterpretationRequest], SealedInterpretationFacts]:
    """Typed full original bundle response only; physical reader authority is Root's gate."""
    from tests.unit.test_ai_interpretation import packet
    from rquant.web.models.ai_assistance import AIInterpretationRequest
    facts = packet(sealed)
    calls = []
    reads = []
    def handle(sent: httpx.Request) -> httpx.Response:
        calls.append(sent)
        system = json.loads(sent.content)["messages"][0]["content"]
        key = system.split("context_sha256=", 1)[1].split("\n", 1)[0]
        text = {"text": "累计收益为 {{summary.total_return}}。", "citations": ["summary.total_return"]}
        raw = {"context_sha256": key, "sections": [{"key": name, "paragraphs": [text]} for name in ("overview", "annual", "risk", "suggestions")]}
        return httpx.Response(200, json=_response(usage={"prompt_tokens": 11, "completion_tokens": 4}, arguments=json.dumps(raw), name="interpret_result"))
    owner, _, adapter = _owner(tmp_path, handle, limit=3)
    def original_response(actor: str, body: AIInterpretationRequest) -> SealedInterpretationFacts:
        assert actor == facts.binding.owner_uid
        assert (body.source_kind, body.job_id, body.spec_sha256, body.manifest_sha256, body.result_sha256) == (facts.binding.source_kind, facts.binding.job_id, facts.binding.spec_sha256, facts.binding.manifest_sha256, facts.binding.result_sha256)
        reads.append(body)
        return facts
    monkeypatch.setattr(owner.contexts, "_interpretation_context", original_response)
    body = AIInterpretationRequest(request_id=uuid4(), **facts.binding.model_dump(exclude={"owner_uid"}))
    return owner, body, adapter, calls, reads, facts


def test_verified_interpretation_cache_new_uuid_and_restarted_lookup_have_zero_extra_calls(
    tmp_path: Path, sealed: PortfolioReadResult, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.page_control import PageControlOutbox
    from rquant.web.models.ai_assistance import AIGenerateAction, AILookupAction
    from rquant.ai_usage import AIRequestConflict, AIRequestNotFound
    domain, _ = _modules()
    owner, body, adapter, calls, reads, facts = _interpretation_owner(tmp_path, sealed, monkeypatch)
    try:
        admission = AIAssistanceAdmission(owner=owner, allowed_users=frozenset({"alice", "other"}))
        first = admission.dispatch(AIGenerateAction(request=body), authenticated_actor_id="alice").request
        assert first.state == "completed" and first.result is not None
        assert len(calls) == 1 and owner.interpretation("alice", body).content == first.result.interpretation
        another = body.model_copy(update={"request_id": uuid4()})
        read_count = len(reads)
        cached = admission.dispatch(AIGenerateAction(request=another), authenticated_actor_id="alice").request
        assert cached.state == "completed" and cached.result == first.result
        assert cached.request_id == another.request_id and len(calls) == 1
        assert len(reads) > read_count
        record = owner.outbox.ai_usage_lookup("alice", another.request_id)
        assert record.state == "not_dispatched" and record.error_code == "cache_reused"
        assert record.dispatched_at is None and record.dispatch_token is None and record.result is None
        assert record.usage.input_tokens is None and record.usage.output_tokens is None
        assert owner.outbox.ai_usage_account_calls(owner.account.account_id, record.binding.budget_date) == 1
        owner.account = owner.account.model_copy(update={"daily_limit": 0})
        owner.provider = None
        closed_cached = owner.generate("alice", body.model_copy(update={"request_id": uuid4()}))
        assert closed_cached.result == first.result and len(calls) == 1
        restarted = domain.AIAssistanceOwner(outbox=PageControlOutbox(owner.outbox.path), account=owner.account, provider=None, contexts=owner.contexts, clock=owner.clock)
        restored = AIAssistanceAdmission(owner=restarted, allowed_users=frozenset({"alice", "other"})).dispatch(AILookupAction(original=another), authenticated_actor_id="alice").request
        assert restored == cached and len(calls) == 1
        with pytest.raises(AIRequestConflict):
            restarted.lookup("alice", another.model_copy(update={"result_sha256": "a" * 64}))
        with pytest.raises(AIRequestNotFound):
            restarted.lookup("other", another)
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


@pytest.mark.parametrize("damage", ["invalid_json", "changed_content", "changed_binding"])
def test_invalid_interpretation_cache_is_rejected_before_any_new_dispatch(
    tmp_path: Path, sealed: PortfolioReadResult, monkeypatch: pytest.MonkeyPatch, damage: str,
) -> None:
    from contextlib import closing
    from rquant.ai_interpretation import ValidatedInterpretation
    owner, body, adapter, calls, _, facts = _interpretation_owner(tmp_path, sealed, monkeypatch)
    try:
        first = owner.generate("alice", body)
        content = first.result.interpretation
        if damage == "invalid_json":
            raw = "bad original cache bytes"
        elif damage == "changed_content":
            payload = content.model_dump(mode="json")
            payload["sections"][0]["paragraphs"][0]["text"] = "changed original content"
            raw = json.dumps(payload)
        else:
            raw = ValidatedInterpretation(
                binding=content.binding.model_copy(update={"model_id": "foreign-model"}),
                sections=content.sections, metrics=content.metrics,
            ).model_dump_json()
        with closing(owner.outbox._connect()) as connection:
            connection.execute("UPDATE ai_interpretation_cache SET content_json=? WHERE owner_uid=? AND cache_key=?", (raw, "alice", content.binding.cache_key))
            connection.commit()
        with pytest.raises(ValueError):
            owner.generate("alice", body.model_copy(update={"request_id": uuid4()}))
        assert len(calls) == 1
        assert owner.outbox.ai_usage_account_calls(owner.account.account_id, owner.clock().date()) == 1
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()


def test_cache_reuse_still_requires_the_current_complete_sealed_reader(
    tmp_path: Path, sealed: PortfolioReadResult, monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner, body, adapter, calls, _, _ = _interpretation_owner(tmp_path, sealed, monkeypatch)
    try:
        first = owner.generate("alice", body)
        cached_body = body.model_copy(update={"request_id": uuid4()})
        assert owner.generate("alice", cached_body).result == first.result
        def reject_changed_seal(actor: str, request: AIInterpretationRequest) -> SealedInterpretationFacts:
            raise ValueError("original complete sealed artifact changed")
        monkeypatch.setattr(owner.contexts, "_interpretation_context", reject_changed_seal)
        with pytest.raises(ValueError, match="complete sealed artifact changed"):
            owner.generate("alice", body.model_copy(update={"request_id": uuid4()}))
        with pytest.raises(ValueError, match="complete sealed artifact changed"):
            owner.lookup("alice", cached_body)
        assert len(calls) == 1
        assert owner.outbox.ai_usage_account_calls(owner.account.account_id, owner.clock().date()) == 1
    finally:
        adapter.close()
        owner.contexts.screen.tracker.close()
