"""Actual bounded HTTP and original source-quota/owner artifacts, using offline bytes."""

from __future__ import annotations

import importlib
import json
from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from rquant.source_quota_store import SourceQuotaStore
from rquant.source_quota_transport import QuotaBoundTransportObserver
from tests.unit.test_stock_news_digest import collection, draft, document

NOW = datetime(2026, 10, 6, 9, tzinfo=UTC)


def module():
    return importlib.import_module("rquant.stock_news_sources")


def observer(tmp_path: Path):
    store = SourceQuotaStore(tmp_path / "original-source-quota.sqlite3", boot_id="synthetic-ai-source", monotonic_ns=lambda: 1000)
    return QuotaBoundTransportObserver(store=store, source="akshare", quota_units_per_window=200,
        window_kind="minute", clock=lambda: NOW)


def test_actual_http_is_quota_dispatched_before_official_finite_request(tmp_path: Path) -> None:
    source = module()
    quota = observer(tmp_path)
    calls = []
    def handle(request):
        calls.append(request)
        attempts = quota.request_attempts("synthetic.original")
        assert len(attempts) == 1 and attempts[0].dispatched_at is not None
        assert str(request.url).startswith("https://np-anotice-stock.eastmoney.com/api/security/ann?")
        assert set(request.extensions["timeout"].values()) == {15.0}
        return httpx.Response(200, json={"success": 1, "data": {"list": [], "total_hits": 0}})
    transport = source.StockNewsHttpTransport(observer=quota, transport=httpx.MockTransport(handle))
    with quota.scope(logical_request_id="synthetic.original", observed_at=NOW):
        assert transport.metadata("https://np-anotice-stock.eastmoney.com/api/security/ann", params={"stock_list": "000001"})["data"]["total_hits"] == 0
    assert len(calls) == 1 and quota.request_attempts("synthetic.original")[0].outcome.value == "success"
    transport.close()


@pytest.mark.parametrize("failure", ["redirect", "oversize", "badhost", "timeout"])
def test_no_retry_redirect_or_unbounded_source_bytes(tmp_path: Path, failure: str) -> None:
    source, quota, calls = module(), observer(tmp_path), []
    def handle(request):
        calls.append(request)
        if failure == "timeout":
            raise httpx.ReadTimeout("offline")
        if failure == "redirect":
            return httpx.Response(302, headers={"Location": "https://example.test/"})
        return httpx.Response(200, content=b" "*(2*1024*1024+1))
    transport = source.StockNewsHttpTransport(observer=quota, transport=httpx.MockTransport(handle))
    with quota.scope(logical_request_id="synthetic.original", observed_at=NOW):
        with pytest.raises((ValueError, RuntimeError)):
            transport.metadata("https://evil.test/" if failure == "badhost" else "https://reportapi.eastmoney.com/report/list")
    assert len(calls) == (0 if failure == "badhost" else 1)
    transport.close()


def test_company_must_be_verified_from_collected_full_body_not_keyword() -> None:
    source = module()
    company = source.StockNewsCompany(stock_code="000001.SZ", company_name="平安银行", source_sha256="a"*64)
    row = {"code": "202610061111", "title": "其他公司的新闻", "date": "2026-10-06 10:00:00", "url": "https://finance.eastmoney.com/a/202610061111.html"}
    with pytest.raises(ValueError):
        source.document_from_original(company=company, source_kind="news", metadata=row,
            body="其他公司公布利润。关键词检索命中000001。", collected_at=NOW)
    row["title"] = "平安银行业绩"
    body = "平安银行披露实际增长12.50%。预计增长15.00%。"
    value = source.document_from_original(company=company, source_kind="news", metadata=row,
        body=body, collected_at=NOW)
    assert value.body == body and value.affiliation_kind == "verified_company"
    assert {fact.nature for fact in value.facts} == {"actual", "forecast"}
    assert all(body[f.body_start:f.body_end] == f.quote for f in value.facts)


def test_article_void_tags_cannot_include_other_companies_after_original_body() -> None:
    raw = '<div id="ContentBody"><p>平安银行实际披露。</p><br><p>第二段事实。</p></div><aside>其他公司盈利。</aside>'
    body = module().extract_news_body(raw.encode())
    assert body == "平安银行实际披露。\n第二段事实。"
    assert "其他公司" not in body


def test_future_calendar_period_is_forecast_not_disclosed_actual() -> None:
    source = module()
    company = source.StockNewsCompany(stock_code="000001.SZ", company_name="平安银行", source_sha256="a"*64)
    row = {"code":"202610061111", "title":"平安银行研究", "date":"2026-10-06 10:00:00", "url":"https://finance.eastmoney.com/a/202610061111.html"}
    value = source.document_from_original(company=company, source_kind="news", metadata=row,
        body="平安银行2025年实际利润12亿元。平安银行2028年每股收益1.6元。", collected_at=NOW)
    assert [fact.nature for fact in value.facts] == ["actual", "forecast"]


def test_finite_collection_does_not_retain_response_hashes_across_nightly_batches(tmp_path: Path) -> None:
    source = module()
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == '/api/security/ann':return httpx.Response(200,json={'success':1,'data':{'list':[],'total_hits':0}})
        if request.url.path == '/report/list':return httpx.Response(200,json={'data':[],'TotalPage':0})
        callback=request.url.params['cb']
        return httpx.Response(200,content=(callback+'({"result":{"cmsArticleWebOld":[]},"hitsTotal":0});').encode())
    transport = source.StockNewsHttpTransport(observer=observer(tmp_path),transport=httpx.MockTransport(handle))
    collector = source.EastmoneyStockNewsCollector(transport,page_limit=1,clock=lambda:NOW)
    company = source.StockNewsCompany(stock_code='000001.SZ',company_name='平安银行',source_sha256='a'*64)
    try:
        for identifier in ('synthetic.hash.first','synthetic.hash.second'):
            result=collector.collect('alice',company=company,start_date=date(2026,10,1),end_date=date(2026,10,6),request_id=identifier)
            assert len(result.transport_receipts)==3 and len(result.response_sha256)==3
            assert transport.response_sha256==[]
    finally:
        transport.close()


def test_original_artifacts_are_private_immutable_and_digest_bound(tmp_path: Path) -> None:
    from rquant.page_control import PageControlOutbox
    from rquant.stock_news_digest import validate_stock_news_digest
    source = module()
    outbox = PageControlOutbox(tmp_path / "owner.sqlite3")
    store = source.StockNewsArtifactStore(tmp_path / "private-news", outbox=outbox)
    facts = collection()
    store.put_facts(facts)
    assert store.read_facts("alice", "000001.SZ", expected_context_sha256=facts.context_sha256) == facts
    with pytest.raises(LookupError):
        store.read_facts("other", "000001.SZ", expected_context_sha256=facts.context_sha256)
    digest = validate_stock_news_digest(facts, draft=draft(facts))
    store.put_digest(facts, digest, model_id="gpt-test", template_version="ai-assistance/v1")
    assert store.read_digest("alice", "000001.SZ").digest == digest
    with pytest.raises(ValueError):
        store.put_digest(facts, validate_stock_news_digest(facts, draft=draft(facts, text="另一摘要 {{doc.one.actual}}。")), model_id="gpt-test", template_version="ai-assistance/v1")
    assert len(tuple((tmp_path / "private-news").iterdir())) == 2


def test_full_pool_watch_union_retains_more_than_one_batch_and_original_version(tmp_path: Path) -> None:
    source = module()
    pool = tuple(f"{600000+i:06d}.SH" for i in range(600))
    scope = source.news_research_scope("alice", pool_members=pool, watchlist_members=(pool[0], "000001.SZ"),
        pool_version="a"*64, watchlist_version="b"*64)
    assert len(scope.stock_codes) == 601
    assert scope == source.news_research_scope("alice", pool_members=pool, watchlist_members=(pool[0], "000001.SZ"), pool_version="a"*64, watchlist_version="b"*64)
    assert scope.pool_watchlist_sha256 != source.news_research_scope("alice", pool_members=pool, watchlist_members=(pool[0], "000001.SZ"), pool_version="c"*64, watchlist_version="b"*64).pool_watchlist_sha256


def original_pdf() -> bytes:
    import io
    from pypdf import PdfWriter
    from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject
    writer = PdfWriter()
    page = writer.add_blank_page(width=612, height=792)
    font = DictionaryObject({NameObject('/Type'): NameObject('/Font'), NameObject('/Subtype'): NameObject('/Type1'), NameObject('/BaseFont'): NameObject('/Helvetica')})
    page[NameObject('/Resources')] = DictionaryObject({NameObject('/Font'): DictionaryObject({NameObject('/F1'): writer._add_object(font)})})
    stream = DecodedStreamObject()
    stream.set_data(b'BT /F1 12 Tf 30 700 Td (SyntheticBank revenue 12.50%. Expected revenue 15.00%.) Tj ET')
    page[NameObject('/Contents')] = writer._add_object(stream)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def test_original_three_source_pages_full_body_and_real_quota_receipts(tmp_path: Path) -> None:
    source, quota, calls = module(), observer(tmp_path), []
    company = source.StockNewsCompany(stock_code='000001.SZ', company_name='平安银行', aliases=('SyntheticBank',), source_sha256='a'*64)
    def handle(request):
        calls.append(str(request.url))
        path = request.url.path
        if path == '/api/security/ann':
            return httpx.Response(200, json={'success':1,'data':{'total_hits':1,'list':[{'art_code':'AN202610060001','codes':[{'stock_code':'000001'}],'title':'平安银行公告','notice_date':'2026-10-06'}]}})
        if path == '/api/content/ann':
            return httpx.Response(200, json={'success':1,'data':{'notice_content':'平安银行披露收入增长12.50%。'}})
        if path == '/search/jsonp':
            callback = request.url.params['cb']
            row = {'code':'202610060002','title':'平安银行新闻','date':'2026-10-06 10:00:00','url':'https://finance.eastmoney.com/a/202610060002.html','content':'这个字段只是摘要'}
            return httpx.Response(200, content=(callback+'('+json.dumps({'result':{'cmsArticleWebOld':[row]},'hitsTotal':1},ensure_ascii=False)+');').encode())
        if path.startswith('/a/'):
            return httpx.Response(200, content='<html><script>不要入库</script><div id="ContentBody"><p>平安银行公布收入增长12.50%。</p><p>预计下期增长15.00%。</p></div><footer>不要入库</footer></html>'.encode())
        if path == '/report/list':
            return httpx.Response(200, json={'hits':1,'TotalPage':1,'data':[{'stockCode':'000001','stockName':'平安银行','infoCode':'AP202610060003','title':'平安银行研究','publishDate':'2026-10-06 00:00:00'}]})
        if path.startswith('/pdf/'):
            return httpx.Response(200, content=original_pdf())
        raise AssertionError(path)
    transport = source.StockNewsHttpTransport(observer=quota, transport=httpx.MockTransport(handle))
    collector = source.EastmoneyStockNewsCollector(transport, page_limit=1, clock=lambda:NOW)
    collected = collector.collect('alice', company=company, start_date=date(2026,10,1), end_date=date(2026,10,6), request_id='synthetic.full-original')
    assert len(collected.facts.documents)==3
    assert all(c.complete for c in collected.facts.coverage)
    assert len(collected.transport_receipts)==len(calls)==6
    assert all(r.dispatched_at <= r.committed_at for r in collected.transport_receipts)
    news = next(d for d in collected.facts.documents if d.source_kind=='news')
    assert '只是摘要' not in news.body and '不要入库' not in news.body
    assert news.published_date==date(2026,10,6) and news.published_at.hour==2 and news.body_date is None
    research = next(d for d in collected.facts.documents if d.source_kind=='research')
    assert research.published_at is None and 'SyntheticBank revenue 12.50%' in research.body
    transport.close()


def test_keyword_first_page_and_failed_body_do_not_claim_complete(tmp_path:Path) -> None:
    source, quota = module(), observer(tmp_path)
    company = source.StockNewsCompany(stock_code='000001.SZ',company_name='平安银行',source_sha256='a'*64)
    def handle(request):
        if request.url.path == '/search/jsonp':
            cb=request.url.params['cb']
            return httpx.Response(200,content=(cb+'('+json.dumps({'result':{'cmsArticleWebOld':[{'code':'one','title':'其他公司','date':'2026-10-06 10:00:00','url':'https://finance.eastmoney.com/a/one.html'}]},'hitsTotal':100})+');').encode())
        if request.url.path.startswith('/a/'):
            return httpx.Response(200,content=b'<div id="ContentBody">Another company.</div>')
        return httpx.Response(403)
    transport=source.StockNewsHttpTransport(observer=quota,transport=httpx.MockTransport(handle))
    collected=source.EastmoneyStockNewsCollector(transport,page_limit=1,clock=lambda:NOW).collect('alice',company=company,start_date=date(2026,10,1),end_date=date(2026,10,6),request_id='synthetic.limited')
    assert not collected.facts.documents
    news=next(c for c in collected.facts.coverage if c.source_kind=='news')
    assert news.status=='available' and news.returned_documents==1 and not news.complete
    assert all(c.returned_documents is None for c in collected.facts.coverage if c.source_kind!='news')
    transport.close()


def scheduling_gate(tmp_path:Path):
    from tests.unit.test_lab_scheduling_control import store_and_port
    from rquant.lab_jobs import LabJobReader
    source=module()
    root=tmp_path/'original-scheduler'
    root.mkdir(mode=0o700)
    store,port=store_and_port(root)
    lease=store.acquire_scheduler_lease(owner_id='original-scheduler',lease_seconds=120,now=NOW)
    store.enable_scheduling_control(lease=lease,barrier_port=port,now=NOW)
    return source.OriginalAiSchedulingGate(LabJobReader(store.path),port.root),store,port,lease


def test_nightly_uses_actual_original_pause_barrier_before_transport(tmp_path:Path) -> None:
    from tests.unit.test_lab_scheduling_control import command
    from datetime import timedelta
    source=module();gate,store,port,lease=scheduling_gate(tmp_path)
    gate()
    port.apply_command(command(store,paused=True,expected_version=0),lease=lease,now=NOW+timedelta(seconds=1))
    calls=[];quota=observer(tmp_path)
    transport=source.StockNewsHttpTransport(observer=quota,transport=httpx.MockTransport(lambda request:calls.append(request)),submission_gate=gate)
    with quota.scope(logical_request_id='synthetic.paused',observed_at=NOW):
        with pytest.raises(source.NewsSchedulingClosed):
            transport.metadata('https://reportapi.eastmoney.com/report/list')
    assert not calls and not quota.request_attempts('synthetic.paused')
    transport.close()


def test_nightly_progress_retains_full_scope_and_crashed_source_uuid_never_resends(tmp_path:Path) -> None:
    from rquant.page_control import PageControlOutbox
    from uuid import uuid4
    source=module();outbox=PageControlOutbox(tmp_path/'original-owner.sqlite3')
    scope=source.news_research_scope('alice',pool_members=tuple(f'{600000+i:06d}.SH' for i in range(600)),watchlist_members=('000001.SZ',),pool_version='a'*64,watchlist_version='b'*64)
    companies=tuple(source.StockNewsCompany(stock_code=code,company_name='公司'+code[:6],source_sha256='c'*64) for code in scope.stock_codes)
    original=source.AINightlyNewsCommand(request_id=uuid4(),scope=scope,companies=companies,start_date=date(2026,10,1),end_date=date(2026,10,6))
    journal=source.AINightlyNewsJournal(outbox)
    journal.reserve(original)
    assert len(journal.progress('alice',original.request_id).pending_codes)==601
    assert journal.claim_source('alice',original.request_id,scope.stock_codes[0])
    reopened=source.AINightlyNewsJournal(outbox)
    assert not reopened.claim_source('alice',original.request_id,scope.stock_codes[0])
    assert reopened.phase('alice',original.request_id,scope.stock_codes[0])=='unknown'
    assert len(reopened.progress('alice',original.request_id).pending_codes)==601
    view = reopened.latest_view('alice')
    assert (view.total,view.completed,view.pending,view.unknown)==(601,0,601,1)
    assert view.scope_sha256==scope.pool_watchlist_sha256
    assert reopened.latest_view('other') is None
    with pytest.raises(LookupError):
        reopened.command('other',original.request_id)


def test_actual_nightly_runner_batches_original_full_union_and_restart_keeps_effects(tmp_path:Path) -> None:
    from tests.unit.test_ai_assistance import _owner
    from uuid import uuid4
    source=module();gate,_,_,_=scheduling_gate(tmp_path)
    owner,_,adapter=_owner(tmp_path,lambda request: (_ for _ in ()).throw(AssertionError('empty facts need no model')))
    owner.clock=lambda:NOW
    owner.contexts.news=source.StockNewsArtifactStore(tmp_path/'private-news',outbox=owner.outbox)
    calls=[]
    def handle(request):
        calls.append(request)
        if request.url.path=='/api/security/ann':return httpx.Response(200,json={'success':1,'data':{'list':[],'total_hits':0}})
        if request.url.path=='/report/list':return httpx.Response(200,json={'data':[],'TotalPage':0})
        cb=request.url.params['cb']
        return httpx.Response(200,content=(cb+'({"result":{"cmsArticleWebOld":[]},"hitsTotal":0});').encode())
    transport=source.StockNewsHttpTransport(observer=observer(tmp_path),transport=httpx.MockTransport(handle))
    collector=source.EastmoneyStockNewsCollector(transport,page_limit=1,clock=lambda:NOW)
    scope=source.news_research_scope('researcher',pool_members=tuple(f'{600000+i:06d}.SH' for i in range(600)),watchlist_members=('000001.SZ',),pool_version='a'*64,watchlist_version='b'*64)
    command=source.AINightlyNewsCommand(request_id=uuid4(),scope=scope,companies=tuple(source.StockNewsCompany(stock_code=code,company_name='公司'+code[:6],source_sha256='c'*64) for code in scope.stock_codes),start_date=date(2026,10,1),end_date=date(2026,10,6))
    runner=source.AINightlyNewsRunner(owner,collector,gate,users=frozenset({'researcher'}),enabled=True,batch_size=2)
    runner.journal.reserve(command)
    first=runner.run_original('researcher',command.request_id)
    assert len(first.completed_codes)==2 and len(first.pending_codes)==599 and len(calls)==6
    second=source.AINightlyNewsRunner(owner,collector,gate,users=frozenset({'researcher'}),enabled=True,batch_size=2).run_original('researcher',command.request_id)
    assert len(second.completed_codes)==4 and len(second.pending_codes)==597 and len(calls)==12
    owner.account=owner.account.model_copy(update={'daily_limit':0})
    assert runner.run_original('researcher',command.request_id)==second and len(calls)==12
    runner.close();adapter.close()


def test_actual_memory_trigger_startup_shutdown_and_same_uuid_recovery(tmp_path: Path) -> None:
    import time
    from uuid import NAMESPACE_URL, uuid5
    from zoneinfo import ZoneInfo
    from tests.support.ai_assistance_fixture import build_ai_integration_fixture
    source = module()
    fixture = build_ai_integration_fixture(tmp_path)
    runner = fixture.install_original_nightly(enabled=True)
    gate = runner.gate
    identifier = uuid5(NAMESPACE_URL, 'rquant:ai-news:researcher:' + fixture.now.astimezone(ZoneInfo('Asia/Shanghai')).date().isoformat())
    command = source.original_news_scope(fixture.owner, 'researcher', identifier)
    assert command.scope.stock_codes == ('600001.SH', '600002.SH', '600003.SH')
    runner.journal.reserve(command)
    try:
        runner.start()
        scheduler = runner.scheduler
        assert scheduler is not None and scheduler.running
        assert scheduler.get_job('ai-news-original-owner') is not None
        deadline = time.monotonic() + 10
        while not runner.journal.progress('researcher', identifier).complete and time.monotonic() < deadline:
            time.sleep(0.01)
        progress = runner.journal.progress('researcher', identifier)
        assert progress.complete and len(progress.completed_codes) == 3 and not progress.pending_codes
        assert len(fixture.source_calls) == 16 and fixture.model.calls == 4
        runner.close()
        assert runner.scheduler is None and not scheduler.running
        count = len(fixture.source_calls), fixture.model.calls
        reopened = source.AINightlyNewsRunner(fixture.owner, fixture.collector, gate, users=frozenset({'researcher'}), enabled=True, batch_size=1)
        fixture.owner.nightly = reopened
        reopened.start()
        try:
            deadline = time.monotonic() + 3
            while reopened.scheduler.get_job('ai-news-original-recovery') is not None and time.monotonic() < deadline:
                time.sleep(0.01)
            # Also exercise the exact restarted command synchronously after startup.
            assert reopened.run_original('researcher', identifier) == progress
            assert (len(fixture.source_calls), fixture.model.calls) == count
        finally:
            reopened.close()
        assert reopened.scheduler is None
    finally:
        runner.close()
        fixture.close()
