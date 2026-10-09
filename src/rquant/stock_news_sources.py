"""Finite original-source collection and private artifacts owned by PageControl."""

from __future__ import annotations

import hashlib
import html
import io
import json
import os
import re
import sqlite3
import stat
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal, TYPE_CHECKING
from threading import RLock
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo
from uuid import UUID, uuid5, NAMESPACE_URL

import httpx
from pydantic import Field, model_validator

from rquant.ai_assistance_contracts import AISealedFact, Sha256
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.source_quota_transport import QuotaBoundTransportObserver, SourceTransportCallReceipt
from rquant.stock_news_digest import (
    StockNewsDocument, NewsSourceFact, StockNewsFacts, StockNewsCoverage,
    StockNewsResearchScope, ValidatedStockNewsDigest, StockCode,
    StockNewsResearchProgress,
)
from rquant.strict_json import strict_json_loads

if TYPE_CHECKING:
    from rquant.page_control import PageControlOutbox, PageControlService
    from rquant.web.models.ai_assistance import AINewsContent
    from rquant.ai_assistance import AIAssistanceOwner
    from rquant.lab_jobs import LabJobReader


def news_content(facts: StockNewsFacts, digest: ValidatedStockNewsDigest) -> 'AINewsContent':
    from rquant.web.models.ai_assistance import AINewsContent, AINewsSource, AINewsCitation
    cited = {identifier for statement in digest.statements for identifier in statement.content.citations}
    return AINewsContent(digest=digest, sources=tuple(AINewsSource(document_id=d.document_id,
        title=d.title, url=d.source_url, provider=d.provider, source_kind=d.source_kind,
        published_date=d.published_date, published_at=d.published_at, body_date=d.body_date,
        first_collected_at=d.first_collected_at) for d in facts.documents), coverage=facts.coverage,
        citations=tuple(AINewsCitation(fact_id=f.fact.fact_id,document_id=d.document_id,nature=f.nature,
            quote=f.quote,body_start=f.body_start,body_end=f.body_end,body_sha256=d.body_sha256,
            source_path=f.fact.source_path,period_end=f.period_end)
            for d in facts.documents for f in d.facts if f.fact.fact_id in cited))

MAX_METADATA_BYTES = 2 * 1024 * 1024
MAX_BODY_BYTES = 16 * 1024 * 1024
MAX_DIGEST_BYTES = 1024 * 1024
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_METADATA_URLS = {
    "https://np-anotice-stock.eastmoney.com/api/security/ann",
    "https://search-api-web.eastmoney.com/search/jsonp",
    "https://reportapi.eastmoney.com/report/list",
    "https://api.tushare.pro",
    "https://np-cnotice-stock.eastmoney.com/api/content/ann",
}
_ARTICLE_HOSTS = {"finance.eastmoney.com", "stock.eastmoney.com", "data.eastmoney.com", "pdf.dfcfw.com"}


class StockNewsHttpTransport:
    def __init__(self, *, observer: QuotaBoundTransportObserver,
                 transport: httpx.BaseTransport | None = None,
                 monotonic: Callable[[], float] = time.monotonic,
                 submission_gate: Callable[[], None] | None = None) -> None:
        if type(observer) is not QuotaBoundTransportObserver:
            raise TypeError("stock sources require the original durable quota observer")
        self.observer, self.monotonic = observer, monotonic
        self.response_sha256: list[str] = []
        self.submission_gate = submission_gate
        self.client = httpx.Client(transport=transport or httpx.HTTPTransport(retries=0),
            timeout=15.0, follow_redirects=False, trust_env=False)

    def _fetch(self, url: str, *, params: Mapping[str, str] | None, payload: dict | None,
               body: bool) -> bytes:
        parts = urlsplit(url)
        if parts.scheme != "https" or parts.username is not None or parts.password is not None or parts.fragment or parts.port not in (None,443):
            raise ValueError("original source URL is not approved")
        if (not body and url not in _METADATA_URLS) or (body and (parts.hostname not in _ARTICLE_HOSTS or not parts.path.startswith(("/a/", "/pdf/", "/report/")))):
            raise ValueError("original source endpoint is not approved")
        limit = MAX_BODY_BYTES if body else MAX_METADATA_BYTES
        def dispatch() -> bytes:
            if self.submission_gate is not None:
                self.submission_gate()
            started = self.monotonic()
            chunks: list[bytes] = []
            size = 0
            with self.client.stream("POST" if payload is not None else "GET", url, params=params, json=payload) as response:
                if response.status_code != 200:
                    raise RuntimeError("original source rejected the request")
                length = response.headers.get("content-length")
                if length is not None and (not length.isdigit() or int(length) > limit):
                    raise ValueError("source response exceeds its byte budget")
                for chunk in response.iter_bytes(chunk_size=64*1024):
                    if self.monotonic()-started > 15:
                        raise RuntimeError("source response exceeded its total deadline")
                    size += len(chunk)
                    if size > limit:
                        raise ValueError("source response exceeds its byte budget")
                    chunks.append(chunk)
                if self.monotonic()-started > 15:
                    raise RuntimeError("source response exceeded its total deadline")
            return b"".join(chunks)
        try:
            if self.submission_gate is not None:
                self.submission_gate()
            raw = self.observer.observe("stock-news:" + parts.hostname + parts.path, dispatch)
            if self.submission_gate is not None:
                self.submission_gate()
            self.response_sha256.append(hashlib.sha256(raw).hexdigest())
            return raw
        except httpx.TransportError as error:
            raise RuntimeError("original source transport failed") from error

    def metadata(self, url: str, *, params: Mapping[str, str] | None = None,
                 payload: dict | None = None, callback: str | None = None) -> dict:
        raw = self._fetch(url, params=params, payload=payload, body=False)
        if callback is not None:
            prefix, text = callback + "(", raw.decode().strip()
            suffix = ");" if text.endswith(");") else ")"
            if not text.startswith(prefix) or not text.endswith(suffix):
                raise ValueError("source JSONP envelope changed")
            raw = text[len(prefix):-len(suffix)].encode()
        value = strict_json_loads(raw)
        if not isinstance(value, dict):
            raise ValueError("source metadata must remain an object")
        return value

    def original_body(self, url: str) -> bytes:
        return self._fetch(url, params=None, payload=None, body=True)

    def close(self) -> None:
        self.client.close()


class StockNewsCompany(RuntimeContractModel):
    stock_code: StockCode
    company_name: str = Field(min_length=2, max_length=128)
    aliases: tuple[str, ...] = Field(default=(), max_length=16)
    source_sha256: Sha256


def _clean_title(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("source title is absent")
    return html.unescape(re.sub(r"<[^>]*>", "", value)).strip()


def _date(value: object) -> date | None:
    if not isinstance(value, str) or len(value)<10:
        return None
    return date.fromisoformat(value[:10])


def _body_date(body: str) -> date | None:
    dates = set()
    for match in re.finditer(r"(?:报告日期|发布日期|日期)\s*[:：]?\s*(20\d{2})[-年/](\d{1,2})[-月/](\d{1,2})日?", body[:2048]):
        dates.add(date(*(int(part) for part in match.groups())))
    return next(iter(dates)) if len(dates)==1 else None


def source_text_facts(body: str, *, document_id: str,
                      reference_date: date | None = None) -> tuple[NewsSourceFact, ...]:
    digest = hashlib.sha256(body.encode()).hexdigest()
    records = []
    for match in re.finditer(r"[^。！？\n]+[。！？]?", body):
        quote = match.group().strip()
        if not quote:
            continue
        if len(records) >= 256:
            raise ValueError("full original fact inventory exceeds its capacity")
        if len(quote.encode())>4096:
            raise ValueError("one original fact exceeds its display capacity")
        start = match.start()+len(match.group())-len(match.group().lstrip())
        future_year = reference_date is not None and any(
            int(year) > reference_date.year
            for year in re.findall(r"(?<!\d)(20\d{2})\s*年", quote)
        )
        nature = "forecast" if future_year or re.search(r"预计|预测|预期|展望|未来|将|目标价|明年|后年|[0-9]{4}[Ee]|forecast|expected|estimate",quote,re.IGNORECASE) else "actual"
        fact_id = "doc." + hashlib.sha256(document_id.encode()).hexdigest()[:16] + "." + str(len(records))
        records.append(NewsSourceFact(fact=AISealedFact(fact_id=fact_id, label="原文", kind="text",
            value=quote, source_path=f"documents.{document_id}.body[{start}:{start+len(quote)}]",
            source_sha256=digest), nature=nature, quote=quote, body_start=start, body_end=start+len(quote)))
    if not records:
        raise ValueError("source body has no original facts")
    return tuple(records)


def document_from_original(*, company: StockNewsCompany, source_kind: Literal["announcement","news","research"],
                           metadata: dict, body: str, collected_at: datetime) -> StockNewsDocument:
    code = company.stock_code[:6]
    names = (company.company_name, *company.aliases)
    if not body.strip() or not any(name and name in body for name in names):
        raise ValueError("original body does not establish company affiliation")
    if source_kind == "announcement":
        if not any(isinstance(row,dict) and row.get("stock_code")==code for row in metadata.get("codes",())):
            raise ValueError("announcement issuer differs from original stock")
        identifier = metadata["art_code"]
        if re.fullmatch(r"AN[0-9]+",identifier) is None:
            raise ValueError("announcement identity is invalid")
        url = "https://data.eastmoney.com/notices/detail/"+code+"/"+identifier+".html"
        published, precise, affiliation = _date(metadata.get("notice_date")), None, "issuer_code"
    elif source_kind == "research":
        if metadata.get("stockCode") != code:
            raise ValueError("research issuer differs from original stock")
        identifier = metadata["infoCode"]
        if re.fullmatch(r"AP[0-9]+",identifier) is None:
            raise ValueError("research identity is invalid")
        url = "https://pdf.dfcfw.com/pdf/H3_"+identifier+"_1.pdf"
        published, precise, affiliation = _date(metadata.get("publishDate")), None, "issuer_code"
    else:
        identifier = metadata["code"]
        url = metadata["url"]
        parts = urlsplit(url)
        if parts.scheme == "http" and parts.hostname in _ARTICLE_HOSTS:
            url = urlunsplit(("https",parts.netloc,parts.path,parts.query,""))
        stamp = metadata.get("date")
        precise = datetime.fromisoformat(stamp).replace(tzinfo=_SHANGHAI).astimezone(UTC) if isinstance(stamp,str) and len(stamp)>10 else None
        published, affiliation = _date(stamp), "verified_company"
    return StockNewsDocument(provider="eastmoney", source_kind=source_kind, document_id=identifier,
        source_url=url,title=_clean_title(metadata["title"]),source_stock_codes=(company.stock_code,),
        affiliation_kind=affiliation, affiliation_source_sha256=canonical_sha256({"company":company,"metadata":metadata,"body":hashlib.sha256(body.encode()).hexdigest()}),
        published_date=published,published_at=precise,body_date=_body_date(body),first_collected_at=collected_at,
        body=body,body_sha256=hashlib.sha256(body.encode()).hexdigest(),facts=source_text_facts(body,
            document_id=identifier, reference_date=published or collected_at.astimezone(_SHANGHAI).date()))


def news_research_scope(owner: str, *, pool_members: Sequence[str], watchlist_members: Sequence[str],
                        pool_version: str, watchlist_version: str) -> StockNewsResearchScope:
    codes = tuple(sorted(set(pool_members)|set(watchlist_members)))
    return StockNewsResearchScope(owner_uid=owner,stock_codes=codes,
        pool_watchlist_sha256=canonical_sha256({"pool_version":pool_version,"watchlist_version":watchlist_version,
            "pool_members":tuple(sorted(set(pool_members))),"watchlist_members":tuple(sorted(set(watchlist_members)))}))


def install_stock_news_tables(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS stock_news_facts (
            owner_uid TEXT NOT NULL, stock_code TEXT NOT NULL, context_sha256 TEXT NOT NULL,
            artifact_sha256 TEXT NOT NULL, collected_at TEXT NOT NULL,
            PRIMARY KEY(owner_uid,stock_code,context_sha256));
        CREATE TABLE IF NOT EXISTS stock_news_head (
            owner_uid TEXT NOT NULL, stock_code TEXT NOT NULL, context_sha256 TEXT NOT NULL,
            collected_at TEXT NOT NULL, PRIMARY KEY(owner_uid,stock_code));
        CREATE TABLE IF NOT EXISTS stock_news_digest (
            owner_uid TEXT NOT NULL, stock_code TEXT NOT NULL, context_sha256 TEXT NOT NULL,
            model_id TEXT NOT NULL, template_version TEXT NOT NULL,
            artifact_sha256 TEXT NOT NULL, filename TEXT NOT NULL,
            content_json TEXT NOT NULL,
            PRIMARY KEY(owner_uid,stock_code,context_sha256,model_id,template_version));
        CREATE TABLE IF NOT EXISTS stock_news_collection (
            logical_request_id TEXT PRIMARY KEY, owner_uid TEXT NOT NULL,
            stock_code TEXT NOT NULL, context_sha256 TEXT NOT NULL,
            evidence_sha256 TEXT NOT NULL, filename TEXT NOT NULL);
    """)


class StockNewsArtifactStore:
    def __init__(self, root: Path, *, outbox: PageControlOutbox) -> None:
        from rquant.page_control import PageControlOutbox, _bind_managed_directory
        if type(outbox) is not PageControlOutbox or not root.is_absolute() or Path(os.path.abspath(root)) != root:
            raise TypeError("news must share the original PageControl owner and exact artifact root")
        self.root,self.outbox=root,outbox
        with closing(_bind_managed_directory(root,create=True)):
            pass
        with closing(outbox._connect()) as connection:
            install_stock_news_tables(connection)
            connection.commit()

    def _read(self,name:str,sha:str,limit:int) -> bytes:
        from rquant.page_control import _bind_managed_directory
        if re.fullmatch(r"[0-9a-f]{64}\.json",name) is None:
            raise ValueError("news artifact name is invalid")
        with closing(_bind_managed_directory(self.root,create=False)) as bound:
            fd=os.open(name,os.O_RDONLY|os.O_CLOEXEC|os.O_NOFOLLOW,dir_fd=bound.descriptor)
            try:
                before=os.fstat(fd)
                if not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode)!=0o600 or before.st_uid!=os.geteuid() or before.st_nlink!=1 or before.st_size>limit:
                    raise ValueError("news artifact authority or byte budget changed")
                parts=[];size=0
                while chunk:=os.read(fd,min(65536,limit+1-size)):
                    size+=len(chunk)
                    if size>limit:raise ValueError("news artifact exceeds its byte budget")
                    parts.append(chunk)
                raw=b"".join(parts)
                after=os.fstat(fd)
                visible=os.stat(name,dir_fd=bound.descriptor,follow_symlinks=False)
                identity=lambda node:(node.st_dev,node.st_ino,node.st_mode,node.st_uid,node.st_nlink,node.st_size,node.st_mtime_ns,node.st_ctime_ns)
                if identity(before)!=identity(after) or identity(after)!=identity(visible) or hashlib.sha256(raw).hexdigest()!=sha:
                    raise ValueError("news artifact original bytes changed")
                bound.verify()
                return raw
            finally:os.close(fd)

    def _put(self,name:str,raw:bytes,limit:int) -> str:
        from rquant.page_control import _bind_managed_directory
        if len(raw)>limit or re.fullmatch(r"[0-9a-f]{64}\.json",name) is None:
            raise ValueError("news artifact exceeds its exact byte budget")
        sha=hashlib.sha256(raw).hexdigest()
        with closing(_bind_managed_directory(self.root,create=False)) as bound:
            try:fd=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_CLOEXEC|os.O_NOFOLLOW,0o600,dir_fd=bound.descriptor)
            except FileExistsError:
                self._read(name,sha,limit)
                return sha
            try:
                remaining=memoryview(raw)
                while remaining:
                    written=os.write(fd,remaining)
                    if written<=0:raise OSError("news artifact write stopped")
                    remaining=remaining[written:]
                os.fsync(fd);bound.verify();os.fsync(bound.descriptor)
            except BaseException:
                os.unlink(name,dir_fd=bound.descriptor)
                raise
            finally:os.close(fd)
        return sha

    def put_facts(self,facts:StockNewsFacts) -> None:
        with self._write_fence(facts.owner_uid):
            self._current_role(facts.owner_uid, write=True)
            value=StockNewsFacts.model_validate(facts.model_dump(mode="python"))
            sha=self._put(value.context_sha256+".json",value.model_dump_json().encode(),MAX_BODY_BYTES)
            collected=max(row.collected_at for row in value.coverage).isoformat()
            with closing(self.outbox._connect()) as connection:
                self._current_role(value.owner_uid, write=True)
                connection.execute("BEGIN IMMEDIATE")
                row=connection.execute("SELECT artifact_sha256 FROM stock_news_facts WHERE owner_uid=? AND stock_code=? AND context_sha256=?",(value.owner_uid,value.stock_code,value.context_sha256)).fetchone()
                if row is not None and row[0]!=sha:raise ValueError("original news facts changed")
                connection.execute("INSERT OR IGNORE INTO stock_news_facts VALUES (?,?,?,?,?)",(value.owner_uid,value.stock_code,value.context_sha256,sha,collected))
                connection.execute("INSERT INTO stock_news_head VALUES (?,?,?,?) ON CONFLICT(owner_uid,stock_code) DO UPDATE SET context_sha256=excluded.context_sha256,collected_at=excluded.collected_at WHERE excluded.collected_at>=stock_news_head.collected_at",(value.owner_uid,value.stock_code,value.context_sha256,collected))
                connection.commit()

    def read_facts(self,owner:str,stock_code:str,*,expected_context_sha256:str|None=None) -> StockNewsFacts:
        self._current_role(owner)
        with closing(self.outbox._connect()) as connection:
            head=connection.execute("SELECT context_sha256 FROM stock_news_head WHERE owner_uid=? AND stock_code=?",(owner,stock_code)).fetchone()
            context=expected_context_sha256 if expected_context_sha256 is not None else None if head is None else head[0]
            row=connection.execute("SELECT artifact_sha256 FROM stock_news_facts WHERE owner_uid=? AND stock_code=? AND context_sha256=?",(owner,stock_code,context)).fetchone()
        if row is None:raise LookupError("original private news facts are unavailable")
        value=StockNewsFacts.model_validate_json(self._read(context+".json",row[0],MAX_BODY_BYTES))
        if (value.owner_uid,value.stock_code,value.context_sha256)!=(owner,stock_code,context):
            raise ValueError("original news owner or context changed")
        self._current_role(owner)
        return value

    def put_collection(self,value:'StockNewsCollection') -> None:
        with self._write_fence(value.facts.owner_uid):
            self._current_role(value.facts.owner_uid, write=True)
            if not value.transport_receipts or len({r.logical_request_id for r in value.transport_receipts})!=1:
                raise ValueError('original source collection has no unique durable transport request')
            proof=StockNewsCollectionProof(owner_uid=value.facts.owner_uid,stock_code=value.facts.stock_code,
                context_sha256=value.facts.context_sha256,transport_receipts=value.transport_receipts,response_sha256=value.response_sha256)
            raw=proof.model_dump_json().encode()
            key=canonical_sha256(proof)
            sha=self._put(key+'.json',raw,MAX_METADATA_BYTES)
            self.put_facts(value.facts)
            identifier=value.transport_receipts[0].logical_request_id
            with closing(self.outbox._connect()) as connection:
                self._current_role(value.facts.owner_uid, write=True)
                connection.execute('BEGIN IMMEDIATE')
                old=connection.execute('SELECT owner_uid,stock_code,context_sha256,evidence_sha256,filename FROM stock_news_collection WHERE logical_request_id=?',(identifier,)).fetchone()
                row=(value.facts.owner_uid,value.facts.stock_code,value.facts.context_sha256,sha,key+'.json')
                if old is not None and tuple(old)!=row:
                    raise ValueError('original collection request effect already differs')
                connection.execute('INSERT OR IGNORE INTO stock_news_collection VALUES (?,?,?,?,?,?)',(identifier,*row))
                connection.commit()

    def read_collection(self,owner:str,identifier:str) -> StockNewsFacts:
        self._current_role(owner)
        with closing(self.outbox._connect()) as connection:
            row=connection.execute('SELECT stock_code,context_sha256,evidence_sha256,filename FROM stock_news_collection WHERE owner_uid=? AND logical_request_id=?',(owner,identifier)).fetchone()
        if row is None:raise LookupError('original collection effect is unavailable')
        proof=StockNewsCollectionProof.model_validate_json(self._read(row[3],row[2],MAX_METADATA_BYTES))
        if (proof.owner_uid,proof.stock_code,proof.context_sha256)!=(owner,row[0],row[1]) or any(r.logical_request_id!=identifier for r in proof.transport_receipts):
            raise ValueError('original transport proof binding differs')
        return self.read_facts(owner,row[0],expected_context_sha256=row[1])

    def put_digest(self,facts:StockNewsFacts,digest:ValidatedStockNewsDigest,*,model_id:str,template_version:str) -> None:
        with self._write_fence(facts.owner_uid):
            self._current_role(facts.owner_uid, write=True)
            if (digest.owner_uid,digest.stock_code,digest.context_sha256)!=(facts.owner_uid,facts.stock_code,facts.context_sha256):
                raise ValueError("news digest differs from its original facts")
            self.read_facts(facts.owner_uid,facts.stock_code,expected_context_sha256=facts.context_sha256)
            content=news_content(facts,digest)
            key=canonical_sha256({"owner":facts.owner_uid,"stock":facts.stock_code,"context":facts.context_sha256,"model":model_id,"template":template_version})
            sha=self._put(key+".json",content.model_dump_json().encode(),MAX_DIGEST_BYTES)
            with closing(self.outbox._connect()) as connection:
                self._current_role(facts.owner_uid, write=True)
                connection.execute("BEGIN IMMEDIATE")
                row=connection.execute("SELECT artifact_sha256 FROM stock_news_digest WHERE owner_uid=? AND stock_code=? AND context_sha256=? AND model_id=? AND template_version=?",(facts.owner_uid,facts.stock_code,facts.context_sha256,model_id,template_version)).fetchone()
                if row is not None and row[0]!=sha:raise ValueError("immutable news digest already differs")
                connection.execute("INSERT OR IGNORE INTO stock_news_digest VALUES (?,?,?,?,?,?,?,?)",(facts.owner_uid,facts.stock_code,facts.context_sha256,model_id,template_version,sha,key+".json",content.model_dump_json()))
                connection.commit()

    def read_digest(self,owner:str,stock_code:str,*,model_id:str|None=None,template_version:str|None=None) -> AINewsContent:
        self._current_role(owner)
        from rquant.web.models.ai_assistance import AINewsContent
        facts=self.read_facts(owner,stock_code)
        with closing(self.outbox._connect()) as connection:
            rows=connection.execute("SELECT * FROM stock_news_digest WHERE owner_uid=? AND stock_code=? AND context_sha256=? AND (? IS NULL OR model_id=?) AND (? IS NULL OR template_version=?) ORDER BY model_id,template_version LIMIT 2",(owner,stock_code,facts.context_sha256,model_id,model_id,template_version,template_version)).fetchall()
        if len(rows)!=1:raise LookupError("an exact private news digest is unavailable")
        value=AINewsContent.model_validate_json(self._read(rows[0]["filename"],rows[0]["artifact_sha256"],MAX_DIGEST_BYTES))
        if (value.digest.owner_uid,value.digest.stock_code,value.digest.context_sha256)!=(owner,stock_code,facts.context_sha256):
            raise ValueError("original digest owner or source changed")
        self._current_role(owner)
        return value

    def _current_role(self, owner: str, *, write: bool = False) -> None:
        authority = self.outbox.collaboration
        if authority is None or authority.mode == "legacy":
            return
        authority.require_outbox_path(self.outbox.path)
        if write:
            authority.require_operation(owner, "POST", "/api/v1/ai/requests")
        else:
            authority.current_role(owner)

    @contextmanager
    def _write_fence(self, owner: str) -> Iterator[None]:
        authority = self.outbox.collaboration
        if authority is None or authority.mode == "legacy":
            yield
            return
        with authority.locked():
            self._current_role(owner, write=True)
            yield


class StockNewsCollection(RuntimeContractModel):
    facts: StockNewsFacts
    transport_receipts: tuple['SourceTransportCallReceipt', ...]
    response_sha256: tuple[Sha256, ...]


class StockNewsCollectionProof(RuntimeContractModel):
    owner_uid: str
    stock_code: StockCode
    context_sha256: Sha256
    transport_receipts: tuple[SourceTransportCallReceipt,...]
    response_sha256: tuple[Sha256,...]


def extract_news_body(raw: bytes) -> str:
    from html.parser import HTMLParser
    class OriginalArticle(HTMLParser):
        void_tags = frozenset({'area','base','br','col','embed','hr','img','input','link','meta','param','source','track','wbr'})
        def __init__(self) -> None:
            super().__init__(convert_charrefs=True)
            self.depth = 0
            self.skip = 0
            self.parts: list[str] = []
        def handle_starttag(self, tag: str, attrs: list[tuple[str,str|None]]) -> None:
            if self.depth:
                if tag not in self.void_tags:
                    self.depth += 1
                if tag in {'script','style','iframe'}:
                    self.skip += 1
                if tag in {'p','br','div'}:
                    self.parts.append('\n')
            elif dict(attrs).get('id') in {'ContentBody','contentBody'}:
                self.depth = 1
        def handle_endtag(self, tag: str) -> None:
            if tag in self.void_tags:
                return
            if self.depth:
                self.depth -= 1
                if tag in {'script','style','iframe'} and self.skip:
                    self.skip -= 1
        def handle_data(self, data: str) -> None:
            if self.depth and not self.skip:
                self.parts.append(data)
    parser=OriginalArticle()
    parser.feed(raw.decode('utf-8',errors='strict'))
    body='\n'.join(part.strip() for part in ''.join(parser.parts).splitlines() if part.strip())
    if not body or len(body.encode())>MAX_BODY_BYTES:
        raise ValueError('complete original article body is unavailable')
    return body


def extract_research_pdf(raw: bytes) -> str:
    from pypdf import PdfReader
    if not 1 <= len(raw) <= MAX_BODY_BYTES or not raw.startswith(b'%PDF-'):
        raise ValueError('original research PDF is unavailable')
    reader=PdfReader(io.BytesIO(raw),strict=True)
    if reader.is_encrypted or not 1 <= len(reader.pages) <= 128:
        raise ValueError('original research PDF exceeds its finite page budget')
    parts:list[str]=[]
    size=0
    for page in reader.pages:
        text=page.extract_text() or ''
        size+=len(text.encode())
        if size>MAX_BODY_BYTES:
            raise ValueError('original PDF text exceeds its byte budget')
        parts.append(text)
    body='\n'.join(parts).strip()
    if not body:
        raise ValueError('original research PDF contains no readable body')
    return body


class EastmoneyStockNewsCollector:
    def __init__(self, transport:StockNewsHttpTransport, *, page_limit:int=1,
                 clock:Callable[[],datetime]) -> None:
        if type(transport) is not StockNewsHttpTransport or transport.observer.source!='akshare' or type(page_limit) is not int or not 1<=page_limit<=16:
            raise ValueError('news must use the original Eastmoney/AKShare quota and finite pages')
        self.transport,self.page_limit,self.clock=transport,page_limit,clock

    def _page(self, kind:str, code:str, start:date, end:date, page:int) -> tuple[list[dict],int|None]:
        if kind=='announcement':
            data=self.transport.metadata('https://np-anotice-stock.eastmoney.com/api/security/ann',params={'sr':'-1','page_size':'100','page_index':str(page),'ann_type':'A','client_source':'web','f_node':'0','s_node':'0','stock_list':code,'begin_time':start.isoformat(),'end_time':end.isoformat()})
            if data.get('success')!=1 or not isinstance(data.get('data'),dict):
                raise RuntimeError('original announcement source is unavailable')
            raw=data['data'];rows,total=raw.get('list'),raw.get('total_hits')
            pages=(total+99)//100 if type(total) is int and total>=0 else None
        elif kind=='research':
            raw=self.transport.metadata('https://reportapi.eastmoney.com/report/list',params={'code':code,'beginTime':start.isoformat(),'endTime':end.isoformat(),'pageSize':'10','pageNo':str(page),'p':str(page),'pageNum':str(page),'pageNumber':str(page)})
            rows=raw.get('data');total=raw.get('TotalPage')
            pages=total if type(total) is int and total>=0 else None
        else:
            callback='rquantOriginalNews'
            param={'uid':'','keyword':code,'type':['cmsArticleWebOld'],'client':'web','clientType':'web','clientVersion':'curr','param':{'cmsArticleWebOld':{'searchScope':'default','sort':'default','pageIndex':page,'pageSize':10,'preTag':'','postTag':''}}}
            raw=self.transport.metadata('https://search-api-web.eastmoney.com/search/jsonp',params={'cb':callback,'param':json.dumps(param,separators=(',',':'))},callback=callback)
            rows=raw.get('result',{}).get('cmsArticleWebOld')
            total=raw.get('hitsTotal')
            if total is None and isinstance(raw.get('result'),dict):
                total=raw['result'].get('hitsTotal')
            pages=(total+9)//10 if type(total) is int and total>=0 else None
        if not isinstance(rows,list) or not all(isinstance(row,dict) for row in rows) or len(rows)>(100 if kind=='announcement' else 10):
            raise ValueError('original source page contract changed')
        return rows,pages

    def _body(self,kind:str,row:dict) -> str:
        if kind=='announcement':
            identifier=row.get('art_code')
            if not isinstance(identifier,str) or re.fullmatch(r'AN[0-9]+',identifier) is None:
                raise ValueError('announcement identity is invalid')
            original=self.transport.metadata('https://np-cnotice-stock.eastmoney.com/api/content/ann',params={'art_code':identifier,'page_index':'1','client_source':'web'})
            body=original.get('data',{}).get('notice_content')
            if original.get('success')!=1 or not isinstance(body,str) or not body.strip():
                raise ValueError('full original announcement body is unavailable')
            return body
        if kind=='research':
            identifier=row.get('infoCode')
            if not isinstance(identifier,str) or re.fullmatch(r'AP[0-9]+',identifier) is None:
                raise ValueError('research identity is invalid')
            return extract_research_pdf(self.transport.original_body('https://pdf.dfcfw.com/pdf/H3_'+identifier+'_1.pdf'))
        raw_url=row.get('url')
        if not isinstance(raw_url,str):
            raise ValueError('original article URL is absent')
        parts=urlsplit(raw_url)
        url=urlunsplit(('https' if parts.scheme=='http' else parts.scheme,parts.netloc,parts.path,parts.query,''))
        return extract_news_body(self.transport.original_body(url))

    def collect(self,owner:str, *, company:StockNewsCompany,start_date:date,end_date:date,
                request_id:str) -> StockNewsCollection:
        self.transport.response_sha256.clear()
        try:
            return self._collect(owner,company=company,start_date=start_date,end_date=end_date,
                request_id=request_id)
        finally:
            self.transport.response_sha256.clear()

    def _collect(self,owner:str, *, company:StockNewsCompany,start_date:date,end_date:date,
                 request_id:str) -> StockNewsCollection:
        collected_at=self.clock()
        if start_date>end_date or end_date>collected_at.astimezone(_SHANGHAI).date():
            raise ValueError('news coverage dates are invalid')
        documents:list[StockNewsDocument]=[];coverage:list[StockNewsCoverage]=[]
        response_start=len(self.transport.response_sha256)
        with self.transport.observer.scope(logical_request_id=request_id,observed_at=collected_at):
            for kind in ('announcement','news','research'):
                requested:list[int]=[];completed:list[int]=[];returned=0;total_pages=None;all_bodies=True
                for page in range(1,self.page_limit+1):
                    requested.append(page)
                    try:
                        rows,pages=self._page(kind,company.stock_code[:6],start_date,end_date,page)
                    except NewsSchedulingClosed:
                        raise
                    except Exception:
                        all_bodies=False
                        break
                    if total_pages is not None and pages!=total_pages:
                        all_bodies=False
                    total_pages=pages;completed.append(page);returned+=len(rows)
                    for row in rows:
                        stamp=_date(row.get('notice_date' if kind=='announcement' else 'publishDate' if kind=='research' else 'date'))
                        if stamp is None or not start_date<=stamp<=end_date:
                            # News searches cover an index, not a server-filtered date interval.
                            continue
                        try:
                            body=self._body(kind,row)
                            doc=document_from_original(company=company,source_kind=kind,metadata=row,body=body,collected_at=collected_at)
                        except NewsSchedulingClosed:
                            raise
                        except Exception:
                            all_bodies=False
                            continue
                        documents.append(doc)
                    if pages is not None and page>=max(1,pages):
                        break
                complete=bool(completed) and all_bodies and requested==completed and total_pages is not None and (total_pages==0 or len(completed)==total_pages)
                coverage.append(StockNewsCoverage(provider='eastmoney',source_kind=kind,start_date=start_date,end_date=end_date,collected_at=collected_at,status='available' if completed else 'unavailable',requested_pages=tuple(requested),collected_pages=tuple(completed),total_pages=total_pages,returned_documents=returned if completed else None,complete=complete))
            receipts=self.transport.observer.current_receipts()
        return StockNewsCollection(facts=StockNewsFacts(owner_uid=owner,stock_code=company.stock_code,documents=tuple(documents),coverage=tuple(coverage)),transport_receipts=receipts,response_sha256=tuple(self.transport.response_sha256[response_start:]))


class NewsSchedulingClosed(RuntimeError):
    pass


class OriginalAiSchedulingGate:
    def __init__(self,reader:'LabJobReader',claim_root:Path) -> None:
        from rquant.lab_jobs import LabJobReader
        if type(reader) is not LabJobReader or not claim_root.is_absolute():
            raise TypeError('nightly calls require the original scheduling reader and barrier')
        self.reader,self.claim_root=reader,claim_root

    def __call__(self) -> None:
        from rquant.lab_scheduling_control import read_scheduling_barrier_at
        from rquant.page_control import _bind_managed_directory
        state=self.reader.scheduling_state()
        if state is None or state.desired_paused or state.applied_paused or state.desired_version!=state.applied_version:
            raise NewsSchedulingClosed('原研究调度已暂停或状态未知。')
        with closing(_bind_managed_directory(self.claim_root,create=False)) as bound:
            node=os.fstat(bound.descriptor)
            marker=read_scheduling_barrier_at(bound.descriptor,expected_root=(node.st_dev,node.st_ino),expected_identity=state.barrier_identity)
            if marker is None or marker.state!='open' or marker.desired_paused or marker.queue_identity!=state.queue_identity or marker.desired_version!=state.desired_version:
                raise NewsSchedulingClosed('原研究调度门禁尚未打开。')
            bound.verify()


class AINightlyNewsCommand(RuntimeContractModel):
    request_id: UUID
    scope: StockNewsResearchScope
    companies: tuple[StockNewsCompany,...]
    start_date: date
    end_date: date

    @model_validator(mode='after')
    def complete_original_input(self) -> 'AINightlyNewsCommand':
        if self.start_date>self.end_date or tuple(c.stock_code for c in self.companies)!=self.scope.stock_codes or len(self.model_dump_json().encode())>MAX_METADATA_BYTES:
            raise ValueError('nightly command must retain the finite complete original union')
        return self


class AINightlyNewsJournal:
    """Per-command effects, without a job store, dispatch queue, leases or worker claims."""
    def __init__(self,outbox:PageControlOutbox) -> None:
        from rquant.page_control import PageControlOutbox
        if type(outbox) is not PageControlOutbox:
            raise TypeError('nightly progress requires the original PageControl journal')
        self.outbox=outbox
        with closing(outbox._connect()) as connection:
            connection.execute('CREATE TABLE IF NOT EXISTS ai_nightly_news (request_id TEXT PRIMARY KEY, owner_uid TEXT NOT NULL, body_sha256 TEXT NOT NULL, command_json TEXT NOT NULL, phases_json TEXT NOT NULL)')
            connection.commit()

    def command(self,owner:str,request_id:UUID) -> AINightlyNewsCommand:
        with closing(self.outbox._connect()) as connection:
            row=connection.execute('SELECT body_sha256,command_json FROM ai_nightly_news WHERE request_id=? AND owner_uid=?',(str(request_id),owner)).fetchone()
        if row is None:
            raise LookupError('原夜间请求不可用。')
        value=AINightlyNewsCommand.model_validate_json(row[1])
        if value.scope.owner_uid!=owner or value.request_id!=request_id or canonical_sha256(value)!=row[0]:
            raise ValueError('nightly original UUID/body/owner differs')
        return value

    def reserve(self,command:AINightlyNewsCommand) -> None:
        value=AINightlyNewsCommand.model_validate(command.model_dump(mode='python'))
        with closing(self.outbox._connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            old=connection.execute('SELECT owner_uid,body_sha256 FROM ai_nightly_news WHERE request_id=?',(str(value.request_id),)).fetchone()
            if old is not None and tuple(old)!=(value.scope.owner_uid,canonical_sha256(value)):
                raise ValueError('nightly original UUID/body already differs')
            connection.execute('INSERT OR IGNORE INTO ai_nightly_news VALUES (?,?,?,?,?)',(str(value.request_id),value.scope.owner_uid,canonical_sha256(value),value.model_dump_json(),json.dumps({code:'pending' for code in value.scope.stock_codes},separators=(',',':'))))
            connection.commit()

    def _phases(self,connection:sqlite3.Connection,owner:str,request_id:UUID) -> dict[str,str]:
        command=self.command(owner,request_id)
        row=connection.execute('SELECT phases_json FROM ai_nightly_news WHERE request_id=? AND owner_uid=?',(str(request_id),owner)).fetchone()
        phases=strict_json_loads(row[0])
        if not isinstance(phases,dict) or set(phases)!=set(command.scope.stock_codes) or any(value not in {'pending','collecting','collected','complete','unknown'} for value in phases.values()):
            raise ValueError('nightly progress differs from the full original scope')
        return phases

    def phase(self,owner:str,request_id:UUID,code:str) -> str:
        with closing(self.outbox._connect()) as connection:
            return self._phases(connection,owner,request_id)[code]

    def claim_source(self,owner:str,request_id:UUID,code:str) -> bool:
        with closing(self.outbox._connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            phases=self._phases(connection,owner,request_id)
            claimed=phases[code]=='pending'
            if claimed:phases[code]='collecting'
            elif phases[code]=='collecting':phases[code]='unknown'
            connection.execute('UPDATE ai_nightly_news SET phases_json=? WHERE request_id=? AND owner_uid=?',(json.dumps(phases,separators=(',',':')),str(request_id),owner))
            connection.commit()
            return claimed

    def settle(self,owner:str,request_id:UUID,code:str,phase:Literal['collected','complete','unknown']) -> None:
        with closing(self.outbox._connect()) as connection:
            connection.execute('BEGIN IMMEDIATE')
            phases=self._phases(connection,owner,request_id)
            if phases[code]=='complete' and phase!='complete':
                raise ValueError('completed nightly effect cannot roll back')
            phases[code]=phase
            connection.execute('UPDATE ai_nightly_news SET phases_json=? WHERE request_id=? AND owner_uid=?',(json.dumps(phases,separators=(',',':')),str(request_id),owner))
            connection.commit()

    def progress(self,owner:str,request_id:UUID) -> StockNewsResearchProgress:
        command=self.command(owner,request_id)
        with closing(self.outbox._connect()) as connection:
            phases=self._phases(connection,owner,request_id)
        return StockNewsResearchProgress(scope=command.scope,completed_codes=tuple(code for code in command.scope.stock_codes if phases[code]=='complete'),pending_codes=tuple(code for code in command.scope.stock_codes if phases[code]!='complete'))

    def latest_view(self, owner:str) -> 'AINewsProgress | None':
        from rquant.web.models.ai_assistance import AINewsProgress
        with closing(self.outbox._connect()) as connection:
            row=connection.execute("SELECT request_id FROM ai_nightly_news WHERE owner_uid=? ORDER BY json_extract(command_json,'$.end_date') DESC, rowid DESC LIMIT 1",(owner,)).fetchone()
            if row is None:return None
            identifier=UUID(row[0])
            command=self.command(owner,identifier)
            phases=self._phases(connection,owner,identifier)
        total=len(phases); completed=sum(phase=='complete' for phase in phases.values())
        return AINewsProgress(request_id=identifier,scope_sha256=command.scope.pool_watchlist_sha256,
            start_date=command.start_date,end_date=command.end_date,total=total,completed=completed,
            pending=total-completed,unknown=sum(phase=='unknown' for phase in phases.values()),complete=completed==total)


def original_news_scope(owner:'AIAssistanceOwner',viewer:str,request_id:UUID, *, days:int=30) -> AINightlyNewsCommand:
    from datetime import timedelta
    from rquant.manual_watchlist import ManualWatchlistRepository,ManualWatchlistScan
    from rquant.web.readers import table_states,stock_names
    from rquant.web.serving import serving_meta
    if type(days) is not int or not 1<=days<=366:
        raise ValueError('nightly source window exceeds its fixed capacity')
    now=owner.clock()
    with owner.contexts.screen.tracker.borrow() as borrowed:
        meta=serving_meta(borrowed,now=now,stale_after=owner.contexts.screen.stale_after,failure=owner.contexts.screen.tracker.failure)
        if borrowed is None or meta.state!='ready':
            raise ValueError('完整池子数据尚未准备。')
        tables=table_states(borrowed.cursor)
        membership=tables.get('pool_membership')
        if membership is None or not membership.available:
            raise ValueError('完整池子成员尚未准备。')
        rows=borrowed.cursor.execute('SELECT pool_name,row_kind,ts_code,status,result_version,trade_date FROM pool_membership ORDER BY pool_name,row_kind,ts_code LIMIT 4609').fetchall()
        if len(rows)>4608 or any(row[3]!='verified' for row in rows) or not any(row[1]=='status' for row in rows):
            raise ValueError('池子成员覆盖尚未完整。')
        pool_codes=tuple(row[2] for row in rows if row[1]=='member')
        with closing(owner.outbox._connect()) as connection:
            if owner.outbox.manual_watchlist_activated_at() is None:
                raise ValueError('盯盘状态尚未准备。')
            watch=ManualWatchlistRepository(connection).scan(ManualWatchlistScan(owner_id=viewer,now=now,limit=500))
            if watch.next_after_ts_code is not None:
                raise ValueError('完整盯盘成员超过原容量。')
        watch_codes=tuple(entry.ts_code for entry in watch.entries if entry.status=='active')
        scope=news_research_scope(viewer,pool_members=pool_codes,watchlist_members=watch_codes,pool_version=canonical_sha256(rows),watchlist_version=canonical_sha256(watch))
        names:dict[str,str]={}
        for start in range(0,len(scope.stock_codes),100):
            names.update(stock_names(borrowed.cursor,tables,scope.stock_codes[start:start+100]))
        if set(names)!=set(scope.stock_codes):
            raise ValueError('公司原始名称尚未完整。')
        companies=tuple(StockNewsCompany(stock_code=code,company_name=names[code],source_sha256=canonical_sha256({'generation':meta.generation_id,'stock_code':code,'company_name':names[code]})) for code in scope.stock_codes)
    end=now.astimezone(_SHANGHAI).date()
    return AINightlyNewsCommand(request_id=request_id,scope=scope,companies=companies,start_date=end-timedelta(days=days-1),end_date=end)


class AINightlyNewsRunner:
    def __init__(self,owner:'AIAssistanceOwner',collector:EastmoneyStockNewsCollector,gate:OriginalAiSchedulingGate, *, users:frozenset[str],batch_size:int=16,enabled:bool=False) -> None:
        from rquant.ai_assistance import AIAssistanceOwner
        if type(owner) is not AIAssistanceOwner or owner.contexts.news is None or type(gate) is not OriginalAiSchedulingGate or type(batch_size) is not int or not 1<=batch_size<=32:
            raise ValueError('nightly trigger requires installed original owners and finite batch')
        self.owner,self.collector,self.gate=owner,collector,gate
        self.users,self.batch_size,self.enabled=users,batch_size,enabled
        self.journal=AINightlyNewsJournal(owner.outbox)
        self.collector.transport.submission_gate=gate
        self.scheduler=None
        self._trigger_lock = RLock()

    def run_original(self,viewer:str,request_id:UUID) -> StockNewsResearchProgress:
        with self._trigger_lock:
            previous = self.collector.transport.submission_gate
            def current_source_gate() -> None:
                self.gate()
                self.owner.require_current_role(viewer, write=True)
            self.collector.transport.submission_gate = current_source_gate
            try:
                return self._run_original(viewer,request_id)
            finally:
                self.collector.transport.submission_gate = previous

    def _run_original(self,viewer:str,request_id:UUID) -> StockNewsResearchProgress:
        from rquant.web.models.ai_assistance import AINewsRequest
        from rquant.stock_news_digest import StockNewsDigestDraft,validate_stock_news_digest
        from rquant.ai_usage import AIBudgetExceeded
        if viewer not in self.users:
            raise PermissionError('nightly owner is not configured')
        self.owner.require_current_role(viewer)
        command=self.journal.command(viewer,request_id)
        if not self.enabled:
            return self.journal.progress(viewer,request_id)
        self.owner.require_current_role(viewer, write=True)
        processed=0
        for company in command.companies:
            self.owner.require_current_role(viewer, write=True)
            phase=self.journal.phase(viewer,request_id,company.stock_code)
            if phase in {'complete','unknown'}:continue
            source_identifier=str(uuid5(request_id,'source:'+company.stock_code))
            if phase=='collecting':
                try:
                    self.owner.contexts.news.read_collection(viewer,source_identifier)
                    self.journal.settle(viewer,request_id,company.stock_code,'collected')
                    phase='collected'
                except LookupError:
                    with self.owner.write_fence(viewer):
                        self.journal.claim_source(viewer,request_id,company.stock_code)
                    continue
            self.gate()
            if not self.owner.capabilities(viewer).can_generate:break
            if phase in {'pending','collecting'}:
                with self.owner.write_fence(viewer):
                    if not self.journal.claim_source(viewer,request_id,company.stock_code):continue
                try:
                    collected=self.collector.collect(viewer,company=company,start_date=command.start_date,end_date=command.end_date,request_id=source_identifier)
                    self.owner.require_current_role(viewer, write=True)
                    self.owner.contexts.news.put_collection(collected)
                except Exception:
                    self.journal.settle(viewer,request_id,company.stock_code,'unknown')
                    break
                self.journal.settle(viewer,request_id,company.stock_code,'collected')
            facts=self.owner.contexts.news.read_facts(viewer,company.stock_code)
            try:
                self.owner.contexts.news.read_digest(viewer,company.stock_code,model_id=self.owner.account.model_id,template_version=self.owner.account.template_version)
            except LookupError:
                if not facts.documents:
                    digest=validate_stock_news_digest(facts,draft=StockNewsDigestDraft(context_sha256=facts.context_sha256))
                    self.owner.contexts.news.put_digest(facts,digest,model_id=self.owner.account.model_id,template_version=self.owner.account.template_version)
                else:
                    body=AINewsRequest(request_id=uuid5(request_id,'model:'+company.stock_code),stock_code=company.stock_code,context_sha256=facts.context_sha256)
                    try:
                        result=self.owner.generate(viewer,body,submission_gate=self.gate)
                    except AIBudgetExceeded:break
                    if result.result is None:
                        self.journal.settle(viewer,request_id,company.stock_code,'unknown')
                        continue
            self.journal.settle(viewer,request_id,company.stock_code,'complete')
            processed+=1
            if processed>=self.batch_size:
                break
        return self.journal.progress(viewer,request_id)

    def trigger(self) -> None:
        with self._trigger_lock:
            self._trigger()

    def _trigger(self) -> None:
        if not self.enabled:return
        day=self.owner.clock().astimezone(_SHANGHAI).date()
        for viewer in sorted(self.users):
            identifier=uuid5(NAMESPACE_URL,'rquant:ai-news:'+viewer+':'+day.isoformat())
            try:
                self.owner.require_current_role(viewer, write=True)
                try:self.journal.command(viewer,identifier)
                except LookupError:
                    original = original_news_scope(self.owner,viewer,identifier)
                    with self.owner.write_fence(viewer):
                        self.journal.reserve(original)
                previous=None
                while True:
                    progress=self.run_original(viewer,identifier)
                    if progress.complete or progress==previous:
                        break
                    previous=progress
            except (ValueError,RuntimeError,PermissionError):
                continue

    def start(self) -> None:
        if not self.enabled:return
        if self.scheduler is not None:raise RuntimeError('nightly trigger already started')
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler=BackgroundScheduler(timezone=_SHANGHAI)
        scheduler.add_job(self.trigger,'cron',hour=23,minute=0,id='ai-news-original-owner',max_instances=1,coalesce=True,misfire_grace_time=3600)
        # Startup reads the same current-day UUID before any context, capacity or dispatch.
        scheduler.add_job(self.trigger,'date',id='ai-news-original-recovery',max_instances=1)
        self.scheduler=scheduler
        scheduler.start()

    def close(self) -> None:
        if self.scheduler is not None:
            self.scheduler.shutdown(wait=True)
            self.scheduler=None
        self.collector.transport.close()


class AINewsRuntimeProfile(RuntimeContractModel):
    enabled: bool = Field(default=False,strict=True)
    quota_path: Path
    quota_units_per_window: int = Field(strict=True,ge=1,le=2**31-1)
    quota_window_kind: Literal['minute','day']
    original_lab_jobs_path: Path
    original_claim_root: Path
    page_limit: int = Field(default=1,strict=True,ge=1,le=16)
    batch_size: int = Field(default=16,strict=True,ge=1,le=32)

    @model_validator(mode='after')
    def exact_original_paths(self) -> 'AINewsRuntimeProfile':
        for path in (self.quota_path,self.original_lab_jobs_path,self.original_claim_root):
            if not path.is_absolute() or Path(os.path.abspath(path))!=path or path.name.startswith('.env'):
                raise ValueError('nightly profile requires exact existing private original paths')
        return self


def install_ai_news_runner(control:'PageControlService',profile_file:Path, *, users:frozenset[str]) -> AINightlyNewsRunner:
    from rquant.page_control import _read_managed_file
    from rquant.source_quota_store import SourceQuotaStore
    from rquant.lab_jobs import LabJobReader
    from rquant.ai_assistance_admission import MAX_REQUEST_BYTES
    for path in (profile_file,):
        node=path.lstat()
        if not stat.S_ISREG(node.st_mode) or stat.S_IMODE(node.st_mode)!=0o600 or node.st_uid!=os.geteuid() or node.st_nlink!=1 or path.name.startswith('.env'):
            raise ValueError('nightly config is not an original-owner private file')
    raw=_read_managed_file(profile_file)
    if len(raw)>MAX_REQUEST_BYTES:raise ValueError('nightly profile exceeds its byte budget')
    profile=AINewsRuntimeProfile.model_validate(strict_json_loads(raw))
    for path in (profile.quota_path,profile.original_lab_jobs_path):
        node=path.lstat()
        if not stat.S_ISREG(node.st_mode) or node.st_uid!=os.geteuid() or stat.S_IMODE(node.st_mode)!=0o600 or node.st_nlink!=1 or path.resolve(strict=True)!=path:
            raise ValueError('nightly quota/result path is not the existing private original ledger')
    owner=control.ai_assistance
    if owner is None or owner.contexts.news is None:
        raise ValueError('nightly source requires the installed original AI and news owner')
    installed=control.consumer.lab_backend
    lab_facade=getattr(installed,'commands',None)
    if lab_facade is None and control.consumer.task_control_backend is not None:
        lab_facade=control.consumer.task_control_backend.lab_facade
    reader=None if lab_facade is None else lab_facade.reader
    if reader is None or reader.path!=profile.original_lab_jobs_path:
        raise ValueError('nightly scheduling path differs from installed original PageControl Lab')
    gate=OriginalAiSchedulingGate(LabJobReader(reader.path),profile.original_claim_root)
    # This is the existing source ledger and original source account, never a second AI ledger.
    quota=QuotaBoundTransportObserver(store=SourceQuotaStore(profile.quota_path),source='akshare',
        quota_units_per_window=profile.quota_units_per_window,window_kind=profile.quota_window_kind,clock=owner.clock)
    collector=EastmoneyStockNewsCollector(StockNewsHttpTransport(observer=quota,submission_gate=gate),page_limit=profile.page_limit,clock=owner.clock)
    runner=AINightlyNewsRunner(owner,collector,gate,users=users,batch_size=profile.batch_size,enabled=profile.enabled)
    owner.nightly=runner
    return runner
