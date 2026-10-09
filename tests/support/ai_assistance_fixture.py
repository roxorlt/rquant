"""Synthetic original owners and producer chain, shared by ASGI and Root browser gates."""

from __future__ import annotations

import json
import os
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import httpx

from rquant.lab_artifact_protocol import LabArtifactCommitSpool, LabFinalizerAuthorityKey
from rquant.lab_artifacts import LabJobArtifactStore
from rquant.lab_job_center import ExperimentLifecycleCoordinator, LabCommandSubmissionFacade
from rquant.lab_job_protocol import LabCommandSpool
from rquant.lab_jobs import LabJobReader, LabJobStore
from rquant.lab_scheduler import LabScheduler
from rquant.lab_shard_protocol import LabClaimSpool, LabReportSpool
from rquant.portfolio_backtest_artifact import PortfolioResultReader
from rquant.portfolio_backtest_source import PortfolioExperimentProtocol, PortfolioSourceData


def private_directory(path: Path) -> Path:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chmod(path, 0o700)
    return path


@dataclass
class PortfolioFoundation:
    commands: LabCommandSubmissionFacade
    reader: LabJobReader
    results: PortfolioResultReader
    scheduler: LabScheduler
    claims: LabClaimSpool
    reports: LabReportSpool
    commits: LabArtifactCommitSpool
    artifacts: LabJobArtifactStore
    key: LabFinalizerAuthorityKey
    metadata_path: Path
    catalog_path: Path
    lake_root: Path
    input_root: Path
    shard_root: Path
    protocol: PortfolioExperimentProtocol
    code_commit: str
    clock: Callable[[], datetime]

    def original_worker_claim_spool(self) -> LabClaimSpool:
        barrier = self.scheduler.scheduling_control
        if barrier is None:
            raise RuntimeError('original synthetic worker has no scheduling barrier')
        return LabClaimSpool(self.claims.root, expected_scheduling_barrier_identity=barrier.identity)

    def seal_with_original_worker(self, job_id: UUID) -> object:
        """Root-only gate: original worker IPC; never called by offline child checks."""
        import threading
        import time
        import tempfile
        import shutil
        from rquant.lab_finalizer import LabFinalizer
        from rquant.lab_worker import LabWorker, build_builtin_shard_runtime_manifest
        ipc_root = Path(tempfile.mkdtemp(prefix='rqai-', dir='/private/tmp'))
        os.chmod(ipc_root, 0o700)
        previous_tempdir, previous_env = tempfile.tempdir, os.environ.get('TMPDIR')
        tempfile.tempdir, os.environ['TMPDIR'] = str(ipc_root), str(ipc_root)
        thread, worker = None, None
        try:
            self.scheduler.run_once()
            worker = LabWorker(worker_id="ai-synthetic-worker", claim_spool=self.original_worker_claim_spool(),
                report_spool=self.reports, artifact_root=self.shard_root,
                verified_code_sha_provider=lambda: self.code_commit,
                shard_runtime_manifest=build_builtin_shard_runtime_manifest(catalog_path=self.metadata_path,
                    forbidden_paths=(), snapshot_root=self.shard_root.parent / "worker-copies",
                    research_lake_root=self.lake_root), heartbeat_interval_seconds=1,
                receipt_timeout_seconds=10, clock=self.clock)
            outcomes, errors = [], []
            def execute() -> None:
                try:
                    outcomes.append(worker.run_once())
                except BaseException as error:
                    errors.append(error)
            thread = threading.Thread(target=execute, name="ai-original-portfolio-worker")
            thread.start()
            deadline = time.monotonic() + 60
            while thread.is_alive() and time.monotonic() < deadline:
                self.scheduler.run_once()
                thread.join(0.02)
            if thread.is_alive():
                raise RuntimeError("original synthetic worker did not settle")
            if errors:
                raise errors[0]
            if len(outcomes) != 1 or outcomes[0].status != "succeeded":
                raise RuntimeError("original synthetic worker failed")
            self.scheduler.run_once()
            finalizer = LabFinalizer(reader=self.reader, shard_artifact_root=self.shard_root,
                artifact_store=self.artifacts, commit_spool=self.commits,
                verified_code_sha_provider=lambda: self.code_commit,
                finalizer_authority_key_provider=lambda: self.key)
            sealed = finalizer.finalize(job_id)
            if sealed.status != "published":
                raise RuntimeError("original synthetic finalizer did not publish")
            self.scheduler.run_once()
            return self.results.read(job_id, expected_result_hash=sealed.complete_result_hash)
        finally:
            cleanup_errors = []
            try:
                if worker is not None:
                    worker.request_stop()
                if thread is not None:
                    thread.join(10)
                if worker is not None:
                    worker.close()
            except BaseException as error:
                cleanup_errors.append(type(error).__name__)
            alive = thread is not None and thread.is_alive()
            remaining_children = 0
            if worker is not None:
                with worker._managed_authority_children_lock:
                    remaining_children = len(worker._managed_authority_children)
            tempfile.tempdir = previous_tempdir
            if previous_env is None:
                os.environ.pop('TMPDIR', None)
            else:
                os.environ['TMPDIR'] = previous_env
            if not alive and not remaining_children and not cleanup_errors:
                shutil.rmtree(ipc_root)
            record = {'job_id': str(job_id), 'original_worker_thread_alive': alive,
                'owned_authority_children_remaining': remaining_children, 'cleanup_errors': cleanup_errors,
                'owned_ipc_root': str(ipc_root), 'mode': '0700', 'owned_ipc_root_removed': not ipc_root.exists(),
                'tempdir_restored': tempfile.tempdir == previous_tempdir, 'environment_restored': os.environ.get('TMPDIR') == previous_env}
            with (self.shard_root.parent / ('worker-cleanup-' + str(job_id) + '-' + str(uuid4()) + '.json')).open('x') as handle:
                json.dump(record, handle, indent=2)
            if alive or remaining_children or cleanup_errors:
                raise RuntimeError('original worker cleanup incomplete; owned IPC root preserved')


def build_portfolio_foundation(root: Path, source: PortfolioSourceData, *, clock: Callable[[], datetime]) -> PortfolioFoundation:
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.experiment_registry import DateRange, ExperimentRegistry
    from rquant.portfolio_backtest_definition import bootstrap_portfolio_definition
    from rquant.runtime_definition_bootstrap import bootstrap_builtin_definitions, plan_builtin_definitions
    from rquant.strategy_evaluators import BuiltinStrategyEvaluatorRegistry
    from rquant.strategy_job_adapters import default_strategy_job_adapter_registry
    from rquant.lab_scheduling_control import LabSchedulingBarrierPort, LabSchedulingMaintenanceScope
    private_directory(root)
    code, now = source.template.producer_commit, clock()
    definitions_root = root / "definitions"
    bootstrap_builtin_definitions(definitions_root, producer_commit=code, registered_at=now,
        available_at=now, expected_plan_id=plan_builtin_definitions(producer_commit=code).plan_id)
    bootstrap_portfolio_definition(definitions_root, producer_commit=code, now=now)
    definitions = ImmutableDefinitionRegistry(definitions_root,
        execution_registry=BuiltinStrategyEvaluatorRegistry(producer_commit=code).trusted_executable_registry())
    experiment_root = private_directory(root / "experiments")
    registry = ExperimentRegistry(experiment_root / "registry.sqlite3", managed_trust_root=experiment_root)
    jobs = LabJobStore(root / "jobs.sqlite3")
    jobs.initialize()
    reader = LabJobReader(jobs.path)
    command_spool = LabCommandSpool(root / "commands")
    claims, reports = LabClaimSpool(root / "claims"), LabReportSpool(root / "reports")
    commits = LabArtifactCommitSpool(root / "commits")
    artifacts = LabJobArtifactStore(root / "sealed-artifacts")
    commands = LabCommandSubmissionFacade(reader=reader, spool=command_spool,
        experiment_registry=registry, definition_registry=definitions, clock=clock)
    key = LabFinalizerAuthorityKey(key_id="ai-synthetic-only", secret=secrets.token_bytes(32))
    maintenance = LabSchedulingMaintenanceScope(report_root=reports.root,
        artifact_commit_root=commits.root, final_artifact_root=artifacts.root)
    scheduling = LabSchedulingBarrierPort(claims.root, store=jobs, maintenance_scope=maintenance)
    scheduler = LabScheduler(store=jobs, spool=command_spool, owner_id="ai-synthetic-scheduler",
        lease_seconds=60, heartbeat_seconds=10, poll_interval_ms=5, report_spool=reports,
        claim_spool=claims, claim_worker_ids=("ai-synthetic-worker",), shard_lease_seconds=120,
        artifact_commit_spool=commits, artifact_store=artifacts,
        adapter_registry=default_strategy_job_adapter_registry(),
        finalizer_authority_key_provider=lambda identity: key if identity == key.key_id else None,
        lifecycle_synchronizer=ExperimentLifecycleCoordinator(commands), scheduling_control=scheduling, clock=clock)
    return PortfolioFoundation(commands=commands, reader=reader,
        results=PortfolioResultReader(reader=reader, artifact_root=artifacts.root), scheduler=scheduler,
        claims=claims, reports=reports, commits=commits, artifacts=artifacts, key=key,
        metadata_path=root / "metadata.duckdb", catalog_path=root / "catalog.duckdb",
        lake_root=root / "lake", input_root=private_directory(root / "prepared-inputs"),
        shard_root=root / "shard-artifacts", protocol=PortfolioExperimentProtocol(
            train_range=DateRange(start_date=date(2025,1,1),end_date=date(2025,6,30)),
            validation_range=DateRange(start_date=date(2025,7,1),end_date=date(2025,12,31)),
            frozen_outer_test_range=DateRange(start_date=date(2026,1,1),end_date=date(2026,6,30))),
        code_commit=code, clock=clock)


class DirectAIGateway:
    def __init__(self, admission: object) -> None:
        self.admission = admission

    def request(self, action: object, *, authenticated_actor_id: str) -> object:
        return self.admission.dispatch(action, authenticated_actor_id=authenticated_actor_id)


@dataclass
class OfflineModelScenario:
    result: dict[str, object] | Exception
    calls: int = 0
    hook: Callable[[], None] | None = None
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        from rquant.web.nl_parser import NlClarificationNeededError
        assert str(request.url) == "https://api.openai.com/v1/chat/completions"
        assert set(request.extensions["timeout"].values()) == {12.0}
        self.calls += 1
        self.requests.append(request)
        if self.hook is not None:
            self.hook()
        if isinstance(self.result, Exception) and not isinstance(self.result, NlClarificationNeededError):
            raise httpx.ReadTimeout("synthetic external timeout", request=request)
        arguments = "invalid synthetic model JSON" if isinstance(self.result, Exception) else json.dumps(self.result)
        body = json.loads(request.content)
        return httpx.Response(200, json={"usage": {"prompt_tokens": 11, "completion_tokens": 4},
            "choices": [{"message": {"tool_calls": [{"type": "function", "function": {
                "name": body["tool_choice"]["function"]["name"], "arguments": arguments}}]}}]})


def original_ai_test_app(serving_root: Path, scenario: OfflineModelScenario | None, *,
                         clock: Callable[[], datetime], replica_path: Path | None = None,
                         primary_path: Path | None = None) -> object:
    """The external HTTP boundary is synthetic; owner, journal and validators are original."""
    from contextlib import asynccontextmanager
    from pydantic import SecretStr
    from rquant.ai_assistance import AIAccountConfig, AIAssistanceContexts, AIAssistanceOwner
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.page_control import PageControlOutbox
    from rquant.screen.query_admission import ScreenQueryExecutor, ScreenQueryPrivateConfig
    from rquant.web.nl_parser import OpenAiScreenPlanParser
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import create_private_test_app, with_test_proxy_identity
    executor = ScreenQueryExecutor(ScreenQueryPrivateConfig(
        socket_path=Path("/private/tmp/offline-ai-fixture-unused.sock"), trusted_web_uid=os.geteuid()+1,
        shared_gid=os.getegid(), allowed_users=frozenset({"researcher", "other"}),
        serving_root=serving_root, replica_path=replica_path, primary_path=primary_path), clock=clock)
    adapter = OpenAiScreenPlanParser(api_key=SecretStr("synthetic-not-a-real-secret"),
        model="configured-model", transport=httpx.MockTransport(scenario)) if scenario is not None else None
    outbox = PageControlOutbox(serving_root.parent / (serving_root.name + "-ai-original.sqlite3"))
    owner = AIAssistanceOwner(outbox=outbox, account=AIAccountConfig(account_id="synthetic-shared",
        model_id="configured-model", daily_limit=20 if scenario is not None else 0),
        provider=adapter, contexts=AIAssistanceContexts(screen=executor), clock=clock)
    settings = with_test_proxy_identity(WebSettings(serving_root=serving_root, stale_after_seconds=600,
        screen_primary_path=primary_path, screen_replica_path=replica_path))
    settings = WebSettings.model_validate(settings.model_dump() | {"ai_users": frozenset({"researcher", "other"})})
    app = create_private_test_app(settings, clock=clock, background=False,
        ai_assistance_gateway=DirectAIGateway(AIAssistanceAdmission(owner=owner,
            allowed_users=frozenset({"researcher", "other"}))) if scenario is not None else None)
    app.state.ai_synthetic_owner = owner
    app.state.ai_synthetic_adapter = adapter
    previous = app.router.lifespan_context
    @asynccontextmanager
    async def lifespan(application: object):
        try:
            async with previous(application):
                yield
        finally:
            executor.tracker.close()
            if adapter is not None:
                adapter.close()
    app.router.lifespan_context = lifespan
    return app


def publish_original_ai_views(owner:object,root:Path, *, sequence:int=1) -> object:
    from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
    from tests.support.web_serving_fixture import build_web_fixture,fixture_built_at
    reader=_ReadonlyPageControlAuditReader(owner.outbox.path)
    with reader.snapshot():
        projections=reader.ai_assistance_projections(fixture_built_at(sequence))
    return build_web_fixture(root,'baseline',sequence=sequence,signal_projections=projections)


@dataclass
class IntegrationModel:
    calls: int = 0
    requests: list[httpx.Request] = field(default_factory=list)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert str(request.url) == 'https://api.openai.com/v1/chat/completions'
        assert set(request.extensions['timeout'].values()) == {12.0}
        self.calls += 1
        self.requests.append(request)
        body = json.loads(request.content)
        system = body['messages'][0]['content']
        tool = body['tool_choice']['function']['name']
        if tool == 'build_screen':
            rules = [{'name': 'not_st', 'args': {}}]
            if '只修改选股条件' in system:
                rules.append({'name': 'circ_mv_lt', 'args': {'threshold_yi': 200.0, 'offset': 0}})
            draft = {'trade_date': '', 'stages': [{'label': '条件', 'rules': rules}]}
            if 'ranking' in body['tools'][0]['function']['parameters']['properties']:
                draft['ranking'] = {'conditions': [{'metric': 'CIRC_MV[0]', 'ascending': True, 'weight': 1}], 'top_n': 1}
        elif tool == 'interpret_result':
            facts = json.loads(system.split('封存事实=', 1)[1])
            fact = next(f for f in facts['facts'] if f['kind'] == 'number')
            text = {'text': '封存结果记录 {{' + fact['fact_id'] + '}}。', 'citations': [fact['fact_id']]}
            draft = {'context_sha256': system.split('context_sha256=', 1)[1].split('\n', 1)[0],
                'sections': [{'key': key, 'paragraphs': [text]} for key in ('overview', 'annual', 'risk', 'suggestions')]}
        elif tool == 'summarize_stock':
            documents = json.loads(system.split('已封存原文事实=', 1)[1])
            statements = []
            for document in documents:
                for fact in document['facts']:
                    identity = fact['fact']['fact_id']
                    statements.append({'nature': fact['nature'], 'content': {'text': '原文记录 {{' + identity + '}}。', 'citations': [identity]}})
            draft = {'context_sha256': system.split('context_sha256=', 1)[1].split('\n', 1)[0], 'statements': statements}
        else:
            raise AssertionError('unexpected original model tool')
        return httpx.Response(200, json={'usage': {'prompt_tokens': 11, 'completion_tokens': 4},
            'choices': [{'message': {'tool_calls': [{'type': 'function', 'function': {'name': tool, 'arguments': json.dumps(draft)}}]}}]})


class DirectScreenGateway:
    def __init__(self, control: object) -> None:
        self.control = control

    def request(self, action: object, *, authenticated_actor_id: str) -> object:
        from rquant.screen.query_admission import dispatch_screen_query_action
        return dispatch_screen_query_action(self.control, authenticated_actor_id=authenticated_actor_id,
            allowed_users=frozenset({'researcher', 'other'}), action=action)


@dataclass
class AIIntegrationFixture:
    root: Path
    app: object
    owner: object
    foundation: PortfolioFoundation
    config: object
    seed_execution_id: str
    model: IntegrationModel
    adapter: object
    collector: object
    source_calls: list[httpx.Request]
    time: list[datetime]
    sequence: int

    @property
    def now(self) -> datetime:
        return self.time[0]

    def publish(self) -> object:
        from rquant.pool_definition_projection import build_pool_definition_rows
        from rquant.serving_page_projection_source import _ReadonlyPageControlAuditReader
        from rquant.serving_read_models import ServingProjectionPayload
        from tests.support.web_serving_fixture import build_web_fixture, fixture_built_at
        self.sequence += 1
        published_at = fixture_built_at(self.sequence)
        self.time[0] = max(self.now, published_at + timedelta(seconds=1))
        reader = _ReadonlyPageControlAuditReader(self.owner.outbox.path)
        with reader.snapshot():
            ai = reader.ai_assistance_projections(published_at)
            mutations = reader.pool_mutations()
        files = {path.stem: json.loads(path.read_text()) for path in (self.root / 'data/user_presets').glob('*.json')}
        pools = build_pool_definition_rows(files, mutations, root_path=str(self.root / 'data/user_presets'))
        membership = tuple({'pool_name': 'user/ai-owned-pool', 'trade_date': self.config.end_date.isoformat(),
            'result_version': 'a' * 64, 'row_kind': kind, 'ts_code': code, 'status': 'verified',
            'entry_trade_date': None, 'entry_close': None, 'entry_result_version': None, 'unknown_reason': None}
            for kind, code in [('status', ''), ('member', '600001.SH'), ('member', '600002.SH')])
        manifest = build_web_fixture(self.root / 'serving', 'panorama', sequence=self.sequence,
            signal_projections=ai + (ServingProjectionPayload(table_name='pool_definition', available_at=published_at, rows=pools),
                ServingProjectionPayload(table_name='pool_membership', available_at=published_at, rows=membership)))
        self.owner.contexts.screen.tracker.refresh()
        if self.app is not None:
            self.app.state.web.tracker.refresh()
        return manifest

    def seal_seed_with_original_worker(self) -> object:
        """Root gate only: prepare, confirm, original worker, finalizer and complete read."""
        from rquant.web.models.ai_assistance import AIBacktestPrepareRequest, AIBacktestConfirmRequest
        prepared = self.owner.backtests.prepare('researcher', AIBacktestPrepareRequest(request_id=uuid4(),
            execution_id=self.seed_execution_id, start_date=self.config.start_date, end_date=self.config.end_date))
        confirmation = self.owner.backtests.confirm('researcher', AIBacktestConfirmRequest(command_id=uuid4(),
            requested_at=self.now, prepared_request_id=prepared.request_id,
            config_sha256=prepared.config_sha256, proof_sha256=prepared.proof_sha256), control=self.owner.control)
        sealed = self.foundation.seal_with_original_worker(confirmation.job_id)
        self.publish()
        return sealed

    def install_original_nightly(self, *, enabled: bool = False) -> object:
        from rquant.lab_jobs import LabJobReader
        from rquant.stock_news_sources import AINightlyNewsRunner, OriginalAiSchedulingGate
        store = self.foundation.scheduler.store
        port = self.foundation.scheduler.scheduling_control
        if port is None:
            raise RuntimeError('original synthetic scheduler has no scheduling barrier')
        lease = store.acquire_scheduler_lease(owner_id='ai-synthetic-scheduler', lease_seconds=120, now=self.now)
        try:
            store.enable_scheduling_control(lease=lease, barrier_port=port, now=self.now)
        finally:
            store.release_scheduler_lease(lease=lease, now=self.now)
        gate = OriginalAiSchedulingGate(LabJobReader(store.path), port.root)
        runner = AINightlyNewsRunner(self.owner, self.collector, gate,
            users=frozenset({'researcher'}), enabled=enabled, batch_size=1)
        self.owner.nightly = runner
        return runner

    def close(self) -> None:
        if self.owner.nightly is not None:
            self.owner.nightly.close()
        self.owner.contexts.screen.tracker.close()
        self.adapter.close()
        self.collector.transport.close()


def build_ai_integration_fixture(root: Path) -> AIIntegrationFixture:
    """Actual original owners and calculators. Only private input and HTTP are synthetic."""
    from pydantic import SecretStr
    from rquant.ai_assistance import AIAccountConfig, AIAssistanceContexts, AIAssistanceOwner
    from rquant.ai_assistance_admission import AIAssistanceAdmission
    from rquant.ai_screen_backtest_source import (AIScreenBacktestPipeline, AIHistoricalScreenSource,
        AIScreenBacktestArtifacts, build_original_portfolio_writer)
    from rquant.llm.schemas import RuleCall
    from rquant.page_control import SaveUserPoolV3, _COMMAND_ADAPTER
    from rquant.manual_watchlist import ManualWatchlistRepository, ManualWatchlistUpsert
    from rquant.source_quota_store import SourceQuotaStore
    from rquant.source_quota_transport import QuotaBoundTransportObserver
    from rquant.stock_news_sources import StockNewsArtifactStore, StockNewsHttpTransport, EastmoneyStockNewsCollector, StockNewsCompany
    from rquant.web.app import create_app
    from rquant.web.models.ai_assistance import AINewsRequest
    from rquant.web.nl_parser import OpenAiScreenPlanParser
    from rquant.web.portfolio_backtest_service import PortfolioWebService
    from rquant.web.settings import WebSettings
    from tests.support.web_proxy_identity import with_test_proxy_identity
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT
    from tests.unit.test_ai_screen_backtest_source import prepared_history_fixture
    private_directory(root)
    screen, base, config, control, history, command, now = prepared_history_fixture(root)
    time = [now]
    clock = lambda: time[0]
    screen.clock = screen.service.clock = control.consumer.clock = clock
    foundation = build_portfolio_foundation(root / 'lab', base, clock=clock)
    pipeline = AIScreenBacktestPipeline(history=history,
        source=AIHistoricalScreenSource(screen=screen, base=base, default_config=config),
        artifacts=AIScreenBacktestArtifacts(root / 'private/prepared'), clock=clock)
    control.consumer.portfolio_backend = build_original_portfolio_writer(pipeline=pipeline, commands=foundation.commands,
        metadata_path=foundation.metadata_path, catalog_path=foundation.catalog_path, lake_root=foundation.lake_root,
        input_root=foundation.input_root, protocol=foundation.protocol, code_commit=foundation.code_commit, clock=clock)
    model = IntegrationModel()
    adapter = OpenAiScreenPlanParser(api_key=SecretStr('synthetic-not-a-real-secret'), model='configured-model', transport=httpx.MockTransport(model))
    news = StockNewsArtifactStore(root / 'private/news', outbox=control.outbox)
    owner = AIAssistanceOwner(outbox=control.outbox, account=AIAccountConfig(account_id='synthetic-shared',
        model_id='configured-model', daily_limit=20), provider=adapter, contexts=AIAssistanceContexts(screen=screen,
        portfolio=foundation.results, portfolio_owner=control.outbox.authorize_ai_portfolio_result, news=news), backtests=pipeline, clock=clock)
    owner.control = control
    control.ai_assistance = owner
    pool = SaveUserPoolV3(command_id=str(uuid4()), requested_at=now, base_name='ai-owned-pool', display_name='研究样本池',
        description='原始研究条件', rule_calls=(RuleCall(name='not_st', args={}),), include_columns=('CLOSE[0]',),
        depends_on='n-shape-pool1', delay_days=2,
        ranking={'conditions': [{'metric': 'CIRC_MV[0]', 'ascending': True, 'weight': 1}], 'top_n': 2})
    receipt = control.submit(pool)
    if receipt.status.value != 'succeeded':
        raise RuntimeError('synthetic original pool seed failed: ' + str(receipt.error))
    control.outbox.activate_manual_watchlist(now)
    with control.outbox._connect() as connection:
        connection.execute('BEGIN IMMEDIATE')
        ManualWatchlistRepository(connection).upsert(ManualWatchlistUpsert(owner_id='researcher', ts_code='600003.SH', source='detail'), now=now)
    calls: list[httpx.Request] = []
    def source_http(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        if request.url.path == '/api/security/ann':
            return httpx.Response(200, json={'success': 1, 'data': {'list': [], 'total_hits': 0}})
        if request.url.path == '/report/list':
            return httpx.Response(200, json={'data': [], 'TotalPage': 0})
        if request.url.path == '/search/jsonp':
            cb = request.url.params['cb']
            search = json.loads(request.url.params['param'])
            code = search['keyword']
            name = {'600001': '样本01', '600002': '样本02', '600003': '样本03'}[code]
            article = {'code': 'synthetic-' + code, 'title': name + '披露与预测', 'date': clock().date().isoformat() + ' 00:00:00',
                'url': 'https://finance.eastmoney.com/a/' + code + '.html'}
            return httpx.Response(200, content=(cb + '(' + json.dumps({'result': {'cmsArticleWebOld': [article]}, 'hitsTotal': 1}) + ');').encode())
        if request.url.path.startswith('/a/'):
            code = request.url.path.rsplit('/', 1)[-1][:6]
            name = {'600001': '样本01', '600002': '样本02', '600003': '样本03'}[code]
            return httpx.Response(200, content=('<div id="ContentBody">' + name + '披露收入增长12.50%。预计下期增长15.00%。</div>').encode())
        raise AssertionError('unexpected original stock news URL')
    quota = QuotaBoundTransportObserver(store=SourceQuotaStore(root / 'private/source-quota.sqlite3',
        boot_id='synthetic-ai-gate', monotonic_ns=lambda: 1000), source='akshare', quota_units_per_window=200, window_kind='minute', clock=clock)
    collector = EastmoneyStockNewsCollector(StockNewsHttpTransport(observer=quota, transport=httpx.MockTransport(source_http)), page_limit=1, clock=clock)
    collected = collector.collect('researcher', company=StockNewsCompany(stock_code='600001.SH', company_name='样本01', source_sha256='c' * 64),
        start_date=clock().date() - timedelta(days=29), end_date=clock().date(), request_id='synthetic-original-news')
    news.put_collection(collected)
    generated = owner.generate('researcher', AINewsRequest(request_id=uuid4(), stock_code='600001.SH', context_sha256=collected.facts.context_sha256))
    if generated.result is None:
        raise RuntimeError('synthetic original news digest failed: ' + str(generated))
    fixture = AIIntegrationFixture(root, None, owner, foundation, config, command.command_id, model, adapter, collector, calls,
        time, int((now - FIXTURE_BUILT_AT).total_seconds() // 60))
    # The executor was built for a missing generation; repoint its original tracker explicitly.
    from rquant.web.serving import GenerationTracker
    screen.tracker.close()
    screen.tracker = GenerationTracker(root / 'serving')
    fixture.publish()
    settings = with_test_proxy_identity(WebSettings(serving_root=root / 'serving', stale_after_seconds=600,
        screen_primary_path=root / 'rquant.duckdb', screen_replica_path=root / 'rquant_ro.duckdb'))
    settings = WebSettings.model_validate(settings.model_dump() | {'ai_users': frozenset({'researcher', 'other'}),
        'screen_query_users': frozenset({'researcher', 'other'}), 'screen_query_socket_path': Path('/private/tmp/ai-screen-unused.sock'),
        'screen_query_service_uid': os.geteuid() + 1, 'screen_query_shared_gid': os.getegid()})
    def submit_pool(payload: dict[str, object]) -> dict[str, object]:
        receipt = control.submit(_COMMAND_ADAPTER.validate_python(payload))
        fixture.publish()
        return receipt.model_dump(mode='json')
    fixture.app = create_app(settings, clock=clock, background=False,
        ai_assistance_gateway=DirectAIGateway(AIAssistanceAdmission(owner=owner, allowed_users=frozenset({'researcher', 'other'}))),
        screen_query_client=DirectScreenGateway(control), pool_command_transport=submit_pool,
        portfolio_backtests=PortfolioWebService(reader=foundation.reader, results=foundation.results))
    from contextlib import asynccontextmanager
    previous = fixture.app.router.lifespan_context
    @asynccontextmanager
    async def lifespan(application: object):
        try:
            async with previous(application):
                yield
        finally:
            fixture.close()
    fixture.app.router.lifespan_context = lifespan
    return fixture
