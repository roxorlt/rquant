"""Freeze original paper facts once and submit through the existing Lab facade."""

from __future__ import annotations

import stat
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from rquant.lab_job_center import CommandSubmissionReceipt, LabCommandSubmissionFacade, _preflight_research_plan, _research_parameter
from rquant.lab_job_protocol import LabCommandEnvelope, SubmitJobCommand
from rquant.paper_reconcile import freeze_paper_reconcile
from rquant.paper_research import FrozenPaperResearchInput, PaperResearchAdapterCatalog, NativePaperResearchAdapterCatalog, PaperResearchCatalog, PaperResearchRunParameters
from rquant.paper_research_adapter import paper_research_adapter_registry
from rquant.paper_research_commands import OwnedRunPaperPortfolioResearch, PaperResearchSubmissionReceipt, RunPaperPortfolioResearch
from rquant.paper_research_source import paper_research_code_identity, publish_paper_research_input
from rquant.paper_portfolio_models import PaperPortfolioStateIdentity
from rquant.paper_portfolio_view_source import PaperPortfolioViewSource
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.paper_research_runtime import NativeMinuteForwardState, NativeMinuteForwardViewSource
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.paper_backtest_source import PaperBacktestSourceReader
from rquant.research_catalog import ResearchCatalog
from rquant.research_run_spec import ResearchJobType, ResearchRunParameters, ResearchRunSpec, ResourceClass
from rquant.runtime_contracts import canonical_sha256, normalize_aware_utc
from rquant.storage.duckdb import DuckDBStore
from rquant.strategy_job_adapters import build_adapter_execution_contract

MAX_PAPER_RESEARCH_ADMISSIONS = 4096
PaperResearchViewSource = PaperPortfolioViewSource | NativeMinuteForwardViewSource
PaperResearchState = PaperPortfolioStateStore | NativeMinuteForwardState
PaperResearchIdentity = PaperPortfolioStateIdentity | StrategyAuthoringIdentity


class PaperResearchRunPreparer:
    def __init__(self, *, sources: tuple[PaperResearchViewSource, ...],
                 metadata_store_factory: Callable[[], AbstractContextManager[DuckDBStore]], research_catalog: ResearchCatalog,
                 input_root: Path, lake_root: Path, code_sha: str, clock: Callable[[], datetime],
                 backtest_reader: PaperBacktestSourceReader | None = None) -> None:
        if not 1 <= len(sources) <= 64 or any(type(item) not in {PaperPortfolioViewSource, NativeMinuteForwardViewSource} for item in sources):
            raise TypeError("paper research requires its finite concrete role sources")
        value = input_root.lstat()
        if (not stat.S_ISDIR(value.st_mode) or stat.S_IMODE(value.st_mode) != 0o700
                or input_root.resolve() != input_root or len(code_sha) != 40 or any(item not in "0123456789abcdef" for item in code_sha)):
            raise ValueError("paper research requires its private installed input root and code identity")
        if len({item.runtime.state.configuration.binding.account_id for item in sources}) != len(sources):
            raise ValueError("paper research account sources are not unique")
        self.sources, self.metadata_store_factory, self.research_catalog = sources, metadata_store_factory, research_catalog
        self.input_root, self.lake_root, self.code_sha, self.clock, self.backtest_reader = input_root, lake_root, code_sha, clock, backtest_reader

    def source_for(self, account_id: str, owner_id: str) -> PaperResearchViewSource:
        matches = tuple(item for item in self.sources if (item.runtime.state.configuration.binding.account_id,
                                                         item.runtime.state.configuration.binding.owner_id) == (account_id, owner_id))
        if len(matches) != 1:
            raise PermissionError("paper research account is unknown or belongs to another user")
        return matches[0]

    def catalog_for(self, source: PaperResearchViewSource) -> PaperResearchCatalog:
        state = source.runtime.state
        if self.source_for(state.configuration.binding.account_id, state.configuration.binding.owner_id) is not source:
            raise PermissionError("paper catalog must use its installed original source")
        native = type(source) is NativeMinuteForwardViewSource
        model = NativePaperResearchAdapterCatalog if native else PaperResearchAdapterCatalog
        return model(metadata_identity=state.identity(), configuration=state.configuration,
            source_code_identity=paper_research_code_identity(native=native))

    def prepare(self, request: RunPaperPortfolioResearch, *, owner_id: str, expected_identity: PaperResearchIdentity) -> OwnedRunPaperPortfolioResearch:
        source = self.source_for(request.account_id, owner_id)
        state = source.runtime.state
        state.refresh_configuration()
        configuration = state.configuration
        native = type(source) is NativeMinuteForwardViewSource
        if native:
            state.authorize(owner_id)
        if state.identity() != expected_identity or configuration.fingerprint != request.configuration_fingerprint:
            raise ValueError("paper research current configuration or metadata changed")
        now = normalize_aware_utc(self.clock())
        view = source.read(as_of=now)
        if view.status != "complete" or view.frame is None or view.frame.account is None:
            raise ValueError("缺少当前估值，暂不能运行研究")
        catalog = self.catalog_for(source)
        reconcile, band = None, None
        if request.task_name == "paper_reconcile":
            if native:
                raise ValueError("原生策略直接核对完整财务账本，不使用组合目标对账")
            reconcile = freeze_paper_reconcile(source.runtime.ledger_source_for(source.broker), configuration, as_of=now,
                                               prices={item.code: item.market_price for item in view.frame.account.holdings})
        else:
            from rquant.paper_backtest_source import PaperBacktestSourceReader
            if (not native and type(self.backtest_reader) is not PaperBacktestSourceReader) or source.runtime.calendar is None:
                raise ValueError("缺少同版本封存回测，暂不能计算区间")
            comparison_dates = view.complete_comparison_dates()
            if comparison_dates is None:
                raise ValueError("模拟净值或日收益有缺口，暂不能计算同日区间")
            band = (source.runtime.band_input(job_id=request.backtest_job_id, comparison_dates=comparison_dates, as_of=now)
                if native else self.backtest_reader.band_input(configuration=configuration, job_id=request.backtest_job_id,
                    calendar=source.runtime.calendar, comparison_dates=comparison_dates, as_of=now))
        value = FrozenPaperResearchInput(task_name=request.task_name, catalog=catalog, code_sha=self.code_sha, available_at=now,
                                         reconcile=reconcile, band=band)
        directory = self.input_root/uuid4().hex
        directory.mkdir(mode=0o700)
        with self.metadata_store_factory() as metadata:
            publication = publish_paper_research_input(value, metadata_store=metadata, source_path=directory/"input.duckdb",
                                                       research_catalog=self.research_catalog, lake_root=self.lake_root, now=now)
        parameters = PaperResearchRunParameters.from_input(value, request_id=request.command_id)
        adapter = "paper-reconcile" if request.task_name == "paper_reconcile" else "paper-backtest-band"
        spec = ResearchRunSpec(schema_version=2, job_type=ResearchJobType.STRATEGY_REPLAY,
                               parameters=ResearchRunParameters(strategy_name=request.task_name, start_date=value.dates[0], end_date=value.dates[1],
                                                                arguments=tuple(_research_parameter(name, getattr(parameters, name)) for name in type(parameters).model_fields)),
                               code_sha=self.code_sha, dataset_snapshot=publication.identity,
                               feature_contract=build_adapter_execution_contract(adapter, "1", self.code_sha), execution_costs=configuration.execution_cost_spec,
                               random_seed=20261005, resource_class=ResourceClass.STANDARD, deadline=now+timedelta(hours=4), research_status="exploratory")
        _preflight_research_plan(spec, paper_catalog=catalog)
        if native:
            state.authorize(owner_id)
            if state.refresh_configuration() != configuration or state.identity() != expected_identity:
                raise ValueError("native forward source changed while freezing the original band")
        return OwnedRunPaperPortfolioResearch(**request.model_dump(mode="python"), owner_id=owner_id, metadata_identity=expected_identity,
                                              accepted_at=now, catalog=catalog, spec=spec,
                                              plan_hash=canonical_sha256(paper_research_adapter_registry(catalog).plan(spec)))


class PaperResearchRunBackend:
    def __init__(self, *, preparer: PaperResearchRunPreparer, facade: LabCommandSubmissionFacade) -> None:
        if type(preparer) is not PaperResearchRunPreparer or type(facade) is not LabCommandSubmissionFacade:
            raise TypeError("paper research requires the concrete installed original services")
        self.preparer, self.facade = preparer, facade
        for source in preparer.sources:
            self._table(source.runtime.state)

    def _state(self, account_id: str, owner_id: str, identity: PaperResearchIdentity) -> PaperResearchState:
        state = self.preparer.source_for(account_id, owner_id).runtime.state
        if state.identity() != identity:
            raise ValueError("original paper research metadata identity was replaced")
        return state

    @staticmethod
    def _table(state: PaperResearchState) -> None:
        with state._connection() as connection:
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='paper_research_admissions'").fetchone():
                return
        with state._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS paper_research_admissions(command_id TEXT PRIMARY KEY,owner_id TEXT NOT NULL,request_body TEXT NOT NULL,owned_body TEXT NOT NULL,receipt_body TEXT)")

    def lookup(self, request: RunPaperPortfolioResearch, *, owner_id: str, expected_identity: PaperResearchIdentity) -> OwnedRunPaperPortfolioResearch | None:
        state = self._state(request.account_id, owner_id, expected_identity)
        self._table(state)
        with state._connection() as connection:
            row = connection.execute("SELECT owner_id,request_body,owned_body FROM paper_research_admissions WHERE command_id=?", (request.command_id,)).fetchone()
        if row is None:
            return None
        owned = OwnedRunPaperPortfolioResearch.model_validate_json(row[2])
        if (row[0], RunPaperPortfolioResearch.model_validate_json(row[1]), owned.original(), owned.metadata_identity) != (
                owner_id, request, request, expected_identity):
            raise ValueError("original paper research request or actor differs")
        return owned

    def compile(self, request: RunPaperPortfolioResearch, *, owner_id: str, expected_identity: PaperResearchIdentity) -> OwnedRunPaperPortfolioResearch:
        state = self._state(request.account_id, owner_id, expected_identity)
        self._table(state)
        old = self.lookup(request, owner_id=owner_id, expected_identity=expected_identity)
        if old is not None:
            return old
        with state._connection(write=True) as connection:
            old = self.lookup(request, owner_id=owner_id, expected_identity=expected_identity)
            if old is not None:
                return old
            count = connection.execute("SELECT COUNT(*) FROM paper_research_admissions").fetchone()[0]
            if count >= MAX_PAPER_RESEARCH_ADMISSIONS:
                raise ValueError("paper research admission budget is full")
            owned = self.preparer.prepare(request, owner_id=owner_id, expected_identity=expected_identity)
            connection.execute("INSERT INTO paper_research_admissions VALUES(?,?,?,?,NULL)", (request.command_id, owner_id, request.model_dump_json(), owned.model_dump_json()))
        return owned

    def _envelope(self, command: OwnedRunPaperPortfolioResearch) -> LabCommandEnvelope:
        submit = SubmitJobCommand(job_id=UUID(command.command_id), spec=command.spec, max_attempts=2)
        return LabCommandEnvelope(request_id=self.facade._request_id("paper-research:"+command.owner_id+":"+command.command_id), command=submit)

    def validate(self, command: OwnedRunPaperPortfolioResearch) -> None:
        if self.lookup(command.original(), owner_id=command.owner_id, expected_identity=command.metadata_identity) != command:
            raise ValueError("original paper research accepted plan differs")

    def has_effect(self, command: OwnedRunPaperPortfolioResearch) -> bool:
        self.validate(command)
        return self.facade._existing(self._envelope(command)) is not None or self.facade.reader.get_job(UUID(command.command_id)) is not None

    def _receipt(self, command: OwnedRunPaperPortfolioResearch, body: str) -> PaperResearchSubmissionReceipt:
        receipt = PaperResearchSubmissionReceipt.model_validate_json(body)
        envelope = self._envelope(command)
        if (receipt.command_id, receipt.account_id, receipt.configuration_fingerprint, receipt.task_name,
                receipt.job_id, receipt.lab_request_id, receipt.lab_content_hash, receipt.spec_hash, receipt.accepted_at) != (
                command.command_id, command.account_id, command.configuration_fingerprint, command.task_name,
                UUID(command.command_id), envelope.request_id, envelope.content_hash, command.spec.spec_hash, command.accepted_at):
            raise ValueError("paper stored receipt differs from the original accepted Lab request")
        return receipt

    def submit(self, command: OwnedRunPaperPortfolioResearch) -> dict[str, object]:
        self.validate(command)
        state = self._state(command.account_id, command.owner_id, command.metadata_identity)
        with state._connection() as connection:
            row = connection.execute("SELECT receipt_body FROM paper_research_admissions WHERE command_id=?", (command.command_id,)).fetchone()
        if row[0] is not None:
            return self._receipt(command, row[0]).model_dump(mode="json")
        envelope = self._envelope(command)
        existing = self.facade._existing(envelope)
        if existing is None:
            existing = self.facade.submit_create(envelope.command, interaction_key="paper-research:"+command.owner_id+":"+command.command_id)
        if (not isinstance(existing, CommandSubmissionReceipt) or existing.request_id != envelope.request_id
                or existing.job_id != UUID(command.command_id) or existing.spool.content_hash != envelope.content_hash):
            raise RuntimeError("paper original Lab publication is not confirmed")
        receipt = PaperResearchSubmissionReceipt(command_id=command.command_id, account_id=command.account_id,
                                                 configuration_fingerprint=command.configuration_fingerprint, task_name=command.task_name,
                                                 job_id=UUID(command.command_id), lab_request_id=envelope.request_id,
                                                 lab_content_hash=envelope.content_hash, spec_hash=command.spec.spec_hash,
                                                 accepted_at=command.accepted_at, submitted_at=self.preparer.clock())
        with state._connection(write=True) as connection:
            current = connection.execute("SELECT receipt_body FROM paper_research_admissions WHERE command_id=?", (command.command_id,)).fetchone()[0]
            if current is not None:
                return self._receipt(command, current).model_dump(mode="json")
            connection.execute("UPDATE paper_research_admissions SET receipt_body=? WHERE command_id=?", (receipt.model_dump_json(), command.command_id))
        return receipt.model_dump(mode="json")
