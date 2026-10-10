"""One concrete sourced portfolio admission dependency for the original batch."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING
from pathlib import Path
from contextlib import contextmanager
from collections.abc import Iterator

from rquant.paper_broker import PaperAccountAuthoritySnapshot, PaperBrokerStore
from rquant.backtest.contracts import SSECalendar
from rquant.paper_operator import PaperOperatorControlStore
from rquant.paper_operator_commands import PaperOperatorApplication
from rquant.paper_portfolio_models import PaperTargetMaterials, PaperTargetQuantityAuthority
from rquant.paper_portfolio_source import PaperPortfolioMaterialStore
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.paper_portfolio_target import PaperPortfolioAdmissionError, prepare_paper_target_quantity
from rquant.portfolio.weights import PortfolioCandidate
from rquant.runtime_contracts import normalize_aware_utc
from rquant.signal_contracts import SignalEnvelopeFamily
from rquant.runtime_service_entrypoint import RuntimeServiceManifest

if TYPE_CHECKING:
    from rquant.paper_research_artifact import PaperResearchResultReader
    from rquant.paper_portfolio_exposure_source import PaperPortfolioExposureStore
    from rquant.paper_signal_worker import PaperQuoteSnapshot, PaperSignalQueueStore, QuoteResolver
    from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame, PaperPortfolioLedgerSource
    from rquant.paper_portfolio_reductions import PaperRiskSignalPublisher, PaperRiskReductionPlan, PaperReductionStatus


class PaperPortfolioRuntime:
    def __init__(self, state: PaperPortfolioStateStore, *, operator: PaperOperatorControlStore,
                 materials: PaperPortfolioMaterialStore, producer_commit: str) -> None:
        if type(operator) is not PaperOperatorControlStore or type(materials) is not PaperPortfolioMaterialStore:
            raise TypeError("paper role requires concrete trusted portfolio sources")
        if operator.state is not state or materials.state is not state:
            raise ValueError("paper portfolio dependencies belong to different sources")
        if len(producer_commit) != 40 or any(char not in "0123456789abcdef" for char in producer_commit):
            raise ValueError("paper runtime requires its exact code identity")
        self.state = state
        self.operator = operator
        self.materials = materials
        self.producer_commit = producer_commit
        self._broker_identity: tuple[int, int] | None = None
        self._ledger_source: PaperPortfolioLedgerSource | None = None
        self._risk_publisher: PaperRiskSignalPublisher | None = None
        self._calendar: SSECalendar | None = None
        self.risk_reason: str | None = None
        self._exposure_store: PaperPortfolioExposureStore | None = None
        self._research_results: PaperResearchResultReader | None = None

    @property
    def research_results(self) -> PaperResearchResultReader | None:
        return self._research_results

    @research_results.setter
    def research_results(self, value: PaperResearchResultReader) -> None:
        from rquant.paper_research_artifact import PaperResearchResultReader
        binding = self.state.configuration.binding
        if type(value) is not PaperResearchResultReader or value.backend.preparer.source_for(binding.account_id, binding.owner_id).runtime is not self:
            raise TypeError("paper result publisher requires its concrete original configured role")
        self._research_results = value

    @property
    def exposure_store(self) -> PaperPortfolioExposureStore | None:
        return self._exposure_store

    @exposure_store.setter
    def exposure_store(self, value: PaperPortfolioExposureStore) -> None:
        from rquant.paper_portfolio_exposure_source import PaperPortfolioExposureStore
        if type(value) is not PaperPortfolioExposureStore or value.state is not self.state:
            raise TypeError("paper industry producer requires its concrete original metadata")
        self._exposure_store = value

    @property
    def calendar(self) -> SSECalendar | None:
        return self._calendar

    @calendar.setter
    def calendar(self, value: SSECalendar) -> None:
        if type(value) is not SSECalendar:
            raise TypeError("paper role requires its original concrete open-day calendar")
        value = SSECalendar.model_validate(value.model_dump(mode="python"))
        if len(value.dates) > 50000 or (self._calendar is not None and self._calendar != value):
            raise ValueError("paper role calendar was replaced or exceeds its read budget")
        self._calendar = value

    @property
    def risk_publisher(self) -> PaperRiskSignalPublisher | None:
        return self._risk_publisher

    @risk_publisher.setter
    def risk_publisher(self, value: PaperRiskSignalPublisher | None) -> None:
        from rquant.paper_portfolio_reductions import PaperRiskSignalPublisher

        if value is not None and (type(value) is not PaperRiskSignalPublisher or value.state is not self.state):
            raise TypeError("paper risk route requires its concrete original configured source")
        self._risk_publisher = value

    def require_broker(self, broker: PaperBrokerStore) -> None:
        binding = self.state.configuration.binding
        if (broker.account_id != binding.account_id or broker.cost_policy.cost_spec_id != binding.cost_spec_id
                or (broker.ledger_id is not None and broker.ledger_id != binding.ledger_id)):
            raise PaperPortfolioAdmissionError("账户或成本配置不一致")
        identity = broker.path.lstat()
        source = (identity.st_dev, identity.st_ino)
        if self._broker_identity is not None and self._broker_identity != source:
            raise PaperPortfolioAdmissionError("原模拟账本来源已替换")
        self._broker_identity = source

    def ledger_source_for(self, broker: PaperBrokerStore) -> PaperPortfolioLedgerSource:
        from rquant.paper_portfolio_ledger import PaperPortfolioLedgerSource

        self.require_broker(broker)
        if self._ledger_source is None:
            self._ledger_source = PaperPortfolioLedgerSource(path=broker.path, account_id=broker.account_id, initial_cash=broker.initial_cash,
                                                            cost_policy=broker.cost_policy, ledger_id=broker.ledger_id,
                                                            anchor_path=broker.ledger_anchor_path, anchor_verifier=broker.ledger_anchor_verifier)
        return self._ledger_source

    def account_authority(self, broker: PaperBrokerStore, *, cutoff: datetime,
                          prices: dict[str, Decimal]) -> PaperAccountAuthoritySnapshot:
        self.require_broker(broker)
        original = broker.account_authority_snapshot(as_of=cutoff, market_prices=prices, producer_commit=self.producer_commit)
        if original.snapshot.as_of_time == cutoff:
            return original
        # The original registry keeps the first observation of unchanged finances.
        # A current original read must still match that exact state/revision.
        with self.ledger_source_for(broker).open() as reader:
            connection = reader._connect()
            row = connection.execute("SELECT revision,state_fingerprint,producer_commit FROM paper_account_authority WHERE account_id=?",
                                     (broker.account_id,)).fetchone()
            if row is None or tuple(row) != (original.revision, original.state_fingerprint, original.producer_commit):
                raise PaperPortfolioAdmissionError("原资金观测序号已变化")
            _, head = reader._attestation_head(connection)
            current = reader.account_snapshot(as_of=cutoff, market_prices=prices)
            _, after = reader._attestation_head(connection)
            if head["revision"] != after["revision"] or head["attestation_fingerprint"] != after["attestation_fingerprint"]:
                raise PaperPortfolioAdmissionError("原账本序号已变化")
            return PaperAccountAuthoritySnapshot.model_validate({**original.model_dump(mode="python"), "snapshot": current})

    def plan_risk_reductions(self, broker: PaperBrokerStore, *, decision_at: datetime, trade_date: date,
                             quote_resolver: QuoteResolver, queue: PaperSignalQueueStore | None = None) -> PaperRiskReductionPlan | None:
        from rquant.paper_portfolio_reductions import plan_paper_reductions

        return plan_paper_reductions(self, broker, decision_at=decision_at, trade_date=trade_date, quote_resolver=quote_resolver, queue=queue)

    def reduction_status(self, broker: PaperBrokerStore, queue: PaperSignalQueueStore, *, as_of: datetime,
                         prices: dict[str, Decimal], frame: PaperPortfolioLedgerFrame | None = None) -> PaperReductionStatus:
        from rquant.paper_portfolio_reductions import paper_reduction_status

        return paper_reduction_status(self, broker, queue, as_of=as_of, prices=prices, frame=frame)

    @contextmanager
    def buy_admission(self, *, observed_at: datetime) -> Iterator[PaperOperatorApplication]:
        with self.state._connection(write=True):
            self.state.refresh_configuration()
            with self.operator.buy_admission(observed_at=observed_at) as application:
                yield application

    def apply_operator(self, *, observed_at: datetime) -> PaperOperatorApplication:
        with self.state._connection(write=True):
            self.state.refresh_configuration()
            return self.operator.apply(observed_at=observed_at)

    def prepare(self, broker: PaperBrokerStore, signal: SignalEnvelopeFamily, quote: PaperQuoteSnapshot,
                *, decision_at: datetime) -> PaperTargetQuantityAuthority:
        configuration = self.state.configuration
        self.require_broker(broker)
        cutoff = normalize_aware_utc(decision_at)
        facts = self.materials.read_for(signal, decision_at=cutoff)
        prices = {item.ts_code: item.valuation_price for item in facts.facts if item.valuation_price is not None}
        if prices.get(signal.candidate_id) != quote.context.executable_price:
            raise PaperPortfolioAdmissionError("原估值与执行报价不同步")
        account = self.account_authority(broker, cutoff=cutoff, prices=prices)
        candidates = tuple(PortfolioCandidate(ts_code=item.ts_code, industry_l1=item.industry_l1, rank_score=item.rank_score)
                           for item in facts.facts if item.candidate)
        risk = None
        if configuration.drawdown_rule is not None:
            risk = self.state.observe_nav(account.snapshot.nav, observed_at=cutoff, ledger_revision=account.revision,
                                          source_fingerprint=account.state_fingerprint)
        basis = PaperTargetMaterials(configuration=configuration, signal=signal, account=account, decision_at=cutoff,
                                    candidates=candidates, industry_by_code={item.ts_code: item.industry_l1 for item in facts.facts if item.industry_l1},
                                    ranking_source_fingerprint=facts.fingerprint, ranking_available_at=facts.available_at,
                                    quote_snapshot_id=quote.snapshot_id, quote=quote.context,
                                    quote_event_time=quote.event_time, quote_available_at=quote.available_at, risk_observation=risk)
        return prepare_paper_target_quantity(basis)

    def validate_submission(self, broker: PaperBrokerStore, authority: PaperTargetQuantityAuthority,
                            signal: SignalEnvelopeFamily, quote: PaperQuoteSnapshot, *, decision_at: datetime) -> None:
        if self.prepare(broker, signal, quote, decision_at=decision_at) != authority:
            raise PaperPortfolioAdmissionError("提交前资金、配置或目标已变化")


class PaperPortfolioRuntimeCatalog:
    def __init__(self, runtimes: tuple[PaperPortfolioRuntime, ...]) -> None:
        if type(runtimes) is not tuple or not 1 <= len(runtimes) <= 64 or any(type(item) is not PaperPortfolioRuntime for item in runtimes):
            raise TypeError("paper catalog requires a bounded finite concrete role directory")
        self._runtimes = {item.state.configuration.binding.role_id: item for item in runtimes}
        if len(self._runtimes) != len(runtimes) or len({item.state.configuration.binding.account_id for item in runtimes}) != len(runtimes):
            raise ValueError("paper role/account directory contains duplicate bindings")

    def for_manifest(self, manifest: RuntimeServiceManifest) -> PaperPortfolioRuntime | None:
        item = self._runtimes.get(manifest.service_id)
        if item is None:
            return None
        binding = item.state.configuration.binding
        if (manifest.manifest_fingerprint != binding.manifest_fingerprint
                or manifest.settings.get("account_id") != binding.account_id or manifest.producer_commit != item.producer_commit):
            raise ValueError("paper catalog differs from the original exact manifest")
        path = manifest.settings.get("broker_path")
        if not isinstance(path, str) or not Path(path).is_absolute():
            raise ValueError("paper role lacks its exact trusted broker path")
        return item

    def for_account(self, account_id: str, *, authenticated_actor_id: str) -> PaperPortfolioRuntime:
        matches = [item for item in self._runtimes.values() if item.state.configuration.binding.account_id == account_id]
        if len(matches) != 1 or matches[0].state.configuration.binding.owner_id != authenticated_actor_id:
            raise PermissionError("paper account is unknown or belongs to a different user")
        return matches[0]
