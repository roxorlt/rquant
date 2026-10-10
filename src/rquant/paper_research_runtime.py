"""Finite private account metadata selects immutable versions for the original Lab."""

from __future__ import annotations

from typing import TYPE_CHECKING
from contextlib import contextmanager
from collections.abc import Iterator
import sqlite3
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo
from typing import Literal
from uuid import UUID

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.paper_portfolio_ledger import PaperPortfolioLedgerFrame
from rquant.paper_portfolio_views import PaperDailyNav

from rquant.paper_portfolio_models import PaperPortfolioStateIdentity
from rquant.paper_portfolio_state import PaperPortfolioStateStore
from rquant.paper_research import PAPER_RESEARCH_TASKS, NativePaperResearchAdapterCatalog, PaperResearchAdapterCatalog, PaperResearchCatalog, PaperResearchRunParameters
from rquant.paper_research_adapter import paper_research_adapter_registry
from rquant.paper_research_source import paper_research_code_identity
from rquant.research_run_spec import ResearchRunSpec
from rquant.strategy_job_adapters import StrategyJobAdapterRegistry
from rquant.strict_json import strict_canonical_json_loads
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_promotion_contracts import NativeMinuteConfiguration, NativeMinuteForwardConfiguration, NativeMinuteForwardValuation
from rquant.experiment_registry import PromotionStage

if TYPE_CHECKING:
    from rquant.lab_worker import LabShardRuntimeManifest
    from rquant.strategy_promotion import StrategyPromotionPageControlBackend
    from rquant.strategy_authoring import StrategyAuthoringStore
    from rquant.paper_broker import PaperBrokerStore
    from rquant.paper_signal_worker import PaperSignalQueueStore
    from rquant.strategy_runner import StrategyRunnerStore
    from rquant.runtime_paper_quote import PaperPitQuoteResolver
    from rquant.runtime_market_session import MarketCalendarAuthority
    from rquant.runtime_service_entrypoint import RuntimeServiceManifest
    from rquant.paper_research_artifact import PaperResearchResultReader
    from rquant.paper_portfolio_ledger import PaperPortfolioLedgerSource
    from rquant.paper_portfolio_band import NativePaperBacktestBandInput

_SHANGHAI = ZoneInfo("Asia/Shanghai")


class NativeMinuteForwardState:
    """Forward configuration is an adjunct of the original manual strategy owner."""

    def __init__(self, owner: StrategyPromotionPageControlBackend,
                 configuration: NativeMinuteForwardConfiguration, *,
                 expected_configuration_fingerprint: str | None = None) -> None:
        from rquant.strategy_promotion import StrategyPromotionPageControlBackend

        if type(owner) is not StrategyPromotionPageControlBackend:
            raise TypeError("native forward requires its installed original promotion owner")
        self.owner = owner
        self.store = owner.domain.store
        self.configuration = NativeMinuteForwardConfiguration.model_validate_json(configuration.model_dump_json())
        self._identity = self.configuration.metadata_identity
        if self.store.identity() != self._identity:
            raise ValueError("native forward metadata identity was replaced")
        self.refresh_approval()
        with self.store._connection(expected_identity=self._identity) as connection:
            exists = connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='native_forward_current'").fetchone()
            current = connection.execute("SELECT body FROM native_forward_current WHERE account_id=? AND owner_id=?",
                (configuration.binding.account_id, configuration.binding.owner_id)).fetchone() if exists else None
        if current is not None and NativeMinuteForwardConfiguration.model_validate_json(current[0]) == self.configuration:
            self.authorize(configuration.target.owner_id, read=True)
            return
        self.authorize(configuration.target.owner_id)
        with self._connection(write=True) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS native_forward_current(account_id TEXT NOT NULL,owner_id TEXT NOT NULL,body TEXT NOT NULL,PRIMARY KEY(account_id,owner_id))")
            connection.execute("CREATE TABLE IF NOT EXISTS native_forward_configurations(fingerprint TEXT PRIMARY KEY,account_id TEXT NOT NULL,owner_id TEXT NOT NULL,version INTEGER NOT NULL,body TEXT NOT NULL,UNIQUE(account_id,owner_id,version))")
            row = connection.execute("SELECT body FROM native_forward_current WHERE account_id=? AND owner_id=?",
                (configuration.binding.account_id, configuration.binding.owner_id)).fetchone()
            previous = NativeMinuteForwardConfiguration.model_validate_json(row[0]) if row else None
            if previous is not None and previous == self.configuration:
                return
            if ((previous is None and (configuration.version != 1 or expected_configuration_fingerprint is not None))
                or (previous is not None and (expected_configuration_fingerprint != previous.fingerprint
                    or configuration.version != previous.version + 1))):
                raise ValueError("native forward configuration predecessor or version changed")
            if previous is None and connection.execute("SELECT COUNT(*) FROM native_forward_current").fetchone()[0] >= 64:
                raise ValueError("native forward account directory exceeds its original 64-source budget")
            if connection.execute("SELECT COUNT(*) FROM native_forward_configurations WHERE account_id=? AND owner_id=?",
                (configuration.binding.account_id, configuration.binding.owner_id)).fetchone()[0] >= 4096:
                raise ValueError("native forward configuration version budget is full")
            connection.execute("INSERT INTO native_forward_configurations VALUES(?,?,?,?,?)", (
                configuration.fingerprint, configuration.binding.account_id, configuration.binding.owner_id,
                configuration.version, configuration.model_dump_json()))
            connection.execute("INSERT INTO native_forward_current VALUES(?,?,?) ON CONFLICT(account_id,owner_id) DO UPDATE SET body=excluded.body",
                (configuration.binding.account_id, configuration.binding.owner_id, configuration.model_dump_json()))

    def identity(self) -> StrategyAuthoringIdentity:
        value = self.store.identity()
        if value != self._identity:
            raise ValueError("native forward original metadata identity changed")
        return value

    def authorize(self, actor_id: str, *, read: bool = False) -> None:
        self.owner.domain._authorize(actor_id, self.configuration.target, read=read)
        if not read and actor_id not in self.owner.operator_users:
            raise PermissionError("native forward actor is not an installed original operator")

    def refresh_approval(self) -> None:
        from rquant.experiment_platform import ExperimentPlatformStore
        from rquant.runtime_contracts import normalize_aware_utc

        self.identity()
        configuration = self.configuration
        domain = self.owner.domain
        self.authorize(configuration.target.owner_id, read=True)
        domain._builtin(configuration.target)
        state = self.store.promotion_state(configuration.target, verify_builtin=domain._builtin)
        approvals = self.store.promotion_approvals(owner_id=configuration.target.owner_id)
        paper = next((item for item in approvals if item.approval_id == configuration.paper_approval_hash), None)
        latest = next((item for item in approvals if item.approval_id == state.latest_approval_hash), None)
        if (state.stage not in {PromotionStage.PAPER_CANDIDATE, PromotionStage.MONITOR_APPROVED}
            or (state.paper_approval_hash, state.paper_approved_at) !=
                (configuration.paper_approval_hash, configuration.paper_approved_at)
            or paper is None or paper.after.stage is not PromotionStage.PAPER_CANDIDATE
            or paper.review.target != configuration.target or latest is None or latest.after != state
            or normalize_aware_utc(domain.roles.clock()) < configuration.configured_at):
            raise ValueError("native forward lacks its exact original human paper approval or stage")
        platform = getattr(domain.source, "platform", None)
        if type(platform) is not ExperimentPlatformStore:
            raise TypeError("native forward requires its original private family owner")
        family = platform.get_family(configuration.target.owner_id, paper.review.selection.family_id)
        if not any(isinstance(value, NativeMinuteConfiguration) and
            (value.selection.target, value.selection.source_key, value.selection.source_version, value.selection.profile_hash) ==
            (configuration.target, configuration.source_key, configuration.source_version, configuration.execution_profile.profile_hash)
            for value in family.actual_configurations):
            raise ValueError("native forward profile differs from its original fixed native family")

    def refresh_configuration(self) -> NativeMinuteForwardConfiguration:
        self.refresh_approval()
        with self._connection() as connection:
            row = connection.execute("SELECT body FROM native_forward_current WHERE account_id=? AND owner_id=?",
                (self.configuration.binding.account_id, self.configuration.binding.owner_id)).fetchone()
        if row is None or NativeMinuteForwardConfiguration.model_validate_json(row[0]) != self.configuration:
            raise ValueError("native forward current configuration changed")
        return self.configuration

    def configuration_at(self, fingerprint: str, *, version: int) -> NativeMinuteForwardConfiguration:
        with self._connection() as connection:
            row = connection.execute("SELECT body FROM native_forward_configurations WHERE fingerprint=? AND account_id=? AND owner_id=? AND version=?",
                (fingerprint, self.configuration.binding.account_id, self.configuration.binding.owner_id, version)).fetchone()
        if row is None:
            raise ValueError("native forward original configuration version is absent")
        value = NativeMinuteForwardConfiguration.model_validate_json(row[0])
        if value.fingerprint != fingerprint or value.version != version or value.metadata_identity != self.identity():
            raise ValueError("native forward original configuration identity differs")
        return value

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        with self.owner.domain.roles.locked():
            self.authorize(self.configuration.target.owner_id, read=not write)
            with self.store._connection(write=write, expected_identity=self._identity) as connection:
                if write:
                    self.owner.domain._builtin(self.configuration.target)
                    before = self.store._promotion_state_in(connection, self.configuration.target)
                    if (before.paper_approval_hash, before.paper_approved_at) != (
                        self.configuration.paper_approval_hash, self.configuration.paper_approved_at):
                        raise ValueError("native forward human paper approval changed before publication")
                yield connection
                if write:
                    self.owner.domain._builtin(self.configuration.target)
                    after = self.store._promotion_state_in(connection, self.configuration.target)
                    if after != before:
                        raise ValueError("native forward human stage changed during publication")


class NativeMinuteForwardView(RuntimeContractModel):
    status: Literal["complete", "unavailable"]
    as_of: AwareUtcDatetime
    frame: PaperPortfolioLedgerFrame | None
    nav: tuple[PaperDailyNav, ...]
    message: str | None = None

    def complete_comparison_dates(self) -> tuple[date, ...] | None:
        if not self.nav or any(point.status != "complete" or point.daily_return is None for point in self.nav):
            return None
        return tuple(point.trade_date for point in self.nav)


class NativeMinuteForwardRuntime:
    """Read the installed native runner and original broker; do not replay history."""

    def __init__(self, *, state: NativeMinuteForwardState, broker: PaperBrokerStore,
                 queue: PaperSignalQueueStore, runner: StrategyRunnerStore,
                 quote: PaperPitQuoteResolver, calendar: MarketCalendarAuthority,
                 manifest: RuntimeServiceManifest) -> None:
        from rquant.backtest.contracts import SSECalendar
        from rquant.paper_broker import PaperBrokerStore
        from rquant.paper_signal_worker import PaperSignalQueueStore
        from rquant.strategy_runner import StrategyRunnerStore
        from rquant.runtime_paper_quote import PaperPitQuoteResolver
        from rquant.runtime_market_session import MarketCalendarAuthority
        from rquant.runtime_service_entrypoint import RuntimeServiceManifest
        from rquant.runtime_builder_strategy import StrategyLiveRuntimeSettings
        from rquant.paper_portfolio_ledger import PaperPortfolioLedgerSource

        if (type(state) is not NativeMinuteForwardState or type(broker) is not PaperBrokerStore
            or type(queue) is not PaperSignalQueueStore or type(runner) is not StrategyRunnerStore
            or type(quote) is not PaperPitQuoteResolver or type(calendar) is not MarketCalendarAuthority
            or type(manifest) is not RuntimeServiceManifest):
            raise TypeError("native forward requires its concrete original owner and installed execution peers")
        configuration = state.refresh_configuration()
        profile, target, binding = configuration.execution_profile, configuration.target, configuration.binding
        calendar = MarketCalendarAuthority.model_validate(calendar.model_dump(mode="python"))
        settings = StrategyLiveRuntimeSettings.model_validate(dict(manifest.settings))
        if (manifest.manifest_fingerprint != binding.manifest_fingerprint
            or manifest.service_id != binding.role_id or manifest.producer_commit != runner.spec.producer_commit
            or (settings.runner_state_path, settings.paper_broker_path, settings.paper_account_id,
                settings.candidate_max_age_seconds)
            != (runner.path, broker.path, binding.account_id, profile.candidate_max_age_seconds)
            or (runner.spec.strategy_id, runner.spec.version, runner.spec.spec_fingerprint, runner.spec.parameter_fingerprint)
            != (target.strategy_id, target.head.version, target.head.spec_fingerprint, target.parameter_fingerprint)
            or queue.policy != profile.paper_policy
            or (broker.account_id, broker.initial_cash, broker.cost_policy.execution_cost_spec)
            != (binding.account_id, profile.initial_cash, profile.execution_costs)
            or calendar.producer_commit != runner.spec.producer_commit
            or tuple(quote._sse_open_dates) != calendar.open_dates
            or (quote.config.expected_producer_commit, quote.config.timestamp_semantics,
                quote.config.quote_max_age_seconds, quote.config.max_finalize_scan_batches, quote.config.max_visible_scan_batches)
            != (runner.spec.producer_commit, profile.timestamp_semantics, profile.quote_max_age_seconds,
                profile.max_finalize_scan_batches, profile.max_visible_scan_batches)):
            raise ValueError("native forward installed manifest/definition/profile/queue/ledger/calendar differs")
        if not calendar.open_dates:
            raise ValueError("native forward requires its complete original open-day calendar")
        self.state, self.broker, self.queue, self.runner, self.quote = state, broker, queue, runner, quote
        self.market_calendar, self.manifest = calendar, manifest
        self.calendar = SSECalendar(source_identity=calendar.content_sha256,
            coverage_start=calendar.open_dates[0], coverage_end=calendar.open_dates[-1], dates=calendar.open_dates)
        self.producer_commit = runner.spec.producer_commit
        self.research_results: PaperResearchResultReader | None = None
        self._peer_identities = tuple((peer.path.lstat().st_dev, peer.path.lstat().st_ino)
            for peer in (broker, queue, runner))
        self._ledger = PaperPortfolioLedgerSource(path=broker.path, account_id=broker.account_id,
            initial_cash=broker.initial_cash, cost_policy=broker.cost_policy, ledger_id=broker.ledger_id,
            anchor_path=broker.ledger_anchor_path, anchor_verifier=broker.ledger_anchor_verifier)
        # The full original financial read proves the actual generation, not an account label.
        frame = self._ledger.read(configuration=configuration, as_of=state.owner.domain.roles.clock(), prices={}, allow_missing_prices=True)
        if any(item.intent.available_at < configuration.paper_approved_at for item in frame.history):
            raise ValueError("native forward financial history precedes its human paper approval")

    def require_original_peers(self) -> None:
        self.state.refresh_configuration()
        current = tuple((peer.path.lstat().st_dev, peer.path.lstat().st_ino)
            for peer in (self.broker, self.queue, self.runner))
        if current != self._peer_identities:
            raise ValueError("native forward original execution peer was replaced")

    def ledger_source_for(self, broker: PaperBrokerStore) -> PaperPortfolioLedgerSource:
        if broker is not self.broker:
            raise ValueError("native forward requires the same original financial broker")
        self.require_original_peers()
        return self._ledger

    def daily_valuation(self, observed_at: datetime) -> NativeMinuteForwardValuation:
        from rquant.live_spool import LiveBatchSpool
        from rquant.live_contracts import LiveChannel
        from rquant.minute_backtest_contracts import MinuteReplayDailyPriceProof
        from rquant.paper_execution_constraints import PaperExecutionConstraintUnavailableError
        from rquant.runtime_paper_quote import PaperQuoteResolutionError
        from rquant.strategy_runner import StrategyRunnerStore

        self.require_original_peers()
        configuration = self.state.configuration
        observed = normalize_aware_utc(observed_at)
        local = observed.astimezone(_SHANGHAI)
        cutoff = normalize_aware_utc(datetime.combine(local.date(), time(15), tzinfo=_SHANGHAI))
        actual = normalize_aware_utc(self.state.owner.domain.roles.clock())
        if (observed < cutoff or actual < observed or actual > cutoff + timedelta(hours=6)
            or actual.astimezone(_SHANGHAI).date() != local.date() or local.date() not in self.calendar.dates
            or local.date() <= configuration.paper_approved_at.astimezone(_SHANGHAI).date()
            or cutoff < configuration.configured_at):
            raise ValueError("native forward close requires its actual post-approval original observation clock")
        before = self._ledger.read(configuration=configuration, as_of=cutoff, prices={}, allow_missing_prices=True)
        raw = LiveBatchSpool(self.quote.config.raw_spool_root, source_read_only=True)
        original_pointer = raw.current(LiveChannel.MARKET_MINUTE)
        prices, proofs, unavailable = {}, [], []
        with self._ledger.open() as broker:
            connection = broker._connect()
            held = connection.execute("SELECT ts_code,MIN(entry_signal_id) FROM paper_lot WHERE account_id=? "
                "AND remaining_quantity>0 GROUP BY ts_code ORDER BY ts_code", (broker.account_id,)).fetchall()
            if len(held) > 500:
                raise ValueError("native forward daily holding proof exceeds the original 500-code budget")
            with sqlite3.connect(f"{self.runner.path.as_uri()}?mode=ro", uri=True) as original:
                original.row_factory = sqlite3.Row
                metadata = original.execute("SELECT strategy_spec_fingerprint,evaluator_contract_fingerprint "
                    "FROM runner_metadata WHERE singleton=1").fetchone()
                if metadata is None or tuple(metadata) != (self.runner.spec.spec_fingerprint, self.runner.evaluator_contract_fingerprint):
                    raise ValueError("native forward runner physical definition was replaced")
                for code, entry in held:
                    row = original.execute("SELECT * FROM runner_signal WHERE signal_id=?", (entry,)).fetchone()
                    if row is None:
                        unavailable.append(f"{code}:missing_original_entry_signal")
                        continue
                    signal = StrategyRunnerStore._runner_signal_from_row(row)
                    if (signal.candidate_id != code or signal.available_at > cutoff
                        or signal.available_at < configuration.paper_approved_at or
                        (signal.strategy_id, signal.strategy_version, signal.parameter_fingerprint)
                        != (configuration.target.strategy_id, str(configuration.target.head.version), configuration.target.parameter_fingerprint)):
                        raise ValueError("native forward holding is detached from its exact original native entry")
                    try:
                        quote = self.quote.resolve(signal, observed_at=cutoff)
                    except (PaperQuoteResolutionError, PaperExecutionConstraintUnavailableError) as error:
                        unavailable.append(f"{code}:{type(error).__name__}:{error}")
                    else:
                        proofs.append(MinuteReplayDailyPriceProof(entry_signal_id=signal.signal_id, quote=quote))
                        prices[code] = quote.context.executable_price
            pointer, envelope = original_pointer, None
            try:
                envelope, payload = self.quote._latest_visible_batch(cutoff)
                self.quote._validated_frame(envelope, payload)
                if (pointer is None or pointer.sequence != envelope.sequence or pointer.published_at > cutoff
                    or envelope.available_at < cutoff - timedelta(seconds=configuration.execution_profile.quote_max_age_seconds)):
                    raise ValueError("original_market_close_missing")
            except (PaperQuoteResolutionError, ValueError) as error:
                unavailable.append(f"original_market_close:{error}")
                pointer, envelope = None, None
        frame = self._ledger.read(configuration=configuration, as_of=cutoff, prices=prices, allow_missing_prices=True)
        if (frame.ledger_revision, frame.head_fingerprint) != (before.ledger_revision, before.head_fingerprint):
            raise ValueError("native forward full financial ledger changed during close observation")
        if raw.current(LiveChannel.MARKET_MINUTE) != original_pointer:
            raise ValueError("native forward original market publication changed during close observation")
        if frame.missing_valuation_codes:
            unavailable.append("original_complete_holding_prices_missing")
        account = None if unavailable else frame.account
        self.require_original_peers()
        return NativeMinuteForwardValuation(input_hash=configuration.fingerprint, trade_date=local.date(),
            as_of=cutoff, observed_at=observed, profile_hash=configuration.execution_profile.profile_hash,
            calendar_sha256=self.quote.config.trade_calendar_sha256, status="unavailable" if unavailable else "complete",
            market_pointer=pointer, market_envelope=envelope, price_proofs=tuple(proofs), account=account,
            unavailable_reasons=tuple(unavailable))

    def band_input(self, *, job_id: UUID, comparison_dates: tuple[date, ...], as_of: datetime) -> NativePaperBacktestBandInput:
        from rquant.minute_backtest_artifact import MinuteSealedReplayReader
        from rquant.minute_backtest_formal import PreparedMinuteRequest
        from rquant.experiment_platform_evidence import read_native_preparation_result, result_from_native
        from rquant.paper_portfolio_band import NativePaperBacktestBandInput, NativeSealedPaperBacktestReturns, SealedPaperDailyReturn
        from uuid import UUID

        if type(job_id) is not UUID:
            raise TypeError("native band requires its exact original backtest UUID")
        configuration = self.state.refresh_configuration()
        source = self.state.owner.domain.source
        if type(source.native_results) is not MinuteSealedReplayReader:
            raise ValueError("同版本原生分钟封存读取尚未安装")
        with source.context_read(as_of) as context:
            snapshot = context.snapshot
            if snapshot is None or configuration.target.owner_id in snapshot.truncated_owners:
                raise ValueError("native band cannot use an incomplete original private snapshot")
            facts = tuple(fact for fact in snapshot.attempts if fact.child.job_id == job_id and fact.owner == configuration.target.owner_id)
            if len(facts) != 1:
                raise PermissionError("native band backtest is not in its exact original private family")
            fact = facts[0]
            families = tuple(family for family in snapshot.families if (family.owner, family.family_id)
                == (configuration.target.owner_id, fact.family_id))
            if len(families) != 1:
                raise ValueError("native band original private family is unavailable")
            family = families[0]
            if not isinstance(fact.configuration, NativeMinuteConfiguration) or (
                fact.configuration.selection.target, fact.configuration.selection.source_key,
                fact.configuration.selection.source_version, fact.configuration.selection.profile_hash) != (
                configuration.target, configuration.source_key, configuration.source_version,
                configuration.execution_profile.profile_hash):
                raise ValueError("native band differs from its fixed original owner/head/source/profile")
            job = source.projection.jobs.get_job(job_id)
            if job is None:
                raise ValueError("native band original Lab job is unavailable")
            original = context.authorize(job, configuration.target.owner_id).prepared
            if type(original) is not PreparedMinuteRequest:
                raise TypeError("native band requires its complete original prepared source")
            sealed = read_native_preparation_result(source.native_results, original, job_id=job_id,
                configuration=fact.configuration, as_of=as_of)
            if sealed is None:
                raise ValueError("同版本原生分钟结果尚未完整封存")
            result_from_native(fact, family, sealed, original)
            replay = sealed.result.replay
            previous, returns = replay.execution_profile.initial_cash, []
            for point in replay.daily_valuations:
                if point.status != "complete" or point.account is None:
                    raise ValueError("native band cannot consume an incomplete daily valuation")
                returns.append(SealedPaperDailyReturn(trade_date=point.trade_date, daily_return=point.account.nav / previous - 1))
                previous = point.account.nav
            native = original.frozen.native_registration
            backtest = NativeSealedPaperBacktestReturns(job_id=job_id, owner_id=configuration.target.owner_id,
                strategy_id=native.logical_id, strategy_version=str(native.version),
                parameter_fingerprint=native.spec.parameter_fingerprint, cost_spec_id=replay.execution_profile.execution_costs.cost_spec_id,
                calendar_source_identity=original.frozen.runtime.market_calendar.content_sha256,
                definition_fingerprint=native.fingerprint, definition_record_hash=native.record_hash,
                native_spec_fingerprint=native.spec.spec_fingerprint, profile_hash=replay.execution_profile.profile_hash,
                full_input_hash=sealed.full_input_hash, source_kind=sealed.source_kind,
                spec_hash=sealed.spec_hash, manifest_hash=sealed.manifest_hash, complete_result_hash=sealed.complete_result_hash,
                backtest_content_hash=sealed.result_hash, returns=tuple(returns))
            value = NativePaperBacktestBandInput(configuration=configuration, backtest=backtest,
                calendar=self.calendar, comparison_dates=comparison_dates)
        self.require_original_peers()
        return value


class NativeMinuteForwardViewSource:
    def __init__(self, runtime: NativeMinuteForwardRuntime) -> None:
        from rquant.paper_portfolio_views import PaperPortfolioViewStore
        if type(runtime) is not NativeMinuteForwardRuntime:
            raise TypeError("native forward view requires its concrete original runtime")
        runtime.require_original_peers()
        self.runtime, self.broker, self.queue = runtime, runtime.broker, runtime.queue
        self.views = PaperPortfolioViewStore(runtime.state)

    @property
    def research_results(self) -> PaperResearchResultReader | None:
        return self.runtime.research_results

    @research_results.setter
    def research_results(self, value: PaperResearchResultReader) -> None:
        from rquant.paper_research_artifact import PaperResearchResultReader
        if type(value) is not PaperResearchResultReader or value.backend.preparer.source_for(
            self.runtime.state.configuration.binding.account_id, self.runtime.state.configuration.target.owner_id) is not self:
            raise TypeError("native band results require the same original Lab and forward source")
        self.runtime.research_results = value

    def record_close(self, *, observed_at: datetime, published_at: datetime) -> PaperDailyNav:
        from rquant.paper_portfolio_views import NativePaperCloseMaterials, PaperClosePrice
        runtime = self.runtime
        valuation = runtime.daily_valuation(observed_at)
        configuration = runtime.state.refresh_configuration()
        material = NativePaperCloseMaterials(configuration=configuration, calendar=runtime.calendar,
            trade_date=valuation.trade_date, close_at=valuation.as_of, available_at=valuation.observed_at,
            valuation=valuation, trade_calendar_sha256=runtime.quote.config.trade_calendar_sha256,
            prices=tuple(PaperClosePrice(ts_code=proof.quote.ts_code, close_price=proof.quote.context.executable_price,
                source_snapshot_id=proof.quote.snapshot_id, available_at=proof.quote.available_at, observed_at=valuation.as_of,
                trading_status="normal") for proof in valuation.price_proofs))
        return self.views.record_close(runtime.ledger_source_for(self.broker), material, published_at=published_at)

    def read(self, *, as_of: datetime) -> NativeMinuteForwardView:
        from rquant.paper_portfolio_views import NativePaperCloseMaterials
        runtime = self.runtime
        runtime.require_original_peers()
        cutoff = normalize_aware_utc(as_of)
        nav = self.views.nav_series()
        local = cutoff.astimezone(_SHANGHAI)
        day = local.date() if local.time().replace(tzinfo=None) >= time(15) else local.date() - timedelta(days=1)
        with runtime.state._connection() as connection:
            row = connection.execute("SELECT body FROM portfolio_close_material WHERE configuration=? AND trade_date<=? ORDER BY trade_date DESC LIMIT 1",
                (runtime.state.configuration.fingerprint, day.isoformat())).fetchone()
        if row is None:
            return NativeMinuteForwardView(status="unavailable", as_of=cutoff, frame=None, nav=nav, message="今天尚未发布收盘净值。")
        material = NativePaperCloseMaterials.model_validate_json(row[0])
        visible = tuple(point for point in nav if point.trade_date == material.trade_date and point.published_at <= cutoff)
        if material.available_at > cutoff or not visible or visible[0].status != "complete":
            return NativeMinuteForwardView(status="unavailable", as_of=cutoff, frame=None, nav=nav, message="收盘估值尚不完整。")
        frame = runtime.ledger_source_for(self.broker).read(configuration=runtime.state.configuration, as_of=cutoff,
            prices={item.ts_code: item.close_price for item in material.prices})
        if (frame.ledger_revision, frame.head_fingerprint) != (visible[0].ledger_revision, visible[0].ledger_head_fingerprint):
            return NativeMinuteForwardView(status="unavailable", as_of=cutoff, frame=frame, nav=nav, message="账本已更新，等待新的完整估值。")
        return NativeMinuteForwardView(status="complete", as_of=cutoff, frame=frame, nav=nav)


class PaperResearchRuntimeDirectory:
    def __init__(self, *, states: tuple[PaperPortfolioStateStore | NativeMinuteForwardState, ...],
                 expected_identities: tuple[PaperPortfolioStateIdentity | StrategyAuthoringIdentity, ...]) -> None:
        if not 1 <= len(states) <= 64 or len(states) != len(expected_identities):
            raise ValueError("paper runtime requires its finite exact account directory")
        if any(type(state) not in {PaperPortfolioStateStore, NativeMinuteForwardState} or state.identity() != identity
               for state, identity in zip(states, expected_identities, strict=True)):
            raise TypeError("paper runtime requires the original concrete metadata sources")
        keys = tuple((state.configuration.binding.account_id, state.configuration.binding.owner_id) for state in states)
        if len(set(keys)) != len(keys):
            raise ValueError("paper runtime account sources must be unique")
        self._states = states
        self._identities = expected_identities

    def catalog_for_spec(self, spec: ResearchRunSpec) -> PaperResearchCatalog:
        if spec.parameters.strategy_name not in PAPER_RESEARCH_TASKS:
            raise ValueError("paper runtime requires one of its two exact task names")
        parameters = PaperResearchRunParameters.model_validate({item.name: item.value for item in spec.parameters.arguments})
        matches = tuple((state, identity) for state, identity in zip(self._states, self._identities, strict=True)
                        if (state.configuration.binding.account_id, state.configuration.binding.owner_id) == (parameters.account_id, parameters.owner_id))
        if len(matches) != 1:
            raise ValueError("paper runtime account or owner is not registered")
        state, identity = matches[0]
        if state.identity() != identity:
            raise ValueError("paper runtime original metadata identity was replaced")
        configuration = state.configuration_at(parameters.configuration_fingerprint, version=parameters.configuration_version)
        catalog = (NativePaperResearchAdapterCatalog(metadata_identity=identity, configuration=configuration,
            source_code_identity=paper_research_code_identity(native=True)) if type(state) is NativeMinuteForwardState
            else PaperResearchAdapterCatalog(metadata_identity=identity, configuration=configuration,
                source_code_identity=paper_research_code_identity()))
        paper_research_adapter_registry(catalog).for_spec(spec).parameters(spec)
        return catalog

    def registry_for_spec(self, spec: ResearchRunSpec) -> StrategyJobAdapterRegistry:
        return paper_research_adapter_registry(self.catalog_for_spec(spec))

    def manifest_for_spec(self, spec: ResearchRunSpec, original: LabShardRuntimeManifest) -> LabShardRuntimeManifest:
        from rquant.lab_worker import _BUILTIN_SHARD_REGISTRY_ID, build_builtin_shard_runtime_manifest
        from rquant.lab_worker_registry import _read_builtin_runtime_configuration

        catalog = self.catalog_for_spec(spec)
        if original.registry.registry_id != _BUILTIN_SHARD_REGISTRY_ID:
            raise ValueError("paper runtime requires the closed builtin manifest")
        config = _read_builtin_runtime_configuration(strict_canonical_json_loads(original.registry.configuration_json))
        if not config.configured or any(path is None for path in (config.catalog_path, config.snapshot_root, config.research_lake_root)):
            raise ValueError("paper runtime immutable source is not configured")
        result = build_builtin_shard_runtime_manifest(catalog_path=config.catalog_path, forbidden_paths=config.forbidden_paths,
                                                      snapshot_root=config.snapshot_root, research_lake_root=config.research_lake_root,
                                                      paper_catalog=catalog)
        if len(result.registry.configuration_json.encode()) > 512*1024:
            raise ValueError("paper runtime catalog exceeds the original wire budget")
        return result


def require_paper_runtime_directory(value: PaperResearchRuntimeDirectory | None) -> PaperResearchRuntimeDirectory | None:
    if value is not None and type(value) is not PaperResearchRuntimeDirectory:
        raise TypeError("paper runtime directory must be the concrete installed metadata reader")
    return value
