"""Replay the original complete screen against a pinned, full portfolio source."""

from __future__ import annotations

import hashlib
import os
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from pydantic import Field, field_validator

from rquant.backtest.contracts import BacktestDayInput, BacktestRequest, RankingSnapshot, Sha256
from rquant.portfolio.weights import PortfolioCandidate
from rquant.portfolio_backtest_models import (
    MAX_SOURCE_CODES, MAX_SOURCE_PAIRS, PortfolioBacktestConfig, PortfolioSourceManifest,
)
from rquant.portfolio_backtest_source import PortfolioSourceData, PortfolioExperimentProtocol, freeze_portfolio_config
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256
from rquant.screen.query_admission import ScreenQueryExecutor
from rquant.screen.query_contracts import ScreenQueryDefinition
from rquant.web.screen_service import ScreenApplicationError

if TYPE_CHECKING:
    from rquant.page_control import PageControlOutbox, PageControlService
    from rquant.screen.query_history import ScreenQueryHistory
    from rquant.web.models.ai_assistance import AIBacktestPrepareRequest, AIBacktestPreparation, AIBacktestConfirmRequest, AIBacktestConfirmation
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.portfolio_backtest_commands import PortfolioCommandWriter
    from rquant.portfolio_backtest_source import PortfolioExperimentProtocol

MAX_SOURCE_ARTIFACT_BYTES = 16 * 1024 * 1024


class AIHistoricalScreenDay(RuntimeContractModel):
    trade_date: date
    source_trade_date: date
    source_identity: Sha256
    normalized_plan_sha256: Sha256
    complete_result_sha256: Sha256
    candidate_count: int = Field(strict=True, ge=0, le=MAX_SOURCE_CODES)


class AIHistoricalScreenProof(RuntimeContractModel):
    complete: bool
    original_definition_sha256: Sha256
    base_material_sha256: Sha256
    days: tuple[AIHistoricalScreenDay, ...]
    candidate_count: int = Field(strict=True, ge=0, le=MAX_SOURCE_CODES)


class AIHistoricalScreenMaterial(RuntimeContractModel):
    source: PortfolioSourceData
    config: PortfolioBacktestConfig
    proof: AIHistoricalScreenProof


class AIHistoricalScreenSource:
    def __init__(
        self, *, screen: ScreenQueryExecutor, base: PortfolioSourceData,
        default_config: PortfolioBacktestConfig,
    ) -> None:
        if type(screen) is not ScreenQueryExecutor:
            raise TypeError("historical screening requires the original executor")
        self.screen = screen
        self.base = PortfolioSourceData.model_validate(base.model_dump(mode="python"))
        self.default_config = PortfolioBacktestConfig.model_validate(default_config.model_dump(mode="python"))

    def build(
        self, definition: ScreenQueryDefinition, *, start_date: date, end_date: date,
    ) -> AIHistoricalScreenMaterial:
        checked = ScreenQueryDefinition.model_validate(definition.model_dump(mode="python"))
        if checked.mode != "daily":
            raise ValueError("历史回测需要日线条件。")
        if not self.base.template.days[0].trade_date <= start_date <= end_date <= self.base.template.days[-1].trade_date:
            raise ValueError("这段日期缺少完整行情，请缩短区间。")
        original = tuple(day for day in self.base.template.days if start_date <= day.trade_date <= end_date)
        expected = tuple(d for d in self.base.template.calendar.dates if start_date <= d <= end_date)
        if tuple(day.trade_date for day in original) != expected or not expected:
            raise ValueError("历史交易日不完整。")
        days: list[BacktestDayInput] = []
        proofs: list[AIHistoricalScreenDay] = []
        union: set[str] = set()
        for day in original:
            previous = self.base.template.calendar.dates[self.base.template.calendar.dates.index(day.trade_date)-1]
            replay = ScreenQueryDefinition.model_validate(checked.model_dump(mode="python") | {"trade_date": previous})
            try:
                result = self.screen(replay)
            except ScreenApplicationError as error:
                raise ValueError("历史筛选数据不完整或已更新，请重新运行。") from error
            if (result.status != "ready" or result.source is None
                or result.source.identity != checked.source_identity or result.trade_date != previous
                or result.next_cursor is not None
                or (result.ranked_count if checked.ranking is not None else result.total) != len(result.rows)
                or result.unknown_count != 0):
                raise ValueError("历史筛选未覆盖全部候选，请补齐数据。")
            if checked.ranking is not None and (result.ranked_count is None or any(row.ranking_score is None or row.rank_position != index for index, row in enumerate(result.rows, 1))):
                raise ValueError("历史排名不完整。")
            industries = {candidate.ts_code: candidate.industry_l1 for candidate in day.ranking.candidates}
            candidates = tuple(PortfolioCandidate(
                ts_code=row.ts_code,
                rank_score=Decimal(str(row.ranking_score)) if row.ranking_score is not None else Decimal("0"),
                industry_l1=industries.get(row.ts_code),
            ) for row in result.rows)
            if self.default_config.weight_rule.max_industry_weight is not None and any(candidate.industry_l1 is None for candidate in candidates):
                raise ValueError("行业数据不完整，暂不能确认回测。")
            if self.default_config.weight_rule.method == "rank_score" and checked.ranking is None:
                raise ValueError("原默认策略需要排名，请先设置排名。")
            union.update(candidate.ts_code for candidate in candidates)
            if len(union) > MAX_SOURCE_CODES:
                raise ValueError("完整历史候选超出回测范围，请缩短区间。")
            proof = AIHistoricalScreenDay(trade_date=day.trade_date, source_trade_date=previous,
                source_identity=result.source.identity, normalized_plan_sha256=replay.normalized_plan_sha256,
                complete_result_sha256=canonical_sha256(result), candidate_count=len(candidates))
            proofs.append(proof)
            # Replay observation times come from the original retrospective source.
            ranking = RankingSnapshot(source_identity=canonical_sha256(proof), source_trade_date=previous,
                observed_at=day.ranking.observed_at, candidates=candidates)
            days.append(BacktestDayInput(trade_date=day.trade_date, ranking=ranking, instruments=day.instruments))
        selected_days: list[BacktestDayInput] = []
        for day in days:
            quotes = {quote.ts_code: quote for quote in day.instruments}
            if not union <= quotes.keys() or any(
                quotes[code].decision_price is None or quotes[code].open_price is None
                or quotes[code].close_price is None or quotes[code].conditions is None for code in union
            ):
                raise ValueError("完整候选缺少行情或成交条件，暂不能确认回测。")
            selected_days.append(day.model_copy(update={"instruments": tuple(quote for quote in day.instruments if quote.ts_code in union)}))
        if sum(len(day.instruments) for day in selected_days) > MAX_SOURCE_PAIRS:
            raise ValueError("完整历史行情超出回测范围，请缩短区间。")
        proof = AIHistoricalScreenProof(complete=True, original_definition_sha256=canonical_sha256(checked),
            base_material_sha256=self.base.material_hash, days=tuple(proofs), candidate_count=len(union))
        proof_sha = canonical_sha256(proof)
        source_key = "ai-screen." + proof_sha
        sources = PortfolioSourceManifest.model_validate(self.base.sources.model_dump(mode="python") | {"ranking_hash": proof_sha})
        template = BacktestRequest.model_validate(self.base.template.model_dump(mode="python") | {"days": tuple(selected_days), "input_generation_id": proof_sha})
        source = PortfolioSourceData(source_key=source_key, source_version=1, template=template,
            sources=sources, benchmarks=self.base.benchmarks)
        config = PortfolioBacktestConfig.model_validate(self.default_config.model_dump(mode="python") | {
            "source_key": source_key, "source_version": 1, "start_date": start_date, "end_date": end_date,
        })
        frozen = freeze_portfolio_config(source, config)
        if frozen.benchmark_unavailable is not None:
            raise ValueError("基准行情不完整，暂不能确认回测。")
        return AIHistoricalScreenMaterial(source=source, config=config, proof=proof)


def install_ai_backtest_tables(connection: sqlite3.Connection) -> None:
    connection.execute("""CREATE TABLE IF NOT EXISTS ai_backtest_preparation (
        request_id TEXT PRIMARY KEY, owner_uid TEXT NOT NULL, body_sha256 TEXT NOT NULL,
        view_json TEXT NOT NULL, source_key TEXT NOT NULL, artifact_sha256 TEXT NOT NULL
    )""")
    connection.execute("CREATE INDEX IF NOT EXISTS ai_backtest_source_idx ON ai_backtest_preparation(source_key)")


class AIScreenBacktestArtifacts:
    def __init__(self, root: Path) -> None:
        from rquant.page_control import _bind_managed_directory
        if not root.is_absolute() or Path(os.path.abspath(root)) != root:
            raise ValueError("prepared source root must be canonical")
        self.root = root
        with closing(_bind_managed_directory(root, create=True)):
            pass

    def _name(self, source_key: str) -> str:
        import re
        if re.fullmatch(r"ai-screen\.[0-9a-f]{64}", source_key) is None:
            raise ValueError("prepared source key is invalid")
        return source_key + ".json"

    def read(self, source_key: str, expected_sha256: str) -> AIHistoricalScreenMaterial:
        from rquant.page_control import _bind_managed_directory
        with closing(_bind_managed_directory(self.root, create=False)) as bound:
            descriptor = os.open(self._name(source_key), os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=bound.descriptor)
            try:
                before = os.fstat(descriptor)
                if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1 or before.st_size > MAX_SOURCE_ARTIFACT_BYTES:
                    raise ValueError("prepared source permissions or size changed")
                chunks: list[bytes] = []
                total = 0
                while chunk := os.read(descriptor, min(64 * 1024, MAX_SOURCE_ARTIFACT_BYTES + 1-total)):
                    total += len(chunk)
                    if total > MAX_SOURCE_ARTIFACT_BYTES:
                        raise ValueError("prepared source exceeds its byte budget")
                    chunks.append(chunk)
                raw = b"".join(chunks)
                after = os.fstat(descriptor)
                visible = os.stat(self._name(source_key), dir_fd=bound.descriptor, follow_symlinks=False)
                identity = lambda node: (node.st_dev,node.st_ino,node.st_size,node.st_mtime_ns,node.st_ctime_ns,node.st_mode,node.st_uid,node.st_nlink)
                if identity(before) != identity(after) or identity(after) != identity(visible) or hashlib.sha256(raw).hexdigest() != expected_sha256:
                    raise ValueError("prepared source bytes changed")
                bound.verify()
            finally:
                os.close(descriptor)
        value = AIHistoricalScreenMaterial.model_validate_json(raw)
        if value.source.source_key != source_key or source_key != "ai-screen." + canonical_sha256(value.proof) or not value.proof.complete:
            raise ValueError("prepared source proof changed")
        freeze_portfolio_config(value.source, value.config)
        return value

    def put(self, value: AIHistoricalScreenMaterial) -> str:
        from rquant.page_control import _bind_managed_directory
        raw = value.model_dump_json().encode()
        if len(raw) > MAX_SOURCE_ARTIFACT_BYTES:
            raise ValueError("完整行情超出保存范围，请缩短区间。")
        digest = hashlib.sha256(raw).hexdigest()
        name = self._name(value.source.source_key)
        with closing(_bind_managed_directory(self.root, create=False)) as bound:
            try:
                descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600, dir_fd=bound.descriptor)
            except FileExistsError:
                self.read(value.source.source_key, digest)
                return digest
            try:
                remaining = memoryview(raw)
                while remaining:
                    written = os.write(descriptor, remaining)
                    if written <= 0:
                        raise OSError("prepared source write stopped")
                    remaining = remaining[written:]
                os.fsync(descriptor)
                bound.verify()
                os.fsync(bound.descriptor)
            except BaseException:
                os.unlink(name, dir_fd=bound.descriptor)
                raise
            finally:
                os.close(descriptor)
        return digest


class AIScreenBacktestPipeline:
    def __init__(self, *, history: ScreenQueryHistory, source: AIHistoricalScreenSource,
                 artifacts: AIScreenBacktestArtifacts, clock: Callable[[], datetime] | None = None) -> None:
        from rquant.screen.query_history import ScreenQueryHistory
        if type(history) is not ScreenQueryHistory:
            raise TypeError("backtest preparation needs the original private screen history")
        history._assert_private_database()
        self.history, self.source, self.artifacts = history, source, artifacts
        self.clock = clock or (lambda: datetime.now(UTC))
        with closing(history.outbox._connect()) as connection:
            install_ai_backtest_tables(connection)
            connection.commit()

    def _lookup(self, connection: sqlite3.Connection, owner: str, request_id: UUID, body_sha256: str | None = None) -> AIBacktestPreparation:
        from rquant.ai_usage import AIRequestConflict, AIRequestNotFound
        from rquant.web.models.ai_assistance import AIBacktestPreparation
        row = connection.execute("SELECT * FROM ai_backtest_preparation WHERE request_id=?", (str(request_id),)).fetchone()
        if row is None or row["owner_uid"] != owner:
            raise AIRequestNotFound("original preparation is unavailable")
        if body_sha256 is not None and row["body_sha256"] != body_sha256:
            raise AIRequestConflict("original preparation body changed")
        view = AIBacktestPreparation.model_validate_json(row["view_json"])
        if view.request_id != request_id or view.config.source_key != row["source_key"] or view.config.config_hash != view.config_sha256:
            raise ValueError("original preparation receipt changed")
        return view

    def lookup(self, owner: str, request: AIBacktestPrepareRequest) -> AIBacktestPreparation:
        self._current_role(owner)
        self.history._assert_private_database()
        with closing(self.history.outbox._connect()) as connection:
            return self._lookup(connection, owner, request.request_id, canonical_sha256(request))

    def prepare(self, owner: str, request: AIBacktestPrepareRequest) -> AIBacktestPreparation:
        from rquant.ai_usage import AIRequestNotFound
        from rquant.web.models.ai_assistance import AIBacktestPreparation
        try:
            return self.lookup(owner, request)
        except AIRequestNotFound:
            with closing(self.history.outbox._connect()) as connection:
                if connection.execute("SELECT 1 FROM ai_backtest_preparation WHERE request_id=?", (str(request.request_id),)).fetchone() is not None:
                    raise
        self._current_role(owner, write=True)
        execution = self.history.detail(owner, request.execution_id)
        if execution is None or execution.status != "succeeded" or execution.artifact_sha256 is None or execution.unknown_count != 0:
            raise ValueError("请先运行选股，并确认原结果完整。")
        material = self.source.build(execution.definition, start_date=request.start_date, end_date=request.end_date)
        with self._write_fence(owner):
            self._current_role(owner, write=True)
            digest = self.artifacts.put(material)
            view = AIBacktestPreparation(request_id=request.request_id, execution_id=request.execution_id,
                created_at=self.clock(), config=material.config, config_sha256=material.config.config_hash,
                material_sha256=material.source.material_hash, proof_sha256=canonical_sha256(material.proof),
                complete=True, trading_days=len(material.proof.days), candidate_count=material.proof.candidate_count)
            with closing(self.history.outbox._connect()) as connection:
                self._current_role(owner, write=True)
                connection.execute("BEGIN IMMEDIATE")
                try:
                    old = self._lookup(connection, owner, request.request_id, canonical_sha256(request))
                except AIRequestNotFound:
                    if connection.execute("SELECT 1 FROM ai_backtest_preparation WHERE request_id=?", (str(request.request_id),)).fetchone() is not None:
                        raise
                    connection.execute("INSERT INTO ai_backtest_preparation VALUES (?,?,?,?,?,?)", (str(request.request_id), owner, canonical_sha256(request), view.model_dump_json(), material.source.source_key, digest))
                    connection.commit()
                    self._current_role(owner)
                    return view
                else:
                    connection.rollback()
                    return old

    def _current_role(self, owner: str, *, write: bool = False) -> None:
        authority = self.history.outbox.collaboration
        if authority is None or authority.mode == "legacy":
            return
        authority.require_outbox_path(self.history.outbox.path)
        if write:
            authority.require_operation(owner, "POST", "/api/v1/ai/backtests/prepare")
        else:
            authority.current_role(owner)

    @contextmanager
    def _write_fence(self, owner: str) -> Iterator[None]:
        authority = self.history.outbox.collaboration
        if authority is None or authority.mode == "legacy":
            yield
            return
        with authority.locked():
            self._current_role(owner, write=True)
            yield

    def provider(self, source_key: str, source_version: int) -> PortfolioSourceData:
        self.history._assert_private_database()
        authority = self.history.outbox.collaboration
        actor = None
        if authority is not None and authority.mode == "enforced":
            from rquant.page_control import _TRUSTED_COLLABORATION_ACTOR
            actor = _TRUSTED_COLLABORATION_ACTOR.get()
            if actor is None:
                raise PermissionError("prepared source requires the original trusted command actor")
            self._current_role(actor, write=True)
        if (source_key, source_version) == (self.source.base.source_key, self.source.base.source_version):
            return self.source.base
        if source_version != 1:
            raise ValueError("prepared source version changed")
        with closing(self.history.outbox._connect()) as connection:
            connection.execute("PRAGMA query_only = ON")
            connection.execute("BEGIN")
            rows = (connection.execute("SELECT DISTINCT artifact_sha256 FROM ai_backtest_preparation WHERE source_key=? LIMIT 2", (source_key,)).fetchall()
                if actor is None else connection.execute("SELECT DISTINCT artifact_sha256 FROM ai_backtest_preparation WHERE source_key=? AND owner_uid=? LIMIT 2", (source_key, actor)).fetchall())
            if len(rows) != 1:
                if actor is None:
                    raise ValueError("prepared source has no exact original receipt")
                raise PermissionError("prepared source has no exact original owner receipt")
            material = self.artifacts.read(source_key, rows[0][0])
            if actor is not None:
                from rquant.web.models.ai_assistance import AIBacktestPreparation, AIBacktestPrepareRequest
                row = connection.execute("""SELECT request_id,body_sha256,
                    CASE WHEN length(CAST(view_json AS BLOB))<=65536 THEN view_json END
                    FROM ai_backtest_preparation WHERE source_key=? AND owner_uid=? ORDER BY rowid LIMIT 1
                """, (source_key, actor)).fetchone()
                if row is None or row[2] is None:
                    raise PermissionError("prepared source owner receipt exceeds capacity or is absent")
                view = AIBacktestPreparation.model_validate_json(row[2])
                original = AIBacktestPrepareRequest(request_id=view.request_id, execution_id=view.execution_id,
                    start_date=view.config.start_date, end_date=view.config.end_date)
                execution = self.history.detail(actor, view.execution_id)
                if (str(view.request_id) != row[0] or canonical_sha256(original) != row[1]
                        or view.config != material.config or view.config_sha256 != material.config.config_hash
                        or view.material_sha256 != material.source.material_hash or view.proof_sha256 != canonical_sha256(material.proof)
                        or not view.complete or view.trading_days != len(material.proof.days)
                        or view.candidate_count != material.proof.candidate_count
                        or execution is None or execution.status != "succeeded" or execution.artifact_sha256 is None
                        or execution.unknown_count != 0 or canonical_sha256(execution.definition) != material.proof.original_definition_sha256
                        or material.proof.base_material_sha256 != self.source.base.material_hash
                        or material.source.sources.model_dump(exclude={"ranking_hash"}) != self.source.base.sources.model_dump(exclude={"ranking_hash"})):
                    raise PermissionError("prepared source original owner, history or complete material binding differs")
                self._current_role(actor, write=True)
        return material.source

    def confirm(self, owner: str, request: AIBacktestConfirmRequest, *, control: PageControlService) -> AIBacktestConfirmation:
        from rquant.portfolio_backtest_commands import SubmitPortfolioBacktest, portfolio_job_id
        from rquant.web.models.ai_assistance import AIBacktestConfirmation
        if control.outbox is not self.history.outbox or control.collaboration is not self.history.outbox.collaboration:
            raise ValueError("confirmation must use the same original PageControl")
        self._current_role(owner, write=True)
        with closing(self.history.outbox._connect()) as connection:
            view = self._lookup(connection, owner, request.prepared_request_id)
        if view.config_sha256 != request.config_sha256 or view.proof_sha256 != request.proof_sha256:
            raise ValueError("准备结果已改变，请重新核对。")
        command = SubmitPortfolioBacktest(command_id=str(request.command_id), requested_at=request.requested_at, actor_id=owner, config=view.config)
        with self._write_fence(owner):
            receipt = (control.submit(command) if control.collaboration.mode == "legacy" else
                control.submit_authorized(command, control.collaboration.issue_authorization(owner, command.model_dump(mode="json"))))
        self._current_role(owner)
        return AIBacktestConfirmation(job_id=portfolio_job_id(owner, request.command_id), receipt=receipt)


def build_original_portfolio_writer(
    *, pipeline: AIScreenBacktestPipeline, commands: LabCommandSubmissionFacade,
    metadata_path: Path, catalog_path: Path, lake_root: Path, input_root: Path,
    protocol: PortfolioExperimentProtocol, code_commit: str,
    clock: Callable[[], datetime] | None = None,
) -> PortfolioCommandWriter:
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.portfolio_backtest_commands import PortfolioCommandWriter
    from rquant.portfolio_backtest_source import PortfolioRequestPreparer
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    if type(commands) is not LabCommandSubmissionFacade or commands.definition_registry is None or commands.experiment_registry is None:
        raise TypeError("AI backtests require the installed original Lab authority")
    if pipeline.source.base.template.producer_commit != code_commit:
        raise ValueError("historical producer differs from the installed code identity")
    prepare = PortfolioRequestPreparer(source_provider=pipeline.provider,
        metadata_store_factory=lambda: DuckDBStore(metadata_path),
        catalog=ResearchCatalog(catalog_path), lake_root=lake_root, input_root=input_root,
        definitions=commands.definition_registry, experiments=commands.experiment_registry,
        protocol=protocol, code_commit=code_commit, clock=clock or (lambda: datetime.now(UTC)))
    return PortfolioCommandWriter(commands=commands, prepare=prepare)


class AIHistoricalProfile(RuntimeContractModel):
    base_source_file: Path
    base_source_sha256: Sha256
    default_config: PortfolioBacktestConfig
    artifact_root: Path
    metadata_path: Path
    catalog_path: Path
    lake_root: Path
    input_root: Path
    protocol: PortfolioExperimentProtocol
    code_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    lab_jobs_path: Path
    lab_command_spool_path: Path
    lab_final_artifact_root: Path

    @field_validator("base_source_file", "artifact_root", "metadata_path", "catalog_path", "lake_root", "input_root", "lab_jobs_path", "lab_command_spool_path", "lab_final_artifact_root")
    @classmethod
    def canonical_private_path(cls, value: Path) -> Path:
        if not value.is_absolute() or Path(os.path.abspath(value)) != value or value.name.startswith(".env"):
            raise ValueError("historical producer paths must be explicit and canonical")
        return value


def load_ai_historical_profile(path: Path) -> AIHistoricalProfile:
    from rquant.page_control import _read_managed_file
    node = path.lstat()
    if not stat.S_ISREG(node.st_mode) or node.st_uid != os.geteuid() or stat.S_IMODE(node.st_mode) != 0o600 or node.st_nlink != 1 or path.name.startswith(".env"):
        raise ValueError("historical profile must be original-owner private")
    raw = _read_managed_file(path)
    if len(raw) > 64 * 1024:
        raise ValueError("historical profile exceeds its byte budget")
    return AIHistoricalProfile.model_validate_json(raw)


def read_original_historical_source(profile: AIHistoricalProfile) -> PortfolioSourceData:
    from rquant.page_control import _bind_managed_directory
    path = profile.base_source_file
    parent = path.parent.lstat()
    if not stat.S_ISDIR(parent.st_mode) or parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) != 0o700:
        raise ValueError("historical source parent must be private")
    with closing(_bind_managed_directory(path.parent, create=False)) as bound:
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=bound.descriptor)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1 or before.st_size > MAX_SOURCE_ARTIFACT_BYTES:
                raise ValueError("historical source permissions or size are invalid")
            chunks: list[bytes] = []
            count = 0
            while chunk := os.read(descriptor, min(64 * 1024, MAX_SOURCE_ARTIFACT_BYTES + 1-count)):
                count += len(chunk)
                if count > MAX_SOURCE_ARTIFACT_BYTES:
                    raise ValueError("historical source exceeds its byte budget")
                chunks.append(chunk)
            raw = b"".join(chunks)
            after = os.fstat(descriptor)
            visible = os.stat(path.name, dir_fd=bound.descriptor, follow_symlinks=False)
            identity=lambda node:(node.st_dev,node.st_ino,node.st_mode,node.st_uid,node.st_nlink,node.st_size,node.st_mtime_ns,node.st_ctime_ns)
            if identity(before) != identity(after) or identity(after) != identity(visible) or hashlib.sha256(raw).hexdigest() != profile.base_source_sha256:
                raise ValueError("historical source bytes changed")
            bound.verify()
        finally:
            os.close(descriptor)
    value = PortfolioSourceData.model_validate_json(raw)
    if value.template.producer_commit != profile.code_commit:
        raise ValueError("historical source producer identity differs")
    return value


def install_ai_screen_backtests(control: PageControlService, profile_path: Path) -> None:
    from rquant.lab_page_control import LabPageControlWriter
    from rquant.portfolio_backtest_artifact import PortfolioResultReader
    profile = load_ai_historical_profile(profile_path)
    lab = control.consumer.lab_backend
    history = control.consumer.screen_query_history
    ai = control.ai_assistance
    if type(lab) is not LabPageControlWriter or history is None or ai is None or control.consumer.portfolio_backend is not None:
        raise ValueError("historical AI must share one installed original screen and Lab chain")
    if lab.commands.reader.path != profile.lab_jobs_path or lab.commands.spool.root != profile.lab_command_spool_path:
        raise ValueError("historical profile differs from original Lab paths")
    base = read_original_historical_source(profile)
    pipeline = AIScreenBacktestPipeline(history=history,
        source=AIHistoricalScreenSource(screen=ai.contexts.screen, base=base, default_config=profile.default_config),
        artifacts=AIScreenBacktestArtifacts(profile.artifact_root), clock=ai.clock)
    writer = build_original_portfolio_writer(pipeline=pipeline, commands=lab.commands,
        metadata_path=profile.metadata_path, catalog_path=profile.catalog_path, lake_root=profile.lake_root,
        input_root=profile.input_root, protocol=profile.protocol, code_commit=profile.code_commit, clock=ai.clock)
    control.consumer.portfolio_backend = writer
    ai.backtests, ai.control = pipeline, control
    if ai.contexts.portfolio is None:
        ai.contexts.portfolio = PortfolioResultReader(reader=lab.commands.reader, artifact_root=profile.lab_final_artifact_root)


def build_installed_ai_lab_backend(profile_path: Path, *, runtime_root: Path, code_commit: str) -> object:
    from rquant.job_center_authority import resolve_current_job_center_authority_binding
    from rquant.lab_daemon import load_lab_job_center_authority_manifest
    from rquant.lab_page_control import build_lab_page_control_writer
    profile = load_ai_historical_profile(profile_path)
    if profile.code_commit != code_commit:
        raise ValueError("historical AI profile code differs from running runtime")
    binding = resolve_current_job_center_authority_binding(runtime_root, expected_code_sha=code_commit,
        runtime_root=profile.lab_jobs_path.parent, lab_jobs_path=profile.lab_jobs_path,
        command_spool_path=profile.lab_command_spool_path, final_artifact_root=profile.lab_final_artifact_root)
    manifest = load_lab_job_center_authority_manifest(binding.runtime_root / "job-center-authority.json",
        expected_code_sha=code_commit, expected_research_root=binding.runtime_root,
        expected_lab_jobs_path=binding.lab_jobs_path, expected_command_spool_path=binding.command_spool_path,
        expected_final_artifact_root=binding.final_artifact_root,
        expected_runtime_deployment_root=binding.runtime_deployment_root,
        expected_deployment_profile_id=binding.deployment_profile_id,
        expected_deployment_generation_hash=binding.deployment_generation_hash)
    return build_lab_page_control_writer(manifest)
