"""Verified causal prefixes and the original targeted plan/ledger/worker for tracking."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Callable
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal
from uuid import uuid4
from zoneinfo import ZoneInfo

from loguru import logger
from pydantic import BaseModel, Field, model_validator

from rquant.factor.daily_feature_source import open_factor_daily_feature_source
from rquant.factor.job_worker import run_one_factor_job
from rquant.factor.member_archive import load_factor_member_archive
from rquant.factor.member_stream import open_factor_member_stream
from rquant.factor.neutralization_context import open_factor_neutralization_context
from rquant.factor.registry import (
    FactorDefinitionRegistry,
    FactorHeadRef,
    _canonical_model_json,
    _sha256,
)
from rquant.factor.result_artifact import _file_identity
from rquant.factor.run_configuration import (
    FactorRunFileReference,
    LoadedFactorRunConfiguration,
    open_factor_run_configuration,
)
from rquant.factor.run_plan import FrozenFactorRunPlan, compile_factor_run_plan
from rquant.factor.run_request import RUN_IMMUTABLE, FactorRunParameters, FactorRunRequest
from rquant.factor.stream_adapter import FactorStreamAdapter
from rquant.factor.stream_job_artifact import (
    VerifiedFactorStreamArtifacts,
    verify_factor_stream_artifacts,
)
from rquant.factor.stream_job_spec import FactorStreamJobSpec
from rquant.factor.stream_snapshot import open_factor_stream_snapshot_admission
from rquant.factor.tracking import (
    FactorTrackingConflict,
    FactorTrackingDay,
    FactorTrackingIdentity,
    FactorTrackingIntegrityError,
    FactorTrackingState,
    FactorTrackingStore,
)
from rquant.factor.tracking_serving import _days
from rquant.factor.universe import select_factor_universe
from rquant.runtime_contracts import AwareUtcDatetime, canonical_sha256


class FactorTrackingPrefixDay(BaseModel):
    model_config = RUN_IMMUTABLE
    trade_date: date
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class FactorTrackingRun(BaseModel):
    model_config = RUN_IMMUTABLE
    run_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    segment_id: str = Field(pattern=r"^[0-9a-f]{32}$")
    factor_id: str
    root: str
    configuration_reference: FactorRunFileReference
    original_cursor: date | None
    plan: FrozenFactorRunPlan
    prefix_spec: FactorStreamJobSpec
    prefix: tuple[FactorTrackingPrefixDay, ...] = Field(min_length=1, max_length=1024)
    status: Literal["planned", "committed"] = "planned"
    committed_at: AwareUtcDatetime | None = None

    @model_validator(mode="after")
    def _bound(self) -> FactorTrackingRun:
        if (self.status == "committed") != (self.committed_at is not None):
            raise ValueError("tracking run completion is inconsistent")
        if (
            tuple(p.trade_date for p in self.prefix)
            != self.prefix_spec.adapter_request.formula.trading_days
        ):
            raise ValueError("tracking prefix schedule differs")
        if self.plan.request.command_id.replace("-", "") != self.run_id:
            raise ValueError("tracking run differs from original ledger command")
        if self.plan.request.parameters.factor_id != self.factor_id:
            raise ValueError("tracking run factor differs")
        return self


class FactorTrackingRunOutcome(BaseModel):
    model_config = RUN_IMMUTABLE
    factor_id: str
    status: Literal["updated", "waiting", "paused"]
    run_id: str | None = None
    job_id: str | None = None
    evaluation_days: tuple[date, ...] = ()
    reason: str | None = None


@dataclass(frozen=True)
class _InputWitness:
    files: tuple[tuple[Path, tuple[int, ...]], ...]

    def recheck(self) -> None:
        for path, identity in self.files:
            if _input_identity(path) != identity:
                raise ValueError("tracking causal input changed during execution")


def _input_identity(path: Path) -> tuple[int, ...]:
    node = os.stat(path, follow_symlinks=False)
    if stat.S_ISDIR(node.st_mode):
        return node.st_dev, node.st_ino, node.st_mode, node.st_uid
    return _file_identity(node)


def _input_files(
    loaded: LoadedFactorRunConfiguration, spec: FactorStreamJobSpec
) -> tuple[Path, ...]:
    config = loaded.configuration
    manifest = load_factor_member_archive(config.member_root, spec.member_archive)
    paths = [loaded.root, config.lake_root, config.member_root]
    paths.extend(loaded.root / name for name in loaded.identities)
    paths.extend(
        config.lake_root / item.relative_path for item in loaded.source.binding.manifest.artifacts
    )
    paths.append(config.lake_root / loaded.source.binding.manifest_relative_path)
    paths.append(config.member_root / spec.member_archive.filename)
    paths.extend(config.member_root / day.filename for day in manifest.days)
    if spec.adapter_request.context is not None:
        for source in (
            spec.adapter_request.context.industry,
            spec.adapter_request.context.market_cap,
        ):
            if source is not None:
                paths.append(config.lake_root / source.artifact.relative_path)
    if spec.adapter_request.daily_feature_source is not None:
        paths.extend(
            config.lake_root / artifact.relative_path
            for artifact in spec.adapter_request.daily_feature_source.input_artifacts()
        )
    return tuple(paths)


def read_factor_tracking_prefix(
    loaded: LoadedFactorRunConfiguration, spec: FactorStreamJobSpec
) -> tuple[tuple[FactorTrackingPrefixDay, ...], _InputWitness]:
    """Read one day and <=500 codes per raw query, without evaluating historical formulas."""
    request, config = spec.adapter_request, loaded.configuration
    if request.daily_feature_source is not None:
        if (
            request.daily_feature_source != loaded.daily_features
            or spec.daily_feature_lake_root != config.lake_root
        ):
            raise ValueError("tracking stored source differs from original configuration")
        request.daily_feature_source.require_prepared(loaded.source)
    witness = _InputWitness(
        tuple((path, _input_identity(path)) for path in _input_files(loaded, spec))
    )
    result = []
    with ExitStack() as stack:
        members = stack.enter_context(
            open_factor_member_stream(config.member_root, spec.member_archive)
        )
        lease, decision = stack.enter_context(
            open_factor_stream_snapshot_admission(
                request.source, metadata_store=loaded.metadata, lake_root=config.lake_root
            )
        )
        context = (
            None
            if request.context is None
            else stack.enter_context(
                open_factor_neutralization_context(request.context, lake_root=config.lake_root)
            )
        )
        stored = (
            None
            if request.daily_feature_source is None
            else stack.enter_context(
                open_factor_daily_feature_source(
                    request.daily_feature_source, lake_root=config.lake_root
                )
            )
        )
        adapter = FactorStreamAdapter(
            request,
            lease=lease,
            decision=decision,
            universe_requests=members,
            context_lease=context,
            daily_feature_lease=stored,
        )
        stack.callback(adapter.close)
        for batch in adapter:
            day = batch.universe.trade_date
            window = adapter._windows.get(day)
            bars = adjustments = ()
            if window is not None:
                raw, _ = adapter._read_stock("daily_bar", (day,), request.source.scope.stock_codes)
                bars = tuple(
                    (raw[key].ts_code, raw[key].trade_date, raw[key].open, raw[key].close)
                    for key in sorted(raw)
                )
                raw, _ = adapter._read_stock("adj_factor", (day,), request.source.scope.stock_codes)
                adjustments = tuple(raw[key] for key in sorted(raw))
                del raw
            facts = (
                None
                if batch.context is None
                else json.loads(batch.context.model_dump_json(exclude={"sources"}))
            )
            # Generation-bearing receipts are provenance, not logical causal prefix values.
            semantic = (
                "factor-tracking-input-v1",
                spec.code_revision,
                spec.definition_content_sha256,
                request.formula.computation_stock_codes,
                day,
                adapter._panels[day],
                batch.feature_points,
                select_factor_universe(batch.universe).stock_codes,
                facts,
                window,
                bars,
                adjustments,
            )
            if batch.daily_features is not None:
                original = batch.daily_features
                derived = original.sources.value_semantics == "history_derived"
                daily_semantic = (
                    "derived-daily-fields-v1" if derived else "stored-daily-fields-v1",
                    tuple(field.column for field in original.sources.fields),
                    original.trade_date,
                    original.panel_date,
                    tuple(
                        (
                            row.stock_code,
                            tuple(
                                (v.status, v.value, v.non_finite_value, v.reason)
                                if derived
                                else (v.status, v.value, v.non_finite_value)
                                for v in row.values
                            ),
                        )
                        for row in original.rows
                    ),
                )
                if derived:
                    daily_semantic += (
                        request.daily_feature_source.technical_history.causal_policy(
                            original.panel_date
                        ),
                    )
                semantic += (daily_semantic,)
                del original
            result.append(
                FactorTrackingPrefixDay(trade_date=day, sha256=canonical_sha256(semantic))
            )
            del batch, facts, semantic, bars, adjustments
        members.require_completion()
        loaded.recheck()
        witness.recheck()
    witness.recheck()
    return tuple(result), witness


def _run_row(store: FactorTrackingStore, row: sqlite3.Row) -> FactorTrackingRun:
    run = store._decode(FactorTrackingRun, row["payload"], row["sha256"])
    if run.run_id != row["run_id"] or run.segment_id != row["segment_id"]:
        raise FactorTrackingIntegrityError("tracking run identity differs")
    return run


def _last_run(
    store: FactorTrackingStore, connection: sqlite3.Connection, segment: str
) -> FactorTrackingRun | None:
    row = connection.execute(
        "SELECT * FROM tracking_runs WHERE segment_id=? ORDER BY rowid DESC LIMIT 1", (segment,)
    ).fetchone()
    return None if row is None else _run_row(store, row)


def _save_run(
    store: FactorTrackingStore, connection: sqlite3.Connection, run: FactorTrackingRun
) -> None:
    payload = _canonical_model_json(FactorTrackingRun.model_validate(run))
    connection.execute(
        "INSERT INTO tracking_runs VALUES (?, ?, ?, ?) ON CONFLICT(run_id) "
        "DO UPDATE SET payload=excluded.payload, sha256=excluded.sha256",
        (run.run_id, run.segment_id, payload, _sha256(payload)),
    )


class FactorTrackingRunner:
    def __init__(
        self,
        root: Path,
        reference: FactorRunFileReference,
        tracking_identity: FactorTrackingIdentity,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.root, self.reference, self.identity, self.clock = (
            Path(root),
            reference,
            tracking_identity,
            clock,
        )
        self.store = FactorTrackingStore(Path(tracking_identity.path), clock=clock)

    def due_tick(self) -> tuple[FactorTrackingRunOutcome, ...]:
        """A due tick needs today's actual SSE row; a historical run is a separate entry."""
        from rquant.research_snapshot import FactorReadQuery

        now = self.clock().astimezone(ZoneInfo("Asia/Shanghai"))
        states = tuple(
            state
            for state in self.store.list_states(expected_identity=self.identity)
            if state.tracked
        )
        reason = None
        target = None
        if now.weekday() >= 5 or (now.hour, now.minute) < (18, 40):
            reason = "尚未到工作日18:40跟踪时点。"
        else:
            try:
                with open_factor_run_configuration(self.root, self.reference) as loaded:
                    source = loaded.source
                    scope = source.admission_request.scope
                    if (
                        not scope.start_date <= now.date() <= scope.end_date
                        or scope.as_of_time > now
                    ):
                        reason = "当日交易日历或已成熟来源尚未就绪。"
                    else:
                        with open_factor_stream_snapshot_admission(
                            source.admission_request,
                            metadata_store=loaded.metadata,
                            lake_root=loaded.configuration.lake_root,
                        ) as (lease, _):
                            rows = lease.query_sse_calendar(
                                FactorReadQuery(
                                    binding_hash=source.admission_request.binding_hash,
                                    stock_codes=(scope.stock_codes[0],),
                                    start_date=now.date(),
                                    end_date=now.date(),
                                    row_limit=1,
                                )
                            ).rows
                            if len(rows) != 1 or rows[0].cal_date != now.date():
                                raise ValueError("actual SSE day is absent")
                            if not rows[0].is_open:
                                reason = "当日SSE休市，等待下一交易日。"
                            else:
                                target = rows[0].pretrade_date
                                if (
                                    target is None
                                    or target not in source.receipt.calendar_open_days
                                ):
                                    reason = "昨日交易日或成熟收益尚未就绪。"
            except (OSError, ValueError, RuntimeError):
                reason = "当日交易日历或已成熟来源暂不可核验。"
        if reason is not None:
            if now.weekday() < 5 and (now.hour, now.minute) >= (18, 40):
                return tuple(
                    self._message(
                        state,
                        paused=state.status == "paused",
                        reason=state.reason or reason if state.status == "paused" else reason,
                    )
                    for state in states
                )
            return tuple(
                FactorTrackingRunOutcome(
                    factor_id=state.factor_id,
                    status="paused" if state.status == "paused" else "waiting",
                    reason=state.reason if state.status == "paused" else reason,
                )
                for state in states
            )
        return tuple(self.run_history(state.factor_id, target_end=target) for state in states)

    def _message(
        self, state: FactorTrackingState, *, paused: bool, reason: str
    ) -> FactorTrackingRunOutcome:
        with self.store._connection(self.identity, write=True) as connection:
            current = self.store._state(connection, state.factor_id)
            if current is not None and current.generation == state.generation and current.tracked:
                if current.status == "paused":
                    return FactorTrackingRunOutcome(
                        factor_id=state.factor_id, status="paused", reason=current.reason
                    )
                if current.cursor == state.cursor:
                    self.store._save_state(
                        connection,
                        current.model_copy(
                            update={"status": "paused" if paused else "waiting", "reason": reason}
                        ),
                    )
        return FactorTrackingRunOutcome(
            factor_id=state.factor_id, status="paused" if paused else "waiting", reason=reason
        )

    def _head(self, state: FactorTrackingState) -> None:
        registry = FactorDefinitionRegistry(Path(state.registry_identity.path))
        record = registry.get_head(state.factor_id, expected_identity=state.registry_identity)
        if (
            record is None
            or record.head.archived
            or FactorHeadRef(version=record.head.version, content_sha256=record.content_sha256)
            != state.head
        ):
            raise FactorTrackingConflict("tracking definition changed")

    def _prepare(
        self, state: FactorTrackingState, target_end: date | None
    ) -> FactorTrackingRun | None:
        with open_factor_run_configuration(self.root, self.reference) as loaded:
            if loaded.configuration.registry_identity != state.registry_identity:
                raise FactorTrackingConflict("tracking registry identity changed")
            record = FactorDefinitionRegistry(Path(state.registry_identity.path)).get_head(
                state.factor_id, expected_identity=state.registry_identity
            )
            opened = loaded.source.receipt.calendar_open_days
            history = record.definition.max_history_window
            eligible = tuple(
                day
                for index, day in enumerate(opened)
                if index >= history
                and index + 1 < len(opened)
                and datetime.combine(opened[index + 1], datetime.min.time(), UTC)
                <= loaded.source.admission_request.scope.as_of_time
            )
            if not eligible:
                return None
            # The actual 09:25 Shanghai visibility is checked by the original adapter/plan too.
            from rquant.factor.historical_adapter import _market_time

            eligible = tuple(
                day
                for day in eligible
                if _market_time(opened[opened.index(day) + 1], 9, 25)
                <= loaded.source.admission_request.scope.as_of_time
            )
            if not eligible or (target_end is not None and target_end > eligible[-1]):
                return None
            target = eligible[-1] if target_end is None else target_end
            dates = tuple(
                day
                for day in eligible
                if day <= target and (state.cursor is None or day > state.cursor)
            )
            if not dates:
                return None
            first = eligible[0] if state.start_date is None else state.start_date
            request = FactorRunRequest(
                command_id=str(uuid4()),
                requested_at=self.clock(),
                serving_generation_id="0" * 64,
                parameters=FactorRunParameters(
                    factor_id=state.factor_id,
                    expected_head=state.head,
                    selection="all",
                    start_date=first,
                    end_date=dates[-1],
                    holding_sessions=1,
                    group_count=5,
                    ic_method="rank",
                    neutralization="none",
                ),
            )
            full = compile_factor_run_plan(
                self.root,
                self.reference,
                request,
                verified_registry_instance_id=state.registry_identity.instance_id,
                clock=self.clock,
            )
            prefix, _ = read_factor_tracking_prefix(loaded, full.spec)
            with self.store._connection(self.identity) as connection:
                previous = _last_run(self.store, connection, state.segment_id)
            if previous is not None:
                before = {day.trade_date: day.sha256 for day in prefix}
                if any(before.get(day.trade_date) != day.sha256 for day in previous.prefix):
                    raise FactorTrackingConflict("tracking causal prefix changed")
            incremental = request.model_copy(
                update={
                    "parameters": request.parameters.model_copy(update={"start_date": dates[0]})
                }
            )
            plan = (
                full
                if state.cursor is None
                else compile_factor_run_plan(
                    self.root,
                    self.reference,
                    incremental,
                    verified_registry_instance_id=state.registry_identity.instance_id,
                    clock=self.clock,
                )
            )
            return FactorTrackingRun(
                run_id=request.command_id.replace("-", ""),
                segment_id=state.segment_id,
                factor_id=state.factor_id,
                root=str(self.root),
                configuration_reference=self.reference,
                original_cursor=state.cursor,
                plan=plan,
                prefix_spec=full.spec,
                prefix=prefix,
            )

    def _reserve(
        self, state: FactorTrackingState, proposed: FactorTrackingRun
    ) -> FactorTrackingRun:
        with self.store._connection(self.identity, write=True) as connection:
            current = self.store._state(connection, state.factor_id)
            if (
                current is None
                or current.generation != state.generation
                or not current.tracked
                or current.cursor != state.cursor
                or current.status == "paused"
            ):
                raise FactorTrackingConflict("tracking generation changed before reservation")
            _days(self.store, connection, current)
            previous = _last_run(self.store, connection, state.segment_id)
            if previous is not None and previous.status == "planned":
                return previous
            _save_run(self.store, connection, proposed)
        return proposed

    def _commit(
        self,
        state: FactorTrackingState,
        run: FactorTrackingRun,
        verified: VerifiedFactorStreamArtifacts,
        witness: _InputWitness,
        ledger: object,
        job_id: str,
    ) -> None:
        days = tuple(
            FactorTrackingDay.from_stream_day(day)
            for day in verified.full.result.research.research.statistics.days
        )
        if tuple(day.trade_date for day in days) != run.plan.spec.adapter_request.evaluation_days:
            raise ValueError("tracking verified result schedule differs")
        registry = FactorDefinitionRegistry(Path(state.registry_identity.path))
        with ExitStack() as registry_readers:
            definitions = registry_readers.enter_context(registry._reader(state.registry_identity))
            with self.store._connection(self.identity, write=True) as connection:
                # Finish the natural reader tail while rollback is possible, retaining its
                # SQLite read lock in this transaction until the tracking effect commits.
                connection.execute(
                    "ATTACH DATABASE ? AS tracking_registry_guard",
                    (f"{registry.path.as_uri()}?mode=ro",),
                )
                guard = connection.execute(
                    "SELECT instance_id FROM tracking_registry_guard.factor_registry_identity "
                    "WHERE singleton=1"
                ).fetchone()
                if guard is None or guard[0] != state.registry_identity.instance_id:
                    raise FactorTrackingIntegrityError("tracking registry guard identity differs")
                head, _ = registry._load_factor(definitions, state.factor_id)
                current = self.store._state(connection, state.factor_id)
                if (
                    current is None
                    or not current.tracked
                    or current.generation != run.segment_id
                    or current.status == "paused"
                    or head is None
                    or head.archived
                    or FactorHeadRef(version=head.version, content_sha256=head.content_sha256)
                    != state.head
                ):
                    raise FactorTrackingConflict(
                        "tracking generation or definition changed before commit"
                    )
                _days(self.store, connection, current)
                stored = _last_run(self.store, connection, state.segment_id)
                if (
                    stored is not None
                    and stored.run_id == run.run_id
                    and stored.status == "committed"
                ):
                    registry_readers.close()
                    return
                if stored != run or current.cursor != run.original_cursor:
                    raise FactorTrackingConflict("tracking cursor changed before commit")
                job = ledger.get(job_id)
                if job is None or job.status != "succeeded" or job.spec != run.plan.spec:
                    raise ValueError("tracking ledger is not completely successful")
                for item in verified.witnesses:
                    item.recheck()
                witness.recheck()
                for day in days:
                    payload = _canonical_model_json(day)
                    connection.execute(
                        "INSERT INTO tracking_days VALUES (?, ?, ?, ?)",
                        (run.segment_id, day.trade_date.isoformat(), payload, _sha256(payload)),
                    )
                at = self.clock()
                updated = current.model_copy(
                    update={
                        "cursor": days[-1].trade_date,
                        "start_date": current.start_date or days[0].trade_date,
                        "updated_at": at,
                        "status": "active",
                        "reason": None,
                    }
                )
                self.store._save_state(
                    connection,
                    updated,
                )
                _save_run(
                    self.store,
                    connection,
                    run.model_copy(update={"status": "committed", "committed_at": at}),
                )
                _days(self.store, connection, updated)
                witness.recheck()
                for item in verified.witnesses:
                    item.recheck()
                registry_readers.close()

    def run_history(
        self, factor_id: str, *, target_end: date | None = None
    ) -> FactorTrackingRunOutcome:
        state = self.store.get(factor_id, expected_identity=self.identity)
        if state is None or not state.tracked:
            return FactorTrackingRunOutcome(
                factor_id=factor_id, status="waiting", reason="尚未加入跟踪。"
            )
        if state.status == "paused":
            return FactorTrackingRunOutcome(
                factor_id=factor_id, status="paused", reason=state.reason
            )
        try:
            self._head(state)
            with self.store._connection(self.identity) as connection:
                _days(self.store, connection, state)
                pending = _last_run(self.store, connection, state.segment_id)
            if pending is None or pending.status != "planned":
                proposed = self._prepare(state, target_end)
                if proposed is None:
                    return FactorTrackingRunOutcome(
                        factor_id=factor_id, status="waiting", reason="没有新的已成熟交易日。"
                    )
                pending = self._reserve(state, proposed)
            with open_factor_run_configuration(
                Path(pending.root), pending.configuration_reference
            ) as loaded:
                if (
                    loaded.configuration.registry_identity != pending.plan.registry_identity
                    or loaded.configuration.ledger_identity != pending.plan.ledger_identity
                ):
                    raise ValueError("tracking original configuration identities differ")
                prefix, _ = read_factor_tracking_prefix(loaded, pending.prefix_spec)
                if prefix != pending.prefix:
                    raise FactorTrackingConflict("tracking causal prefix changed before worker")
                ledger = loaded.open_ledger(clock=self.clock)
                job = ledger.submit(pending.plan.request.command_id, pending.plan.spec)
                if job.status in ("queued", "running"):
                    run_one_factor_job(
                        ledger,
                        metadata_store=loaded.metadata,
                        lake_root=loaded.configuration.lake_root,
                        artifact_root=loaded.configuration.artifact_root,
                        member_root=loaded.configuration.member_root,
                        runner_now=self.clock,
                        job_id=job.job_id,
                    )
                job = ledger.get(job.job_id)
                if job is None or job.status != "succeeded" or job.completion is None:
                    return self._message(
                        state, paused=False, reason="原跟踪任务尚未完整成功，保留已有历史。"
                    )
                stored_fields = pending.prefix_spec.adapter_request.daily_feature_source is not None
                if stored_fields:
                    # The stored reader removes its private copy before the artifact root
                    # witness freezes the directory identity for the original commit.
                    final_prefix, witness = read_factor_tracking_prefix(loaded, pending.prefix_spec)
                    if final_prefix != pending.prefix:
                        raise FactorTrackingConflict("tracking causal prefix changed after worker")
                verified = verify_factor_stream_artifacts(
                    pending.plan.spec,
                    job.completion,
                    loaded.configuration.artifact_root,
                    loaded.configuration.member_root,
                )
                if stored_fields:
                    witness.recheck()
                else:
                    final_prefix, witness = read_factor_tracking_prefix(loaded, pending.prefix_spec)
                    if final_prefix != pending.prefix:
                        raise FactorTrackingConflict("tracking causal prefix changed after worker")
                loaded.recheck()
            # Finish all source contexts before the atomic effect; recheck witnesses in the writer.
            self._commit(state, pending, verified, witness, ledger, job.job_id)
            return FactorTrackingRunOutcome(
                factor_id=factor_id,
                status="updated",
                run_id=pending.run_id,
                job_id=job.job_id,
                evaluation_days=pending.plan.spec.adapter_request.evaluation_days,
            )
        except FactorTrackingConflict:
            return self._message(
                state, paused=True, reason="定义、跟踪代或历史输入已变化，请重新加入跟踪。"
            )
        except (OSError, ValueError, RuntimeError) as error:
            logger.warning(
                "Factor tracking input/completion rejected: {}: {}", type(error).__name__, error
            )
            return self._message(
                state, paused=False, reason="来源或完成产物暂不可核验，保留已有历史。"
            )
