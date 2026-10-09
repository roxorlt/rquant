"""The parameter contract reuses the original bounded physical archive restore."""

from __future__ import annotations

import hashlib
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, time
from io import BytesIO
from typing import Iterator
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd

from rquant.executable_dependencies import (
    ExecutableBinding, ExecutableDependencyError, ExecutableDependencyGuard,
    capture_executable_dependency_guard,
)
from rquant.minute_backtest_contracts import MAX_INPUT_BYTES, MinuteReplayMaterial, MinuteReplayWork
from rquant.minute_backtest_parameter_contracts import (
    PARAMETER_SOURCE_TABLE, FrozenMinuteParameterInput, FrozenMinuteParameterResearchInput,
    MinuteParameterRuntimeReceipt, MinuteParameterWork,
)
from rquant.minute_backtest_parameter_features import (
    PARAMETER_CANDIDATE_FEATURE, MinuteParameterCandidate, MinuteParameterSessionFacts,
)
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding
from rquant.live_contracts import BatchEnvelope, BatchQualityStatus
from rquant.market_minute_gateway import MarketMinuteGateway
from rquant.strategy_candidate_snapshot import StrategyCandidateSnapshot
from rquant.minute_backtest_source import RestoredMinuteRuntimeSource, _restore_minute_replay_archive
from rquant.runtime_contracts import canonical_sha256
from rquant.strict_json import strict_json_loads

_SHANGHAI = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class MinuteParameterProjection:
    cutoff: datetime
    raw_sequence: int | None
    input_batch_ids: tuple[str, ...]
    minutes: pd.DataFrame
    historical_minutes: pd.DataFrame
    candidates: tuple[MinuteParameterCandidate, ...]
    quality: BatchQualityStatus


def parameter_archive_projections(parameters: MinuteParameterSet, *,
    materials: tuple[MinuteReplayMaterial, ...], tick_times: tuple[datetime, ...],
    warmup_available_at: datetime,
) -> tuple[MinuteParameterProjection, ...]:
    """Build only recorded clock prefixes; physical authority is verified separately."""
    by_path = {item.relative_path: item for item in materials}
    warmup = by_path["warmup.parquet"]
    historical = pd.read_parquet(BytesIO(warmup.payload()))
    if "available_at" not in historical:
        historical["available_at"] = warmup_available_at
    envelopes, frames = [], []
    for path in sorted(by_path):
        if not path.startswith("market/batches/market_minute/") or not path.endswith(".json"):
            continue
        envelope = BatchEnvelope.model_validate_json(by_path[path].payload())
        payload = by_path[path.removesuffix(".json")+".payload"]
        if payload.content_sha256 != envelope.content_sha256:
            raise PermissionError("parameter projection raw bytes differ from the original envelope")
        frame = MarketMinuteGateway.decode_payload(payload.payload())
        if len(frame) != envelope.row_count:
            raise PermissionError("parameter projection raw rows differ")
        frame["available_at"] = envelope.available_at
        frame["_source_sequence"] = envelope.sequence
        envelopes.append(envelope)
        frames.append(frame)
    if not envelopes or tuple(item.sequence for item in envelopes) != tuple(range(len(envelopes))):
        raise PermissionError("parameter projection lacks the complete original prefix")
    if not {item.available_at for item in envelopes}.issubset(tick_times):
        raise PermissionError("parameter clock omits an original raw publication observation")
    candidates = []
    for item in materials:
        if not item.relative_path.startswith("candidates/generations/"):
            continue
        snapshot = StrategyCandidateSnapshot.model_validate_json(item.payload())
        if snapshot.authority_binding is None or snapshot.authority_binding.strategy_id != parameters.definition_id or snapshot.authority_binding.strategy_version != "1":
            raise PermissionError("parameter candidate archive belongs to another complete definition")
        for record in snapshot.rows:
            raw = record.static_features.get(PARAMETER_CANDIDATE_FEATURE)
            if not isinstance(raw, str):
                raise PermissionError("parameter archive lacks a full candidate fact record")
            candidate = MinuteParameterCandidate.model_validate(strict_json_loads(raw))
            if (candidate.parameter_hash, candidate.family, candidate.ts_code, candidate.trade_date,
                    candidate.reference_date, candidate.available_at) != (
                    parameters.fingerprint, parameters.parameters.family, record.candidate_id,
                    record.effective_trade_date, record.reference_trade_date, record.available_at):
                raise PermissionError("parameter candidate full JSON differs from the original record")
            candidates.append(candidate)
    points = []
    for index, envelope in enumerate(envelopes):
        cutoff = envelope.available_at
        day = cutoff.astimezone(_SHANGHAI).date()
        visible_frames = [frame for original, frame in zip(envelopes[:index+1], frames[:index+1], strict=True)
            if original.available_at <= cutoff and original.quality_status is not BatchQualityStatus.STALE]
        visible = pd.concat(visible_frames, ignore_index=True) if visible_frames else frames[0].iloc[:0].copy()
        visible = visible.sort_values(["ts_code", "trade_time", "_source_sequence"], kind="stable").drop_duplicates(
            ["ts_code", "trade_time"], keep="last").drop(columns="_source_sequence")
        days = pd.to_datetime(visible.trade_time, utc=True).dt.tz_convert(_SHANGHAI).dt.date
        current = visible.loc[days == day].copy()
        history = pd.concat([historical, visible.loc[days < day]], ignore_index=True)
        by_code = {}
        for candidate in sorted(candidates, key=lambda row: (row.trade_date, row.available_at, row.ts_code)):
            if candidate.trade_date <= day and candidate.available_at <= cutoff:
                by_code[candidate.ts_code] = candidate
        current_codes = set(current.ts_code)
        selected = tuple(by_code[code] for code in sorted(set(by_code) & current_codes))
        parents = tuple(sorted({warmup.content_sha256, *(original.batch_id for original in envelopes[:index+1]
            if original.available_at <= cutoff)}))
        points.append(MinuteParameterProjection(cutoff, envelope.sequence, parents, current, history, selected,
            envelope.quality_status))
    for cutoff in tick_times:
        if cutoff.astimezone(_SHANGHAI).time().replace(tzinfo=None) != time(15) or any(point.cutoff == cutoff for point in points):
            continue
        earlier = [point for point in points if point.cutoff < cutoff and point.cutoff.astimezone(_SHANGHAI).date() == cutoff.astimezone(_SHANGHAI).date()]
        if not earlier:
            continue
        last = earlier[-1]
        from rquant.runtime_contracts import canonical_sha256
        parents = tuple(sorted((*last.input_batch_ids, canonical_sha256({"parameter_close_observation": cutoff}))))
        points.append(MinuteParameterProjection(cutoff, None, parents, last.minutes, last.historical_minutes,
            last.candidates, last.quality))
    return tuple(sorted(points, key=lambda point: (point.cutoff, -1 if point.raw_sequence is None else point.raw_sequence)))


@dataclass(frozen=True, slots=True)
class _ParameterWorkEntry:
    payload: str
    payload_sha256: str
    guard: ExecutableDependencyGuard


_PARAMETER_WORK_STATE: ContextVar[dict[str, _ParameterWorkEntry] | None] = ContextVar(
    "minute_parameter_request_pure_work", default=None)
_MAX_WORK_DESCRIPTIONS = 8


@contextmanager
def minute_parameter_work_scope() -> Iterator[None]:
    if _PARAMETER_WORK_STATE.get() is not None:
        yield
        return
    state: dict[str, _ParameterWorkEntry] = {}
    token = _PARAMETER_WORK_STATE.set(state)
    try:
        yield
    finally:
        state.clear()
        _PARAMETER_WORK_STATE.reset(token)


def measure_minute_parameter_work(parameters: MinuteParameterSet, *, materials: tuple[MinuteReplayMaterial, ...],
    tick_times: tuple[datetime, ...], warmup_available_at: datetime, runtime_work: MinuteReplayWork,
    session_facts: tuple[MinuteParameterSessionFacts, ...],
    study_binding: MinuteParameterStudyBinding | None = None,
) -> MinuteParameterWork:
    arguments = dict(parameters=parameters, materials=materials, tick_times=tick_times,
        warmup_available_at=warmup_available_at, runtime_work=runtime_work,
        session_facts=session_facts, study_binding=study_binding)
    state = _PARAMETER_WORK_STATE.get()
    if state is None:
        return _measure_minute_parameter_work(**arguments)
    # Complete archive bytes and the full recipe/three-part study are the input.
    # No source path, receipt or claimed input hash can authorize a cache hit.
    key = canonical_sha256({"contract": "minute-parameter-pure-work/v1",
        "parameters": parameters.model_dump(mode="json"),
        "materials": tuple(item.model_dump(mode="json") for item in materials),
        "tick_times": tick_times, "warmup_available_at": warmup_available_at,
        "runtime_work": runtime_work.model_dump(mode="json"),
        "session_facts": tuple(item.model_dump(mode="json") for item in session_facts),
        "study_binding": None if study_binding is None else study_binding.model_dump(mode="json")})
    entry = state.get(key)
    if entry is None:
        from rquant.minute_backtest_parameter_study_features import project_minute_parameter_study_decisions

        guard = capture_executable_dependency_guard(tuple(ExecutableBinding.from_callable(root) for root in (
            _measure_minute_parameter_work, parameter_archive_projections,
            project_minute_parameter_study_decisions)), contract="minute-parameter-pure-work-dependencies/v1")
        value = _measure_minute_parameter_work(**arguments)
        guard.assert_unchanged()
        payload = value.model_dump_json(exclude_computed_fields=True)
        entry = _ParameterWorkEntry(payload=payload,
            payload_sha256=hashlib.sha256(payload.encode()).hexdigest(), guard=guard)
        if len(state) < _MAX_WORK_DESCRIPTIONS:
            state[key] = entry
    else:
        entry.guard.assert_unchanged()
    if hashlib.sha256(entry.payload.encode()).hexdigest() != entry.payload_sha256:
        raise ExecutableDependencyError("minute parameter pure work payload changed")
    value = MinuteParameterWork.model_validate_json(entry.payload)
    entry.guard.assert_unchanged()
    return value


def _measure_minute_parameter_work(parameters: MinuteParameterSet, *, materials: tuple[MinuteReplayMaterial, ...],
    tick_times: tuple[datetime, ...], warmup_available_at: datetime, runtime_work: MinuteReplayWork,
    session_facts: tuple[MinuteParameterSessionFacts, ...],
    study_binding: MinuteParameterStudyBinding | None = None,
) -> MinuteParameterWork:
    points = parameter_archive_projections(parameters, materials=materials, tick_times=tick_times,
        warmup_available_at=warmup_available_at)
    if any(candidate.study_binding != study_binding for point in points for candidate in point.candidates):
        raise PermissionError("parameter candidate archive differs from the complete runtime study")
    prefix = sum(len(point.minutes) * len(point.candidates) for point in points)
    history = sum(len(point.historical_minutes) * len(point.candidates) for point in points)
    derived = sum(len(point.candidates) for point in points)
    # Every possible original occurrence may still hold. Count its entire visible
    # post-entry minute scan, including occurrences that the entry gate rejects.
    occurrences = sum(len(StrategyCandidateSnapshot.model_validate_json(item.payload()).rows)
        for item in materials if item.relative_path.startswith("candidates/generations/"))
    lifecycle = occurrences * sum(len(point.minutes) + max(0, len(point.historical_minutes)-runtime_work.warmup_rows)
        for point in points)
    study_work = {}
    if study_binding is not None:
        from rquant.topn_selection import resolve_score_profiles
        from rquant.minute_backtest_parameter_study_features import project_minute_parameter_study_decisions

        profile = resolve_score_profiles([study_binding.protocol.score_profile])[0]
        feature_count = len(profile.terms) + (1 if profile.env_gate is not None else 0)
        # The complete repeated prefix includes validation, causal entry readiness
        # and scoring for every possible new occurrence. Holdings never reduce this bound.
        study_work = {"study_prefix_rows": prefix * 2, "study_history_rows": history * 2,
            "study_feature_rows": derived * feature_count,
            "study_selection_rows": sum(len(point.candidates)**2 * 3 for point in points)}
        for point in points:
            project_minute_parameter_study_decisions(study_binding, candidates=point.candidates,
                minutes=point.minutes, historical_minutes=point.historical_minutes,
                source_frequency=parameters.parameters.freq, decision_cutoff=point.cutoff,
                new_entry_codes=tuple(sorted(candidate.ts_code for candidate in point.candidates
                    if candidate.trade_date == point.cutoff.astimezone(_SHANGHAI).date())))
    return MinuteParameterWork(runtime_work=runtime_work, prefix_rows=prefix, history_rows=history,
        derived_rows=derived, lifecycle_rows=lifecycle, session_fact_rows=len(session_facts), **study_work)


def restore_minute_parameter_source(
    value: FrozenMinuteParameterInput, *, expected: MinuteParameterRuntimeReceipt,
    research_root: Path,
) -> RestoredMinuteRuntimeSource:
    value = FrozenMinuteParameterInput.model_validate(value.model_dump(mode="python"))
    expected = MinuteParameterRuntimeReceipt.model_validate(expected.model_dump(mode="python"))
    expected.verify(value)
    source = _restore_minute_replay_archive(value, expected_work=expected.frozen.work, research_root=research_root)
    measured = measure_minute_parameter_work(value.parameters, materials=value.materials, tick_times=value.tick_times,
        warmup_available_at=value.warmup_available_at, runtime_work=source.work, session_facts=value.session_facts,
        study_binding=value.study_binding)
    if measured != value.parameter_work:
        raise PermissionError("parameter projection work differs from the complete independent source receipt")
    return source


def read_minute_parameter_input_table(connection: duckdb.DuckDBPyConnection) -> FrozenMinuteParameterResearchInput:
    if connection.execute("SHOW TABLES").fetchall() != [(PARAMETER_SOURCE_TABLE,)]:
        raise PermissionError("parameter source requires exactly its complete input table")
    schema = connection.execute("PRAGMA table_info('minute_parameter_replay_input')").fetchall()
    if [(row[1], row[2], row[3], row[5]) for row in schema] != [
        ("input_hash", "VARCHAR", True, True), ("payload", "VARCHAR", True, False)]:
        raise PermissionError("parameter source requires exact PK and NOT NULL columns")
    rows = connection.execute("SELECT input_hash, payload FROM minute_parameter_replay_input").fetchmany(2)
    if len(rows) != 1 or not isinstance(rows[0][1], str) or len(rows[0][1].encode("utf-8")) > MAX_INPUT_BYTES:
        raise PermissionError("parameter source requires one bounded complete input")
    strict_json_loads(rows[0][1])
    value = FrozenMinuteParameterResearchInput.model_validate_json(rows[0][1])
    if rows[0][0] != value.full_input_hash:
        raise PermissionError("parameter source row identity differs from the complete payload")
    return value


def write_minute_parameter_input_table(
    connection: duckdb.DuckDBPyConnection, value: FrozenMinuteParameterResearchInput,
) -> None:
    checked = FrozenMinuteParameterResearchInput.model_validate(value.model_dump(mode="python"))
    if connection.execute("SHOW TABLES").fetchall():
        raise PermissionError("parameter publication requires a new empty source")
    connection.execute("CREATE TABLE minute_parameter_replay_input (input_hash VARCHAR PRIMARY KEY, payload VARCHAR NOT NULL)")
    connection.execute("INSERT INTO minute_parameter_replay_input VALUES (?, ?)",
        [checked.full_input_hash, checked.model_dump_json()])
