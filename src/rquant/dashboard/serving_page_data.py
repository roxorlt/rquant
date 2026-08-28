"""One-generation read boundary shared by a complete page render."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Generic, Literal, Self, TypeVar

import pandas as pd

from rquant.dashboard.serving_only_page_data import (
    ServingFrameResult,
    ServingFrameState,
    query_acquired_serving_frame,
)
from rquant.research_gate import (
    ResearchGateDecision,
    ResearchGateFailure,
    ResearchGateRequest,
)
from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_publisher import ServingGenerationLease, ServingReader
from rquant.serving_read_models import (
    PAGE_PROJECTION_CONTRACTS,
    NlScreenPage,
    NlScreenPageError,
    decode_nl_screen_cursor,
    nl_screen_query_digest,
    paginate_nl_screen_projection,
)

_ValueT = TypeVar("_ValueT")
NlScreenPageNavigation = Literal["replace", "next", "previous"]
NL_SCREEN_PAGE_RERUN_REQUIRED = "分页状态已失效，请重新运行筛选。"


@dataclass(frozen=True)
class ServingPageResult(Generic[_ValueT]):
    """One convenience value with its immutable Serving health evidence."""

    state: ServingFrameState
    detail: str
    generation_id: str | None
    generated_at: datetime | None
    value: _ValueT


def _page_result(result: ServingFrameResult, value: _ValueT) -> ServingPageResult[_ValueT]:
    return ServingPageResult(
        state=result.state,
        detail=result.detail,
        generation_id=result.generation_id,
        generated_at=result.generated_at,
        value=value,
    )


def reset_nl_screen_page_session(
    state: MutableMapping[str, object],
    *,
    cursor_signing_key: bytes,
    plan_digest: str | None,
) -> None:
    if not isinstance(cursor_signing_key, bytes) or len(cursor_signing_key) != 32:
        raise ValueError("nl screen cursor signing key must be exactly 32 bytes")
    state.update(
        {
            "nl_result_df": None,
            "nl_diagnostics": [],
            "nl_current_cursor": None,
            "nl_start_cursor": None,
            "nl_next_cursor": None,
            "nl_cursor_history": [],
            "nl_page_error": None,
            "nl_cursor_signing_key": cursor_signing_key,
            "nl_plan_digest": plan_digest,
        }
    )


def bind_nl_screen_plan_session(
    state: MutableMapping[str, object],
    *,
    plan_digest: str,
    cursor_signing_key_factory: Callable[[], bytes],
) -> bool:
    if state.get("nl_plan_digest") == plan_digest:
        return False
    reset_nl_screen_page_session(
        state,
        cursor_signing_key=cursor_signing_key_factory(),
        plan_digest=plan_digest,
    )
    return True


def load_nl_screen_page_session(
    state: MutableMapping[str, object],
    *,
    load_page: Callable[[], ServingPageResult[NlScreenPage]],
    navigation: NlScreenPageNavigation,
) -> ServingPageResult[NlScreenPage] | None:
    history_value = state.get("nl_cursor_history", [])
    if not isinstance(history_value, list) or not all(
        isinstance(cursor, str) for cursor in history_value
    ):
        raise ValueError("nl screen cursor history is invalid")
    history = list(history_value)
    if navigation == "replace":
        next_history: list[str] = []
    elif navigation == "next":
        current = state.get("nl_current_cursor")
        if not isinstance(current, str) or not current:
            raise ValueError("nl screen current cursor is invalid")
        next_history = [*history, current]
    elif navigation == "previous":
        if not history:
            raise ValueError("nl screen cursor history is empty")
        next_history = history[:-1]
    else:
        raise ValueError("nl screen page navigation is invalid")

    try:
        result = load_page()
    except NlScreenPageError:
        state["nl_page_error"] = NL_SCREEN_PAGE_RERUN_REQUIRED
        return None

    page = result.value
    state.update(
        {
            "nl_result_df": page.rows,
            "nl_diagnostics": page.diagnostics,
            "nl_current_cursor": page.start_cursor,
            "nl_start_cursor": page.start_cursor,
            "nl_next_cursor": page.next_cursor,
            "nl_cursor_history": next_history,
            "nl_page_error": None,
        }
    )
    return result


class ServingPageRenderContext:
    """Hold one verified Serving lease until one page render finishes."""

    def __init__(
        self,
        *,
        lease: ServingGenerationLease,
        observed_at: datetime,
        stale_after: timedelta,
    ) -> None:
        self._lease = lease
        self._observed_at = normalize_aware_utc(observed_at)
        if stale_after <= timedelta(0):
            lease.close()
            raise ValueError("stale_after must be positive")
        self._stale_after = stale_after

    @classmethod
    def open(
        cls,
        serving_root: str | Path,
        *,
        now: datetime | None = None,
        stale_after: timedelta = timedelta(minutes=10),
    ) -> Self:
        observed = normalize_aware_utc(now or datetime.now(UTC))
        return cls(
            lease=ServingReader(serving_root).acquire_generation(),
            observed_at=observed,
            stale_after=stale_after,
        )

    @property
    def generation_id(self) -> str:
        return self._lease.manifest.generation_id

    @property
    def closed(self) -> bool:
        return self._lease.closed

    def __enter__(self) -> Self:
        if self.closed:
            raise RuntimeError("page render context is closed")
        return self

    def __exit__(
        self,
        _error_type: type[BaseException] | None,
        _error: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._lease.close()

    def query(
        self,
        sql: str,
        parameters: Sequence[object] = (),
        *,
        max_rows: int = 10_000,
        max_result_bytes: int = 8 * 1024 * 1024,
        max_query_seconds: float = 2.0,
        required_projections: Sequence[str] = (),
    ) -> ServingFrameResult:
        return query_acquired_serving_frame(
            self._lease,
            sql,
            parameters,
            now=self._observed_at,
            max_rows=max_rows,
            max_result_bytes=max_result_bytes,
            max_query_seconds=max_query_seconds,
            stale_after=self._stale_after,
            required_projections=required_projections,
        )

    def dataframe(
        self,
        sql: str,
        parameters: Sequence[object] = (),
        **query_options: object,
    ) -> ServingPageResult[pd.DataFrame | None]:
        result = self.query(sql, parameters, **query_options)
        if result.state is ServingFrameState.UNAVAILABLE:
            return _page_result(result, None)
        frame = result.dataframe()
        assert isinstance(frame, pd.DataFrame)
        return _page_result(result, frame)

    def trading_calendar(self) -> ServingPageResult[tuple[date, ...]]:
        result = self.query(
            "SELECT trade_date FROM trade_calendar WHERE is_open ORDER BY trade_date",
            max_rows=5_000,
            max_result_bytes=256 * 1024,
            required_projections=("trade_calendar",),
        )
        if result.state is ServingFrameState.UNAVAILABLE:
            return _page_result(result, ())
        return _page_result(
            result,
            tuple(item[0].date() if hasattr(item[0], "date") else item[0] for item in result.rows),
        )

    def screen_bounds(
        self,
        preset_name: str,
    ) -> ServingPageResult[tuple[date | None, date | None, int]]:
        names = (
            ("n-shape-pool1", "n-shape-pool2")
            if preset_name == "n-shape-combined"
            else (preset_name,)
        )
        marks = ",".join("?" for _ in names)
        result = self.query(
            f"""
            SELECT MIN(min_date), MAX(max_date), SUM(candidate_count)
            FROM screen_bounds
            WHERE preset_name IN ({marks})
            """,
            names,
            max_rows=1,
            max_result_bytes=4 * 1024,
            required_projections=("screen_bounds",),
        )
        if result.state is ServingFrameState.UNAVAILABLE or not result.rows:
            return _page_result(result, (None, None, 0))
        minimum, maximum, candidates = result.rows[0]
        if minimum is None:
            return _page_result(result, (None, None, 0))
        return _page_result(
            result,
            (
                minimum.date() if hasattr(minimum, "date") else minimum,
                maximum.date() if hasattr(maximum, "date") else maximum,
                int(candidates or 0),
            ),
        )

    def minute_coverage(self) -> ServingPageResult[pd.DataFrame | None]:
        return self.dataframe(
            """
            SELECT source, rows_count, codes_count, trade_dates, min_time, max_time
            FROM minute_coverage
            ORDER BY is_total DESC, rows_count DESC, source
            """,
            max_rows=128,
            max_result_bytes=128 * 1024,
            required_projections=("minute_coverage",),
        )

    def canvas_latest_trade_date(self) -> ServingPageResult[str | None]:
        result = self.query(
            """
            SELECT trade_date FROM canvas_latest_trade_date
            WHERE snapshot_key = 'current'
            LIMIT 1
            """,
            max_rows=1,
            max_result_bytes=1024,
            required_projections=("canvas_latest_trade_date",),
        )
        if result.state is ServingFrameState.UNAVAILABLE or not result.rows:
            return _page_result(result, None)
        value = result.rows[0][0]
        return _page_result(result, None if value is None else value.isoformat())

    def canvas_definitions(self) -> ServingPageResult[pd.DataFrame | None]:
        return self.dataframe(
            """
            SELECT name, description, pool_refs_json, created_at, updated_at, source,
                   command_id, command_hash, source_identity_hash, record_hash, version_hash
            FROM canvas_definition
            ORDER BY name ASC
            LIMIT 512
            """,
            max_rows=512,
            max_result_bytes=2 * 1024 * 1024,
            required_projections=("canvas_definition",),
        )

    def canvas_diagnostic(
        self,
        preset_name: str,
        trade_date: str,
    ) -> ServingPageResult[tuple[pd.DataFrame, list[tuple[str, int]]]]:
        diagnostics = self.query(
            """
            SELECT rule_label, remaining_count
            FROM canvas_diagnostic
            WHERE preset_name = ? AND trade_date = CAST(? AS DATE)
            ORDER BY step_index
            """,
            (preset_name, trade_date),
            max_rows=1_000,
            max_result_bytes=128 * 1024,
            required_projections=("canvas_diagnostic", "canvas_hit"),
        )
        hits = self.query(
            """
            SELECT row_json FROM canvas_hit
            WHERE preset_name = ? AND trade_date = CAST(? AS DATE)
            ORDER BY ts_code
            LIMIT 20000
            """,
            (preset_name, trade_date),
            max_rows=20_000,
            max_result_bytes=6 * 1024 * 1024,
            required_projections=("canvas_diagnostic", "canvas_hit"),
        )
        if (
            diagnostics.state is ServingFrameState.UNAVAILABLE
            or hits.state is ServingFrameState.UNAVAILABLE
        ):
            selected = diagnostics if diagnostics.state is ServingFrameState.UNAVAILABLE else hits
            return _page_result(selected, (pd.DataFrame(), []))
        decoded = tuple(json.loads(str(row[0])) for row in hits.rows)
        severity = {
            ServingFrameState.READY: 0,
            ServingFrameState.STALE: 1,
            ServingFrameState.DEGRADED: 2,
            ServingFrameState.UNAVAILABLE: 3,
        }
        selected = max((diagnostics, hits), key=lambda item: severity[item.state])
        detail = "; ".join(
            dict.fromkeys(item.detail for item in (diagnostics, hits) if item.detail)
        )
        return ServingPageResult(
            state=selected.state,
            detail=detail,
            generation_id=selected.generation_id,
            generated_at=selected.generated_at,
            value=(
                pd.DataFrame(decoded),
                [(str(label), int(count)) for label, count in diagnostics.rows],
            ),
        )

    def research_gate(
        self,
        request: ResearchGateRequest,
    ) -> ServingPageResult[ResearchGateDecision]:
        result = self.query(
            """
            SELECT audit_run_id, dataset_snapshot_id, dataset_binding_hash,
                   coverage_ratios_json, coverage_counts_json, failures_json,
                   metadata_ready
            FROM research_gate_metadata
            WHERE strategy_name = ?
              AND range_start <= ? AND range_end >= ?
              AND code_commit = ?
            ORDER BY as_of_time DESC, completed_at DESC
            LIMIT 1
            """,
            (
                request.strategy_name,
                request.start_date,
                request.end_date,
                request.code_commit or "",
            ),
            max_rows=1,
            max_result_bytes=256 * 1024,
            required_projections=("research_gate_metadata",),
        )
        if result.state is ServingFrameState.UNAVAILABLE or not result.rows:
            return _page_result(result, self.unavailable_research_gate(request))
        (
            audit_run_id,
            snapshot_id,
            binding_hash,
            ratios_json,
            counts_json,
            failures_json,
            metadata_ready,
        ) = result.rows[0]
        failures = tuple(
            ResearchGateFailure.model_validate(item) for item in json.loads(str(failures_json))
        )
        ready = bool(metadata_ready) and not failures
        return _page_result(
            result,
            ResearchGateDecision(
                allowed=request.mode == "exploratory" or ready,
                research_status=(
                    "comparable" if request.mode == "formal" and ready else "exploratory"
                ),
                audit_run_id=None if audit_run_id is None else str(audit_run_id),
                dataset_snapshot_id=None if snapshot_id is None else str(snapshot_id),
                dataset_binding_hash=None if binding_hash is None else str(binding_hash),
                coverage_ratios={
                    str(key): (None if value is None else float(value))
                    for key, value in json.loads(str(ratios_json)).items()
                },
                coverage_counts={
                    str(key): (int(value[0]), int(value[1]))
                    for key, value in json.loads(str(counts_json)).items()
                },
                failures=failures,
            ),
        )

    @staticmethod
    def unavailable_research_gate(request: ResearchGateRequest) -> ResearchGateDecision:
        failure = ResearchGateFailure(
            code="metadata_unavailable",
            message="研究门 Serving 元数据不可用",
        )
        return ResearchGateDecision(
            allowed=request.mode == "exploratory",
            research_status="exploratory",
            audit_run_id=None,
            dataset_snapshot_id=None,
            dataset_binding_hash=None,
            coverage_ratios={},
            coverage_counts={},
            failures=(failure,),
        )


def read_nl_screen_page(
    serving_root: str | Path,
    *,
    trade_date: str,
    rules: Sequence[Callable[[pd.DataFrame], pd.Series]],
    rule_labels: Sequence[str],
    normalized_plan: Mapping[str, object],
    include_columns: Sequence[str] = (),
    page_size: int,
    signing_key: bytes,
    cursor: str | None = None,
    now: datetime | None = None,
    stale_after: timedelta = timedelta(minutes=10),
) -> ServingPageResult[NlScreenPage]:
    """Read one complete bounded NL universe from a pinned serving generation."""

    query_digest = nl_screen_query_digest(normalized_plan, include_columns)
    decoded = None if cursor is None else decode_nl_screen_cursor(cursor, signing_key=signing_key)
    if decoded is not None and decoded.query_digest != query_digest:
        raise NlScreenPageError("nl screen cursor requires rerun: query changed")
    observed_at = normalize_aware_utc(now or datetime.now(UTC))
    contract = PAGE_PROJECTION_CONTRACTS["nl_screen_universe"]
    reader = ServingReader(serving_root)
    try:
        lease = (
            reader.acquire_generation()
            if decoded is None
            else reader.acquire_historical_generation(decoded.generation_id)
        )
    except Exception as exc:
        raise NlScreenPageError(
            "nl screen cursor requires rerun: generation is unavailable"
        ) from exc
    try:
        with lease:
            result = query_acquired_serving_frame(
                lease,
                """
                SELECT * FROM nl_screen_universe
                WHERE trade_date = CAST(? AS DATE)
                ORDER BY trade_date, ts_code
                """,
                (trade_date,),
                now=observed_at,
                max_rows=contract.max_rows,
                max_result_bytes=contract.max_bytes,
                stale_after=stale_after,
                required_projections=("nl_screen_universe",),
            )
            if result.state is ServingFrameState.UNAVAILABLE:
                exceeded_budget = (
                    "exceeded its row budget" in result.detail
                    or "exceeded its result byte budget" in result.detail
                )
                if exceeded_budget:
                    raise NlScreenPageError(
                        "nl screen candidate universe exceeds registered serving budget"
                    )
                raise NlScreenPageError("nl screen serving generation is unavailable")
            universe = result.dataframe()
            assert isinstance(universe, pd.DataFrame)
            page = paginate_nl_screen_projection(
                universe,
                generation_id=lease.manifest.generation_id,
                trade_date=trade_date,
                rules=rules,
                rule_labels=rule_labels,
                normalized_plan=normalized_plan,
                include_columns=include_columns,
                page_size=page_size,
                signing_key=signing_key,
                cursor=cursor,
            )
    except NlScreenPageError:
        raise
    except Exception as exc:
        raise NlScreenPageError("nl screen serving generation is unavailable") from exc
    finally:
        lease.close()
    return _page_result(result, page)


__all__ = [
    "NL_SCREEN_PAGE_RERUN_REQUIRED",
    "ServingPageRenderContext",
    "ServingPageResult",
    "bind_nl_screen_plan_session",
    "load_nl_screen_page_session",
    "read_nl_screen_page",
    "reset_nl_screen_page_session",
]
