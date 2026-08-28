"""Lightweight one-generation query boundary for Serving-only pages."""

from __future__ import annotations

import json
import re
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType, TracebackType
from typing import Any, Literal, Protocol, Self

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.serving_contracts import (
    FreshnessStatus,
    ServingGenerationManifest,
)
from rquant.serving_publisher import (
    ServingGenerationLease,
    ServingIntegrityError,
    ServingReader,
)

_MAX_QUERY_BYTES = 32 * 1024
_MAX_QUERY_PARAMETERS = 512
_DEFAULT_RESULT_BYTES = 8 * 1024 * 1024
_MAX_RESULT_BYTES = 64 * 1024 * 1024
_FORBIDDEN_QUERY_TOKEN = re.compile(
    r"\b(?:ATTACH|COPY|EXPORT|IMPORT|INSTALL|LOAD|PRAGMA|CALL|CREATE|DELETE|DROP|INSERT|"
    r"MERGE|UPDATE|ALTER|VACUUM|CHECKPOINT|READ_CSV|READ_JSON|READ_PARQUET|PARQUET_SCAN|"
    r"SQLITE_SCAN)\b",
    flags=re.IGNORECASE,
)


class ServingFreshness(StrEnum):
    FRESH = "fresh"
    STALE = "stale"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class ServingFrameState(StrEnum):
    READY = "ready"
    STALE = "stale"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class ServingFrameResult(RuntimeContractModel):
    source: Literal["serving"] = "serving"
    state: ServingFrameState
    detail: str
    generation_id: str | None = None
    generated_at: AwareUtcDatetime | None = None
    columns: tuple[str, ...] = ()
    rows: tuple[tuple[Any, ...], ...] = ()

    def dataframe(self) -> object:
        import pandas as pd

        frame = pd.DataFrame(self.rows, columns=self.columns)
        frame.attrs["serving"] = {
            "source": self.source,
            "state": self.state.value,
            "detail": self.detail,
            "generation_id": self.generation_id,
            "generated_at": self.generated_at,
        }
        return frame


class _QueryResult(Protocol):
    def fetchall(self) -> list[tuple[object, ...]]: ...

    def fetchmany(self, size: int) -> list[tuple[object, ...]]: ...

    @property
    def description(self) -> Sequence[Sequence[object]]: ...


class _ReadonlyConnection(Protocol):
    def execute(self, sql: str, parameters: tuple[object, ...]) -> _QueryResult: ...


@dataclass(frozen=True)
class _ProjectionEvidence:
    available_at: Mapping[str, datetime]
    owner_dataset_ids: frozenset[str]


def _error_detail(error: Exception) -> str:
    message = " ".join(str(error).split())
    return f"{type(error).__name__}: {message or 'serving read failed'}"[:300]


def _validate_bounded_select(sql: str, parameters: Sequence[object]) -> str:
    statement = sql.strip()
    if not statement:
        raise ValueError("sql cannot be empty")
    if len(statement.encode("utf-8")) > _MAX_QUERY_BYTES:
        raise ValueError("serving query exceeds its SQL byte budget")
    if len(parameters) > _MAX_QUERY_PARAMETERS:
        raise ValueError("serving query exceeds its parameter budget")
    without_trailing = statement[:-1].rstrip() if statement.endswith(";") else statement
    if ";" in without_trailing:
        raise ServingIntegrityError("serving queries must contain one read-only SELECT")
    first_token = without_trailing.split(None, 1)[0].upper()
    if first_token not in {"SELECT", "WITH"} or _FORBIDDEN_QUERY_TOKEN.search(without_trailing):
        raise ServingIntegrityError("serving queries must be a bounded read-only SELECT")
    return without_trailing


def require_serving_projections(
    connection: _ReadonlyConnection,
    required_projections: Sequence[str],
) -> _ProjectionEvidence:
    """Bind projection availability and owner watermarks to one connection."""

    names = tuple(dict.fromkeys(required_projections))
    if not names:
        return _ProjectionEvidence(MappingProxyType({}), frozenset())
    if len(names) > 64 or any(not name or len(name) > 100 for name in names):
        raise ValueError("required serving projections are invalid")
    placeholders = ",".join("?" for _ in names)
    rows = connection.execute(
        "SELECT table_name, available, reason, owner_dataset_id, available_at "
        f"FROM projection_status WHERE table_name IN ({placeholders})",
        names,
    ).fetchall()
    by_name = {str(row[0]): (bool(row[1]), row[2], row[3], row[4]) for row in rows}
    unavailable: list[str] = []
    available_at: dict[str, datetime] = {}
    owner_dataset_ids: set[str] = set()
    for name in names:
        state = by_name.get(name)
        if state is None:
            unavailable.append(f"{name}: status missing")
        elif not state[0]:
            reason = str(state[1] or "projection unavailable").replace("_", " ")
            unavailable.append(f"{name}: {reason}")
        elif not state[2]:
            unavailable.append(f"{name}: owner dataset missing")
        elif state[3] is None:
            unavailable.append(f"{name}: availability timestamp missing")
        else:
            owner_dataset_ids.add(str(state[2]))
            available_at[name] = normalize_aware_utc(state[3])
    if unavailable:
        raise ServingIntegrityError("serving projection not published: " + "; ".join(unavailable))
    return _ProjectionEvidence(
        available_at=MappingProxyType(available_at),
        owner_dataset_ids=frozenset(owner_dataset_ids),
    )


def manifest_freshness(
    manifest: ServingGenerationManifest,
    *,
    age: timedelta,
    stale_after: timedelta,
    dataset_ids: frozenset[str] | None = None,
) -> ServingFreshness:
    statuses = {
        watermark.status
        for watermark in manifest.watermarks
        if dataset_ids is None or watermark.dataset_id in dataset_ids
    }
    if FreshnessStatus.UNAVAILABLE in statuses or FreshnessStatus.DEGRADED in statuses:
        return ServingFreshness.DEGRADED
    if age > stale_after or FreshnessStatus.STALE in statuses:
        return ServingFreshness.STALE
    return ServingFreshness.FRESH


def query_acquired_serving_frame(
    acquired: ServingGenerationLease,
    sql: str,
    parameters: Sequence[object] = (),
    *,
    now: datetime | None = None,
    max_rows: int = 10_000,
    max_result_bytes: int = _DEFAULT_RESULT_BYTES,
    max_query_seconds: float = 2.0,
    stale_after: timedelta = timedelta(minutes=10),
    required_projections: Sequence[str] = (),
) -> ServingFrameResult:
    """Run one bounded query without re-reading the generation pointer."""

    if type(max_rows) is not int or not 1 <= max_rows <= 100_000:
        raise ValueError("max_rows must be an integer between 1 and 100000")
    if type(max_result_bytes) is not int or not 1 <= max_result_bytes <= _MAX_RESULT_BYTES:
        raise ValueError("max_result_bytes must be between 1 and 67108864")
    if (
        isinstance(max_query_seconds, bool)
        or not isinstance(max_query_seconds, (int, float))
        or not 0.001 <= float(max_query_seconds) <= 30.0
    ):
        raise ValueError("max_query_seconds must be between 0.001 and 30")
    if stale_after <= timedelta(0):
        raise ValueError("stale_after must be positive")
    observed_at = normalize_aware_utc(now or datetime.now(UTC))
    manifest: ServingGenerationManifest | None = None
    try:
        statement = _validate_bounded_select(sql, parameters)
        if acquired.closed:
            raise ServingIntegrityError("serving generation lease is closed")
        manifest = acquired.manifest
        if manifest.built_at > observed_at:
            raise ServingIntegrityError("serving generation contains future evidence")
        projection_evidence = require_serving_projections(
            acquired.connection,
            required_projections,
        )
        timer = threading.Timer(float(max_query_seconds), acquired.connection.interrupt)
        timer.daemon = True
        timer.start()
        try:
            result = acquired.connection.execute(statement, tuple(parameters))
            rows = result.fetchmany(max_rows + 1)
        finally:
            timer.cancel()
            timer.join()
        if len(rows) > max_rows:
            raise ServingIntegrityError("serving query exceeded its row budget")
        result_bytes = len(
            json.dumps(rows, ensure_ascii=True, default=str, separators=(",", ":")).encode()
        )
        if result_bytes > max_result_bytes:
            raise ServingIntegrityError("serving query exceeded its result byte budget")
        columns = tuple(str(column[0]) for column in result.description)
        required_dataset_ids = projection_evidence.owner_dataset_ids
        relevant_watermarks = tuple(
            item
            for item in manifest.watermarks
            if not required_dataset_ids or item.dataset_id in required_dataset_ids
        )
        missing_watermarks = required_dataset_ids.difference(
            item.dataset_id for item in relevant_watermarks
        )
        if missing_watermarks:
            raise ServingIntegrityError(
                "serving generation lacks projection owner watermarks: "
                + ", ".join(sorted(missing_watermarks))
            )
        freshness = manifest_freshness(
            manifest,
            age=max(observed_at - manifest.built_at, timedelta(0)),
            stale_after=stale_after,
            dataset_ids=required_dataset_ids or None,
        )
        future_projections = tuple(
            sorted(
                name
                for name, available_at in projection_evidence.available_at.items()
                if available_at > observed_at
            )
        )
        if future_projections:
            raise ServingIntegrityError(
                "serving projections contain future evidence: " + ", ".join(future_projections)
            )
        stale_projections = tuple(
            sorted(
                name
                for name, available_at in projection_evidence.available_at.items()
                if observed_at - available_at > stale_after
            )
        )
        if freshness is ServingFreshness.FRESH and stale_projections:
            freshness = ServingFreshness.STALE
        state = {
            ServingFreshness.FRESH: ServingFrameState.READY,
            ServingFreshness.STALE: ServingFrameState.STALE,
            ServingFreshness.DEGRADED: ServingFrameState.DEGRADED,
            ServingFreshness.UNAVAILABLE: ServingFrameState.DEGRADED,
        }[freshness]
        non_fresh = tuple(
            f"{item.dataset_id}:{item.status.value}:{item.reason or 'unspecified'}"
            for item in relevant_watermarks
            if item.status is not FreshnessStatus.FRESH
        )
        detail = (
            "serving generation verified"
            if state is ServingFrameState.READY
            else f"serving generation {state.value}: "
            + (
                "; ".join(non_fresh)
                or (
                    "stale projections: " + ", ".join(stale_projections)
                    if stale_projections
                    else "built_at exceeded freshness budget"
                )
            )
        )
        return ServingFrameResult(
            state=state,
            detail=detail,
            generation_id=manifest.generation_id,
            generated_at=manifest.built_at,
            columns=columns,
            rows=tuple(tuple(row) for row in rows),
        )
    except Exception as error:
        return ServingFrameResult(
            state=ServingFrameState.UNAVAILABLE,
            detail=_error_detail(error),
            generation_id=None if manifest is None else manifest.generation_id,
            generated_at=None if manifest is None else manifest.built_at,
        )


def query_serving_frame(
    serving_root: str | Path,
    sql: str,
    parameters: Sequence[object] = (),
    *,
    now: datetime | None = None,
    max_rows: int = 10_000,
    max_result_bytes: int = _DEFAULT_RESULT_BYTES,
    max_query_seconds: float = 2.0,
    stale_after: timedelta = timedelta(minutes=10),
    required_projections: Sequence[str] = (),
) -> ServingFrameResult:
    """Acquire one generation and run one bounded query against it."""

    try:
        with ServingReader(serving_root).acquire_generation() as acquired:
            return query_acquired_serving_frame(
                acquired,
                sql,
                parameters,
                now=now,
                max_rows=max_rows,
                max_result_bytes=max_result_bytes,
                max_query_seconds=max_query_seconds,
                stale_after=stale_after,
                required_projections=required_projections,
            )
    except Exception as error:
        return ServingFrameResult(
            state=ServingFrameState.UNAVAILABLE,
            detail=_error_detail(error),
        )


class ServingOnlyRenderContext:
    """Hold one verified immutable Serving lease for a complete page render."""

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
        observed_at = normalize_aware_utc(now or datetime.now(UTC))
        return cls(
            lease=ServingReader(serving_root).acquire_generation(),
            observed_at=observed_at,
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
        max_result_bytes: int = _DEFAULT_RESULT_BYTES,
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


__all__ = [
    "ServingFrameResult",
    "ServingFrameState",
    "ServingFreshness",
    "ServingOnlyRenderContext",
    "manifest_freshness",
    "query_acquired_serving_frame",
    "query_serving_frame",
    "require_serving_projections",
]
