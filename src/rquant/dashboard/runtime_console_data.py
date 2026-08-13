"""Read-only, bounded data boundary for the runtime operations console."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, TypeVar

from pydantic import Field

from rquant.dashboard.serving_only_page_data import (
    ServingFrameResult,
    ServingFrameState,
    manifest_freshness,
    query_acquired_serving_frame,
    query_serving_frame,
    require_serving_projections,
)
from rquant.dashboard.serving_only_page_data import (
    ServingFreshness as ConsoleFreshness,
)
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, normalize_aware_utc
from rquant.serving_contracts import ServingGenerationManifest
from rquant.serving_publisher import (
    ServingReader,
    quote_serving_column_identifier,
    quote_serving_table_identifier,
)


class ConsoleLoadState(StrEnum):
    READY = "ready"
    DEGRADED = "degraded"


class ConsoleLimits(RuntimeContractModel):
    services: int = Field(default=100, ge=1, le=500)
    signals: int = Field(default=100, ge=1, le=500)
    deliveries: int = Field(default=100, ge=1, le=500)
    paper_accounts: int = Field(default=50, ge=1, le=500)
    paper_holdings: int = Field(default=200, ge=1, le=500)
    lab_jobs: int = Field(default=100, ge=1, le=500)
    promotions: int = Field(default=100, ge=1, le=500)


class RuntimeServiceRow(RuntimeContractModel):
    service_id: str
    plane: str
    status: str
    stale: bool
    observed_at: AwareUtcDatetime
    heartbeat_at: AwareUtcDatetime | None
    input_sequence: int
    output_sequence: int
    backlog_count: int = Field(ge=0)
    consecutive_failures: int = Field(ge=0)
    last_error: str | None


class SignalRow(RuntimeContractModel):
    global_sequence: int = Field(ge=1)
    signal_id: str
    strategy_id: str
    strategy_version: str
    candidate_id: str
    action: str
    available_at: AwareUtcDatetime
    expires_at: AwareUtcDatetime | None
    reason_codes_json: str


class DeliveryRow(RuntimeContractModel):
    outbox_id: str
    signal_id: str
    recipient_id: str
    channel: str
    status: str
    attempt_count: int = Field(ge=0)
    updated_at: AwareUtcDatetime
    last_error: str | None


class PaperAccountRow(RuntimeContractModel):
    account_id: str
    as_of_time: AwareUtcDatetime
    cash: Decimal
    available_cash: Decimal
    frozen_cash: Decimal
    nav: Decimal
    unrealized_pnl: Decimal
    realized_pnl: Decimal


class PaperHoldingRow(RuntimeContractModel):
    account_id: str
    ts_code: str
    quantity: Decimal
    available_quantity: Decimal
    frozen_quantity: Decimal
    average_cost: Decimal
    market_price: Decimal
    market_value: Decimal
    unrealized_pnl: Decimal
    as_of_time: AwareUtcDatetime


class LabJobRow(RuntimeContractModel):
    job_id: str
    strategy_name: str
    job_type: str
    resource_class: str
    status: str
    progress_fraction: float = Field(ge=0, le=1)
    phase: str
    terminal_shards: int = Field(ge=0)
    total_shards: int = Field(ge=0)
    eta_status: str | None
    eta_finish_low: AwareUtcDatetime | None
    eta_finish_center: AwareUtcDatetime | None
    eta_finish_high: AwareUtcDatetime | None
    updated_at: AwareUtcDatetime


class PromotionRow(RuntimeContractModel):
    decision_id: str
    stage: str
    approved: bool
    experiment_ids_json: str
    gate_failures_json: str
    decided_at: AwareUtcDatetime


class RuntimeConsoleSnapshot(RuntimeContractModel):
    state: ConsoleLoadState
    freshness: ConsoleFreshness
    detail: str
    generation_id: str | None = None
    generated_at: AwareUtcDatetime | None = None
    age_seconds: int | None = Field(default=None, ge=0)
    producer_commit: str | None = None
    services: tuple[RuntimeServiceRow, ...] = ()
    signals: tuple[SignalRow, ...] = ()
    deliveries: tuple[DeliveryRow, ...] = ()
    paper_accounts: tuple[PaperAccountRow, ...] = ()
    paper_holdings: tuple[PaperHoldingRow, ...] = ()
    lab_jobs: tuple[LabJobRow, ...] = ()
    promotions: tuple[PromotionRow, ...] = ()


class _QueryResult(Protocol):
    def fetchall(self) -> list[tuple[object, ...]]: ...

    def fetchmany(self, size: int) -> list[tuple[object, ...]]: ...

    @property
    def description(self) -> Sequence[Sequence[object]]: ...


class _ReadonlyConnection(Protocol):
    def execute(self, sql: str, parameters: tuple[object, ...]) -> _QueryResult: ...


class _QuerySpec(RuntimeContractModel):
    table: str
    columns: tuple[str, ...]
    order_by: tuple[str, ...]

    @property
    def sql(self) -> str:
        quoted_columns = ", ".join(
            quote_serving_column_identifier(column) for column in self.columns
        )
        quoted_order = ", ".join(self._order_term(term) for term in self.order_by)
        quoted_table = quote_serving_table_identifier(self.table)
        return f"SELECT {quoted_columns} FROM {quoted_table} ORDER BY {quoted_order} LIMIT ?"

    @staticmethod
    def _order_term(term: str) -> str:
        parts = term.split()
        if len(parts) == 1:
            return quote_serving_column_identifier(parts[0])
        if len(parts) == 2 and parts[1] in {"ASC", "DESC"}:
            return f"{quote_serving_column_identifier(parts[0])} {parts[1]}"
        raise ValueError("invalid fixed serving order term")


_QUERY_SPECS: Mapping[str, _QuerySpec] = MappingProxyType(
    {
        "services": _QuerySpec(
            table="runtime_services",
            columns=(
                "service_id",
                "plane",
                "status",
                "stale",
                "observed_at",
                "heartbeat_at",
                "input_sequence",
                "output_sequence",
                "backlog_count",
                "consecutive_failures",
                "last_error",
            ),
            order_by=("plane", "service_id"),
        ),
        "signals": _QuerySpec(
            table="signals",
            columns=(
                "global_sequence",
                "signal_id",
                "strategy_id",
                "strategy_version",
                "candidate_id",
                "action",
                "available_at",
                "expires_at",
                "reason_codes_json",
            ),
            order_by=("global_sequence DESC",),
        ),
        "deliveries": _QuerySpec(
            table="deliveries",
            columns=(
                "outbox_id",
                "signal_id",
                "recipient_id",
                "channel",
                "status",
                "attempt_count",
                "updated_at",
                "last_error",
            ),
            order_by=("updated_at DESC", "outbox_id"),
        ),
        "paper_accounts": _QuerySpec(
            table="paper_accounts",
            columns=(
                "account_id",
                "as_of_time",
                "cash",
                "available_cash",
                "frozen_cash",
                "nav",
                "unrealized_pnl",
                "realized_pnl",
            ),
            order_by=("as_of_time DESC", "account_id"),
        ),
        "paper_holdings": _QuerySpec(
            table="paper_holdings",
            columns=(
                "account_id",
                "ts_code",
                "quantity",
                "available_quantity",
                "frozen_quantity",
                "average_cost",
                "market_price",
                "market_value",
                "unrealized_pnl",
                "as_of_time",
            ),
            order_by=("account_id", "market_value DESC", "ts_code"),
        ),
        "lab_jobs": _QuerySpec(
            table="lab_jobs",
            columns=(
                "job_id",
                "strategy_name",
                "job_type",
                "resource_class",
                "status",
                "progress_fraction",
                "phase",
                "terminal_shards",
                "total_shards",
                "eta_status",
                "eta_finish_low",
                "eta_finish_center",
                "eta_finish_high",
                "updated_at",
            ),
            order_by=("updated_at DESC", "job_id"),
        ),
        "promotions": _QuerySpec(
            table="promotions",
            columns=(
                "decision_id",
                "stage",
                "approved",
                "experiment_ids_json",
                "gate_failures_json",
                "decided_at",
            ),
            order_by=("decided_at DESC", "decision_id"),
        ),
    }
)


_Row = TypeVar("_Row", bound=RuntimeContractModel)


def _read_rows(
    connection: _ReadonlyConnection,
    *,
    section: str,
    limit: int,
    model: type[_Row],
) -> tuple[_Row, ...]:
    spec = _QUERY_SPECS[section]
    rows = connection.execute(spec.sql, (limit,)).fetchall()
    return tuple(model.model_validate(dict(zip(spec.columns, row, strict=True))) for row in rows)


def _error_detail(error: Exception) -> str:
    message = " ".join(str(error).split())
    return f"{type(error).__name__}: {message or 'serving read failed'}"[:300]


def _unavailable(error: Exception) -> RuntimeConsoleSnapshot:
    return RuntimeConsoleSnapshot(
        state=ConsoleLoadState.DEGRADED,
        freshness=ConsoleFreshness.UNAVAILABLE,
        detail=_error_detail(error),
    )


def load_runtime_console(
    serving_root: str | Path,
    *,
    now: datetime | None = None,
    stale_after: timedelta = timedelta(minutes=10),
    limits: ConsoleLimits | None = None,
) -> RuntimeConsoleSnapshot:
    """Load one verified generation through fixed, bounded, read-only queries."""

    if stale_after <= timedelta(0):
        raise ValueError("stale_after must be positive")
    observed_at = normalize_aware_utc(now or datetime.now(UTC))
    query_limits = limits or ConsoleLimits()
    try:
        reader = ServingReader(serving_root)
        lease = reader.acquire_generation()
    except Exception as error:
        return _unavailable(error)

    manifest: ServingGenerationManifest | None = None
    try:
        with lease as acquired:
            manifest = acquired.manifest
            age = observed_at - manifest.built_at
            if age < timedelta(0):
                return RuntimeConsoleSnapshot(
                    state=ConsoleLoadState.DEGRADED,
                    freshness=ConsoleFreshness.DEGRADED,
                    detail="serving generation has a future built_at timestamp",
                    generation_id=manifest.generation_id,
                    generated_at=manifest.built_at,
                    age_seconds=0,
                    producer_commit=manifest.producer_commit,
                )
            freshness = manifest_freshness(
                manifest,
                age=age,
                stale_after=stale_after,
            )
            connection = acquired.connection
            sections = {
                "services": _read_rows(
                    connection,
                    section="services",
                    limit=query_limits.services,
                    model=RuntimeServiceRow,
                ),
                "signals": _read_rows(
                    connection,
                    section="signals",
                    limit=query_limits.signals,
                    model=SignalRow,
                ),
                "deliveries": _read_rows(
                    connection,
                    section="deliveries",
                    limit=query_limits.deliveries,
                    model=DeliveryRow,
                ),
                "paper_accounts": _read_rows(
                    connection,
                    section="paper_accounts",
                    limit=query_limits.paper_accounts,
                    model=PaperAccountRow,
                ),
                "paper_holdings": _read_rows(
                    connection,
                    section="paper_holdings",
                    limit=query_limits.paper_holdings,
                    model=PaperHoldingRow,
                ),
                "lab_jobs": _read_rows(
                    connection,
                    section="lab_jobs",
                    limit=query_limits.lab_jobs,
                    model=LabJobRow,
                ),
                "promotions": _read_rows(
                    connection,
                    section="promotions",
                    limit=query_limits.promotions,
                    model=PromotionRow,
                ),
            }
    except Exception as error:
        if manifest is None:
            return _unavailable(error)
        age = max(observed_at - manifest.built_at, timedelta(0))
        return RuntimeConsoleSnapshot(
            state=ConsoleLoadState.DEGRADED,
            freshness=ConsoleFreshness.DEGRADED,
            detail=_error_detail(error),
            generation_id=manifest.generation_id,
            generated_at=manifest.built_at,
            age_seconds=int(age.total_seconds()),
            producer_commit=manifest.producer_commit,
        )

    return RuntimeConsoleSnapshot(
        state=(
            ConsoleLoadState.DEGRADED
            if freshness is ConsoleFreshness.DEGRADED
            else ConsoleLoadState.READY
        ),
        freshness=freshness,
        detail="serving generation verified",
        generation_id=manifest.generation_id,
        generated_at=manifest.built_at,
        age_seconds=int(age.total_seconds()),
        producer_commit=manifest.producer_commit,
        **sections,
    )


__all__ = [
    "ConsoleFreshness",
    "ConsoleLimits",
    "ConsoleLoadState",
    "DeliveryRow",
    "LabJobRow",
    "PaperAccountRow",
    "PaperHoldingRow",
    "PromotionRow",
    "RuntimeConsoleSnapshot",
    "RuntimeServiceRow",
    "SignalRow",
    "ServingFrameResult",
    "ServingFrameState",
    "load_runtime_console",
    "query_serving_frame",
    "query_acquired_serving_frame",
    "require_serving_projections",
]
