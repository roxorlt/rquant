"""Reusable point-in-time visibility decisions and guarded dataset queries."""

from __future__ import annotations

import re
from collections.abc import Sequence
from datetime import date, datetime, time, timedelta
from typing import Protocol, cast

import duckdb
import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from rquant.data_contracts import (
    CONTRACTS_BY_ID,
    EXCHANGE_TIMEZONE,
    DatasetContract,
    VisibilityRule,
)


class _StoreWithConnection(Protocol):
    _conn: duckdb.DuckDBPyConnection


class VisibilityInput(BaseModel):
    """Dataset event facts required by a visibility decision."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_id: str
    event_date: date | None = None
    event_time: datetime | None = None
    source: str | None = None


class VisibilityDecision(BaseModel):
    """Auditable result of evaluating one event at one decision time."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    dataset_id: str
    visibility: VisibilityRule
    as_of_time: datetime
    available_at: datetime | None
    visible: bool
    reason: str


class VisibilityQueryScope(BaseModel):
    """Typed, bounded filters accepted by the guarded query entry point."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ts_codes: tuple[str, ...] = ()
    start_date: date | None = None
    end_date: date | None = None
    start_time: datetime | None = None
    end_time: datetime | None = None
    columns: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()

    @field_validator("ts_codes", "columns", "sources")
    @classmethod
    def validate_unique_nonempty_values(
        cls,
        values: tuple[str, ...],
    ) -> tuple[str, ...]:
        if any(not value for value in values):
            raise ValueError("query scope values cannot be empty")
        if len(values) != len(set(values)):
            raise ValueError("query scope values cannot contain duplicates")
        return values

    @field_validator("start_time", "end_time")
    @classmethod
    def validate_aware_query_time(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _aware_exchange_time(value, field_name="query time")

    @model_validator(mode="after")
    def validate_ranges(self) -> VisibilityQueryScope:
        if (
            self.start_date is not None
            and self.end_date is not None
            and self.start_date > self.end_date
        ):
            raise ValueError("start_date must be before or equal to end_date")
        if (
            self.start_time is not None
            and self.end_time is not None
            and self.start_time > self.end_time
        ):
            raise ValueError("start_time must be before or equal to end_time")
        return self


def _contract_for(dataset_id: str) -> DatasetContract:
    contract = CONTRACTS_BY_ID.get(dataset_id)
    if contract is None:
        raise ValueError(f"unknown dataset_id: {dataset_id!r}")
    return contract


def _aware_exchange_time(value: datetime, *, field_name: str) -> datetime:
    try:
        offset = value.utcoffset() if value.tzinfo is not None else None
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be timezone-aware") from exc
    if offset is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(EXCHANGE_TIMEZONE)


def _event_date(value: date | None) -> date:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise ValueError("event_date must be a civil date")
    return value


def _event_time(value: datetime | None) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("event_time must be a datetime")
    if value.tzinfo is None:
        return value.replace(tzinfo=EXCHANGE_TIMEZONE)
    return _aware_exchange_time(value, field_name="event_time")


def available_at_for_input(value: VisibilityInput) -> datetime | None:
    """Compute an event's earliest usable time from its registered contract."""

    contract = _contract_for(value.dataset_id)
    if contract.visibility is VisibilityRule.UNKNOWN:
        return None
    if contract.visibility is VisibilityRule.MINUTE_AS_OF:
        return _event_time(value.event_time)

    event_date = _event_date(value.event_date)
    if contract.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION:
        return datetime.combine(
            event_date + timedelta(days=1),
            time.min,
            tzinfo=EXCHANGE_TIMEZONE,
        )

    availability = next(
        (item for item in contract.source_availability if item.source == value.source),
        None,
    )
    if availability is None:
        return None
    return datetime.combine(
        event_date,
        availability.available_at,
        tzinfo=EXCHANGE_TIMEZONE,
    )


def evaluate_visibility(
    value: VisibilityInput,
    *,
    as_of_time: datetime,
) -> VisibilityDecision:
    """Evaluate one registered event, failing closed for unknown visibility."""

    local_as_of = _aware_exchange_time(as_of_time, field_name="as_of_time")
    contract = _contract_for(value.dataset_id)
    available_at = available_at_for_input(value)

    if contract.visibility is VisibilityRule.UNKNOWN:
        return VisibilityDecision(
            dataset_id=value.dataset_id,
            visibility=contract.visibility,
            as_of_time=local_as_of,
            available_at=None,
            visible=False,
            reason="unknown_visibility",
        )
    if contract.visibility is VisibilityRule.AUCTION_0925 and available_at is None:
        return VisibilityDecision(
            dataset_id=value.dataset_id,
            visibility=contract.visibility,
            as_of_time=local_as_of,
            available_at=None,
            visible=False,
            reason="unknown_source",
        )

    visible = contract.is_visible(
        as_of_time=local_as_of,
        event_date=value.event_date,
        event_time=value.event_time,
        source=value.source,
    )
    return VisibilityDecision(
        dataset_id=value.dataset_id,
        visibility=contract.visibility,
        as_of_time=local_as_of,
        available_at=available_at,
        visible=visible,
        reason="visible" if visible else "not_yet_available",
    )


def is_input_visible(value: VisibilityInput, *, as_of_time: datetime) -> bool:
    """Return whether one event may be consumed at ``as_of_time``."""

    return evaluate_visibility(value, as_of_time=as_of_time).visible


def visible_sources_at(
    dataset_id: str,
    *,
    event_date: date,
    as_of_time: datetime,
) -> tuple[str, ...]:
    """Return registered sources usable at a same-session decision instant."""
    contract = _contract_for(dataset_id)
    return tuple(
        source
        for source in contract.sources
        if is_input_visible(
            VisibilityInput(
                dataset_id=dataset_id,
                event_date=event_date,
                source=source,
            ),
            as_of_time=as_of_time,
        )
    )


def derive_available_at(inputs: Sequence[datetime | None]) -> datetime:
    """Return the latest availability among all required derived-field inputs."""

    if not inputs:
        raise ValueError("available_at inputs cannot be empty")
    normalized: list[datetime] = []
    for value in inputs:
        if value is None:
            raise ValueError("available_at inputs cannot be missing")
        normalized.append(_aware_exchange_time(value, field_name="available_at input"))
    return max(normalized)


def _connection(
    value: duckdb.DuckDBPyConnection | _StoreWithConnection,
) -> duckdb.DuckDBPyConnection:
    if isinstance(value, duckdb.DuckDBPyConnection):
        return value
    candidate = getattr(value, "_conn", None)
    if not isinstance(candidate, duckdb.DuckDBPyConnection):
        raise TypeError("query target must be a DuckDB connection or store")
    return candidate


def _quoted_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def _table_columns(
    conn: duckdb.DuckDBPyConnection,
    contract: DatasetContract,
) -> tuple[tuple[str, str], ...]:
    rows = conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_catalog = current_database()
          AND table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [contract.table_name],
    ).fetchall()
    if not rows:
        raise ValueError(f"registered table is unavailable: {contract.table_name}")
    return tuple((str(row[0]), str(row[1])) for row in rows)


def _normalize_timestamp_type(value: str) -> str:
    normalized = re.sub(
        r"TIMESTAMP\s*\(\s*\d+\s*\)",
        "TIMESTAMP",
        value.upper(),
    )
    normalized = " ".join(normalized.split())
    if normalized not in {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}:
        raise ValueError(f"unsupported event-time type: {value!r}")
    return normalized


def _physical_timestamp_value(value: datetime, physical_type: str) -> datetime:
    local_value = _aware_exchange_time(value, field_name="query time")
    if _normalize_timestamp_type(physical_type) == "TIMESTAMP WITH TIME ZONE":
        return local_value
    return local_value.replace(tzinfo=None)


def _scope_columns(
    scope: VisibilityQueryScope,
    physical_columns: tuple[tuple[str, str], ...],
) -> tuple[str, ...]:
    available = tuple(name for name, _ in physical_columns)
    selected = scope.columns or available
    unknown = set(selected) - set(available)
    if unknown:
        raise ValueError(f"unknown column(s): {', '.join(sorted(unknown))}")
    return selected


def _validate_scope_shape(
    contract: DatasetContract,
    scope: VisibilityQueryScope,
    physical_column_names: set[str],
) -> None:
    if scope.ts_codes and "ts_code" not in physical_column_names:
        raise ValueError(f"dataset {contract.dataset_id} has no ts_code column")
    if (scope.start_date is not None or scope.end_date is not None) and (
        contract.event_date_column is None
    ):
        raise ValueError(f"dataset {contract.dataset_id} has no event-date scope")
    if (scope.start_time is not None or scope.end_time is not None) and (
        contract.event_time_column is None
    ):
        raise ValueError(f"dataset {contract.dataset_id} has no event-time scope")
    if scope.sources and "source" not in physical_column_names:
        raise ValueError(f"dataset {contract.dataset_id} has no source column")
    unknown_sources = set(scope.sources) - set(contract.sources)
    if unknown_sources:
        raise ValueError(
            f"unregistered source(s) for {contract.dataset_id}: "
            f"{', '.join(sorted(unknown_sources))}"
        )


def _append_in_predicate(
    predicates: list[str],
    parameters: list[object],
    column: str,
    values: Sequence[str],
) -> None:
    placeholders = ", ".join("?" for _ in values)
    predicates.append(f"{_quoted_identifier(column)} IN ({placeholders})")
    parameters.extend(values)


def _visibility_predicates(
    contract: DatasetContract,
    scope: VisibilityQueryScope,
    local_as_of: datetime,
    column_types: dict[str, str],
    effective_sources: tuple[str, ...],
) -> tuple[list[str], list[object]]:
    predicates: list[str] = []
    parameters: list[object] = []

    if contract.visibility is VisibilityRule.UNKNOWN:
        return ["FALSE"], []

    if contract.visibility is VisibilityRule.MINUTE_AS_OF:
        if scope.start_time is None or scope.end_time is None:
            raise ValueError("MINUTE_AS_OF queries require start_time and end_time")
        if scope.end_time > local_as_of:
            raise ValueError("end_time cannot be later than as_of_time")
        event_column_name = cast(str, contract.event_time_column)
        event_column = _quoted_identifier(event_column_name)
        physical_type = column_types.get(event_column_name)
        if physical_type is None:
            raise ValueError(
                f"registered event-time column is unavailable: "
                f"{contract.table_name}.{event_column_name}"
            )
        predicates.extend([f"{event_column} >= ?", f"{event_column} <= ?"])
        parameters.extend(
            [
                _physical_timestamp_value(scope.start_time, physical_type),
                _physical_timestamp_value(scope.end_time, physical_type),
            ]
        )
        return predicates, parameters

    event_column_name = cast(str, contract.event_date_column)
    event_column = _quoted_identifier(event_column_name)
    if contract.visibility is VisibilityRule.PANEL_CLOSE_NEXT_SESSION:
        predicates.append(f"{event_column} < ?")
        parameters.append(local_as_of.date())
        return predicates, parameters

    visible_today_sources = tuple(
        item.source
        for item in contract.source_availability
        if item.source in effective_sources
        and local_as_of.time().replace(tzinfo=None) >= item.available_at
    )
    historical = f"{event_column} < ?"
    parameters.append(local_as_of.date())
    if not visible_today_sources:
        predicates.append(f"({historical})")
        return predicates, parameters
    placeholders = ", ".join("?" for _ in visible_today_sources)
    predicates.append(
        f"({historical} OR "
        f"({event_column} = ? AND {_quoted_identifier('source')} "
        f"IN ({placeholders})))"
    )
    parameters.extend([local_as_of.date(), *visible_today_sources])
    return predicates, parameters


def _scope_predicates(
    contract: DatasetContract,
    scope: VisibilityQueryScope,
) -> tuple[list[str], list[object]]:
    predicates: list[str] = []
    parameters: list[object] = []
    if scope.ts_codes:
        _append_in_predicate(predicates, parameters, "ts_code", scope.ts_codes)
    if contract.event_date_column is not None:
        event_column = _quoted_identifier(contract.event_date_column)
        if scope.start_date is not None:
            predicates.append(f"{event_column} >= ?")
            parameters.append(scope.start_date)
        if scope.end_date is not None:
            predicates.append(f"{event_column} <= ?")
            parameters.append(scope.end_date)
    return predicates, parameters


def _ranked_query(
    contract: DatasetContract,
    selected_columns: tuple[str, ...],
    predicates: Sequence[str],
    *,
    has_source: bool,
) -> tuple[str, list[object]]:
    table = _quoted_identifier(contract.table_name)
    projection = ", ".join(_quoted_identifier(column) for column in selected_columns)
    where = " AND ".join(f"({predicate})" for predicate in predicates)
    if not has_source:
        return f"SELECT {projection} FROM {table} WHERE {where}", []

    logical_key = ", ".join(_quoted_identifier(column) for column in contract.logical_key)
    source_order = " ".join(
        f"WHEN ? THEN {position}" for position, _ in enumerate(contract.sources)
    )
    sql = f"""
        WITH visible_rows AS (
            SELECT * FROM {table} WHERE {where}
        ), ranked_rows AS (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {logical_key}
                ORDER BY CASE {_quoted_identifier("source")}
                    {source_order}
                    ELSE {len(contract.sources)}
                END
            ) AS __pit_rank
            FROM visible_rows
        )
        SELECT {projection}
        FROM ranked_rows
        WHERE __pit_rank = 1
    """
    return sql, list(contract.sources)


def query_visible_rows(
    target: duckdb.DuckDBPyConnection | _StoreWithConnection,
    dataset_id: str,
    as_of_time: datetime,
    *,
    scope: VisibilityQueryScope | None = None,
) -> pd.DataFrame:
    """Query one registered table with its contract's PIT predicate applied."""

    local_as_of = _aware_exchange_time(as_of_time, field_name="as_of_time")
    contract = _contract_for(dataset_id)
    conn = _connection(target)
    query_scope = scope or VisibilityQueryScope()
    physical_columns = _table_columns(conn, contract)
    column_types = dict(physical_columns)
    physical_column_names = set(column_types)
    _validate_scope_shape(contract, query_scope, physical_column_names)
    selected_columns = _scope_columns(query_scope, physical_columns)

    has_source = "source" in physical_column_names
    effective_sources = (
        tuple(source for source in contract.sources if source in query_scope.sources)
        if query_scope.sources
        else contract.sources
    )
    visibility_predicates, visibility_parameters = _visibility_predicates(
        contract,
        query_scope,
        local_as_of,
        column_types,
        effective_sources,
    )
    scope_predicates, scope_parameters = _scope_predicates(contract, query_scope)
    predicates = [*visibility_predicates, *scope_predicates]
    parameters = [*visibility_parameters, *scope_parameters]
    if has_source:
        _append_in_predicate(predicates, parameters, "source", effective_sources)

    sql, rank_parameters = _ranked_query(
        contract,
        selected_columns,
        predicates,
        has_source=has_source,
    )
    return conn.execute(sql, [*parameters, *rank_parameters]).fetchdf()


__all__ = [
    "VisibilityDecision",
    "VisibilityInput",
    "VisibilityQueryScope",
    "available_at_for_input",
    "derive_available_at",
    "evaluate_visibility",
    "is_input_visible",
    "query_visible_rows",
    "visible_sources_at",
]
