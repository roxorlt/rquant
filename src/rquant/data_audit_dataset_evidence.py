"""Aggregate fixed read-only catalog sources; never return raw security observations."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal

import duckdb

from rquant.data_audit_contracts import MAX_AUDIT_DAYS
from rquant.data_audit_coverage import CalendarDay, CalendarEvidence
from rquant.data_audit_datasets import (
    MAX_DATASET_FIELDS,
    MAX_DIMENSIONS,
    DatasetAuditEvidence,
    DatasetAuditResult,
    DatasetDayCount,
    DatasetFieldNulls,
    DatasetFrequencyCount,
    audit_dataset_evidence,
)
from rquant.data_catalog.build import CATALOG_CONTRACTS
from rquant.data_contracts import EXCHANGE_TIMEZONE, DatasetContract, VisibilityRule
from rquant.research_lake import (
    ResearchPartitionKey,
    ResearchPartitionManifest,
    verify_research_partition,
)


def _id(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def read_audit_calendar(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    audit_start: date,
    observed_through: date,
) -> CalendarEvidence:
    span = (observed_through - audit_start).days + 1
    if not 1 <= span <= MAX_AUDIT_DAYS:
        raise ValueError("invalid bounded audit date range")
    try:
        rows = connection.execute(
            "SELECT cal_date,is_open FROM trade_calendar WHERE exchange='SSE' "
            "AND cal_date BETWEEN ? AND ? ORDER BY cal_date LIMIT ?",
            [audit_start, observed_through, span + 1],
        ).fetchall()
    except duckdb.Error as exc:
        raise ValueError("audit calendar unavailable") from exc
    if len(rows) != span or any(
        row[0] != audit_start + timedelta(days=i) for i, row in enumerate(rows)
    ):
        raise ValueError("audit calendar must contain every date exactly once")
    return CalendarEvidence(
        snapshot_id=source_id,
        exchange="SSE",
        days=tuple(CalendarDay(day=d, is_open=o) for d, o in rows),
    )


def _readonly(connection: duckdb.DuckDBPyConnection) -> None:
    row = connection.execute("SELECT current_setting('access_mode')").fetchone()
    if row is None or str(row[0]).lower() != "read_only":
        raise ValueError("catalog audit requires a read-only DuckDB connection")


def _local_expression(column: str, data_type: str) -> str:
    quoted = _id(column)
    return (
        f"timezone('Asia/Shanghai', {quoted})"
        if data_type == "TIMESTAMP WITH TIME ZONE"
        else quoted
    )


def _visible(
    contract: DatasetContract, *, event: str | None, as_of: datetime, has_source: bool
) -> str:
    local = as_of.astimezone(EXCHANGE_TIMEZONE)
    if contract.visibility == VisibilityRule.UNKNOWN or event is None:
        return "false"
    day = _literal(local.date().isoformat())
    if contract.visibility == VisibilityRule.PANEL_CLOSE_NEXT_SESSION:
        return f"({event} < DATE {day})"
    if contract.visibility == VisibilityRule.MINUTE_AS_OF:
        stamp = _literal(local.replace(tzinfo=None).isoformat())
        return f"({event} <= TIMESTAMP {stamp})"
    allowed = tuple(
        s
        for s in contract.sources
        if contract.is_visible(as_of_time=as_of, event_date=local.date(), source=s)
    )
    today = (
        f"source IN ({','.join(_literal(s) for s in allowed)})"
        if has_source and allowed
        else "true"
        if not has_source and allowed
        else "false"
    )
    return f"({event} < DATE {day} OR ({event} = DATE {day} AND {today}))"


def _read_table(
    connection: duckdb.DuckDBPyConnection,
    *,
    contract: DatasetContract,
    source_id: str,
    audit_start: date,
    observed_through: date,
    as_of: datetime,
    reader: str | None = None,
    source_kind: Literal["fixed_replica", "named_lake"] = "fixed_replica",
) -> DatasetAuditEvidence:
    common = dict(
        dataset_id=contract.dataset_id,
        source_id=source_id,
        source_kind=source_kind,
        audit_start=audit_start,
        observed_through=observed_through,
        as_of=as_of,
    )
    table = _id(contract.table_name) if reader is None else reader
    if reader is None:
        described = connection.execute(
            "SELECT column_name,data_type FROM information_schema.columns "
            "WHERE table_catalog=current_database() AND table_schema='main' AND table_name=? "
            "ORDER BY ordinal_position LIMIT ?",
            [contract.table_name, MAX_DATASET_FIELDS + 1],
        ).fetchall()
    else:
        described = [
            (r[0], r[1]) for r in connection.execute(f"DESCRIBE SELECT * FROM {table}").fetchall()
        ]
    if not described:
        return DatasetAuditEvidence(**common, source_state="missing_source")
    if len(described) > MAX_DATASET_FIELDS:
        raise ValueError("catalog table exceeds column budget")
    types = dict(described)
    required = set(contract.physical_primary_key) | {contract.freshness.watermark_column}
    required.update(
        c
        for c in (
            contract.event_date_column,
            contract.event_time_column,
            contract.ingested_at_column,
        )
        if c
    )
    missing = sorted(required - types.keys())
    if missing:
        return DatasetAuditEvidence(
            **common, source_state="schema_mismatch", missing_columns=tuple(missing)
        )
    for column in (contract.event_time_column, contract.ingested_at_column):
        if column is not None and types[column] not in {"TIMESTAMP", "TIMESTAMP WITH TIME ZONE"}:
            return DatasetAuditEvidence(
                **common, source_state="schema_mismatch", missing_columns=(column,)
            )
    if contract.event_date_column and types[contract.event_date_column] != "DATE":
        return DatasetAuditEvidence(
            **common, source_state="schema_mismatch", missing_columns=(contract.event_date_column,)
        )
    event_column = contract.event_time_column or contract.event_date_column
    event = _local_expression(event_column, types[event_column]) if event_column else None
    visible = _visible(contract, event=event, as_of=as_of, has_source="source" in types)
    if contract.visibility == VisibilityRule.MINUTE_AS_OF and contract.event_date_column:
        date_event = _id(contract.event_date_column)
    else:
        date_event = f"CAST({event} AS DATE)" if event else None
    ranged = event is not None and contract.historized
    # A missing availability timestamp cannot pull a valid trade_date outside the range.
    where = f"({date_event} BETWEEN ? AND ? OR {date_event} IS NULL)" if ranged else "true"
    params = [audit_start, observed_through] if ranged else []
    stamp = _literal(as_of.astimezone(EXCHANGE_TIMEZONE).replace(tzinfo=None).isoformat())
    ingest = (
        _local_expression(contract.ingested_at_column, types[contract.ingested_at_column])
        if contract.ingested_at_column
        else None
    )
    late = (
        f"COALESCE(SUM(CASE WHEN {ingest} > TIMESTAMP {stamp} THEN 1 ELSE 0 END),0)"
        if ingest
        else "0"
    )
    unknown_source = (
        "COALESCE(SUM(CASE WHEN source IS NULL OR source NOT IN ("
        + ",".join(_literal(s) for s in contract.sources)
        + ") THEN 1 ELSE 0 END),0)"
        if "source" in types
        else "0"
    )
    valid_frequencies = []
    if contract.dataset_id == "minute_bar":
        for freq in ("1min", "5min", "15min", "30min", "60min"):
            ResearchPartitionKey(dataset="minute_bar", trade_date=audit_start, freq=freq)
            valid_frequencies.append(freq)
    unknown_freq = (
        "COALESCE(SUM(CASE WHEN freq IS NULL OR freq NOT IN ("
        + ",".join(_literal(f) for f in valid_frequencies)
        + ") THEN 1 ELSE 0 END),0)"
        if valid_frequencies
        else "0"
    )
    # An offline audit still avoids scanning every large money-flow measure.
    representatives = {"close", "price", "adj_factor", "vol", "amount", "coverage_state"}
    selected_fields = sorted(required | (representatives & types.keys()))
    nulls = ",".join(
        f"COALESCE(SUM(CASE WHEN {_id(f)} IS NULL THEN 1 ELSE 0 END),0)" for f in selected_fields
    )
    max_date = f"MAX(CASE WHEN {visible} THEN {date_event} END)" if date_event else "NULL"
    max_time = (
        f"MAX(CASE WHEN {visible} THEN {event} END)" if contract.event_time_column else "NULL"
    )
    null_event = f"COALESCE(SUM(CASE WHEN {event} IS NULL THEN 1 ELSE 0 END),0)" if event else "0"
    row = connection.execute(
        f"SELECT COUNT(*), COALESCE(SUM(CASE WHEN {visible} THEN 1 ELSE 0 END),0),"
        f"{late},{null_event},{unknown_source},{unknown_freq},{max_date},{max_time},"
        f"{f'MAX({ingest})' if ingest else 'NULL'},{nulls} FROM {table} WHERE {where}",
        params,
    ).fetchone()
    assert row is not None
    days: tuple[DatasetDayCount, ...] = ()
    if date_event and contract.historized:
        day_rows = connection.execute(
            f"SELECT {date_event},COUNT(*),COALESCE(SUM(CASE WHEN {visible} THEN 1 ELSE 0 END),0) "
            f"FROM {table} WHERE {where} AND {date_event} IS NOT NULL "
            f"GROUP BY 1 ORDER BY 1 LIMIT ?",
            [*params, MAX_AUDIT_DAYS + 1],
        ).fetchall()
        days = tuple(
            DatasetDayCount(day=d, row_count=int(n), visible_rows=int(v)) for d, n, v in day_rows
        )
    frequencies: tuple[DatasetFrequencyCount, ...] = ()
    if valid_frequencies:
        rows = connection.execute(
            f"SELECT COALESCE(freq,'(missing)'),COUNT(*),"
            f"COALESCE(SUM(CASE WHEN {visible} THEN 1 ELSE 0 END),0) "
            f"FROM {table} WHERE {where} GROUP BY 1 ORDER BY 1 LIMIT ?",
            [*params, MAX_DIMENSIONS + 1],
        ).fetchall()
        frequencies = tuple(
            DatasetFrequencyCount(frequency=f, row_count=int(n), visible_rows=int(v))
            for f, n, v in rows
        )
    return DatasetAuditEvidence(
        **common,
        source_state="ready",
        source_column_present="source" in types,
        observed_rows=int(row[0]),
        visible_rows=int(row[1]),
        recorded_after_as_of_rows=int(row[2]),
        null_event_rows=int(row[3]),
        unknown_source_rows=int(row[4]),
        unknown_frequency_rows=int(row[5]),
        latest_visible_date=row[6],
        latest_visible_time=row[7],
        latest_ingested_at=row[8],
        days=days,
        frequencies=frequencies,
        fields=tuple(
            DatasetFieldNulls(
                field_name=f,
                observed_rows=int(row[0]),
                null_rows=int(row[9 + i]),
                required_key=f in contract.physical_primary_key,
            )
            for i, f in enumerate(selected_fields)
        ),
    )


def read_catalog_audit_from_connection(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_id: str,
    audit_start: date,
    observed_through: date,
    as_of: datetime,
    dataset_ids: tuple[str, ...] | None = None,
) -> tuple[DatasetAuditResult, ...]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("audit observation time must be timezone-aware")
    _readonly(connection)
    calendar = read_audit_calendar(
        connection, source_id=source_id, audit_start=audit_start, observed_through=observed_through
    )
    ids = {c.dataset_id for c in CATALOG_CONTRACTS}
    if dataset_ids is not None and (
        not dataset_ids or len(dataset_ids) != len(set(dataset_ids)) or set(dataset_ids) - ids
    ):
        raise ValueError("audit dataset selection is empty, repeated or unknown")
    return tuple(
        audit_dataset_evidence(
            _read_table(
                connection,
                contract=contract,
                source_id=source_id,
                audit_start=audit_start,
                observed_through=observed_through,
                as_of=as_of,
            ),
            calendar=calendar,
            contract=contract,
        )
        for contract in sorted(CATALOG_CONTRACTS, key=lambda c: c.dataset_id)
        if dataset_ids is None or contract.dataset_id in dataset_ids
    )


def read_named_lake_audit(
    connection: duckdb.DuckDBPyConnection,
    *,
    lake_root: Path,
    manifests: tuple[ResearchPartitionManifest, ...],
    source_id: str,
    calendar: CalendarEvidence,
    audit_start: date,
    observed_through: date,
    as_of: datetime,
) -> tuple[DatasetAuditResult, ...]:
    """A named list proves only those immutable partitions, never the whole lake."""
    if not manifests or len(manifests) > 512:
        raise ValueError("named audit requires 1 to 512 partitions")
    ids = tuple(m.partition.partition_id for m in manifests)
    if ids != tuple(sorted(set(ids))):
        raise ValueError("named audit partitions must be unique and ordered")
    paths: dict[str, list[str]] = {}
    for manifest in manifests:
        if not audit_start <= manifest.partition.trade_date <= observed_through:
            raise ValueError("named audit partition falls outside range")
        path = verify_research_partition(lake_root=lake_root, manifest=manifest, as_of_time=as_of)
        paths.setdefault(manifest.dataset, []).append(_literal(str(path)))
    results = []
    for contract in sorted(CATALOG_CONTRACTS, key=lambda c: c.dataset_id):
        if contract.dataset_id not in paths:
            continue
        reader = (
            "read_parquet([" + ",".join(paths[contract.dataset_id]) + "],hive_partitioning=false)"
        )
        evidence = _read_table(
            connection,
            contract=contract,
            source_id=source_id,
            audit_start=audit_start,
            observed_through=observed_through,
            as_of=as_of,
            reader=reader,
            source_kind="named_lake",
        )
        result = audit_dataset_evidence(evidence, calendar=calendar, contract=contract)
        results.append(result.model_copy(update={"coverage_reason": "named_partitions_only"}))
    return tuple(results)
