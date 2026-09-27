"""Build immutable daily fundamental evidence at a fixed historical decision time."""

from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

import duckdb
from pydantic import Field, StringConstraints, model_validator

from rquant.daily_valuation_pit import (
    DailyValuationPITQuery,
    DailyValuationRow,
    _row_sha256,
    query_daily_valuation_pit,
)
from rquant.financial_pit_facts import FinancialPITQuery, _cursor, query_financial_pit
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

TsCode = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
FieldName = Literal["pe_ttm", "pb", "dv_ttm", "roe", "or_yoy", "netprofit_yoy"]
SourceAPI = Literal["daily_basic", "fina_indicator"]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_FINANCIAL_FIELDS: tuple[FieldName, ...] = ("roe", "or_yoy", "netprofit_yoy")
_VALUATION_FIELDS: tuple[FieldName, ...] = ("pe_ttm", "pb", "dv_ttm")
_ALL_FIELDS: tuple[FieldName, ...] = (*_VALUATION_FIELDS, *_FINANCIAL_FIELDS)
_MAX_PERIOD_OBSERVATIONS = 10_000


class FundamentalDailyQuery(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date


class FundamentalFieldEvidence(RuntimeContractModel):
    field: FieldName
    source_api: SourceAPI
    status: Literal["selected", "unknown"]
    reason: str = Field(min_length=1)
    value: Decimal | None
    decision_at: AwareUtcDatetime
    source_date: date | None
    source_version_sha256: Sha256Hex | None

    @model_validator(mode="after")
    def require_value_for_selection(self) -> FundamentalFieldEvidence:
        if (self.status == "selected") != (self.value is not None):
            raise ValueError("fundamental field status does not match its value")
        if self.value is not None and not self.value.is_finite():
            raise ValueError("fundamental field value is not finite")
        return self


class FinancialSource(RuntimeContractModel):
    archive_id: str | None
    last_observed_at: AwareUtcDatetime | None
    anchor_generation: int | None
    anchor_record_sha256: Sha256Hex | None
    tip_request_id: str | None
    tip_file_sha256: Sha256Hex | None
    observation_sha256: Sha256Hex | None


class ValuationSource(RuntimeContractModel):
    trade_date: date | None
    candidate_generation_id: Sha256Hex | None
    row_sha256: Sha256Hex | None
    source_generation_id: Sha256Hex | None
    source_sequence: int | None
    source_batch_id: Sha256Hex | None
    revision: int | None
    observed_at: AwareUtcDatetime | None


class FundamentalDailyVersion(RuntimeContractModel):
    version_id: Sha256Hex
    ts_code: TsCode
    trade_date: date
    revision: int = Field(ge=1)
    decision_at: AwareUtcDatetime
    target_report_period: date | None
    target_period_reason: str = Field(min_length=1)
    financial_source: FinancialSource
    valuation_source: ValuationSource
    fields: dict[FieldName, FundamentalFieldEvidence]

    @model_validator(mode="after")
    def require_all_six_fields(self) -> FundamentalDailyVersion:
        if set(self.fields) != set(_ALL_FIELDS):
            raise ValueError("fundamental daily version needs exactly six fields")
        if any(
            name != field.field or field.decision_at != self.decision_at
            for name, field in self.fields.items()
        ):
            raise ValueError("fundamental daily field identity changed")
        return self


def _decision_at(day: date) -> datetime:
    return datetime.combine(day, time(17, 0), tzinfo=_SHANGHAI)


def _financial_observation_digest(
    conn: duckdb.DuckDBPyConnection,
    *,
    archive_id: str,
    ts_code: str,
    decision_at: datetime,
    ceiling: datetime | None,
) -> str | None:
    if ceiling is None:
        return canonical_sha256(())
    rows = conn.execute(
        "SELECT o.request_id, o.row_index, o.observed_at, o.report_period, "
        "o.report_type, o.ann_date, o.f_ann_date, o.raw_json, o.row_sha256, "
        "o.pit_usable, o.conflicted FROM financial_observation AS o "
        "JOIN financial_import_batch AS b ON b.archive_id = o.archive_id "
        "AND b.request_id = o.request_id AND b.observed_at = o.observed_at "
        "WHERE o.archive_id = ? AND o.source_api = 'fina_indicator' "
        "AND o.ts_code = ? AND o.observed_at < ? AND o.observed_at <= ? "
        "ORDER BY o.observed_at, o.request_id, o.row_index LIMIT ?",
        [archive_id, ts_code, decision_at.astimezone(UTC), ceiling, _MAX_PERIOD_OBSERVATIONS + 1],
    ).fetchall()
    return canonical_sha256(rows) if len(rows) <= _MAX_PERIOD_OBSERVATIONS else None


def _financial_source(
    conn: duckdb.DuckDBPyConnection, *, ts_code: str, decision_at: datetime
) -> FinancialSource:
    cursor = _cursor(conn)
    if cursor is None:
        return FinancialSource(
            archive_id=None,
            last_observed_at=None,
            anchor_generation=None,
            anchor_record_sha256=None,
            tip_request_id=None,
            tip_file_sha256=None,
            observation_sha256=None,
        )
    tip = conn.execute(
        "SELECT request_id, file_sha256 FROM financial_import_batch "
        "WHERE archive_id = ? ORDER BY observed_at DESC LIMIT 1",
        [cursor[0]],
    ).fetchone()
    return FinancialSource(
        archive_id=cursor[0],
        last_observed_at=cursor[1],
        anchor_generation=cursor[2],
        anchor_record_sha256=cursor[3],
        tip_request_id=tip[0] if tip else None,
        tip_file_sha256=tip[1] if tip else None,
        observation_sha256=_financial_observation_digest(
            conn,
            archive_id=cursor[0],
            ts_code=ts_code,
            decision_at=decision_at,
            ceiling=cursor[1],
        ),
    )


def _target_period(
    conn: duckdb.DuckDBPyConnection,
    *,
    source: FinancialSource,
    ts_code: str,
    decision_at: datetime,
) -> tuple[date | None, str, str | None]:
    if source.archive_id is None:
        return None, "no_financial_archive", None
    rows = conn.execute(
        "SELECT o.report_period, o.report_type, o.observed_at, o.request_id, "
        "o.row_index, o.row_sha256 FROM financial_observation AS o "
        "JOIN financial_import_batch AS b ON b.archive_id = o.archive_id "
        "AND b.request_id = o.request_id AND b.observed_at = o.observed_at "
        "WHERE o.archive_id = ? AND o.source_api = 'fina_indicator' AND o.ts_code = ? "
        "AND o.observed_at < ? ORDER BY o.observed_at, o.request_id, o.row_index LIMIT ?",
        [source.archive_id, ts_code, decision_at.astimezone(UTC), _MAX_PERIOD_OBSERVATIONS + 1],
    ).fetchall()
    if len(rows) > _MAX_PERIOD_OBSERVATIONS:
        return None, "period_candidate_limit", None
    if not rows:
        return None, "no_period_evidence", None
    if any(period is None or report_type != "default" for period, report_type, *_ in rows):
        return None, "unkeyed_observation", canonical_sha256(rows)
    latest = max(row[0] for row in rows)
    if latest > decision_at.astimezone(_SHANGHAI).date():
        return None, "future_report_period", canonical_sha256(rows)
    evidence = [row for row in rows if row[0] == latest]
    return latest, "observed_period", canonical_sha256(evidence)


def _valuation_source(
    conn: duckdb.DuckDBPyConnection,
    *,
    ts_code: str,
    decision_date: date,
    decision_at: datetime,
) -> ValuationSource:
    previous = conn.execute(
        "SELECT MAX(cal_date) FROM trade_calendar "
        "WHERE exchange = 'SSE' AND is_open = TRUE AND cal_date < ?",
        [decision_date],
    ).fetchone()[0]
    if previous is None:
        return ValuationSource(
            trade_date=None,
            candidate_generation_id=None,
            row_sha256=None,
            source_generation_id=None,
            source_sequence=None,
            source_batch_id=None,
            revision=None,
            observed_at=None,
        )
    batch = conn.execute(
        "SELECT candidate_generation_id, source_generation_id, source_sequence, "
        "source_batch_id, revision, observed_at FROM daily_basic_valuation_batch "
        "WHERE trade_date = ? AND observed_at <= ? "
        "ORDER BY observed_at DESC, source_sequence DESC LIMIT 1",
        [previous, decision_at.astimezone(UTC)],
    ).fetchone()
    row = (
        conn.execute(
            "SELECT row_sha256 FROM daily_basic_valuation_observation "
            "WHERE candidate_generation_id = ? AND ts_code = ? AND trade_date = ?",
            [batch[0], ts_code, previous],
        ).fetchone()
        if batch is not None
        else None
    )
    return ValuationSource(
        trade_date=previous,
        candidate_generation_id=batch[0] if batch else None,
        row_sha256=row[0] if row else None,
        source_generation_id=batch[1] if batch else None,
        source_sequence=batch[2] if batch else None,
        source_batch_id=batch[3] if batch else None,
        revision=batch[4] if batch else None,
        observed_at=batch[5] if batch else None,
    )


def _financial_fields(
    conn: duckdb.DuckDBPyConnection,
    *,
    ts_code: str,
    period: date | None,
    period_reason: str,
    period_digest: str | None,
    decision_at: datetime,
) -> dict[FieldName, FundamentalFieldEvidence]:
    fields: dict[FieldName, FundamentalFieldEvidence] = {}
    for name in _FINANCIAL_FIELDS:
        if period is None:
            fields[name] = FundamentalFieldEvidence(
                field=name,
                source_api="fina_indicator",
                status="unknown",
                reason=period_reason,
                value=None,
                decision_at=decision_at,
                source_date=None,
                source_version_sha256=period_digest,
            )
            continue
        selected = query_financial_pit(
            conn,
            FinancialPITQuery(
                source_api="fina_indicator",
                field=name,
                ts_code=ts_code,
                report_period=period,
                report_type="default",
                as_of=decision_at,
            ),
        )
        selected_value = selected.fact.value if selected.fact is not None else None
        try:
            representable = selected_value is None or math.isfinite(float(selected_value))
        except OverflowError:
            representable = False
        fields[name] = FundamentalFieldEvidence(
            field=name,
            source_api="fina_indicator",
            status=selected.status if representable else "unknown",
            reason=selected.reason if representable else "unrepresentable_numeric",
            value=selected_value if representable else None,
            decision_at=decision_at,
            source_date=period,
            source_version_sha256=(
                selected.content_sha256 if selected.content_sha256 is not None else period_digest
            ),
        )
    return fields


def _valuation_fields(
    conn: duckdb.DuckDBPyConnection,
    *,
    ts_code: str,
    decision_date: date,
    decision_at: datetime,
    source: ValuationSource,
) -> dict[FieldName, FundamentalFieldEvidence]:
    selected = query_daily_valuation_pit(
        conn, DailyValuationPITQuery(ts_code=ts_code, decision_date=decision_date)
    )
    if selected.status == "selected" and (
        selected.observed_at is None
        or selected.first_observed_at is None
        or selected.observed_at >= decision_at
        or selected.first_observed_at >= decision_at
    ):
        reason = "observed_at_decision_boundary"
        values: dict[FieldName, Decimal | None] = {name: None for name in _VALUATION_FIELDS}
    elif selected.status == "selected":
        reason = selected.reason
        values = {
            name: Decimal(str(getattr(selected, name)))
            if getattr(selected, name) is not None
            else None
            for name in _VALUATION_FIELDS
        }
    else:
        reason = selected.reason
        values = {name: None for name in _VALUATION_FIELDS}
    return {
        name: FundamentalFieldEvidence(
            field=name,
            source_api="daily_basic",
            status="selected" if values[name] is not None else "unknown",
            reason=(
                "missing_value"
                if selected.status == "selected" and values[name] is None and reason == "visible"
                else reason
            ),
            value=values[name],
            decision_at=decision_at,
            source_date=source.trade_date,
            source_version_sha256=(
                selected.row_sha256
                if selected.row_sha256 is not None
                else source.row_sha256 or source.candidate_generation_id
            ),
        )
        for name in _VALUATION_FIELDS
    }


def _source_progress(
    conn: duckdb.DuckDBPyConnection,
    old: FundamentalDailyVersion,
    financial: FinancialSource,
    valuation: ValuationSource,
) -> None:
    prior_financial = old.financial_source
    if prior_financial.archive_id is not None:
        if (
            financial.archive_id != prior_financial.archive_id
            or financial.anchor_generation is None
            or prior_financial.anchor_generation is None
            or financial.anchor_generation < prior_financial.anchor_generation
            or (
                financial.anchor_generation == prior_financial.anchor_generation
                and financial.anchor_record_sha256 != prior_financial.anchor_record_sha256
            )
            or financial.last_observed_at is None
            or (
                prior_financial.last_observed_at is not None
                and financial.last_observed_at < prior_financial.last_observed_at
            )
        ):
            raise ValueError("financial source rollback or unproven archive successor")
        if prior_financial.tip_request_id is not None:
            tip = conn.execute(
                "SELECT file_sha256 FROM financial_import_batch "
                "WHERE archive_id = ? AND request_id = ?",
                [prior_financial.archive_id, prior_financial.tip_request_id],
            ).fetchone()
            if tip is None or tip[0] != prior_financial.tip_file_sha256:
                raise ValueError("financial source tip was removed or changed")
        observed_digest = _financial_observation_digest(
            conn,
            archive_id=prior_financial.archive_id,
            ts_code=old.ts_code,
            decision_at=old.decision_at,
            ceiling=prior_financial.last_observed_at,
        )
        if observed_digest is None or observed_digest != prior_financial.observation_sha256:
            raise ValueError("financial source observation prefix changed")

    prior_valuation = old.valuation_source
    if prior_valuation.trade_date != valuation.trade_date:
        raise ValueError("valuation source date changed")
    if prior_valuation.candidate_generation_id is None:
        return
    persisted = conn.execute(
        "SELECT source_generation_id, source_sequence, source_batch_id, revision, observed_at "
        "FROM daily_basic_valuation_batch WHERE candidate_generation_id = ?",
        [prior_valuation.candidate_generation_id],
    ).fetchone()
    if persisted != (
        prior_valuation.source_generation_id,
        prior_valuation.source_sequence,
        prior_valuation.source_batch_id,
        prior_valuation.revision,
        prior_valuation.observed_at,
    ):
        raise ValueError("valuation source rollback or changed batch")
    row = conn.execute(
        "SELECT row_sha256, pe_ttm, pb, dv_ttm FROM daily_basic_valuation_observation "
        "WHERE candidate_generation_id = ? AND ts_code = ? AND trade_date = ?",
        [prior_valuation.candidate_generation_id, old.ts_code, prior_valuation.trade_date],
    ).fetchone()
    if (row is None) != (prior_valuation.row_sha256 is None):
        raise ValueError("valuation source row was removed or changed")
    if row is not None:
        try:
            actual_digest = _row_sha256(
                DailyValuationRow(
                    ts_code=old.ts_code,
                    trade_date=prior_valuation.trade_date,
                    pe_ttm=row[1],
                    pb=row[2],
                    dv_ttm=row[3],
                )
            )
        except ValueError as exc:
            raise ValueError("valuation source row is invalid") from exc
        if row[0] != prior_valuation.row_sha256 or actual_digest != row[0]:
            raise ValueError("valuation source row was removed or changed")
    if (
        valuation.candidate_generation_id is None
        or valuation.source_generation_id != prior_valuation.source_generation_id
        or valuation.source_sequence is None
        or prior_valuation.source_sequence is None
        or valuation.source_sequence < prior_valuation.source_sequence
        or valuation.revision is None
        or prior_valuation.revision is None
        or valuation.revision < prior_valuation.revision
        or valuation.observed_at is None
        or prior_valuation.observed_at is None
        or valuation.observed_at < prior_valuation.observed_at
        or (
            valuation.revision == prior_valuation.revision
            and valuation.candidate_generation_id != prior_valuation.candidate_generation_id
        )
    ):
        raise ValueError("valuation source rollback or unproven successor")


def _version_identity(
    query: FundamentalDailyQuery,
    decision_at: datetime,
    target_period: date | None,
    target_reason: str,
    financial: FinancialSource,
    valuation: ValuationSource,
    fields: dict[FieldName, FundamentalFieldEvidence],
) -> str:
    return canonical_sha256(
        {
            "ts_code": query.ts_code,
            "trade_date": query.trade_date,
            "decision_at": decision_at,
            "target_report_period": target_period,
            "target_period_reason": target_reason,
            "financial_source": financial,
            "valuation_source": valuation,
            "fields": fields,
        }
    )


def _write_head(conn: duckdb.DuckDBPyConnection, version: FundamentalDailyVersion) -> None:
    conn.execute(
        "INSERT INTO fundamental_daily_head VALUES (?, ?, ?, ?) "
        "ON CONFLICT (ts_code, trade_date) DO UPDATE SET "
        "version_id = excluded.version_id, revision = excluded.revision",
        [version.ts_code, version.trade_date, version.version_id, version.revision],
    )


def read_fundamental_daily(
    conn: duckdb.DuckDBPyConnection,
    query: FundamentalDailyQuery,
    *,
    version_id: str | None = None,
) -> FundamentalDailyVersion | None:
    """Read the current pointer or an explicitly bound historical version."""

    head_revision: int | None = None
    if version_id is None:
        head = conn.execute(
            "SELECT version_id, revision FROM fundamental_daily_head "
            "WHERE ts_code = ? AND trade_date = ?",
            [query.ts_code, query.trade_date],
        ).fetchone()
        if head is None:
            return None
        version_id = head[0]
        head_revision = head[1]
    row = conn.execute(
        "SELECT version_id, ts_code, trade_date, revision, decision_at, "
        "target_report_period, target_period_reason, financial_source_json, "
        "valuation_source_json, fields_json, pe_ttm, pb, dv_ttm, roe, or_yoy, "
        "netprofit_yoy FROM fundamental_daily_version "
        "WHERE ts_code = ? AND trade_date = ? AND version_id = ?",
        [query.ts_code, query.trade_date, version_id],
    ).fetchone()
    if row is None:
        raise ValueError("fundamental daily head or requested version is missing")
    version = FundamentalDailyVersion.model_validate(
        {
            "version_id": row[0],
            "ts_code": row[1],
            "trade_date": row[2],
            "revision": row[3],
            "decision_at": row[4],
            "target_report_period": row[5],
            "target_period_reason": row[6],
            "financial_source": FinancialSource.model_validate_json(row[7]),
            "valuation_source": ValuationSource.model_validate_json(row[8]),
            "fields": _load_fields(row[9]),
        }
    )
    expected_values = tuple(
        float(version.fields[name].value) if version.fields[name].value is not None else None
        for name in _ALL_FIELDS
    )
    identity = _version_identity(
        query,
        version.decision_at,
        version.target_report_period,
        version.target_period_reason,
        version.financial_source,
        version.valuation_source,
        version.fields,
    )
    if (
        row[10:] != expected_values
        or identity != version.version_id
        or (head_revision is not None and head_revision != version.revision)
    ):
        raise ValueError("fundamental daily version receipt mismatch")
    return version


def _load_fields(raw: str) -> dict[FieldName, FundamentalFieldEvidence]:
    return {
        name: FundamentalFieldEvidence.model_validate(value)
        for name, value in json.loads(raw).items()
    }


def derive_fundamental_daily(
    conn: duckdb.DuckDBPyConnection, query: FundamentalDailyQuery
) -> FundamentalDailyVersion:
    """Choose six PIT fields and atomically append/switch one daily version."""

    decision_at = _decision_at(query.trade_date)
    conn.execute("BEGIN TRANSACTION")
    try:
        financial = _financial_source(conn, ts_code=query.ts_code, decision_at=decision_at)
        valuation = _valuation_source(
            conn, ts_code=query.ts_code, decision_date=query.trade_date, decision_at=decision_at
        )
        period, period_reason, period_digest = _target_period(
            conn, source=financial, ts_code=query.ts_code, decision_at=decision_at
        )
        fields = _financial_fields(
            conn,
            ts_code=query.ts_code,
            period=period,
            period_reason=period_reason,
            period_digest=period_digest,
            decision_at=decision_at,
        )
        fields.update(
            _valuation_fields(
                conn,
                ts_code=query.ts_code,
                decision_date=query.trade_date,
                decision_at=decision_at,
                source=valuation,
            )
        )
        version_id = _version_identity(
            query, decision_at, period, period_reason, financial, valuation, fields
        )
        current = read_fundamental_daily(conn, query)
        if current is not None and current.version_id == version_id:
            conn.execute("COMMIT")
            return current
        if current is not None:
            _source_progress(conn, current, financial, valuation)
        version = FundamentalDailyVersion(
            version_id=version_id,
            ts_code=query.ts_code,
            trade_date=query.trade_date,
            revision=current.revision + 1 if current is not None else 1,
            decision_at=decision_at,
            target_report_period=period,
            target_period_reason=period_reason,
            financial_source=financial,
            valuation_source=valuation,
            fields=fields,
        )
        conn.execute(
            "INSERT INTO fundamental_daily_version VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                version.version_id,
                version.ts_code,
                version.trade_date,
                version.revision,
                version.decision_at,
                version.target_report_period,
                version.target_period_reason,
                version.financial_source.model_dump_json(),
                version.valuation_source.model_dump_json(),
                json.dumps(
                    {name: item.model_dump(mode="json") for name, item in fields.items()},
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                *[
                    float(fields[name].value) if fields[name].value is not None else None
                    for name in _ALL_FIELDS
                ],
            ],
        )
        _write_head(conn, version)
        conn.execute("COMMIT")
        return version
    except BaseException:
        conn.execute("ROLLBACK")
        raise
