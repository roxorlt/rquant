"""Observed daily valuation rows and their dedicated point-in-time selector."""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Annotated, Literal
from zoneinfo import ZoneInfo

import duckdb
from pydantic import Field, StringConstraints, ValidationError, model_validator

from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel, canonical_sha256

Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
TsCode = Annotated[str, StringConstraints(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")]
FiniteFloat = Annotated[float, Field(strict=True, allow_inf_nan=False)]
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_MAX_CANDIDATE_BATCHES = 256


class DailyValuationRow(RuntimeContractModel):
    ts_code: TsCode
    trade_date: date
    pe_ttm: FiniteFloat | None
    pb: FiniteFloat | None
    dv_ttm: FiniteFloat | None


class DailyValuationBatch(RuntimeContractModel):
    candidate_generation_id: Sha256Hex
    source_generation_id: Sha256Hex
    source_sequence: int = Field(ge=0)
    source_batch_id: Sha256Hex
    revision: int = Field(ge=1)
    trade_date: date
    observed_at: AwareUtcDatetime
    valuation_observed: bool
    rows: tuple[DailyValuationRow, ...]

    @model_validator(mode="after")
    def validate_rows(self) -> DailyValuationBatch:
        market_close = datetime.combine(self.trade_date, time(15, 0), tzinfo=_SHANGHAI)
        if self.observed_at.astimezone(_SHANGHAI) < market_close:
            raise ValueError("daily valuation observation precedes market close")
        if any(row.trade_date != self.trade_date for row in self.rows):
            raise ValueError("valuation row date changed")
        if len({row.ts_code for row in self.rows}) != len(self.rows):
            raise ValueError("duplicate daily valuation symbol")
        if not self.valuation_observed and self.rows:
            raise ValueError("unobserved valuations cannot contain rows")
        return self


class DailyValuationPITQuery(RuntimeContractModel):
    ts_code: TsCode
    decision_date: date


class DailyValuationPITSelection(RuntimeContractModel):
    status: Literal["selected", "unknown"]
    reason: str
    trade_date: date | None = None
    pe_ttm: float | None = None
    pb: float | None = None
    dv_ttm: float | None = None
    observed_at: AwareUtcDatetime | None = None
    first_observed_at: AwareUtcDatetime | None = None
    source_batch_id: Sha256Hex | None = None
    row_sha256: Sha256Hex | None = None
    candidate_generation_id: Sha256Hex | None = None


def _row_sha256(row: DailyValuationRow) -> str:
    return canonical_sha256(row.model_dump(mode="python"))


def _stored_batch(conn: duckdb.DuckDBPyConnection, candidate_id: str) -> tuple[object, ...] | None:
    return conn.execute(
        "SELECT source_generation_id, source_sequence, source_batch_id, revision, "
        "trade_date, observed_at, valuation_observed "
        "FROM daily_basic_valuation_batch WHERE candidate_generation_id = ?",
        [candidate_id],
    ).fetchone()


def _record_daily_valuation_batch_in_transaction(
    conn: duckdb.DuckDBPyConnection,
    batch: DailyValuationBatch,
) -> int:
    """Append one signed-candidate projection inside the caller's write transaction."""

    expected_batch = (
        batch.source_generation_id,
        batch.source_sequence,
        batch.source_batch_id,
        batch.revision,
        batch.trade_date,
        batch.observed_at,
        batch.valuation_observed,
    )
    existing = _stored_batch(conn, batch.candidate_generation_id)
    if existing is not None:
        if existing != expected_batch:
            raise ValueError("daily valuation candidate replay conflicts with stored batch")
        stored_rows = conn.execute(
            "SELECT ts_code, trade_date, observed_at, row_sha256, pe_ttm, pb, dv_ttm "
            "FROM daily_basic_valuation_observation WHERE candidate_generation_id = ? "
            "ORDER BY ts_code",
            [batch.candidate_generation_id],
        ).fetchall()
        expected_rows = [
            (
                row.ts_code,
                row.trade_date,
                batch.observed_at,
                _row_sha256(row),
                row.pe_ttm,
                row.pb,
                row.dv_ttm,
            )
            for row in sorted(batch.rows, key=lambda item: item.ts_code)
        ]
        if stored_rows != expected_rows:
            raise ValueError("daily valuation candidate replay conflicts with stored rows")
        return 0

    latest = conn.execute(
        "SELECT source_generation_id, source_sequence, revision, observed_at "
        "FROM daily_basic_valuation_batch WHERE trade_date = ? "
        "ORDER BY revision DESC LIMIT 1",
        [batch.trade_date],
    ).fetchone()
    if latest is not None and not (
        latest[0] == batch.source_generation_id
        and latest[1] < batch.source_sequence
        and latest[2] < batch.revision
        and latest[3] <= batch.observed_at
    ):
        raise ValueError("daily valuation source revision or observation time regressed")
    conn.execute(
        "INSERT INTO daily_basic_valuation_batch VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            batch.candidate_generation_id,
            *expected_batch,
        ),
    )
    for row in batch.rows:
        digest = _row_sha256(row)
        prior = conn.execute(
            "SELECT MIN(observed_at) FROM daily_basic_valuation_observation "
            "WHERE ts_code = ? AND trade_date = ? AND row_sha256 = ?",
            [row.ts_code, row.trade_date, digest],
        ).fetchone()
        first_observed_at = (
            prior[0] if prior is not None and prior[0] is not None else batch.observed_at
        )
        conn.execute(
            "INSERT INTO daily_basic_valuation_observation "
            "(candidate_generation_id, ts_code, trade_date, observed_at, first_observed_at, "
            "row_sha256, pe_ttm, pb, dv_ttm) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                batch.candidate_generation_id,
                row.ts_code,
                row.trade_date,
                batch.observed_at,
                first_observed_at,
                digest,
                row.pe_ttm,
                row.pb,
                row.dv_ttm,
            ],
        )
    return len(batch.rows)


def _unknown(reason: str) -> DailyValuationPITSelection:
    return DailyValuationPITSelection(status="unknown", reason=reason)


def query_daily_valuation_pit(
    conn: duckdb.DuckDBPyConnection,
    query: DailyValuationPITQuery,
    *,
    max_candidate_batches: int = _MAX_CANDIDATE_BATCHES,
) -> DailyValuationPITSelection:
    """Select the sole previous SSE session's row as known at T 17:00 Shanghai."""

    if (
        type(max_candidate_batches) is not int
        or not 1 <= max_candidate_batches <= _MAX_CANDIDATE_BATCHES
    ):
        raise ValueError("daily valuation candidate bound is invalid")
    decision_day = query.decision_date
    decision_row = conn.execute(
        "SELECT is_open FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = ?",
        [decision_day],
    ).fetchone()
    if decision_row is None:
        return _unknown("incomplete_calendar")
    if not decision_row[0]:
        return _unknown("decision_not_open")
    previous = conn.execute(
        "SELECT MAX(cal_date) FROM trade_calendar "
        "WHERE exchange = 'SSE' AND is_open = TRUE AND cal_date < ?",
        [decision_day],
    ).fetchone()[0]
    if previous is None:
        return _unknown("no_previous_session")
    expected_days = (decision_day - previous).days + 1
    actual_days = conn.execute(
        "SELECT COUNT(*) FROM trade_calendar WHERE exchange = 'SSE' AND cal_date BETWEEN ? AND ?",
        [previous, decision_day],
    ).fetchone()[0]
    if actual_days != expected_days:
        return _unknown("incomplete_calendar")
    as_of_local = datetime.combine(decision_day, time(17, 0), tzinfo=_SHANGHAI)
    next_open_at = datetime.combine(decision_day, time(9, 30), tzinfo=_SHANGHAI)
    if as_of_local < next_open_at:
        return _unknown("before_next_session")
    as_of = as_of_local.astimezone(UTC)
    batches = conn.execute(
        "SELECT candidate_generation_id, source_batch_id, source_sequence, observed_at, "
        "valuation_observed FROM daily_basic_valuation_batch "
        "WHERE trade_date = ? AND observed_at <= ? "
        "ORDER BY observed_at DESC, source_sequence DESC LIMIT ?",
        [previous, as_of, max_candidate_batches + 1],
    ).fetchall()
    if len(batches) > max_candidate_batches:
        return _unknown("candidate_limit")
    if not batches:
        return _unknown("no_observed_batch")
    candidate_id, source_batch_id, _, batch_at, valuation_observed = batches[0]
    if not valuation_observed:
        return _unknown("valuation_not_observed")
    observed = conn.execute(
        "SELECT trade_date, observed_at, first_observed_at, row_sha256, pe_ttm, pb, dv_ttm "
        "FROM daily_basic_valuation_observation "
        "WHERE candidate_generation_id = ? AND ts_code = ? AND trade_date = ?",
        [candidate_id, query.ts_code, previous],
    ).fetchone()
    if observed is None:
        return _unknown("missing_symbol_row")
    trade_date, observed_at, first_observed_at, digest, pe_ttm, pb, dv_ttm = observed
    try:
        row = DailyValuationRow(
            ts_code=query.ts_code,
            trade_date=trade_date,
            pe_ttm=pe_ttm,
            pb=pb,
            dv_ttm=dv_ttm,
        )
    except ValidationError:
        return _unknown("invalid_observation_evidence")
    earliest = conn.execute(
        "SELECT MIN(observed_at) FROM daily_basic_valuation_observation "
        "WHERE ts_code = ? AND trade_date = ? AND row_sha256 = ?",
        [query.ts_code, previous, digest],
    ).fetchone()[0]
    if (
        observed_at != batch_at
        or _row_sha256(row) != digest
        or earliest != first_observed_at
        or not first_observed_at <= observed_at <= as_of
    ):
        return _unknown("invalid_observation_evidence")
    return DailyValuationPITSelection(
        status="selected",
        reason="visible",
        trade_date=trade_date,
        pe_ttm=pe_ttm,
        pb=pb,
        dv_ttm=dv_ttm,
        observed_at=observed_at,
        first_observed_at=first_observed_at,
        source_batch_id=source_batch_id,
        row_sha256=digest,
        candidate_generation_id=candidate_id,
    )
