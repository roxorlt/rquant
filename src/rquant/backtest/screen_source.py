"""Verify one historical screen result from an existing frozen DuckDB file."""

from __future__ import annotations

import math
import re
from datetime import date, datetime, time
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

import duckdb
from pydantic import Field, ValidationError

from rquant.pool_result_receipt import (
    ScreenRunReceipt,
    member_price_digest,
    member_rank_digest,
    member_set_digest,
)
from rquant.runtime_contracts import RuntimeContractModel

MAX_SCREEN_CANDIDATES = 10_000
_SHANGHAI = ZoneInfo("Asia/Shanghai")
_RECEIPT_COLUMNS = (
    "trade_date",
    "preset_name",
    "definition_version",
    "parent_trade_date",
    "parent_result_version",
    "hit_count",
    "member_digest",
    "lineage_complete",
    "completed_at",
    "result_version",
    "contract",
    "price_digest",
)


class ScreenCandidateSourceError(ValueError):
    """The frozen file cannot prove the requested pre-open candidate set."""


class VerifiedScreenCandidate(RuntimeContractModel):
    ts_code: str = Field(min_length=1)
    previous_close: float = Field(gt=0, allow_inf_nan=False)


class VerifiedScreenCandidateSnapshot(RuntimeContractModel):
    source_mode: Literal["captured_receipt"] = "captured_receipt"
    source_trade_date: date
    decision_trade_date: date
    preset_name: str = Field(min_length=1)
    receipt: ScreenRunReceipt
    candidates: tuple[VerifiedScreenCandidate, ...]

    @property
    def result_version(self) -> str:
        assert self.receipt.result_version is not None
        return self.receipt.result_version

    @property
    def completed_at(self) -> datetime:
        return self.receipt.completed_at


class VerifiedRankedScreenCandidate(VerifiedScreenCandidate):
    rank_position: int = Field(ge=1, strict=True)
    ranking_score: float = Field(ge=0, le=100, allow_inf_nan=False)


class VerifiedRankedScreenCandidateSnapshot(RuntimeContractModel):
    source_mode: Literal["captured_receipt"] = "captured_receipt"
    source_trade_date: date
    decision_trade_date: date
    preset_name: str = Field(min_length=1)
    receipt: ScreenRunReceipt
    candidates: tuple[VerifiedRankedScreenCandidate, ...]

    @property
    def result_version(self) -> str:
        assert self.receipt.result_version is not None
        return self.receipt.result_version

    @property
    def completed_at(self) -> datetime:
        return self.receipt.completed_at


def _read_receipt(
    connection: duckdb.DuckDBPyConnection,
    source_trade_date: date,
    preset_name: str,
    *,
    ranked: bool = False,
) -> ScreenRunReceipt:
    columns = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info('screen_run_receipt')").fetchall()
    }
    if not {"contract", "price_digest"} <= columns:
        raise ScreenCandidateSourceError("screen receipt v2 price proof columns are missing")
    if not set(_RECEIPT_COLUMNS) <= columns:
        raise ScreenCandidateSourceError("screen receipt columns are missing")
    if ranked and "rank_digest" not in columns:
        raise ScreenCandidateSourceError("screen receipt v3 rank proof column is missing")
    selected = _RECEIPT_COLUMNS + (("rank_digest",) if "rank_digest" in columns else ())
    rows = connection.execute(
        f"SELECT {', '.join(selected)} FROM screen_run_receipt "
        "WHERE trade_date = ? AND preset_name = ? LIMIT 2",
        [source_trade_date, preset_name],
    ).fetchall()
    if len(rows) != 1:
        raise ScreenCandidateSourceError("expected exactly one screen receipt")
    try:
        receipt = ScreenRunReceipt.model_validate(dict(zip(selected, rows[0], strict=True)))
    except ValidationError as exc:
        raise ScreenCandidateSourceError("invalid screen receipt or result version") from exc
    expected_contract = "screen-run-receipt/v3" if ranked else "screen-run-receipt/v2"
    if receipt.contract != expected_contract or receipt.price_digest is None:
        raise ScreenCandidateSourceError(f"screen receipt {expected_contract} proof is required")
    return receipt


def _verify_snapshot(
    connection: duckdb.DuckDBPyConnection,
    receipt: ScreenRunReceipt,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
) -> VerifiedScreenCandidateSnapshot:
    if not receipt.lineage_complete:
        raise ScreenCandidateSourceError("screen receipt lineage is incomplete")
    close = datetime.combine(source_trade_date, time(15), tzinfo=_SHANGHAI)
    cutoff = datetime.combine(decision_trade_date, time(9, 25), tzinfo=_SHANGHAI)
    if receipt.completed_at < close:
        raise ScreenCandidateSourceError("screen receipt predates the source-day close")
    if receipt.completed_at >= cutoff:
        raise ScreenCandidateSourceError("screen receipt missed the decision-day 09:25 cutoff")
    if receipt.hit_count > MAX_SCREEN_CANDIDATES:
        raise ScreenCandidateSourceError("screen candidate count exceeds the limit")

    rows = connection.execute(
        "SELECT ts_code, close FROM screen_result "
        "WHERE trade_date = ? AND preset_name = ? "
        "ORDER BY ts_code LIMIT ?",
        [source_trade_date, preset_name, MAX_SCREEN_CANDIDATES + 1],
    ).fetchall()
    if len(rows) != receipt.hit_count:
        raise ScreenCandidateSourceError("screen result hit count differs from receipt")
    codes = [code for code, _ in rows]
    if len(codes) != len(set(codes)):
        raise ScreenCandidateSourceError("screen result contains duplicate codes")
    if any(not isinstance(code, str) or not code.strip() for code in codes):
        raise ScreenCandidateSourceError("screen result contains an invalid code")
    if member_set_digest(codes) != receipt.member_digest:
        raise ScreenCandidateSourceError("screen result member digest differs from receipt")
    for _, price in rows:
        if not isinstance(price, float) or not math.isfinite(price) or price <= 0:
            raise ScreenCandidateSourceError("screen result has a missing or invalid close")
    if member_price_digest(rows) != receipt.price_digest:
        raise ScreenCandidateSourceError("screen result price digest differs from receipt")

    return VerifiedScreenCandidateSnapshot(
        source_trade_date=source_trade_date,
        decision_trade_date=decision_trade_date,
        preset_name=preset_name,
        receipt=receipt,
        candidates=tuple(
            VerifiedScreenCandidate(ts_code=code, previous_close=price) for code, price in rows
        ),
    )


def load_verified_screen_candidates(
    frozen_path: Path,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
) -> VerifiedScreenCandidateSnapshot:
    """Read receipt and rows in one read-only snapshot; dates use Asia/Shanghai market time."""
    if not frozen_path.is_file():
        raise ScreenCandidateSourceError("an existing frozen DuckDB file is required")
    if source_trade_date >= decision_trade_date:
        raise ScreenCandidateSourceError("decision date must follow the source date")
    if not preset_name.strip():
        raise ScreenCandidateSourceError("a screen preset is required")
    try:
        connection = duckdb.connect(str(frozen_path), read_only=True)
    except duckdb.Error as exc:
        raise ScreenCandidateSourceError("frozen DuckDB cannot be opened read-only") from exc
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            snapshot = verify_screen_candidates(
                connection, source_trade_date, decision_trade_date, preset_name
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return snapshot
    except duckdb.Error as exc:
        raise ScreenCandidateSourceError("frozen screen tables are unavailable") from exc
    finally:
        connection.close()


def verify_screen_candidates(
    connection: duckdb.DuckDBPyConnection,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
) -> VerifiedScreenCandidateSnapshot:
    """Verify a receipt and its rows inside the caller's read-only transaction."""
    if source_trade_date >= decision_trade_date:
        raise ScreenCandidateSourceError("decision date must follow the source date")
    if not preset_name.strip():
        raise ScreenCandidateSourceError("a screen preset is required")
    try:
        receipt = _read_receipt(connection, source_trade_date, preset_name)
        return _verify_snapshot(
            connection, receipt, source_trade_date, decision_trade_date, preset_name
        )
    except duckdb.Error as exc:
        raise ScreenCandidateSourceError("frozen screen tables are unavailable") from exc


def _assert_expected_definition(expected_definition_version: str | None) -> str:
    if (
        not isinstance(expected_definition_version, str)
        or re.fullmatch(r"[0-9a-f]{64}", expected_definition_version) is None
    ):
        raise ScreenCandidateSourceError("a trusted expected definition version is required")
    return expected_definition_version


def verify_ranked_screen_candidates(
    connection: duckdb.DuckDBPyConnection,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
    *,
    expected_definition_version: str | None = None,
) -> VerifiedRankedScreenCandidateSnapshot:
    """Verify ranked facts in the caller's one frozen read-only transaction."""
    expected = _assert_expected_definition(expected_definition_version)
    if source_trade_date >= decision_trade_date:
        raise ScreenCandidateSourceError("decision date must follow the source date")
    if not preset_name.strip():
        raise ScreenCandidateSourceError("a screen preset is required")
    try:
        receipt = _read_receipt(connection, source_trade_date, preset_name, ranked=True)
        if receipt.definition_version != expected:
            raise ScreenCandidateSourceError(
                "screen receipt differs from expected definition version"
            )
        base = _verify_snapshot(
            connection, receipt, source_trade_date, decision_trade_date, preset_name
        )
        columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info('screen_result')").fetchall()
        }
        if not {"rank_position", "ranking_score"} <= columns:
            raise ScreenCandidateSourceError("screen result ranking columns are missing")
        rows = connection.execute(
            "SELECT ts_code, rank_position, ranking_score FROM screen_result "
            "WHERE trade_date = ? AND preset_name = ? ORDER BY ts_code LIMIT ?",
            [source_trade_date, preset_name, MAX_SCREEN_CANDIDATES + 1],
        ).fetchall()
        prices = {item.ts_code: item.previous_close for item in base.candidates}
        if len(rows) != len(prices) or {code for code, _, _ in rows} != set(prices):
            raise ScreenCandidateSourceError("screen ranking members differ from price proof")
        try:
            digest = member_rank_digest(rows)
        except ValueError as exc:
            raise ScreenCandidateSourceError("invalid screen ranking rows") from exc
        if digest != receipt.rank_digest:
            raise ScreenCandidateSourceError("screen ranking digest differs from receipt")
        return VerifiedRankedScreenCandidateSnapshot(
            source_trade_date=source_trade_date,
            decision_trade_date=decision_trade_date,
            preset_name=preset_name,
            receipt=receipt,
            candidates=tuple(
                VerifiedRankedScreenCandidate(
                    ts_code=code,
                    previous_close=prices[code],
                    rank_position=position,
                    ranking_score=score,
                )
                for code, position, score in sorted(rows, key=lambda row: row[1])
            ),
        )
    except duckdb.Error as exc:
        raise ScreenCandidateSourceError("frozen screen tables are unavailable") from exc


def load_verified_ranked_screen_candidates(
    frozen_path: Path,
    source_trade_date: date,
    decision_trade_date: date,
    preset_name: str,
    *,
    expected_definition_version: str | None = None,
) -> VerifiedRankedScreenCandidateSnapshot:
    """Read a v3 ranked result from one existing frozen DuckDB file and transaction."""
    _assert_expected_definition(expected_definition_version)
    if not frozen_path.is_file():
        raise ScreenCandidateSourceError("an existing frozen DuckDB file is required")
    try:
        connection = duckdb.connect(str(frozen_path), read_only=True)
    except duckdb.Error as exc:
        raise ScreenCandidateSourceError("frozen DuckDB cannot be opened read-only") from exc
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            snapshot = verify_ranked_screen_candidates(
                connection, source_trade_date, decision_trade_date, preset_name,
                expected_definition_version=expected_definition_version,
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return snapshot
    except duckdb.Error as exc:
        raise ScreenCandidateSourceError("frozen screen tables are unavailable") from exc
    finally:
        connection.close()
