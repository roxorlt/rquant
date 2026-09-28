"""Exact retrospective daily prices from a frozen DuckDB snapshot."""

from __future__ import annotations

import math
import re
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Literal, Self

import duckdb
from pydantic import Field, model_validator

from rquant.backtest.contracts import Sha256
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

MAX_REQUEST_DAYS = 3660
MAX_REQUEST_CODES = 500
MAX_REQUEST_PAIRS = 20_000
MAX_SQL_BATCH = 200
_SOURCE_MODE = "retrospective_daily_bar"
_SOURCE_TABLE = "daily_bar"
_CODE_PATTERN = re.compile(r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
_REQUIRED_COLUMNS = {
    "ts_code": ("VARCHAR", True),
    "trade_date": ("DATE", True),
    "open": ("DOUBLE", False),
    "close": ("DOUBLE", False),
    "pre_close": ("DOUBLE", False),
}


class DailyPriceSourceError(ValueError):
    """The frozen file cannot prove every requested daily price fact."""


class DailyPricePair(RuntimeContractModel):
    trade_date: date
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")


class RetrospectiveDailyPrice(RuntimeContractModel):
    trade_date: date
    ts_code: str = Field(pattern=r"^[0-9]{6}\.(?:SH|SZ|BJ)$")
    open: float = Field(gt=0, allow_inf_nan=False)
    close: float = Field(gt=0, allow_inf_nan=False)
    pre_close: float = Field(gt=0, allow_inf_nan=False)


def _identity(
    requested_pairs: tuple[DailyPricePair, ...], rows: tuple[RetrospectiveDailyPrice, ...]
) -> str:
    return canonical_sha256(
        {
            "source_mode": _SOURCE_MODE,
            "source_table": _SOURCE_TABLE,
            "requested_pairs": requested_pairs,
            "rows": rows,
        }
    )


class RetrospectiveDailyPriceSnapshot(RuntimeContractModel):
    """Complete requested OHLC facts, with no historical observation or fill assertion."""

    source_mode: Literal["retrospective_daily_bar"] = _SOURCE_MODE
    source_table: Literal["daily_bar"] = _SOURCE_TABLE
    source_identity: Sha256
    requested_pairs: tuple[DailyPricePair, ...] = Field(min_length=1)
    rows: tuple[RetrospectiveDailyPrice, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def validate_facts(self) -> Self:
        requested = tuple((pair.trade_date, pair.ts_code) for pair in self.requested_pairs)
        actual = tuple((row.trade_date, row.ts_code) for row in self.rows)
        if requested != tuple(sorted(set(requested))) or actual != requested:
            raise ValueError("daily price rows must exactly match ordered requested pairs")
        if self.source_identity != _identity(self.requested_pairs, self.rows):
            raise ValueError("daily price source identity disagrees with frozen facts")
        return self


def _check_requested_pairs(
    requested_pairs: Sequence[tuple[date, str]],
) -> tuple[DailyPricePair, ...]:
    if not isinstance(requested_pairs, Sequence) or isinstance(requested_pairs, (str, bytes)):
        raise DailyPriceSourceError("daily price request must be a bounded sequence of pairs")
    if not requested_pairs or len(requested_pairs) > MAX_REQUEST_PAIRS:
        raise DailyPriceSourceError(
            f"daily price request exceeds {MAX_REQUEST_PAIRS} pairs or is empty"
        )
    checked: list[DailyPricePair] = []
    for pair in requested_pairs:
        if not isinstance(pair, tuple) or len(pair) != 2 or type(pair[0]) is not date:
            raise DailyPriceSourceError(
                "daily price request requires exact civil date and code pairs"
            )
        trade_date, code = pair
        if not isinstance(code, str) or _CODE_PATTERN.fullmatch(code) is None:
            raise DailyPriceSourceError("daily price request contains an invalid stock code")
        checked.append(DailyPricePair(trade_date=trade_date, ts_code=code))
    ordered = tuple(sorted(checked, key=lambda pair: (pair.trade_date, pair.ts_code)))
    requested_keys = [(pair.trade_date, pair.ts_code) for pair in ordered]
    if len(set(requested_keys)) != len(ordered):
        raise DailyPriceSourceError("duplicate request daily price pair")
    if (ordered[-1].trade_date - ordered[0].trade_date).days + 1 > MAX_REQUEST_DAYS:
        raise DailyPriceSourceError(f"daily price request exceeds {MAX_REQUEST_DAYS} civil dates")
    if len({pair.ts_code for pair in ordered}) > MAX_REQUEST_CODES:
        raise DailyPriceSourceError(f"daily price request exceeds {MAX_REQUEST_CODES} codes")
    return ordered


def _check_schema(connection: duckdb.DuckDBPyConnection) -> None:
    try:
        columns = connection.execute("PRAGMA table_info('daily_bar')").fetchall()
    except duckdb.CatalogException as exc:
        raise DailyPriceSourceError("frozen daily_bar schema is missing or incompatible") from exc
    actual = {
        str(name): (str(data_type), bool(not_null)) for _, name, data_type, not_null, *_ in columns
    }
    if any(actual.get(name) != required for name, required in _REQUIRED_COLUMNS.items()):
        raise DailyPriceSourceError("frozen daily_bar schema is missing or incompatible")


def _price(value: object, column: str, trade_date: date, code: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
    ):
        raise DailyPriceSourceError(
            f"missing or invalid daily_bar.{column} for {trade_date} {code}"
        )
    return float(value)


def _read_rows(
    connection: duckdb.DuckDBPyConnection, requested_pairs: tuple[DailyPricePair, ...]
) -> tuple[RetrospectiveDailyPrice, ...]:
    _check_schema(connection)
    all_rows: list[RetrospectiveDailyPrice] = []
    for offset in range(0, len(requested_pairs), MAX_SQL_BATCH):
        batch = requested_pairs[offset : offset + MAX_SQL_BATCH]
        placeholders = ", ".join("(?, ?)" for _ in batch)
        parameters: list[object] = []
        for pair in batch:
            parameters.extend((pair.trade_date, pair.ts_code))
        raw = connection.execute(
            "SELECT ts_code, trade_date, open, close, pre_close FROM daily_bar "
            f"WHERE trade_date BETWEEN ? AND ? AND (trade_date, ts_code) IN ({placeholders}) "
            "ORDER BY trade_date, ts_code LIMIT ?",
            [batch[0].trade_date, batch[-1].trade_date, *parameters, len(batch) + 1],
        ).fetchall()
        if len(raw) > len(batch):
            raise DailyPriceSourceError("duplicate daily_bar pair exceeds bounded result")
        found: dict[tuple[date, str], RetrospectiveDailyPrice] = {}
        requested = {(pair.trade_date, pair.ts_code) for pair in batch}
        for code, trade_date, open_price, close, pre_close in raw:
            key = (trade_date, code)
            if key not in requested:
                raise DailyPriceSourceError("unexpected daily_bar pair in bounded result")
            if key in found:
                raise DailyPriceSourceError(f"duplicate daily_bar pair for {trade_date} {code}")
            found[key] = RetrospectiveDailyPrice(
                trade_date=trade_date,
                ts_code=code,
                open=_price(open_price, "open", trade_date, code),
                close=_price(close, "close", trade_date, code),
                pre_close=_price(pre_close, "pre_close", trade_date, code),
            )
        for pair in batch:
            key = (pair.trade_date, pair.ts_code)
            if key not in found:
                raise DailyPriceSourceError(
                    f"missing daily_bar pair for {pair.trade_date} {pair.ts_code}"
                )
            all_rows.append(found[key])
    return tuple(all_rows)


def verify_retrospective_daily_prices(
    connection: duckdb.DuckDBPyConnection,
    requested_pairs: Sequence[tuple[date, str]],
) -> RetrospectiveDailyPriceSnapshot:
    """Verify exact price facts inside the caller's existing read-only transaction."""
    checked = _check_requested_pairs(requested_pairs)
    try:
        rows = _read_rows(connection, checked)
    except duckdb.Error as exc:
        raise DailyPriceSourceError("frozen daily_bar is unavailable") from exc
    return RetrospectiveDailyPriceSnapshot(
        source_identity=_identity(checked, rows), requested_pairs=checked, rows=rows
    )


def load_retrospective_daily_prices(
    frozen_path: Path,
    requested_pairs: Sequence[tuple[date, str]],
) -> RetrospectiveDailyPriceSnapshot:
    """Use one read-only connection and transaction for a frozen local file."""
    _check_requested_pairs(requested_pairs)
    if not frozen_path.is_file():
        raise DailyPriceSourceError("an existing frozen DuckDB file is required")
    try:
        connection = duckdb.connect(str(frozen_path), read_only=True)
    except duckdb.Error as exc:
        raise DailyPriceSourceError("frozen DuckDB cannot be opened read-only") from exc
    try:
        connection.execute("BEGIN TRANSACTION")
        try:
            snapshot = verify_retrospective_daily_prices(connection, requested_pairs)
        except Exception:
            connection.execute("ROLLBACK")
            raise
        connection.execute("COMMIT")
        return snapshot
    except duckdb.Error as exc:
        raise DailyPriceSourceError("frozen daily_bar is unavailable") from exc
    finally:
        connection.close()
