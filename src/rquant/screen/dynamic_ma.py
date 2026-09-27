"""Bounded, same-connection MA values for periods absent from daily_indicator."""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from datetime import date

import duckdb
import numpy as np
import pandas as pd

FIXED_MA_PERIODS = frozenset({5, 10, 20, 60})
MAX_DYNAMIC_MA_FACTS = 8_000 * 281
_MA_COLUMN = re.compile(r"MA([1-9][0-9]*)\[(0|[1-9][0-9]*)\]\Z")
_FETCH_ROWS = 8_192


class DynamicMaFactError(RuntimeError):
    """The bounded source contains duplicate or invalid MA facts."""


def requested_dynamic_ma(columns: Collection[str]) -> dict[str, tuple[int, int]]:
    """Select only valid, non-persisted MA dependencies from registered rules."""
    selected: dict[str, tuple[int, int]] = {}
    for column in columns:
        if not isinstance(column, str):
            continue
        match = _MA_COLUMN.fullmatch(column)
        if match is None:
            continue
        period, offset = (int(value) for value in match.groups())
        if period in FIXED_MA_PERIODS:
            continue
        if period < 2 or period > 250 or offset > 30:
            raise ValueError(f"unsupported dynamic MA dependency: {column}")
        selected[column] = (period, offset)
    return selected


def dynamic_ma_day_count(columns: Mapping[str, tuple[int, int]]) -> int:
    return max((period + offset for period, offset in columns.values()), default=0)


def _read_matrix(
    connection: duckdb.DuckDBPyConnection,
    *,
    table: str,
    value_column: str,
    dates: Sequence[str],
    ts_codes: Sequence[str],
) -> np.ndarray:
    shape = (len(dates), len(ts_codes))
    values = np.full(shape, np.nan, dtype=np.float64)
    seen = np.zeros(shape, dtype=np.bool_)
    code_index = {code: index for index, code in enumerate(ts_codes)}
    date_index = {date.fromisoformat(day): index for index, day in enumerate(dates)}
    query = connection.execute(
        f"SELECT ts_code, trade_date, {value_column} FROM {table} "
        "WHERE ts_code = ANY(?) AND trade_date BETWEEN ? AND ?",
        [list(ts_codes), dates[-1], dates[0]],
    )
    row_count = 0
    while batch := query.fetchmany(_FETCH_ROWS):
        row_count += len(batch)
        if row_count > MAX_DYNAMIC_MA_FACTS:
            raise DynamicMaFactError("dynamic MA facts exceed the bounded read")
        for code, trade_date, value in batch:
            code_slot = code_index.get(code)
            day_slot = date_index.get(trade_date)
            if code_slot is None or day_slot is None:
                raise DynamicMaFactError("dynamic MA fact is outside the trade calendar")
            if seen[day_slot, code_slot]:
                raise DynamicMaFactError("duplicate dynamic MA fact")
            seen[day_slot, code_slot] = True
            if value is not None:
                numeric = float(value)
                if np.isfinite(numeric) and numeric > 0:
                    values[day_slot, code_slot] = numeric
    return values


def derive_requested_ma(
    connection: duckdb.DuckDBPyConnection,
    *,
    dates: Sequence[str],
    ts_codes: Sequence[str],
    columns: Mapping[str, tuple[int, int]],
) -> pd.DataFrame:
    """SMA(close × factor) / target factor, with full-window coverage required."""
    if not columns:
        return pd.DataFrame({"ts_code": list(ts_codes)})
    if not dates or len(set(ts_codes)) != len(ts_codes):
        raise DynamicMaFactError("dynamic MA source has duplicate stock identity")
    if len(dates) * len(ts_codes) > MAX_DYNAMIC_MA_FACTS:
        raise ValueError("dynamic MA fact budget exceeded")
    prices = _read_matrix(
        connection, table="daily_bar", value_column="close",
        dates=dates, ts_codes=ts_codes,
    )
    factors = _read_matrix(
        connection, table="adj_factor", value_column="adj_factor",
        dates=dates, ts_codes=ts_codes,
    )
    with np.errstate(over="ignore", invalid="ignore"):
        adjusted = prices * factors
    result: dict[str, object] = {"ts_code": list(ts_codes)}
    for column, (period, offset) in columns.items():
        with np.errstate(over="ignore", divide="ignore", invalid="ignore"):
            average = adjusted[offset:offset + period].mean(axis=0)
            values = average / factors[offset]
        values[~np.isfinite(values)] = np.nan
        result[column] = values
    return pd.DataFrame(result)
