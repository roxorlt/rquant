"""Requested MA periods derive from the same bounded replica facts as screening."""

from __future__ import annotations

import os
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from ta.trend import SMAIndicator

from rquant.replica_generation import (
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.screen.replica_source import (
    ScreenReplicaBudgetError,
    ScreenReplicaDataError,
    ScreenReplicaUnavailableError,
    VerifiedReplicaScreenSource,
)
from rquant.screen.rules import above_ma, gt
from rquant.storage.duckdb import DuckDBStore

_CODE = "600001.SH"
_NEW_CODE = "600002.SH"


def _publish(primary: Path, replica: Path) -> None:
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )


def _world(
    tmp_path: Path, *, days: int = 35, with_new_stock: bool = False,
) -> tuple[VerifiedReplicaScreenSource, Path, Path, list[date], list[float], list[float]]:
    primary = tmp_path / "rquant.duckdb"
    replica = tmp_path / "rquant_ro.duckdb"
    dates = [date(2026, 4, 15) - timedelta(days=offset) for offset in range(days)]
    prices = [20.0 - 0.02 * offset for offset in range(days)]
    factors = [1.0 + 0.005 * offset for offset in range(days)]
    with DuckDBStore(primary) as store:
        store._conn.executemany(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, TRUE, 'fixture', ?)",
            [(day, datetime(2026, 4, 16, tzinfo=UTC)) for day in dates],
        )
        store._conn.executemany(
            "INSERT INTO daily_bar (ts_code, trade_date, close, pct_chg) "
            "VALUES (?, ?, ?, 1)",
            [(_CODE, day, price) for day, price in zip(dates, prices, strict=True)]
            + ([(_NEW_CODE, day, 10.0) for day in dates[:4]] if with_new_stock else []),
        )
        store._conn.executemany(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) VALUES (?, ?, ?)",
            [(_CODE, day, factor) for day, factor in zip(dates, factors, strict=True)]
            + ([(_NEW_CODE, day, 1.0) for day in dates[:4]] if with_new_stock else []),
        )
        adjusted = pd.Series(
            [price * factor for price, factor in zip(prices, factors, strict=True)][::-1],
            dtype="float64",
        )
        fixed = {
            period: float(
                SMAIndicator(adjusted, window=period).sma_indicator().iloc[-1]
                / factors[0]
            )
            for period in (5, 10, 20)
        }
        store._conn.execute(
            "INSERT INTO daily_indicator (ts_code, trade_date, ma5, ma10, ma20) "
            "VALUES (?, ?, ?, ?, ?)",
            [_CODE, dates[0], fixed[5], fixed[10], fixed[20]],
        )
    _publish(primary, replica)
    return (
        VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica),
        primary, replica, dates, prices, factors,
    )


def _expected(prices: list[float], factors: list[float], period: int, offset: int) -> float:
    adjusted = pd.Series(
        [price * factor for price, factor in zip(prices, factors, strict=True)][::-1],
        dtype="float64",
    )
    return float(
        SMAIndicator(adjusted, window=period).sma_indicator().iloc[-1 - offset]
        / factors[offset]
    )


def test_requested_dynamic_periods_and_offsets_use_adjusted_sma(tmp_path: Path) -> None:
    source, _, _, dates, prices, factors = _world(tmp_path)

    result = source.load(
        dates[0], [above_ma(7, offset=2), gt("MA12[1]", "MA3[0]")],
    )

    frame = result.frame
    assert frame.loc[0, "MA7[2]"] == pytest.approx(_expected(prices, factors, 7, 2))
    assert frame.loc[0, "MA12[1]"] == pytest.approx(_expected(prices, factors, 12, 1))
    assert frame.loc[0, "MA3[0]"] == pytest.approx(_expected(prices, factors, 3, 0))
    assert "MA8[0]" not in frame.columns
    assert "CLOSE[11]" not in frame.columns


def test_fixed_period_stays_persisted_and_dynamic_matches_its_sma_definition(
    tmp_path: Path,
) -> None:
    source, _, _, dates, prices, factors = _world(tmp_path)
    frame = source.load(dates[0], [gt("MA5[0]", "MA7[0]")]).frame

    assert frame.loc[0, "MA5[0]"] == pytest.approx(_expected(prices, factors, 5, 0))
    assert frame.loc[0, "MA7[0]"] == pytest.approx(_expected(prices, factors, 7, 0))


def test_period_250_and_offset_30_use_only_their_bounded_history(tmp_path: Path) -> None:
    source, _, _, dates, prices, factors = _world(tmp_path, days=280)
    frame = source.load(dates[0], [above_ma(250, offset=30)]).frame

    assert frame.loc[0, "MA250[30]"] == pytest.approx(
        _expected(prices, factors, 250, 30)
    )
    assert "CLOSE[279]" not in frame.columns


def test_new_listing_missing_bar_or_factor_is_unknown_per_stock(tmp_path: Path) -> None:
    source, primary, replica, dates, _, _ = _world(tmp_path, with_new_stock=True)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "DELETE FROM daily_bar WHERE ts_code = ? AND trade_date = ?",
            [_CODE, dates[3]],
        )
    _publish(primary, replica)
    frame = source.load(dates[0], [above_ma(7)]).frame.set_index("ts_code")
    assert pd.isna(frame.loc[_CODE, "MA7[0]"])
    assert pd.isna(frame.loc[_NEW_CODE, "MA7[0]"])

    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close, pct_chg) "
            "VALUES (?, ?, 19.94, 1)",
            [_CODE, dates[3]],
        )
        store._conn.execute(
            "DELETE FROM adj_factor WHERE ts_code = ? AND trade_date = ?",
            [_CODE, dates[4]],
        )
    _publish(primary, replica)
    frame = source.load(dates[0], [above_ma(7)]).frame.set_index("ts_code")
    assert pd.isna(frame.loc[_CODE, "MA7[0]"])


def test_missing_dynamic_calendar_is_rejected(tmp_path: Path) -> None:
    source, primary, replica, dates, _, _ = _world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute("DELETE FROM trade_calendar WHERE cal_date = ?", [dates[9]])
    _publish(primary, replica)

    with pytest.raises(ScreenReplicaDataError):
        source.load(dates[0], [above_ma(12)])


@pytest.mark.parametrize("table", ["daily_bar", "adj_factor"])
def test_duplicate_dynamic_fact_is_rejected(tmp_path: Path, table: str) -> None:
    source, primary, replica, dates, _, _ = _world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(f"CREATE TABLE duplicate_facts AS SELECT * FROM {table}")
        store._conn.execute(
            f"INSERT INTO duplicate_facts SELECT * FROM {table} "
            "WHERE ts_code = ? AND trade_date = ?",
            [_CODE, dates[4]],
        )
        store._conn.execute(f"DROP TABLE {table}")
        store._conn.execute(f"ALTER TABLE duplicate_facts RENAME TO {table}")
    _publish(primary, replica)

    with pytest.raises(ScreenReplicaDataError):
        source.load(dates[0], [above_ma(7)])


def test_dynamic_fact_budget_rejects_before_history_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.screen.replica_source as replica_source

    source, _, _, dates, _, _ = _world(tmp_path)
    monkeypatch.setattr(replica_source, "MAX_DYNAMIC_MA_FACTS", 6)
    monkeypatch.setattr(
        replica_source,
        "load_universe",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("history queried")),
    )
    with pytest.raises(ScreenReplicaBudgetError):
        source.load(dates[0], [above_ma(7)])


def test_non_text_dependency_stays_a_validation_error(tmp_path: Path) -> None:
    source, _, _, dates, _, _ = _world(tmp_path)
    with pytest.raises(ValueError, match="unsupported screen dependency"):
        source.load(dates[0], [], include_columns=[5])  # type: ignore[list-item]


def test_dynamic_read_still_discards_generation_changed_during_query(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.screen.replica_source as replica_source

    source, _, replica, dates, _, _ = _world(tmp_path)
    original = replica_source.load_universe

    def replace(*args, **kwargs):
        result = original(*args, **kwargs)
        replacement = tmp_path / "new.duckdb"
        shutil.copy2(replica, replacement)
        os.replace(replacement, replica)
        return result

    monkeypatch.setattr(replica_source, "load_universe", replace)
    with pytest.raises(ScreenReplicaUnavailableError):
        source.load(dates[0], [above_ma(7)])
