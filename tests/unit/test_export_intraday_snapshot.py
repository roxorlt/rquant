"""The exported parquet has to satisfy the feature engine, not the exporter's own idea.

The strongest available check is to run `live_compute` over the exported frame as the
historical half — the same call `feature_live` makes — because that is the code that decides
whether a snapshot is usable, and it is far pickier than a column list: whole-minute bars,
finite positive OHLC with consistent geometry, `available_at` never before the bar end, one
bar per ts_code minute, and every historical row strictly before the decision date.
"""

from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import scripts.export_intraday_snapshot as exporter
from rquant.intraday_feature_engine import (
    INPUT_COLUMNS,
    SHANGHAI,
    IntradayFeatureConfig,
    live_compute,
)

COMMIT = "a" * 40
#: Twenty-two weekdays ending 2026-08-31, so twenty sessions can be requested with room to
#: prove the exporter takes the newest ones.
LAST_SESSION = date(2026, 8, 31)


def _sessions(count: int) -> list[date]:
    days: list[date] = []
    cursor = LAST_SESSION
    while len(days) < count:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor -= timedelta(days=1)
    return sorted(days)


def _minute_rows(
    sessions: list[date],
    *,
    source: str,
    freq: str = "1min",
    price_override: float | None = None,
    ts_codes: tuple[str, ...] = ("600000.SH", "600001.SH"),
) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for session in sessions:
        for ts_code in ts_codes:
            for minute in range(3):
                trade_time = datetime.combine(session, datetime.min.time()) + timedelta(
                    hours=9,
                    minutes=31 + minute,
                )
                price = 10.0 + minute if price_override is None else price_override
                rows.append(
                    (
                        ts_code,
                        trade_time,
                        freq,
                        price,
                        price + 0.5,
                        price - 0.5,
                        price + 0.1,
                        1000.0 + minute,
                        (1000.0 + minute) * price,
                        source,
                        trade_time,
                    )
                )
    return rows


def _write_minute_database(
    path: Path,
    *,
    sessions: int = 22,
    with_rt_rows: bool = True,
    with_other_freq: bool = True,
    suspended_session: bool = False,
) -> Path:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE minute_bar (
                ts_code    VARCHAR   NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                freq       VARCHAR   NOT NULL,
                open       DOUBLE,
                high       DOUBLE,
                low        DOUBLE,
                close      DOUBLE,
                vol        DOUBLE,
                amount     DOUBLE,
                source     VARCHAR   DEFAULT 'tushare',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ts_code, trade_time, freq, source)
            )
            """
        )
        days = _sessions(sessions)
        rows = _minute_rows(days, source="tushare")
        if with_rt_rows:
            # The same minutes under the realtime source: the primary key allows it, and an
            # exporter that forgot to filter would emit two bars for one ts_code minute.
            rows += _minute_rows(days, source="tushare_rt")
        if with_other_freq:
            # `minute_bar`'s primary key carries `freq`, so the same source can hold 5min
            # bars on the same minutes. Without these rows the freq filter is untested.
            rows += _minute_rows(days, source="tushare", freq="5min")
        if suspended_session:
            # One suspended name inside the exported window: its minute bars come back as
            # zero prices, which is exactly what `_normalize_frame` refuses.
            rows += _minute_rows(
                [days[-1]],
                source="tushare",
                price_override=0.0,
                ts_codes=("600002.SH",),
            )
        connection.executemany(
            "INSERT INTO minute_bar VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    finally:
        connection.close()
    return path


@pytest.fixture
def exported(tmp_path: Path) -> tuple[Path, str]:
    _write_minute_database(tmp_path / "rquant_ro.duckdb")
    output = tmp_path / "minute-history.parquet"
    assert (
        exporter.main(
            [
                "--database",
                str(tmp_path / "rquant_ro.duckdb"),
                "--output",
                str(output),
                "--sessions",
                "20",
            ]
        )
        == 0
    )
    return output, hashlib.sha256(output.read_bytes()).hexdigest()


def test_the_snapshot_carries_exactly_the_engine_input_columns(exported: tuple[Path, str]) -> None:
    output, _digest = exported

    frame = pd.read_parquet(output)

    assert tuple(frame.columns) == INPUT_COLUMNS


def test_the_snapshot_holds_the_newest_twenty_sessions_of_the_bar_end_source(
    exported: tuple[Path, str],
) -> None:
    output, _digest = exported

    frame = pd.read_parquet(output)
    sessions = sorted({value.date() for value in frame["trade_time"]})

    assert len(sessions) == 20
    assert sessions[-1] == LAST_SESSION
    assert sessions == _sessions(22)[2:]
    # Two ts_codes, three minutes, twenty sessions — and not double that, which is what a
    # missing `source` filter would produce out of the realtime rows in the same table.
    assert len(frame) == 2 * 3 * 20


def test_available_at_never_precedes_the_bar_end(exported: tuple[Path, str]) -> None:
    output, _digest = exported

    frame = pd.read_parquet(output)

    assert (frame["available_at"] >= frame["trade_time"]).all()
    assert (frame["available_at"] == frame["trade_time"]).all()


def test_the_feature_engine_accepts_the_snapshot_as_its_history(
    exported: tuple[Path, str],
) -> None:
    """`live_compute` is the consumer; a snapshot it rejects is not a snapshot."""

    output, _digest = exported
    historical = pd.read_parquet(output)
    decision = datetime(2026, 9, 1, 9, 40, tzinfo=SHANGHAI)
    current = pd.DataFrame(
        [
            {
                "ts_code": "600000.SH",
                "trade_time": datetime(2026, 9, 1, 9, 31),
                "available_at": datetime(2026, 9, 1, 9, 31),
                "open": 10.0,
                "high": 10.5,
                "low": 9.5,
                "close": 10.1,
                "vol": 1000.0,
                "amount": 10100.0,
            }
        ]
    )

    result = live_compute(
        current,
        historical,
        decision_time=decision,
        input_available_at=decision,
        input_batch_ids=("history-0001", "current-0001"),
        sequence=1,
        config=IntradayFeatureConfig(producer_commit=COMMIT, lookback_sessions=20),
    )

    assert result.envelope.row_count == 1
    # The twenty sessions were actually used: the same-clock median needs history, and with
    # none the engine degrades the field to `missing_same_clock_history` instead.
    status = result.envelope.field_status(
        "hist_same_minute_amount_median",
        candidate_id="600000.SH",
    )
    assert status is not None
    assert status.reason != "missing_same_clock_history"


def test_the_printed_digest_is_the_digest_of_the_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The printed line is what the operator pastes into the inputs document, so it has to
    be the hash of the bytes on disk and not of something the exporter held in memory."""

    _write_minute_database(tmp_path / "rquant_ro.duckdb")
    output = tmp_path / "minute-history.parquet"

    assert (
        exporter.main(
            [
                "--database",
                str(tmp_path / "rquant_ro.duckdb"),
                "--output",
                str(output),
                "--sessions",
                "20",
            ]
        )
        == 0
    )

    printed = {
        line.split(" ", 1)[0]: line.split(" ", 1)[1]
        for line in capsys.readouterr().out.splitlines()
        if " " in line
    }
    assert printed["sha256"] == hashlib.sha256(output.read_bytes()).hexdigest()


def test_a_short_history_is_refused_rather_than_silently_truncated(tmp_path: Path) -> None:
    _write_minute_database(tmp_path / "rquant_ro.duckdb", sessions=12)

    assert (
        exporter.main(
            [
                "--database",
                str(tmp_path / "rquant_ro.duckdb"),
                "--output",
                str(tmp_path / "minute-history.parquet"),
                "--sessions",
                "20",
            ]
        )
        == 2
    )
    assert not (tmp_path / "minute-history.parquet").exists()


def test_the_primary_database_is_refused_without_an_explicit_override(tmp_path: Path) -> None:
    """The 2026-05-20 lock incident, restated as a guard: readers open the replica."""

    _write_minute_database(tmp_path / "rquant.duckdb")

    assert (
        exporter.main(
            [
                "--database",
                str(tmp_path / "rquant.duckdb"),
                "--output",
                str(tmp_path / "minute-history.parquet"),
                "--sessions",
                "20",
            ]
        )
        == 2
    )


def test_mixing_two_minute_sources_is_refused(tmp_path: Path) -> None:
    """Asking for a source that shares minutes with another one must not silently duplicate.

    The guard is inside `_shape_snapshot`, so it fires even if a future caller widens the
    query; here it is provoked directly with a frame holding both sources' rows.
    """

    days = _sessions(1)
    rows = _minute_rows(days, source="tushare") + _minute_rows(days, source="tushare_rt")
    frame = pd.DataFrame(
        [
            {
                "ts_code": row[0],
                "trade_time": row[1],
                "open": row[3],
                "high": row[4],
                "low": row[5],
                "close": row[6],
                "vol": row[7],
                "amount": row[8],
            }
            for row in rows
        ]
    )

    with pytest.raises(exporter.ExportError, match="more than one bar"):
        exporter._shape_snapshot(frame, availability_lag_seconds=0)


def test_the_snapshot_written_is_private_to_its_owner(exported: tuple[Path, str]) -> None:
    output, _digest = exported

    assert output.stat().st_mode & 0o777 == 0o600


def test_a_positive_availability_lag_is_carried_through(tmp_path: Path) -> None:
    _write_minute_database(tmp_path / "rquant_ro.duckdb")
    output = tmp_path / "minute-history.parquet"

    assert (
        exporter.main(
            [
                "--database",
                str(tmp_path / "rquant_ro.duckdb"),
                "--output",
                str(output),
                "--sessions",
                "20",
                "--availability-lag-seconds",
                "2",
            ]
        )
        == 0
    )

    frame = pd.read_parquet(output)
    assert ((frame["available_at"] - frame["trade_time"]) == pd.Timedelta(seconds=2)).all()


def test_rerunning_the_export_reproduces_the_same_digest(tmp_path: Path) -> None:
    _write_minute_database(tmp_path / "rquant_ro.duckdb")
    output = tmp_path / "minute-history.parquet"
    argv = [
        "--database",
        str(tmp_path / "rquant_ro.duckdb"),
        "--output",
        str(output),
        "--sessions",
        "20",
    ]

    assert exporter.main(argv) == 0
    first = hashlib.sha256(output.read_bytes()).hexdigest()
    assert exporter.main(argv) == 0
    second = hashlib.sha256(output.read_bytes()).hexdigest()

    assert first == second


# --------------------------------------------------------------------------- S-2 / S-3


def test_the_freq_filter_keeps_the_five_minute_bars_out(exported: tuple[Path, str]) -> None:
    """`minute_bar` is keyed by `(ts_code, trade_time, freq, source)`, so the same source
    can hold a 5min bar on the same minute. Two bars for one ts_code minute is exactly
    what the feature engine refuses, so the filter has to be real, not incidental."""

    output, _digest = exported

    frame = pd.read_parquet(output)

    assert len(frame) == 2 * 3 * 20
    assert not frame.duplicated(subset=["ts_code", "trade_time"]).any()


def test_a_suspended_session_is_refused_before_the_snapshot_is_written(
    tmp_path: Path,
) -> None:
    """S-2: zero-price bars are what a suspended session looks like in `minute_bar`.

    `feature_live` would hash the parquet at start-up and then refuse the whole batch in
    `_normalize_frame`; by then the profile has already recorded the sha256. Catching it
    here means the export simply did not happen.
    """

    _write_minute_database(tmp_path / "rquant_ro.duckdb", suspended_session=True)
    output = tmp_path / "minute-history.parquet"

    exit_code = exporter.main(
        [
            "--database",
            str(tmp_path / "rquant_ro.duckdb"),
            "--output",
            str(output),
            "--sessions",
            "20",
        ]
    )

    assert exit_code == 2
    assert not output.exists()


def test_a_universe_file_narrows_the_export(tmp_path: Path) -> None:
    """The whole market's twenty sessions is several million rows that `feature_live`
    reads into memory on every start; the subscribed codes are the only other lever."""

    _write_minute_database(tmp_path / "rquant_ro.duckdb")
    universe = tmp_path / "universe.txt"
    universe.write_text("600001.SH\n\n600001.SH\n", encoding="utf-8")
    output = tmp_path / "minute-history.parquet"

    assert (
        exporter.main(
            [
                "--database",
                str(tmp_path / "rquant_ro.duckdb"),
                "--output",
                str(output),
                "--sessions",
                "20",
                "--ts-code-file",
                str(universe),
            ]
        )
        == 0
    )

    frame = pd.read_parquet(output)
    assert set(frame["ts_code"]) == {"600001.SH"}
    assert len(frame) == 3 * 20


def test_an_empty_universe_file_is_refused(tmp_path: Path) -> None:
    _write_minute_database(tmp_path / "rquant_ro.duckdb")
    universe = tmp_path / "universe.txt"
    universe.write_text("\n  \n", encoding="utf-8")

    assert (
        exporter.main(
            [
                "--database",
                str(tmp_path / "rquant_ro.duckdb"),
                "--output",
                str(tmp_path / "minute-history.parquet"),
                "--sessions",
                "20",
                "--ts-code-file",
                str(universe),
            ]
        )
        == 2
    )
