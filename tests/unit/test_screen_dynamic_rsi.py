"""Full-history RSI values sealed for bounded replica screening."""

from __future__ import annotations

import json
import shutil
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from ta.momentum import RSIIndicator

from rquant.replica_generation import (
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.screen.dynamic_rsi import (
    DynamicRsiProjectionBudgetError,
    DynamicRsiProjectionUnavailableError,
    VerifiedDynamicRsiProjection,
    publish_dynamic_rsi_projection,
    requested_dynamic_rsi,
)
from rquant.screen.replica_source import VerifiedReplicaScreenSource
from rquant.storage.duckdb import DuckDBStore


def _publish(primary: Path, replica: Path) -> None:
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )


def _replica_world(tmp_path: Path) -> tuple[Path, Path, date]:
    primary = tmp_path / "rquant.duckdb"
    replica = tmp_path / "rquant_ro.duckdb"
    latest = date(2026, 4, 15)
    with DuckDBStore(primary) as store:
        for offset in range(100):
            day = latest - timedelta(days=offset)
            store._conn.execute(
                "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
                "VALUES ('SSE', ?, TRUE, 'fixture', ?)",
                [day, datetime.now(UTC)],
            )
            for code in ("600001.SH", "600002.SH", "600003.SH"):
                store._conn.execute(
                    "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES (?, ?, 10)",
                    [code, day],
                )
    _publish(primary, replica)
    return primary, replica, latest


def _built(tmp_path: Path) -> tuple[VerifiedDynamicRsiProjection, str, Path, Path]:
    primary, replica, latest = _replica_world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO stock_basic (ts_code, list_date) VALUES "
            "('600001.SH', ?), ('600002.SH', ?), ('600003.SH', ?)",
            [latest - timedelta(days=99), latest - timedelta(days=69), latest - timedelta(days=99)],
        )
        store._conn.execute(
            "UPDATE daily_bar SET close = 10 + 2 * sin(date_diff('day', trade_date, ?)) "
            "WHERE ts_code = '600001.SH'",
            [latest],
        )
        store._conn.execute(
            "UPDATE daily_bar SET close = 15 + cos(date_diff('day', trade_date, ?)) "
            "WHERE ts_code = '600002.SH'",
            [latest],
        )
        store._conn.execute(
            "DELETE FROM daily_bar WHERE ts_code = '600002.SH' AND trade_date < ?",
            [latest - timedelta(days=69)],
        )
        store._conn.execute(
            "DELETE FROM daily_bar WHERE ts_code = '600003.SH' AND trade_date = ?",
            [latest - timedelta(days=10)],
        )
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, CASE WHEN trade_date < ? THEN 2 ELSE 1 END "
            "FROM daily_bar",
            [latest - timedelta(days=40)],
        )
    _publish(primary, replica)
    source = VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica)
    root = tmp_path / "rsi"
    publish_dynamic_rsi_projection(source, root)
    return VerifiedDynamicRsiProjection(root), source.available_dates().identity, primary, replica


def test_projection_matches_full_history_ta_per_stock_with_adjustment_and_pause(
    tmp_path: Path,
) -> None:
    projection, identity, primary, _ = _built(tmp_path)
    latest = date(2026, 4, 15)
    dates = [latest - timedelta(days=offset) for offset in (0, 30)]
    columns = {
        f"RSI{period}[{offset}]": (period, offset)
        for period in (2, 6, 14, 60)
        for offset in (0, 30)
    }
    result = projection.values(identity, latest, ["600001.SH", "600002.SH", "600003.SH"], columns)
    with DuckDBStore(primary, read_only=True) as store:
        bars = store._conn.execute(
            "SELECT ts_code, trade_date, daily_bar.close * adj_factor.adj_factor "
            "FROM daily_bar JOIN adj_factor USING (ts_code, trade_date) "
            "ORDER BY ts_code, trade_date"
        ).fetchdf()
    for code in ("600001.SH", "600002.SH", "600003.SH"):
        one = bars[bars.ts_code == code].reset_index(drop=True)
        for period in (2, 6, 14, 60):
            expected = RSIIndicator(one.iloc[:, 2].astype(float), window=period).rsi()
            for offset, day in ((0, dates[0]), (30, dates[1])):
                match = one.index[one.trade_date == pd.Timestamp(day)]
                actual = result.loc[result.ts_code == code, f"RSI{period}[{offset}]"].iloc[0]
                if len(match) == 0 or pd.isna(expected.iloc[match[0]]):
                    assert pd.isna(actual)
                else:
                    assert actual == pytest.approx(expected.iloc[match[0]], abs=1e-9)


def test_projection_rejects_changed_source_and_tampered_or_missing_generation(
    tmp_path: Path,
) -> None:
    projection, identity, primary, replica = _built(tmp_path)
    latest = date(2026, 4, 15)
    columns = {"RSI7[0]": (7, 0)}
    with pytest.raises(DynamicRsiProjectionUnavailableError):
        projection.values("0" * 64, latest, ["600001.SH"], columns)
    manifest = json.loads((tmp_path / "rsi" / "current.json").read_text())
    artifact = tmp_path / "rsi" / manifest["file_name"]
    artifact.chmod(0o644)
    artifact.write_bytes(artifact.read_bytes() + b"changed")
    with pytest.raises(DynamicRsiProjectionUnavailableError):
        projection.values(identity, latest, ["600001.SH"], columns)
    _publish(primary, replica)


def test_failed_build_keeps_previous_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    projection, identity, primary, replica = _built(tmp_path)
    before = (tmp_path / "rsi" / "current.json").read_bytes()
    source = VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica)
    original = source._finish

    def fail(*args: object) -> None:
        raise RuntimeError("source changed")

    monkeypatch.setattr(source, "_finish", fail)
    with pytest.raises(RuntimeError, match="source changed"):
        publish_dynamic_rsi_projection(source, tmp_path / "rsi")
    assert (tmp_path / "rsi" / "current.json").read_bytes() == before
    assert projection.catalog(identity).dates
    monkeypatch.setattr(source, "_finish", original)


def test_missing_row_and_request_budget_fail_closed(tmp_path: Path) -> None:
    projection, identity, _, _ = _built(tmp_path)
    latest = date(2026, 4, 15)
    assert projection.values(identity, latest, [], {"RSI7[0]": (7, 0)}).columns.tolist() == [
        "ts_code",
        "RSI7[0]",
    ]
    with pytest.raises(DynamicRsiProjectionBudgetError):
        projection.values(
            identity,
            latest,
            [f"{code:06d}.SH" for code in range(8_001)],
            {
                "RSI7[0]": (7, 0),
            },
        )
    manifest = json.loads((tmp_path / "rsi" / "current.json").read_text())
    artifact = tmp_path / "rsi" / manifest["file_name"]
    artifact.chmod(0o644)
    with sqlite3.connect(artifact) as connection:
        connection.execute(
            "DELETE FROM rsi WHERE ts_code='600001.SH' AND trade_date=?",
            (latest.isoformat(),),
        )
    with pytest.raises(DynamicRsiProjectionUnavailableError):
        projection.values(identity, latest, ["600001.SH"], {"RSI7[0]": (7, 0)})


def test_legacy_rsi_offsets_keep_the_persisted_path() -> None:
    assert requested_dynamic_rsi(frozenset({"RSI6[31]", "RSI14[90]"})) == {}
    with pytest.raises(ValueError, match="dynamic RSI"):
        requested_dynamic_rsi(frozenset({"RSI7[31]"}))


def test_projection_excludes_future_and_not_yet_closed_days(tmp_path: Path) -> None:
    primary, replica, today = _replica_world(tmp_path)
    tomorrow = today + timedelta(days=1)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO trade_calendar (exchange, cal_date, is_open, source, updated_at) "
            "VALUES ('SSE', ?, TRUE, 'fixture', ?)",
            [tomorrow, datetime.now(UTC)],
        )
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES "
            "('600001.SH', ?, 11), ('600002.SH', ?, 12), ('600003.SH', ?, 13)",
            [tomorrow] * 3,
        )
        store._conn.execute(
            "INSERT INTO adj_factor (ts_code, trade_date, adj_factor) "
            "SELECT ts_code, trade_date, 1 FROM daily_bar"
        )
    _publish(primary, replica)
    source = VerifiedReplicaScreenSource(primary_path=primary, replica_path=replica)
    root = tmp_path / "rsi"

    before_close = publish_dynamic_rsi_projection(
        source, root, as_of=datetime(2026, 4, 15, 8, 59, tzinfo=UTC)
    )
    assert before_close.dates[0] == today - timedelta(days=1)
    assert today not in before_close.dates
    assert tomorrow not in before_close.dates

    after_close = publish_dynamic_rsi_projection(
        source, root, as_of=datetime(2026, 4, 15, 9, 0, tzinfo=UTC)
    )
    assert after_close.dates[0] == today
    assert tomorrow not in after_close.dates
