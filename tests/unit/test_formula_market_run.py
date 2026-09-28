"""Offline formula runs bind one captured market list to one history generation."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import rquant.screen.formula_market_run as runner
from rquant.screen.formula_history_projection import (
    FormulaProjectionChangedError,
    FormulaProjectionDateError,
    VerifiedFormulaHistoryProjection,
)
from rquant.screen.formula_market_universe import (
    FormulaMarketEntry,
    FormulaMarketPartition,
    FormulaMarketUniverseError,
    FormulaMarketUniverseSnapshot,
    publish_formula_market_universe,
)
from rquant.screen.tdx.evaluate import EvaluationRejectedError

DAY = date(2026, 4, 15)
OPEN_DAYS = (date(2026, 4, 13), date(2026, 4, 14), DAY)
SHANGHAI = ZoneInfo("Asia/Shanghai")
DECISION_AT = datetime(2026, 4, 15, 17, 15, tzinfo=SHANGHAI)
ENTRIES = (
    FormulaMarketEntry(
        ts_code="000001.SZ", exchange="SZSE", list_status="L", list_date=OPEN_DAYS[0]
    ),
    FormulaMarketEntry(
        ts_code="300001.SZ", exchange="SZSE", list_status="P", list_date=OPEN_DAYS[0]
    ),
    FormulaMarketEntry(
        ts_code="600001.SH", exchange="SSE", list_status="L", list_date=OPEN_DAYS[0]
    ),
    FormulaMarketEntry(
        ts_code="830001.BJ", exchange="BSE", list_status="L", list_date=OPEN_DAYS[0]
    ),
)
PARTITION_KEYS = (
    ("SSE", "L"),
    ("SSE", "P"),
    ("SZSE", "L"),
    ("SZSE", "P"),
    ("BSE", "L"),
    ("BSE", "P"),
)


def _market(
    tmp_path: Path,
    *,
    entries: tuple[FormulaMarketEntry, ...] = ENTRIES,
    completed_at: datetime | None = None,
) -> tuple[Path, str]:
    root = tmp_path / "market"
    partitions = tuple(
        FormulaMarketPartition(
            exchange=exchange,
            list_status=status,
            raw_rows=sum(
                item.exchange == exchange and item.list_status == status for item in entries
            ),
            included_rows=sum(
                item.exchange == exchange and item.list_status == status for item in entries
            ),
            excluded_b_shares=0,
            excluded_other=0,
        )
        for exchange, status in PARTITION_KEYS
    )
    snapshot = FormulaMarketUniverseSnapshot.create(
        trade_date=DAY,
        started_at=datetime(2026, 4, 15, 17, 1, tzinfo=SHANGHAI),
        completed_at=completed_at or datetime(2026, 4, 15, 17, 5, tzinfo=SHANGHAI),
        calendar_sha256="a" * 64,
        calendar_generated_at=datetime(2026, 4, 15, 8, tzinfo=UTC),
        partitions=partitions,
        entries=entries,
    )
    publish_formula_market_universe(root, snapshot)
    return root, snapshot.content_sha256


def _history(
    tmp_path: Path,
    *,
    listings: list[tuple[str, str | None]] | None = None,
    bars: dict[str, list[tuple[date, float]]] | None = None,
    name: str = "a" * 32,
    updated_at: datetime | None = None,
    open_day: bool = True,
) -> tuple[Path, str]:
    listings = (
        listings
        if listings is not None
        else [(item.ts_code, item.list_date.isoformat()) for item in ENTRIES]
    )
    bars = (
        bars
        if bars is not None
        else {
            "000001.SZ": list(zip(OPEN_DAYS, (1.0, 2.0, 3.0), strict=True)),
            "600001.SH": list(zip(OPEN_DAYS, (2.0, 2.0, 1.0), strict=True)),
            "830001.BJ": list(zip(OPEN_DAYS, (3.0, 4.0, 5.0), strict=True)),
        }
    )
    root = tmp_path / "history"
    root.mkdir(exist_ok=True)
    path = root / f"{name}.sqlite"
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "CREATE TABLE bars (ts_code TEXT NOT NULL, trade_date TEXT NOT NULL, "
            "open REAL, high REAL, low REAL, close REAL, vol REAL, amount REAL, "
            "PRIMARY KEY(ts_code,trade_date)) WITHOUT ROWID"
        )
        connection.execute(
            "CREATE TABLE calendar (exchange TEXT NOT NULL, cal_date TEXT NOT NULL, "
            "is_open INTEGER NOT NULL, PRIMARY KEY(exchange,cal_date)) WITHOUT ROWID"
        )
        connection.execute(
            "CREATE TABLE listing (ts_code TEXT PRIMARY KEY, list_date TEXT) WITHOUT ROWID"
        )
        connection.executemany(
            "INSERT INTO calendar VALUES ('SSE', ?, ?)",
            [(day.isoformat(), int(day != DAY or open_day)) for day in OPEN_DAYS],
        )
        connection.executemany("INSERT INTO listing VALUES (?, ?)", listings)
        connection.executemany(
            "INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, 100, 1000)",
            [
                (code, day.isoformat(), close, close + 1, close - 1, close)
                for code, history in bars.items()
                for day, close in history
            ],
        )
        connection.commit()
    finally:
        connection.close()
    os.chmod(path, 0o444)
    observed = path.stat()
    manifest = {
        "schema_version": 1,
        "file_name": path.name,
        "file_device": observed.st_dev,
        "file_inode": observed.st_ino,
        "file_size": observed.st_size,
        "file_mtime_ns": observed.st_mtime_ns,
        "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "source_identity": "f" * 64,
        "source_updated_at": (updated_at or datetime(2026, 4, 15, 9, 10, tzinfo=UTC)).isoformat(),
        "dates": [DAY.isoformat()],
        "bar_count": sum(len(history) for history in bars.values()),
    }
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    (root / "current.json").write_bytes(payload)
    return root, hashlib.sha256(payload).hexdigest()


def _run(
    market: tuple[Path, str],
    history: tuple[Path, str],
    *,
    formula: str = "CLOSE>2",
    trade_date: date = DAY,
    decision_at: datetime = DECISION_AT,
):
    return runner.run_formula_market(
        market[0],
        history[0],
        formula,
        trade_date,
        decision_at,
        expected_universe_sha256=market[1],
        expected_projection_identity=history[1],
    )


def test_run_uses_captured_market_as_denominator_and_conserves_counts(tmp_path: Path) -> None:
    market = _market(tmp_path)
    history = _history(
        tmp_path,
        listings=[(item.ts_code, item.list_date.isoformat()) for item in ENTRIES]
        + [("600002.SH", "2020-01-01")],
        bars={
            "000001.SZ": list(zip(OPEN_DAYS, (1.0, 2.0, 3.0), strict=True)),
            "600001.SH": list(zip(OPEN_DAYS, (2.0, 2.0, 1.0), strict=True)),
            "830001.BJ": list(zip(OPEN_DAYS, (3.0, 4.0, 5.0), strict=True)),
            "600002.SH": [(DAY, 100.0)],
        },
    )

    result = _run(market, history)

    assert result.scope == "captured_a_share_market"
    assert result.trade_date == DAY
    assert result.decision_at == DECISION_AT
    assert (result.universe_identity, result.projection_identity) == (market[1], history[1])
    assert result.universe_completed_at == datetime(2026, 4, 15, 9, 5, tzinfo=UTC)
    assert result.projection_updated_at == datetime(2026, 4, 15, 9, 10, tzinfo=UTC)
    assert (result.market_total, result.listed_count, result.paused_count) == (4, 3, 1)
    assert (result.match_count, result.no_match_count, result.unknown_count) == (2, 1, 1)
    assert result.unknown_reasons == {"missing_date": 1}
    assert result.match_codes == ("000001.SZ", "830001.BJ")


def test_missing_projection_code_and_listing_conflict_are_unknown(tmp_path: Path) -> None:
    market = _market(tmp_path)
    history = _history(
        tmp_path,
        listings=[
            ("000001.SZ", "2026-04-14"),
            ("300001.SZ", "2026-04-13"),
            ("600001.SH", "2026-04-13"),
        ],
    )

    result = _run(market, history)

    assert (result.match_count, result.no_match_count, result.unknown_count) == (0, 1, 3)
    assert result.unknown_reasons == {
        "listing_conflict": 1,
        "missing_projection_code": 1,
        "missing_date": 1,
    }
    assert result.market_total == sum(
        (result.match_count, result.no_match_count, result.unknown_count)
    )


@pytest.mark.parametrize("fault", ["market_date", "closed_projection_day"])
def test_date_mismatch_or_closed_projection_day_rejects_whole_run(
    tmp_path: Path,
    fault: str,
) -> None:
    market = _market(tmp_path)
    history = _history(tmp_path, open_day=fault != "closed_projection_day")
    with pytest.raises((FormulaMarketUniverseError, FormulaProjectionDateError)):
        _run(market, history, trade_date=DAY.replace(day=14) if fault == "market_date" else DAY)


@pytest.mark.parametrize(
    "decision_at",
    [
        datetime(2026, 4, 15, 16, 59, tzinfo=SHANGHAI),
        datetime(2026, 4, 15, 17, 4, tzinfo=SHANGHAI),
        datetime(2026, 4, 15, 17, 9, tzinfo=SHANGHAI),
    ],
)
def test_run_rejects_before_daily_market_or_projection_visibility(
    tmp_path: Path,
    decision_at: datetime,
) -> None:
    with pytest.raises(EvaluationRejectedError) as error:
        _run(_market(tmp_path), _history(tmp_path), decision_at=decision_at)
    assert error.value.code == "time"


def test_projection_updated_after_decision_rejects_whole_run(tmp_path: Path) -> None:
    market = _market(tmp_path)
    history = _history(tmp_path, updated_at=datetime(2026, 4, 15, 9, 20, tzinfo=UTC))
    with pytest.raises(EvaluationRejectedError) as error:
        _run(market, history)
    assert error.value.code == "time"


@pytest.mark.parametrize("source", ["market", "projection"])
def test_switching_either_source_discards_entire_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    source: str,
) -> None:
    market = _market(tmp_path)
    history = _history(tmp_path)
    original = VerifiedFormulaHistoryProjection.formula_history
    switched = False

    def switch_after_first(self, *args, **kwargs):
        nonlocal switched
        answer = original(self, *args, **kwargs)
        if not switched:
            switched = True
            if source == "market":
                _market(
                    tmp_path,
                    completed_at=datetime(2026, 4, 15, 17, 6, tzinfo=SHANGHAI),
                )
            else:
                _history(tmp_path, name="b" * 32)
        return answer

    monkeypatch.setattr(VerifiedFormulaHistoryProjection, "formula_history", switch_after_first)
    with pytest.raises((FormulaMarketUniverseError, FormulaProjectionChangedError)):
        _run(market, history)


def test_market_over_capacity_rejects_instead_of_truncating(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(runner, "MAX_MARKET_RUN_STOCKS", 3)
    with pytest.raises(runner.FormulaMarketRunBudgetError):
        _run(_market(tmp_path), _history(tmp_path))


def test_timeout_discards_partial_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    market = _market(tmp_path)
    history = _history(tmp_path)
    calls = 0

    def elapsed() -> float:
        nonlocal calls
        calls += 1
        return 0.0 if calls < 5 else runner.MAX_MARKET_RUN_SECONDS + 1.0

    monkeypatch.setattr(runner.time, "monotonic", elapsed)
    with pytest.raises(runner.FormulaMarketRunTimeoutError):
        _run(market, history)


def test_invalid_formula_rejects_whole_run(tmp_path: Path) -> None:
    with pytest.raises(EvaluationRejectedError) as error:
        _run(_market(tmp_path), _history(tmp_path), formula="BAD(CLOSE)>0")
    assert error.value.code == "formula"
