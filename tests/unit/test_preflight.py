"""preflight must remain safe while the intraday monitor owns the primary DB."""

from __future__ import annotations

import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from rquant.data_contracts import CONTRACTS_BY_ID, EXCHANGE_TIMEZONE
from rquant.preflight import (
    check_data_freshness,
    detail_duckdb_lock,
    smoke_screen,
)
from rquant.storage import duckdb as duckdb_module


class _Cursor:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self._row = row

    def fetchone(self) -> tuple[Any, ...]:
        return self._row


class _Connection:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self._row = row

    def execute(self, *_: object, **__: object) -> _Cursor:
        return _Cursor(self._row)


class _ReadonlyStore:
    def __init__(self, row: tuple[Any, ...]) -> None:
        self._conn = _Connection(row)
        self.entered = False
        self.closed = False

    def __enter__(self) -> _ReadonlyStore:
        self.entered = True
        return self

    def __exit__(self, *_: object) -> None:
        self.closed = True

    def latest_trading_day(self, anchor: date, *, exchange: str = "SSE") -> date:
        del exchange
        return anchor

    def previous_trading_day(self, anchor: date, *, exchange: str = "SSE") -> date:
        del anchor, exchange
        return date(2026, 7, 10)

    def is_trading_day(self, exchange: str, cal_date: date) -> bool:
        del exchange, cal_date
        return True

    def list_trade_calendar(
        self, exchange: str, start: date, end: date
    ) -> list[object]:
        del exchange, start, end
        return []


def _forbid_primary_store(*_: object, **__: object) -> None:
    raise AssertionError("preflight must not construct DuckDBStore directly")


def test_data_freshness_uses_readonly_store_helper(monkeypatch: Any) -> None:
    store = _ReadonlyStore((date(2026, 7, 10), 123))
    seen_required_tables: list[tuple[str, ...]] = []

    def open_readonly_store(
        *, required_tables: tuple[str, ...] | list[str] | None = None
    ) -> _ReadonlyStore:
        seen_required_tables.append(tuple(required_tables or ()))
        return store

    monkeypatch.setattr(duckdb_module, "open_readonly_store", open_readonly_store)
    monkeypatch.setattr(duckdb_module, "DuckDBStore", _forbid_primary_store)

    result = check_data_freshness(
        (CONTRACTS_BY_ID["daily_bar"],),
        as_of=datetime.combine(
            date(2026, 7, 13),
            datetime.min.time(),
            tzinfo=EXCHANGE_TIMEZONE,
        ),
        replica_path=None,
    )

    assert result.status == "ok"
    assert seen_required_tables == [()]
    assert store.entered is True
    assert store.closed is True


def test_smoke_screen_uses_readonly_store_helper(monkeypatch: Any) -> None:
    store = _ReadonlyStore((date(2026, 7, 10),))
    seen_required_tables: list[tuple[str, ...]] = []

    def open_readonly_store(
        *, required_tables: tuple[str, ...] | list[str] | None = None
    ) -> _ReadonlyStore:
        seen_required_tables.append(tuple(required_tables or ()))
        return store

    def fake_screen(*_: object, **kwargs: object) -> pd.DataFrame:
        assert kwargs["store"] is store
        return pd.DataFrame()

    monkeypatch.setattr(duckdb_module, "open_readonly_store", open_readonly_store)
    monkeypatch.setattr(duckdb_module, "DuckDBStore", _forbid_primary_store)
    monkeypatch.setattr("rquant.screen.core.screen", fake_screen)

    result = smoke_screen()

    assert result.status == "ok"
    assert seen_required_tables == [
        (
            "screen_result",
            "daily_bar",
            "daily_indicator",
            "daily_state",
            "daily_basic",
            "stock_basic",
        )
    ]
    assert store.entered is True
    assert store.closed is True


def test_lock_detail_does_not_treat_unclassified_fd_as_no_writer(
    monkeypatch: Any,
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "rquant.duckdb"
    db_path.touch()
    output = "\n".join(
        [
            "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME",
            f"python3.14 3847085 lighthouse mem REG 1,2 0 42 {db_path}",
        ]
    )
    completed = subprocess.CompletedProcess(
        args=["lsof", str(db_path)], returncode=0, stdout=output, stderr=""
    )

    monkeypatch.setattr("rquant.preflight.shutil.which", lambda _: "/usr/bin/lsof")
    monkeypatch.setattr("rquant.preflight.subprocess.run", lambda *_args, **_kwargs: completed)

    result = detail_duckdb_lock(db_path)

    assert result.status == "warn"
    assert "不能判断 monitor 未运行" in result.summary
    assert any("python3.14 pid=3847085 FD=mem" in line for line in result.details)
