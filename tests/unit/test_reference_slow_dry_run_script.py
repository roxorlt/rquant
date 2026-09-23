"""`scripts/reference_slow_dry_run.py`: the whole capture path, and not one write (#293)."""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from datetime import date
from pathlib import Path
from types import ModuleType

import duckdb
import pandas as pd
import pytest

from tests.unit.test_reference_slow_source import (
    PRIOR_DATE,
    TARGET_DATE,
    _calendar,
    _database,
    _production_shaped_pro,
    _tushare_adapter,
)

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "reference_slow_dry_run.py"


def _load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("reference_slow_dry_run", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    #: the script's dataclasses resolve their annotations through `sys.modules`
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _tree(root: Path) -> dict[str, tuple[int, int]]:
    return {
        str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _trade_cal_adapter(calls: list[dict[str, str]]):  # type: ignore[no-untyped-def]
    adapter = _tushare_adapter(_production_shaped_pro(calls))
    adapter.trade_cal = lambda start, end: [  # type: ignore[method-assign]
        item for item in _calendar().open_dates if start <= item <= end
    ]
    return adapter


def test_the_dry_run_validates_the_whole_path_and_writes_nothing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "data").mkdir()
    database = _database(tmp_path / "data")
    tempdir = tmp_path / "tmp"
    tempdir.mkdir()
    #: a private copy of the database, had the read taken one, would land inside the tree
    monkeypatch.setattr(tempfile, "tempdir", str(tempdir))
    before = _tree(tmp_path)
    lines: list[str] = []
    calls: list[dict[str, str]] = []

    status = _load_script().run_dry_run(
        database=database,
        trade_date=TARGET_DATE,
        adapter=_trade_cal_adapter(calls),
        out=lines.append,
    )

    assert status == 0, "\n".join(lines)
    assert lines[-1].startswith("DRY RUN OK: target_trade_date=2026-07-31")
    assert f"prior={PRIOR_DATE.isoformat()}" in "\n".join(lines)
    assert any("rows=2" in line and "daily_bar JOIN adj_factor" in line for line in lines)
    assert any(line.strip().startswith("stock_basic(D)") and "rows=2" in line for line in lines)
    assert "  D: facts=1 skipped_invalid_codes=1 (T600018.SH)" in lines
    assert "  L: facts=2 skipped_invalid_codes=0" in lines
    assert any("envelope quality_status=published row_count=2" in line for line in lines)
    assert any("serving dataset_id=reference_slow_authority" in line for line in lines)
    assert [call["list_status"] for call in calls] == ["L", "D", "P"]
    #: no spool, no quota ledger, no registry, no authority, no private copy of the database
    assert _tree(tmp_path) == before


def test_the_dry_run_names_the_step_that_refused(tmp_path: Path) -> None:
    database = _database(tmp_path)
    calls: list[dict[str, str]] = []
    adapter = _trade_cal_adapter(calls)
    adapter._pro.stock_basic = lambda **_kwargs: pd.DataFrame(  # type: ignore[attr-defined]
        [{"ts_code": "600000.SH", "name": "普通样本", "list_date": "19991110", "market": "主板"}]
    )
    lines: list[str] = []

    status = _load_script().run_dry_run(
        database=database, trade_date=TARGET_DATE, adapter=adapter, out=lines.append
    )

    assert status == 1
    assert lines[-1] == (
        "DRY RUN FAILED at step 'source capture': ReferenceSlowSourceError: "
        "stock_basic source is missing columns: delist_date"
    )


def test_the_dry_run_refuses_a_session_the_calendar_does_not_open(tmp_path: Path) -> None:
    lines: list[str] = []

    status = _load_script().run_dry_run(
        database=_database(tmp_path),
        trade_date=date(2026, 8, 1),
        adapter=_trade_cal_adapter([]),
        calendar=_calendar(),
        out=lines.append,
    )

    assert status == 1
    assert lines[-1].startswith("DRY RUN FAILED at step 'calendar'")


def test_the_dry_run_opens_the_replica_read_only(tmp_path: Path) -> None:
    database = _database(tmp_path)
    holder = duckdb.connect(str(database), read_only=True)
    try:
        #: a second read-only open next to a read-only holder works; a writer would not
        assert _load_script()._prior_universe_count(database, PRIOR_DATE) == 2
    finally:
        holder.close()
    assert not Path(f"{database}.wal").exists()
    assert oct(os.stat(database).st_mode & 0o777) == "0o600"
