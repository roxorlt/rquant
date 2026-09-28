"""Bounded formula runs over one synthetic immutable history generation."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import rquant.screen.formula_catalog_run as runner
import rquant.screen.formula_history_projection as projection_module
import rquant.screen.tdx.evaluate as evaluate_module
from rquant.screen.formula_history_projection import (
    FormulaProjectionChangedError,
    FormulaProjectionUnavailableError,
    VerifiedFormulaHistoryProjection,
)
from rquant.screen.tdx.evaluate import EvaluationRejectedError

DAY = date(2026, 4, 15)
OPEN_DAYS = (date(2026, 4, 13), date(2026, 4, 14), DAY)
AFTER_CLOSE = datetime(2026, 4, 15, 17, tzinfo=ZoneInfo("Asia/Shanghai"))


def _generation(
    tmp_path: Path,
    listings: list[tuple[str, str | None]],
    bars: dict[str, list[tuple[date, float]]],
    *,
    name: str = "a" * 32,
    listing_primary_key: bool = True,
) -> tuple[Path, str]:
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
            "CREATE TABLE listing (ts_code TEXT "
            + ("PRIMARY KEY, " if listing_primary_key else "NOT NULL, ")
            + "list_date TEXT)"
            + (" WITHOUT ROWID" if listing_primary_key else "")
        )
        connection.executemany(
            "INSERT INTO calendar VALUES ('SSE', ?, 1)",
            [(day.isoformat(),) for day in OPEN_DAYS],
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
        "source_updated_at": datetime(2026, 4, 15, 9, tzinfo=UTC).isoformat(),
        "dates": [DAY.isoformat()],
        "bar_count": sum(map(len, bars.values())),
    }
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    (root / "current.json").write_bytes(payload)
    return root, hashlib.sha256(payload).hexdigest()


def _run(root: Path, identity: str, formula: str = "CLOSE>MA(CLOSE,3)"):
    return runner.run_formula_catalog(
        root, formula, DAY, AFTER_CLOSE, expected_identity=identity,
    )


def test_catalog_run_keeps_missing_facts_unknown_and_conserves_counts(tmp_path: Path) -> None:
    listings = [
        ("600001.SH", "2026-04-13"),
        ("600002.SH", "2026-04-13"),
        ("600003.SH", "2026-04-13"),
        ("600004.SH", None),
        ("600005.SH", "2026-04-16"),
        ("600006.SH", "2026-04-13"),
    ]
    bars = {
        "600001.SH": list(zip(OPEN_DAYS, (1.0, 2.0, 3.0), strict=True)),
        "600002.SH": list(zip(OPEN_DAYS, (5.0, 5.0, 1.0), strict=True)),
        "600003.SH": list(zip(OPEN_DAYS[:2], (1.0, 2.0), strict=True)),
        "600004.SH": list(zip(OPEN_DAYS, (1.0, 2.0, 3.0), strict=True)),
        "600005.SH": [(DAY, 9.0)],
        "600006.SH": [(OPEN_DAYS[0], 1.0), (DAY, 3.0)],
    }
    root, identity = _generation(tmp_path, listings, bars)

    result = _run(root, identity)

    assert result.scope == "historical_projection_catalog"
    assert result.identity == identity
    assert result.trade_date == DAY
    assert (
        result.catalog_total, result.candidate_total, result.future_listing_excluded,
    ) == (6, 5, 1)
    assert (result.match_count, result.no_match_count, result.unknown_count) == (1, 1, 3)
    assert result.match_codes == ("600001.SH",)
    assert result.unknown_reasons == {
        "missing_date": 1,
        "missing_listing": 1,
        "missing_history": 1,
    }


def test_recursive_formula_uses_full_listing_history_and_parses_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    codes = ("600001.SH", "600002.SH")
    root, identity = _generation(
        tmp_path,
        [(code, "2026-04-13") for code in codes],
        {
            codes[0]: list(zip(OPEN_DAYS, (100.0, 1.0, 1.0), strict=True)),
            codes[1]: list(zip(OPEN_DAYS, (1.0, 1.0, 1.0), strict=True)),
        },
    )
    calls = 0
    original = evaluate_module.parse_formula

    def count_parse(source: str):
        nonlocal calls
        calls += 1
        return original(source)

    monkeypatch.setattr(evaluate_module, "parse_formula", count_parse)

    result = _run(root, identity, "EMA(CLOSE,2)>5")

    assert result.match_codes == ("600001.SH",)
    assert (result.match_count, result.no_match_count, result.unknown_count) == (1, 1, 0)
    assert calls == 1


@pytest.mark.parametrize("bad_code,bad_date", [
    ("BAD", "2026-04-13"),
    ("600001.SH", "2026-13-40"),
])
def test_catalog_rejects_bad_listing_record(
    tmp_path: Path, bad_code: str, bad_date: str,
) -> None:
    root, identity = _generation(tmp_path, [(bad_code, bad_date)], {})

    with pytest.raises(FormulaProjectionUnavailableError):
        VerifiedFormulaHistoryProjection(root).catalog_snapshot(
            DAY, expected_identity=identity,
        )


def test_catalog_rejects_duplicate_code(tmp_path: Path) -> None:
    root, identity = _generation(
        tmp_path,
        [("600001.SH", "2026-04-13"), ("600001.SH", "2026-04-13")],
        {}, listing_primary_key=False,
    )

    with pytest.raises(FormulaProjectionUnavailableError):
        VerifiedFormulaHistoryProjection(root).catalog_snapshot(
            DAY, expected_identity=identity,
        )


def test_catalog_rejects_over_limit_instead_of_truncating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(projection_module, "MAX_CATALOG_STOCKS", 2)
    root, identity = _generation(
        tmp_path,
        [(f"60000{index}.SH", "2026-04-13") for index in range(3)],
        {},
    )

    with pytest.raises(projection_module.FormulaProjectionBudgetError):
        VerifiedFormulaHistoryProjection(root).catalog_snapshot(
            DAY, expected_identity=identity,
        )


def test_single_stock_history_budget_becomes_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, identity = _generation(
        tmp_path,
        [("600001.SH", "2026-04-13")],
        {"600001.SH": list(zip(OPEN_DAYS, (1.0, 2.0, 3.0), strict=True))},
    )
    monkeypatch.setattr(projection_module, "MAX_BARS_PER_STOCK", 2)

    result = _run(root, identity, "EMA(CLOSE,2)>2")

    assert (result.match_count, result.no_match_count, result.unknown_count) == (0, 0, 1)
    assert result.unknown_reasons == {"history_budget": 1}


def test_generation_switch_discards_entire_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    listings = [("600001.SH", "2026-04-13"), ("600002.SH", "2026-04-13")]
    bars = {code: list(zip(OPEN_DAYS, (1.0, 2.0, 3.0), strict=True)) for code, _ in listings}
    root, identity = _generation(tmp_path, listings, bars)
    original = VerifiedFormulaHistoryProjection.formula_history
    switched = False

    def switch_after_read(self, *args, **kwargs):
        nonlocal switched
        result = original(self, *args, **kwargs)
        if not switched:
            switched = True
            _generation(tmp_path, listings, bars, name="b" * 32)
        return result

    monkeypatch.setattr(VerifiedFormulaHistoryProjection, "formula_history", switch_after_read)

    with pytest.raises(FormulaProjectionChangedError):
        _run(root, identity)


def test_run_timeout_discards_partial_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    listings = [("600001.SH", "2026-04-13"), ("600002.SH", "2026-04-13")]
    bars = {code: list(zip(OPEN_DAYS, (1.0, 2.0, 3.0), strict=True)) for code, _ in listings}
    root, identity = _generation(tmp_path, listings, bars)
    ticks = iter((0.0, 0.0, 0.0, runner.MAX_CATALOG_RUN_SECONDS + 1.0))
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(ticks))
    original = VerifiedFormulaHistoryProjection.formula_history
    seen: list[str] = []

    def record_read(self, trade_date, stock_code, **kwargs):
        seen.append(stock_code)
        return original(self, trade_date, stock_code, **kwargs)

    monkeypatch.setattr(VerifiedFormulaHistoryProjection, "formula_history", record_read)

    with pytest.raises(runner.FormulaCatalogRunTimeoutError):
        _run(root, identity)
    assert seen == ["600001.SH"]


def test_run_checks_timeout_after_final_generation_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, identity = _generation(tmp_path, [], {})
    ticks = iter((0.0, 0.0, 0.0, runner.MAX_CATALOG_RUN_SECONDS + 1.0))
    monkeypatch.setattr(runner.time, "monotonic", lambda: next(ticks))

    with pytest.raises(runner.FormulaCatalogRunTimeoutError):
        _run(root, identity)


@pytest.mark.parametrize("formula,decision_at", [
    ("CLOSE>0", datetime(2026, 4, 15, 16, 59, tzinfo=ZoneInfo("Asia/Shanghai"))),
    ("CLOSE>0", datetime(2026, 4, 15, 17)),
    ("BAD(CLOSE)>0", AFTER_CLOSE),
])
def test_run_rejects_bad_time_or_formula_before_delivering_summary(
    tmp_path: Path, formula: str, decision_at: datetime,
) -> None:
    root, identity = _generation(tmp_path, [("600001.SH", "2026-04-13")], {})

    with pytest.raises((EvaluationRejectedError, ValueError)):
        runner.run_formula_catalog(
            root, formula, DAY, decision_at, expected_identity=identity,
        )
