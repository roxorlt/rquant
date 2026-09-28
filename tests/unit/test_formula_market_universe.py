"""Offline market-list capture uses six bounded stock-basic partitions."""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from rquant.adapter.tushare import STOCK_BASIC_COLUMNS, TushareAdapter
from rquant.runtime_market_session import MarketCalendarAuthority
from rquant.screen.formula_market_universe import (
    FORMULA_STOCK_BASIC_COLUMNS,
    FormulaMarketUniverseError,
    capture_formula_market_universe,
    load_formula_market_universe,
    publish_formula_market_universe,
)

DAY = date(2026, 9, 28)
SHANGHAI = ZoneInfo("Asia/Shanghai")
PARTITIONS = (
    ("SSE", "L"),
    ("SSE", "P"),
    ("SZSE", "L"),
    ("SZSE", "P"),
    ("BSE", "L"),
    ("BSE", "P"),
)


def _row(code: str, status: str = "L", **changes: str) -> dict[str, str]:
    row = dict.fromkeys(STOCK_BASIC_COLUMNS, "")
    row.update(
        ts_code=code,
        symbol=code.split(".")[0],
        name="测试股份",
        area="北京",
        industry="制造",
        list_date="20200101",
        delist_date="",
        market="主板",
        list_status=status,
    )
    row.update(changes)
    return row


def _partitions() -> dict[tuple[str, str], pd.DataFrame]:
    rows = {
        ("SSE", "L"): [_row("600001.SH"), _row("900001.SH"), _row("510001.SH")],
        ("SSE", "P"): [],
        ("SZSE", "L"): [_row("000001.SZ")],
        ("SZSE", "P"): [_row("300001.SZ", "P")],
        ("BSE", "L"): [_row("830001.BJ")],
        ("BSE", "P"): [],
    }
    return {key: pd.DataFrame(value, columns=STOCK_BASIC_COLUMNS) for key, value in rows.items()}


class FakeAdapter:
    def __init__(self, frames: dict[tuple[str, str], pd.DataFrame]) -> None:
        self.frames = frames
        self.calls: list[tuple[str, str]] = []

    def stock_basic(self, list_status: str = "L", exchange: str = "") -> pd.DataFrame:
        key = (exchange, list_status)
        self.calls.append(key)
        return self.frames[key].copy()


def _calendar(
    *, is_open: bool = True, generated_at: datetime | None = None
) -> MarketCalendarAuthority:
    return MarketCalendarAuthority.create(
        schema_version=1,
        exchange="SSE",
        producer_commit="a" * 40,
        coverage_start=DAY - timedelta(days=1),
        coverage_end=DAY + timedelta(days=1),
        open_dates=(DAY,) if is_open else (),
        generated_at=generated_at or datetime(2026, 9, 28, 8, 0, tzinfo=UTC),
    )


def _clock(
    start: datetime | None = None,
    end: datetime | None = None,
):
    values = iter(
        (
            start or datetime(2026, 9, 28, 17, 1, tzinfo=SHANGHAI),
            end or datetime(2026, 9, 28, 17, 2, tzinfo=SHANGHAI),
        )
    )
    return values.__next__


def _capture(
    frames: dict[tuple[str, str], pd.DataFrame] | None = None,
    *,
    calendar: MarketCalendarAuthority | None = None,
    clock=None,
):
    adapter = FakeAdapter(frames or _partitions())
    result = capture_formula_market_universe(
        adapter,
        calendar or _calendar(),
        DAY,
        clock=clock or _clock(),
    )
    return result, adapter


def test_stock_basic_partition_parameter_preserves_default_call() -> None:
    calls: list[dict[str, str]] = []

    def stock_basic(**kwargs: str) -> pd.DataFrame:
        calls.append(kwargs)
        return pd.DataFrame(columns=STOCK_BASIC_COLUMNS)

    adapter = TushareAdapter.__new__(TushareAdapter)
    adapter._pro = SimpleNamespace(stock_basic=stock_basic)  # type: ignore[attr-defined]
    adapter._transport_observer = None  # type: ignore[attr-defined]

    adapter.stock_basic()
    adapter.stock_basic(list_status="P", exchange="BSE")

    assert [(call["exchange"], call["list_status"]) for call in calls] == [
        ("", "L"),
        ("BSE", "P"),
    ]
    assert all(call["fields"].split(",") == list(STOCK_BASIC_COLUMNS) for call in calls)


def test_capture_contract_asks_for_the_adapters_existing_columns() -> None:
    assert FORMULA_STOCK_BASIC_COLUMNS == STOCK_BASIC_COLUMNS


def test_capture_six_partitions_excludes_b_and_other_without_dropping_suspended() -> None:
    snapshot, adapter = _capture()

    assert adapter.calls == list(PARTITIONS)
    assert snapshot.trade_date == DAY
    assert snapshot.calendar_sha256 == _calendar().content_sha256
    assert [item.ts_code for item in snapshot.entries] == [
        "000001.SZ",
        "300001.SZ",
        "600001.SH",
        "830001.BJ",
    ]
    assert snapshot.entries[1].list_status == "P"
    assert sum(item.raw_rows for item in snapshot.partitions) == 6
    assert sum(item.excluded_b_shares for item in snapshot.partitions) == 1
    assert sum(item.excluded_other for item in snapshot.partitions) == 1
    assert sum(item.included_rows for item in snapshot.partitions) == 4


def test_capture_accepts_provider_missing_value_for_unset_delist_date() -> None:
    frames = _partitions()
    frames[("SSE", "L")].loc[0, "delist_date"] = float("nan")
    snapshot, _ = _capture(frames)
    assert "600001.SH" in {item.ts_code for item in snapshot.entries}


@pytest.mark.parametrize(
    "fault",
    [
        "missing_column",
        "limit",
        "duplicate",
        "cross_partition",
        "wrong_exchange",
        "wrong_status",
        "future_list",
        "past_delist",
        "invalid_date",
        "empty_sse_list",
    ],
)
def test_capture_rejects_incomplete_or_contradictory_partition(fault: str) -> None:
    frames = _partitions()
    if fault == "missing_column":
        frames[("SSE", "P")] = frames[("SSE", "P")].drop(columns=["list_date"])
    elif fault == "limit":
        frames[("SSE", "L")] = pd.DataFrame(
            [_row(f"6{index:05d}.SH") for index in range(6000)],
            columns=STOCK_BASIC_COLUMNS,
        )
    elif fault == "duplicate":
        frames[("SSE", "L")] = pd.concat([frames[("SSE", "L")]] * 2, ignore_index=True)
    elif fault == "cross_partition":
        frames[("SSE", "P")] = pd.DataFrame([_row("600001.SH", "P")])
    elif fault == "wrong_exchange":
        frames[("BSE", "P")] = pd.DataFrame([_row("000003.SZ", "P")])
    elif fault == "wrong_status":
        frames[("BSE", "P")] = pd.DataFrame([_row("830002.BJ", "L")])
    elif fault == "future_list":
        frames[("BSE", "P")] = pd.DataFrame([_row("830002.BJ", "P", list_date="20260929")])
    elif fault == "past_delist":
        frames[("BSE", "P")] = pd.DataFrame([_row("830002.BJ", "P", delist_date="20260928")])
    elif fault == "invalid_date":
        frames[("BSE", "P")] = pd.DataFrame([_row("830002.BJ", "P", list_date="2026-09-01")])
    else:
        frames[("SSE", "L")] = pd.DataFrame([_row("900001.SH")])

    with pytest.raises(FormulaMarketUniverseError):
        _capture(frames)


def test_capture_rejects_closed_day_and_invalid_observation_window() -> None:
    with pytest.raises(FormulaMarketUniverseError, match="open"):
        _capture(calendar=_calendar(is_open=False))
    with pytest.raises(FormulaMarketUniverseError, match="17:00"):
        _capture(clock=_clock(start=datetime(2026, 9, 28, 17, 0, tzinfo=SHANGHAI)))
    with pytest.raises(FormulaMarketUniverseError, match="17:00"):
        _capture(clock=_clock(start=datetime(2026, 9, 28, 16, 59, tzinfo=SHANGHAI)))
    with pytest.raises(FormulaMarketUniverseError, match="date"):
        _capture(clock=_clock(end=datetime(2026, 9, 29, 0, 0, tzinfo=SHANGHAI)))
    with pytest.raises(FormulaMarketUniverseError, match="calendar"):
        _capture(calendar=_calendar(generated_at=datetime(2026, 9, 28, 10, tzinfo=UTC)))


def test_archive_is_idempotent_and_keeps_corrected_generation(tmp_path: Path) -> None:
    root = tmp_path / "market"
    first, _ = _capture()
    saved = publish_formula_market_universe(root, first)
    assert saved.published
    assert load_formula_market_universe(root, DAY, expected_sha256=first.content_sha256) == first
    assert not publish_formula_market_universe(root, first).published

    corrected_frames = _partitions()
    corrected_frames[("SSE", "L")] = pd.concat(
        [
            corrected_frames[("SSE", "L")],
            pd.DataFrame([_row("600002.SH")]),
        ],
        ignore_index=True,
    )
    corrected, _ = _capture(corrected_frames)
    second = publish_formula_market_universe(root, corrected)
    assert second.published
    assert saved.generation_path.is_file()
    assert second.generation_path.is_file()
    with pytest.raises(FormulaMarketUniverseError, match="changed"):
        load_formula_market_universe(root, DAY, expected_sha256=first.content_sha256)
    assert (
        load_formula_market_universe(root, DAY, expected_sha256=corrected.content_sha256)
        == corrected
    )


def test_archive_rejects_corrupt_pointer_and_generation(tmp_path: Path) -> None:
    root = tmp_path / "market"
    snapshot, _ = _capture()
    receipt = publish_formula_market_universe(root, snapshot)
    pointer = root / DAY.isoformat() / "current.json"
    pointer.write_text("{}")
    with pytest.raises(FormulaMarketUniverseError):
        load_formula_market_universe(root, DAY, expected_sha256=snapshot.content_sha256)

    pointer.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "trade_date": DAY.isoformat(),
                "content_sha256": snapshot.content_sha256,
                "file_size": receipt.generation_path.stat().st_size,
            }
        )
    )
    receipt.generation_path.write_text("{}")
    with pytest.raises(FormulaMarketUniverseError):
        load_formula_market_universe(root, DAY, expected_sha256=snapshot.content_sha256)


def test_archive_rejects_symlinked_day_directory(tmp_path: Path) -> None:
    root = tmp_path / "market"
    snapshot, _ = _capture()
    publish_formula_market_universe(root, snapshot)
    date_path = root / DAY.isoformat()
    original = root / "original"
    date_path.rename(original)
    date_path.symlink_to(original, target_is_directory=True)
    with pytest.raises(FormulaMarketUniverseError):
        load_formula_market_universe(root, DAY, expected_sha256=snapshot.content_sha256)


def test_failed_pointer_replace_leaves_current_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "market"
    first, _ = _capture()
    publish_formula_market_universe(root, first)
    frames = _partitions()
    frames[("BSE", "P")] = pd.DataFrame([_row("830002.BJ", "P")])
    second, _ = _capture(frames)

    def fail_replace(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated pointer failure")

    monkeypatch.setattr("rquant.screen.formula_market_universe.os.replace", fail_replace)
    with pytest.raises(FormulaMarketUniverseError):
        publish_formula_market_universe(root, second)
    assert load_formula_market_universe(root, DAY, expected_sha256=first.content_sha256) == first


def test_retry_recovers_complete_generation_left_with_stage_link(tmp_path: Path) -> None:
    root = tmp_path / "market"
    snapshot, _ = _capture()
    receipt = publish_formula_market_universe(root, snapshot)
    day = root / DAY.isoformat()
    (day / "current.json").unlink()
    stage = receipt.generation_path.parent / f".{snapshot.content_sha256}.stage"
    os.link(receipt.generation_path, stage)

    retry = publish_formula_market_universe(root, snapshot)

    assert retry.published
    assert not stage.exists()
    assert receipt.generation_path.stat().st_nlink == 1
    assert (
        load_formula_market_universe(root, DAY, expected_sha256=snapshot.content_sha256) == snapshot
    )
