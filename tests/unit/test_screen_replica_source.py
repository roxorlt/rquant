"""The web screen reads one verified, bounded replica generation."""

from __future__ import annotations

import importlib
import json
import os
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from rquant.replica_generation import (
    capture_database_watermark,
    replica_generation_path,
    write_replica_generation_metadata,
)
from rquant.screen.rules import (
    gt,
    has_prior_limit_up,
    not_limit_up,
    volume_ratio_gte,
)
from rquant.storage.duckdb import DuckDBStore


def _source_module():
    return importlib.import_module("rquant.screen.replica_source")


def _world(tmp_path: Path, *, days: int = 3) -> tuple[Path, Path, list[date]]:
    primary = tmp_path / "rquant.duckdb"
    replica = tmp_path / "rquant_ro.duckdb"
    dates = [date(2026, 4, 15) - timedelta(days=n) for n in range(days)]
    with DuckDBStore(primary) as store:
        for offset, day in enumerate(dates):
            store._conn.execute(
                "INSERT INTO trade_calendar "
                "(exchange, cal_date, is_open, source, updated_at) "
                "VALUES ('SSE', ?, TRUE, 'fixture', ?)",
                [day, datetime(2026, 4, 16, tzinfo=UTC)],
            )
            store._conn.execute(
                "INSERT INTO daily_bar "
                "(ts_code, trade_date, open, high, low, close, pre_close, "
                "pct_chg, vol, amount) VALUES "
                "('600001.SH', ?, 10, 11, 9, ?, 10, 1, ?, 1000)",
                [day, 11.0 if offset == 0 else 10.0, 100.0 if offset == 0 else 10.0],
            )
            store._conn.execute(
                "INSERT INTO stock_status_daily "
                "(ts_code, trade_date, name, is_st, name_source, st_source, "
                "available_at, ingested_at) "
                "VALUES ('600001.SH', ?, '测试股票', FALSE, 'fixture', 'fixture', ?, ?)",
                [
                    day,
                    datetime(day.year, day.month, day.day, 9, 25, tzinfo=UTC),
                    datetime(2026, 4, 16, tzinfo=UTC),
                ],
            )
            store._conn.execute(
                "INSERT INTO daily_state "
                "(ts_code, trade_date, is_st, is_bj, board_type, is_limit_up, "
                "is_limit_down, is_first_limit_up, is_yiziban, consecutive_limit_ups) "
                "VALUES ('600001.SH', ?, FALSE, FALSE, 'main', FALSE, FALSE, "
                "FALSE, FALSE, 0)",
                [day],
            )
    shutil.copy2(primary, replica)
    before = capture_database_watermark(primary)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=before,
    )
    return primary, replica, dates


def _reader(primary: Path, replica: Path):
    return _source_module().VerifiedReplicaScreenSource(
        primary_path=primary,
        replica_path=replica,
    )


def test_verified_replica_returns_full_history_with_cursor_identity_and_time(
    tmp_path: Path,
) -> None:
    primary, replica, dates = _world(tmp_path)
    result = _reader(primary, replica).load(
        dates[0],
        [gt("CLOSE[0]", "CLOSE[1]"), volume_ratio_gte(2, offset=1, window=1)],
    )

    assert result.frame.loc[0, "trade_date"] == dates[0]
    assert result.frame.loc[0, "CLOSE[1]"] == 10.0
    assert result.frame.loc[0, "VOL[2]"] == 10.0
    assert len(result.identity) == 64
    assert result.updated_at.tzinfo is UTC
    file_updated_at = datetime.fromtimestamp(replica.stat().st_mtime, tz=UTC)
    assert abs((result.updated_at - file_updated_at).total_seconds()) < 0.000002
    assert result.identity == _reader(primary, replica).load(dates[0], []).identity
    assert _reader(primary, replica).available_dates().dates == [dates[0], dates[1], dates[2]]


def test_maximum_volume_offset_and_long_aggregate_do_not_widen_to_500_days(
    tmp_path: Path,
) -> None:
    primary, replica, dates = _world(tmp_path, days=500)
    result = _reader(primary, replica).load(
        dates[0],
        [volume_ratio_gte(2, offset=30, window=60), has_prior_limit_up(window=500)],
    )

    assert "VOL[90]" in result.frame.columns
    assert "VOL[91]" not in result.frame.columns
    assert "count_limit_up_500d_ex1" in result.frame.columns
    assert "CLOSE[499]" not in result.frame.columns
    assert result.frame.loc[0, "trade_date"] == dates[0]


@pytest.mark.parametrize("alias", ["symlink", "hardlink"])
def test_main_database_alias_is_never_opened(
    tmp_path: Path, alias: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica, dates = _world(tmp_path)
    replica.unlink()
    if alias == "symlink":
        replica.symlink_to(primary)
    else:
        os.link(primary, replica)
    opened: list[Path] = []
    original = _source_module().connect_pinned_readonly

    def record(path: Path, descriptor: int):
        opened.append(path)
        return original(path, descriptor)

    monkeypatch.setattr(_source_module(), "connect_pinned_readonly", record)
    with pytest.raises(_source_module().ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [])
    assert opened == []


def test_replica_directory_alias_is_not_a_configured_canonical_source(
    tmp_path: Path,
) -> None:
    primary, replica, _dates = _world(tmp_path)
    alias = tmp_path.parent / f"{tmp_path.name}-alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="canonical"):
        _reader(primary, alias / replica.name)


def test_missing_replica_never_falls_back_to_primary(tmp_path: Path) -> None:
    primary, replica, dates = _world(tmp_path)
    replica.unlink()
    with pytest.raises(_source_module().ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [])


def test_sidecar_matching_invalid_database_is_unavailable(tmp_path: Path) -> None:
    primary, replica, dates = _world(tmp_path)
    replica.write_bytes(b"not a duckdb file")
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )
    with pytest.raises(_source_module().ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [])


@pytest.mark.parametrize("change", ["fingerprint", "source_changed", "symlink"])
def test_invalid_sidecar_fails_closed(tmp_path: Path, change: str) -> None:
    primary, replica, dates = _world(tmp_path)
    sidecar = replica_generation_path(replica)
    if change == "symlink":
        target = tmp_path / "sidecar.json"
        sidecar.rename(target)
        sidecar.symlink_to(target)
    else:
        payload = json.loads(sidecar.read_text(encoding="utf-8"))
        if change == "fingerprint":
            payload["replica"]["size"] += 1
        else:
            payload["source_after"]["main"]["size"] += 1
        sidecar.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(_source_module().ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [not_limit_up()])


def test_atomic_replace_during_query_discards_the_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica, dates = _world(tmp_path)
    module = _source_module()
    original = module.load_universe

    def replace(*args, **kwargs):
        result = original(*args, **kwargs)
        replacement = tmp_path / "new.duckdb"
        shutil.copy2(replica, replacement)
        os.replace(replacement, replica)
        return result

    monkeypatch.setattr(module, "load_universe", replace)
    with pytest.raises(module.ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [not_limit_up()])


def test_sidecar_rotation_while_opening_closes_the_held_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica, dates = _world(tmp_path)
    module = _source_module()
    original = module.connect_pinned_readonly
    opened = []

    def rotate(path: Path, descriptor: int):
        connection, branch = original(path, descriptor)
        opened.append(connection)
        sidecar = replica_generation_path(replica)
        sidecar.unlink()
        return connection, branch

    monkeypatch.setattr(module, "connect_pinned_readonly", rotate)
    with pytest.raises(module.ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [])
    assert len(opened) == 1
    with pytest.raises(Exception, match="closed"):
        opened[0].execute("SELECT 1")


def test_in_place_rewrite_with_restored_mtime_discards_the_answer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    primary, replica, dates = _world(tmp_path)
    module = _source_module()
    original = module.load_universe

    def touch_same_bytes(*args, **kwargs):
        result = original(*args, **kwargs)
        before = replica.stat()
        with replica.open("r+b") as handle:
            first = handle.read(1)
            handle.seek(0)
            handle.write(first)
        os.utime(replica, ns=(before.st_atime_ns, before.st_mtime_ns))
        return result

    monkeypatch.setattr(module, "load_universe", touch_same_bytes)
    with pytest.raises(module.ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [not_limit_up()])


def test_in_place_rewrite_before_request_cannot_reuse_the_sidecar(
    tmp_path: Path,
) -> None:
    primary, replica, dates = _world(tmp_path)
    before = replica.stat()
    with replica.open("r+b") as handle:
        first = handle.read(1)
        handle.seek(0)
        handle.write(first)
    os.utime(replica, ns=(before.st_atime_ns, before.st_mtime_ns))

    with pytest.raises(_source_module().ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [])


def test_missing_calendar_does_not_report_an_empty_success(tmp_path: Path) -> None:
    primary, replica, dates = _world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute("DELETE FROM trade_calendar WHERE cal_date = ?", [dates[1]])
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )
    with pytest.raises(_source_module().ScreenReplicaDataError):
        _reader(primary, replica).load(dates[0], [gt("CLOSE[0]", "CLOSE[2]")])


def test_open_trade_date_without_daily_rows_is_data_unavailable_not_zero_hits(
    tmp_path: Path,
) -> None:
    primary, replica, dates = _world(tmp_path)
    with DuckDBStore(primary) as store:
        store._conn.execute("DELETE FROM daily_bar WHERE trade_date = ?", [dates[0]])
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )

    with pytest.raises(_source_module().ScreenReplicaDataError) as failed:
        _reader(primary, replica).load(dates[0], [gt("CLOSE[0]", 1000.0)])
    assert "rquant_ro.duckdb" not in str(failed.value)
    assert "daily_bar" not in str(failed.value)


def test_condition_and_universe_budgets_prevent_unbounded_load(tmp_path: Path) -> None:
    primary, replica, dates = _world(tmp_path)
    module = _source_module()
    with pytest.raises(module.ScreenReplicaBudgetError):
        _reader(primary, replica).load(dates[0], [not_limit_up()] * 27)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date) "
            "SELECT 'X' || CAST(i AS VARCHAR), ? FROM range(8000) AS t(i)",
            [dates[0]],
        )
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )
    with pytest.raises(module.ScreenReplicaBudgetError):
        _reader(primary, replica).load(dates[0], [not_limit_up()])


def test_volume_ratio_missing_day_fails_closed() -> None:
    frame = pd.DataFrame({"VOL[0]": [100.0], "VOL[1]": [10.0], "VOL[2]": [float("nan")]})
    assert not bool(volume_ratio_gte(2, window=2)(frame).iloc[0])
