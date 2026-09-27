"""The web screen reads one verified, bounded replica generation."""

from __future__ import annotations

import importlib
import json
import os
import shutil
import time
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
    has_lower_shadow,
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


def test_verified_replica_reads_only_rule_base_and_requested_ranking_columns(
    tmp_path: Path,
) -> None:
    primary, replica, dates = _world(tmp_path)
    result = _reader(primary, replica).load(
        dates[0], [gt("CLOSE[1]", 1)], include_columns=["CIRC_MV[0]"],
    )
    assert {"CLOSE[0]", "PCT_CHG[0]", "CLOSE[1]", "CIRC_MV[0]"}.issubset(
        result.frame.columns
    )
    assert "OPEN[0]" not in result.frame.columns
    assert "VOL[1]" not in result.frame.columns
    assert pd.isna(result.frame.loc[0, "CIRC_MV[0]"])


def test_verified_replica_rejects_unregistered_dependency(tmp_path: Path) -> None:
    primary, replica, dates = _world(tmp_path)
    with pytest.raises(ValueError, match="metadata"):
        _reader(primary, replica).load(
            dates[0], [lambda frame: frame["CLOSE[0]"] > 0]
        )


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


def test_maximum_volume_and_500_day_aggregate_match_full_same_replica(
    tmp_path: Path,
) -> None:
    from rquant.screen.loader import load_universe

    primary, replica, dates = _world(tmp_path, days=500)
    rules = [volume_ratio_gte(2, offset=30, window=60), has_prior_limit_up(window=500)]
    result = _reader(primary, replica).load(dates[0], rules)
    with DuckDBStore(replica, read_only=True) as store:
        full = load_universe(
            dates[0].isoformat(), lookback=90, store=store,
            aggregate_requests=rules[1].aggregate_requests,
        )
    comparison = [
        "ts_code", "CLOSE[0]", "PCT_CHG[0]", "VOL[30]", "VOL[90]",
        "count_limit_up_500d_ex1",
    ]
    pd.testing.assert_frame_equal(result.frame[comparison], full[comparison])
    for rule in rules:
        pd.testing.assert_series_equal(rule(result.frame), rule(full), check_names=False)


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


def test_primary_path_is_never_statted_or_resolved_by_web_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary, replica, dates = _world(tmp_path)
    original_stat = Path.stat
    original_lstat = Path.lstat
    original_resolve = Path.resolve

    def no_primary_stat(path: Path, *args, **kwargs):
        if path == primary:
            raise PermissionError("primary is hidden from the web service")
        return original_stat(path, *args, **kwargs)

    def no_primary_lstat(path: Path, *args, **kwargs):
        if path == primary:
            raise PermissionError("primary is hidden from the web service")
        return original_lstat(path, *args, **kwargs)

    def no_primary_resolve(path: Path, *args, **kwargs):
        if path == primary:
            raise PermissionError("primary is hidden from the web service")
        return original_resolve(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", no_primary_stat)
    monkeypatch.setattr(Path, "lstat", no_primary_lstat)
    monkeypatch.setattr(Path, "resolve", no_primary_resolve)
    source = _reader(primary, replica)
    assert source.available_dates().dates[0] == dates[0]
    assert source.load(dates[0], [not_limit_up()]).frame["ts_code"].tolist() == ["600001.SH"]


def test_matching_sidecar_cannot_authorize_a_primary_hardlink(tmp_path: Path) -> None:
    primary, replica, dates = _world(tmp_path)
    replica.unlink()
    os.link(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary,
        replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )
    with pytest.raises(_source_module().ScreenReplicaUnavailableError):
        _reader(primary, replica).load(dates[0], [])


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
    sidecar_ctime = replica_generation_path(replica).stat().st_ctime_ns
    deadline = time.monotonic() + 3
    while True:
        before = replica.stat()
        with replica.open("r+b") as handle:
            first = handle.read(1)
            handle.seek(0)
            handle.write(first)
        os.utime(replica, ns=(before.st_atime_ns, before.st_mtime_ns))
        if replica.stat().st_ctime_ns > sidecar_ctime:
            break
        assert time.monotonic() < deadline, "test filesystem did not advance replica ctime"
        time.sleep(0.02)

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


def test_effective_wide_budget_rejects_oversized_history_before_loading(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.screen.loader import (
        BASIC_COLS_MAP,
        IND_COLS_MAP,
        PRICE_COLS_MAP,
        STATE_COLS_MAP,
    )

    primary, replica, dates = _world(tmp_path, days=91)
    with DuckDBStore(primary) as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date) "
            "SELECT 'X' || CAST(i AS VARCHAR), ? FROM range(7999) AS t(i)",
            [dates[0]],
        )
    shutil.copy2(primary, replica)
    write_replica_generation_metadata(
        primary_path=primary, replica_path=replica,
        output_path=replica_generation_path(replica),
        source_before=capture_database_watermark(primary),
    )
    module = _source_module()
    monkeypatch.setattr(
        module, "load_universe",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("loader called")),
    )
    all_fields = {
        *PRICE_COLS_MAP.values(), *IND_COLS_MAP.values(),
        *STATE_COLS_MAP.values(), *BASIC_COLS_MAP.values(),
    }
    too_wide = [
        f"{field}[{offset}]" for field in sorted(all_fields)
        for offset in range(91)
    ]
    with pytest.raises(module.ScreenReplicaBudgetError, match="historical columns"):
        _reader(primary, replica).load(dates[0], [], include_columns=too_wide)

    monkeypatch.setattr(
        module, "load_universe",
        lambda *args, **kwargs: pd.DataFrame({"ts_code": ["600001.SH"]}),
    )
    legal_rules = [has_lower_shadow(offset=offset) for offset in range(26)]
    accepted = _reader(primary, replica).load(
        dates[0], legal_rules,
        include_columns=["CIRC_MV[0]", "TURNOVER_RATE[0]"],
    )
    assert accepted.frame["ts_code"].tolist() == ["600001.SH"]
    monkeypatch.undo()
    actual = _reader(primary, replica).load(
        dates[0], legal_rules,
        include_columns=["CIRC_MV[0]", "TURNOVER_RATE[0]"],
    )
    assert len(actual.frame) == 8000
    assert {"BODY_LOWER[25]", "HIGH[25]", "CIRC_MV[0]", "TURNOVER_RATE[0]"}.issubset(
        actual.frame.columns
    )


def test_volume_ratio_missing_day_fails_closed() -> None:
    frame = pd.DataFrame({"VOL[0]": [100.0], "VOL[1]": [10.0], "VOL[2]": [float("nan")]})
    assert not bool(volume_ratio_gte(2, window=2)(frame).iloc[0])
