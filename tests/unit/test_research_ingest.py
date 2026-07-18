"""Daily cloud research ingestion and candidate-observation tests."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

from rquant.research_catalog import ResearchCatalog
from rquant.research_ingest import (
    ResearchAuctionRepairObservation,
    ResearchAuctionRepairPartitionChange,
    ResearchIngestPaths,
    ResearchWatchlistItem,
    assess_research_ingest_readiness,
    inspect_research_authority,
    run_daily_research_ingest,
    write_research_watchlist_snapshot,
)
from rquant.research_lake import export_research_dataset
from rquant.research_migration import ResearchAuthorityCandidate

_COMMIT = "a" * 40
_SNAPSHOT_ID = "research-20260716T215935Z-4e713ead"
_CST = ZoneInfo("Asia/Shanghai")


def _paths(data_dir: Path) -> ResearchIngestPaths:
    return ResearchIngestPaths.from_data_dir(data_dir)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _seed_source(path: Path, trade_date: date) -> None:
    previous = trade_date - timedelta(days=1)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            CREATE TABLE trade_calendar (
                exchange VARCHAR NOT NULL,
                cal_date DATE NOT NULL,
                is_open BOOLEAN NOT NULL,
                PRIMARY KEY (exchange, cal_date)
            );
            CREATE TABLE minute_bar (
                ts_code VARCHAR NOT NULL,
                trade_time TIMESTAMP NOT NULL,
                freq VARCHAR NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                source VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (ts_code, trade_time, freq, source)
            );
            CREATE TABLE auction_bar (
                ts_code VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                auction_type VARCHAR NOT NULL,
                price DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                turnover_rate DOUBLE,
                volume_ratio DOUBLE,
                source VARCHAR NOT NULL,
                created_at TIMESTAMP NOT NULL,
                PRIMARY KEY (ts_code, trade_date, auction_type, source)
            );
            CREATE TABLE daily_bar (
                ts_code VARCHAR NOT NULL,
                trade_date DATE NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                pre_close DOUBLE,
                pct_chg DOUBLE,
                vol DOUBLE,
                amount DOUBLE,
                source VARCHAR,
                PRIMARY KEY (ts_code, trade_date)
            );
            """
        )
        connection.executemany(
            "INSERT INTO trade_calendar VALUES ('SSE', ?, TRUE)",
            [(previous,), (trade_date,)],
        )
        connection.executemany(
            """
            INSERT INTO daily_bar VALUES
                (?, ?, 10, 10.2, 9.9, 10.1, 10, 1, 1000, 10000, 'tushare')
            """,
            [
                ("000001.SZ", previous),
                ("600000.SH", previous),
                ("000001.SZ", trade_date),
                ("600000.SH", trade_date),
            ],
        )


def _minute_frame(trade_date: date, codes: tuple[str, ...]) -> pd.DataFrame:
    morning = pd.date_range(
        f"{trade_date.isoformat()} 09:30:00",
        f"{trade_date.isoformat()} 11:30:00",
        freq="1min",
    )
    afternoon = pd.date_range(
        f"{trade_date.isoformat()} 13:01:00",
        f"{trade_date.isoformat()} 15:00:00",
        freq="1min",
    )
    times = morning.append(afternoon)
    assert len(times) == 241
    rows: list[dict[str, object]] = []
    for code in codes:
        for index, observed_at in enumerate(times):
            price = 10.0 + index / 10_000
            rows.append(
                {
                    "ts_code": code,
                    "trade_time": observed_at,
                    "freq": "1min",
                    "open": price,
                    "high": price + 0.01,
                    "low": price - 0.01,
                    "close": price,
                    "vol": 1000.0,
                    "amount": 10_000.0,
                    "source": "tushare_rt_daily",
                }
            )
    return pd.DataFrame(rows)


class _Adapter:
    def __init__(self, trade_date: date) -> None:
        self.trade_date = trade_date
        self.minute_calls: list[tuple[str, ...]] = []
        self.auction_calls: list[date] = []

    def rt_min_daily(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
        assert freq == "1min"
        self.minute_calls.append(tuple(ts_codes))
        return _minute_frame(self.trade_date, tuple(ts_codes))

    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        self.auction_calls.append(trade_date)
        return pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "trade_date": trade_date,
                    "auction_type": "open_realtime",
                    "price": price,
                    "vol": 1000.0,
                    "amount": 10_000.0,
                    "turnover_rate": 0.1,
                    "volume_ratio": 1.5,
                    "source": "tushare",
                }
                for code, price in (("000001.SZ", 10.0), ("600000.SH", 11.0))
            ]
        )


class _HistoricalAdapter(_Adapter):
    def __init__(self, trade_date: date) -> None:
        super().__init__(trade_date)
        self.historical_minute_calls: list[tuple[str, str, datetime, datetime]] = []

    def stk_mins(
        self,
        ts_code: str,
        freq: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:
        self.historical_minute_calls.append((ts_code, freq, start, end))
        frame = _minute_frame(self.trade_date, (ts_code,))
        frame["source"] = "tushare"
        return frame


def _seed_bootstrap_candidate(data_dir: Path, *, paths: ResearchIngestPaths | None = None) -> Path:
    resolved = paths or _paths(data_dir)
    catalog_path = resolved.catalog_path
    ResearchCatalog(catalog_path).get_coverage("minute_bar")
    candidate = ResearchAuthorityCandidate(
        snapshot_id=_SNAPSHOT_ID,
        code_commit=_COMMIT,
        published_at=datetime(2026, 7, 17, 6, 23, tzinfo=UTC),
        bundle_manifest_sha256="b" * 64,
        source_snapshot_sha256="c" * 64,
        catalog_sha256=_sha256(catalog_path),
        partition_count=670,
        row_count=21_065_728,
        auxiliary_table_count=7,
        artifact_file_count=37,
    )
    candidate_path = data_dir / "research-authority-candidate.json"
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    candidate_path.write_text(candidate.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return candidate_path


def _write_watchlist(
    data_dir: Path,
    trade_date: date,
    *,
    paths: ResearchIngestPaths | None = None,
    code_commit: str = _COMMIT,
) -> Path:
    resolved = paths or _paths(data_dir)
    return write_research_watchlist_snapshot(
        resolved.staging_root,
        trade_date=trade_date,
        items=(
            ResearchWatchlistItem(ts_code="000001.SZ", pool="pool1"),
            ResearchWatchlistItem(ts_code="600000.SH", pool="pool2"),
        ),
        captured_at=datetime.combine(
            trade_date,
            datetime.min.time(),
            tzinfo=_CST,
        ).replace(hour=9, minute=25),
        code_commit=code_commit,
    )


def _seed_bootstrap_partition(paths: ResearchIngestPaths, trade_date: date) -> Path:
    import rquant.research_ingest as ingest_module

    minutes = _minute_frame(trade_date, ("000001.SZ",))
    minutes["created_at"] = datetime.combine(trade_date, datetime.min.time()).replace(
        hour=15, minute=1
    )
    auction = _Adapter(trade_date).stk_auction(trade_date)
    auction["created_at"] = datetime.combine(trade_date, datetime.min.time()).replace(
        hour=9, minute=26
    )
    connection = ingest_module._build_export_source(
        trade_date=trade_date,
        minutes=minutes,
        auction=auction,
    )
    try:
        for dataset in ("minute_bar", "auction_bar"):
            export_research_dataset(
                connection,
                catalog=ResearchCatalog(paths.catalog_path),
                lake_root=paths.lake_root,
                dataset=dataset,
                start_date=trade_date,
                end_date=trade_date,
                code_commit=_COMMIT,
                as_of_date=trade_date,
            )
    finally:
        connection.close()
    manifests = sorted(paths.lake_root.glob("**/manifest.json"))
    assert manifests
    payload = json.loads(manifests[0].read_text(encoding="utf-8"))
    return paths.lake_root / payload["relative_path"]


def test_readiness_requires_complete_current_daily_universe(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)

    ready = assess_research_ingest_readiness(source, trade_date)

    assert ready.status == "ready"
    assert ready.trade_date == trade_date
    assert ready.daily_bar_code_count == 2
    assert ready.latest_daily_bar_date == trade_date

    with duckdb.connect(str(source)) as connection:
        connection.execute(
            "DELETE FROM daily_bar WHERE trade_date = ? AND ts_code = '600000.SH'",
            [trade_date],
        )

    stale = assess_research_ingest_readiness(source, trade_date)

    assert stale.status == "not_ready"
    assert stale.daily_bar_code_count == 1
    assert "daily_bar_universe_incomplete" in stale.issues


def test_readiness_treats_authoritative_closed_day_as_ready_skip(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    with duckdb.connect(str(source)) as connection:
        connection.execute(
            "UPDATE trade_calendar SET is_open = FALSE WHERE cal_date = ?",
            [trade_date],
        )
        connection.execute("DELETE FROM daily_bar WHERE trade_date = ?", [trade_date])

    result = assess_research_ingest_readiness(source, trade_date)

    assert result.status == "closed"
    assert result.daily_bar_code_count == 0
    assert result.issues == ()


def test_historical_recovery_uses_stk_mins_in_sequence(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    previous = date(2026, 7, 16)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, previous)
    _write_watchlist(tmp_path, trade_date)
    run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=previous,
        adapter=_Adapter(previous),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 16, 15, 30, tzinfo=_CST),
    )
    adapter = _HistoricalAdapter(trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=adapter,
        code_commit=_COMMIT,
        recovery=True,
        now=lambda: datetime(2026, 7, 18, 8, 0, tzinfo=_CST),
    )

    assert result.status == "candidate"
    assert adapter.minute_calls == []
    assert [call[0] for call in adapter.historical_minute_calls] == [
        "000001.SZ",
        "600000.SH",
    ]
    assert all(call[1] == "1min" for call in adapter.historical_minute_calls)
    assert all(call[2].date() == trade_date for call in adapter.historical_minute_calls)
    assert all(call[3].date() == trade_date for call in adapter.historical_minute_calls)


def test_first_historical_recovery_uses_bootstrap_catalog_anchor(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    previous = date(2026, 7, 16)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_partition(paths, previous)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    adapter = _HistoricalAdapter(trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=adapter,
        code_commit=_COMMIT,
        recovery=True,
        now=lambda: datetime(2026, 7, 18, 8, 0, tzinfo=_CST),
    )

    assert result.status == "candidate"
    assert len(adapter.historical_minute_calls) == 2


def test_first_historical_recovery_rejects_gap_from_bootstrap_anchor(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 18)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_partition(paths, date(2026, 7, 16))
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    adapter = _HistoricalAdapter(trade_date)

    with pytest.raises(RuntimeError, match="observation gap"):
        run_daily_research_ingest(
            source_database=source,
            paths=paths,
            trade_date=trade_date,
            adapter=adapter,
            code_commit=_COMMIT,
            recovery=True,
            now=lambda: datetime(2026, 7, 19, 8, 0, tzinfo=_CST),
        )

    assert adapter.historical_minute_calls == []
    assert adapter.auction_calls == []


def test_normal_ingest_cannot_skip_a_missing_observation_day(tmp_path: Path) -> None:
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, date(2026, 7, 17))
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, date(2026, 7, 16))
    run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=date(2026, 7, 16),
        adapter=_Adapter(date(2026, 7, 16)),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 16, 15, 30, tzinfo=_CST),
    )
    with duckdb.connect(str(source)) as connection:
        connection.execute(
            "INSERT INTO trade_calendar VALUES ('SSE', DATE '2026-07-18', TRUE)"
        )
        connection.executemany(
            """
            INSERT INTO daily_bar VALUES
                (?, DATE '2026-07-18', 10, 10.2, 9.9, 10.1, 10, 1, 1000, 10000, 'tushare')
            """,
            [("000001.SZ",), ("600000.SH",)],
        )
    _write_watchlist(tmp_path, date(2026, 7, 18))
    adapter = _Adapter(date(2026, 7, 18))

    with pytest.raises(RuntimeError, match="observation gap"):
        run_daily_research_ingest(
            source_database=source,
            paths=paths,
            trade_date=date(2026, 7, 18),
            adapter=adapter,
            code_commit=_COMMIT,
            now=lambda: datetime(2026, 7, 18, 15, 30, tzinfo=_CST),
        )

    assert adapter.minute_calls == []
    assert adapter.auction_calls == []


def test_historical_recovery_is_forbidden_during_market_protection_window(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 16)
    adapter = _HistoricalAdapter(trade_date)

    with pytest.raises(ValueError, match="market protection window"):
        run_daily_research_ingest(
            source_database=tmp_path / "not-opened.duckdb",
            paths=_paths(tmp_path),
            trade_date=trade_date,
            adapter=adapter,
            code_commit=_COMMIT,
            recovery=True,
            now=lambda: datetime(2026, 7, 17, 10, 0, tzinfo=_CST),
        )

    assert adapter.historical_minute_calls == []


def test_watchlist_snapshot_is_atomic_typed_and_idempotent(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)

    first = _write_watchlist(tmp_path, trade_date)
    second = _write_watchlist(tmp_path, trade_date)

    assert first == second
    assert first.is_file()
    assert not list(first.parent.glob("*.tmp-*"))
    assert '"ts_code": "000001.SZ"' in first.read_text(encoding="utf-8")


def test_trade_date_probe_distinguishes_closed_day_from_calendar_gap(
    tmp_path: Path,
) -> None:
    from rquant.research_ingest import research_trade_date_is_open

    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    with duckdb.connect(str(source)) as connection:
        connection.execute(
            "UPDATE trade_calendar SET is_open = FALSE WHERE cal_date = ?",
            [trade_date],
        )

    assert research_trade_date_is_open(source, trade_date) is False

    with duckdb.connect(str(source)) as connection:
        connection.execute(
            "DELETE FROM trade_calendar WHERE cal_date = ?",
            [trade_date],
        )
    with pytest.raises(ValueError, match="missing SSE trade date"):
        research_trade_date_is_open(source, trade_date)


def test_watchlist_snapshot_cannot_be_created_or_replaced_after_open(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    first = _write_watchlist(tmp_path, trade_date)
    before = first.read_bytes()

    with pytest.raises(RuntimeError, match="immutable"):
        write_research_watchlist_snapshot(
            _paths(tmp_path).staging_root,
            trade_date=trade_date,
            items=(ResearchWatchlistItem(ts_code="000001.SZ", pool="pool1"),),
            captured_at=datetime(2026, 7, 17, 14, 0, tzinfo=_CST),
            code_commit=_COMMIT,
        )
    assert first.read_bytes() == before

    other = tmp_path / "other"
    with pytest.raises(ValueError, match="before 09:30"):
        write_research_watchlist_snapshot(
            _paths(other).staging_root,
            trade_date=trade_date,
            items=(ResearchWatchlistItem(ts_code="000001.SZ", pool="pool1"),),
            captured_at=datetime(2026, 7, 17, 14, 0, tzinfo=_CST),
            code_commit=_COMMIT,
        )
    assert not (other / "research_staging").exists()


def test_daily_ingest_rejects_current_session_before_network_or_writes(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    adapter = _Adapter(trade_date)

    with pytest.raises(ValueError, match="15:15"):
        run_daily_research_ingest(
            source_database=source,
            paths=_paths(tmp_path),
            trade_date=trade_date,
            adapter=adapter,
            code_commit=_COMMIT,
            now=lambda: datetime(2026, 7, 17, 14, 59, tzinfo=_CST),
        )

    assert adapter.minute_calls == []
    assert adapter.auction_calls == []
    assert not (tmp_path / "research-authority-current.json").exists()


def test_daily_ingest_completes_partitions_and_publishes_candidate_observation(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    adapter = _Adapter(trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    assert result.status == "candidate"
    assert result.stable_trading_days == 1
    assert result.minute.expected_code_count == 2
    assert result.minute.complete_code_count == 2
    assert result.minute.coverage_ratio == 1.0
    assert result.auction.expected_code_count == 2
    assert result.auction.coverage_ratio == 1.0
    assert result.issues == ()
    assert adapter.minute_calls == [("000001.SZ", "600000.SH")]
    assert adapter.auction_calls == [trade_date]

    catalog = tmp_path / "research.duckdb"
    readonly = tmp_path / "research_ro.duckdb"
    current = tmp_path / "research-authority-current.json"
    observation = (
        tmp_path
        / "research_observations"
        / "trade_date=2026-07-17"
        / f"{result.observation_id}.json"
    )
    assert readonly.is_file()
    assert current.is_file()
    assert observation.is_file()
    assert _sha256(catalog) == result.catalog_sha256
    assert _sha256(readonly) == result.readonly_catalog_sha256

    with duckdb.connect(str(catalog), read_only=True) as connection:
        coverage = connection.execute(
            "SELECT dataset, latest_date, row_count FROM research_dataset_coverage ORDER BY dataset"
        ).fetchall()
    assert coverage == [
        ("auction_bar", trade_date, 2),
        ("minute_bar", trade_date, 482),
    ]

    status = inspect_research_authority(_paths(tmp_path))
    assert status.status == "candidate"
    assert status.catalog_hash_matches is True
    assert status.readonly_catalog_hash_matches is True
    assert status.stable_trading_days == 1
    assert status.eligible_for_promotion is False


def test_missing_watchlist_never_becomes_candidate_even_with_observed_minutes(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    with duckdb.connect(str(source)) as connection:
        rows = _minute_frame(trade_date, ("000001.SZ",))
        rows["created_at"] = datetime(2026, 7, 17, 15, 1)
        connection.register("minutes", rows)
        connection.execute("INSERT INTO minute_bar SELECT * FROM minutes")
    adapter = _Adapter(trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    assert result.status == "degraded"
    assert result.stable_trading_days == 0
    assert "watchlist_snapshot_missing" in result.issues
    assert adapter.minute_calls == [("000001.SZ",)]


def test_merge_frames_normalizes_mixed_business_date_types() -> None:
    import rquant.research_ingest as ingest_module

    base = {
        "ts_code": "000001.SZ",
        "auction_type": "open",
        "price": 10.0,
        "vol": 1_000.0,
        "amount": 10_000.0,
        "turnover_rate": 0.1,
        "volume_ratio": 1.5,
        "source": "tushare",
    }
    existing = pd.DataFrame(
        [
            {
                **base,
                "trade_date": date(2026, 7, 16),
                "created_at": datetime(2026, 7, 16, 9, 26),
            }
        ]
    )
    fetched = pd.DataFrame(
        [
            {
                **base,
                "trade_date": pd.Timestamp("2026-07-16"),
                "price": 10.1,
                "created_at": pd.Timestamp("2026-07-16 15:50:00"),
            }
        ]
    )

    merged = ingest_module._merge_frames(
        existing,
        fetched,
        columns=ingest_module._AUCTION_COLUMNS,
        primary_key=("ts_code", "trade_date", "auction_type", "source"),
    )

    assert len(merged) == 1
    assert merged.iloc[0]["trade_date"] == date(2026, 7, 16)
    assert merged.iloc[0]["price"] == 10.1
    assert merged.iloc[0]["created_at"] == pd.Timestamp("2026-07-16 15:50:00")


def test_merge_frames_keeps_first_observation_when_business_values_match() -> None:
    import rquant.research_ingest as ingest_module

    existing = _Adapter(date(2026, 7, 16)).stk_auction(date(2026, 7, 16)).iloc[[0]]
    existing["created_at"] = pd.Timestamp("2026-07-16 09:26:00")
    fetched = existing.copy()
    fetched["created_at"] = pd.Timestamp("2026-07-16 15:50:00")

    merged = ingest_module._merge_frames(
        existing,
        fetched,
        columns=ingest_module._AUCTION_COLUMNS,
        primary_key=("ts_code", "trade_date", "auction_type", "source"),
    )

    assert len(merged) == 1
    assert merged.iloc[0]["created_at"] == pd.Timestamp("2026-07-16 09:26:00")


def test_dry_run_merges_parquet_dates_with_operational_duckdb_timestamps(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 17)
    paths = _paths(tmp_path)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    with duckdb.connect(str(source)) as connection:
        connection.execute(
            """
            INSERT INTO auction_bar VALUES (
                '000001.SZ', ?, 'open_realtime', 10.1, 1000, 10000,
                0.1, 1.5, 'tushare', ?
            )
            """,
            [trade_date, datetime(2026, 7, 17, 15, 50)],
        )

    _seed_bootstrap_partition(paths, trade_date)
    candidate_path = _seed_bootstrap_candidate(tmp_path, paths=paths)
    _write_watchlist(tmp_path, trade_date, paths=paths)
    catalog_before = _sha256(paths.catalog_path)
    candidate_before = _sha256(candidate_path)
    adapter = _Adapter(trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=adapter,
        code_commit=_COMMIT,
        dry_run=True,
        now=lambda: datetime(2026, 7, 17, 10, 0, tzinfo=_CST),
    )

    assert result.status == "planned"
    assert adapter.minute_calls == []
    assert adapter.auction_calls == []
    assert _sha256(paths.catalog_path) == catalog_before
    assert _sha256(candidate_path) == candidate_before
    assert not paths.readonly_catalog_path.exists()


def test_dry_run_does_not_call_network_or_mutate_candidate(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    candidate_path = _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    before = _sha256(candidate_path)
    adapter = _Adapter(trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=adapter,
        code_commit="unknown",
        dry_run=True,
        now=lambda: datetime(2026, 7, 17, 10, 0, tzinfo=_CST),
    )

    assert result.status == "planned"
    assert adapter.minute_calls == []
    assert adapter.auction_calls == []
    assert _sha256(candidate_path) == before
    assert not (tmp_path / "research-authority-current.json").exists()
    assert not (tmp_path / "research_ro.duckdb").exists()


def test_authority_status_fails_closed_after_catalog_tamper(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    with (tmp_path / "research.duckdb").open("ab") as handle:
        handle.write(b"tampered")

    status = inspect_research_authority(_paths(tmp_path))
    assert status.status == "invalid"
    assert status.catalog_hash_matches is False
    assert status.eligible_for_promotion is False
    assert "catalog_hash_mismatch" in status.issues


def test_second_dataset_failure_does_not_publish_partial_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.research_ingest as ingest_module

    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    candidate_path = _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    catalog_path = tmp_path / "research.duckdb"
    catalog_before = _sha256(catalog_path)
    candidate_before = _sha256(candidate_path)
    real_export = ingest_module.export_research_dataset

    def fail_auction(*args: object, **kwargs: object):
        if kwargs["dataset"] == "auction_bar":
            raise RuntimeError("auction export failed")
        return real_export(*args, **kwargs)

    monkeypatch.setattr(ingest_module, "export_research_dataset", fail_auction)

    with pytest.raises(RuntimeError, match="auction export failed"):
        run_daily_research_ingest(
            source_database=source,
            paths=_paths(tmp_path),
            trade_date=trade_date,
            adapter=_Adapter(trade_date),
            code_commit=_COMMIT,
            now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
        )

    assert _sha256(catalog_path) == catalog_before
    assert _sha256(candidate_path) == candidate_before
    assert not (tmp_path / "research-authority-current.json").exists()
    assert not list(tmp_path.glob(".research.duckdb.ingest-*.tmp*"))
    assert not list((tmp_path / "lake").glob("**/manifest.json"))


def test_watchlist_commit_mismatch_degrades_candidate(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date, code_commit="b" * 40)

    result = run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    assert result.status == "degraded"
    assert "watchlist_code_commit_mismatch" in result.issues


def test_non_dry_run_only_accepts_current_trade_date(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 16)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    adapter = _Adapter(trade_date)

    with pytest.raises(ValueError, match="current trade date"):
        run_daily_research_ingest(
            source_database=source,
            paths=_paths(tmp_path),
            trade_date=trade_date,
            adapter=adapter,
            code_commit=_COMMIT,
            now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
        )

    assert adapter.minute_calls == []
    assert adapter.auction_calls == []


def test_missing_middle_minute_fails_exact_grid_coverage(tmp_path: Path) -> None:
    class MissingMinuteAdapter(_Adapter):
        def rt_min_daily(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
            frame = super().rt_min_daily(ts_codes, freq)
            missing = pd.Timestamp(f"{self.trade_date.isoformat()} 10:07:00")
            return frame.loc[frame["trade_time"] != missing].reset_index(drop=True)

    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=MissingMinuteAdapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    assert result.status == "degraded"
    assert result.minute.complete_code_count == 0
    assert "minute_watchlist_coverage_incomplete" in result.issues


def test_partial_daily_bar_cannot_shrink_auction_denominator(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    with duckdb.connect(str(source)) as connection:
        connection.execute(
            "DELETE FROM daily_bar WHERE trade_date = ? AND ts_code = '600000.SH'",
            [trade_date],
        )
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    assert result.status == "degraded"
    assert "daily_bar_auction_universe_incomplete" in result.issues
    assert "auction_observed_precision_below_98pct" in result.issues


def test_extra_off_session_minute_fails_exact_grid_coverage(tmp_path: Path) -> None:
    class ExtraMinuteAdapter(_Adapter):
        def rt_min_daily(self, ts_codes: list[str], freq: str = "1min") -> pd.DataFrame:
            frame = super().rt_min_daily(ts_codes, freq)
            extras = frame.groupby("ts_code", as_index=False).first()
            extras["trade_time"] = pd.Timestamp(f"{self.trade_date.isoformat()} 12:00:00")
            return pd.concat([frame, extras], ignore_index=True)

    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)

    result = run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=ExtraMinuteAdapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    assert result.status == "degraded"
    assert result.minute.complete_code_count == 0


def test_custom_research_paths_are_used_end_to_end(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    paths = ResearchIngestPaths(
        state_dir=tmp_path / "state",
        catalog_path=tmp_path / "catalog" / "metadata.duckdb",
        readonly_catalog_path=tmp_path / "readonly" / "metadata_ro.duckdb",
        lake_root=tmp_path / "parquet-lake",
        staging_root=tmp_path / "evidence",
    )
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(paths.state_dir, paths=paths)
    _write_watchlist(paths.state_dir, trade_date, paths=paths)

    result = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )

    assert result.status == "candidate"
    assert paths.catalog_path.is_file()
    assert paths.readonly_catalog_path.is_file()
    assert list(paths.lake_root.glob("**/manifest.json"))
    assert (paths.state_dir / "research-authority-current.json").is_file()
    assert not (tmp_path / "research.duckdb").exists()


def test_current_marker_tamper_cannot_forge_promotion(tmp_path: Path) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    run_daily_research_ingest(
        source_database=source,
        paths=_paths(tmp_path),
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )
    current_path = tmp_path / "research-authority-current.json"
    payload = json.loads(current_path.read_text(encoding="utf-8"))
    payload["stable_trading_days"] = 10
    payload["stability_parent_sha256"] = "d" * 64
    payload["previous_stable_trade_date"] = "2026-07-16"
    current_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    status = inspect_research_authority(_paths(tmp_path))

    assert status.status == "invalid"
    assert status.eligible_for_promotion is False
    assert "current_observation_mismatch" in status.issues


def test_publish_failure_rolls_back_catalog_lake_and_marker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.research_ingest as ingest_module

    trade_date = date(2026, 7, 17)
    paths = _paths(tmp_path)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    catalog_before = _sha256(paths.catalog_path)
    real_copy = ingest_module._copy_file_atomic
    failed = False

    def fail_once_on_readonly(source_path: Path, target_path: Path) -> None:
        nonlocal failed
        if target_path == paths.readonly_catalog_path and not failed:
            failed = True
            raise RuntimeError("readonly publish failed")
        real_copy(source_path, target_path)

    monkeypatch.setattr(ingest_module, "_copy_file_atomic", fail_once_on_readonly)

    with pytest.raises(RuntimeError, match="readonly publish failed"):
        run_daily_research_ingest(
            source_database=source,
            paths=paths,
            trade_date=trade_date,
            adapter=_Adapter(trade_date),
            code_commit=_COMMIT,
            now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
        )

    assert _sha256(paths.catalog_path) == catalog_before
    assert not paths.readonly_catalog_path.exists()
    assert not (paths.state_dir / "research-authority-current.json").exists()
    assert not list(paths.lake_root.glob("**/manifest.json"))
    assert not list(paths.lake_root.glob("**/*.parquet"))
    assert not list(paths.transactions_root.glob("*/publish-journal.json"))

    result = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 31, tzinfo=_CST),
    )
    assert result.status == "candidate"


def test_next_run_recovers_interrupted_publish_before_validation(tmp_path: Path) -> None:
    import rquant.research_ingest as ingest_module

    trade_date = date(2026, 7, 17)
    paths = _paths(tmp_path)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    first = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )
    catalog_before = _sha256(paths.catalog_path)
    current_before = (paths.state_dir / "research-authority-current.json").read_bytes()
    transaction_root = paths.transactions_root / "tx-interrupted"
    staged_catalog, _ = ingest_module._prepare_catalog_generation(
        paths,
        trade_date=trade_date,
        transaction_root=transaction_root,
        expected_hash=catalog_before,
    )
    staged_readonly = transaction_root / "readonly.next.duckdb"
    ingest_module._prepare_readonly_generation(staged_catalog, staged_readonly)
    interrupted_result = first.model_copy(
        update={
            "observation_id": "interrupted",
            "generated_at": datetime(2026, 7, 17, 15, 31, tzinfo=_CST),
        }
    )
    ingest_module._prepare_publish_journal(
        paths,
        transaction_root=transaction_root,
        staged_catalog=staged_catalog,
        staged_readonly=staged_readonly,
        result=interrupted_result,
    )
    pending = inspect_research_authority(paths)
    assert pending.status == "invalid"
    assert "interrupted_publish_pending_recovery" in pending.issues

    second = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 32, tzinfo=_CST),
    )

    assert second.status == "candidate"
    assert second.stable_trading_days == first.stable_trading_days
    assert second.previous_observation_sha256 == hashlib.sha256(current_before).hexdigest()
    assert second.minute.export.unchanged_count == 1
    assert second.auction.export.unchanged_count == 1
    assert not list(paths.transactions_root.glob("*/publish-journal.json"))
    assert (paths.state_dir / "research-authority-current.json").read_bytes() != current_before


def test_recovery_rejects_a_dangling_publish_journal_symlink(
    tmp_path: Path,
) -> None:
    import rquant.research_ingest as ingest_module

    paths = _paths(tmp_path)
    transaction_root = paths.transactions_root / "tx-dangling-journal"
    transaction_root.mkdir(parents=True)
    journal_path = transaction_root / "minute-repair-journal.json"
    journal_path.symlink_to(tmp_path / "missing-journal.json")

    with pytest.raises(RuntimeError, match="publish journal path is invalid"):
        ingest_module._recover_interrupted_publish(paths)

    assert transaction_root.is_dir()
    assert journal_path.is_symlink()


def test_recovery_rejects_a_symlinked_transactions_root_without_deleting_target(
    tmp_path: Path,
) -> None:
    import rquant.research_ingest as ingest_module

    paths = _paths(tmp_path)
    external_root = tmp_path / "external-transactions"
    victim = external_root / "victim"
    victim.mkdir(parents=True)
    marker = victim / "keep.txt"
    marker.write_text("keep\n", encoding="utf-8")
    paths.transactions_root.symlink_to(
        external_root,
        target_is_directory=True,
    )

    with pytest.raises(RuntimeError, match="transactions root"):
        ingest_module._recover_interrupted_publish(paths)

    assert marker.read_text(encoding="utf-8") == "keep\n"


def test_rollback_cas_never_overwrites_a_third_party_generation(tmp_path: Path) -> None:
    import rquant.research_ingest as ingest_module

    trade_date = date(2026, 7, 17)
    paths = _paths(tmp_path)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    first = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )
    transaction_root = paths.transactions_root / "tx-cas-conflict"
    staged_catalog, _ = ingest_module._prepare_catalog_generation(
        paths,
        trade_date=trade_date,
        transaction_root=transaction_root,
        expected_hash=_sha256(paths.catalog_path),
    )
    staged_readonly = transaction_root / "readonly.next.duckdb"
    ingest_module._prepare_readonly_generation(staged_catalog, staged_readonly)
    interrupted_result = first.model_copy(
        update={
            "observation_id": "cas-conflict",
            "generated_at": datetime(2026, 7, 17, 15, 31, tzinfo=_CST),
        }
    )
    ingest_module._prepare_publish_journal(
        paths,
        transaction_root=transaction_root,
        staged_catalog=staged_catalog,
        staged_readonly=staged_readonly,
        result=interrupted_result,
    )
    with paths.catalog_path.open("ab") as handle:
        handle.write(b"third-party-generation")
    third_party_hash = _sha256(paths.catalog_path)

    with pytest.raises(RuntimeError, match="rollback CAS mismatch"):
        ingest_module._rollback_publish_transaction(paths, transaction_root)

    assert _sha256(paths.catalog_path) == third_party_hash
    assert (transaction_root / "publish-journal.json").is_file()


def test_rollback_verifies_every_backup_before_restoring_any_target(
    tmp_path: Path,
) -> None:
    import rquant.research_ingest as ingest_module

    trade_date = date(2026, 7, 17)
    paths = _paths(tmp_path)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    first = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )
    transaction_root = paths.transactions_root / "tx-corrupt-backup"
    staged_catalog, _ = ingest_module._prepare_catalog_generation(
        paths,
        trade_date=trade_date,
        transaction_root=transaction_root,
        expected_hash=_sha256(paths.catalog_path),
    )
    staged_readonly = transaction_root / "readonly.next.duckdb"
    ingest_module._prepare_readonly_generation(staged_catalog, staged_readonly)
    interrupted_result = first.model_copy(
        update={
            "observation_id": "corrupt-backup",
            "generated_at": datetime(2026, 7, 17, 15, 31, tzinfo=_CST),
        }
    )
    ingest_module._prepare_publish_journal(
        paths,
        transaction_root=transaction_root,
        staged_catalog=staged_catalog,
        staged_readonly=staged_readonly,
        result=interrupted_result,
    )
    current_path = paths.state_dir / "research-authority-current.json"
    ingest_module._write_model_atomic(current_path, interrupted_result)
    current_after = current_path.read_bytes()
    with (transaction_root / "minute-manifest.before").open("ab") as handle:
        handle.write(b"corrupt-backup")

    with pytest.raises(RuntimeError, match="backup hash mismatch"):
        ingest_module._rollback_publish_transaction(paths, transaction_root)

    assert current_path.read_bytes() == current_after
    assert (transaction_root / "publish-journal.json").is_file()


def test_rollback_preflights_late_observation_conflict_before_any_restore(
    tmp_path: Path,
) -> None:
    import rquant.research_ingest as ingest_module

    trade_date = date(2026, 7, 17)
    paths = _paths(tmp_path)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    first = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )
    transaction_root = paths.transactions_root / "tx-late-cas-conflict"
    staged_catalog, _ = ingest_module._prepare_catalog_generation(
        paths,
        trade_date=trade_date,
        transaction_root=transaction_root,
        expected_hash=_sha256(paths.catalog_path),
    )
    staged_readonly = transaction_root / "readonly.next.duckdb"
    ingest_module._prepare_readonly_generation(staged_catalog, staged_readonly)
    interrupted_result = first.model_copy(
        update={
            "observation_id": "late-cas-conflict",
            "generated_at": datetime(2026, 7, 17, 15, 31, tzinfo=_CST),
        }
    )
    journal = ingest_module._prepare_publish_journal(
        paths,
        transaction_root=transaction_root,
        staged_catalog=staged_catalog,
        staged_readonly=staged_readonly,
        result=interrupted_result,
    )
    current_path = paths.state_dir / "research-authority-current.json"
    ingest_module._write_model_atomic(current_path, interrupted_result)
    current_after = current_path.read_bytes()
    journal.observation_path.parent.mkdir(parents=True, exist_ok=True)
    journal.observation_path.write_bytes(b"third-party-observation")

    with pytest.raises(RuntimeError, match="observation CAS mismatch"):
        ingest_module._rollback_publish_transaction(paths, transaction_root)

    assert current_path.read_bytes() == current_after
    assert (transaction_root / "publish-journal.json").is_file()


def test_rollback_rejects_missing_version_that_existed_before_publish(
    tmp_path: Path,
) -> None:
    import rquant.research_ingest as ingest_module

    trade_date = date(2026, 7, 17)
    paths = _paths(tmp_path)
    source = tmp_path / "source.duckdb"
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    first = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 15, 30, tzinfo=_CST),
    )
    transaction_root = paths.transactions_root / "tx-missing-existing-version"
    staged_catalog, _ = ingest_module._prepare_catalog_generation(
        paths,
        trade_date=trade_date,
        transaction_root=transaction_root,
        expected_hash=_sha256(paths.catalog_path),
    )
    staged_readonly = transaction_root / "readonly.next.duckdb"
    ingest_module._prepare_readonly_generation(staged_catalog, staged_readonly)
    journal = ingest_module._prepare_publish_journal(
        paths,
        transaction_root=transaction_root,
        staged_catalog=staged_catalog,
        staged_readonly=staged_readonly,
        result=first.model_copy(
            update={
                "observation_id": "missing-existing-version",
                "generated_at": datetime(2026, 7, 17, 15, 31, tzinfo=_CST),
            }
        ),
    )
    assert journal.minute_version_existed is True
    minute_version = paths.lake_root / journal.minute_version_relative_path
    minute_version.unlink()

    with pytest.raises(RuntimeError, match="version CAS mismatch"):
        ingest_module._rollback_publish_transaction(paths, transaction_root)

    assert (transaction_root / "publish-journal.json").is_file()


def test_atomic_model_creation_fsyncs_every_new_directory_entry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.research_ingest as ingest_module

    fsynced: list[Path] = []
    monkeypatch.setattr(ingest_module, "_fsync_directory", fsynced.append)
    target = tmp_path / "research_observations" / "trade_date=2026-07-17" / "item.json"

    ingest_module._write_model_atomic(
        target,
        ResearchWatchlistItem(ts_code="000001.SZ", pool="pool1"),
    )

    assert fsynced == [
        tmp_path,
        tmp_path / "research_observations",
        tmp_path / "research_observations" / "trade_date=2026-07-17",
    ]


def test_promotion_requires_ten_intact_candidate_observations(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    bootstrap_partition = _seed_bootstrap_partition(paths, date(2026, 6, 30))
    _seed_bootstrap_candidate(tmp_path)
    first_date = date(2026, 7, 1)

    for offset in range(10):
        trade_date = first_date + timedelta(days=offset)
        source = tmp_path / f"source-{trade_date.isoformat()}.duckdb"
        _seed_source(source, trade_date)
        _write_watchlist(tmp_path, trade_date)
        result = run_daily_research_ingest(
            source_database=source,
            paths=paths,
            trade_date=trade_date,
            adapter=_Adapter(trade_date),
            code_commit=_COMMIT,
            now=lambda current=trade_date: datetime.combine(
                current,
                datetime.min.time(),
                tzinfo=_CST,
            ).replace(hour=15, minute=30),
        )
        assert result.stable_trading_days == offset + 1

    status = inspect_research_authority(paths)
    assert status.status == "candidate"
    assert status.stable_trading_days == 10
    assert status.eligible_for_promotion is True

    bootstrap_before = bootstrap_partition.read_bytes()
    with bootstrap_partition.open("ab") as handle:
        handle.write(b"tampered-bootstrap")
    tampered_bootstrap = inspect_research_authority(paths)
    assert tampered_bootstrap.status == "invalid"
    assert "catalog_lake_partition_hash_mismatch" in tampered_bootstrap.issues
    bootstrap_partition.write_bytes(bootstrap_before)
    assert inspect_research_authority(paths).eligible_for_promotion is True

    evidence = sorted((paths.state_dir / "research_observations").glob("**/*.json"))[4]
    payload = json.loads(evidence.read_text(encoding="utf-8"))
    relative_data_path = payload["minute"]["export"]["partitions"][0]["manifest"]["relative_path"]
    historical_partition = paths.lake_root / relative_data_path
    partition_before = historical_partition.read_bytes()
    with historical_partition.open("ab") as handle:
        handle.write(b"tampered-history")

    tampered_partition = inspect_research_authority(paths)
    assert tampered_partition.status == "invalid"
    assert tampered_partition.eligible_for_promotion is False
    assert "lake_partition_hash_mismatch" in tampered_partition.issues

    historical_partition.write_bytes(partition_before)
    assert inspect_research_authority(paths).eligible_for_promotion is True

    payload["code_commit"] = "b" * 40
    evidence.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    tampered = inspect_research_authority(paths)
    assert tampered.status == "invalid"
    assert tampered.eligible_for_promotion is False
    assert "observation_lineage_broken" in tampered.issues


def test_legacy_daily_observation_without_kind_remains_authoritative(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    result = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    observation_path = (
        paths.state_dir
        / "research_observations"
        / f"trade_date={trade_date.isoformat()}"
        / f"{result.observation_id}.json"
    )
    payload = result.model_dump(mode="json")
    assert "observation_kind" not in payload
    legacy = json.dumps(payload, ensure_ascii=False, indent=2).encode() + b"\n"
    observation_path.write_bytes(legacy)
    (paths.state_dir / "research-authority-current.json").write_bytes(legacy)

    status = inspect_research_authority(paths)

    assert status.status == "candidate"
    assert status.latest_trade_date == trade_date
    assert status.stable_trading_days == 1
    assert status.issues == ()


def test_repair_observation_resets_stability_and_next_daily_starts_at_one(
    tmp_path: Path,
) -> None:
    first_date = date(2026, 7, 16)
    first_source = tmp_path / "first.duckdb"
    paths = _paths(tmp_path)
    _seed_source(first_source, first_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, first_date)
    first = run_daily_research_ingest(
        source_database=first_source,
        paths=paths,
        trade_date=first_date,
        adapter=_Adapter(first_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 16, 16, 0, tzinfo=_CST),
    )
    current_path = paths.state_dir / "research-authority-current.json"
    parent_hash = hashlib.sha256(current_path.read_bytes()).hexdigest()
    manifest = first.auction.export.partitions[0].manifest
    assert manifest is not None
    manifest_path = paths.lake_root / "auction" / (
        f"year={first_date.year:04d}/month={first_date.month:02d}/"
        f"trade_date={first_date.isoformat()}/manifest.json"
    )
    manifest_hash = _sha256(manifest_path)
    repair = ResearchAuctionRepairObservation(
        observation_id="research-auction-repair-20260718T080000Z-test",
        bootstrap_snapshot_id=_SNAPSHOT_ID,
        trade_date=first_date,
        generated_at=datetime(2026, 7, 18, 16, 0, tzinfo=_CST),
        code_commit=_COMMIT,
        plan_id="f" * 64,
        previous_observation_sha256=parent_hash,
        catalog_before_sha256=first.catalog_sha256,
        catalog_sha256=first.catalog_sha256,
        readonly_catalog_before_sha256=first.readonly_catalog_sha256,
        readonly_catalog_sha256=first.readonly_catalog_sha256,
        repairs=(
            ResearchAuctionRepairPartitionChange(
                trade_date=first_date,
                before_manifest_sha256=None,
                after_manifest_sha256=manifest_hash,
                before_content_hash=None,
                before_manifest=None,
                after_manifest=manifest,
            ),
        ),
    )
    repair_payload = (repair.model_dump_json(indent=2) + "\n").encode()
    repair_path = (
        paths.state_dir
        / "research_observations"
        / f"trade_date={first_date.isoformat()}"
        / f"{repair.observation_id}.json"
    )
    repair_path.write_bytes(repair_payload)
    current_path.write_bytes(repair_payload)

    repaired_status = inspect_research_authority(paths)

    assert repaired_status.status == "candidate"
    assert repaired_status.latest_trade_date == first_date
    assert repaired_status.stable_trading_days == 0
    assert repaired_status.eligible_for_promotion is False
    assert repaired_status.issues == ()

    forged = repair.model_copy(
        update={
            "observation_id": "research-auction-repair-forged-future-date",
            "trade_date": date(2026, 7, 17),
        }
    )
    forged_payload = (forged.model_dump_json(indent=2) + "\n").encode()
    forged_path = (
        paths.state_dir
        / "research_observations"
        / "trade_date=2026-07-17"
        / f"{forged.observation_id}.json"
    )
    forged_path.parent.mkdir(parents=True, exist_ok=True)
    forged_path.write_bytes(forged_payload)
    current_path.write_bytes(forged_payload)
    forged_status = inspect_research_authority(paths)
    assert forged_status.status == "invalid"
    assert "observation_lineage_broken" in forged_status.issues
    forged_path.unlink()
    current_path.write_bytes(repair_payload)

    next_date = date(2026, 7, 17)
    next_source = tmp_path / "next.duckdb"
    _seed_source(next_source, next_date)
    _write_watchlist(tmp_path, next_date)
    next_result = run_daily_research_ingest(
        source_database=next_source,
        paths=paths,
        trade_date=next_date,
        adapter=_Adapter(next_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )

    assert next_result.status == "candidate"
    assert next_result.previous_observation_sha256 == hashlib.sha256(
        repair_payload
    ).hexdigest()
    assert next_result.stable_trading_days == 1
    assert next_result.stability_parent_sha256 is None

    manifest_path.write_text("{}\n", encoding="utf-8")
    tampered = inspect_research_authority(paths)
    assert tampered.status == "invalid"
    assert "lake_manifest_invalid" in tampered.issues


def test_repair_partition_change_rejects_unbound_prior_evidence(
    tmp_path: Path,
) -> None:
    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    result = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    manifest = result.auction.export.partitions[0].manifest
    assert manifest is not None
    manifest_path = (
        paths.lake_root
        / "auction"
        / f"year={trade_date.year:04d}"
        / f"month={trade_date.month:02d}"
        / f"trade_date={trade_date.isoformat()}"
        / "manifest.json"
    )

    with pytest.raises(ValueError, match="content hash does not match"):
        ResearchAuctionRepairPartitionChange(
            trade_date=trade_date,
            before_manifest_sha256=_sha256(manifest_path),
            after_manifest_sha256=_sha256(manifest_path),
            before_content_hash="f" * 64,
            before_manifest=manifest,
            after_manifest=manifest,
        )


def test_minute_repair_observation_is_parsed_and_resets_authority_stability(
    tmp_path: Path,
) -> None:
    import rquant.research_ingest as ingest_module
    from rquant.research_ingest import (
        ResearchMinuteRepairObservation,
        ResearchMinuteRepairPartitionChange,
    )

    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    daily = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    current_path = paths.state_dir / "research-authority-current.json"
    parent_hash = hashlib.sha256(current_path.read_bytes()).hexdigest()
    minute_manifest = daily.minute.export.partitions[0].manifest
    assert minute_manifest is not None
    minute_manifest_path = (
        paths.lake_root
        / "minute"
        / "freq=1min"
        / f"year={trade_date.year:04d}"
        / f"month={trade_date.month:02d}"
        / f"trade_date={trade_date.isoformat()}"
        / "manifest.json"
    )
    repair = ResearchMinuteRepairObservation(
        observation_id="research-minute-repair-20260718T080000Z-test",
        bootstrap_snapshot_id=_SNAPSHOT_ID,
        trade_date=trade_date,
        generated_at=datetime(2026, 7, 18, 16, 0, tzinfo=_CST),
        code_commit=_COMMIT,
        manifest_id="e" * 64,
        plan_id="f" * 64,
        previous_observation_sha256=parent_hash,
        catalog_before_sha256=daily.catalog_sha256,
        catalog_sha256=daily.catalog_sha256,
        readonly_catalog_before_sha256=daily.readonly_catalog_sha256,
        readonly_catalog_sha256=daily.readonly_catalog_sha256,
        repairs=(
            ResearchMinuteRepairPartitionChange(
                trade_date=trade_date,
                before_manifest_sha256=None,
                after_manifest_sha256=_sha256(minute_manifest_path),
                before_content_hash=None,
                before_manifest=None,
                after_manifest=minute_manifest,
            ),
        ),
    )
    payload = (repair.model_dump_json(indent=2) + "\n").encode()
    observation_path = (
        paths.state_dir
        / "research_observations"
        / f"trade_date={trade_date.isoformat()}"
        / f"{repair.observation_id}.json"
    )
    observation_path.parent.mkdir(parents=True, exist_ok=True)
    observation_path.write_bytes(payload)
    current_path.write_bytes(payload)

    status = inspect_research_authority(paths)

    assert status.status == "candidate"
    assert status.latest_trade_date == trade_date
    assert status.stable_trading_days == 0
    assert status.eligible_for_promotion is False
    assert status.issues == ()

    next_date = date(2026, 7, 18)
    next_source = tmp_path / "next-source.duckdb"
    _seed_source(next_source, next_date)
    _write_watchlist(tmp_path, next_date)
    next_result = run_daily_research_ingest(
        source_database=next_source,
        paths=paths,
        trade_date=next_date,
        adapter=_Adapter(next_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 18, 16, 0, tzinfo=_CST),
    )

    assert next_result.previous_observation_sha256 == hashlib.sha256(
        payload
    ).hexdigest()
    assert next_result.stable_trading_days == 1
    assert next_result.stability_parent_sha256 is None

    outside_manifest = tmp_path / "outside-minute-manifest.json"
    outside_manifest.write_bytes(minute_manifest_path.read_bytes())
    minute_manifest_path.unlink()
    minute_manifest_path.symlink_to(outside_manifest)

    tampered = inspect_research_authority(paths)

    assert tampered.status == "invalid"
    assert "lake_manifest_invalid" in tampered.issues
    assert ingest_module._catalog_lake_integrity_issues(paths) == (
        "catalog_lake_manifest_invalid",
    )


def test_existing_partition_reader_can_disable_disk_spill(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.research_ingest as ingest_module
    from rquant.research_lake import ResearchPartitionKey

    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    original_connect = ingest_module.duckdb.connect
    configs: list[dict[str, str] | None] = []

    def connect_spy(*args, **kwargs):
        configs.append(kwargs.get("config"))
        return original_connect(*args, **kwargs)

    monkeypatch.setattr(ingest_module.duckdb, "connect", connect_spy)

    frame = ingest_module._query_existing_research_partition(
        paths,
        ResearchPartitionKey(
            dataset="minute_bar",
            trade_date=trade_date,
            freq="1min",
        ),
        ("ts_code", "trade_time"),
        memory_only=True,
    )

    assert not frame.empty
    assert configs == [{"temp_directory": ""}]


def test_same_date_auction_and_minute_repairs_keep_both_lake_bindings(
    tmp_path: Path,
) -> None:
    from rquant.research_ingest import (
        ResearchMinuteRepairObservation,
        ResearchMinuteRepairPartitionChange,
    )

    trade_date = date(2026, 7, 17)
    source = tmp_path / "source.duckdb"
    paths = _paths(tmp_path)
    _seed_source(source, trade_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, trade_date)
    daily = run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=trade_date,
        adapter=_Adapter(trade_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    current_path = paths.state_dir / "research-authority-current.json"
    daily_hash = hashlib.sha256(current_path.read_bytes()).hexdigest()
    auction_manifest = daily.auction.export.partitions[0].manifest
    minute_manifest = daily.minute.export.partitions[0].manifest
    assert auction_manifest is not None
    assert minute_manifest is not None
    auction_manifest_path = (
        paths.lake_root
        / "auction"
        / f"year={trade_date.year:04d}"
        / f"month={trade_date.month:02d}"
        / f"trade_date={trade_date.isoformat()}"
        / "manifest.json"
    )
    minute_manifest_path = (
        paths.lake_root
        / "minute"
        / "freq=1min"
        / f"year={trade_date.year:04d}"
        / f"month={trade_date.month:02d}"
        / f"trade_date={trade_date.isoformat()}"
        / "manifest.json"
    )
    auction_repair = ResearchAuctionRepairObservation(
        observation_id="research-auction-repair-same-date-test",
        bootstrap_snapshot_id=_SNAPSHOT_ID,
        trade_date=trade_date,
        generated_at=datetime(2026, 7, 18, 15, 30, tzinfo=_CST),
        code_commit=_COMMIT,
        plan_id="a" * 64,
        previous_observation_sha256=daily_hash,
        catalog_before_sha256=daily.catalog_sha256,
        catalog_sha256=daily.catalog_sha256,
        readonly_catalog_before_sha256=daily.readonly_catalog_sha256,
        readonly_catalog_sha256=daily.readonly_catalog_sha256,
        repairs=(
            ResearchAuctionRepairPartitionChange(
                trade_date=trade_date,
                before_manifest_sha256=None,
                after_manifest_sha256=_sha256(auction_manifest_path),
                before_content_hash=None,
                before_manifest=None,
                after_manifest=auction_manifest,
            ),
        ),
    )
    auction_payload = (
        auction_repair.model_dump_json(indent=2) + "\n"
    ).encode()
    auction_observation_path = (
        paths.state_dir
        / "research_observations"
        / f"trade_date={trade_date.isoformat()}"
        / f"{auction_repair.observation_id}.json"
    )
    auction_observation_path.write_bytes(auction_payload)
    current_path.write_bytes(auction_payload)

    minute_repair = ResearchMinuteRepairObservation(
        observation_id="research-minute-repair-same-date-test",
        bootstrap_snapshot_id=_SNAPSHOT_ID,
        trade_date=trade_date,
        generated_at=datetime(2026, 7, 18, 16, 0, tzinfo=_CST),
        code_commit=_COMMIT,
        manifest_id="b" * 64,
        plan_id="c" * 64,
        previous_observation_sha256=hashlib.sha256(auction_payload).hexdigest(),
        catalog_before_sha256=daily.catalog_sha256,
        catalog_sha256=daily.catalog_sha256,
        readonly_catalog_before_sha256=daily.readonly_catalog_sha256,
        readonly_catalog_sha256=daily.readonly_catalog_sha256,
        repairs=(
            ResearchMinuteRepairPartitionChange(
                trade_date=trade_date,
                before_manifest_sha256=None,
                after_manifest_sha256=_sha256(minute_manifest_path),
                before_content_hash=None,
                before_manifest=None,
                after_manifest=minute_manifest,
            ),
        ),
    )
    minute_payload = (
        minute_repair.model_dump_json(indent=2) + "\n"
    ).encode()
    minute_observation_path = (
        paths.state_dir
        / "research_observations"
        / f"trade_date={trade_date.isoformat()}"
        / f"{minute_repair.observation_id}.json"
    )
    minute_observation_path.write_bytes(minute_payload)
    current_path.write_bytes(minute_payload)

    assert inspect_research_authority(paths).issues == ()

    auction_manifest_path.write_text("{}\n", encoding="utf-8")
    tampered = inspect_research_authority(paths)

    assert tampered.status == "invalid"
    assert "lake_manifest_invalid" in tampered.issues
