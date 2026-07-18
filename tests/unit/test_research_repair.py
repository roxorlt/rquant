"""Governed historical auction repair tests."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
import pytest

import rquant.research_repair as repair_module
from rquant.research_ingest import (
    ResearchIngestPaths,
    inspect_research_authority,
    run_daily_research_ingest,
)
from rquant.research_lake import (
    ResearchPartitionKey,
    ResearchPartitionManifest,
    partition_manifest_relative_path,
    verify_research_partition,
)
from rquant.research_repair import (
    ResearchAuctionRepairDayPlan,
    ResearchAuctionRepairPlan,
    assess_tushare_auction_rows,
    build_auction_repair_day_plan,
    hash_auction_business_rows,
    hash_code_universe,
    merge_auction_partition,
    normalize_repair_dates,
    normalize_tushare_auction_rows,
    run_research_auction_repair,
)
from tests.unit.test_research_ingest import (
    _Adapter as _DailyAdapter,
)
from tests.unit.test_research_ingest import (
    _paths as _daily_paths,
)
from tests.unit.test_research_ingest import (
    _seed_bootstrap_candidate,
    _seed_source,
    _write_watchlist,
)

_CST = ZoneInfo("Asia/Shanghai")
_COMMIT = "a" * 40
_HASH_A = "a" * 64
_HASH_B = "b" * 64
_HASH_C = "c" * 64
_TRADE_DATE = date(2026, 7, 14)
_GENERATED_AT = datetime(2026, 7, 18, 15, 20, tzinfo=_CST)


def _auction_frame(
    codes: list[str],
    *,
    trade_date: date = _TRADE_DATE,
    source: str = "tushare",
    auction_type: str = "open_realtime",
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": code,
                "trade_date": trade_date,
                "auction_type": auction_type,
                "price": 10.0 + index / 100,
                "vol": 1_000.0 + index,
                "amount": 10_000.0 + index,
                "turnover_rate": 0.1,
                "volume_ratio": 1.5,
                "source": source,
            }
            for index, code in enumerate(codes)
        ]
    )


def _codes(count: int, *, prefix: str = "0") -> list[str]:
    return [f"{prefix}{index:05d}.SZ" for index in range(count)]


def _day_plan(trade_date: date) -> ResearchAuctionRepairDayPlan:
    return ResearchAuctionRepairDayPlan(
        trade_date=trade_date,
        expected_code_count=100,
        expected_codes_sha256=_HASH_A,
        existing_manifest_sha256=_HASH_B,
        fetched_business_sha256=_HASH_C,
        merged_business_sha256="d" * 64,
        existing_row_count=90,
        fetched_row_count=100,
        merged_row_count=100,
        observed_code_count=100,
        valid_code_count=100,
        expected_valid_code_count=100,
        expected_observed_code_count=100,
        unexpected_code_count=0,
        changed=True,
    )


def test_normalize_repair_dates_sorts_deduplicates_and_rejects_empty() -> None:
    assert normalize_repair_dates(
        [
            date(2026, 7, 14),
            date(2026, 4, 20),
            date(2026, 7, 14),
        ]
    ) == (date(2026, 4, 20), date(2026, 7, 14))

    with pytest.raises(ValueError, match="at least one"):
        normalize_repair_dates([])


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda frame: frame.drop(columns=["vol"]), "missing columns"),
        (
            lambda frame: frame.assign(trade_date=date(2026, 7, 15)),
            "outside target date",
        ),
        (lambda frame: frame.assign(source="minute_0930_fallback"), "source"),
        (lambda frame: frame.assign(auction_type="close"), "auction type"),
        (
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "duplicate physical key",
        ),
    ],
)
def test_normalize_tushare_auction_rows_rejects_structural_corruption(
    mutate: object,
    message: str,
) -> None:
    frame = _auction_frame(["000001.SZ", "600000.SH"])

    with pytest.raises(ValueError, match=message):
        normalize_tushare_auction_rows(
            mutate(frame),  # type: ignore[operator]
            trade_date=_TRADE_DATE,
            generated_at=_GENERATED_AT,
        )


def test_normalize_tushare_auction_rows_rejects_empty_and_na_keys() -> None:
    with pytest.raises(ValueError, match="empty"):
        normalize_tushare_auction_rows(
            pd.DataFrame(),
            trade_date=_TRADE_DATE,
            generated_at=_GENERATED_AT,
        )

    frame = _auction_frame(["000001.SZ"])
    frame.loc[0, "ts_code"] = None
    with pytest.raises(ValueError, match="null physical key"):
        normalize_tushare_auction_rows(
            frame,
            trade_date=_TRADE_DATE,
            generated_at=_GENERATED_AT,
        )


def test_normalized_rows_use_real_repair_time_and_stable_sorting() -> None:
    frame = _auction_frame(["600000.SH", "000001.SZ"])

    normalized = normalize_tushare_auction_rows(
        frame,
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    assert normalized["ts_code"].tolist() == ["000001.SZ", "600000.SH"]
    assert normalized["created_at"].tolist() == [
        _GENERATED_AT.replace(tzinfo=None),
        _GENERATED_AT.replace(tzinfo=None),
    ]


def test_tushare_quality_accepts_exact_98_percent_boundary() -> None:
    expected = set(_codes(100))
    observed = _codes(98) + _codes(2, prefix="9")
    normalized = normalize_tushare_auction_rows(
        _auction_frame(observed),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    audit = assess_tushare_auction_rows(normalized, expected_codes=expected)

    assert audit.passed is True
    assert audit.expected_code_count == 100
    assert audit.observed_code_count == 100
    assert audit.valid_code_count == 100
    assert audit.expected_valid_code_count == 98
    assert audit.expected_observed_code_count == 98
    assert audit.unexpected_code_count == 2
    assert audit.issues == ()


def test_tushare_quality_rejects_97_percent_and_nonpositive_values() -> None:
    expected = set(_codes(100))
    observed = _codes(97) + _codes(3, prefix="9")
    frame = _auction_frame(observed)
    frame.loc[0, "price"] = 0
    frame.loc[1, "vol"] = -1
    normalized = normalize_tushare_auction_rows(
        frame,
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    audit = assess_tushare_auction_rows(normalized, expected_codes=expected)

    assert audit.passed is False
    assert audit.valid_code_count == 98
    assert audit.expected_valid_code_count == 95
    assert set(audit.issues) == {
        "tushare_valid_coverage_below_98pct",
        "tushare_observed_precision_below_98pct",
    }


def test_tushare_quality_rejects_empty_expected_universe() -> None:
    normalized = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ"]),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    with pytest.raises(ValueError, match="expected universe"):
        assess_tushare_auction_rows(normalized, expected_codes=set())


def test_business_hash_excludes_created_at_but_binds_business_values() -> None:
    first = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ", "600000.SH"]),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )
    second = first.iloc[::-1].copy()
    second["created_at"] = datetime(2026, 7, 18, 16, 0)

    assert hash_auction_business_rows(first) == hash_auction_business_rows(second)

    second.loc[second["ts_code"] == "000001.SZ", "price"] = 10.5
    assert hash_auction_business_rows(first) != hash_auction_business_rows(second)


def test_universe_hash_is_order_independent_and_binds_membership() -> None:
    assert hash_code_universe({"600000.SH", "000001.SZ"}) == hash_code_universe(
        {"000001.SZ", "600000.SH"}
    )
    assert hash_code_universe({"000001.SZ"}) != hash_code_universe(
        {"000001.SZ", "600000.SH"}
    )


def test_plan_id_is_canonical_and_binds_authority_and_day_content() -> None:
    days = (_day_plan(date(2026, 4, 20)), _day_plan(date(2026, 7, 14)))
    first = ResearchAuctionRepairPlan(
        code_commit=_COMMIT,
        authority_current_sha256=_HASH_A,
        catalog_sha256=_HASH_B,
        readonly_catalog_sha256=_HASH_C,
        days=days,
    )
    second = ResearchAuctionRepairPlan.model_validate(first.model_dump())

    assert first.plan_id == second.plan_id
    assert len(first.plan_id) == 64
    assert first.trade_dates == (date(2026, 4, 20), date(2026, 7, 14))

    changed = ResearchAuctionRepairPlan(
        code_commit=_COMMIT,
        authority_current_sha256="f" * 64,
        catalog_sha256=_HASH_B,
        readonly_catalog_sha256=_HASH_C,
        days=days,
    )
    assert changed.plan_id != first.plan_id


def test_plan_rejects_unsorted_or_duplicate_days() -> None:
    with pytest.raises(ValueError, match="strictly ordered"):
        ResearchAuctionRepairPlan(
            code_commit=_COMMIT,
            authority_current_sha256=_HASH_A,
            catalog_sha256=_HASH_B,
            readonly_catalog_sha256=_HASH_C,
            days=(
                _day_plan(date(2026, 7, 14)),
                _day_plan(date(2026, 4, 20)),
            ),
        )

    with pytest.raises(ValueError, match="strictly ordered"):
        ResearchAuctionRepairPlan(
            code_commit=_COMMIT,
            authority_current_sha256=_HASH_A,
            catalog_sha256=_HASH_B,
            readonly_catalog_sha256=_HASH_C,
            days=(
                _day_plan(date(2026, 7, 14)),
                _day_plan(date(2026, 7, 14)),
            ),
        )


def test_merge_replaces_tushare_snapshot_and_preserves_fallback_created_at() -> None:
    old_time = datetime(2026, 7, 14, 9, 26)
    fallback_time = datetime(2026, 7, 14, 9, 31)
    existing = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ", "000002.SZ", "000003.SZ"]),
        trade_date=_TRADE_DATE,
        generated_at=old_time.replace(tzinfo=_CST),
    )
    fallback = existing.iloc[[0]].copy()
    fallback["ts_code"] = "000004.SZ"
    fallback["source"] = "minute_0930_fallback"
    fallback["created_at"] = fallback_time
    existing = pd.concat([existing, fallback], ignore_index=True)

    fetched_frame = _auction_frame(["000001.SZ", "000002.SZ", "000005.SZ"])
    fetched_frame.loc[fetched_frame["ts_code"] == "000002.SZ", "price"] = 88.0
    fetched = normalize_tushare_auction_rows(
        fetched_frame,
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    merged = merge_auction_partition(existing, fetched, trade_date=_TRADE_DATE)

    keyed = merged.set_index(["ts_code", "source"])
    assert keyed.loc[("000001.SZ", "tushare"), "created_at"] == old_time
    assert keyed.loc[("000002.SZ", "tushare"), "created_at"] == _GENERATED_AT.replace(
        tzinfo=None
    )
    assert keyed.loc[("000002.SZ", "tushare"), "price"] == 88.0
    assert ("000003.SZ", "tushare") not in keyed.index
    assert keyed.loc[("000004.SZ", "minute_0930_fallback"), "created_at"] == fallback_time
    assert keyed.loc[("000005.SZ", "tushare"), "created_at"] == _GENERATED_AT.replace(
        tzinfo=None
    )
    assert not merged.duplicated(
        ["ts_code", "trade_date", "auction_type", "source"]
    ).any()
    assert merged[["ts_code", "source"]].values.tolist() == sorted(
        merged[["ts_code", "source"]].values.tolist()
    )


def test_merge_rejects_existing_rows_from_another_date_or_duplicate_key() -> None:
    existing = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ"]),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )
    fetched = existing.copy()

    wrong_date = existing.copy()
    wrong_date["trade_date"] = date(2026, 7, 15)
    with pytest.raises(ValueError, match="outside target date"):
        merge_auction_partition(wrong_date, fetched, trade_date=_TRADE_DATE)

    duplicate = pd.concat([existing, existing], ignore_index=True)
    with pytest.raises(ValueError, match="duplicate physical key"):
        merge_auction_partition(duplicate, fetched, trade_date=_TRADE_DATE)


def test_build_day_plan_reports_unchanged_business_content() -> None:
    existing = normalize_tushare_auction_rows(
        _auction_frame(["000001.SZ", "600000.SH"]),
        trade_date=_TRADE_DATE,
        generated_at=datetime(2026, 7, 14, 9, 26, tzinfo=_CST),
    )
    fetched = normalize_tushare_auction_rows(
        existing.drop(columns=["created_at"]).iloc[::-1],
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    day, merged = build_auction_repair_day_plan(
        trade_date=_TRADE_DATE,
        expected_codes={"000001.SZ", "600000.SH"},
        existing_manifest_sha256=_HASH_A,
        existing=existing,
        fetched=fetched,
    )

    assert day.changed is False
    assert day.existing_row_count == 2
    assert day.fetched_row_count == 2
    assert day.merged_row_count == 2
    assert day.fetched_business_sha256 == day.merged_business_sha256
    assert hash_auction_business_rows(existing) == hash_auction_business_rows(merged)
    assert merged["created_at"].tolist() == existing["created_at"].tolist()


def test_build_day_plan_rejects_quality_failure() -> None:
    expected = set(_codes(100))
    fetched = normalize_tushare_auction_rows(
        _auction_frame(_codes(97) + _codes(3, prefix="9")),
        trade_date=_TRADE_DATE,
        generated_at=_GENERATED_AT,
    )

    with pytest.raises(ValueError, match="quality gate"):
        build_auction_repair_day_plan(
            trade_date=_TRADE_DATE,
            expected_codes=expected,
            existing_manifest_sha256=None,
            existing=pd.DataFrame(),
            fetched=fetched,
        )


class _RepairAdapter:
    def __init__(self, frames: dict[date, pd.DataFrame]) -> None:
        self.frames = frames
        self.calls: list[date] = []

    def stk_auction(self, trade_date: date) -> pd.DataFrame:
        self.calls.append(trade_date)
        return self.frames[trade_date].copy()


def _seed_current_authority(tmp_path: Path) -> ResearchIngestPaths:
    latest_date = date(2026, 7, 17)
    source = tmp_path / "latest-source.duckdb"
    paths = _daily_paths(tmp_path)
    _seed_source(source, latest_date)
    _seed_bootstrap_candidate(tmp_path)
    _write_watchlist(tmp_path, latest_date)
    run_daily_research_ingest(
        source_database=source,
        paths=paths,
        trade_date=latest_date,
        adapter=_DailyAdapter(latest_date),
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 17, 16, 0, tzinfo=_CST),
    )
    return paths


def _seed_repair_source(path: Path, trade_dates: tuple[date, ...]) -> set[str]:
    codes = set(_codes(100))
    calendar_dates = sorted(
        set(trade_dates)
        | {target.fromordinal(target.toordinal() - 1) for target in trade_dates}
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            CREATE TABLE trade_calendar (
                exchange VARCHAR NOT NULL,
                cal_date DATE NOT NULL,
                is_open BOOLEAN NOT NULL,
                PRIMARY KEY (exchange, cal_date)
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
            [(value,) for value in calendar_dates],
        )
        connection.executemany(
            """
            INSERT INTO daily_bar VALUES
                (?, ?, 10, 10.2, 9.9, 10.1, 10, 1, 1000, 10000, 'tushare')
            """,
            [
                (code, value)
                for value in calendar_dates
                for code in sorted(codes)
            ],
        )
    return codes


def _tree_fingerprint(root: Path) -> dict[str, str]:
    output: dict[str, str] = {}
    for path in sorted(root.glob("**/*")):
        if path.is_file() and not path.is_symlink():
            output[path.relative_to(root).as_posix()] = hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
    return output


def test_repair_preview_fetches_real_data_without_writing_files(tmp_path: Path) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 14)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})
    before = _tree_fingerprint(tmp_path)

    result = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )

    assert result.status == "planned"
    assert result.plan.trade_dates == (target,)
    assert result.plan_id == result.plan.plan_id
    assert result.observation is None
    assert adapter.calls == [target]
    assert _tree_fingerprint(tmp_path) == before


def test_repair_apply_rejects_stale_plan_before_any_publish(tmp_path: Path) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 14)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    frame = _auction_frame(sorted(codes), trade_date=target)
    adapter = _RepairAdapter({target: frame})
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )
    before = _tree_fingerprint(tmp_path)
    adapter.frames[target].loc[0, "price"] = 99.0

    with pytest.raises(ValueError, match="stale repair plan"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=lambda: _GENERATED_AT,
        )

    assert _tree_fingerprint(tmp_path) == before
    assert inspect_research_authority(paths).stable_trading_days == 1


def test_preview_rejects_target_after_current_authority_date(tmp_path: Path) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 18)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})

    with pytest.raises(ValueError, match="after current authority"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            now=lambda: datetime(2026, 7, 19, 8, 0, tzinfo=_CST),
        )


def test_apply_rechecks_market_window_immediately_before_publish(
    tmp_path: Path,
) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 17)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 20, 8, 0, tzinfo=_CST),
    )
    before = _tree_fingerprint(tmp_path)
    times = [
        datetime(2026, 7, 20, 9, 14, 59, tzinfo=_CST),
        datetime(2026, 7, 20, 9, 14, 59, tzinfo=_CST),
        datetime(2026, 7, 20, 9, 16, 0, tzinfo=_CST),
    ]

    def advancing_clock() -> datetime:
        return times.pop(0) if len(times) > 1 else times[0]

    with pytest.raises(ValueError, match="market protection window"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=advancing_clock,
        )

    assert _tree_fingerprint(tmp_path) == before
    assert not tuple(paths.transactions_root.glob("*"))


def test_apply_recovers_interrupted_publish_before_market_window_guard(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 17)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: datetime(2026, 7, 20, 8, 0, tzinfo=_CST),
    )
    events: list[str] = []

    def record_recovery(recovery_paths: ResearchIngestPaths) -> None:
        assert recovery_paths == paths
        events.append("recovered")

    monkeypatch.setattr(
        repair_module,
        "_recover_interrupted_publish",
        record_recovery,
    )

    with pytest.raises(ValueError, match="market protection window"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=lambda: datetime(2026, 7, 20, 9, 16, tzinfo=_CST),
        )

    assert events == ["recovered"]


def test_repair_apply_rolls_back_all_dates_after_mid_publish_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _seed_current_authority(tmp_path)
    targets = (date(2026, 7, 14), date(2026, 7, 15))
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, targets)
    adapter = _RepairAdapter(
        {
            target: _auction_frame(sorted(codes), trade_date=target)
            for target in targets
        }
    )
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=targets,
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )
    before = _tree_fingerprint(tmp_path)

    def fail_after_manifests(step: str) -> None:
        if step == "manifests_published":
            raise RuntimeError("injected publish failure")

    monkeypatch.setattr(repair_module, "_publish_step_hook", fail_after_manifests)

    with pytest.raises(RuntimeError, match="injected publish failure"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=targets,
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=lambda: _GENERATED_AT,
        )

    assert _tree_fingerprint(tmp_path) == before
    assert not tuple(paths.transactions_root.glob("*"))
    assert inspect_research_authority(paths).stable_trading_days == 1


def test_repair_rollback_preflights_late_cas_conflict_before_any_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 14)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )
    catalog_before = hashlib.sha256(paths.catalog_path.read_bytes()).hexdigest()
    current_path = paths.state_dir / "research-authority-current.json"

    def create_late_conflict(step: str) -> None:
        if step == "manifests_published":
            current_path.write_text("third-party-current\n", encoding="utf-8")
            raise RuntimeError("injected late CAS conflict")

    monkeypatch.setattr(repair_module, "_publish_step_hook", create_late_conflict)

    with pytest.raises(RuntimeError, match="rollback is pending"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=lambda: _GENERATED_AT,
        )

    assert hashlib.sha256(paths.catalog_path.read_bytes()).hexdigest() == catalog_before
    assert current_path.read_text(encoding="utf-8") == "third-party-current\n"
    manifest_path = paths.lake_root / partition_manifest_relative_path(
        ResearchPartitionKey(dataset="auction_bar", trade_date=target)
    )
    assert manifest_path.is_file()
    pending = tuple(paths.transactions_root.glob("*/auction-repair-journal.json"))
    assert len(pending) == 1


def test_repair_rollback_preflights_manifest_backup_symlink_before_restore(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 17)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )
    manifest_path = paths.lake_root / partition_manifest_relative_path(
        ResearchPartitionKey(dataset="auction_bar", trade_date=target)
    )
    current_path = paths.state_dir / "research-authority-current.json"
    published_hashes: dict[Path, str] = {}

    def replace_manifest_backup_with_symlink(step: str) -> None:
        if step != "readonly_published":
            return
        transaction_roots = tuple(paths.transactions_root.glob("auction-repair-*"))
        assert len(transaction_roots) == 1
        transaction_root = transaction_roots[0]
        backup = transaction_root / f"manifest-{target.isoformat()}.before"
        same_hash_target = transaction_root / "same-hash-manifest-backup.json"
        same_hash_target.write_bytes(backup.read_bytes())
        backup.unlink()
        backup.symlink_to(same_hash_target)
        for path in (
            paths.catalog_path,
            paths.readonly_catalog_path,
            current_path,
            manifest_path,
        ):
            published_hashes[path] = hashlib.sha256(path.read_bytes()).hexdigest()
        raise RuntimeError("injected manifest backup symlink")

    monkeypatch.setattr(
        repair_module,
        "_publish_step_hook",
        replace_manifest_backup_with_symlink,
    )

    with pytest.raises(RuntimeError, match="rollback is pending"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=lambda: _GENERATED_AT,
        )

    assert {
        path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in published_hashes
    } == published_hashes
    pending = tuple(paths.transactions_root.glob("*/auction-repair-journal.json"))
    assert len(pending) == 1


def test_repair_rollback_never_resolves_and_unlinks_version_symlink_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 14)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )
    captured: dict[str, Path] = {}

    def replace_version_with_symlink(step: str) -> None:
        if step != "manifests_published":
            return
        manifest_path = paths.lake_root / partition_manifest_relative_path(
            ResearchPartitionKey(dataset="auction_bar", trade_date=target)
        )
        manifest = ResearchPartitionManifest.model_validate_json(
            manifest_path.read_text(encoding="utf-8")
        )
        version_path = paths.lake_root / manifest.relative_path
        unrelated = paths.lake_root / "unrelated-same-hash.parquet"
        unrelated.write_bytes(version_path.read_bytes())
        version_path.unlink()
        version_path.symlink_to(unrelated)
        captured["version"] = version_path
        captured["unrelated"] = unrelated
        raise RuntimeError("injected version symlink conflict")

    monkeypatch.setattr(
        repair_module,
        "_publish_step_hook",
        replace_version_with_symlink,
    )

    with pytest.raises(RuntimeError, match="rollback is pending"):
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=lambda: _GENERATED_AT,
        )

    assert captured["version"].is_symlink()
    assert captured["unrelated"].is_file()


def test_repair_replaces_existing_head_but_preserves_old_content_version(
    tmp_path: Path,
) -> None:
    paths = _seed_current_authority(tmp_path)
    target = date(2026, 7, 17)
    key = ResearchPartitionKey(dataset="auction_bar", trade_date=target)
    manifest_path = paths.lake_root / partition_manifest_relative_path(key)
    before_manifest = ResearchPartitionManifest.model_validate_json(
        manifest_path.read_text(encoding="utf-8")
    )
    old_version = verify_research_partition(
        lake_root=paths.lake_root,
        manifest=before_manifest,
        as_of_time=datetime(2026, 7, 18, 8, 0, tzinfo=UTC),
    )
    old_version_hash = hashlib.sha256(old_version.read_bytes()).hexdigest()

    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (target,))
    adapter = _RepairAdapter({target: _auction_frame(sorted(codes), trade_date=target)})
    preview = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )
    applied = run_research_auction_repair(
        source_database=source,
        paths=paths,
        trade_dates=(target,),
        adapter=adapter,
        code_commit=_COMMIT,
        apply=True,
        plan_id=preview.plan_id,
        now=lambda: _GENERATED_AT,
    )

    assert applied.status == "candidate"
    assert applied.observation is not None
    change = applied.observation.repairs[0]
    assert change.before_manifest == before_manifest
    assert change.before_content_hash == before_manifest.content_hash
    assert change.after_manifest.parent_content_hash == before_manifest.content_hash
    assert change.after_manifest.file_hash != before_manifest.file_hash
    assert verify_research_partition(
        lake_root=paths.lake_root,
        manifest=before_manifest,
        as_of_time=datetime(2026, 7, 18, 8, 0, tzinfo=UTC),
    ) == old_version
    assert hashlib.sha256(old_version.read_bytes()).hexdigest() == old_version_hash
    status = inspect_research_authority(paths)
    assert status.status == "candidate"
    assert status.stable_trading_days == 0


def test_consecutive_repairs_keep_prior_repaired_partition_under_authority(
    tmp_path: Path,
) -> None:
    paths = _seed_current_authority(tmp_path)
    first_date = date(2026, 7, 14)
    second_date = date(2026, 7, 15)
    source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(source, (first_date, second_date))
    adapter = _RepairAdapter(
        {
            first_date: _auction_frame(sorted(codes), trade_date=first_date),
            second_date: _auction_frame(sorted(codes), trade_date=second_date),
        }
    )
    for target in (first_date, second_date):
        preview = run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            now=lambda: _GENERATED_AT,
        )
        run_research_auction_repair(
            source_database=source,
            paths=paths,
            trade_dates=(target,),
            adapter=adapter,
            code_commit=_COMMIT,
            apply=True,
            plan_id=preview.plan_id,
            now=lambda: _GENERATED_AT,
        )
    first_manifest_path = paths.lake_root / partition_manifest_relative_path(
        ResearchPartitionKey(dataset="auction_bar", trade_date=first_date)
    )
    first_manifest_path.write_text("{}\n", encoding="utf-8")

    status = inspect_research_authority(paths)

    assert status.status == "invalid"
    assert "lake_manifest_invalid" in status.issues


def test_repair_requires_ten_new_daily_candidates_before_promotion(
    tmp_path: Path,
) -> None:
    paths = _seed_current_authority(tmp_path)
    repair_date = date(2026, 7, 17)
    repair_source = tmp_path / "repair-source.duckdb"
    codes = _seed_repair_source(repair_source, (repair_date,))
    adapter = _RepairAdapter(
        {repair_date: _auction_frame(sorted(codes), trade_date=repair_date)}
    )
    preview = run_research_auction_repair(
        source_database=repair_source,
        paths=paths,
        trade_dates=(repair_date,),
        adapter=adapter,
        code_commit=_COMMIT,
        now=lambda: _GENERATED_AT,
    )
    run_research_auction_repair(
        source_database=repair_source,
        paths=paths,
        trade_dates=(repair_date,),
        adapter=adapter,
        code_commit=_COMMIT,
        apply=True,
        plan_id=preview.plan_id,
        now=lambda: _GENERATED_AT,
    )

    for offset in range(1, 11):
        trade_date = repair_date.fromordinal(repair_date.toordinal() + offset)
        source = tmp_path / f"daily-{trade_date.isoformat()}.duckdb"
        _seed_source(source, trade_date)
        _write_watchlist(tmp_path, trade_date)
        result = run_daily_research_ingest(
            source_database=source,
            paths=paths,
            trade_date=trade_date,
            adapter=_DailyAdapter(trade_date),
            code_commit=_COMMIT,
            now=lambda current=trade_date: datetime.combine(
                current,
                datetime.min.time(),
                tzinfo=_CST,
            ).replace(hour=16),
        )
        status = inspect_research_authority(paths)
        assert result.stable_trading_days == offset
        assert status.stable_trading_days == offset
        assert status.eligible_for_promotion is (offset == 10)
