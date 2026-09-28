"""A saved formula is re-evaluated against each day's own complete sources."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import rquant.formula_pool_daily as daily_module
from rquant.formula_pool_daily import (
    FormulaPoolDailyRecalculator,
    FormulaPoolDailyResultStore,
)
from rquant.page_control import PageControlStatus
from rquant.runtime_contracts import canonical_sha256
from rquant.screen.formula_market_jobs import (
    FormulaMarketArtifactUnavailableError,
    FormulaMarketJobWorker,
)
from rquant.screen.formula_market_universe import (
    FormulaMarketPartition,
    FormulaMarketUniverseSnapshot,
    publish_formula_market_universe,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_market_admission import _command
from tests.unit.test_formula_market_run import DAY, ENTRIES, PARTITION_KEYS
from tests.unit.test_formula_pool_save_core import _save, _setup

SHANGHAI = ZoneInfo("Asia/Shanghai")
DAY2 = date(2026, 4, 16)
NOW = datetime(2026, 4, 17, 10, tzinfo=UTC)


def _created(tmp_path: Path, *, formula: str = "CLOSE>2") -> tuple[object, object, str, Path]:
    service, admission, definitions, data_dir = _setup(tmp_path)
    queued = service.submit(_command("create-daily-pool", formula=formula))
    assert queued.status is PageControlStatus.SUCCEEDED and queued.result is not None
    task_id = queued.result["task_id"]
    completed = FormulaMarketJobWorker(
        admission.store,
        trusted_source_roots=(admission.config.universe_root, admission.config.projection_root),
    ).run_one()
    assert completed is not None and completed.status == "succeeded"
    saved = service.submit(_save(task_id))
    assert saved.status is PageControlStatus.SUCCEEDED and saved.result is not None
    return admission, definitions, saved.result["version"], data_dir


def _runner(
    admission: object, definitions: object, data_dir: Path, *, now: datetime = NOW
) -> FormulaPoolDailyRecalculator:
    return FormulaPoolDailyRecalculator(
        config=admission.config,
        task_store=admission.store,
        definitions=definitions,
        result_root=data_dir / "formula_pool_daily",
        clock=lambda: now,
    )


def _finish(admission: object) -> object:
    receipt = FormulaMarketJobWorker(
        admission.store,
        trusted_source_roots=(admission.config.universe_root, admission.config.projection_root),
    ).run_one()
    assert receipt is not None
    return receipt


def _publish_market_day(root: Path, trade_date: date, *, minute: int = 5) -> str:
    partitions = tuple(
        FormulaMarketPartition(
            exchange=exchange,
            list_status=status,
            raw_rows=sum(
                item.exchange == exchange and item.list_status == status for item in ENTRIES
            ),
            included_rows=sum(
                item.exchange == exchange and item.list_status == status for item in ENTRIES
            ),
            excluded_b_shares=0,
            excluded_other=0,
        )
        for exchange, status in PARTITION_KEYS
    )
    snapshot = FormulaMarketUniverseSnapshot.create(
        trade_date=trade_date,
        started_at=datetime(
            trade_date.year, trade_date.month, trade_date.day, 17, 1, tzinfo=SHANGHAI
        ),
        completed_at=datetime(
            trade_date.year, trade_date.month, trade_date.day, 17, minute, tzinfo=SHANGHAI
        ),
        calendar_sha256="a" * 64,
        calendar_generated_at=datetime(2026, 4, 15, 8, tzinfo=UTC),
        partitions=partitions,
        entries=ENTRIES,
    )
    publish_formula_market_universe(root, snapshot)
    return snapshot.content_sha256


def _publish_history_day2(root: Path) -> str:
    first = root / ("a" * 32 + ".sqlite")
    second = root / ("b" * 32 + ".sqlite")
    with sqlite3.connect(first) as source, sqlite3.connect(second) as target:
        source.backup(target)
        target.execute("INSERT INTO calendar VALUES ('SSE', ?, 1)", (DAY2.isoformat(),))
        target.executemany(
            "INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, 100, 1000)",
            [
                (code, DAY2.isoformat(), close, close + 1, close - 1, close)
                for code, close in (("000001.SZ", 1.0), ("600001.SH", 4.0), ("830001.BJ", 1.0))
            ],
        )
        target.commit()
    os.chmod(second, 0o444)
    info = second.stat()
    manifest = {
        "schema_version": 1,
        "file_name": second.name,
        "file_device": info.st_dev,
        "file_inode": info.st_ino,
        "file_size": info.st_size,
        "file_mtime_ns": info.st_mtime_ns,
        "file_sha256": hashlib.sha256(second.read_bytes()).hexdigest(),
        "source_identity": "f" * 64,
        "source_updated_at": datetime(2026, 4, 16, 9, 10, tzinfo=UTC).isoformat(),
        "dates": [DAY.isoformat(), DAY2.isoformat()],
        "bar_count": 12,
    }
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    (root / "current.json").write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def _republish_history_with_day_coverage(
    root: Path, *, later_days: tuple[date, ...] = (), drop_target_bars: bool = False
) -> str:
    source_path = root / ("a" * 32 + ".sqlite")
    new_path = root / ("c" * 32 + ".sqlite")
    with sqlite3.connect(source_path) as source, sqlite3.connect(new_path) as target:
        source.backup(target)
        if drop_target_bars:
            target.execute("DELETE FROM bars WHERE trade_date=?", (DAY.isoformat(),))
        for day in later_days:
            target.execute("INSERT INTO calendar VALUES ('SSE', ?, 1)", (day.isoformat(),))
            target.execute(
                "INSERT INTO bars VALUES ('000001.SZ', ?, 1, 2, 0, 1, 100, 1000)",
                (day.isoformat(),),
            )
        count = target.execute("SELECT COUNT(*) FROM bars").fetchone()[0]
        target.commit()
    os.chmod(new_path, 0o444)
    info = new_path.stat()
    manifest = json.loads((root / "current.json").read_bytes())
    manifest.update(
        file_name=new_path.name,
        file_device=info.st_dev,
        file_inode=info.st_ino,
        file_size=info.st_size,
        file_mtime_ns=info.st_mtime_ns,
        file_sha256=hashlib.sha256(new_path.read_bytes()).hexdigest(),
        source_updated_at=datetime(2026, 6, 1, 9, tzinfo=UTC).isoformat()
        if later_days
        else manifest["source_updated_at"],
        dates=[day.isoformat() for day in reversed(later_days)],
        bar_count=count,
    )
    payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    (root / "current.json").write_bytes(payload)
    return hashlib.sha256(payload).hexdigest()


def test_two_closed_days_have_independent_members_and_source_evidence(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    first_task = runner.admit("research", version, DAY)
    assert first_task.status == "queued"
    assert _finish(admission).status == "succeeded"
    first = runner.publish("research", version, DAY)
    assert first.match_codes == ("000001.SZ", "830001.BJ")
    assert (first.match_count, first.no_match_count, first.unknown_count) == (2, 1, 1)
    assert first.unknown_reasons == {"missing_date": 1}

    second_universe = _publish_market_day(admission.config.universe_root, DAY2)
    second_projection = _publish_history_day2(admission.config.projection_root)
    second_task = runner.admit("research", version, DAY2)
    assert second_task.task_id != first_task.task_id
    assert _finish(admission).status == "succeeded"
    second = runner.publish("research", version, DAY2)
    assert second.match_codes == ("600001.SH",)
    assert (second.match_count, second.no_match_count, second.unknown_count) == (1, 2, 1)
    assert (second.universe_identity, second.projection_identity) == (
        second_universe,
        second_projection,
    )
    assert first.universe_identity != second.universe_identity
    assert first.projection_identity != second.projection_identity
    assert first.member_sha256 != second.member_sha256
    assert runner.read_exact("research", version, DAY) == first
    assert runner.read_exact("research", version, DAY2) == second


def test_missing_sources_and_preclose_date_do_not_admit(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    with pytest.raises(ValueError):
        runner.admit("research", version, DAY2)
    _publish_market_day(admission.config.universe_root, DAY2)
    with pytest.raises(ValueError):
        runner.admit("research", version, DAY2)
    _publish_history_day2(admission.config.projection_root)
    early = _runner(
        admission,
        definitions,
        data_dir,
        now=datetime(2026, 4, 16, 16, 30, tzinfo=SHANGHAI),
    )
    with pytest.raises(ValueError, match="收盘|closed"):
        early.admit("research", version, DAY2)
    assert admission.store.latest().status == "succeeded"


def test_historical_day_outside_recent_catalog_still_has_verified_bars(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    later: list[date] = []
    cursor = DAY
    while len(later) < 30:
        cursor += timedelta(days=1)
        if cursor.weekday() < 5:
            later.append(cursor)
    identity = _republish_history_with_day_coverage(
        admission.config.projection_root, later_days=tuple(later)
    )
    runner = _runner(admission, definitions, data_dir, now=datetime(2026, 6, 2, 10, tzinfo=UTC))
    daily = runner.run_one("research", version, DAY)
    assert daily.trade_date == DAY
    assert daily.projection_identity == identity
    assert daily.match_codes == ("000001.SZ", "830001.BJ")


def test_target_day_calendar_without_any_bars_refuses_admission(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    _republish_history_with_day_coverage(admission.config.projection_root, drop_target_bars=True)
    runner = _runner(admission, definitions, data_dir)
    with pytest.raises(ValueError, match="sources|history"):
        runner.admit("research", version, DAY)
    assert not list((data_dir / "formula_pool_daily").rglob("*.json"))


def test_same_identity_recovers_task_then_daily_result_and_conflicts_after_source_change(
    tmp_path: Path,
) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    queued = runner.admit("research", version, DAY)
    later = _runner(admission, definitions, data_dir, now=NOW.replace(hour=11))
    assert later.admit("research", version, DAY) == queued
    assert _finish(admission).status == "succeeded"
    first = runner.publish("research", version, DAY)
    assert later.publish("research", version, DAY) == first
    assert len(list((data_dir / "formula_pool_daily").rglob("*.json"))) == 1

    _publish_market_day(admission.config.universe_root, DAY, minute=6)
    with pytest.raises(ValueError, match="conflict"):
        runner.admit("research", version, DAY)
    assert runner.read_exact("research", version, DAY) == first
    with pytest.raises(ValueError, match="version"):
        runner.read_exact("research", "0" * 64, DAY)


def test_source_generation_change_or_bad_task_cannot_publish(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    task = runner.admit("research", version, DAY)
    _publish_market_day(admission.config.universe_root, DAY, minute=6)
    assert _finish(admission).status == "failed"
    with pytest.raises(ValueError):
        runner.publish("research", version, DAY)
    assert task.result_sha256 is None
    assert not list((data_dir / "formula_pool_daily").rglob("*.json"))


def test_queued_task_and_post_success_source_change_cannot_publish(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    runner.admit("research", version, DAY)
    with pytest.raises(ValueError, match="succeeded"):
        runner.publish("research", version, DAY)
    assert _finish(admission).status == "succeeded"
    _publish_market_day(admission.config.universe_root, DAY, minute=6)
    with pytest.raises(ValueError):
        runner.publish("research", version, DAY)
    assert not list((data_dir / "formula_pool_daily").rglob("*.json"))


def test_sealed_result_damage_refuses_publication_and_exact_read(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    runner.admit("research", version, DAY)
    finished = _finish(admission)
    assert finished.status == "succeeded"
    artifact = admission.config.artifact_directory / admission.store._artifact_name(
        finished.task_id, finished.result_sha256
    )
    artifact.write_bytes(b"{}")
    with pytest.raises(FormulaMarketArtifactUnavailableError):
        runner.publish("research", version, DAY)
    assert not list((data_dir / "formula_pool_daily").rglob("*.json"))


def test_exact_read_detects_damaged_daily_and_sealed_result(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    runner.admit("research", version, DAY)
    finished = _finish(admission)
    daily = runner.publish("research", version, DAY)
    daily_file = data_dir / "formula_pool_daily" / "research" / f"{DAY.isoformat()}.json"
    daily_file.write_bytes(b"{}")
    with pytest.raises(ValueError):
        runner.read_exact("research", version, DAY)
    content = daily.model_dump(mode="python", exclude={"content_sha256"})
    content["match_codes"] = ("000001.SZ", "600001.SH")
    content["member_sha256"] = canonical_sha256(content["match_codes"])
    forged = content | {"content_sha256": canonical_sha256(content)}
    daily_file.write_bytes(
        canonical_json_bytes(daily.model_copy(update=forged).model_dump(mode="json"))
    )
    with pytest.raises(ValueError, match="sealed task"):
        runner.read_exact("research", version, DAY)
    daily_file.write_bytes(
        json.dumps(daily.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    )
    artifact = admission.config.artifact_directory / admission.store._artifact_name(
        finished.task_id, finished.result_sha256
    )
    artifact.write_bytes(b"{}")
    with pytest.raises(FormulaMarketArtifactUnavailableError):
        runner.read_exact("research", version, DAY)


def test_daily_store_rejects_unsafe_pool_name_even_for_direct_caller(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    runner.admit("research", version, DAY)
    assert _finish(admission).status == "succeeded"
    daily = runner.publish("research", version, DAY)
    content = daily.model_dump(mode="python", exclude={"content_sha256"})
    content["pool_name"] = "user/bad.name"
    content["run_identity"] = canonical_sha256(
        {
            "contract": "formula-pool-daily/v1",
            "pool_name": content["pool_name"],
            "definition_version": version,
            "trade_date": DAY,
            "universe_identity": daily.universe_identity,
            "projection_identity": daily.projection_identity,
        }
    )
    unsafe = daily.model_copy(update=content | {"content_sha256": canonical_sha256(content)})
    with pytest.raises(ValueError):
        FormulaPoolDailyResultStore(data_dir / "formula_pool_daily").create(unsafe)
    assert not (data_dir / "formula_pool_daily" / "bad.name").exists()


def test_unsupported_formula_syntax_and_wrong_definition_version_refuse_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    with pytest.raises(ValueError, match="version"):
        runner.admit("research", "0" * 64, DAY)
    monkeypatch.setattr(daily_module, "SYNTAX_VERSION", "tdx-v2")
    with pytest.raises(ValueError, match="syntax"):
        runner.admit("research", version, DAY)
    assert not list((data_dir / "formula_pool_daily").rglob("*.json"))


def test_verified_zero_matches_keeps_unknowns_distinct(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path, formula="CLOSE>99")
    runner = _runner(admission, definitions, data_dir)
    runner.admit("research", version, DAY)
    assert _finish(admission).status == "succeeded"
    result = runner.publish("research", version, DAY)
    assert (result.match_count, result.no_match_count, result.unknown_count) == (0, 3, 1)
    assert result.match_codes == ()
    assert result.unknown_reasons == {"missing_date": 1}
    assert runner.read_exact("research", version, DAY) == result


def test_trusted_one_shot_entry_runs_only_one_pool_day(tmp_path: Path) -> None:
    admission, definitions, version, data_dir = _created(tmp_path)
    runner = _runner(admission, definitions, data_dir)
    daily = runner.run_one("research", version, DAY)
    assert daily.match_codes == ("000001.SZ", "830001.BJ")
    assert runner.run_one("research", version, DAY) == daily
    assert len(list((data_dir / "formula_pool_daily").rglob("*.json"))) == 1
