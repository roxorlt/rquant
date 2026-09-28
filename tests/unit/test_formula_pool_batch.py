"""Daily formula pool batches never confuse admission with a sealed day result."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

import rquant.formula_pool_batch as batch_module
from rquant.formula_market_private_config import FormulaMarketPrivateConfig
from rquant.formula_pool_batch import (
    FormulaPoolBatchCoordinator,
    FormulaPoolBatchPrivateConfig,
    load_private_formula_pool_batch_config,
    main,
)
from rquant.page_control import PageControlStatus
from rquant.screen.formula_market_jobs import FormulaMarketJobWorker
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_market_admission import _command
from tests.unit.test_formula_pool_daily_recalc import (
    DAY,
    DAY2,
    NOW,
    _publish_history_day2,
    _publish_market_day,
)
from tests.unit.test_formula_pool_save_core import _save, _setup


def _fixture(tmp_path: Path) -> tuple[FormulaPoolBatchCoordinator, object, Path]:
    service, admission, definitions, data_dir = _setup(tmp_path)
    for name in ("alpha", "beta"):
        queued = service.submit(_command(f"create-{name}", formula="CLOSE>2"))
        assert queued.status is PageControlStatus.SUCCEEDED and queued.result is not None
        completed = FormulaMarketJobWorker(
            admission.store,
            trusted_source_roots=(admission.config.universe_root, admission.config.projection_root),
        ).run_one()
        assert completed is not None and completed.status == "succeeded"
        saved = service.submit(
            _save(queued.result["task_id"], command_id=f"save-{name}", base_name=name)
        )
        assert saved.status is PageControlStatus.SUCCEEDED
    _publish_market_day(admission.config.universe_root, DAY2)
    _publish_history_day2(admission.config.projection_root)
    config = FormulaPoolBatchPrivateConfig(
        market=admission.config,
        definition_root=definitions.definition_root,
        rule_pool_root=definitions.rule_pool_root,
        daily_result_root=data_dir / "formula_pool_daily",
    )
    config.rule_pool_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return FormulaPoolBatchCoordinator(config=config, clock=lambda: NOW), admission, data_dir


def _worker(admission: object) -> object:
    return FormulaMarketJobWorker(
        admission.store,
        trusted_source_roots=(admission.config.universe_root, admission.config.projection_root),
    ).run_one()


def test_two_pools_two_days_admit_wait_publish_and_retry(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    for day in (DAY, DAY2):
        first = batch.run(day, limit=2)
        assert first.trade_date == day
        assert [item.pool_name for item in first.pools] == ["user/alpha", "user/beta"]
        assert first.completed_count == 0
        assert first.waiting_count == 2
        assert first.failed_count == first.unprocessed_count == 0
        assert not first.all_complete
        assert _worker(admission).status == "succeeded"
        middle = batch.run(day, limit=2)
        assert middle.completed_count == 1
        assert middle.waiting_count == 1
        assert _worker(admission).status == "succeeded"
        done = batch.run(day, limit=2)
        assert done.all_complete
        assert done.completed_count == 2
        assert batch.run(day, limit=2) == done
        assert all(item.daily is not None and item.daily.trade_date == day for item in done.pools)
        assert all(
            item.daily is not None and "match_codes" not in item.daily.model_dump()
            for item in done.pools
        )
    version = batch.definitions.read("alpha").version
    assert batch.runner.read_exact("alpha", version, DAY).match_codes
    assert batch.runner.read_exact("alpha", version, DAY2).match_codes


def test_page_cursor_advances_past_waiting_and_cannot_claim_full_day(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    first = batch.run(DAY, limit=1)
    assert first.page_start == 0
    assert first.waiting_count == first.unprocessed_count == 1
    assert first.next_cursor is not None and not first.all_complete
    second = batch.run(DAY, limit=1, cursor=first.next_cursor)
    assert second.page_start == 1
    assert second.pools[0].pool_name == "user/beta"
    assert second.unprocessed_count == 1  # the previous page is outside this invocation
    assert second.next_cursor is None and not second.all_complete
    assert _worker(admission).status == "succeeded"
    with pytest.raises(ValueError, match="cursor"):
        batch.run(DAY2, limit=1, cursor=first.next_cursor)


def test_preflight_rejects_dirty_catalog_before_new_admission(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    dirty = batch.config.definition_root / ".alpha.stage"
    dirty.write_text("partial")
    os.chmod(dirty, 0o600)
    with pytest.raises(ValueError, match="definition|catalog"):
        batch.run(DAY)
    assert admission.store.latest().status == "succeeded"
    dirty.unlink()
    rule_root = batch.config.rule_pool_root
    rule_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    (rule_root / "alpha.json").write_text("collision")
    with pytest.raises((ValueError, FileExistsError), match="rule|collision"):
        batch.run(DAY)
    assert admission.store.latest().status == "succeeded"


def test_bad_catalog_and_bad_private_config_leave_new_task_paths_uncreated(
    tmp_path: Path,
) -> None:
    batch, _, _ = _fixture(tmp_path)
    state_path = tmp_path / "fresh-tasks" / "jobs.sqlite"
    artifact_root = tmp_path / "fresh-artifacts"
    config = FormulaPoolBatchPrivateConfig(
        market=FormulaMarketPrivateConfig(
            universe_root=batch.config.market.universe_root,
            projection_root=batch.config.market.projection_root,
            state_path=state_path,
            artifact_directory=artifact_root,
        ),
        definition_root=batch.config.definition_root,
        rule_pool_root=batch.config.rule_pool_root,
        daily_result_root=batch.config.daily_result_root,
    )
    dirty = config.definition_root / ".unfinished.stage"
    dirty.write_text("partial")
    with pytest.raises(ValueError, match="catalog"):
        FormulaPoolBatchCoordinator(config=config, clock=lambda: NOW)
    assert not state_path.parent.exists()
    assert not artifact_root.exists()
    dirty.unlink()

    config_path = tmp_path / "unsafe-batch.json"
    config_path.write_bytes(canonical_json_bytes(config.model_dump(mode="json")))
    os.chmod(config_path, 0o644)
    assert main(["--config", str(config_path), "--trade-date", DAY.isoformat()]) == 2
    assert not state_path.parent.exists()
    assert not artifact_root.exists()


class _GuardedScan:
    def __init__(self, scan: object, counts: list[int]) -> None:
        self.scan = scan
        self.counts = counts
        self.counts.append(0)

    def __enter__(self) -> _GuardedScan:
        return self

    def __exit__(self, *_args: object) -> None:
        self.scan.close()

    def __iter__(self) -> _GuardedScan:
        return self

    def __next__(self) -> object:
        entry = next(self.scan)
        self.counts[-1] += 1
        if self.counts[-1] > 513:
            raise AssertionError("catalog read beyond the 513th entry")
        return entry


def test_initial_catalog_scan_stops_on_entry_513(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch, _, _ = _fixture(tmp_path)
    for number in range(600):
        (batch.config.definition_root / f"extra-{number}.json").write_text("{}")
    original = os.scandir
    counts: list[int] = []
    monkeypatch.setattr(
        batch_module.os, "scandir", lambda directory: _GuardedScan(original(directory), counts)
    )
    with pytest.raises(ValueError, match="capacity"):
        batch._catalog()
    assert counts == [513]


def test_recheck_catalog_scan_stops_on_entry_513(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    batch, _, _ = _fixture(tmp_path)
    original = os.scandir
    counts: list[int] = []

    def scan(directory: int) -> _GuardedScan:
        if counts:
            for number in range(511):
                (batch.config.definition_root / f"late-{number}.json").write_text("{}")
        return _GuardedScan(original(directory), counts)

    monkeypatch.setattr(batch_module.os, "scandir", scan)
    with pytest.raises(ValueError, match="capacity"):
        batch._catalog()
    assert counts == [2, 513]


def test_missing_rule_pool_directory_is_not_silent_empty_catalog(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    batch.config.rule_pool_root.rmdir()
    with pytest.raises(FileNotFoundError):
        batch.run(DAY)
    assert admission.store.latest().status == "succeeded"


def test_preclose_and_capacity_reject_without_admission(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    batch = FormulaPoolBatchCoordinator(
        config=batch.config, clock=lambda: datetime(2026, 4, 16, 8, 30, tzinfo=UTC)
    )
    with pytest.raises(ValueError, match="closed"):
        batch.run(DAY2)
    assert admission.store.latest().status == "succeeded"
    batch = FormulaPoolBatchCoordinator(config=batch.config, clock=lambda: NOW)
    for number in range(511):
        (batch.config.definition_root / f"pollution-{number}.json").write_text("{}")
    with pytest.raises(ValueError, match="capacity"):
        batch.run(DAY)
    assert admission.store.latest().status == "succeeded"


def test_failed_pool_does_not_starve_next_pool(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    first = batch.run(DAY, limit=2)
    assert first.waiting_count == 2
    claimed = admission.store._claim()
    assert claimed is not None
    failed = admission.store._finish_failure(claimed, "source_changed")
    assert failed.status == "failed"
    retried = batch.run(DAY, limit=2)
    assert [(item.pool_name, item.status) for item in retried.pools] == [
        ("user/alpha", "failed"),
        ("user/beta", "waiting"),
    ]
    assert retried.pools[0].error_code == "source_changed"
    assert _worker(admission).status == "succeeded"
    final = batch.run(DAY, limit=2)
    assert final.failed_count == 1 and final.completed_count == 1
    assert not final.all_complete


def test_source_generation_change_rejects_old_daily_and_page_cursor(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    page = batch.run(DAY, limit=1)
    assert page.next_cursor is not None
    assert _worker(admission).status == "succeeded"
    batch.run(DAY, limit=1)
    _publish_market_day(batch.config.market.universe_root, DAY, minute=6)
    with pytest.raises(ValueError, match="cursor"):
        batch.run(DAY, limit=1, cursor=page.next_cursor)
    changed = batch.run(DAY, limit=2)
    assert changed.pools[0].status == "failed"
    assert changed.pools[0].error_code == "admission_rejected"
    assert not changed.all_complete


def test_source_generation_change_at_end_cannot_claim_all_complete(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    for _ in range(2):
        batch.run(DAY, limit=2)
        assert _worker(admission).status == "succeeded"
    original = batch._process
    seen = 0

    def process_then_replace_source(*args: object) -> object:
        nonlocal seen
        result = original(*args)
        seen += 1
        if seen == 2:
            _publish_market_day(batch.config.market.universe_root, DAY, minute=6)
        return result

    batch._process = process_then_replace_source  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="sources changed"):
        batch.run(DAY, limit=2)


def test_corrupt_sealed_result_or_daily_file_never_counts_completed(tmp_path: Path) -> None:
    batch, admission, data_dir = _fixture(tmp_path)
    batch.run(DAY, limit=1)
    receipt = _worker(admission)
    assert receipt.status == "succeeded"
    artifact = next(batch.config.market.artifact_directory.glob(f"*{receipt.task_id}*.json"))
    artifact.write_text("{}")
    rejected = batch.run(DAY, limit=1)
    assert rejected.failed_count == 1 and rejected.completed_count == 0
    assert not list((data_dir / "formula_pool_daily").rglob("*.json"))

    # A separate successful day result corrupted after publication is equally untrusted.
    other = batch.run(DAY2, limit=1)
    assert other.waiting_count == 1
    assert _worker(admission).status == "succeeded"
    published = batch.run(DAY2, limit=1)
    assert published.completed_count == 1
    daily_file = data_dir / "formula_pool_daily" / "alpha" / f"{DAY2.isoformat()}.json"
    daily_file.write_text("{}")
    corrupted = batch.run(DAY2, limit=1)
    assert corrupted.failed_count == 1 and corrupted.completed_count == 0


def test_private_config_and_entry_reject_missing_or_unsafe_input(
    tmp_path: Path, capsys: object
) -> None:
    batch, _, _ = _fixture(tmp_path)
    config_path = tmp_path / "batch.json"
    config_path.write_bytes(canonical_json_bytes(batch.config.model_dump(mode="json")))
    os.chmod(config_path, 0o600)
    assert load_private_formula_pool_batch_config(config_path) == batch.config
    assert (
        main(["--config", str(config_path), "--trade-date", DAY.isoformat(), "--limit", "1"]) == 0
    )
    output = json.loads(capsys.readouterr().out)
    assert output["trade_date"] == DAY.isoformat()
    os.chmod(config_path, 0o644)
    with pytest.raises(ValueError, match="private"):
        load_private_formula_pool_batch_config(config_path)
    assert main(["--config", str(config_path), "--trade-date", DAY.isoformat()]) == 2
