"""Daily formula pool batches never confuse admission with a sealed day result."""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

import rquant.formula_pool_batch as batch_module
from rquant.formula_market_private_config import FormulaMarketPrivateConfig
from rquant.formula_pool_batch import (
    FormulaPoolBatchCoordinator,
    FormulaPoolBatchItem,
    FormulaPoolBatchPrivateConfig,
    load_private_formula_pool_batch_config,
    main,
)
from rquant.formula_pool_definition import FormulaPoolDefinitionV1
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


def _expand_catalog(batch: FormulaPoolBatchCoordinator, total: int) -> None:
    source = batch.definitions.read("alpha")
    for number in range(total - 2):
        name = f"extra-{number:03d}"
        definition = FormulaPoolDefinitionV1.create(
            pool_name=f"user/{name}",
            display_name=name,
            formula=source.formula,
            created_at=source.created_at,
            creation=source.creation,
            command_id=f"create-{name}",
            command_hash=source.command_hash,
        )
        path = batch.config.definition_root / f"{name}.json"
        path.write_bytes(canonical_json_bytes(definition.model_dump(mode="json")))
        os.chmod(path, 0o600)


def test_day_sweep_reconciles_all_65_pools_without_claiming_waiting_as_complete(
    tmp_path: Path,
) -> None:
    batch, admission, _ = _fixture(tmp_path)
    _expand_catalog(batch, 65)
    first = batch.run_day(DAY)
    assert first.total_count == len(first.pools) == 65
    assert first.completed_count == first.failed_count == 0
    assert first.waiting_count == 65 and not first.all_complete
    assert [item.pool_name for item in first.pools] == sorted(
        item.pool_name for item in first.pools
    )
    assert len(first.catalog_identity) == len(first.universe_identity) == len(
        first.projection_identity
    ) == 64
    assert _worker(admission).status == "succeeded"
    again = batch.run_day(DAY)
    assert again.completed_count == 1 and again.waiting_count == 64
    assert not again.all_complete
    assert again.pools[0].daily is not None
    assert again.pools[0].daily.universe_identity == again.universe_identity


def test_day_sweep_512_pools_is_bounded_and_covers_every_page(tmp_path: Path) -> None:
    batch, _, _ = _fixture(tmp_path)
    _expand_catalog(batch, 512)
    seen: list[str] = []

    def waiting(definition: FormulaPoolDefinitionV1, _day: date) -> FormulaPoolBatchItem:
        seen.append(definition.pool_name)
        return FormulaPoolBatchItem(
            pool_name=definition.pool_name,
            definition_version=definition.version,
            status="waiting",
        )

    batch._process = waiting  # type: ignore[method-assign]
    result = batch.run_day(DAY)
    assert result.total_count == result.waiting_count == len(result.pools) == 512
    assert result.completed_count == result.failed_count == 0
    assert not result.all_complete
    assert seen == [item.pool_name for item in result.pools]


def test_day_sweep_empty_catalog_is_complete_only_with_trusted_sources(tmp_path: Path) -> None:
    batch, admission, _ = _fixture(tmp_path)
    for path in batch.config.definition_root.iterdir():
        path.unlink()
    result = batch.run_day(DAY)
    assert result.total_count == result.completed_count == result.waiting_count == 0
    assert result.pools == () and result.all_complete
    assert admission.store.latest().status == "succeeded"
    with pytest.raises(ValueError, match="closed"):
        batch.run_day(date(2026, 4, 18))


@pytest.mark.parametrize("change", ["catalog", "definition", "universe", "projection"])
def test_day_sweep_rejects_identity_shift_between_pages(
    tmp_path: Path, change: str
) -> None:
    batch, _, _ = _fixture(tmp_path)
    _expand_catalog(batch, 65)
    original = batch.run
    shifted = False

    def run_and_shift(
        trade_date: date, *, limit: int = 64, cursor: str | None = None
    ) -> object:
        nonlocal shifted
        page = original(trade_date, limit=limit, cursor=cursor)
        if page.next_cursor is not None and not shifted:
            shifted = True
            if change == "catalog":
                _expand_catalog(batch, 66)
            elif change == "definition":
                source = batch.definitions.read("alpha")
                revised = FormulaPoolDefinitionV1.create(
                    pool_name=source.pool_name,
                    display_name="alpha revised",
                    formula=source.formula,
                    created_at=source.created_at,
                    creation=source.creation,
                    command_id=source.command_id,
                    command_hash=source.command_hash,
                )
                path = batch.config.definition_root / "alpha.json"
                path.write_bytes(canonical_json_bytes(revised.model_dump(mode="json")))
                os.chmod(path, 0o600)
            elif change == "universe":
                _publish_market_day(batch.config.market.universe_root, DAY, minute=6)
            else:
                manifest_path = batch.config.market.projection_root / "current.json"
                manifest = json.loads(manifest_path.read_bytes())
                manifest["source_updated_at"] = "2026-04-16T09:11:00+00:00"
                manifest_path.write_bytes(
                    json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
                )
        return page

    batch.run = run_and_shift  # type: ignore[method-assign]
    with pytest.raises(ValueError, match="changed|cursor|stale"):
        batch.run_day(DAY)


@pytest.mark.parametrize("fail_page", [2, 3])
def test_day_sweep_raises_instead_of_returning_partial_pages(
    tmp_path: Path, fail_page: int
) -> None:
    batch, _, _ = _fixture(tmp_path)
    _expand_catalog(batch, 130)
    original = batch.run
    calls = 0

    def run_or_fail(trade_date: date, *, limit: int = 64, cursor: str | None = None) -> object:
        nonlocal calls
        calls += 1
        if calls == fail_page:
            raise RuntimeError("page failed")
        return original(trade_date, limit=limit, cursor=cursor)

    batch.run = run_or_fail  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="page failed"):
        batch.run_day(DAY)
    assert calls == fail_page


def test_day_sweep_worker_progress_failure_and_corrupt_daily(tmp_path: Path) -> None:
    batch, admission, data_dir = _fixture(tmp_path)
    first = batch.run_day(DAY)
    assert first.waiting_count == 2
    assert _worker(admission).status == "succeeded"
    second = batch.run_day(DAY)
    assert second.completed_count == second.waiting_count == 1
    assert _worker(admission).status == "succeeded"
    done = batch.run_day(DAY)
    assert done.completed_count == 2 and done.all_complete
    assert batch.run_day(DAY) == done
    daily_file = data_dir / "formula_pool_daily" / "alpha" / f"{DAY.isoformat()}.json"
    daily_file.write_text("{}")
    damaged = batch.run_day(DAY)
    assert damaged.failed_count == 1 and damaged.completed_count == 1
    assert not damaged.all_complete
    assert batch.run_day(DAY2).waiting_count == 2
    claimed = admission.store._claim()
    assert claimed is not None
    assert admission.store._finish_failure(claimed, "source_changed").status == "failed"
    failed_task = batch.run_day(DAY2)
    assert failed_task.failed_count == failed_task.waiting_count == 1
    assert failed_task.pools[0].error_code == "source_changed"
    assert not failed_task.all_complete


def test_day_sweep_cli_outputs_full_state_and_rejects_mixed_paging(
    tmp_path: Path, capsys: object
) -> None:
    batch, _, _ = _fixture(tmp_path)
    config_path = tmp_path / "batch.json"
    config_path.write_bytes(canonical_json_bytes(batch.config.model_dump(mode="json")))
    os.chmod(config_path, 0o600)
    args = ["--config", str(config_path), "--trade-date", DAY.isoformat(), "--all"]
    assert main(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["total_count"] == output["waiting_count"] == 2
    assert output["all_complete"] is False
    assert len(output["pools"]) == 2
    assert main([*args, "--limit", "1"]) == 2
    assert not capsys.readouterr().out
    assert main([*args, "--cursor", "invalid"]) == 2
    assert not capsys.readouterr().out
    assert (
        main(["--config", str(config_path), "--trade-date", "2026-04-18", "--all"])
        == 1
    )
    assert not capsys.readouterr().out


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
