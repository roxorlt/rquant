"""The notifier's explicit formula-pool authority enters its real page projection path."""

from __future__ import annotations

import os
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

import rquant.serving_page_projection_source as source_module
from rquant.formula_pool_serving_projection import (
    FORMULA_POOL_PROJECTION_TABLES,
    FormulaPoolServingConfig,
    read_formula_pool_serving_group,
)
from rquant.page_control import PageControlOutbox
from rquant.runtime_builder_signal import NotifierSettings, notifier_builder
from rquant.runtime_serving_authority import ServingSourceAuthorityReader
from rquant.runtime_serving_snapshot import SIGNALS_DATASET_ID
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_page_projection_source import PageProjectionSourceIntegrityError
from rquant.serving_publisher import ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.web.app import create_app
from rquant.web.settings import WebSettings
from tests.unit.test_formula_pool_daily_recalc import (
    DAY,
    DAY2,
    _publish_history_day2,
    _publish_market_day,
    _runner,
)
from tests.unit.test_formula_pool_save_core import _setup
from tests.unit.test_formula_pool_serving import _config, _saved
from tests.unit.test_runtime_builder_signal import (
    COMMIT,
    _notifier_manifest,
    _page_projection_replica,
    _seed_outbox,
)


def _formula_pool_paths(tmp_path: Path) -> dict[str, str]:
    return {
        name: str(tmp_path / name)
        for name in (
            "universe_root",
            "projection_root",
            "task_state_path",
            "artifact_root",
            "definition_root",
            "daily_root",
            "rule_root",
        )
    }


def _settings(tmp_path: Path, **overrides: object) -> dict[str, object]:
    settings = dict(_notifier_manifest(tmp_path).settings)
    settings.update(
        serving_authority_root=str(tmp_path / "authority"),
        page_projection_database_path=str(tmp_path / "replica.duckdb"),
        page_projection_page_control_outbox_path=str(tmp_path / "page-control.sqlite"),
        page_projection_formula_pool_config=_formula_pool_paths(tmp_path),
    )
    settings.update(overrides)
    return settings


def test_formula_pool_runtime_config_is_typed_and_accepts_sole_page_control_consumer(
    tmp_path: Path,
) -> None:
    settings = NotifierSettings.model_validate(_settings(tmp_path))

    assert isinstance(settings.page_projection_formula_pool_config, FormulaPoolServingConfig)
    assert settings.page_projection_formula_pool_config.definition_root == (
        tmp_path / "definition_root"
    )


@pytest.mark.parametrize(
    "missing",
    (
        "serving_authority_root",
        "page_projection_database_path",
        "page_projection_page_control_outbox_path",
    ),
)
def test_formula_pool_runtime_config_requires_all_three_read_authorities(
    tmp_path: Path, missing: str
) -> None:
    with pytest.raises(ValidationError):
        NotifierSettings.model_validate(_settings(tmp_path, **{missing: None}))


def test_formula_pool_runtime_config_rejects_incomplete_source_paths(tmp_path: Path) -> None:
    paths = _formula_pool_paths(tmp_path)
    paths.pop("daily_root")

    with pytest.raises(ValidationError):
        NotifierSettings.model_validate(
            _settings(tmp_path, page_projection_formula_pool_config=paths)
        )


def _notifier_step(
    tmp_path: Path,
    *,
    observed: datetime,
    config: FormulaPoolServingConfig | None,
    audit_path: Path | None,
    clock: Callable[[], datetime] | None = None,
) -> tuple[object, Path]:
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    _seed_outbox(runtime)
    replica = _page_projection_replica(runtime, synced_at=observed - timedelta(minutes=1))
    authority = runtime / "signals-authority"
    settings: dict[str, object] = {
        "serving_authority_root": str(authority),
        "page_projection_database_path": str(replica),
        "paused": True,
    }
    if config is not None:
        settings["page_projection_formula_pool_config"] = config.model_dump(mode="json")
    if audit_path is not None:
        settings["page_projection_page_control_outbox_path"] = str(audit_path)
    step = notifier_builder(clock=clock or (lambda: observed))(
        _notifier_manifest(runtime, **settings)
    )
    return step, authority


def _read_authority(authority: Path, observed: datetime) -> object:
    return ServingSourceAuthorityReader(
        root=authority,
        expected_producer_commit=COMMIT,
        expected_dataset_id=SIGNALS_DATASET_ID,
        expected_payload_kind="signal_delivery",
    )(observed)


def test_notifier_publishes_saved_daily_pool_for_web_in_one_serving_generation(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, definitions, version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    daily = _runner(admission, definitions, data_dir).run_one("research", version, DAY)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    step, authority = _notifier_step(
        tmp_path, observed=observed, config=config, audit_path=service.outbox.path
    )

    step()
    published = _read_authority(authority, observed)
    formula = {
        item.table_name: item
        for item in published.payload.projections
        if item.table_name in FORMULA_POOL_PROJECTION_TABLES
    }
    assert set(formula) == FORMULA_POOL_PROJECTION_TABLES
    assert formula["formula_pool_definition"].rows[0]["version"] == version
    assert formula["formula_pool_latest_result"].rows[0]["result_sha256"] == (daily.result_sha256)

    serving = tmp_path / "serving"
    bound = tuple(
        ServingProjectionInput.bind(
            item,
            owner_dataset_id=SIGNALS_DATASET_ID,
            owner_generation_id=published.generation_id,
        )
        for item in published.payload.projections
    )
    tables = build_serving_read_models(
        ServingReadModelInput(observed_at=observed, projections=bound)
    )
    ServingPublisher(serving, producer_commit=COMMIT, table_specs=SERVING_TABLE_SPECS).publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id=SIGNALS_DATASET_ID,
                generation_id=published.generation_id,
                event_time=observed,
                published_at=observed,
                sequence=published.sequence,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={SIGNALS_DATASET_ID: published.generation_id},
        built_at=observed,
    )
    group = read_formula_pool_serving_group(ServingReader(serving))
    assert group is not None and set(group) == FORMULA_POOL_PROJECTION_TABLES
    assert len({row.available_at for row in group.values()}) == 1
    with TestClient(
        create_app(
            WebSettings(serving_root=serving, formula_pool_daily_result_root=config.daily_root),
            clock=lambda: observed + timedelta(minutes=1),
            background=False,
        )
    ) as client:
        response = client.get("/api/v1/pools/formula", headers={"x-rquant-user": "researcher"})
        members = client.get(
            "/api/v1/pools/formula/research/members",
            headers={"x-rquant-user": "researcher"},
        )
    assert response.status_code == 200
    pools = response.json()["data"]["pools"]
    assert pools[0]["version"] == version
    assert pools[0]["latest_result"]["trade_date"] == DAY.isoformat()
    assert members.status_code == 200
    assert members.json()["data"]["match_codes"] == list(daily.match_codes)


def test_notifier_formula_pool_absent_and_explicitly_empty_stay_distinct(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, _definitions, data_dir = _setup(source)
    config = _config(admission, data_dir)
    config.definition_root.mkdir(mode=0o700)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    step, authority = _notifier_step(
        tmp_path, observed=observed, config=config, audit_path=service.outbox.path
    )
    step()
    configured = {
        item.table_name: item for item in _read_authority(authority, observed).payload.projections
    }
    assert configured["formula_pool_state"].rows[0]["availability"] == "empty"
    assert configured["formula_pool_definition"].rows == ()
    assert configured["formula_pool_latest_result"].rows == ()

    absent = tmp_path / "unconfigured"
    absent.mkdir()
    old_step, old_authority = _notifier_step(
        absent, observed=observed, config=None, audit_path=None
    )
    old_step()
    old_names = {
        item.table_name for item in _read_authority(old_authority, observed).payload.projections
    }
    assert FORMULA_POOL_PROJECTION_TABLES.isdisjoint(old_names)


def test_bad_configured_pool_source_keeps_old_authority_then_recovers(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, definitions, version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    _runner(admission, definitions, data_dir).run_one("research", version, DAY)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    clock = [observed]
    step, authority = _notifier_step(
        tmp_path,
        observed=observed,
        config=config,
        audit_path=service.outbox.path,
        clock=lambda: clock[0],
    )
    step()
    old = _read_authority(authority, observed)
    missing = data_dir / "hidden_definitions"
    config.definition_root.rename(missing)

    with pytest.raises(PageProjectionSourceIntegrityError, match="formula pool authority"):
        step()
    assert _read_authority(authority, observed).generation_id == old.generation_id

    missing.rename(config.definition_root)
    _publish_market_day(config.universe_root, DAY2)
    _publish_history_day2(config.projection_root)
    result = _runner(admission, definitions, data_dir).run_one("research", version, DAY2)
    clock[0] += timedelta(seconds=1)
    step()
    new = _read_authority(authority, clock[0])
    assert new.generation_id != old.generation_id
    latest = {item.table_name: item for item in new.payload.projections}[
        "formula_pool_latest_result"
    ].rows[0]
    assert latest["trade_date"] == DAY2.isoformat()
    assert latest["result_sha256"] == result.result_sha256


def test_unconfigured_notifier_retains_existing_replica_fallback(tmp_path: Path) -> None:
    observed = datetime.now(UTC) + timedelta(minutes=2)
    clock = [observed]
    step, authority = _notifier_step(
        tmp_path,
        observed=observed,
        config=None,
        audit_path=None,
        clock=lambda: clock[0],
    )
    step()
    old = _read_authority(authority, observed)
    (tmp_path / "runtime" / "rquant_ro.duckdb").unlink()
    clock[0] += timedelta(minutes=20)

    step()
    fallback = _read_authority(authority, clock[0])
    old_projections = {item.table_name: item for item in old.payload.projections}
    fallback_projections = {item.table_name: item for item in fallback.payload.projections}
    assert fallback.generation_id != old.generation_id
    assert fallback_projections["screen_bounds"].rows == old_projections["screen_bounds"].rows
    assert FORMULA_POOL_PROJECTION_TABLES.isdisjoint(fallback_projections)


def test_wrong_page_control_audit_does_not_advance_configured_authority(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, _definitions, _version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    step, authority = _notifier_step(
        tmp_path, observed=observed, config=config, audit_path=service.outbox.path
    )
    step()
    old = _read_authority(authority, observed)
    wrong_audit = PageControlOutbox(tmp_path / "wrong" / "page-control.sqlite")
    runtime = tmp_path / "runtime"
    bad_step = notifier_builder(clock=lambda: observed + timedelta(seconds=1))(
        _notifier_manifest(
            runtime,
            serving_authority_root=str(authority),
            page_projection_database_path=str(runtime / "rquant_ro.duckdb"),
            page_projection_page_control_outbox_path=str(wrong_audit.path),
            page_projection_formula_pool_config=config.model_dump(mode="json"),
            paused=True,
        )
    )

    with pytest.raises((PageProjectionSourceIntegrityError, OSError, ValueError)):
        bad_step()
    assert _read_authority(authority, observed + timedelta(seconds=1)).generation_id == (
        old.generation_id
    )


def test_unchanged_pool_does_not_republish_or_rescan_every_notifier_tick(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, _definitions, _version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    clock = [observed]
    scanned: list[datetime] = []
    original = source_module.read_formula_pool_projections

    def counted(*args: object, **kwargs: object) -> object:
        scanned.append(clock[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(source_module, "read_formula_pool_projections", counted)
    step, authority = _notifier_step(
        tmp_path,
        observed=observed,
        config=config,
        audit_path=service.outbox.path,
        clock=lambda: clock[0],
    )

    first = step()
    initial = _read_authority(authority, clock[0]).generation_id
    clock[0] += timedelta(seconds=2)
    second = step()

    assert first.projection_published is True
    assert second.projection_published is False
    assert _read_authority(authority, clock[0]).generation_id == initial
    assert scanned == [observed]


def test_new_daily_file_in_existing_pool_directory_invalidates_cache(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, definitions, version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    _runner(admission, definitions, data_dir).run_one("research", version, DAY)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    clock = [observed]
    step, authority = _notifier_step(
        tmp_path,
        observed=observed,
        config=config,
        audit_path=service.outbox.path,
        clock=lambda: clock[0],
    )
    step()
    old = _read_authority(authority, clock[0])
    root_mtime = config.daily_root.stat().st_mtime_ns
    _publish_market_day(config.universe_root, DAY2)
    _publish_history_day2(config.projection_root)
    latest = _runner(admission, definitions, data_dir).run_one("research", version, DAY2)
    assert config.daily_root.stat().st_mtime_ns == root_mtime

    clock[0] += timedelta(seconds=2)
    result = step()
    new = _read_authority(authority, clock[0])
    assert result.projection_published is True
    assert new.generation_id != old.generation_id
    rows = {item.table_name: item for item in new.payload.projections}
    assert rows["formula_pool_latest_result"].rows[0]["result_sha256"] == (latest.result_sha256)


def test_task_sqlite_wal_change_invalidates_cache_and_refuses_bad_task(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, definitions, version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    definition = definitions.read("research", expected_version=version)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    clock = [observed]
    step, authority = _notifier_step(
        tmp_path,
        observed=observed,
        config=config,
        audit_path=service.outbox.path,
        clock=lambda: clock[0],
    )
    with sqlite3.connect(config.task_state_path) as connection:
        assert connection.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
        connection.execute("PRAGMA wal_autocheckpoint=0")
        step()
        old = _read_authority(authority, clock[0]).generation_id
        main_before = config.task_state_path.stat()
        connection.execute(
            "UPDATE formula_market_job SET status='failed' WHERE task_id=?",
            (definition.creation.task_id,),
        )
        connection.commit()
        assert (main_before.st_size, main_before.st_mtime_ns) == (
            config.task_state_path.stat().st_size,
            config.task_state_path.stat().st_mtime_ns,
        )
        assert Path(f"{config.task_state_path}-wal").stat().st_size > 0

        clock[0] += timedelta(seconds=2)
        with pytest.raises(PageProjectionSourceIntegrityError, match="formula pool authority"):
            step()
        assert _read_authority(authority, clock[0]).generation_id == old


@pytest.mark.parametrize("target", ("definition", "daily"))
def test_in_place_pool_file_corruption_invalidates_cache_and_keeps_old_authority(
    tmp_path: Path, target: str
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, definitions, version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    _runner(admission, definitions, data_dir).run_one("research", version, DAY)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    clock = [observed]
    step, authority = _notifier_step(
        tmp_path,
        observed=observed,
        config=config,
        audit_path=service.outbox.path,
        clock=lambda: clock[0],
    )
    step()
    old = _read_authority(authority, clock[0]).generation_id
    file = (
        config.definition_root / "research.json"
        if target == "definition"
        else config.daily_root / "research" / f"{DAY.isoformat()}.json"
    )
    before = file.stat()
    payload = file.read_bytes()
    file.write_bytes(b"!" + payload[1:])
    os.utime(file, ns=(before.st_atime_ns, before.st_mtime_ns))
    after = file.stat()
    assert (after.st_size, after.st_mtime_ns) == (before.st_size, before.st_mtime_ns)
    assert after.st_ctime_ns != before.st_ctime_ns

    clock[0] += timedelta(seconds=2)
    with pytest.raises(PageProjectionSourceIntegrityError, match="formula pool authority"):
        step()
    assert _read_authority(authority, clock[0]).generation_id == old


@pytest.mark.parametrize("new_entry", ("definition", "daily"))
def test_new_catalog_entry_during_first_full_read_refuses_cache_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, new_entry: str
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    service, admission, definitions, version, data_dir = _saved(source)
    config = _config(admission, data_dir)
    _runner(admission, definitions, data_dir).run_one("research", version, DAY)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    original = source_module.read_formula_pool_projections

    def changed_after_read(*args: object, **kwargs: object) -> object:
        rows = original(*args, **kwargs)
        path = (
            config.definition_root / "late.json"
            if new_entry == "definition"
            else config.daily_root / "research" / f"{DAY2.isoformat()}.json"
        )
        path.write_bytes(b"{}")
        path.chmod(0o600)
        return rows

    monkeypatch.setattr(source_module, "read_formula_pool_projections", changed_after_read)
    step, authority = _notifier_step(
        tmp_path, observed=observed, config=config, audit_path=service.outbox.path
    )

    with pytest.raises(PageProjectionSourceIntegrityError, match="changed while binding"):
        step()
    assert not (authority / "current.json").exists()
