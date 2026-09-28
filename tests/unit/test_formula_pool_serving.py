"""Saved formula pools enter Serving only with audited, sealed daily evidence."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import rquant.formula_pool_serving_projection as projection_module
from rquant.formula_pool_serving_projection import (
    FORMULA_POOL_PROJECTION_TABLES,
    FormulaPoolServingConfig,
    read_formula_pool_indexed_result,
    read_formula_pool_serving_group,
    validate_formula_pool_projections,
)
from rquant.page_control import PageControlStatus
from rquant.screen.formula_market_jobs import FormulaMarketJobWorker
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    PageProjectionSourceIntegrityError,
)
from rquant.serving_publisher import ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_formula_market_admission import _command
from tests.unit.test_formula_pool_daily_recalc import (
    DAY,
    DAY2,
    _publish_history_day2,
    _publish_market_day,
    _runner,
)
from tests.unit.test_formula_pool_save_core import _queued, _save, _setup
from tests.unit.test_serving_page_projection_source import _signal_projection_database


def _saved(tmp_path: Path, *, formula: str = "CLOSE>2") -> tuple[object, object, object, str, Path]:
    service, admission, definitions, data_dir = _setup(tmp_path)
    if formula == "CLOSE>2":
        task_id = _queued(service)
    else:
        queued = service.submit(_command("formula-pool-run", formula=formula))
        assert queued.status is PageControlStatus.SUCCEEDED and queued.result is not None
        task_id = queued.result["task_id"]
    assert FormulaMarketJobWorker(admission.store).run_one().status == "succeeded"
    saved = service.submit(_save(task_id))
    assert saved.status is PageControlStatus.SUCCEEDED and saved.result is not None
    return service, admission, definitions, saved.result["version"], data_dir


def _config(admission: object, data_dir: Path) -> FormulaPoolServingConfig:
    daily_root = data_dir / "formula_pool_daily"
    daily_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return FormulaPoolServingConfig(
        universe_root=admission.config.universe_root,
        projection_root=admission.config.projection_root,
        task_state_path=admission.config.state_path,
        artifact_root=admission.config.artifact_directory,
        definition_root=data_dir / "formula_pools",
        daily_root=daily_root,
        rule_root=data_dir / "user_presets",
    )


def _source(tmp_path: Path, *, outbox: object | None, config: object | None) -> object:
    database = tmp_path / "research_ro.duckdb"
    if not database.exists():
        _signal_projection_database(database)
    return DuckDBSignalPageProjectionSource(
        database,
        page_control_outbox=outbox,
        formula_pool_config=config,
    )


def _group(source: object) -> dict[str, object]:
    observed = datetime.now(UTC) + timedelta(minutes=2)
    return {
        item.table_name: item
        for item in source(observed).projections
        if item.table_name in FORMULA_POOL_PROJECTION_TABLES
    }


def _publish_tables(root: Path, tables: object, *, generation: str, observed: datetime) -> None:
    ServingPublisher(root, producer_commit="a" * 40, table_specs=SERVING_TABLE_SPECS).publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="signals",
                generation_id=generation,
                event_time=observed,
                published_at=observed,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"signals": generation},
        built_at=observed,
    )


def test_opt_in_empty_then_two_day_latest_result_enters_one_serving_generation(
    tmp_path: Path,
) -> None:
    assert _group(_source(tmp_path, outbox=None, config=None)) == {}
    service, admission, definitions, version, data_dir = _saved(tmp_path)
    config = _config(admission, data_dir)
    source = _source(tmp_path, outbox=service.outbox, config=config)
    unrun = _group(source)
    assert set(unrun) == FORMULA_POOL_PROJECTION_TABLES
    assert unrun["formula_pool_state"].rows[0]["availability"] == "ready"
    assert unrun["formula_pool_state"].rows[0]["pool_count"] == 1
    assert unrun["formula_pool_state"].rows[0]["run_count"] == 0
    assert unrun["formula_pool_latest_result"].rows == ()

    runner = _runner(admission, definitions, data_dir)
    first = runner.run_one("research", version, DAY)
    assert _group(source)["formula_pool_latest_result"].rows[0]["trade_date"] == DAY.isoformat()
    _publish_market_day(admission.config.universe_root, DAY2)
    _publish_history_day2(admission.config.projection_root)
    second = runner.run_one("research", version, DAY2)
    group = _group(source)
    assert len({item.available_at for item in group.values()}) == 1
    definition = group["formula_pool_definition"].rows[0]
    latest = group["formula_pool_latest_result"].rows[0]
    assert definition["pool_name"] == "user/research"
    assert definition["version"] == version
    assert definition["formula"] == "CLOSE>2"
    assert latest["trade_date"] == DAY2.isoformat()
    assert latest["result_sha256"] == second.result_sha256
    assert latest["member_sha256"] == second.member_sha256
    assert latest["unknown_count"] == 1
    assert latest["match_count"] == 1
    assert "match_codes" not in latest
    assert first.result_sha256 != second.result_sha256

    observed = datetime.now(UTC) + timedelta(minutes=2)
    bound = tuple(
        ServingProjectionInput.bind(item, owner_dataset_id="signals", owner_generation_id="a" * 64)
        for item in group.values()
    )
    tables = build_serving_read_models(
        ServingReadModelInput(observed_at=observed, projections=bound)
    )
    _publish_tables(tmp_path / "serving", tables, generation="a" * 64, observed=observed)
    with ServingReader(tmp_path / "serving").open_current_readonly() as connection:
        cursor = connection.execute("SELECT * FROM formula_pool_latest_result")
        columns = tuple(column[0] for column in cursor.description)
        stored = dict(zip(columns, cursor.fetchone(), strict=True))
        assert stored["trade_date"] == DAY2
    read_group = read_formula_pool_serving_group(ServingReader(tmp_path / "serving"))
    assert read_group is not None
    assert read_group["formula_pool_latest_result"].rows[0]["member_sha256"] == (
        second.member_sha256
    )
    assert read_formula_pool_indexed_result(config, stored) == second


def test_configured_empty_definition_catalog_is_explicitly_empty(tmp_path: Path) -> None:
    service, admission, _definitions, data_dir = _setup(tmp_path)
    (data_dir / "formula_pools").mkdir(mode=0o700, parents=True)
    group = _group(_source(tmp_path, outbox=service.outbox, config=_config(admission, data_dir)))
    assert group["formula_pool_state"].rows[0]["availability"] == "empty"
    assert group["formula_pool_definition"].rows == ()
    assert group["formula_pool_latest_result"].rows == ()


def test_serving_reader_distinguishes_absent_empty_and_partial_group(tmp_path: Path) -> None:
    observed = datetime.now(UTC) + timedelta(minutes=2)
    absent_tables = build_serving_read_models(ServingReadModelInput(observed_at=observed))
    _publish_tables(tmp_path / "absent", absent_tables, generation="a" * 64, observed=observed)
    assert read_formula_pool_serving_group(ServingReader(tmp_path / "absent")) is None

    (tmp_path / "source").mkdir()
    service, admission, _definitions, data_dir = _setup(tmp_path / "source")
    (data_dir / "formula_pools").mkdir(mode=0o700, parents=True)
    group = _group(
        _source(tmp_path / "source", outbox=service.outbox, config=_config(admission, data_dir))
    )
    bound = tuple(
        ServingProjectionInput.bind(item, owner_dataset_id="signals", owner_generation_id="b" * 64)
        for item in group.values()
    )
    empty_tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=datetime.now(UTC) + timedelta(minutes=2), projections=bound
        )
    )
    _publish_tables(
        tmp_path / "empty",
        empty_tables,
        generation="b" * 64,
        observed=datetime.now(UTC) + timedelta(minutes=2),
    )
    read_group = read_formula_pool_serving_group(ServingReader(tmp_path / "empty"))
    assert read_group is not None
    assert read_group["formula_pool_state"].rows[0]["availability"] == "empty"

    partial = {name: frame.copy() for name, frame in empty_tables.items()}
    status = partial["projection_status"]
    status.loc[status.table_name == "formula_pool_latest_result", "available"] = False
    _publish_tables(
        tmp_path / "partial",
        partial,
        generation="b" * 64,
        observed=datetime.now(UTC) + timedelta(minutes=2),
    )
    with pytest.raises(ValueError, match="partially available"):
        read_formula_pool_serving_group(ServingReader(tmp_path / "partial"))

    mixed = {name: frame.copy() for name, frame in empty_tables.items()}
    mixed_status = mixed["projection_status"]
    mixed_status.loc[
        mixed_status.table_name == "formula_pool_definition", "owner_generation_id"
    ] = "c" * 64
    _publish_tables(
        tmp_path / "mixed",
        mixed,
        generation="b" * 64,
        observed=datetime.now(UTC) + timedelta(minutes=2),
    )
    with pytest.raises(ValueError, match="generation"):
        read_formula_pool_serving_group(ServingReader(tmp_path / "mixed"))


def test_missing_definition_with_successful_audit_refuses_empty(tmp_path: Path) -> None:
    service, admission, _definitions, _version, data_dir = _saved(tmp_path)
    (data_dir / "formula_pools" / "research.json").unlink()
    with pytest.raises(PageProjectionSourceIntegrityError):
        _group(_source(tmp_path, outbox=service.outbox, config=_config(admission, data_dir)))


def test_zero_match_is_distinct_from_unrun_and_unknown(tmp_path: Path) -> None:
    service, admission, definitions, version, data_dir = _saved(tmp_path, formula="CLOSE>1000")
    source = _source(tmp_path, outbox=service.outbox, config=_config(admission, data_dir))
    assert _group(source)["formula_pool_latest_result"].rows == ()
    daily = _runner(admission, definitions, data_dir).run_one("research", version, DAY)
    latest = _group(source)["formula_pool_latest_result"].rows[0]
    assert daily.match_count == latest["match_count"] == 0
    assert latest["unknown_count"] == 1
    assert latest["market_total"] == latest["no_match_count"] + latest["unknown_count"]
    assert latest["trade_date"] == DAY.isoformat()


@pytest.mark.parametrize(
    "failure", ("definition", "audit", "audit_receipt", "daily", "task", "task_row")
)
def test_bad_authority_or_result_refuses_projection(tmp_path: Path, failure: str) -> None:
    service, admission, definitions, version, data_dir = _saved(tmp_path)
    daily = _runner(admission, definitions, data_dir).run_one("research", version, DAY)
    config = _config(admission, data_dir)
    if failure == "definition":
        (config.definition_root / "research.json").write_bytes(b"{}")
    elif failure == "audit":
        with sqlite3.connect(service.outbox.path) as connection:
            connection.execute(
                "DELETE FROM page_control_effect WHERE command_id = ?",
                ("save-formula-pool-1",),
            )
    elif failure == "audit_receipt":
        bogus = canonical_json_bytes({"pool_name": "user/research", "version": "0" * 64}).decode(
            "utf-8"
        )
        with sqlite3.connect(service.outbox.path) as connection:
            connection.execute(
                "UPDATE page_control_command SET result_json=? WHERE command_id=?",
                (bogus, "save-formula-pool-1"),
            )
            connection.execute(
                "UPDATE page_control_effect SET result_json=? WHERE command_id=?",
                (bogus, "save-formula-pool-1"),
            )
    elif failure == "daily":
        (config.daily_root / "research" / f"{DAY.isoformat()}.json").write_bytes(b"{}")
    elif failure == "task_row":
        with sqlite3.connect(config.task_state_path) as connection:
            connection.execute("DELETE FROM formula_market_job WHERE task_id=?", (daily.task_id,))
    else:
        artifact = config.artifact_root / admission.store._artifact_name(
            daily.task_id, daily.result_sha256
        )
        artifact.write_bytes(b"{}")
    with pytest.raises(PageProjectionSourceIntegrityError):
        _group(_source(tmp_path, outbox=service.outbox, config=config))


def test_partial_or_mixed_generation_tables_are_rejected(tmp_path: Path) -> None:
    service, admission, _definitions, _version, data_dir = _saved(tmp_path)
    group = _group(_source(tmp_path, outbox=service.outbox, config=_config(admission, data_dir)))
    with pytest.raises(ValueError, match="incomplete"):
        validate_formula_pool_projections({"formula_pool_state": group["formula_pool_state"]})
    mixed = {
        name: ServingProjectionInput.bind(
            item,
            owner_dataset_id="signals",
            owner_generation_id=("b" if name == "formula_pool_definition" else "a") * 64,
        )
        for name, item in group.items()
    }
    with pytest.raises(ValueError, match="generation"):
        validate_formula_pool_projections(mixed)
    with pytest.raises(ValueError, match="incomplete"):
        ServingReadModelInput(
            observed_at=datetime.now(UTC) + timedelta(minutes=2),
            projections=(
                ServingProjectionInput.bind(
                    group["formula_pool_state"],
                    owner_dataset_id="signals",
                    owner_generation_id="a" * 64,
                ),
            ),
        )
    with pytest.raises(ValueError, match="generation"):
        ServingReadModelInput(
            observed_at=datetime.now(UTC) + timedelta(minutes=2),
            projections=tuple(mixed.values()),
        )
    state = group["formula_pool_state"]
    forged = ServingProjectionPayload(
        table_name=state.table_name,
        available_at=state.available_at,
        rows=({**state.rows[0], "run_count": 1},),
    )
    with pytest.raises(ValueError, match="counts"):
        validate_formula_pool_projections(group | {"formula_pool_state": forged})


def test_old_creation_task_still_verified_beyond_recent_job_window(tmp_path: Path) -> None:
    service, admission, definitions, version, data_dir = _saved(tmp_path)
    definition = definitions.read("research", expected_version=version)
    request, _result = admission.store.read_succeeded_task(definition.creation.task_id)
    created = (datetime.now(UTC) + timedelta(seconds=1)).isoformat()
    with sqlite3.connect(admission.config.state_path) as connection:
        for index in range(101):
            padded = request.model_copy(update={"idempotency_key": f"padding-{index:016d}"})
            payload = canonical_json_bytes(padded.model_dump(mode="json"))
            connection.execute(
                """INSERT INTO formula_market_job
                   (task_id, idempotency_key, request_json, request_sha256, status,
                    attempts, error_code, created_at, updated_at)
                   VALUES (?, ?, ?, ?, 'failed', 1, 'internal_error', ?, ?)""",
                (
                    f"{index + 1:032x}",
                    padded.idempotency_key,
                    payload.decode("utf-8"),
                    hashlib.sha256(payload).hexdigest(),
                    created,
                    created,
                ),
            )
    group = _group(_source(tmp_path, outbox=service.outbox, config=_config(admission, data_dir)))
    assert group["formula_pool_definition"].rows[0]["creation_task_id"] == (
        definition.creation.task_id
    )


def test_rule_pool_name_collision_is_rejected(tmp_path: Path) -> None:
    service, admission, _definitions, _version, data_dir = _saved(tmp_path)
    rules = data_dir / "user_presets"
    rules.mkdir(mode=0o700)
    (rules / "research.json").write_text("{}")
    with pytest.raises(PageProjectionSourceIntegrityError):
        _group(_source(tmp_path, outbox=service.outbox, config=_config(admission, data_dir)))


def test_missing_catalog_and_row_limit_refuse_instead_of_empty(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, admission, _definitions, _version, data_dir = _saved(tmp_path)
    config = _config(admission, data_dir)
    config.definition_root.rename(data_dir / "moved-formula-pools")
    with pytest.raises(PageProjectionSourceIntegrityError):
        _group(_source(tmp_path, outbox=service.outbox, config=config))
    (data_dir / "moved-formula-pools").rename(config.definition_root)
    config.daily_root.rename(data_dir / "moved-formula-daily")
    with pytest.raises(PageProjectionSourceIntegrityError):
        _group(_source(tmp_path, outbox=service.outbox, config=config))
    (data_dir / "moved-formula-daily").rename(config.daily_root)
    monkeypatch.setattr(projection_module, "MAX_FORMULA_POOL_DEFINITIONS", 0)
    with pytest.raises(PageProjectionSourceIntegrityError):
        _group(_source(tmp_path, outbox=service.outbox, config=config))
