"""A pool rule is publishable only with matching PageControl effect evidence."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.llm.schemas import RuleCall
from rquant.notification_state import NotificationStateStore
from rquant.page_control import (
    DeleteUserPool,
    ForkBuiltinPool,
    PageControlConsumer,
    PageControlOutbox,
    PageControlService,
    PageControlStatus,
    SaveNlPreset,
    SaveUserPool,
    SaveUserPoolV2,
    SaveUserPoolV3,
)
from rquant.pool_definition_projection import PoolMutation, build_pool_definition_rows
from rquant.runtime_builder_signal import NotifierSettings
from rquant.runtime_contracts import canonical_sha256
from rquant.runtime_deployment_bundle import _validate_manifest_authority
from rquant.runtime_service_control import RuntimeServicePlane
from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    PageProjectionSourceIntegrityError,
    SignalPageProjectionProducer,
    SignalPageProjectionSnapshot,
    _ReadonlyPageControlAuditReader,
)
from rquant.serving_read_models import ServingProjectionPayload
from tests.unit.test_runtime_deployment_bundle import _manifest
from tests.unit.test_serving_page_projection_source import _signal_projection_database

NOW = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)


def _service(root: Path) -> PageControlService:
    outbox = PageControlOutbox(root / "control.sqlite3")
    return PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=root / "data",
            log_dir=root / "logs",
            clock=lambda: NOW,
            lease_seconds=1,
        ),
    )


def _v2(command_id: str, **changes: object) -> SaveUserPoolV2:
    fields: dict[str, object] = {
        "command_id": command_id,
        "requested_at": NOW,
        "base_name": "breakout",
        "display_name": "突破观察",
        "description": "日终筛选",
        "rule_calls": (RuleCall(name="not_st", args={}),),
        "include_columns": ("CLOSE[0]",),
        "depends_on": "n-shape-pool1",
        "delay_days": 2,
    }
    fields.update(changes)
    return SaveUserPoolV2.model_validate(fields)


def _rows(root: Path) -> dict[str, dict[str, object]]:
    from rquant.serving_page_projection_source import _pool_definition_projection

    reader = _ReadonlyPageControlAuditReader(root / "control.sqlite3")
    with reader.snapshot():
        projection = _pool_definition_projection(root / "data" / "user_presets", reader)
    return {str(row["pool_name"]): dict(row) for row in projection.rows}


def test_builtin_metadata_import_is_pure() -> None:
    process = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import rquant.builtin_presets as b; "
            "assert 'rquant.config' not in sys.modules; "
            "assert 'rquant.presets' not in sys.modules; "
            "assert b.BUILTIN_PRESET_SCREENS['n-shape-pool2'].offset_days == 2",
        ],
        env={**os.environ, "RQUANT_DISABLE_DOTENV": "1", "PYTHONPATH": "src"},
        check=False,
        capture_output=True,
        text=True,
    )
    assert process.returncode == 0, process.stderr


def test_verified_v2_is_exact_and_has_matching_command_version(tmp_path: Path) -> None:
    service = _service(tmp_path)
    receipt = service.submit(_v2("save-v2"))
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert isinstance(receipt.result, dict)

    row = _rows(tmp_path)["user/breakout"]
    assert row["state"] == "available"
    assert row["version"] == receipt.result["version"]
    assert row["command_id"] == "save-v2"
    assert row["depends_on"] == "n-shape-pool1"
    assert row["delay_mode"] == "exact"
    assert row["delay_days"] == 2
    assert row["can_edit"] is True
    assert row["ranking_json"] is None
    assert json.loads(str(row["rules_json"]))[0]["name"] == "not_st"
    assert _rows(tmp_path)["n-shape-pool2"]["delay_mode"] == "legacy_window"


def test_ranked_definition_requires_exact_audited_ranking_and_publishes_it() -> None:
    ranking = {
        "conditions": [
            {"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 60.0},
            {"metric": "TURNOVER_RATE[0]", "ascending": False, "weight": 40.0},
        ],
        "top_n": 20,
    }
    payload: dict[str, object] = {
        "kind": "save_user_pool_v3",
        "command_id": "ranked-save",
        "requested_at": NOW.isoformat(),
        "base_name": "breakout",
        "display_name": "突破观察",
        "description": "日终筛选",
        "rule_calls": [{"name": "not_st", "args": {}}],
        "include_columns": ["CLOSE[0]"],
        "depends_on": None,
        "delay_days": 0,
        "expected_version": None,
        "ranking": ranking,
    }
    raw: dict[str, object] = {
        "schema_version": 3,
        "name": "breakout",
        "display_name": "突破观察",
        "description": "日终筛选",
        "rules": payload["rule_calls"],
        "include_columns": payload["include_columns"],
        "depends_on": None,
        "delay_days": 0,
        "ranking": ranking,
        "source": "page_control_v3",
        "updated_at": NOW.isoformat(timespec="seconds"),
        "command_id": "ranked-save",
        "command_hash": "a" * 64,
    }
    mutation = PoolMutation(
        command_id="ranked-save",
        command_kind="save_user_pool_v3",
        command_hash="a" * 64,
        payload=payload,
        result={"path": "/synthetic/breakout.json", "version": canonical_sha256(raw)},
    )

    def published(
        document: dict[str, object], evidence: PoolMutation = mutation
    ) -> dict[str, object]:
        return next(
            row
            for row in build_pool_definition_rows(
                {"breakout": document}, {"breakout": evidence}, root_path="/synthetic"
            )
            if row["pool_name"] == "user/breakout"
        )

    verified = published(raw)
    assert verified["state"] == "available"
    assert verified["can_edit"] is True
    assert json.loads(str(verified["ranking_json"])) == ranking

    altered = json.loads(json.dumps(raw))
    altered["ranking"]["top_n"] = 21
    rejected = published(altered)
    assert rejected["state"] == "unavailable"
    assert rejected["can_edit"] is False
    assert rejected["ranking_json"] is None

    missing = {key: value for key, value in raw.items() if key != "ranking"}
    assert published(missing)["state"] == "unavailable"

    unranked_raw = {**raw, "ranking": None}
    unranked_evidence = PoolMutation(
        command_id=mutation.command_id,
        command_kind=mutation.command_kind,
        command_hash=mutation.command_hash,
        payload={**payload, "ranking": None},
        result={
            "path": "/synthetic/breakout.json",
            "version": canonical_sha256(unranked_raw),
        },
    )
    unranked = published(unranked_raw, unranked_evidence)
    assert unranked["state"] == "available"
    assert unranked["can_edit"] is True
    assert unranked["ranking_json"] == "null"


def test_v3_page_control_audit_reaches_the_readonly_projection(tmp_path: Path) -> None:
    service = _service(tmp_path)
    ranked = SaveUserPoolV3.model_validate(
        {
            **_v2("save-ranked", depends_on=None, delay_days=0).model_dump(mode="json"),
            "kind": "save_user_pool_v3",
            "ranking": {
                "conditions": [{"metric": "RETURN_20D_PCT[0]", "ascending": False, "weight": 100}],
                "top_n": 10,
            },
        }
    )
    receipt = service.submit(ranked)
    assert receipt.status is PageControlStatus.SUCCEEDED

    row = _rows(tmp_path)["user/breakout"]
    assert row["state"] == "available"
    assert row["can_edit"] is True
    assert json.loads(str(row["ranking_json"]))["top_n"] == 10


def test_direct_legacy_file_has_no_verified_editable_rules(tmp_path: Path) -> None:
    _service(tmp_path)
    directory = tmp_path / "data" / "user_presets"
    directory.mkdir(parents=True)
    (directory / "orphan.json").write_text(
        json.dumps({"name": "orphan", "rules": [{"name": "not_st", "args": {}}]}),
        encoding="utf-8",
    )

    row = _rows(tmp_path)["user/orphan"]
    assert row["state"] == "migration_required"
    assert row["can_edit"] is False
    assert row["rules_json"] is None


def test_matching_legacy_command_keeps_window_semantics(tmp_path: Path) -> None:
    service = _service(tmp_path)
    receipt = service.submit(
        SaveUserPool(
            command_id="save-legacy",
            requested_at=NOW,
            base_name="breakout",
            rule_calls=(RuleCall(name="not_st", args={}),),
        )
    )
    assert receipt.status is PageControlStatus.SUCCEEDED
    row = _rows(tmp_path)["user/breakout"]
    assert row["state"] == "available"
    assert row["delay_mode"] == "none"
    assert row["version"] is not None


def test_page_control_nl_and_builtin_fork_are_verified(tmp_path: Path) -> None:
    service = _service(tmp_path)
    nl = service.submit(
        SaveNlPreset(
            command_id="save-nl",
            requested_at=NOW,
            name="自然语言池",
            rule_calls=(RuleCall(name="not_st", args={}),),
        )
    )
    fork = service.submit(
        ForkBuiltinPool(
            command_id="fork-builtin",
            requested_at=NOW,
            builtin_name="n-shape-pool1",
            target_base_name="copy-pool",
        )
    )
    assert nl.status is PageControlStatus.SUCCEEDED
    assert fork.status is PageControlStatus.SUCCEEDED

    rows = _rows(tmp_path)
    assert rows["user/自然语言池"]["state"] == "available"
    assert rows["user/copy-pool"]["state"] == "available", rows["user/copy-pool"]["reason"]
    assert rows["user/copy-pool"]["source_kind"] == "user"
    assert "builtin/" not in rows["user/copy-pool"]["description"]
    assert rows["n-shape-pool1"]["display_name"] == "N 形态一池"
    assert "Pool1" not in rows["n-shape-pool1"]["description"]


def test_legacy_command_cannot_authorize_added_dependency_fields(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert (
        service.submit(
            SaveUserPool(
                command_id="save-legacy",
                requested_at=NOW,
                base_name="breakout",
                rule_calls=(RuleCall(name="not_st", args={}),),
            )
        ).status
        is PageControlStatus.SUCCEEDED
    )
    path = tmp_path / "data" / "user_presets" / "breakout.json"
    raw = json.loads(path.read_text(encoding="utf-8"))
    raw.update({"depends_on": "n-shape-pool1", "offset_days": 2})
    path.write_text(json.dumps(raw), encoding="utf-8")

    row = _rows(tmp_path)["user/breakout"]
    assert row["state"] == "unavailable"
    assert row["depends_on"] is None


def test_tampered_v2_content_is_unavailable_not_effective(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    path = tmp_path / "data" / "user_presets" / "breakout.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    data["display_name"] = "伪造内容"
    path.write_text(json.dumps(data), encoding="utf-8")

    row = _rows(tmp_path)["user/breakout"]
    assert row["state"] == "unavailable"
    assert row["can_edit"] is False
    assert row["rules_json"] is None


def test_invalid_saved_json_is_distinct_from_missing_file(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    path = tmp_path / "data" / "user_presets" / "breakout.json"

    path.write_text("{broken", encoding="utf-8")
    broken = _rows(tmp_path)["user/breakout"]
    assert broken["state"] == "unavailable"
    assert broken["reason"] == "invalid_content"

    path.unlink()
    missing = _rows(tmp_path)["user/breakout"]
    assert missing["state"] == "unavailable"
    assert missing["reason"] == "file_missing"


def test_delete_tombstone_cannot_publish_leftover_file(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    path = tmp_path / "data" / "user_presets" / "breakout.json"
    old_content = path.read_bytes()
    receipt = service.submit(
        DeleteUserPool(command_id="delete-v2", requested_at=NOW, base_name="breakout")
    )
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert _rows(tmp_path)["user/breakout"]["state"] == "deleted"
    path.write_bytes(old_content)
    assert _rows(tmp_path)["user/breakout"]["state"] == "unavailable"
    path.write_text("{broken", encoding="utf-8")
    assert _rows(tmp_path)["user/breakout"]["state"] == "unavailable"


def test_effect_result_tamper_fails_closed(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    with sqlite3.connect(tmp_path / "control.sqlite3") as connection:
        connection.execute(
            "UPDATE page_control_effect SET result_json = ? WHERE command_id = ?",
            ('{"path":"/wrong","version":"' + "0" * 64 + '"}', "save-v2"),
        )
    with pytest.raises(PageProjectionSourceIntegrityError):
        _rows(tmp_path)


def test_duplicate_audit_payload_key_is_not_authority(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    with sqlite3.connect(tmp_path / "control.sqlite3") as connection:
        original = connection.execute(
            "SELECT payload_json FROM page_control_command WHERE command_id = ?",
            ("save-v2",),
        ).fetchone()[0]
        connection.execute(
            "UPDATE page_control_command SET payload_json = ? WHERE command_id = ?",
            (original[:-1] + ',"command_id":"save-v2"}', "save-v2"),
        )
    with pytest.raises(PageProjectionSourceIntegrityError, match="audit"):
        _rows(tmp_path)


def test_older_file_rollback_cannot_reclaim_latest_version(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first = service.submit(_v2("save-first"))
    assert first.status is PageControlStatus.SUCCEEDED
    path = tmp_path / "data" / "user_presets" / "breakout.json"
    old = path.read_bytes()
    assert isinstance(first.result, dict)
    updated = service.submit(
        _v2("save-second", expected_version=first.result["version"], display_name="新版本")
    )
    assert updated.status is PageControlStatus.SUCCEEDED
    path.write_bytes(old)
    assert _rows(tmp_path)["user/breakout"]["state"] == "unavailable"


def test_missing_parent_disables_child_without_guessing_edge(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert (
        service.submit(_v2("parent", base_name="parent", depends_on=None, delay_days=0)).status
        is PageControlStatus.SUCCEEDED
    )
    assert (
        service.submit(
            _v2("child", base_name="child", depends_on="user/parent", delay_days=1)
        ).status
        is PageControlStatus.SUCCEEDED
    )
    assert (
        service.submit(
            DeleteUserPool(command_id="delete-parent", requested_at=NOW, base_name="parent")
        ).status
        is PageControlStatus.SUCCEEDED
    )

    rows = _rows(tmp_path)
    assert rows["user/parent"]["state"] == "deleted"
    assert rows["user/child"]["state"] == "unavailable"
    assert rows["user/child"]["depends_on"] is None
    assert rows["user/child"]["rules_json"] is None


def test_pool_source_rejects_symlinks_and_oversize(tmp_path: Path) -> None:
    _service(tmp_path)
    root = tmp_path / "data" / "user_presets"
    root.mkdir(parents=True)
    target = tmp_path / "outside.json"
    target.write_text("{}", encoding="utf-8")
    (root / "linked.json").symlink_to(target)
    with pytest.raises(PageProjectionSourceIntegrityError, match="non-symlink"):
        _rows(tmp_path)
    (root / "linked.json").unlink()
    (root / "huge.json").write_text("x" * (64 * 1024 + 1), encoding="utf-8")
    with pytest.raises(PageProjectionSourceIntegrityError, match="size bound"):
        _rows(tmp_path)


def test_pool_projection_joins_same_signal_generation(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    source = DuckDBSignalPageProjectionSource(
        database,
        user_presets_root=tmp_path / "data" / "user_presets",
        page_control_outbox=tmp_path / "control.sqlite3",
    )

    snapshot = source(NOW)
    assert isinstance(snapshot, SignalPageProjectionSnapshot)
    projection = {item.table_name: item for item in snapshot.projections}["pool_definition"]
    row = next(item for item in projection.rows if item["pool_name"] == "user/breakout")
    assert row["state"] == "available"
    assert projection.available_at == datetime(1970, 1, 1, tzinfo=UTC)


def test_unchanged_pool_projection_does_not_publish_new_generation(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(
            database,
            user_presets_root=tmp_path / "data" / "user_presets",
            page_control_outbox=tmp_path / "control.sqlite3",
        ),
        store=NotificationStateStore(tmp_path / "notification.sqlite3"),
    )

    first = producer.publish(NOW)
    second = producer.publish(NOW + timedelta(seconds=2))
    assert first.written is True
    assert second.written is False
    assert second.generation_id == first.generation_id

    (tmp_path / "data" / "user_presets" / "breakout.json").write_text("{broken", encoding="utf-8")
    changed = producer.publish(NOW + timedelta(seconds=4))
    assert changed.written is True
    assert changed.generation_id != first.generation_id


def test_valid_client_clock_skew_keeps_new_pool_definition_published(tmp_path: Path) -> None:
    service = _service(tmp_path)
    first_save = service.submit(_v2("save-first"))
    assert first_save.status is PageControlStatus.SUCCEEDED
    assert isinstance(first_save.result, dict)
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(
            database,
            user_presets_root=tmp_path / "data" / "user_presets",
            page_control_outbox=tmp_path / "control.sqlite3",
        ),
        store=store,
    )
    first_generation = producer.publish(NOW)

    future_save = service.submit(
        _v2(
            "save-future",
            requested_at=NOW + timedelta(minutes=2),
            expected_version=first_save.result["version"],
            display_name="新版本",
        )
    )
    assert future_save.status is PageControlStatus.SUCCEEDED
    assert isinstance(future_save.result, dict)

    new_generation = producer.publish(NOW + timedelta(seconds=2))
    assert new_generation.written is True
    assert new_generation.generation_id != first_generation.generation_id
    latest = store.serving_snapshot(observed_at=NOW + timedelta(seconds=2), history_limit=1)
    projection = {item.table_name: item for item in latest.payload.projections}["pool_definition"]
    row = next(item for item in projection.rows if item["pool_name"] == "user/breakout")
    assert row["state"] == "available"
    assert row["version"] == future_save.result["version"]


def test_old_signal_generation_without_optional_pool_projection_still_valid(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(database), store=store
    ).publish(NOW)
    latest = store.serving_snapshot(observed_at=NOW, history_limit=1)
    assert "pool_definition" not in {item.table_name for item in latest.payload.projections}


def test_pool_contract_rejects_row_and_byte_overflow(tmp_path: Path) -> None:
    _service(tmp_path)
    seed = _rows(tmp_path)["n-shape-pool1"]
    too_many = tuple({**seed, "pool_name": f"pool-{index}"} for index in range(513))
    with pytest.raises(ValueError, match="row budget"):
        ServingProjectionPayload(table_name="pool_definition", available_at=NOW, rows=too_many)
    too_large = tuple(
        {**seed, "pool_name": f"pool-{index}", "description": "x" * 60_000} for index in range(40)
    )
    with pytest.raises(ValueError, match="byte budget"):
        ServingProjectionPayload(table_name="pool_definition", available_at=NOW, rows=too_large)


def test_invalid_next_source_revokes_previously_published_pool_rules(tmp_path: Path) -> None:
    service = _service(tmp_path)
    assert service.submit(_v2("save-v2")).status is PageControlStatus.SUCCEEDED
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(
            database,
            user_presets_root=tmp_path / "data" / "user_presets",
            page_control_outbox=tmp_path / "control.sqlite3",
        ),
        store=store,
    )
    producer.publish(NOW)
    root = tmp_path / "data" / "user_presets"
    target = tmp_path / "outside.json"
    target.write_bytes((root / "breakout.json").read_bytes())
    (root / "breakout.json").unlink()
    (root / "breakout.json").symlink_to(target)

    next_iteration = NOW + timedelta(microseconds=1)
    producer.publish(next_iteration)
    latest = store.serving_snapshot(observed_at=next_iteration, history_limit=1)
    assert "pool_definition" not in {item.table_name for item in latest.payload.projections}


def test_notifier_pool_setting_requires_explicit_page_control_audit(tmp_path: Path) -> None:
    settings: dict[str, object] = {
        "signal_spool_root": str(tmp_path / "signal-spool"),
        "notification_state_path": str(tmp_path / "notification.sqlite3"),
        "worker_id": "notifier-test",
        "batch_limit": 1,
        "lease_seconds": 1,
        "serving_authority_root": str(tmp_path / "serving"),
        "page_projection_database_path": str(tmp_path / "rquant_ro.duckdb"),
        "page_projection_user_presets_root": str(tmp_path / "page-control" / "user_presets"),
        "page_projection_page_control_outbox_path": str(tmp_path / "control.sqlite3"),
    }
    parsed = NotifierSettings.model_validate(settings)
    assert parsed.page_projection_user_presets_root == tmp_path / "page-control" / "user_presets"
    with pytest.raises(ValueError, match="pool.*audit"):
        NotifierSettings.model_validate(
            {
                key: value
                for key, value in settings.items()
                if key != "page_projection_page_control_outbox_path"
            }
        )


def test_deployment_manifest_cannot_redirect_pool_catalog(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    manifest = _manifest(
        root,
        service_id="notifier-admin",
        kind=RuntimeServiceKind.NOTIFIER,
        plane=RuntimeServicePlane.LIVE,
    )
    settings = dict(manifest.settings)
    settings["page_projection_user_presets_root"] = str(tmp_path / "wrong" / "user_presets")
    settings["page_projection_page_control_outbox_path"] = str(
        root / "control" / "page-control.sqlite3"
    )
    wrong = manifest.model_copy(update={"settings": settings})
    with pytest.raises(ValueError, match="user_presets"):
        _validate_manifest_authority(
            wrong, producer_commit=manifest.producer_commit, runtime_root=root
        )
