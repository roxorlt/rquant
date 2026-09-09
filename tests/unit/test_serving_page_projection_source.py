from __future__ import annotations

import errno
import json
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from rquant.canvas_publication_receipt import CanvasPublicationReceipt
from rquant.notification_state import NotificationStateStore
from rquant.page_control import (
    DeleteCanvas,
    PageControlConsumer,
    PageControlOutbox,
    PageControlReceipt,
    PageControlService,
    PageControlStatus,
    SaveCanvas,
)
from rquant.research_gate import ResearchGateFailure
from rquant.runtime_contracts import canonical_sha256
from rquant.serving_page_projection_source import (
    CanvasDiagnosticProjectionRow,
    CanvasHitProjectionRow,
    CanvasLatestTradeDateProjectionRow,
    DuckDBLabPageProjectionSource,
    DuckDBSignalPageProjectionSource,
    LabPageProjectionSnapshot,
    MinuteCoverageProjectionRow,
    PageProjectionSourceIntegrityError,
    ResearchGateProjectionRow,
    ScreenBoundsProjectionRow,
    SignalPageProjectionProducer,
    SignalPageProjectionSnapshot,
    _ReadonlyPageControlAuditReader,
)
from rquant.storage.duckdb import DuckDBStore
from tests.canvas_ed25519_support import (
    create_canvas_ed25519_test_authority,
    create_rotating_canvas_ed25519_test_authority,
)


def _generation_binds(outbox: object) -> Path:
    """A directory the reading role owns, which is never the outbox's own directory.

    #241: `snapshot()` pins the generation it reads with a hard link, and it used to do
    that beside the outbox -- a directory that belongs to the page-control service and is
    read-only in every other unit. Every reader now says where its own scratch goes.
    """

    path = Path(getattr(outbox, "path", outbox))
    return path.parent / "generation-binds"


NOW = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)
COMMIT = "a" * 40
_CATALOG_AUTHORITIES: dict[Path, object] = {}


def _save_canvas_catalog_record(
    tmp_path: Path,
    *,
    name: str = "breakout",
    description: str = "突破候选",
    pool_refs: tuple[str, ...] = ("n-shape-pool1",),
    requested_at: datetime = NOW - timedelta(days=1),
    source: str = "page_control",
    command_id: str = "canvas-command",
) -> tuple[PageControlOutbox, Path, SaveCanvas, PageControlReceipt]:
    outbox, catalog, command, receipt, _authority = _save_signed_canvas_catalog_record(
        tmp_path,
        name=name,
        description=description,
        pool_refs=pool_refs,
        requested_at=requested_at,
        source=source,
        command_id=command_id,
    )
    return outbox, catalog, command, receipt


def _save_signed_canvas_catalog_record(
    tmp_path: Path,
    *,
    name: str = "breakout",
    description: str = "突破候选",
    pool_refs: tuple[str, ...] = ("n-shape-pool1",),
    requested_at: datetime = NOW - timedelta(days=1),
    source: str = "page_control",
    command_id: str = "canvas-command",
) -> tuple[PageControlOutbox, Path, SaveCanvas, PageControlReceipt, object]:
    authority = create_canvas_ed25519_test_authority(tmp_path / f"{command_id}-keys")
    outbox = PageControlOutbox(tmp_path / f"{command_id}.sqlite3")
    data_dir = tmp_path / f"{command_id}-data"
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / f"{command_id}-logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    command = SaveCanvas(
        command_id=command_id,
        requested_at=requested_at,
        name=name,
        description=description,
        pool_refs=pool_refs,
        source=source,
    )
    receipt = service.submit(command)
    assert receipt.status is PageControlStatus.SUCCEEDED
    _CATALOG_AUTHORITIES[data_dir / "canvases"] = authority
    return outbox, data_dir / "canvases", command, receipt, authority


def _canvas_source(
    database: Path,
    *,
    catalog: Path,
    outbox: PageControlOutbox | None,
) -> DuckDBSignalPageProjectionSource:
    authority = _CATALOG_AUTHORITIES[catalog]
    return DuckDBSignalPageProjectionSource(
        database,
        canvas_catalog_root=catalog,
        canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
        canvas_publication_keyring=authority.keyring,
        page_control_outbox=outbox,
    )


def _tamper_catalog_record(path: Path, **updates: object) -> dict[str, object]:
    record = json.loads(path.read_text(encoding="utf-8"))
    record.update(updates)
    record["source_identity_hash"] = canonical_sha256(
        {
            "schema_version": record["schema_version"],
            "command_id": record["command_id"],
            "command_hash": record["command_hash"],
            "source": record["source"],
        }
    )
    record["record_hash"] = canonical_sha256(
        {key: value for key, value in record.items() if key != "record_hash"}
    )
    path.write_text(json.dumps(record, ensure_ascii=True), encoding="utf-8")
    return record


def _tamper_outbox_canvas_result(
    outbox: PageControlOutbox,
    command_id: str,
    **updates: object,
) -> None:
    with sqlite3.connect(outbox.path) as connection:
        row = connection.execute(
            "SELECT result_json FROM page_control_command WHERE command_id = ?",
            (command_id,),
        ).fetchone()
        assert row is not None
        result = json.loads(row[0])
        assert isinstance(result, dict)
        result.update(updates)
        connection.execute(
            "UPDATE page_control_command SET result_json = ? WHERE command_id = ?",
            (json.dumps(result, ensure_ascii=True), command_id),
        )


def _rewrite_publication_receipt(receipt_path: Path, **claim_updates: object) -> None:
    publication = CanvasPublicationReceipt.model_validate_json(
        receipt_path.read_text(encoding="utf-8")
    )
    payload = publication.model_dump(mode="json")
    claims = payload["claims"]
    assert isinstance(claims, dict)
    claims.update(claim_updates)
    receipt_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def _signal_projection_database(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE screen_result (
                trade_date DATE NOT NULL,
                preset_name VARCHAR NOT NULL,
                ts_code VARCHAR NOT NULL,
                name VARCHAR,
                close DOUBLE,
                pct_chg DOUBLE,
                extra JSON,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO screen_result VALUES
              ('2026-07-31', 'n-shape-pool1', '600000.SH', 'PF', 10.6, 6.0, '{}',
               '2026-07-31 15:05:00'),
              ('2026-08-03', 'future', '000001.SZ', 'FUT', 11.0, 1.0, '{}',
               '2026-08-03 17:00:00')
            """
        )
        connection.execute(
            """
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
                source VARCHAR,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO minute_bar VALUES
              ('600000.SH', '2026-07-31 09:30:00', '1min', 10, 10.2, 9.9, 10.1,
               1000, 10000, 'tushare', '2026-07-31 09:31:00'),
              ('000001.SZ', '2026-08-03 17:00:00', '1min', 11, 11, 11, 11,
               1000, 11000, 'future-source', '2026-08-03 17:00:01')
            """
        )
    finally:
        connection.close()


def test_duckdb_signal_source_builds_bounded_pit_page_projections(tmp_path: Path) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)

    snapshot = DuckDBSignalPageProjectionSource(database)(NOW)

    projections = {item.table_name: item for item in snapshot.projections}
    assert projections["screen_bounds"].rows == (
        {
            "preset_name": "n-shape-pool1",
            "min_date": "2026-07-31",
            "max_date": "2026-07-31",
            "candidate_count": 1,
        },
    )
    assert [row["source"] for row in projections["minute_coverage"].rows] == [
        "all",
        "tushare",
    ]
    assert projections["canvas_latest_trade_date"].rows == (
        {"snapshot_key": "current", "trade_date": "2026-07-31"},
    )
    assert len(projections["canvas_hit"].rows) == 1
    assert projections["canvas_definition"].rows == ()
    assert "future" not in snapshot.model_dump_json()


def test_signal_source_publishes_generation_pinned_pulse_and_runtime_config(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    live_root = tmp_path / "surge_live"
    live_root.mkdir()
    (live_root / "pulse-2026-08-03.jsonl").write_text(
        json.dumps(
            {
                "t": "09:31",
                "limit_up": 20,
                "limit_down": 2,
                "broken": 1,
                "up": 2600,
                "down": 2400,
                "up_ratio_pct": 50.0,
                "total": 5400,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (live_root / "pulse_alerts-2026-08-03.jsonl").write_text(
        json.dumps(
            {
                "t": "10:15",
                "kind": "broken_surge",
                "kind_label": "炸板潮",
                "before": 2.0,
                "after": 6.0,
                "window_minutes": 10,
                "message": "炸板异动",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    (live_root / "runtime_config.json").write_text(
        json.dumps(
            {
                "day": "2026-08-03",
                "boards": ["main", "gem"],
                "k_rough": 1.2,
                "k_cum": 2.5,
                "ratio_cap": 8.0,
                "skip_first_minutes": 0,
                "tushare_rate_per_min": 2,
                "require_price_strength": True,
                "max_room_to_limit_pct": 1.0,
            }
        ),
        encoding="utf-8",
    )
    source_timestamp = (NOW - timedelta(seconds=1)).timestamp()
    source_available_at = datetime.fromtimestamp(source_timestamp, tz=UTC)
    for path in live_root.iterdir():
        os.utime(path, (source_timestamp, source_timestamp))

    snapshot = DuckDBSignalPageProjectionSource(
        database,
        surge_live_root=live_root,
    )(NOW)

    projections = {item.table_name: item for item in snapshot.projections}
    assert {
        projections[name].available_at
        for name in ("pulse_history", "pulse_alert", "surge_runtime_config")
    } == {source_available_at}
    assert projections["pulse_history"].rows[0]["t"] == "09:31"
    assert projections["pulse_history"].rows[0]["as_of"] == "2026-08-03T01:31:00+00:00"
    assert projections["pulse_alert"].rows[0]["kind"] == "broken_surge"
    assert projections["surge_runtime_config"].rows == (
        {
            "snapshot_key": "current",
            "trade_date": "2026-08-03",
            "as_of": source_available_at.isoformat(),
            "boards_json": '["main","gem"]',
            "k_rough": 1.2,
            "k_cum": 2.5,
            "ratio_cap": 8.0,
            "skip_first_minutes": 0,
            "tushare_rate_per_min": 2,
            "require_price_strength": True,
            "max_room_to_limit_pct": 1.0,
        },
    )
    assert snapshot.available_at == max(item.available_at for item in snapshot.projections)


def test_signal_source_omits_missing_optional_pulse_projections(tmp_path: Path) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)

    snapshot = DuckDBSignalPageProjectionSource(
        database,
        surge_live_root=tmp_path / "missing-surge-live",
    )(NOW)

    names = {item.table_name for item in snapshot.projections}
    assert not {"pulse_history", "pulse_alert", "surge_runtime_config"}.intersection(names)


def test_signal_source_rejects_malformed_present_pulse_file(tmp_path: Path) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    live_root = tmp_path / "surge_live"
    live_root.mkdir()
    pulse = live_root / "pulse-2026-08-03.jsonl"
    pulse.write_text("{broken\n", encoding="utf-8")
    source_timestamp = (NOW - timedelta(seconds=1)).timestamp()
    os.utime(pulse, (source_timestamp, source_timestamp))

    with pytest.raises(PageProjectionSourceIntegrityError, match="invalid JSON"):
        DuckDBSignalPageProjectionSource(database, surge_live_root=live_root)(NOW)


@pytest.mark.parametrize(
    ("filename", "payload", "error_match"),
    (
        (
            "pulse-2026-08-03.jsonl",
            {
                "t": "09:31",
                "limit_up": "20",
                "limit_down": 2,
                "broken": 1,
                "up": 2600,
                "down": 2400,
                "up_ratio_pct": 50.0,
                "total": 5400,
            },
            "pulse history row is invalid",
        ),
        (
            "pulse-2026-08-03.jsonl",
            {
                "t": "09:31",
                "limit_up": True,
                "limit_down": 2,
                "broken": 1,
                "up": 2600,
                "down": 2400,
                "up_ratio_pct": 50.0,
                "total": 5400,
            },
            "pulse history row is invalid",
        ),
        (
            "pulse_alerts-2026-08-03.jsonl",
            {
                "t": "10:15",
                "kind": "broken_surge",
                "kind_label": "炸板潮",
                "before": "2.0",
                "after": 6.0,
                "window_minutes": 10,
                "message": "炸板异动",
            },
            "pulse alert row is invalid",
        ),
        (
            "runtime_config.json",
            {
                "day": "2026-08-03",
                "boards": ["main", "gem"],
                "k_rough": "1.2",
                "k_cum": 2.5,
                "ratio_cap": 8.0,
                "skip_first_minutes": 0,
                "tushare_rate_per_min": 2,
                "require_price_strength": True,
                "max_room_to_limit_pct": 1.0,
            },
            "surge runtime config is invalid",
        ),
        (
            "runtime_config.json",
            {
                "day": "2026-08-03",
                "boards": ["main", "gem"],
                "k_rough": 1.2,
                "k_cum": 2.5,
                "ratio_cap": 8.0,
                "skip_first_minutes": 0,
                "tushare_rate_per_min": 2,
                "require_price_strength": "false",
                "max_room_to_limit_pct": 1.0,
            },
            "surge runtime config is invalid",
        ),
        (
            "runtime_config.json",
            {
                "day": "2026-08-03",
                "boards": ["main", "gem"],
                "k_rough": 1.2,
                "k_cum": 2.5,
                "ratio_cap": 8.0,
                "skip_first_minutes": 0,
                "tushare_rate_per_min": 2,
                "require_price_strength": 0,
                "max_room_to_limit_pct": 1.0,
            },
            "surge runtime config is invalid",
        ),
        (
            "runtime_config.json",
            {
                "day": "2026-08-03",
                "boards": ["main", 3],
                "k_rough": 1.2,
                "k_cum": 2.5,
                "ratio_cap": 8.0,
                "skip_first_minutes": 0,
                "tushare_rate_per_min": 2,
                "require_price_strength": True,
                "max_room_to_limit_pct": 1.0,
            },
            "surge runtime config is invalid",
        ),
    ),
)
def test_signal_projection_producer_rejects_wrong_source_types_without_generation(
    tmp_path: Path,
    filename: str,
    payload: dict[str, object],
    error_match: str,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    live_root = tmp_path / "surge_live"
    live_root.mkdir()
    source_path = live_root / filename
    suffix = "\n" if filename.endswith(".jsonl") else ""
    source_path.write_text(json.dumps(payload, ensure_ascii=False) + suffix, encoding="utf-8")
    source_timestamp = (NOW - timedelta(seconds=1)).timestamp()
    os.utime(source_path, (source_timestamp, source_timestamp))
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(database, surge_live_root=live_root),
        store=store,
    )

    before = store.serving_snapshot(observed_at=NOW, history_limit=1)
    with pytest.raises(PageProjectionSourceIntegrityError, match=error_match):
        producer.publish(NOW)
    after = store.serving_snapshot(observed_at=NOW, history_limit=1)

    assert before.projection_generation_id is None
    assert after.projection_generation_id is None


def test_duckdb_signal_source_binds_generation_opened_during_connect(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    attacker = tmp_path / "attacker.duckdb"
    _signal_projection_database(database)
    _signal_projection_database(attacker)
    with duckdb.connect(str(attacker)) as connection:
        connection.execute(
            "UPDATE screen_result SET preset_name = 'attacker' WHERE preset_name = 'n-shape-pool1'"
        )
    original_connect = duckdb.connect
    attacker_swaps = 0

    def swap_only_during_connect(path: str, *args: object, **kwargs: object):
        nonlocal attacker_swaps
        attacker_swaps += 1
        trusted_hold = tmp_path / "trusted-held.duckdb"
        os.replace(database, trusted_hold)
        os.replace(attacker, database)
        try:
            connection = original_connect(path, *args, **kwargs)
        finally:
            os.replace(database, attacker)
            os.replace(trusted_hold, database)
        return connection

    monkeypatch.setattr(duckdb, "connect", swap_only_during_connect)

    #: what the notifier's unit gives it: `live/notifications/%i`, the one directory it
    #: may write. On Linux the descriptor pins the generation and this is never touched;
    #: where the engine refuses the descriptor (macOS, and the production host -- #255)
    #: the pinned inode is copied here and the swap below cannot reach the copy.
    snapshot = DuckDBSignalPageProjectionSource(
        database,
        control_root=tmp_path / "notifier-state",
        #: the read-only replica is replaced by `rename()` (`sync-readonly-replica.sh`),
        #: which is the only shape the copy branch is allowed on (review SF-1)
        atomically_published=True,
    )(NOW)

    screen_bounds = {
        str(row["preset_name"])
        for projection in snapshot.projections
        if projection.table_name == "screen_bounds"
        for row in projection.rows
    }
    assert "n-shape-pool1" in screen_bounds
    assert "attacker" not in screen_bounds
    assert attacker_swaps >= 1


def test_signal_source_publishes_canvas_definitions_from_bounded_catalog(tmp_path: Path) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, command, receipt = _save_canvas_catalog_record(
        tmp_path,
        pool_refs=("n-shape-pool1", "user/strong"),
        requested_at=datetime(2026, 8, 2, 7, 0, tzinfo=UTC),
        source="canvas_page",
        command_id="canvas-page-command",
    )
    assert command.command_id == "canvas-page-command"
    assert isinstance(receipt.result, dict)

    snapshot = _canvas_source(database, catalog=catalog, outbox=outbox)(NOW)

    projections = {item.table_name: item for item in snapshot.projections}
    definition = projections["canvas_definition"].rows[0]
    assert {key: value for key, value in definition.items() if key != "version_hash"} == {
        "name": "breakout",
        "description": "突破候选",
        "pool_refs_json": '["n-shape-pool1","user/strong"]',
        "created_at": "2026-08-02T07:00:00Z",
        "updated_at": "2026-08-02T07:00:00Z",
        "source": "canvas_page",
        "command_id": "canvas-page-command",
        "command_hash": str(definition["command_hash"]),
        "source_identity_hash": str(definition["source_identity_hash"]),
        "record_hash": receipt.result["record_hash"],
    }
    assert len(str(definition["version_hash"])) == 64
    assert len(str(definition["record_hash"])) == 64


def test_signal_source_rejects_catalog_and_outbox_tamper_even_when_hashes_recomputed(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id="signed-canvas-command",
    )
    assert isinstance(receipt.result, dict)
    tampered = _tamper_catalog_record(
        catalog / "breakout.json",
        description="tampered but self-consistent",
    )
    _tamper_outbox_canvas_result(
        outbox,
        "signed-canvas-command",
        record_hash=tampered["record_hash"],
        source_identity_hash=tampered["source_identity_hash"],
    )

    with pytest.raises(Exception, match="canvas.*receipt.*catalog"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox,
        )(NOW)


@pytest.mark.parametrize(
    "mutation",
    ["invalid_signature", "source_swap", "command_swap"],
)
def test_signal_source_rejects_tampered_canvas_publication_receipt(
    tmp_path: Path,
    mutation: str,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id=f"signed-canvas-{mutation}",
    )
    assert isinstance(receipt.result, dict)
    receipt_path = (
        catalog.parent
        / "canvas-publication-receipts"
        / f"{receipt.result['publication_receipt_id']}.json"
    )
    if mutation == "invalid_signature":
        payload = json.loads(receipt_path.read_text(encoding="utf-8"))
        payload["signature"] = "A" * 88
        receipt_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    elif mutation == "source_swap":
        claims = CanvasPublicationReceipt.model_validate_json(
            receipt_path.read_text(encoding="utf-8")
        ).claims
        command_payload = claims.command.model_dump(mode="json")
        command_payload["source"] = "attacker"
        _rewrite_publication_receipt(receipt_path, command=command_payload)
    else:
        claims = CanvasPublicationReceipt.model_validate_json(
            receipt_path.read_text(encoding="utf-8")
        ).claims
        command_payload = claims.command.model_dump(mode="json")
        command_payload["command_id"] = "attacker-command"
        _rewrite_publication_receipt(receipt_path, command=command_payload)

    with pytest.raises(Exception, match="canvas.*receipt"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox,
        )(NOW)


def test_signal_source_rejects_previous_key_signed_canvas_publication_receipt(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    authority = create_rotating_canvas_ed25519_test_authority(tmp_path / "keys")
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    data_dir = tmp_path / "page-data"
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            canvas_publication_signer=authority.previous_signer,
            canvas_publication_keyring=authority.previous_keyring,
        ),
    )
    receipt = service.submit(
        SaveCanvas(
            command_id="previous-key-projection",
            requested_at=NOW - timedelta(days=1),
            name="breakout",
            description="valid previous-key receipt",
            pool_refs=("n-shape-pool1",),
            source="canvas_page",
        )
    )
    assert receipt.status is PageControlStatus.SUCCEEDED
    assert isinstance(receipt.result, dict)
    publication_path = (
        data_dir
        / "canvas-publication-receipts"
        / f"{receipt.result['publication_receipt_id']}.json"
    )
    publication = CanvasPublicationReceipt.model_validate_json(
        publication_path.read_text(encoding="utf-8")
    )
    assert authority.keyring.verify_publication_receipt(publication)
    assert not authority.keyring.verify_publication_receipt(
        publication,
        require_active=True,
    )

    with pytest.raises(Exception, match="active|signature"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=data_dir / "canvases",
            canvas_receipt_root=data_dir / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox,
        )(NOW)


def test_signal_source_rejects_canvas_publication_receipt_symlink(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id="signed-canvas-symlink",
    )
    assert isinstance(receipt.result, dict)
    receipt_path = (
        catalog.parent
        / "canvas-publication-receipts"
        / f"{receipt.result['publication_receipt_id']}.json"
    )
    external = tmp_path / "external-receipt.json"
    external.write_bytes(receipt_path.read_bytes())
    receipt_path.unlink()
    receipt_path.symlink_to(external)

    with pytest.raises(Exception, match="canvas.*receipt.*(regular|symlink)"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox,
        )(NOW)


@pytest.mark.parametrize(
    "name,content",
    [
        pytest.param("bad.json", "[]", id="not-an-object"),
        pytest.param("wrong-name.json", '{"name":"other","pool_refs":[]}', id="name-mismatch"),
        # An explicit id keeps the 70 KB padding out of the nodeid.
        pytest.param(
            "too-large.json",
            '{"name":"too-large","description":"' + "x" * 70000 + '"}',
            id="oversize",
        ),
    ],
)
def test_signal_source_rejects_malformed_or_oversized_canvas_catalog_records(
    tmp_path: Path,
    name: str,
    content: str,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    catalog = tmp_path / "canvas-catalog"
    catalog.mkdir()
    (catalog / name).write_text(content, encoding="utf-8")
    outbox = PageControlOutbox(tmp_path / "malformed-page-control.sqlite3")
    authority = create_canvas_ed25519_test_authority(tmp_path / "malformed-keys")

    with pytest.raises(Exception, match="canvas.*(object|name|bounded|size|record)"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=tmp_path / "malformed-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox.path,
        )(NOW)


def test_signal_source_rejects_canvas_record_without_page_control_identity(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    catalog = tmp_path / "canvas-catalog"
    catalog.mkdir()
    (catalog / "breakout.json").write_text(
        json.dumps(
            {
                "name": "breakout",
                "description": "legacy",
                "pool_refs": ["n-shape-pool1"],
                "created_at": "2026-07-31T07:00:00Z",
                "updated_at": "2026-08-02T07:00:00Z",
                "source": "page_control",
            },
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    outbox = PageControlOutbox(tmp_path / "legacy-page-control.sqlite3")
    authority = create_canvas_ed25519_test_authority(tmp_path / "legacy-keys")

    with pytest.raises(Exception, match="canvas.*(identity|command|record)"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=tmp_path / "legacy-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox.path,
        )(NOW)


def test_signal_source_rejects_tampered_canvas_record_identity(tmp_path: Path) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt = _save_canvas_catalog_record(tmp_path)
    record_path = catalog / "breakout.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["source_identity_hash"] = "f" * 64
    record["record_hash"] = canonical_sha256(
        {key: value for key, value in record.items() if key != "record_hash"}
    )
    record_path.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(Exception, match="canvas.*identity"):
        _canvas_source(database, catalog=catalog, outbox=outbox)(NOW)


def test_signal_source_rejects_canvas_record_without_succeeded_page_control_receipt(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    _outbox, catalog, _command, _receipt = _save_canvas_catalog_record(tmp_path)
    empty_outbox = PageControlOutbox(tmp_path / "empty-page-control.sqlite3")

    with pytest.raises(Exception, match="PageControl.*receipt"):
        _canvas_source(database, catalog=catalog, outbox=empty_outbox)(NOW)


def test_signal_source_readonly_audit_refuses_missing_path_without_creating_it(
    tmp_path: Path,
) -> None:
    missing = tmp_path / "missing-page-control.sqlite3"

    with pytest.raises(PageProjectionSourceIntegrityError, match="does not exist"):
        DuckDBSignalPageProjectionSource(
            tmp_path / "unused.duckdb",
            page_control_outbox=missing,
        )

    assert not missing.exists()


def test_signal_source_readonly_audit_refuses_invalid_schema_without_mutation(
    tmp_path: Path,
) -> None:
    legacy = tmp_path / "legacy-page-control.sqlite3"
    with sqlite3.connect(legacy) as connection:
        connection.execute("CREATE TABLE page_control_command (command_id TEXT PRIMARY KEY)")
    before = legacy.read_bytes()

    with pytest.raises(PageProjectionSourceIntegrityError, match="schema"):
        DuckDBSignalPageProjectionSource(
            tmp_path / "unused.duckdb",
            page_control_outbox=legacy,
        )

    assert legacy.read_bytes() == before


def test_signal_source_readonly_audit_refuses_near_valid_unconstrained_schema(
    tmp_path: Path,
) -> None:
    near_valid = tmp_path / "near-valid-page-control.sqlite3"
    with sqlite3.connect(near_valid) as connection:
        connection.execute(
            """
            CREATE TABLE page_control_command (
                command_id TEXT,
                command_kind TEXT,
                command_hash TEXT,
                payload_json TEXT,
                status TEXT,
                enqueued_at TEXT,
                completed_at TEXT,
                result_json TEXT,
                error TEXT,
                processing_owner TEXT,
                lease_expires_at TEXT,
                attempt_count INTEGER,
                claim_token TEXT
            )
            """
        )
    before = near_valid.read_bytes()

    with pytest.raises(PageProjectionSourceIntegrityError, match="schema"):
        DuckDBSignalPageProjectionSource(
            tmp_path / "unused.duckdb",
            page_control_outbox=near_valid,
        )

    assert near_valid.read_bytes() == before


def test_signal_source_readonly_audit_refuses_any_inflight_command(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id="signed-canvas-with-inflight",
    )
    outbox.enqueue(
        DeleteCanvas(
            command_id="unrelated-inflight-command",
            requested_at=NOW,
            name="another-canvas",
        )
    )

    with pytest.raises(PageProjectionSourceIntegrityError, match="in-flight"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox.path,
        )(NOW)


@pytest.mark.parametrize("replacement", ["regular", "symlink"])
def test_signal_source_readonly_audit_rejects_path_replacement_after_construction(
    tmp_path: Path,
    replacement: str,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id=f"audit-path-{replacement}",
    )
    source = DuckDBSignalPageProjectionSource(
        database,
        canvas_catalog_root=catalog,
        canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
        canvas_publication_keyring=authority.keyring,
        page_control_outbox=outbox.path,
    )
    trusted = tmp_path / f"trusted-{replacement}.sqlite3"
    alternate = tmp_path / f"alternate-{replacement}.sqlite3"
    alternate.write_bytes(outbox.path.read_bytes())
    os.replace(outbox.path, trusted)
    if replacement == "regular":
        os.replace(alternate, outbox.path)
    else:
        outbox.path.symlink_to(alternate)

    with pytest.raises(
        PageProjectionSourceIntegrityError,
        match="rotated|regular non-symlink|generation",
    ):
        source(NOW)


def test_signal_source_readonly_audit_allows_same_inode_completed_mutation(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    data_dir = tmp_path / "page-data"
    authority = create_canvas_ed25519_test_authority(tmp_path / "page-keys")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    first = service.submit(
        SaveCanvas(
            command_id="same-inode-v1",
            requested_at=NOW - timedelta(days=2),
            name="breakout",
            description="v1",
        )
    )
    assert first.status is PageControlStatus.SUCCEEDED
    source = DuckDBSignalPageProjectionSource(
        database,
        canvas_catalog_root=data_dir / "canvases",
        canvas_receipt_root=data_dir / "canvas-publication-receipts",
        canvas_publication_keyring=authority.keyring,
        page_control_outbox=outbox.path,
    )
    second = service.submit(
        SaveCanvas(
            command_id="same-inode-v2",
            requested_at=NOW - timedelta(days=1),
            name="breakout",
            description="v2",
        )
    )
    assert second.status is PageControlStatus.SUCCEEDED

    snapshot = source(NOW)

    definition = {item.table_name: item for item in snapshot.projections}["canvas_definition"].rows[
        0
    ]
    assert definition["command_id"] == "same-inode-v2"


@pytest.mark.parametrize("effect_status", ["started", "failed"])
def test_signal_source_requires_matching_succeeded_page_control_effect(
    tmp_path: Path,
    effect_status: str,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id=f"effect-{effect_status}",
    )
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            """
            UPDATE page_control_effect
            SET status = ?, completed_at = NULL
            WHERE command_id = ?
            """,
            (effect_status, f"effect-{effect_status}"),
        )

    with pytest.raises(PageProjectionSourceIntegrityError, match="effect"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox.path,
        )(NOW)


def test_signal_source_ignores_non_authoritative_page_control_result_json(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, command, _receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id="signed-canvas-result-json",
    )
    _tamper_outbox_canvas_result(
        outbox,
        command.command_id,
        publication_receipt_id="f" * 64,
        publication_generation_id="e" * 64,
        record_hash="d" * 64,
    )

    snapshot = DuckDBSignalPageProjectionSource(
        database,
        canvas_catalog_root=catalog,
        canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
        canvas_publication_keyring=authority.keyring,
        page_control_outbox=outbox.path,
    )(NOW)

    definitions = {projection.table_name: projection for projection in snapshot.projections}[
        "canvas_definition"
    ]
    assert definitions.rows[0]["command_id"] == command.command_id


def test_signal_source_detects_command_entering_inflight_during_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id="signed-canvas-racing-inflight",
    )
    source = DuckDBSignalPageProjectionSource(
        database,
        canvas_catalog_root=catalog,
        canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
        canvas_publication_keyring=authority.keyring,
        page_control_outbox=outbox.path,
    )
    original = source._canvas_definitions

    def enqueue_during_projection(*, observed: datetime):
        outbox.enqueue(
            DeleteCanvas(
                command_id="racing-inflight",
                requested_at=NOW,
                name="another-canvas",
            )
        )
        return original(observed=observed)

    monkeypatch.setattr(source, "_canvas_definitions", enqueue_during_projection)

    with pytest.raises(PageProjectionSourceIntegrityError, match="in-flight|changed"):
        source(NOW)


def test_signal_source_rejects_canvas_record_command_hash_mismatch(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt = _save_canvas_catalog_record(tmp_path)
    _tamper_catalog_record(catalog / "breakout.json", command_hash="f" * 64)

    with pytest.raises(Exception, match="canvas.*receipt.*command hash"):
        _canvas_source(database, catalog=catalog, outbox=outbox)(NOW)


def test_signal_source_rejects_canvas_catalog_symlink(tmp_path: Path) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    catalog = tmp_path / "canvas-catalog"
    catalog.mkdir()
    external = tmp_path / "external.json"
    external.write_text('{"name":"escape","pool_refs":[]}', encoding="utf-8")
    (catalog / "escape.json").symlink_to(external)
    outbox = PageControlOutbox(tmp_path / "symlink-page-control.sqlite3")
    authority = create_canvas_ed25519_test_authority(tmp_path / "symlink-keys")

    with pytest.raises(Exception, match="canvas.*(regular|symlink|record)"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=tmp_path / "symlink-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox.path,
        )(NOW)


def test_signal_source_canvas_definition_update_and_delete_create_new_snapshots(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    data_dir = tmp_path / "page-data"
    authority = create_canvas_ed25519_test_authority(tmp_path / "page-keys")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    catalog = data_dir / "canvases"
    _CATALOG_AUTHORITIES[catalog] = authority

    def publish_definition(*, description: str, updated_at: str) -> str:
        receipt = service.submit(
            SaveCanvas(
                command_id=f"canvas-{description}",
                requested_at=datetime.fromisoformat(updated_at.replace("Z", "+00:00")),
                name="breakout",
                description=description,
                pool_refs=("n-shape-pool1",),
            )
        )
        assert receipt.status is PageControlStatus.SUCCEEDED
        snapshot = _canvas_source(database, catalog=catalog, outbox=outbox)(NOW)
        return snapshot.content_sha256

    first = publish_definition(description="v1", updated_at="2026-08-01T07:00:00Z")
    second = publish_definition(description="v2", updated_at="2026-08-02T07:00:00Z")
    deleted_receipt = service.submit(
        DeleteCanvas(
            command_id="delete-breakout",
            requested_at=datetime(2026, 8, 2, 8, 0, tzinfo=UTC),
            name="breakout",
        )
    )
    assert deleted_receipt.status is PageControlStatus.SUCCEEDED
    deleted = _canvas_source(database, catalog=catalog, outbox=outbox)(NOW)

    assert first != second
    definitions = {item.table_name: item for item in deleted.projections}["canvas_definition"]
    assert definitions.rows == ()


@pytest.mark.parametrize("after_delete", [False, True])
def test_signal_source_rejects_replayed_old_signed_canvas_version(
    tmp_path: Path,
    after_delete: bool,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    data_dir = tmp_path / "page-data"
    authority = create_canvas_ed25519_test_authority(tmp_path / "page-keys")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            consumer_service_id="page-control-test",
            consumer_id="page-control-instance-1",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    catalog_path = data_dir / "canvases" / "breakout.json"
    first = service.submit(
        SaveCanvas(
            command_id="canvas-v1",
            requested_at=NOW - timedelta(days=2),
            name="breakout",
            description="v1",
            pool_refs=("n-shape-pool1",),
        )
    )
    assert first.status is PageControlStatus.SUCCEEDED
    old_signed_catalog = catalog_path.read_bytes()
    second = service.submit(
        SaveCanvas(
            command_id="canvas-v2",
            requested_at=NOW - timedelta(days=1),
            name="breakout",
            description="v2",
            pool_refs=("n-shape-pool1", "user/strong"),
        )
    )
    assert second.status is PageControlStatus.SUCCEEDED
    if after_delete:
        deleted = service.submit(
            DeleteCanvas(
                command_id="canvas-delete",
                requested_at=NOW - timedelta(hours=1),
                name="breakout",
            )
        )
        assert deleted.status is PageControlStatus.SUCCEEDED
    catalog_path.write_bytes(old_signed_catalog)

    with pytest.raises(PageProjectionSourceIntegrityError, match="current head"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=data_dir / "canvases",
            canvas_receipt_root=data_dir / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox,
        )(NOW)


def test_signal_source_rejects_full_mutable_authority_rollback(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox_path = tmp_path / "page-control.sqlite3"
    outbox = PageControlOutbox(outbox_path)
    data_dir = tmp_path / "page-data"
    authority = create_canvas_ed25519_test_authority(tmp_path / "page-keys")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            clock=lambda: NOW,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    first = service.submit(
        SaveCanvas(
            command_id="rollback-v1",
            requested_at=NOW - timedelta(days=2),
            name="breakout",
            description="v1",
        )
    )
    assert first.status is PageControlStatus.SUCCEEDED
    backup = tmp_path / "v1-backup"
    backup.mkdir()
    for root_name in (
        "canvases",
        "canvas-publication-receipts",
        "canvas-publication-heads",
    ):
        shutil.copytree(data_dir / root_name, backup / root_name)
    shutil.copy2(outbox_path, backup / outbox_path.name)
    second = service.submit(
        SaveCanvas(
            command_id="rollback-v2",
            requested_at=NOW - timedelta(days=1),
            name="breakout",
            description="v2",
        )
    )
    assert second.status is PageControlStatus.SUCCEEDED
    for root_name in (
        "canvases",
        "canvas-publication-receipts",
        "canvas-publication-heads",
    ):
        os.replace(data_dir / root_name, tmp_path / f"detached-{root_name}")
        shutil.copytree(backup / root_name, data_dir / root_name)
    os.replace(outbox_path, tmp_path / "detached-page-control.sqlite3")
    shutil.copy2(backup / outbox_path.name, outbox_path)

    with pytest.raises(PageProjectionSourceIntegrityError, match="watermark|rollback"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=data_dir / "canvases",
            canvas_receipt_root=data_dir / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox_path,
        )(NOW)


def test_signal_source_rejects_future_signed_delete_receipt_and_normal_rebuild(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox_path = tmp_path / "page-control.sqlite3"
    outbox = PageControlOutbox(outbox_path)
    data_dir = tmp_path / "page-data"
    authority = create_canvas_ed25519_test_authority(tmp_path / "page-keys")
    future = NOW + timedelta(days=3650)
    future_clock_service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            clock=lambda: future,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    saved = future_clock_service.submit(
        SaveCanvas(
            command_id="future-delete-save",
            requested_at=NOW - timedelta(minutes=1),
            name="breakout",
        )
    )
    deleted = future_clock_service.submit(
        DeleteCanvas(
            command_id="future-delete",
            requested_at=future,
            name="breakout",
        )
    )
    assert saved.status is PageControlStatus.SUCCEEDED
    assert deleted.status is PageControlStatus.SUCCEEDED

    with pytest.raises(PageProjectionSourceIntegrityError, match="future"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=data_dir / "canvases",
            canvas_receipt_root=data_dir / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox_path,
        )(NOW)

    normal_clock_service = PageControlService(
        outbox=PageControlOutbox(outbox_path),
        consumer=PageControlConsumer(
            outbox=PageControlOutbox(outbox_path),
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            clock=lambda: NOW,
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    rebuilt = normal_clock_service.submit(
        SaveCanvas(
            command_id="normal-rebuild-after-future-delete",
            requested_at=NOW,
            name="breakout",
        )
    )
    assert rebuilt.status is PageControlStatus.FAILED
    assert "newer" in (rebuilt.error or "") or "future" in (rebuilt.error or "")


def test_signal_source_rejects_removed_catalog_and_head_authority(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
        tmp_path,
        command_id="signed-canvas-removed-authority",
    )
    os.replace(catalog, tmp_path / "detached-canvases")
    os.replace(
        catalog.parent / "canvas-publication-heads",
        tmp_path / "detached-canvas-heads",
    )

    with pytest.raises(PageProjectionSourceIntegrityError, match="head authority is missing"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox.path,
        )(NOW)


@pytest.mark.parametrize("after_delete", [False, True])
def test_canvas_head_suffix_deletion_blocks_projection_and_subsequent_update(
    tmp_path: Path,
    after_delete: bool,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")
    data_dir = tmp_path / "page-data"
    authority = create_canvas_ed25519_test_authority(tmp_path / "page-keys")
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=data_dir,
            log_dir=tmp_path / "page-logs",
            canvas_publication_signer=authority.signer,
            canvas_publication_keyring=authority.keyring,
        ),
    )
    catalog_path = data_dir / "canvases" / "breakout.json"
    first = service.submit(
        SaveCanvas(
            command_id="suffix-v1",
            requested_at=NOW - timedelta(days=2),
            name="breakout",
            description="v1",
        )
    )
    assert first.status is PageControlStatus.SUCCEEDED
    old_catalog = catalog_path.read_bytes()
    second = service.submit(
        SaveCanvas(
            command_id="suffix-v2",
            requested_at=NOW - timedelta(days=1),
            name="breakout",
            description="v2",
        )
    )
    assert second.status is PageControlStatus.SUCCEEDED
    if after_delete:
        deleted = service.submit(
            DeleteCanvas(
                command_id="suffix-delete",
                requested_at=NOW - timedelta(hours=12),
                name="breakout",
            )
        )
        assert deleted.status is PageControlStatus.SUCCEEDED
    head_files = tuple((data_dir / "canvas-publication-heads" / "breakout").glob("*.json"))

    def head_sequence(path: Path) -> int:
        publication = CanvasPublicationReceipt.model_validate_json(path.read_text(encoding="utf-8"))
        payload = json.loads(publication.claims.command.description)
        return int(payload["sequence"])

    newest = max(head_files, key=head_sequence)
    os.replace(newest, tmp_path / "detached-newest-head.json")
    catalog_path.write_bytes(old_catalog)

    with pytest.raises(
        PageProjectionSourceIntegrityError,
        match="exact current head|latest|authority",
    ):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=data_dir / "canvases",
            canvas_receipt_root=data_dir / "canvas-publication-receipts",
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=outbox.path,
        )(NOW)
    update = service.submit(
        SaveCanvas(
            command_id="suffix-v3",
            requested_at=NOW,
            name="breakout",
            description="must not derive from rolled-back v1",
        )
    )
    assert update.status is PageControlStatus.FAILED
    assert update.error is not None
    assert "current head" in update.error


@pytest.mark.parametrize("signed_catalog", [False, True])
def test_signal_source_configured_canvas_root_requires_readonly_audit_authority(
    tmp_path: Path,
    signed_catalog: bool,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    if signed_catalog:
        _outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
            tmp_path,
            command_id="configured-root-no-audit",
        )
        receipt_root = catalog.parent / "canvas-publication-receipts"
    else:
        catalog = tmp_path / "configured-empty-canvases"
        catalog.mkdir()
        authority = create_canvas_ed25519_test_authority(tmp_path / "empty-keys")
        receipt_root = tmp_path / "empty-receipts"

    with pytest.raises(PageProjectionSourceIntegrityError, match="audit authority"):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=receipt_root,
            canvas_publication_keyring=authority.keyring,
            page_control_outbox=None,
        )(NOW)


@pytest.mark.parametrize("missing_authority", ["receipt_root", "keyring"])
def test_signal_source_configured_canvas_root_requires_complete_receipt_authority(
    tmp_path: Path,
    missing_authority: str,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    catalog = tmp_path / "configured-empty-canvases"
    catalog.mkdir()
    receipt_root = tmp_path / "canvas-publication-receipts"
    authority = create_canvas_ed25519_test_authority(tmp_path / "canvas-keys")
    outbox = PageControlOutbox(tmp_path / "page-control.sqlite3")

    with pytest.raises(
        PageProjectionSourceIntegrityError,
        match="receipt|keyring|authority",
    ):
        DuckDBSignalPageProjectionSource(
            database,
            canvas_catalog_root=catalog,
            canvas_receipt_root=(None if missing_authority == "receipt_root" else receipt_root),
            canvas_publication_keyring=(
                None if missing_authority == "keyring" else authority.keyring
            ),
            page_control_outbox=outbox.path,
        )(NOW)


def test_signal_projection_producer_persists_complete_notification_authority(
    tmp_path: Path,
) -> None:
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(
        source=DuckDBSignalPageProjectionSource(database),
        store=store,
    )

    receipt = producer.publish(NOW)
    serving = store.serving_snapshot(observed_at=NOW, history_limit=10)

    assert receipt.generation_id == serving.projection_generation_id
    assert set(serving.projection_source_receipts) == {
        "signal-companion-projections",
        "signal-page-projections",
    }
    assert {item.table_name for item in serving.payload.projections} >= {
        "screen_bounds",
        "minute_coverage",
        "canvas_diagnostic",
        "canvas_latest_trade_date",
        "canvas_hit",
        "canvas_definition",
    }


def test_duckdb_lab_source_publishes_explicit_empty_gate_projection(tmp_path: Path) -> None:
    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass

    snapshot = DuckDBLabPageProjectionSource(database)(NOW)

    assert len(snapshot.projections) == 1
    assert snapshot.projections[0].table_name == "research_gate_metadata"
    assert snapshot.projections[0].rows == ()


def test_duckdb_lab_source_reuses_bound_generation_for_research_gate_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "research_ro.duckdb"
    attacker = tmp_path / "attacker-research.duckdb"
    with DuckDBStore(database), DuckDBStore(attacker):
        pass
    original_connect = duckdb.connect
    attacker_swaps = 0

    def swap_only_during_original_path_connect(
        path: str,
        *args: object,
        **kwargs: object,
    ):
        nonlocal attacker_swaps
        attacker_swaps += 1
        trusted_hold = tmp_path / "trusted-research-held.duckdb"
        os.replace(database, trusted_hold)
        os.replace(attacker, database)
        try:
            connection = original_connect(path, *args, **kwargs)
        finally:
            os.replace(database, attacker)
            os.replace(trusted_hold, database)
        return connection

    monkeypatch.setattr(duckdb, "connect", swap_only_during_original_path_connect)

    snapshot = DuckDBLabPageProjectionSource(database)(NOW)

    assert snapshot.projections[0].rows == ()
    assert attacker_swaps >= 2


def test_signal_page_projection_snapshot_builds_complete_bounded_contract() -> None:
    snapshot = SignalPageProjectionSnapshot.create(
        available_at=NOW,
        screen_bounds=(
            ScreenBoundsProjectionRow(
                preset_name="n-shape-pool1",
                min_date=date(2026, 7, 1),
                max_date=date(2026, 7, 31),
                candidate_count=12,
            ),
        ),
        minute_coverage=(
            MinuteCoverageProjectionRow(
                is_total=True,
                source="all",
                rows_count=240,
                codes_count=1,
                trade_dates=1,
                min_time=NOW - timedelta(days=3),
                max_time=NOW - timedelta(days=3, hours=-6),
            ),
        ),
        canvas_diagnostics=(
            CanvasDiagnosticProjectionRow(
                trade_date=date(2026, 7, 31),
                preset_name="n-shape-pool1",
                step_index=0,
                rule_label="all",
                remaining_count=1,
            ),
        ),
        canvas_latest_trade_date=CanvasLatestTradeDateProjectionRow(trade_date=date(2026, 7, 31)),
        canvas_hits=(
            CanvasHitProjectionRow(
                trade_date=date(2026, 7, 31),
                preset_name="n-shape-pool1",
                ts_code="600000.SH",
                row_json='{"ts_code":"600000.SH"}',
            ),
        ),
    )

    assert {item.table_name for item in snapshot.projections} == {
        "screen_bounds",
        "minute_coverage",
        "canvas_diagnostic",
        "canvas_latest_trade_date",
        "canvas_hit",
        "canvas_definition",
    }
    assert snapshot.content_sha256


def test_signal_page_projection_rejects_future_coverage() -> None:
    with pytest.raises(ValueError, match="future timestamp"):
        SignalPageProjectionSnapshot.create(
            available_at=NOW,
            minute_coverage=(
                MinuteCoverageProjectionRow(
                    is_total=True,
                    source="all",
                    rows_count=1,
                    codes_count=1,
                    trade_dates=1,
                    min_time=NOW,
                    max_time=NOW + timedelta(seconds=1),
                ),
            ),
        )


def test_lab_page_projection_serializes_research_gate_metadata() -> None:
    snapshot = LabPageProjectionSnapshot.create(
        available_at=NOW,
        rows=(
            ResearchGateProjectionRow(
                strategy_name="n_shape",
                range_start=date(2026, 7, 1),
                range_end=date(2026, 7, 31),
                as_of_time=NOW - timedelta(days=1),
                completed_at=NOW - timedelta(hours=1),
                code_commit=COMMIT,
                audit_run_id="audit-1",
                dataset_snapshot_id="snapshot-1",
                dataset_binding_hash="binding-1",
                coverage_ratios={"minute": 0.9},
                coverage_counts={"minute": (90, 100)},
                failures=(ResearchGateFailure(code="sample_warning", message="sample warning"),),
                metadata_ready=False,
            ),
        ),
    )

    projection = snapshot.projections[0]
    assert projection.table_name == "research_gate_metadata"
    assert projection.rows[0]["coverage_ratios_json"] == '{"minute":0.9}'
    assert '"sample_warning"' in str(projection.rows[0]["failures_json"])


# ---------------------------------------------------------------------------------------
# #241: the PageControl audit reader inside the notifier unit's sandbox
# ---------------------------------------------------------------------------------------
#
# `rquant-runtime-notifier@svc-f2518f7a….service` was DEGRADED on every iteration of the
# 2026-09-08 Route A window:
#
#     OSError: [Errno 30] Read-only file system:
#         '…/data/runtime/control/.page-control.sqlite3.0975d9…b2.b3xxs1w1'
#
# That name was this class's own: `snapshot()` pinned the generation it was about to read
# by hard-linking the outbox into a temporary directory it created *beside* the outbox,
# and the outbox belongs to `rquant-page-control.service`.
#
# Moving that scratch directory into a directory the notifier owns is NOT the fix, and
# this file is where that was first believed. systemd builds every `ReadWritePaths=` and
# `ReadOnlyPaths=` entry as its own bind mount, and Linux `link()` refuses across mounts
# (`do_linkat` compares `mnt`, not the superblock), so the same call would have come back
# as `EXDEV` on the host while passing in a test that runs in one temporary directory.
# The reader now pins with an open descriptor and writes nothing at all, which is the only
# shape that survives a unit model where "the file I read" and "the directory I may write"
# are always two different mounts.


def test_the_audit_reader_writes_nothing_anywhere(tmp_path: Path) -> None:
    """The property that makes the mount topology irrelevant: no writes, at all.

    Not "no writes in the outbox's directory" -- nowhere. A reader that creates nothing
    cannot be defeated by which mount its scratch would have landed on, and this is the
    assertion neither hard-link version could have passed on a host: beside the outbox is
    `EROFS`, and into a directory the notifier owns is `EXDEV`, because systemd makes every
    granted path its own bind mount.
    """

    from tests.runtime_readonly_sandbox import readonly_runtime, tree_state

    control = tmp_path / "control"
    control.mkdir()
    outbox, _catalog, _command, _receipt, _authority = _save_signed_canvas_catalog_record(
        control,
        command_id="sandboxed-canvas",
    )
    reader = _ReadonlyPageControlAuditReader(Path(outbox.path))
    before = tree_state(tmp_path)

    #: nothing is writable -- not the outbox's directory, not a scratch root, nothing
    with readonly_runtime(tmp_path, writable=()) as violations, reader.snapshot():
        mutations = reader.canvas_mutations()

    assert violations == [], violations
    assert tree_state(tmp_path) == before
    assert mutations is not None


def test_the_audit_reader_never_links_the_outbox(tmp_path: Path) -> None:
    """#241's real shape, guarded where a test without mount namespaces can see it.

    `os.link` is what returned `EXDEV` on the host. The reader must not call it -- not
    beside the outbox, not into a directory of its own, not anywhere -- because there is
    no target on a runtime host that is on the same mount as the file it reads.
    """

    control = tmp_path / "control"
    control.mkdir()
    database = tmp_path / "rquant_ro.duckdb"
    _signal_projection_database(database)
    outbox, catalog, _command, _receipt, authority = _save_signed_canvas_catalog_record(
        control,
        command_id="never-links",
    )
    source = DuckDBSignalPageProjectionSource(
        database,
        canvas_catalog_root=catalog,
        canvas_receipt_root=catalog.parent / "canvas-publication-receipts",
        canvas_publication_keyring=authority.keyring,
        page_control_outbox=outbox,
    )
    del source
    reader = _ReadonlyPageControlAuditReader(Path(outbox.path))
    attempted: list[tuple[str, str]] = []
    real_link = os.link

    def refuse_link(src, dst, *args, **kwargs):  # type: ignore[no-untyped-def]
        attempted.append((str(src), str(dst)))
        raise OSError(errno.EXDEV, "Invalid cross-device link", str(src))

    os.link = refuse_link  # type: ignore[assignment]
    try:
        with reader.snapshot():
            mutations = reader.canvas_mutations()
    finally:
        os.link = real_link  # type: ignore[assignment]

    assert attempted == []
    assert mutations is not None


def test_the_pinned_generation_is_the_one_the_reader_opened(tmp_path: Path) -> None:
    """A rotation under the reader's feet does not change the bytes it already reads.

    This is what the hard link was for. The descriptor gives the same guarantee: the
    connection holds the inode, so replacing the *name* leaves the open read alone -- and
    the reader still refuses the snapshot, because a generation that rotated mid-read is
    not the quiescent one it promised to project.
    """

    control = tmp_path / "control"
    control.mkdir()
    outbox, _catalog, _command, _receipt, _authority = _save_signed_canvas_catalog_record(
        control,
        command_id="rotating-canvas",
    )
    reader = _ReadonlyPageControlAuditReader(Path(outbox.path))
    replacement = control / "replacement.sqlite3"
    shutil.copyfile(outbox.path, replacement)

    with (
        pytest.raises(PageProjectionSourceIntegrityError, match="changed or entered"),
        reader.snapshot(),
    ):
        #: the same rows are still readable through the pinned descriptor
        assert reader.canvas_mutations() is not None
        os.replace(replacement, outbox.path)



def test_the_duckdb_reader_pins_through_a_descriptor_where_the_engine_takes_one(
    tmp_path: Path,
) -> None:
    """Both openers, asserted -- which one runs is a platform fact, not a surprise.

    On Linux `duckdb.connect("/proc/self/fd/<n>")` opens the inode the descriptor holds,
    so the reader pins the generation and creates nothing: that is the branch package L
    expected a runtime host to take. macOS DuckDB rebuilds the path from the descriptor's
    real name and refuses -- and so does the build on the production host (#255), which is
    why the fallback may no longer write beside the database. It writes a private copy in
    the reader's own control root instead, and never in the database's directory.
    """

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB
    from tests.runtime_readonly_sandbox import tree_state

    replica = tmp_path / "replica"
    replica.mkdir()
    control = tmp_path / "control"
    database = replica / "rquant_ro.duckdb"
    _signal_projection_database(database)
    before = tree_state(replica)
    reader = _StableReadonlyDuckDB(database, control_root=control, atomically_published=True)

    with reader as connection:
        assert connection.execute("SELECT count(*) FROM screen_result").fetchone()[0] >= 1
        opened_through = reader.opened_through
        inside = tree_state(replica)

    assert opened_through in {"descriptor", "copy"}
    #: whichever branch ran, the database's own directory is untouched throughout
    assert inside == before, "nothing may be created beside the database"
    assert tree_state(replica) == before
    #: and the copy, if there was one, does not survive the read either
    assert not control.exists() or not any(control.iterdir())


def test_the_lab_page_source_pins_the_same_way(tmp_path: Path) -> None:
    """#241's third site: the lab jobs role reads a replica under a read-only `research/`.

    `runtime_builder_authority` builds `DuckDBLabPageProjectionSource` for
    `lab_jobs_publisher`, whose unit grants `control/lab-jobs-publishers/%i` and its
    serving authority and nothing else -- `research/` is read-only. The reader used to
    create its scratch beside the replica there; it now takes the same descriptor path as
    the signal reader, so on a host it writes nothing.
    """

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    database = tmp_path / "research_ro.duckdb"
    with DuckDBStore(database):
        pass
    source = DuckDBLabPageProjectionSource(database)

    assert not hasattr(source, "generation_bind_root")
    with _StableReadonlyDuckDB(database) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)


# ---------------------------------------------------------------------------------------
# #255: the engine on the production host refuses the descriptor path too
# ---------------------------------------------------------------------------------------


def _engine_that_refuses_descriptors(monkeypatch: pytest.MonkeyPatch) -> None:
    """The DuckDB build on the production host (and on macOS): no `/proc/self/fd/<n>`."""

    original = duckdb.connect

    def refuse_descriptor_paths(path: str, *args: object, **kwargs: object):
        if str(path).startswith(("/proc/self/fd/", "/dev/fd/")):
            raise duckdb.IOException(f"cannot open {path}")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", refuse_descriptor_paths)


def test_a_refused_descriptor_copies_into_the_readers_own_control_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#255: `notifier.admin.shadow.v1` DEGRADED every iteration on `errno 30 EROFS`.

    Package L kept a link-beside-the-database branch for engines that refuse the
    descriptor, on the assumption that only macOS does. The build on the host refuses it
    too, and `data/` is read-only for that unit, so the link failed on every iteration.
    The copy goes where the role already writes its own state and nothing at all is
    created beside the database.
    """

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB
    from tests.runtime_readonly_sandbox import tree_state

    _engine_that_refuses_descriptors(monkeypatch)
    replica = tmp_path / "data"
    replica.mkdir()
    database = replica / "rquant_ro.duckdb"
    _signal_projection_database(database)
    control = tmp_path / "notifications" / "svc"
    before = tree_state(replica)

    reader = _StableReadonlyDuckDB(database, control_root=control, atomically_published=True)
    with reader as connection:
        assert connection.execute("SELECT count(*) FROM screen_result").fetchone()[0] >= 1
        assert reader.opened_through == "copy"
        assert Path(reader.generation_path).is_relative_to(control)
        assert tree_state(replica) == before, "nothing may be created beside the database"

    assert tree_state(replica) == before
    assert not any(control.iterdir()), "the copy does not survive the read"


def test_a_generation_over_the_copy_cap_is_read_in_place(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ~10 GB production replica: copying it is not a pinning mechanism, it is a stall.

    `max_copy_bytes` is what says so. Above it the reader opens the database where it is,
    pins nothing, and leans on the identity check `__exit__` already performs.
    """

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB
    from tests.runtime_readonly_sandbox import tree_state

    _engine_that_refuses_descriptors(monkeypatch)
    replica = tmp_path / "data"
    replica.mkdir()
    database = replica / "rquant_ro.duckdb"
    _signal_projection_database(database)
    control = tmp_path / "notifications" / "svc"
    before = tree_state(replica)

    reader = _StableReadonlyDuckDB(
        database,
        control_root=control,
        max_copy_bytes=1,
        atomically_published=True,
    )
    with reader as connection:
        assert connection.execute("SELECT count(*) FROM screen_result").fetchone()[0] >= 1
        assert reader.opened_through == "in_place"
        assert reader.generation_path == str(database)

    assert tree_state(replica) == before
    assert not control.exists() or not any(control.iterdir())


def test_an_in_place_read_reports_a_generation_that_moved_under_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No pinning is not no check: a replaced generation is named, not quietly served."""

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    _engine_that_refuses_descriptors(monkeypatch)
    replica = tmp_path / "data"
    replica.mkdir()
    database = replica / "rquant_ro.duckdb"
    replacement = replica / "next.duckdb"
    _signal_projection_database(database)
    _signal_projection_database(replacement)

    reader = _StableReadonlyDuckDB(database, control_root=None)
    with pytest.raises(PageProjectionSourceIntegrityError, match="rotated"), reader as connection:
        assert reader.opened_through == "in_place"
        assert connection.execute("SELECT count(*) FROM screen_result").fetchone()[0] >= 1
        os.replace(replacement, database)


def test_nothing_is_ever_linked_beside_the_database_again(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The branch #255 is about is gone, not merely unused: `os.link` is never called."""

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    _engine_that_refuses_descriptors(monkeypatch)

    def refuse_link(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("the reader must never hard-link a generation")

    monkeypatch.setattr(os, "link", refuse_link)
    replica = tmp_path / "data"
    replica.mkdir()
    database = replica / "rquant_ro.duckdb"
    _signal_projection_database(database)

    for control in (tmp_path / "control", None):
        with _StableReadonlyDuckDB(
            database,
            control_root=control,
            atomically_published=control is not None,
        ) as connection:
            assert connection.execute("SELECT 1").fetchone() == (1,)


# ---------------------------------------------------------------------------------------
# Review MF-3 / SF-1: a write lock is a write lock, and a copy must never serve stale rows
# ---------------------------------------------------------------------------------------


def _write_locked_child(database: Path, ready: Path) -> subprocess.Popen[str]:
    """Another process holding DuckDB's write lock on `database`, with a row uncommitted.

    This is `rquant-monitor` from 09:25 (#250), which is what the notifier actually meets
    on the host: `page_projection_database_path` is the production **main** database
    (`runtime_production_profile.py:1521`), not a replica.
    """

    import sys

    program = f"""
import duckdb, pathlib, time
connection = duckdb.connect({str(database)!r})
connection.execute("CREATE TABLE IF NOT EXISTS probe (a INTEGER)")
connection.execute("INSERT INTO probe VALUES (1)")
connection.execute("CHECKPOINT")
connection.execute("INSERT INTO probe VALUES (2)")
pathlib.Path({str(ready)!r}).write_text("held")
time.sleep(60)
"""
    child = subprocess.Popen(
        [sys.executable, "-c", program],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    for _ in range(200):
        if ready.exists():
            return child
        time.sleep(0.05)
    child.kill()
    child.communicate()
    raise AssertionError("the write-lock holder never started")


def test_a_write_locked_database_is_reported_as_a_lock_not_as_a_refused_path(
    tmp_path: Path,
) -> None:
    """Review MF-3: #255's message named the wrong cause because two failures were one.

    v0.33.5 reached the fallback through a bare `except Exception`, so "another process
    holds the write lock" and "this engine does not understand this path" produced the
    same sentence -- and the window read that sentence as the engine's fault. With the
    pinned duckdb 1.5.2 the descriptor path is accepted on Linux, so on the host the
    likelier cause is the lock `rquant-monitor` holds from 09:25 (#250).

    The classifier is fed a real exception from the real engine (the message wording is
    the whole point), and the reader as a whole is asserted to refuse a write-locked
    database however it gets there -- which branch raises is a platform fact.
    """

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    database = tmp_path / "rquant.duckdb"
    ready = tmp_path / "held"
    child = _write_locked_child(database, ready)
    try:
        with pytest.raises(Exception) as engine:  # noqa: PT011 - duckdb's own type
            duckdb.connect(str(database), read_only=True)
        with pytest.raises(PageProjectionSourceIntegrityError, match="write lock"):
            _StableReadonlyDuckDB._refuse_if_write_locked(
                engine.value,
                opened_path="/proc/self/fd/6",
            )

        reader = _StableReadonlyDuckDB(
            database,
            control_root=tmp_path / "control",
            atomically_published=True,
        )
        with pytest.raises(PageProjectionSourceIntegrityError), reader:
            pass  # pragma: no cover - the open is what raises
    finally:
        child.kill()
        child.communicate()

    #: and an error that is *not* a lock still falls through to the next opener
    _StableReadonlyDuckDB._refuse_if_write_locked(
        RuntimeError("no such file or directory"),
        opened_path="/dev/fd/6",
    )


def test_a_live_writer_never_gets_a_byte_copy_served_as_a_pinned_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review SF-1: the branch that replaced the hard link must fail closed where it did.

    A hard link shares the inode, so it shared the lock and refused. A byte copy of the
    main file does not: it opens cleanly and returns the **last checkpoint** -- older rows,
    with nothing to say they are old. Here the engine is forced to refuse the descriptor
    path, which is exactly the situation that used to reach the copy.
    """

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    database = tmp_path / "rquant.duckdb"
    ready = tmp_path / "held"
    child = _write_locked_child(database, ready)
    try:
        _engine_that_refuses_descriptors(monkeypatch)
        reader = _StableReadonlyDuckDB(
            database,
            control_root=tmp_path / "control",
            atomically_published=True,
        )
        with pytest.raises(PageProjectionSourceIntegrityError) as raised, reader:
            pass  # pragma: no cover - the open is what raises
    finally:
        child.kill()
        child.communicate()

    #: either half of the guard is a refusal; what must never happen is a connection
    assert reader.connection is None
    assert "write-ahead log" in str(raised.value) or "write lock" in str(raised.value)


def test_the_copy_branch_is_off_unless_the_owner_publishes_by_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review SF-1: copying is only safe for an artifact nobody writes in place."""

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    _engine_that_refuses_descriptors(monkeypatch)
    replica = tmp_path / "data"
    replica.mkdir()
    database = replica / "rquant_ro.duckdb"
    _signal_projection_database(database)
    control = tmp_path / "control"

    with _StableReadonlyDuckDB(database, control_root=control) as connection:
        assert connection.execute("SELECT 1").fetchone() == (1,)
    #: not stated, not copied
    assert not control.exists() or not any(control.iterdir())

    reader = _StableReadonlyDuckDB(database, control_root=control, atomically_published=True)
    with reader:
        assert reader.opened_through == "copy"


def test_a_source_touched_during_the_copy_is_refused_rather_than_served(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review SF-1: an in-place write that leaves the size alone still has to be caught."""

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    _engine_that_refuses_descriptors(monkeypatch)
    replica = tmp_path / "data"
    replica.mkdir()
    database = replica / "rquant_ro.duckdb"
    _signal_projection_database(database)
    control = tmp_path / "control"
    reader = _StableReadonlyDuckDB(database, control_root=control, atomically_published=True)

    real_copy = _StableReadonlyDuckDB._copy_descriptor

    def copy_then_touch(self: object, destination: Path, *, size: int) -> None:
        real_copy(self, destination, size=size)
        #: the same bytes, the same length, a different moment
        payload = database.read_bytes()
        database.write_bytes(payload)

    monkeypatch.setattr(_StableReadonlyDuckDB, "_copy_descriptor", copy_then_touch)

    with pytest.raises(PageProjectionSourceIntegrityError, match="changed while"), reader:
        pass  # pragma: no cover - the open is what raises
    assert not any(control.iterdir())


def test_a_source_whose_only_changed_stamp_is_ctime_is_still_refused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review SF-7: the previous case moved mtime too, so it could not see `st_ctime_ns` work.

    A writer that rewrites a page in place leaves the size and the inode alone, and mtime
    can be put back -- deliberately by a tool that preserves timestamps, or accidentally
    by a filesystem whose mtime granularity is coarser than the write. `st_ctime_ns` is
    the stamp no unprivileged caller can restore, and it is the reason it is in
    `_copy_identity` at all. Here everything else is held identical and only ctime moves.
    """

    from rquant.serving_page_projection_source import _StableReadonlyDuckDB

    _engine_that_refuses_descriptors(monkeypatch)
    replica = tmp_path / "data"
    replica.mkdir()
    database = replica / "rquant_ro.duckdb"
    _signal_projection_database(database)
    control = tmp_path / "control"
    before = database.lstat()
    reader = _StableReadonlyDuckDB(database, control_root=control, atomically_published=True)

    real_copy = _StableReadonlyDuckDB._copy_descriptor

    def copy_then_move_only_ctime(self: object, destination: Path, *, size: int) -> None:
        real_copy(self, destination, size=size)
        #: the same bytes into the same inode, then mtime handed back -- so `st_dev`,
        #: `st_ino`, `st_size` and `st_mtime_ns` are all exactly what they were
        database.write_bytes(database.read_bytes())
        os.utime(database, ns=(before.st_atime_ns, before.st_mtime_ns))

    monkeypatch.setattr(_StableReadonlyDuckDB, "_copy_descriptor", copy_then_move_only_ctime)

    with pytest.raises(PageProjectionSourceIntegrityError, match="changed while"), reader:
        pass  # pragma: no cover - the open is what raises

    after = database.lstat()
    #: the premise of the case: only `st_ctime_ns` moved
    assert (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns) == (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    assert after.st_ctime_ns != before.st_ctime_ns
    assert not any(control.iterdir())



def _count_database_opens(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """How many times the page projection actually opened the replica (#256)."""

    import rquant.serving_page_projection_source as module

    opens = [0]
    original = module._StableReadonlyDuckDB.__enter__

    def counted(self: object) -> object:
        opens[0] += 1
        return original(self)

    monkeypatch.setattr(module._StableReadonlyDuckDB, "__enter__", counted)
    return opens


def _synced_replica(path: Path, *, synced_at: datetime) -> None:
    """The replica as the five-minute timer leaves it: written before anyone reads it."""

    _signal_projection_database(path)
    stamp = synced_at.timestamp()
    os.utime(path, (stamp, stamp))


def test_an_unchanged_replica_is_not_opened_again_by_the_page_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#256: the notifier's interval is two seconds and the replica changes every five minutes.

    Every iteration used to scan all of `minute_bar` in a 10 GB file. Three iterations
    over one generation now open it once, and the answer is the same one.
    """

    database = tmp_path / "rquant_ro.duckdb"
    _synced_replica(database, synced_at=NOW - timedelta(minutes=1))
    opens = _count_database_opens(monkeypatch)
    source = DuckDBSignalPageProjectionSource(database)

    snapshots = [source(NOW + timedelta(seconds=2 * index)) for index in range(3)]

    assert opens[0] == 1
    assert source.last_replica_read is not None
    assert source.last_replica_read.opened is False
    assert {snapshot.content_sha256 for snapshot in snapshots} == {snapshots[0].content_sha256}


def test_the_page_projection_opens_exactly_once_more_after_the_replica_is_replaced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A `mv` over the name is what `sync-readonly-replica.sh` does every five minutes."""

    database = tmp_path / "rquant_ro.duckdb"
    replacement = tmp_path / "rquant_ro.duckdb.tmp.1"
    _synced_replica(database, synced_at=NOW - timedelta(minutes=6))
    _synced_replica(replacement, synced_at=NOW - timedelta(minutes=1))
    opens = _count_database_opens(monkeypatch)
    source = DuckDBSignalPageProjectionSource(database)

    source(NOW)
    source(NOW + timedelta(seconds=2))
    os.replace(replacement, database)
    after = [source(NOW + timedelta(seconds=4 + 2 * index)) for index in range(3)]

    assert opens[0] == 2
    assert len(after) == 3
    assert source.last_replica_read is not None
    assert source.last_replica_read.opened is False


def test_an_answer_taken_before_the_generation_was_written_is_never_reused(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The reuse rule is "at or after the file's own mtime", and it is load-bearing.

    A replica written after this iteration started may hold rows this iteration's cutoff
    excluded; the next one must ask again rather than be served the narrower answer.
    """

    database = tmp_path / "rquant_ro.duckdb"
    _synced_replica(database, synced_at=NOW + timedelta(minutes=1))
    opens = _count_database_opens(monkeypatch)
    source = DuckDBSignalPageProjectionSource(database)

    source(NOW)
    source(NOW + timedelta(seconds=2))

    assert opens[0] == 2


def test_what_is_not_in_the_replica_is_still_read_every_iteration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """What is cached is the replica's part, not the projection.

    `surge_live`, the canvas catalog and the PageControl audit are files that change on
    their own; freezing them with the database would delay them by up to a replica period.
    """

    database = tmp_path / "rquant_ro.duckdb"
    _synced_replica(database, synced_at=NOW - timedelta(minutes=1))
    live_root = tmp_path / "surge_live"
    opens = _count_database_opens(monkeypatch)
    source = DuckDBSignalPageProjectionSource(database, surge_live_root=live_root)

    before = source(NOW)
    live_root.mkdir()
    alerts = live_root / "pulse_alerts-2026-08-03.jsonl"
    alerts.write_text(
        json.dumps(
            {
                "t": "10:15",
                "kind": "broken_surge",
                "kind_label": "炸板潮",
                "before": 2.0,
                "after": 6.0,
                "window_minutes": 10,
                "message": "炸板异动",
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    stamp = (NOW - timedelta(seconds=1)).timestamp()
    os.utime(alerts, (stamp, stamp))
    after = source(NOW + timedelta(seconds=2))

    assert opens[0] == 1
    assert "pulse_alert" not in {item.table_name for item in before.projections}
    assert "pulse_alert" in {item.table_name for item in after.projections}
