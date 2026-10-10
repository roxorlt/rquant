from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.notification_state import NotificationStateStore
from rquant.page_control import PageControlOutbox
from rquant.serving_manual_watchlist_projection import (
    build_manual_watchlist_projections,
    validate_manual_watchlist_projections,
)
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    PageProjectionSourceIntegrityError,
    SignalPageProjectionProducer,
    _ReadonlyPageControlAuditReader,
)
from rquant.serving_read_models import ServingProjectionPayload
from tests.unit.test_formula_pool_save_core import _setup
from tests.unit.test_formula_pool_serving import _config
from tests.unit.test_serving_page_projection_source import _signal_projection_database

NOW = datetime(2026, 9, 28, 4, 0, tzinfo=UTC)
ACTIVATED = NOW - timedelta(days=1)


def _rows(path: Path, *rows: tuple[object, ...]) -> None:
    with sqlite3.connect(path) as connection:
        connection.executemany(
            "INSERT INTO manual_watchlist "
            "(owner_id, ts_code, version, deleted, source, price_levels_json, "
            "expires_at_utc, updated_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def _by_name(
    projections: tuple[ServingProjectionPayload, ...],
) -> dict[str, ServingProjectionPayload]:
    return {item.table_name: item for item in projections}


def test_unactivated_and_activated_empty_are_distinct(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        assert reader.manual_watchlist_snapshot() is None
    unavailable = _by_name(build_manual_watchlist_projections(None, observed_at=NOW))
    assert unavailable["manual_watchlist_state"].rows[0]["state"] == "unavailable"
    assert "manual_watchlist" not in unavailable

    outbox.activate_manual_watchlist(ACTIVATED)
    with reader.snapshot():
        snapshot = reader.manual_watchlist_snapshot()
    assert snapshot is not None
    assert snapshot.row_count == 0
    ready = _by_name(build_manual_watchlist_projections(snapshot, observed_at=NOW))
    assert ready["manual_watchlist_state"].rows[0]["state"] == "ready"
    assert ready["manual_watchlist_state"].rows[0]["row_count"] == 0
    assert ready["manual_watchlist"].rows == ()
    validate_manual_watchlist_projections(ready)


def test_one_generation_retains_owners_versions_expiry_tombstones_and_exact_prices(
    tmp_path: Path,
) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_manual_watchlist(ACTIVATED)
    _rows(
        outbox.path,
        ("bob", "600002.SH", 3, 1, None, "[]", None, None),
        (
            "alice",
            "600001.SH",
            2,
            0,
            "screen_result",
            '["10.00","12.50"]',
            (NOW - timedelta(seconds=1)).isoformat(),
            (NOW - timedelta(minutes=2)).isoformat(),
        ),
        (
            "alice",
            "600000.SH",
            1,
            0,
            "detail",
            '["9.10"]',
            (NOW + timedelta(days=1)).isoformat(),
            (NOW - timedelta(minutes=3)).isoformat(),
        ),
    )
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        snapshot = reader.manual_watchlist_snapshot()
    assert snapshot is not None
    projections = _by_name(build_manual_watchlist_projections(snapshot, observed_at=NOW))
    rows = projections["manual_watchlist"].rows
    assert [(row["owner_id"], row["ts_code"], row["version"]) for row in rows] == [
        ("alice", "600000.SH", 1),
        ("alice", "600001.SH", 2),
        ("bob", "600002.SH", 3),
    ]
    assert rows[1]["price_levels_json"] == '["10.00","12.50"]'
    assert rows[1]["expires_at"] == (NOW - timedelta(seconds=1)).isoformat()
    assert rows[2]["deleted"] is True
    assert rows[2]["source"] is None
    assert rows[2]["price_levels_json"] == "[]"
    assert "active" not in rows[1]
    validate_manual_watchlist_projections(projections)
    missing = {
        name: projection for name, projection in projections.items() if name != "manual_watchlist"
    }
    with pytest.raises(ValueError, match="lacks rows"):
        validate_manual_watchlist_projections(missing)
    state = projections["manual_watchlist_state"]
    wrong_count = dict(state.rows[0])
    wrong_count["row_count"] = 2
    tampered_state = state.model_copy(update={"rows": (wrong_count,)})
    with pytest.raises(ValueError, match="count or digest"):
        validate_manual_watchlist_projections(
            {**projections, "manual_watchlist_state": tampered_state}
        )

    _rows(
        outbox.path,
        ("carol", "000001.SZ", 1, 0, "pool_member", "[]", None, NOW.isoformat()),
    )
    with reader.snapshot():
        next_snapshot = reader.manual_watchlist_snapshot()
    assert next_snapshot is not None
    assert next_snapshot.row_count == 4
    assert next_snapshot.rows_sha256 != snapshot.rows_sha256


@pytest.mark.parametrize("fault", ("missing_table", "bad_marker", "bad_row", "too_many"))
def test_bad_manual_authority_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_manual_watchlist(ACTIVATED)
    if fault == "missing_table":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute("DROP TABLE manual_watchlist")
    elif fault == "bad_marker":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute(
                "UPDATE page_control_protocol_activation SET protocol_version = 99 "
                "WHERE marker_name = 'manual-watchlist/v1'"
            )
    elif fault == "bad_row":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute("PRAGMA ignore_check_constraints = ON")
            connection.execute(
                "INSERT INTO manual_watchlist VALUES "
                "('alice', '600000.SH', 1, 1, 'detail', '[\"10.00\"]', NULL, NULL)"
            )
    else:
        import rquant.serving_page_projection_source as source_module

        monkeypatch.setattr(source_module, "_MAX_MANUAL_WATCHLIST_ROWS", 2)
        _rows(
            outbox.path,
            ("alice", "600000.SH", 1, 1, None, "[]", None, None),
            ("alice", "600001.SH", 1, 1, None, "[]", None, None),
            ("alice", "600002.SH", 1, 1, None, "[]", None, None),
        )
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot(), pytest.raises(PageProjectionSourceIntegrityError):
        reader.manual_watchlist_snapshot()


def test_source_failure_revokes_old_manual_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "replica.duckdb"
    _signal_projection_database(database)
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    outbox.activate_manual_watchlist(ACTIVATED)
    _rows(
        outbox.path,
        ("alice", "600000.SH", 1, 0, "detail", '["10.00"]', None, ACTIVATED.isoformat()),
    )
    source = DuckDBSignalPageProjectionSource(database, page_control_outbox=outbox)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(source=source, store=store)
    observed = datetime.now(UTC)
    producer.publish(observed)
    first = store.serving_snapshot(observed_at=observed, history_limit=1)
    before = _by_name(first.payload.projections)
    assert len(before["manual_watchlist"].rows) == 1

    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE manual_watchlist SET version = 2, price_levels_json = ?, "
            "updated_at_utc = ? WHERE owner_id = 'alice' AND ts_code = '600000.SH'",
            ('["12.50"]', (observed - timedelta(seconds=1)).isoformat()),
        )
    revised_at = observed + timedelta(seconds=1)
    producer.publish(revised_at)
    revised = store.serving_snapshot(observed_at=revised_at, history_limit=1)
    revised_rows = _by_name(revised.payload.projections)["manual_watchlist"].rows
    assert revised.projection_generation_id != first.projection_generation_id
    assert revised_rows[0]["version"] == 2
    assert revised_rows[0]["price_levels_json"] == '["12.50"]'

    def fail_read(_observed: datetime) -> None:
        raise PageProjectionSourceIntegrityError("synthetic unavailable")

    monkeypatch.setattr(source, "_build_snapshot", fail_read)
    later = revised_at + timedelta(seconds=2)
    producer.publish(later)
    after = _by_name(store.serving_snapshot(observed_at=later, history_limit=1).payload.projections)
    assert after["manual_watchlist_state"].rows[0]["state"] == "unavailable"
    assert "manual_watchlist" not in after


def test_manual_read_failure_revokes_rows_with_configured_formula_pools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, admission, _definitions, data_dir = _setup(tmp_path)
    (data_dir / "formula_pools").mkdir(mode=0o700, parents=True)
    outbox = service.outbox
    outbox.activate_manual_watchlist(ACTIVATED)
    _rows(
        outbox.path,
        ("alice", "600000.SH", 1, 0, "detail", '["10.00"]', None, ACTIVATED.isoformat()),
    )
    database = tmp_path / "replica.duckdb"
    _signal_projection_database(database)
    source = DuckDBSignalPageProjectionSource(
        database,
        page_control_outbox=outbox,
        formula_pool_config=_config(admission, data_dir),
    )
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(source=source, store=store)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    producer.publish(observed)
    initial = _by_name(
        store.serving_snapshot(observed_at=observed, history_limit=1).payload.projections
    )
    assert initial["manual_watchlist_state"].rows[0]["state"] == "ready"
    assert len(initial["manual_watchlist"].rows) == 1
    assert initial["formula_pool_state"].rows[0]["availability"] == "empty"

    assert source.page_control_outbox is not None

    def fail_manual_read() -> None:
        raise PageProjectionSourceIntegrityError("manual rows unavailable")

    with monkeypatch.context() as patcher:
        patcher.setattr(source.page_control_outbox, "manual_watchlist_snapshot", fail_manual_read)
        later = observed + timedelta(seconds=2)
        producer.publish(later)
    revoked = _by_name(
        store.serving_snapshot(observed_at=later, history_limit=1).payload.projections
    )
    assert revoked["manual_watchlist_state"].rows[0]["state"] == "unavailable"
    assert "manual_watchlist" not in revoked
    assert revoked["formula_pool_state"].rows[0]["availability"] == "empty"

    def fail_formula_read(_observed: datetime) -> None:
        raise ValueError("formula authority unavailable")

    monkeypatch.setattr(source, "_read_formula_pool_projections", fail_formula_read)
    with pytest.raises(PageProjectionSourceIntegrityError, match="formula pool authority"):
        producer.publish(later + timedelta(seconds=2))


@pytest.mark.parametrize("failure", ("missing_at_entry", "invalid_on_exit"))
@pytest.mark.parametrize("formula_pool_configured", (True, False))
def test_shared_audit_failure_blocks_formula_pools_or_revokes_manual_without_them(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
    formula_pool_configured: bool,
) -> None:
    service, admission, _definitions, data_dir = _setup(tmp_path)
    (data_dir / "formula_pools").mkdir(mode=0o700, parents=True)
    outbox = service.outbox
    outbox.activate_manual_watchlist(ACTIVATED)
    _rows(
        outbox.path,
        ("alice", "600000.SH", 1, 0, "detail", '["10.00"]', None, ACTIVATED.isoformat()),
    )
    database = tmp_path / "replica.duckdb"
    _signal_projection_database(database)
    source = DuckDBSignalPageProjectionSource(
        database,
        page_control_outbox=outbox,
        formula_pool_config=_config(admission, data_dir) if formula_pool_configured else None,
    )
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(source=source, store=store)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    producer.publish(observed)
    first = store.serving_snapshot(observed_at=observed, history_limit=1)
    initial = _by_name(first.payload.projections)
    assert initial["manual_watchlist_state"].rows[0]["state"] == "ready"
    assert len(initial["manual_watchlist"].rows) == 1
    if formula_pool_configured:
        assert initial["formula_pool_state"].rows[0]["availability"] == "empty"
    else:
        assert "formula_pool_state" not in initial

    if failure == "missing_at_entry":
        outbox.path.rename(tmp_path / "moved-control.sqlite3")
    else:
        assert source.page_control_outbox is not None
        original_snapshot = source.page_control_outbox.snapshot

        @contextmanager
        def fail_on_exit() -> Iterator[None]:
            with original_snapshot():
                yield
            raise PageProjectionSourceIntegrityError("audit changed after read")

        monkeypatch.setattr(source.page_control_outbox, "snapshot", fail_on_exit)
    later = observed + timedelta(seconds=2)
    if formula_pool_configured:
        with pytest.raises(PageProjectionSourceIntegrityError, match="PageControl"):
            producer.publish(later)
        unchanged = store.serving_snapshot(observed_at=later, history_limit=1)
        assert unchanged.projection_generation_id == first.projection_generation_id
        assert _by_name(unchanged.payload.projections)["manual_watchlist_state"].rows[0][
            "state"
        ] == "ready"
        return

    producer.publish(later)
    revoked = _by_name(
        store.serving_snapshot(observed_at=later, history_limit=1).payload.projections
    )
    assert revoked["manual_watchlist_state"].rows[0]["state"] == "unavailable"
    assert "manual_watchlist" not in revoked
    assert "pool_definition" not in revoked
    assert revoked["alert_ack_state"].rows[0]["state"] == "unavailable"
    assert revoked["price_alert_rule_state"].rows[0]["state"] == "unavailable"
    assert "formula_pool_state" not in revoked
