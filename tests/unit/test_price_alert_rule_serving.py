from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.alert_price_rule import PriceAlertRule
from rquant.notification_state import NotificationStateStore
from rquant.page_control import PageControlOutbox
from rquant.serving_contracts import FreshnessStatus, ServingDatasetWatermark
from rquant.serving_manual_watchlist_projection import build_manual_watchlist_projections
from rquant.serving_page_projection_source import (
    DuckDBSignalPageProjectionSource,
    PageProjectionSourceIntegrityError,
    SignalPageProjectionProducer,
    _ReadonlyPageControlAuditReader,
)
from rquant.serving_price_alert_rule_projection import (
    build_price_alert_rule_projections,
    price_rule_scope_status,
    validate_price_alert_rule_projections,
)
from rquant.serving_publisher import ServingGenerationLease, ServingPublisher, ServingReader
from rquant.serving_read_models import (
    SERVING_TABLE_SPECS,
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)
from tests.unit.test_serving_page_projection_source import _signal_projection_database

NOW = datetime(2026, 9, 29, 4, 0, tzinfo=UTC)
ACTIVATED = NOW - timedelta(days=1)


def _by_name(items: tuple[ServingProjectionPayload, ...]) -> dict[str, ServingProjectionPayload]:
    return {item.table_name: item for item in items}


def _bound(
    items: tuple[ServingProjectionPayload, ...], *, generation_id: str = "a" * 64
) -> dict[str, ServingProjectionInput]:
    return {
        item.table_name: ServingProjectionInput.bind(
            item, owner_dataset_id="signals", owner_generation_id=generation_id
        )
        for item in items
    }


def _current_lease(
    tmp_path: Path, projections: Mapping[str, ServingProjectionInput]
) -> ServingGenerationLease:
    tables = build_serving_read_models(
        ServingReadModelInput(observed_at=NOW, projections=tuple(projections.values()))
    )
    publisher = ServingPublisher(
        tmp_path / "scope-serving", producer_commit="1" * 40, table_specs=SERVING_TABLE_SPECS
    )
    publisher.publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="signals",
                generation_id="a" * 64,
                event_time=NOW,
                published_at=NOW,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"signals": "a" * 64},
        built_at=NOW,
    )
    return ServingReader(tmp_path / "scope-serving").acquire_generation()


def _rule_json(rule_id: str, *, enabled: bool = True) -> str:
    return PriceAlertRule(
        rule_id=rule_id,
        name="到价提醒",
        priority="P2",
        enabled=enabled,
        comparison="gte",
        threshold=Decimal("10.50"),
        valid_from=time(9, 30),
        valid_until=time(14, 57),
    ).model_dump_json()


def _member(
    path: Path,
    owner: str,
    code: str,
    version: int,
    *,
    expires_at: datetime | None = None,
    deleted: bool = False,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO manual_watchlist VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                owner,
                code,
                version,
                int(deleted),
                None if deleted else "detail",
                "[]",
                None if expires_at is None else expires_at.isoformat(),
                None if deleted else ACTIVATED.isoformat(),
            ),
        )


def _rule(
    path: Path,
    owner: str,
    rule_id: str,
    version: int,
    *,
    code: str = "600001.SH",
    member_version: int = 1,
    deleted: bool = False,
    enabled: bool = True,
) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO price_alert_rule VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                owner,
                rule_id,
                version,
                int(deleted),
                None if deleted else code,
                None if deleted else member_version,
                None if deleted else _rule_json(rule_id, enabled=enabled),
                ACTIVATED.isoformat(),
            ),
        )


def _activated_outbox(path: Path) -> PageControlOutbox:
    outbox = PageControlOutbox(path)
    outbox.activate_manual_watchlist(ACTIVATED)
    outbox.activate_price_alert_rules(ACTIVATED)
    return outbox


def test_unactivated_and_activated_zero_rules_are_distinct(tmp_path: Path) -> None:
    outbox = PageControlOutbox(tmp_path / "control.sqlite3")
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        assert reader.price_alert_rule_snapshot() is None
    absent = _by_name(build_price_alert_rule_projections(None, observed_at=NOW))
    assert absent["price_alert_rule_state"].rows[0]["state"] == "not_activated"
    assert "price_alert_rule" not in absent

    outbox.activate_price_alert_rules(ACTIVATED)
    with reader.snapshot():
        snapshot = reader.price_alert_rule_snapshot()
    assert snapshot is not None and snapshot.row_count == 0
    ready = _by_name(build_price_alert_rule_projections(snapshot, observed_at=NOW))
    assert ready["price_alert_rule_state"].rows[0]["state"] == "ready"
    assert ready["price_alert_rule_state"].rows[0]["row_count"] == 0
    assert ready["price_alert_rule"].rows == ()
    validate_price_alert_rule_projections(ready)


def test_all_heads_are_sorted_and_scope_requires_same_owner_version_and_current_expiry(
    tmp_path: Path,
) -> None:
    outbox = _activated_outbox(tmp_path / "control.sqlite3")
    _member(outbox.path, "alice", "600001.SH", 2, expires_at=NOW + timedelta(minutes=1))
    _member(outbox.path, "bob", "600001.SH", 1)
    _rule(outbox.path, "bob", "same", 1)
    _rule(outbox.path, "alice", "same", 3, member_version=2)
    _rule(outbox.path, "alice", "old", 1, member_version=1)
    _rule(outbox.path, "alice", "off", 2, member_version=2, enabled=False)
    _rule(outbox.path, "alice", "gone", 4, deleted=True)
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        members = reader.manual_watchlist_snapshot()
        rules = reader.price_alert_rule_snapshot()
    assert members is not None and rules is not None
    projections = _by_name(build_price_alert_rule_projections(rules, observed_at=NOW))
    facts = projections["price_alert_rule"].rows
    assert [(row["owner_id"], row["rule_id"]) for row in facts] == [
        ("alice", "gone"),
        ("alice", "off"),
        ("alice", "old"),
        ("alice", "same"),
        ("bob", "same"),
    ]
    assert facts[0]["deleted"] is True and facts[0]["name"] is None
    assert facts[3]["threshold"] == "10.50"
    assert facts[3]["enabled"] is True
    assert "effective" not in facts[3]
    validate_price_alert_rule_projections(projections)
    same_generation = {
        **_bound(build_price_alert_rule_projections(rules, observed_at=NOW)),
        **_bound(build_manual_watchlist_projections(members, observed_at=NOW)),
    }
    lease = _current_lease(tmp_path, same_generation)
    age_budget = timedelta(minutes=5)
    assert (
        price_rule_scope_status(
            same_generation,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="same",
            at=NOW,
        )
        == "active"
    )
    assert (
        price_rule_scope_status(
            same_generation,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="same",
            at=ACTIVATED - timedelta(seconds=1),
        )
        == "unavailable"
    )
    assert (
        price_rule_scope_status(
            same_generation,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="same",
            at=NOW + timedelta(minutes=1),
        )
        == "inactive"
    )
    assert (
        price_rule_scope_status(
            same_generation,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="old",
            at=NOW,
        )
        == "inactive"
    )
    assert (
        price_rule_scope_status(
            same_generation,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="off",
            at=NOW,
        )
        == "inactive"
    )
    assert (
        price_rule_scope_status(
            same_generation,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="gone",
            at=NOW,
        )
        == "inactive"
    )
    assert (
        price_rule_scope_status(
            projections,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="same",
            at=NOW,
        )
        == "unavailable"
    )
    wrong_generation = {
        **same_generation,
        "manual_watchlist": _bound(
            build_manual_watchlist_projections(members, observed_at=NOW),
            generation_id="b" * 64,
        )["manual_watchlist"],
    }
    assert (
        price_rule_scope_status(
            wrong_generation,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="same",
            at=NOW,
        )
        == "unavailable"
    )
    lease.close()

    wrong_count = dict(projections["price_alert_rule_state"].rows[0])
    wrong_count["row_count"] = 1
    bad_state = projections["price_alert_rule_state"].model_copy(update={"rows": (wrong_count,)})
    with pytest.raises(ValueError, match="count or digest"):
        validate_price_alert_rule_projections({**projections, "price_alert_rule_state": bad_state})


def test_unexpired_rule_cannot_be_active_from_year_old_generation(tmp_path: Path) -> None:
    outbox = _activated_outbox(tmp_path / "control.sqlite3")
    _member(outbox.path, "alice", "600001.SH", 1)
    _rule(outbox.path, "alice", "rule-a", 1)
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        members = reader.manual_watchlist_snapshot()
        rules = reader.price_alert_rule_snapshot()
    assert members is not None and rules is not None
    projections = {
        **_bound(build_price_alert_rule_projections(rules, observed_at=NOW)),
        **_bound(build_manual_watchlist_projections(members, observed_at=NOW)),
    }
    lease = _current_lease(tmp_path, projections)
    age_budget = timedelta(minutes=5)
    assert (
        price_rule_scope_status(
            projections,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="rule-a",
            at=NOW,
        )
        == "active"
    )
    assert (
        price_rule_scope_status(
            projections,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="rule-a",
            at=NOW + age_budget,
        )
        == "active"
    )
    assert (
        price_rule_scope_status(
            projections,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="rule-a",
            at=NOW + age_budget + timedelta(microseconds=1),
        )
        == "unavailable"
    )
    assert (
        price_rule_scope_status(
            projections,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="rule-a",
            at=NOW + timedelta(days=365),
        )
        == "unavailable"
    )
    lease.close()
    assert (
        price_rule_scope_status(
            projections,
            lease=lease,
            max_generation_age=age_budget,
            owner_id="alice",
            rule_id="rule-a",
            at=NOW,
        )
        == "unavailable"
    )


@pytest.mark.parametrize(
    "fault",
    (
        "missing_table",
        "bad_marker",
        "bad_schema",
        "bad_row",
        "too_many",
        "owner_capacity",
        "huge_threshold",
    ),
)
def test_corrupt_rule_source_fails_without_truncating_facts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    outbox = _activated_outbox(tmp_path / "control.sqlite3")
    if fault == "missing_table":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute("DROP TABLE price_alert_rule")
    elif fault == "bad_marker":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute(
                "UPDATE page_control_protocol_activation SET protocol_version = 99 "
                "WHERE marker_name = 'price-alert-rule/v1'"
            )
    elif fault == "bad_schema":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute("ALTER TABLE price_alert_rule ADD COLUMN extra TEXT")
    elif fault == "bad_row":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute("PRAGMA ignore_check_constraints = ON")
            connection.execute(
                "INSERT INTO price_alert_rule VALUES "
                "('alice', 'bad', 1, 1, '600001.SH', 1, '{}', ?)",
                (ACTIVATED.isoformat(),),
            )
    elif fault == "huge_threshold":
        oversized = PriceAlertRule(
            rule_id="huge",
            name="过大",
            priority="P2",
            enabled=True,
            comparison="gte",
            threshold=Decimal("1e65"),
            valid_from=time(9, 30),
            valid_until=time(14, 57),
        )
        with sqlite3.connect(outbox.path) as connection:
            connection.execute(
                "INSERT INTO price_alert_rule VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "alice",
                    "huge",
                    1,
                    0,
                    "600001.SH",
                    1,
                    oversized.model_dump_json(),
                    ACTIVATED.isoformat(),
                ),
            )
    elif fault == "owner_capacity":
        for index in range(101):
            _rule(outbox.path, "alice", f"r{index}", 1)
    else:
        import rquant.serving_page_projection_source as source_module

        monkeypatch.setattr(source_module, "_MAX_PRICE_ALERT_RULE_ROWS", 2)
        for index in range(3):
            _rule(outbox.path, "alice", f"r{index}", 1, deleted=True)
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot(), pytest.raises(PageProjectionSourceIntegrityError):
        reader.price_alert_rule_snapshot()


@pytest.mark.parametrize("suffix", ("-wal", "-shm"))
def test_price_rules_reject_sidecars_without_formula_pool_config(
    tmp_path: Path, suffix: str
) -> None:
    outbox = _activated_outbox(tmp_path / "control.sqlite3")
    Path(f"{outbox.path}{suffix}").write_bytes(b"unsafe")
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with pytest.raises(PageProjectionSourceIntegrityError, match="sidecar"), reader.snapshot():
        reader.price_alert_rule_snapshot()


@pytest.mark.parametrize("failure", ("reader", "marker", "rotation"))
def test_next_serving_generation_revokes_old_rules_after_source_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    database = tmp_path / "replica.duckdb"
    _signal_projection_database(database)
    outbox = _activated_outbox(tmp_path / "control.sqlite3")
    _rule(outbox.path, "alice", "rule-a", 1)
    source = DuckDBSignalPageProjectionSource(database, page_control_outbox=outbox)
    store = NotificationStateStore(tmp_path / "notification.sqlite3")
    producer = SignalPageProjectionProducer(source=source, store=store)
    observed = datetime.now(UTC) + timedelta(minutes=2)
    producer.publish(observed)
    initial = _by_name(
        store.serving_snapshot(observed_at=observed, history_limit=1).payload.projections
    )
    assert initial["price_alert_rule_state"].rows[0]["state"] == "ready"
    assert len(initial["price_alert_rule"].rows) == 1

    if failure == "reader":
        assert source.page_control_outbox is not None

        def fail_rule_read() -> None:
            raise PageProjectionSourceIntegrityError("synthetic rule failure")

        monkeypatch.setattr(source.page_control_outbox, "price_alert_rule_snapshot", fail_rule_read)
    elif failure == "marker":
        with sqlite3.connect(outbox.path) as connection:
            connection.execute(
                "UPDATE page_control_protocol_activation SET protocol_version = 99 "
                "WHERE marker_name = 'price-alert-rule/v1'"
            )
    else:
        outbox.path.rename(tmp_path / "rotated-control.sqlite3")
    later = observed + timedelta(seconds=2)
    producer.publish(later)
    after = _by_name(store.serving_snapshot(observed_at=later, history_limit=1).payload.projections)
    assert after["price_alert_rule_state"].rows[0]["state"] == "unavailable"
    assert "price_alert_rule" not in after


def test_serving_read_model_rejects_rule_digest_tamper_and_accepts_old_absence(
    tmp_path: Path,
) -> None:
    outbox = _activated_outbox(tmp_path / "control.sqlite3")
    _rule(outbox.path, "alice", "rule-a", 1)
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        snapshot = reader.price_alert_rule_snapshot()
    assert snapshot is not None
    bound = _bound(build_price_alert_rule_projections(snapshot, observed_at=NOW))
    ServingReadModelInput(observed_at=NOW, projections=tuple(bound.values()))
    ServingReadModelInput(observed_at=NOW, projections=())

    state = bound["price_alert_rule_state"]
    tampered_row = {**state.rows[0], "rows_sha256": "f" * 64}
    tampered = state.model_copy(update={"rows": (tampered_row,)})
    with pytest.raises(ValueError, match="digest"):
        ServingReadModelInput(
            observed_at=NOW,
            projections=(tampered, bound["price_alert_rule"]),
        )


def test_synthetic_serving_generation_reads_rule_state_and_facts(tmp_path: Path) -> None:
    outbox = _activated_outbox(tmp_path / "control.sqlite3")
    _rule(outbox.path, "alice", "rule-a", 3)
    reader = _ReadonlyPageControlAuditReader(outbox.path)
    with reader.snapshot():
        snapshot = reader.price_alert_rule_snapshot()
    assert snapshot is not None
    projections = _bound(build_price_alert_rule_projections(snapshot, observed_at=NOW))
    tables = build_serving_read_models(
        ServingReadModelInput(observed_at=NOW, projections=tuple(projections.values()))
    )
    publisher = ServingPublisher(
        tmp_path / "serving", producer_commit="1" * 40, table_specs=SERVING_TABLE_SPECS
    )
    publisher.publish(
        tables,
        watermarks=(
            ServingDatasetWatermark(
                dataset_id="signals",
                generation_id="a" * 64,
                event_time=NOW,
                published_at=NOW,
                sequence=1,
                status=FreshnessStatus.FRESH,
            ),
        ),
        source_generations={"signals": "a" * 64},
        built_at=NOW,
    )
    with ServingReader(tmp_path / "serving").open_current_readonly() as connection:
        assert connection.execute(
            "SELECT state, row_count FROM price_alert_rule_state"
        ).fetchone() == ("ready", 1)
        assert connection.execute(
            "SELECT owner_id, rule_id, version, threshold FROM price_alert_rule"
        ).fetchone() == ("alice", "rule-a", 3, "10.50")
