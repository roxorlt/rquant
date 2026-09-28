from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, time
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from threading import Barrier
from typing import Any

import pytest

from rquant.alert_price_rule import PriceAlertRule
from rquant.manual_watchlist import (
    ManualWatchlistDelete,
    ManualWatchlistKey,
    ManualWatchlistRepository,
    ManualWatchlistUpsert,
)

NOW = datetime(2026, 9, 29, 2, 0, tzinfo=UTC)
CODE = "600001.SH"


def _api() -> Any:
    return import_module("rquant.price_alert_rule_store")


def _connection(path: str | Path = ":memory:") -> sqlite3.Connection:
    return sqlite3.connect(path, timeout=5, isolation_level=None, check_same_thread=False)


def _setup(connection: sqlite3.Connection, api: Any) -> Any:
    connection.execute("BEGIN IMMEDIATE")
    ManualWatchlistRepository(connection).install_schema()
    store = api.PriceAlertRuleRepository(connection)
    store.install_schema()
    connection.commit()
    return store


def _member(
    connection: sqlite3.Connection,
    owner: str,
    *,
    code: str = CODE,
    expected: int | None = None,
    expires_at: datetime | None = None,
) -> int:
    connection.execute("BEGIN IMMEDIATE")
    try:
        entry = ManualWatchlistRepository(connection).upsert(
            ManualWatchlistUpsert(
                owner_id=owner,
                ts_code=code,
                expected_version=expected,
                source="detail",
                expires_at=expires_at,
            ),
            now=NOW,
        )
        connection.commit()
        return entry.version
    except Exception:
        connection.rollback()
        raise


def _remove_member(connection: sqlite3.Connection, owner: str, *, expected: int) -> None:
    connection.execute("BEGIN IMMEDIATE")
    try:
        ManualWatchlistRepository(connection).delete(
            ManualWatchlistDelete(owner_id=owner, ts_code=CODE, expected_version=expected),
            now=NOW,
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise


def _rule(
    rule_id: str = "rule-a", *, enabled: bool = True, threshold: str = "10"
) -> PriceAlertRule:
    return PriceAlertRule(
        rule_id=rule_id,
        name="到价提醒",
        priority="P2",
        enabled=enabled,
        comparison="gte",
        threshold=Decimal(threshold),
        valid_from=time(9, 30),
        valid_until=time(14, 57),
    )


def _upsert(
    api: Any,
    owner: str,
    *,
    rule_id: str = "rule-a",
    code: str = CODE,
    membership_version: int = 1,
    expected_version: int | None = None,
    enabled: bool = True,
    threshold: str = "10",
) -> Any:
    return api.PriceAlertRuleUpsert(
        owner_id=owner,
        ts_code=code,
        membership_version=membership_version,
        expected_version=expected_version,
        rule=_rule(rule_id, enabled=enabled, threshold=threshold),
    )


def _save(connection: sqlite3.Connection, store: Any, command: Any) -> Any:
    connection.execute("BEGIN IMMEDIATE")
    try:
        entry = store.upsert(command, now=NOW)
        connection.commit()
        return entry
    except Exception:
        connection.rollback()
        raise


def _delete(connection: sqlite3.Connection, store: Any, api: Any, owner: str, version: int) -> Any:
    connection.execute("BEGIN IMMEDIATE")
    try:
        entry = store.delete(
            api.PriceAlertRuleDelete(owner_id=owner, rule_id="rule-a", expected_version=version),
            now=NOW,
        )
        connection.commit()
        return entry
    except Exception:
        connection.rollback()
        raise


def test_owner_and_rule_identity_are_isolated_for_same_stock_and_id() -> None:
    api = _api()
    with _connection() as connection:
        store = _setup(connection, api)
        _member(connection, "alice")
        _member(connection, "bob")
        alice = _save(connection, store, _upsert(api, "alice"))
        bob = _save(connection, store, _upsert(api, "bob"))
        assert alice.owner_id == "alice" and bob.owner_id == "bob"
        assert alice.rule_id == bob.rule_id == "rule-a"
        assert alice.ts_code == bob.ts_code == CODE
        assert alice.membership_version == bob.membership_version == 1
        assert [(row.owner_id, row.rule_id) for row in store.list_current("alice")] == [
            ("alice", "rule-a")
        ]
        assert store.get(api.PriceAlertRuleKey(owner_id="carol", rule_id="rule-a")) is None
        with pytest.raises(api.PriceAlertRuleVersionConflictError):
            _save(connection, store, _upsert(api, "carol", expected_version=1))
        with pytest.raises(api.PriceAlertRuleVersionConflictError):
            _delete(connection, store, api, "carol", 1)
        updated_alice = _save(
            connection,
            store,
            _upsert(api, "alice", expected_version=1, threshold="11"),
        )
        assert updated_alice.version == 2
        assert store.get(api.PriceAlertRuleKey(owner_id="bob", rule_id="rule-a")) == bob
        deleted_bob = _delete(connection, store, api, "bob", 1)
        assert deleted_bob.version == 2 and deleted_bob.deleted
        assert store.list_current("bob") == ()
        assert store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="rule-a")) == updated_alice


def test_create_and_reenable_require_same_owner_active_exact_membership_version() -> None:
    api = _api()
    with _connection() as connection:
        store = _setup(connection, api)
        _member(connection, "bob")
        with pytest.raises(api.PriceAlertRuleScopeError):
            _save(connection, store, _upsert(api, "alice"))
        _member(connection, "alice", expires_at=NOW)
        with pytest.raises(api.PriceAlertRuleScopeError):
            _save(connection, store, _upsert(api, "alice"))
        _member(connection, "alice", expected=1)
        with pytest.raises(api.PriceAlertRuleScopeError):
            _save(connection, store, _upsert(api, "alice", membership_version=1))
        first = _save(connection, store, _upsert(api, "alice", membership_version=2))
        assert first.version == 1
        _remove_member(connection, "alice", expected=2)
        with pytest.raises(api.PriceAlertRuleScopeError):
            _save(
                connection,
                store,
                _upsert(api, "alice", membership_version=2, expected_version=1),
            )
        disabled = _save(
            connection,
            store,
            _upsert(
                api,
                "alice",
                membership_version=2,
                expected_version=1,
                enabled=False,
            ),
        )
        assert disabled.version == 2 and disabled.rule.enabled is False
        with pytest.raises(api.PriceAlertRuleScopeError):
            _save(
                connection,
                store,
                _upsert(api, "alice", membership_version=2, expected_version=2),
            )
        assert store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="rule-a")) == disabled


def test_tombstone_cas_and_rebuild_keep_versions_monotonic_after_scope_loss() -> None:
    api = _api()
    with _connection() as connection:
        store = _setup(connection, api)
        _member(connection, "alice")
        created = _save(connection, store, _upsert(api, "alice"))
        assert created.version == 1
        with pytest.raises(api.PriceAlertRuleVersionConflictError):
            _save(connection, store, _upsert(api, "alice", expected_version=None))
        with pytest.raises(api.PriceAlertRuleVersionConflictError):
            _save(connection, store, _upsert(api, "alice", expected_version=2))
        _remove_member(connection, "alice", expected=1)
        tombstone = _delete(connection, store, api, "alice", 1)
        assert tombstone.deleted is True and tombstone.version == 2 and tombstone.rule is None
        assert store.list_current("alice") == ()
        with pytest.raises(api.PriceAlertRuleVersionConflictError):
            _save(connection, store, _upsert(api, "alice", expected_version=1))
        with pytest.raises(api.PriceAlertRuleScopeError):
            _save(connection, store, _upsert(api, "alice", expected_version=2))
        _member(connection, "alice", expected=2)
        rebuilt = _save(
            connection,
            store,
            _upsert(api, "alice", membership_version=3, expected_version=2),
        )
        assert rebuilt.version == 3 and rebuilt.membership_version == 3
        assert store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="rule-a")) == rebuilt


def test_removed_and_readded_watchlist_does_not_reactivate_old_enabled_rule() -> None:
    api = _api()
    with _connection() as connection:
        store = _setup(connection, api)
        _member(connection, "alice")
        old = _save(connection, store, _upsert(api, "alice"))
        assert [
            (row.rule_id, row.membership_version) for row in store.list_effective("alice", now=NOW)
        ] == [("rule-a", 1)]
        _remove_member(connection, "alice", expected=1)
        assert store.list_effective("alice", now=NOW) == ()
        _member(connection, "alice", expected=2)
        assert old.rule.enabled is True
        assert store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="rule-a")) == old
        assert store.list_effective("alice", now=NOW) == ()


def test_transaction_boundary_rolls_back_rule_and_member_together() -> None:
    api = _api()
    with _connection() as connection:
        store = api.PriceAlertRuleRepository(connection)
        with pytest.raises(api.PriceAlertRuleTransactionError):
            store.install_schema()
        connection.execute("BEGIN IMMEDIATE")
        ManualWatchlistRepository(connection).install_schema()
        store.install_schema()
        connection.commit()
        with pytest.raises(api.PriceAlertRuleTransactionError):
            store.upsert(_upsert(api, "alice"), now=NOW)

        connection.execute("BEGIN IMMEDIATE")
        ManualWatchlistRepository(connection).upsert(
            ManualWatchlistUpsert(owner_id="alice", ts_code=CODE, source="detail"), now=NOW
        )
        entry = store.upsert(_upsert(api, "alice"), now=NOW)
        assert entry.version == 1
        connection.rollback()
        assert (
            ManualWatchlistRepository(connection).get(
                ManualWatchlistKey(owner_id="alice", ts_code=CODE), now=NOW
            )
            is None
        )
        assert store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="rule-a")) is None


def test_missing_schema_bad_schema_and_bad_row_are_not_trusted_empty() -> None:
    api = _api()
    with _connection() as connection:
        store = api.PriceAlertRuleRepository(connection)
        with pytest.raises(api.PriceAlertRuleUnavailableError):
            store.list_current("alice")
        connection.execute("CREATE TABLE price_alert_rule (bad_column TEXT)")
        with pytest.raises(api.PriceAlertRuleUnavailableError):
            store.list_current("alice")
    with _connection() as connection:
        store = _setup(connection, api)
        assert store.list_current("alice") == ()
        _member(connection, "alice")
        _save(connection, store, _upsert(api, "alice"))
        connection.execute(
            "UPDATE price_alert_rule SET rule_json = ? WHERE owner_id = ? AND rule_id = ?",
            ('{"rule_id":"other"}', "alice", "rule-a"),
        )
        with pytest.raises(api.PriceAlertRuleIntegrityError):
            store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="rule-a"))
        with pytest.raises(api.PriceAlertRuleIntegrityError):
            store.list_current("alice")


def test_effective_zero_requires_an_installed_watchlist_authority() -> None:
    api = _api()
    with _connection() as connection:
        connection.execute("BEGIN IMMEDIATE")
        store = api.PriceAlertRuleRepository(connection)
        store.install_schema()
        connection.commit()
        assert store.list_current("alice") == ()
        with pytest.raises(api.PriceAlertRuleUnavailableError):
            store.list_effective("alice", now=NOW)


def test_corrupt_tombstone_is_not_reported_as_trusted_zero() -> None:
    api = _api()
    with _connection() as connection:
        store = _setup(connection, api)
        _member(connection, "alice")
        _save(connection, store, _upsert(api, "alice"))
        _delete(connection, store, api, "alice", 1)
        assert store.list_current("alice") == ()
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE price_alert_rule SET rule_json = '{}' WHERE owner_id = ? AND rule_id = ?",
            ("alice", "rule-a"),
        )
        with pytest.raises(api.PriceAlertRuleIntegrityError):
            store.list_current("alice")


def test_same_head_and_capacity_are_safe_across_two_connections(tmp_path: Path) -> None:
    api = _api()
    path = tmp_path / "rules.sqlite3"
    setup = _connection(path)
    store = _setup(setup, api)
    _member(setup, "alice")
    _save(setup, store, _upsert(api, "alice"))

    def race(commands: tuple[Any, Any]) -> list[str]:
        barrier = Barrier(2)

        def attempt(command: Any) -> str:
            with _connection(path) as connection:
                local = api.PriceAlertRuleRepository(connection)
                barrier.wait(timeout=5)
                try:
                    _save(connection, local, command)
                except api.PriceAlertRuleVersionConflictError:
                    return "conflict"
                except api.PriceAlertRuleCapacityError:
                    return "capacity"
                return "saved"

        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(pool.map(attempt, commands))

    assert sorted(
        race(
            (
                _upsert(api, "alice", expected_version=1, threshold="11"),
                _upsert(api, "alice", expected_version=1, threshold="12"),
            )
        )
    ) == ["conflict", "saved"]
    assert store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="rule-a")).version == 2

    setup.execute("BEGIN IMMEDIATE")
    for index in range(1, 99):
        store.upsert(_upsert(api, "alice", rule_id=f"rule-{index:03d}"), now=NOW)
    setup.commit()
    assert len(store.list_current("alice")) == 99
    assert sorted(
        race(
            (
                _upsert(api, "alice", rule_id="next-a"),
                _upsert(api, "alice", rule_id="next-b"),
            )
        )
    ) == ["capacity", "saved"]
    assert len(store.list_current("alice")) == 100
    with pytest.raises(api.PriceAlertRuleCapacityError):
        _save(setup, store, _upsert(api, "alice", rule_id="rule-101"))
    _delete(setup, store, api, "alice", 2)
    assert len(store.list_current("alice")) == 99
    rebuilt = _save(setup, store, _upsert(api, "alice", expected_version=3))
    assert rebuilt.version == 4
    assert len(store.list_current("alice")) == 100
    setup.close()


def test_corrupt_over_capacity_rows_are_rejected_instead_of_truncated() -> None:
    api = _api()
    with _connection() as connection:
        store = _setup(connection, api)
        _member(connection, "alice")
        first = _save(connection, store, _upsert(api, "alice"))
        connection.execute("BEGIN IMMEDIATE")
        for index in range(1, 101):
            connection.execute(
                "INSERT INTO price_alert_rule "
                "(owner_id, rule_id, version, deleted, ts_code, membership_version, "
                "rule_json, updated_at_utc) "
                "VALUES (?, ?, ?, 0, ?, ?, ?, ?)",
                (
                    "alice",
                    f"raw-{index:03d}",
                    1,
                    CODE,
                    1,
                    _rule(f"raw-{index:03d}").model_dump_json(),
                    NOW.isoformat(),
                ),
            )
        connection.commit()
        assert first.version == 1
        with pytest.raises(api.PriceAlertRuleIntegrityError):
            store.list_current("alice")


@pytest.mark.parametrize("threshold", ["0", "-1", "NaN", "Infinity", "-Infinity"])
def test_invalid_price_threshold_is_rejected(threshold: str) -> None:
    with pytest.raises(ValueError):
        _rule(threshold=threshold)


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (time(9, 30), time(9, 30)),
        (time(14, 0), time(9, 30)),
        (time(9, 30, tzinfo=UTC), time(14, 0)),
    ],
)
def test_invalid_shanghai_window_is_rejected(start: time, end: time) -> None:
    payload = _rule().model_dump(mode="python")
    payload.update(valid_from=start, valid_until=end)
    with pytest.raises(ValueError):
        PriceAlertRule.model_validate(payload)


def test_parameterized_identity_and_aware_clock_boundary() -> None:
    api = _api()
    with _connection() as connection:
        store = _setup(connection, api)
        _member(connection, "alice")
        odd = "rule'); DROP TABLE manual_watchlist; --"
        saved = _save(connection, store, _upsert(api, "alice", rule_id=odd))
        assert saved.rule_id == odd
        assert (
            ManualWatchlistRepository(connection).get(
                ManualWatchlistKey(owner_id="alice", ts_code=CODE), now=NOW
            )
            is not None
        )
        connection.execute("BEGIN IMMEDIATE")
        with pytest.raises(ValueError):
            store.upsert(
                _upsert(api, "alice", rule_id="naive"),
                now=datetime(2026, 9, 29, 2, 0),
            )
        connection.rollback()
        assert store.get(api.PriceAlertRuleKey(owner_id="alice", rule_id="naive")) is None
