"""Point-in-time calendar guard and auditable repair tests."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pandas as pd
import pytest

import rquant.data_quality as data_quality
from rquant.research_sync import LOCAL_ONLY_TABLES, MERGE_TABLES, REPLACE_TABLES
from rquant.storage import schema
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.migrations import MIGRATIONS, initialize_schema

UPDATED_AT = datetime(2026, 7, 14, tzinfo=UTC)


def _seed_calendar(
    store: DuckDBStore,
    rows: list[tuple[date, bool]],
) -> None:
    store._conn.executemany(
        "INSERT INTO trade_calendar "
        "(exchange, cal_date, is_open, pretrade_date, source, updated_at) "
        "VALUES ('SSE', ?, ?, NULL, 'test', ?)",
        [[cal_date, is_open, UPDATED_AT] for cal_date, is_open in rows],
    )


def _seed_pool(
    store: DuckDBStore,
    rows: list[tuple[str, date, str]],
) -> None:
    store._conn.executemany(
        "INSERT INTO limit_up_pool_daily (ts_code, trade_date, source) "
        "VALUES (?, ?, ?)",
        rows,
    )


def _pool_frame(ts_code: str, trade_date: date, source: str) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": ts_code,
                "trade_date": trade_date,
                "name": None,
                "pct_chg": None,
                "close": None,
                "amount": None,
                "circ_mv": None,
                "total_mv": None,
                "turnover_rate": None,
                "seal_amount": None,
                "first_seal_time": None,
                "last_seal_time": None,
                "break_count": None,
                "limit_up_stat": None,
                "consecutive_boards": None,
                "industry": None,
                "source": source,
            }
        ]
    )


def test_v5_creates_local_data_repair_audit_with_count_constraints() -> None:
    assert hasattr(schema, "DATA_REPAIR_AUDIT_DDL")
    data_repair_audit_ddl = schema.DATA_REPAIR_AUDIT_DDL
    assert [migration.version for migration in MIGRATIONS[:5]] == [1, 2, 3, 4, 5]
    assert MIGRATIONS[4].statements == (data_repair_audit_ddl,)
    assert (
        MIGRATIONS[4].checksum
        == "cde414cecf9e6ef662c97dc9aa00dda6cc1247591a86f39491d7e42130b6bf5d"
    )

    conn = duckdb.connect(":memory:")
    initialize_schema(conn, migrations=MIGRATIONS[:4])
    assert conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'data_repair_audit'"
    ).fetchone() == (0,)

    initialize_schema(conn)
    columns = conn.execute(
        "SELECT column_name, is_nullable, data_type "
        "FROM information_schema.columns "
        "WHERE table_name = 'data_repair_audit' ORDER BY ordinal_position"
    ).fetchall()
    primary_key = conn.execute(
        "SELECT constraint_column_names FROM duckdb_constraints() "
        "WHERE table_name = 'data_repair_audit' "
        "AND constraint_type = 'PRIMARY KEY'"
    ).fetchone()
    assert columns == [
        ("audit_id", "NO", "VARCHAR"),
        ("plan_id", "NO", "VARCHAR"),
        ("action_id", "NO", "VARCHAR"),
        ("dataset_id", "NO", "VARCHAR"),
        ("target_table", "NO", "VARCHAR"),
        ("key_columns", "NO", "JSON"),
        ("candidate_keys", "NO", "JSON"),
        ("before_count", "NO", "BIGINT"),
        ("deleted_count", "NO", "BIGINT"),
        ("after_count", "NO", "BIGINT"),
        ("applied_at", "NO", "TIMESTAMP WITH TIME ZONE"),
    ]
    assert primary_key == (["audit_id"],)
    initialize_schema(conn)
    assert conn.execute(
        "SELECT COUNT(*) FROM schema_migration WHERE version = 5"
    ).fetchone() == (1,)
    conn.close()
    with duckdb.connect(":memory:") as invalid:
        initialize_schema(invalid)
        for counts in ((-1, 0, 0), (1, 0, 2), (2, 1, 0)):
            try:
                invalid.execute(
                    "INSERT INTO data_repair_audit VALUES "
                    "('a', 'p', 'action/v1', 'dataset', 'target', "
                    "CAST('[]' AS JSON), CAST('[]' AS JSON), ?, ?, ?, now())",
                    list(counts),
                )
            except duckdb.ConstraintException:
                pass
            else:
                raise AssertionError(f"invalid counts were accepted: {counts}")


def test_data_repair_audit_is_local_only() -> None:
    assert "data_repair_audit" in LOCAL_ONLY_TABLES
    assert "data_repair_audit" not in REPLACE_TABLES
    assert "data_repair_audit" not in MERGE_TABLES


def test_v6_creates_local_limit_up_pool_write_guard() -> None:
    assert hasattr(schema, "LIMIT_UP_POOL_WRITE_GUARD_DDL")
    assert hasattr(schema, "LIMIT_UP_POOL_WRITE_GUARD_SEED_DML")
    assert [migration.version for migration in MIGRATIONS] == list(range(1, 11))
    assert MIGRATIONS[5].statements == (
        schema.LIMIT_UP_POOL_WRITE_GUARD_DDL,
        schema.LIMIT_UP_POOL_WRITE_GUARD_SEED_DML,
    )
    assert (
        MIGRATIONS[5].checksum
        == "aefd8cac1bc85931e48960fecfb63a64fa9f3959a93d378376c2929d5e54dbce"
    )
    assert schema.LIMIT_UP_POOL_WRITE_GUARD_DDL in schema.ALL_DDL
    assert schema.LIMIT_UP_POOL_WRITE_GUARD_SEED_DML in schema.ALL_DDL
    assert "limit_up_pool_write_guard" in LOCAL_ONLY_TABLES
    assert "limit_up_pool_write_guard" not in REPLACE_TABLES
    assert "limit_up_pool_write_guard" not in MERGE_TABLES

    conn = duckdb.connect(":memory:")
    initialize_schema(conn)
    assert conn.execute(
        "SELECT guard_id, generation FROM limit_up_pool_write_guard"
    ).fetchall() == [("limit_up_pool_daily", 0)]
    initialize_schema(conn)
    assert conn.execute(
        "SELECT guard_id, generation FROM limit_up_pool_write_guard"
    ).fetchall() == [("limit_up_pool_daily", 0)]
    conn.close()


def test_upsert_pool_touches_guard_inside_existing_transaction(
    tmp_path: Path,
) -> None:
    with DuckDBStore(tmp_path / "guard-existing-transaction.duckdb") as store:
        store._conn.execute("BEGIN")
        store.upsert_limit_up_pool(
            _pool_frame("600001.SH", date(2026, 7, 12), "eastmoney")
        )
        inside = store._conn.execute(
            "SELECT generation FROM limit_up_pool_write_guard "
            "WHERE guard_id = 'limit_up_pool_daily'"
        ).fetchone()
        store._conn.execute("ROLLBACK")
        after = store._conn.execute(
            "SELECT generation FROM limit_up_pool_write_guard "
            "WHERE guard_id = 'limit_up_pool_daily'"
        ).fetchone()
        pool_count = store._conn.execute(
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone()

    assert inside == (1,)
    assert after == (0,)
    assert pool_count == (0,)


def test_closed_day_repair_plan_is_stable_and_uses_complete_primary_keys(
    tmp_path: Path,
) -> None:
    assert hasattr(
        data_quality,
        "build_limit_up_pool_closed_day_repair_plan",
    )
    build_plan = data_quality.build_limit_up_pool_closed_day_repair_plan
    closed = date(2026, 7, 12)
    opened = date(2026, 7, 13)
    rows = [
        ("600001.SH", closed, "source-b"),
        ("000001.SZ", opened, "source-a"),
        ("600001.SH", closed, "source-a"),
        ("000002.SZ", closed, "source-a"),
    ]

    plans = []
    for index, insertion_order in enumerate((rows, list(reversed(rows)))):
        with DuckDBStore(tmp_path / f"stable-{index}.duckdb") as store:
            _seed_calendar(store, [(closed, False), (opened, True)])
            _seed_pool(store, insertion_order)
            plans.append(build_plan(store))

    first, second = plans
    assert first.status == "ready"
    assert first.severity is None
    assert first.plan_id == second.plan_id
    assert first.plan_id is not None
    assert len(first.plan_id) == 64
    int(first.plan_id, 16)
    assert first.key_columns == ("ts_code", "trade_date", "source")
    assert [
        (key.ts_code, key.trade_date, key.source)
        for key in first.candidate_keys
    ] == [
        ("000002.SZ", closed, "source-a"),
        ("600001.SH", closed, "source-a"),
        ("600001.SH", closed, "source-b"),
    ]
    assert first.before_count == 3


def test_repair_plan_blocks_on_any_unknown_calendar_date_without_writes(
    tmp_path: Path,
) -> None:
    known_closed = date(2026, 7, 12)
    unknown = date(2026, 7, 11)
    with DuckDBStore(tmp_path / "unknown.duckdb") as store:
        _seed_calendar(store, [(known_closed, False)])
        _seed_pool(
            store,
            [
                ("600001.SH", known_closed, "eastmoney"),
                ("000001.SZ", unknown, "eastmoney"),
            ],
        )

        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        pool_count = store._conn.execute(
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone()
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert plan.status == "blocked"
    assert plan.severity == "P0"
    assert plan.plan_id is None
    assert plan.candidate_keys == ()
    assert plan.unknown_dates == (unknown,)
    assert pool_count == (2,)
    assert audit_count == (0,)


def test_ready_dry_run_writes_no_audit_and_rejects_readonly_replica(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "dry-run.duckdb"
    closed = date(2026, 7, 12)
    with DuckDBStore(db_path) as store:
        _seed_calendar(store, [(closed, False)])
        _seed_pool(store, [("600001.SH", closed, "eastmoney")])

        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert plan.status == "ready"
    assert audit_count == (0,)
    with (
        DuckDBStore(db_path, read_only=True) as readonly_store,
        pytest.raises(ValueError, match="writable DuckDBStore"),
    ):
        data_quality.build_limit_up_pool_closed_day_repair_plan(readonly_store)


def test_empty_ready_plan_rejects_repeated_apply_without_audit(
    tmp_path: Path,
) -> None:
    with DuckDBStore(tmp_path / "empty-plan.duckdb") as store:
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.status == "ready"
        assert plan.before_count == 0
        assert plan.plan_id is not None

        for _ in range(2):
            with pytest.raises(
                data_quality.LimitUpPoolRepairNoOpError,
                match="no candidates",
            ):
                data_quality.apply_limit_up_pool_closed_day_repair(
                    store,
                    plan.plan_id,
                )
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()
        guard_generation = store._conn.execute(
            "SELECT generation FROM limit_up_pool_write_guard "
            "WHERE guard_id = 'limit_up_pool_daily'"
        ).fetchone()

    assert audit_count == (0,)
    assert guard_generation == (0,)


def test_apply_deletes_exact_candidates_and_persists_matching_audit(
    tmp_path: Path,
) -> None:
    assert hasattr(
        data_quality,
        "apply_limit_up_pool_closed_day_repair",
    )
    apply_repair = data_quality.apply_limit_up_pool_closed_day_repair
    closed = date(2026, 7, 12)
    opened = date(2026, 7, 13)
    with DuckDBStore(tmp_path / "apply.duckdb") as store:
        _seed_calendar(store, [(closed, False), (opened, True)])
        _seed_pool(
            store,
            [
                ("600001.SH", closed, "source-b"),
                ("600001.SH", closed, "source-a"),
                ("000001.SZ", opened, "source-a"),
            ],
        )
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None

        result = apply_repair(
            store,
            plan.plan_id,
            applied_at=UPDATED_AT,
        )
        remaining = store._conn.execute(
            "SELECT ts_code, trade_date, source FROM limit_up_pool_daily "
            "ORDER BY ts_code, trade_date, source"
        ).fetchall()
        audit = store._conn.execute(
            "SELECT audit_id, plan_id, action_id, dataset_id, target_table, "
            "key_columns, candidate_keys, before_count, deleted_count, "
            "after_count, strftime(applied_at AT TIME ZONE 'UTC', "
            "'%Y-%m-%dT%H:%M:%S.%fZ') FROM data_repair_audit"
        ).fetchone()

    assert result.plan_id == plan.plan_id
    assert result.before_count == 2
    assert result.deleted_count == 2
    assert result.after_count == 0
    assert remaining == [("000001.SZ", opened, "source-a")]
    assert audit is not None
    assert audit[:5] == (
        result.audit_id,
        plan.plan_id,
        plan.action_id,
        plan.dataset_id,
        plan.target_table,
    )
    assert tuple(json.loads(audit[5])) == plan.key_columns
    assert json.loads(audit[6]) == [
        key.model_dump(mode="json") for key in plan.candidate_keys
    ]
    assert audit[7:10] == (2, 2, 0)
    assert audit[10] == "2026-07-14T00:00:00.000000Z"


def test_repair_audit_applied_at_supports_native_timestamptz_fetchall(
    tmp_path: Path,
) -> None:
    closed = date(2026, 7, 12)
    with DuckDBStore(tmp_path / "native-timestamptz.duckdb") as store:
        _seed_calendar(store, [(closed, False)])
        _seed_pool(store, [("600001.SH", closed, "eastmoney")])
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None
        data_quality.apply_limit_up_pool_closed_day_repair(
            store,
            plan.plan_id,
            applied_at=UPDATED_AT,
        )

        rows = store._conn.execute(
            "SELECT applied_at FROM data_repair_audit"
        ).fetchall()

    assert rows == [(UPDATED_AT,)]


def test_apply_plan_id_mismatch_deletes_nothing_and_writes_no_audit(
    tmp_path: Path,
) -> None:
    closed = date(2026, 7, 12)
    with DuckDBStore(tmp_path / "mismatch.duckdb") as store:
        _seed_calendar(store, [(closed, False)])
        _seed_pool(store, [("600001.SH", closed, "eastmoney")])

        with pytest.raises(
            data_quality.LimitUpPoolRepairPlanMismatchError,
            match="plan id mismatch",
        ):
            data_quality.apply_limit_up_pool_closed_day_repair(
                store,
                "0" * 64,
            )
        pool_count = store._conn.execute(
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone()
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert pool_count == (1,)
    assert audit_count == (0,)


def test_apply_recomputes_candidates_and_rejects_stale_plan(
    tmp_path: Path,
) -> None:
    closed = date(2026, 7, 12)
    with DuckDBStore(tmp_path / "cas.duckdb") as store:
        _seed_calendar(store, [(closed, False)])
        _seed_pool(store, [("600001.SH", closed, "eastmoney")])
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None
        _seed_pool(store, [("000001.SZ", closed, "eastmoney")])

        with pytest.raises(data_quality.LimitUpPoolRepairPlanMismatchError):
            data_quality.apply_limit_up_pool_closed_day_repair(
                store,
                plan.plan_id,
            )
        rows = store._conn.execute(
            "SELECT ts_code FROM limit_up_pool_daily ORDER BY ts_code"
        ).fetchall()
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert rows == [("000001.SZ",), ("600001.SH",)]
    assert audit_count == (0,)


def test_apply_blocks_if_calendar_becomes_unknown_after_dry_run(
    tmp_path: Path,
) -> None:
    closed = date(2026, 7, 12)
    with DuckDBStore(tmp_path / "apply-unknown.duckdb") as store:
        _seed_calendar(store, [(closed, False)])
        _seed_pool(store, [("600001.SH", closed, "eastmoney")])
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None
        store._conn.execute(
            "DELETE FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = ?",
            [closed],
        )

        with pytest.raises(
            data_quality.LimitUpPoolRepairBlockedError,
            match="unknown trade calendar",
        ) as caught:
            data_quality.apply_limit_up_pool_closed_day_repair(
                store,
                plan.plan_id,
            )
        pool_count = store._conn.execute(
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone()
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert caught.value.plan.status == "blocked"
    assert caught.value.plan.severity == "P0"
    assert pool_count == (1,)
    assert audit_count == (0,)


def test_apply_rolls_back_delete_when_after_check_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed = date(2026, 7, 12)
    with DuckDBStore(tmp_path / "after-check.duckdb") as store:
        _seed_calendar(store, [(closed, False)])
        _seed_pool(store, [("600001.SH", closed, "eastmoney")])
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None
        original = data_quality._load_limit_up_pool_repair_plan
        calls = 0

        def fail_second_check(
            current_store: DuckDBStore,
        ) -> data_quality.LimitUpPoolRepairPlan:
            nonlocal calls
            calls += 1
            if calls == 2:
                raise RuntimeError("after-check failed")
            return original(current_store)

        monkeypatch.setattr(
            data_quality,
            "_load_limit_up_pool_repair_plan",
            fail_second_check,
        )
        with pytest.raises(RuntimeError, match="after-check failed"):
            data_quality.apply_limit_up_pool_closed_day_repair(
                store,
                plan.plan_id,
            )
        pool_count = store._conn.execute(
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone()
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert calls == 2
    assert pool_count == (1,)
    assert audit_count == (0,)


def test_apply_rolls_back_delete_and_audit_on_base_exception(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    closed = date(2026, 7, 12)
    with DuckDBStore(tmp_path / "audit-failure.duckdb") as store:
        _seed_calendar(store, [(closed, False)])
        _seed_pool(store, [("600001.SH", closed, "eastmoney")])
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(store)
        assert plan.plan_id is not None
        original = data_quality._insert_data_repair_audit

        def insert_then_interrupt(
            current_store: DuckDBStore,
            audit: data_quality.DataRepairAudit,
        ) -> None:
            original(current_store, audit)
            raise KeyboardInterrupt("audit interrupted")

        monkeypatch.setattr(
            data_quality,
            "_insert_data_repair_audit",
            insert_then_interrupt,
        )
        with pytest.raises(KeyboardInterrupt, match="audit interrupted"):
            data_quality.apply_limit_up_pool_closed_day_repair(
                store,
                plan.plan_id,
            )
        pool_count = store._conn.execute(
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone()
        audit_count = store._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert pool_count == (1,)
    assert audit_count == (0,)


def test_apply_prevents_calendar_change_after_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "calendar-concurrency.duckdb"
    closed = date(2026, 7, 12)
    with DuckDBStore(db_path) as setup:
        _seed_calendar(setup, [(closed, False)])
        _seed_pool(setup, [("600001.SH", closed, "eastmoney")])

    with DuckDBStore(db_path) as repair_store, DuckDBStore(db_path) as writer_store:
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(
            repair_store
        )
        assert plan.plan_id is not None
        original = data_quality._delete_limit_up_pool_repair_candidates
        writer_outcomes: list[str] = []

        def update_calendar_then_delete(
            current_store: DuckDBStore,
            candidate_keys: tuple[data_quality.LimitUpPoolRepairKey, ...],
        ) -> tuple[data_quality.LimitUpPoolRepairKey, ...]:
            try:
                writer_store._conn.execute(
                    "UPDATE trade_calendar SET is_open = TRUE "
                    "WHERE exchange = 'SSE' AND cal_date = ?",
                    [closed],
                )
            except duckdb.TransactionException:
                writer_outcomes.append("conflicted")
            else:
                writer_outcomes.append("committed")
            return original(current_store, candidate_keys)

        monkeypatch.setattr(
            data_quality,
            "_delete_limit_up_pool_repair_candidates",
            update_calendar_then_delete,
        )
        data_quality.apply_limit_up_pool_closed_day_repair(
            repair_store,
            plan.plan_id,
        )

    with DuckDBStore(db_path, read_only=True) as check:
        calendar = check._conn.execute(
            "SELECT is_open FROM trade_calendar "
            "WHERE exchange = 'SSE' AND cal_date = ?",
            [closed],
        ).fetchone()
        pool_count = check._conn.execute(
            "SELECT COUNT(*) FROM limit_up_pool_daily"
        ).fetchone()
        audit_count = check._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert writer_outcomes == ["conflicted"]
    assert calendar == (False,)
    assert pool_count == (0,)
    assert audit_count == (1,)


def test_apply_prevents_candidate_insert_after_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    db_path = tmp_path / "candidate-concurrency.duckdb"
    closed = date(2026, 7, 12)
    with DuckDBStore(db_path) as setup:
        _seed_calendar(setup, [(closed, False)])
        _seed_pool(setup, [("600001.SH", closed, "eastmoney")])

    with DuckDBStore(db_path) as repair_store, DuckDBStore(db_path) as writer_store:
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(
            repair_store
        )
        assert plan.plan_id is not None
        original = data_quality._delete_limit_up_pool_repair_candidates
        writer_outcomes: list[str] = []

        def insert_candidate_then_delete(
            current_store: DuckDBStore,
            candidate_keys: tuple[data_quality.LimitUpPoolRepairKey, ...],
        ) -> tuple[data_quality.LimitUpPoolRepairKey, ...]:
            try:
                writer_store.upsert_limit_up_pool(
                    _pool_frame("000001.SZ", closed, "eastmoney")
                )
            except duckdb.TransactionException:
                writer_outcomes.append("conflicted")
            else:
                writer_outcomes.append("committed")
            return original(current_store, candidate_keys)

        monkeypatch.setattr(
            data_quality,
            "_delete_limit_up_pool_repair_candidates",
            insert_candidate_then_delete,
        )
        data_quality.apply_limit_up_pool_closed_day_repair(
            repair_store,
            plan.plan_id,
        )

    with DuckDBStore(db_path, read_only=True) as check:
        pool_rows = check._conn.execute(
            "SELECT ts_code FROM limit_up_pool_daily ORDER BY ts_code"
        ).fetchall()
        audit_count = check._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert writer_outcomes == ["conflicted"]
    assert pool_rows == []
    assert audit_count == (1,)


def test_apply_rolls_back_when_candidate_writer_wins_guard(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "candidate-writer-wins.duckdb"
    closed = date(2026, 7, 12)
    with DuckDBStore(db_path) as setup:
        _seed_calendar(setup, [(closed, False)])
        _seed_pool(setup, [("600001.SH", closed, "eastmoney")])

    with DuckDBStore(db_path) as repair_store, DuckDBStore(db_path) as writer_store:
        plan = data_quality.build_limit_up_pool_closed_day_repair_plan(
            repair_store
        )
        assert plan.plan_id is not None
        writer_store._conn.execute("BEGIN")
        writer_store.upsert_limit_up_pool(
            _pool_frame("000001.SZ", closed, "eastmoney")
        )

        with pytest.raises(duckdb.TransactionException, match="Conflict on"):
            data_quality.apply_limit_up_pool_closed_day_repair(
                repair_store,
                plan.plan_id,
            )
        writer_store._conn.execute("COMMIT")

    with DuckDBStore(db_path, read_only=True) as check:
        pool_rows = check._conn.execute(
            "SELECT ts_code FROM limit_up_pool_daily ORDER BY ts_code"
        ).fetchall()
        audit_count = check._conn.execute(
            "SELECT COUNT(*) FROM data_repair_audit"
        ).fetchone()

    assert pool_rows == [("000001.SZ",), ("600001.SH",)]
    assert audit_count == (0,)
