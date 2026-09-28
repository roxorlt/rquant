"""Ranked daily pool facts are persisted and verified as one frozen result."""

from __future__ import annotations

import hashlib
import json
import struct
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

import duckdb
import pandas as pd
import pytest
from pydantic import ValidationError

from rquant.pool_result_receipt import (
    ScreenRunReceipt,
    ScreenRunReceiptDraft,
    member_price_digest,
    member_rank_digest,
    member_set_digest,
)
from rquant.runtime_contracts import canonical_sha256
from rquant.storage.duckdb import DuckDBStore
from rquant.storage.migrations import MIGRATIONS, initialize_schema

DAY = date(2026, 8, 4)
DECISION_DAY = date(2026, 8, 5)
STAMP = datetime(2026, 8, 4, 9, tzinfo=UTC)
DEFINITION = "a" * 64
V3 = "screen-run-receipt/v3"


def _receipt_fields() -> dict[str, object]:
    return {
        "trade_date": DAY,
        "preset_name": "pool",
        "definition_version": DEFINITION,
        "hit_count": 1,
        "member_digest": member_set_digest(["A"]),
        "lineage_complete": True,
        "completed_at": STAMP,
    }


def test_rank_digest_is_stable_by_code_and_binds_binary64_score() -> None:
    from rquant.pool_result_receipt import member_rank_digest

    rows = [("B", 2, 50.0), ("A", 1, 100.0)]
    digest = hashlib.sha256()
    digest.update(b"rquant/screen-run-rank/v3\x00")
    digest.update(struct.pack(">Q", 2))
    for code, position, score in (("A", 1, 100.0), ("B", 2, 50.0)):
        code_bytes = code.encode("utf-8")
        digest.update(struct.pack(">I", len(code_bytes)))
        digest.update(code_bytes)
        digest.update(struct.pack(">Q", position))
        digest.update(struct.pack(">d", score))
    assert member_rank_digest(rows) == digest.hexdigest()
    assert member_rank_digest(list(reversed(rows))) == digest.hexdigest()
    assert member_rank_digest([]) != member_rank_digest([("A", 1, 0.0)])
    assert member_rank_digest([("A", 1, -0.0)]) != member_rank_digest([("A", 1, 0.0)])
    assert member_rank_digest([("A", 1, 50.0)]) != member_rank_digest([("A", 1, 50.00000000000001)])


@pytest.mark.parametrize(
    "rows",
    [
        [("A", 1, 50.0), ("A", 2, 60.0)],
        [("A", 1, 50.0), ("B", 1, 60.0)],
        [("A", 2, 50.0)],
        [("A", 1, float("nan"))],
        [("A", 1, float("inf"))],
        [("A", 1, -0.1)],
        [("A", 1, 100.1)],
        [("", 1, 50.0)],
    ],
)
def test_rank_digest_rejects_invalid_members_positions_and_scores(
    rows: list[tuple[str, int, float]],
) -> None:
    from rquant.pool_result_receipt import member_rank_digest

    with pytest.raises(ValueError):
        member_rank_digest(rows)


def test_v3_hash_binds_both_proofs_without_changing_v1_v2_versions() -> None:
    from rquant.pool_result_receipt import member_rank_digest

    fields = _receipt_fields()
    v1 = ScreenRunReceipt.model_validate(fields)
    v2 = ScreenRunReceipt.model_validate(
        {
            **fields,
            "contract": "screen-run-receipt/v2",
            "price_digest": member_price_digest([("A", 10.5)]),
        }
    )
    v3 = ScreenRunReceipt.model_validate(
        {
            **fields,
            "contract": V3,
            "price_digest": member_price_digest([("A", 10.5)]),
            "rank_digest": member_rank_digest([("A", 1, 50.0)]),
        }
    )
    assert v1.result_version == "b0acdee9741d5928154a6fb39a4e8259547beb42dbc20110d7b6a5531abfd0e6"
    assert v2.result_version == "9890e93f99972f6d922fd5016129d03269adea86f7812ea2fb3922d2a057f065"
    assert v3.result_version not in {v1.result_version, v2.result_version}
    with pytest.raises(ValidationError, match="result version"):
        ScreenRunReceipt.model_validate(
            {**v3.model_dump(mode="python"), "rank_digest": member_rank_digest([("A", 1, 60.0)])}
        )
    with pytest.raises(ValidationError, match="rank_digest"):
        ScreenRunReceipt.model_validate({**fields, "contract": V3, "price_digest": v2.price_digest})


def _frame(rows: list[tuple[str, float, int, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "trade_date": DAY.isoformat(),
                "preset_name": "pool",
                "ts_code": code,
                "name": code,
                "close": close,
                "pct_chg": 0.0,
                "extra": None,
                "rank_position": position,
                "ranking_score": score,
            }
            for code, close, position, score in rows
        ],
        columns=(
            "trade_date",
            "preset_name",
            "ts_code",
            "name",
            "close",
            "pct_chg",
            "extra",
            "rank_position",
            "ranking_score",
        ),
    )


def _draft(
    rows: list[tuple[str, float, int, float]], *, definition: str = DEFINITION
) -> ScreenRunReceiptDraft:
    return ScreenRunReceiptDraft(
        contract=V3,
        trade_date=DAY,
        preset_name="pool",
        definition_version=definition,
        hit_count=len(rows),
        member_digest=member_set_digest([code for code, _, _, _ in rows]),
        rank_digest=member_rank_digest(
            [(code, position, score) for code, _, position, score in rows]
        ),
        lineage_complete=True,
        completed_at=STAMP,
    )


def test_rank_columns_are_added_by_one_idempotent_writer_migration() -> None:
    connection = duckdb.connect(":memory:")
    try:
        initialize_schema(connection, migrations=MIGRATIONS[:15])
        old_result = {
            row[1] for row in connection.execute("PRAGMA table_info('screen_result')").fetchall()
        }
        old_receipt = {
            row[1]
            for row in connection.execute("PRAGMA table_info('screen_run_receipt')").fetchall()
        }
        assert "rank_position" not in old_result and "ranking_score" not in old_result
        assert "rank_digest" not in old_receipt

        initialize_schema(connection)
        initialize_schema(connection)
        result = {
            row[1] for row in connection.execute("PRAGMA table_info('screen_result')").fetchall()
        }
        receipt = {
            row[1]
            for row in connection.execute("PRAGMA table_info('screen_run_receipt')").fetchall()
        }
        assert {"rank_position", "ranking_score"} <= result
        assert "rank_digest" in receipt
        assert connection.execute(
            "SELECT COUNT(*) FROM schema_migration WHERE version = 16"
        ).fetchone() == (1,)
    finally:
        connection.close()


def test_ranked_store_seals_persisted_rows_and_zero_hit_then_clears_on_unranked_rerun(
    tmp_path: Path,
) -> None:
    rows = [("B", 20.0, 2, 50.0), ("A", 10.0, 1, 100.0), ("C", 30.0, 3, 0.0)]
    with DuckDBStore(tmp_path / "ranked.duckdb") as store:
        store.replace_screen_result_with_receipt(
            DAY.isoformat(), "pool", _frame(rows), _draft(rows)
        )
        persisted = store._conn.execute(
            "SELECT ts_code, close, rank_position, ranking_score "
            "FROM screen_result ORDER BY ts_code"
        ).fetchall()
        sealed = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert sealed is not None and sealed.contract == V3
        assert persisted == [("A", 10.0, 1, 100.0), ("B", 20.0, 2, 50.0), ("C", 30.0, 3, 0.0)]
        assert sealed.price_digest == member_price_digest(
            [(code, price) for code, price, _, _ in persisted]
        )
        assert sealed.rank_digest == member_rank_digest(
            [(code, position, score) for code, _, position, score in persisted]
        )

        fewer = [("C", 31.0, 1, 75.0)]
        store.replace_screen_result_with_receipt(
            DAY.isoformat(), "pool", _frame(fewer), _draft(fewer)
        )
        assert store._conn.execute(
            "SELECT ts_code FROM screen_result WHERE preset_name = 'pool'"
        ).fetchall() == [("C",)]

        store.replace_screen_result_with_receipt(DAY.isoformat(), "pool", _frame([]), _draft([]))
        empty = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert empty is not None and empty.contract == V3
        assert empty.hit_count == 0 and empty.rank_digest == member_rank_digest([])
        assert store.query_screen_result(DAY.isoformat(), "pool").empty

        unranked = pd.DataFrame(
            [
                {
                    "trade_date": DAY.isoformat(),
                    "preset_name": "pool",
                    "ts_code": "A",
                    "name": "A",
                    "close": 11.0,
                    "pct_chg": 0.0,
                    "extra": None,
                }
            ]
        )
        draft = ScreenRunReceiptDraft(
            trade_date=DAY,
            preset_name="pool",
            definition_version=DEFINITION,
            hit_count=1,
            member_digest=member_set_digest(["A"]),
            lineage_complete=True,
            completed_at=STAMP,
        )
        store.replace_screen_result_with_receipt(DAY.isoformat(), "pool", unranked, draft)
        assert store._conn.execute(
            "SELECT rank_position, ranking_score FROM screen_result WHERE ts_code = 'A'"
        ).fetchone() == (None, None)
        old = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        assert (
            old is not None and old.contract == "screen-run-receipt/v2" and old.rank_digest is None
        )


def test_ranked_store_refuses_changed_persisted_score_and_rolls_back_every_fact(
    tmp_path: Path,
) -> None:
    rows = [("A", 10.0, 1, 100.0)]
    with DuckDBStore(tmp_path / "rollback.duckdb") as store:
        store.replace_screen_result_with_receipt(
            DAY.isoformat(), "pool", _frame(rows), _draft(rows)
        )
        before = store.query_screen_run_receipt(DAY.isoformat(), "pool")
        real_replace = store.replace_screen_result

        def changed(trade_date: str, preset_name: str, frame: pd.DataFrame) -> int:
            altered = frame.copy()
            altered["ranking_score"] = 80.0
            return real_replace(trade_date, preset_name, altered)

        with (
            patch.object(store, "replace_screen_result", side_effect=changed),
            pytest.raises(ValueError, match="rank"),
        ):
            store.replace_screen_result_with_receipt(
                DAY.isoformat(), "pool", _frame(rows), _draft(rows)
            )
        assert store.query_screen_run_receipt(DAY.isoformat(), "pool") == before
        assert store._conn.execute(
            "SELECT close, rank_position, ranking_score FROM screen_result"
        ).fetchone() == (10.0, 1, 100.0)

        changed_rows = [("B", 20.0, 1, 90.0)]
        with (
            patch.object(store, "_upsert_screen_run_receipt", side_effect=RuntimeError("failed")),
            pytest.raises(RuntimeError, match="failed"),
        ):
            store.replace_screen_result_with_receipt(
                DAY.isoformat(), "pool", _frame(changed_rows), _draft(changed_rows)
            )
        assert store.query_screen_run_receipt(DAY.isoformat(), "pool") == before
        assert store._conn.execute(
            "SELECT ts_code, close, rank_position, ranking_score FROM screen_result"
        ).fetchall() == [("A", 10.0, 1, 100.0)]


def test_store_refuses_presealed_v3_without_persisted_readback(tmp_path: Path) -> None:
    rows = [("A", 10.0, 1, 100.0)]
    draft = _draft(rows)
    sealed = ScreenRunReceipt.model_validate(
        {
            **draft.model_dump(mode="python"),
            "price_digest": member_price_digest([("A", 10.0)]),
        }
    )
    with DuckDBStore(tmp_path / "presealed.duckdb") as store:
        with pytest.raises(ValueError, match="draft|persisted"):
            store.replace_screen_result_with_receipt(
                DAY.isoformat(), "pool", _frame(rows), sealed
            )
        assert store.query_screen_result(DAY.isoformat(), "pool").empty


def _saved_ranked_definition(directory: Path, *, top_n: int = 2) -> str:
    directory.mkdir(parents=True)
    raw = {
        "schema_version": 3,
        "source": "page_control_v3",
        "name": "pool",
        "description": "日终池子",
        "rules": [{"name": "not_st", "args": {}}],
        "include_columns": ["PCT_CHG[0]"],
        "delay_days": 0,
        "ranking": {
            "conditions": [{"metric": "PCT_CHG[0]", "ascending": False, "weight": 100}],
            "top_n": top_n,
        },
    }
    (directory / "pool.json").write_text(json.dumps(raw), encoding="utf-8")
    return canonical_sha256(raw)


def _daily_frame(changes: list[tuple[str, float | None]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ts_code": [code for code, _ in changes],
            "name": [code for code, _ in changes],
            "CLOSE[0]": [10.0 + index for index, _ in enumerate(changes)],
            "PCT_CHG[0]": [change for _, change in changes],
        }
    )


def test_daily_writer_ranks_only_after_blacklist_and_seals_actual_top_n(
    tmp_path: Path,
) -> None:
    from rquant.pipeline import run_daily_screen_stage

    definition = _saved_ranked_definition(tmp_path / "presets")
    frame = _daily_frame([("A", 9.0), ("B", 2.0), ("C", 3.0)])
    with DuckDBStore(tmp_path / "daily.duckdb") as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES ('A', ?, 10.0)", [DAY]
        )
        with (
            patch("rquant.pipeline.screen", return_value=frame),
            patch("rquant.pipeline.load_active_blacklist", return_value={"A": object()}),
        ):
            result = run_daily_screen_stage(
                DAY.isoformat(),
                preset_names=["user/pool"],
                store=store,
                preset_directory=tmp_path / "presets",
            )
        receipt = store.query_screen_run_receipt(DAY.isoformat(), "user/pool")
        rows = store._conn.execute(
            "SELECT ts_code, close, rank_position, ranking_score FROM screen_result "
            "WHERE preset_name = 'user/pool' ORDER BY rank_position"
        ).fetchall()
    assert result.errors == () and result.preset_hits == {"user/pool": 2}
    assert rows == [("C", 12.0, 1, 100.0), ("B", 11.0, 2, 50.0)]
    assert receipt is not None and receipt.contract == V3
    assert receipt.definition_version == definition
    assert receipt.rank_digest == member_rank_digest(
        [(code, position, score) for code, _, position, score in rows]
    )


def test_daily_writer_ties_missing_scores_and_zero_hit_have_ranked_receipts(
    tmp_path: Path,
) -> None:
    from rquant.pipeline import run_daily_screen_stage

    _saved_ranked_definition(tmp_path / "presets", top_n=3)
    frame = _daily_frame([("B", 4.0), ("A", 4.0), ("C", None)])
    with DuckDBStore(tmp_path / "ties.duckdb") as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES ('A', ?, 10.0)", [DAY]
        )
        with (
            patch("rquant.pipeline.screen", return_value=frame),
            patch("rquant.pipeline.load_active_blacklist", return_value={}),
        ):
            result = run_daily_screen_stage(
                DAY.isoformat(),
                preset_names=["user/pool"],
                store=store,
                preset_directory=tmp_path / "presets",
            )
        rows = store._conn.execute(
            "SELECT ts_code, rank_position, ranking_score FROM screen_result "
            "WHERE preset_name = 'user/pool' ORDER BY rank_position"
        ).fetchall()
        assert result.errors == () and rows == [("A", 1, 75.0), ("B", 2, 75.0), ("C", 3, 0.0)]

        empty = frame.iloc[0:0].copy()
        with patch("rquant.pipeline.screen", return_value=empty):
            result = run_daily_screen_stage(
                DAY.isoformat(),
                preset_names=["user/pool"],
                store=store,
                preset_directory=tmp_path / "presets",
            )
        receipt = store.query_screen_run_receipt(DAY.isoformat(), "user/pool")
        assert result.errors == () and result.preset_hits == {"user/pool": 0}
        assert receipt is not None and receipt.contract == V3
        assert receipt.rank_digest == member_rank_digest([])
        assert store.query_screen_result(DAY.isoformat(), "user/pool").empty


def test_ranked_pool_with_no_parent_members_still_seals_zero_hit(tmp_path: Path) -> None:
    from rquant.builtin_presets import ScreenPreset
    from rquant.pipeline import run_daily_screen_stage
    from rquant.screen.pool_ranking import PoolRankingPlan

    preset = ScreenPreset(
        name="user/pool", description="child", rules=[], depends_on="n-shape-pool1",
        offset_days=1, definition_version=DEFINITION,
        ranking=PoolRankingPlan.model_validate({
            "conditions": [{"metric": "PCT_CHG[0]", "ascending": False, "weight": 100}],
            "top_n": 2,
        }),
    )
    with DuckDBStore(tmp_path / "no-parent.duckdb") as store:
        store._conn.execute(
            "INSERT INTO daily_bar (ts_code, trade_date, close) VALUES ('A', ?, 10.0)", [DAY]
        )
        with patch("rquant.pipeline.load_user_presets", return_value={"user/pool": preset}):
            result = run_daily_screen_stage(
                DAY.isoformat(), preset_names=["user/pool"], store=store,
                preset_directory=tmp_path / "unused",
            )
        receipt = store.query_screen_run_receipt(DAY.isoformat(), "user/pool")
        assert result.errors == () and result.preset_hits == {"user/pool": 0}
        assert receipt is not None and receipt.contract == V3 and receipt.hit_count == 0
        assert receipt.rank_digest == member_rank_digest([])
        assert store.query_screen_result(DAY.isoformat(), "user/pool").empty


def _ranked_file(
    path: Path,
    rows: list[tuple[str, float, int, float]] | None = None,
    *,
    definition: str = DEFINITION,
    completed_at: datetime = STAMP,
) -> Path:
    rows = [("A", 10.0, 1, 100.0), ("B", 20.0, 2, 50.0)] if rows is None else rows
    with DuckDBStore(path) as store:
        draft = ScreenRunReceiptDraft.model_validate(
            {
                **_draft(rows, definition=definition).model_dump(mode="python"),
                "completed_at": completed_at,
            }
        )
        store.replace_screen_result_with_receipt(DAY.isoformat(), "pool", _frame(rows), draft)
    return path


def test_ranked_frozen_reader_needs_external_definition_version_and_returns_ranks(
    tmp_path: Path,
) -> None:
    from rquant.backtest.screen_source import (
        ScreenCandidateSourceError,
        load_verified_ranked_screen_candidates,
        verify_ranked_screen_candidates,
    )

    a = _ranked_file(tmp_path / "a.duckdb")
    b = _ranked_file(tmp_path / "b.duckdb", definition="b" * 64)
    snapshot = load_verified_ranked_screen_candidates(
        a, DAY, DECISION_DAY, "pool", expected_definition_version=DEFINITION
    )
    assert snapshot.receipt.contract == V3
    assert [
        (
            candidate.ts_code,
            candidate.previous_close,
            candidate.rank_position,
            candidate.ranking_score,
        )
        for candidate in snapshot.candidates
    ] == [
        ("A", 10.0, 1, 100.0),
        ("B", 20.0, 2, 50.0),
    ]
    assert snapshot.result_version == snapshot.receipt.result_version
    with duckdb.connect(str(a), read_only=True) as connection:
        connection.execute("BEGIN")
        same = verify_ranked_screen_candidates(
            connection, DAY, DECISION_DAY, "pool", expected_definition_version=DEFINITION
        )
        connection.execute("COMMIT")
    assert same == snapshot

    for path, expected in ((a, None), (a, "b" * 64), (b, DEFINITION)):
        with pytest.raises(ScreenCandidateSourceError, match="definition"):
            load_verified_ranked_screen_candidates(
                path, DAY, DECISION_DAY, "pool", expected_definition_version=expected
            )
    assert (
        load_verified_ranked_screen_candidates(
            b, DAY, DECISION_DAY, "pool", expected_definition_version="b" * 64
        ).receipt.definition_version
        == "b" * 64
    )
    with pytest.raises(ScreenCandidateSourceError, match="date"):
        load_verified_ranked_screen_candidates(
            a, DECISION_DAY, DAY, "pool", expected_definition_version=DEFINITION
        )


def test_ranked_frozen_reader_verifies_zero_hit_and_rejects_v2(tmp_path: Path) -> None:
    from rquant.backtest.screen_source import (
        ScreenCandidateSourceError,
        load_verified_ranked_screen_candidates,
    )

    empty = _ranked_file(tmp_path / "empty.duckdb", [])
    snapshot = load_verified_ranked_screen_candidates(
        empty, DAY, DECISION_DAY, "pool", expected_definition_version=DEFINITION
    )
    assert snapshot.candidates == () and snapshot.receipt.rank_digest == member_rank_digest([])

    v2_path = tmp_path / "v2.duckdb"
    with DuckDBStore(v2_path) as store:
        plain = _frame([("A", 10.0, 1, 100.0)]).drop(columns=["rank_position", "ranking_score"])
        draft = ScreenRunReceiptDraft(
            trade_date=DAY,
            preset_name="pool",
            definition_version=DEFINITION,
            hit_count=1,
            member_digest=member_set_digest(["A"]),
            lineage_complete=True,
            completed_at=STAMP,
        )
        store.replace_screen_result_with_receipt(DAY.isoformat(), "pool", plain, draft)
    with pytest.raises(ScreenCandidateSourceError, match="v3"):
        load_verified_ranked_screen_candidates(
            v2_path, DAY, DECISION_DAY, "pool", expected_definition_version=DEFINITION
        )


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE screen_result SET ranking_score = 80 WHERE ts_code = 'A'",
        "UPDATE screen_result SET rank_position = 2 WHERE ts_code = 'A'",
        "UPDATE screen_result SET rank_position = 3 WHERE ts_code = 'B'",
        "UPDATE screen_result SET ranking_score = CAST('NaN' AS DOUBLE) WHERE ts_code = 'A'",
        "UPDATE screen_result SET ranking_score = CAST('Inf' AS DOUBLE) WHERE ts_code = 'A'",
        "DELETE FROM screen_result WHERE ts_code = 'A'",
        "ALTER TABLE screen_result DROP COLUMN rank_position",
        "ALTER TABLE screen_run_receipt DROP COLUMN rank_digest",
        "DELETE FROM screen_run_receipt",
    ],
)
def test_ranked_frozen_reader_rejects_tampered_rows_or_missing_proofs(
    tmp_path: Path,
    sql: str,
) -> None:
    from rquant.backtest.screen_source import (
        ScreenCandidateSourceError,
        load_verified_ranked_screen_candidates,
    )

    path = _ranked_file(tmp_path / "tampered.duckdb")
    with duckdb.connect(str(path)) as connection:
        connection.execute(sql)
    with pytest.raises(ScreenCandidateSourceError):
        load_verified_ranked_screen_candidates(
            path, DAY, DECISION_DAY, "pool", expected_definition_version=DEFINITION
        )


@pytest.mark.parametrize(
    "completed_at",
    [datetime(2026, 8, 4, 6, 59, tzinfo=UTC), datetime(2026, 8, 5, 1, 25, tzinfo=UTC)],
)
def test_ranked_frozen_reader_enforces_market_close_and_preopen_cutoff(
    tmp_path: Path,
    completed_at: datetime,
) -> None:
    from rquant.backtest.screen_source import (
        ScreenCandidateSourceError,
        load_verified_ranked_screen_candidates,
    )

    path = _ranked_file(tmp_path / "wrong-time.duckdb", completed_at=completed_at)
    with pytest.raises(ScreenCandidateSourceError):
        load_verified_ranked_screen_candidates(
            path, DAY, DECISION_DAY, "pool", expected_definition_version=DEFINITION
        )
