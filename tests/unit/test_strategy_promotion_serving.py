"""Original promotion layouts; synthetic generation header and real DuckDB rows."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import duckdb
import pytest

from rquant.experiment_registry import ExperimentRegistry, ExperimentRegistryReadonlyReader
from rquant.promotions_serving_authority import PromotionsSourceReader
from rquant.runtime_serving_snapshot import PromotionsPayload
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS
from rquant.strategy_promotion_projection import StrategyPromotionProjectionReader
from rquant.strategy_promotion_projection_contract import PRIVATE_TABLES
from tests.unit.test_strategy_promotion import NOW, saved_target


def reviewed_store(tmp_path: Path):
    from rquant.strategy_promotion import evaluate_review
    from rquant.strategy_promotion_commands import RequestPromotionReview
    from rquant.strategy_promotion_contracts import (
        PromotionEvidenceBundle,
        PromotionEvidenceSelection,
    )

    store, target = saved_target(tmp_path)
    request = RequestPromotionReview(
        command_id=str(uuid4()),
        requested_at=NOW,
        generation_id="synthetic-generation",
        target=target,
        expected_revision=0,
        selection=PromotionEvidenceSelection(family_id="unsealed-family", experiment_id="f" * 64),
    )
    review = evaluate_review(
        request,
        state=store.promotion_state(target),
        bundle=PromotionEvidenceBundle(target=target, observed_at=NOW, missing=("尚未封存",)),
        actor_id="alice",
        metadata_identity=store.identity(),
    )
    store.record_promotion_review(request, review)
    return store, target


def borrowed_for(payloads: tuple, cursor: Any, *, generation: str = "a" * 64) -> SimpleNamespace:
    cursor.execute(
        "CREATE TABLE projection_status(table_name VARCHAR,available BOOLEAN,row_count BIGINT,"
        "owner_dataset_id VARCHAR,owner_generation_id VARCHAR,available_at TIMESTAMPTZ)"
    )
    for payload in payloads:
        contract = PAGE_PROJECTION_CONTRACTS[payload.table_name]
        types = {"string": "VARCHAR", "int": "BIGINT", "timestamp": "TIMESTAMPTZ"}
        columns = ",".join(f"{name} {types[kind]}" for name, kind in contract.columns)
        cursor.execute(f"CREATE TABLE {payload.table_name}({columns})")
        for row in payload.rows:
            cursor.execute(
                f"INSERT INTO {payload.table_name} "
                f"VALUES ({','.join('?' for _ in contract.columns)})",
                tuple(row[key] for key in contract.column_names),
            )
        cursor.execute(
            "INSERT INTO projection_status VALUES (?,TRUE,?,'promotions',?,?)",
            (payload.table_name, len(payload.rows), generation, NOW),
        )
    return SimpleNamespace(
        cursor=cursor,
        manifest=SimpleNamespace(
            built_at=NOW,
            row_counts={p.table_name: len(p.rows) for p in payloads},
            watermarks=(SimpleNamespace(dataset_id="promotions", generation_id=generation),),
        ),
    )


def test_original_promotions_source_registers_manual_private_without_shared_decisions(
    tmp_path: Path,
) -> None:
    store, target = reviewed_store(tmp_path)
    path = tmp_path / "original-experiments.sqlite"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    readonly = ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path)
    source = PromotionsSourceReader(
        registry=readonly, strategy_promotion_reader=StrategyPromotionProjectionReader(store)
    )
    actual = source(NOW)
    assert actual.payload.promotions == ()
    assert {p.table_name for p in actual.payload.projections} == set(PRIVATE_TABLES)
    assert all(row.get("owner_id") == "alice" for p in actual.payload.projections for row in p.rows)
    assert PromotionsSourceReader(registry=readonly)(NOW).payload == PromotionsPayload()


def test_manual_reader_filters_owner_before_read_and_checks_original_generation(
    tmp_path: Path,
) -> None:
    from rquant.web.strategy_promotion_reader import read_strategy_promotion

    store, target = reviewed_store(tmp_path)
    payloads = StrategyPromotionProjectionReader(store)(NOW)
    with duckdb.connect(":memory:") as cursor:
        borrowed = borrowed_for(payloads, cursor)
        alice = read_strategy_promotion(borrowed, owner_id="alice")
        assert alice.available_at == NOW and alice.states[0].state.target == target
        other = read_strategy_promotion(borrowed, owner_id="bob")
        assert other.states == other.reviews == () and other.metadata_identity is None
        cursor.execute(
            "UPDATE strategy_manual_state SET state_json='invalid-private-body' "
            "WHERE owner_id='alice'"
        )
        assert read_strategy_promotion(borrowed, owner_id="bob").states == ()
        with pytest.raises(ValueError):
            read_strategy_promotion(borrowed, owner_id="alice")
        cursor.execute(
            "UPDATE projection_status SET owner_generation_id=? WHERE table_name=?",
            ("b" * 64, PRIVATE_TABLES[1]),
        )
        with pytest.raises(ValueError, match="generation"):
            read_strategy_promotion(borrowed, owner_id="bob")


def test_partial_projection_and_physical_window_counts_reject(tmp_path: Path) -> None:
    from rquant.web.strategy_promotion_reader import read_strategy_promotion

    store, target = reviewed_store(tmp_path)
    payloads = StrategyPromotionProjectionReader(store)(NOW)
    with duckdb.connect(":memory:") as cursor:
        borrowed = borrowed_for(payloads, cursor)
        cursor.execute("UPDATE strategy_manual_window SET state_count=2")
        with pytest.raises(ValueError, match="count"):
            read_strategy_promotion(borrowed, owner_id="alice")
        cursor.execute(
            "UPDATE projection_status SET available=FALSE WHERE table_name=?", (PRIVATE_TABLES[1],)
        )
        with pytest.raises(ValueError, match="partial"):
            read_strategy_promotion(borrowed, owner_id="alice")
