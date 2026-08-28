from __future__ import annotations

import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from rquant.experiment_registry import (
    ExperimentRegistry,
    ExperimentRegistryError,
    ExperimentRegistryReadonlyReader,
    PromotionDecision,
    PromotionStage,
)
from rquant.promotions_serving_authority import (
    PromotionsAuthorityPublisher,
    PromotionsSourceReader,
)
from rquant.runtime_serving_authority import (
    ServingSourceAuthorityPublisher,
    ServingSourceAuthorityReader,
)
from rquant.runtime_serving_snapshot import PROMOTIONS_DATASET_ID, PromotionsPayload
from rquant.serving_contracts import FreshnessStatus

NOW = datetime(2026, 8, 1, 4, 0, tzinfo=UTC)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
COMMIT = "a" * 40


def _decision(*, decided_at: datetime, marker: str) -> PromotionDecision:
    return PromotionDecision(
        stage=PromotionStage.EXPLORATORY,
        experiment_ids=(marker * 64,),
        evidence_artifact_hash=marker * 64,
        decided_at=decided_at,
        approved=True,
        minimum_trade_count=30,
        significance_level=Decimal("0.05"),
        forward_trading_days=0,
        forward_fills=0,
        minimum_forward_days=10,
        minimum_forward_fills=20,
        maximum_forward_drawdown=Decimal("0.10"),
        policy_fingerprint=(marker.upper() if marker != "a" else "9") * 64,
    )


def _insert(path: Path, decision: PromotionDecision, *, decided_at: datetime | None = None) -> None:
    # `with connection:` is a transaction scope, not a close. The connection it leaves open
    # is only reachable through sqlite3's own statement-cache cycle, so it survives until a
    # cyclic collection runs - and SQLite unlinks `-wal`/`-shm` when that finally happens.
    # Which side of a publish that lands on decided whether this file passed.
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            """
            INSERT INTO promotion_decision(
                decision_id, stage, approved, decided_at, payload_json
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                decision.decision_id,
                decision.stage.value,
                int(decision.approved),
                (decided_at or decision.decided_at)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z"),
                decision.model_dump_json(),
            ),
        )


def test_empty_initialized_registry_is_fresh_and_deterministic(tmp_path: Path) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    reader = PromotionsSourceReader(
        registry=ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path),
        limit=10,
    )

    first = reader(NOW)
    repeated = reader(NOW + timedelta(minutes=1))

    assert first == repeated
    assert first.dataset_id == PROMOTIONS_DATASET_ID
    assert first.sequence == 0
    assert first.event_time == EPOCH
    assert first.published_at == EPOCH
    assert first.status is FreshnessStatus.FRESH
    assert first.reason is None
    assert first.payload == PromotionsPayload()


def test_source_reader_publishes_latest_visible_decisions_in_stable_order(
    tmp_path: Path,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    decisions = tuple(
        _decision(decided_at=NOW + timedelta(minutes=index), marker=marker)
        for index, marker in enumerate(("1", "2", "3", "4"), start=1)
    )
    for decision in decisions:
        _insert(path, decision)

    result = PromotionsSourceReader(
        registry=ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path),
        limit=2,
    )(NOW + timedelta(minutes=3))

    assert result.sequence == 3
    assert result.event_time == decisions[2].decided_at
    assert isinstance(result.payload, PromotionsPayload)
    assert result.payload.promotions == decisions[1:3]


def test_source_reader_fails_closed_instead_of_synthesizing_missing_data(
    tmp_path: Path,
) -> None:
    reader = PromotionsSourceReader(
        registry=ExperimentRegistryReadonlyReader(
            tmp_path / "missing.sqlite3",
            managed_trust_root=tmp_path,
        ),
        limit=10,
    )

    with pytest.raises(ExperimentRegistryError, match="does not exist"):
        reader(NOW)


def test_source_reader_rejects_future_payload_hidden_behind_visible_column(
    tmp_path: Path,
) -> None:
    path = tmp_path / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=tmp_path)
    future = _decision(decided_at=NOW + timedelta(seconds=1), marker="5")
    _insert(path, future, decided_at=NOW)

    with pytest.raises(ExperimentRegistryError, match="future|does not match"):
        PromotionsSourceReader(
            registry=ExperimentRegistryReadonlyReader(path, managed_trust_root=tmp_path),
            limit=10,
        )(NOW)


def test_authority_publisher_is_atomic_and_idempotent_for_same_observation(
    tmp_path: Path,
) -> None:
    # A managed trust root is a directory whose identity, ctime included, must not move under
    # the reader holding it. Publishing into a sibling of the registry inside that same root
    # moves it on the first publish, which production never does and this test used to.
    registry_root = tmp_path / "registry"
    registry_root.mkdir(mode=0o700)
    path = registry_root / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=registry_root)
    _insert(path, _decision(decided_at=NOW - timedelta(minutes=1), marker="6"))
    source = PromotionsSourceReader(
        registry=ExperimentRegistryReadonlyReader(path, managed_trust_root=registry_root),
        limit=10,
    )
    authority_root = tmp_path / "authority"
    generic = ServingSourceAuthorityPublisher(
        root=authority_root,
        producer_commit=COMMIT,
        dataset_id=PROMOTIONS_DATASET_ID,
        payload_kind="promotions",
        clock=lambda: NOW,
    )
    publisher = PromotionsAuthorityPublisher(reader=source, publisher=generic)

    first = publisher.publish(NOW)
    repeated = publisher.publish(NOW)
    loaded = ServingSourceAuthorityReader(
        root=authority_root,
        expected_producer_commit=COMMIT,
        expected_dataset_id=PROMOTIONS_DATASET_ID,
        expected_payload_kind="promotions",
    )(NOW)

    assert repeated == first
    assert loaded == source(NOW)
    assert tuple((authority_root / "generations").iterdir()) == (
        authority_root / "generations" / f"{first.generation_id}.json",
    )


def test_authority_publisher_rejects_wrong_dataset_owner(tmp_path: Path) -> None:
    registry_root = tmp_path / "registry"
    registry_root.mkdir(mode=0o700)
    path = registry_root / "experiments.sqlite3"
    ExperimentRegistry(path, managed_trust_root=registry_root)
    source = PromotionsSourceReader(
        registry=ExperimentRegistryReadonlyReader(path, managed_trust_root=registry_root),
        limit=10,
    )

    with pytest.raises(ValueError, match="promotions dataset"):
        PromotionsAuthorityPublisher(
            reader=source,
            publisher=ServingSourceAuthorityPublisher(
                root=tmp_path / "authority",
                producer_commit=COMMIT,
                dataset_id="signals",
                payload_kind="promotions",
                clock=lambda: NOW,
            ),
        )
