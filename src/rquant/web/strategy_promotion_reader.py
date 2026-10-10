"""Read the original manual facts only after the private owner WHERE clause."""

from __future__ import annotations

from datetime import datetime

from rquant.factor_definition_admission import _ACTOR_ADAPTER
from rquant.runtime_contracts import AwareUtcDatetime, RuntimeContractModel
from rquant.serving_read_models import PAGE_PROJECTION_CONTRACTS, ServingProjectionPayload
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_promotion_contracts import StrategyPromotionReview, StrategyPromotionState
from rquant.strategy_promotion_projection import validate_strategy_promotion_projections
from rquant.strategy_promotion_projection_contract import (
    PRIVATE_TABLES,
    StrategyPromotionReviewFact,
    StrategyPromotionStateFact,
)
from rquant.web.serving import BorrowedGeneration


class StrategyPromotionPublishedFacts(RuntimeContractModel):
    available_at: AwareUtcDatetime
    metadata_identity: StrategyAuthoringIdentity | None
    states: tuple[StrategyPromotionStateFact, ...]
    reviews: tuple[StrategyPromotionReviewFact, ...]


def read_strategy_promotion(
    borrowed: BorrowedGeneration | None, *, owner_id: str
) -> StrategyPromotionPublishedFacts | None:
    _ACTOR_ADAPTER.validate_python(owner_id)
    if borrowed is None:
        return None
    names = tuple(sorted(PRIVATE_TABLES))
    marks = borrowed.cursor.execute(
        "SELECT table_name,available,row_count,owner_dataset_id,owner_generation_id,available_at "
        "FROM projection_status WHERE table_name IN (?,?,?) ORDER BY table_name LIMIT 4",
        names,
    ).fetchall()
    if (
        len(marks) != 3
        or tuple(row[0] for row in marks) != names
        or any(type(row[1]) is not bool or type(row[2]) is not int for row in marks)
    ):
        raise ValueError("manual private projection status is partial")
    if not any(row[1] for row in marks):
        if any(
            row[2] != 0
            or row[3] != "promotions"
            or row[4] is not None
            or row[5] is not None
            or borrowed.manifest.row_counts.get(row[0]) != 0
            for row in marks
        ):
            raise ValueError("unpublished manual projection contains facts")
        return None
    watermark = next(
        (mark for mark in borrowed.manifest.watermarks if mark.dataset_id == "promotions"), None
    )
    at = marks[0][5]
    if not all(row[1] for row in marks):
        raise ValueError("manual private projection is partial")
    if watermark is None or any(
        row[3] != "promotions"
        or row[4] != watermark.generation_id
        or row[5] != at
        or at is None
        or at > borrowed.manifest.built_at
        or row[2] != borrowed.manifest.row_counts.get(row[0])
        for row in marks
    ):
        raise ValueError("manual private projection generation differs")
    projections: dict[str, ServingProjectionPayload] = {}
    for name, _, count, *_ in marks:
        contract = PAGE_PROJECTION_CONTRACTS[name]
        if (
            not 0 <= count <= contract.max_rows
            or borrowed.cursor.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0] != count
        ):
            raise ValueError("manual projection physical count differs")
        values = borrowed.cursor.execute(
            f"SELECT {', '.join(contract.column_names)} FROM {name} "
            f"WHERE owner_id=? ORDER BY {', '.join(contract.sort_keys)} LIMIT ?",
            (owner_id, contract.max_rows + 1),
        ).fetchall()
        if len(values) > contract.max_rows:
            raise ValueError("manual owner projection exceeds capacity")
        projections[name] = ServingProjectionPayload(
            table_name=name,
            available_at=at,
            rows=tuple(
                {
                    key: value.isoformat() if isinstance(value, datetime) else value
                    for key, value in zip(contract.column_names, row, strict=True)
                }
                for row in values
            ),
        )
    validate_strategy_promotion_projections(projections)
    windows = projections[PRIVATE_TABLES[2]].rows
    identity = (
        None
        if not windows
        else StrategyAuthoringIdentity.model_validate_json(windows[0]["metadata_json"])
    )
    return StrategyPromotionPublishedFacts(
        available_at=at,
        metadata_identity=identity,
        states=tuple(
            StrategyPromotionStateFact(
                owner_id=row["owner_id"],
                target_key=row["target_key"],
                state=StrategyPromotionState.model_validate_json(row["state_json"]),
                applied_at=row["applied_at"],
            )
            for row in projections[PRIVATE_TABLES[0]].rows
        ),
        reviews=tuple(
            StrategyPromotionReviewFact(
                owner_id=row["owner_id"],
                review_id=row["review_id"],
                review=StrategyPromotionReview.model_validate_json(row["review_json"]),
            )
            for row in projections[PRIVATE_TABLES[1]].rows
        ),
    )
