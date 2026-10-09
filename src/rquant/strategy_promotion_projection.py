"""Original manual facts projected with explicit private owner columns."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from rquant.runtime_contracts import normalize_aware_utc
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    _projection_json_bytes,
)
from rquant.strategy_authoring import StrategyAuthoringStore
from rquant.strategy_authoring_commands import StrategyAuthoringIdentity
from rquant.strategy_promotion_contracts import StrategyPromotionReview, StrategyPromotionState
from rquant.strategy_promotion_projection_contract import (
    MAX_CELL_BYTES,
    MAX_PRIVATE_BYTES,
    MAX_REVIEW_BYTES,
    MAX_STATE_BYTES,
    PRIVATE_TABLES,
    StrategyPromotionPrivateSnapshot,
    StrategyPromotionReviewFact,
    StrategyPromotionStateFact,
)


class StrategyPromotionProjectionReader:
    def __init__(self, store: StrategyAuthoringStore) -> None:
        if type(store) is not StrategyAuthoringStore:
            raise TypeError("manual projection requires the original private metadata owner")
        self.store = store

    def snapshot(self, observed_at: datetime) -> StrategyPromotionPrivateSnapshot:
        observed = normalize_aware_utc(observed_at)
        identity = self.store.identity()
        states, reviews = self.store.promotion_snapshot()
        approvals = self.store.promotion_approvals()
        indexed = {value.approval_id: value for value in approvals}
        facts = []
        for state in states:
            approval = indexed.get(state.latest_approval_hash)
            if state.revision > 0 and (approval is None or approval.after != state):
                raise ValueError("manual stage lacks its unchanged original approval")
            facts.append(
                StrategyPromotionStateFact(
                    owner_id=state.target.owner_id,
                    target_key=state.target.version_key,
                    state=state,
                    applied_at=None if approval is None else approval.applied_at,
                )
            )
        snapshot = StrategyPromotionPrivateSnapshot(
            metadata_identity=identity,
            observed_at=observed,
            states=tuple(facts),
            reviews=tuple(
                StrategyPromotionReviewFact(
                    owner_id=value.actor_id, review_id=value.review_id, review=value
                )
                for value in reviews
            ),
        )
        if (
            self.store.identity() != identity
            or self.store.promotion_snapshot() != (states, reviews)
            or self.store.promotion_approvals() != approvals
        ):
            raise ValueError("manual original source changed during private publication")
        return snapshot

    def __call__(self, observed_at: datetime) -> tuple[ServingProjectionPayload, ...]:
        snapshot = self.snapshot(observed_at)
        state_rows = tuple(
            {
                "owner_id": fact.owner_id,
                "target_key": fact.target_key,
                "strategy_id": fact.state.target.strategy_id,
                "version": fact.state.target.head.version,
                "state_json": fact.state.model_dump_json(),
                "applied_at": None if fact.applied_at is None else fact.applied_at.isoformat(),
            }
            for fact in snapshot.states
        )
        review_rows = tuple(
            {
                "owner_id": fact.owner_id,
                "review_id": fact.review_id,
                "target_key": fact.review.target.version_key,
                "review_json": fact.review.model_dump_json(),
                "observed_at": fact.review.observed_at.isoformat(),
            }
            for fact in snapshot.reviews
        )
        owners = sorted(
            {f.owner_id for f in snapshot.states} | {f.owner_id for f in snapshot.reviews}
        )
        window_rows = tuple(
            {
                "owner_id": owner,
                "state_count": sum(f.owner_id == owner for f in snapshot.states),
                "review_count": sum(f.owner_id == owner for f in snapshot.reviews),
                "metadata_json": snapshot.metadata_identity.model_dump_json(),
            }
            for owner in owners
        )
        rows = (state_rows, review_rows, window_rows)
        if any(
            len(str(cell).encode()) > MAX_CELL_BYTES
            for table in rows
            for row in table
            for cell in row.values()
            if cell is not None
        ):
            raise ValueError("manual private cell exceeds original bound")
        payloads = tuple(
            ServingProjectionPayload(table_name=name, available_at=snapshot.observed_at, rows=table)
            for name, table in zip(PRIVATE_TABLES, rows, strict=True)
        )
        sizes = tuple(_projection_json_bytes(payload) for payload in payloads)
        if (
            sizes[0] > MAX_STATE_BYTES
            or sizes[1] > MAX_REVIEW_BYTES
            or sum(sizes) > MAX_PRIVATE_BYTES
        ):
            raise ValueError("manual private layouts exceed original promotions allocation")
        validate_strategy_promotion_projections(
            {payload.table_name: payload for payload in payloads}
        )
        return payloads


def validate_strategy_promotion_projections(
    projections: Mapping[str, ServingProjectionInput | ServingProjectionPayload],
) -> None:
    selected = {name: projections[name] for name in PRIVATE_TABLES if name in projections}
    if len(selected) != len(PRIVATE_TABLES):
        raise ValueError("manual private projection is partial")
    if len({p.available_at for p in selected.values()}) != 1:
        raise ValueError("manual private projection mixes observation times")
    observed = selected[PRIVATE_TABLES[0]].available_at
    sizes = tuple(_projection_json_bytes(selected[name]) for name in PRIVATE_TABLES)
    if sizes[0] > MAX_STATE_BYTES or sizes[1] > MAX_REVIEW_BYTES or sum(sizes) > MAX_PRIVATE_BYTES:
        raise ValueError("manual private projection exceeds its original budget")
    states = {}
    reviews = {}
    identities = set()
    for row in selected[PRIVATE_TABLES[0]].rows:
        state = StrategyPromotionState.model_validate_json(row["state_json"])
        applied = (
            None
            if row["applied_at"] is None
            else normalize_aware_utc(datetime.fromisoformat(row["applied_at"]))
        )
        StrategyPromotionStateFact(
            owner_id=row["owner_id"], target_key=row["target_key"], state=state, applied_at=applied
        )
        if (
            (row["strategy_id"], row["version"])
            != (state.target.strategy_id, state.target.head.version)
            or applied is not None
            and applied > observed
        ):
            raise ValueError("manual private state has changed index or future approval")
        states[(row["owner_id"], row["target_key"])] = state
    for row in selected[PRIVATE_TABLES[1]].rows:
        review = StrategyPromotionReview.model_validate_json(row["review_json"])
        StrategyPromotionReviewFact(
            owner_id=row["owner_id"], review_id=row["review_id"], review=review
        )
        if (
            row["target_key"] != review.target.version_key
            or normalize_aware_utc(datetime.fromisoformat(row["observed_at"])) != review.observed_at
            or review.observed_at > observed
            or states.get((row["owner_id"], row["target_key"])) is None
        ):
            raise ValueError("manual private review index, owner or observation differs")
        reviews[(row["owner_id"], row["review_id"])] = review
        identities.add(review.metadata_identity.model_dump_json())
    window_owners = set()
    for row in selected[PRIVATE_TABLES[2]].rows:
        owner = row["owner_id"]
        window_owners.add(owner)
        identity = StrategyAuthoringIdentity.model_validate_json(row["metadata_json"])
        identities.add(identity.model_dump_json())
        if (row["state_count"], row["review_count"]) != (
            sum(key[0] == owner for key in states),
            sum(key[0] == owner for key in reviews),
        ):
            raise ValueError("manual private owner window count differs")
    if (
        window_owners != {key[0] for key in states} | {key[0] for key in reviews}
        or len(identities) > 1
    ):
        raise ValueError("manual private owner or original metadata identity differs")
