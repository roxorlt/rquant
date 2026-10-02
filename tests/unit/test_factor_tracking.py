"""Tracking contributions and original toggles are a separate research authority."""

from datetime import UTC, date, datetime, timedelta
from math import prod
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import ValidationError


def _day(index: int, *, value: float = -0.1, status: str = "complete") -> object:
    from rquant.factor.evaluate import CorrelationResult
    from rquant.factor.tracking import FactorTrackingDay

    return FactorTrackingDay(
        trade_date=date(2026, 8, 1) + timedelta(days=index),
        rank_ic=CorrelationResult(
            status="ok", value=value, source_sample_count=10, effective_sample_count=10
        ),
        status=status,
        expected_count=10,
        valid_count=10 if status == "complete" else 9,
        low_return=0.001 * index,
        high_return=0.002 * index,
    )


def _registry(tmp_path: Path) -> tuple[object, object, object]:
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.expression import FeatureCatalog
    from rquant.factor.registry import (
        FactorDefinitionRegistry,
        FactorHeadRef,
        SaveFactorDefinitionRequest,
    )

    registry = FactorDefinitionRegistry(tmp_path / "definitions.sqlite")
    identity = registry.initialize()
    definition = build_factor_definition(
        factor_id="tracked_test",
        name_zh="合成跟踪",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression="close",
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    receipt = registry.save(
        SaveFactorDefinitionRequest(command_id="save", definition=definition, expected_head=None),
        expected_identity=identity,
    )
    return registry, identity, FactorHeadRef(version=1, content_sha256=receipt.content_sha256)


def _request(head: object, **changes: object) -> object:
    from rquant.factor.tracking import FactorTrackingRequest

    return FactorTrackingRequest(
        command_id=str(uuid4()),
        requested_at=datetime(2026, 9, 1, tzinfo=UTC),
        serving_generation_id="a" * 64,
        factor_id="tracked_test",
        tracked=True,
        expected_head=head,
        **changes,
    )


def test_summary_uses_mature_date_window_and_original_sleeve_compounding() -> None:
    from rquant.factor.tracking import summarize_factor_tracking

    days = tuple(_day(i, value=(-0.2 if i < 5 else 0.05)) for i in range(25))
    result = summarize_factor_tracking(days)
    assert result.ic_20.source_day_count == result.ic_20.valid_day_count == 20
    assert result.ic_20.mean == 0.05 and result.ic_20.ir is None
    assert not result.invalidated
    assert result.week_long_short == prod(1 + d.high_return for d in days[-5:]) - prod(
        1 + d.low_return for d in days[-5:]
    )
    assert result.cumulative_long_short == prod(1 + d.high_return for d in days) - prod(
        1 + d.low_return for d in days
    )
    assert result.yesterday_long_short == days[-1].high_return - days[-1].low_return


def test_partial_day_blocks_invalidation_and_cumulative_without_zero_fill() -> None:
    from rquant.factor.tracking import summarize_factor_tracking

    days = tuple(_day(i, status="partial" if i == 19 else "complete") for i in range(20))
    result = summarize_factor_tracking(days)
    assert result.ic_20.valid_day_count == 20 and result.complete_day_count == 19
    assert not result.invalidated
    assert result.week_long_short is None and result.cumulative_long_short is None
    assert result.yesterday_ic == -0.1
    complete = summarize_factor_tracking(tuple(_day(i) for i in range(20)))
    assert complete.invalidated and complete.ic_20.mean == -0.1


def test_unavailable_ic_is_not_replaced_by_an_older_valid_date() -> None:
    from rquant.factor.evaluate import CorrelationResult
    from rquant.factor.tracking import summarize_factor_tracking

    days = tuple(_day(i) for i in range(21))
    missing = days[-1].model_copy(
        update={
            "rank_ic": CorrelationResult(
                status="zero_variance",
                value=None,
                source_sample_count=10,
                effective_sample_count=10,
            )
        }
    )
    result = summarize_factor_tracking((*days[:-1], missing))
    assert result.ic_20.source_day_count == 20 and result.ic_20.valid_day_count == 19
    assert result.yesterday_ic is None and not result.invalidated
    assert summarize_factor_tracking(days[:4]).week_long_short is None


def test_public_request_has_only_intent_and_original_operation_fences(tmp_path: Path) -> None:
    from rquant.factor.tracking import FactorTrackingRequest

    _, _, head = _registry(tmp_path)
    request = _request(head)
    assert request.expected_tracking_generation is None
    for extra in (
        {"actor_id": "alice"},
        {"source_path": "/private/tmp/x"},
        {"selection": "gem"},
        {"tracked": "true"},
    ):
        with pytest.raises(ValidationError):
            FactorTrackingRequest.model_validate({**request.model_dump(), **extra})


def test_original_toggle_replays_after_head_change_and_checks_actor_and_payload(
    tmp_path: Path,
) -> None:
    from rquant.factor.registry import SaveFactorDefinitionRequest
    from rquant.factor.tracking import FactorTrackingConflict, FactorTrackingStore

    registry, identity, head = _registry(tmp_path)
    store = FactorTrackingStore(tmp_path / "tracking.sqlite")
    tracked_identity = store.initialize()
    request = _request(head)
    receipt = store.set_tracked(
        request, actor_id="alice", expected_identity=tracked_identity, registry_identity=identity
    )
    old = registry.get_head(request.factor_id, expected_identity=identity)
    registry.save(
        SaveFactorDefinitionRequest(
            command_id="update",
            definition=old.definition.model_copy(update={"version": 2}),
            expected_head=head,
        ),
        expected_identity=identity,
    )
    assert (
        store.set_tracked(
            request,
            actor_id="alice",
            expected_identity=tracked_identity,
            registry_identity=identity,
        )
        == receipt
    )
    for actor, candidate in (
        ("bob", request),
        ("alice", request.model_copy(update={"tracked": False})),
        ("alice", request.model_copy(update={"serving_generation_id": "b" * 64})),
    ):
        with pytest.raises(FactorTrackingConflict):
            store.set_tracked(
                candidate,
                actor_id=actor,
                expected_identity=tracked_identity,
                registry_identity=identity,
            )
    assert (
        store.get(request.factor_id, expected_identity=tracked_identity).generation
        == receipt.tracking_generation
    )


def test_cancel_readd_changes_segment_and_stale_toggle_cannot_overwrite(tmp_path: Path) -> None:
    from rquant.factor.tracking import FactorTrackingConflict, FactorTrackingStore

    _, identity, head = _registry(tmp_path)
    store = FactorTrackingStore(tmp_path / "tracking.sqlite")
    expected = store.initialize()
    joined = store.set_tracked(
        _request(head), actor_id="alice", expected_identity=expected, registry_identity=identity
    )
    cancel = _request(head, expected_tracking_generation=joined.tracking_generation).model_copy(
        update={"tracked": False}
    )
    removed = store.set_tracked(
        cancel, actor_id="alice", expected_identity=expected, registry_identity=identity
    )
    new = store.set_tracked(
        _request(head, expected_tracking_generation=removed.tracking_generation),
        actor_id="alice",
        expected_identity=expected,
        registry_identity=identity,
    )
    assert (
        new.segment_id != joined.segment_id
        and new.tracking_generation != joined.tracking_generation
    )
    with pytest.raises(FactorTrackingConflict):
        store.set_tracked(
            cancel.model_copy(update={"command_id": str(uuid4())}),
            actor_id="alice",
            expected_identity=expected,
            registry_identity=identity,
        )
    assert store.lookup(cancel, actor_id="alice", expected_identity=expected) == removed


def test_fixed_file_identity_and_row_integrity_are_not_repaired(tmp_path: Path) -> None:
    import sqlite3

    from rquant.factor.tracking import FactorTrackingStore

    _, identity, head = _registry(tmp_path)
    store = FactorTrackingStore(tmp_path / "tracking.sqlite")
    expected = store.initialize()
    store.set_tracked(
        _request(head), actor_id="alice", expected_identity=expected, registry_identity=identity
    )
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE tracking_states SET payload = '{}' ")
    with pytest.raises(RuntimeError):
        store.list_states(expected_identity=expected)
    store.path.rename(tmp_path / "old.sqlite")
    store.path.touch()
    with pytest.raises(RuntimeError):
        store.get("tracked_test", expected_identity=expected)
