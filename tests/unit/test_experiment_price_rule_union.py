from __future__ import annotations

import json

import pytest

from rquant.experiment_platform_projection import PRIVATE_TABLES
from rquant.price_alert_runtime_projection import unavailable_price_runtime_projections
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingReadModelInput,
    _projection_json_bytes,
    build_serving_read_models,
)
from tests.unit.test_price_alert_event_contracts import AT
from tests.unit.test_price_alert_runtime_capacity import exact_projection_inputs
from tests.unit.test_serving_read_models import _projection

MIB = 1024 * 1024


def private_inputs(size: int | None = None) -> tuple[ServingProjectionInput, ...]:
    # These rows exercise outer collection capacity, not private research facts.
    attempts = (
        [
            {
                "owner": "alice",
                "experiment_id": f"{index:064x}",
                "family_id": "experiment-search:fixture",
                "registered_at": AT.isoformat(),
                "payload_json": json.dumps({"padding": "x" * 60000}),
            }
            for index in range(100)
        ]
        if size is not None
        else []
    )
    families = (
        [
            {
                "owner": "alice",
                "family_id": f"fixture-{index}",
                "payload_json": json.dumps({"padding": "x" * 60000}),
            }
            for index in range(50)
        ]
        if size is not None
        else []
    )

    def build() -> tuple[ServingProjectionInput, ...]:
        return tuple(
            _projection(name, tuple(rows), owner_dataset_id="promotions", available_at=AT)
            for name, rows in zip(PRIVATE_TABLES, (attempts, families, []), strict=True)
        )

    result = build()
    if size is not None:
        excess = sum(_projection_json_bytes(value) for value in result) - size
        assert excess >= 0
        for row in (*attempts, *families):
            removed = min(60000, excess)
            row["payload_json"] = json.dumps({"padding": "x" * (60000 - removed)})
            excess -= removed
            if not excess:
                break
        assert excess == 0
        result = build()
        assert sum(_projection_json_bytes(value) for value in result) == size
    return result


def prices() -> tuple[ServingProjectionInput, ...]:
    return tuple(
        ServingProjectionInput.bind(value, owner_dataset_id="signals", owner_generation_id="6" * 64)
        for value in unavailable_price_runtime_projections(observed_at=AT, shadow=False)
    )


def test_joint_complete_groups_and_default_absence() -> None:
    values = (*private_inputs(), *prices())
    snapshot = ServingReadModelInput(observed_at=AT, projections=values)
    assert snapshot.projections == values
    tables = build_serving_read_models(snapshot)
    assert all(name in tables for name in (*PRIVATE_TABLES, "price_alert_runtime_state"))
    assert ServingReadModelInput(observed_at=AT).projections == ()


@pytest.mark.parametrize("fault", ("private_missing", "price_missing", "price_digest"))
def test_joint_groups_preserve_each_complete_receipt(fault: str) -> None:
    private, price = private_inputs(), prices()
    if fault == "private_missing":
        private = private[:-1]
        reason = "private experiment projection is partial"
    elif fault == "price_missing":
        price = price[:-1]
        reason = "price runtime domain is incomplete"
    else:
        state = price[0]
        row = dict(state.rows[0])
        body = json.loads(row["body_json"])
        body["event_rows_sha256"] = "f" * 64
        row["body_json"] = json.dumps(body)
        price = (
            ServingProjectionInput.model_validate(
                state.model_dump(mode="python") | {"rows": (row,)}
            ),
            *price[1:],
        )
        reason = "price runtime rows differ from the exact complete receipt"
    with pytest.raises(ValueError, match=reason):
        ServingReadModelInput(observed_at=AT, projections=(*private, *price))


def test_joint_price_domain_actual_two_mib_and_one_over() -> None:
    private = private_inputs()
    bounded = exact_projection_inputs(2 * MIB)
    assert (
        len(ServingReadModelInput(observed_at=AT, projections=(*private, *bounded)).projections)
        == 7
    )
    with pytest.raises(ValueError, match="price runtime projections exceed their 2 MiB domain"):
        ServingReadModelInput(
            observed_at=AT, projections=(*private, *exact_projection_inputs(2 * MIB + 1))
        )


def test_joint_private_actual_eight_mib_is_separate_from_shared_seven_mib() -> None:
    price = prices()
    assert (
        len(
            ServingReadModelInput(
                observed_at=AT, projections=(*private_inputs(8 * MIB), *price)
            ).projections
        )
        == 7
    )
    with pytest.raises(
        ValueError, match="private experiment projections exceed their authority byte budget"
    ):
        ServingReadModelInput(observed_at=AT, projections=(*private_inputs(8 * MIB + 1), *price))


def test_joint_shared_owner_cap_after_only_private_bytes_subtracted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant import serving_read_models as models

    assert models._MAX_OWNER_PROJECTION_BYTES == 7 * MIB
    private = private_inputs(8 * MIB)
    shared = _projection("experiment_attempt", (), owner_dataset_id="promotions", available_at=AT)
    values = (*private, shared, *prices())
    original_size = models._projection_json_bytes
    size = 7 * MIB

    def measured(value: object) -> int:
        # Current shared table caps total below 7 MiB. Inject only the already
        # validated shared wrapper's measured size to exercise this outer guard.
        if isinstance(value, ServingProjectionInput) and value.table_name == shared.table_name:
            return size
        return original_size(value)

    monkeypatch.setattr(models, "_projection_json_bytes", measured)
    assert len(ServingReadModelInput(observed_at=AT, projections=values).projections) == 8
    size += 1
    with pytest.raises(ValueError, match="authority byte budget: promotions"):
        ServingReadModelInput(observed_at=AT, projections=values)
