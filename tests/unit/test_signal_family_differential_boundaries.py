"""R07 exact fixture, call-shape, sentinel, and B01..B19 matrix contracts."""

from __future__ import annotations

import json

import pytest

from rquant.signal_family_differential_gate import (
    BOUNDARY_PROBES,
    BoundaryProbeV1,
    BoundaryReachedSentinelV1,
    CallShapeV1,
    CurrentFixtureV1,
    FixtureValueV1,
    ProbeSetupV1,
    strict_fixture_value,
)


def test_complete_inventory_has_immutable_b01_to_b19_order() -> None:
    assert tuple(probe.inventory_id for probe in BOUNDARY_PROBES) == tuple(
        f"R07-B{index:02d}" for index in range(1, 20)
    )
    assert BOUNDARY_PROBES[-2].variant == "static_only"
    assert BOUNDARY_PROBES[-1].variant == "static_only"


def test_fixture_value_requires_exact_kind_and_canonical_digest() -> None:
    fixture = FixtureValueV1(
        fixture_id="scalar.one",
        kind="scalar",
        value="current",
        sha256="".join(["0"] * 64),
    )
    with pytest.raises(ValueError, match="sha256"):
        strict_fixture_value(fixture)


def test_fixture_value_composite_rejects_forward_reference_and_cycles() -> None:
    with pytest.raises(ValueError, match="prior"):
        FixtureValueV1(
            fixture_id="tuple.forward",
            kind="tuple",
            value=["scalar.later"],
            sha256="0" * 64,
        ).validate_references(seen_ids={"scalar.first"})
    with pytest.raises(ValueError, match="cycle"):
        FixtureValueV1(
            fixture_id="tuple.cycle",
            kind="tuple",
            value=["tuple.cycle"],
            sha256="0" * 64,
        ).validate_references(seen_ids={"tuple.cycle"})


def test_call_shape_has_sorted_keywords_and_one_result_action() -> None:
    shape = CallShapeV1(
        receiver_fixture_id=None,
        positional_fixture_ids=(),
        keyword_fixture_ids={"b": "scalar.b", "a": "scalar.a"},
        call_result_action="none",
    )
    assert tuple(shape.keyword_fixture_ids) == ("a", "b")
    with pytest.raises(ValueError, match="consume_tuple"):
        shape.model_copy(update={"call_result_action": "consume_twice"}).validate_contract()


def test_sentinel_requires_exactly_one_boundary_reach_and_zero_mutations() -> None:
    sentinel = BoundaryReachedSentinelV1(
        sentinel_id="sentinel.r07-b06",
        inventory_id="R07-B06",
        source_span="signal_bus.py:594",
        ast_digest="a" * 64,
        reached_count=1,
        mutation_reached_count=0,
    )
    assert sentinel.passed
    assert not sentinel.model_copy(update={"reached_count": 2}).passed


def test_boundary_probe_requires_exact_exception_phase_and_snapshot_contract() -> None:
    probe = next(item for item in BOUNDARY_PROBES if item.inventory_id == "R07-B03")
    assert isinstance(probe, BoundaryProbeV1)
    assert probe.call_shape.call_result_action == "consume_tuple"
    assert probe.expected_exception_phase == "consumption"
    assert probe.expected_yielded_count == 0
    assert isinstance(probe.current_fixture, CurrentFixtureV1)
    assert isinstance(probe.setup, ProbeSetupV1)


def test_policy_probe_json_has_no_duplicate_keys() -> None:
    assert json.loads(json.dumps(BOUNDARY_PROBES[0].model_dump(mode="json")))
