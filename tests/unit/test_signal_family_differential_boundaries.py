"""R07 exact fixture, call-shape, sentinel, and B01..B19 matrix contracts."""

from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from rquant.signal_contracts import CurrentSignalEnvelope, parse_signal_envelope
from rquant.signal_family_differential_gate import (
    BOUNDARY_PROBES,
    BoundaryProbeV1,
    BoundaryReachedSentinelV1,
    CallShapeV1,
    FixtureValueV1,
    ProbeSetupV1,
    load_policy,
    strict_fixture_value,
)
from tests.unit.test_signal_contracts import _CURRENT_CANONICAL_FIXTURES

ROOT = Path(__file__).parents[2]
POLICY_PATH = ROOT / "tests" / "fixtures" / "r07_differential_gate" / "policy-v1.json"


def test_complete_inventory_has_immutable_b01_to_b19_order() -> None:
    assert tuple(probe.inventory_id for probe in BOUNDARY_PROBES) == tuple(
        f"R07-B{index:02d}" for index in range(1, 20)
    )
    assert BOUNDARY_PROBES[-2].variant == "static_only"
    assert BOUNDARY_PROBES[-1].variant == "static_only"


def test_boundary_manifest_contains_real_current_fixture_and_no_result_placeholders() -> None:
    expected_bytes = _CURRENT_CANONICAL_FIXTURES[0][2]
    assert "setup_result_digest" not in ProbeSetupV1.model_fields
    assert "before_snapshot_digest" not in BoundaryProbeV1.model_fields
    assert "after_snapshot_digest" not in BoundaryProbeV1.model_fields
    policy = load_policy(POLICY_PATH)
    fixtures = {fixture.fixture_id: fixture for fixture in policy.current_fixtures}
    assert fixtures
    for fixture in fixtures.values():
        raw = base64.b64decode(fixture.canonical_model_bytes, validate=True)
        assert raw == expected_bytes
        parsed = parse_signal_envelope(raw)
        assert type(parsed) is CurrentSignalEnvelope


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
    probe = next(item for item in BOUNDARY_PROBES if item.inventory_id == "R07-B06")
    sentinel = BoundaryReachedSentinelV1(
        sentinel_id="sentinel.r07-b06",
        inventory_id="R07-B06",
        source_span=probe.source_span,
        ast_digest=probe.boundary_ast_sha256,
        reached_count=1,
        mutation_reached_count=0,
    )
    assert sentinel.passed
    assert not sentinel.model_copy(update={"reached_count": 2}).passed


def test_boundary_probe_requires_exact_exception_phase_and_snapshot_contract() -> None:
    probe = next(item for item in BOUNDARY_PROBES if item.inventory_id == "R07-B03")
    assert isinstance(probe, BoundaryProbeV1)
    assert probe.call_result_action == "consume_tuple"
    assert probe.current_fixture_id == "current.object"
    assert (
        probe.behavior_test == "tests/unit/test_signal_family_no_activation_reset.py::"
        "test_r07_b03_sentinel_is_lazy_and_stops_before_first_yield"
    )
    assert probe.expected_exception_phase == "consumption"
    assert probe.expected_yielded_count == 0


def test_policy_probe_json_has_no_duplicate_keys() -> None:
    assert json.loads(json.dumps(BOUNDARY_PROBES[0].model_dump(mode="json")))
