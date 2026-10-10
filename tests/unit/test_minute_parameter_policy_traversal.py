"""Live policy traversal keeps ordered reads, exact types and schema aliases."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any

import pytest
from pydantic import BaseModel, Field

from rquant import minute_backtest_parameter_contracts as contracts


def _factory() -> int:
    return 1


class _TraversalModel(BaseModel):
    value: int = Field(default_factory=_factory)


def _schema_probe(
    monkeypatch: pytest.MonkeyPatch, value: dict[str, Any],
) -> contracts._ParameterPolicyValue:
    monkeypatch.setitem(_TraversalModel.__pydantic_core_schema__, "traversal_probe", value)
    rows, _, supported = contracts._parameter_policy_structure((_TraversalModel,), ())
    assert supported
    return rows[0][3]


def _matches(probe: contracts._ParameterPolicyValue) -> bool:
    return probe.matches(_TraversalModel.__pydantic_core_schema__, {})


def test_equivalent_replaced_mutable_structure_is_accepted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    shared = {"values": [1, 2], "nested": [{"enabled": True}]}
    value = {"first": shared, "second": shared, "label": "same"}
    probe = _schema_probe(monkeypatch, value)
    assert _matches(probe)
    replacement = deepcopy(value)
    monkeypatch.setitem(_TraversalModel.__pydantic_core_schema__, "traversal_probe", replacement)
    assert replacement["first"] is replacement["second"]
    assert _matches(probe)


@pytest.mark.parametrize("change", (
    "nested", "scalar_type", "sequence_length", "sequence_type", "mapping_type",
    "key_order", "alias_split", "alias_join",
))
def test_actual_structural_mutation_is_rejected(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    shared = {"values": [1, 2], "nested": [{"enabled": True}]}
    value = {"first": shared, "second": shared, "separate": deepcopy(shared), "label": "same"}
    probe = _schema_probe(monkeypatch, value)
    assert _matches(probe)
    if change == "nested":
        shared["nested"][0]["enabled"] = False
    elif change == "scalar_type":
        shared["values"][0] = True
    elif change == "sequence_length":
        shared["values"].append(3)
    elif change == "sequence_type":
        shared["values"] = tuple(shared["values"])
    elif change == "mapping_type":
        class ChangedMapping(dict):
            pass

        value["first"] = ChangedMapping(shared)
    elif change == "key_order":
        value["first"] = value.pop("first")
    elif change == "alias_split":
        value["second"] = deepcopy(shared)
    else:
        value["separate"] = shared
    assert not _matches(probe)


def _live_factory() -> tuple[Callable[..., int], dict[str, list[int]]]:
    captured = {"nested": [1]}

    def factory(default: dict[str, list[int]] = {"nested": [1]}, *,
                keyword: dict[str, list[int]] = {"nested": [1]}) -> int:
        return captured["nested"][0] + default["nested"][0] + keyword["nested"][0]

    return factory, captured


@pytest.mark.parametrize("change", ("default", "keyword_default", "closure", "factory", "field"))
def test_actual_field_and_factory_mutable_state_is_rejected(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    factory, captured = _live_factory()
    field = _TraversalModel.model_fields["value"]
    monkeypatch.setattr(field, "default_factory", factory)
    monkeypatch.setattr(field, "json_schema_extra", {"nested": [{"value": 1}]})
    rows, _, supported = contracts._parameter_policy_structure((_TraversalModel,), ())
    assert supported
    probe = rows[0][4]
    assert probe.matches(_TraversalModel.model_fields, {})
    if change == "default":
        factory.__defaults__[0]["nested"][0] = 2
    elif change == "keyword_default":
        factory.__kwdefaults__["keyword"]["nested"][0] = 2
    elif change == "closure":
        captured["nested"][0] = 2
    elif change == "factory":
        field.default_factory = lambda: 1
    else:
        field.json_schema_extra["nested"][0]["value"] = 2
    assert not probe.matches(_TraversalModel.model_fields, {})


@pytest.mark.parametrize("mismatch", (False, True))
def test_later_child_is_read_only_after_prior_child_result(
    monkeypatch: pytest.MonkeyPatch, mismatch: bool,
) -> None:
    visits: list[str] = []
    actions: dict[str, Callable[[], None]] = {}

    class Observed:
        def __init__(self, name: str) -> None:
            self.name = name
            self.changed = False

        def __repr__(self) -> str:
            visits.append(self.name)
            action = actions.get(self.name)
            if action is not None:
                action()
            return self.name + ("-changed" if self.changed else "")

    first, later = Observed("first"), Observed("later")
    value = {"first": first, "later": later}
    probe = _schema_probe(monkeypatch, value)
    visits.clear()
    actions["first"] = lambda: value.pop("later")
    first.changed = mismatch
    if mismatch:
        assert not _matches(probe)
    else:
        with pytest.raises(KeyError, match="later"):
            _matches(probe)
    assert visits == ["first"]
