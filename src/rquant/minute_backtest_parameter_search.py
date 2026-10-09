"""Finite complete recipes; source authority and execution remain with the original preparer."""

from __future__ import annotations

import copy
import random
from collections.abc import Mapping, Sequence
from types import UnionType
from typing import Literal, Self, Union, cast, get_args, get_origin

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StrictFloat,
    StrictInt,
    TypeAdapter,
    model_validator,
)

from rquant.minute_backtest_contracts import MAX_INPUT_BYTES, MAX_WORK_UNITS
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.runtime_contracts import RuntimeContractModel, canonical_sha256

# The original Lab control frame is 1 MiB; a recipe count is not an execution-work proof.
_MAX_CONTROL_BYTES = 1_048_576
_JSON = TypeAdapter(object)
AxisValue = StrictBool | StrictInt | StrictFloat | tuple[StrictInt, ...] | None


class _SearchModel(RuntimeContractModel):
    model_config = ConfigDict(allow_inf_nan=False, str_strip_whitespace=False)


class MinuteParameterSearchAxis(_SearchModel):
    path: str = Field(
        strict=True, min_length=1, max_length=128, pattern=r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)?$"
    )
    values: tuple[AxisValue, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)


class MinuteParameterSearchRequest(_SearchModel):
    base: MinuteParameterSet
    axes: tuple[MinuteParameterSearchAxis, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    mode: Literal["grid", "random"]
    seed: int = Field(strict=True, ge=0, lt=2**63)
    requested_trials: int | None = Field(default=None, strict=True, ge=1, le=MAX_WORK_UNITS)

    @model_validator(mode="after")
    def finite_request(self) -> Self:
        space_size = _space_size(self.axes)
        if len({axis.path for axis in self.axes}) != len(self.axes):
            raise ValueError("duplicate parameter axis")
        if self.mode == "random" and self.requested_trials is None:
            raise ValueError("random search requires an explicit trial count")
        if self.mode == "grid" and self.requested_trials not in (None, space_size):
            raise ValueError("grid trial count must equal the complete space")
        if self.requested_trials is not None and self.requested_trials > space_size:
            raise ValueError("requested trials exceed the finite space")
        _check_bytes(len(_JSON.dump_json(self.model_dump(mode="json"))))
        _normalized_axes(self)
        return self


class MinuteParameterSearchPlan(_SearchModel):
    kind: Literal["minute-parameter-search-plan"] = "minute-parameter-search-plan"
    schema_version: Literal[1] = 1
    request: MinuteParameterSearchRequest
    trials: tuple[MinuteParameterSet, ...] = Field(min_length=1, max_length=MAX_WORK_UNITS)
    space_size: int = Field(strict=True, ge=1, le=MAX_WORK_UNITS)
    trial_count: int = Field(strict=True, ge=1, le=MAX_WORK_UNITS)
    mode: Literal["grid", "random"]
    seed: int = Field(strict=True, ge=0, lt=2**63)
    plan_hash: str = Field(strict=True, pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="before")
    @classmethod
    def complete_plan(cls, value: object) -> object:
        if not isinstance(value, Mapping) or "request" not in value:
            return value
        request = MinuteParameterSearchRequest.model_validate(value["request"])
        trials = _complete_trials(request)
        expected = _plan_body(request, trials)
        expected["plan_hash"] = canonical_sha256(expected)
        for name in (
            "kind",
            "schema_version",
            "space_size",
            "trial_count",
            "mode",
            "seed",
            "plan_hash",
        ):
            if name in value and canonical_sha256(value[name]) != canonical_sha256(expected[name]):
                raise ValueError(f"search plan {name} differs from its complete request")
        if "trials" in value:
            supplied = value["trials"]
            if (
                not isinstance(supplied, Sequence)
                or isinstance(supplied, (str, bytes))
                or len(supplied) != len(trials)
            ):
                raise ValueError("search plan trial count differs from its complete request")
            for actual, original in zip(supplied, trials, strict=True):
                if MinuteParameterSet.model_validate(actual) != original:
                    raise ValueError("search plan trial differs from its original complete recipe")
        return {**value, **expected, "request": request, "trials": trials}


def build_minute_parameter_search_plan(
    request: MinuteParameterSearchRequest,
) -> MinuteParameterSearchPlan:
    return MinuteParameterSearchPlan.model_validate({"request": request})


def _space_size(axes: tuple[MinuteParameterSearchAxis, ...]) -> int:
    result = 1
    for axis in axes:
        result *= len(axis.values)
        if result > MAX_WORK_UNITS:
            raise ValueError("search space exceeds the original 20,000 finite budget")
    return result


def _adjustable(annotation: object) -> bool:
    if annotation in (bool, int, float):
        return True
    origin, arguments = get_origin(annotation), get_args(annotation)
    if origin in (Union, UnionType):
        return all(item is type(None) or _adjustable(item) for item in arguments)
    return origin is tuple and arguments == (int, Ellipsis)


def _axis_adapter(base: MinuteParameterSet, path: str) -> TypeAdapter[object]:
    owner: BaseModel = base.parameters
    parts = path.split(".")
    if len(parts) == 2:
        if parts[0] not in type(owner).model_fields:
            raise ValueError("unknown parameter axis")
        nested = getattr(owner, parts[0])
        if not isinstance(nested, BaseModel):
            raise ValueError("parameter axis must select an original numeric or boolean field")
        owner = nested
    field = type(owner).model_fields.get(parts[-1])
    if field is None or not _adjustable(field.annotation):
        raise ValueError(
            "parameter axis is unknown or changes identity, dates, frequency "
            "or another source boundary"
        )
    return TypeAdapter(field.rebuild_annotation())


def _normalized_axes(request: MinuteParameterSearchRequest) -> tuple[tuple[AxisValue, ...], ...]:
    result: list[tuple[AxisValue, ...]] = []
    for axis in request.axes:
        adapter = _axis_adapter(request.base, axis.path)
        values = tuple(
            cast(AxisValue, adapter.validate_python(value, strict=True)) for value in axis.values
        )
        identities = set(values)
        if len(identities) != len(values):
            raise ValueError("duplicate equivalent parameter values")
        result.append(values)
    return tuple(result)


def _recipe_data(
    request: MinuteParameterSearchRequest, values: tuple[tuple[AxisValue, ...], ...], index: int
) -> dict[str, object]:
    recipe = copy.deepcopy(request.base.model_dump(mode="json"))
    parameters = recipe["parameters"]
    for axis, choices in reversed(tuple(zip(request.axes, values, strict=True))):
        index, position = divmod(index, len(choices))
        parts = axis.path.split(".")
        owner = parameters if len(parts) == 1 else parameters[parts[0]]
        owner[parts[-1]] = choices[position]
    return recipe


def _check_bytes(size: int) -> None:
    if size > MAX_INPUT_BYTES or size > _MAX_CONTROL_BYTES:
        raise ValueError("search plan exceeds the original input/control byte budget")


def _plan_body(
    request: MinuteParameterSearchRequest, trials: tuple[MinuteParameterSet, ...]
) -> dict[str, object]:
    return {
        "kind": "minute-parameter-search-plan",
        "schema_version": 1,
        "request": request.model_dump(mode="json"),
        "trials": [trial.model_dump(mode="json") for trial in trials],
        "space_size": _space_size(request.axes),
        "trial_count": len(trials),
        "mode": request.mode,
        "seed": request.seed,
    }


def _complete_trials(request: MinuteParameterSearchRequest) -> tuple[MinuteParameterSet, ...]:
    values, space_size = _normalized_axes(request), _space_size(request.axes)
    indices = list(range(space_size))
    if request.mode == "random":
        random.Random(request.seed).shuffle(indices)
        indices = indices[: request.requested_trials]
    empty = _plan_body(request, ())
    empty["trial_count"], empty["plan_hash"] = len(indices), "0" * 64
    byte_size = len(_JSON.dump_json(empty)) + max(0, len(indices) - 1)
    for index in indices:
        byte_size += len(_JSON.dump_json(_recipe_data(request, values, index)))
        _check_bytes(byte_size)
    chosen: dict[int, MinuteParameterSet] = {}
    seen: set[str] = set()
    selected = set(indices)
    # Validate the entire declared space, including unselected random combinations.
    for index in range(space_size):
        recipe = MinuteParameterSet.model_validate(_recipe_data(request, values, index))
        if recipe.fingerprint in seen:
            raise ValueError("duplicate complete recipe after original owner validation")
        seen.add(recipe.fingerprint)
        if index in selected:
            chosen[index] = recipe
    trials = tuple(chosen[index] for index in indices)
    body = _plan_body(request, trials)
    body["plan_hash"] = canonical_sha256(body)
    _check_bytes(len(_JSON.dump_json(body)))
    return trials
