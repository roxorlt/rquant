"""Project verified training trials; the caller must verify their sealed authority."""

from __future__ import annotations

import math
from collections.abc import Sequence
from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import Field, StringConstraints, model_validator

from rquant.minute_backtest_contracts import (
    MAX_INPUT_BYTES,
    MAX_RESULT_WIRE_BYTES,
    MAX_WORK_UNITS,
)
from rquant.minute_backtest_parameter_optimizer import (
    MinuteStudyTrainingObservation,
    MinuteStudyTrainingRank,
    rank_minute_study_training,
)
from rquant.minute_backtest_parameters import MinuteParameterSet
from rquant.minute_backtest_study_protocols import MinuteStudyProtocol, StudyHash
from rquant.runtime_contracts import (
    AwareUtcDatetime,
    RuntimeContractModel,
    canonical_sha256,
    normalize_aware_utc,
)

AxisValue = (
    Annotated[bool, Field(strict=True)]
    | Annotated[int, Field(strict=True)]
    | Annotated[float, Field(strict=True, allow_inf_nan=False)]
)
ParameterPath = Annotated[
    str,
    StringConstraints(max_length=128, pattern=r"^[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)?$"),
]
GridCoordinate = tuple[
    Annotated[int, Field(strict=True, ge=0)], Annotated[int, Field(strict=True, ge=0)]
]


class MinuteStudyHeatmapAxis(RuntimeContractModel):
    parameter_name: ParameterPath
    values: tuple[AxisValue, ...] = Field(min_length=2, max_length=MAX_WORK_UNITS)

    @model_validator(mode="after")
    def natural_order(self) -> Self:
        if len({type(value) for value in self.values}) != 1:
            raise ValueError("an axis must use one original numeric or boolean type")
        if tuple(sorted(set(self.values))) != self.values:
            raise ValueError("axis values must be distinct and naturally ordered")
        return self


class MinuteStudyHeatmapNeighborhood(RuntimeContractModel):
    status: Literal["available", "unavailable"]
    reason: Literal["complete", "center_unavailable", "missing_neighbors", "no_neighbors"]
    coordinates: tuple[GridCoordinate, ...] = Field(max_length=8)
    unavailable_coordinates: tuple[GridCoordinate, ...] = Field(max_length=8)
    minimum_score: Annotated[float, Field(allow_inf_nan=False)] | None

    @model_validator(mode="after")
    def complete_minimum(self) -> Self:
        if self.status == "available":
            if (
                self.minimum_score is None
                or not self.coordinates
                or self.unavailable_coordinates
                or self.reason != "complete"
            ):
                raise ValueError("an available neighborhood needs every legal adjacent score")
        elif self.minimum_score is not None or self.reason == "complete":
            raise ValueError("an unavailable neighborhood cannot report a numeric minimum")
        if not set(self.unavailable_coordinates).issubset(self.coordinates):
            raise ValueError("unavailable neighbors must belong to the original one-cell ring")
        return self


class MinuteStudyHeatmapCell(RuntimeContractModel):
    x_index: Annotated[int, Field(strict=True, ge=0)]
    y_index: Annotated[int, Field(strict=True, ge=0)]
    x_value: AxisValue
    y_value: AxisValue
    status: Literal["available", "insufficient_trades", "missing_trial", "invalid_parameters"]
    is_current: bool
    study_id: StudyHash | None
    protocol: MinuteStudyProtocol | None
    observation: MinuteStudyTrainingObservation | None
    training_rank: MinuteStudyTrainingRank | None
    training_score: Annotated[float, Field(allow_inf_nan=False)] | None
    neighborhood: MinuteStudyHeatmapNeighborhood

    @model_validator(mode="after")
    def retain_original_trial(self) -> Self:
        if self.status in {"missing_trial", "invalid_parameters"}:
            if any(
                value is not None
                for value in (
                    self.study_id, self.protocol, self.observation, self.training_rank,
                    self.training_score,
                )
            ) or self.is_current:
                raise ValueError("an absent trial cannot claim an original identity or score")
            return self
        if self.protocol is None or self.observation is None:
            raise ValueError("an actual trial needs its complete protocol and observation")
        if self.study_id != self.protocol.study_id or self.study_id != self.observation.study_id:
            raise ValueError("cell identity differs from the original trial")
        if self.status == "available":
            if self.training_rank is None or self.training_rank.study_id != self.study_id:
                raise ValueError("an available score needs the original training rank")
            if self.training_score != self.training_rank.training_score:
                raise ValueError("cell score differs from the original training rank")
        elif self.training_rank is not None or self.training_score is not None:
            raise ValueError("insufficient trades cannot receive a score")
        return self


class MinuteStudyHeatmap(RuntimeContractModel):
    x_axis: MinuteStudyHeatmapAxis
    y_axis: MinuteStudyHeatmapAxis
    current_study_id: StudyHash
    selection_cutoff: AwareUtcDatetime
    trial_set_hash: StudyHash
    cells: tuple[MinuteStudyHeatmapCell, ...] = Field(max_length=MAX_WORK_UNITS)

    @model_validator(mode="after")
    def bounded_grid(self) -> Self:
        if self.x_axis.parameter_name == self.y_axis.parameter_name:
            raise ValueError("heatmap axes must differ")
        expected = len(self.x_axis.values) * len(self.y_axis.values)
        if expected > MAX_WORK_UNITS or len(self.cells) != expected:
            raise ValueError("heatmap grid exceeds or differs from the original work budget")
        current = [cell for cell in self.cells if cell.is_current]
        if len(current) != 1 or current[0].study_id != self.current_study_id:
            raise ValueError("the current point must identify exactly one original trial")
        for index, cell in enumerate(self.cells):
            y_index, x_index = divmod(index, len(self.x_axis.values))
            if (cell.x_index, cell.y_index) != (x_index, y_index):
                raise ValueError("heatmap cells must cover the ordered grid exactly once")
            if (
                cell.x_value != self.x_axis.values[x_index]
                or cell.y_value != self.y_axis.values[y_index]
            ):
                raise ValueError("cell values differ from their original axes")
        return self


def _parameter_value(protocol: MinuteStudyProtocol, path: str) -> bool | int | float:
    value: object = protocol.parameters.parameters.model_dump(mode="python")
    for part in path.split("."):
        if not isinstance(value, dict) or part not in value:
            raise ValueError("unknown parameter axis")
        value = value[part]
    if type(value) not in (bool, int, float) or (type(value) is float and not math.isfinite(value)):
        raise ValueError("parameter axes must be finite numeric or boolean leaves")
    return value


def _replace_axis(body: dict[str, object], path: str, value: object) -> None:
    branch = body
    parts = path.split(".")
    for part in parts[:-1]:
        branch = branch[part]
    branch[parts[-1]] = value


def _fixed_dimensions(protocol: MinuteStudyProtocol, x: str, y: str) -> str:
    body = protocol.model_dump(mode="json")
    # Heads bind each physical recipe independently through the original rank owner.
    del body["head"]
    del body["requested_at"]
    for path in (x, y):
        _replace_axis(body["parameters"]["parameters"], path, None)
    return canonical_sha256(body)


def build_minute_study_heatmap(
    protocols: Sequence[MinuteStudyProtocol],
    observations: Sequence[MinuteStudyTrainingObservation],
    *,
    selection_cutoff: datetime,
    x_parameter: str,
    y_parameter: str,
    current_study_id: str,
) -> MinuteStudyHeatmap:
    """Use training-only scores; typed inputs alone do not prove a sealed authority."""
    if not protocols:
        raise ValueError("heatmap needs actual trials")
    if len(protocols) > MAX_WORK_UNITS or len(observations) > MAX_WORK_UNITS:
        raise ValueError("heatmap trials exceed the original work budget")
    if x_parameter == y_parameter:
        raise ValueError("heatmap axes must differ")
    cutoff = normalize_aware_utc(selection_cutoff)
    verified = tuple(MinuteStudyProtocol.model_validate(p) for p in protocols)
    facts = tuple(MinuteStudyTrainingObservation.model_validate(o) for o in observations)
    input_bytes = sum(len(row.model_dump_json().encode("utf-8")) for row in (*verified, *facts))
    if input_bytes > MAX_INPUT_BYTES:
        raise ValueError("heatmap material exceeds the original input byte budget")
    _parameter_value(verified[0], x_parameter)
    _parameter_value(verified[0], y_parameter)
    dimensions = _fixed_dimensions(verified[0], x_parameter, y_parameter)
    by_values: dict[tuple[bool | int | float, bool | int | float], MinuteStudyProtocol] = {}
    for protocol in verified:
        x = _parameter_value(protocol, x_parameter)
        y = _parameter_value(protocol, y_parameter)
        if _fixed_dimensions(protocol, x_parameter, y_parameter) != dimensions:
            raise ValueError("heatmap non-axis dimensions must remain identical")
        if protocol.requested_at > cutoff or protocol.source.published_at > cutoff:
            raise ValueError("heatmap protocol is not available at the selection cutoff")
        if (x, y) in by_values:
            raise ValueError("duplicate heatmap cell")
        by_values[(x, y)] = protocol
    if current_study_id not in {p.study_id for p in verified}:
        raise ValueError("current heatmap point is outside the exact trial collection")
    x_values = tuple(sorted({point[0] for point in by_values}))
    y_values = tuple(sorted({point[1] for point in by_values}))
    if len(x_values) < 2 or len(y_values) < 2:
        raise ValueError("each heatmap axis needs at least two distinct actual values")
    if len(x_values) * len(y_values) > MAX_WORK_UNITS:
        raise ValueError("heatmap grid exceeds the original work budget")
    x_axis = MinuteStudyHeatmapAxis(parameter_name=x_parameter, values=x_values)
    y_axis = MinuteStudyHeatmapAxis(parameter_name=y_parameter, values=y_values)
    ranks = {
        rank.study_id: rank
        for rank in rank_minute_study_training(verified, facts, selection_cutoff=cutoff)
    }
    fact_by_study = {fact.study_id: fact for fact in facts}
    legal: set[GridCoordinate] = set()
    scores: dict[GridCoordinate, float] = {}
    for y_index, y in enumerate(y_values):
        for x_index, x in enumerate(x_values):
            coordinate = (x_index, y_index)
            body = verified[0].parameters.model_dump(mode="python")
            _replace_axis(body["parameters"], x_parameter, x)
            _replace_axis(body["parameters"], y_parameter, y)
            try:
                MinuteParameterSet.model_validate(body)
            except ValueError:
                continue
            legal.add(coordinate)
            protocol = by_values.get((x, y))
            if protocol is not None and protocol.study_id in ranks:
                scores[coordinate] = ranks[protocol.study_id].training_score
    cells = []
    for y_index, y in enumerate(y_values):
        for x_index, x in enumerate(x_values):
            coordinate = (x_index, y_index)
            protocol = by_values.get((x, y))
            rank = ranks.get(protocol.study_id) if protocol is not None else None
            neighbors = tuple(
                (nx, ny)
                for ny in range(max(0, y_index - 1), min(len(y_values), y_index + 2))
                for nx in range(max(0, x_index - 1), min(len(x_values), x_index + 2))
                if (nx, ny) != coordinate and (nx, ny) in legal
            )
            unavailable = tuple(point for point in neighbors if point not in scores)
            if coordinate not in scores:
                reason = "center_unavailable"
            elif not neighbors:
                reason = "no_neighbors"
            elif unavailable:
                reason = "missing_neighbors"
            else:
                reason = "complete"
            neighborhood = MinuteStudyHeatmapNeighborhood(
                status="available" if reason == "complete" else "unavailable",
                reason=reason,
                coordinates=neighbors,
                unavailable_coordinates=unavailable,
                minimum_score=min(scores[p] for p in neighbors) if reason == "complete" else None,
            )
            if coordinate not in legal:
                status = "invalid_parameters"
            elif protocol is None:
                status = "missing_trial"
            else:
                status = "available" if rank is not None else "insufficient_trades"
            cells.append(
                MinuteStudyHeatmapCell(
                    x_index=x_index, y_index=y_index, x_value=x, y_value=y,
                    status=status,
                    is_current=protocol is not None and protocol.study_id == current_study_id,
                    study_id=protocol.study_id if protocol is not None else None,
                    protocol=protocol,
                    observation=fact_by_study[protocol.study_id] if protocol is not None else None,
                    training_rank=rank,
                    training_score=rank.training_score if rank is not None else None,
                    neighborhood=neighborhood,
                )
            )
    result = MinuteStudyHeatmap(
        x_axis=x_axis, y_axis=y_axis, current_study_id=current_study_id,
        selection_cutoff=cutoff,
        trial_set_hash=canonical_sha256({
            "protocols": [
                p.model_dump(mode="json") for p in sorted(verified, key=lambda p: p.study_id)
            ],
            "observations": [
                o.model_dump(mode="json") for o in sorted(facts, key=lambda o: o.study_id)
            ],
        }),
        cells=tuple(cells),
    )
    if len(result.model_dump_json().encode("utf-8")) > MAX_RESULT_WIRE_BYTES:
        raise ValueError("heatmap exceeds the original result wire byte budget")
    return result
