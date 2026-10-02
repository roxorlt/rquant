"""Pure daily evaluation of validated factor expressions."""

from __future__ import annotations

import ast
import math
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from itertools import pairwise
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from rquant.factor.definition import FactorDefinition
from rquant.factor.expression import parse_factor_expression

MAX_STOCKS = 5_000
MAX_OBSERVATIONS = 50_000
MAX_CONTEXT_OBSERVATIONS = 50_000
MAX_TRADE_DAYS = 1_024
MAX_RESULT_POINTS = 100_000

_MARKET_TZ = timezone(timedelta(hours=8))
_CROSS_SECTIONAL_FUNCTIONS = frozenset(
    {"cs_rank", "cs_zscore", "cs_winsorize", "industry_neutralize", "size_neutralize"}
)

FiniteValue = Annotated[float, Field(strict=True, allow_inf_nan=False)]
MissingReason = Literal[
    "before_available_date",
    "insufficient_history",
    "missing_observation",
    "missing_value",
    "zero_division",
    "zero_variance",
    "insufficient_samples",
    "non_finite_result",
    "precision_limit",
    "missing_context",
]
EvaluationErrorReason = Literal["unsupported_operator", "invalid_definition"]


class FactorTimeSeriesError(ValueError):
    """Stable whole-evaluation refusal reason."""

    def __init__(self, reason: EvaluationErrorReason) -> None:
        self.reason = reason
        super().__init__(reason)


class DecisionTime(BaseModel):
    """One calendar date's decision cutoff in the A-share market timezone."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    trade_date: date
    decision_at: AwareDatetime

    @model_validator(mode="after")
    def _same_market_date(self) -> DecisionTime:
        if self.decision_at.astimezone(_MARKET_TZ).date() != self.trade_date:
            raise PydanticCustomError(
                "factor_invalid_decision_time", "decision time is outside trade date"
            )
        return self


class FeatureObservation(BaseModel):
    """A finite or explicitly missing feature with its first visible instant."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    stock_code: str
    trade_date: date
    column: str
    value: FiniteValue | None
    first_visible_at: AwareDatetime

    @field_validator("stock_code", "column")
    @classmethod
    def _nonempty_identifier(cls, value: str) -> str:
        if not value or value.strip() != value or not value.isprintable():
            raise PydanticCustomError("factor_invalid_identifier", "identifier is invalid")
        return value


class IndustryObservation(BaseModel):
    """Industry membership first visible for one stock and trade date."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    stock_code: str
    trade_date: date
    industry: str | None
    first_visible_at: AwareDatetime

    @field_validator("stock_code")
    @classmethod
    def _stock_code(cls, value: str) -> str:
        if not value or value.strip() != value or not value.isprintable():
            raise PydanticCustomError("factor_invalid_identifier", "identifier is invalid")
        return value

    @field_validator("industry")
    @classmethod
    def _industry(cls, value: str | None) -> str | None:
        if value is not None and (
            not value or len(value) > 64 or value.strip() != value or not value.isprintable()
        ):
            raise PydanticCustomError("factor_invalid_industry", "industry is invalid")
        return value


class MarketCapObservation(BaseModel):
    """Positive market cap first visible for one stock and trade date."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    stock_code: str
    trade_date: date
    market_cap: FiniteValue | None
    first_visible_at: AwareDatetime

    @field_validator("stock_code")
    @classmethod
    def _stock_code(cls, value: str) -> str:
        if not value or value.strip() != value or not value.isprintable():
            raise PydanticCustomError("factor_invalid_identifier", "identifier is invalid")
        return value

    @field_validator("market_cap")
    @classmethod
    def _positive_cap(cls, value: float | None) -> float | None:
        if value is not None and value <= 0:
            raise PydanticCustomError("factor_invalid_market_cap", "market cap must be positive")
        return value


class FactorTimeSeriesInput(BaseModel):
    """All dates, stocks, cutoffs, and observations needed for an offline run."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )

    definition: FactorDefinition
    universe: tuple[str, ...]
    trading_days: tuple[date, ...]
    decision_times: tuple[DecisionTime, ...]
    observations: tuple[FeatureObservation, ...]
    industry_observations: tuple[IndustryObservation, ...] = ()
    market_cap_observations: tuple[MarketCapObservation, ...] = ()

    @model_validator(mode="after")
    def _validate_batch(self) -> FactorTimeSeriesInput:
        if (
            not self.universe
            or not self.trading_days
            or len(self.universe) > MAX_STOCKS
            or len(self.trading_days) > MAX_TRADE_DAYS
            or len(self.observations) > MAX_OBSERVATIONS
            or len(self.industry_observations) + len(self.market_cap_observations)
            > MAX_CONTEXT_OBSERVATIONS
            or len(self.universe) * len(self.trading_days) > MAX_RESULT_POINTS
        ):
            raise PydanticCustomError("factor_input_too_large", "factor input exceeds its bound")
        if any(not code or code.strip() != code for code in self.universe):
            raise PydanticCustomError("factor_invalid_stock", "universe stock is invalid")
        if len(set(self.universe)) != len(self.universe):
            raise PydanticCustomError("factor_duplicate_stock", "universe has duplicate stocks")
        if any(left >= right for left, right in pairwise(self.trading_days)):
            raise PydanticCustomError("factor_invalid_calendar", "trading days must ascend")
        if tuple(item.trade_date for item in self.decision_times) != self.trading_days:
            raise PydanticCustomError(
                "factor_decision_mismatch", "decision dates must match calendar"
            )

        stock_set = set(self.universe)
        date_to_decision = {item.trade_date: item.decision_at for item in self.decision_times}
        columns = set(self.definition.feature_catalog.columns)
        seen: set[tuple[str, date, str]] = set()
        for observation in self.observations:
            if observation.stock_code not in stock_set:
                raise PydanticCustomError(
                    "factor_unknown_stock", "observation stock is outside pool"
                )
            if observation.trade_date not in date_to_decision:
                raise PydanticCustomError(
                    "factor_unknown_trade_date", "observation date is outside calendar"
                )
            if observation.column not in columns:
                raise PydanticCustomError(
                    "factor_unknown_column", "observation column is outside catalog"
                )
            key = (observation.stock_code, observation.trade_date, observation.column)
            if key in seen:
                raise PydanticCustomError(
                    "factor_duplicate_observation", "observation key is duplicate"
                )
            seen.add(key)
            if observation.first_visible_at > date_to_decision[observation.trade_date]:
                raise PydanticCustomError(
                    "factor_future_observation", "observation is not visible at decision"
                )
        for label, rows in (
            ("industry", self.industry_observations),
            ("market_cap", self.market_cap_observations),
        ):
            seen_context: set[tuple[str, date]] = set()
            for row in rows:
                if row.stock_code not in stock_set:
                    raise PydanticCustomError(
                        "factor_unknown_stock", "context stock is outside pool"
                    )
                if row.trade_date not in date_to_decision:
                    raise PydanticCustomError(
                        "factor_unknown_trade_date", "context date is outside calendar"
                    )
                key = (row.stock_code, row.trade_date)
                if key in seen_context:
                    raise PydanticCustomError(
                        f"factor_duplicate_{label}", "context key is duplicate"
                    )
                seen_context.add(key)
                if row.first_visible_at > date_to_decision[row.trade_date]:
                    raise PydanticCustomError(
                        "factor_future_context", "context is not visible at decision"
                    )
        return self


class FactorTimeSeriesValue(BaseModel):
    """One stock and decision date's value or explicit absence."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    stock_code: str
    trade_date: date
    value: FiniteValue | None
    missing_reason: MissingReason | None
    latest_visible_at: AwareDatetime | None

    @model_validator(mode="after")
    def _value_or_reason(self) -> FactorTimeSeriesValue:
        if (self.value is None) == (self.missing_reason is None):
            raise PydanticCustomError(
                "factor_value_state_invalid", "value must have exactly one outcome"
            )
        return self


class FactorTimeSeriesResult(BaseModel):
    """Date-major values, with stable identity from the validated definition."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    factor_id: str
    version: int
    values: tuple[FactorTimeSeriesValue, ...]


@dataclass(frozen=True, slots=True)
class _Cell:
    value: float | None
    reason: MissingReason | None
    latest_visible_at: datetime | None


def _missing(reason: MissingReason) -> _Cell:
    return _Cell(value=None, reason=reason, latest_visible_at=None)


def _present(value: float, latest_visible_at: datetime | None) -> _Cell:
    if not math.isfinite(value):
        return _missing("non_finite_result")
    return _Cell(value=value, reason=None, latest_visible_at=latest_visible_at)


def _latest(cells: list[_Cell] | tuple[_Cell, ...]) -> datetime | None:
    instants = [cell.latest_visible_at for cell in cells if cell.latest_visible_at is not None]
    return max(instants) if instants else None


def _literal_integer(node: ast.AST) -> int:
    sign = 1
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        sign = -1 if isinstance(node.op, ast.USub) else 1
        node = node.operand
    if not isinstance(node, ast.Constant) or type(node.value) is not int:
        raise FactorTimeSeriesError("invalid_definition")
    return sign * node.value


def _literal_number(node: ast.AST) -> float:
    sign = 1
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        sign = -1 if isinstance(node.op, ast.USub) else 1
        node = node.operand
    if not isinstance(node, ast.Constant) or type(node.value) not in (int, float):
        raise FactorTimeSeriesError("invalid_definition")
    return sign * float(node.value)


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[middle]
    left, right = ordered[middle - 1], ordered[middle]
    if left == right:
        return left
    if (left < 0) == (right < 0):
        return left + (right - left) / 2
    return left / 2 + right / 2


def _scaled_mean(values: list[float]) -> float:
    scale = max(abs(value) for value in values)
    if scale == 0:
        return 0.0
    return math.fsum(value / scale for value in values) / len(values) * scale


def _log_cap_ratio(cap: float, pivot: float) -> float:
    ratio = cap / pivot
    if math.isfinite(ratio) and 0.5 <= ratio <= 2:
        return math.log1p((cap - pivot) / pivot)
    return math.log(cap) - math.log(pivot)


def _sample_std(values: list[float]) -> float:
    if len(set(values)) == 1:
        return 0.0
    scale = max(abs(value) for value in values)
    scaled = [value / scale for value in values]
    mean = math.fsum(scaled) / len(scaled)
    squares = math.fsum((value - mean) ** 2 for value in scaled)
    return math.sqrt(squares / (len(values) - 1)) * scale


def _pearson(left: list[float], right: list[float]) -> float | None:
    if len(set(left)) == 1 or len(set(right)) == 1:
        return None
    left_scale = max(abs(value) for value in left)
    right_scale = max(abs(value) for value in right)
    left_scaled = [value / left_scale for value in left]
    right_scaled = [value / right_scale for value in right]
    left_mean = math.fsum(left_scaled) / len(left)
    right_mean = math.fsum(right_scaled) / len(right)
    left_centered = [value - left_mean for value in left_scaled]
    right_centered = [value - right_mean for value in right_scaled]
    left_square = math.fsum(value * value for value in left_centered)
    right_square = math.fsum(value * value for value in right_centered)
    if left_square == 0 or right_square == 0:
        return None
    numerator = math.fsum(
        lvalue * rvalue for lvalue, rvalue in zip(left_centered, right_centered, strict=True)
    )
    return max(-1.0, min(1.0, numerator / math.sqrt(left_square * right_square)))


def neutralize_factor_cells(
    mode: Literal["industry", "size", "industry_size"],
    inputs: dict[str, _Cell],
    *,
    industries: dict[str, IndustryObservation | None] | None = None,
    market_caps: dict[str, MarketCapObservation | None] | None = None,
) -> dict[str, _Cell]:
    results = inputs.copy()
    valid = [(stock, cell) for stock, cell in inputs.items() if cell.value is not None]
    if mode == "industry":
        _neutralize_industry(valid, results, industries or {})
    elif mode == "size":
        _neutralize_size(valid, results, market_caps or {})
    elif mode == "industry_size":
        _neutralize_joint(valid, results, industries or {}, market_caps or {})
    else:
        raise ValueError("unsupported neutralization")
    return results


def _neutralize_joint(
    valid: list[tuple[str, _Cell]],
    results: dict[str, _Cell],
    industries: dict[str, IndustryObservation | None],
    market_caps: dict[str, MarketCapObservation | None],
) -> None:
    groups: dict[str, list[tuple[str, _Cell, IndustryObservation, MarketCapObservation]]] = {}
    for stock, cell in valid:
        industry, cap = industries.get(stock), market_caps.get(stock)
        if industry is None or industry.industry is None or cap is None or cap.market_cap is None:
            results[stock] = _missing("missing_context")
        else:
            groups.setdefault(industry.industry, []).append((stock, cell, industry, cap))
    eligible = []
    for group in groups.values():
        if len(group) < 2:
            results[group[0][0]] = _missing("insufficient_samples")
        else:
            eligible.append(group)
    samples = [sample for group in eligible for sample in group]
    if len(samples) <= len(eligible) + 1:
        for stock, *_ in samples:
            results[stock] = _missing("insufficient_samples")
        return
    if all(len({cap.market_cap for _, _, _, cap in group}) == 1 for group in eligible):
        for stock, *_ in samples:
            results[stock] = _missing("zero_variance")
        return
    scale = max(abs(cell.value) for _, cell, _, _ in samples) or 1.0
    if any(cell.value != 0 and cell.value / scale == 0 for _, cell, _, _ in samples):
        for stock, *_ in samples:
            results[stock] = _missing("precision_limit")
        return
    centered = []
    for group in eligible:
        pivot = group[0][3].market_cap
        offsets = [_log_cap_ratio(cap.market_cap, pivot) for _, _, _, cap in group]
        y = [cell.value / scale for _, cell, _, _ in group]
        x_mean, y_mean = math.fsum(offsets) / len(group), math.fsum(y) / len(group)
        centered.extend(
            (sample, x - x_mean, value - y_mean)
            for sample, x, value in zip(group, offsets, y, strict=True)
        )
    denominator = math.fsum(x * x for _, x, _ in centered)
    if denominator == 0:
        for stock, *_ in samples:
            results[stock] = _missing("precision_limit")
        return
    slope = math.fsum(x * y for _, x, y in centered) / denominator
    latest = max(
        instant
        for _, cell, industry, cap in samples
        for instant in (cell.latest_visible_at, industry.first_visible_at, cap.first_visible_at)
        if instant is not None
    )
    for (stock, _, _, _), x, y in centered:
        prediction = slope * x
        results[stock] = (
            _present(math.fsum((y, -prediction)) * scale, latest)
            if math.isfinite(prediction)
            else _missing("non_finite_result")
        )


def _neutralize_industry(
    valid: list[tuple[str, _Cell]],
    results: dict[str, _Cell],
    industries: dict[str, IndustryObservation | None],
) -> None:
    groups: dict[str, list[tuple[str, _Cell, IndustryObservation]]] = {}
    for stock, cell in valid:
        context = industries.get(stock)
        if context is None or context.industry is None:
            results[stock] = _missing("missing_context")
        else:
            groups.setdefault(context.industry, []).append((stock, cell, context))
    for members in groups.values():
        if len(members) < 2:
            results[members[0][0]] = _missing("insufficient_samples")
            continue
        values = [cell.value for _, cell, _ in members if cell.value is not None]
        center = _scaled_mean(values)
        latest = max(
            instant
            for _, cell, context in members
            for instant in (cell.latest_visible_at, context.first_visible_at)
            if instant is not None
        )
        for stock, cell, _ in members:
            assert cell.value is not None
            residual = cell.value - center
            if (center != 0 and residual == cell.value) or (
                cell.value != 0 and residual == -center
            ):
                results[stock] = _missing("precision_limit")
            else:
                results[stock] = _present(residual, latest)


def _neutralize_size(
    valid: list[tuple[str, _Cell]],
    results: dict[str, _Cell],
    market_caps: dict[str, MarketCapObservation | None],
) -> None:
    samples: list[tuple[str, _Cell, MarketCapObservation]] = []
    for stock, cell in valid:
        context = market_caps.get(stock)
        if context is None or context.market_cap is None:
            results[stock] = _missing("missing_context")
        else:
            samples.append((stock, cell, context))
    if len(samples) < 3:
        for stock, _, _ in samples:
            results[stock] = _missing("insufficient_samples")
        return
    caps = [context.market_cap for _, _, context in samples]
    if len(set(caps)) == 1:
        for stock, _, _ in samples:
            results[stock] = _missing("zero_variance")
        return
    pivot = caps[0]
    assert pivot is not None
    offsets = [_log_cap_ratio(cap, pivot) for cap in caps if cap is not None]
    if len(set(offsets)) == 1:
        for stock, _, _ in samples:
            results[stock] = _missing("precision_limit")
        return
    x_mean = math.fsum(offsets) / len(offsets)
    centered_x = [value - x_mean for value in offsets]
    denominator = math.fsum(value * value for value in centered_x)
    if denominator == 0:
        for stock, _, _ in samples:
            results[stock] = _missing("precision_limit")
        return
    values = [cell.value for _, cell, _ in samples]
    scale = max(abs(value) for value in values if value is not None) or 1.0
    scaled_y = [value / scale for value in values if value is not None]
    y_mean = math.fsum(scaled_y) / len(scaled_y)
    centered_y = [value - y_mean for value in scaled_y]
    slope = math.fsum(x * y for x, y in zip(centered_x, centered_y, strict=True)) / denominator
    latest = max(
        instant
        for _, cell, context in samples
        for instant in (cell.latest_visible_at, context.first_visible_at)
        if instant is not None
    )
    for (stock, _, _), x, y in zip(samples, centered_x, centered_y, strict=True):
        prediction = slope * x
        if not math.isfinite(prediction):
            results[stock] = _missing("non_finite_result")
            continue
        residual = math.fsum((y, -prediction)) * scale
        results[stock] = _present(residual, latest)


class _SeriesEvaluator:
    def __init__(
        self,
        stock_code: str,
        universe: tuple[str, ...],
        trading_days: tuple[date, ...],
        observations: dict[tuple[str, date, str], FeatureObservation],
        industries: dict[tuple[str, date], IndustryObservation],
        market_caps: dict[tuple[str, date], MarketCapObservation],
        cross_cache: dict[tuple[int, int], dict[str, _Cell]],
    ) -> None:
        self.stock_code = stock_code
        self.universe = universe
        self.trading_days = trading_days
        self.observations = observations
        self.industries = industries
        self.market_caps = market_caps
        self.cross_cache = cross_cache
        self.cache: dict[tuple[int, int], _Cell] = {}

    def evaluate(self, node: ast.AST, day_index: int) -> _Cell:
        key = (id(node), day_index)
        if key not in self.cache:
            self.cache[key] = self._evaluate_uncached(node, day_index)
        return self.cache[key]

    def _evaluate_uncached(self, node: ast.AST, day_index: int) -> _Cell:
        if isinstance(node, ast.Constant):
            return _present(float(node.value), None)
        if isinstance(node, ast.Name):
            key = (self.stock_code, self.trading_days[day_index], node.id)
            observation = self.observations.get(key)
            if observation is None:
                return _missing("missing_observation")
            if observation.value is None:
                return _missing("missing_value")
            return _present(observation.value, observation.first_visible_at)
        if isinstance(node, ast.UnaryOp):
            cell = self.evaluate(node.operand, day_index)
            if cell.value is None:
                return cell
            if isinstance(node.op, ast.USub):
                value = -cell.value
            elif isinstance(node.op, ast.UAdd):
                value = cell.value
            else:
                raise FactorTimeSeriesError("invalid_definition")
            return _present(value, cell.latest_visible_at)
        if isinstance(node, ast.BinOp):
            return self._binary(node, day_index)
        if isinstance(node, ast.Compare):
            left = self.evaluate(node.left, day_index)
            if left.value is None:
                return left
            right = self.evaluate(node.comparators[0], day_index)
            if right.value is None:
                return right
            op = node.ops[0]
            if isinstance(op, ast.Lt):
                outcome = left.value < right.value
            elif isinstance(op, ast.LtE):
                outcome = left.value <= right.value
            elif isinstance(op, ast.Gt):
                outcome = left.value > right.value
            elif isinstance(op, ast.GtE):
                outcome = left.value >= right.value
            else:
                raise FactorTimeSeriesError("invalid_definition")
            return _present(float(outcome), _latest((left, right)))
        if isinstance(node, ast.Call):
            return self._call(node, day_index)
        raise FactorTimeSeriesError("invalid_definition")

    def _binary(self, node: ast.BinOp, day_index: int) -> _Cell:
        left = self.evaluate(node.left, day_index)
        if left.value is None:
            return left
        right = self.evaluate(node.right, day_index)
        if right.value is None:
            return right
        if isinstance(node.op, ast.Div) and right.value == 0:
            return _missing("zero_division")
        try:
            if isinstance(node.op, ast.Add):
                value = left.value + right.value
            elif isinstance(node.op, ast.Sub):
                value = left.value - right.value
            elif isinstance(node.op, ast.Mult):
                value = left.value * right.value
            elif isinstance(node.op, ast.Div):
                value = left.value / right.value
            else:
                raise FactorTimeSeriesError("invalid_definition")
        except OverflowError:
            return _missing("non_finite_result")
        if (
            isinstance(node.op, ast.Add)
            and left.value != 0
            and right.value != 0
            and (value == left.value or value == right.value)
        ):
            return _missing("precision_limit")
        if isinstance(node.op, ast.Sub) and right.value != 0 and value == left.value:
            return _missing("precision_limit")
        if isinstance(node.op, ast.Sub) and left.value != 0 and value == -right.value:
            return _missing("precision_limit")
        if (
            value == 0
            and left.value != 0
            and right.value != 0
            and isinstance(node.op, (ast.Mult, ast.Div))
        ):
            return _missing("precision_limit")
        return _present(value, _latest((left, right)))

    def _window(self, node: ast.AST, day_index: int, size: int) -> list[_Cell] | _Cell:
        start = day_index - size + 1
        if start < 0:
            return _missing("insufficient_history")
        cells: list[_Cell] = []
        for index in range(start, day_index + 1):
            cell = self.evaluate(node, index)
            if cell.value is None:
                return cell
            cells.append(cell)
        return cells

    def _call(self, node: ast.Call, day_index: int) -> _Cell:
        if not isinstance(node.func, ast.Name):
            raise FactorTimeSeriesError("invalid_definition")
        name = node.func.id
        if name in _CROSS_SECTIONAL_FUNCTIONS:
            return self._cross_section(node, day_index, name)
        if name == "ref":
            offset = _literal_integer(node.args[1])
            if day_index < offset:
                return _missing("insufficient_history")
            return self.evaluate(node.args[0], day_index - offset)
        if name == "ts_delta":
            offset = _literal_integer(node.args[1])
            if day_index < offset:
                return _missing("insufficient_history")
            current = self.evaluate(node.args[0], day_index)
            if current.value is None:
                return current
            previous = self.evaluate(node.args[0], day_index - offset)
            if previous.value is None:
                return previous
            difference = current.value - previous.value
            if previous.value != 0 and difference == current.value:
                return _missing("precision_limit")
            if current.value != 0 and difference == -previous.value:
                return _missing("precision_limit")
            return _present(difference, _latest((current, previous)))
        if name not in {"ts_mean", "ts_std", "ts_rank", "ts_corr"}:
            raise FactorTimeSeriesError("invalid_definition")
        size = _literal_integer(node.args[-1])
        if name in {"ts_std", "ts_corr"} and size < 2:
            return _missing("insufficient_samples")
        first = self._window(node.args[0], day_index, size)
        if isinstance(first, _Cell):
            return first
        values = [cell.value for cell in first if cell.value is not None]
        if name == "ts_mean":
            return _present(_scaled_mean(values), _latest(first))
        if name == "ts_std":
            result = _sample_std(values)
            if result == 0 and len(set(values)) > 1:
                return _missing("precision_limit")
            return _present(result, _latest(first))
        if name == "ts_rank":
            current = values[-1]
            less = sum(value < current for value in values)
            equal = sum(value == current for value in values)
            rank = (2 * less + equal + 1) / (2 * len(values))
            return _present(rank, _latest(first))
        second = self._window(node.args[1], day_index, size)
        if isinstance(second, _Cell):
            return second
        other = [cell.value for cell in second if cell.value is not None]
        coefficient = _pearson(values, other)
        if coefficient is None:
            return _missing("zero_variance")
        return _present(coefficient, _latest(first + second))

    def _cross_section(self, node: ast.Call, day_index: int, name: str) -> _Cell:
        key = (id(node), day_index)
        if key not in self.cross_cache:
            inputs = {
                stock: (
                    self
                    if stock == self.stock_code
                    else _SeriesEvaluator(
                        stock,
                        self.universe,
                        self.trading_days,
                        self.observations,
                        self.industries,
                        self.market_caps,
                        self.cross_cache,
                    )
                ).evaluate(node.args[0], day_index)
                for stock in self.universe
            }
            valid = [(stock, cell) for stock, cell in inputs.items() if cell.value is not None]
            latest = _latest([cell for _, cell in valid])
            results = inputs.copy()
            if name == "cs_rank":
                self._cross_rank(valid, latest, results)
            elif name == "cs_zscore":
                self._cross_zscore(valid, latest, results)
            elif name == "cs_winsorize":
                self._cross_winsorize(valid, latest, results, _literal_number(node.args[1]))
            elif name == "industry_neutralize":
                self._cross_industry(day_index, valid, results)
            elif name == "size_neutralize":
                self._cross_size(day_index, valid, results)
            else:
                raise FactorTimeSeriesError("invalid_definition")
            self.cross_cache[key] = results
        return self.cross_cache[key][self.stock_code]

    @staticmethod
    def _cross_rank(
        valid: list[tuple[str, _Cell]], latest: datetime | None, results: dict[str, _Cell]
    ) -> None:
        ordered = sorted(valid, key=lambda item: item[1].value)
        index = 0
        while index < len(ordered):
            end = index + 1
            while end < len(ordered) and ordered[end][1].value == ordered[index][1].value:
                end += 1
            rank = (index + 1 + end) / (2 * len(ordered))
            for stock, _ in ordered[index:end]:
                results[stock] = _present(rank, latest)
            index = end

    def _cross_industry(
        self, day_index: int, valid: list[tuple[str, _Cell]], results: dict[str, _Cell]
    ) -> None:
        day = self.trading_days[day_index]
        results.update(
            neutralize_factor_cells(
                "industry",
                dict(valid),
                industries={stock: self.industries.get((stock, day)) for stock, _ in valid},
            )
        )

    def _cross_size(
        self, day_index: int, valid: list[tuple[str, _Cell]], results: dict[str, _Cell]
    ) -> None:
        day = self.trading_days[day_index]
        results.update(
            neutralize_factor_cells(
                "size",
                dict(valid),
                market_caps={stock: self.market_caps.get((stock, day)) for stock, _ in valid},
            )
        )

    @staticmethod
    def _cross_zscore(
        valid: list[tuple[str, _Cell]], latest: datetime | None, results: dict[str, _Cell]
    ) -> None:
        values = [cell.value for _, cell in valid if cell.value is not None]
        if len(values) < 2:
            outcome = _missing("insufficient_samples")
            for stock, _ in valid:
                results[stock] = outcome
            return
        if len(set(values)) == 1:
            outcome = _missing("zero_variance")
            for stock, _ in valid:
                results[stock] = outcome
            return
        scale = max(abs(value) for value in values)
        scaled = [value / scale for value in values]
        center = math.fsum(scaled) / len(scaled)
        variance = math.fsum((value - center) ** 2 for value in scaled) / len(scaled)
        if variance == 0:
            outcome = _missing("precision_limit")
            for stock, _ in valid:
                results[stock] = outcome
            return
        deviation = math.sqrt(variance)
        for (stock, _), value in zip(valid, scaled, strict=True):
            results[stock] = _present((value - center) / deviation, latest)

    @staticmethod
    def _cross_winsorize(
        valid: list[tuple[str, _Cell]],
        latest: datetime | None,
        results: dict[str, _Cell],
        multiple: float,
    ) -> None:
        if not valid:
            return
        values = [cell.value for _, cell in valid if cell.value is not None]
        center = _median(values)
        deviations = [abs(value - center) for value in values]
        if not all(math.isfinite(value) for value in deviations):
            outcome = _missing("non_finite_result")
        else:
            mad = _median(deviations)
            spread = multiple * 1.4826 * mad
            lower, upper = center - spread, center + spread
            if not all(math.isfinite(value) for value in (spread, lower, upper)):
                outcome = _missing("non_finite_result")
            else:
                for (stock, _), value in zip(valid, values, strict=True):
                    results[stock] = _present(max(lower, min(value, upper)), latest)
                return
        for stock, _ in valid:
            results[stock] = outcome


def evaluate_factor_time_series(data: FactorTimeSeriesInput) -> FactorTimeSeriesResult:
    """Evaluate each stock at each decision date without external I/O."""
    checked = FactorTimeSeriesInput.model_validate(data)
    parsed = parse_factor_expression(
        checked.definition.expression, checked.definition.feature_catalog
    )
    tree = ast.parse(parsed.expression, mode="eval")
    observations = {
        (row.stock_code, row.trade_date, row.column): row for row in checked.observations
    }
    industries = {(row.stock_code, row.trade_date): row for row in checked.industry_observations}
    market_caps = {(row.stock_code, row.trade_date): row for row in checked.market_cap_observations}
    cross_cache: dict[tuple[int, int], dict[str, _Cell]] = {}
    by_day: list[list[FactorTimeSeriesValue]] = [[] for _ in checked.trading_days]
    for stock_code in checked.universe:
        evaluator = _SeriesEvaluator(
            stock_code,
            checked.universe,
            checked.trading_days,
            observations,
            industries,
            market_caps,
            cross_cache,
        )
        for index, trade_date in enumerate(checked.trading_days):
            cell = (
                _missing("before_available_date")
                if checked.definition.earliest_available_date is not None
                and trade_date < checked.definition.earliest_available_date
                else evaluator.evaluate(tree.body, index)
            )
            by_day[index].append(
                FactorTimeSeriesValue(
                    stock_code=stock_code,
                    trade_date=trade_date,
                    value=cell.value,
                    missing_reason=cell.reason,
                    latest_visible_at=cell.latest_visible_at,
                )
            )
    return FactorTimeSeriesResult(
        factor_id=checked.definition.factor_id,
        version=checked.definition.version,
        values=tuple(point for day in by_day for point in day),
    )
