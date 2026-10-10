"""Map the original five growth ablations onto complete immutable recipes."""

from __future__ import annotations

from rquant.dashboard.strategy_lab_data import growth_board_ablation_specs
from rquant.minute_backtest_parameters import MinuteGrowthParameters, MinuteParameterSet
from rquant.runtime_contracts import RuntimeContractModel


class MinuteGrowthAblationVariant(RuntimeContractModel):
    key: str
    label: str
    description: str
    parameters: MinuteParameterSet


def growth_board_parameter_ablation(
    parameters: MinuteParameterSet,
) -> tuple[MinuteGrowthAblationVariant, ...]:
    parameters = MinuteParameterSet.model_validate(parameters)
    if not isinstance(parameters.parameters, MinuteGrowthParameters):
        raise ValueError("growth ablation requires a complete growth-board recipe")
    variants: list[MinuteGrowthAblationVariant] = []
    for spec in growth_board_ablation_specs():
        body = parameters.model_dump(mode="python")
        body["parameters"].update(
            {
                "require_vwap_strength": spec.require_vwap_strength,
                "use_same_minute_surge": spec.use_same_minute_surge,
                "use_accel_surge": spec.use_accel_surge,
            }
        )
        variants.append(
            MinuteGrowthAblationVariant(
                key=spec.key,
                label=spec.label,
                description=spec.description,
                parameters=MinuteParameterSet.model_validate(body),
            )
        )
    return tuple(variants)
