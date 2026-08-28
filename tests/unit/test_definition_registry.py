from __future__ import annotations

import hashlib
import json
import math
import multiprocessing
import os
import stat
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event
from time import sleep
from types import MappingProxyType

import pytest

import rquant.definition_registry as definition_registry_module
import rquant.strategy_evaluators as strategy_evaluators_module
from rquant.definition_registry import (
    DefinitionConflictError,
    DefinitionExecutableIntegrityError,
    DefinitionIntegrityError,
    DefinitionReferenceError,
    FeatureExecutionBinding,
    ImmutableDefinitionRegistry,
    StrategyExecutionBinding,
    StrategyExitEligibility,
    StrategyExitPriceBasis,
    StrategyExitRule,
    StrategyPercentStop,
    StrategySellTranche,
    StrategyStructureStop,
    StrategyTrailingTakeProfit,
    TrustedExecutableRegistry,
    TrustedFeatureImplementation,
    TrustedStrategyImplementation,
)
from rquant.executable_dependencies import (
    ExecutableBinding,
    ExecutableDependencyError,
    capture_executable_dependency_guard,
)
from rquant.feature_contracts import (
    FeatureContract,
    FeatureDefinition,
    FeatureRequirement,
    RequirementLevel,
)
from rquant.intraday_feature_engine import live_compute, replay_compute
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_evaluators import _n_shape_evaluator
from rquant.strategy_spec import (
    StateTransition,
    StrategyLifecycleState,
    StrategyRunMode,
    StrategySpec,
)
from rquant.strict_json import canonical_json_bytes

COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
NOW = datetime(2026, 8, 2, 1, 0, tzinfo=UTC)

_FINGERPRINT_DEPENDENCY: object = None
_MUTABLE_LIST_DEPENDENCY = ["one"]
_MUTABLE_SET_DEPENDENCY = {"one"}
_MUTABLE_MAPPING_SOURCE = {"one": 1}
_MUTABLE_MAPPING_DEPENDENCY = MappingProxyType(_MUTABLE_MAPPING_SOURCE)


def _fingerprint_dependency_probe() -> object:
    return _FINGERPRINT_DEPENDENCY


def _always_finite(_value: object) -> bool:
    return True


def _mutable_container_dependency_probe() -> int:
    return (
        len(_MUTABLE_LIST_DEPENDENCY)
        + len(_MUTABLE_SET_DEPENDENCY)
        + len(_MUTABLE_MAPPING_DEPENDENCY)
    )


class _InjectedPublishAbort(BaseException):
    pass


def _hard_exit_after_version_publish(
    root: str,
    parent_fingerprint: str,
) -> None:
    registry = _new_registry(Path(root))
    original = registry._publish_version_from_leases

    def exit_after_rename(**kwargs: object) -> int:
        descriptor = original(**kwargs)  # type: ignore[arg-type]
        del descriptor
        os._exit(93)

    registry._publish_version_from_leases = exit_after_rename  # type: ignore[method-assign]
    contract = _contract(version=2)
    registry.register_feature_contract(
        contract,
        registered_at=NOW + timedelta(seconds=1),
        available_at=NOW + timedelta(seconds=1),
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
        parent_fingerprint=parent_fingerprint,
        supersedes=1,
        replacement_reason="hard-exit probe",
    )


def _exit_rule(
    *,
    evaluator_id: str,
    event: str,
    action: str,
    sequence: int,
    position_fraction: float,
    reevaluate_after_fill: bool,
    terminal_after_fill: bool,
    fill_event: str | None = None,
) -> StrategyExitRule:
    return StrategyExitRule(
        event=event,
        fill_event=fill_event,
        action=action,
        evaluator_id=evaluator_id,
        eligibility=StrategyExitEligibility(
            settlement_rule="a_share_t_plus_one",
            minimum_holding_trading_sessions=1,
            same_day_sell_allowed=False,
            sellable_position_required=True,
        ),
        price_basis=StrategyExitPriceBasis(
            adjustment_basis="raw",
            decision_price="minute_close",
            execution_price="next_minute_open",
        ),
        structure_stop=StrategyStructureStop(
            reference="signal_support",
            buffer_bps=0,
        ),
        percent_stop=StrategyPercentStop(
            maximum_loss_bps=300,
            acts_as_fallback=True,
        ),
        trailing_take_profit=StrategyTrailingTakeProfit(
            activation_gain_bps=800,
            retracement_bps=300,
            high_watermark="eligible_intraday_high",
        ),
        sell_tranche=StrategySellTranche(
            sequence=sequence,
            position_fraction=position_fraction,
            reevaluate_after_fill=reevaluate_after_fill,
            terminal_after_fill=terminal_after_fill,
        ),
    )


def _feature(
    name: str,
    *,
    source: str = "minute_bar",
) -> FeatureDefinition:
    return FeatureDefinition(
        name=name,
        dtype="float64",
        source_datasets=(source,),
        lookback=20,
        pit_rule="available_at <= decision_time",
        price_basis="raw",
        availability_contract={
            "source_available_at_basis": "max_source_available_at",
            "max_delay_seconds": 60,
            "missing_policy": "mark_unavailable",
            "late_policy": "mark_stale",
            "decision_visibility_gate": "available_at_lte_decision_time",
        },
    )


def _feature_bindings(contract: FeatureContract) -> tuple[FeatureExecutionBinding, ...]:
    return _execution_registry().feature_bindings(contract)


def _feature_definition_fingerprint(
    contract: FeatureContract,
    *,
    parent_fingerprint: str | None = None,
    replacement_reason: str | None = None,
) -> str:
    return definition_registry_module._feature_definition_fingerprint(
        contract,
        _feature_bindings(contract),
        parent_fingerprint=parent_fingerprint,
        supersedes=contract.version - 1 if contract.version > 1 else None,
        replacement_reason=replacement_reason,
    )


def _contract(
    *,
    contract_id: str = "intraday-core",
    version: int = 1,
    features: tuple[FeatureDefinition, ...] | None = None,
    producer_commit: str = COMMIT_A,
) -> FeatureContract:
    return FeatureContract(
        contract_id=contract_id,
        version=version,
        features=features or (_feature("same_minute_volume_ratio"), _feature("vwap")),
        producer_commit=producer_commit,
    )


def _requirement(name: str, level: RequirementLevel) -> FeatureRequirement:
    return FeatureRequirement(
        name=name,
        level=level,
        min_contract_version=1,
        allow_degraded=level is RequirementLevel.OPTIONAL,
    )


def _strategy(
    *,
    strategy_id: str = "n-shape",
    version: int = 1,
    contract_id: str = "intraday-core",
    min_contract_version: int = 1,
    required: tuple[str, ...] = ("same_minute_volume_ratio",),
    optional: tuple[str, ...] = ("vwap",),
    producer_commit: str = COMMIT_B,
    threshold: float = 1.5,
) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        version=version,
        feature_contract_id=contract_id,
        min_feature_contract_version=min_contract_version,
        required_features=tuple(_requirement(name, RequirementLevel.REQUIRED) for name in required),
        optional_features=tuple(_requirement(name, RequirementLevel.OPTIONAL) for name in optional),
        initial_state=StrategyLifecycleState.IDLE,
        transitions=(
            StateTransition(
                from_state=StrategyLifecycleState.IDLE,
                event="candidate",
                to_state=StrategyLifecycleState.WATCHING,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.WATCHING,
                event="entry",
                to_state=StrategyLifecycleState.HOLDING,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.HOLDING,
                event="take_profit_partial",
                to_state=StrategyLifecycleState.HOLDING,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.HOLDING,
                event="exit",
                to_state=StrategyLifecycleState.HOLDING,
            ),
            StateTransition(
                from_state=StrategyLifecycleState.HOLDING,
                event="exit_filled",
                to_state=StrategyLifecycleState.TERMINAL,
            ),
        ),
        parameters={"threshold": threshold},
        allowed_actions=("buy", "emit_signal", "reduce", "s_intent"),
        run_mode=StrategyRunMode.SHADOW,
        producer_commit=producer_commit,
    )


def _strategy_binding(spec: StrategySpec) -> StrategyExecutionBinding:
    return _execution_registry().strategy_binding(spec)


def _execution_registry(
    *,
    feature_overrides: dict[str, object] | None = None,
    omitted_features: set[str] | None = None,
) -> TrustedExecutableRegistry:
    feature_names = {
        "left",
        "right",
        "same_minute_volume_ratio",
        "volume_acceleration",
        "volume_ratio",
        "vwap",
    }
    feature_names -= omitted_features or set()
    feature_evaluators = {name: live_compute for name in feature_names}
    feature_evaluators.update(feature_overrides or {})
    evaluator_id = definition_registry_module._callable_identity(_n_shape_evaluator)
    return TrustedExecutableRegistry(
        features=tuple(
            TrustedFeatureImplementation(
                feature_name=name,
                implementation_version="1.0.0",
                evaluator=evaluator,  # type: ignore[arg-type]
            )
            for name, evaluator in sorted(feature_evaluators.items())
        ),
        strategies=(
            TrustedStrategyImplementation(
                strategy_id="n-shape",
                implementation_version="1.0.0",
                candidate_schema_fingerprint="9" * 64,
                entry_evaluator=_n_shape_evaluator,
                exit_evaluator=_n_shape_evaluator,
                runtime_evaluator=_n_shape_evaluator,
                entry_event="entry",
                exit_rules=(
                    _exit_rule(
                        evaluator_id=evaluator_id,
                        event="take_profit_partial",
                        action="reduce",
                        sequence=1,
                        position_fraction=0.5,
                        reevaluate_after_fill=True,
                        terminal_after_fill=False,
                    ),
                    _exit_rule(
                        evaluator_id=evaluator_id,
                        event="exit",
                        action="s_intent",
                        sequence=2,
                        position_fraction=1.0,
                        reevaluate_after_fill=False,
                        terminal_after_fill=True,
                        fill_event="exit_filled",
                    ),
                ),
            ),
        ),
    )


def _new_registry(root: Path) -> ImmutableDefinitionRegistry:
    return ImmutableDefinitionRegistry(
        root,
        execution_registry=_execution_registry(),
    )


def _registry(tmp_path: Path) -> ImmutableDefinitionRegistry:
    return _new_registry(tmp_path / "definitions")


def _only_record(root: Path, kind: str) -> Path:
    records = list((root / kind).rglob("*.json"))
    assert len(records) == 1
    return records[0]


def test_registry_rejects_arbitrary_execution_binding_resolvers(tmp_path: Path) -> None:
    with pytest.raises(TypeError):
        ImmutableDefinitionRegistry(
            tmp_path / "definitions",
            feature_binding_resolver=_feature_bindings,
            strategy_binding_resolver=_strategy_binding,
        )


def test_exit_rule_rejects_implicit_risk_and_execution_defaults() -> None:
    with pytest.raises(ValueError):
        StrategyExitRule(
            event="exit",
            action="s_intent",
            evaluator_id="tests.fake:exit",
            position_fraction=1.0,
        )


def test_trusted_allowlist_rejects_non_callable_implementation_path() -> None:
    with pytest.raises(TypeError, match="top-level Python function"):
        TrustedExecutableRegistry(
            features=(
                TrustedFeatureImplementation(
                    feature_name="vwap",
                    implementation_version="1.0.0",
                    evaluator="missing.module:compute",  # type: ignore[arg-type]
                ),
            ),
            strategies=(),
        )


def test_forged_allowlist_cannot_inject_an_artificial_formula_hash(tmp_path: Path) -> None:
    artificial = FeatureExecutionBinding(
        feature_name="vwap",
        implementation_id="missing.module:compute",
        implementation_version="1.0.0",
        formula_fingerprint="0" * 64,
    )

    class ForgedRegistry(TrustedExecutableRegistry):
        def feature_bindings(
            self,
            contract: FeatureContract,
        ) -> tuple[FeatureExecutionBinding, ...]:
            return (artificial,)

    forged = object.__new__(ForgedRegistry)
    with pytest.raises(TypeError, match="concrete TrustedExecutableRegistry"):
        ImmutableDefinitionRegistry(
            tmp_path / "definitions",
            execution_registry=forged,
        )


def test_real_callable_allowlist_binds_shared_live_replay_and_strategy_evaluators() -> None:
    execution_registry = _execution_registry()
    feature_binding = next(
        binding
        for binding in execution_registry.feature_bindings(_contract())
        if binding.feature_name == "vwap"
    )
    strategy_binding = execution_registry.strategy_binding(_strategy())

    assert feature_binding.live_replay_shared is True
    assert feature_binding.implementation_id == definition_registry_module._callable_identity(
        live_compute
    )
    assert feature_binding.formula_fingerprint == definition_registry_module._callable_fingerprint(
        live_compute,
        "1.0.0",
    )
    assert strategy_binding.entry_evaluator_id == definition_registry_module._callable_identity(
        _n_shape_evaluator
    )
    assert strategy_binding.exit_evaluator_id == strategy_binding.entry_evaluator_id


def test_definition_read_revalidates_executable_against_current_allowlist(
    tmp_path: Path,
) -> None:
    contract = _contract()
    writer = _new_registry(tmp_path / "definitions")
    record = writer.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    reader = ImmutableDefinitionRegistry(
        tmp_path / "definitions",
        execution_registry=_execution_registry(
            feature_overrides={
                "same_minute_volume_ratio": replay_compute,
                "vwap": replay_compute,
            }
        ),
    )

    with pytest.raises(DefinitionIntegrityError, match="trusted executable allowlist"):
        reader.read_feature_contract(record.fingerprint)


def test_strategy_runtime_reverify_binds_referenced_module_attribute(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    feature = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    spec = _strategy()
    record = registry.register_strategy_spec(
        spec,
        feature_contract_fingerprint=feature.fingerprint,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_B,
        expected_fingerprint=spec.spec_fingerprint,
    )
    original = record.execution_binding.runtime_evaluator_fingerprint

    monkeypatch.setattr(strategy_evaluators_module.math, "isfinite", _always_finite)

    changed = _execution_registry().strategy_binding(spec)
    assert changed.runtime_evaluator_fingerprint != original
    with pytest.raises(DefinitionExecutableIntegrityError, match="trusted executable allowlist"):
        registry.read_strategy_spec(record.fingerprint)


def test_unreferenced_module_attribute_does_not_change_callable_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = definition_registry_module._callable_fingerprint(_n_shape_evaluator, "1.0.0")

    monkeypatch.setattr(math, "_rquant_unreferenced_probe", object(), raising=False)

    assert definition_registry_module._callable_fingerprint(_n_shape_evaluator, "1.0.0") == before


@pytest.mark.parametrize(
    "dependency",
    [
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive-infinity"),
        pytest.param(float("-inf"), id="negative-infinity"),
        pytest.param(object(), id="noncanonical-object"),
    ],
)
def test_callable_fingerprint_rejects_noncanonical_dependencies(
    dependency: object,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys.modules[__name__],
        "_FINGERPRINT_DEPENDENCY",
        dependency,
    )

    with pytest.raises(TypeError, match="finite|canonical|fingerprint"):
        definition_registry_module._callable_fingerprint(
            _fingerprint_dependency_probe,
            "1.0.0",
        )


@pytest.mark.parametrize("dependency_kind", ["deep", "cyclic", "too-many-nodes", "too-large"])
def test_callable_fingerprint_fails_closed_on_bounded_dependency_graphs(
    dependency_kind: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if dependency_kind == "deep":
        dependency: object = "leaf"
        for _ in range(128):
            dependency = [dependency]
    elif dependency_kind == "cyclic":
        cycle: list[object] = []
        cycle.append(cycle)
        dependency = cycle
    elif dependency_kind == "too-many-nodes":
        dependency = list(range(20_000))
    else:
        dependency = "x" * (2 * 1024 * 1024)
    monkeypatch.setattr(
        sys.modules[__name__],
        "_FINGERPRINT_DEPENDENCY",
        dependency,
    )

    with pytest.raises(TypeError, match="cycle|depth|node|byte|budget|bounded"):
        definition_registry_module._callable_fingerprint(
            _fingerprint_dependency_probe,
            "1.0.0",
        )


def test_dependency_guard_snapshots_mutable_container_content_and_restores() -> None:
    guard = capture_executable_dependency_guard(
        (ExecutableBinding.from_callable(_mutable_container_dependency_probe),),
        contract="test-mutable-container-dependency/v1",
    )
    mutations = (
        (
            lambda: _MUTABLE_LIST_DEPENDENCY.append("two"),
            lambda: _MUTABLE_LIST_DEPENDENCY.pop(),
        ),
        (
            lambda: _MUTABLE_SET_DEPENDENCY.add("two"),
            lambda: _MUTABLE_SET_DEPENDENCY.remove("two"),
        ),
        (
            lambda: _MUTABLE_MAPPING_SOURCE.__setitem__("two", 2),
            lambda: _MUTABLE_MAPPING_SOURCE.__delitem__("two"),
        ),
    )

    for mutate, restore in mutations:
        try:
            mutate()
            assert guard.current_fingerprint() != guard.fingerprint
            with pytest.raises(ExecutableDependencyError, match="fingerprint changed"):
                guard.assert_unchanged()
        finally:
            restore()
        assert guard.current_fingerprint() == guard.fingerprint
        guard.assert_unchanged()


def test_schema_v3_record_requires_explicit_reregistration_without_hash_reinterpretation(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    payload = json.loads(path.read_bytes())
    payload["schema_version"] = 3
    payload["schema_compatibility"] = {
        "producer_schema_version": 3,
        "min_consumer_schema_version": 3,
        "max_consumer_schema_version": 3,
        "legacy_schema_policy": "explicit_re_registration_required",
    }
    payload["record_hash"] = canonical_sha256(
        {key: value for key, value in payload.items() if key != "record_hash"}
    )
    path.chmod(0o600)
    path.write_bytes(canonical_json_bytes(payload))
    path.chmod(0o400)

    with pytest.raises(
        DefinitionIntegrityError,
        match="schema v3.*explicit re-registration|explicit re-registration.*schema v3",
    ):
        registry.read_feature_contract(record.fingerprint)


def _is_publish_staging_name(name: str) -> bool:
    prefix = ".publish-"
    suffix = name.removeprefix(prefix)
    return (
        name.startswith(prefix)
        and len(suffix) == 32
        and all(character in "0123456789abcdef" for character in suffix)
    )


def _install_bounded_scandir_probe(
    monkeypatch: pytest.MonkeyPatch,
    *,
    target: Path,
) -> list[int]:
    target_stat = target.stat()
    target_identity = (target_stat.st_dev, target_stat.st_ino)
    original_listdir = definition_registry_module.os.listdir
    original_scandir = definition_registry_module.os.scandir
    consumed = [0]

    def is_target(path: os.PathLike[str] | str | int) -> bool:
        observed = (
            definition_registry_module.os.fstat(path)
            if isinstance(path, int)
            else definition_registry_module.os.stat(path)
        )
        return (observed.st_dev, observed.st_ino) == target_identity

    class CountingScandir:
        def __init__(self, wrapped: object) -> None:
            self._wrapped = wrapped

        def __enter__(self) -> CountingScandir:
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_value: BaseException | None,
            traceback: object,
        ) -> None:
            self.close()

        def __iter__(self) -> CountingScandir:
            return self

        def __next__(self) -> os.DirEntry[str]:
            entry = next(self._wrapped)  # type: ignore[arg-type]
            consumed[0] += 1
            return entry

        def close(self) -> None:
            self._wrapped.close()  # type: ignore[union-attr]

    def reject_listdir(path: os.PathLike[str] | str | int) -> list[str]:
        if is_target(path):
            raise AssertionError("bounded enumeration used listdir")
        return original_listdir(path)

    def count_scandir(path: os.PathLike[str] | str | int = ".") -> object:
        wrapped = original_scandir(path)
        return CountingScandir(wrapped) if is_target(path) else wrapped

    monkeypatch.setattr(definition_registry_module.os, "listdir", reject_listdir)
    monkeypatch.setattr(definition_registry_module.os, "scandir", count_scandir)
    return consumed


def test_feature_registration_is_content_addressed_private_and_idempotent(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()

    first = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    second = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert first == second
    assert first.fingerprint != contract.contract_fingerprint
    assert first.execution_bindings == _feature_bindings(contract)
    assert first.record_hash
    assert registry.read_feature_contract(first.fingerprint) == first
    record_path = _only_record(registry.root, "features")
    assert record_path.name == f"{first.fingerprint}.json"
    assert stat.S_IMODE(record_path.stat().st_mode) == 0o400
    assert not list(registry.root.rglob("*current*"))
    assert not list(registry.root.rglob("*latest*"))


def test_feature_formula_binding_changes_definition_fingerprint(tmp_path: Path) -> None:
    contract = _contract()
    standard = _new_registry(tmp_path / "standard").register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    changed_registry = ImmutableDefinitionRegistry(
        tmp_path / "changed",
        execution_registry=_execution_registry(
            feature_overrides={"same_minute_volume_ratio": replay_compute}
        ),
    )
    changed = changed_registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert changed.fingerprint != standard.fingerprint


def test_feature_registration_requires_exact_execution_bindings(tmp_path: Path) -> None:
    registry = ImmutableDefinitionRegistry(
        tmp_path / "definitions",
        execution_registry=_execution_registry(omitted_features={"vwap"}),
    )
    contract = _contract()

    with pytest.raises(DefinitionReferenceError, match="missing"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )


def test_strategy_requires_reachable_terminal_exit_and_sell_semantics(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    feature = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    incomplete = _strategy().model_copy(
        update={
            "transitions": (
                StateTransition(
                    from_state=StrategyLifecycleState.IDLE,
                    event="candidate",
                    to_state=StrategyLifecycleState.WATCHING,
                ),
            )
        }
    )

    with pytest.raises(DefinitionReferenceError, match="terminal exit"):
        registry.register_strategy_spec(
            incomplete,
            feature_contract_fingerprint=feature.fingerprint,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_B,
            expected_fingerprint=incomplete.spec_fingerprint,
        )


def test_strategy_registration_binds_structured_t_plus_one_and_staged_exit(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    feature = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    strategy = _strategy()
    registered = registry.register_strategy_spec(
        strategy,
        feature_contract_fingerprint=feature.fingerprint,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_B,
        expected_fingerprint=strategy.spec_fingerprint,
    )

    partial, terminal = registered.execution_binding.exit_rules
    assert registered.schema_version == 5
    assert registered.candidate_schema_fingerprint == "9" * 64
    assert (
        registered.execution_binding.candidate_schema_fingerprint
        == registered.candidate_schema_fingerprint
    )
    assert registered.executable_fingerprint == (
        definition_registry_module._strategy_executable_fingerprint(
            registered.spec,
            registered.execution_binding,
        )
    )
    assert partial.eligibility.settlement_rule == "a_share_t_plus_one"
    assert partial.eligibility.same_day_sell_allowed is False
    assert partial.price_basis.adjustment_basis == "raw"
    assert partial.structure_stop.reference == "signal_support"
    assert partial.percent_stop.acts_as_fallback is True
    assert partial.trailing_take_profit.high_watermark == "eligible_intraday_high"
    assert partial.sell_tranche.position_fraction == 0.5
    assert partial.sell_tranche.reevaluate_after_fill is True
    assert partial.sell_tranche.terminal_after_fill is False
    assert terminal.sell_tranche.position_fraction == 1.0
    assert terminal.sell_tranche.terminal_after_fill is True


def test_strategy_rejects_partial_sell_transition_that_becomes_terminal(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    feature = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    strategy = _strategy()
    broken = strategy.model_copy(
        update={
            "transitions": tuple(
                transition.model_copy(update={"to_state": StrategyLifecycleState.TERMINAL})
                if transition.event == "take_profit_partial"
                else transition
                for transition in strategy.transitions
            )
        }
    )

    with pytest.raises(DefinitionReferenceError, match="terminal transition events"):
        registry.register_strategy_spec(
            broken,
            feature_contract_fingerprint=feature.fingerprint,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_B,
            expected_fingerprint=broken.spec_fingerprint,
        )


def test_strategy_rejects_terminal_exit_unreachable_from_initial_state(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    feature = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    unreachable_exit = _strategy().model_copy(
        update={
            "transitions": (
                StateTransition(
                    from_state=StrategyLifecycleState.IDLE,
                    event="candidate",
                    to_state=StrategyLifecycleState.WATCHING,
                ),
                StateTransition(
                    from_state=StrategyLifecycleState.ARMED,
                    event="exit",
                    to_state=StrategyLifecycleState.TERMINAL,
                ),
            )
        }
    )

    with pytest.raises(DefinitionReferenceError, match="reachable terminal exit"):
        registry.register_strategy_spec(
            unreachable_exit,
            feature_contract_fingerprint=feature.fingerprint,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_B,
            expected_fingerprint=unreachable_exit.spec_fingerprint,
        )


def test_definition_versions_require_contiguous_auditable_lineage(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    first_contract = _contract(version=1)
    first = registry.register_feature_contract(
        first_contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first_contract.contract_fingerprint,
    )
    third = _contract(version=3)

    with pytest.raises(DefinitionConflictError, match="contiguous"):
        registry.register_feature_contract(
            third,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=third.contract_fingerprint,
            parent_fingerprint=first.fingerprint,
            supersedes=2,
            replacement_reason="attempt to skip a version",
        )

    second_contract = _contract(version=2)
    second = registry.register_feature_contract(
        second_contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=second_contract.contract_fingerprint,
        parent_fingerprint=first.fingerprint,
        supersedes=1,
        replacement_reason="add revised formulas",
    )
    assert second.parent_fingerprint == first.fingerprint
    assert second.supersedes == 1
    assert second.replacement_reason == "add revised formulas"
    assert registry.latest_feature_contract("intraday-core", as_of=NOW) == second


def test_definition_reads_fail_closed_when_lineage_parent_is_missing(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    first_contract = _contract(version=1)
    first = registry.register_feature_contract(
        first_contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first_contract.contract_fingerprint,
    )
    second_contract = _contract(version=2)
    second = registry.register_feature_contract(
        second_contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=second_contract.contract_fingerprint,
        parent_fingerprint=first.fingerprint,
        supersedes=1,
        replacement_reason="add revised formulas",
    )
    logical_dir = (
        registry.root
        / "features"
        / hashlib.sha256(first_contract.contract_id.encode("utf-8")).hexdigest()
    )
    (logical_dir / "v1").chmod(0o700)
    (logical_dir / "v1").rename(tmp_path / "removed-v1")

    with pytest.raises(DefinitionIntegrityError, match="lineage|contiguous"):
        registry.latest_feature_contract("intraday-core", as_of=NOW)
    with pytest.raises(DefinitionIntegrityError, match="lineage|contiguous"):
        registry.read_feature_contract(second.fingerprint)


def test_unknown_definition_schema_fails_closed_at_dispatch(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    path.chmod(0o600)
    path.write_bytes(path.read_bytes().replace(b'"schema_version":5', b'"schema_version":99'))
    path.chmod(0o400)

    with pytest.raises(DefinitionIntegrityError, match="unsupported definition schema"):
        registry.read_feature_contract(record.fingerprint)


@pytest.mark.parametrize("legacy_version", (1, 2, 3, 4))
def test_legacy_definition_schema_requires_explicit_reregistration(
    tmp_path: Path,
    legacy_version: int,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    path.chmod(0o600)
    path.write_bytes(
        path.read_bytes().replace(
            b'"schema_version":5',
            f'"schema_version":{legacy_version}'.encode(),
        )
    )
    path.chmod(0o400)

    with pytest.raises(
        DefinitionIntegrityError,
        match="explicit re-registration|cannot be inferred",
    ):
        registry.read_feature_contract(record.fingerprint)


def test_same_logical_feature_version_cannot_be_overwritten(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    first = _contract(features=(_feature("vwap"),))
    changed = _contract(features=(_feature("vwap", source="minute_bar_v2"),))
    registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )

    with pytest.raises(DefinitionConflictError, match="logical id and version"):
        registry.register_feature_contract(
            changed,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=changed.contract_fingerprint,
        )


def test_as_of_latest_excludes_future_definitions(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    visible = _contract(version=1)
    future = _contract(version=2)
    old = registry.register_feature_contract(
        visible,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=visible.contract_fingerprint,
    )
    future_record = registry.register_feature_contract(
        future,
        registered_at=NOW,
        available_at=NOW + timedelta(days=1),
        producer_commit=COMMIT_A,
        expected_fingerprint=future.contract_fingerprint,
        parent_fingerprint=old.fingerprint,
        supersedes=1,
        replacement_reason="add next contract version",
    )

    assert registry.latest_feature_contract("intraday-core", as_of=NOW) == old
    assert registry.read_feature_contract(future_record.fingerprint, as_of=NOW) is None
    assert registry.latest_feature_contract("intraday-core", as_of=NOW - timedelta(1)) is None


def test_strategy_binds_exact_feature_fingerprint_and_validates_features(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    feature_record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    strategy = _strategy()

    registered = registry.register_strategy_spec(
        strategy,
        feature_contract_fingerprint=feature_record.fingerprint,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_B,
        expected_fingerprint=strategy.spec_fingerprint,
    )

    assert registered.feature_contract_fingerprint == feature_record.fingerprint
    assert registered.feature_contract_version == contract.version
    assert registered.fingerprint != strategy.spec_fingerprint
    assert registered.execution_binding == _strategy_binding(strategy)
    assert registry.read_strategy_spec(registered.fingerprint) == registered
    assert registry.latest_strategy_spec("n-shape", as_of=NOW) == registered


@pytest.mark.parametrize(
    ("strategy", "fingerprint", "message"),
    [
        (_strategy(contract_id="other-contract"), None, "contract id"),
        (_strategy(min_contract_version=2), None, "minimum feature contract version"),
        (_strategy(required=("missing",)), None, "missing feature"),
        (_strategy(optional=("missing",)), None, "missing feature"),
        (_strategy(), "f" * 64, "not registered"),
    ],
)
def test_strategy_registration_fails_closed_for_invalid_feature_reference(
    tmp_path: Path,
    strategy: StrategySpec,
    fingerprint: str | None,
    message: str,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    feature = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    with pytest.raises(DefinitionReferenceError, match=message):
        registry.register_strategy_spec(
            strategy,
            feature_contract_fingerprint=fingerprint or feature.fingerprint,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_B,
            expected_fingerprint=strategy.spec_fingerprint,
        )


def test_registration_rejects_producer_commit_and_fingerprint_mismatch(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()

    with pytest.raises(DefinitionIntegrityError, match="producer_commit"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_B,
            expected_fingerprint=contract.contract_fingerprint,
        )
    with pytest.raises(DefinitionIntegrityError, match="fingerprint"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint="f" * 64,
        )


def test_tampered_record_is_rejected(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    path.chmod(0o600)
    payload = json.loads(path.read_text())
    payload["available_at"] = (NOW + timedelta(days=3)).isoformat()
    path.write_text(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    path.chmod(0o400)

    with pytest.raises(DefinitionIntegrityError, match="hash|canonical|tamper"):
        registry.read_feature_contract(record.fingerprint)


@pytest.mark.parametrize("link_kind", ["symbolic", "hard"])
def test_linked_record_is_rejected(tmp_path: Path, link_kind: str) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    path.parent.chmod(0o700)
    if link_kind == "symbolic":
        original = path.with_suffix(".original")
        path.rename(original)
        path.symlink_to(original.name)
    else:
        os.link(path, path.with_suffix(".hardlink"))

    with pytest.raises(DefinitionIntegrityError, match="link|unsafe|published safely"):
        registry.read_feature_contract(record.fingerprint)


def test_noncanonical_or_misnamed_record_fails_closed(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    path.parent.chmod(0o700)
    path.rename(path.with_name(f"{'0' * 64}.json"))

    with pytest.raises(DefinitionIntegrityError, match="file name|path|published safely"):
        registry.latest_feature_contract("intraday-core", as_of=NOW)


def test_concurrent_different_content_for_same_version_has_one_winner(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    left = _contract(features=(_feature("left"),))
    right = _contract(features=(_feature("right"),))

    def publish(contract: FeatureContract) -> object:
        return registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish, contract) for contract in (left, right)]
    successes = [future.result() for future in futures if future.exception() is None]
    errors = [future.exception() for future in futures if future.exception() is not None]

    assert len(successes) == 1
    assert len(errors) == 1
    assert isinstance(errors[0], DefinitionConflictError)
    assert len(list((registry.root / "features").rglob("*.json"))) == 1


def test_readonly_queries_do_not_create_or_modify_files(tmp_path: Path) -> None:
    absent_root = tmp_path / "absent"
    absent = _new_registry(absent_root)
    assert absent.latest_feature_contract("intraday-core", as_of=NOW) is None
    assert absent.read_feature_contract("f" * 64) is None
    assert not absent_root.exists()

    registry = _registry(tmp_path)
    contract = _contract()
    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    before = {
        path.relative_to(registry.root): (path.stat().st_mtime_ns, path.stat().st_size)
        for path in registry.root.rglob("*")
    }
    registry.latest_feature_contract("intraday-core", as_of=NOW)
    registry.read_feature_contract(contract.contract_fingerprint, as_of=NOW)
    after = {
        path.relative_to(registry.root): (path.stat().st_mtime_ns, path.stat().st_size)
        for path in registry.root.rglob("*")
    }
    assert after == before


def test_registry_can_be_created_below_a_nonprivate_external_parent(
    tmp_path: Path,
) -> None:
    external_parent = tmp_path / "shared-data"
    external_parent.mkdir(mode=0o755)
    external_parent.chmod(0o755)
    registry = _new_registry(external_parent / "definitions")
    contract = _contract()

    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert stat.S_IMODE(registry.root.stat().st_mode) == 0o700


def test_read_rejects_a_dangling_registry_root_symlink(tmp_path: Path) -> None:
    root = tmp_path / "definitions"
    root.symlink_to(tmp_path / "missing")
    registry = _new_registry(root)

    with pytest.raises(DefinitionIntegrityError, match="symbolic link|unsafe"):
        registry.latest_feature_contract("intraday-core", as_of=NOW)


def test_semantically_equal_feature_order_has_identical_persistent_bytes(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(
        features=(
            _feature("vwap", source="minute_bar"),
            FeatureDefinition(
                name="volume_ratio",
                dtype="float64",
                source_datasets=("daily_bar", "minute_bar"),
                lookback=5,
                pit_rule="available_at <= decision_time",
                price_basis="raw",
                availability_contract=_feature("volume_ratio").availability_contract,
            ),
        )
    )
    reordered = _contract(
        features=(
            FeatureDefinition(
                name="volume_ratio",
                dtype="float64",
                source_datasets=("minute_bar", "daily_bar"),
                lookback=5,
                pit_rule="available_at <= decision_time",
                price_basis="raw",
                availability_contract=_feature("volume_ratio").availability_contract,
            ),
            _feature("vwap", source="minute_bar"),
        )
    )
    assert first.contract_fingerprint == reordered.contract_fingerprint

    initial = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    initial_bytes = path.read_bytes()
    repeated = registry.register_feature_contract(
        reordered,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=reordered.contract_fingerprint,
    )

    assert repeated == initial
    assert path.read_bytes() == initial_bytes
    assert repeated.contract.features[0].name == "volume_ratio"
    assert repeated.contract.features[0].source_datasets == ("daily_bar", "minute_bar")


def test_semantically_equal_required_feature_order_has_identical_strategy_bytes(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract(
        features=(
            _feature("same_minute_volume_ratio"),
            _feature("vwap"),
            _feature("volume_acceleration"),
        )
    )
    feature = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    first = _strategy(required=("volume_acceleration", "same_minute_volume_ratio"))
    reordered = _strategy(required=("same_minute_volume_ratio", "volume_acceleration"))
    assert first.spec_fingerprint == reordered.spec_fingerprint

    initial = registry.register_strategy_spec(
        first,
        feature_contract_fingerprint=feature.fingerprint,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_B,
        expected_fingerprint=first.spec_fingerprint,
    )
    path = _only_record(registry.root, "strategies")
    initial_bytes = path.read_bytes()
    repeated = registry.register_strategy_spec(
        reordered,
        feature_contract_fingerprint=feature.fingerprint,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_B,
        expected_fingerprint=reordered.spec_fingerprint,
    )

    assert repeated == initial
    assert path.read_bytes() == initial_bytes
    assert tuple(item.name for item in repeated.spec.required_features) == (
        "same_minute_volume_ratio",
        "volume_acceleration",
    )


def test_as_of_excludes_a_definition_registered_in_the_future(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW + timedelta(hours=1),
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert registry.read_feature_contract(record.fingerprint, as_of=NOW) is None
    assert registry.latest_feature_contract("intraday-core", as_of=NOW) is None


def test_preexisting_empty_version_directory_is_quarantined(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    logical_key = hashlib.sha256(contract.contract_id.encode()).hexdigest()
    version_path = registry.root / "features" / logical_key / "v1"
    version_path.mkdir(parents=True, mode=0o700)
    for path in (
        registry.root,
        registry.root / "features",
        registry.root / "features" / logical_key,
        version_path,
    ):
        path.chmod(0o700)

    with pytest.raises(DefinitionIntegrityError, match="quarantined|untrusted"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    assert not version_path.exists()
    assert not list(version_path.parent.glob(".publish-*"))


def test_atomic_publish_quarantines_version_squatter_and_retry_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_rename = ImmutableDefinitionRegistry._atomic_rename_no_replace
    injected = False

    def occupy_final_version(parent_fd: int, source: str, target: str) -> None:
        nonlocal injected
        if target == "v1" and not injected:
            injected = True
            os.mkdir(target, 0o700, dir_fd=parent_fd)
        original_rename(parent_fd, source, target)

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_atomic_rename_no_replace",
        staticmethod(occupy_final_version),
    )

    with pytest.raises(DefinitionIntegrityError, match="quarantined|untrusted|occupied"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    logical_dir = (
        registry.root
        / "features"
        / hashlib.sha256(contract.contract_id.encode("utf-8")).hexdigest()
    )
    assert not (logical_dir / "v1").exists()
    assert not list(logical_dir.glob(".publish-*"))
    assert not list((registry.root / "lookup").rglob("*.json"))
    assert not list(registry.root.rglob("*current*"))

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_atomic_rename_no_replace",
        staticmethod(original_rename),
    )
    published = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    repeated = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert repeated == published
    assert registry.read_feature_contract(published.fingerprint) == published


def test_atomic_publish_quarantines_final_version_replaced_after_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_rename = ImmutableDefinitionRegistry._atomic_rename_no_replace
    stolen_path = tmp_path / "stolen-published-version"
    injected = False
    logical_dir = (
        registry.root
        / "features"
        / hashlib.sha256(contract.contract_id.encode("utf-8")).hexdigest()
    )

    def replace_after_atomic_rename(parent_fd: int, source: str, target: str) -> None:
        nonlocal injected
        original_rename(parent_fd, source, target)
        if target == "v1" and not injected:
            injected = True
            os.rename(logical_dir / target, stolen_path)
            os.mkdir(target, 0o700, dir_fd=parent_fd)

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_atomic_rename_no_replace",
        staticmethod(replace_after_atomic_rename),
    )

    with pytest.raises(DefinitionIntegrityError, match="identity|quarantined|staging"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    assert not (logical_dir / "v1").exists()
    assert not list(logical_dir.glob(".publish-*"))
    assert not list((registry.root / "lookup").rglob("*.json"))
    assert not list(registry.root.rglob("*current*"))

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_atomic_rename_no_replace",
        staticmethod(original_rename),
    )
    published = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    assert registry.read_feature_contract(published.fingerprint) == published


def test_concurrent_identical_publish_is_idempotent(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()

    def publish() -> object:
        return registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        records = list(executor.map(lambda _: publish(), range(24)))

    assert all(record == records[0] for record in records)
    assert len(list((registry.root / "features").rglob("*.json"))) == 1


def test_intermediate_directory_symlink_is_rejected(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    features = registry.root / "features"
    detached = tmp_path / "features-detached"
    features.rename(detached)
    features.symlink_to(detached.name)

    with pytest.raises(DefinitionIntegrityError, match="symbolic link|unsafe"):
        registry.read_feature_contract(record.fingerprint)


def test_directory_swap_during_nested_read_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    version_path = _only_record(registry.root, "features").parent
    detached = version_path.with_name("v1-detached")
    original_read = registry._read_private_record
    swapped = False

    def swap_after_read(directory_fd: int, name: str) -> bytes:
        nonlocal swapped
        payload = original_read(directory_fd, name)
        if not swapped:
            swapped = True
            version_path.chmod(0o700)
            version_path.rename(detached)
            version_path.mkdir(mode=0o700)
        return payload

    monkeypatch.setattr(registry, "_read_private_record", swap_after_read)

    with pytest.raises(DefinitionIntegrityError, match="identity changed|swapped|unsafe"):
        registry.read_feature_contract(record.fingerprint)


def test_malformed_canonical_json_is_rejected(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    payload = path.read_bytes()
    path.chmod(0o600)
    path.write_bytes(b"{\n" + payload[1:])
    path.chmod(0o400)

    with pytest.raises(DefinitionIntegrityError, match="non-canonical"):
        registry.read_feature_contract(record.fingerprint)


def test_registry_root_behind_an_ancestor_symlink_is_rejected(tmp_path: Path) -> None:
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir(mode=0o700)
    real_registry = _new_registry(real_parent / "definitions")
    contract = _contract()
    record = real_registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    alias = tmp_path / "alias"
    alias.symlink_to(real_parent, target_is_directory=True)
    linked_registry = _new_registry(alias / "definitions")

    with pytest.raises(DefinitionIntegrityError, match="ancestor|symbolic link|unsafe"):
        linked_registry.read_feature_contract(record.fingerprint)


@pytest.mark.parametrize("unsafe_kind", ["symlink", "permissions"])
def test_query_rejects_an_unsafe_unused_legal_registry_entry(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    strategies = registry.root / "strategies"
    if unsafe_kind == "symlink":
        external = tmp_path / "external-strategies"
        external.mkdir(mode=0o700)
        strategies.symlink_to(external, target_is_directory=True)
    else:
        strategies.mkdir(mode=0o700)
        strategies.chmod(0o755)

    with pytest.raises(
        DefinitionIntegrityError,
        match="symbolic link|owner-only|unsafe",
    ):
        registry.read_feature_contract(record.fingerprint)


def test_reader_sees_none_while_publish_is_hidden_between_record_and_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record_written = Event()
    allow_seal = Event()
    original_write = registry._write_private_record

    def pause_after_record(directory_fd: int, name: str, payload: bytes) -> None:
        original_write(directory_fd, name, payload)
        if name.endswith(".json"):
            record_written.set()
            assert allow_seal.wait(timeout=5)

    monkeypatch.setattr(registry, "_write_private_record", pause_after_record)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            registry.register_feature_contract,
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )
        assert record_written.wait(timeout=5)
        try:
            assert registry.latest_feature_contract("intraday-core", as_of=NOW) is None
        finally:
            allow_seal.set()
        published = future.result(timeout=5)

    assert registry.latest_feature_contract("intraday-core", as_of=NOW) == published


def test_reader_sees_none_before_atomic_version_publish(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    before_publish = Event()
    allow_publish = Event()
    original_rename = ImmutableDefinitionRegistry._atomic_rename_no_replace
    paused = False

    def pause_before_atomic_publish(
        parent_fd: int,
        source: str,
        target: str,
    ) -> None:
        nonlocal paused
        if target == "v1" and not paused:
            paused = True
            before_publish.set()
            assert allow_publish.wait(timeout=5)
        original_rename(parent_fd, source, target)

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_atomic_rename_no_replace",
        staticmethod(pause_before_atomic_publish),
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            registry.register_feature_contract,
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )
        assert before_publish.wait(timeout=5)
        try:
            assert registry.latest_feature_contract("intraday-core", as_of=NOW) is None
        finally:
            allow_publish.set()
        published = future.result(timeout=5)

    assert registry.latest_feature_contract("intraday-core", as_of=NOW) == published


def test_publish_rejects_record_hardlink_added_after_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_write = registry._write_private_record
    writes = 0
    record_name = ""

    def hardlink_after_seal(directory_fd: int, name: str, payload: bytes) -> None:
        nonlocal record_name, writes
        original_write(directory_fd, name, payload)
        writes += 1
        if writes == 1:
            record_name = name
        if writes == 2:
            os.link(
                record_name,
                "linked-record.json",
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )

    monkeypatch.setattr(registry, "_write_private_record", hardlink_after_seal)

    with pytest.raises(DefinitionIntegrityError, match="linked|extra|identity"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    assert registry.read_feature_contract(contract.contract_fingerprint) is None
    logical_dir = (
        registry.root
        / "features"
        / hashlib.sha256(contract.contract_id.encode("utf-8")).hexdigest()
    )
    assert not (logical_dir / "v1").exists()
    assert not list(logical_dir.glob(".publish-*"))
    assert not list(registry.root.rglob("*current*"))


def test_publish_rejects_third_file_injected_after_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_write = registry._write_private_record
    writes = 0

    def inject_after_seal(directory_fd: int, name: str, payload: bytes) -> None:
        nonlocal writes
        original_write(directory_fd, name, payload)
        writes += 1
        if writes == 2:
            descriptor = os.open(
                "injected",
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o400,
                dir_fd=directory_fd,
            )
            os.close(descriptor)

    monkeypatch.setattr(registry, "_write_private_record", inject_after_seal)

    with pytest.raises(DefinitionIntegrityError, match="extra|entries"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    assert registry.read_feature_contract(contract.contract_fingerprint) is None
    logical_dir = (
        registry.root
        / "features"
        / hashlib.sha256(contract.contract_id.encode("utf-8")).hexdigest()
    )
    assert not (logical_dir / "v1").exists()
    assert not list(logical_dir.glob(".publish-*"))
    assert not list(registry.root.rglob("*current*"))


def test_publish_rejects_record_replaced_after_final_staging_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_validate = registry._validate_staging_for_publish
    original_write = registry._write_private_record
    attacker_name = ".attacker-held-record"

    def replace_after_validation(
        *,
        logical_fd: int,
        staging_name: str,
        staging_fd: int,
        expected: dict[str, tuple[object, bytes]],
    ) -> None:
        original_validate(
            logical_fd=logical_fd,
            staging_name=staging_name,
            staging_fd=staging_fd,
            expected=expected,  # type: ignore[arg-type]
        )
        record_name = next(name for name in expected if name.endswith(".json"))
        os.fchmod(staging_fd, 0o700)
        os.rename(
            record_name,
            attacker_name,
            src_dir_fd=staging_fd,
            dst_dir_fd=logical_fd,
        )
        original_write(staging_fd, record_name, expected[record_name][1])

    monkeypatch.setattr(registry, "_validate_staging_for_publish", replace_after_validation)

    try:
        with pytest.raises(DefinitionIntegrityError, match="identity|replaced|staging"):
            registry.register_feature_contract(
                contract,
                registered_at=NOW,
                available_at=NOW,
                producer_commit=COMMIT_A,
                expected_fingerprint=contract.contract_fingerprint,
            )
    finally:
        logical_dir = (
            registry.root
            / "features"
            / hashlib.sha256(contract.contract_id.encode("utf-8")).hexdigest()
        )
        (logical_dir / attacker_name).unlink(missing_ok=True)

    assert not (logical_dir / "v1").exists()
    assert registry.read_feature_contract(_feature_definition_fingerprint(contract)) is None
    assert not list(registry.root.rglob("*current*"))


def test_publish_rejects_record_replaced_after_materialized_version_review(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_validate = ImmutableDefinitionRegistry._validate_materialized_version
    attacker_path = tmp_path / "attacker-held-record"

    def replace_after_materialized_review(
        cls: type[ImmutableDefinitionRegistry],
        version_fd: int,
        *,
        expected: dict[str, tuple[object, bytes]],
    ) -> None:
        original_validate(version_fd, expected=expected)  # type: ignore[arg-type]
        staging_dir = next(registry.root.rglob(".publish-*"))
        record_name = next(name for name in expected if name.endswith(".json"))
        os.chmod(staging_dir, 0o700)
        os.rename(staging_dir / record_name, attacker_path)
        replacement = staging_dir / record_name
        replacement.write_bytes(expected[record_name][1])
        replacement.chmod(0o400)

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_validate_materialized_version",
        classmethod(replace_after_materialized_review),
    )

    with pytest.raises(DefinitionIntegrityError, match="identity|replaced|staging"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    logical_dir = (
        registry.root
        / "features"
        / hashlib.sha256(contract.contract_id.encode("utf-8")).hexdigest()
    )
    assert not (logical_dir / "v1").exists()
    assert registry.read_feature_contract(_feature_definition_fingerprint(contract)) is None
    assert not list(registry.root.rglob("*current*"))


def test_reader_does_not_fail_when_staging_is_published_after_enumeration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record_written = Event()
    allow_seal = Event()
    atomically_published = Event()
    original_write = registry._write_private_record
    original_rename = registry._atomic_rename_no_replace
    original_validate = registry._validate_hidden_staging

    def pause_after_record(directory_fd: int, name: str, payload: bytes) -> None:
        original_write(directory_fd, name, payload)
        if name.endswith(".json"):
            record_written.set()
            assert allow_seal.wait(timeout=5)

    def record_atomic_publish(parent_fd: int, source: str, target: str) -> None:
        original_rename(parent_fd, source, target)
        atomically_published.set()

    def validate_after_atomic_publish(logical_fd: int, name: str) -> None:
        allow_seal.set()
        assert atomically_published.wait(timeout=5)
        original_validate(logical_fd, name)

    monkeypatch.setattr(registry, "_write_private_record", pause_after_record)
    monkeypatch.setattr(registry, "_atomic_rename_no_replace", record_atomic_publish)
    monkeypatch.setattr(registry, "_validate_hidden_staging", validate_after_atomic_publish)
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            registry.register_feature_contract,
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )
        assert record_written.wait(timeout=5)
        observed = registry.latest_feature_contract("intraday-core", as_of=NOW)
        published = future.result(timeout=5)

    assert observed is None or observed == published
    assert registry.latest_feature_contract("intraday-core", as_of=NOW) == published


@pytest.mark.parametrize(
    "mutation",
    ["truncated", "duplicate-key", "wrong-type", "unknown-field"],
)
def test_structurally_malformed_persistent_json_is_rejected(
    tmp_path: Path,
    mutation: str,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    payload = path.read_bytes()
    if mutation == "truncated":
        changed = payload[:-1]
    elif mutation == "duplicate-key":
        changed = b'{"schema_version":3,' + payload[1:]
    else:
        decoded = json.loads(payload)
        if mutation == "wrong-type":
            decoded["version"] = "not-an-integer"
        else:
            decoded["unknown_field"] = True
        changed = json.dumps(decoded, sort_keys=True, separators=(",", ":")).encode()
    path.chmod(0o600)
    path.write_bytes(changed)
    path.chmod(0o400)

    with pytest.raises(DefinitionIntegrityError, match="non-canonical|tampering"):
        registry.read_feature_contract(record.fingerprint)


def test_record_hardlink_created_after_read_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    path = _only_record(registry.root, "features")
    linked = path.with_suffix(".late-hardlink")
    original_read = definition_registry_module.os.read
    target_identity = (path.stat().st_dev, path.stat().st_ino)
    injected = False

    def add_link_after_read(descriptor: int, size: int) -> bytes:
        nonlocal injected
        payload = original_read(descriptor, size)
        observed = definition_registry_module.os.fstat(descriptor)
        if not injected and (observed.st_dev, observed.st_ino) == target_identity:
            injected = True
            path.parent.chmod(0o700)
            os.link(path, linked)
        return payload

    monkeypatch.setattr(definition_registry_module.os, "read", add_link_after_read)

    with pytest.raises(DefinitionIntegrityError, match="hard link|identity|changed"):
        registry.read_feature_contract(record.fingerprint)


def test_relative_root_is_bound_to_construction_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    origin = tmp_path / "origin"
    elsewhere = tmp_path / "elsewhere"
    origin.mkdir()
    elsewhere.mkdir()
    monkeypatch.chdir(origin)
    registry = _new_registry(Path("definitions"))
    monkeypatch.chdir(elsewhere)
    contract = _contract()

    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert registry.root == origin / "definitions"
    assert registry.root.is_dir()
    assert not (elsewhere / "definitions").exists()


def test_ancestor_fstat_failure_does_not_leak_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    before = len(os.listdir("/dev/fd"))
    original_fstat = definition_registry_module.os.fstat
    target = registry.root.stat()
    target_identity = (target.st_dev, target.st_ino)

    def fail_for_registry_root(descriptor: int) -> os.stat_result:
        observed = original_fstat(descriptor)
        if (observed.st_dev, observed.st_ino) == target_identity:
            raise OSError("injected root fstat failure")
        return observed

    with monkeypatch.context() as patch:
        patch.setattr(definition_registry_module.os, "fstat", fail_for_registry_root)
        with pytest.raises(DefinitionIntegrityError, match="ancestor|unsafe"):
            registry.latest_feature_contract("intraday-core", as_of=NOW)

    assert len(os.listdir("/dev/fd")) == before


def test_ancestor_stat_failure_after_open_does_not_leak_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    before = len(os.listdir("/dev/fd"))
    original_stat = definition_registry_module.os.stat
    root_name = registry.root.name
    root_observations = 0

    def fail_for_registry_root_after_open(
        path: os.PathLike[str] | str | int,
        *,
        dir_fd: int | None = None,
        follow_symlinks: bool = True,
    ) -> os.stat_result:
        nonlocal root_observations
        if path == root_name and dir_fd is not None and not follow_symlinks:
            root_observations += 1
            if root_observations == 2:
                raise OSError("injected root stat failure")
        return original_stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)

    with monkeypatch.context() as patch:
        patch.setattr(
            definition_registry_module.os,
            "stat",
            fail_for_registry_root_after_open,
        )
        with pytest.raises(DefinitionIntegrityError, match="ancestor|unsafe"):
            registry.latest_feature_contract("intraday-core", as_of=NOW)

    assert len(os.listdir("/dev/fd")) == before


def test_small_stale_staging_set_is_recovered_under_publish_lock(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    first_record = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    logical_dir = _only_record(registry.root, "features").parent.parent
    for suffix in range(3):
        (logical_dir / f".publish-{suffix:032x}").mkdir(mode=0o700)
    second = _contract(version=2)

    registry.register_feature_contract(
        second,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=second.contract_fingerprint,
        parent_fingerprint=first_record.fingerprint,
        supersedes=1,
        replacement_reason="advance definition",
    )

    assert not list(logical_dir.glob(".publish-*"))


def test_stale_staging_recovery_is_bounded_and_resumable(tmp_path: Path) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    first_record = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    logical_dir = _only_record(registry.root, "features").parent.parent
    limit = definition_registry_module._MAX_STAGING_RECOVERY
    for suffix in range(limit + 2):
        (logical_dir / f".publish-{suffix:032x}").mkdir(mode=0o700)
    second = _contract(version=2)

    with pytest.raises(DefinitionIntegrityError, match="recovery limit"):
        registry.register_feature_contract(
            second,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=second.contract_fingerprint,
            parent_fingerprint=first_record.fingerprint,
            supersedes=1,
            replacement_reason="advance definition",
        )
    assert len(list(logical_dir.glob(".publish-*"))) == 2

    registry.register_feature_contract(
        second,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=second.contract_fingerprint,
        parent_fingerprint=first_record.fingerprint,
        supersedes=1,
        replacement_reason="advance definition",
    )
    assert not list(logical_dir.glob(".publish-*"))


def test_stale_lookup_staging_files_are_recovered_under_bucket_lock(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    first_record = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    second = _contract(version=2)
    second_fingerprint = _feature_definition_fingerprint(
        second,
        parent_fingerprint=first_record.fingerprint,
        replacement_reason="advance definition",
    )
    bucket = registry.root / "lookups" / "features" / second_fingerprint[:2]
    bucket.mkdir(mode=0o700, parents=True, exist_ok=True)
    bucket.chmod(0o700)
    for suffix in range(3):
        stale = bucket / f".publish-{suffix:032x}"
        stale.write_bytes(b"")
        stale.chmod(0o400)

    registry.register_feature_contract(
        second,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=second.contract_fingerprint,
        parent_fingerprint=first_record.fingerprint,
        supersedes=1,
        replacement_reason="advance definition",
    )

    assert not [path for path in bucket.glob(".publish-*") if path.name != ".publish.lock"]


def test_publish_lock_never_reclaims_an_active_staging_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    second = _contract(version=2)
    first_record_written = Event()
    allow_first = Event()
    original_write = registry._write_private_record

    def pause_first_record(directory_fd: int, name: str, payload: bytes) -> None:
        original_write(directory_fd, name, payload)
        if name.endswith(".json") and not first_record_written.is_set():
            first_record_written.set()
            assert allow_first.wait(timeout=5)

    monkeypatch.setattr(registry, "_write_private_record", pause_first_record)
    with ThreadPoolExecutor(max_workers=2) as executor:
        first_future = executor.submit(
            registry.register_feature_contract,
            first,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=first.contract_fingerprint,
        )
        assert first_record_written.wait(timeout=5)
        second_future = executor.submit(
            registry.register_feature_contract,
            second,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=second.contract_fingerprint,
            parent_fingerprint=_feature_definition_fingerprint(first),
            supersedes=1,
            replacement_reason="advance definition",
        )
        sleep(0.05)
        assert not second_future.done()
        allow_first.set()
        first_future.result(timeout=5)
        second_future.result(timeout=5)


def test_exact_fingerprint_read_uses_direct_lookup_not_registry_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    def forbid_scan(**_: object) -> list[object]:
        raise AssertionError("exact fingerprint lookup scanned the registry")

    monkeypatch.setattr(registry, "_read_all", forbid_scan)

    assert registry.read_feature_contract(record.fingerprint) == record


def test_idempotent_lookup_publish_revalidates_bucket_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    bucket = registry.root / "lookups" / "features" / record.fingerprint[:2]
    detached = bucket.with_name(f"{bucket.name}-detached")
    original_read = registry._read_lookup_at
    swapped = False

    def swap_bucket_after_read(directory_fd: int, name: str) -> object:
        nonlocal swapped
        lookup = original_read(directory_fd, name)
        if not swapped:
            swapped = True
            bucket.rename(detached)
            bucket.mkdir(mode=0o700)
        return lookup

    monkeypatch.setattr(registry, "_read_lookup_at", swap_bucket_after_read)

    with pytest.raises(DefinitionIntegrityError, match="identity changed"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )


def test_fingerprint_lookup_bucket_limit_is_enforced_before_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    first_record = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    second = _contract(version=2)
    monkeypatch.setattr(definition_registry_module, "_MAX_LOOKUP_BUCKET_ENTRIES", 0)

    with pytest.raises(DefinitionIntegrityError, match="unbounded"):
        registry.register_feature_contract(
            second,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=second.contract_fingerprint,
            parent_fingerprint=first_record.fingerprint,
            supersedes=1,
            replacement_reason="advance definition",
        )

    bucket = registry.root / "lookups" / "features" / first_record.fingerprint[:2]
    assert len(list(bucket.glob("*.json"))) == 1


def test_crash_before_derived_lookup_is_repaired_from_authoritative_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_publish_lookup = registry._publish_fingerprint_lookup

    class SimulatedCrash(BaseException):
        pass

    def crash_before_lookup(
        root_fd: int,
        *,
        kind: str,
        record: object,
    ) -> None:
        raise SimulatedCrash

    monkeypatch.setattr(registry, "_publish_fingerprint_lookup", crash_before_lookup)
    with pytest.raises(SimulatedCrash):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    assert registry.latest_feature_contract("intraday-core", as_of=NOW) is None
    assert not list(registry.root.rglob("v1"))
    assert not list((registry.root / "lookups").rglob("*.json"))

    monkeypatch.setattr(
        registry,
        "_publish_fingerprint_lookup",
        original_publish_lookup,
    )
    repaired = registry.register_feature_contract(
        contract,
        registered_at=NOW + timedelta(minutes=5),
        available_at=NOW + timedelta(minutes=5),
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert repaired.registered_at == NOW + timedelta(minutes=5)
    assert registry.read_feature_contract(repaired.fingerprint) == repaired


def test_crash_after_derived_lookup_is_retryable_with_different_registration_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_publish_lookup = registry._publish_fingerprint_lookup

    class SimulatedCrash(BaseException):
        pass

    def crash_after_lookup(
        root_fd: int,
        *,
        kind: str,
        record: object,
    ) -> None:
        original_publish_lookup(root_fd, kind=kind, record=record)
        raise SimulatedCrash

    monkeypatch.setattr(registry, "_publish_fingerprint_lookup", crash_after_lookup)
    with pytest.raises(SimulatedCrash):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    assert registry.latest_feature_contract("intraday-core", as_of=NOW) is None
    assert not list(registry.root.rglob("v1"))
    assert not list((registry.root / "lookups").rglob("*.json"))

    monkeypatch.setattr(
        registry,
        "_publish_fingerprint_lookup",
        original_publish_lookup,
    )
    repaired = registry.register_feature_contract(
        contract,
        registered_at=NOW + timedelta(minutes=5),
        available_at=NOW + timedelta(minutes=5),
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    assert repaired.registered_at == NOW + timedelta(minutes=5)
    assert registry.read_feature_contract(repaired.fingerprint) == repaired


@pytest.mark.parametrize(
    "failure_stage",
    ("before_version", "before_lookup", "after_lookup", "after_final_revalidation"),
)
def test_publish_failure_at_every_commit_stage_is_not_discoverable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    original_publish_version = registry._publish_version_from_leases
    original_publish_lookup = registry._publish_fingerprint_lookup
    original_revalidate = registry._revalidate_publication_path
    revalidation_count = 0

    if failure_stage == "before_version":

        def fail_before_version(**_kwargs: object) -> int:
            raise RuntimeError("injected before version")

        monkeypatch.setattr(registry, "_publish_version_from_leases", fail_before_version)
    elif failure_stage == "before_lookup":

        def fail_before_lookup(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("injected before lookup")

        monkeypatch.setattr(registry, "_publish_fingerprint_lookup", fail_before_lookup)
    elif failure_stage == "after_lookup":

        def fail_after_lookup(*args: object, **kwargs: object) -> None:
            original_publish_lookup(*args, **kwargs)
            raise RuntimeError("injected after lookup")

        monkeypatch.setattr(registry, "_publish_fingerprint_lookup", fail_after_lookup)
    else:

        def fail_after_final_revalidation(**kwargs: object) -> None:
            nonlocal revalidation_count
            original_revalidate(**kwargs)
            revalidation_count += 1
            if revalidation_count == 2:
                raise RuntimeError("injected after final revalidation")

        monkeypatch.setattr(
            registry,
            "_revalidate_publication_path",
            fail_after_final_revalidation,
        )

    with pytest.raises(RuntimeError, match="injected"):
        registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    assert registry.latest_feature_contract("intraday-core", as_of=NOW) is None
    assert not list(registry.root.rglob("v1"))
    assert not list((registry.root / "lookups").rglob("*.json"))
    assert not list(registry.root.rglob("*current*"))

    monkeypatch.setattr(registry, "_publish_version_from_leases", original_publish_version)
    monkeypatch.setattr(registry, "_publish_fingerprint_lookup", original_publish_lookup)
    monkeypatch.setattr(registry, "_revalidate_publication_path", original_revalidate)
    retried = registry.register_feature_contract(
        contract,
        registered_at=NOW + timedelta(seconds=1),
        available_at=NOW + timedelta(seconds=1),
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    assert (
        registry.latest_feature_contract("intraday-core", as_of=NOW + timedelta(seconds=1))
        == retried
    )


@pytest.mark.parametrize("kind", ("feature", "strategy"))
@pytest.mark.parametrize(
    "failure_stage",
    ("rename", "chmod", "fsync", "dup", "return"),
)
def test_baseexception_after_version_rename_never_publishes_definition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    failure_stage: str,
) -> None:
    registry = _registry(tmp_path)
    feature_record = None
    if kind == "strategy":
        contract = _contract()
        feature_record = registry.register_feature_contract(
            contract,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=contract.contract_fingerprint,
        )

    original_publish = registry._publish_version_from_leases
    original_rename = registry._atomic_rename_no_replace
    original_fchmod = definition_registry_module.os.fchmod
    original_fsync = definition_registry_module.os.fsync
    original_dup = definition_registry_module.os.dup
    renamed = False
    chmod_after_rename = False
    fsync_aborted = False

    def rename_then_abort(
        _cls: type[ImmutableDefinitionRegistry],
        parent_fd: int,
        source: str,
        target: str,
    ) -> None:
        nonlocal renamed
        original_rename(parent_fd, source, target)
        if source.startswith(".publish-") and target == "v1":
            renamed = True
            if failure_stage == "rename":
                raise _InjectedPublishAbort("rename")

    def chmod_then_abort(descriptor: int, mode: int) -> None:
        nonlocal chmod_after_rename
        original_fchmod(descriptor, mode)
        if renamed and mode == definition_registry_module._VERSION_PUBLISHED_MODE:
            chmod_after_rename = True
            if failure_stage == "chmod":
                raise _InjectedPublishAbort("chmod")

    def fsync_then_abort(descriptor: int) -> None:
        nonlocal fsync_aborted
        original_fsync(descriptor)
        if failure_stage == "fsync" and chmod_after_rename and not fsync_aborted:
            fsync_aborted = True
            raise _InjectedPublishAbort("fsync")

    def dup_then_abort(descriptor: int) -> int:
        if failure_stage == "dup" and renamed:
            raise _InjectedPublishAbort("dup")
        return original_dup(descriptor)

    def return_then_abort(**kwargs: object) -> int:
        descriptor = original_publish(**kwargs)  # type: ignore[arg-type]
        if failure_stage == "return":
            raise _InjectedPublishAbort("return")
        return descriptor

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_atomic_rename_no_replace",
        classmethod(rename_then_abort),
    )
    monkeypatch.setattr(definition_registry_module.os, "fchmod", chmod_then_abort)
    monkeypatch.setattr(definition_registry_module.os, "fsync", fsync_then_abort)
    monkeypatch.setattr(definition_registry_module.os, "dup", dup_then_abort)
    if failure_stage == "return":
        monkeypatch.setattr(registry, "_publish_version_from_leases", return_then_abort)

    with pytest.raises(_InjectedPublishAbort, match=failure_stage):
        if kind == "feature":
            contract = _contract()
            registry.register_feature_contract(
                contract,
                registered_at=NOW,
                available_at=NOW,
                producer_commit=COMMIT_A,
                expected_fingerprint=contract.contract_fingerprint,
            )
        else:
            assert feature_record is not None
            spec = _strategy()
            registry.register_strategy_spec(
                spec,
                feature_contract_fingerprint=feature_record.fingerprint,
                registered_at=NOW,
                available_at=NOW,
                producer_commit=COMMIT_B,
                expected_fingerprint=spec.spec_fingerprint,
            )

    latest = (
        registry.latest_feature_contract("intraday-core", as_of=NOW)
        if kind == "feature"
        else registry.latest_strategy_spec("n-shape", as_of=NOW)
    )
    assert latest is None
    logical_kind = "features" if kind == "feature" else "strategies"
    assert not list((registry.root / logical_kind).rglob("v1"))
    assert not list((registry.root / "lookups" / logical_kind).rglob("*.json"))


def test_hard_exit_after_version_rename_does_not_commit_or_shadow_latest(
    tmp_path: Path,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    first_record = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    process = multiprocessing.get_context("spawn").Process(
        target=_hard_exit_after_version_publish,
        args=(str(registry.root), first_record.fingerprint),
    )

    process.start()
    process.join(timeout=15)

    assert process.exitcode == 93
    restarted = _new_registry(registry.root)
    assert restarted.latest_feature_contract("intraday-core", as_of=NOW + timedelta(minutes=1)) == (
        first_record
    )
    lookups = list((registry.root / "lookups" / "features").rglob("*.json"))
    assert len(lookups) == 1

    second = _contract(version=2)
    recovered = restarted.register_feature_contract(
        second,
        registered_at=NOW + timedelta(minutes=1),
        available_at=NOW + timedelta(minutes=1),
        producer_commit=COMMIT_A,
        expected_fingerprint=second.contract_fingerprint,
        parent_fingerprint=first_record.fingerprint,
        supersedes=1,
        replacement_reason="recover after hard exit",
    )
    assert (
        restarted.latest_feature_contract("intraday-core", as_of=NOW + timedelta(minutes=1))
        == recovered
    )


def test_large_nonempty_staging_is_recovered_in_bounded_batches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    first_record = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    logical_dir = _only_record(registry.root, "features").parent.parent
    staging = logical_dir / f".publish-{'f' * 32}"
    staging.mkdir(mode=0o700)
    for index in range(10):
        entry = staging / f"{index:064x}.json"
        entry.write_bytes(b"{}")
        entry.chmod(0o400)
    staging_identity = (staging.stat().st_dev, staging.stat().st_ino)
    original_listdir = definition_registry_module.os.listdir

    def reject_staging_materialization(path: os.PathLike[str] | str | int) -> list[str]:
        if isinstance(path, int):
            observed = definition_registry_module.os.fstat(path)
            if (observed.st_dev, observed.st_ino) == staging_identity:
                raise AssertionError("staging cleanup materialized the full directory")
        return original_listdir(path)

    monkeypatch.setattr(definition_registry_module, "_MAX_STAGING_RECOVERY", 4)
    monkeypatch.setattr(
        definition_registry_module.os,
        "listdir",
        reject_staging_materialization,
    )
    second = _contract(version=2)

    with pytest.raises(DefinitionIntegrityError, match="recovery limit"):
        registry.register_feature_contract(
            second,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=second.contract_fingerprint,
            parent_fingerprint=first_record.fingerprint,
            supersedes=1,
            replacement_reason="advance definition",
        )
    remaining = [
        path
        for path in logical_dir.iterdir()
        if path.is_dir() and _is_publish_staging_name(path.name)
    ]
    assert len(remaining) == 1
    assert len(list(remaining[0].iterdir())) == 6

    with pytest.raises(DefinitionIntegrityError, match="recovery limit"):
        registry.register_feature_contract(
            second,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=second.contract_fingerprint,
            parent_fingerprint=first_record.fingerprint,
            supersedes=1,
            replacement_reason="advance definition",
        )
    remaining = [
        path
        for path in logical_dir.iterdir()
        if path.is_dir() and _is_publish_staging_name(path.name)
    ]
    assert len(remaining) == 1
    assert len(list(remaining[0].iterdir())) == 2

    registered = registry.register_feature_contract(
        second,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=second.contract_fingerprint,
        parent_fingerprint=first_record.fingerprint,
        supersedes=1,
        replacement_reason="advance definition",
    )
    assert registered.parent_fingerprint == first_record.fingerprint
    assert not any(
        path.is_dir() and _is_publish_staging_name(path.name) for path in logical_dir.iterdir()
    )


def test_staging_cleanup_rejects_directory_swap_without_deleting_either_tree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    first_record = registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    logical_dir = _only_record(registry.root, "features").parent.parent
    staging_name = f".publish-{'e' * 32}"
    staging = logical_dir / staging_name
    staging.mkdir(mode=0o700)
    original_record = staging / f"{'1' * 64}.json"
    original_record.write_bytes(b"original")
    original_record.chmod(0o400)
    detached = logical_dir / f"{staging_name}-detached"
    replacement_record = staging / f"{'2' * 64}.json"
    original_open = ImmutableDefinitionRegistry._open_staging_directory_at
    swapped = False

    def swap_after_staging_lease(
        cls: type[ImmutableDefinitionRegistry],
        parent_fd: int,
        name: str,
    ) -> int:
        nonlocal swapped
        descriptor = original_open(parent_fd, name)
        if name == staging_name and not swapped:
            swapped = True
            staging.rename(detached)
            staging.mkdir(mode=0o700)
            replacement_record.write_bytes(b"replacement")
            replacement_record.chmod(0o400)
        return descriptor

    monkeypatch.setattr(
        ImmutableDefinitionRegistry,
        "_open_staging_directory_at",
        classmethod(swap_after_staging_lease),
    )
    second = _contract(version=2)

    with pytest.raises(DefinitionIntegrityError, match="identity changed"):
        registry.register_feature_contract(
            second,
            registered_at=NOW,
            available_at=NOW,
            producer_commit=COMMIT_A,
            expected_fingerprint=second.contract_fingerprint,
            parent_fingerprint=first_record.fingerprint,
            supersedes=1,
            replacement_reason="advance definition",
        )

    assert (detached / original_record.name).read_bytes() == b"original"
    assert replacement_record.read_bytes() == b"replacement"


def test_eexist_namespace_is_fsynced_before_child_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    registry.root.mkdir(mode=0o700)
    logical_name = hashlib.sha256(b"intraday-core").hexdigest()
    original_mkdir = definition_registry_module.os.mkdir
    original_fsync = definition_registry_module.os.fsync
    original_write = registry._write_private_record
    simulated: set[str] = set()
    events: list[tuple[str, tuple[int, int] | None]] = []

    def concurrent_mkdir(
        path: os.PathLike[str] | str,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        name = os.fspath(path)
        if name in {"features", logical_name} and name not in simulated:
            simulated.add(name)
            original_mkdir(path, mode, dir_fd=dir_fd)
            raise FileExistsError(name)
        original_mkdir(path, mode, dir_fd=dir_fd)

    def record_fsync(descriptor: int) -> None:
        observed = definition_registry_module.os.fstat(descriptor)
        events.append(("fsync", (observed.st_dev, observed.st_ino)))
        original_fsync(descriptor)

    def record_first_write(directory_fd: int, name: str, payload: bytes) -> None:
        events.append(("write", None))
        original_write(directory_fd, name, payload)

    monkeypatch.setattr(definition_registry_module.os, "mkdir", concurrent_mkdir)
    monkeypatch.setattr(definition_registry_module.os, "fsync", record_fsync)
    monkeypatch.setattr(registry, "_write_private_record", record_first_write)
    contract = _contract()

    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )

    root_stat = registry.root.stat()
    features_stat = (registry.root / "features").stat()
    required = {
        (root_stat.st_dev, root_stat.st_ino),
        (features_stat.st_dev, features_stat.st_ino),
    }
    first_write = next(index for index, event in enumerate(events) if event[0] == "write")
    durable_before_write = {
        identity
        for event, identity in events[:first_write]
        if event == "fsync" and identity is not None
    }
    assert required <= durable_before_write


def test_lookup_cleanup_never_unlinks_replacement_inserted_at_final_syscall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    bucket = registry.root / "lookups" / "features" / record.fingerprint[:2]
    stale_name = f".publish-{'c' * 32}"
    stale = bucket / stale_name
    stale.write_bytes(b"original")
    stale.chmod(0o400)
    detached = bucket / f"{stale_name}-detached"
    original_unlink = definition_registry_module.os.unlink
    swapped = False

    def swap_at_unlink(
        path: os.PathLike[str] | str,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal swapped
        name = os.fspath(path)
        if not swapped and _is_publish_staging_name(name):
            swapped = True
            if stale.exists():
                stale.rename(detached)
            stale.write_bytes(b"replacement")
            stale.chmod(0o400)
        original_unlink(path, dir_fd=dir_fd)

    monkeypatch.setattr(definition_registry_module.os, "unlink", swap_at_unlink)
    bucket_fd = os.open(bucket, os.O_RDONLY)
    try:
        with pytest.raises(DefinitionIntegrityError, match="replacement|identity"):
            registry._remove_hidden_lookup_file(bucket_fd, stale_name)
    finally:
        os.close(bucket_fd)

    assert stale.read_bytes() == b"replacement"


def test_staging_cleanup_never_rmdirs_replacement_inserted_at_final_syscall(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    first = _contract(version=1)
    registry.register_feature_contract(
        first,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=first.contract_fingerprint,
    )
    logical_dir = _only_record(registry.root, "features").parent.parent
    staging_name = f".publish-{'b' * 32}"
    staging = logical_dir / staging_name
    staging.mkdir(mode=0o700)
    entry = staging / f"{'3' * 64}.json"
    entry.write_bytes(b"original")
    entry.chmod(0o400)
    detached = logical_dir / f"{staging_name}-detached"
    original_rmdir = definition_registry_module.os.rmdir
    swapped = False

    def swap_at_rmdir(
        path: os.PathLike[str] | str,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal swapped
        name = os.fspath(path)
        if not swapped and _is_publish_staging_name(name):
            swapped = True
            if staging.exists():
                staging.rename(detached)
            staging.mkdir(mode=0o700)
        original_rmdir(path, dir_fd=dir_fd)

    monkeypatch.setattr(definition_registry_module.os, "rmdir", swap_at_rmdir)
    logical_fd = os.open(logical_dir, os.O_RDONLY)
    try:
        with pytest.raises(DefinitionIntegrityError, match="replacement|identity"):
            registry._remove_hidden_staging(logical_fd, staging_name)
    finally:
        os.close(logical_fd)

    assert staging.is_dir()


def test_read_side_staging_enumeration_stops_at_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    logical_dir = _only_record(registry.root, "features").parent.parent
    for index in range(10):
        (logical_dir / f".publish-{index:032x}").mkdir(mode=0o700)
    consumed = _install_bounded_scandir_probe(
        monkeypatch,
        target=logical_dir,
    )
    monkeypatch.setattr(definition_registry_module, "_MAX_STAGING_RECOVERY", 4)

    with pytest.raises(DefinitionIntegrityError, match="staging.*limit"):
        registry.latest_feature_contract("intraday-core", as_of=NOW)

    assert consumed[0] <= 7


def test_lookup_bucket_enumeration_stops_at_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = _registry(tmp_path)
    contract = _contract()
    record = registry.register_feature_contract(
        contract,
        registered_at=NOW,
        available_at=NOW,
        producer_commit=COMMIT_A,
        expected_fingerprint=contract.contract_fingerprint,
    )
    bucket = registry.root / "lookups" / "features" / record.fingerprint[:2]
    for index in range(10):
        path = bucket / f"{index:064x}.json"
        path.write_bytes(b"{}")
        path.chmod(0o400)
    consumed = _install_bounded_scandir_probe(monkeypatch, target=bucket)
    monkeypatch.setattr(definition_registry_module, "_MAX_LOOKUP_BUCKET_ENTRIES", 4)

    with pytest.raises(DefinitionIntegrityError, match="unbounded|limit"):
        registry.read_feature_contract(record.fingerprint)

    assert consumed[0] <= 6
