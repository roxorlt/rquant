"""Preparation clock orchestration; publication/plan doubles confer no authority."""

from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from rquant import experiment_platform_commands as commands
from rquant.experiment_platform import (
    ExperimentFamilyRecord,
    ExperimentPreparationReservation,
    HoldoutPolicy,
    NativeMinuteExperimentRequest,
    NativeMinuteSourceProfile,
)
from rquant.experiment_registry import DateRange
from rquant.minute_backtest_formal import MinuteExperimentProtocol
from rquant.minute_backtest_producer import minute_metadata_identities
from rquant.minute_backtest_publication_contracts import MinuteSourceContentSeed
from rquant.runtime_contracts import canonical_sha256
from rquant.strategy_authoring_commands import StrategyTemplateHead
from rquant.strategy_promotion_contracts import (
    NativeMinuteConfiguration,
    NativeMinuteSelection,
    StrategyPromotionTarget,
)
from tests.unit.test_minute_backtest_producer import source_seed


class _PlanBoundaryReachedError(Exception):
    pass


@pytest.fixture
def preparation_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    seed = source_seed(tmp_path)
    start = seed.provenance.published_at + timedelta(hours=1)
    native = seed.native_registration
    target = StrategyPromotionTarget(
        source_kind="builtin",
        owner_id=seed.runtime.owner_id,
        strategy_id=native.logical_id,
        name="N 字形态",
        head=StrategyTemplateHead(
            version=native.version,
            registration_fingerprint=native.fingerprint,
            record_hash=native.record_hash,
            spec_fingerprint=native.spec.spec_fingerprint,
        ),
        parameter_fingerprint=native.spec.parameter_fingerprint,
        cost_fingerprint=canonical_sha256(seed.runtime.execution_profile.execution_costs),
    )
    configuration = NativeMinuteConfiguration(
        selection=NativeMinuteSelection(
            target=target,
            source_key=seed.runtime.source_key,
            source_version=seed.runtime.source_version,
            profile_hash=seed.runtime.execution_profile.profile_hash,
        ),
        start_date=seed.runtime.start_date,
        end_date=seed.runtime.end_date,
    )
    outer = seed.runtime.market_calendar.open_dates[
        seed.runtime.market_calendar.open_dates.index(seed.runtime.end_date) + 1
    ]
    request = NativeMinuteExperimentRequest(
        name="时钟编排单测",
        configurations=(configuration,),
        protocol=MinuteExperimentProtocol(
            train_range=DateRange(
                start_date=seed.runtime.start_date, end_date=seed.runtime.start_date
            ),
            validation_range=DateRange(
                start_date=seed.runtime.end_date, end_date=seed.runtime.end_date
            ),
            frozen_outer_test_range=DateRange(start_date=outer, end_date=outer),
        ),
    )
    policy = HoldoutPolicy(version=1, months=0, updated_at=start - timedelta(minutes=1))
    record = ExperimentFamilyRecord(
        owner=seed.runtime.owner_id,
        request_id=UUID(int=74),
        body_hash=canonical_sha256(request),
        family_id="clock-orchestration-unit",
        request=request,
        actual_configurations=(configuration,),
        registered_at=start - timedelta(minutes=1),
        policy=policy,
    )
    profile = NativeMinuteSourceProfile(
        selection=configuration.selection,
        execution_profile=seed.runtime.execution_profile,
        producer_commit=seed.runtime.producer_commit,
        calendar=seed.runtime.market_calendar,
        coverage=DateRange(
            start_date=seed.runtime.start_date, end_date=seed.runtime.market_calendar.coverage_end
        ),
        latest_complete=seed.runtime.market_calendar.open_dates[-1],
        phase_slice_available=True,
    )
    state = SimpleNamespace(
        start=start,
        record=record,
        seed=seed,
        profile=profile,
        published_at=start + timedelta(seconds=1),
        ticks=[start, start + timedelta(seconds=2), start + timedelta(seconds=3)],
        source_override=None,
        reservation=None,
        reservations=[],
        published=[],
        plan_calls=[],
        reads=[],
    )

    def clock() -> datetime:
        return state.ticks.pop(0)

    def provider(read: object) -> MinuteSourceContentSeed:
        state.reads.append(read)
        value = seed.model_dump(mode="python", exclude_computed_fields=True)
        value["runtime"]["source_key"] = state.source_override or read.publication_source_key
        value["provenance"]["published_at"] = state.published_at
        return MinuteSourceContentSeed.model_validate(value)

    def reserve(value: ExperimentPreparationReservation) -> ExperimentPreparationReservation:
        state.reservations.append(value)
        return value

    store = SimpleNamespace(
        policy=lambda: policy,
        preparation=lambda *args: None,
        preparation_reservation=lambda *args: state.reservation,
        reserve_preparation=reserve,
    )
    inputs = tmp_path / "inputs"
    inputs.mkdir(mode=0o700)
    preparer = commands.ExperimentFamilyPreparer(
        store=store,
        definitions=object(),
        profiles=(),
        phase_provider=lambda *args: pytest.fail("native cannot use daily provider"),
        native_profiles=(profile,),
        native_phase_provider=provider,
        metadata_store_factory=lambda: nullcontext(None),
        catalog=object(),
        lake_root=tmp_path / "lake",
        input_root=inputs,
        clock=clock,
        max_task_seconds=10,
    )

    def publish(value: MinuteSourceContentSeed, **kwargs: object) -> SimpleNamespace:
        state.published.append(value)
        audit, snapshot = minute_metadata_identities(value)
        frozen = value.freeze(
            audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id
        )
        return SimpleNamespace(reference=object(), receipt=SimpleNamespace(frozen=frozen))

    def plan(frozen: object, published: object, **kwargs: object) -> None:
        assert frozen is published.receipt.frozen
        state.plan_calls.append(kwargs)
        raise _PlanBoundaryReachedError

    monkeypatch.setattr(commands, "publish_minute_input", publish)
    monkeypatch.setattr(commands, "MinuteReplayCatalog", lambda **kwargs: object())
    monkeypatch.setattr(commands, "build_minute_plan", plan)
    state.preparer, state.provider, state.publish = preparer, provider, publish
    return state


def test_native_provider_publication_between_start_and_return_uses_actual_plan_clock(
    preparation_time: SimpleNamespace,
) -> None:
    state = preparation_time
    with pytest.raises(_PlanBoundaryReachedError):
        state.preparer._prepare_native(state.record, grant=None)
    assert state.plan_calls[0]["now"] == state.start + timedelta(seconds=3)
    assert state.published[0].provenance.published_at == state.published_at
    assert state.reservations[0].created_at == state.published_at
    assert state.plan_calls[0]["deadline"] == state.start + timedelta(seconds=10)


@pytest.mark.parametrize("case", ("future", "before_registration"))
def test_native_unavailable_publication_times_still_refuse_before_publication(
    preparation_time: SimpleNamespace, case: str
) -> None:
    state = preparation_time
    state.published_at = (
        state.start + timedelta(seconds=5)
        if case == "future"
        else state.record.registered_at - timedelta(seconds=1)
    )
    with pytest.raises(PermissionError, match="native publication time"):
        state.preparer._prepare_native(state.record, grant=None)
    assert not state.published and not state.reservations and not state.plan_calls


def test_native_plan_clock_does_not_extend_original_preparation_deadline(
    preparation_time: SimpleNamespace,
) -> None:
    state = preparation_time
    state.ticks[-1] = state.start + timedelta(seconds=11)
    with pytest.raises(_PlanBoundaryReachedError):
        state.preparer._prepare_native(state.record, grant=None)
    assert state.plan_calls[0]["now"] > state.plan_calls[0]["deadline"]
    assert state.plan_calls[0]["deadline"] == state.start + timedelta(seconds=10)


@pytest.mark.parametrize("corrupt", ("input_hash", "created_at"))
def test_native_existing_reservation_cannot_rebind(
    preparation_time: SimpleNamespace, corrupt: str
) -> None:
    state = preparation_time
    cfg = state.record.actual_configurations[0]
    read = commands.NativeMinutePhaseRead(
        owner=state.record.owner,
        family_id=state.record.family_id,
        source_identity=state.profile.source_identity,
        source_key=cfg.source_key,
        source_version=cfg.source_version,
        phase="search",
        window=DateRange(start_date=cfg.start_date, end_date=cfg.end_date),
        outer_grant_id=None,
        index=0,
        configuration=cfg,
    )
    seed = state.provider(read)
    audit, snapshot = minute_metadata_identities(seed)
    frozen = seed.freeze(audit_run_id=audit.audit_run_id, dataset_snapshot_id=snapshot.snapshot_id)
    state.reads.clear()
    state.reservation = ExperimentPreparationReservation(
        owner=state.record.owner,
        family_id=state.record.family_id,
        index=0,
        source_identity=state.profile.source_identity,
        source_path=str(state.preparer.input_root / ("a" * 32) / "input.duckdb"),
        input_hash="0" * 64 if corrupt == "input_hash" else frozen.full_input_hash,
        created_at=state.published_at - timedelta(seconds=1)
        if corrupt == "created_at"
        else state.published_at,
    )
    with pytest.raises(PermissionError, match="reservation cannot be rebound"):
        state.preparer._prepare_native(state.record, grant=None)
    assert not state.published and not state.reservations and not state.plan_calls


def test_native_wrong_source_remains_refused_before_time_acceptance(
    preparation_time: SimpleNamespace,
) -> None:
    state = preparation_time
    state.source_override = "another-native-source"
    with pytest.raises(PermissionError, match="another owner/source/profile/interval"):
        state.preparer._prepare_native(state.record, grant=None)
    assert not state.published and not state.reservations and not state.plan_calls
