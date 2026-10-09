from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime, timedelta
from importlib import import_module, util
from pathlib import Path
from types import ModuleType
from uuid import uuid4

import pandas as pd
import pytest

from rquant.experiment_registry import DateRange
from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalReplayResult
from rquant.minute_backtest_performance import build_minute_performance


def study_execution() -> ModuleType:
    assert util.find_spec("rquant.minute_backtest_parameter_study_execution") is not None
    return import_module("rquant.minute_backtest_parameter_study_execution")


def test_carrier_execution_uses_original_preparation_and_complete_result_entry_points() -> None:
    api = study_execution()
    for name in (
        "MinuteParameterStudyExecutionRequest",
        "build_minute_parameter_study_execution",
        "prepare_minute_parameter_study_trial",
        "submit_minute_parameter_study_trial",
        "read_minute_parameter_study_execution",
        "MinuteParameterStudyExecutionEffect",
    ):
        assert hasattr(api, name), f"source-bound study execution entry is missing: {name}"


@pytest.fixture(scope="module")
def carrier_owner(tmp_path_factory: pytest.TempPathFactory) -> object:
    from types import SimpleNamespace

    from rquant.metadata_catalog import ImmutableDuckDBMetadataCatalog
    from rquant.minute_backtest_commands import MinuteCommandWriter
    from rquant.minute_backtest_installation import load_minute_replay_installation
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterFactSourceReference,
        MinuteParameterReplayCatalog,
        publish_minute_parameter_input,
    )
    from rquant.minute_backtest_parameters import MinuteGrowthParameters, MinuteParameterSet
    from rquant.research_catalog import ResearchCatalog
    from rquant.storage.duckdb import DuckDBStore
    from tests.support.minute_backtest_installed import installed_minute, private_directory
    from tests.support.minute_parameter_formal_fixture import parameter_source_seed

    original = installed_minute.__wrapped__(tmp_path_factory)
    installation = next(original)
    references, baselines = [], {}
    # The original raw blueprint freezes its last completed reference on July 30.
    days = (date(2026, 7, 31), date(2026, 8, 3), date(2026, 8, 4))
    for family in ("n_shape", "growth_board_surge"):
        root = private_directory(installation.root / ("study-facts-" + family))
        recipe = (
            None if family == "n_shape" else MinuteParameterSet(parameters=MinuteGrowthParameters())
        )
        seed = parameter_source_seed(
            root / "originals", parameters=recipe, days=days, sparse=True, study_facts=True
        )
        body = seed.model_dump(mode="python")
        body["runtime"]["source_key"] = "synthetic.study." + family
        seed = type(seed).model_validate(body)
        with DuckDBStore(root / "metadata.duckdb") as metadata:
            metadata.path.chmod(0o600)
            published = publish_minute_parameter_input(
                seed,
                metadata_store=metadata,
                source_path=root / "source.duckdb",
                receipt_path=root / "receipt.json",
                catalog=ResearchCatalog(root / "catalog.duckdb"),
                lake_root=installation.profile.research_lake_root,
                installed_policies=(seed.provenance.visibility_policy,),
                now=seed.provenance.published_at,
            )
        with ImmutableDuckDBMetadataCatalog.open(
            root / "metadata.duckdb", snapshot_root=installation.profile.snapshot_root
        ) as metadata:
            descriptor = metadata.descriptor
        reference = MinuteParameterFactSourceReference(
            **published.reference.model_dump(mode="python"),
            full_input_hash=published.receipt.frozen.full_input_hash,
            metadata_identity=descriptor,
            display_name="明确合成的三日研究事实",
            source_nature="synthetic_validation",
            supported_parameter_names=(
                "paper.stop_loss_pct",
                "require_vwap_strength",
                "use_same_minute_surge",
                "use_accel_surge",
            ),
        )
        references.append(reference)
        baselines[family] = published
    catalog = MinuteParameterReplayCatalog(
        fact_sources=tuple(references),
        prepared_root=private_directory(
            installation.profile.runtime_root / "minute-parameter-prepared"
        ),
        snapshot_root=installation.profile.snapshot_root,
        research_lake_root=installation.profile.research_lake_root,
        forbidden_paths=installation.profile.forbidden_paths,
        installed_policies=tuple(
            dict.fromkeys(
                value.receipt.frozen.provenance.visibility_policy for value in baselines.values()
            )
        ),
    )
    profile = installation.profile.model_copy(update={"parameter_catalog": catalog})
    path = installation.root / "installed-study-parameters.json"
    path.write_text(profile.model_dump_json(exclude_computed_fields=True))
    path.chmod(0o600)
    installation.now[0] = max(
        value.receipt.frozen.provenance.published_at for value in baselines.values()
    ) + timedelta(seconds=1)
    installed = load_minute_replay_installation(
        path, expected_code_sha=profile.code_sha, writable=True, clock=lambda: installation.now[0]
    )
    writer = MinuteCommandWriter(installed)
    try:
        yield SimpleNamespace(
            original=installation,
            installed=installed,
            writer=writer,
            catalog=catalog,
            baselines=baselines,
            days=days,
        )
    finally:
        writer.close()
        original.close()


def carrier_request(carrier_owner: object, **updates: object) -> object:
    value = carrier_owner.baselines["n_shape"].receipt.frozen
    body = dict(
        request_id=uuid4(),
        owner_id=value.runtime.owner_id,
        source_key=value.runtime.source_key,
        source_version=value.runtime.source_version,
        full_input_hash=value.full_input_hash,
        parameters=value.runtime.parameters,
        formal_protocol=dict(
            train_range=window(carrier_owner.days[0], carrier_owner.days[0]),
            validation_range=window(carrier_owner.days[1], carrier_owner.days[1]),
            frozen_outer_test_range=window(carrier_owner.days[2], carrier_owner.days[2]),
        ),
        settings=({"score_profile": "v1", "top_n": 1, "min_trades": 1},),
        random_seed=17,
        requested_at=carrier_owner.installed.clock(),
        deadline=carrier_owner.installed.clock() + timedelta(hours=1),
        mode="single",
    )
    body.update(updates)
    return study_execution().MinuteParameterStudyExecutionRequest.model_validate(body)


def carrier_plan(carrier_owner: object, request: object) -> object:
    return study_execution().build_minute_parameter_study_execution(
        request, catalog=carrier_owner.catalog, as_of=carrier_owner.installed.clock()
    )


def test_carrier_grid_preserves_complete_recipes_and_actual_selection_controls(
    carrier_owner: object,
) -> None:
    from rquant.minute_backtest_commands import minute_job_id
    from rquant.minute_backtest_parameter_search import MinuteParameterSearchRequest

    request = carrier_request(carrier_owner)
    search = MinuteParameterSearchRequest(
        base=request.parameters,
        axes=({"path": "paper.stop_loss_pct", "values": (0.01, 0.02)},),
        mode="grid",
        seed=17,
    )
    request = carrier_request(
        carrier_owner,
        request_id=request.request_id,
        mode="grid",
        search=search,
        settings=(
            {"score_profile": "v1", "top_n": 1, "min_trades": 1},
            {"score_profile": "accumulation_heavy", "top_n": 2, "min_trades": 3},
        ),
    )
    plan = carrier_plan(carrier_owner, request)
    assert plan.state == "ready" and plan.trial_count == 4
    assert len({trial.command.command_id for trial in plan.trials}) == 4
    assert [
        trial.command.config.parameters.parameters.paper.stop_loss_pct for trial in plan.trials
    ] == [0.01, 0.01, 0.02, 0.02]
    assert [trial.command.config.study.score_profile for trial in plan.trials] == [
        "v1",
        "accumulation_heavy",
        "v1",
        "accumulation_heavy",
    ]
    for trial in plan.trials:
        config = trial.command.config
        assert config.random_seed == 17 and config.protocol == request.formal_protocol
        assert (
            config.parameters.parameters.volume_profile
            == request.parameters.parameters.volume_profile
        )
        assert trial.job_id == minute_job_id(request.owner_id, trial.command.command_id)
    assert carrier_plan(carrier_owner, request) == plan
    from rquant.minute_backtest_parameter_study_commands import SubmitMinuteParameterStudy

    command = SubmitMinuteParameterStudy(
        command_id=str(request.request_id),
        actor_id=request.owner_id,
        requested_at=request.requested_at,
        request=request,
    )
    effect = study_execution().MinuteParameterStudyExecutionEffect(
        command=command, plan=plan, prepared=()
    )
    restored = type(effect).model_validate_json(
        effect.model_dump_json(exclude_computed_fields=True)
    )
    assert restored == effect and restored.plan.plan_id == plan.plan_id
    body = effect.model_dump(mode="python", exclude_computed_fields=True)
    body["command"]["request"]["settings"] = ({"score_profile": "v1", "top_n": 3, "min_trades": 1},)
    with pytest.raises(ValueError, match="complete original request"):
        type(effect).model_validate(body)


def test_carrier_random_uses_the_frozen_search_owner_seed_without_duplicates(
    carrier_owner: object,
) -> None:
    from rquant.minute_backtest_parameter_search import (
        MinuteParameterSearchRequest,
        build_minute_parameter_search_plan,
    )

    request = carrier_request(carrier_owner)
    search = MinuteParameterSearchRequest(
        base=request.parameters,
        axes=({"path": "paper.stop_loss_pct", "values": (0.01, 0.02, 0.03)},),
        mode="random",
        seed=17,
        requested_trials=2,
    )
    request = carrier_request(carrier_owner, mode="random", search=search)
    actual = carrier_plan(carrier_owner, request)
    expected = build_minute_parameter_search_plan(search)
    assert [trial.command.config.parameters for trial in actual.trials] == list(expected.trials)
    assert actual.trial_count == 2 and actual.random_seed == 17


def test_carrier_five_ablation_recipes_come_from_the_original_owner(carrier_owner: object) -> None:
    from rquant.minute_backtest_parameter_ablation import growth_board_parameter_ablation

    original = carrier_owner.baselines["growth_board_surge"].receipt.frozen
    request = carrier_request(
        carrier_owner,
        mode="ablation",
        parameters=original.runtime.parameters,
        source_key=original.runtime.source_key,
        source_version=original.runtime.source_version,
        full_input_hash=original.full_input_hash,
    )
    expected = growth_board_parameter_ablation(request.parameters)
    actual = carrier_plan(carrier_owner, request)
    assert actual.trial_count == 5
    assert [
        (trial.variant_key, trial.label, trial.command.config.parameters) for trial in actual.trials
    ] == [(item.key, item.label, item.parameters) for item in expected]


def test_carrier_three_part_windows_use_owned_calendar_and_keep_incomplete_six_unavailable(
    carrier_owner: object,
) -> None:
    from rquant.topn_walk_forward import build_expanding_folds

    request = carrier_request(
        carrier_owner,
        mode="walk_forward",
        walk_forward={"fold_count": 1, "min_training_dates": 1, "validation_date_count": 1},
    )
    plan = carrier_plan(carrier_owner, request)
    expected = build_expanding_folds(list(carrier_owner.days), fold_count=1, min_train_dates=2)
    assert plan.trial_count == 1
    for trial, fold in zip(plan.trials, expected, strict=True):
        formal = trial.command.config.protocol
        assert formal.train_range == window(fold.train_dates[0], fold.train_dates[-2])
        assert formal.validation_range == window(fold.train_dates[-1], fold.train_dates[-1])
        assert formal.frozen_outer_test_range == window(fold.test_dates[0], fold.test_dates[-1])
    unavailable = carrier_plan(
        carrier_owner,
        carrier_request(
            carrier_owner,
            mode="walk_forward",
            walk_forward={"fold_count": 6, "min_training_dates": 2, "validation_date_count": 1},
        ),
    )
    assert unavailable.state == "unavailable" and unavailable.trial_count == 0
    assert unavailable.unavailable_reasons == ("insufficient_fold_dates",)


def test_carrier_prepare_binds_real_source_head_config_hash_and_original_uuid(
    carrier_owner: object,
) -> None:
    from rquant.runtime_contracts import canonical_sha256

    api = study_execution()
    plan = carrier_plan(
        carrier_owner,
        carrier_request(
            carrier_owner,
            settings=(
                {"score_profile": "v1", "top_n": 1, "min_trades": 1},
                {"score_profile": "accumulation_heavy", "top_n": 2, "min_trades": 1},
            ),
        ),
    )
    prepared = api.prepare_minute_parameter_study_trial(
        plan, trial_index=0, writer=carrier_owner.writer
    )
    trial = plan.trials[0]
    assert prepared.trial == trial and prepared.plan_id == plan.plan_id
    assert prepared.binding.protocol.source == plan.baseline_source
    assert prepared.binding.request_hash == canonical_sha256(
        trial.command.config.model_dump(mode="json")
    )
    assert prepared.binding.request_hash != plan.plan_id
    assert prepared.binding.protocol.parameters == trial.command.config.parameters
    assert prepared.marker.command.job_id == trial.job_id
    assert prepared.full_input_hash != plan.baseline_source.full_input_hash
    assert prepared.binding.protocol.head.registration_fingerprint != "0" * 64
    again = api.prepare_minute_parameter_study_trial(
        plan, trial_index=0, writer=carrier_owner.writer
    )
    assert again == prepared
    # An original submitted command is not a physical success or a study score.
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader

    reader = MinuteParameterSealedReplayReader(
        reader=carrier_owner.installed.reader,
        artifact_reader=ArtifactPreviewReader(
            reader=carrier_owner.installed.reader,
            artifact_root=carrier_owner.installed.profile.final_artifact_root,
        ),
        submission_facade=carrier_owner.installed.commands,
        catalog=carrier_owner.catalog,
    )
    unsubmitted = api.read_minute_parameter_study_execution(
        plan, prepared=(prepared,), reader=reader, as_of=carrier_owner.installed.clock()
    )
    assert [item.state for item in unsubmitted.trial_states] == [
        "awaiting_submission_receipt",
        "not_prepared",
    ]
    first = api.submit_minute_parameter_study_trial(prepared, writer=carrier_owner.writer)
    second = api.submit_minute_parameter_study_trial(prepared, writer=carrier_owner.writer)
    assert first == second and first.result == "submitted" and first.job_id == trial.job_id
    assert len(carrier_owner.writer.installation.commands.spool.pending()) == 1
    pending = api.read_minute_parameter_study_execution(
        plan, prepared=(prepared,), reader=reader, as_of=carrier_owner.installed.clock()
    )
    assert pending.state == "pending" and pending.training_ranks == ()
    assert pending.missing_trial_indices == (0, 1) and pending.results == ()
    assert [(item.index, item.job_id, item.state) for item in pending.trial_states] == [
        (0, trial.job_id, "pending"),
        (1, plan.trials[1].job_id, "not_prepared"),
    ]


@pytest.mark.parametrize(
    "fault", ["source", "owner", "frequency", "search_base", "seed", "zero_min_trades"]
)
def test_carrier_rejects_unknown_or_inconsistent_original_inputs(
    carrier_owner: object, fault: str
) -> None:
    from rquant.minute_backtest_parameter_search import MinuteParameterSearchRequest

    request = carrier_request(carrier_owner)
    body = request.model_dump(mode="python")
    if fault == "source":
        body["full_input_hash"] = "0" * 64
    elif fault == "owner":
        body["owner_id"] = "different-owner"
    elif fault == "frequency":
        recipe = request.parameters.model_dump(mode="python")
        recipe["parameters"]["freq"] = "5min"
        body["parameters"] = recipe
    elif fault in {"search_base", "seed"}:
        changed = request.parameters.model_dump(mode="python")
        changed["parameters"]["paper"]["stop_loss_pct"] = 0.03
        body.update(
            mode="grid",
            search=MinuteParameterSearchRequest(
                base=changed if fault == "search_base" else request.parameters,
                axes=({"path": "paper.stop_loss_pct", "values": (0.01, 0.02)},),
                mode="grid",
                seed=18 if fault == "seed" else 17,
            ),
        )
    else:
        body["settings"] = ({"score_profile": "v1", "top_n": 1, "min_trades": 0},)
    with pytest.raises((ValueError, PermissionError)):
        carrier_plan(
            carrier_owner,
            study_execution().MinuteParameterStudyExecutionRequest.model_validate(body),
        )


def test_carrier_trial_budget_is_checked_before_generating_a_multiplied_grid(
    carrier_owner: object,
) -> None:
    from rquant.minute_backtest_parameter_search import MinuteParameterSearchRequest

    request = carrier_request(carrier_owner)
    search = MinuteParameterSearchRequest(
        base=request.parameters,
        mode="grid",
        seed=17,
        axes=({"path": "paper.stop_loss_pct", "values": tuple(i / 1000 for i in range(1, 101))},),
    )
    request = carrier_request(
        carrier_owner,
        mode="grid",
        search=search,
        settings=tuple({"score_profile": "v1", "top_n": 1, "min_trades": i} for i in range(1, 202)),
    )
    with pytest.raises(ValueError, match="20,000"):
        carrier_plan(carrier_owner, request)


def test_carrier_changed_recipe_cannot_reuse_an_original_prepared_marker(
    carrier_owner: object,
) -> None:
    api = study_execution()
    plan = carrier_plan(carrier_owner, carrier_request(carrier_owner))
    original = api.prepare_minute_parameter_study_trial(
        plan, trial_index=0, writer=carrier_owner.writer
    )
    changed = original.model_dump(mode="python")
    changed["trial"]["command"]["config"]["study"]["top_n"] = 2
    with pytest.raises((ValueError, PermissionError)):
        api.MinuteParameterPreparedStudyTrial.model_validate(changed)
    changed = original.model_dump(mode="python")
    changed["marker"]["config_hash"] = "0" * 64
    with pytest.raises(ValueError, match="config hash"):
        api.MinuteParameterPreparedStudyTrial.model_validate(changed)


def test_carrier_archived_actual_seal_keeps_validation_trade_out_of_training() -> None:
    import json

    from rquant.minute_backtest_commands import MinuteRunEffect, SubmitMinuteReplay
    from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayResult
    from rquant.minute_backtest_parameter_optimizer import (
        MinuteStudyTrainingObservation,
        rank_minute_study_training,
    )

    root = Path(__file__).resolve().parents[2]
    archive = (
        root
        / "data/verification/minute-engine-completion-20261007/"
        "legacy-parameters-implementation-06/study-joint-freeze-17/evidence"
    )
    payload = (archive / "parameter-sealed-full.json").read_bytes()
    assert (
        hashlib.sha256(payload).hexdigest()
        == "f937c2e45fc1fee4984ef298c1c09c0c2e074cc17cac2cdbc1771516cccd5a54"
    )
    control = (archive / "parameter-sealed-control.json").read_bytes()
    assert (
        hashlib.sha256(control).hexdigest()
        == "5fafd9d2c5a4e3f9c17c3f6843c700fc3768ff2b3d5ca3a819e10feb572cdc1d"
    )
    command = SubmitMinuteReplay.model_validate_json(json.dumps(json.loads(control)["command"]))
    sealed = MinuteParameterSealedReplayResult.model_validate_json(payload)
    effect_bytes = (archive / "minute-run-effect.json").read_bytes()
    assert hashlib.sha256(effect_bytes).hexdigest() == (
        "2f313c88331bcf1e9d3650ee270e9e53e417db71a5612c90acf54d9e020fa314"
    )
    marker = MinuteRunEffect.model_validate_json(effect_bytes)
    records_bytes = (archive / "page-control-accepted-records.json").read_bytes()
    assert hashlib.sha256(records_bytes).hexdigest() == (
        "d7b123a06c991d361d227bf0218b7065c98dafa9037a58d4b2a6c52944e6606d"
    )
    records = json.loads(records_bytes)
    assert records["command"]["status"] == records["effect"]["status"] == "succeeded"
    assert json.loads(records["command"]["payload_json"]) == command.model_dump(mode="json")
    assert records["effect"]["original_admission_json"].encode("utf-8") == effect_bytes
    assert (
        records["command"]["command_hash"]
        == records["effect"]["command_hash"]
        == (marker.command_hash)
    )
    assert marker.command.spec == sealed.accepted_spec and marker.command.job_id == sealed.job_id
    binding = sealed.result.replay.study_binding
    assert binding is not None and binding == sealed.result.publication.frozen.runtime.study_binding
    binding.verify_request(command.config)
    assert marker.config_hash == binding.request_hash
    assert sealed.job_id == study_execution().minute_job_id(command.actor_id, command.command_id)
    projected = study_execution().project_minute_parameter_study_sealed_windows(
        sealed, as_of=command.requested_at
    )
    assert [
        projected.training.window,
        projected.validation.window,
        projected.independent_test.window,
    ] == [binding.train_range, binding.validation_range, binding.frozen_outer_test_range]
    assert projected.training.summary.trades == 0
    assert projected.training.cross_window_trades == 1
    assert projected.validation.cross_window_trades == 1
    assert len(sealed.result.replay.signals) == 2 and len(sealed.result.replay.fills) == 2
    fact = MinuteStudyTrainingObservation(
        study_id=binding.study_id,
        source=binding.protocol.source,
        head=binding.protocol.head,
        parameter_fingerprint=binding.protocol.parameters.fingerprint,
        train_start=binding.train_range.start_date,
        train_end=binding.train_range.end_date,
        result_hash=sealed.result_hash,
        available_at=projected.read_at,
        summary=projected.training.summary,
    )
    assert (
        rank_minute_study_training((binding.protocol,), (fact,), selection_cutoff=projected.read_at)
        == ()
    )
    with pytest.raises(ValueError, match="predate"):
        study_execution().project_minute_parameter_study_sealed_windows(
            sealed, as_of=sealed.completed_at - timedelta(microseconds=1)
        )


def three_part_request(
    original_result: MinuteParameterFormalReplayResult, **updates: object
) -> object:
    from rquant.minute_backtest_study_protocols import MinuteStudyProtocol

    parameters = original_result.replay.parameters
    # Pure calendar-planning inputs, never a claim of installed source/head authority.
    template = MinuteStudyProtocol(
        source={
            "source_key": "synthetic.study.planning",
            "source_version": 1,
            "owner_id": "fixture-owner",
            "full_input_hash": "a" * 64,
            "dataset_snapshot_id": "synthetic.study.planning",
            "frequency": parameters.parameters.freq,
            "start_date": date(2026, 7, 1),
            "end_date": date(2026, 8, 3),
            "published_at": datetime(2026, 10, 7, 8, tzinfo=UTC),
        },
        head={
            "definition_id": parameters.definition_id,
            "definition_version": parameters.definition_version,
            "evaluator_semantic_version": parameters.evaluator_semantic_version,
            "parameter_fingerprint": parameters.fingerprint,
            "registration_fingerprint": "b" * 64,
            "spec_fingerprint": "c" * 64,
            "executable_fingerprint": "d" * 64,
            "producer_commit": "a" * 40,
        },
        parameters=parameters,
        split={
            "train_start": date(2026, 7, 1),
            "train_end": date(2026, 7, 15),
            "test_start": date(2026, 7, 16),
            "test_end": date(2026, 8, 3),
        },
        score_profile="v1",
        top_n=2,
        min_trades=5,
        random_seed=17,
        requested_at=datetime(2026, 10, 7, 9, tzinfo=UTC),
    )
    body = {
        "templates": (template,),
        "calendar_source": template.source,
        "calendar_dates": tuple(
            day for i in range(30) if (day := date(2026, 7, 1) + timedelta(days=i)).weekday() < 5
        ),
        "calendar_complete": True,
        "fold_count": 6,
        "min_training_dates": 5,
        "validation_date_count": 2,
    }
    body.update(updates)
    api = study_execution()
    assert hasattr(api, "MinuteParameterThreePartWalkForwardRequest"), (
        "explicit three-part walk-forward request is missing"
    )
    return api.MinuteParameterThreePartWalkForwardRequest.model_validate(body)


@pytest.fixture(scope="module")
def original_result() -> MinuteParameterFormalReplayResult:
    # Actual original adapter wire. It is deliberately not a claimed physical seal.
    path = (
        Path(__file__).resolve().parents[2]
        / "data/verification/minute-engine-completion-20261007"
        / "parameter-reader-implementation-ai-11/green-05-wire-evidence/formal-result.json"
    )
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == (
        "0b49a8c831bb08b63caf1cbfc3ef0671e971881ac851c453e3bae2e8af20cdfa"
    )
    return MinuteParameterFormalReplayResult.model_validate_json(data)


def window(start: date = date(2026, 7, 31), end: date = date(2026, 8, 3)) -> DateRange:
    return DateRange(start_date=start, end_date=end)


def test_complete_original_fees_fifo_and_summary_owner_supply_the_observation(
    original_result: MinuteParameterFormalReplayResult, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import strategy_compare

    complete = build_minute_performance(
        original_result.replay, runtime=original_result.publication.frozen.runtime
    )
    assert complete.metrics is not None and len(complete.metrics.round_trips) == 1
    trip = complete.metrics.round_trips[0]
    # Financial returns and rounding come from their original owners.
    expected = strategy_compare._summary_row(
        "first_break",
        "baseline",
        pd.DataFrame([{"ret_pct": trip.return_rate * 100, "exit_reason": "gap_stop"}]),
        0,
    )
    owner = strategy_compare._summary_row
    observed: list[pd.DataFrame] = []

    def original_summary(*args: object) -> dict[str, object]:
        observed.append(args[2].copy())
        return owner(*args)

    monkeypatch.setattr(strategy_compare, "_summary_row", original_summary)
    value = study_execution().project_minute_parameter_study_window(
        original_result, window=window()
    )
    assert value.status == "complete" and value.summary is not None
    assert len(observed) == 1
    assert observed[0]["ret_pct"].tolist() == [trip.return_rate * 100]
    assert value.summary.model_dump() == {
        key: expected[key] for key in type(value.summary).model_fields
    }
    assert value.summary.trades == 1 and value.summary.gap_stop_rate_pct == 100.0
    assert value.full_input_hash == original_result.full_input_hash
    assert value.parameter_hash == original_result.replay.parameters.fingerprint
    assert value.profile_hash == original_result.replay.profile_hash
    assert value.daily == complete.daily
    assert value.cross_window_trades == 0 and value.unavailable_reasons == ()
    assert not hasattr(value, "study_id")  # This projection cannot certify actual entry selection.


@pytest.mark.parametrize("day", [date(2026, 7, 31), date(2026, 8, 3)])
def test_a_cross_partition_trade_cannot_use_a_later_exit_for_training(
    original_result: MinuteParameterFormalReplayResult, day: date
) -> None:
    value = study_execution().project_minute_parameter_study_window(
        original_result, window=window(day, day)
    )
    assert value.status == "complete" and value.summary.trades == 0
    assert value.summary.mean_ret_pct is None and value.summary.gap_stop_rate_pct is None
    assert value.cross_window_trades == 1
    assert [point.trade_date for point in value.daily] == [day]


def test_three_part_uses_the_original_date_owner_with_an_explicit_validation_tail(
    original_result: MinuteParameterFormalReplayResult, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import topn_walk_forward

    request = three_part_request(original_result)
    owner = topn_walk_forward.build_expanding_folds
    expected = owner(list(request.calendar_dates), fold_count=6, min_train_dates=7)
    calls = []

    def original_owner(dates: list[date], *, fold_count: int, min_train_dates: int) -> object:
        calls.append((dates, fold_count, min_train_dates))
        return owner(dates, fold_count=fold_count, min_train_dates=min_train_dates)

    monkeypatch.setattr(topn_walk_forward, "build_expanding_folds", original_owner)
    api = study_execution()
    plan = api.build_minute_parameter_three_part_walk_forward(request)
    assert calls == [(list(request.calendar_dates), 6, 7)]
    assert plan.state == "ready" and plan.results_state == "pending"
    assert len(plan.folds) == 6 and plan.owner_min_train_dates == 7
    for fold, original in zip(plan.folds, expected, strict=True):
        assert fold.fold == original.fold
        assert fold.train_dates + fold.validation_dates == tuple(original.train_dates)
        assert fold.validation_dates == tuple(original.train_dates[-2:])
        assert fold.test_dates == tuple(original.test_dates)
        assert len(fold.train_dates) >= 5
        assert fold.train_dates[-1] < fold.validation_dates[0] < fold.test_dates[0]
        assert fold.train_range == window(fold.train_dates[0], fold.train_dates[-1])
        assert fold.validation_range == window(fold.validation_dates[0], fold.validation_dates[-1])
        assert fold.out_of_sample_range == window(fold.test_dates[0], fold.test_dates[-1])
        assert fold.formal_protocol.train_range == fold.train_range
        assert fold.formal_protocol.validation_range == fold.validation_range
        assert fold.formal_protocol.frozen_outer_test_range == fold.out_of_sample_range
        protocol = fold.protocols[0]
        assert protocol.source == request.templates[0].source
        assert protocol.head == request.templates[0].head
        assert protocol.parameters == request.templates[0].parameters
        assert protocol.worker_seed == 17 and protocol.top_n == 2
        assert protocol.score_profile == "v1" and protocol.min_trades == 5
        assert protocol.split.train_end == fold.train_dates[-1]
        assert protocol.split.test_start == fold.test_dates[0]
        with pytest.raises(ValueError, match="outside"):
            protocol.split.partition(fold.validation_dates[0])
    round_trip = api.MinuteParameterThreePartWalkForwardPlan.model_validate_json(
        plan.model_dump_json()
    )
    assert round_trip == plan and round_trip.plan_id == plan.plan_id


def test_three_part_calendar_gaps_do_not_fabricate_the_requested_fold_count(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    api = study_execution()
    request = three_part_request(original_result)
    short = request.model_dump(mode="python")
    short["calendar_dates"] = request.calendar_dates[:9]
    plan = api.build_minute_parameter_three_part_walk_forward(
        api.MinuteParameterThreePartWalkForwardRequest.model_validate(short)
    )
    assert plan.state == "unavailable" and len(plan.folds) == 2
    assert plan.unavailable_reasons == ("insufficient_fold_dates",)
    unknown = api.build_minute_parameter_three_part_walk_forward(
        three_part_request(original_result, calendar_complete=False)
    )
    assert unknown.state == "unavailable" and unknown.folds == ()
    assert unknown.unavailable_reasons == ("incomplete_calendar",)


@pytest.mark.parametrize(
    "changes",
    [
        {"validation_date_count": 0},
        {"validation_date_count": True},
        {"validation_date_count": 1.0},
        {"min_training_dates": None},
        {"min_training_dates": True},
        {"future_test_score": 999},
    ],
)
def test_three_part_counts_and_unregistered_inputs_are_explicit_and_strict(
    original_result: MinuteParameterFormalReplayResult, changes: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        three_part_request(original_result, **changes)


def test_three_part_requires_both_counts_and_binds_them_to_the_plan_identity(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    api = study_execution()
    request = three_part_request(original_result)
    for key in ("validation_date_count", "min_training_dates"):
        body = request.model_dump(mode="python")
        del body[key]
        with pytest.raises(ValueError):
            api.MinuteParameterThreePartWalkForwardRequest.model_validate(body)
    original = api.build_minute_parameter_three_part_walk_forward(request)
    changed = api.build_minute_parameter_three_part_walk_forward(
        three_part_request(original_result, validation_date_count=3)
    )
    assert changed.plan_id != original.plan_id


def test_three_part_work_capacity_is_checked_before_any_fold_generation(
    original_result: MinuteParameterFormalReplayResult, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant import topn_walk_forward
    from rquant.minute_backtest_contracts import MAX_DATE_SPAN

    original = three_part_request(original_result).templates[0]
    templates = tuple(
        type(original).model_validate({**original.model_dump(mode="python"), "top_n": count})
        for count in range(1, 13)
    )

    def forbidden_generation(*args: object, **kwargs: object) -> None:
        pytest.fail("the original work limit must be checked before generating folds")

    monkeypatch.setattr(topn_walk_forward, "build_expanding_folds", forbidden_generation)
    with pytest.raises(ValueError, match="work budget"):
        request = three_part_request(original_result, templates=templates, fold_count=MAX_DATE_SPAN)
        study_execution().build_minute_parameter_three_part_walk_forward(request)


def test_three_part_ranking_accepts_only_the_actual_training_prefix(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    from rquant.minute_backtest_parameter_optimizer import (
        MinuteStudyTrainingObservation,
        rank_minute_study_training,
    )

    api = study_execution()
    plan = api.build_minute_parameter_three_part_walk_forward(three_part_request(original_result))
    fold = plan.folds[0]
    protocol = fold.protocols[0]
    observation = MinuteStudyTrainingObservation(
        study_id=protocol.study_id,
        source=protocol.source,
        head=protocol.head,
        parameter_fingerprint=protocol.parameters.fingerprint,
        train_start=fold.train_dates[0],
        train_end=fold.train_dates[-1],
        result_hash="e" * 64,
        available_at=protocol.requested_at,
        summary={
            "trades": 8,
            "mean_ret_pct": 1.0,
            "win_rate_pct": 60.0,
            "worst_ret_pct": -2.0,
            "gap_stop_rate_pct": 0.0,
        },
    )
    actual = api.select_minute_parameter_three_part_training(
        plan, fold=1, observations=(observation,), selection_cutoff=protocol.requested_at
    )
    expected = rank_minute_study_training(
        fold.protocols, (observation,), selection_cutoff=protocol.requested_at
    )
    assert actual == expected
    for interval in (fold.validation_range, fold.out_of_sample_range):
        changed = observation.model_copy(
            update={"train_start": interval.start_date, "train_end": interval.end_date}
        )
        with pytest.raises(ValueError, match="window|training"):
            api.select_minute_parameter_three_part_training(
                plan, fold=1, observations=(changed,), selection_cutoff=protocol.requested_at
            )


def test_fold_execution_bindings_use_c6_owner_with_the_entire_plan_identity(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    from rquant.minute_backtest_parameter_study import MinuteParameterStudyBinding

    api = study_execution()
    plan = api.build_minute_parameter_three_part_walk_forward(three_part_request(original_result))
    assert hasattr(api, "bind_minute_parameter_three_part_walk_forward")
    groups = api.bind_minute_parameter_three_part_walk_forward(plan)
    assert len(groups) == 6
    for fold, bindings in zip(plan.folds, groups, strict=True):
        assert len(bindings) == len(fold.protocols)
        for protocol, bound in zip(fold.protocols, bindings, strict=True):
            assert bound == MinuteParameterStudyBinding.from_formal_protocol(
                protocol=protocol, formal_protocol=fold.formal_protocol, request_hash=plan.plan_id
            )
            assert bound.study_id == protocol.study_id
            assert bound.partition(fold.train_dates[-1]) == "training"
            assert bound.partition(fold.validation_dates[-1]) == "validation"
            assert bound.partition(fold.test_dates[-1]) == "out_of_sample"
            assert bound.protocol.parameters == protocol.parameters
            assert bound.protocol.source == protocol.source


def test_unavailable_or_forged_fold_plan_cannot_produce_execution_bindings(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    api = study_execution()
    assert hasattr(api, "bind_minute_parameter_three_part_walk_forward")
    plan = api.build_minute_parameter_three_part_walk_forward(
        three_part_request(original_result, calendar_complete=False)
    )
    with pytest.raises(ValueError, match="unavailable"):
        api.bind_minute_parameter_three_part_walk_forward(plan)
    ready = api.build_minute_parameter_three_part_walk_forward(three_part_request(original_result))
    truncated = ready.model_copy(update={"folds": ready.folds[:1]})
    with pytest.raises(ValueError, match="owner|fold"):
        api.bind_minute_parameter_three_part_walk_forward(truncated)


@pytest.mark.parametrize(
    "change",
    ["missing_reason", "reason_conflict", "duplicate_queue", "missing_queue", "foreign_order"],
)
def test_missing_or_ambiguous_actual_sell_identity_stays_unavailable(
    original_result: MinuteParameterFormalReplayResult, change: str
) -> None:
    replay = original_result.replay
    buy, sell = replay.queue_records
    if change in {"missing_reason", "reason_conflict"}:
        evidence = dict(sell.signal.evidence)
        if change == "missing_reason":
            evidence.pop("exit_reason")
        else:
            evidence["exit_reason"] = "time_exit"
        signal = sell.signal.model_copy(update={"evidence": evidence})
        sell = sell.model_copy(update={"signal": signal})
        changed = replay.model_copy(
            update={"queue_records": (buy, sell), "signals": (buy.signal, signal)}
        )
    elif change == "duplicate_queue":
        changed = replay.model_copy(update={"queue_records": (buy, sell, sell)})
    elif change == "missing_queue":
        changed = replay.model_copy(update={"queue_records": (buy,)})
    else:
        sell = sell.model_copy(
            update={"order": sell.order.model_copy(update={"account_id": "other"})}
        )
        changed = replay.model_copy(update={"queue_records": (buy, sell)})
    value = study_execution().project_minute_parameter_study_window(
        original_result.model_copy(update={"replay": changed}), window=window()
    )
    assert value.status == "unavailable" and value.summary is None
    assert "sell_execution_identity_unavailable" in value.unavailable_reasons


def test_unrelated_future_exit_reason_does_not_fill_or_contaminate_training(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    replay = original_result.replay
    buy, sell = replay.queue_records
    evidence = dict(sell.signal.evidence)
    evidence.pop("exit_reason")
    signal = sell.signal.model_copy(update={"evidence": evidence})
    sell = sell.model_copy(update={"signal": signal})
    changed = replay.model_copy(
        update={"queue_records": (buy, sell), "signals": (buy.signal, signal)}
    )
    value = study_execution().project_minute_parameter_study_window(
        original_result.model_copy(update={"replay": changed}),
        window=window(date(2026, 7, 31), date(2026, 7, 31)),
    )
    assert value.status == "complete" and value.summary.trades == 0
    assert value.summary.mean_ret_pct is None


def test_missing_original_nav_cannot_be_a_complete_or_zero_training_observation(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    replay = original_result.replay
    changed = replay.model_copy(
        update={
            "status": "incomplete",
            "daily_status": "unavailable",
            "incomplete_reasons": ("missing_nav",),
        }
    )
    value = study_execution().project_minute_parameter_study_window(
        original_result.model_copy(update={"replay": changed}), window=window()
    )
    assert value.status == "unavailable" and value.summary is None
    assert "daily_nav_unavailable" in value.unavailable_reasons
    assert all(point.daily_return is None for point in value.daily)


def test_a_non_session_window_is_not_an_invented_zero_result(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    value = study_execution().project_minute_parameter_study_window(
        original_result, window=window(date(2026, 8, 1), date(2026, 8, 2))
    )
    assert value.status == "unavailable" and value.summary is None
    assert value.unavailable_reasons == ("window_has_no_original_trading_dates",)


@pytest.mark.parametrize("day", [date(2026, 7, 30), date(2026, 8, 4)])
def test_projection_rejects_a_window_outside_the_actual_frozen_input(
    original_result: MinuteParameterFormalReplayResult, day: date
) -> None:
    with pytest.raises(ValueError, match="window"):
        study_execution().project_minute_parameter_study_window(
            original_result, window=window(day, day)
        )


def test_projection_rejects_a_different_actual_input_instead_of_binding_only_a_hash(
    original_result: MinuteParameterFormalReplayResult,
) -> None:
    changed = original_result.replay.model_copy(update={"profile_hash": "0" * 64})
    with pytest.raises(ValueError, match="source/profile"):
        study_execution().project_minute_parameter_study_window(
            original_result.model_copy(update={"replay": changed}), window=window()
        )


def test_actual_original_reader_does_not_turn_complete_wire_into_a_sealed_study(
    original_result: MinuteParameterFormalReplayResult, tmp_path: Path
) -> None:
    from rquant.definition_registry import ImmutableDefinitionRegistry
    from rquant.experiment_registry import ExperimentRegistry
    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_job_center import LabCommandSubmissionFacade
    from rquant.lab_job_protocol import LabCommandSpool
    from rquant.lab_jobs import LabJobReader, LabJobStore
    from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader
    from rquant.minute_backtest_parameter_definition import minute_parameter_research_registry
    from rquant.minute_backtest_parameter_producer import (
        MinuteParameterPublicationReference,
        MinuteParameterReplayCatalog,
    )
    from rquant.minute_backtest_producer import _secure_private_bytes

    runtime = original_result.publication.frozen.runtime
    store = LabJobStore(tmp_path / "jobs.sqlite3")
    store.initialize()
    reader = LabJobReader(store.path)
    trust = tmp_path / "trust"
    trust.mkdir(mode=0o700)
    # These bytes are deliberately uninstalled and cannot prove a complete source or sealed job.
    source_path, receipt_path = tmp_path / "uninstalled-source", tmp_path / "uninstalled-receipt"
    for path in (source_path, receipt_path):
        path.write_bytes(b"uninstalled complete-wire fixture")
        path.chmod(0o600)
    reference = MinuteParameterPublicationReference(
        source_key=runtime.source_key,
        source_version=runtime.source_version,
        owner_id=runtime.owner_id,
        source=_secure_private_bytes(source_path)[1],
        receipt=_secure_private_bytes(receipt_path)[1],
    )
    catalog = MinuteParameterReplayCatalog(
        entries=(reference,),
        installed_policies=(original_result.publication.frozen.provenance.visibility_policy,),
    )
    facade = LabCommandSubmissionFacade(
        reader=reader,
        spool=LabCommandSpool(tmp_path / "commands"),
        experiment_registry=ExperimentRegistry(
            trust / "registry.sqlite3", managed_trust_root=trust
        ),
        definition_registry=ImmutableDefinitionRegistry(
            tmp_path / "definitions",
            execution_registry=minute_parameter_research_registry(
                runtime.parameters, producer_commit=runtime.producer_commit
            ),
        ),
    )
    sealed_reader = MinuteParameterSealedReplayReader(
        reader=reader,
        artifact_reader=ArtifactPreviewReader(reader=reader, artifact_root=tmp_path / "artifacts"),
        submission_facade=facade,
        catalog=catalog,
    )
    assert (
        study_execution().read_minute_parameter_study_windows(
            sealed_reader,
            job_id=uuid4(),
            owner_id=runtime.owner_id,
            parameters=runtime.parameters,
            as_of=original_result.publication.frozen.provenance.published_at,
        )
        is None
    )
