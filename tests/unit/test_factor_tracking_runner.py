"""Synthetic sealed generations exercise the original worker and atomic tracking cursor."""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.factor.tracking import FactorTrackingRequest, FactorTrackingStore

_AT = datetime(2026, 9, 1, tzinfo=UTC)


def _sources(tmp_path: Path) -> tuple[Path, object, object, object]:
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.expression import FeatureCatalog
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.registry import FactorDefinitionRegistry, SaveFactorDefinitionRequest
    from tests.unit.test_factor_member_archive import _private
    from tests.unit.test_factor_source_prepare import _replica

    replica = _replica(tmp_path, count=10, days=36)
    root = _private(tmp_path / "config")
    for name in ("members", "inputs", "artifacts"):
        _private(tmp_path / name)
    registry = FactorDefinitionRegistry(tmp_path / "registry.sqlite")
    registry_identity = registry.initialize()
    definition = build_factor_definition(
        factor_id="tracked",
        name_zh="合成跟踪",
        category="technical",
        direction="lower_is_better",
        version=1,
        earliest_available_date=None,
        expression="ts_mean(close, 3)",
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    registry.save(
        SaveFactorDefinitionRequest(command_id="seed", definition=definition, expected_head=None),
        expected_identity=registry_identity,
    )
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AT)
    ledger_identity = ledger.initialize()
    return replica, root, registry_identity, ledger_identity


def _generation(
    tmp_path: Path, sources: tuple[Path, object, object, object], *, days: int
) -> object:
    from rquant.factor.member_archive import (
        FactorMemberArchiveRequest,
        publish_factor_member_archive,
    )
    from rquant.factor.run_configuration import (
        FactorRunConfiguration,
        FactorRunMemberBinding,
        save_factor_prepared_source,
        save_factor_run_configuration,
    )
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.factor.universe import DailySecurityBatch, DailySecurityFact
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_factor_member_archive import _write
    from tests.unit.test_factor_source_prepare import _request

    replica, root, registry_identity, ledger_identity = sources
    from tests.unit.test_factor_source_prepare import _FIRST

    request = _request(replica, count=10, days=4)
    request = request.model_copy(
        update={
            "scope": request.scope.model_copy(
                update={"as_of_time": _AT, "end_date": _FIRST + timedelta(days=days - 1)}
            )
        }
    )
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        source = prepare_factor_stream_source(
            request, metadata_store=metadata, lake_root=tmp_path / "lake", now=lambda: _AT
        )
    codes = source.admission_request.scope.stock_codes
    for i, day in enumerate(source.receipt.calendar_open_days):
        batch = DailySecurityBatch(
            trade_date=day,
            source_id="synthetic-securities",
            source_sha256="a" * 64,
            source_mode="historical_retrospective",
            security_scope="china_a_share",
            observed_at=_AT - timedelta(hours=1),
            complete_stock_codes=codes,
            facts=tuple(
                DailySecurityFact(
                    stock_code=code,
                    exchange="SZ",
                    board="main",
                    is_listed=True,
                    is_st=i == 12 and j == 0,
                )
                for j, code in enumerate(codes)
            ),
        )
        _write(
            tmp_path / "inputs",
            f"day-{i}.json",
            {
                "schema_version": 1,
                "trade_date": day.isoformat(),
                "securities": batch.model_dump(mode="json"),
                "membership": None,
            },
        )
    archive = publish_factor_member_archive(
        FactorMemberArchiveRequest(
            selection="all",
            trading_days=source.receipt.calendar_open_days,
            as_of=_AT,
            computation_stock_codes=codes,
        ),
        input_root=tmp_path / "inputs",
        daily_filenames=(f"day-{i}.json" for i in range(days)),
        root=tmp_path / "members",
    )
    return save_factor_run_configuration(
        root,
        FactorRunConfiguration(
            enabled=True,
            factor_run_users=("alice",),
            registry_identity=registry_identity,
            ledger_identity=ledger_identity,
            prepared_source=save_factor_prepared_source(root, source),
            lake_root=tmp_path / "lake",
            member_root=tmp_path / "members",
            artifact_root=tmp_path / "artifacts",
            members=(FactorRunMemberBinding(selection="all", archive=archive),),
            code_revision="b" * 40,
        ),
    )


def _joined(tmp_path: Path, registry_identity: object) -> tuple[FactorTrackingStore, object]:
    from rquant.factor.registry import FactorDefinitionRegistry, FactorHeadRef

    record = FactorDefinitionRegistry(Path(registry_identity.path)).get_head(
        "tracked", expected_identity=registry_identity
    )
    store = FactorTrackingStore(tmp_path / "tracking.sqlite", clock=lambda: _AT)
    identity = store.initialize()
    store.set_tracked(
        FactorTrackingRequest(
            command_id=str(uuid4()),
            requested_at=_AT,
            serving_generation_id="c" * 64,
            factor_id="tracked",
            tracked=True,
            expected_head=FactorHeadRef(version=1, content_sha256=record.content_sha256),
        ),
        actor_id="alice",
        expected_identity=identity,
        registry_identity=registry_identity,
    )
    return store, identity


def test_tracking_incremental_ts_dynamic_members_matches_whole_source(tmp_path: Path) -> None:
    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    first = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = FactorTrackingRunner(sources[1], first, identity, clock=lambda: _AT)
    baseline = runner.run_history("tracked", target_end=_FIRST + timedelta(days=20))
    assert baseline.status == "updated" and len(baseline.evaluation_days) == 18
    old_days = store.days("tracked", expected_identity=identity)
    extended = _generation(tmp_path, sources, days=34)
    runner.reference = extended
    appended = runner.run_history("tracked", target_end=_FIRST + timedelta(days=31))
    assert appended.evaluation_days == tuple(_FIRST + timedelta(days=i) for i in range(21, 32))
    assert store.days("tracked", expected_identity=identity)[:18] == old_days
    all_days = store.days("tracked", expected_identity=identity)
    assert all_days[9].expected_count == 9 and all_days[0].expected_count == 10
    # Independent daily golden: increasing close/return ranks, reversed definition direction.
    for day in all_days:
        index = (day.trade_date - _FIRST).days
        ordered = list(range(10, 0, -1))
        if index == 12:
            ordered.remove(1)
        size, extra = divmod(len(ordered), 5)
        low = ordered[: size + bool(extra)]
        high = ordered[-size:]
        assert day.rank_ic.value == pytest.approx(-1.0)
        assert day.low_return == pytest.approx(
            sum((10.0 + code / 100.0 + index) / 10.0 - 1.0 for code in low) / len(low)
        )
        assert day.high_return == pytest.approx(
            sum((10.0 + code / 100.0 + index) / 10.0 - 1.0 for code in high) / len(high)
        )
    # New segment against the same sealed generation: the original worker recomputes the full range.
    state = store.get("tracked", expected_identity=identity)
    store.set_tracked(
        FactorTrackingRequest(
            command_id=str(uuid4()),
            requested_at=_AT,
            serving_generation_id="c" * 64,
            factor_id="tracked",
            tracked=False,
            expected_head=state.head,
            expected_tracking_generation=state.generation,
        ),
        actor_id="alice",
        expected_identity=identity,
        registry_identity=sources[2],
    )
    state = store.get("tracked", expected_identity=identity)
    store.set_tracked(
        FactorTrackingRequest(
            command_id=str(uuid4()),
            requested_at=_AT,
            serving_generation_id="c" * 64,
            factor_id="tracked",
            tracked=True,
            expected_head=state.head,
            expected_tracking_generation=state.generation,
        ),
        actor_id="alice",
        expected_identity=identity,
        registry_identity=sources[2],
    )
    whole = runner.run_history("tracked", target_end=_FIRST + timedelta(days=31))
    assert (
        whole.status == "updated" and store.days("tracked", expected_identity=identity) == all_days
    )
    duplicate = runner.run_history("tracked", target_end=_FIRST + timedelta(days=31))
    assert duplicate.status == "waiting" and not duplicate.evaluation_days
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))


def test_tracking_source_revision_pauses_without_moving_cursor(tmp_path: Path) -> None:
    import duckdb

    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_source_prepare import _FIRST, _sidecar

    sources = _sources(tmp_path)
    first = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = FactorTrackingRunner(sources[1], first, identity, clock=lambda: _AT)
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=20)).status == "updated"
    previous = store.days("tracked", expected_identity=identity)
    with duckdb.connect(str(sources[0])) as connection:
        connection.execute(
            "UPDATE daily_bar SET close=99. WHERE ts_code='000001.SZ' AND trade_date=?", [_FIRST]
        )
    _sidecar(sources[0])
    runner.reference = _generation(tmp_path, sources, days=34)
    outcome = runner.run_history("tracked", target_end=_FIRST + timedelta(days=31))
    assert (
        outcome.status == "paused" and store.days("tracked", expected_identity=identity) == previous
    )
    assert store.get("tracked", expected_identity=identity).status == "paused"


def test_tracking_completion_tail_failure_commits_no_contribution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    monkeypatch.setattr(
        module,
        "verify_factor_stream_artifacts",
        lambda *a, **k: (_ for _ in ()).throw(OSError("synthetic tail")),
    )
    outcome = module.FactorTrackingRunner(
        sources[1], reference, identity, clock=lambda: _AT
    ).run_history("tracked", target_end=_FIRST + timedelta(days=20))
    assert (
        outcome.status == "waiting"
        and store.get("tracked", expected_identity=identity).cursor is None
    )
    assert not store.days("tracked", expected_identity=identity)
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))


def test_tracking_pending_original_job_recovers_after_reference_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = module.FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
    worker = module.run_one_factor_job
    monkeypatch.setattr(
        module,
        "run_one_factor_job",
        lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt("after durable ledger submit")),
    )
    with pytest.raises(KeyboardInterrupt):
        runner.run_history("tracked", target_end=_FIRST + timedelta(days=20))
    assert not store.days("tracked", expected_identity=identity)
    with store._connection(identity) as connection:
        original = module._last_run(
            store, connection, store.get("tracked", expected_identity=identity).segment_id
        )
    runner.reference = reference.model_copy(
        update={"sha256": "0" * 64, "filename": "factor-run-configuration-v1-" + "0" * 64 + ".json"}
    )
    monkeypatch.setattr(module, "run_one_factor_job", worker)
    restored = runner.run_history("tracked", target_end=_FIRST + timedelta(days=30))
    assert restored.status == "updated" and restored.run_id == original.run_id
    assert store.get("tracked", expected_identity=identity).cursor == _FIRST + timedelta(days=20)


def test_tracking_cancel_rejoin_during_worker_fences_old_contribution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = module.FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
    worker = module.run_one_factor_job

    def crossed(*args: object, **kwargs: object) -> object:
        result = worker(*args, **kwargs)
        for tracked in (False, True):
            state = store.get("tracked", expected_identity=identity)
            store.set_tracked(
                FactorTrackingRequest(
                    command_id=str(uuid4()),
                    requested_at=_AT,
                    serving_generation_id="c" * 64,
                    factor_id="tracked",
                    tracked=tracked,
                    expected_head=state.head,
                    expected_tracking_generation=state.generation,
                ),
                actor_id="alice",
                expected_identity=identity,
                registry_identity=sources[2],
            )
        return result

    monkeypatch.setattr(module, "run_one_factor_job", crossed)
    original = store.get("tracked", expected_identity=identity).generation
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=20)).status == "paused"
    state = store.get("tracked", expected_identity=identity)
    assert (
        state.tracked
        and state.generation != original
        and state.cursor is None
        and state.status == "waiting"
    )
    assert not store.days("tracked", expected_identity=identity)


def test_tracking_verified_source_tail_change_rolls_back_atomic_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = module.FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
    commit = runner._commit

    def changed(
        state: object, run: object, verified: object, witness: object, ledger: object, job_id: str
    ) -> None:
        # Mutation at the final verified input boundary, after the worker and final prefix read.
        path = next(path for path, _ in witness.files if path.suffix == ".parquet")
        path.write_bytes(path.read_bytes() + b"changed")
        commit(state, run, verified, witness, ledger, job_id)

    monkeypatch.setattr(runner, "_commit", changed)
    outcome = runner.run_history("tracked", target_end=_FIRST + timedelta(days=20))
    assert (
        outcome.status == "waiting"
        and store.get("tracked", expected_identity=identity).cursor is None
    )
    assert not store.days("tracked", expected_identity=identity)


def test_tracking_interruption_inside_contribution_transaction_recovers_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
    save = runner.store._save_state

    def interrupted(connection: object, state: object) -> None:
        if state.cursor is not None:
            raise KeyboardInterrupt("after all contribution INSERTs, before cursor")
        save(connection, state)

    monkeypatch.setattr(runner.store, "_save_state", interrupted)
    with pytest.raises(KeyboardInterrupt):
        runner.run_history("tracked", target_end=_FIRST + timedelta(days=20))
    assert not store.days("tracked", expected_identity=identity)
    assert store.get("tracked", expected_identity=identity).cursor is None
    monkeypatch.setattr(runner.store, "_save_state", save)
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=20)).status == "updated"
    assert len(store.days("tracked", expected_identity=identity)) == 18
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=20)).status == "waiting"


def test_tracking_definition_dsl_context_runs_with_fixed_post_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.definition import build_factor_definition
    from rquant.factor.expression import FeatureCatalog
    from rquant.factor.registry import (
        FactorDefinitionRegistry,
        FactorHeadRef,
        SaveFactorDefinitionRequest,
    )
    from rquant.factor.tracking_runner import FactorTrackingRunner, _last_run
    from tests.unit.test_factor_neutralization_jobs import _ready
    from tests.unit.test_factor_source_prepare import _FIRST

    root, reference, request, _, config, _, _ = _ready(tmp_path, monkeypatch)
    registry = FactorDefinitionRegistry(Path(config.registry_identity.path))
    definition = build_factor_definition(
        factor_id="context_tracked",
        name_zh="有行业算子",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression="industry_neutralize(close)",
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    receipt = registry.save(
        SaveFactorDefinitionRequest(
            command_id="context-seed", definition=definition, expected_head=None
        ),
        expected_identity=config.registry_identity,
    )
    store = FactorTrackingStore(tmp_path / "tracking.sqlite")
    identity = store.initialize()
    store.set_tracked(
        FactorTrackingRequest(
            command_id=str(uuid4()),
            requested_at=request.requested_at,
            serving_generation_id="c" * 64,
            factor_id=definition.factor_id,
            tracked=True,
            expected_head=FactorHeadRef(version=1, content_sha256=receipt.content_sha256),
        ),
        actor_id="alice",
        expected_identity=identity,
        registry_identity=config.registry_identity,
    )
    result = FactorTrackingRunner(
        root, reference, identity, clock=lambda: request.requested_at
    ).run_history(definition.factor_id, target_end=_FIRST + timedelta(days=19))
    assert result.status == "updated", result
    state = store.get(definition.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        run = _last_run(store, connection, state.segment_id)
    assert run.plan.spec.adapter_request.formula.neutralization == "none"
    assert run.plan.spec.adapter_request.context.industry is not None
    assert run.plan.spec.adapter_request.context.market_cap is None
    assert all(
        day.valid_count == 3 for day in store.days(definition.factor_id, expected_identity=identity)
    )
    assert not list(config.lake_root.glob(".execution_sessions/*"))


def test_tracking_concurrent_reservation_uses_one_original_job(tmp_path: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
    state = store.get("tracked", expected_identity=identity)
    candidates = tuple(runner._prepare(state, _FIRST + timedelta(days=20)) for _ in range(2))
    assert candidates[0].run_id != candidates[1].run_id
    with ThreadPoolExecutor(
        max_workers=2, thread_name_prefix="synthetic-tracking-reservation"
    ) as workers:
        results = tuple(workers.map(lambda proposed: runner._reserve(state, proposed), candidates))
    assert results[0] == results[1]
    with store._connection(identity) as connection:
        assert connection.execute("SELECT count(*) FROM tracking_runs").fetchone()[0] == 1
    outcome = runner.run_history("tracked", target_end=_FIRST + timedelta(days=20))
    assert outcome.status == "updated" and outcome.run_id == results[0].run_id
    assert len(store.days("tracked", expected_identity=identity)) == 18


def test_tracking_natural_configuration_tail_finishes_before_cursor_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import contextmanager

    import rquant.factor.tracking_runner as module
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    verified = False
    original_verify = module.verify_factor_stream_artifacts
    original_open = module.open_factor_run_configuration

    def verify(*args: object, **kwargs: object) -> object:
        nonlocal verified
        result = original_verify(*args, **kwargs)
        verified = True
        return result

    @contextmanager
    def source(*args: object, **kwargs: object):
        with original_open(*args, **kwargs) as loaded:
            yield loaded
        if verified:
            raise OSError("synthetic final natural configuration tail")

    monkeypatch.setattr(module, "verify_factor_stream_artifacts", verify)
    monkeypatch.setattr(module, "open_factor_run_configuration", source)
    result = module.FactorTrackingRunner(
        sources[1], reference, identity, clock=lambda: _AT
    ).run_history("tracked", target_end=_FIRST + timedelta(days=20))
    assert (
        result.status == "waiting"
        and store.get("tracked", expected_identity=identity).cursor is None
    )
    assert not store.days("tracked", expected_identity=identity)


@pytest.mark.parametrize("boundary", ("reserve", "commit"))
def test_tracking_paused_revision_fences_inflight_old_runner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    import duckdb

    import rquant.factor.tracking_runner as module
    from rquant.factor.tracking import FactorTrackingConflict
    from tests.unit.test_factor_source_prepare import _FIRST, _sidecar

    sources = _sources(tmp_path)
    first = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    baseline_runner = module.FactorTrackingRunner(sources[1], first, identity, clock=lambda: _AT)
    assert (
        baseline_runner.run_history("tracked", target_end=_FIRST + timedelta(days=20)).status
        == "updated"
    )
    before = store.get("tracked", expected_identity=identity)
    old_days = store.days("tracked", expected_identity=identity)
    clean = _generation(tmp_path, sources, days=34)
    with duckdb.connect(str(sources[0])) as connection:
        connection.execute(
            "UPDATE daily_bar SET close=99. WHERE ts_code='000001.SZ' AND trade_date=?", [_FIRST]
        )
    _sidecar(sources[0])
    revised = _generation(tmp_path, sources, days=34)
    old = module.FactorTrackingRunner(sources[1], clean, identity, clock=lambda: _AT)
    new = module.FactorTrackingRunner(sources[1], revised, identity, clock=lambda: _AT)
    prepare, commit = old._prepare, old._commit

    def prepared(state: object, target: object) -> object:
        proposed = prepare(state, target)
        assert proposed is not None
        if boundary == "reserve":
            assert new.run_history("tracked", target_end=target).status == "paused"
        else:
            with pytest.raises(FactorTrackingConflict):
                new._prepare(state, target)
        return proposed

    def committing(*args: object) -> None:
        assert new._message(before, paused=True, reason="历史因果输入已修订。").status == "paused"
        commit(*args)

    monkeypatch.setattr(old, "_prepare", prepared)
    if boundary == "commit":
        monkeypatch.setattr(old, "_commit", committing)
    outcome = old.run_history("tracked", target_end=_FIRST + timedelta(days=31))
    current = store.get("tracked", expected_identity=identity)
    assert outcome.status == current.status == "paused"
    assert current.cursor == before.cursor and current.generation == before.generation
    assert store.days("tracked", expected_identity=identity) == old_days
    reason = current.reason
    assert old._message(before, paused=False, reason="旧运行等待。").status == "paused"
    assert store.get("tracked", expected_identity=identity).reason == reason
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))


def test_tracking_missing_middle_contribution_rejects_projection_and_atomic_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sqlite3

    from rquant.factor.tracking import FactorTrackingIntegrityError
    from rquant.factor.tracking_runner import FactorTrackingRunner
    from rquant.factor.tracking_serving import project_factor_tracking_snapshot
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=34)
    store, identity = _joined(tmp_path, sources[2])
    runner = FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=31)).status == "updated"
    before = store.get("tracked", expected_identity=identity)
    days = store.days("tracked", expected_identity=identity)
    assert len(days) == 29 and days[-2].trade_date not in (before.start_date, before.cursor)
    commit = runner._commit

    def missing(*args: object) -> None:
        with sqlite3.connect(store.path) as connection:
            connection.execute(
                "DELETE FROM tracking_days WHERE segment_id=? AND trade_date=?",
                (before.segment_id, days[-2].trade_date.isoformat()),
            )
        commit(*args)

    monkeypatch.setattr(runner, "_commit", missing)
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=32)).status == "waiting"
    current = store.get("tracked", expected_identity=identity)
    assert current.cursor == before.cursor and current.start_date == before.start_date
    with pytest.raises(FactorTrackingIntegrityError):
        store.days("tracked", expected_identity=identity)
    with pytest.raises(FactorTrackingIntegrityError):
        project_factor_tracking_snapshot(identity, registry_identity=sources[2], available_at=_AT)
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=32)).status == "waiting"
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT count(*) FROM tracking_days").fetchone()[0] == 28
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))


def test_tracking_real_registry_inode_tail_rolls_back_and_retains_head_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import shutil
    import sqlite3

    from rquant.factor.tracking_runner import FactorTrackingRunner
    from tests.unit.test_factor_source_prepare import _FIRST

    sources = _sources(tmp_path)
    reference = _generation(tmp_path, sources, days=23)
    store, identity = _joined(tmp_path, sources[2])
    runner = FactorTrackingRunner(sources[1], reference, identity, clock=lambda: _AT)
    registry_path = Path(sources[2].path)
    original_path = registry_path.with_name("registry-original.sqlite")
    save, check = runner.store._save_state, runner.store._check
    replaced, recovering, lock_checked = False, False, False

    def replace(connection: object, state: object) -> None:
        nonlocal replaced
        save(connection, state)
        if state.cursor is not None and not replaced:
            registry_path.rename(original_path)
            shutil.copy2(original_path, registry_path)
            assert registry_path.read_bytes() == original_path.read_bytes()
            assert registry_path.stat().st_ino != original_path.stat().st_ino
            replaced = True

    def checked(connection: object, expected: object) -> None:
        nonlocal lock_checked
        check(connection, expected)
        current = runner.store._state(connection, "tracked")
        if recovering and current.cursor is not None:
            writer = sqlite3.connect(
                f"{registry_path.as_uri()}?mode=rw", uri=True, timeout=0, isolation_level=None
            )
            try:
                writer.execute("BEGIN IMMEDIATE")
                writer.execute("UPDATE factor_heads SET archived=1 WHERE factor_id='tracked'")
                with pytest.raises(sqlite3.OperationalError, match="locked"):
                    writer.execute("COMMIT")
                lock_checked = True
            finally:
                if writer.in_transaction:
                    writer.execute("ROLLBACK")
                writer.close()

    monkeypatch.setattr(runner.store, "_save_state", replace)
    monkeypatch.setattr(runner.store, "_check", checked)
    try:
        outcome = runner.run_history("tracked", target_end=_FIRST + timedelta(days=20))
        assert replaced and outcome.status == "waiting"
        assert store.get("tracked", expected_identity=identity).cursor is None
        assert not store.days("tracked", expected_identity=identity)
    finally:
        if replaced:
            registry_path.unlink()
            original_path.rename(registry_path)
    recovering = True
    assert runner.run_history("tracked", target_end=_FIRST + timedelta(days=20)).status == "updated"
    assert lock_checked and len(store.days("tracked", expected_identity=identity)) == 18
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))
