from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from importlib import import_module, util
from pathlib import Path
from types import FrameType, ModuleType
from uuid import uuid4

import pytest

from rquant.experiment_registry import DateRange
from rquant.minute_backtest_parameter_study_execution import MinuteParameterStudyWindowObservation


def projection_api() -> ModuleType:
    assert util.find_spec('rquant.minute_backtest_parameter_study_projection') is not None, (
        'sealed trials have no authenticated bounded derived-publication/read API'
    )
    return import_module('rquant.minute_backtest_parameter_study_projection')


@pytest.fixture(scope='module')
def installed(tmp_path_factory: pytest.TempPathFactory) -> object:
    from tests.support.minute_backtest_installed import installed_minute

    source = installed_minute.__wrapped__(tmp_path_factory)
    value = next(source)
    try:
        yield value
    finally:
        source.close()


def opened(api: ModuleType, tmp_path: Path, installed: object, *, writable: bool = True) -> object:
    root = tmp_path / 'derived'
    root.mkdir(mode=0o700)
    authority = api.bootstrap_minute_study_projection(installed.readonly, state_root=root)
    return api.load_minute_study_projection(installed.readonly, authority.path,
        expected_sha256=authority.content_sha256, writable=writable)


def certificate(api: ModuleType, *, job_id: object | None = None) -> object:
    binding = api.MinuteStudyProjectionBinding(owner_id='owner', job_id=job_id or uuid4(), shard_id=uuid4(),
        spec_hash='1' * 64, plan_hash='2' * 64, payload_hash='3' * 64, manifest_hash='4' * 64,
        complete_result_hash='5' * 64, result_hash='6' * 64, full_input_hash='7' * 64,
        core_input_hash='8' * 64, seed_hash='9' * 64, profile_hash='a' * 64,
        parameter_hash='b' * 64, study_binding_hash='c' * 64, publication_hash='d' * 64,
        formal_plan_id='e' * 64, completed_at=datetime(2026, 8, 5, tzinfo=UTC))
    windows = tuple(MinuteParameterStudyWindowObservation(full_input_hash=binding.full_input_hash,
        parameter_hash=binding.parameter_hash, profile_hash=binding.profile_hash,
        window=DateRange(start_date=date(2026, 8, day), end_date=date(2026, 8, day)),
        status='unavailable', daily=(), summary=None, cross_window_trades=0,
        unavailable_reasons=('original_daily_nav_missing',)) for day in (1, 2, 3))
    return api.MinuteStudyProjectionCertificate(binding=binding, training=windows[0],
        validation=windows[1], independent_test=windows[2], original_table_bytes=1024,
        complete_wire_bytes=2048, algorithm_fingerprint=api.projection_algorithm_fingerprint())


def test_authenticated_roundtrip_has_no_read_clock_and_cannot_write_from_readonly(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    try:
        runtime._publish_verified(value)
        observed = runtime.read_certificate(value.binding)
        assert observed == value
        assert not {'read_at', 'available_at', 'selection_cutoff', 'trial_set_hash'} & set(observed.model_dump())
        readonly = api.load_minute_study_projection(installed.readonly, runtime.reference.path,
            expected_sha256=runtime.reference.content_sha256)
        with pytest.raises(PermissionError):
            readonly._publish_verified(value)
        readonly.close()
    finally:
        runtime.close()


@pytest.mark.parametrize('field', ('owner_id', 'job_id', 'spec_hash', 'manifest_hash', 'full_input_hash', 'profile_hash', 'study_binding_hash'))
def test_current_binding_mismatch_cannot_reuse_certificate(tmp_path: Path, installed: object, field: str) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    try:
        runtime._publish_verified(value)
        changed = 'other-owner' if field == 'owner_id' else (uuid4() if field == 'job_id' else 'f' * 64)
        requested = value.binding.model_copy(update={field: changed})
        if field == 'job_id':
            assert runtime.read_certificate(requested) is None
        else:
            with pytest.raises(PermissionError):
                runtime.read_certificate(requested)
    finally:
        runtime.close()


def test_payload_and_index_joint_tamper_cannot_self_authorize(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    try:
        runtime._publish_verified(value)
        payload = runtime.payload_path(value.binding.job_id)
        body = json.loads(payload.read_bytes())
        body['training']['unavailable_reasons'] = ['forged']
        payload.chmod(0o600)
        payload.write_text(json.dumps(body))
        payload.chmod(0o400)
        import hashlib
        with sqlite3.connect(runtime.index_path) as db:
            db.execute('UPDATE projection_entry SET payload_sha256=? WHERE job_id=?',
                (hashlib.sha256(payload.read_bytes()).hexdigest(), str(value.binding.job_id)))
        with pytest.raises(PermissionError):
            runtime.read_certificate(value.binding)
    finally:
        runtime.close()


def test_same_key_conflict_is_no_overwrite_and_orphan_is_not_ready(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    try:
        path = runtime.payload_path(value.binding.job_id)
        path.write_bytes(b'unindexed candidate')
        path.chmod(0o400)
        assert runtime.read_certificate(value.binding) is None
        with pytest.raises(PermissionError):
            runtime._publish_verified(value)
        assert path.read_bytes() == b'unindexed candidate'
    finally:
        runtime.close()


def test_shared_result_capacity_is_not_another_cache_pool(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api).model_copy(update={'original_table_bytes': 62_128_104})
    try:
        assert runtime._publish_verified(value) is False
        assert runtime.read_certificate(value.binding) is None
        assert not runtime.payload_path(value.binding.job_id).exists()
    finally:
        runtime.close()


def test_key_and_authority_reference_change_are_rejected(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    try:
        key = runtime.authority.key_reference.path
        key.write_bytes(os.urandom(32))
        with pytest.raises(PermissionError):
            runtime.read_certificate(certificate(api).binding)
    finally:
        runtime.close()


def test_current_window_schema_mutation_cannot_reuse_valid_certificate(tmp_path: Path, installed: object, monkeypatch: pytest.MonkeyPatch) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    try:
        runtime._publish_verified(value)
        monkeypatch.setitem(MinuteParameterStudyWindowObservation.model_config, 'extra', 'allow')
        with pytest.raises(PermissionError):
            runtime.read_certificate(value.binding)
    finally:
        runtime.close()


def test_index_generation_is_authenticated(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    try:
        runtime._publish_verified(value)
        with sqlite3.connect(runtime.index_path) as db:
            db.execute("UPDATE projection_meta SET body_mac=? WHERE id=1", ('0' * 64,))
        with pytest.raises(PermissionError):
            runtime.read_certificate(value.binding)
    finally:
        runtime.close()


def test_index_failure_leaves_unreadable_orphan_and_retry_completes_same_bytes(tmp_path: Path, installed: object, monkeypatch: pytest.MonkeyPatch) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    original = runtime.verify_current
    calls = []

    def fail_before_commit() -> None:
        calls.append(True)
        if runtime.payload_path(value.binding.job_id).exists():
            raise RuntimeError('synthetic crash after atomic file publication')
        original()

    try:
        monkeypatch.setattr(runtime, 'verify_current', fail_before_commit)
        with pytest.raises(RuntimeError, match='after atomic'):
            runtime._publish_verified(value)
        assert runtime.payload_path(value.binding.job_id).exists()
        identity = runtime.payload_path(value.binding.job_id).stat()
        monkeypatch.setattr(runtime, 'verify_current', original)
        assert runtime.read_certificate(value.binding) is None
        assert runtime._publish_verified(value) is True
        assert runtime.read_certificate(value.binding) == value
        assert runtime.payload_path(value.binding.job_id).stat().st_ino == identity.st_ino
        assert tuple(runtime.payload_path(value.binding.job_id).parent.glob('.*.tmp')) == ()
    finally:
        runtime.close()


def test_independent_processes_publish_identical_key_once(tmp_path: Path, installed: object) -> None:
    import multiprocessing

    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    context = multiprocessing.get_context('fork')
    reader, writer = context.Pipe(duplex=False)

    def publish() -> None:
        try:
            writer.send(runtime._publish_verified(value))
        except BaseException as exc:
            writer.send(type(exc).__name__)
        finally:
            writer.close()

    children = [context.Process(target=publish) for _ in range(2)]
    try:
        for child in children:
            child.start()
        writer.close()
        for child in children:
            child.join(timeout=30)
            assert not child.is_alive()
            assert child.exitcode == 0
        assert [reader.recv(), reader.recv()] == [True, True]
        assert runtime.read_certificate(value.binding) == value
        assert runtime.payload_path(value.binding.job_id).stat().st_nlink == 1
        with sqlite3.connect(runtime.index_path) as db:
            assert db.execute('SELECT count(*) FROM projection_entry').fetchone() == (1,)
    finally:
        for child in children:
            if child.is_alive():
                child.terminate()
            child.join()
            child.close()
        reader.close()
        runtime.close()


def test_closed_runtime_cannot_reopen_or_publish(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    runtime.close()
    runtime.close()
    with pytest.raises(RuntimeError):
        runtime.read_certificate(certificate(api).binding)


def test_reconcile_uses_actual_filters_bounded_pages_and_late_membership(tmp_path: Path, installed: object, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace
    from rquant.lab_jobs import JobStatus, LabJobListFilters

    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    assert callable(getattr(runtime, 'reconcile_one', None)), 'durable single-unit reconciliation is missing'
    job_id = uuid4()
    pages = [(), (SimpleNamespace(job_id=job_id),), ()]
    attempted = []

    def list_jobs(*, filters: LabJobListFilters, limit: int, cursor: str | None) -> object:
        assert filters.statuses == (JobStatus.SUCCEEDED,)
        assert limit == 8 and cursor is None
        return SimpleNamespace(items=pages.pop(0), next_cursor=None)

    def materialize(candidate: object, *, as_of: datetime) -> bool:
        attempted.append(candidate)
        return True

    monkeypatch.setattr(installed.readonly.reader, 'list_jobs', list_jobs)
    monkeypatch.setattr(runtime, 'materialize_original', materialize)
    try:
        first = runtime.reconcile_one(as_of=installed.readonly.clock())
        assert first.candidate_job_id is None
        second = runtime.reconcile_one(as_of=installed.readonly.clock())
        assert second.published and second.candidate_job_id == job_id
        runtime.reconcile_one(as_of=installed.readonly.clock())
        assert attempted == [job_id]
    finally:
        runtime.close()


def test_reconcile_failure_is_durable_and_retried_before_cursor_advances(tmp_path: Path, installed: object, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    assert callable(getattr(runtime, 'reconcile_one', None)), 'durable reconciliation is missing'
    job_id = uuid4()
    seen = []

    def page(**kwargs: object) -> object:
        assert not seen
        return SimpleNamespace(items=(SimpleNamespace(job_id=job_id),), next_cursor='valid-original-cursor')

    def fail(candidate: object, *, as_of: datetime) -> bool:
        seen.append(candidate)
        raise PermissionError('source changed')

    monkeypatch.setattr(installed.readonly.reader, 'list_jobs', page)
    monkeypatch.setattr(runtime, 'materialize_original', fail)
    try:
        with pytest.raises(PermissionError, match='source changed'):
            runtime.reconcile_one(as_of=installed.readonly.clock())
        replacement = api.load_minute_study_projection(installed.readonly, runtime.reference.path,
            expected_sha256=runtime.reference.content_sha256, writable=True)
        monkeypatch.setattr(replacement, 'materialize_original', lambda candidate, *, as_of: seen.append(candidate) or True)
        result = replacement.reconcile_one(as_of=installed.readonly.clock())
        assert result.published and seen == [job_id, job_id]
        replacement.close()
    finally:
        runtime.close()


def test_materialization_requires_actual_indexed_authority_before_any_full_reader(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    assert callable(getattr(runtime, 'materialize_original', None)), 'controlled indexed full materializer is missing'
    try:
        assert runtime.materialize_original(uuid4(), as_of=installed.readonly.clock()) is False
        assert tuple(runtime.authority.payload_directory.path.iterdir()) == ()
    finally:
        runtime.close()


@pytest.mark.parametrize('change', ('field_default', 'field_alias', 'projector_defaults', 'table_closure', 'serializer'))
def test_portable_complete_policy_reads_actual_mutable_fields_functions_and_engine(change: str, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import minute_backtest_parameter_runner as tables
    from rquant import minute_backtest_parameter_study_execution as windows

    api = projection_api()
    assert callable(getattr(api, 'projection_complete_semantic_fingerprint', None)), 'complete portable current-result/derivation policy is missing'
    before = api.projection_complete_semantic_fingerprint()
    assert before is not None, 'the original supported result graph needs a proven complete fast path'
    if change.startswith('field_'):
        field = windows.MinuteParameterStudyWindowObservation.model_fields['unavailable_reasons']
        monkeypatch.setattr(field, 'default' if change == 'field_default' else 'alias', ('mutated',) if change == 'field_default' else 'mutated')
    elif change == 'projector_defaults':
        monkeypatch.setattr(windows.project_minute_parameter_study_window, '__kwdefaults__', {'window': {'changed': [1]}})
    elif change == 'table_closure':
        policy = {'changed': [1]}

        def changed(value: object) -> object:
            return policy['changed']

        monkeypatch.setattr(tables, 'minute_parameter_result_tables', changed)
    else:
        monkeypatch.setattr(windows.MinuteParameterStudyWindowObservation, '__pydantic_serializer__', object())
    after = api.projection_complete_semantic_fingerprint()
    assert after is None or after != before


def test_stop_between_payload_and_index_does_not_authorize_or_lose_cleanup(tmp_path: Path, installed: object, monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.runtime_read_interrupt import ReadInterruptedError, request_read_interrupt, reset_read_interrupts

    api = projection_api()
    runtime = opened(api, tmp_path, installed)
    value = certificate(api)
    original = runtime.verify_current

    def stopped_after_original_validation() -> None:
        original()
        if runtime.payload_path(value.binding.job_id).exists():
            request_read_interrupt()

    try:
        monkeypatch.setattr(runtime, 'verify_current', stopped_after_original_validation)
        with pytest.raises(ReadInterruptedError):
            runtime._publish_verified(value)
        monkeypatch.setattr(runtime, 'verify_current', original)
        reset_read_interrupts()
        assert runtime.read_certificate(value.binding) is None
        assert tuple(runtime.payload_path(value.binding.job_id).parent.glob('.*.tmp')) == ()
    finally:
        reset_read_interrupts()
        runtime.close()


def test_read_side_needs_original_prepared_trial_not_a_certificate_dto(tmp_path: Path, installed: object) -> None:
    api = projection_api()
    runtime = opened(api, tmp_path, installed, writable=False)
    assert callable(getattr(runtime, 'read_trial', None)), 'fresh authorized projection trial reading is missing'
    try:
        with pytest.raises((TypeError, ValueError)):
            runtime.read_trial(certificate(api), as_of=installed.readonly.clock())
        assert tuple(runtime.authority.payload_directory.path.iterdir()) == ()
    finally:
        runtime.close()


def _portable_policy_observation() -> dict[str, object]:
    from rquant.research_run_spec import ResearchExperimentIdentity, ResearchRunParameters

    api = projection_api()
    original = api._ProjectionSemanticSnapshot
    engines, prefix = [], []
    targets = {ResearchExperimentIdentity, ResearchRunParameters}

    class PrefixComplete(Exception):
        pass

    class Observe(original):
        def engine(self: object, engine: object, model: object) -> str:
            before = len(self.references)
            value = super().engine(engine, model)
            if model in targets:
                args = engine.__reduce__()[1]
                assert all(item is None or type(item) in (str, int, bool) for item in args[1].values())
                engines.append({'model': model.__qualname__, 'kind': type(engine).__name__,
                    'schema_is_current': args[0] is model.__pydantic_core_schema__,
                    'schema_alias': self.references.get(id(args[0])), 'config': dict(sorted(args[1].items())),
                    'use_prebuilt': args[2], 'aliases_before': before, 'aliases_after': len(self.references), 'sha256': value})
            return value

        def model(self: object, model: object) -> object:
            before = len(self.references)
            value = super().model(model)
            prefix.append({'model': model.__module__ + '.' + model.__qualname__, 'aliases_before': before,
                'aliases_after': len(self.references), 'schema_alias': self.references.get(id(model.__pydantic_core_schema__)), 'sha256': self.digest(value)})
            if model is ResearchRunParameters:
                raise PrefixComplete
            return value

    try:
        api._ProjectionSemanticSnapshot = Observe
        try:
            api._projection_semantic_components(detailed=True)
        except PrefixComplete:
            pass
    finally:
        api._ProjectionSemanticSnapshot = original
    assert len(engines) == 4
    return {'engines': engines, 'prefix': prefix}


def test_complete_policy_is_portable_and_not_a_process_identity_hash() -> None:
    import subprocess
    import sys

    api = projection_api()
    first = api.projection_complete_semantic_fingerprint()
    assert first is not None and first == api.projection_complete_semantic_fingerprint()
    root = Path(__file__).resolve().parents[2]
    observed = subprocess.run([sys.executable, '-B', '-c',
        'import ast,json; from pathlib import Path; import rquant.minute_backtest_parameter_study_projection as api; '
        'tree=ast.parse(Path("tests/unit/test_minute_parameter_study_projection.py").read_text()); '
        'node=next(item for item in tree.body if isinstance(item,ast.FunctionDef) and item.name=="_portable_policy_observation"); '
        'exec(compile(ast.Module(body=[node],type_ignores=[]),"portable-scalar-observer","exec"),globals()); '
        'projection_api=lambda:api; print(json.dumps({"fingerprint":api.projection_complete_semantic_fingerprint(),"algorithm":api.projection_algorithm_fingerprint(),"parts":api._projection_semantic_components(detailed=True),"observation":_portable_policy_observation()}))'],
        cwd=root, env={'PATH': '/usr/bin:/bin', 'PYTHONPATH': str(root / 'src'), 'RQUANT_DISABLE_DOTENV': '1'},
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    child = json.loads(observed.stdout)
    if child['fingerprint'] != first:
        local, remote = dict(api._projection_semantic_components(detailed=True)), dict(child['parts'])
        differences = [name for name in dict.fromkeys((*local, *remote)) if local.get(name) != remote.get(name)]
        print(json.dumps({'parent': _portable_policy_observation(), 'child': child['observation']}))
        assert not differences, differences[:12]
    assert child['fingerprint'] == first
    assert child['algorithm'] == api.projection_algorithm_fingerprint()


def test_complete_policy_keeps_alias_order_type_and_original_finite_units() -> None:
    from rquant.executable_dependencies import ExecutableDependencyError

    api = projection_api()
    shared = []
    first = api._ProjectionSemanticSnapshot().part({'left': shared, 'right': shared})
    assert first != api._ProjectionSemanticSnapshot().part({'left': [], 'right': []})
    assert first != api._ProjectionSemanticSnapshot().part({'right': shared, 'left': shared})
    assert api._ProjectionSemanticSnapshot().part((1,)) != api._ProjectionSemanticSnapshot().part([1])
    sentinel = object()
    assert api._ProjectionSemanticSnapshot().part((sentinel, sentinel)) != api._ProjectionSemanticSnapshot().part((object(), object()))

    class Opaque:
        pass

    with pytest.raises(ExecutableDependencyError, match='opaque'):
        api._ProjectionSemanticSnapshot().part(Opaque())
    with pytest.raises(ExecutableDependencyError, match='node/depth'):
        api._ProjectionSemanticSnapshot().part([0] * 8192)
    nested: object = None
    for _ in range(65):
        nested = (nested,)
    with pytest.raises(ExecutableDependencyError, match='node/depth'):
        api._ProjectionSemanticSnapshot().part(nested)


def test_complete_policy_reads_live_default_factory_and_nested_mutable_closure(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import minute_backtest_parameter_study_execution as windows

    api = projection_api()
    field = windows.MinuteParameterStudyWindowObservation.model_fields['unavailable_reasons']
    policy = {'values': [0]}

    def factory() -> tuple[str, ...]:
        return tuple(str(item) for item in policy['values'])

    monkeypatch.setattr(field, 'default_factory', factory)
    first = api.projection_complete_semantic_fingerprint()
    assert first is not None
    policy['values'][0] = 1
    assert first != api.projection_complete_semantic_fingerprint()


def test_typing_wrapper_interning_is_portable_but_arguments_and_mutable_aliases_are_live(monkeypatch: pytest.MonkeyPatch) -> None:
    from typing import Literal
    from rquant.research_run_spec import ResearchExperimentIdentity

    api = projection_api()
    original = Literal[1, 2]
    independent = type(original)(Literal, (1, 2))
    assert independent is not original
    capture = lambda item: api._ProjectionSemanticSnapshot().part(item)
    assert capture((original, original)) == capture((original, independent))
    assert capture(original) != capture(Literal[1, 3])
    shared = []
    assert capture((shared, shared)) != capture(([], []))
    field = ResearchExperimentIdentity.model_fields['schema_version']
    before = api.projection_complete_semantic_fingerprint()
    assert before is not None
    monkeypatch.setattr(field, 'annotation', Literal[1, 3])
    assert api.projection_complete_semantic_fingerprint() != before


@pytest.fixture(scope='module')
def actual_study(tmp_path_factory: pytest.TempPathFactory) -> object:
    from tests.unit.test_minute_backtest_parameter_study_execution import carrier_owner

    source = carrier_owner.__wrapped__(tmp_path_factory)
    value = next(source)
    try:
        yield value
    finally:
        source.close()


def test_complete_policy_reads_live_performance_helper_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant.perf import core

    api = projection_api()
    before = api.projection_complete_semantic_fingerprint()
    assert before is not None
    monkeypatch.setattr(core, 'TRADING_DAYS_PER_YEAR', core.TRADING_DAYS_PER_YEAR + 1)
    after = api.projection_complete_semantic_fingerprint()
    assert after is None or after != before


def test_aggregate_projection_missing_keeps_original_pending_states_and_complete_request(tmp_path: Path, actual_study: object) -> None:
    from rquant.minute_backtest_parameter_study_execution import prepare_minute_parameter_study_trial, read_minute_parameter_study_execution
    from rquant.web.minute_backtest_service import _InstalledStudyReplayReader
    from tests.unit.test_minute_backtest_parameter_study_execution import carrier_plan, carrier_request

    api = projection_api()
    plan = carrier_plan(actual_study, carrier_request(actual_study))
    prepared = prepare_minute_parameter_study_trial(plan, trial_index=0, writer=actual_study.writer)
    reader = _InstalledStudyReplayReader(actual_study.installed, prepared.marker.command.spec)
    now = actual_study.installed.clock()
    original = read_minute_parameter_study_execution(plan, prepared=(prepared,), reader=reader, as_of=now)
    root = tmp_path / 'actual-independent-projection'
    root.mkdir(mode=0o700)
    authority = api.bootstrap_minute_study_projection(actual_study.installed, state_root=root)
    with api.load_minute_study_projection(actual_study.installed, authority.path, expected_sha256=authority.content_sha256) as projection:
        observed = read_minute_parameter_study_execution(plan, prepared=(prepared,), reader=reader, as_of=now, projection=projection)
    assert observed == original
    assert observed.results == () and observed.missing_trial_indices == (0,)
    assert observed.trial_states[0].state == 'awaiting_submission_receipt'
    assert tuple((root / 'payloads').iterdir()) == ()


def test_actual_prepared_aggregate_adopts_typed_facts_and_propagates_corrupt_results(tmp_path: Path, actual_study: object, monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import timedelta
    from rquant.minute_backtest_parameter_study_execution import MinuteParameterStudyTrialResult, prepare_minute_parameter_study_trial, read_minute_parameter_study_execution
    from rquant.web.minute_backtest_service import _InstalledStudyReplayReader
    from tests.unit.test_minute_backtest_parameter_study_execution import carrier_plan, carrier_request

    api = projection_api()
    plan = carrier_plan(actual_study, carrier_request(actual_study))
    prepared = prepare_minute_parameter_study_trial(plan, trial_index=0, writer=actual_study.writer)
    reader = _InstalledStudyReplayReader(actual_study.installed, prepared.marker.command.spec)
    now = actual_study.installed.clock()
    root = tmp_path / 'actual-typed-aggregate-projection'
    root.mkdir(mode=0o700)
    authority = api.bootstrap_minute_study_projection(actual_study.installed, state_root=root)
    windows = tuple(MinuteParameterStudyWindowObservation(full_input_hash=prepared.full_input_hash,
        parameter_hash=prepared.binding.protocol.parameters.fingerprint, profile_hash=prepared.profile_hash,
        window=window, status='unavailable', summary=None, daily=(), cross_window_trades=0,
        unavailable_reasons=('original_daily_nav_missing',)) for window in (
            prepared.binding.train_range, prepared.binding.validation_range, prepared.binding.frozen_outer_test_range))
    derived = MinuteParameterStudyTrialResult(prepared=prepared, spec_hash=prepared.marker.command.spec.spec_hash,
        manifest_hash='b' * 64, complete_result_hash='c' * 64, result_hash='d' * 64,
        completed_at=now, read_at=now, training=windows[0], validation=windows[1], independent_test=windows[2])
    # The true preparation/aggregate is exercised; these results isolate its
    # already-proven projection boundary rather than authorizing a physical seal.
    with api.load_minute_study_projection(actual_study.installed, authority.path, expected_sha256=authority.content_sha256) as projection:
        monkeypatch.setattr(projection, 'read_trial', lambda value, *, as_of: derived)
        observed = read_minute_parameter_study_execution(plan, prepared=(prepared,), reader=reader, as_of=now, projection=projection)
        assert observed.plan.plan_id == plan.plan_id and observed.results == (derived,)
        assert observed.missing_trial_indices == () and observed.trial_states[0].state == 'sealed'
        assert observed.training_ranks == () and observed.state == 'unavailable'
        for wrong in (object(), derived.model_copy(update={'read_at': now + timedelta(seconds=1)}),
            derived.model_copy(update={'prepared': prepared.model_copy(update={'plan_id': 'f' * 64})})):
            monkeypatch.setattr(projection, 'read_trial', lambda value, *, as_of: wrong)
            with pytest.raises(PermissionError):
                read_minute_parameter_study_execution(plan, prepared=(prepared,), reader=reader, as_of=now, projection=projection)

        def corrupt(value: object, *, as_of: datetime) -> object:
            raise PermissionError('authenticated projection is corrupt')

        monkeypatch.setattr(projection, 'read_trial', corrupt)
        with pytest.raises(PermissionError, match='authenticated projection is corrupt'):
            read_minute_parameter_study_execution(plan, prepared=(prepared,), reader=reader, as_of=now, projection=projection)
    assert projection._closed and tuple((root / 'payloads').iterdir()) == ()


@pytest.fixture
def sealed_boundary(tmp_path: Path, installed: object, monkeypatch: pytest.MonkeyPatch) -> object:
    """Typed original archived wire with explicit authority/IO seams, never a new seal."""
    import hashlib
    from datetime import timedelta
    from types import SimpleNamespace

    from rquant.lab_artifact_preview import ArtifactPreviewReader
    from rquant.lab_artifacts import LabArtifactFileIdentity
    from rquant.lab_finalizer import LabFinalizerMetrics, LabFinalizerShardSummary, LabFinalizerTableSummary
    from rquant.lab_job_protocol import LabCommandEnvelope
    from rquant.lab_jobs import ShardStatus
    from rquant.minute_backtest_commands import MinuteRunEffect, SubmitMinuteReplay
    from rquant.minute_backtest_parameter_artifact import MinuteParameterSealedReplayReader, MinuteParameterSealedReplayResult
    from rquant.minute_backtest_parameter_study_execution import MinuteParameterPreparedStudyTrial, MinuteParameterStudyTrial, project_minute_parameter_study_sealed_windows

    archive = Path(__file__).resolve().parents[2] / 'data/verification/minute-engine-completion-20261007/legacy-parameters-implementation-06/study-joint-freeze-17/evidence'
    payload = (archive / 'parameter-sealed-full.json').read_bytes()
    assert hashlib.sha256(payload).hexdigest() == 'f937c2e45fc1fee4984ef298c1c09c0c2e074cc17cac2cdbc1771516cccd5a54'
    sealed = MinuteParameterSealedReplayResult.model_validate_json(payload)
    control = (archive / 'parameter-sealed-control.json').read_bytes()
    assert hashlib.sha256(control).hexdigest() == '5fafd9d2c5a4e3f9c17c3f6843c700fc3768ff2b3d5ca3a819e10feb572cdc1d'
    command = SubmitMinuteReplay.model_validate_json(json.dumps(json.loads(control)['command']))
    marker_bytes = (archive / 'minute-run-effect.json').read_bytes()
    assert hashlib.sha256(marker_bytes).hexdigest() == '2f313c88331bcf1e9d3650ee270e9e53e417db71a5612c90acf54d9e020fa314'
    marker = MinuteRunEffect.model_validate_json(marker_bytes)
    binding = sealed.result.replay.study_binding
    assert binding is not None
    prepared = MinuteParameterPreparedStudyTrial(plan_id='a' * 64,
        trial=MinuteParameterStudyTrial(index=0, variant_key='baseline', label='original', fold=1, command=command),
        marker=marker, binding=binding, full_input_hash=sealed.full_input_hash,
        core_input_hash=sealed.core_input_hash, seed_hash=sealed.seed_hash,
        profile_hash=sealed.result.replay.profile_hash, work_units=sealed.result.replay.parameter_work.work_units)
    as_of = max(command.requested_at, sealed.completed_at) + timedelta(seconds=1)
    windows = project_minute_parameter_study_sealed_windows(sealed, as_of=as_of)
    spec, manifest = sealed.accepted_spec, sealed.manifest
    parameters = MinuteParameterSealedReplayReader._parameter_model().model_validate({item.name: item.value for item in spec.parameters.arguments})
    shard = SimpleNamespace(shard_id=sealed.shard_id, shard_index=0, plan_hash=sealed.plan_hash,
        payload_hash=sealed.payload_hash, payload_json='explicit-unit-original-plan',
        adapter_id=manifest.adapter_id, adapter_version=manifest.adapter_version,
        work_units=parameters.work_units, status=ShardStatus.SUCCEEDED)
    authority = SimpleNamespace(job=SimpleNamespace(job_id=sealed.job_id, spec=spec,
        spec_hash=spec.spec_hash, updated_at=sealed.completed_at),
        evidence=SimpleNamespace(indexed_at=sealed.completed_at, manifest_hash=sealed.manifest_hash,
            complete_result_hash=sealed.complete_result_hash))
    metrics = LabFinalizerMetrics(job_id=sealed.job_id, spec_hash=spec.spec_hash,
        plan_hash=sealed.plan_hash, adapter_id=manifest.adapter_id, adapter_version=manifest.adapter_version,
        result_contract_version=manifest.result_contract_version, finalizer_code_sha=spec.code_sha,
        result_hash='c' * 64, shard_count=1,
        shards=(LabFinalizerShardSummary(shard_index=0, shard_id=sealed.shard_id, result_manifest_hash='c' * 64, metrics=()),),
        tables=tuple(LabFinalizerTableSummary(name=item.parquet.table_name, row_count=item.parquet.row_count, columns=item.parquet.columns)
            for item in manifest.files if item.parquet is not None))
    # These byte identities are explicit boundary probes. PFA's real complete-FD
    # tests and the final original four-trial read establish physical byte proof.
    evidence = SimpleNamespace(authority=authority, manifest=manifest, spec=spec,
        metrics=metrics.model_dump(mode='json'), encoded_table_bytes=sum(item.size for item in manifest.files if item.parquet is not None),
        file_identities=tuple(LabArtifactFileIdentity(relative_path=item.relative_path, device=1, inode=i + 10,
            size=item.size, mtime_ns=1, ctime_ns=1) for i, item in enumerate(manifest.files)),
        tables=tuple(item.parquet for item in manifest.files if item.parquet is not None), verified_bundle_bytes=sum(item.size for item in manifest.files))
    envelope = LabCommandEnvelope(request_id=uuid4(), command=marker.command)
    counts = {'full': 0, 'bytes': 0, 'validation': 0, 'elapsed': 0.0}
    state = SimpleNamespace(authority=authority, evidence=evidence, semantic='d' * 64, stop_in_full=False, counts=counts)

    def validate(envelope: object, *, observed_at: datetime) -> None:
        assert envelope.command == marker.command and observed_at >= sealed.completed_at
        counts['validation'] += 1

    facade = SimpleNamespace(reader=installed.readonly.reader,
        experiment_registry=SimpleNamespace(get_submission_intent_for_job=lambda job_id: SimpleNamespace(envelope_json=envelope.model_dump_json()),
            resolve_formal_plan_by_id=lambda plan_id, *, as_of: sealed.formal_plan),
        definition_registry=SimpleNamespace(read_strategy_spec=lambda fingerprint, *, as_of: sealed.result.publication.frozen.native_registration),
        validate_prepared_experiment_submission=validate)
    adapter = SimpleNamespace(parameters=lambda candidate: parameters, expected=lambda candidate: sealed.result.publication,
        adapter_id=manifest.adapter_id, adapter_version=manifest.adapter_version)

    def init(self: object, **kwargs: object) -> None:
        self.reader = kwargs['reader']
        self.submission_facade = kwargs['submission_facade']
        self.artifact_reader = kwargs['artifact_reader']
        self.catalog = kwargs['catalog']

    def full(reader: object, job_id: object, **kwargs: object) -> object:
        assert job_id == sealed.job_id
        counts['full'] += 1
        counts['elapsed'] += 31.0
        if state.stop_in_full:
            from rquant.runtime_read_interrupt import request_read_interrupt
            request_read_interrupt()
        return sealed

    def complete_bytes(reader: object, job_id: object, *, table_names: tuple[str, ...], budget: object) -> object:
        assert job_id == sealed.job_id and len(table_names) == 8 and budget.max_table_count == 8
        counts['bytes'] += 1
        return state.evidence

    api = projection_api()
    monkeypatch.setattr(MinuteParameterSealedReplayReader, '__init__', init)
    monkeypatch.setattr(MinuteParameterSealedReplayReader, 'read', full)
    monkeypatch.setattr(MinuteParameterSealedReplayReader, '_adapter', lambda reader: adapter)
    monkeypatch.setattr(MinuteParameterSealedReplayReader, '_registry', lambda reader: SimpleNamespace(plan=lambda candidate: (shard,)))
    monkeypatch.setattr(ArtifactPreviewReader, 'read_complete_byte_evidence', complete_bytes)
    monkeypatch.setattr(installed.readonly.reader, 'get_artifact_preview_authority', lambda job_id: state.authority)
    monkeypatch.setattr(installed.readonly.reader, 'list_shards', lambda job_id: (shard,))
    monkeypatch.setattr(installed.readonly, 'parameter_submission_facade', lambda candidate: facade)
    monkeypatch.setattr(api, 'projection_complete_semantic_fingerprint', lambda: state.semantic)
    monkeypatch.setattr(api, '_byte_fingerprint', lambda candidate: 'e' * 64 if candidate is evidence else 'f' * 64)
    runtime = opened(api, tmp_path, installed)
    try:
        yield SimpleNamespace(runtime=runtime, sealed=sealed, prepared=prepared, state=state,
            windows=windows, as_of=as_of, shard=shard, authority=authority, evidence=evidence)
    finally:
        from rquant.runtime_read_interrupt import reset_read_interrupts
        reset_read_interrupts()
        runtime.close()


def test_materializer_one_complete_unit_over_soft_observation_is_published(sealed_boundary: object, monkeypatch: pytest.MonkeyPatch) -> None:
    import time

    fixture = sealed_boundary
    monkeypatch.setattr(time, 'monotonic', lambda: fixture.state.counts['elapsed'])
    start = time.monotonic()
    assert fixture.runtime.materialize_original(fixture.sealed.job_id, as_of=fixture.as_of)
    assert time.monotonic() - start > 30 and fixture.state.counts['full'] == 1
    key = fixture.runtime._verified_key()
    with fixture.runtime._connection() as db:
        entry = fixture.runtime._read_entry(db, key, fixture.sealed.job_id)
    observed = fixture.runtime.read_certificate(entry.binding)
    assert (observed.training, observed.validation, observed.independent_test) == (
        fixture.windows.training, fixture.windows.validation, fixture.windows.independent_test)
    assert observed.binding.result_hash == fixture.sealed.result_hash


@pytest.mark.parametrize('change', ('authority', 'stop'))
def test_materializer_fresh_source_and_stop_prevent_commit(sealed_boundary: object, change: str) -> None:
    from types import SimpleNamespace
    from rquant.runtime_read_interrupt import ReadInterruptedError

    fixture = sealed_boundary
    if change == 'authority':
        fixture.state.evidence = SimpleNamespace(**(vars(fixture.evidence) | {'authority': object()}))
        failure = PermissionError
    else:
        fixture.state.stop_in_full = True
        failure = ReadInterruptedError
    with pytest.raises(failure):
        fixture.runtime.materialize_original(fixture.sealed.job_id, as_of=fixture.as_of)
    assert fixture.state.counts['full'] == 1
    assert not tuple(fixture.runtime.authority.payload_directory.path.iterdir())


def test_light_reader_restores_complete_typed_windows_and_fresh_clock_without_full(sealed_boundary: object) -> None:
    from datetime import timedelta

    fixture = sealed_boundary
    assert fixture.runtime.materialize_original(fixture.sealed.job_id, as_of=fixture.as_of)
    later = fixture.as_of + timedelta(seconds=2)
    observed = fixture.runtime.read_trial(fixture.prepared, as_of=later)
    assert observed is not None and observed.prepared == fixture.prepared and observed.read_at == later
    assert (observed.training, observed.validation, observed.independent_test) == (
        fixture.windows.training, fixture.windows.validation, fixture.windows.independent_test)
    assert observed.result_hash == fixture.sealed.result_hash
    assert fixture.state.counts['full'] == 1 and fixture.state.counts['validation'] == 1


@pytest.mark.parametrize('change', ('owner', 'shard', 'metrics', 'bytes', 'semantic'))
def test_light_reader_rejects_fresh_original_changes(sealed_boundary: object, change: str) -> None:
    from types import SimpleNamespace
    from rquant.minute_backtest_artifact import MinuteSealedReplayIntegrityError

    fixture = sealed_boundary
    assert fixture.runtime.materialize_original(fixture.sealed.job_id, as_of=fixture.as_of)
    if change == 'owner':
        body = fixture.prepared.model_dump(mode='python')
        body['trial']['command']['actor_id'] = 'another-owner'
        with pytest.raises(ValueError):
            type(fixture.prepared).model_validate(body)
        return
    if change == 'shard':
        fixture.shard.payload_hash = 'f' * 64
    elif change == 'metrics':
        fixture.state.evidence.metrics['finalizer_code_sha'] = 'f' * 40
    elif change == 'bytes':
        fixture.state.evidence = SimpleNamespace(**vars(fixture.evidence))
    else:
        fixture.state.semantic = 'f' * 64
    with pytest.raises((PermissionError, MinuteSealedReplayIntegrityError)):
        fixture.runtime.read_trial(fixture.prepared, as_of=fixture.as_of)
    assert fixture.state.counts['full'] == 1


def test_light_reader_opaque_proof_falls_back_but_corrupt_index_is_not_absence(sealed_boundary: object) -> None:
    fixture = sealed_boundary
    assert fixture.runtime.materialize_original(fixture.sealed.job_id, as_of=fixture.as_of)
    fixture.state.semantic = None
    assert fixture.runtime.read_trial(fixture.prepared, as_of=fixture.as_of) is None
    with sqlite3.connect(fixture.runtime.index_path) as db:
        db.execute('UPDATE projection_entry SET entry_mac=?', ('0' * 64,))
    with pytest.raises(PermissionError):
        fixture.runtime.read_trial(fixture.prepared, as_of=fixture.as_of)


def test_resolved_read_unit_without_request_owner_keeps_original_path(monkeypatch: pytest.MonkeyPatch) -> None:
    from rquant import minute_backtest_parameter_producer as owner

    factory = getattr(owner, 'resolved_minute_parameter_read_unit', None)
    assert callable(factory), 'the same trial has no lexical verified-source unit'
    catalog = owner.MinuteParameterReplayCatalog.model_construct()
    prepared = owner.MinuteParameterPreparedPublication.model_construct()

    def unexpected_read(*args: object, **kwargs: object) -> object:
        raise AssertionError('an unretained source unit opened authority files')

    monkeypatch.setattr(owner.os, 'open', unexpected_read)
    with factory(catalog, prepared) as unit:
        assert unit is None


def test_resolved_read_unit_file_evidence_reads_actual_bytes_and_inode(tmp_path: Path) -> None:
    from rquant import minute_backtest_parameter_producer as owner

    read = getattr(owner, '_resolved_read_file', None)
    assert callable(read), 'a resolved unit has no fresh complete FD byte/identity evidence'
    private = tmp_path / 'original-unit'
    private.mkdir(mode=0o700)
    path = private / 'snapshot.parquet'
    path.write_bytes(b'complete-original-bytes')
    path.chmod(0o600)
    original = read(path)
    assert read(path, expected=original) == original
    stamp = path.stat().st_mtime_ns
    path.write_bytes(b'changed--original-bytes')
    os.utime(path, ns=(stamp, stamp))
    with pytest.raises(PermissionError):
        read(path, expected=original)
    path.write_bytes(b'complete-original-bytes')
    os.utime(path, ns=(stamp, stamp))
    replacement = private / 'replacement'
    replacement.write_bytes(path.read_bytes())
    replacement.chmod(0o600)
    os.utime(replacement, ns=(stamp, stamp))
    os.replace(replacement, path)
    with pytest.raises(PermissionError):
        read(path, expected=original)


@pytest.fixture(scope='module')
def resolved_original_source(tmp_path_factory: pytest.TempPathFactory) -> object:
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from tests.unit.test_minute_backtest_parameter_formal import prepared_source

    source = prepared_source.__wrapped__(tmp_path_factory)
    with minute_parameter_validation_scope():
        value = next(source)
    try:
        yield value
    finally:
        source.close()


def test_resolved_read_unit_preserves_complete_receipt_copy_and_original_plan(resolved_original_source: object) -> None:
    from datetime import timedelta
    from rquant.minute_backtest_parameter_adapter import MinuteParameterFormalParameters, MinuteParameterFormalReplayAdapter
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from rquant.minute_backtest_parameter_formal import build_minute_parameter_plan
    from rquant.minute_backtest_parameter_producer import resolved_minute_parameter_read_unit
    from rquant.strategy_job_adapters import StrategyJobAdapterRegistry
    from tests.unit.test_minute_backtest_parameter_formal import _protocol

    value = resolved_original_source
    with minute_parameter_validation_scope():
        prepared = build_minute_parameter_plan(value.published.receipt.frozen, value.published,
            prepared_publication=value.carrier, catalog=value.catalog, definitions=value.definitions,
            protocol=_protocol(value), now=value.now + timedelta(seconds=5), deadline=value.now + timedelta(hours=1))
        spec = prepared.submission(job_id=uuid4()).spec
        original = StrategyJobAdapterRegistry((MinuteParameterFormalReplayAdapter(value.catalog),)).plan(spec)
    with minute_parameter_validation_scope():
        with resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
            assert unit is not None, 'the supported original complete publication did not retain a read unit'
            first = unit.resolve(value.catalog, value.carrier)
            assert first == value.published.receipt and first is not value.published.receipt
            object.__setattr__(first.frozen.runtime, 'owner_id', 'polluted-return-copy')
            assert unit.resolve(value.catalog, value.carrier) == value.published.receipt
            adapter = MinuteParameterFormalReplayAdapter(value.catalog, resolved_read_unit=unit)
            parameters = MinuteParameterFormalParameters.from_prepared(value.published.receipt.frozen, value.carrier)
            assert adapter.expected(parameters) == value.published.receipt
            assert adapter.parameters(spec) == parameters
            observed = StrategyJobAdapterRegistry((adapter,)).plan(spec)
            assert tuple(item.model_dump_json() for item in observed) == tuple(item.model_dump_json() for item in original)
        with pytest.raises(PermissionError, match='closed'):
            unit.resolve(value.catalog, value.carrier)


@pytest.mark.parametrize('change', ('source', 'receipt', 'metadata', 'manifest', 'snapshot', 'file_mode', 'directory_mode',
    'baseline_source', 'baseline_receipt', 'baseline_metadata', 'baseline_manifest', 'baseline_snapshot'))
def test_resolved_read_unit_rechecks_actual_complete_gate_files(resolved_original_source: object, change: str) -> None:
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from rquant.minute_backtest_parameter_producer import resolved_minute_parameter_read_unit

    value = resolved_original_source
    binding = value.published.receipt.binding
    baseline = value.catalog.fact_sources[0]
    baseline_binding = value.baseline.receipt.binding
    paths = dict(source=value.carrier.source.path, receipt=value.carrier.receipt.path,
        metadata=value.carrier.metadata_identity.source_path,
        manifest=value.catalog.research_lake_root / binding.manifest_relative_path,
        snapshot=value.catalog.research_lake_root / binding.manifest.artifacts[0].relative_path,
        baseline_source=baseline.source.path, baseline_receipt=baseline.receipt.path,
        baseline_metadata=baseline.metadata_identity.source_path,
        baseline_manifest=value.catalog.research_lake_root / baseline_binding.manifest_relative_path,
        baseline_snapshot=value.catalog.research_lake_root / baseline_binding.manifest.artifacts[0].relative_path)
    path = paths.get(change, value.carrier.source.path)
    before = path.read_bytes()
    identity = path.stat()
    parent_mode = path.parent.stat().st_mode & 0o777
    try:
        with minute_parameter_validation_scope(), pytest.raises(PermissionError):
            with resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
                assert unit is not None
                if change == 'file_mode':
                    path.chmod(0o640)
                elif change == 'directory_mode':
                    path.parent.chmod(0o755)
                else:
                    path.write_bytes(bytes((before[0] ^ 1,)) + before[1:])
                    os.utime(path, ns=(identity.st_atime_ns, identity.st_mtime_ns))
                unit.resolve(value.catalog, value.carrier)
    finally:
        path.parent.chmod(parent_mode)
        path.chmod(identity.st_mode & 0o777)
        path.write_bytes(before)
        os.utime(path, ns=(identity.st_atime_ns, identity.st_mtime_ns))


@pytest.mark.parametrize('change', ('catalog', 'prepared', 'retained', 'field', 'code', 'closure_default', 'gate_model', 'proof_helper'))
def test_resolved_read_unit_rechecks_live_content_and_original_policy(resolved_original_source: object,
    monkeypatch: pytest.MonkeyPatch, change: str) -> None:
    from rquant import minute_backtest_parameter_producer as owner
    from rquant.executable_dependencies import ExecutableDependencyError
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    value = resolved_original_source
    cells = ['original']
    generator = owner.MinuteParameterReplayCatalog._metadata_gate.__wrapped__
    if change == 'closure_default':
        monkeypatch.setattr(generator, '__defaults__', (cells,))
    with minute_parameter_validation_scope():
        with owner.resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
            assert unit is not None
            with monkeypatch.context() as patch:
                current_catalog, current_prepared = value.catalog, value.carrier
                if change == 'catalog':
                    current_catalog = value.catalog.model_copy(update={'installed_policies': ()})
                elif change == 'prepared':
                    current_prepared = value.carrier.model_copy(update={'loaded_bytes': value.carrier.loaded_bytes - 1})
                elif change == 'retained':
                    before_owner = unit._content.expected.frozen.runtime.owner_id
                    object.__setattr__(unit._content.expected.frozen.runtime, 'owner_id', 'changed-original')
                elif change == 'field':
                    patch.setattr(owner.MinuteParameterPublicationReceipt.model_fields['snapshot_artifact_bytes'], 'description', 'changed-live-field')
                elif change == 'code':
                    patch.setattr(owner.MinuteParameterReplayCatalog.resolve_prepared, '__code__', (lambda *args, **kwargs: None).__code__)
                elif change == 'gate_model':
                    patch.setattr(owner.ResearchGateRequest.model_fields['mode'], 'description', 'changed-live-gate-model')
                elif change == 'proof_helper':
                    from rquant.minute_backtest_parameter_study_projection import _ProjectionSemanticSnapshot
                    implementation = _ProjectionSemanticSnapshot.value
                    patch.setattr(implementation, '__code__', implementation.__code__.replace(co_firstlineno=implementation.__code__.co_firstlineno + 1))
                else:
                    cells.append('changed')
                try:
                    with pytest.raises((PermissionError, ExecutableDependencyError)):
                        unit.resolve(current_catalog, current_prepared)
                finally:
                    cells[:] = ['original']
                    if change == 'retained':
                        object.__setattr__(unit._content.expected.frozen.runtime, 'owner_id', before_owner)


def test_resolved_read_unit_exception_releases_only_its_charged_lease(resolved_original_source: object) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from rquant.minute_backtest_parameter_producer import resolved_minute_parameter_read_unit

    value = resolved_original_source
    with minute_parameter_validation_scope():
        state = contracts._PARAMETER_CONTENT_STATE.get()
        with pytest.raises(RuntimeError, match='trial failed'):
            with resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
                assert unit is not None and unit.retained_bytes > 0
                assert state.resolved_read_unit_count == 1
                control_bytes = state.resolved_read_unit_bytes - unit.retained_bytes
                assert control_bytes > 0
                assert state.retained_bytes == (
                    state.policy_bytes + sum(entry.retained_bytes for entry in state.entries.values())
                    + unit.retained_bytes + control_bytes
                )
                assert state.retained_bytes <= 16 * 1024 * 1024
                raise RuntimeError('trial failed')
        assert state.resolved_read_unit_bytes == state.resolved_read_unit_count == 0
        assert state.retained_bytes == state.policy_bytes + sum(entry.retained_bytes for entry in state.entries.values())
        with pytest.raises(PermissionError, match='closed'):
            unit.resolve(value.catalog, value.carrier)
    assert contracts._PARAMETER_CONTENT_STATE.get() is None and state.retained_bytes == 0


@pytest.mark.parametrize('reason', ('tiny', 'full', 'opaque'))
def test_resolved_read_unit_unretained_original_receipt_uses_full_path(resolved_original_source: object,
    monkeypatch: pytest.MonkeyPatch, reason: str) -> None:
    from contextlib import ExitStack
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant import minute_backtest_parameter_producer as owner
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope

    class OpaqueSchema(dict):
        pass

    value = resolved_original_source
    with minute_parameter_validation_scope(), ExitStack() as stack:
        if reason == 'tiny':
            assert stack.enter_context(contracts._parameter_read_unit_retention(retained_bytes=16 * 1024 * 1024 - 1))
        elif reason == 'full':
            for _ in range(8):
                assert stack.enter_context(contracts._parameter_read_unit_retention(retained_bytes=0))
        else:
            monkeypatch.setattr(owner.MinuteParameterPublicationReceipt, '__pydantic_core_schema__',
                OpaqueSchema(owner.MinuteParameterPublicationReceipt.__pydantic_core_schema__))
        state = contracts._PARAMETER_CONTENT_STATE.get()
        before_bytes, before_count = state.resolved_read_unit_bytes, state.resolved_read_unit_count
        with owner.resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
            assert unit is None
            assert value.catalog.resolve_prepared(value.carrier) == value.published.receipt
        assert (state.resolved_read_unit_bytes, state.resolved_read_unit_count) == (before_bytes, before_count)


def test_resolved_read_unit_content_limits_and_real_alias_are_preserved() -> None:
    from rquant import minute_backtest_parameter_producer as owner
    from rquant.executable_dependencies import ExecutableDependencyError

    shared = ['original']
    assert owner._resolved_read_content((shared, shared)) != owner._resolved_read_content((['original'], ['original']))
    before = owner._resolved_read_content((shared, shared))
    shared.append('changed')
    assert owner._resolved_read_content((shared, shared)) != before
    with pytest.raises(ExecutableDependencyError, match='node/depth'):
        owner._resolved_read_content(list(range(8192)))
    with pytest.raises(ExecutableDependencyError, match='byte limit'):
        owner._resolved_read_content('x' * (16 * 1024 * 1024))


def test_resolved_read_unit_fee_includes_retained_readonly_backing_map() -> None:
    import sys
    from types import MappingProxyType
    from rquant import minute_backtest_parameter_producer as owner

    backing = {'bound-key': 'complete-value'}
    view = MappingProxyType(backing)
    assert owner._resolved_read_retained_bytes(view) >= (sys.getsizeof(view) + sys.getsizeof(backing)
        + sum(sys.getsizeof(key) + sys.getsizeof(value) for key, value in backing.items()))


@contextmanager
def _finite_unit_budget_events() -> Iterator[dict[str, int]]:
    import sys
    from rquant import minute_backtest_parameter_producer as producer
    from rquant import minute_backtest_parameter_contracts as contracts

    functions = (producer._resolved_minute_parameter_read_unit, producer._resolved_read_unit_fee,
        producer._resolved_read_guard_budget, contracts._parameter_content_guard)
    targets = {id(function.__code__): (function.__code__, function.__name__) for function in functions}
    events = {}
    old_profile, old_trace = sys.getprofile(), sys.gettrace()
    assert old_profile is old_trace is None
    def observe(frame: FrameType, event: str, value: object) -> None:
        if event != "return":
            return
        target = targets.get(id(frame.f_code))
        if target is None or frame.f_code is not target[0]:
            return
        if target[1] == "_parameter_content_guard":
            label = "probe" if value is not None and value.model_policy_probe is not None else "no_probe"
        else:
            label = str(value) if type(value) is int else "None" if value is None else "typed"
        key = target[1] + ":" + label
        events[key] = events.get(key, 0) + 1
    def exception(frame: FrameType, event: str, value: object) -> object:
        target = targets.get(id(frame.f_code))
        if target is None or frame.f_code is not target[0]:
            return None
        frame.f_trace_lines = frame.f_trace_opcodes = False
        if event == "exception":
            kind, error, _ = value
            message = str(error)
            flags = ",".join(name for name in ("ambiguous", "node", "depth", "byte", "opaque") if name in message)
            key = target[1] + ":" + kind.__name__ + ":" + flags
            events[key] = events.get(key, 0) + 1
        return exception
    sys.setprofile(observe)
    sys.settrace(exception)
    try:
        yield events
    finally:
        sys.settrace(old_trace)
        sys.setprofile(old_profile)


def test_resolved_read_unit_keeps_seven_prior_content_entries_and_retires_its_additions(resolved_original_source: object) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from rquant.minute_backtest_parameter_producer import resolved_minute_parameter_read_unit

    value = resolved_original_source
    with minute_parameter_validation_scope():
        for version in range(2, 9):
            value.published.receipt.frozen.runtime.model_copy(update={"source_version": version}).content
        state = contracts._PARAMETER_CONTENT_STATE.get()
        previous = dict(state.entries)
        assert len(previous) == 7
        for _ in range(2):
            with _finite_unit_budget_events() as events, resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
                assert unit is not None, ("this trial's constructor consumed the last shared content slot", events)
                assert unit.resolve(value.catalog, value.carrier) == value.published.receipt
                assert len(state.entries) + state.resolved_read_unit_count <= 8
                assert state.retained_bytes <= contracts._MAX_PARAMETER_CONTENT_BYTES
            assert state.entries == previous
            assert state.resolved_read_unit_count == state.resolved_read_unit_bytes == 0


@pytest.mark.parametrize('allowance', (4_762_823, 5_600_000))
def test_resolved_read_unit_allocates_optional_plans_after_complete_mandatory_charge(resolved_original_source: object, allowance: int) -> None:
    from rquant import minute_backtest_parameter_contracts as contracts
    from rquant.executable_dependencies import ExecutableDependencyError
    from rquant.minute_backtest_parameter_definition import minute_parameter_validation_scope
    from rquant.minute_backtest_parameter_producer import resolved_minute_parameter_read_unit

    value = resolved_original_source
    with minute_parameter_validation_scope():
        assert value.published.receipt.frozen.source_content_seed == value.published.receipt.seed
        state = contracts._PARAMETER_CONTENT_STATE.get()
        capacity = contracts._parameter_read_unit_capacity()
        assert capacity is not None and capacity > allowance
        with contracts._parameter_read_unit_retention(retained_bytes=capacity - allowance) as held:
            assert held
            with _finite_unit_budget_events() as events, resolved_minute_parameter_read_unit(value.catalog, value.carrier) as unit:
                if allowance == 4_762_823:
                    assert unit is None, 'the complete mandatory unit must not exceed its real allowance'
                    assert value.catalog.resolve_prepared(value.carrier) == value.published.receipt
                    return
                assert unit is not None, ("optional plans occupied the mandatory complete unit allowance", events)
                assert unit.resolve(value.catalog, value.carrier) == value.published.receipt
                assert unit.retained_bytes <= allowance
                assert state.retained_bytes <= contracts._MAX_PARAMETER_CONTENT_BYTES
                field = type(unit._content.expected.frozen.runtime).model_fields["owner_id"]
                old = field.description
                try:
                    field.description = "changed during the bounded current validation"
                    with pytest.raises(ExecutableDependencyError):
                        unit.resolve(value.catalog, value.carrier)
                finally:
                    field.description = old
