from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from rquant import minute_backtest_commands as commands
from rquant import minute_backtest_parameter_study_execution as execution
from rquant import minute_backtest_parameter_study_journal as journal
from rquant import minute_backtest_parameter_study_projection as projections
from rquant import strict_json
from rquant.runtime_read_interrupt import ReadInterruptedError, interruptible_read, request_read_interrupt
from rquant.web.app import create_app
from rquant.web import minute_backtest_service as services
from rquant.web.settings import WebSettings

NOW = datetime(2026, 10, 8, 6, tzinfo=UTC)
COMMAND_ID = UUID('891bfe91-7dcd-461b-b1a3-ecfbef66036e')
OWNER = 'synthetic-researcher'
CODE = 'a' * 40
PIN = 'b' * 64


def test_projection_private_configuration_cannot_be_half_pinned(tmp_path: Path) -> None:
    assert 'minute_study_projection_authority' in WebSettings.model_fields, 'web cannot consume an explicitly pinned projection authority'
    with pytest.raises(ValueError):
        WebSettings(serving_root=tmp_path, minute_study_projection_authority=tmp_path / 'projection.json')


def test_projection_configuration_requires_the_original_minute_installation(tmp_path: Path) -> None:
    assert 'minute_study_projection_expected_sha256' in WebSettings.model_fields, 'projection authority expected SHA cannot be configured'
    with pytest.raises(ValueError):
        WebSettings(serving_root=tmp_path, minute_study_projection_authority=tmp_path / 'projection.json',
            minute_study_projection_expected_sha256='a' * 64)


def _settings(tmp_path: Path, **updates: object) -> WebSettings:
    values: dict[str, object] = dict(serving_root=tmp_path / 'serving',
        minute_replay_installation=tmp_path / 'installed.json', minute_replay_expected_code_sha=CODE,
        minute_study_projection_authority=tmp_path / 'projection.json', minute_study_projection_expected_sha256=PIN)
    return WebSettings.model_validate({**values, **updates})


@pytest.mark.parametrize('pin', ['', 'a' * 63, 'A' * 64, 'a' * 64 + '\n'])
def test_projection_configuration_rejects_bad_pin(tmp_path: Path, pin: str) -> None:
    with pytest.raises(ValueError):
        _settings(tmp_path, minute_study_projection_expected_sha256=pin)


@pytest.mark.parametrize('path', [Path('relative.json'), Path('/private/tmp/one/../projection.json')])
def test_projection_configuration_rejects_noncanonical_locator(tmp_path: Path, path: Path) -> None:
    with pytest.raises(ValueError, match='absolute and normalized'):
        _settings(tmp_path, minute_study_projection_authority=path)


def test_projection_environment_pair_and_app_forwarding_are_lazy(tmp_path: Path) -> None:
    settings = WebSettings.from_env({'RQUANT_SERVING_ROOT': str(tmp_path / 'serving'),
        'RQUANT_MINUTE_REPLAY_INSTALLATION': str(tmp_path / 'installed.json'),
        'RQUANT_RUNTIME_COMMIT': CODE, 'RQUANT_MINUTE_STUDY_PROJECTION_AUTHORITY': str(tmp_path / 'projection.json'),
        'RQUANT_MINUTE_STUDY_PROJECTION_SHA256': PIN})
    before = tuple(tmp_path.rglob('*'))
    app = create_app(settings, background=False, clock=lambda: NOW)
    service = app.state.web.minute_backtests
    assert type(service) is services.LazyMinuteWebService
    assert service.study_projection_authority == settings.minute_study_projection_authority
    assert service.study_projection_expected_sha256 == PIN and service._service is None
    app.openapi()
    assert service._service is None and tuple(tmp_path.rglob('*')) == before


def test_default_environment_and_app_do_not_open_a_projection(tmp_path: Path) -> None:
    settings = WebSettings.from_env({'RQUANT_SERVING_ROOT': str(tmp_path / 'serving'),
        'RQUANT_MINUTE_REPLAY_INSTALLATION': str(tmp_path / 'installed.json'), 'RQUANT_RUNTIME_COMMIT': CODE})
    app = create_app(settings, background=False, clock=lambda: NOW)
    service = app.state.web.minute_backtests
    assert service.study_projection_authority is None and service.study_projection_expected_sha256 is None
    assert service._service is None and not tuple(tmp_path.rglob('*'))


class _ProjectionContext:
    def __init__(self, probe: SimpleNamespace, path: Path) -> None:
        self.probe, self.closed = probe, False
        self.fd = os.open(path, os.O_RDONLY)

    def __enter__(self) -> _ProjectionContext:
        self.probe.events.append('enter_projection')
        return self

    def close(self) -> None:
        if not self.closed:
            os.close(self.fd)
            self.closed = True
            self.probe.events.append('close_projection')

    def __exit__(self, *args: object) -> None:
        self.close()


@pytest.fixture
def request_probe(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    # This isolates existing authority/aggregate boundaries. It is not an installed or sealed execution proof.
    probe = SimpleNamespace(events=[], contexts=[], load_error=None, aggregate_error=None,
        baseline_error=None, no_admission=False, changed_journal=False, current_error=None,
        interrupt_during_aggregate=False, projection_argument=None, aggregate_result=object())
    plan = object()
    command = object()
    prepared = SimpleNamespace(marker=SimpleNamespace(command=SimpleNamespace(spec=object())))
    probe.effect = SimpleNamespace(command=command, plan=plan, prepared=(prepared,))
    probe.fact = SimpleNamespace(command=command, admission_json='explicit-synthetic-boundary', status='succeeded')
    profile = SimpleNamespace(code_sha=CODE, catalog=None)
    reference = SimpleNamespace(path=tmp_path / 'installed.json')
    authority = object()

    def current() -> None:
        probe.events.append('current')
        if probe.current_error is not None and probe.events.count('current') > 1:
            raise probe.current_error

    cached = SimpleNamespace(profile=profile, reference=reference, authority=authority,
        reader=object(), clock=lambda: NOW, verify_current=current)
    probe.fresh = SimpleNamespace(profile=profile, reference=reference, authority=authority,
        reader=object(), clock=lambda: NOW)
    probe.service = services.MinuteWebService(cached, study_projection_authority=tmp_path / 'projection.json',
        study_projection_expected_sha256=PIN)

    def fresh_loader(path: Path, *, expected_code_sha: str, clock: object) -> object:
        assert path == reference.path and expected_code_sha == CODE and clock is cached.clock
        probe.events.append('fresh_installation')
        return probe.fresh

    def parse(model: object, body: str) -> object:
        assert model is execution.MinuteParameterStudyExecutionEffect and body == probe.fact.admission_json
        probe.events.append('original_parser')
        return probe.effect

    def read_journal(owner_id: str, *, command_id: UUID) -> object:
        assert owner_id == OWNER and command_id == COMMAND_ID
        probe.events.append('journal')
        if probe.no_admission:
            return SimpleNamespace(command=command, admission_json=None, status='pending')
        if probe.changed_journal and probe.events.count('journal') > 1:
            return object()
        return probe.fact

    def minute_writer(installation: object) -> object:
        assert installation is probe.fresh
        return SimpleNamespace(installation=installation)

    def verify(value: object) -> None:
        assert value is plan
        probe.events.append('baseline')
        if probe.baseline_error is not None:
            raise probe.baseline_error

    def study_writer(writer: object) -> object:
        assert writer.installation is probe.fresh
        return SimpleNamespace(_verify_baseline=verify)

    def original_reader(installation: object, spec: object, *, defer_facade: bool = False) -> object:
        assert installation is probe.fresh and spec is prepared.marker.command.spec
        assert defer_facade is (probe.service.study_projection_authority is not None)
        probe.events.append('original_full_reader')
        return probe.original_reader

    owned = tmp_path / 'owned-projection-fd'
    owned.write_bytes(b'synthetic-readonly-context')
    owned.chmod(0o600)

    def projection_loader(installation: object, path: Path, *, expected_sha256: str, writable: bool = False) -> _ProjectionContext:
        assert installation is probe.fresh and path == probe.service.study_projection_authority
        assert expected_sha256 == PIN and writable is False
        assert 'baseline' in probe.events and probe.events.index('fresh_installation') < probe.events.index('baseline')
        probe.events.append('load_projection')
        if probe.load_error is not None:
            raise probe.load_error
        context = _ProjectionContext(probe, owned)
        probe.contexts.append(context)
        return context

    def aggregate(value: object, *, prepared: object, reader: object, as_of: datetime,
        projection: object | None = None) -> object:
        assert value is plan and prepared is probe.effect.prepared and reader is probe.original_reader and as_of == NOW
        probe.events.append('aggregate')
        probe.projection_argument = projection
        if probe.interrupt_during_aggregate:
            request_read_interrupt()
            with interruptible_read(SimpleNamespace(interrupt=lambda: None)):
                raise AssertionError('latched original stop must refuse the read')
        if probe.aggregate_error is not None:
            raise probe.aggregate_error
        return probe.aggregate_result

    probe.original_reader = object()
    probe.collaboration = SimpleNamespace(study_journal=read_journal)
    monkeypatch.setattr(services, 'load_minute_replay_installation', fresh_loader)
    monkeypatch.setattr(strict_json, 'strict_model_validate_json', parse)
    monkeypatch.setattr(commands, 'MinuteCommandWriter', minute_writer)
    monkeypatch.setattr(journal, 'MinuteParameterStudyCommandWriter', study_writer)
    monkeypatch.setattr(services, '_InstalledStudyReplayReader', original_reader)
    monkeypatch.setattr(projections, 'load_minute_study_projection', projection_loader)
    monkeypatch.setattr(execution, 'read_minute_parameter_study_execution', aggregate)
    try:
        yield probe
    finally:
        leaked = [context for context in probe.contexts if not context.closed]
        for context in leaked:
            context.close()
        assert not leaked, 'request left an owned projection FD open'


def _read(probe: Any) -> object:
    return probe.service._study_execution(COMMAND_ID, owner_id=OWNER, collaboration=probe.collaboration)


def test_request_loads_readonly_projection_after_fresh_installation_and_baseline(request_probe: Any) -> None:
    fact, effect, result = _read(request_probe)
    assert fact is request_probe.fact and effect is request_probe.effect and result is request_probe.aggregate_result
    assert len(request_probe.contexts) == 1, 'configured readonly projection was never loaded'
    assert request_probe.projection_argument is request_probe.contexts[0] and request_probe.contexts[0].closed
    events = request_probe.events
    assert events.index('baseline') < events.index('load_projection') < events.index('aggregate') < events.index('close_projection')
    assert events.count('journal') == 2 and events.count('current') == 2


def test_deferred_reader_preserves_original_fresh_facade_denial_on_full_fallback(tmp_path: Path) -> None:
    from rquant.minute_backtest_parameter_producer import MinuteParameterPublicationReference, MinuteParameterReplayCatalog
    from rquant.minute_backtest_producer import _secure_private_bytes

    spec, current = object(), [None]
    calls: list[object] = []

    def facade(value: object) -> object:
        calls.append(value)
        raise PermissionError('actual fresh facade boundary denied')

    original_reader = SimpleNamespace(get_command_context=lambda job_id: current[0])
    references = []
    for name in ('source', 'receipt'):
        path = tmp_path / name
        path.write_bytes(b'explicit facade-boundary fixture; no publication claim')
        path.chmod(0o600)
        _, reference = _secure_private_bytes(path)
        references.append(reference)
    catalog = MinuteParameterReplayCatalog(entries=(MinuteParameterPublicationReference(
        source_key='boundary', source_version=1, owner_id=OWNER, source=references[0], receipt=references[1]),))
    installation = SimpleNamespace(reader=original_reader,
        authority=SimpleNamespace(final_artifact_root=tmp_path / 'artifacts'),
        profile=SimpleNamespace(parameter_catalog=catalog),
        parameter_submission_facade=facade)
    reader = services._InstalledStudyReplayReader(installation, spec, defer_facade=True)
    assert calls == [] and reader.submission_facade is None
    assert reader.read(COMMAND_ID, owner_id=OWNER, native_id='n_shape', native_version=1, as_of=NOW) is None
    assert calls == []
    current[0] = SimpleNamespace(job=SimpleNamespace(spec=spec))
    with pytest.raises(PermissionError, match='fresh facade boundary denied'):
        reader.read(COMMAND_ID, owner_id=OWNER, native_id='n_shape', native_version=1, as_of=NOW)
    assert calls == [spec]
    with pytest.raises(PermissionError, match='fresh facade boundary denied'):
        services._InstalledStudyReplayReader(installation, spec)
    assert calls == [spec, spec]


def test_each_request_has_a_new_projection_context(request_probe: Any) -> None:
    _read(request_probe)
    _read(request_probe)
    assert len(request_probe.contexts) == 2, 'projection must be opened once in each read request'
    assert request_probe.contexts[0] is not request_probe.contexts[1]
    assert all(context.closed for context in request_probe.contexts)
    assert request_probe.events.count('fresh_installation') == 2


def test_unconfigured_request_uses_original_aggregate_without_projection_io(request_probe: Any) -> None:
    request_probe.service.study_projection_authority = None
    request_probe.service.study_projection_expected_sha256 = None
    assert _read(request_probe)[2] is request_probe.aggregate_result
    assert request_probe.projection_argument is None and not request_probe.contexts
    assert request_probe.events.count('aggregate') == 1 and 'load_projection' not in request_probe.events


@pytest.mark.parametrize('error', [PermissionError('bad authority pin'), PermissionError('unsafe permission'),
    ValueError('corrupt derived fact')])
def test_projection_loader_failures_never_downgrade_to_full_fallback(request_probe: Any, error: BaseException) -> None:
    request_probe.load_error = error
    with pytest.raises(type(error)) as raised:
        _read(request_probe)
    assert raised.value is error and 'aggregate' not in request_probe.events and not request_probe.contexts


@pytest.mark.parametrize('error', [PermissionError('current source changed'), ValueError('corrupt certified result'),
    ReadInterruptedError('original aggregate interrupted')])
def test_aggregate_exception_closes_projection_and_is_preserved(request_probe: Any, error: BaseException) -> None:
    request_probe.aggregate_error = error
    with pytest.raises(type(error)) as raised:
        _read(request_probe)
    assert raised.value is error
    assert len(request_probe.contexts) == 1 and request_probe.contexts[0].closed


def test_original_latched_interrupt_unwinds_the_request_projection(request_probe: Any) -> None:
    request_probe.interrupt_during_aggregate = True
    with pytest.raises(ReadInterruptedError):
        _read(request_probe)
    assert len(request_probe.contexts) == 1 and request_probe.contexts[0].closed


def test_baseline_denial_is_preserved_before_projection_load(request_probe: Any) -> None:
    error = PermissionError('original physical baseline changed')
    request_probe.baseline_error = error
    with pytest.raises(PermissionError) as raised:
        _read(request_probe)
    assert raised.value is error and 'load_projection' not in request_probe.events and 'aggregate' not in request_probe.events


def test_fresh_installation_mismatch_cannot_open_projection(request_probe: Any) -> None:
    request_probe.fresh.profile = object()
    with pytest.raises(PermissionError, match='installed authority changed'):
        _read(request_probe)
    assert 'baseline' not in request_probe.events and 'load_projection' not in request_probe.events


def test_changed_parent_command_is_rejected_before_any_fresh_load(request_probe: Any) -> None:
    request_probe.effect.command = object()
    with pytest.raises(PermissionError, match='another complete parent'):
        _read(request_probe)
    assert 'fresh_installation' not in request_probe.events and 'load_projection' not in request_probe.events


def test_journal_change_after_aggregate_cannot_leave_projection_open(request_probe: Any) -> None:
    request_probe.changed_journal = True
    with pytest.raises(ValueError, match='journal changed'):
        _read(request_probe)
    assert len(request_probe.contexts) == 1 and request_probe.contexts[0].closed


def test_last_current_denial_cannot_leave_projection_open(request_probe: Any) -> None:
    request_probe.current_error = PermissionError('current authority revoked after read')
    with pytest.raises(PermissionError) as raised:
        _read(request_probe)
    assert raised.value is request_probe.current_error
    assert len(request_probe.contexts) == 1 and request_probe.contexts[0].closed


@pytest.mark.parametrize('reason', ['no_admission', 'no_prepared_trials'])
def test_request_without_prepared_work_does_not_open_projection(request_probe: Any, reason: str) -> None:
    if reason == 'no_admission':
        request_probe.no_admission = True
    else:
        request_probe.effect.prepared = ()
    assert _read(request_probe)[2] is None
    assert 'load_projection' not in request_probe.events and 'aggregate' not in request_probe.events
