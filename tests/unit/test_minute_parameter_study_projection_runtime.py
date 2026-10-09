from __future__ import annotations

import builtins
import os
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from importlib import import_module, util
from pathlib import Path
from threading import Event
from types import ModuleType
from uuid import uuid4

import pytest

from rquant.runtime_service_entrypoint import RuntimeServiceKind
from rquant.runtime_service_control import RuntimeServicePlane, RuntimeServiceStatus, RuntimeStepResult
from rquant.runtime_service_entrypoint import RuntimeServiceManifest, RuntimeServiceRegistry, run_runtime_service_manifest
from rquant.runtime_read_interrupt import ReadInterruptedError, request_read_interrupt

NOW = datetime(2026, 10, 8, 5, 30, tzinfo=UTC)
COMMIT = "a" * 40
PROJECTION_SHA = "b" * 64


def _api() -> ModuleType:
    assert util.find_spec("rquant.runtime_builder_minute_study_projection") is not None, "typed projection builder is missing"
    return import_module("rquant.runtime_builder_minute_study_projection")


def _settings(tmp_path: Path) -> dict[str, str]:
    return {"installation_path": str(tmp_path / "private-installation.json"),
        "expected_code_sha": COMMIT,
        "projection_authority_path": str(tmp_path / "new-namespace/authority.json"),
        "projection_expected_sha256": PROJECTION_SHA}


def _manifest(tmp_path: Path, **changes: object) -> RuntimeServiceManifest:
    values: dict[str, object] = {"service_id": "research.minute-study-projection",
        "service_kind": "minute_study_projection", "plane": RuntimeServicePlane.RESEARCH,
        "interval_seconds": 0.0, "stale_after_seconds": 60.0,
        "producer_commit": COMMIT, "settings": _settings(tmp_path)}
    values.update(changes)
    return RuntimeServiceManifest.model_validate(values)


@pytest.fixture(autouse=True)
def _no_descriptor_leaks() -> Iterator[None]:
    before = len(tuple(Path("/dev/fd").iterdir()))
    yield
    assert len(tuple(Path("/dev/fd").iterdir())) == before


class _ProjectionContext:
    def __init__(self, path: Path, receipt: object, events: list[object],
        error: BaseException | None = None, on_reconcile: Callable[[], None] | None = None) -> None:
        self.descriptor = os.open(path, os.O_RDONLY)
        self.receipt, self.events, self.error, self.on_reconcile = receipt, events, error, on_reconcile
        self.closed = False

    def __enter__(self) -> _ProjectionContext:
        self.events.append("enter")
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        if not self.closed:
            os.close(self.descriptor)
            self.closed = True
            self.events.append("close")

    def reconcile_one(self, *, as_of: datetime) -> object:
        assert not self.closed
        self.events.append(("reconcile", as_of))
        if self.on_reconcile is not None:
            self.on_reconcile()
        if self.error is not None:
            raise self.error
        return self.receipt


def _loaders(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *,
    published: bool = True, error: BaseException | None = None,
    on_installation: Callable[[], None] | None = None,
    on_reconcile: Callable[[], None] | None = None,
) -> tuple[list[object], list[_ProjectionContext]]:
    installation_api = import_module("rquant.minute_backtest_installation")
    projection_api = import_module("rquant.minute_backtest_parameter_study_projection")
    receipt = projection_api.MinuteStudyProjectionReconcileResult(sequence=7,
        candidate_job_id=uuid4() if published else None, published=published, pending_jobs=2)
    sentinel = tmp_path / "owned-sentinel"
    sentinel.write_bytes(b"independent synthetic context handle")
    events: list[object] = []
    contexts: list[_ProjectionContext] = []
    installations: list[object] = []

    def load_installation(path: Path, *, expected_code_sha: str, writable: bool,
        clock: Callable[[], datetime]) -> object:
        assert path == Path(_settings(tmp_path)["installation_path"])
        assert expected_code_sha == COMMIT and writable is False
        events.append(("installation", clock))
        installation = object()
        installations.append(installation)
        if on_installation is not None:
            on_installation()
        return installation

    def load_projection(installation: object, path: Path, *, expected_sha256: str,
        writable: bool) -> _ProjectionContext:
        assert installation is installations[-1]
        assert path == Path(_settings(tmp_path)["projection_authority_path"])
        assert expected_sha256 == PROJECTION_SHA and writable is True
        events.append("projection")
        context = _ProjectionContext(sentinel, receipt, events, error, on_reconcile)
        contexts.append(context)
        return context

    monkeypatch.setattr(installation_api, "load_minute_replay_installation", load_installation)
    monkeypatch.setattr(projection_api, "load_minute_study_projection", load_projection)
    return events, contexts


def test_minute_projection_kind_and_builder_are_an_independent_existing_loop_role() -> None:
    assert 'minute_study_projection' in {item.value for item in RuntimeServiceKind}, (
        'indexed minute results have no independent projection/reconciliation role'
    )
    assert util.find_spec('rquant.runtime_builder_minute_study_projection') is not None


def test_builder_rejects_incomplete_trust_settings_without_opening_resources(tmp_path: Path) -> None:
    assert util.find_spec('rquant.runtime_builder_minute_study_projection') is not None, 'typed projection builder is missing'
    api = import_module('rquant.runtime_builder_minute_study_projection')
    with pytest.raises(ValueError):
        api.MinuteStudyProjectionSettings.model_validate({'installation_path': tmp_path / 'private.json'})
    assert tuple(tmp_path.iterdir()) == ()


@pytest.mark.parametrize("field", ["installation_path", "expected_code_sha", "projection_authority_path", "projection_expected_sha256"])
def test_all_four_settings_are_required(tmp_path: Path, field: str) -> None:
    api = _api()
    settings = _settings(tmp_path)
    del settings[field]
    with pytest.raises(ValueError):
        api.MinuteStudyProjectionSettings.model_validate(settings)


@pytest.mark.parametrize("field,value", [
    ("installation_path", "relative/private.json"),
    ("projection_authority_path", "/private/parent/../authority.json"),
    ("projection_authority_path", "/private//authority.json"),
    ("installation_path", "/private/./installation.json"),
    ("installation_path", "/private/invalid\x00.json"),
    ("expected_code_sha", "a" * 39), ("expected_code_sha", "A" * 40),
    ("expected_code_sha", " " + COMMIT), ("expected_code_sha", True),
    ("projection_expected_sha256", "b" * 63), ("projection_expected_sha256", "B" * 64),
    ("projection_expected_sha256", PROJECTION_SHA + " "),
])
def test_settings_reject_noncanonical_paths_or_nonexact_hashes(tmp_path: Path, field: str, value: object) -> None:
    api = _api()
    settings: dict[str, object] = dict(_settings(tmp_path))
    settings[field] = value
    with pytest.raises(ValueError):
        api.MinuteStudyProjectionSettings.model_validate(settings)


@pytest.mark.parametrize("change", [{"service_kind": RuntimeServiceKind.LAB_JOBS_PUBLISHER},
    {"plane": RuntimeServicePlane.LIVE}, {"producer_commit": "c" * 40}])
def test_builder_rejects_kind_plane_or_code_binding(tmp_path: Path, change: dict[str, object]) -> None:
    api = _api()
    manifest = _manifest(tmp_path, **change)
    with pytest.raises(ValueError):
        api.minute_study_projection_builder(clock=lambda: NOW)(manifest)
    assert not tuple(tmp_path.iterdir())


def test_build_has_no_filesystem_or_installation_io(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    manifest = _manifest(tmp_path)

    def forbidden_io(*args: object, **kwargs: object) -> object:
        pytest.fail("projection builder construction performed IO")

    with monkeypatch.context() as scoped:
        scoped.setattr(builtins, "open", forbidden_io)
        scoped.setattr(os, "open", forbidden_io)
        scoped.setattr(os, "stat", forbidden_io)
        scoped.setattr(Path, "open", forbidden_io)
        scoped.setattr(Path, "resolve", forbidden_io)
        scoped.setattr(Path, "mkdir", forbidden_io)
        step = api.minute_study_projection_builder(clock=lambda: NOW)(manifest)
        step.close()
        step.close()
    assert not tuple(tmp_path.iterdir())


@pytest.mark.parametrize("published", [True, False])
def test_step_uses_real_clock_readonly_installation_and_writable_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, published: bool,
) -> None:
    api = _api()
    events, contexts = _loaders(monkeypatch, tmp_path, published=published)
    clock = lambda: NOW
    step = api.minute_study_projection_builder(clock=clock)(_manifest(tmp_path))
    result = step()
    assert isinstance(result, RuntimeStepResult)
    assert (result.input_sequence, result.output_sequence, result.processed_count,
        result.backlog_count, result.projection_published) == (7, 7, int(published), 2, published)
    assert events == [("installation", clock), "projection", "enter", ("reconcile", NOW), "close"]
    assert contexts[0].closed
    step.close()


def test_steps_reload_each_operation_and_close_disables_later_steps(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    events, contexts = _loaders(monkeypatch, tmp_path)
    times = iter((NOW, NOW + timedelta(minutes=1)))
    clock = lambda: next(times)
    step = api.minute_study_projection_builder(clock=clock)(_manifest(tmp_path))
    step()
    step()
    assert len(contexts) == 2 and all(item.closed for item in contexts)
    assert [item for item in events if isinstance(item, tuple) and item[0] == "reconcile"] == [
        ("reconcile", NOW), ("reconcile", NOW + timedelta(minutes=1))]
    step.close()
    step.close()
    frozen = list(events)
    with pytest.raises(RuntimeError, match="closed"):
        step()
    assert events == frozen


@pytest.mark.parametrize("error", [PermissionError("current authority changed"),
    RuntimeError("synthetic materialization failed"), ReadInterruptedError("original stop")])
def test_reconcile_failure_is_not_retried_or_swallowed_and_closes_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: BaseException,
) -> None:
    api = _api()
    events, contexts = _loaders(monkeypatch, tmp_path, error=error)
    step = api.minute_study_projection_builder(clock=lambda: NOW)(_manifest(tmp_path))
    with pytest.raises(type(error)) as observed:
        step()
    assert observed.value is error
    assert contexts[0].closed and events.count("close") == 1
    assert len([item for item in events if isinstance(item, tuple) and item[0] == "reconcile"]) == 1
    step.close()


@pytest.mark.parametrize("phase", ["before_step", "after_installation", "after_reconcile"])
def test_original_read_interrupt_prevents_more_work_and_closes_owned_context(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str,
) -> None:
    api = _api()
    events, contexts = _loaders(monkeypatch, tmp_path,
        on_installation=request_read_interrupt if phase == "after_installation" else None,
        on_reconcile=request_read_interrupt if phase == "after_reconcile" else None)
    step = api.minute_study_projection_builder(clock=lambda: NOW)(_manifest(tmp_path))
    if phase == "before_step":
        request_read_interrupt()
    with pytest.raises(ReadInterruptedError):
        step()
    if phase == "before_step":
        assert events == []
    elif phase == "after_installation":
        assert len(events) == 1 and contexts == []
    else:
        assert contexts[0].closed and events[-1] == "close"
    step.close()


def test_builtin_registry_defers_projection_builder_and_domain_imports(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _api()
    from rquant.runtime_service_builtin import build_builtin_registry

    original_import = builtins.__import__

    def guarded_import(name: str, *args: object, **kwargs: object) -> object:
        assert name not in {"rquant.runtime_builder_minute_study_projection",
            "rquant.minute_backtest_parameter_study_projection", "rquant.minute_backtest_installation"}, (
            "projection discovery eagerly imported its builder or installed storage")
        return original_import(name, *args, **kwargs)

    with monkeypatch.context() as scoped:
        scoped.setattr(builtins, "__import__", guarded_import)
        registry = build_builtin_registry(clock=lambda: NOW)
    assert RuntimeServiceKind("minute_study_projection") in registry.registered_kinds
    assert callable(registry.build(_manifest(tmp_path)))
    assert not tuple(tmp_path.iterdir())


def test_registry_preserves_existing_runtime_root_at_watchlist_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _api()
    builtin = import_module("rquant.runtime_service_builtin")
    seen: list[object] = []

    def market(*, adapter_factory: object, universe_loader: object,
        clock: Callable[[], datetime]) -> Callable[[RuntimeServiceManifest], Callable[[], RuntimeStepResult]]:
        seen.append("market")
        return lambda manifest: lambda: RuntimeStepResult()

    def watchlist(*, provider_factory: object, universe_loader: object,
        clock: Callable[[], datetime], runtime_root: Path | None = None,
    ) -> Callable[[RuntimeServiceManifest], Callable[[], RuntimeStepResult]]:
        seen.append(("watchlist", runtime_root))
        return lambda manifest: lambda: RuntimeStepResult()

    monkeypatch.setattr(builtin, "market_minute_source_builder", market)
    monkeypatch.setattr(builtin, "watchlist_quote_source_builder", watchlist)
    builtin.build_builtin_registry(runtime_root=tmp_path, clock=lambda: NOW)
    assert seen == ["market", ("watchlist", tmp_path)]
    assert not tuple(tmp_path.iterdir())


def test_existing_loop_records_projection_receipt_and_closes_step(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    events, contexts = _loaders(monkeypatch, tmp_path)
    clock = lambda: NOW
    step = api.minute_study_projection_builder(clock=clock)(_manifest(tmp_path))
    registry = RuntimeServiceRegistry()
    registry.register(RuntimeServiceKind("minute_study_projection"), lambda manifest: step)
    heartbeat = run_runtime_service_manifest(_manifest(tmp_path), registry=registry,
        control_root=tmp_path / "owned-loop-control", clock=clock, stop_event=Event(), max_iterations=1)
    assert heartbeat.status is RuntimeServiceStatus.STOPPED
    assert heartbeat.output_sequence == 7 and heartbeat.total_successes == 1
    assert heartbeat.backlog_count == 2 and heartbeat.projection_published is True
    assert heartbeat.heartbeat_at == NOW and contexts[0].closed
    with pytest.raises(RuntimeError, match="closed"):
        step()


def test_existing_loop_handles_original_stop_without_recording_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    api = _api()
    stop = Event()
    error = ReadInterruptedError("synthetic original stop")
    events, contexts = _loaders(monkeypatch, tmp_path, error=error, on_reconcile=stop.set)
    step = api.minute_study_projection_builder(clock=lambda: NOW)(_manifest(tmp_path))
    registry = RuntimeServiceRegistry()
    registry.register(RuntimeServiceKind("minute_study_projection"), lambda manifest: step)
    heartbeat = run_runtime_service_manifest(_manifest(tmp_path), registry=registry,
        control_root=tmp_path / "owned-stop-control", clock=lambda: NOW, stop_event=stop, max_iterations=1)
    assert heartbeat.status is RuntimeServiceStatus.STOPPED and heartbeat.total_successes == 0
    assert contexts[0].closed and events.count("close") == 1
