"""Saved real source and member files can compile and execute a persisted run."""

import gc
import json
import os
import subprocess
import sys
import time
import tracemalloc
import weakref
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from rquant.factor.definition import build_factor_definition
from rquant.factor.expression import FeatureCatalog
from rquant.factor.job_ledger import FactorEvaluationJobLedger
from rquant.factor.member_archive import FactorMemberArchiveRequest, publish_factor_member_archive
from rquant.factor.registry import (
    FactorDefinitionRegistry,
    FactorHeadRef,
    SaveFactorDefinitionRequest,
)
from rquant.factor.run_configuration import (
    FactorRunConfiguration,
    FactorRunFileReference,
    FactorRunMemberBinding,
    open_factor_run_configuration,
    run_configured_factor_worker,
    save_factor_prepared_source,
    save_factor_run_configuration,
)
from rquant.factor.run_plan import compile_factor_run_plan
from rquant.factor.run_request import FactorRunParameters, FactorRunRequest
from rquant.factor.source_prepare import prepare_factor_stream_source
from rquant.factor.universe import DailyIndexConstituentBatch
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_factor_member_archive import _payload, _private, _write
from tests.unit.test_factor_source_prepare import _AS_OF, _CODES, _FIRST, _replica, _request


def _configured(
    tmp_path: Path, *, all_pools: bool = False, selection: str = "all", days: int = 30
) -> tuple[Path, FactorRunFileReference, FactorRunRequest]:
    path = _replica(tmp_path, days=days)
    as_of = max(
        _AS_OF, datetime.combine(_FIRST + timedelta(days=days + 1), datetime.min.time(), UTC)
    )
    source_request = _request(path)
    source_request = source_request.model_copy(
        update={
            "scope": source_request.scope.model_copy(
                update={"as_of_time": as_of, "end_date": _FIRST + timedelta(days=days - 1)}
            )
        }
    )
    lake = tmp_path / "lake"
    with DuckDBStore(tmp_path / "metadata.duckdb") as metadata:
        source = prepare_factor_stream_source(
            source_request,
            metadata_store=metadata,
            lake_root=lake,
            now=lambda: as_of,
        )
    root, members = _private(tmp_path / "config"), _private(tmp_path / "members")
    input_root = _private(tmp_path / "inputs")
    days = source.receipt.calendar_open_days
    archives = []
    for pool in ("all", "hs300", "zz1000", "gem") if all_pools else (selection,):
        for i, day in enumerate(days):
            payload = _payload(day)
            payload["securities"]["observed_at"] = (as_of - timedelta(hours=1)).isoformat()
            if all_pools and i == 10:
                for fact in payload["securities"]["facts"]:
                    fact["is_st"] = True
                    if pool == "gem":
                        fact["board"] = "main"
            if pool in ("hs300", "zz1000"):
                payload["membership"] = DailyIndexConstituentBatch(
                    selection=pool,
                    trade_date=day,
                    source_id=f"actual-{pool}-normalized",
                    source_sha256="d" * 64,
                    source_mode="historical_retrospective",
                    source_kind="daily_complete_membership",
                    observed_at=as_of - timedelta(hours=1),
                    stock_codes=() if all_pools and i == 10 else _CODES,
                ).model_dump(mode="json")
            _write(input_root, f"day{i}.json", payload)
        archive = publish_factor_member_archive(
            FactorMemberArchiveRequest(
                selection=pool, trading_days=days, as_of=as_of, computation_stock_codes=_CODES
            ),
            input_root=input_root,
            daily_filenames=(f"day{i}.json" for i in range(len(days))),
            root=members,
        )
        archives.append(FactorRunMemberBinding(selection=pool, archive=archive))
    registry = FactorDefinitionRegistry(tmp_path / "registry.sqlite")
    registry_identity = registry.initialize()
    definition = build_factor_definition(
        factor_id="entry_test",
        name_zh="试验因子",
        category="technical",
        direction="higher_is_better",
        version=1,
        earliest_available_date=None,
        expression="ts_mean(close, 3)",
        feature_catalog=FeatureCatalog(columns=("close",)),
    )
    saved = registry.save(
        SaveFactorDefinitionRequest(
            command_id="save-entry",
            definition=definition,
            expected_head=None,
        ),
        expected_identity=registry_identity,
    )
    ledger = FactorEvaluationJobLedger(tmp_path / "ledger.sqlite", clock=lambda: _AS_OF)
    identity = ledger.initialize()
    source_ref = save_factor_prepared_source(root, source)
    config = FactorRunConfiguration(
        enabled=True,
        factor_run_users=("alice",),
        registry_identity=registry_identity,
        ledger_identity=identity,
        prepared_source=source_ref,
        lake_root=lake,
        member_root=members,
        artifact_root=_private(tmp_path / "artifacts"),
        members=tuple(archives),
        code_revision="b" * 40,
    )
    reference = save_factor_run_configuration(root, config)
    request = FactorRunRequest(
        command_id=str(uuid4()),
        requested_at=as_of,
        serving_generation_id="c" * 64,
        parameters=FactorRunParameters(
            factor_id=definition.factor_id,
            expected_head=FactorHeadRef(version=1, content_sha256=saved.content_sha256),
            selection=selection,
            start_date=_FIRST + timedelta(days=5),
            end_date=_FIRST + timedelta(days=10),
            holding_sessions=5,
        ),
    )
    return root, reference, request


def test_saved_configuration_compiles_then_real_worker_succeeds(tmp_path: Path) -> None:
    from rquant.factor.run_configuration import open_factor_run_configuration

    root, reference, request = _configured(tmp_path)
    with open_factor_run_configuration(root, reference) as loaded:
        instance = loaded.configuration.registry_identity.instance_id
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
    plan = compile_factor_run_plan(
        root, reference, request, verified_registry_instance_id=instance, clock=lambda: _AS_OF
    )
    assert plan.spec.adapter_request.formula.trading_days[0] == _FIRST + timedelta(days=3)
    assert plan.spec.adapter_request.evaluation_days == (
        _FIRST + timedelta(days=5),
        _FIRST + timedelta(days=10),
    )
    submitted = ledger.submit(request.command_id, plan.spec)
    result = run_configured_factor_worker(root, reference, clock=lambda: _AS_OF)
    assert result.status == "succeeded", result
    assert result.record.job_id == submitted.job_id
    assert result.record.spec == plan.spec
    assert not list((tmp_path / "lake").glob("**/execution-*"))


def test_missing_actual_member_file_disables_pool_and_refuses_compile(tmp_path: Path) -> None:
    from rquant.factor.member_archive import load_factor_member_archive
    from rquant.factor.run_backend import FactorRunPageControlBackend

    root, reference, request = _configured(tmp_path)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    config = backend.configuration()
    assert backend.availability("alice").pools[0].available
    manifest = load_factor_member_archive(config.member_root, config.members[0].archive)
    (config.member_root / manifest.days[-1].filename).unlink()
    pool = backend.availability("alice").pools[0]
    assert not pool.available and pool.reason
    with pytest.raises((OSError, ValueError)):
        backend.compile(request, verified_registry_instance_id=config.registry_identity.instance_id)
    assert not list(config.lake_root.glob(".execution_sessions/*"))


def test_factory_refuses_wrong_head_instance_scope_and_changed_source(tmp_path: Path) -> None:
    from rquant.factor.run_backend import FactorRunPageControlBackend

    root, reference, request = _configured(tmp_path)
    backend = FactorRunPageControlBackend(root, reference, clock=lambda: _AS_OF)
    config = backend.configuration()
    instance = config.registry_identity.instance_id
    with pytest.raises(ValueError, match="来源已变化"):
        backend.compile(request, verified_registry_instance_id="0" * 32)
    changed = request.model_copy(
        update={
            "parameters": request.parameters.model_copy(
                update={
                    "expected_head": FactorHeadRef(version=2, content_sha256="0" * 64),
                }
            )
        }
    )
    with pytest.raises(ValueError, match="版本已变化"):
        backend.compile(changed, verified_registry_instance_id=instance)
    for start, end in (
        (_FIRST, _FIRST + timedelta(days=4)),
        (_FIRST + timedelta(days=5), _FIRST + timedelta(days=29)),
    ):
        changed = request.model_copy(
            update={
                "parameters": request.parameters.model_copy(
                    update={
                        "start_date": start,
                        "end_date": end,
                    }
                )
            }
        )
        with pytest.raises(ValueError, match="预热|收益窗口"):
            backend.compile(changed, verified_registry_instance_id=instance)
    unsupported = request.model_dump()
    unsupported["parameters"]["neutralization"] = "industry"
    with pytest.raises(ValueError, match="行业或市值来源"):
        backend.compile(
            FactorRunRequest.model_validate(unsupported), verified_registry_instance_id=instance
        )
    (root / config.prepared_source.filename).write_bytes(b"{}")
    with pytest.raises(ValueError, match="digest"):
        backend.compile(request, verified_registry_instance_id=instance)
    assert not list(config.lake_root.glob(".execution_sessions/*"))


def test_explicit_cli_saves_crash_recovery_and_worker_use_original_files(tmp_path: Path) -> None:
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.page_control import PageControlConsumer, PageControlOutbox, PageControlService

    root, reference, request = _configured(tmp_path)
    with open_factor_run_configuration(root, reference) as loaded:
        config = loaded.configuration
    for action, expected in (
        ("save-source", config.prepared_source),
        ("save-configuration", reference),
    ):
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "rquant.factor.run_entry",
                action,
                "--root",
                str(root),
                "--input",
                str(root / expected.filename),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert FactorRunFileReference.model_validate_json(result.stdout) == expected
    control_path = tmp_path / "control.sqlite"
    crash_code = """
import os, sys
from pathlib import Path
from rquant.factor.run_configuration import FactorRunFileReference
from rquant.factor.run_request import FactorRunRequest
from rquant.factor.run_backend import FactorRunPageControlBackend
from rquant.page_control import PageControlOutbox, PageControlConsumer, PageControlService
root, reference, request, control = sys.argv[1:]
ref = FactorRunFileReference.model_validate_json(reference)
backend = FactorRunPageControlBackend(Path(root), ref)
outbox = PageControlOutbox(Path(control))
outbox.finish_effect = lambda *args, **kwargs: os._exit(23)
service = PageControlService(outbox=outbox, consumer=PageControlConsumer(
    outbox=outbox, data_dir=Path(control).parent, log_dir=Path(control).parent,
    factor_run_backend=backend))
service._submit_trusted_factor_run(FactorRunRequest.model_validate_json(request),
    authenticated_actor_id='alice',
    verified_registry_instance_id=backend.configuration().registry_identity.instance_id)
"""
    crashed = subprocess.run(
        [
            sys.executable,
            "-c",
            crash_code,
            str(root),
            reference.model_dump_json(),
            request.model_dump_json(),
            str(control_path),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert crashed.returncode == 23, crashed.stderr
    backend = FactorRunPageControlBackend(root, reference)
    outbox = PageControlOutbox(control_path)
    owned, _ = outbox.lookup_factor_run_command(request, authenticated_actor_id="alice")
    assert outbox.effect(request.command_id).status.value == "started"
    service = PageControlService(
        outbox=outbox,
        consumer=PageControlConsumer(
            outbox=outbox,
            data_dir=tmp_path,
            log_dir=tmp_path,
            factor_run_backend=backend,
            clock=lambda: datetime.now(UTC) + timedelta(minutes=10),
        ),
    )
    receipt = service._resume_trusted_factor_run(request, authenticated_actor_id="alice")
    assert receipt.result["spec_sha256"] == owned.spec.spec_sha256
    worker = subprocess.run(
        [
            sys.executable,
            "-m",
            "rquant.factor.run_entry",
            "worker",
            "--root",
            str(root),
            "--reference",
            reference.model_dump_json(),
        ],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert worker.returncode == 0, worker.stderr
    assert json.loads(worker.stdout)["status"] == "succeeded"
    assert json.loads(worker.stdout)["job_id"] == receipt.result["job_id"]
    with open_factor_run_configuration(root, reference) as loaded:
        assert len(loaded.open_ledger(clock=lambda: datetime.now(UTC)).list_recent()) == 1
    assert not list(config.lake_root.glob(".execution_sessions/*"))
    print("CLI receipts: save-source=0 save-configuration=0 crash=23 worker=0; children all reaped")


def test_factory_1024_day_files_release_batches_with_bounded_python_allocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.factor.member_stream import FactorMemberStream

    root, reference, request = _configured(tmp_path, days=1024)
    request = request.model_copy(
        update={
            "parameters": request.parameters.model_copy(
                update={
                    "start_date": _FIRST + timedelta(days=3),
                    "end_date": _FIRST + timedelta(days=1021),
                    "holding_sessions": 1,
                }
            )
        }
    )
    refs = []
    original = FactorMemberStream.__next__

    def tracked(stream: FactorMemberStream) -> object:
        batch = original(stream)
        refs.append(weakref.ref(batch))
        return batch

    monkeypatch.setattr(FactorMemberStream, "__next__", tracked)
    with open_factor_run_configuration(root, reference) as loaded:
        instance = loaded.configuration.registry_identity.instance_id
    gc.collect()
    tracemalloc.start()
    started = time.monotonic()
    try:
        plan = compile_factor_run_plan(
            root,
            reference,
            request,
            verified_registry_instance_id=instance,
            clock=lambda: request.requested_at,
        )
        gc.collect()
        retained, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert len(plan.spec.adapter_request.formula.trading_days) == 1021
    assert len(refs) == 1024 and all(ref() is None for ref in refs)
    assert peak < 128 * 1024 * 1024
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))
    print(
        f"factory only: 1024 source day files x 3 securities; released={len(refs)}; "
        f"retained={retained}; peak={peak}; seconds={time.monotonic() - started:.3f}; "
        "Python allocations, not RSS/provider/7000-load evidence"
    )


def test_factory_cancel_closes_configuration_member_and_execution_copy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.factor import run_configuration
    from rquant.factor.member_stream import FactorMemberStream

    root, reference, request = _configured(tmp_path)
    with open_factor_run_configuration(root, reference) as loaded:
        instance = loaded.configuration.registry_identity.instance_id
    descriptors, streams, refs = [], [], []
    open_root = run_configuration._open_private_root
    original = FactorMemberStream.__next__

    def tracked(path: Path) -> int:
        descriptor = open_root(path)
        descriptors.append(descriptor)
        return descriptor

    def cancel(stream: FactorMemberStream) -> object:
        batch = original(stream)
        streams.append(stream)
        refs.append(weakref.ref(batch))
        del batch
        raise KeyboardInterrupt("cancel after first verified member day")

    monkeypatch.setattr(run_configuration, "_open_private_root", tracked)
    monkeypatch.setattr(FactorMemberStream, "__next__", cancel)
    with pytest.raises(KeyboardInterrupt):
        compile_factor_run_plan(
            root, reference, request, verified_registry_instance_id=instance, clock=lambda: _AS_OF
        )
    gc.collect()
    assert streams and all(stream.closed and stream.completion is None for stream in streams)
    assert all(ref() is None for ref in refs)
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)
    assert not list((tmp_path / "lake").glob(".execution_sessions/*"))


def test_runtime_quality_callbacks_still_require_callables() -> None:
    from pydantic import ValidationError

    from rquant.data_quality import AuditRule, RepairAction

    with pytest.raises(ValidationError) as audit:
        AuditRule(rule_id="example", dataset_id="daily", severity="P0", description="test", check=1)
    assert audit.value.errors()[0]["type"] == "callable_type"
    for count, apply in ((1, lambda store: None), (lambda store: 0, 1)):
        with pytest.raises(ValidationError) as repair:
            RepairAction(action_id="example", description="test", count_affected=count, apply=apply)
        assert repair.value.errors()[0]["type"] == "callable_type"
