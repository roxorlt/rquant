"""Stored facts use the original toggle, causal prefix and incremental worker."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

from rquant.factor.run_configuration import open_factor_run_configuration
from rquant.factor.run_plan import compile_factor_run_plan
from rquant.factor.tracking import summarize_factor_tracking
from rquant.factor.tracking_runner import (
    FactorTrackingRunner,
    _last_run,
    read_factor_tracking_prefix,
)
from tests.unit.test_factor_daily_feature_pipeline import _configured, _tracking_control
from tests.unit.test_factor_daily_feature_source import _AS_OF, _FIRST


def _submit(service: object, backend: object, body: object) -> object:
    return service._submit_trusted_factor_tracking(
        body,
        authenticated_actor_id="alice",
        verified_registry_instance_id=backend.registry_identity.instance_id,
    )


def test_stored_tracking_toggle_incremental_worker_matches_whole_and_serving(
    tmp_path: Path,
) -> None:
    from rquant.factor.run_backend import FactorRunPageControlBackend
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts
    from rquant.factor.tracking_serving import project_factor_tracking_snapshot

    service, backend, _, store, identity, body = _tracking_control(
        tmp_path, expression="ref(ma5, 1) + turnover_rate + close", source_present=True
    )
    assert _submit(service, backend, body).result["tracked"]
    caps = FactorRunPageControlBackend(backend.root, backend.reference).capabilities()
    assert all(f.tracking_supported is True for f in caps.fields if f.value_semantics is not None)
    runner = FactorTrackingRunner(backend.root, backend.reference, identity, clock=lambda: _AS_OF)
    first = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=2))
    assert first.status == "updated" and len(first.evaluation_days) == 1
    before = store.days(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        initial = _last_run(
            store, connection, store.get(body.factor_id, expected_identity=identity).segment_id
        )
    appended = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=4))
    assert appended.status == "updated" and len(appended.evaluation_days) == 2
    days = store.days(body.factor_id, expected_identity=identity)
    assert days[:1] == before
    state = store.get(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        incremental = _last_run(store, connection, state.segment_id)
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        config = loaded.configuration
        ledger = loaded.open_ledger(clock=lambda: _AS_OF)
        job = ledger.lookup_command(
            incremental.plan.request.command_id, incremental.plan.spec.spec_sha256
        )
        verified = verify_factor_stream_artifacts(
            job.spec, job.completion, config.artifact_root, config.member_root
        )
        from rquant.factor.daily_stream import FactorDailyStreamBatch

        def values(original: object) -> dict:
            return {
                entry.trade_date: FactorDailyStreamBatch.model_validate_json(
                    (config.artifact_root / entry.artifact.filename).read_bytes()
                ).factor_values
                for entry in original.full.journal.days
            }

        initial_job = ledger.lookup_command(
            initial.plan.request.command_id, initial.plan.spec.spec_sha256
        )
        initial_verified = verify_factor_stream_artifacts(
            initial_job.spec, initial_job.completion, config.artifact_root, config.member_root
        )
        incremental_values = {**values(initial_verified), **values(verified)}
        for trade_date, points in incremental_values.items():
            offset = (trade_date - _FIRST).days
            for point in points:
                number = int(point.stock_code[:6])
                expected = (
                    11 * offset - 11 + number * 1.01 + 0.5387
                    if number == 1
                    else 21 * offset - 21 + number * 2.01
                )
                assert point.value == pytest.approx(expected)
        assert verified.display.daily_features.fields[0].column == "ma5"
    cancelled = _submit(
        service,
        backend,
        body.model_copy(
            update={
                "command_id": "cancel",
                "tracked": False,
                "expected_tracking_generation": state.generation,
            }
        ),
    )
    _submit(
        service,
        backend,
        body.model_copy(
            update={
                "command_id": "rejoin",
                "expected_tracking_generation": cancelled.result["tracking_generation"],
            }
        ),
    )
    whole = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=4))
    assert whole.status == "updated"
    assert store.days(body.factor_id, expected_identity=identity) == days
    current = store.get(body.factor_id, expected_identity=identity)
    with store._connection(identity) as connection:
        full = _last_run(store, connection, current.segment_id)
    assert full.prefix == incremental.prefix
    whole_job = ledger.lookup_command(full.plan.request.command_id, full.plan.spec.spec_sha256)
    whole_verified = verify_factor_stream_artifacts(
        whole_job.spec, whole_job.completion, config.artifact_root, config.member_root
    )
    assert values(whole_verified) == incremental_values
    snapshot = project_factor_tracking_snapshot(
        identity, registry_identity=backend.registry_identity, available_at=_AS_OF
    )
    assert snapshot.panels[0].summary == summarize_factor_tracking(days)
    assert (
        runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=4)).status
        == "waiting"
    )
    assert store.days(body.factor_id, expected_identity=identity) == days
    assert not list(config.lake_root.glob(".daily-feature-*"))


def test_legacy_six_field_prefix_golden_ignores_unused_stored_configuration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module
    from rquant.factor.run_configuration import save_factor_run_configuration

    root, reference, request, _, config, _ = _configured(tmp_path, expression="close")
    plan = compile_factor_run_plan(
        root,
        reference,
        request,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )

    def forbidden(*args: object, **kwargs: object) -> object:
        pytest.fail("unused stored source must not open a reader")

    monkeypatch.setattr(module, "open_factor_daily_feature_source", forbidden)
    expected = (
        "1bcfac6a56fa20c2055525f0e461db37a52f9e42b00618150f783d19f14bf1c2",
        "0ceefa073710dd691f215bf4008c9f644094c1cd6944af25c9309c9fdc2d49ec",
    )
    for selected in (
        reference,
        save_factor_run_configuration(
            root, config.model_copy(update={"daily_feature_source": None})
        ),
    ):
        with open_factor_run_configuration(root, selected) as loaded:
            prefix, _ = module.read_factor_tracking_prefix(loaded, plan.spec)
        assert tuple(day.sha256 for day in prefix) == expected
    assert plan.spec.adapter_request.daily_feature_source is None


def test_stored_missing_artifact_refuses_start_before_enqueue_but_cancel_remains_available(
    tmp_path: Path,
) -> None:
    service, backend, outbox, store, identity, body = _tracking_control(
        tmp_path, expression="ma5", source_present=True
    )
    started = _submit(service, backend, body)
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        artifact = (
            loaded.configuration.lake_root / loaded.daily_features.tables[0].artifact.relative_path
        )
        lake = loaded.configuration.lake_root
    artifact.unlink()
    cancelled = _submit(
        service,
        backend,
        body.model_copy(
            update={
                "command_id": "cancel-missing",
                "tracked": False,
                "expected_tracking_generation": started.result["tracking_generation"],
            }
        ),
    )
    rejoin = body.model_copy(
        update={
            "command_id": "start-missing",
            "expected_tracking_generation": cancelled.result["tracking_generation"],
        }
    )
    with pytest.raises((OSError, ValueError)):
        _submit(service, backend, rejoin)
    assert cancelled.result["tracked"] is False and outbox.receipt(rejoin.command_id) is None
    assert not store.get(body.factor_id, expected_identity=identity).tracked
    assert not list(lake.glob(".daily-feature-*"))


def _refresh(tmp_path: Path, root: Path, reference: object) -> object:
    from rquant.factor.daily_feature_source import (
        FactorDailyFeaturePrepareRequest,
        prepare_factor_daily_feature_source,
    )
    from rquant.factor.run_configuration import (
        save_factor_daily_feature_source,
        save_factor_prepared_source,
        save_factor_run_configuration,
    )
    from rquant.factor.source_prepare import prepare_factor_stream_source
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_factor_source_prepare import _sidecar

    with open_factor_run_configuration(root, reference) as loaded:
        config, request = loaded.configuration, loaded.source.receipt.request
    _sidecar(request.replica_path)
    with DuckDBStore(tmp_path / "refresh-metadata.duckdb") as metadata:
        prepared = prepare_factor_stream_source(
            request, metadata_store=metadata, lake_root=config.lake_root, now=lambda: _AS_OF
        )
    stored = prepare_factor_daily_feature_source(
        FactorDailyFeaturePrepareRequest(prepared_source=prepared),
        lake_root=config.lake_root,
        now=lambda: _AS_OF + timedelta(minutes=5),
    )
    return save_factor_run_configuration(
        root,
        config.model_copy(
            update={
                "prepared_source": save_factor_prepared_source(root, prepared),
                "daily_feature_source": save_factor_daily_feature_source(root, stored),
            }
        ),
    )


def test_stored_prefix_generation_refresh_keeps_logical_values(tmp_path: Path) -> None:
    root, reference, request, _, config, _ = _configured(tmp_path)
    plan = compile_factor_run_plan(
        root,
        reference,
        request,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    with open_factor_run_configuration(root, reference) as loaded:
        before, _ = read_factor_tracking_prefix(loaded, plan.spec)
        old_sha = loaded.daily_features.sha256
    refreshed = _refresh(tmp_path, root, reference)
    plan = compile_factor_run_plan(
        root,
        refreshed,
        request,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    with open_factor_run_configuration(root, refreshed) as loaded:
        after, _ = read_factor_tracking_prefix(loaded, plan.spec)
        assert loaded.daily_features.sha256 != old_sha
    assert after == before


@pytest.mark.parametrize("revision", ("value", "missing_to_null", "null_to_nan", "nan_to_infinity"))
def test_stored_history_value_or_state_revision_pauses_without_append(
    tmp_path: Path, revision: str
) -> None:
    import duckdb

    service, backend, _, store, identity, body = _tracking_control(
        tmp_path, expression="ma5 * 0 + close", source_present=True
    )
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        replica = loaded.source.receipt.request.replica_path
        config = loaded.configuration
    date = _FIRST + timedelta(days=1)
    with duckdb.connect(str(replica)) as raw:
        if revision == "missing_to_null":
            raw.execute(
                "DELETE FROM daily_indicator WHERE ts_code='000002.SZ' AND trade_date=?", [date]
            )
        elif revision in ("null_to_nan", "nan_to_infinity"):
            raw.execute(
                "UPDATE daily_indicator SET ma5=? WHERE ts_code='000002.SZ' AND trade_date=?",
                [None if revision == "null_to_nan" else float("nan"), date],
            )
    backend.reference = _refresh(tmp_path, backend.root, backend.reference)
    _submit(service, backend, body)
    runner = FactorTrackingRunner(backend.root, backend.reference, identity, clock=lambda: _AS_OF)
    assert (
        runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=2)).status
        == "updated"
    )
    before = store.days(body.factor_id, expected_identity=identity)
    cursor = store.get(body.factor_id, expected_identity=identity).cursor
    with duckdb.connect(str(replica)) as raw:
        if revision == "missing_to_null":
            raw.execute(
                "INSERT INTO daily_indicator(ts_code,trade_date,ma5) VALUES ('000002.SZ',?,NULL)",
                [date],
            )
        else:
            value = (
                99.0
                if revision == "value"
                else float("nan")
                if revision == "null_to_nan"
                else float("inf")
            )
            raw.execute(
                "UPDATE daily_indicator SET ma5=? WHERE ts_code='000002.SZ' AND trade_date=?",
                [value, date],
            )
    runner.reference = _refresh(tmp_path, backend.root, backend.reference)
    outcome = runner.run_history(body.factor_id, target_end=_FIRST + timedelta(days=4))
    assert outcome.status == "paused"
    assert store.days(body.factor_id, expected_identity=identity) == before
    assert store.get(body.factor_id, expected_identity=identity).cursor == cursor
    assert not list((config.lake_root / ".execution_sessions").iterdir())


def test_stored_prefix_opens_paired_reader_and_witnesses_both_artifacts(tmp_path: Path) -> None:
    root, reference, request, _, config, source = _configured(tmp_path)
    plan = compile_factor_run_plan(
        root,
        reference,
        request,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    with open_factor_run_configuration(root, reference) as loaded:
        prefix, witness = read_factor_tracking_prefix(loaded, plan.spec)
    assert tuple(p.trade_date for p in prefix) == plan.spec.adapter_request.formula.trading_days
    paths = {p for p, _ in witness.files}
    assert all(config.lake_root / t.artifact.relative_path in paths for t in source.tables)
    assert not list(config.lake_root.glob(".daily-feature-*"))


def test_stored_prefix_all_sixteen_states_use_actual_previous_sse_and_keep_missing_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import math

    import duckdb

    import rquant.factor.tracking_runner as module
    from tests.unit.test_factor_daily_feature_source import _BASIC, _FIELDS, _INDICATOR

    closed, following = _FIRST + timedelta(days=2), _FIRST + timedelta(days=3)

    def mutate(connection: object) -> None:
        connection.execute("UPDATE trade_calendar SET is_open=FALSE WHERE cal_date=?", [closed])
        connection.execute(
            "UPDATE trade_calendar SET pretrade_date=? WHERE cal_date=?",
            [_FIRST + timedelta(days=1), following],
        )
        for table in ("daily_indicator", "daily_basic"):
            connection.execute(
                f"DELETE FROM {table} WHERE ts_code='000001.SZ' AND trade_date=?", [following]
            )

    expression = "ref(ma5, 1) + " + " + ".join(f for f in _FIELDS if f != "ma5")
    root, reference, request, prepared, config, _ = _configured(
        tmp_path, count=8, mutate=mutate, expression=expression
    )
    request = request.model_copy(
        update={
            "parameters": request.parameters.model_copy(
                update={"start_date": following, "end_date": following + timedelta(days=1)}
            )
        }
    )
    plan = compile_factor_run_plan(
        root,
        reference,
        request,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    logical = []
    digest = module.canonical_sha256

    def observe(value: object) -> str:
        if isinstance(value, tuple) and value[0] == "factor-tracking-input-v1":
            logical.append(value[-1])
        return digest(value)

    monkeypatch.setattr(module, "canonical_sha256", observe)
    with open_factor_run_configuration(root, reference) as loaded:
        prefix, _ = module.read_factor_tracking_prefix(loaded, plan.spec)
    assert tuple(day.trade_date for day in prefix) == (
        _FIRST + timedelta(days=1),
        following,
        following + timedelta(days=1),
    )
    assert tuple(item[3] for item in logical) == (_FIRST, _FIRST + timedelta(days=1), following)
    with duckdb.connect(str(prepared.receipt.request.replica_path), read_only=True) as raw:
        for tag, columns, trade_date, panel_date, rows in logical:
            assert tag == "stored-daily-fields-v1" and columns == _FIELDS
            assert trade_date != closed and panel_date != closed
            values = {}
            for table, fields in (("daily_indicator", _INDICATOR), ("daily_basic", _BASIC)):
                for row in raw.execute(
                    f"SELECT ts_code,{','.join(fields)} FROM {table} WHERE trade_date=?",
                    [panel_date],
                ).fetchall():
                    values.update(
                        ((row[0], column), value)
                        for column, value in zip(fields, row[1:], strict=True)
                    )
            for code, facts in rows:
                for column, actual in zip(columns, facts, strict=True):
                    if (code, column) not in values:
                        expected = ("missing", None, None)
                    else:
                        value = values[code, column]
                        expected = (
                            ("null", None, None)
                            if value is None
                            else ("valid", value, None)
                            if math.isfinite(value)
                            else (
                                "non_finite",
                                None,
                                "NaN"
                                if math.isnan(value)
                                else "Infinity"
                                if value > 0
                                else "-Infinity",
                            )
                        )
                    assert actual == expected
    assert all(fact[0] == "missing" for fact in dict(logical[-1][-1])["000001.SZ"])
    assert not list(config.lake_root.glob(".daily-feature-*"))


def test_stored_prefix_queries_only_dependencies_in_five_hundred_code_chunks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureReadLease

    root, reference, request, _, config, _ = _configured(tmp_path, count=501, expression="ma5")
    plan = compile_factor_run_plan(
        root,
        reference,
        request,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    queries, leases = [], []
    query = FactorDailyFeatureReadLease.query

    def observed(lease: object, request: object) -> object:
        queries.append(request)
        leases.append(lease)
        assert request.fields == ("ma5",) and len(request.stock_codes) <= 500
        return query(lease, request)

    monkeypatch.setattr(FactorDailyFeatureReadLease, "query", observed)
    with open_factor_run_configuration(root, reference) as loaded:
        module_prefix, _ = read_factor_tracking_prefix(loaded, plan.spec)
    assert len(queries) == 2 * len(module_prefix)
    assert {len(q.stock_codes) for q in queries} == {1, 500}
    assert all(lease.closed and not lease._private_root.exists() for lease in leases)


def test_stored_tracking_artifact_change_after_final_prefix_does_not_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import rquant.factor.tracking_runner as module

    service, backend, _, store, identity, body = _tracking_control(
        tmp_path, expression="ma5", source_present=True
    )
    _submit(service, backend, body)
    with open_factor_run_configuration(backend.root, backend.reference) as loaded:
        path = (
            loaded.configuration.lake_root / loaded.daily_features.tables[0].artifact.relative_path
        )
        lake = loaded.configuration.lake_root
    verify = module.verify_factor_stream_artifacts
    calls = []

    def changed(*args: object, **kwargs: object) -> object:
        verified = verify(*args, **kwargs)
        assert not list(lake.glob(".daily-feature-*"))
        calls.append(True)
        path.write_bytes(path.read_bytes() + b"changed after final-prefix close")
        return verified

    monkeypatch.setattr(module, "verify_factor_stream_artifacts", changed)
    outcome = module.FactorTrackingRunner(
        backend.root, backend.reference, identity, clock=lambda: _AS_OF
    ).run_history(body.factor_id, target_end=_FIRST + timedelta(days=2))
    assert calls == [True] and outcome.status == "waiting"
    assert store.get(body.factor_id, expected_identity=identity).cursor is None
    assert not store.days(body.factor_id, expected_identity=identity)
    assert not list(lake.glob(".daily-feature-*"))
    assert not list((lake / ".execution_sessions").iterdir())


def test_stored_prefix_source_natural_tail_failure_closes_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor.daily_feature_source import FactorDailyFeatureReadLease

    root, reference, request, _, config, source = _configured(tmp_path, expression="ma5")
    plan = compile_factor_run_plan(
        root,
        reference,
        request,
        verified_registry_instance_id=config.registry_identity.instance_id,
        clock=lambda: _AS_OF,
    )
    path = config.lake_root / source.tables[0].artifact.relative_path
    leases = []
    query = FactorDailyFeatureReadLease.query

    def changed(lease: object, request: object) -> object:
        batch = query(lease, request)
        leases.append(lease)
        if request.trade_date == _FIRST + timedelta(days=2):
            path.write_bytes(path.read_bytes() + b"changed while private copy is read")
        return batch

    monkeypatch.setattr(FactorDailyFeatureReadLease, "query", changed)
    with open_factor_run_configuration(root, reference) as loaded, pytest.raises(ValueError):
        read_factor_tracking_prefix(loaded, plan.spec)
    assert leases and all(lease.closed and not lease._private_root.exists() for lease in leases)
    assert not list(config.lake_root.glob(".daily-feature-*"))
