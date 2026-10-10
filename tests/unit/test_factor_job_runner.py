"""The factor runner seals only one admitted retrospective result."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from rquant.data_metadata import DatasetSnapshot, DatasetSnapshotBinding
from rquant.factor.display_artifact import (
    load_factor_display_artifact,
    project_factor_display_artifact,
)
from rquant.factor.historical_adapter import (
    HistoricalFactorAdapterRequest,
    HistoricalFactorResearch,
)
from rquant.factor.job_ledger import FactorEvaluationJobLedger
from rquant.factor.job_runner import FactorEvaluationCompletion, run_factor_evaluation_job
from rquant.factor.job_spec import FactorEvaluationJobSpec
from rquant.factor.result_artifact import load_factor_research_artifact
from rquant.factor_snapshot_admission import (
    FactorSnapshotAdmissionDecision,
    FactorSnapshotAdmissionError,
    FactorSnapshotAdmissionRequest,
    FactorSnapshotMetadataStore,
)
from rquant.research_snapshot import FactorDailyBarBatch, FactorReadLease, FactorReadQuery
from rquant.strict_json import canonical_json_bytes
from tests.unit.test_factor_historical_adapter import (
    _EVALUATION_DAYS,
    _FIRST,
    _LAST,
    _STOCKS,
    _admitted,
    _at,
    _definition,
)

_REVISION = "c" * 40


def _spec(snapshot: DatasetSnapshot, binding: DatasetSnapshotBinding) -> FactorEvaluationJobSpec:
    definition = _definition()
    adapter = HistoricalFactorAdapterRequest(
        definition=definition,
        stock_codes=_STOCKS,
        pool_basis="explicit_fixed_list",
        evaluation_days=_EVALUATION_DAYS,
        query_start_date=_FIRST,
        query_end_date=_LAST,
        holding_sessions=5,
        as_of=_at(_LAST, 9, 25),
    )
    return FactorEvaluationJobSpec(
        code_revision=_REVISION,
        admission_request=FactorSnapshotAdmissionRequest(
            snapshot_id=snapshot.snapshot_id,
            binding_hash=binding.binding_hash,
            start_date=_FIRST,
            end_date=_LAST,
            source_mode="historical_retrospective",
        ),
        adapter_request=adapter,
        definition_content_sha256=hashlib.sha256(
            canonical_json_bytes(definition.model_dump(mode="json", round_trip=True))
        ).hexdigest(),
        deadline=snapshot.as_of_time + timedelta(days=1),
    )


def _artifact_root(tmp_path: Path) -> Path:
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o700)
    return root


def _run(
    spec: FactorEvaluationJobSpec,
    store: FactorSnapshotMetadataStore,
    tmp_path: Path,
    *,
    artifact_root: Path,
    now: datetime,
) -> FactorEvaluationCompletion:
    return run_factor_evaluation_job(
        spec,
        metadata_store=store,
        lake_root=tmp_path / "lake",
        artifact_root=artifact_root,
        now=lambda: now,
    )


def test_runner_round_trip_retry_and_single_closed_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        root = _artifact_root(tmp_path)
        real_open = module.open_factor_snapshot_admission
        calls: list[FactorSnapshotAdmissionRequest] = []
        leases: list[FactorReadLease] = []

        @contextmanager
        def counted_open(
            request: FactorSnapshotAdmissionRequest,
            *,
            metadata_store: FactorSnapshotMetadataStore,
            lake_root: Path,
        ) -> Iterator[tuple[FactorReadLease, FactorSnapshotAdmissionDecision]]:
            calls.append(request)
            with real_open(request, metadata_store=metadata_store, lake_root=lake_root) as pair:
                leases.append(pair[0])
                yield pair

        monkeypatch.setattr(module, "open_factor_snapshot_admission", counted_open)
        first = _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        again = _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        artifact = load_factor_research_artifact(root, first.artifact_sha256)
        display = load_factor_display_artifact(root, first.display_artifact_sha256)

        assert first == again
        assert calls == [spec.admission_request, spec.admission_request]
        assert len(leases) == 2
        assert first.spec_sha256 == spec.spec_sha256
        assert first.artifact_sha256 == artifact.content_sha256
        assert first.artifact_filename == f"factor-research-v1-{artifact.content_sha256}.json"
        assert first.artifact_byte_count == (root / first.artifact_filename).stat().st_size
        assert first.display_artifact_filename == f"factor-display-v1-{display.content_sha256}.json"
        assert (
            first.display_artifact_byte_count
            == (root / first.display_artifact_filename).stat().st_size
        )
        assert display == project_factor_display_artifact(artifact)
        assert first.result_sha256 == artifact.research.result.sha256
        assert first.source_sha256 == artifact.research.receipt.source_sha256
        assert first.snapshot_id == snapshot.snapshot_id
        assert first.binding_hash == binding.binding_hash
        assert first.source_mode == "historical_retrospective"
        assert first.research_status == "exploratory"
        assert first.result_kind == "research_diagnostic"
        assert first.completed_at == snapshot.as_of_time.astimezone(UTC)
        assert artifact.code_revision == spec.code_revision
        assert artifact.research.receipt.snapshot_id == first.snapshot_id
        assert len(list(root.iterdir())) == 2
        with pytest.raises(RuntimeError, match="closed"):
            leases[0].query_sse_calendar(
                FactorReadQuery(
                    binding_hash=binding.binding_hash,
                    stock_codes=_STOCKS,
                    start_date=_FIRST,
                    end_date=_LAST,
                    row_limit=100,
                )
            )


def test_runner_rejects_wider_admission_before_opening_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        widened = spec.admission_request.model_copy(
            update={"start_date": spec.adapter_request.query_start_date - timedelta(days=1)}
        )
        forged = FactorEvaluationJobSpec.model_construct(
            **{**spec.__dict__, "admission_request": widened}
        )

        def unexpected_open(*_args: object, **_kwargs: object) -> None:
            pytest.fail("invalid admission opened a snapshot")

        monkeypatch.setattr(module, "open_factor_snapshot_admission", unexpected_open)
        with pytest.raises(ValidationError, match="admission"):
            _run(
                forged,
                store,
                tmp_path,
                artifact_root=_artifact_root(tmp_path),
                now=snapshot.as_of_time,
            )


@pytest.mark.parametrize("offset", (timedelta(), timedelta(seconds=1)))
def test_runner_refuses_expired_job_before_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, offset: timedelta
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        root = _artifact_root(tmp_path)

        def unexpected_open(*_args: object, **_kwargs: object) -> None:
            pytest.fail("expired job opened a snapshot")

        monkeypatch.setattr(module, "open_factor_snapshot_admission", unexpected_open)
        with pytest.raises(TimeoutError, match="deadline"):
            _run(spec, store, tmp_path, artifact_root=root, now=spec.deadline + offset)
        assert list(root.iterdir()) == []


def test_runner_refuses_expiry_before_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        root = _artifact_root(tmp_path)
        instants = iter((snapshot.as_of_time, spec.deadline))

        def unexpected_publish(*_args: object, **_kwargs: object) -> None:
            pytest.fail("expired job attempted artifact publication")

        monkeypatch.setattr(module, "publish_factor_research_artifact", unexpected_publish)
        with pytest.raises(TimeoutError, match="deadline"):
            module.run_factor_evaluation_job(
                spec,
                metadata_store=store,
                lake_root=tmp_path / "lake",
                artifact_root=root,
                now=lambda: next(instants),
            )
        assert list(root.iterdir()) == []


@pytest.mark.parametrize("phase", ("start", "seal"))
def test_runner_requires_aware_clock_at_both_boundaries(tmp_path: Path, phase: str) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = _artifact_root(tmp_path)
        instants = iter(
            (
                snapshot.as_of_time
                if phase == "seal"
                else snapshot.as_of_time.replace(tzinfo=None),
                snapshot.as_of_time.replace(tzinfo=None),
            )
        )
        with pytest.raises(ValueError, match="aware UTC"):
            run_factor_evaluation_job(
                _spec(snapshot, binding),
                metadata_store=store,
                lake_root=tmp_path / "lake",
                artifact_root=root,
                now=lambda: next(instants),
            )
        assert list(root.iterdir()) == []


@pytest.mark.parametrize("damage", ("definition_digest", "definition_version"))
def test_runner_revalidates_full_spec_before_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        if damage == "definition_digest":
            corrupted = FactorEvaluationJobSpec.model_construct(
                **{**dict(spec), "definition_content_sha256": "f" * 64}
            )
        else:
            changed = spec.adapter_request.definition.model_copy(update={"version": 2})
            adapter = HistoricalFactorAdapterRequest.model_construct(
                **{**dict(spec.adapter_request), "definition": changed}
            )
            corrupted = FactorEvaluationJobSpec.model_construct(
                **{**dict(spec), "adapter_request": adapter}
            )

        def unexpected_open(*_args: object, **_kwargs: object) -> None:
            pytest.fail("invalid spec opened a snapshot")

        monkeypatch.setattr(module, "open_factor_snapshot_admission", unexpected_open)
        with pytest.raises(ValidationError, match="definition"):
            _run(
                corrupted,
                store,
                tmp_path,
                artifact_root=_artifact_root(tmp_path),
                now=snapshot.as_of_time,
            )


@pytest.mark.parametrize(
    "failure", ("missing_snapshot", "missing_binding", "changed_binding", "corrupt_artifact")
)
def test_runner_refuses_unadmitted_or_changed_binding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        root = _artifact_root(tmp_path)
        if failure == "missing_snapshot":
            monkeypatch.setattr(store, "get_dataset_snapshot", lambda _id: None)
        elif failure == "missing_binding":
            monkeypatch.setattr(store, "get_dataset_snapshot_binding", lambda _id: None)
        elif failure == "changed_binding":
            original = store.get_dataset_snapshot_binding
            calls = 0

            def changing(snapshot_id: str) -> DatasetSnapshotBinding | None:
                nonlocal calls
                calls += 1
                return original(snapshot_id) if calls == 1 else None

            monkeypatch.setattr(store, "get_dataset_snapshot_binding", changing)
        else:
            artifact = binding.manifest.artifacts[0]
            (tmp_path / "lake" / artifact.relative_path).write_bytes(b"corrupt")
        with pytest.raises(FactorSnapshotAdmissionError):
            _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        assert list(root.iterdir()) == []


def test_runner_refuses_changed_read_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = _artifact_root(tmp_path)
        original = FactorReadLease.query_daily_bars

        def changed(lease: FactorReadLease, query: FactorReadQuery) -> FactorDailyBarBatch:
            batch = original(lease, query)
            return batch.model_copy(
                update={"receipt": batch.receipt.model_copy(update={"binding_hash": "f" * 64})}
            )

        monkeypatch.setattr(FactorReadLease, "query_daily_bars", changed)
        with pytest.raises(ValueError, match="batch differs"):
            _run(
                _spec(snapshot, binding),
                store,
                tmp_path,
                artifact_root=root,
                now=snapshot.as_of_time,
            )
        assert list(root.iterdir()) == []


def test_runner_refuses_adapter_failure_without_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = _artifact_root(tmp_path)

        def failed_adapter(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("injected adapter failure")

        monkeypatch.setattr(module, "assemble_historical_factor_research", failed_adapter)
        with pytest.raises(RuntimeError, match="injected adapter failure"):
            _run(
                _spec(snapshot, binding),
                store,
                tmp_path,
                artifact_root=root,
                now=snapshot.as_of_time,
            )
        assert list(root.iterdir()) == []


@pytest.mark.parametrize(
    "changed_request",
    (
        {"stock_codes": _STOCKS[:2]},
        {"evaluation_days": _EVALUATION_DAYS[:1]},
        {"holding_sessions": 1},
    ),
)
def test_runner_rejects_valid_research_for_a_different_adapter_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, changed_request: dict[str, object]
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = _artifact_root(tmp_path)
        original = module.assemble_historical_factor_research

        def wrong_research(
            lease: FactorReadLease,
            decision: FactorSnapshotAdmissionDecision,
            request: HistoricalFactorAdapterRequest,
        ) -> HistoricalFactorResearch:
            return original(lease, decision, request.model_copy(update=changed_request))

        monkeypatch.setattr(module, "assemble_historical_factor_research", wrong_research)
        with pytest.raises(ValueError, match="job source"):
            _run(
                _spec(snapshot, binding),
                store,
                tmp_path,
                artifact_root=root,
                now=snapshot.as_of_time,
            )
        assert list(root.iterdir()) == []


@pytest.mark.parametrize("root_form", ("missing", "public"))
def test_runner_refuses_untrusted_artifact_root(tmp_path: Path, root_form: str) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = tmp_path / "artifacts"
        if root_form == "public":
            root.mkdir(mode=0o755)
            root.chmod(0o755)
        with pytest.raises((OSError, ValueError)):
            _run(
                _spec(snapshot, binding),
                store,
                tmp_path,
                artifact_root=root,
                now=snapshot.as_of_time,
            )
        if root.exists():
            assert list(root.iterdir()) == []


def test_runner_conflict_keeps_other_trusted_artifact(tmp_path: Path) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = _artifact_root(tmp_path)
        first_spec = _spec(snapshot, binding)
        first = _run(first_spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        second_spec = first_spec.model_copy(update={"code_revision": "d" * 40})
        second = _run(second_spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        occupied = root / second.artifact_filename
        occupied.write_bytes(b"occupied")

        with pytest.raises(ValueError):
            _run(second_spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        assert occupied.read_bytes() == b"occupied"
        assert (
            load_factor_research_artifact(root, first.artifact_sha256).research.result.sha256
            == first.result_sha256
        )


def test_runner_refuses_post_publish_wrong_reload(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = _artifact_root(tmp_path)
        first_spec = _spec(snapshot, binding)
        first = _run(first_spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        prior_artifact = load_factor_research_artifact(root, first.artifact_sha256)
        second_spec = first_spec.model_copy(update={"code_revision": "d" * 40})
        monkeypatch.setattr(
            module, "load_factor_research_artifact", lambda _root, _sha: prior_artifact
        )

        with pytest.raises(ValueError, match="reloaded"):
            _run(second_spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        assert load_factor_research_artifact(root, first.artifact_sha256) == prior_artifact


def test_runner_publish_failure_keeps_prior_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        root = _artifact_root(tmp_path)
        spec = _spec(snapshot, binding)
        first = _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)

        def failed_publish(*_args: object, **_kwargs: object) -> None:
            raise OSError("injected publication interruption")

        monkeypatch.setattr(module, "publish_factor_research_artifact", failed_publish)
        with pytest.raises(OSError, match="publication interruption"):
            _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        assert (
            load_factor_research_artifact(root, first.artifact_sha256).content_sha256
            == first.artifact_sha256
        )


def test_display_publish_failure_keeps_complete_file_for_idempotent_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_runner as module

    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        root = _artifact_root(tmp_path)
        real_publish = module.publish_factor_display_artifact

        def interrupted(*_args: object, **_kwargs: object) -> None:
            raise OSError("injected display publication interruption")

        monkeypatch.setattr(module, "publish_factor_display_artifact", interrupted)
        with pytest.raises(OSError, match="display publication interruption"):
            _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        complete_files = list(root.glob("factor-research-v1-*.json"))
        assert len(complete_files) == 1
        assert list(root.glob("factor-display-v1-*.json")) == []

        monkeypatch.setattr(module, "publish_factor_display_artifact", real_publish)
        receipt = _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        assert complete_files[0].name == receipt.artifact_filename
        assert receipt.display_status == "available"
        assert load_factor_display_artifact(root, receipt.display_artifact_sha256) == (
            project_factor_display_artifact(
                load_factor_research_artifact(root, receipt.artifact_sha256)
            )
        )


def test_admitted_runner_two_artifacts_ledger_success_and_identity_reopen(tmp_path: Path) -> None:
    with _admitted(tmp_path) as (store, _lease, _decision, snapshot, binding):
        spec = _spec(snapshot, binding)
        root = _artifact_root(tmp_path)
        completion = _run(spec, store, tmp_path, artifact_root=root, now=snapshot.as_of_time)
        ledger = FactorEvaluationJobLedger(
            tmp_path / "factor-jobs.sqlite3", clock=lambda: snapshot.as_of_time
        )
        identity = ledger.initialize()
        job = ledger.submit("synthetic-factor-evaluation", spec)
        lease = ledger.claim(lease_seconds=60)
        assert lease is not None
        success = ledger.complete(job.job_id, lease.lease_token, lease.version, completion, root)
        assert success.status == "succeeded"
        assert success.completion.display_status == "available"
        assert (root / completion.artifact_filename).is_file()
        assert (root / completion.display_artifact_filename).is_file()
        reopened = FactorEvaluationJobLedger.open_existing(
            identity, clock=lambda: snapshot.as_of_time
        )
        assert reopened.get(job.job_id) == success
