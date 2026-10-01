"""Real member files and a frozen raw source drive one journalled execution."""

from pathlib import Path

import pytest

from tests.unit.test_factor_member_stream import _archive
from tests.unit.test_factor_stream_adapter import _pools, _prepared
from tests.unit.test_factor_stream_job_spec import _spec


def test_actual_execution_journals_and_independently_replays_statistics(tmp_path: Path) -> None:
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts
    from rquant.factor.stream_job_runner import run_factor_stream_job

    with _prepared(tmp_path) as (metadata, lake, request):
        member_root, reference, request = _archive(tmp_path, request, _pools(request))
        spec = _spec(tmp_path, request, member_root, reference)
        artifacts = tmp_path / "artifacts"
        artifacts.mkdir(mode=0o700)
        completion = run_factor_stream_job(
            spec,
            metadata_store=metadata,
            lake_root=lake,
            member_root=member_root,
            artifact_root=artifacts,
            now=lambda: request.formula.as_of,
        )
        verified = verify_factor_stream_artifacts(spec, completion, artifacts, member_root)
        assert verified.full.result.research.research.statistics.days
        assert verified.full.result.research.decay.periods
        assert verified.full.journal.processed_days == len(request.evaluation_days)
        assert not list((lake / ".execution_sessions").iterdir())


@pytest.mark.parametrize(
    "selection,label",
    [
        ("all", "全市场（沪深非 ST）"),
        ("gem", "创业板与科创板"),
        ("hs300", "沪深300"),
        ("zz1000", "中证1000"),
    ],
)
def test_file_driven_job_worker_serving_and_web_with_real_pool_labels(
    tmp_path: Path, selection: str, label: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import job_ledger, stream_job_artifact
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.job_worker import run_one_factor_job
    from rquant.factor.result_serving import (
        project_factor_result_projections,
        validate_factor_result_projections,
    )
    from tests.support.web_proxy_identity import ResearcherTestClient
    from tests.support.web_serving_fixture import FIXTURE_BUILT_AT, build_web_fixture
    from tests.unit.test_factor_job_ledger import _Clock
    from tests.unit.test_web_factor_results import _app

    with _prepared(tmp_path, selection=selection) as (metadata, lake, request):
        codes, days = request.formula.computation_stock_codes, request.formula.trading_days
        memberships = {day: codes for day in days}
        memberships[days[1]], memberships[days[2]] = codes[:6], ()
        pools = list(_pools(request, memberships))
        if selection == "gem":
            pools = [
                pool.model_copy(
                    update={
                        "securities": pool.securities.model_copy(
                            update={
                                "facts": tuple(
                                    fact.model_copy(
                                        update={
                                            "board": "gem"
                                            if fact.stock_code in memberships[pool.trade_date]
                                            else "main"
                                        }
                                    )
                                    for fact in pool.securities.facts
                                )
                            }
                        )
                    }
                )
                for pool in pools
            ]
        member_root, reference, request = _archive(tmp_path, request, pools)
        spec = _spec(tmp_path, request, member_root, reference)
        authority, root = tmp_path / "authority", tmp_path / "artifacts"
        authority.mkdir(mode=0o700)
        root.mkdir(mode=0o700)
        clock = _Clock(request.formula.as_of)
        ledger = FactorEvaluationJobLedger(authority / "jobs.sqlite3", clock=clock)
        identity = ledger.initialize()
        job = ledger.submit("run", spec)
        receipt = run_one_factor_job(
            ledger,
            metadata_store=metadata,
            lake_root=lake,
            artifact_root=root,
            member_root=member_root,
            runner_now=clock,
        )
        assert receipt.status == "succeeded"
        assert ledger.submit("replay", spec).job_id == job.job_id
        monkeypatch.setattr(
            job_ledger,
            "verify_factor_stream_artifacts",
            lambda *a: pytest.fail("Serving replayed statistics"),
        )
        monkeypatch.setattr(
            stream_job_artifact,
            "evaluate_factor_daily_stream",
            lambda *a: pytest.fail("Serving replayed statistics"),
        )
        group = project_factor_result_projections(identity, root, available_at=FIXTURE_BUILT_AT)
        snapshot = validate_factor_result_projections({p.table_name: p for p in group})
        assert snapshot.displays[0].pool_label == label
        assert snapshot.displays[0].coverage_days[1].coverage.expected_count == 0
        serving = tmp_path / "serving"
        build_web_fixture(serving, "baseline", factor_result_projections=group)
        with ResearcherTestClient(_app(serving)) as client:
            response = client.get(f"/api/v1/factors/results/{job.job_id}")
            assert response.status_code == 200, response.text
            view = response.json()["data"]["research"]
            assert view["schema_version"] == 2 and view["pool_label"] == label
            assert len(view["decay_periods"]) == 10
            assert "members" not in str(view["portfolio_days"])
        assert not ledger._prepared and not list((lake / ".execution_sessions").iterdir())


@pytest.mark.parametrize("failure", ["tail_corrupt", "observer_error", "cancel"])
def test_failure_after_first_processed_batch_has_no_completion_and_cleans_resources(
    tmp_path: Path, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    import os
    import threading

    from rquant.factor import stream_job_artifact as artifacts
    from rquant.factor.job_ledger import FactorEvaluationJobLedger
    from rquant.factor.job_worker import run_one_factor_job
    from rquant.factor.member_archive import load_factor_member_archive
    from tests.unit.test_factor_job_ledger import _Clock

    with _prepared(tmp_path) as (metadata, lake, request):
        members, ref, request = _archive(tmp_path, request, _pools(request))
        spec = _spec(tmp_path, request, members, ref)
        root, authority = tmp_path / "artifacts", tmp_path / "authority"
        root.mkdir(mode=0o700)
        authority.mkdir(mode=0o700)
        clock = _Clock(request.formula.as_of)
        ledger = FactorEvaluationJobLedger(authority / "jobs.sqlite3", clock=clock)
        ledger.initialize()
        job = ledger.submit("failure", spec)
        original, descriptors = artifacts.FactorStreamJournalWriter.consume, []
        tail = load_factor_member_archive(members, ref).days[-1].filename

        def consume(writer: object, batch: object) -> None:
            descriptors.append(writer._fd)
            original(writer, batch)
            if len(writer._days) == 1:
                if failure == "tail_corrupt":
                    (members / tail).write_bytes(b"{}")
                elif failure == "observer_error":
                    raise ValueError("synthetic processed batch observer failure")
                else:
                    raise KeyboardInterrupt("synthetic cancellation")

        monkeypatch.setattr(artifacts.FactorStreamJournalWriter, "consume", consume)
        kwargs = dict(
            metadata_store=metadata,
            lake_root=lake,
            artifact_root=root,
            member_root=members,
            runner_now=clock,
        )
        if failure == "cancel":
            with pytest.raises(KeyboardInterrupt):
                run_one_factor_job(ledger, **kwargs)
            assert ledger.get(job.job_id).status == "running"
        else:
            assert run_one_factor_job(ledger, **kwargs).status == "failed"
        for descriptor in descriptors:
            with pytest.raises(OSError):
                os.fstat(descriptor)
        assert not list(root.glob("factor-stream-journal-v2-*"))
        assert not list(root.glob("factor-stream-full-v2-*"))
        assert not list(root.glob(".*tmp*"))
        assert not ledger._prepared and not list((lake / ".execution_sessions").iterdir())
        assert not any(
            t.name == "factor-job-heartbeat" and t.is_alive() for t in threading.enumerate()
        )
        print(
            f"SJ_FAILURE_RESOURCES: failure={failure} completed=false fd_closed=true "
            "threads_joined=true execution_copies_clean=true"
        )
