"""Self-consistent hashes cannot replace independent journal statistics."""

import gc
import os
import time
import tracemalloc
import weakref
from pathlib import Path

import pytest

from rquant.factor.member_archive import _read_file
from rquant.factor.result_artifact import _open_private_root
from rquant.runtime_contracts import canonical_sha256
from tests.unit.test_factor_stream_job_authority import _sealed


def _hashed(model: object, **updates: object) -> object:
    fields = model.model_dump(mode="python", exclude={"sha256"})
    fields.update(updates)
    return type(model)(**fields, sha256=canonical_sha256(fields))


def _replace_full(
    full: object, root: Path, completion: object, *, result: object, days: object = None
) -> object:
    from rquant.factor.stream_job_artifact import (
        _with_digest,
        checked_from_full,
        project_factor_stream_display,
        publish_stream_artifact,
    )

    journal = _with_digest(
        type(full.journal),
        dict(
            spec_sha256=full.spec.spec_sha256,
            result_sha256=result.sha256,
            days=full.journal.days if days is None else days,
        ),
    )
    ref = publish_stream_artifact(root, "journal", journal)
    forged = _with_digest(
        type(full), dict(spec=full.spec, result=result, journal=journal, journal_reference=ref)
    )
    full_ref = publish_stream_artifact(root, "full", forged)
    display_ref = publish_stream_artifact(root, "display", project_factor_stream_display(forged))
    return checked_from_full(forged, full_ref, display_ref, completion.completed_at)


@pytest.mark.parametrize("damage", ["ic", "group", "cumulative", "turnover", "decay"])
def test_rehashed_wrong_numbers_are_rejected_by_independent_replay(
    tmp_path: Path, damage: str
) -> None:
    from rquant.factor.decay_stream import _summary_day
    from rquant.factor.evaluate import FactorEvaluation
    from rquant.factor.job_ledger import FactorLedgerCompletionError
    from rquant.factor.stream_job_artifact import verify_factor_stream_artifacts
    from rquant.factor.summary import summarize_factor_ic

    with _sealed(tmp_path) as (ledger, _, claim, completion, root, members, *_):
        full = verify_factor_stream_artifacts(claim.job.spec, completion, root, members).full
        research = full.result.research.research
        stats, decay = research.statistics, full.result.research.decay
        if damage == "decay":
            period = decay.periods[0]
            day = period.days[0]
            bad_day = day.model_copy(
                update={"normal_ic": day.normal_ic.model_copy(update={"value": 0.125})}
            )
            days = (bad_day, *period.days[1:])
            summary = summarize_factor_ic(
                FactorEvaluation(days=tuple(_summary_day(d) for d in days))
            )
            bad_period = period.model_copy(update={"days": days, "ic_summary": summary})
            decay = _hashed(decay, periods=(bad_period, *decay.periods[1:]))
        else:
            day = stats.days[0]
            if damage == "ic":
                day = day.model_copy(
                    update={
                        "evaluation": day.evaluation.model_copy(
                            update={
                                "normal_ic": day.evaluation.normal_ic.model_copy(
                                    update={"value": 0.125}
                                )
                            }
                        )
                    }
                )
            else:
                grouping = day.portfolio_groupings[0]
                field = {
                    "group": "period_return",
                    "cumulative": "cumulative_return",
                    "turnover": "target_weight_turnover",
                }[damage]
                group = grouping.groups[0].model_copy(update={field: 0.125})
                grouping = grouping.model_copy(update={"groups": (group, *grouping.groups[1:])})
                day = day.model_copy(
                    update={"portfolio_groupings": (grouping, *day.portfolio_groupings[1:])}
                )
            stats = _hashed(stats, days=(day, *stats.days[1:]))
        result = _hashed(
            full.result,
            research=_hashed(
                full.result.research, research=_hashed(research, statistics=stats), decay=decay
            ),
        )
        forged = _replace_full(full, root, completion, result=result)
        assert forged.artifact_sha256 != completion.artifact_sha256
        with pytest.raises(FactorLedgerCompletionError, match="cannot be verified"):
            ledger.prepare_stream_completion(
                claim.job.job_id, claim.lease_token, forged, root, members
            )
        assert ledger.get(claim.job.job_id).status == "running" and not ledger._prepared


def test_forged_selection_with_recomputed_journal_and_outputs_refuses_real_member_files(
    tmp_path: Path,
) -> None:
    from rquant.factor.daily_stream import FactorDailyStreamBatch, evaluate_factor_daily_stream
    from rquant.factor.decay_stream import FactorICDecayStream
    from rquant.factor.job_ledger import FactorLedgerCompletionError
    from rquant.factor.stream_job_artifact import (
        FactorStreamJournalDay,
        publish_stream_artifact,
        verify_factor_stream_artifacts,
    )

    with _sealed(tmp_path) as (ledger, _, claim, completion, root, members, *_):
        full = verify_factor_stream_artifacts(claim.job.spec, completion, root, members).full
        descriptor = _open_private_root(root)
        try:
            batches = [
                FactorDailyStreamBatch.model_validate_json(
                    _read_file(
                        descriptor, day.artifact.filename, 16 * 1024 * 1024, day.artifact.sha256
                    )[0]
                )
                for day in full.journal.days
            ]
        finally:
            os.close(descriptor)
        first = batches[0]
        batches[0] = first.model_copy(
            update={"universe": first.universe.model_copy(update={"input_sha256": "0" * 64})}
        )
        decay = FactorICDecayStream(full.result.research.decay.request)

        def replay() -> object:
            for batch in batches:
                yield batch
                decay.consume(batch)

        stats = evaluate_factor_daily_stream(
            full.result.research.research.statistics.request, replay()
        )
        decay_result = decay.finish(stats)
        decay.close()
        raw = full.result.research.research.adapter_completion
        returns = tuple(
            day.model_copy(update={"statistics_batch_sha256": sha})
            for day, sha in zip(raw.return_days, stats.batch_sha256s, strict=True)
        )
        input_fields = raw.model_dump(mode="python", exclude={"input_sha256", "sha256"})
        input_fields["return_days"] = returns
        input_sha = canonical_sha256(input_fields)
        research = _hashed(
            full.result.research.research,
            statistics=stats,
            adapter_completion=_hashed(raw, return_days=returns, input_sha256=input_sha),
        )
        result = _hashed(
            full.result,
            research=_hashed(full.result.research, research=research, decay=decay_result),
        )
        days = tuple(
            FactorStreamJournalDay(
                trade_date=b.universe.trade_date,
                artifact=publish_stream_artifact(root, "journal-day", b),
                batch_sha256=canonical_sha256(b),
            )
            for b in batches
        )
        forged = _replace_full(full, root, completion, result=result, days=days)
        with pytest.raises(FactorLedgerCompletionError) as error:
            ledger.prepare_stream_completion(
                claim.job.job_id, claim.lease_token, forged, root, members
            )
        assert "selection differs" in str(error.value.__cause__)


def test_byte_budget_is_enforced_before_json_parser(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import stream_job_artifact as artifacts
    from rquant.factor.job_ledger import FactorLedgerCompletionError

    with _sealed(tmp_path) as (ledger, _, claim, completion, root, members, *_):
        path = root / completion.artifact_filename
        with path.open("r+b") as output:
            output.truncate(artifacts.MAX_STREAM_FULL_BYTES + 1)
        monkeypatch.setattr(
            artifacts, "strict_canonical_json_loads", lambda *args: pytest.fail("oversized parse")
        )
        with pytest.raises(FactorLedgerCompletionError):
            ledger.prepare_stream_completion(
                claim.job.job_id, claim.lease_token, completion, root, members
            )


def test_public_display_lookup_rejects_invalid_digest_before_open(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from rquant.factor import stream_job_artifact as artifacts

    monkeypatch.setattr(
        artifacts, "_open_private_root", lambda *a: pytest.fail("invalid digest opened files")
    )
    with pytest.raises(ValueError, match="SHA-256"):
        artifacts.load_factor_stream_display(Path("/unused"), "../outside")


def test_journal_consumption_releases_batches_and_records_only_bounded_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from rquant.factor import stream_job_artifact as artifacts
    from rquant.factor.stream_job_runner import run_factor_stream_job
    from tests.unit.test_factor_member_stream import _archive
    from tests.unit.test_factor_stream_adapter import _pools, _prepared
    from tests.unit.test_factor_stream_job_spec import _spec

    refs, counts = [], []
    original = artifacts.FactorStreamJournalWriter.consume

    def consume(writer: object, batch: object) -> None:
        refs.extend((weakref.ref(batch), weakref.ref(batch.universe)))
        original(writer, batch)
        counts.append(len(writer._days))
        assert all(not hasattr(day, "factor_values") for day in writer._days)

    monkeypatch.setattr(artifacts.FactorStreamJournalWriter, "consume", consume)
    with _prepared(tmp_path, calculation=tuple(range(1, 18)), evaluation=tuple(range(2, 17))) as (
        metadata,
        lake,
        request,
    ):
        members, ref, request = _archive(tmp_path, request, _pools(request))
        root = tmp_path / "artifacts"
        root.mkdir(mode=0o700)
        spec = _spec(tmp_path, request, members, ref)
        completion = run_factor_stream_job(
            spec,
            metadata_store=metadata,
            lake_root=lake,
            member_root=members,
            artifact_root=root,
            now=lambda: request.formula.as_of,
        )
        tracemalloc.start()
        started = time.perf_counter()
        artifacts.verify_factor_stream_artifacts(spec, completion, root, members)
        elapsed = time.perf_counter() - started
        gc.collect()
        retained, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        assert len(refs) == 30 and all(ref() is None for ref in refs)
        assert max(counts) == 15
        assert not list((lake / ".execution_sessions").iterdir())
        print(
            "SJ_JOURNAL_RELEASE: evaluation_days=15 metadata_max=15 released=30/30 "
            "execution_copies_clean=true"
        )
        print(
            f"SJ_JOURNAL_COST: stocks=12 days=15 seconds={elapsed:.6f} "
            f"python_peak_bytes={peak} python_retained_bytes={retained} rss_measured=false"
        )
