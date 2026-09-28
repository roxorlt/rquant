"""Formula task state and immutable results enter one bounded Serving generation."""

from __future__ import annotations

import os
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from rquant.formula_market_job_projection import (
    FormulaMarketArtifactIndexRow,
    read_formula_market_job_snapshot,
    read_formula_market_result,
)
from rquant.screen.formula_market_jobs import FormulaMarketJobStore, FormulaMarketJobWorker
from rquant.serving_page_projection_source import (
    DuckDBLabPageProjectionSource,
    PageProjectionSourceIntegrityError,
)
from rquant.serving_read_models import (
    ServingProjectionInput,
    ServingProjectionPayload,
    ServingReadModelInput,
    build_serving_read_models,
)
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_formula_market_jobs import Clock, _request, _store
from tests.unit.test_formula_market_run import _history, _market


def _fixture(
    tmp_path: Path,
) -> tuple[FormulaMarketJobStore, Path, tuple[Path, str], tuple[Path, str]]:
    market, history = _market(tmp_path), _history(tmp_path)
    store = _store(tmp_path, Clock())
    research = tmp_path / "research.duckdb"
    with DuckDBStore(research):
        pass
    return store, research, market, history


def _source(store: FormulaMarketJobStore, research: Path) -> DuckDBLabPageProjectionSource:
    return DuckDBLabPageProjectionSource(
        research,
        formula_market_job_state_path=store.state_path,
        formula_market_job_directory=store.artifact_directory,
    )


def _formula_projections(
    store: FormulaMarketJobStore, research: Path
) -> dict[str, ServingProjectionPayload]:
    observed = datetime.now(UTC) + timedelta(minutes=1)
    snapshot = _source(store, research)(observed)
    return {
        item.table_name: item
        for item in snapshot.projections
        if item.table_name
        in {"formula_market_job_state", "formula_market_job", "research_artifact_index"}
    }


def test_missing_and_empty_source_have_explicit_availability_without_source_writes(
    tmp_path: Path,
) -> None:
    store, research, _, _ = _fixture(tmp_path)
    empty = _formula_projections(store, research)
    assert empty["formula_market_job_state"].rows[0]["availability"] == "empty"
    assert empty["formula_market_job"].rows == ()
    assert empty["research_artifact_index"].rows == ()
    assert not Path(f"{store.state_path}-wal").exists()
    assert not Path(f"{store.state_path}-shm").exists()

    store.state_path.unlink()
    unavailable = _formula_projections(store, research)
    assert unavailable["formula_market_job_state"].rows[0]["availability"] == "unavailable"
    assert unavailable["formula_market_job"].rows == ()
    assert unavailable["research_artifact_index"].rows == ()
    assert not store.state_path.exists()


def test_success_is_indexed_without_embedding_matches_and_reader_verifies_file(
    tmp_path: Path,
) -> None:
    store, research, market, history = _fixture(tmp_path)
    task = store.submit(_request(market, history))
    completed = FormulaMarketJobWorker(store).run_one()
    assert completed is not None and completed.status == "succeeded"

    projections = _formula_projections(store, research)
    assert set(projections) == {
        "formula_market_job_state",
        "formula_market_job",
        "research_artifact_index",
    }
    assert len({item.available_at for item in projections.values()}) == 1
    state = projections["formula_market_job_state"].rows[0]
    job = projections["formula_market_job"].rows[0]
    index = projections["research_artifact_index"].rows[0]
    assert (state["availability"], state["total_task_count"], state["retained_task_count"]) == (
        "ready",
        1,
        1,
    )
    assert (job["task_id"], job["status"], job["result_sha256"]) == (
        task.task_id,
        "succeeded",
        completed.result_sha256,
    )
    assert index["artifact_type"] == "formula_market_result"
    assert index["task_id"] == task.task_id
    assert index["content_sha256"] == completed.result_sha256
    assert index["relative_path"].startswith("formula-market-v1-")
    assert index["match_count"] == 2
    assert "match_codes" not in index
    assert index["byte_count"] < 2 * 1024 * 1024
    result = read_formula_market_result(
        store.artifact_directory, FormulaMarketArtifactIndexRow.model_validate(dict(index))
    )
    assert result.summary.match_codes == ("000001.SZ", "830001.BJ")
    tables = build_serving_read_models(
        ServingReadModelInput(
            observed_at=datetime.now(UTC) + timedelta(minutes=1),
            projections=tuple(
                ServingProjectionInput.bind(
                    item, owner_dataset_id="lab_jobs", owner_generation_id="a" * 64
                )
                for item in projections.values()
            ),
        )
    )
    assert len(tables["research_artifact_index"]) == 1


def test_queued_and_failed_tasks_have_no_success_index(tmp_path: Path) -> None:
    store, research, market, history = _fixture(tmp_path)
    task = store.submit(_request(market, history))
    queued = _formula_projections(store, research)
    assert queued["formula_market_job"].rows[0]["status"] == "queued"
    assert queued["research_artifact_index"].rows == ()

    claim = store._claim()
    assert claim is not None
    store._finish_failure(claim, "internal_error")
    failed = _formula_projections(store, research)
    assert failed["formula_market_job"].rows[0]["task_id"] == task.task_id
    assert failed["formula_market_job"].rows[0]["status"] == "failed"
    assert failed["research_artifact_index"].rows == ()


def test_older_success_remains_indexed_when_newer_task_is_queued(tmp_path: Path) -> None:
    store, research, market, history = _fixture(tmp_path)
    first = store.submit(_request(market, history))
    assert FormulaMarketJobWorker(store).run_one() is not None
    second = store.submit(_request(market, history, key="formula-market-0002"))

    projections = _formula_projections(store, research)
    jobs = projections["formula_market_job"].rows
    index = projections["research_artifact_index"].rows
    assert [(row["rank"], row["task_id"], row["status"]) for row in jobs] == [
        (0, second.task_id, "queued"),
        (1, first.task_id, "succeeded"),
    ]
    assert len(index) == 1 and index[0]["task_id"] == first.task_id


def test_concurrent_new_submission_does_not_splice_into_open_source_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _, market, history = _fixture(tmp_path)
    first = store.submit(_request(market, history))
    assert FormulaMarketJobWorker(store).run_one() is not None
    import rquant.formula_market_job_projection as projection

    original = projection._parse_result
    submitted: list[str] = []

    def interleaved(*args: object, **kwargs: object) -> object:
        if not submitted:
            second = store.submit(_request(market, history, key="formula-market-0002"))
            submitted.append(second.task_id)
        return original(*args, **kwargs)

    monkeypatch.setattr(projection, "_parse_result", interleaved)
    with sqlite3.connect(store.state_path) as keeper:
        keeper.execute("PRAGMA journal_mode=WAL")
        keeper.execute("BEGIN")
        keeper.execute("SELECT 1 FROM formula_market_job").fetchone()
        snapshot = read_formula_market_job_snapshot(
            store.state_path,
            store.artifact_directory,
            observed_at=datetime.now(UTC) + timedelta(minutes=1),
        )
    assert submitted
    assert snapshot.state.total_task_count == 1
    assert [(row.task_id, row.status) for row in snapshot.jobs] == [(first.task_id, "succeeded")]
    assert [row.task_id for row in snapshot.artifacts] == [first.task_id]
    assert store.latest() is not None and store.latest().task_id == submitted[0]


def test_recent_task_window_is_bounded_and_reports_older_rows(tmp_path: Path) -> None:
    store, research, market, history = _fixture(tmp_path)
    task_ids: list[str] = []
    for number in range(101):
        receipt = store.submit(_request(market, history, key=f"formula-market-{number:04d}"))
        claim = store._claim()
        assert claim is not None
        store._finish_failure(claim, "internal_error")
        task_ids.append(receipt.task_id)

    projections = _formula_projections(store, research)
    state = projections["formula_market_job_state"].rows[0]
    jobs = projections["formula_market_job"].rows
    assert (state["total_task_count"], state["retained_task_count"], state["has_older_tasks"]) == (
        101,
        100,
        True,
    )
    assert len(jobs) == 100
    assert (jobs[0]["rank"], jobs[0]["task_id"]) == (0, task_ids[-1])
    assert (jobs[-1]["rank"], jobs[-1]["task_id"]) == (99, task_ids[1])
    assert projections["research_artifact_index"].rows == ()


def test_broken_source_schema_is_not_reported_as_an_empty_task_list(tmp_path: Path) -> None:
    store, research, _, _ = _fixture(tmp_path)
    with sqlite3.connect(store.state_path) as connection:
        connection.execute("DROP TABLE formula_market_job")

    with pytest.raises(PageProjectionSourceIntegrityError, match="formula"):
        _formula_projections(store, research)


def test_pure_result_reader_rejects_relative_escape_and_wrong_size(tmp_path: Path) -> None:
    store, research, market, history = _fixture(tmp_path)
    store.submit(_request(market, history))
    assert FormulaMarketJobWorker(store).run_one() is not None
    row = dict(_formula_projections(store, research)["research_artifact_index"].rows[0])

    with pytest.raises(ValueError, match="relative path"):
        FormulaMarketArtifactIndexRow.model_validate(
            {**row, "relative_path": "../" + str(row["relative_path"])}
        )
    with pytest.raises(ValueError, match="byte count"):
        read_formula_market_result(
            store.artifact_directory,
            FormulaMarketArtifactIndexRow.model_validate(
                {**row, "byte_count": int(row["byte_count"]) + 1}
            ),
        )


@pytest.mark.parametrize("damage", ["mutate", "symlink", "hardlink", "delete"])
def test_damaged_success_file_rejects_whole_projection(tmp_path: Path, damage: str) -> None:
    store, research, market, history = _fixture(tmp_path)
    store.submit(_request(market, history))
    assert FormulaMarketJobWorker(store).run_one() is not None
    artifact = next(store.artifact_directory.glob("*.json"))
    if damage == "mutate":
        artifact.write_bytes(artifact.read_bytes().replace(b"000001.SZ", b"000002.SZ"))
    elif damage == "symlink":
        original = artifact.with_suffix(".copy")
        artifact.rename(original)
        artifact.symlink_to(original)
    elif damage == "hardlink":
        os.link(artifact, artifact.with_suffix(".copy"))
    else:
        artifact.unlink()

    with pytest.raises(PageProjectionSourceIntegrityError, match="formula"):
        _formula_projections(store, research)


def test_cross_table_task_reference_and_available_at_cannot_be_spliced(tmp_path: Path) -> None:
    store, research, market, history = _fixture(tmp_path)
    store.submit(_request(market, history))
    assert FormulaMarketJobWorker(store).run_one() is not None
    originals = _formula_projections(store, research)
    index = originals["research_artifact_index"]
    bad_index = ServingProjectionPayload(
        table_name=index.table_name,
        available_at=index.available_at,
        rows=({**dict(index.rows[0]), "task_id": "f" * 32},),
    )
    with pytest.raises(ValueError, match="formula|artifact|task"):
        ServingReadModelInput(
            observed_at=datetime.now(UTC) + timedelta(minutes=1),
            projections=tuple(
                ServingProjectionInput.bind(
                    bad_index if item.table_name == index.table_name else item,
                    owner_dataset_id="lab_jobs",
                    owner_generation_id="a" * 64,
                )
                for item in originals.values()
            ),
        )
    later_index = ServingProjectionPayload(
        table_name=index.table_name,
        available_at=index.available_at + timedelta(seconds=1),
        rows=index.rows,
    )
    with pytest.raises(ValueError, match="formula|artifact|time"):
        ServingReadModelInput(
            observed_at=datetime.now(UTC) + timedelta(minutes=1),
            projections=tuple(
                ServingProjectionInput.bind(
                    later_index if item.table_name == index.table_name else item,
                    owner_dataset_id="lab_jobs",
                    owner_generation_id="a" * 64,
                )
                for item in originals.values()
            ),
        )

    with pytest.raises(ValueError, match="formula|generation"):
        ServingReadModelInput(
            observed_at=datetime.now(UTC) + timedelta(minutes=1),
            projections=tuple(
                ServingProjectionInput.bind(
                    item,
                    owner_dataset_id="lab_jobs",
                    owner_generation_id=("b" if item.table_name == index.table_name else "a") * 64,
                )
                for item in originals.values()
            ),
        )
