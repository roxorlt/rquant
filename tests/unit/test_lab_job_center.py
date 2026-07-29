from __future__ import annotations

import json
import sqlite3
from base64 import urlsafe_b64decode, urlsafe_b64encode
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from rquant.lab_job_protocol import CancelJobCommand, LabCommandEnvelope
from rquant.lab_jobs import (
    LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX,
    LAB_JOB_LIST_QUERY_PARAMETER_MAX,
    MAX_JOB_SHARDS,
    InvalidStoredJobError,
    JobStatus,
    LabJobListFilters,
    LabJobReader,
    LabJobStore,
    ResourceClass,
)
from rquant.lab_shard_protocol import LabShardFailed
from rquant.research_run_spec import ResearchJobType

from .test_lab_finalizer import _ready_scenario
from .test_lab_jobs import NOW, _lease, _register_unprivileged_job_functions, _spec, _submit
from .test_lab_shard_control_plane import _claim, _report, _setup


class _CountingReader(LabJobReader):
    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.statements: list[str] = []

    def _connect(self):  # type: ignore[no-untyped-def]
        connection = super()._connect()
        connection.set_trace_callback(
            lambda statement: self.statements.append(" ".join(statement.split()))
        )
        return connection


def _seed_jobs(tmp_path: Path, count: int) -> tuple[LabJobStore, tuple[UUID, ...]]:
    store = LabJobStore(tmp_path / "lab_jobs.sqlite3")
    store.initialize()
    lease = _lease(store, seconds=10_000)
    job_ids: list[UUID] = []
    for index in range(count):
        job_id = UUID(int=index + 1)
        spec = _spec(
            job_type=(
                ResearchJobType.PARAMETER_SEARCH if index % 2 else ResearchJobType.STRATEGY_REPLAY
            ),
            resource_class=(ResourceClass.HEAVY if index % 3 == 0 else ResourceClass.STANDARD),
        )
        spec = spec.model_copy(
            update={
                "parameters": spec.parameters.model_copy(
                    update={"strategy_name": f"strategy-{index:03d}"}
                )
            }
        )
        envelope = _submit(job_id=job_id, spec=spec)
        receipt = store.apply_command(
            envelope,
            lease=lease,
            now=NOW + timedelta(seconds=index),
        )
        assert receipt.status == "applied"
        job_ids.append(job_id)
    return store, tuple(job_ids)


def test_list_jobs_keyset_pagination_is_stable_bounded_and_has_no_n_plus_one(
    tmp_path: Path,
) -> None:
    store, expected_ids = _seed_jobs(tmp_path, 125)
    reader = _CountingReader(store.path)

    cursor: str | None = None
    observed: list[UUID] = []
    while True:
        page = reader.list_jobs(limit=17, cursor=cursor)
        observed.extend(item.job_id for item in page.items)
        assert page.total_count == 125
        if not page.has_more:
            assert page.next_cursor is None
            break
        assert page.next_cursor is not None
        cursor = page.next_cursor

    assert observed == list(reversed(expected_ids))
    assert len(observed) == len(set(observed)) == 125
    selects = [
        statement for statement in reader.statements if statement.startswith(("SELECT", "WITH"))
    ]
    assert len(selects) == 3 * 8
    assert reader.statements.count("BEGIN") == 8
    assert reader.statements.count("COMMIT") == 8


def test_list_jobs_immutable_cursor_survives_updates_and_live_insert(
    tmp_path: Path,
) -> None:
    store, initial_ids = _seed_jobs(tmp_path, 25)
    reader = LabJobReader(store.path)
    first = reader.list_jobs(limit=7)
    assert first.next_cursor is not None
    assert first.total_count == 25

    leases = reader.list_leases()
    assert len(leases) == 1
    lease = leases[0]
    unseen_id = initial_ids[0]
    unseen = reader.get_job(unseen_id)
    assert unseen is not None
    cancelled = store.apply_command(
        LabCommandEnvelope(
            request_id=uuid4(),
            command=CancelJobCommand(
                job_id=unseen_id,
                expected_version=unseen.version,
                reason="concurrent status update",
            ),
        ),
        lease=lease,
        now=NOW + timedelta(seconds=200),
    )
    inserted_id = UUID(int=10_000)
    inserted = store.apply_command(
        _submit(job_id=inserted_id, spec=_spec()),
        lease=lease,
        now=NOW + timedelta(seconds=201),
    )
    assert cancelled.status == inserted.status == "applied"

    observed = [item.job_id for item in first.items]
    cursor = first.next_cursor
    live_totals: list[int] = []
    while cursor is not None:
        page = reader.list_jobs(limit=7, cursor=cursor)
        observed.extend(item.job_id for item in page.items)
        live_totals.append(page.total_count)
        assert page.has_more is (page.next_cursor is not None)
        cursor = page.next_cursor

    assert observed == list(reversed(initial_ids))
    assert len(observed) == len(set(observed)) == len(initial_ids)
    assert inserted_id not in observed
    assert live_totals and set(live_totals) == {26}


def test_list_jobs_cursor_is_versioned_and_bound_to_filter_identity(tmp_path: Path) -> None:
    store, _ = _seed_jobs(tmp_path, 4)
    reader = LabJobReader(store.path)
    queued_filter = LabJobListFilters(statuses=(JobStatus.QUEUED,))
    first = reader.list_jobs(filters=queued_filter, limit=2)
    assert first.next_cursor is not None

    padding = "=" * (-len(first.next_cursor) % 4)
    payload = json.loads(urlsafe_b64decode(f"{first.next_cursor}{padding}"))
    assert payload["cursor_type"] == "lab_job_list"
    assert payload["schema_version"] == 1
    assert payload["filter_identity"]

    with pytest.raises(ValueError, match="cursor.*filter"):
        reader.list_jobs(
            filters=LabJobListFilters(job_types=(ResearchJobType.PARAMETER_SEARCH,)),
            limit=2,
            cursor=first.next_cursor,
        )

    payload["schema_version"] = 2
    unsupported = (
        urlsafe_b64encode(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        )
        .decode("ascii")
        .rstrip("=")
    )
    with pytest.raises(ValueError, match="cursor"):
        reader.list_jobs(filters=queued_filter, limit=2, cursor=unsupported)


def test_list_jobs_combines_status_type_resource_date_and_keyword_filters(
    tmp_path: Path,
) -> None:
    store, _ = _seed_jobs(tmp_path, 12)
    reader = LabJobReader(store.path)

    page = reader.list_jobs(
        filters=LabJobListFilters(
            statuses=(JobStatus.QUEUED,),
            job_types=(ResearchJobType.PARAMETER_SEARCH,),
            resource_classes=(ResourceClass.STANDARD,),
            created_from=NOW + timedelta(seconds=1),
            created_before=NOW + timedelta(seconds=11),
            keyword="strategy-007",
        ),
        limit=10,
    )

    assert page.total_count == 1
    assert tuple(item.strategy_name for item in page.items) == ("strategy-007",)
    with pytest.raises(ValueError, match="cursor"):
        reader.list_jobs(limit=10, cursor="not-an-opaque-cursor")
    with pytest.raises(ValueError, match="limit"):
        reader.list_jobs(limit=101)


@pytest.mark.parametrize(
    "corrupt_spec",
    (
        '{"parameters":{"strategy_name":"excluded","strategy_name":"needle"}}',
        '{ "parameters":{"strategy_name":"needle"}}',
        '{"parameters":{"strategy_name":7}}',
    ),
)
def test_list_jobs_keyword_fails_closed_before_filtering_corrupt_specs(
    tmp_path: Path,
    corrupt_spec: str,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 2)
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (corrupt_spec, str(job_ids[0])),
        )

    reader = LabJobReader(store.path)
    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.list_jobs(filters=LabJobListFilters(keyword="needle"), limit=1)


def test_list_jobs_keyword_rejects_corrupt_row_beyond_first_page_and_cursor(
    tmp_path: Path,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 4)
    reader = LabJobReader(store.path)
    filters = LabJobListFilters(keyword="strategy")
    first = reader.list_jobs(filters=filters, limit=1)
    assert first.next_cursor is not None
    valid = reader.get_job(job_ids[0])
    assert valid is not None
    noncanonical = json.dumps(valid.spec.model_dump(mode="json"), indent=2, default=str)
    with sqlite3.connect(store.path) as connection:
        _register_unprivileged_job_functions(connection)
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE lab_job SET spec_json = ? WHERE job_id = ?",
            (noncanonical, str(job_ids[0])),
        )

    with pytest.raises(InvalidStoredJobError, match="stored lab job"):
        reader.list_jobs(
            filters=filters,
            limit=1,
            cursor=first.next_cursor,
        )


def test_job_list_filters_canonicalize_enum_tuples_and_bound_sql_parameters() -> None:
    filters = LabJobListFilters(
        statuses=tuple(reversed(tuple(JobStatus))) + tuple(JobStatus),
        job_types=tuple(reversed(tuple(ResearchJobType))) + tuple(ResearchJobType),
        resource_classes=tuple(reversed(tuple(ResourceClass))) + tuple(ResourceClass),
        created_from=NOW,
        created_before=NOW + timedelta(days=1),
        keyword="strategy",
    )

    assert filters.statuses == tuple(sorted(JobStatus, key=lambda item: item.value))
    assert filters.job_types == tuple(sorted(ResearchJobType, key=lambda item: item.value))
    assert filters.resource_classes == tuple(sorted(ResourceClass, key=lambda item: item.value))
    _, parameters = LabJobReader._job_filters_sql(filters)
    assert len(parameters) == LAB_JOB_LIST_FILTER_SQL_PARAMETER_MAX
    assert len(parameters) + 4 == LAB_JOB_LIST_QUERY_PARAMETER_MAX
    assert LabJobReader._job_filters_sql(LabJobListFilters()) == ([], [])


def test_job_list_filters_reject_pathological_raw_tuple_before_sql() -> None:
    with pytest.raises(ValidationError, match="statuses"):
        LabJobListFilters(statuses=(JobStatus.QUEUED,) * 100_000)


def test_job_detail_is_bounded_and_reports_first_failure_without_paused_eta(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=5, max_attempts=3, with_work_plan=True)
    claim = _claim(store, lease)
    failed = store.apply_worker_report(
        _report(
            claim,
            LabShardFailed(failure_json='{"kind":"first"}'),
            offset=4,
        ),
        lease=lease,
        now=NOW + timedelta(seconds=4),
    )
    assert failed.status == "accepted"
    reader = _CountingReader(store.path)

    detail = reader.get_job_detail(
        job_id,
        as_of=NOW + timedelta(seconds=20),
        shard_limit=2,
        event_limit=2,
        artifact_limit=1,
    )

    assert detail is not None
    assert detail.job.status is JobStatus.FAILED
    assert len(detail.shards) == 2
    assert detail.shard_count == 5
    assert detail.shards_truncated is True
    assert len(detail.events) == 2
    assert detail.events_truncated is True
    assert detail.first_failure is not None
    assert detail.first_failure.failure.failure_json == '{"kind":"first"}'
    assert detail.eta is not None and detail.eta.finish_at is None
    assert detail.command_availability.retry is True
    selects = [
        statement for statement in reader.statements if statement.startswith(("SELECT", "WITH"))
    ]
    assert len(selects) <= 13


def test_job_detail_marks_running_heartbeat_stale_and_truncates_independently(
    tmp_path: Path,
) -> None:
    store, lease, job_id = _setup(tmp_path, count=3, with_work_plan=True)
    _claim(store, lease, duration=120)
    reader = _CountingReader(store.path)

    detail = reader.get_job_detail(
        job_id,
        as_of=NOW + timedelta(seconds=40),
        heartbeat_stale_after=timedelta(seconds=10),
        shard_limit=1,
        event_limit=10,
    )

    assert detail is not None
    assert detail.heartbeat.active_shards == 1
    assert detail.heartbeat.stale is True
    assert detail.progress.phase == "strategy_replay"
    assert detail.shards_truncated is True
    assert any(f"LIMIT {MAX_JOB_SHARDS + 1}" in statement for statement in reader.statements)


def test_eta_and_detail_fail_closed_on_damaged_oversized_remaining_shard_graph(
    tmp_path: Path,
) -> None:
    store, job_ids = _seed_jobs(tmp_path, 1)
    timestamp = NOW.isoformat(timespec="microseconds")
    with sqlite3.connect(store.path) as connection:
        connection.executemany(
            """
            INSERT INTO lab_shard (
                shard_id, job_id, shard_index, status, version,
                attempt_count, max_attempts, created_at, updated_at
            ) VALUES (?, ?, ?, 'queued', 0, 0, 3, ?, ?)
            """,
            (
                (
                    str(UUID(int=10_000 + index)),
                    str(job_ids[0]),
                    index,
                    timestamp,
                    timestamp,
                )
                for index in range(MAX_JOB_SHARDS + 1)
            ),
        )

    eta_reader = _CountingReader(store.path)
    with pytest.raises(InvalidStoredJobError, match="shard limit"):
        eta_reader.get_eta_input(job_ids[0], as_of=NOW)
    eta_selects = [
        statement for statement in eta_reader.statements if statement.startswith("SELECT")
    ]
    assert len(eta_selects) == 2
    assert any(f"LIMIT {MAX_JOB_SHARDS + 1}" in statement for statement in eta_selects)
    assert not any("completion_sequence FROM lab_shard" in statement for statement in eta_selects)
    with pytest.raises(InvalidStoredJobError, match="shard limit"):
        eta_reader.list_shards(job_ids[0])

    detail_reader = _CountingReader(store.path)
    with pytest.raises(InvalidStoredJobError, match="shard limit"):
        detail_reader.get_job_detail(
            job_ids[0],
            as_of=NOW,
            shard_limit=1,
            event_limit=1,
            artifact_limit=1,
        )
    detail_selects = [
        statement
        for statement in detail_reader.statements
        if statement.startswith(("SELECT", "WITH"))
    ]
    assert len(detail_selects) <= 13


def test_list_finalization_candidates_is_typed_readonly_and_bounded(tmp_path: Path) -> None:
    scenario = _ready_scenario(tmp_path, hold_days=(1,))
    reader = _CountingReader(scenario.store.path)

    page = reader.list_finalization_candidates(limit=1)

    assert tuple(item.job_id for item in page.items) == (scenario.job_id,)
    assert page.has_more is False
    assert reader.statements.count("BEGIN") == 1
    assert reader.statements.count("COMMIT") == 1
    reader.execute_for_test("SELECT 1")
    with pytest.raises(Exception, match="readonly|read-only|query_only"):
        reader.execute_for_test("DELETE FROM lab_job")
