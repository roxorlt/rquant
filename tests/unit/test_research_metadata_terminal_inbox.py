from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from rquant.data_metadata import (
    DataAuditRun,
    DataAuditRunFinalization,
    DatasetSnapshot,
    DatasetSnapshotArtifact,
    DatasetSnapshotBinding,
    DatasetSnapshotBindingFinalization,
    DatasetSnapshotBindingManifest,
    DatasetSnapshotFinalization,
)
from rquant.research_metadata_terminal_inbox import (
    ResearchMetadataTerminalCommand,
    ResearchMetadataTerminalCommandProcessor,
    ResearchMetadataTerminalInbox,
)
from rquant.storage.duckdb import DuckDBStore

NOW = datetime(2026, 8, 3, 8, 0, tzinfo=UTC)


def _commands() -> tuple[ResearchMetadataTerminalCommand, ResearchMetadataTerminalCommand]:
    audit = DataAuditRun.create(
        as_of_date=date(2026, 8, 2),
        range_start=date(2026, 8, 1),
        range_end=date(2026, 8, 2),
        rule_set_version="metadata-inbox/v1",
        observed_at=NOW - timedelta(minutes=3),
    )
    snapshot = DatasetSnapshot.create(
        strategy_name="n_shape",
        as_of_time=NOW - timedelta(minutes=3),
        code_commit="a" * 40,
        origin="metadata-inbox/v1",
        created_at=NOW - timedelta(minutes=3),
    )
    binding = DatasetSnapshotBinding.create(
        manifest=DatasetSnapshotBindingManifest(
            snapshot_id=snapshot.snapshot_id,
            strategy_name=snapshot.strategy_name,
            start_date=date(2026, 8, 1),
            end_date=date(2026, 8, 2),
            as_of_time=snapshot.as_of_time,
            code_commit=snapshot.code_commit,
            dependency_contract_version="stage1-v1",
            builder_version="metadata-inbox/v1",
            artifacts=(
                DatasetSnapshotArtifact(
                    artifact_type="materialized_table",
                    dataset_id="daily_bar",
                    table_name="daily_bar",
                    artifact_key="daily_bar:2026-08-01:2026-08-02",
                    relative_path="tables/daily_bar.parquet",
                    row_count=1,
                    schema_hash="1" * 64,
                    content_hash="2" * 64,
                    file_hash="3" * 64,
                ),
            ),
        ),
        artifact_root="/srv/rquant/research-lake",
        manifest_relative_path="snapshots/metadata-inbox.json",
        created_at=NOW - timedelta(minutes=2),
    )
    return (
        ResearchMetadataTerminalCommand(
            kind="audit_completed",
            submitted_at=NOW,
            audit_run=audit,
            audit_finalization=DataAuditRunFinalization(p0_count=0, completed_at=NOW),
        ),
        ResearchMetadataTerminalCommand(
            kind="snapshot_ready",
            submitted_at=NOW,
            snapshot=snapshot,
            snapshot_finalization=DatasetSnapshotFinalization(completed_at=NOW),
            snapshot_binding=binding,
            snapshot_binding_finalization=DatasetSnapshotBindingFinalization(completed_at=NOW),
        ),
    )


def test_typed_metadata_inbox_replays_terminal_facts_once_after_restart(tmp_path: Path) -> None:
    inbox = ResearchMetadataTerminalInbox(tmp_path / "metadata-inbox")
    audit, snapshot = _commands()
    assert inbox.submit(audit)
    assert inbox.submit(snapshot)
    assert not inbox.submit(audit)

    database = tmp_path / "operational.duckdb"
    claimed = inbox.claim_next(limit=1)
    assert len(claimed) == 1
    assert claimed[0].command_id in {audit.command_id, snapshot.command_id}
    assert ResearchMetadataTerminalCommandProcessor(
        inbox=ResearchMetadataTerminalInbox(tmp_path / "metadata-inbox"),
        database_path=database,
    ).run_once() == 2
    assert ResearchMetadataTerminalCommandProcessor(
        inbox=ResearchMetadataTerminalInbox(tmp_path / "metadata-inbox"),
        database_path=database,
    ).run_once() == 0

    with DuckDBStore(database, read_only=True) as store:
        assert store.get_data_audit_run(audit.audit_run.audit_run_id).status == "completed"  # type: ignore[union-attr]
        assert store.get_dataset_snapshot(snapshot.snapshot.snapshot_id).status == "ready"  # type: ignore[union-attr]
        assert store.get_dataset_snapshot_binding(snapshot.snapshot.snapshot_id).status == "ready"  # type: ignore[union-attr]
    assert inbox.pending_count() == 0
