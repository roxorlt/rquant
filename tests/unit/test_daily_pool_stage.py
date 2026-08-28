from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from rquant.daily_canonical_publisher import (
    CanonicalDatabaseIdentity,
    DailyCanonicalPublishReceipt,
)
from rquant.daily_pipeline_ledger import DailyPipelineMode, DailyStageReceipt, StageResult
from rquant.daily_pool_stage import (
    DailyDownstreamArtifactStore,
    DailyDownstreamStageError,
    DailyPoolStageArtifact,
    DailyScreenStageArtifact,
)

NOW = datetime(2026, 8, 3, 9, 1, tzinfo=UTC)
TRADE_DATE = date(2026, 8, 3)


def _canonical_receipt() -> DailyCanonicalPublishReceipt:
    result = StageResult(content_hash="a" * 64, evidence_hash="b" * 64)
    ledger_receipt = DailyStageReceipt(
        mode=DailyPipelineMode.SHADOW,
        run_id="daily-unit",
        stage_id="canonical_publish",
        attempt_number=1,
        input_identity="c" * 64,
        result=result,
        prepared_at=NOW,
    )
    return DailyCanonicalPublishReceipt(
        generation_id="d" * 64,
        trade_date=TRADE_DATE,
        revision=1,
        source_generation_id="e" * 64,
        source_sequence=1,
        source_batch_id="f" * 64,
        raw_content_sha256="1" * 64,
        calendar_generation_id="2" * 64,
        calendar_producer_commit="3" * 40,
        calendar_content_sha256="2" * 64,
        calendar_as_of=NOW,
        database_identity=CanonicalDatabaseIdentity(
            canonical_path="/tmp/canonical.duckdb",
            device=1,
            inode=2,
        ),
        available_at=NOW,
        committed_at=NOW,
        db_content_sha256="4" * 64,
        watermarks=(),
        ledger_fencing_token=1,
        stage_result=result,
        expected_ledger_receipt=ledger_receipt,
    )


def _screen_artifact() -> DailyScreenStageArtifact:
    receipt = _canonical_receipt()
    return DailyScreenStageArtifact(
        canonical_receipt_id=receipt.receipt_id,
        canonical_generation_id=receipt.generation_id,
        trade_date=receipt.trade_date,
        stage_result=StageResult(content_hash="5" * 64, evidence_hash="6" * 64),
        created_at=NOW,
        preset_hits={"n-shape-pool1": 2, "n-shape-pool2": 1},
        errors=(),
    )


def test_screen_artifact_is_content_addressed_and_replay_is_idempotent(
    tmp_path: Path,
) -> None:
    store = DailyDownstreamArtifactStore(tmp_path / "artifacts")
    artifact = _screen_artifact()

    first = store.persist_screen(artifact)
    replay = store.persist_screen(artifact)

    assert first == replay == artifact
    assert store.load_screen(artifact.canonical_receipt_id) == artifact
    assert (tmp_path / "artifacts" / artifact.canonical_receipt_id / "screen.json").is_file()


def test_pool_artifact_rejects_conflicting_replay(tmp_path: Path) -> None:
    receipt = _canonical_receipt()
    store = DailyDownstreamArtifactStore(tmp_path / "artifacts")
    base = DailyPoolStageArtifact(
        canonical_receipt_id=receipt.receipt_id,
        canonical_generation_id=receipt.generation_id,
        trade_date=receipt.trade_date,
        stage_result=StageResult(content_hash="7" * 64, evidence_hash="8" * 64),
        created_at=NOW,
        pool2_added=1,
        pool2_exited=0,
    )
    store.persist_pool(base)

    conflicting = DailyPoolStageArtifact(
        canonical_receipt_id=receipt.receipt_id,
        canonical_generation_id=receipt.generation_id,
        trade_date=receipt.trade_date,
        stage_result=StageResult(content_hash="7" * 64, evidence_hash="8" * 64),
        created_at=NOW,
        pool2_added=2,
        pool2_exited=0,
    )
    with pytest.raises(DailyDownstreamStageError, match="conflicts"):
        store.persist_pool(conflicting)


def test_artifacts_require_a_canonical_receipt_identity(tmp_path: Path) -> None:
    artifact = _screen_artifact().model_copy(update={"canonical_receipt_id": ""})
    with pytest.raises(ValueError):
        DailyDownstreamArtifactStore(tmp_path / "artifacts").persist_screen(artifact)
