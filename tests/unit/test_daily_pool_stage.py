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


def test_original_screen_stage_binds_canonical_authority_in_its_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from contextlib import contextmanager
    from datetime import timedelta
    import rquant.daily_pool_stage as stages
    import rquant.pipeline as pipeline
    from rquant.daily_pipeline_ledger import DailyStageAttempt
    from rquant.storage.duckdb import DuckDBStore
    from tests.unit.test_daily_screen_reproducible import _daily_preset
    from tests.unit.test_screen_dynamic_ma import _world

    _, primary, _, days, _, _ = _world(tmp_path)
    canonical = DailyCanonicalPublishReceipt.model_validate(
        _canonical_receipt().model_dump(exclude={"receipt_id"}) | {"trade_date": days[0]}
    )
    monkeypatch.setattr(pipeline, "PRESET_SCREENS", {"reproducible": _daily_preset()})

    class Fence:
        def assert_current(self, checked_at: datetime, /) -> None:
            assert checked_at == NOW

        def assert_source(self, generation: str, content: str, /) -> None:
            assert generation == canonical.source_generation_id
            assert content == canonical.raw_content_sha256

        def assert_input(self, identity: str, /) -> None:
            assert identity == "c" * 64

    @contextmanager
    def guard(attempt: DailyStageAttempt, checked_at: datetime, /):
        assert attempt.stage_id == "screen" and checked_at == NOW
        yield Fence()

    original = pipeline.run_daily_screen_stage
    observed = []

    def run(trade_date: str, **kwargs):
        assert kwargs["transaction_open"] is True
        output = original(trade_date, **kwargs, preset_directory=tmp_path / "presets")
        proof = kwargs["store"].query_screen_run_evidence(trade_date, "reproducible")
        assert proof.input.canonical_authority.canonical_receipt_id == canonical.receipt_id
        assert proof.input.canonical_authority.canonical_generation_id == canonical.generation_id
        assert proof.input.canonical_authority.source_generation_id == canonical.source_generation_id
        observed.append(proof)
        return output

    monkeypatch.setattr(stages, "run_daily_screen_stage", run)
    stage = stages.DailyScreenStage(
        writer_factory=lambda: DuckDBStore(primary),
        artifact_store=DailyDownstreamArtifactStore(tmp_path / "artifacts"),
        ledger_fence_verifier=guard,
        clock=lambda: NOW,
        canonical_verifier=lambda store, receipt, checked_at: None,
    )
    output = stage.run(
        canonical,
        attempt=DailyStageAttempt(run_id="daily-unit", stage_id="screen", attempt_number=1,
            fencing_token=1, claimed_at=NOW, lease_expires_at=NOW + timedelta(minutes=5)),
        ledger_input_identity="c" * 64, preset_names=["reproducible"],
    )
    assert output.preset_hits == {"reproducible": 1}
    with DuckDBStore(primary) as store:
        assert store.query_screen_run_evidence(days[0].isoformat(), "reproducible") == observed[0]
