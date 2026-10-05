"""Separate fixture writer checkpointing from the readonly research window."""

import gc
import hashlib

from tests.unit.test_paper_research_runtime_chain import host, accept_and_change_head


def test_fixture_writer_lifecycle_settles_before_readonly_research_baseline(tmp_path):
    page, backend, runtime, request, scheduler, worker, finalizer, reader, source = host(tmp_path)
    ledger = runtime.ledger_source_for(source.broker)
    paths = (ledger.path, ledger.path.with_name(ledger.path.name+"-wal"), ledger.anchor_path)
    def hashes():
        return tuple(hashlib.sha256(path.read_bytes()).hexdigest() if path is not None and path.exists() else None for path in paths)
    try:
        idle_writer = source.broker._connect()
        writer_stage = hashes()
        before_revision = source.read(as_of=backend.clock()).frame.ledger_revision
        gc.collect()
        readonly_stage = hashes()
        job_id, owned = accept_and_change_head(page, backend, runtime, request, scheduler)
        gc.collect()
        after = hashes()
        assert after == readonly_stage
        assert owned.spec.parameters.strategy_name == "paper_reconcile"
        assert source.read(as_of=backend.clock()).frame.ledger_revision == before_revision
        assert hashes() == readonly_stage
        print(f"WRITER_LIFECYCLE_BEFORE={writer_stage}; SETTLED_READONLY_BASELINE={readonly_stage}; RESEARCH_AFTER={after}; ORIGINAL_LEDGER_REVISION={before_revision}; GC_SETTLED_ORIGINAL_WRITER_ONLY={writer_stage != readonly_stage}")
    finally:
        idle_writer.close()
        worker.close()
        scheduler.release()


def test_cold_wal_mode_source_without_side_file_is_unavailable_without_creating_wal(tmp_path):
    import pytest
    from rquant.paper_broker import PaperBrokerStore, PaperBrokerReconciliationError
    from tests.unit.test_paper_portfolio_ledger_views import filled
    broker, _, _, _ = filled(tmp_path)
    gc.collect()
    wal = broker.path.with_name(broker.path.name+"-wal")
    assert not wal.exists()
    before = hashlib.sha256(broker.path.read_bytes()).hexdigest()
    with pytest.raises(PaperBrokerReconciliationError, match="WAL"):
        with PaperBrokerStore.open_readonly(broker.path, account_id=broker.account_id, initial_cash=broker.initial_cash, cost_policy=broker.cost_policy):
            pytest.fail("cold WAL-mode source lacks safe readonly side-file conditions")
    assert not wal.exists() and hashlib.sha256(broker.path.read_bytes()).hexdigest() == before
