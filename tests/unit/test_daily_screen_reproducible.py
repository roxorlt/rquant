"""Daily writer uses the screening domain's original math and persisted proof."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from ta.momentum import RSIIndicator

from rquant.screen.daily_inputs import prepare_daily_screen_inputs
from rquant.screen.rules import above_ma, gt, has_prior_limit_up
from rquant.storage.duckdb import DuckDBStore
from tests.unit.test_screen_dynamic_ma import _CODE, _expected, _world
import rquant.pipeline as pipeline
from rquant.builtin_presets import ScreenPreset
from rquant.llm.schemas import RuleCall
from rquant.screen.pool_ranking import PoolRankingCondition, PoolRankingPlan


def _daily_preset() -> ScreenPreset:
    return ScreenPreset(
        name="reproducible", description="daily contract", rules=[gt("MA7[0]", 0)],
        rule_calls=[RuleCall(name="gt", args={"left": "MA7[0]", "right": 0})],
        definition_version="a" * 64,
        ranking=PoolRankingPlan(conditions=(PoolRankingCondition(metric="PCT_CHG[0]",ascending=False,weight=100),),top_n=10),
    )


def test_daily_dynamic_ma_and_full_history_rsi_match_original_math(tmp_path: Path) -> None:
    _, primary, _, days, prices, factors = _world(tmp_path, days=100)
    with DuckDBStore(primary) as store:
        inputs = prepare_daily_screen_inputs(
            days[0].isoformat(), [above_ma(7), gt("RSI7[0]", 0)], store=store
        )
    row = inputs.frame.set_index("ts_code").loc[_CODE]
    assert row["MA7[0]"] == pytest.approx(_expected(prices, factors, 7, 0))
    adjusted = pd.Series([p * a for p, a in zip(prices, factors, strict=True)][::-1])
    assert row["RSI7[0]"] == pytest.approx(RSIIndicator(adjusted, window=7).rsi().iloc[-1])
    assert inputs.evidence.trade_date == days[0]
    assert inputs.evidence.method == "daily-screen-inputs/v1"
    assert len(inputs.evidence.content_digest) == 64
    assert "CLOSE[99]" not in inputs.frame.columns


def test_daily_missing_per_stock_dependency_is_unknown(tmp_path: Path) -> None:
    _, primary, _, days, _, _ = _world(tmp_path, with_new_stock=True)
    with DuckDBStore(primary) as store:
        inputs = prepare_daily_screen_inputs(days[0].isoformat(), [above_ma(7)], store=store)
    assert inputs.evidence.universe_count == 2
    assert inputs.evidence.unknown_count == 1
    assert pd.isna(inputs.frame.set_index("ts_code").loc["600002.SH", "MA7[0]"])


def test_daily_input_identity_changes_with_actual_selected_fact(tmp_path: Path) -> None:
    _, primary, _, days, _, _ = _world(tmp_path)
    with DuckDBStore(primary) as store:
        first = prepare_daily_screen_inputs(days[0].isoformat(), [above_ma(7)], store=store)
        store._conn.execute("UPDATE daily_bar SET close=123 WHERE ts_code=? AND trade_date=?", [_CODE, days[1]])
        second = prepare_daily_screen_inputs(days[0].isoformat(), [above_ma(7)], store=store)
    assert first.evidence.content_digest != second.evidence.content_digest
    assert first.evidence.source_identity != second.evidence.source_identity


def test_actual_daily_writer_commits_rank_and_input_evidence(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, primary, _, days, _, _ = _world(tmp_path)
    monkeypatch.setattr(pipeline, "PRESET_SCREENS", {"reproducible": _daily_preset()})
    with DuckDBStore(primary) as store:
        result = pipeline.run_daily_screen_stage(days[0].isoformat(), preset_names=["reproducible"], store=store, preset_directory=tmp_path / "presets")
        assert result.preset_hits == {"reproducible": 1}
        proof = store.query_screen_run_evidence(days[0].isoformat(), "reproducible")
        receipt = store.query_screen_run_receipt(days[0].isoformat(), "reproducible")
        assert proof is not None and receipt is not None
        assert proof.result_version == receipt.result_version
        assert proof.hit_count == 1
        import json
        extra = json.loads(store.query_screen_result(days[0].isoformat(), "reproducible").iloc[0].extra)
        assert extra["rank_position"] == 1 and extra["ranking_score"] == 100
        store._conn.execute("UPDATE screen_result SET extra='{\"rank_position\":1,\"ranking_score\":0}' WHERE preset_name='reproducible'")
        with pytest.raises(ValueError, match="evidence differs"):
            store.query_screen_run_evidence(days[0].isoformat(), "reproducible")


def test_daily_fundamental_inputs_bind_fixed_pit_and_future_is_unknown(tmp_path: Path) -> None:
    from tests.unit.test_fundamental_daily import _conn, _derive, _finance, _valuation, MONDAY, SYMBOL
    from rquant.financial_pit_acquisition import FinancialArchive
    from datetime import UTC, datetime
    primary = tmp_path / "pit.duckdb"
    with _conn(primary) as conn:
        _finance(conn, FinancialArchive(tmp_path / "archive"), observed_at=datetime(2026,9,29,8,tzinfo=UTC))
        _valuation(conn)
        _derive(conn)
        conn.execute("INSERT INTO daily_bar(ts_code,trade_date,close,pct_chg) VALUES (?,?,10,1)",[SYMBOL,MONDAY])
    with DuckDBStore(primary) as store:
        inputs = prepare_daily_screen_inputs(MONDAY.isoformat(),[gt("PE_TTM[0]",0),gt("ROE[0]",0)],store=store)
    assert inputs.frame.iloc[0]["PE_TTM[0]"] == 10
    assert pd.isna(inputs.frame.iloc[0]["ROE[0]"])
    assert inputs.evidence.unknown_count == 1
    assert len(inputs.evidence.fundamental_versions) == 1


def test_daily_failure_at_evidence_write_keeps_previous_rows_and_both_receipts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, primary, _, days, _, _ = _world(tmp_path)
    monkeypatch.setattr(pipeline,"PRESET_SCREENS",{"reproducible":_daily_preset()})
    with DuckDBStore(primary) as store:
        first=pipeline.run_daily_screen_stage(days[0].isoformat(),preset_names=["reproducible"],store=store,preset_directory=tmp_path/"presets")
        assert first.errors == ()
        old_rows=store.query_screen_result(days[0].isoformat(),"reproducible")
        old_receipt=store.query_screen_run_receipt(days[0].isoformat(),"reproducible")
        old_evidence=store.query_screen_run_evidence(days[0].isoformat(),"reproducible")
        def fail(_proof: object) -> None:
            raise OSError("synthetic evidence disk failure")
        monkeypatch.setattr(store,"_upsert_screen_run_evidence",fail)
        second=pipeline.run_daily_screen_stage(days[0].isoformat(),preset_names=["reproducible"],store=store,preset_directory=tmp_path/"presets")
        assert second.preset_hits == {"reproducible":-1}
        pd.testing.assert_frame_equal(old_rows,store.query_screen_result(days[0].isoformat(),"reproducible"))
        assert store.query_screen_run_receipt(days[0].isoformat(),"reproducible") == old_receipt
        assert store.query_screen_run_evidence(days[0].isoformat(),"reproducible") == old_evidence


def test_daily_all_fundamental_source_missing_is_failure_not_zero(tmp_path: Path) -> None:
    _, primary, _, days, _, _ = _world(tmp_path)
    with DuckDBStore(primary) as store:
        with pytest.raises(ValueError,match="source is unavailable"):
            prepare_daily_screen_inputs(days[0].isoformat(),[gt("PE_TTM[0]",0)],store=store)


def test_daily_unknown_has_proof_and_cannot_claim_complete_lineage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _, primary, _, days, _, _ = _world(tmp_path,with_new_stock=True)
    preset=_daily_preset()
    preset.ranking=None
    monkeypatch.setattr(pipeline,"PRESET_SCREENS",{"reproducible":preset})
    with DuckDBStore(primary) as store:
        result=pipeline.run_daily_screen_stage(days[0].isoformat(),preset_names=["reproducible"],store=store,preset_directory=tmp_path/"presets")
        assert result.errors == ()
        proof=store.query_screen_run_evidence(days[0].isoformat(),"reproducible")
        receipt=store.query_screen_run_receipt(days[0].isoformat(),"reproducible")
        assert proof is not None and proof.input.unknown_count == 1
        assert receipt is not None and not receipt.lineage_complete


def test_daily_aggregate_uses_original_window_and_reports_missing_fact(tmp_path: Path) -> None:
    _,primary,_,days,_,_=_world(tmp_path,days=150)
    with DuckDBStore(primary) as store:
        inputs=prepare_daily_screen_inputs(days[0].isoformat(),[has_prior_limit_up(window=120)],store=store)
    assert inputs.evidence.unknown_count == 1


def test_real_writer_evidence_publisher_rejects_persisted_rank_tamper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from datetime import UTC,datetime,timedelta
    from zoneinfo import ZoneInfo
    from rquant.serving_page_projection_source import DuckDBSignalPageProjectionSource
    _,primary,_,days,_,_=_world(tmp_path)
    target=datetime.now(UTC).astimezone(ZoneInfo("Asia/Shanghai")).date()-timedelta(days=1)
    shift=(target-days[0]).days
    monkeypatch.setattr(pipeline,"PRESET_SCREENS",{"reproducible":_daily_preset()})
    with DuckDBStore(primary) as store:
        for table in ("daily_bar","daily_indicator","adj_factor"):
            store._conn.execute(f"UPDATE {table} SET trade_date=trade_date+CAST(? AS INTEGER)",[shift])
        store._conn.execute("UPDATE trade_calendar SET cal_date=cal_date+CAST(? AS INTEGER)",[shift])
        result=pipeline.run_daily_screen_stage(target.isoformat(),preset_names=["reproducible"],store=store,preset_directory=tmp_path/"presets")
        assert result.errors == ()
    source=DuckDBSignalPageProjectionSource(primary)
    before={item.table_name:item for item in source(datetime.now(UTC)+timedelta(seconds=1)).projections}
    assert len(before["screen_run_evidence"].rows) == 1
    assert before["screen_run_evidence"].rows[0]["hit_count"] == 1
    with DuckDBStore(primary) as store:
        store._conn.execute("UPDATE screen_result SET extra='{\"rank_position\":1,\"ranking_score\":0}' WHERE preset_name='reproducible'")
    after={item.table_name:item for item in source(datetime.now(UTC)+timedelta(seconds=1)).projections}
    assert after["screen_run_evidence"].rows == ()


def test_writer_authority_publication_requires_original_receipt_and_current_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import hashlib
    from datetime import UTC, datetime, timedelta
    from zoneinfo import ZoneInfo
    from rquant.daily_canonical_publisher import DailyCanonicalPublishReceipt, _PUBLICATION_DDL, _RECEIPT_DDL
    from rquant.pool_result_receipt import DailyScreenAuthority
    from rquant.runtime_contracts import canonical_sha256
    from rquant.serving_page_projection_source import DuckDBSignalPageProjectionSource
    from tests.unit.test_daily_pool_stage import _canonical_receipt

    _, primary, _, days, _, _ = _world(tmp_path)
    now = datetime.now(UTC)
    target = now.astimezone(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=1)
    canonical = DailyCanonicalPublishReceipt.model_validate(
        _canonical_receipt().model_dump(exclude={"receipt_id"}) |
        {"trade_date":target,"available_at":now,"committed_at":now}
    )
    authority = DailyScreenAuthority(trade_date=target,canonical_receipt_id=canonical.receipt_id,
        canonical_generation_id=canonical.generation_id,source_generation_id=canonical.source_generation_id,
        available_at=canonical.available_at)
    monkeypatch.setattr(pipeline,"PRESET_SCREENS",{"reproducible":_daily_preset()})
    shift = (target - days[0]).days
    with DuckDBStore(primary) as store:
        for table in ("daily_bar","daily_indicator","adj_factor"):
            store._conn.execute(f"UPDATE {table} SET trade_date=trade_date+CAST(? AS INTEGER)",[shift])
        store._conn.execute("UPDATE trade_calendar SET cal_date=cal_date+CAST(? AS INTEGER)",[shift])
        result = pipeline.run_daily_screen_stage(target.isoformat(),preset_names=["reproducible"],store=store,
            preset_directory=tmp_path/"presets",canonical_authority=authority)
        assert result.errors == ()
    source = DuckDBSignalPageProjectionSource(primary)
    read = lambda: {item.table_name:item for item in source(datetime.now(UTC)+timedelta(seconds=1)).projections}
    assert read()["screen_run_evidence"].rows == ()
    with DuckDBStore(primary) as store:
        store._conn.execute(_PUBLICATION_DDL)
        store._conn.execute(_RECEIPT_DDL)
        payload = canonical.model_dump_json()
        ledger = canonical.expected_ledger_receipt
        store._conn.execute("INSERT INTO daily_canonical_publish_receipt VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",[
            canonical.receipt_id,canonical.generation_id,ledger.run_id,ledger.stage_id,ledger.attempt_number,
            canonical.ledger_fencing_token,ledger.receipt_id,ledger.input_identity,canonical.db_content_sha256,
            canonical_sha256(canonical.watermarks),canonical.receipt_id,hashlib.sha256(payload.encode()).hexdigest(),payload,
        ])
        store._conn.execute("INSERT INTO daily_canonical_publication VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",[
            canonical.generation_id,target,1,None,canonical.source_generation_id,canonical.source_sequence,
            canonical.source_batch_id,canonical.raw_content_sha256,canonical.available_at,canonical.committed_at,
            canonical.db_content_sha256,"[]",canonical.receipt_id,canonical.database_identity.model_dump_json(),True,
        ])
    rows = read()["screen_run_evidence"].rows
    assert len(rows) == 1 and rows[0]["canonical_receipt_id"] == canonical.receipt_id
    with DuckDBStore(primary) as store:
        store._conn.execute("UPDATE daily_canonical_publication SET is_current=FALSE")
    assert read()["screen_run_evidence"].rows == ()
