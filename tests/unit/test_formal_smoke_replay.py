"""Formal-only fixed Stage 1 strategy replay tests."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.parametrize(
    ("strategy", "run_type"),
    [
        ("n_shape", "n_shape_compare"),
        ("growth_board_surge", "growth_board_surge"),
        ("auction_gap", "auction_gap"),
    ],
)
def test_formal_smoke_specs_are_fixed_versioned_and_hash_stable(
    strategy: str,
    run_type: str,
) -> None:
    from rquant.formal_smoke_replay import build_formal_smoke_spec

    first = build_formal_smoke_spec(
        strategy,
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )
    second = build_formal_smoke_spec(
        strategy,
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )

    assert first == second
    assert first.run_type == run_type
    assert first.fixed_spec_version == "stage1-smoke-v1"
    assert first.start_date == date(2026, 4, 1)
    assert first.end_date == date(2026, 7, 2)
    assert len(first.spec_hash) == 64
    assert first.spec_hash == second.spec_hash
    assert first.model_copy(
        update={
            "parameters": {
                **first.parameters,
                "test_parameter": "changed",
            }
        }
    ).spec_hash != first.spec_hash


def test_formal_smoke_specs_bind_the_documented_v1_parameters() -> None:
    from rquant.formal_smoke_replay import build_formal_smoke_spec

    n_shape = build_formal_smoke_spec(
        "n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )
    growth = build_formal_smoke_spec(
        "growth_board_surge",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )
    auction = build_formal_smoke_spec(
        "auction_gap",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )

    assert n_shape.parameters["preset_name"] == "n-shape-combined"
    assert n_shape.parameters["entry_modes"] == ["first_break"]
    assert n_shape.parameters["profile_variants"] == ["baseline"]
    assert n_shape.parameters["max_hold_days"] == 1
    assert n_shape.parameters["factor_score_threshold"] == 35.0
    assert growth.parameters["lookback_days"] == 20
    assert growth.parameters["min_hist_days"] == 10
    assert growth.parameters["min_cum_amount_ratio"] == 1.4
    assert growth.parameters["min_same_minute_amount_ratio"] == 2.0
    assert growth.parameters["min_amount_accel_5m"] == 2.0
    assert growth.parameters["require_vwap_strength"] is True
    assert growth.parameters["max_hold_days"] == 1
    assert auction.parameters["gap_mode"] == "close"
    assert auction.parameters["st_filter"] == "case_insensitive"
    assert auction.parameters["min_auction_vol_ratio_5d"] == 0.15
    assert auction.parameters["max_auction_vol_ratio_5d"] == 5.0
    assert auction.parameters["max_hold_days"] == 1


def test_formal_smoke_spec_rejects_unknown_strategy_and_invalid_range() -> None:
    from rquant.formal_smoke_replay import build_formal_smoke_spec

    with pytest.raises(ValueError, match="unsupported formal smoke strategy"):
        build_formal_smoke_spec(
            "unknown",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 2),
        )
    with pytest.raises(ValueError, match="start_date"):
        build_formal_smoke_spec(
            "n_shape",
            start_date=date(2026, 7, 3),
            end_date=date(2026, 7, 2),
        )


def _formal_decision(
    *,
    audit_run_id: str = "a" * 64,
    snapshot_id: str = "b" * 64,
    binding_hash: str = "c" * 64,
    research_status: str = "comparable",
):
    from rquant.research_gate import ResearchGateDecision

    return ResearchGateDecision(
        allowed=True,
        research_status=research_status,
        audit_run_id=audit_run_id,
        dataset_snapshot_id=snapshot_id,
        dataset_binding_hash=binding_hash,
        coverage_ratios={
            "eligibility": 1.0,
            "baseline": 1.0,
            "entry": 1.0,
            "exit": 1.0,
        },
        coverage_counts={
            "eligibility": (10, 10),
            "baseline": (100, 100),
            "entry": (10, 10),
            "exit": (100, 100),
        },
        failures=(),
    )


def test_formal_smoke_execution_uses_exact_gate_and_persists_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.formal_smoke_replay as smoke_module
    from rquant.dashboard.strategy_lab_runs import load_strategy_lab_run
    from rquant.formal_smoke_replay import (
        FormalSmokeComputation,
        FormalSmokeReplayRequest,
        run_formal_smoke_replay,
    )

    request = FormalSmokeReplayRequest(
        strategy="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
        code_commit="d" * 40,
    )
    captured_requests = []
    execution_store = object()

    @contextmanager
    def open_exact_gate(gate_request):
        captured_requests.append(gate_request)
        yield execution_store, _formal_decision()

    def execute_fixed_spec(store, spec):
        assert store is execution_store
        assert spec.strategy == "n_shape"
        return FormalSmokeComputation(
            metrics={
                "candidate_count": 2,
                "trade_count": 1,
                "mean_ret_pct": 3.5,
                "win_rate_pct": 100.0,
            },
            tables={
                "strategy_summary": pd.DataFrame(
                    [{"strategy": "n_shape", "trades": 1}]
                ),
                "trades": pd.DataFrame(
                    [{"ts_code": "000001.SZ", "ret_pct": 3.5}]
                ),
            },
            sample_count=1,
        )

    monkeypatch.setattr(
        smoke_module,
        "open_gated_research_store",
        open_exact_gate,
    )
    monkeypatch.setattr(
        smoke_module,
        "_execute_formal_smoke_spec",
        execute_fixed_spec,
    )

    result = run_formal_smoke_replay(request, base_dir=tmp_path)

    assert len(captured_requests) == 1
    gate_request = captured_requests[0]
    assert gate_request.mode == "formal"
    assert gate_request.strategy_name == "n_shape"
    assert gate_request.audit_run_id == request.audit_run_id
    assert gate_request.dataset_snapshot_id == request.dataset_snapshot_id
    assert gate_request.dataset_binding_hash == request.dataset_binding_hash
    assert gate_request.code_commit == request.code_commit
    assert result.status == "comparable"
    assert result.audit_run_id == request.audit_run_id
    assert result.dataset_snapshot_id == request.dataset_snapshot_id
    assert result.dataset_binding_hash == request.dataset_binding_hash
    assert result.sample_count == 1
    assert result.missing_evidence == ()
    assert len(result.strategy_spec_hash) == 64
    assert len(result.result_hash) == 64

    saved = load_strategy_lab_run(result.run_id, base_dir=tmp_path)
    assert saved.manifest.schema_version == 2
    assert saved.manifest.research_status == "comparable"
    assert saved.manifest.dataset_snapshot_id == request.dataset_snapshot_id
    assert (
        saved.manifest.dataset_binding_hash
        == request.dataset_binding_hash
    )
    assert saved.manifest.strategy_spec_hash == result.strategy_spec_hash
    assert saved.manifest.result_hash == result.result_hash
    assert saved.manifest.missing_evidence == []
    evidence = next(
        table for table in saved.tables if table.name == "formal_evidence"
    )
    assert evidence.rows == [
        {
            "audit_run_id": request.audit_run_id,
            "dataset_snapshot_id": request.dataset_snapshot_id,
            "dataset_binding_hash": request.dataset_binding_hash,
            "code_commit": request.code_commit,
        }
    ]


@pytest.mark.parametrize(
    ("decision", "message"),
    [
        (_formal_decision(audit_run_id="e" * 64), "audit_run_id"),
        (_formal_decision(snapshot_id="e" * 64), "dataset_snapshot_id"),
        (_formal_decision(binding_hash="e" * 64), "dataset_binding_hash"),
        (_formal_decision(research_status="exploratory"), "comparable"),
    ],
)
def test_formal_smoke_execution_fails_closed_on_gate_evidence_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    decision,
    message: str,
) -> None:
    import rquant.formal_smoke_replay as smoke_module
    from rquant.formal_smoke_replay import (
        FormalSmokeReplayRequest,
        run_formal_smoke_replay,
    )

    request = FormalSmokeReplayRequest(
        strategy="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
        code_commit="d" * 40,
    )
    executed = False

    @contextmanager
    def open_mismatched_gate(gate_request):
        del gate_request
        yield object(), decision

    def reject_execution(store, spec):
        nonlocal executed
        del store, spec
        executed = True
        raise AssertionError("strategy compute must not run")

    monkeypatch.setattr(
        smoke_module,
        "open_gated_research_store",
        open_mismatched_gate,
    )
    monkeypatch.setattr(
        smoke_module,
        "_execute_formal_smoke_spec",
        reject_execution,
    )

    with pytest.raises(PermissionError, match=message):
        run_formal_smoke_replay(request, base_dir=tmp_path)

    assert executed is False
    assert not (tmp_path / "strategy_lab_runs").exists()


def test_n_shape_formal_smoke_adapter_uses_only_fixed_v1_variant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.strategy_compare as compare_module
    from rquant.formal_smoke_replay import (
        _execute_formal_smoke_spec,
        build_formal_smoke_spec,
    )
    from rquant.strategy_compare import StrategyComparisonResult

    captured: dict[str, object] = {}

    def run_fixed(store, **kwargs):
        captured["store"] = store
        captured.update(kwargs)
        return StrategyComparisonResult(
            candidates_count=2,
            summary=pd.DataFrame(
                [
                    {
                        "entry_mode": "first_break",
                        "profile_variant": "baseline",
                        "trades": 1,
                    }
                ]
            ),
            trades=pd.DataFrame(
                [{"ts_code": "000001.SZ", "ret_pct": 3.5}]
            ),
        )

    monkeypatch.setattr(
        compare_module,
        "run_entry_mode_comparison",
        run_fixed,
    )
    store = object()
    spec = build_formal_smoke_spec(
        "n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )

    computation = _execute_formal_smoke_spec(store, spec)

    assert captured["store"] is store
    assert captured["entry_modes"] == ["first_break"]
    assert captured["profile_variants"] == ["baseline"]
    assert captured["preset_name"] == "n-shape-combined"
    assert captured["max_hold_days"] == 1
    assert captured["freq"] == "1min"
    assert captured["factor_score_threshold"] == 35.0
    assert computation.sample_count == 1
    assert computation.metrics["candidate_count"] == 2
    assert computation.metrics["trade_count"] == 1
    assert computation.metrics["mean_ret_pct"] == 3.5
    assert computation.metrics["win_rate_pct"] == 100.0
    assert set(computation.tables) == {"strategy_summary", "trades"}


def test_growth_formal_smoke_adapter_uses_complete_fixed_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.growth_board_surge_strategy as growth_module
    from rquant.formal_smoke_replay import (
        _execute_formal_smoke_spec,
        build_formal_smoke_spec,
    )

    captured: dict[str, object] = {}

    def run_fixed(store, **kwargs):
        captured["store"] = store
        captured.update(kwargs)
        return pd.DataFrame(
            [
                {
                    "ts_code": "300001.SZ",
                    "ret_pct": 2.0,
                    "hit_limit_up_today": True,
                },
                {
                    "ts_code": "688001.SH",
                    "ret_pct": -1.0,
                    "hit_limit_up_today": False,
                },
            ]
        )

    monkeypatch.setattr(
        growth_module,
        "run_growth_board_surge_replay",
        run_fixed,
    )
    store = object()
    spec = build_formal_smoke_spec(
        "growth_board_surge",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )

    computation = _execute_formal_smoke_spec(store, spec)

    config = captured["config"]
    assert captured["store"] is store
    assert captured["start_date"] == date(2026, 4, 1)
    assert captured["end_date"] == date(2026, 7, 2)
    assert config.model_dump(mode="python") == spec.parameters
    assert config.max_hold_days == 1
    assert computation.sample_count == 2
    assert computation.metrics["trade_count"] == 2
    assert computation.metrics["mean_ret_pct"] == 0.5
    assert computation.metrics["win_rate_pct"] == 50.0
    assert set(computation.tables) == {"strategy_summary", "trades"}


def test_auction_formal_smoke_adapter_reuses_baseline_candidates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.auction_gap_strategy as auction_module
    from rquant.formal_smoke_replay import (
        _execute_formal_smoke_spec,
        build_formal_smoke_spec,
    )

    baseline = pd.DataFrame(
        [
            {
                "ts_code": "605366.SH",
                "next_open_ret_pct": 1.0,
                "hit_limit_up_today": True,
                "intraday_high_ret_pct": 9.8,
                "day_close_ret_pct": 9.5,
            },
            {
                "ts_code": "002253.SZ",
                "next_open_ret_pct": -1.0,
                "hit_limit_up_today": False,
                "intraday_high_ret_pct": 3.0,
                "day_close_ret_pct": 1.0,
            },
        ]
    )
    minute = pd.DataFrame(
        [
            {
                "ts_code": "605366.SH",
                "ret_pct": 4.0,
                "b_hit_limit_up_today": True,
                "exit_reason": "trailing_stop",
            }
        ]
    )
    captured: dict[str, object] = {}

    def run_baseline(store, config):
        captured["baseline_store"] = store
        captured["baseline_config"] = config
        return baseline

    def run_minute(store, config, **kwargs):
        captured["minute_store"] = store
        captured["minute_config"] = config
        captured.update(kwargs)
        return minute

    monkeypatch.setattr(
        auction_module,
        "run_auction_gap_replay",
        run_baseline,
    )
    monkeypatch.setattr(
        auction_module,
        "run_auction_gap_minute_replay",
        run_minute,
    )
    store = object()
    spec = build_formal_smoke_spec(
        "auction_gap",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
    )

    computation = _execute_formal_smoke_spec(store, spec)

    config = captured["minute_config"]
    assert captured["baseline_store"] is store
    assert captured["minute_store"] is store
    assert captured["candidates"] is baseline
    assert captured["persist_positions"] is False
    assert config.start_date == "2026-04-01"
    assert config.end_date == "2026-07-02"
    assert config.max_hold_days == 1
    assert captured["baseline_config"] == config.auction_config()
    assert computation.sample_count == 1
    assert computation.metrics["candidate_count"] == 2
    assert computation.metrics["trade_count"] == 1
    assert computation.metrics["mean_ret_pct"] == 4.0
    assert computation.metrics["win_rate_pct"] == 100.0
    assert set(computation.tables) == {
        "strategy_comparison",
        "baseline_candidates",
        "trades",
    }
