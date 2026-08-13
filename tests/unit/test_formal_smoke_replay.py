"""Formal-only fixed Stage 1 strategy replay tests."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError


def _open_signed_runtime_capability(
    tmp_path: Path,
    *,
    provenance_commit: str = "d" * 40,
) -> "RuntimeCodeGenerationCapability":
    """Open the same signed immutable-generation capability used by runtime tests."""
    from rquant.runtime_code_generation import RuntimeCodeGenerationCapability
    from tests.runtime_code_e2e_support import (
        build_test_package,
        install_test_package,
        open_test_capability,
    )

    package = build_test_package(
        tmp_path / "runtime-package",
        provenance_commit=provenance_commit,
    )
    trusted_base, runtime_root, _installer = install_test_package(
        tmp_path,
        package,
    )
    return open_test_capability(
        trusted_base=trusted_base,
        runtime_root=runtime_root,
        package=package,
    )


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

    capability = _open_signed_runtime_capability(tmp_path)
    request = FormalSmokeReplayRequest(
        strategy="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
        code_commit="d" * 40,
        runtime_capability=capability,
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

    try:
        result = run_formal_smoke_replay(request, base_dir=tmp_path)
        repeated = run_formal_smoke_replay(request, base_dir=tmp_path)
    finally:
        capability.close()

    assert len(captured_requests) == 2
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
    assert repeated.run_id != result.run_id
    assert repeated.strategy_spec_hash == result.strategy_spec_hash
    assert repeated.result_hash == result.result_hash

    saved = load_strategy_lab_run(result.run_id, base_dir=tmp_path)
    assert saved.manifest.schema_version == 3
    assert saved.manifest.research_status == "comparable"
    assert saved.manifest.dataset_snapshot_id == request.dataset_snapshot_id
    assert (
        saved.manifest.dataset_binding_hash
        == request.dataset_binding_hash
    )
    assert saved.manifest.strategy_spec_hash == result.strategy_spec_hash
    assert saved.manifest.result_hash == result.result_hash
    assert saved.manifest.missing_evidence == []
    assert saved.manifest.code_trust_evidence == request.runtime_capability.evidence
    evidence = next(
        table for table in saved.tables if table.name == "formal_evidence"
    )
    assert evidence.rows == [
        {
            "audit_run_id": request.audit_run_id,
            "dataset_snapshot_id": request.dataset_snapshot_id,
            "dataset_binding_hash": request.dataset_binding_hash,
            "code_commit": request.code_commit,
            "code_trust_evidence": request.runtime_capability.evidence.model_dump(
                mode="json"
            ),
        }
    ]


def test_formal_smoke_result_hash_is_stable_when_strategy_row_order_changes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import rquant.formal_smoke_replay as smoke_module
    from rquant.formal_smoke_replay import (
        FormalSmokeComputation,
        FormalSmokeReplayRequest,
        run_formal_smoke_replay,
    )

    capability = _open_signed_runtime_capability(tmp_path)
    request = FormalSmokeReplayRequest(
        strategy="growth_board_surge",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
        code_commit="d" * 40,
        runtime_capability=capability,
    )
    execution_count = 0

    @contextmanager
    def open_exact_gate(_gate_request):
        yield object(), _formal_decision()

    def execute_with_unstable_order(_store, _spec):
        nonlocal execution_count
        execution_count += 1
        rows = [
            {
                "ts_code": "300002.SZ",
                "entry_time": pd.Timestamp("2026-05-06 09:37:00"),
                "ret_pct": -1.25,
            },
            {
                "ts_code": "300001.SZ",
                "entry_time": pd.Timestamp("2026-05-05 09:36:00"),
                "ret_pct": 3.5,
            },
        ]
        if execution_count % 2 == 0:
            rows.reverse()
        trades = pd.DataFrame(rows)
        return FormalSmokeComputation(
            metrics={
                "trade_count": 2,
                "mean_ret_pct": 1.125,
                "win_rate_pct": 50.0,
            },
            tables={
                "strategy_summary": pd.DataFrame(
                    [
                        {"metric": "win_rate_pct", "value": 50.0},
                        {"metric": "mean_ret_pct", "value": 1.125},
                    ][:: 1 if execution_count % 2 else -1]
                ),
                "trades": trades,
            },
            sample_count=2,
        )

    monkeypatch.setattr(
        smoke_module,
        "open_gated_research_store",
        open_exact_gate,
    )
    monkeypatch.setattr(
        smoke_module,
        "_execute_formal_smoke_spec",
        execute_with_unstable_order,
    )

    try:
        first = run_formal_smoke_replay(request, base_dir=tmp_path)
        second = run_formal_smoke_replay(request, base_dir=tmp_path)
    finally:
        capability.close()

    assert first.run_id != second.run_id
    assert first.result_hash == second.result_hash


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

    capability = _open_signed_runtime_capability(tmp_path)
    request = FormalSmokeReplayRequest(
        strategy="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
        code_commit="d" * 40,
        runtime_capability=capability,
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

    try:
        with pytest.raises(PermissionError, match=message):
            run_formal_smoke_replay(request, base_dir=tmp_path)
    finally:
        capability.close()

    assert executed is False
    assert not (tmp_path / "strategy_lab_runs").exists()


def test_formal_smoke_rejects_missing_runtime_generation_capability(
    tmp_path: Path,
) -> None:
    from rquant.formal_smoke_replay import FormalSmokeReplayRequest

    with pytest.raises(ValidationError, match="runtime_capability"):
        FormalSmokeReplayRequest(
            strategy="n_shape",
            start_date=date(2026, 4, 1),
            end_date=date(2026, 7, 2),
            audit_run_id="a" * 64,
            dataset_snapshot_id="b" * 64,
            dataset_binding_hash="c" * 64,
            code_commit="d" * 40,
        )

    assert not (tmp_path / "strategy_lab_runs").exists()


def test_formal_smoke_rejects_capability_provenance_commit_mismatch(
    tmp_path: Path,
) -> None:
    from rquant.formal_smoke_replay import FormalSmokeReplayRequest

    capability = _open_signed_runtime_capability(tmp_path)
    try:
        with pytest.raises(ValidationError, match="code_commit"):
            FormalSmokeReplayRequest(
                strategy="n_shape",
                start_date=date(2026, 4, 1),
                end_date=date(2026, 7, 2),
                audit_run_id="a" * 64,
                dataset_snapshot_id="b" * 64,
                dataset_binding_hash="c" * 64,
                code_commit="e" * 40,
                runtime_capability=capability,
            )
    finally:
        capability.close()


@pytest.mark.parametrize(
    "invalid_state",
    ["closed", "generation_changed", "bundle_tampered"],
)
def test_formal_smoke_rejects_invalid_runtime_generation_before_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid_state: str,
) -> None:
    import rquant.formal_smoke_replay as smoke_module
    from rquant.formal_smoke_replay import (
        FormalSmokeReplayRequest,
        run_formal_smoke_replay,
    )

    capability = _open_signed_runtime_capability(tmp_path)
    request = FormalSmokeReplayRequest(
        strategy="n_shape",
        start_date=date(2026, 4, 1),
        end_date=date(2026, 7, 2),
        audit_run_id="a" * 64,
        dataset_snapshot_id="b" * 64,
        dataset_binding_hash="c" * 64,
        code_commit="d" * 40,
        runtime_capability=capability,
    )
    executed = False

    def reject_execution(_store: object, _spec: object) -> object:
        nonlocal executed
        executed = True
        raise AssertionError("strategy compute must not run")

    monkeypatch.setattr(smoke_module, "_execute_formal_smoke_spec", reject_execution)
    if invalid_state == "closed":
        capability.close()
    elif invalid_state == "generation_changed":
        pointer = capability.loaded.generation_root.parent.parent / "current"
        pointer.write_text("e" * 64 + "\n", encoding="ascii")
    else:
        source = capability.release_root / "src/rquant/app.py"
        replacement = source.with_name("replacement.py")
        replacement.write_bytes(b"TAMPERED = True\n")
        replacement.chmod(0o444)
        replacement.replace(source)

    try:
        with pytest.raises(Exception, match="runtime|generation|capability|unchanged"):
            run_formal_smoke_replay(request, base_dir=tmp_path)
    finally:
        capability.close()

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
